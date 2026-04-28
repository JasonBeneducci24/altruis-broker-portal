"""
HTTP client for the Joshu API.

ARCHITECTURE AND SAFETY
=======================

This client talks to the real Joshu API at ``{JOSHU_BASE_URL}/api/insurance/v3/*``.

Test vs. production filtering uses Joshu's ``test`` boolean query parameter
on list endpoints:

    GET  /api/insurance/v3/submissions?test=true            (test only)
    GET  /api/insurance/v3/submissions?test=false           (production only)
    GET  /api/insurance/v3/policies?test=true&_page=1&status=Active

This is the actual Joshu mechanism documented in the API spec. An earlier
version of this client used a ``container=Test`` query parameter that does
NOT exist in the Joshu API and was being silently ignored, causing list
endpoints to return all records (test + production mixed).

**SAFETY INVARIANTS**:

  1. ``_test_filter`` is fixed at construction time based on the startup
     environment (``JOSHU_ENVIRONMENT``). It CANNOT be overridden per call.
  2. ``_build_params()`` injects ``test=<bool>`` on list endpoints and
     strips any caller attempt to set ``test`` or the legacy ``container``
     parameter, logging the attempt as a safety event.
  3. ``_assert_test_mode_for_write()`` runs BEFORE any mutating call to
     refuse production writes when the production override is not set.
  4. ``_verify_record_matches_mode()`` runs BEFORE every write — fetches
     the target record and refuses the write if its ``test`` field
     doesn't match the environment. Guards against the case where a
     production record ID surfaces in the UI (stale link, manual URL
     edit, list-filter bug) and a write is attempted against it.
  5. Env guard in ``config.py`` — app refuses to start with
     JOSHU_ENVIRONMENT unset, or in production without the explicit
     override token.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

import httpx
import re

from app.config import settings
from app.joshu.client_base import JoshuClientBase
from app.joshu.schemas import (
    Product, Policy, Submission, Quote, Document, Transaction,
    BrokerUser, Paginated, OngoingChange, DocumentType
)


log = logging.getLogger("altruis.joshu.http")

# ─────────────────────────────────────────────────────────────
# Per-operation write flags
# ─────────────────────────────────────────────────────────────
# Writes are unlocked individually, not all at once. Each flag gates ONE
# Joshu-mutating operation. When disabled (False), that method raises
# HttpClientNotReadyError. This lets us turn on writes progressively as
# they're verified against real Joshu behavior.
#
# PHASE 1 (read-only): all False
# PHASE 2 (edit workflow): _UPDATE_SUBMISSION_DATA, _UPDATE_SUBMISSION enabled
#   — lets brokers fill in, save, and submit/resubmit drafts
# PHASE 3 (full lifecycle): all enabled
#
# We're in PHASE 2 now. Production gate still applies regardless.

_ENABLE_UPDATE_SUBMISSION_DATA = True   # PUT /submission-data/{id}
_ENABLE_UPDATE_SUBMISSION = True        # PUT /submissions/{id} (status change)
_ENABLE_CREATE_POLICY = False           # POST /policies
_ENABLE_CREATE_TRANSACTION = False      # POST /transactions
_ENABLE_UPDATE_QUOTE = False            # PUT /quotes/{id} (publish/bind)


class HttpClientNotReadyError(RuntimeError):
    """Raised when a disabled write method is called."""


def _not_ready(operation: str) -> HttpClientNotReadyError:
    return HttpClientNotReadyError(
        f"HttpJoshuClient.{operation}() is not yet enabled.\n"
        f"  Per-operation write flag is False.\n"
        f"  JOSHU_ENVIRONMENT='{settings.joshu_environment}'\n"
        "  To enable, flip the corresponding _ENABLE_* constant in client_http.py\n"
        "  and verify behavior against the Joshu test container."
    )


def _flatten_code_value_array(raw: Any) -> dict[str, Any]:
    """Flatten Joshu's [{code, asset_idx, value}] datapoint response.

    Joshu returns datapoint values as an array of objects like::

        [
          {"code": "insured.name", "asset_idx": 0, "value": {"V1": {"Text": "Acme LLC"}}},
          {"code": "app.location_number", "asset_idx": 0, "value": {"V1": {"Number": "1.00"}}},
          {"code": "app.location_number", "asset_idx": 1, "value": {"V1": {"Number": "2.00"}}},
          ...
        ]

    For ASSET fields, the same `code` appears once per asset instance,
    differentiated by `asset_idx`. Our old flatten just used code as the key,
    which collapsed all asset values down to the last one written — losing data.

    New representation:
      - result[code] = simple value (for non-asset / asset_idx=0 convenience)
      - result["_assets"][code][asset_idx] = simple value (for asset-aware lookups)
      - result["_raw"] = original response

    Callers that only care about scalar datapoints (insured.name, app.*) keep
    using result[code]. Callers that need asset data (structures, locations)
    walk result["_assets"][code] instead.
    """
    if not isinstance(raw, list):
        return {"_raw": raw, "_assets": {}}

    result: dict[str, Any] = {"_raw": raw, "_assets": {}}
    for item in raw:
        if not isinstance(item, dict):
            continue
        code = item.get("code")
        if not code:
            continue
        asset_idx = item.get("asset_idx", 0) or 0
        try:
            asset_idx = int(asset_idx)
        except (TypeError, ValueError):
            asset_idx = 0

        extracted = _extract_simple_value(item.get("value"))

        # Asset-indexed map
        if code not in result["_assets"]:
            result["_assets"][code] = {}
        result["_assets"][code][asset_idx] = extracted

        # Scalar-style convenience: keep the asset_idx=0 value at the top level
        # (most non-asset fields only have index 0 anyway)
        if asset_idx == 0 or code not in result:
            result[code] = extracted
    return result


def _get_value_for_field(data: dict[str, Any], code: str, asset_idx: int = 0) -> Any:
    """Retrieve the value for a specific (code, asset_idx) from flattened data.

    Use this instead of ``data[code]`` when you know the field might be an
    asset-indexed field. Falls back to the top-level scalar if no asset
    mapping is present (covers data from older code paths).
    """
    if not isinstance(data, dict):
        return None
    assets_map = data.get("_assets") or {}
    if code in assets_map:
        vals = assets_map[code]
        if asset_idx in vals:
            return vals[asset_idx]
    # Fall back to scalar lookup for safety
    return data.get(code)


def _merge_asset_data(data: dict[str, Any], asset_data: Any) -> None:
    """Merge asset-data response into the flattened data map.

    The /asset-data/{id} endpoint returns, for each asset collection on the
    submission, a list of per-asset records. The exact wire shape was not
    verifiable from the docs alone, so this function handles the most likely
    candidates defensively:

    Candidate A — top-level array of {code, value: {V1: {Array: [...]}}}::

        [
          {"code": "app.structures",
           "value": {"V1": {"Array": [
             {"V1": {"Array": [
               {"code": "location_number",   "value": {"V1": {"Number": "1.00"}}},
               {"code": "location_address",  "value": {"V1": {"Text": "2811 John D Odom Rd..."}}},
               ...
             ]}},
             {"V1": {"Array": [...]}},   # second structure
           ]}}},
          {"code": "app.PerilsScoringAddresses", "value": {...}},
        ]

    Candidate B — flat list with asset_idx per entry::

        [
          {"code": "structures.location_number", "asset_idx": 0, "value": {"V1": {"Number": "1.00"}}},
          {"code": "structures.location_number", "asset_idx": 1, "value": {"V1": {"Number": "2.00"}}},
          ...
        ]

    Candidate C — dict keyed by collection code::

        {
          "app.structures": [ {record}, {record}, ... ],
          "app.PerilsScoringAddresses": [ ... ],
        }

    For each asset record we see, we populate ``data["_assets"][sub_code][asset_idx]``
    where sub_code is the full field code. For Candidate A we synthesize
    codes by prefixing the sub-field code with the collection code base
    (so "location_number" in an `app.structures` record becomes
    "app.location_number" to match the schema codes Joshu's /submission-status
    returns for the app.structures section).

    We never raise — if the response has an unexpected shape, we log and
    skip. This means in the worst case, asset fields remain "Not provided"
    but the rest of the form still renders.
    """
    if not isinstance(data, dict):
        return
    assets_map = data.setdefault("_assets", {})

    # Candidate C first — a dict keyed by collection code
    if isinstance(asset_data, dict):
        for collection_code, records in asset_data.items():
            if not isinstance(records, list):
                continue
            _merge_one_collection(assets_map, collection_code, records)
        return

    # Candidates A and B are both lists, distinguishable by inner structure
    if not isinstance(asset_data, list):
        if asset_data is not None:
            log.warning("asset_data had unexpected shape: %s", type(asset_data).__name__)
        return

    # Candidate B detection: entries with asset_idx
    looks_like_b = any(
        isinstance(e, dict) and "asset_idx" in e for e in asset_data
    )
    if looks_like_b:
        for item in asset_data:
            if not isinstance(item, dict):
                continue
            code = item.get("code")
            if not code:
                continue
            try:
                idx = int(item.get("asset_idx", 0) or 0)
            except (TypeError, ValueError):
                idx = 0
            val = _extract_simple_value(item.get("value"))
            assets_map.setdefault(code, {})[idx] = val
        return

    # Candidate A: top-level array of {code, value: {V1: {Array: [...]}}}
    for item in asset_data:
        if not isinstance(item, dict):
            continue
        collection_code = item.get("code")
        if not collection_code:
            continue
        # Value should be {V1: {Array: [...records...]}} (or similar)
        value_wrap = item.get("value")
        records = _extract_array_records(value_wrap)
        if records is not None:
            _merge_one_collection(assets_map, collection_code, records)


def _extract_array_records(wrapped: Any) -> list | None:
    """Try to pull the inner array out of a V1/V0/Plain-wrapped Array value.

    Returns the list of per-asset records, or None if the shape doesn't match.
    """
    if not isinstance(wrapped, dict):
        return None
    # Walk through V1 / V0 / Plain wrappers looking for an Array
    for wrapper_key in ("V1", "V0", "Plain"):
        if wrapper_key in wrapped and isinstance(wrapped[wrapper_key], dict):
            inner = wrapped[wrapper_key]
            if "Array" in inner and isinstance(inner["Array"], list):
                return inner["Array"]
            # Recurse — sometimes Plain is nested inside V1
            deeper = _extract_array_records(inner)
            if deeper is not None:
                return deeper
    if "Array" in wrapped and isinstance(wrapped["Array"], list):
        return wrapped["Array"]
    return None


def _merge_one_collection(
    assets_map: dict[str, dict[int, Any]],
    collection_code: str,
    records: list,
) -> None:
    """Expand one asset collection's records into the assets_map.

    Each record may come in two shapes:
      (a) A list of {code, value} entries representing one asset's fields
      (b) A {V1: {Array: [{code, value}, ...]}} wrapper around that list
      (c) A plain dict {code: value} (simpler shape)

    The ``collection_code`` is the outer code (e.g. "app.structures"). When
    the records use short sub-codes like "location_number", we synthesize
    the full code by replacing the last segment of the collection code —
    so "app.structures" + "location_number" → "app.location_number" (which
    matches what the schema reports).
    """
    # Derive the prefix we'll use for sub-codes. collection_code might be
    # "app.structures" — replace "structures" with the sub-field name.
    prefix_base = collection_code.rsplit(".", 1)[0] if "." in collection_code else ""

    for idx, record in enumerate(records):
        # Unwrap if needed
        entries = record
        if isinstance(record, dict):
            # Try common wrapper patterns
            arr = _extract_array_records(record)
            if arr is not None:
                entries = arr
            elif any(isinstance(v, dict) and ("V1" in v or "V0" in v or "Plain" in v)
                     for v in record.values()):
                # Plain dict of {sub_code: wrapped_value}
                for sub_code, wrapped in record.items():
                    val = _extract_simple_value(wrapped)
                    full_code = f"{prefix_base}.{sub_code}" if prefix_base else sub_code
                    assets_map.setdefault(full_code, {})[idx] = val
                    # Also register under the exact sub_code for flexibility
                    assets_map.setdefault(sub_code, {})[idx] = val
                continue

        if isinstance(entries, list):
            for e in entries:
                if not isinstance(e, dict):
                    continue
                sub_code = e.get("code")
                if not sub_code:
                    continue
                val = _extract_simple_value(e.get("value"))
                full_code = f"{prefix_base}.{sub_code}" if prefix_base and "." not in sub_code else sub_code
                assets_map.setdefault(full_code, {})[idx] = val


def _extract_simple_value(wrapped: Any) -> Any:
    """Unwrap Joshu's versioned-tagged discriminated-union values.

    Accepts shapes like:
      {"V1": {"Text": "..."}} → "..."
      {"V1": {"Number": "5000"}} → "5000"
      {"V1": {"Boolean": true}} → True
      {"V1": {"Null": {}}} → None
      {"V1": {"Monetary": {"currency": "USD", "amount": "1000"}}} → dict as-is
      {"V1": {"Location": {"NamedParsedAddress": {"name": "..."}}}} → dict as-is
      {"V1": {"Array": [...]}} → list (each element recursively unwrapped)

    Values we don't recognize pass through untouched for the UI to render.
    """
    if wrapped is None:
        return None
    if not isinstance(wrapped, dict):
        return wrapped
    # Unwrap the version/wrapper tag — Joshu uses "Plain" for the current
    # version of data responses; "V0"/"V1" are documented in the spec for
    # legacy compatibility. Accept any of these.
    inner = None
    for version_key in ("Plain", "V1", "V0"):
        if version_key in wrapped:
            inner = wrapped[version_key]
            break
    if inner is None:
        # Unknown wrapper — take first key if there's only one
        if len(wrapped) == 1:
            inner = next(iter(wrapped.values()))
        else:
            return wrapped

    if not isinstance(inner, dict):
        return inner

    # Type discriminator — Null/Boolean/Text/Number/Monetary/Date/DateTime/Location/Array
    for type_key in ("Null",):
        if type_key in inner:
            return None
    for type_key in ("Text", "Number", "Date", "DateTime", "Boolean"):
        if type_key in inner:
            return inner[type_key]
    if "Monetary" in inner:
        # Return the whole monetary dict — the UI can render "1000 USD"
        return inner["Monetary"]
    if "Location" in inner:
        # Location has its own OneOf shape — pass through and let UI handle
        return inner["Location"]
    if "Array" in inner:
        arr = inner["Array"]
        return [_extract_simple_value(v) for v in arr] if isinstance(arr, list) else arr
    # Unknown type — return as-is
    return inner


def _wrap_value_for_put(value: Any, type_hint: str | None = None) -> dict[str, Any]:
    """Inverse of _extract_simple_value — wrap a Python value into Joshu's
    V1/{TypeTag: value} shape for PUT requests.

    Joshu's read path returns values wrapped with the "Plain" tag, but the
    write path requires "V1" (per the OpenAPI docs, and confirmed by the
    error message "unknown variant 'Plain', expected 'V1'").

    The shape Joshu expects for PUT /submission-data/{id}::

        {"data": [
          {"code": "insured.name", "value": {"V1": {"Text": "Acme LLC"}}},
          {"code": "app.aop_deductible", "value": {"V1": {"Number": "5000"}}},
          {"code": "app.cyber_status", "value": {"V1": {"Boolean": true}}},
          {"code": "app.effective_date", "value": {"V1": {"Date": "2026-06-01"}}},
        ]}

    type_hint lets callers force a specific tag ("Text", "Number", etc.) —
    without it we infer from the Python type.
    """
    if value is None:
        return {"V1": {"Null": {}}}
    if type_hint:
        tag = type_hint
    elif isinstance(value, bool):
        tag = "Boolean"
    elif isinstance(value, (int, float)):
        tag = "Number"
    elif isinstance(value, str):
        # Heuristic: ISO date → Date, else Text
        if len(value) == 10 and value[4] == "-" and value[7] == "-":
            tag = "Date"
        else:
            tag = "Text"
    elif isinstance(value, dict):
        # Monetary and Location pass through as their own tag — caller provides shape
        if "currency" in value and "amount" in value:
            tag = "Monetary"
        elif any(k in value for k in ("formatted_address", "NamedParsedAddress", "ParsedGoogleAddress")):
            tag = "Location"
        else:
            # Unknown dict; try as Text-encoded JSON fallback
            import json as _json
            return {"V1": {"Text": _json.dumps(value)}}
    elif isinstance(value, list):
        return {"V1": {"Array": [_wrap_value_for_put(v).get("V1", {}) for v in value]}}
    else:
        return {"V1": {"Text": str(value)}}

    # Numeric values must be sent as strings per the Joshu schema
    if tag == "Number" and not isinstance(value, str):
        value = str(value)
    return {"V1": {tag: value}}


def _encode_data_payload(code_values: dict[str, Any], type_hints: dict[str, str] | None = None) -> dict[str, Any]:
    """Turn {code: value} into the body Joshu's PUT /submission-data expects.

    type_hints is an optional {code: type_tag} map. When provided, each
    value is wrapped using the specified tag. Otherwise the wrapper
    infers the tag from the Python type.
    """
    type_hints = type_hints or {}
    entries = []
    for code, val in code_values.items():
        if code.startswith("_"):  # skip internal keys like _raw
            continue

        hint = type_hints.get(code)
        # If Joshu expects a Location but the user typed a plain string,
        # wrap it as NamedParsedAddress so Joshu accepts it.
        if hint == "Location" and isinstance(val, str):
            val = {"NamedParsedAddress": {"name": val}}
        entries.append({"code": code, "value": _wrap_value_for_put(val, type_hint=hint)})
    return {"data": entries}


# ─────────────────────────────────────────────────────────────
# Schema normalization for /submission-status
# ─────────────────────────────────────────────────────────────

# Acronyms we want to render in UPPERCASE instead of title case
_UPPERCASE_TERMS = {
    "aop", "gl", "ui", "tria", "epli", "id", "us", "eb", "dba",
    "llc", "llp", "ein", "tin", "ssn", "uw", "ex", "iv", "pl",
    "tiv", "dep", "bpp", "ho", "bop", "cgl", "cpp", "faq", "url",
    "sr", "jr", "po", "usa", "ny", "ca", "tx", "fl",
}
# Words that should stay lowercase (prepositions, articles) when not first
_LOWERCASE_WORDS = {"of", "and", "or", "the", "a", "an", "in", "on", "at", "to", "for", "by"}

_CAMEL_SPLIT_RE = re.compile(r"([a-z0-9])([A-Z])")
_CONTIG_CAPS_RE = re.compile(r"([A-Z]+)([A-Z][a-z])")


def _split_tokens(segment: str) -> list[str]:
    """Break a code segment into words.

    Handles:
      - snake_case: "aop_deductible" → ["aop", "deductible"]
      - camelCase: "cyberStatus" → ["cyber", "Status"]
      - PascalCase: "EffectiveDate" → ["Effective", "Date"]
      - acronym-first: "EPLIstatus" → ["EPLI", "status"]
      - acronym-mid: "maxEPLILimit" → ["max", "EPLI", "Limit"]
      - mixed: "split_addressCity" → ["split", "address", "City"]

    Strategy: first split on explicit separators (_, -, space), then for
    each piece scan for known acronyms from _UPPERCASE_TERMS and extract
    them, falling back to camelCase splitting on the remainder.
    """
    if not segment:
        return []

    # Split on explicit separators
    parts = re.split(r"[_\-\s]+", segment)
    tokens: list[str] = []

    for part in parts:
        if not part:
            continue
        # Scan for known acronyms (sorted longest-first so EPLI wins over EL)
        remaining = part
        buf = ""

        def flush_buf():
            nonlocal buf
            if buf:
                # Split the buffer by camelCase
                pieces = _CAMEL_SPLIT_RE.sub(r"\1_\2", buf).split("_")
                for piece in pieces:
                    if piece:
                        tokens.append(piece)
                buf = ""

        # Build pattern: longest acronyms first
        sorted_acronyms = sorted(_UPPERCASE_TERMS, key=len, reverse=True)
        i = 0
        while i < len(remaining):
            matched = False
            for acr in sorted_acronyms:
                # Case-insensitive match but require the case in source to be all-upper
                n = len(acr)
                if i + n <= len(remaining):
                    candidate = remaining[i:i + n]
                    if candidate.upper() == acr.upper() and candidate.isupper():
                        # Must be followed by a non-letter or a lowercase letter
                        # (so we don't grab "EPL" out of "EPLOYMENT")
                        next_char = remaining[i + n] if i + n < len(remaining) else ""
                        if not next_char or not next_char.isupper() or \
                           (i + n + 1 < len(remaining) and remaining[i + n + 1].islower()):
                            flush_buf()
                            tokens.append(acr.upper())
                            i += n
                            matched = True
                            break
            if not matched:
                buf += remaining[i]
                i += 1
        flush_buf()

    return [t for t in tokens if t]


def _humanize_token(token: str) -> str:
    """Title-case a single token, respecting acronyms and lowercase words."""
    if not token:
        return token
    low = token.lower()
    if low in _UPPERCASE_TERMS:
        return low.upper()
    return token[0].upper() + token[1:].lower() if token[0].isalpha() else token


def _humanize_code(code: str) -> str:
    """Turn a dotted code into a human-readable label.

    Examples::
        insured.name                  → "Name"
        insured.split_address.zipcode → "Zip Code"   (special case)
        app.aop_deductible            → "AOP Deductible"
        app.EPLIstatus                → "EPLI Status"
        app.named_insured_structure   → "Named Insured Structure"
        app.claims_history_flag       → "Claims History Flag"
    """
    if not code:
        return code
    last = code.rsplit(".", 1)[-1]

    # A few hard-coded specializations that read better than their auto form
    hardcoded = {
        "zipcode": "Zip Code",
        "dob": "Date of Birth",
        "naics": "NAICS Code",
        "sic": "SIC Code",
        "fein": "FEIN",
        "yoe": "Years of Experience",
        "yob": "Year of Business",
        "glonlystatus": "GL Only Status",
        "ex_wind_status": "Ex Wind Status",
        "windhailpercentage": "Wind/Hail Percentage",
        "ol_2and3_limit": "OL 2 & 3 Limit",
        "lro_status": "LRO Status",
        "bpp_limit": "BPP Limit",
        "epliserp": "EPLI SERP",
        "eplistatus": "EPLI Status",
        "cglll_limit": "CGLL Limit",
        "sdll_limit": "SDLL Limit",
        "eb_sublimit": "EB Sublimit",
        "papersandrecords": "Papers and Records",
        "pollutantcleanuplim": "Pollutant Cleanup Limit",
        "waterbackuplimit": "Water Backup Limit",
        "sltaxstate": "SL Tax State",
        "sltaxmunicipality_total": "SL Tax Municipality Total",
        "tria_status": "TRIA Status",
        "roofacvstatus": "Roof ACV Status",
    }
    if last.lower() in hardcoded:
        return hardcoded[last.lower()]

    tokens = _split_tokens(last)
    if not tokens:
        return last
    out_tokens = []
    for i, t in enumerate(tokens):
        if i > 0 and t.lower() in _LOWERCASE_WORDS:
            out_tokens.append(t.lower())
        else:
            out_tokens.append(_humanize_token(t))
    return " ".join(out_tokens)


def _section_from_code(code: str) -> str:
    """Return the first path segment — e.g. 'insured' / 'app' / 'data'."""
    return code.split(".", 1)[0] if "." in code else "other"


def _infer_section_label(section_code: str, datapoints: list) -> str:
    """Guess a human-friendly section name based on its code and contents.

    Joshu section codes often look like "app.qV1fzIWW" or "app.structures"
    — a common prefix followed by either a meaningful word or a random id.

    Strategy (in priority order):
      1. If the section code has a second segment that's a known label or
         looks human-readable (contains "_" or lowercase words), use THAT
         as the label. So "app.structures" → "Structures",
         "app.loc_specific_enhancements" → "Loc Specific Enhancements".
      2. If the second segment looks random (mixed case random id like
         "qV1fzIWW"), fall back to the FIRST segment ("app" → "Application"
         via PREFIX_LABELS).
      3. If no code, look at the datapoints' dominant prefix.
      4. Final fallback: humanize the code, or return "Section".
    """
    PREFIX_LABELS = {
        "insured":    "Insured",
        "insureds":   "Insured",
        "app":        "Application",
        "property":   "Property",
        "structure":  "Structures",
        "structures": "Structures",
        "location":   "Locations",
        "locations":  "Locations",
        "asset":      "Assets",
        "assets":     "Assets",
        "vehicle":    "Vehicles",
        "vehicles":   "Vehicles",
        "driver":     "Drivers",
        "drivers":    "Drivers",
        "employee":   "Employees",
        "employees":  "Employees",
        "data":       "Coverage",
        "coverage":   "Coverage",
        "bind":       "Bind Questions",
        "peril":      "Perils",
        "perils":     "Perils",
        "claim":      "Claims",
        "claims":     "Claims",
        "exposure":   "Exposures",
        "exposures":  "Exposures",
        "details":    "Details",
    }

    def _is_random_id(s: str) -> bool:
        """Detects random-looking section ids like 'qV1fzIWW' or 'Fn5RjSZ5'.
        A random id has NO underscores, mixes upper and lower case, and is
        short-ish (6–12 chars)."""
        if not s or "_" in s:
            return False
        if len(s) < 6 or len(s) > 14:
            return False
        if not any(c.isupper() for c in s):
            return False
        if not any(c.islower() for c in s):
            return False
        # Too many digits mixed in suggests a random id
        return True

    # PRIORITY 1 — use the second segment of the section code if meaningful
    if section_code and "." in section_code:
        first, rest = section_code.split(".", 1)
        second = rest.split(".", 1)[0]  # in case of 3+ segments
        # Known label for the second segment?
        if second.lower() in PREFIX_LABELS:
            return PREFIX_LABELS[second.lower()]
        # Looks like a real word (has underscore or is purely alphabetic word)?
        if "_" in second or (second.islower() and len(second) > 3):
            return _humanize_code(second)
        # Capitalized multi-word ("Additional_Insureds", "PerilsScoringAddresses")
        if "_" in second or (second[0].isupper() and not _is_random_id(second)):
            return _humanize_code(second)
        # Falls through: random id like "qV1fzIWW" → use first segment

    # PRIORITY 2 — first segment of section code
    if section_code:
        if "." in section_code:
            first = section_code.split(".", 1)[0].lower()
            if first in PREFIX_LABELS:
                return PREFIX_LABELS[first]
        # No dot — use the whole code if it's known
        lower = section_code.lower()
        if lower in PREFIX_LABELS:
            return PREFIX_LABELS[lower]

    # PRIORITY 3 — infer from dominant datapoint prefix
    prefix_counts: dict[str, int] = {}
    for dp in datapoints or []:
        if isinstance(dp, dict):
            dcode = dp.get("code", "")
            if "." in dcode:
                prefix = dcode.split(".", 1)[0].lower()
                prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1
    if prefix_counts:
        dominant = max(prefix_counts, key=prefix_counts.get)
        if dominant in PREFIX_LABELS:
            return PREFIX_LABELS[dominant]
        return _humanize_code(dominant)

    # PRIORITY 4 — humanize the section code itself, or generic fallback
    if section_code:
        if _is_random_id(section_code):
            return "Section"
        return _humanize_code(section_code)
    return "Section"


def _parse_field_type(kind: Any) -> dict[str, Any]:
    """Extract type info from Joshu's tagged type kind.

    Joshu shape:  {"Boolean": {}}
                  {"Text": {"format": "EmailAddress", "options": [...]}}
                  {"Number": {"format": {...}, "options": [...]}}
                  {"Monetary": {"format": {...}}}
                  {"Date": {"format": "MonthDayYear"}}
                  {"DateTime": {"date_format": "..."}}
                  {"Location": {}}
                  {"File": {...}}
                  {"User": {}}
                  {"Array": {"type": {...inner type...}}}
    """
    if not isinstance(kind, dict):
        return {"type": "unknown"}

    if "Boolean" in kind:
        return {"type": "boolean"}

    if "Text" in kind:
        details = kind.get("Text") or {}
        options = details.get("options") or []
        out = {
            "type": "text",
            "format": details.get("format"),  # EmailAddress / PhoneNumber / WebsiteAddress
            "default": details.get("default", {}).get("value") if details.get("default") else None,
        }
        if options:
            out["options"] = [
                {"value": o.get("value"), "label": o.get("display") or o.get("value")}
                for o in options
            ]
        return out

    if "Number" in kind:
        details = kind.get("Number") or {}
        options = details.get("options") or []
        fmt = details.get("format") or {}
        out = {
            "type": "number",
            "decimal_places": fmt.get("decimal_places", 0),
            "default": details.get("default", {}).get("value") if details.get("default") else None,
        }
        if options:
            out["options"] = [
                {"value": o.get("value"), "label": o.get("display") or o.get("value")}
                for o in options
            ]
        return out

    if "Monetary" in kind:
        details = kind.get("Monetary") or {}
        options = details.get("options") or []
        out = {
            "type": "monetary",
            "default": details.get("default", {}).get("value") if details.get("default") else None,
        }
        if options:
            out["options"] = [
                {"value": o.get("value"), "label": o.get("display") or o.get("value")}
                for o in options
            ]
        return out

    if "Date" in kind:
        return {"type": "date", "format": (kind.get("Date") or {}).get("format", "MonthDayYear")}

    if "DateTime" in kind:
        return {"type": "datetime", "format": (kind.get("DateTime") or {}).get("date_format", "MonthDayYear")}

    if "Location" in kind:
        return {"type": "location"}

    if "File" in kind:
        return {"type": "file"}

    if "User" in kind:
        return {"type": "user"}

    if "Array" in kind:
        inner = kind.get("Array", {}).get("type")
        inner_parsed = _parse_field_type(inner) if inner else {"type": "unknown"}
        return {"type": "array", "item": inner_parsed}

    return {"type": "unknown", "raw": kind}


def _is_underwriter_section(section_code: str, datapoints: list) -> bool:
    """Return True for sections the broker should never see.

    Perils Scoring Addresses is derived by the underwriter from the
    broker's Structures input. It has no business appearing in the
    broker portal's submission flow — it will be auto-populated from
    Structures in a later update.

    We detect it by either:
      - The section code containing "peril" (case-insensitive), OR
      - Any datapoint code starting with "perils" or "PerilsScoring"
    """
    if section_code:
        low = section_code.lower()
        if "peril" in low:
            return True
    # Fallback: inspect the datapoint codes
    for dp in datapoints or []:
        if isinstance(dp, dict):
            dcode = (dp.get("code") or "").lower()
            if dcode.startswith("peril"):
                return True
    return False


# Field codes (final segment, case-insensitive) the broker should never
# see in the portal — these are decisions the underwriter makes after
# reviewing the application, not data the broker provides. The fields
# remain in Joshu's schema and may be set there directly; we just don't
# render them in either the locked view or the edit form on the broker side.
_BROKER_HIDDEN_FIELD_CODES = {
    # Underwriter-managed fields originally in the small Application 1 set —
    # the broker doesn't supply these; they're decisions the UW makes.
    "renewal_status",
    "exclusions_status",
    "claims_history_flag",
    # Former "Application 4" section — entirely underwriter/internal-managed.
    # Removing these fields collapses Application 4 to zero broker-facing
    # fields, which means the section won't render at all (POST-PASS 2 will
    # see populated_count==0 + visible_count==0 and drop it).
    "different_valuation_status",
    "competitor_premium",
    "stop_loss_status",
    "sltaxstate",                  # codes are case-insensitive in _is_broker_hidden_field
    "sltaxmunicipality_total",
    "ag_enhancement_status",
    "roofacvstatus",
}


def _is_broker_hidden_field(code: str) -> bool:
    """Return True for individual datapoint codes hidden from the broker."""
    if not code:
        return False
    tail = code.rsplit(".", 1)[-1].lower()
    return tail in _BROKER_HIDDEN_FIELD_CODES


def normalize_submission_status(raw: Any, data_values: dict[str, Any] | None = None) -> dict[str, Any]:
    """Transform Joshu's /submission-status response into a UI-friendly schema.

    Per the Joshu v3 docs, the response shape is::

        {
          insured_details_section: {
            code: "insured",
            is_asset: bool,
            condition_met: bool | null,
            section_validation_issue: object | null,
            datapoints: [ SubmissionDatapointStatus, ... ]
          },
          counters: { total, completed, validation_issues, non_retryable_validation_issues },
          sections: [
            {
              code: "app" or "equipment" or "property" etc,
              is_asset: bool,
              condition_met: bool | null,
              section_validation_issue: object | null,
              datapoints: [ SubmissionDatapointStatus, ... ]
            },
            ...
          ],
          bind_sections: [ ... similar shape ]
        }

    Our job: walk every section's datapoints, tag each field with its
    section info, merge in current values, and return a flat list the
    UI can render.

    Output shape::

        {
          "fields": [
            {
              "code": "insured.name",
              "label": "Name",
              "section": "insured",           // tag from the source section's code
              "section_label": "Insured",     // humanized section name
              "section_order": 0,             // display order
              "asset_idx": 0,
              "type": "text",
              "required": true,
              "visible": true,
              "exists": true,
              "value": "Acme LLC",
              "options": null | [{value, label}],
              "format": null | "EmailAddress",
              "validation_error": null | "Wrong value type",
              "decimal_places": 2,
              "item": {...},  // for arrays
            },
            ...
          ],
          "sections": [
            {"code": "insured", "label": "Insured", "field_count": 6, "completed": 4,
             "has_errors": false, "order": 0},
            {"code": "app",     "label": "Application", "field_count": 14, "completed": 10, ...},
            ...
          ],
          "counters": {total, completed, validation_issues, ...},
          "has_errors": bool
        }
    """
    if not isinstance(raw, dict):
        return {"fields": [], "sections": [], "counters": {}, "has_errors": False}

    data_values = data_values or {}
    has_errors = False

    def _validation_error_from(issue: Any) -> str | None:
        """Flatten a Joshu validation_issue into a human-readable string."""
        if not isinstance(issue, dict):
            return None
        kind = issue.get("kind")
        if not isinstance(kind, dict):
            return None
        if "WrongValueType" in kind:
            return "Wrong value type"
        if "MissingRelatedDatapointAnswer" in kind:
            missing = kind["MissingRelatedDatapointAnswer"].get("missing_datapoints_answers", [])
            return f"Requires: {', '.join(missing)}" if missing else "Requires related field"
        if "IntegrationCallFailed" in kind:
            return kind["IntegrationCallFailed"].get("message", "Integration failed")
        if "IntegrationResponseParsingError" in kind:
            return kind["IntegrationResponseParsingError"].get("message", "Integration error")
        if "InvalidNumberOfAssetsInSection" in kind:
            info = kind["InvalidNumberOfAssetsInSection"]
            return f"Need {info.get('min_count', 0)}-{info.get('max_count', '∞')} items, have {info.get('asset_count', 0)}"
        return next(iter(kind.keys()), "Validation issue")

    def _process_datapoints(dps: list, section_code: str, section_label: str, section_order: int):
        """Extract fields from a section's datapoints array."""
        nonlocal has_errors
        results = []
        if not isinstance(dps, list):
            return results
        for dp in dps:
            if not isinstance(dp, dict):
                continue
            code = dp.get("code", "")
            if not code:
                continue

            # Skip underwriter-only fields (renewal_status, exclusions_status,
            # claims_history_flag) — these are decisions the underwriter makes,
            # not broker inputs. The fields remain in Joshu but never render here.
            if _is_broker_hidden_field(code):
                continue

            ve = _validation_error_from(dp.get("validation_issue"))
            if ve:
                has_errors = True

            type_info = _parse_field_type(dp.get("kind"))
            asset_idx = dp.get("asset_idx", 0) or 0
            try:
                asset_idx = int(asset_idx)
            except (TypeError, ValueError):
                asset_idx = 0

            # Asset-aware value lookup.
            # data_values may come in one of two shapes:
            #   (a) Legacy: {code: simple_value} (no asset support)
            #   (b) New:    {code: simple_value, "_assets": {code: {idx: value}}}
            # We prefer (b) so assets with idx>0 get their real values,
            # not the idx=0 collapsed value.
            field_value = None
            if data_values:
                assets_map = data_values.get("_assets") if isinstance(data_values, dict) else None
                if isinstance(assets_map, dict) and code in assets_map:
                    field_value = assets_map[code].get(asset_idx)
                    if field_value is None and asset_idx != 0:
                        # Didn't find a value for this specific asset idx — leave blank
                        field_value = None
                else:
                    field_value = data_values.get(code)

            field = {
                "code": code,
                "label": _humanize_code(code),
                "section": section_code,
                "section_label": section_label,
                "section_order": section_order,
                "asset_idx": asset_idx,
                "type": type_info.get("type"),
                "required": bool(dp.get("required")),
                # condition_met: True or None → visible; False → hidden (conditional)
                "visible": dp.get("condition_met") is not False,
                "exists": bool(dp.get("exists")),
                "value": field_value,
                "validation_error": ve,
            }
            for extra in ("format", "options", "default", "decimal_places", "item"):
                if extra in type_info:
                    field[extra] = type_info[extra]
            results.append(field)
        return results

    fields: list[dict[str, Any]] = []
    section_summaries: list[dict[str, Any]] = []
    section_order = 0
    # Track how many "generic" sections we've seen for numbering
    _generic_count = 0

    def _compute_label(section_code: str, datapoints: list, suffix: str = "") -> str:
        """Compute a human label; number generic ones."""
        nonlocal _generic_count
        label = _infer_section_label(section_code, datapoints)
        if label == "Section":
            _generic_count += 1
            label = f"Section {_generic_count}"
        return f"{label}{suffix}"

    # 1. insured_details_section (always comes first)
    insured = raw.get("insured_details_section")
    if isinstance(insured, dict):
        code = insured.get("code") or "insured"
        dps = insured.get("datapoints", [])
        # This is ALWAYS the insured entity — the label should always be
        # "Insured" regardless of what the code looks like ("insured.details",
        # "Insured", or whatever else Joshu might send).
        label = "Insured"
        sec_error = _validation_error_from(insured.get("section_validation_issue"))
        sec_fields = _process_datapoints(dps, code, label, section_order)
        fields.extend(sec_fields)
        section_summaries.append({
            "code": code,
            "label": label,
            "order": section_order,
            "field_count": len(sec_fields),
            "completed": sum(1 for f in sec_fields if f["exists"]),
            "visible_count": sum(1 for f in sec_fields if f["visible"]),
            "has_errors": bool(sec_error) or any(f["validation_error"] for f in sec_fields),
            "is_asset": bool(insured.get("is_asset")),
            "condition_met": insured.get("condition_met") is not False,
            "section_error": sec_error,
        })
        if sec_error:
            has_errors = True
        section_order += 1

    # 2. sections array (application data sections — where most fields live)
    sections = raw.get("sections")
    if isinstance(sections, list):
        for sec in sections:
            if not isinstance(sec, dict):
                continue
            code = sec.get("code") or f"section_{section_order}"
            dps = sec.get("datapoints", [])

            # Skip underwriter-only sections (Perils Scoring Addresses).
            # These will be derived automatically from Structures on save,
            # and should never appear in the broker's submission flow.
            if _is_underwriter_section(code, dps):
                continue

            label = _compute_label(code, dps)
            sec_error = _validation_error_from(sec.get("section_validation_issue"))
            sec_fields = _process_datapoints(dps, code, label, section_order)
            fields.extend(sec_fields)
            section_summaries.append({
                "code": code,
                "label": label,
                "order": section_order,
                "field_count": len(sec_fields),
                "completed": sum(1 for f in sec_fields if f["exists"]),
                "visible_count": sum(1 for f in sec_fields if f["visible"]),
                "has_errors": bool(sec_error) or any(f["validation_error"] for f in sec_fields),
                "is_asset": bool(sec.get("is_asset")),
                "condition_met": sec.get("condition_met") is not False,
                "section_error": sec_error,
            })
            if sec_error:
                has_errors = True
            section_order += 1

    # 3. bind_sections (DELIBERATELY SKIPPED in the broker flow)
    # Bind questions are collected only when a broker wants to formally bind
    # a published quote, not during initial submission. Keeping them out of
    # the editing form reduces friction for brokers during the application
    # stage. When we build the bind workflow, these will surface as a
    # dedicated screen separate from the main form.
    # (The raw bind_sections data remains intact on Joshu's side; we just
    # don't render it here.)

    # 4. Fallback: if nothing was found via the known structure, look for a
    # top-level `datapoints` array (older response shape or unknown variant)
    if not fields:
        top_dps = raw.get("datapoints")
        if isinstance(top_dps, list):
            fallback_fields = _process_datapoints(top_dps, "other", "Other", 0)
            fields.extend(fallback_fields)
            section_summaries.append({
                "code": "other", "label": "Other", "order": 0,
                "field_count": len(fallback_fields),
                "completed": sum(1 for f in fallback_fields if f["exists"]),
                "visible_count": sum(1 for f in fallback_fields if f["visible"]),
                "has_errors": any(f["validation_error"] for f in fallback_fields),
                "is_asset": False, "condition_met": True, "section_error": None,
            })

    # POST-PASS 1 — disambiguate duplicate section labels.
    #
    # A single Joshu product often splits its "Application" into multiple
    # sub-sections with random codes like "app.qV1fzIWW", "app.Fn5RjSZ5".
    # Our infer_section_label can't distinguish them, so they all get
    # "Application". Number the duplicates: "Application", "Application 2",
    # "Application 3", etc. — preserves readability without inventing
    # labels we can't justify.
    from collections import Counter as _Counter
    label_counts = _Counter(s["label"] for s in section_summaries)
    # Only labels that appear more than once need numbering
    duplicated = {lab for lab, n in label_counts.items() if n > 1}
    if duplicated:
        per_label_seen: dict[str, int] = {}
        for sec in section_summaries:
            lab = sec["label"]
            if lab in duplicated:
                per_label_seen[lab] = per_label_seen.get(lab, 0) + 1
                n = per_label_seen[lab]
                new_label = lab if n == 1 else f"{lab} {n}"
                sec["label"] = new_label
                # Also propagate to the fields that reference this section
                for f in fields:
                    if f.get("section") == sec["code"]:
                        f["section_label"] = new_label

    # POST-PASS 1.5 — merge "Insured" + first non-asset Application section.
    #
    # The broker views these as one logical step ("application"), so showing
    # them as separate left-rail items adds friction. We merge:
    #   - Insured section (named insured, address, business structure)
    #   - The FIRST non-asset section after Insured (typically the small
    #     opening Application section with effective date + status flags)
    # into a single section labeled "Application" that owns the union of
    # both fields. The other Application 2/3/4 sections stay separate.
    #
    # The merged section keeps the Insured section's `code` so backend
    # save flows continue to work — Insured's code is the more "anchored"
    # of the two (the Application section codes are random IDs like
    # `app.qV1fzIWW`).
    if len(section_summaries) >= 2:
        first = section_summaries[0]
        second = section_summaries[1]
        # Only merge if BOTH are non-asset and non-empty-shell.
        # We don't want to merge into a Structures section by accident.
        if (not first.get("is_asset") and not second.get("is_asset")
                and first.get("label") == "Insured"):
            # Re-tag every field that belonged to `second` so it now reports
            # under `first`'s code/label. The fields keep their original
            # `code` (e.g. "app.effective_date") — only the section pointer
            # changes.
            insured_code = first["code"]
            absorbed_code = second["code"]
            for f in fields:
                if f.get("section") == absorbed_code:
                    f["section"] = insured_code
                    f["section_label"] = "Application"

            # Update the merged section summary
            first["label"] = "Application"
            first["field_count"] = first["field_count"] + second["field_count"]
            first["completed"] = first["completed"] + second["completed"]
            first["visible_count"] = first["visible_count"] + second["visible_count"]
            first["has_errors"] = first["has_errors"] or second["has_errors"]
            # Section error: prefer the absorbed one's error text if first didn't have any
            if not first.get("section_error") and second.get("section_error"):
                first["section_error"] = second["section_error"]

            # Drop the second section summary (its fields are now under `first`)
            section_summaries = [first] + section_summaries[2:]
            # Re-number sections for the rail (keeps consistent ordering)
            for new_order, sec in enumerate(section_summaries):
                sec["order"] = new_order


    # Rules:
    #  - `condition_met: false` + 0 visible fields → hide (the section
    #    is gated by a condition that isn't satisfied, so there's nothing
    #    to show to the user).
    #  - `is_asset: true` + 0 populated values across all asset_idx groups
    #    → keep in the list but tell the UI it's "empty-shell" via the
    #    `is_empty_asset_shell` flag so it renders an empty-state card
    #    instead of N repeated empty schema fields.
    kept_sections: list[dict[str, Any]] = []
    for sec in section_summaries:
        # Count fields in this section that have actual values
        sec_fields = [f for f in fields if f.get("section") == sec["code"]]
        populated = sum(1 for f in sec_fields
                        if f.get("value") is not None
                        and f.get("value") != ""
                        and f.get("value") != {"Plain": {"Null": {}}})
        sec["populated_count"] = populated

        # Flag 1: hidden-by-condition
        if not sec.get("condition_met", True) and sec.get("visible_count", 0) == 0:
            # Drop the fields AND the summary
            fields[:] = [f for f in fields if f.get("section") != sec["code"]]
            continue

        # Flag 2: empty asset shell (is_asset + no populated values)
        if sec.get("is_asset") and populated == 0:
            sec["is_empty_asset_shell"] = True
            # Drop the fields themselves so the frontend doesn't render
            # 188 empty schema rows. The summary stays in the list so the
            # broker sees the section title + empty-state message.
            fields[:] = [f for f in fields if f.get("section") != sec["code"]]

        kept_sections.append(sec)
    section_summaries = kept_sections

    # POST-PASS 2.5 — drop empty non-asset sections, then apply friendly
    # broker-facing names to the remaining sections.
    #
    # When all fields in a section have been filtered out (e.g. former
    # Application 4 — every field is in _BROKER_HIDDEN_FIELD_CODES), the
    # section ends up with 0 fields but POST-PASS 2 above only drops it
    # when condition_met is False. So we drop here on `field_count == 0`
    # for non-asset sections.
    section_summaries = [
        s for s in section_summaries
        if s.get("is_asset") or s.get("field_count", 0) > 0
    ]

    # Rename the surviving non-asset sections to broker-friendly labels.
    # The order of non-asset sections (after the Insured+Application merge
    # collapsed the first two) is now stable: Basic Information,
    # Limits and Coverages, Deductibles. Asset sections (Structures) keep
    # whatever label they already have.
    NON_ASSET_FRIENDLY_LABELS = [
        "Basic Information",
        "Limits and Coverages",
        "Deductibles",
    ]
    non_asset_idx = 0
    for sec in section_summaries:
        if sec.get("is_asset"):
            continue
        if non_asset_idx < len(NON_ASSET_FRIENDLY_LABELS):
            new_label = NON_ASSET_FRIENDLY_LABELS[non_asset_idx]
            sec["label"] = new_label
            # Propagate to fields' section_label too — left rail and
            # any per-field display reads from this.
            for f in fields:
                if f.get("section") == sec["code"]:
                    f["section_label"] = new_label
            non_asset_idx += 1
        # If there are MORE non-asset sections than friendly labels (e.g.
        # Joshu adds a 5th section we didn't anticipate), we leave its
        # existing auto-numbered label alone rather than running off the
        # end of NON_ASSET_FRIENDLY_LABELS.

    # Re-assign section order numbers so the rail/progress shows them
    # correctly post-rename and post-drop.
    for new_order, sec in enumerate(section_summaries):
        sec["order"] = new_order

    counters = raw.get("counters") or {}

    return {
        "fields": fields,
        "sections": section_summaries,
        "counters": counters,
        "has_errors": has_errors,
    }


class HttpJoshuClient(JoshuClientBase):
    """Real HTTP client for altruis.joshu.insure — reads enabled, writes dormant."""

    API_PREFIX = "/api/insurance/v3"

    def __init__(self):
        if not settings.joshu_base_url:
            raise RuntimeError("JOSHU_BASE_URL is required for HttpJoshuClient")

        if settings.is_production and not settings.allow_production:
            raise RuntimeError(
                "Refusing to instantiate HttpJoshuClient against production "
                "without ALTRUIS_ALLOW_PRODUCTION override."
            )

        # Joshu's actual filter mechanism is `?test=true` (or false) on list
        # endpoints, NOT `?container=Test` as we previously assumed. The
        # container query parameter does not exist in the Joshu API and was
        # being silently ignored, which caused list endpoints to return
        # ALL records — including production ones. (The schema confirms
        # `test boolean | null` as a query parameter on /submissions and
        # the same pattern on /quotes and /policies.)
        #
        # In test mode we send `test=true` to filter list responses to test
        # records only. In production mode we send `test=false` (only when
        # the override token is set; the constructor refuses otherwise).
        self._test_filter: bool = {
            "test": True,
            "production": False,
        }.get(settings.joshu_environment, True)

        # Human-readable label for logging and the /api/diagnostics endpoint.
        self._mode_label: str = "Test" if self._test_filter else "Production"

        self.base_url = settings.joshu_base_url.rstrip("/")
        self.api_token = settings.joshu_api_token

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(30.0, connect=5.0),
        )

        enabled_writes = [
            name for name, flag in [
                ("update_submission_data", _ENABLE_UPDATE_SUBMISSION_DATA),
                ("update_submission", _ENABLE_UPDATE_SUBMISSION),
                ("create_policy", _ENABLE_CREATE_POLICY),
                ("create_transaction", _ENABLE_CREATE_TRANSACTION),
                ("update_quote", _ENABLE_UPDATE_QUOTE),
            ] if flag
        ]
        log.info(
            "HttpJoshuClient initialized · base_url=%s · mode=%s · test_filter=%s · enabled_writes=%s",
            self.base_url, self._mode_label, self._test_filter, enabled_writes or "none",
        )

    # Backwards-compatibility shim — older code paths read self._container.
    # New code should use self._mode_label or self._test_filter.
    @property
    def _container(self) -> str:
        return self._mode_label

    # ------------------------------------------------------------------
    # Core request helpers — ALL traffic flows through these
    # ------------------------------------------------------------------

    def _headers(self, bearer_token: str | None = None, *, with_body: bool = False) -> dict[str, str]:
        """Build auth headers.

        Joshu accepts either:
          - Authorization: Bearer <token>   (from email/password login)
          - Authorization: Token <api_key>  (from pre-generated API token)

        Content-Type is only set when ``with_body=True``. GET requests
        have no body, and sending ``Content-Type: application/json`` on
        a GET causes Joshu's JSON parser to attempt to parse the empty
        body and return a 400 "EOF while parsing a value at line 1
        column 0" error.

        If a real bearer_token is passed (future broker login), prefer it.
        If the caller passes the API_TOKEN_SENTINEL, or None, or an empty
        string, fall back to the statically-configured API token.
        """
        from app.session import API_TOKEN_SENTINEL

        headers: dict[str, str] = {"Accept": "application/json"}
        if with_body:
            headers["Content-Type"] = "application/json"

        # Use the bearer token only if it's a real token (not our sentinel)
        real_bearer = (
            bearer_token
            if bearer_token and bearer_token != API_TOKEN_SENTINEL
            else None
        )
        if real_bearer:
            headers["Authorization"] = f"Bearer {real_bearer}"
        elif self.api_token:
            headers["Authorization"] = f"Token {self.api_token}"
        return headers

    def _build_params(self, extra: dict[str, Any] | None = None,
                      *, list_endpoint: bool = True) -> dict[str, Any]:
        """Inject the ``container`` filter parameter — SAFETY LINCHPIN.

        Joshu's list endpoints filter by `container=Test` or
        `container=Production`. This was confirmed by intercepting the
        network request from Joshu's own UI:

            GET /api/insurance/v3/policies?status=...&container=Test&_page=1

        The published API spec lists `test boolean | null` as a query
        parameter, but in practice that parameter is silently ignored
        on this account — the actual filter mechanism is `container`.
        We had this right originally and incorrectly "fixed" it to
        `test=true` after misreading the docs.

        The container value is fixed at construction time based on the
        startup environment. Callers cannot override it. Any caller
        attempt to set `test` or `container` is logged and ignored.
        """
        params: dict[str, Any] = {}
        if extra:
            for k, v in extra.items():
                key_low = k.lower()
                if key_low in ("test", "container"):
                    log.error(
                        "SAFETY: Caller attempted to set %r parameter. Ignored. "
                        "caller_value=%r enforced_container=%s",
                        k, v, self._mode_label,
                    )
                    continue
                if v is not None:
                    params[k] = v
        if list_endpoint:
            # Set last so it cannot be stomped by an earlier caller value.
            # Capitalized form ("Test"/"Production") matches what Joshu's UI
            # itself sends on every list call.
            params["container"] = self._mode_label
        return params

    async def _get(
        self, path: str, *, params: dict[str, Any] | None = None,
        bearer_token: str | None = None, list_endpoint: bool = True,
    ) -> Any:
        """Single choke point for all JSON reads.

        list_endpoint=True (default) → adds the `test` query filter so
            list responses are scoped to test or production records as
            appropriate for the environment.
        list_endpoint=False → skips the `test` parameter (single-record
            GETs like /submissions/{id} don't accept it). The defensive
            check at the calling layer verifies the returned record's
            own `test` field matches our expected mode.

        Defensive check: when list_endpoint=True, verify every returned
        item's `test` field matches our expected mode. If Joshu's API
        ignores or mishandles our filter param, we'd otherwise leak
        production records into a test-mode portal. The check logs
        loudly and filters offending records out of the response so
        the UI never shows them.
        """
        full_params = self._build_params(params, list_endpoint=list_endpoint)
        url = f"{self.API_PREFIX}{path}"
        log.debug("GET %s params=%s", url, full_params)
        resp = await self._client.get(
            url, params=full_params, headers=self._headers(bearer_token),
        )
        self._raise_for_status(resp, "GET", url)
        data = resp.json()
        if list_endpoint:
            data = self._filter_list_response_to_expected_mode(path, data)
        return data

    def _filter_list_response_to_expected_mode(self, path: str, data: Any) -> Any:
        """Strip records whose `test` field doesn't match our mode.

        Belt-and-suspenders defense layered on top of the `container`
        query filter. Joshu's list endpoints return records WITHOUT the
        `test` field populated (it's always null on items in list
        responses), so this layer is normally a no-op — we trust the
        `container=Test` filter on the request side.

        If a future Joshu API version starts surfacing a `test` field on
        list items AND a record's value disagrees with our mode, this
        will catch it and log loudly.

        Returns the same shape (paginated dict or bare list), with
        offending items removed and an audit log emitted.
        """
        expected_test = self._test_filter
        items = None
        if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
            items = data["items"]
        elif isinstance(data, list):
            items = data
        else:
            return data  # not a list response shape we recognize

        kept: list[Any] = []
        dropped: list[Any] = []
        for it in items:
            if not isinstance(it, dict):
                kept.append(it)
                continue
            test_val = it.get("test")
            # Joshu doesn't populate the `test` field on list items —
            # it's always null. We trust the `container` query filter
            # for these. Only drop records when the field IS present
            # AND disagrees with our mode (a future-proof guard).
            if test_val is None:
                kept.append(it)
                continue
            actual_is_test = test_val if isinstance(test_val, bool) else (
                str(test_val).strip().lower() in ("true", "1", "yes")
            )
            if actual_is_test == expected_test:
                kept.append(it)
            else:
                dropped.append(it)

        if dropped:
            log.error(
                "SAFETY: %d record(s) with mismatched `test` field returned by %s "
                "(expected test=%s, container=%s). Records were dropped before "
                "reaching the UI. This indicates Joshu's container filter is not "
                "working as expected. Sample dropped IDs: %s",
                len(dropped), path, expected_test, self._mode_label,
                [r.get("id") or r.get("unique_id") for r in dropped[:5]],
            )
            if isinstance(data, dict) and "items" in data:
                data = {**data, "items": kept}
                if "total_items" in data and isinstance(data["total_items"], int):
                    data["total_items"] = max(0, data["total_items"] - len(dropped))
            else:
                data = kept

        return data

    async def _get_raw(
        self, path: str, *, params: dict[str, Any] | None = None,
        bearer_token: str | None = None, list_endpoint: bool = True,
    ) -> tuple[bytes, str]:
        """Binary read (for document downloads)."""
        full_params = self._build_params(params, list_endpoint=list_endpoint)
        url = f"{self.API_PREFIX}{path}"
        resp = await self._client.get(
            url, params=full_params, headers=self._headers(bearer_token),
        )
        self._raise_for_status(resp, "GET", url)
        return resp.content, resp.headers.get("content-type", "application/octet-stream")

    async def _put(
        self, path: str, *, body: Any, params: dict[str, Any] | None = None,
        bearer_token: str | None = None, list_endpoint: bool = False,
    ) -> Any:
        """Single choke point for PUT requests.

        Defaults to list_endpoint=False since writes always target a
        specific record by ID. The `_assert_test_mode_for_write` guard
        enforces test mode at the environment layer, and callers SHOULD
        verify the target record's `test` field matches before writing.
        """
        self._assert_test_mode_for_write()
        full_params = self._build_params(params, list_endpoint=list_endpoint)
        url = f"{self.API_PREFIX}{path}"
        log.info("PUT %s params=%s", url, full_params)  # info, not debug — writes matter
        resp = await self._client.put(
            url, params=full_params,
            headers=self._headers(bearer_token, with_body=True),
            json=body,
        )
        self._raise_for_status(resp, "PUT", url)
        if resp.content:
            try:
                return resp.json()
            except Exception:
                return {"raw": resp.text}
        return {}

    async def _post(
        self, path: str, *, body: Any = None, params: dict[str, Any] | None = None,
        bearer_token: str | None = None, list_endpoint: bool = False,
    ) -> Any:
        """Single choke point for POST requests."""
        self._assert_test_mode_for_write()
        full_params = self._build_params(params, list_endpoint=list_endpoint)
        url = f"{self.API_PREFIX}{path}"
        log.info("POST %s params=%s", url, full_params)
        resp = await self._client.post(
            url, params=full_params,
            headers=self._headers(bearer_token, with_body=(body is not None)),
            json=body if body is not None else None,
        )
        self._raise_for_status(resp, "POST", url)
        if resp.content:
            try:
                return resp.json()
            except Exception:
                return {"raw": resp.text}
        return {}

    def _raise_for_status(self, resp: httpx.Response, method: str, url: str) -> None:
        """Translate HTTP errors into FastAPI HTTPExceptions."""
        if resp.is_success:
            return
        body_preview = resp.text[:500] if resp.text else ""
        log.warning("Joshu API error · %s %s · status=%d · body=%s",
                    method, url, resp.status_code, body_preview)
        from fastapi import HTTPException
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Joshu API returned {resp.status_code}: {body_preview[:200]}",
        )

    # ------------------------------------------------------------------
    # Auth / user
    # ------------------------------------------------------------------

    async def login(self, email: str, password: str) -> tuple[str, BrokerUser]:
        # The Joshu API v3 reference doesn't document a password-auth
        # endpoint in the sections reviewed. Email/password login is
        # referenced but not detailed — likely a separate auth subsystem.
        # Until we know the login URL, the portal uses a single shared API
        # token; there is no per-broker login against Joshu.
        raise _not_ready("login (Joshu password-auth endpoint not yet documented)")

    async def whoami(self, token: str) -> BrokerUser:
        # Stub — Joshu docs don't expose /me in the sampled sections.
        # When we learn how to fetch the user attached to a token, implement
        # here. For now return a synthetic user so the UI renders.
        return BrokerUser(
            id=0, email="api-token-user@altruis", name="API Token User",
            store_id=None, store_name="Altruis Group",
            role="Portal (via API key)",
        )

    # ------------------------------------------------------------------
    # Products
    # ------------------------------------------------------------------

    async def list_products(self, token: str) -> list[Product]:
        data = await self._get("/products", bearer_token=token)
        items = data.get("items", data) if isinstance(data, dict) else data
        return [Product.model_validate(p) for p in (items or [])]

    async def get_product(self, token: str, product_id: int) -> Product:
        data = await self._get(f"/products/{product_id}", bearer_token=token, list_endpoint=False)
        return Product.model_validate(data)

    # ------------------------------------------------------------------
    # Submissions
    # ------------------------------------------------------------------

    async def list_submissions(
        self, token: str, *, user_id=None, store_id=None, status=None, flow=None,
        page=1, per_page=25,
    ) -> Paginated:
        params: dict[str, Any] = {"_page": page, "_per_page": per_page}
        if user_id is not None: params["user_id"] = user_id
        if store_id is not None: params["store_id"] = store_id
        if status: params["status"] = status
        if flow: params["flow"] = flow
        data = await self._get("/submissions", params=params, bearer_token=token)
        return Paginated.model_validate(data)

    async def get_submission(self, token, submission_id: str | int) -> Submission:
        # Joshu path params use unique_id (UUID), not the numeric id.
        # The numeric id is for display only. Accept either here and pass
        # through — the routers will ensure UUIDs are used when known.
        data = await self._get(f"/submissions/{submission_id}", bearer_token=token, list_endpoint=False)
        return Submission.model_validate(data)

    async def get_submission_data(self, token, submission_id: str | int) -> dict[str, Any]:
        """Fetch datapoint values for a submission.

        Per Joshu docs: GET /submission-data/{id} returns an Array of
        {code, value} objects, where value is a discriminated union
        (Boolean/Text/Number/Monetary/Date/Location/etc.). We flatten
        this into a simple {code: simplified_value} dict for display,
        preserving the original shape under `_raw` for downstream use.
        """
        try:
            raw = await self._get(f"/submission-data/{submission_id}", bearer_token=token, list_endpoint=False)
        except Exception as e:
            log.warning("submission-data fetch failed for %s: %s", submission_id, e)
            return {}
        return _flatten_code_value_array(raw)

    async def get_submission_status(self, token, submission_id: str | int) -> dict[str, Any]:
        """Fetch submission schema + validation state via /submission-status/{id}.

        Returns Joshu's raw response. Normalization into a flat field list
        happens in the router (which also merges in the current data values).
        """
        try:
            return await self._get(f"/submission-status/{submission_id}", bearer_token=token, list_endpoint=False)
        except Exception as e:
            log.warning("submission-status fetch failed for %s: %s", submission_id, e)
            return {}

    async def get_asset_data(self, token, submission_id: str | int) -> Any:
        """Fetch asset-level data via GET /asset-data/{id}.

        Per Joshu v3 docs, /submission-data/{id} returns ONLY the root-level
        datapoints — things like `insured.name`, `app.aop_deductible`. For any
        code that's an asset collection (like `app.structures`), the root-level
        data returns just `Null` as a placeholder. The actual per-asset
        records live under /asset-data/{id}, which returns an Array where
        each element has:
          - code: the asset collection code ("app.structures")
          - value: a V1-wrapped Array of JoPlainValueV1 (each one is a
            record for that asset-idx)

        Joshu quietly has two separate data endpoints: `submission-data`
        for scalars, `asset-data` for assets. A fact I only learned after
        two days of debugging empty structure cards.

        Returns the raw response (likely a list) for the caller to merge.
        Returns None on failure so the caller can gracefully render scalars
        only if asset-data is unreachable.
        """
        try:
            return await self._get(f"/asset-data/{submission_id}", bearer_token=token, list_endpoint=False)
        except Exception as e:
            log.warning("asset-data fetch failed for %s: %s", submission_id, e)
            return None

    async def update_submission_data(self, token, submission_id, data, *, type_hints=None):
        """Save submission data via PUT /submission-data/{id}.

        Body shape expected by Joshu::
          {"data": [{"code": "insured.name", "value": {"V1": {"Text": "..."}}}]}

        The ``data`` argument is a flat {code: value} dict from the frontend.
        ``type_hints`` lets the caller (router) pre-map codes to Joshu type tags,
        which we use when the Python value doesn't uniquely determine the tag
        (e.g. a string could be Text or it could be a Location).
        """
        if not _ENABLE_UPDATE_SUBMISSION_DATA:
            raise _not_ready("update_submission_data")
        self._assert_test_mode_for_write()
        await self._verify_record_matches_mode(
            token, "/submissions", submission_id, "submission"
        )

        body = _encode_data_payload(data, type_hints=type_hints)
        log.info("Updating submission %s with %d datapoints", submission_id, len(body["data"]))
        resp = await self._put(
            f"/submission-data/{submission_id}",
            body=body, bearer_token=token,
        )
        # Re-fetch the merged data so the caller sees the post-save state
        merged = await self.get_submission_data(token, submission_id)
        return merged

    async def submit_submission(self, token, submission_id) -> Submission:
        """Move Incomplete → Submitted via PUT /submissions/{id}."""
        if not _ENABLE_UPDATE_SUBMISSION:
            raise _not_ready("submit_submission")
        self._assert_test_mode_for_write()
        await self._verify_record_matches_mode(
            token, "/submissions", submission_id, "submission"
        )

        log.info("Submitting submission %s (status → Submitted)", submission_id)
        await self._put(
            f"/submissions/{submission_id}",
            body={"status": "Submitted"},
            bearer_token=token,
        )
        # Re-fetch to return the updated submission record
        return await self.get_submission(token, submission_id)

    async def reopen_submission(self, token, submission_id) -> Submission:
        """Move Submitted/Pending → Incomplete via PUT /submissions/{id}.

        Enables the broker's "Edit & Resubmit" workflow.
        """
        if not _ENABLE_UPDATE_SUBMISSION:
            raise _not_ready("reopen_submission")
        self._assert_test_mode_for_write()
        await self._verify_record_matches_mode(
            token, "/submissions", submission_id, "submission"
        )

        log.info("Reopening submission %s (status → Incomplete)", submission_id)
        await self._put(
            f"/submissions/{submission_id}",
            body={"status": "Incomplete"},
            bearer_token=token,
        )
        return await self.get_submission(token, submission_id)

    # ------------------------------------------------------------------
    # Policies
    # ------------------------------------------------------------------

    async def create_policy(self, token: str) -> Policy:
        self._assert_test_mode_for_write()
        raise _not_ready("create_policy")

    async def list_policies(
        self, token, *, status=None, page=1, per_page=25,
    ) -> Paginated:
        params: dict[str, Any] = {"_page": page, "_per_page": per_page}
        if status: params["status"] = status
        data = await self._get("/policies", params=params, bearer_token=token)
        return Paginated.model_validate(data)

    async def get_policy(self, token, policy_id: str) -> Policy:
        data = await self._get(f"/policies/{policy_id}", bearer_token=token, list_endpoint=False)
        return Policy.model_validate(data)

    # ------------------------------------------------------------------
    # Policy discovery cache
    # ------------------------------------------------------------------
    #
    # The dashboard calls /api/submissions, /api/quotes, and /api/documents
    # in parallel on every load. With our policy-driven discovery, each
    # of these would re-fetch the full policy list and re-fan-out to
    # policy details — same work, three times. We cache the (policies,
    # submission-id-map) result for a short TTL keyed by token + page +
    # per_page so all three callers within one page-load share it.
    #
    # The cache is process-local and TTL-based. No invalidation on
    # writes — but writes don't happen in this read flow, and a 30s TTL
    # is short enough that any stale data from a future write path
    # would refresh quickly.
    _POLICY_DISCOVERY_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
    _POLICY_DISCOVERY_TTL_SEC = 30

    async def _get_policy_discovery(
        self, token: str, *, page: int, per_page: int,
    ) -> dict[str, Any]:
        """Fetch (and cache) the policy discovery result.

        Returns:
          {
            "policies":   list of policy summary dicts (from /policies),
            "details":    list of policy detail dicts (from /policies/{id}),
            "policies_total": total_items from the list response,
          }
        """
        import time
        cache_key = f"{token[:32]}::{page}::{per_page}"
        now = time.monotonic()
        cached = self._POLICY_DISCOVERY_CACHE.get(cache_key)
        if cached and (now - cached[0]) < self._POLICY_DISCOVERY_TTL_SEC:
            return cached[1]

        # Cache miss — do the real work.
        policies_resp = await self.list_policies(token, page=page, per_page=per_page)
        policies = policies_resp.items or []

        async def _fetch_detail(p):
            pid = p.get("id") if isinstance(p, dict) else getattr(p, "id", None)
            if not pid:
                return None
            try:
                return await self._get(
                    f"/policies/{pid}", bearer_token=token, list_endpoint=False,
                )
            except Exception as e:
                log.warning("policy detail fetch failed for %s: %s", pid, e)
                return None

        details = await asyncio.gather(*(_fetch_detail(p) for p in policies))

        result = {
            "policies": policies,
            "details": [d for d in details if d],
            "policies_total": policies_resp.total_items,
        }
        self._POLICY_DISCOVERY_CACHE[cache_key] = (now, result)

        # Opportunistic eviction of expired entries to avoid unbounded
        # growth. Cheap because entries are few.
        for k, (ts, _) in list(self._POLICY_DISCOVERY_CACHE.items()):
            if (now - ts) >= self._POLICY_DISCOVERY_TTL_SEC:
                self._POLICY_DISCOVERY_CACHE.pop(k, None)

        return result

    # ------------------------------------------------------------------
    # Policy-driven submission discovery
    # ------------------------------------------------------------------
    #
    # The /submissions list endpoint does not honor `container=Test` for
    # our API token — it returns ALL submissions (test + production
    # mixed). The /policies list endpoint DOES honor it, which is the
    # discovery flow Joshu's own UI uses.
    #
    # So we discover submissions through policies:
    #   1. List /policies?container=Test (filtered correctly)
    #   2. For each policy, fetch its detail to get
    #      `ongoing_change_submission_id` (the in-flight submission ID)
    #   3. For policies with no in-flight change, fall back to the
    #      latest transaction's `latest_submission_id`
    #   4. Fetch each submission record by ID (single-record endpoints
    #      don't need filtering — IDs are unique system-wide)
    #
    # This is N+1 in shape but parallel in execution. With 50 policies
    # the total wall time is ~2 seconds. We cache results for 60 seconds
    # so subsequent dashboard loads are instant.

    async def discover_test_submissions(
        self, token: str, *,
        page: int = 1, per_page: int = 25,
        status_filter: str | None = None,
        flow_filter: str | None = None,
    ) -> dict[str, Any]:
        """Return a paginated, container-filtered list of submission rows.

        Each row is a dict shaped like the /submissions list response
        (id, status, flow, policy_id, modified_at, etc.) PLUS an
        `insured_name` field copied from the parent policy.

        The pagination contract matches the existing /submissions
        endpoint shape: {items, total_items, page, per_page, total_pages}.
        Pagination is applied to the FILTERED list of policies, not to
        the raw policy count, so per_page/page produce the right slice.
        """
        # Step 1+2: list policies + fetch each one's detail (cached).
        policies_per_page = max(per_page * 2, 50)
        discovery = await self._get_policy_discovery(
            token, page=page, per_page=policies_per_page,
        )
        details = discovery["details"]

        # Step 3: derive submission IDs from policy details.
        # Primary: ongoing_change_submission_id (active in-flight submission)
        # Fallback: list transactions for the policy and pick the most
        # recent one's latest_submission_id (catches bound policies with
        # no current change).
        sub_id_to_policy: dict[int, dict[str, Any]] = {}
        policies_needing_txn_lookup: list[dict[str, Any]] = []

        for d in details:
            sub_id = d.get("ongoing_change_submission_id")
            insured_name = d.get("insured_name")
            policy_id = d.get("id")
            if sub_id is not None:
                sub_id_to_policy[int(sub_id)] = {
                    "policy_id": policy_id,
                    "insured_name": insured_name,
                    "policy_status": d.get("status"),
                    "ongoing_change": d.get("ongoing_change"),
                }
            else:
                policies_needing_txn_lookup.append(d)

        # Fallback step: for policies with no in-flight submission,
        # look up their transactions to find the most recent submission.
        # Only do this for the handful that need it (~10% of policies).
        if policies_needing_txn_lookup:
            async def _fetch_latest_txn_sub_id(policy_d):
                pid = policy_d.get("id")
                try:
                    txn_data = await self._get(
                        "/transactions",
                        params={"policy_id": pid, "_per_page": 5},
                        bearer_token=token,
                    )
                except Exception:
                    return None
                items = (txn_data.get("items") if isinstance(txn_data, dict) else None) or []
                # Pick the latest transaction (sorted by modified_at desc; fall
                # back to first if missing). Each transaction has latest_submission_id.
                latest = None
                for t in items:
                    if not isinstance(t, dict):
                        continue
                    if t.get("latest_submission_id") is None:
                        continue
                    if latest is None or (t.get("modified_at") or "") > (latest.get("modified_at") or ""):
                        latest = t
                if latest:
                    return (policy_d, int(latest["latest_submission_id"]))
                return None

            txn_results = await asyncio.gather(
                *(_fetch_latest_txn_sub_id(p) for p in policies_needing_txn_lookup)
            )
            for r in txn_results:
                if r is None: continue
                policy_d, sub_id = r
                sub_id_to_policy[sub_id] = {
                    "policy_id": policy_d.get("id"),
                    "insured_name": policy_d.get("insured_name"),
                    "policy_status": policy_d.get("status"),
                    "ongoing_change": policy_d.get("ongoing_change"),
                }

        # Step 4: fetch each submission by ID (in parallel) so we can
        # return real submission status/flow/dates rather than just IDs.
        async def _fetch_submission(sub_id: int):
            try:
                data = await self._get(
                    f"/submissions/{sub_id}",
                    bearer_token=token,
                    list_endpoint=False,
                )
                return data
            except Exception as e:
                log.warning("submission fetch failed for %s: %s", sub_id, e)
                return None

        submission_records = await asyncio.gather(
            *(_fetch_submission(sid) for sid in sub_id_to_policy.keys())
        )

        # Build enriched rows. Each row carries the standard submission
        # fields PLUS insured_name from the policy linkage.
        rows: list[dict[str, Any]] = []
        for sub in submission_records:
            if not isinstance(sub, dict):
                continue
            sid = sub.get("id")
            if sid is None:
                continue
            link = sub_id_to_policy.get(int(sid)) or {}
            row = dict(sub)  # copy
            if link.get("insured_name"):
                row["insured_name"] = link["insured_name"]
            rows.append(row)

        # Apply caller-side filters (status, flow). The Joshu policy
        # endpoint accepted some statuses but we want the broker-facing
        # submission status filter to apply here.
        if status_filter:
            rows = [r for r in rows if r.get("status") == status_filter]
        if flow_filter:
            rows = [r for r in rows if r.get("flow") == flow_filter]

        # Sort by modified_at descending (newest first). Submissions
        # with no modified_at sort last.
        rows.sort(key=lambda r: r.get("modified_at") or "", reverse=True)

        # Apply pagination — at this point `rows` is the full slice for
        # the current policy page. Cut down to per_page.
        total_items = discovery.get("policies_total") or len(rows)
        items_slice = rows[:per_page]

        return {
            "items": items_slice,
            "total_items": total_items,
            "page": page,
            "per_page": per_page,
            "total_pages": max(1, (total_items + per_page - 1) // per_page),
        }

    # ------------------------------------------------------------------
    # Transactions
    # ------------------------------------------------------------------

    async def create_transaction(self, token, **kwargs) -> Transaction:
        self._assert_test_mode_for_write()
        raise _not_ready("create_transaction")

    async def list_transactions(
        self, token, *, policy_id=None, page=1, per_page=25,
    ) -> Paginated:
        params: dict[str, Any] = {"_page": page, "_per_page": per_page}
        if policy_id: params["policy_id"] = policy_id
        data = await self._get("/transactions", params=params, bearer_token=token)
        return Paginated.model_validate(data)

    # ------------------------------------------------------------------
    # Quotes
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Policy-driven quote discovery
    # ------------------------------------------------------------------
    #
    # Same problem as submissions: /quotes list endpoint without a
    # submission_id parameter doesn't honor the container filter for
    # our token, so it leaks production quotes. The fix: rediscover
    # via policies (which are correctly filtered), then fan out to
    # /quotes?submission_id=X per policy.
    #
    # Joshu's UI uses this exact pattern — its initial page-load
    # network panel shows /policies?container=Test followed by N
    # parallel /quotes?submission_id=... calls.

    async def discover_test_quotes(
        self, token: str, *,
        page: int = 1, per_page: int = 25,
        status_filter: str | None = None,
    ) -> dict[str, Any]:
        """Return a paginated, container-filtered list of quote rows.

        Each row is shaped like Joshu's /quotes list response (id, status,
        submission_id, etc.) PLUS:
          • `insured_name` from the parent policy
          • `policy_id` from the parent policy

        The pagination contract matches existing list endpoints:
        {items, total_items, page, per_page, total_pages}.
        """
        # Step 1: discover policies (filtered) and their submission IDs
        # via the shared discovery cache (so the dashboard's parallel
        # /api/submissions, /api/quotes, /api/documents calls all share
        # the same upstream lookups).
        policies_per_page = max(per_page * 2, 50)
        discovery = await self._get_policy_discovery(
            token, page=page, per_page=policies_per_page,
        )
        details = discovery["details"]

        # Build {submission_id: policy_context} so we can attach insured
        # name to each quote later.
        sub_to_policy: dict[int, dict[str, Any]] = {}
        for d in details:
            sub_id = d.get("ongoing_change_submission_id")
            if sub_id is None:
                continue
            sub_to_policy[int(sub_id)] = {
                "policy_id": d.get("id"),
                "insured_name": d.get("insured_name"),
                "policy_status": d.get("status"),
            }

        # Step 2: for each submission, fetch quotes in parallel.
        # Per-submission /quotes calls don't need container filtering —
        # the submission_id is unique system-wide and only resolves to
        # quotes attached to it.
        async def _fetch_quotes_for_submission(sub_id: int):
            try:
                data = await self._get(
                    "/quotes",
                    params={"submission_id": sub_id, "_per_page": 50},
                    bearer_token=token,
                )
            except Exception as e:
                log.warning("quote list fetch failed for submission %s: %s", sub_id, e)
                return []
            items = (data.get("items") if isinstance(data, dict) else None) or []
            ctx = sub_to_policy.get(sub_id, {})
            for q in items:
                if not isinstance(q, dict):
                    continue
                if ctx.get("insured_name") and not q.get("insured_name"):
                    q["insured_name"] = ctx["insured_name"]
                if ctx.get("policy_id") and not q.get("policy_id"):
                    q["policy_id"] = ctx["policy_id"]
                # Ensure submission_id is present on the row even if
                # Joshu didn't echo it back (unlikely, but defensive).
                if not q.get("submission_id"):
                    q["submission_id"] = sub_id
            return items

        all_quotes_nested = await asyncio.gather(
            *(_fetch_quotes_for_submission(sid) for sid in sub_to_policy.keys())
        )
        all_quotes: list[dict[str, Any]] = []
        for batch in all_quotes_nested:
            all_quotes.extend(batch)

        # Filter by quote status if requested (e.g. only QuotePublished).
        if status_filter:
            all_quotes = [q for q in all_quotes if q.get("status") == status_filter]

        # Sort newest first, then paginate.
        all_quotes.sort(key=lambda q: q.get("modified_at") or q.get("created_at") or "", reverse=True)
        items_slice = all_quotes[:per_page]
        total_items = len(all_quotes)

        return {
            "items": items_slice,
            "total_items": total_items,
            "page": page,
            "per_page": per_page,
            "total_pages": max(1, (total_items + per_page - 1) // per_page),
        }

    async def list_quotes(
        self, token, *, submission_id=None, page=1, per_page=25,
    ) -> Paginated:
        params: dict[str, Any] = {"_page": page, "_per_page": per_page}
        if submission_id: params["submission_id"] = submission_id
        data = await self._get("/quotes", params=params, bearer_token=token)
        return Paginated.model_validate(data)

    async def get_quote(self, token, quote_id: str | int) -> Quote:
        data = await self._get(f"/quotes/{quote_id}", bearer_token=token, list_endpoint=False)
        return Quote.model_validate(data)

    async def get_quote_data(self, token, quote_id: str | int) -> dict[str, Any]:
        """Fetch datapoint values for a quote via /quote-data/{id}."""
        try:
            raw = await self._get(f"/quote-data/{quote_id}", bearer_token=token, list_endpoint=False)
        except Exception as e:
            log.warning("quote-data fetch failed for %s: %s", quote_id, e)
            return {}
        return _flatten_code_value_array(raw)

    async def update_quote_status(self, token, quote_id: int, status: str) -> Quote:
        self._assert_test_mode_for_write()
        raise _not_ready("update_quote_status")

    # ------------------------------------------------------------------
    # Documents
    # ------------------------------------------------------------------

    async def list_documents(
        self, token, *, quote_id=None, document_type=None, page=1, per_page=25,
    ) -> Paginated:
        params: dict[str, Any] = {"_page": page, "_per_page": per_page}
        if quote_id: params["quote_id"] = quote_id
        if document_type: params["document_type"] = document_type
        data = await self._get("/documents", params=params, bearer_token=token)
        return Paginated.model_validate(data)

    # ------------------------------------------------------------------
    # Policy-driven document discovery
    # ------------------------------------------------------------------
    #
    # Documents have no own container field — they're attached to a
    # quote, which is attached to a submission, which is attached to a
    # policy. So we discover by composing on top of discover_test_quotes:
    # find all test quotes, then fan out /documents?quote_id=X per quote.
    #
    # This is N+1 in shape (1 policy list + N policy details + M quote
    # lists + K document lists). For ~50 policies, that's roughly 150
    # parallel HTTP calls. Each is small and fast; total wall time is
    # ~3-4 seconds. Worth caching at the router level if this becomes
    # a hot path.

    async def discover_test_documents(
        self, token: str, *,
        page: int = 1, per_page: int = 25,
        document_type: str | None = None,
    ) -> dict[str, Any]:
        """Return a paginated, container-filtered list of document rows.

        Each row is shaped like Joshu's /documents list response, with
        these enrichments where available:
          • `insured_name` from the parent policy
          • `policy_id` from the parent policy
          • `submission_id` from the parent quote
        """
        # Step 1: find all test quotes (deeper per_page so we cover the book).
        quotes_payload = await self.discover_test_quotes(
            token, page=1, per_page=200,
        )
        test_quotes = quotes_payload.get("items") or []

        # Step 2: for each quote, fetch its documents in parallel.
        async def _docs_for_quote(q: dict[str, Any]):
            qid = q.get("id")
            if not qid:
                return []
            params: dict[str, Any] = {"quote_id": qid, "_per_page": 50}
            if document_type:
                params["document_type"] = document_type
            try:
                data = await self._get("/documents", params=params, bearer_token=token)
            except Exception as e:
                log.warning("documents fetch failed for quote %s: %s", qid, e)
                return []
            items = (data.get("items") if isinstance(data, dict) else None) or []
            for d in items:
                if not isinstance(d, dict):
                    continue
                # Enrich each document row with parent context. Joshu may
                # not echo all of these on the document record itself.
                if q.get("insured_name") and not d.get("insured_name"):
                    d["insured_name"] = q["insured_name"]
                if q.get("policy_id") and not d.get("policy_id"):
                    d["policy_id"] = q["policy_id"]
                if q.get("submission_id") and not d.get("submission_id"):
                    d["submission_id"] = q["submission_id"]
                if not d.get("quote_id"):
                    d["quote_id"] = qid
            return items

        all_docs_nested = await asyncio.gather(*(_docs_for_quote(q) for q in test_quotes))
        all_docs: list[dict[str, Any]] = []
        for batch in all_docs_nested:
            all_docs.extend(batch)

        # Sort newest first, paginate.
        all_docs.sort(key=lambda d: d.get("created_at") or d.get("modified_at") or "", reverse=True)
        items_slice = all_docs[(page - 1) * per_page : page * per_page]
        total_items = len(all_docs)

        return {
            "items": items_slice,
            "total_items": total_items,
            "page": page,
            "per_page": per_page,
            "total_pages": max(1, (total_items + per_page - 1) // per_page),
        }

    async def get_document(self, token, document_id: int) -> Document:
        data = await self._get(f"/documents/{document_id}", bearer_token=token, list_endpoint=False)
        return Document.model_validate(data)

    async def download_document(self, token, document_id: int) -> tuple[bytes, str]:
        # Joshu's API returns a file_id on the document record. The binary
        # is usually fetched via /documents/{id}/download or a related file
        # endpoint. We try the common pattern first; if Joshu uses a
        # different URL structure, you'll get a 404 and we'll adjust.
        return await self._get_raw(
            f"/documents/{document_id}/download", bearer_token=token,
            list_endpoint=False,
        )

    # ------------------------------------------------------------------
    # Safety check for writes (unused in read-only phase)
    # ------------------------------------------------------------------

    def _assert_test_mode_for_write(self) -> None:
        """Last-line-of-defense check before any mutating call.

        Runs BEFORE any request is built, so if production mode got
        accidentally flipped without the override, we fail closed.
        """
        if settings.is_production and not settings.allow_production:
            log.critical(
                "BLOCKED write attempt in production without ALTRUIS_ALLOW_PRODUCTION. "
                "This indicates a misconfiguration or bug."
            )
            raise RuntimeError(
                "BLOCKED: production write attempted without override flag. "
                "This is a safety stop — investigate before proceeding."
            )

    async def _verify_record_matches_mode(
        self,
        token: str,
        path: str,
        record_id: str | int,
        record_kind: str,
    ) -> None:
        """Defensive per-record check before writing.

        Fetches the target record and verifies its `test` field matches
        the environment's expected mode. Refuses the write if it doesn't.

        This guards against the scenario where a production record ID
        somehow surfaces in the UI (e.g. a stale link, manual URL edit,
        a bug in the list filter) and a write is attempted against it.
        Even with the environment locked to test, hitting a production
        record ID via the single-record endpoint would write to
        production. This check makes that impossible.
        """
        try:
            record = await self._get(
                f"{path}/{record_id}",
                bearer_token=token,
                list_endpoint=False,
            )
        except Exception as e:
            # If we can't fetch the record at all, fail closed — better
            # to refuse a write than to risk writing to the wrong place.
            log.error(
                "BLOCKED write to %s/%s — could not verify record's test mode: %s",
                path, record_id, e,
            )
            raise RuntimeError(
                f"Cannot verify {record_kind} {record_id} test/production status — "
                f"write blocked. Original error: {e}"
            )
        record_test = record.get("test")
        # Joshu sometimes returns null for the field. Treat null as
        # production (the conservative default). Only proceed if the
        # record's `test` value is truthy AND we're in test mode.
        record_is_test = bool(record_test)
        if record_is_test != self._test_filter:
            log.critical(
                "BLOCKED write to %s/%s — record.test=%r does not match "
                "environment expected_test=%s. This is a safety stop.",
                path, record_id, record_test, self._test_filter,
            )
            raise RuntimeError(
                f"BLOCKED: {record_kind} {record_id} has test={record_test!r}, "
                f"but the environment expects test={self._test_filter}. "
                f"Refusing to write across the test/production boundary."
            )

    async def aclose(self) -> None:
        """Close the underlying HTTPX client. Call on app shutdown."""
        await self._client.aclose()
