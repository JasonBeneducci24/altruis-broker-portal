"""
HTTP client for the Joshu API — READ-ONLY phase.

ARCHITECTURE AND SAFETY
=======================

This client talks to the real Joshu API at ``{JOSHU_BASE_URL}/api/insurance/v3/*``.
The test-vs-production container is selected by a query-string parameter:

    GET  /api/insurance/v3/submissions?container=Test
    GET  /api/insurance/v3/policies?container=Test&_page=1&status=Active

**SAFETY INVARIANT**: The ``container`` parameter is fixed at construction
time based on the startup environment (``JOSHU_ENVIRONMENT``). It CANNOT
be overridden per-call. Every outbound request goes through ``_get()``,
which injects ``container`` before the request leaves the process —
individual methods have no ability to change it.

Additional layered defenses:

  1. Env guard in ``config.py`` — app refuses to start with JOSHU_ENVIRONMENT
     unset or in production without the explicit override token.
  2. ``_assert_test_mode_for_write()`` — belt-and-suspenders check before
     any mutation (not used in this read-only phase, in place for when
     writes are enabled).
  3. ``_build_params()`` actively strips any attempt by a caller to set
     ``container`` in extra params and logs it loudly as a safety event.

PHASE
=====

This is the READ-ONLY phase. GET methods are active. Every mutating method
still raises ``HttpClientNotReadyError``. Once reads are verified against
the test container, writes get enabled in a second pass.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

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
    """Flatten Joshu's [{code, value}] datapoint response to {code: simple_value}.

    Joshu returns datapoint values as an array of objects like::

        [
          {"code": "insured.name", "value": {"V1": {"Text": "Acme LLC"}}},
          {"code": "insured.location", "value": {"V1": {"Location": {...}}}},
          {"code": "app.aop_deductible", "value": {"V1": {"Number": "5000"}}},
          {"code": "app.effective_date", "value": {"V1": {"Date": "2026-06-01"}}},
          {"code": "app.cyber_status", "value": {"V1": {"Boolean": true}}},
        ]

    The value is wrapped in a version tag ("V1") then a type discriminator.
    We extract the underlying simple value for display, while also preserving
    the raw response under `_raw` so callers who want to re-submit the data
    (for updates) can do so in Joshu's expected shape.
    """
    if not isinstance(raw, list):
        return {"_raw": raw}

    result: dict[str, Any] = {"_raw": raw}
    for item in raw:
        if not isinstance(item, dict):
            continue
        code = item.get("code")
        if not code:
            continue
        result[code] = _extract_simple_value(item.get("value"))
    return result


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

def _humanize_code(code: str) -> str:
    """Turn 'insured.split_address.zipcode' → 'Zipcode' and 'app.aop_deductible' → 'AOP Deductible'."""
    if not code:
        return code
    # Take last segment after the last dot
    last = code.rsplit(".", 1)[-1]
    # Split on underscores, handle common abbreviations
    parts = last.split("_")
    abbreviations = {"aop", "gl", "ui", "tria", "epli", "id", "us", "eb", "dba"}
    out = []
    for p in parts:
        if p.lower() in abbreviations:
            out.append(p.upper())
        else:
            out.append(p.capitalize())
    return " ".join(out)


def _section_from_code(code: str) -> str:
    """Return the first path segment — e.g. 'insured' / 'app' / 'data'."""
    return code.split(".", 1)[0] if "." in code else "other"


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

    def _section_label(code: str) -> str:
        """Humanize a section code like 'insured' or 'app.equipment' for display."""
        if not code:
            return "Other"
        # Take the last segment for readability
        last = code.rsplit(".", 1)[-1]
        return _humanize_code(last) or last.capitalize()

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

    def _process_datapoints(dps: list, section_code: str, section_order: int):
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

            ve = _validation_error_from(dp.get("validation_issue"))
            if ve:
                has_errors = True

            type_info = _parse_field_type(dp.get("kind"))

            field = {
                "code": code,
                "label": _humanize_code(code),
                "section": section_code,
                "section_label": _section_label(section_code),
                "section_order": section_order,
                "asset_idx": dp.get("asset_idx", 0),
                "type": type_info.get("type"),
                "required": bool(dp.get("required")),
                # condition_met: True or None → visible; False → hidden (conditional)
                "visible": dp.get("condition_met") is not False,
                "exists": bool(dp.get("exists")),
                "value": data_values.get(code) if data_values else None,
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

    # 1. insured_details_section (always comes first)
    insured = raw.get("insured_details_section")
    if isinstance(insured, dict):
        code = insured.get("code") or "insured"
        sec_error = _validation_error_from(insured.get("section_validation_issue"))
        sec_fields = _process_datapoints(insured.get("datapoints", []), code, section_order)
        fields.extend(sec_fields)
        section_summaries.append({
            "code": code,
            "label": _section_label(code),
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
            sec_error = _validation_error_from(sec.get("section_validation_issue"))
            sec_fields = _process_datapoints(sec.get("datapoints", []), code, section_order)
            fields.extend(sec_fields)
            section_summaries.append({
                "code": code,
                "label": _section_label(code),
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

    # 3. bind_sections (relevant at bind time — only include if condition_met)
    bind_sections = raw.get("bind_sections")
    if isinstance(bind_sections, list):
        for sec in bind_sections:
            if not isinstance(sec, dict):
                continue
            # Skip bind sections that aren't currently relevant
            if sec.get("condition_met") is False:
                continue
            code = sec.get("code") or f"bind_{section_order}"
            sec_error = _validation_error_from(sec.get("section_validation_issue"))
            sec_fields = _process_datapoints(sec.get("datapoints", []), code, section_order)
            if not sec_fields:
                continue  # empty bind section — skip
            fields.extend(sec_fields)
            section_summaries.append({
                "code": code,
                "label": _section_label(code) + " (Bind)",
                "order": section_order,
                "field_count": len(sec_fields),
                "completed": sum(1 for f in sec_fields if f["exists"]),
                "visible_count": sum(1 for f in sec_fields if f["visible"]),
                "has_errors": bool(sec_error) or any(f["validation_error"] for f in sec_fields),
                "is_asset": bool(sec.get("is_asset")),
                "condition_met": True,
                "section_error": sec_error,
                "is_bind": True,
            })
            if sec_error:
                has_errors = True
            section_order += 1

    # 4. Fallback: if nothing was found via the known structure, look for a
    # top-level `datapoints` array (older response shape or unknown variant)
    if not fields:
        top_dps = raw.get("datapoints")
        if isinstance(top_dps, list):
            fallback_fields = _process_datapoints(top_dps, "other", 0)
            fields.extend(fallback_fields)
            section_summaries.append({
                "code": "other", "label": "Other", "order": 0,
                "field_count": len(fallback_fields),
                "completed": sum(1 for f in fallback_fields if f["exists"]),
                "visible_count": sum(1 for f in fallback_fields if f["visible"]),
                "has_errors": any(f["validation_error"] for f in fallback_fields),
                "is_asset": False, "condition_met": True, "section_error": None,
            })

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

        # Map environment to Joshu's container value. This is the ONLY place
        # the container string is chosen. Callers cannot override it.
        self._container: str = {
            "test": "Test",
            "production": "Production",
        }.get(settings.joshu_environment, "Test")

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
            "HttpJoshuClient initialized · base_url=%s · container=%s · enabled_writes=%s",
            self.base_url, self._container, enabled_writes or "none",
        )

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

    def _build_params(self, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        """Inject the ``container`` parameter — SAFETY LINCHPIN.

        The container is fixed at construction time. Extra params from the
        caller are merged but CANNOT override container. Any attempt is
        logged as a safety event and silently ignored.
        """
        params: dict[str, Any] = {}
        if extra:
            for k, v in extra.items():
                if k.lower() == "container":
                    log.error(
                        "SAFETY: A caller attempted to set 'container' parameter. "
                        "Ignored. caller_value=%r enforced_value=%r",
                        v, self._container,
                    )
                    continue
                if v is not None:
                    params[k] = v
        # Container is set LAST so it cannot be stomped by a later update
        params["container"] = self._container
        assert params["container"] == self._container, \
            "Container param was unexpectedly mutated — this is a bug"
        return params

    async def _get(
        self, path: str, *, params: dict[str, Any] | None = None,
        bearer_token: str | None = None,
    ) -> Any:
        """Single choke point for all JSON reads."""
        full_params = self._build_params(params)
        url = f"{self.API_PREFIX}{path}"
        log.debug("GET %s params=%s", url, full_params)
        resp = await self._client.get(
            url, params=full_params, headers=self._headers(bearer_token),
        )
        self._raise_for_status(resp, "GET", url)
        return resp.json()

    async def _get_raw(
        self, path: str, *, params: dict[str, Any] | None = None,
        bearer_token: str | None = None,
    ) -> tuple[bytes, str]:
        """Binary read (for document downloads)."""
        full_params = self._build_params(params)
        url = f"{self.API_PREFIX}{path}"
        resp = await self._client.get(
            url, params=full_params, headers=self._headers(bearer_token),
        )
        self._raise_for_status(resp, "GET", url)
        return resp.content, resp.headers.get("content-type", "application/octet-stream")

    async def _put(
        self, path: str, *, body: Any, params: dict[str, Any] | None = None,
        bearer_token: str | None = None,
    ) -> Any:
        """Single choke point for PUT requests. Container is still forced."""
        self._assert_test_mode_for_write()
        full_params = self._build_params(params)
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
        bearer_token: str | None = None,
    ) -> Any:
        """Single choke point for POST requests. Container is still forced."""
        self._assert_test_mode_for_write()
        full_params = self._build_params(params)
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
        data = await self._get(f"/products/{product_id}", bearer_token=token)
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
        data = await self._get(f"/submissions/{submission_id}", bearer_token=token)
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
            raw = await self._get(f"/submission-data/{submission_id}", bearer_token=token)
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
            return await self._get(f"/submission-status/{submission_id}", bearer_token=token)
        except Exception as e:
            log.warning("submission-status fetch failed for %s: %s", submission_id, e)
            return {}

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
        data = await self._get(f"/policies/{policy_id}", bearer_token=token)
        return Policy.model_validate(data)

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

    async def list_quotes(
        self, token, *, submission_id=None, page=1, per_page=25,
    ) -> Paginated:
        params: dict[str, Any] = {"_page": page, "_per_page": per_page}
        if submission_id: params["submission_id"] = submission_id
        data = await self._get("/quotes", params=params, bearer_token=token)
        return Paginated.model_validate(data)

    async def get_quote(self, token, quote_id: str | int) -> Quote:
        data = await self._get(f"/quotes/{quote_id}", bearer_token=token)
        return Quote.model_validate(data)

    async def get_quote_data(self, token, quote_id: str | int) -> dict[str, Any]:
        """Fetch datapoint values for a quote via /quote-data/{id}."""
        try:
            raw = await self._get(f"/quote-data/{quote_id}", bearer_token=token)
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

    async def get_document(self, token, document_id: int) -> Document:
        data = await self._get(f"/documents/{document_id}", bearer_token=token)
        return Document.model_validate(data)

    async def download_document(self, token, document_id: int) -> tuple[bytes, str]:
        # Joshu's API returns a file_id on the document record. The binary
        # is usually fetched via /documents/{id}/download or a related file
        # endpoint. We try the common pattern first; if Joshu uses a
        # different URL structure, you'll get a 404 and we'll adjust.
        return await self._get_raw(
            f"/documents/{document_id}/download", bearer_token=token,
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

    async def aclose(self) -> None:
        """Close the underlying HTTPX client. Call on app shutdown."""
        await self._client.aclose()
