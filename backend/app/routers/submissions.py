"""Submission endpoints — thin proxy over Joshu."""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from app.joshu.factory import get_joshu_client
from app.joshu.client_base import JoshuClientBase
from app.joshu.schemas import OngoingChange
from app.session import require_session


router = APIRouter(prefix="/api/submissions", tags=["submissions"])


class StartSubmissionRequest(BaseModel):
    """
    Per Joshu v3, creating a new submission really means:
      1. Create a policy (container)
      2. Create a New transaction on that policy
      → Joshu auto-creates the submission and returns latest_submission_id.
    The portal handles this whole dance from one endpoint.
    """
    product_version_id: int | None = None
    effective_date: str | None = None  # ISO date


@router.post("/start")
async def start_submission(
    body: StartSubmissionRequest,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    policy = await client.create_policy(session["t"])
    eff = None
    if body.effective_date:
        eff = datetime.fromisoformat(body.effective_date)
    txn = await client.create_transaction(
        session["t"], flow="New", policy_id=policy.id,
        product_version_id=body.product_version_id, effective_date=eff,
    )
    return {
        "policy_id": policy.id,
        "transaction_id": txn.id,
        "submission_id": txn.latest_submission_id,
    }


@router.get("/_debug/insured-lookup")
async def debug_insured_lookup(
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    """Diagnostic endpoint — figure out why insured names aren't appearing.

    Returns a structured report:
    - submissions_sample: first 3 submission rows from /submissions
    - policies_sample: first 3 policies from /policies (or error)
    - policy_count: how many policies were returned overall
    - policy_id_overlap: do submission policy_ids appear in the policies list?
    - sample_submission_data: try /submission-data on the first submission
    """
    report: dict[str, Any] = {}

    # 1. Get submissions
    try:
        subs_result = await client.list_submissions(session["t"], page=1, per_page=10)
        sub_items = subs_result.items or []
        report["submissions_count"] = len(sub_items)
        report["submissions_sample"] = [
            {
                "id": (r.get("id") if isinstance(r, dict) else getattr(r, "id", None)),
                "policy_id": (r.get("policy_id") if isinstance(r, dict) else getattr(r, "policy_id", None)),
                "status": (r.get("status") if isinstance(r, dict) else getattr(r, "status", None)),
                "insured_name_in_row": (r.get("insured_name") if isinstance(r, dict) else getattr(r, "insured_name", None)),
                "all_keys": list(r.keys()) if isinstance(r, dict) else None,
            }
            for r in sub_items[:3]
        ]
    except Exception as e:
        report["submissions_error"] = f"{type(e).__name__}: {e}"
        return report

    # 2. Get policies
    try:
        pols_result = await client.list_policies(session["t"], page=1, per_page=50)
        pol_items = pols_result.items or []
        report["policies_count"] = len(pol_items)
        report["policies_sample"] = [
            {
                "id": (r.get("id") if isinstance(r, dict) else getattr(r, "id", None)),
                "insured_name": (r.get("insured_name") if isinstance(r, dict) else getattr(r, "insured_name", None)),
                "insured_id": (r.get("insured_id") if isinstance(r, dict) else getattr(r, "insured_id", None)),
                "status": (r.get("status") if isinstance(r, dict) else getattr(r, "status", None)),
            }
            for r in pol_items[:3]
        ]

        # 3. Check overlap between submission policy_ids and policy ids
        sub_policy_ids = set()
        for r in sub_items:
            pid = r.get("policy_id") if isinstance(r, dict) else getattr(r, "policy_id", None)
            if pid:
                sub_policy_ids.add(str(pid))
        pol_ids = set()
        for r in pol_items:
            pid = r.get("id") if isinstance(r, dict) else getattr(r, "id", None)
            if pid:
                pol_ids.add(str(pid))
        report["sub_policy_id_count"] = len(sub_policy_ids)
        report["pol_id_count"] = len(pol_ids)
        report["overlap_count"] = len(sub_policy_ids & pol_ids)
        report["sub_policy_id_sample"] = list(sub_policy_ids)[:3]
        report["pol_id_sample"] = list(pol_ids)[:3]
    except Exception as e:
        report["policies_error"] = f"{type(e).__name__}: {e}"

    # 4. Try /submission-data on the first submission
    try:
        if sub_items:
            first = sub_items[0]
            first_id = first.get("id") if isinstance(first, dict) else getattr(first, "id", None)
            if first_id:
                data = await client.get_submission_data(session["t"], first_id)
                # Just report what 'insured.name' looks like
                report["sample_submission_id"] = first_id
                report["sample_data_has_insured_name"] = "insured.name" in (data or {})
                report["sample_insured_name_value"] = (data or {}).get("insured.name")
                report["sample_data_keys_count"] = len(data or {})
                # Sample 5 keys to see the shape
                report["sample_data_keys"] = list((data or {}).keys())[:10]
    except Exception as e:
        report["submission_data_error"] = f"{type(e).__name__}: {e}"

    return report


@router.get("")
async def list_submissions(
    status: str | None = None, flow: str | None = None,
    mine_only: bool = False, page: int = 1, per_page: int = 25,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    """List submissions, container-filtered.

    Discovery flow: list /policies (which honors container=Test), fan out
    to fetch each policy's detail to extract `ongoing_change_submission_id`,
    then fetch each submission record by ID. See
    HttpJoshuClient.discover_test_submissions() for the full mechanics.

    The /submissions list endpoint is NOT used here — it does not honor
    the container filter for our API token and would leak production
    records into a test-mode portal. We learned this the hard way.

    Caller-side filters (status, flow) are applied to the discovered
    submissions. The mine_only filter is currently ignored at this
    layer (no submission-level user_id filtering in the discovery flow);
    if/when we need it, we'll filter on the policy's user_id instead.
    """
    payload = await client.discover_test_submissions(
        session["t"],
        page=page, per_page=per_page,
        status_filter=status,
        flow_filter=flow,
    )
    items = payload.get("items") or []

    # Insured-name fallback: most submissions get insured_name straight
    # from the parent policy (set during discovery), but a small fraction
    # may not have a name on the policy record. For those, fall back to
    # /submission-data to pull `insured.name` directly. Capped at the
    # page size, runs in parallel.
    needs_fallback = [r for r in items if not r.get("insured_name")]
    if needs_fallback:
        async def _fetch_name(row):
            sub_id = row.get("id")
            if not sub_id:
                return (row, None)
            try:
                data = await client.get_submission_data(session["t"], sub_id)
            except Exception:
                return (row, None)
            return (row, _extract_name_from_data_dict(data))

        results = await asyncio.gather(*(_fetch_name(r) for r in needs_fallback))
        for row, name in results:
            if name:
                row["insured_name"] = name

    payload["_meta"] = {
        "discovery_flow": "policies-driven",
        "fallback_attempted": len(needs_fallback),
    }
    return payload


def _extract_name_from_data_dict(data: dict) -> str | None:
    """Pull insured.name out of a flattened /submission-data response.

    The flatten step in joshu/client_http.py converts Joshu's
    Array<{code, value}> into a flat {code: simplified_value} dict.
    For Text-typed datapoints the simplified value is just the string.
    """
    if not isinstance(data, dict):
        return None
    val = data.get("insured.name")
    if val is None:
        return None
    if isinstance(val, str) and val.strip():
        return val.strip()
    # Some flatten paths may leave a {Plain:{Text:...}} envelope intact —
    # walk it.
    if isinstance(val, dict):
        return _unwrap_simple(val)
    return None


def _extract_insured_name(row: dict) -> str | None:
    """Best-effort lookup of the named insured from a Joshu list row.

    Tries the most likely paths and returns the first non-empty match,
    or None if nothing usable is found.
    """
    # Top-level shortcut
    name = row.get("insured_name")
    if isinstance(name, str) and name.strip():
        return name.strip()

    data = row.get("data")
    if isinstance(data, dict):
        # Nested object form: {insured: {name: "..."}}
        ins = data.get("insured")
        if isinstance(ins, dict):
            n = ins.get("name")
            if isinstance(n, str) and n.strip():
                return n.strip()

        # Flat datapoint form: {"insured.name": "..."}
        flat = data.get("insured.name")
        if flat is not None:
            extracted = _unwrap_simple(flat)
            if isinstance(extracted, str) and extracted.strip():
                return extracted.strip()

        # Last-resort fallbacks
        for k in ("named_insured", "business_name", "company_name"):
            v = data.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

    # Top-level nested
    ins = row.get("insured")
    if isinstance(ins, dict) and isinstance(ins.get("name"), str):
        return ins["name"].strip() or None

    return None


def _unwrap_simple(wrapped) -> str | None:
    """Unwrap a Joshu wrapped value to its scalar — text-only.

    Walks {Plain|V1|V0: {Text|Number|Date|Boolean: ...}} until a primitive
    is found. Returns None for non-text or unrecognized shapes.
    """
    if isinstance(wrapped, str):
        return wrapped
    if not isinstance(wrapped, dict):
        return None
    for outer in ("Plain", "V1", "V0"):
        inner = wrapped.get(outer)
        if isinstance(inner, dict):
            for type_key in ("Text", "Number", "Date", "DateTime"):
                if type_key in inner:
                    v = inner[type_key]
                    if v is None:
                        return None
                    return str(v)
            if "Null" in inner:
                return None
    # Already-unwrapped fallback
    for type_key in ("Text", "Number"):
        if type_key in wrapped:
            return str(wrapped[type_key])
    return None



@router.get("/{submission_id}")
async def get_submission(
    submission_id: str,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    """Submission metadata + flattened data values.

    Submission path params are numeric i32 ids in Joshu. The frontend passes
    numeric ids from the list response, so submission_id here should be a
    string-coerced int (FastAPI accepts any string for a str-typed path param,
    Joshu accepts it because it's still numeric).
    """
    sub = await client.get_submission(session["t"], submission_id)
    data = await client.get_submission_data(session["t"], submission_id)
    return {**sub.model_dump(mode="json"), "data": data}


@router.get("/{submission_id}/form")
async def get_submission_form(
    submission_id: str,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    """Return a UI-ready form schema for this submission.

    Calls THREE Joshu endpoints and merges the results:
      - GET /submission-status/{id}  — the schema + validation state
      - GET /submission-data/{id}    — scalar root-level values
      - GET /asset-data/{id}         — asset collection values (structures, etc.)
    """
    from app.joshu.client_http import (
        normalize_submission_status,
        _merge_asset_data,
    )

    status_raw = await client.get_submission_status(session["t"], submission_id)
    data_values = await client.get_submission_data(session["t"], submission_id)
    asset_data = await client.get_asset_data(session["t"], submission_id)

    # Merge asset data into the _assets map so the normalizer can do
    # asset-aware value lookups. Without this step, the /submission-data
    # endpoint only carries scalars — asset collections appear as Null.
    if asset_data:
        _merge_asset_data(data_values, asset_data)

    # Strip _raw (the original wire payload), but keep _assets so the
    # normalizer can do asset-aware value lookups (structures, locations, etc.)
    clean_values = {k: v for k, v in data_values.items()
                    if not k.startswith("_") or k == "_assets"}
    normalized = normalize_submission_status(status_raw, clean_values)
    return normalized


@router.get("/{submission_id}/debug")
async def debug_submission_raw(
    submission_id: str,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    """DIAGNOSTIC: return raw Joshu responses side-by-side for debugging.

    Hits all three data endpoints and shows what each returns so we can see
    where asset values actually live.
    """
    status_raw = await client.get_submission_status(session["t"], submission_id)
    data_values = await client.get_submission_data(session["t"], submission_id)
    asset_data = await client.get_asset_data(session["t"], submission_id)
    raw_data = data_values.get("_raw") if isinstance(data_values, dict) else None

    codes = []
    structure_entries = []
    if isinstance(raw_data, list):
        for item in raw_data:
            if not isinstance(item, dict):
                continue
            code = item.get("code", "")
            codes.append(code)
            low = code.lower()
            if ("structure" in low or "location" in low or "building" in low or
                "address" in low or "peril" in low):
                structure_entries.append(item)

    # Summarize asset-data response — this is the key unknown shape
    asset_summary: dict = {"shape": type(asset_data).__name__}
    if isinstance(asset_data, list):
        asset_summary["length"] = len(asset_data)
        asset_summary["top_level_codes"] = [
            entry.get("code", "?") for entry in asset_data[:10] if isinstance(entry, dict)
        ]
        asset_summary["sample_full_entries"] = asset_data[:3]
    elif isinstance(asset_data, dict):
        asset_summary["keys"] = sorted(list(asset_data.keys()))[:50]
        asset_summary["sample"] = {k: asset_data[k] for k in list(asset_data.keys())[:3]}
    else:
        asset_summary["raw"] = asset_data

    return {
        "submission_id": submission_id,
        "submission_data": {
            "data_raw_length": len(raw_data) if isinstance(raw_data, list) else "not_a_list",
            "data_codes_sorted": sorted(set(codes)),
            "structure_entries": structure_entries,
        },
        "asset_data_summary": asset_summary,
        "status_raw_sections_summary": [
            {"code": s.get("code"), "is_asset": s.get("is_asset"),
             "condition_met": s.get("condition_met"),
             "datapoint_count": len(s.get("datapoints", []) or [])}
            for s in status_raw.get("sections", []) if isinstance(s, dict)
        ] if isinstance(status_raw, dict) else [],
    }



@router.put("/{submission_id}/data")
async def update_submission_data(
    submission_id: str,
    body: dict[str, Any],
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    """Save partial or full submission data.

    Body is a flat {code: value} dict. The backend converts each value
    into Joshu's V1-tagged union format before PUT.

    We fetch the schema first so each field can be type-hinted correctly —
    a plain string sent for a Location field must be wrapped differently
    than a string sent for a Text field.
    """
    # Look up the field types via the schema
    type_hints = {}
    try:
        status_raw = await client.get_submission_status(session["t"], submission_id)
        from app.joshu.client_http import normalize_submission_status
        normalized = normalize_submission_status(status_raw, {})
        for f in normalized.get("fields", []):
            code = f.get("code")
            if not code:
                continue
            t = f.get("type")
            if t == "text": type_hints[code] = "Text"
            elif t == "number": type_hints[code] = "Number"
            elif t == "monetary": type_hints[code] = "Monetary"
            elif t == "boolean": type_hints[code] = "Boolean"
            elif t == "date": type_hints[code] = "Date"
            elif t == "datetime": type_hints[code] = "DateTime"
            elif t == "location": type_hints[code] = "Location"
    except Exception as e:
        # If the schema fetch fails we fall back to type inference
        import logging
        logging.getLogger("altruis").warning("Schema lookup for write failed: %s", e)

    merged = await client.update_submission_data(
        session["t"], submission_id, body, type_hints=type_hints,
    )
    return {"data": merged}


@router.post("/{submission_id}/submit")
async def submit_submission(
    submission_id: str,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    """Submit an Incomplete submission — moves status to Submitted.

    Joshu triggers its rating engine on status transition to Submitted,
    which generates the first quote. Also works as "resubmit" on a
    Submitted record that was re-opened.
    """
    sub = await client.submit_submission(session["t"], submission_id)
    return sub.model_dump(mode="json")


@router.post("/{submission_id}/reopen")
async def reopen_submission(
    submission_id: str,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    """Move a Submitted/Pending submission back to Incomplete for editing."""
    sub = await client.reopen_submission(session["t"], submission_id)
    return sub.model_dump(mode="json")

