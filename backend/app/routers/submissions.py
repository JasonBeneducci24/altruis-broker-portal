"""Submission endpoints — thin proxy over Joshu."""
from __future__ import annotations

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


@router.get("")
async def list_submissions(
    status: str | None = None, flow: str | None = None,
    mine_only: bool = False, page: int = 1, per_page: int = 25,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    kwargs: dict[str, Any] = {
        "status": status, "flow": flow,
        "page": page, "per_page": per_page,
    }
    if mine_only:
        kwargs["user_id"] = session["uid"]
    result = await client.list_submissions(session["t"], **kwargs)
    return result.model_dump(mode="json")


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

