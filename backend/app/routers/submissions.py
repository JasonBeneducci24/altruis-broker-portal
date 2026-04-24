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
    # If broker wants just their own work, filter by user_id
    if mine_only:
        kwargs["user_id"] = session["uid"]
    result = await client.list_submissions(session["t"], **kwargs)
    return result.model_dump(mode="json")


import re

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


async def _resolve_submission_uuid(client: JoshuClientBase, token: str, submission_id: str) -> str:
    """If submission_id looks numeric, try to find its UUID via the list endpoint.

    Joshu's v3 detail endpoints use unique_id (UUID), not the numeric id. The
    frontend passes UUIDs for records the user clicked (since those come from
    the list response). But some callers (e.g. transaction navigation) only
    know the numeric id — this helper translates if possible, and otherwise
    just passes through to let Joshu's 404 propagate.
    """
    if _UUID_RE.match(submission_id):
        return submission_id
    try:
        numeric = int(submission_id)
    except ValueError:
        return submission_id

    # Try a single id-filter query — cheap, and if Joshu doesn't support it
    # we just fall through and let the numeric id go to Joshu (which 404s).
    try:
        # Call the underlying client method directly with a kwarg Joshu might
        # support. Not all implementations accept this; we tolerate the error.
        import inspect
        sig = inspect.signature(client.list_submissions)
        if "id" in sig.parameters:
            batch = await client.list_submissions(token, id=numeric, per_page=1)  # type: ignore[call-arg]
            if batch.items:
                uid = batch.items[0].get("unique_id")
                if uid:
                    return uid
    except Exception:
        pass
    return submission_id


@router.get("/{submission_id}")
async def get_submission(
    submission_id: str,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    uid = await _resolve_submission_uuid(client, session["t"], submission_id)
    sub = await client.get_submission(session["t"], uid)
    data = await client.get_submission_data(session["t"], uid)
    return {**sub.model_dump(mode="json"), "data": data}


@router.put("/{submission_id}/data")
async def update_submission_data(
    submission_id: str,
    body: dict[str, Any],
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    merged = await client.update_submission_data(session["t"], submission_id, body)
    return {"data": merged}


@router.post("/{submission_id}/submit")
async def submit_submission(
    submission_id: str,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    sub = await client.submit_submission(session["t"], submission_id)
    return sub.model_dump(mode="json")
