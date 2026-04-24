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


@router.get("/{submission_id}")
async def get_submission(
    submission_id: int,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    sub = await client.get_submission(session["t"], submission_id)
    data = await client.get_submission_data(session["t"], submission_id)
    return {**sub.model_dump(mode="json"), "data": data}


@router.put("/{submission_id}/data")
async def update_submission_data(
    submission_id: int,
    body: dict[str, Any],
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    merged = await client.update_submission_data(session["t"], submission_id, body)
    return {"data": merged}


@router.post("/{submission_id}/submit")
async def submit_submission(
    submission_id: int,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    sub = await client.submit_submission(session["t"], submission_id)
    return sub.model_dump(mode="json")
