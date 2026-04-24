"""Policy endpoints — list, get, lifecycle transactions."""
from __future__ import annotations

from datetime import datetime
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.joshu.factory import get_joshu_client
from app.joshu.client_base import JoshuClientBase
from app.joshu.schemas import OngoingChange
from app.session import require_session


router = APIRouter(prefix="/api/policies", tags=["policies"])


@router.get("")
async def list_policies(
    status: str | None = None, page: int = 1, per_page: int = 25,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    result = await client.list_policies(
        session["t"], status=status, page=page, per_page=per_page,
    )
    return result.model_dump(mode="json")


@router.get("/{policy_id}")
async def get_policy(
    policy_id: str,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    policy = await client.get_policy(session["t"], policy_id)
    txns = await client.list_transactions(session["t"], policy_id=policy_id)
    return {
        **policy.model_dump(mode="json"),
        "transactions": txns.model_dump(mode="json").get("items", []),
    }


class LifecycleTransactionRequest(BaseModel):
    flow: OngoingChange
    effective_date: str | None = None


@router.post("/{policy_id}/transactions")
async def create_lifecycle_transaction(
    policy_id: str,
    body: LifecycleTransactionRequest,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    """Start an endorsement / renewal / cancellation / reinstatement.

    Returns the new submission_id the broker should navigate to and fill in.
    """
    eff = datetime.fromisoformat(body.effective_date) if body.effective_date else None
    txn = await client.create_transaction(
        session["t"], flow=body.flow, policy_id=policy_id, effective_date=eff,
    )
    return {
        "transaction_id": txn.id,
        "submission_id": txn.latest_submission_id,
        "flow": body.flow,
    }
