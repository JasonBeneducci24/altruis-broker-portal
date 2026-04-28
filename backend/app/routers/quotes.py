"""Quote endpoints — thin proxy over Joshu."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.joshu.factory import get_joshu_client
from app.joshu.client_base import JoshuClientBase
from app.session import require_session


router = APIRouter(prefix="/api/quotes", tags=["quotes"])


class QuoteStatusUpdate(BaseModel):
    status: str  # QuotePublished, BinderPublished, CoverageActive, etc.


@router.get("")
async def list_quotes(
    submission_id: int | None = None,
    status: str | None = None,
    page: int = 1, per_page: int = 25,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    """List quotes, container-filtered.

    Two distinct usage shapes:
      • submission_id given → fetch quotes for one specific submission.
        Per-submission lookups don't need container filtering — the
        submission_id is unique system-wide and only resolves to quotes
        attached to it.
      • no submission_id → broker dashboard / quotes list view. Uses the
        policy-driven discovery flow because /quotes without a submission_id
        does not honor the container filter for our token (same problem
        we hit on /submissions).

    See HttpJoshuClient.discover_test_quotes() for the discovery flow:
    list policies → fan out /quotes per policy's submission → flatten
    + sort + paginate.
    """
    if submission_id is not None:
        result = await client.list_quotes(
            session["t"], submission_id=submission_id,
            page=page, per_page=per_page,
        )
        return result.model_dump(mode="json")

    payload = await client.discover_test_quotes(
        session["t"], page=page, per_page=per_page,
        status_filter=status,
    )
    payload["_meta"] = {"discovery_flow": "policies-driven"}
    return payload


@router.get("/{quote_id}")
async def get_quote(
    quote_id: str,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    q = await client.get_quote(session["t"], quote_id)
    data = await client.get_quote_data(session["t"], quote_id)
    return {**q.model_dump(mode="json"), "data": data}


@router.post("/{quote_id}/publish")
async def publish_quote(
    quote_id: str,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    q = await client.update_quote_status(session["t"], quote_id, "QuotePublished")
    return q.model_dump(mode="json")


@router.post("/{quote_id}/bind")
async def bind_quote(
    quote_id: str,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    """Request a binder (broker-initiated; carrier/UW may still need to approve)."""
    q = await client.update_quote_status(session["t"], quote_id, "BinderPending")
    return q.model_dump(mode="json")
