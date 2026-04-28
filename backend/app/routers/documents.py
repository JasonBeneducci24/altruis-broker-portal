"""Document endpoints — list, metadata, and binary download."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import Response

from app.joshu.factory import get_joshu_client
from app.joshu.client_base import JoshuClientBase
from app.joshu.schemas import DocumentType
from app.session import require_session


router = APIRouter(prefix="/api/documents", tags=["documents"])


@router.get("")
async def list_documents(
    quote_id: int | None = None,
    document_type: DocumentType | None = None,
    page: int = 1, per_page: int = 25,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    """List documents, container-filtered.

    Two distinct usage shapes:
      • quote_id given → fetch documents for one specific quote.
        Per-quote lookups don't need container filtering — the quote_id
        is unique system-wide and only resolves to documents attached
        to it.
      • no quote_id → broker library / dashboard widget. Uses the
        quote-driven discovery flow (which itself uses the policy-driven
        quote discovery) to ensure only test-container documents
        surface. /documents without a quote_id does not honor the
        container filter for our token (same problem as /submissions
        and /quotes).
    """
    if quote_id is not None:
        result = await client.list_documents(
            session["t"], quote_id=quote_id, document_type=document_type,
            page=page, per_page=per_page,
        )
        return result.model_dump(mode="json")

    payload = await client.discover_test_documents(
        session["t"], page=page, per_page=per_page,
        document_type=document_type,
    )
    payload["_meta"] = {"discovery_flow": "quote-driven (via policies)"}
    return payload


@router.get("/{document_id}")
async def get_document(
    document_id: str,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    doc = await client.get_document(session["t"], document_id)
    return doc.model_dump(mode="json")


@router.get("/{document_id}/download")
async def download_document(
    document_id: str,
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
):
    content, content_type = await client.download_document(session["t"], document_id)
    return Response(
        content=content, media_type=content_type,
        headers={"Content-Disposition": f'inline; filename="document_{document_id}.pdf"'},
    )
