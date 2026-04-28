"""Products endpoint — broker-facing list of insurance products.

Used by the New Submission flow: the broker picks which product they're
quoting, and the portal sends the corresponding `product_version_id`
when creating the underlying transaction.

Joshu's `/products` endpoint returns each product with its `published`
ProductVersion populated. We expose just the fields the picker needs
(id, name, display_name, published.id) so the frontend doesn't have
to know the full Joshu schema.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from app.joshu.factory import get_joshu_client
from app.joshu.client_base import JoshuClientBase
from app.session import require_session


router = APIRouter(prefix="/api/products", tags=["products"])


@router.get("")
async def list_products(
    session=Depends(require_session),
    client: JoshuClientBase = Depends(get_joshu_client),
) -> dict[str, Any]:
    """List published products available for new submissions.

    Returns a flat list — one entry per product — with the published
    version's ID included as `product_version_id` (the value the
    broker portal actually needs when creating a transaction).
    Unpublished products are filtered out. If a product has no
    `published` version (rare), it's omitted.
    """
    products = await client.list_products(session["t"])
    items = []
    for p in products:
        published = getattr(p, "published", None)
        if not published:
            continue
        items.append({
            "id": p.id,
            "name": p.name,
            "display_name": p.display_name or p.name,
            "product_version_id": published.id,
            "version_label": published.version,
        })
    return {"items": items}
