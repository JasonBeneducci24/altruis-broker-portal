"""
Factory for the Joshu client.

Returns the mock client in mock mode; returns the (dormant) HTTP client
otherwise. Routers depend on this factory, never on a concrete client.
"""
from __future__ import annotations

from functools import lru_cache

from app.config import settings
from app.joshu.client_base import JoshuClientBase
from app.joshu.client_mock import MockJoshuClient
from app.joshu.client_http import HttpJoshuClient


@lru_cache(maxsize=1)
def get_joshu_client() -> JoshuClientBase:
    """Return the client appropriate for the current environment."""
    if settings.is_mock:
        return MockJoshuClient()
    # test or production → HTTP client (will refuse until activated)
    return HttpJoshuClient()
