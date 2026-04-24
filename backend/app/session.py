"""
Session management for the broker portal.

The portal uses **signed HTTP-only cookies** to carry the broker's Joshu
bearer token across requests. We don't store sessions server-side — the
cookie IS the session.

Security notes:
  - Cookie is signed with SESSION_SECRET (itsdangerous). Forging it requires
    the secret.
  - HttpOnly + SameSite=Lax prevents JS access and CSRF-via-top-nav.
  - In production we set Secure=True (HTTPS only).
  - We DO NOT store passwords. We store only the bearer token Joshu gave us
    in exchange for valid credentials.
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import Request, Response, HTTPException
from itsdangerous import URLSafeSerializer, BadSignature

from app.config import settings


COOKIE_NAME = "altruis_broker_session"
SESSION_SERIALIZER = URLSafeSerializer(settings.session_secret, salt="broker-session")

# Sentinel value stored in the session when the portal is using its
# configured API token instead of a per-broker bearer token. See
# routers/auth.py for details.
API_TOKEN_SENTINEL = "__api_token__"


def set_session(response: Response, token: str, user_id: int, email: str, store_id: int | None) -> None:
    payload = {"t": token, "uid": user_id, "em": email, "sid": store_id}
    cookie_value = SESSION_SERIALIZER.dumps(payload)
    response.set_cookie(
        COOKIE_NAME,
        cookie_value,
        httponly=True,
        samesite="lax",
        secure=settings.is_production,  # HTTPS-only in prod
        max_age=60 * 60 * 12,  # 12 hours
        path="/",
    )


def clear_session(response: Response) -> None:
    response.delete_cookie(COOKIE_NAME, path="/")


def read_session(request: Request) -> dict[str, Any] | None:
    raw = request.cookies.get(COOKIE_NAME)
    if not raw:
        return None
    try:
        return SESSION_SERIALIZER.loads(raw)
    except BadSignature:
        return None


def require_session(request: Request) -> dict[str, Any]:
    """FastAPI dependency: return session payload or 401."""
    sess = read_session(request)
    if not sess or not sess.get("t"):
        raise HTTPException(401, "Not authenticated")
    return sess


def bearer_from_session(sess: dict[str, Any]) -> str | None:
    """Extract the real bearer token from session, or None if using the API-token sentinel.

    When the session holds the API_TOKEN_SENTINEL, the HTTP client should
    fall back to its statically-configured API token. Returning None
    signals that intent.
    """
    tok = sess.get("t")
    if tok == API_TOKEN_SENTINEL:
        return None
    return tok
