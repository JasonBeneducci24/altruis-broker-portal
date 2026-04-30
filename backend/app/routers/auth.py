"""Auth endpoints: login, logout, whoami.

Auth model depends on environment:
  - mock: seed accounts, any password accepted, returns a mock token
  - test / production: Joshu's password-auth endpoint (not yet documented)
    falls back to "continue with API token" — the session stores a sentinel
    that tells the Joshu client to use its configured API token for outbound
    calls. Per-broker login against Joshu will be wired in once we know
    the /accounts/login URL.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, EmailStr

from app.joshu.factory import get_joshu_client
from app.joshu.client_base import JoshuClientBase
from app.session import set_session, clear_session, read_session, require_session
from app.config import settings


router = APIRouter(prefix="/api/auth", tags=["auth"])


# Session-stored sentinel meaning "use the portal's configured API token
# for outbound Joshu calls on behalf of this user". Eliminates the need
# to embed secrets in the cookie.
API_TOKEN_SENTINEL = "__api_token__"


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


@router.post("/login")
async def login(
    req: LoginRequest,
    response: Response,
    client: JoshuClientBase = Depends(get_joshu_client),
):
    if settings.is_mock:
        # Mock mode: accept seeded emails, any password
        token, user = await client.login(req.email, req.password)
        set_session(response, token=token, user_id=user.id,
                    email=user.email, store_id=user.store_id)
        return {
            "user": user.model_dump(mode="json"),
            "environment": settings.joshu_environment,
        }

    # Non-mock (test/production): real per-broker login against
    # Joshu's auth subsystem at /api/auth/v1/password/login.
    #
    # The HTTP client now implements a real login() — it POSTs the
    # email/password to Joshu, parses the access_token JWT from the
    # 201 response, and returns (jwt, user). The JWT becomes the
    # session's bearer token for all subsequent broker requests
    # (reads AND writes).
    #
    # The previous API_TOKEN_SENTINEL fallback path is gone. The
    # long-lived API token cannot perform writes against Joshu
    # (verified by direct probe: 401 'Invalid API token' on POST
    # /policies even with a valid Token-auth header), so the
    # sentinel session would be unable to support the create flow.
    # Real per-broker JWTs are required.
    if not settings.joshu_base_url:
        raise HTTPException(
            503,
            "Portal is not configured: JOSHU_BASE_URL is missing. "
            "Contact your administrator.",
        )

    token, user = await client.login(req.email, req.password)
    set_session(
        response, token=token, user_id=user.id,
        email=user.email, store_id=user.store_id,
    )
    return {
        "user": user.model_dump(mode="json"),
        "environment": settings.joshu_environment,
    }


@router.post("/logout")
async def logout(response: Response):
    clear_session(response)
    return {"status": "ok"}


@router.get("/me")
async def me(
    request: Request,
    client: JoshuClientBase = Depends(get_joshu_client),
):
    sess = read_session(request)
    if not sess or not sess.get("t"):
        return {"authenticated": False, "environment": settings.joshu_environment}

    # If session is using the API token sentinel, don't try to call whoami
    # (it would fail in HTTP mode because Joshu has no /me endpoint).
    # Synthesize the user from the session instead.
    if sess.get("t") == API_TOKEN_SENTINEL:
        email = sess.get("em", "")
        return {
            "authenticated": True,
            "user": {
                "id": 0, "email": email,
                "name": email.split("@")[0].replace(".", " ").title() if email else "User",
                "store_id": sess.get("sid"), "store_name": "Altruis Group",
                "role": "Broker (API token session)",
            },
            "environment": settings.joshu_environment,
        }

    try:
        user = await client.whoami(sess["t"])
    except Exception:
        return {"authenticated": False, "environment": settings.joshu_environment}
    return {
        "authenticated": True,
        "user": user.model_dump(mode="json"),
        "environment": settings.joshu_environment,
    }
