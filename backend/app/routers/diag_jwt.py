"""
Throwaway diagnostic: confirm whether the JWT auth scheme determines
container routing for v3 writes.

Hypothesis
==========
Joshu's UI uses ``Authorization: Bearer <JWT>`` for writes; we use the
long-lived ``Authorization: Token <key>``. The long-lived token's container
scope appears to be locked to PRODUCTION at issuance, so writes from us
land in prod even when ``container=Test`` is on the URL or ``test:true``
is in the body. Reads filter correctly because the read path honors the
query param.

What this endpoint does
=======================
Lets us POST exactly one v3 write using a JWT we manually copied from a
logged-in browser session in Joshu's UI. If the resulting object lands
in the test container, the hypothesis is confirmed.

Safety
======
- Off by default. Must set ``DIAG_JWT_ENABLED=1`` in the environment.
- Refuses to run unless ``JOSHU_ENVIRONMENT=test`` (we never want this
  pointing at production, even by accident).
- Dry-run is the default. The request only goes out when the caller
  passes BOTH ``?execute=1`` AND ``?confirm=jwt-test-<YYYYMMDD>`` where
  the date matches today (UTC). Forces a deliberate, dated decision.
- One-shot: after a successful execute, a sentinel file is written and
  subsequent execute attempts return 423 LOCKED. Reset by removing the
  sentinel or restarting the dyno.
- JWT is never logged in full — only the first 8 and last 4 characters.
- Endpoint and any logs scrub the JWT from echoed request payloads.

Removal
=======
This is a temporary investigation tool. Once the auth question is
answered, delete this file and remove its include_router line in
main.py. There is no migration story — it is throwaway by design.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.config import settings


log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/_diag", tags=["_diagnostic"])


# Sentinel file marking that a real execute has fired. Written under
# /tmp so it survives the request but not a dyno restart — Render's
# filesystem is ephemeral, which is fine for this purpose.
_SENTINEL_PATH = Path("/tmp/altruis_jwt_diag_fired.flag")


def _today_token_utc() -> str:
    return "jwt-test-" + datetime.now(timezone.utc).strftime("%Y%m%d")


def _redact_jwt(jwt: str) -> str:
    if not jwt:
        return ""
    if len(jwt) <= 16:
        return "<short-jwt-redacted>"
    return f"{jwt[:8]}…{jwt[-4:]}"


class JwtWriteTestRequest(BaseModel):
    """Body for the JWT diagnostic.

    Keep this small and explicit — we don't want to accidentally accept
    arbitrary upstream parameters.
    """

    jwt: str = Field(
        ...,
        min_length=20,
        description=(
            "A JWT copied from the Authorization: Bearer header of a "
            "Joshu UI request. The full three-part token, no 'Bearer ' "
            "prefix."
        ),
    )
    endpoint: Literal["policies", "transactions"] = Field(
        ...,
        description="Which v3 create endpoint to target.",
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "JSON body to POST. Joshu's UI sends an empty body for the "
            "create-policy POST, so the default is fine for that case. "
            "For transactions, supply the body that the UI sends."
        ),
    )
    extra_query_params: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Optional extra query params to include on the URL. We do "
            "NOT auto-add container=Test here — the whole point is to "
            "see whether the JWT alone routes to test."
        ),
    )


def _ensure_enabled() -> None:
    if os.environ.get("DIAG_JWT_ENABLED", "").strip() != "1":
        raise HTTPException(
            status_code=404,
            detail="diagnostic disabled (set DIAG_JWT_ENABLED=1 to enable)",
        )
    if not settings.is_test:
        raise HTTPException(
            status_code=409,
            detail=(
                f"diagnostic refuses to run with JOSHU_ENVIRONMENT="
                f"{settings.joshu_environment!r}; expected 'test'"
            ),
        )
    if not settings.joshu_base_url:
        raise HTTPException(
            status_code=500,
            detail="JOSHU_BASE_URL is not configured",
        )


@router.get("/jwt-write-test/status")
def jwt_write_test_status() -> dict[str, Any]:
    """Tells you whether the diagnostic is enabled and whether it has
    already fired its one allowed real execute.
    """
    enabled = os.environ.get("DIAG_JWT_ENABLED", "").strip() == "1"
    fired = _SENTINEL_PATH.exists()
    return {
        "enabled": enabled,
        "joshu_environment": settings.joshu_environment,
        "already_fired": fired,
        "sentinel_path": str(_SENTINEL_PATH),
        "today_confirm_token": _today_token_utc(),
        "usage": {
            "dry_run": (
                "POST /api/_diag/jwt-write-test  body={jwt, endpoint, payload}"
            ),
            "execute": (
                "POST /api/_diag/jwt-write-test"
                f"?execute=1&confirm={_today_token_utc()}"
            ),
        },
    }


@router.post("/jwt-write-test")
async def jwt_write_test(
    body: JwtWriteTestRequest,
    execute: int = Query(0, ge=0, le=1),
    confirm: str = Query("", description="Required for execute=1; must equal today's token"),
) -> dict[str, Any]:
    """Run the JWT write diagnostic.

    Default: dry-run. Returns the constructed upstream request without
    sending it. The JWT is redacted in the echoed Authorization header.

    Real send: requires ``execute=1`` AND ``confirm`` matching today's
    UTC token (e.g. ``jwt-test-20260428``). One-shot — refuses to fire
    again until the sentinel file is removed.
    """
    _ensure_enabled()

    upstream_url = (
        f"{settings.joshu_base_url}/api/insurance/v3/{body.endpoint}"
    )
    headers = {
        "Authorization": f"Bearer {body.jwt}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    redacted_headers = dict(headers)
    redacted_headers["Authorization"] = f"Bearer {_redact_jwt(body.jwt)}"

    constructed = {
        "method": "POST",
        "url": upstream_url,
        "query_params": body.extra_query_params,
        "headers": redacted_headers,
        "body": body.payload,
    }

    if execute != 1:
        return {
            "mode": "dry_run",
            "would_send": constructed,
            "note": (
                "No request was made. To execute, pass "
                f"?execute=1&confirm={_today_token_utc()}"
            ),
        }

    # --- execute path -----------------------------------------------
    expected = _today_token_utc()
    if confirm != expected:
        raise HTTPException(
            status_code=400,
            detail=(
                f"confirm token mismatch; expected {expected!r}, "
                f"got {confirm!r}"
            ),
        )

    if _SENTINEL_PATH.exists():
        raise HTTPException(
            status_code=423,
            detail=(
                "diagnostic has already fired its one allowed execute. "
                f"Remove {_SENTINEL_PATH} or restart the dyno to reset."
            ),
        )

    log.warning(
        "DIAG JWT WRITE: POST %s endpoint=%s jwt=%s extra_params=%s",
        upstream_url,
        body.endpoint,
        _redact_jwt(body.jwt),
        body.extra_query_params,
    )

    # Mark fired BEFORE the request so a hang or crash still consumes
    # the one-shot. Better to need a manual reset than to accidentally
    # fire twice.
    _SENTINEL_PATH.write_text(
        json.dumps(
            {
                "fired_at_utc": datetime.now(timezone.utc).isoformat(),
                "endpoint": body.endpoint,
                "jwt_redacted": _redact_jwt(body.jwt),
            }
        )
    )

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                upstream_url,
                headers=headers,
                params=body.extra_query_params or None,
                json=body.payload,
            )
    except httpx.HTTPError as exc:
        log.exception("DIAG JWT WRITE: transport error")
        return {
            "mode": "executed",
            "transport_error": str(exc),
            "constructed": constructed,
            "sentinel_written": True,
        }

    # Capture the upstream response in full so we can see exactly what
    # container the new object landed in.
    try:
        resp_body: Any = resp.json()
    except ValueError:
        resp_body = {"_raw_text": resp.text[:4000]}

    log.warning(
        "DIAG JWT WRITE: response status=%s id=%s",
        resp.status_code,
        (
            resp_body.get("id")
            if isinstance(resp_body, dict) else "<non-dict>"
        ),
    )

    return {
        "mode": "executed",
        "constructed": constructed,
        "response": {
            "status": resp.status_code,
            "headers": dict(resp.headers),
            "body": resp_body,
        },
        "interpretation_hints": [
            "If the response body has a 'container' or 'tenant' field, "
            "that tells you where the object landed.",
            "If status is 2xx and the new ID does NOT show up in your "
            "test-container UI, it likely landed in production.",
            "If status is 401/403, the JWT may be expired — copy a "
            "fresh one from DevTools and try again.",
        ],
        "sentinel_written": True,
    }


@router.post("/jwt-write-test/reset")
def jwt_write_test_reset() -> dict[str, Any]:
    """Remove the one-shot sentinel so another execute can fire.
    Still requires DIAG_JWT_ENABLED=1.
    """
    _ensure_enabled()
    existed = _SENTINEL_PATH.exists()
    if existed:
        _SENTINEL_PATH.unlink()
    return {"sentinel_existed": existed, "sentinel_path": str(_SENTINEL_PATH)}
