"""
HTTP client for the Joshu API — READ-ONLY phase.

ARCHITECTURE AND SAFETY
=======================

This client talks to the real Joshu API at ``{JOSHU_BASE_URL}/api/insurance/v3/*``.
The test-vs-production container is selected by a query-string parameter:

    GET  /api/insurance/v3/submissions?container=Test
    GET  /api/insurance/v3/policies?container=Test&_page=1&status=Active

**SAFETY INVARIANT**: The ``container`` parameter is fixed at construction
time based on the startup environment (``JOSHU_ENVIRONMENT``). It CANNOT
be overridden per-call. Every outbound request goes through ``_get()``,
which injects ``container`` before the request leaves the process —
individual methods have no ability to change it.

Additional layered defenses:

  1. Env guard in ``config.py`` — app refuses to start with JOSHU_ENVIRONMENT
     unset or in production without the explicit override token.
  2. ``_assert_test_mode_for_write()`` — belt-and-suspenders check before
     any mutation (not used in this read-only phase, in place for when
     writes are enabled).
  3. ``_build_params()`` actively strips any attempt by a caller to set
     ``container`` in extra params and logs it loudly as a safety event.

PHASE
=====

This is the READ-ONLY phase. GET methods are active. Every mutating method
still raises ``HttpClientNotReadyError``. Once reads are verified against
the test container, writes get enabled in a second pass.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from app.config import settings
from app.joshu.client_base import JoshuClientBase
from app.joshu.schemas import (
    Product, Policy, Submission, Quote, Document, Transaction,
    BrokerUser, Paginated, OngoingChange, DocumentType
)


log = logging.getLogger("altruis.joshu.http")

# Writes remain locked until reads are verified and this is explicitly flipped.
_WRITES_ENABLED = False


class HttpClientNotReadyError(RuntimeError):
    """Raised when a disabled (write) method is called."""


def _not_ready(operation: str) -> HttpClientNotReadyError:
    return HttpClientNotReadyError(
        f"HttpJoshuClient.{operation}() is not yet enabled.\n"
        f"  _WRITES_ENABLED={_WRITES_ENABLED}\n"
        f"  JOSHU_ENVIRONMENT='{settings.joshu_environment}'\n"
        "  Writes remain disabled until reads are verified and writes are\n"
        "  explicitly enabled in client_http.py."
    )


class HttpJoshuClient(JoshuClientBase):
    """Real HTTP client for altruis.joshu.insure — reads enabled, writes dormant."""

    API_PREFIX = "/api/insurance/v3"

    def __init__(self):
        if not settings.joshu_base_url:
            raise RuntimeError("JOSHU_BASE_URL is required for HttpJoshuClient")

        if settings.is_production and not settings.allow_production:
            raise RuntimeError(
                "Refusing to instantiate HttpJoshuClient against production "
                "without ALTRUIS_ALLOW_PRODUCTION override."
            )

        # Map environment to Joshu's container value. This is the ONLY place
        # the container string is chosen. Callers cannot override it.
        self._container: str = {
            "test": "Test",
            "production": "Production",
        }.get(settings.joshu_environment, "Test")

        self.base_url = settings.joshu_base_url.rstrip("/")
        self.api_token = settings.joshu_api_token

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(30.0, connect=5.0),
        )

        log.info(
            "HttpJoshuClient initialized · base_url=%s · container=%s · writes_enabled=%s",
            self.base_url, self._container, _WRITES_ENABLED,
        )

    # ------------------------------------------------------------------
    # Core request helpers — ALL traffic flows through these
    # ------------------------------------------------------------------

    def _headers(self, bearer_token: str | None = None) -> dict[str, str]:
        """Build auth headers.

        Joshu accepts either:
          - Authorization: Bearer <token>   (from email/password login)
          - Authorization: Token <api_key>  (from pre-generated API token)

        If a real bearer_token is passed (future broker login), prefer it.
        If the caller passes the API_TOKEN_SENTINEL, or None, or an empty
        string, fall back to the statically-configured API token.
        """
        from app.session import API_TOKEN_SENTINEL

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        # Use the bearer token only if it's a real token (not our sentinel)
        real_bearer = (
            bearer_token
            if bearer_token and bearer_token != API_TOKEN_SENTINEL
            else None
        )
        if real_bearer:
            headers["Authorization"] = f"Bearer {real_bearer}"
        elif self.api_token:
            headers["Authorization"] = f"Token {self.api_token}"
        return headers

    def _build_params(self, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        """Inject the ``container`` parameter — SAFETY LINCHPIN.

        The container is fixed at construction time. Extra params from the
        caller are merged but CANNOT override container. Any attempt is
        logged as a safety event and silently ignored.
        """
        params: dict[str, Any] = {}
        if extra:
            for k, v in extra.items():
                if k.lower() == "container":
                    log.error(
                        "SAFETY: A caller attempted to set 'container' parameter. "
                        "Ignored. caller_value=%r enforced_value=%r",
                        v, self._container,
                    )
                    continue
                if v is not None:
                    params[k] = v
        # Container is set LAST so it cannot be stomped by a later update
        params["container"] = self._container
        assert params["container"] == self._container, \
            "Container param was unexpectedly mutated — this is a bug"
        return params

    async def _get(
        self, path: str, *, params: dict[str, Any] | None = None,
        bearer_token: str | None = None,
    ) -> Any:
        """Single choke point for all JSON reads."""
        full_params = self._build_params(params)
        url = f"{self.API_PREFIX}{path}"
        log.debug("GET %s params=%s", url, full_params)
        resp = await self._client.get(
            url, params=full_params, headers=self._headers(bearer_token),
        )
        self._raise_for_status(resp, "GET", url)
        return resp.json()

    async def _get_raw(
        self, path: str, *, params: dict[str, Any] | None = None,
        bearer_token: str | None = None,
    ) -> tuple[bytes, str]:
        """Binary read (for document downloads)."""
        full_params = self._build_params(params)
        url = f"{self.API_PREFIX}{path}"
        resp = await self._client.get(
            url, params=full_params, headers=self._headers(bearer_token),
        )
        self._raise_for_status(resp, "GET", url)
        return resp.content, resp.headers.get("content-type", "application/octet-stream")

    def _raise_for_status(self, resp: httpx.Response, method: str, url: str) -> None:
        """Translate HTTP errors into FastAPI HTTPExceptions."""
        if resp.is_success:
            return
        body_preview = resp.text[:500] if resp.text else ""
        log.warning("Joshu API error · %s %s · status=%d · body=%s",
                    method, url, resp.status_code, body_preview)
        from fastapi import HTTPException
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Joshu API returned {resp.status_code}: {body_preview[:200]}",
        )

    # ------------------------------------------------------------------
    # Auth / user
    # ------------------------------------------------------------------

    async def login(self, email: str, password: str) -> tuple[str, BrokerUser]:
        # The Joshu API v3 reference doesn't document a password-auth
        # endpoint in the sections reviewed. Email/password login is
        # referenced but not detailed — likely a separate auth subsystem.
        # Until we know the login URL, the portal uses a single shared API
        # token; there is no per-broker login against Joshu.
        raise _not_ready("login (Joshu password-auth endpoint not yet documented)")

    async def whoami(self, token: str) -> BrokerUser:
        # Stub — Joshu docs don't expose /me in the sampled sections.
        # When we learn how to fetch the user attached to a token, implement
        # here. For now return a synthetic user so the UI renders.
        return BrokerUser(
            id=0, email="api-token-user@altruis", name="API Token User",
            store_id=None, store_name="Altruis Group",
            role="Portal (via API key)",
        )

    # ------------------------------------------------------------------
    # Products
    # ------------------------------------------------------------------

    async def list_products(self, token: str) -> list[Product]:
        data = await self._get("/products", bearer_token=token)
        items = data.get("items", data) if isinstance(data, dict) else data
        return [Product.model_validate(p) for p in (items or [])]

    async def get_product(self, token: str, product_id: int) -> Product:
        data = await self._get(f"/products/{product_id}", bearer_token=token)
        return Product.model_validate(data)

    # ------------------------------------------------------------------
    # Submissions
    # ------------------------------------------------------------------

    async def list_submissions(
        self, token: str, *, user_id=None, store_id=None, status=None, flow=None,
        page=1, per_page=25,
    ) -> Paginated:
        params: dict[str, Any] = {"_page": page, "_per_page": per_page}
        if user_id is not None: params["user_id"] = user_id
        if store_id is not None: params["store_id"] = store_id
        if status: params["status"] = status
        if flow: params["flow"] = flow
        data = await self._get("/submissions", params=params, bearer_token=token)
        return Paginated.model_validate(data)

    async def get_submission(self, token, submission_id: int) -> Submission:
        data = await self._get(f"/submissions/{submission_id}", bearer_token=token)
        return Submission.model_validate(data)

    async def get_submission_data(self, token, submission_id: int) -> dict[str, Any]:
        data = await self._get(
            f"/submissions/{submission_id}/data", bearer_token=token,
        )
        return data if isinstance(data, dict) else {"raw": data}

    async def update_submission_data(self, token, submission_id, data):
        self._assert_test_mode_for_write()
        raise _not_ready("update_submission_data")

    async def submit_submission(self, token, submission_id) -> Submission:
        self._assert_test_mode_for_write()
        raise _not_ready("submit_submission")

    # ------------------------------------------------------------------
    # Policies
    # ------------------------------------------------------------------

    async def create_policy(self, token: str) -> Policy:
        self._assert_test_mode_for_write()
        raise _not_ready("create_policy")

    async def list_policies(
        self, token, *, status=None, page=1, per_page=25,
    ) -> Paginated:
        params: dict[str, Any] = {"_page": page, "_per_page": per_page}
        if status: params["status"] = status
        data = await self._get("/policies", params=params, bearer_token=token)
        return Paginated.model_validate(data)

    async def get_policy(self, token, policy_id: str) -> Policy:
        data = await self._get(f"/policies/{policy_id}", bearer_token=token)
        return Policy.model_validate(data)

    # ------------------------------------------------------------------
    # Transactions
    # ------------------------------------------------------------------

    async def create_transaction(self, token, **kwargs) -> Transaction:
        self._assert_test_mode_for_write()
        raise _not_ready("create_transaction")

    async def list_transactions(
        self, token, *, policy_id=None, page=1, per_page=25,
    ) -> Paginated:
        params: dict[str, Any] = {"_page": page, "_per_page": per_page}
        if policy_id: params["policy_id"] = policy_id
        data = await self._get("/transactions", params=params, bearer_token=token)
        return Paginated.model_validate(data)

    # ------------------------------------------------------------------
    # Quotes
    # ------------------------------------------------------------------

    async def list_quotes(
        self, token, *, submission_id=None, page=1, per_page=25,
    ) -> Paginated:
        params: dict[str, Any] = {"_page": page, "_per_page": per_page}
        if submission_id: params["submission_id"] = submission_id
        data = await self._get("/quotes", params=params, bearer_token=token)
        return Paginated.model_validate(data)

    async def get_quote(self, token, quote_id: int) -> Quote:
        data = await self._get(f"/quotes/{quote_id}", bearer_token=token)
        return Quote.model_validate(data)

    async def get_quote_data(self, token, quote_id: int) -> dict[str, Any]:
        data = await self._get(f"/quotes/{quote_id}/data", bearer_token=token)
        return data if isinstance(data, dict) else {"raw": data}

    async def update_quote_status(self, token, quote_id: int, status: str) -> Quote:
        self._assert_test_mode_for_write()
        raise _not_ready("update_quote_status")

    # ------------------------------------------------------------------
    # Documents
    # ------------------------------------------------------------------

    async def list_documents(
        self, token, *, quote_id=None, document_type=None, page=1, per_page=25,
    ) -> Paginated:
        params: dict[str, Any] = {"_page": page, "_per_page": per_page}
        if quote_id: params["quote_id"] = quote_id
        if document_type: params["document_type"] = document_type
        data = await self._get("/documents", params=params, bearer_token=token)
        return Paginated.model_validate(data)

    async def get_document(self, token, document_id: int) -> Document:
        data = await self._get(f"/documents/{document_id}", bearer_token=token)
        return Document.model_validate(data)

    async def download_document(self, token, document_id: int) -> tuple[bytes, str]:
        # Joshu's API returns a file_id on the document record. The binary
        # is usually fetched via /documents/{id}/download or a related file
        # endpoint. We try the common pattern first; if Joshu uses a
        # different URL structure, you'll get a 404 and we'll adjust.
        return await self._get_raw(
            f"/documents/{document_id}/download", bearer_token=token,
        )

    # ------------------------------------------------------------------
    # Safety check for writes (unused in read-only phase)
    # ------------------------------------------------------------------

    def _assert_test_mode_for_write(self) -> None:
        """Last-line-of-defense check before any mutating call.

        Runs BEFORE any request is built, so if production mode got
        accidentally flipped without the override, we fail closed.
        """
        if settings.is_production and not settings.allow_production:
            log.critical(
                "BLOCKED write attempt in production without ALTRUIS_ALLOW_PRODUCTION. "
                "This indicates a misconfiguration or bug."
            )
            raise RuntimeError(
                "BLOCKED: production write attempted without override flag. "
                "This is a safety stop — investigate before proceeding."
            )

    async def aclose(self) -> None:
        """Close the underlying HTTPX client. Call on app shutdown."""
        await self._client.aclose()
