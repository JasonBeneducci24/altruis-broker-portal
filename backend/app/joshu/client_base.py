"""
Abstract Joshu client interface.

Both ``MockJoshuClient`` and ``HttpJoshuClient`` implement this contract so
the rest of the portal code is environment-agnostic. Routers depend on this
base class — swap the concrete implementation at app startup via ``config``.

Every mutating method takes an explicit ``test: bool`` parameter defaulting
to ``True`` in non-production builds. The HTTP client will raise if a caller
tries to write with ``test=False`` while running in any non-production mode.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from app.joshu.schemas import (
    Product, Policy, Submission, Quote, Document, Transaction,
    BrokerUser, Paginated, OngoingChange, DocumentType
)


class JoshuClientBase(ABC):
    """Interface implemented by the mock and HTTP Joshu clients."""

    # ---------- Session / auth ----------
    @abstractmethod
    async def login(self, email: str, password: str) -> tuple[str, BrokerUser]:
        """Exchange credentials for a bearer token + broker profile."""

    @abstractmethod
    async def whoami(self, token: str) -> BrokerUser:
        """Return the current user for a bearer token."""

    # ---------- Products ----------
    @abstractmethod
    async def list_products(self, token: str) -> list[Product]:
        ...

    @abstractmethod
    async def get_product(self, token: str, product_id: str | int) -> Product:
        ...

    # ---------- Submissions ----------
    @abstractmethod
    async def list_submissions(
        self,
        token: str,
        *,
        user_id: int | None = None,
        store_id: int | None = None,
        status: str | None = None,
        flow: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> Paginated:
        ...

    async def discover_test_submissions(
        self,
        token: str,
        *,
        page: int = 1,
        per_page: int = 25,
        status_filter: str | None = None,
        flow_filter: str | None = None,
    ) -> dict[str, Any]:
        """Default: fall back to list_submissions for clients that don't
        override this. The HTTP client overrides with the policy-driven
        discovery flow that actually filters by container.
        """
        result = await self.list_submissions(
            token, status=status_filter, flow=flow_filter,
            page=page, per_page=per_page,
        )
        return result.model_dump(mode="json")

    @abstractmethod
    async def get_submission(self, token: str, submission_id: str | int) -> Submission:
        ...

    @abstractmethod
    async def get_submission_data(self, token: str, submission_id: str | int) -> dict[str, Any]:
        """Return the structured data points for a submission (insured.*, app.*, data.*)."""

    @abstractmethod
    async def get_submission_status(self, token: str, submission_id: str | int) -> dict[str, Any]:
        """Return the submission schema + per-field validation/condition state.

        This is the foundation of the dynamic form: it tells us what fields
        exist for this submission's product version, their types, required
        flags, options for dropdowns, conditional visibility, and any
        current validation errors.
        """

    async def get_asset_data(self, token: str, submission_id: str | int) -> Any:
        """Fetch asset-level data (structures, perils, etc).

        Default implementation returns None — concrete clients override this.
        Joshu splits scalar datapoints (/submission-data) and asset-indexed
        datapoints (/asset-data) into two separate endpoints; callers that
        need multi-asset data must call this too.
        """
        return None

    @abstractmethod
    async def update_submission_data(
        self, token: str, submission_id: str | int, data: dict[str, Any],
        *, type_hints: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Save {code: value} data to the submission.

        type_hints is an optional mapping {code: type_tag} where type_tag is
        "Text" / "Number" / "Boolean" / "Date" / "Monetary" / "Location".
        When provided, the value is wrapped into Joshu's union format using
        that specific tag, bypassing Python-type inference.
        """

    @abstractmethod
    async def submit_submission(self, token: str, submission_id: str | int) -> Submission:
        """Move a submission from Incomplete → Submitted (triggers rating)."""

    @abstractmethod
    async def reopen_submission(self, token: str, submission_id: str | int) -> Submission:
        """Move a Submitted/Pending submission back to Incomplete for editing.

        This enables the broker's "Edit & resubmit" workflow. Per Joshu v3,
        the PUT /submissions/{id} endpoint accepts a status change — we
        send status: Incomplete to unlock the form.
        """

    # ---------- Policies ----------
    @abstractmethod
    async def create_policy(self, token: str) -> Policy:
        ...

    @abstractmethod
    async def list_policies(
        self,
        token: str,
        *,
        status: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> Paginated:
        ...

    @abstractmethod
    async def get_policy(self, token: str, policy_id: str) -> Policy:
        ...

    # ---------- Transactions ----------
    @abstractmethod
    async def create_transaction(
        self,
        token: str,
        *,
        flow: OngoingChange,
        policy_id: str,
        product_version_id: int | None = None,
        effective_date: datetime | str | None = None,
    ) -> Transaction:
        ...

    @abstractmethod
    async def list_transactions(
        self, token: str, *, policy_id: str | None = None,
        page: int = 1, per_page: int = 25,
    ) -> Paginated:
        ...

    # ---------- Quotes ----------
    @abstractmethod
    async def list_quotes(
        self, token: str, *, submission_id: str | int | None = None,
        page: int = 1, per_page: int = 25,
    ) -> Paginated:
        ...

    async def discover_test_quotes(
        self, token: str, *,
        page: int = 1, per_page: int = 25,
        status_filter: str | None = None,
    ) -> dict[str, Any]:
        """Default: fall back to list_quotes for clients that don't
        override. The HTTP client overrides with the policy-driven
        discovery flow that actually filters by container.
        """
        result = await self.list_quotes(token, page=page, per_page=per_page)
        payload = result.model_dump(mode="json")
        if status_filter:
            payload["items"] = [
                q for q in (payload.get("items") or [])
                if q.get("status") == status_filter
            ]
        return payload

    @abstractmethod
    async def get_quote(self, token: str, quote_id: str | int) -> Quote:
        ...

    @abstractmethod
    async def get_quote_data(self, token: str, quote_id: str | int) -> dict[str, Any]:
        ...

    @abstractmethod
    async def update_quote_status(self, token: str, quote_id: str | int, status: str) -> Quote:
        """Broker uses this to publish a quote → binder → coverage active."""

    @abstractmethod
    async def create_quote_variation(
        self,
        token: str,
        *,
        parent_quote_id: int,
        parent_submission_id: int,
        overrides: dict[str, Any],
    ) -> dict[str, Any]:
        """Spawn a sibling quote with the parent submission's data plus
        broker overrides on a whitelist of fields. Old quotes remain valid."""

    @abstractmethod
    async def close_quote(self, token: str, quote_id: int) -> dict[str, Any]:
        """Close/void a quote. Quote remains in history but no longer bindable."""

    # ---------- Documents ----------
    @abstractmethod
    async def list_documents(
        self,
        token: str,
        *,
        quote_id: str | int | None = None,
        document_type: DocumentType | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> Paginated:
        ...

    async def discover_test_documents(
        self, token: str, *,
        page: int = 1, per_page: int = 25,
        document_type: str | None = None,
    ) -> dict[str, Any]:
        """Default: fall back to list_documents for clients that don't
        override. The HTTP client overrides with a quote-driven
        discovery flow that filters by container.
        """
        result = await self.list_documents(
            token, document_type=document_type, page=page, per_page=per_page,
        )
        return result.model_dump(mode="json")

    @abstractmethod
    async def get_document(self, token: str, document_id: str | int) -> Document:
        ...

    @abstractmethod
    async def download_document(self, token: str, document_id: str | int) -> tuple[bytes, str]:
        """Return (file_bytes, content_type)."""
