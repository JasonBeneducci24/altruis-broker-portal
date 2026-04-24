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

    @abstractmethod
    async def get_submission(self, token: str, submission_id: str | int) -> Submission:
        ...

    @abstractmethod
    async def get_submission_data(self, token: str, submission_id: str | int) -> dict[str, Any]:
        """Return the structured data points for a submission (insured.*, app.*, data.*)."""

    @abstractmethod
    async def update_submission_data(
        self, token: str, submission_id: str | int, data: dict[str, Any]
    ) -> dict[str, Any]:
        ...

    @abstractmethod
    async def submit_submission(self, token: str, submission_id: str | int) -> Submission:
        """Move a submission from Incomplete → Submitted (triggers rating)."""

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

    @abstractmethod
    async def get_quote(self, token: str, quote_id: str | int) -> Quote:
        ...

    @abstractmethod
    async def get_quote_data(self, token: str, quote_id: str | int) -> dict[str, Any]:
        ...

    @abstractmethod
    async def update_quote_status(self, token: str, quote_id: str | int, status: str) -> Quote:
        """Broker uses this to publish a quote → binder → coverage active."""

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

    @abstractmethod
    async def get_document(self, token: str, document_id: str | int) -> Document:
        ...

    @abstractmethod
    async def download_document(self, token: str, document_id: str | int) -> tuple[bytes, str]:
        """Return (file_bytes, content_type)."""
