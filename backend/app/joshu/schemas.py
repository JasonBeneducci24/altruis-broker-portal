"""
Pydantic schemas for Joshu API v3 resources.

Shapes taken directly from the Joshu Platform API PDF (April 2026 edition):
  - Insureds, Products, Policies, Submissions, Quotes, Documents, Transactions.

These are the objects the portal consumes and renders. They're deliberately
permissive (Optional everywhere the docs say `| null`) because the Joshu API
is an upstream we don't control — new fields should flow through without
breaking the portal.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional
from pydantic import BaseModel, ConfigDict, Field


# ------------------------------------------------------------------
# Shared paginated wrapper
# ------------------------------------------------------------------

class Paginated(BaseModel):
    """Standard Joshu paginated envelope."""
    page: int
    per_page: int
    total_items: int
    total_pages: int
    items: list[Any]


# ------------------------------------------------------------------
# Product
# ------------------------------------------------------------------

class ProductVersion(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: int
    unique_id: str | None = None
    version: str | None = None
    effective_from: datetime | None = None
    published_at: datetime | None = None


class Product(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: int
    unique_id: str | None = None
    name: str
    display_name: str | None = None
    published: ProductVersion | None = None
    versions: list[ProductVersion] = Field(default_factory=list)


# ------------------------------------------------------------------
# Policy
# ------------------------------------------------------------------

PolicyStatus = Literal[
    "Incomplete", "Future", "Active", "Canceled", "Declined", "Expired"
]

OngoingChange = Literal[
    "New", "FlatCancellation", "ManualCancellation", "Endorsement",
    "Renewal", "CancellationReissuance", "Reinstatement"
]


class Policy(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str  # uuid
    created_at: datetime
    user_id: int
    insured_id: int | None = None
    insured_name: str | None = None
    status: PolicyStatus
    ongoing_change: OngoingChange | None = None
    ongoing_change_submission_id: int | None = None
    ongoing_change_user_role: str | None = None
    last_modified: datetime | None = None
    product_version_id: int | None = None
    product_name: str | None = None
    effective_date: datetime | None = None
    pending_change: OngoingChange | None = None
    latest_active_transaction_id: str | None = None
    renews_at: datetime | None = None


# ------------------------------------------------------------------
# Submission
# ------------------------------------------------------------------

SubmissionStatus = Literal[
    "Incomplete", "Declined", "Pending", "Submitted", "Blocked", "Error"
]


class Submission(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: int
    unique_id: str | None = None
    product_version_id: int
    product_version_revision_id: str | None = None
    user_id: int
    underwriter_id: int | None = None
    store_id: int | None = None
    opened_by_underwriter_at: datetime | None = None
    created_at: datetime
    submitted_at: datetime | None = None
    modified_at: datetime | None = None
    status: SubmissionStatus
    bound_at: datetime | None = None
    bind_request_at: datetime | None = None
    declined_at: datetime | None = None
    declined_by: int | None = None
    decline_reason: str | None = None
    external_id: str | None = None
    original_submission_id: int | None = None
    clearance_approval_reason: str | None = None
    processing: bool = False
    additional_details: str | None = None
    test: bool = False
    policy_id: str | None = None
    transaction_id: str | None = None
    flow: OngoingChange
    effective_at: datetime | None = None
    # Structured data (may not be present on list responses from Joshu)
    data: dict[str, Any] | None = None
    # Clearance hash contains the list of datapoint names (not values)
    clearance_hash: dict[str, Any] | None = None


# ------------------------------------------------------------------
# Quote
# ------------------------------------------------------------------

QuoteStatus = Literal[
    "QuoteStoreEdit", "QuotePending", "QuoteDeclined", "QuotePublished",
    "QuoteClosed", "BinderPending", "BinderPublished", "BinderDeclined",
    "CoveragePending", "CoverageActive", "Error", "QuoteIndication"
]


class Quote(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: int
    unique_id: str | None = None
    submission_id: int
    created_at: datetime
    quote_expires_at: datetime | None = None
    authority_level: int | None = None
    status: QuoteStatus
    external_id: str | None = None
    modified_at: datetime | None = None
    user_id: int
    additional_details: str | None = None
    rater_file_id: str | None = None
    processing: bool = False
    has_error: bool = False
    documents_generation: int | None = None
    # Product-specific rated data
    data: dict[str, Any] | None = None


# ------------------------------------------------------------------
# Document
# ------------------------------------------------------------------

DocumentType = Literal[
    "Application", "NewQuote", "Binder", "Policy",
    "EndorsementQuote", "RenewalQuote", "Cancellation"
]
DocumentStatus = Literal["Processing", "Ready", "UnmetCondition", "Error"]


class Document(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: int
    unique_id: str | None = None
    name: str | None = None
    created_time: datetime
    quote_id: int
    document_type: DocumentType
    updated_time: datetime | None = None
    status: DocumentStatus
    error: str | None = None
    file_id: str | None = None
    code: str | None = None
    preview: bool = False
    segment_id: str | None = None


# ------------------------------------------------------------------
# Transaction
# ------------------------------------------------------------------

TransactionStatus = Literal["Ongoing", "Completed", "Closed"]


class Transaction(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    created_at: datetime
    modified_at: datetime | None = None
    policy_id: str
    flow: OngoingChange
    effective_at: datetime
    expires_at: datetime | None = None
    status: TransactionStatus
    latest_submission_id: int | None = None


# ------------------------------------------------------------------
# User / Store (synthesized from submission fields)
# ------------------------------------------------------------------

class BrokerUser(BaseModel):
    """Synthesized view of a logged-in broker.

    Joshu itself doesn't expose /me explicitly in the docs I reviewed, so the
    portal's session layer will resolve this from the token + a probe request.
    """
    model_config = ConfigDict(extra="allow")

    id: int
    email: str
    name: str
    store_id: int | None = None
    store_name: str | None = None
    role: str | None = None  # Broker, Underwriter, etc.
