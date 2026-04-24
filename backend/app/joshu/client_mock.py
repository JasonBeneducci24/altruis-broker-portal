"""
In-memory mock of the Joshu API.

Implements the full JoshuClientBase contract with realistic seed data matching
the shapes the real API returns. Used exclusively during initial portal
development so we can build the full broker UX with no network dependencies
on Joshu's test or production containers.

Every mutation stays in-process and resets when the server restarts.
"""
from __future__ import annotations

import asyncio
import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

from app.joshu.client_base import JoshuClientBase
from app.joshu.schemas import (
    Product, ProductVersion, Policy, Submission, Quote, Document,
    Transaction, BrokerUser, Paginated, OngoingChange, DocumentType
)


UTC = timezone.utc
NOW = datetime.now(UTC)


# ------------------------------------------------------------------
# Seed data
# ------------------------------------------------------------------

def _seed() -> dict[str, Any]:
    """Build a realistic Altruis-flavored dataset for the broker to explore."""
    store_id = 101
    store_name = "Acme Surplus Brokers"

    users = {
        "sandbox-token-broker-01": BrokerUser(
            id=5001, email="jane.broker@acmesurplus.com", name="Jane Broker",
            store_id=store_id, store_name=store_name, role="Broker",
        ),
        "sandbox-token-broker-02": BrokerUser(
            id=5002, email="marcus.ortiz@acmesurplus.com", name="Marcus Ortiz",
            store_id=store_id, store_name=store_name, role="Broker",
        ),
        "sandbox-token-admin-01": BrokerUser(
            id=5010, email="admin@acmesurplus.com", name="Agency Admin",
            store_id=store_id, store_name=store_name, role="Agency Admin",
        ),
    }

    products = [
        Product(
            id=201, unique_id=str(uuid.uuid4()),
            name="altruis_cpp_v59",
            display_name="Altruis Commercial Package Policy",
            published=ProductVersion(id=301, version="v59", published_at=NOW - timedelta(days=30)),
            versions=[
                ProductVersion(id=298, version="v57", published_at=NOW - timedelta(days=180)),
                ProductVersion(id=299, version="v58", published_at=NOW - timedelta(days=90)),
                ProductVersion(id=301, version="v59", published_at=NOW - timedelta(days=30)),
            ],
        ),
    ]

    # Build 5 realistic submissions in different states
    def mk_submission(
        sub_id: int, insured_name: str, state: str, status: str,
        days_ago: int, policy_id: str | None, bound: bool = False,
        broker_id: int = 5001,
    ) -> Submission:
        created = NOW - timedelta(days=days_ago)
        sub = Submission(
            id=sub_id,
            unique_id=str(uuid.uuid4()),
            product_version_id=301,
            user_id=broker_id,
            store_id=store_id,
            created_at=created,
            modified_at=created + timedelta(hours=2),
            status=status,
            submitted_at=(created + timedelta(hours=1)) if status != "Incomplete" else None,
            bound_at=(created + timedelta(days=2)) if bound else None,
            test=True,
            policy_id=policy_id,
            flow="New",
            effective_at=created + timedelta(days=7),
            data={
                "insured.name": insured_name,
                "insured.split_address.street1": "100 Sample Ave",
                "insured.split_address.city": "Tampa",
                "insured.split_address.state": state,
                "insured.split_address.zipcode": "33602",
                "app.effective_date": (created + timedelta(days=7)).date().isoformat(),
                "app.named_insured_structure": "LLC",
                "app.aop_deductible": 5000,
                "app.GL_limits": "$1,000,000 / $2,000,000 / $2,000,000",
                "app.cyber_status": 1,
                "app.cyber_limit": 250000,
                "app.equipment_breakdown_status": 1,
                "app.EPLIstatus": 0,
                "app.tria_status": 1,
            },
        )
        return sub

    policies_by_id: dict[str, Policy] = {}
    quotes_by_id: dict[int, Quote] = {}
    docs_by_id: dict[int, Document] = {}
    transactions_by_id: dict[str, Transaction] = {}

    sub_defs = [
        # (id, insured, state, status, days_ago, has_policy?, bound?, broker)
        (1001, "Riverview Holdings LLC", "FL", "Submitted", 3, False, False, 5001),
        (1002, "Gulf Coast Properties LLC", "FL", "Pending", 5, False, False, 5001),
        (1003, "Orlando Retail Partners", "FL", "Incomplete", 1, False, False, 5002),
        (1004, "Sunshine Industrial LLC", "FL", "Submitted", 10, True, True, 5001),
        (1005, "Bay Area Services Inc", "FL", "Declined", 21, False, False, 5002),
    ]

    submissions: dict[int, Submission] = {}
    for (sid, name, state_code, st, days, has_policy, bound, broker) in sub_defs:
        pid = None
        if has_policy:
            pid = str(uuid.uuid4())
            policies_by_id[pid] = Policy(
                id=pid,
                created_at=NOW - timedelta(days=days - 2),
                user_id=broker,
                insured_id=9000 + sid,
                insured_name=name,
                status="Active" if bound else "Future",
                last_modified=NOW - timedelta(days=days - 2),
                product_version_id=301,
                product_name="Altruis Commercial Package Policy",
                effective_date=NOW + timedelta(days=max(1, 7 - days)),
                renews_at=NOW + timedelta(days=365 + max(1, 7 - days)),
            )
        submissions[sid] = mk_submission(sid, name, state_code, st, days, pid, bound, broker)

    # Add two quotes each for 1001, 1002 (version iterations) and one for 1004
    def mk_quote(qid: int, sub_id: int, status: str, total: float, days_ago: int) -> Quote:
        return Quote(
            id=qid, unique_id=str(uuid.uuid4()), submission_id=sub_id,
            created_at=NOW - timedelta(days=days_ago),
            modified_at=NOW - timedelta(days=days_ago) + timedelta(hours=1),
            quote_expires_at=NOW + timedelta(days=30),
            status=status, user_id=5001,
            data={
                "property_premium": round(total * 0.30, 2),
                "gl_premium": round(total * 0.55, 2),
                "eb_premium": round(total * 0.05, 2),
                "cyber_premium": round(total * 0.05, 2),
                "epli_premium": 0.0,
                "tria_premium": round(total * 0.01, 2),
                "total_premium": round(total, 2),
                "total_taxes": round(total * 0.05, 2),
                "total_fees": 300.0,
                "total_invoice": round(total * 1.05 + 300, 2),
            },
        )

    quote_defs = [
        (701, 1001, "QuotePublished", 14_250.00, 2),
        (702, 1002, "QuotePending", 31_900.00, 3),
        (703, 1002, "QuotePublished", 29_450.00, 1),   # requote
        (704, 1004, "CoverageActive", 48_100.00, 8),
    ]
    for (qid, sid, st, total, days) in quote_defs:
        quotes_by_id[qid] = mk_quote(qid, sid, st, total, days)

    # Documents for quote 704 (bound policy) and 701 (published quote)
    def mk_doc(did: int, qid: int, dtype: DocumentType, name: str, days_ago: int) -> Document:
        return Document(
            id=did, unique_id=str(uuid.uuid4()), name=name,
            created_time=NOW - timedelta(days=days_ago),
            quote_id=qid, document_type=dtype, status="Ready",
            file_id=str(uuid.uuid4()), code=dtype.lower(),
        )

    doc_defs = [
        (8001, 701, "NewQuote", "Quote Letter — Riverview Holdings LLC", 2),
        (8002, 704, "Binder", "Binder — Sunshine Industrial LLC", 8),
        (8003, 704, "Policy", "Policy Dec Page — Sunshine Industrial LLC", 6),
        (8004, 704, "Application", "Signed Application — Sunshine Industrial", 10),
    ]
    for args in doc_defs:
        docs_by_id[args[0]] = mk_doc(*args)

    # Transactions for the bound policy
    policy_1004_id = submissions[1004].policy_id
    if policy_1004_id:
        tx_id = str(uuid.uuid4())
        transactions_by_id[tx_id] = Transaction(
            id=tx_id,
            created_at=NOW - timedelta(days=8),
            policy_id=policy_1004_id,
            flow="New",
            effective_at=NOW + timedelta(days=1),
            expires_at=NOW + timedelta(days=366),
            status="Completed",
            latest_submission_id=1004,
        )

    return {
        "users": users,
        "products": products,
        "submissions": submissions,
        "quotes": quotes_by_id,
        "policies": policies_by_id,
        "documents": docs_by_id,
        "transactions": transactions_by_id,
        "next_submission_id": 1100,
        "next_quote_id": 710,
        "next_document_id": 8100,
    }


# ------------------------------------------------------------------
# Mock client
# ------------------------------------------------------------------

class MockJoshuClient(JoshuClientBase):
    """In-memory mock used during initial portal development."""

    def __init__(self):
        self._data = _seed()
        self._lock = asyncio.Lock()

    def _require_user(self, token: str) -> BrokerUser:
        user = self._data["users"].get(token)
        if not user:
            from fastapi import HTTPException
            raise HTTPException(401, "Invalid or expired session")
        return user

    # ---------- Auth ----------
    async def login(self, email: str, password: str) -> tuple[str, BrokerUser]:
        # In mock mode we accept any password for the seeded emails
        for token, user in self._data["users"].items():
            if user.email.lower() == email.lower():
                return token, user
        from fastapi import HTTPException
        raise HTTPException(401, "Invalid email or password")

    async def whoami(self, token: str) -> BrokerUser:
        return self._require_user(token)

    # ---------- Products ----------
    async def list_products(self, token: str) -> list[Product]:
        self._require_user(token)
        return list(self._data["products"])

    async def get_product(self, token: str, product_id: int) -> Product:
        self._require_user(token)
        for p in self._data["products"]:
            if p.id == product_id:
                return p
        from fastapi import HTTPException
        raise HTTPException(404, "Product not found")

    # ---------- Submissions ----------
    async def list_submissions(
        self, token: str, *, user_id=None, store_id=None, status=None, flow=None,
        page=1, per_page=25,
    ) -> Paginated:
        user = self._require_user(token)
        subs = list(self._data["submissions"].values())
        # Scope to the broker's store by default (this is the whole point of
        # "brokers see the agency's submissions" — Joshu does this server-side
        # in reality; the mock mirrors it here).
        subs = [s for s in subs if s.store_id == user.store_id]
        if user_id:
            subs = [s for s in subs if s.user_id == user_id]
        if status:
            subs = [s for s in subs if s.status == status]
        if flow:
            subs = [s for s in subs if s.flow == flow]
        subs.sort(key=lambda s: s.created_at, reverse=True)
        total = len(subs)
        start = (page - 1) * per_page
        page_items = subs[start:start + per_page]
        return Paginated(
            page=page, per_page=per_page, total_items=total,
            total_pages=max(1, (total + per_page - 1) // per_page),
            items=[s.model_dump(mode="json") for s in page_items],
        )

    async def get_submission(self, token: str, submission_id: int) -> Submission:
        self._require_user(token)
        sub = self._data["submissions"].get(submission_id)
        if not sub:
            from fastapi import HTTPException
            raise HTTPException(404, "Submission not found")
        return sub

    async def get_submission_data(self, token: str, submission_id: int) -> dict[str, Any]:
        sub = await self.get_submission(token, submission_id)
        return deepcopy(sub.data or {})

    async def get_submission_status(self, token, submission_id) -> dict[str, Any]:
        """Mock schema mirroring what Joshu's /submission-status returns.

        This is a simplified version of the Altruis CPP product schema so
        the portal's form renderer can be exercised in mock mode. The real
        client returns Joshu's actual schema for the specific product version.
        """
        sub = await self.get_submission(token, submission_id)

        def dp(code, kind, required=False, condition_met=True, value=None):
            d = {
                "code": code, "asset_idx": 0, "required": required,
                "condition_met": condition_met, "exists": value is not None,
                "validation_issue": None, "kind": kind,
            }
            return d

        limit_options = [
            {"value": "250000", "display": "$250,000"},
            {"value": "500000", "display": "$500,000"},
            {"value": "1000000", "display": "$1,000,000"},
        ]
        aop_options = [
            {"value": "2500", "display": "$2,500"},
            {"value": "5000", "display": "$5,000"},
            {"value": "10000", "display": "$10,000"},
        ]
        gl_limit_options = [
            {"value": "1m/2m/2m", "display": "$1M / $2M / $2M"},
            {"value": "2m/4m/4m", "display": "$2M / $4M / $4M"},
        ]
        structure_options = [
            {"value": "LLC", "display": "LLC"},
            {"value": "Corp", "display": "Corporation"},
            {"value": "Partnership", "display": "Partnership"},
            {"value": "Sole", "display": "Sole Proprietorship"},
        ]

        datapoints = [
            # Insured section
            dp("insured.name", {"Text": {}}, required=True,
               value=sub.data.get("insured.name") if sub.data else None),
            dp("insured.split_address.street1", {"Text": {}}, required=True),
            dp("insured.split_address.city", {"Text": {}}, required=True),
            dp("insured.split_address.state", {"Text": {"options": [
                {"value": s, "display": s} for s in ["FL", "GA", "TX", "CA", "NY"]
            ]}}, required=True),
            dp("insured.split_address.zipcode", {"Text": {}}, required=True),
            dp("insured.phone", {"Text": {"format": "PhoneNumber"}}, required=False),
            dp("insured.email", {"Text": {"format": "EmailAddress"}}, required=False),
            # Application section
            dp("app.effective_date", {"Date": {"format": "MonthDayYear"}}, required=True),
            dp("app.named_insured_structure", {"Text": {"options": structure_options}}, required=True),
            dp("app.aop_deductible", {"Number": {"options": aop_options, "format": {"decimal_places": 0}}}, required=True),
            dp("app.GL_limits", {"Text": {"options": gl_limit_options}}, required=True),
            dp("app.cyber_status", {"Boolean": {}}, required=True),
            dp("app.cyber_limit", {"Number": {"options": limit_options, "format": {"decimal_places": 0}}},
               required=True, condition_met=bool(sub.data and sub.data.get("app.cyber_status"))),
            dp("app.equipment_breakdown_status", {"Boolean": {}}, required=True),
            dp("app.EPLIstatus", {"Boolean": {}}, required=True),
            dp("app.EPLI_limit", {"Number": {"options": limit_options, "format": {"decimal_places": 0}}},
               required=True, condition_met=bool(sub.data and sub.data.get("app.EPLIstatus"))),
            dp("app.tria_status", {"Boolean": {}}, required=False),
        ]
        return {"datapoints": datapoints}

    async def update_submission_data(self, token, submission_id, data, *, type_hints=None):
        async with self._lock:
            sub = await self.get_submission(token, submission_id)
            merged = dict(sub.data or {})
            merged.update(data)
            sub.data = merged
            sub.modified_at = datetime.now(UTC)
            return deepcopy(merged)

    async def submit_submission(self, token, submission_id) -> Submission:
        async with self._lock:
            sub = await self.get_submission(token, submission_id)
            sub.status = "Submitted"
            sub.submitted_at = datetime.now(UTC)
            sub.modified_at = sub.submitted_at
            # Auto-create a quote (mirrors real Joshu workflow)
            qid = self._data["next_quote_id"]
            self._data["next_quote_id"] += 1
            total = 25_000 + (submission_id % 100) * 250
            self._data["quotes"][qid] = Quote(
                id=qid, unique_id=str(uuid.uuid4()),
                submission_id=submission_id,
                created_at=datetime.now(UTC),
                quote_expires_at=datetime.now(UTC) + timedelta(days=30),
                status="QuotePending",
                user_id=sub.user_id,
                data={
                    "property_premium": round(total * 0.30, 2),
                    "gl_premium": round(total * 0.55, 2),
                    "eb_premium": round(total * 0.05, 2),
                    "cyber_premium": round(total * 0.05, 2),
                    "epli_premium": 0.0,
                    "tria_premium": round(total * 0.01, 2),
                    "total_premium": round(total, 2),
                    "total_taxes": round(total * 0.05, 2),
                    "total_fees": 300.0,
                    "total_invoice": round(total * 1.05 + 300, 2),
                },
            )
            return sub

    async def reopen_submission(self, token, submission_id) -> Submission:
        """Unlock a Submitted/Pending mock submission for editing."""
        async with self._lock:
            sub = await self.get_submission(token, submission_id)
            sub.status = "Incomplete"
            sub.submitted_at = None
            sub.modified_at = datetime.now(UTC)
            return sub

    # ---------- Policies ----------
    async def create_policy(self, token: str) -> Policy:
        async with self._lock:
            user = self._require_user(token)
            pid = str(uuid.uuid4())
            p = Policy(
                id=pid, created_at=datetime.now(UTC),
                user_id=user.id, status="Incomplete",
                last_modified=datetime.now(UTC),
                product_version_id=301,
                product_name="Altruis Commercial Package Policy",
            )
            self._data["policies"][pid] = p
            return p

    async def list_policies(self, token, *, status=None, page=1, per_page=25) -> Paginated:
        user = self._require_user(token)
        # Scope: all submissions in the broker's store with a policy_id
        submissions_in_store = [
            s for s in self._data["submissions"].values() if s.store_id == user.store_id
        ]
        policy_ids_in_store = {s.policy_id for s in submissions_in_store if s.policy_id}
        policies = [p for pid, p in self._data["policies"].items() if pid in policy_ids_in_store]
        if status:
            policies = [p for p in policies if p.status == status]
        policies.sort(key=lambda p: p.created_at, reverse=True)
        total = len(policies)
        start = (page - 1) * per_page
        page_items = policies[start:start + per_page]
        return Paginated(
            page=page, per_page=per_page, total_items=total,
            total_pages=max(1, (total + per_page - 1) // per_page),
            items=[p.model_dump(mode="json") for p in page_items],
        )

    async def get_policy(self, token, policy_id: str) -> Policy:
        self._require_user(token)
        p = self._data["policies"].get(policy_id)
        if not p:
            from fastapi import HTTPException
            raise HTTPException(404, "Policy not found")
        return p

    # ---------- Transactions ----------
    async def create_transaction(
        self, token, *, flow, policy_id, product_version_id=None, effective_date=None,
    ) -> Transaction:
        async with self._lock:
            self._require_user(token)
            tx_id = str(uuid.uuid4())
            # Create a fresh submission tied to this transaction (mirrors Joshu v3 flow)
            sid = self._data["next_submission_id"]
            self._data["next_submission_id"] += 1

            policy = await self.get_policy(token, policy_id)
            user = self._require_user(token)
            sub = Submission(
                id=sid, unique_id=str(uuid.uuid4()),
                product_version_id=product_version_id or 301,
                user_id=user.id, store_id=user.store_id,
                created_at=datetime.now(UTC),
                modified_at=datetime.now(UTC),
                status="Incomplete", test=True, policy_id=policy_id,
                flow=flow,
                effective_at=(
                    effective_date if isinstance(effective_date, datetime)
                    else datetime.now(UTC) + timedelta(days=7)
                ),
                data={},
            )
            self._data["submissions"][sid] = sub

            tx = Transaction(
                id=tx_id, created_at=datetime.now(UTC),
                policy_id=policy_id, flow=flow,
                effective_at=sub.effective_at or datetime.now(UTC),
                expires_at=(sub.effective_at or datetime.now(UTC)) + timedelta(days=365),
                status="Ongoing", latest_submission_id=sid,
            )
            self._data["transactions"][tx_id] = tx
            return tx

    async def list_transactions(self, token, *, policy_id=None, page=1, per_page=25) -> Paginated:
        self._require_user(token)
        txs = list(self._data["transactions"].values())
        if policy_id:
            txs = [t for t in txs if t.policy_id == policy_id]
        txs.sort(key=lambda t: t.created_at, reverse=True)
        total = len(txs)
        start = (page - 1) * per_page
        page_items = txs[start:start + per_page]
        return Paginated(
            page=page, per_page=per_page, total_items=total,
            total_pages=max(1, (total + per_page - 1) // per_page),
            items=[t.model_dump(mode="json") for t in page_items],
        )

    # ---------- Quotes ----------
    async def list_quotes(self, token, *, submission_id=None, page=1, per_page=25) -> Paginated:
        user = self._require_user(token)
        quotes = list(self._data["quotes"].values())
        # Scope to the broker's store via submission
        scoped = []
        for q in quotes:
            sub = self._data["submissions"].get(q.submission_id)
            if sub and sub.store_id == user.store_id:
                scoped.append(q)
        if submission_id:
            scoped = [q for q in scoped if q.submission_id == submission_id]
        scoped.sort(key=lambda q: q.created_at, reverse=True)
        total = len(scoped)
        start = (page - 1) * per_page
        page_items = scoped[start:start + per_page]
        return Paginated(
            page=page, per_page=per_page, total_items=total,
            total_pages=max(1, (total + per_page - 1) // per_page),
            items=[q.model_dump(mode="json") for q in page_items],
        )

    async def get_quote(self, token, quote_id: int) -> Quote:
        self._require_user(token)
        q = self._data["quotes"].get(quote_id)
        if not q:
            from fastapi import HTTPException
            raise HTTPException(404, "Quote not found")
        return q

    async def get_quote_data(self, token, quote_id: int) -> dict[str, Any]:
        q = await self.get_quote(token, quote_id)
        return deepcopy(q.data or {})

    async def update_quote_status(self, token, quote_id: int, status: str) -> Quote:
        async with self._lock:
            q = await self.get_quote(token, quote_id)
            q.status = status  # type: ignore[assignment]
            q.modified_at = datetime.now(UTC)
            return q

    # ---------- Documents ----------
    async def list_documents(
        self, token, *, quote_id=None, document_type=None, page=1, per_page=25,
    ) -> Paginated:
        user = self._require_user(token)
        docs = list(self._data["documents"].values())
        # Scope via quote → submission → store
        scoped = []
        for d in docs:
            q = self._data["quotes"].get(d.quote_id)
            if not q:
                continue
            sub = self._data["submissions"].get(q.submission_id)
            if sub and sub.store_id == user.store_id:
                scoped.append(d)
        if quote_id:
            scoped = [d for d in scoped if d.quote_id == quote_id]
        if document_type:
            scoped = [d for d in scoped if d.document_type == document_type]
        scoped.sort(key=lambda d: d.created_time, reverse=True)
        total = len(scoped)
        start = (page - 1) * per_page
        page_items = scoped[start:start + per_page]
        return Paginated(
            page=page, per_page=per_page, total_items=total,
            total_pages=max(1, (total + per_page - 1) // per_page),
            items=[d.model_dump(mode="json") for d in page_items],
        )

    async def get_document(self, token, document_id: int) -> Document:
        self._require_user(token)
        d = self._data["documents"].get(document_id)
        if not d:
            from fastapi import HTTPException
            raise HTTPException(404, "Document not found")
        return d

    async def download_document(self, token, document_id: int) -> tuple[bytes, str]:
        doc = await self.get_document(token, document_id)
        # Return a tiny placeholder PDF — just enough for the browser to open
        placeholder = self._make_placeholder_pdf(doc.name or doc.code or f"Document {doc.id}")
        return placeholder, "application/pdf"

    @staticmethod
    def _make_placeholder_pdf(title: str) -> bytes:
        """A minimal, valid 1-page PDF so downloads render in the browser."""
        # Simplest possible hand-written PDF — avoids pulling in reportlab here
        content = f"BT /F1 14 Tf 72 720 Td ({title[:80]}) Tj ET\nBT /F1 10 Tf 72 700 Td (Mock document — generated in portal dev mode) Tj ET"
        content_bytes = content.encode("latin-1", errors="replace")
        pdf = (
            b"%PDF-1.4\n"
            b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
            b"2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj\n"
            b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]"
            b"/Resources<</Font<</F1 4 0 R>>>>/Contents 5 0 R>>endobj\n"
            b"4 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n"
            b"5 0 obj<</Length " + str(len(content_bytes)).encode() + b">>\n"
            b"stream\n" + content_bytes + b"\nendstream\nendobj\n"
            b"xref\n0 6\n0000000000 65535 f \n"
            b"trailer<</Size 6/Root 1 0 R>>\nstartxref\n0\n%%EOF\n"
        )
        return pdf
