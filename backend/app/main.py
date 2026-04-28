"""
Altruis Broker Portal — main FastAPI app.

Run locally:
    cd backend
    pip install -r requirements.txt
    JOSHU_ENVIRONMENT=mock uvicorn app.main:app --reload --port 8001

Then open http://localhost:8001/

In mock mode the portal uses an in-memory dataset — no network calls to Joshu.
"""
from __future__ import annotations

from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse

# Import config FIRST so the environment guardrail runs before anything else
from app.config import settings
from app.routers import auth, submissions, quotes, policies, documents, products


app = FastAPI(
    title="Altruis Broker Portal",
    description=(
        "Broker-facing portal for Altruis Group. Submissions, quotes, "
        "policies, documents — all backed by the Joshu Insurance API."
    ),
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8001", "http://127.0.0.1:8001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(auth.router)
app.include_router(submissions.router)
app.include_router(quotes.router)
app.include_router(policies.router)
app.include_router(documents.router)
app.include_router(products.router)


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "service": "altruis-broker-portal",
        "environment": settings.joshu_environment,
    }


@app.get("/api/config")
def public_config():
    """Public config the frontend needs to know (no secrets)."""
    return {
        "environment": settings.joshu_environment,
        "is_mock": settings.is_mock,
        "is_test": settings.is_test,
        "is_production": settings.is_production,
    }


@app.get("/api/diagnostics")
def diagnostics():
    """Shows how outbound Joshu requests will be constructed, without making one.

    Useful as a pre-flight check: confirm the container parameter, base URL,
    and auth scheme are correct BEFORE the first real call.
    """
    from app.joshu.factory import get_joshu_client
    client = get_joshu_client()
    if settings.is_mock:
        return {
            "mode": "mock",
            "note": "In mock mode — no outbound HTTP. Seed data only.",
        }
    # http client — show what a sample request would look like
    sample_path = "/api/insurance/v3/submissions"
    sample_params = client._build_params({"_page": 1, "_per_page": 25})  # type: ignore[attr-defined]
    sample_headers = client._headers()  # type: ignore[attr-defined]
    # Redact the auth token — show only the scheme and prefix
    auth = sample_headers.get("Authorization", "")
    if auth:
        parts = auth.split(" ", 1)
        if len(parts) == 2:
            scheme, tok = parts
            redacted = f"{scheme} {tok[:8]}…{tok[-4:] if len(tok) > 12 else ''}"
            sample_headers["Authorization"] = redacted

    from app.joshu import client_http as _ch
    writes = {
        "update_submission_data": _ch._ENABLE_UPDATE_SUBMISSION_DATA,
        "update_submission": _ch._ENABLE_UPDATE_SUBMISSION,
        "create_policy": _ch._ENABLE_CREATE_POLICY,
        "create_transaction": _ch._ENABLE_CREATE_TRANSACTION,
        "update_quote": _ch._ENABLE_UPDATE_QUOTE,
    }

    # Show how the params actually serialize on the wire — Python's
    # `True` becomes `"True"` (capital T) in URLs, which Joshu may not
    # parse as the boolean.
    serialized = {}
    for k, v in sample_params.items():
        serialized[k] = {"python_repr": repr(v), "python_type": type(v).__name__}
    try:
        import httpx as _httpx
        sample_request = _httpx.Request("GET", f"{settings.joshu_base_url}{sample_path}", params=sample_params)
        sample_serialized_url = str(sample_request.url)
    except Exception as e:
        sample_serialized_url = f"<error: {e}>"

    return {
        "mode": settings.joshu_environment,
        "base_url": settings.joshu_base_url,
        "api_prefix": getattr(client, "API_PREFIX", "/api/insurance/v3"),
        "container": getattr(client, "_container", "unknown"),
        "test_filter": getattr(client, "_test_filter", None),
        "sample_url": f"{settings.joshu_base_url}{sample_path}",
        "sample_params": sample_params,
        "sample_serialized_url": sample_serialized_url,
        "param_types": serialized,
        "sample_headers": sample_headers,
        "writes_enabled": writes,
        "note": "No request was made. This is a description of how the next request will be built.",
    }


@app.get("/api/diagnostics/write-construction")
def diagnostics_write_construction():
    """Show what the URLs and params would look like for write requests
    (POST /policies, POST /transactions) WITHOUT making them.

    Used to verify that the container parameter is correctly applied to
    write requests before re-enabling the create flow. The previous
    build had a bug where _build_params only added container on list
    endpoints, leaving writes unscoped — which caused a write to land
    in production despite a test-mode portal.
    """
    if settings.is_mock:
        return {"mode": "mock"}

    from app.joshu.factory import get_joshu_client
    import httpx as _httpx
    client = get_joshu_client()

    # POST /policies — empty body, no caller params
    policies_params = client._build_params({}, list_endpoint=False)  # type: ignore[attr-defined]
    policies_url = f"{settings.joshu_base_url}/api/insurance/v3/policies"
    policies_serialized = str(_httpx.Request("POST", policies_url, params=policies_params).url)

    # POST /transactions — caller provides body, params still go through _build_params
    txn_params = client._build_params({}, list_endpoint=False)  # type: ignore[attr-defined]
    txn_url = f"{settings.joshu_base_url}/api/insurance/v3/transactions"
    txn_serialized = str(_httpx.Request("POST", txn_url, params=txn_params).url)
    txn_body_shape = {
        "New": {
            "product_version_id": "<int — broker-selected>",
            "policy_id": "<uuid — from prior POST /policies>",
            "test": getattr(client, "_test_filter", None),
        }
    }

    return {
        "mode": settings.joshu_environment,
        "test_filter": getattr(client, "_test_filter", None),
        "container_label": getattr(client, "_mode_label", None),
        "post_policies": {
            "method": "POST",
            "url": policies_serialized,
            "params": policies_params,
            "body": None,
            "expected_container_in_url": "Test" in policies_serialized.split("?")[-1],
        },
        "post_transactions": {
            "method": "POST",
            "url": txn_serialized,
            "params": txn_params,
            "body_shape": txn_body_shape,
            "expected_container_in_url": "Test" in txn_serialized.split("?")[-1],
        },
        "writes_currently_enabled": {
            "create_policy": __import__("app.joshu.client_http", fromlist=["_ENABLE_CREATE_POLICY"])._ENABLE_CREATE_POLICY,
            "create_transaction": __import__("app.joshu.client_http", fromlist=["_ENABLE_CREATE_TRANSACTION"])._ENABLE_CREATE_TRANSACTION,
        },
        "note": "No request was made. If `expected_container_in_url` is true on both, the fix is working and writes will land in the correct container.",
    }


@app.get("/api/diagnostics/products-live")
async def diagnostics_products_live():
    """Show the raw response from Joshu's /products endpoint so we can see
    why some products are missing from the broker portal's picker.

    The picker filters on `published` being non-null. If a product has
    no `published` ProductVersion, it's hidden. This endpoint surfaces
    the full list (with version structure) so we can verify whether
    missing products are unpublished, archived, or named differently.
    """
    if settings.is_mock:
        return {"mode": "mock"}

    from app.joshu.factory import get_joshu_client
    import httpx as _httpx
    client = get_joshu_client()
    headers = client._headers()  # type: ignore[attr-defined]

    # Try the basic call first
    url = f"{settings.joshu_base_url}/api/insurance/v3/products"
    try:
        async with _httpx.AsyncClient(timeout=30.0) as http:
            # Default call (with whatever filters our _build_params adds)
            params_default = client._build_params({})  # type: ignore[attr-defined]
            resp_default = await http.get(url, params=params_default, headers=headers)
            # Try with is_archived=false to see if there's a difference
            resp_archived = await http.get(url, params={**params_default, "is_archived": "false"}, headers=headers)
            # Try without container filter at all (raw)
            resp_raw = await http.get(url, params={"_per_page": 50}, headers=headers)
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

    def summarize(resp):
        out = {"status": resp.status_code, "url": str(resp.url)}
        try:
            body = resp.json()
        except Exception:
            return {**out, "body_preview": resp.text[:300]}
        items = body.get("items", body) if isinstance(body, dict) else body
        out["item_count"] = len(items) if isinstance(items, list) else None
        if isinstance(items, list):
            out["items"] = []
            for p in items:
                if not isinstance(p, dict):
                    continue
                out["items"].append({
                    "id": p.get("id"),
                    "name": p.get("name"),
                    "display_name": p.get("display_name"),
                    "is_archived": p.get("is_archived"),
                    "container": p.get("container"),
                    "published_present": p.get("published") is not None,
                    "published_id": (p.get("published") or {}).get("id") if isinstance(p.get("published"), dict) else None,
                    "versions_count": len(p.get("versions") or []) if isinstance(p.get("versions"), list) else None,
                    "all_top_level_keys": list(p.keys()),
                })
        return out

    return {
        "default_call": summarize(resp_default),
        "with_is_archived_false": summarize(resp_archived),
        "no_container_filter": summarize(resp_raw),
    }


@app.get("/api/diagnostics/test-submission-ids")
async def diagnostics_test_submission_ids():
    """End-to-end discovery: list test policies, then fan out to fetch
    each one's detail to extract `ongoing_change_submission_id`.

    Reports timing, count, and sample IDs so we can sanity-check the
    final flow before refactoring the main routers.
    """
    if settings.is_mock:
        return {"mode": "mock"}
    import asyncio
    import time
    from app.joshu.factory import get_joshu_client
    import httpx as _httpx

    client = get_joshu_client()
    headers = client._headers()  # type: ignore[attr-defined]

    list_url = f"{settings.joshu_base_url}/api/insurance/v3/policies"
    list_params = client._build_params({"_page": 1, "_per_page": 50})  # type: ignore[attr-defined]

    t0 = time.monotonic()
    async with _httpx.AsyncClient(timeout=30.0) as http:
        list_resp = await http.get(list_url, params=list_params, headers=headers)
        list_body = list_resp.json()
        policies = list_body.get("items", []) if isinstance(list_body, dict) else []
        t_list = time.monotonic() - t0

        # Fan out detail fetches in parallel
        t1 = time.monotonic()

        async def fetch_one(p):
            url = f"{settings.joshu_base_url}/api/insurance/v3/policies/{p['id']}"
            try:
                r = await http.get(url, headers=headers)
                return r.json() if r.status_code == 200 else None
            except Exception:
                return None

        details = await asyncio.gather(*(fetch_one(p) for p in policies))
        t_detail = time.monotonic() - t1

    submission_ids = []
    for d in details:
        if not d: continue
        sid = d.get("ongoing_change_submission_id")
        if sid is not None:
            submission_ids.append({
                "submission_id": sid,
                "policy_id": d.get("id"),
                "insured_name": d.get("insured_name"),
                "container": d.get("container"),
            })

    return {
        "policies_total": list_body.get("total_items"),
        "policies_returned": len(policies),
        "details_fetched": sum(1 for d in details if d),
        "submission_ids_found": len(submission_ids),
        "list_call_ms": int(t_list * 1000),
        "detail_calls_ms_total_parallel": int(t_detail * 1000),
        "sample_submission_ids": submission_ids[:10],
    }


@app.get("/api/diagnostics/policy-detail")
async def diagnostics_policy_detail(policy_id: str = "499be470-94d3-4d9d-af00-01f24b44f147"):
    """Fetch a single policy by ID and show every field returned.

    Used to find which fields contain the linkage to submissions /
    quotes / transactions. The /policies list response has submission_id
    and latest_submission_id as null — but the single-record endpoint
    may populate them.
    """
    if settings.is_mock:
        return {"mode": "mock"}

    from app.joshu.factory import get_joshu_client
    import httpx as _httpx
    client = get_joshu_client()
    headers = client._headers()  # type: ignore[attr-defined]

    url = f"{settings.joshu_base_url}/api/insurance/v3/policies/{policy_id}"
    try:
        async with _httpx.AsyncClient(timeout=30.0) as http:
            resp = await http.get(url, headers=headers)
        return {
            "request_url": url,
            "response_status": resp.status_code,
            "body": resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text[:1000],
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.get("/api/diagnostics/transactions-by-policy")
async def diagnostics_transactions_by_policy(policy_id: str = "499be470-94d3-4d9d-af00-01f24b44f147"):
    """List transactions for a given policy. Each transaction has
    `latest_submission_id` per the API spec, so this gives us the
    submission IDs for a policy.
    """
    if settings.is_mock:
        return {"mode": "mock"}

    from app.joshu.factory import get_joshu_client
    import httpx as _httpx
    client = get_joshu_client()
    params = client._build_params({"policy_id": policy_id, "_per_page": 50})  # type: ignore[attr-defined]
    headers = client._headers()  # type: ignore[attr-defined]

    url = f"{settings.joshu_base_url}/api/insurance/v3/transactions"
    constructed_url = str(_httpx.Request("GET", url, params=params).url)
    try:
        async with _httpx.AsyncClient(timeout=30.0) as http:
            resp = await http.get(url, params=params, headers=headers)
        body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text[:1000]
        items = (body.get("items") if isinstance(body, dict) else None) or []
        return {
            "request_url": constructed_url,
            "response_status": resp.status_code,
            "total_items": body.get("total_items") if isinstance(body, dict) else None,
            "items": [
                {
                    "id": it.get("id"),
                    "flow": it.get("flow"),
                    "status": it.get("status"),
                    "latest_submission_id": it.get("latest_submission_id"),
                    "effective_at": it.get("effective_at"),
                }
                for it in items
            ],
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.get("/api/diagnostics/policies-live")
async def diagnostics_policies_live():
    """Mirror Joshu's UI initial page load: GET /policies with container=Test
    and the same status / ongoing_change filter set the UI uses.

    If our hypothesis is correct, this endpoint honors container=Test even
    though /submissions does not. Returned total_items should be much
    smaller than 1281 — it'll match what Joshu's UI shows in test mode.
    """
    if settings.is_mock:
        return {"mode": "mock", "note": "Mock mode — skipping live call."}

    from app.joshu.factory import get_joshu_client
    import httpx as _httpx
    client = get_joshu_client()

    # Mirror Joshu's UI request shape exactly. Multiple status= and
    # ongoing_change= values, joined with httpx's list-param convention.
    params = client._build_params({  # type: ignore[attr-defined]
        "status": ["Incomplete", "Future", "Active", "Canceled", "Declined", "Expired"],
        "ongoing_change": ["New", "FlatCancellation", "ManualCancellation",
                           "Endorsement", "Renewal", "CancellationReissuance",
                           "Reinstatement"],
        "_page": 1,
        "_per_page": 10,
    })
    headers = client._headers()  # type: ignore[attr-defined]

    url = f"{settings.joshu_base_url}/api/insurance/v3/policies"
    constructed_url = str(_httpx.Request("GET", url, params=params).url)

    try:
        async with _httpx.AsyncClient(timeout=30.0) as http:
            resp = await http.get(url, params=params, headers=headers)
        status = resp.status_code
        body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text[:500]
    except Exception as e:
        return {
            "mode": settings.joshu_environment,
            "request_url": constructed_url,
            "error": f"{type(e).__name__}: {e}",
        }

    summary = {
        "mode": settings.joshu_environment,
        "request_url": constructed_url,
        "response_status": status,
    }
    if isinstance(body, dict):
        items = body.get("items") or []
        summary["total_items"] = body.get("total_items")
        summary["returned_count"] = len(items)
        # Pull a few key fields from each item — including `test` and
        # `submission_id` so we can see the linkage.
        summary["records"] = []
        for it in items[:10]:
            summary["records"].append({
                "id": it.get("id"),
                "test_field_value": it.get("test"),
                "test_field_type": type(it.get("test")).__name__,
                "status": it.get("status"),
                "ongoing_change": it.get("ongoing_change"),
                "insured_id": it.get("insured_id"),
                "insured_name": it.get("insured_name"),
                "submission_id": it.get("submission_id"),
                "latest_submission_id": it.get("latest_submission_id"),
            })
        # Aggregate test field values
        test_counts = {"true": 0, "false": 0, "null_or_missing": 0, "other": 0}
        for it in items:
            v = it.get("test")
            if v is True: test_counts["true"] += 1
            elif v is False: test_counts["false"] += 1
            elif v is None: test_counts["null_or_missing"] += 1
            else: test_counts["other"] += 1
        summary["test_field_breakdown"] = test_counts
    else:
        summary["body_preview"] = str(body)[:500]

    return summary


@app.get("/api/diagnostics/live")
async def diagnostics_live():
    """Make a LIVE call to Joshu's submissions list and report back.

    This is the definitive end-to-end test: it shows what actually comes
    back from Joshu when we send our `test=true` filter. If the response
    contains records that say `test: false`, then the filter isn't being
    honored on Joshu's side and we have a real bug to fix.

    Returns:
      • the request URL we sent (with params serialized as on the wire)
      • count of records returned
      • for each record: insured_id, status, flow, and the value of the
        record's `test` field (so we can see if mixed test/prod records
        came back)
    """
    if settings.is_mock:
        return {"mode": "mock", "note": "Mock mode — skipping live call."}

    from app.joshu.factory import get_joshu_client
    import httpx as _httpx
    client = get_joshu_client()

    sample_params = client._build_params({"_page": 1, "_per_page": 10})  # type: ignore[attr-defined]
    headers = client._headers()  # type: ignore[attr-defined]

    url = f"{settings.joshu_base_url}/api/insurance/v3/submissions"
    constructed_url = str(_httpx.Request("GET", url, params=sample_params).url)

    try:
        async with _httpx.AsyncClient(timeout=30.0) as http:
            resp = await http.get(url, params=sample_params, headers=headers)
        status = resp.status_code
        body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text[:500]
    except Exception as e:
        return {
            "mode": settings.joshu_environment,
            "request_url": constructed_url,
            "request_params": sample_params,
            "error": f"{type(e).__name__}: {e}",
        }

    # Summarize the response — pull out each record's test field so we
    # can see if filtering is working
    summary = {
        "mode": settings.joshu_environment,
        "request_url": constructed_url,
        "request_params": sample_params,
        "request_param_types": {k: type(v).__name__ for k, v in sample_params.items()},
        "response_status": status,
    }
    if isinstance(body, dict):
        items = body.get("items") or []
        summary["total_items"] = body.get("total_items")
        summary["page"] = body.get("page")
        summary["per_page"] = body.get("per_page")
        summary["returned_count"] = len(items)
        summary["records"] = []
        for it in items[:10]:
            summary["records"].append({
                "id": it.get("id"),
                "test_field_value": it.get("test"),
                "test_field_type": type(it.get("test")).__name__,
                "status": it.get("status"),
                "flow": it.get("flow"),
                "policy_id": it.get("policy_id"),
                "insured_id": it.get("insured_id"),
                "modified_at": it.get("modified_at") or it.get("modi(cid:21)ed_at"),
            })
        # Aggregate: how many returned records have test=true vs test=false?
        test_counts = {"true": 0, "false": 0, "null_or_missing": 0, "other": 0}
        for it in items:
            v = it.get("test")
            if v is True:
                test_counts["true"] += 1
            elif v is False:
                test_counts["false"] += 1
            elif v is None:
                test_counts["null_or_missing"] += 1
            else:
                test_counts["other"] += 1
        summary["test_field_breakdown"] = test_counts
    else:
        summary["body_preview"] = str(body)[:500]

    return summary


# Serve the single-file UI at /
FRONTEND_DIR = Path(__file__).resolve().parent.parent.parent / "frontend"
INDEX_HTML = FRONTEND_DIR / "index.html"
ASSETS_DIR = FRONTEND_DIR / "assets"

# Mount assets (logos, etc.) at /assets/*
if ASSETS_DIR.exists():
    from fastapi.staticfiles import StaticFiles
    app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")


@app.get("/", response_class=HTMLResponse)
def root():
    if INDEX_HTML.exists():
        return FileResponse(INDEX_HTML)
    return HTMLResponse(
        "<h1>Altruis Broker Portal</h1>"
        "<p>UI not built. API at /docs</p>"
    )
