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
from app.routers import auth, submissions, quotes, policies, documents


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

    return {
        "mode": settings.joshu_environment,
        "base_url": settings.joshu_base_url,
        "api_prefix": getattr(client, "API_PREFIX", "/api/insurance/v3"),
        "container": getattr(client, "_container", "unknown"),
        "sample_url": f"{settings.joshu_base_url}{sample_path}",
        "sample_params": sample_params,
        "sample_headers": sample_headers,
        "writes_enabled": False,
        "note": "No request was made. This is a description of how the next request will be built.",
    }


# Serve the single-file UI at /
FRONTEND_DIR = Path(__file__).resolve().parent.parent.parent / "frontend"
INDEX_HTML = FRONTEND_DIR / "index.html"


@app.get("/", response_class=HTMLResponse)
def root():
    if INDEX_HTML.exists():
        return FileResponse(INDEX_HTML)
    return HTMLResponse(
        "<h1>Altruis Broker Portal</h1>"
        "<p>UI not built. API at /docs</p>"
    )
