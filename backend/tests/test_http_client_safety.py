"""
Safety tests for HttpJoshuClient.

These tests verify the SAFETY INVARIANT that the ``container`` query
parameter is always set correctly and cannot be overridden by callers.

Run with:
    JOSHU_ENVIRONMENT=test \
    JOSHU_BASE_URL=https://altruis.joshu.insure \
    JOSHU_API_TOKEN=fake-for-testing \
    python -m pytest tests/test_http_client_safety.py -v

The tests use httpx's MockTransport so no real network calls are made.
Every HTTP request is captured and asserted against expected shape.
"""
from __future__ import annotations

import os
import pytest
import httpx


# Environment setup MUST happen before importing the client
os.environ.setdefault("JOSHU_ENVIRONMENT", "test")
os.environ.setdefault("JOSHU_BASE_URL", "https://altruis.joshu.insure")
os.environ.setdefault("JOSHU_API_TOKEN", "fake-token-for-tests")

from app.joshu.client_http import HttpJoshuClient


class RequestCapture:
    """Captures every request made by the client for later inspection."""
    def __init__(self):
        self.requests: list[httpx.Request] = []

    def transport(self) -> httpx.MockTransport:
        def handler(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            # Return a minimal successful paginated response
            return httpx.Response(
                200,
                json={
                    "page": 1, "per_page": 25, "total_items": 0, "total_pages": 1,
                    "items": [],
                },
            )
        return httpx.MockTransport(handler)


@pytest.fixture
def client_and_capture(monkeypatch):
    capture = RequestCapture()
    client = HttpJoshuClient()
    # Replace the underlying AsyncClient with one that uses our MockTransport
    client._client = httpx.AsyncClient(
        base_url=client.base_url, transport=capture.transport(),
    )
    yield client, capture


@pytest.mark.asyncio
async def test_container_param_is_test_on_every_read(client_and_capture):
    client, capture = client_and_capture

    # Fire each read method once
    await client.list_submissions("fake-token")
    await client.list_quotes("fake-token")
    await client.list_policies("fake-token")
    await client.list_documents("fake-token")
    await client.list_transactions("fake-token")

    assert len(capture.requests) == 5
    for req in capture.requests:
        container_vals = req.url.params.get_list("container")
        assert container_vals == ["Test"], \
            f"Expected ?container=Test, got {container_vals} on {req.url}"


@pytest.mark.asyncio
async def test_caller_cannot_override_container(client_and_capture):
    """A malicious caller passing container=Production must be ignored."""
    client, capture = client_and_capture

    # Call the underlying _get with a container override — the safety layer
    # should strip it and emit a safety log.
    await client._get(
        "/submissions",
        params={"container": "Production", "_page": 1},
        bearer_token="fake-token",
    )

    assert len(capture.requests) == 1
    req = capture.requests[0]
    container_vals = req.url.params.get_list("container")
    assert container_vals == ["Test"], \
        f"Container override was not stripped! Got {container_vals}"


@pytest.mark.asyncio
async def test_auth_header_is_token_scheme_when_using_api_token(client_and_capture):
    client, capture = client_and_capture
    # Pass the sentinel; expect API token used
    from app.session import API_TOKEN_SENTINEL
    await client.list_submissions(API_TOKEN_SENTINEL)
    req = capture.requests[0]
    auth = req.headers.get("authorization", "")
    assert auth.startswith("Token "), f"Expected 'Token' auth scheme, got: {auth!r}"


@pytest.mark.asyncio
async def test_auth_header_is_bearer_when_real_token(client_and_capture):
    client, capture = client_and_capture
    await client.list_submissions("real-jwt-123")
    req = capture.requests[0]
    auth = req.headers.get("authorization", "")
    assert auth == "Bearer real-jwt-123"


@pytest.mark.asyncio
async def test_pagination_params_are_underscore_prefixed(client_and_capture):
    client, capture = client_and_capture
    await client.list_submissions("fake", page=2, per_page=50)
    req = capture.requests[0]
    assert req.url.params.get("_page") == "2"
    assert req.url.params.get("_per_page") == "50"


@pytest.mark.asyncio
async def test_all_write_methods_raise(client_and_capture):
    """Every write must raise HttpClientNotReadyError in the read-only phase."""
    from app.joshu.client_http import HttpClientNotReadyError
    client, _ = client_and_capture

    with pytest.raises(HttpClientNotReadyError):
        await client.create_policy("t")
    with pytest.raises(HttpClientNotReadyError):
        await client.create_transaction("t", flow="New", policy_id="x")
    with pytest.raises(HttpClientNotReadyError):
        await client.update_submission_data("t", 1, {})
    with pytest.raises(HttpClientNotReadyError):
        await client.submit_submission("t", 1)
    with pytest.raises(HttpClientNotReadyError):
        await client.update_quote_status("t", 1, "QuotePublished")


@pytest.mark.asyncio
async def test_api_prefix_is_correct(client_and_capture):
    client, capture = client_and_capture
    await client.list_submissions("fake")
    req = capture.requests[0]
    assert req.url.path == "/api/insurance/v3/submissions"
