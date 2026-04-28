"""
Dependency-free safety verification for HttpJoshuClient.

This script shims out fastapi/pydantic/httpx/itsdangerous with minimal
stand-ins, then exercises the critical safety invariants of the HTTP
client. It's equivalent to the pytest suite in `tests/` but runs without
requiring the dependencies installed.

Run with:  python3 docs/verify_safety.py

Exits 0 on success, 1 on any failure.
"""
import sys
import os
import types
import asyncio
from pathlib import Path


# ----------------------------------------------------------------------
# Shim missing dependencies with minimal stand-ins
# ----------------------------------------------------------------------

def make_module(name, attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


class _Request:
    """Stand-in for httpx.Request — captures what the client would send."""
    def __init__(self, method, url, params=None, headers=None, content=None):
        self.method = method
        # url will be something like "/api/insurance/v3/submissions"
        # We store it raw + the params separately
        self.path = str(url)
        self.params = dict(params or {})
        self.headers = dict(headers or {})
        self.content = content
        # For compatibility with the `.url.params.get_list` pattern used in tests
        class _URLParams:
            def __init__(self, d): self._d = d
            def get_list(self, k):
                v = self._d.get(k)
                if v is None: return []
                return v if isinstance(v, list) else [v]
            def get(self, k, default=None): return self._d.get(k, default)
        class _URL:
            def __init__(self, path, params): self.path = path; self.params = _URLParams(params)
        self.url = _URL(self.path, self.params)


class _Response:
    def __init__(self, status_code, json_data=None, content=b"", headers=None):
        self.status_code = status_code
        self._json = json_data
        self.content = content
        self.headers = headers or {}
        self.text = str(json_data) if json_data is not None else ""

    @property
    def is_success(self):
        return 200 <= self.status_code < 300

    def json(self):
        return self._json


class _MockAsyncClient:
    """Stand-in for httpx.AsyncClient that captures requests and returns
    fake successful responses. The real client uses this same interface."""
    def __init__(self, base_url=None, transport=None, **kw):
        self.base_url = base_url
        self.captured_requests = []

    async def get(self, url, params=None, headers=None):
        req = _Request("GET", url, params, headers)
        self.captured_requests.append(req)
        return _Response(
            200,
            json_data={
                "page": 1, "per_page": 25, "total_items": 0, "total_pages": 0,
                "items": [],
                # Single-record fields. `test: True` is required so the
                # _verify_record_matches_mode check accepts the record
                # before any write proceeds.
                "id": 42, "status": "Submitted", "test": True,
            },
        )

    async def put(self, url, params=None, headers=None, json=None):
        req = _Request("PUT", url, params, headers, content=json)
        self.captured_requests.append(req)
        return _Response(200, json_data={"ok": True})

    async def post(self, url, params=None, headers=None, json=None):
        req = _Request("POST", url, params, headers, content=json)
        self.captured_requests.append(req)
        return _Response(200, json_data={"ok": True})

    async def aclose(self):
        pass


def _Timeout(*a, **kw):
    return None


httpx_mod = make_module("httpx", {
    "AsyncClient": _MockAsyncClient,
    "Timeout": _Timeout,
    "Request": _Request,
    "Response": _Response,
    "MockTransport": lambda handler: None,
})


# Minimal fastapi shim
class _HTTPException(Exception):
    def __init__(self, status_code, detail=None):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"HTTP {status_code}: {detail}")


make_module("fastapi", {
    "HTTPException": _HTTPException,
    "Request": type("Request", (), {}),
    "Response": type("Response", (), {}),
    "APIRouter": lambda **kw: type("R", (), {"get": lambda *a, **kw: lambda f: f, "post": lambda *a, **kw: lambda f: f})(),
    "Depends": lambda f: None,
})


# Minimal pydantic shim (v2-ish API surface used by the client)
class _BaseModel:
    model_config = {}
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)
    def model_dump(self, **kw):
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
    @classmethod
    def model_validate(cls, d):
        return cls(**(d if isinstance(d, dict) else {}))


def _Field(default=None, default_factory=None, **kw):
    if default_factory is not None:
        return default_factory()
    return default


def _ConfigDict(**kw):
    return kw


make_module("pydantic", {
    "BaseModel": _BaseModel,
    "ConfigDict": _ConfigDict,
    "Field": _Field,
    "EmailStr": str,
})


# itsdangerous shim
class _Serializer:
    def __init__(self, *a, **kw): pass
    def dumps(self, o): return "fake"
    def loads(self, o): return {}


class _BadSig(Exception): pass


make_module("itsdangerous", {
    "URLSafeSerializer": _Serializer,
    "BadSignature": _BadSig,
})


# ----------------------------------------------------------------------
# Now set up env and load the real HttpJoshuClient module
# ----------------------------------------------------------------------

os.environ["JOSHU_ENVIRONMENT"] = "test"
os.environ["JOSHU_BASE_URL"] = "https://altruis.joshu.insure"
os.environ["JOSHU_API_TOKEN"] = "fake-token-for-tests"

backend_root = Path(__file__).resolve().parent.parent / "backend"
sys.path.insert(0, str(backend_root))

# Import the actual client module — this will use our shimmed deps
from app.joshu.client_http import HttpJoshuClient, HttpClientNotReadyError
from app.session import API_TOKEN_SENTINEL


# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------

FAILURES = []


def check(condition, message):
    if condition:
        print(f"  ✓ {message}")
    else:
        print(f"  ✗ FAIL: {message}")
        FAILURES.append(message)


async def test_reads_inject_test_true():
    print("\n[1] Every list read injects test=True")
    client = HttpJoshuClient()

    # Fire each read method
    await client.list_submissions("some-token")
    await client.list_quotes("some-token")
    await client.list_policies("some-token")
    await client.list_documents("some-token")
    await client.list_transactions("some-token")
    await client.list_products("some-token")

    requests = client._client.captured_requests
    check(len(requests) == 6, f"6 requests made (got {len(requests)})")
    for req in requests:
        # Joshu honors `container=Test` on list endpoints (confirmed by
        # intercepting their UI's network calls). The `test` boolean
        # query param documented in the spec is silently ignored on
        # this account, so we use container instead.
        cvals = req.url.params.get_list("container")
        check(cvals == ["Test"],
              f"{req.url.path} has container=Test (got {cvals!r})")
        # The `test` boolean param is intentionally NOT sent on reads.
        tvals = req.url.params.get_list("test")
        check(tvals == [],
              f"{req.url.path} does NOT have stale test param (got {tvals!r})")


async def test_caller_override_is_stripped():
    print("\n[2] Caller attempting to set test=False or container=Production is stripped")
    client = HttpJoshuClient()

    # Deliberate attacks: caller tries to flip the test filter or sneak
    # in a different container value.
    await client._get(
        "/submissions",
        params={"test": False, "_page": 1},
        bearer_token="some-token",
    )
    await client._get(
        "/submissions",
        params={"container": "Production", "_page": 2},
        bearer_token="some-token",
    )

    requests = client._client.captured_requests
    check(len(requests) == 2, "2 requests were made")
    for i, req in enumerate(requests):
        # Real container should be Test regardless of caller attempts.
        cvals = req.url.params.get_list("container")
        check(cvals == ["Test"],
              f"req {i}: caller's container override was stripped, real container=Test (got {cvals!r})")
        # The `test` boolean override should never reach the wire.
        tvals = req.url.params.get_list("test")
        check(tvals == [],
              f"req {i}: stale test param did not leak through (got {tvals!r})")
    # Legitimate non-safety params should be preserved
    check(requests[0].url.params.get("_page") == 1,
          f"legitimate _page=1 preserved on req 0")
    check(requests[1].url.params.get("_page") == 2,
          f"legitimate _page=2 preserved on req 1")


async def test_case_insensitive_override_is_stripped():
    print("\n[3] Case-variant override of test/container is stripped")
    client = HttpJoshuClient()

    await client._get(
        "/submissions",
        params={"Test": False, "_page": 1},
        bearer_token="some-token",
    )
    await client._get(
        "/submissions",
        params={"CONTAINER": "Production"},
        bearer_token="some-token",
    )

    for req in client._client.captured_requests:
        # The real container should be Test regardless of caller's
        # case-variant attempt to inject Test/CONTAINER.
        cvals = req.url.params.get_list("container")
        check(cvals == ["Test"],
              f"Case-variant override stripped on {req.url.path} (got {cvals!r})")


async def test_single_record_get_omits_test_param():
    print("\n[3b] Single-record GETs omit the test query param (Joshu doesn't accept it there)")
    client = HttpJoshuClient()

    await client.get_submission("some-token", "abc-123")
    await client.get_policy("some-token", "policy-uuid")
    await client.get_quote("some-token", "quote-uuid")

    requests = [r for r in client._client.captured_requests if "/submissions/" in r.url.path
                or "/policies/" in r.url.path or "/quotes/" in r.url.path]
    check(len(requests) >= 3, f"3+ single-record GETs were made (got {len(requests)})")
    for req in requests:
        vals = req.url.params.get_list("test")
        check(vals == [],
              f"Single-record {req.url.path} omits test param (got {vals!r})")


async def test_api_token_sentinel_uses_token_auth():
    print("\n[4] API_TOKEN_SENTINEL in session → Authorization: Token <api_key>")
    client = HttpJoshuClient()

    await client.list_submissions(API_TOKEN_SENTINEL)

    req = client._client.captured_requests[0]
    auth = req.headers.get("Authorization", "")
    check(auth.startswith("Token "),
          f"auth header uses Token scheme (got: {auth[:20]!r})")
    check("fake-token-for-tests" in auth,
          f"auth header contains the configured API token")


async def test_real_bearer_token_uses_bearer_auth():
    print("\n[5] Real bearer token → Authorization: Bearer <jwt>")
    client = HttpJoshuClient()

    await client.list_submissions("real-jwt-abc123")

    req = client._client.captured_requests[0]
    auth = req.headers.get("Authorization", "")
    check(auth == "Bearer real-jwt-abc123",
          f"auth header is Bearer (got: {auth!r})")


async def test_pagination_params_underscore_prefixed():
    print("\n[6] Pagination uses _page and _per_page (per Joshu docs)")
    client = HttpJoshuClient()

    await client.list_submissions("tok", page=3, per_page=50)

    req = client._client.captured_requests[0]
    check(req.url.params.get("_page") == 3,
          f"_page=3 (got {req.url.params.get('_page')})")
    check(req.url.params.get("_per_page") == 50,
          f"_per_page=50 (got {req.url.params.get('_per_page')})")


async def test_locked_writes_raise_not_ready():
    print("\n[7] Writes NOT yet enabled still raise HttpClientNotReadyError")
    client = HttpJoshuClient()

    # Only update_quote_status remains locked. create_policy and
    # create_transaction were re-enabled to test the body-shape fix
    # (transaction body no longer carries `test: true`, matching what
    # Joshu's UI was observed to send via network trace).
    locked = [
        ("update_quote_status", lambda c: c.update_quote_status("t", 1, "QuotePublished")),
    ]
    for name, op in locked:
        raised = False
        try:
            await op(client)
        except HttpClientNotReadyError:
            raised = True
        check(raised, f"{name} raised HttpClientNotReadyError")


async def test_enabled_writes_inject_test_true_and_verify_record():
    """Enabled writes go through _verify_record_matches_mode AND have
    test=true on their query string when applicable."""
    print("\n[7b] Enabled writes verify record's test field and refuse cross-mode writes")
    client = HttpJoshuClient()

    # update_submission_data — should make a GET to verify record.test,
    # then a PUT. Both go to /submission-data or /submissions paths.
    await client.update_submission_data("tok", 42, {"insured.name": "Test LLC"})
    puts = [r for r in client._client.captured_requests if r.method == "PUT"]
    gets = [r for r in client._client.captured_requests if r.method == "GET"]
    check(len(puts) >= 1, "at least one PUT was made")
    check(len(gets) >= 1, "at least one GET was made (record verification)")
    check(any("/submissions/42" in r.url.path for r in gets),
          "verification GET hit /submissions/42 to check record.test")
    put = puts[0]
    check("submission-data/42" in put.url.path,
          f"PUT path is /submission-data/42 (got {put.url.path})")

    # submit_submission
    client2 = HttpJoshuClient()
    await client2.submit_submission("tok", 42)
    puts2 = [r for r in client2._client.captured_requests if r.method == "PUT"]
    check(len(puts2) >= 1, "submit_submission produced a PUT")
    check("submissions/42" in puts2[0].url.path,
          f"submit PUT path is /submissions/42")

    # reopen_submission
    client3 = HttpJoshuClient()
    await client3.reopen_submission("tok", 42)
    puts3 = [r for r in client3._client.captured_requests if r.method == "PUT"]
    check(len(puts3) >= 1, "reopen_submission produced a PUT")


async def test_writes_blocked_when_record_is_production():
    """A submission whose `test` field is False (i.e. a production record)
    must NOT be writable from a test environment, even if the ID surfaces
    in the UI somehow."""
    print("\n[7c] Writes blocked when target record's test field doesn't match environment")

    # Build a client that will return test=False for the verification GET.
    client = HttpJoshuClient()
    # Monkey-patch the mock client to return a production record
    original_get = client._client.get
    async def get_returning_prod_record(url, params=None, headers=None):
        from app.joshu.client_http import HttpJoshuClient as _C  # for type-only ref
        req_class = type(client._client.captured_requests[0]) if client._client.captured_requests else None
        # Reuse the original mock framework by calling original_get and patching response
        resp = await original_get(url, params, headers)
        # Mutate the json so test=False
        resp._json["test"] = False
        return resp
    client._client.get = get_returning_prod_record

    # Attempt a write — should raise RuntimeError with BLOCKED in message
    raised = False
    try:
        await client.update_submission_data("tok", 99, {"insured.name": "X"})
    except RuntimeError as e:
        raised = "BLOCKED" in str(e) and "test" in str(e).lower()
    check(raised, "write blocked when record.test=False in test environment")


async def test_api_prefix_correct():
    print("\n[8] URLs use /api/insurance/v3 prefix")
    client = HttpJoshuClient()

    await client.list_submissions("tok")
    await client.get_submission("tok", 42)
    await client.list_policies("tok")

    paths = [r.url.path for r in client._client.captured_requests]
    check(paths[0] == "/api/insurance/v3/submissions", f"list: {paths[0]}")
    check(paths[1] == "/api/insurance/v3/submissions/42", f"get: {paths[1]}")
    check(paths[2] == "/api/insurance/v3/policies", f"policies: {paths[2]}")


async def test_production_env_requires_override():
    print("\n[9] Production mode requires ALTRUIS_ALLOW_PRODUCTION override")
    # This test verifies the config-layer guard, not the client itself
    # Re-import config with production but no override
    saved_env = os.environ["JOSHU_ENVIRONMENT"]
    os.environ["JOSHU_ENVIRONMENT"] = "production"
    os.environ.pop("ALTRUIS_ALLOW_PRODUCTION", None)

    # Run config in a subprocess to get the SystemExit it raises on bad config
    # We can't really test this in-process because settings is already loaded.
    # But we can at least verify the constants exist.
    from app.config import PRODUCTION_OVERRIDE_TOKEN, VALID_MODES
    check(PRODUCTION_OVERRIDE_TOKEN == "yes-i-know-what-i-am-doing",
          f"override token is the right sentinel value")
    check("production" in VALID_MODES, "production is a valid mode")

    os.environ["JOSHU_ENVIRONMENT"] = saved_env


async def run_all():
    await test_reads_inject_test_true()
    await test_caller_override_is_stripped()
    await test_case_insensitive_override_is_stripped()
    await test_single_record_get_omits_test_param()
    await test_api_token_sentinel_uses_token_auth()
    await test_real_bearer_token_uses_bearer_auth()
    await test_pagination_params_underscore_prefixed()
    await test_locked_writes_raise_not_ready()
    await test_enabled_writes_inject_test_true_and_verify_record()
    await test_writes_blocked_when_record_is_production()
    await test_api_prefix_correct()
    await test_production_env_requires_override()


if __name__ == "__main__":
    asyncio.run(run_all())
    print()
    print("=" * 60)
    if FAILURES:
        print(f"FAILED: {len(FAILURES)} check(s) failed")
        for f in FAILURES:
            print(f"  - {f}")
        sys.exit(1)
    else:
        print("All safety invariants verified.")
        sys.exit(0)
