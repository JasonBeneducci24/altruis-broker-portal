# Altruis Broker Portal

Broker-facing portal for Altruis Group, backed by the Joshu Insurance API v3.

## What it does

- **Login** → session held in a signed HTTP-only cookie
- **Dashboard** with agency pipeline at a glance
- **Submissions** list (scoped to the broker's agency) with detail view and
  editable data panel
- **Quotes** list and detail with premium breakdown, document downloads,
  and binder request flow
- **Policies** list and detail with lifecycle actions (Endorsement,
  Renewal, Cancellation)
- **Documents** library filtered to the broker's agency

## Architecture

```
┌──────────────────┐
│  Broker (web)    │
└────────┬─────────┘
         │ HTTPS (signed session cookie)
┌────────▼──────────────────────────────────────┐
│       Altruis Broker Portal (FastAPI)          │
│                                                │
│   Routers:                                     │
│     auth.py          /api/auth/*               │
│     submissions.py   /api/submissions/*        │
│     quotes.py        /api/quotes/*             │
│     policies.py      /api/policies/*           │
│     documents.py     /api/documents/*          │
│                                                │
│   Joshu client (abstracted):                   │
│     JoshuClientBase                            │
│       ├─ MockJoshuClient  (in-memory dev)     │
│       └─ HttpJoshuClient  (READS LIVE)         │
│                                                │
└────────┬───────────────────────────────────────┘
         │  Authorization: Token <api_key>
         │  ?container=Test   ← forced by client
┌────────▼──────────────────────────────────────┐
│              Joshu Platform API                │
│          altruis.joshu.insure                  │
│        /api/insurance/v3/*                     │
└────────────────────────────────────────────────┘
```

## Current phase: READS LIVE, WRITES LOCKED

The HTTP client is wired up for **all read endpoints** against the Joshu
test container. Every mutation method (`create_policy`, `create_transaction`,
`update_submission_data`, `submit_submission`, `update_quote_status`) still
raises `HttpClientNotReadyError` — they stay locked until the first round
of reads is verified against real Joshu.

## Safety guardrails (what prevents production traffic)

Four independent layers:

**1. Environment guard (`config.py`)**
- `JOSHU_ENVIRONMENT` must be explicitly set — app refuses to start silently
- Production requires BOTH `JOSHU_ENVIRONMENT=production` AND
  `ALTRUIS_ALLOW_PRODUCTION=yes-i-know-what-i-am-doing`

**2. Container parameter enforcement (`client_http.py`)**
- `container=Test` injected at construction time from env config
- Every outbound request goes through `_build_params()` which **strips
  any caller-supplied `container` key** and logs the attempt
- Case-insensitive: `Container`, `CONTAINER`, `container` all stripped
- Verified by the dependency-free safety suite in `docs/verify_safety.py`

**3. Write lockout (`_WRITES_ENABLED = False`)**
- Every mutation method raises `HttpClientNotReadyError`
- `_assert_test_mode_for_write()` additionally blocks any write if
  env drifts to production without the override

**4. Diagnostics endpoint (`/api/diagnostics`)**
- Returns the exact URL, params, and headers that the NEXT request would
  use — without making a call
- Auth token is redacted; safe to share
- Use this to verify config BEFORE firing the first real call

## Running the portal

### Mock mode (no Joshu calls)

```bash
cd backend
pip install -r requirements.txt
JOSHU_ENVIRONMENT=mock uvicorn app.main:app --reload --port 8001
```

Open http://localhost:8001 and sign in with any seeded account:
- `jane.broker@acmesurplus.com`
- `marcus.ortiz@acmesurplus.com`
- `admin@acmesurplus.com`

Any password works in mock mode.

### Test mode (reads live against Joshu test container)

```bash
cd backend
pip install -r requirements.txt
export JOSHU_ENVIRONMENT=test
export JOSHU_BASE_URL=https://altruis.joshu.insure
export JOSHU_API_TOKEN=<your-test-token>
uvicorn app.main:app --reload --port 8001
```

**Recommended first step**: before logging in, hit `GET /api/diagnostics`.
It will show you the exact outbound URL, params, and (redacted) auth
headers that the next request would use. Confirm:
- `base_url` is `https://altruis.joshu.insure`
- `container` is `Test`
- Auth scheme is `Token`
- `api_prefix` is `/api/insurance/v3`

If anything looks wrong, stop and fix config before making real calls.

## Safety verification

The dependency-free safety suite proves the container invariants hold:

```bash
python3 docs/verify_safety.py
```

Expected output:
```
✓ 6 requests made (got 6)
✓ /api/insurance/v3/submissions has container=Test
✓ ... (23 more checks)
All safety invariants verified.
```

For the full pytest suite (requires `pip install pytest pytest-asyncio`):

```bash
cd backend
pytest tests/ -v
```

## Deploying

`render.yaml` is included. Push to GitHub and connect to Render as in the
PAS deploy guide. Set these environment variables in Render's dashboard:

- `JOSHU_ENVIRONMENT=test`
- `JOSHU_BASE_URL=https://altruis.joshu.insure`
- `JOSHU_API_TOKEN=<your-test-token>`  *(use Render's env var UI — never commit)*
- `SESSION_SECRET=<a long random string>`

## Project layout

```
altruis_broker_portal/
├── backend/
│   ├── app/
│   │   ├── main.py             # FastAPI app, mounts routers, serves UI
│   │   ├── config.py           # Environment guardrail (refuses to start without explicit env)
│   │   ├── session.py          # Signed-cookie sessions, API_TOKEN_SENTINEL
│   │   ├── joshu/
│   │   │   ├── schemas.py      # Pydantic models for Joshu v3 resources
│   │   │   ├── client_base.py  # Abstract client contract
│   │   │   ├── client_mock.py  # In-memory mock with seed data
│   │   │   ├── client_http.py  # LIVE READS, writes locked
│   │   │   └── factory.py      # Picks client based on env
│   │   └── routers/
│   │       ├── auth.py         # login / logout / me
│   │       ├── submissions.py  # list / detail / start / update / submit
│   │       ├── quotes.py       # list / detail / publish / bind
│   │       ├── policies.py     # list / detail / lifecycle transactions
│   │       └── documents.py    # list / detail / download
│   ├── tests/
│   │   └── test_http_client_safety.py   # pytest suite for safety invariants
│   ├── pytest.ini
│   └── requirements.txt
├── frontend/
│   └── index.html              # Single-file React-style SPA
├── docs/
│   └── verify_safety.py        # Dependency-free safety verification
└── README.md
```

## Known limitations / follow-ups

**Login against Joshu** — the Joshu API v3 reference doesn't document a
password-auth endpoint in the sections reviewed. Currently the portal
uses a single shared API token for all outbound calls; all broker logins
map to that one identity in Joshu. Per-broker login will be wired in
once Joshu's auth URL is documented.

**Write methods locked** — creating policies/transactions, updating
submission data, submitting for rating, and updating quote statuses all
raise `HttpClientNotReadyError`. Unlock in a second pass after reads
are verified end-to-end against real Joshu data.

**Document download URL** — assumed to be `/api/insurance/v3/documents/{id}/download`
based on common patterns. If Joshu uses a different URL shape (e.g.
`/files/{file_id}/content`), the first download attempt will 404 and
we'll adjust.
