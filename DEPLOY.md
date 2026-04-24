# Deploying the Altruis Broker Portal

This portal talks to real Joshu infrastructure. Following this guide keeps
us pointed at the **test container** until we've deliberately decided to
flip to production.

## Before you deploy

Do all of these first:

**1. Confirm the API token.** If the token you first generated
(`sXQhA6n21f.iLaZ9qE0RzMe7TQ1bVT_Tw`) is still active — revoke it.
It was shared in our conversation and should be treated as burned.

**2. Generate a fresh token for test**, scoped as narrowly as Joshu
supports. Copy the token string once, then don't show it anywhere —
not chat, not screenshots, not docs.

**3. Run the local safety verification** to confirm the code is in the
state you expect:

```bash
cd altruis_broker_portal
python3 docs/verify_safety.py
```

You should see "All safety invariants verified." at the end. If anything
fails, don't deploy until it's green.

## Deploy to Render (free tier, 10 minutes)

### 1. Put the code on GitHub

Follow the same flow as the PAS project: create a repo at
https://github.com/new, drag-and-drop the unzipped portal contents onto
the upload page, commit.

### 2. Create the Render service

Go to https://dashboard.render.com → **New** → **Web Service**, connect
your GitHub account, select the repo. Render will auto-detect
`render.yaml` and fill in most of the config.

### 3. Set the secret environment variables

In the Render service dashboard, go to **Environment** → **Environment
Variables**. Two values need to be set manually (they're marked
`sync: false` in `render.yaml` so they're never committed):

| Variable           | Value                                        |
|--------------------|----------------------------------------------|
| `JOSHU_API_TOKEN`  | Your Joshu test token                        |
| `SESSION_SECRET`   | A long random string (e.g. `openssl rand -hex 32`) |

Leave `JOSHU_ENVIRONMENT=test` and `JOSHU_BASE_URL=https://altruis.joshu.insure`
alone — they're already set correctly from the yaml.

### 4. Click Create Web Service

Render will build (~90 seconds) and start. Watch the logs. When you see:

```
========================================================================
  Altruis Broker Portal  ·  🟡 TEST MODE — API calls target the Joshu TEST container.
  Base URL: https://altruis.joshu.insure
========================================================================
INFO: Uvicorn running on http://0.0.0.0:10000
```

…the portal is live at the URL Render gave you.

## Post-deploy verification (DO THIS BEFORE LOGGING IN)

Before you click around, hit the diagnostics endpoint:

```
https://<your-render-url>.onrender.com/api/diagnostics
```

You should see something like:

```json
{
  "mode": "test",
  "base_url": "https://altruis.joshu.insure",
  "api_prefix": "/api/insurance/v3",
  "container": "Test",
  "sample_url": "https://altruis.joshu.insure/api/insurance/v3/submissions",
  "sample_params": {
    "_page": 1, "_per_page": 25, "container": "Test"
  },
  "sample_headers": {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": "Token <first8>…<last4>"
  },
  "writes_enabled": false,
  "note": "No request was made. This is a description of how the next request will be built."
}
```

Check three things:
1. `container` is `"Test"` — not `"Production"`, not missing
2. `sample_url` hostname is correct
3. `writes_enabled` is `false`

**If any of these look wrong, stop.** Something in the environment is
misconfigured and we should diagnose before making real calls.

## First login

Once diagnostics looks clean, go to `/` and sign in. In test mode the
portal accepts any email (the Joshu password-auth endpoint isn't wired
yet, so the portal uses the shared API token for all outbound calls).

The first real thing to try: click **Submissions** in the sidebar.
This hits `GET /api/insurance/v3/submissions?container=Test&_page=1&_per_page=25`
against real Joshu.

What you'll see:
- If Joshu has test submissions for Altruis → they render in the table
- If the token is valid but the test container has no submissions → empty
  state ("No submissions")
- If the token is wrong → 401 from Joshu, shown as an error toast
- If the Joshu API shape is different from what we inferred from the PDF
  → possibly a parsing error, send me the error message

## What to do if something breaks

**Send the full error** — not a summary. Include:
- The URL path (e.g. `/api/insurance/v3/submissions`)
- The HTTP status code
- The response body Joshu returned
- Any stack trace from the Render logs

Redact only the `Authorization` header if it appears. Everything else is
fair game.

## Things NOT yet wired up

Until reads are fully verified, these will fail with
`HttpClientNotReadyError`:
- Creating a submission
- Editing submission data
- Submitting for quote
- Requesting a binder
- Policy lifecycle actions (endorse, renew, cancel)

The UI will show these actions but clicking them will toast an error.
This is by design — writes get enabled in a second pass.

## Switching to production (do not do this yet)

When the time comes, production will require:

1. `JOSHU_ENVIRONMENT=production` (not `test`)
2. `ALTRUIS_ALLOW_PRODUCTION=yes-i-know-what-i-am-doing`
3. A production-scoped API token
4. `_WRITES_ENABLED = True` in `client_http.py` (a code change, not just
   an env var)

All four are deliberately independent to prevent accidental production
access. The app will refuse to start if any are missing or mismatched.
