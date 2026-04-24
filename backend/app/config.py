"""
Environment configuration for the Altruis Broker Portal.

SAFETY GUARDRAILS
=================

This module enforces three invariants at startup and on every outbound call:

  1. JOSHU_ENVIRONMENT must be explicitly set — we refuse to default.
  2. While in development, JOSHU_ENVIRONMENT must be 'test' or 'mock'.
  3. To switch to production, BOTH JOSHU_ENVIRONMENT=production AND
     ALTRUIS_ALLOW_PRODUCTION=yes-i-know-what-i-am-doing must be set.
     (The second variable doesn't exist yet — it's a forcing function for a
     future deliberate decision.)

The HTTP client additionally verifies the environment on every request, so
even if config is mutated at runtime, no production write can slip through.

During the initial build we run in MOCK mode — no HTTP calls to Joshu at all.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Literal

# Valid modes
ModeType = Literal["mock", "test", "production"]
VALID_MODES: set[ModeType] = {"mock", "test", "production"}

PRODUCTION_OVERRIDE_TOKEN = "yes-i-know-what-i-am-doing"


@dataclass(frozen=True)
class Settings:
    joshu_environment: ModeType
    joshu_base_url: str | None
    joshu_api_token: str | None
    session_secret: str
    allow_production: bool

    @property
    def is_mock(self) -> bool:
        return self.joshu_environment == "mock"

    @property
    def is_test(self) -> bool:
        return self.joshu_environment == "test"

    @property
    def is_production(self) -> bool:
        return self.joshu_environment == "production"


def load_settings() -> Settings:
    """
    Load settings from environment, enforcing the production guardrail.

    Raises SystemExit if the configuration would allow unsafe production access.
    """
    env_raw = os.environ.get("JOSHU_ENVIRONMENT", "").strip().lower()

    # Require explicit environment — never silently default
    if not env_raw:
        print(
            "FATAL: JOSHU_ENVIRONMENT is not set.\n"
            "  Set it to one of: mock, test, production\n"
            "  During initial development, use: JOSHU_ENVIRONMENT=mock",
            file=sys.stderr,
        )
        sys.exit(1)

    if env_raw not in VALID_MODES:
        print(
            f"FATAL: JOSHU_ENVIRONMENT='{env_raw}' is not valid.\n"
            f"  Valid values: {', '.join(sorted(VALID_MODES))}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Production requires deliberate override
    allow_prod_raw = os.environ.get("ALTRUIS_ALLOW_PRODUCTION", "").strip()
    allow_production = allow_prod_raw == PRODUCTION_OVERRIDE_TOKEN

    if env_raw == "production" and not allow_production:
        print(
            "FATAL: JOSHU_ENVIRONMENT=production was requested, but the\n"
            "production override flag is NOT set.\n"
            "\n"
            "  To use production, you must ALSO set:\n"
            f"    ALTRUIS_ALLOW_PRODUCTION={PRODUCTION_OVERRIDE_TOKEN}\n"
            "\n"
            "  This is intentional. Production writes require a deliberate,\n"
            "  reviewed decision — not a config mistake.",
            file=sys.stderr,
        )
        sys.exit(1)

    base_url = os.environ.get("JOSHU_BASE_URL", "").strip() or None
    api_token = os.environ.get("JOSHU_API_TOKEN", "").strip() or None

    # When not mocking, we need the real integration details
    if env_raw in ("test", "production"):
        if not base_url:
            print(
                f"FATAL: JOSHU_ENVIRONMENT='{env_raw}' requires JOSHU_BASE_URL.\n"
                "  Example: JOSHU_BASE_URL=https://altruis.joshu.insure",
                file=sys.stderr,
            )
            sys.exit(1)
        if not api_token:
            print(
                f"FATAL: JOSHU_ENVIRONMENT='{env_raw}' requires JOSHU_API_TOKEN.\n"
                "  Generate one from the Joshu Admin UI.",
                file=sys.stderr,
            )
            sys.exit(1)

    session_secret = os.environ.get(
        "SESSION_SECRET",
        "dev-secret-CHANGE-ME-in-production-please-please-please",
    )

    settings = Settings(
        joshu_environment=env_raw,  # type: ignore[arg-type]
        joshu_base_url=base_url,
        joshu_api_token=api_token,
        session_secret=session_secret,
        allow_production=allow_production,
    )

    # Emit the startup banner so it's obvious which environment we're in
    banner = {
        "mock":       "🟢 MOCK MODE — No calls will be made to Joshu at all.",
        "test":       "🟡 TEST MODE — API calls target the Joshu TEST container.",
        "production": "🔴 PRODUCTION MODE — LIVE DATA — OVERRIDE FLAG SET.",
    }[settings.joshu_environment]
    print("=" * 68, file=sys.stderr)
    print(f"  Altruis Broker Portal  ·  {banner}", file=sys.stderr)
    if settings.joshu_base_url:
        print(f"  Base URL: {settings.joshu_base_url}", file=sys.stderr)
    print("=" * 68, file=sys.stderr)

    return settings


# Loaded once at import time (FastAPI startup imports this module)
settings = load_settings()
