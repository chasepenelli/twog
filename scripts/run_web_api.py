"""Run the twog web API against Neon — the servable form of web_api.dispatch.

Public read routes (/public/*) need no auth and power the public STATE/EVIDENCE/RUNS site. The
auth-gated operator/contribute routes require a verified bearer token; until WorkOS is wired, the
verifier returns None (those routes 401), which is fine for the Tier-1 public launch.

Usage:
    NEON_DATABASE_URL=postgresql://...neon.tech/...?sslmode=require \
        PYTHONPATH=src python scripts/run_web_api.py [--host 0.0.0.0] [--port 8000]
"""

from __future__ import annotations

import argparse
import os
import pathlib

from hsa_research.ingestion_bridge.postgres_store import PostgresResearchRepository
from hsa_research.ingestion_bridge.service import HSAResearchService
from hsa_research.ingestion_bridge.web_api import run_api_server

_ENV_KEYS = ("NEON_DATABASE_URL", "DATABASE_URL", "POSTGRES_URL", "HSA_DATABASE_URL")


def _database_url() -> str:
    for key in _ENV_KEYS:
        if os.environ.get(key):
            return os.environ[key]
    env = pathlib.Path(__file__).resolve().parent.parent / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            for key in _ENV_KEYS:
                if line.startswith(f"{key}="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit(f"No database URL found (set one of {', '.join(_ENV_KEYS)} in env or .env).")


def _verify_token(_token: str) -> str | None:
    """WorkOS verification seam. Returns the stable auth_subject for a valid token, else None.
    Stubbed (always None) until WorkOS is stood up — public routes do not call this."""
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the twog web API against Neon.")
    parser.add_argument("--host", default=os.environ.get("TWOG_API_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("TWOG_API_PORT", "8000")))
    parser.add_argument("--allow-origin", default=os.environ.get("TWOG_API_ALLOW_ORIGIN", "*"))
    args = parser.parse_args()

    url = _database_url()

    # Per-request service so a dropped Neon connection (serverless idle timeout) never wedges the
    # server — each request gets a fresh, healthy connection via the pooled URL. Single-threaded
    # stdlib server + low launch traffic makes this cheap; swap in a pool if traffic grows.
    def _service_factory() -> HSAResearchService:
        return HSAResearchService(PostgresResearchRepository(url, seed=False))

    print(f"twog web API → http://{args.host}:{args.port}  (public reads open; gated routes 401 until WorkOS)")
    run_api_server(
        service_factory=_service_factory,
        verify_token=_verify_token,
        host=args.host,
        port=args.port,
        allow_origin=args.allow_origin,
    )


if __name__ == "__main__":
    main()
