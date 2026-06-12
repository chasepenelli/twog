"""Materialize and verify the TWOG v2.1 schema against a fresh Neon (Postgres) database.

The app self-bootstraps its schema: constructing PostgresResearchRepository runs
_init_schema(), which issues 42 `CREATE TABLE IF NOT EXISTS` statements. So pointing
this at an empty Neon branch builds the full v2.1 schema with the new provider_job_id
naming — no migration files needed.

Usage:
    export NEON_DATABASE_URL='postgresql://...neon.tech/neondb?sslmode=require'
    python scripts/init_neon_schema.py
"""

from __future__ import annotations

import os
import sys

from hsa_research.ingestion_bridge.postgres_store import PostgresResearchRepository

_ENV_KEYS = ("NEON_DATABASE_URL", "DATABASE_URL", "POSTGRES_URL", "HSA_DATABASE_URL")


def _database_url() -> str:
    for key in _ENV_KEYS:
        val = os.getenv(key)
        if val and val.strip():
            print(f"Using {key}")
            return val.strip()
    sys.exit(f"No connection string. Set one of: {', '.join(_ENV_KEYS)}")


def main() -> None:
    url = _database_url()
    # Constructing the repo runs _init_schema() -> creates all tables if absent.
    repo = PostgresResearchRepository(url, seed=False)
    cur = repo.conn.cursor()

    cur.execute("select count(*) as n from information_schema.tables where table_schema = 'public'")
    n_tables = cur.fetchone()["n"]

    # Confirm the rename landed and no RunPod-era column survived.
    cur.execute(
        """
        select table_name, column_name
        from information_schema.columns
        where table_schema = 'public' and column_name in ('provider_job_id', 'runpod_job_id')
        order by table_name, column_name
        """
    )
    cols = [(r["table_name"], r["column_name"]) for r in cur.fetchall()]
    provider = sorted(t for t, c in cols if c == "provider_job_id")
    legacy = sorted(t for t, c in cols if c == "runpod_job_id")

    print(f"\nPublic tables created: {n_tables}")
    print(f"provider_job_id present on: {provider or '(none — unexpected)'}")
    print(f"legacy runpod_job_id on:    {legacy or '(none — good)'}")

    ok = n_tables >= 40 and "compute_jobs" in provider and not legacy
    print("\n✅ Schema looks correct for v2.1." if ok else "\n⚠️  Schema check did not pass — inspect above.")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
