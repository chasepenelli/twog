#!/usr/bin/env python3
"""Apply Proof Network migrations to a Neon/Postgres database.

Applies four idempotent migrations in order:

    db/migrations/008_work_packets.sql
    db/migrations/009_proof_capsules.sql
    twog/db/migrations/003_work_packets.sql
    twog/db/migrations/004_proof_capsules.sql

All four are CREATE TABLE IF NOT EXISTS + CREATE INDEX IF NOT EXISTS, so
reruns are safe. Use this against a Neon staging branch first.

Reads the connection string from (in order):
    NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, HSA_DATABASE_URL

Usage:
    HSA_DATABASE_URL='<url>' PYTHONPATH=src uv run python \\
      scripts/apply_proof_network_migrations.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import psycopg2


REPO_ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS = [
    REPO_ROOT / "db" / "migrations" / "008_work_packets.sql",
    REPO_ROOT / "db" / "migrations" / "009_proof_capsules.sql",
    REPO_ROOT / "db" / "migrations" / "010_proof_capsule_submission_rate.sql",
    REPO_ROOT / "twog" / "db" / "migrations" / "003_work_packets.sql",
    REPO_ROOT / "twog" / "db" / "migrations" / "004_proof_capsules.sql",
    REPO_ROOT / "twog" / "db" / "migrations" / "005_proof_capsule_submission_rate.sql",
]


def _resolve_url() -> str | None:
    for var in ("NEON_DATABASE_URL", "DATABASE_URL", "POSTGRES_URL", "HSA_DATABASE_URL"):
        value = os.environ.get(var)
        if value:
            return value
    return None


def main() -> int:
    url = _resolve_url()
    if not url:
        print(
            "Missing connection string. Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL.",
            file=sys.stderr,
        )
        return 1

    # Print host (no creds) so the operator sees which DB is being touched.
    parsed = urlparse(url)
    print(f"Target: {parsed.hostname}{parsed.path}")

    try:
        with psycopg2.connect(url) as connection:
            with connection.cursor() as cursor:
                for migration in MIGRATIONS:
                    rel = migration.relative_to(REPO_ROOT)
                    print(f"Applying {rel} …", end=" ", flush=True)
                    sql = migration.read_text()
                    cursor.execute(sql)
                    print("ok")
                connection.commit()

                cursor.execute(
                    """
                    select table_name
                    from information_schema.tables
                    where table_schema = 'public'
                      and table_name in (
                          'work_packets',
                          'proof_capsules',
                          'proof_capsule_reviews',
                          'proof_capsule_submission_rate'
                      )
                    order by table_name
                    """
                )
                print("\nTables now present in public schema:")
                for (name,) in cursor.fetchall():
                    print(f"  - {name}")
    except psycopg2.Error as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
