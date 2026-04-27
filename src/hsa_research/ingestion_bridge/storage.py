"""Repository factory for local and hosted ingestion bridge runtimes."""

from __future__ import annotations

import os
from pathlib import Path

from .local_store import SQLiteResearchRepository
from .repository import InMemoryResearchRepository, ResearchRepository


def build_research_repository(db_path: str | Path | None = None) -> ResearchRepository:
    """Build a repository from environment configuration.

    Supported backends:
    - ``sqlite``: default local repository.
    - ``postgres``: hosted runtime repository for Dagster+.
    - ``memory``: ephemeral smoke-test repository.
    """

    backend = os.getenv("HSA_STORAGE_BACKEND", "sqlite").lower()
    if db_path is not None:
        return SQLiteResearchRepository(db_path)
    if backend == "memory":
        return InMemoryResearchRepository()
    if backend == "sqlite":
        return SQLiteResearchRepository()
    if backend == "postgres":
        database_url = os.getenv("HSA_DATABASE_URL")
        if not database_url:
            raise RuntimeError("HSA_STORAGE_BACKEND=postgres requires HSA_DATABASE_URL")
        from .postgres_store import PostgresResearchRepository

        return PostgresResearchRepository(database_url)
    raise ValueError(f"Unsupported HSA_STORAGE_BACKEND: {backend}")


def build_sql_repository(db_path: str | Path | None = None):
    """Build a repository that supports ingestion, extraction, curation, and QA."""

    repository = build_research_repository(db_path)
    if isinstance(repository, InMemoryResearchRepository):
        raise RuntimeError("The ingestion pipeline requires sqlite or postgres storage")
    return repository
