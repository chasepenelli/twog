"""Dagster resources for the Ingestion Bridge asset graph."""

from __future__ import annotations

import os

from .local_store import SQLiteResearchRepository
from .repository import ResearchRepository

try:
    import dagster as dg
except ImportError:  # pragma: no cover - Dagster is optional for non-orchestration imports
    dg = None  # type: ignore[assignment]


if dg is not None:

    class ResearchRepositoryResource(dg.ConfigurableResource):
        """Build repository adapters from Dagster resource configuration."""

        storage_backend: str | None = None
        database_url: str | None = None
        sqlite_path: str | None = None

        def build_repository(self) -> ResearchRepository:
            backend = (self.storage_backend or os.getenv("HSA_STORAGE_BACKEND", "sqlite")).lower()
            database_url = self.database_url or os.getenv("HSA_DATABASE_URL")
            sqlite_path = self.sqlite_path or os.getenv("HSA_SQLITE_PATH")

            if backend == "sqlite":
                return SQLiteResearchRepository(sqlite_path) if sqlite_path else SQLiteResearchRepository()
            if backend == "postgres":
                if not database_url:
                    raise RuntimeError("HSA_STORAGE_BACKEND=postgres requires HSA_DATABASE_URL")
                from .postgres_store import PostgresResearchRepository

                return PostgresResearchRepository(database_url)
            if backend == "memory":
                raise RuntimeError("The ingestion pipeline requires sqlite or postgres storage")
            raise ValueError(f"Unsupported HSA_STORAGE_BACKEND: {backend}")


else:
    ResearchRepositoryResource = None  # type: ignore[assignment]
