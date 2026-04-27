"""Local ingestion pipeline for Ingestion Bridge v2."""

from __future__ import annotations

from .chunker import chunk_text
from .contracts import IngestionResult, RunStatus, SourceQuery
from .dagster_assets import build_source_queries
from .harvesters_v2 import get_harvester
from .local_store import SQLiteResearchRepository
from .storage import build_sql_repository
from .source_registry import get_initial_sources

LEGACY_STARTER_QUERY_KEYS = (
    ("pubmed", "canine_hsa_core"),
    ("europe_pmc", "canine_hsa_open_access"),
    ("openalex", "hsa_comparative_oncology"),
    ("crossref", "hsa_journal_backbone"),
)


class LocalIngestionPipeline:
    """Small local-first ingestion runner used by CLI, MCP, and smoke tests."""

    def __init__(self, repository: SQLiteResearchRepository | None = None) -> None:
        self.repository = repository or build_sql_repository()

    def initialize(self) -> dict:
        """Seed source registry and starter source queries."""

        sources = get_initial_sources()
        queries = build_source_queries()
        self.repository.seed_sources(sources)
        for source_key, query_name in LEGACY_STARTER_QUERY_KEYS:
            self.repository.set_source_query_active(source_key, query_name, False)
        for query in queries:
            self.repository.upsert_source_query(query)
        return {
            "sources": len(sources),
            "source_queries": len(queries),
            "coverage": self.repository.coverage_summary(),
        }

    def ingest_query(self, query: SourceQuery, limit: int = 25) -> IngestionResult:
        """Fetch a source query and persist raw records plus research objects."""

        self.repository.upsert_source_query(query)
        fetch_run_id = self.repository.create_fetch_run(query.source_key, query.query_name)
        raw_count = 0
        object_count = 0
        chunk_count = 0
        errors: list[str] = []

        try:
            harvester = get_harvester(query.source_key)
            records = harvester.fetch(query.query_text, limit=limit, **query.query_params)
            for record in records:
                raw_id = self.repository.upsert_raw_record(record.raw_record, fetch_run_id)
                object_id = self.repository.upsert_research_object(record.research_object, raw_id)
                for doc_chunk in chunk_text(
                    object_id,
                    harvester.text_for_chunking(record),
                    section_label=harvester.chunk_section_label(record),
                    metadata={
                        "source_key": query.source_key,
                        "query_name": query.query_name,
                        "harvester": "v2",
                    },
                ):
                    self.repository.upsert_document_chunk(doc_chunk)
                    chunk_count += 1
                raw_count += 1
                object_count += 1
            self.repository.finish_fetch_run(
                fetch_run_id,
                "completed",
                records_found=len(records),
                records_inserted=raw_count,
            )
            status = RunStatus.COMPLETED
        except Exception as exc:
            errors.append(str(exc))
            self.repository.finish_fetch_run(fetch_run_id, "failed", error_message=str(exc))
            status = RunStatus.FAILED

        return IngestionResult(
            source_key=query.source_key,
            query_name=query.query_name,
            query_text=query.query_text,
            fetch_run_id=fetch_run_id,
            raw_records=raw_count,
            research_objects=object_count,
            document_chunks=chunk_count,
            status=status,
            errors=errors,
        )

    def ingest_source(self, source_key: str, limit: int = 25) -> list[IngestionResult]:
        """Run all active local queries for one source."""

        queries = self.repository.list_source_queries(source_key=source_key)
        return [self.ingest_query(query, limit=limit) for query in queries]

    def coverage(self) -> dict:
        return self.repository.coverage_summary()
