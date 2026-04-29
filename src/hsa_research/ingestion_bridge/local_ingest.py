"""Local ingestion pipeline for Ingestion Bridge v2."""

from __future__ import annotations

from .chunker import chunk_text
from .contracts import IngestionResult, RunStatus, SourceQuery
from .dagster_assets import build_source_queries
from .harvesters_v2 import get_harvester
from .local_store import SQLiteResearchRepository, research_object_has_full_text
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

    def ingest_query(
        self,
        query: SourceQuery,
        limit: int = 25,
        *,
        query_param_overrides: dict | None = None,
        query_name_suffix: str | None = None,
        persist_query: bool = True,
    ) -> IngestionResult:
        """Fetch a source query and persist raw records plus research objects."""

        if query_param_overrides or query_name_suffix:
            query = query.model_copy(
                update={
                    "query_name": f"{query.query_name}:{query_name_suffix}" if query_name_suffix else query.query_name,
                    "query_params": {
                        **query.query_params,
                        **(query_param_overrides or {}),
                    },
                }
            )
        if persist_query:
            self.repository.upsert_source_query(query)
        fetch_run_id = self.repository.create_fetch_run(query.source_key, query.query_name)
        raw_count = 0
        object_count = 0
        chunk_count = 0
        full_text_object_count = 0
        section_chunk_counts: dict[str, int] = {}
        errors: list[str] = []

        try:
            harvester = get_harvester(query.source_key)
            records = harvester.fetch(query.query_text, limit=limit, **query.query_params)
            for record in records:
                raw_id = self.repository.upsert_raw_record(record.raw_record, fetch_run_id)
                object_id = self.repository.upsert_research_object(record.research_object, raw_id)
                if record.research_object.metadata.get("full_text_available"):
                    full_text_object_count += 1
                persisted_object = self.repository.get_research_object(object_id)
                preserve_existing_chunks = (
                    research_object_has_full_text(persisted_object)
                    and not research_object_has_full_text(record.research_object)
                )
                if preserve_existing_chunks:
                    raw_count += 1
                    object_count += 1
                    continue
                doc_chunks = []
                next_chunk_index = 0
                for section_label, section_text in harvester.chunk_text_sections(record):
                    section_chunks = chunk_text(
                        object_id,
                        section_text,
                        section_label=section_label,
                        start_index=next_chunk_index,
                        metadata={
                            "source_key": query.source_key,
                            "query_name": query.query_name,
                            "harvester": "v2",
                            "section_label": section_label,
                            "full_text_available": bool(record.research_object.metadata.get("full_text_available")),
                        },
                    )
                    next_chunk_index += len(section_chunks)
                    section_chunk_counts[section_label] = section_chunk_counts.get(section_label, 0) + len(section_chunks)
                    doc_chunks.extend(section_chunks)
                if hasattr(self.repository, "replace_document_chunks"):
                    written_chunks = self.repository.replace_document_chunks(object_id, doc_chunks)
                    chunk_count += len(written_chunks)
                else:
                    for doc_chunk in doc_chunks:
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
            full_text_research_objects=full_text_object_count,
            section_chunk_counts=section_chunk_counts,
            status=status,
            errors=errors,
        )

    def ingest_source(
        self,
        source_key: str,
        limit: int = 25,
        *,
        query_param_overrides: dict | None = None,
        query_name_suffix: str | None = None,
        persist_query_overrides: bool = False,
    ) -> list[IngestionResult]:
        """Run all active local queries for one source."""

        queries = self.repository.list_source_queries(source_key=source_key)
        return [
            self.ingest_query(
                query,
                limit=limit,
                query_param_overrides=query_param_overrides,
                query_name_suffix=query_name_suffix,
                persist_query=not (query_param_overrides or query_name_suffix) or persist_query_overrides,
            )
            for query in queries
        ]

    def coverage(self) -> dict:
        return self.repository.coverage_summary()
