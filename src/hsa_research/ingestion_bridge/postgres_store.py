"""Postgres runtime repository for hosted Dagster+ execution.

This adapter intentionally mirrors the working local SQLite payload schema. It
is the hosted runtime bridge for Dagster+ while the richer normalized Postgres
schema is still being designed.
"""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from .contracts import (
    AgentRunRecord,
    AgentRunReviewRecord,
    ArtifactHandle,
    AsyncRunHandle,
    CandidateDossier,
    CandidateDossierRequest,
    ClaimSearchRequest,
    ClaimSearchResult,
    CommandCenterActivityEventRecord,
    CommandCenterBoardStageRecord,
    ComputeJobRecord,
    CommitHypothesisRequest,
    DocumentChunk,
    EmbeddingCoverageSummary,
    EntityAlias,
    EntityMention,
    MDExpertApprovalRecord,
    MDExpertReviewPacketRecord,
    RawSourceRecord,
    ResearchChunkSearchRequest,
    ResearchChunkSearchResult,
    ResearchProgramRecord,
    ResearchBriefEvaluationRecord,
    ResearchBriefQueueItem,
    ResearchBriefRecord,
    ResearchLeadRecord,
    ResolvedEntity,
    ResearchObject,
    ResearchSource,
    ScrapeReviewRecord,
    ScrapeSourceProfileReview,
    SourceFollowupQueueItem,
    SourceQuery,
    TextEmbedding,
    TextEmbeddingSearchRequest,
    TextEmbeddingSearchResult,
    TherapyIdeaRecord,
    ValidationDecisionRecord,
    ValidationPlanRecord,
    ValidationRequest,
    ValidationRequestQueueItem,
)
from .local_store import (
    build_research_object_dedupe_key,
    document_chunk_from_payload,
    should_preserve_existing_research_object,
)
from .repository import ResearchRepository, cosine_similarity, keyword_chunk_score, keyword_terms, seed_claims


class PostgresResearchRepository(ResearchRepository):
    """Postgres-backed repository using the local runtime payload contract."""

    def __init__(self, database_url: str, seed: bool = True) -> None:
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
        except ImportError as exc:  # pragma: no cover - exercised only in hosted config
            raise RuntimeError("Postgres storage requires psycopg2-binary") from exc

        self.database_url = database_url
        self._json = _load_json_adapter()
        self.conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
        self._init_schema()
        if seed:
            self._seed_claims_if_empty()

    @property
    def db_path(self) -> Path:
        return Path("postgres://configured")

    def seed_sources(self, sources: list[ResearchSource]) -> None:
        for source in sources:
            self.upsert_source(source)

    def upsert_source(self, source: ResearchSource) -> ResearchSource:
        payload = source.model_dump(mode="json")
        self._execute(
            """
            insert into ingestion_sources (
              source_key, display_name, source_kind, base_url, documentation_url,
              license_policy, requires_api_key, enabled, priority, phase,
              rate_limit_per_minute, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(source_key) do update set
              display_name = excluded.display_name,
              source_kind = excluded.source_kind,
              base_url = excluded.base_url,
              documentation_url = excluded.documentation_url,
              license_policy = excluded.license_policy,
              requires_api_key = excluded.requires_api_key,
              enabled = excluded.enabled,
              priority = excluded.priority,
              phase = excluded.phase,
              rate_limit_per_minute = excluded.rate_limit_per_minute,
              payload = excluded.payload,
              updated_at = now()
            """,
            (
                source.source_key,
                source.display_name,
                str(source.source_kind),
                str(source.base_url) if source.base_url else None,
                str(source.documentation_url) if source.documentation_url else None,
                source.license_policy,
                source.requires_api_key,
                source.enabled,
                source.priority,
                source.phase,
                source.rate_limit_per_minute,
                self._json(payload),
            ),
        )
        return source

    def list_sources(self, enabled_only: bool = False) -> list[ResearchSource]:
        sql = "select payload from ingestion_sources"
        params: list[object] = []
        if enabled_only:
            sql += " where enabled = %s"
            params.append(True)
        sql += " order by priority asc, source_key asc"
        return [ResearchSource.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def upsert_source_query(self, query: SourceQuery) -> SourceQuery:
        payload = query.model_dump(mode="json")
        self._execute(
            """
            insert into source_queries (
              source_key, query_name, query_text, query_params, track, object_type, active, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(source_key, query_name) do update set
              query_text = excluded.query_text,
              query_params = excluded.query_params,
              track = excluded.track,
              object_type = excluded.object_type,
              active = excluded.active,
              payload = excluded.payload,
              updated_at = now()
            """,
            (
                query.source_key,
                query.query_name,
                query.query_text,
                self._json(query.query_params),
                query.track,
                str(query.object_type) if query.object_type else None,
                query.active,
                self._json(payload),
            ),
        )
        return query

    def list_source_queries(self, source_key: str | None = None, active_only: bool = True) -> list[SourceQuery]:
        clauses: list[str] = []
        params: list[object] = []
        if source_key:
            clauses.append("source_key = %s")
            params.append(source_key)
        if active_only:
            clauses.append("active = %s")
            params.append(True)
        sql = "select payload from source_queries"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by source_key asc, query_name asc"
        return [SourceQuery.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def set_source_query_active(self, source_key: str, query_name: str, active: bool) -> None:
        row = self._fetchone(
            "select payload from source_queries where source_key = %s and query_name = %s",
            (source_key, query_name),
        )
        if row is None:
            return
        query = SourceQuery.model_validate(_payload(row))
        updated = query.model_copy(update={"active": active})
        payload = updated.model_dump(mode="json")
        self._execute(
            """
            update source_queries
            set active = %s, payload = %s, updated_at = now()
            where source_key = %s and query_name = %s
            """,
            (active, self._json(payload), source_key, query_name),
        )

    def create_fetch_run(self, source_key: str, query_name: str | None = None) -> UUID:
        fetch_run_id = uuid4()
        self._execute(
            """
            insert into source_fetch_runs (fetch_run_id, source_key, query_name, status, started_at)
            values (%s, %s, %s, 'running', now())
            """,
            (str(fetch_run_id), source_key, query_name),
        )
        return fetch_run_id

    def finish_fetch_run(
        self,
        fetch_run_id: UUID,
        status: str,
        records_found: int = 0,
        records_inserted: int = 0,
        records_updated: int = 0,
        error_message: str | None = None,
    ) -> None:
        self._execute(
            """
            update source_fetch_runs
            set status = %s,
                records_found = %s,
                records_inserted = %s,
                records_updated = %s,
                error_message = %s,
                completed_at = now(),
                updated_at = now()
            where fetch_run_id = %s
            """,
            (
                status,
                records_found,
                records_inserted,
                records_updated,
                error_message,
                str(fetch_run_id),
            ),
        )

    def upsert_raw_record(self, record: RawSourceRecord, fetch_run_id: UUID | None = None) -> UUID:
        raw_record_id = record.id or uuid4()
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into raw_source_records (
              raw_record_id, source_key, fetch_run_id, source_record_id, source_url,
              content_hash, payload, retrieved_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(source_key, content_hash) do update set
              fetch_run_id = excluded.fetch_run_id,
              source_record_id = excluded.source_record_id,
              source_url = excluded.source_url,
              payload = excluded.payload,
              last_seen_at = now(),
              updated_at = now()
            """,
            (
                str(raw_record_id),
                record.source_key,
                str(fetch_run_id) if fetch_run_id else None,
                record.source_record_id,
                record.source_url,
                record.content_hash,
                self._json(payload),
                record.retrieved_at,
            ),
        )
        row = self._fetchone(
            "select raw_record_id from raw_source_records where source_key = %s and content_hash = %s",
            (record.source_key, record.content_hash),
        )
        return UUID(row["raw_record_id"])

    def upsert_research_object(self, obj: ResearchObject, raw_record_id: UUID | None = None) -> UUID:
        object_id = obj.id or uuid4()
        dedupe_key = obj.dedupe_key or build_research_object_dedupe_key(obj)
        existing = self._fetchone("select object_id, payload from research_objects where dedupe_key = %s", (dedupe_key,))
        if existing is not None:
            existing_obj = ResearchObject.model_validate(_payload(existing))
            object_id = UUID(existing["object_id"])
            if should_preserve_existing_research_object(existing_obj, obj):
                for identifier_type, identifier_value in obj.identifiers.items():
                    if identifier_value:
                        self.link_identifier(object_id, identifier_type, identifier_value)
                return object_id
        payload = obj.model_copy(update={"id": object_id, "raw_record_id": raw_record_id, "dedupe_key": dedupe_key}).model_dump(
            mode="json"
        )
        self._execute(
            """
            insert into research_objects (
              object_id, object_type, title, abstract, canonical_url, publication_year,
              published_at, source_key, raw_record_id, dedupe_key, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(dedupe_key) do update set
              object_type = excluded.object_type,
              title = excluded.title,
              abstract = excluded.abstract,
              canonical_url = excluded.canonical_url,
              publication_year = excluded.publication_year,
              published_at = excluded.published_at,
              source_key = excluded.source_key,
              raw_record_id = excluded.raw_record_id,
              payload = excluded.payload,
              updated_at = now()
            """,
            (
                str(object_id),
                str(obj.object_type),
                obj.title,
                obj.abstract,
                obj.canonical_url,
                obj.publication_year,
                obj.published_at,
                obj.source_key,
                str(raw_record_id) if raw_record_id else None,
                dedupe_key,
                self._json(payload),
            ),
        )
        row = self._fetchone("select object_id from research_objects where dedupe_key = %s", (dedupe_key,))
        object_uuid = UUID(row["object_id"])
        for identifier_type, identifier_value in obj.identifiers.items():
            if identifier_value:
                self.link_identifier(object_uuid, identifier_type, identifier_value)
        return object_uuid

    def link_identifier(self, object_id: UUID, identifier_type: str, identifier_value: str) -> None:
        self._execute(
            """
            insert into identifier_links (object_id, identifier_type, identifier_value)
            values (%s, %s, %s)
            on conflict(object_id, identifier_type, identifier_value) do nothing
            """,
            (str(object_id), identifier_type, identifier_value),
        )

    def update_research_object(self, obj: ResearchObject) -> ResearchObject | None:
        existing = self._fetchone("select object_id from research_objects where object_id = %s", (str(obj.id),))
        if existing is None:
            return None
        dedupe_key = obj.dedupe_key or build_research_object_dedupe_key(obj)
        conflict = self._fetchone(
            "select object_id from research_objects where dedupe_key = %s and object_id != %s",
            (dedupe_key, str(obj.id)),
        )
        if conflict is not None:
            raise ValueError(f"Research object dedupe key already exists: {dedupe_key}")
        payload = obj.model_copy(update={"dedupe_key": dedupe_key}).model_dump(mode="json")
        self._execute(
            """
            update research_objects
            set object_type = %s,
                title = %s,
                abstract = %s,
                canonical_url = %s,
                publication_year = %s,
                published_at = %s,
                source_key = %s,
                raw_record_id = %s,
                dedupe_key = %s,
                payload = %s,
                updated_at = now()
            where object_id = %s
            """,
            (
                str(obj.object_type),
                obj.title,
                obj.abstract,
                obj.canonical_url,
                obj.publication_year,
                obj.published_at,
                obj.source_key,
                str(obj.raw_record_id) if obj.raw_record_id else None,
                dedupe_key,
                self._json(payload),
                str(obj.id),
            ),
        )
        self._execute("delete from identifier_links where object_id = %s", (str(obj.id),))
        for identifier_type, identifier_value in payload.get("identifiers", {}).items():
            if identifier_value:
                self.link_identifier(obj.id, identifier_type, identifier_value)
        return ResearchObject.model_validate(payload)

    def get_research_object(self, object_id: UUID) -> ResearchObject | None:
        row = self._fetchone("select payload from research_objects where object_id = %s", (str(object_id),))
        if row is None:
            return None
        return ResearchObject.model_validate(_payload(row))

    def list_research_objects(
        self,
        object_type: str | None = None,
        source_key: str | None = None,
        limit: int | None = None,
    ) -> list[ResearchObject]:
        clauses: list[str] = []
        params: list[object] = []
        if object_type:
            clauses.append("object_type = %s")
            params.append(object_type)
        if source_key:
            clauses.append("source_key = %s")
            params.append(source_key)
        sql = "select payload from research_objects"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by updated_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [ResearchObject.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def get_document_chunk(self, chunk_id: UUID) -> DocumentChunk | None:
        row = self._fetchone(
            "select chunk_id, object_id, payload from document_chunks where chunk_id = %s",
            (str(chunk_id),),
        )
        if row is None:
            return None
        return document_chunk_from_payload(
            _payload(row),
            chunk_id=row["chunk_id"],
            object_id=row["object_id"],
        )

    def get_raw_record_payload(self, raw_record_id: UUID) -> dict[str, Any] | None:
        row = self._fetchone("select payload from raw_source_records where raw_record_id = %s", (str(raw_record_id),))
        if row is None:
            return None
        return _payload(row)

    def upsert_entity(self, entity: ResolvedEntity) -> ResolvedEntity:
        payload = entity.model_dump(mode="json")
        self._execute(
            """
            insert into resolved_entities (
              entity_id, entity_type, canonical_name, normalized_key,
              resolver_name, resolver_version, confidence, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(entity_type, normalized_key) do update set
              canonical_name = excluded.canonical_name,
              resolver_name = excluded.resolver_name,
              resolver_version = excluded.resolver_version,
              confidence = excluded.confidence,
              payload = excluded.payload,
              updated_at = now()
            """,
            (
                str(entity.entity_id),
                entity.entity_type,
                entity.canonical_name,
                entity.normalized_key,
                entity.resolver_name,
                entity.resolver_version,
                entity.confidence,
                self._json(payload),
            ),
        )
        row = self._fetchone(
            "select payload from resolved_entities where entity_type = %s and normalized_key = %s",
            (entity.entity_type, entity.normalized_key),
        )
        return ResolvedEntity.model_validate(_payload(row))

    def list_entities(
        self,
        *,
        entity_type: str | None = None,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[ResolvedEntity]:
        clauses: list[str] = []
        params: list[object] = []
        if entity_type:
            clauses.append("entity_type = %s")
            params.append(entity_type)
        if query:
            clauses.append("(canonical_name ilike %s or normalized_key ilike %s)")
            params.extend((f"%{query}%", f"%{query}%"))
        sql = "select payload from resolved_entities"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by entity_type, canonical_name"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [ResolvedEntity.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def upsert_entity_alias(self, alias: EntityAlias) -> EntityAlias:
        payload = alias.model_dump(mode="json")
        self._execute(
            """
            insert into entity_aliases (
              alias_id, entity_id, entity_type, alias, alias_normalized,
              canonical_name, normalized_key, resolver_name, resolver_version, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(entity_type, alias_normalized, normalized_key) do update set
              entity_id = excluded.entity_id,
              alias = excluded.alias,
              canonical_name = excluded.canonical_name,
              resolver_name = excluded.resolver_name,
              resolver_version = excluded.resolver_version,
              payload = excluded.payload,
              updated_at = now()
            """,
            (
                str(alias.alias_id),
                str(alias.entity_id),
                alias.entity_type,
                alias.alias,
                alias.alias_normalized,
                alias.canonical_name,
                alias.normalized_key,
                alias.resolver_name,
                alias.resolver_version,
                self._json(payload),
            ),
        )
        row = self._fetchone(
            """
            select payload from entity_aliases
            where entity_type = %s and alias_normalized = %s and normalized_key = %s
            """,
            (alias.entity_type, alias.alias_normalized, alias.normalized_key),
        )
        return EntityAlias.model_validate(_payload(row))

    def upsert_entity_mention(self, mention: EntityMention) -> EntityMention:
        payload = mention.model_dump(mode="json")
        self._execute(
            """
            insert into entity_mentions (
              mention_id, entity_id, object_id, chunk_id, chunk_index,
              source_key, entity_type, canonical_name, normalized_key,
              matched_text, matched_alias, chunk_char_start, chunk_char_end,
              resolver_name, resolver_version, confidence, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(chunk_id, entity_type, normalized_key, chunk_char_start, chunk_char_end, resolver_name) do update set
              entity_id = excluded.entity_id,
              canonical_name = excluded.canonical_name,
              matched_text = excluded.matched_text,
              matched_alias = excluded.matched_alias,
              confidence = excluded.confidence,
              payload = excluded.payload,
              updated_at = now()
            """,
            (
                str(mention.mention_id),
                str(mention.entity_id) if mention.entity_id else None,
                str(mention.research_object_id),
                str(mention.chunk_id),
                mention.chunk_index,
                mention.source_key,
                mention.entity_type,
                mention.canonical_name,
                mention.normalized_key,
                mention.matched_text,
                mention.matched_alias,
                mention.chunk_char_start,
                mention.chunk_char_end,
                mention.resolver_name,
                mention.resolver_version,
                mention.confidence,
                self._json(payload),
            ),
        )
        row = self._fetchone(
            """
            select payload from entity_mentions
            where chunk_id = %s and entity_type = %s and normalized_key = %s
              and chunk_char_start = %s and chunk_char_end = %s and resolver_name = %s
            """,
            (
                str(mention.chunk_id),
                mention.entity_type,
                mention.normalized_key,
                mention.chunk_char_start,
                mention.chunk_char_end,
                mention.resolver_name,
            ),
        )
        return EntityMention.model_validate(_payload(row))

    def list_entity_mentions(
        self,
        *,
        source_key: str | None = None,
        object_id: UUID | None = None,
        chunk_id: UUID | None = None,
        entity_type: str | None = None,
        limit: int | None = None,
    ) -> list[EntityMention]:
        clauses: list[str] = []
        params: list[object] = []
        if source_key:
            clauses.append("source_key = %s")
            params.append(source_key)
        if object_id:
            clauses.append("object_id = %s")
            params.append(str(object_id))
        if chunk_id:
            clauses.append("chunk_id = %s")
            params.append(str(chunk_id))
        if entity_type:
            clauses.append("entity_type = %s")
            params.append(entity_type)
        sql = "select payload from entity_mentions"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by object_id, chunk_index, chunk_char_start"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [EntityMention.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def upsert_document_chunk(self, chunk: DocumentChunk) -> DocumentChunk:
        payload = chunk.model_dump(mode="json")
        self._execute(
            """
            insert into document_chunks (
              chunk_id, object_id, chunk_index, section_label, text_content,
              content_hash, token_count, char_start, char_end, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(object_id, chunk_index) do update set
              section_label = excluded.section_label,
              text_content = excluded.text_content,
              content_hash = excluded.content_hash,
              token_count = excluded.token_count,
              char_start = excluded.char_start,
              char_end = excluded.char_end,
              payload = excluded.payload,
              updated_at = now()
            """,
            (
                str(chunk.id),
                str(chunk.research_object_id),
                chunk.chunk_index,
                chunk.section_label,
                chunk.text_content,
                chunk.content_hash,
                chunk.token_count,
                chunk.char_start,
                chunk.char_end,
                self._json(payload),
            ),
        )
        row = self._fetchone(
            """
            select chunk_id, object_id, payload
            from document_chunks
            where object_id = %s and chunk_index = %s
            """,
            (str(chunk.research_object_id), chunk.chunk_index),
        )
        if row is None:
            return chunk
        return document_chunk_from_payload(
            _payload(row),
            chunk_id=row["chunk_id"],
            object_id=row["object_id"],
        )

    def replace_document_chunks(self, object_id: UUID, chunks: list[DocumentChunk]) -> list[DocumentChunk]:
        """Replace all chunks for an object and clear derived chunk-level rows."""

        object_id_text = str(object_id)
        self._execute("delete from entity_mentions where object_id = %s", (object_id_text,))
        self._execute("delete from text_embeddings where object_id = %s", (object_id_text,))
        self._execute("delete from document_chunks where object_id = %s", (object_id_text,))
        return [self.upsert_document_chunk(chunk) for chunk in chunks]

    def list_document_chunks(
        self,
        object_id: UUID | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
        limit: int | None = None,
    ) -> list[DocumentChunk]:
        params: list[object] = []
        sql = "select dc.chunk_id, dc.object_id, dc.payload from document_chunks dc"
        if source_key or object_type:
            sql += " join research_objects ro on ro.object_id = dc.object_id"
        clauses: list[str] = []
        if object_id:
            clauses.append("dc.object_id = %s")
            params.append(str(object_id))
        if source_key:
            clauses.append("ro.source_key = %s")
            params.append(source_key)
        if object_type:
            clauses.append("ro.object_type = %s")
            params.append(object_type)
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by dc.object_id, dc.chunk_index"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [
            document_chunk_from_payload(
                _payload(row),
                chunk_id=row["chunk_id"],
                object_id=row["object_id"],
            )
            for row in self._fetchall(sql, params)
        ]

    def list_document_chunks_for_fetch_runs(
        self,
        fetch_run_ids: list[UUID],
        limit: int | None = None,
    ) -> list[DocumentChunk]:
        if not fetch_run_ids:
            return []
        placeholders = ", ".join("%s" for _ in fetch_run_ids)
        params: list[object] = [str(fetch_run_id) for fetch_run_id in fetch_run_ids]
        sql = f"""
            select dc.chunk_id, dc.object_id, dc.payload
            from document_chunks dc
            join research_objects ro on ro.object_id = dc.object_id
            join raw_source_records rr on rr.raw_record_id = ro.raw_record_id
            where rr.fetch_run_id in ({placeholders})
            order by dc.object_id, dc.chunk_index
        """
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [
            document_chunk_from_payload(
                _payload(row),
                chunk_id=row["chunk_id"],
                object_id=row["object_id"],
            )
            for row in self._fetchall(sql, params)
        ]

    def search_research_chunks(self, request: ResearchChunkSearchRequest) -> list[ResearchChunkSearchResult]:
        terms = keyword_terms(request.query)
        if not terms:
            return []

        params: list[object] = []
        clauses: list[str] = []
        if request.research_object_id:
            clauses.append("dc.object_id = %s")
            params.append(str(request.research_object_id))
        if request.source_key:
            clauses.append("ro.source_key = %s")
            params.append(request.source_key)
        if request.object_type:
            clauses.append("ro.object_type = %s")
            params.append(str(request.object_type))

        term_clauses: list[str] = []
        for term in terms:
            like_term = f"%{term}%"
            term_clauses.append(
                """
                (
                  dc.text_content ilike %s
                  or coalesce(dc.section_label, '') ilike %s
                  or coalesce(ro.title, '') ilike %s
                  or coalesce(ro.abstract, '') ilike %s
                )
                """
            )
            params.extend([like_term, like_term, like_term, like_term])
        clauses.append("(" + " or ".join(term_clauses) + ")")

        fetch_limit = min(max(request.limit * 200, 1000), 5000)
        rows = self._fetchall(
            f"""
            select
              dc.chunk_id as chunk_id,
              dc.object_id as chunk_object_id,
              dc.payload as chunk_payload,
              ro.payload as object_payload
            from document_chunks dc
            join research_objects ro on ro.object_id = dc.object_id
            where {' and '.join(clauses)}
            order by dc.object_id, dc.chunk_index
            limit %s
            """,
            [*params, fetch_limit],
        )
        results: list[ResearchChunkSearchResult] = []
        for row in rows:
            chunk = document_chunk_from_payload(
                _payload({"payload": row["chunk_payload"]}),
                chunk_id=row["chunk_id"],
                object_id=row["chunk_object_id"],
            )
            obj = ResearchObject.model_validate(_payload({"payload": row["object_payload"]}))
            score = keyword_chunk_score(request.query, terms, chunk, obj)
            if score <= 0.0:
                continue
            if request.min_score is not None and score < request.min_score:
                continue
            results.append(
                ResearchChunkSearchResult(
                    rank=1,
                    chunk=chunk,
                    research_object=obj,
                    score=score,
                    match_type="keyword",
                )
            )

        results.sort(
            key=lambda result: (
                -(result.score or 0.0),
                str(result.chunk.research_object_id),
                result.chunk.chunk_index,
            )
        )
        return [
            result.model_copy(update={"rank": index})
            for index, result in enumerate(results[: request.limit], start=1)
        ]

    def upsert_text_embedding(self, embedding: TextEmbedding) -> TextEmbedding:
        existing = self._fetchone(
            """
            select embedding_id
            from text_embeddings
            where chunk_id = %s and embedding_model = %s
            """,
            (str(embedding.chunk_id), embedding.embedding_model),
        )
        embedding_id = UUID(existing["embedding_id"]) if existing else embedding.embedding_id
        record = embedding.model_copy(update={"embedding_id": embedding_id})
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into text_embeddings (
              embedding_id, chunk_id, object_id, chunk_index, source_key,
              object_type, embedding_model, embedding_dimensions, content_hash,
              vector_json, payload, embedded_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(chunk_id, embedding_model) do update set
              object_id = excluded.object_id,
              chunk_index = excluded.chunk_index,
              source_key = excluded.source_key,
              object_type = excluded.object_type,
              embedding_dimensions = excluded.embedding_dimensions,
              content_hash = excluded.content_hash,
              vector_json = excluded.vector_json,
              payload = excluded.payload,
              embedded_at = excluded.embedded_at,
              updated_at = now()
            """,
            (
                str(record.embedding_id),
                str(record.chunk_id),
                str(record.research_object_id),
                record.chunk_index,
                record.source_key,
                str(record.object_type) if record.object_type else None,
                record.embedding_model,
                record.embedding_dimensions,
                record.content_hash,
                self._json(record.embedding),
                self._json(payload),
                record.embedded_at,
            ),
        )
        row = self._fetchone("select payload from text_embeddings where embedding_id = %s", (str(record.embedding_id),))
        return TextEmbedding.model_validate(_payload(row))

    def get_text_embedding(self, embedding_id: UUID) -> TextEmbedding | None:
        row = self._fetchone("select payload from text_embeddings where embedding_id = %s", (str(embedding_id),))
        if row is None:
            return None
        return TextEmbedding.model_validate(_payload(row))

    def list_text_embeddings(
        self,
        *,
        embedding_model: str | None = None,
        source_key: str | None = None,
        research_object_id: UUID | None = None,
        chunk_id: UUID | None = None,
        object_type: str | None = None,
        limit: int | None = None,
    ) -> list[TextEmbedding]:
        clauses: list[str] = []
        params: list[object] = []
        if embedding_model:
            clauses.append("embedding_model = %s")
            params.append(embedding_model)
        if source_key:
            clauses.append("source_key = %s")
            params.append(source_key)
        if research_object_id:
            clauses.append("object_id = %s")
            params.append(str(research_object_id))
        if chunk_id:
            clauses.append("chunk_id = %s")
            params.append(str(chunk_id))
        if object_type:
            clauses.append("object_type = %s")
            params.append(object_type)
        sql = "select payload from text_embeddings"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by source_key, object_id, chunk_index, embedding_model"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [TextEmbedding.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def search_text_embeddings(self, request: TextEmbeddingSearchRequest) -> list[TextEmbeddingSearchResult]:
        candidates = self.list_text_embeddings(
            embedding_model=request.embedding_model,
            source_key=request.source_key,
            research_object_id=request.research_object_id,
            object_type=str(request.object_type) if request.object_type else None,
        )
        results: list[TextEmbeddingSearchResult] = []
        for embedding in candidates:
            score = cosine_similarity(request.query_embedding, embedding.embedding)
            if score is None:
                continue
            if request.min_score is not None and score < request.min_score:
                continue
            results.append(TextEmbeddingSearchResult(embedding=embedding, score=score))
        results.sort(key=lambda result: result.score, reverse=True)
        return results[: request.limit]

    def embedding_coverage(
        self,
        *,
        source_key: str | None = None,
        object_type: str | None = None,
        embedding_model: str | None = None,
    ) -> EmbeddingCoverageSummary:
        base_clauses: list[str] = []
        base_params: list[object] = []
        if source_key:
            base_clauses.append("ro.source_key = %s")
            base_params.append(source_key)
        if object_type:
            base_clauses.append("ro.object_type = %s")
            base_params.append(object_type)
        where = " where " + " and ".join(base_clauses) if base_clauses else ""
        total_chunks = self._scalar(
            f"""
            select count(distinct dc.chunk_id)
            from document_chunks dc
            join research_objects ro on ro.object_id = dc.object_id
            {where}
            """,
            base_params,
        )

        embedded_clauses = list(base_clauses)
        embedded_params = list(base_params)
        if embedding_model:
            embedded_clauses.append("te.embedding_model = %s")
            embedded_params.append(embedding_model)
        embedded_where = " where " + " and ".join(embedded_clauses) if embedded_clauses else ""
        embedded_chunks = self._scalar(
            f"""
            select count(distinct dc.chunk_id)
            from document_chunks dc
            join research_objects ro on ro.object_id = dc.object_id
            join text_embeddings te on te.chunk_id = dc.chunk_id
            {embedded_where}
            """,
            embedded_params,
        )

        model_rows = self._fetchall(
            f"""
            select te.embedding_model, count(distinct te.chunk_id) as count
            from document_chunks dc
            join research_objects ro on ro.object_id = dc.object_id
            join text_embeddings te on te.chunk_id = dc.chunk_id
            {where}
            group by te.embedding_model
            order by te.embedding_model
            """,
            base_params,
        )
        missing_chunks = max(total_chunks - embedded_chunks, 0)
        coverage_ratio = embedded_chunks / total_chunks if total_chunks else 0.0
        return EmbeddingCoverageSummary(
            source_key=source_key,
            object_type=object_type,
            embedding_model=embedding_model,
            total_chunks=total_chunks,
            embedded_chunks=embedded_chunks,
            missing_chunks=missing_chunks,
            coverage_ratio=coverage_ratio,
            embedding_models={row["embedding_model"]: int(row["count"]) for row in model_rows},
        )

    def count_orphan_text_embeddings(
        self,
        *,
        embedding_model: str | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
    ) -> int:
        clauses, params = self._orphan_text_embedding_clauses(
            embedding_model=embedding_model,
            source_key=source_key,
            object_type=object_type,
        )
        return self._scalar(
            f"""
            select count(*)
            from text_embeddings te
            left join document_chunks dc on dc.chunk_id = te.chunk_id
            where {' and '.join(clauses)}
            """,
            params,
        )

    def delete_orphan_text_embeddings(
        self,
        *,
        embedding_model: str | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
    ) -> int:
        clauses, params = self._orphan_text_embedding_clauses(
            embedding_model=embedding_model,
            source_key=source_key,
            object_type=object_type,
        )
        with self.conn.cursor() as cursor:
            cursor.execute(
                f"""
                delete from text_embeddings
                where embedding_id in (
                  select te.embedding_id
                  from text_embeddings te
                  left join document_chunks dc on dc.chunk_id = te.chunk_id
                  where {' and '.join(clauses)}
                )
                """,
                params,
            )
            deleted = cursor.rowcount
        self.conn.commit()
        return int(deleted) if deleted and deleted > 0 else 0

    def _orphan_text_embedding_clauses(
        self,
        *,
        embedding_model: str | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
    ) -> tuple[list[str], list[object]]:
        clauses = ["dc.chunk_id is null"]
        params: list[object] = []
        if embedding_model:
            clauses.append("te.embedding_model = %s")
            params.append(embedding_model)
        if source_key:
            clauses.append("te.source_key = %s")
            params.append(source_key)
        if object_type:
            clauses.append("te.object_type = %s")
            params.append(object_type)
        return clauses, params

    def coverage_summary(self) -> dict[str, Any]:
        source_count = self._scalar("select count(*) from ingestion_sources")
        query_count = self._scalar("select count(*) from source_queries")
        active_query_count = self._scalar("select count(*) from source_queries where active = true")
        raw_count = self._scalar("select count(*) from raw_source_records")
        object_count = self._scalar("select count(*) from research_objects")
        chunk_count = self._scalar("select count(*) from document_chunks")
        embedding_count = self._scalar("select count(*) from text_embeddings")
        claims_count = self._scalar("select count(*) from claims")
        entity_count = self._scalar("select count(*) from resolved_entities")
        entity_alias_count = self._scalar("select count(*) from entity_aliases")
        entity_mention_count = self._scalar("select count(*) from entity_mentions")
        scrape_review_count = self._scalar("select count(*) from scrape_review_records")
        source_followup_count = self._scalar("select count(*) from source_followup_queue")
        scrape_profile_review_count = self._scalar("select count(*) from scrape_source_profile_reviews")
        by_source = [
            dict(row)
            for row in self._fetchall(
                """
                select s.source_key,
                       s.display_name,
                       coalesce(r.raw_records, 0) as raw_records,
                       coalesce(o.research_objects, 0) as research_objects
                from ingestion_sources s
                left join (
                  select source_key, count(*) as raw_records
                  from raw_source_records
                  group by source_key
                ) r on r.source_key = s.source_key
                left join (
                  select source_key, count(*) as research_objects
                  from research_objects
                  group by source_key
                ) o on o.source_key = s.source_key
                order by s.source_key
                """
            )
        ]
        return {
            "storage_backend": "postgres",
            "db_path": "postgres://configured",
            "sources": source_count,
            "source_queries": query_count,
            "active_source_queries": active_query_count,
            "raw_records": raw_count,
            "research_objects": object_count,
            "document_chunks": chunk_count,
            "text_embeddings": embedding_count,
            "claims": claims_count,
            "resolved_entities": entity_count,
            "entity_aliases": entity_alias_count,
            "entity_mentions": entity_mention_count,
            "scrape_review_records": scrape_review_count,
            "source_followup_queue": source_followup_count,
            "scrape_source_profile_reviews": scrape_profile_review_count,
            "claim_curation": self.claim_curation_summary(),
            "by_source": by_source,
        }

    def search_claims(self, request: ClaimSearchRequest) -> list[ClaimSearchResult]:
        clauses = ["confidence >= %s"]
        params: list[object] = [request.min_confidence]
        if request.species:
            clauses.append("lower(coalesce(species, '')) = lower(%s)")
            params.append(request.species)
        if request.claim_types:
            placeholders = ",".join("%s" for _ in request.claim_types)
            clauses.append(f"claim_type in ({placeholders})")
            params.extend(str(claim_type) for claim_type in request.claim_types)
        if request.evidence_levels:
            placeholders = ",".join("%s" for _ in request.evidence_levels)
            clauses.append(f"evidence_level in ({placeholders})")
            params.extend(str(level) for level in request.evidence_levels)
        if request.query:
            clauses.append("statement ilike %s")
            params.append(f"%{request.query}%")
        rows = self._fetchall(
            f"""
            select payload
            from claims
            where {' and '.join(clauses)}
            order by confidence desc, updated_at desc
            """,
            params,
        )
        results = [ClaimSearchResult.model_validate(_payload(row)) for row in rows]
        results = [claim for claim in results if _claim_visible_in_search(claim, include_drafts=request.include_drafts)]
        if request.targets:
            targets = {target.lower() for target in request.targets}
            results = [
                claim
                for claim in results
                if any(entity.canonical_name.lower() in targets for entity in claim.entities)
            ]
        if request.compounds:
            compounds = {compound.lower() for compound in request.compounds}
            results = [
                claim
                for claim in results
                if any(entity.canonical_name.lower() in compounds for entity in claim.entities)
            ]
        return results[: request.limit]

    def list_claims(
        self,
        *,
        source_key: str | None = None,
        query: str | None = None,
        min_confidence: float = 0.0,
        extraction_status: str | None = None,
        curation_status: str | None = None,
        include_seed_claims: bool = True,
        limit: int | None = None,
    ) -> list[ClaimSearchResult]:
        clauses = ["c.confidence >= %s"]
        params: list[object] = [min_confidence]
        join = ""
        if source_key:
            join = " left join research_objects ro on ro.object_id = c.source_object_id"
            clauses.append("ro.source_key = %s")
            params.append(source_key)
        if query:
            clauses.append("c.statement ilike %s")
            params.append(f"%{query}%")
        rows = self._fetchall(
            f"""
            select c.payload
            from claims c
            {join}
            where {' and '.join(clauses)}
            order by c.updated_at asc, c.claim_id asc
            """,
            params,
        )
        claims = [ClaimSearchResult.model_validate(_payload(row)) for row in rows]
        filtered: list[ClaimSearchResult] = []
        for claim in claims:
            metadata = claim.metadata
            if extraction_status and metadata.get("extraction_status") != extraction_status:
                continue
            if curation_status and metadata.get("curation_status") != curation_status:
                continue
            if not include_seed_claims and metadata.get("seed"):
                continue
            filtered.append(claim)
        return filtered[:limit] if limit is not None else filtered

    def claim_curation_summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self._fetchall("select payload from claims"):
            claim = ClaimSearchResult.model_validate(_payload(row))
            status = claim.metadata.get("curation_status") or ("seed" if claim.metadata.get("seed") else "uncurated")
            counts[str(status)] = counts.get(str(status), 0) + 1
        return dict(sorted(counts.items()))

    def source_runtime_summary(self, source_key: str, *, sample_limit: int = 5) -> dict[str, Any]:
        raw_records = self._scalar("select count(*) from raw_source_records where source_key = %s", (source_key,))
        research_objects = self._scalar("select count(*) from research_objects where source_key = %s", (source_key,))
        document_chunks = self._scalar(
            """
            select count(*)
            from document_chunks dc
            join research_objects ro on ro.object_id = dc.object_id
            where ro.source_key = %s
            """,
            (source_key,),
        )
        entity_mentions = self._scalar(
            "select count(*) from entity_mentions where source_key = %s",
            (source_key,),
        )
        claim_status = {
            row["status"]: row["count"]
            for row in self._fetchall(
                """
                select coalesce(c.payload->'metadata'->>'curation_status', 'uncurated') as status,
                       count(*) as count
                from claims c
                join research_objects ro on ro.object_id = c.source_object_id
                where ro.source_key = %s
                group by status
                order by status
                """,
                (source_key,),
            )
        }
        claim_types = {
            row["claim_type"]: row["count"]
            for row in self._fetchall(
                """
                select c.claim_type, count(*) as count
                from claims c
                join research_objects ro on ro.object_id = c.source_object_id
                where ro.source_key = %s
                group by c.claim_type
                order by c.claim_type
                """,
                (source_key,),
            )
        }
        sample_claims = [
            {
                "claim_id": row["claim_id"],
                "statement": row["statement"],
                "confidence": float(row["confidence"]),
                "curation_status": row["curation_status"] or "uncurated",
            }
            for row in self._fetchall(
                """
                select c.claim_id,
                       c.statement,
                       c.confidence,
                       c.payload->'metadata'->>'curation_status' as curation_status
                from claims c
                join research_objects ro on ro.object_id = c.source_object_id
                where ro.source_key = %s
                order by c.confidence desc, c.updated_at desc
                limit %s
                """,
                (source_key, sample_limit),
            )
        ]
        claims = sum(claim_status.values())
        return {
            "source_key": source_key,
            "raw_records": raw_records,
            "research_objects": research_objects,
            "document_chunks": document_chunks,
            "entity_mentions": entity_mentions,
            "claims": claims,
            "claim_status": claim_status,
            "claim_types": claim_types,
            "sample_claims": sample_claims,
            "passes_minimum_bar": raw_records > 0 and research_objects > 0 and claims > 0,
        }

    def get_claim(self, claim_id: UUID) -> ClaimSearchResult | None:
        row = self._fetchone("select payload from claims where claim_id = %s", (str(claim_id),))
        if row is None:
            return None
        return ClaimSearchResult.model_validate(_payload(row))

    def get_candidate(self, request: CandidateDossierRequest) -> CandidateDossier | None:
        candidate_name = request.candidate_name or "unknown candidate"
        claim_request = ClaimSearchRequest(compounds=[candidate_name], limit=50)
        claims = self.search_claims(claim_request) if request.include_claims else []
        return CandidateDossier(
            candidate_id=request.candidate_id or uuid4(),
            name=candidate_name,
            status="investigating",
            summary="Hosted dossier backed by Postgres. Candidate tables arrive with the scoring layer.",
            evidence_claims=claims,
            validation_runs=[],
            artifacts=[],
            risk_flags=[],
            metadata={"storage_backend": "postgres"},
        )

    def commit_hypothesis(self, request: CommitHypothesisRequest):
        hypothesis_id = request.draft.hypothesis_id or uuid4()
        committed = request.draft.model_copy(
            update={
                "hypothesis_id": hypothesis_id,
                "status": "approved",
                "metadata": {
                    **request.draft.metadata,
                    "approved_by": request.approved_by,
                    "approval_note": request.approval_note,
                },
            }
        )
        payload = committed.model_dump(mode="json")
        self._execute(
            """
            insert into hypotheses (hypothesis_id, title, hypothesis, status, payload)
            values (%s, %s, %s, %s, %s)
            on conflict(hypothesis_id) do update set
              title = excluded.title,
              hypothesis = excluded.hypothesis,
              status = excluded.status,
              payload = excluded.payload,
              updated_at = now()
            """,
            (str(committed.hypothesis_id), committed.title, committed.hypothesis, committed.status, self._json(payload)),
        )
        return committed

    def enqueue_validation(self, request: ValidationRequest) -> AsyncRunHandle:
        status = "needs_approval" if request.require_approval else "queued"
        handle = AsyncRunHandle(
            run_name=f"{request.validation_type}:{request.candidate_name or request.candidate_id or 'unknown'}",
            status=status,
            metadata=request.model_dump(mode="json"),
        )
        self._upsert_run(handle)
        return handle

    def get_run_status(self, run_id: UUID) -> AsyncRunHandle | None:
        row = self._fetchone("select payload from async_runs where run_id = %s", (str(run_id),))
        if row is None:
            return None
        return AsyncRunHandle.model_validate(_payload(row))

    def get_artifact(self, artifact_id: UUID) -> ArtifactHandle | None:
        row = self._fetchone("select payload from artifacts where artifact_id = %s", (str(artifact_id),))
        if row is None:
            return None
        return ArtifactHandle.model_validate(_payload(row))

    def create_agent_run(self, record: AgentRunRecord) -> AgentRunRecord:
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into agent_runs (
              agent_run_id, agent_name, agent_version, model_profile, status,
              source_key, partition_date, dagster_run_id, started_at,
              completed_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(agent_run_id) do update set
              agent_name = excluded.agent_name,
              agent_version = excluded.agent_version,
              model_profile = excluded.model_profile,
              status = excluded.status,
              source_key = excluded.source_key,
              partition_date = excluded.partition_date,
              dagster_run_id = excluded.dagster_run_id,
              started_at = excluded.started_at,
              completed_at = excluded.completed_at,
              payload = excluded.payload,
              updated_at = now()
            """,
            (
                str(record.agent_run_id),
                record.agent_name,
                record.agent_version,
                record.model_profile,
                str(record.status),
                record.source_key,
                record.partition_date,
                record.dagster_run_id,
                record.started_at,
                record.completed_at,
                self._json(payload),
            ),
        )
        return record

    def finish_agent_run(
        self,
        agent_run_id: UUID,
        *,
        status: str,
        output_payload: dict,
        summary: dict,
        errors: list[str],
    ) -> AgentRunRecord | None:
        record = self.get_agent_run(agent_run_id)
        if record is None:
            return None
        updated = record.model_copy(
            update={
                "status": status,
                "completed_at": datetime.now(UTC),
                "output_payload": output_payload,
                "summary": summary,
                "errors": errors,
            }
        )
        return self.create_agent_run(updated)

    def get_agent_run(self, agent_run_id: UUID) -> AgentRunRecord | None:
        row = self._fetchone("select payload from agent_runs where agent_run_id = %s", (str(agent_run_id),))
        if row is None:
            return None
        return AgentRunRecord.model_validate(_payload(row))

    def list_agent_runs(
        self,
        *,
        agent_name: str | None = None,
        status: str | None = None,
        source_key: str | None = None,
        limit: int = 50,
    ) -> list[AgentRunRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if agent_name:
            clauses.append("agent_name = %s")
            params.append(agent_name)
        if status:
            clauses.append("status = %s")
            params.append(status)
        if source_key:
            clauses.append("source_key = %s")
            params.append(source_key)
        sql = "select payload from agent_runs"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc limit %s"
        params.append(limit)
        return [AgentRunRecord.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def create_agent_run_review(self, record: AgentRunReviewRecord) -> AgentRunReviewRecord:
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into agent_run_reviews (
              review_id, agent_run_id, reviewer, verdict, created_at, payload
            )
            values (%s, %s, %s, %s, %s, %s)
            on conflict(review_id) do update set
              agent_run_id = excluded.agent_run_id,
              reviewer = excluded.reviewer,
              verdict = excluded.verdict,
              created_at = excluded.created_at,
              payload = excluded.payload,
              updated_at = now()
            """,
            (
                str(record.review_id),
                str(record.agent_run_id),
                record.reviewer,
                record.verdict,
                record.created_at,
                self._json(payload),
            ),
        )
        return record

    def get_agent_run_review(self, review_id: UUID) -> AgentRunReviewRecord | None:
        row = self._fetchone("select payload from agent_run_reviews where review_id = %s", (str(review_id),))
        if row is None:
            return None
        return AgentRunReviewRecord.model_validate(_payload(row))

    def list_agent_run_reviews(
        self,
        *,
        agent_run_id: UUID | None = None,
        verdict: str | None = None,
        reviewer: str | None = None,
        limit: int = 50,
    ) -> list[AgentRunReviewRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if agent_run_id:
            clauses.append("agent_run_id = %s")
            params.append(str(agent_run_id))
        if verdict:
            clauses.append("verdict = %s")
            params.append(verdict)
        if reviewer:
            clauses.append("reviewer = %s")
            params.append(reviewer)
        sql = "select payload from agent_run_reviews"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc limit %s"
        params.append(limit)
        return [AgentRunReviewRecord.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def upsert_research_brief(self, record: ResearchBriefRecord) -> ResearchBriefRecord:
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into research_briefs (
              brief_id, agent_run_id, topic, disease_scope, source_key,
              brief_style, model_profile, review_mode, status, citation_count,
              finding_count, hypothesis_count, research_lead_count, error_count,
              created_at, updated_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(brief_id) do update set
              agent_run_id = excluded.agent_run_id,
              topic = excluded.topic,
              disease_scope = excluded.disease_scope,
              source_key = excluded.source_key,
              brief_style = excluded.brief_style,
              model_profile = excluded.model_profile,
              review_mode = excluded.review_mode,
              status = excluded.status,
              citation_count = excluded.citation_count,
              finding_count = excluded.finding_count,
              hypothesis_count = excluded.hypothesis_count,
              research_lead_count = excluded.research_lead_count,
              error_count = excluded.error_count,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                str(record.brief_id),
                str(record.agent_run_id) if record.agent_run_id else None,
                record.topic,
                record.disease_scope,
                record.source_key,
                record.brief_style,
                record.model_profile,
                record.review_mode,
                record.status,
                record.citation_count,
                record.finding_count,
                record.hypothesis_count,
                record.research_lead_count,
                record.error_count,
                record.created_at,
                record.updated_at,
                self._json(payload),
            ),
        )
        return record

    def get_research_brief(self, brief_id: UUID) -> ResearchBriefRecord | None:
        row = self._fetchone("select payload from research_briefs where brief_id = %s", (str(brief_id),))
        if row is None:
            return None
        return ResearchBriefRecord.model_validate(_payload(row))

    def list_research_briefs(
        self,
        *,
        status: str | None = None,
        source_key: str | None = None,
        topic_query: str | None = None,
        limit: int | None = 50,
    ) -> list[ResearchBriefRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if status:
            clauses.append("status = %s")
            params.append(status)
        if source_key:
            clauses.append("source_key = %s")
            params.append(source_key)
        if topic_query:
            clauses.append("(topic ilike %s or disease_scope ilike %s)")
            normalized = f"%{topic_query}%"
            params.extend([normalized, normalized])
        sql = "select payload from research_briefs"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [ResearchBriefRecord.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def upsert_research_brief_evaluation(
        self,
        record: ResearchBriefEvaluationRecord,
    ) -> ResearchBriefEvaluationRecord:
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into research_brief_evaluations (
              evaluation_id, brief_id, agent_run_id, topic, source_key,
              model_profile, overall_score, passes_quality_bar, readiness,
              created_at, updated_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(evaluation_id) do update set
              brief_id = excluded.brief_id,
              agent_run_id = excluded.agent_run_id,
              topic = excluded.topic,
              source_key = excluded.source_key,
              model_profile = excluded.model_profile,
              overall_score = excluded.overall_score,
              passes_quality_bar = excluded.passes_quality_bar,
              readiness = excluded.readiness,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                str(record.evaluation_id),
                str(record.brief_id),
                str(record.agent_run_id) if record.agent_run_id else None,
                record.topic,
                record.source_key,
                record.model_profile,
                record.overall_score,
                record.passes_quality_bar,
                record.readiness,
                record.created_at,
                record.updated_at,
                self._json(payload),
            ),
        )
        return record

    def get_research_brief_evaluation(self, evaluation_id: UUID) -> ResearchBriefEvaluationRecord | None:
        row = self._fetchone(
            "select payload from research_brief_evaluations where evaluation_id = %s",
            (str(evaluation_id),),
        )
        if row is None:
            return None
        return ResearchBriefEvaluationRecord.model_validate(_payload(row))

    def list_research_brief_evaluations(
        self,
        *,
        brief_id: UUID | None = None,
        readiness: str | None = None,
        passes_quality_bar: bool | None = None,
        limit: int | None = 50,
    ) -> list[ResearchBriefEvaluationRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if brief_id:
            clauses.append("brief_id = %s")
            params.append(str(brief_id))
        if readiness:
            clauses.append("readiness = %s")
            params.append(readiness)
        if passes_quality_bar is not None:
            clauses.append("passes_quality_bar = %s")
            params.append(passes_quality_bar)
        sql = "select payload from research_brief_evaluations"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [
            ResearchBriefEvaluationRecord.model_validate(_payload(row))
            for row in self._fetchall(sql, params)
        ]

    def upsert_therapy_idea(self, record: TherapyIdeaRecord) -> TherapyIdeaRecord:
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into therapy_ideas (
              therapy_idea_id, committee_run_id, agent_run_id, source_program_id,
              source_brief_id, source_evaluation_id, topic, source_key, status,
              promotion_state, score, created_at, updated_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(therapy_idea_id) do update set
              committee_run_id = excluded.committee_run_id,
              agent_run_id = excluded.agent_run_id,
              source_program_id = excluded.source_program_id,
              source_brief_id = excluded.source_brief_id,
              source_evaluation_id = excluded.source_evaluation_id,
              topic = excluded.topic,
              source_key = excluded.source_key,
              status = excluded.status,
              promotion_state = excluded.promotion_state,
              score = excluded.score,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                str(record.therapy_idea_id),
                str(record.committee_run_id) if record.committee_run_id else None,
                str(record.agent_run_id) if record.agent_run_id else None,
                str(record.source_program_id) if record.source_program_id else None,
                str(record.source_brief_id) if record.source_brief_id else None,
                str(record.source_evaluation_id) if record.source_evaluation_id else None,
                record.topic,
                record.source_key,
                record.status,
                record.promotion_state,
                record.score,
                record.created_at,
                record.updated_at,
                self._json(payload),
            ),
        )
        return record

    def get_therapy_idea(self, therapy_idea_id: UUID) -> TherapyIdeaRecord | None:
        row = self._fetchone(
            "select payload from therapy_ideas where therapy_idea_id = %s",
            (str(therapy_idea_id),),
        )
        if row is None:
            return None
        return TherapyIdeaRecord.model_validate(_payload(row))

    def list_therapy_ideas(
        self,
        *,
        status: str | None = None,
        statuses: list[str] | None = None,
        source_program_id: UUID | None = None,
        source_brief_id: UUID | None = None,
        source_evaluation_id: UUID | None = None,
        committee_run_id: UUID | None = None,
        topic_query: str | None = None,
        limit: int | None = 50,
    ) -> list[TherapyIdeaRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if status:
            clauses.append("status = %s")
            params.append(status)
        if statuses:
            clauses.append("status = any(%s)")
            params.append(statuses)
        if source_program_id:
            clauses.append("source_program_id = %s")
            params.append(str(source_program_id))
        if source_brief_id:
            clauses.append("source_brief_id = %s")
            params.append(str(source_brief_id))
        if source_evaluation_id:
            clauses.append("source_evaluation_id = %s")
            params.append(str(source_evaluation_id))
        if committee_run_id:
            clauses.append("committee_run_id = %s")
            params.append(str(committee_run_id))
        if topic_query:
            clauses.append("topic ilike %s")
            params.append(f"%{topic_query}%")
        sql = "select payload from therapy_ideas"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by score desc, updated_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [TherapyIdeaRecord.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def upsert_research_program(self, record: ResearchProgramRecord) -> ResearchProgramRecord:
        existing = self.get_research_program(record.program_id)
        if existing:
            record = record.model_copy(
                update={
                    "created_at": existing.created_at,
                    "updated_at": datetime.now(UTC),
                    "metadata": {**existing.metadata, **record.metadata},
                }
            )
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into research_programs (
              program_id, agent_run_id, title, thesis_area, status,
              gate_decision, confidence_score, evidence_loop_count,
              max_evidence_loops, source_query, created_at, updated_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(program_id) do update set
              agent_run_id = excluded.agent_run_id,
              title = excluded.title,
              thesis_area = excluded.thesis_area,
              status = excluded.status,
              gate_decision = excluded.gate_decision,
              confidence_score = excluded.confidence_score,
              evidence_loop_count = excluded.evidence_loop_count,
              max_evidence_loops = excluded.max_evidence_loops,
              source_query = excluded.source_query,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                str(record.program_id),
                str(record.agent_run_id) if record.agent_run_id else None,
                record.title,
                record.thesis_area,
                record.status,
                record.gate_decision,
                record.confidence_score,
                record.evidence_loop_count,
                record.max_evidence_loops,
                record.source_query,
                record.created_at,
                record.updated_at,
                self._json(payload),
            ),
        )
        return record

    def get_research_program(self, program_id: UUID) -> ResearchProgramRecord | None:
        row = self._fetchone(
            "select payload from research_programs where program_id = %s",
            (str(program_id),),
        )
        if row is None:
            return None
        return ResearchProgramRecord.model_validate(_payload(row))

    def list_research_programs(
        self,
        *,
        status: str | None = None,
        gate_decision: str | None = None,
        thesis_query: str | None = None,
        limit: int | None = 50,
    ) -> list[ResearchProgramRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if status:
            clauses.append("status = %s")
            params.append(status)
        if gate_decision:
            clauses.append("gate_decision = %s")
            params.append(gate_decision)
        if thesis_query:
            clauses.append("(title ilike %s or source_query ilike %s or payload::text ilike %s)")
            like_query = f"%{thesis_query}%"
            params.extend([like_query, like_query, like_query])
        sql = "select payload from research_programs"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by confidence_score desc, updated_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [ResearchProgramRecord.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def upsert_validation_decision(self, record: ValidationDecisionRecord) -> ValidationDecisionRecord:
        existing = self.get_validation_decision(record.decision_id)
        if existing:
            record = record.model_copy(
                update={
                    "decision_record_id": existing.decision_record_id,
                    "created_at": existing.created_at,
                    "updated_at": datetime.now(UTC),
                    "metadata": {**existing.metadata, **record.metadata},
                }
            )
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into validation_decisions (
              decision_record_id, decision_id, packet_id, candidate_id, source_type,
              source_id, therapy_idea_id, title, outcome, confidence,
              validation_ready, created_at, updated_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(decision_id) do update set
              packet_id = excluded.packet_id,
              candidate_id = excluded.candidate_id,
              source_type = excluded.source_type,
              source_id = excluded.source_id,
              therapy_idea_id = excluded.therapy_idea_id,
              title = excluded.title,
              outcome = excluded.outcome,
              confidence = excluded.confidence,
              validation_ready = excluded.validation_ready,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                str(record.decision_record_id),
                record.decision_id,
                record.packet_id,
                record.candidate_id,
                record.source_type,
                record.source_id,
                str(record.therapy_idea_id) if record.therapy_idea_id else None,
                record.title,
                record.outcome,
                record.confidence,
                record.validation_ready,
                record.created_at,
                record.updated_at,
                self._json(payload),
            ),
        )
        return record

    def get_validation_decision(self, decision_id: str) -> ValidationDecisionRecord | None:
        row = self._fetchone(
            "select payload from validation_decisions where decision_id = %s",
            (decision_id,),
        )
        if row is None:
            return None
        return ValidationDecisionRecord.model_validate(_payload(row))

    def list_validation_decisions(
        self,
        *,
        outcome: str | None = None,
        therapy_idea_id: UUID | None = None,
        candidate_id: str | None = None,
        limit: int | None = 50,
    ) -> list[ValidationDecisionRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if outcome:
            clauses.append("outcome = %s")
            params.append(outcome)
        if therapy_idea_id:
            clauses.append("therapy_idea_id = %s")
            params.append(str(therapy_idea_id))
        if candidate_id:
            clauses.append("candidate_id = %s")
            params.append(candidate_id)
        sql = "select payload from validation_decisions"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by updated_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [ValidationDecisionRecord.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def upsert_validation_plan(self, record: ValidationPlanRecord) -> ValidationPlanRecord:
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into validation_plans (
              plan_id, agent_run_id, brief_id, evaluation_id, topic, source_key,
              model_profile, status, readiness, task_count, hypothesis_count,
              created_at, updated_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(plan_id) do update set
              agent_run_id = excluded.agent_run_id,
              brief_id = excluded.brief_id,
              evaluation_id = excluded.evaluation_id,
              topic = excluded.topic,
              source_key = excluded.source_key,
              model_profile = excluded.model_profile,
              status = excluded.status,
              readiness = excluded.readiness,
              task_count = excluded.task_count,
              hypothesis_count = excluded.hypothesis_count,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                str(record.plan_id),
                str(record.agent_run_id) if record.agent_run_id else None,
                str(record.brief_id),
                str(record.evaluation_id) if record.evaluation_id else None,
                record.topic,
                record.source_key,
                record.model_profile,
                record.status,
                record.readiness,
                record.task_count,
                record.hypothesis_count,
                record.created_at,
                record.updated_at,
                self._json(payload),
            ),
        )
        return record

    def get_validation_plan(self, plan_id: UUID) -> ValidationPlanRecord | None:
        row = self._fetchone("select payload from validation_plans where plan_id = %s", (str(plan_id),))
        if row is None:
            return None
        return ValidationPlanRecord.model_validate(_payload(row))

    def list_validation_plans(
        self,
        *,
        brief_id: UUID | None = None,
        evaluation_id: UUID | None = None,
        status: str | None = None,
        readiness: str | None = None,
        limit: int | None = 50,
    ) -> list[ValidationPlanRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if brief_id:
            clauses.append("brief_id = %s")
            params.append(str(brief_id))
        if evaluation_id:
            clauses.append("evaluation_id = %s")
            params.append(str(evaluation_id))
        if status:
            clauses.append("status = %s")
            params.append(status)
        if readiness:
            clauses.append("readiness = %s")
            params.append(readiness)
        sql = "select payload from validation_plans"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [
            ValidationPlanRecord.model_validate(_payload(row))
            for row in self._fetchall(sql, params)
        ]

    def upsert_validation_request_queue_item(
        self,
        item: ValidationRequestQueueItem,
    ) -> ValidationRequestQueueItem:
        existing = self._fetchone(
            "select payload from validation_request_queue where identity_key = %s",
            (item.identity_key,),
        )
        if existing is not None:
            existing_item = ValidationRequestQueueItem.model_validate(_payload(existing))
            item = item.model_copy(
                update={
                    "queue_item_id": existing_item.queue_item_id,
                    "status": existing_item.status,
                    "attempts": existing_item.attempts,
                    "last_run_id": existing_item.last_run_id,
                    "last_error": existing_item.last_error,
                    "approved_by": existing_item.approved_by,
                    "approval_note": existing_item.approval_note,
                    "created_at": existing_item.created_at,
                    "updated_at": datetime.now(UTC),
                    "metadata": {**existing_item.metadata, **item.metadata},
                }
            )
        payload = item.model_dump(mode="json")
        validation_payload = item.validation_request.model_dump(mode="json")
        self._execute(
            """
            insert into validation_request_queue (
              queue_item_id, identity_key, status, priority, plan_id, task_id,
              brief_id, evaluation_id, source_key, task_type, title,
              validation_type, target_name, candidate_name, last_run_id,
              attempts, last_error, approved_by, created_at, updated_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(identity_key) do update set
              status = excluded.status,
              priority = excluded.priority,
              source_key = excluded.source_key,
              task_type = excluded.task_type,
              title = excluded.title,
              validation_type = excluded.validation_type,
              target_name = excluded.target_name,
              candidate_name = excluded.candidate_name,
              last_run_id = excluded.last_run_id,
              attempts = excluded.attempts,
              last_error = excluded.last_error,
              approved_by = excluded.approved_by,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                str(item.queue_item_id),
                item.identity_key,
                item.status,
                item.priority,
                str(item.plan_id),
                str(item.task_id),
                str(item.brief_id),
                str(item.evaluation_id) if item.evaluation_id else None,
                item.source_key,
                item.task_type,
                item.title,
                validation_payload.get("validation_type"),
                validation_payload.get("target_name"),
                validation_payload.get("candidate_name"),
                str(item.last_run_id) if item.last_run_id else None,
                item.attempts,
                item.last_error,
                item.approved_by,
                item.created_at,
                item.updated_at,
                self._json(payload),
            ),
        )
        row = self._fetchone(
            "select payload from validation_request_queue where identity_key = %s",
            (item.identity_key,),
        )
        return ValidationRequestQueueItem.model_validate(_payload(row))

    def get_validation_request_queue_item(
        self,
        queue_item_id: UUID,
    ) -> ValidationRequestQueueItem | None:
        row = self._fetchone(
            "select payload from validation_request_queue where queue_item_id = %s",
            (str(queue_item_id),),
        )
        if row is None:
            return None
        return ValidationRequestQueueItem.model_validate(_payload(row))

    def list_validation_request_queue_items(
        self,
        *,
        plan_id: UUID | None = None,
        status: str | None = None,
        statuses: list[str] | None = None,
        source_key: str | None = None,
        task_type: str | None = None,
        topic_query: str | None = None,
        limit: int | None = 50,
    ) -> list[ValidationRequestQueueItem]:
        clauses: list[str] = []
        params: list[object] = []
        if plan_id:
            clauses.append("plan_id = %s")
            params.append(str(plan_id))
        if status:
            clauses.append("status = %s")
            params.append(status)
        if statuses:
            placeholders = ",".join("%s" for _ in statuses)
            clauses.append(f"status in ({placeholders})")
            params.extend(statuses)
        if source_key:
            clauses.append("source_key = %s")
            params.append(source_key)
        if task_type:
            clauses.append("task_type = %s")
            params.append(task_type)
        if topic_query:
            clauses.append("(title ilike %s or payload::text ilike %s)")
            normalized = f"%{topic_query}%"
            params.extend([normalized, normalized])
        sql = "select payload from validation_request_queue"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by priority asc, created_at asc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [
            ValidationRequestQueueItem.model_validate(_payload(row))
            for row in self._fetchall(sql, params)
        ]

    def update_validation_request_queue_item(
        self,
        queue_item_id: UUID,
        *,
        status: str | None = None,
        priority: int | None = None,
        attempts: int | None = None,
        last_run_id: UUID | None = None,
        last_error: str | None = None,
        approved_by: str | None = None,
        approval_note: str | None = None,
        quality_gates: list[str] | None = None,
        dispatch_blockers: list[str] | None = None,
        metadata: dict | None = None,
    ) -> ValidationRequestQueueItem | None:
        item = self.get_validation_request_queue_item(queue_item_id)
        if item is None:
            return None
        updated = item.model_copy(
            update={
                "status": item.status if status is None else status,
                "priority": item.priority if priority is None else priority,
                "attempts": item.attempts if attempts is None else attempts,
                "last_run_id": item.last_run_id if last_run_id is None else last_run_id,
                "last_error": last_error,
                "approved_by": item.approved_by if approved_by is None else approved_by,
                "approval_note": item.approval_note if approval_note is None else approval_note,
                "quality_gates": item.quality_gates if quality_gates is None else quality_gates,
                "dispatch_blockers": item.dispatch_blockers if dispatch_blockers is None else dispatch_blockers,
                "updated_at": datetime.now(UTC),
                "metadata": {**item.metadata, **(metadata or {})},
            }
        )
        validation_payload = updated.validation_request.model_dump(mode="json")
        payload = updated.model_dump(mode="json")
        self._execute(
            """
            update validation_request_queue
            set status = %s,
                priority = %s,
                last_run_id = %s,
                attempts = %s,
                last_error = %s,
                approved_by = %s,
                validation_type = %s,
                target_name = %s,
                candidate_name = %s,
                updated_at = %s,
                payload = %s
            where queue_item_id = %s
            """,
            (
                updated.status,
                updated.priority,
                str(updated.last_run_id) if updated.last_run_id else None,
                updated.attempts,
                updated.last_error,
                updated.approved_by,
                validation_payload.get("validation_type"),
                validation_payload.get("target_name"),
                validation_payload.get("candidate_name"),
                updated.updated_at,
                self._json(payload),
                str(queue_item_id),
            ),
        )
        return updated

    def upsert_compute_job(self, record: ComputeJobRecord) -> ComputeJobRecord:
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into compute_jobs (
              compute_job_id, queue_item_id, status, runner_kind, compute_profile,
              validation_type, title, runpod_job_id, dagster_run_id, created_at,
              updated_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(compute_job_id) do update set
              queue_item_id = excluded.queue_item_id,
              status = excluded.status,
              runner_kind = excluded.runner_kind,
              compute_profile = excluded.compute_profile,
              validation_type = excluded.validation_type,
              title = excluded.title,
              runpod_job_id = excluded.runpod_job_id,
              dagster_run_id = excluded.dagster_run_id,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                str(record.compute_job_id),
                str(record.queue_item_id) if record.queue_item_id else None,
                record.status,
                record.runner_kind,
                record.compute_profile,
                record.validation_type,
                record.title,
                record.runpod_job_id,
                record.dagster_run_id,
                record.created_at,
                record.updated_at,
                self._json(payload),
            ),
        )
        return record

    def get_compute_job(self, compute_job_id: UUID) -> ComputeJobRecord | None:
        row = self._fetchone(
            "select payload from compute_jobs where compute_job_id = %s",
            (str(compute_job_id),),
        )
        if row is None:
            return None
        return ComputeJobRecord.model_validate(_payload(row))

    def list_compute_jobs(
        self,
        *,
        status: str | None = None,
        runner_kind: str | None = None,
        queue_item_id: UUID | None = None,
        limit: int | None = 50,
    ) -> list[ComputeJobRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if status:
            clauses.append("status = %s")
            params.append(status)
        if runner_kind:
            clauses.append("runner_kind = %s")
            params.append(runner_kind)
        if queue_item_id:
            clauses.append("queue_item_id = %s")
            params.append(str(queue_item_id))
        sql = "select payload from compute_jobs"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by updated_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [ComputeJobRecord.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def update_compute_job(
        self,
        compute_job_id: UUID,
        *,
        status: str | None = None,
        output_payload: dict | None = None,
        external_run_id: str | None = None,
        dagster_run_id: str | None = None,
        runpod_job_id: str | None = None,
        cost_actual_usd: float | None = None,
        last_error: str | None = None,
        metadata: dict | None = None,
    ) -> ComputeJobRecord | None:
        record = self.get_compute_job(compute_job_id)
        if record is None:
            return None
        now = datetime.now(UTC)
        update: dict[str, object] = {
            "status": record.status if status is None else status,
            "output_payload": record.output_payload if output_payload is None else output_payload,
            "external_run_id": record.external_run_id if external_run_id is None else external_run_id,
            "dagster_run_id": record.dagster_run_id if dagster_run_id is None else dagster_run_id,
            "runpod_job_id": record.runpod_job_id if runpod_job_id is None else runpod_job_id,
            "cost_actual_usd": record.cost_actual_usd if cost_actual_usd is None else cost_actual_usd,
            "last_error": last_error,
            "updated_at": now,
            "metadata": {**record.metadata, **(metadata or {})},
        }
        if status == "submitted":
            update["submitted_at"] = now
        if status == "running":
            update["started_at"] = now
        if status in {"completed", "failed", "cancelled", "blocked"}:
            update["completed_at"] = now
        updated = record.model_copy(update=update)
        return self.upsert_compute_job(updated)

    def upsert_command_center_board_stage(
        self,
        record: CommandCenterBoardStageRecord,
    ) -> CommandCenterBoardStageRecord:
        existing = self.get_command_center_board_stage(record.entity_type, record.entity_id)
        if existing is not None:
            record = record.model_copy(update={"created_at": existing.created_at})
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into command_center_board_stages (
              entity_type, entity_id, board_stage, actor, updated_at, payload
            )
            values (%s, %s, %s, %s, %s, %s)
            on conflict(entity_type, entity_id) do update set
              board_stage = excluded.board_stage,
              actor = excluded.actor,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                record.entity_type,
                record.entity_id,
                record.board_stage,
                record.actor,
                record.updated_at,
                self._json(payload),
            ),
        )
        return record

    def get_command_center_board_stage(
        self,
        entity_type: str,
        entity_id: str,
    ) -> CommandCenterBoardStageRecord | None:
        row = self._fetchone(
            """
            select payload from command_center_board_stages
            where entity_type = %s and entity_id = %s
            """,
            (entity_type, entity_id),
        )
        if row is None:
            return None
        return CommandCenterBoardStageRecord.model_validate(_payload(row))

    def list_command_center_board_stages(
        self,
        *,
        entity_type: str | None = None,
        board_stage: str | None = None,
        limit: int | None = 500,
    ) -> list[CommandCenterBoardStageRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if entity_type:
            clauses.append("entity_type = %s")
            params.append(entity_type)
        if board_stage:
            clauses.append("board_stage = %s")
            params.append(board_stage)
        sql = "select payload from command_center_board_stages"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by updated_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [CommandCenterBoardStageRecord.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def append_command_center_activity_event(
        self,
        record: CommandCenterActivityEventRecord,
    ) -> CommandCenterActivityEventRecord:
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into command_center_activity_events (
              event_id, occurred_at, actor, source, event_type, entity_type,
              entity_id, severity, correlation_id, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(event_id) do update set
              payload = excluded.payload
            """,
            (
                str(record.event_id),
                record.occurred_at,
                record.actor,
                record.source,
                record.event_type,
                record.entity_type,
                record.entity_id,
                record.severity,
                record.correlation_id,
                self._json(payload),
            ),
        )
        return record

    def list_command_center_activity_events(
        self,
        *,
        entity_type: str | None = None,
        entity_id: str | None = None,
        source: str | None = None,
        limit: int | None = 100,
    ) -> list[CommandCenterActivityEventRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if entity_type:
            clauses.append("entity_type = %s")
            params.append(entity_type)
        if entity_id:
            clauses.append("entity_id = %s")
            params.append(entity_id)
        if source:
            clauses.append("source = %s")
            params.append(source)
        sql = "select payload from command_center_activity_events"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by occurred_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [CommandCenterActivityEventRecord.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def upsert_md_expert_review_packet(
        self,
        record: MDExpertReviewPacketRecord,
    ) -> MDExpertReviewPacketRecord:
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into md_expert_review_packets (
              packet_id, packet_hash, status, compute_job_id, queue_item_id,
              endpoint_id, created_at, updated_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(packet_id) do update set
              packet_hash = excluded.packet_hash,
              status = excluded.status,
              compute_job_id = excluded.compute_job_id,
              queue_item_id = excluded.queue_item_id,
              endpoint_id = excluded.endpoint_id,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                str(record.packet_id),
                record.packet_hash,
                record.status,
                str(record.compute_job_id) if record.compute_job_id else None,
                str(record.queue_item_id) if record.queue_item_id else None,
                record.endpoint_id,
                record.created_at,
                record.updated_at,
                self._json(payload),
            ),
        )
        return record

    def get_md_expert_review_packet(self, packet_id: UUID) -> MDExpertReviewPacketRecord | None:
        row = self._fetchone(
            "select payload from md_expert_review_packets where packet_id = %s",
            (str(packet_id),),
        )
        if row is None:
            return None
        return MDExpertReviewPacketRecord.model_validate(_payload(row))

    def get_md_expert_review_packet_by_hash(self, packet_hash: str) -> MDExpertReviewPacketRecord | None:
        row = self._fetchone(
            "select payload from md_expert_review_packets where packet_hash = %s order by updated_at desc limit 1",
            (packet_hash,),
        )
        if row is None:
            return None
        return MDExpertReviewPacketRecord.model_validate(_payload(row))

    def list_md_expert_review_packets(
        self,
        *,
        packet_hash: str | None = None,
        status: str | None = None,
        limit: int | None = 50,
    ) -> list[MDExpertReviewPacketRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if packet_hash:
            clauses.append("packet_hash = %s")
            params.append(packet_hash)
        if status:
            clauses.append("status = %s")
            params.append(status)
        sql = "select payload from md_expert_review_packets"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by updated_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [MDExpertReviewPacketRecord.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def upsert_md_expert_approval(self, record: MDExpertApprovalRecord) -> MDExpertApprovalRecord:
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into md_expert_approvals (
              approval_id, packet_id, packet_hash, decision, reviewer_name,
              reviewer_contact, reviewed_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(approval_id) do update set
              packet_id = excluded.packet_id,
              packet_hash = excluded.packet_hash,
              decision = excluded.decision,
              reviewer_name = excluded.reviewer_name,
              reviewer_contact = excluded.reviewer_contact,
              reviewed_at = excluded.reviewed_at,
              payload = excluded.payload,
              updated_at = now()
            """,
            (
                str(record.approval_id),
                str(record.packet_id),
                record.packet_hash,
                record.decision,
                record.reviewer_name,
                record.reviewer_contact,
                record.reviewed_at,
                self._json(payload),
            ),
        )
        packet = self.get_md_expert_review_packet(record.packet_id)
        if packet is not None:
            self.upsert_md_expert_review_packet(
                packet.model_copy(update={"status": record.decision, "updated_at": datetime.now(UTC)})
            )
        return record

    def list_md_expert_approvals(
        self,
        *,
        packet_hash: str | None = None,
        decision: str | None = None,
        limit: int | None = 50,
    ) -> list[MDExpertApprovalRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if packet_hash:
            clauses.append("packet_hash = %s")
            params.append(packet_hash)
        if decision:
            clauses.append("decision = %s")
            params.append(decision)
        sql = "select payload from md_expert_approvals"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by reviewed_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [MDExpertApprovalRecord.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def upsert_research_brief_queue_item(self, item: ResearchBriefQueueItem) -> ResearchBriefQueueItem:
        existing = self._fetchone(
            "select payload from research_brief_queue where identity_key = %s",
            (item.identity_key,),
        )
        if existing is not None:
            existing_item = ResearchBriefQueueItem.model_validate(_payload(existing))
            item = item.model_copy(
                update={
                    "queue_item_id": existing_item.queue_item_id,
                    "status": existing_item.status,
                    "attempts": existing_item.attempts,
                    "last_brief_id": existing_item.last_brief_id,
                    "last_agent_run_id": existing_item.last_agent_run_id,
                    "last_error": existing_item.last_error,
                    "created_at": existing_item.created_at,
                    "updated_at": datetime.now(UTC),
                    "metadata": {**existing_item.metadata, **item.metadata},
                }
            )
        payload = item.model_dump(mode="json")
        self._execute(
            """
            insert into research_brief_queue (
              queue_item_id, identity_key, status, priority, topic, disease_scope,
              source_key, brief_style, model_profile, review_mode, last_brief_id,
              last_agent_run_id, attempts, created_at, updated_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(identity_key) do update set
              status = excluded.status,
              priority = excluded.priority,
              topic = excluded.topic,
              disease_scope = excluded.disease_scope,
              source_key = excluded.source_key,
              brief_style = excluded.brief_style,
              model_profile = excluded.model_profile,
              review_mode = excluded.review_mode,
              last_brief_id = excluded.last_brief_id,
              last_agent_run_id = excluded.last_agent_run_id,
              attempts = excluded.attempts,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                str(item.queue_item_id),
                item.identity_key,
                item.status,
                item.priority,
                item.topic,
                item.disease_scope,
                item.source_key,
                item.brief_style,
                item.model_profile,
                item.review_mode,
                str(item.last_brief_id) if item.last_brief_id else None,
                str(item.last_agent_run_id) if item.last_agent_run_id else None,
                item.attempts,
                item.created_at,
                item.updated_at,
                self._json(payload),
            ),
        )
        row = self._fetchone(
            "select payload from research_brief_queue where identity_key = %s",
            (item.identity_key,),
        )
        return ResearchBriefQueueItem.model_validate(_payload(row))

    def get_research_brief_queue_item(self, queue_item_id: UUID) -> ResearchBriefQueueItem | None:
        row = self._fetchone(
            "select payload from research_brief_queue where queue_item_id = %s",
            (str(queue_item_id),),
        )
        if row is None:
            return None
        return ResearchBriefQueueItem.model_validate(_payload(row))

    def list_research_brief_queue_items(
        self,
        *,
        status: str | None = None,
        statuses: list[str] | None = None,
        source_key: str | None = None,
        topic_query: str | None = None,
        limit: int | None = 50,
    ) -> list[ResearchBriefQueueItem]:
        clauses: list[str] = []
        params: list[object] = []
        if status:
            clauses.append("status = %s")
            params.append(status)
        if statuses:
            placeholders = ",".join("%s" for _ in statuses)
            clauses.append(f"status in ({placeholders})")
            params.extend(statuses)
        if source_key:
            clauses.append("source_key = %s")
            params.append(source_key)
        if topic_query:
            clauses.append("(topic ilike %s or disease_scope ilike %s)")
            normalized = f"%{topic_query}%"
            params.extend([normalized, normalized])
        sql = "select payload from research_brief_queue"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by priority asc, created_at asc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [ResearchBriefQueueItem.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def update_research_brief_queue_item(
        self,
        queue_item_id: UUID,
        *,
        status: str | None = None,
        priority: int | None = None,
        attempts: int | None = None,
        last_brief_id: UUID | None = None,
        last_agent_run_id: UUID | None = None,
        last_error: str | None = None,
        metadata: dict | None = None,
    ) -> ResearchBriefQueueItem | None:
        item = self.get_research_brief_queue_item(queue_item_id)
        if item is None:
            return None
        updated = item.model_copy(
            update={
                "status": item.status if status is None else status,
                "priority": item.priority if priority is None else priority,
                "attempts": item.attempts if attempts is None else attempts,
                "last_brief_id": item.last_brief_id if last_brief_id is None else last_brief_id,
                "last_agent_run_id": item.last_agent_run_id if last_agent_run_id is None else last_agent_run_id,
                "last_error": last_error,
                "updated_at": datetime.now(UTC),
                "metadata": {**item.metadata, **(metadata or {})},
            }
        )
        payload = updated.model_dump(mode="json")
        self._execute(
            """
            update research_brief_queue
            set status = %s,
                priority = %s,
                last_brief_id = %s,
                last_agent_run_id = %s,
                attempts = %s,
                updated_at = %s,
                payload = %s
            where queue_item_id = %s
            """,
            (
                updated.status,
                updated.priority,
                str(updated.last_brief_id) if updated.last_brief_id else None,
                str(updated.last_agent_run_id) if updated.last_agent_run_id else None,
                updated.attempts,
                updated.updated_at,
                self._json(payload),
                str(queue_item_id),
            ),
        )
        return updated

    def upsert_artifact(self, artifact: ArtifactHandle) -> ArtifactHandle:
        payload = artifact.model_dump(mode="json")
        self._execute(
            """
            insert into artifacts (artifact_id, artifact_type, uri, legal_status, mime_type, payload)
            values (%s, %s, %s, %s, %s, %s)
            on conflict(artifact_id) do update set
              artifact_type = excluded.artifact_type,
              uri = excluded.uri,
              legal_status = excluded.legal_status,
              mime_type = excluded.mime_type,
              payload = excluded.payload,
              updated_at = now()
            """,
            (str(artifact.artifact_id), artifact.artifact_type, artifact.uri, artifact.legal_status, artifact.mime_type, self._json(payload)),
        )
        return artifact

    def list_artifacts(
        self,
        *,
        artifact_type: str | None = None,
        source_key: str | None = None,
        limit: int | None = None,
    ) -> list[ArtifactHandle]:
        clauses: list[str] = []
        params: list[object] = []
        if artifact_type:
            clauses.append("artifact_type = %s")
            params.append(artifact_type)
        if source_key:
            clauses.append("payload->'metadata'->>'source_key' = %s")
            params.append(source_key)
        sql = "select payload from artifacts"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by updated_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [ArtifactHandle.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def upsert_scrape_review(self, record: ScrapeReviewRecord) -> ScrapeReviewRecord:
        existing = self._fetchone(
            """
            select payload from scrape_review_records
            where source_key = %s and artifact_id = %s and source_record_id = %s
            """,
            (record.source_key, str(record.artifact_id), record.source_record_id),
        )
        if existing is not None:
            existing_record = ScrapeReviewRecord.model_validate(_payload(existing))
            record = record.model_copy(
                update={
                    "review_id": existing_record.review_id,
                    "review_status": existing_record.review_status,
                    "reviewer": existing_record.reviewer,
                    "review_note": existing_record.review_note,
                    "reviewed_at": existing_record.reviewed_at,
                }
            )
        payload = record.model_dump(mode="json")
        self._execute(
            """
            insert into scrape_review_records (
              review_id, source_key, artifact_id, source_record_id, title,
              canonical_url, parser_confidence, review_status, reviewer,
              parsed_at, reviewed_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(source_key, artifact_id, source_record_id) do update set
              title = excluded.title,
              canonical_url = excluded.canonical_url,
              parser_confidence = excluded.parser_confidence,
              review_status = excluded.review_status,
              reviewer = excluded.reviewer,
              parsed_at = excluded.parsed_at,
              reviewed_at = excluded.reviewed_at,
              payload = excluded.payload,
              updated_at = now()
            """,
            (
                str(record.review_id),
                record.source_key,
                str(record.artifact_id),
                record.source_record_id,
                record.title,
                record.canonical_url,
                record.parser_confidence,
                record.review_status,
                record.reviewer,
                record.parsed_at.isoformat(),
                record.reviewed_at.isoformat() if record.reviewed_at else None,
                self._json(payload),
            ),
        )
        row = self._fetchone(
            """
            select payload from scrape_review_records
            where source_key = %s and artifact_id = %s and source_record_id = %s
            """,
            (record.source_key, str(record.artifact_id), record.source_record_id),
        )
        return ScrapeReviewRecord.model_validate(_payload(row))

    def list_scrape_reviews(
        self,
        *,
        source_key: str | None = None,
        review_status: str | None = None,
        review_ids: list[UUID] | None = None,
        artifact_ids: list[UUID] | None = None,
        limit: int | None = None,
    ) -> list[ScrapeReviewRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if source_key:
            clauses.append("source_key = %s")
            params.append(source_key)
        if review_status:
            clauses.append("review_status = %s")
            params.append(review_status)
        if review_ids:
            placeholders = ",".join("%s" for _ in review_ids)
            clauses.append(f"review_id in ({placeholders})")
            params.extend(str(review_id) for review_id in review_ids)
        if artifact_ids:
            placeholders = ",".join("%s" for _ in artifact_ids)
            clauses.append(f"artifact_id in ({placeholders})")
            params.extend(str(artifact_id) for artifact_id in artifact_ids)
        sql = "select payload from scrape_review_records"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by updated_at desc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [ScrapeReviewRecord.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def update_scrape_review(
        self,
        review_id: UUID,
        *,
        review_status: str,
        reviewed_by: str,
        review_note: str | None = None,
    ) -> ScrapeReviewRecord | None:
        row = self._fetchone(
            "select payload from scrape_review_records where review_id = %s",
            (str(review_id),),
        )
        if row is None:
            return None
        record = ScrapeReviewRecord.model_validate(_payload(row))
        updated = record.model_copy(
            update={
                "review_status": review_status,
                "reviewer": reviewed_by,
                "review_note": review_note,
                "reviewed_at": datetime.now(UTC),
            }
        )
        payload = updated.model_dump(mode="json")
        self._execute(
            """
            update scrape_review_records
            set review_status = %s,
                reviewer = %s,
                reviewed_at = %s,
                payload = %s,
                updated_at = now()
            where review_id = %s
            """,
            (
                updated.review_status,
                updated.reviewer,
                updated.reviewed_at.isoformat() if updated.reviewed_at else None,
                self._json(payload),
                str(review_id),
            ),
        )
        return updated

    def upsert_scrape_profile_review(self, review: ScrapeSourceProfileReview) -> ScrapeSourceProfileReview:
        payload = review.model_dump(mode="json")
        self._execute(
            """
            insert into scrape_source_profile_reviews (
              source_key, robots_policy, approved_for_fetch, reviewed_by,
              review_note, storage_policy, reviewed_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(source_key) do update set
              robots_policy = excluded.robots_policy,
              approved_for_fetch = excluded.approved_for_fetch,
              reviewed_by = excluded.reviewed_by,
              review_note = excluded.review_note,
              storage_policy = excluded.storage_policy,
              reviewed_at = excluded.reviewed_at,
              payload = excluded.payload,
              updated_at = now()
            """,
            (
                review.source_key,
                review.robots_policy,
                review.approved_for_fetch,
                review.reviewed_by,
                review.review_note,
                review.storage_policy,
                review.reviewed_at.isoformat(),
                self._json(payload),
            ),
        )
        return review

    def get_scrape_profile_review(self, source_key: str) -> ScrapeSourceProfileReview | None:
        row = self._fetchone(
            "select payload from scrape_source_profile_reviews where source_key = %s",
            (source_key,),
        )
        if row is None:
            return None
        return ScrapeSourceProfileReview.model_validate(_payload(row))

    def upsert_source_followup(self, item: SourceFollowupQueueItem) -> SourceFollowupQueueItem:
        existing = self._fetchone(
            "select payload from source_followup_queue where identity_key = %s",
            (item.identity_key,),
        )
        if existing is not None:
            existing_item = SourceFollowupQueueItem.model_validate(_payload(existing))
            item = item.model_copy(
                update={
                    "followup_id": existing_item.followup_id,
                    "status": existing_item.status,
                    "attempts": existing_item.attempts,
                    "last_error": existing_item.last_error,
                    "created_at": existing_item.created_at,
                    "updated_at": datetime.now(UTC),
                }
            )
        payload = item.model_dump(mode="json")
        self._execute(
            """
            insert into source_followup_queue (
              followup_id, identity_key, source_key, identifier_type, identifier,
              status, priority, attempts, origin_source_key, origin_review_id,
              origin_artifact_id, origin_agent_run_id, created_at, updated_at, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(identity_key) do update set
              source_key = excluded.source_key,
              identifier_type = excluded.identifier_type,
              identifier = excluded.identifier,
              status = excluded.status,
              priority = excluded.priority,
              attempts = excluded.attempts,
              origin_source_key = excluded.origin_source_key,
              origin_review_id = excluded.origin_review_id,
              origin_artifact_id = excluded.origin_artifact_id,
              origin_agent_run_id = excluded.origin_agent_run_id,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                str(item.followup_id),
                item.identity_key,
                item.source_key,
                item.identifier_type,
                item.identifier,
                item.status,
                item.priority,
                item.attempts,
                item.origin_source_key,
                str(item.origin_review_id) if item.origin_review_id else None,
                str(item.origin_artifact_id) if item.origin_artifact_id else None,
                str(item.origin_agent_run_id) if item.origin_agent_run_id else None,
                item.created_at,
                item.updated_at,
                self._json(payload),
            ),
        )
        row = self._fetchone(
            "select payload from source_followup_queue where identity_key = %s",
            (item.identity_key,),
        )
        return SourceFollowupQueueItem.model_validate(_payload(row))

    def get_source_followup(self, followup_id: UUID) -> SourceFollowupQueueItem | None:
        row = self._fetchone(
            "select payload from source_followup_queue where followup_id = %s",
            (str(followup_id),),
        )
        if row is None:
            return None
        return SourceFollowupQueueItem.model_validate(_payload(row))

    def list_source_followups(
        self,
        *,
        source_key: str | None = None,
        status: str | None = None,
        statuses: list[str] | None = None,
        identifier_type: str | None = None,
        limit: int | None = None,
    ) -> list[SourceFollowupQueueItem]:
        clauses: list[str] = []
        params: list[object] = []
        if source_key:
            clauses.append("source_key = %s")
            params.append(source_key)
        if status:
            clauses.append("status = %s")
            params.append(status)
        if statuses:
            placeholders = ",".join("%s" for _ in statuses)
            clauses.append(f"status in ({placeholders})")
            params.extend(statuses)
        if identifier_type:
            clauses.append("identifier_type = %s")
            params.append(identifier_type)
        sql = "select payload from source_followup_queue"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by priority asc, created_at asc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [SourceFollowupQueueItem.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def update_source_followup(
        self,
        followup_id: UUID,
        *,
        status: str,
        attempts: int | None = None,
        last_error: str | None = None,
        metadata: dict | None = None,
    ) -> SourceFollowupQueueItem | None:
        item = self.get_source_followup(followup_id)
        if item is None:
            return None
        updated = item.model_copy(
            update={
                "status": status,
                "attempts": item.attempts if attempts is None else attempts,
                "last_error": last_error,
                "updated_at": datetime.now(UTC),
                "metadata": {**item.metadata, **(metadata or {})},
            }
        )
        payload = updated.model_dump(mode="json")
        self._execute(
            """
            update source_followup_queue
            set status = %s,
                attempts = %s,
                updated_at = %s,
                payload = %s
            where followup_id = %s
            """,
            (
                updated.status,
                updated.attempts,
                updated.updated_at,
                self._json(payload),
                str(followup_id),
            ),
        )
        return updated

    def upsert_research_lead(self, lead: ResearchLeadRecord) -> ResearchLeadRecord:
        existing = self._fetchone(
            "select payload from research_leads where identity_key = %s",
            (lead.identity_key,),
        )
        if existing is not None:
            existing_lead = ResearchLeadRecord.model_validate(_payload(existing))
            lead = lead.model_copy(
                update={
                    "lead_id": existing_lead.lead_id,
                    "status": existing_lead.status,
                    "created_at": existing_lead.created_at,
                    "updated_at": datetime.now(UTC),
                    "metadata": {**existing_lead.metadata, **lead.metadata},
                }
            )
        payload = lead.model_dump(mode="json")
        self._execute(
            """
            insert into research_leads (
              lead_id, identity_key, lead_type, status, priority, source_key,
              origin_source_key, origin_record_id, origin_review_id,
              origin_artifact_id, origin_agent_run_id, created_at, updated_at,
              payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(identity_key) do update set
              lead_type = excluded.lead_type,
              status = excluded.status,
              priority = excluded.priority,
              source_key = excluded.source_key,
              origin_source_key = excluded.origin_source_key,
              origin_record_id = excluded.origin_record_id,
              origin_review_id = excluded.origin_review_id,
              origin_artifact_id = excluded.origin_artifact_id,
              origin_agent_run_id = excluded.origin_agent_run_id,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                str(lead.lead_id),
                lead.identity_key,
                lead.lead_type,
                lead.status,
                lead.priority,
                lead.source_key,
                lead.origin_source_key,
                lead.origin_record_id,
                str(lead.origin_review_id) if lead.origin_review_id else None,
                str(lead.origin_artifact_id) if lead.origin_artifact_id else None,
                str(lead.origin_agent_run_id) if lead.origin_agent_run_id else None,
                lead.created_at,
                lead.updated_at,
                self._json(payload),
            ),
        )
        row = self._fetchone(
            "select payload from research_leads where identity_key = %s",
            (lead.identity_key,),
        )
        return ResearchLeadRecord.model_validate(_payload(row))

    def get_research_lead(self, lead_id: UUID) -> ResearchLeadRecord | None:
        row = self._fetchone(
            "select payload from research_leads where lead_id = %s",
            (str(lead_id),),
        )
        if row is None:
            return None
        return ResearchLeadRecord.model_validate(_payload(row))

    def list_research_leads(
        self,
        *,
        status: str | None = None,
        statuses: list[str] | None = None,
        lead_type: str | None = None,
        source_key: str | None = None,
        limit: int | None = None,
    ) -> list[ResearchLeadRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if status:
            clauses.append("status = %s")
            params.append(status)
        if statuses:
            placeholders = ",".join("%s" for _ in statuses)
            clauses.append(f"status in ({placeholders})")
            params.extend(statuses)
        if lead_type:
            clauses.append("lead_type = %s")
            params.append(lead_type)
        if source_key:
            clauses.append("(source_key = %s or origin_source_key = %s)")
            params.extend([source_key, source_key])
        sql = "select payload from research_leads"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by priority asc, created_at asc"
        if limit is not None:
            sql += " limit %s"
            params.append(limit)
        return [ResearchLeadRecord.model_validate(_payload(row)) for row in self._fetchall(sql, params)]

    def update_research_lead(
        self,
        lead_id: UUID,
        *,
        status: str | None = None,
        metadata: dict | None = None,
    ) -> ResearchLeadRecord | None:
        lead = self.get_research_lead(lead_id)
        if lead is None:
            return None
        updated = lead.model_copy(
            update={
                "status": lead.status if status is None else status,
                "updated_at": datetime.now(UTC),
                "metadata": {**lead.metadata, **(metadata or {})},
            }
        )
        payload = updated.model_dump(mode="json")
        self._execute(
            """
            update research_leads
            set status = %s,
                updated_at = %s,
                payload = %s
            where lead_id = %s
            """,
            (
                updated.status,
                updated.updated_at,
                self._json(payload),
                str(lead_id),
            ),
        )
        return updated

    def upsert_claim(self, claim: ClaimSearchResult) -> ClaimSearchResult:
        payload = claim.model_dump(mode="json")
        self._execute(
            """
            insert into claims (
              claim_id, statement, claim_type, direction, confidence, evidence_level,
              species, source_object_id, source_title, source_url, support_count,
              contradiction_count, payload
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict(claim_id) do update set
              statement = excluded.statement,
              claim_type = excluded.claim_type,
              direction = excluded.direction,
              confidence = excluded.confidence,
              evidence_level = excluded.evidence_level,
              species = excluded.species,
              source_object_id = excluded.source_object_id,
              source_title = excluded.source_title,
              source_url = excluded.source_url,
              support_count = excluded.support_count,
              contradiction_count = excluded.contradiction_count,
              payload = excluded.payload,
              updated_at = now()
            """,
            (
                str(claim.claim_id),
                claim.statement,
                str(claim.claim_type),
                str(claim.direction),
                claim.confidence,
                str(claim.evidence_level),
                claim.species,
                str(claim.source_object_id) if claim.source_object_id else None,
                claim.source_title,
                claim.source_url,
                claim.support_count,
                claim.contradiction_count,
                self._json(payload),
            ),
        )
        return claim

    def _upsert_run(self, handle: AsyncRunHandle) -> None:
        payload = handle.model_dump(mode="json")
        self._execute(
            """
            insert into async_runs (run_id, run_kind, run_name, status, payload)
            values (%s, %s, %s, %s, %s)
            on conflict(run_id) do update set
              run_kind = excluded.run_kind,
              run_name = excluded.run_name,
              status = excluded.status,
              payload = excluded.payload,
              updated_at = now()
            """,
            (str(handle.run_id), handle.run_kind, handle.run_name, str(handle.status), self._json(payload)),
        )

    def _init_schema(self) -> None:
        self._execute(
            """
            create table if not exists ingestion_sources (
              source_key text primary key,
              display_name text not null,
              source_kind text not null,
              base_url text,
              documentation_url text,
              license_policy text not null,
              requires_api_key boolean not null default false,
              enabled boolean not null default true,
              priority integer not null default 100,
              phase integer not null default 1,
              rate_limit_per_minute integer,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create table if not exists source_queries (
              source_key text not null,
              query_name text not null,
              query_text text not null,
              query_params jsonb not null default '{}'::jsonb,
              track text,
              object_type text,
              active boolean not null default true,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now(),
              primary key (source_key, query_name)
            );

            create table if not exists source_fetch_runs (
              fetch_run_id text primary key,
              source_key text not null,
              query_name text,
              status text not null,
              records_found integer not null default 0,
              records_inserted integer not null default 0,
              records_updated integer not null default 0,
              error_message text,
              started_at timestamptz,
              completed_at timestamptz,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create table if not exists raw_source_records (
              raw_record_id text primary key,
              source_key text not null,
              fetch_run_id text,
              source_record_id text,
              source_url text,
              content_hash text not null,
              payload jsonb not null,
              retrieved_at timestamptz,
              first_seen_at timestamptz not null default now(),
              last_seen_at timestamptz not null default now(),
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now(),
              unique (source_key, content_hash)
            );

            create index if not exists raw_source_records_source_idx
              on raw_source_records(source_key, last_seen_at desc);

            create table if not exists research_objects (
              object_id text primary key,
              object_type text not null,
              title text,
              abstract text,
              canonical_url text,
              publication_year integer,
              published_at text,
              source_key text,
              raw_record_id text,
              dedupe_key text not null unique,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists research_objects_source_idx
              on research_objects(source_key, updated_at desc);
            create index if not exists research_objects_title_idx
              on research_objects(title);

            create table if not exists identifier_links (
              object_id text not null,
              identifier_type text not null,
              identifier_value text not null,
              created_at timestamptz not null default now(),
              primary key (object_id, identifier_type, identifier_value)
            );

            create table if not exists document_chunks (
              chunk_id text primary key,
              object_id text not null,
              chunk_index integer not null,
              section_label text,
              text_content text not null,
              content_hash text not null,
              token_count integer,
              char_start integer,
              char_end integer,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now(),
              unique (object_id, chunk_index)
            );

            create index if not exists document_chunks_object_idx
              on document_chunks(object_id, chunk_index);
            create index if not exists document_chunks_hash_idx
              on document_chunks(content_hash);

            create table if not exists text_embeddings (
              embedding_id text primary key,
              chunk_id text not null,
              object_id text not null,
              chunk_index integer not null,
              source_key text,
              object_type text,
              embedding_model text not null,
              embedding_dimensions integer not null,
              content_hash text not null,
              vector_json jsonb not null,
              payload jsonb not null,
              embedded_at timestamptz not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now(),
              unique (chunk_id, embedding_model)
            );

            create index if not exists text_embeddings_chunk_idx
              on text_embeddings(chunk_id, embedding_model);
            create index if not exists text_embeddings_object_idx
              on text_embeddings(object_id, chunk_index);
            create index if not exists text_embeddings_source_model_idx
              on text_embeddings(source_key, embedding_model);

            create table if not exists resolved_entities (
              entity_id text primary key,
              entity_type text not null,
              canonical_name text not null,
              normalized_key text not null,
              resolver_name text not null,
              resolver_version text not null,
              confidence real not null default 1,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now(),
              unique (entity_type, normalized_key)
            );

            create index if not exists resolved_entities_type_key_idx
              on resolved_entities(entity_type, normalized_key);
            create index if not exists resolved_entities_name_idx
              on resolved_entities(canonical_name);

            create table if not exists entity_aliases (
              alias_id text primary key,
              entity_id text not null,
              entity_type text not null,
              alias text not null,
              alias_normalized text not null,
              canonical_name text not null,
              normalized_key text not null,
              resolver_name text not null,
              resolver_version text not null,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now(),
              unique (entity_type, alias_normalized, normalized_key)
            );

            create index if not exists entity_aliases_alias_idx
              on entity_aliases(alias_normalized);
            create index if not exists entity_aliases_entity_idx
              on entity_aliases(entity_id);

            create table if not exists entity_mentions (
              mention_id text primary key,
              entity_id text,
              object_id text not null,
              chunk_id text not null,
              chunk_index integer not null,
              source_key text,
              entity_type text not null,
              canonical_name text not null,
              normalized_key text not null,
              matched_text text not null,
              matched_alias text not null,
              chunk_char_start integer not null,
              chunk_char_end integer not null,
              resolver_name text not null,
              resolver_version text not null,
              confidence real not null default 1,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now(),
              unique (chunk_id, entity_type, normalized_key, chunk_char_start, chunk_char_end, resolver_name)
            );

            create index if not exists entity_mentions_source_idx
              on entity_mentions(source_key, entity_type);
            create index if not exists entity_mentions_chunk_idx
              on entity_mentions(chunk_id, chunk_char_start);
            create index if not exists entity_mentions_entity_idx
              on entity_mentions(entity_id);

            create table if not exists claims (
              claim_id text primary key,
              statement text not null,
              claim_type text not null,
              direction text,
              confidence real not null default 0,
              evidence_level text,
              species text,
              source_object_id text,
              source_title text,
              source_url text,
              support_count integer not null default 0,
              contradiction_count integer not null default 0,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists claims_statement_idx on claims(statement);
            create index if not exists claims_species_idx on claims(species);
            create index if not exists claims_type_idx on claims(claim_type);
            create index if not exists claims_confidence_idx on claims(confidence desc);

            create table if not exists hypotheses (
              hypothesis_id text primary key,
              title text not null,
              hypothesis text not null,
              status text not null,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create table if not exists async_runs (
              run_id text primary key,
              run_kind text not null,
              run_name text not null,
              status text not null,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists async_runs_status_idx on async_runs(status, updated_at desc);

            create table if not exists agent_runs (
              agent_run_id text primary key,
              agent_name text not null,
              agent_version text not null,
              model_profile text not null,
              status text not null,
              source_key text,
              partition_date text,
              dagster_run_id text,
              started_at timestamptz not null,
              completed_at timestamptz,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists agent_runs_name_status_idx
              on agent_runs(agent_name, status, created_at desc);
            create index if not exists agent_runs_source_idx
              on agent_runs(source_key, created_at desc);

            create table if not exists agent_run_reviews (
              review_id text primary key,
              agent_run_id text not null,
              reviewer text not null,
              verdict text not null,
              created_at timestamptz not null,
              payload jsonb not null,
              updated_at timestamptz not null default now()
            );

            create index if not exists agent_run_reviews_run_idx
              on agent_run_reviews(agent_run_id, created_at desc);
            create index if not exists agent_run_reviews_verdict_idx
              on agent_run_reviews(verdict, created_at desc);

            create table if not exists research_briefs (
              brief_id text primary key,
              agent_run_id text,
              topic text not null,
              disease_scope text not null,
              source_key text,
              brief_style text not null,
              model_profile text not null,
              review_mode text not null,
              status text not null,
              citation_count integer not null default 0,
              finding_count integer not null default 0,
              hypothesis_count integer not null default 0,
              research_lead_count integer not null default 0,
              error_count integer not null default 0,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists research_briefs_status_idx
              on research_briefs(status, created_at desc);
            create index if not exists research_briefs_source_idx
              on research_briefs(source_key, created_at desc);
            create index if not exists research_briefs_topic_idx
              on research_briefs(topic, created_at desc);

            create table if not exists research_brief_evaluations (
              evaluation_id text primary key,
              brief_id text not null,
              agent_run_id text,
              topic text not null,
              source_key text,
              model_profile text not null,
              overall_score double precision not null,
              passes_quality_bar boolean not null default false,
              readiness text not null,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists research_brief_evaluations_brief_idx
              on research_brief_evaluations(brief_id, created_at desc);
            create index if not exists research_brief_evaluations_readiness_idx
              on research_brief_evaluations(readiness, created_at desc);
            create index if not exists research_brief_evaluations_quality_idx
              on research_brief_evaluations(passes_quality_bar, overall_score desc);

            create table if not exists therapy_ideas (
              therapy_idea_id text primary key,
              committee_run_id text,
              agent_run_id text,
              source_program_id text,
              source_brief_id text,
              source_evaluation_id text,
              topic text not null,
              source_key text,
              status text not null,
              promotion_state text,
              score double precision not null default 0.5,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists therapy_ideas_status_idx
              on therapy_ideas(status, score desc, updated_at desc);
            create index if not exists therapy_ideas_brief_idx
              on therapy_ideas(source_brief_id, updated_at desc);
            create index if not exists therapy_ideas_evaluation_idx
              on therapy_ideas(source_evaluation_id, updated_at desc);
            create index if not exists therapy_ideas_committee_idx
              on therapy_ideas(committee_run_id, updated_at desc);

            create table if not exists research_programs (
              program_id text primary key,
              agent_run_id text,
              title text not null,
              thesis_area text not null,
              status text not null,
              gate_decision text not null,
              confidence_score double precision not null default 0.5,
              evidence_loop_count integer not null default 0,
              max_evidence_loops integer not null default 2,
              source_query text,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists research_programs_status_idx
              on research_programs(status, confidence_score desc, updated_at desc);
            create index if not exists research_programs_gate_idx
              on research_programs(gate_decision, confidence_score desc, updated_at desc);
            create index if not exists research_programs_area_idx
              on research_programs(thesis_area, updated_at desc);
            create index if not exists research_programs_agent_run_idx
              on research_programs(agent_run_id, updated_at desc);

            create table if not exists validation_decisions (
              decision_record_id text primary key,
              decision_id text not null unique,
              packet_id text not null,
              candidate_id text not null,
              source_type text not null,
              source_id text not null,
              therapy_idea_id text,
              title text not null,
              outcome text not null,
              confidence double precision not null default 0.5,
              validation_ready boolean not null default false,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists validation_decisions_outcome_idx
              on validation_decisions(outcome, confidence desc, updated_at desc);
            create index if not exists validation_decisions_therapy_idx
              on validation_decisions(therapy_idea_id, updated_at desc);
            create index if not exists validation_decisions_candidate_idx
              on validation_decisions(candidate_id, updated_at desc);
            create index if not exists validation_decisions_packet_idx
              on validation_decisions(packet_id, updated_at desc);

            create table if not exists validation_plans (
              plan_id text primary key,
              agent_run_id text,
              brief_id text not null,
              evaluation_id text,
              topic text not null,
              source_key text,
              model_profile text not null,
              status text not null,
              readiness text not null,
              task_count integer not null default 0,
              hypothesis_count integer not null default 0,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists validation_plans_brief_idx
              on validation_plans(brief_id, created_at desc);
            create index if not exists validation_plans_evaluation_idx
              on validation_plans(evaluation_id, created_at desc);
            create index if not exists validation_plans_status_idx
              on validation_plans(status, readiness, created_at desc);

            create table if not exists validation_request_queue (
              queue_item_id text primary key,
              identity_key text not null unique,
              status text not null default 'needs_approval',
              priority integer not null default 100,
              plan_id text not null,
              task_id text not null,
              brief_id text not null,
              evaluation_id text,
              source_key text,
              task_type text not null,
              title text not null,
              validation_type text not null,
              target_name text,
              candidate_name text,
              last_run_id text,
              attempts integer not null default 0,
              last_error text,
              approved_by text,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists validation_request_queue_plan_idx
              on validation_request_queue(plan_id, priority, created_at);
            create index if not exists validation_request_queue_status_idx
              on validation_request_queue(status, priority, created_at);
            create index if not exists validation_request_queue_source_idx
              on validation_request_queue(source_key, status, priority, created_at);
            create index if not exists validation_request_queue_type_idx
              on validation_request_queue(task_type, validation_type, status);

            create table if not exists compute_jobs (
              compute_job_id text primary key,
              queue_item_id text,
              status text not null,
              runner_kind text not null,
              compute_profile text not null,
              validation_type text,
              title text not null,
              runpod_job_id text,
              dagster_run_id text,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists compute_jobs_status_idx
              on compute_jobs(status, updated_at desc);
            create index if not exists compute_jobs_runner_idx
              on compute_jobs(runner_kind, updated_at desc);
            create index if not exists compute_jobs_queue_item_idx
              on compute_jobs(queue_item_id, updated_at desc);

            create table if not exists command_center_board_stages (
              entity_type text not null,
              entity_id text not null,
              board_stage text not null,
              actor text not null,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now(),
              primary key (entity_type, entity_id)
            );

            create index if not exists command_center_board_stages_stage_idx
              on command_center_board_stages(board_stage, updated_at desc);

            create table if not exists command_center_activity_events (
              event_id text primary key,
              occurred_at timestamptz not null,
              actor text not null,
              source text not null,
              event_type text not null,
              entity_type text not null,
              entity_id text not null,
              severity text not null,
              correlation_id text,
              payload jsonb not null,
              created_at timestamptz not null default now()
            );

            create index if not exists command_center_activity_events_time_idx
              on command_center_activity_events(occurred_at desc);
            create index if not exists command_center_activity_events_entity_idx
              on command_center_activity_events(entity_type, entity_id, occurred_at desc);
            create index if not exists command_center_activity_events_source_idx
              on command_center_activity_events(source, occurred_at desc);

            create table if not exists md_expert_review_packets (
              packet_id text primary key,
              packet_hash text not null,
              status text not null,
              compute_job_id text,
              queue_item_id text,
              endpoint_id text not null,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists md_expert_review_packets_hash_idx
              on md_expert_review_packets(packet_hash, updated_at desc);
            create index if not exists md_expert_review_packets_status_idx
              on md_expert_review_packets(status, updated_at desc);

            create table if not exists md_expert_approvals (
              approval_id text primary key,
              packet_id text not null,
              packet_hash text not null,
              decision text not null,
              reviewer_name text not null,
              reviewer_contact text not null,
              reviewed_at timestamptz not null,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists md_expert_approvals_hash_idx
              on md_expert_approvals(packet_hash, decision, reviewed_at desc);

            create table if not exists research_brief_queue (
              queue_item_id text primary key,
              identity_key text not null unique,
              status text not null default 'queued',
              priority integer not null default 100,
              topic text not null,
              disease_scope text not null,
              source_key text,
              brief_style text not null,
              model_profile text not null,
              review_mode text not null,
              last_brief_id text,
              last_agent_run_id text,
              attempts integer not null default 0,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists research_brief_queue_status_idx
              on research_brief_queue(status, priority, created_at);
            create index if not exists research_brief_queue_source_idx
              on research_brief_queue(source_key, status, priority, created_at);
            create index if not exists research_brief_queue_topic_idx
              on research_brief_queue(topic, created_at);

            create table if not exists artifacts (
              artifact_id text primary key,
              artifact_type text not null,
              uri text not null,
              legal_status text,
              mime_type text,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create table if not exists scrape_review_records (
              review_id text primary key,
              source_key text not null,
              artifact_id text not null,
              source_record_id text not null,
              title text,
              canonical_url text,
              parser_confidence real not null default 0,
              review_status text not null default 'needs_review',
              reviewer text,
              parsed_at timestamptz,
              reviewed_at timestamptz,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now(),
              unique (source_key, artifact_id, source_record_id)
            );

            create index if not exists scrape_review_records_status_idx
              on scrape_review_records(source_key, review_status, updated_at desc);

            create table if not exists source_followup_queue (
              followup_id text primary key,
              identity_key text not null unique,
              source_key text not null,
              identifier_type text not null,
              identifier text not null,
              status text not null default 'queued',
              priority integer not null default 100,
              attempts integer not null default 0,
              origin_source_key text,
              origin_review_id text,
              origin_artifact_id text,
              origin_agent_run_id text,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists source_followup_queue_status_idx
              on source_followup_queue(source_key, status, priority, created_at);
            create index if not exists source_followup_queue_identifier_idx
              on source_followup_queue(identifier_type, identifier);

            create table if not exists research_leads (
              lead_id text primary key,
              identity_key text not null unique,
              lead_type text not null,
              status text not null default 'new',
              priority integer not null default 100,
              source_key text,
              origin_source_key text,
              origin_record_id text,
              origin_review_id text,
              origin_artifact_id text,
              origin_agent_run_id text,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );

            create index if not exists research_leads_status_idx
              on research_leads(status, priority, created_at);
            create index if not exists research_leads_source_idx
              on research_leads(source_key, status, priority, created_at);
            create index if not exists research_leads_agent_idx
              on research_leads(origin_agent_run_id, created_at desc);

            create table if not exists scrape_source_profile_reviews (
              source_key text primary key,
              robots_policy text not null,
              approved_for_fetch boolean not null default false,
              reviewed_by text not null,
              review_note text,
              storage_policy text,
              reviewed_at timestamptz not null,
              payload jsonb not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );
            """
        )
        self._execute("alter table therapy_ideas add column if not exists source_program_id text")
        self._execute(
            "create index if not exists therapy_ideas_program_idx on therapy_ideas(source_program_id, updated_at desc)"
        )

    def _seed_claims_if_empty(self) -> None:
        if self._scalar("select count(*) from claims") > 0:
            return
        for claim in seed_claims():
            self.upsert_claim(claim)

    def _execute(self, sql: str, params: tuple[object, ...] | list[object] | None = None) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)
        self.conn.commit()

    def _fetchone(self, sql: str, params: tuple[object, ...] | list[object] | None = None) -> dict[str, Any] | None:
        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def _fetchall(self, sql: str, params: tuple[object, ...] | list[object] | None = None) -> list[dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall())

    def _scalar(self, sql: str, params: tuple[object, ...] | list[object] | None = None) -> int:
        row = self._fetchone(sql, params)
        if row is None:
            return 0
        return int(next(iter(row.values())))


def _load_json_adapter():
    from psycopg2.extras import Json

    return Json


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    value = row["payload"]
    if isinstance(value, str):
        return json.loads(value)
    return value


def _claim_visible_in_search(claim: ClaimSearchResult, *, include_drafts: bool) -> bool:
    metadata = claim.metadata
    curation_status = metadata.get("curation_status")
    if curation_status in {"reject", "merge_duplicate"}:
        return False
    if include_drafts:
        return True
    if metadata.get("extraction_status") == "draft" and curation_status != "promote":
        return False
    return True
