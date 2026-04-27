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
    ArtifactHandle,
    AsyncRunHandle,
    CandidateDossier,
    CandidateDossierRequest,
    ClaimSearchRequest,
    ClaimSearchResult,
    CommitHypothesisRequest,
    DocumentChunk,
    EmbeddingCoverageSummary,
    EntityAlias,
    EntityMention,
    RawSourceRecord,
    ResearchChunkSearchRequest,
    ResearchChunkSearchResult,
    ResolvedEntity,
    ResearchObject,
    ResearchSource,
    ScrapeReviewRecord,
    ScrapeSourceProfileReview,
    SourceQuery,
    TextEmbedding,
    TextEmbeddingSearchRequest,
    TextEmbeddingSearchResult,
    ValidationRequest,
)
from .local_store import build_research_object_dedupe_key, document_chunk_from_payload
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

    def get_research_object(self, object_id: UUID) -> ResearchObject | None:
        row = self._fetchone("select payload from research_objects where object_id = %s", (str(object_id),))
        if row is None:
            return None
        return ResearchObject.model_validate(_payload(row))

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

        fetch_limit = min(max(request.limit * 20, request.limit), 500)
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
        raise NotImplementedError("Scrape review persistence is not wired for hosted Postgres yet")

    def list_scrape_reviews(self, **_: Any) -> list[ScrapeReviewRecord]:
        raise NotImplementedError("Scrape review persistence is not wired for hosted Postgres yet")

    def update_scrape_review(self, review_id: UUID, **_: Any) -> ScrapeReviewRecord | None:
        raise NotImplementedError("Scrape review persistence is not wired for hosted Postgres yet")

    def upsert_scrape_profile_review(self, review: ScrapeSourceProfileReview) -> ScrapeSourceProfileReview:
        raise NotImplementedError("Scrape profile review persistence is not wired for hosted Postgres yet")

    def get_scrape_profile_review(self, source_key: str) -> ScrapeSourceProfileReview | None:
        raise NotImplementedError("Scrape profile review persistence is not wired for hosted Postgres yet")

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
