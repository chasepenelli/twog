"""Local-first SQLite repository for Ingestion Bridge v2.

This is the default storage backend while the v2 system is taking shape. It
keeps the MCP/service/Dagster contracts useful without requiring Supabase or a
remote Postgres instance during design and early implementation.
"""

from __future__ import annotations

from datetime import UTC, datetime
import json
import os
import sqlite3
from hashlib import sha256
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
    ComputeJobRecord,
    CommitHypothesisRequest,
    DocumentChunk,
    EmbeddingCoverageSummary,
    EntityAlias,
    EntityMention,
    HypothesisDraft,
    MDExpertApprovalRecord,
    MDExpertReviewPacketRecord,
    PrimitiveCallEvent,
    PublicCandidateDecisionEvent,
    PublicCandidateLibraryRequest,
    PublicCandidateRecord,
    PublicCandidateSnapshot,
    RawSourceRecord,
    ResearchChunkSearchRequest,
    ResearchChunkSearchResult,
    ResearchProgramRecord,
    ResearchBriefEvaluationRecord,
    ResearchBriefQueueItem,
    ResearchBriefRecord,
    ResearchLeadRecord,
    RewardEventRecord,
    ResolvedEntity,
    ResearchObject,
    ResearchSource,
    ScrapeSourceProfileReview,
    ScrapeReviewRecord,
    SourceFollowupQueueItem,
    SourceQuery,
    SourceVersionRecord,
    TextEmbedding,
    TextEmbeddingSearchRequest,
    TextEmbeddingSearchResult,
    TherapyIdeaRecord,
    ValidationDecisionRecord,
    ValidationPlanRecord,
    ValidationRequest,
    ValidationRequestQueueItem,
)
from .repository import ResearchRepository, cosine_similarity, keyword_chunk_score, keyword_terms, seed_claims


DEFAULT_LOCAL_DB_PATH = Path(
    os.getenv("HSA_LOCAL_DB_PATH", "var/hsa_research/ingestion_bridge.sqlite3")
)


class SQLiteResearchRepository(ResearchRepository):
    """SQLite-backed local repository.

    The table layout stores typed JSON payloads plus query-friendly columns.
    That gives us durable local state now and leaves the service contract
    unchanged when a Postgres/Supabase adapter is added later.
    """

    def __init__(self, db_path: str | Path | None = None, seed: bool = True) -> None:
        self.db_path = Path(db_path) if db_path else DEFAULT_LOCAL_DB_PATH
        sqlite_target = ":memory:" if str(self.db_path) == ":memory:" else self.db_path
        if sqlite_target != ":memory:":
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(sqlite_target)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("pragma foreign_keys = on")
        self._init_schema()
        if seed:
            self._seed_claims_if_empty()

    def seed_sources(self, sources: list[ResearchSource]) -> None:
        for source in sources:
            self.upsert_source(source)

    def upsert_source(self, source: ResearchSource) -> ResearchSource:
        payload = source.model_dump(mode="json")
        self.conn.execute(
            """
            insert into ingestion_sources (
              source_key, display_name, source_kind, base_url, documentation_url,
              license_policy, requires_api_key, enabled, priority, phase,
              rate_limit_per_minute, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
              updated_at = current_timestamp
            """,
            (
                source.source_key,
                source.display_name,
                str(source.source_kind),
                str(source.base_url) if source.base_url else None,
                str(source.documentation_url) if source.documentation_url else None,
                source.license_policy,
                int(source.requires_api_key),
                int(source.enabled),
                source.priority,
                source.phase,
                source.rate_limit_per_minute,
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return source

    def list_sources(self, enabled_only: bool = False) -> list[ResearchSource]:
        sql = "select payload from ingestion_sources"
        params: list[object] = []
        if enabled_only:
            sql += " where enabled = ?"
            params.append(1)
        sql += " order by priority asc, source_key asc"
        return [
            ResearchSource.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_source_query(self, query: SourceQuery) -> SourceQuery:
        payload = query.model_dump(mode="json")
        self.conn.execute(
            """
            insert into source_queries (
              source_key, query_name, query_text, query_params, track, object_type, active, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(source_key, query_name) do update set
              query_text = excluded.query_text,
              query_params = excluded.query_params,
              track = excluded.track,
              object_type = excluded.object_type,
              active = excluded.active,
              payload = excluded.payload,
              updated_at = current_timestamp
            """,
            (
                query.source_key,
                query.query_name,
                query.query_text,
                json.dumps(query.query_params, sort_keys=True),
                query.track,
                str(query.object_type) if query.object_type else None,
                int(query.active),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return query

    def list_source_queries(self, source_key: str | None = None, active_only: bool = True) -> list[SourceQuery]:
        clauses: list[str] = []
        params: list[object] = []
        if source_key:
            clauses.append("source_key = ?")
            params.append(source_key)
        if active_only:
            clauses.append("active = ?")
            params.append(1)
        sql = "select payload from source_queries"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by source_key asc, query_name asc"
        return [
            SourceQuery.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def set_source_query_active(self, source_key: str, query_name: str, active: bool) -> None:
        row = self.conn.execute(
            "select payload from source_queries where source_key = ? and query_name = ?",
            (source_key, query_name),
        ).fetchone()
        if row is None:
            return
        query = SourceQuery.model_validate(json.loads(row["payload"]))
        updated = query.model_copy(update={"active": active})
        payload = updated.model_dump(mode="json")
        self.conn.execute(
            """
            update source_queries
            set active = ?, payload = ?, updated_at = current_timestamp
            where source_key = ? and query_name = ?
            """,
            (int(active), json.dumps(payload, sort_keys=True), source_key, query_name),
        )
        self.conn.commit()

    def create_fetch_run(self, source_key: str, query_name: str | None = None) -> UUID:
        fetch_run_id = uuid4()
        self.conn.execute(
            """
            insert into source_fetch_runs (fetch_run_id, source_key, query_name, status, started_at)
            values (?, ?, ?, 'running', current_timestamp)
            """,
            (str(fetch_run_id), source_key, query_name),
        )
        self.conn.commit()
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
        self.conn.execute(
            """
            update source_fetch_runs
            set status = ?,
                records_found = ?,
                records_inserted = ?,
                records_updated = ?,
                error_message = ?,
                completed_at = current_timestamp,
                updated_at = current_timestamp
            where fetch_run_id = ?
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
        self.conn.commit()

    def upsert_raw_record(self, record: RawSourceRecord, fetch_run_id: UUID | None = None) -> UUID:
        raw_record_id = record.id or uuid4()
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into raw_source_records (
              raw_record_id, source_key, fetch_run_id, source_record_id, source_url,
              content_hash, payload, retrieved_at
            )
            values (?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(source_key, content_hash) do update set
              fetch_run_id = excluded.fetch_run_id,
              source_record_id = excluded.source_record_id,
              source_url = excluded.source_url,
              payload = excluded.payload,
              last_seen_at = current_timestamp,
              updated_at = current_timestamp
            """,
            (
                str(raw_record_id),
                record.source_key,
                str(fetch_run_id) if fetch_run_id else None,
                record.source_record_id,
                record.source_url,
                record.content_hash,
                json.dumps(payload, sort_keys=True),
                record.retrieved_at.isoformat(),
            ),
        )
        row = self.conn.execute(
            "select raw_record_id from raw_source_records where source_key = ? and content_hash = ?",
            (record.source_key, record.content_hash),
        ).fetchone()
        self.conn.commit()
        return UUID(row["raw_record_id"])

    def upsert_research_object(self, obj: ResearchObject, raw_record_id: UUID | None = None) -> UUID:
        dedupe_key = obj.dedupe_key or build_research_object_dedupe_key(obj)
        existing = self.conn.execute(
            "select object_id, payload from research_objects where dedupe_key = ?",
            (dedupe_key,),
        ).fetchone()
        object_id = UUID(existing["object_id"]) if existing else obj.id
        if existing:
            existing_obj = ResearchObject.model_validate(json.loads(existing["payload"]))
            if should_preserve_existing_research_object(existing_obj, obj):
                for identifier_type, identifier_value in obj.identifiers.items():
                    if identifier_value:
                        self.link_identifier(object_id, identifier_type, identifier_value)
                return object_id
        payload = obj.model_copy(
            update={"id": object_id, "raw_record_id": raw_record_id, "dedupe_key": dedupe_key}
        ).model_dump(mode="json")
        self.conn.execute(
            """
            insert into research_objects (
              object_id, object_type, title, abstract, canonical_url,
              publication_year, published_at, source_key, raw_record_id,
              dedupe_key, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
              updated_at = current_timestamp
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
                json.dumps(payload, sort_keys=True),
            ),
        )
        row = self.conn.execute(
            "select object_id from research_objects where dedupe_key = ?",
            (dedupe_key,),
        ).fetchone()
        object_uuid = UUID(row["object_id"])
        for identifier_type, identifier_value in obj.identifiers.items():
            if identifier_value:
                self.link_identifier(object_uuid, identifier_type, identifier_value)
        self.conn.commit()
        return object_uuid

    def list_research_objects(
        self,
        object_type: str | None = None,
        source_key: str | None = None,
        limit: int | None = None,
    ) -> list[ResearchObject]:
        clauses: list[str] = []
        params: list[object] = []
        if object_type:
            clauses.append("object_type = ?")
            params.append(object_type)
        if source_key:
            clauses.append("source_key = ?")
            params.append(source_key)
        sql = "select payload from research_objects"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by updated_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            ResearchObject.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def link_identifier(self, object_id: UUID, identifier_type: str, identifier_value: str) -> None:
        self.conn.execute(
            """
            insert into identifier_links (object_id, identifier_type, identifier_value)
            values (?, ?, ?)
            on conflict(object_id, identifier_type, identifier_value) do nothing
            """,
            (str(object_id), identifier_type, identifier_value),
        )

    def update_research_object(self, obj: ResearchObject) -> ResearchObject | None:
        existing = self.conn.execute(
            "select object_id from research_objects where object_id = ?",
            (str(obj.id),),
        ).fetchone()
        if existing is None:
            return None
        dedupe_key = obj.dedupe_key or build_research_object_dedupe_key(obj)
        conflict = self.conn.execute(
            "select object_id from research_objects where dedupe_key = ? and object_id != ?",
            (dedupe_key, str(obj.id)),
        ).fetchone()
        if conflict is not None:
            raise ValueError(f"Research object dedupe key already exists: {dedupe_key}")
        payload = obj.model_copy(update={"dedupe_key": dedupe_key}).model_dump(mode="json")
        self.conn.execute(
            """
            update research_objects
            set object_type = ?,
                title = ?,
                abstract = ?,
                canonical_url = ?,
                publication_year = ?,
                published_at = ?,
                source_key = ?,
                raw_record_id = ?,
                dedupe_key = ?,
                payload = ?,
                updated_at = current_timestamp
            where object_id = ?
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
                json.dumps(payload, sort_keys=True),
                str(obj.id),
            ),
        )
        self.conn.execute("delete from identifier_links where object_id = ?", (str(obj.id),))
        for identifier_type, identifier_value in payload.get("identifiers", {}).items():
            if identifier_value:
                self.link_identifier(obj.id, identifier_type, identifier_value)
        self.conn.commit()
        return ResearchObject.model_validate(payload)

    def get_research_object(self, object_id: UUID) -> ResearchObject | None:
        row = self.conn.execute(
            "select payload from research_objects where object_id = ?",
            (str(object_id),),
        ).fetchone()
        if row is None:
            return None
        return ResearchObject.model_validate(json.loads(row["payload"]))

    def get_document_chunk(self, chunk_id: UUID) -> DocumentChunk | None:
        row = self.conn.execute(
            "select chunk_id, object_id, payload from document_chunks where chunk_id = ?",
            (str(chunk_id),),
        ).fetchone()
        if row is None:
            return None
        return document_chunk_from_payload(
            json.loads(row["payload"]),
            chunk_id=row["chunk_id"],
            object_id=row["object_id"],
        )

    def get_raw_record_payload(self, raw_record_id: UUID) -> dict[str, Any] | None:
        row = self.conn.execute(
            "select payload from raw_source_records where raw_record_id = ?",
            (str(raw_record_id),),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row["payload"])

    def upsert_entity(self, entity: ResolvedEntity) -> ResolvedEntity:
        payload = entity.model_dump(mode="json")
        self.conn.execute(
            """
            insert into resolved_entities (
              entity_id, entity_type, canonical_name, normalized_key,
              resolver_name, resolver_version, confidence, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(entity_type, normalized_key) do update set
              canonical_name = excluded.canonical_name,
              resolver_name = excluded.resolver_name,
              resolver_version = excluded.resolver_version,
              confidence = excluded.confidence,
              payload = excluded.payload,
              updated_at = current_timestamp
            """,
            (
                str(entity.entity_id),
                entity.entity_type,
                entity.canonical_name,
                entity.normalized_key,
                entity.resolver_name,
                entity.resolver_version,
                entity.confidence,
                json.dumps(payload, sort_keys=True),
            ),
        )
        row = self.conn.execute(
            "select payload from resolved_entities where entity_type = ? and normalized_key = ?",
            (entity.entity_type, entity.normalized_key),
        ).fetchone()
        self.conn.commit()
        return ResolvedEntity.model_validate(json.loads(row["payload"]))

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
            clauses.append("entity_type = ?")
            params.append(entity_type)
        if query:
            clauses.append("(lower(canonical_name) like lower(?) or lower(normalized_key) like lower(?))")
            params.extend((f"%{query}%", f"%{query}%"))
        sql = "select payload from resolved_entities"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by entity_type, canonical_name"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            ResolvedEntity.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_entity_alias(self, alias: EntityAlias) -> EntityAlias:
        payload = alias.model_dump(mode="json")
        self.conn.execute(
            """
            insert into entity_aliases (
              alias_id, entity_id, entity_type, alias, alias_normalized,
              canonical_name, normalized_key, resolver_name, resolver_version, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(entity_type, alias_normalized, normalized_key) do update set
              entity_id = excluded.entity_id,
              alias = excluded.alias,
              canonical_name = excluded.canonical_name,
              resolver_name = excluded.resolver_name,
              resolver_version = excluded.resolver_version,
              payload = excluded.payload,
              updated_at = current_timestamp
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
                json.dumps(payload, sort_keys=True),
            ),
        )
        row = self.conn.execute(
            """
            select payload from entity_aliases
            where entity_type = ? and alias_normalized = ? and normalized_key = ?
            """,
            (alias.entity_type, alias.alias_normalized, alias.normalized_key),
        ).fetchone()
        self.conn.commit()
        return EntityAlias.model_validate(json.loads(row["payload"]))

    def list_entity_aliases(
        self,
        *,
        entity_type: str | None = None,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[EntityAlias]:
        clauses: list[str] = []
        params: list[object] = []
        if entity_type:
            clauses.append("entity_type = ?")
            params.append(entity_type)
        if query:
            clauses.append("(lower(alias_normalized) like lower(?) or lower(normalized_key) like lower(?))")
            params.extend((f"%{query}%", f"%{query}%"))
        sql = "select payload from entity_aliases"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by entity_type, alias_normalized, normalized_key"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            EntityAlias.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_entity_mention(self, mention: EntityMention) -> EntityMention:
        payload = mention.model_dump(mode="json")
        self.conn.execute(
            """
            insert into entity_mentions (
              mention_id, entity_id, object_id, chunk_id, chunk_index,
              source_key, entity_type, canonical_name, normalized_key,
              matched_text, matched_alias, chunk_char_start, chunk_char_end,
              resolver_name, resolver_version, confidence, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(chunk_id, entity_type, normalized_key, chunk_char_start, chunk_char_end, resolver_name) do update set
              entity_id = excluded.entity_id,
              canonical_name = excluded.canonical_name,
              matched_text = excluded.matched_text,
              matched_alias = excluded.matched_alias,
              confidence = excluded.confidence,
              payload = excluded.payload,
              updated_at = current_timestamp
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
                json.dumps(payload, sort_keys=True),
            ),
        )
        row = self.conn.execute(
            """
            select payload from entity_mentions
            where chunk_id = ? and entity_type = ? and normalized_key = ?
              and chunk_char_start = ? and chunk_char_end = ? and resolver_name = ?
            """,
            (
                str(mention.chunk_id),
                mention.entity_type,
                mention.normalized_key,
                mention.chunk_char_start,
                mention.chunk_char_end,
                mention.resolver_name,
            ),
        ).fetchone()
        self.conn.commit()
        return EntityMention.model_validate(json.loads(row["payload"]))

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
            clauses.append("source_key = ?")
            params.append(source_key)
        if object_id:
            clauses.append("object_id = ?")
            params.append(str(object_id))
        if chunk_id:
            clauses.append("chunk_id = ?")
            params.append(str(chunk_id))
        if entity_type:
            clauses.append("entity_type = ?")
            params.append(entity_type)
        sql = "select payload from entity_mentions"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by object_id, chunk_index, chunk_char_start"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            EntityMention.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_source_version(self, record: SourceVersionRecord) -> SourceVersionRecord:
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into source_versions (
              source_key, source_version, materialized_at, source_url, artifact_id, payload
            )
            values (?, ?, ?, ?, ?, ?)
            on conflict(source_key) do update set
              source_version = excluded.source_version,
              materialized_at = excluded.materialized_at,
              source_url = excluded.source_url,
              artifact_id = excluded.artifact_id,
              payload = excluded.payload,
              updated_at = current_timestamp
            """,
            (
                record.source_key,
                record.source_version,
                record.materialized_at.isoformat(),
                record.source_url,
                str(record.artifact_id) if record.artifact_id else None,
                json.dumps(payload, sort_keys=True),
            ),
        )
        row = self.conn.execute(
            "select payload from source_versions where source_key = ?",
            (record.source_key,),
        ).fetchone()
        self.conn.commit()
        return SourceVersionRecord.model_validate(json.loads(row["payload"]))

    def list_source_versions(
        self,
        *,
        source_key: str | None = None,
        limit: int | None = 100,
    ) -> list[SourceVersionRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if source_key:
            clauses.append("source_key = ?")
            params.append(source_key)
        sql = "select payload from source_versions"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by materialized_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            SourceVersionRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def create_primitive_call_event(self, event: PrimitiveCallEvent) -> PrimitiveCallEvent:
        payload = event.model_dump(mode="json")
        self.conn.execute(
            """
            insert into primitive_call_events (
              event_id, primitive_name, status, request_hash, result_hash,
              agent_run_id, payload, created_at
            )
            values (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(event.event_id),
                event.primitive_name,
                event.status,
                event.request_hash,
                event.result_hash,
                str(event.agent_run_id) if event.agent_run_id else None,
                json.dumps(payload, sort_keys=True),
                event.created_at.isoformat(),
            ),
        )
        self.conn.commit()
        return event

    def list_primitive_call_events(
        self,
        *,
        primitive_name: str | None = None,
        agent_run_id: UUID | None = None,
        limit: int | None = 50,
    ) -> list[PrimitiveCallEvent]:
        clauses: list[str] = []
        params: list[object] = []
        if primitive_name:
            clauses.append("primitive_name = ?")
            params.append(primitive_name)
        if agent_run_id:
            clauses.append("agent_run_id = ?")
            params.append(str(agent_run_id))
        sql = "select payload from primitive_call_events"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            PrimitiveCallEvent.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_document_chunk(self, chunk: DocumentChunk) -> DocumentChunk:
        payload = chunk.model_dump(mode="json")
        self.conn.execute(
            """
            insert into document_chunks (
              chunk_id, object_id, chunk_index, section_label, text_content,
              content_hash, token_count, char_start, char_end, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(object_id, chunk_index) do update set
              section_label = excluded.section_label,
              text_content = excluded.text_content,
              content_hash = excluded.content_hash,
              token_count = excluded.token_count,
              char_start = excluded.char_start,
              char_end = excluded.char_end,
              payload = excluded.payload,
              updated_at = current_timestamp
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
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        row = self.conn.execute(
            """
            select chunk_id, object_id, payload
            from document_chunks
            where object_id = ? and chunk_index = ?
            """,
            (str(chunk.research_object_id), chunk.chunk_index),
        ).fetchone()
        if row is None:
            return chunk
        return document_chunk_from_payload(
            json.loads(row["payload"]),
            chunk_id=row["chunk_id"],
            object_id=row["object_id"],
        )

    def replace_document_chunks(self, object_id: UUID, chunks: list[DocumentChunk]) -> list[DocumentChunk]:
        """Replace all chunks for an object and clear derived chunk-level rows."""

        object_id_text = str(object_id)
        self.conn.execute("delete from entity_mentions where object_id = ?", (object_id_text,))
        self.conn.execute("delete from text_embeddings where object_id = ?", (object_id_text,))
        self.conn.execute("delete from document_chunks where object_id = ?", (object_id_text,))
        self.conn.commit()
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
            clauses.append("dc.object_id = ?")
            params.append(str(object_id))
        if source_key:
            clauses.append("ro.source_key = ?")
            params.append(source_key)
        if object_type:
            clauses.append("ro.object_type = ?")
            params.append(object_type)
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by dc.object_id, dc.chunk_index"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            document_chunk_from_payload(
                json.loads(row["payload"]),
                chunk_id=row["chunk_id"],
                object_id=row["object_id"],
            )
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def list_document_chunks_for_fetch_runs(
        self,
        fetch_run_ids: list[UUID],
        limit: int | None = None,
    ) -> list[DocumentChunk]:
        if not fetch_run_ids:
            return []
        placeholders = ", ".join("?" for _ in fetch_run_ids)
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
            sql += " limit ?"
            params.append(limit)
        return [
            document_chunk_from_payload(
                json.loads(row["payload"]),
                chunk_id=row["chunk_id"],
                object_id=row["object_id"],
            )
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def search_research_chunks(self, request: ResearchChunkSearchRequest) -> list[ResearchChunkSearchResult]:
        terms = keyword_terms(request.query)
        if not terms:
            return []

        params: list[object] = []
        clauses: list[str] = []
        if request.research_object_id:
            clauses.append("dc.object_id = ?")
            params.append(str(request.research_object_id))
        if request.source_key:
            clauses.append("ro.source_key = ?")
            params.append(request.source_key)
        if request.object_type:
            clauses.append("ro.object_type = ?")
            params.append(str(request.object_type))

        term_clauses: list[str] = []
        for term in terms:
            like_term = f"%{term}%"
            term_clauses.append(
                """
                (
                  lower(dc.text_content) like ?
                  or lower(coalesce(dc.section_label, '')) like ?
                  or lower(coalesce(ro.title, '')) like ?
                  or lower(coalesce(ro.abstract, '')) like ?
                )
                """
            )
            params.extend([like_term, like_term, like_term, like_term])
        clauses.append("(" + " or ".join(term_clauses) + ")")

        fetch_limit = min(max(request.limit * 200, 1000), 5000)
        sql = f"""
            select
              dc.chunk_id as chunk_id,
              dc.object_id as chunk_object_id,
              dc.payload as chunk_payload,
              ro.payload as object_payload
            from document_chunks dc
            join research_objects ro on ro.object_id = dc.object_id
            where {' and '.join(clauses)}
            order by dc.object_id, dc.chunk_index
            limit ?
        """
        rows = self.conn.execute(sql, [*params, fetch_limit]).fetchall()
        results: list[ResearchChunkSearchResult] = []
        for row in rows:
            chunk = document_chunk_from_payload(
                json.loads(row["chunk_payload"]),
                chunk_id=row["chunk_id"],
                object_id=row["chunk_object_id"],
            )
            obj = ResearchObject.model_validate(json.loads(row["object_payload"]))
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
        existing = self.conn.execute(
            """
            select embedding_id
            from text_embeddings
            where chunk_id = ? and embedding_model = ?
            """,
            (str(embedding.chunk_id), embedding.embedding_model),
        ).fetchone()
        embedding_id = UUID(existing["embedding_id"]) if existing else embedding.embedding_id
        record = embedding.model_copy(update={"embedding_id": embedding_id})
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into text_embeddings (
              embedding_id, chunk_id, object_id, chunk_index, source_key,
              object_type, embedding_model, embedding_dimensions, content_hash,
              vector_json, payload, embedded_at
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
              updated_at = current_timestamp
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
                json.dumps(record.embedding, separators=(",", ":"), ensure_ascii=True),
                json.dumps(payload, sort_keys=True),
                record.embedded_at.isoformat(),
            ),
        )
        row = self.conn.execute(
            "select payload from text_embeddings where embedding_id = ?",
            (str(record.embedding_id),),
        ).fetchone()
        self.conn.commit()
        return TextEmbedding.model_validate(json.loads(row["payload"]))

    def get_text_embedding(self, embedding_id: UUID) -> TextEmbedding | None:
        row = self.conn.execute(
            "select payload from text_embeddings where embedding_id = ?",
            (str(embedding_id),),
        ).fetchone()
        if row is None:
            return None
        return TextEmbedding.model_validate(json.loads(row["payload"]))

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
            clauses.append("embedding_model = ?")
            params.append(embedding_model)
        if source_key:
            clauses.append("source_key = ?")
            params.append(source_key)
        if research_object_id:
            clauses.append("object_id = ?")
            params.append(str(research_object_id))
        if chunk_id:
            clauses.append("chunk_id = ?")
            params.append(str(chunk_id))
        if object_type:
            clauses.append("object_type = ?")
            params.append(object_type)
        sql = "select payload from text_embeddings"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by source_key, object_id, chunk_index, embedding_model"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            TextEmbedding.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

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
            base_clauses.append("ro.source_key = ?")
            base_params.append(source_key)
        if object_type:
            base_clauses.append("ro.object_type = ?")
            base_params.append(object_type)
        where = " where " + " and ".join(base_clauses) if base_clauses else ""
        total_chunks = self.conn.execute(
            f"""
            select count(distinct dc.chunk_id) as count
            from document_chunks dc
            join research_objects ro on ro.object_id = dc.object_id
            {where}
            """,
            base_params,
        ).fetchone()["count"]

        embedded_clauses = list(base_clauses)
        embedded_params = list(base_params)
        if embedding_model:
            embedded_clauses.append("te.embedding_model = ?")
            embedded_params.append(embedding_model)
        embedded_where = " where " + " and ".join(embedded_clauses) if embedded_clauses else ""
        embedded_chunks = self.conn.execute(
            f"""
            select count(distinct dc.chunk_id) as count
            from document_chunks dc
            join research_objects ro on ro.object_id = dc.object_id
            join text_embeddings te on te.chunk_id = dc.chunk_id
            {embedded_where}
            """,
            embedded_params,
        ).fetchone()["count"]

        model_rows = self.conn.execute(
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
        ).fetchall()
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
            embedding_models={row["embedding_model"]: row["count"] for row in model_rows},
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
        row = self.conn.execute(
            f"""
            select count(*) as count
            from text_embeddings te
            left join document_chunks dc on dc.chunk_id = te.chunk_id
            where {' and '.join(clauses)}
            """,
            params,
        ).fetchone()
        return int(row["count"])

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
        cursor = self.conn.execute(
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
        self.conn.commit()
        return cursor.rowcount if cursor.rowcount >= 0 else 0

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
            clauses.append("te.embedding_model = ?")
            params.append(embedding_model)
        if source_key:
            clauses.append("te.source_key = ?")
            params.append(source_key)
        if object_type:
            clauses.append("te.object_type = ?")
            params.append(object_type)
        return clauses, params

    def coverage_summary(self) -> dict[str, Any]:
        source_count = self.conn.execute("select count(*) as count from ingestion_sources").fetchone()["count"]
        query_count = self.conn.execute("select count(*) as count from source_queries").fetchone()["count"]
        active_query_count = self.conn.execute(
            "select count(*) as count from source_queries where active = 1"
        ).fetchone()["count"]
        raw_count = self.conn.execute("select count(*) as count from raw_source_records").fetchone()["count"]
        object_count = self.conn.execute("select count(*) as count from research_objects").fetchone()["count"]
        chunk_count = self.conn.execute("select count(*) as count from document_chunks").fetchone()["count"]
        embedding_count = self.conn.execute("select count(*) as count from text_embeddings").fetchone()["count"]
        claims_count = self.conn.execute("select count(*) as count from claims").fetchone()["count"]
        entity_count = self.conn.execute("select count(*) as count from resolved_entities").fetchone()["count"]
        entity_alias_count = self.conn.execute("select count(*) as count from entity_aliases").fetchone()["count"]
        entity_mention_count = self.conn.execute("select count(*) as count from entity_mentions").fetchone()["count"]
        scrape_review_count = self.conn.execute("select count(*) as count from scrape_review_records").fetchone()["count"]
        source_followup_count = self.conn.execute("select count(*) as count from source_followup_queue").fetchone()["count"]
        scrape_profile_review_count = self.conn.execute(
            "select count(*) as count from scrape_source_profile_reviews"
        ).fetchone()["count"]
        claim_curation = self.claim_curation_summary()
        by_source = [
            dict(row)
            for row in self.conn.execute(
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
            ).fetchall()
        ]
        return {
            "storage_backend": "sqlite",
            "db_path": str(self.db_path),
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
            "claim_curation": claim_curation,
            "by_source": by_source,
        }

    def search_claims(self, request: ClaimSearchRequest) -> list[ClaimSearchResult]:
        clauses = ["confidence >= ?"]
        params: list[object] = [request.min_confidence]

        if request.species:
            clauses.append("lower(coalesce(species, '')) = lower(?)")
            params.append(request.species)

        if request.claim_types:
            placeholders = ",".join("?" for _ in request.claim_types)
            clauses.append(f"claim_type in ({placeholders})")
            params.extend(str(claim_type) for claim_type in request.claim_types)

        if request.evidence_levels:
            placeholders = ",".join("?" for _ in request.evidence_levels)
            clauses.append(f"evidence_level in ({placeholders})")
            params.extend(str(level) for level in request.evidence_levels)

        if request.query:
            clauses.append("lower(statement) like lower(?)")
            params.append(f"%{request.query}%")

        sql = f"""
            select payload
            from claims
            where {' and '.join(clauses)}
            order by confidence desc, updated_at desc
        """
        rows = self.conn.execute(sql, params).fetchall()
        results = [ClaimSearchResult.model_validate(json.loads(row["payload"])) for row in rows]
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
        clauses = ["c.confidence >= ?"]
        params: list[object] = [min_confidence]
        join = ""

        if source_key:
            join = " left join research_objects ro on ro.object_id = c.source_object_id"
            clauses.append("ro.source_key = ?")
            params.append(source_key)

        if query:
            clauses.append("lower(c.statement) like lower(?)")
            params.append(f"%{query}%")

        sql = f"""
            select c.payload
            from claims c
            {join}
            where {' and '.join(clauses)}
            order by c.updated_at asc, c.claim_id asc
        """
        claims = [ClaimSearchResult.model_validate(json.loads(row["payload"])) for row in self.conn.execute(sql, params)]
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

        if limit is None:
            return filtered
        return filtered[:limit]

    def claim_curation_summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        rows = self.conn.execute("select payload from claims").fetchall()
        for row in rows:
            claim = ClaimSearchResult.model_validate(json.loads(row["payload"]))
            status = claim.metadata.get("curation_status")
            if not status:
                status = "seed" if claim.metadata.get("seed") else "uncurated"
            counts[str(status)] = counts.get(str(status), 0) + 1
        return dict(sorted(counts.items()))

    def source_runtime_summary(self, source_key: str, *, sample_limit: int = 5) -> dict[str, Any]:
        """Return source-scoped counts for orchestration QA."""

        raw_records = self.conn.execute(
            "select count(*) as count from raw_source_records where source_key = ?",
            (source_key,),
        ).fetchone()["count"]
        research_objects = self.conn.execute(
            "select count(*) as count from research_objects where source_key = ?",
            (source_key,),
        ).fetchone()["count"]
        document_chunks = self.conn.execute(
            """
            select count(*) as count
            from document_chunks dc
            join research_objects ro on ro.object_id = dc.object_id
            where ro.source_key = ?
            """,
            (source_key,),
        ).fetchone()["count"]
        entity_mentions = self.conn.execute(
            "select count(*) as count from entity_mentions where source_key = ?",
            (source_key,),
        ).fetchone()["count"]
        claim_status = {
            row["status"]: row["count"]
            for row in self.conn.execute(
                """
                select coalesce(json_extract(c.payload, '$.metadata.curation_status'), 'uncurated') as status,
                       count(*) as count
                from claims c
                join research_objects ro on ro.object_id = c.source_object_id
                where ro.source_key = ?
                group by status
                order by status
                """,
                (source_key,),
            ).fetchall()
        }
        claim_types = {
            row["claim_type"]: row["count"]
            for row in self.conn.execute(
                """
                select c.claim_type, count(*) as count
                from claims c
                join research_objects ro on ro.object_id = c.source_object_id
                where ro.source_key = ?
                group by c.claim_type
                order by c.claim_type
                """,
                (source_key,),
            ).fetchall()
        }
        sample_claims = [
            {
                "claim_id": row["claim_id"],
                "statement": row["statement"],
                "confidence": row["confidence"],
                "curation_status": row["curation_status"] or "uncurated",
            }
            for row in self.conn.execute(
                """
                select c.claim_id,
                       c.statement,
                       c.confidence,
                       json_extract(c.payload, '$.metadata.curation_status') as curation_status
                from claims c
                join research_objects ro on ro.object_id = c.source_object_id
                where ro.source_key = ?
                order by c.confidence desc, c.updated_at desc
                limit ?
                """,
                (source_key, sample_limit),
            ).fetchall()
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
        row = self.conn.execute("select payload from claims where claim_id = ?", (str(claim_id),)).fetchone()
        if row is None:
            return None
        return ClaimSearchResult.model_validate(json.loads(row["payload"]))

    def get_candidate(self, request: CandidateDossierRequest) -> CandidateDossier | None:
        candidate_name = request.candidate_name or "unknown candidate"
        claim_request = ClaimSearchRequest(compounds=[candidate_name], limit=50)
        claims = self.search_claims(claim_request) if request.include_claims else []
        return CandidateDossier(
            candidate_id=request.candidate_id or uuid4(),
            name=candidate_name,
            status="investigating",
            summary="Local-first dossier backed by SQLite. Candidate tables arrive with the scoring layer.",
            evidence_claims=claims,
            validation_runs=[],
            artifacts=[],
            risk_flags=[],
            metadata={"storage_backend": "sqlite", "db_path": str(self.db_path)},
        )

    def commit_hypothesis(self, request: CommitHypothesisRequest) -> HypothesisDraft:
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
        self.conn.execute(
            """
            insert into hypotheses (hypothesis_id, title, hypothesis, status, payload)
            values (?, ?, ?, ?, ?)
            on conflict(hypothesis_id) do update set
              title = excluded.title,
              hypothesis = excluded.hypothesis,
              status = excluded.status,
              payload = excluded.payload,
              updated_at = current_timestamp
            """,
            (
                str(committed.hypothesis_id),
                committed.title,
                committed.hypothesis,
                committed.status,
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
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
        row = self.conn.execute("select payload from async_runs where run_id = ?", (str(run_id),)).fetchone()
        if row is None:
            return None
        return AsyncRunHandle.model_validate(json.loads(row["payload"]))

    def get_artifact(self, artifact_id: UUID) -> ArtifactHandle | None:
        row = self.conn.execute("select payload from artifacts where artifact_id = ?", (str(artifact_id),)).fetchone()
        if row is None:
            return None
        return ArtifactHandle.model_validate(json.loads(row["payload"]))

    def create_agent_run(self, record: AgentRunRecord) -> AgentRunRecord:
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into agent_runs (
              agent_run_id, agent_name, agent_version, model_profile, status,
              source_key, partition_date, dagster_run_id, started_at,
              completed_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
              updated_at = current_timestamp
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
                record.started_at.isoformat(),
                record.completed_at.isoformat() if record.completed_at else None,
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
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
        row = self.conn.execute(
            "select payload from agent_runs where agent_run_id = ?",
            (str(agent_run_id),),
        ).fetchone()
        if row is None:
            return None
        return AgentRunRecord.model_validate(json.loads(row["payload"]))

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
            clauses.append("agent_name = ?")
            params.append(agent_name)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if source_key:
            clauses.append("source_key = ?")
            params.append(source_key)
        sql = "select payload from agent_runs"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc"
        sql += " limit ?"
        params.append(limit)
        return [
            AgentRunRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def create_agent_run_review(self, record: AgentRunReviewRecord) -> AgentRunReviewRecord:
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into agent_run_reviews (
              review_id, agent_run_id, reviewer, verdict, created_at, payload
            )
            values (?, ?, ?, ?, ?, ?)
            on conflict(review_id) do update set
              agent_run_id = excluded.agent_run_id,
              reviewer = excluded.reviewer,
              verdict = excluded.verdict,
              created_at = excluded.created_at,
              payload = excluded.payload,
              updated_at = current_timestamp
            """,
            (
                str(record.review_id),
                str(record.agent_run_id),
                record.reviewer,
                record.verdict,
                record.created_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return record

    def get_agent_run_review(self, review_id: UUID) -> AgentRunReviewRecord | None:
        row = self.conn.execute(
            "select payload from agent_run_reviews where review_id = ?",
            (str(review_id),),
        ).fetchone()
        if row is None:
            return None
        return AgentRunReviewRecord.model_validate(json.loads(row["payload"]))

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
            clauses.append("agent_run_id = ?")
            params.append(str(agent_run_id))
        if verdict:
            clauses.append("verdict = ?")
            params.append(verdict)
        if reviewer:
            clauses.append("reviewer = ?")
            params.append(reviewer)
        sql = "select payload from agent_run_reviews"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc limit ?"
        params.append(limit)
        return [
            AgentRunReviewRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def create_reward_event(self, record: RewardEventRecord) -> RewardEventRecord:
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into reward_events (
              reward_event_id, identity_key, event_source, score, agent_run_id,
              source_review_id, agent_name, model_profile, source_key, created_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(reward_event_id) do update set
              identity_key = excluded.identity_key,
              event_source = excluded.event_source,
              score = excluded.score,
              agent_run_id = excluded.agent_run_id,
              source_review_id = excluded.source_review_id,
              agent_name = excluded.agent_name,
              model_profile = excluded.model_profile,
              source_key = excluded.source_key,
              created_at = excluded.created_at,
              payload = excluded.payload,
              updated_at = current_timestamp
            """,
            (
                str(record.reward_event_id),
                record.identity_key,
                record.event_source,
                record.score,
                str(record.agent_run_id) if record.agent_run_id else None,
                str(record.source_review_id) if record.source_review_id else None,
                record.agent_name,
                record.model_profile,
                record.source_key,
                record.created_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return record

    def get_reward_event(self, reward_event_id: UUID) -> RewardEventRecord | None:
        row = self.conn.execute(
            "select payload from reward_events where reward_event_id = ?",
            (str(reward_event_id),),
        ).fetchone()
        if row is None:
            return None
        return RewardEventRecord.model_validate(json.loads(row["payload"]))

    def list_reward_events(
        self,
        *,
        agent_run_id: UUID | None = None,
        source_review_id: UUID | None = None,
        agent_name: str | None = None,
        source_key: str | None = None,
        event_source: str | None = None,
        limit: int = 50,
    ) -> list[RewardEventRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if agent_run_id:
            clauses.append("agent_run_id = ?")
            params.append(str(agent_run_id))
        if source_review_id:
            clauses.append("source_review_id = ?")
            params.append(str(source_review_id))
        if agent_name:
            clauses.append("agent_name = ?")
            params.append(agent_name)
        if source_key:
            clauses.append("source_key = ?")
            params.append(source_key)
        if event_source:
            clauses.append("event_source = ?")
            params.append(event_source)
        sql = "select payload from reward_events"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc limit ?"
        params.append(limit)
        return [
            RewardEventRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_research_brief(self, record: ResearchBriefRecord) -> ResearchBriefRecord:
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into research_briefs (
              brief_id, agent_run_id, topic, disease_scope, source_key,
              brief_style, model_profile, review_mode, status, citation_count,
              finding_count, hypothesis_count, research_lead_count, error_count,
              created_at, updated_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return record

    def get_research_brief(self, brief_id: UUID) -> ResearchBriefRecord | None:
        row = self.conn.execute(
            "select payload from research_briefs where brief_id = ?",
            (str(brief_id),),
        ).fetchone()
        if row is None:
            return None
        return ResearchBriefRecord.model_validate(json.loads(row["payload"]))

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
            clauses.append("status = ?")
            params.append(status)
        if source_key:
            clauses.append("source_key = ?")
            params.append(source_key)
        if topic_query:
            clauses.append("(lower(topic) like ? or lower(disease_scope) like ?)")
            normalized = f"%{topic_query.lower()}%"
            params.extend([normalized, normalized])
        sql = "select payload from research_briefs"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            ResearchBriefRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_research_brief_evaluation(
        self,
        record: ResearchBriefEvaluationRecord,
    ) -> ResearchBriefEvaluationRecord:
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into research_brief_evaluations (
              evaluation_id, brief_id, agent_run_id, topic, source_key,
              model_profile, overall_score, passes_quality_bar, readiness,
              created_at, updated_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                1 if record.passes_quality_bar else 0,
                record.readiness,
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return record

    def get_research_brief_evaluation(self, evaluation_id: UUID) -> ResearchBriefEvaluationRecord | None:
        row = self.conn.execute(
            "select payload from research_brief_evaluations where evaluation_id = ?",
            (str(evaluation_id),),
        ).fetchone()
        if row is None:
            return None
        return ResearchBriefEvaluationRecord.model_validate(json.loads(row["payload"]))

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
            clauses.append("brief_id = ?")
            params.append(str(brief_id))
        if readiness:
            clauses.append("readiness = ?")
            params.append(readiness)
        if passes_quality_bar is not None:
            clauses.append("passes_quality_bar = ?")
            params.append(1 if passes_quality_bar else 0)
        sql = "select payload from research_brief_evaluations"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            ResearchBriefEvaluationRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_therapy_idea(self, record: TherapyIdeaRecord) -> TherapyIdeaRecord:
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into therapy_ideas (
              therapy_idea_id, committee_run_id, agent_run_id, source_program_id,
              source_brief_id, source_evaluation_id, topic, source_key, status,
              promotion_state, score, created_at, updated_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return record

    def get_therapy_idea(self, therapy_idea_id: UUID) -> TherapyIdeaRecord | None:
        row = self.conn.execute(
            "select payload from therapy_ideas where therapy_idea_id = ?",
            (str(therapy_idea_id),),
        ).fetchone()
        if row is None:
            return None
        return TherapyIdeaRecord.model_validate(json.loads(row["payload"]))

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
            clauses.append("status = ?")
            params.append(status)
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            clauses.append(f"status in ({placeholders})")
            params.extend(statuses)
        if source_program_id:
            clauses.append("source_program_id = ?")
            params.append(str(source_program_id))
        if source_brief_id:
            clauses.append("source_brief_id = ?")
            params.append(str(source_brief_id))
        if source_evaluation_id:
            clauses.append("source_evaluation_id = ?")
            params.append(str(source_evaluation_id))
        if committee_run_id:
            clauses.append("committee_run_id = ?")
            params.append(str(committee_run_id))
        if topic_query:
            clauses.append("lower(topic) like ?")
            params.append(f"%{topic_query.lower()}%")
        sql = "select payload from therapy_ideas"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by score desc, updated_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            TherapyIdeaRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

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
        self.conn.execute(
            """
            insert into research_programs (
              program_id, agent_run_id, title, thesis_area, status,
              gate_decision, confidence_score, evidence_loop_count,
              max_evidence_loops, source_query, created_at, updated_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return record

    def get_research_program(self, program_id: UUID) -> ResearchProgramRecord | None:
        row = self.conn.execute(
            "select payload from research_programs where program_id = ?",
            (str(program_id),),
        ).fetchone()
        if row is None:
            return None
        return ResearchProgramRecord.model_validate(json.loads(row["payload"]))

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
            clauses.append("status = ?")
            params.append(status)
        if gate_decision:
            clauses.append("gate_decision = ?")
            params.append(gate_decision)
        if thesis_query:
            clauses.append("(lower(title) like ? or lower(source_query) like ? or lower(payload) like ?)")
            like_query = f"%{thesis_query.lower()}%"
            params.extend([like_query, like_query, like_query])
        sql = "select payload from research_programs"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by confidence_score desc, updated_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            ResearchProgramRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

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
        self.conn.execute(
            """
            insert into validation_decisions (
              decision_record_id, decision_id, packet_id, candidate_id, source_type,
              source_id, therapy_idea_id, title, outcome, confidence,
              validation_ready, created_at, updated_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                int(record.validation_ready),
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return record

    def get_validation_decision(self, decision_id: str) -> ValidationDecisionRecord | None:
        row = self.conn.execute(
            "select payload from validation_decisions where decision_id = ?",
            (decision_id,),
        ).fetchone()
        if row is None:
            return None
        return ValidationDecisionRecord.model_validate(json.loads(row["payload"]))

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
            clauses.append("outcome = ?")
            params.append(outcome)
        if therapy_idea_id:
            clauses.append("therapy_idea_id = ?")
            params.append(str(therapy_idea_id))
        if candidate_id:
            clauses.append("candidate_id = ?")
            params.append(candidate_id)
        sql = "select payload from validation_decisions"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by updated_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            ValidationDecisionRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_public_candidate(self, record: PublicCandidateRecord) -> PublicCandidateRecord:
        existing = self.get_public_candidate(record.candidate_id)
        if existing:
            record = record.model_copy(
                update={
                    "created_at": existing.created_at,
                    "updated_at": datetime.now(UTC),
                    "metadata": {**existing.metadata, **record.metadata},
                }
            )
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into public_candidates (
              candidate_id, display_id, candidate_kind, public_status, visibility,
              therapy_idea_id, latest_snapshot_id, content_hash, priority_score,
              title, created_at, updated_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(candidate_id) do update set
              display_id = excluded.display_id,
              candidate_kind = excluded.candidate_kind,
              public_status = excluded.public_status,
              visibility = excluded.visibility,
              therapy_idea_id = excluded.therapy_idea_id,
              latest_snapshot_id = excluded.latest_snapshot_id,
              content_hash = excluded.content_hash,
              priority_score = excluded.priority_score,
              title = excluded.title,
              updated_at = excluded.updated_at,
              payload = excluded.payload
            """,
            (
                record.candidate_id,
                record.display_id,
                record.candidate_kind,
                record.public_status,
                record.visibility,
                str(record.therapy_idea_id) if record.therapy_idea_id else None,
                str(record.latest_snapshot_id) if record.latest_snapshot_id else None,
                record.content_hash,
                record.priority_score,
                record.title,
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return record

    def get_public_candidate(self, candidate_id: str) -> PublicCandidateRecord | None:
        row = self.conn.execute(
            "select payload from public_candidates where candidate_id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            return None
        return PublicCandidateRecord.model_validate(json.loads(row["payload"]))

    def list_public_candidates(self, request: PublicCandidateLibraryRequest | None = None) -> list[PublicCandidateRecord]:
        request = request or PublicCandidateLibraryRequest(limit=50)
        clauses: list[str] = []
        params: list[object] = []
        if request.candidate_id:
            clauses.append("candidate_id = ?")
            params.append(request.candidate_id)
        if request.therapy_idea_id:
            clauses.append("therapy_idea_id = ?")
            params.append(str(request.therapy_idea_id))
        if request.public_status:
            clauses.append("public_status = ?")
            params.append(request.public_status)
        if request.visibility:
            clauses.append("visibility = ?")
            params.append(request.visibility)
        if request.candidate_kind:
            clauses.append("candidate_kind = ?")
            params.append(request.candidate_kind)
        if request.query:
            clauses.append("(lower(title) like ? or lower(payload) like ?)")
            query = f"%{request.query.lower()}%"
            params.extend([query, query])
        sql = "select payload from public_candidates"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by priority_score desc, updated_at desc"
        sql += " limit ?"
        params.append(request.limit)
        return [
            PublicCandidateRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_public_candidate_snapshot(self, record: PublicCandidateSnapshot) -> PublicCandidateSnapshot:
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into public_candidate_snapshots (
              snapshot_id, candidate_id, snapshot_version, content_hash, created_at, payload
            )
            values (?, ?, ?, ?, ?, ?)
            on conflict(snapshot_id) do update set
              candidate_id = excluded.candidate_id,
              snapshot_version = excluded.snapshot_version,
              content_hash = excluded.content_hash,
              payload = excluded.payload
            """,
            (
                str(record.snapshot_id),
                record.candidate_id,
                record.snapshot_version,
                record.content_hash,
                record.created_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return record

    def get_public_candidate_snapshot(self, snapshot_id: UUID) -> PublicCandidateSnapshot | None:
        row = self.conn.execute(
            "select payload from public_candidate_snapshots where snapshot_id = ?",
            (str(snapshot_id),),
        ).fetchone()
        if row is None:
            return None
        return PublicCandidateSnapshot.model_validate(json.loads(row["payload"]))

    def list_public_candidate_snapshots(
        self,
        *,
        candidate_id: str | None = None,
        limit: int | None = 50,
    ) -> list[PublicCandidateSnapshot]:
        clauses: list[str] = []
        params: list[object] = []
        if candidate_id:
            clauses.append("candidate_id = ?")
            params.append(candidate_id)
        sql = "select payload from public_candidate_snapshots"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by snapshot_version desc, created_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            PublicCandidateSnapshot.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def append_public_candidate_decision_event(
        self,
        record: PublicCandidateDecisionEvent,
    ) -> PublicCandidateDecisionEvent:
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into public_candidate_decision_events (
              event_id, candidate_id, occurred_at, action, actor, new_status,
              related_snapshot_id, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(event_id) do update set
              payload = excluded.payload
            """,
            (
                str(record.event_id),
                record.candidate_id,
                record.occurred_at.isoformat(),
                record.action,
                record.actor,
                record.new_status,
                str(record.related_snapshot_id) if record.related_snapshot_id else None,
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return record

    def list_public_candidate_decision_events(
        self,
        *,
        candidate_id: str | None = None,
        limit: int | None = 100,
    ) -> list[PublicCandidateDecisionEvent]:
        clauses: list[str] = []
        params: list[object] = []
        if candidate_id:
            clauses.append("candidate_id = ?")
            params.append(candidate_id)
        sql = "select payload from public_candidate_decision_events"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by occurred_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            PublicCandidateDecisionEvent.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_validation_plan(self, record: ValidationPlanRecord) -> ValidationPlanRecord:
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into validation_plans (
              plan_id, agent_run_id, brief_id, evaluation_id, topic, source_key,
              model_profile, status, readiness, task_count, hypothesis_count,
              created_at, updated_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return record

    def get_validation_plan(self, plan_id: UUID) -> ValidationPlanRecord | None:
        row = self.conn.execute(
            "select payload from validation_plans where plan_id = ?",
            (str(plan_id),),
        ).fetchone()
        if row is None:
            return None
        return ValidationPlanRecord.model_validate(json.loads(row["payload"]))

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
            clauses.append("brief_id = ?")
            params.append(str(brief_id))
        if evaluation_id:
            clauses.append("evaluation_id = ?")
            params.append(str(evaluation_id))
        if status:
            clauses.append("status = ?")
            params.append(status)
        if readiness:
            clauses.append("readiness = ?")
            params.append(readiness)
        sql = "select payload from validation_plans"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            ValidationPlanRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_validation_request_queue_item(
        self,
        item: ValidationRequestQueueItem,
    ) -> ValidationRequestQueueItem:
        existing = self.conn.execute(
            "select payload from validation_request_queue where identity_key = ?",
            (item.identity_key,),
        ).fetchone()
        if existing is not None:
            existing_item = ValidationRequestQueueItem.model_validate(json.loads(existing["payload"]))
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
        self.conn.execute(
            """
            insert into validation_request_queue (
              queue_item_id, identity_key, status, priority, plan_id, task_id,
              brief_id, evaluation_id, source_key, task_type, title,
              validation_type, target_name, candidate_name, last_run_id,
              attempts, last_error, approved_by, created_at, updated_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                item.created_at.isoformat(),
                item.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        row = self.conn.execute(
            "select payload from validation_request_queue where identity_key = ?",
            (item.identity_key,),
        ).fetchone()
        return ValidationRequestQueueItem.model_validate(json.loads(row["payload"]))

    def get_validation_request_queue_item(
        self,
        queue_item_id: UUID,
    ) -> ValidationRequestQueueItem | None:
        row = self.conn.execute(
            "select payload from validation_request_queue where queue_item_id = ?",
            (str(queue_item_id),),
        ).fetchone()
        if row is None:
            return None
        return ValidationRequestQueueItem.model_validate(json.loads(row["payload"]))

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
            clauses.append("plan_id = ?")
            params.append(str(plan_id))
        if status:
            clauses.append("status = ?")
            params.append(status)
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            clauses.append(f"status in ({placeholders})")
            params.extend(statuses)
        if source_key:
            clauses.append("source_key = ?")
            params.append(source_key)
        if task_type:
            clauses.append("task_type = ?")
            params.append(task_type)
        if topic_query:
            clauses.append("(lower(title) like ? or lower(payload) like ?)")
            normalized = f"%{topic_query.lower()}%"
            params.extend([normalized, normalized])
        sql = "select payload from validation_request_queue"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by priority asc, created_at asc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            ValidationRequestQueueItem.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
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
        self.conn.execute(
            """
            update validation_request_queue
            set status = ?,
                priority = ?,
                last_run_id = ?,
                attempts = ?,
                last_error = ?,
                approved_by = ?,
                validation_type = ?,
                target_name = ?,
                candidate_name = ?,
                updated_at = ?,
                payload = ?
            where queue_item_id = ?
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
                updated.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
                str(queue_item_id),
            ),
        )
        self.conn.commit()
        return updated

    def upsert_compute_job(self, record: ComputeJobRecord) -> ComputeJobRecord:
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into compute_jobs (
              compute_job_id, queue_item_id, status, runner_kind, compute_profile,
              validation_type, title, runpod_job_id, dagster_run_id, created_at,
              updated_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return record

    def get_compute_job(self, compute_job_id: UUID) -> ComputeJobRecord | None:
        row = self.conn.execute(
            "select payload from compute_jobs where compute_job_id = ?",
            (str(compute_job_id),),
        ).fetchone()
        if row is None:
            return None
        return ComputeJobRecord.model_validate(json.loads(row["payload"]))

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
            clauses.append("status = ?")
            params.append(status)
        if runner_kind:
            clauses.append("runner_kind = ?")
            params.append(runner_kind)
        if queue_item_id:
            clauses.append("queue_item_id = ?")
            params.append(str(queue_item_id))
        sql = "select payload from compute_jobs"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by updated_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            ComputeJobRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

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

    def upsert_md_expert_review_packet(
        self,
        record: MDExpertReviewPacketRecord,
    ) -> MDExpertReviewPacketRecord:
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into md_expert_review_packets (
              packet_id, packet_hash, status, compute_job_id, queue_item_id,
              endpoint_id, created_at, updated_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return record

    def get_md_expert_review_packet(self, packet_id: UUID) -> MDExpertReviewPacketRecord | None:
        row = self.conn.execute(
            "select payload from md_expert_review_packets where packet_id = ?",
            (str(packet_id),),
        ).fetchone()
        if row is None:
            return None
        return MDExpertReviewPacketRecord.model_validate(json.loads(row["payload"]))

    def get_md_expert_review_packet_by_hash(self, packet_hash: str) -> MDExpertReviewPacketRecord | None:
        row = self.conn.execute(
            "select payload from md_expert_review_packets where packet_hash = ? order by updated_at desc limit 1",
            (packet_hash,),
        ).fetchone()
        if row is None:
            return None
        return MDExpertReviewPacketRecord.model_validate(json.loads(row["payload"]))

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
            clauses.append("packet_hash = ?")
            params.append(packet_hash)
        if status:
            clauses.append("status = ?")
            params.append(status)
        sql = "select payload from md_expert_review_packets"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by updated_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            MDExpertReviewPacketRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_md_expert_approval(self, record: MDExpertApprovalRecord) -> MDExpertApprovalRecord:
        payload = record.model_dump(mode="json")
        self.conn.execute(
            """
            insert into md_expert_approvals (
              approval_id, packet_id, packet_hash, decision, reviewer_name,
              reviewer_contact, reviewed_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(approval_id) do update set
              packet_id = excluded.packet_id,
              packet_hash = excluded.packet_hash,
              decision = excluded.decision,
              reviewer_name = excluded.reviewer_name,
              reviewer_contact = excluded.reviewer_contact,
              reviewed_at = excluded.reviewed_at,
              payload = excluded.payload,
              updated_at = current_timestamp
            """,
            (
                str(record.approval_id),
                str(record.packet_id),
                record.packet_hash,
                record.decision,
                record.reviewer_name,
                record.reviewer_contact,
                record.reviewed_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        packet = self.get_md_expert_review_packet(record.packet_id)
        if packet is not None:
            self.upsert_md_expert_review_packet(
                packet.model_copy(update={"status": record.decision, "updated_at": datetime.now(UTC)})
            )
        self.conn.commit()
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
            clauses.append("packet_hash = ?")
            params.append(packet_hash)
        if decision:
            clauses.append("decision = ?")
            params.append(decision)
        sql = "select payload from md_expert_approvals"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by reviewed_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            MDExpertApprovalRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_research_brief_queue_item(self, item: ResearchBriefQueueItem) -> ResearchBriefQueueItem:
        existing = self.conn.execute(
            "select payload from research_brief_queue where identity_key = ?",
            (item.identity_key,),
        ).fetchone()
        if existing is not None:
            existing_item = ResearchBriefQueueItem.model_validate(json.loads(existing["payload"]))
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
        self.conn.execute(
            """
            insert into research_brief_queue (
              queue_item_id, identity_key, status, priority, topic, disease_scope,
              source_key, brief_style, model_profile, review_mode, last_brief_id,
              last_agent_run_id, attempts, created_at, updated_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                item.created_at.isoformat(),
                item.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        row = self.conn.execute(
            "select payload from research_brief_queue where identity_key = ?",
            (item.identity_key,),
        ).fetchone()
        return ResearchBriefQueueItem.model_validate(json.loads(row["payload"]))

    def get_research_brief_queue_item(self, queue_item_id: UUID) -> ResearchBriefQueueItem | None:
        row = self.conn.execute(
            "select payload from research_brief_queue where queue_item_id = ?",
            (str(queue_item_id),),
        ).fetchone()
        if row is None:
            return None
        return ResearchBriefQueueItem.model_validate(json.loads(row["payload"]))

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
            clauses.append("status = ?")
            params.append(status)
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            clauses.append(f"status in ({placeholders})")
            params.extend(statuses)
        if source_key:
            clauses.append("source_key = ?")
            params.append(source_key)
        if topic_query:
            clauses.append("(lower(topic) like ? or lower(disease_scope) like ?)")
            normalized = f"%{topic_query.lower()}%"
            params.extend([normalized, normalized])
        sql = "select payload from research_brief_queue"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by priority asc, created_at asc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            ResearchBriefQueueItem.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

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
        self.conn.execute(
            """
            update research_brief_queue
            set status = ?,
                priority = ?,
                last_brief_id = ?,
                last_agent_run_id = ?,
                attempts = ?,
                updated_at = ?,
                payload = ?
            where queue_item_id = ?
            """,
            (
                updated.status,
                updated.priority,
                str(updated.last_brief_id) if updated.last_brief_id else None,
                str(updated.last_agent_run_id) if updated.last_agent_run_id else None,
                updated.attempts,
                updated.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
                str(queue_item_id),
            ),
        )
        self.conn.commit()
        return updated

    def upsert_artifact(self, artifact: ArtifactHandle) -> ArtifactHandle:
        payload = artifact.model_dump(mode="json")
        self.conn.execute(
            """
            insert into artifacts (artifact_id, artifact_type, uri, legal_status, mime_type, payload)
            values (?, ?, ?, ?, ?, ?)
            on conflict(artifact_id) do update set
              artifact_type = excluded.artifact_type,
              uri = excluded.uri,
              legal_status = excluded.legal_status,
              mime_type = excluded.mime_type,
              payload = excluded.payload,
              updated_at = current_timestamp
            """,
            (
                str(artifact.artifact_id),
                artifact.artifact_type,
                artifact.uri,
                artifact.legal_status,
                artifact.mime_type,
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
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
            clauses.append("artifact_type = ?")
            params.append(artifact_type)
        if source_key:
            clauses.append("json_extract(payload, '$.metadata.source_key') = ?")
            params.append(source_key)
        sql = "select payload from artifacts"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by updated_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            ArtifactHandle.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def upsert_scrape_review(self, record: ScrapeReviewRecord) -> ScrapeReviewRecord:
        existing = self.conn.execute(
            """
            select payload from scrape_review_records
            where source_key = ? and artifact_id = ? and source_record_id = ?
            """,
            (record.source_key, str(record.artifact_id), record.source_record_id),
        ).fetchone()
        if existing is not None:
            existing_record = ScrapeReviewRecord.model_validate(json.loads(existing["payload"]))
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
        self.conn.execute(
            """
            insert into scrape_review_records (
              review_id, source_key, artifact_id, source_record_id, title,
              canonical_url, parser_confidence, review_status, reviewer,
              parsed_at, reviewed_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(source_key, artifact_id, source_record_id) do update set
              title = excluded.title,
              canonical_url = excluded.canonical_url,
              parser_confidence = excluded.parser_confidence,
              review_status = excluded.review_status,
              reviewer = excluded.reviewer,
              parsed_at = excluded.parsed_at,
              reviewed_at = excluded.reviewed_at,
              payload = excluded.payload,
              updated_at = current_timestamp
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
                json.dumps(payload, sort_keys=True),
            ),
        )
        row = self.conn.execute(
            """
            select payload from scrape_review_records
            where source_key = ? and artifact_id = ? and source_record_id = ?
            """,
            (record.source_key, str(record.artifact_id), record.source_record_id),
        ).fetchone()
        self.conn.commit()
        return ScrapeReviewRecord.model_validate(json.loads(row["payload"]))

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
            clauses.append("source_key = ?")
            params.append(source_key)
        if review_status:
            clauses.append("review_status = ?")
            params.append(review_status)
        if review_ids:
            placeholders = ",".join("?" for _ in review_ids)
            clauses.append(f"review_id in ({placeholders})")
            params.extend(str(review_id) for review_id in review_ids)
        if artifact_ids:
            placeholders = ",".join("?" for _ in artifact_ids)
            clauses.append(f"artifact_id in ({placeholders})")
            params.extend(str(artifact_id) for artifact_id in artifact_ids)
        sql = "select payload from scrape_review_records"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by updated_at desc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            ScrapeReviewRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def update_scrape_review(
        self,
        review_id: UUID,
        *,
        review_status: str,
        reviewed_by: str,
        review_note: str | None = None,
    ) -> ScrapeReviewRecord | None:
        row = self.conn.execute(
            "select payload from scrape_review_records where review_id = ?",
            (str(review_id),),
        ).fetchone()
        if row is None:
            return None
        record = ScrapeReviewRecord.model_validate(json.loads(row["payload"]))
        updated = record.model_copy(
            update={
                "review_status": review_status,
                "reviewer": reviewed_by,
                "review_note": review_note,
                "reviewed_at": datetime.now(UTC),
            }
        )
        payload = updated.model_dump(mode="json")
        self.conn.execute(
            """
            update scrape_review_records
            set review_status = ?,
                reviewer = ?,
                reviewed_at = ?,
                payload = ?,
                updated_at = current_timestamp
            where review_id = ?
            """,
            (
                updated.review_status,
                updated.reviewer,
                updated.reviewed_at.isoformat() if updated.reviewed_at else None,
                json.dumps(payload, sort_keys=True),
                str(review_id),
            ),
        )
        self.conn.commit()
        return updated

    def upsert_scrape_profile_review(self, review: ScrapeSourceProfileReview) -> ScrapeSourceProfileReview:
        payload = review.model_dump(mode="json")
        self.conn.execute(
            """
            insert into scrape_source_profile_reviews (
              source_key, robots_policy, approved_for_fetch, reviewed_by,
              review_note, storage_policy, reviewed_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(source_key) do update set
              robots_policy = excluded.robots_policy,
              approved_for_fetch = excluded.approved_for_fetch,
              reviewed_by = excluded.reviewed_by,
              review_note = excluded.review_note,
              storage_policy = excluded.storage_policy,
              reviewed_at = excluded.reviewed_at,
              payload = excluded.payload,
              updated_at = current_timestamp
            """,
            (
                review.source_key,
                review.robots_policy,
                int(review.approved_for_fetch),
                review.reviewed_by,
                review.review_note,
                review.storage_policy,
                review.reviewed_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return review

    def get_scrape_profile_review(self, source_key: str) -> ScrapeSourceProfileReview | None:
        row = self.conn.execute(
            "select payload from scrape_source_profile_reviews where source_key = ?",
            (source_key,),
        ).fetchone()
        if row is None:
            return None
        return ScrapeSourceProfileReview.model_validate(json.loads(row["payload"]))

    def upsert_source_followup(self, item: SourceFollowupQueueItem) -> SourceFollowupQueueItem:
        existing = self.conn.execute(
            "select payload from source_followup_queue where identity_key = ?",
            (item.identity_key,),
        ).fetchone()
        if existing is not None:
            existing_item = SourceFollowupQueueItem.model_validate(json.loads(existing["payload"]))
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
        self.conn.execute(
            """
            insert into source_followup_queue (
              followup_id, identity_key, source_key, identifier_type, identifier,
              status, priority, attempts, origin_source_key, origin_review_id,
              origin_artifact_id, origin_agent_run_id, created_at, updated_at, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                item.created_at.isoformat(),
                item.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        row = self.conn.execute(
            "select payload from source_followup_queue where identity_key = ?",
            (item.identity_key,),
        ).fetchone()
        return SourceFollowupQueueItem.model_validate(json.loads(row["payload"]))

    def get_source_followup(self, followup_id: UUID) -> SourceFollowupQueueItem | None:
        row = self.conn.execute(
            "select payload from source_followup_queue where followup_id = ?",
            (str(followup_id),),
        ).fetchone()
        if row is None:
            return None
        return SourceFollowupQueueItem.model_validate(json.loads(row["payload"]))

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
            clauses.append("source_key = ?")
            params.append(source_key)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            clauses.append(f"status in ({placeholders})")
            params.extend(statuses)
        if identifier_type:
            clauses.append("identifier_type = ?")
            params.append(identifier_type)
        sql = "select payload from source_followup_queue"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by priority asc, created_at asc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            SourceFollowupQueueItem.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

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
        self.conn.execute(
            """
            update source_followup_queue
            set status = ?,
                attempts = ?,
                updated_at = ?,
                payload = ?
            where followup_id = ?
            """,
            (
                updated.status,
                updated.attempts,
                updated.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
                str(followup_id),
            ),
        )
        self.conn.commit()
        return updated

    def upsert_research_lead(self, lead: ResearchLeadRecord) -> ResearchLeadRecord:
        existing = self.conn.execute(
            "select payload from research_leads where identity_key = ?",
            (lead.identity_key,),
        ).fetchone()
        if existing is not None:
            existing_lead = ResearchLeadRecord.model_validate(json.loads(existing["payload"]))
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
        self.conn.execute(
            """
            insert into research_leads (
              lead_id, identity_key, lead_type, status, priority, source_key,
              origin_source_key, origin_record_id, origin_review_id,
              origin_artifact_id, origin_agent_run_id, created_at, updated_at,
              payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                lead.created_at.isoformat(),
                lead.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        row = self.conn.execute(
            "select payload from research_leads where identity_key = ?",
            (lead.identity_key,),
        ).fetchone()
        return ResearchLeadRecord.model_validate(json.loads(row["payload"]))

    def get_research_lead(self, lead_id: UUID) -> ResearchLeadRecord | None:
        row = self.conn.execute(
            "select payload from research_leads where lead_id = ?",
            (str(lead_id),),
        ).fetchone()
        if row is None:
            return None
        return ResearchLeadRecord.model_validate(json.loads(row["payload"]))

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
            clauses.append("status = ?")
            params.append(status)
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            clauses.append(f"status in ({placeholders})")
            params.extend(statuses)
        if lead_type:
            clauses.append("lead_type = ?")
            params.append(lead_type)
        if source_key:
            clauses.append("(source_key = ? or origin_source_key = ?)")
            params.extend([source_key, source_key])
        sql = "select payload from research_leads"
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by priority asc, created_at asc"
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        return [
            ResearchLeadRecord.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

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
        self.conn.execute(
            """
            update research_leads
            set status = ?,
                updated_at = ?,
                payload = ?
            where lead_id = ?
            """,
            (
                updated.status,
                updated.updated_at.isoformat(),
                json.dumps(payload, sort_keys=True),
                str(lead_id),
            ),
        )
        self.conn.commit()
        return updated

    def upsert_claim(self, claim: ClaimSearchResult) -> ClaimSearchResult:
        payload = claim.model_dump(mode="json")
        self.conn.execute(
            """
            insert into claims (
              claim_id, statement, claim_type, direction, confidence, evidence_level,
              species, source_object_id, source_title, source_url, support_count,
              contradiction_count, payload
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
              updated_at = current_timestamp
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
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()
        return claim

    def _upsert_run(self, handle: AsyncRunHandle) -> None:
        payload = handle.model_dump(mode="json")
        self.conn.execute(
            """
            insert into async_runs (run_id, run_kind, run_name, status, payload)
            values (?, ?, ?, ?, ?)
            on conflict(run_id) do update set
              run_kind = excluded.run_kind,
              run_name = excluded.run_name,
              status = excluded.status,
              payload = excluded.payload,
              updated_at = current_timestamp
            """,
            (
                str(handle.run_id),
                handle.run_kind,
                handle.run_name,
                str(handle.status),
                json.dumps(payload, sort_keys=True),
            ),
        )
        self.conn.commit()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            create table if not exists ingestion_sources (
              source_key text primary key,
              display_name text not null,
              source_kind text not null,
              base_url text,
              documentation_url text,
              license_policy text not null,
              requires_api_key integer not null default 0,
              enabled integer not null default 1,
              priority integer not null default 100,
              phase integer not null default 1,
              rate_limit_per_minute integer,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
            );

            create table if not exists source_queries (
              source_key text not null,
              query_name text not null,
              query_text text not null,
              query_params text not null default '{}',
              track text,
              object_type text,
              active integer not null default 1,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp,
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
              started_at text,
              completed_at text,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
            );

            create table if not exists raw_source_records (
              raw_record_id text primary key,
              source_key text not null,
              fetch_run_id text,
              source_record_id text,
              source_url text,
              content_hash text not null,
              payload text not null,
              retrieved_at text,
              first_seen_at text not null default current_timestamp,
              last_seen_at text not null default current_timestamp,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp,
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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
            );

            create index if not exists research_objects_source_idx
              on research_objects(source_key, updated_at desc);
            create index if not exists research_objects_title_idx
              on research_objects(title);

            create table if not exists identifier_links (
              object_id text not null,
              identifier_type text not null,
              identifier_value text not null,
              created_at text not null default current_timestamp,
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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp,
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
              vector_json text not null,
              payload text not null,
              embedded_at text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp,
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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp,
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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp,
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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp,
              unique (chunk_id, entity_type, normalized_key, chunk_char_start, chunk_char_end, resolver_name)
            );

            create index if not exists entity_mentions_source_idx
              on entity_mentions(source_key, entity_type);
            create index if not exists entity_mentions_chunk_idx
              on entity_mentions(chunk_id, chunk_char_start);
            create index if not exists entity_mentions_entity_idx
              on entity_mentions(entity_id);

            create table if not exists source_versions (
              source_key text primary key,
              source_version text not null,
              materialized_at text not null,
              source_url text,
              artifact_id text,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
            );

            create index if not exists source_versions_materialized_idx
              on source_versions(materialized_at desc);

            create table if not exists primitive_call_events (
              event_id text primary key,
              primitive_name text not null,
              status text not null,
              request_hash text not null,
              result_hash text,
              agent_run_id text,
              payload text not null,
              created_at text not null default current_timestamp
            );

            create index if not exists primitive_call_events_name_idx
              on primitive_call_events(primitive_name, created_at desc);
            create index if not exists primitive_call_events_agent_idx
              on primitive_call_events(agent_run_id, created_at desc);

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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
            );

            create table if not exists async_runs (
              run_id text primary key,
              run_kind text not null,
              run_name text not null,
              status text not null,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              started_at text not null,
              completed_at text,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              created_at text not null,
              payload text not null,
              updated_at text not null default current_timestamp
            );

            create index if not exists agent_run_reviews_run_idx
              on agent_run_reviews(agent_run_id, created_at desc);
            create index if not exists agent_run_reviews_verdict_idx
              on agent_run_reviews(verdict, created_at desc);

            create table if not exists reward_events (
              reward_event_id text primary key,
              identity_key text not null unique,
              event_source text not null,
              score real not null,
              agent_run_id text,
              source_review_id text,
              agent_name text,
              model_profile text,
              source_key text,
              created_at text not null,
              payload text not null,
              updated_at text not null default current_timestamp
            );

            create index if not exists reward_events_agent_idx
              on reward_events(agent_name, created_at desc);
            create index if not exists reward_events_run_idx
              on reward_events(agent_run_id, created_at desc);
            create index if not exists reward_events_source_idx
              on reward_events(source_key, created_at desc);

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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              overall_score real not null,
              passes_quality_bar integer not null default 0,
              readiness text not null,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              score real not null default 0.5,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              confidence_score real not null default 0.5,
              evidence_loop_count integer not null default 0,
              max_evidence_loops integer not null default 2,
              source_query text,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              confidence real not null default 0.5,
              validation_ready integer not null default 0,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
            );

            create index if not exists validation_decisions_outcome_idx
              on validation_decisions(outcome, confidence desc, updated_at desc);
            create index if not exists validation_decisions_therapy_idx
              on validation_decisions(therapy_idea_id, updated_at desc);
            create index if not exists validation_decisions_candidate_idx
              on validation_decisions(candidate_id, updated_at desc);
            create index if not exists validation_decisions_packet_idx
              on validation_decisions(packet_id, updated_at desc);

            create table if not exists public_candidates (
              candidate_id text primary key,
              display_id text,
              candidate_kind text not null,
              public_status text not null,
              visibility text not null,
              therapy_idea_id text,
              latest_snapshot_id text,
              content_hash text,
              priority_score real not null default 0.5,
              title text not null,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
            );

            create index if not exists public_candidates_status_idx
              on public_candidates(public_status, priority_score desc, updated_at desc);
            create index if not exists public_candidates_visibility_idx
              on public_candidates(visibility, updated_at desc);
            create index if not exists public_candidates_kind_idx
              on public_candidates(candidate_kind, updated_at desc);
            create index if not exists public_candidates_therapy_idx
              on public_candidates(therapy_idea_id, updated_at desc);
            create index if not exists public_candidates_display_idx
              on public_candidates(display_id);

            create table if not exists public_candidate_snapshots (
              snapshot_id text primary key,
              candidate_id text not null,
              snapshot_version integer not null,
              content_hash text not null,
              payload text not null,
              created_at text not null default current_timestamp,
              unique(candidate_id, snapshot_version)
            );

            create index if not exists public_candidate_snapshots_candidate_idx
              on public_candidate_snapshots(candidate_id, snapshot_version desc);
            create index if not exists public_candidate_snapshots_hash_idx
              on public_candidate_snapshots(content_hash);

            create table if not exists public_candidate_decision_events (
              event_id text primary key,
              candidate_id text not null,
              occurred_at text not null,
              action text not null,
              actor text not null,
              new_status text,
              related_snapshot_id text,
              payload text not null,
              created_at text not null default current_timestamp
            );

            create index if not exists public_candidate_decision_events_candidate_idx
              on public_candidate_decision_events(candidate_id, occurred_at desc);
            create index if not exists public_candidate_decision_events_action_idx
              on public_candidate_decision_events(action, occurred_at desc);

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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
            );

            create index if not exists compute_jobs_status_idx
              on compute_jobs(status, updated_at desc);
            create index if not exists compute_jobs_runner_idx
              on compute_jobs(runner_kind, updated_at desc);
            create index if not exists compute_jobs_queue_item_idx
              on compute_jobs(queue_item_id, updated_at desc);

            create table if not exists md_expert_review_packets (
              packet_id text primary key,
              packet_hash text not null,
              status text not null,
              compute_job_id text,
              queue_item_id text,
              endpoint_id text not null,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              reviewed_at text not null,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              parsed_at text,
              reviewed_at text,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp,
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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
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
              approved_for_fetch integer not null default 0,
              reviewed_by text not null,
              review_note text,
              storage_policy text,
              reviewed_at text not null,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
            );
            """
        )
        self._ensure_column("therapy_ideas", "source_program_id", "text")
        self.conn.execute(
            "create index if not exists therapy_ideas_program_idx on therapy_ideas(source_program_id, updated_at desc)"
        )
        self.conn.commit()

    def _ensure_column(self, table_name: str, column_name: str, column_type: str) -> None:
        columns = {
            str(row["name"])
            for row in self.conn.execute(f"pragma table_info({table_name})").fetchall()
        }
        if column_name not in columns:
            self.conn.execute(f"alter table {table_name} add column {column_name} {column_type}")

    def _seed_claims_if_empty(self) -> None:
        row = self.conn.execute("select count(*) as count from claims").fetchone()
        if row and row["count"] > 0:
            return
        for claim in seed_claims():
            self.upsert_claim(claim)


def stable_json_hash(value: dict[str, Any]) -> str:
    """Return a deterministic SHA-256 hash for a JSON-like payload."""

    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return sha256(encoded).hexdigest()


def build_research_object_dedupe_key(obj: ResearchObject) -> str:
    """Build a stable local dedupe key from identifiers or title/source."""

    for identifier_type in ("doi", "pmid", "pmcid", "openalex_id", "nct_id", "source_id"):
        value = obj.identifiers.get(identifier_type)
        if value:
            return f"{identifier_type}:{value.lower()}"
    if obj.title:
        return f"title:{(obj.source_key or 'unknown').lower()}:{obj.title.strip().lower()}"
    return f"object:{obj.id}"


def research_object_has_full_text(obj: ResearchObject | None) -> bool:
    """Return whether an object represents a body-text-bearing source record."""

    return bool(obj and obj.metadata.get("full_text_available") is True)


def should_preserve_existing_research_object(existing: ResearchObject, incoming: ResearchObject) -> bool:
    """Avoid downgrading a full-text object when a weaker duplicate arrives later."""

    return research_object_has_full_text(existing) and not research_object_has_full_text(incoming)


def document_chunk_from_payload(payload: dict[str, Any], *, chunk_id: object, object_id: object) -> DocumentChunk:
    """Hydrate a chunk with stable table IDs, even if older payload JSON drifted."""

    return DocumentChunk.model_validate(
        {
            **payload,
            "id": str(chunk_id),
            "research_object_id": str(object_id),
        }
    )


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
