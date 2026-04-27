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
    ArtifactHandle,
    AsyncRunHandle,
    CandidateDossier,
    CandidateDossierRequest,
    ClaimSearchRequest,
    ClaimSearchResult,
    CommitHypothesisRequest,
    DocumentChunk,
    HypothesisDraft,
    RawSourceRecord,
    ResearchObject,
    ResearchSource,
    ScrapeSourceProfileReview,
    ScrapeReviewRecord,
    SourceQuery,
    ValidationRequest,
)
from .repository import ResearchRepository, seed_claims


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
            "select object_id from research_objects where dedupe_key = ?",
            (dedupe_key,),
        ).fetchone()
        object_id = UUID(existing["object_id"]) if existing else obj.id
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

    def get_research_object(self, object_id: UUID) -> ResearchObject | None:
        row = self.conn.execute(
            "select payload from research_objects where object_id = ?",
            (str(object_id),),
        ).fetchone()
        if row is None:
            return None
        return ResearchObject.model_validate(json.loads(row["payload"]))

    def get_raw_record_payload(self, raw_record_id: UUID) -> dict[str, Any] | None:
        row = self.conn.execute(
            "select payload from raw_source_records where raw_record_id = ?",
            (str(raw_record_id),),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row["payload"])

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
        return chunk

    def list_document_chunks(
        self,
        object_id: UUID | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
        limit: int | None = None,
    ) -> list[DocumentChunk]:
        params: list[object] = []
        sql = "select dc.payload from document_chunks dc"
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
            DocumentChunk.model_validate(json.loads(row["payload"]))
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def coverage_summary(self) -> dict[str, Any]:
        source_count = self.conn.execute("select count(*) as count from ingestion_sources").fetchone()["count"]
        query_count = self.conn.execute("select count(*) as count from source_queries").fetchone()["count"]
        active_query_count = self.conn.execute(
            "select count(*) as count from source_queries where active = 1"
        ).fetchone()["count"]
        raw_count = self.conn.execute("select count(*) as count from raw_source_records").fetchone()["count"]
        object_count = self.conn.execute("select count(*) as count from research_objects").fetchone()["count"]
        chunk_count = self.conn.execute("select count(*) as count from document_chunks").fetchone()["count"]
        claims_count = self.conn.execute("select count(*) as count from claims").fetchone()["count"]
        scrape_review_count = self.conn.execute("select count(*) as count from scrape_review_records").fetchone()["count"]
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
            "claims": claims_count,
            "scrape_review_records": scrape_review_count,
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
        self.conn.commit()

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
