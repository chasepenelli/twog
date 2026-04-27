"""Repository adapters for Ingestion Bridge v2.

The in-memory repository is deliberately useful for local MCP and unit tests.
Database-backed implementations should preserve these method contracts.
"""

from __future__ import annotations

from datetime import UTC, datetime
import re
from typing import Protocol
from uuid import UUID, uuid4

from .contracts import (
    ArtifactHandle,
    AsyncRunHandle,
    CandidateDossier,
    CandidateDossierRequest,
    ClaimDirection,
    ClaimSearchRequest,
    ClaimSearchResult,
    CommitHypothesisRequest,
    DocumentChunk,
    EmbeddingCoverageSummary,
    EntityAlias,
    EntityMention,
    EvidenceLevel,
    HypothesisDraft,
    ResearchChunkSearchRequest,
    ResearchChunkSearchResult,
    ResearchObject,
    ResolvedEntity,
    RunStatus,
    ScrapeSourceProfileReview,
    ScrapeReviewRecord,
    TextEmbedding,
    TextEmbeddingSearchRequest,
    TextEmbeddingSearchResult,
    ValidationRequest,
)


_KEYWORD_PATTERN = re.compile(r"[a-z0-9]+(?:[._/-][a-z0-9]+)*")


class ResearchRepository(Protocol):
    def get_research_object(self, object_id: UUID) -> ResearchObject | None:
        """Return a canonical research object by ID."""

    def get_document_chunk(self, chunk_id: UUID) -> DocumentChunk | None:
        """Return a single document chunk by ID."""

    def list_document_chunks(
        self,
        object_id: UUID | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
        limit: int | None = None,
    ) -> list[DocumentChunk]:
        """Return document chunks by durable filters."""

    def search_research_chunks(self, request: ResearchChunkSearchRequest) -> list[ResearchChunkSearchResult]:
        """Search document chunks with keyword matching as a retrieval fallback."""

    def search_claims(self, request: ClaimSearchRequest) -> list[ClaimSearchResult]:
        """Search claims by typed filters."""

    def get_claim(self, claim_id: UUID) -> ClaimSearchResult | None:
        """Return a single claim by ID."""

    def upsert_entity(self, entity: ResolvedEntity) -> ResolvedEntity:
        """Persist a canonical resolved entity."""

    def list_entities(
        self,
        *,
        entity_type: str | None = None,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[ResolvedEntity]:
        """Return canonical resolved entities."""

    def upsert_entity_alias(self, alias: EntityAlias) -> EntityAlias:
        """Persist a deterministic alias for a resolved entity."""

    def upsert_entity_mention(self, mention: EntityMention) -> EntityMention:
        """Persist a chunk-level resolved entity mention."""

    def list_entity_mentions(
        self,
        *,
        source_key: str | None = None,
        object_id: UUID | None = None,
        chunk_id: UUID | None = None,
        entity_type: str | None = None,
        limit: int | None = None,
    ) -> list[EntityMention]:
        """Return chunk-level resolved entity mentions."""

    def upsert_text_embedding(self, embedding: TextEmbedding) -> TextEmbedding:
        """Persist a text embedding for one document chunk and model."""

    def get_text_embedding(self, embedding_id: UUID) -> TextEmbedding | None:
        """Return a single text embedding by ID."""

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
        """Return stored text embeddings by durable filters."""

    def search_text_embeddings(self, request: TextEmbeddingSearchRequest) -> list[TextEmbeddingSearchResult]:
        """Search stored text embeddings using local JSON-vector similarity."""

    def embedding_coverage(
        self,
        *,
        source_key: str | None = None,
        object_type: str | None = None,
        embedding_model: str | None = None,
    ) -> EmbeddingCoverageSummary:
        """Return document chunk embedding coverage."""

    def get_candidate(self, request: CandidateDossierRequest) -> CandidateDossier | None:
        """Return a candidate dossier."""

    def commit_hypothesis(self, request: CommitHypothesisRequest) -> HypothesisDraft:
        """Persist an approved hypothesis."""

    def enqueue_validation(self, request: ValidationRequest) -> AsyncRunHandle:
        """Queue a validation job and return an async handle."""

    def get_run_status(self, run_id: UUID) -> AsyncRunHandle | None:
        """Return async run status."""

    def get_artifact(self, artifact_id: UUID) -> ArtifactHandle | None:
        """Return artifact metadata."""

    def upsert_artifact(self, artifact: ArtifactHandle) -> ArtifactHandle:
        """Persist artifact metadata."""

    def list_artifacts(
        self,
        *,
        artifact_type: str | None = None,
        source_key: str | None = None,
        limit: int | None = None,
    ) -> list[ArtifactHandle]:
        """Return stored artifact metadata."""

    def upsert_scrape_review(self, record: ScrapeReviewRecord) -> ScrapeReviewRecord:
        """Persist a parsed scrape record for review."""

    def list_scrape_reviews(
        self,
        *,
        source_key: str | None = None,
        review_status: str | None = None,
        review_ids: list[UUID] | None = None,
        artifact_ids: list[UUID] | None = None,
        limit: int | None = None,
    ) -> list[ScrapeReviewRecord]:
        """Return parsed scrape records pending or after review."""

    def update_scrape_review(
        self,
        review_id: UUID,
        *,
        review_status: str,
        reviewed_by: str,
        review_note: str | None = None,
    ) -> ScrapeReviewRecord | None:
        """Update review decision for a parsed scrape record."""

    def upsert_scrape_profile_review(self, review: ScrapeSourceProfileReview) -> ScrapeSourceProfileReview:
        """Persist operator review for a scrape source profile."""

    def get_scrape_profile_review(self, source_key: str) -> ScrapeSourceProfileReview | None:
        """Return operator review for a scrape source profile."""


class InMemoryResearchRepository:
    """Small deterministic repository for local development and MCP smoke tests."""

    def __init__(self) -> None:
        self.claims = seed_claims()
        self.runs: dict[UUID, AsyncRunHandle] = {}
        self.artifacts: dict[UUID, ArtifactHandle] = {}
        self.research_objects: dict[UUID, ResearchObject] = {}
        self.document_chunks: dict[UUID, DocumentChunk] = {}
        self.entities: dict[tuple[str, str], ResolvedEntity] = {}
        self.entity_aliases: dict[tuple[str, str, str], EntityAlias] = {}
        self.entity_mentions: dict[UUID, EntityMention] = {}
        self.text_embeddings: dict[UUID, TextEmbedding] = {}
        self.scrape_reviews: dict[UUID, ScrapeReviewRecord] = {}
        self.scrape_profile_reviews: dict[str, ScrapeSourceProfileReview] = {}
        self.hypotheses: dict[UUID, HypothesisDraft] = {}

    def get_research_object(self, object_id: UUID) -> ResearchObject | None:
        return self.research_objects.get(object_id)

    def get_document_chunk(self, chunk_id: UUID) -> DocumentChunk | None:
        return self.document_chunks.get(chunk_id)

    def list_document_chunks(
        self,
        object_id: UUID | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
        limit: int | None = None,
    ) -> list[DocumentChunk]:
        chunks = list(self.document_chunks.values())
        if object_id:
            chunks = [chunk for chunk in chunks if chunk.research_object_id == object_id]
        if source_key or object_type:
            filtered: list[DocumentChunk] = []
            for chunk in chunks:
                obj = self.get_research_object(chunk.research_object_id)
                if source_key and (obj is None or obj.source_key != source_key):
                    continue
                if object_type and (obj is None or str(obj.object_type) != object_type):
                    continue
                filtered.append(chunk)
            chunks = filtered
        chunks.sort(key=lambda chunk: (str(chunk.research_object_id), chunk.chunk_index))
        return chunks[:limit] if limit is not None else chunks

    def search_research_chunks(self, request: ResearchChunkSearchRequest) -> list[ResearchChunkSearchResult]:
        terms = keyword_terms(request.query)
        if not terms:
            return []
        chunks = self.list_document_chunks(
            object_id=request.research_object_id,
            source_key=request.source_key,
            object_type=str(request.object_type) if request.object_type else None,
        )
        results: list[ResearchChunkSearchResult] = []
        for chunk in chunks:
            obj = self.get_research_object(chunk.research_object_id)
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

    def search_claims(self, request: ClaimSearchRequest) -> list[ClaimSearchResult]:
        results = list(self.claims)

        if request.query:
            query = request.query.lower()
            results = [claim for claim in results if query in claim.statement.lower()]

        if request.species:
            species = request.species.lower()
            results = [claim for claim in results if (claim.species or "").lower() == species]

        if request.claim_types:
            allowed = set(request.claim_types)
            results = [claim for claim in results if claim.claim_type in allowed]

        if request.evidence_levels:
            allowed_levels = set(request.evidence_levels)
            results = [claim for claim in results if claim.evidence_level in allowed_levels]

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

        results = [claim for claim in results if claim.confidence >= request.min_confidence]
        return results[: request.limit]

    def get_claim(self, claim_id: UUID) -> ClaimSearchResult | None:
        return next((claim for claim in self.claims if claim.claim_id == claim_id), None)

    def upsert_entity(self, entity: ResolvedEntity) -> ResolvedEntity:
        key = (entity.entity_type, entity.normalized_key)
        existing = self.entities.get(key)
        if existing:
            entity = entity.model_copy(update={"entity_id": existing.entity_id})
        self.entities[key] = entity
        return entity

    def list_entities(
        self,
        *,
        entity_type: str | None = None,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[ResolvedEntity]:
        entities = list(self.entities.values())
        if entity_type:
            entities = [entity for entity in entities if entity.entity_type == entity_type]
        if query:
            q = query.lower()
            entities = [
                entity
                for entity in entities
                if q in entity.canonical_name.lower() or q in entity.normalized_key.lower()
            ]
        entities.sort(key=lambda entity: (entity.entity_type, entity.canonical_name))
        return entities[:limit] if limit is not None else entities

    def upsert_entity_alias(self, alias: EntityAlias) -> EntityAlias:
        key = (alias.entity_type, alias.alias_normalized, alias.normalized_key)
        existing = self.entity_aliases.get(key)
        if existing:
            alias = alias.model_copy(update={"alias_id": existing.alias_id})
        self.entity_aliases[key] = alias
        return alias

    def upsert_entity_mention(self, mention: EntityMention) -> EntityMention:
        existing = next(
            (
                item
                for item in self.entity_mentions.values()
                if item.chunk_id == mention.chunk_id
                and item.entity_type == mention.entity_type
                and item.normalized_key == mention.normalized_key
                and item.chunk_char_start == mention.chunk_char_start
                and item.chunk_char_end == mention.chunk_char_end
                and item.resolver_name == mention.resolver_name
            ),
            None,
        )
        if existing:
            mention = mention.model_copy(update={"mention_id": existing.mention_id})
        self.entity_mentions[mention.mention_id] = mention
        return mention

    def list_entity_mentions(
        self,
        *,
        source_key: str | None = None,
        object_id: UUID | None = None,
        chunk_id: UUID | None = None,
        entity_type: str | None = None,
        limit: int | None = None,
    ) -> list[EntityMention]:
        mentions = list(self.entity_mentions.values())
        if source_key:
            mentions = [mention for mention in mentions if mention.source_key == source_key]
        if object_id:
            mentions = [mention for mention in mentions if mention.research_object_id == object_id]
        if chunk_id:
            mentions = [mention for mention in mentions if mention.chunk_id == chunk_id]
        if entity_type:
            mentions = [mention for mention in mentions if mention.entity_type == entity_type]
        mentions.sort(key=lambda mention: (str(mention.research_object_id), mention.chunk_index, mention.chunk_char_start))
        return mentions[:limit] if limit is not None else mentions

    def upsert_text_embedding(self, embedding: TextEmbedding) -> TextEmbedding:
        existing = next(
            (
                item
                for item in self.text_embeddings.values()
                if item.chunk_id == embedding.chunk_id and item.embedding_model == embedding.embedding_model
            ),
            None,
        )
        if existing:
            embedding = embedding.model_copy(update={"embedding_id": existing.embedding_id})
        self.text_embeddings[embedding.embedding_id] = embedding
        return embedding

    def get_text_embedding(self, embedding_id: UUID) -> TextEmbedding | None:
        return self.text_embeddings.get(embedding_id)

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
        embeddings = list(self.text_embeddings.values())
        if embedding_model:
            embeddings = [embedding for embedding in embeddings if embedding.embedding_model == embedding_model]
        if source_key:
            embeddings = [embedding for embedding in embeddings if embedding.source_key == source_key]
        if research_object_id:
            embeddings = [
                embedding for embedding in embeddings if embedding.research_object_id == research_object_id
            ]
        if chunk_id:
            embeddings = [embedding for embedding in embeddings if embedding.chunk_id == chunk_id]
        if object_type:
            embeddings = [embedding for embedding in embeddings if str(embedding.object_type) == object_type]
        embeddings.sort(
            key=lambda embedding: (
                embedding.source_key or "",
                str(embedding.research_object_id),
                embedding.chunk_index,
                embedding.embedding_model,
            )
        )
        return embeddings[:limit] if limit is not None else embeddings

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
        embeddings = self.list_text_embeddings(
            embedding_model=embedding_model,
            source_key=source_key,
            object_type=object_type,
        )
        embedded_chunks = len({embedding.chunk_id for embedding in embeddings})
        by_model: dict[str, int] = {}
        for embedding in self.list_text_embeddings(source_key=source_key, object_type=object_type):
            by_model[embedding.embedding_model] = by_model.get(embedding.embedding_model, 0) + 1
        return EmbeddingCoverageSummary(
            source_key=source_key,
            object_type=object_type,
            embedding_model=embedding_model,
            total_chunks=embedded_chunks,
            embedded_chunks=embedded_chunks,
            missing_chunks=0,
            coverage_ratio=1.0 if embedded_chunks else 0.0,
            embedding_models=dict(sorted(by_model.items())),
        )

    def get_candidate(self, request: CandidateDossierRequest) -> CandidateDossier | None:
        name = request.candidate_name or "propranolol"
        candidate_id = request.candidate_id or uuid4()
        matching_claims = [
            claim
            for claim in self.claims
            if any(entity.canonical_name.lower() == name.lower() for entity in claim.entities)
        ]
        return CandidateDossier(
            candidate_id=candidate_id,
            name=name,
            status="investigating",
            summary="Local scaffold dossier. Wire to Supabase candidate tables in Phase 1.",
            evidence_claims=matching_claims if request.include_claims else [],
            risk_flags=[],
            metadata={"repository": "in_memory"},
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
        self.hypotheses[hypothesis_id] = committed
        return committed

    def enqueue_validation(self, request: ValidationRequest) -> AsyncRunHandle:
        status = RunStatus.NEEDS_APPROVAL if request.require_approval else RunStatus.QUEUED
        handle = AsyncRunHandle(
            run_name=f"{request.validation_type}:{request.candidate_name or request.candidate_id or 'unknown'}",
            status=status,
            metadata=request.model_dump(mode="json"),
        )
        self.runs[handle.run_id] = handle
        return handle

    def get_run_status(self, run_id: UUID) -> AsyncRunHandle | None:
        return self.runs.get(run_id)

    def get_artifact(self, artifact_id: UUID) -> ArtifactHandle | None:
        return self.artifacts.get(artifact_id)

    def upsert_artifact(self, artifact: ArtifactHandle) -> ArtifactHandle:
        self.artifacts[artifact.artifact_id] = artifact
        return artifact

    def list_artifacts(
        self,
        *,
        artifact_type: str | None = None,
        source_key: str | None = None,
        limit: int | None = None,
    ) -> list[ArtifactHandle]:
        artifacts = list(self.artifacts.values())
        if artifact_type:
            artifacts = [artifact for artifact in artifacts if artifact.artifact_type == artifact_type]
        if source_key:
            artifacts = [artifact for artifact in artifacts if artifact.metadata.get("source_key") == source_key]
        return artifacts[:limit] if limit is not None else artifacts

    def upsert_scrape_review(self, record: ScrapeReviewRecord) -> ScrapeReviewRecord:
        existing = next(
            (
                item
                for item in self.scrape_reviews.values()
                if item.source_key == record.source_key
                and item.artifact_id == record.artifact_id
                and item.source_record_id == record.source_record_id
            ),
            None,
        )
        if existing:
            record = record.model_copy(
                update={
                    "review_id": existing.review_id,
                    "review_status": existing.review_status,
                    "reviewer": existing.reviewer,
                    "review_note": existing.review_note,
                    "reviewed_at": existing.reviewed_at,
                }
            )
        self.scrape_reviews[record.review_id] = record
        return record

    def list_scrape_reviews(
        self,
        *,
        source_key: str | None = None,
        review_status: str | None = None,
        review_ids: list[UUID] | None = None,
        artifact_ids: list[UUID] | None = None,
        limit: int | None = None,
    ) -> list[ScrapeReviewRecord]:
        records = list(self.scrape_reviews.values())
        if source_key:
            records = [record for record in records if record.source_key == source_key]
        if review_status:
            records = [record for record in records if record.review_status == review_status]
        if review_ids:
            allowed = set(review_ids)
            records = [record for record in records if record.review_id in allowed]
        if artifact_ids:
            allowed_artifacts = set(artifact_ids)
            records = [record for record in records if record.artifact_id in allowed_artifacts]
        return records[:limit] if limit is not None else records

    def update_scrape_review(
        self,
        review_id: UUID,
        *,
        review_status: str,
        reviewed_by: str,
        review_note: str | None = None,
    ) -> ScrapeReviewRecord | None:
        record = self.scrape_reviews.get(review_id)
        if record is None:
            return None
        updated = record.model_copy(
            update={
                "review_status": review_status,
                "reviewer": reviewed_by,
                "review_note": review_note,
                "reviewed_at": datetime.now(UTC),
            }
        )
        self.scrape_reviews[review_id] = updated
        return updated

    def upsert_scrape_profile_review(self, review: ScrapeSourceProfileReview) -> ScrapeSourceProfileReview:
        self.scrape_profile_reviews[review.source_key] = review
        return review

    def get_scrape_profile_review(self, source_key: str) -> ScrapeSourceProfileReview | None:
        return self.scrape_profile_reviews.get(source_key)


def seed_claims() -> list[ClaimSearchResult]:
    """Seed claims keep the local MCP server useful before DB wiring exists."""

    from .contracts import ClaimType, EntityRef

    propranolol = EntityRef(entity_type="compound", canonical_name="propranolol", role="compound")
    doxorubicin = EntityRef(entity_type="compound", canonical_name="doxorubicin", role="combination_partner")
    hsa = EntityRef(entity_type="disease", canonical_name="canine hemangiosarcoma", role="disease")
    vegf = EntityRef(entity_type="target", canonical_name="VEGFA", role="target")

    return [
        ClaimSearchResult(
            claim_id=uuid4(),
            statement="Propranolol has been investigated with doxorubicin in canine hemangiosarcoma.",
            claim_type=ClaimType.COMPOUND_AFFECTS_OUTCOME,
            direction=ClaimDirection.MIXED,
            confidence=0.55,
            evidence_level=EvidenceLevel.CANINE_CLINICAL,
            species="canine",
            entities=[propranolol, doxorubicin, hsa],
            support_count=1,
            metadata={"seed": True},
        ),
        ClaimSearchResult(
            claim_id=uuid4(),
            statement="Angiogenic signaling is a recurring therapeutic axis in canine hemangiosarcoma research.",
            claim_type=ClaimType.PATHWAY_ACTIVE_IN_DISEASE,
            direction=ClaimDirection.POSITIVE,
            confidence=0.65,
            evidence_level=EvidenceLevel.REVIEW,
            species="canine",
            entities=[vegf, hsa],
            support_count=1,
            metadata={"seed": True},
        ),
    ]


_seed_claims = seed_claims


def cosine_similarity(left: list[float], right: list[float]) -> float | None:
    """Return cosine similarity for same-length non-zero vectors."""

    if not left or len(left) != len(right):
        return None
    left_norm = sum(value * value for value in left) ** 0.5
    right_norm = sum(value * value for value in right) ** 0.5
    if left_norm == 0 or right_norm == 0:
        return None
    score = sum(left_value * right_value for left_value, right_value in zip(left, right, strict=True))
    return max(min(score / (left_norm * right_norm), 1.0), -1.0)


def keyword_terms(query: str) -> list[str]:
    """Return stable lowercase terms for conservative chunk fallback search."""

    seen: set[str] = set()
    terms: list[str] = []
    for term in _KEYWORD_PATTERN.findall(query.lower()):
        if len(term) < 2 or term in seen:
            continue
        seen.add(term)
        terms.append(term)
    return terms


def keyword_chunk_score(
    query: str,
    terms: list[str],
    chunk: DocumentChunk,
    research_object: ResearchObject | None,
) -> float:
    """Return a bounded keyword relevance score for one chunk/object pair."""

    if not terms:
        return 0.0
    title = research_object.title if research_object and research_object.title else ""
    abstract = research_object.abstract if research_object and research_object.abstract else ""
    haystack = f"{title}\n{abstract}\n{chunk.section_label or ''}\n{chunk.text_content}".lower()
    matches = sum(1 for term in terms if term in haystack)
    if matches == 0:
        return 0.0

    query_text = " ".join(query.lower().split())
    phrase_bonus = 0.2 if query_text and query_text in haystack else 0.0
    title_bonus = 0.1 * sum(1 for term in terms if term in title.lower())
    score = (matches / len(terms)) * 0.7 + phrase_bonus + min(title_bonus, 0.1)
    return min(score, 1.0)
