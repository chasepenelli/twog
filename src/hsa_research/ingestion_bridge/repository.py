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
    AgentRunRecord,
    AgentRunReviewRecord,
    ArtifactHandle,
    AsyncRunHandle,
    CandidateDossier,
    CandidateDossierRequest,
    ClaimDirection,
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
    EvidenceLevel,
    HypothesisDraft,
    MDExpertApprovalRecord,
    MDExpertReviewPacketRecord,
    PublicCandidateDecisionEvent,
    PublicCandidateLibraryRequest,
    PublicCandidateRecord,
    PublicCandidateSnapshot,
    ResearchChunkSearchRequest,
    ResearchChunkSearchResult,
    ResearchProgramRecord,
    ResearchBriefEvaluationRecord,
    ResearchBriefQueueItem,
    ResearchBriefRecord,
    ResearchLeadRecord,
    ResearchObject,
    ValidationDecisionRecord,
    ValidationPlanRecord,
    ValidationRequestQueueItem,
    ResolvedEntity,
    RunStatus,
    SourceFollowupQueueItem,
    SourceQuery,
    ScrapeSourceProfileReview,
    ScrapeReviewRecord,
    TextEmbedding,
    TextEmbeddingSearchRequest,
    TextEmbeddingSearchResult,
    TherapyIdeaRecord,
    ValidationRequest,
)


_KEYWORD_PATTERN = re.compile(r"[a-z0-9]+(?:[._/-][a-z0-9]+)*")


class ResearchRepository(Protocol):
    def get_research_object(self, object_id: UUID) -> ResearchObject | None:
        """Return a canonical research object by ID."""

    def update_research_object(self, obj: ResearchObject) -> ResearchObject | None:
        """Replace a canonical research object payload by ID."""

    def list_research_objects(
        self,
        object_type: str | None = None,
        source_key: str | None = None,
        limit: int | None = None,
    ) -> list[ResearchObject]:
        """Return canonical research objects by durable filters."""

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

    def list_document_chunks_for_fetch_runs(
        self,
        fetch_run_ids: list[UUID],
        limit: int | None = None,
    ) -> list[DocumentChunk]:
        """Return document chunks linked to raw records from specific fetch runs."""

    def replace_document_chunks(self, object_id: UUID, chunks: list[DocumentChunk]) -> list[DocumentChunk]:
        """Replace all chunks for a research object."""

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

    def count_orphan_text_embeddings(
        self,
        *,
        embedding_model: str | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
    ) -> int:
        """Return embeddings whose chunk no longer exists."""

    def delete_orphan_text_embeddings(
        self,
        *,
        embedding_model: str | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
    ) -> int:
        """Delete embeddings whose chunk no longer exists."""

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

    def create_agent_run(self, record: AgentRunRecord) -> AgentRunRecord:
        """Persist a newly started agent run."""

    def finish_agent_run(
        self,
        agent_run_id: UUID,
        *,
        status: str,
        output_payload: dict,
        summary: dict,
        errors: list[str],
    ) -> AgentRunRecord | None:
        """Mark an agent run terminal and persist its output payload."""

    def get_agent_run(self, agent_run_id: UUID) -> AgentRunRecord | None:
        """Return a persisted agent run."""

    def list_agent_runs(
        self,
        *,
        agent_name: str | None = None,
        status: str | None = None,
        source_key: str | None = None,
        limit: int = 50,
    ) -> list[AgentRunRecord]:
        """Return recent agent runs by durable filters."""

    def create_agent_run_review(self, record: AgentRunReviewRecord) -> AgentRunReviewRecord:
        """Persist an operator review for an agent run."""

    def get_agent_run_review(self, review_id: UUID) -> AgentRunReviewRecord | None:
        """Return a persisted agent run review."""

    def list_agent_run_reviews(
        self,
        *,
        agent_run_id: UUID | None = None,
        verdict: str | None = None,
        reviewer: str | None = None,
        limit: int = 50,
    ) -> list[AgentRunReviewRecord]:
        """Return recent operator reviews for agent runs."""

    def upsert_research_brief(self, record: ResearchBriefRecord) -> ResearchBriefRecord:
        """Persist a generated research brief and its full typed payload."""

    def get_research_brief(self, brief_id: UUID) -> ResearchBriefRecord | None:
        """Return a generated research brief."""

    def list_research_briefs(
        self,
        *,
        status: str | None = None,
        source_key: str | None = None,
        topic_query: str | None = None,
        limit: int | None = 50,
    ) -> list[ResearchBriefRecord]:
        """Return generated research briefs by durable filters."""

    def upsert_research_brief_evaluation(
        self,
        record: ResearchBriefEvaluationRecord,
    ) -> ResearchBriefEvaluationRecord:
        """Persist a synthesis-quality evaluation for a generated brief."""

    def get_research_brief_evaluation(self, evaluation_id: UUID) -> ResearchBriefEvaluationRecord | None:
        """Return a generated research brief evaluation."""

    def list_research_brief_evaluations(
        self,
        *,
        brief_id: UUID | None = None,
        readiness: str | None = None,
        passes_quality_bar: bool | None = None,
        limit: int | None = 50,
    ) -> list[ResearchBriefEvaluationRecord]:
        """Return generated research brief evaluations by durable filters."""

    def upsert_therapy_idea(self, record: TherapyIdeaRecord) -> TherapyIdeaRecord:
        """Persist a therapy idea promoted by the committee layer."""

    def get_therapy_idea(self, therapy_idea_id: UUID) -> TherapyIdeaRecord | None:
        """Return a persisted therapy idea."""

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
        """Return persisted therapy ideas by durable filters."""

    def upsert_research_program(self, record: ResearchProgramRecord) -> ResearchProgramRecord:
        """Persist a big-bet research program."""

    def get_research_program(self, program_id: UUID) -> ResearchProgramRecord | None:
        """Return a persisted research program."""

    def list_research_programs(
        self,
        *,
        status: str | None = None,
        gate_decision: str | None = None,
        thesis_query: str | None = None,
        limit: int | None = 50,
    ) -> list[ResearchProgramRecord]:
        """Return persisted research programs by durable filters."""

    def upsert_validation_decision(self, record: ValidationDecisionRecord) -> ValidationDecisionRecord:
        """Persist a finite validation decision generated from a validation packet."""

    def get_validation_decision(self, decision_id: str) -> ValidationDecisionRecord | None:
        """Return a persisted validation decision by stable decision ID."""

    def list_validation_decisions(
        self,
        *,
        outcome: str | None = None,
        therapy_idea_id: UUID | None = None,
        candidate_id: str | None = None,
        limit: int | None = 50,
    ) -> list[ValidationDecisionRecord]:
        """Return persisted validation decisions by durable filters."""

    def upsert_public_candidate(self, record: PublicCandidateRecord) -> PublicCandidateRecord:
        """Persist an inspectable public-proof candidate record."""

    def get_public_candidate(self, candidate_id: str) -> PublicCandidateRecord | None:
        """Return a persisted public candidate by stable candidate ID."""

    def list_public_candidates(self, request: PublicCandidateLibraryRequest | None = None) -> list[PublicCandidateRecord]:
        """Return public candidates by durable filters."""

    def upsert_public_candidate_snapshot(self, record: PublicCandidateSnapshot) -> PublicCandidateSnapshot:
        """Persist a generated candidate page snapshot."""

    def get_public_candidate_snapshot(self, snapshot_id: UUID) -> PublicCandidateSnapshot | None:
        """Return a persisted candidate snapshot by ID."""

    def list_public_candidate_snapshots(
        self,
        *,
        candidate_id: str | None = None,
        limit: int | None = 50,
    ) -> list[PublicCandidateSnapshot]:
        """Return candidate snapshots newest-first."""

    def append_public_candidate_decision_event(
        self,
        record: PublicCandidateDecisionEvent,
    ) -> PublicCandidateDecisionEvent:
        """Append a candidate decision-log event."""

    def list_public_candidate_decision_events(
        self,
        *,
        candidate_id: str | None = None,
        limit: int | None = 100,
    ) -> list[PublicCandidateDecisionEvent]:
        """Return candidate decision-log events newest-first."""

    def upsert_validation_plan(self, record: ValidationPlanRecord) -> ValidationPlanRecord:
        """Persist a recommend-only validation plan derived from a research brief."""

    def get_validation_plan(self, plan_id: UUID) -> ValidationPlanRecord | None:
        """Return a persisted validation plan."""

    def list_validation_plans(
        self,
        *,
        brief_id: UUID | None = None,
        evaluation_id: UUID | None = None,
        status: str | None = None,
        readiness: str | None = None,
        limit: int | None = 50,
    ) -> list[ValidationPlanRecord]:
        """Return persisted validation plans by durable filters."""

    def upsert_validation_request_queue_item(
        self,
        item: ValidationRequestQueueItem,
    ) -> ValidationRequestQueueItem:
        """Persist a queued validation request derived from a plan task."""

    def get_validation_request_queue_item(
        self,
        queue_item_id: UUID,
    ) -> ValidationRequestQueueItem | None:
        """Return one queued validation request."""

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
        """Return queued validation requests by durable filters."""

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
        """Update queued validation request lifecycle fields."""

    def upsert_compute_job(self, record: ComputeJobRecord) -> ComputeJobRecord:
        """Persist a durable compute job record for approval-first execution lanes."""

    def get_compute_job(self, compute_job_id: UUID) -> ComputeJobRecord | None:
        """Return one compute job by ID."""

    def list_compute_jobs(
        self,
        *,
        status: str | None = None,
        runner_kind: str | None = None,
        queue_item_id: UUID | None = None,
        limit: int | None = 50,
    ) -> list[ComputeJobRecord]:
        """Return compute jobs by durable filters."""

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
        """Update compute job lifecycle fields."""

    def upsert_command_center_board_stage(
        self,
        record: CommandCenterBoardStageRecord,
    ) -> CommandCenterBoardStageRecord:
        """Persist the command-center board stage for one entity."""

    def get_command_center_board_stage(
        self,
        entity_type: str,
        entity_id: str,
    ) -> CommandCenterBoardStageRecord | None:
        """Return the persisted command-center board stage for one entity."""

    def list_command_center_board_stages(
        self,
        *,
        entity_type: str | None = None,
        board_stage: str | None = None,
        limit: int | None = 500,
    ) -> list[CommandCenterBoardStageRecord]:
        """Return persisted command-center board stages."""

    def append_command_center_activity_event(
        self,
        record: CommandCenterActivityEventRecord,
    ) -> CommandCenterActivityEventRecord:
        """Append one command-center activity event."""

    def list_command_center_activity_events(
        self,
        *,
        entity_type: str | None = None,
        entity_id: str | None = None,
        source: str | None = None,
        limit: int | None = 100,
    ) -> list[CommandCenterActivityEventRecord]:
        """Return command-center activity events newest first."""

    def upsert_md_expert_review_packet(
        self,
        record: MDExpertReviewPacketRecord,
    ) -> MDExpertReviewPacketRecord:
        """Persist an MD expert review packet for pre-run human approval."""

    def get_md_expert_review_packet(self, packet_id: UUID) -> MDExpertReviewPacketRecord | None:
        """Return one MD expert review packet."""

    def get_md_expert_review_packet_by_hash(self, packet_hash: str) -> MDExpertReviewPacketRecord | None:
        """Return one MD expert review packet by stable packet hash."""

    def list_md_expert_review_packets(
        self,
        *,
        packet_hash: str | None = None,
        status: str | None = None,
        limit: int | None = 50,
    ) -> list[MDExpertReviewPacketRecord]:
        """Return persisted MD expert review packets by durable filters."""

    def upsert_md_expert_approval(self, record: MDExpertApprovalRecord) -> MDExpertApprovalRecord:
        """Persist an MD expert approval or rejection ledger entry."""

    def list_md_expert_approvals(
        self,
        *,
        packet_hash: str | None = None,
        decision: str | None = None,
        limit: int | None = 50,
    ) -> list[MDExpertApprovalRecord]:
        """Return MD expert approval ledger entries by durable filters."""

    def upsert_research_brief_queue_item(self, item: ResearchBriefQueueItem) -> ResearchBriefQueueItem:
        """Persist a queued research brief request."""

    def get_research_brief_queue_item(self, queue_item_id: UUID) -> ResearchBriefQueueItem | None:
        """Return one queued research brief request."""

    def list_research_brief_queue_items(
        self,
        *,
        status: str | None = None,
        statuses: list[str] | None = None,
        source_key: str | None = None,
        topic_query: str | None = None,
        limit: int | None = 50,
    ) -> list[ResearchBriefQueueItem]:
        """Return queued research brief requests by durable filters."""

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
        """Update queued research brief lifecycle fields."""

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

    def upsert_source_followup(self, item: SourceFollowupQueueItem) -> SourceFollowupQueueItem:
        """Persist a source follow-up queue item."""

    def get_source_followup(self, followup_id: UUID) -> SourceFollowupQueueItem | None:
        """Return a source follow-up queue item."""

    def list_source_followups(
        self,
        *,
        source_key: str | None = None,
        status: str | None = None,
        statuses: list[str] | None = None,
        identifier_type: str | None = None,
        limit: int | None = None,
    ) -> list[SourceFollowupQueueItem]:
        """Return source follow-up queue items by durable filters."""

    def update_source_followup(
        self,
        followup_id: UUID,
        *,
        status: str,
        attempts: int | None = None,
        last_error: str | None = None,
        metadata: dict | None = None,
    ) -> SourceFollowupQueueItem | None:
        """Update source follow-up queue lifecycle fields."""

    def upsert_source_query(self, query: SourceQuery) -> SourceQuery:
        """Persist a durable source query."""

    def list_source_queries(self, source_key: str | None = None, active_only: bool = True) -> list[SourceQuery]:
        """Return durable source queries."""

    def upsert_research_lead(self, lead: ResearchLeadRecord) -> ResearchLeadRecord:
        """Persist a watchlist lead for evidence not yet cleanly ingestible."""

    def get_research_lead(self, lead_id: UUID) -> ResearchLeadRecord | None:
        """Return a watchlist lead."""

    def list_research_leads(
        self,
        *,
        status: str | None = None,
        statuses: list[str] | None = None,
        lead_type: str | None = None,
        source_key: str | None = None,
        limit: int | None = None,
    ) -> list[ResearchLeadRecord]:
        """Return watchlist leads by durable filters."""

    def update_research_lead(
        self,
        lead_id: UUID,
        *,
        status: str | None = None,
        metadata: dict | None = None,
    ) -> ResearchLeadRecord | None:
        """Update watchlist lead lifecycle fields."""


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
        self.source_followups: dict[UUID, SourceFollowupQueueItem] = {}
        self.source_queries: dict[tuple[str, str], SourceQuery] = {}
        self.research_leads: dict[UUID, ResearchLeadRecord] = {}
        self.research_briefs: dict[UUID, ResearchBriefRecord] = {}
        self.research_brief_evaluations: dict[UUID, ResearchBriefEvaluationRecord] = {}
        self.therapy_ideas: dict[UUID, TherapyIdeaRecord] = {}
        self.research_programs: dict[UUID, ResearchProgramRecord] = {}
        self.validation_decisions: dict[str, ValidationDecisionRecord] = {}
        self.public_candidates: dict[str, PublicCandidateRecord] = {}
        self.public_candidate_snapshots: dict[UUID, PublicCandidateSnapshot] = {}
        self.public_candidate_decision_events: dict[UUID, PublicCandidateDecisionEvent] = {}
        self.validation_plans: dict[UUID, ValidationPlanRecord] = {}
        self.validation_request_queue: dict[UUID, ValidationRequestQueueItem] = {}
        self.compute_jobs: dict[UUID, ComputeJobRecord] = {}
        self.command_center_board_stages: dict[str, CommandCenterBoardStageRecord] = {}
        self.command_center_activity_events: dict[UUID, CommandCenterActivityEventRecord] = {}
        self.md_expert_review_packets: dict[UUID, MDExpertReviewPacketRecord] = {}
        self.md_expert_approvals: dict[UUID, MDExpertApprovalRecord] = {}
        self.research_brief_queue: dict[UUID, ResearchBriefQueueItem] = {}
        self.hypotheses: dict[UUID, HypothesisDraft] = {}
        self.agent_runs: dict[UUID, AgentRunRecord] = {}
        self.agent_run_reviews: dict[UUID, AgentRunReviewRecord] = {}

    def get_research_object(self, object_id: UUID) -> ResearchObject | None:
        return self.research_objects.get(object_id)

    def update_research_object(self, obj: ResearchObject) -> ResearchObject | None:
        if obj.id not in self.research_objects:
            return None
        self.research_objects[obj.id] = obj
        return obj

    def list_research_objects(
        self,
        object_type: str | None = None,
        source_key: str | None = None,
        limit: int | None = None,
    ) -> list[ResearchObject]:
        objects = list(self.research_objects.values())
        if object_type:
            objects = [obj for obj in objects if str(obj.object_type) == object_type]
        if source_key:
            objects = [obj for obj in objects if obj.source_key == source_key]
        objects.sort(key=lambda obj: (obj.source_key or "", obj.title or "", str(obj.id)))
        return objects[:limit] if limit is not None else objects

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

    def list_document_chunks_for_fetch_runs(
        self,
        fetch_run_ids: list[UUID],
        limit: int | None = None,
    ) -> list[DocumentChunk]:
        return []

    def replace_document_chunks(self, object_id: UUID, chunks: list[DocumentChunk]) -> list[DocumentChunk]:
        existing_chunk_ids = {
            chunk.id for chunk in self.document_chunks.values() if chunk.research_object_id == object_id
        }
        self.document_chunks = {
            chunk_id: chunk
            for chunk_id, chunk in self.document_chunks.items()
            if chunk.research_object_id != object_id
        }
        self.entity_mentions = {
            mention_id: mention
            for mention_id, mention in self.entity_mentions.items()
            if mention.research_object_id != object_id
        }
        self.text_embeddings = {
            embedding_id: embedding
            for embedding_id, embedding in self.text_embeddings.items()
            if embedding.research_object_id != object_id and embedding.chunk_id not in existing_chunk_ids
        }
        for chunk in chunks:
            self.document_chunks[chunk.id] = chunk
        return chunks

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
        chunks = self.list_document_chunks(
            source_key=source_key,
            object_type=object_type,
        )
        live_chunk_ids = {chunk.id for chunk in chunks}
        embeddings = [
            embedding
            for embedding in self.list_text_embeddings(
                embedding_model=embedding_model,
                source_key=source_key,
                object_type=object_type,
            )
            if embedding.chunk_id in live_chunk_ids
        ]
        embedded_chunks = len({embedding.chunk_id for embedding in embeddings})
        by_model: dict[str, int] = {}
        for embedding in self.list_text_embeddings(source_key=source_key, object_type=object_type):
            if embedding.chunk_id not in live_chunk_ids:
                continue
            by_model[embedding.embedding_model] = by_model.get(embedding.embedding_model, 0) + 1
        total_chunks = len(live_chunk_ids)
        missing_chunks = max(total_chunks - embedded_chunks, 0)
        return EmbeddingCoverageSummary(
            source_key=source_key,
            object_type=object_type,
            embedding_model=embedding_model,
            total_chunks=total_chunks,
            embedded_chunks=embedded_chunks,
            missing_chunks=missing_chunks,
            coverage_ratio=embedded_chunks / total_chunks if total_chunks else 0.0,
            embedding_models=dict(sorted(by_model.items())),
        )

    def count_orphan_text_embeddings(
        self,
        *,
        embedding_model: str | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
    ) -> int:
        return len(
            [
                embedding
                for embedding in self.text_embeddings.values()
                if self._is_orphan_text_embedding(
                    embedding,
                    embedding_model=embedding_model,
                    source_key=source_key,
                    object_type=object_type,
                )
            ]
        )

    def delete_orphan_text_embeddings(
        self,
        *,
        embedding_model: str | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
    ) -> int:
        orphan_ids = [
            embedding_id
            for embedding_id, embedding in self.text_embeddings.items()
            if self._is_orphan_text_embedding(
                embedding,
                embedding_model=embedding_model,
                source_key=source_key,
                object_type=object_type,
            )
        ]
        for embedding_id in orphan_ids:
            del self.text_embeddings[embedding_id]
        return len(orphan_ids)

    def _is_orphan_text_embedding(
        self,
        embedding: TextEmbedding,
        *,
        embedding_model: str | None,
        source_key: str | None,
        object_type: str | None,
    ) -> bool:
        if embedding_model and embedding.embedding_model != embedding_model:
            return False
        if source_key and embedding.source_key != source_key:
            return False
        if object_type and str(embedding.object_type) != object_type:
            return False
        return embedding.chunk_id not in self.document_chunks

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

    def create_agent_run(self, record: AgentRunRecord) -> AgentRunRecord:
        self.agent_runs[record.agent_run_id] = record
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
        record = self.agent_runs.get(agent_run_id)
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
        self.agent_runs[agent_run_id] = updated
        return updated

    def get_agent_run(self, agent_run_id: UUID) -> AgentRunRecord | None:
        return self.agent_runs.get(agent_run_id)

    def list_agent_runs(
        self,
        *,
        agent_name: str | None = None,
        status: str | None = None,
        source_key: str | None = None,
        limit: int = 50,
    ) -> list[AgentRunRecord]:
        runs = list(self.agent_runs.values())
        if agent_name:
            runs = [run for run in runs if run.agent_name == agent_name]
        if status:
            runs = [run for run in runs if str(run.status) == status]
        if source_key:
            runs = [run for run in runs if run.source_key == source_key]
        runs.sort(key=lambda run: run.started_at, reverse=True)
        return runs[:limit]

    def create_agent_run_review(self, record: AgentRunReviewRecord) -> AgentRunReviewRecord:
        self.agent_run_reviews[record.review_id] = record
        return record

    def get_agent_run_review(self, review_id: UUID) -> AgentRunReviewRecord | None:
        return self.agent_run_reviews.get(review_id)

    def list_agent_run_reviews(
        self,
        *,
        agent_run_id: UUID | None = None,
        verdict: str | None = None,
        reviewer: str | None = None,
        limit: int = 50,
    ) -> list[AgentRunReviewRecord]:
        reviews = list(self.agent_run_reviews.values())
        if agent_run_id:
            reviews = [review for review in reviews if review.agent_run_id == agent_run_id]
        if verdict:
            reviews = [review for review in reviews if review.verdict == verdict]
        if reviewer:
            reviews = [review for review in reviews if review.reviewer == reviewer]
        reviews.sort(key=lambda review: review.created_at, reverse=True)
        return reviews[:limit]

    def upsert_research_brief(self, record: ResearchBriefRecord) -> ResearchBriefRecord:
        self.research_briefs[record.brief_id] = record
        return record

    def get_research_brief(self, brief_id: UUID) -> ResearchBriefRecord | None:
        return self.research_briefs.get(brief_id)

    def list_research_briefs(
        self,
        *,
        status: str | None = None,
        source_key: str | None = None,
        topic_query: str | None = None,
        limit: int | None = 50,
    ) -> list[ResearchBriefRecord]:
        records = list(self.research_briefs.values())
        if status:
            records = [record for record in records if record.status == status]
        if source_key:
            records = [record for record in records if record.source_key == source_key]
        if topic_query:
            normalized = topic_query.lower()
            records = [
                record
                for record in records
                if normalized in record.topic.lower() or normalized in record.disease_scope.lower()
            ]
        records.sort(key=lambda record: record.created_at, reverse=True)
        return records[:limit] if limit is not None else records

    def upsert_research_brief_evaluation(
        self,
        record: ResearchBriefEvaluationRecord,
    ) -> ResearchBriefEvaluationRecord:
        self.research_brief_evaluations[record.evaluation_id] = record
        return record

    def get_research_brief_evaluation(self, evaluation_id: UUID) -> ResearchBriefEvaluationRecord | None:
        return self.research_brief_evaluations.get(evaluation_id)

    def list_research_brief_evaluations(
        self,
        *,
        brief_id: UUID | None = None,
        readiness: str | None = None,
        passes_quality_bar: bool | None = None,
        limit: int | None = 50,
    ) -> list[ResearchBriefEvaluationRecord]:
        records = list(self.research_brief_evaluations.values())
        if brief_id:
            records = [record for record in records if record.brief_id == brief_id]
        if readiness:
            records = [record for record in records if record.readiness == readiness]
        if passes_quality_bar is not None:
            records = [record for record in records if record.passes_quality_bar == passes_quality_bar]
        records.sort(key=lambda record: record.created_at, reverse=True)
        return records[:limit] if limit is not None else records

    def upsert_therapy_idea(self, record: TherapyIdeaRecord) -> TherapyIdeaRecord:
        existing = self.therapy_ideas.get(record.therapy_idea_id)
        if existing:
            record = record.model_copy(
                update={
                    "created_at": existing.created_at,
                    "updated_at": datetime.now(UTC),
                    "metadata": {**existing.metadata, **record.metadata},
                }
            )
        self.therapy_ideas[record.therapy_idea_id] = record
        return record

    def get_therapy_idea(self, therapy_idea_id: UUID) -> TherapyIdeaRecord | None:
        return self.therapy_ideas.get(therapy_idea_id)

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
        records = list(self.therapy_ideas.values())
        if status:
            records = [record for record in records if record.status == status]
        if statuses:
            allowed = set(statuses)
            records = [record for record in records if record.status in allowed]
        if source_program_id:
            records = [record for record in records if record.source_program_id == source_program_id]
        if source_brief_id:
            records = [record for record in records if record.source_brief_id == source_brief_id]
        if source_evaluation_id:
            records = [record for record in records if record.source_evaluation_id == source_evaluation_id]
        if committee_run_id:
            records = [record for record in records if record.committee_run_id == committee_run_id]
        if topic_query:
            normalized = topic_query.lower()
            records = [
                record
                for record in records
                if normalized in record.topic.lower()
                or normalized in record.idea.title.lower()
                or normalized in record.idea.hypothesis.lower()
            ]
        records.sort(key=lambda record: (-record.score, record.updated_at))
        return records[:limit] if limit is not None else records

    def upsert_research_program(self, record: ResearchProgramRecord) -> ResearchProgramRecord:
        existing = self.research_programs.get(record.program_id)
        if existing:
            record = record.model_copy(
                update={
                    "created_at": existing.created_at,
                    "updated_at": datetime.now(UTC),
                    "metadata": {**existing.metadata, **record.metadata},
                }
            )
        self.research_programs[record.program_id] = record
        return record

    def get_research_program(self, program_id: UUID) -> ResearchProgramRecord | None:
        return self.research_programs.get(program_id)

    def list_research_programs(
        self,
        *,
        status: str | None = None,
        gate_decision: str | None = None,
        thesis_query: str | None = None,
        limit: int | None = 50,
    ) -> list[ResearchProgramRecord]:
        records = list(self.research_programs.values())
        if status:
            records = [record for record in records if record.status == status]
        if gate_decision:
            records = [record for record in records if record.gate_decision == gate_decision]
        if thesis_query:
            normalized = thesis_query.lower()
            records = [
                record
                for record in records
                if normalized in record.title.lower()
                or normalized in record.thesis.lower()
                or normalized in record.disease_model.lower()
            ]
        records.sort(key=lambda record: (record.confidence_score, record.updated_at), reverse=True)
        return records[:limit] if limit is not None else records

    def upsert_validation_decision(self, record: ValidationDecisionRecord) -> ValidationDecisionRecord:
        existing = self.validation_decisions.get(record.decision_id)
        if existing:
            record = record.model_copy(
                update={
                    "decision_record_id": existing.decision_record_id,
                    "created_at": existing.created_at,
                    "updated_at": datetime.now(UTC),
                    "metadata": {**existing.metadata, **record.metadata},
                }
            )
        self.validation_decisions[record.decision_id] = record
        return record

    def get_validation_decision(self, decision_id: str) -> ValidationDecisionRecord | None:
        return self.validation_decisions.get(decision_id)

    def list_validation_decisions(
        self,
        *,
        outcome: str | None = None,
        therapy_idea_id: UUID | None = None,
        candidate_id: str | None = None,
        limit: int | None = 50,
    ) -> list[ValidationDecisionRecord]:
        records = list(self.validation_decisions.values())
        if outcome:
            records = [record for record in records if record.outcome == outcome]
        if therapy_idea_id:
            records = [record for record in records if record.therapy_idea_id == therapy_idea_id]
        if candidate_id:
            records = [record for record in records if record.candidate_id == candidate_id]
        records.sort(key=lambda record: record.updated_at, reverse=True)
        return records[:limit] if limit is not None else records

    def upsert_public_candidate(self, record: PublicCandidateRecord) -> PublicCandidateRecord:
        existing = self.public_candidates.get(record.candidate_id)
        if existing:
            record = record.model_copy(
                update={
                    "created_at": existing.created_at,
                    "updated_at": datetime.now(UTC),
                    "metadata": {**existing.metadata, **record.metadata},
                }
            )
        self.public_candidates[record.candidate_id] = record
        return record

    def get_public_candidate(self, candidate_id: str) -> PublicCandidateRecord | None:
        return self.public_candidates.get(candidate_id)

    def list_public_candidates(self, request: PublicCandidateLibraryRequest | None = None) -> list[PublicCandidateRecord]:
        request = request or PublicCandidateLibraryRequest(limit=50)
        records = list(self.public_candidates.values())
        if request.candidate_id:
            records = [record for record in records if record.candidate_id == request.candidate_id]
        if request.therapy_idea_id:
            records = [record for record in records if record.therapy_idea_id == request.therapy_idea_id]
        if request.public_status:
            records = [record for record in records if record.public_status == request.public_status]
        if request.visibility:
            records = [record for record in records if record.visibility == request.visibility]
        if request.candidate_kind:
            records = [record for record in records if record.candidate_kind == request.candidate_kind]
        if request.query:
            query = request.query.lower()
            records = [
                record
                for record in records
                if query in record.title.lower()
                or query in record.summary.lower()
                or query in record.rationale_md.lower()
                or any(query in target.lower() for target in record.targets)
                or any(query in therapy.lower() for therapy in record.candidate_therapies)
            ]
        records.sort(key=lambda record: (record.priority_score, record.updated_at), reverse=True)
        return records[: request.limit]

    def upsert_public_candidate_snapshot(self, record: PublicCandidateSnapshot) -> PublicCandidateSnapshot:
        self.public_candidate_snapshots[record.snapshot_id] = record
        return record

    def get_public_candidate_snapshot(self, snapshot_id: UUID) -> PublicCandidateSnapshot | None:
        return self.public_candidate_snapshots.get(snapshot_id)

    def list_public_candidate_snapshots(
        self,
        *,
        candidate_id: str | None = None,
        limit: int | None = 50,
    ) -> list[PublicCandidateSnapshot]:
        records = list(self.public_candidate_snapshots.values())
        if candidate_id:
            records = [record for record in records if record.candidate_id == candidate_id]
        records.sort(key=lambda record: (record.snapshot_version, record.created_at), reverse=True)
        return records[:limit] if limit is not None else records

    def append_public_candidate_decision_event(
        self,
        record: PublicCandidateDecisionEvent,
    ) -> PublicCandidateDecisionEvent:
        self.public_candidate_decision_events[record.event_id] = record
        return record

    def list_public_candidate_decision_events(
        self,
        *,
        candidate_id: str | None = None,
        limit: int | None = 100,
    ) -> list[PublicCandidateDecisionEvent]:
        records = list(self.public_candidate_decision_events.values())
        if candidate_id:
            records = [record for record in records if record.candidate_id == candidate_id]
        records.sort(key=lambda record: record.occurred_at, reverse=True)
        return records[:limit] if limit is not None else records

    def upsert_validation_plan(self, record: ValidationPlanRecord) -> ValidationPlanRecord:
        self.validation_plans[record.plan_id] = record
        return record

    def get_validation_plan(self, plan_id: UUID) -> ValidationPlanRecord | None:
        return self.validation_plans.get(plan_id)

    def list_validation_plans(
        self,
        *,
        brief_id: UUID | None = None,
        evaluation_id: UUID | None = None,
        status: str | None = None,
        readiness: str | None = None,
        limit: int | None = 50,
    ) -> list[ValidationPlanRecord]:
        records = list(self.validation_plans.values())
        if brief_id:
            records = [record for record in records if record.brief_id == brief_id]
        if evaluation_id:
            records = [record for record in records if record.evaluation_id == evaluation_id]
        if status:
            records = [record for record in records if record.status == status]
        if readiness:
            records = [record for record in records if record.readiness == readiness]
        records.sort(key=lambda record: record.created_at, reverse=True)
        return records[:limit] if limit is not None else records

    def upsert_validation_request_queue_item(
        self,
        item: ValidationRequestQueueItem,
    ) -> ValidationRequestQueueItem:
        existing = next(
            (
                candidate
                for candidate in self.validation_request_queue.values()
                if candidate.identity_key == item.identity_key
            ),
            None,
        )
        if existing:
            item = item.model_copy(
                update={
                    "queue_item_id": existing.queue_item_id,
                    "status": existing.status,
                    "attempts": existing.attempts,
                    "last_run_id": existing.last_run_id,
                    "last_error": existing.last_error,
                    "approved_by": existing.approved_by,
                    "approval_note": existing.approval_note,
                    "created_at": existing.created_at,
                    "updated_at": datetime.now(UTC),
                    "metadata": {**existing.metadata, **item.metadata},
                }
            )
        self.validation_request_queue[item.queue_item_id] = item
        return item

    def get_validation_request_queue_item(
        self,
        queue_item_id: UUID,
    ) -> ValidationRequestQueueItem | None:
        return self.validation_request_queue.get(queue_item_id)

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
        items = list(self.validation_request_queue.values())
        if plan_id:
            items = [item for item in items if item.plan_id == plan_id]
        if status:
            items = [item for item in items if item.status == status]
        if statuses:
            allowed = set(statuses)
            items = [item for item in items if item.status in allowed]
        if source_key:
            items = [item for item in items if item.source_key == source_key]
        if task_type:
            items = [item for item in items if item.task_type == task_type]
        if topic_query:
            normalized = topic_query.lower()
            items = [
                item
                for item in items
                if normalized in item.topic.lower()
                or normalized in item.title.lower()
                or normalized in item.objective.lower()
            ]
        items.sort(key=lambda item: (item.priority, item.created_at))
        return items[:limit] if limit is not None else items

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
        item = self.validation_request_queue.get(queue_item_id)
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
        self.validation_request_queue[queue_item_id] = updated
        return updated

    def upsert_compute_job(self, record: ComputeJobRecord) -> ComputeJobRecord:
        self.compute_jobs[record.compute_job_id] = record
        return record

    def get_compute_job(self, compute_job_id: UUID) -> ComputeJobRecord | None:
        return self.compute_jobs.get(compute_job_id)

    def list_compute_jobs(
        self,
        *,
        status: str | None = None,
        runner_kind: str | None = None,
        queue_item_id: UUID | None = None,
        limit: int | None = 50,
    ) -> list[ComputeJobRecord]:
        records = list(self.compute_jobs.values())
        if status:
            records = [record for record in records if record.status == status]
        if runner_kind:
            records = [record for record in records if record.runner_kind == runner_kind]
        if queue_item_id:
            records = [record for record in records if record.queue_item_id == queue_item_id]
        records.sort(key=lambda record: record.updated_at, reverse=True)
        return records[:limit] if limit is not None else records

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
        record = self.compute_jobs.get(compute_job_id)
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
        self.compute_jobs[compute_job_id] = updated
        return updated

    def upsert_command_center_board_stage(
        self,
        record: CommandCenterBoardStageRecord,
    ) -> CommandCenterBoardStageRecord:
        existing = self.command_center_board_stages.get(record.stage_key)
        if existing is not None:
            record = record.model_copy(update={"created_at": existing.created_at})
        self.command_center_board_stages[record.stage_key] = record
        return record

    def get_command_center_board_stage(
        self,
        entity_type: str,
        entity_id: str,
    ) -> CommandCenterBoardStageRecord | None:
        return self.command_center_board_stages.get(f"{entity_type}:{entity_id}")

    def list_command_center_board_stages(
        self,
        *,
        entity_type: str | None = None,
        board_stage: str | None = None,
        limit: int | None = 500,
    ) -> list[CommandCenterBoardStageRecord]:
        records = list(self.command_center_board_stages.values())
        if entity_type:
            records = [record for record in records if record.entity_type == entity_type]
        if board_stage:
            records = [record for record in records if record.board_stage == board_stage]
        records.sort(key=lambda record: record.updated_at, reverse=True)
        return records[:limit] if limit is not None else records

    def append_command_center_activity_event(
        self,
        record: CommandCenterActivityEventRecord,
    ) -> CommandCenterActivityEventRecord:
        self.command_center_activity_events[record.event_id] = record
        return record

    def list_command_center_activity_events(
        self,
        *,
        entity_type: str | None = None,
        entity_id: str | None = None,
        source: str | None = None,
        limit: int | None = 100,
    ) -> list[CommandCenterActivityEventRecord]:
        records = list(self.command_center_activity_events.values())
        if entity_type:
            records = [record for record in records if record.entity_type == entity_type]
        if entity_id:
            records = [record for record in records if record.entity_id == entity_id]
        if source:
            records = [record for record in records if record.source == source]
        records.sort(key=lambda record: record.occurred_at, reverse=True)
        return records[:limit] if limit is not None else records

    def upsert_md_expert_review_packet(
        self,
        record: MDExpertReviewPacketRecord,
    ) -> MDExpertReviewPacketRecord:
        self.md_expert_review_packets[record.packet_id] = record
        return record

    def get_md_expert_review_packet(self, packet_id: UUID) -> MDExpertReviewPacketRecord | None:
        return self.md_expert_review_packets.get(packet_id)

    def get_md_expert_review_packet_by_hash(self, packet_hash: str) -> MDExpertReviewPacketRecord | None:
        return next(
            (
                record
                for record in self.md_expert_review_packets.values()
                if record.packet_hash == packet_hash
            ),
            None,
        )

    def list_md_expert_review_packets(
        self,
        *,
        packet_hash: str | None = None,
        status: str | None = None,
        limit: int | None = 50,
    ) -> list[MDExpertReviewPacketRecord]:
        records = list(self.md_expert_review_packets.values())
        if packet_hash:
            records = [record for record in records if record.packet_hash == packet_hash]
        if status:
            records = [record for record in records if record.status == status]
        records.sort(key=lambda record: record.updated_at, reverse=True)
        return records[:limit] if limit is not None else records

    def upsert_md_expert_approval(self, record: MDExpertApprovalRecord) -> MDExpertApprovalRecord:
        self.md_expert_approvals[record.approval_id] = record
        packet = self.get_md_expert_review_packet(record.packet_id)
        if packet is not None:
            self.md_expert_review_packets[packet.packet_id] = packet.model_copy(
                update={"status": record.decision, "updated_at": datetime.now(UTC)}
            )
        return record

    def list_md_expert_approvals(
        self,
        *,
        packet_hash: str | None = None,
        decision: str | None = None,
        limit: int | None = 50,
    ) -> list[MDExpertApprovalRecord]:
        records = list(self.md_expert_approvals.values())
        if packet_hash:
            records = [record for record in records if record.packet_hash == packet_hash]
        if decision:
            records = [record for record in records if record.decision == decision]
        records.sort(key=lambda record: record.reviewed_at, reverse=True)
        return records[:limit] if limit is not None else records

    def upsert_research_brief_queue_item(self, item: ResearchBriefQueueItem) -> ResearchBriefQueueItem:
        existing = next(
            (
                candidate
                for candidate in self.research_brief_queue.values()
                if candidate.identity_key == item.identity_key
            ),
            None,
        )
        if existing:
            item = item.model_copy(
                update={
                    "queue_item_id": existing.queue_item_id,
                    "status": existing.status,
                    "attempts": existing.attempts,
                    "last_brief_id": existing.last_brief_id,
                    "last_agent_run_id": existing.last_agent_run_id,
                    "last_error": existing.last_error,
                    "created_at": existing.created_at,
                    "updated_at": datetime.now(UTC),
                    "metadata": {**existing.metadata, **item.metadata},
                }
            )
        self.research_brief_queue[item.queue_item_id] = item
        return item

    def get_research_brief_queue_item(self, queue_item_id: UUID) -> ResearchBriefQueueItem | None:
        return self.research_brief_queue.get(queue_item_id)

    def list_research_brief_queue_items(
        self,
        *,
        status: str | None = None,
        statuses: list[str] | None = None,
        source_key: str | None = None,
        topic_query: str | None = None,
        limit: int | None = 50,
    ) -> list[ResearchBriefQueueItem]:
        items = list(self.research_brief_queue.values())
        if status:
            items = [item for item in items if item.status == status]
        if statuses:
            allowed = set(statuses)
            items = [item for item in items if item.status in allowed]
        if source_key:
            items = [item for item in items if item.source_key == source_key]
        if topic_query:
            normalized = topic_query.lower()
            items = [
                item
                for item in items
                if normalized in item.topic.lower() or normalized in item.disease_scope.lower()
            ]
        items.sort(key=lambda item: (item.priority, item.created_at))
        return items[:limit] if limit is not None else items

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
        item = self.research_brief_queue.get(queue_item_id)
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
        self.research_brief_queue[queue_item_id] = updated
        return updated

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

    def upsert_source_followup(self, item: SourceFollowupQueueItem) -> SourceFollowupQueueItem:
        existing = next(
            (
                candidate
                for candidate in self.source_followups.values()
                if candidate.identity_key == item.identity_key
            ),
            None,
        )
        if existing:
            item = item.model_copy(
                update={
                    "followup_id": existing.followup_id,
                    "status": existing.status,
                    "attempts": existing.attempts,
                    "last_error": existing.last_error,
                    "created_at": existing.created_at,
                    "updated_at": datetime.now(UTC),
                }
            )
        self.source_followups[item.followup_id] = item
        return item

    def get_source_followup(self, followup_id: UUID) -> SourceFollowupQueueItem | None:
        return self.source_followups.get(followup_id)

    def list_source_followups(
        self,
        *,
        source_key: str | None = None,
        status: str | None = None,
        statuses: list[str] | None = None,
        identifier_type: str | None = None,
        limit: int | None = None,
    ) -> list[SourceFollowupQueueItem]:
        items = list(self.source_followups.values())
        if source_key:
            items = [item for item in items if item.source_key == source_key]
        if status:
            items = [item for item in items if item.status == status]
        if statuses:
            allowed = set(statuses)
            items = [item for item in items if item.status in allowed]
        if identifier_type:
            items = [item for item in items if item.identifier_type == identifier_type]
        items.sort(key=lambda item: (item.priority, item.created_at))
        return items[:limit] if limit is not None else items

    def update_source_followup(
        self,
        followup_id: UUID,
        *,
        status: str,
        attempts: int | None = None,
        last_error: str | None = None,
        metadata: dict | None = None,
    ) -> SourceFollowupQueueItem | None:
        item = self.source_followups.get(followup_id)
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
        self.source_followups[followup_id] = updated
        return updated

    def upsert_source_query(self, query: SourceQuery) -> SourceQuery:
        self.source_queries[(query.source_key, query.query_name)] = query
        return query

    def list_source_queries(self, source_key: str | None = None, active_only: bool = True) -> list[SourceQuery]:
        queries = list(self.source_queries.values())
        if source_key is not None:
            queries = [query for query in queries if query.source_key == source_key]
        if active_only:
            queries = [query for query in queries if query.active]
        return sorted(queries, key=lambda query: (query.source_key, query.query_name))

    def upsert_research_lead(self, lead: ResearchLeadRecord) -> ResearchLeadRecord:
        existing = next(
            (
                candidate
                for candidate in self.research_leads.values()
                if candidate.identity_key == lead.identity_key
            ),
            None,
        )
        if existing:
            lead = lead.model_copy(
                update={
                    "lead_id": existing.lead_id,
                    "status": existing.status,
                    "created_at": existing.created_at,
                    "updated_at": datetime.now(UTC),
                    "metadata": {**existing.metadata, **lead.metadata},
                }
            )
        self.research_leads[lead.lead_id] = lead
        return lead

    def get_research_lead(self, lead_id: UUID) -> ResearchLeadRecord | None:
        return self.research_leads.get(lead_id)

    def list_research_leads(
        self,
        *,
        status: str | None = None,
        statuses: list[str] | None = None,
        lead_type: str | None = None,
        source_key: str | None = None,
        limit: int | None = None,
    ) -> list[ResearchLeadRecord]:
        leads = list(self.research_leads.values())
        if status:
            leads = [lead for lead in leads if lead.status == status]
        if statuses:
            allowed = set(statuses)
            leads = [lead for lead in leads if lead.status in allowed]
        if lead_type:
            leads = [lead for lead in leads if lead.lead_type == lead_type]
        if source_key:
            leads = [lead for lead in leads if lead.source_key == source_key or lead.origin_source_key == source_key]
        leads.sort(key=lambda lead: (lead.priority, lead.created_at))
        return leads[:limit] if limit is not None else leads

    def update_research_lead(
        self,
        lead_id: UUID,
        *,
        status: str | None = None,
        metadata: dict | None = None,
    ) -> ResearchLeadRecord | None:
        lead = self.research_leads.get(lead_id)
        if lead is None:
            return None
        updated = lead.model_copy(
            update={
                "status": lead.status if status is None else status,
                "updated_at": datetime.now(UTC),
                "metadata": {**lead.metadata, **(metadata or {})},
            }
        )
        self.research_leads[lead_id] = updated
        return updated


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


_LOW_SIGNAL_RETRIEVAL_TERMS = {
    "angiosarcoma",
    "canine",
    "cell",
    "cells",
    "dog",
    "dogs",
    "hemangiosarcoma",
    "human",
    "humans",
    "line",
    "lines",
    "sarcoma",
    "tissue",
    "tissues",
}

_HIGH_SIGNAL_RETRIEVAL_TERMS = {
    "cd31",
    "doxorubicin",
    "flt4",
    "kit",
    "kdr",
    "paclitaxel",
    "pazopanib",
    "pdgfr",
    "pik3ca",
    "propranolol",
    "sirolimus",
    "sorafenib",
    "toceranib",
    "vegf",
    "vegfa",
    "vegfr",
    "vegfr2",
}

_EVIDENCE_SIGNAL_RETRIEVAL_TERMS = {
    "auc",
    "bioavailability",
    "cmax",
    "dose",
    "limiting",
    "pharmacodynamic",
    "pharmacokinetic",
    "response",
    "safety",
    "survival",
    "toxicity",
    "tolerability",
}


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
    weights = {term: _retrieval_term_weight(term) for term in terms}
    matched_weight = sum(weight for term, weight in weights.items() if term in haystack)
    if matched_weight <= 0.0:
        return 0.0
    total_weight = sum(weights.values()) or float(len(terms))

    query_text = " ".join(query.lower().split())
    phrase_bonus = 0.2 if query_text and query_text in haystack else 0.0
    title_weight = sum(weight for term, weight in weights.items() if term in title.lower())
    title_bonus = min((title_weight / total_weight) * 0.2, 0.2)
    score = (matched_weight / total_weight) * 0.72 + phrase_bonus + title_bonus
    return min(score, 1.0)


def _retrieval_term_weight(term: str) -> float:
    if term in _HIGH_SIGNAL_RETRIEVAL_TERMS:
        return 1.9
    if term in _EVIDENCE_SIGNAL_RETRIEVAL_TERMS:
        return 1.2
    if term in _LOW_SIGNAL_RETRIEVAL_TERMS:
        return 0.55
    if len(term) >= 8:
        return 1.25
    return 1.0
