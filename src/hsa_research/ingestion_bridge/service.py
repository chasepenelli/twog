"""Service layer for Ingestion Bridge v2.

The service is the boundary shared by MCP tools, Dagster assets, the future
TWOG dashboard, and tests. Keep business rules here rather than inside MCP
decorators or Dagster assets.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import hashlib
import os
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from .contracts import (
    AgentFindingEscalationRequest,
    AgentFindingEscalationResult,
    AgentPerformanceEvaluationRequest,
    AgentPerformanceEvaluationResult,
    AgentPerformanceReportRequest,
    AgentPerformanceReportResult,
    AgentRunRecord,
    AgentRunReviewRecord,
    AsyncRunHandle,
    BoltzRunRequest,
    CandidateDossier,
    CandidateDossierRequest,
    ChunkContextRequest,
    ChunkContextResult,
    ClaimCurationRequest,
    ClaimCurationResult,
    ClaimSearchRequest,
    ClaimSearchResults,
    CommandCenterRecommendation,
    CommandCenterRequest,
    CommandCenterResult,
    CommitHypothesisRequest,
    DoiOpenAccessFollowupQueueRequest,
    DocumentChunk,
    EvidenceGapResolverRequest,
    EvidenceGapResolverResult,
    FullTextTriageRequest,
    FullTextTriageResult,
    FullTextOpsRequest,
    FullTextOpsResult,
    HypothesisDraft,
    HypothesisProposalRequest,
    ModelProfile,
    PubMedIdentifierRepairRequest,
    PubMedIdentifierRepairResult,
    ResearchBriefEvaluationRecord,
    ResearchBriefEvaluationRequest,
    ResearchBriefEvaluationResult,
    ResearchBriefFollowupQueueRequest,
    ResearchBriefFollowupQueueResult,
    ResearchBriefQualityReportRequest,
    ResearchBriefQualityReportResult,
    ResearchBriefQualityRow,
    ResearchBriefQueueBatchRequest,
    ResearchBriefQueueBatchResult,
    ResearchBriefQueueItem,
    ResearchBriefQueueMaintenanceRequest,
    ResearchBriefQueueMaintenanceResult,
    ResearchBriefQueueRequest,
    ResearchBriefQueueRunRequest,
    ResearchBriefQueueRunResult,
    ResearchBriefRecord,
    ResearchBriefPlaygroundPack,
    ResearchBriefRequest,
    ResearchBriefResult,
    ResearchFollowupRefinementRequest,
    ResearchFollowupRefinementResult,
    ResearchChunkSearchRequest,
    ResearchChunkSearchResult,
    ResearchChunkSearchResults,
    ResearchFollowupResolverRequest,
    ResearchFollowupResolverResult,
    ResearchFollowupLoopRequest,
    ResearchFollowupLoopResult,
    ResearchLeadCollectRequest,
    ResearchLeadCollectResult,
    ResearchLeadRecord,
    ResearchObject,
    ResearchObjectReadRequest,
    ResearchObjectReadResult,
    RetrievalSmokeRequest,
    RetrievalSmokeResult,
    SourceScoutRequest,
    SourceScoutResult,
    SourceFollowupIngestRequest,
    SourceFollowupIngestResult,
    SourceFollowupQueueRequest,
    SourceFollowupQueueResult,
    SourceFollowupQueueItem,
    TextEmbeddingSearchRequest,
    TherapyCommitteeRequest,
    TherapyCommitteeResult,
    TherapyCommitteeValidationQueueRequest,
    TherapyCommitteeValidationQueueResult,
    TherapyIdea,
    ValidationAgentResult,
    ValidationAssayContext,
    ValidationAutopilotQueueRecord,
    ValidationAutopilotRequest,
    ValidationAutopilotResult,
    ValidationGapSourceIngestRequest,
    ValidationGapSourceIngestResult,
    ValidationGapSourcePackRequest,
    ValidationGapSourcePackResult,
    ValidationPlanRecord,
    ValidationPlanRequest,
    ValidationPlanResult,
    ValidationPlanTask,
    ValidationRequest,
    ValidationRequestQueueItem,
    ValidationRequestQueueRequest,
    ValidationRequestQueueResult,
    XLinkedArticleReviewRequest,
    XLinkedArticleReviewResult,
    XTopicReviewRequest,
    XTopicReviewResult,
    XLinkedArticleFollowupRequest,
    XLinkedArticleFollowupResult,
)
from .agent_finding_escalation import (
    AGENT_FINDING_ESCALATION_AGENT_NAME,
    AGENT_FINDING_ESCALATION_AGENT_VERSION,
    escalate_agent_findings,
    summarize_agent_finding_escalation,
)
from .agent_performance import (
    AGENT_PERFORMANCE_EVALUATOR_AGENT_NAME,
    AGENT_PERFORMANCE_EVALUATOR_AGENT_VERSION,
    build_agent_performance_report,
    run_agent_performance_evaluation,
    summarize_agent_performance_evaluation,
)
from .agent_runner import AgentRunner
from .claim_curator import ClaimCuratorAgent
from .claim_extractor import extract_claims_for_chunks
from .embeddings import LOCAL_HASH_EMBEDDING_MODEL, build_embedding_provider, select_embedding_model_from_coverage
from .evidence_fit import assess_research_followup_ingest_evidence_fit
from .evidence_gap_resolver import (
    EVIDENCE_GAP_RESOLVER_AGENT_NAME,
    EVIDENCE_GAP_RESOLVER_AGENT_VERSION,
    resolve_evidence_gaps,
    summarize_evidence_gap_resolver,
)
from .full_text_ops import FULL_TEXT_OPS_AGENT_NAME, FULL_TEXT_OPS_AGENT_VERSION, FullTextOpsAgent
from .full_text_triage import FullTextTriageAgent
from .research_brief_agent import (
    PERSPECTIVE_ORDER,
    RESEARCH_BRIEF_AGENT_VERSION,
    RESEARCH_SYNTHESIS_EDITOR_AGENT_NAME,
    ResearchBriefAgent,
    summarize_perspective_report,
    summarize_research_brief,
)
from .research_brief_evaluation import (
    RESEARCH_BRIEF_EVALUATION_AGENT_NAME,
    RESEARCH_BRIEF_EVALUATION_AGENT_VERSION,
    evaluate_research_brief_synthesis,
    summarize_research_brief_evaluation,
)
from .research_brief_errors import split_research_brief_errors
from .repository import ResearchRepository
from .research_leads import collect_research_leads_from_agent_runs, persist_research_leads_from_agent_result
from .research_followup_resolver import (
    RESEARCH_FOLLOWUP_RESOLVER_AGENT_NAME,
    RESEARCH_FOLLOWUP_RESOLVER_AGENT_VERSION,
    resolve_research_followup_leads,
    summarize_research_followup_resolver,
)
from .research_followup_refinement import (
    RESEARCH_FOLLOWUP_REFINEMENT_AGENT_NAME,
    RESEARCH_FOLLOWUP_REFINEMENT_AGENT_VERSION,
    refine_research_followups,
    summarize_research_followup_refinement,
)
from .pubmed_identifier_repair import repair_pubmed_identifier_metadata
from .validation_gap_source_pack import (
    VALIDATION_GAP_SOURCE_PACK_AGENT_NAME,
    VALIDATION_GAP_SOURCE_PACK_AGENT_VERSION,
    build_validation_gap_source_pack,
    summarize_validation_gap_source_pack,
)
from .validation_gap_ingest import ingest_validation_gap_source_queries
from .therapy_committee import (
    THERAPY_COMMITTEE_AGENT_NAME,
    THERAPY_COMMITTEE_AGENT_VERSION,
    run_therapy_committee,
    summarize_therapy_committee,
)
from .validation_planning import (
    VALIDATION_PLANNING_AGENT_NAME,
    VALIDATION_PLANNING_AGENT_VERSION,
    plan_validation_from_research_brief,
    summarize_validation_plan,
    validation_plan_record_from_result,
)
from .validation_agents import (
    VALIDATION_AGENT_MODEL_PROFILE,
    VALIDATION_AGENT_VERSION,
    run_validation_agent,
    summarize_validation_agent_result,
    validation_agent_name,
)
from .source_scout import SourceScoutAgent
from .source_followup import (
    ingest_source_followups,
    queue_source_followups_from_scrape_reviews,
    queue_unpaywall_doi_followups,
)
from .storage import build_research_repository
from .x_linked_article_review import (
    X_LINKED_ARTICLE_REVIEW_AGENT_NAME,
    X_LINKED_ARTICLE_REVIEW_AGENT_VERSION,
    XLinkedArticleReviewAgent,
)
from .x_topic_review import X_TOPIC_REVIEW_AGENT_NAME, X_TOPIC_REVIEW_AGENT_VERSION, XTopicReviewAgent
from .x_linked_article_followup import run_x_linked_article_followup


DEFAULT_MODEL_PROFILES: dict[str, ModelProfile] = {
    "extractor": ModelProfile(profile_key="extractor", provider="claude", purpose="Claim and entity extraction"),
    "hypothesis": ModelProfile(profile_key="hypothesis", provider="openai", purpose="Hypothesis drafting"),
    "reviewer": ModelProfile(
        profile_key="reviewer",
        provider="other",
        model_name="openrouter:compare",
        purpose="Scientific and operational review through external ChatGPT Pro or OpenRouter",
    ),
    "cheap_classifier": ModelProfile(profile_key="cheap_classifier", provider="openai", purpose="Fast classification"),
    "long_context_reviewer": ModelProfile(
        profile_key="long_context_reviewer",
        provider="claude",
        purpose="Long-context literature review",
    ),
    "research_brief": ModelProfile(
        profile_key="research_brief",
        provider="other",
        model_name="openrouter:~anthropic/claude-sonnet-latest",
        purpose="Citation-first multi-perspective research briefs",
    ),
    "synthesis_quality_evaluator": ModelProfile(
        profile_key="synthesis_quality_evaluator",
        provider="local",
        purpose="Reproducible quality review for generated research briefs",
    ),
    "validation_planner": ModelProfile(
        profile_key="validation_planner",
        provider="local",
        purpose="Recommend-only hypothesis and validation planning from evaluated briefs",
    ),
    "therapy_committee": ModelProfile(
        profile_key="therapy_committee",
        provider="other",
        model_name="openrouter:~anthropic/claude-sonnet-latest",
        purpose="Multi-perspective therapy ideation committee over cited evidence",
    ),
}

VALIDATION_AUTOPILOT_AGENT_NAME = "validation_autopilot_agent"
VALIDATION_AUTOPILOT_AGENT_VERSION = "v1"
VALIDATION_AUTOPILOT_APPROVED_BY = "validation_autopilot"
VALIDATION_AUTOPILOT_POLICY_VERSION = "v1"


@dataclass
class _SourceFollowupLinkStats:
    followup_ids: list[UUID] = field(default_factory=list)
    ingestable_followup_ids: list[UUID] = field(default_factory=list)
    linked: int = 0
    newly_queued: int = 0
    preexisting: int = 0
    already_ingested: int = 0
    pending: int = 0


def _validation_request_quality_gates(request: ValidationRequest) -> list[str]:
    gates = ["human_approval_required" if request.require_approval else "operator_dispatch_recorded"]
    if request.assay_context is not None:
        gates.append("assay_context_present")
    if request.validation_type in {"boltz", "docking", "md", "homology"}:
        gates.append("target_identity_required")
    if request.validation_type in {"docking", "admet", "safety"}:
        gates.append("candidate_identity_required")
    if request.validation_type in {"admet", "safety"}:
        gates.append("safety_context_required")
    if request.validation_type == "omics":
        gates.append("omics_dataset_context_required")
    if request.validation_type != "expert_review":
        gates.append("species_and_disease_context_required")
    return _dedupe_quality_labels([*request.quality_gates, *gates])


def _validation_request_dispatch_blockers(request: ValidationRequest) -> list[str]:
    if request.validation_type == "expert_review":
        return []

    blockers: list[str] = []
    if request.validation_type in {"boltz", "docking", "md", "homology"} and not request.target_name:
        blockers.append("target_name_required")
    if request.validation_type in {"docking", "admet", "safety"} and not (
        request.candidate_id or request.candidate_name
    ):
        blockers.append("candidate_identity_required")

    context = request.assay_context
    if context is None:
        blockers.append("assay_context_required")
        return blockers

    if not context.disease_context:
        blockers.append("disease_context_required")
    if not context.species:
        blockers.append("species_context_required")
    if request.validation_type in {"admet", "safety"} and not context.safety_context:
        blockers.append("safety_context_required")
    if request.validation_type in {"docking", "boltz", "md", "homology", "omics"} and not context.model_system:
        blockers.append("model_system_required")
    return _dedupe_quality_labels(blockers)


def _dedupe_quality_labels(labels: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for label in labels:
        normalized = str(label).strip()
        key = normalized.casefold()
        if normalized and key not in seen:
            deduped.append(normalized)
            seen.add(key)
    return deduped


def summarize_validation_autopilot_result(result: ValidationAutopilotResult) -> dict[str, Any]:
    return {
        "dry_run": result.dry_run,
        "selected_count": result.selected_count,
        "dispatched_count": result.dispatched_count,
        "skipped_count": result.skipped_count,
        "blocker_count": len(result.blockers),
        "estimated_cost_usd": result.estimated_cost_usd,
        "actual_cost_usd": result.actual_cost_usd,
    }


def _summarize_research_followup_loop(result: ResearchFollowupLoopResult) -> dict[str, Any]:
    evidence_fit = result.evidence_fit
    return {
        "lead_id": str(result.lead_id),
        "lead_status_before": result.lead_status_before,
        "lead_status_after": result.lead_status_after,
        "dry_run": result.dry_run,
        "query_count": result.query_count,
        "raw_records": result.raw_records,
        "document_chunks": result.document_chunks,
        "source_followups_linked": result.source_followups_linked,
        "source_followups_queued": result.source_followups_queued,
        "source_followups_newly_queued": result.source_followups_newly_queued,
        "source_followups_preexisting": result.source_followups_preexisting,
        "source_followups_already_ingested": result.source_followups_already_ingested,
        "source_followups_pending": result.source_followups_pending,
        "source_followups_ingested": result.source_followups_ingested,
        "source_followups_ingested_this_run": result.source_followups_ingested_this_run,
        "source_followup_document_chunks": result.source_followup_document_chunks,
        "claim_chunks_seen": result.claim_chunks_seen,
        "claims_extracted": result.claims_extracted,
        "claims_written": result.claims_written,
        "evidence_fit": evidence_fit.fit if evidence_fit else None,
        "target_safety_fit": evidence_fit.target_safety_fit if evidence_fit else None,
        "disease_directness_fit": evidence_fit.disease_directness_fit if evidence_fit else None,
        "actionability": evidence_fit.actionability if evidence_fit else None,
        "transfer_risk": evidence_fit.transfer_risk if evidence_fit else None,
        "overall_fit": evidence_fit.overall_fit if evidence_fit else None,
        "latest_evaluator_verdict": result.latest_evaluator_verdict,
        "estimated_cost_usd": result.estimated_cost_usd,
        "actual_cost_usd": result.actual_cost_usd,
        "errors": len(result.errors),
    }


def _agent_run_review_cost_usd(review: AgentRunReviewRecord) -> float:
    metadata = review.metadata or {}
    evaluation = metadata.get("agent_performance_evaluation") if isinstance(metadata, dict) else {}
    model_metadata = evaluation.get("model_metadata") if isinstance(evaluation, dict) else {}
    usage = model_metadata.get("usage") if isinstance(model_metadata, dict) else {}
    if not isinstance(usage, dict):
        usage = {}
    for value in (
        usage.get("cost"),
        usage.get("total_cost"),
        usage.get("cost_usd"),
        model_metadata.get("cost") if isinstance(model_metadata, dict) else None,
        model_metadata.get("total_cost") if isinstance(model_metadata, dict) else None,
        model_metadata.get("cost_usd") if isinstance(model_metadata, dict) else None,
    ):
        parsed = _float_or_none(value)
        if parsed is not None:
            return parsed
    return 0.0


def _requires_openrouter_for_model_profile(model_profile: str) -> bool:
    normalized = model_profile.strip()
    return normalized not in {"deterministic_only", "external_required"}


def _validation_autopilot_record(
    item: ValidationRequestQueueItem,
    *,
    reason: str,
    cost_usd: float | None = None,
) -> ValidationAutopilotQueueRecord:
    result_payload = (item.metadata or {}).get("validation_agent_result") or {}
    return ValidationAutopilotQueueRecord(
        queue_item_id=item.queue_item_id,
        plan_id=item.plan_id,
        task_id=item.task_id,
        status=item.status,
        priority=item.priority,
        task_type=item.task_type,
        validation_type=item.validation_request.validation_type,
        title=item.title,
        source_key=item.source_key,
        reason=reason,
        decision=result_payload.get("decision"),
        agent_run_id=item.last_run_id,
        cost_usd=cost_usd,
        last_error=item.last_error,
    )


def _validation_manual_activity_at(items: list[ValidationRequestQueueItem]) -> datetime | None:
    activity_times: list[datetime] = []
    for item in items:
        if not item.approved_by or item.approved_by == VALIDATION_AUTOPILOT_APPROVED_BY:
            continue
        metadata = item.metadata or {}
        for key in ("approved_at", "dispatched_at", "completed_at", "dispatch_failed_at", "dispatch_blocked_at"):
            value = _parse_datetime(metadata.get(key))
            if value is not None:
                activity_times.append(value)
        activity_times.append(_coerce_utc(item.updated_at))
    return max(activity_times) if activity_times else None


def _validation_autopilot_spend_since(items: list[ValidationRequestQueueItem], since: datetime) -> float:
    total = 0.0
    for item in items:
        metadata = item.metadata or {}
        autopilot = metadata.get("validation_autopilot") or {}
        if not isinstance(autopilot, dict):
            continue
        dispatched_at = _parse_datetime(autopilot.get("dispatched_at")) or _parse_datetime(metadata.get("dispatched_at"))
        if dispatched_at is None or dispatched_at < since:
            continue
        total += _queue_item_cost_usd(item) or _float_or_zero(autopilot.get("actual_cost_usd"))
    return round(total, 6)


def _queue_item_cost_usd(item: ValidationRequestQueueItem) -> float | None:
    result_payload = (item.metadata or {}).get("validation_agent_result") or {}
    if not isinstance(result_payload, dict):
        return None
    raw_response = result_payload.get("raw_response") or {}
    provider_metadata = raw_response.get("provider_metadata") if isinstance(raw_response, dict) else {}
    if not isinstance(provider_metadata, dict):
        provider_metadata = {}
    usage = provider_metadata.get("usage") if isinstance(provider_metadata.get("usage"), dict) else {}
    for value in (
        usage.get("cost"),
        usage.get("total_cost"),
        usage.get("cost_usd"),
        provider_metadata.get("cost"),
        provider_metadata.get("total_cost"),
        provider_metadata.get("cost_usd"),
    ):
        parsed = _float_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _coerce_utc(value)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _coerce_utc(parsed)


def _coerce_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


def _float_or_zero(value: Any) -> float:
    return _float_or_none(value) or 0.0


class HSAResearchService:
    """Typed use-case service for research actions."""

    def __init__(
        self,
        repository: ResearchRepository | None = None,
        model_profiles: dict[str, ModelProfile] | None = None,
    ) -> None:
        self.repository = repository or build_default_repository()
        self.model_profiles = model_profiles or DEFAULT_MODEL_PROFILES

    def search_claims(self, request: ClaimSearchRequest) -> ClaimSearchResults:
        results = self.repository.search_claims(request)
        return ClaimSearchResults(results=results, total=len(results), query=request)

    def get_claim(self, claim_id: UUID):
        return self.repository.get_claim(claim_id)

    def search_research_chunks(self, request: ResearchChunkSearchRequest) -> ResearchChunkSearchResults:
        embedding_model = self._select_embedding_model(request)
        embedding_results = self._search_chunks_with_embeddings(request, embedding_model)
        if embedding_results:
            keyword_results: list[ResearchChunkSearchResult] = []
            if request.include_keyword_fallback:
                keyword_results = [
                    self._truncate_search_result(result, request.max_chunk_chars)
                    for result in self.repository.search_research_chunks(request)
                ]
            if keyword_results:
                embedding_results = self._merge_embedding_and_keyword_results(
                    embedding_results,
                    keyword_results,
                    request.limit,
                )
            return ResearchChunkSearchResults(
                results=embedding_results,
                total=len(embedding_results),
                query=request,
                search_mode="embedding",
                embedding_model=embedding_model,
            )

        keyword_results: list[ResearchChunkSearchResult] = []
        if request.include_keyword_fallback:
            keyword_results = [
                self._truncate_search_result(result, request.max_chunk_chars)
                for result in self.repository.search_research_chunks(request)
            ]
        return ResearchChunkSearchResults(
            results=keyword_results,
            total=len(keyword_results),
            query=request,
            search_mode="keyword" if keyword_results else "none",
            embedding_model=embedding_model,
        )

    def get_chunk_context(self, request: ChunkContextRequest) -> ChunkContextResult | None:
        chunk = self.repository.get_document_chunk(request.chunk_id)
        if chunk is None:
            return None

        research_object = self.repository.get_research_object(chunk.research_object_id)
        sibling_chunks = self.repository.list_document_chunks(object_id=chunk.research_object_id)
        sibling_chunks.sort(key=lambda item: item.chunk_index)
        before = [
            item
            for item in sibling_chunks
            if item.chunk_index < chunk.chunk_index
        ][-request.window :]
        after = [
            item
            for item in sibling_chunks
            if item.chunk_index > chunk.chunk_index
        ][: request.window]
        mentions = (
            self.repository.list_entity_mentions(chunk_id=chunk.id)
            if request.include_entity_mentions
            else []
        )
        return ChunkContextResult(
            chunk=_truncate_chunk(chunk, request.max_chunk_chars),
            research_object=research_object,
            before_chunks=[_truncate_chunk(item, request.max_chunk_chars) for item in before],
            after_chunks=[_truncate_chunk(item, request.max_chunk_chars) for item in after],
            entity_mentions=mentions,
        )

    def get_research_object(self, request: ResearchObjectReadRequest) -> ResearchObjectReadResult | None:
        research_object = self.repository.get_research_object(request.research_object_id)
        if research_object is None:
            return None

        chunks: list[DocumentChunk] = []
        if request.include_chunks and request.max_chunks > 0:
            chunks = [
                _truncate_chunk(chunk, request.max_chunk_chars)
                for chunk in self.repository.list_document_chunks(
                    object_id=request.research_object_id,
                    limit=request.max_chunks,
                )
            ]
        return ResearchObjectReadResult(research_object=research_object, chunks=chunks)

    def run_research_brief(self, request: ResearchBriefRequest) -> ResearchBriefResult:
        agent = ResearchBriefAgent(self.repository)
        evidence = agent.build_evidence(request)
        runner = AgentRunner(self.repository)
        perspective_reports = [
            runner.run(
                agent_name=agent_name,
                agent_version=RESEARCH_BRIEF_AGENT_VERSION,
                model_profile=request.model_profile,
                input_payload={
                    "request": request.model_dump(mode="json"),
                    "perspective": perspective,
                    "citation_count": len(evidence.citations),
                    "claim_count": len(evidence.claims),
                },
                execute=lambda perspective=perspective: agent.run_perspective(
                    request,
                    perspective=perspective,
                    evidence=evidence,
                ),
                dagster_run_id=request.dagster_run_id,
                metadata=request.metadata,
                summarize=summarize_perspective_report,
            )
            for perspective, agent_name in (
                (perspective, f"{perspective}_agent")
                for perspective in PERSPECTIVE_ORDER
            )
        ]
        result = runner.run(
            agent_name=RESEARCH_SYNTHESIS_EDITOR_AGENT_NAME,
            agent_version=RESEARCH_BRIEF_AGENT_VERSION,
            model_profile=request.model_profile,
            input_payload={
                "request": request.model_dump(mode="json"),
                "perspective_agent_run_ids": [
                    str(report.agent_run_id)
                    for report in perspective_reports
                    if report.agent_run_id is not None
                ],
                "citation_count": len(evidence.citations),
            },
            execute=lambda: agent.synthesize(request, perspective_reports, evidence=evidence),
            dagster_run_id=request.dagster_run_id,
            metadata=request.metadata,
            summarize=summarize_research_brief,
        )
        brief_id = result.brief_id or uuid4()
        result = result.model_copy(update={"brief_id": brief_id})
        self.repository.upsert_research_brief(_research_brief_record_from_result(result, request))
        return result

    def build_research_brief_playground_pack(
        self,
        request: ResearchBriefRequest,
    ) -> ResearchBriefPlaygroundPack:
        return ResearchBriefAgent(self.repository).build_playground_pack(request)

    def run_therapy_committee(self, request: TherapyCommitteeRequest) -> TherapyCommitteeResult:
        return AgentRunner(self.repository).run(
            agent_name=THERAPY_COMMITTEE_AGENT_NAME,
            agent_version=THERAPY_COMMITTEE_AGENT_VERSION,
            model_profile=request.model_profile,
            input_payload=request.model_dump(mode="json"),
            source_key=request.source_key,
            dagster_run_id=request.dagster_run_id,
            metadata=request.metadata,
            execute=lambda: run_therapy_committee(self.repository, request),
            summarize=summarize_therapy_committee,
        )

    def queue_therapy_committee_validation_requests(
        self,
        request: TherapyCommitteeValidationQueueRequest,
    ) -> TherapyCommitteeValidationQueueResult:
        agent_run = _select_therapy_committee_agent_run(self.repository, request.agent_run_id)
        if agent_run is None:
            return TherapyCommitteeValidationQueueResult(
                origin_agent_run_id=request.agent_run_id,
                dry_run=request.dry_run,
                errors=["No completed therapy committee agent run matched the request."],
            )

        try:
            committee_result = TherapyCommitteeResult.model_validate(agent_run.output_payload)
        except Exception as exc:
            return TherapyCommitteeValidationQueueResult(
                origin_agent_run_id=agent_run.agent_run_id,
                dry_run=request.dry_run,
                errors=[f"Invalid therapy committee output payload: {exc}"],
            )

        selected_idea_ids = set(request.idea_ids)
        candidate_ideas = [
            idea
            for idea in committee_result.ranked_ideas
            if not selected_idea_ids or idea.idea_id in selected_idea_ids
        ][: request.max_ideas]
        plan_id = uuid5(
            NAMESPACE_URL,
            "therapy_committee_validation_plan:"
            f"{agent_run.agent_run_id}:"
            + ",".join(str(idea.idea_id) for idea in candidate_ideas),
        )
        synthetic_brief_id = committee_result.committee_run_id
        existing_items = self.repository.list_validation_request_queue_items(limit=None)
        existing_identity_keys = {item.identity_key for item in existing_items}

        tasks: list[ValidationPlanTask] = []
        queue_items: list[ValidationRequestQueueItem] = []
        skipped: list[dict[str, Any]] = []
        queued_count = 0
        existing_count = 0

        for idea_index, idea in enumerate(candidate_ideas):
            idea_tasks = _validation_tasks_from_therapy_idea(
                idea,
                committee_result=committee_result,
                origin_agent_run_id=agent_run.agent_run_id,
                priority=min(1000, request.priority + idea_index * 10),
            )
            if not idea_tasks:
                skipped.append(
                    {
                        "idea_id": str(idea.idea_id),
                        "title": idea.title,
                        "reason": "No validation tasks could be generated from the therapy idea.",
                    }
                )
                continue
            tasks.extend(idea_tasks)
            for task in idea_tasks:
                if task.validation_request is None:
                    skipped.append(
                        {
                            "idea_id": str(idea.idea_id),
                            "task_id": str(task.task_id),
                            "title": task.title,
                            "reason": "Task has no validation request.",
                        }
                    )
                    continue
                queue_item = ValidationRequestQueueItem(
                    identity_key=(
                        "therapy_committee_validation_queue:"
                        f"{agent_run.agent_run_id}:"
                        f"{idea.idea_id}:"
                        f"{task.task_type}:"
                        f"{task.validation_request.validation_type}"
                    ),
                    plan_id=plan_id,
                    task_id=task.task_id,
                    brief_id=synthetic_brief_id,
                    source_key=committee_result.reports[0].evidence.get("source_key")
                    if committee_result.reports and committee_result.reports[0].evidence
                    else None,
                    topic=committee_result.topic,
                    task_type=task.task_type,
                    title=task.title,
                    objective=task.objective,
                    rationale=task.rationale,
                    validation_request=task.validation_request,
                    priority=task.priority,
                    requires_human_approval=task.requires_human_approval,
                    quality_gates=_validation_request_quality_gates(task.validation_request),
                    dispatch_blockers=_validation_request_dispatch_blockers(
                        task.validation_request.model_copy(update={"require_approval": False})
                    ),
                    metadata={
                        **request.metadata,
                        "queued_from": "therapy_committee",
                        "origin_agent_run_id": str(agent_run.agent_run_id),
                        "committee_run_id": str(committee_result.committee_run_id),
                        "idea_id": str(idea.idea_id),
                        "idea_title": idea.title,
                        "idea_priority_score": idea.priority_score,
                        "evidence_refs": task.evidence_refs,
                        "required_inputs": task.required_inputs,
                        "expected_outputs": task.expected_outputs,
                        "tool_hint": task.tool_hint,
                        "recommend_only": True,
                        "brief_id_semantics": "committee_run_id",
                    },
                )
                if queue_item.identity_key in existing_identity_keys:
                    existing_count += 1
                    queue_items.append(
                        next(
                            item
                            for item in existing_items
                            if item.identity_key == queue_item.identity_key
                        )
                    )
                    continue
                queue_items.append(queue_item if request.dry_run else self.repository.upsert_validation_request_queue_item(queue_item))
                if not request.dry_run:
                    queued_count += 1
                    existing_identity_keys.add(queue_item.identity_key)

        if not request.dry_run and tasks:
            self.repository.upsert_validation_plan(
                ValidationPlanRecord(
                    plan_id=plan_id,
                    agent_run_id=agent_run.agent_run_id,
                    brief_id=synthetic_brief_id,
                    topic=committee_result.topic,
                    model_profile="validation_planner",
                    status="ready_for_review",
                    readiness="ready_for_expert_review",
                    task_count=len(tasks),
                    hypothesis_count=len(candidate_ideas),
                    summary={
                        "origin": "therapy_committee",
                        "committee_run_id": str(committee_result.committee_run_id),
                        "idea_count": len(candidate_ideas),
                        "task_count": len(tasks),
                    },
                    result_payload={
                        "plan_id": str(plan_id),
                        "agent_run_id": str(agent_run.agent_run_id),
                        "brief_id": str(synthetic_brief_id),
                        "topic": committee_result.topic,
                        "status": "ready_for_review",
                        "readiness": "ready_for_expert_review",
                        "tasks": [task.model_dump(mode="json") for task in tasks],
                        "evidence": {
                            "origin": "therapy_committee",
                            "committee_run_id": str(committee_result.committee_run_id),
                            "origin_agent_run_id": str(agent_run.agent_run_id),
                            "citation_count": committee_result.evidence.get("citation_count", 0),
                        },
                        "errors": [],
                    },
                    metadata={
                        **request.metadata,
                        "queued_from": "therapy_committee",
                        "origin_agent_run_id": str(agent_run.agent_run_id),
                        "committee_run_id": str(committee_result.committee_run_id),
                        "brief_id_semantics": "committee_run_id",
                    },
                )
            )

        return TherapyCommitteeValidationQueueResult(
            origin_agent_run_id=agent_run.agent_run_id,
            committee_run_id=committee_result.committee_run_id,
            plan_id=plan_id,
            candidate_idea_count=len(candidate_ideas),
            candidate_task_count=len(tasks),
            queued_count=queued_count,
            existing_count=existing_count,
            skipped_count=len(skipped),
            dry_run=request.dry_run,
            queue_items=queue_items,
            skipped=skipped,
        )

    def get_research_brief(self, brief_id: UUID) -> ResearchBriefRecord | None:
        return self.repository.get_research_brief(brief_id)

    def list_research_briefs(
        self,
        *,
        status: str | None = None,
        source_key: str | None = None,
        topic_query: str | None = None,
        limit: int | None = 50,
    ) -> list[ResearchBriefRecord]:
        return self.repository.list_research_briefs(
            status=status,
            source_key=source_key,
            topic_query=topic_query,
            limit=limit,
        )

    def evaluate_research_brief(self, request: ResearchBriefEvaluationRequest) -> ResearchBriefEvaluationResult:
        brief = _select_brief_for_evaluation(self.repository, request)
        if brief is None:
            raise ValueError("No persisted research brief matched the evaluation request.")
        result = AgentRunner(self.repository).run(
            agent_name=RESEARCH_BRIEF_EVALUATION_AGENT_NAME,
            agent_version=RESEARCH_BRIEF_EVALUATION_AGENT_VERSION,
            model_profile=request.model_profile,
            input_payload={
                "request": request.model_dump(mode="json"),
                "brief_id": str(brief.brief_id),
                "topic": brief.topic,
                "source_key": brief.source_key,
            },
            execute=lambda: evaluate_research_brief_synthesis(brief, request),
            source_key=brief.source_key,
            dagster_run_id=request.dagster_run_id,
            metadata=request.metadata,
            summarize=summarize_research_brief_evaluation,
        )
        self.repository.upsert_research_brief_evaluation(
            _research_brief_evaluation_record_from_result(result, request)
        )
        return result

    def get_research_brief_evaluation(self, evaluation_id: UUID) -> ResearchBriefEvaluationRecord | None:
        return self.repository.get_research_brief_evaluation(evaluation_id)

    def list_research_brief_evaluations(
        self,
        *,
        brief_id: UUID | None = None,
        readiness: str | None = None,
        passes_quality_bar: bool | None = None,
        limit: int | None = 50,
    ) -> list[ResearchBriefEvaluationRecord]:
        return self.repository.list_research_brief_evaluations(
            brief_id=brief_id,
            readiness=readiness,
            passes_quality_bar=passes_quality_bar,
            limit=limit,
        )

    def build_research_brief_quality_report(
        self,
        request: ResearchBriefQualityReportRequest,
    ) -> ResearchBriefQualityReportResult:
        briefs = self.repository.list_research_briefs(
            status=request.status,
            source_key=request.source_key,
            topic_query=request.topic_query,
            limit=request.limit,
        )
        rows: list[ResearchBriefQualityRow] = []
        errors: list[str] = []
        scores: list[float] = []
        for brief in briefs:
            try:
                latest_evaluation = None
                if request.include_evaluations:
                    evaluations = self.repository.list_research_brief_evaluations(
                        brief_id=brief.brief_id,
                        limit=1,
                    )
                    latest_evaluation = evaluations[0] if evaluations else None
                row = _research_brief_quality_row(brief, latest_evaluation)
                rows.append(row)
                if row.overall_score is not None:
                    scores.append(row.overall_score)
            except Exception as exc:  # pragma: no cover - defensive control-panel boundary
                errors.append(f"brief {brief.brief_id}: {exc}")

        status_counts = Counter(row.status for row in rows)
        quality_status_counts = Counter(row.quality_status for row in rows)
        return ResearchBriefQualityReportResult(
            brief_count=len(rows),
            evaluated_count=sum(1 for row in rows if row.evaluation_id is not None),
            ready_count=sum(1 for row in rows if row.quality_status == "ready_for_validation"),
            failed_count=sum(1 for row in rows if row.quality_status == "brief_failed"),
            followup_count=sum(1 for row in rows if row.quality_status == "needs_followup_research"),
            needs_evaluation_count=sum(1 for row in rows if row.quality_status == "needs_evaluation"),
            average_overall_score=(sum(scores) / len(scores)) if scores else None,
            status_counts=dict(sorted(status_counts.items())),
            quality_status_counts=dict(sorted(quality_status_counts.items())),
            rows=rows,
            errors=errors,
        )

    def queue_research_brief_followups(
        self,
        request: ResearchBriefFollowupQueueRequest,
    ) -> ResearchBriefFollowupQueueResult:
        briefs = self.repository.list_research_briefs(
            status=request.status,
            source_key=request.source_key,
            topic_query=request.topic_query,
            limit=request.limit,
        )
        existing_identity_keys = {
            lead.identity_key
            for lead in self.repository.list_research_leads(limit=None)
            if lead.identity_key
        }
        followup_leads: list[ResearchLeadRecord] = []
        skipped: list[dict[str, Any]] = []
        errors: list[str] = []
        candidate_brief_count = 0
        limitation_count = 0
        queued_count = 0
        existing_count = 0

        for brief in briefs:
            try:
                latest_evaluation = None
                if request.include_evaluations:
                    evaluations = self.repository.list_research_brief_evaluations(
                        brief_id=brief.brief_id,
                        limit=1,
                    )
                    latest_evaluation = evaluations[0] if evaluations else None
                row = _research_brief_quality_row(brief, latest_evaluation)
                if row.quality_status != "needs_followup_research":
                    skipped.append(
                        {
                            "brief_id": str(brief.brief_id),
                            "quality_status": row.quality_status,
                            "reason": "brief_does_not_need_followup_research",
                        }
                    )
                    continue

                candidate_brief_count += 1
                leads = _research_brief_followup_leads_from_brief(
                    brief,
                    row,
                    max_limitations=request.max_limitations_per_brief,
                )
                if not leads:
                    skipped.append(
                        {
                            "brief_id": str(brief.brief_id),
                            "quality_status": row.quality_status,
                            "reason": "no_evidence_limitations_available",
                        }
                    )
                    continue

                for lead in leads:
                    limitation_count += 1
                    identity_key = lead.identity_key
                    if identity_key in existing_identity_keys:
                        existing_count += 1
                    else:
                        queued_count += 1
                        existing_identity_keys.add(identity_key)
                    persisted = lead if request.dry_run else self.repository.upsert_research_lead(lead)
                    followup_leads.append(persisted)
            except Exception as exc:  # pragma: no cover - defensive control-panel boundary
                errors.append(f"brief {brief.brief_id}: {exc}")

        return ResearchBriefFollowupQueueResult(
            candidate_brief_count=candidate_brief_count,
            limitation_count=limitation_count,
            queued_count=0 if request.dry_run else queued_count,
            existing_count=existing_count,
            skipped_count=len(skipped),
            dry_run=request.dry_run,
            followup_leads=followup_leads,
            skipped=skipped,
            errors=errors,
        )

    def plan_validation(self, request: ValidationPlanRequest) -> ValidationPlanResult:
        source_key = request.source_key or _source_key_for_validation_plan(self.repository, request)
        result = AgentRunner(self.repository).run(
            agent_name=VALIDATION_PLANNING_AGENT_NAME,
            agent_version=VALIDATION_PLANNING_AGENT_VERSION,
            model_profile=request.model_profile,
            input_payload=request.model_dump(mode="json"),
            source_key=source_key,
            dagster_run_id=request.dagster_run_id,
            metadata=request.metadata,
            execute=lambda: plan_validation_from_research_brief(self.repository, request),
            summarize=summarize_validation_plan,
        )
        self.repository.upsert_validation_plan(validation_plan_record_from_result(result, request))
        return result

    def get_validation_plan(self, plan_id: UUID) -> ValidationPlanRecord | None:
        return self.repository.get_validation_plan(plan_id)

    def list_validation_plans(
        self,
        *,
        brief_id: UUID | None = None,
        evaluation_id: UUID | None = None,
        status: str | None = None,
        readiness: str | None = None,
        limit: int | None = 50,
    ) -> list[ValidationPlanRecord]:
        return self.repository.list_validation_plans(
            brief_id=brief_id,
            evaluation_id=evaluation_id,
            status=status,
            readiness=readiness,
            limit=limit,
        )

    def queue_validation_requests_from_plan(
        self,
        request: ValidationRequestQueueRequest,
    ) -> ValidationRequestQueueResult:
        plan = self.repository.get_validation_plan(request.plan_id)
        if plan is None:
            return ValidationRequestQueueResult(
                plan_id=request.plan_id,
                dry_run=request.dry_run,
                errors=[f"Validation plan not found: {request.plan_id}"],
            )
        if plan.status != "ready_for_review" or plan.readiness != "ready_for_expert_review":
            return ValidationRequestQueueResult(
                plan_id=request.plan_id,
                dry_run=request.dry_run,
                errors=[
                    "Validation plan is not ready for request queueing: "
                    f"status={plan.status}, readiness={plan.readiness}"
                ],
            )

        selected_task_ids = set(request.task_ids)
        existing_items = self.repository.list_validation_request_queue_items(
            plan_id=plan.plan_id,
            limit=None,
        )
        existing_identity_keys = {item.identity_key for item in existing_items}
        candidate_task_count = 0
        queued_count = 0
        existing_count = 0
        skipped: list[dict[str, Any]] = []
        errors: list[str] = []
        queue_items: list[ValidationRequestQueueItem] = []

        for raw_task in plan.result_payload.get("tasks", []):
            try:
                task = ValidationPlanTask.model_validate(raw_task)
            except Exception as exc:
                errors.append(f"Invalid validation plan task payload: {exc}")
                continue
            if selected_task_ids and task.task_id not in selected_task_ids:
                continue
            candidate_task_count += 1
            if task.validation_request is None:
                skipped.append(
                    {
                        "task_id": str(task.task_id),
                        "title": task.title,
                        "reason": "Task does not include a validation_request.",
                    }
                )
                continue
            queue_item = ValidationRequestQueueItem(
                plan_id=plan.plan_id,
                task_id=task.task_id,
                brief_id=plan.brief_id,
                evaluation_id=plan.evaluation_id,
                source_key=plan.source_key,
                topic=plan.topic,
                task_type=task.task_type,
                title=task.title,
                objective=task.objective,
                rationale=task.rationale,
                validation_request=task.validation_request,
                priority=task.priority,
                requires_human_approval=task.requires_human_approval,
                quality_gates=_validation_request_quality_gates(task.validation_request),
                metadata={
                    **request.metadata,
                    "queued_from": "validation_plan",
                    "plan_id": str(plan.plan_id),
                    "task_id": str(task.task_id),
                    "evidence_refs": task.evidence_refs,
                    "required_inputs": task.required_inputs,
                    "expected_outputs": task.expected_outputs,
                    "tool_hint": task.tool_hint,
                    "recommend_only": True,
                },
            )
            if queue_item.identity_key in existing_identity_keys:
                existing_count += 1
                queue_items.append(
                    next(
                        item
                        for item in existing_items
                        if item.identity_key == queue_item.identity_key
                    )
                )
                continue
            if request.dry_run:
                queue_items.append(queue_item)
                continue
            persisted = self.repository.upsert_validation_request_queue_item(queue_item)
            queued_count += 1
            queue_items.append(persisted)

        return ValidationRequestQueueResult(
            plan_id=plan.plan_id,
            candidate_task_count=candidate_task_count,
            queued_count=queued_count,
            existing_count=existing_count,
            skipped_count=len(skipped),
            dry_run=request.dry_run,
            queue_items=queue_items,
            skipped=skipped,
            errors=errors,
        )

    def get_validation_request_queue_item(
        self,
        queue_item_id: UUID,
    ) -> ValidationRequestQueueItem | None:
        return self.repository.get_validation_request_queue_item(queue_item_id)

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
        return self.repository.list_validation_request_queue_items(
            plan_id=plan_id,
            status=status,
            statuses=statuses,
            source_key=source_key,
            task_type=task_type,
            topic_query=topic_query,
            limit=limit,
        )

    def approve_validation_request_queue_item(
        self,
        queue_item_id: UUID,
        *,
        approved_by: str,
        approval_note: str | None = None,
    ) -> ValidationRequestQueueItem | None:
        return self.repository.update_validation_request_queue_item(
            queue_item_id,
            status="approved",
            approved_by=approved_by,
            approval_note=approval_note,
            metadata={
                "approved_at": datetime.now(UTC).isoformat(),
                "approval_note": approval_note,
            },
        )

    def dispatch_validation_request_queue_item(
        self,
        queue_item_id: UUID,
        *,
        model_profile: str = "deterministic_only",
    ) -> ValidationRequestQueueItem | None:
        item = self.repository.get_validation_request_queue_item(queue_item_id)
        if item is None:
            return None
        if item.status != "approved":
            return self.repository.update_validation_request_queue_item(
                queue_item_id,
                attempts=item.attempts + 1,
                last_error="Validation request queue item must be approved before dispatch.",
                dispatch_blockers=["approval_required"],
            )
        request = item.validation_request.model_copy(update={"require_approval": False})
        dispatch_blockers = _validation_request_dispatch_blockers(request)
        if dispatch_blockers:
            return self.repository.update_validation_request_queue_item(
                queue_item_id,
                status="blocked",
                attempts=item.attempts + 1,
                last_error=(
                    "Validation request dispatch blocked by missing execution context: "
                    + ", ".join(dispatch_blockers)
                ),
                quality_gates=_validation_request_quality_gates(request),
                dispatch_blockers=dispatch_blockers,
                metadata={
                    "dispatch_blocked_at": datetime.now(UTC).isoformat(),
                    "dispatch_blockers": dispatch_blockers,
                },
            )
        model_profile = model_profile.strip() or VALIDATION_AGENT_MODEL_PROFILE
        try:
            result = self.run_validation_queue_item_agent(
                item.model_copy(update={"validation_request": request}),
                model_profile=model_profile,
            )
        except Exception as exc:
            return self.repository.update_validation_request_queue_item(
                queue_item_id,
                status="failed",
                attempts=item.attempts + 1,
                last_error=str(exc),
                quality_gates=_validation_request_quality_gates(request),
                dispatch_blockers=[],
                metadata={
                    "dispatch_failed_at": datetime.now(UTC).isoformat(),
                    "validation_agent_model_profile": model_profile,
                    "error_type": type(exc).__name__,
                },
            )
        return self.repository.update_validation_request_queue_item(
            queue_item_id,
            status="completed",
            attempts=item.attempts + 1,
            last_run_id=result.agent_run_id,
            last_error=None,
            quality_gates=_validation_request_quality_gates(request),
            dispatch_blockers=[],
            metadata={
                "dispatched_at": datetime.now(UTC).isoformat(),
                "completed_at": datetime.now(UTC).isoformat(),
                "run_kind": "agent_run",
                "run_name": result.agent_name,
                "validation_agent_model_profile": model_profile,
                "validation_agent_result": result.model_dump(mode="json"),
            },
        )

    def run_validation_queue_item_agent(
        self,
        item: ValidationRequestQueueItem,
        *,
        model_profile: str = VALIDATION_AGENT_MODEL_PROFILE,
    ) -> ValidationAgentResult:
        return AgentRunner(self.repository).run(
            agent_name=validation_agent_name(item),
            agent_version=VALIDATION_AGENT_VERSION,
            model_profile=model_profile,
            input_payload=item.model_dump(mode="json"),
            source_key=item.source_key,
            execute=lambda: run_validation_agent(item, model_profile=model_profile),
            metadata={
                "queue_item_id": str(item.queue_item_id),
                "plan_id": str(item.plan_id),
                "task_id": str(item.task_id),
                "task_type": item.task_type,
                "validation_type": item.validation_request.validation_type,
            },
            summarize=summarize_validation_agent_result,
        )

    def run_validation_autopilot(
        self,
        request: ValidationAutopilotRequest,
    ) -> ValidationAutopilotResult:
        return AgentRunner(self.repository).run(
            agent_name=VALIDATION_AUTOPILOT_AGENT_NAME,
            agent_version=VALIDATION_AUTOPILOT_AGENT_VERSION,
            model_profile=request.model_profile,
            input_payload=request.model_dump(mode="json"),
            dagster_run_id=request.dagster_run_id,
            metadata=request.metadata,
            execute=lambda: self._run_validation_autopilot_policy(request),
            summarize=summarize_validation_autopilot_result,
        )

    def preview_validation_autopilot(
        self,
        request: ValidationAutopilotRequest,
    ) -> ValidationAutopilotResult:
        return self._run_validation_autopilot_policy(request.model_copy(update={"dry_run": True}))

    def _run_validation_autopilot_policy(
        self,
        request: ValidationAutopilotRequest,
    ) -> ValidationAutopilotResult:
        now = datetime.now(UTC)
        all_items = self.repository.list_validation_request_queue_items(limit=None)
        candidate_items = [
            item
            for item in all_items
            if item.status == "needs_approval"
            and (not request.source_keys or (item.source_key or "").casefold() in request.source_keys)
        ]
        candidate_items.sort(key=lambda item: (item.priority, _coerce_utc(item.created_at)))

        last_manual_activity_at = _validation_manual_activity_at(all_items)
        manual_grace_period_ends_at = (
            last_manual_activity_at + timedelta(hours=request.manual_grace_period_hours)
            if last_manual_activity_at is not None
            else None
        )
        hourly_spend = _validation_autopilot_spend_since(all_items, now - timedelta(hours=1))
        daily_spend = _validation_autopilot_spend_since(all_items, now - timedelta(hours=24))
        blockers: list[str] = []
        errors: list[str] = []
        selected: list[ValidationAutopilotQueueRecord] = []
        skipped: list[ValidationAutopilotQueueRecord] = []
        eligible_count = 0

        if not request.enabled and not request.dry_run:
            blockers.append("autopilot_disabled")
        if (
            not request.force
            and manual_grace_period_ends_at is not None
            and now < manual_grace_period_ends_at
        ):
            blockers.append("manual_grace_period_active")
        if _requires_openrouter_for_model_profile(request.model_profile) and not os.getenv("OPENROUTER_API_KEY"):
            blockers.append("openrouter_api_key_missing")
        if daily_spend >= request.daily_budget_usd:
            blockers.append("daily_budget_exhausted")
        if hourly_spend >= request.hourly_budget_usd:
            blockers.append("hourly_budget_exhausted")

        allowed_task_types = set(request.allowed_task_types)
        allowed_validation_types = set(request.allowed_validation_types)
        projected_hourly_spend = hourly_spend
        projected_daily_spend = daily_spend
        for item in candidate_items:
            skip_reason = self._validation_autopilot_skip_reason(
                item,
                request=request,
                now=now,
                allowed_task_types=allowed_task_types,
                allowed_validation_types=allowed_validation_types,
            )
            if skip_reason is not None:
                skipped.append(_validation_autopilot_record(item, reason=skip_reason))
                continue
            eligible_count += 1
            projected_hourly_spend += request.estimated_cost_per_item_usd
            projected_daily_spend += request.estimated_cost_per_item_usd
            if projected_hourly_spend > request.hourly_budget_usd:
                skipped.append(_validation_autopilot_record(item, reason="hourly_budget_would_be_exceeded"))
                projected_hourly_spend -= request.estimated_cost_per_item_usd
                projected_daily_spend -= request.estimated_cost_per_item_usd
                continue
            if projected_daily_spend > request.daily_budget_usd:
                skipped.append(_validation_autopilot_record(item, reason="daily_budget_would_be_exceeded"))
                projected_hourly_spend -= request.estimated_cost_per_item_usd
                projected_daily_spend -= request.estimated_cost_per_item_usd
                continue
            selected.append(_validation_autopilot_record(item, reason="eligible_for_autopilot_dispatch"))
            if len(selected) >= request.max_per_run:
                break

        should_dispatch = bool(selected) and not request.dry_run and not blockers
        dispatched: list[ValidationAutopilotQueueRecord] = []
        actual_cost = 0.0
        if should_dispatch:
            for record in selected:
                item = self.repository.get_validation_request_queue_item(record.queue_item_id)
                if item is None:
                    errors.append(f"{record.queue_item_id}: queue item disappeared before dispatch")
                    continue
                approval_note = request.approval_note or (
                    f"{VALIDATION_AUTOPILOT_AGENT_NAME} {VALIDATION_AUTOPILOT_POLICY_VERSION}: "
                    f"selected after {request.manual_grace_period_hours:g}h manual inactivity window; "
                    f"max_per_run={request.max_per_run}."
                )
                approved = self.approve_validation_request_queue_item(
                    item.queue_item_id,
                    approved_by=request.approved_by,
                    approval_note=approval_note,
                )
                if approved is None:
                    errors.append(f"{record.queue_item_id}: approval failed")
                    continue
                autopilot_metadata = {
                    "policy_version": VALIDATION_AUTOPILOT_POLICY_VERSION,
                    "selected_at": now.isoformat(),
                    "estimated_cost_usd": request.estimated_cost_per_item_usd,
                    "model_profile": request.model_profile,
                    "reason": record.reason,
                    "manual_grace_period_hours": request.manual_grace_period_hours,
                    "minimum_queue_age_hours": request.minimum_queue_age_hours,
                    "dry_run": False,
                }
                self.repository.update_validation_request_queue_item(
                    approved.queue_item_id,
                    metadata={"validation_autopilot": autopilot_metadata},
                )
                dispatched_item = self.dispatch_validation_request_queue_item(
                    approved.queue_item_id,
                    model_profile=request.model_profile,
                )
                if dispatched_item is None:
                    errors.append(f"{record.queue_item_id}: dispatch returned no queue item")
                    continue
                cost = _queue_item_cost_usd(dispatched_item)
                if cost is not None:
                    actual_cost += cost
                final_metadata = {
                    **autopilot_metadata,
                    "dispatched_at": datetime.now(UTC).isoformat(),
                    "actual_cost_usd": cost or 0.0,
                    "result_status": dispatched_item.status,
                }
                final_item = self.repository.update_validation_request_queue_item(
                    dispatched_item.queue_item_id,
                    metadata={"validation_autopilot": final_metadata},
                ) or dispatched_item
                dispatched.append(
                    _validation_autopilot_record(
                        final_item,
                        reason=f"autopilot_dispatched:{final_item.status}",
                        cost_usd=cost,
                    )
                )
                if final_item.status in {"blocked", "failed"}:
                    break

        return ValidationAutopilotResult(
            agent_name=VALIDATION_AUTOPILOT_AGENT_NAME,
            policy_version=VALIDATION_AUTOPILOT_POLICY_VERSION,
            enabled=request.enabled,
            dry_run=request.dry_run,
            force=request.force,
            model_profile=request.model_profile,
            scanned_count=len(candidate_items),
            eligible_count=eligible_count,
            selected_count=len(selected),
            dispatched_count=len(dispatched),
            skipped_count=len(skipped),
            should_dispatch=should_dispatch,
            blockers=blockers,
            errors=errors,
            last_manual_activity_at=last_manual_activity_at,
            manual_grace_period_ends_at=manual_grace_period_ends_at,
            hourly_budget_usd=request.hourly_budget_usd,
            daily_budget_usd=request.daily_budget_usd,
            hourly_spend_usd=hourly_spend,
            daily_spend_usd=daily_spend,
            estimated_cost_usd=round(len(selected) * request.estimated_cost_per_item_usd, 6),
            actual_cost_usd=round(actual_cost, 6),
            selected=selected,
            dispatched=dispatched,
            skipped=skipped,
            created_at=now,
        )

    def _validation_autopilot_skip_reason(
        self,
        item: ValidationRequestQueueItem,
        *,
        request: ValidationAutopilotRequest,
        now: datetime,
        allowed_task_types: set[str],
        allowed_validation_types: set[str],
    ) -> str | None:
        if item.task_type not in allowed_task_types:
            return f"task_type_not_allowlisted:{item.task_type}"
        validation_type = item.validation_request.validation_type
        if validation_type not in allowed_validation_types:
            return f"validation_type_not_allowlisted:{validation_type}"
        if validation_type in {"wet_lab", "safety", "admet", "docking", "boltz", "md"}:
            return f"validation_type_requires_manual_review:{validation_type}"
        if item.attempts:
            return "prior_attempts_present"
        if not request.force:
            minimum_age_at = _coerce_utc(item.created_at) + timedelta(hours=request.minimum_queue_age_hours)
            if now < minimum_age_at:
                return "minimum_queue_age_active"
        dispatch_request = item.validation_request.model_copy(update={"require_approval": False})
        dispatch_blockers = _validation_request_dispatch_blockers(dispatch_request)
        if dispatch_blockers:
            return "dispatch_blockers:" + ",".join(dispatch_blockers)
        return None

    def resolve_evidence_gaps(
        self,
        request: EvidenceGapResolverRequest,
    ) -> EvidenceGapResolverResult:
        return AgentRunner(self.repository).run(
            agent_name=EVIDENCE_GAP_RESOLVER_AGENT_NAME,
            agent_version=EVIDENCE_GAP_RESOLVER_AGENT_VERSION,
            model_profile="deterministic_resolver",
            input_payload=request.model_dump(mode="json"),
            dagster_run_id=request.dagster_run_id,
            metadata=request.metadata,
            execute=lambda: resolve_evidence_gaps(
                self.repository,
                request,
                queue_research_brief=self.queue_research_brief,
            ),
            summarize=summarize_evidence_gap_resolver,
        )

    def queue_research_brief(self, request: ResearchBriefQueueRequest) -> ResearchBriefQueueItem:
        return self.repository.upsert_research_brief_queue_item(
            ResearchBriefQueueItem(
                topic=request.topic,
                disease_scope=request.disease_scope,
                source_key=request.source_key,
                priority=request.priority,
                max_chunks_per_perspective=request.max_chunks_per_perspective,
                max_claims=request.max_claims,
                max_chunk_chars=request.max_chunk_chars,
                brief_style=request.brief_style,
                model_profile=request.model_profile,
                review_mode=request.review_mode,
                review_models=request.review_models,
                metadata=request.metadata,
            )
        )

    def get_research_brief_queue_item(self, queue_item_id: UUID) -> ResearchBriefQueueItem | None:
        return self.repository.get_research_brief_queue_item(queue_item_id)

    def list_research_brief_queue_items(
        self,
        *,
        status: str | None = None,
        statuses: list[str] | None = None,
        source_key: str | None = None,
        topic_query: str | None = None,
        limit: int | None = 50,
    ) -> list[ResearchBriefQueueItem]:
        return self.repository.list_research_brief_queue_items(
            status=status,
            statuses=statuses,
            source_key=source_key,
            topic_query=topic_query,
            limit=limit,
        )

    def requeue_research_brief_queue_item(
        self,
        queue_item_id: UUID,
        *,
        priority: int | None = None,
    ) -> ResearchBriefQueueItem | None:
        item = self.repository.get_research_brief_queue_item(queue_item_id)
        if item is None:
            return None
        if item.status != "failed":
            raise ValueError("only failed research brief queue items can be requeued")
        if priority is not None and not 0 <= priority <= 1000:
            raise ValueError("priority must be between 0 and 1000")
        return self.repository.update_research_brief_queue_item(
            item.queue_item_id,
            status="queued",
            priority=priority,
            last_error=None,
            metadata={
                "queue_control": {
                    "last_action": "requeue",
                    "previous_status": item.status,
                    "previous_attempts": item.attempts,
                }
            },
        )

    def archive_research_brief_queue_item(self, queue_item_id: UUID) -> ResearchBriefQueueItem | None:
        item = self.repository.get_research_brief_queue_item(queue_item_id)
        if item is None:
            return None
        if item.status == "archived":
            return item
        if item.status != "completed":
            raise ValueError("only completed research brief queue items can be archived")
        return self.repository.update_research_brief_queue_item(
            item.queue_item_id,
            status="archived",
            last_error=None,
            metadata={
                "queue_control": {
                    "last_action": "archive",
                    "previous_status": item.status,
                    "previous_attempts": item.attempts,
                }
            },
        )

    def maintain_research_brief_queue(
        self,
        request: ResearchBriefQueueMaintenanceRequest,
    ) -> ResearchBriefQueueMaintenanceResult:
        seen: set[UUID] = set()
        candidates: list[ResearchBriefQueueItem] = []
        skipped: list[dict[str, Any]] = []
        errors: list[str] = []
        now = datetime.now(UTC)

        def add_skip(item: ResearchBriefQueueItem | None, reason: str, queue_item_id: UUID | None = None) -> None:
            skipped.append(
                {
                    "queue_item_id": str(item.queue_item_id if item else queue_item_id),
                    "status": item.status if item else None,
                    "source_key": item.source_key if item else None,
                    "topic": item.topic[:300] if item else None,
                    "reason": reason,
                }
            )

        if request.queue_item_ids:
            raw_items: list[ResearchBriefQueueItem] = []
            for queue_item_id in request.queue_item_ids:
                item = self.repository.get_research_brief_queue_item(queue_item_id)
                if item is None:
                    add_skip(None, "queue item not found", queue_item_id=queue_item_id)
                    continue
                raw_items.append(item)
        else:
            raw_items = self.repository.list_research_brief_queue_items(
                statuses=list(request.statuses),
                source_key=request.source_key,
                topic_query=request.topic_query,
                limit=None,
            )

        for item in raw_items:
            if item.queue_item_id in seen:
                continue
            seen.add(item.queue_item_id)
            if item.status not in request.statuses:
                add_skip(item, f"status {item.status!r} is outside maintenance scope")
                continue
            if item.status not in {"failed", "completed"}:
                add_skip(item, f"status {item.status!r} cannot be archived by maintenance")
                continue
            if request.source_key and item.source_key != request.source_key:
                add_skip(item, "source key does not match")
                continue
            if request.topic_query:
                normalized_query = request.topic_query.lower()
                if normalized_query not in item.topic.lower() and normalized_query not in item.disease_scope.lower():
                    add_skip(item, "topic query does not match")
                    continue
            if item.attempts < request.min_attempts:
                add_skip(item, f"attempts {item.attempts} below minimum {request.min_attempts}")
                continue
            item_age_hours = (now - item.updated_at).total_seconds() / 3600
            if item_age_hours < request.max_updated_age_hours:
                add_skip(item, f"updated {item_age_hours:.2f} hours ago")
                continue
            candidates.append(item)
            if len(candidates) >= request.limit:
                break

        archived_items: list[ResearchBriefQueueItem] = []
        if not request.dry_run:
            for item in candidates:
                updated = self.repository.update_research_brief_queue_item(
                    item.queue_item_id,
                    status="archived",
                    last_error=item.last_error,
                    metadata={
                        "queue_control": {
                            "last_action": "maintenance_archive",
                            "reason": request.reason,
                            "previous_status": item.status,
                            "previous_attempts": item.attempts,
                            "previous_last_error": item.last_error,
                            "dagster_run_id": request.dagster_run_id,
                            "archived_at": now.isoformat(),
                        },
                        "queue_maintenance": {
                            "dry_run": False,
                            "source_key": request.source_key,
                            "topic_query": request.topic_query,
                            "min_attempts": request.min_attempts,
                            "max_updated_age_hours": request.max_updated_age_hours,
                            **request.metadata,
                        },
                    },
                )
                if updated is None:
                    errors.append(f"queue item disappeared before archive: {item.queue_item_id}")
                    continue
                archived_items.append(updated)

        return ResearchBriefQueueMaintenanceResult(
            action=request.action,
            dry_run=request.dry_run,
            candidate_count=len(candidates),
            archived_count=0 if request.dry_run else len(archived_items),
            skipped_count=len(skipped),
            queue_items=candidates if request.dry_run else archived_items,
            skipped=skipped,
            errors=errors,
        )

    def queue_research_brief_batch(self, request: ResearchBriefQueueBatchRequest) -> ResearchBriefQueueBatchResult:
        queue_items: list[ResearchBriefQueueItem] = []
        skipped: list[dict[str, Any]] = []
        errors: list[str] = []
        lead_count = 0
        research_followup_count = 0
        source_health_count = 0

        def remaining() -> int:
            return request.limit - len(queue_items)

        if request.mode in ("research_leads", "both") and remaining() > 0:
            source_key_filter = request.source_keys[0] if len(request.source_keys) == 1 else None
            leads = self.repository.list_research_leads(
                statuses=list(request.lead_statuses),
                source_key=source_key_filter,
                limit=None if request.source_keys and source_key_filter is None else request.limit,
            )
            if request.source_keys:
                allowed_source_keys = set(request.source_keys)
                leads = [
                    lead
                    for lead in leads
                    if lead.source_key in allowed_source_keys or lead.origin_source_key in allowed_source_keys
                ]
            if request.lead_types:
                allowed_lead_types = set(request.lead_types)
                leads = [lead for lead in leads if lead.lead_type in allowed_lead_types]
            for lead in leads[: remaining()]:
                try:
                    followup = _route_evidence_light_research_lead(self.repository, lead)
                    if followup is not None:
                        research_followup_count += 1
                        skipped.append(followup)
                        continue
                    queue_item = self.queue_research_brief(
                        ResearchBriefQueueRequest(
                            topic=_research_brief_topic_from_lead(lead),
                            disease_scope=request.disease_scope,
                            source_key=_research_brief_source_key_from_lead(lead),
                            priority=min(request.priority, lead.priority),
                            max_chunks_per_perspective=request.max_chunks_per_perspective,
                            max_claims=request.max_claims,
                            max_chunk_chars=request.max_chunk_chars,
                            brief_style=request.brief_style,
                            model_profile=request.model_profile,
                            review_mode=request.review_mode,
                            review_models=request.review_models,
                            metadata={
                                **request.metadata,
                                "batch_queue": {
                                    "origin": "research_lead",
                                    "lead_id": str(lead.lead_id),
                                    "lead_type": lead.lead_type,
                                    "lead_status": lead.status,
                                    "origin_source_key": lead.origin_source_key,
                                    "origin_record_id": lead.origin_record_id,
                                    "origin_agent_run_id": str(lead.origin_agent_run_id)
                                    if lead.origin_agent_run_id
                                    else None,
                                    "reason": lead.reason,
                                    "evidence_refs": lead.evidence_refs,
                                    "topic_tags": lead.topic_tags,
                                },
                            },
                        )
                    )
                    queue_items.append(queue_item)
                    lead_count += 1
                    self.repository.update_research_lead(
                        lead.lead_id,
                        status="queued",
                        metadata={
                            "research_brief_queue": {
                                "queue_item_id": str(queue_item.queue_item_id),
                                "topic": queue_item.topic,
                            }
                        },
                    )
                except Exception as exc:  # pragma: no cover - defensive service boundary
                    errors.append(f"lead {lead.lead_id}: {exc}")

        if request.mode in ("source_health", "both") and remaining() > 0:
            report = request.source_health_report
            if report is None:
                from .source_health import build_source_health_report

                report = build_source_health_report(
                    self.repository,
                    source_keys=request.source_keys or None,
                )
            allowed_statuses = set(request.source_health_statuses)
            selected_sources = request.source_keys and set(request.source_keys)
            source_reports = report.get("sources", [])
            for source_report in source_reports:
                if remaining() <= 0:
                    break
                source_key = str(source_report.get("source_key") or "")
                if not source_key:
                    continue
                if selected_sources and source_key not in selected_sources:
                    continue
                health_status = str(source_report.get("health_status") or "")
                if health_status not in allowed_statuses:
                    continue
                document_chunks = int(source_report.get("document_chunks") or 0)
                if document_chunks < 1 and not request.include_empty_sources:
                    skipped.append(
                        {
                            "origin": "source_health",
                            "source_key": source_key,
                            "reason": "source_has_no_document_chunks",
                            "health_status": health_status,
                        }
                    )
                    continue
                try:
                    queue_item = self.queue_research_brief(
                        ResearchBriefQueueRequest(
                            topic=f"Source health gap review for {source_key}",
                            disease_scope=request.disease_scope,
                            source_key=source_key,
                            priority=min(request.priority, _priority_for_source_health(health_status)),
                            max_chunks_per_perspective=request.max_chunks_per_perspective,
                            max_claims=request.max_claims,
                            max_chunk_chars=request.max_chunk_chars,
                            brief_style=request.brief_style,
                            model_profile=request.model_profile,
                            review_mode=request.review_mode,
                            review_models=request.review_models,
                            metadata={
                                **request.metadata,
                                "batch_queue": {
                                    "origin": "source_health",
                                    "source_key": source_key,
                                    "health_status": health_status,
                                    "passes_minimum_bar": source_report.get("passes_minimum_bar"),
                                    "health_score": source_report.get("health_score"),
                                    "risks": source_report.get("risks", []),
                                    "recommended_actions": source_report.get("recommended_actions", []),
                                },
                            },
                        )
                    )
                    queue_items.append(queue_item)
                    source_health_count += 1
                except Exception as exc:  # pragma: no cover - defensive service boundary
                    errors.append(f"source {source_key}: {exc}")

        return ResearchBriefQueueBatchResult(
            mode=request.mode,
            queued_count=len(queue_items),
            lead_count=lead_count,
            research_followup_count=research_followup_count,
            source_health_count=source_health_count,
            skipped_count=len(skipped),
            queue_items=queue_items,
            skipped=skipped,
            errors=errors,
        )

    def build_command_center_report(self, request: CommandCenterRequest) -> CommandCenterResult:
        errors: list[str] = []
        recommendations: list[CommandCenterRecommendation] = []

        all_queue_items = self.repository.list_research_brief_queue_items(limit=None)
        visible_queue_items = self.repository.list_research_brief_queue_items(limit=request.queue_limit)
        queue_status_counts = Counter(str(item.status) for item in all_queue_items)
        research_brief_queue = {
            "total": len(all_queue_items),
            "status_counts": dict(sorted(queue_status_counts.items())),
            "items": [_command_center_queue_item(item) for item in visible_queue_items],
        }

        all_leads = self.repository.list_research_leads(limit=None)
        visible_leads = self.repository.list_research_leads(limit=request.lead_limit)
        lead_status_counts = Counter(str(lead.status) for lead in all_leads)
        research_leads = {
            "total": len(all_leads),
            "status_counts": dict(sorted(lead_status_counts.items())),
            "items": [_command_center_lead(lead) for lead in visible_leads],
        }

        source_health_report: dict[str, Any] | None = None
        if request.include_source_health:
            if request.source_health_report is not None:
                source_health_report = request.source_health_report
            else:
                try:
                    from .source_health import build_source_health_report

                    source_health_report = build_source_health_report(
                        self.repository,
                        source_keys=request.source_keys or None,
                        min_health_score=request.min_health_score,
                        require_claims=request.require_claims,
                    )
                except Exception as exc:  # pragma: no cover - defensive reporting boundary
                    errors.append(f"source_health: {exc}")

        recent_agent_runs: list[dict[str, Any]] = []
        agent_status_counts: Counter[str] = Counter()
        if request.include_recent_agents:
            try:
                agent_runs = self.repository.list_agent_runs(limit=request.agent_run_limit)
                agent_status_counts = Counter(str(run.status) for run in agent_runs)
                recent_agent_runs = [_command_center_agent_run(run) for run in agent_runs]
            except Exception as exc:  # pragma: no cover - defensive reporting boundary
                errors.append(f"agent_runs: {exc}")

        _extend_command_center_recommendations(
            recommendations,
            queue_status_counts=queue_status_counts,
            lead_status_counts=lead_status_counts,
            source_health_report=source_health_report,
            agent_status_counts=agent_status_counts,
        )
        summary = {
            "brief_queue_total": len(all_queue_items),
            "brief_queue_ready": queue_status_counts.get("queued", 0),
            "brief_queue_failed": queue_status_counts.get("failed", 0),
            "research_leads_total": len(all_leads),
            "research_leads_actionable": lead_status_counts.get("new", 0) + lead_status_counts.get("watching", 0),
            "research_leads_followup": lead_status_counts.get("followup", 0),
            "recent_agent_failures": agent_status_counts.get("failed", 0),
            "source_health_failed": len(source_health_report.get("failed_sources", [])) if source_health_report else None,
            "source_health_watch": len(source_health_report.get("watch_sources", [])) if source_health_report else None,
            "source_health_triage": len(source_health_report.get("triage_sources", [])) if source_health_report else None,
            "recommendation_count": len(recommendations),
            "blocking_recommendations": sum(1 for item in recommendations if item.severity == "blocking"),
        }
        return CommandCenterResult(
            summary=summary,
            research_brief_queue=research_brief_queue,
            research_leads=research_leads,
            source_health=source_health_report,
            recent_agent_runs=recent_agent_runs,
            recommendations=recommendations,
            errors=errors,
        )

    def run_next_research_brief_queue_item(
        self,
        request: ResearchBriefQueueRunRequest,
    ) -> ResearchBriefQueueRunResult:
        if request.queue_item_ids:
            statuses = set(request.statuses)
            topic_query = (request.topic_query or "").casefold().strip()
            seen: set[UUID] = set()
            candidates = []
            for queue_item_id in request.queue_item_ids:
                if queue_item_id in seen:
                    continue
                seen.add(queue_item_id)
                item = self.repository.get_research_brief_queue_item(queue_item_id)
                if item is None:
                    continue
                if statuses and item.status not in statuses:
                    continue
                if request.source_key and item.source_key != request.source_key:
                    continue
                if topic_query and topic_query not in f"{item.topic} {item.disease_scope}".casefold():
                    continue
                candidates.append(item)
            candidates = sorted(candidates, key=lambda item: (item.priority, item.updated_at))[: request.limit]
        else:
            candidates = self.repository.list_research_brief_queue_items(
                statuses=list(request.statuses),
                source_key=request.source_key,
                topic_query=request.topic_query,
                limit=request.limit,
            )
        if not candidates:
            return ResearchBriefQueueRunResult(ran=False)

        item = candidates[0]
        running = self.repository.update_research_brief_queue_item(
            item.queue_item_id,
            status="running",
            attempts=item.attempts + 1,
            last_error=None,
        ) or item
        try:
            brief = self.run_research_brief(_brief_request_from_queue_item(running, request))
            completion_error = _research_brief_completion_error(brief)
            if completion_error is not None:
                failed = self.repository.update_research_brief_queue_item(
                    running.queue_item_id,
                    status="failed",
                    last_brief_id=brief.brief_id,
                    last_agent_run_id=brief.agent_run_id,
                    last_error=completion_error,
                ) or running
                return ResearchBriefQueueRunResult(
                    ran=True,
                    queue_item=failed,
                    brief=brief,
                    errors=[completion_error, *brief.errors],
                )
            completed = self.repository.update_research_brief_queue_item(
                running.queue_item_id,
                status="completed",
                last_brief_id=brief.brief_id,
                last_agent_run_id=brief.agent_run_id,
                last_error=None,
            ) or running
            return ResearchBriefQueueRunResult(ran=True, queue_item=completed, brief=brief)
        except Exception as exc:
            failed = self.repository.update_research_brief_queue_item(
                running.queue_item_id,
                status="failed",
                last_error=str(exc),
            ) or running
            return ResearchBriefQueueRunResult(ran=True, queue_item=failed, errors=[str(exc)])

    def run_retrieval_smoke(self, request: RetrievalSmokeRequest) -> RetrievalSmokeResult:
        """Exercise the full read path: search, chunk context, and parent object."""

        search = self.search_research_chunks(
            ResearchChunkSearchRequest(
                query=request.query,
                source_key=request.source_key,
                object_type=request.object_type,
                embedding_model=request.embedding_model,
                limit=request.limit,
                max_chunk_chars=request.max_chunk_chars,
                include_keyword_fallback=request.include_keyword_fallback,
            )
        )
        errors: list[str] = []
        chunk_context: ChunkContextResult | None = None
        research_object: ResearchObjectReadResult | None = None
        selected_chunk_id: UUID | None = None
        selected_research_object_id: UUID | None = None

        if request.require_embedding and search.search_mode != "embedding":
            errors.append(f"expected embedding search, got {search.search_mode}")

        if not search.results:
            errors.append("search_research_chunks returned no results")
        else:
            first_hit = search.results[0]
            selected_chunk_id = first_hit.chunk.id
            selected_research_object_id = first_hit.chunk.research_object_id
            chunk_context = self.get_chunk_context(
                ChunkContextRequest(
                    chunk_id=first_hit.chunk.id,
                    window=request.context_window,
                    max_chunk_chars=request.max_chunk_chars,
                    include_entity_mentions=request.include_entity_mentions,
                )
            )
            if chunk_context is None:
                errors.append(f"get_chunk_context returned no result for chunk {first_hit.chunk.id}")

            research_object = self.get_research_object(
                ResearchObjectReadRequest(
                    research_object_id=first_hit.chunk.research_object_id,
                    include_chunks=True,
                    max_chunks=max(request.limit, 1),
                    max_chunk_chars=request.max_chunk_chars,
                )
            )
            if research_object is None:
                errors.append(
                    f"get_research_object returned no result for object {first_hit.chunk.research_object_id}"
                )

        return RetrievalSmokeResult(
            request=request,
            passed=not errors,
            errors=errors,
            selected_chunk_id=selected_chunk_id,
            selected_research_object_id=selected_research_object_id,
            search=search,
            chunk_context=chunk_context,
            research_object=research_object,
        )

    def curate_claims(self, request: ClaimCurationRequest) -> ClaimCurationResult:
        if not hasattr(self.repository, "list_claims") or not hasattr(self.repository, "upsert_claim"):
            raise RuntimeError("Claim curation requires a SQL repository")
        return AgentRunner(self.repository).run(
            agent_name="claim_curator_agent",
            model_profile=request.model_profile,
            input_payload=request.model_dump(mode="json"),
            source_key=request.source_key,
            execute=lambda: ClaimCuratorAgent(self.repository).curate(request),
            summarize=lambda result: {
                "claims_seen": result.claims_seen,
                "promoted": result.promoted,
                "merged_duplicates": result.merged_duplicates,
                "needs_review": result.needs_review,
                "rejected": result.rejected,
                "dry_run": result.dry_run,
            },
        )

    def scout_sources(self, request: SourceScoutRequest) -> SourceScoutResult:
        if not hasattr(self.repository, "coverage_summary"):
            raise RuntimeError("Source scouting requires a SQL repository")
        return AgentRunner(self.repository).run(
            agent_name="source_scout_agent",
            model_profile=request.model_profile,
            input_payload=request.model_dump(mode="json"),
            execute=lambda: SourceScoutAgent(self.repository).scout(request),
            summarize=lambda result: {
                "focus": result.focus,
                "recommendations": len(result.recommendations),
                "errors": len(result.errors),
            },
        )

    def triage_full_text_issue(self, request: FullTextTriageRequest) -> FullTextTriageResult:
        return AgentRunner(self.repository).run(
            agent_name=FullTextTriageAgent.triage_name,
            model_profile=request.model_profile,
            input_payload=request.model_dump(mode="json"),
            source_key=request.source_key,
            partition_date=request.metadata.get("partition_date"),
            execute=lambda: FullTextTriageAgent().triage(request),
            summarize=lambda result: {
                "source_key": result.source_key,
                "action": result.action,
                "severity": result.severity,
                "should_retry": result.should_retry,
                "should_block_schedule": result.should_block_schedule,
            },
        )

    def run_full_text_ops(self, request: FullTextOpsRequest) -> FullTextOpsResult:
        source_key = request.source_keys[0] if len(request.source_keys) == 1 else None
        return AgentRunner(self.repository).run(
            agent_name=FULL_TEXT_OPS_AGENT_NAME,
            agent_version=FULL_TEXT_OPS_AGENT_VERSION,
            model_profile=request.model_profile,
            input_payload=request.model_dump(mode="json"),
            source_key=source_key,
            partition_date=request.partition_date,
            dagster_run_id=request.dagster_run_id,
            execute=lambda: FullTextOpsAgent(self.repository).run(request),
            summarize=lambda result: {
                "actions": len(result.actions),
                "blocking_actions": sum(1 for action in result.actions if action.severity == "blocking"),
                "should_block_schedule": result.should_block_schedule,
                "schedule_readiness": result.schedule_readiness,
            },
        )

    def run_x_topic_review(self, request: XTopicReviewRequest) -> XTopicReviewResult:
        result = AgentRunner(self.repository).run(
            agent_name=X_TOPIC_REVIEW_AGENT_NAME,
            agent_version=X_TOPIC_REVIEW_AGENT_VERSION,
            model_profile=request.model_profile,
            input_payload=request.model_dump(mode="json"),
            source_key="x_topic_monitor",
            dagster_run_id=request.dagster_run_id,
            execute=lambda: XTopicReviewAgent().run(request),
            summarize=lambda result: {
                "actions": len(result.actions),
                "ingestion_candidate_count": result.ingestion_candidate_count,
                "needs_human_review_count": result.needs_human_review_count,
                "rejected_count": result.rejected_count,
            },
        )
        persist_research_leads_from_agent_result(self.repository, result)
        return result

    def run_x_linked_article_followup(
        self,
        request: XLinkedArticleFollowupRequest,
    ) -> XLinkedArticleFollowupResult:
        return run_x_linked_article_followup(self.repository, request)

    def run_x_linked_article_review(
        self,
        request: XLinkedArticleReviewRequest,
    ) -> XLinkedArticleReviewResult:
        result = AgentRunner(self.repository).run(
            agent_name=X_LINKED_ARTICLE_REVIEW_AGENT_NAME,
            agent_version=X_LINKED_ARTICLE_REVIEW_AGENT_VERSION,
            model_profile=request.model_profile,
            input_payload=request.model_dump(mode="json"),
            source_key="x_linked_article",
            dagster_run_id=request.dagster_run_id,
            execute=lambda: XLinkedArticleReviewAgent(self.repository).run(request),
            summarize=lambda result: {
                "actions": len(result.actions),
                "queue_candidate_count": result.queue_candidate_count,
                "needs_human_review_count": result.needs_human_review_count,
                "rejected_count": result.rejected_count,
            },
        )
        persist_research_leads_from_agent_result(self.repository, result)
        return result

    def queue_source_followups(self, request: SourceFollowupQueueRequest) -> SourceFollowupQueueResult:
        return queue_source_followups_from_scrape_reviews(self.repository, request)

    def queue_unpaywall_doi_followups(
        self,
        request: DoiOpenAccessFollowupQueueRequest,
    ) -> SourceFollowupQueueResult:
        return queue_unpaywall_doi_followups(self.repository, request)

    def ingest_source_followups(self, request: SourceFollowupIngestRequest) -> SourceFollowupIngestResult:
        return ingest_source_followups(self.repository, request)

    def list_source_followups(
        self,
        *,
        source_key: str | None = None,
        status: str | None = None,
        identifier_type: str | None = None,
        limit: int | None = None,
    ) -> list[SourceFollowupQueueItem]:
        return self.repository.list_source_followups(
            source_key=source_key,
            status=status,
            identifier_type=identifier_type,
            limit=limit,
        )

    def collect_research_leads(self, request: ResearchLeadCollectRequest) -> ResearchLeadCollectResult:
        return collect_research_leads_from_agent_runs(self.repository, request)

    def resolve_research_followups(
        self,
        request: ResearchFollowupResolverRequest,
    ) -> ResearchFollowupResolverResult:
        return AgentRunner(self.repository).run(
            agent_name=RESEARCH_FOLLOWUP_RESOLVER_AGENT_NAME,
            agent_version=RESEARCH_FOLLOWUP_RESOLVER_AGENT_VERSION,
            model_profile="deterministic_resolver",
            input_payload=request.model_dump(mode="json"),
            dagster_run_id=request.dagster_run_id,
            metadata=request.metadata,
            execute=lambda: resolve_research_followup_leads(self.repository, request),
            summarize=summarize_research_followup_resolver,
        )

    def build_validation_gap_source_pack(
        self,
        request: ValidationGapSourcePackRequest,
    ) -> ValidationGapSourcePackResult:
        return AgentRunner(self.repository).run(
            agent_name=VALIDATION_GAP_SOURCE_PACK_AGENT_NAME,
            agent_version=VALIDATION_GAP_SOURCE_PACK_AGENT_VERSION,
            model_profile="deterministic_query_builder",
            input_payload=request.model_dump(mode="json"),
            dagster_run_id=request.dagster_run_id,
            metadata=request.metadata,
            execute=lambda: build_validation_gap_source_pack(self.repository, request),
            summarize=summarize_validation_gap_source_pack,
        )

    def ingest_validation_gap_source_queries(
        self,
        request: ValidationGapSourceIngestRequest,
    ) -> ValidationGapSourceIngestResult:
        return ingest_validation_gap_source_queries(self.repository, request)

    def repair_pubmed_identifiers(
        self,
        request: PubMedIdentifierRepairRequest,
    ) -> PubMedIdentifierRepairResult:
        return repair_pubmed_identifier_metadata(self.repository, request)

    def run_research_followup_loop(
        self,
        request: ResearchFollowupLoopRequest,
    ) -> ResearchFollowupLoopResult:
        return AgentRunner(self.repository).run(
            agent_name="research_followup_loop_agent",
            agent_version="v1",
            model_profile=request.model_profile,
            input_payload=request.model_dump(mode="json"),
            dagster_run_id=request.dagster_run_id,
            metadata=request.metadata,
            execute=lambda: self._execute_research_followup_loop(request),
            summarize=_summarize_research_followup_loop,
        )

    def _execute_research_followup_loop(
        self,
        request: ResearchFollowupLoopRequest,
    ) -> ResearchFollowupLoopResult:
        lead = self.repository.get_research_lead(request.lead_id)
        result = ResearchFollowupLoopResult(
            lead_id=request.lead_id,
            dry_run=request.dry_run,
            followup_lane=request.followup_lane,
            estimated_cost_usd=round(request.estimated_evaluator_cost_usd if request.evaluate else 0.0, 6),
        )
        if lead is None:
            result.errors.append("Research lead not found.")
            return result

        result.lead_status_before = lead.status
        origin_review_ids = [lead.origin_review_id] if lead.origin_review_id else []
        origin_agent_run_ids = [lead.origin_agent_run_id] if lead.origin_agent_run_id else []
        source_keys = request.source_keys or lead.suggested_sources

        if request.ingest:
            if not request.dry_run:
                self._transition_research_followup_lead(
                    result,
                    request.lead_id,
                    "queued",
                    request.operator,
                    {"phase": "followup_search_started"},
                )
            ingest_result = self.ingest_validation_gap_source_queries(
                ValidationGapSourceIngestRequest(
                    source_keys=source_keys,
                    query_names=request.query_names,
                    followup_lane=request.followup_lane,
                    origin_review_ids=origin_review_ids,
                    origin_agent_run_ids=origin_agent_run_ids,
                    limit_per_query=request.limit_per_query,
                    max_queries=request.max_queries,
                    dry_run=request.dry_run,
                    metadata={"research_followup_loop": True, **request.metadata},
                )
            )
            result.ingest_result = ingest_result
            result.query_count = ingest_result.query_count
            result.raw_records = ingest_result.raw_records
            result.research_objects = ingest_result.research_objects
            result.document_chunks = ingest_result.document_chunks
            result.errors.extend(ingest_result.errors)
            evidence_fit = assess_research_followup_ingest_evidence_fit(self.repository, lead, ingest_result)
            result.evidence_fit = evidence_fit
            source_followup_links = _SourceFollowupLinkStats()
            if request.queue_identifier_followups:
                source_followup_links = self._queue_identifier_followups_from_ingest_result(
                    lead,
                    ingest_result,
                    request,
                )
                result.source_followups_linked = source_followup_links.linked
                result.source_followups_queued = source_followup_links.newly_queued
                result.source_followups_newly_queued = source_followup_links.newly_queued
                result.source_followups_preexisting = source_followup_links.preexisting
                result.source_followups_already_ingested = source_followup_links.already_ingested
                result.source_followups_pending = source_followup_links.pending
                if not request.ingest_identifier_followups and not request.dry_run:
                    result.source_followups_pending += source_followup_links.newly_queued
            if request.ingest_identifier_followups and source_followup_links.ingestable_followup_ids and not request.dry_run:
                source_followup_result = self.ingest_source_followups(
                    SourceFollowupIngestRequest(
                        followup_ids=source_followup_links.ingestable_followup_ids[: request.max_identifier_followups],
                        statuses=["queued", "approved"],
                        limit=request.max_identifier_followups,
                        approved_by=request.operator,
                        run_claim_extraction=False,
                        dry_run=False,
                        metadata={"research_followup_loop": True, **request.metadata},
                    )
                )
                result.source_followup_result = source_followup_result
                result.source_followups_ingested = source_followup_result.ingested
                result.source_followups_ingested_this_run = source_followup_result.ingested
                result.source_followup_document_chunks = source_followup_result.document_chunks
                result.source_followups_pending = self._count_pending_source_followups(
                    source_followup_links.followup_ids
                )
                result.errors.extend(source_followup_result.errors)
            if request.run_claim_extraction and not request.dry_run:
                claim_result = self._extract_research_followup_claims(ingest_result, result.source_followup_result)
                result.claim_chunks_seen = claim_result.chunks_seen
                result.claims_extracted = claim_result.claims_extracted
                result.claims_written = claim_result.claims_written
                result.claim_extraction_errors = claim_result.errors
                result.errors.extend(f"claim_extraction: {error}" for error in claim_result.errors)
            if not request.dry_run:
                next_status = "watching" if evidence_fit.fit == "strong" else "followup"
                self._transition_research_followup_lead(
                    result,
                    request.lead_id,
                    next_status,
                    request.operator,
                    {
                        "phase": "followup_search_completed",
                        "query_count": ingest_result.query_count,
                        "raw_records": ingest_result.raw_records,
                        "research_objects": ingest_result.research_objects,
                        "document_chunks": ingest_result.document_chunks,
                        "failed_query_count": ingest_result.failed_query_count,
                        "source_followups_linked": result.source_followups_linked,
                        "source_followups_queued": result.source_followups_queued,
                        "source_followups_newly_queued": result.source_followups_newly_queued,
                        "source_followups_preexisting": result.source_followups_preexisting,
                        "source_followups_already_ingested": result.source_followups_already_ingested,
                        "source_followups_pending": result.source_followups_pending,
                        "source_followups_ingested": result.source_followups_ingested,
                        "source_followups_ingested_this_run": result.source_followups_ingested_this_run,
                        "source_followup_document_chunks": result.source_followup_document_chunks,
                        "claim_chunks_seen": result.claim_chunks_seen,
                        "claims_extracted": result.claims_extracted,
                        "claims_written": result.claims_written,
                        "claim_extraction_errors": result.claim_extraction_errors[:10],
                        "evidence_fit": evidence_fit.model_dump(mode="json"),
                    },
                )

        resolver_result: ResearchFollowupResolverResult | None = None
        if request.resolve:
            try:
                resolver_result = self.resolve_research_followups(
                    ResearchFollowupResolverRequest(
                        lead_ids=[request.lead_id],
                        statuses=["queued", "watching", "followup", "new"],
                        search_source_keys=source_keys,
                        limit=1,
                        ingest_source_followups=True,
                        search_missing_identifiers=True,
                        promote_ready_leads=True,
                        run_claim_extraction=True,
                        dry_run=request.dry_run,
                        force_live_search=request.force_live_search,
                        inspect_evidence_refs=True,
                        min_evidence_chunks=request.min_evidence_chunks,
                        evidence_inspection_limit=8,
                        search_limit_per_source=request.search_limit_per_source,
                        approved_by=request.operator,
                        metadata={"research_followup_loop": True, **request.metadata},
                    )
                )
                result.resolver_result = resolver_result
                result.resolver_agent_run_id = resolver_result.agent_run_id
                result.errors.extend(resolver_result.errors)
            except Exception as exc:
                result.errors.append(f"resolver_failed: {exc}")

        if request.evaluate and resolver_result and resolver_result.agent_run_id and not request.dry_run:
            try:
                evaluation_result = self.run_agent_performance_evaluation(
                    AgentPerformanceEvaluationRequest(
                        agent_run_ids=[resolver_result.agent_run_id],
                        status=None,
                        limit=1,
                        reviewed_only=False,
                        model_profile=request.model_profile,
                        review_models=request.review_models,
                        metadata={"research_followup_loop": True, **request.metadata},
                    )
                )
                result.evaluation_result = evaluation_result
                result.evaluator_agent_run_id = evaluation_result.agent_run_id
                result.actual_cost_usd = round(
                    sum(
                        _agent_run_review_cost_usd(review)
                        for review_id in evaluation_result.review_ids
                        if (review := self.repository.get_agent_run_review(review_id)) is not None
                    ),
                    6,
                )
                if evaluation_result.evaluations:
                    verdict = str(evaluation_result.evaluations[0].get("verdict") or "")
                    if verdict in {"useful", "needs_followup", "bad", "unclear"}:
                        result.latest_evaluator_verdict = verdict  # type: ignore[assignment]
                        next_status = "ingested" if verdict == "useful" else "watching" if verdict == "unclear" else "followup"
                        self._transition_research_followup_lead(
                            result,
                            request.lead_id,
                            next_status,
                            request.operator,
                            {
                                "phase": "followup_evaluated",
                                "verdict": verdict,
                                "evaluator_agent_run_id": str(evaluation_result.agent_run_id)
                                if evaluation_result.agent_run_id
                                else None,
                                "actual_cost_usd": result.actual_cost_usd,
                            },
                        )
                result.errors.extend(evaluation_result.errors)
            except Exception as exc:
                result.errors.append(f"evaluation_failed: {exc}")
                self._transition_research_followup_lead(
                    result,
                    request.lead_id,
                    "followup",
                    request.operator,
                    {"phase": "followup_evaluation_failed", "error": str(exc)},
                )

        final_lead = self.repository.get_research_lead(request.lead_id)
        result.lead_status_after = final_lead.status if final_lead else result.lead_status_before
        return result

    def _queue_identifier_followups_from_ingest_result(
        self,
        lead: ResearchLeadRecord,
        ingest_result: ValidationGapSourceIngestResult,
        request: ResearchFollowupLoopRequest,
    ) -> _SourceFollowupLinkStats:
        fetch_run_ids = _fetch_run_ids_from_ingest_results(ingest_result)
        if not fetch_run_ids:
            return _SourceFollowupLinkStats()
        chunks = self.repository.list_document_chunks_for_fetch_runs(fetch_run_ids, limit=500)
        objects = []
        seen_object_ids: set[UUID] = set()
        for chunk in chunks:
            if chunk.research_object_id in seen_object_ids:
                continue
            obj = self.repository.get_research_object(chunk.research_object_id)
            if obj is None:
                continue
            objects.append(obj)
            seen_object_ids.add(chunk.research_object_id)

        stats = _SourceFollowupLinkStats()
        seen_identity_keys: set[str] = set()
        existing_by_identity_key = {
            item.identity_key: item
            for item in self.repository.list_source_followups(limit=None)
            if item.identity_key
        }
        for obj in objects:
            for item in _source_followup_items_for_research_object(lead, obj, request):
                if item.identity_key in seen_identity_keys:
                    continue
                seen_identity_keys.add(item.identity_key or "")
                existing = existing_by_identity_key.get(item.identity_key)
                if request.dry_run:
                    persisted = existing or item
                    if existing is None:
                        stats.newly_queued += 1
                else:
                    if existing is None:
                        persisted = self.repository.upsert_source_followup(item)
                        existing_by_identity_key[persisted.identity_key] = persisted
                        stats.newly_queued += 1
                    else:
                        persisted = existing
                stats.linked += 1
                stats.followup_ids.append(persisted.followup_id)
                if existing is not None:
                    stats.preexisting += 1
                    if existing.status == "ingested":
                        stats.already_ingested += 1
                    if existing.status in {"queued", "approved"}:
                        stats.pending += 1
                if persisted.status in {"queued", "approved"}:
                    stats.ingestable_followup_ids.append(persisted.followup_id)
                if stats.linked >= request.max_identifier_followups:
                    return stats
        return stats

    def _extract_research_followup_claims(
        self,
        ingest_result: ValidationGapSourceIngestResult,
        source_followup_result: SourceFollowupIngestResult | None,
    ):
        fetch_run_ids = _fetch_run_ids_from_ingest_results(ingest_result)
        if source_followup_result is not None:
            fetch_run_ids.extend(
                item.fetch_run_id
                for item in source_followup_result.items
                if item.fetch_run_id is not None and item.document_chunks > 0
            )
        fetch_run_ids = _dedupe_uuids(fetch_run_ids)
        chunks = self.repository.list_document_chunks_for_fetch_runs(fetch_run_ids, limit=1000)
        return extract_claims_for_chunks(self.repository, chunks)

    def _count_pending_source_followups(self, followup_ids: list[UUID]) -> int:
        return sum(
            1
            for followup_id in followup_ids
            if (item := self.repository.get_source_followup(followup_id)) is not None
            and item.status in {"queued", "approved"}
        )

    def _transition_research_followup_lead(
        self,
        result: ResearchFollowupLoopResult,
        lead_id: UUID,
        status: str,
        operator: str,
        metadata: dict[str, Any],
    ) -> None:
        before = self.repository.get_research_lead(lead_id)
        updated = self.update_research_lead(
            lead_id,
            status=status,
            metadata={
                "research_followup_loop": {
                    "operator": operator,
                    "updated_at": datetime.now(UTC).isoformat(),
                    **metadata,
                }
            },
        )
        if updated is not None:
            result.status_transitions.append(
                {
                    "from": before.status if before else None,
                    "to": updated.status,
                    "metadata": metadata,
                }
            )

    def get_research_lead(self, lead_id: UUID) -> ResearchLeadRecord | None:
        return self.repository.get_research_lead(lead_id)

    def list_research_leads(
        self,
        *,
        status: str | None = None,
        statuses: list[str] | None = None,
        lead_type: str | None = None,
        source_key: str | None = None,
        limit: int | None = 50,
    ) -> list[ResearchLeadRecord]:
        return self.repository.list_research_leads(
            status=status,
            statuses=statuses,
            lead_type=lead_type,
            source_key=source_key,
            limit=limit,
        )

    def update_research_lead(
        self,
        lead_id: UUID,
        *,
        status: str | None = None,
        metadata: dict | None = None,
    ) -> ResearchLeadRecord | None:
        return self.repository.update_research_lead(lead_id, status=status, metadata=metadata)

    def get_agent_run(self, agent_run_id: UUID) -> AgentRunRecord | None:
        return self.repository.get_agent_run(agent_run_id)

    def list_agent_runs(
        self,
        *,
        agent_name: str | None = None,
        status: str | None = None,
        source_key: str | None = None,
        limit: int = 50,
    ) -> list[AgentRunRecord]:
        return self.repository.list_agent_runs(
            agent_name=agent_name,
            status=status,
            source_key=source_key,
            limit=limit,
        )

    def create_agent_run_review(self, record: AgentRunReviewRecord) -> AgentRunReviewRecord:
        return self.repository.create_agent_run_review(record)

    def get_agent_run_review(self, review_id: UUID) -> AgentRunReviewRecord | None:
        return self.repository.get_agent_run_review(review_id)

    def list_agent_run_reviews(
        self,
        *,
        agent_run_id: UUID | None = None,
        verdict: str | None = None,
        reviewer: str | None = None,
        limit: int = 50,
    ) -> list[AgentRunReviewRecord]:
        return self.repository.list_agent_run_reviews(
            agent_run_id=agent_run_id,
            verdict=verdict,
            reviewer=reviewer,
            limit=limit,
        )

    def build_agent_performance_report(
        self,
        request: AgentPerformanceReportRequest,
    ) -> AgentPerformanceReportResult:
        return build_agent_performance_report(self.repository, request)

    def run_agent_performance_evaluation(
        self,
        request: AgentPerformanceEvaluationRequest,
    ) -> AgentPerformanceEvaluationResult:
        return AgentRunner(self.repository).run(
            agent_name=AGENT_PERFORMANCE_EVALUATOR_AGENT_NAME,
            agent_version=AGENT_PERFORMANCE_EVALUATOR_AGENT_VERSION,
            model_profile=request.model_profile,
            input_payload={"request": request.model_dump(mode="json")},
            execute=lambda: run_agent_performance_evaluation(self.repository, request),
            source_key=request.source_key,
            dagster_run_id=request.dagster_run_id,
            metadata=request.metadata,
            summarize=summarize_agent_performance_evaluation,
        )

    def escalate_agent_findings(
        self,
        request: AgentFindingEscalationRequest,
    ) -> AgentFindingEscalationResult:
        return AgentRunner(self.repository).run(
            agent_name=AGENT_FINDING_ESCALATION_AGENT_NAME,
            agent_version=AGENT_FINDING_ESCALATION_AGENT_VERSION,
            model_profile="deterministic_escalation",
            input_payload={"request": request.model_dump(mode="json")},
            execute=lambda: escalate_agent_findings(self.repository, request),
            metadata=request.metadata,
            summarize=summarize_agent_finding_escalation,
        )

    def refine_research_followups(
        self,
        request: ResearchFollowupRefinementRequest,
    ) -> ResearchFollowupRefinementResult:
        return AgentRunner(self.repository).run(
            agent_name=RESEARCH_FOLLOWUP_REFINEMENT_AGENT_NAME,
            agent_version=RESEARCH_FOLLOWUP_REFINEMENT_AGENT_VERSION,
            model_profile="deterministic_refinement",
            input_payload={"request": request.model_dump(mode="json")},
            execute=lambda: refine_research_followups(self.repository, request),
            metadata=request.metadata,
            summarize=summarize_research_followup_refinement,
        )

    def get_candidate(self, request: CandidateDossierRequest) -> CandidateDossier | None:
        return self.repository.get_candidate(request)

    def propose_hypothesis(self, request: HypothesisProposalRequest) -> HypothesisDraft:
        claims = []
        if request.claim_ids:
            claims = [claim for claim_id in request.claim_ids if (claim := self.repository.get_claim(claim_id))]
        else:
            search = ClaimSearchRequest(
                query=request.objective,
                species=request.species,
                limit=request.max_supporting_claims,
                min_confidence=0.0,
            )
            claims = self.repository.search_claims(search)
            if not claims and request.candidate_name:
                claims = self.repository.search_claims(
                    ClaimSearchRequest(
                        compounds=[request.candidate_name],
                        species=request.species,
                        limit=request.max_supporting_claims,
                    )
                )
            if not claims and request.target_name:
                claims = self.repository.search_claims(
                    ClaimSearchRequest(
                        targets=[request.target_name],
                        species=request.species,
                        limit=request.max_supporting_claims,
                    )
                )

        title_target = request.candidate_name or request.target_name or "HSA research hypothesis"
        rationale_parts = [claim.statement for claim in claims[: request.max_supporting_claims]]
        rationale = " ".join(rationale_parts) if rationale_parts else "No supporting claims were supplied yet."
        draft = HypothesisDraft(
            hypothesis_id=uuid4() if request.commit else None,
            title=f"{title_target}: proposed HSA hypothesis",
            hypothesis=(
                f"{title_target} may be worth prioritizing for {request.species} HSA research "
                f"because the current claim set suggests a testable evidence path."
            ),
            rationale=rationale,
            status="proposed" if request.commit else "draft",
            supporting_claim_ids=[claim.claim_id for claim in claims],
            confidence=0.35 if claims else 0.1,
            proposed_by="mcp",
            metadata={"model_profile": request.model_profile, "committed": request.commit},
        )

        if request.commit:
            return self.repository.commit_hypothesis(
                CommitHypothesisRequest(draft=draft, approved_by="system", approval_note="Auto-committed by request")
            )
        return draft

    def commit_hypothesis(self, request: CommitHypothesisRequest) -> HypothesisDraft:
        return self.repository.commit_hypothesis(request)

    def run_boltz(self, request: BoltzRunRequest) -> AsyncRunHandle:
        validation = ValidationRequest(
            validation_type="boltz",
            candidate_id=request.candidate_id,
            candidate_name=request.ligand_name,
            target_name=request.target_name,
            objective=f"Run Boltz prediction for {request.target_name}",
            priority=request.priority,
            require_approval=request.require_approval,
            metadata=request.metadata
            | {
                "ligand_smiles": request.ligand_smiles,
                "protein_sequence_supplied": request.protein_sequence is not None,
            },
        )
        return self.repository.enqueue_validation(validation)

    def request_validation(self, request: ValidationRequest) -> AsyncRunHandle:
        return self.repository.enqueue_validation(request)

    def get_run_status(self, run_id: UUID) -> AsyncRunHandle | None:
        return self.repository.get_run_status(run_id)

    def get_artifact(self, artifact_id: UUID):
        return self.repository.get_artifact(artifact_id)

    def list_model_profiles(self) -> list[ModelProfile]:
        return list(self.model_profiles.values())

    def set_model_profile(self, profile: ModelProfile) -> ModelProfile:
        self.model_profiles[profile.profile_key] = profile
        return profile

    def _select_embedding_model(self, request: ResearchChunkSearchRequest) -> str:
        if request.embedding_model:
            return request.embedding_model
        coverage = self.repository.embedding_coverage(
            source_key=request.source_key,
            object_type=str(request.object_type) if request.object_type else None,
        )
        if coverage.embedding_models:
            return select_embedding_model_from_coverage(coverage.embedding_models)
        return LOCAL_HASH_EMBEDDING_MODEL

    def _search_chunks_with_embeddings(
        self,
        request: ResearchChunkSearchRequest,
        embedding_model: str,
    ) -> list[ResearchChunkSearchResult]:
        query_vector = self._embed_query_for_model(request, embedding_model)
        if not any(query_vector):
            return []
        fetch_limit = min(max(request.limit * 20, request.limit), 100)
        embedding_hits = self.repository.search_text_embeddings(
            TextEmbeddingSearchRequest(
                query_embedding=query_vector,
                embedding_model=embedding_model,
                source_key=request.source_key,
                research_object_id=request.research_object_id,
                object_type=request.object_type,
                min_score=request.min_score,
                limit=fetch_limit,
            )
        )

        results: list[ResearchChunkSearchResult] = []
        seen_chunks: set[UUID] = set()
        for hit in embedding_hits:
            chunk = self.repository.get_document_chunk(hit.embedding.chunk_id)
            if chunk is None or chunk.id in seen_chunks:
                continue
            research_object = self.repository.get_research_object(chunk.research_object_id)
            seen_chunks.add(chunk.id)
            results.append(
                ResearchChunkSearchResult(
                    rank=len(results) + 1,
                    chunk=_truncate_chunk(chunk, request.max_chunk_chars),
                    research_object=research_object,
                    score=hit.score,
                    match_type="embedding",
                    text_truncated=len(chunk.text_content) > request.max_chunk_chars,
                )
            )
            if len(results) >= request.limit:
                break
        return results

    def _embed_query_for_model(self, request: ResearchChunkSearchRequest, embedding_model: str) -> list[float]:
        dimensions = None
        existing = self.repository.list_text_embeddings(
            embedding_model=embedding_model,
            source_key=request.source_key,
            research_object_id=request.research_object_id,
            object_type=str(request.object_type) if request.object_type else None,
            limit=1,
        )
        if existing:
            dimensions = existing[0].embedding_dimensions
        provider = build_embedding_provider(embedding_model, dimensions=dimensions)
        return provider.embed_text(request.query)

    @staticmethod
    def _merge_embedding_and_keyword_results(
        embedding_results: list[ResearchChunkSearchResult],
        keyword_results: list[ResearchChunkSearchResult],
        limit: int,
    ) -> list[ResearchChunkSearchResult]:
        by_chunk: dict[UUID, dict[str, Any]] = {}
        for result in embedding_results:
            by_chunk[result.chunk.id] = {
                "embedding": result,
                "embedding_score": result.score,
                "keyword": None,
                "keyword_score": None,
            }
        for result in keyword_results:
            entry = by_chunk.setdefault(
                result.chunk.id,
                {
                    "embedding": None,
                    "embedding_score": None,
                    "keyword": result,
                    "keyword_score": result.score,
                },
            )
            entry["keyword"] = result
            entry["keyword_score"] = result.score

        ranked = sorted(
            by_chunk.values(),
            key=lambda entry: (
                -_hybrid_chunk_score(entry["embedding_score"], entry["keyword_score"]),
                -float(entry["keyword_score"] or 0.0),
                -float(entry["embedding_score"] or -1.0),
                str((entry["embedding"] or entry["keyword"]).chunk.research_object_id),
                (entry["embedding"] or entry["keyword"]).chunk.chunk_index,
            ),
        )
        results: list[ResearchChunkSearchResult] = []
        for entry in ranked[:limit]:
            result = entry["embedding"] or entry["keyword"]
            score = _hybrid_chunk_score(entry["embedding_score"], entry["keyword_score"])
            results.append(
                result.model_copy(
                    update={
                        "rank": len(results) + 1,
                        "score": score,
                        "match_type": "embedding" if entry["embedding"] else "keyword",
                    }
                )
            )
        return results

    @staticmethod
    def _truncate_search_result(
        result: ResearchChunkSearchResult,
        max_chunk_chars: int,
    ) -> ResearchChunkSearchResult:
        truncated = _truncate_chunk(result.chunk, max_chunk_chars)
        return result.model_copy(
            update={
                "chunk": truncated,
                "text_truncated": len(result.chunk.text_content) > max_chunk_chars,
            }
        )


def _truncate_chunk(chunk: DocumentChunk, max_chars: int) -> DocumentChunk:
    if len(chunk.text_content) <= max_chars:
        return chunk
    return chunk.model_copy(
        update={
            "text_content": chunk.text_content[:max_chars],
            "metadata": {
                **chunk.metadata,
                "text_truncated": True,
                "original_text_chars": len(chunk.text_content),
            },
        }
    )


def _research_brief_record_from_result(
    result: ResearchBriefResult,
    request: ResearchBriefRequest,
) -> ResearchBriefRecord:
    finding_count = sum(len(report.findings) for report in result.perspective_reports)
    hard_errors = _research_brief_hard_errors(result)
    evidence_limitations = _research_brief_evidence_limitations(result)
    metadata = {
        **request.metadata,
        "dagster_run_id": request.dagster_run_id,
        "review_models": request.review_models,
    }
    return ResearchBriefRecord(
        brief_id=result.brief_id or uuid4(),
        agent_run_id=result.agent_run_id,
        agent_run_ids=result.agent_run_ids,
        topic=result.topic,
        disease_scope=result.disease_scope,
        source_key=request.source_key,
        brief_style=result.brief_style,
        model_profile=result.model_profile,
        review_mode=request.review_mode,
        status="failed" if _research_brief_completion_error(result) is not None else "completed",
        final_brief=result.final_brief,
        summary=summarize_research_brief(result),
        result_payload=result.model_dump(mode="json"),
        citation_count=len(result.citations),
        finding_count=finding_count,
        hypothesis_count=len(result.ranked_hypotheses),
        unresolved_question_count=len(result.unresolved_questions),
        research_lead_count=int(result.evidence.get("research_lead_count", 0)),
        hard_error_count=len(hard_errors),
        evidence_limitation_count=len(evidence_limitations),
        error_count=len(hard_errors),
        metadata={key: value for key, value in metadata.items() if value not in (None, [], {})},
    )


def _research_brief_completion_error(result: ResearchBriefResult) -> str | None:
    finding_count = sum(len(report.findings) for report in result.perspective_reports)
    missing: list[str] = []
    if not result.citations:
        missing.append("citations")
    if finding_count < 1:
        missing.append("findings")
    if not result.ranked_hypotheses:
        missing.append("ranked_hypotheses")
    if not missing:
        return None
    hard_errors = _research_brief_hard_errors(result)
    suffix = f"; agent_error_count={len(hard_errors)}" if hard_errors else ""
    return f"research brief did not meet completion bar; missing {', '.join(missing)}{suffix}"


def _research_brief_hard_errors(result: ResearchBriefResult) -> list[str]:
    if result.hard_errors:
        return list(result.hard_errors)
    hard_errors, _ = split_research_brief_errors(result.errors)
    return hard_errors


def _research_brief_evidence_limitations(result: ResearchBriefResult) -> list[str]:
    limitations = list(result.evidence_limitations)
    if not limitations and result.errors:
        _, limitations = split_research_brief_errors(result.errors)
    return limitations


def _research_brief_evaluation_record_from_result(
    result: ResearchBriefEvaluationResult,
    request: ResearchBriefEvaluationRequest,
) -> ResearchBriefEvaluationRecord:
    return ResearchBriefEvaluationRecord(
        evaluation_id=result.evaluation_id,
        brief_id=result.brief_id,
        agent_run_id=result.agent_run_id,
        topic=result.topic,
        source_key=result.source_key,
        model_profile=result.model_profile,
        overall_score=result.overall_score,
        passes_quality_bar=result.passes_quality_bar,
        readiness=result.readiness,
        summary=summarize_research_brief_evaluation(result),
        result_payload=result.model_dump(mode="json"),
        errors=result.errors,
        created_at=result.created_at,
        metadata={
            **request.metadata,
            "minimum_overall_score": request.minimum_overall_score,
        },
    )


def _research_brief_quality_row(
    brief: ResearchBriefRecord,
    evaluation: ResearchBriefEvaluationRecord | None,
) -> ResearchBriefQualityRow:
    hard_error_count, evidence_limitation_count = _research_brief_record_error_counts(brief)
    passes_completion_bar = (
        brief.status == "completed"
        and brief.citation_count > 0
        and brief.finding_count > 0
        and brief.hypothesis_count > 0
    )
    if not passes_completion_bar:
        if hard_error_count == 0 and evidence_limitation_count > 0:
            quality_status = "needs_followup_research"
        else:
            quality_status = "brief_failed"
    elif evaluation is None:
        quality_status = "needs_evaluation"
    elif evaluation.passes_quality_bar:
        quality_status = "ready_for_validation"
    else:
        quality_status = evaluation.readiness
    return ResearchBriefQualityRow(
        brief_id=brief.brief_id,
        evaluation_id=evaluation.evaluation_id if evaluation else None,
        agent_run_id=brief.agent_run_id,
        status=brief.status,
        quality_status=quality_status,
        topic=brief.topic,
        source_key=brief.source_key,
        brief_style=brief.brief_style,
        model_profile=brief.model_profile,
        review_mode=brief.review_mode,
        review_models=list(brief.metadata.get("review_models") or []),
        citation_count=brief.citation_count,
        finding_count=brief.finding_count,
        hypothesis_count=brief.hypothesis_count,
        hard_error_count=hard_error_count,
        evidence_limitation_count=evidence_limitation_count,
        error_count=hard_error_count,
        passes_completion_bar=passes_completion_bar,
        passes_quality_bar=evaluation.passes_quality_bar if evaluation else None,
        readiness=evaluation.readiness if evaluation else None,
        overall_score=evaluation.overall_score if evaluation else None,
        created_at=brief.created_at,
        updated_at=brief.updated_at,
    )


def _research_brief_record_error_counts(brief: ResearchBriefRecord) -> tuple[int, int]:
    result_payload = brief.result_payload or {}
    hard_error_count = brief.hard_error_count
    evidence_limitation_count = brief.evidence_limitation_count
    if not result_payload:
        return hard_error_count, evidence_limitation_count

    errors = _as_text_list(result_payload.get("errors", []))
    if errors and "hard_errors" not in result_payload and "evidence_limitations" not in result_payload:
        hard_errors, evidence_limitations = split_research_brief_errors(errors)
        return len(hard_errors), len(evidence_limitations)

    if "hard_errors" in result_payload:
        hard_error_count = len(_as_text_list(result_payload.get("hard_errors", [])))
    if "evidence_limitations" in result_payload:
        evidence_limitation_count = len(_as_text_list(result_payload.get("evidence_limitations", [])))
    return hard_error_count, evidence_limitation_count


def _research_brief_followup_leads_from_brief(
    brief: ResearchBriefRecord,
    row: ResearchBriefQualityRow,
    *,
    max_limitations: int,
) -> list[ResearchLeadRecord]:
    limitations = _research_brief_record_evidence_limitations(brief)[:max_limitations]
    leads: list[ResearchLeadRecord] = []
    for limitation in limitations:
        limitation_digest = hashlib.sha1(limitation.lower().encode("utf-8")).hexdigest()[:12]
        leads.append(
            ResearchLeadRecord(
                identity_key=f"research_lead:brief_followup:{brief.brief_id}:{limitation_digest}",
                title=f"Follow up research gap: {brief.topic}"[:240],
                lead_type="unknown",
                status="followup",
                priority=25,
                source_key=brief.source_key,
                origin_source_key="research_brief_quality",
                origin_record_id=str(brief.brief_id),
                origin_agent_run_id=brief.agent_run_id,
                reason=limitation[:1000],
                summary=(
                    "Research brief did not meet the completion bar because supplied evidence was insufficient. "
                    "Find durable source evidence before routing this back into synthesis."
                ),
                evidence_refs=[f"research_brief:{brief.brief_id}"],
                topic_tags=[
                    "research_brief",
                    "evidence_gap",
                    "followup_research",
                    brief.source_key or "",
                    brief.disease_scope,
                ],
                metadata={
                    "research_followup_queue": {
                        "origin": "research_brief_quality",
                        "brief_id": str(brief.brief_id),
                        "topic": brief.topic,
                        "source_key": brief.source_key,
                        "quality_status": row.quality_status,
                        "hard_error_count": row.hard_error_count,
                        "evidence_limitation_count": row.evidence_limitation_count,
                        "limitation": limitation,
                        "requires_manual_research": True,
                    }
                },
            )
        )
    return leads


def _research_brief_record_evidence_limitations(brief: ResearchBriefRecord) -> list[str]:
    result_payload = brief.result_payload or {}
    limitations: list[str] = []
    limitations.extend(_as_text_list(result_payload.get("evidence_limitations", [])))
    for report in result_payload.get("perspective_reports", []):
        if isinstance(report, dict):
            limitations.extend(_as_text_list(report.get("evidence_limitations", [])))
    if not limitations:
        errors = _as_text_list(result_payload.get("errors", []))
        _, limitations = split_research_brief_errors(errors)
    return _dedupe_texts(limitations)


def _as_text_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _dedupe_texts(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        key = normalized.lower()
        if not normalized or key in seen:
            continue
        deduped.append(normalized)
        seen.add(key)
    return deduped


def _select_brief_for_evaluation(
    repository: ResearchRepository,
    request: ResearchBriefEvaluationRequest,
) -> ResearchBriefRecord | None:
    if request.brief_id is not None:
        return repository.get_research_brief(request.brief_id)
    candidates = repository.list_research_briefs(
        status="completed",
        source_key=request.source_key,
        topic_query=request.topic_query,
        limit=request.limit,
    )
    return candidates[0] if candidates else None


def _source_key_for_validation_plan(
    repository: ResearchRepository,
    request: ValidationPlanRequest,
) -> str | None:
    if request.evaluation_id is not None:
        evaluation = repository.get_research_brief_evaluation(request.evaluation_id)
        return evaluation.source_key if evaluation else None
    if request.brief_id is not None:
        brief = repository.get_research_brief(request.brief_id)
        return brief.source_key if brief else None
    candidates = repository.list_research_briefs(
        status="completed",
        source_key=request.source_key,
        topic_query=request.topic_query,
        limit=1,
    )
    return candidates[0].source_key if candidates else None


def _select_therapy_committee_agent_run(
    repository: ResearchRepository,
    agent_run_id: UUID | None,
) -> AgentRunRecord | None:
    if agent_run_id is not None:
        record = repository.get_agent_run(agent_run_id)
        return record if record and record.agent_name == THERAPY_COMMITTEE_AGENT_NAME else None
    runs = repository.list_agent_runs(
        agent_name=THERAPY_COMMITTEE_AGENT_NAME,
        status="completed",
        limit=1,
    )
    return runs[0] if runs else None


def _validation_tasks_from_therapy_idea(
    idea: TherapyIdea,
    *,
    committee_result: TherapyCommitteeResult,
    origin_agent_run_id: UUID,
    priority: int,
) -> list[ValidationPlanTask]:
    tasks = [
        _expert_review_task_from_therapy_idea(
            idea,
            committee_result=committee_result,
            origin_agent_run_id=origin_agent_run_id,
            priority=priority,
        )
    ]
    if idea.targets or idea.biomarkers:
        tasks.append(
            _assay_design_task_from_therapy_idea(
                idea,
                committee_result=committee_result,
                origin_agent_run_id=origin_agent_run_id,
                priority=min(1000, priority + 5),
            )
        )
    if idea.candidate_therapies or idea.risks:
        tasks.append(
            _safety_review_task_from_therapy_idea(
                idea,
                committee_result=committee_result,
                origin_agent_run_id=origin_agent_run_id,
                priority=min(1000, priority + 10),
            )
        )
    return tasks


def _expert_review_task_from_therapy_idea(
    idea: TherapyIdea,
    *,
    committee_result: TherapyCommitteeResult,
    origin_agent_run_id: UUID,
    priority: int,
) -> ValidationPlanTask:
    task_id = uuid5(NAMESPACE_URL, f"therapy_committee_task:{origin_agent_run_id}:{idea.idea_id}:expert_review")
    request = ValidationRequest(
        validation_type="expert_review",
        candidate_name=_candidate_name(idea),
        target_name=_target_name(idea),
        objective=_truncate(
            "Review the therapy committee idea, evidence refs, negative evidence needs, and "
            f"promotion criteria for: {idea.hypothesis}",
            2000,
        ),
        priority=priority,
        require_approval=True,
        assay_context=_assay_context_from_therapy_idea(
            idea,
            committee_result,
            assay_type="expert evidence review",
            readout="go/no-go validation readiness, missing evidence, and contradiction map",
            endpoint="approved validation plan or demotion/follow-up decision",
        ),
        quality_gates=[
            "human_approval_required",
            "source_traceability_required",
            "committee_evidence_refs_required",
            "negative_evidence_review_required",
        ],
        metadata=_therapy_validation_metadata(idea, committee_result, origin_agent_run_id),
    )
    return ValidationPlanTask(
        task_id=task_id,
        task_type="expert_review",
        title=_truncate(f"Expert review: {idea.title}", 500),
        objective=request.objective,
        rationale=_truncate(idea.rationale, 3000),
        validation_request=request,
        required_inputs=[
            "committee-ranked therapy idea",
            "cited evidence refs",
            "evidence limitations",
            "candidate therapy and target list",
        ],
        expected_outputs=[
            "approved validation route",
            "follow-up evidence needs",
            "demotion criteria if evidence remains weak",
        ],
        evidence_refs=idea.evidence_refs,
        priority=priority,
        requires_human_approval=True,
        tool_hint="expert_review",
        metadata=_therapy_validation_metadata(idea, committee_result, origin_agent_run_id),
    )


def _assay_design_task_from_therapy_idea(
    idea: TherapyIdea,
    *,
    committee_result: TherapyCommitteeResult,
    origin_agent_run_id: UUID,
    priority: int,
) -> ValidationPlanTask:
    task_id = uuid5(NAMESPACE_URL, f"therapy_committee_task:{origin_agent_run_id}:{idea.idea_id}:wet_lab")
    target_name = _target_name(idea)
    request = ValidationRequest(
        validation_type="wet_lab",
        candidate_name=_candidate_name(idea),
        target_name=target_name,
        objective=_truncate(
            "Design a conservative assay plan to validate mechanism, biomarker selection, "
            f"and pathway readouts for {target_name or idea.title}.",
            2000,
        ),
        priority=priority,
        require_approval=True,
        assay_context=_assay_context_from_therapy_idea(
            idea,
            committee_result,
            assay_type="in vitro or ex vivo validation design",
            readout="target expression, pathway suppression, viability, and biomarker-response correlation",
            endpoint="assay-ready protocol with controls and stop criteria",
        ),
        quality_gates=[
            "human_approval_required",
            "assay_context_present",
            "source_traceability_required",
            "controls_required",
            "stop_criteria_required",
        ],
        metadata=_therapy_validation_metadata(idea, committee_result, origin_agent_run_id),
    )
    return ValidationPlanTask(
        task_id=task_id,
        task_type="wet_lab",
        title=_truncate(f"Assay design: {idea.title}", 500),
        objective=request.objective,
        rationale=_truncate(idea.mechanism or idea.rationale, 3000),
        validation_request=request,
        required_inputs=[
            "target and biomarker list",
            "candidate therapy list",
            "model system availability",
            "positive and negative controls",
        ],
        expected_outputs=[
            "assay design",
            "readout panel",
            "control strategy",
            "failure criteria",
        ],
        evidence_refs=idea.evidence_refs,
        priority=priority,
        requires_human_approval=True,
        tool_hint="wet_lab_protocol_review",
        metadata=_therapy_validation_metadata(idea, committee_result, origin_agent_run_id),
    )


def _safety_review_task_from_therapy_idea(
    idea: TherapyIdea,
    *,
    committee_result: TherapyCommitteeResult,
    origin_agent_run_id: UUID,
    priority: int,
) -> ValidationPlanTask:
    task_id = uuid5(NAMESPACE_URL, f"therapy_committee_task:{origin_agent_run_id}:{idea.idea_id}:safety")
    request = ValidationRequest(
        validation_type="safety",
        candidate_name=_candidate_name(idea),
        target_name=_target_name(idea),
        objective=_truncate(
            "Review safety, toxicity, PK/PD, hemorrhage/coagulation, and species-translation risks "
            f"before promoting: {idea.title}",
            2000,
        ),
        priority=priority,
        require_approval=True,
        assay_context=_assay_context_from_therapy_idea(
            idea,
            committee_result,
            assay_type="safety and translational risk review",
            readout="toxicity flags, coagulation risk, species PK/PD gaps, and contraindications",
            endpoint="safety gate decision before any animal-facing validation",
        ),
        quality_gates=[
            "human_approval_required",
            "source_traceability_required",
            "safety_context_required",
            "species_translation_review_required",
            "stop_criteria_required",
        ],
        metadata=_therapy_validation_metadata(idea, committee_result, origin_agent_run_id),
    )
    return ValidationPlanTask(
        task_id=task_id,
        task_type="safety",
        title=_truncate(f"Safety gate: {idea.title}", 500),
        objective=request.objective,
        rationale=_truncate(" | ".join(idea.risks) or idea.rationale, 3000),
        validation_request=request,
        required_inputs=[
            "candidate therapy list",
            "known safety risks",
            "species-specific PK/PD evidence",
            "coagulation or toxicity monitoring requirements",
        ],
        expected_outputs=[
            "safety gate decision",
            "required monitoring plan",
            "contraindications and stop criteria",
        ],
        evidence_refs=idea.evidence_refs,
        priority=priority,
        requires_human_approval=True,
        tool_hint="safety_review",
        metadata=_therapy_validation_metadata(idea, committee_result, origin_agent_run_id),
    )


def _assay_context_from_therapy_idea(
    idea: TherapyIdea,
    committee_result: TherapyCommitteeResult,
    *,
    assay_type: str,
    readout: str,
    endpoint: str,
) -> ValidationAssayContext:
    return ValidationAssayContext(
        species=["canine", "human"],
        disease_context=committee_result.disease_scope,
        model_system="canine HSA models with human angiosarcoma translational comparator",
        assay_type=assay_type,
        readout=readout,
        endpoint=endpoint,
        comparator_or_control=_truncate(
            "biomarker-negative or untreated controls; standard-of-care comparator when available",
            500,
        ),
        sample_context=_truncate(
            "canine HSA tissue, cell model, organoid, or retrospective genomic cohort matched to idea biomarkers",
            500,
        ),
        dose_or_exposure_context="to be specified after candidate identity, PK/PD, and safety review",
        safety_context=_truncate("; ".join(idea.risks) or "No explicit safety risks supplied by committee.", 500),
        evidence_refs=idea.evidence_refs,
        negative_evidence_needs=idea.risks,
        provenance_requirements=[
            "therapy_committee_agent_run_id",
            "committee_run_id",
            "citation_id_traceability",
            "human_approval_before_dispatch",
        ],
    )


def _therapy_validation_metadata(
    idea: TherapyIdea,
    committee_result: TherapyCommitteeResult,
    origin_agent_run_id: UUID,
) -> dict[str, Any]:
    return {
        "origin": "therapy_committee",
        "origin_agent_run_id": str(origin_agent_run_id),
        "committee_run_id": str(committee_result.committee_run_id),
        "idea_id": str(idea.idea_id),
        "idea_title": idea.title,
        "priority_score": idea.priority_score,
        "evidence_refs": idea.evidence_refs,
        "candidate_therapies": idea.candidate_therapies,
        "targets": idea.targets,
        "biomarkers": idea.biomarkers,
        "recommend_only": True,
    }


def _candidate_name(idea: TherapyIdea) -> str | None:
    if not idea.candidate_therapies:
        return None
    return _truncate(" / ".join(idea.candidate_therapies[:3]), 500)


def _target_name(idea: TherapyIdea) -> str | None:
    if not idea.targets:
        return None
    return _truncate(" / ".join(idea.targets[:3]), 500)


def _truncate(value: str, limit: int) -> str:
    normalized = " ".join(str(value).split())
    return normalized[:limit]


def _research_brief_topic_from_lead(lead: ResearchLeadRecord) -> str:
    title = lead.title or lead.summary or lead.reason or "Untitled research lead"
    parts = [f"Review research lead: {title}"]
    if lead.reason:
        parts.append(f"Reason: {lead.reason}")
    if lead.summary and lead.summary != title:
        parts.append(f"Summary: {lead.summary}")
    if lead.topic_tags:
        parts.append(f"Tags: {', '.join(lead.topic_tags[:8])}")
    return " | ".join(parts)[:1000]


_NON_EVIDENCE_RESEARCH_LEAD_SOURCES = {
    "x_linked_article",
    "x_topic",
    "x_topic_monitor",
}
_FOLLOWUP_SOURCE_BY_IDENTIFIER_TYPE = {
    "doi": "crossref",
    "pmid": "pubmed",
    "pmcid": "pmc_oa",
    "nct": "clinicaltrials_gov",
}


def _fetch_run_ids_from_ingest_results(ingest_result: ValidationGapSourceIngestResult) -> list[UUID]:
    return _dedupe_uuids(
        [
            result.fetch_run_id
            for result in ingest_result.results
            if result.fetch_run_id is not None and (result.raw_records or result.research_objects or result.document_chunks)
        ]
    )


def _dedupe_uuids(values: list[UUID | None]) -> list[UUID]:
    deduped: list[UUID] = []
    seen: set[UUID] = set()
    for value in values:
        if value is None or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _source_followup_items_for_research_object(
    lead: ResearchLeadRecord,
    obj: ResearchObject,
    request: ResearchFollowupLoopRequest,
) -> list[SourceFollowupQueueItem]:
    items: list[SourceFollowupQueueItem] = []
    identifiers = obj.identifiers or {}
    identifier_targets = [
        ("pmcid", "pmc_oa", "pmcid_full_text"),
        ("pmid", "pubmed", "pubmed_metadata_fallback"),
        ("doi", "unpaywall", "doi_open_access_fallback"),
        ("doi", "crossref", "doi_metadata_fallback"),
    ]
    for identifier_type, source_key, fallback_type in identifier_targets:
        identifier = identifiers.get(identifier_type) or identifiers.get(identifier_type.upper())
        if not identifier:
            continue
        items.append(
            SourceFollowupQueueItem(
                source_key=source_key,
                identifier_type=identifier_type,  # type: ignore[arg-type]
                identifier=str(identifier),
                url=_source_followup_url(identifier_type, str(identifier), fallback=obj.canonical_url),
                title=obj.title,
                origin_source_key=obj.source_key,
                origin_agent_run_id=lead.origin_agent_run_id,
                reason=f"Research follow-up loop identifier fallback: {fallback_type}.",
                priority=20 if identifier_type == "pmcid" else 30 if identifier_type == "pmid" else 40,
                metadata={
                    "research_followup_loop": True,
                    "fallback_type": fallback_type,
                    "lead_id": str(lead.lead_id),
                    "origin_review_id": str(lead.origin_review_id) if lead.origin_review_id else None,
                    "origin_research_object_id": str(obj.id),
                    "origin_dedupe_key": obj.dedupe_key,
                    "operator": request.operator,
                    "followup_lane": request.followup_lane,
                },
            )
        )
    return items


def _route_evidence_light_research_lead(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
) -> dict[str, Any] | None:
    followup_reason = _research_lead_followup_reason(repository, lead)
    if followup_reason is None:
        return None

    source_followup = _source_followup_from_research_lead(lead)
    persisted_followup = repository.upsert_source_followup(source_followup) if source_followup else None
    metadata = {
        "research_followup_queue": {
            "reason": followup_reason,
            "brief_source_key": _research_brief_source_key_from_lead(lead),
            "source_followup_id": str(persisted_followup.followup_id) if persisted_followup else None,
            "source_followup_source_key": persisted_followup.source_key if persisted_followup else None,
            "source_followup_identifier_type": persisted_followup.identifier_type if persisted_followup else None,
            "source_followup_identifier": persisted_followup.identifier if persisted_followup else None,
            "requires_manual_research": persisted_followup is None,
        }
    }
    repository.update_research_lead(lead.lead_id, status="followup", metadata=metadata)
    return {
        "origin": "research_lead",
        "lead_id": str(lead.lead_id),
        "source_key": lead.source_key,
        "origin_source_key": lead.origin_source_key,
        "reason": "lead_needs_research_followup",
        "followup_reason": followup_reason,
        "source_followup_id": str(persisted_followup.followup_id) if persisted_followup else None,
        "source_followup_source_key": persisted_followup.source_key if persisted_followup else None,
        "source_followup_identifier_type": persisted_followup.identifier_type if persisted_followup else None,
        "requires_manual_research": persisted_followup is None,
    }


def _research_lead_followup_reason(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
) -> str | None:
    if lead.suggested_sources:
        return None
    source_key = _research_brief_source_key_from_lead(lead)
    if _source_followup_from_research_lead(lead) is not None:
        return "durable identifier must be ingested before synthesis"
    if source_key in _NON_EVIDENCE_RESEARCH_LEAD_SOURCES:
        return "social/linked-article lead has no durable source attached"
    if not source_key:
        return "lead has no synthesis source attached"
    if not repository.list_document_chunks(source_key=source_key, limit=1):
        return f"source {source_key} has no document chunks available for synthesis"
    return None


def _source_followup_from_research_lead(lead: ResearchLeadRecord) -> SourceFollowupQueueItem | None:
    for identifier_type, source_key in _FOLLOWUP_SOURCE_BY_IDENTIFIER_TYPE.items():
        identifier = lead.identifiers.get(identifier_type)
        if not identifier:
            continue
        return SourceFollowupQueueItem(
            source_key=source_key,
            identifier_type=identifier_type,  # type: ignore[arg-type]
            identifier=identifier,
            url=_source_followup_url(identifier_type, identifier, fallback=lead.url),
            title=lead.title,
            origin_source_key=lead.origin_source_key or lead.source_key,
            origin_review_id=lead.origin_review_id,
            origin_artifact_id=lead.origin_artifact_id,
            origin_agent_run_id=lead.origin_agent_run_id,
            reason="Research lead needs durable evidence ingestion before synthesis.",
            priority=lead.priority,
            metadata={
                "followup_type": "research_lead_evidence_enrichment",
                "research_lead_id": str(lead.lead_id),
                "research_lead_type": lead.lead_type,
                "research_lead_status": lead.status,
                "research_lead_reason": lead.reason,
                "research_lead_url": lead.url,
                "topic_tags": lead.topic_tags,
            },
        )
    return None


def _source_followup_url(identifier_type: str, identifier: str, *, fallback: str | None) -> str | None:
    if identifier_type == "doi":
        return f"https://doi.org/{identifier}"
    if identifier_type == "pmid":
        return f"https://pubmed.ncbi.nlm.nih.gov/{identifier}/"
    if identifier_type == "pmcid":
        return f"https://www.ncbi.nlm.nih.gov/pmc/articles/{identifier}/"
    if identifier_type == "nct":
        return f"https://clinicaltrials.gov/study/{identifier}"
    return fallback


def _research_brief_source_key_from_lead(lead: ResearchLeadRecord) -> str | None:
    if lead.suggested_sources:
        return lead.suggested_sources[0]
    return lead.origin_source_key or lead.source_key


def _priority_for_source_health(health_status: str) -> int:
    return {
        "failing": 30,
        "triage": 45,
        "watch": 65,
        "healthy": 90,
    }.get(health_status, 80)


def _command_center_queue_item(item: ResearchBriefQueueItem) -> dict[str, Any]:
    return {
        "queue_item_id": str(item.queue_item_id),
        "status": item.status,
        "priority": item.priority,
        "topic": item.topic,
        "source_key": item.source_key,
        "brief_style": item.brief_style,
        "review_mode": item.review_mode,
        "attempts": item.attempts,
        "last_brief_id": str(item.last_brief_id) if item.last_brief_id else None,
        "last_agent_run_id": str(item.last_agent_run_id) if item.last_agent_run_id else None,
        "last_error": item.last_error,
        "created_at": item.created_at.isoformat(),
        "updated_at": item.updated_at.isoformat(),
        "origin": (item.metadata.get("batch_queue") or {}).get("origin"),
    }


def _command_center_lead(lead: ResearchLeadRecord) -> dict[str, Any]:
    return {
        "lead_id": str(lead.lead_id),
        "status": lead.status,
        "priority": lead.priority,
        "lead_type": lead.lead_type,
        "title": lead.title,
        "url": lead.url,
        "source_key": lead.source_key,
        "origin_source_key": lead.origin_source_key,
        "reason": lead.reason,
        "topic_tags": lead.topic_tags,
        "suggested_sources": lead.suggested_sources,
        "created_at": lead.created_at.isoformat(),
        "updated_at": lead.updated_at.isoformat(),
        "queue_item_id": (lead.metadata.get("research_brief_queue") or {}).get("queue_item_id"),
    }


def _command_center_agent_run(run: AgentRunRecord) -> dict[str, Any]:
    return {
        "agent_run_id": str(run.agent_run_id),
        "agent_name": run.agent_name,
        "agent_version": run.agent_version,
        "model_profile": run.model_profile,
        "status": str(run.status),
        "source_key": run.source_key,
        "partition_date": run.partition_date,
        "dagster_run_id": run.dagster_run_id,
        "started_at": run.started_at.isoformat(),
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "error_count": len(run.errors),
        "summary": run.summary,
    }


def _extend_command_center_recommendations(
    recommendations: list[CommandCenterRecommendation],
    *,
    queue_status_counts: Counter[str],
    lead_status_counts: Counter[str],
    source_health_report: dict[str, Any] | None,
    agent_status_counts: Counter[str],
) -> None:
    if queue_status_counts.get("failed", 0) > 0:
        recommendations.append(
            CommandCenterRecommendation(
                area="brief_queue",
                severity="watch",
                action="Inspect or requeue failed research brief queue items.",
                reason=f"{queue_status_counts['failed']} queued brief item(s) are failed.",
                job_name="research_brief_queue_job",
                metadata={"failed_count": queue_status_counts["failed"]},
            )
        )
    if queue_status_counts.get("queued", 0) > 0:
        recommendations.append(
            CommandCenterRecommendation(
                area="brief_queue",
                severity="info",
                action="Run the queued research brief worker.",
                reason=f"{queue_status_counts['queued']} brief item(s) are ready to run.",
                job_name="research_brief_queue_runner_job",
                metadata={"queued_count": queue_status_counts["queued"]},
            )
        )
    actionable_leads = lead_status_counts.get("new", 0) + lead_status_counts.get("watching", 0)
    if actionable_leads > 0:
        recommendations.append(
            CommandCenterRecommendation(
                area="research_leads",
                severity="info",
                action="Batch queue actionable research leads for synthesis.",
                reason=f"{actionable_leads} lead(s) are new or watching.",
                job_name="research_brief_queue_batch_job",
                metadata={"actionable_leads": actionable_leads, "mode": "research_leads"},
            )
        )
    followup_leads = lead_status_counts.get("followup", 0)
    if followup_leads > 0:
        recommendations.append(
            CommandCenterRecommendation(
                area="research_leads",
                severity="watch",
                action="Resolve research follow-up leads before synthesis.",
                reason=f"{followup_leads} lead(s) need durable evidence before briefing.",
                job_name="research_followup_resolver_job",
                metadata={"followup_leads": followup_leads},
            )
        )
    if source_health_report:
        failed_sources = source_health_report.get("failed_sources", [])
        triage_sources = source_health_report.get("triage_sources", [])
        watch_sources = source_health_report.get("watch_sources", [])
        embedding_missing_sources = source_health_report.get("embedding_missing_sources", [])
        full_text_blocking_sources = source_health_report.get("full_text_blocking_sources", [])
        if failed_sources:
            recommendations.append(
                CommandCenterRecommendation(
                    area="source_health",
                    severity="blocking",
                    action="Inspect failed source health before expanding synthesis volume.",
                    reason=f"{len(failed_sources)} source(s) are below the minimum health bar.",
                    job_name="source_health_report_job",
                    metadata={"failed_sources": failed_sources},
                )
            )
        if triage_sources or watch_sources:
            recommendations.append(
                CommandCenterRecommendation(
                    area="source_health",
                    severity="watch",
                    action="Queue source-health gap reviews for synthesis triage.",
                    reason=f"{len(triage_sources)} triage and {len(watch_sources)} watch source(s) need review.",
                    job_name="research_brief_queue_batch_job",
                    metadata={"triage_sources": triage_sources, "watch_sources": watch_sources, "mode": "source_health"},
                )
            )
        if embedding_missing_sources:
            recommendations.append(
                CommandCenterRecommendation(
                    area="embeddings",
                    severity="watch",
                    action="Refresh and maintain the embedding index.",
                    reason=f"{len(embedding_missing_sources)} source(s) have missing embeddings.",
                    job_name="embedding_maintenance_job",
                    metadata={"embedding_missing_sources": embedding_missing_sources},
                )
            )
        if full_text_blocking_sources:
            recommendations.append(
                CommandCenterRecommendation(
                    area="full_text",
                    severity="blocking",
                    action="Run full-text ops before enabling aggressive full-text schedules.",
                    reason=f"{len(full_text_blocking_sources)} full-text source(s) are blocking.",
                    job_name="full_text_ops_agent_job",
                    metadata={"full_text_blocking_sources": full_text_blocking_sources},
                )
            )
    if agent_status_counts.get("failed", 0) > 0:
        recommendations.append(
            CommandCenterRecommendation(
                area="agents",
                severity="watch",
                action="Inspect recent failed agent runs.",
                reason=f"{agent_status_counts['failed']} recent agent run(s) failed.",
                job_name="agent_runs",
                metadata={"failed_agent_runs": agent_status_counts["failed"]},
            )
        )


def _hybrid_chunk_score(embedding_score: float | None, keyword_score: float | None) -> float:
    normalized_embedding = 0.0
    if embedding_score is not None:
        normalized_embedding = max(0.0, min(1.0, (float(embedding_score) + 1.0) / 2.0))
    normalized_keyword = max(0.0, min(1.0, float(keyword_score or 0.0)))
    if keyword_score is None:
        return round(normalized_embedding, 6)
    if embedding_score is None:
        return round(normalized_keyword, 6)
    return round(min(1.0, normalized_embedding * 0.45 + normalized_keyword * 0.55), 6)


def _brief_request_from_queue_item(
    item: ResearchBriefQueueItem,
    request: ResearchBriefQueueRunRequest,
) -> ResearchBriefRequest:
    return ResearchBriefRequest(
        topic=item.topic,
        disease_scope=item.disease_scope,
        source_key=item.source_key,
        max_chunks_per_perspective=item.max_chunks_per_perspective,
        max_claims=item.max_claims,
        max_chunk_chars=item.max_chunk_chars,
        brief_style=item.brief_style,
        model_profile=item.model_profile,
        review_mode=item.review_mode,
        review_models=item.review_models,
        dagster_run_id=request.dagster_run_id,
        metadata={
            **item.metadata,
            "research_brief_queue_item_id": str(item.queue_item_id),
        },
    )


_SERVICE: HSAResearchService | None = None


def build_default_repository() -> ResearchRepository:
    """Build the default local-first repository.

    Set ``HSA_STORAGE_BACKEND=memory`` for ephemeral smoke tests,
    ``HSA_STORAGE_BACKEND=postgres`` plus ``HSA_DATABASE_URL`` for hosted
    Dagster+ execution, or use the default SQLite local repository.
    """

    return build_research_repository()


def get_service() -> HSAResearchService:
    """Return the process-local service singleton."""

    global _SERVICE
    if _SERVICE is None:
        _SERVICE = HSAResearchService()
    return _SERVICE


def reset_service_for_tests() -> None:
    """Reset the process-local service singleton."""

    global _SERVICE
    _SERVICE = None
