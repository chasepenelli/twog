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
import json
import os
import re
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
    ArtifactHandle,
    AsyncRunHandle,
    BoltzRunRequest,
    CandidateDossier,
    CandidateDossierRequest,
    ChunkContextRequest,
    ChunkContextResult,
    ClaimCurationRequest,
    ClaimCurationResult,
    ClaimSearchResult,
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
    ResearchBriefFinding,
    ResearchBriefOperatorDocRequest,
    ResearchBriefOperatorDocResult,
    ResearchBriefOperatorDocument,
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
    ResearchHuntSynthesisQueueRequest,
    ResearchHuntSynthesisQueueResult,
    ResearchHuntSynthesisDocument,
    ResearchHuntSynthesisDocRequest,
    ResearchHuntSynthesisDocResult,
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
    ResearchHuntTaskRunRequest,
    ResearchHuntTaskRunResult,
    ResearchHuntQueueReportRequest,
    ResearchHuntQueueReportResult,
    ResearchHuntLeadQueueRow,
    ResearchHuntTaskQueueRow,
    ResearchHuntQueueMaintenanceRequest,
    ResearchHuntQueueMaintenanceResult,
    ResearchHuntQueueMaintenanceItem,
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
    HypothesisPromotionCandidate,
    HypothesisPromotionReportRequest,
    HypothesisPromotionReportResult,
    TherapyCommitteeRequest,
    TherapyCommitteeResult,
    TherapyCommitteeValidationQueueRequest,
    TherapyCommitteeValidationQueueResult,
    TherapyIdea,
    TherapyIdeaLibraryRequest,
    TherapyIdeaLibraryResult,
    TherapyIdeaRecord,
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
    ValidationToolCatalogRequest,
    ValidationToolCatalogResult,
    ValidationToolMatchRequest,
    ValidationToolMatchResult,
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
from .validation_tool_catalog import build_validation_tool_catalog_report, match_validation_tools
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
        model_name="openrouter:~anthropic/claude-opus-latest",
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
    if request.validation_type in {"boltz", "docking", "md"}:
        blockers.append("live_compute_runner_not_enabled")
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


def _catalog_dispatch_blockers_for_task(task: ValidationPlanTask) -> list[str]:
    catalog = (task.metadata or {}).get("validation_tool_catalog")
    if not isinstance(catalog, dict):
        catalog = ((task.validation_request.metadata or {}).get("validation_tool_catalog") if task.validation_request else {})
    if not isinstance(catalog, dict):
        return []
    blockers = [str(item) for item in catalog.get("dispatch_blockers", []) if str(item).strip()]
    return _dedupe_quality_labels(blockers)


def _validation_queue_dispatch_blockers_for_task(task: ValidationPlanTask) -> list[str]:
    blockers = [*_catalog_dispatch_blockers_for_task(task)]
    if task.requires_human_approval or (task.validation_request and task.validation_request.require_approval):
        blockers.append("human_approval_required")
    request_metadata = task.validation_request.metadata if task.validation_request else {}
    catalog = (task.metadata or {}).get("validation_tool_catalog")
    if not isinstance(catalog, dict):
        catalog = request_metadata.get("validation_tool_catalog")
    if not isinstance(catalog, dict):
        catalog = {}
    if (
        (task.metadata or {}).get("recommend_only") is True
        or request_metadata.get("recommend_only") is True
        or catalog.get("recommend_only") is True
        or catalog.get("runner_status") == "recommend_only"
    ):
        blockers.append("recommend_only_runner")
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
        "signal_status": result.signal_status,
        "coverage_status": result.coverage_status,
        "hunt_tasks_created": result.hunt_tasks_created,
        "hunt_tasks_suppressed": result.hunt_tasks_suppressed,
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


def _summarize_research_hunt_task_run(result: ResearchHuntTaskRunResult) -> dict[str, Any]:
    return {
        "dry_run": result.dry_run,
        "scanned_count": result.scanned_count,
        "selected_count": result.selected_count,
        "completed_count": result.completed_count,
        "failed_count": result.failed_count,
        "skipped_count": result.skipped_count,
        "claim_chunks_seen": result.claim_chunks_seen,
        "claims_written": result.claims_written,
        "loop_runs": result.loop_runs,
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
        result = AgentRunner(self.repository).run(
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
        for idea in result.ranked_ideas:
            self.repository.upsert_therapy_idea(
                _therapy_idea_record_from_result(idea, result, request)
            )
        return result

    def list_validation_tool_catalog(
        self,
        request: ValidationToolCatalogRequest | None = None,
    ) -> ValidationToolCatalogResult:
        return build_validation_tool_catalog_report(request or ValidationToolCatalogRequest())

    def match_validation_tools(self, request: ValidationToolMatchRequest) -> ValidationToolMatchResult:
        return match_validation_tools(request)

    def get_therapy_idea(self, therapy_idea_id: UUID) -> TherapyIdeaRecord | None:
        return self.repository.get_therapy_idea(therapy_idea_id)

    def list_therapy_ideas(
        self,
        request: TherapyIdeaLibraryRequest | None = None,
    ) -> TherapyIdeaLibraryResult:
        request = request or TherapyIdeaLibraryRequest()
        if request.therapy_idea_id:
            record = self.repository.get_therapy_idea(request.therapy_idea_id)
            records = [record] if record else []
        else:
            records = self.repository.list_therapy_ideas(
                status=request.status,
                statuses=list(request.statuses) if request.statuses else None,
                source_brief_id=request.source_brief_id,
                source_evaluation_id=request.source_evaluation_id,
                committee_run_id=request.committee_run_id,
                topic_query=request.topic_query,
                limit=request.limit,
            )
        return TherapyIdeaLibraryResult(
            idea_count=len(records),
            status_counts=dict(sorted(Counter(record.status for record in records).items())),
            ideas=records,
        )

    def build_hypothesis_promotion_report(
        self,
        request: HypothesisPromotionReportRequest | None = None,
    ) -> HypothesisPromotionReportResult:
        return _build_hypothesis_promotion_report(
            self.repository,
            request or HypothesisPromotionReportRequest(),
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

    def create_research_brief_operator_docs(
        self,
        request: ResearchBriefOperatorDocRequest,
    ) -> ResearchBriefOperatorDocResult:
        candidates: list[ResearchBriefRecord] = []
        errors: list[str] = []
        skipped: list[dict[str, Any]] = []
        if request.brief_ids:
            for brief_id in request.brief_ids:
                brief = self.repository.get_research_brief(brief_id)
                if brief is None:
                    errors.append(f"brief {brief_id}: not found")
                    continue
                candidates.append(brief)
        else:
            candidates = self.repository.list_research_briefs(
                status=request.status,
                source_key=request.source_key,
                topic_query=request.topic_query,
                limit=request.limit,
            )

        documents: list[ResearchBriefOperatorDocument] = []
        artifacts: list[ArtifactHandle] = []
        for brief in candidates[: request.limit]:
            if brief.status != "completed":
                skipped.append(
                    {
                        "brief_id": str(brief.brief_id),
                        "status": brief.status,
                        "reason": "brief_not_completed",
                    }
                )
                continue
            try:
                document, artifact = _research_brief_operator_doc_from_brief(
                    self.repository,
                    brief,
                    request,
                )
                documents.append(document)
                if artifact is not None:
                    artifacts.append(artifact)
            except Exception as exc:
                errors.append(f"brief {brief.brief_id}: {exc}")

        return ResearchBriefOperatorDocResult(
            dry_run=request.dry_run,
            candidate_count=len(candidates),
            document_count=len(documents),
            artifact_count=len(artifacts),
            skipped_count=len(skipped),
            documents=documents,
            artifacts=artifacts,
            skipped=skipped,
            errors=errors,
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

        brief_candidates: list[tuple[ResearchBriefRecord, ResearchBriefEvaluationRecord | None]] = []
        if request.evaluation_ids:
            for evaluation_id in request.evaluation_ids:
                evaluation = self.repository.get_research_brief_evaluation(evaluation_id)
                if evaluation is None:
                    errors.append(f"evaluation {evaluation_id}: not found")
                    continue
                brief = self.repository.get_research_brief(evaluation.brief_id)
                if brief is None:
                    errors.append(f"brief {evaluation.brief_id}: not found for evaluation {evaluation_id}")
                    continue
                if not _research_brief_matches_followup_filters(brief, request):
                    skipped.append(
                        {
                            "brief_id": str(brief.brief_id),
                            "evaluation_id": str(evaluation_id),
                            "reason": "brief_filtered_out",
                        }
                    )
                    continue
                brief_candidates.append((brief, evaluation))
        elif request.brief_ids:
            for brief_id in request.brief_ids:
                brief = self.repository.get_research_brief(brief_id)
                if brief is None:
                    errors.append(f"brief {brief_id}: not found")
                    continue
                if not _research_brief_matches_followup_filters(brief, request):
                    skipped.append({"brief_id": str(brief_id), "reason": "brief_filtered_out"})
                    continue
                latest_evaluation = (
                    _latest_research_brief_evaluation(self.repository, brief)
                    if request.include_evaluations
                    else None
                )
                brief_candidates.append((brief, latest_evaluation))
        else:
            briefs = self.repository.list_research_briefs(
                status=request.status,
                source_key=request.source_key,
                topic_query=request.topic_query,
                limit=request.limit,
            )
            for brief in briefs:
                latest_evaluation = (
                    _latest_research_brief_evaluation(self.repository, brief)
                    if request.include_evaluations
                    else None
                )
                brief_candidates.append((brief, latest_evaluation))

        for brief, latest_evaluation in brief_candidates[: request.limit]:
            try:
                row = _research_brief_quality_row(brief, latest_evaluation)
                if not request.force and row.quality_status not in _RESEARCH_BRIEF_FOLLOWUP_QUALITY_STATUSES:
                    skipped.append(
                        {
                            "brief_id": str(brief.brief_id),
                            "evaluation_id": str(latest_evaluation.evaluation_id) if latest_evaluation else None,
                            "quality_status": row.quality_status,
                            "reason": "brief_does_not_need_followup_research",
                        }
                    )
                    continue

                candidate_brief_count += 1
                leads = _research_brief_followup_leads_from_brief(
                    brief,
                    row,
                    evaluation=latest_evaluation,
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
        existing_by_identity = {item.identity_key: item for item in existing_items}
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
                dispatch_blockers=_validation_queue_dispatch_blockers_for_task(task),
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
            if queue_item.identity_key in existing_by_identity:
                existing_count += 1
                if request.dry_run:
                    queue_items.append(existing_by_identity[queue_item.identity_key])
                else:
                    queue_items.append(self.repository.upsert_validation_request_queue_item(queue_item))
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

    def queue_ready_research_hunt_synthesis(
        self,
        request: ResearchHuntSynthesisQueueRequest,
    ) -> ResearchHuntSynthesisQueueResult:
        report = self.build_research_hunt_queue_report(
            ResearchHuntQueueReportRequest(
                lead_ids=request.lead_ids,
                lead_statuses=request.lead_statuses,
                source_keys=request.source_keys,
                limit=1000 if not request.lead_ids else max(len(request.lead_ids), 1),
                task_limit=2000,
                include_tasks=True,
                include_suppressed=True,
            )
        )
        existing_by_identity_key = {
            item.identity_key: item
            for item in self.repository.list_research_brief_queue_items(limit=None)
            if item.identity_key
        }
        queue_items: list[ResearchBriefQueueItem] = []
        skipped: list[dict[str, Any]] = []
        errors: list[str] = []
        candidate_count = 0
        queued_count = 0
        preexisting_count = 0
        updated_lead_count = 0
        handoff_documents: list[ResearchHuntSynthesisDocument] = []
        handoff_artifacts: list[ArtifactHandle] = []

        for lead_row in report.leads:
            if candidate_count >= request.limit:
                break
            if lead_row.control_status != "ready_for_synthesis":
                skipped.append(
                    {
                        "lead_id": str(lead_row.lead_id),
                        "title": lead_row.title,
                        "status": lead_row.status,
                        "source_key": lead_row.source_key,
                        "control_status": lead_row.control_status,
                        "recommended_action": lead_row.recommended_action,
                        "reason": "lead_not_ready_for_synthesis",
                    }
                )
                continue

            candidate_count += 1
            lead = self.repository.get_research_lead(lead_row.lead_id)
            if lead is None:
                errors.append(f"lead {lead_row.lead_id}: not found")
                continue

            if request.create_handoff_docs:
                try:
                    doc, artifact = _research_hunt_synthesis_doc_from_lead(
                        self.repository,
                        lead,
                        lead_row,
                        ResearchHuntSynthesisDocRequest(
                            lead_ids=[lead.lead_id],
                            lead_statuses=request.lead_statuses,
                            source_keys=request.source_keys,
                            limit=1,
                            max_claims=request.max_claims,
                            dry_run=request.dry_run,
                            operator=request.operator,
                            dagster_run_id=request.dagster_run_id,
                            metadata=request.metadata,
                        ),
                    )
                    handoff_documents.append(doc)
                    if artifact is not None:
                        handoff_artifacts.append(artifact)
                except Exception as exc:  # pragma: no cover - defensive service boundary
                    errors.append(f"lead {lead.lead_id} handoff doc: {exc}")

            queue_request = _research_hunt_synthesis_queue_request_from_lead(lead, lead_row, request)
            preview_item = ResearchBriefQueueItem(
                topic=queue_request.topic,
                disease_scope=queue_request.disease_scope,
                source_key=queue_request.source_key,
                priority=queue_request.priority,
                max_chunks_per_perspective=queue_request.max_chunks_per_perspective,
                max_claims=queue_request.max_claims,
                max_chunk_chars=queue_request.max_chunk_chars,
                brief_style=queue_request.brief_style,
                model_profile=queue_request.model_profile,
                review_mode=queue_request.review_mode,
                review_models=queue_request.review_models,
                metadata=queue_request.metadata,
            )
            existing = existing_by_identity_key.get(preview_item.identity_key or "")
            if existing is not None:
                preexisting_count += 1
                queue_items.append(existing)
                if not request.dry_run and request.transition_leads:
                    updated = self.repository.update_research_lead(
                        lead.lead_id,
                        status="queued" if existing.status in {"queued", "running"} else None,
                        metadata={
                            "research_hunt_synthesis_queue": {
                                "queue_item_id": str(existing.queue_item_id),
                                "topic": existing.topic,
                                "preexisting": True,
                                "queue_status": existing.status,
                                "operator": request.operator,
                                "dagster_run_id": request.dagster_run_id,
                                "queued_at": datetime.now(UTC).isoformat(),
                            }
                        },
                    )
                    if updated is not None:
                        updated_lead_count += 1
                continue

            if request.dry_run:
                queue_items.append(preview_item)
                continue

            try:
                queue_item = self.queue_research_brief(queue_request)
                queued_count += 1
                queue_items.append(queue_item)
                if queue_item.identity_key:
                    existing_by_identity_key[queue_item.identity_key] = queue_item
                if request.transition_leads:
                    updated = self.repository.update_research_lead(
                        lead.lead_id,
                        status="queued",
                        metadata={
                            "research_hunt_synthesis_queue": {
                                "queue_item_id": str(queue_item.queue_item_id),
                                "topic": queue_item.topic,
                                "preexisting": False,
                                "queue_status": queue_item.status,
                                "operator": request.operator,
                                "dagster_run_id": request.dagster_run_id,
                                "queued_at": datetime.now(UTC).isoformat(),
                            }
                        },
                    )
                    if updated is not None:
                        updated_lead_count += 1
            except Exception as exc:  # pragma: no cover - defensive service boundary
                errors.append(f"lead {lead.lead_id}: {exc}")

        return ResearchHuntSynthesisQueueResult(
            dry_run=request.dry_run,
            candidate_count=candidate_count,
            queued_count=queued_count,
            preexisting_count=preexisting_count,
            updated_lead_count=updated_lead_count,
            handoff_document_count=len(handoff_documents),
            handoff_artifact_count=len(handoff_artifacts),
            skipped_count=len(skipped),
            queue_items=queue_items,
            handoff_documents=handoff_documents,
            handoff_artifacts=handoff_artifacts,
            skipped=skipped,
            errors=errors,
        )

    def create_ready_research_hunt_synthesis_docs(
        self,
        request: ResearchHuntSynthesisDocRequest,
    ) -> ResearchHuntSynthesisDocResult:
        report = self.build_research_hunt_queue_report(
            ResearchHuntQueueReportRequest(
                lead_ids=request.lead_ids,
                lead_statuses=request.lead_statuses,
                source_keys=request.source_keys,
                limit=1000 if not request.lead_ids else max(len(request.lead_ids), 1),
                task_limit=2000,
                include_tasks=True,
                include_suppressed=True,
            )
        )
        documents: list[ResearchHuntSynthesisDocument] = []
        artifacts: list[ArtifactHandle] = []
        skipped: list[dict[str, Any]] = []
        errors: list[str] = []
        candidate_count = 0
        updated_lead_count = 0

        for lead_row in report.leads:
            if candidate_count >= request.limit:
                break
            if lead_row.control_status != "ready_for_synthesis":
                skipped.append(
                    {
                        "lead_id": str(lead_row.lead_id),
                        "title": lead_row.title,
                        "status": lead_row.status,
                        "source_key": lead_row.source_key,
                        "control_status": lead_row.control_status,
                        "recommended_action": lead_row.recommended_action,
                        "reason": "lead_not_ready_for_synthesis",
                    }
                )
                continue

            candidate_count += 1
            lead = self.repository.get_research_lead(lead_row.lead_id)
            if lead is None:
                errors.append(f"lead {lead_row.lead_id}: not found")
                continue
            try:
                doc, artifact = _research_hunt_synthesis_doc_from_lead(self.repository, lead, lead_row, request)
                documents.append(doc)
                if artifact is not None:
                    artifacts.append(artifact)
                    updated_lead_count += 1
            except Exception as exc:  # pragma: no cover - defensive service boundary
                errors.append(f"lead {lead.lead_id}: {exc}")

        return ResearchHuntSynthesisDocResult(
            dry_run=request.dry_run,
            candidate_count=candidate_count,
            document_count=len(documents),
            artifact_count=len(artifacts),
            updated_lead_count=updated_lead_count,
            skipped_count=len(skipped),
            documents=documents,
            artifacts=artifacts,
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

    def run_research_hunt_tasks(
        self,
        request: ResearchHuntTaskRunRequest,
    ) -> ResearchHuntTaskRunResult:
        return AgentRunner(self.repository).run(
            agent_name="research_hunt_task_executor_agent",
            agent_version="v1",
            model_profile=request.model_profile,
            input_payload=request.model_dump(mode="json"),
            dagster_run_id=request.dagster_run_id,
            metadata=request.metadata,
            execute=lambda: self._execute_research_hunt_tasks(request),
            summarize=_summarize_research_hunt_task_run,
        )

    def _execute_research_hunt_tasks(
        self,
        request: ResearchHuntTaskRunRequest,
    ) -> ResearchHuntTaskRunResult:
        result = ResearchHuntTaskRunResult(dry_run=request.dry_run)
        selected = _select_research_hunt_tasks(self.repository, request)
        result.scanned_count = len(selected)
        selected = selected[: request.limit]
        result.selected_count = len(selected)
        extraction_cache: dict[tuple[str, ...], tuple[str, Any]] = {}
        for lead, task in selected:
            item = {
                "lead_id": str(lead.lead_id),
                "task_id": str(task.get("task_id") or ""),
                "task_type": str(task.get("task_type") or "research_followup"),
                "action": str(task.get("action") or ""),
                "status_before": str(task.get("status") or ""),
            }
            if request.dry_run:
                item["status_after"] = item["status_before"]
                item["planned"] = True
                result.skipped_count += 1
                result.items.append(item)
                continue
            try:
                task_type = item["task_type"]
                if task_type == "source_followup_ingest":
                    source_followup_result = self._execute_research_hunt_source_followup_task(lead, task, request)
                    item["source_followup"] = source_followup_result
                    status_after = "completed" if source_followup_result["candidate_count"] and not source_followup_result["errors"] else "failed"
                    item["status_after"] = status_after
                    if status_after == "completed":
                        result.completed_count += 1
                    else:
                        result.failed_count += 1
                        item["errors"] = source_followup_result["errors"] or [
                            "No source follow-up candidates were available for ingestion."
                        ]
                elif task_type in {"claim_extract", "safety_extract", "full_text_extract"}:
                    chunks = _research_hunt_task_evidence_chunks(
                        self.repository,
                        lead,
                        task,
                        limit=request.claim_chunk_limit,
                    )
                    chunk_key = tuple(str(chunk.id) for chunk in chunks)
                    cached = extraction_cache.get(chunk_key) if chunk_key else None
                    if cached is not None:
                        reused_from_task_id, claim_result = cached
                        item["reused_claim_extraction_from_task_id"] = reused_from_task_id
                        item["claim_extraction"] = claim_result.model_dump(mode="json")
                        item["claim_chunks_seen"] = 0
                        item["claims_written"] = 0
                        status_after = "completed" if claim_result.chunks_seen and not claim_result.errors else "failed"
                        item["status_after"] = status_after
                        if status_after == "completed":
                            result.completed_count += 1
                        else:
                            result.failed_count += 1
                            item["errors"] = claim_result.errors or ["No evidence chunks were available for claim extraction."]
                        self._mark_research_hunt_task(
                            lead.lead_id,
                            item["task_id"],
                            status=str(item["status_after"]),
                            execution=item,
                        )
                        result.items.append(item)
                        continue
                    claim_result = extract_claims_for_chunks(self.repository, chunks)
                    if chunk_key:
                        extraction_cache[chunk_key] = (item["task_id"], claim_result)
                    item["claim_extraction"] = claim_result.model_dump(mode="json")
                    item["claim_chunks_seen"] = claim_result.chunks_seen
                    item["claims_written"] = claim_result.claims_written
                    result.claim_chunks_seen += claim_result.chunks_seen
                    result.claims_written += claim_result.claims_written
                    status_after = "completed" if claim_result.chunks_seen and not claim_result.errors else "failed"
                    item["status_after"] = status_after
                    if status_after == "completed":
                        result.completed_count += 1
                    else:
                        result.failed_count += 1
                        item["errors"] = claim_result.errors or ["No evidence chunks were available for claim extraction."]
                else:
                    task_source_keys = _research_hunt_task_source_keys(lead, task, request)
                    loop_result = self.run_research_followup_loop(
                        ResearchFollowupLoopRequest(
                            lead_id=lead.lead_id,
                            source_keys=task_source_keys,
                            search_query_text=_research_hunt_task_search_query(lead, task),
                            ingest=False,
                            resolve=True,
                            evaluate=request.evaluate,
                            dry_run=False,
                            force_live_search=request.force_live_search,
                            search_limit_per_source=request.search_limit_per_source,
                            model_profile=request.model_profile,
                            review_models=request.review_models,
                            operator=request.operator,
                            metadata={
                                **request.metadata,
                                "research_hunt_task_executor": True,
                                "research_hunt_task_id": item["task_id"],
                                "research_hunt_parent_task_type": task_type,
                                "research_hunt_allow_broad_task_fanout": request.allow_broad_task_fanout,
                            },
                        )
                    )
                    item["loop_result"] = _summarize_research_followup_loop(loop_result)
                    item["loop_agent_run_id"] = str(loop_result.agent_run_id) if loop_result.agent_run_id else None
                    result.loop_runs += 1
                    status_after = "completed" if not loop_result.errors else "failed"
                    item["status_after"] = status_after
                    if status_after == "completed":
                        result.completed_count += 1
                    else:
                        result.failed_count += 1
                        item["errors"] = loop_result.errors
                self._mark_research_hunt_task(
                    lead.lead_id,
                    item["task_id"],
                    status=str(item["status_after"]),
                    execution=item,
                )
            except Exception as exc:
                item["status_after"] = "failed"
                item["errors"] = [str(exc)]
                result.failed_count += 1
                result.errors.append(f"{item['task_id']}: {exc}")
                self._mark_research_hunt_task(
                    lead.lead_id,
                    item["task_id"],
                    status="failed",
                    execution=item,
                )
            result.items.append(item)
        return result

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
                self._update_research_hunt_state(
                    result,
                    request,
                    verdict=None,
                    followup_actions=[],
                )
            if not request.dry_run:
                next_status = (
                    "watching"
                    if evidence_fit.fit == "strong" or _research_hunt_has_supported_signal(result)
                    else "followup"
                )
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
                        "signal_status": result.signal_status,
                        "coverage_status": result.coverage_status,
                        "hunt_tasks_created": result.hunt_tasks_created,
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
                        search_query_text=request.search_query_text,
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
                        followup_actions = _evaluation_followup_actions(evaluation_result)
                        hunt_state = self._update_research_hunt_state(
                            result,
                            request,
                            verdict=verdict,
                            followup_actions=followup_actions,
                        )
                        has_supported_signal = _research_hunt_state_has_supported_signal(hunt_state)
                        coverage_status = str(hunt_state.get("coverage_status") or "")
                        if verdict == "useful":
                            next_status = "watching" if coverage_status == "hunting" else "ingested"
                        elif verdict == "unclear":
                            next_status = "watching"
                        elif verdict == "needs_followup" and has_supported_signal:
                            next_status = "watching"
                        else:
                            next_status = "followup"
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
                                "signal_status": result.signal_status,
                                "coverage_status": result.coverage_status,
                                "hunt_tasks_created": result.hunt_tasks_created,
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

    def _execute_research_hunt_claim_task(
        self,
        lead: ResearchLeadRecord,
        task: dict[str, Any],
        request: ResearchHuntTaskRunRequest,
    ):
        chunks = _research_hunt_task_evidence_chunks(self.repository, lead, task, limit=request.claim_chunk_limit)
        return extract_claims_for_chunks(self.repository, chunks)

    def _execute_research_hunt_source_followup_task(
        self,
        lead: ResearchLeadRecord,
        task: dict[str, Any],
        request: ResearchHuntTaskRunRequest,
    ) -> dict[str, Any]:
        candidates = _research_hunt_source_followup_items_for_task(self.repository, lead, task, request)
        existing_by_identity_key = {
            item.identity_key: item
            for item in self.repository.list_source_followups(limit=None)
            if item.identity_key
        }
        queued: list[SourceFollowupQueueItem] = []
        preexisting: list[SourceFollowupQueueItem] = []
        errors: list[str] = []
        for candidate in candidates:
            if not candidate.identity_key:
                continue
            existing = existing_by_identity_key.get(candidate.identity_key)
            if existing is not None:
                preexisting.append(existing)
                continue
            persisted = self.repository.upsert_source_followup(candidate)
            existing_by_identity_key[persisted.identity_key] = persisted
            queued.append(persisted)

        ingestable_ids = [
            item.followup_id
            for item in [*queued, *preexisting]
            if item.status in {"queued", "approved"}
        ]
        ingest_result = None
        if ingestable_ids:
            source_followup_result = self.ingest_source_followups(
                SourceFollowupIngestRequest(
                    followup_ids=ingestable_ids[: request.claim_chunk_limit],
                    statuses=["queued", "approved"],
                    limit=min(len(ingestable_ids), request.claim_chunk_limit),
                    approved_by=request.operator,
                    run_claim_extraction=False,
                    dry_run=False,
                    metadata={
                        **request.metadata,
                        "research_hunt_task_executor": True,
                        "research_hunt_task_id": str(task.get("task_id") or ""),
                        "research_lead_id": str(lead.lead_id),
                    },
                )
            )
            ingest_result = source_followup_result.model_dump(mode="json")
            errors.extend(source_followup_result.errors)

        return {
            "candidate_count": len(candidates),
            "queued_count": len(queued),
            "preexisting_count": len(preexisting),
            "ingestable_count": len(ingestable_ids),
            "followup_ids": [str(item.followup_id) for item in [*queued, *preexisting]],
            "ingest_result": ingest_result,
            "errors": errors,
        }

    def _mark_research_hunt_task(
        self,
        lead_id: UUID,
        task_id: str,
        *,
        status: str,
        execution: dict[str, Any],
    ) -> None:
        lead = self.repository.get_research_lead(lead_id)
        if lead is None:
            return
        state = dict(lead.metadata.get("research_hunt") or {})
        tasks = [
            dict(task)
            for task in state.get("tasks", [])
            if isinstance(task, dict)
        ]
        now = datetime.now(UTC).isoformat()
        for task in tasks:
            if str(task.get("task_id") or "") != task_id:
                continue
            task["status"] = status
            task["updated_at"] = now
            task["last_execution"] = execution
            if status == "completed":
                task["completed_at"] = now
            if status == "failed":
                task["failed_at"] = now
        open_tasks = [task for task in tasks if task.get("status") == "open"]
        best_signal = state.get("best_signal") if isinstance(state.get("best_signal"), dict) else None
        signal_status = "supported" if _research_hunt_signal_score(best_signal) >= 80 else "developing"
        state.update(
            {
                "tasks": tasks,
                "open_task_count": len(open_tasks),
                "signal_status": signal_status,
                "coverage_status": "hunting" if open_tasks else "supported" if signal_status == "supported" else "insufficient",
                "last_updated_at": now,
            }
        )
        self.update_research_lead(lead_id, metadata={"research_hunt": state})

    def _update_research_hunt_state(
        self,
        result: ResearchFollowupLoopResult,
        request: ResearchFollowupLoopRequest,
        *,
        verdict: str | None,
        followup_actions: list[str],
    ) -> dict[str, Any]:
        lead = self.repository.get_research_lead(request.lead_id)
        if lead is None:
            return {}

        existing_state = dict(lead.metadata.get("research_hunt") or {})
        existing_tasks = [
            dict(task)
            for task in existing_state.get("tasks", [])
            if isinstance(task, dict)
        ]
        current_signal = _research_hunt_signal_candidate(result, verdict=verdict)
        best_signal = existing_state.get("best_signal") if isinstance(existing_state.get("best_signal"), dict) else None
        if current_signal and (
            best_signal is None or _research_hunt_signal_score(current_signal) >= _research_hunt_signal_score(best_signal)
        ):
            best_signal = current_signal

        new_tasks, suppressed_tasks = _research_hunt_tasks_from_result(
            lead,
            result,
            request,
            verdict=verdict,
            followup_actions=followup_actions,
            has_best_signal=best_signal is not None,
            existing_tasks=existing_tasks,
        )
        existing_suppressed_tasks = [
            dict(task)
            for task in existing_state.get("suppressed_tasks", [])
            if isinstance(task, dict)
        ]
        suppressed_history = [*existing_suppressed_tasks, *suppressed_tasks][-200:]
        tasks = [*existing_tasks, *new_tasks][-100:]
        open_tasks = [task for task in tasks if task.get("status") == "open"]
        signal_status = "supported" if _research_hunt_signal_score(best_signal) >= 80 else "developing"
        coverage_status = "hunting" if open_tasks else "supported" if signal_status == "supported" else "insufficient"
        state = {
            "version": "v1",
            "signal_status": signal_status,
            "coverage_status": coverage_status,
            "best_signal": best_signal,
            "tasks": tasks,
            "open_task_count": len(open_tasks),
            "suppressed_tasks": suppressed_history,
            "suppressed_task_count": len(suppressed_history),
            "last_updated_at": datetime.now(UTC).isoformat(),
            "last_loop": {
                "verdict": verdict,
                "evidence_fit": result.evidence_fit.fit if result.evidence_fit else None,
                "document_chunks": result.document_chunks,
                "claims_written": result.claims_written,
                "source_followups_linked": result.source_followups_linked,
                "source_followups_pending": result.source_followups_pending,
                "hunt_tasks_created": len(new_tasks),
                "hunt_tasks_suppressed": len(suppressed_tasks),
                "resolver_agent_run_id": str(result.resolver_agent_run_id) if result.resolver_agent_run_id else None,
                "evaluator_agent_run_id": str(result.evaluator_agent_run_id) if result.evaluator_agent_run_id else None,
            },
        }
        updated = self.update_research_lead(request.lead_id, metadata={"research_hunt": state})
        applied_state = dict((updated.metadata.get("research_hunt") if updated else state) or state)
        result.signal_status = str(applied_state.get("signal_status") or signal_status)
        result.coverage_status = str(applied_state.get("coverage_status") or coverage_status)
        result.best_signal = applied_state.get("best_signal") if isinstance(applied_state.get("best_signal"), dict) else None
        result.hunt_tasks_created += len(new_tasks)
        result.hunt_tasks_suppressed += len(suppressed_tasks)
        result.hunt_tasks = new_tasks
        result.suppressed_hunt_tasks = suppressed_tasks
        return applied_state

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

    def build_research_hunt_queue_report(
        self,
        request: ResearchHuntQueueReportRequest,
    ) -> ResearchHuntQueueReportResult:
        lead_id_filter = set(request.lead_ids)
        if lead_id_filter:
            leads = [
                lead
                for lead_id in lead_id_filter
                if (lead := self.repository.get_research_lead(lead_id)) is not None
            ]
        else:
            leads = self.repository.list_research_leads(statuses=request.lead_statuses, limit=None)
        if request.source_keys:
            source_keys = set(request.source_keys)
            leads = [
                lead
                for lead in leads
                if (
                    (lead.source_key and lead.source_key in source_keys)
                    or (lead.origin_source_key and lead.origin_source_key in source_keys)
                    or bool(source_keys.intersection(lead.suggested_sources))
                )
            ]
        scanned_lead_count = len(leads)
        leads = sorted(leads, key=lambda lead: (lead.priority, lead.created_at))[: request.limit]

        lead_rows: list[ResearchHuntLeadQueueRow] = []
        task_rows: list[ResearchHuntTaskQueueRow] = []
        all_task_rows: list[ResearchHuntTaskQueueRow] = []
        status_counts: Counter[str] = Counter()
        task_class_counts: Counter[str] = Counter()
        control_status_counts: Counter[str] = Counter()

        for lead in leads:
            state = lead.metadata.get("research_hunt") if isinstance(lead.metadata, dict) else None
            if not isinstance(state, dict):
                lead_row = ResearchHuntLeadQueueRow(
                    lead_id=lead.lead_id,
                    title=lead.title,
                    status=lead.status,
                    source_key=lead.source_key or lead.origin_source_key,
                    priority=lead.priority,
                    control_status="no_hunt_state",
                    recommended_action="no_hunt_state",
                )
                lead_rows.append(lead_row)
                control_status_counts[lead_row.control_status] += 1
                continue

            tasks = [dict(task) for task in state.get("tasks", []) if isinstance(task, dict)]
            suppressed_tasks = [
                dict(task)
                for task in state.get("suppressed_tasks", [])
                if isinstance(task, dict)
            ] if request.include_suppressed else []
            lead_task_rows: list[ResearchHuntTaskQueueRow] = []
            if request.include_tasks:
                for task in tasks:
                    row = _research_hunt_task_queue_row(
                        lead,
                        task,
                        stale_after_hours=request.stale_after_hours,
                    )
                    lead_task_rows.append(row)
                    all_task_rows.append(row)
                for task in suppressed_tasks:
                    row = _research_hunt_task_queue_row(
                        lead,
                        task,
                        stale_after_hours=request.stale_after_hours,
                    )
                    lead_task_rows.append(row)
                    all_task_rows.append(row)

            open_rows = [row for row in lead_task_rows if row.status == "open"]
            failed_rows = [row for row in lead_task_rows if row.status == "failed"]
            completed_rows = [row for row in lead_task_rows if row.status == "completed"]
            suppressed_rows = [row for row in lead_task_rows if row.status == "suppressed"]
            open_concrete_rows = [row for row in open_rows if row.task_class == "concrete"]
            open_broad_rows = [row for row in open_rows if row.task_class == "broad"]
            open_passive_rows = [row for row in open_rows if row.task_class == "passive"]
            stale_rows = [row for row in open_rows if row.stale]
            best_signal = state.get("best_signal") if isinstance(state.get("best_signal"), dict) else None
            best_signal_score = _research_hunt_signal_score(best_signal)
            control_status = _research_hunt_lead_control_status(
                has_hunt_state=True,
                best_signal_score=best_signal_score,
                open_concrete_count=len(open_concrete_rows),
                open_broad_count=len(open_broad_rows),
                open_passive_count=len(open_passive_rows),
                failed_count=len(failed_rows),
            )
            lead_row = ResearchHuntLeadQueueRow(
                lead_id=lead.lead_id,
                title=lead.title,
                status=lead.status,
                source_key=lead.source_key or lead.origin_source_key,
                priority=lead.priority,
                signal_status=state.get("signal_status"),
                coverage_status=state.get("coverage_status"),
                control_status=control_status,
                open_task_count=len(open_rows),
                open_concrete_count=len(open_concrete_rows),
                open_broad_count=len(open_broad_rows),
                open_passive_count=len(open_passive_rows),
                blocked_task_count=len(failed_rows),
                stale_task_count=len(stale_rows),
                suppressed_task_count=len(suppressed_rows),
                completed_task_count=len(completed_rows),
                failed_task_count=len(failed_rows),
                best_signal_score=best_signal_score,
                recommended_action=_research_hunt_lead_recommended_action(control_status),
            )
            lead_rows.append(lead_row)
            control_status_counts[lead_row.control_status] += 1

        for row in all_task_rows:
            status_counts[row.status] += 1
            task_class_counts[row.task_class] += 1
        task_rows = sorted(
            all_task_rows,
            key=lambda row: (
                0 if row.runnable_by_default else 1,
                _research_hunt_task_status_rank(row.status),
                row.priority if row.priority is not None else 1000,
                row.age_hours if row.age_hours is not None else 0,
            ),
        )[: request.task_limit]

        return ResearchHuntQueueReportResult(
            scanned_lead_count=scanned_lead_count,
            lead_count=len(lead_rows),
            executable_task_count=sum(1 for row in all_task_rows if row.runnable_by_default),
            broad_task_count=sum(1 for row in all_task_rows if row.status == "open" and row.task_class == "broad"),
            passive_task_count=sum(1 for row in all_task_rows if row.status == "open" and row.task_class == "passive"),
            stale_task_count=sum(1 for row in all_task_rows if row.stale),
            suppressed_task_count=sum(1 for row in all_task_rows if row.status == "suppressed"),
            blocked_lead_count=control_status_counts["blocked"],
            ready_for_synthesis_count=control_status_counts["ready_for_synthesis"],
            watching_count=control_status_counts["watching"],
            hunting_count=control_status_counts["hunting"],
            status_counts=dict(status_counts),
            task_class_counts=dict(task_class_counts),
            control_status_counts=dict(control_status_counts),
            leads=lead_rows,
            tasks=task_rows,
        )

    def maintain_research_hunt_queue(
        self,
        request: ResearchHuntQueueMaintenanceRequest,
    ) -> ResearchHuntQueueMaintenanceResult:
        report = self.build_research_hunt_queue_report(
            ResearchHuntQueueReportRequest(
                lead_ids=request.lead_ids,
                lead_statuses=request.lead_statuses,
                source_keys=request.source_keys,
                limit=1000,
                task_limit=2000,
                stale_after_hours=request.stale_after_hours,
                include_tasks=True,
                include_suppressed=False,
            )
        )
        allowed_reasons = set(request.reasons)
        candidates: list[ResearchHuntQueueMaintenanceItem] = []
        skipped: list[dict[str, Any]] = []
        seen_open_family_keys: set[tuple[UUID, str, str]] = set()
        tasks_by_lead: dict[UUID, list[ResearchHuntTaskQueueRow]] = {}
        for task in report.tasks:
            tasks_by_lead.setdefault(task.lead_id, []).append(task)

        for lead in report.leads:
            for task in tasks_by_lead.get(lead.lead_id, []):
                if task.status != "open":
                    continue
                reason = _research_hunt_maintenance_suppression_reason(
                    task,
                    seen_open_family_keys=seen_open_family_keys,
                    allowed_reasons=allowed_reasons,
                )
                if reason is None:
                    skipped.append(
                        {
                            "lead_id": str(task.lead_id),
                            "task_id": task.task_id,
                            "task_type": task.task_type,
                            "task_class": task.task_class,
                            "reason": "outside_maintenance_scope",
                        }
                    )
                    continue
                candidates.append(
                    ResearchHuntQueueMaintenanceItem(
                        lead_id=task.lead_id,
                        task_id=task.task_id or "",
                        task_type=task.task_type,
                        task_class=task.task_class,
                        action=task.action,
                        previous_status=task.status,
                        suppression_reason=reason,  # type: ignore[arg-type]
                        identity_key=task.identity_key,
                        family_key=task.family_key,
                        age_hours=task.age_hours,
                        dry_run=request.dry_run,
                    )
                )
                if len(candidates) >= request.limit:
                    break
            if len(candidates) >= request.limit:
                break

        errors: list[str] = []
        updated_lead_ids: set[UUID] = set()
        applied_items: list[ResearchHuntQueueMaintenanceItem] = []
        if not request.dry_run:
            now = datetime.now(UTC).isoformat()
            candidates_by_lead: dict[UUID, list[ResearchHuntQueueMaintenanceItem]] = {}
            for item in candidates:
                candidates_by_lead.setdefault(item.lead_id, []).append(item)
            for lead_id, lead_items in candidates_by_lead.items():
                lead = self.repository.get_research_lead(lead_id)
                if lead is None:
                    errors.append(f"lead not found during maintenance: {lead_id}")
                    continue
                state = dict(lead.metadata.get("research_hunt") or {})
                tasks = [dict(task) for task in state.get("tasks", []) if isinstance(task, dict)]
                suppressed_tasks = [
                    dict(task)
                    for task in state.get("suppressed_tasks", [])
                    if isinstance(task, dict)
                ]
                item_by_task_id = {item.task_id: item for item in lead_items}
                remaining_tasks: list[dict[str, Any]] = []
                moved_tasks: list[dict[str, Any]] = []
                for task in tasks:
                    task_id = str(task.get("task_id") or "")
                    item = item_by_task_id.get(task_id)
                    if item is None:
                        remaining_tasks.append(task)
                        continue
                    moved = dict(task)
                    moved["status"] = "suppressed"
                    moved["suppression_reason"] = item.suppression_reason
                    moved["suppressed_at"] = now
                    moved["updated_at"] = now
                    moved["maintenance"] = {
                        "operator": request.operator,
                        "dagster_run_id": request.dagster_run_id,
                        "previous_status": item.previous_status,
                        "reason": item.suppression_reason,
                        **request.metadata,
                    }
                    moved_tasks.append(moved)
                    applied_items.append(item.model_copy(update={"dry_run": False}))
                if not moved_tasks:
                    continue
                suppressed_history = [*suppressed_tasks, *moved_tasks][-200:]
                best_signal = state.get("best_signal") if isinstance(state.get("best_signal"), dict) else None
                signal_status = "supported" if _research_hunt_signal_score(best_signal) >= 80 else "developing"
                open_tasks = [task for task in remaining_tasks if task.get("status") == "open"]
                control_status = _research_hunt_control_status_from_state(
                    remaining_tasks,
                    best_signal=best_signal,
                    has_hunt_state=True,
                )
                state.update(
                    {
                        "tasks": remaining_tasks,
                        "suppressed_tasks": suppressed_history,
                        "suppressed_task_count": len(suppressed_history),
                        "open_task_count": len(open_tasks),
                        "signal_status": signal_status,
                        "coverage_status": _research_hunt_coverage_status_from_control(
                            control_status,
                            signal_status=signal_status,
                        ),
                        "control_status": control_status,
                        "last_updated_at": now,
                        "last_maintenance": {
                            "action": request.action,
                            "operator": request.operator,
                            "dry_run": False,
                            "candidate_count": len(lead_items),
                            "suppressed_count": len(moved_tasks),
                            "dagster_run_id": request.dagster_run_id,
                            "updated_at": now,
                            **request.metadata,
                        },
                    }
                )
                updated = self.update_research_lead(lead_id, metadata={"research_hunt": state})
                if updated is None:
                    errors.append(f"lead disappeared during maintenance update: {lead_id}")
                    continue
                updated_lead_ids.add(lead_id)

        return ResearchHuntQueueMaintenanceResult(
            action=request.action,
            dry_run=request.dry_run,
            candidate_count=len(candidates),
            suppressed_count=0 if request.dry_run else len(applied_items),
            updated_lead_count=0 if request.dry_run else len(updated_lead_ids),
            skipped_count=len(skipped),
            items=candidates if request.dry_run else applied_items,
            skipped=skipped,
            errors=errors,
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


def _therapy_idea_record_from_result(
    idea: TherapyIdea,
    result: TherapyCommitteeResult,
    request: TherapyCommitteeRequest,
) -> TherapyIdeaRecord:
    status = "ready_for_promotion" if idea.priority_score >= 0.65 and len(idea.evidence_refs) >= 2 else "proposed"
    promotion_state = "ready_for_validation_plan" if status == "ready_for_promotion" else "needs_more_evidence"
    return TherapyIdeaRecord(
        therapy_idea_id=idea.idea_id,
        idea=idea,
        committee_run_id=result.committee_run_id,
        agent_run_id=result.agent_run_id,
        source_brief_id=result.source_brief_id or request.brief_id,
        source_evaluation_id=result.source_evaluation_id or request.evaluation_id,
        topic=result.topic,
        disease_scope=result.disease_scope,
        source_key=request.source_key,
        status=status,
        promotion_state=promotion_state,
        score=idea.priority_score,
        evidence_refs=idea.evidence_refs,
        targets=idea.targets,
        biomarkers=idea.biomarkers,
        candidate_therapies=idea.candidate_therapies,
        risks=idea.risks,
        next_experiments=idea.next_experiments,
        promotion_metadata={
            "review_mode": request.review_mode,
            "model_profile": request.model_profile,
            "source_brief_id": str(result.source_brief_id or request.brief_id) if result.source_brief_id or request.brief_id else None,
            "source_evaluation_id": (
                str(result.source_evaluation_id or request.evaluation_id)
                if result.source_evaluation_id or request.evaluation_id
                else None
            ),
            "recommend_only": True,
        },
        metadata={
            "therapy_committee": {
                "committee_run_id": str(result.committee_run_id),
                "agent_run_id": str(result.agent_run_id) if result.agent_run_id else None,
            }
        },
    )


def _build_hypothesis_promotion_report(
    repository: ResearchRepository,
    request: HypothesisPromotionReportRequest,
) -> HypothesisPromotionReportResult:
    candidates: list[HypothesisPromotionCandidate] = []
    errors: list[str] = []
    queued_idea_ids = _queued_therapy_idea_ids(repository)

    try:
        therapy_records = repository.list_therapy_ideas(
            source_brief_id=request.brief_id,
            source_evaluation_id=request.evaluation_id,
            topic_query=request.topic_query,
            limit=request.limit,
        )
        if request.therapy_idea_id:
            therapy_records = [
                record for record in therapy_records if record.therapy_idea_id == request.therapy_idea_id
            ] or [
                record
                for record in [repository.get_therapy_idea(request.therapy_idea_id)]
                if record is not None
            ]
        if request.source_key:
            therapy_records = [record for record in therapy_records if record.source_key == request.source_key]
        for record in therapy_records:
            candidates.append(_promotion_candidate_from_therapy_idea(repository, record, queued_idea_ids))
    except Exception as exc:
        errors.append(f"Could not build therapy idea promotion candidates: {exc}")

    if request.therapy_idea_id is None:
        try:
            briefs = _promotion_briefs(repository, request)
            for brief in briefs:
                evaluation = _latest_promotion_evaluation(repository, brief.brief_id, request.evaluation_id)
                candidates.extend(
                    _promotion_candidates_from_brief(
                        repository=repository,
                        brief=brief,
                        evaluation=evaluation,
                    )
                )
        except Exception as exc:
            errors.append(f"Could not build research brief promotion candidates: {exc}")

    candidates = [_filter_promotion_candidate(candidate, request) for candidate in candidates]
    candidates = [candidate for candidate in candidates if candidate is not None]
    candidates.sort(key=lambda candidate: (_promotion_state_rank(candidate.promotion_state), -candidate.score, candidate.title))
    candidates = candidates[: request.limit]
    return HypothesisPromotionReportResult(
        candidate_count=len(candidates),
        state_counts=dict(sorted(Counter(candidate.promotion_state for candidate in candidates).items())),
        candidates=candidates,
        errors=errors,
    )


def _promotion_candidate_from_therapy_idea(
    repository: ResearchRepository,
    record: TherapyIdeaRecord,
    queued_idea_ids: set[str],
) -> HypothesisPromotionCandidate:
    state = _therapy_idea_promotion_state(repository, record, queued_idea_ids)
    blockers = _therapy_idea_blockers(repository, record, state)
    objective = " ".join(
        [
            record.idea.hypothesis,
            " ".join(record.candidate_therapies),
            " ".join(record.targets),
            " ".join(record.biomarkers),
            " ".join(record.risks),
        ]
    )
    matches = match_validation_tools(
        ValidationToolMatchRequest(
            validation_type=_promotion_validation_type(record),
            task_type=_promotion_task_type(record),
            objective=objective,
            candidate_name=record.candidate_therapies[0] if record.candidate_therapies else None,
            target_name=record.targets[0] if record.targets else None,
            required_inputs=[
                "committee-ranked therapy idea",
                "cited evidence refs",
                *("target and biomarker list" for _ in record.targets[:1]),
                *("candidate therapy list" for _ in record.candidate_therapies[:1]),
            ],
            limit=3,
        )
    ).matches
    return HypothesisPromotionCandidate(
        candidate_id=f"therapy_idea:{record.therapy_idea_id}",
        source_type="therapy_idea",
        source_id=str(record.therapy_idea_id),
        brief_id=record.source_brief_id,
        evaluation_id=record.source_evaluation_id,
        therapy_idea_id=record.therapy_idea_id,
        committee_run_id=record.committee_run_id,
        title=record.idea.title,
        hypothesis=record.idea.hypothesis,
        promotion_state=state,
        score=record.score,
        candidate_therapies=record.candidate_therapies,
        targets=record.targets,
        biomarkers=record.biomarkers,
        evidence_refs=record.evidence_refs,
        risks=record.risks,
        next_experiments=record.next_experiments,
        blockers=blockers,
        recommended_next_action=_recommended_promotion_action(state),
        recommended_job_name=_recommended_promotion_job(state),
        matched_tools=matches,
        metadata={
            "status": record.status,
            "evidence_strength": record.idea.evidence_strength,
            "promotion_metadata": record.promotion_metadata,
        },
    )


def _promotion_candidates_from_brief(
    *,
    repository: ResearchRepository,
    brief: ResearchBriefRecord,
    evaluation: ResearchBriefEvaluationRecord | None,
) -> list[HypothesisPromotionCandidate]:
    payload = brief.result_payload or {}
    findings: list[ResearchBriefFinding] = []
    for raw in payload.get("ranked_hypotheses", []):
        if not isinstance(raw, dict):
            continue
        try:
            findings.append(ResearchBriefFinding.model_validate(raw))
        except Exception:
            continue
    candidates: list[HypothesisPromotionCandidate] = []
    for index, finding in enumerate(findings[:8], start=1):
        state = _brief_hypothesis_promotion_state(brief, evaluation)
        text = f"{finding.claim} {finding.reasoning} {' '.join(finding.open_questions)}"
        therapies = _extract_known_terms(text, _PROMOTION_THERAPY_TERMS)
        targets = _extract_known_terms(text, _PROMOTION_TARGET_TERMS)
        biomarkers = _extract_known_terms(text, _PROMOTION_BIOMARKER_TERMS)
        matches = match_validation_tools(
            ValidationToolMatchRequest(
                validation_type="expert_review" if state == "ready_for_committee" else "omics",
                task_type="expert_review" if state == "ready_for_committee" else "omics",
                objective=text,
                candidate_name=therapies[0] if therapies else None,
                target_name=targets[0] if targets else None,
                required_inputs=["research brief", "citation set", "synthesis evaluation"],
                limit=3,
            )
        ).matches
        candidates.append(
            HypothesisPromotionCandidate(
                candidate_id=f"brief_hypothesis:{brief.brief_id}:{index}",
                source_type="research_brief_hypothesis",
                source_id=f"{brief.brief_id}:{index}",
                brief_id=brief.brief_id,
                evaluation_id=evaluation.evaluation_id if evaluation else None,
                title=_title_from_claim(finding.claim),
                hypothesis=finding.claim,
                promotion_state=state,
                score=_promotion_score_from_finding(finding, evaluation),
                candidate_therapies=therapies,
                targets=targets,
                biomarkers=biomarkers,
                evidence_refs=finding.citations,
                risks=finding.open_questions,
                next_experiments=["Run therapy committee review from this evaluated brief."],
                blockers=_brief_hypothesis_blockers(brief, evaluation, state),
                recommended_next_action=_recommended_promotion_action(state),
                recommended_job_name=_recommended_promotion_job(state),
                matched_tools=matches,
                metadata={
                    "topic": brief.topic,
                    "source_key": brief.source_key,
                    "evidence_strength": finding.evidence_strength,
                    "stance": finding.stance,
                    "evaluation_readiness": evaluation.readiness if evaluation else None,
                    "evaluation_score": evaluation.overall_score if evaluation else None,
                },
            )
        )
    return candidates


def _promotion_briefs(
    repository: ResearchRepository,
    request: HypothesisPromotionReportRequest,
) -> list[ResearchBriefRecord]:
    if request.brief_id:
        brief = repository.get_research_brief(request.brief_id)
        return [brief] if brief else []
    if request.evaluation_id:
        evaluation = repository.get_research_brief_evaluation(request.evaluation_id)
        if evaluation is None:
            return []
        brief = repository.get_research_brief(evaluation.brief_id)
        return [brief] if brief else []
    return repository.list_research_briefs(
        status="completed",
        source_key=request.source_key,
        topic_query=request.topic_query,
        limit=request.limit,
    )


def _latest_promotion_evaluation(
    repository: ResearchRepository,
    brief_id: UUID,
    evaluation_id: UUID | None,
) -> ResearchBriefEvaluationRecord | None:
    if evaluation_id:
        return repository.get_research_brief_evaluation(evaluation_id)
    evaluations = repository.list_research_brief_evaluations(brief_id=brief_id, limit=1)
    return evaluations[0] if evaluations else None


def _therapy_idea_promotion_state(
    repository: ResearchRepository,
    record: TherapyIdeaRecord,
    queued_idea_ids: set[str],
) -> str:
    if record.status in {"archived", "rejected"}:
        return "blocked"
    if str(record.therapy_idea_id) in queued_idea_ids or record.status == "queued_for_validation":
        return "queued_for_validation"
    evaluation = repository.get_research_brief_evaluation(record.source_evaluation_id) if record.source_evaluation_id else None
    if _needs_citation_repair(record=record, evaluation=evaluation):
        return "needs_citation_repair"
    if len(record.evidence_refs) < 2 or record.score < 0.55:
        return "needs_more_evidence"
    return "ready_for_validation_plan"


def _brief_hypothesis_promotion_state(
    brief: ResearchBriefRecord,
    evaluation: ResearchBriefEvaluationRecord | None,
) -> str:
    if _needs_citation_repair(brief=brief, evaluation=evaluation):
        return "needs_citation_repair"
    if evaluation is None or not evaluation.passes_quality_bar or evaluation.readiness not in {
        "ready_for_hypothesis_review",
        "ready_for_validation",
    }:
        return "needs_more_evidence"
    return "ready_for_committee"


def _needs_citation_repair(
    *,
    brief: ResearchBriefRecord | None = None,
    evaluation: ResearchBriefEvaluationRecord | None = None,
    record: TherapyIdeaRecord | None = None,
) -> bool:
    payloads = [
        payload
        for payload in (
            brief.result_payload if brief else None,
            evaluation.result_payload if evaluation else None,
            evaluation.summary if evaluation else None,
            record.promotion_metadata if record else None,
            record.metadata if record else None,
        )
        if isinstance(payload, dict)
    ]
    if any(_payload_has_citation_repair_flag(payload) for payload in payloads):
        return True
    repair_text = " ".join(_citation_repair_text_fields(payloads)).casefold()
    if _CITATION_REPAIR_PATTERN.search(repair_text):
        return True
    return False


_CITATION_REPAIR_PATTERN = re.compile(
    r"\b(?:citation|citations|reference|references|doi|pmid|pmcid)\b.{0,80}\b(?:repair|deduplicate|dedupe)\b.{0,40}\b(?:need(?:ed|s)?|required|before promotion|before validation)\b"
    r"|\b(?:repair|deduplicate|dedupe)\b.{0,40}\b(?:citation|citations|reference|references)\b.{0,40}\b(?:need(?:ed|s)?|required|before promotion|before validation)\b"
)


def _payload_has_citation_repair_flag(payload: dict[str, Any]) -> bool:
    for key in (
        "citation_repair_required",
        "needs_citation_repair",
        "unresolved_citation_duplicates",
        "has_unresolved_citation_duplicates",
    ):
        if payload.get(key) is True:
            return True
    for key in ("unresolved_citation_duplicate_count", "citation_repair_count"):
        try:
            if int(payload.get(key) or 0) > 0:
                return True
        except (TypeError, ValueError):
            pass
    for value in payload.values():
        if isinstance(value, dict) and _payload_has_citation_repair_flag(value):
            return True
    return False


def _citation_repair_text_fields(payloads: list[dict[str, Any]]) -> list[str]:
    values: list[str] = []
    for payload in payloads:
        values.extend(_nested_strings(payload.get("errors")))
        values.extend(_nested_strings(payload.get("weaknesses")))
        values.extend(_nested_strings(payload.get("recommendations")))
        evidence = payload.get("evidence")
        if isinstance(evidence, dict):
            values.extend(_nested_strings(evidence.get("errors")))
            values.extend(_nested_strings(evidence.get("weaknesses")))
            values.extend(_nested_strings(evidence.get("notable_risks")))
            values.extend(_nested_strings(evidence.get("recommendations")))
        for value in payload.values():
            if isinstance(value, dict):
                values.extend(_citation_repair_text_fields([value]))
    return values


def _therapy_idea_blockers(
    repository: ResearchRepository,
    record: TherapyIdeaRecord,
    state: str,
) -> list[str]:
    blockers: list[str] = []
    if state == "needs_citation_repair":
        blockers.append("citation_repair_required")
    if len(record.evidence_refs) < 2:
        blockers.append("at_least_two_evidence_refs_required")
    if record.source_evaluation_id:
        evaluation = repository.get_research_brief_evaluation(record.source_evaluation_id)
        if evaluation and not evaluation.passes_quality_bar:
            blockers.append("source_evaluation_did_not_pass_quality_bar")
    if state == "blocked":
        blockers.append(f"therapy_idea_status_{record.status}")
    return _dedupe_quality_labels(blockers)


def _brief_hypothesis_blockers(
    brief: ResearchBriefRecord,
    evaluation: ResearchBriefEvaluationRecord | None,
    state: str,
) -> list[str]:
    blockers: list[str] = []
    if state == "needs_citation_repair":
        blockers.append("citation_repair_required")
    if evaluation is None:
        blockers.append("synthesis_evaluation_required")
    elif not evaluation.passes_quality_bar:
        blockers.append("synthesis_quality_bar_required")
    if brief.citation_count < 2:
        blockers.append("at_least_two_unique_citations_required")
    return _dedupe_quality_labels(blockers)


def _queued_therapy_idea_ids(repository: ResearchRepository) -> set[str]:
    queued: set[str] = set()
    for item in repository.list_validation_request_queue_items(limit=None):
        idea_id = str((item.metadata or {}).get("idea_id") or "").strip()
        if idea_id:
            queued.add(idea_id)
    return queued


def _filter_promotion_candidate(
    candidate: HypothesisPromotionCandidate,
    request: HypothesisPromotionReportRequest,
) -> HypothesisPromotionCandidate | None:
    if candidate.promotion_state in {"needs_citation_repair", "needs_more_evidence", "blocked"} and not request.include_blocked:
        return None
    if candidate.promotion_state == "ready_for_committee" and not request.include_ready_for_committee:
        return None
    if candidate.promotion_state in {"ready_for_validation_plan", "queued_for_validation"} and not request.include_ready_for_validation:
        return None
    return candidate


def _recommended_promotion_action(state: str) -> str:
    return {
        "needs_citation_repair": "Repair/dedupe citations and rerun synthesis evaluation before promotion.",
        "needs_more_evidence": "Queue focused evidence acquisition before committee or validation planning.",
        "ready_for_committee": "Run therapy committee from the evaluated brief.",
        "ready_for_validation_plan": "Create a recommend-only validation plan using the matched tool hints.",
        "queued_for_validation": "Wait for validation queue review/dispatch outcome.",
        "blocked": "Keep blocked until an operator changes the idea status or resolves blockers.",
    }[state]


def _recommended_promotion_job(state: str) -> str | None:
    return {
        "needs_citation_repair": "research_brief_evaluation_job",
        "needs_more_evidence": "research_hunt_synthesis_queue_job",
        "ready_for_committee": "therapy_committee_job",
        "ready_for_validation_plan": "validation_plan_job",
        "queued_for_validation": None,
        "blocked": None,
    }[state]


def _promotion_state_rank(state: str) -> int:
    return {
        "ready_for_validation_plan": 0,
        "ready_for_committee": 1,
        "queued_for_validation": 2,
        "needs_citation_repair": 3,
        "needs_more_evidence": 4,
        "blocked": 5,
    }.get(state, 9)


def _promotion_validation_type(record: TherapyIdeaRecord) -> str:
    if record.risks:
        return "safety"
    if record.candidate_therapies and (record.targets or record.biomarkers):
        return "wet_lab"
    if record.targets or record.biomarkers:
        return "omics"
    return "expert_review"


def _promotion_task_type(record: TherapyIdeaRecord) -> str:
    if record.risks:
        return "safety"
    if record.candidate_therapies and (record.targets or record.biomarkers):
        return "wet_lab"
    if record.targets or record.biomarkers:
        return "omics"
    return "expert_review"


def _promotion_score_from_finding(
    finding: ResearchBriefFinding,
    evaluation: ResearchBriefEvaluationRecord | None,
) -> float:
    strength = {"high": 0.75, "medium": 0.6, "low": 0.42, "unknown": 0.32}.get(finding.evidence_strength, 0.32)
    eval_score = evaluation.overall_score if evaluation else 0.4
    return round(min(1.0, (strength * 0.55) + (eval_score * 0.45)), 3)


def _title_from_claim(claim: str) -> str:
    text = re.sub(r"\s+", " ", claim).strip()
    if len(text) <= 96:
        return text
    return text[:93].rstrip(" .,;:") + "..."


def _extract_known_terms(text: str, terms: set[str]) -> list[str]:
    normalized = text.casefold()
    return sorted({term for term in terms if term.casefold() in normalized})


_PROMOTION_THERAPY_TERMS = {
    "dasatinib",
    "doxorubicin",
    "mirdametinib",
    "paclitaxel",
    "pazopanib",
    "propranolol",
    "rapamycin",
    "sirolimus",
    "sorafenib",
    "toceranib",
    "trametinib",
}

_PROMOTION_TARGET_TERMS = {
    "ANGPT2",
    "EGFR",
    "FLT4",
    "KDR",
    "KIT",
    "MET",
    "MTOR",
    "PDGFRB",
    "PIK3CA",
    "PTEN",
    "TP53",
    "VEGFA",
    "VEGFR",
}

_PROMOTION_BIOMARKER_TERMS = {
    "CD31",
    "CD34",
    "KIT",
    "KDR",
    "MKI67",
    "PDGFRB",
    "VEGFA",
}


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


def _research_brief_operator_doc_from_brief(
    repository: ResearchRepository,
    brief: ResearchBriefRecord,
    request: ResearchBriefOperatorDocRequest,
) -> tuple[ResearchBriefOperatorDocument, ArtifactHandle | None]:
    title = _research_brief_operator_doc_title(brief)
    hypotheses = _research_brief_result_list(brief, "ranked_hypotheses")[: request.max_hypotheses]
    unresolved_questions = _research_brief_result_list(brief, "unresolved_questions")[
        : request.max_unresolved_questions
    ]
    evidence_limitations = _research_brief_record_evidence_limitations(brief)[
        : request.max_evidence_limitations
    ]
    citations = _research_brief_result_list(brief, "citations")
    plain_summary = _research_brief_operator_doc_plain_summary(
        brief,
        hypotheses=hypotheses,
        evidence_limitations=evidence_limitations,
    )
    markdown, footnote_count = _render_research_brief_operator_doc_markdown(
        brief=brief,
        title=title,
        plain_summary=plain_summary,
        hypotheses=hypotheses,
        unresolved_questions=unresolved_questions,
        evidence_limitations=evidence_limitations,
        citations=citations,
        request=request,
    )
    artifact_id = uuid5(NAMESPACE_URL, f"twog:research-brief-operator-doc:{brief.brief_id}")
    artifact_uri = f"twog://research-brief/operator-doc/{artifact_id}"
    document = ResearchBriefOperatorDocument(
        brief_id=brief.brief_id,
        title=title,
        artifact_id=None if request.dry_run else artifact_id,
        artifact_uri=None if request.dry_run else artifact_uri,
        markdown=markdown,
        plain_language_summary=plain_summary,
        citation_count=brief.citation_count,
        hypothesis_count=len(hypotheses),
        unresolved_question_count=len(unresolved_questions),
        evidence_limitation_count=len(evidence_limitations),
        technical_footnote_count=footnote_count,
        metadata={
            **request.metadata,
            "source_key": brief.source_key,
            "brief_status": brief.status,
            "brief_style": brief.brief_style,
            "model_profile": brief.model_profile,
            "review_mode": brief.review_mode,
            "operator": request.operator,
            "dagster_run_id": request.dagster_run_id,
        },
    )
    if request.dry_run:
        return document, None

    artifact = ArtifactHandle(
        artifact_id=artifact_id,
        artifact_type="research_brief_operator_markdown",
        uri=artifact_uri,
        legal_status="derived_from_local_research_metadata",
        mime_type="text/markdown",
        metadata={
            **document.metadata,
            "brief_id": str(brief.brief_id),
            "agent_run_id": str(brief.agent_run_id) if brief.agent_run_id else None,
            "agent_run_ids": [str(agent_run_id) for agent_run_id in brief.agent_run_ids],
            "title": title,
            "markdown": markdown,
            "plain_language_summary": plain_summary,
            "citation_count": brief.citation_count,
            "hypothesis_count": len(hypotheses),
            "unresolved_question_count": len(unresolved_questions),
            "evidence_limitation_count": len(evidence_limitations),
            "technical_footnote_count": footnote_count,
            "created_at": document.created_at.isoformat(),
        },
    )
    repository.upsert_artifact(artifact)
    return document.model_copy(update={"artifact_id": artifact.artifact_id, "artifact_uri": artifact.uri}), artifact


def _research_brief_operator_doc_title(brief: ResearchBriefRecord) -> str:
    topic = brief.topic.strip()
    if topic.lower().startswith("review research lead:"):
        topic = topic.split(":", 1)[1].strip()
    topic = topic.split("|", 1)[0].strip()
    return topic[:240] or "Research synthesis brief"


def _research_brief_operator_doc_plain_summary(
    brief: ResearchBriefRecord,
    *,
    hypotheses: list[Any],
    evidence_limitations: list[str],
) -> str:
    first_hypothesis = _brief_finding_claim(hypotheses[0]) if hypotheses else ""
    if first_hypothesis:
        basis = first_hypothesis
    elif brief.final_brief:
        basis = _first_nonheading_sentence(brief.final_brief)
    else:
        basis = str(brief.summary.get("topic") if isinstance(brief.summary, dict) else "")
    limitation = evidence_limitations[0] if evidence_limitations else ""
    if limitation:
        return (
            f"The synthesis currently says: {basis} The important caveat is: {limitation} "
            "This should be treated as a decision-support brief, not as proof of clinical benefit."
        )
    return (
        f"The synthesis currently says: {basis} This should be treated as a decision-support brief, "
        "not as proof of clinical benefit."
    )


def _render_research_brief_operator_doc_markdown(
    *,
    brief: ResearchBriefRecord,
    title: str,
    plain_summary: str,
    hypotheses: list[Any],
    unresolved_questions: list[Any],
    evidence_limitations: list[str],
    citations: list[Any],
    request: ResearchBriefOperatorDocRequest,
) -> tuple[str, int]:
    footnotes: list[str] = []
    citation_by_id = {
        str(citation.get("citation_id") or ""): citation
        for citation in citations
        if isinstance(citation, dict)
    }
    lines = [
        f"# {title}",
        "",
        "## Plain-language summary",
        plain_summary,
        "",
        "## Bottom line",
        (
            "This brief is ready for human review and next-step planning. It does not say a therapy is proven. "
            "It separates what the agents found from what still needs direct evidence."
        ),
        "",
        "## What looks supported",
    ]
    if hypotheses:
        for hypothesis in hypotheses:
            claim = _brief_finding_claim(hypothesis)
            if not claim:
                continue
            marker = _append_operator_doc_footnote(
                footnotes,
                request,
                _brief_hypothesis_footnote(hypothesis, citation_by_id),
            )
            lines.append(f"- {claim}{marker}")
    else:
        lines.append("- No ranked hypotheses were persisted with this brief.")

    lines.extend(["", "## What is still missing"])
    if evidence_limitations:
        for limitation in evidence_limitations:
            lines.append(f"- {_humanize_doc_text(limitation)}")
    else:
        lines.append("- No evidence limitations were persisted with this brief.")

    if unresolved_questions:
        lines.extend(["", "## Questions to hand to the next agent"])
        for question in unresolved_questions:
            text = str(question).strip()
            if text:
                lines.append(f"- {_humanize_doc_text(text)}")

    lines.extend(
        [
            "",
            "## Recommended next move",
            (
                "- Create focused evidence-acquisition tasks for the missing direct evidence, then rerun synthesis "
                "after those tasks either find records or confirm the gap."
            ),
            (
                "- Keep treatment-context labels explicit: monotherapy, maintenance, adjuvant combination, "
                "in-vitro mechanism, safety, and cross-species translation are separate claims."
            ),
            "",
            "## System record",
            f"- Brief ID: {brief.brief_id}",
            f"- Status: {brief.status}",
            f"- Source key: {brief.source_key or 'mixed'}",
            f"- Citations: {brief.citation_count}",
            f"- Findings: {brief.finding_count}",
            f"- Ranked hypotheses: {brief.hypothesis_count}",
            f"- Evidence limitations: {brief.evidence_limitation_count}",
            f"- Open questions: {brief.unresolved_question_count}",
        ]
    )

    if request.include_technical_footnotes:
        lines.extend(["", "## Technical footnotes"])
        _append_operator_doc_footnote(
            footnotes,
            request,
            (
                f"Brief generated by agent run {brief.agent_run_id}; perspective runs: "
                f"{[str(agent_run_id) for agent_run_id in brief.agent_run_ids]}; "
                f"model_profile={brief.model_profile}; review_mode={brief.review_mode}."
            ),
        )
        for citation in citations:
            if isinstance(citation, dict):
                _append_operator_doc_footnote(footnotes, request, _brief_citation_footnote(citation))
        if not footnotes:
            lines.append("No technical footnotes were available for this brief.")
        else:
            for index, footnote in enumerate(footnotes, start=1):
                lines.append(f"[T{index}] {footnote}")

    return "\n".join(lines).strip() + "\n", len(footnotes)


def _research_brief_result_list(brief: ResearchBriefRecord, key: str) -> list[Any]:
    value = (brief.result_payload or {}).get(key)
    return value if isinstance(value, list) else []


def _brief_finding_claim(finding: Any) -> str:
    if isinstance(finding, dict):
        return str(finding.get("claim") or "").strip()
    return str(finding).strip()


def _brief_hypothesis_footnote(finding: Any, citation_by_id: dict[str, Any]) -> str:
    if not isinstance(finding, dict):
        return str(finding)
    citations = [str(citation_id) for citation_id in finding.get("citations", [])]
    citation_summaries = [
        _brief_citation_footnote(citation_by_id[citation_id])
        for citation_id in citations
        if citation_id in citation_by_id
    ]
    return "; ".join(
        item
        for item in [
            f"stance={finding.get('stance')}",
            f"evidence_strength={finding.get('evidence_strength')}",
            f"reasoning={finding.get('reasoning')}",
            f"citations={citation_summaries[:3]}",
        ]
        if item and item != "citations=[]"
    )


def _brief_citation_footnote(citation: dict[str, Any]) -> str:
    return _truncate(
        " ".join(
            item
            for item in [
                str(citation.get("citation_id") or "").strip(),
                str(citation.get("title") or "").strip(),
                str(citation.get("source_key") or "").strip(),
                str(citation.get("source_url") or "").strip(),
                str(citation.get("quote") or "").strip(),
            ]
            if item
        ),
        1800,
    )


def _append_operator_doc_footnote(
    footnotes: list[str],
    request: ResearchBriefOperatorDocRequest,
    text: str,
) -> str:
    if not request.include_technical_footnotes or len(footnotes) >= request.max_technical_footnotes:
        return ""
    normalized = _truncate(re.sub(r"\s+", " ", str(text).strip()), 1800)
    if not normalized:
        return ""
    if normalized in footnotes:
        return f" [T{footnotes.index(normalized) + 1}]"
    footnotes.append(normalized)
    return f" [T{len(footnotes)}]"


def _first_nonheading_sentence(markdown: str) -> str:
    for raw_line in markdown.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        sentence = re.split(r"(?<=[.!?])\s+", line, maxsplit=1)[0].strip()
        if sentence:
            return sentence[:1000]
    return ""


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


_RESEARCH_BRIEF_FOLLOWUP_QUALITY_STATUSES = {
    "needs_followup_research",
    "needs_more_evidence",
    "needs_human_review",
}
_RESEARCH_BRIEF_EVALUATION_FOLLOWUP_KINDS = {
    "citation_dedupe_repair",
    "citation_provenance_repair",
    "focused_evidence_acquisition",
}


def _latest_research_brief_evaluation(
    repository: ResearchRepository,
    brief: ResearchBriefRecord,
) -> ResearchBriefEvaluationRecord | None:
    evaluations = repository.list_research_brief_evaluations(brief_id=brief.brief_id, limit=1)
    return evaluations[0] if evaluations else None


def _research_brief_matches_followup_filters(
    brief: ResearchBriefRecord,
    request: ResearchBriefFollowupQueueRequest,
) -> bool:
    if request.status is not None and brief.status != request.status:
        return False
    if request.source_key is not None and brief.source_key != request.source_key:
        return False
    if request.topic_query:
        query = request.topic_query.lower()
        if query not in brief.topic.lower() and query not in brief.disease_scope.lower():
            return False
    return True


def _research_brief_followup_leads_from_brief(
    brief: ResearchBriefRecord,
    row: ResearchBriefQualityRow,
    *,
    evaluation: ResearchBriefEvaluationRecord | None = None,
    max_limitations: int,
) -> list[ResearchLeadRecord]:
    if evaluation is not None:
        grouped_feedback = _research_brief_evaluation_followup_groups(evaluation)
        if grouped_feedback:
            leads = []
            for followup_kind, feedback_items in list(grouped_feedback.items())[:max_limitations]:
                digest = hashlib.sha1(
                    "\n".join(item["text"].lower() for item in feedback_items).encode("utf-8")
                ).hexdigest()[:12]
                reason = " ".join(item["text"] for item in feedback_items)[:1000]
                leads.append(
                    ResearchLeadRecord(
                        identity_key=f"research_lead:brief_eval_followup:{brief.brief_id}:{followup_kind}:{digest}",
                        title=_research_brief_evaluation_followup_title(brief, followup_kind),
                        lead_type="unknown",
                        status="followup",
                        priority=_research_brief_evaluation_followup_priority(followup_kind),
                        source_key=brief.source_key,
                        origin_source_key="research_brief_quality",
                        origin_record_id=str(brief.brief_id),
                        origin_agent_run_id=evaluation.agent_run_id or brief.agent_run_id,
                        reason=reason,
                        summary=_research_brief_evaluation_followup_summary(followup_kind),
                        evidence_refs=[
                            f"research_brief:{brief.brief_id}",
                            f"research_brief_evaluation:{evaluation.evaluation_id}",
                        ],
                        suggested_sources=_research_brief_evaluation_followup_sources(followup_kind, brief),
                        topic_tags=[
                            "research_brief",
                            "evaluation_followup",
                            followup_kind,
                            brief.source_key or "",
                            brief.disease_scope,
                        ],
                        metadata={
                            "research_followup_queue": {
                                "origin": "research_brief_evaluation",
                                "brief_id": str(brief.brief_id),
                                "evaluation_id": str(evaluation.evaluation_id),
                                "agent_run_id": str(evaluation.agent_run_id) if evaluation.agent_run_id else None,
                                "topic": brief.topic,
                                "source_key": brief.source_key,
                                "quality_status": row.quality_status,
                                "readiness": evaluation.readiness,
                                "overall_score": evaluation.overall_score,
                                "passes_quality_bar": evaluation.passes_quality_bar,
                                "followup_kind": followup_kind,
                                "feedback_items": feedback_items,
                                "requires_manual_research": False,
                            }
                        },
                    )
                )
            return leads

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


def _research_brief_evaluation_followup_groups(
    evaluation: ResearchBriefEvaluationRecord,
) -> dict[str, list[dict[str, str]]]:
    payload = evaluation.result_payload or {}
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {}
    candidates: list[tuple[str, str]] = []
    candidates.extend(("weakness", item) for item in _as_text_list(payload.get("weaknesses", [])))
    candidates.extend(("recommendation", item) for item in _as_text_list(payload.get("recommendations", [])))
    candidates.extend(("notable_risk", item) for item in _as_text_list(evidence.get("notable_risks", [])))

    grouped: dict[str, list[dict[str, str]]] = {}
    for source, text in candidates:
        followup_kind = _research_brief_evaluation_followup_kind(text)
        if followup_kind not in _RESEARCH_BRIEF_EVALUATION_FOLLOWUP_KINDS:
            continue
        grouped.setdefault(followup_kind, []).append({"source": source, "text": text})
    return grouped


def _research_brief_evaluation_followup_kind(text: str) -> str:
    normalized = text.lower()
    if any(term in normalized for term in ("duplicate", "dedupe", "deduplication")) and "citation" in normalized:
        return "citation_dedupe_repair"
    if any(
        term in normalized
        for term in (
            "provenance",
            "doi",
            "pmid",
            "pmcid",
            "publication year",
            "citation metadata",
            "independent verification",
            "claim verification",
            "recency assessment",
        )
    ):
        return "citation_provenance_repair"
    if (
        any(term in normalized for term in ("toceranib", "vegfr", "vegfr-2", "vegf"))
        and any(
            term in normalized
            for term in (
                "monotherapy",
                "canine splenic",
                "splenic hsa",
                "survival",
                "response",
                "clinical",
                "evidence-acquisition",
                "evidence acquisition",
            )
        )
    ):
        return "focused_evidence_acquisition"
    return "other"


def _research_brief_evaluation_followup_title(
    brief: ResearchBriefRecord,
    followup_kind: str,
) -> str:
    if followup_kind == "citation_dedupe_repair":
        prefix = "Repair duplicate citations"
    elif followup_kind == "citation_provenance_repair":
        prefix = "Strengthen citation provenance"
    elif followup_kind == "focused_evidence_acquisition":
        return "Find toceranib/VEGFR inhibitor monotherapy outcomes in canine splenic HSA"
    else:
        prefix = "Follow up research brief evaluation"
    return f"{prefix}: {brief.topic}"[:240]


def _research_brief_evaluation_followup_summary(followup_kind: str) -> str:
    if followup_kind == "citation_dedupe_repair":
        return (
            "Repair duplicate or inflated citations before downstream coverage scoring. "
            "Confirm unique source objects and citation-to-claim mapping."
        )
    if followup_kind == "citation_provenance_repair":
        return (
            "Strengthen provenance by finding PMID, DOI, PMCID, publication year, and source URLs "
            "for cited evidence before promotion."
        )
    if followup_kind == "focused_evidence_acquisition":
        return (
            "Run focused evidence acquisition for toceranib or VEGFR inhibitor monotherapy clinical "
            "outcomes in canine splenic hemangiosarcoma."
        )
    return "Follow up evaluator feedback before promoting this brief."


def _research_brief_evaluation_followup_priority(followup_kind: str) -> int:
    return {
        "focused_evidence_acquisition": 15,
        "citation_provenance_repair": 20,
        "citation_dedupe_repair": 25,
    }.get(followup_kind, 35)


def _research_brief_evaluation_followup_sources(
    followup_kind: str,
    brief: ResearchBriefRecord,
) -> list[str]:
    if followup_kind == "focused_evidence_acquisition":
        return ["pubmed", "europe_pmc", "openalex", "clinicaltrials_gov", "crossref"]
    if followup_kind in {"citation_dedupe_repair", "citation_provenance_repair"}:
        return _dedupe_source_keys([brief.source_key or "", "pubmed", "crossref", "europe_pmc", "openalex"])
    return _dedupe_source_keys([brief.source_key or ""])


def _dedupe_source_keys(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip().lower()
        if not normalized or normalized in seen:
            continue
        deduped.append(normalized)
        seen.add(normalized)
    return deduped


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


def _research_hunt_synthesis_queue_request_from_lead(
    lead: ResearchLeadRecord,
    lead_row: ResearchHuntLeadQueueRow,
    request: ResearchHuntSynthesisQueueRequest,
) -> ResearchBriefQueueRequest:
    state = lead.metadata.get("research_hunt") if isinstance(lead.metadata, dict) else None
    state = state if isinstance(state, dict) else {}
    best_signal = state.get("best_signal") if isinstance(state.get("best_signal"), dict) else {}
    return ResearchBriefQueueRequest(
        topic=_research_brief_topic_from_lead(lead),
        disease_scope=request.disease_scope,
        source_key=None,
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
            "research_hunt_synthesis_queue": {
                "origin": "research_hunt_ready_lead",
                "lead_id": str(lead.lead_id),
                "lead_type": lead.lead_type,
                "lead_status": lead.status,
                "control_status": lead_row.control_status,
                "recommended_action": lead_row.recommended_action,
                "source_key": lead_row.source_key,
                "origin_source_key": lead.origin_source_key,
                "origin_record_id": lead.origin_record_id,
                "origin_agent_run_id": str(lead.origin_agent_run_id) if lead.origin_agent_run_id else None,
                "signal_status": lead_row.signal_status,
                "coverage_status": lead_row.coverage_status,
                "best_signal_score": lead_row.best_signal_score,
                "open_task_count": lead_row.open_task_count,
                "open_concrete_count": lead_row.open_concrete_count,
                "open_broad_count": lead_row.open_broad_count,
                "open_passive_count": lead_row.open_passive_count,
                "suppressed_task_count": lead_row.suppressed_task_count,
                "completed_task_count": lead_row.completed_task_count,
                "failed_task_count": lead_row.failed_task_count,
                "best_signal": best_signal,
                "reason": lead.reason,
                "evidence_refs": lead.evidence_refs,
                "topic_tags": lead.topic_tags,
                "suggested_sources": lead.suggested_sources,
                "operator": request.operator,
                "dagster_run_id": request.dagster_run_id,
                "dry_run": request.dry_run,
            },
        },
    )


def _research_hunt_synthesis_doc_from_lead(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
    lead_row: ResearchHuntLeadQueueRow,
    request: ResearchHuntSynthesisDocRequest,
) -> tuple[ResearchHuntSynthesisDocument, ArtifactHandle | None]:
    state = lead.metadata.get("research_hunt") if isinstance(lead.metadata, dict) else None
    state = state if isinstance(state, dict) else {}
    best_signal = state.get("best_signal") if isinstance(state.get("best_signal"), dict) else {}
    task_rows = state.get("tasks") if isinstance(state.get("tasks"), list) else []
    suppressed_rows = state.get("suppressed_tasks") if isinstance(state.get("suppressed_tasks"), list) else []
    evidence_refs = _research_hunt_synthesis_doc_evidence_refs(lead, best_signal, task_rows, suppressed_rows)
    chunks, objects = _research_hunt_synthesis_doc_evidence(repository, evidence_refs, request)
    claims = _research_hunt_synthesis_doc_claims(repository, lead, objects, request)
    title = lead.title or lead.summary or lead.reason or "Ready research lead"
    summary = _research_hunt_synthesis_doc_plain_summary(lead, lead_row, best_signal)
    markdown, footnote_count = _render_research_hunt_synthesis_doc_markdown(
        lead=lead,
        lead_row=lead_row,
        title=title,
        plain_summary=summary,
        best_signal=best_signal,
        claims=claims,
        chunks=chunks,
        objects=objects,
        evidence_refs=evidence_refs,
        task_rows=task_rows,
        suppressed_rows=suppressed_rows,
        request=request,
    )
    artifact_id = uuid5(NAMESPACE_URL, f"twog:research-hunt-synthesis-doc:{lead.lead_id}")
    artifact_uri = f"twog://research-hunt/synthesis-doc/{artifact_id}"
    document = ResearchHuntSynthesisDocument(
        lead_id=lead.lead_id,
        title=title,
        control_status=lead_row.control_status,
        recommended_action=lead_row.recommended_action,
        artifact_id=None if request.dry_run else artifact_id,
        artifact_uri=None if request.dry_run else artifact_uri,
        markdown=markdown,
        plain_language_summary=summary,
        claim_count=len(claims),
        chunk_count=len(chunks),
        research_object_count=len(objects),
        evidence_ref_count=len(evidence_refs),
        technical_footnote_count=footnote_count,
        metadata={
            **request.metadata,
            "source_key": lead_row.source_key or lead.source_key,
            "signal_status": lead_row.signal_status,
            "coverage_status": lead_row.coverage_status,
            "best_signal_score": lead_row.best_signal_score,
            "operator": request.operator,
            "dagster_run_id": request.dagster_run_id,
        },
    )
    if request.dry_run:
        return document, None

    artifact = ArtifactHandle(
        artifact_id=artifact_id,
        artifact_type="research_hunt_synthesis_handoff_markdown",
        uri=artifact_uri,
        legal_status="derived_from_local_research_metadata",
        mime_type="text/markdown",
        metadata={
            **document.metadata,
            "lead_id": str(lead.lead_id),
            "title": title,
            "markdown": markdown,
            "plain_language_summary": summary,
            "claim_count": len(claims),
            "chunk_count": len(chunks),
            "research_object_count": len(objects),
            "evidence_ref_count": len(evidence_refs),
            "technical_footnote_count": footnote_count,
            "created_at": document.created_at.isoformat(),
        },
    )
    repository.upsert_artifact(artifact)
    repository.update_research_lead(
        lead.lead_id,
        metadata={
            "research_hunt_synthesis_doc": {
                "artifact_id": str(artifact.artifact_id),
                "artifact_uri": artifact.uri,
                "artifact_type": artifact.artifact_type,
                "operator": request.operator,
                "dagster_run_id": request.dagster_run_id,
                "created_at": document.created_at.isoformat(),
            }
        },
    )
    return document.model_copy(update={"artifact_id": artifact.artifact_id, "artifact_uri": artifact.uri}), artifact


def _research_hunt_synthesis_doc_evidence_refs(
    lead: ResearchLeadRecord,
    best_signal: dict[str, Any],
    task_rows: list[Any],
    suppressed_rows: list[Any],
) -> list[str]:
    refs: list[str] = []
    refs.extend(str(ref) for ref in lead.evidence_refs)
    for value in _nested_strings(best_signal):
        if _looks_like_evidence_ref(value):
            refs.append(value)
    for row in [*task_rows, *suppressed_rows]:
        if isinstance(row, dict):
            for key in ("evidence_refs", "result_evidence_refs", "refs"):
                value = row.get(key)
                if isinstance(value, list):
                    refs.extend(str(item) for item in value if _looks_like_evidence_ref(str(item)))
            for value in _nested_strings(row.get("result") or row.get("metadata") or {}):
                if _looks_like_evidence_ref(value):
                    refs.append(value)
    return _dedupe_preserve_order(refs)


def _research_hunt_synthesis_doc_evidence(
    repository: ResearchRepository,
    evidence_refs: list[str],
    request: ResearchHuntSynthesisDocRequest,
) -> tuple[list[DocumentChunk], list[ResearchObject]]:
    chunk_ids: list[UUID] = []
    object_ids: list[UUID] = []
    for ref in evidence_refs:
        chunk_ids.extend(UUID(match.group(1)) for match in _RESEARCH_HUNT_CHUNK_REF_RE.finditer(ref))
        object_ids.extend(UUID(match.group(1)) for match in _RESEARCH_HUNT_OBJECT_REF_RE.finditer(ref))

    chunks: list[DocumentChunk] = []
    objects_by_id: dict[UUID, ResearchObject] = {}
    for chunk_id in _dedupe_preserve_order(chunk_ids):
        chunk = repository.get_document_chunk(chunk_id)
        if chunk is None:
            continue
        chunks.append(chunk)
        obj = repository.get_research_object(chunk.research_object_id)
        if obj is not None:
            objects_by_id[obj.id] = obj
    for object_id in _dedupe_preserve_order(object_ids):
        obj = repository.get_research_object(object_id)
        if obj is None:
            continue
        objects_by_id[obj.id] = obj
        if len(chunks) < request.max_chunks:
            for chunk in repository.list_document_chunks(object_id=obj.id, limit=max(request.max_chunks - len(chunks), 0)):
                if chunk.id not in {item.id for item in chunks}:
                    chunks.append(chunk)
                if len(chunks) >= request.max_chunks:
                    break

    chunks = chunks[: request.max_chunks]
    objects = list(objects_by_id.values())
    objects.sort(key=lambda obj: (obj.source_key or "", obj.title or "", str(obj.id)))
    return chunks, objects


def _research_hunt_synthesis_doc_claims(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
    objects: list[ResearchObject],
    request: ResearchHuntSynthesisDocRequest,
) -> list[ClaimSearchResult]:
    object_ids = {obj.id for obj in objects}
    list_claims = getattr(repository, "list_claims", None)
    if callable(list_claims):
        claims = list_claims(
            min_confidence=0.0,
            include_seed_claims=False,
            limit=None if object_ids else max(request.max_claims * 50, 250),
        )
    else:
        claims = repository.search_claims(
            ClaimSearchRequest(
                query=None,
                min_confidence=0.0,
                include_drafts=True,
                limit=max(request.max_claims * 4, request.max_claims, 1),
            )
        )
    if object_ids:
        claims = [claim for claim in claims if claim.source_object_id in object_ids]
    if not claims and request.max_claims:
        for query in _research_hunt_doc_query_terms(lead):
            claims.extend(
                repository.search_claims(
                    ClaimSearchRequest(
                        query=query,
                        min_confidence=0.0,
                        include_drafts=True,
                        limit=request.max_claims,
                    )
                )
            )
            if len(claims) >= request.max_claims:
                break
    seen: set[UUID] = set()
    seen_statements: set[str] = set()
    deduped: list[ClaimSearchResult] = []
    terms = set(_research_hunt_doc_query_terms(lead))
    scored_claims = [(claim, _claim_relevance_score(claim, terms)) for claim in claims]
    if terms and any(score > 0 for _, score in scored_claims):
        scored_claims = [(claim, score) for claim, score in scored_claims if score > 0]
    elif terms:
        scored_claims = []
    for claim, _score in sorted(scored_claims, key=lambda item: (-item[1], -item[0].confidence, item[0].statement)):
        statement_key = re.sub(r"\s+", " ", claim.statement.strip().lower())
        if claim.claim_id in seen or statement_key in seen_statements:
            continue
        seen.add(claim.claim_id)
        seen_statements.add(statement_key)
        deduped.append(claim)
    return deduped[: request.max_claims]


def _claim_relevance_score(claim: ClaimSearchResult, terms: set[str]) -> int:
    if not terms:
        return 0
    haystack = " ".join(
        [
            claim.statement,
            claim.source_title or "",
            claim.source_url or "",
            " ".join(entity.canonical_name for entity in claim.entities),
        ]
    ).lower()
    return sum(1 for term in terms if term in haystack)


def _research_hunt_doc_query_terms(lead: ResearchLeadRecord) -> list[str]:
    candidates = [lead.title or "", lead.reason or "", lead.summary or "", *lead.topic_tags]
    useful: list[str] = []
    for candidate in candidates:
        for term in re.findall(r"[A-Za-z0-9]{3,}", candidate):
            normalized = term.strip(".,;:()[]{}").lower()
            if len(normalized) < 4 or normalized in _RESEARCH_HUNT_DOC_STOPWORDS:
                continue
            useful.append(normalized)
    return _dedupe_preserve_order(useful)[:8]


def _research_hunt_synthesis_doc_plain_summary(
    lead: ResearchLeadRecord,
    lead_row: ResearchHuntLeadQueueRow,
    best_signal: dict[str, Any],
) -> str:
    signal_summary = best_signal.get("summary") if isinstance(best_signal.get("summary"), str) else None
    basis = signal_summary or lead.summary or lead.reason or "The research hunt found enough supported evidence to move this topic into synthesis."
    return (
        f"{basis} The system has marked this lead as ready for synthesis because concrete hunt tasks are closed "
        f"and the best current signal score is {lead_row.best_signal_score}/100."
    )


def _render_research_hunt_synthesis_doc_markdown(
    *,
    lead: ResearchLeadRecord,
    lead_row: ResearchHuntLeadQueueRow,
    title: str,
    plain_summary: str,
    best_signal: dict[str, Any],
    claims: list[ClaimSearchResult],
    chunks: list[DocumentChunk],
    objects: list[ResearchObject],
    evidence_refs: list[str],
    task_rows: list[Any],
    suppressed_rows: list[Any],
    request: ResearchHuntSynthesisDocRequest,
) -> tuple[str, int]:
    footnotes: list[str] = []
    lines = [
        f"# {title}",
        "",
        "## Plain-language summary",
        plain_summary,
        "",
        "## What this means",
        (
            "This is a handoff document for synthesis. It means the research hunt found a signal worth briefing, "
            "not that the therapy or idea is proven. The next synthesis pass should compare the strongest direct "
            "evidence against adjacent evidence and decide what still needs validation."
        ),
        "",
        "## What the system found",
    ]
    if claims:
        for index, claim in enumerate(claims[: min(6, len(claims))], start=1):
            marker = _append_footnote(
                footnotes,
                request,
                _claim_technical_footnote(claim, objects),
            )
            lines.append(f"- {claim.statement}{marker}")
            if index >= 6:
                break
    elif best_signal:
        marker = _append_footnote(footnotes, request, f"Best signal payload: {best_signal}")
        lines.append(f"- The strongest available signal is captured in the research-hunt state.{marker}")
    else:
        lines.append("- No extracted claims were attached yet; synthesis should start from the evidence references below.")

    lines.extend(
        [
            "",
            "## What this does not prove yet",
            (
                "- This does not establish clinical efficacy on its own. Synthesis still needs to separate direct canine "
                "hemangiosarcoma evidence from human angiosarcoma, broader canine oncology, and mechanism-only evidence."
            ),
            (
                "- Treatment context matters. Maintenance, combination therapy, in-vitro response, safety, and true "
                "monotherapy outcomes should be labeled separately before any validation plan is proposed."
            ),
            "",
            "## Why it matters",
            (
                "A ready lead is where the system stops hunting broadly and asks the synthesis agents to turn the evidence "
                "into a usable brief: what appears supported, what is weak, what is missing, and what should be tested next."
            ),
            "",
            "## What should happen next",
            "- Queue or run the synthesis brief for this lead.",
            "- Ask the synthesis agents to preserve the distinction between direct evidence and translational evidence.",
            "- Convert missing evidence into focused follow-up tasks instead of leaving vague caveats in the brief.",
            "",
            "## System state",
            f"- Control status: {lead_row.control_status}",
            f"- Recommended action: {lead_row.recommended_action}",
            f"- Signal status: {lead_row.signal_status or 'unknown'}",
            f"- Coverage status: {lead_row.coverage_status or 'unknown'}",
            f"- Best signal score: {lead_row.best_signal_score}/100",
            f"- Completed hunt tasks: {lead_row.completed_task_count}",
            f"- Open concrete hunt tasks: {lead_row.open_concrete_count}",
            f"- Open broad/passive hunt tasks: {lead_row.open_broad_count + lead_row.open_passive_count}",
        ]
    )
    open_tasks = [
        row
        for row in task_rows
        if isinstance(row, dict) and str(row.get("status") or "open").lower() not in {"completed", "done", "suppressed"}
    ]
    if open_tasks:
        lines.append("")
        lines.append("## Remaining watch items")
        for row in open_tasks[:6]:
            action = _humanize_doc_text(str(row.get("action") or row.get("task_type") or "Follow-up task"))
            lines.append(f"- {action[:240]}")
    if suppressed_rows:
        suppressed_actions = [
            _humanize_doc_text(str(row.get("action") or row.get("task_type") or "Suppressed task"))
            for row in suppressed_rows[:5]
            if isinstance(row, dict)
        ]
        marker = _append_footnote(
            footnotes,
            request,
            f"Suppressed hunt tasks retained for audit: count={len(suppressed_rows)}, examples={suppressed_actions}",
        )
        lines.append(f"- Suppressed duplicate or passive tasks are retained for audit.{marker}")

    if request.include_technical_footnotes:
        lines.extend(["", "## Technical footnotes"])
        for obj in objects:
            marker_text = _object_technical_footnote(obj)
            _append_footnote(footnotes, request, marker_text)
        for chunk in chunks:
            obj = next((item for item in objects if item.id == chunk.research_object_id), None)
            _append_footnote(footnotes, request, _chunk_technical_footnote(chunk, obj, request.max_chunk_chars))
        if evidence_refs:
            _append_footnote(footnotes, request, f"Raw evidence refs on the lead/state: {evidence_refs[:25]}")
        if not footnotes:
            lines.append("No technical footnotes were available for this handoff.")
        else:
            for index, footnote in enumerate(footnotes, start=1):
                lines.append(f"[T{index}] {footnote}")

    return "\n".join(lines).strip() + "\n", len(footnotes)


def _append_footnote(footnotes: list[str], request: ResearchHuntSynthesisDocRequest, text: str) -> str:
    if not request.include_technical_footnotes or len(footnotes) >= request.max_technical_footnotes:
        return ""
    normalized = _truncate(text, 1800)
    if normalized in footnotes:
        return f" [T{footnotes.index(normalized) + 1}]"
    footnotes.append(normalized)
    return f" [T{len(footnotes)}]"


def _claim_technical_footnote(claim: ClaimSearchResult, objects: list[ResearchObject]) -> str:
    obj = next((item for item in objects if item.id == claim.source_object_id), None)
    source = _object_source_label(obj) if obj is not None else claim.source_title or claim.source_url or str(claim.source_object_id or "unknown source")
    return (
        f"Claim {claim.claim_id}: type={claim.claim_type}, direction={claim.direction}, "
        f"confidence={claim.confidence:.2f}, evidence_level={claim.evidence_level}, source={source}."
    )


def _object_technical_footnote(obj: ResearchObject) -> str:
    return f"Research object {obj.id}: {_object_source_label(obj)}."


def _chunk_technical_footnote(chunk: DocumentChunk, obj: ResearchObject | None, max_chars: int) -> str:
    preview = _truncate(chunk.text_content.replace("\n", " "), max_chars)
    source = _object_source_label(obj) if obj is not None else str(chunk.research_object_id)
    section = f", section={chunk.section_label}" if chunk.section_label else ""
    return f"Chunk {chunk.id}: source={source}, index={chunk.chunk_index}{section}, preview={preview}"


def _object_source_label(obj: ResearchObject | None) -> str:
    if obj is None:
        return "unknown source"
    ids = []
    for key in ("pmid", "pmcid", "doi", "nct_id"):
        value = obj.identifiers.get(key) if isinstance(obj.identifiers, dict) else None
        if value:
            ids.append(f"{key.upper()} {value}")
    id_text = f" ({'; '.join(ids)})" if ids else ""
    title = obj.title or obj.canonical_url or str(obj.id)
    source = f", source={obj.source_key}" if obj.source_key else ""
    return f"{title}{id_text}{source}"


def _humanize_doc_text(value: str) -> str:
    text = value.replace("_", " ")
    text = re.sub(r"\s+", " ", text)
    text = text.replace("( e.g.,", "(e.g.,")
    return text.strip()


def _nested_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        strings: list[str] = []
        for item in value.values():
            strings.extend(_nested_strings(item))
        return strings
    if isinstance(value, list | tuple | set):
        strings = []
        for item in value:
            strings.extend(_nested_strings(item))
        return strings
    return []


def _looks_like_evidence_ref(value: str) -> bool:
    return any(
        token in value.lower()
        for token in ("chunk:", "research_object:", "pmid", "pmcid", "doi:", "nct")
    )


def _dedupe_preserve_order(values: list[Any]) -> list[Any]:
    deduped: list[Any] = []
    seen: set[Any] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


_RESEARCH_HUNT_DOC_STOPWORDS = {
    "about",
    "after",
    "against",
    "and",
    "angiosarcoma",
    "canine",
    "clinical",
    "data",
    "evidence",
    "find",
    "found",
    "from",
    "hemangiosarcoma",
    "hsa",
    "human",
    "inhibitor",
    "inhibitors",
    "into",
    "lead",
    "monotherapy",
    "outcomes",
    "research",
    "review",
    "search",
    "signal",
    "splenic",
    "study",
    "therapy",
    "with",
}


_RESEARCH_HUNT_CHUNK_REF_RE = re.compile(r"chunk:([0-9a-fA-F-]{36})")
_RESEARCH_HUNT_OBJECT_REF_RE = re.compile(r"research_object:([0-9a-fA-F-]{36})")
_RESEARCH_HUNT_PMID_RE = re.compile(r"\bpmid[:\s]*(\d+)\b", re.IGNORECASE)
_RESEARCH_HUNT_PMCID_RE = re.compile(r"\bpmcid[:\s]*(PMC\d+)\b", re.IGNORECASE)
_RESEARCH_HUNT_DOI_RE = re.compile(r"\bdoi[:\s]*(10\.[^\s,;)\]]+)", re.IGNORECASE)
_RESEARCH_HUNT_NCT_RE = re.compile(r"\b(NCT\d{8})\b", re.IGNORECASE)

_RESEARCH_HUNT_BROAD_TASK_TYPES = {"broaden_query", "add_source", "research_followup", "citation_chase"}
_RESEARCH_HUNT_CONCRETE_TASK_TYPES = {
    "source_followup_ingest",
    "claim_extract",
    "safety_extract",
    "full_text_extract",
    "subgroup_extract",
}
_RESEARCH_HUNT_PASSIVE_ACTION_RE = re.compile(
    r"\b("
    r"monitor|watch|when new|future publication|new publications|manual review|human review|"
    r"document caveat|document as caveat|track future|periodic|revisit later|keep observing"
    r")\b",
    re.IGNORECASE,
)
_RESEARCH_HUNT_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "can",
    "check",
    "complete",
    "consider",
    "data",
    "for",
    "from",
    "if",
    "in",
    "into",
    "investigate",
    "more",
    "new",
    "of",
    "on",
    "or",
    "review",
    "run",
    "search",
    "specific",
    "the",
    "to",
    "verify",
    "whether",
    "with",
}


def _select_research_hunt_tasks(
    repository,
    request: ResearchHuntTaskRunRequest,
) -> list[tuple[ResearchLeadRecord, dict[str, Any]]]:
    task_id_filter = {str(task_id) for task_id in request.task_ids}
    task_type_filter = {task_type.casefold() for task_type in request.task_types}
    status_filter = {status.casefold() for status in request.statuses}
    lead_id_filter = set(request.lead_ids)
    explicit_task_filter = bool(task_id_filter or task_type_filter)
    if lead_id_filter:
        leads = [
            lead
            for lead_id in lead_id_filter
            if (lead := repository.get_research_lead(lead_id)) is not None
        ]
    else:
        leads = repository.list_research_leads(statuses=["watching", "followup", "new", "queued"], limit=None)

    selected: list[tuple[ResearchLeadRecord, dict[str, Any]]] = []
    for lead in leads:
        state = lead.metadata.get("research_hunt") if isinstance(lead.metadata, dict) else None
        if not isinstance(state, dict):
            continue
        for task in state.get("tasks", []):
            if not isinstance(task, dict):
                continue
            task_id = str(task.get("task_id") or "")
            task_type = str(task.get("task_type") or "research_followup").casefold()
            status = str(task.get("status") or "").casefold()
            if task_id_filter and task_id not in task_id_filter:
                continue
            if task_type_filter and task_type not in task_type_filter:
                continue
            if status_filter and status not in status_filter:
                continue
            action = str(task.get("action") or "")
            if _research_hunt_is_passive_action(action) and not task_id_filter:
                continue
            if (
                _research_hunt_is_broad_task_type(task_type)
                and not request.include_broad_tasks
                and not explicit_task_filter
            ):
                continue
            selected.append((lead, dict(task)))
    return sorted(
        selected,
        key=lambda item: (
            int(item[1].get("priority") or 1000),
            str(item[1].get("created_at") or ""),
        ),
    )


def _research_hunt_task_evidence_chunks(
    repository,
    lead: ResearchLeadRecord,
    task: dict[str, Any],
    *,
    limit: int,
) -> list[DocumentChunk]:
    refs = _research_hunt_task_evidence_refs(lead, task)
    chunks: list[DocumentChunk] = []
    seen: set[UUID] = set()
    for ref in refs:
        chunk_match = _RESEARCH_HUNT_CHUNK_REF_RE.search(ref)
        if chunk_match:
            chunk_id = UUID(chunk_match.group(1))
            chunk = repository.get_document_chunk(chunk_id)
            if chunk is not None and chunk.id not in seen:
                seen.add(chunk.id)
                chunks.append(chunk)
        object_match = _RESEARCH_HUNT_OBJECT_REF_RE.search(ref)
        if object_match:
            object_id = UUID(object_match.group(1))
            for chunk in repository.list_document_chunks(object_id=object_id, limit=limit):
                if chunk.id in seen:
                    continue
                seen.add(chunk.id)
                chunks.append(chunk)
                if len(chunks) >= limit:
                    return chunks
        if len(chunks) >= limit:
            break
    return chunks[:limit]


def _research_hunt_task_queue_row(
    lead: ResearchLeadRecord,
    task: dict[str, Any],
    *,
    stale_after_hours: int,
) -> ResearchHuntTaskQueueRow:
    task_type = str(task.get("task_type") or "research_followup")
    action = str(task.get("action") or "")
    status = str(task.get("status") or "open")
    task_class = _research_hunt_task_class(task_type, action)
    age_hours = _research_hunt_task_age_hours(task)
    stale = bool(status == "open" and age_hours is not None and age_hours >= stale_after_hours)
    runnable_by_default = bool(status == "open" and task_class == "concrete")
    return ResearchHuntTaskQueueRow(
        lead_id=lead.lead_id,
        task_id=str(task.get("task_id") or "") or None,
        task_type=task_type,
        task_class=task_class,  # type: ignore[arg-type]
        status=status,
        priority=int(task["priority"]) if str(task.get("priority") or "").isdigit() else None,
        action=action,
        reason=str(task.get("reason") or "") or None,
        identity_key=str(task.get("identity_key") or "") or _research_hunt_task_identity_key(task_type, action),
        family_key=str(task.get("family_key") or "") or _research_hunt_task_family_key(task_type, action),
        suppression_reason=str(task.get("suppression_reason") or "") or None,
        age_hours=age_hours,
        stale=stale,
        runnable_by_default=runnable_by_default,
        recommended_action=_research_hunt_task_recommended_action(
            status=status,
            task_class=task_class,
            stale=stale,
        ),
    )


def _research_hunt_task_class(task_type: str, action: str) -> str:
    if _research_hunt_is_passive_action(action):
        return "passive"
    if _research_hunt_is_concrete_task_type(task_type):
        return "concrete"
    if _research_hunt_is_broad_task_type(task_type):
        return "broad"
    return "unknown"


def _research_hunt_task_age_hours(task: dict[str, Any]) -> float | None:
    raw_timestamp = task.get("created_at") or task.get("updated_at")
    if not raw_timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw_timestamp).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    age = datetime.now(UTC) - parsed.astimezone(UTC)
    return round(max(0.0, age.total_seconds() / 3600), 2)


def _research_hunt_task_recommended_action(
    *,
    status: str,
    task_class: str,
    stale: bool,
) -> str:
    if status == "suppressed":
        return "keep_suppressed"
    if status == "failed":
        return "inspect_failed_task"
    if status != "open":
        return "no_action"
    if stale and task_class in {"broad", "passive"}:
        return "suppress_or_archive"
    if task_class == "concrete":
        return "run_research_hunt_tasks"
    if task_class == "broad":
        return "explicit_review_required"
    if task_class == "passive":
        return "convert_to_monitoring_note"
    return "inspect_task"


def _research_hunt_task_source_keys(
    lead: ResearchLeadRecord,
    task: dict[str, Any],
    request: ResearchHuntTaskRunRequest,
) -> list[str]:
    if request.source_keys:
        return request.source_keys

    action = _research_hunt_normalize_action_text(str(task.get("action") or ""))
    source_keys: list[str] = []
    source_aliases = [
        ("clinicaltrials_gov", ("clinicaltrials.gov", "clinicaltrials", "clinical trial", "nct")),
        ("europe_pmc", ("europe_pmc", "europe pmc", "europepmc")),
        ("pubmed", ("pubmed", "pmid")),
        ("pmc_oa", ("pmc_oa", "pmcid", "full_text")),
        ("unpaywall", ("unpaywall", "open access")),
        ("crossref", ("crossref", "doi")),
        ("openalex", ("openalex",)),
    ]
    for source_key, aliases in source_aliases:
        if any(alias in action for alias in aliases):
            source_keys.append(source_key)

    if "full_text" in action:
        source_keys.extend(["pubmed", "europe_pmc", "pmc_oa", "unpaywall", "crossref"])
    if not source_keys and isinstance(task.get("source_keys"), list):
        source_keys.extend(str(source).strip().lower() for source in task.get("source_keys", []) if str(source).strip())
    if not source_keys:
        source_keys.extend(lead.suggested_sources)
    return _dedupe_sequence(source_keys)


def _research_hunt_task_search_query(
    lead: ResearchLeadRecord,
    task: dict[str, Any],
) -> str | None:
    action = str(task.get("action") or "")
    refs = _research_hunt_action_identifier_refs(action)
    if refs:
        return " ".join(ref.partition(":")[2] for ref in refs if ref.partition(":")[2])

    context = _research_hunt_normalize_action_text(
        " ".join(
            [
                action,
                lead.title or "",
                lead.summary or "",
                lead.reason or "",
                " ".join(lead.topic_tags),
            ]
        )
    )
    priority_terms = [
        ("toceranib", ("toceranib", "palladia")),
        ("vegfr", ("vegfr", "vegfr2", "kdr")),
        ("inhibitor", ("inhibitor", "inhibitors", "inhibition")),
        ("monotherapy", ("monotherapy",)),
        ("maintenance", ("maintenance",)),
        ("doxorubicin", ("doxorubicin",)),
        ("canine", ("canine", "dog", "dogs", "veterinary")),
        ("splenic", ("splenic", "spleen")),
        ("hemangiosarcoma", ("hemangiosarcoma", "hsa")),
        ("angiosarcoma", ("angiosarcoma",)),
        ("clinical", ("clinical",)),
        ("trial", ("trial", "trials")),
        ("response", ("response", "responses")),
        ("survival", ("survival", "progression-free")),
        ("safety", ("safety",)),
        ("toxicity", ("toxicity", "toxicities", "hepatotoxicity")),
        ("adverse", ("adverse",)),
    ]
    terms: list[str] = []
    for canonical, aliases in priority_terms:
        if any(alias in context for alias in aliases):
            terms.append(canonical)
    if not terms:
        tokens = [
            token
            for token in re.findall(r"[a-z0-9]+", context)
            if len(token) > 2 and token not in _RESEARCH_HUNT_STOPWORDS
        ][:12]
        terms.extend(tokens)
    return " ".join(_dedupe_sequence(terms)) or None


def _research_hunt_source_followup_items_for_task(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
    task: dict[str, Any],
    request: ResearchHuntTaskRunRequest,
) -> list[SourceFollowupQueueItem]:
    items: list[SourceFollowupQueueItem] = []
    action = str(task.get("action") or "")
    for ref in _research_hunt_action_identifier_refs(action):
        identifier_type, _, identifier = ref.partition(":")
        items.extend(
            _research_hunt_source_followup_items_for_identifier(
                lead,
                identifier_type=identifier_type,
                identifier=identifier,
                task=task,
                request=request,
            )
        )

    object_ids: set[UUID] = set()
    for ref in _research_hunt_task_evidence_refs(lead, task):
        ref_type, _, ref_id = ref.partition(":")
        if ref_type not in {"chunk", "research_object"}:
            continue
        try:
            parsed_id = UUID(ref_id)
        except ValueError:
            continue
        if ref_type == "chunk":
            chunk = repository.get_document_chunk(parsed_id)
            if chunk is not None:
                object_ids.add(chunk.research_object_id)
        elif ref_type == "research_object":
            object_ids.add(parsed_id)

    loop_request = ResearchFollowupLoopRequest(
        lead_id=lead.lead_id,
        followup_lane="research_hunt_source_followup",
        max_identifier_followups=min(max(1, request.claim_chunk_limit), 50),
        operator=request.operator,
        metadata=request.metadata,
    )
    for object_id in object_ids:
        obj = repository.get_research_object(object_id)
        if obj is None:
            continue
        items.extend(_source_followup_items_for_research_object(lead, obj, loop_request))

    deduped: list[SourceFollowupQueueItem] = []
    seen: set[str] = set()
    for item in items:
        if not item.identity_key or item.identity_key in seen:
            continue
        seen.add(item.identity_key)
        deduped.append(item)
    return deduped[: request.claim_chunk_limit]


def _research_hunt_source_followup_items_for_identifier(
    lead: ResearchLeadRecord,
    *,
    identifier_type: str,
    identifier: str,
    task: dict[str, Any],
    request: ResearchHuntTaskRunRequest,
) -> list[SourceFollowupQueueItem]:
    identifier_type = identifier_type.strip().lower()
    identifier = identifier.strip()
    if not identifier:
        return []
    source_keys_by_identifier = {
        "doi": ["unpaywall", "crossref"],
        "pmid": ["pubmed"],
        "pmcid": ["pmc_oa"],
        "nct": ["clinicaltrials_gov"],
    }
    source_keys = source_keys_by_identifier.get(identifier_type, [])
    items: list[SourceFollowupQueueItem] = []
    for source_key in source_keys:
        items.append(
            SourceFollowupQueueItem(
                source_key=source_key,
                identifier_type=identifier_type,  # type: ignore[arg-type]
                identifier=identifier,
                url=_source_followup_url(identifier_type, identifier, fallback=lead.url),
                title=lead.title,
                origin_source_key=lead.origin_source_key or lead.source_key,
                origin_review_id=lead.origin_review_id,
                origin_artifact_id=lead.origin_artifact_id,
                origin_agent_run_id=lead.origin_agent_run_id,
                reason="Research hunt task requested source follow-up ingestion.",
                priority=10,
                metadata={
                    "research_hunt_task_executor": True,
                    "research_hunt_task_id": str(task.get("task_id") or ""),
                    "research_lead_id": str(lead.lead_id),
                    "operator": request.operator,
                },
            )
        )
    return items


def _research_hunt_task_status_rank(status: str) -> int:
    return {
        "open": 0,
        "failed": 1,
        "suppressed": 2,
        "completed": 3,
    }.get(status, 4)


def _research_hunt_lead_control_status(
    *,
    has_hunt_state: bool,
    best_signal_score: int,
    open_concrete_count: int,
    open_broad_count: int,
    open_passive_count: int,
    failed_count: int,
) -> str:
    if not has_hunt_state:
        return "no_hunt_state"
    if failed_count and not open_concrete_count and best_signal_score < 80:
        return "blocked"
    if open_concrete_count:
        return "hunting"
    if best_signal_score >= 80:
        return "ready_for_synthesis"
    if open_broad_count or open_passive_count:
        return "watching"
    return "idle"


def _research_hunt_control_status_from_state(
    tasks: list[dict[str, Any]],
    *,
    best_signal: dict[str, Any] | None,
    has_hunt_state: bool,
) -> str:
    rows = [
        _research_hunt_task_queue_row(
            ResearchLeadRecord(title="control-status-placeholder"),
            task,
            stale_after_hours=72,
        )
        for task in tasks
    ]
    open_rows = [row for row in rows if row.status == "open"]
    failed_rows = [row for row in rows if row.status == "failed"]
    return _research_hunt_lead_control_status(
        has_hunt_state=has_hunt_state,
        best_signal_score=_research_hunt_signal_score(best_signal),
        open_concrete_count=sum(1 for row in open_rows if row.task_class == "concrete"),
        open_broad_count=sum(1 for row in open_rows if row.task_class == "broad"),
        open_passive_count=sum(1 for row in open_rows if row.task_class == "passive"),
        failed_count=len(failed_rows),
    )


def _research_hunt_coverage_status_from_control(control_status: str, *, signal_status: str) -> str:
    if control_status == "hunting":
        return "hunting"
    if control_status in {"ready_for_synthesis", "watching"} and signal_status == "supported":
        return "supported"
    if control_status == "blocked":
        return "blocked"
    return "insufficient"


def _research_hunt_lead_recommended_action(control_status: str) -> str:
    return {
        "ready_for_synthesis": "queue_synthesis",
        "hunting": "run_concrete_hunt_tasks",
        "watching": "review_optional_broad_tasks",
        "blocked": "inspect_blocked_hunt",
        "idle": "no_action",
        "no_hunt_state": "no_hunt_state",
    }.get(control_status, "inspect_lead")


def _research_hunt_maintenance_suppression_reason(
    task: ResearchHuntTaskQueueRow,
    *,
    seen_open_family_keys: set[tuple[UUID, str, str]],
    allowed_reasons: set[str],
) -> str | None:
    family_key = task.family_key or task.identity_key or task.action
    family_scope = (task.lead_id, task.task_type, family_key)
    duplicate_broad = task.task_class == "broad" and family_scope in seen_open_family_keys
    if task.status == "open" and task.task_class == "broad":
        seen_open_family_keys.add(family_scope)
    if task.status != "open":
        return None
    if task.task_class == "passive" and "passive_monitoring_note" in allowed_reasons:
        return "passive_monitoring_note"
    if duplicate_broad and "duplicate_broad_family" in allowed_reasons:
        return "duplicate_broad_family"
    if (
        task.stale
        and task.task_class in {"broad", "passive"}
        and "stale_broad_or_passive" in allowed_reasons
    ):
        return "stale_broad_or_passive"
    return None


def _research_hunt_task_evidence_refs(lead: ResearchLeadRecord, task: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    for value in [task.get("action"), task.get("reason")]:
        if isinstance(value, str):
            refs.extend(match.group(0) for match in _RESEARCH_HUNT_CHUNK_REF_RE.finditer(value))
            refs.extend(match.group(0) for match in _RESEARCH_HUNT_OBJECT_REF_RE.finditer(value))
    raw_refs = task.get("evidence_refs")
    if isinstance(raw_refs, list):
        refs.extend(str(ref) for ref in raw_refs if str(ref).strip())
    state = lead.metadata.get("research_hunt") if isinstance(lead.metadata, dict) else None
    if isinstance(state, dict):
        best_signal = state.get("best_signal") if isinstance(state.get("best_signal"), dict) else None
        raw_signal_refs = best_signal.get("evidence_refs") if isinstance(best_signal, dict) else None
        if isinstance(raw_signal_refs, list):
            refs.extend(str(ref) for ref in raw_signal_refs if str(ref).strip())
    deduped: list[str] = []
    seen: set[str] = set()
    for ref in refs:
        normalized = ref.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _evaluation_followup_actions(result: AgentPerformanceEvaluationResult) -> list[str]:
    actions: list[str] = []
    for evaluation in result.evaluations:
        raw_actions = evaluation.get("recommended_followup_actions", [])
        if isinstance(raw_actions, list):
            actions.extend(str(action).strip() for action in raw_actions if str(action).strip())
    deduped: list[str] = []
    seen: set[str] = set()
    for action in actions:
        key = action.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(action)
    return deduped


def _research_hunt_signal_candidate(
    result: ResearchFollowupLoopResult,
    *,
    verdict: str | None,
) -> dict[str, Any] | None:
    evidence_fit = _research_hunt_best_evidence_fit_payload(result)
    if evidence_fit is None:
        return None
    if evidence_fit.get("fit") == "weak" and result.claims_written == 0 and verdict != "useful":
        return None
    evidence_refs: list[str] = []
    if result.resolver_result and result.resolver_result.lead_results:
        evidence_refs = result.resolver_result.lead_results[0].evidence_refs[:25]
    signal = {
        "captured_at": datetime.now(UTC).isoformat(),
        "verdict": verdict,
        "evidence_fit": evidence_fit,
        "score": 0,
        "document_chunks": result.document_chunks,
        "claim_chunks_seen": result.claim_chunks_seen,
        "claims_written": result.claims_written,
        "source_followups_ingested": result.source_followups_ingested,
        "source_followups_ingested_this_run": result.source_followups_ingested_this_run,
        "source_followup_document_chunks": result.source_followup_document_chunks,
        "resolver_agent_run_id": str(result.resolver_agent_run_id) if result.resolver_agent_run_id else None,
        "evaluator_agent_run_id": str(result.evaluator_agent_run_id) if result.evaluator_agent_run_id else None,
        "evidence_refs": evidence_refs,
    }
    signal["score"] = _research_hunt_signal_score(signal)
    return signal


def _research_hunt_best_evidence_fit_payload(result: ResearchFollowupLoopResult) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    if result.evidence_fit is not None:
        candidates.append(result.evidence_fit.model_dump(mode="json"))
    if result.resolver_result:
        for lead_result in result.resolver_result.lead_results:
            raw = lead_result.metadata.get("evidence_fit") if isinstance(lead_result.metadata, dict) else None
            if isinstance(raw, dict):
                candidates.append(raw)
    if not candidates:
        return None
    return max(candidates, key=_research_hunt_evidence_fit_score)


def _research_hunt_evidence_fit_score(evidence_fit: dict[str, Any]) -> int:
    fit_score = {"weak": 0, "partial": 40, "strong": 75}.get(str(evidence_fit.get("fit") or "weak"), 0)
    matched = int(evidence_fit.get("matched_required_count") or 0)
    total = int(evidence_fit.get("total_required_count") or 0)
    term_score = round(10 * matched / total) if total else 0
    dimension_score = sum(
        5
        for key in ("target_safety_fit", "disease_directness_fit", "actionability")
        if evidence_fit.get(key) == "strong"
    )
    return max(0, min(100, fit_score + term_score + dimension_score))


def _research_hunt_signal_score(signal: dict[str, Any] | None) -> int:
    if not signal:
        return 0
    evidence_fit = signal.get("evidence_fit") if isinstance(signal.get("evidence_fit"), dict) else {}
    fit_score = {"weak": 0, "partial": 40, "strong": 75}.get(str(evidence_fit.get("fit") or "weak"), 0)
    matched = int(evidence_fit.get("matched_required_count") or 0)
    total = int(evidence_fit.get("total_required_count") or 0)
    term_score = round(10 * matched / total) if total else 0
    claim_score = min(10, int(signal.get("claims_written") or 0) // 25)
    verdict_score = {"useful": 15, "needs_followup": 5, "unclear": 2, "bad": -15}.get(
        str(signal.get("verdict") or ""),
        0,
    )
    return max(0, min(100, fit_score + term_score + claim_score + verdict_score))


def _research_hunt_has_supported_signal(result: ResearchFollowupLoopResult) -> bool:
    return _research_hunt_signal_score(result.best_signal) >= 80 or result.signal_status == "supported"


def _research_hunt_state_has_supported_signal(state: dict[str, Any]) -> bool:
    best_signal = state.get("best_signal") if isinstance(state.get("best_signal"), dict) else None
    return _research_hunt_signal_score(best_signal) >= 80 or state.get("signal_status") == "supported"


def _research_hunt_tasks_from_result(
    lead: ResearchLeadRecord,
    result: ResearchFollowupLoopResult,
    request: ResearchFollowupLoopRequest,
    *,
    verdict: str | None,
    followup_actions: list[str],
    has_best_signal: bool,
    existing_tasks: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    created_at = datetime.now(UTC).isoformat()
    raw_tasks: list[tuple[str, str, str]] = []
    for action in followup_actions:
        raw_tasks.append((_research_hunt_task_type(action), action, "evaluator_followup_action"))
    if has_best_signal and result.evidence_fit and result.evidence_fit.fit == "weak" and result.document_chunks == 0:
        raw_tasks.append(
            (
                "broaden_query",
                "Broaden source coverage or query terms after a no-result follow-up run.",
                "no_result_after_supported_signal",
            )
        )
    if result.source_followups_pending:
        raw_tasks.append(
            (
                "source_followup_ingest",
                f"Process {result.source_followups_pending} pending source follow-up identifier(s).",
                "pending_source_followups",
            )
        )
    if result.document_chunks and request.run_claim_extraction is False:
        raw_tasks.append(
            (
                "claim_extract",
                "Run claim extraction on the retrieved follow-up chunks.",
                "chunks_without_claim_extraction",
            )
        )
    existing_keys: set[str] = {
        str(task.get("identity_key") or "")
        for task in existing_tasks
        if str(task.get("identity_key") or "")
    }
    existing_family_counts = Counter(
        str(task.get("family_key") or _research_hunt_task_family_key(
            str(task.get("task_type") or "research_followup"),
            str(task.get("action") or ""),
        ))
        for task in existing_tasks
        if str(task.get("family_key") or task.get("action") or "")
    )
    parent_task_id = str(request.metadata.get("research_hunt_task_id") or "")
    parent_task_type = str(request.metadata.get("research_hunt_parent_task_type") or "")
    if parent_task_id and not parent_task_type:
        for task in existing_tasks:
            if str(task.get("task_id") or "") == parent_task_id:
                parent_task_type = str(task.get("task_type") or "")
                break
    parent_is_broad = _research_hunt_is_broad_task_type(parent_task_type)
    allow_broad_fanout = bool(request.metadata.get("research_hunt_allow_broad_task_fanout") is True)
    no_new_evidence = _research_hunt_loop_has_no_new_evidence(result)
    tasks: list[dict[str, Any]] = []
    suppressed_tasks: list[dict[str, Any]] = []
    for task_type, action, reason in raw_tasks:
        task_type = str(task_type or "research_followup")
        action = str(action)
        identity_key = _research_hunt_task_identity_key(task_type, action)
        family_key = _research_hunt_task_family_key(task_type, action)
        suppression_reason: str | None = None
        if _research_hunt_is_passive_action(action):
            suppression_reason = "passive_or_monitoring_guidance"
        elif identity_key in existing_keys:
            suppression_reason = "duplicate_existing_task"
        elif (
            _research_hunt_is_broad_task_type(task_type)
            and parent_is_broad
            and no_new_evidence
            and not allow_broad_fanout
        ):
            suppression_reason = "broad_child_fanout_without_new_evidence"
        elif _research_hunt_is_broad_task_type(task_type) and existing_family_counts[family_key] >= 1:
            suppression_reason = "broad_family_already_seen"
        if suppression_reason:
            suppressed_tasks.append(
                _research_hunt_suppressed_task_payload(
                    lead,
                    task_type=task_type,
                    action=action,
                    reason=reason,
                    suppression_reason=suppression_reason,
                    identity_key=identity_key,
                    family_key=family_key,
                    created_at=created_at,
                    result=result,
                )
            )
            existing_keys.add(identity_key)
            existing_family_counts[family_key] += 1
            continue
        if identity_key in existing_keys:
            continue
        existing_keys.add(identity_key)
        existing_family_counts[family_key] += 1
        task_id = uuid5(NAMESPACE_URL, f"research_hunt_task:{lead.lead_id}:{identity_key}")
        tasks.append(
            {
                "task_id": str(task_id),
                "identity_key": identity_key,
                "family_key": family_key,
                "status": "open",
                "task_type": task_type,
                "action": str(action),
                "reason": reason,
                "priority": _research_hunt_task_priority(task_type, verdict),
                "lead_id": str(lead.lead_id),
                "origin": "research_followup_loop",
                "source_keys": request.source_keys or lead.suggested_sources,
                "query_names": request.query_names,
                "created_at": created_at,
                "updated_at": created_at,
                "resolver_agent_run_id": str(result.resolver_agent_run_id) if result.resolver_agent_run_id else None,
                "evaluator_agent_run_id": str(result.evaluator_agent_run_id) if result.evaluator_agent_run_id else None,
            }
        )
    return tasks, suppressed_tasks


def _research_hunt_suppressed_task_payload(
    lead: ResearchLeadRecord,
    *,
    task_type: str,
    action: str,
    reason: str,
    suppression_reason: str,
    identity_key: str,
    family_key: str,
    created_at: str,
    result: ResearchFollowupLoopResult,
) -> dict[str, Any]:
    task_id = uuid5(NAMESPACE_URL, f"research_hunt_task_suppressed:{lead.lead_id}:{identity_key}:{suppression_reason}")
    return {
        "task_id": str(task_id),
        "identity_key": identity_key,
        "family_key": family_key,
        "status": "suppressed",
        "task_type": task_type,
        "action": action,
        "reason": reason,
        "suppression_reason": suppression_reason,
        "lead_id": str(lead.lead_id),
        "origin": "research_followup_loop",
        "created_at": created_at,
        "updated_at": created_at,
        "resolver_agent_run_id": str(result.resolver_agent_run_id) if result.resolver_agent_run_id else None,
        "evaluator_agent_run_id": str(result.evaluator_agent_run_id) if result.evaluator_agent_run_id else None,
    }


def _research_hunt_loop_has_no_new_evidence(result: ResearchFollowupLoopResult) -> bool:
    return (
        result.raw_records == 0
        and result.research_objects == 0
        and result.document_chunks == 0
        and result.source_followups_ingested_this_run == 0
        and result.source_followup_document_chunks == 0
        and result.claims_written == 0
    )


def _research_hunt_is_broad_task_type(task_type: str) -> bool:
    normalized = str(task_type or "").casefold()
    return normalized in _RESEARCH_HUNT_BROAD_TASK_TYPES


def _research_hunt_is_concrete_task_type(task_type: str) -> bool:
    normalized = str(task_type or "").casefold()
    return normalized in _RESEARCH_HUNT_CONCRETE_TASK_TYPES


def _research_hunt_is_passive_action(action: str) -> bool:
    normalized = " ".join(str(action or "").replace("_", " ").split()).casefold()
    return bool(_RESEARCH_HUNT_PASSIVE_ACTION_RE.search(normalized))


def _research_hunt_task_identity_key(task_type: str, action: str) -> str:
    refs = _research_hunt_action_identifier_refs(action)
    if refs:
        return f"{task_type}:{'|'.join(refs)[:260]}"
    if not _research_hunt_is_broad_task_type(task_type):
        normalized = _research_hunt_normalize_action_text(action)
        tokens = [
            token
            for token in re.findall(r"[a-z0-9]+", normalized)
            if len(token) > 1 and token not in _RESEARCH_HUNT_STOPWORDS
        ][:24]
        return f"{task_type}:{'-'.join(tokens)[:260] or 'general'}"
    return f"{task_type}:{_research_hunt_task_family_key(task_type, action)[:260]}"


def _research_hunt_task_family_key(task_type: str, action: str) -> str:
    normalized = _research_hunt_normalize_action_text(action)
    refs = _research_hunt_action_identifier_refs(action)
    terms = _research_hunt_family_terms(normalized)
    if refs:
        terms.extend(refs)
    if not terms:
        terms = [
            token
            for token in re.findall(r"[a-z0-9]+", normalized)
            if len(token) > 2 and token not in _RESEARCH_HUNT_STOPWORDS
        ][:12]
    family = "|".join(_dedupe_sequence(terms)) or "general"
    broadness = "broad" if _research_hunt_is_broad_task_type(task_type) else "concrete"
    return f"{broadness}:{task_type}:{family}"


def _research_hunt_action_identifier_refs(action: str) -> list[str]:
    refs: list[str] = []
    refs.extend(f"chunk:{match.group(1).lower()}" for match in _RESEARCH_HUNT_CHUNK_REF_RE.finditer(action))
    refs.extend(f"research_object:{match.group(1).lower()}" for match in _RESEARCH_HUNT_OBJECT_REF_RE.finditer(action))
    refs.extend(f"pmid:{match.group(1)}" for match in _RESEARCH_HUNT_PMID_RE.finditer(action))
    refs.extend(f"pmcid:{match.group(1).upper()}" for match in _RESEARCH_HUNT_PMCID_RE.finditer(action))
    refs.extend(f"doi:{match.group(1).rstrip('.').casefold()}" for match in _RESEARCH_HUNT_DOI_RE.finditer(action))
    refs.extend(f"nct:{match.group(1).upper()}" for match in _RESEARCH_HUNT_NCT_RE.finditer(action))
    return _dedupe_sequence(refs)


def _research_hunt_normalize_action_text(action: str) -> str:
    normalized = str(action or "").replace("_", " ").casefold()
    replacements = {
        "vegfr-2": "vegfr2",
        "vegfr 2": "vegfr2",
        "vascular endothelial growth factor receptor 2": "vegfr2",
        "pmc oa": "pmc_oa",
        "pmc-oa": "pmc_oa",
        "full text": "full_text",
        "full-text": "full_text",
        "source follow-up": "source_followup",
        "source followup": "source_followup",
        "human angiosarcoma": "human_angiosarcoma",
        "canine hemangiosarcoma": "canine_hemangiosarcoma",
        "dog hemangiosarcoma": "canine_hemangiosarcoma",
        "hemangiosarcoma": "hemangiosarcoma",
        "clinical response": "clinical_response",
    }
    for old, new in replacements.items():
        normalized = normalized.replace(old, new)
    return " ".join(normalized.split())


def _research_hunt_family_terms(normalized_action: str) -> list[str]:
    terms: list[str] = []
    aliases = [
        ("toceranib", ("toceranib", "palladia")),
        ("sorafenib", ("sorafenib",)),
        ("propranolol", ("propranolol",)),
        ("vegfr2", ("vegfr2", "kdr")),
        ("human_angiosarcoma", ("human_angiosarcoma",)),
        ("canine_hemangiosarcoma", ("canine_hemangiosarcoma", "hemangiosarcoma")),
        ("safety", ("safety", "toxicity", "adverse", "tolerability")),
        ("clinical_response", ("clinical_response", "survival", "response")),
        ("full_text", ("full_text", "parser", "license", "pmc_oa")),
        ("source_followup", ("source_followup", "identifier")),
        ("clinical_trial", ("clinicaltrials", "nct")),
        ("openalex", ("openalex",)),
        ("pubmed", ("pubmed", "pmid")),
        ("europe_pmc", ("europe pmc", "europepmc")),
    ]
    for canonical, variants in aliases:
        if any(variant in normalized_action for variant in variants):
            terms.append(canonical)
    return terms


def _dedupe_sequence(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _research_hunt_task_type(action: str) -> str:
    haystack = action.casefold()
    if (
        ("source follow-up" in haystack or "source followup" in haystack or "source_followup" in haystack)
        and ("process" in haystack or "ingest" in haystack or "queue" in haystack)
    ):
        return "source_followup_ingest"
    if "full text" in haystack or "full_text" in haystack or "parser" in haystack or "license" in haystack:
        return "full_text_extract"
    if "citation" in haystack or "citing" in haystack or "cited" in haystack:
        return "citation_chase"
    if "query" in haystack or "search" in haystack or "coverage" in haystack or "newer" in haystack:
        return "broaden_query"
    if "source" in haystack or "pubmed" in haystack or "europe" in haystack or "semantic" in haystack:
        return "add_source"
    if "claim" in haystack or "extract" in haystack:
        return "claim_extract"
    if "adverse" in haystack or "safety" in haystack or "tox" in haystack:
        return "safety_extract"
    if "subgroup" in haystack or "hemangiosarcoma-specific" in haystack:
        return "subgroup_extract"
    return "research_followup"


def _research_hunt_task_priority(task_type: str, verdict: str | None) -> int:
    base = {
        "source_followup_ingest": 10,
        "claim_extract": 20,
        "safety_extract": 25,
        "subgroup_extract": 30,
        "citation_chase": 35,
        "broaden_query": 40,
        "add_source": 45,
        "full_text_extract": 50,
        "research_followup": 60,
    }.get(task_type, 60)
    if verdict == "needs_followup":
        return max(0, base - 5)
    return base


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
