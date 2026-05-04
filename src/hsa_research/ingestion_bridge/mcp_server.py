"""MCP server for HSA AutoResearch.

Run locally:

    python -m hsa_research.ingestion_bridge.mcp_server

Then connect Claude Code or an MCP inspector to the streamable HTTP server.
"""

from __future__ import annotations

from uuid import UUID

from .contracts import (
    AgentPerformanceEvaluationRequest,
    AgentPerformanceReportRequest,
    BoltzRunRequest,
    CandidateDossierRequest,
    ChunkContextRequest,
    ClaimCurationRequest,
    ClaimSearchRequest,
    CommandCenterRequest,
    CommitHypothesisRequest,
    DoiOpenAccessFollowupQueueRequest,
    EvidenceGapResolverRequest,
    FullTextTriageRequest,
    FullTextOpsRequest,
    HypothesisDraft,
    HypothesisProposalRequest,
    ModelProfile,
    ResearchBriefEvaluationRequest,
    ResearchBriefQueueBatchRequest,
    ResearchBriefQueueMaintenanceRequest,
    ResearchBriefQueueRequest,
    ResearchBriefQueueRunRequest,
    ResearchBriefRequest,
    ResearchChunkSearchRequest,
    ResearchFollowupResolverRequest,
    ResearchLeadCollectRequest,
    ResearchObjectReadRequest,
    RetrievalSmokeRequest,
    SourceScoutRequest,
    SourceFollowupIngestRequest,
    SourceFollowupQueueRequest,
    TherapyCommitteeRequest,
    TherapyCommitteeValidationQueueRequest,
    ValidationAutopilotRequest,
    ValidationGapSourceIngestRequest,
    ValidationGapSourcePackRequest,
    ValidationPlanRequest,
    ValidationAssayContext,
    ValidationRequest,
    ValidationRequestQueueRequest,
    XLinkedArticleReviewRequest,
    XLinkedArticleFollowupRequest,
    XTopicReviewRequest,
)
from .service import get_service

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:  # pragma: no cover - exercised only when optional dependency is missing
    FastMCP = None  # type: ignore[assignment]


if FastMCP is None:  # pragma: no cover
    mcp = None
else:
    mcp = FastMCP(
        "HSA AutoResearch",
        stateless_http=True,
        json_response=True,
        instructions=(
            "Use these tools to inspect HSA claims, candidates, hypotheses, "
            "and async validation runs. Expensive tools return run handles."
        ),
    )


def search_research_chunks_tool(
    query: str,
    source_key: str | None = None,
    research_object_id: str | None = None,
    object_type: str | None = None,
    embedding_model: str | None = None,
    min_score: float | None = None,
    limit: int = 10,
    max_chunk_chars: int = 2000,
    include_keyword_fallback: bool = True,
) -> dict:
    """Search retrieval chunks without returning raw embedding vectors."""

    request = ResearchChunkSearchRequest(
        query=query,
        source_key=source_key,
        research_object_id=UUID(research_object_id) if research_object_id else None,
        object_type=object_type,  # type: ignore[arg-type]
        embedding_model=embedding_model,
        min_score=min_score,
        limit=limit,
        max_chunk_chars=max_chunk_chars,
        include_keyword_fallback=include_keyword_fallback,
    )
    return get_service().search_research_chunks(request).model_dump(mode="json")


def get_chunk_context_tool(
    chunk_id: str,
    window: int = 1,
    max_chunk_chars: int = 4000,
    include_entity_mentions: bool = True,
) -> dict:
    """Return a chunk plus nearby chunks and resolved entity mentions."""

    request = ChunkContextRequest(
        chunk_id=UUID(chunk_id),
        window=window,
        max_chunk_chars=max_chunk_chars,
        include_entity_mentions=include_entity_mentions,
    )
    context = get_service().get_chunk_context(request)
    return {} if context is None else context.model_dump(mode="json")


def get_research_object_tool(
    research_object_id: str,
    include_chunks: bool = True,
    max_chunks: int = 20,
    max_chunk_chars: int = 2000,
) -> dict:
    """Return a canonical research object and bounded stored chunks."""

    request = ResearchObjectReadRequest(
        research_object_id=UUID(research_object_id),
        include_chunks=include_chunks,
        max_chunks=max_chunks,
        max_chunk_chars=max_chunk_chars,
    )
    result = get_service().get_research_object(request)
    return {} if result is None else result.model_dump(mode="json")


def run_research_brief_tool(
    topic: str,
    disease_scope: str = "canine hemangiosarcoma and human angiosarcoma",
    source_key: str | None = None,
    max_chunks_per_perspective: int = 8,
    max_claims: int = 12,
    max_chunk_chars: int = 1800,
    brief_style: str = "technical",
    model_profile: str = "research_brief",
    review_mode: str = "openrouter_required",
    review_models: list[str] | None = None,
) -> dict:
    """Run citation-first perspective agents and return a synthesized research brief."""

    request = ResearchBriefRequest(
        topic=topic,
        disease_scope=disease_scope,
        source_key=source_key,
        max_chunks_per_perspective=max_chunks_per_perspective,
        max_claims=max_claims,
        max_chunk_chars=max_chunk_chars,
        brief_style=brief_style,  # type: ignore[arg-type]
        model_profile=model_profile,
        review_mode=review_mode,  # type: ignore[arg-type]
        review_models=review_models or [],
    )
    return get_service().run_research_brief(request).model_dump(mode="json")


def build_research_brief_playground_pack_tool(
    topic: str,
    disease_scope: str = "canine hemangiosarcoma and human angiosarcoma",
    source_key: str | None = None,
    max_chunks_per_perspective: int = 8,
    max_claims: int = 12,
    max_chunk_chars: int = 1800,
    brief_style: str = "technical",
    model_profile: str = "research_brief",
) -> dict:
    """Build playground-ready prompts for manual model review."""

    request = ResearchBriefRequest(
        topic=topic,
        disease_scope=disease_scope,
        source_key=source_key,
        max_chunks_per_perspective=max_chunks_per_perspective,
        max_claims=max_claims,
        max_chunk_chars=max_chunk_chars,
        brief_style=brief_style,  # type: ignore[arg-type]
        model_profile=model_profile,
        review_mode="external_required",
    )
    return get_service().build_research_brief_playground_pack(request).model_dump(mode="json")


def run_therapy_committee_tool(
    topic: str = "curative or disease-modifying therapy ideas for canine hemangiosarcoma",
    disease_scope: str = "canine hemangiosarcoma and human angiosarcoma",
    source_key: str | None = None,
    max_chunks_per_perspective: int = 10,
    max_claims: int = 20,
    max_chunk_chars: int = 2200,
    max_ideas_per_perspective: int = 4,
    model_profile: str = "therapy_committee",
    review_mode: str = "openrouter_required",
    review_models: list[str] | None = None,
) -> dict:
    """Run the cited therapy ideation committee."""

    request = TherapyCommitteeRequest(
        topic=topic,
        disease_scope=disease_scope,
        source_key=source_key,
        max_chunks_per_perspective=max_chunks_per_perspective,
        max_claims=max_claims,
        max_chunk_chars=max_chunk_chars,
        max_ideas_per_perspective=max_ideas_per_perspective,
        model_profile=model_profile,
        review_mode=review_mode,  # type: ignore[arg-type]
        review_models=review_models or [],
    )
    return get_service().run_therapy_committee(request).model_dump(mode="json")


def get_research_brief_tool(brief_id: str) -> dict:
    """Return a persisted research brief by ID."""

    record = get_service().get_research_brief(UUID(brief_id))
    return {} if record is None else record.model_dump(mode="json")


def list_research_briefs_tool(
    status: str | None = None,
    source_key: str | None = None,
    topic_query: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return persisted research brief ledger rows."""

    return [
        record.model_dump(mode="json")
        for record in get_service().list_research_briefs(
            status=status,
            source_key=source_key,
            topic_query=topic_query,
            limit=limit,
        )
    ]


def evaluate_research_brief_tool(
    brief_id: str | None = None,
    topic_query: str | None = None,
    source_key: str | None = None,
    limit: int = 1,
    minimum_overall_score: float = 0.7,
    model_profile: str = "synthesis_quality_evaluator",
) -> dict:
    """Evaluate persisted research brief synthesis quality."""

    request = ResearchBriefEvaluationRequest(
        brief_id=UUID(brief_id) if brief_id else None,
        topic_query=topic_query,
        source_key=source_key,
        limit=limit,
        minimum_overall_score=minimum_overall_score,
        model_profile=model_profile,
    )
    return get_service().evaluate_research_brief(request).model_dump(mode="json")


def get_research_brief_evaluation_tool(evaluation_id: str) -> dict:
    """Return a persisted research brief evaluation by ID."""

    record = get_service().get_research_brief_evaluation(UUID(evaluation_id))
    return {} if record is None else record.model_dump(mode="json")


def list_research_brief_evaluations_tool(
    brief_id: str | None = None,
    readiness: str | None = None,
    passes_quality_bar: bool | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return persisted research brief synthesis evaluations."""

    return [
        record.model_dump(mode="json")
        for record in get_service().list_research_brief_evaluations(
            brief_id=UUID(brief_id) if brief_id else None,
            readiness=readiness,
            passes_quality_bar=passes_quality_bar,
            limit=limit,
        )
    ]


def plan_validation_tool(
    brief_id: str | None = None,
    evaluation_id: str | None = None,
    topic_query: str | None = None,
    source_key: str | None = None,
    require_ready_evaluation: bool = True,
    max_tasks: int = 8,
    model_profile: str = "validation_planner",
) -> dict:
    """Create a recommend-only validation plan from a persisted research brief."""

    request = ValidationPlanRequest(
        brief_id=UUID(brief_id) if brief_id else None,
        evaluation_id=UUID(evaluation_id) if evaluation_id else None,
        topic_query=topic_query,
        source_key=source_key,
        require_ready_evaluation=require_ready_evaluation,
        max_tasks=max_tasks,
        model_profile=model_profile,
    )
    return get_service().plan_validation(request).model_dump(mode="json")


def get_validation_plan_tool(plan_id: str) -> dict:
    """Return a persisted validation plan by ID."""

    record = get_service().get_validation_plan(UUID(plan_id))
    return {} if record is None else record.model_dump(mode="json")


def list_validation_plans_tool(
    brief_id: str | None = None,
    evaluation_id: str | None = None,
    status: str | None = None,
    readiness: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return persisted validation plans."""

    return [
        record.model_dump(mode="json")
        for record in get_service().list_validation_plans(
            brief_id=UUID(brief_id) if brief_id else None,
            evaluation_id=UUID(evaluation_id) if evaluation_id else None,
            status=status,
            readiness=readiness,
            limit=limit,
        )
    ]


def queue_validation_requests_tool(
    plan_id: str,
    task_ids: list[str] | None = None,
    dry_run: bool = True,
) -> dict:
    """Queue validation requests from a ready validation plan."""

    request = ValidationRequestQueueRequest(
        plan_id=UUID(plan_id),
        task_ids=[UUID(value) for value in task_ids or []],
        dry_run=dry_run,
    )
    return get_service().queue_validation_requests_from_plan(request).model_dump(mode="json")


def queue_therapy_committee_validation_requests_tool(
    agent_run_id: str | None = None,
    idea_ids: list[str] | None = None,
    max_ideas: int = 1,
    priority: int = 40,
    dry_run: bool = True,
) -> dict:
    """Queue validation requests from a completed therapy committee agent run."""

    request = TherapyCommitteeValidationQueueRequest(
        agent_run_id=UUID(agent_run_id) if agent_run_id else None,
        idea_ids=[UUID(value) for value in idea_ids or []],
        max_ideas=max_ideas,
        priority=priority,
        dry_run=dry_run,
    )
    return get_service().queue_therapy_committee_validation_requests(request).model_dump(mode="json")


def get_validation_request_queue_item_tool(queue_item_id: str) -> dict:
    """Return one queued validation request."""

    item = get_service().get_validation_request_queue_item(UUID(queue_item_id))
    return {} if item is None else item.model_dump(mode="json")


def list_validation_request_queue_tool(
    plan_id: str | None = None,
    status: str | None = None,
    statuses: list[str] | None = None,
    source_key: str | None = None,
    task_type: str | None = None,
    topic_query: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return queued validation requests."""

    return [
        item.model_dump(mode="json")
        for item in get_service().list_validation_request_queue_items(
            plan_id=UUID(plan_id) if plan_id else None,
            status=status,
            statuses=statuses,
            source_key=source_key,
            task_type=task_type,
            topic_query=topic_query,
            limit=limit,
        )
    ]


def approve_validation_request_tool(
    queue_item_id: str,
    approved_by: str,
    approval_note: str | None = None,
) -> dict:
    """Approve a queued validation request for explicit dispatch."""

    item = get_service().approve_validation_request_queue_item(
        UUID(queue_item_id),
        approved_by=approved_by,
        approval_note=approval_note,
    )
    return {} if item is None else item.model_dump(mode="json")


def dispatch_validation_request_tool(queue_item_id: str, model_profile: str = "openrouter_required") -> dict:
    """Dispatch an approved queued validation request."""

    item = get_service().dispatch_validation_request_queue_item(
        UUID(queue_item_id),
        model_profile=model_profile,
    )
    return {} if item is None else item.model_dump(mode="json")


def run_validation_autopilot_tool(
    dry_run: bool = True,
    force: bool = False,
    max_per_run: int = 2,
    manual_grace_period_hours: float = 6.0,
    minimum_queue_age_hours: float = 1.0,
    hourly_budget_usd: float = 0.25,
    daily_budget_usd: float = 1.50,
    allowed_task_types: list[str] | None = None,
    allowed_validation_types: list[str] | None = None,
    source_keys: list[str] | None = None,
    model_profile: str = "openrouter_required",
) -> dict:
    """Preview or run the conservative validation autopilot."""

    request = ValidationAutopilotRequest(
        dry_run=dry_run,
        force=force,
        max_per_run=max_per_run,
        manual_grace_period_hours=manual_grace_period_hours,
        minimum_queue_age_hours=minimum_queue_age_hours,
        hourly_budget_usd=hourly_budget_usd,
        daily_budget_usd=daily_budget_usd,
        allowed_task_types=allowed_task_types or ["expert_review", "target_validation", "omics"],
        allowed_validation_types=allowed_validation_types or ["expert_review", "homology", "omics"],
        source_keys=source_keys or [],
        model_profile=model_profile,
    )
    service = get_service()
    if dry_run:
        return service.preview_validation_autopilot(request).model_dump(mode="json")
    return service.run_validation_autopilot(request).model_dump(mode="json")


def resolve_evidence_gaps_tool(
    queue_item_ids: list[str] | None = None,
    plan_id: str | None = None,
    statuses: list[str] | None = None,
    decisions: list[str] | None = None,
    task_types: list[str] | None = None,
    gap_types: list[str] | None = None,
    limit: int = 25,
    max_gaps_per_item: int = 8,
    priority: int = 30,
    dry_run: bool = True,
    queue_research_briefs: bool = False,
) -> dict:
    """Convert validation-agent evidence gaps into research leads and optional brief queue items."""

    request = EvidenceGapResolverRequest(
        queue_item_ids=[UUID(value) for value in queue_item_ids or []],
        plan_id=UUID(plan_id) if plan_id else None,
        statuses=statuses or ["completed"],  # type: ignore[arg-type]
        decisions=decisions or ["hold", "demote"],  # type: ignore[arg-type]
        task_types=task_types or [],  # type: ignore[arg-type]
        gap_types=gap_types or ["missing_evidence", "risk", "next_action"],  # type: ignore[arg-type]
        limit=limit,
        max_gaps_per_item=max_gaps_per_item,
        priority=priority,
        dry_run=dry_run,
        queue_research_briefs=queue_research_briefs,
    )
    return get_service().resolve_evidence_gaps(request).model_dump(mode="json")


def build_validation_gap_source_pack_tool(
    queue_item_ids: list[str] | None = None,
    lead_ids: list[str] | None = None,
    lead_statuses: list[str] | None = None,
    source_keys: list[str] | None = None,
    lanes: list[str] | None = None,
    limit: int = 25,
    max_queries_per_lane: int = 3,
    persist_queries: bool = False,
    active: bool = True,
    dry_run: bool = True,
) -> dict:
    """Build targeted SourceQuery rows for validation evidence gaps."""

    request = ValidationGapSourcePackRequest(
        queue_item_ids=[UUID(value) for value in queue_item_ids or []],
        lead_ids=[UUID(value) for value in lead_ids or []],
        lead_statuses=lead_statuses or ["new", "followup"],  # type: ignore[arg-type]
        source_keys=source_keys or [],
        lanes=lanes or [],  # type: ignore[arg-type]
        limit=limit,
        max_queries_per_lane=max_queries_per_lane,
        persist_queries=persist_queries,
        active=active,
        dry_run=dry_run,
    )
    return get_service().build_validation_gap_source_pack(request).model_dump(mode="json")


def ingest_validation_gap_source_queries_tool(
    source_keys: list[str] | None = None,
    query_names: list[str] | None = None,
    limit_per_query: int = 5,
    max_queries: int = 50,
    dry_run: bool = True,
) -> dict:
    """Ingest only active validation-gap SourceQuery rows."""

    request = ValidationGapSourceIngestRequest(
        source_keys=source_keys or [],
        query_names=query_names or [],
        limit_per_query=limit_per_query,
        max_queries=max_queries,
        dry_run=dry_run,
    )
    return get_service().ingest_validation_gap_source_queries(request).model_dump(mode="json")


def queue_research_brief_tool(
    topic: str,
    disease_scope: str = "canine hemangiosarcoma and human angiosarcoma",
    source_key: str | None = None,
    priority: int = 100,
    max_chunks_per_perspective: int = 8,
    max_claims: int = 12,
    max_chunk_chars: int = 1800,
    brief_style: str = "technical",
    model_profile: str = "research_brief",
    review_mode: str = "openrouter_required",
    review_models: list[str] | None = None,
) -> dict:
    """Queue a research brief request for later execution."""

    request = ResearchBriefQueueRequest(
        topic=topic,
        disease_scope=disease_scope,
        source_key=source_key,
        priority=priority,
        max_chunks_per_perspective=max_chunks_per_perspective,
        max_claims=max_claims,
        max_chunk_chars=max_chunk_chars,
        brief_style=brief_style,  # type: ignore[arg-type]
        model_profile=model_profile,
        review_mode=review_mode,  # type: ignore[arg-type]
        review_models=review_models or [],
    )
    return get_service().queue_research_brief(request).model_dump(mode="json")


def queue_research_brief_batch_tool(
    mode: str = "both",
    lead_statuses: list[str] | None = None,
    lead_types: list[str] | None = None,
    source_keys: list[str] | None = None,
    source_health_statuses: list[str] | None = None,
    source_health_report: dict | None = None,
    include_empty_sources: bool = False,
    limit: int = 25,
    disease_scope: str = "canine hemangiosarcoma and human angiosarcoma",
    priority: int = 80,
    max_chunks_per_perspective: int = 8,
    max_claims: int = 12,
    max_chunk_chars: int = 1800,
    brief_style: str = "technical",
    model_profile: str = "research_brief",
    review_mode: str = "openrouter_required",
    review_models: list[str] | None = None,
) -> dict:
    """Queue research brief requests from watchlist leads and source-health gaps."""

    request = ResearchBriefQueueBatchRequest(
        mode=mode,  # type: ignore[arg-type]
        lead_statuses=lead_statuses or ["new", "watching"],  # type: ignore[arg-type]
        lead_types=lead_types or [],  # type: ignore[arg-type]
        source_keys=source_keys or [],
        source_health_statuses=source_health_statuses or ["failing", "triage", "watch"],  # type: ignore[arg-type]
        source_health_report=source_health_report,
        include_empty_sources=include_empty_sources,
        limit=limit,
        disease_scope=disease_scope,
        priority=priority,
        max_chunks_per_perspective=max_chunks_per_perspective,
        max_claims=max_claims,
        max_chunk_chars=max_chunk_chars,
        brief_style=brief_style,  # type: ignore[arg-type]
        model_profile=model_profile,
        review_mode=review_mode,  # type: ignore[arg-type]
        review_models=review_models or [],
    )
    return get_service().queue_research_brief_batch(request).model_dump(mode="json")


def get_research_brief_queue_item_tool(queue_item_id: str) -> dict:
    """Return a queued research brief request by ID."""

    item = get_service().get_research_brief_queue_item(UUID(queue_item_id))
    return {} if item is None else item.model_dump(mode="json")


def list_research_brief_queue_tool(
    status: str | None = None,
    source_key: str | None = None,
    topic_query: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """List queued research brief requests."""

    return [
        item.model_dump(mode="json")
        for item in get_service().list_research_brief_queue_items(
            status=status,
            source_key=source_key,
            topic_query=topic_query,
            limit=limit,
        )
    ]


def run_research_brief_queue_tool(
    statuses: list[str] | None = None,
    source_key: str | None = None,
    topic_query: str | None = None,
    limit: int = 1,
) -> dict:
    """Run the next queued research brief request."""

    request = ResearchBriefQueueRunRequest(
        statuses=statuses or ["queued"],  # type: ignore[arg-type]
        source_key=source_key,
        topic_query=topic_query,
        limit=limit,
    )
    return get_service().run_next_research_brief_queue_item(request).model_dump(mode="json")


def requeue_research_brief_queue_item_tool(
    queue_item_id: str,
    priority: int | None = None,
) -> dict:
    """Move a failed research brief queue item back to queued."""

    item = get_service().requeue_research_brief_queue_item(UUID(queue_item_id), priority=priority)
    return {} if item is None else item.model_dump(mode="json")


def archive_research_brief_queue_item_tool(queue_item_id: str) -> dict:
    """Archive a completed research brief queue item."""

    item = get_service().archive_research_brief_queue_item(UUID(queue_item_id))
    return {} if item is None else item.model_dump(mode="json")


def maintain_research_brief_queue_tool(
    queue_item_ids: list[str] | None = None,
    statuses: list[str] | None = None,
    source_key: str | None = None,
    topic_query: str | None = None,
    min_attempts: int = 1,
    max_updated_age_hours: float = 12.0,
    limit: int = 50,
    dry_run: bool = True,
    reason: str = "stale_research_brief_queue_cleanup",
) -> dict:
    """Dry-run or apply safe research brief queue maintenance."""

    request = ResearchBriefQueueMaintenanceRequest(
        queue_item_ids=[UUID(value) for value in queue_item_ids or []],
        statuses=statuses or ["failed"],  # type: ignore[arg-type]
        source_key=source_key,
        topic_query=topic_query,
        min_attempts=min_attempts,
        max_updated_age_hours=max_updated_age_hours,
        limit=limit,
        dry_run=dry_run,
        reason=reason,
    )
    return get_service().maintain_research_brief_queue(request).model_dump(mode="json")


def run_retrieval_smoke_tool(
    query: str = "hemangiosarcoma angiogenesis",
    source_key: str | None = None,
    object_type: str | None = None,
    embedding_model: str | None = None,
    limit: int = 3,
    max_chunk_chars: int = 1200,
    context_window: int = 1,
    include_entity_mentions: bool = True,
    include_keyword_fallback: bool = True,
    require_embedding: bool = False,
) -> dict:
    """Exercise MCP retrieval reads: search, chunk context, and parent object."""

    request = RetrievalSmokeRequest(
        query=query,
        source_key=source_key,
        object_type=object_type,  # type: ignore[arg-type]
        embedding_model=embedding_model,
        limit=limit,
        max_chunk_chars=max_chunk_chars,
        context_window=context_window,
        include_entity_mentions=include_entity_mentions,
        include_keyword_fallback=include_keyword_fallback,
        require_embedding=require_embedding,
    )
    return get_service().run_retrieval_smoke(request).model_dump(mode="json")


def triage_full_text_issue_tool(
    source_key: str,
    stage: str = "qa",
    query_name: str | None = None,
    error_message: str | None = None,
    errors: list[str] | None = None,
    runtime_seconds: float | None = None,
    timeout_seconds: float | None = None,
    raw_records: int = 0,
    research_objects: int = 0,
    document_chunks: int = 0,
    full_text_document_chunks: int = 0,
    full_text_body_chars: int = 0,
    claims: int = 0,
    entity_mentions: int = 0,
    current_failed_runs: list[str] | None = None,
    http_status: int | None = None,
    model_profile: str = "cheap_classifier",
    metadata: dict | None = None,
) -> dict:
    """Classify a full-text edge case into a bounded operational action."""

    request = FullTextTriageRequest(
        source_key=source_key,
        stage=stage,  # type: ignore[arg-type]
        query_name=query_name,
        error_message=error_message,
        errors=errors or [],
        runtime_seconds=runtime_seconds,
        timeout_seconds=timeout_seconds,
        raw_records=raw_records,
        research_objects=research_objects,
        document_chunks=document_chunks,
        full_text_document_chunks=full_text_document_chunks,
        full_text_body_chars=full_text_body_chars,
        claims=claims,
        entity_mentions=entity_mentions,
        current_failed_runs=current_failed_runs or [],
        http_status=http_status,
        model_profile=model_profile,
        metadata=metadata or {},
    )
    return get_service().triage_full_text_issue(request).model_dump(mode="json")


def run_full_text_ops_tool(
    source_keys: list[str] | None = None,
    partition_date: str | None = None,
    source_health_report: dict | None = None,
    full_text_report: dict | None = None,
    recent_run_limit: int = 10,
    model_profile: str = "reviewer",
    review_mode: str = "openrouter_required",
    review_models: list[str] | None = None,
    dagster_run_id: str | None = None,
    metadata: dict | None = None,
) -> dict:
    """Run the recommend-only full-text ops agent."""

    request = FullTextOpsRequest(
        source_keys=source_keys or [],
        partition_date=partition_date,
        source_health_report=source_health_report,
        full_text_report=full_text_report,
        recent_run_limit=recent_run_limit,
        model_profile=model_profile,
        review_mode=review_mode,  # type: ignore[arg-type]
        review_models=review_models or [],
        dagster_run_id=dagster_run_id,
        metadata=metadata or {},
    )
    return get_service().run_full_text_ops(request).model_dump(mode="json")


def run_x_topic_review_tool(
    provider_report: dict | None = None,
    candidates: list[dict] | None = None,
    recent_run_limit: int = 5,
    max_candidates: int = 20,
    model_profile: str = "reviewer",
    review_mode: str = "openrouter_required",
    review_models: list[str] | None = None,
    dagster_run_id: str | None = None,
    metadata: dict | None = None,
) -> dict:
    """Run the recommend-only X topic review agent."""

    request = XTopicReviewRequest(
        provider_report=provider_report,
        candidates=candidates or [],
        recent_run_limit=recent_run_limit,
        max_candidates=max_candidates,
        model_profile=model_profile,
        review_mode=review_mode,  # type: ignore[arg-type]
        review_models=review_models or [],
        dagster_run_id=dagster_run_id,
        metadata=metadata or {},
    )
    return get_service().run_x_topic_review(request).model_dump(mode="json")


def run_x_linked_article_followup_tool(
    urls: list[str] | None = None,
    recent_run_limit: int = 10,
    max_urls: int = 10,
    fetch: bool = True,
    parse: bool = True,
    approved_by: str | None = None,
    approval_note: str | None = None,
    robots_policy: str = "reviewed",
    metadata: dict | None = None,
) -> dict:
    """Fetch and parse controlled article links queued by X topic review."""

    request = XLinkedArticleFollowupRequest(
        urls=urls or [],
        recent_run_limit=recent_run_limit,
        max_urls=max_urls,
        fetch=fetch,
        parse=parse,
        approved_by=approved_by,
        approval_note=approval_note,
        robots_policy=robots_policy,  # type: ignore[arg-type]
        metadata=metadata or {},
    )
    return get_service().run_x_linked_article_followup(request).model_dump(mode="json")


def run_x_linked_article_review_tool(
    review_ids: list[str] | None = None,
    review_status: str | None = "needs_review",
    limit: int = 50,
    model_profile: str = "reviewer",
    review_mode: str = "openrouter_required",
    review_models: list[str] | None = None,
    dagster_run_id: str | None = None,
    metadata: dict | None = None,
) -> dict:
    """Run the recommend-only linked-article review agent."""

    request = XLinkedArticleReviewRequest(
        review_ids=[UUID(review_id) for review_id in (review_ids or [])],
        review_status=review_status,  # type: ignore[arg-type]
        limit=limit,
        model_profile=model_profile,
        review_mode=review_mode,  # type: ignore[arg-type]
        review_models=review_models or [],
        dagster_run_id=dagster_run_id,
        metadata=metadata or {},
    )
    return get_service().run_x_linked_article_review(request).model_dump(mode="json")


def queue_source_followups_tool(
    source_key: str = "x_linked_article",
    review_ids: list[str] | None = None,
    review_status: str | None = None,
    limit: int = 100,
    include_existing: bool = False,
    include_agent_recommendations: bool = True,
    agent_run_limit: int = 20,
) -> dict:
    """Queue primary-source follow-ups from parsed records and review-agent recommendations."""

    request = SourceFollowupQueueRequest(
        source_key=source_key,
        review_ids=[UUID(review_id) for review_id in (review_ids or [])],
        review_status=review_status,  # type: ignore[arg-type]
        limit=limit,
        include_existing=include_existing,
        include_agent_recommendations=include_agent_recommendations,
        agent_run_limit=agent_run_limit,
    )
    return get_service().queue_source_followups(request).model_dump(mode="json")


def queue_unpaywall_doi_followups_tool(
    source_keys: list[str] | None = None,
    limit: int = 100,
    include_existing: bool = False,
) -> dict:
    """Queue manual Unpaywall open-access enrichment for DOI-bearing objects."""

    request = DoiOpenAccessFollowupQueueRequest(
        source_keys=source_keys or [],
        limit=limit,
        include_existing=include_existing,
    )
    return get_service().queue_unpaywall_doi_followups(request).model_dump(mode="json")


def list_source_followups_tool(
    source_key: str | None = None,
    status: str | None = None,
    identifier_type: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return queued source follow-ups."""

    return [
        item.model_dump(mode="json")
        for item in get_service().list_source_followups(
            source_key=source_key,
            status=status,
            identifier_type=identifier_type,
            limit=limit,
        )
    ]


def collect_research_leads_tool(
    agent_names: list[str] | None = None,
    statuses: list[str] | None = None,
    limit: int = 50,
    include_existing: bool = False,
) -> dict:
    """Collect watchlist leads from recent agent runs."""

    request = ResearchLeadCollectRequest(
        agent_names=agent_names or ["x_linked_article_review_agent", "x_topic_review_agent"],
        statuses=statuses or ["completed"],
        limit=limit,
        include_existing=include_existing,
    )
    return get_service().collect_research_leads(request).model_dump(mode="json")


def get_research_lead_tool(lead_id: str) -> dict:
    """Return a persisted research watchlist lead."""

    record = get_service().get_research_lead(UUID(lead_id))
    return {} if record is None else record.model_dump(mode="json")


def list_research_leads_tool(
    status: str | None = None,
    lead_type: str | None = None,
    source_key: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return persisted research watchlist leads."""

    return [
        lead.model_dump(mode="json")
        for lead in get_service().list_research_leads(
            status=status,
            lead_type=lead_type,
            source_key=source_key,
            limit=limit,
        )
    ]


def resolve_research_followups_tool(
    lead_ids: list[str] | None = None,
    statuses: list[str] | None = None,
    source_keys: list[str] | None = None,
    search_source_keys: list[str] | None = None,
    limit: int = 25,
    ingest_source_followups: bool = True,
    search_missing_identifiers: bool = True,
    promote_ready_leads: bool = True,
    run_claim_extraction: bool = True,
    dry_run: bool = False,
    min_evidence_chunks: int = 1,
    search_limit_per_source: int = 2,
    max_search_terms: int = 12,
    approved_by: str | None = None,
    metadata: dict | None = None,
) -> dict:
    """Resolve evidence-light research leads into durable source evidence."""

    request = ResearchFollowupResolverRequest(
        lead_ids=[UUID(lead_id) for lead_id in (lead_ids or [])],
        statuses=statuses or ["followup"],  # type: ignore[list-item]
        source_keys=source_keys or [],
        search_source_keys=search_source_keys or [],
        limit=limit,
        ingest_source_followups=ingest_source_followups,
        search_missing_identifiers=search_missing_identifiers,
        promote_ready_leads=promote_ready_leads,
        run_claim_extraction=run_claim_extraction,
        dry_run=dry_run,
        min_evidence_chunks=min_evidence_chunks,
        search_limit_per_source=search_limit_per_source,
        max_search_terms=max_search_terms,
        approved_by=approved_by,
        metadata=metadata or {},
    )
    return get_service().resolve_research_followups(request).model_dump(mode="json")


def ingest_source_followups_tool(
    followup_ids: list[str] | None = None,
    source_keys: list[str] | None = None,
    statuses: list[str] | None = None,
    limit: int = 25,
    approved_by: str | None = None,
    run_claim_extraction: bool = True,
    dry_run: bool = False,
    metadata: dict | None = None,
) -> dict:
    """Ingest queued primary-source follow-ups through existing API harvesters."""

    request = SourceFollowupIngestRequest(
        followup_ids=[UUID(followup_id) for followup_id in (followup_ids or [])],
        source_keys=source_keys or [],
        statuses=statuses or ["queued", "approved"],  # type: ignore[list-item]
        limit=limit,
        approved_by=approved_by,
        run_claim_extraction=run_claim_extraction,
        dry_run=dry_run,
        metadata=metadata or {},
    )
    return get_service().ingest_source_followups(request).model_dump(mode="json")


def get_agent_run_tool(agent_run_id: str) -> dict:
    """Return a persisted agent run."""

    record = get_service().get_agent_run(UUID(agent_run_id))
    return {} if record is None else record.model_dump(mode="json")


def list_agent_runs_tool(
    agent_name: str | None = None,
    status: str | None = None,
    source_key: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Return recent persisted agent runs."""

    return [
        run.model_dump(mode="json")
        for run in get_service().list_agent_runs(
            agent_name=agent_name,
            status=status,
            source_key=source_key,
            limit=limit,
        )
    ]


def command_center_tool(
    source_keys: list[str] | None = None,
    include_source_health: bool = True,
    include_recent_agents: bool = True,
    queue_limit: int = 25,
    lead_limit: int = 25,
    agent_run_limit: int = 25,
    min_health_score: float = 0.65,
    require_claims: bool = True,
) -> dict:
    """Return the read-only TWOG command-center report."""

    request = CommandCenterRequest(
        source_keys=source_keys or [],
        include_source_health=include_source_health,
        include_recent_agents=include_recent_agents,
        queue_limit=queue_limit,
        lead_limit=lead_limit,
        agent_run_limit=agent_run_limit,
        min_health_score=min_health_score,
        require_claims=require_claims,
    )
    return get_service().build_command_center_report(request).model_dump(mode="json")


def agent_performance_report_tool(
    agent_name: str | None = None,
    status: str | None = None,
    source_key: str | None = None,
    limit: int = 500,
    min_sample_size: int = 3,
) -> dict:
    """Return hybrid operator/evaluator performance aggregation."""

    return get_service().build_agent_performance_report(
        AgentPerformanceReportRequest(
            agent_name=agent_name,
            status=status,
            source_key=source_key,
            limit=limit,
            min_sample_size=min_sample_size,
        )
    ).model_dump(mode="json")


def run_agent_performance_evaluation_tool(
    agent_name: str | None = None,
    status: str | None = "completed",
    source_key: str | None = None,
    limit: int = 25,
    reviewed_only: bool = True,
    model_profile: str = "agent_performance_evaluator",
    review_models: list[str] | None = None,
) -> dict:
    """Run OpenRouter specialist evaluators over recent reviewed agent runs."""

    return get_service().run_agent_performance_evaluation(
        AgentPerformanceEvaluationRequest(
            agent_name=agent_name,
            status=status,
            source_key=source_key,
            limit=limit,
            reviewed_only=reviewed_only,
            model_profile=model_profile,
            review_models=review_models or [],
        )
    ).model_dump(mode="json")


if mcp is not None:

    @mcp.tool()
    def search_claims(
        query: str | None = None,
        species: str | None = None,
        targets: list[str] | None = None,
        compounds: list[str] | None = None,
        min_confidence: float = 0.0,
        include_drafts: bool = False,
        limit: int = 20,
    ) -> dict:
        """Search provenance-backed scientific claims."""

        request = ClaimSearchRequest(
            query=query,
            species=species,
            targets=targets or [],
            compounds=compounds or [],
            min_confidence=min_confidence,
            include_drafts=include_drafts,
            limit=limit,
        )
        return get_service().search_claims(request).model_dump(mode="json")

    @mcp.tool()
    def search_research_chunks(
        query: str,
        source_key: str | None = None,
        research_object_id: str | None = None,
        object_type: str | None = None,
        embedding_model: str | None = None,
        min_score: float | None = None,
        limit: int = 10,
        max_chunk_chars: int = 2000,
        include_keyword_fallback: bool = True,
    ) -> dict:
        """Search retrieval chunks using embeddings first, with keyword fallback."""

        return search_research_chunks_tool(
            query=query,
            source_key=source_key,
            research_object_id=research_object_id,
            object_type=object_type,
            embedding_model=embedding_model,
            min_score=min_score,
            limit=limit,
            max_chunk_chars=max_chunk_chars,
            include_keyword_fallback=include_keyword_fallback,
        )

    @mcp.tool()
    def get_chunk_context(
        chunk_id: str,
        window: int = 1,
        max_chunk_chars: int = 4000,
        include_entity_mentions: bool = True,
    ) -> dict:
        """Return a chunk with nearby chunks and entity context."""

        return get_chunk_context_tool(
            chunk_id=chunk_id,
            window=window,
            max_chunk_chars=max_chunk_chars,
            include_entity_mentions=include_entity_mentions,
        )

    @mcp.tool()
    def get_research_object(
        research_object_id: str,
        include_chunks: bool = True,
        max_chunks: int = 20,
        max_chunk_chars: int = 2000,
    ) -> dict:
        """Return a canonical research object and bounded chunk list."""

        return get_research_object_tool(
            research_object_id=research_object_id,
            include_chunks=include_chunks,
            max_chunks=max_chunks,
            max_chunk_chars=max_chunk_chars,
        )

    @mcp.tool()
    def run_research_brief(
        topic: str,
        disease_scope: str = "canine hemangiosarcoma and human angiosarcoma",
        source_key: str | None = None,
        max_chunks_per_perspective: int = 8,
        max_claims: int = 12,
        max_chunk_chars: int = 1800,
        brief_style: str = "technical",
        model_profile: str = "research_brief",
        review_mode: str = "openrouter_required",
        review_models: list[str] | None = None,
    ) -> dict:
        """Run evidence scout, translational, skeptic, and synthesis agents."""

        return run_research_brief_tool(
            topic=topic,
            disease_scope=disease_scope,
            source_key=source_key,
            max_chunks_per_perspective=max_chunks_per_perspective,
            max_claims=max_claims,
            max_chunk_chars=max_chunk_chars,
            brief_style=brief_style,
            model_profile=model_profile,
            review_mode=review_mode,
            review_models=review_models,
        )

    @mcp.tool()
    def build_research_brief_playground_pack(
        topic: str,
        disease_scope: str = "canine hemangiosarcoma and human angiosarcoma",
        source_key: str | None = None,
        max_chunks_per_perspective: int = 8,
        max_claims: int = 12,
        max_chunk_chars: int = 1800,
        brief_style: str = "technical",
        model_profile: str = "research_brief",
    ) -> dict:
        """Return system/user prompts, evidence payloads, and rubrics for model playgrounds."""

        return build_research_brief_playground_pack_tool(
            topic=topic,
            disease_scope=disease_scope,
            source_key=source_key,
            max_chunks_per_perspective=max_chunks_per_perspective,
            max_claims=max_claims,
            max_chunk_chars=max_chunk_chars,
            brief_style=brief_style,
            model_profile=model_profile,
        )

    @mcp.tool()
    def run_therapy_committee(
        topic: str = "curative or disease-modifying therapy ideas for canine hemangiosarcoma",
        disease_scope: str = "canine hemangiosarcoma and human angiosarcoma",
        source_key: str | None = None,
        max_chunks_per_perspective: int = 10,
        max_claims: int = 20,
        max_chunk_chars: int = 2200,
        max_ideas_per_perspective: int = 4,
        model_profile: str = "therapy_committee",
        review_mode: str = "openrouter_required",
        review_models: list[str] | None = None,
    ) -> dict:
        """Run the cited therapy ideation committee."""

        return run_therapy_committee_tool(
            topic=topic,
            disease_scope=disease_scope,
            source_key=source_key,
            max_chunks_per_perspective=max_chunks_per_perspective,
            max_claims=max_claims,
            max_chunk_chars=max_chunk_chars,
            max_ideas_per_perspective=max_ideas_per_perspective,
            model_profile=model_profile,
            review_mode=review_mode,
            review_models=review_models,
        )

    @mcp.tool()
    def get_research_brief(brief_id: str) -> dict:
        """Return one persisted research brief ledger record."""

        return get_research_brief_tool(brief_id)

    @mcp.tool()
    def list_research_briefs(
        status: str | None = None,
        source_key: str | None = None,
        topic_query: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """List persisted research brief ledger records."""

        return list_research_briefs_tool(
            status=status,
            source_key=source_key,
            topic_query=topic_query,
            limit=limit,
        )

    @mcp.tool()
    def evaluate_research_brief(
        brief_id: str | None = None,
        topic_query: str | None = None,
        source_key: str | None = None,
        limit: int = 1,
        minimum_overall_score: float = 0.7,
        model_profile: str = "synthesis_quality_evaluator",
    ) -> dict:
        """Evaluate a persisted research brief for synthesis readiness."""

        return evaluate_research_brief_tool(
            brief_id=brief_id,
            topic_query=topic_query,
            source_key=source_key,
            limit=limit,
            minimum_overall_score=minimum_overall_score,
            model_profile=model_profile,
        )

    @mcp.tool()
    def get_research_brief_evaluation(evaluation_id: str) -> dict:
        """Return one persisted research brief synthesis evaluation."""

        return get_research_brief_evaluation_tool(evaluation_id)

    @mcp.tool()
    def list_research_brief_evaluations(
        brief_id: str | None = None,
        readiness: str | None = None,
        passes_quality_bar: bool | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """List persisted research brief synthesis evaluations."""

        return list_research_brief_evaluations_tool(
            brief_id=brief_id,
            readiness=readiness,
            passes_quality_bar=passes_quality_bar,
            limit=limit,
        )

    @mcp.tool()
    def plan_validation(
        brief_id: str | None = None,
        evaluation_id: str | None = None,
        topic_query: str | None = None,
        source_key: str | None = None,
        require_ready_evaluation: bool = True,
        max_tasks: int = 8,
        model_profile: str = "validation_planner",
    ) -> dict:
        """Create a recommend-only validation plan from a persisted research brief."""

        return plan_validation_tool(
            brief_id=brief_id,
            evaluation_id=evaluation_id,
            topic_query=topic_query,
            source_key=source_key,
            require_ready_evaluation=require_ready_evaluation,
            max_tasks=max_tasks,
            model_profile=model_profile,
        )

    @mcp.tool()
    def get_validation_plan(plan_id: str) -> dict:
        """Return one persisted validation plan."""

        return get_validation_plan_tool(plan_id)

    @mcp.tool()
    def list_validation_plans(
        brief_id: str | None = None,
        evaluation_id: str | None = None,
        status: str | None = None,
        readiness: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """List persisted validation plans."""

        return list_validation_plans_tool(
            brief_id=brief_id,
            evaluation_id=evaluation_id,
            status=status,
            readiness=readiness,
            limit=limit,
        )

    @mcp.tool()
    def queue_validation_requests(
        plan_id: str,
        task_ids: list[str] | None = None,
        dry_run: bool = True,
    ) -> dict:
        """Queue validation requests from a ready validation plan."""

        return queue_validation_requests_tool(
            plan_id=plan_id,
            task_ids=task_ids,
            dry_run=dry_run,
        )

    @mcp.tool()
    def queue_therapy_committee_validation_requests(
        agent_run_id: str | None = None,
        idea_ids: list[str] | None = None,
        max_ideas: int = 1,
        priority: int = 40,
        dry_run: bool = True,
    ) -> dict:
        """Queue validation requests from a completed therapy committee agent run."""

        return queue_therapy_committee_validation_requests_tool(
            agent_run_id=agent_run_id,
            idea_ids=idea_ids,
            max_ideas=max_ideas,
            priority=priority,
            dry_run=dry_run,
        )

    @mcp.tool()
    def get_validation_request_queue_item(queue_item_id: str) -> dict:
        """Return one queued validation request."""

        return get_validation_request_queue_item_tool(queue_item_id)

    @mcp.tool()
    def list_validation_request_queue(
        plan_id: str | None = None,
        status: str | None = None,
        statuses: list[str] | None = None,
        source_key: str | None = None,
        task_type: str | None = None,
        topic_query: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """List queued validation requests."""

        return list_validation_request_queue_tool(
            plan_id=plan_id,
            status=status,
            statuses=statuses,
            source_key=source_key,
            task_type=task_type,
            topic_query=topic_query,
            limit=limit,
        )

    @mcp.tool()
    def approve_validation_request(
        queue_item_id: str,
        approved_by: str,
        approval_note: str | None = None,
    ) -> dict:
        """Approve a queued validation request for explicit dispatch."""

        return approve_validation_request_tool(
            queue_item_id=queue_item_id,
            approved_by=approved_by,
            approval_note=approval_note,
        )

    @mcp.tool()
    def dispatch_validation_request(queue_item_id: str, model_profile: str = "openrouter_required") -> dict:
        """Dispatch an approved queued validation request."""

        return dispatch_validation_request_tool(queue_item_id, model_profile=model_profile)

    @mcp.tool()
    def run_validation_autopilot(
        dry_run: bool = True,
        force: bool = False,
        max_per_run: int = 2,
        manual_grace_period_hours: float = 6.0,
        minimum_queue_age_hours: float = 1.0,
        hourly_budget_usd: float = 0.25,
        daily_budget_usd: float = 1.50,
        allowed_task_types: list[str] | None = None,
        allowed_validation_types: list[str] | None = None,
        source_keys: list[str] | None = None,
        model_profile: str = "openrouter_required",
    ) -> dict:
        """Preview or run conservative automatic validation approval and dispatch."""

        return run_validation_autopilot_tool(
            dry_run=dry_run,
            force=force,
            max_per_run=max_per_run,
            manual_grace_period_hours=manual_grace_period_hours,
            minimum_queue_age_hours=minimum_queue_age_hours,
            hourly_budget_usd=hourly_budget_usd,
            daily_budget_usd=daily_budget_usd,
            allowed_task_types=allowed_task_types,
            allowed_validation_types=allowed_validation_types,
            source_keys=source_keys,
            model_profile=model_profile,
        )

    @mcp.tool()
    def resolve_evidence_gaps(
        queue_item_ids: list[str] | None = None,
        plan_id: str | None = None,
        statuses: list[str] | None = None,
        decisions: list[str] | None = None,
        task_types: list[str] | None = None,
        gap_types: list[str] | None = None,
        limit: int = 25,
        max_gaps_per_item: int = 8,
        priority: int = 30,
        dry_run: bool = True,
        queue_research_briefs: bool = False,
    ) -> dict:
        """Convert validation-agent evidence gaps into research leads and optional brief queue items."""

        return resolve_evidence_gaps_tool(
            queue_item_ids=queue_item_ids,
            plan_id=plan_id,
            statuses=statuses,
            decisions=decisions,
            task_types=task_types,
            gap_types=gap_types,
            limit=limit,
            max_gaps_per_item=max_gaps_per_item,
            priority=priority,
            dry_run=dry_run,
            queue_research_briefs=queue_research_briefs,
        )

    @mcp.tool()
    def build_validation_gap_source_pack(
        queue_item_ids: list[str] | None = None,
        lead_ids: list[str] | None = None,
        lead_statuses: list[str] | None = None,
        source_keys: list[str] | None = None,
        lanes: list[str] | None = None,
        limit: int = 25,
        max_queries_per_lane: int = 3,
        persist_queries: bool = False,
        active: bool = True,
        dry_run: bool = True,
    ) -> dict:
        """Build targeted source-query packs for validation evidence gaps."""

        return build_validation_gap_source_pack_tool(
            queue_item_ids=queue_item_ids,
            lead_ids=lead_ids,
            lead_statuses=lead_statuses,
            source_keys=source_keys,
            lanes=lanes,
            limit=limit,
            max_queries_per_lane=max_queries_per_lane,
            persist_queries=persist_queries,
            active=active,
            dry_run=dry_run,
        )

    @mcp.tool()
    def ingest_validation_gap_source_queries(
        source_keys: list[str] | None = None,
        query_names: list[str] | None = None,
        limit_per_query: int = 5,
        max_queries: int = 50,
        dry_run: bool = True,
    ) -> dict:
        """Ingest only active validation-gap source queries."""

        return ingest_validation_gap_source_queries_tool(
            source_keys=source_keys,
            query_names=query_names,
            limit_per_query=limit_per_query,
            max_queries=max_queries,
            dry_run=dry_run,
        )

    @mcp.tool()
    def queue_research_brief(
        topic: str,
        disease_scope: str = "canine hemangiosarcoma and human angiosarcoma",
        source_key: str | None = None,
        priority: int = 100,
        max_chunks_per_perspective: int = 8,
        max_claims: int = 12,
        max_chunk_chars: int = 1800,
        brief_style: str = "technical",
        model_profile: str = "research_brief",
        review_mode: str = "openrouter_required",
        review_models: list[str] | None = None,
    ) -> dict:
        """Queue a citation-first research brief request."""

        return queue_research_brief_tool(
            topic=topic,
            disease_scope=disease_scope,
            source_key=source_key,
            priority=priority,
            max_chunks_per_perspective=max_chunks_per_perspective,
            max_claims=max_claims,
            max_chunk_chars=max_chunk_chars,
            brief_style=brief_style,
            model_profile=model_profile,
            review_mode=review_mode,
            review_models=review_models,
        )

    @mcp.tool()
    def queue_research_brief_batch(
        mode: str = "both",
        lead_statuses: list[str] | None = None,
        lead_types: list[str] | None = None,
        source_keys: list[str] | None = None,
        source_health_statuses: list[str] | None = None,
        source_health_report: dict | None = None,
        include_empty_sources: bool = False,
        limit: int = 25,
        disease_scope: str = "canine hemangiosarcoma and human angiosarcoma",
        priority: int = 80,
        max_chunks_per_perspective: int = 8,
        max_claims: int = 12,
        max_chunk_chars: int = 1800,
        brief_style: str = "technical",
        model_profile: str = "research_brief",
        review_mode: str = "openrouter_required",
        review_models: list[str] | None = None,
    ) -> dict:
        """Queue research brief requests from watchlist leads and source-health gaps."""

        return queue_research_brief_batch_tool(
            mode=mode,
            lead_statuses=lead_statuses,
            lead_types=lead_types,
            source_keys=source_keys,
            source_health_statuses=source_health_statuses,
            source_health_report=source_health_report,
            include_empty_sources=include_empty_sources,
            limit=limit,
            disease_scope=disease_scope,
            priority=priority,
            max_chunks_per_perspective=max_chunks_per_perspective,
            max_claims=max_claims,
            max_chunk_chars=max_chunk_chars,
            brief_style=brief_style,
            model_profile=model_profile,
            review_mode=review_mode,
            review_models=review_models,
        )

    @mcp.tool()
    def get_research_brief_queue_item(queue_item_id: str) -> dict:
        """Return one queued research brief request."""

        return get_research_brief_queue_item_tool(queue_item_id)

    @mcp.tool()
    def list_research_brief_queue(
        status: str | None = None,
        source_key: str | None = None,
        topic_query: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """List queued research brief requests."""

        return list_research_brief_queue_tool(
            status=status,
            source_key=source_key,
            topic_query=topic_query,
            limit=limit,
        )

    @mcp.tool()
    def run_research_brief_queue(
        statuses: list[str] | None = None,
        source_key: str | None = None,
        topic_query: str | None = None,
        limit: int = 1,
    ) -> dict:
        """Run the next queued research brief request."""

        return run_research_brief_queue_tool(
            statuses=statuses,
            source_key=source_key,
            topic_query=topic_query,
            limit=limit,
        )

    @mcp.tool()
    def requeue_research_brief_queue_item(
        queue_item_id: str,
        priority: int | None = None,
    ) -> dict:
        """Move a failed research brief queue item back to queued."""

        return requeue_research_brief_queue_item_tool(queue_item_id, priority=priority)

    @mcp.tool()
    def archive_research_brief_queue_item(queue_item_id: str) -> dict:
        """Archive a completed research brief queue item."""

        return archive_research_brief_queue_item_tool(queue_item_id)

    @mcp.tool()
    def maintain_research_brief_queue(
        queue_item_ids: list[str] | None = None,
        statuses: list[str] | None = None,
        source_key: str | None = None,
        topic_query: str | None = None,
        min_attempts: int = 1,
        max_updated_age_hours: float = 12.0,
        limit: int = 50,
        dry_run: bool = True,
        reason: str = "stale_research_brief_queue_cleanup",
    ) -> dict:
        """Dry-run or apply safe research brief queue maintenance."""

        return maintain_research_brief_queue_tool(
            queue_item_ids=queue_item_ids,
            statuses=statuses,
            source_key=source_key,
            topic_query=topic_query,
            min_attempts=min_attempts,
            max_updated_age_hours=max_updated_age_hours,
            limit=limit,
            dry_run=dry_run,
            reason=reason,
        )

    @mcp.tool()
    def run_retrieval_smoke(
        query: str = "hemangiosarcoma angiogenesis",
        source_key: str | None = None,
        object_type: str | None = None,
        embedding_model: str | None = None,
        limit: int = 3,
        max_chunk_chars: int = 1200,
        context_window: int = 1,
        include_entity_mentions: bool = True,
        include_keyword_fallback: bool = True,
        require_embedding: bool = False,
    ) -> dict:
        """Smoke-test retrieval by chaining search, chunk context, and object reads."""

        return run_retrieval_smoke_tool(
            query=query,
            source_key=source_key,
            object_type=object_type,
            embedding_model=embedding_model,
            limit=limit,
            max_chunk_chars=max_chunk_chars,
            context_window=context_window,
            include_entity_mentions=include_entity_mentions,
            include_keyword_fallback=include_keyword_fallback,
            require_embedding=require_embedding,
        )

    @mcp.tool()
    def triage_full_text_issue(
        source_key: str,
        stage: str = "qa",
        query_name: str | None = None,
        error_message: str | None = None,
        errors: list[str] | None = None,
        runtime_seconds: float | None = None,
        timeout_seconds: float | None = None,
        raw_records: int = 0,
        research_objects: int = 0,
        document_chunks: int = 0,
        full_text_document_chunks: int = 0,
        full_text_body_chars: int = 0,
        claims: int = 0,
        entity_mentions: int = 0,
        current_failed_runs: list[str] | None = None,
        http_status: int | None = None,
        model_profile: str = "cheap_classifier",
        metadata: dict | None = None,
    ) -> dict:
        """Classify a full-text ingestion edge case into the next operational action."""

        return triage_full_text_issue_tool(
            source_key=source_key,
            stage=stage,
            query_name=query_name,
            error_message=error_message,
            errors=errors,
            runtime_seconds=runtime_seconds,
            timeout_seconds=timeout_seconds,
            raw_records=raw_records,
            research_objects=research_objects,
            document_chunks=document_chunks,
            full_text_document_chunks=full_text_document_chunks,
            full_text_body_chars=full_text_body_chars,
            claims=claims,
            entity_mentions=entity_mentions,
            current_failed_runs=current_failed_runs,
            http_status=http_status,
            model_profile=model_profile,
            metadata=metadata,
        )

    @mcp.tool()
    def run_full_text_ops(
        source_keys: list[str] | None = None,
        partition_date: str | None = None,
        source_health_report: dict | None = None,
        full_text_report: dict | None = None,
        recent_run_limit: int = 10,
        model_profile: str = "reviewer",
        review_mode: str = "openrouter_required",
        review_models: list[str] | None = None,
        dagster_run_id: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        """Run the recommend-only full-text ops agent and persist its agent run."""

        return run_full_text_ops_tool(
            source_keys=source_keys,
            partition_date=partition_date,
            source_health_report=source_health_report,
            full_text_report=full_text_report,
            recent_run_limit=recent_run_limit,
            model_profile=model_profile,
            review_mode=review_mode,
            review_models=review_models,
            dagster_run_id=dagster_run_id,
            metadata=metadata,
        )

    @mcp.tool()
    def run_x_topic_review(
        provider_report: dict | None = None,
        candidates: list[dict] | None = None,
        recent_run_limit: int = 5,
        max_candidates: int = 20,
        model_profile: str = "reviewer",
        review_mode: str = "openrouter_required",
        review_models: list[str] | None = None,
        dagster_run_id: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        """Run the X topic review agent and persist its recommendations."""

        return run_x_topic_review_tool(
            provider_report=provider_report,
            candidates=candidates,
            recent_run_limit=recent_run_limit,
            max_candidates=max_candidates,
            model_profile=model_profile,
            review_mode=review_mode,
            review_models=review_models,
            dagster_run_id=dagster_run_id,
            metadata=metadata,
        )

    @mcp.tool()
    def run_x_linked_article_followup(
        urls: list[str] | None = None,
        recent_run_limit: int = 10,
        max_urls: int = 10,
        fetch: bool = True,
        parse: bool = True,
        approved_by: str | None = None,
        approval_note: str | None = None,
        robots_policy: str = "reviewed",
        metadata: dict | None = None,
    ) -> dict:
        """Fetch and parse controlled article links queued by X topic review."""

        return run_x_linked_article_followup_tool(
            urls=urls,
            recent_run_limit=recent_run_limit,
            max_urls=max_urls,
            fetch=fetch,
            parse=parse,
            approved_by=approved_by,
            approval_note=approval_note,
            robots_policy=robots_policy,
            metadata=metadata,
        )

    @mcp.tool()
    def run_x_linked_article_review(
        review_ids: list[str] | None = None,
        review_status: str | None = "needs_review",
        limit: int = 50,
        model_profile: str = "reviewer",
        review_mode: str = "openrouter_required",
        review_models: list[str] | None = None,
        dagster_run_id: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        """Run the linked-article review agent and persist recommendations."""

        return run_x_linked_article_review_tool(
            review_ids=review_ids,
            review_status=review_status,
            limit=limit,
            model_profile=model_profile,
            review_mode=review_mode,
            review_models=review_models,
            dagster_run_id=dagster_run_id,
            metadata=metadata,
        )

    @mcp.tool()
    def queue_source_followups(
        source_key: str = "x_linked_article",
        review_ids: list[str] | None = None,
        review_status: str | None = None,
        limit: int = 100,
        include_existing: bool = False,
        include_agent_recommendations: bool = True,
        agent_run_limit: int = 20,
    ) -> dict:
        """Queue primary-source follow-ups from parsed records and review-agent recommendations."""

        return queue_source_followups_tool(
            source_key=source_key,
            review_ids=review_ids,
            review_status=review_status,
            limit=limit,
            include_existing=include_existing,
            include_agent_recommendations=include_agent_recommendations,
            agent_run_limit=agent_run_limit,
        )

    @mcp.tool()
    def queue_unpaywall_doi_followups(
        source_keys: list[str] | None = None,
        limit: int = 100,
        include_existing: bool = False,
    ) -> dict:
        """Queue manual Unpaywall DOI enrichment from stored research objects."""

        return queue_unpaywall_doi_followups_tool(
            source_keys=source_keys,
            limit=limit,
            include_existing=include_existing,
        )

    @mcp.tool()
    def list_source_followups(
        source_key: str | None = None,
        status: str | None = None,
        identifier_type: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Return queued primary-source follow-ups."""

        return list_source_followups_tool(
            source_key=source_key,
            status=status,
            identifier_type=identifier_type,
            limit=limit,
        )

    @mcp.tool()
    def collect_research_leads(
        agent_names: list[str] | None = None,
        statuses: list[str] | None = None,
        limit: int = 50,
        include_existing: bool = False,
    ) -> dict:
        """Collect watchlist leads from recent agent runs."""

        return collect_research_leads_tool(
            agent_names=agent_names,
            statuses=statuses,
            limit=limit,
            include_existing=include_existing,
        )

    @mcp.tool()
    def get_research_lead(lead_id: str) -> dict:
        """Return a persisted research watchlist lead."""

        return get_research_lead_tool(lead_id)

    @mcp.tool()
    def list_research_leads(
        status: str | None = None,
        lead_type: str | None = None,
        source_key: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Return persisted research watchlist leads."""

        return list_research_leads_tool(
            status=status,
            lead_type=lead_type,
            source_key=source_key,
            limit=limit,
        )

    @mcp.tool()
    def resolve_research_followups(
        lead_ids: list[str] | None = None,
        statuses: list[str] | None = None,
        source_keys: list[str] | None = None,
        search_source_keys: list[str] | None = None,
        limit: int = 25,
        ingest_source_followups: bool = True,
        search_missing_identifiers: bool = True,
        promote_ready_leads: bool = True,
        run_claim_extraction: bool = True,
        dry_run: bool = False,
        min_evidence_chunks: int = 1,
        search_limit_per_source: int = 2,
        max_search_terms: int = 12,
        approved_by: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        """Resolve evidence-light research leads into durable source evidence."""

        return resolve_research_followups_tool(
            lead_ids=lead_ids,
            statuses=statuses,
            source_keys=source_keys,
            search_source_keys=search_source_keys,
            limit=limit,
            ingest_source_followups=ingest_source_followups,
            search_missing_identifiers=search_missing_identifiers,
            promote_ready_leads=promote_ready_leads,
            run_claim_extraction=run_claim_extraction,
            dry_run=dry_run,
            min_evidence_chunks=min_evidence_chunks,
            search_limit_per_source=search_limit_per_source,
            max_search_terms=max_search_terms,
            approved_by=approved_by,
            metadata=metadata,
        )

    @mcp.tool()
    def ingest_source_followups(
        followup_ids: list[str] | None = None,
        source_keys: list[str] | None = None,
        statuses: list[str] | None = None,
        limit: int = 25,
        approved_by: str | None = None,
        run_claim_extraction: bool = True,
        dry_run: bool = False,
        metadata: dict | None = None,
    ) -> dict:
        """Ingest queued source follow-ups through the primary API harvesters."""

        return ingest_source_followups_tool(
            followup_ids=followup_ids,
            source_keys=source_keys,
            statuses=statuses,
            limit=limit,
            approved_by=approved_by,
            run_claim_extraction=run_claim_extraction,
            dry_run=dry_run,
            metadata=metadata,
        )

    @mcp.tool()
    def get_agent_run(agent_run_id: str) -> dict:
        """Return a persisted agent run."""

        return get_agent_run_tool(agent_run_id)

    @mcp.tool()
    def list_agent_runs(
        agent_name: str | None = None,
        status: str | None = None,
        source_key: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Return recent persisted agent runs."""

        return list_agent_runs_tool(
            agent_name=agent_name,
            status=status,
            source_key=source_key,
            limit=limit,
        )

    @mcp.tool()
    def command_center(
        source_keys: list[str] | None = None,
        include_source_health: bool = True,
        include_recent_agents: bool = True,
        queue_limit: int = 25,
        lead_limit: int = 25,
        agent_run_limit: int = 25,
        min_health_score: float = 0.65,
        require_claims: bool = True,
    ) -> dict:
        """Return the read-only TWOG command-center report."""

        return command_center_tool(
            source_keys=source_keys,
            include_source_health=include_source_health,
            include_recent_agents=include_recent_agents,
            queue_limit=queue_limit,
            lead_limit=lead_limit,
            agent_run_limit=agent_run_limit,
            min_health_score=min_health_score,
            require_claims=require_claims,
        )

    @mcp.tool()
    def agent_performance_report(
        agent_name: str | None = None,
        status: str | None = None,
        source_key: str | None = None,
        limit: int = 500,
        min_sample_size: int = 3,
    ) -> dict:
        """Return hybrid operator/evaluator performance aggregation."""

        return agent_performance_report_tool(
            agent_name=agent_name,
            status=status,
            source_key=source_key,
            limit=limit,
            min_sample_size=min_sample_size,
        )

    @mcp.tool()
    def run_agent_performance_evaluation(
        agent_name: str | None = None,
        status: str | None = "completed",
        source_key: str | None = None,
        limit: int = 25,
        reviewed_only: bool = True,
        model_profile: str = "agent_performance_evaluator",
        review_models: list[str] | None = None,
    ) -> dict:
        """Run OpenRouter specialist evaluators over recent reviewed agent runs."""

        return run_agent_performance_evaluation_tool(
            agent_name=agent_name,
            status=status,
            source_key=source_key,
            limit=limit,
            reviewed_only=reviewed_only,
            model_profile=model_profile,
            review_models=review_models,
        )

    @mcp.tool()
    def curate_claims(
        source_key: str | None = None,
        query: str | None = None,
        limit: int = 100,
        min_confidence: float = 0.0,
        promote_threshold: float = 0.5,
        include_seed_claims: bool = False,
        dry_run: bool = False,
        model_profile: str = "reviewer",
    ) -> dict:
        """Run the claim curator agent to dedupe, review, and promote draft claims."""

        request = ClaimCurationRequest(
            source_key=source_key,
            query=query,
            limit=limit,
            min_confidence=min_confidence,
            promote_threshold=promote_threshold,
            include_seed_claims=include_seed_claims,
            dry_run=dry_run,
            model_profile=model_profile,
        )
        return get_service().curate_claims(request).model_dump(mode="json")

    @mcp.tool()
    def scout_sources(
        focus: str = "all",
        max_phase: int = 3,
        max_recommendations: int = 12,
        include_registered_sources: bool = True,
        include_expansion_sources: bool = True,
        model_profile: str = "long_context_reviewer",
    ) -> dict:
        """Prioritize ingestion source gaps and starter queries."""

        request = SourceScoutRequest(
            focus=focus,  # type: ignore[arg-type]
            max_phase=max_phase,
            max_recommendations=max_recommendations,
            include_registered_sources=include_registered_sources,
            include_expansion_sources=include_expansion_sources,
            model_profile=model_profile,
        )
        return get_service().scout_sources(request).model_dump(mode="json")

    @mcp.tool()
    def get_candidate(
        candidate_id: str | None = None,
        candidate_name: str | None = None,
        include_claims: bool = True,
        include_artifacts: bool = True,
        include_validation: bool = True,
    ) -> dict:
        """Return a candidate dossier with evidence, artifacts, validation state, and risk flags."""

        request = CandidateDossierRequest(
            candidate_id=UUID(candidate_id) if candidate_id else None,
            candidate_name=candidate_name,
            include_claims=include_claims,
            include_artifacts=include_artifacts,
            include_validation=include_validation,
        )
        dossier = get_service().get_candidate(request)
        return {} if dossier is None else dossier.model_dump(mode="json")

    @mcp.tool()
    def propose_hypothesis(
        objective: str,
        claim_ids: list[str] | None = None,
        candidate_name: str | None = None,
        target_name: str | None = None,
        species: str = "canine",
        commit: bool = False,
    ) -> dict:
        """Draft a hypothesis from claims and gaps. By default this does not commit durable state."""

        request = HypothesisProposalRequest(
            objective=objective,
            claim_ids=[UUID(claim_id) for claim_id in (claim_ids or [])],
            candidate_name=candidate_name,
            target_name=target_name,
            species=species,
            commit=commit,
        )
        return get_service().propose_hypothesis(request).model_dump(mode="json")

    @mcp.tool()
    def commit_hypothesis(draft: dict, approved_by: str, approval_note: str | None = None) -> dict:
        """Persist a human-approved hypothesis draft."""

        request = CommitHypothesisRequest(
            draft=HypothesisDraft.model_validate(draft),
            approved_by=approved_by,
            approval_note=approval_note,
        )
        return get_service().commit_hypothesis(request).model_dump(mode="json")

    @mcp.tool()
    def run_boltz(
        target_name: str,
        ligand_smiles: str | None = None,
        ligand_name: str | None = None,
        protein_sequence: str | None = None,
        candidate_id: str | None = None,
        priority: int = 100,
        require_approval: bool = True,
    ) -> dict:
        """Queue a Boltz prediction request and return an async run handle."""

        request = BoltzRunRequest(
            target_name=target_name,
            ligand_smiles=ligand_smiles,
            ligand_name=ligand_name,
            protein_sequence=protein_sequence,
            candidate_id=UUID(candidate_id) if candidate_id else None,
            priority=priority,
            require_approval=require_approval,
        )
        return get_service().run_boltz(request).model_dump(mode="json")

    @mcp.tool()
    def request_validation(
        validation_type: str,
        objective: str,
        candidate_id: str | None = None,
        candidate_name: str | None = None,
        target_name: str | None = None,
        priority: int = 100,
        require_approval: bool = True,
        assay_context: dict | None = None,
        quality_gates: list[str] | None = None,
        metadata: dict | None = None,
    ) -> dict:
        """Queue validation such as docking, MD, ADMET, homology, safety, or expert review."""

        request = ValidationRequest(
            validation_type=validation_type,  # type: ignore[arg-type]
            objective=objective,
            candidate_id=UUID(candidate_id) if candidate_id else None,
            candidate_name=candidate_name,
            target_name=target_name,
            priority=priority,
            require_approval=require_approval,
            assay_context=ValidationAssayContext.model_validate(assay_context) if assay_context else None,
            quality_gates=quality_gates or [],
            metadata=metadata or {},
        )
        return get_service().request_validation(request).model_dump(mode="json")

    @mcp.tool()
    def get_run_status(run_id: str) -> dict:
        """Return status for a Dagster, RunPod, MCP, local, or external async run."""

        handle = get_service().get_run_status(UUID(run_id))
        return {} if handle is None else handle.model_dump(mode="json")

    @mcp.tool()
    def get_artifact(artifact_id: str) -> dict:
        """Return artifact metadata and links."""

        artifact = get_service().get_artifact(UUID(artifact_id))
        return {} if artifact is None else artifact.model_dump(mode="json")

    @mcp.tool()
    def list_model_profiles() -> dict:
        """List simple logical model profiles used by the service layer."""

        profiles = get_service().list_model_profiles()
        return {"profiles": [profile.model_dump(mode="json") for profile in profiles]}

    @mcp.tool()
    def set_model_profile(
        profile_key: str,
        provider: str,
        purpose: str,
        model_name: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> dict:
        """Update a simple model profile without adding a model gateway."""

        profile = ModelProfile(
            profile_key=profile_key,
            provider=provider,  # type: ignore[arg-type]
            model_name=model_name,
            purpose=purpose,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return get_service().set_model_profile(profile).model_dump(mode="json")

    @mcp.resource("claim://{claim_id}")
    def claim_resource(claim_id: str) -> dict:
        """Fetch a claim as an MCP resource."""

        claim = get_service().get_claim(UUID(claim_id))
        return {} if claim is None else claim.model_dump(mode="json")

    @mcp.resource("chunk://{chunk_id}")
    def chunk_resource(chunk_id: str) -> dict:
        """Fetch a retrieval chunk with immediate context as an MCP resource."""

        return get_chunk_context_tool(chunk_id=chunk_id)

    @mcp.resource("research-object://{research_object_id}")
    def research_object_resource(research_object_id: str) -> dict:
        """Fetch a canonical research object as an MCP resource."""

        return get_research_object_tool(research_object_id=research_object_id)

    @mcp.resource("run://{run_id}")
    def run_resource(run_id: str) -> dict:
        """Fetch an async run as an MCP resource."""

        handle = get_service().get_run_status(UUID(run_id))
        return {} if handle is None else handle.model_dump(mode="json")

    @mcp.resource("agent-run://{agent_run_id}")
    def agent_run_resource(agent_run_id: str) -> dict:
        """Fetch a persisted agent run as an MCP resource."""

        return get_agent_run_tool(agent_run_id)

    @mcp.resource("research-lead://{lead_id}")
    def research_lead_resource(lead_id: str) -> dict:
        """Fetch a persisted research watchlist lead as an MCP resource."""

        return get_research_lead_tool(lead_id)

    @mcp.resource("research-brief://{brief_id}")
    def research_brief_resource(brief_id: str) -> dict:
        """Fetch a persisted research brief as an MCP resource."""

        return get_research_brief_tool(brief_id)

    @mcp.resource("research-brief-evaluation://{evaluation_id}")
    def research_brief_evaluation_resource(evaluation_id: str) -> dict:
        """Fetch a persisted research brief synthesis evaluation as an MCP resource."""

        return get_research_brief_evaluation_tool(evaluation_id)

    @mcp.resource("validation-plan://{plan_id}")
    def validation_plan_resource(plan_id: str) -> dict:
        """Fetch a persisted recommend-only validation plan as an MCP resource."""

        return get_validation_plan_tool(plan_id)

    @mcp.resource("validation-request-queue://{queue_item_id}")
    def validation_request_queue_resource(queue_item_id: str) -> dict:
        """Fetch a queued validation request as an MCP resource."""

        return get_validation_request_queue_item_tool(queue_item_id)

    @mcp.resource("research-brief-queue://{queue_item_id}")
    def research_brief_queue_resource(queue_item_id: str) -> dict:
        """Fetch a queued research brief request as an MCP resource."""

        return get_research_brief_queue_item_tool(queue_item_id)

    @mcp.resource("artifact://{artifact_id}")
    def artifact_resource(artifact_id: str) -> dict:
        """Fetch artifact metadata as an MCP resource."""

        artifact = get_service().get_artifact(UUID(artifact_id))
        return {} if artifact is None else artifact.model_dump(mode="json")


def main() -> None:
    """Run the MCP server using streamable HTTP."""

    if mcp is None:
        raise RuntimeError('Install the MCP SDK first: pip install "mcp[cli]"')
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
