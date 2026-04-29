"""MCP server for HSA AutoResearch.

Run locally:

    python -m hsa_research.ingestion_bridge.mcp_server

Then connect Claude Code or an MCP inspector to the streamable HTTP server.
"""

from __future__ import annotations

from uuid import UUID

from .contracts import (
    BoltzRunRequest,
    CandidateDossierRequest,
    ChunkContextRequest,
    ClaimCurationRequest,
    ClaimSearchRequest,
    CommitHypothesisRequest,
    DoiOpenAccessFollowupQueueRequest,
    FullTextTriageRequest,
    FullTextOpsRequest,
    HypothesisDraft,
    HypothesisProposalRequest,
    ModelProfile,
    ResearchBriefRequest,
    ResearchChunkSearchRequest,
    ResearchObjectReadRequest,
    RetrievalSmokeRequest,
    SourceScoutRequest,
    SourceFollowupIngestRequest,
    SourceFollowupQueueRequest,
    ValidationRequest,
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
    review_mode: str = "external_required",
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
    review_mode: str = "deterministic_only",
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


def ingest_source_followups_tool(
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
        review_mode: str = "external_required",
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
        review_mode: str = "deterministic_only",
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
    def ingest_source_followups(
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
