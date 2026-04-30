"""Service layer for Ingestion Bridge v2.

The service is the boundary shared by MCP tools, Dagster assets, the future
TWOG dashboard, and tests. Keep business rules here rather than inside MCP
decorators or Dagster assets.
"""

from __future__ import annotations

from uuid import UUID, uuid4

from .contracts import (
    AgentRunRecord,
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
    CommitHypothesisRequest,
    DoiOpenAccessFollowupQueueRequest,
    DocumentChunk,
    FullTextTriageRequest,
    FullTextTriageResult,
    FullTextOpsRequest,
    FullTextOpsResult,
    HypothesisDraft,
    HypothesisProposalRequest,
    ModelProfile,
    ResearchBriefEvaluationRecord,
    ResearchBriefEvaluationRequest,
    ResearchBriefEvaluationResult,
    ResearchBriefQueueItem,
    ResearchBriefQueueRequest,
    ResearchBriefQueueRunRequest,
    ResearchBriefQueueRunResult,
    ResearchBriefRecord,
    ResearchBriefPlaygroundPack,
    ResearchBriefRequest,
    ResearchBriefResult,
    ResearchChunkSearchRequest,
    ResearchChunkSearchResult,
    ResearchChunkSearchResults,
    ResearchLeadCollectRequest,
    ResearchLeadCollectResult,
    ResearchLeadRecord,
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
    ValidationPlanRecord,
    ValidationPlanRequest,
    ValidationPlanResult,
    ValidationRequest,
    XLinkedArticleReviewRequest,
    XLinkedArticleReviewResult,
    XTopicReviewRequest,
    XTopicReviewResult,
    XLinkedArticleFollowupRequest,
    XLinkedArticleFollowupResult,
)
from .agent_runner import AgentRunner
from .claim_curator import ClaimCuratorAgent
from .embeddings import LOCAL_HASH_EMBEDDING_MODEL, LocalDeterministicEmbeddingProvider
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
from .repository import ResearchRepository
from .research_leads import collect_research_leads_from_agent_runs, persist_research_leads_from_agent_result
from .validation_planning import (
    VALIDATION_PLANNING_AGENT_NAME,
    VALIDATION_PLANNING_AGENT_VERSION,
    plan_validation_from_research_brief,
    summarize_validation_plan,
    validation_plan_record_from_result,
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
}


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

    def run_next_research_brief_queue_item(
        self,
        request: ResearchBriefQueueRunRequest,
    ) -> ResearchBriefQueueRunResult:
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
            return max(coverage.embedding_models.items(), key=lambda item: item[1])[0]
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
        provider = LocalDeterministicEmbeddingProvider(
            embedding_model=embedding_model,
            dimensions=dimensions or LocalDeterministicEmbeddingProvider().embedding_dimensions,
        )
        return provider.embed_text(request.query)

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
        status="completed",
        final_brief=result.final_brief,
        summary=summarize_research_brief(result),
        result_payload=result.model_dump(mode="json"),
        citation_count=len(result.citations),
        finding_count=finding_count,
        hypothesis_count=len(result.ranked_hypotheses),
        unresolved_question_count=len(result.unresolved_questions),
        research_lead_count=int(result.evidence.get("research_lead_count", 0)),
        error_count=len(result.errors),
        metadata={key: value for key, value in metadata.items() if value not in (None, [], {})},
    )


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
