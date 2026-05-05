import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
import xml.etree.ElementTree as ET
from uuid import uuid4

import pytest
from pydantic import ValidationError

from hsa_research.ingestion_bridge import dagster_assets as dagster_asset_module
from hsa_research.ingestion_bridge import cli as cli_module
from hsa_research.ingestion_bridge import command_center_web
from hsa_research.ingestion_bridge import entity_resolution
from hsa_research.ingestion_bridge import source_query_params
from hsa_research.ingestion_bridge import storage, structured_orchestration
from hsa_research.ingestion_bridge.contracts import (
    AgentFindingEscalationRequest,
    AgentFindingEscalationResult,
    AgentPerformanceEvaluationRequest,
    AgentPerformanceEvaluationResult,
    AgentPerformanceReportRequest,
    AgentPerformanceReportResult,
    AgentPerformanceRow,
    AgentRunRecord,
    AgentRunReviewRecord,
    BoltzRunRequest,
    CandidateDossierRequest,
    ChunkContextRequest,
    ClaimDirection,
    ClaimSearchRequest,
    ClaimSearchResult,
    ClaimType,
    CommandCenterRequest,
    CommitHypothesisRequest,
    DoiOpenAccessFollowupQueueRequest,
    DocumentChunk,
    EvidenceFitAssessment,
    EvidenceGapResolverRequest,
    EvidenceGapResolverResult,
    EmbeddingCoverageSummary,
    EntityMention,
    EvidenceLevel,
    FullTextTriageRequest,
    FullTextOpsAction,
    FullTextOpsRequest,
    FullTextOpsResult,
    HypothesisProposalRequest,
    IngestionResult,
    RawSourceRecord,
    ResearchChunkSearchRequest,
    ResearchObject,
    ResearchObjectType,
    ResearchObjectReadRequest,
    ResearchBriefCitation,
    ResearchBriefEvaluationRecord,
    ResearchBriefEvaluationRequest,
    ResearchBriefEvaluationResult,
    ResearchBriefFollowupQueueRequest,
    ResearchBriefFollowupQueueResult,
    ResearchBriefFinding,
    ResearchBriefPerspectiveReport,
    ResearchBriefQualityReportRequest,
    ResearchBriefQueueBatchRequest,
    ResearchBriefQueueItem,
    ResearchBriefQueueMaintenanceRequest,
    ResearchBriefQueueRequest,
    ResearchBriefQueueRunRequest,
    ResearchBriefRecord,
    ResearchBriefRequest,
    ResearchBriefResult,
    ResearchFollowupLeadResult,
    ResearchFollowupLoopRequest,
    ResearchFollowupLoopResult,
    ResearchFollowupRefinementRequest,
    ResearchFollowupResolverRequest,
    ResearchFollowupResolverResult,
    ResearchLeadCollectRequest,
    ResearchLeadRecord,
    RetrievalSmokeRequest,
    ScrapeFetchRequest,
    ScrapeIngestRequest,
    ScrapeManifestFetchRequest,
    ScrapeManifestRequest,
    ScrapeProfileReviewRequest,
    ScrapeReviewRecord,
    ScrapeReviewRequest,
    ScrapeSourceProfile,
    SourceFollowupIngestRequest,
    SourceFollowupQueueItem,
    SourceFollowupQueueRequest,
    SourceQuery,
    TextEmbedding,
    TextEmbeddingSearchRequest,
    TherapyCommitteeRequest,
    TherapyCommitteeResult,
    TherapyCommitteeValidationQueueRequest,
    TherapyCommitteeValidationQueueResult,
    TherapyIdea,
    ValidationAgentResult,
    ValidationPlanRecord,
    ValidationPlanRequest,
    ValidationPlanResult,
    ValidationPlanTask,
    ValidationAssayContext,
    ValidationAutopilotRequest,
    ValidationGapSourceIngestRequest,
    ValidationGapSourcePackRequest,
    ValidationGapSourcePackResult,
    ValidationGapSourceQuery,
    ValidationRequest,
    ValidationRequestQueueItem,
    ValidationRequestQueueRequest,
    ClaimCurationRequest,
    SourceScoutRequest,
    RunStatus,
    XLinkedArticleReviewAction,
    XLinkedArticleReviewRequest,
    XLinkedArticleReviewResult,
    XLinkedArticleFollowupRequest,
    XLinkedArticleFollowupResult,
    XTopicLinkedSource,
    XTopicReviewAction,
    XTopicReviewRequest,
    XTopicReviewResult,
)
from hsa_research.ingestion_bridge.backfill import backfill_deep_dives, backfill_papers_json
from hsa_research.ingestion_bridge.claim_curator import ClaimCuratorAgent
from hsa_research.ingestion_bridge.claim_extractor import LocalRuleClaimExtractor, extract_claims_for_repository
from hsa_research.ingestion_bridge.chunker import chunk_text
from hsa_research.ingestion_bridge.full_text_triage import FullTextTriageAgent
from hsa_research.ingestion_bridge import agent_performance
from hsa_research.ingestion_bridge import full_text_ops
from hsa_research.ingestion_bridge import research_brief_evaluation
from hsa_research.ingestion_bridge import research_followup_resolver
from hsa_research.ingestion_bridge import research_brief_agent
from hsa_research.ingestion_bridge import therapy_committee
from hsa_research.ingestion_bridge import source_followup
from hsa_research.ingestion_bridge.full_text_ops import FullTextOpsAgent
from hsa_research.ingestion_bridge.dagster_assets import (
    ALL_API_SMOKE_KEYS,
    HOSTED_API_REPORT_KEYS,
    LITERATURE_CLINICAL_SMOKE_KEYS,
    LITERATURE_CORPUS_SOURCE_KEYS,
    LITERATURE_CORPUS_SOURCE_LIMITS,
    LITERATURE_FULL_TEXT_SOURCE_KEYS,
    LITERATURE_FULL_TEXT_SOURCE_LIMITS,
)
from hsa_research.ingestion_bridge.dagster_resources import ResearchRepositoryResource
from hsa_research.ingestion_bridge.entity_resolution import normalize_entity_key, resolve_entities_for_repository
from hsa_research.ingestion_bridge.embedding_bakeoff import EmbeddingBenchmark, run_embedding_bakeoff
from hsa_research.ingestion_bridge.embeddings import (
    EmbeddingIndexResult,
    EmbeddingMaintenanceResult,
    LocalDeterministicEmbeddingProvider,
    OpenRouterEmbeddingProvider,
    build_chunk_embedding_text,
    build_embedding_provider,
    default_embedding_model_for_environment,
    index_embeddings_for_repository,
    maintain_embedding_index,
    select_embedding_model_from_coverage,
)
from hsa_research.ingestion_bridge import harvesters_v2
from hsa_research.ingestion_bridge.harvesters_v2 import (
    AVMAVCTRHarvesterV2,
    ChEMBLHarvesterV2,
    ClinicalTrialsGovHarvesterV2,
    CrossrefHarvesterV2,
    EuropePMCHarvesterV2,
    GEOHarvesterV2,
    HARVESTERS_V2,
    ICDCHarvesterV2,
    OpenAlexHarvesterV2,
    OpenFDAAnimalEventsHarvesterV2,
    PMCOAHarvesterV2,
    PubChemHarvesterV2,
    PubMedHarvesterV2,
    RCSBPDBHarvesterV2,
    SRAHarvesterV2,
    UniProtHarvesterV2,
    UnpaywallHarvesterV2,
)
from hsa_research.ingestion_bridge.local_ingest import LocalIngestionPipeline
from hsa_research.ingestion_bridge import local_ingest as local_ingest_module
from hsa_research.ingestion_bridge.local_store import SQLiteResearchRepository
from hsa_research.ingestion_bridge import mcp_server
from hsa_research.ingestion_bridge.query_policy import build_scholarly_source_queries, infer_comparative_scope
from hsa_research.ingestion_bridge.repository import InMemoryResearchRepository
from hsa_research.ingestion_bridge import scraper_bridge
from hsa_research.ingestion_bridge import x_topic_monitor
from hsa_research.ingestion_bridge import x_topic_review
from hsa_research.ingestion_bridge.scraper_bridge import ScrapeBridge, list_scrape_profiles
from hsa_research.ingestion_bridge import service as service_module
from hsa_research.ingestion_bridge import validation_gap_ingest
from hsa_research.ingestion_bridge.service import HSAResearchService
from hsa_research.ingestion_bridge.source_scout import SourceScoutAgent
from hsa_research.ingestion_bridge.source_health import build_source_health_report
from hsa_research.ingestion_bridge.structured_orchestration import (
    build_structured_source_count_report,
    full_text_source_qa,
    run_structured_sources_pipeline,
    structured_source_qa,
)


def make_service(tmp_path):
    return HSAResearchService(SQLiteResearchRepository(tmp_path / "hsa.sqlite3"))


def _contains_key(value, key: str) -> bool:
    if isinstance(value, dict):
        return key in value or any(_contains_key(item, key) for item in value.values())
    if isinstance(value, list):
        return any(_contains_key(item, key) for item in value)
    return False


def test_research_repository_resource_uses_hsa_sqlite_path(monkeypatch, tmp_path):
    db_path = tmp_path / "dagster-resource.sqlite3"
    monkeypatch.setenv("HSA_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("HSA_SQLITE_PATH", str(db_path))
    monkeypatch.delenv("HSA_DATABASE_URL", raising=False)

    repo = ResearchRepositoryResource().build_repository()
    try:
        assert isinstance(repo, SQLiteResearchRepository)
        assert repo.db_path == db_path
        assert repo.coverage_summary()["claims"] >= 1
    finally:
        repo.conn.close()


def test_research_repository_resource_rejects_memory_backend():
    with pytest.raises(RuntimeError, match="sqlite or postgres storage"):
        ResearchRepositoryResource(storage_backend="memory").build_repository()


def test_agent_run_and_full_text_ops_contracts_validate():
    record = AgentRunRecord(
        agent_name="full_text_ops_agent",
        model_profile="reviewer",
        status=RunStatus.RUNNING,
        source_key="europe_pmc",
        input_payload={"source_keys": ["europe_pmc"]},
    )
    action = FullTextOpsAction(
        source_key="europe_pmc",
        action="run_source_date_partition",
        severity="watch",
        reason="Partition evidence is missing.",
        dagster_job_name="literature_full_text_source_date_job",
        partition_date="2026-04-27",
    )
    result = FullTextOpsResult(actions=[action], schedule_readiness="needs_partition_validation")

    assert record.status == "running"
    assert result.actions[0].action == "run_source_date_partition"
    with pytest.raises(ValueError):
        FullTextOpsAction(source_key="europe_pmc", action="bad", severity="watch", reason="bad")
    with pytest.raises(ValueError):
        FullTextOpsAction(source_key="europe_pmc", action="mark_clean", severity="bad", reason="bad")
    with pytest.raises(ValueError):
        AgentRunRecord(agent_name="x", status="bad")


def test_validation_plan_contracts_validate():
    assay_context = ValidationAssayContext(
        disease_context="canine hemangiosarcoma and human angiosarcoma",
        species=["canine", "human", "canine"],
        model_system="Human-reviewed literature packet.",
        assay_type="structured expert evidence review",
        readout="ready/not-ready decision",
        evidence_refs=["brief:1", "C1"],
    )
    validation_request = ValidationRequest(
        validation_type="expert_review",
        objective="Review the hypothesis before validation.",
        require_approval=True,
        assay_context=assay_context,
        quality_gates=["approval_required", "approval_required"],
    )
    task = ValidationPlanTask(
        task_type="expert_review",
        title="Expert review",
        objective="Assess whether this should move into validation.",
        rationale="The brief is source-traceable and ready for review.",
        validation_request=validation_request,
        evidence_refs=["brief:1", "evaluation:1", "C1"],
    )
    result = ValidationPlanResult(
        brief_id=uuid4(),
        topic="VEGF in canine HSA",
        status="ready_for_review",
        readiness="ready_for_expert_review",
        tasks=[task],
    )
    record = ValidationPlanRecord(
        plan_id=result.plan_id,
        brief_id=result.brief_id,
        topic=result.topic,
        status=result.status,
        readiness=result.readiness,
        task_count=1,
        result_payload=result.model_dump(mode="json"),
    )

    assert record.status == "ready_for_review"
    assert result.tasks[0].validation_request.validation_type == "expert_review"
    assert result.tasks[0].validation_request.assay_context.species == ["canine", "human"]
    assert result.tasks[0].validation_request.quality_gates == ["approval_required"]
    with pytest.raises(ValueError):
        ValidationPlanTask(task_type="bad", title="x", objective="x", rationale="x")
    with pytest.raises(ValueError):
        ValidationPlanResult(brief_id=uuid4(), topic="x", status="bad")
    with pytest.raises(ValueError):
        ValidationPlanResult(brief_id=uuid4(), topic="x", readiness="bad")


def test_validation_request_queue_contracts_validate():
    plan_id = uuid4()
    task_id = uuid4()
    item = ValidationRequestQueueItem(
        plan_id=plan_id,
        task_id=task_id,
        brief_id=uuid4(),
        topic="VEGF validation path",
        task_type="expert_review",
        title="Review target validation",
        objective="Review whether this target is ready for validation.",
        rationale="The plan is source-traceable.",
        validation_request=ValidationRequest(
            validation_type="expert_review",
            objective="Review whether this target is ready for validation.",
        ),
    )
    request = ValidationRequestQueueRequest(plan_id=plan_id, task_ids=[task_id])

    assert item.status == "needs_approval"
    assert item.identity_key == f"validation_request_queue:{plan_id}:{task_id}"
    assert request.dry_run is True
    autopilot_request = ValidationAutopilotRequest(
        allowed_task_types=["expert_review", "expert_review"],
        allowed_validation_types=["expert_review", "Expert Review"],
        source_keys=["PubMed", "pubmed"],
    )
    assert autopilot_request.dry_run is True
    assert autopilot_request.max_per_run == 2
    assert autopilot_request.allowed_task_types == ["expert_review"]
    assert autopilot_request.allowed_validation_types == ["expert_review"]
    assert autopilot_request.source_keys == ["pubmed"]
    with pytest.raises(ValueError):
        ValidationAutopilotRequest(allowed_task_types=["wet_magic"])
    source_pack_request = ValidationGapSourcePackRequest(
        source_keys=["PubMed", "pubmed"],
        lanes=["safety_signal"],
    )
    assert source_pack_request.source_keys == ["pubmed"]
    assert source_pack_request.lanes == ["safety_signal"]
    source_query = ValidationGapSourceQuery(
        lane="safety_signal",
        source_key="pubmed",
        query_name="validation_gap_safety_pubmed",
        query_text="sorafenib AND canine AND safety",
        reason="Need direct canine tolerability evidence.",
        required_terms=["sorafenib", "sorafenib"],
    )
    assert source_query.required_terms == ["sorafenib"]
    assert source_query.as_source_query().track == "validation_gap"
    with pytest.raises(ValueError):
        ValidationGapSourcePackRequest(lanes=["bad_lane"])
    ingest_request = ValidationGapSourceIngestRequest(
        source_keys=["PubMed", "pubmed"],
        query_names=["gap_a", "gap_a"],
    )
    assert ingest_request.source_keys == ["pubmed"]
    assert ingest_request.query_names == ["gap_a"]
    agent_result = ValidationAgentResult(
        queue_item_id=item.queue_item_id,
        plan_id=plan_id,
        task_id=task_id,
        task_type="expert_review",
        validation_type="expert_review",
        agent_name="evidence_review_validation_agent",
        model_profile="deterministic_only",
        decision="hold",
        confidence=0.51,
        summary="Evidence needs expert review before promotion.",
        evidence_used=["C1", "C1"],
    )
    assert agent_result.evidence_used == ["C1"]
    with pytest.raises(ValueError):
        ValidationRequestQueueItem(
            status="bad",
            plan_id=plan_id,
            task_id=task_id,
            brief_id=uuid4(),
            topic="VEGF validation path",
            task_type="expert_review",
            title="Review target validation",
            objective="Review whether this target is ready for validation.",
            rationale="The plan is source-traceable.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review whether this target is ready for validation.",
            ),
        )
    with pytest.raises(ValueError):
        ValidationAgentResult(
            queue_item_id=item.queue_item_id,
            plan_id=plan_id,
            task_id=task_id,
            task_type="expert_review",
            validation_type="expert_review",
            agent_name="evidence_review_validation_agent",
            model_profile="deterministic_only",
            decision="bad",
            summary="bad",
        )


def test_x_linked_article_followup_contracts_validate():
    request = XLinkedArticleFollowupRequest(
        urls=["https://cancer.ufl.edu/article"],
        approved_by="unit-test",
        max_urls=1,
    )
    result = XLinkedArticleFollowupResult(
        candidate_urls=request.urls,
        primary_source_links=[
            {
                "recommended_source_key": "crossref",
                "identifier_type": "doi",
                "identifier": "10.1234/test",
                "url": "https://doi.org/10.1234/test",
                "should_ingest": True,
            }
        ],
    )

    assert request.robots_policy == "reviewed"
    assert result.source_key == "x_linked_article"
    assert result.candidate_results == []
    assert result.primary_source_links[0]["recommended_source_key"] == "crossref"
    with pytest.raises(ValueError):
        XLinkedArticleFollowupRequest(max_urls=0)
    with pytest.raises(ValueError):
        XLinkedArticleFollowupRequest(robots_policy="bad")


def test_source_followup_and_linked_article_review_contracts_validate():
    item = SourceFollowupQueueItem(
        source_key="crossref",
        identifier_type="doi",
        identifier="10.1234/Test",
        origin_source_key="x_linked_article",
    )
    action = XLinkedArticleReviewAction(
        review_id=uuid4(),
        source_record_id="article-1",
        action="queue_primary_source_followup",
        severity="watch",
        reason="Primary DOI found.",
        followup_links=[
            XTopicLinkedSource(
                url="https://doi.org/10.1234/test",
                recommended_source_key="crossref",
                identifier_type="doi",
                identifier="10.1234/test",
                should_ingest=True,
                reason="DOI.",
            )
        ],
    )
    result = XLinkedArticleReviewResult(actions=[action])

    assert item.identifier == "10.1234/test"
    assert item.identity_key == "crossref:doi:10.1234/test"
    tracked_item = SourceFollowupQueueItem(
        source_key="crossref",
        identifier_type="doi",
        identifier="10.3389/fvets.2026.1778366?utm_source=twitter#section",
    )
    assert tracked_item.identifier == "10.3389/fvets.2026.1778366"
    assert SourceFollowupQueueRequest().source_key == "x_linked_article"
    assert SourceFollowupIngestRequest().statuses == ["queued", "approved"]
    assert XLinkedArticleReviewRequest().review_mode == "openrouter_required"
    assert result.actions[0].action == "queue_primary_source_followup"
    with pytest.raises(ValueError):
        SourceFollowupQueueItem(source_key="crossref", identifier_type="bad", identifier="x")
    with pytest.raises(ValueError):
        XLinkedArticleReviewAction(review_id=uuid4(), source_record_id="x", action="bad", severity="watch", reason="bad")


def test_research_lead_contracts_validate_and_normalize_identity():
    lead = ResearchLeadRecord(
        title="  AACR HSA Abstract  ",
        url="https://example.edu/news/hsa#section",
        lead_type="institutional_article",
        status="new",
        topic_tags=["HSA", "hsa", "Angiosarcoma"],
        suggested_sources=["PubMed", "pubmed"],
    )

    assert lead.title == "AACR HSA Abstract"
    assert lead.url == "https://example.edu/news/hsa"
    assert lead.identity_key == "research_lead:url:https://example.edu/news/hsa"
    assert lead.topic_tags == ["hsa", "angiosarcoma"]
    assert lead.suggested_sources == ["pubmed"]
    assert ResearchLeadCollectRequest().agent_names == ["x_linked_article_review_agent", "x_topic_review_agent"]
    with pytest.raises(ValueError):
        ResearchLeadRecord(title="x", lead_type="bad")
    with pytest.raises(ValueError):
        ResearchLeadRecord(title="x", status="bad")


def test_research_followup_resolver_contracts_validate():
    lead_id = uuid4()
    request = ResearchFollowupResolverRequest(
        lead_ids=[lead_id],
        statuses=["followup"],
        search_source_keys=["pubmed"],
        limit=1,
        min_evidence_chunks=1,
    )
    lead_result = ResearchFollowupLeadResult(
        lead_id=lead_id,
        status_before="followup",
        status_after="watching",
        actions=["promoted_to_watching"],
        evidence_refs=["chunk:1"],
        durable_source_keys=["pubmed"],
        promoted=True,
    )
    result = ResearchFollowupResolverResult(
        leads_seen=1,
        promoted_leads=1,
        lead_results=[lead_result],
    )

    assert request.statuses == ["followup"]
    assert request.force_live_search is False
    assert request.inspect_evidence_refs is True
    assert result.lead_results[0].promoted is True
    with pytest.raises(ValueError):
        ResearchFollowupResolverRequest(limit=0)
    with pytest.raises(ValueError):
        ResearchFollowupLeadResult(
            lead_id=lead_id,
            status_before="followup",
            status_after="bad",
        )
    with pytest.raises(ValueError):
        ResearchFollowupLeadResult(
            lead_id=lead_id,
            status_before="followup",
            status_after="watching",
            actions=["bad"],
        )


def test_agent_run_repository_roundtrip_sqlite_and_memory(tmp_path):
    sqlite_repo = SQLiteResearchRepository(tmp_path / "agent-runs.sqlite3", seed=False)
    memory_repo = InMemoryResearchRepository()

    for repo in (sqlite_repo, memory_repo):
        record = repo.create_agent_run(
            AgentRunRecord(
                agent_name="full_text_ops_agent",
                model_profile="reviewer",
                status=RunStatus.RUNNING,
                source_key="europe_pmc",
                partition_date="2026-04-27",
                input_payload={"source_keys": ["europe_pmc"]},
            )
        )
        finished = repo.finish_agent_run(
            record.agent_run_id,
            status="completed",
            output_payload={"schedule_readiness": "ready_to_enable"},
            summary={"actions": 1},
            errors=[],
        )

        assert finished is not None
        assert finished.status == "completed"
        assert finished.completed_at is not None
        assert repo.get_agent_run(record.agent_run_id).summary == {"actions": 1}
        assert repo.list_agent_runs(agent_name="full_text_ops_agent", status="completed", source_key="europe_pmc")
        assert repo.list_agent_runs(agent_name="source_scout_agent") == []


def test_agent_run_review_repository_roundtrip_sqlite_and_memory(tmp_path):
    sqlite_repo = SQLiteResearchRepository(tmp_path / "agent-run-reviews.sqlite3", seed=False)
    memory_repo = InMemoryResearchRepository()

    for repo in (sqlite_repo, memory_repo):
        run = repo.create_agent_run(AgentRunRecord(agent_name="therapy_committee_chair_agent"))
        review = repo.create_agent_run_review(
            AgentRunReviewRecord(
                agent_run_id=run.agent_run_id,
                reviewer=" operator ",
                reviewer_type="operator",
                verdict="needs_followup",
                feedback="  Need mutation-function evidence. ",
                tags=["KDR", "kdr", "omics"],
                followup_actions=["queue_research", "queue_research"],
            )
        )

        assert review.reviewer == "operator"
        assert review.reviewer_type == "operator"
        assert review.feedback == "Need mutation-function evidence."
        assert review.tags == ["kdr", "omics"]
        assert review.followup_actions == ["queue_research"]
        assert repo.get_agent_run_review(review.review_id).verdict == "needs_followup"
        assert repo.list_agent_run_reviews(agent_run_id=run.agent_run_id, verdict="needs_followup", reviewer="operator")
        assert repo.list_agent_run_reviews(verdict="bad") == []

    with pytest.raises(ValidationError):
        AgentRunReviewRecord(agent_run_id=uuid4(), verdict="wrong")
    with pytest.raises(ValidationError):
        AgentRunReviewRecord(agent_run_id=uuid4(), reviewer_type="robot", verdict="useful")


def test_agent_finding_escalation_contracts_validate_allowed_values():
    request = AgentFindingEscalationRequest(verdicts=["bad"], source_keys=["pubmed"], limit=5)
    result = AgentFindingEscalationResult(dry_run=True)

    assert request.verdicts == ["bad"]
    assert result.agent_name == "agent_finding_escalation_agent"

    with pytest.raises(ValidationError):
        AgentFindingEscalationRequest(verdicts=["wrong"])
    with pytest.raises(ValidationError):
        AgentFindingEscalationRequest(limit=0)


def test_evidence_fit_assessment_contracts_validate_allowed_values():
    assessment = EvidenceFitAssessment(
        fit="strong",
        matched_terms=["sorafenib", "sorafenib"],
        missing_terms=[],
        required_terms=["sorafenib", "canine/dog/veterinary"],
        matched_required_count=2,
        total_required_count=2,
        source_keys=["PubMed"],
        chunk_count=3,
        reason="Matched the critical follow-up concepts.",
    )

    assert assessment.fit == "strong"
    assert assessment.matched_terms == ["sorafenib"]
    assert assessment.source_keys == ["pubmed"]

    with pytest.raises(ValidationError):
        EvidenceFitAssessment(fit="great")
    with pytest.raises(ValidationError):
        EvidenceFitAssessment(matched_required_count=2, total_required_count=1)


def test_agent_finding_escalation_creates_research_lead_and_source_queries(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-finding-escalation.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead_id = uuid4()
    run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="research_followup_resolver_agent",
            status=RunStatus.COMPLETED,
            source_key="pubmed",
            output_payload={
                "lead_results": [
                    {
                        "lead_id": str(lead_id),
                        "title": "Sorafenib canine dose escalation",
                        "durable_source_keys": ["pubmed", "clinicaltrials_gov"],
                        "evidence_refs": ["chunk:one"],
                    }
                ],
                "evidence_refs": ["chunk:one"],
            },
            summary={"blocked": True, "unresolved_lead_ids": 0},
        )
    )
    review = repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="bad",
            feedback="Retrieved records do not address canine sorafenib DLT/MTD.",
            followup_actions=[
                "rerun_search_with_refined_terms:_'sorafenib_canine_maximum_tolerated_dose',_'sorafenib_veterinary_phase_i_dog',_'robat_sorafenib_dog'",
                "manually_ingest_known_sorafenib_canine_dose-escalation_papers",
            ],
            metadata={"confidence": 0.9},
        )
    )

    result = service.escalate_agent_findings(
        AgentFindingEscalationRequest(review_ids=[review.review_id], operator="operator")
    )
    source_queries = repo.list_source_queries(active_only=True)
    source_keys = {query.source_key for query in source_queries}
    persisted_leads = repo.list_research_leads(status="followup", limit=10)
    escalation_run = repo.get_agent_run(result.agent_run_id)

    assert result.escalated_count == 1
    assert result.research_leads_created == 1
    assert result.source_queries_created >= 5
    assert persisted_leads[0].origin_review_id == review.review_id
    assert persisted_leads[0].origin_agent_run_id == run.agent_run_id
    assert persisted_leads[0].status == "followup"
    assert "sorafenib" in persisted_leads[0].topic_tags
    assert {"pubmed", "europe_pmc", "openalex", "clinicaltrials_gov", "icdc", "openfda_animal_events"}.issubset(source_keys)
    assert "avma_vctr" not in source_keys
    assert all(query.track == "validation_gap" for query in source_queries)
    assert all(query.query_params["followup_lane"] == "agent_evaluator_followup" for query in source_queries)
    assert any("sorafenib canine maximum tolerated dose" in query.query_text for query in source_queries)
    assert escalation_run is not None
    assert escalation_run.status == RunStatus.COMPLETED
    assert escalation_run.summary["research_leads_created"] == 1


def test_agent_finding_escalation_dry_run_does_not_persist(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-finding-escalation-dry-run.sqlite3", seed=False)
    service = HSAResearchService(repo)
    run = repo.create_agent_run(AgentRunRecord(agent_name="research_followup_resolver_agent", status=RunStatus.COMPLETED))
    review = repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="needs_followup",
            feedback="Needs a refined PubMed query.",
        )
    )

    result = service.escalate_agent_findings(
        AgentFindingEscalationRequest(review_ids=[review.review_id], dry_run=True)
    )

    assert result.escalated_count == 1
    assert result.research_leads_created == 0
    assert result.source_queries_created == 0
    assert result.research_leads
    assert result.source_queries
    assert repo.list_research_leads(limit=10) == []
    assert repo.list_source_queries(active_only=False) == []


def test_research_followup_refinement_creates_refined_source_queries(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-refinement.sqlite3", seed=False)
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine DLT",
            status="followup",
            priority=5,
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            topic_tags=["sorafenib", "canine", "safety", "mtd", "dlt"],
            metadata={"created_by": "agent_finding_escalation_agent"},
        )
    )
    resolver_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="research_followup_resolver_agent",
            status=RunStatus.COMPLETED,
            source_key="pubmed",
            output_payload={
                "lead_results": [
                    {
                        "lead_id": str(lead.lead_id),
                        "title": lead.title,
                        "durable_source_keys": ["pubmed"],
                        "metadata": {"evidence_fit": {"fit": "weak", "missing_terms": ["sorafenib"]}},
                    }
                ]
            },
        )
    )
    evaluator_review = repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=resolver_run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="needs_followup",
            feedback="Retry PubMed with more specific sorafenib canine DLT terms.",
            followup_actions=[
                "retry_pubmed_search_with_refined_terms:_'sorafenib_canine_dose-limiting_toxicity'_or_'sorafenib_dog_maximum_tolerated_dose'",
                "search_for_known_sorafenib_veterinary_oncology_studies_(e.g.,_robat_et_al._2012,_london_et_al.)_by_pmid_for_direct_ingestion",
                "increase_limit_per_source_to_at_least_5_and_add_source_keys_veterinary_databases_e.g._cab_abstracts_vetmed_resource",
            ],
        )
    )

    result = service.refine_research_followups(
        ResearchFollowupRefinementRequest(lead_ids=[lead.lead_id], operator="operator")
    )
    queries = repo.list_source_queries(active_only=True)
    refinement_run = repo.get_agent_run(result.agent_run_id)

    assert result.scanned_count == 1
    assert result.lead_count == 1
    assert result.source_queries_created >= 2
    assert result.query_count >= 2
    assert any("sorafenib canine dose-limiting toxicity" in query.query_text for query in queries)
    assert any("robat" in query.query_text.lower() for query in queries)
    assert not any("increase limit" in query.query_text.lower() for query in queries)
    assert any(skip["reason"] == "operational_recommendation_not_query" for skip in result.skipped)
    assert all(query.query_params["followup_lane"] == "agent_evaluator_followup" for query in queries)
    assert all(query.query_params["origin_review_id"] == str(origin_review_id) for query in queries)
    assert all(query.query_params["origin_agent_run_id"] == str(origin_agent_run_id) for query in queries)
    assert all(query.query_params["origin_evaluator_review_id"] == str(evaluator_review.review_id) for query in queries)
    assert any("sorafenib" in query.query_params["required_terms"] for query in queries)
    safe_params = source_query_params.source_safe_query_params(queries[0])
    assert "origin_evaluator_review_id" not in safe_params
    assert "why_this_query_exists" not in safe_params
    assert refinement_run is not None
    assert refinement_run.summary["source_queries_created"] == result.source_queries_created


def test_command_center_web_refines_research_followup_payload(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-refine-followup.sqlite3", seed=False)
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine DLT",
            status="followup",
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            topic_tags=["sorafenib", "canine", "safety", "mtd", "dlt"],
            metadata={
                "created_by": "agent_finding_escalation_agent",
                "research_followup_loop": {"verdict": "needs_followup"},
            },
        )
    )
    resolver_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="research_followup_resolver_agent",
            status=RunStatus.COMPLETED,
            output_payload={"lead_results": [{"lead_id": str(lead.lead_id), "title": lead.title}]},
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=resolver_run.agent_run_id,
            reviewer_type="llm_evaluator",
            verdict="needs_followup",
            followup_actions=["retry_pubmed_search_with_refined_terms:_'sorafenib_canine_dlt_mtd'"],
        )
    )

    payload = command_center_web.refine_research_followup_payload(
        service,
        str(lead.lead_id),
        {"operator": "operator"},
    )
    action_payload = command_center_web.build_action_items_payload(service, {"limit": ["5"]})
    lead_item = next(item for item in action_payload["items"] if item["item_id"] == str(lead.lead_id))

    assert payload["source_queries_created"] >= 1
    assert "create_refined_queries" in lead_item["actions"]


def test_agent_performance_contracts_validate_allowed_values():
    row = AgentPerformanceRow(
        group_type="agent_name",
        group_value="therapy_committee_chair_agent",
        run_count=3,
        reviewed_run_count=2,
        performance_score=78,
    )
    result = AgentPerformanceReportResult(rows=[row], top_rows=[row], bottom_rows=[row])
    evaluation = AgentPerformanceEvaluationResult(evaluated_count=1, review_created_count=1)

    assert result.rows[0].group_type == "agent_name"
    assert evaluation.agent_name == "agent_performance_evaluator_agent"
    assert AgentPerformanceReportRequest().limit == 500
    assert AgentPerformanceEvaluationRequest().reviewed_only is True
    with pytest.raises(ValidationError):
        AgentPerformanceRow(group_type="wrong", group_value="bad")


def test_agent_performance_report_aggregates_latest_reviews_by_group(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-performance.sqlite3", seed=False)
    service = HSAResearchService(repo)
    now = datetime.now(UTC)

    run_one = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            agent_version="prompt-default",
            model_profile="openrouter_required",
            status=RunStatus.COMPLETED,
            source_key="pubmed",
            started_at=now - timedelta(minutes=4),
            metadata={"model_name": "anthropic/claude-sonnet-4.6", "prompt_version": "therapy-v2"},
        )
    )
    run_two = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            agent_version="prompt-default",
            model_profile="openrouter_required",
            status=RunStatus.COMPLETED,
            source_key="pubmed",
            started_at=now - timedelta(minutes=3),
            input_payload={"openrouter_model": "anthropic/claude-sonnet-4.6", "prompt_key": "therapy-v2"},
        )
    )
    run_three = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            agent_version="prompt-default",
            model_profile="openrouter_required",
            status=RunStatus.COMPLETED,
            source_key="pubmed",
            started_at=now - timedelta(minutes=2),
        )
    )
    run_four = repo.create_agent_run(
        AgentRunRecord(
            agent_name="full_text_ops_agent",
            model_profile="reviewer",
            status=RunStatus.COMPLETED,
            started_at=now - timedelta(minutes=1),
            output_payload={"evidence": {"selected_model": "anthropic/claude-sonnet-latest"}},
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_one.agent_run_id,
            reviewer="operator",
            verdict="useful",
            created_at=now - timedelta(minutes=3),
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_one.agent_run_id,
            reviewer="operator",
            verdict="needs_followup",
            created_at=now - timedelta(minutes=1),
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_one.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="useful",
            created_at=now,
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_two.agent_run_id,
            reviewer="operator",
            verdict="bad",
            created_at=now,
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_four.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="useful",
            created_at=now,
        )
    )

    report = service.build_agent_performance_report(AgentPerformanceReportRequest(limit=10, min_sample_size=3))
    agent_row = next(row for row in report.rows if row.group_type == "agent_name" and row.group_value == "therapy_committee_chair_agent")
    model_row = next(row for row in report.rows if row.group_type == "model_key" and row.group_value == "anthropic/claude-sonnet-4.6")
    prompt_row = next(row for row in report.rows if row.group_type == "prompt_key" and row.group_value == "therapy-v2")

    assert report.agent_run_count == 4
    assert report.reviewed_run_count == 3
    assert report.unreviewed_run_count == 1
    assert report.operator_reviewed_count == 2
    assert report.evaluator_reviewed_count == 2
    assert report.disagreement_count == 1
    assert report.verdict_counts == {"bad": 1, "useful": 2}
    assert agent_row.run_count == 3
    assert agent_row.reviewed_run_count == 2
    assert agent_row.operator_reviewed_count == 2
    assert agent_row.evaluator_reviewed_count == 1
    assert agent_row.performance_score == 50
    assert agent_row.low_sample is True
    assert agent_row.disagreement_count == 1
    assert model_row.reviewed_run_count == 2
    assert prompt_row.reviewed_run_count == 2
    assert any(row.group_value == "full_text_ops_agent" for row in report.top_rows)


def test_agent_performance_evaluator_persists_specialist_reviews(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-performance-evaluator.sqlite3", seed=False)
    service = HSAResearchService(repo)
    agent_names = [
        "research_brief_synthesis_editor_agent",
        "validation_agent_omics",
        "full_text_ops_agent",
        "misc_agent",
    ]
    for agent_name in agent_names:
        run = repo.create_agent_run(
            AgentRunRecord(
                agent_name=agent_name,
                model_profile="openrouter_required",
                status=RunStatus.COMPLETED,
                input_payload={"topic": "KDR angiosarcoma"},
                output_payload={"summary": "Useful output."},
            )
        )
        repo.create_agent_run_review(
            AgentRunReviewRecord(
                agent_run_id=run.agent_run_id,
                reviewer="operator",
                verdict="useful",
            )
        )

    specialists = []

    def fake_openrouter(model_name, review_payload):
        specialists.append(review_payload["specialist"])
        assert review_payload["operator_review"]["verdict"] == "useful"
        return {
            "text": json.dumps(
                {
                    "verdict": "useful",
                    "confidence": 0.82,
                    "rationale": f"{review_payload['specialist']} evaluator agrees.",
                    "strengths": ["Clear next step."],
                    "failure_modes": [],
                    "recommended_followup_actions": ["keep_tracking"],
                    "rubric_scores": {"actionability": 0.8},
                }
            ),
            "metadata": {"provider": "openrouter", "model_name": model_name},
        }

    monkeypatch.setattr(agent_performance, "_openrouter_review_model", fake_openrouter)

    result = service.run_agent_performance_evaluation(
        AgentPerformanceEvaluationRequest(
            limit=4,
            review_models=["anthropic/claude-sonnet-4.6"],
        )
    )
    evaluator_reviews = repo.list_agent_run_reviews(reviewer="synthesis_openrouter_evaluator", limit=10)
    batch_runs = repo.list_agent_runs(agent_name="agent_performance_evaluator_agent", status="completed", limit=5)

    assert result.agent_run_id is not None
    assert result.evaluated_count == 4
    assert result.review_created_count == 4
    assert set(specialists) == {"synthesis", "validation", "ingestion", "general"}
    assert evaluator_reviews[0].reviewer_type == "llm_evaluator"
    assert evaluator_reviews[0].metadata["agent_performance_evaluation"]["model_name"] == "anthropic/claude-sonnet-4.6"
    assert batch_runs
    assert batch_runs[0].summary["review_created_count"] == 4


def test_agent_performance_evaluator_can_target_specific_agent_run(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-performance-targeted.sqlite3", seed=False)
    service = HSAResearchService(repo)
    target = repo.create_agent_run(AgentRunRecord(agent_name="research_followup_resolver_agent", status=RunStatus.COMPLETED))
    other = repo.create_agent_run(AgentRunRecord(agent_name="research_followup_resolver_agent", status=RunStatus.COMPLETED))

    def fake_openrouter(model_name, review_payload):
        assert review_payload["run"]["agent_run_id"] == str(target.agent_run_id)
        return {
            "text": json.dumps(
                {
                    "verdict": "useful",
                    "confidence": 0.8,
                    "rationale": "Targeted run is good.",
                    "strengths": [],
                    "failure_modes": [],
                    "recommended_followup_actions": [],
                    "rubric_scores": {},
                }
            ),
            "metadata": {"usage": {"cost": 0.01}},
        }

    monkeypatch.setattr(agent_performance, "_openrouter_review_model", fake_openrouter)

    result = service.run_agent_performance_evaluation(
        AgentPerformanceEvaluationRequest(
            agent_run_ids=[target.agent_run_id],
            status=None,
            reviewed_only=False,
            limit=1,
        )
    )

    assert result.evaluated_count == 1
    assert repo.list_agent_run_reviews(agent_run_id=target.agent_run_id, limit=10)
    assert repo.list_agent_run_reviews(agent_run_id=other.agent_run_id, limit=10) == []


def test_agent_performance_specialist_routing_covers_agent_lanes():
    assert agent_performance._specialist_for_agent("research_synthesis_editor_agent") == "synthesis"
    assert agent_performance._specialist_for_agent("therapy_committee_chair_agent") == "synthesis"
    assert agent_performance._specialist_for_agent("validation_gap_source_pack_agent") == "ingestion"
    assert agent_performance._specialist_for_agent("research_followup_resolver_agent") == "ingestion"
    assert agent_performance._specialist_for_agent("claim_curator_agent") == "ingestion"
    assert agent_performance._specialist_for_agent("evidence_scout_agent") == "ingestion"
    assert agent_performance._specialist_for_agent("omics_validation_agent") == "validation"
    assert agent_performance._specialist_for_agent("unknown_agent") == "general"


def test_agent_performance_evaluator_invalid_json_fails_batch_without_review(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-performance-evaluator-fail.sqlite3", seed=False)
    service = HSAResearchService(repo)
    run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            status=RunStatus.COMPLETED,
        )
    )
    repo.create_agent_run_review(AgentRunReviewRecord(agent_run_id=run.agent_run_id, reviewer="operator", verdict="useful"))

    def fake_openrouter(model_name, review_payload):
        return {"text": "not json", "metadata": {"provider": "openrouter", "model_name": model_name}}

    monkeypatch.setattr(agent_performance, "_openrouter_review_model", fake_openrouter)

    with pytest.raises(json.JSONDecodeError):
        service.run_agent_performance_evaluation(AgentPerformanceEvaluationRequest(limit=1))

    failed_runs = repo.list_agent_runs(agent_name="agent_performance_evaluator_agent", status="failed", limit=5)
    evaluator_reviews = [
        review for review in repo.list_agent_run_reviews(agent_run_id=run.agent_run_id, limit=10)
        if review.reviewer_type == "llm_evaluator"
    ]
    assert failed_runs
    assert evaluator_reviews == []


def test_research_lead_repository_roundtrip_sqlite_and_memory(tmp_path):
    sqlite_repo = SQLiteResearchRepository(tmp_path / "research-leads.sqlite3", seed=False)
    memory_repo = InMemoryResearchRepository()

    for repo in (sqlite_repo, memory_repo):
        lead = repo.upsert_research_lead(
            ResearchLeadRecord(
                title="Institutional HSA lead",
                url="https://example.edu/research/hsa",
                lead_type="institutional_article",
                source_key="x_linked_article",
                reason="Parser found a credible non-durable item.",
                topic_tags=["hemangiosarcoma"],
            )
        )
        duplicate = repo.upsert_research_lead(
            ResearchLeadRecord(
                title="Institutional HSA lead duplicate",
                url="https://example.edu/research/hsa",
                lead_type="institutional_article",
                source_key="x_linked_article",
                reason="Same URL.",
            )
        )
        updated = repo.update_research_lead(lead.lead_id, status="watching", metadata={"reviewed": True})

        assert duplicate.lead_id == lead.lead_id
        assert updated is not None
        assert updated.status == "watching"
        assert updated.metadata["reviewed"] is True
        assert repo.get_research_lead(lead.lead_id).identity_key == lead.identity_key
        assert repo.list_research_leads(status="watching", source_key="x_linked_article")
        assert repo.list_research_leads(lead_type="institutional_article", limit=1)[0].lead_id == lead.lead_id


def test_model_review_summary_compacts_agent_run_payload():
    run = {
        "agent_run_id": "run-1",
        "agent_name": "full_text_ops_agent",
        "status": "completed",
        "source_key": "pmc_oa",
        "partition_date": "2026-04-27",
        "completed_at": "2026-04-28T16:51:49Z",
        "output_payload": {
            "schedule_readiness": "ready_to_enable",
            "should_block_schedule": False,
            "errors": [],
            "actions": [
                {"source_key": "all", "action": "ready_to_enable_schedule", "severity": "info", "reason": "clean"}
            ],
            "evidence": {
                "selected_model": "~anthropic/claude-sonnet-latest",
                "review_packet": {"large": "x" * 10000},
                "model_reviews": [
                    {
                        "model_name": "~anthropic/claude-sonnet-latest",
                        "status": "completed",
                        "metadata": {
                            "requested_model": "~anthropic/claude-sonnet-latest",
                            "model_name": "anthropic/claude-4.6-sonnet-20260217",
                            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15, "cost": 0.01},
                        },
                        "result": {
                            "schedule_readiness": "ready_to_enable",
                            "should_block_schedule": False,
                            "actions": [{"action": "ready_to_enable_schedule"}],
                        },
                    }
                ],
            },
        },
    }

    summary = cli_module._model_review_summary(run)

    assert summary["selected_model"] == "~anthropic/claude-sonnet-latest"
    assert summary["model_reviews"][0]["resolved_model"] == "anthropic/claude-4.6-sonnet-20260217"
    assert summary["model_reviews"][0]["usage"]["cost"] == 0.01
    assert not _contains_key(summary, "review_packet")


def test_model_review_summary_includes_therapy_committee_ideas_and_model_reviews():
    run = {
        "agent_run_id": "run-therapy",
        "agent_name": "therapy_committee_chair_agent",
        "status": "completed",
        "source_key": None,
        "partition_date": None,
        "completed_at": "2026-05-04T01:19:36Z",
        "summary": {"idea_count": 1, "top_idea": "PD-1 plus VEGFR2"},
        "output_payload": {
            "committee_run_id": "committee-1",
            "decision_summary": "Top recommend-only idea: PD-1 plus VEGFR2.",
            "errors": [],
            "ranked_ideas": [
                {
                    "idea_id": "idea-1",
                    "title": "PD-1 plus VEGFR2",
                    "priority_score": 0.82,
                    "evidence_strength": "low",
                }
            ],
            "reports": [
                {
                    "perspective": "target_biology",
                    "evidence": {
                        "model_review": {
                            "requested_model": "anthropic/claude-sonnet-4.6",
                            "model_name": "anthropic/claude-sonnet-4.6",
                            "json_repair_attempted": True,
                            "usage": {"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": 30},
                            "original_review": {"model_name": "anthropic/claude-sonnet-4.6"},
                        }
                    },
                }
            ],
        },
    }

    summary = cli_module._model_review_summary(run)

    assert summary["committee_run_id"] == "committee-1"
    assert summary["idea_count"] == 1
    assert summary["top_ideas"][0]["title"] == "PD-1 plus VEGFR2"
    assert summary["model_reviews"][0]["perspective"] == "target_biology"
    assert summary["model_reviews"][0]["json_repair_attempted"] is True
    assert summary["model_reviews"][0]["usage"]["total_tokens"] == 30


def test_dagster_structured_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    def fail_direct_factory():
        raise AssertionError("dagster assets must use the injected repository resource")

    def fake_pipeline(repository, **kwargs):
        assert repository is sentinel_repository
        return {"repository": "injected", "source_keys": kwargs["source_keys"]}

    monkeypatch.setattr(storage, "build_sql_repository", fail_direct_factory)
    monkeypatch.setattr(structured_orchestration, "run_structured_sources_pipeline", fake_pipeline)

    result = dagster_asset_module.structured_source_smoke_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == ["build_repository"]
    assert result == {"repository": "injected", "source_keys": dagster_asset_module.STRUCTURED_SOURCE_SMOKE_KEYS}


def test_dagster_all_api_smoke_is_ingestion_only(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    def fail_full_pipeline(*args, **kwargs):
        raise AssertionError("all_api_smoke_job must stay ingestion-only")

    def fake_ingestion_pipeline(repository, **kwargs):
        assert repository is sentinel_repository
        assert kwargs == {
            "source_keys": dagster_asset_module.ALL_API_SMOKE_KEYS,
            "source_limits": {source_key: 1 for source_key in dagster_asset_module.ALL_API_SMOKE_KEYS},
        }
        return {
            "mode": "ingestion_only",
            "source_keys": kwargs["source_keys"],
            "sources": [
                {
                    "source_key": "unpaywall",
                    "qa": {
                        "raw_records": 1,
                        "research_objects": 1,
                        "document_chunks": 1,
                        "claims": 0,
                    },
                }
            ],
            "totals": {
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 1,
                "claims": 0,
            },
            "errors": [],
        }

    monkeypatch.setattr(structured_orchestration, "run_structured_sources_pipeline", fail_full_pipeline)
    monkeypatch.setattr(structured_orchestration, "run_structured_sources_ingestion_pipeline", fake_ingestion_pipeline)

    result = dagster_asset_module.all_api_smoke_report.node_def.compute_fn.decorated_fn(FakeRepositoryResource())
    check = dagster_asset_module.all_api_smoke_has_minimum_outputs.node_def.compute_fn.decorated_fn(result)

    assert calls == ["build_repository"]
    assert result["mode"] == "ingestion_only"
    assert check.passed is True
    assert check.metadata["mode"].value == "ingestion_only"


def test_dagster_literature_corpus_source_date_asset_uses_single_source_partition(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    def fake_pipeline(repository, **kwargs):
        assert repository is sentinel_repository
        return {
            **kwargs,
            "sources": [],
            "totals": {"raw_records": 0},
            "errors": [],
        }

    monkeypatch.setattr(structured_orchestration, "run_structured_sources_pipeline", fake_pipeline)

    partition_context = SimpleNamespace(
        multi_partition_key=dagster_asset_module.dg.MultiPartitionKey(
            {
                "source": "pubmed",
                "date": "2026-04-27",
            }
        )
    )
    result = dagster_asset_module.literature_corpus_source_date_report.node_def.compute_fn.decorated_fn(
        partition_context,
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert isinstance(result, dagster_asset_module.dg.MaterializeResult)
    assert result.value["source_keys"] == ("pubmed",)
    assert result.value["source_limits"] == {"pubmed": 100}
    assert result.value["partition_date"] == "2026-04-27"
    assert result.value["mode"] == "source_date_partition"
    assert result.value["date_filter_status"] == "orchestration_metadata_only"
    assert result.metadata["source_key"] == "pubmed"
    assert result.metadata["partition_date"] == "2026-04-27"
    assert result.metadata["date_filter_status"] == "orchestration_metadata_only"


def test_dagster_literature_corpus_source_date_partitions_and_job_are_wired():
    assert dagster_asset_module.literature_corpus_source_date_job is not None
    assert (
        dagster_asset_module.literature_corpus_source_date_report.partitions_def
        is dagster_asset_module.LITERATURE_CORPUS_SOURCE_DATE_PARTITIONS
    )
    assert dagster_asset_module.LITERATURE_CORPUS_SOURCE_PARTITIONS.get_partition_keys() == list(
        dagster_asset_module.LITERATURE_CORPUS_SOURCE_KEYS
    )
    assert dagster_asset_module.LITERATURE_CORPUS_DATE_PARTITIONS.start.date().isoformat() == "2026-01-01"


def test_dagster_full_text_ops_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def run_full_text_ops(self, request):
            assert isinstance(request, FullTextOpsRequest)
            return FullTextOpsResult(
                agent_run_id=uuid4(),
                actions=[
                    FullTextOpsAction(
                        source_key="europe_pmc",
                        action="run_source_date_partition",
                        severity="watch",
                        reason="Partition evidence is missing.",
                    )
                ],
                schedule_readiness="needs_partition_validation",
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.full_text_ops_agent_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == ["build_repository"]
    assert result.value["agent_name"] == "full_text_ops_agent"
    assert result.metadata["action_count"] == 1


def test_dagster_research_brief_evaluation_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    brief_id = uuid4()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def evaluate_research_brief(self, request):
            assert isinstance(request, ResearchBriefEvaluationRequest)
            assert request.brief_id == brief_id
            assert request.review_mode == "openrouter_required"
            return ResearchBriefEvaluationResult(
                brief_id=brief_id,
                agent_run_id=uuid4(),
                topic="VEGF therapy",
                source_key="pubmed",
                overall_score=0.82,
                citation_coverage_score=0.8,
                perspective_balance_score=0.8,
                contradiction_handling_score=0.8,
                novelty_score=0.8,
                actionability_score=0.8,
                weakness_transparency_score=0.8,
                passes_quality_bar=True,
                readiness="ready_for_hypothesis_review",
                recommendations=["Promote this brief."],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.research_brief_evaluation_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "brief_id": str(brief_id),
                "limit": 1,
                "minimum_overall_score": 0.7,
            },
            run_id="dagster-test-run",
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert result.value["brief_id"] == str(brief_id)
    assert result.metadata["readiness"] == "ready_for_hypothesis_review"


def test_dagster_agent_performance_assets_use_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def build_agent_performance_report(self, request):
            assert request.limit == 25
            return AgentPerformanceReportResult(
                agent_run_count=2,
                reviewed_run_count=1,
                operator_reviewed_count=1,
                rows=[
                    AgentPerformanceRow(
                        group_type="agent_name",
                        group_value="therapy_committee_chair_agent",
                        run_count=2,
                        reviewed_run_count=1,
                        performance_score=100,
                        low_sample=True,
                    )
                ],
            )

        def run_agent_performance_evaluation(self, request):
            assert request.dagster_run_id == "dagster-agent-performance-test"
            return AgentPerformanceEvaluationResult(
                evaluated_count=1,
                review_created_count=1,
                evaluations=[
                    {
                        "agent_run_id": str(uuid4()),
                        "agent_name": "therapy_committee_chair_agent",
                        "specialist": "synthesis",
                        "model_name": "test-model",
                        "verdict": "useful",
                        "confidence": 0.8,
                        "review_id": str(uuid4()),
                        "rationale": "Useful output.",
                    }
                ],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    report_result = dagster_asset_module.agent_performance_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(op_config={"limit": 25, "min_sample_size": 3}),
        FakeRepositoryResource(),
    )
    evaluation_result = dagster_asset_module.agent_performance_evaluation_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "status": "completed",
                "limit": 1,
                "reviewed_only": True,
                "model_profile": "agent_performance_evaluator",
                "review_models": [],
            },
            run_id="dagster-agent-performance-test",
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository", "build_repository"]
    assert report_result.value["reviewed_run_count"] == 1
    assert report_result.metadata["reviewed_run_count"].value == 1
    assert evaluation_result.value["review_created_count"] == 1
    assert evaluation_result.metadata["review_created_count"].value == 1


def test_dagster_validation_plan_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    brief_id = uuid4()
    evaluation_id = uuid4()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def plan_validation(self, request):
            assert isinstance(request, ValidationPlanRequest)
            assert request.evaluation_id == evaluation_id
            return ValidationPlanResult(
                brief_id=brief_id,
                evaluation_id=evaluation_id,
                agent_run_id=uuid4(),
                topic="VEGF therapy",
                source_key="pubmed",
                status="ready_for_review",
                readiness="ready_for_expert_review",
                tasks=[
                    ValidationPlanTask(
                        task_type="expert_review",
                        title="Expert review",
                        objective="Assess the validation path.",
                        rationale="Ready brief.",
                        evidence_refs=["C1"],
                    )
                ],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.validation_plan_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "evaluation_id": str(evaluation_id),
                "require_ready_evaluation": True,
                "max_tasks": 8,
            },
            run_id="dagster-test-run",
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert result.value["brief_id"] == str(brief_id)
    assert result.metadata["readiness"] == "ready_for_expert_review"
    assert result.metadata["task_count"].value == 1


def test_dagster_research_brief_followup_queue_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    lead_id = uuid4()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def queue_research_brief_followups(self, request):
            assert isinstance(request, ResearchBriefFollowupQueueRequest)
            assert request.limit == 10
            assert request.dry_run is True
            return ResearchBriefFollowupQueueResult(
                candidate_brief_count=1,
                limitation_count=1,
                queued_count=0,
                dry_run=True,
                followup_leads=[
                    ResearchLeadRecord(
                        lead_id=lead_id,
                        identity_key=f"research_lead:brief_followup:{lead_id}",
                        title="Follow up evidence limitation",
                        status="followup",
                        evidence_refs=["research_brief:test"],
                    )
                ],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.research_brief_followup_queue_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "limit": 10,
                "include_evaluations": True,
                "max_limitations_per_brief": 20,
                "dry_run": True,
            }
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert result.value["candidate_brief_count"] == 1
    assert result.metadata["limitation_count"].value == 1


def test_dagster_research_followup_resolver_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    lead_id = uuid4()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def resolve_research_followups(self, request):
            assert isinstance(request, ResearchFollowupResolverRequest)
            assert request.lead_ids == [lead_id]
            assert request.dagster_run_id == "dagster-test-run"
            return ResearchFollowupResolverResult(
                agent_run_id=uuid4(),
                leads_seen=1,
                promoted_leads=1,
                lead_results=[
                    ResearchFollowupLeadResult(
                        lead_id=lead_id,
                        status_before="followup",
                        status_after="watching",
                        actions=["promoted_to_watching"],
                        evidence_refs=["chunk:1"],
                        durable_source_keys=["pubmed"],
                        promoted=True,
                    )
                ],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.research_followup_resolver_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "lead_ids": [str(lead_id)],
                "statuses": ["followup"],
                "source_keys": [],
                "search_source_keys": ["pubmed"],
                "limit": 25,
                "ingest_source_followups": True,
                "search_missing_identifiers": True,
                "promote_ready_leads": True,
                "run_claim_extraction": True,
                "dry_run": False,
                "min_evidence_chunks": 1,
                "search_limit_per_source": 2,
                "max_search_terms": 12,
            },
            run_id="dagster-test-run",
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert result.value["leads_seen"] == 1
    assert result.metadata["promoted_leads"].value == 1
    assert result.metadata["lead_results"].records[0].data["durable_source_keys"] == '["pubmed"]'


def test_dagster_source_health_report_lives_in_control_panel_group():
    assert dagster_asset_module.source_health_report.group_names_by_key == {
        dagster_asset_module.dg.AssetKey(["source_health_report"]): "control_panel"
    }


def test_dagster_full_text_source_specific_assets_use_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    def fake_pipeline(repository, **kwargs):
        assert repository is sentinel_repository
        return {
            **kwargs,
            "sources": [],
            "totals": {},
            "errors": [],
        }

    monkeypatch.setattr(structured_orchestration, "run_structured_sources_pipeline", fake_pipeline)
    monkeypatch.setattr(structured_orchestration, "run_structured_sources_ingestion_pipeline", fake_pipeline)

    europe_pmc_report = dagster_asset_module.europe_pmc_full_text_refresh_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    pmc_oa_report = dagster_asset_module.pmc_oa_full_text_refresh_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    ingest_smoke_report = dagster_asset_module.literature_full_text_ingest_smoke_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    europe_pmc_ingest_report = dagster_asset_module.europe_pmc_full_text_ingest_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    pmc_oa_ingest_report = dagster_asset_module.pmc_oa_full_text_ingest_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    smoke_report = dagster_asset_module.literature_full_text_smoke_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    partition_context = SimpleNamespace(
        multi_partition_key=dagster_asset_module.dg.MultiPartitionKey(
            {
                "source": "europe_pmc",
                "date": "2026-04-27",
            }
        )
    )
    partition_result = (
        dagster_asset_module.literature_full_text_source_date_report.node_def.compute_fn.decorated_fn(
            partition_context,
            FakeRepositoryResource(),
        )
    )

    assert calls == [
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
    ]
    assert europe_pmc_report["source_keys"] == ("europe_pmc",)
    assert europe_pmc_report["source_limits"] == {"europe_pmc": 10}
    assert europe_pmc_report["mode"] == "refresh"
    assert pmc_oa_report["source_keys"] == ("pmc_oa",)
    assert pmc_oa_report["source_limits"] == {"pmc_oa": 3}
    assert pmc_oa_report["mode"] == "refresh"
    assert ingest_smoke_report["source_keys"] == dagster_asset_module.LITERATURE_FULL_TEXT_SOURCE_KEYS
    assert ingest_smoke_report["source_limits"] == {"europe_pmc": 1, "pmc_oa": 1}
    assert ingest_smoke_report["mode"] == "ingestion_only"
    assert europe_pmc_ingest_report["source_keys"] == ("europe_pmc",)
    assert europe_pmc_ingest_report["source_limits"] == {"europe_pmc": 10}
    assert europe_pmc_ingest_report["mode"] == "ingestion_only"
    assert pmc_oa_ingest_report["source_keys"] == ("pmc_oa",)
    assert pmc_oa_ingest_report["source_limits"] == {"pmc_oa": 3}
    assert pmc_oa_ingest_report["mode"] == "ingestion_only"
    assert smoke_report["source_keys"] == dagster_asset_module.LITERATURE_FULL_TEXT_SOURCE_KEYS
    assert smoke_report["source_limits"] == {"europe_pmc": 1, "pmc_oa": 1}
    assert smoke_report["mode"] == "refresh"
    assert partition_result.value["source_keys"] == ("europe_pmc",)
    assert partition_result.value["source_limits"] == {"europe_pmc": 10}
    assert partition_result.value["partition_date"] == "2026-04-27"
    assert partition_result.value["mode"] == "source_date_partition"


def test_full_text_ingestion_pipeline_skips_downstream_work(monkeypatch):
    calls = []

    class FakeIngestionResult:
        def model_dump(self, mode):
            assert mode == "json"
            return {
                "source_key": "europe_pmc",
                "query_name": "licensed_full_text_hsa",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 2,
                "full_text_research_objects": 1,
                "section_chunk_counts": {"title_abstract": 1, "full_text": 1},
                "status": "completed",
                "errors": [],
            }

    class FakePipeline:
        def __init__(self, repository):
            calls.append(("init", repository))

        def initialize(self):
            calls.append(("initialize",))

        def ingest_source(self, source_key, limit):
            calls.append(("ingest_source", source_key, limit))
            return [FakeIngestionResult()]

    class FakeRepository:
        def source_runtime_summary(self, source_key, sample_limit=5):
            assert source_key == "europe_pmc"
            return {
                "source_key": source_key,
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 2,
                "entity_mentions": 0,
                "claims": 0,
            }

        def list_research_objects(self, source_key=None):
            assert source_key == "europe_pmc"
            return [SimpleNamespace(metadata={"full_text_available": True})]

        def list_document_chunks(self, source_key=None):
            assert source_key == "europe_pmc"
            return [
                SimpleNamespace(section_label="title_abstract", text_content="title"),
                SimpleNamespace(section_label="full_text", text_content="body"),
            ]

        def coverage_summary(self):
            return {"document_chunks": 2}

    def fail_downstream(*args, **kwargs):
        raise AssertionError("ingestion-only pipeline must not run downstream claim work")

    monkeypatch.setattr(structured_orchestration, "LocalIngestionPipeline", FakePipeline)
    monkeypatch.setattr(structured_orchestration, "resolve_entities_for_repository", fail_downstream)
    monkeypatch.setattr(structured_orchestration, "extract_claims_for_repository", fail_downstream)
    monkeypatch.setattr(structured_orchestration, "curate_claims_for_repository", fail_downstream)

    report = structured_orchestration.run_structured_sources_ingestion_pipeline(
        FakeRepository(),
        source_keys=("europe_pmc",),
        source_limits={"europe_pmc": 1},
    )

    assert calls[-1] == ("ingest_source", "europe_pmc", 1)
    assert report["mode"] == "ingestion_only"
    assert report["totals"]["document_chunks"] == 2
    source_report = report["sources"][0]
    assert source_report["entity_resolution"]["status"] == "skipped"
    assert source_report["extraction"]["status"] == "skipped"
    assert source_report["curation"]["status"] == "skipped"
    assert source_report["full_text_qa"]["passes_full_text_bar"] is True
    assert source_report["full_text_qa"]["triage"]["action"] == "no_action"
    assert source_report["full_text_triage_action"] == "no_action"


def test_full_text_partition_allows_empty_current_day(monkeypatch):
    calls = []

    class EmptyIngestionResult:
        def model_dump(self, mode):
            assert mode == "json"
            return {
                "source_key": "europe_pmc",
                "query_name": "comparative_hsa_open_access:partition_2026-04-27",
                "raw_records": 0,
                "research_objects": 0,
                "document_chunks": 0,
                "full_text_research_objects": 0,
                "section_chunk_counts": {},
                "status": "completed",
                "errors": [],
            }

    class FakePipeline:
        def __init__(self, repository):
            calls.append(("init", repository))

        def initialize(self):
            calls.append(("initialize",))

        def ingest_source(self, source_key, limit=25, **kwargs):
            calls.append(("ingest_source", source_key, limit, kwargs))
            return [EmptyIngestionResult()]

    class FakeRepository:
        def source_runtime_summary(self, source_key, sample_limit=5):
            return {
                "source_key": source_key,
                "raw_records": 0,
                "research_objects": 0,
                "document_chunks": 0,
                "entity_mentions": 0,
                "claims": 0,
            }

        def list_research_objects(self, source_key=None):
            return []

        def list_document_chunks(self, source_key=None):
            return []

        def coverage_summary(self):
            return {}

    monkeypatch.setattr(structured_orchestration, "LocalIngestionPipeline", FakePipeline)

    report = structured_orchestration.run_structured_sources_ingestion_pipeline(
        FakeRepository(),
        source_keys=("europe_pmc",),
        source_limits={"europe_pmc": 1},
        partition_date="2026-04-27",
    )

    source_report = report["sources"][0]
    assert calls[-1] == (
        "ingest_source",
        "europe_pmc",
        1,
        {
            "query_param_overrides": {
                "published_after": "2026-04-27",
                "published_before": "2026-04-27",
            },
            "query_name_suffix": "partition_2026-04-27",
        },
    )
    assert report["partition_date"] == "2026-04-27"
    assert source_report["full_text_qa"]["current_empty_passes"] is True
    assert source_report["full_text_qa"]["passes_full_text_bar"] is True
    assert source_report["full_text_qa"]["triage"]["action"] == "no_action"


def test_dagster_metadata_table_rows_encode_nested_values():
    rows = dagster_asset_module._compact_table_rows(
        [
            {
                "source_key": "pubchem",
                "raw_records": 1,
                "claim_status": {"promote": 1},
                "sample_claims": [{"statement": "Propranolol has PubChem identity CID 4946."}],
                "passes_minimum_bar": True,
            }
        ],
        columns=("source_key", "raw_records", "claim_status", "sample_claims", "passes_minimum_bar"),
    )

    assert rows[0]["source_key"] == "pubchem"
    assert rows[0]["raw_records"] == 1
    assert rows[0]["passes_minimum_bar"] is True
    assert json.loads(rows[0]["claim_status"]) == {"promote": 1}
    assert json.loads(rows[0]["sample_claims"]) == [
        {"statement": "Propranolol has PubChem identity CID 4946."}
    ]
    assert all(value is None or isinstance(value, str | int | float | bool) for value in rows[0].values())


def test_dagster_count_report_asset_returns_materialize_result_with_report_value(monkeypatch):
    sentinel_repository = object()
    calls = []
    report = {
        "source_keys": ["pubchem"],
        "sources": [
            {
                "source_key": "pubchem",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 1,
                "entity_mentions": 0,
                "claims": 1,
                "passes_minimum_bar": True,
                "claim_status": {"promote": 1},
                "claim_types": {"other": 1},
            }
        ],
        "totals": {
            "raw_records": 1,
            "research_objects": 1,
            "document_chunks": 1,
            "entity_mentions": 0,
            "claims": 1,
        },
        "failed_sources": [],
        "passes_minimum_bar": True,
        "minimum_bar": {"require_claims": True},
        "coverage": {"claims": 1},
    }

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    def fake_count_report(repository, **kwargs):
        assert repository is sentinel_repository
        assert kwargs == {
            "source_keys": dagster_asset_module.HOSTED_API_REPORT_KEYS,
            "sample_limit": 3,
            "require_claims": True,
        }
        return report

    monkeypatch.setattr(structured_orchestration, "build_structured_source_count_report", fake_count_report)

    result = dagster_asset_module.structured_source_count_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == ["build_repository"]
    assert isinstance(result, dagster_asset_module.dg.MaterializeResult)
    assert result.value is report
    assert result.metadata["source_count"] == 1
    assert result.metadata["passes_minimum_bar"] is True
    table_row = result.metadata["source_count_table"].records[0].data
    assert json.loads(table_row["claim_status"]) == {"promote": 1}
    assert json.loads(table_row["claim_types"]) == {"other": 1}


def test_dagster_embedding_index_asset_uses_injected_repository(monkeypatch):
    calls = []

    class FakeRepository:
        def embedding_coverage(self, *, embedding_model=None, **kwargs):
            calls.append(("embedding_coverage", embedding_model, kwargs))
            return EmbeddingCoverageSummary(
                embedding_model=embedding_model,
                total_chunks=1,
                embedded_chunks=1,
                missing_chunks=0,
                coverage_ratio=1.0,
                embedding_models={embedding_model: 1},
            )

        def coverage_summary(self):
            calls.append(("coverage_summary",))
            return {"document_chunks": 1, "text_embeddings": 1}

    sentinel_repository = FakeRepository()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append(("build_repository",))
            return sentinel_repository

    def fake_index_embeddings_for_repository(repository, **kwargs):
        assert repository is sentinel_repository
        assert kwargs == {"embedding_model": "local-hash-v1"}
        calls.append(("index_embeddings_for_repository",))
        return EmbeddingIndexResult(
            embedding_model="local-hash-v1",
            chunks_seen=1,
            embeddings_created=1,
        )

    monkeypatch.setattr(
        "hsa_research.ingestion_bridge.embeddings.index_embeddings_for_repository",
        fake_index_embeddings_for_repository,
    )

    result = dagster_asset_module.embedding_index_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == [
        ("build_repository",),
        ("index_embeddings_for_repository",),
        ("embedding_coverage", "local-hash-v1", {}),
        ("coverage_summary",),
    ]
    assert isinstance(result, dagster_asset_module.dg.MaterializeResult)
    assert result.value["embedding_model"] == "local-hash-v1"
    assert result.value["totals"]["chunks_seen"] == 1
    assert result.value["totals"]["embeddings_created"] == 1
    assert result.value["embedding_coverage"]["embedded_chunks"] == 1
    assert result.value["passes_minimum_bar"] is True
    assert result.metadata["embedded_chunks"] == 1
    assert result.metadata["passes_minimum_bar"] is True


def test_dagster_embedding_maintenance_asset_uses_injected_repository(monkeypatch):
    calls = []

    class FakeRepository:
        pass

    sentinel_repository = FakeRepository()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append(("build_repository",))
            return sentinel_repository

    def fake_maintain_embedding_index(repository, **kwargs):
        assert repository is sentinel_repository
        assert kwargs == {"embedding_model": "local-hash-v1"}
        calls.append(("maintain_embedding_index",))
        return EmbeddingMaintenanceResult(
            embedding_model="local-hash-v1",
            prune_embedding_model=None,
            source_key=None,
            object_type=None,
            orphan_embeddings_seen=2,
            orphan_embeddings_deleted=2,
            prune_enabled=True,
            embedding_coverage=EmbeddingCoverageSummary(
                embedding_model="local-hash-v1",
                total_chunks=3,
                embedded_chunks=3,
                missing_chunks=0,
                coverage_ratio=1.0,
                embedding_models={"local-hash-v1": 3},
            ),
            coverage={"document_chunks": 3, "text_embeddings": 3},
        )

    monkeypatch.setattr(
        "hsa_research.ingestion_bridge.embeddings.maintain_embedding_index",
        fake_maintain_embedding_index,
    )

    result = dagster_asset_module.embedding_maintenance_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == [("build_repository",), ("maintain_embedding_index",)]
    assert isinstance(result, dagster_asset_module.dg.MaterializeResult)
    assert result.value["orphan_embeddings"]["seen"] == 2
    assert result.value["orphan_embeddings"]["deleted"] == 2
    assert result.value["embedding_coverage"]["missing_chunks"] == 0
    assert result.value["passes_minimum_bar"] is True
    assert result.metadata["orphan_embeddings_deleted"] == 2
    assert result.metadata["embedded_chunks"] == 3
    assert result.metadata["passes_minimum_bar"] is True


def test_dagster_embedding_index_check_requires_embedding_when_chunks_exist():
    failing_result = dagster_asset_module.embedding_index_has_minimum_outputs.node_def.compute_fn.decorated_fn(
        {
            "errors": [],
            "totals": {"chunks_seen": 1},
            "embedding_coverage": {"total_chunks": 1, "embedded_chunks": 0},
        }
    )
    empty_store_result = dagster_asset_module.embedding_index_has_minimum_outputs.node_def.compute_fn.decorated_fn(
        {
            "errors": [],
            "totals": {"chunks_seen": 0},
            "embedding_coverage": {"total_chunks": 0, "embedded_chunks": 0},
        }
    )
    populated_store_result = dagster_asset_module.embedding_index_has_minimum_outputs.node_def.compute_fn.decorated_fn(
        {
            "errors": [],
            "totals": {"chunks_seen": 3},
            "embedding_coverage": {"total_chunks": 3, "embedded_chunks": 3},
        }
    )

    assert failing_result.passed is False
    assert empty_store_result.passed is True
    assert populated_store_result.passed is True


def test_dagster_embedding_maintenance_check_requires_full_active_model_coverage():
    failing_result = dagster_asset_module.embedding_maintenance_has_clean_coverage.node_def.compute_fn.decorated_fn(
        {
            "errors": [],
            "orphan_embeddings": {"seen": 0, "deleted": 0},
            "embedding_coverage": {"total_chunks": 3, "embedded_chunks": 2, "missing_chunks": 1},
        }
    )
    empty_store_result = dagster_asset_module.embedding_maintenance_has_clean_coverage.node_def.compute_fn.decorated_fn(
        {
            "errors": [],
            "orphan_embeddings": {"seen": 0, "deleted": 0},
            "embedding_coverage": {"total_chunks": 0, "embedded_chunks": 0, "missing_chunks": 0},
        }
    )
    populated_store_result = dagster_asset_module.embedding_maintenance_has_clean_coverage.node_def.compute_fn.decorated_fn(
        {
            "errors": [],
            "orphan_embeddings": {"seen": 1, "deleted": 1},
            "embedding_coverage": {"total_chunks": 3, "embedded_chunks": 3, "missing_chunks": 0},
        }
    )

    assert failing_result.passed is False
    assert empty_store_result.passed is True
    assert populated_store_result.passed is True


def test_dagster_full_text_check_requires_full_text_body_chunks():
    report = {
        "source_keys": ["europe_pmc"],
        "sources": [
            {
                "source_key": "europe_pmc",
                "qa": {
                    "raw_records": 1,
                    "research_objects": 1,
                    "document_chunks": 1,
                    "claims": 1,
                },
                "full_text_qa": {
                    "passes_full_text_bar": False,
                    "full_text_document_chunks": 0,
                    "triage": {
                        "action": "needs_parser_fix",
                        "severity": "blocking",
                        "should_retry": False,
                        "should_block_schedule": True,
                        "reasons": ["Records were persisted but no full-text body chunks were written."],
                        "recommended_next_actions": ["Inspect chunk_text_sections."],
                    },
                },
            }
        ],
        "errors": [],
        "totals": {"raw_records": 1, "research_objects": 1, "document_chunks": 1, "claims": 1},
    }
    annotated = dagster_asset_module._annotate_full_text_report(report, mode="refresh")

    result = dagster_asset_module.literature_full_text_refresh_has_outputs.node_def.compute_fn.decorated_fn(annotated)

    assert result.passed is False
    assert result.metadata["failed_sources"].data == ["europe_pmc"]
    assert result.metadata["full_text_blocking_sources"].data == ["europe_pmc"]
    assert "full_text_triage" in result.metadata


def test_dagster_full_text_partition_check_allows_empty_partition():
    report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "source_keys": ["europe_pmc"],
        "sources": [
            {
                "source_key": "europe_pmc",
                "qa": {
                    "raw_records": 0,
                    "research_objects": 0,
                    "document_chunks": 0,
                    "claims": 0,
                },
                "full_text_qa": {
                    "passes_full_text_bar": True,
                    "current_empty_passes": True,
                    "triage": {
                        "action": "no_action",
                        "severity": "info",
                        "should_retry": False,
                        "should_block_schedule": False,
                        "reasons": ["The date-partitioned source run completed with no records."],
                        "recommended_next_actions": ["Mark the partition clean."],
                    },
                },
            }
        ],
        "errors": [],
        "full_text_triage": [],
        "totals": {"raw_records": 0, "research_objects": 0, "document_chunks": 0, "claims": 0},
    }

    result = dagster_asset_module.literature_full_text_source_date_has_outputs.node_def.compute_fn.decorated_fn(
        report
    )

    assert result.passed is True
    assert result.metadata["empty_sources"].data == ["europe_pmc"]


def _seed_minimal_source_claim(
    repo: SQLiteResearchRepository,
    source_key: str,
    *,
    curation_status: str = "promote",
    extraction_status: str = "typed",
) -> None:
    raw_record = RawSourceRecord(
        source_key=source_key,
        source_record_id=f"{source_key}:1",
        content_hash=f"{source_key}-raw",
        source_url=f"https://example.org/{source_key}/1",
        raw_payload={"source_key": source_key},
    )
    raw_record_id = repo.upsert_raw_record(raw_record)
    research_object = ResearchObject(
        object_type="publication",
        title=f"{source_key} source record",
        canonical_url=f"https://example.org/{source_key}/1",
        source_key=source_key,
        raw_record_id=raw_record_id,
        dedupe_key=f"{source_key}:1",
    )
    object_id = repo.upsert_research_object(research_object, raw_record_id)
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=f"{source_key} mentions canine hemangiosarcoma and human angiosarcoma context.",
            content_hash=f"{source_key}-chunk",
        )
    )
    repo.upsert_claim(
        ClaimSearchResult(
            claim_id=uuid4(),
            statement=f"{source_key} provides source context for canine HSA research.",
            claim_type=ClaimType.OTHER,
            direction=ClaimDirection.NEUTRAL,
            confidence=0.7,
            evidence_level=EvidenceLevel.REVIEW,
            source_object_id=object_id,
            source_title=f"{source_key} source record",
            source_url=f"https://example.org/{source_key}/1",
            support_count=1,
            metadata={"curation_status": curation_status, "extraction_status": extraction_status},
        )
    )


def _seed_full_text_source_claim(repo: SQLiteResearchRepository, source_key: str = "europe_pmc") -> None:
    raw_record = RawSourceRecord(
        source_key=source_key,
        source_record_id=f"{source_key}:full-text",
        content_hash=f"{source_key}-full-text-raw",
        source_url=f"https://example.org/{source_key}/full-text",
        raw_payload={"source_key": source_key, "full_text": "Full text body mentions canine hemangiosarcoma."},
    )
    raw_record_id = repo.upsert_raw_record(raw_record)
    research_object = ResearchObject(
        object_type="publication",
        title=f"{source_key} full text source record",
        canonical_url=f"https://example.org/{source_key}/full-text",
        source_key=source_key,
        raw_record_id=raw_record_id,
        dedupe_key=f"{source_key}:full-text",
        metadata={"full_text_available": True},
    )
    object_id = repo.upsert_research_object(research_object, raw_record_id)
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="full_text",
            text_content="Full text body mentions canine hemangiosarcoma and human angiosarcoma context.",
            content_hash=f"{source_key}-full-text-chunk",
        )
    )
    repo.upsert_claim(
        ClaimSearchResult(
            claim_id=uuid4(),
            statement=f"{source_key} provides full-text source context for canine HSA research.",
            claim_type=ClaimType.OTHER,
            direction=ClaimDirection.NEUTRAL,
            confidence=0.7,
            evidence_level=EvidenceLevel.REVIEW,
            source_object_id=object_id,
            source_title=f"{source_key} full text source record",
            source_url=f"https://example.org/{source_key}/full-text",
            support_count=1,
            metadata={"curation_status": "promote", "extraction_status": "typed"},
        )
    )


def test_service_existing_agents_create_agent_run_ledger_rows(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-service.sqlite3", seed=True)
    _seed_full_text_source_claim(repo, "europe_pmc")
    _seed_minimal_source_claim(repo, "pubmed", curation_status="uncurated", extraction_status="draft")
    service = HSAResearchService(repo)

    triage = service.triage_full_text_issue(
        FullTextTriageRequest(
            source_key="europe_pmc",
            full_text_document_chunks=1,
            full_text_body_chars=250,
        )
    )
    scout = service.scout_sources(SourceScoutRequest(max_recommendations=2))
    curation = service.curate_claims(ClaimCurationRequest(limit=5, dry_run=True))

    assert triage.action == "no_action"
    assert scout.recommendations
    assert curation.claims_seen >= 1
    assert repo.list_agent_runs(agent_name="full_text_triage_agent", status="completed")
    assert repo.list_agent_runs(agent_name="source_scout_agent", status="completed")
    assert repo.list_agent_runs(agent_name="claim_curator_agent", status="completed")


def test_service_failed_agent_execution_records_failed_run(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-failure.sqlite3", seed=False)
    service = HSAResearchService(repo)

    def fail_run(self, request):
        raise RuntimeError("forced full-text ops failure")

    monkeypatch.setattr(full_text_ops.FullTextOpsAgent, "run", fail_run)

    with pytest.raises(RuntimeError, match="forced full-text ops failure"):
        service.run_full_text_ops(FullTextOpsRequest())

    runs = repo.list_agent_runs(agent_name="full_text_ops_agent", status="failed")
    assert len(runs) == 1
    assert runs[0].errors == ["forced full-text ops failure"]


def test_full_text_ops_ready_when_health_and_partition_are_clean(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-ready.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    _seed_full_text_source_claim(repo, "pmc_oa")
    partition_report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "sources": [
            {"source_key": "europe_pmc", "full_text_qa": {"passes_full_text_bar": True}},
            {"source_key": "pmc_oa", "full_text_qa": {"current_empty_passes": True}},
        ],
        "errors": [],
    }

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            partition_date="2026-04-27",
            full_text_report=partition_report,
            review_mode="deterministic_only",
        )
    )

    assert result.schedule_readiness == "ready_to_enable"
    assert result.should_block_schedule is False
    assert any(action.action == "ready_to_enable_schedule" for action in result.actions)
    assert {action.action for action in result.actions if action.source_key != "all"} == {"mark_clean"}


def test_full_text_ops_external_review_packet_includes_deterministic_guardrail(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-external-review.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    partition_report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "sources": [
            {"source_key": "europe_pmc", "full_text_qa": {"passes_full_text_bar": True}},
        ],
        "errors": [],
    }

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            full_text_report=partition_report,
            review_mode="external_required",
        )
    )

    assert result.schedule_readiness == "keep_stopped"
    assert result.should_block_schedule is True
    assert any(action.action == "needs_human_review" for action in result.actions)
    assert result.evidence["external_reviewer"]["provider"] == "openai_chatgpt_pro"
    assert result.evidence["review_packet"]["deterministic_guardrail_result"]["schedule_readiness"] == "ready_to_enable"


def test_full_text_ops_openrouter_compare_records_each_model(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-openrouter.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    repo.create_agent_run(
        AgentRunRecord(
            agent_name="full_text_ops_agent",
            status=RunStatus.COMPLETED,
            source_key="europe_pmc",
            partition_date="2026-04-26",
            output_payload={
                "schedule_readiness": "keep_stopped",
                "should_block_schedule": True,
                "actions": [{"source_key": "all", "action": "needs_human_review", "severity": "watch"}],
                "evidence": {
                    "review_packet": {"large_nested_payload": "x" * 10000},
                    "model_reviews": [{"model_name": "old", "status": "failed", "error": "old failure"}],
                },
            },
            summary={"actions": 1},
            errors=["old failure"],
        )
    )
    partition_report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "sources": [
            {"source_key": "europe_pmc", "full_text_qa": {"passes_full_text_bar": True}},
        ],
        "errors": [],
    }

    def fake_review_model(model_name, review_payload):
        assert review_payload["deterministic_guardrail_result"]["schedule_readiness"] == "ready_to_enable"
        assert not _contains_key(review_payload, "output_payload")
        assert not _contains_key(review_payload, "review_packet")
        assert review_payload["recent_agent_runs"][0]["output"]["model_review_statuses"][0]["model_name"] == "old"
        return {
            "text": json.dumps(
                {
                    "agent_name": "full_text_ops_agent",
                    "model_profile": "reviewer",
                    "schedule_readiness": "ready_to_enable",
                    "should_block_schedule": False,
                    "actions": [
                        {
                            "source_key": "all",
                            "action": "ready_to_enable_schedule",
                            "severity": "info",
                            "reason": f"{model_name} agrees the evidence is clean.",
                            "evidence_refs": ["deterministic_guardrail_result"],
                        }
                    ],
                    "evidence": {"model_name": model_name},
                    "errors": [],
                }
            ),
            "metadata": {"provider": "openrouter", "model_name": model_name},
        }

    monkeypatch.setattr(full_text_ops, "_openrouter_review_model", fake_review_model)

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            full_text_report=partition_report,
            review_mode="openrouter_compare",
            review_models=["openai/gpt-5.1", "anthropic/claude-sonnet-4.5"],
        )
    )

    assert result.schedule_readiness == "ready_to_enable"
    assert result.evidence["selected_model"] == "openai/gpt-5.1"
    assert [review["model_name"] for review in result.evidence["model_reviews"]] == [
        "openai/gpt-5.1",
        "anthropic/claude-sonnet-4.5",
    ]
    assert all(review["status"] == "completed" for review in result.evidence["model_reviews"])


def test_full_text_ops_openrouter_defaults_to_sonnet_latest(monkeypatch):
    monkeypatch.delenv("HSA_FULL_TEXT_OPS_MODEL", raising=False)
    monkeypatch.delenv("HSA_FULL_TEXT_OPS_REVIEW_MODELS", raising=False)

    assert full_text_ops._review_models(FullTextOpsRequest(review_mode="openrouter_required")) == [
        "~anthropic/claude-sonnet-latest"
    ]
    assert full_text_ops._review_models(FullTextOpsRequest(review_mode="openrouter_compare")) == [
        "~anthropic/claude-sonnet-latest"
    ]


def test_full_text_ops_openrouter_compare_persists_model_failures(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-openrouter-failures.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    partition_report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "sources": [
            {"source_key": "europe_pmc", "full_text_qa": {"passes_full_text_bar": True}},
        ],
        "errors": [],
    }

    def failing_review_model(model_name, review_payload):
        assert review_payload["deterministic_guardrail_result"]["schedule_readiness"] == "ready_to_enable"
        raise RuntimeError(f"{model_name} unavailable")

    monkeypatch.setattr(full_text_ops, "_openrouter_review_model", failing_review_model)

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            full_text_report=partition_report,
            review_mode="openrouter_compare",
            review_models=["openai/gpt-5.1", "anthropic/claude-sonnet-4.5"],
        )
    )

    assert result.schedule_readiness == "keep_stopped"
    assert result.should_block_schedule is True
    assert result.evidence["openrouter_all_models_failed"] is True
    assert result.evidence["selected_model"] is None
    assert [review["status"] for review in result.evidence["model_reviews"]] == ["failed", "failed"]
    assert any("openai/gpt-5.1 unavailable" in error for error in result.errors)
    assert any(action.action == "needs_human_review" for action in result.actions)


def test_full_text_ops_requests_partition_or_ingest_when_validation_is_missing(tmp_path):
    clean_repo = SQLiteResearchRepository(tmp_path / "full-text-ops-partition.sqlite3", seed=False)
    _seed_full_text_source_claim(clean_repo, "europe_pmc")

    partition_result = FullTextOpsAgent(clean_repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            review_mode="deterministic_only",
        )
    )

    assert partition_result.schedule_readiness == "needs_partition_validation"
    assert any(action.action == "run_source_date_partition" for action in partition_result.actions)

    empty_repo = SQLiteResearchRepository(tmp_path / "full-text-ops-empty.sqlite3", seed=False)
    ingest_result = FullTextOpsAgent(empty_repo).run(
        FullTextOpsRequest(source_keys=["europe_pmc"], review_mode="deterministic_only")
    )

    assert ingest_result.schedule_readiness == "keep_stopped"
    assert any(action.action == "run_ingest_smoke" for action in ingest_result.actions)


def test_full_text_ops_maps_triage_actions_to_recommendations(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-triage.sqlite3", seed=False)
    report = {
        "source_keys": ["europe_pmc", "pmc_oa"],
        "sources": [
            {
                "source_key": "europe_pmc",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 1,
                "full_text_qa": {
                    "passes_full_text_bar": False,
                    "triage": {"action": "needs_parser_fix", "severity": "blocking"},
                },
            },
            {
                "source_key": "pmc_oa",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 1,
                "full_text_qa": {
                    "passes_full_text_bar": False,
                    "triage": {"action": "needs_license_review", "severity": "blocking"},
                },
            },
        ],
    }

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc", "pmc_oa"],
            source_health_report=report,
            review_mode="deterministic_only",
        )
    )

    assert result.schedule_readiness == "blocked"
    assert result.should_block_schedule is True
    assert {action.action for action in result.actions} >= {"inspect_parser", "inspect_license"}


def test_full_text_ops_service_is_recommend_only(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-recommend-only.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    claims_before = len(repo.list_claims(source_key="europe_pmc", include_seed_claims=True))
    source_queries_before = repo.list_source_queries(source_key="europe_pmc")

    result = HSAResearchService(repo).run_full_text_ops(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            review_mode="deterministic_only",
        )
    )

    assert result.agent_run_id is not None
    assert len(repo.list_claims(source_key="europe_pmc", include_seed_claims=True)) == claims_before
    assert repo.list_source_queries(source_key="europe_pmc") == source_queries_before
    assert repo.list_agent_runs(agent_name="full_text_ops_agent", status="completed")


def test_search_claims_uses_typed_contracts(tmp_path):
    service = make_service(tmp_path)

    results = service.search_claims(
        ClaimSearchRequest(query="propranolol", species="canine", min_confidence=0.1)
    )

    assert results.total == 1
    assert "Propranolol" in results.results[0].statement


def test_propose_hypothesis_defaults_to_draft(tmp_path):
    service = make_service(tmp_path)

    draft = service.propose_hypothesis(
        HypothesisProposalRequest(objective="propranolol in canine HSA", candidate_name="propranolol")
    )

    assert draft.status == "draft"
    assert draft.hypothesis_id is None
    assert draft.supporting_claim_ids


def test_commit_hypothesis_requires_explicit_call(tmp_path):
    service = make_service(tmp_path)
    draft = service.propose_hypothesis(
        HypothesisProposalRequest(objective="angiogenesis in canine HSA", target_name="VEGFA")
    )

    committed = service.commit_hypothesis(
        CommitHypothesisRequest(draft=draft, approved_by="test", approval_note="unit test")
    )

    assert committed.status == "approved"
    assert committed.hypothesis_id is not None
    assert committed.metadata["approved_by"] == "test"


def test_run_boltz_returns_approval_gated_handle(tmp_path):
    service = make_service(tmp_path)

    handle = service.run_boltz(
        BoltzRunRequest(target_name="cKDR", ligand_name="test ligand", ligand_smiles="CCO")
    )

    assert handle.status == "needs_approval"
    assert service.get_run_status(handle.run_id) == handle


def test_request_validation_can_queue_without_approval(tmp_path):
    service = make_service(tmp_path)

    handle = service.request_validation(
        ValidationRequest(
            validation_type="admet",
            candidate_name="propranolol",
            objective="Screen canine safety risk",
            require_approval=False,
        )
    )

    assert handle.status == "queued"
    assert service.get_candidate(CandidateDossierRequest(candidate_name="propranolol")) is not None


def test_local_pipeline_initializes_sources_and_queries(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)

    output = pipeline.initialize()
    coverage = pipeline.coverage()

    assert output["sources"] >= 4
    assert coverage["sources"] >= 4
    assert coverage["source_queries"] >= 4
    assert any(query.query_name == "licensed_full_text_hsa" for query in repo.list_source_queries("pmc_oa"))
    unpaywall_queries = repo.list_source_queries("unpaywall", active_only=False)
    assert any(query.query_name == "oa_discovery_hsa_titles" and query.active for query in unpaywall_queries)
    assert any(
        query.query_name == "human_vascular_sarcoma_trials"
        for query in repo.list_source_queries("clinicaltrials_gov")
    )
    assert any(query.query_name == "canine_hsa_trials" for query in repo.list_source_queries("avma_vctr"))
    assert any(query.query_name == "canine_hsa_cases" for query in repo.list_source_queries("icdc"))
    assert any(query.query_name == "canine_hsa_expression" for query in repo.list_source_queries("geo"))
    assert any(query.query_name == "canine_hsa_sequence_runs" for query in repo.list_source_queries("sra"))
    assert any(query.query_name == "priority_compounds" for query in repo.list_source_queries("pubchem"))
    assert any(query.query_name == "priority_compound_bioactivities" for query in repo.list_source_queries("chembl"))
    chembl_query = next(query for query in repo.list_source_queries("chembl") if query.query_name == "priority_compound_bioactivities")
    assert "CHEMBL279" in chembl_query.query_params["target_chembl_ids"]
    assert chembl_query.query_params["target_organisms"] == ["Homo sapiens", "Canis lupus familiaris"]
    assert chembl_query.query_params["include_cell_line_assays"] is True
    assert "sarcoma" in chembl_query.query_params["cell_line_terms"]
    pubchem_query = next(query for query in repo.list_source_queries("pubchem") if query.query_name == "priority_compounds")
    assert pubchem_query.query_params["require_exact_match"] is True
    assert any(query.query_name == "canine_human_priority_targets" for query in repo.list_source_queries("uniprot"))
    assert any(query.query_name == "priority_target_structures" for query in repo.list_source_queries("rcsb_pdb"))
    assert any(query.query_name == "priority_drug_safety" for query in repo.list_source_queries("openfda_animal_events"))


def test_structured_source_qa_reports_source_scoped_counts(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    raw_record = RawSourceRecord(
        source_key="pubchem",
        source_record_id="CID:4946",
        content_hash="pubchem-4946",
        source_url="https://pubchem.ncbi.nlm.nih.gov/compound/4946",
        raw_payload={"cid": 4946},
    )
    raw_record_id = repo.upsert_raw_record(raw_record)
    research_object = ResearchObject(
        object_type="compound_record",
        title="Propranolol",
        canonical_url="https://pubchem.ncbi.nlm.nih.gov/compound/4946",
        source_key="pubchem",
        raw_record_id=raw_record_id,
        dedupe_key="pubchem:4946",
        identifiers={"cid": "4946"},
    )
    object_id = repo.upsert_research_object(research_object, raw_record_id)
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="pubchem_identity",
            text_content="Propranolol has PubChem CID 4946.",
            content_hash="chunk-pubchem-4946",
        )
    )
    repo.upsert_claim(
        ClaimSearchResult(
            claim_id=uuid4(),
            statement="Propranolol has PubChem identity CID 4946.",
            claim_type=ClaimType.OTHER,
            direction=ClaimDirection.NEUTRAL,
            confidence=0.82,
            evidence_level=EvidenceLevel.IN_SILICO,
            source_object_id=object_id,
            source_title="Propranolol",
            source_url="https://pubchem.ncbi.nlm.nih.gov/compound/4946",
            support_count=1,
            metadata={"curation_status": "promote"},
        )
    )

    qa = structured_source_qa(repo, "pubchem")

    assert qa["raw_records"] == 1
    assert qa["research_objects"] == 1
    assert qa["document_chunks"] == 1
    assert qa["claims"] == 1
    assert qa["claim_status"] == {"promote": 1}
    assert qa["claim_types"] == {"other": 1}
    assert qa["passes_minimum_bar"] is True
    assert qa["sample_claims"][0]["curation_status"] == "promote"

    report = build_structured_source_count_report(repo, source_keys=["pubchem", "chembl"], sample_limit=1)

    assert report["source_keys"] == ["pubchem", "chembl"]
    assert report["totals"] == {
        "raw_records": 1,
        "research_objects": 1,
        "document_chunks": 1,
        "entity_mentions": 0,
        "claims": 1,
    }
    assert report["failed_sources"] == ["chembl"]
    assert report["passes_minimum_bar"] is False
    assert report["minimum_bar"] == {"require_claims": True}
    assert report["sources"][0]["sample_claims"][0]["statement"] == "Propranolol has PubChem identity CID 4946."

    source_health_report = build_structured_source_count_report(
        repo,
        source_keys=["pubchem"],
        sample_limit=1,
        require_claims=False,
    )

    assert source_health_report["failed_sources"] == []
    assert source_health_report["minimum_bar"] == {"require_claims": False}


def test_source_health_report_separates_failed_and_watch_sources(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_minimal_source_claim(
        repo,
        "pubchem",
        curation_status="needs_review",
        extraction_status="source_context",
    )

    report = build_source_health_report(repo, source_keys=["pubchem", "chembl"], sample_limit=1)
    pubchem = next(source for source in report["sources"] if source["source_key"] == "pubchem")
    chembl = next(source for source in report["sources"] if source["source_key"] == "chembl")

    assert report["source_keys"] == ["pubchem", "chembl"]
    assert report["summary"] == {
        "sources": 2,
        "healthy": 0,
        "triage": 0,
        "watch": 1,
        "failing": 1,
        "embedding_missing": 1,
        "source_followup_failed": 0,
        "source_followup_pending": 0,
        "sources_without_active_queries": 2,
    }
    assert report["failed_sources"] == ["chembl"]
    assert report["watch_sources"] == ["pubchem"]
    assert report["triage_sources"] == []
    assert pubchem["health_status"] == "watch"
    assert pubchem["source_role"] == "evidence"
    assert pubchem["health_score"] >= report["minimum_bar"]["min_health_score"]
    assert pubchem["passes_minimum_bar"] is True
    assert pubchem["claim_metadata"]["extraction_status"] == {"source_context": 1}
    assert pubchem["embedding_health"]["missing_chunks"] == 1
    assert pubchem["source_followup_health"]["failed"] == 0
    assert pubchem["source_query_health"]["active_source_queries"] == 0
    assert any("source-context" in risk for risk in pubchem["risks"])
    assert chembl["health_status"] == "failing"
    assert chembl["passes_minimum_bar"] is False


def test_source_health_report_includes_operational_readiness(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_minimal_source_claim(repo, "pubmed")
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="pubmed_hsa_active",
            query_text="canine hemangiosarcoma",
            active=True,
        )
    )
    repo.upsert_source_followup(
        SourceFollowupQueueItem(
            source_key="pubmed",
            identifier_type="doi",
            identifier="10.1000/hsa.1",
            status="failed",
            attempts=1,
            last_error="HTTP 429",
        )
    )

    report = build_source_health_report(repo, source_keys=["pubmed"], sample_limit=1)
    pubmed = report["sources"][0]

    assert report["embedding_missing_sources"] == ["pubmed"]
    assert report["source_followup_failed_sources"] == ["pubmed"]
    assert report["source_followup_pending_sources"] == []
    assert report["sources_without_active_queries"] == []
    assert pubmed["health_status"] == "watch"
    assert pubmed["embedding_health"]["available"] is True
    assert pubmed["embedding_health"]["total_chunks"] == 1
    assert pubmed["embedding_health"]["missing_chunks"] == 1
    assert pubmed["missing_embeddings"] == 1
    assert pubmed["source_followup_health"]["failed"] == 1
    assert pubmed["source_followup_failed"] == 1
    assert pubmed["source_followup_health"]["recent_failed"][0]["last_error"] == "HTTP 429"
    assert pubmed["source_query_health"]["active_source_queries"] == 1
    assert pubmed["active_source_queries"] == 1
    assert any("embedding_index_job" in action for action in pubmed["recommended_actions"])
    assert any("pubmed_source_followup_ingest_job" in action for action in pubmed["recommended_actions"])


def test_source_health_report_marks_complete_embeddings(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_minimal_source_claim(repo, "pubmed")
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="pubmed_hsa_active",
            query_text="canine hemangiosarcoma",
            active=True,
        )
    )
    chunk = repo.list_document_chunks(source_key="pubmed")[0]
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunk.id,
            research_object_id=chunk.research_object_id,
            chunk_index=chunk.chunk_index,
            source_key="pubmed",
            object_type="publication",
            content_hash=chunk.content_hash,
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[1.0, 0.0, 0.0],
        )
    )

    report = build_source_health_report(repo, source_keys=["pubmed"], sample_limit=1)
    pubmed = report["sources"][0]

    assert report["embedding_missing_sources"] == []
    assert pubmed["health_status"] == "healthy"
    assert pubmed["embedding_health"]["coverage_ratio"] == 1.0
    assert pubmed["missing_embeddings"] == 0
    assert "embedding_coverage_complete" in pubmed["signals"]
    assert "active_source_queries_present" in pubmed["signals"]


def test_source_health_report_marks_expected_triage_sources(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_minimal_source_claim(
        repo,
        "sra",
        curation_status="needs_review",
        extraction_status="source_context",
    )

    report = build_source_health_report(repo, source_keys=["sra"], sample_limit=1)
    sra = report["sources"][0]

    assert report["summary"] == {
        "sources": 1,
        "healthy": 0,
        "triage": 1,
        "watch": 0,
        "failing": 0,
        "embedding_missing": 1,
        "source_followup_failed": 0,
        "source_followup_pending": 0,
        "sources_without_active_queries": 1,
    }
    assert report["failed_sources"] == []
    assert report["triage_sources"] == ["sra"]
    assert report["watch_sources"] == []
    assert sra["source_role"] == "triage"
    assert sra["health_status"] == "triage"
    assert sra["passes_minimum_bar"] is True
    assert "triage_only_source" in sra["signals"]
    assert any("specialized triage agent" in action for action in sra["recommended_actions"])


def test_full_text_source_health_requires_body_chunks(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_minimal_source_claim(repo, "europe_pmc")

    report = build_source_health_report(repo, source_keys=["europe_pmc"], sample_limit=1)
    europe_pmc = report["sources"][0]

    assert report["failed_sources"] == ["europe_pmc"]
    assert report["passes_minimum_bar"] is False
    assert europe_pmc["full_text_qa"]["passes_full_text_bar"] is False
    assert europe_pmc["full_text_qa"]["triage"]["action"] == "needs_parser_fix"
    assert europe_pmc["full_text_triage_action"] == "needs_parser_fix"
    assert europe_pmc["minimum_bar"]["full_text_required_passes"] is False
    assert report["full_text_blocking_sources"] == ["europe_pmc"]
    assert any("Full-text source lacks" in risk for risk in europe_pmc["risks"])
    assert any("Full-text triage action: needs_parser_fix" in risk for risk in europe_pmc["risks"])


def test_full_text_source_count_report_passes_with_body_chunks(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")

    report = build_structured_source_count_report(repo, source_keys=["europe_pmc"], sample_limit=1)
    qa = full_text_source_qa(repo, "europe_pmc")

    assert report["failed_sources"] == []
    assert report["passes_minimum_bar"] is True
    assert report["sources"][0]["full_text_qa"]["passes_full_text_bar"] is True
    assert report["sources"][0]["full_text_triage_action"] == "no_action"
    assert qa["full_text_research_objects"] == 1
    assert qa["full_text_document_chunks"] == 1
    assert qa["triage"]["action"] == "no_action"
    current_run_qa = full_text_source_qa(repo, "europe_pmc", ingestion_results=[])
    assert current_run_qa["passes_full_text_bar"] is False
    assert current_run_qa["triage"]["action"] == "retry_later"


def test_full_text_qa_uses_body_chunks_as_persisted_gate(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    raw_record = RawSourceRecord(
        source_key="pmc_oa",
        source_record_id="pmc_oa:metadata-gap",
        content_hash="pmc-oa-metadata-gap-raw",
        source_url="https://example.org/pmc_oa/metadata-gap",
        raw_payload={"source_key": "pmc_oa", "full_text": "Full text body is persisted."},
    )
    raw_record_id = repo.upsert_raw_record(raw_record)
    research_object = ResearchObject(
        object_type="publication",
        title="PMC OA full text with missing object flag",
        canonical_url="https://example.org/pmc_oa/metadata-gap",
        source_key="pmc_oa",
        raw_record_id=raw_record_id,
        dedupe_key="pmc_oa:metadata-gap",
        metadata={},
    )
    object_id = repo.upsert_research_object(research_object, raw_record_id)
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="full_text:results",
            text_content="Full text body mentions canine hemangiosarcoma.",
            content_hash="pmc-oa-metadata-gap-chunk",
        )
    )

    qa = full_text_source_qa(repo, "pmc_oa")
    current_qa = full_text_source_qa(
        repo,
        "pmc_oa",
        ingestion_results=[
            {
                "source_key": "pmc_oa",
                "query_name": "metadata_gap_current_run",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 1,
                "full_text_research_objects": 0,
                "section_chunk_counts": {"full_text:results": 1},
                "status": "completed",
                "errors": [],
            }
        ],
    )

    assert qa["full_text_research_objects"] == 0
    assert qa["full_text_document_chunks"] == 1
    assert qa["passes_persisted_full_text_bar"] is True
    assert qa["passes_full_text_bar"] is True
    assert current_qa["passes_current_full_text_bar"] is True
    assert current_qa["triage"]["action"] == "no_action"


def test_full_text_triage_agent_accepts_clean_body_chunks():
    result = FullTextTriageAgent().triage(
        FullTextTriageRequest(
            source_key="europe_pmc",
            stage="qa",
            raw_records=1,
            research_objects=1,
            document_chunks=2,
            full_text_document_chunks=1,
            full_text_body_chars=1000,
        )
    )

    assert result.action == "no_action"
    assert result.severity == "info"
    assert result.should_retry is False
    assert result.should_block_schedule is False


def test_full_text_triage_agent_reduces_batch_on_timeout():
    result = FullTextTriageAgent().triage(
        FullTextTriageRequest(
            source_key="europe_pmc",
            stage="dagster_run",
            error_message="Timed out after 2700 seconds",
            runtime_seconds=2700,
            timeout_seconds=2700,
            raw_records=10,
            research_objects=10,
            document_chunks=10,
        )
    )

    assert result.action == "reduce_batch_size"
    assert result.severity == "watch"
    assert result.should_retry is True
    assert result.should_block_schedule is True
    assert any("source/date partitioning" in action for action in result.recommended_next_actions)


def test_full_text_triage_agent_allows_empty_date_partition():
    result = FullTextTriageAgent().triage(
        FullTextTriageRequest(
            source_key="europe_pmc",
            stage="qa",
            raw_records=0,
            metadata={"allow_empty_current_run": True},
        )
    )

    assert result.action == "no_action"
    assert result.severity == "info"
    assert result.should_retry is False
    assert result.should_block_schedule is False


def test_full_text_triage_agent_flags_parser_fixture():
    result = FullTextTriageAgent().triage(
        FullTextTriageRequest(
            source_key="pmc_oa",
            stage="parse",
            error_message="XML parse error: unsupported JATS body shape",
            raw_records=1,
            research_objects=1,
            document_chunks=1,
            full_text_document_chunks=0,
        )
    )

    assert result.action == "needs_parser_fix"
    assert result.severity == "blocking"
    assert result.should_block_schedule is True
    assert any("fixture" in action for action in result.recommended_next_actions)


def test_service_triages_full_text_issue(tmp_path):
    service = make_service(tmp_path)

    result = service.triage_full_text_issue(
        FullTextTriageRequest(
            source_key="pmc_oa",
            stage="fetch",
            error_message="429 too many requests",
            http_status=429,
        )
    )

    assert result.action == "retry_later"
    assert result.should_retry is True
    assert result.should_block_schedule is False


def test_entity_resolution_persists_entities_aliases_and_mentions_idempotently(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:1",
            content_hash="entity-resolution-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/1/",
            raw_payload={"pmid": "1"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Entity resolution example",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            dedupe_key="pmid:1",
            identifiers={"pmid": "1"},
        ),
        raw_record_id,
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Adriamycin is discussed with VEGF receptor 2 and KIT, but not kitchen, in human angiosarcoma.",
            content_hash="entity-resolution-chunk",
        )
    )

    result = resolve_entities_for_repository(repo, source_key="pubmed")
    second_result = resolve_entities_for_repository(repo, source_key="pubmed")
    entities = repo.list_entities()
    mentions = repo.list_entity_mentions(source_key="pubmed")

    assert result.errors == []
    assert result.chunks_seen == 1
    assert result.mentions_upserted >= 4
    assert second_result.errors == []
    assert {entity.canonical_name for entity in entities} >= {"doxorubicin", "KDR", "KIT"}
    assert len(mentions) == len({mention.mention_id for mention in mentions})
    assert len(mentions) == len(repo.list_entity_mentions(chunk_id=chunk.id))
    assert sum(1 for mention in mentions if mention.canonical_name == "KIT") == 1
    assert normalize_entity_key("compound", "propranolol", {"pubchem_cid": "4946"}) == "pubchem_cid:4946"
    coverage = repo.coverage_summary()
    assert coverage["entity_aliases"] >= 1
    assert coverage["entity_mentions"] == len(mentions)
    assert repo.source_runtime_summary("pubmed")["entity_mentions"] == len(mentions)


def test_research_brief_contracts_require_known_citations():
    citation = ResearchBriefCitation(
        citation_id="C1",
        chunk_id=uuid4(),
        research_object_id=uuid4(),
        quote="Canine hemangiosarcoma evidence quote.",
    )
    finding = ResearchBriefFinding(
        claim="VEGF biology is relevant enough to brief.",
        stance="supporting",
        citations=["C1"],
        evidence_strength="medium",
        reasoning="The finding is tied to a stored citation.",
    )

    report = ResearchBriefPerspectiveReport(
        perspective="evidence_scout",
        agent_name="evidence_scout_agent",
        summary="Evidence was found.",
        findings=[finding],
        citations=[citation],
    )
    result = ResearchBriefResult(
        topic="VEGF therapy",
        disease_scope="canine hemangiosarcoma",
        perspective_reports=[report],
        final_brief="The stored evidence supports review [C1].",
        ranked_hypotheses=[finding],
        citations=[citation],
    )

    assert result.final_brief.endswith("[C1].")
    brief_record = ResearchBriefRecord(
        agent_run_id=uuid4(),
        agent_run_ids=[uuid4()],
        topic="VEGF therapy",
        disease_scope="canine hemangiosarcoma",
        source_key="pubmed",
        review_mode="deterministic_only",
        final_brief=result.final_brief,
        result_payload=result.model_dump(mode="json"),
        citation_count=1,
        finding_count=1,
        hypothesis_count=1,
    )
    assert brief_record.status == "completed"
    assert brief_record.citation_count == 1
    assert brief_record.hard_error_count == 0
    assert brief_record.evidence_limitation_count == 0
    with pytest.raises(ValueError):
        ResearchBriefPerspectiveReport(
            perspective="evidence_scout",
            agent_name="evidence_scout_agent",
            summary="Bad citation.",
            findings=[finding.model_copy(update={"citations": ["C2"]})],
            citations=[citation],
        )
    with pytest.raises(ValueError):
        ResearchBriefResult(
            topic="VEGF therapy",
            disease_scope="canine hemangiosarcoma",
            final_brief="The stored evidence supports review.",
            citations=[citation],
        )


def test_research_brief_record_splits_legacy_errors_from_evidence_limitations():
    record = ResearchBriefRecord(
        topic="VEGF therapy",
        disease_scope="canine hemangiosarcoma",
        source_key="pubmed",
        review_mode="openrouter_required",
        final_brief="The stored evidence supports review [C1].",
        result_payload={
            "errors": [
                "No supplied citation directly addresses survival outcome; evidence is indirect.",
                "OpenRouter request failed: timeout",
            ]
        },
        error_count=2,
    )

    assert record.hard_error_count == 1
    assert record.evidence_limitation_count == 1


def test_research_brief_evaluation_contract_rejects_invalid_values():
    result = ResearchBriefEvaluationResult(
        brief_id=uuid4(),
        topic="VEGF therapy",
        overall_score=0.8,
        citation_coverage_score=0.8,
        perspective_balance_score=0.8,
        contradiction_handling_score=0.8,
        novelty_score=0.8,
        actionability_score=0.8,
        weakness_transparency_score=0.8,
        passes_quality_bar=True,
        readiness="ready_for_hypothesis_review",
    )
    record = ResearchBriefEvaluationRecord(
        evaluation_id=result.evaluation_id,
        brief_id=result.brief_id,
        topic=result.topic,
        overall_score=result.overall_score,
        passes_quality_bar=result.passes_quality_bar,
        readiness=result.readiness,
        result_payload=result.model_dump(mode="json"),
    )

    assert record.readiness == "ready_for_hypothesis_review"
    with pytest.raises(ValueError):
        ResearchBriefEvaluationResult(
            brief_id=uuid4(),
            topic="VEGF therapy",
            overall_score=1.2,
            citation_coverage_score=0.8,
            perspective_balance_score=0.8,
            contradiction_handling_score=0.8,
            novelty_score=0.8,
            actionability_score=0.8,
            weakness_transparency_score=0.8,
            readiness="ready_for_hypothesis_review",
        )
    with pytest.raises(ValueError):
        ResearchBriefEvaluationRecord(
            brief_id=uuid4(),
            topic="VEGF therapy",
            overall_score=0.5,
            readiness="not_real",
        )


def test_research_brief_evaluation_openrouter_judge_uses_model_payload(monkeypatch):
    citation = ResearchBriefCitation(
        citation_id="C1",
        chunk_id=uuid4(),
        research_object_id=uuid4(),
        source_key="pubmed",
        title="PD-1 and VEGFR-2 translational evidence",
        quote="PD-1 and VEGFR-2 evidence supports a testable translational hypothesis.",
    )
    finding = ResearchBriefFinding(
        claim="PD-1 plus VEGFR-2 blockade has a testable translational rationale.",
        stance="supporting",
        citations=["C1"],
        evidence_strength="medium",
        reasoning="The cited evidence connects immune checkpoint and angiogenic biology.",
    )
    brief_payload = ResearchBriefResult(
        topic="PD-1 plus VEGFR-2 in canine hemangiosarcoma",
        disease_scope="canine hemangiosarcoma and human angiosarcoma",
        final_brief="The synthesis is actionable and cited [C1].",
        citations=[citation],
        perspective_reports=[
            ResearchBriefPerspectiveReport(
                perspective="evidence_scout",
                agent_name="evidence_scout_agent",
                summary="Evidence scout found cited rationale.",
                findings=[finding],
                citations=[citation],
            ),
            ResearchBriefPerspectiveReport(
                perspective="translational_hypothesis",
                agent_name="translational_hypothesis_agent",
                summary="Translational hypothesis is testable.",
                findings=[finding],
                citations=[citation],
            ),
            ResearchBriefPerspectiveReport(
                perspective="skeptic_validation",
                agent_name="skeptic_validation_agent",
                summary="Skeptic view flagged no blocking contradiction.",
                findings=[finding.model_copy(update={"stance": "uncertain"})],
                citations=[citation],
            ),
        ],
        ranked_hypotheses=[finding],
        unresolved_questions=["Confirm effect size and assay readout."],
    )
    brief = ResearchBriefRecord(
        topic=brief_payload.topic,
        disease_scope=brief_payload.disease_scope,
        source_key="pubmed",
        final_brief=brief_payload.final_brief,
        citation_count=1,
        finding_count=3,
        hypothesis_count=1,
        result_payload=brief_payload.model_dump(mode="json"),
    )

    def fake_openrouter(model_name, review_payload):
        assert model_name == "test/model"
        assert review_payload["brief"]["brief_id"] == str(brief.brief_id)
        return {
            "text": json.dumps(
                {
                    "overall_score": 0.91,
                    "citation_coverage_score": 0.9,
                    "perspective_balance_score": 0.88,
                    "contradiction_handling_score": 0.82,
                    "novelty_score": 0.86,
                    "actionability_score": 0.93,
                    "weakness_transparency_score": 0.78,
                    "passes_quality_bar": True,
                    "readiness": "ready_for_hypothesis_review",
                    "strengths": ["Cited and actionable."],
                    "weaknesses": ["Needs assay confirmation."],
                    "recommendations": ["Promote into validation planning."],
                    "evidence": {"agent_review_summary": "Model judged the brief ready."},
                    "errors": [],
                }
            ),
            "metadata": {"provider": "openrouter", "requested_model": model_name},
        }

    monkeypatch.setattr(research_brief_evaluation, "_openrouter_review_model", fake_openrouter)

    result = research_brief_evaluation.evaluate_research_brief_synthesis(
        brief,
        ResearchBriefEvaluationRequest(
            brief_id=brief.brief_id,
            review_mode="openrouter_required",
            review_models=["test/model"],
        ),
    )

    assert result.overall_score == 0.91
    assert result.passes_quality_bar is True
    assert result.readiness == "ready_for_hypothesis_review"
    assert result.evidence["model_review"]["requested_model"] == "test/model"
    assert result.evidence["deterministic_floor"]["brief_id"] == str(brief.brief_id)


def test_research_brief_evaluation_requires_ready_readiness_to_pass_quality_bar():
    brief = ResearchBriefRecord(
        topic="Toceranib PK evidence",
        disease_scope="canine hemangiosarcoma and human angiosarcoma",
        source_key="pubmed",
        final_brief="Evidence is incomplete [C1].",
        citation_count=1,
        finding_count=1,
        hypothesis_count=1,
    )
    deterministic = ResearchBriefEvaluationResult(
        brief_id=brief.brief_id,
        topic=brief.topic,
        source_key=brief.source_key,
        overall_score=1.0,
        citation_coverage_score=1.0,
        perspective_balance_score=1.0,
        contradiction_handling_score=1.0,
        novelty_score=1.0,
        actionability_score=1.0,
        weakness_transparency_score=1.0,
        passes_quality_bar=True,
        readiness="ready_for_hypothesis_review",
    )

    result = research_brief_evaluation._evaluation_from_model(
        brief,
        ResearchBriefEvaluationRequest(brief_id=brief.brief_id, review_mode="openrouter_required"),
        deterministic,
        {
            "text": json.dumps(
                {
                    "overall_score": 0.76,
                    "citation_coverage_score": 0.7,
                    "perspective_balance_score": 0.7,
                    "contradiction_handling_score": 0.7,
                    "novelty_score": 0.7,
                    "actionability_score": 0.7,
                    "weakness_transparency_score": 0.7,
                    "passes_quality_bar": True,
                    "readiness": "needs_more_evidence",
                    "strengths": [],
                    "weaknesses": ["Primary question remains indirect."],
                    "recommendations": ["Run a narrower evidence search."],
                    "evidence": {},
                    "errors": [],
                }
            ),
            "metadata": {"provider": "openrouter"},
        },
    )

    assert result.readiness == "needs_more_evidence"
    assert result.passes_quality_bar is False
    assert "model_quality_bar_overridden" in result.evidence


def test_research_brief_followup_queue_contracts_validate():
    lead = ResearchLeadRecord(
        identity_key="research_lead:brief_followup:test",
        title="Follow up evidence limitation",
        status="followup",
        evidence_refs=["research_brief:abc"],
    )
    result = ResearchBriefFollowupQueueResult(
        candidate_brief_count=1,
        limitation_count=1,
        queued_count=1,
        followup_leads=[lead],
    )

    assert ResearchBriefFollowupQueueRequest(limit=25).max_limitations_per_brief == 20
    assert result.followup_leads[0].status == "followup"
    with pytest.raises(ValueError):
        ResearchBriefFollowupQueueRequest(max_limitations_per_brief=0)


def test_research_brief_repository_roundtrip_sqlite_and_memory(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "research-brief-ledger.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        record = ResearchBriefRecord(
            agent_run_id=uuid4(),
            topic="VEGF therapy in canine hemangiosarcoma",
            disease_scope="canine hemangiosarcoma and human angiosarcoma",
            source_key="pubmed",
            brief_style="technical",
            model_profile="research_brief",
            review_mode="deterministic_only",
            final_brief="Stored synthesis [C1].",
            summary={"finding_count": 1},
            result_payload={"final_brief": "Stored synthesis [C1]."},
            citation_count=1,
            finding_count=1,
            research_lead_count=2,
            hard_error_count=1,
            evidence_limitation_count=3,
            error_count=1,
        )

        saved = repo.upsert_research_brief(record)
        fetched = repo.get_research_brief(saved.brief_id)
        listed = repo.list_research_briefs(source_key="pubmed", topic_query="vegf")

        assert fetched is not None
        assert fetched.brief_id == saved.brief_id
        assert fetched.result_payload["final_brief"] == "Stored synthesis [C1]."
        assert fetched.hard_error_count == 1
        assert fetched.evidence_limitation_count == 3
        assert fetched.error_count == 1
        assert listed[0].brief_id == saved.brief_id
        assert repo.list_research_briefs(status="archived") == []


def test_research_brief_evaluation_repository_roundtrip_sqlite_and_memory(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "research-brief-evaluations.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        brief_id = uuid4()
        evaluation = ResearchBriefEvaluationRecord(
            brief_id=brief_id,
            agent_run_id=uuid4(),
            topic="VEGF therapy in canine hemangiosarcoma",
            source_key="pubmed",
            overall_score=0.82,
            passes_quality_bar=True,
            readiness="ready_for_hypothesis_review",
            summary={"overall_score": 0.82},
            result_payload={"overall_score": 0.82, "recommendations": ["Promote."]},
        )

        saved = repo.upsert_research_brief_evaluation(evaluation)
        fetched = repo.get_research_brief_evaluation(saved.evaluation_id)
        listed = repo.list_research_brief_evaluations(
            brief_id=brief_id,
            readiness="ready_for_hypothesis_review",
            passes_quality_bar=True,
        )

        assert fetched is not None
        assert fetched.evaluation_id == saved.evaluation_id
        assert fetched.result_payload["overall_score"] == 0.82
        assert listed[0].evaluation_id == saved.evaluation_id
        assert repo.list_research_brief_evaluations(passes_quality_bar=False) == []


def test_research_brief_quality_report_joins_latest_evaluations(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "research-brief-quality.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        service = HSAResearchService(repo)
        ready_brief = repo.upsert_research_brief(
            ResearchBriefRecord(
                agent_run_id=uuid4(),
                topic="VEGF therapy in canine hemangiosarcoma",
                disease_scope="canine hemangiosarcoma and human angiosarcoma",
                source_key="pubmed",
                review_mode="openrouter_required",
                final_brief="Stored synthesis [C1].",
                citation_count=3,
                finding_count=2,
                hypothesis_count=1,
                result_payload={
                    "errors": [
                        "No supplied citation directly addresses clinical trial outcome; evidence is indirect."
                    ]
                },
                error_count=1,
                metadata={"review_models": ["anthropic/claude-sonnet-test"]},
            )
        )
        failed_brief = repo.upsert_research_brief(
            ResearchBriefRecord(
                topic="Evidence-light linked article",
                disease_scope="canine hemangiosarcoma and human angiosarcoma",
                source_key="x_linked_article",
                status="failed",
                review_mode="openrouter_required",
                error_count=4,
            )
        )
        followup_brief = repo.upsert_research_brief(
            ResearchBriefRecord(
                topic="Conference-only angiosarcoma lead",
                disease_scope="canine hemangiosarcoma and human angiosarcoma",
                source_key="x_linked_article",
                status="completed",
                review_mode="openrouter_required",
                result_payload={
                    "evidence_limitations": [
                        "Only a conference abstract was supplied; find durable peer-reviewed evidence."
                    ],
                    "errors": [],
                },
                evidence_limitation_count=1,
            )
        )
        repo.upsert_research_brief_evaluation(
            ResearchBriefEvaluationRecord(
                brief_id=ready_brief.brief_id,
                agent_run_id=uuid4(),
                topic=ready_brief.topic,
                source_key="pubmed",
                overall_score=0.88,
                passes_quality_bar=True,
                readiness="ready_for_hypothesis_review",
                summary={"overall_score": 0.88},
                result_payload={"recommendations": ["Promote to validation."]},
            )
        )

        report = service.build_research_brief_quality_report(
            ResearchBriefQualityReportRequest(limit=10)
        )
        rows_by_id = {row.brief_id: row for row in report.rows}

        assert report.brief_count == 3
        assert report.evaluated_count == 1
        assert report.ready_count == 1
        assert report.failed_count == 1
        assert report.followup_count == 1
        assert report.average_overall_score == pytest.approx(0.88)
        assert rows_by_id[ready_brief.brief_id].quality_status == "ready_for_validation"
        assert rows_by_id[ready_brief.brief_id].review_models == ["anthropic/claude-sonnet-test"]
        assert rows_by_id[ready_brief.brief_id].error_count == 0
        assert rows_by_id[ready_brief.brief_id].hard_error_count == 0
        assert rows_by_id[ready_brief.brief_id].evidence_limitation_count == 1
        assert rows_by_id[failed_brief.brief_id].quality_status == "brief_failed"
        assert rows_by_id[followup_brief.brief_id].quality_status == "needs_followup_research"


def test_research_brief_followup_queue_creates_idempotent_followup_leads(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-followup-queue.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="AACR angiosarcoma abstract",
            disease_scope="canine hemangiosarcoma and human angiosarcoma",
            source_key="x_linked_article",
            status="completed",
            review_mode="openrouter_required",
            result_payload={
                "evidence_limitations": [
                    "Only a conference abstract was supplied; find durable peer-reviewed evidence.",
                    "No PMID, DOI, PMCID, or NCT identifier was available.",
                ],
                "errors": [],
            },
            evidence_limitation_count=2,
        )
    )

    result = service.queue_research_brief_followups(
        ResearchBriefFollowupQueueRequest(limit=10)
    )
    rerun = service.queue_research_brief_followups(
        ResearchBriefFollowupQueueRequest(limit=10)
    )
    leads = repo.list_research_leads(status="followup", limit=10)

    assert result.candidate_brief_count == 1
    assert result.limitation_count == 2
    assert result.queued_count == 2
    assert result.existing_count == 0
    assert rerun.queued_count == 0
    assert rerun.existing_count == 2
    assert len(leads) == 2
    assert {lead.origin_record_id for lead in leads} == {str(brief.brief_id)}
    assert all(f"research_brief:{brief.brief_id}" in lead.evidence_refs for lead in leads)
    assert all(lead.metadata["research_followup_queue"]["origin"] == "research_brief_quality" for lead in leads)


def test_validation_plan_repository_roundtrip_sqlite_and_memory(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "validation-plans.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        brief_id = uuid4()
        evaluation_id = uuid4()
        record = ValidationPlanRecord(
            brief_id=brief_id,
            evaluation_id=evaluation_id,
            agent_run_id=uuid4(),
            topic="VEGF validation path",
            source_key="pubmed",
            status="ready_for_review",
            readiness="ready_for_expert_review",
            task_count=2,
            hypothesis_count=1,
            result_payload={"plan_id": "payload"},
            summary={"task_count": 2},
        )

        saved = repo.upsert_validation_plan(record)
        fetched = repo.get_validation_plan(saved.plan_id)
        listed = repo.list_validation_plans(
            brief_id=brief_id,
            evaluation_id=evaluation_id,
            status="ready_for_review",
            readiness="ready_for_expert_review",
            limit=1,
        )

        assert fetched is not None
        assert fetched.plan_id == saved.plan_id
        assert listed[0].plan_id == saved.plan_id
        assert repo.list_validation_plans(status="blocked") == []


def test_validation_request_queue_repository_roundtrip_sqlite_and_memory(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "validation-request-queue.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        plan_id = uuid4()
        task_id = uuid4()
        item = repo.upsert_validation_request_queue_item(
            ValidationRequestQueueItem(
                plan_id=plan_id,
                task_id=task_id,
                brief_id=uuid4(),
                source_key="pubmed",
                topic="VEGF validation path",
                task_type="expert_review",
                title="Review target validation",
                objective="Review whether this target is ready for validation.",
                rationale="The plan is source-traceable.",
                priority=25,
                validation_request=ValidationRequest(
                    validation_type="expert_review",
                    target_name="VEGFA",
                    objective="Review whether this target is ready for validation.",
                ),
            )
        )
        duplicate = repo.upsert_validation_request_queue_item(
            item.model_copy(update={"priority": 50})
        )
        updated = repo.update_validation_request_queue_item(
            item.queue_item_id,
            status="approved",
            approved_by="unit-test",
            approval_note="Looks actionable.",
            quality_gates=["approval_required"],
            dispatch_blockers=["assay_context_required"],
        )
        listed = repo.list_validation_request_queue_items(
            plan_id=plan_id,
            status="approved",
            source_key="pubmed",
            task_type="expert_review",
            topic_query="VEGF",
            limit=1,
        )

        assert duplicate.queue_item_id == item.queue_item_id
        assert updated is not None
        assert updated.status == "approved"
        assert updated.approved_by == "unit-test"
        assert updated.approval_note == "Looks actionable."
        assert updated.quality_gates == ["approval_required"]
        assert updated.dispatch_blockers == ["assay_context_required"]
        assert listed[0].queue_item_id == item.queue_item_id
        assert repo.get_validation_request_queue_item(item.queue_item_id).queue_item_id == item.queue_item_id


def test_research_brief_queue_contract_and_repository_roundtrip(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "research-brief-queue.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        item = repo.upsert_research_brief_queue_item(
            ResearchBriefQueueItem(
                topic=" VEGF therapy in canine hemangiosarcoma ",
                disease_scope="canine hemangiosarcoma",
                source_key="pubmed",
                priority=10,
                review_mode="deterministic_only",
                review_models=["model-a", "model-a"],
            )
        )
        duplicate = repo.upsert_research_brief_queue_item(
            ResearchBriefQueueItem(
                topic="VEGF therapy in canine hemangiosarcoma",
                disease_scope="canine hemangiosarcoma",
                source_key="pubmed",
                priority=20,
                review_mode="deterministic_only",
            )
        )
        updated = repo.update_research_brief_queue_item(
            item.queue_item_id,
            status="running",
            priority=5,
            attempts=1,
            metadata={"runner": "test"},
        )

        assert item.identity_key is not None
        assert item.topic == "VEGF therapy in canine hemangiosarcoma"
        assert item.review_models == ["model-a"]
        assert duplicate.queue_item_id == item.queue_item_id
        assert updated is not None
        assert updated.status == "running"
        assert updated.priority == 5
        assert updated.attempts == 1
        assert updated.metadata["runner"] == "test"
        assert repo.get_research_brief_queue_item(item.queue_item_id).queue_item_id == item.queue_item_id
        assert repo.list_research_brief_queue_items(status="running", source_key="pubmed", topic_query="vegf")[0].queue_item_id == item.queue_item_id


def test_research_brief_queue_controls_requeue_and_archive(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "research-brief-queue-controls.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        service = HSAResearchService(repo)
        queued = service.queue_research_brief(
            ResearchBriefQueueRequest(
                topic="Angiogenesis resistance patterns in canine hemangiosarcoma",
                source_key="pubmed",
                priority=50,
            )
        )
        failed = repo.update_research_brief_queue_item(
            queued.queue_item_id,
            status="failed",
            attempts=2,
            last_error="timeout",
        )

        assert failed is not None
        with pytest.raises(ValueError, match="priority must be between 0 and 1000"):
            service.requeue_research_brief_queue_item(failed.queue_item_id, priority=1001)

        requeued = service.requeue_research_brief_queue_item(failed.queue_item_id, priority=5)
        assert requeued is not None
        assert requeued.status == "queued"
        assert requeued.priority == 5
        assert requeued.attempts == 2
        assert requeued.last_error is None
        assert requeued.metadata["queue_control"]["last_action"] == "requeue"
        assert requeued.metadata["queue_control"]["previous_status"] == "failed"

        with pytest.raises(ValueError, match="only completed"):
            service.archive_research_brief_queue_item(requeued.queue_item_id)

        completed = repo.update_research_brief_queue_item(requeued.queue_item_id, status="completed")
        assert completed is not None
        archived = service.archive_research_brief_queue_item(completed.queue_item_id)
        assert archived is not None
        assert archived.status == "archived"
        assert archived.priority == 5
        assert archived.metadata["queue_control"]["last_action"] == "archive"
        assert archived.metadata["queue_control"]["previous_status"] == "completed"
        assert service.archive_research_brief_queue_item(archived.queue_item_id).status == "archived"
        assert service.requeue_research_brief_queue_item(uuid4()) is None


def test_research_brief_queue_maintenance_archives_stale_failed_items(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "research-brief-queue-maintenance.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        service = HSAResearchService(repo)
        stale_failed = service.queue_research_brief(
            ResearchBriefQueueRequest(
                topic="Stale linked article angiosarcoma review",
                source_key="x_linked_article",
                priority=80,
            )
        )
        fresh_failed = service.queue_research_brief(
            ResearchBriefQueueRequest(
                topic="Fresh PubMed angiosarcoma review",
                source_key="pubmed",
                priority=80,
            )
        )
        repo.update_research_brief_queue_item(
            stale_failed.queue_item_id,
            status="failed",
            attempts=2,
            last_error="old evidence-light item",
        )
        repo.update_research_brief_queue_item(
            fresh_failed.queue_item_id,
            status="failed",
            attempts=0,
            last_error="fresh failure",
        )

        dry_run = service.maintain_research_brief_queue(
            ResearchBriefQueueMaintenanceRequest(
                statuses=["failed"],
                source_key="x_linked_article",
                min_attempts=1,
                max_updated_age_hours=0,
                dry_run=True,
            )
        )
        assert dry_run.dry_run is True
        assert dry_run.candidate_count == 1
        assert dry_run.archived_count == 0
        assert dry_run.queue_items[0].queue_item_id == stale_failed.queue_item_id
        assert repo.get_research_brief_queue_item(stale_failed.queue_item_id).status == "failed"

        archived = service.maintain_research_brief_queue(
            ResearchBriefQueueMaintenanceRequest(
                statuses=["failed"],
                source_key="x_linked_article",
                min_attempts=1,
                max_updated_age_hours=0,
                dry_run=False,
                reason="superseded_by_pubmed_backed_synthesis",
            )
        )
        updated = repo.get_research_brief_queue_item(stale_failed.queue_item_id)
        untouched = repo.get_research_brief_queue_item(fresh_failed.queue_item_id)

        assert archived.archived_count == 1
        assert archived.queue_items[0].status == "archived"
        assert updated.status == "archived"
        assert updated.last_error == "old evidence-light item"
        assert updated.metadata["queue_control"]["last_action"] == "maintenance_archive"
        assert updated.metadata["queue_control"]["reason"] == "superseded_by_pubmed_backed_synthesis"
        assert untouched.status == "failed"


def test_research_brief_queue_maintenance_rejects_active_statuses():
    with pytest.raises(ValidationError, match="cannot target queued or running"):
        ResearchBriefQueueMaintenanceRequest(statuses=["queued"])


def test_research_brief_queue_batch_from_leads_and_source_health(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-queue-batch.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="VEGF resistance signal in canine hemangiosarcoma",
            lead_type="linked_article",
            status="new",
            priority=20,
            source_key="x_topic",
            origin_source_key="x_topic",
            reason="Agent flagged a durable article for synthesis follow-up.",
            summary="The linked article discusses VEGF resistance and translational therapy signals.",
            topic_tags=["VEGF", "resistance"],
            suggested_sources=["pubmed"],
        )
    )
    source_health_report = {
        "sources": [
            {
                "source_key": "chembl",
                "health_status": "failing",
                "document_chunks": 0,
                "passes_minimum_bar": False,
                "health_score": 0.2,
                "risks": ["No chunks are present."],
                "recommended_actions": ["Run structured source refresh."],
            },
            {
                "source_key": "pubmed",
                "health_status": "triage",
                "document_chunks": 4,
                "passes_minimum_bar": False,
                "health_score": 0.55,
                "risks": ["No promoted claims are present."],
                "recommended_actions": ["Inspect curator decisions."],
            },
        ]
    }

    result = service.queue_research_brief_batch(
        ResearchBriefQueueBatchRequest(
            mode="both",
            source_health_report=source_health_report,
            limit=5,
            priority=80,
        )
    )
    updated_lead = repo.get_research_lead(lead.lead_id)
    origins = {item.metadata["batch_queue"]["origin"] for item in result.queue_items}

    assert result.queued_count == 2
    assert result.lead_count == 1
    assert result.research_followup_count == 0
    assert result.source_health_count == 1
    assert result.skipped_count == 1
    assert result.skipped[0]["source_key"] == "chembl"
    assert origins == {"research_lead", "source_health"}
    assert updated_lead is not None
    assert updated_lead.status == "queued"
    assert updated_lead.metadata["research_brief_queue"]["queue_item_id"]
    assert any(item.source_key == "pubmed" for item in result.queue_items)
    assert any(item.priority == 20 for item in result.queue_items if item.metadata["batch_queue"]["origin"] == "research_lead")
    assert any(item.priority == 45 for item in result.queue_items if item.metadata["batch_queue"]["origin"] == "source_health")


def test_research_brief_queue_batch_routes_evidence_light_leads_to_followup(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-evidence-light-followup.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="UF angiosarcoma AACR report",
            url="https://cancer.ufl.edu/angiosarcoma-report",
            lead_type="institutional_article",
            status="new",
            priority=25,
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
            reason="No DOI, PMID, PMCID, NCT, or suggested durable source was found.",
            topic_tags=["angiosarcoma", "therapy"],
        )
    )

    result = service.queue_research_brief_batch(
        ResearchBriefQueueBatchRequest(mode="research_leads", limit=5)
    )
    updated_lead = repo.get_research_lead(lead.lead_id)

    assert result.queued_count == 0
    assert result.lead_count == 0
    assert result.research_followup_count == 1
    assert result.skipped_count == 1
    assert result.skipped[0]["reason"] == "lead_needs_research_followup"
    assert result.skipped[0]["requires_manual_research"] is True
    assert updated_lead is not None
    assert updated_lead.status == "followup"
    assert updated_lead.metadata["research_followup_queue"]["requires_manual_research"] is True
    assert repo.list_source_followups(limit=10) == []


def test_research_brief_queue_batch_filters_research_leads_by_source_key(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-source-filter.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_minimal_source_claim(repo, "pubmed")
    selected = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Resolved research brief quality follow-up",
            lead_type="unknown",
            status="watching",
            priority=25,
            source_key="pubmed",
            origin_source_key="research_brief_quality",
            suggested_sources=["pubmed"],
            reason="Durable chunks satisfy the prior evidence limitation.",
            evidence_refs=["chunk:pubmed:1"],
        )
    )
    ignored = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Unrelated watchlist lead",
            lead_type="unknown",
            status="watching",
            priority=10,
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
            reason="Not part of the research brief quality follow-up lane.",
            evidence_refs=["chunk:pubmed:2"],
        )
    )

    result = service.queue_research_brief_batch(
        ResearchBriefQueueBatchRequest(
            mode="research_leads",
            lead_statuses=["watching"],
            source_keys=["research_brief_quality"],
            limit=10,
        )
    )

    updated_selected = repo.get_research_lead(selected.lead_id)
    updated_ignored = repo.get_research_lead(ignored.lead_id)

    assert result.queued_count == 1
    assert result.lead_count == 1
    assert result.queue_items[0].metadata["batch_queue"]["lead_id"] == str(selected.lead_id)
    assert updated_selected is not None
    assert updated_selected.status == "queued"
    assert updated_ignored is not None
    assert updated_ignored.status == "watching"


def test_research_brief_queue_batch_routes_identifier_leads_to_source_followup(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-identifier-followup.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Angiosarcoma DOI lead",
            lead_type="linked_article",
            status="new",
            priority=35,
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
            identifiers={"doi": "10.1234/HSA.FOLLOWUP"},
            reason="Agent found a DOI that is not yet ingested.",
        )
    )

    result = service.queue_research_brief_batch(
        ResearchBriefQueueBatchRequest(mode="research_leads", limit=5)
    )
    updated_lead = repo.get_research_lead(lead.lead_id)
    followups = repo.list_source_followups(source_key="crossref", limit=10)

    assert result.queued_count == 0
    assert result.research_followup_count == 1
    assert result.skipped[0]["source_followup_source_key"] == "crossref"
    assert result.skipped[0]["source_followup_identifier_type"] == "doi"
    assert updated_lead is not None
    assert updated_lead.status == "followup"
    assert updated_lead.metadata["research_followup_queue"]["source_followup_source_key"] == "crossref"
    assert len(followups) == 1
    assert followups[0].identifier == "10.1234/hsa.followup"
    assert followups[0].metadata["followup_type"] == "research_lead_evidence_enrichment"


def test_research_followup_resolver_promotes_lead_with_ingested_source_followup(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-ingested.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Angiosarcoma DOI follow-up",
            lead_type="linked_article",
            status="followup",
            priority=20,
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
            identifiers={"doi": "10.1234/HSA.RESOLVED"},
        )
    )
    fetch_run_id = uuid4()
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="crossref",
            source_record_id="10.1234/hsa.resolved",
            source_url="https://doi.org/10.1234/hsa.resolved",
            content_hash="crossref-hsa-resolved",
            raw_payload={"title": "Angiosarcoma durable DOI evidence"},
        ),
        fetch_run_id=fetch_run_id,
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Angiosarcoma durable DOI evidence",
            abstract="Durable source evidence for angiosarcoma follow-up resolution.",
            canonical_url="https://doi.org/10.1234/hsa.resolved",
            source_key="crossref",
            dedupe_key="crossref:10.1234/hsa.resolved",
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Angiosarcoma durable evidence for the DOI follow-up lead.",
            content_hash="crossref-hsa-resolved-chunk",
        )
    )
    followup = repo.upsert_source_followup(
        SourceFollowupQueueItem(
            source_key="crossref",
            identifier_type="doi",
            identifier="10.1234/hsa.resolved",
            status="ingested",
            metadata={
                "research_lead_id": str(lead.lead_id),
                "last_ingestion_report": {
                    "research_objects": 1,
                    "document_chunks": 2,
                    "fetch_run_id": str(fetch_run_id),
                },
            },
        )
    )

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_missing_identifiers=False,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.leads_seen == 1
    assert result.promoted_leads == 1
    assert result.lead_results[0].source_followup_ids == [followup.followup_id]
    assert result.lead_results[0].durable_source_keys == ["crossref"]
    assert updated is not None
    assert updated.status == "watching"
    assert updated.source_key == "crossref"
    assert updated.suggested_sources == ["crossref"]
    assert f"source_followup:{followup.followup_id}" in updated.evidence_refs


def test_research_followup_resolver_uses_stored_durable_chunks_before_promotion(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-chunks.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_minimal_source_claim(repo, "pubmed")
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="PubMed source record canine HSA",
            lead_type="linked_article",
            status="followup",
            priority=20,
            source_key="x_topic",
            origin_source_key="x_topic",
            reason="Needs durable stored evidence.",
            topic_tags=["canine", "hemangiosarcoma"],
        )
    )

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_source_keys=["pubmed"],
            ingest_source_followups=False,
            search_missing_identifiers=False,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.promoted_leads == 1
    assert result.lead_results[0].durable_source_keys == ["pubmed"]
    assert any(ref.startswith("chunk:") for ref in result.lead_results[0].evidence_refs)
    inspection = result.lead_results[0].metadata["evidence_inspection"]
    assert inspection["inspected_count"] >= 1
    assert inspection["records"][0]["source_key"] == "pubmed"
    assert "canine hemangiosarcoma" in inspection["records"][0]["text_preview"]
    assert updated is not None
    assert updated.status == "watching"
    assert updated.source_key == "pubmed"
    assert updated.suggested_sources == ["pubmed"]


def test_research_followup_resolver_keeps_unresolved_lead_in_followup(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-manual.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="UF institutional article without primary source",
            lead_type="institutional_article",
            status="followup",
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
        )
    )

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_missing_identifiers=False,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.promoted_leads == 0
    assert result.manual_research_required == 1
    assert "manual_research_required" in result.lead_results[0].actions
    assert result.lead_results[0].metadata["source_followup_ingest"] == {
        "status": "skipped",
        "reason": "no_source_followups_queued_or_linked",
        "source_followup_count": 0,
        "ingestable_count": 0,
        "statuses": [],
    }
    assert updated is not None
    assert updated.status == "followup"
    assert updated.metadata["research_followup_resolver"]["requires_manual_research"] is True


def test_research_followup_resolver_blocks_missing_explicit_lead_ids(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-missing.sqlite3", seed=False)
    service = HSAResearchService(repo)
    missing_lead_id = uuid4()

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(lead_ids=[missing_lead_id])
    )
    run = service.list_agent_runs(agent_name=research_followup_resolver.RESEARCH_FOLLOWUP_RESOLVER_AGENT_NAME, limit=1)[0]

    assert result.blocked is True
    assert result.leads_seen == 0
    assert result.skipped_leads == 1
    assert result.failed_leads == 1
    assert result.unresolved_lead_ids == [missing_lead_id]
    assert result.skip_reasons == [{"lead_id": str(missing_lead_id), "reason": "lead_not_found"}]
    assert "lead_not_found" in result.errors[0]
    assert run.summary["blocked"] is True
    assert run.summary["unresolved_lead_ids"] == 1


def test_research_followup_resolver_blocks_status_filtered_explicit_leads(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-status-filter.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Already promoted lead",
            lead_type="linked_article",
            status="watching",
            source_key="pubmed",
        )
    )

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(lead_ids=[lead.lead_id], statuses=["followup"])
    )

    assert result.blocked is True
    assert result.leads_seen == 0
    assert result.skipped_leads == 1
    assert result.failed_leads == 0
    assert result.unresolved_lead_ids == []
    assert result.skip_reasons[0]["reason"] == "status_not_allowed"
    assert result.skip_reasons[0]["status"] == "watching"
    assert "status_not_allowed" in result.errors[0]


def test_research_followup_resolver_dry_run_reports_planned_identifier_work(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-dry-run.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Dry run DOI follow-up",
            lead_type="linked_article",
            status="followup",
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
            identifiers={"doi": "10.1234/HSA.DRYRUN"},
        )
    )

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            dry_run=True,
            search_missing_identifiers=False,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.dry_run is True
    assert result.source_followups_queued == 0
    assert result.source_followups_ingested == 0
    assert repo.list_source_followups(limit=None) == []
    assert result.lead_results[0].metadata["planned_source_followups"][0]["action"] == "would_queue_source_followup"
    assert result.lead_results[0].metadata["planned_source_followups"][0]["source_key"] == "crossref"
    assert result.lead_results[0].metadata["planned_action"] == "would_mark_manual_research_required"
    assert updated is not None
    assert updated.status == "followup"
    assert "research_followup_resolver" not in updated.metadata


def test_research_followup_resolver_dry_run_reports_planned_promotion(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-dry-run-promotion.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_minimal_source_claim(repo, "pubmed")
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="PubMed source record canine HSA",
            lead_type="linked_article",
            status="followup",
            source_key="x_topic",
            origin_source_key="x_topic",
            topic_tags=["canine", "hemangiosarcoma"],
        )
    )

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_source_keys=["pubmed"],
            ingest_source_followups=False,
            search_missing_identifiers=False,
            dry_run=True,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.promoted_leads == 0
    assert result.kept_in_followup == 1
    assert result.lead_results[0].metadata["planned_action"] == "would_promote_to_watching"
    assert result.lead_results[0].durable_source_keys == ["pubmed"]
    assert updated is not None
    assert updated.status == "followup"


def test_research_followup_resolver_expands_safety_gap_query():
    lead = ResearchLeadRecord(
        title="Safety signal: Safety/tolerability profile of sorafenib in dogs at proposed doses (DLT, MTD data)",
        lead_type="unknown",
        status="watching",
        source_key="pubmed",
    )

    query = research_followup_resolver._lead_search_query(lead, max_terms=12)

    assert "sorafenib" in query
    assert "dlt" in query
    assert "mtd" in query
    assert "canine" in query
    assert "veterinary" in query
    assert "dose limiting" in query


def test_research_followup_resolver_preserves_hyphenated_therapy_query_terms():
    lead = ResearchLeadRecord(
        title="Safety signal: Anti-PD-1 monotherapy efficacy and safety data in canine HSA",
        summary="Need canine CA-4F12-E6 anti-PD-1 safety and tolerability evidence.",
        lead_type="unknown",
        status="watching",
        source_key="pubmed",
        topic_tags=["canine", "hemangiosarcoma"],
    )

    query = research_followup_resolver._lead_search_query(lead, max_terms=12)

    assert "ca-4f12-e6" in query
    assert "anti-pd-1" in query
    assert "pd-1" in query
    assert "canine" in query
    assert "hemangiosarcoma" in query
    assert " anti " not in f" {query} "


def test_research_followup_resolver_force_live_search_refreshes_existing_evidence(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-force-live.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_minimal_source_claim(repo, "pubmed")
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Sorafenib canine toxicity follow-up",
            lead_type="linked_article",
            status="followup",
            source_key="x_topic",
            origin_source_key="x_topic",
            topic_tags=["sorafenib", "toxicity"],
        )
    )
    calls = []

    def fake_search(repository, lead_record, request):
        calls.append((lead_record.lead_id, request.force_live_search, request.search_missing_identifiers))
        _seed_minimal_source_claim(repository, "clinicaltrials_gov")
        return {
            "query_text": "sorafenib canine toxicity",
            "source_keys": request.search_source_keys,
            "limit_per_source": request.search_limit_per_source,
            "reports": [{"source_key": "clinicaltrials_gov", "document_chunks": 1}],
            "errors": [],
        }

    monkeypatch.setattr(research_followup_resolver, "_search_durable_sources", fake_search)

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_source_keys=["pubmed", "clinicaltrials_gov"],
            search_missing_identifiers=False,
            force_live_search=True,
            promote_ready_leads=False,
            min_evidence_chunks=1,
        )
    )
    lead_result = result.lead_results[0]

    assert calls == [(lead.lead_id, True, False)]
    assert result.force_live_search is True
    assert result.durable_source_searches == 1
    assert result.evidence_inspections == 1
    assert "searched_durable_sources" in lead_result.actions
    assert lead_result.metadata["durable_source_search"]["force_live_search"] is True
    assert lead_result.metadata["durable_source_search"]["evidence_refs_before_search"] >= 1
    assert set(lead_result.durable_source_keys) == {"pubmed", "clinicaltrials_gov"}
    inspected_sources = {
        record["source_key"]
        for record in lead_result.metadata["evidence_inspection"]["records"]
        if record.get("source_key")
    }
    assert {"pubmed", "clinicaltrials_gov"} <= inspected_sources


def test_research_followup_resolver_blocks_promotion_for_weak_evidence_fit(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-fit-gate.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Safety signal: Safety/tolerability profile of sorafenib in dogs at proposed doses (DLT, MTD data)",
            lead_type="unknown",
            status="followup",
            source_key="pubmed",
            origin_source_key="agent_evaluator",
            topic_tags=["sorafenib", "canine", "safety", "mtd", "dlt"],
        )
    )

    def fake_search(repository, lead_record, request):
        fetch_run_id = uuid4()
        raw_record_id = repository.upsert_raw_record(
            RawSourceRecord(
                source_key="pubmed",
                source_record_id="41900948",
                source_url="https://pubmed.ncbi.nlm.nih.gov/41900948/",
                content_hash="pubmed-41900948",
                raw_payload={"title": "Comparative oncology framework"},
            ),
            fetch_run_id=fetch_run_id,
        )
        object_id = repository.upsert_research_object(
            ResearchObject(
                object_type="publication",
                title="Comparative Cancer Genetics and Veterinary Therapeutics in Dogs and Cats",
                abstract=(
                    "Dogs and cats develop naturally occurring tumors that resemble human malignancies, "
                    "supporting a species-aware comparative oncology framework."
                ),
                canonical_url="https://pubmed.ncbi.nlm.nih.gov/41900948/",
                source_key="pubmed",
                identifiers={"pmid": "41900948"},
                dedupe_key="pubmed:41900948",
            ),
            raw_record_id,
        )
        repository.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=0,
                section_label="title_abstract",
                text_content=(
                    "Comparative oncology review covering dogs, cats, and cancer genetics across species-aware "
                    "therapeutic frameworks."
                ),
                content_hash="pubmed-41900948-chunk",
            )
        )
        return {
            "query_text": "sorafenib canine dlt mtd safety",
            "source_keys": ["pubmed"],
            "limit_per_source": request.search_limit_per_source,
            "reports": [
                {
                    "source_key": "pubmed",
                    "fetch_run_id": str(fetch_run_id),
                    "raw_records": 1,
                    "research_objects": 1,
                    "document_chunks": 1,
                }
            ],
            "errors": [],
        }

    monkeypatch.setattr(research_followup_resolver, "_search_durable_sources", fake_search)

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_source_keys=["pubmed"],
            search_missing_identifiers=False,
            force_live_search=True,
            promote_ready_leads=True,
            min_evidence_chunks=1,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)
    lead_result = result.lead_results[0]

    assert result.promoted_leads == 0
    assert result.kept_in_followup == 1
    assert lead_result.metadata["evidence_fit"]["fit"] == "weak"
    assert "sorafenib" in lead_result.metadata["evidence_fit"]["missing_terms"]
    assert "promoted_to_watching" not in lead_result.actions
    assert updated is not None
    assert updated.status == "followup"
    assert updated.metadata["research_followup_resolver"]["evidence_fit"]["fit"] == "weak"


def test_research_followup_resolver_ingests_only_linked_followup(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-ingest.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Angiosarcoma DOI follow-up",
            lead_type="linked_article",
            status="followup",
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
            identifiers={"doi": "10.1234/HSA.INGEST"},
        )
    )
    unrelated = repo.upsert_source_followup(
        SourceFollowupQueueItem(
            source_key="crossref",
            identifier_type="doi",
            identifier="10.9999/unrelated",
            status="queued",
        )
    )

    def fake_ingest(repository, request):
        assert len(request.followup_ids) == 1
        assert unrelated.followup_id not in request.followup_ids
        item = repository.get_source_followup(request.followup_ids[0])
        assert item is not None
        fetch_run_id = uuid4()
        raw_record_id = repository.upsert_raw_record(
            RawSourceRecord(
                source_key="crossref",
                source_record_id="10.1234/hsa.ingest",
                source_url="https://doi.org/10.1234/hsa.ingest",
                content_hash="crossref-hsa-ingest",
                raw_payload={"title": "Angiosarcoma linked follow-up evidence"},
            ),
            fetch_run_id=fetch_run_id,
        )
        object_id = repository.upsert_research_object(
            ResearchObject(
                object_type="publication",
                title="Angiosarcoma linked follow-up evidence",
                abstract="Durable angiosarcoma evidence for the linked DOI follow-up lead.",
                canonical_url="https://doi.org/10.1234/hsa.ingest",
                source_key="crossref",
                dedupe_key="crossref:10.1234/hsa.ingest",
            ),
            raw_record_id,
        )
        repository.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=0,
                section_label="abstract",
                text_content="Angiosarcoma durable evidence for the linked follow-up lead.",
                content_hash="crossref-hsa-ingest-chunk",
            )
        )
        repository.update_source_followup(
            item.followup_id,
            status="ingested",
            attempts=item.attempts + 1,
            metadata={
                "research_lead_id": str(lead.lead_id),
                "last_ingestion_report": {
                    "research_objects": 1,
                    "document_chunks": 1,
                    "fetch_run_id": str(fetch_run_id),
                },
            },
        )

    class FakeIngestResult:
        ingested = 1

        def model_dump(self, mode):
            return {"ingested": 1, "items": []}

    def fake_ingest_result(repository, request):
        fake_ingest(repository, request)
        return FakeIngestResult()

    monkeypatch.setattr(research_followup_resolver, "ingest_source_followups", fake_ingest_result)

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_missing_identifiers=False,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.source_followups_queued == 1
    assert result.source_followups_ingested == 1
    assert result.promoted_leads == 1
    assert repo.get_source_followup(unrelated.followup_id).status == "queued"
    assert updated is not None
    assert updated.status == "watching"


def test_command_center_report_summarizes_operational_state(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center.sqlite3", seed=False)
    service = HSAResearchService(repo)
    queued = service.queue_research_brief(
        ResearchBriefQueueRequest(
            topic="VEGF synthesis queue item",
            source_key="pubmed",
            priority=10,
        )
    )
    repo.update_research_brief_queue_item(queued.queue_item_id, status="failed", last_error="model timeout")
    repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Angiosarcoma trial lead",
            lead_type="linked_article",
            status="new",
            priority=25,
            suggested_sources=["clinicaltrials_gov"],
        )
    )
    repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evidence-light linked article",
            lead_type="institutional_article",
            status="followup",
            priority=40,
            source_key="x_linked_article",
        )
    )
    repo.create_agent_run(
        AgentRunRecord(
            agent_name="research_synthesis_editor_agent",
            status=RunStatus.FAILED,
            errors=["failed synthesis smoke"],
        )
    )
    source_health_report = {
        "failed_sources": ["chembl"],
        "triage_sources": ["pubmed"],
        "watch_sources": ["openalex"],
        "embedding_missing_sources": ["pubmed"],
        "full_text_blocking_sources": ["pmc_oa"],
        "sources": [],
        "summary": {},
    }

    report = service.build_command_center_report(
        CommandCenterRequest(
            source_health_report=source_health_report,
            queue_limit=10,
            lead_limit=10,
            agent_run_limit=10,
        )
    )
    recommendation_areas = {item.area for item in report.recommendations}

    assert report.summary["brief_queue_failed"] == 1
    assert report.summary["research_leads_actionable"] == 1
    assert report.summary["research_leads_followup"] == 1
    assert report.summary["recent_agent_failures"] == 1
    assert report.summary["source_health_failed"] == 1
    assert report.summary["blocking_recommendations"] >= 2
    assert report.research_brief_queue["status_counts"]["failed"] == 1
    assert report.research_leads["status_counts"]["new"] == 1
    assert report.research_leads["status_counts"]["followup"] == 1
    assert report.source_health == source_health_report
    assert recommendation_areas >= {"brief_queue", "research_leads", "source_health", "embeddings", "full_text", "agents"}


def test_command_center_web_validation_queue_actions(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web.sqlite3", seed=False)
    service = HSAResearchService(repo)
    queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            source_key="pubmed",
            topic="VEGF validation plan",
            task_type="expert_review",
            title="Expert review: VEGF translational signal",
            objective="Review whether the hypothesis has enough evidence for validation.",
            rationale="Human approval is required before dispatch.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review whether the hypothesis has enough evidence for validation.",
            ),
            quality_gates=["human_approval_required"],
            priority=25,
        )
    )

    listed = command_center_web.list_validation_queue_payload(service, {"status": ["needs_approval"]})
    approved = command_center_web.approve_validation_request_payload(
        service,
        str(queue_item.queue_item_id),
        {"approved_by": "operator"},
    )
    dispatched = command_center_web.dispatch_validation_request_payload(
        service,
        str(queue_item.queue_item_id),
        {"model_profile": "deterministic_only"},
    )

    assert listed["visible"] == 1
    assert listed["items"][0]["queue_item_id"] == str(queue_item.queue_item_id)
    assert approved["item"]["status"] == "approved"
    assert approved["item"]["approved_by"] == "operator"
    assert dispatched["item"]["status"] == "completed"
    assert dispatched["item"]["dispatch_blockers"] == []
    assert dispatched["item"]["last_run_id"]
    assert dispatched["item"]["metadata"]["validation_agent_result"]["decision"] in {"promote", "hold", "demote"}


def test_command_center_web_dispatch_reports_blockers(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-blocked.sqlite3", seed=False)
    service = HSAResearchService(repo)
    queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            source_key="pubmed",
            topic="Structure validation plan",
            task_type="protein_structure",
            title="Run structure validation",
            objective="Run structure validation when target context is complete.",
            rationale="The generated plan lacks a target identity.",
            validation_request=ValidationRequest(
                validation_type="boltz",
                objective="Run structure validation when target context is complete.",
                assay_context=ValidationAssayContext(
                    disease_context="canine hemangiosarcoma and human angiosarcoma",
                    species=["canine", "human"],
                ),
            ),
            quality_gates=["target_identity_required"],
            priority=30,
        )
    )
    command_center_web.approve_validation_request_payload(
        service,
        str(queue_item.queue_item_id),
        {"approved_by": "operator"},
    )

    dispatched = command_center_web.dispatch_validation_request_payload(
        service,
        str(queue_item.queue_item_id),
        {"model_profile": "deterministic_only"},
    )

    assert dispatched["item"]["status"] == "blocked"
    assert "target_name_required" in dispatched["item"]["dispatch_blockers"]
    assert "model_system_required" in dispatched["item"]["dispatch_blockers"]


def test_command_center_web_dispatch_preflight_requires_openrouter_key(monkeypatch, tmp_path):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-openrouter-preflight.sqlite3", seed=False)
    service = HSAResearchService(repo)
    queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            source_key="pubmed",
            topic="VEGF validation plan",
            task_type="expert_review",
            title="Expert review: VEGF translational signal",
            objective="Review whether the hypothesis has enough evidence for validation.",
            rationale="Human approval is required before dispatch.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review whether the hypothesis has enough evidence for validation.",
            ),
            priority=25,
        )
    )
    command_center_web.approve_validation_request_payload(
        service,
        str(queue_item.queue_item_id),
        {"approved_by": "operator"},
    )

    readiness = command_center_web.runtime_payload()["validation_dispatch"]
    with pytest.raises(RuntimeError, match="OPENROUTER_API_KEY"):
        command_center_web.dispatch_validation_request_payload(service, str(queue_item.queue_item_id))
    stored = service.get_validation_request_queue_item(queue_item.queue_item_id)

    assert readiness["dispatch_ready"] is False
    assert stored is not None
    assert stored.status == "approved"
    assert stored.attempts == 0
    assert stored.last_error is None


def test_command_center_web_validation_autopilot_payloads(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-autopilot.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            source_key="pubmed",
            topic="Autopilot command center preview",
            task_type="expert_review",
            title="Expert review: command center autopilot",
            objective="Review the evidence packet.",
            rationale="The command center should preview conservative selection.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review the evidence packet.",
            ),
            created_at=datetime.now(UTC) - timedelta(hours=2),
        )
    )

    preview = command_center_web.validation_autopilot_preview_payload(
        service,
        {
            "model_profile": ["deterministic_only"],
            "minimum_queue_age_hours": ["0"],
            "max_per_run": ["2"],
        },
    )
    dry_run = command_center_web.run_validation_autopilot_payload(
        service,
        {
            "dry_run": True,
            "model_profile": "deterministic_only",
            "minimum_queue_age_hours": 0,
        },
    )
    stored = service.get_validation_request_queue_item(item.queue_item_id)

    assert preview["selected_count"] == 1
    assert preview["selected"][0]["queue_item_id"] == str(item.queue_item_id)
    assert dry_run["dry_run"] is True
    assert dry_run["agent_run_id"]
    assert stored is not None
    assert stored.status == "needs_approval"


def test_command_center_web_lists_idea_records(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-ideas.sqlite3", seed=False)
    service = HSAResearchService(repo)
    idea = TherapyIdea(
        title="KDR mutation-gated VEGFR inhibition",
        hypothesis="KDR-altered canine HSA may reveal a translational VEGFR inhibition lane.",
        rationale="Comparative oncology evidence supports testing the conserved angiogenic pathway.",
        candidate_therapies=["pazopanib"],
        targets=["KDR"],
        biomarkers=["VEGFR2"],
        evidence_refs=["C1", "C2"],
        evidence_strength="medium",
        risks=["PK bridge is incomplete."],
        next_experiments=["Run canine/human KDR sequence conservation review."],
        priority_score=0.82,
    )
    committee_result = TherapyCommitteeResult(
        agent_run_id=uuid4(),
        topic="KDR therapy ideas",
        disease_scope="canine hemangiosarcoma and human angiosarcoma",
        ranked_ideas=[idea],
        decision_summary="Prioritize KDR validation.",
    )
    repo.create_agent_run(
        AgentRunRecord(
            agent_run_id=committee_result.agent_run_id,
            agent_name="therapy_committee_chair_agent",
            model_profile="therapy_committee",
            status=RunStatus.COMPLETED,
            output_payload=committee_result.model_dump(mode="json"),
        )
    )
    repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            source_key="pubmed",
            topic="KDR validation",
            task_type="expert_review",
            title="Expert review: KDR mutation-gated VEGFR inhibition",
            objective="Review the therapy idea.",
            rationale="Human review required.",
            validation_request=ValidationRequest(validation_type="expert_review", objective="Review the therapy idea."),
            metadata={"idea_id": str(idea.idea_id), "idea_title": idea.title},
        )
    )
    plan = repo.upsert_validation_plan(
        ValidationPlanRecord(
            brief_id=uuid4(),
            topic="VEGFR PK bridge",
            source_key="pubmed",
            result_payload={
                "hypothesis_drafts": [
                    {
                        "hypothesis_id": str(uuid4()),
                        "title": "PK bridge hypothesis",
                        "hypothesis": "Canine and human PK gaps should gate pazopanib translation.",
                        "rationale": "Validation agents need explicit exposure context.",
                        "status": "draft",
                        "confidence": 0.64,
                    }
                ]
            },
        )
    )

    payload = command_center_web.list_ideas_payload(service)
    therapy_payload = command_center_web.list_ideas_payload(service, {"kind": ["therapy_idea"]})
    query_payload = command_center_web.list_ideas_payload(service, {"query": ["pazopanib"]})

    assert payload["total"] == 2
    assert payload["kind_counts"] == {"therapy_idea": 1, "validation_hypothesis": 1}
    assert payload["status_counts"] == {"draft": 1, "needs_approval": 1}
    assert therapy_payload["visible"] == 1
    assert therapy_payload["items"][0]["idea_id"] == str(idea.idea_id)
    assert therapy_payload["items"][0]["validation_status_counts"] == {"needs_approval": 1}
    assert query_payload["visible"] == 2
    assert any(item.get("plan_id") == str(plan.plan_id) for item in payload["items"])


def test_command_center_web_lists_research_briefs_with_quality_state(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-briefs.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="KDR translational synthesis",
            disease_scope="canine hemangiosarcoma and human angiosarcoma",
            source_key="pubmed",
            status="completed",
            final_brief="KDR/VEGFR2 translation should move into validation planning [C1].",
            citation_count=1,
            finding_count=1,
            hypothesis_count=1,
            result_payload={
                "citations": [
                    {
                        "citation_id": "C1",
                        "chunk_id": str(uuid4()),
                        "research_object_id": str(uuid4()),
                        "source_key": "pubmed",
                        "title": "VEGFR2 in angiosarcoma",
                        "source_url": "https://pubmed.ncbi.nlm.nih.gov/example",
                        "quote": "VEGFR2 signal.",
                    }
                ],
                "ranked_hypotheses": [
                    {
                        "claim": "KDR-altered tumors should be reviewed for VEGFR2 inhibition.",
                        "stance": "supports",
                        "citations": ["C1"],
                        "evidence_strength": "medium",
                        "reasoning": "Citation-backed rationale.",
                    }
                ],
                "evidence_limitations": ["Canine clinical-response bridge remains incomplete."],
            },
        )
    )
    repo.upsert_research_brief_evaluation(
        ResearchBriefEvaluationRecord(
            brief_id=brief.brief_id,
            topic=brief.topic,
            source_key="pubmed",
            overall_score=0.82,
            passes_quality_bar=True,
            readiness="ready_for_hypothesis_review",
        )
    )

    payload = command_center_web.list_research_briefs_payload(service)
    ready_payload = command_center_web.list_research_briefs_payload(
        service,
        {"quality_status": ["ready_for_validation"], "query": ["KDR"]},
    )

    assert payload["total"] == 1
    assert payload["ready_count"] == 1
    assert ready_payload["visible"] == 1
    assert ready_payload["items"][0]["brief_id"] == str(brief.brief_id)
    assert ready_payload["items"][0]["quality_status"] == "ready_for_validation"
    assert ready_payload["items"][0]["final_brief"].startswith("KDR/VEGFR2")
    assert ready_payload["items"][0]["citation_preview"][0]["citation_id"] == "C1"
    assert ready_payload["items"][0]["hypothesis_preview"][0]["evidence_strength"] == "medium"


def test_command_center_web_lists_agent_runs_with_payloads(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-agent-runs.sqlite3", seed=False)
    service = HSAResearchService(repo)
    started_at = datetime.now(UTC) - timedelta(seconds=12)
    therapy_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            model_profile="openrouter_required",
            status=RunStatus.RUNNING,
            source_key="pubmed",
            started_at=started_at,
            input_payload={"topic": "KDR angiosarcoma therapy", "committee": "translational"},
            metadata={"estimated_cost_usd": 0.04},
        )
    )
    completed = repo.finish_agent_run(
        therapy_run.agent_run_id,
        status="completed",
        output_payload={"ranked_ideas": [{"title": "KDR/VEGFR2 validation lane"}]},
        summary={"topic": "KDR angiosarcoma therapy", "ideas": 1},
        errors=[],
    )
    assert completed is not None
    failed_seed = repo.create_agent_run(
        AgentRunRecord(
            agent_name="validation_planning_agent",
            model_profile="openrouter_required",
            status=RunStatus.RUNNING,
            source_key="x_topic_monitor",
            input_payload={"topic": "KIT mutation function"},
        )
    )
    failed = repo.finish_agent_run(
        failed_seed.agent_run_id,
        status="failed",
        output_payload={},
        summary={"topic": "KIT mutation function"},
        errors=["Missing mutation-function evidence."],
    )
    assert failed is not None
    review_payload = command_center_web.create_agent_run_review_payload(
        service,
        str(completed.agent_run_id),
        {
            "verdict": "useful",
            "feedback": "Good committee output.",
            "reviewer": "operator",
            "tags": ["KDR", "committee"],
        },
    )

    payload = command_center_web.list_agent_runs_payload(
        service,
        {"agent_name": ["therapy_committee_chair_agent"], "query": ["VEGFR2"]},
    )
    detail = command_center_web.get_agent_run_payload(service, str(completed.agent_run_id))
    failed_payload = command_center_web.list_agent_runs_payload(service, {"status": ["failed"]})

    assert payload["total"] == 1
    assert payload["visible"] == 1
    assert payload["status_counts"] == {"completed": 1}
    assert payload["agent_counts"] == {"therapy_committee_chair_agent": 1}
    assert payload["items"][0]["input_payload"]["topic"] == "KDR angiosarcoma therapy"
    assert payload["items"][0]["output_payload"]["ranked_ideas"][0]["title"] == "KDR/VEGFR2 validation lane"
    assert payload["items"][0]["duration_seconds"] is not None
    assert payload["items"][0]["review_count"] == 1
    assert payload["items"][0]["latest_review"]["verdict"] == "useful"
    assert payload["items"][0]["latest_review"]["reviewer_type"] == "operator"
    assert payload["items"][0]["latest_review"]["tags"] == ["kdr", "committee"]
    assert review_payload["item"]["feedback"] == "Good committee output."
    assert detail["item"]["agent_run_id"] == str(completed.agent_run_id)
    assert detail["item"]["summary"]["ideas"] == 1
    assert detail["item"]["latest_reviews"][0]["reviewer"] == "operator"
    assert failed_payload["visible"] == 1
    assert failed_payload["items"][0]["errors"] == ["Missing mutation-function evidence."]
    with pytest.raises(ValueError):
        command_center_web.get_agent_run_payload(service, "bad-id")
    with pytest.raises(LookupError):
        command_center_web.get_agent_run_payload(service, str(uuid4()))
    with pytest.raises(ValueError):
        command_center_web.create_agent_run_review_payload(service, str(completed.agent_run_id), {"verdict": "wrong"})
    with pytest.raises(LookupError):
        command_center_web.create_agent_run_review_payload(service, str(uuid4()), {"verdict": "bad"})


def test_command_center_web_agent_performance_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-agent-performance.sqlite3", seed=False)
    service = HSAResearchService(repo)
    run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            model_profile="openrouter_required",
            status=RunStatus.COMPLETED,
            output_payload={"ranked_ideas": [{"title": "KDR validation"}]},
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run.agent_run_id,
            reviewer="operator",
            verdict="useful",
        )
    )

    def fake_openrouter(model_name, review_payload):
        assert review_payload["specialist"] == "synthesis"
        return {
            "text": json.dumps(
                {
                    "verdict": "useful",
                    "confidence": 0.9,
                    "rationale": "Clear committee output.",
                    "strengths": ["Specific validation idea."],
                    "failure_modes": [],
                    "recommended_followup_actions": ["keep"],
                    "rubric_scores": {"actionability": 0.9},
                }
            ),
            "metadata": {"provider": "openrouter", "model_name": model_name},
        }

    monkeypatch.setattr(agent_performance, "_openrouter_review_model", fake_openrouter)

    payload = command_center_web.agent_performance_payload(service)
    evaluation = command_center_web.run_agent_performance_evaluation_payload(
        service,
        {"limit": 1, "operator": "operator"},
    )
    updated = command_center_web.agent_performance_payload(service)

    assert payload["agent_run_count"] == 1
    assert payload["reviewed_run_count"] == 1
    assert payload["rows"][0]["performance_score"] == 100
    assert evaluation["review_created_count"] == 1
    assert updated["evaluator_reviewed_count"] == 1


def test_command_center_web_action_items_and_research_lead_status_updates(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-actions.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Potential VEGF follow-up article",
            lead_type="linked_article",
            status="new",
            priority=20,
            source_key="x_linked_article",
            reason="Agent flagged a durable article link for follow-up.",
            topic_tags=["vegf", "therapy"],
            suggested_sources=["pubmed"],
        )
    )

    action_items = command_center_web.build_action_items_payload(service, {"limit": ["10"]})
    listed = command_center_web.list_research_leads_payload(service, {"status": ["new,watching,followup"]})
    promoted = command_center_web.update_research_lead_status_payload(
        service,
        str(lead.lead_id),
        {"status": "watching", "operator": "operator"},
    )
    demoted = command_center_web.update_research_lead_status_payload(
        service,
        str(lead.lead_id),
        {"status": "dismissed", "operator": "operator"},
    )

    assert any(item["kind"] == "research_lead" for item in action_items["items"])
    assert listed["visible"] == 1
    assert listed["items"][0]["lead_id"] == str(lead.lead_id)
    assert promoted["item"]["status"] == "watching"
    assert promoted["item"]["metadata"]["command_center"]["operator"] == "operator"
    assert demoted["item"]["status"] == "dismissed"


def test_command_center_web_action_items_surface_latest_evaluator_findings(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-agent-findings.sqlite3", seed=False)
    service = HSAResearchService(repo)
    resolved_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="research_followup_resolver_agent",
            status="completed",
            summary={"blocked": True, "unresolved_lead_ids": 1},
        )
    )
    stale_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="claim_curator_agent",
            status="completed",
            summary={"claims_reviewed": 5},
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=resolved_run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="bad",
            feedback="Resolver completed with no usable lead work.",
            followup_actions=["rerun dry-run with explicit lead IDs", "inspect skip reasons"],
            metadata={"confidence": 0.82},
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=stale_run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="needs_followup",
            feedback="Older evaluator concern.",
            created_at=datetime.now(UTC) - timedelta(minutes=5),
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=stale_run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="useful",
            feedback="Latest evaluator cleared this run.",
            created_at=datetime.now(UTC),
        )
    )

    action_items = command_center_web.build_action_items_payload(service, {"limit": ["10"]})
    evaluator_items = [item for item in action_items["items"] if item["kind"] == "agent_evaluator_finding"]

    assert len(evaluator_items) == 1
    assert evaluator_items[0]["severity"] == "blocking"
    assert evaluator_items[0]["title"] == "research_followup_resolver_agent: Bad"
    assert "Inspect Skip Reasons" in evaluator_items[0]["description"]
    assert evaluator_items[0]["actions"] == ["escalate_agent_finding"]
    assert evaluator_items[0]["metadata"]["agent_run_id"] == str(resolved_run.agent_run_id)
    assert evaluator_items[0]["metadata"]["confidence"] == 0.82


def test_command_center_web_escalates_agent_finding_payload(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-agent-finding-escalation.sqlite3", seed=False)
    service = HSAResearchService(repo)
    run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="research_followup_resolver_agent",
            status="completed",
            output_payload={
                "lead_results": [
                    {
                        "title": "Sorafenib canine DLT gap",
                        "evidence_refs": ["chunk:gap"],
                    }
                ]
            },
        )
    )
    review = repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="bad",
            feedback="Need a canine sorafenib DLT/MTD follow-up search.",
            followup_actions=["rerun_search_with_refined_terms:_'sorafenib_canine_maximum_tolerated_dose'"],
        )
    )

    payload = command_center_web.escalate_agent_findings_payload(
        service,
        {"review_id": str(review.review_id), "operator": "operator"},
    )

    assert payload["research_leads_created"] == 1
    assert payload["source_queries_created"] >= 3
    assert payload["research_leads"][0]["metadata"]["command_center"]["operator"] == "operator"
    assert repo.list_research_leads(status="followup", limit=10)
    assert repo.list_source_queries(active_only=True)
    action_items = command_center_web.build_action_items_payload(service, {"limit": ["10"]})
    assert all(item["item_id"] != f"agent-review:{review.review_id}" for item in action_items["items"])
    lead_items = [item for item in action_items["items"] if item["kind"] == "research_lead"]
    assert lead_items
    assert "run_followup_search" in lead_items[0]["actions"]
    assert "reevaluate_followup" in lead_items[0]["actions"]


def test_command_center_web_runs_research_followup_loop_payload(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-followup-loop.sqlite3", seed=False)
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine DLT",
            status="followup",
            priority=5,
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            metadata={"created_by": "agent_finding_escalation_agent"},
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="agent_eval_sorafenib",
            query_text="sorafenib canine maximum tolerated dose",
            query_params={
                "followup_lane": "agent_evaluator_followup",
                "origin_review_id": str(origin_review_id),
                "origin_agent_run_id": str(origin_agent_run_id),
            },
            track="validation_gap",
        )
    )

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            research_object = ResearchObject(
                object_type="publication",
                title="Sorafenib dose-limiting toxicity in dogs",
                abstract="Canine sorafenib maximum tolerated dose and dose-limiting toxicity data.",
                source_key=query.source_key,
                dedupe_key=f"{query.source_key}:{query.query_name}:strong",
            )
            object_id = self.repository.upsert_research_object(research_object)
            for index in range(2):
                self.repository.upsert_document_chunk(
                    DocumentChunk(
                        research_object_id=object_id,
                        chunk_index=index,
                        section_label="abstract",
                        text_content=(
                            "Sorafenib was evaluated in canine patients with safety, tolerability, "
                            "maximum tolerated dose, and dose-limiting toxicity endpoints."
                        ),
                        content_hash=f"{query.source_key}:{query.query_name}:strong:{index}",
                    )
                )
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                raw_records=1,
                research_objects=1,
                document_chunks=1,
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    payload = command_center_web.run_research_followup_loop_payload(
        service,
        str(lead.lead_id),
        {"ingest": True, "resolve": False, "evaluate": False, "operator": "operator"},
    )

    assert payload["lead_status_before"] == "followup"
    assert payload["lead_status_after"] == "watching"
    assert payload["query_count"] == 1
    assert payload["document_chunks"] == 1
    assert payload["evidence_fit"]["fit"] == "strong"


def test_therapy_committee_contract_rejects_invalid_priority_score():
    with pytest.raises(ValidationError):
        TherapyIdea(
            title="Invalid idea",
            hypothesis="Priority score is outside the allowed range.",
            rationale="The contract should reject malformed committee output.",
            evidence_refs=["C1"],
            priority_score=1.5,
        )


def test_therapy_committee_load_json_object_repairs_model_formatting():
    payload = therapy_committee._load_json_object(
        """
        Model output:
        ```json
        {
          "summary": "Parsed after local cleanup"
          "ideas": [],
          "evidence_limitations": [],
          "errors": [],
        }
        ```
        """
    )

    assert payload["summary"] == "Parsed after local cleanup"
    assert payload["ideas"] == []


def test_therapy_committee_openrouter_perspective_repairs_invalid_model_json(monkeypatch):
    review_calls = []
    repair_calls = []

    def fake_review_model(model_name, review_payload):
        review_calls.append((model_name, review_payload))
        return {
            "text": '{"summary": "truncated", "ideas": [',
            "metadata": {"model_name": model_name, "request_id": "review-1"},
        }

    def fake_repair_model(model_name, malformed_text, *, parse_error, original_metadata=None):
        repair_calls.append((model_name, malformed_text, parse_error, original_metadata))
        return {
            "text": json.dumps(
                {
                    "summary": "Repaired committee JSON",
                    "ideas": [
                        {
                            "title": "KDR-gated kinase validation",
                            "hypothesis": "KDR-positive HSA warrants a cited kinase validation pass.",
                            "rationale": "The cited evidence links vascular signaling with the disease context.",
                            "candidate_therapies": ["toceranib"],
                            "targets": ["KDR"],
                            "biomarkers": ["KDR"],
                            "mechanism": "VEGFR signaling blockade may reduce vascular tumor signaling.",
                            "evidence_refs": ["C1"],
                            "evidence_strength": "low",
                            "translational_path": "Start with ex vivo or cell-model validation.",
                            "risks": ["Evidence remains indirect."],
                            "next_experiments": ["Run a KDR/phospho-VEGFR readout."],
                            "priority_score": 0.71,
                        }
                    ],
                    "evidence_limitations": ["Repair preserved model content after syntax failure."],
                    "errors": [],
                }
            ),
            "metadata": {
                "model_name": model_name,
                "request_id": "repair-1",
                "json_repair_attempted": True,
            },
        }

    monkeypatch.setattr(therapy_committee, "_openrouter_review_model", fake_review_model)
    monkeypatch.setattr(therapy_committee, "_openrouter_repair_json_model", fake_repair_model)

    citation = ResearchBriefCitation(
        citation_id="C1",
        chunk_id=uuid4(),
        research_object_id=uuid4(),
        source_key="pubmed",
        title="KDR therapy signal",
        quote="Canine hemangiosarcoma evidence discusses KDR and vascular tumor signaling.",
    )
    report = therapy_committee._run_openrouter_perspective(
        TherapyCommitteeRequest(
            topic="KDR therapy ideas for canine hemangiosarcoma",
            review_mode="openrouter_required",
            review_models=["test/model"],
            max_ideas_per_perspective=1,
        ),
        "target_biology",
        {"citations": [citation], "claims": [], "research_leads": [], "search_queries": {}, "errors": []},
    )

    assert len(review_calls) == 1
    assert len(repair_calls) == 1
    assert repair_calls[0][3]["request_id"] == "review-1"
    assert report.summary == "Repaired committee JSON"
    assert report.ideas[0].title == "KDR-gated kinase validation"
    assert report.evidence["model_review"]["json_repair_attempted"] is True


def test_therapy_committee_runs_cited_idea_layer(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "therapy-committee.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:therapy",
            content_hash="therapy-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/therapy/",
            raw_payload={"pmid": "therapy"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="KDR VEGFA and mTOR therapy in canine hemangiosarcoma",
            abstract=(
                "Canine hemangiosarcoma and human angiosarcoma share vascular biology. "
                "KDR, VEGFA, MTOR, CD31, propranolol, sirolimus, and paclitaxel are discussed."
            ),
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/therapy/",
            dedupe_key="pmid:therapy",
            identifiers={"pmid": "therapy"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma translational therapy evidence discusses KDR, VEGFA, "
                "MTOR, CD31, propranolol, sirolimus, paclitaxel, toxicity, target selection, "
                "biomarker readouts, and human angiosarcoma analog evidence."
            ),
            content_hash="therapy-chunk",
        )
    )

    result = HSAResearchService(repo).run_therapy_committee(
        TherapyCommitteeRequest(
            topic="KDR VEGFA mTOR therapy ideas for canine hemangiosarcoma",
            max_chunks_per_perspective=3,
            max_claims=0,
            review_mode="deterministic_only",
        )
    )

    assert isinstance(result, TherapyCommitteeResult)
    assert len(result.reports) == 4
    assert result.ranked_ideas
    assert result.ranked_ideas[0].evidence_refs
    assert result.evidence["citation_count"] >= 1
    agent_runs = repo.list_agent_runs(agent_name="therapy_committee_chair_agent", limit=10)
    assert agent_runs
    assert agent_runs[0].status == RunStatus.COMPLETED


def test_therapy_committee_validation_queue_promotes_ranked_ideas(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "therapy-committee-validation.sqlite3", seed=False)
    idea = TherapyIdea(
        title="KDR/VEGFR2 mutation-gated TKI validation",
        hypothesis="KDR/FLT4 altered HSA may respond to VEGFR-targeting TKIs.",
        rationale="The committee connected cross-species vascular tumor biology with KDR/FLT4 evidence.",
        candidate_therapies=["toceranib", "sorafenib"],
        targets=["KDR", "FLT4"],
        biomarkers=["KDR mutation", "phospho-VEGFR2"],
        mechanism="VEGFR blockade should suppress downstream angiogenic signaling.",
        evidence_refs=["C1", "C2"],
        evidence_strength="medium",
        translational_path="Use canine HSA as a comparative model for human angiosarcoma.",
        risks=["coagulation risk", "species PK/PD uncertainty"],
        next_experiments=["TKI dose-response assay", "coagulation safety review"],
        priority_score=0.82,
    )
    committee = TherapyCommitteeResult(
        topic="KDR VEGFA mTOR therapy ideas for canine hemangiosarcoma",
        disease_scope="canine hemangiosarcoma and human angiosarcoma",
        review_mode="openrouter_required",
        ranked_ideas=[idea],
        decision_summary="Top idea is KDR/VEGFR2 mutation-gated TKI validation.",
        evidence={"citation_count": 2, "recommend_only": True},
    )
    agent_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            model_profile="therapy_committee",
            status=RunStatus.COMPLETED,
            output_payload=committee.model_dump(mode="json"),
            summary={"idea_count": 1},
        )
    )
    service = HSAResearchService(repo)

    preview = service.queue_therapy_committee_validation_requests(
        TherapyCommitteeValidationQueueRequest(agent_run_id=agent_run.agent_run_id)
    )
    applied = service.queue_therapy_committee_validation_requests(
        TherapyCommitteeValidationQueueRequest(agent_run_id=agent_run.agent_run_id, dry_run=False)
    )
    duplicate = service.queue_therapy_committee_validation_requests(
        TherapyCommitteeValidationQueueRequest(agent_run_id=agent_run.agent_run_id, dry_run=False)
    )
    persisted = service.list_validation_request_queue_items(status="needs_approval", limit=10)

    assert isinstance(preview, TherapyCommitteeValidationQueueResult)
    assert preview.dry_run is True
    assert preview.candidate_idea_count == 1
    assert preview.candidate_task_count == 3
    assert preview.queued_count == 0
    assert applied.queued_count == 3
    assert duplicate.existing_count == 3
    assert len(persisted) == 3
    assert {item.task_type for item in persisted} == {"expert_review", "wet_lab", "safety"}
    assert {item.validation_request.validation_type for item in persisted} == {"expert_review", "wet_lab", "safety"}
    assert all(item.metadata["queued_from"] == "therapy_committee" for item in persisted)
    assert all(item.validation_request.assay_context is not None for item in persisted)
    assert all("source_traceability_required" in item.quality_gates for item in persisted)
    assert repo.get_validation_plan(applied.plan_id) is not None


def test_research_brief_service_runs_three_perspectives_and_synthesis(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:brief",
            content_hash="brief-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/brief/",
            raw_payload={"pmid": "brief"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF therapy in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/brief/",
            dedupe_key="pmid:brief",
            identifiers={"pmid": "brief"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma and human angiosarcoma share vascular biology. "
                "VEGF therapy, toxicity, species mismatch, target selection, translational "
                "biomarker work, and clinical evidence are discussed."
            ),
            content_hash="brief-chunk",
        )
    )
    repo.upsert_research_lead(
        ResearchLeadRecord(
            title="VEGF hemangiosarcoma conference abstract needs durable source review",
            url="https://www.abstractsonline.com/vegf-hsa",
            lead_type="conference_abstract",
            source_key="x_linked_article",
            reason="Agent found a credible but non-durable lead.",
            topic_tags=["hemangiosarcoma", "therapy"],
        )
    )

    result = HSAResearchService(repo).run_research_brief(
        ResearchBriefRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            review_mode="deterministic_only",
            max_chunks_per_perspective=3,
            max_claims=0,
        )
    )
    runs = repo.list_agent_runs(limit=10)

    assert len(result.perspective_reports) == 3
    assert result.final_brief.startswith("# Research Brief")
    assert "[C1]" in result.final_brief
    assert result.citations
    assert result.evidence["research_lead_count"] == 1
    assert result.ranked_hypotheses
    assert result.brief_id is not None
    assert result.agent_run_id is not None
    saved_brief = repo.get_research_brief(result.brief_id)
    assert saved_brief is not None
    assert saved_brief.agent_run_id == result.agent_run_id
    assert saved_brief.citation_count == len(result.citations)
    assert saved_brief.research_lead_count == 1
    assert saved_brief.result_payload["brief_id"] == str(result.brief_id)
    assert HSAResearchService(repo).list_research_briefs(topic_query="VEGF")[0].brief_id == result.brief_id
    assert {run.agent_name for run in runs} >= {
        "evidence_scout_agent",
        "translational_hypothesis_agent",
        "skeptic_validation_agent",
        "research_synthesis_editor_agent",
    }


def test_research_brief_perspective_queries_stay_within_chunk_search_contract():
    long_topic = " ".join(["follow-up research lead with verbose evidence limitation"] * 60)
    request = ResearchBriefRequest(
        topic=long_topic[:1000],
        disease_scope="canine hemangiosarcoma and human angiosarcoma",
        review_mode="deterministic_only",
    )

    queries = research_brief_agent._perspective_queries(request)

    for query_specs in queries.values():
        for query_spec in query_specs:
            assert 1 <= len(query_spec.query) <= 1000
            assert any(
                term in query_spec.query
                for term in ("biomarker", "mechanism", "comparative", "clinical", "negative", "inhibitor", "validation")
            )
            ResearchChunkSearchRequest(query=query_spec.query)


def test_research_brief_model_json_loader_repairs_common_llm_commas():
    payload = research_brief_agent._load_json_object(
        """
        ```json
        {
          "summary": "Model reviewed the cited evidence."
          "findings": [
            {
              "claim": "VEGF signaling is relevant."
              "stance": "supporting",
              "citations": ["C1"],
              "evidence_strength": "medium",
              "reasoning": "The citation discusses VEGF biology.",
              "open_questions": []
            }
            {
              "claim": "Translation remains uncertain.",
              "stance": "risk",
              "citations": ["C2"],
              "evidence_strength": "low",
              "reasoning": "The citation is indirect.",
              "open_questions": []
            },
          ]
          "errors": []
        }
        ```
        """
    )

    assert payload["summary"] == "Model reviewed the cited evidence."
    assert len(payload["findings"]) == 2
    assert payload["errors"] == []


def test_research_brief_openrouter_payload_includes_contract(monkeypatch):
    captured = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "id": "or-test",
                    "model": "anthropic/claude-sonnet-test",
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "summary": "Reviewed.",
                                        "findings": [],
                                        "errors": [],
                                    }
                                )
                            }
                        }
                    ],
                    "usage": {"total_tokens": 100},
                }
            ).encode("utf-8")

    def fake_urlopen(request, timeout):
        captured["timeout"] = timeout
        captured["payload"] = json.loads(request.data.decode("utf-8"))
        return FakeResponse()

    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    monkeypatch.delenv("HSA_RESEARCH_BRIEF_MAX_TOKENS", raising=False)
    monkeypatch.setattr(research_brief_agent.urllib.request, "urlopen", fake_urlopen)

    review = research_brief_agent._openrouter_review_model(
        "anthropic/claude-sonnet-test",
        {"topic": "VEGF therapy", "citations": [{"citation_id": "C1"}]},
    )
    user_payload = json.loads(captured["payload"]["messages"][1]["content"])

    assert captured["payload"]["max_tokens"] == 6000
    assert user_payload["response_contract"]["required"] == [
        "summary",
        "findings",
        "evidence_limitations",
        "errors",
    ]
    assert user_payload["evidence_payload"]["topic"] == "VEGF therapy"
    assert review["metadata"]["request_id"] == "or-test"


def test_research_brief_model_report_splits_limitations_from_errors():
    citation = ResearchBriefCitation(
        citation_id="C1",
        chunk_id=uuid4(),
        research_object_id=uuid4(),
        quote="VEGF therapy evidence was reviewed.",
        relevance="evidence_scout:direct_evidence",
    )
    evidence = research_brief_agent.ResearchBriefEvidenceBundle(
        citations=[citation],
        claims=[],
        research_leads=[],
        search_queries={},
        errors=["claim search failed: timeout"],
    )

    report = research_brief_agent._perspective_report_from_model(
        ResearchBriefRequest(topic="VEGF therapy"),
        "evidence_scout",
        evidence,
        {
            "metadata": {"model": "test-model"},
            "text": json.dumps(
                {
                    "summary": "Reviewed supplied evidence.",
                    "findings": [
                        {
                            "claim": "VEGF therapy has enough evidence for review.",
                            "stance": "supporting",
                            "citations": ["C1"],
                            "evidence_strength": "medium",
                            "reasoning": "The supplied citation discusses VEGF therapy.",
                            "open_questions": [],
                        }
                    ],
                    "evidence_limitations": ["No direct survival endpoint was supplied."],
                    "errors": [
                        "No supplied citation directly addresses dosing.",
                        "Invalid citation C99 was ignored.",
                    ],
                }
            ),
        },
    )

    assert report.errors == ["claim search failed: timeout", "Invalid citation C99 was ignored."]
    assert report.evidence_limitations == [
        "No direct survival endpoint was supplied.",
        "No supplied citation directly addresses dosing.",
    ]


def test_research_brief_evaluation_service_persists_ready_result(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-evaluation.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:evaluation-brief",
            content_hash="evaluation-brief-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/evaluation-brief/",
            raw_payload={"pmid": "evaluation-brief"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF therapy and translational validation in hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/evaluation-brief/",
            dedupe_key="pmid:evaluation-brief",
            identifiers={"pmid": "evaluation-brief"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma and human angiosarcoma share vascular biology. "
                "VEGF therapy, biomarker validation, species mismatch, toxicity, clinical "
                "translation, and target selection are all discussed as research needs."
            ),
            content_hash="evaluation-brief-chunk",
        )
    )
    service = HSAResearchService(repo)
    brief = service.run_research_brief(
        ResearchBriefRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            review_mode="deterministic_only",
            max_chunks_per_perspective=3,
            max_claims=0,
        )
    )

    result = service.evaluate_research_brief(
        ResearchBriefEvaluationRequest(brief_id=brief.brief_id)
    )
    saved = repo.get_research_brief_evaluation(result.evaluation_id)
    runs = repo.list_agent_runs(agent_name="research_brief_synthesis_evaluator_agent", status="completed")

    assert result.readiness == "ready_for_hypothesis_review"
    assert result.passes_quality_bar is True
    assert result.overall_score >= 0.7
    assert result.agent_run_id is not None
    assert saved is not None
    assert saved.brief_id == brief.brief_id
    assert runs[0].output_payload["evaluation_id"] == str(result.evaluation_id)


def test_research_brief_evaluation_service_blocks_uncited_record(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-evaluation-blocked.sqlite3", seed=False)
    record = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="VEGF therapy in canine hemangiosarcoma",
            disease_scope="canine hemangiosarcoma",
            source_key="pubmed",
            review_mode="deterministic_only",
            final_brief="Stored synthesis without citations.",
            result_payload={
                "topic": "VEGF therapy in canine hemangiosarcoma",
                "disease_scope": "canine hemangiosarcoma",
                "final_brief": "Stored synthesis without citations.",
                "citations": [],
                "perspective_reports": [],
                "ranked_hypotheses": [],
                "unresolved_questions": [],
                "evidence": {},
                "errors": [],
            },
        )
    )

    result = HSAResearchService(repo).evaluate_research_brief(
        ResearchBriefEvaluationRequest(brief_id=record.brief_id)
    )

    assert result.readiness == "blocked"
    assert result.passes_quality_bar is False
    assert result.citation_coverage_score == 0.0


def test_research_brief_evaluation_tracks_soft_evidence_limitations(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-evaluation-limitations.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:evaluation-limitations",
            content_hash="evaluation-limitations-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/evaluation-limitations/",
            raw_payload={"pmid": "evaluation-limitations"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF therapy and translational validation in hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/evaluation-limitations/",
            dedupe_key="pmid:evaluation-limitations",
            identifiers={"pmid": "evaluation-limitations"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma and human angiosarcoma share vascular biology. "
                "VEGF therapy, biomarker validation, species mismatch, toxicity, clinical "
                "translation, and target selection are all discussed as research needs."
            ),
            content_hash="evaluation-limitations-chunk",
        )
    )
    service = HSAResearchService(repo)
    brief = service.run_research_brief(
        ResearchBriefRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            review_mode="deterministic_only",
            max_chunks_per_perspective=3,
            max_claims=0,
        )
    )
    saved = repo.get_research_brief(brief.brief_id)
    assert saved is not None
    payload = dict(saved.result_payload)
    payload["errors"] = [
        "No supplied citation directly addresses a clinical trial outcome; evidence is indirect."
    ]
    repo.upsert_research_brief(
        saved.model_copy(update={"result_payload": payload, "error_count": 1})
    )

    result = service.evaluate_research_brief(ResearchBriefEvaluationRequest(brief_id=brief.brief_id))

    assert result.readiness == "ready_for_hypothesis_review"
    assert result.passes_quality_bar is True
    assert result.errors == []
    assert result.evidence["synthesis_limitation_count"] == 1
    assert any("follow-up research queue" in item for item in result.recommendations)


def test_research_brief_evaluation_keeps_system_errors_hard(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-evaluation-hard-errors.sqlite3", seed=False)
    citation = {
        "citation_id": "C1",
        "chunk_id": str(uuid4()),
        "research_object_id": str(uuid4()),
        "quote": "VEGF therapy and validation are discussed.",
        "relevance": "evidence_scout:direct_evidence",
    }
    finding = {
        "claim": "VEGF therapy should be reviewed for validation.",
        "stance": "supporting",
        "citations": ["C1"],
        "evidence_strength": "medium",
        "reasoning": "The supplied citation discusses VEGF therapy and validation.",
        "open_questions": ["What validation experiment should run next?"],
    }
    record = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="VEGF therapy in canine hemangiosarcoma",
            disease_scope="canine hemangiosarcoma",
            source_key="pubmed",
            review_mode="deterministic_only",
            final_brief="VEGF therapy has evidence for validation planning [C1].",
            result_payload={
                "topic": "VEGF therapy in canine hemangiosarcoma",
                "disease_scope": "canine hemangiosarcoma",
                "brief_style": "technical",
                "model_profile": "research_brief",
                "final_brief": "VEGF therapy has evidence for validation planning [C1].",
                "citations": [citation],
                "perspective_reports": [
                    {
                        "perspective": "evidence_scout",
                        "agent_name": "evidence_scout_agent",
                        "model_profile": "research_brief",
                        "summary": "Evidence was reviewed.",
                        "findings": [finding],
                        "citations": [citation],
                        "errors": [],
                    },
                    {
                        "perspective": "translational_hypothesis",
                        "agent_name": "translational_hypothesis_agent",
                        "model_profile": "research_brief",
                        "summary": "Translation was reviewed.",
                        "findings": [finding],
                        "citations": [citation],
                        "errors": [],
                    },
                    {
                        "perspective": "skeptic_validation",
                        "agent_name": "skeptic_validation_agent",
                        "model_profile": "research_brief",
                        "summary": "Risks were reviewed.",
                        "findings": [finding],
                        "citations": [citation],
                        "errors": [],
                    },
                ],
                "ranked_hypotheses": [finding],
                "unresolved_questions": ["What validation experiment should run next?"],
                "evidence": {},
                "errors": ["chunk search failed: upstream timeout"],
            },
        )
    )

    result = HSAResearchService(repo).evaluate_research_brief(
        ResearchBriefEvaluationRequest(brief_id=record.brief_id)
    )

    assert result.readiness == "needs_human_review"
    assert result.passes_quality_bar is False
    assert result.errors == ["chunk search failed: upstream timeout"]


def test_validation_planning_service_persists_ready_recommend_only_plan(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-planning.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:validation-plan",
            content_hash="validation-plan-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/validation-plan/",
            raw_payload={"pmid": "validation-plan"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF therapy and target validation in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/validation-plan/",
            dedupe_key="pmid:validation-plan",
            identifiers={"pmid": "validation-plan"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma and human angiosarcoma share vascular biology. "
                "VEGF, KDR, toxicity, biomarker expression, docking, and translational "
                "target validation are discussed as testable research paths."
            ),
            content_hash="validation-plan-chunk",
        )
    )
    service = HSAResearchService(repo)
    brief = service.run_research_brief(
        ResearchBriefRequest(
            topic="VEGF omics biomarker expression target validation in canine hemangiosarcoma",
            review_mode="deterministic_only",
            max_chunks_per_perspective=3,
            max_claims=0,
        )
    )
    evaluation = service.evaluate_research_brief(ResearchBriefEvaluationRequest(brief_id=brief.brief_id))

    result = service.plan_validation(
        ValidationPlanRequest(evaluation_id=evaluation.evaluation_id, max_tasks=6)
    )
    saved = repo.get_validation_plan(result.plan_id)
    runs = repo.list_agent_runs(agent_name="validation_planning_agent", status="completed")

    assert result.status == "ready_for_review"
    assert result.readiness == "ready_for_expert_review"
    assert result.agent_run_id is not None
    assert result.hypothesis_drafts
    assert result.tasks
    omics_tasks = [task for task in result.tasks if task.task_type == "omics"]
    assert omics_tasks
    assert all(task.validation_request is not None for task in omics_tasks)
    assert {task.validation_request.validation_type for task in omics_tasks if task.validation_request} == {"omics"}
    assert all(task.requires_human_approval for task in result.tasks)
    assert all(task.validation_request is None or task.validation_request.require_approval for task in result.tasks)
    validation_requests = [task.validation_request for task in result.tasks if task.validation_request is not None]
    assert validation_requests
    assert all(request.assay_context is not None for request in validation_requests)
    assert any("canine" in request.assay_context.species for request in validation_requests if request.assay_context)
    assert all("source_traceability_required" in request.quality_gates for request in validation_requests)
    omics_requests = [request for request in validation_requests if request.validation_type == "omics"]
    assert all("omics_dataset_context_required" in request.quality_gates for request in omics_requests)
    assert saved is not None
    assert saved.brief_id == brief.brief_id
    assert saved.evaluation_id == evaluation.evaluation_id
    assert saved.task_count == len(result.tasks)
    assert runs[0].output_payload["plan_id"] == str(result.plan_id)
    assert service.get_run_status(uuid4()) is None


def test_validation_request_queue_promotes_ready_plan_tasks(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-request-queue-service.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:validation-request-queue",
            content_hash="validation-request-queue-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/validation-request-queue/",
            raw_payload={"pmid": "validation-request-queue"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF target validation request queue evidence",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/validation-request-queue/",
            dedupe_key="pmid:validation-request-queue",
            identifiers={"pmid": "validation-request-queue"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma target validation evidence discusses VEGF, "
                "KDR, toxicity, docking, and translational biomarker review."
            ),
            content_hash="validation-request-queue-chunk",
        )
    )
    service = HSAResearchService(repo)
    brief = service.run_research_brief(
        ResearchBriefRequest(
            topic="VEGF omics biomarker expression target validation in canine hemangiosarcoma",
            review_mode="deterministic_only",
            max_chunks_per_perspective=3,
            max_claims=0,
        )
    )
    evaluation = service.evaluate_research_brief(ResearchBriefEvaluationRequest(brief_id=brief.brief_id))
    plan = service.plan_validation(ValidationPlanRequest(evaluation_id=evaluation.evaluation_id, max_tasks=6))

    preview = service.queue_validation_requests_from_plan(
        ValidationRequestQueueRequest(plan_id=plan.plan_id, dry_run=True)
    )
    applied = service.queue_validation_requests_from_plan(
        ValidationRequestQueueRequest(plan_id=plan.plan_id, dry_run=False)
    )
    duplicate = service.queue_validation_requests_from_plan(
        ValidationRequestQueueRequest(plan_id=plan.plan_id, dry_run=False)
    )
    queued_item = applied.queue_items[0]
    blocked_dispatch = service.dispatch_validation_request_queue_item(queued_item.queue_item_id)
    approved = service.approve_validation_request_queue_item(
        queued_item.queue_item_id,
        approved_by="unit-test",
        approval_note="Ready for controlled validation.",
    )
    dispatched = service.dispatch_validation_request_queue_item(
        queued_item.queue_item_id,
        model_profile="deterministic_only",
    )

    assert preview.dry_run is True
    assert preview.queued_count == 0
    assert preview.queue_items
    assert applied.queued_count == len(applied.queue_items)
    assert any(item.validation_request.validation_type == "omics" for item in applied.queue_items)
    assert duplicate.queued_count == 0
    assert duplicate.existing_count == len(applied.queue_items)
    assert blocked_dispatch is not None
    assert blocked_dispatch.status == "needs_approval"
    assert "approved before dispatch" in blocked_dispatch.last_error
    assert approved is not None
    assert approved.status == "approved"
    assert dispatched is not None
    assert dispatched.status == "completed"
    assert dispatched.last_run_id is not None
    assert service.get_agent_run(dispatched.last_run_id) is not None
    assert dispatched.metadata["validation_agent_result"]["decision"] in {"promote", "hold", "demote"}


def test_validation_request_queue_blocks_dispatch_without_assay_context(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-request-queue-blocked.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Docking queue guardrail",
            task_type="docking",
            title="Dock KDR candidate",
            objective="Run docking only after target, candidate, and assay context are present.",
            rationale="Execution lanes need enough context to be reproducible.",
            validation_request=ValidationRequest(
                validation_type="docking",
                target_name="KDR",
                candidate_name="candidate A",
                objective="Dock candidate A against KDR.",
                require_approval=True,
            ),
        )
    )
    approved = service.approve_validation_request_queue_item(
        item.queue_item_id,
        approved_by="unit-test",
        approval_note="Approval alone should not bypass execution context.",
    )
    blocked = service.dispatch_validation_request_queue_item(item.queue_item_id)

    assert approved is not None
    assert approved.status == "approved"
    assert blocked is not None
    assert blocked.status == "blocked"
    assert "assay_context_required" in blocked.dispatch_blockers
    assert "dispatch blocked" in blocked.last_error.lower()
    assert blocked.last_run_id is None


def test_omics_validation_request_dispatches_after_approval(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "omics-validation-request.sqlite3", seed=False)
    service = HSAResearchService(repo)
    context = ValidationAssayContext(
        disease_context="canine hemangiosarcoma and human angiosarcoma",
        species=["canine", "human"],
        model_system="Comparative canine and human molecular dataset review.",
        assay_type="omics evidence review",
        readout="species-conserved signal, expression context, and dataset caveats",
        endpoint="translational molecular support",
        evidence_refs=["brief:1", "evaluation:1", "C1"],
        negative_evidence_needs=["Check whether negative or null expression datasets exist."],
    )
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Omics support queue guardrail",
            task_type="omics",
            title="Omics support check",
            objective="Check expression and biomarker evidence across canine and human datasets.",
            rationale="The synthesis identified a molecular support question.",
            validation_request=ValidationRequest(
                validation_type="omics",
                objective="Review comparative omics support.",
                require_approval=True,
                assay_context=context,
                quality_gates=["omics_dataset_context_required"],
            ),
            quality_gates=["omics_dataset_context_required"],
            metadata={
                "evidence_refs": ["brief:1", "evaluation:1", "C1"],
                "expected_outputs": ["dataset support", "species translation notes"],
            },
        )
    )

    approved = service.approve_validation_request_queue_item(
        item.queue_item_id,
        approved_by="unit-test",
        approval_note="Omics context is present.",
    )
    dispatched = service.dispatch_validation_request_queue_item(
        item.queue_item_id,
        model_profile="deterministic_only",
    )

    assert approved is not None
    assert approved.status == "approved"
    assert dispatched is not None
    assert dispatched.status == "completed"
    assert "omics_dataset_context_required" in dispatched.quality_gates
    assert dispatched.metadata["validation_agent_result"]["agent_name"] == "omics_validation_agent"
    assert dispatched.metadata["validation_agent_result"]["validation_type"] == "omics"


def test_validation_autopilot_dry_run_selects_allowlisted_items(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-autopilot-preview.sqlite3", seed=False)
    service = HSAResearchService(repo)
    eligible = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Autopilot eligible review",
            task_type="expert_review",
            title="Expert review: VEGF evidence packet",
            objective="Review the evidence packet.",
            rationale="This is a low-risk recommend-only review.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review the evidence packet.",
            ),
            created_at=datetime.now(UTC) - timedelta(hours=2),
        )
    )
    repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Autopilot excluded wet lab",
            task_type="wet_lab",
            title="Wet lab protocol",
            objective="Design an experiment.",
            rationale="Wet lab work stays manual.",
            validation_request=ValidationRequest(
                validation_type="wet_lab",
                objective="Design an experiment.",
            ),
            created_at=datetime.now(UTC) - timedelta(hours=2),
        )
    )

    result = service.preview_validation_autopilot(
        ValidationAutopilotRequest(
            model_profile="deterministic_only",
            minimum_queue_age_hours=1.0,
        )
    )
    stored = service.get_validation_request_queue_item(eligible.queue_item_id)

    assert result.dry_run is True
    assert result.selected_count == 1
    assert result.selected[0].queue_item_id == eligible.queue_item_id
    assert any(record.reason == "task_type_not_allowlisted:wet_lab" for record in result.skipped)
    assert stored is not None
    assert stored.status == "needs_approval"
    assert stored.approved_by is None


def test_validation_autopilot_apply_dispatches_deterministic_item(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-autopilot-apply.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Autopilot apply review",
            task_type="expert_review",
            title="Expert review: KDR mutation signal",
            objective="Review KDR mutation support.",
            rationale="This is a low-risk recommend-only review.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review KDR mutation support.",
            ),
            metadata={"expected_outputs": ["go/no-go validation readiness"], "evidence_refs": ["C1", "C2"]},
        )
    )

    result = service.run_validation_autopilot(
        ValidationAutopilotRequest(
            dry_run=False,
            force=True,
            model_profile="deterministic_only",
            minimum_queue_age_hours=0.0,
        )
    )
    stored = service.get_validation_request_queue_item(item.queue_item_id)

    assert result.agent_run_id is not None
    assert result.dispatched_count == 1
    assert result.actual_cost_usd == 0.0
    assert stored is not None
    assert stored.status == "completed"
    assert stored.approved_by == "validation_autopilot"
    assert stored.metadata["validation_autopilot"]["result_status"] == "completed"
    assert stored.metadata["validation_agent_result"]["decision"] in {"promote", "hold", "demote"}
    assert service.get_agent_run(result.agent_run_id) is not None


def test_validation_autopilot_blocks_recent_manual_activity(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-autopilot-grace.sqlite3", seed=False)
    service = HSAResearchService(repo)
    candidate = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Autopilot grace candidate",
            task_type="expert_review",
            title="Expert review candidate",
            objective="Review the evidence packet.",
            rationale="This should wait because an operator was active.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review the evidence packet.",
            ),
            created_at=datetime.now(UTC) - timedelta(hours=2),
        )
    )
    manual = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            status="approved",
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Recent manual activity",
            task_type="expert_review",
            title="Manually approved review",
            objective="Review manually.",
            rationale="This records recent operator activity.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review manually.",
            ),
            approved_by="operator",
            metadata={"approved_at": datetime.now(UTC).isoformat()},
        )
    )

    result = service.run_validation_autopilot(
        ValidationAutopilotRequest(
            dry_run=False,
            model_profile="deterministic_only",
            minimum_queue_age_hours=0.0,
            manual_grace_period_hours=6.0,
        )
    )
    stored = service.get_validation_request_queue_item(candidate.queue_item_id)

    assert manual.approved_by == "operator"
    assert "manual_grace_period_active" in result.blockers
    assert result.selected_count == 1
    assert result.dispatched_count == 0
    assert stored is not None
    assert stored.status == "needs_approval"
    assert stored.approved_by is None


def test_validation_autopilot_blocks_openrouter_missing_before_mutation(monkeypatch, tmp_path):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    repo = SQLiteResearchRepository(tmp_path / "validation-autopilot-openrouter.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Autopilot live model candidate",
            task_type="expert_review",
            title="Expert review live model candidate",
            objective="Review the evidence packet.",
            rationale="OpenRouter must be ready before mutation.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review the evidence packet.",
            ),
            created_at=datetime.now(UTC) - timedelta(hours=2),
        )
    )

    result = service.run_validation_autopilot(
        ValidationAutopilotRequest(
            dry_run=False,
            model_profile="openrouter_required",
            minimum_queue_age_hours=0.0,
        )
    )
    stored = service.get_validation_request_queue_item(item.queue_item_id)

    assert "openrouter_api_key_missing" in result.blockers
    assert result.dispatched_count == 0
    assert stored is not None
    assert stored.status == "needs_approval"
    assert stored.approved_by is None
    assert stored.attempts == 0


def test_validation_request_queue_records_failed_live_agent_dispatch_without_key(monkeypatch, tmp_path):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    repo = SQLiteResearchRepository(tmp_path / "validation-agent-failure.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="VEGF validation agent dispatch",
            task_type="expert_review",
            title="Expert review: VEGF translational signal",
            objective="Review whether the hypothesis has enough evidence for validation.",
            rationale="Human approval is required before dispatch.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review whether the hypothesis has enough evidence for validation.",
                require_approval=True,
                assay_context=ValidationAssayContext(
                    disease_context="canine hemangiosarcoma and human angiosarcoma",
                    species=["canine", "human"],
                    model_system="human-reviewed evidence packet",
                    assay_type="expert evidence review",
                    readout="go/no-go validation readiness",
                ),
            ),
            quality_gates=["human_approval_required", "assay_context_present"],
        )
    )
    service.approve_validation_request_queue_item(item.queue_item_id, approved_by="unit-test")

    failed = service.dispatch_validation_request_queue_item(
        item.queue_item_id,
        model_profile="openrouter_required",
    )

    assert failed is not None
    assert failed.status == "failed"
    assert "OPENROUTER_API_KEY" in failed.last_error
    assert failed.metadata["validation_agent_model_profile"] == "openrouter_required"


def test_evidence_gap_resolver_creates_research_leads_and_brief_queue_items(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "evidence-gap-resolver.sqlite3", seed=False)
    service = HSAResearchService(repo)
    queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="KDR VEGFR2 validation",
            task_type="expert_review",
            title="Expert review: KDR mutation-gated TKI validation",
            objective="Review KDR mutation function and TKI response evidence.",
            rationale="The validation agent held the idea pending stronger evidence.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                target_name="KDR",
                candidate_name="sorafenib",
                objective="Review KDR mutation function and TKI response evidence.",
                require_approval=False,
                assay_context=ValidationAssayContext(
                    disease_context="canine hemangiosarcoma and human angiosarcoma",
                    species=["canine", "human"],
                    model_system="evidence packet",
                    assay_type="expert evidence review",
                    readout="go/no-go validation readiness",
                ),
            ),
            status="completed",
            last_run_id=uuid4(),
            metadata={
                "idea_id": str(uuid4()),
                "idea_title": "KDR mutation-gated TKI validation",
                "validation_agent_result": ValidationAgentResult(
                    agent_run_id=uuid4(),
                    queue_item_id=uuid4(),
                    plan_id=uuid4(),
                    task_id=uuid4(),
                    task_type="expert_review",
                    validation_type="expert_review",
                    agent_name="evidence_review_validation_agent",
                    model_profile="openrouter_required",
                    decision="hold",
                    confidence=0.67,
                    summary="Hold pending mutation function and clinical response evidence.",
                    evidence_used=["C1", "C2"],
                    missing_evidence=[
                        "Functional classification of KDR/FLT4 mutations as activating versus passenger.",
                        "Clinical response correlation between KDR mutation status and TKI response.",
                    ],
                    risks=["Sorafenib coagulopathy and hemorrhage safety risk."],
                    next_actions=["Validate phospho-VEGFR2 IHC assay as a pharmacodynamic readout."],
                ).model_dump(mode="json"),
            },
        )
    )

    preview = service.resolve_evidence_gaps(
        EvidenceGapResolverRequest(queue_item_ids=[queue_item.queue_item_id])
    )
    applied = service.resolve_evidence_gaps(
        EvidenceGapResolverRequest(
            queue_item_ids=[queue_item.queue_item_id],
            dry_run=False,
            queue_research_briefs=True,
        )
    )
    duplicate = service.resolve_evidence_gaps(
        EvidenceGapResolverRequest(queue_item_ids=[queue_item.queue_item_id], dry_run=False)
    )
    leads = repo.list_research_leads(status="new", limit=20)
    queued_briefs = repo.list_research_brief_queue_items(status="queued", limit=20)

    assert isinstance(preview, EvidenceGapResolverResult)
    assert preview.dry_run is True
    assert preview.gap_count == 4
    assert preview.leads_created == 0
    assert applied.queue_items_seen == 1
    assert applied.gap_count == 4
    assert applied.leads_created == 4
    assert applied.brief_queue_count == 4
    assert duplicate.existing_leads == 4
    assert len(leads) == 4
    assert len(queued_briefs) == 4
    assert {"mutation_function", "clinical_response", "safety_signal", "assay_protocol"}.issubset(
        {lead.metadata["evidence_gap_resolver"]["lane"] for lead in leads}
    )
    assert all("validation_gap" in lead.topic_tags for lead in leads)
    assert repo.list_agent_runs(agent_name="evidence_gap_resolver_agent", status="completed", limit=1)


def test_validation_gap_source_pack_builds_and_persists_targeted_queries(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-gap-source-pack.sqlite3", seed=False)
    service = HSAResearchService(repo)
    plan_id = uuid4()
    task_id = uuid4()
    origin_agent_run_id = uuid4()
    queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=plan_id,
            task_id=task_id,
            brief_id=uuid4(),
            topic="Sorafenib VEGFR safety validation",
            task_type="expert_review",
            title="Review sorafenib safety in canine HSA",
            objective="Find direct canine sorafenib safety and PK/PD evidence before promotion.",
            rationale="Validation held because direct canine dosing evidence is missing.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                candidate_name="sorafenib",
                target_name="KDR",
                objective="Find direct canine sorafenib safety and PK/PD evidence before promotion.",
                require_approval=False,
            ),
            status="completed",
            metadata={
                "validation_agent_result": ValidationAgentResult(
                    queue_item_id=uuid4(),
                    plan_id=plan_id,
                    task_id=task_id,
                    task_type="expert_review",
                    validation_type="expert_review",
                    agent_name="evidence_review_validation_agent",
                    model_profile="openrouter_required",
                    decision="hold",
                    confidence=0.62,
                    summary="Hold pending direct canine dosing evidence.",
                    evidence_used=["C1"],
                    missing_evidence=["Direct canine sorafenib safety and dose-limiting toxicity evidence."],
                ).model_dump(mode="json"),
            },
        )
    )
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            identity_key=f"research_lead:validation_gap:{queue_item.queue_item_id}:safety",
            title="Safety signal: direct canine sorafenib DLT evidence",
            status="new",
            priority=10,
            source_key="pubmed",
            origin_source_key="validation_agent",
            origin_record_id=str(queue_item.queue_item_id),
            origin_review_id=queue_item.queue_item_id,
            origin_agent_run_id=origin_agent_run_id,
            reason="Direct canine sorafenib safety and dose-limiting toxicity evidence.",
            evidence_refs=[f"validation_queue:{queue_item.queue_item_id}"],
            topic_tags=["validation_gap", "safety_signal", "missing_evidence"],
            suggested_sources=["pubmed", "chembl", "openfda_animal_events"],
            metadata={
                "evidence_gap_resolver": {
                    "origin": "validation_agent_result",
                    "gap_type": "missing_evidence",
                    "lane": "safety_signal",
                    "gap_text": "Direct canine sorafenib safety and dose-limiting toxicity evidence.",
                    "queue_item_id": str(queue_item.queue_item_id),
                    "plan_id": str(plan_id),
                    "task_id": str(task_id),
                    "task_type": "expert_review",
                    "validation_type": "expert_review",
                    "decision": "hold",
                }
            },
        )
    )

    preview = service.build_validation_gap_source_pack(
        ValidationGapSourcePackRequest(
            lead_ids=[lead.lead_id],
            source_keys=["pubmed", "europe_pmc", "chembl", "openfda_animal_events"],
            max_queries_per_lane=5,
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="old_source_pack_query",
            query_text="stale sorafenib query",
            query_params={
                "source_pack_request": {"persist_queries": True},
                "lead_id": str(lead.lead_id),
                "lane": "safety_signal",
            },
            track="validation_gap",
            active=True,
        )
    )
    applied = service.build_validation_gap_source_pack(
        ValidationGapSourcePackRequest(
            lead_ids=[lead.lead_id],
            source_keys=["pubmed", "europe_pmc", "chembl", "openfda_animal_events"],
            max_queries_per_lane=5,
            persist_queries=True,
            dry_run=False,
        )
    )
    stored_queries = repo.list_source_queries(active_only=True)
    all_stored_queries = repo.list_source_queries(active_only=False)
    stale_query = next(query for query in all_stored_queries if query.query_name == "old_source_pack_query")

    assert isinstance(preview, ValidationGapSourcePackResult)
    assert preview.query_count == 4
    assert preview.persisted_query_count == 0
    assert {query.source_key for query in preview.queries} == {"pubmed", "europe_pmc", "chembl", "openfda_animal_events"}
    assert any("sorafenib" in query.query_text.lower() and "safety" in query.query_text.lower() for query in preview.queries)
    assert applied.persisted_query_count == 4
    assert len(stored_queries) == 4
    assert stale_query.active is False
    assert all(query.track == "validation_gap" for query in stored_queries)
    assert all(query.query_params["followup_lane"] == "agent_evaluator_followup" for query in stored_queries)
    assert all(query.query_params["origin_review_id"] == str(queue_item.queue_item_id) for query in stored_queries)
    assert all(query.query_params["origin_agent_run_id"] == str(origin_agent_run_id) for query in stored_queries)
    europe_pmc_query = next(query for query in stored_queries if query.source_key == "europe_pmc")
    assert europe_pmc_query.query_params["fetch_full_text"] is True
    assert europe_pmc_query.query_params["full_text_time_budget_seconds"] == 20
    assert repo.list_agent_runs(agent_name="validation_gap_source_pack_agent", status="completed", limit=2)


def test_validation_gap_source_pack_compacts_long_candidate_and_target_terms(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-gap-source-pack-compact.sqlite3", seed=False)
    service = HSAResearchService(repo)
    plan_id = uuid4()
    task_id = uuid4()
    queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=plan_id,
            task_id=task_id,
            brief_id=uuid4(),
            topic="Anti-PD-1 VEGFR-2 safety validation",
            task_type="expert_review",
            title="Review anti-PD-1 and VEGFR-2 safety in canine HSA",
            objective="Find canine anti-PD-1 monotherapy safety evidence.",
            rationale="Validation held because direct canine HSA evidence is missing.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                candidate_name=(
                    "Canine-specific anti-PD-1 monoclonal antibody (e.g., ca-4F12-E6 or next-generation "
                    "canine PD-1 inhibitor) / VEGFR-2 blocking antibody or small-molecule VEGFR-2 inhibitor "
                    "(toceranib phosphate as a clinically available canine-approved option) / Combination arm: "
                    "anti-PD-1 + VEGFR-2 inhibitor administered concurrently or sequentially following splenectomy"
                ),
                target_name="PD-1 / PD-L1 axis / VEGFR-2 (KDR) / VEGF (upstream ligand)",
                objective="Find canine anti-PD-1 monotherapy safety evidence.",
                require_approval=False,
            ),
            status="completed",
            metadata={},
        )
    )
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            identity_key=f"research_lead:validation_gap:{queue_item.queue_item_id}:safety",
            title="Safety signal: anti-PD-1 monotherapy in canine HSA",
            status="new",
            priority=10,
            origin_source_key="validation_agent",
            origin_record_id=str(queue_item.queue_item_id),
            origin_review_id=queue_item.queue_item_id,
            origin_agent_run_id=uuid4(),
            reason=(
                "Anti-PD-1 monotherapy efficacy and safety data in canine HSA; "
                "the ca-4F12-E6 precedent is melanoma-only."
            ),
            evidence_refs=[f"validation_queue:{queue_item.queue_item_id}"],
            topic_tags=["validation_gap", "safety_signal", "missing_evidence"],
            suggested_sources=["pubmed", "chembl", "openfda_animal_events"],
            metadata={
                "evidence_gap_resolver": {
                    "origin": "validation_agent_result",
                    "gap_type": "missing_evidence",
                    "lane": "safety_signal",
                    "gap_text": (
                        "Anti-PD-1 monotherapy efficacy and safety data in canine HSA; "
                        "the ca-4F12-E6 precedent is melanoma-only."
                    ),
                    "queue_item_id": str(queue_item.queue_item_id),
                    "plan_id": str(plan_id),
                    "task_id": str(task_id),
                    "task_type": "expert_review",
                    "validation_type": "expert_review",
                    "decision": "hold",
                }
            },
        )
    )

    result = service.build_validation_gap_source_pack(
        ValidationGapSourcePackRequest(
            lead_ids=[lead.lead_id],
            source_keys=["pubmed", "chembl", "openfda_animal_events"],
            max_queries_per_lane=5,
        )
    )
    query_text = " ".join(query.query_text for query in result.queries).lower()
    required_terms = [term for query in result.queries for term in query.required_terms]

    assert result.query_count == 3
    assert "combination arm" not in query_text
    assert "administered concurrently" not in query_text
    assert "anti-pd-1" in query_text
    assert "ca-4f12-e6" in query_text
    assert "toceranib" not in query_text
    assert "vegfr-2" not in query_text
    assert all(len(term) <= 80 for term in required_terms)


def test_validation_gap_ingest_runs_only_validation_gap_queries(monkeypatch):
    repo = InMemoryResearchRepository()
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="validation_gap_safety",
            query_text="sorafenib canine safety",
            query_params={
                "lane": "safety_signal",
                "validation_gap": True,
                "source_pack_request": {"dry_run": False},
                "require_policy_match": False,
                "mindate": "2026/01/01",
            },
            track="validation_gap",
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="starter_query",
            query_text="hemangiosarcoma",
            track="comparative_oncology",
        )
    )
    calls = []

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            calls.append((query.source_key, query.query_name, limit, persist_query, query.query_params))
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                raw_records=2,
                research_objects=2,
                document_chunks=2,
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    preview = HSAResearchService(repo).ingest_validation_gap_source_queries(
        ValidationGapSourceIngestRequest(source_keys=["pubmed"])
    )
    applied = HSAResearchService(repo).ingest_validation_gap_source_queries(
        ValidationGapSourceIngestRequest(source_keys=["pubmed"], dry_run=False, limit_per_query=3)
    )

    assert preview.dry_run is True
    assert preview.query_count == 1
    assert preview.attempted_query_count == 0
    assert applied.query_count == 1
    assert applied.completed_query_count == 1
    assert applied.raw_records == 2
    assert calls == [
        (
            "pubmed",
            "validation_gap_safety",
            3,
            False,
            {"require_policy_match": False, "mindate": "2026/01/01"},
        )
    ]


def test_validation_gap_ingest_filters_agent_evaluator_followup_lane(monkeypatch):
    repo = InMemoryResearchRepository()
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    selected = SourceQuery(
        source_key="pubmed",
        query_name="agent_eval_selected",
        query_text="sorafenib canine maximum tolerated dose",
        query_params={
            "followup_lane": "agent_evaluator_followup",
            "origin_review_id": str(origin_review_id),
            "origin_agent_run_id": str(origin_agent_run_id),
        },
        track="validation_gap",
    )
    repo.upsert_source_query(selected)
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="validation_gap_other",
            query_text="sorafenib canine safety",
            query_params={"lane": "safety_signal"},
            track="validation_gap",
        )
    )
    calls = []

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            calls.append((query.query_name, query.query_params))
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    result = HSAResearchService(repo).ingest_validation_gap_source_queries(
        ValidationGapSourceIngestRequest(
            source_keys=["pubmed"],
            followup_lane="agent_evaluator_followup",
            origin_review_ids=[origin_review_id],
            origin_agent_run_ids=[origin_agent_run_id],
            dry_run=False,
        )
    )

    assert result.query_count == 1
    assert result.source_queries == [selected]
    assert calls == [("agent_eval_selected", {})]


def test_research_followup_loop_runs_search_and_updates_status(monkeypatch):
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine DLT",
            status="followup",
            priority=5,
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            metadata={"created_by": "agent_finding_escalation_agent"},
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="agent_eval_sorafenib",
            query_text="sorafenib canine maximum tolerated dose",
            query_params={
                "followup_lane": "agent_evaluator_followup",
                "origin_review_id": str(origin_review_id),
                "origin_agent_run_id": str(origin_agent_run_id),
            },
            track="validation_gap",
        )
    )

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            research_object = ResearchObject(
                object_type="publication",
                title="Sorafenib dose-limiting toxicity in dogs",
                abstract="Canine sorafenib maximum tolerated dose and dose-limiting toxicity data.",
                source_key=query.source_key,
                dedupe_key=f"{query.source_key}:{query.query_name}:strong",
            )
            self.repository.research_objects[research_object.id] = research_object
            for index in range(2):
                chunk = DocumentChunk(
                    research_object_id=research_object.id,
                    chunk_index=index,
                    section_label="abstract",
                    text_content=(
                        "Sorafenib was evaluated in canine patients with safety, tolerability, "
                        "maximum tolerated dose, and dose-limiting toxicity endpoints."
                    ),
                    content_hash=f"{query.source_key}:{query.query_name}:strong:{index}",
                )
                self.repository.document_chunks[chunk.id] = chunk
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                raw_records=1,
                research_objects=1,
                document_chunks=2,
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    result = service.run_research_followup_loop(
        ResearchFollowupLoopRequest(
            lead_id=lead.lead_id,
            dry_run=False,
            ingest=True,
            resolve=False,
            evaluate=False,
            operator="operator",
        )
    )
    updated = repo.get_research_lead(lead.lead_id)
    loop_runs = repo.list_agent_runs(agent_name="research_followup_loop_agent", status="completed", limit=5)

    assert isinstance(result, ResearchFollowupLoopResult)
    assert result.query_count == 1
    assert result.document_chunks == 2
    assert result.evidence_fit is not None
    assert result.evidence_fit.fit == "strong"
    assert result.evidence_fit.missing_terms == []
    assert result.lead_status_before == "followup"
    assert result.lead_status_after == "watching"
    assert [transition["to"] for transition in result.status_transitions] == ["queued", "watching"]
    assert updated.status == "watching"
    assert updated.metadata["research_followup_loop"]["document_chunks"] == 2
    assert updated.metadata["research_followup_loop"]["evidence_fit"]["fit"] == "strong"
    assert loop_runs
    assert loop_runs[0].summary["document_chunks"] == 2
    assert loop_runs[0].summary["evidence_fit"] == "strong"


def test_research_followup_loop_keeps_weak_evidence_in_followup(monkeypatch):
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine DLT",
            status="followup",
            priority=5,
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            metadata={"created_by": "agent_finding_escalation_agent"},
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="agent_eval_sorafenib",
            query_text="sorafenib canine maximum tolerated dose",
            query_params={
                "followup_lane": "agent_evaluator_followup",
                "origin_review_id": str(origin_review_id),
                "origin_agent_run_id": str(origin_agent_run_id),
            },
            track="validation_gap",
        )
    )

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            research_object = ResearchObject(
                object_type="publication",
                title="Comparative oncology framework in dogs and cats",
                abstract="Veterinary comparative oncology review covering canine and feline cancer models.",
                source_key=query.source_key,
                dedupe_key=f"{query.source_key}:{query.query_name}:weak",
            )
            self.repository.research_objects[research_object.id] = research_object
            chunk = DocumentChunk(
                research_object_id=research_object.id,
                chunk_index=0,
                section_label="abstract",
                text_content=(
                    "Dogs and cats can inform comparative oncology, but this review does not report "
                    "the requested drug-specific dosing or safety findings."
                ),
                content_hash=f"{query.source_key}:{query.query_name}:weak",
            )
            self.repository.document_chunks[chunk.id] = chunk
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                raw_records=1,
                research_objects=1,
                document_chunks=1,
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    result = service.run_research_followup_loop(
        ResearchFollowupLoopRequest(
            lead_id=lead.lead_id,
            dry_run=False,
            ingest=True,
            resolve=False,
            evaluate=False,
            operator="operator",
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.document_chunks == 1
    assert result.evidence_fit is not None
    assert result.evidence_fit.fit == "weak"
    assert "sorafenib" in result.evidence_fit.missing_terms
    assert result.lead_status_after == "followup"
    assert [transition["to"] for transition in result.status_transitions] == ["queued", "followup"]
    assert updated.status == "followup"
    assert updated.metadata["research_followup_loop"]["evidence_fit"]["fit"] == "weak"


def test_research_followup_loop_evidence_fit_prefers_run_scoped_chunks(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "followup-run-scoped.sqlite3", seed=False)
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    old_raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="old-strong",
            content_hash="old-strong",
            raw_payload={"title": "Old strong sorafenib evidence"},
        ),
        fetch_run_id=uuid4(),
    )
    old_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Old sorafenib canine maximum tolerated dose study",
            abstract="Sorafenib canine safety maximum tolerated dose and dose-limiting toxicity.",
            source_key="pubmed",
            dedupe_key="old-strong",
        ),
        old_raw_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=old_object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Sorafenib canine maximum tolerated dose and DLT evidence.",
            content_hash="old-strong-chunk",
        )
    )
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine DLT",
            status="followup",
            priority=5,
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            metadata={"created_by": "agent_finding_escalation_agent"},
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="agent_eval_sorafenib",
            query_text="sorafenib canine maximum tolerated dose",
            query_params={
                "followup_lane": "agent_evaluator_followup",
                "origin_review_id": str(origin_review_id),
                "origin_agent_run_id": str(origin_agent_run_id),
            },
            track="validation_gap",
        )
    )

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            fetch_run_id = uuid4()
            raw_id = self.repository.upsert_raw_record(
                RawSourceRecord(
                    source_key=query.source_key,
                    source_record_id="new-weak",
                    content_hash="new-weak",
                    raw_payload={"title": "New broad comparative oncology review"},
                ),
                fetch_run_id=fetch_run_id,
            )
            object_id = self.repository.upsert_research_object(
                ResearchObject(
                    object_type="publication",
                    title="Comparative oncology framework in dogs and cats",
                    abstract="Veterinary comparative oncology review covering canine and feline cancer models.",
                    source_key=query.source_key,
                    dedupe_key="new-weak",
                ),
                raw_id,
            )
            self.repository.upsert_document_chunk(
                DocumentChunk(
                    research_object_id=object_id,
                    chunk_index=0,
                    section_label="abstract",
                    text_content="Dogs and cats can inform comparative oncology broadly.",
                    content_hash="new-weak-chunk",
                )
            )
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=fetch_run_id,
                raw_records=1,
                research_objects=1,
                document_chunks=1,
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    result = service.run_research_followup_loop(
        ResearchFollowupLoopRequest(
            lead_id=lead.lead_id,
            dry_run=False,
            ingest=True,
            resolve=False,
            evaluate=False,
            operator="operator",
        )
    )

    assert result.evidence_fit is not None
    assert result.evidence_fit.fit == "weak"
    assert result.evidence_fit.chunk_count == 1
    assert result.lead_status_after == "followup"


def test_validation_gap_ingest_strips_internal_params_before_api_calls(monkeypatch):
    repo = InMemoryResearchRepository()
    repo.upsert_source_query(
        SourceQuery(
            source_key="openalex",
            query_name="validation_gap_openalex",
            query_text="sorafenib angiosarcoma",
            query_params={
                "lane": "clinical_response",
                "lead_id": "lead-1",
                "queue_item_id": "queue-1",
                "required_terms": ["sorafenib"],
                "source_pack_request": {"persist_queries": True},
                "validation_gap": True,
                "filter": "from_publication_date:2020-01-01",
                "sort": "cited_by_count:desc",
            },
            track="validation_gap",
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="clinicaltrials_gov",
            query_name="validation_gap_trials",
            query_text="angiosarcoma sorafenib",
            query_params={
                "lane": "clinical_response",
                "search_area": "term",
                "validation_gap": True,
            },
            track="validation_gap",
        )
    )
    calls = []

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            calls.append((query.source_key, query.query_params))
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    result = HSAResearchService(repo).ingest_validation_gap_source_queries(
        ValidationGapSourceIngestRequest(
            source_keys=["openalex", "clinicaltrials_gov"],
            dry_run=False,
        )
    )

    assert result.completed_query_count == 2
    assert calls == [
        ("clinicaltrials_gov", {"search_area": "term"}),
        ("openalex", {"filter": "from_publication_date:2020-01-01", "sort": "cited_by_count:desc"}),
    ]


def test_local_ingestion_sanitizes_validation_gap_query_params(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-gap-local-ingest.sqlite3", seed=False)
    calls = []

    class FakeHarvester:
        def fetch(self, query_text, limit=25, **params):
            calls.append((query_text, limit, params))
            return []

    monkeypatch.setattr(local_ingest_module, "get_harvester", lambda source_key: FakeHarvester())

    result = LocalIngestionPipeline(repo).ingest_query(
        SourceQuery(
            source_key="openalex",
            query_name="validation_gap_openalex",
            query_text="sorafenib angiosarcoma",
            query_params={
                "lane": "clinical_response",
                "lead_id": "lead-1",
                "validation_gap": True,
                "filter": "from_publication_date:2020-01-01",
            },
            track="validation_gap",
        ),
        limit=1,
        persist_query=True,
    )
    stored = repo.list_source_queries(source_key="openalex", active_only=True)[0]

    assert result.status == RunStatus.COMPLETED
    assert calls == [("sorafenib angiosarcoma", 1, {"filter": "from_publication_date:2020-01-01"})]
    assert stored.query_params["lane"] == "clinical_response"
    assert stored.query_params["validation_gap"] is True


def test_validation_planning_blocks_when_evaluation_is_not_ready(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-planning-blocked.sqlite3", seed=False)
    brief = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="Uncited VEGF hypothesis",
            disease_scope="canine hemangiosarcoma",
            source_key="pubmed",
            review_mode="deterministic_only",
            final_brief="Uncited synthesis.",
            result_payload={
                "topic": "Uncited VEGF hypothesis",
                "disease_scope": "canine hemangiosarcoma",
                "final_brief": "Uncited synthesis.",
                "citations": [],
                "perspective_reports": [],
                "ranked_hypotheses": [],
                "unresolved_questions": [],
                "evidence": {},
                "errors": [],
            },
        )
    )
    evaluation = HSAResearchService(repo).evaluate_research_brief(
        ResearchBriefEvaluationRequest(brief_id=brief.brief_id)
    )

    result = HSAResearchService(repo).plan_validation(
        ValidationPlanRequest(evaluation_id=evaluation.evaluation_id)
    )

    assert result.status == "blocked"
    assert result.readiness == "needs_better_synthesis"
    assert result.hypothesis_drafts == []
    assert result.tasks[0].task_type == "expert_review"
    assert any("not ready" in error for error in result.errors)


def test_research_brief_queue_runner_persists_brief_and_updates_queue(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-queue-runner.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:queue-brief",
            content_hash="queue-brief-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/queue-brief/",
            raw_payload={"pmid": "queue-brief"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF queue runner evidence in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/queue-brief/",
            dedupe_key="pmid:queue-brief",
            identifiers={"pmid": "queue-brief"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma VEGF therapy evidence includes clinical outcome, "
                "toxicity, translational relevance, biomarker uncertainty, and target selection."
            ),
            content_hash="queue-brief-chunk",
        )
    )
    service = HSAResearchService(repo)
    queued = service.queue_research_brief(
        ResearchBriefQueueRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            source_key="pubmed",
            review_mode="deterministic_only",
            max_claims=0,
            max_chunks_per_perspective=2,
        )
    )

    result = service.run_next_research_brief_queue_item(
        ResearchBriefQueueRunRequest(source_key="pubmed")
    )
    updated = repo.get_research_brief_queue_item(queued.queue_item_id)

    assert result.ran is True
    assert result.brief is not None
    assert result.brief.brief_id is not None
    assert updated is not None
    assert updated.status == "completed"
    assert updated.last_brief_id == result.brief.brief_id
    saved_brief = repo.get_research_brief(result.brief.brief_id)
    assert saved_brief is not None
    assert saved_brief.status == "completed"


def test_research_brief_queue_runner_can_target_explicit_queue_ids(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-queue-runner-targeted.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:queue-brief-targeted",
            content_hash="queue-brief-targeted-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/queue-brief-targeted/",
            raw_payload={"pmid": "queue-brief-targeted"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF targeted queue runner evidence in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/queue-brief-targeted/",
            dedupe_key="pmid:queue-brief-targeted",
            identifiers={"pmid": "queue-brief-targeted"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma VEGF therapy evidence includes clinical outcome, "
                "toxicity, translational relevance, biomarker uncertainty, and target selection."
            ),
            content_hash="queue-brief-targeted-chunk",
        )
    )
    service = HSAResearchService(repo)
    unselected = service.queue_research_brief(
        ResearchBriefQueueRequest(
            topic="KIT therapy in canine mast cell tumor",
            source_key="pubmed",
            priority=1,
            review_mode="deterministic_only",
            max_claims=0,
            max_chunks_per_perspective=2,
        )
    )
    selected = service.queue_research_brief(
        ResearchBriefQueueRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            source_key="pubmed",
            priority=100,
            review_mode="deterministic_only",
            max_claims=0,
            max_chunks_per_perspective=2,
        )
    )

    result = service.run_next_research_brief_queue_item(
        ResearchBriefQueueRunRequest(queue_item_ids=[selected.queue_item_id])
    )
    untouched = repo.get_research_brief_queue_item(unselected.queue_item_id)
    updated = repo.get_research_brief_queue_item(selected.queue_item_id)

    assert result.ran is True
    assert result.queue_item is not None
    assert result.queue_item.queue_item_id == selected.queue_item_id
    assert untouched is not None
    assert untouched.status == "queued"
    assert updated is not None
    assert updated.status == "completed"


def test_research_brief_queue_runner_fails_unusable_brief(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-queue-runner-fail.sqlite3", seed=False)
    service = HSAResearchService(repo)
    queued = service.queue_research_brief(
        ResearchBriefQueueRequest(
            topic="Unbacked linked article lead",
            source_key="x_linked_article",
            review_mode="deterministic_only",
            max_claims=0,
            max_chunks_per_perspective=2,
        )
    )

    result = service.run_next_research_brief_queue_item(
        ResearchBriefQueueRunRequest(source_key="x_linked_article")
    )
    updated = repo.get_research_brief_queue_item(queued.queue_item_id)

    assert result.ran is True
    assert result.brief is not None
    assert result.errors
    assert "did not meet completion bar" in result.errors[0]
    assert updated is not None
    assert updated.status == "failed"
    assert updated.last_brief_id == result.brief.brief_id
    assert updated.last_agent_run_id == result.brief.agent_run_id
    assert updated.last_error is not None
    assert "citations" in updated.last_error
    saved_brief = repo.get_research_brief(result.brief.brief_id)
    assert saved_brief is not None
    assert saved_brief.status == "failed"


def test_research_brief_skeptic_retrieval_prefers_clinical_outcome_evidence(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-skeptic.sqlite3", seed=False)
    weak_raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:mirna",
            content_hash="brief-mirna-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/mirna/",
            raw_payload={"pmid": "mirna"},
        )
    )
    weak_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="miRNA expression profiling in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=weak_raw_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/mirna/",
            dedupe_key="pmid:mirna",
            identifiers={"pmid": "mirna"},
        ),
        weak_raw_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=weak_object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma miRNA expression profiles describe predicted "
                "VEGF pathway targets and biomarker hypotheses."
            ),
            content_hash="brief-mirna-chunk",
        )
    )
    clinical_raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:clinical",
            content_hash="brief-clinical-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/clinical/",
            raw_payload={"pmid": "clinical"},
        )
    )
    clinical_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF inhibitor clinical outcomes in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=clinical_raw_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/clinical/",
            dedupe_key="pmid:clinical",
            identifiers={"pmid": "clinical"},
        ),
        clinical_raw_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=clinical_object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "A canine hemangiosarcoma VEGF VEGFR inhibitor clinical trial reported "
                "response, survival outcome, toxicity, adverse events, and no clear "
                "progression-free benefit."
            ),
            content_hash="brief-clinical-chunk",
        )
    )

    result = HSAResearchService(repo).run_research_brief(
        ResearchBriefRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            review_mode="deterministic_only",
            max_chunks_per_perspective=1,
            max_claims=0,
        )
    )

    skeptic = next(report for report in result.perspective_reports if report.perspective == "skeptic_validation")
    assert skeptic.citations
    assert "clinical outcomes" in (skeptic.citations[0].title or "").lower()
    assert result.evidence["retrieval_strategy"] == "embedding_keyword_blended_perspective_rerank"


def test_research_brief_playground_pack_exports_prompt_contracts(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-playground.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:playground",
            content_hash="brief-playground-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/playground/",
            raw_payload={"pmid": "playground"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF inhibitor evidence in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/playground/",
            dedupe_key="pmid:playground",
            identifiers={"pmid": "playground"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma VEGF VEGFR inhibitor evidence includes "
                "clinical outcome, toxicity, translational relevance, and biomarker limitations."
            ),
            content_hash="brief-playground-chunk",
        )
    )
    repo.upsert_research_lead(
        ResearchLeadRecord(
            title="VEGF hemangiosarcoma institutional article for review",
            url="https://example.edu/vegf-hsa-review",
            lead_type="institutional_article",
            source_key="x_topic_monitor",
            reason="Social monitoring found a credible non-durable source.",
            topic_tags=["hemangiosarcoma", "therapy"],
        )
    )

    pack = HSAResearchService(repo).build_research_brief_playground_pack(
        ResearchBriefRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            review_mode="external_required",
            max_chunks_per_perspective=2,
            max_claims=0,
        )
    )

    assert [prompt.perspective for prompt in pack.prompts] == [
        "evidence_scout",
        "translational_hypothesis",
        "skeptic_validation",
    ]
    assert pack.evidence["mode"] == "manual_playground_prompt_pack"
    assert pack.evidence["retrieval_strategy"] == "embedding_keyword_blended_perspective_rerank"
    assert pack.evidence["research_lead_count"] == 1
    assert pack.prompts[0].response_contract["required"] == [
        "summary",
        "findings",
        "evidence_limitations",
        "errors",
    ]
    assert "EVIDENCE_PAYLOAD_JSON" in pack.prompts[0].user_prompt
    assert "Return JSON only" in pack.prompts[0].user_prompt
    assert pack.prompts[0].prompt_payload["requirements"]["use_only_supplied_citation_ids"] is True
    assert pack.prompts[0].prompt_payload["requirements"]["research_leads_are_watchlist_context_not_citable_evidence"] is True
    assert pack.prompts[0].prompt_payload["research_leads"][0]["lead_type"] == "institutional_article"
    assert pack.prompts[2].prompt_payload["perspective"] == "skeptic_validation"
    assert any("clinical outcomes" in item for item in pack.prompts[2].evaluation_rubric)


def test_pubtator_external_ids_normalize_vocabulary_identifiers():
    assert entity_resolution._pubtator_external_ids(  # noqa: SLF001
        {"infons": {"type": "Gene", "identifier": "NCBI Gene:7157"}}
    ) == {
        "pubtator_identifier": "NCBI Gene:7157",
        "ncbi_gene_id": "7157",
    }
    assert entity_resolution._pubtator_external_ids(  # noqa: SLF001
        {"infons": {"type": "Species", "identifier": "9606"}}
    ) == {
        "pubtator_identifier": "9606",
        "taxonomy_id": "9606",
    }
    assert entity_resolution._pubtator_external_ids(  # noqa: SLF001
        {"infons": {"type": "Disease", "identifier": "MESH:D012878|OMIM:614420"}}
    ) == {
        "pubtator_identifier": "MESH:D012878|OMIM:614420",
        "mesh_id": "D012878",
        "omim_id": "614420",
    }
    assert entity_resolution._pubtator_external_ids(  # noqa: SLF001
        {"infons": {"type": "Chemical", "identifier": "CHEBI:16236"}}
    ) == {
        "pubtator_identifier": "CHEBI:16236",
        "chebi_id": "CHEBI:16236",
    }
    assert normalize_entity_key("compound", "water", {"chebi_id": "CHEBI:16236"}) == "chebi_id:chebi:16236"


def test_pubtator_resolution_uses_external_vocab_ids_for_stable_keys():
    obj = ResearchObject(
        object_type="publication",
        source_key="pubmed",
        identifiers={"pmid": "1"},
    )
    chunk = DocumentChunk(
        research_object_id=obj.id,
        chunk_index=0,
        section_label="abstract",
        text_content="TP53 and Homo sapiens were annotated by PubTator.",
        content_hash="pubtator-entity-chunk",
    )

    mentions = entity_resolution.resolve_chunk_with_pubtator_annotations(
        chunk,
        obj,
        [
            {"text": "TP53", "infons": {"type": "Gene", "identifier": "7157"}},
            {"text": "Homo sapiens", "infons": {"type": "Species", "identifier": "NCBI Taxon:9606"}},
        ],
    )

    assert {mention.normalized_key for mention in mentions} == {
        "ncbi_gene_id:7157",
        "taxonomy_id:9606",
    }
    assert any(mention.external_ids.get("ncbi_gene_id") == "7157" for mention in mentions)
    assert any(mention.external_ids.get("taxonomy_id") == "9606" for mention in mentions)


def test_structured_pipeline_can_report_empty_selection(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)

    report = run_structured_sources_pipeline(repo, source_keys=[], initialize=False)

    assert report["source_keys"] == []
    assert report["sources"] == []
    assert report["totals"] == {
        "raw_records": 0,
        "research_objects": 0,
        "document_chunks": 0,
        "entity_mentions": 0,
        "claims": 0,
    }
    assert report["errors"] == []


def test_openalex_v2_normalizer_produces_raw_and_research_object():
    harvester = OpenAlexHarvesterV2()

    record = harvester.normalize(
        {
            "id": "https://openalex.org/W123",
            "doi": "https://doi.org/10.1234/example",
            "title": "Canine hemangiosarcoma example",
            "publication_year": 2026,
            "publication_date": "2026-01-02",
            "abstract_inverted_index": {"Canine": [0], "HSA": [1]},
            "ids": {"pmid": "https://pubmed.ncbi.nlm.nih.gov/123"},
            "primary_location": {
                "landing_page_url": "https://doi.org/10.1234/example",
                "source": {"display_name": "Example Journal"},
            },
        }
    )

    assert record.raw_record.source_key == "openalex"
    assert record.research_object.title == "Canine hemangiosarcoma example"
    assert record.research_object.identifiers["doi"] == "10.1234/example"
    assert record.research_object.abstract == "Canine HSA"
    assert record.research_object.metadata["harvester"] == "v2"


def test_unpaywall_v2_normalizer_preserves_oa_location_metadata():
    record = UnpaywallHarvesterV2().normalize(
        {
            "score": 0.42,
            "snippet": "<b>Angiosarcoma</b> open access title match",
            "response": {
                "doi": "https://doi.org/10.1234/HSA.OA",
                "doi_url": "https://doi.org/10.1234/HSA.OA",
                "title": "Human angiosarcoma open access review",
                "year": 2026,
                "published_date": "2026-04-01",
                "publisher": "Example Publisher",
                "journal_name": "Example Journal",
                "genre": "journal-article",
                "is_oa": True,
                "oa_status": "gold",
                "journal_is_in_doaj": True,
                "best_oa_location": {
                    "url_for_landing_page": "https://example.org/article",
                    "url_for_pdf": "https://example.org/article.pdf",
                    "license": "cc-by",
                    "host_type": "publisher",
                    "version": "publishedVersion",
                },
                "oa_locations": [
                    {
                        "url_for_landing_page": "https://example.org/article",
                        "license": "cc-by",
                        "host_type": "publisher",
                    }
                ],
                "z_authors": [{"given": "Ada", "family": "Lovelace"}],
            },
        }
    )

    assert record.raw_record.source_key == "unpaywall"
    assert record.research_object.identifiers["doi"] == "10.1234/hsa.oa"
    assert record.research_object.canonical_url == "https://example.org/article"
    assert record.research_object.metadata["best_oa_location"]["url_for_pdf"] == "https://example.org/article.pdf"
    assert record.research_object.metadata["authors"] == ["Ada Lovelace"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["human_angiosarcoma"]
    assert UnpaywallHarvesterV2().chunk_section_label(record) == "oa_discovery_metadata"


def test_unpaywall_v2_fetch_uses_title_search_endpoint(monkeypatch):
    calls = []

    def fake_get_json(url, params, **kwargs):
        calls.append((url, params, kwargs))
        return {
            "results": [
                {
                    "score": 1.0,
                    "response": {
                        "doi": "10.1234/example",
                        "title": "Canine hemangiosarcoma open access paper",
                        "is_oa": True,
                        "best_oa_location": {"url_for_landing_page": "https://example.org/paper"},
                    },
                }
            ]
        }

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    records = UnpaywallHarvesterV2().fetch(
        '"canine hemangiosarcoma"',
        limit=1,
        email="contact@example.org",
        is_oa=True,
    )

    assert len(records) == 1
    assert calls == [
        (
            "https://api.unpaywall.org/v2/search/",
            {"query": '"canine hemangiosarcoma"', "is_oa": "true", "email": "contact@example.org"},
            {"timeout_seconds": harvesters_v2.DEFAULT_REQUEST_TIMEOUT_SECONDS, "attempts": harvesters_v2.DEFAULT_REQUEST_ATTEMPTS},
        )
    ]


def test_unpaywall_v2_fetch_uses_doi_endpoint_for_doi_queries(monkeypatch):
    calls = []

    def fake_get_json(url, params, **kwargs):
        calls.append((url, params, kwargs))
        return {
            "doi": "10.1234/example",
            "title": "Human angiosarcoma open access DOI record",
            "is_oa": True,
            "best_oa_location": {"url_for_landing_page": "https://example.org/doi-record"},
        }

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    records = UnpaywallHarvesterV2().fetch(
        "https://doi.org/10.1234/example",
        limit=1,
        email="contact@example.org",
    )

    assert len(records) == 1
    assert records[0].research_object.identifiers["doi"] == "10.1234/example"
    assert calls == [
        (
            "https://api.unpaywall.org/v2/10.1234%2Fexample",
            {"email": "contact@example.org"},
            {"timeout_seconds": harvesters_v2.DEFAULT_REQUEST_TIMEOUT_SECONDS, "attempts": harvesters_v2.DEFAULT_REQUEST_ATTEMPTS},
        )
    ]


def test_scholarly_query_policy_always_includes_human_angiosarcoma():
    queries = build_scholarly_source_queries()
    pubmed_query = next(query for query in queries if query.source_key == "pubmed" and query.query_name == "comparative_hsa_required")
    pmc_query = next(query for query in queries if query.source_key == "pmc_oa")
    unpaywall_query = next(query for query in queries if query.source_key == "unpaywall")

    assert queries
    assert all("angiosarcoma" in query.query_text.lower() for query in queries)
    assert all("hemangiosarcoma" in query.query_text.lower() for query in queries)
    assert "angiosarcoma[tiab]" in pubmed_query.query_text
    assert "[tiab]" in pmc_query.query_text
    assert "comparative oncology" not in pmc_query.query_text.lower()
    assert unpaywall_query.query_params == {"is_oa": True}
    assert unpaywall_query.active is True


def test_comparative_scope_does_not_match_angiosarcoma_inside_hemangiosarcoma():
    policy = infer_comparative_scope(
        "Canine hemangiosarcoma angiogenesis",
        "Canine hemangiosarcoma studies discuss VEGF.",
    )

    assert policy["matched_concepts"] == ["canine_hsa"]


def test_pubmed_v2_normalizer_handles_nested_xml_text():
    article = ET.fromstring(
        """
        <PubmedArticle>
          <MedlineCitation>
            <PMID>123</PMID>
            <Article>
              <ArticleTitle>Canine <i>hemangiosarcoma</i> and human angiosarcoma</ArticleTitle>
              <Abstract>
                <AbstractText>Human <b>angiosarcoma</b> analog evidence.</AbstractText>
              </Abstract>
              <Journal>
                <Title>Example Journal</Title>
                <JournalIssue><PubDate><Year>2026</Year></PubDate></JournalIssue>
              </Journal>
            </Article>
          </MedlineCitation>
        </PubmedArticle>
        """
    )

    record = PubMedHarvesterV2().normalize(article)

    assert record.research_object.title == "Canine hemangiosarcoma and human angiosarcoma"
    assert record.research_object.abstract == "Human angiosarcoma analog evidence."
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == [
        "canine_hsa",
        "human_angiosarcoma",
    ]


def test_pubmed_v2_normalizer_uses_only_current_article_identifiers():
    article = ET.fromstring(
        """
        <PubmedArticle>
          <MedlineCitation>
            <PMID>36548371</PMID>
            <Article>
              <ArticleTitle>Pilot safety evaluation for canine splenic hemangiosarcoma</ArticleTitle>
              <Abstract>
                <AbstractText>Canine hemangiosarcoma immunotherapy safety study.</AbstractText>
              </Abstract>
              <Journal>
                <Title>PLOS One</Title>
                <JournalIssue><PubDate><Year>2022</Year></PubDate></JournalIssue>
              </Journal>
            </Article>
          </MedlineCitation>
          <PubmedData>
            <ArticleIdList>
              <ArticleId IdType="pubmed">36548371</ArticleId>
              <ArticleId IdType="doi">10.1371/journal.pone.0279594</ArticleId>
              <ArticleId IdType="pmc">PMC9778498</ArticleId>
            </ArticleIdList>
            <ReferenceList>
              <Reference>
                <ArticleIdList>
                  <ArticleId IdType="pubmed">20977336</ArticleId>
                  <ArticleId IdType="doi">10.1208/s12249-010-9526-5</ArticleId>
                  <ArticleId IdType="pmc">PMC3011075</ArticleId>
                </ArticleIdList>
              </Reference>
            </ReferenceList>
          </PubmedData>
        </PubmedArticle>
        """
    )

    record = PubMedHarvesterV2().normalize(article)

    assert record.research_object.identifiers["pmid"] == "36548371"
    assert record.research_object.identifiers["doi"] == "10.1371/journal.pone.0279594"
    assert record.research_object.identifiers["pmcid"] == "PMC9778498"


def test_europe_pmc_v2_normalizer_cleans_escaped_title_markup():
    record = EuropePMCHarvesterV2().normalize(
        {
            "id": "x1",
            "title": "Primary &lt;i&gt;Vaginal&lt;/i&gt; Angiosarcoma",
            "abstractText": "Human angiosarcoma case report.",
            "pubYear": "2026",
        }
    )

    assert record.research_object.title == "Primary Vaginal Angiosarcoma"
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["human_angiosarcoma"]


def test_europe_pmc_v2_normalizer_can_store_licensed_full_text():
    record = EuropePMCHarvesterV2().normalize(
        {
            "id": "PMC123",
            "pmcid": "PMC123",
            "title": "Endothelial biology review",
            "abstractText": "Sparse abstract.",
            "isOpenAccess": "Y",
        },
        full_text_xml="""
        <article xmlns="http://jats.nlm.nih.gov">
          <front>
            <article-meta>
              <article-id pub-id-type="pmc">PMC123</article-id>
            </article-meta>
          </front>
          <body>
            <sec>
              <title>Results</title>
              <p>Human angiosarcoma full text mentions VEGF and propranolol.</p>
            </sec>
          </body>
        </article>
        """,
    )
    harvester = EuropePMCHarvesterV2()

    assert record.raw_record.raw_payload["full_text"] == "Results Human angiosarcoma full text mentions VEGF and propranolol."
    assert record.raw_record.raw_payload["full_text_sections"] == [
        {
            "section_label": "full_text:results",
            "title": "Results",
            "text": "Results Human angiosarcoma full text mentions VEGF and propranolol.",
        }
    ]
    assert record.research_object.metadata["full_text_available"] is True
    assert record.research_object.metadata["body_only_match"] is True
    assert record.research_object.metadata["body_ingestion_policy"]["matched_concepts"] == ["human_angiosarcoma"]
    assert harvester.chunk_section_label(record) == "full_text"
    assert "full text mentions VEGF" in harvester.text_for_chunking(record)
    sections = harvester.chunk_text_sections(record)
    assert [section_label for section_label, _text in sections] == ["title_abstract", "full_text:results"]
    assert "Sparse abstract" in sections[0][1]
    assert "Sparse abstract" not in sections[1][1]
    assert "full text mentions VEGF" in sections[1][1]


def test_europe_pmc_v2_fetch_keeps_body_only_policy_match(monkeypatch):
    def fake_get_json(url, params):
        assert url.endswith("/search")
        assert params["resultType"] == "core"
        return {
            "resultList": {
                "result": [
                    {
                        "id": "PMC123",
                        "pmcid": "PMC123",
                        "title": "Endothelial biology review",
                        "abstractText": "Sparse abstract.",
                        "isOpenAccess": "Y",
                    }
                ]
            }
        }

    def fake_get_text(url, params, **kwargs):
        assert url == "https://www.ebi.ac.uk/europepmc/webservices/rest/PMC123/fullTextXML"
        assert params == {}
        assert kwargs["timeout_seconds"] == harvesters_v2.FULL_TEXT_REQUEST_TIMEOUT_SECONDS
        assert kwargs["attempts"] == harvesters_v2.FULL_TEXT_REQUEST_ATTEMPTS
        return """
        <article xmlns="http://jats.nlm.nih.gov">
          <front><article-meta><article-id pub-id-type="pmc">PMC123</article-id></article-meta></front>
          <body><p>Canine hemangiosarcoma full text mentions VEGF and propranolol.</p></body>
        </article>
        """

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)
    monkeypatch.setattr(harvesters_v2, "_get_text", fake_get_text)

    records = EuropePMCHarvesterV2().fetch("hemangiosarcoma", limit=1, open_access=True, require_policy_match=True)

    assert len(records) == 1
    assert records[0].research_object.metadata["body_only_match"] is True
    assert records[0].research_object.metadata["body_ingestion_policy"]["matched_concepts"] == ["canine_hsa"]


def test_europe_pmc_v2_fetch_applies_publication_date_range(monkeypatch):
    def fake_get_json(url, params):
        assert url.endswith("/search")
        assert "FIRST_PDATE:[2026-04-27 TO 2026-04-27]" in params["query"]
        return {"resultList": {"result": []}}

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    records = EuropePMCHarvesterV2().fetch(
        "hemangiosarcoma",
        limit=1,
        open_access=True,
        fetch_full_text=False,
        require_policy_match=False,
        published_after="2026-04-27",
        published_before="2026-04-27",
    )

    assert records == []


def test_pmc_oa_v2_normalizer_extracts_license_and_full_text():
    xml = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <GetRecord>
        <record>
          <metadata>
            <article xmlns="http://jats.nlm.nih.gov" xmlns:xlink="http://www.w3.org/1999/xlink" article-type="research-article">
              <front>
                <journal-meta>
                  <journal-title-group><journal-title>Example Journal</journal-title></journal-title-group>
                </journal-meta>
                <article-meta>
                  <article-id pub-id-type="pmid">12345</article-id>
                  <article-id pub-id-type="pmc">PMC999999</article-id>
                  <article-id pub-id-type="doi">10.1234/PMC.TEST</article-id>
                  <title-group>
                    <article-title>Canine <italic>hemangiosarcoma</italic> and human angiosarcoma</article-title>
                  </title-group>
                  <pub-date pub-type="epub"><year>2026</year><month>04</month><day>01</day></pub-date>
                  <permissions>
                    <license license-type="open-access" xlink:href="https://creativecommons.org/licenses/by/4.0/">
                      <license-p>Creative Commons Attribution License</license-p>
                    </license>
                  </permissions>
                  <abstract><p>Human angiosarcoma analog evidence.</p></abstract>
                </article-meta>
              </front>
              <body>
                <sec>
                  <title>Results</title>
                  <p>Canine hemangiosarcoma full text mentions VEGF and propranolol.</p>
                </sec>
              </body>
            </article>
          </metadata>
        </record>
      </GetRecord>
    </OAI-PMH>
    """

    record = PMCOAHarvesterV2().normalize(
        xml,
        oa_metadata={"oa_license": "CC BY", "links": [{"format": "tgz", "href": "ftp://example.test/a.tgz"}]},
        source_query="hemangiosarcoma",
    )

    assert record.raw_record.source_key == "pmc_oa"
    assert record.raw_record.raw_payload["full_text"] == "Results Canine hemangiosarcoma full text mentions VEGF and propranolol."
    assert record.raw_record.raw_payload["full_text_sections"] == [
        {
            "section_label": "full_text:results",
            "title": "Results",
            "text": "Results Canine hemangiosarcoma full text mentions VEGF and propranolol.",
        }
    ]
    assert record.research_object.identifiers["pmcid"] == "PMC999999"
    assert record.research_object.identifiers["doi"] == "10.1234/pmc.test"
    assert record.research_object.metadata["journal"] == "Example Journal"
    assert record.research_object.metadata["license"]["oa_license"] == "CC BY"
    assert record.research_object.metadata["license"]["jats_license_url"] == "https://creativecommons.org/licenses/by/4.0/"
    assert record.research_object.metadata["full_text_available"] is True
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == [
        "canine_hsa",
        "human_angiosarcoma",
    ]


def test_pmc_oa_v2_normalizer_splits_jats_body_sections():
    record = PMCOAHarvesterV2().normalize(
        """
        <article xmlns="http://jats.nlm.nih.gov">
          <front>
            <article-meta>
              <article-id pub-id-type="pmc">PMC3</article-id>
              <title-group><article-title>Canine hemangiosarcoma</article-title></title-group>
              <abstract><p>Short abstract.</p></abstract>
            </article-meta>
          </front>
          <body>
            <sec>
              <title>Materials and Methods</title>
              <p>Cells were profiled with a canine hemangiosarcoma assay.</p>
            </sec>
            <sec>
              <title>Results</title>
              <p>Human angiosarcoma VEGF signaling was compared.</p>
            </sec>
            <sec>
              <title>References</title>
              <p>Reference text should not become a body chunk.</p>
            </sec>
          </body>
        </article>
        """,
        oa_metadata={"oa_license": "CC BY"},
    )

    sections = PMCOAHarvesterV2().chunk_text_sections(record)

    assert [section_label for section_label, _text in sections] == [
        "title_abstract",
        "full_text:methods",
        "full_text:results",
    ]
    assert "canine hemangiosarcoma assay" in sections[1][1]
    assert "VEGF signaling" in sections[2][1]
    assert all("Reference text" not in text for _section_label, text in sections)


def test_pmc_oa_v2_normalizer_preserves_nested_jats_sections():
    record = PMCOAHarvesterV2().normalize(
        """
        <article xmlns="http://jats.nlm.nih.gov">
          <front>
            <article-meta>
              <article-id pub-id-type="pmc">PMC33</article-id>
              <title-group><article-title>Canine hemangiosarcoma nested sections</article-title></title-group>
              <abstract><p>Short abstract.</p></abstract>
            </article-meta>
          </front>
          <body>
            <sec>
              <title>Results</title>
              <p>Human angiosarcoma comparison was summarized.</p>
              <sec>
                <title>VEGF Signaling</title>
                <p>Nested VEGF signaling evidence should become its own searchable section.</p>
              </sec>
            </sec>
            <sec>
              <title>References</title>
              <sec>
                <title>Ignored Nested Reference</title>
                <p>Nested reference text should not become a body chunk.</p>
              </sec>
            </sec>
          </body>
        </article>
        """,
        oa_metadata={"oa_license": "CC BY"},
    )

    sections = PMCOAHarvesterV2().chunk_text_sections(record)

    assert [section_label for section_label, _text in sections] == [
        "title_abstract",
        "full_text:results",
        "full_text:vegf_signaling",
    ]
    assert "Human angiosarcoma comparison" in sections[1][1]
    assert "Nested VEGF signaling evidence" not in sections[1][1]
    assert "Nested VEGF signaling evidence" in sections[2][1]
    assert all("Nested reference text" not in text for _section_label, text in sections)


def test_pmc_oa_v2_chunks_full_text_not_just_abstract():
    record = PMCOAHarvesterV2().normalize(
        """
        <article xmlns="http://jats.nlm.nih.gov">
          <front>
            <article-meta>
              <article-id pub-id-type="pmc">PMC1</article-id>
              <title-group><article-title>Canine hemangiosarcoma</article-title></title-group>
              <abstract><p>Short abstract.</p></abstract>
            </article-meta>
          </front>
          <body><p>Full text body with human angiosarcoma comparative evidence.</p></body>
        </article>
        """,
        oa_metadata={"oa_license": "CC BY"},
    )
    harvester = PMCOAHarvesterV2()

    assert harvester.chunk_section_label(record) == "full_text"
    assert "Full text body" in harvester.text_for_chunking(record)
    sections = harvester.chunk_text_sections(record)
    assert [section_label for section_label, _text in sections] == ["title_abstract", "full_text"]
    assert "Short abstract" in sections[0][1]
    assert "Short abstract" not in sections[1][1]
    assert sections[1][1] == "Full text body with human angiosarcoma comparative evidence."


def test_pmc_oa_v2_does_not_label_abstract_only_records_as_full_text():
    record = PMCOAHarvesterV2().normalize(
        """
        <article xmlns="http://jats.nlm.nih.gov">
          <front>
            <article-meta>
              <article-id pub-id-type="pmc">PMC2</article-id>
              <title-group><article-title>Canine hemangiosarcoma</article-title></title-group>
              <abstract><p>Short abstract.</p></abstract>
            </article-meta>
          </front>
        </article>
        """,
        oa_metadata={"oa_license": "CC BY"},
    )
    harvester = PMCOAHarvesterV2()

    assert record.research_object.metadata["full_text_available"] is False
    assert harvester.chunk_section_label(record) == "title_abstract"
    assert harvester.chunk_text_sections(record) == [("title_abstract", "Canine hemangiosarcoma\n\nShort abstract.")]


def test_pmc_oa_v2_fetch_keeps_body_only_policy_match(monkeypatch):
    xml = """
    <article xmlns="http://jats.nlm.nih.gov">
      <front>
        <article-meta>
          <article-id pub-id-type="pmc">PMC123456</article-id>
          <title-group><article-title>Open access endothelial biology review</article-title></title-group>
          <permissions>
            <license license-type="open-access">
              <license-p>Creative Commons Attribution License</license-p>
            </license>
          </permissions>
        </article-meta>
      </front>
      <body><p>Human angiosarcoma full text mentions VEGF and propranolol.</p></body>
    </article>
    """

    def fake_get_json(url, params):
        assert url.endswith("/esearch.fcgi")
        assert params["db"] == "pmc"
        return {"esearchresult": {"idlist": ["123456"]}}

    def fake_get_text(url, params, **kwargs):
        assert url == "https://pmc.ncbi.nlm.nih.gov/api/oai/v1/mh/"
        assert params["identifier"] == "oai:pubmedcentral.nih.gov:123456"
        assert kwargs["timeout_seconds"] == harvesters_v2.FULL_TEXT_REQUEST_TIMEOUT_SECONDS
        assert kwargs["attempts"] == harvesters_v2.FULL_TEXT_REQUEST_ATTEMPTS
        return xml

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)
    monkeypatch.setattr(harvesters_v2, "_get_text", fake_get_text)
    monkeypatch.setattr(
        harvesters_v2,
        "_pmc_oa_metadata",
        lambda pmcid, **kwargs: {"oa_license": "CC BY", "retracted": "no"},
    )
    monkeypatch.setattr(harvesters_v2.time, "sleep", lambda _seconds: None)

    records = PMCOAHarvesterV2().fetch("hemangiosarcoma", limit=1, require_policy_match=True)

    assert len(records) == 1
    assert records[0].research_object.metadata["body_only_match"] is True
    assert records[0].research_object.metadata["body_ingestion_policy"]["matched_concepts"] == [
        "human_angiosarcoma"
    ]


def test_pmc_oa_v2_fetch_caps_candidate_metadata_scans(monkeypatch):
    metadata_calls = []

    def fake_get_json(url, params):
        assert url.endswith("/esearch.fcgi")
        assert params["retmax"] == 4
        return {"esearchresult": {"idlist": [str(index) for index in range(1, 20)]}}

    def fake_metadata(pmcid, **kwargs):
        metadata_calls.append((pmcid, kwargs))
        return None

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)
    monkeypatch.setattr(harvesters_v2, "_pmc_oa_metadata", fake_metadata)

    records = PMCOAHarvesterV2().fetch(
        "hemangiosarcoma",
        limit=3,
        max_candidate_records=4,
    )

    assert records == []
    assert [pmcid for pmcid, _kwargs in metadata_calls] == ["PMC1", "PMC2", "PMC3", "PMC4"]
    assert all(
        kwargs["timeout_seconds"] == harvesters_v2.FULL_TEXT_REQUEST_TIMEOUT_SECONDS
        and kwargs["attempts"] == harvesters_v2.FULL_TEXT_REQUEST_ATTEMPTS
        for _pmcid, kwargs in metadata_calls
    )


def test_pmc_oa_v2_fetch_applies_publication_date_params(monkeypatch):
    def fake_get_json(url, params):
        assert url.endswith("/esearch.fcgi")
        assert params["datetype"] == "pdat"
        assert params["mindate"] == "2026/04/27"
        assert params["maxdate"] == "2026/04/27"
        return {"esearchresult": {"idlist": []}}

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    records = PMCOAHarvesterV2().fetch(
        "hemangiosarcoma",
        limit=1,
        published_after="2026-04-27",
        published_before="2026-04-27",
    )

    assert records == []


def test_local_ingestion_preserves_full_text_chunks_when_duplicate_metadata_arrives(tmp_path, monkeypatch):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    pipeline = LocalIngestionPipeline(repo)
    query = SourceQuery(
        source_key="europe_pmc",
        query_name="licensed_full_text_test",
        query_text="hemangiosarcoma",
    )
    full_text_record = EuropePMCHarvesterV2().normalize(
        {
            "id": "PMC123",
            "pmcid": "PMC123",
            "title": "Endothelial biology review",
            "abstractText": "Sparse abstract.",
            "isOpenAccess": "Y",
        },
        full_text_xml="""
        <article xmlns="http://jats.nlm.nih.gov">
          <body><p>Canine hemangiosarcoma full text body mentions VEGF and propranolol.</p></body>
        </article>
        """,
    )
    abstract_only_record = EuropePMCHarvesterV2().normalize(
        {
            "id": "PMC123",
            "pmcid": "PMC123",
            "title": "Endothelial biology review",
            "abstractText": "Sparse abstract.",
            "isOpenAccess": "Y",
        }
    )

    class FakeEuropePMCHarvester(EuropePMCHarvesterV2):
        records = [full_text_record]

        def fetch(self, query_text, limit=25, **params):
            return self.records

    monkeypatch.setitem(harvesters_v2.HARVESTERS_V2, "europe_pmc", FakeEuropePMCHarvester)

    result = pipeline.ingest_query(query, limit=1)
    chunks = repo.list_document_chunks(source_key="europe_pmc")

    assert result.document_chunks == 2
    assert result.full_text_research_objects == 1
    assert result.section_chunk_counts == {"title_abstract": 1, "full_text": 1}
    assert [chunk.section_label for chunk in chunks] == ["title_abstract", "full_text"]
    assert "Sparse abstract" not in chunks[1].text_content

    FakeEuropePMCHarvester.records = [abstract_only_record]
    refreshed = pipeline.ingest_query(query, limit=1)
    refreshed_chunks = repo.list_document_chunks(source_key="europe_pmc")
    refreshed_object = repo.get_research_object(full_text_record.research_object.id)

    assert refreshed.document_chunks == 0
    assert refreshed.full_text_research_objects == 0
    assert refreshed.section_chunk_counts == {}
    assert refreshed_object is not None
    assert refreshed_object.metadata["full_text_available"] is True
    assert [chunk.section_label for chunk in refreshed_chunks] == ["title_abstract", "full_text"]


def test_pmc_oa_v2_is_registered_harvester():
    assert HARVESTERS_V2["pmc_oa"] is PMCOAHarvesterV2


def test_hosted_literature_smoke_includes_pmc_oa():
    assert "pmc_oa" in LITERATURE_CLINICAL_SMOKE_KEYS
    assert "pmc_oa" in HOSTED_API_REPORT_KEYS


def test_unpaywall_is_registered_for_manual_oa_discovery():
    assert HARVESTERS_V2["unpaywall"] is UnpaywallHarvesterV2
    assert "unpaywall" not in HOSTED_API_REPORT_KEYS


def test_literature_corpus_harvest_targets_hundreds_of_papers():
    assert LITERATURE_CORPUS_SOURCE_KEYS == (
        "openalex",
        "pubmed",
        "crossref",
    )
    assert sum(LITERATURE_CORPUS_SOURCE_LIMITS.values()) >= 300
    assert set(LITERATURE_CORPUS_SOURCE_KEYS).isdisjoint(LITERATURE_FULL_TEXT_SOURCE_KEYS)


def test_full_text_refresh_keeps_heavy_sources_bounded():
    assert LITERATURE_FULL_TEXT_SOURCE_KEYS == ("europe_pmc", "pmc_oa")
    assert LITERATURE_FULL_TEXT_SOURCE_LIMITS["europe_pmc"] <= 10
    assert LITERATURE_FULL_TEXT_SOURCE_LIMITS["pmc_oa"] <= 3


def test_all_api_smoke_covers_every_hosted_report_source():
    assert ALL_API_SMOKE_KEYS == HOSTED_API_REPORT_KEYS
    assert set(ALL_API_SMOKE_KEYS) == {
        "pubchem",
        "chembl",
        "uniprot",
        "rcsb_pdb",
        "openfda_animal_events",
        "icdc",
        "geo",
        "sra",
        "openalex",
        "pubmed",
        "europe_pmc",
        "crossref",
        "pmc_oa",
        "clinicaltrials_gov",
    }


def test_x_topic_monitor_builds_official_api_request_only():
    request = x_topic_monitor.build_recent_search_request(
        x_topic_monitor.XTopicRequest(
            query='"canine hemangiosarcoma"',
            query_name="x_disease_monitoring",
            max_results=10,
        )
    )

    assert request.method == "GET"
    assert request.url.startswith("https://api.x.com/2/tweets/search/recent?")
    assert request.params["query"] == '"canine hemangiosarcoma" lang:en -is:retweet'
    assert request.headers["Authorization"] == "Bearer <X_BEARER_TOKEN>"
    assert request.billable is True
    assert any("Official X API" in note for note in request.notes)


def test_x_topic_monitor_builds_twitterapi_io_request():
    request = x_topic_monitor.build_twitterapi_io_search_request(
        x_topic_monitor.XTopicRequest(
            query='"canine hemangiosarcoma"',
            query_name="x_disease_monitoring",
            max_results=10,
        )
    )

    assert request.method == "GET"
    assert request.url.startswith("https://api.twitterapi.io/twitter/tweet/advanced_search?")
    assert request.params["query"] == '"canine hemangiosarcoma" lang:en -filter:retweets'
    assert request.params["queryType"] == "Latest"
    assert request.headers["x-api-key"] == "<TWITTERAPI_IO_KEY>"
    assert request.billable is True


def test_twitterapi_io_provider_searches_and_normalizes_candidates():
    calls = []

    def fake_transport(url, params, headers, timeout_seconds):
        calls.append((url, params, headers, timeout_seconds))
        return {
            "tweets": [
                {
                    "id": "123",
                    "url": "https://x.com/vetonc/status/123",
                    "text": "New canine hemangiosarcoma trial links to PubMed.",
                    "createdAt": "2026-04-28T10:00:00Z",
                    "lang": "en",
                    "conversationId": "789",
                    "retweetCount": 1,
                    "replyCount": 2,
                    "likeCount": 3,
                    "quoteCount": 4,
                    "viewCount": 500,
                    "author": {"id": "456", "userName": "vetonc", "name": "Vet Onc"},
                    "entities": {
                        "urls": [
                            {"expanded_url": "https://pubmed.ncbi.nlm.nih.gov/123456/"},
                        ]
                    },
                }
            ],
            "has_next_page": False,
            "next_cursor": "",
        }

    result = x_topic_monitor.TwitterApiIoProvider(
        api_key="test-key",
        transport=fake_transport,
        timeout_seconds=12.0,
    ).search(
        x_topic_monitor.XTopicRequest(
            query='"canine hemangiosarcoma"',
            query_name="x_trial_monitoring",
            max_results=10,
        )
    )

    assert calls == [
        (
            "https://api.twitterapi.io/twitter/tweet/advanced_search",
            {"query": '"canine hemangiosarcoma" lang:en -filter:retweets', "queryType": "Latest", "cursor": ""},
            {"x-api-key": "test-key"},
            12.0,
        )
    ]
    assert result.provider == "twitterapi_io"
    assert result.raw_tweet_count == 1
    assert len(result.candidates) == 1
    assert result.candidates[0].canonical_url == "https://x.com/vetonc/status/123"
    assert result.candidates[0].username == "vetonc"
    assert result.candidates[0].durable_links == ["https://pubmed.ncbi.nlm.nih.gov/123456/"]
    assert result.candidates[0].metadata["provider_payload"]["provider"] == "twitterapi_io"


def test_twitterapi_io_provider_requires_key(monkeypatch):
    monkeypatch.delenv("TWITTERAPI_IO_KEY", raising=False)

    with pytest.raises(ValueError):
        x_topic_monitor.TwitterApiIoProvider().search(
            x_topic_monitor.XTopicRequest(query='"canine hemangiosarcoma"')
        )


def test_dagster_x_topic_monitor_review_asset_uses_twitterapi_io(monkeypatch):
    calls = []
    repo = InMemoryResearchRepository()

    class FakeTwitterApiIoProvider:
        def search(self, request):
            calls.append(request.query_name)
            assert request.max_results == 10
            return x_topic_monitor.XTopicProviderResult(
                provider="twitterapi_io",
                query_name=request.query_name,
                raw_tweet_count=2,
                candidates=[
                    x_topic_monitor.XTopicReviewCandidate(
                        source_record_id="123",
                        canonical_url="https://x.com/vetonc/status/123",
                        username="vetonc",
                        matched_query_name=request.query_name,
                        matched_terms=["canine hemangiosarcoma", "trial"],
                        durable_links=["https://pubmed.ncbi.nlm.nih.gov/123456/"],
                        quality_score=0.7,
                    )
                ],
            )

    class FakeRepositoryResource:
        def build_repository(self):
            return repo

    monkeypatch.setenv("HSA_X_TOPIC_QUERY_NAME", "x_trial_monitoring")
    monkeypatch.setenv("HSA_X_TOPIC_MAX_RESULTS", "10")
    monkeypatch.setenv("HSA_X_TOPIC_REVIEW_MODE", "deterministic_only")
    monkeypatch.setattr(x_topic_monitor, "TwitterApiIoProvider", FakeTwitterApiIoProvider)

    result = dagster_asset_module.x_topic_monitor_review_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == ["x_trial_monitoring"]
    assert result.value["provider"] == "twitterapi_io"
    assert result.value["raw_tweet_count"] == 2
    assert result.value["candidate_count"] == 1
    assert result.value["candidates"][0]["post_id"] == "123"
    assert result.value["agent_review"]["ingestion_candidate_count"] == 1
    assert result.value["agent_review"]["actions"][0]["action"] == "flag_for_ingestion"
    assert result.value["manual_review_required"] is True
    assert repo.list_agent_runs(agent_name="x_topic_review_agent", status="completed")
    assert dagster_asset_module.x_topic_monitor_review_job is not None


def test_dagster_x_topic_monitor_review_asset_paces_twitterapi_io_queries(monkeypatch):
    calls = []
    sleeps = []
    repo = InMemoryResearchRepository()
    queries = [
        SourceQuery(
            source_key=x_topic_monitor.X_TOPIC_SOURCE_KEY,
            query_name="first_query",
            query_text='"canine hemangiosarcoma"',
            object_type=ResearchObjectType.KNOWLEDGE_ENTRY,
        ),
        SourceQuery(
            source_key=x_topic_monitor.X_TOPIC_SOURCE_KEY,
            query_name="second_query",
            query_text='"angiosarcoma" "dog"',
            object_type=ResearchObjectType.KNOWLEDGE_ENTRY,
        ),
    ]

    class FakeTwitterApiIoProvider:
        def search(self, request):
            calls.append(request.query_name)
            return x_topic_monitor.XTopicProviderResult(
                provider="twitterapi_io",
                query_name=request.query_name,
                raw_tweet_count=0,
                candidates=[],
            )

    class FakeRepositoryResource:
        def build_repository(self):
            return repo

    monkeypatch.delenv("HSA_X_TOPIC_QUERY_NAME", raising=False)
    monkeypatch.setenv("HSA_X_TOPIC_MAX_RESULTS", "10")
    monkeypatch.setenv("HSA_X_TOPIC_QUERY_DELAY_SECONDS", "0.25")
    monkeypatch.setenv("HSA_X_TOPIC_REVIEW_MODE", "deterministic_only")
    monkeypatch.setattr(x_topic_monitor, "build_default_source_queries", lambda: queries)
    monkeypatch.setattr(x_topic_monitor, "TwitterApiIoProvider", FakeTwitterApiIoProvider)
    monkeypatch.setattr(dagster_asset_module.time, "sleep", lambda seconds: sleeps.append(seconds))

    result = dagster_asset_module.x_topic_monitor_review_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == ["first_query", "second_query"]
    assert sleeps == [0.25]
    assert result.value["query_delay_seconds"] == 0.25
    assert result.value["raw_tweet_count"] == 0


def test_x_topic_monitor_normalizes_post_for_manual_review():
    candidate = x_topic_monitor.normalize_post_payload(
        {
            "id": "123",
            "author_id": "456",
            "author": {"username": "vetonc"},
            "conversation_id": "789",
            "created_at": "2026-04-28T10:00:00Z",
            "lang": "en",
            "text": "New canine hemangiosarcoma trial links to PubMed.",
            "entities": {
                "urls": [
                    {"expanded_url": "https://pubmed.ncbi.nlm.nih.gov/123456/"},
                ]
            },
        },
        query_name="x_trial_monitoring",
    )

    assert candidate.source_record_id == "123"
    assert candidate.canonical_url == "https://x.com/vetonc/status/123"
    assert candidate.review_status == x_topic_monitor.XReviewStatus.NEEDS_REVIEW
    assert "canine hemangiosarcoma" in candidate.matched_terms
    assert candidate.durable_links == ["https://pubmed.ncbi.nlm.nih.gov/123456/"]
    assert candidate.quality_score > 0.5


def test_x_topic_monitor_extracts_durable_links_from_text_fallbacks():
    candidate = x_topic_monitor.normalize_post_payload(
        {
            "id": "123",
            "author": {"username": "vetonc"},
            "lang": "en",
            "text": (
                "Canine hemangiosarcoma update with DOI 10.1158/0008-5472.CAN-26-0002, "
                "PMID: 87654321, PMCID: PMC6686562, NCT12345678, and "
                "https://www.nature.com/articles/s41586-026-00001."
            ),
        },
        query_name="x_disease_monitoring",
    )

    assert candidate.durable_links == [
        "https://clinicaltrials.gov/study/NCT12345678",
        "https://doi.org/10.1158/0008-5472.CAN-26-0002",
        "https://pmc.ncbi.nlm.nih.gov/articles/PMC6686562/",
        "https://pubmed.ncbi.nlm.nih.gov/87654321/",
        "https://www.nature.com/articles/s41586-026-00001",
    ]
    assert "contains durable source link" in candidate.review_reasons


def test_x_topic_review_contracts_validate():
    linked_source = XTopicLinkedSource(
        url="https://pubmed.ncbi.nlm.nih.gov/123456/",
        recommended_source_key="pubmed",
        identifier_type="pmid",
        identifier="123456",
        should_ingest=True,
        reason="PubMed record.",
    )
    action = XTopicReviewAction(
        source_record_id="123",
        query_name="x_trial_monitoring",
        action="flag_for_ingestion",
        severity="watch",
        reason="Linked PubMed record should be harvested.",
        ingestible_links=[linked_source],
    )
    result = XTopicReviewResult(actions=[action])

    assert XTopicReviewRequest().review_mode == "openrouter_required"
    assert result.actions[0].ingestible_links[0].identifier_type == "pmid"
    with pytest.raises(ValueError):
        XTopicReviewAction(source_record_id="123", action="bad", severity="watch", reason="bad")
    with pytest.raises(ValueError):
        XTopicLinkedSource(url="https://example.com", identifier_type="bad", reason="bad")


def test_x_topic_review_agent_flags_ingestible_links_and_skips_social_only():
    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_trial_monitoring",
                    "username": "vetonc",
                    "quality_score": 0.7,
                    "durable_links": ["https://pubmed.ncbi.nlm.nih.gov/123456/"],
                    "matched_terms": ["canine hemangiosarcoma"],
                },
                {
                    "post_id": "456",
                    "query_name": "x_disease_monitoring",
                    "username": "owner",
                    "quality_score": 0.4,
                    "durable_links": [],
                    "matched_terms": ["angiosarcoma"],
                },
            ],
        )
    )

    assert result.ingestion_candidate_count == 1
    assert result.rejected_count == 1
    assert result.actions[0].action == "flag_for_ingestion"
    assert result.actions[0].ingestible_links[0].recommended_source_key == "pubmed"
    assert result.actions[0].ingestible_links[0].identifier == "123456"
    assert result.actions[1].action == "skip_no_durable_source"


def test_x_topic_review_resolves_short_links_before_classification(monkeypatch):
    monkeypatch.setattr(
        x_topic_review,
        "_follow_redirects",
        lambda url: "https://pubmed.ncbi.nlm.nih.gov/123456/",
    )

    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "quality_score": 0.7,
                    "durable_links": ["https://go.ufl.edu/r2uqpua"],
                }
            ],
        )
    )

    link = result.actions[0].ingestible_links[0]
    assert result.ingestion_candidate_count == 1
    assert result.actions[0].action == "flag_for_ingestion"
    assert link.url == "https://pubmed.ncbi.nlm.nih.gov/123456/"
    assert link.recommended_source_key == "pubmed"
    assert link.identifier == "123456"
    assert link.metadata["original_url"] == "https://go.ufl.edu/r2uqpua"
    assert link.metadata["resolved"] is True


def test_x_topic_review_queues_resolved_articles_for_scrape_followup(monkeypatch):
    resolved_url = (
        "https://cancer.ufl.edu/2026/04/20/"
        "researchers-characterize-genetic-landscape-of-angiosarcoma-opening-new-frontier-in-rare-cancer/"
    )
    monkeypatch.setattr(x_topic_review, "_follow_redirects", lambda url: resolved_url)

    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "quality_score": 0.7,
                    "durable_links": ["https://go.ufl.edu/r2uqpua"],
                }
            ],
        )
    )

    action = result.actions[0]
    link = action.ingestible_links[0]
    assert result.ingestion_candidate_count == 1
    assert action.action == "queue_source_followup"
    assert link.url == resolved_url
    assert link.recommended_source_key == "x_linked_article"
    assert link.should_ingest is False
    assert link.metadata["followup_type"] == "controlled_scrape_review"
    assert link.metadata["source_profile"] == "x_linked_article"
    assert link.metadata["original_url"] == "https://go.ufl.edu/r2uqpua"


def test_x_topic_review_queues_publisher_articles_for_scrape_followup():
    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "quality_score": 0.7,
                    "durable_links": ["https://www.nature.com/articles/s41586-026-00001"],
                }
            ],
        )
    )

    action = result.actions[0]
    link = action.ingestible_links[0]
    assert action.action == "queue_source_followup"
    assert link.recommended_source_key == "x_linked_article"
    assert link.metadata["followup_type"] == "controlled_scrape_review"
    assert link.metadata["resolution_status"] == "not_short_link"


def test_x_topic_review_queues_unresolved_short_links_for_followup(monkeypatch):
    def fake_follow_redirects(url):
        raise RuntimeError("timeout")

    monkeypatch.setattr(x_topic_review, "_follow_redirects", fake_follow_redirects)

    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "quality_score": 0.7,
                    "durable_links": ["https://t.co/source"],
                }
            ],
        )
    )

    action = result.actions[0]
    link = action.ingestible_links[0]
    assert action.action == "queue_source_followup"
    assert link.url == "https://t.co/source"
    assert link.recommended_source_key == "x_linked_article"
    assert link.metadata["resolution_status"] == "failed"
    assert link.metadata["fallback_reason"] == "unresolved_short_link"


def test_x_topic_review_strips_tracking_params_from_publisher_dois():
    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "quality_score": 0.7,
                    "durable_links": [
                        "https://www.frontiersin.org/journals/veterinary-science/articles/10.3389/fvets.2026.1778366/full?utm_source=twitter#metrics"
                    ],
                }
            ],
        )
    )

    link = result.actions[0].ingestible_links[0]
    assert link.recommended_source_key == "crossref"
    assert link.identifier == "10.3389/fvets.2026.1778366"


def test_x_topic_review_sends_high_volume_link_lists_to_human_review():
    links = [f"https://pubmed.ncbi.nlm.nih.gov/{100000 + index}/" for index in range(21)]

    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "quality_score": 0.9,
                    "durable_links": links,
                }
            ],
        )
    )

    action = result.actions[0]
    assert action.action == "needs_human_review"
    assert action.ingestible_links == []
    assert action.metadata["actionable_link_count"] == 21
    assert result.ingestion_candidate_count == 0
    assert result.needs_human_review_count == 1


def test_x_topic_review_openrouter_preserves_deterministic_ingestion_guardrail(monkeypatch):
    def fake_review_model(model_name, review_payload):
        assert model_name == "anthropic/claude-sonnet-test"
        assert review_payload["candidates"][0]["durable_links"] == [
            "https://doi.org/10.7717/peerj.4375"
        ]
        return {
            "text": json.dumps(
                {
                    "actions": [
                        {
                            "source_record_id": "123",
                            "query_name": "x_disease_monitoring",
                            "username": "vetonc",
                            "action": "needs_human_review",
                            "severity": "critical",
                            "reason": "Model wants a human pass.",
                            "links": ["https://doi.org/10.7717/peerj.4375"],
                            "evidence_refs": ["candidate:123"],
                            "metadata": {"reviewer": "model"},
                            "extra_field": "ignored",
                        }
                    ],
                    "evidence": {"review_summary": "reviewed"},
                    "errors": [],
                }
            ),
            "metadata": {"model_name": "anthropic/claude-sonnet-test", "usage": {"cost": 0.01}},
        }

    monkeypatch.setattr(x_topic_review, "_openrouter_review_model", fake_review_model)

    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="openrouter_required",
            review_models=["anthropic/claude-sonnet-test"],
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "username": "vetonc",
                    "quality_score": 0.7,
                    "durable_links": ["https://doi.org/10.7717/peerj.4375"],
                }
            ],
        )
    )

    assert result.ingestion_candidate_count == 1
    assert [action.action for action in result.actions] == ["needs_human_review", "flag_for_ingestion"]
    assert result.actions[0].severity == "watch"
    assert result.actions[1].ingestible_links[0].recommended_source_key == "crossref"
    assert result.evidence["model_reviews"][0]["status"] == "completed"


def test_service_x_topic_review_creates_agent_run(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "x-topic-agent.sqlite3", seed=False)

    result = HSAResearchService(repo).run_x_topic_review(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_trial_monitoring",
                    "quality_score": 0.7,
                    "durable_links": ["https://clinicaltrials.gov/study/NCT12345678"],
                }
            ],
        )
    )

    assert result.agent_run_id is not None
    assert result.actions[0].ingestible_links[0].recommended_source_key == "clinicaltrials_gov"
    runs = repo.list_agent_runs(agent_name="x_topic_review_agent", status="completed", source_key="x_topic_monitor")
    assert runs
    assert runs[0].summary["ingestion_candidate_count"] == 1


def test_x_topic_monitor_requires_review_before_storage_contracts():
    payload = {
        "id": "123",
        "author_id": "456",
        "author": {"username": "vetonc"},
        "created_at": "2026-04-28T10:00:00Z",
        "lang": "en",
        "text": "Canine hemangiosarcoma signal.",
    }
    candidate = x_topic_monitor.normalize_post_payload(payload, query_name="x_disease_monitoring")

    with pytest.raises(ValueError):
        x_topic_monitor.to_research_record(candidate, payload, accepted_by="operator")

    accepted = candidate.model_copy(update={"review_status": x_topic_monitor.XReviewStatus.ACCEPTED_SIGNAL})
    record = x_topic_monitor.to_research_record(accepted, payload, accepted_by="operator")

    assert record.raw_record.source_key == "x_topic_monitor"
    assert record.research_object.object_type == ResearchObjectType.KNOWLEDGE_ENTRY
    assert record.research_object.dedupe_key == "x_topic_monitor:post:123"
    assert record.document_chunk.section_label == "x_topic_signal"
    assert "text retention mode=store_metadata_only" in record.document_chunk.text_content


def test_clinicaltrials_gov_v2_normalizer_extracts_trial_fields():
    study = {
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT00000001",
                "orgStudyIdInfo": {"id": "ORG-1"},
                "briefTitle": "Pazopanib in Angiosarcoma",
                "officialTitle": "Pazopanib for Patients With Advanced Human Angiosarcoma",
            },
            "statusModule": {
                "overallStatus": "RECRUITING",
                "startDateStruct": {"date": "2026-01-01", "type": "ACTUAL"},
                "studyFirstPostDateStruct": {"date": "2026-02-01", "type": "ACTUAL"},
                "completionDateStruct": {"date": "2028-01", "type": "ESTIMATED"},
            },
            "descriptionModule": {
                "briefSummary": "This study tests pazopanib in human angiosarcoma.",
                "detailedDescription": "Participants receive pazopanib and undergo response assessment.",
            },
            "conditionsModule": {"conditions": ["Angiosarcoma", "Vascular Sarcoma"]},
            "designModule": {
                "studyType": "INTERVENTIONAL",
                "phases": ["PHASE2"],
                "enrollmentInfo": {"count": 42, "type": "ESTIMATED"},
            },
            "armsInterventionsModule": {
                "interventions": [
                    {"type": "DRUG", "name": "Pazopanib"},
                    {"type": "DRUG", "name": "Paclitaxel"},
                ]
            },
            "outcomesModule": {
                "primaryOutcomes": [{"measure": "Objective response rate"}],
                "secondaryOutcomes": [{"measure": "Progression-free survival"}],
            },
            "eligibilityModule": {
                "eligibilityCriteria": "Inclusion: measurable angiosarcoma.",
                "minimumAge": "18 Years",
                "sex": "ALL",
                "stdAges": ["ADULT", "OLDER_ADULT"],
            },
            "sponsorCollaboratorsModule": {
                "leadSponsor": {"name": "Example Cancer Center"},
                "collaborators": [{"name": "National Cancer Institute"}],
            },
            "contactsLocationsModule": {
                "locations": [
                    {
                        "facility": "Example Hospital",
                        "city": "Denver",
                        "state": "Colorado",
                        "country": "United States",
                        "status": "RECRUITING",
                    }
                ]
            },
        }
    }

    harvester = ClinicalTrialsGovHarvesterV2()
    record = harvester.normalize(study)

    assert record.raw_record.source_key == "clinicaltrials_gov"
    assert record.research_object.object_type == "clinical_trial"
    assert record.research_object.identifiers["nct_id"] == "NCT00000001"
    assert record.research_object.canonical_url == "https://clinicaltrials.gov/study/NCT00000001"
    assert record.research_object.metadata["overall_status"] == "RECRUITING"
    assert record.research_object.metadata["interventions"] == ["Pazopanib", "Paclitaxel"]
    assert record.research_object.metadata["primary_outcomes"] == ["Objective response rate"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == [
        "human_angiosarcoma",
        "vascular_sarcoma_analog",
    ]
    assert "Inclusion: measurable angiosarcoma." in harvester.text_for_chunking(record)


def test_clinicaltrials_gov_v2_is_registered_harvester():
    assert HARVESTERS_V2["clinicaltrials_gov"] is ClinicalTrialsGovHarvesterV2


def _avma_vctr_study_card():
    return {
        "id": 12345,
        "is_similar": False,
        "description": "<p>Dogs with splenic hemangiosarcoma receive antibody therapy after splenectomy.</p>",
        "absolute_url": "https://veterinaryclinicaltrials.org/s/antibody-therapy-hsa/",
        "tagline": "Dogs with splenic hemangiosarcoma after splenectomy",
        "name": "Safety and efficacy of antibody therapy for dogs with splenic hemangiosarcoma",
        "visible_sc_items": [
            {"id": "species-dog", "tag_path": "/species/dogs", "label": "Dogs", "parent_label": "Species"},
            {
                "id": "oncology-hsa",
                "tag_path": "/primary-field/oncology/hemangiosarcoma",
                "label": "Hemangiosarcoma",
                "parent_label": "Oncology",
            },
            {
                "id": "intervention-biologic",
                "tag_path": "/intervention-type/biologic",
                "label": "Biologic",
                "parent_label": "Intervention type",
            },
            {
                "id": "financial-covered",
                "tag_path": "/financial-incentive/study-costs-covered",
                "label": "Study costs covered",
                "parent_label": "Financial incentive",
            },
        ],
        "thumbnail": "https://veterinaryclinicaltrials.org/media/cache/thumb.jpg",
        "study_type": "Interventional",
        "target_gender": "All",
        "age_range": "1 year and older",
        "top_investigator": "Claire Lemons, DVM",
        "status": "Recruiting",
        "status_color": "green",
        "distance_to_location": None,
        "vct_code": "VCT16000189",
    }


def test_avma_vctr_v2_normalizer_extracts_study_card_metadata():
    harvester = AVMAVCTRHarvesterV2()
    record = harvester.normalize(_avma_vctr_study_card(), source_query="hemangiosarcoma", source_total=27)

    assert record.raw_record.source_key == "avma_vctr"
    assert record.raw_record.source_record_id == "VCT16000189"
    assert record.research_object.object_type == "veterinary_trial"
    assert record.research_object.identifiers["vct_code"] == "VCT16000189"
    assert record.research_object.identifiers["avma_study_id"] == "12345"
    assert record.research_object.dedupe_key == "vct_code:vct16000189"
    assert record.research_object.canonical_url == "https://veterinaryclinicaltrials.org/s/antibody-therapy-hsa/"
    assert record.research_object.title == "Safety and efficacy of antibody therapy for dogs with splenic hemangiosarcoma"
    assert record.research_object.abstract == (
        "Dogs with splenic hemangiosarcoma receive antibody therapy after splenectomy."
    )
    assert record.research_object.metadata["status"] == "Recruiting"
    assert record.research_object.metadata["species"] == ["Dogs"]
    assert record.research_object.metadata["conditions"] == ["Hemangiosarcoma"]
    assert record.research_object.metadata["intervention_types"] == ["Biologic"]
    assert record.research_object.metadata["financial_incentives"] == ["Study costs covered"]
    assert record.research_object.metadata["visible_search_categories"]["Species"] == ["Dogs"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["canine_hsa"]
    assert record.research_object.metadata["source_total"] == 27
    assert "VCT16000189" in harvester.text_for_chunking(record)
    assert "Biologic" in harvester.text_for_chunking(record)


def test_avma_vctr_v2_normalizer_does_not_treat_late_reference_as_primary_hsa():
    study = _avma_vctr_study_card() | {
        "name": "The effect of a novel mushroom formula on canine oral malignant melanoma",
        "description": (
            "Oral malignant melanoma is an aggressive cancer in dogs. This study evaluates a mushroom "
            "supplement for melanoma. " + ("General oncology background. " * 40)
            + "A cited hemangiosarcoma study informed the dose."
        ),
        "tagline": "Evaluation of Medicinal Mushroom Supplementation in Canine Oral Malignant Melanoma",
        "visible_sc_items": [
            {"id": "species-dog", "tag_path": "/species/dogs", "label": "Canine", "parent_label": "Species"},
            {
                "id": "oncology-melanoma",
                "tag_path": "/primary-field/oncology/melanoma",
                "label": "Melanoma",
                "parent_label": "Oncology",
            },
        ],
        "vct_code": "VCT-MELANOMA",
    }

    record = AVMAVCTRHarvesterV2().normalize(study, source_query="hemangiosarcoma")

    assert record.research_object.metadata["conditions"] == ["Melanoma"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == []


def test_avma_vctr_v2_fetch_uses_public_search_endpoint(monkeypatch):
    captured = {}

    def fake_get_json(url, params):
        captured["url"] = url
        captured["params"] = params
        return {"total": 1, "studies": [_avma_vctr_study_card()]}

    monkeypatch.setattr("hsa_research.ingestion_bridge.harvesters_v2._get_json", fake_get_json)

    records = AVMAVCTRHarvesterV2().fetch("hemangiosarcoma", limit=5)

    assert len(records) == 1
    assert captured["url"] == "https://veterinaryclinicaltrials.org/avma/studies/search/json/"
    assert captured["params"]["search"] == "hemangiosarcoma"
    assert captured["params"]["skip"] == 0
    assert captured["params"]["take"] == 5
    assert captured["params"]["sort_by"] == "score"
    assert captured["params"]["skip_similar_studies"] == "true"
    assert captured["params"]["extra_aggregations"] == "[]"


def test_avma_vctr_v2_is_registered_harvester():
    assert HARVESTERS_V2["avma_vctr"] is AVMAVCTRHarvesterV2


def test_avma_vctr_claim_extractor_requires_primary_hsa_scope():
    extractor = LocalRuleClaimExtractor()
    melanoma = ResearchObject(
        object_type="veterinary_trial",
        title="The effect of a novel mushroom formula on canine oral malignant melanoma",
        abstract=(
            "This canine melanoma study evaluates immune activity and angiogenesis. "
            "A cited hemangiosarcoma paper informed dose selection."
        ),
        source_key="avma_vctr",
        metadata={"conditions": ["Melanoma"]},
    )
    melanoma_chunk = DocumentChunk(
        research_object_id=melanoma.id,
        chunk_index=0,
        section_label="veterinary_trial_record",
        text_content="Dogs receive an immune supplement. Angiogenesis and macrophage activity are monitored.",
        content_hash="melanoma",
    )
    hsa = ResearchObject(
        object_type="veterinary_trial",
        title="Combination therapy for dogs with hemangiosarcoma",
        abstract="Dogs with splenic hemangiosarcoma receive doxorubicin.",
        source_key="avma_vctr",
        metadata={"conditions": ["Hemangiosarcoma"]},
    )
    hsa_chunk = DocumentChunk(
        research_object_id=hsa.id,
        chunk_index=0,
        section_label="veterinary_trial_record",
        text_content="Dogs with hemangiosarcoma receive doxorubicin chemotherapy in this trial.",
        content_hash="hsa",
    )

    assert extractor.extract_chunk(melanoma_chunk, melanoma) == []
    claims = extractor.extract_chunk(hsa_chunk, hsa)
    assert any(claim.statement.startswith("doxorubicin is discussed") for claim in claims)


def test_icdc_v2_normalizer_extracts_canine_case_metadata():
    case = {
        "case_id": "TCL01-DEN-HSA",
        "study_code": "TCL01",
        "study_type": "Genomics",
        "cohort": "Cell line",
        "breed": "Golden Retriever",
        "diagnosis": "Hemangiosarcoma",
        "disease_site": "Kidney",
        "primary_disease_site": "Kidney",
        "stage_of_disease": "Unknown",
        "age": 11.0,
        "sex": "Male",
        "response_to_treatment": "Not Applicable",
        "files": ["file-1", "file-2"],
        "treatment_data": "Yes",
        "follow_up_data": "No",
        "pathology_report": "No",
    }
    study = {
        "clinical_study_designation": "TCL01",
        "clinical_study_name": "Whole exome sequencing analysis of canine cancer cell lines",
        "clinical_study_description": "This study analyzes canine cancer cell lines including hemangiosarcoma.",
        "clinical_study_type": "Genomics",
        "accession_id": "000008",
        "dates_of_conduct": "2017-2019",
        "study_disposition": "Unrestricted",
    }

    harvester = ICDCHarvesterV2()
    record = harvester.normalize(case, study)

    assert record.raw_record.source_key == "icdc"
    assert record.research_object.object_type == "dataset"
    assert record.research_object.identifiers["icdc_case_id"] == "TCL01-DEN-HSA"
    assert record.research_object.identifiers["study_code"] == "TCL01"
    assert record.research_object.metadata["breed"] == "Golden Retriever"
    assert record.research_object.metadata["file_count"] == 2
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["canine_hsa"]
    assert "Diagnosis: Hemangiosarcoma" in harvester.text_for_chunking(record)


def test_icdc_v2_is_registered_harvester():
    assert HARVESTERS_V2["icdc"] is ICDCHarvesterV2


def test_geo_v2_normalizer_extracts_dataset_metadata():
    item = {
        "uid": "200310480",
        "accession": "GSE310480",
        "title": "MicroRNA biomarkers for canine visceral hemangiosarcoma",
        "summary": "Canine visceral hemangiosarcoma samples identify miRNA biomarkers.",
        "gse": "310480",
        "taxon": "Canis lupus familiaris",
        "entrytype": "GSE",
        "gdstype": "Non-coding RNA profiling by high throughput sequencing",
        "pdat": "2026/04/08",
        "suppfile": "TXT, XLSX",
        "samples": [{"accession": "GSM1", "title": "Cancer spleen 1"}],
        "n_samples": 36,
        "pubmedids": ["41924723"],
        "ftplink": "ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE310nnn/GSE310480/",
        "bioproject": "PRJNA1366394",
    }

    harvester = GEOHarvesterV2()
    record = harvester.normalize(item)

    assert record.raw_record.source_key == "geo"
    assert record.research_object.object_type == "dataset"
    assert record.research_object.identifiers["geo_accession"] == "GSE310480"
    assert record.research_object.identifiers["bioproject"] == "PRJNA1366394"
    assert record.research_object.metadata["sample_accessions"] == ["GSM1"]
    assert record.research_object.metadata["supplementary_file_types"] == ["TXT", "XLSX"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["canine_hsa"]
    assert "Cancer spleen 1" in harvester.text_for_chunking(record)


def test_sra_v2_normalizer_extracts_run_metadata():
    item = {
        "uid": "42394755",
        "expxml": """
          <Summary>
            <Title>NM-BS23039 Canine hemangiosarcoma primary cell</Title>
            <Platform instrument_model="Illumina NovaSeq 6000">ILLUMINA</Platform>
            <Statistics total_runs="1" total_spots="26421116" total_bases="7926334800" total_size="2407693008"/>
          </Summary>
          <Submitter acc="SRA2311501" center_name="Tokyo University of Agriculture and Technology"/>
          <Experiment acc="SRX31723477" name="NM-BS23039 Canine hemangiosarcoma primary cell"/>
          <Study acc="SRP660537" name="Canine hemangiosarcoma primary cell RNA sequencing"/>
          <Organism taxid="9615" ScientificName="Canis lupus familiaris"/>
          <Sample acc="SRS27692090"/>
          <Library_descriptor>
            <LIBRARY_NAME>NM-BS23039_L1_1</LIBRARY_NAME>
            <LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY>
            <LIBRARY_SOURCE>TRANSCRIPTOMIC</LIBRARY_SOURCE>
            <LIBRARY_SELECTION>RANDOM</LIBRARY_SELECTION>
            <LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT>
          </Library_descriptor>
          <Bioproject>PRJNA1399620</Bioproject>
          <Biosample>SAMN54501165</Biosample>
        """,
        "runs": '<Run acc="SRR36719144" total_spots="26421116" total_bases="7926334800" is_public="true"/>',
        "createdate": "2026/03/31",
        "updatedate": "2026/01/07",
    }

    harvester = SRAHarvesterV2()
    record = harvester.normalize(item)

    assert record.raw_record.source_key == "sra"
    assert record.research_object.object_type == "dataset"
    assert record.research_object.identifiers["sra_experiment"] == "SRX31723477"
    assert record.research_object.identifiers["sra_run"] == "SRR36719144"
    assert record.research_object.identifiers["bioproject"] == "PRJNA1399620"
    assert record.research_object.metadata["library_strategy"] == "RNA-Seq"
    assert record.research_object.metadata["library_layout"] == "PAIRED"
    assert record.research_object.metadata["statistics"]["total_spots"] == "26421116"
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["canine_hsa"]
    assert "SRR36719144" in harvester.text_for_chunking(record)


def test_geo_and_sra_v2_are_registered_harvesters():
    assert HARVESTERS_V2["geo"] is GEOHarvesterV2
    assert HARVESTERS_V2["sra"] is SRAHarvesterV2


def test_pubchem_v2_normalizer_extracts_compound_metadata():
    payload = {
        "query_term": "propranolol",
        "properties": {
            "CID": 4946,
            "Title": "Propranolol",
            "MolecularFormula": "C16H21NO2",
            "MolecularWeight": 259.34,
            "CanonicalSMILES": "CC(C)NCC(COC1=CC=CC2=CC=CC=C21)O",
            "InChIKey": "AQHHHDLHHXJYJD-UHFFFAOYSA-N",
            "IUPACName": "1-naphthalen-1-yloxy-3-(propan-2-ylamino)propan-2-ol",
            "XLogP": 3.0,
            "TPSA": 41.5,
        },
        "synonyms": ["Propranolol", "Inderal"],
    }

    harvester = PubChemHarvesterV2()
    record = harvester.normalize(payload)

    assert record.raw_record.source_key == "pubchem"
    assert record.research_object.object_type == "compound_record"
    assert record.research_object.identifiers["pubchem_cid"] == "4946"
    assert record.research_object.identifiers["inchikey"] == "AQHHHDLHHXJYJD-UHFFFAOYSA-N"
    assert record.research_object.dedupe_key == "pubchem_cid:4946"
    assert record.research_object.canonical_url == "https://pubchem.ncbi.nlm.nih.gov/compound/4946"
    assert record.research_object.metadata["canonical_smiles"].startswith("CC(C)")
    assert "Inderal" in harvester.text_for_chunking(record)


def test_chembl_v2_normalizer_extracts_bioactivity_metadata():
    payload = {
        "query_term": "toceranib",
        "molecule": {
            "molecule_chembl_id": "CHEMBL13608",
            "pref_name": "TOCERANIB",
            "max_phase": 4,
            "molecule_type": "Small molecule",
        },
        "activity": {
            "activity_id": 123,
            "molecule_chembl_id": "CHEMBL13608",
            "target_chembl_id": "CHEMBL279",
            "target_pref_name": "Vascular endothelial growth factor receptor 2",
            "target_organism": "Homo sapiens",
            "assay_chembl_id": "CHEMBL-A",
            "document_chembl_id": "CHEMBL-D",
            "standard_type": "IC50",
            "standard_relation": "=",
            "standard_value": "5.0",
            "standard_units": "nM",
            "pchembl_value": "8.3",
            "assay_description": "Inhibition of VEGFR2 kinase activity.",
        },
    }

    harvester = ChEMBLHarvesterV2()
    record = harvester.normalize(payload)

    assert record.raw_record.source_key == "chembl"
    assert record.research_object.object_type == "bioactivity_assay"
    assert record.research_object.identifiers["chembl_activity_id"] == "123"
    assert record.research_object.identifiers["chembl_molecule_id"] == "CHEMBL13608"
    assert record.research_object.dedupe_key == "chembl_activity_id:123"
    assert record.research_object.metadata["standard_type"] == "IC50"
    assert record.research_object.metadata["target_pref_name"] == "Vascular endothelial growth factor receptor 2"
    assert record.research_object.metadata["target_gene"] == "KDR"
    assert record.research_object.metadata["target_category"] == "vegf_angiogenesis"
    assert record.research_object.metadata["pchembl_numeric"] == 8.3
    assert "Target gate: KDR (vegf_angiogenesis)" in harvester.text_for_chunking(record)
    assert "pChEMBL: 8.3" in harvester.text_for_chunking(record)


def test_chembl_v2_fetches_only_target_gated_relevant_bioactivities(monkeypatch):
    def fake_get_json(url, params):
        if url.endswith("/molecule.json"):
            if params.get("pref_name__iexact") == "toceranib":
                return {
                    "molecules": [
                        {
                            "molecule_chembl_id": "CHEMBL13608",
                            "pref_name": "TOCERANIB",
                            "max_phase": 2,
                            "molecule_type": "Small molecule",
                        }
                    ]
                }
            return {"molecules": []}
        if url.endswith("/activity.json"):
            assert params["target_chembl_id__in"] == "CHEMBL279"
            assert params["standard_type__in"] == "IC50"
            assert params["assay_type__in"] == "B"
            assert params["order_by"] == "-pchembl_value"
            return {
                "activities": [
                    {
                        "activity_id": 1,
                        "molecule_chembl_id": "CHEMBL13608",
                        "target_chembl_id": "CHEMBL279",
                        "target_pref_name": "Vascular endothelial growth factor receptor 2",
                        "target_organism": "Homo sapiens",
                        "assay_type": "B",
                        "standard_type": "IC50",
                        "standard_relation": "=",
                        "standard_value": "60.0",
                        "standard_units": "nM",
                        "pchembl_value": "7.22",
                        "assay_description": "Inhibition of VEGFR2.",
                    },
                    {
                        "activity_id": 2,
                        "target_chembl_id": "CHEMBL999",
                        "target_organism": "Homo sapiens",
                        "assay_type": "B",
                        "standard_type": "IC50",
                        "pchembl_value": "9.0",
                    },
                    {
                        "activity_id": 3,
                        "target_chembl_id": "CHEMBL279",
                        "target_organism": "Homo sapiens",
                        "assay_type": "B",
                        "standard_type": "IC50",
                        "pchembl_value": "3.5",
                    },
                ]
            }
        raise AssertionError(f"Unexpected ChEMBL URL: {url}")

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    harvester = ChEMBLHarvesterV2()
    records = harvester.fetch(
        "toceranib",
        limit=3,
        target_chembl_ids=["CHEMBL279"],
        target_organisms=["Homo sapiens"],
        standard_types=["IC50"],
        assay_types=["B"],
        min_pchembl=6.0,
        activities_per_molecule=3,
        include_cell_line_assays=False,
    )

    assert len(records) == 1
    assert records[0].research_object.identifiers["chembl_activity_id"] == "1"
    assert records[0].research_object.metadata["target_gene"] == "KDR"
    assert records[0].research_object.metadata["target_category"] == "vegf_angiogenesis"


def test_chembl_v2_cell_line_lane_requires_real_disease_term(monkeypatch):
    def fake_get_json(url, params):
        if url.endswith("/molecule.json"):
            if params.get("pref_name__iexact") == "paclitaxel":
                return {
                    "molecules": [
                        {
                            "molecule_chembl_id": "CHEMBL428647",
                            "pref_name": "PACLITAXEL",
                            "max_phase": 4,
                            "molecule_type": "Small molecule",
                        }
                    ]
                }
            return {"molecules": []}
        if url.endswith("/activity.json") and params.get("target_type") == "CELL-LINE":
            return {
                "activities": [
                    {
                        "activity_id": 10,
                        "target_chembl_id": "CHEMBL210",
                        "target_pref_name": "Beta-2 adrenergic receptor",
                        "target_organism": "Homo sapiens",
                        "assay_type": "F",
                        "standard_type": "IC50",
                        "pchembl_value": "9.2",
                        "assay_description": "Activity in endogenously expressing cells.",
                    },
                    {
                        "activity_id": 11,
                        "target_chembl_id": "CHEMBL613827",
                        "target_pref_name": "MES-SA/Dx5",
                        "target_organism": "Homo sapiens",
                        "assay_type": "F",
                        "standard_type": "IC50",
                        "pchembl_value": "10.4",
                        "assay_description": "Cytotoxic activity against uterine sarcoma cells.",
                    },
                ]
            }
        if url.endswith("/activity.json"):
            return {"activities": []}
        raise AssertionError(f"Unexpected ChEMBL URL: {url}")

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    harvester = ChEMBLHarvesterV2()
    records = harvester.fetch(
        "paclitaxel",
        limit=3,
        target_chembl_ids=["CHEMBL210"],
        target_organisms=["Homo sapiens"],
        include_cell_line_assays=True,
        cell_line_terms=["sarcoma", "dog"],
        cell_line_records_per_molecule=2,
    )

    assert len(records) == 1
    assert records[0].research_object.identifiers["chembl_activity_id"] == "11"
    assert records[0].research_object.metadata["target_category"] == "cell_cytotoxicity"
    assert records[0].research_object.metadata["matched_cell_line_term"] == "sarcoma"


def test_uniprot_v2_normalizer_extracts_target_metadata():
    entry = {
        "primaryAccession": "P35968",
        "uniProtKBId": "VGFR2_HUMAN",
        "entryType": "UniProtKB reviewed (Swiss-Prot)",
        "proteinDescription": {
            "recommendedName": {
                "fullName": {"value": "Vascular endothelial growth factor receptor 2"}
            }
        },
        "genes": [{"geneName": {"value": "KDR"}, "synonyms": [{"value": "VEGFR2"}]}],
        "organism": {"scientificName": "Homo sapiens", "taxonId": 9606},
        "sequence": {"length": 1356, "molWeight": 151527},
        "comments": [
            {
                "commentType": "FUNCTION",
                "texts": [{"value": "Tyrosine-protein kinase receptor for VEGFA."}],
            }
        ],
        "keywords": [{"name": "Angiogenesis"}],
        "uniProtKBCrossReferences": [{"database": "AlphaFoldDB", "id": "AF-P35968-F1"}],
    }

    harvester = UniProtHarvesterV2()
    record = harvester.normalize(entry, source_query="KDR")

    assert record.raw_record.source_key == "uniprot"
    assert record.research_object.object_type == "structure"
    assert record.research_object.identifiers["uniprot_accession"] == "P35968"
    assert record.research_object.identifiers["gene_symbol"] == "KDR"
    assert record.research_object.dedupe_key == "uniprot_accession:p35968"
    assert record.research_object.metadata["reviewed"] is True
    assert record.research_object.metadata["target_gene"] == "KDR"
    assert record.research_object.metadata["target_category"] == "vegf_angiogenesis"
    assert record.research_object.metadata["species_scope"] == "human"
    assert record.research_object.metadata["gene_match_verified"] is True
    assert record.research_object.metadata["alphafold_ids"] == ["AF-P35968-F1"]
    assert "AlphaFold IDs: AF-P35968-F1" in harvester.text_for_chunking(record)


def test_rcsb_pdb_v2_normalizer_extracts_structure_metadata():
    payload = {
        "query_term": "KDR",
        "search_hit": {"identifier": "3VHE", "score": 42.0},
        "entry": {
            "rcsb_id": "3VHE",
            "struct": {"title": "Crystal structure of VEGFR2 kinase domain"},
            "exptl": [{"method": "X-RAY DIFFRACTION"}],
            "rcsb_accession_info": {
                "deposit_date": "2011-01-01",
                "initial_release_date": "2012-02-01",
                "revision_date": "2020-01-01",
                "has_released_experimental_data": True,
            },
            "citation": [{"pdbx_database_id_PubMed": 22212345}],
            "rcsb_entry_info": {"polymer_entity_count_protein": 1},
        },
    }

    harvester = RCSBPDBHarvesterV2()
    record = harvester.normalize(payload)

    assert record.raw_record.source_key == "rcsb_pdb"
    assert record.research_object.object_type == "structure"
    assert record.research_object.identifiers["pdb_id"] == "3VHE"
    assert record.research_object.identifiers["pmid"] == "22212345"
    assert record.research_object.dedupe_key == "pdb_id:3vhe"
    assert record.research_object.publication_year == 2012
    assert record.research_object.metadata["target_gene"] == "KDR"
    assert record.research_object.metadata["target_category"] == "vegf_angiogenesis"
    assert record.research_object.metadata["experimental_methods"] == ["X-RAY DIFFRACTION"]
    assert record.research_object.metadata["protein_entity_count"] == 1
    assert "PDB ID: 3VHE" in harvester.text_for_chunking(record)


def test_openfda_animal_events_v2_normalizer_extracts_safety_metadata():
    event = {
        "unique_aer_id_number": "US-FDA-CVM-2026-0001",
        "original_receive_date": "20260401",
        "animal": {"species": "Dog", "breed": "Golden Retriever", "gender": "Female", "age": {"unit": "Year", "value": "9"}},
        "drug": [
            {
                "brand_name": "Example Doxorubicin",
                "active_ingredients": [{"name": "doxorubicin"}],
            }
        ],
        "reaction": [{"veddra_term_name": "Vomiting", "veddra_term_code": "334"}, {"veddra_term_name": "Neutropenia"}],
        "outcome": "Recovered",
        "serious_ae": "true",
        "primary_reporter": "Veterinarian",
    }

    harvester = OpenFDAAnimalEventsHarvesterV2()
    record = harvester.normalize(event, source_query="doxorubicin", source_search='animal.species:"Dog"')

    assert record.raw_record.source_key == "openfda_animal_events"
    assert record.research_object.object_type == "safety_report"
    assert record.research_object.identifiers["openfda_report_id"] == "US-FDA-CVM-2026-0001"
    assert record.research_object.dedupe_key == "openfda_report_id:us-fda-cvm-2026-0001"
    assert record.research_object.publication_year == 2026
    assert record.research_object.metadata["species"] == "Dog"
    assert record.research_object.metadata["drug_names"] == ["Example Doxorubicin", "doxorubicin"]
    assert record.research_object.metadata["reaction_terms"] == ["Vomiting", "Neutropenia"]
    assert record.research_object.metadata["reaction_codes"] == ["334"]
    assert "Responsible use: signal_generation_only_not_clinical_decision_support" in harvester.text_for_chunking(record)


def test_phase_three_api_harvesters_are_registered():
    assert HARVESTERS_V2["pubchem"] is PubChemHarvesterV2
    assert HARVESTERS_V2["chembl"] is ChEMBLHarvesterV2
    assert HARVESTERS_V2["uniprot"] is UniProtHarvesterV2
    assert HARVESTERS_V2["rcsb_pdb"] is RCSBPDBHarvesterV2
    assert HARVESTERS_V2["openfda_animal_events"] is OpenFDAAnimalEventsHarvesterV2


def test_openalex_v2_filters_unmatched_records_by_default():
    harvester = OpenAlexHarvesterV2()
    matched = harvester.normalize(
        {
            "id": "https://openalex.org/W1",
            "title": "Human angiosarcoma therapy",
            "publication_year": 2026,
            "abstract_inverted_index": {"Human": [0], "angiosarcoma": [1]},
            "primary_location": {"landing_page_url": "https://example.test/matched"},
        }
    )
    unmatched = harvester.normalize(
        {
            "id": "https://openalex.org/W2",
            "title": "Unrelated oncology therapy",
            "publication_year": 2026,
            "abstract_inverted_index": {"Unrelated": [0], "oncology": [1]},
            "primary_location": {"landing_page_url": "https://example.test/unmatched"},
        }
    )

    assert harvester.filter_relevant([matched, unmatched], {}) == [matched]


def test_local_store_persists_raw_and_research_object(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    record = OpenAlexHarvesterV2().normalize(
        {
            "id": "https://openalex.org/W456",
            "doi": "https://doi.org/10.1234/local",
            "title": "Local persistence example",
            "publication_year": 2026,
            "abstract_inverted_index": {"Local": [0], "object": [1]},
            "primary_location": {"landing_page_url": "https://doi.org/10.1234/local"},
        }
    )

    fetch_run_id = repo.create_fetch_run("openalex", "unit_test")
    raw_id = repo.upsert_raw_record(record.raw_record, fetch_run_id)
    object_id = repo.upsert_research_object(record.research_object, raw_id)
    saved = repo.get_research_object(object_id)

    assert saved is not None
    assert saved.title == "Local persistence example"
    assert repo.coverage_summary()["research_objects"] == 1


def test_local_deterministic_embedding_provider_is_repeatable():
    provider = LocalDeterministicEmbeddingProvider(dimensions=32)

    vector = provider.embed_text("VEGF signaling in canine hemangiosarcoma.")
    repeated = provider.embed_text("VEGF signaling in canine hemangiosarcoma.")
    fresh_provider_vector = LocalDeterministicEmbeddingProvider(dimensions=32).embed_text(
        "VEGF signaling in canine hemangiosarcoma."
    )
    different_vector = provider.embed_text("Doxorubicin toxicity monitoring in dogs.")

    assert vector == repeated
    assert vector == fresh_provider_vector
    assert vector != different_vector
    assert len(vector) == 32
    assert sum(value * value for value in vector) == pytest.approx(1.0)


def test_openrouter_embedding_provider_calls_embeddings_api(monkeypatch):
    calls = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def read(self):
            return json.dumps(
                {
                    "data": [
                        {"index": 0, "embedding": [0.1, 0.2, 0.3]},
                        {"index": 1, "embedding": [0.4, 0.5, 0.6]},
                    ]
                }
            ).encode("utf-8")

    def fake_urlopen(request, timeout):
        calls.append(
            {
                "url": request.full_url,
                "payload": json.loads(request.data.decode("utf-8")),
                "authorization": request.headers.get("Authorization"),
                "timeout": timeout,
            }
        )
        return FakeResponse()

    monkeypatch.setattr("hsa_research.ingestion_bridge.embeddings.urllib_request.urlopen", fake_urlopen)

    provider = OpenRouterEmbeddingProvider(
        embedding_model="openai/text-embedding-3-small",
        api_key="unit-test-key",
        timeout_seconds=12,
    )
    vectors = provider.embed_texts(["alpha", "beta"])

    assert provider.embedding_model == "openrouter:openai/text-embedding-3-small"
    assert provider.provider_name == "openrouter"
    assert provider.provider_model == "openai/text-embedding-3-small"
    assert provider.embedding_dimensions == 3
    assert vectors == [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]
    assert calls[0]["url"] == "https://openrouter.ai/api/v1/embeddings"
    assert calls[0]["payload"]["model"] == "openai/text-embedding-3-small"
    assert calls[0]["payload"]["input"] == ["alpha", "beta"]
    assert calls[0]["authorization"] == "Bearer unit-test-key"
    assert calls[0]["timeout"] == 12


def test_build_embedding_provider_selects_openrouter_and_local(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "unit-test-key")

    local_provider = build_embedding_provider("local-hash-test", dimensions=16)
    openrouter_provider = build_embedding_provider("openrouter:openai/text-embedding-3-small", dimensions=3)

    assert isinstance(local_provider, LocalDeterministicEmbeddingProvider)
    assert local_provider.embedding_dimensions == 16
    assert isinstance(openrouter_provider, OpenRouterEmbeddingProvider)
    assert openrouter_provider.embedding_model == "openrouter:openai/text-embedding-3-small"
    assert openrouter_provider.embedding_dimensions == 3


def test_embedding_model_selection_prefers_configured_then_openrouter(monkeypatch):
    models = {
        "local-hash-v1": 10,
        "openrouter:openai/text-embedding-3-small": 10,
        "openrouter:openai/text-embedding-3-large": 10,
    }

    monkeypatch.delenv("HSA_EMBEDDING_MODEL", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    assert default_embedding_model_for_environment() == "local-hash-v1"
    assert select_embedding_model_from_coverage(models) == "local-hash-v1"

    monkeypatch.setenv("OPENROUTER_API_KEY", "unit-test-key")
    assert default_embedding_model_for_environment() == "openrouter:openai/text-embedding-3-large"
    assert select_embedding_model_from_coverage(models) == "openrouter:openai/text-embedding-3-large"

    monkeypatch.setenv("HSA_EMBEDDING_MODEL", "openrouter:openai/text-embedding-3-small")
    assert default_embedding_model_for_environment() == "openrouter:openai/text-embedding-3-small"
    assert select_embedding_model_from_coverage(models) == "openrouter:openai/text-embedding-3-small"


def test_build_chunk_embedding_text_includes_canonical_entity_context():
    object_id = uuid4()
    chunk_id = uuid4()
    research_object = ResearchObject(
        id=object_id,
        object_type="publication",
        title="Canonical entity embedding example",
        abstract="Canine HSA angiogenesis context.",
        source_key="pubmed",
        identifiers={"pmid": "123"},
    )
    chunk = DocumentChunk(
        id=chunk_id,
        research_object_id=object_id,
        chunk_index=0,
        section_label="abstract",
        text_content="KDR is also called VEGF receptor 2 in canine hemangiosarcoma.",
        content_hash="embedding-text-context",
    )
    mentions = [
        EntityMention(
            research_object_id=object_id,
            chunk_id=chunk_id,
            chunk_index=0,
            section_label="abstract",
            source_key="pubmed",
            entity_type="target",
            canonical_name="KDR",
            normalized_key="target:kdr",
            matched_text="VEGF receptor 2",
            matched_alias="VEGF receptor 2",
            chunk_char_start=19,
            chunk_char_end=34,
            external_ids={"chembl_id": "CHEMBL279"},
            resolver_name="unit",
            resolver_version="1",
            match_rule="unit",
        ),
        EntityMention(
            research_object_id=object_id,
            chunk_id=chunk_id,
            chunk_index=0,
            section_label="abstract",
            source_key="pubmed",
            entity_type="disease",
            canonical_name="canine hemangiosarcoma",
            normalized_key="disease:canine_hsa",
            matched_text="canine hemangiosarcoma",
            matched_alias="canine hemangiosarcoma",
            chunk_char_start=38,
            chunk_char_end=61,
            resolver_name="unit",
            resolver_version="1",
            match_rule="unit",
        ),
    ]

    text = build_chunk_embedding_text(chunk, research_object, mentions)

    assert "title: Canonical entity embedding example" in text
    assert "chunk_text: KDR is also called VEGF receptor 2" in text
    assert "canonical_entities:" in text
    assert "target: KDR [target:kdr] (chembl_id=CHEMBL279)" in text
    assert "disease: canine hemangiosarcoma [disease:canine_hsa]" in text


def test_text_embedding_contract_rejects_dimension_mismatch():
    with pytest.raises(ValueError, match="embedding_dimensions"):
        TextEmbedding(
            chunk_id=uuid4(),
            research_object_id=uuid4(),
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash="bad-dimensions",
            embedding_model="unit-embedding-v1",
            embedding_dimensions=2,
            embedding=[1.0],
        )


def test_sqlite_text_embeddings_persist_and_report_coverage(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Embedding persistence example",
            source_key="pubmed",
            dedupe_key="pubmed:embedding-persistence",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGF signaling is discussed in canine hemangiosarcoma.",
            content_hash="embedding-chunk-1",
        )
    )

    saved = repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunk.id,
            research_object_id=object_id,
            chunk_index=chunk.chunk_index,
            source_key="pubmed",
            object_type="publication",
            content_hash=chunk.content_hash,
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[1.0, 0.0, 0.0],
            text_preview=chunk.text_content,
        )
    )

    fetched = repo.get_text_embedding(saved.embedding_id)
    listed = repo.list_text_embeddings(source_key="pubmed", embedding_model="unit-embedding-v1")
    coverage = repo.embedding_coverage(source_key="pubmed", embedding_model="unit-embedding-v1")

    assert fetched == saved
    assert listed == [saved]
    assert repo.coverage_summary()["text_embeddings"] == 1
    assert coverage.total_chunks == 1
    assert coverage.embedded_chunks == 1
    assert coverage.missing_chunks == 0
    assert coverage.coverage_ratio == 1.0
    assert coverage.embedding_models == {"unit-embedding-v1": 1}


def test_embedding_maintenance_prunes_orphan_embeddings_and_reports_full_coverage(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Embedding maintenance example",
            source_key="pubmed",
            dedupe_key="pubmed:embedding-maintenance",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis is discussed in canine hemangiosarcoma.",
            content_hash="embedding-maintenance-live",
        )
    )
    live_embedding = repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunk.id,
            research_object_id=object_id,
            chunk_index=chunk.chunk_index,
            source_key="pubmed",
            object_type="publication",
            content_hash=chunk.content_hash,
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[1.0, 0.0, 0.0],
        )
    )
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=uuid4(),
            research_object_id=object_id,
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash="embedding-maintenance-orphan",
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[0.0, 1.0, 0.0],
        )
    )

    assert repo.coverage_summary()["text_embeddings"] == 2
    assert repo.count_orphan_text_embeddings(embedding_model="unit-embedding-v1") == 1

    result = maintain_embedding_index(repo, embedding_model="unit-embedding-v1")
    report = result.to_report()

    assert result.passes_minimum_bar is True
    assert report["passed"] is True
    assert report["orphan_embeddings"]["seen"] == 1
    assert report["orphan_embeddings"]["deleted"] == 1
    assert report["embedding_coverage"]["total_chunks"] == 1
    assert report["embedding_coverage"]["embedded_chunks"] == 1
    assert report["embedding_coverage"]["missing_chunks"] == 0
    assert repo.count_orphan_text_embeddings(embedding_model="unit-embedding-v1") == 0
    assert repo.list_text_embeddings(embedding_model="unit-embedding-v1") == [live_embedding]
    assert repo.coverage_summary()["text_embeddings"] == 1


def test_index_embeddings_for_repository_is_idempotent_and_includes_entities(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Embedding index example",
            source_key="pubmed",
            dedupe_key="pubmed:embedding-index",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGF receptor 2 appears in canine hemangiosarcoma literature.",
            content_hash="embedding-index-chunk-1",
        )
    )
    repo.upsert_entity_mention(
        EntityMention(
            research_object_id=object_id,
            chunk_id=chunk.id,
            chunk_index=chunk.chunk_index,
            section_label=chunk.section_label,
            source_key="pubmed",
            entity_type="target",
            canonical_name="KDR",
            normalized_key="target:kdr",
            matched_text="VEGF receptor 2",
            matched_alias="VEGF receptor 2",
            chunk_char_start=0,
            chunk_char_end=15,
            resolver_name="unit",
            resolver_version="1",
            match_rule="unit",
        )
    )

    first_result = index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    first_embedding = repo.list_text_embeddings(source_key="pubmed", embedding_model="local-hash-test")[0]
    second_result = index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    second_embedding = repo.list_text_embeddings(source_key="pubmed", embedding_model="local-hash-test")[0]

    assert first_result.errors == ()
    assert first_result.chunks_seen == 1
    assert first_result.embeddings_created == 1
    assert second_result.embeddings_skipped == 1
    assert second_result.embeddings_created == 0
    assert second_embedding.embedding_id == first_embedding.embedding_id
    assert second_embedding.embedding == first_embedding.embedding
    assert len(repo.list_text_embeddings(embedding_model="local-hash-test")) == 1
    assert "canonical_entities: target: KDR [target:kdr]" in second_embedding.text_preview
    assert second_embedding.metadata["chunk_content_hash"] == chunk.content_hash
    assert second_embedding.metadata["canonical_entity_count"] == 1


def test_index_embeddings_for_repository_uses_configured_provider(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="OpenRouter embedding index example",
            source_key="pubmed",
            dedupe_key="pubmed:openrouter-embedding-index",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Sorafenib safety in dogs.",
            content_hash="openrouter-embedding-index-chunk",
        )
    )

    class FakeProvider:
        embedding_model = "openrouter:unit-embedding"
        provider_name = "openrouter"
        provider_model = "unit-embedding"

        @property
        def embedding_dimensions(self):
            return 3

        def embed_text(self, text):
            return [1.0, 0.0, 0.0]

        def embed_texts(self, texts):
            return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr(
        "hsa_research.ingestion_bridge.embeddings.build_embedding_provider",
        lambda embedding_model, dimensions=None: FakeProvider(),
    )

    result = index_embeddings_for_repository(
        repo,
        source_key="pubmed",
        embedding_model="openrouter:unit-embedding",
        batch_size=8,
    )
    embedding = repo.list_text_embeddings(embedding_model="openrouter:unit-embedding")[0]

    assert result.errors == ()
    assert result.embeddings_created == 1
    assert embedding.embedding == [1.0, 0.0, 0.0]
    assert embedding.metadata["provider"] == "openrouter"
    assert embedding.metadata["provider_model"] == "unit-embedding"


def test_index_embeddings_rebuilds_on_content_hash_change_and_force(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Embedding rebuild example",
            source_key="pubmed",
            dedupe_key="pubmed:embedding-rebuild",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="KIT signaling appears in canine hemangiosarcoma.",
            content_hash="embedding-rebuild-original",
        )
    )

    first_result = index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    first_embedding = repo.list_text_embeddings(source_key="pubmed", embedding_model="local-hash-test")[0]
    repo.upsert_document_chunk(
        chunk.model_copy(
            update={
                "text_content": "MTOR signaling appears in canine hemangiosarcoma.",
                "content_hash": "embedding-rebuild-updated",
            }
        )
    )
    rebuild_result = index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    rebuilt_embedding = repo.list_text_embeddings(source_key="pubmed", embedding_model="local-hash-test")[0]
    unchanged_result = index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    forced_result = index_embeddings_for_repository(
        repo,
        source_key="pubmed",
        embedding_model="local-hash-test",
        force=True,
    )
    forced_embedding = repo.list_text_embeddings(source_key="pubmed", embedding_model="local-hash-test")[0]

    assert first_result.embeddings_created == 1
    assert rebuild_result.embeddings_updated == 1
    assert rebuild_result.embeddings_skipped == 0
    assert rebuilt_embedding.embedding_id == first_embedding.embedding_id
    assert rebuilt_embedding.content_hash != first_embedding.content_hash
    assert rebuilt_embedding.embedding != first_embedding.embedding
    assert rebuilt_embedding.metadata["chunk_content_hash"] == "embedding-rebuild-updated"
    assert unchanged_result.embeddings_skipped == 1
    assert forced_result.embeddings_updated == 1
    assert forced_embedding.embedding_id == first_embedding.embedding_id
    assert len(repo.list_text_embeddings(embedding_model="local-hash-test")) == 1


def test_sqlite_text_embedding_search_uses_json_vectors_and_upserts_by_chunk_model(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Embedding search example",
            source_key="pubmed",
            dedupe_key="pubmed:embedding-search",
        )
    )
    chunks = [
        repo.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=index,
                section_label="abstract",
                text_content=text,
                content_hash=f"embedding-search-{index}",
            )
        )
        for index, text in enumerate(
            [
                "VEGF signaling and angiogenesis in hemangiosarcoma.",
                "Chemotherapy toxicity monitoring in dogs.",
            ]
        )
    ]
    first = repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunks[0].id,
            research_object_id=object_id,
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash=chunks[0].content_hash,
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[1.0, 0.0, 0.0],
        )
    )
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunks[1].id,
            research_object_id=object_id,
            chunk_index=1,
            source_key="pubmed",
            object_type="publication",
            content_hash=chunks[1].content_hash,
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[0.0, 1.0, 0.0],
        )
    )

    results = repo.search_text_embeddings(
        TextEmbeddingSearchRequest(
            query_embedding=[0.95, 0.05, 0.0],
            embedding_model="unit-embedding-v1",
            source_key="pubmed",
            limit=2,
        )
    )

    updated = repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunks[0].id,
            research_object_id=object_id,
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash="embedding-search-0-updated",
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[0.0, 0.0, 1.0],
        )
    )
    updated_results = repo.search_text_embeddings(
        TextEmbeddingSearchRequest(query_embedding=[0.0, 0.0, 1.0], embedding_model="unit-embedding-v1")
    )

    assert [result.embedding.chunk_id for result in results] == [chunks[0].id, chunks[1].id]
    assert results[0].score > results[1].score
    assert updated.embedding_id == first.embedding_id
    assert updated.content_hash == "embedding-search-0-updated"
    assert len(repo.list_text_embeddings(embedding_model="unit-embedding-v1")) == 2
    assert updated_results[0].embedding.embedding_id == first.embedding_id


def test_service_search_research_chunks_uses_embeddings_without_returning_vectors(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGFA angiogenesis in canine hemangiosarcoma",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-embedding",
        )
    )
    vegf_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis VEGFA angiogenesis in canine hemangiosarcoma.",
            content_hash="retrieval-embedding-vegf",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=1,
            section_label="methods",
            text_content="Doxorubicin cardiotoxicity monitoring in dogs.",
            content_hash="retrieval-embedding-dox",
        )
    )
    index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")

    results = service.search_research_chunks(
        ResearchChunkSearchRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            embedding_model="local-hash-test",
            limit=2,
        )
    )
    payload = results.model_dump(mode="json")

    assert results.search_mode == "embedding"
    assert results.results[0].match_type == "embedding"
    assert results.results[0].chunk.id == vegf_chunk.id
    assert not _contains_key(payload, "embedding")


def test_service_search_research_chunks_hybrid_reranks_keyword_specific_hits(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    provider = LocalDeterministicEmbeddingProvider(embedding_model="local-hash-test")
    query = "sorafenib canine hemangiosarcoma safety dose limiting toxicity"
    generic_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="dataset",
            title="canine hemangiosarcoma cell lines",
            source_key="geo",
            dedupe_key="geo:retrieval-hybrid-generic",
        )
    )
    generic_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=generic_object_id,
            chunk_index=0,
            section_label="dataset",
            text_content="canine hemangiosarcoma cell lines and tissues",
            content_hash="retrieval-hybrid-generic",
        )
    )
    specific_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Sorafenib safety and dose limiting toxicity in canine hemangiosarcoma",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-hybrid-specific",
        )
    )
    specific_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=specific_object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Sorafenib canine hemangiosarcoma safety dose limiting toxicity evidence.",
            content_hash="retrieval-hybrid-specific",
        )
    )
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=generic_chunk.id,
            research_object_id=generic_object_id,
            chunk_index=0,
            source_key="geo",
            object_type="dataset",
            content_hash="retrieval-hybrid-generic-embedding",
            embedding_model="local-hash-test",
            embedding_dimensions=provider.embedding_dimensions,
            embedding=provider.embed_text(query),
        )
    )
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=specific_chunk.id,
            research_object_id=specific_object_id,
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash="retrieval-hybrid-specific-embedding",
            embedding_model="local-hash-test",
            embedding_dimensions=provider.embedding_dimensions,
            embedding=provider.embed_text("unrelated assay protocol"),
        )
    )

    results = service.search_research_chunks(
        ResearchChunkSearchRequest(
            query=query,
            embedding_model="local-hash-test",
            limit=2,
        )
    )

    assert results.search_mode == "embedding"
    assert results.results[0].chunk.id == specific_chunk.id
    assert results.results[1].chunk.id == generic_chunk.id


def test_embedding_bakeoff_scores_configured_models(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Sorafenib safety and coagulopathy in a dog",
            abstract="Sorafenib toxicity and dose monitoring in dogs.",
            source_key="pmc_oa",
            dedupe_key="pmc_oa:embedding-bakeoff",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Sorafenib dog safety toxicity coagulopathy evidence.",
            content_hash="embedding-bakeoff-chunk",
        )
    )
    index_embeddings_for_repository(repo, embedding_model="local-hash-test")

    report = run_embedding_bakeoff(
        repo,
        embedding_models=("local-hash-test",),
        benchmarks=(
            EmbeddingBenchmark(
                name="unit_sorafenib_dog_safety",
                query="sorafenib dog safety toxicity",
                expected_terms=("sorafenib", "dog", "safety", "toxicity"),
                preferred_source_keys=("pmc_oa",),
                expected_title_terms=("sorafenib", "dog"),
            ),
        ),
    )

    assert report["best_model"] == "local-hash-test"
    assert report["models"][0]["average_score"] > 0.8
    assert report["models"][0]["benchmarks"][0]["top_source_key"] == "pmc_oa"


def test_service_search_research_chunks_overfetches_stale_embedding_hits(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGFA angiogenesis overfetch example",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-overfetch",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis in canine hemangiosarcoma.",
            content_hash="retrieval-overfetch-valid",
        )
    )
    provider = LocalDeterministicEmbeddingProvider(embedding_model="local-hash-test")
    stale_vector = provider.embed_text("VEGFA angiogenesis")
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=uuid4(),
            research_object_id=object_id,
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash="retrieval-overfetch-stale",
            embedding_model="local-hash-test",
            embedding_dimensions=provider.embedding_dimensions,
            embedding=stale_vector,
        )
    )
    index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")

    results = service.search_research_chunks(
        ResearchChunkSearchRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            embedding_model="local-hash-test",
            limit=1,
        )
    )

    assert results.search_mode == "embedding"
    assert results.results[0].chunk.id == chunk.id


def test_service_search_research_chunks_falls_back_to_keyword_and_bounds_text(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Keyword fallback retrieval example",
            abstract="VEGFA angiogenesis context for canine HSA.",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-keyword",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis " + ("canine hemangiosarcoma " * 20),
            content_hash="retrieval-keyword-vegf",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=1,
            section_label="methods",
            text_content="Chemotherapy dosing schedule.",
            content_hash="retrieval-keyword-other",
        )
    )

    results = service.search_research_chunks(
        ResearchChunkSearchRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            limit=5,
            max_chunk_chars=220,
        )
    )

    assert results.search_mode == "keyword"
    assert results.results[0].match_type == "keyword"
    assert results.results[0].chunk.id == chunk.id
    assert len(results.results[0].chunk.text_content) == 220
    assert results.results[0].text_truncated is True


def test_service_keyword_retrieval_scores_before_final_limit(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    generic_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="dataset",
            title="canine hemangiosarcoma generic dataset",
            source_key="geo",
            dedupe_key="geo:retrieval-keyword-limit-generic",
        )
    )
    for index in range(150):
        repo.upsert_document_chunk(
            DocumentChunk(
                research_object_id=generic_object_id,
                chunk_index=index,
                section_label="dataset",
                text_content="canine hemangiosarcoma cell line metadata",
                content_hash=f"retrieval-keyword-limit-generic-{index}",
            )
        )
    specific_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Sorafenib safety dose limiting toxicity in a dog",
            source_key="pmc_oa",
            dedupe_key="pmc_oa:retrieval-keyword-limit-specific",
        )
    )
    specific_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=specific_object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Sorafenib safety dose limiting toxicity in a dog with sarcoma.",
            content_hash="retrieval-keyword-limit-specific",
        )
    )

    results = service.search_research_chunks(
        ResearchChunkSearchRequest(
            query="canine hemangiosarcoma sorafenib safety dose limiting toxicity",
            limit=1,
        )
    )

    assert results.search_mode == "keyword"
    assert results.results[0].chunk.id == specific_chunk.id


def test_service_get_chunk_context_and_research_object_are_bounded(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Chunk context example",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-context",
        )
    )
    chunks = [
        repo.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=index,
                section_label=f"section-{index}",
                text_content=text,
                content_hash=f"retrieval-context-{index}",
            )
        )
        for index, text in enumerate(
            [
                "Background chunk about canine hemangiosarcoma.",
                "Middle chunk mentions VEGFA receptor signaling in canine hemangiosarcoma.",
                "Follow-up chunk about translational validation.",
            ]
        )
    ]
    repo.upsert_entity_mention(
        EntityMention(
            research_object_id=object_id,
            chunk_id=chunks[1].id,
            chunk_index=1,
            section_label="section-1",
            source_key="pubmed",
            entity_type="target",
            canonical_name="VEGFA",
            normalized_key="target:vegfa",
            matched_text="VEGFA",
            matched_alias="VEGFA",
            chunk_char_start=22,
            chunk_char_end=27,
            resolver_name="unit",
            resolver_version="1",
            match_rule="unit",
        )
    )

    context = service.get_chunk_context(
        ChunkContextRequest(chunk_id=chunks[1].id, window=1, max_chunk_chars=220)
    )
    object_result = service.get_research_object(
        ResearchObjectReadRequest(research_object_id=object_id, max_chunks=2, max_chunk_chars=220)
    )

    assert context is not None
    assert context.chunk.id == chunks[1].id
    assert [chunk.id for chunk in context.before_chunks] == [chunks[0].id]
    assert [chunk.id for chunk in context.after_chunks] == [chunks[2].id]
    assert context.entity_mentions[0].canonical_name == "VEGFA"
    assert object_result is not None
    assert object_result.research_object.id == object_id
    assert [chunk.id for chunk in object_result.chunks] == [chunks[0].id, chunks[1].id]


def test_service_retrieval_smoke_chains_embedding_search_context_and_object(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Retrieval smoke example",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-smoke",
        )
    )
    target_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis in canine hemangiosarcoma retrieval smoke context.",
            content_hash="retrieval-smoke-target",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=1,
            section_label="methods",
            text_content="Doxorubicin dosing background.",
            content_hash="retrieval-smoke-other",
        )
    )
    index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")

    result = service.run_retrieval_smoke(
        RetrievalSmokeRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            embedding_model="local-hash-test",
            limit=2,
            require_embedding=True,
        )
    )

    assert result.passed is True
    assert result.errors == []
    assert result.search.search_mode == "embedding"
    assert result.selected_chunk_id == target_chunk.id
    assert result.selected_research_object_id == object_id
    assert result.chunk_context is not None
    assert result.chunk_context.chunk.id == target_chunk.id
    assert result.research_object is not None
    assert result.research_object.research_object.id == object_id


def test_service_retrieval_smoke_can_require_embeddings(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Retrieval smoke keyword fallback example",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-smoke-keyword",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis in canine hemangiosarcoma.",
            content_hash="retrieval-smoke-keyword",
        )
    )

    result = service.run_retrieval_smoke(
        RetrievalSmokeRequest(query="VEGFA angiogenesis", source_key="pubmed", require_embedding=True)
    )

    assert result.passed is False
    assert result.search.search_mode == "keyword"
    assert result.errors == ["expected embedding search, got keyword"]


def test_document_chunk_upsert_preserves_readable_stable_chunk_id(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Stable chunk id retrieval example",
            source_key="pubmed",
            dedupe_key="pubmed:stable-chunk-id",
        )
    )
    original_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Original text.",
            content_hash="stable-chunk-original",
        )
    )
    replacement_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis stable chunk context.",
            content_hash="stable-chunk-replacement",
        )
    )

    search = service.search_research_chunks(
        ResearchChunkSearchRequest(query="VEGFA angiogenesis", source_key="pubmed", limit=1)
    )
    index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    smoke = service.run_retrieval_smoke(
        RetrievalSmokeRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            embedding_model="local-hash-test",
            require_embedding=True,
        )
    )

    assert replacement_chunk.id == original_chunk.id
    assert repo.get_document_chunk(replacement_chunk.id) is not None
    assert search.results[0].chunk.id == original_chunk.id
    assert smoke.passed is True
    assert smoke.selected_chunk_id == original_chunk.id
    assert smoke.chunk_context is not None
    assert smoke.chunk_context.chunk.content_hash == "stable-chunk-replacement"


def test_mcp_retrieval_tool_helpers_dump_bounded_read_results(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="MCP retrieval example",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-mcp",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Canine hemangiosarcoma retrieval context for MCP.",
            content_hash="retrieval-mcp-chunk",
        )
    )
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)

    search_payload = mcp_server.search_research_chunks_tool(
        query="hemangiosarcoma retrieval",
        source_key="pubmed",
        limit=1,
    )
    chunk_payload = mcp_server.get_chunk_context_tool(str(chunk.id), window=0)
    object_payload = mcp_server.get_research_object_tool(str(object_id), max_chunks=1)

    assert search_payload["search_mode"] == "keyword"
    assert search_payload["results"][0]["chunk"]["id"] == str(chunk.id)
    assert chunk_payload["chunk"]["id"] == str(chunk.id)
    assert object_payload["research_object"]["id"] == str(object_id)
    assert len(object_payload["chunks"]) == 1


def test_mcp_full_text_triage_helper_dumps_action(monkeypatch, tmp_path):
    service = make_service(tmp_path)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)

    payload = mcp_server.triage_full_text_issue_tool(
        source_key="europe_pmc",
        stage="dagster_run",
        error_message="Timed out while running hosted refresh",
        runtime_seconds=2700,
        timeout_seconds=2700,
        raw_records=10,
        research_objects=10,
    )

    assert payload["triage_name"] == "full_text_triage_agent"
    assert payload["action"] == "reduce_batch_size"
    assert payload["should_retry"] is True
    assert payload["should_block_schedule"] is True


def test_mcp_agent_run_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-agent-runs.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)

    payload = mcp_server.run_full_text_ops_tool(
        source_keys=["europe_pmc"],
        partition_date="2026-04-27",
        review_mode="deterministic_only",
    )
    run_payload = mcp_server.get_agent_run_tool(payload["agent_run_id"])
    runs_payload = mcp_server.list_agent_runs_tool(agent_name="full_text_ops_agent")

    assert payload["agent_run_id"]
    assert payload["actions"]
    assert run_payload["agent_run_id"] == payload["agent_run_id"]
    assert runs_payload[0]["agent_run_id"] == payload["agent_run_id"]


def test_mcp_agent_performance_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-agent-performance.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)
    run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="full_text_ops_agent",
            status=RunStatus.COMPLETED,
            output_payload={"schedule_readiness": "keep_stopped"},
        )
    )
    repo.create_agent_run_review(AgentRunReviewRecord(agent_run_id=run.agent_run_id, verdict="needs_followup"))

    def fake_openrouter(model_name, review_payload):
        return {
            "text": json.dumps(
                {
                    "verdict": "needs_followup",
                    "confidence": 0.7,
                    "rationale": "Needs partition evidence.",
                    "strengths": ["Useful blocker."],
                    "failure_modes": [],
                    "recommended_followup_actions": ["run_source_date_partition"],
                    "rubric_scores": {"evidence_paths": 0.7},
                }
            ),
            "metadata": {"provider": "openrouter", "model_name": model_name},
        }

    monkeypatch.setattr(agent_performance, "_openrouter_review_model", fake_openrouter)

    report_payload = mcp_server.agent_performance_report_tool(limit=10)
    evaluation_payload = mcp_server.run_agent_performance_evaluation_tool(limit=1)

    assert report_payload["reviewed_run_count"] == 1
    assert report_payload["rows"]
    assert evaluation_payload["review_created_count"] == 1


def test_mcp_research_lead_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-research-leads.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="HSA institutional article",
            url="https://example.edu/hsa",
            lead_type="institutional_article",
            source_key="x_linked_article",
        )
    )

    fetched = mcp_server.get_research_lead_tool(str(lead.lead_id))
    listed = mcp_server.list_research_leads_tool(status="new")

    assert fetched["lead_id"] == str(lead.lead_id)
    assert listed[0]["identity_key"] == lead.identity_key


def test_mcp_validation_gap_source_pack_tool_dumps_json_safe_payload(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-validation-gap-source-pack.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="PK/PD evidence: canine sorafenib exposure",
            status="new",
            priority=25,
            reason="Need canine sorafenib pharmacokinetic exposure and dose evidence.",
            topic_tags=["validation_gap", "pkpd"],
            suggested_sources=["pubmed", "chembl"],
            metadata={
                "evidence_gap_resolver": {
                    "lane": "pkpd",
                    "gap_text": "Need canine sorafenib pharmacokinetic exposure and dose evidence.",
                    "task_type": "expert_review",
                    "validation_type": "expert_review",
                }
            },
        )
    )

    payload = mcp_server.build_validation_gap_source_pack_tool(
        lead_ids=[str(lead.lead_id)],
        source_keys=["pubmed", "chembl"],
        max_queries_per_lane=5,
        dry_run=True,
    )

    assert payload["agent_run_id"]
    assert payload["query_count"] == 2
    assert {query["source_key"] for query in payload["queries"]} == {"pubmed", "chembl"}


def test_mcp_research_brief_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-research-briefs.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)
    record = repo.upsert_research_brief(
        ResearchBriefRecord(
            agent_run_id=uuid4(),
            topic="VEGF therapy in canine hemangiosarcoma",
            disease_scope="canine hemangiosarcoma",
            source_key="pubmed",
            review_mode="deterministic_only",
            final_brief="Stored MCP synthesis [C1].",
            result_payload={"final_brief": "Stored MCP synthesis [C1]."},
            citation_count=1,
            finding_count=1,
        )
    )

    fetched = mcp_server.get_research_brief_tool(str(record.brief_id))
    listed = mcp_server.list_research_briefs_tool(topic_query="vegf")

    assert fetched["brief_id"] == str(record.brief_id)
    assert fetched["result_payload"]["final_brief"] == "Stored MCP synthesis [C1]."
    assert listed[0]["brief_id"] == str(record.brief_id)


def test_mcp_research_brief_evaluation_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-research-brief-evaluations.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)
    record = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="VEGF therapy in canine hemangiosarcoma",
            disease_scope="canine hemangiosarcoma",
            source_key="pubmed",
            review_mode="deterministic_only",
            final_brief="Stored synthesis without citations.",
            result_payload={
                "topic": "VEGF therapy in canine hemangiosarcoma",
                "disease_scope": "canine hemangiosarcoma",
                "final_brief": "Stored synthesis without citations.",
                "citations": [],
                "perspective_reports": [],
                "ranked_hypotheses": [],
                "unresolved_questions": [],
                "evidence": {},
                "errors": [],
            },
        )
    )

    evaluated = mcp_server.evaluate_research_brief_tool(brief_id=str(record.brief_id))
    fetched = mcp_server.get_research_brief_evaluation_tool(evaluated["evaluation_id"])
    listed = mcp_server.list_research_brief_evaluations_tool(readiness="blocked")

    assert evaluated["brief_id"] == str(record.brief_id)
    assert evaluated["readiness"] == "blocked"
    assert fetched["evaluation_id"] == evaluated["evaluation_id"]
    assert listed[0]["evaluation_id"] == evaluated["evaluation_id"]


def test_mcp_validation_plan_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-validation-plans.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)
    object_id = uuid4()
    chunk_id = uuid4()
    citation = ResearchBriefCitation(
        citation_id="C1",
        chunk_id=chunk_id,
        research_object_id=object_id,
        source_key="pubmed",
        quote="VEGF target validation is discussed.",
    )
    finding = ResearchBriefFinding(
        claim="VEGF target validation should be prioritized in canine HSA.",
        stance="opportunity",
        citations=["C1"],
        evidence_strength="medium",
        reasoning="The cited evidence supports a testable translational target path.",
    )
    result = ResearchBriefResult(
        brief_id=uuid4(),
        topic="VEGF target validation in canine HSA",
        disease_scope="canine hemangiosarcoma",
        final_brief="VEGF target validation is a candidate path [C1].",
        ranked_hypotheses=[finding],
        citations=[citation],
    )
    brief_record = repo.upsert_research_brief(
        ResearchBriefRecord(
            brief_id=result.brief_id,
            topic=result.topic,
            disease_scope=result.disease_scope,
            source_key="pubmed",
            final_brief=result.final_brief,
            result_payload=result.model_dump(mode="json"),
            citation_count=1,
            hypothesis_count=1,
        )
    )
    evaluation = repo.upsert_research_brief_evaluation(
        ResearchBriefEvaluationRecord(
            brief_id=brief_record.brief_id,
            topic=brief_record.topic,
            source_key=brief_record.source_key,
            overall_score=0.85,
            passes_quality_bar=True,
            readiness="ready_for_hypothesis_review",
            result_payload={"readiness": "ready_for_hypothesis_review"},
        )
    )

    planned = mcp_server.plan_validation_tool(evaluation_id=str(evaluation.evaluation_id))
    fetched = mcp_server.get_validation_plan_tool(planned["plan_id"])
    listed = mcp_server.list_validation_plans_tool(readiness="ready_for_expert_review")
    queued = mcp_server.queue_validation_requests_tool(planned["plan_id"], dry_run=False)
    queue_item = queued["queue_items"][0]
    queue_fetched = mcp_server.get_validation_request_queue_item_tool(queue_item["queue_item_id"])
    queue_listed = mcp_server.list_validation_request_queue_tool(status="needs_approval")
    approved = mcp_server.approve_validation_request_tool(
        queue_item["queue_item_id"],
        approved_by="unit-test",
    )
    dispatched = mcp_server.dispatch_validation_request_tool(
        queue_item["queue_item_id"],
        model_profile="deterministic_only",
    )

    assert planned["agent_run_id"]
    assert planned["tasks"]
    assert fetched["plan_id"] == planned["plan_id"]
    assert listed[0]["plan_id"] == planned["plan_id"]
    assert queued["queued_count"] == len(queued["queue_items"])
    assert queued["queued_count"] >= 1
    assert queue_fetched["queue_item_id"] == queue_item["queue_item_id"]
    assert queue_listed[0]["queue_item_id"] == queue_item["queue_item_id"]
    assert approved["status"] == "approved"
    assert dispatched["status"] == "completed"
    assert dispatched["last_run_id"]
    assert dispatched["metadata"]["validation_agent_result"]["decision"] in {"promote", "hold", "demote"}


def test_mcp_research_brief_queue_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-research-brief-queue.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)

    queued = mcp_server.queue_research_brief_tool(
        topic="VEGF therapy in canine hemangiosarcoma",
        source_key="pubmed",
        review_mode="deterministic_only",
    )
    fetched = mcp_server.get_research_brief_queue_item_tool(queued["queue_item_id"])
    listed = mcp_server.list_research_brief_queue_tool(status="queued")
    failed = repo.update_research_brief_queue_item(
        queued["queue_item_id"],
        status="failed",
        attempts=1,
        last_error="model timeout",
    )
    assert failed is not None
    requeued = mcp_server.requeue_research_brief_queue_item_tool(queued["queue_item_id"], priority=7)
    failed_again = repo.update_research_brief_queue_item(
        queued["queue_item_id"],
        status="failed",
        attempts=2,
        last_error="superseded",
    )
    assert failed_again is not None
    maintenance = mcp_server.maintain_research_brief_queue_tool(
        queue_item_ids=[queued["queue_item_id"]],
        statuses=["failed"],
        max_updated_age_hours=0,
        dry_run=True,
    )
    completed = repo.update_research_brief_queue_item(queued["queue_item_id"], status="completed")
    assert completed is not None
    archived = mcp_server.archive_research_brief_queue_item_tool(queued["queue_item_id"])
    repo.upsert_research_lead(
        ResearchLeadRecord(
            title="PDGF biomarker lead",
            lead_type="linked_article",
            status="new",
            priority=30,
            suggested_sources=["pubmed"],
        )
    )
    batch = mcp_server.queue_research_brief_batch_tool(mode="research_leads", limit=1)
    followup_lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evidence light linked article",
            lead_type="linked_article",
            status="followup",
            source_key="x_linked_article",
        )
    )
    resolver = mcp_server.resolve_research_followups_tool(
        lead_ids=[str(followup_lead.lead_id)],
        search_missing_identifiers=False,
    )
    command_center = mcp_server.command_center_tool(include_source_health=False, queue_limit=5, lead_limit=5)

    assert queued["queue_item_id"]
    assert fetched["queue_item_id"] == queued["queue_item_id"]
    assert listed[0]["identity_key"] == queued["identity_key"]
    assert requeued["status"] == "queued"
    assert requeued["priority"] == 7
    assert requeued["last_error"] is None
    assert maintenance["candidate_count"] == 1
    assert maintenance["dry_run"] is True
    assert maintenance["queue_items"][0]["queue_item_id"] == queued["queue_item_id"]
    assert archived["status"] == "archived"
    assert archived["metadata"]["queue_control"]["previous_status"] == "completed"
    assert batch["queued_count"] == 1
    assert batch["queue_items"][0]["metadata"]["batch_queue"]["origin"] == "research_lead"
    assert resolver["leads_seen"] == 1
    assert resolver["manual_research_required"] == 1
    assert command_center["summary"]["brief_queue_total"] >= 1
    assert command_center["recommendations"]


def test_mcp_retrieval_smoke_helper_dumps_full_read_chain(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="MCP retrieval smoke example",
            source_key="pubmed",
            dedupe_key="pubmed:mcp-retrieval-smoke",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis retrieval smoke context for MCP.",
            content_hash="mcp-retrieval-smoke",
        )
    )
    index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)

    payload = mcp_server.run_retrieval_smoke_tool(
        query="VEGFA angiogenesis",
        source_key="pubmed",
        embedding_model="local-hash-test",
        require_embedding=True,
    )

    assert payload["passed"] is True
    assert payload["selected_chunk_id"] == str(chunk.id)
    assert payload["search"]["search_mode"] == "embedding"
    assert payload["chunk_context"]["chunk"]["id"] == str(chunk.id)
    assert payload["research_object"]["research_object"]["id"] == str(object_id)


def test_backfill_papers_json_creates_object_and_chunk(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    papers_path = tmp_path / "papers.json"
    papers_path.write_text(
        """
        [
          {
            "pmid": "123",
            "doi": "10.1234/hsa",
            "title": "Canine hemangiosarcoma backfill",
            "abstract": "This abstract mentions canine hemangiosarcoma.",
            "journal": "Example Journal",
            "year": "2026",
            "source": "pubmed",
            "url": "https://pubmed.ncbi.nlm.nih.gov/123/"
          }
        ]
        """
    )

    result = backfill_papers_json(repo, papers_path)
    coverage = repo.coverage_summary()

    assert result.raw_records == 1
    assert result.research_objects == 1
    assert result.document_chunks == 1
    assert coverage["document_chunks"] == 1


def test_backfill_deep_dives_creates_knowledge_entry_chunks(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    deep_dives = tmp_path / "deep_dives"
    deep_dives.mkdir()
    (deep_dives / "treatment_example.md").write_text(
        "# Treatment Example\n\n## TL;DR\n\nThis is a local knowledge entry.\n\n## Detail\n\nMore text."
    )

    result = backfill_deep_dives(repo, deep_dives)
    objects = repo.list_research_objects(object_type="knowledge_entry")

    assert result.raw_records == 1
    assert result.research_objects == 1
    assert result.document_chunks == 1
    assert objects[0].metadata["track"] == "treatment"


def test_local_claim_extractor_creates_draft_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    papers_path = tmp_path / "papers.json"
    papers_path.write_text(
        """
        [
          {
            "pmid": "123",
            "title": "Propranolol and VEGF in canine hemangiosarcoma",
            "abstract": "Canine hemangiosarcoma studies discuss propranolol with VEGF and angiogenesis.",
            "journal": "Example Journal",
            "year": "2026",
            "source": "pubmed"
          }
        ]
        """
    )
    backfill_papers_json(repo, papers_path)

    result = extract_claims_for_repository(repo, source_key="current_papers")
    claims = repo.search_claims(
        ClaimSearchRequest(query="propranolol", species="canine", min_confidence=0.1, include_drafts=True)
    )

    assert result.chunks_seen == 1
    assert result.claims_written >= 1
    assert any(claim.metadata.get("extraction_status") == "draft" for claim in claims)


def test_local_claim_extractor_attaches_persisted_entity_mentions(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    title = "Propranolol and VEGF in canine hemangiosarcoma"
    obj_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title=title,
            abstract="Canine hemangiosarcoma studies discuss propranolol with VEGF and angiogenesis.",
            source_key="pubmed",
        )
    )
    for chunk in chunk_text(obj_id, title, section_label="title_abstract"):
        repo.upsert_document_chunk(chunk)
    resolved = resolve_entities_for_repository(repo, source_key="pubmed")
    mentions = repo.list_entity_mentions(source_key="pubmed")

    result = extract_claims_for_repository(repo, source_key="pubmed")
    claims = repo.search_claims(
        ClaimSearchRequest(query="propranolol", species="canine", min_confidence=0.1, include_drafts=True)
    )

    assert resolved.mentions_upserted >= 2
    assert result.claims_written >= 1
    assert claims
    assert set(claims[0].metadata["source_entity_mention_ids"]) == {str(mention.mention_id) for mention in mentions}
    assert set(claims[0].metadata["source_entity_canonical_names"]) >= {"propranolol", "VEGFA"}
    assert set(claims[0].metadata["source_entity_types"]) >= {"compound", "target"}


def test_local_claim_extractor_handles_human_angiosarcoma_analogs(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    title = "Paclitaxel targets VEGF signaling in human angiosarcoma"
    obj_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title=title,
            abstract="Human angiosarcoma studies discuss paclitaxel with VEGF and angiogenesis.",
            source_key="pubmed",
        )
    )
    for chunk in chunk_text(obj_id, title, section_label="title_abstract"):
        repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, source_key="pubmed")
    claims = repo.search_claims(
        ClaimSearchRequest(query="paclitaxel", species="human", min_confidence=0.1, include_drafts=True)
    )

    assert result.claims_written >= 1
    assert any(claim.metadata.get("context_key") == "human_angiosarcoma_analog" for claim in claims)


def test_local_claim_extractor_creates_sparse_scholarly_context_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    europe_pmc_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Primary vaginal angiosarcoma case report",
            source_key="europe_pmc",
        )
    )
    crossref_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Vascular sarcoma clinical series",
            source_key="crossref",
        )
    )
    for object_id, text in (
        (europe_pmc_id, "Primary vaginal angiosarcoma case report."),
        (crossref_id, "Vascular sarcoma clinical series."),
    ):
        for chunk in chunk_text(object_id, text, section_label="title_abstract"):
            repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, limit=10)
    claims = repo.search_claims(ClaimSearchRequest(query="source context", min_confidence=0.1, include_drafts=True, limit=10))
    statements = [claim.statement for claim in claims]

    assert result.claims_written == 2
    assert any("Europe PMC record provides human angiosarcoma" in statement for statement in statements)
    assert any("Crossref record provides human angiosarcoma" in statement for statement in statements)


def test_unpaywall_claim_extractor_creates_oa_discovery_context_claim(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Human angiosarcoma open access review",
            source_key="unpaywall",
            metadata={
                "license_policy": "metadata_and_open_access_location_links",
                "best_oa_location": {
                    "url_for_landing_page": "https://example.org/article",
                    "url_for_pdf": "https://example.org/article.pdf",
                    "license": "cc-by",
                },
            },
        )
    )
    for chunk in chunk_text(
        object_id,
        "Human angiosarcoma open access review. OA status: gold. License: cc-by.",
        section_label="oa_discovery_metadata",
    ):
        repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, source_key="unpaywall", limit=10)
    claims = repo.list_claims(source_key="unpaywall", include_seed_claims=True, limit=10)

    assert result.claims_written == 1
    assert claims[0].statement.startswith("Unpaywall record provides human angiosarcoma")
    assert claims[0].metadata["rule_key"] == "source-context:human_angiosarcoma_analog"


def test_local_claim_extractor_creates_dataset_source_context_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    geo_id = repo.upsert_research_object(
        ResearchObject(
            object_type="dataset",
            title="Canine hemangiosarcoma expression dataset",
            source_key="geo",
        )
    )
    sra_id = repo.upsert_research_object(
        ResearchObject(
            object_type="dataset",
            title="Dog hemangiosarcoma sequence runs",
            source_key="sra",
        )
    )
    icdc_id = repo.upsert_research_object(
        ResearchObject(
            object_type="dataset",
            title="ICDC canine case CASE-1: Hemangiosarcoma",
            source_key="icdc",
        )
    )
    for object_id, text in (
        (geo_id, "Canine hemangiosarcoma expression dataset."),
        (sra_id, "Dog hemangiosarcoma sequence runs."),
        (icdc_id, "Diagnosis: Hemangiosarcoma. Species: canine."),
    ):
        for chunk in chunk_text(object_id, text, section_label="dataset_metadata"):
            repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, limit=10)
    claims = repo.search_claims(ClaimSearchRequest(query="source context", min_confidence=0.1, include_drafts=True, limit=10))
    statements = [claim.statement for claim in claims]

    assert result.claims_written == 3
    assert any("GEO record provides canine HSA source context" in statement for statement in statements)
    assert any("SRA record provides canine HSA source context" in statement for statement in statements)
    assert any("ICDC record provides canine HSA source context" in statement for statement in statements)


def test_local_claim_extractor_creates_structured_chembl_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    title = "TOCERANIB IC50 against Vascular endothelial growth factor receptor 2"
    obj_id = repo.upsert_research_object(
        ResearchObject(
            object_type="bioactivity_assay",
            title=title,
            abstract="Inhibition of Vascular endothelial growth factor receptor 2.",
            source_key="chembl",
            identifiers={"chembl_activity_id": "726668", "chembl_target_id": "CHEMBL279"},
            metadata={
                "query_term": "toceranib",
                "molecule_pref_name": "TOCERANIB",
                "target_pref_name": "Vascular endothelial growth factor receptor 2",
                "target_gene": "KDR",
                "target_category": "vegf_angiogenesis",
                "target_organism": "Homo sapiens",
                "assay_type": "B",
                "standard_type": "IC50",
                "standard_relation": "=",
                "standard_value": "60.0",
                "standard_units": "nM",
                "pchembl_value": "7.22",
                "pchembl_numeric": 7.22,
            },
        )
    )
    for chunk in chunk_text(obj_id, title, section_label="bioactivity_assay"):
        repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, source_key="chembl")
    claims = repo.search_claims(
        ClaimSearchRequest(query="toceranib", species="human", min_confidence=0.1, include_drafts=True)
    )

    assert result.chunks_seen == 1
    assert result.claims_written == 1
    assert claims[0].claim_type == "compound_modulates_target"
    assert claims[0].evidence_level == "in_vitro"
    assert claims[0].metadata["context_key"] == "chembl_target_bioactivity"
    assert "pChEMBL 7.22" in claims[0].statement


def test_local_claim_extractor_creates_structured_source_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    pubchem_id = repo.upsert_research_object(
        ResearchObject(
            object_type="compound_record",
            title="Propranolol",
            source_key="pubchem",
            identifiers={"pubchem_cid": "4946", "inchikey": "AQHHHDLHHXJYJD-UHFFFAOYSA-N"},
            metadata={"identity_match": {"identity_verified": True}},
        )
    )
    uniprot_id = repo.upsert_research_object(
        ResearchObject(
            object_type="structure",
            title="KDR Vascular endothelial growth factor receptor 2",
            source_key="uniprot",
            identifiers={"uniprot_accession": "P35968"},
            metadata={
                "target_gene": "KDR",
                "organism": "Homo sapiens",
                "species_scope": "human",
                "reviewed": True,
                "alphafold_ids": ["AF-P35968-F1"],
            },
        )
    )
    rcsb_id = repo.upsert_research_object(
        ResearchObject(
            object_type="structure",
            title="Human VEGFR2 kinase domain",
            source_key="rcsb_pdb",
            identifiers={"pdb_id": "3VHE"},
            metadata={"target_gene": "KDR", "experimental_methods": ["X-RAY DIFFRACTION"]},
        )
    )
    openfda_id = repo.upsert_research_object(
        ResearchObject(
            object_type="safety_report",
            title="openFDA animal adverse event for Doxorubicin in Dog",
            source_key="openfda_animal_events",
            identifiers={"openfda_report_id": "US-FDA-CVM-2026-0001"},
            metadata={
                "matched_drug_name": "Doxorubicin",
                "species": "Dog",
                "reaction_terms": ["Vomiting", "Neutropenia"],
                "serious_ae": "true",
            },
        )
    )
    for object_id, label in (
        (pubchem_id, "compound_metadata"),
        (uniprot_id, "protein_target_metadata"),
        (rcsb_id, "structure_metadata"),
        (openfda_id, "safety_report_metadata"),
    ):
        for chunk in chunk_text(object_id, "structured source text", section_label=label):
            repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, limit=10)
    claims = repo.search_claims(ClaimSearchRequest(min_confidence=0.1, include_drafts=True, limit=20))
    statements = [claim.statement for claim in claims]

    assert result.claims_written == 4
    assert any("PubChem compound identity CID 4946" in statement for statement in statements)
    assert any("UniProtKB target metadata for Homo sapiens" in statement for statement in statements)
    assert any("RCSB PDB contains experimental structure 3VHE" in statement for statement in statements)
    assert any("openFDA animal adverse event signal reports in Dog" in statement for statement in statements)


def test_claim_curator_agent_promotes_supported_draft_claim(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    papers_path = tmp_path / "papers.json"
    papers_path.write_text(
        """
        [
          {
            "pmid": "123",
            "title": "VEGF in canine hemangiosarcoma",
            "abstract": "Canine hemangiosarcoma studies discuss VEGF and angiogenesis.",
            "journal": "Example Journal",
            "year": "2026",
            "source": "pubmed"
          },
          {
            "pmid": "124",
            "title": "Canine hemangiosarcoma angiogenesis",
            "abstract": "Canine hemangiosarcoma work again discusses VEGF and angiogenesis.",
            "journal": "Example Journal",
            "year": "2026",
            "source": "pubmed"
          }
        ]
        """
    )
    backfill_papers_json(repo, papers_path)
    extract_claims_for_repository(repo, source_key="current_papers")

    result = ClaimCuratorAgent(repo).curate(ClaimCurationRequest(limit=20, promote_threshold=0.5))
    visible_claims = repo.search_claims(ClaimSearchRequest(query="VEGFA", species="canine", min_confidence=0.1))

    assert result.claims_seen >= 2
    assert result.promoted >= 1
    assert result.merged_duplicates >= 1
    assert any(claim.metadata["curation_status"] == "promote" for claim in visible_claims)


def test_claim_curator_keeps_pmc_oa_source_context_review_only(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Licensed full text source context",
            source_key="pmc_oa",
        )
    )
    text = "Human angiosarcoma source context. " + ("Licensed full text background. " * 12)
    for index in range(6):
        repo.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=index,
                section_label="full_text",
                text_content=text,
                content_hash=f"pmc-oa-source-context-{index}",
            )
        )

    extract_claims_for_repository(repo, source_key="pmc_oa")
    result = ClaimCuratorAgent(repo).curate(ClaimCurationRequest(source_key="pmc_oa", limit=20, promote_threshold=0.5))
    review_decisions = [item for item in result.decisions if item.decision == "needs_review"]

    assert result.promoted == 0
    assert result.needs_review == 1
    assert result.merged_duplicates == 5
    assert review_decisions
    assert "source-context triage claim is review-only" in review_decisions[0].reasons
    assert "licensed full-text chunk has substantive snippet" in review_decisions[0].reasons


def test_claim_curator_downgrades_stale_source_context_promotions(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Stale source context",
            source_key="crossref",
        )
    )
    claim_id = uuid4()
    repo.upsert_claim(
        ClaimSearchResult(
            claim_id=claim_id,
            statement="Crossref record provides canine-human comparative angiosarcoma/HSA source context relevant to HSA evidence triage.",
            claim_type=ClaimType.OTHER,
            direction=ClaimDirection.NEUTRAL,
            confidence=0.7,
            evidence_level=EvidenceLevel.UNKNOWN,
            source_object_id=object_id,
            support_count=1,
            metadata={
                "curation_status": "promote",
                "curation_score": 0.7,
                "extraction_status": "curated",
                "rule_key": "source-context:canine_human_comparative",
                "context_key": "canine_human_comparative",
                "source_chunk_id": str(uuid4()),
            },
        )
    )

    result = ClaimCuratorAgent(repo).curate(ClaimCurationRequest(source_key="crossref", limit=20))
    updated = repo.get_claim(claim_id)

    assert result.needs_review == 1
    assert updated is not None
    assert updated.metadata["curation_status"] == "needs_review"
    assert updated.metadata["extraction_status"] == "draft"
    assert updated.confidence == 0.49


def test_source_scout_prioritizes_zero_coverage_bridges(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()

    result = SourceScoutAgent(repo).scout(SourceScoutRequest(max_recommendations=5))
    keys = [recommendation.source_key for recommendation in result.recommendations]

    assert "pubmed" in keys
    assert "europe_pmc" in keys
    assert result.recommendations[0].status == "coverage_gap"
    assert result.next_actions


def test_scrape_profiles_keep_avma_approval_gated():
    profiles = {profile.source_key: profile for profile in list_scrape_profiles()}

    assert "avma_vctr" in profiles
    assert profiles["avma_vctr"].approval_required is True
    assert profiles["avma_vctr"].enabled is False
    assert profiles["avma_vctr"].robots_policy == "unknown"
    assert profiles["avma_vctr"].parser == "avma_vctr"
    assert profiles["x_linked_article"].approval_required is True
    assert profiles["x_linked_article"].enabled is False
    assert profiles["x_linked_article"].parser == "generic_html"


def test_scrape_bridge_refuses_approval_gated_fetch_without_approval(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    result = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts").fetch(
        ScrapeFetchRequest(
            source_key="avma_vctr",
            urls=["https://veterinaryclinicaltrials.org/"],
        )
    )

    assert result.fetched_pages == 0
    assert result.artifact_ids == []
    assert "requires explicit approval" in result.errors[0]


def test_disabled_scrape_source_requires_profile_review_before_approved_fetch(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    result = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts").fetch(
        ScrapeFetchRequest(
            source_key="avma_vctr",
            urls=["https://veterinaryclinicaltrials.org/"],
            approved_by="unit-test",
        )
    )

    assert result.fetched_pages == 0
    assert "requires source profile review" in result.errors[0]


def test_scrape_profile_review_is_persisted(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    review = bridge.review_profile(
        ScrapeProfileReviewRequest(
            source_key="avma_vctr",
            robots_policy="reviewed",
            approved_for_fetch=True,
            reviewed_by="unit-test",
            review_note="robots and storage policy reviewed",
        )
    )

    assert review.approved_for_fetch is True
    assert review.robots_policy == "reviewed"
    assert repo.get_scrape_profile_review("avma_vctr").reviewed_by == "unit-test"


def test_scrape_bridge_stores_snapshot_and_parses_generic_html(tmp_path, monkeypatch):
    html_path = tmp_path / "trial.html"
    html_path.write_text(
        """
        <html>
          <head><title>Canine Hemangiosarcoma Trial</title></head>
          <body><a href="/trial/1">Trial detail</a></body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="generic_html",
        storage_policy="metadata_only",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    fetch = bridge.fetch(ScrapeFetchRequest(source_key="test_scraper", urls=[html_path.as_uri()]))
    artifact = repo.get_artifact(fetch.artifact_ids[0])
    parse = bridge.parse("test_scraper")

    assert fetch.fetched_pages == 1
    assert artifact is not None
    assert artifact.artifact_type == "scrape_snapshot"
    assert artifact.metadata["source_key"] == "test_scraper"
    assert artifact.metadata["requires_review"] is True
    assert parse.artifacts_seen == 1
    assert parse.parsed_records == 1
    assert len(parse.review_ids) == 1
    assert parse.records[0].title == "Canine Hemangiosarcoma Trial"
    assert parse.records[0].record_type == "veterinary_trial"
    assert parse.records[0].review_status == "needs_review"
    reviews = repo.list_scrape_reviews(source_key="test_scraper", review_status="needs_review")
    assert len(reviews) == 1
    assert reviews[0].title == "Canine Hemangiosarcoma Trial"


def test_x_linked_article_parser_extracts_primary_source_links(tmp_path, monkeypatch):
    html_path = tmp_path / "article.html"
    html_path.write_text(
        """
        <html>
          <head><title>Angiosarcoma genomic landscape</title></head>
          <body>
            <a href="https://pubmed.ncbi.nlm.nih.gov/12345678/">PubMed</a>
            <a href="https://doi.org/10.1158/0008-5472.CAN-26-0001">Cancer Research DOI</a>
            Clinical trial NCT12345678 is also mentioned.
            A trailing delimiter should normalize: 10.1186/s40425-017-0263-0&
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="x_linked_article",
        display_name="X Linked Article Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="generic_html",
        storage_policy="metadata_and_link_review",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="x_linked_article", urls=[html_path.as_uri()]))
    record = bridge.parse("x_linked_article").records[0]
    source_links = record.fields["primary_source_links"]

    assert record.record_type == "publication"
    assert record.parser_confidence >= 0.55
    assert {
        (link["recommended_source_key"], link["identifier_type"], link["identifier"])
        for link in source_links
    } >= {
        ("pubmed", "pmid", "12345678"),
        ("crossref", "doi", "10.1158/0008-5472.CAN-26-0001"),
        ("crossref", "doi", "10.1186/s40425-017-0263-0"),
        ("clinicaltrials_gov", "nct", "NCT12345678"),
    }
    assert all(not link["identifier"].endswith("&") for link in source_links)


def test_x_linked_article_parser_extracts_metadata_identifiers(tmp_path, monkeypatch):
    html_path = tmp_path / "article-metadata.html"
    html_path.write_text(
        """
        <html>
          <head>
            <meta name="citation_title" content="Angiosarcoma translational trial">
            <meta name="citation_doi" content="10.1186/s40425-019-0689-7">
            <meta name="citation_pmid" content="31395100">
            <script type="application/ld+json">
            {
              "@type": "ScholarlyArticle",
              "headline": "JSON-LD title",
              "identifier": ["PMC6686562"],
              "datePublished": "2019-08-01"
            }
            </script>
          </head>
          <body><p>No visible primary links.</p></body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="x_linked_article",
        display_name="X Linked Article Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="generic_html",
        storage_policy="metadata_and_link_review",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="x_linked_article", urls=[html_path.as_uri()]))
    record = bridge.parse("x_linked_article").records[0]

    assert record.title == "Angiosarcoma translational trial"
    assert record.fields["article_metadata"]["doi"] == "10.1186/s40425-019-0689-7"
    assert record.fields["article_metadata"]["pmid"] == "31395100"
    assert record.fields["article_metadata"]["pmcid"] == "PMC6686562"
    assert {
        (link["recommended_source_key"], link["identifier_type"], link["identifier"])
        for link in record.fields["primary_source_links"]
    } >= {
        ("crossref", "doi", "10.1186/s40425-019-0689-7"),
        ("pubmed", "pmid", "31395100"),
        ("pmc_oa", "pmcid", "PMC6686562"),
    }


def test_x_linked_article_parser_keeps_context_separate_when_no_primary_identifier(tmp_path, monkeypatch):
    html_path = tmp_path / "article-context.html"
    html_path.write_text(
        """
        <html>
          <head><title>Angiosarcoma frontier</title></head>
          <body>
            <p>Now, for the first time, the team has performed comprehensive genomic
            profiling of angiosarcoma cells, analyzing hundreds of genes in specific
            cell types and studying how they interact with the environment.</p>
            <p>The work was <a href="https://www.abstractsonline.com/pp8/#!/21436/presentation/7856">
            presented</a> at the American Association for Cancer Research Annual
            Meeting 2026 and showed RAS plays a role in survival and spread.</p>
            <p>Research in Kim's lab focuses on angiosarcoma's counterpart in dogs,
            hemangiosarcoma, which is common in dogs.</p>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="x_linked_article",
        display_name="X Linked Article Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="generic_html",
        storage_policy="metadata_and_link_review",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="x_linked_article", urls=[html_path.as_uri()]))
    record = bridge.parse("x_linked_article").records[0]

    assert record.fields["primary_source_links"] == []
    assert record.parser_confidence >= 0.45
    assert "hemangiosarcoma" in record.fields["article_text_preview"]
    assert any("comprehensive genomic profiling" in span["text"] for span in record.fields["evidence_spans"])
    assert record.fields["context_links"] == [
        {
            "href": "https://www.abstractsonline.com/pp8/#!/21436/presentation/7856",
            "text": "presented",
            "host": "www.abstractsonline.com",
            "reason": "conference_abstract_link",
        }
    ]


def test_x_linked_article_followup_collects_agent_links_and_parses(tmp_path, monkeypatch):
    article_url = "https://cancer.ufl.edu/2026/04/20/angiosarcoma-frontier/"
    html = b"""
    <html>
      <head><title>Angiosarcoma frontier</title></head>
      <body>
        <a href="https://pubmed.ncbi.nlm.nih.gov/87654321/">Primary paper</a>
        DOI: 10.1158/0008-5472.CAN-26-0002
      </body>
    </html>
    """

    def fake_fetch_url(url):
        assert url == article_url
        return scraper_bridge.FetchedPage(url=url, status_code=200, mime_type="text/html", content=html)

    monkeypatch.setattr(scraper_bridge, "_fetch_url", fake_fetch_url)
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    repo.create_agent_run(
        AgentRunRecord(
            agent_name="x_topic_review_agent",
            status=RunStatus.COMPLETED,
            source_key="x_topic_monitor",
            output_payload={
                "actions": [
                    {
                        "source_record_id": "tweet-1",
                        "action": "queue_source_followup",
                        "ingestible_links": [
                            {
                                "url": article_url,
                                "recommended_source_key": "x_linked_article",
                                "identifier_type": "unknown",
                                "identifier": None,
                                "should_ingest": False,
                                "reason": "Controlled scraper follow-up.",
                            }
                        ],
                    }
                ]
            },
        )
    )

    result = HSAResearchService(repo).run_x_linked_article_followup(
        XLinkedArticleFollowupRequest(
            approved_by="unit-test",
            approval_note="robots reviewed",
            recent_run_limit=5,
        )
    )

    assert result.candidate_urls == [article_url]
    assert result.fetched_pages == 1
    assert result.parsed_records == 1
    assert result.review_ids
    assert result.candidate_results[0]["status"] == "parsed"
    assert result.candidate_results[0]["url"] == article_url
    assert result.candidate_results[0]["artifact_id"]
    assert result.candidate_results[0]["review_id"]
    assert result.candidate_results[0]["primary_source_link_count"] == 2
    assert {
        (link["recommended_source_key"], link["identifier_type"], link["identifier"])
        for link in result.primary_source_links
    } >= {
        ("pubmed", "pmid", "87654321"),
        ("crossref", "doi", "10.1158/0008-5472.CAN-26-0002"),
    }


def test_x_linked_article_followup_requires_fetch_approval(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)

    result = HSAResearchService(repo).run_x_linked_article_followup(
        XLinkedArticleFollowupRequest(urls=["https://cancer.ufl.edu/article"])
    )

    assert result.candidate_urls == ["https://cancer.ufl.edu/article"]
    assert result.requires_fetch_approval is True
    assert result.fetched_pages == 0
    assert result.candidate_results == [
        {
            "url": "https://cancer.ufl.edu/article",
            "status": "requires_fetch_approval",
            "reason": "Explicit approval is required before fetching X-linked article URLs.",
        }
    ]


def test_source_followup_queue_roundtrips_sqlite_and_memory(tmp_path):
    sqlite_repo = SQLiteResearchRepository(tmp_path / "followups.sqlite3", seed=False)
    memory_repo = InMemoryResearchRepository()

    for repo in (sqlite_repo, memory_repo):
        review = repo.upsert_scrape_review(
            ScrapeReviewRecord(
                source_key="x_linked_article",
                artifact_id=uuid4(),
                source_record_id="article-1",
                title="Angiosarcoma article",
                canonical_url="https://example.edu/article",
                parser_confidence=0.7,
                fields={
                    "primary_source_links": [
                        {
                            "url": "https://doi.org/10.1234/test",
                            "recommended_source_key": "crossref",
                            "identifier_type": "doi",
                            "identifier": "10.1234/Test",
                            "should_ingest": True,
                            "reason": "DOI found.",
                        }
                    ]
                },
            )
        )

        queued = HSAResearchService(repo).queue_source_followups(
            SourceFollowupQueueRequest(review_ids=[review.review_id])
        )
        queued_again = HSAResearchService(repo).queue_source_followups(
            SourceFollowupQueueRequest(review_ids=[review.review_id])
        )
        queued_existing = HSAResearchService(repo).queue_source_followups(
            SourceFollowupQueueRequest(review_ids=[review.review_id], include_existing=True)
        )
        rows = HSAResearchService(repo).list_source_followups(source_key="crossref")

        assert queued.queued == 1
        assert queued_again.skipped_existing == 1
        assert queued_existing.queued == 0
        assert len(queued_existing.items) == 1
        assert len(rows) == 1
        assert rows[0].identifier == "10.1234/test"
        assert rows[0].status == "queued"


def test_source_followup_queue_reads_linked_article_agent_recommendations(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-followups.sqlite3", seed=False)
    review = repo.upsert_scrape_review(
        ScrapeReviewRecord(
            source_key="x_linked_article",
            artifact_id=uuid4(),
            source_record_id="article-context",
            title="Angiosarcoma context article",
            canonical_url="https://example.edu/article",
            parser_confidence=0.45,
            fields={
                "primary_source_links": [],
                "evidence_spans": [
                    {
                        "text": "The article mentions DOI 10.1158/0008-5472.CAN-26-0002.",
                        "matched_terms": ["doi"],
                    }
                ],
            },
        )
    )
    agent_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="x_linked_article_review_agent",
            status=RunStatus.COMPLETED,
            source_key="x_linked_article",
            output_payload={
                "actions": [
                    {
                        "review_id": str(review.review_id),
                        "source_record_id": review.source_record_id,
                        "action": "queue_primary_source_followup",
                        "reason": "Agent found a validated DOI in the article context.",
                        "followup_links": [
                            {
                                "url": "https://doi.org/10.1158/0008-5472.CAN-26-0002",
                                "recommended_source_key": "crossref",
                                "identifier_type": "doi",
                                "identifier": "10.1158/0008-5472.CAN-26-0002",
                                "should_ingest": True,
                                "reason": "Validated DOI from linked article review.",
                            }
                        ],
                    }
                ]
            },
        )
    )

    result = HSAResearchService(repo).queue_source_followups(
        SourceFollowupQueueRequest(review_ids=[review.review_id])
    )

    assert result.reviewed_records == 1
    assert result.agent_runs_seen == 1
    assert result.agent_recommendations_seen == 1
    assert result.queued == 1
    row = HSAResearchService(repo).list_source_followups(source_key="crossref")[0]
    assert row.identifier == "10.1158/0008-5472.can-26-0002"
    assert row.origin_agent_run_id == agent_run.agent_run_id
    assert row.metadata["recommendation_source"] == "linked_article_review_agent"
    assert row.metadata["agent_action_reason"] == "Agent found a validated DOI in the article context."


def test_source_followup_queue_reads_x_topic_agent_primary_source_flags(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "x-topic-followups.sqlite3", seed=False)
    agent_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="x_topic_review_agent",
            status=RunStatus.COMPLETED,
            source_key="x_topic_monitor",
            output_payload={
                "actions": [
                    {
                        "source_record_id": "tweet-1",
                        "query_name": "x_trial_monitoring",
                        "username": "vetonc",
                        "action": "flag_for_ingestion",
                        "reason": "Candidate links to a durable PubMed record.",
                        "ingestible_links": [
                            {
                                "url": "https://pubmed.ncbi.nlm.nih.gov/12345678/",
                                "recommended_source_key": "pubmed",
                                "identifier_type": "pmid",
                                "identifier": "12345678",
                                "should_ingest": True,
                                "reason": "PubMed link found in X topic review.",
                            },
                            {
                                "url": "https://example.edu/article",
                                "recommended_source_key": "x_linked_article",
                                "identifier_type": "unknown",
                                "identifier": None,
                                "should_ingest": False,
                                "reason": "Context article.",
                            },
                        ],
                    }
                ]
            },
        )
    )

    result = HSAResearchService(repo).queue_source_followups(
        SourceFollowupQueueRequest(source_key="x_topic_monitor")
    )

    assert result.reviewed_records == 0
    assert result.agent_runs_seen == 1
    assert result.agent_recommendations_seen == 2
    assert result.queued == 1
    assert result.skipped_uningestible == 1
    row = HSAResearchService(repo).list_source_followups(source_key="pubmed")[0]
    assert row.identifier == "12345678"
    assert row.origin_source_key == "x_topic_monitor"
    assert row.origin_agent_run_id == agent_run.agent_run_id
    assert row.metadata["recommendation_source"] == "x_topic_review_agent"
    assert row.metadata["source_record_id"] == "tweet-1"


def test_source_followup_ingest_dry_run_lists_queue_items(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "followups-dry-run.sqlite3", seed=False)
    repo.upsert_source_followup(
        SourceFollowupQueueItem(
            source_key="pubmed",
            identifier_type="pmid",
            identifier="12345678",
            origin_source_key="x_linked_article",
        )
    )

    result = HSAResearchService(repo).ingest_source_followups(SourceFollowupIngestRequest(dry_run=True))

    assert result.queue_items_seen == 1
    assert result.skipped == 1
    assert result.items[0].identifier == "12345678"
    assert repo.list_source_followups(source_key="pubmed")[0].status == "queued"


def test_source_followup_pmc_oa_fallback_queues_pubmed_and_crossref(tmp_path, monkeypatch):
    repo = SQLiteResearchRepository(tmp_path / "pmc-fallback.sqlite3", seed=False)
    item = repo.upsert_source_followup(
        SourceFollowupQueueItem(
            source_key="pmc_oa",
            identifier_type="pmcid",
            identifier="PMC5634767",
            origin_source_key="x_topic_monitor",
            metadata={"recommendation_source": "x_topic_review_agent"},
        )
    )
    monkeypatch.setattr(
        source_followup,
        "_pmc_idconv_metadata",
        lambda pmcid: {"pmcid": pmcid, "pmid": 28606972, "doi": "10.1634/theoncologist.2016-0429"},
    )

    fallback_items = source_followup._queue_pmc_metadata_fallbacks(
        repo,
        item,
        approved_by="unit-test",
    )
    rows = HSAResearchService(repo).list_source_followups(limit=10)

    assert {(row.source_key, row.identifier_type, row.identifier) for row in fallback_items} == {
        ("pubmed", "pmid", "28606972"),
        ("crossref", "doi", "10.1634/theoncologist.2016-0429"),
    }
    assert len(rows) == 3
    pubmed_row = next(row for row in rows if row.source_key == "pubmed")
    assert pubmed_row.metadata["fallback_type"] == "pmc_idconv_pubmed"
    assert pubmed_row.metadata["fallback_from_identifier"] == "PMC5634767"
    assert pubmed_row.metadata["approved_by"] == "unit-test"


def test_unpaywall_doi_followups_queue_only_doi_objects_idempotently(tmp_path):
    sqlite_repo = SQLiteResearchRepository(tmp_path / "doi-followups.sqlite3", seed=False)
    memory_repo = InMemoryResearchRepository()

    doi_object = ResearchObject(
        object_type=ResearchObjectType.PUBLICATION,
        title="Open access DOI candidate",
        source_key="crossref",
        identifiers={"doi": "https://doi.org/10.1234/HSA.OA"},
    )
    non_doi_object = ResearchObject(
        object_type=ResearchObjectType.PUBLICATION,
        title="No DOI candidate",
        source_key="pubmed",
        identifiers={"pmid": "12345678"},
    )

    sqlite_repo.upsert_research_object(doi_object)
    sqlite_repo.upsert_research_object(non_doi_object)
    memory_repo.research_objects[doi_object.id] = doi_object
    memory_repo.research_objects[non_doi_object.id] = non_doi_object

    for repo in (sqlite_repo, memory_repo):
        service = HSAResearchService(repo)
        queued = service.queue_unpaywall_doi_followups(DoiOpenAccessFollowupQueueRequest(limit=10))
        queued_again = service.queue_unpaywall_doi_followups(DoiOpenAccessFollowupQueueRequest(limit=10))
        rows = service.list_source_followups(source_key="unpaywall")

        assert queued.reviewed_records == 2
        assert queued.queued == 1
        assert queued.skipped_uningestible == 1
        assert queued_again.queued == 0
        assert queued_again.skipped_existing == 1
        assert len(rows) == 1
        assert rows[0].identifier_type == "doi"
        assert rows[0].identifier == "10.1234/hsa.oa"
        assert rows[0].metadata["followup_type"] == "doi_open_access_enrichment"
        assert rows[0].metadata["lookup_mode"] == "doi"
        assert rows[0].metadata["title_search"] is False


def test_source_followup_query_params_are_source_safe():
    crossref_query = source_followup._query_for_followup(
        SourceFollowupQueueItem(
            source_key="crossref",
            identifier_type="doi",
            identifier="10.1234/test",
            origin_source_key="x_linked_article",
        )
    )
    clinical_query = source_followup._query_for_followup(
        SourceFollowupQueueItem(
            source_key="clinicaltrials_gov",
            identifier_type="nct",
            identifier="NCT12345678",
            origin_source_key="x_linked_article",
        )
    )

    assert crossref_query.query_params == {
        "comparative_policy": "disabled",
        "require_policy_match": False,
    }
    assert clinical_query.query_params == {"require_policy_match": False}

    unpaywall_query = source_followup._query_for_followup(
        SourceFollowupQueueItem(
            source_key="unpaywall",
            identifier_type="doi",
            identifier="10.1234/HSA.OA",
            origin_source_key="crossref",
        )
    )
    assert unpaywall_query.source_key == "unpaywall"
    assert unpaywall_query.query_text == "10.1234/hsa.oa"
    assert unpaywall_query.query_name == "source_followup_doi_10_1234_hsa_oa"


def test_followup_internal_policy_params_do_not_reach_external_apis(monkeypatch):
    crossref_calls = []

    def fake_crossref_get_json(url, params):
        crossref_calls.append((url, params))
        return {
            "message": {
                "items": [
                    {
                        "DOI": "10.1234/test",
                        "title": ["Follow-up article without local policy terms"],
                        "URL": "https://doi.org/10.1234/test",
                    }
                ]
            }
        }

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_crossref_get_json)
    crossref_records = CrossrefHarvesterV2().fetch(
        "10.1234/test",
        limit=1,
        comparative_policy="disabled",
        require_policy_match=False,
    )

    assert len(crossref_records) == 1
    assert crossref_calls == [
        (
            "https://api.crossref.org/works",
            {"query": "10.1234/test", "rows": 1},
        )
    ]

    clinical_calls = []

    def fake_clinical_get_json(url, params):
        clinical_calls.append((url, params))
        return {"studies": []}

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_clinical_get_json)
    clinical_records = ClinicalTrialsGovHarvesterV2().fetch(
        "NCT12345678",
        limit=1,
        comparative_policy="disabled",
        require_policy_match=False,
    )

    assert clinical_records == []
    assert clinical_calls == [
        (
            "https://clinicaltrials.gov/api/v2/studies",
            {"query.term": "NCT12345678", "pageSize": 1, "format": "json"},
        )
    ]


def test_x_linked_article_review_agent_recommends_queue_and_ledgers(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "linked-review.sqlite3", seed=False)
    review = repo.upsert_scrape_review(
        ScrapeReviewRecord(
            source_key="x_linked_article",
            artifact_id=uuid4(),
            source_record_id="article-1",
            title="Angiosarcoma article",
            parser_confidence=0.7,
            fields={
                "primary_source_links": [
                    {
                        "url": "https://pubmed.ncbi.nlm.nih.gov/12345678/",
                        "recommended_source_key": "pubmed",
                        "identifier_type": "pmid",
                        "identifier": "12345678",
                        "should_ingest": True,
                        "reason": "PubMed link found.",
                    }
                ]
            },
        )
    )

    result = HSAResearchService(repo).run_x_linked_article_review(
        XLinkedArticleReviewRequest(review_ids=[review.review_id], review_mode="deterministic_only")
    )

    assert result.agent_run_id is not None
    assert result.queue_candidate_count == 1
    assert result.actions[0].action == "queue_primary_source_followup"
    assert result.actions[0].followup_links[0].recommended_source_key == "pubmed"
    runs = repo.list_agent_runs(agent_name="x_linked_article_review_agent", status="completed")
    assert runs
    assert runs[0].summary["queue_candidate_count"] == 1


def test_x_linked_article_review_agent_uses_context_without_queueing_it(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "linked-review-context.sqlite3", seed=False)
    review = repo.upsert_scrape_review(
        ScrapeReviewRecord(
            source_key="x_linked_article",
            artifact_id=uuid4(),
            source_record_id="article-context",
            title="Angiosarcoma conference article",
            canonical_url="https://cancer.ufl.edu/article",
            parser_confidence=0.45,
            fields={
                "primary_source_links": [],
                "context_links": [
                    {
                        "href": "https://www.abstractsonline.com/pp8/#!/21436/presentation/7856",
                        "text": "presented",
                        "host": "www.abstractsonline.com",
                        "reason": "conference_abstract_link",
                    }
                ],
                "evidence_spans": [
                    {
                        "text": "The work was presented at AACR and showed RAS plays a role.",
                        "matched_terms": ["presented", "RAS"],
                        "reason": "article_body_source_context",
                    }
                ],
                "article_text_preview": "The work was presented at AACR.",
            },
        )
    )

    result = HSAResearchService(repo).run_x_linked_article_review(
        XLinkedArticleReviewRequest(review_ids=[review.review_id], review_mode="deterministic_only")
    )

    action = result.actions[0]
    assert action.action == "needs_human_review"
    assert action.followup_links == []
    assert action.metadata["context_link_count"] == 1
    assert action.metadata["context_links"][0]["reason"] == "conference_abstract_link"
    assert result.queue_candidate_count == 0
    assert result.needs_human_review_count == 1
    leads = repo.list_research_leads(status="new", source_key="x_linked_article")
    assert len(leads) == 1
    assert leads[0].origin_review_id == review.review_id
    assert leads[0].url == "https://cancer.ufl.edu/article"
    assert "angiosarcoma" in leads[0].topic_tags

    collected = HSAResearchService(repo).collect_research_leads(ResearchLeadCollectRequest())
    assert collected.skipped_existing == 1


def test_x_linked_article_review_openrouter_preserves_queue_guardrail(tmp_path, monkeypatch):
    from hsa_research.ingestion_bridge import x_linked_article_review

    repo = SQLiteResearchRepository(tmp_path / "linked-review-openrouter.sqlite3", seed=False)
    review = repo.upsert_scrape_review(
        ScrapeReviewRecord(
            source_key="x_linked_article",
            artifact_id=uuid4(),
            source_record_id="article-1",
            title="Angiosarcoma article",
            parser_confidence=0.7,
            fields={
                "primary_source_links": [
                    {
                        "url": "https://doi.org/10.1234/test",
                        "recommended_source_key": "crossref",
                        "identifier_type": "doi",
                        "identifier": "10.1234/test",
                        "should_ingest": True,
                        "reason": "DOI found.",
                    }
                ]
            },
        )
    )

    def fake_review_model(model_name, review_payload):
        assert model_name == "anthropic/claude-sonnet-test"
        assert review_payload["articles"][0]["review_id"] == str(review.review_id)
        return {
            "text": json.dumps(
                {
                    "actions": [
                        {
                            "review_id": str(review.review_id),
                            "source_record_id": "article-1",
                            "action": "needs_human_review",
                            "severity": "watch",
                            "reason": "Model wants a human pass.",
                        }
                    ],
                    "evidence": {"review_summary": "reviewed"},
                    "errors": [],
                }
            ),
            "metadata": {"model_name": "anthropic/claude-sonnet-test", "usage": {"cost": 0.01}},
        }

    monkeypatch.setattr(x_linked_article_review, "_openrouter_review_model", fake_review_model)

    result = HSAResearchService(repo).run_x_linked_article_review(
        XLinkedArticleReviewRequest(
            review_ids=[review.review_id],
            review_mode="openrouter_required",
            review_models=["anthropic/claude-sonnet-test"],
        )
    )

    assert result.queue_candidate_count == 1
    assert [action.action for action in result.actions] == [
        "needs_human_review",
        "queue_primary_source_followup",
    ]
    assert result.evidence["model_reviews"][0]["status"] == "completed"


def test_x_linked_article_review_openrouter_rejects_unvalidated_context_links(tmp_path, monkeypatch):
    from hsa_research.ingestion_bridge import x_linked_article_review

    repo = SQLiteResearchRepository(tmp_path / "linked-review-invalid-model.sqlite3", seed=False)
    review = repo.upsert_scrape_review(
        ScrapeReviewRecord(
            source_key="x_linked_article",
            artifact_id=uuid4(),
            source_record_id="article-context",
            title="Angiosarcoma conference article",
            parser_confidence=0.45,
            fields={
                "primary_source_links": [],
                "context_links": [
                    {
                        "href": "https://www.abstractsonline.com/pp8/#!/21436/presentation/7856",
                        "text": "presented",
                        "host": "www.abstractsonline.com",
                        "reason": "conference_abstract_link",
                    }
                ],
                "evidence_spans": [
                    {
                        "text": "The work was presented at AACR.",
                        "matched_terms": ["presented"],
                        "reason": "article_body_source_context",
                    }
                ],
            },
        )
    )

    def fake_review_model(model_name, review_payload):
        assert review_payload["articles"][0]["context_links"][0]["host"] == "www.abstractsonline.com"
        assert review_payload["articles"][0]["evidence_spans"]
        return {
            "text": json.dumps(
                {
                    "actions": [
                        {
                            "review_id": str(review.review_id),
                            "source_record_id": "article-context",
                            "action": "queue_primary_source_followup",
                            "severity": "watch",
                            "reason": "Model tried to queue a conference page.",
                            "followup_links": [
                                {
                                    "url": "https://www.abstractsonline.com/pp8/#!/21436/presentation/7856",
                                    "recommended_source_key": "x_linked_article",
                                    "identifier_type": "unknown",
                                    "identifier": None,
                                    "should_ingest": True,
                                    "reason": "Conference context.",
                                }
                            ],
                        }
                    ],
                    "evidence": {"review_summary": "reviewed"},
                    "errors": [],
                }
            ),
            "metadata": {"model_name": "anthropic/claude-sonnet-test", "usage": {"cost": 0.01}},
        }

    monkeypatch.setattr(x_linked_article_review, "_openrouter_review_model", fake_review_model)

    result = HSAResearchService(repo).run_x_linked_article_review(
        XLinkedArticleReviewRequest(
            review_ids=[review.review_id],
            review_mode="openrouter_required",
            review_models=["anthropic/claude-sonnet-test"],
        )
    )

    assert result.actions[0].action == "needs_human_review"
    assert result.actions[0].followup_links == []
    assert result.queue_candidate_count == 0
    assert result.needs_human_review_count >= 1


def test_dagster_exposes_source_followup_jobs():
    assert dagster_asset_module.x_linked_article_review_job is not None
    assert dagster_asset_module.source_followup_queue_job is not None
    assert dagster_asset_module.source_followup_ingest_job is not None
    assert dagster_asset_module.command_center_job is not None
    assert dagster_asset_module.agent_performance_report_job is not None
    assert dagster_asset_module.agent_performance_evaluation_job is not None
    assert dagster_asset_module.pubmed_source_followup_ingest_job is not None
    assert dagster_asset_module.crossref_source_followup_ingest_job is not None
    assert dagster_asset_module.pmc_oa_source_followup_ingest_job is not None
    assert dagster_asset_module.clinicaltrials_gov_source_followup_ingest_job is not None
    assert dagster_asset_module.unpaywall_source_followup_ingest_job is not None
    assert dagster_asset_module.research_brief_agent_job is not None
    assert dagster_asset_module.research_brief_library_job is not None
    assert dagster_asset_module.research_brief_evaluation_job is not None
    assert dagster_asset_module.research_brief_evaluation_library_job is not None
    assert dagster_asset_module.research_brief_quality_job is not None
    assert dagster_asset_module.research_brief_followup_queue_job is not None
    assert dagster_asset_module.validation_plan_job is not None
    assert dagster_asset_module.validation_plan_library_job is not None
    assert dagster_asset_module.validation_request_queue_job is not None
    assert dagster_asset_module.validation_request_queue_library_job is not None
    assert dagster_asset_module.validation_autopilot_job is not None
    assert dagster_asset_module.research_brief_queue_job is not None
    assert dagster_asset_module.research_brief_queue_batch_job is not None
    assert dagster_asset_module.research_brief_queue_seed_job is not None
    assert dagster_asset_module.research_brief_queue_runner_job is not None
    assert dagster_asset_module.research_brief_queue_maintenance_job is not None
    assert dagster_asset_module.research_brief_playground_pack_job is not None
    assert dagster_asset_module.therapy_committee_validation_queue_job is not None
    assert dagster_asset_module.research_leads_job is not None
    assert dagster_asset_module.evidence_gap_resolver_job is not None
    assert dagster_asset_module.validation_gap_source_pack_job is not None
    assert dagster_asset_module.validation_gap_source_ingest_job is not None
    assert dagster_asset_module.research_followup_resolver_job is not None
    assert dagster_asset_module.validation_autopilot_hourly_schedule is not None
    assert dagster_asset_module.validation_autopilot_hourly_schedule.cron_schedule == "0 * * * *"


def test_dagster_schedules_source_followup_lanes():
    assert dagster_asset_module.source_followup_queue_daily_schedule is not None
    assert dagster_asset_module.source_followup_queue_daily_schedule.cron_schedule == "5 3 * * *"
    assert dagster_asset_module.pubmed_source_followup_ingest_daily_schedule is not None
    assert dagster_asset_module.pubmed_source_followup_ingest_daily_schedule.cron_schedule == "20 3 * * *"
    assert dagster_asset_module.crossref_source_followup_ingest_daily_schedule is not None
    assert dagster_asset_module.crossref_source_followup_ingest_daily_schedule.cron_schedule == "35 3 * * *"
    assert dagster_asset_module.pmc_oa_source_followup_ingest_daily_schedule is not None
    assert dagster_asset_module.pmc_oa_source_followup_ingest_daily_schedule.cron_schedule == "50 3 * * *"
    assert dagster_asset_module.clinicaltrials_gov_source_followup_ingest_daily_schedule is not None
    assert dagster_asset_module.clinicaltrials_gov_source_followup_ingest_daily_schedule.cron_schedule == "5 4 * * *"
    assert dagster_asset_module.unpaywall_source_followup_ingest_daily_schedule is not None
    assert dagster_asset_module.unpaywall_source_followup_ingest_daily_schedule.cron_schedule == "20 4 * * *"
    assert dagster_asset_module.research_leads_daily_schedule is not None
    assert dagster_asset_module.research_leads_daily_schedule.cron_schedule == "35 4 * * *"


def test_scrape_bridge_ingest_requires_approval(tmp_path, monkeypatch):
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    result = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts").ingest(
        ScrapeIngestRequest(source_key="test_scraper")
    )

    assert result.promoted_records == 0
    assert "requires explicit approval" in result.errors[0]


def test_scrape_bridge_promotes_snapshot_after_review_approval(tmp_path, monkeypatch):
    html_path = tmp_path / "trial.html"
    html_path.write_text(
        """
        <html>
          <head><title>Canine Hemangiosarcoma Trial</title></head>
          <body><a href="/trial/1">Trial detail</a></body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="generic_html",
        storage_policy="metadata_only",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")
    fetch = bridge.fetch(ScrapeFetchRequest(source_key="test_scraper", urls=[html_path.as_uri()]))
    parse = bridge.parse("test_scraper")
    review = bridge.review(
        ScrapeReviewRequest(
            source_key="test_scraper",
            review_ids=parse.review_ids,
            decision="accepted",
            reviewed_by="unit-test",
            review_note="fields look valid",
        )
    )

    ingest = bridge.ingest(
        ScrapeIngestRequest(
            source_key="test_scraper",
            review_ids=[record.review_id for record in review.records],
            approved_by="unit-test",
            approval_note="reviewed parsed fields",
        )
    )
    objects = repo.list_research_objects(source_key="test_scraper")

    assert fetch.fetched_pages == 1
    assert review.reviewed_records == 1
    assert ingest.promoted_records == 1
    assert ingest.review_records_seen == 1
    assert ingest.raw_records == 1
    assert ingest.research_objects == 1
    assert ingest.document_chunks == 1
    assert objects[0].title == "Canine Hemangiosarcoma Trial"
    assert objects[0].object_type == "veterinary_trial"
    assert objects[0].metadata["review_status"] == "accepted"
    assert objects[0].metadata["approved_by"] == "unit-test"
    assert objects[0].metadata["review_id"] == str(review.records[0].review_id)


def test_scrape_review_queue_preserves_review_decision_on_reparse(tmp_path, monkeypatch):
    html_path = tmp_path / "trial.html"
    html_path.write_text("<html><head><title>Reviewed Trial</title></head><body></body></html>", encoding="utf-8")
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        parser="generic_html",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")
    bridge.fetch(ScrapeFetchRequest(source_key="test_scraper", urls=[html_path.as_uri()]))
    first_parse = bridge.parse("test_scraper")
    bridge.review(
        ScrapeReviewRequest(
            source_key="test_scraper",
            review_ids=first_parse.review_ids,
            decision="rejected",
            reviewed_by="unit-test",
            review_note="not a target source",
        )
    )

    second_parse = bridge.parse("test_scraper")
    reviews = repo.list_scrape_reviews(source_key="test_scraper")

    assert second_parse.review_ids == first_parse.review_ids
    assert second_parse.records[0].review_status == "rejected"
    assert len(reviews) == 1
    assert reviews[0].review_status == "rejected"
    assert reviews[0].reviewer == "unit-test"


def test_avma_vctr_parser_extracts_trial_fields(tmp_path, monkeypatch):
    html_path = tmp_path / "avma.html"
    html_path.write_text(
        """
        <html>
          <head>
            <meta property="og:title" content="Evaluation of a combination of three drugs in dogs with hemangiosarcoma">
            <meta name="description" content="Combination therapy for dogs with Hemangiosarcoma.">
          </head>
          <body>
            <h1>Evaluation of a combination of three drugs in dogs with hemangiosarcoma</h1>
            <p>The objective of this study is to investigate doxorubicin or carboplatin and temozolomide with propranolol in dogs with hemangiosarcoma.</p>
            <dl>
              <dt>Condition</dt><dd>Hemangiosarcoma</dd>
              <dt>Species</dt><dd>Canine</dd>
              <dt>Study Type</dt><dd>Drug</dd>
              <dt>Funding</dt><dd>Unfunded</dd>
              <dt>Status</dt><dd>Recruiting</dd>
              <dt>Investigator</dt><dd>Claire Lemons, DVM</dd>
            </dl>
            <a href="/s/combination-therapy-hsa-123456/">Learn More</a>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="avma_vctr_test",
        display_name="AVMA Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="avma_vctr",
        storage_policy="link_and_registry_metadata",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[html_path.as_uri()]))
    parse = bridge.parse("avma_vctr_test")
    record = parse.records[0]

    assert parse.parsed_records == 1
    assert record.title == "Evaluation of a combination of three drugs in dogs with hemangiosarcoma"
    assert record.source_record_id.endswith("/avma.html")
    assert record.record_type == "veterinary_trial"
    assert record.fields["condition"] == "Hemangiosarcoma"
    assert record.fields["species"] == "Canine"
    assert record.fields["study_type"] == "Drug"
    assert record.fields["funding"] == "Unfunded"
    assert record.fields["status"] == "Recruiting"
    assert record.fields["investigator"] == "Claire Lemons, DVM"
    assert record.parser_confidence >= 0.3


def test_avma_vctr_parser_extracts_embedded_study_json(tmp_path, monkeypatch):
    html_path = tmp_path / "embedded.html"
    html_path.write_text(
        """
        <html>
          <head><meta property="og:title" content="Antibody therapy for dogs with splenic hemangiosarcoma"></head>
          <body>
            <script id="d_study_keywords" type="application/json">["hemangiosarcoma", "VEGF"]</script>
            <script id="d_avma_study_data" type="application/json">
              {"vct_code": "VCT16000189", "patients_randomly_assigned": true}
            </script>
            <script id="d_avma_studycontent_data" type="application/json">
              {
                "diagnosis": "Hemangiosarcoma",
                "inclusion_criteria": "<p>Splenic hemangiosarcoma after splenectomy.</p>",
                "exclusion_criteria": "Metastatic disease at screening.",
                "intervention_name": "Anti-VEGF antibody",
                "potential_benefits": "Increased time to progression",
                "potential_risks": "Elevated blood pressure",
                "pri_outcome_name": "Safety",
                "pri_outcome_measure": "Blood pressure measurement",
                "pri_outcome_endpoint": "Safety",
                "sec_outcome1_name": "Overall survival",
                "sec_outcome1_measure": "Survival tracking",
                "sec_outcome1_endpoint": "Death or euthanasia",
                "funding_source_institution": true
              }
            </script>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="avma_vctr_test",
        display_name="AVMA Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        parser="avma_vctr",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[html_path.as_uri()]))
    record = bridge.parse("avma_vctr_test").records[0]

    assert record.fields["vct_code"] == "VCT16000189"
    assert record.fields["condition"] == "Hemangiosarcoma"
    assert record.fields["keywords"] == ["hemangiosarcoma", "VEGF"]
    assert record.fields["intervention"] == "Anti-VEGF antibody"
    assert record.fields["eligibility"] == "Splenic hemangiosarcoma after splenectomy."
    assert record.fields["primary_outcome"]["measure"] == "Blood pressure measurement"
    assert record.fields["secondary_outcomes"][0]["name"] == "Overall survival"
    assert record.fields["funding_sources"] == ["institution"]
    assert record.parser_confidence >= 0.65


def test_avma_vctr_parser_keeps_sparse_pages_low_confidence(tmp_path, monkeypatch):
    html_path = tmp_path / "sparse.html"
    html_path.write_text("<html><head><title>Unknown Veterinary Page</title></head><body></body></html>", encoding="utf-8")
    profile = ScrapeSourceProfile(
        source_key="avma_vctr_test",
        display_name="AVMA Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        parser="avma_vctr",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[html_path.as_uri()]))
    parse = bridge.parse("avma_vctr_test")
    record = parse.records[0]

    assert record.title == "Unknown Veterinary Page"
    assert "condition" not in record.fields
    assert "species" not in record.fields
    assert record.parser_confidence < 0.3


def test_scrape_manifest_discovers_avma_candidate_urls_from_stored_seed_page(tmp_path, monkeypatch):
    seed_path = tmp_path / "seed.html"
    detail_dir = tmp_path / "s"
    detail_dir.mkdir()
    detail_path = detail_dir / "combination-therapy-hsa-123456.html"
    detail_path.write_text(
        "<html><head><title>Combination therapy in canine hemangiosarcoma</title></head><body></body></html>",
        encoding="utf-8",
    )
    seed_path.write_text(
        f"""
        <html>
          <body>
            <a href="{detail_path.as_uri()}">Hemangiosarcoma clinical trial</a>
            <a href="{(tmp_path / "about.html").as_uri()}">About</a>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="avma_vctr_test",
        display_name="AVMA Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="avma_vctr",
        storage_policy="link_and_registry_metadata",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")
    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[seed_path.as_uri()]))

    manifest = bridge.build_manifest(ScrapeManifestRequest(source_key="avma_vctr_test"))
    manifest_artifact = repo.get_artifact(manifest.manifest_artifact_id)

    assert manifest.seed_artifacts_seen == 1
    assert len(manifest.candidate_urls) == 1
    assert manifest.candidate_urls[0].url == detail_path.as_uri()
    assert manifest.candidate_urls[0].confidence == 0.8
    assert manifest_artifact.artifact_type == "scrape_manifest"
    assert manifest_artifact.metadata["candidate_count"] == 1


def test_fetch_scrape_manifest_fetches_manifest_candidate_pages(tmp_path, monkeypatch):
    seed_path = tmp_path / "seed.html"
    detail_dir = tmp_path / "s"
    detail_dir.mkdir()
    detail_path = detail_dir / "solid-tumor-study.html"
    detail_path.write_text(
        "<html><head><title>Solid tumor study</title></head><body>Canine solid tumor trial.</body></html>",
        encoding="utf-8",
    )
    seed_path.write_text(f'<html><body><a href="{detail_path.as_uri()}">Solid tumor study</a></body></html>', encoding="utf-8")
    profile = ScrapeSourceProfile(
        source_key="avma_vctr_test",
        display_name="AVMA Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="avma_vctr",
        storage_policy="link_and_registry_metadata",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")
    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[seed_path.as_uri()]))
    manifest = bridge.build_manifest(ScrapeManifestRequest(source_key="avma_vctr_test"))

    fetch = bridge.fetch_manifest(
        ScrapeManifestFetchRequest(
            source_key="avma_vctr_test",
            manifest_artifact_id=manifest.manifest_artifact_id,
            max_pages=1,
        )
    )

    assert fetch.fetched_pages == 1
    assert len(fetch.artifact_ids) == 1
    assert repo.get_artifact(fetch.artifact_ids[0]).metadata["source_url"] == detail_path.as_uri()


def test_scrape_bridge_skips_urls_outside_profile_allowlist(tmp_path, monkeypatch):
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url="file:///allowed",
        allowed_url_patterns=["file:///allowed/*"],
        robots_policy="reviewed",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    result = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts").fetch(
        ScrapeFetchRequest(source_key="test_scraper", urls=[(tmp_path / "outside.html").as_uri()])
    )

    assert result.fetched_pages == 0
    assert result.skipped_pages == 1
    assert "outside allowed patterns" in result.errors[0]
