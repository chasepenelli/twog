import json
import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
import xml.etree.ElementTree as ET
from uuid import UUID, uuid4
import zipfile
import pytest
from pydantic import ValidationError
from hsa_research.ingestion_bridge import dagster_assets as dagster_asset_module
from hsa_research.ingestion_bridge import cli as cli_module
from hsa_research.ingestion_bridge import command_center_web
from hsa_research.ingestion_bridge import candidate_contribution_intake
from hsa_research.ingestion_bridge import entity_resolution
from hsa_research.ingestion_bridge import evidence_fit
from hsa_research.ingestion_bridge import source_query_params
from hsa_research.ingestion_bridge import storage, structured_orchestration
from hsa_research.ingestion_bridge import compute_runners
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
    ArtifactHandle,
    BoltzRunRequest,
    CandidateDossierRequest,
    ChunkContextRequest,
    ClaimDirection,
    ClaimSearchRequest,
    ClaimSearchResult,
    ClaimType,
    CommandCenterRequest,
    ComputeJobRecord,
    ComputeJobReportRequest,
    ComputeJobReportResult,
    CommitHypothesisRequest,
    DaytonaWorkspaceRequest,
    DaytonaWorkspaceResult,
    DoiOpenAccessFollowupQueueRequest,
    DocumentChunk,
    EvidenceRefRepairItem,
    EvidenceRefRepairReport,
    EvidenceRefRepairRequest,
    EvidenceFitAssessment,
    EvidenceGapResolverRequest,
    EvidenceGapResolverResult,
    EmbeddingCoverageSummary,
    EntityAlias,
    EntityLookupIndexRequest,
    EntityLookupRequest,
    EntityMention,
    EvidenceLevel,
    FullTextTriageRequest,
    FullTextOpsAction,
    FullTextOpsRequest,
    FullTextOpsResult,
    HypothesisPromotionCandidate,
    HypothesisPromotionReportRequest,
    HypothesisProposalRequest,
    IngestionResult,
    MDExpertAgentReviewRequest,
    MDExpertAgentReviewResult,
    MDExpertApprovalRecord,
    MDExpertReviewPacketRecord,
    MDInputPacket,
    NeonBranchWorkspaceRequest,
    NeonBranchWorkspaceResult,
    OmicsAccessionHuntRequest,
    OmicsAccessionHuntResult,
    OmicsEvidencePacketRequest,
    OmicsEvidencePacketResult,
    OmicsFollowupRequest,
    OmicsFollowupResult,
    OmicsFollowupTask,
    OmicsLocusSignalRequest,
    OmicsLocusSignalResult,
    OmicsGeneSetScore,
    OmicsReadoutRequest,
    OmicsReadoutResult,
    PrimitiveCallEvent,
    ProofCapsuleLibraryRequest,
    ProofCapsuleLibraryResult,
    ProofCapsuleRecord,
    ProofCapsuleSourceRef,
    ProofCapsuleSubmitRequest,
    ProofCapsuleSubmitResult,
    ProofCapsuleSummary,
    ProofCapsuleTarget,
    PublicCandidateDecisionEvent,
    PublicCandidateGenerateRequest,
    PublicCandidateIntegrityReportRequest,
    PublicCandidateLibraryRequest,
    PublicCandidateRecord,
    PublicCandidateSnapshot,
    RawSourceRecord,
    ResolvedEntity,
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
    ResearchBriefOperatorDocRequest,
    ResearchBriefOperatorDocResult,
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
    ResearchHuntTaskRunRequest,
    ResearchHuntQueueReportRequest,
    ResearchHuntQueueReportResult,
    ResearchHuntLeadQueueRow,
    ResearchHuntTaskQueueRow,
    ResearchHuntQueueMaintenanceRequest,
    ResearchHuntQueueMaintenanceResult,
    ResearchHuntQueueMaintenanceItem,
    ResearchHuntSynthesisDocument,
    ResearchHuntSynthesisDocRequest,
    ResearchHuntSynthesisDocResult,
    ResearchHuntSynthesisQueueRequest,
    ResearchHuntSynthesisQueueResult,
    ResearchFollowupRefinementRequest,
    ResearchFollowupResolverRequest,
    ResearchFollowupResolverResult,
    ResearchLeadCollectRequest,
    ResearchLeadRecord,
    ResearchProgramBoardRequest,
    ResearchProgramBoardResult,
    ResearchProgramEvidenceLoopRequest,
    ResearchProgramEvidenceLoopResult,
    ResearchProgramEvidenceTask,
    ResearchProgramQuestion,
    ResearchProgramRecord,
    ResearchProgramReviewRequest,
    ResearchProgramReviewResult,
    ResearchWorkspaceCleanupRequest,
    ResearchWorkspaceCleanupResult,
    ResearchWorkspaceCheckoutManifestRequest,
    ResearchWorkspaceCheckoutManifestResult,
    ResearchWorkspaceLibraryRequest,
    ResearchWorkspaceLibraryResult,
    ResearchWorkspaceRecord,
    RewardEventRecord,
    RewardEventSyncRequest,
    RewardReportRequest,
    RewardReportRow,
    RunManifestRecord,
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
    SourceFollowupIngestResult,
    SourceFollowupQueueItem,
    SourceFollowupQueueRequest,
    SourceQuery,
    SourceVersionRecord,
    TextEmbedding,
    TextEmbeddingSearchRequest,
    TherapyCommitteeRequest,
    TherapyCommitteeResult,
    TherapyCommitteeValidationQueueRequest,
    TherapyCommitteeValidationQueueResult,
    TherapyIdea,
    TherapyIdeaLibraryRequest,
    TherapyIdeaRecord,
    ValidationAgentResult,
    ValidationPacketAddendumBrief,
    ValidationDecisionPacket,
    ValidationDecisionRecord,
    ValidationDecisionReportRequest,
    ValidationDecisionReportResult,
    ValidationPlanRecord,
    ValidationPlanRequest,
    ValidationPlanResult,
    ValidationPlanTask,
    ValidationAssayContext,
    ValidationAutopilotRequest,
    ValidationGapSourceIngestRequest,
    ValidationGapSourceIngestResult,
    ValidationGapSourcePackRequest,
    ValidationGapSourcePackResult,
    ValidationGapSourceQuery,
    ValidationPacket,
    ValidationPacketEvidenceAddendum,
    ValidationPacketRequest,
    ValidationPacketResult,
    ValidationRequest,
    ValidationRequestQueueItem,
    ValidationRequestQueueRequest,
    ValidationToolCatalogRequest,
    ValidationToolCapability,
    ValidationToolMatchRequest,
    ClaimCurationRequest,
    SourceScoutRequest,
    PubMedIdentifierRepairRequest,
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
from hsa_research.ingestion_bridge.research_workspaces import (
    build_research_workspace_checkout_manifest,
    cleanup_neon_research_workspaces,
    plan_daytona_workspace,
    provision_neon_branch_workspace,
)
from hsa_research.ingestion_bridge.proof_capsules import (
    build_proof_capsule_library,
    submit_proof_capsule,
)
from hsa_research.ingestion_bridge import full_text_ops
from hsa_research.ingestion_bridge import research_brief_evaluation
from hsa_research.ingestion_bridge import research_followup_resolver
from hsa_research.ingestion_bridge import pubmed_identifier_repair
from hsa_research.ingestion_bridge import omics_accession_hunt
from hsa_research.ingestion_bridge import research_brief_agent
from hsa_research.ingestion_bridge import model_policy
from hsa_research.ingestion_bridge import research_program_board
from hsa_research.ingestion_bridge import md_expert_agent
from hsa_research.ingestion_bridge import therapy_committee
from hsa_research.ingestion_bridge import validation_agents
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
    DOIDHarvesterV2,
    EnsemblComparaHarvesterV2,
    EnsemblXrefsHarvesterV2,
    EuropePMCHarvesterV2,
    GEOHarvesterV2,
    HARVESTERS_V2,
    HGNCHarvesterV2,
    ICDCHarvesterV2,
    MONDOHarvesterV2,
    NCBIGeneHarvesterV2,
    OMAHarvesterV2,
    OpenAlexHarvesterV2,
    OpenFDAAnimalEventsHarvesterV2,
    PMCOAHarvesterV2,
    PubChemHarvesterV2,
    PubMedHarvesterV2,
    RCSBPDBHarvesterV2,
    ReactomeHarvesterV2,
    SRAHarvesterV2,
    UniChemHarvesterV2,
    UniProtHarvesterV2,
    UnpaywallHarvesterV2,
    VGNCHarvesterV2,
    WikiPathwaysHarvesterV2,
)
from hsa_research.ingestion_bridge.local_ingest import LocalIngestionPipeline
from hsa_research.ingestion_bridge import local_ingest as local_ingest_module
from hsa_research.ingestion_bridge.local_store import SQLiteResearchRepository
from hsa_research.ingestion_bridge import mcp_server
from hsa_research.ingestion_bridge.agent_runner import AgentRunner
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
from hsa_research.ingestion_bridge.validation_tool_catalog import (
    build_validation_tool_task,
    get_validation_tool,
    list_validation_tool_catalog,
)
def make_service(tmp_path):
    return HSAResearchService(SQLiteResearchRepository(tmp_path / "hsa.sqlite3"))
def _contains_key(value, key: str) -> bool:
    if isinstance(value, dict):
        return key in value or any(_contains_key(item, key) for item in value.values())
    if isinstance(value, list):
        return any(_contains_key(item, key) for item in value)
    return False
def _research_program_fixture() -> ResearchProgramRecord:
    return ResearchProgramRecord(
        title="Vascular ecology program",
        thesis="HSA may be vulnerable through vascular injury, coagulation, and angiogenesis ecology.",
        disease_model="Endothelial tumor biology may intersect with coagulation and VEGF-axis signaling.",
        thesis_area="vascular coagulation angiogenesis",
        therapy_families=["vascular normalization"],
        decisive_questions=[
            ResearchProgramQuestion(
                question="Is coagulation biology outcome-relevant in canine HSA?",
                metric_plan=["direct evidence count"],
                tool_hints=["literature_review"],
            ),
            ResearchProgramQuestion(
                question="Can KDR or pathway biomarkers stratify the program?",
                metric_plan=["expression support"],
                tool_hints=["omics_expression_review"],
            ),
        ],
        evidence_tasks=[
            ResearchProgramEvidenceTask(
                title="Coagulation evidence acquisition",
                objective="Search for HSA coagulation and vascular injury evidence.",
                source_keys=["pubmed", "europe_pmc"],
                metrics=["unique source count"],
                pass_values=["three independent sources"],
                fail_values=["only nonspecific bleeding references"],
            )
        ],
        stop_criteria=["Archive if no assayable vascular/coagulation signal emerges after two loops."],
    )
def _ready_for_therapy_ideas_program() -> ResearchProgramRecord:
    return _research_program_fixture().model_copy(
        update={
            "status": "ready_for_therapy_ideas",
            "gate_decision": "ready_for_therapy_ideas",
            "evidence_loop_count": 2,
            "max_evidence_loops": 2,
            "therapy_families": [
                "biomarker-stratified vascular ecology therapy",
                "endothelial antigen immunomodulation",
            ],
            "downstream_therapy_opportunities": [
                "PIK3CA/mTOR biomarker-stratified therapy",
                "anti-extracellular vimentin immunotherapy",
                "VEGFR-axis combinations under biomarker gating",
            ],
            "review_summary": "Program reached the finite idea gate after the capped evidence loop.",
        }
    )
class _FakeNeonBranchClient:
    def __init__(
        self,
        *,
        existing_branches: list[dict] | None = None,
        connection: dict | None = None,
    ) -> None:
        self.existing_branches = existing_branches or []
        self.connection = connection or {"uri": "postgres://secret.example/neondb"}
        self.created: list[dict] = []
        self.connection_requests: list[dict] = []
        self.deleted: list[dict] = []

    def list_branches(self, *, project_id: str, search: str | None = None) -> list[dict]:
        if not search:
            return self.existing_branches
        return [branch for branch in self.existing_branches if search in branch.get("name", "")]

    def create_branch(self, *, project_id: str, branch: dict, endpoints: list[dict]) -> dict:
        self.created.append({"project_id": project_id, "branch": branch, "endpoints": endpoints})
        return {
            "branch": {"id": "br-created", "name": branch["name"]},
            "endpoints": [{"id": "ep-created", "type": "read_write"}],
            "operations": [{"id": "op-created"}],
        }

    def get_connection_uri(
        self,
        *,
        project_id: str,
        branch_id: str,
        database_name: str,
        role_name: str,
        endpoint_id: str | None = None,
    ) -> dict:
        self.connection_requests.append(
            {
                "project_id": project_id,
                "branch_id": branch_id,
                "database_name": database_name,
                "role_name": role_name,
                "endpoint_id": endpoint_id,
            }
        )
        return self.connection

    def delete_branch(self, *, project_id: str, branch_id: str) -> dict:
        self.deleted.append({"project_id": project_id, "branch_id": branch_id})
        return {"branch": {"id": branch_id}}
class _FailingNeonBranchClient(_FakeNeonBranchClient):
    def delete_branch(self, *, project_id: str, branch_id: str) -> dict:
        self.deleted.append({"project_id": project_id, "branch_id": branch_id})
        raise RuntimeError("delete blocked")
def _cleanup_workspace(
    *,
    branch_id: str = "br-expired",
    status: str = "ready",
    expires_at: datetime | None = None,
    candidate_id: str = "twog-candidate-447eb8089965",
    work_packet_id: str = "wp-cleanup-1",
) -> ResearchWorkspaceRecord:
    return ResearchWorkspaceRecord(
        candidate_id=candidate_id,
        work_packet_id=work_packet_id,
        provider="neon",
        provider_workspace_id=branch_id,
        neon_branch_id=branch_id,
        neon_branch_name=f"twog-{branch_id}",
        database_secret_ref=f"neon://project-test/{branch_id}/neondb/neondb_owner",
        status=status,
        expires_at=expires_at or datetime.now(UTC) - timedelta(hours=1),
    )
def _write_minimal_xlsx(path, rows):
    sheet_rows = []
    for row_index, row in enumerate(rows, start=1):
        cells = []
        for column_index, value in enumerate(row):
            reference = f"{_xlsx_column_name(column_index)}{row_index}"
            cells.append(
                f'<c r="{reference}" t="inlineStr"><is><t>{value}</t></is></c>'
            )
        sheet_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(sheet_rows)}</sheetData></worksheet>'
    )
    with zipfile.ZipFile(path, "w") as workbook:
        workbook.writestr("xl/worksheets/sheet1.xml", sheet_xml)
def _xlsx_column_name(index):
    name = ""
    index += 1
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(ord("A") + remainder) + name
    return name
def _seed_evaluated_brief(
    repo,
    *,
    topic: str = "Toceranib VEGFR monotherapy in canine splenic HSA",
    duplicate_count: int = 0,
    evaluation_weaknesses: list[str] | None = None,
) -> tuple[ResearchBriefRecord, ResearchBriefEvaluationRecord]:
    citation_1 = ResearchBriefCitation(
        citation_id="C1",
        chunk_id=uuid4(),
        research_object_id=uuid4(),
        source_key="pmc_oa",
        title="Canine hemangiosarcoma VEGFR evidence",
        source_url="https://example.org/c1",
        section_label="Results",
        quote="Canine hemangiosarcoma evidence discusses toceranib, VEGFR, KDR, response, dose, and safety.",
        relevance="Direct canine HSA VEGFR context.",
        metadata={"provenance": {"doi": "10.1/example"}, "dedupe": {"duplicate_count": duplicate_count}},
    )
    citation_2 = ResearchBriefCitation(
        citation_id="C2",
        chunk_id=uuid4(),
        research_object_id=uuid4(),
        source_key="pubmed",
        title="Comparative angiosarcoma VEGFR evidence",
        source_url="https://example.org/c2",
        section_label="Abstract",
        quote="Human angiosarcoma analog evidence links VEGFR signaling with vascular tumor therapy selection.",
        relevance="Analog human angiosarcoma support.",
    )
    finding = ResearchBriefFinding(
        claim="Toceranib/VEGFR signaling is a therapy-aware validation hypothesis for canine HSA.",
        stance="supporting",
        citations=["C1", "C2"],
        evidence_strength="medium",
        reasoning="The hypothesis has direct canine context plus comparative angiosarcoma support.",
        open_questions=["Find negative monotherapy evidence."],
    )
    result = ResearchBriefResult(
        brief_id=uuid4(),
        topic=topic,
        disease_scope="canine hemangiosarcoma and human angiosarcoma",
        final_brief="Toceranib/VEGFR should be reviewed as a validation lane [C1] [C2].",
        ranked_hypotheses=[finding],
        citations=[citation_1, citation_2],
        evidence={"citation_duplicate_count": duplicate_count},
    )
    brief = repo.upsert_research_brief(
        ResearchBriefRecord(
            brief_id=result.brief_id,
            topic=result.topic,
            disease_scope=result.disease_scope,
            source_key="pmc_oa",
            review_mode="deterministic_only",
            final_brief=result.final_brief,
            result_payload=result.model_dump(mode="json"),
            citation_count=2,
            finding_count=1,
            hypothesis_count=1,
        )
    )
    evaluation = repo.upsert_research_brief_evaluation(
        ResearchBriefEvaluationRecord(
            evaluation_id=uuid4(),
            brief_id=brief.brief_id,
            topic=brief.topic,
            source_key=brief.source_key,
            overall_score=0.78,
            passes_quality_bar=True,
            readiness="ready_for_hypothesis_review",
            summary={"duplicate_citation_count": duplicate_count},
            result_payload={
                "weaknesses": (
                    evaluation_weaknesses
                    if evaluation_weaknesses is not None
                    else ["duplicate citation repair needed"] if duplicate_count else []
                ),
                "recommendations": [],
            },
        )
    )
    return brief, evaluation
def _seed_program_committee_corpus(repo) -> None:
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id=f"PMID:program-therapy-{uuid4()}",
            content_hash=f"program-therapy-raw-{uuid4()}",
            raw_payload={"pmid": "program-therapy"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type=ResearchObjectType.PUBLICATION,
            title="Vascular ecology therapy evidence in canine hemangiosarcoma",
            abstract=(
                "Canine HSA evidence links vascular injury, coagulation, angiogenesis, "
                "KDR, VEGF, PIK3CA, extracellular vimentin, and immunotherapy opportunities."
            ),
            source_key="pubmed",
            raw_record_id=raw_record_id,
            dedupe_key=f"pmid:program-therapy-{uuid4()}",
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma and human angiosarcoma evidence discusses vascular injury, "
                "coagulation, angiogenesis, KDR, VEGF, PI3K-mTOR, PIK3CA, extracellular vimentin, "
                "anti-angiogenic combinations, endothelial antigen immunotherapy, response biomarkers, "
                "and validation readouts."
            ),
            content_hash=f"program-therapy-chunk-{uuid4()}",
        )
    )
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
_MINIMAL_MD_PDB = (
    "ATOM      1  N   ALA A   1      11.104  13.207   8.678  1.00 20.00           N\n"
    "ATOM      2  CA  ALA A   1      12.560  13.235   8.421  1.00 20.00           C\n"
    "TER\n"
    "END\n"
)
def _md_runpod_input(**overrides):
    payload = {
        "protein_pdb": _MINIMAL_MD_PDB,
        "compound_smiles": "CCO",
        "target_name": "KDR",
        "compound_name": "ethanol smoke ligand",
        "simulation_steps": 10,
        "temperature": 300.0,
        "protein_source": "unit-test minimal PDB smoke fixture",
        "ligand_source": "unit-test SMILES fixture",
        "preparation_method": "unprepared smoke input for contract validation only",
    }
    payload.update(overrides)
    return payload
def _md_queue_item(runpod_input=None) -> ValidationRequestQueueItem:
    return ValidationRequestQueueItem(
        plan_id=uuid4(),
        task_id=uuid4(),
        brief_id=uuid4(),
        topic="MD queue compute job",
        task_type="target_validation",
        title="Run a smoke MD job against KDR",
        objective="Submit one expert-approved smoke-scale MD worker test.",
        rationale="The RunPod lane needs an expert-gated input contract before live execution.",
        validation_request=ValidationRequest(
            validation_type="md",
            target_name="KDR",
            candidate_name="ethanol smoke ligand",
            objective="Run one smoke-scale MD worker test.",
            require_approval=True,
            metadata={"runpod_input": runpod_input or _md_runpod_input()},
            assay_context=ValidationAssayContext(
                disease_context="canine hemangiosarcoma and human angiosarcoma",
                species=["canine", "human"],
                model_system="Computational target or structure model with explicit source provenance.",
                assay_type="in silico MD smoke test",
                readout="worker contract acceptance and structured failure modes",
                endpoint="compute lane readiness",
            ),
        ),
    )
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


__all__ = [
    'ALL_API_SMOKE_KEYS',
    'AVMAVCTRHarvesterV2',
    'AgentFindingEscalationRequest',
    'AgentFindingEscalationResult',
    'AgentPerformanceEvaluationRequest',
    'AgentPerformanceEvaluationResult',
    'AgentPerformanceReportRequest',
    'AgentPerformanceReportResult',
    'AgentPerformanceRow',
    'AgentRunRecord',
    'AgentRunReviewRecord',
    'AgentRunner',
    'ArtifactHandle',
    'BoltzRunRequest',
    'CandidateDossierRequest',
    'ChEMBLHarvesterV2',
    'ChunkContextRequest',
    'ClaimCurationRequest',
    'ClaimCuratorAgent',
    'ClaimDirection',
    'ClaimSearchRequest',
    'ClaimSearchResult',
    'ClaimType',
    'ClinicalTrialsGovHarvesterV2',
    'CommandCenterRequest',
    'CommitHypothesisRequest',
    'ComputeJobRecord',
    'ComputeJobReportRequest',
    'ComputeJobReportResult',
    'CrossrefHarvesterV2',
    'DOIDHarvesterV2',
    'DaytonaWorkspaceRequest',
    'DaytonaWorkspaceResult',
    'DocumentChunk',
    'DoiOpenAccessFollowupQueueRequest',
    'ET',
    'EmbeddingBenchmark',
    'EmbeddingCoverageSummary',
    'EmbeddingIndexResult',
    'EmbeddingMaintenanceResult',
    'EnsemblComparaHarvesterV2',
    'EnsemblXrefsHarvesterV2',
    'EntityAlias',
    'EntityLookupIndexRequest',
    'EntityLookupRequest',
    'EntityMention',
    'EuropePMCHarvesterV2',
    'EvidenceFitAssessment',
    'EvidenceGapResolverRequest',
    'EvidenceGapResolverResult',
    'EvidenceLevel',
    'EvidenceRefRepairItem',
    'EvidenceRefRepairReport',
    'EvidenceRefRepairRequest',
    'FullTextOpsAction',
    'FullTextOpsAgent',
    'FullTextOpsRequest',
    'FullTextOpsResult',
    'FullTextTriageAgent',
    'FullTextTriageRequest',
    'GEOHarvesterV2',
    'HARVESTERS_V2',
    'HGNCHarvesterV2',
    'HOSTED_API_REPORT_KEYS',
    'HSAResearchService',
    'HypothesisPromotionCandidate',
    'HypothesisPromotionReportRequest',
    'HypothesisProposalRequest',
    'ICDCHarvesterV2',
    'InMemoryResearchRepository',
    'IngestionResult',
    'LITERATURE_CLINICAL_SMOKE_KEYS',
    'LITERATURE_CORPUS_SOURCE_KEYS',
    'LITERATURE_CORPUS_SOURCE_LIMITS',
    'LITERATURE_FULL_TEXT_SOURCE_KEYS',
    'LITERATURE_FULL_TEXT_SOURCE_LIMITS',
    'LocalDeterministicEmbeddingProvider',
    'LocalIngestionPipeline',
    'LocalRuleClaimExtractor',
    'MDExpertAgentReviewRequest',
    'MDExpertAgentReviewResult',
    'MDExpertApprovalRecord',
    'MDExpertReviewPacketRecord',
    'MDInputPacket',
    'MONDOHarvesterV2',
    'NCBIGeneHarvesterV2',
    'NeonBranchWorkspaceRequest',
    'NeonBranchWorkspaceResult',
    'OMAHarvesterV2',
    'OmicsAccessionHuntRequest',
    'OmicsAccessionHuntResult',
    'OmicsEvidencePacketRequest',
    'OmicsEvidencePacketResult',
    'OmicsFollowupRequest',
    'OmicsFollowupResult',
    'OmicsFollowupTask',
    'OmicsGeneSetScore',
    'OmicsLocusSignalRequest',
    'OmicsLocusSignalResult',
    'OmicsReadoutRequest',
    'OmicsReadoutResult',
    'OpenAlexHarvesterV2',
    'OpenFDAAnimalEventsHarvesterV2',
    'OpenRouterEmbeddingProvider',
    'PMCOAHarvesterV2',
    'PrimitiveCallEvent',
    'ProofCapsuleLibraryRequest',
    'ProofCapsuleLibraryResult',
    'ProofCapsuleRecord',
    'ProofCapsuleSourceRef',
    'ProofCapsuleSubmitRequest',
    'ProofCapsuleSubmitResult',
    'ProofCapsuleSummary',
    'ProofCapsuleTarget',
    'PubChemHarvesterV2',
    'PubMedHarvesterV2',
    'PubMedIdentifierRepairRequest',
    'PublicCandidateDecisionEvent',
    'PublicCandidateGenerateRequest',
    'PublicCandidateIntegrityReportRequest',
    'PublicCandidateLibraryRequest',
    'PublicCandidateRecord',
    'PublicCandidateSnapshot',
    'RCSBPDBHarvesterV2',
    'RawSourceRecord',
    'ReactomeHarvesterV2',
    'ResearchBriefCitation',
    'ResearchBriefEvaluationRecord',
    'ResearchBriefEvaluationRequest',
    'ResearchBriefEvaluationResult',
    'ResearchBriefFinding',
    'ResearchBriefFollowupQueueRequest',
    'ResearchBriefFollowupQueueResult',
    'ResearchBriefOperatorDocRequest',
    'ResearchBriefOperatorDocResult',
    'ResearchBriefPerspectiveReport',
    'ResearchBriefQualityReportRequest',
    'ResearchBriefQueueBatchRequest',
    'ResearchBriefQueueItem',
    'ResearchBriefQueueMaintenanceRequest',
    'ResearchBriefQueueRequest',
    'ResearchBriefQueueRunRequest',
    'ResearchBriefRecord',
    'ResearchBriefRequest',
    'ResearchBriefResult',
    'ResearchChunkSearchRequest',
    'ResearchFollowupLeadResult',
    'ResearchFollowupLoopRequest',
    'ResearchFollowupLoopResult',
    'ResearchFollowupRefinementRequest',
    'ResearchFollowupResolverRequest',
    'ResearchFollowupResolverResult',
    'ResearchHuntLeadQueueRow',
    'ResearchHuntQueueMaintenanceItem',
    'ResearchHuntQueueMaintenanceRequest',
    'ResearchHuntQueueMaintenanceResult',
    'ResearchHuntQueueReportRequest',
    'ResearchHuntQueueReportResult',
    'ResearchHuntSynthesisDocRequest',
    'ResearchHuntSynthesisDocResult',
    'ResearchHuntSynthesisDocument',
    'ResearchHuntSynthesisQueueRequest',
    'ResearchHuntSynthesisQueueResult',
    'ResearchHuntTaskQueueRow',
    'ResearchHuntTaskRunRequest',
    'ResearchLeadCollectRequest',
    'ResearchLeadRecord',
    'ResearchObject',
    'ResearchObjectReadRequest',
    'ResearchObjectType',
    'ResearchProgramBoardRequest',
    'ResearchProgramBoardResult',
    'ResearchProgramEvidenceLoopRequest',
    'ResearchProgramEvidenceLoopResult',
    'ResearchProgramEvidenceTask',
    'ResearchProgramQuestion',
    'ResearchProgramRecord',
    'ResearchProgramReviewRequest',
    'ResearchProgramReviewResult',
    'ResearchRepositoryResource',
    'ResearchWorkspaceCheckoutManifestRequest',
    'ResearchWorkspaceCheckoutManifestResult',
    'ResearchWorkspaceCleanupRequest',
    'ResearchWorkspaceCleanupResult',
    'ResearchWorkspaceLibraryRequest',
    'ResearchWorkspaceLibraryResult',
    'ResearchWorkspaceRecord',
    'ResolvedEntity',
    'RetrievalSmokeRequest',
    'RewardEventRecord',
    'RewardEventSyncRequest',
    'RewardReportRequest',
    'RewardReportRow',
    'RunManifestRecord',
    'RunStatus',
    'SQLiteResearchRepository',
    'SRAHarvesterV2',
    'ScrapeBridge',
    'ScrapeFetchRequest',
    'ScrapeIngestRequest',
    'ScrapeManifestFetchRequest',
    'ScrapeManifestRequest',
    'ScrapeProfileReviewRequest',
    'ScrapeReviewRecord',
    'ScrapeReviewRequest',
    'ScrapeSourceProfile',
    'SimpleNamespace',
    'SourceFollowupIngestRequest',
    'SourceFollowupIngestResult',
    'SourceFollowupQueueItem',
    'SourceFollowupQueueRequest',
    'SourceQuery',
    'SourceScoutAgent',
    'SourceScoutRequest',
    'SourceVersionRecord',
    'TextEmbedding',
    'TextEmbeddingSearchRequest',
    'TherapyCommitteeRequest',
    'TherapyCommitteeResult',
    'TherapyCommitteeValidationQueueRequest',
    'TherapyCommitteeValidationQueueResult',
    'TherapyIdea',
    'TherapyIdeaLibraryRequest',
    'TherapyIdeaRecord',
    'UTC',
    'UUID',
    'UniChemHarvesterV2',
    'UniProtHarvesterV2',
    'UnpaywallHarvesterV2',
    'VGNCHarvesterV2',
    'ValidationAgentResult',
    'ValidationAssayContext',
    'ValidationAutopilotRequest',
    'ValidationDecisionPacket',
    'ValidationDecisionRecord',
    'ValidationDecisionReportRequest',
    'ValidationDecisionReportResult',
    'ValidationError',
    'ValidationGapSourceIngestRequest',
    'ValidationGapSourceIngestResult',
    'ValidationGapSourcePackRequest',
    'ValidationGapSourcePackResult',
    'ValidationGapSourceQuery',
    'ValidationPacket',
    'ValidationPacketAddendumBrief',
    'ValidationPacketEvidenceAddendum',
    'ValidationPacketRequest',
    'ValidationPacketResult',
    'ValidationPlanRecord',
    'ValidationPlanRequest',
    'ValidationPlanResult',
    'ValidationPlanTask',
    'ValidationRequest',
    'ValidationRequestQueueItem',
    'ValidationRequestQueueRequest',
    'ValidationToolCapability',
    'ValidationToolCatalogRequest',
    'ValidationToolMatchRequest',
    'WikiPathwaysHarvesterV2',
    'XLinkedArticleFollowupRequest',
    'XLinkedArticleFollowupResult',
    'XLinkedArticleReviewAction',
    'XLinkedArticleReviewRequest',
    'XLinkedArticleReviewResult',
    'XTopicLinkedSource',
    'XTopicReviewAction',
    'XTopicReviewRequest',
    'XTopicReviewResult',
    '_FailingNeonBranchClient',
    '_FakeNeonBranchClient',
    '_MINIMAL_MD_PDB',
    '_avma_vctr_study_card',
    '_cleanup_workspace',
    '_contains_key',
    '_md_queue_item',
    '_md_runpod_input',
    '_ready_for_therapy_ideas_program',
    '_research_program_fixture',
    '_seed_evaluated_brief',
    '_seed_full_text_source_claim',
    '_seed_minimal_source_claim',
    '_seed_program_committee_corpus',
    '_write_minimal_xlsx',
    '_xlsx_column_name',
    'agent_performance',
    'backfill_deep_dives',
    'backfill_papers_json',
    'build_chunk_embedding_text',
    'build_embedding_provider',
    'build_proof_capsule_library',
    'build_research_workspace_checkout_manifest',
    'build_scholarly_source_queries',
    'build_source_health_report',
    'build_structured_source_count_report',
    'build_validation_tool_task',
    'candidate_contribution_intake',
    'chunk_text',
    'cleanup_neon_research_workspaces',
    'cli_module',
    'command_center_web',
    'compute_runners',
    'dagster_asset_module',
    'datetime',
    'default_embedding_model_for_environment',
    'entity_resolution',
    'evidence_fit',
    'extract_claims_for_repository',
    'full_text_ops',
    'full_text_source_qa',
    'get_validation_tool',
    'harvesters_v2',
    'index_embeddings_for_repository',
    'infer_comparative_scope',
    'json',
    'list_scrape_profiles',
    'list_validation_tool_catalog',
    'local_ingest_module',
    'maintain_embedding_index',
    'make_service',
    'mcp_server',
    'md_expert_agent',
    'model_policy',
    'normalize_entity_key',
    'omics_accession_hunt',
    'plan_daytona_workspace',
    'provision_neon_branch_workspace',
    'pubmed_identifier_repair',
    'pytest',
    'research_brief_agent',
    'research_brief_evaluation',
    'research_followup_resolver',
    'research_program_board',
    'resolve_entities_for_repository',
    'run_embedding_bakeoff',
    'run_structured_sources_pipeline',
    'scraper_bridge',
    'select_embedding_model_from_coverage',
    'service_module',
    'source_followup',
    'source_query_params',
    'sqlite3',
    'storage',
    'structured_orchestration',
    'structured_source_qa',
    'submit_proof_capsule',
    'sys',
    'therapy_committee',
    'timedelta',
    'uuid4',
    'validation_agents',
    'validation_gap_ingest',
    'x_topic_monitor',
    'x_topic_review',
    'zipfile',
]
