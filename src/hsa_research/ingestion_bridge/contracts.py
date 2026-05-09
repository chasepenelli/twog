"""Typed contracts for Ingestion Bridge v2.

These models are the shared contract between MCP tools, Dagster assets,
service functions, and future UI/API surfaces.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
import re
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator

from .research_brief_errors import split_research_brief_errors


class StrictBaseModel(BaseModel):
    """Base model that keeps contracts explicit but allows future metadata."""

    model_config = ConfigDict(extra="forbid", use_enum_values=True)


def re_search_citation_token(value: str) -> bool:
    return bool(re.search(r"\[C\d+\]", value))


def _normalize_optional_url(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized.split("#", 1)[0].rstrip(").,;")


def _identity_slug(value: str) -> str:
    return "-".join(re.findall(r"[a-z0-9]+", value.lower()))[:160] or "unknown"


def _dedupe_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        deduped.append(normalized)
        seen.add(normalized)
    return deduped


def _normalized_unique_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        key = normalized.casefold()
        if not normalized or key in seen:
            continue
        deduped.append(normalized)
        seen.add(key)
    return deduped


def _dedupe_lower_tokens(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = re.sub(r"\s+", "_", str(value).strip().lower())
        if not normalized or normalized in seen:
            continue
        deduped.append(normalized)
        seen.add(normalized)
    return deduped


class SourceKind(str, Enum):
    SCHOLARLY_METADATA = "scholarly_metadata"
    OPEN_ACCESS_FULL_TEXT = "open_access_full_text"
    VETERINARY_TRIAL = "veterinary_trial"
    CLINICAL_TRIAL = "clinical_trial"
    CANINE_ONCOLOGY = "canine_oncology"
    OMICS = "omics"
    CHEMISTRY = "chemistry"
    TARGET_STRUCTURE = "target_structure"
    SAFETY = "safety"
    INTERNAL = "internal"


class ResearchObjectType(str, Enum):
    PUBLICATION = "publication"
    PREPRINT = "preprint"
    CLINICAL_TRIAL = "clinical_trial"
    VETERINARY_TRIAL = "veterinary_trial"
    DATASET = "dataset"
    COMPOUND_RECORD = "compound_record"
    BIOACTIVITY_ASSAY = "bioactivity_assay"
    SAFETY_REPORT = "safety_report"
    DRUG_LABEL = "drug_label"
    STRUCTURE = "structure"
    VALIDATION_RUN = "validation_run"
    KNOWLEDGE_ENTRY = "knowledge_entry"


class ClaimType(str, Enum):
    TARGET_ASSOCIATED_WITH_DISEASE = "target_associated_with_disease"
    COMPOUND_MODULATES_TARGET = "compound_modulates_target"
    COMPOUND_AFFECTS_OUTCOME = "compound_affects_outcome"
    PATHWAY_ACTIVE_IN_DISEASE = "pathway_active_in_disease"
    BIOMARKER_PREDICTS_STATE = "biomarker_predicts_state"
    SAFETY_SIGNAL = "safety_signal"
    SPECIES_TRANSLATION = "species_translation"
    VALIDATION_RESULT = "validation_result"
    OTHER = "other"


class EvidenceLevel(str, Enum):
    IN_SILICO = "in_silico"
    IN_VITRO = "in_vitro"
    EX_VIVO = "ex_vivo"
    ANIMAL_MODEL = "animal_model"
    CANINE_CLINICAL = "canine_clinical"
    HUMAN_CLINICAL = "human_clinical"
    REVIEW = "review"
    UNKNOWN = "unknown"


class ClaimDirection(str, Enum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class ClaimCurationDecision(str, Enum):
    PROMOTE = "promote"
    MERGE_DUPLICATE = "merge_duplicate"
    NEEDS_REVIEW = "needs_review"
    REJECT = "reject"


class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    NEEDS_APPROVAL = "needs_approval"


class ToolMode(str, Enum):
    READ = "read"
    DRAFT = "draft"
    WRITE = "write"
    ASYNC_COMPUTE = "async_compute"


FullTextOpsActionName = Literal[
    "mark_clean",
    "run_ingest_smoke",
    "run_full_text_smoke",
    "run_source_date_partition",
    "reduce_batch_size",
    "inspect_parser",
    "inspect_license",
    "keep_schedule_stopped",
    "ready_to_enable_schedule",
    "needs_human_review",
]

AgentSeverity = Literal["info", "watch", "blocking"]

XTopicReviewActionName = Literal[
    "flag_for_ingestion",
    "queue_source_followup",
    "needs_link_review",
    "needs_human_review",
    "reject_noise",
    "compliance_hold",
    "skip_no_durable_source",
]

XLinkedArticleReviewActionName = Literal[
    "queue_primary_source_followup",
    "needs_human_review",
    "reject_low_signal",
]

ResearchBriefPerspectiveName = Literal[
    "evidence_scout",
    "translational_hypothesis",
    "skeptic_validation",
]

ResearchBriefStance = Literal[
    "supporting",
    "contradicting",
    "uncertain",
    "opportunity",
    "risk",
]

ResearchBriefEvidenceStrength = Literal["high", "medium", "low", "unknown"]

TherapyCommitteePerspectiveName = Literal[
    "target_biology",
    "drug_repurposing",
    "translational_clinical",
    "peptide_specialist",
    "skeptic_risk",
]

ValidationToolCategory = Literal[
    "expert_review",
    "assay_design",
    "target_expression",
    "biomarker_response",
    "omics_expression",
    "mutation_function",
    "peptide_specialist",
    "safety_translational_risk",
]

ValidationToolRunnerStatus = Literal["recommend_only", "dry_run", "live"]

ValidationToolComputeProfile = Literal[
    "manual_review",
    "local_cpu",
    "llm_review",
    "wet_lab_planning",
    "external_compute",
    "gpu_compute",
]

TherapyIdeaStatus = Literal[
    "proposed",
    "ready_for_promotion",
    "queued_for_validation",
    "archived",
    "rejected",
]

HypothesisPromotionState = Literal[
    "needs_citation_repair",
    "needs_more_evidence",
    "ready_for_committee",
    "ready_for_validation_plan",
    "queued_for_validation",
    "blocked",
]

HypothesisPromotionSourceType = Literal["research_brief_hypothesis", "therapy_idea"]

ValidationPacketStatus = Literal[
    "draft",
    "ready_for_review",
    "queued_for_validation",
    "blocked",
    "archived",
]

ValidationPacketReadiness = Literal[
    "ready_for_committee",
    "ready_for_validation_plan",
    "ready_for_validation_queue",
    "queued_for_validation",
    "needs_more_evidence",
    "needs_citation_repair",
    "blocked",
]

DiscoveryReadiness = Literal[
    "ready_for_validation_strategy",
    "needs_more_evidence",
    "needs_citation_repair",
    "blocked",
]

ValidationStrategyReadiness = Literal[
    "ready_for_validation_strategy",
    "queued_for_validation_strategy",
    "needs_more_evidence",
    "needs_citation_repair",
    "blocked",
]

ProtocolReadiness = Literal[
    "not_protocol_ready",
    "needs_protocol_inputs",
    "ready_for_protocol_review",
    "blocked",
]

ResearchProgramStatus = Literal[
    "proposed",
    "active",
    "needs_one_more_pass",
    "ready_for_therapy_ideas",
    "ready_for_validation_strategy",
    "archived",
]

ResearchProgramGateDecision = Literal[
    "archive",
    "needs_one_more_pass",
    "ready_for_therapy_ideas",
    "ready_for_validation_strategy",
]

ResearchProgramEvidenceTaskStatus = Literal[
    "proposed",
    "queued",
    "completed",
    "failed",
    "skipped",
]

ResearchProgramEvidenceTaskType = Literal[
    "literature_search",
    "full_text_review",
    "omics_lookup",
    "clinical_trial_lookup",
    "drug_target_lookup",
    "safety_lookup",
    "expert_review",
    "x_topic_scan",
    "other",
]

CommandCenterArea = Literal[
    "brief_queue",
    "research_leads",
    "source_health",
    "embeddings",
    "full_text",
    "agents",
]
CommandCenterSeverity = Literal["info", "watch", "blocking"]

XTopicIdentifierType = Literal[
    "doi",
    "pmid",
    "pmcid",
    "nct",
    "pubchem",
    "chembl",
    "uniprot",
    "rcsb_pdb",
    "geo",
    "sra",
    "unknown",
]

SourceFollowupStatus = Literal[
    "queued",
    "approved",
    "ingested",
    "failed",
    "skipped",
    "rejected",
]

ResearchLeadType = Literal[
    "conference_abstract",
    "institutional_article",
    "press_release",
    "preprint",
    "social_post",
    "linked_article",
    "unknown",
]

ResearchLeadStatus = Literal[
    "new",
    "watching",
    "followup",
    "queued",
    "ingested",
    "dismissed",
    "archived",
]

ResearchBriefStatus = Literal[
    "completed",
    "failed",
    "archived",
]

ResearchBriefQueueStatus = Literal[
    "queued",
    "running",
    "completed",
    "failed",
    "archived",
]

ResearchBriefQueueBatchMode = Literal["research_leads", "source_health", "both"]
SourceHealthStatus = Literal["healthy", "watch", "triage", "failing"]
EvidenceGapType = Literal["missing_evidence", "risk", "next_action"]
EvidenceGapResolverLane = Literal[
    "mutation_function",
    "clinical_response",
    "pkpd",
    "safety_signal",
    "assay_protocol",
    "trial_design",
    "omics_context",
    "general_evidence",
]

ValidationGapEvidenceLane = Literal[
    "mutation_function",
    "clinical_response",
    "pkpd",
    "safety_signal",
    "assay_protocol",
    "trial_design",
    "omics_context",
    "species_translation",
    "chemistry",
    "general_evidence",
]
EvidenceFitLevel = Literal["weak", "partial", "strong"]
EvidenceTransferRiskLevel = Literal["low", "moderate", "high", "unknown"]

ResearchBriefEvaluationReadiness = Literal[
    "ready_for_hypothesis_review",
    "needs_more_evidence",
    "needs_human_review",
    "blocked",
]

ValidationPlanStatus = Literal[
    "draft",
    "ready_for_review",
    "blocked",
    "archived",
]

ValidationPlanReadiness = Literal[
    "ready_for_expert_review",
    "needs_better_synthesis",
    "blocked",
]

ValidationPlanTaskType = Literal[
    "literature_review",
    "expert_review",
    "target_validation",
    "protein_structure",
    "compound_screen",
    "docking",
    "boltz",
    "md",
    "admet",
    "safety",
    "omics",
    "wet_lab",
    "partner_review",
]

ValidationRequestQueueStatus = Literal[
    "needs_approval",
    "queued",
    "approved",
    "blocked",
    "dispatched",
    "completed",
    "failed",
    "rejected",
    "archived",
]


class AgentRunRecord(StrictBaseModel):
    agent_run_id: UUID = Field(default_factory=uuid4)
    agent_name: str
    agent_version: str = "v1"
    model_profile: str = "deterministic"
    status: RunStatus = RunStatus.QUEUED
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None
    source_key: str | None = None
    partition_date: str | None = None
    dagster_run_id: str | None = None
    input_payload: dict[str, Any] = Field(default_factory=dict)
    output_payload: dict[str, Any] = Field(default_factory=dict)
    summary: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


AgentRunReviewVerdict = Literal["useful", "needs_followup", "bad", "unclear"]
AgentRunReviewerType = Literal["operator", "llm_evaluator", "system"]
AgentPerformanceGroupType = Literal["agent_name", "model_profile", "model_key", "prompt_key"]


class AgentRunReviewRecord(StrictBaseModel):
    review_id: UUID = Field(default_factory=uuid4)
    agent_run_id: UUID
    reviewer: str = "command_center_operator"
    reviewer_type: AgentRunReviewerType = "operator"
    verdict: AgentRunReviewVerdict
    feedback: str | None = None
    tags: list[str] = Field(default_factory=list)
    followup_actions: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_review(self) -> AgentRunReviewRecord:
        self.reviewer = self.reviewer.strip() or "command_center_operator"
        self.feedback = self.feedback.strip() if self.feedback and self.feedback.strip() else None
        self.tags = _dedupe_lower_tokens(self.tags)
        self.followup_actions = _dedupe_lower_tokens(self.followup_actions)
        return self


class AgentPerformanceReportRequest(StrictBaseModel):
    agent_name: str | None = None
    status: str | None = None
    source_key: str | None = None
    limit: int = Field(default=500, ge=1, le=2000)
    review_limit: int | None = Field(default=None, ge=1, le=10000)
    min_sample_size: int = Field(default=3, ge=1, le=100)


class AgentPerformanceRow(StrictBaseModel):
    group_type: AgentPerformanceGroupType
    group_value: str
    run_count: int = Field(default=0, ge=0)
    reviewed_run_count: int = Field(default=0, ge=0)
    unreviewed_run_count: int = Field(default=0, ge=0)
    operator_reviewed_count: int = Field(default=0, ge=0)
    evaluator_reviewed_count: int = Field(default=0, ge=0)
    useful_count: int = Field(default=0, ge=0)
    needs_followup_count: int = Field(default=0, ge=0)
    bad_count: int = Field(default=0, ge=0)
    unclear_count: int = Field(default=0, ge=0)
    useful_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    followup_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    bad_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    unclear_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    review_coverage: float = Field(default=0.0, ge=0.0, le=1.0)
    performance_score: int | None = Field(default=None, ge=0, le=100)
    disagreement_count: int = Field(default=0, ge=0)
    low_sample: bool = False
    latest_run_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class AgentPerformanceReportResult(StrictBaseModel):
    agent_run_count: int = Field(default=0, ge=0)
    reviewed_run_count: int = Field(default=0, ge=0)
    unreviewed_run_count: int = Field(default=0, ge=0)
    operator_reviewed_count: int = Field(default=0, ge=0)
    evaluator_reviewed_count: int = Field(default=0, ge=0)
    disagreement_count: int = Field(default=0, ge=0)
    review_coverage: float = Field(default=0.0, ge=0.0, le=1.0)
    verdict_counts: dict[str, int] = Field(default_factory=dict)
    reviewer_type_counts: dict[str, int] = Field(default_factory=dict)
    rows: list[AgentPerformanceRow] = Field(default_factory=list)
    top_rows: list[AgentPerformanceRow] = Field(default_factory=list)
    bottom_rows: list[AgentPerformanceRow] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class AgentPerformanceEvaluationRequest(StrictBaseModel):
    agent_run_ids: list[UUID] = Field(default_factory=list, max_length=100)
    agent_name: str | None = None
    status: str | None = "completed"
    source_key: str | None = None
    limit: int = Field(default=25, ge=1, le=100)
    reviewed_only: bool = True
    model_profile: str = "agent_performance_evaluator"
    review_mode: Literal["openrouter_required"] = "openrouter_required"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class AgentPerformanceEvaluationResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    agent_name: str = "agent_performance_evaluator_agent"
    agent_version: str = "v1"
    model_profile: str = "agent_performance_evaluator"
    reviewed_only: bool = True
    scanned_count: int = Field(default=0, ge=0)
    candidate_count: int = Field(default=0, ge=0)
    evaluated_count: int = Field(default=0, ge=0)
    review_created_count: int = Field(default=0, ge=0)
    failed_count: int = Field(default=0, ge=0)
    review_ids: list[UUID] = Field(default_factory=list)
    evaluations: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchLeadRecord(StrictBaseModel):
    lead_id: UUID = Field(default_factory=uuid4)
    identity_key: str | None = None
    title: str | None = None
    url: str | None = None
    lead_type: ResearchLeadType = "unknown"
    status: ResearchLeadStatus = "new"
    priority: int = Field(default=100, ge=0, le=1000)
    source_key: str | None = None
    origin_source_key: str | None = None
    origin_record_id: str | None = None
    origin_review_id: UUID | None = None
    origin_artifact_id: UUID | None = None
    origin_agent_run_id: UUID | None = None
    reason: str | None = None
    summary: str | None = None
    evidence_refs: list[str] = Field(default_factory=list)
    topic_tags: list[str] = Field(default_factory=list)
    identifiers: dict[str, str] = Field(default_factory=dict)
    suggested_sources: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_identity(self) -> "ResearchLeadRecord":
        self.title = self.title.strip() if self.title else None
        self.url = _normalize_optional_url(self.url)
        self.topic_tags = _dedupe_lower_tokens(self.topic_tags)
        self.evidence_refs = _dedupe_strings(self.evidence_refs)
        self.suggested_sources = _dedupe_lower_tokens(self.suggested_sources)
        if not self.identity_key:
            if self.url:
                self.identity_key = f"research_lead:url:{self.url.lower()}"
            elif self.origin_agent_run_id and (self.origin_review_id or self.origin_record_id):
                origin = self.origin_review_id or self.origin_record_id
                self.identity_key = f"research_lead:origin:{self.origin_agent_run_id}:{origin}"
            elif self.title:
                self.identity_key = f"research_lead:title:{_identity_slug(self.title)}"
            else:
                self.identity_key = f"research_lead:id:{self.lead_id}"
        else:
            self.identity_key = self.identity_key.strip()
        return self


class ResearchLeadCollectRequest(StrictBaseModel):
    agent_names: list[str] = Field(
        default_factory=lambda: ["x_linked_article_review_agent", "x_topic_review_agent"],
        max_length=20,
    )
    statuses: list[str] = Field(default_factory=lambda: ["completed"], max_length=10)
    limit: int = Field(default=50, ge=1, le=500)
    include_existing: bool = False


class ResearchLeadCollectResult(StrictBaseModel):
    agent_runs_seen: int = 0
    leads_created: int = 0
    skipped_existing: int = 0
    items: list[ResearchLeadRecord] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


ResearchFollowupResolverActionName = Literal[
    "queued_source_followup",
    "ingested_source_followup",
    "searched_durable_sources",
    "promoted_to_watching",
    "manual_research_required",
    "kept_in_followup",
    "failed",
]


class ResearchFollowupResolverRequest(StrictBaseModel):
    lead_ids: list[UUID] = Field(default_factory=list, max_length=100)
    statuses: list[ResearchLeadStatus] = Field(default_factory=lambda: ["followup"], max_length=10)
    source_keys: list[str] = Field(default_factory=list, max_length=50)
    search_source_keys: list[str] = Field(default_factory=list, max_length=20)
    search_query_text: str | None = Field(default=None, max_length=500)
    limit: int = Field(default=25, ge=1, le=200)
    ingest_source_followups: bool = True
    search_missing_identifiers: bool = True
    promote_ready_leads: bool = True
    run_claim_extraction: bool = True
    dry_run: bool = False
    force_live_search: bool = False
    inspect_evidence_refs: bool = True
    min_evidence_chunks: int = Field(default=1, ge=1, le=25)
    evidence_inspection_limit: int = Field(default=8, ge=1, le=50)
    search_limit_per_source: int = Field(default=2, ge=1, le=25)
    max_search_terms: int = Field(default=12, ge=3, le=30)
    approved_by: str | None = None
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_research_followup_resolver_request(self) -> "ResearchFollowupResolverRequest":
        if self.search_query_text is not None:
            self.search_query_text = re.sub(r"\s+", " ", self.search_query_text).strip() or None
        return self


class ResearchFollowupLeadResult(StrictBaseModel):
    lead_id: UUID
    title: str | None = None
    status_before: ResearchLeadStatus
    status_after: ResearchLeadStatus
    actions: list[ResearchFollowupResolverActionName] = Field(default_factory=list)
    source_followup_ids: list[UUID] = Field(default_factory=list)
    evidence_refs: list[str] = Field(default_factory=list)
    durable_source_keys: list[str] = Field(default_factory=list)
    manual_research_required: bool = False
    promoted: bool = False
    errors: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResearchFollowupResolverResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    agent_name: str = "research_followup_resolver_agent"
    model_profile: str = "deterministic_resolver"
    dry_run: bool = False
    force_live_search: bool = False
    blocked: bool = False
    leads_seen: int = 0
    skipped_leads: int = 0
    unresolved_lead_ids: list[UUID] = Field(default_factory=list)
    skip_reasons: list[dict[str, Any]] = Field(default_factory=list)
    source_followups_queued: int = 0
    source_followups_ingested: int = 0
    durable_source_searches: int = 0
    evidence_inspections: int = 0
    promoted_leads: int = 0
    manual_research_required: int = 0
    kept_in_followup: int = 0
    failed_leads: int = 0
    lead_results: list[ResearchFollowupLeadResult] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


PubMedIdentifierRepairStatus = Literal["clean", "repaired", "would_repair", "skipped", "conflict", "failed"]


class PubMedIdentifierRepairRequest(StrictBaseModel):
    pmids: list[str] = Field(default_factory=list, max_length=500)
    limit: int = Field(default=250, ge=1, le=5000)
    batch_size: int = Field(default=100, ge=1, le=200)
    dry_run: bool = True

    @model_validator(mode="after")
    def normalize_pubmed_identifier_repair_request(self) -> "PubMedIdentifierRepairRequest":
        self.pmids = _normalized_unique_strings(self.pmids)
        return self


class PubMedIdentifierRepairItem(StrictBaseModel):
    object_id: UUID
    pmid: str
    status: PubMedIdentifierRepairStatus
    old_dedupe_key: str | None = None
    new_dedupe_key: str | None = None
    old_identifiers: dict[str, str] = Field(default_factory=dict)
    new_identifiers: dict[str, str] = Field(default_factory=dict)
    error: str | None = None


class PubMedIdentifierRepairResult(StrictBaseModel):
    dry_run: bool = True
    scanned_objects: int = 0
    fetched_pmids: int = 0
    clean: int = 0
    repaired: int = 0
    would_repair: int = 0
    skipped: int = 0
    conflicts: int = 0
    failed: int = 0
    items: list[PubMedIdentifierRepairItem] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class FullTextOpsRequest(StrictBaseModel):
    source_keys: list[str] = Field(default_factory=list)
    partition_date: str | None = None
    source_health_report: dict[str, Any] | None = None
    full_text_report: dict[str, Any] | None = None
    recent_run_limit: int = Field(default=10, ge=0, le=100)
    model_profile: str = "reviewer"
    review_mode: Literal[
        "external_required",
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "openrouter_required"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class FullTextOpsAction(StrictBaseModel):
    source_key: str
    action: FullTextOpsActionName
    severity: AgentSeverity
    reason: str
    dagster_job_name: str | None = None
    partition_date: str | None = None
    evidence_refs: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class FullTextOpsResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    agent_name: str = "full_text_ops_agent"
    model_profile: str = "reviewer"
    actions: list[FullTextOpsAction] = Field(default_factory=list)
    should_block_schedule: bool = False
    schedule_readiness: Literal[
        "ready_to_enable",
        "needs_partition_validation",
        "keep_stopped",
        "blocked",
    ] = "keep_stopped"
    evidence: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchBriefRequest(StrictBaseModel):
    topic: str = Field(min_length=3, max_length=1000)
    disease_scope: str = Field(default="canine hemangiosarcoma and human angiosarcoma", max_length=500)
    source_key: str | None = None
    max_chunks_per_perspective: int = Field(default=8, ge=1, le=25)
    max_claims: int = Field(default=12, ge=0, le=50)
    max_chunk_chars: int = Field(default=1800, ge=500, le=12000)
    brief_style: Literal["technical", "operator", "substack", "vet_partner"] = "technical"
    model_profile: str = "research_brief"
    review_mode: Literal[
        "external_required",
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "openrouter_required"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResearchBriefCitation(StrictBaseModel):
    citation_id: str
    chunk_id: UUID
    research_object_id: UUID
    source_key: str | None = None
    title: str | None = None
    source_url: str | None = None
    section_label: str | None = None
    quote: str = Field(min_length=1, max_length=1200)
    relevance: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResearchBriefFinding(StrictBaseModel):
    claim: str = Field(min_length=1, max_length=2000)
    stance: ResearchBriefStance
    citations: list[str] = Field(min_length=1, max_length=10)
    evidence_strength: ResearchBriefEvidenceStrength = "unknown"
    reasoning: str = Field(min_length=1, max_length=3000)
    open_questions: list[str] = Field(default_factory=list, max_length=10)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def normalize_model_citations(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "citations" not in data:
            return data
        citations = data.get("citations")
        if not isinstance(citations, list):
            return data
        normalized = _dedupe_strings([str(value) for value in citations])
        if len(normalized) > 10:
            data = {**data}
            data["citations"] = normalized[:10]
            metadata = dict(data.get("metadata") or {})
            metadata["citation_truncation"] = {
                "original_count": len(normalized),
                "kept_count": 10,
                "dropped_citations": normalized[10:],
            }
            data["metadata"] = metadata
        else:
            data = {**data, "citations": normalized}
        return data


class ResearchBriefPerspectiveReport(StrictBaseModel):
    agent_run_id: UUID | None = None
    perspective: ResearchBriefPerspectiveName
    agent_name: str
    model_profile: str = "research_brief"
    summary: str = Field(min_length=1, max_length=3000)
    findings: list[ResearchBriefFinding] = Field(default_factory=list, max_length=20)
    citations: list[ResearchBriefCitation] = Field(default_factory=list)
    evidence: dict[str, Any] = Field(default_factory=dict)
    evidence_limitations: list[str] = Field(default_factory=list, max_length=50)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def _findings_must_use_known_citations(self) -> "ResearchBriefPerspectiveReport":
        known = {citation.citation_id for citation in self.citations}
        unknown = [
            citation_id
            for finding in self.findings
            for citation_id in finding.citations
            if citation_id not in known
        ]
        if unknown:
            raise ValueError(f"findings reference unknown citations: {sorted(set(unknown))}")
        return self


class ResearchBriefResult(StrictBaseModel):
    brief_id: UUID | None = None
    agent_run_id: UUID | None = None
    agent_run_ids: list[UUID] = Field(default_factory=list)
    agent_name: str = "research_synthesis_editor_agent"
    topic: str
    disease_scope: str
    brief_style: Literal["technical", "operator", "substack", "vet_partner"] = "technical"
    model_profile: str = "research_brief"
    perspective_reports: list[ResearchBriefPerspectiveReport] = Field(default_factory=list)
    final_brief: str = ""
    ranked_hypotheses: list[ResearchBriefFinding] = Field(default_factory=list)
    unresolved_questions: list[str] = Field(default_factory=list)
    citations: list[ResearchBriefCitation] = Field(default_factory=list)
    evidence: dict[str, Any] = Field(default_factory=dict)
    hard_errors: list[str] = Field(default_factory=list)
    evidence_limitations: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def _brief_must_remain_cited(self) -> "ResearchBriefResult":
        known = {citation.citation_id for citation in self.citations}
        unknown = [
            citation_id
            for finding in self.ranked_hypotheses
            for citation_id in finding.citations
            if citation_id not in known
        ]
        if unknown:
            raise ValueError(f"ranked hypotheses reference unknown citations: {sorted(set(unknown))}")
        if self.final_brief and self.citations and not re_search_citation_token(self.final_brief):
            raise ValueError("final_brief must include citation tokens such as [C1]")
        return self


class ResearchBriefRecord(StrictBaseModel):
    brief_id: UUID = Field(default_factory=uuid4)
    agent_run_id: UUID | None = None
    agent_run_ids: list[UUID] = Field(default_factory=list)
    topic: str = Field(min_length=3, max_length=1000)
    disease_scope: str = Field(default="canine hemangiosarcoma and human angiosarcoma", max_length=500)
    source_key: str | None = None
    brief_style: Literal["technical", "operator", "substack", "vet_partner"] = "technical"
    model_profile: str = "research_brief"
    review_mode: Literal[
        "external_required",
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "openrouter_required"
    status: ResearchBriefStatus = "completed"
    final_brief: str = ""
    summary: dict[str, Any] = Field(default_factory=dict)
    result_payload: dict[str, Any] = Field(default_factory=dict)
    citation_count: int = Field(default=0, ge=0)
    finding_count: int = Field(default=0, ge=0)
    hypothesis_count: int = Field(default=0, ge=0)
    unresolved_question_count: int = Field(default=0, ge=0)
    research_lead_count: int = Field(default=0, ge=0)
    hard_error_count: int = Field(default=0, ge=0)
    evidence_limitation_count: int = Field(default=0, ge=0)
    error_count: int = Field(default=0, ge=0)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_brief_summary(self) -> "ResearchBriefRecord":
        self.topic = self.topic.strip()
        self.disease_scope = self.disease_scope.strip()
        result_payload = self.result_payload or {}
        has_hard_error_count = "hard_error_count" in self.model_fields_set
        has_evidence_limitation_count = "evidence_limitation_count" in self.model_fields_set
        if not has_hard_error_count or not has_evidence_limitation_count:
            hard_errors = [
                str(error)
                for error in result_payload.get("hard_errors", [])
                if str(error).strip()
            ]
            evidence_limitations = [
                str(item)
                for item in result_payload.get("evidence_limitations", [])
                if str(item).strip()
            ]
            legacy_errors = [
                str(error)
                for error in result_payload.get("errors", [])
                if str(error).strip()
            ]
            if not hard_errors and not evidence_limitations and legacy_errors:
                hard_errors, evidence_limitations = split_research_brief_errors(legacy_errors)
            if not has_hard_error_count:
                self.hard_error_count = len(hard_errors) if hard_errors or evidence_limitations else self.error_count
            if not has_evidence_limitation_count:
                self.evidence_limitation_count = len(evidence_limitations)
        seen_agent_run_ids: set[UUID] = set()
        deduped_agent_run_ids: list[UUID] = []
        for agent_run_id in self.agent_run_ids:
            if agent_run_id in seen_agent_run_ids:
                continue
            deduped_agent_run_ids.append(agent_run_id)
            seen_agent_run_ids.add(agent_run_id)
        self.agent_run_ids = deduped_agent_run_ids
        return self


class ResearchBriefOperatorDocRequest(StrictBaseModel):
    brief_ids: list[UUID] = Field(default_factory=list, max_length=100)
    status: ResearchBriefStatus | None = "completed"
    source_key: str | None = None
    topic_query: str | None = None
    limit: int = Field(default=10, ge=1, le=100)
    max_hypotheses: int = Field(default=5, ge=1, le=20)
    max_unresolved_questions: int = Field(default=8, ge=0, le=50)
    max_evidence_limitations: int = Field(default=8, ge=0, le=50)
    max_technical_footnotes: int = Field(default=30, ge=0, le=100)
    include_technical_footnotes: bool = True
    dry_run: bool = True
    operator: str = "research_brief_operator_doc"
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_research_brief_operator_doc_request(self) -> "ResearchBriefOperatorDocRequest":
        self.brief_ids = list(dict.fromkeys(self.brief_ids))
        self.operator = self.operator.strip() or "research_brief_operator_doc"
        self.source_key = self.source_key.strip().lower() if self.source_key and self.source_key.strip() else None
        self.topic_query = self.topic_query.strip() if self.topic_query and self.topic_query.strip() else None
        return self


class ResearchBriefOperatorDocument(StrictBaseModel):
    brief_id: UUID
    title: str
    artifact_id: UUID | None = None
    artifact_uri: str | None = None
    markdown: str
    plain_language_summary: str
    citation_count: int = Field(default=0, ge=0)
    hypothesis_count: int = Field(default=0, ge=0)
    unresolved_question_count: int = Field(default=0, ge=0)
    evidence_limitation_count: int = Field(default=0, ge=0)
    technical_footnote_count: int = Field(default=0, ge=0)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResearchBriefOperatorDocResult(StrictBaseModel):
    dry_run: bool = True
    candidate_count: int = Field(default=0, ge=0)
    document_count: int = Field(default=0, ge=0)
    artifact_count: int = Field(default=0, ge=0)
    skipped_count: int = Field(default=0, ge=0)
    documents: list[ResearchBriefOperatorDocument] = Field(default_factory=list)
    artifacts: list[ArtifactHandle] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchBriefQueueItem(StrictBaseModel):
    queue_item_id: UUID = Field(default_factory=uuid4)
    identity_key: str | None = None
    status: ResearchBriefQueueStatus = "queued"
    priority: int = Field(default=100, ge=0, le=1000)
    topic: str = Field(min_length=3, max_length=1000)
    disease_scope: str = Field(default="canine hemangiosarcoma and human angiosarcoma", max_length=500)
    source_key: str | None = None
    max_chunks_per_perspective: int = Field(default=8, ge=1, le=25)
    max_claims: int = Field(default=12, ge=0, le=50)
    max_chunk_chars: int = Field(default=1800, ge=500, le=12000)
    brief_style: Literal["technical", "operator", "substack", "vet_partner"] = "technical"
    model_profile: str = "research_brief"
    review_mode: Literal[
        "external_required",
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "openrouter_required"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    last_brief_id: UUID | None = None
    last_agent_run_id: UUID | None = None
    attempts: int = Field(default=0, ge=0)
    last_error: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_queue_item(self) -> "ResearchBriefQueueItem":
        self.topic = self.topic.strip()
        self.disease_scope = self.disease_scope.strip()
        self.review_models = _dedupe_strings(self.review_models)
        if not self.identity_key:
            source = self.source_key or "all_sources"
            self.identity_key = (
                "research_brief_queue:"
                f"{_identity_slug(self.topic)}:"
                f"{_identity_slug(self.disease_scope)}:"
                f"{_identity_slug(source)}:"
                f"{self.brief_style}:"
                f"{self.review_mode}"
            )
        else:
            self.identity_key = self.identity_key.strip()
        return self


class ResearchBriefQueueRequest(StrictBaseModel):
    topic: str = Field(min_length=3, max_length=1000)
    disease_scope: str = Field(default="canine hemangiosarcoma and human angiosarcoma", max_length=500)
    source_key: str | None = None
    priority: int = Field(default=100, ge=0, le=1000)
    max_chunks_per_perspective: int = Field(default=8, ge=1, le=25)
    max_claims: int = Field(default=12, ge=0, le=50)
    max_chunk_chars: int = Field(default=1800, ge=500, le=12000)
    brief_style: Literal["technical", "operator", "substack", "vet_partner"] = "technical"
    model_profile: str = "research_brief"
    review_mode: Literal[
        "external_required",
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "openrouter_required"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResearchBriefQueueRunRequest(StrictBaseModel):
    queue_item_ids: list[UUID] = Field(default_factory=list, max_length=100)
    statuses: list[ResearchBriefQueueStatus] = Field(default_factory=lambda: ["queued"], max_length=5)
    source_key: str | None = None
    topic_query: str | None = None
    limit: int = Field(default=1, ge=1, le=10)
    dagster_run_id: str | None = None


class ResearchBriefQueueRunResult(StrictBaseModel):
    ran: bool = False
    queue_item: ResearchBriefQueueItem | None = None
    brief: ResearchBriefResult | None = None
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchBriefQueueMaintenanceRequest(StrictBaseModel):
    action: Literal["archive"] = "archive"
    queue_item_ids: list[UUID] = Field(default_factory=list, max_length=100)
    statuses: list[ResearchBriefQueueStatus] = Field(default_factory=lambda: ["failed"], max_length=5)
    source_key: str | None = None
    topic_query: str | None = None
    min_attempts: int = Field(default=1, ge=0, le=100)
    max_updated_age_hours: float = Field(default=12.0, ge=0.0, le=8760.0)
    limit: int = Field(default=50, ge=1, le=500)
    dry_run: bool = True
    reason: str = Field(default="stale_research_brief_queue_cleanup", max_length=300)
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_safe_maintenance_scope(self) -> "ResearchBriefQueueMaintenanceRequest":
        self.statuses = _dedupe_strings(self.statuses)
        unsafe_statuses = {"queued", "running"}.intersection(self.statuses)
        if unsafe_statuses:
            raise ValueError("research brief queue maintenance cannot target queued or running items")
        unsupported_statuses = set(self.statuses).difference({"failed", "completed"})
        if unsupported_statuses:
            raise ValueError("research brief queue maintenance only supports failed or completed items")
        if not self.statuses and not self.queue_item_ids:
            raise ValueError("research brief queue maintenance requires statuses or queue_item_ids")
        self.reason = self.reason.strip() or "stale_research_brief_queue_cleanup"
        return self


class ResearchBriefQueueMaintenanceResult(StrictBaseModel):
    action: Literal["archive"] = "archive"
    dry_run: bool = True
    candidate_count: int = 0
    archived_count: int = 0
    skipped_count: int = 0
    queue_items: list[ResearchBriefQueueItem] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchBriefQueueBatchRequest(StrictBaseModel):
    mode: ResearchBriefQueueBatchMode = "both"
    lead_statuses: list[ResearchLeadStatus] = Field(default_factory=lambda: ["new", "watching"], max_length=10)
    lead_types: list[ResearchLeadType] = Field(default_factory=list, max_length=10)
    source_keys: list[str] = Field(default_factory=list, max_length=50)
    source_health_statuses: list[SourceHealthStatus] = Field(
        default_factory=lambda: ["failing", "triage", "watch"],
        max_length=4,
    )
    source_health_report: dict[str, Any] | None = None
    include_empty_sources: bool = False
    limit: int = Field(default=25, ge=1, le=100)
    disease_scope: str = Field(default="canine hemangiosarcoma and human angiosarcoma", max_length=500)
    priority: int = Field(default=80, ge=0, le=1000)
    max_chunks_per_perspective: int = Field(default=8, ge=1, le=25)
    max_claims: int = Field(default=12, ge=0, le=50)
    max_chunk_chars: int = Field(default=1800, ge=500, le=12000)
    brief_style: Literal["technical", "operator", "substack", "vet_partner"] = "technical"
    model_profile: str = "research_brief"
    review_mode: Literal[
        "external_required",
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "openrouter_required"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResearchBriefQueueBatchResult(StrictBaseModel):
    mode: ResearchBriefQueueBatchMode = "both"
    queued_count: int = 0
    lead_count: int = 0
    research_followup_count: int = 0
    source_health_count: int = 0
    skipped_count: int = 0
    queue_items: list[ResearchBriefQueueItem] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchHuntSynthesisQueueRequest(StrictBaseModel):
    lead_ids: list[UUID] = Field(default_factory=list, max_length=100)
    lead_statuses: list[ResearchLeadStatus] = Field(
        default_factory=lambda: ["new", "watching", "followup"],
        max_length=10,
    )
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    limit: int = Field(default=10, ge=1, le=100)
    disease_scope: str = Field(default="canine hemangiosarcoma and human angiosarcoma", max_length=500)
    priority: int = Field(default=40, ge=0, le=1000)
    max_chunks_per_perspective: int = Field(default=10, ge=1, le=25)
    max_claims: int = Field(default=20, ge=0, le=50)
    max_chunk_chars: int = Field(default=2200, ge=500, le=12000)
    brief_style: Literal["technical", "operator", "substack", "vet_partner"] = "technical"
    model_profile: str = "research_brief"
    review_mode: Literal[
        "external_required",
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "openrouter_required"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    create_handoff_docs: bool = True
    dry_run: bool = True
    transition_leads: bool = True
    operator: str = "research_hunt_synthesis_queue"
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_research_hunt_synthesis_queue_request(self) -> "ResearchHuntSynthesisQueueRequest":
        self.lead_statuses = _dedupe_strings(self.lead_statuses)
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        self.review_models = _dedupe_strings(self.review_models)
        self.operator = self.operator.strip() or "research_hunt_synthesis_queue"
        return self


class ResearchHuntSynthesisQueueResult(StrictBaseModel):
    dry_run: bool = True
    candidate_count: int = Field(default=0, ge=0)
    queued_count: int = Field(default=0, ge=0)
    preexisting_count: int = Field(default=0, ge=0)
    updated_lead_count: int = Field(default=0, ge=0)
    handoff_document_count: int = Field(default=0, ge=0)
    handoff_artifact_count: int = Field(default=0, ge=0)
    skipped_count: int = Field(default=0, ge=0)
    queue_items: list[ResearchBriefQueueItem] = Field(default_factory=list)
    handoff_documents: list["ResearchHuntSynthesisDocument"] = Field(default_factory=list)
    handoff_artifacts: list["ArtifactHandle"] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchHuntSynthesisDocRequest(StrictBaseModel):
    lead_ids: list[UUID] = Field(default_factory=list, max_length=100)
    lead_statuses: list[ResearchLeadStatus] = Field(
        default_factory=lambda: ["new", "watching", "followup", "queued"],
        max_length=10,
    )
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    limit: int = Field(default=10, ge=1, le=100)
    max_claims: int = Field(default=16, ge=0, le=50)
    max_chunks: int = Field(default=12, ge=0, le=40)
    max_chunk_chars: int = Field(default=900, ge=200, le=5000)
    max_technical_footnotes: int = Field(default=30, ge=0, le=100)
    include_technical_footnotes: bool = True
    dry_run: bool = True
    operator: str = "research_hunt_synthesis_doc"
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_research_hunt_synthesis_doc_request(self) -> "ResearchHuntSynthesisDocRequest":
        self.lead_statuses = _dedupe_strings(self.lead_statuses)
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        self.operator = self.operator.strip() or "research_hunt_synthesis_doc"
        return self


class ResearchHuntSynthesisDocument(StrictBaseModel):
    lead_id: UUID
    title: str
    control_status: ResearchHuntControlStatus
    recommended_action: str
    artifact_id: UUID | None = None
    artifact_uri: str | None = None
    markdown: str
    plain_language_summary: str
    claim_count: int = Field(default=0, ge=0)
    chunk_count: int = Field(default=0, ge=0)
    research_object_count: int = Field(default=0, ge=0)
    evidence_ref_count: int = Field(default=0, ge=0)
    technical_footnote_count: int = Field(default=0, ge=0)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResearchHuntSynthesisDocResult(StrictBaseModel):
    dry_run: bool = True
    candidate_count: int = Field(default=0, ge=0)
    document_count: int = Field(default=0, ge=0)
    artifact_count: int = Field(default=0, ge=0)
    updated_lead_count: int = Field(default=0, ge=0)
    skipped_count: int = Field(default=0, ge=0)
    documents: list[ResearchHuntSynthesisDocument] = Field(default_factory=list)
    artifacts: list[ArtifactHandle] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class CommandCenterRequest(StrictBaseModel):
    source_keys: list[str] = Field(default_factory=list, max_length=50)
    include_source_health: bool = True
    include_recent_agents: bool = True
    source_health_report: dict[str, Any] | None = None
    queue_limit: int = Field(default=25, ge=1, le=200)
    lead_limit: int = Field(default=25, ge=1, le=200)
    agent_run_limit: int = Field(default=25, ge=1, le=200)
    min_health_score: float = Field(default=0.65, ge=0.0, le=1.0)
    require_claims: bool = True


class CommandCenterRecommendation(StrictBaseModel):
    area: CommandCenterArea
    severity: CommandCenterSeverity
    action: str
    reason: str
    job_name: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class CommandCenterResult(StrictBaseModel):
    summary: dict[str, Any] = Field(default_factory=dict)
    research_brief_queue: dict[str, Any] = Field(default_factory=dict)
    research_leads: dict[str, Any] = Field(default_factory=dict)
    source_health: dict[str, Any] | None = None
    recent_agent_runs: list[dict[str, Any]] = Field(default_factory=list)
    recommendations: list[CommandCenterRecommendation] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchBriefPlaygroundPrompt(StrictBaseModel):
    perspective: ResearchBriefPerspectiveName
    agent_name: str
    recommended_model: str | None = None
    system_prompt: str = Field(min_length=1, max_length=12000)
    user_prompt: str = Field(min_length=1, max_length=100000)
    prompt_payload: dict[str, Any] = Field(default_factory=dict)
    response_contract: dict[str, Any] = Field(default_factory=dict)
    evaluation_rubric: list[str] = Field(default_factory=list, max_length=20)
    playground_steps: list[str] = Field(default_factory=list, max_length=20)


class ResearchBriefPlaygroundPack(StrictBaseModel):
    topic: str
    disease_scope: str
    brief_style: Literal["technical", "operator", "substack", "vet_partner"] = "technical"
    model_profile: str = "research_brief"
    prompts: list[ResearchBriefPlaygroundPrompt] = Field(default_factory=list)
    citations: list[ResearchBriefCitation] = Field(default_factory=list)
    evidence: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class TherapyCommitteeRequest(StrictBaseModel):
    topic: str = Field(
        default="curative or disease-modifying therapy ideas for canine hemangiosarcoma",
        min_length=3,
        max_length=1000,
    )
    disease_scope: str = Field(default="canine hemangiosarcoma and human angiosarcoma", max_length=500)
    source_key: str | None = None
    program_id: UUID | None = None
    brief_id: UUID | None = None
    evaluation_id: UUID | None = None
    max_chunks_per_perspective: int = Field(default=10, ge=1, le=30)
    max_claims: int = Field(default=20, ge=0, le=75)
    max_chunk_chars: int = Field(default=2200, ge=500, le=12000)
    max_ideas_per_perspective: int = Field(default=4, ge=1, le=10)
    model_profile: str = "therapy_committee"
    review_mode: Literal[
        "external_required",
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "openrouter_required"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class TherapyIdea(StrictBaseModel):
    idea_id: UUID = Field(default_factory=uuid4)
    title: str = Field(min_length=1, max_length=500)
    hypothesis: str = Field(min_length=1, max_length=2000)
    rationale: str = Field(min_length=1, max_length=3000)
    candidate_therapies: list[str] = Field(default_factory=list, max_length=20)
    targets: list[str] = Field(default_factory=list, max_length=20)
    biomarkers: list[str] = Field(default_factory=list, max_length=20)
    mechanism: str | None = Field(default=None, max_length=1500)
    evidence_refs: list[str] = Field(default_factory=list, max_length=25)
    evidence_strength: ResearchBriefEvidenceStrength = "unknown"
    translational_path: str | None = Field(default=None, max_length=2000)
    risks: list[str] = Field(default_factory=list, max_length=25)
    next_experiments: list[str] = Field(default_factory=list, max_length=25)
    priority_score: float = Field(default=0.5, ge=0.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_therapy_idea(self) -> "TherapyIdea":
        self.title = self.title.strip()
        self.hypothesis = self.hypothesis.strip()
        self.rationale = self.rationale.strip()
        self.candidate_therapies = _dedupe_strings(self.candidate_therapies)
        self.targets = _dedupe_strings(self.targets)
        self.biomarkers = _dedupe_strings(self.biomarkers)
        self.evidence_refs = _dedupe_strings(self.evidence_refs)
        self.risks = _dedupe_strings(self.risks)
        self.next_experiments = _dedupe_strings(self.next_experiments)
        return self


class TherapyCommitteeReport(StrictBaseModel):
    agent_run_id: UUID | None = None
    perspective: TherapyCommitteePerspectiveName
    agent_name: str
    model_profile: str = "therapy_committee"
    summary: str = Field(min_length=1, max_length=3000)
    ideas: list[TherapyIdea] = Field(default_factory=list, max_length=20)
    evidence: dict[str, Any] = Field(default_factory=dict)
    evidence_limitations: list[str] = Field(default_factory=list, max_length=50)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class TherapyCommitteeResult(StrictBaseModel):
    committee_run_id: UUID = Field(default_factory=uuid4)
    agent_run_id: UUID | None = None
    agent_run_ids: list[UUID] = Field(default_factory=list)
    agent_name: str = "therapy_committee_chair_agent"
    topic: str
    disease_scope: str
    source_program_id: UUID | None = None
    source_brief_id: UUID | None = None
    source_evaluation_id: UUID | None = None
    model_profile: str = "therapy_committee"
    review_mode: Literal[
        "external_required",
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "openrouter_required"
    reports: list[TherapyCommitteeReport] = Field(default_factory=list, max_length=10)
    ranked_ideas: list[TherapyIdea] = Field(default_factory=list, max_length=25)
    decision_summary: str = ""
    evidence: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ValidationToolCapability(StrictBaseModel):
    tool_key: str = Field(min_length=2, max_length=120)
    display_name: str = Field(min_length=2, max_length=200)
    category: ValidationToolCategory
    description: str = Field(min_length=3, max_length=2000)
    compatible_validation_types: list[str] = Field(default_factory=list, min_length=1, max_length=25)
    compatible_task_types: list[str] = Field(default_factory=list, min_length=1, max_length=25)
    required_inputs: list[str] = Field(default_factory=list, max_length=25)
    optional_inputs: list[str] = Field(default_factory=list, max_length=25)
    outputs: list[str] = Field(default_factory=list, max_length=25)
    artifacts: list[str] = Field(default_factory=list, max_length=25)
    quality_gates: list[str] = Field(default_factory=list, max_length=25)
    dispatch_blockers: list[str] = Field(default_factory=list, max_length=25)
    estimated_runtime_minutes: int | None = Field(default=None, ge=0, le=10080)
    estimated_cost_usd: float | None = Field(default=None, ge=0.0, le=100000.0)
    compute_profile: ValidationToolComputeProfile = "manual_review"
    runner_status: ValidationToolRunnerStatus = "recommend_only"
    sop_path: str | None = Field(default=None, max_length=500)
    tool_hint: str = Field(min_length=2, max_length=200)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_tool_capability(self) -> "ValidationToolCapability":
        self.tool_key = re.sub(r"[^a-z0-9_:-]+", "_", self.tool_key.strip().lower()).strip("_")
        self.tool_hint = self.tool_hint.strip()
        self.display_name = self.display_name.strip()
        self.description = self.description.strip()
        self.compatible_validation_types = _dedupe_lower_tokens(self.compatible_validation_types)
        self.compatible_task_types = _dedupe_lower_tokens(self.compatible_task_types)
        self.required_inputs = _normalized_unique_strings(self.required_inputs)
        self.optional_inputs = _normalized_unique_strings(self.optional_inputs)
        self.outputs = _normalized_unique_strings(self.outputs)
        self.artifacts = _normalized_unique_strings(self.artifacts)
        self.quality_gates = _normalized_unique_strings(self.quality_gates)
        self.dispatch_blockers = _normalized_unique_strings(self.dispatch_blockers)
        if self.sop_path:
            self.sop_path = self.sop_path.strip() or None
        if not self.tool_key:
            raise ValueError("tool_key cannot be empty after normalization")
        return self


class ValidationToolCatalogRequest(StrictBaseModel):
    category: ValidationToolCategory | None = None
    runner_status: ValidationToolRunnerStatus | None = None
    validation_type: str | None = None
    task_type: str | None = None
    query: str | None = None
    limit: int = Field(default=50, ge=1, le=500)

    @model_validator(mode="after")
    def normalize_catalog_request(self) -> "ValidationToolCatalogRequest":
        if self.validation_type:
            self.validation_type = re.sub(r"\s+", "_", self.validation_type.strip().lower()) or None
        if self.task_type:
            self.task_type = re.sub(r"\s+", "_", self.task_type.strip().lower()) or None
        if self.query:
            self.query = self.query.strip() or None
        return self


class ValidationToolCatalogResult(StrictBaseModel):
    tool_count: int = 0
    runner_status_counts: dict[str, int] = Field(default_factory=dict)
    category_counts: dict[str, int] = Field(default_factory=dict)
    tools: list[ValidationToolCapability] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ValidationToolMatchRequest(StrictBaseModel):
    validation_type: str | None = None
    task_type: str | None = None
    objective: str | None = None
    candidate_name: str | None = None
    target_name: str | None = None
    species: list[str] = Field(default_factory=list, max_length=10)
    required_inputs: list[str] = Field(default_factory=list, max_length=25)
    runner_status: ValidationToolRunnerStatus | None = "recommend_only"
    limit: int = Field(default=5, ge=1, le=50)

    @model_validator(mode="after")
    def normalize_match_request(self) -> "ValidationToolMatchRequest":
        for field_name in ("validation_type", "task_type", "objective", "candidate_name", "target_name"):
            value = getattr(self, field_name)
            if isinstance(value, str):
                setattr(self, field_name, value.strip() or None)
        if self.validation_type:
            self.validation_type = re.sub(r"\s+", "_", self.validation_type.lower())
        if self.task_type:
            self.task_type = re.sub(r"\s+", "_", self.task_type.lower())
        self.species = _dedupe_lower_tokens(self.species)
        self.required_inputs = _normalized_unique_strings(self.required_inputs)
        return self


class ValidationToolMatch(StrictBaseModel):
    tool: ValidationToolCapability
    score: float = Field(default=0.0, ge=0.0, le=1.0)
    reasons: list[str] = Field(default_factory=list, max_length=25)
    missing_inputs: list[str] = Field(default_factory=list, max_length=25)
    dispatch_blockers: list[str] = Field(default_factory=list, max_length=25)


class ValidationToolMatchResult(StrictBaseModel):
    match_count: int = 0
    matches: list[ValidationToolMatch] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class TherapyIdeaRecord(StrictBaseModel):
    therapy_idea_id: UUID = Field(default_factory=uuid4)
    idea: TherapyIdea
    committee_run_id: UUID | None = None
    agent_run_id: UUID | None = None
    source_program_id: UUID | None = None
    source_brief_id: UUID | None = None
    source_evaluation_id: UUID | None = None
    topic: str = Field(default="", max_length=1000)
    disease_scope: str = Field(default="canine hemangiosarcoma and human angiosarcoma", max_length=500)
    source_key: str | None = None
    status: TherapyIdeaStatus = "proposed"
    promotion_state: HypothesisPromotionState | None = None
    score: float = Field(default=0.5, ge=0.0, le=1.0)
    evidence_refs: list[str] = Field(default_factory=list, max_length=50)
    targets: list[str] = Field(default_factory=list, max_length=50)
    biomarkers: list[str] = Field(default_factory=list, max_length=50)
    candidate_therapies: list[str] = Field(default_factory=list, max_length=50)
    risks: list[str] = Field(default_factory=list, max_length=50)
    next_experiments: list[str] = Field(default_factory=list, max_length=50)
    promotion_metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_therapy_idea_record(self) -> "TherapyIdeaRecord":
        self.therapy_idea_id = self.idea.idea_id
        self.topic = self.topic.strip()
        self.disease_scope = self.disease_scope.strip()
        self.score = self.idea.priority_score if "score" not in self.model_fields_set else self.score
        self.evidence_refs = _dedupe_strings(self.evidence_refs or self.idea.evidence_refs)
        self.targets = _dedupe_strings(self.targets or self.idea.targets)
        self.biomarkers = _dedupe_strings(self.biomarkers or self.idea.biomarkers)
        self.candidate_therapies = _dedupe_strings(self.candidate_therapies or self.idea.candidate_therapies)
        self.risks = _dedupe_strings(self.risks or self.idea.risks)
        self.next_experiments = _dedupe_strings(self.next_experiments or self.idea.next_experiments)
        return self


class TherapyIdeaLibraryRequest(StrictBaseModel):
    therapy_idea_id: UUID | None = None
    status: TherapyIdeaStatus | None = None
    statuses: list[TherapyIdeaStatus] = Field(default_factory=list, max_length=10)
    source_program_id: UUID | None = None
    source_brief_id: UUID | None = None
    source_evaluation_id: UUID | None = None
    committee_run_id: UUID | None = None
    topic_query: str | None = None
    limit: int = Field(default=50, ge=1, le=500)

    @model_validator(mode="after")
    def normalize_therapy_idea_library_request(self) -> "TherapyIdeaLibraryRequest":
        if self.topic_query:
            self.topic_query = self.topic_query.strip() or None
        self.statuses = _dedupe_strings(self.statuses)
        return self


class TherapyIdeaLibraryResult(StrictBaseModel):
    idea_count: int = 0
    status_counts: dict[str, int] = Field(default_factory=dict)
    ideas: list[TherapyIdeaRecord] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class TherapyCommitteeValidationQueueRequest(StrictBaseModel):
    agent_run_id: UUID | None = None
    idea_ids: list[UUID] = Field(default_factory=list, max_length=10)
    max_ideas: int = Field(default=1, ge=1, le=10)
    priority: int = Field(default=40, ge=1, le=1000)
    dry_run: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class HypothesisPromotionCandidate(StrictBaseModel):
    candidate_id: str = Field(min_length=3, max_length=240)
    source_type: HypothesisPromotionSourceType
    source_id: str = Field(min_length=1, max_length=200)
    brief_id: UUID | None = None
    evaluation_id: UUID | None = None
    therapy_idea_id: UUID | None = None
    committee_run_id: UUID | None = None
    title: str = Field(min_length=1, max_length=500)
    hypothesis: str = Field(min_length=1, max_length=3000)
    promotion_state: HypothesisPromotionState
    score: float = Field(default=0.0, ge=0.0, le=1.0)
    candidate_therapies: list[str] = Field(default_factory=list, max_length=50)
    targets: list[str] = Field(default_factory=list, max_length=50)
    biomarkers: list[str] = Field(default_factory=list, max_length=50)
    evidence_refs: list[str] = Field(default_factory=list, max_length=50)
    risks: list[str] = Field(default_factory=list, max_length=50)
    next_experiments: list[str] = Field(default_factory=list, max_length=50)
    blockers: list[str] = Field(default_factory=list, max_length=25)
    recommended_next_action: str = Field(default="", max_length=1000)
    recommended_job_name: str | None = Field(default=None, max_length=200)
    matched_tools: list[ValidationToolMatch] = Field(default_factory=list, max_length=10)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_promotion_candidate(self) -> "HypothesisPromotionCandidate":
        self.candidate_id = self.candidate_id.strip()
        self.source_id = self.source_id.strip()
        self.title = self.title.strip()
        self.hypothesis = self.hypothesis.strip()
        self.candidate_therapies = _dedupe_strings(self.candidate_therapies)
        self.targets = _dedupe_strings(self.targets)
        self.biomarkers = _dedupe_strings(self.biomarkers)
        self.evidence_refs = _dedupe_strings(self.evidence_refs)
        self.risks = _dedupe_strings(self.risks)
        self.next_experiments = _dedupe_strings(self.next_experiments)
        self.blockers = _dedupe_strings(self.blockers)
        self.recommended_next_action = self.recommended_next_action.strip()
        return self


class HypothesisPromotionReportRequest(StrictBaseModel):
    brief_id: UUID | None = None
    evaluation_id: UUID | None = None
    therapy_idea_id: UUID | None = None
    topic_query: str | None = None
    source_key: str | None = None
    include_blocked: bool = True
    include_ready_for_committee: bool = True
    include_ready_for_validation: bool = True
    limit: int = Field(default=50, ge=1, le=500)

    @model_validator(mode="after")
    def normalize_hypothesis_promotion_request(self) -> "HypothesisPromotionReportRequest":
        if self.topic_query:
            self.topic_query = self.topic_query.strip() or None
        if self.source_key:
            self.source_key = self.source_key.strip().lower() or None
        return self


class HypothesisPromotionReportResult(StrictBaseModel):
    candidate_count: int = 0
    state_counts: dict[str, int] = Field(default_factory=dict)
    candidates: list[HypothesisPromotionCandidate] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchBriefEvaluationRequest(StrictBaseModel):
    brief_id: UUID | None = None
    topic_query: str | None = None
    source_key: str | None = None
    limit: int = Field(default=1, ge=1, le=50)
    minimum_overall_score: float = Field(default=0.7, ge=0.0, le=1.0)
    model_profile: str = "synthesis_quality_evaluator"
    review_mode: Literal[
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "deterministic_only"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResearchBriefEvaluationResult(StrictBaseModel):
    evaluation_id: UUID = Field(default_factory=uuid4)
    brief_id: UUID
    agent_run_id: UUID | None = None
    agent_name: str = "research_brief_synthesis_evaluator_agent"
    model_profile: str = "synthesis_quality_evaluator"
    topic: str
    source_key: str | None = None
    overall_score: float = Field(ge=0.0, le=1.0)
    citation_coverage_score: float = Field(ge=0.0, le=1.0)
    perspective_balance_score: float = Field(ge=0.0, le=1.0)
    contradiction_handling_score: float = Field(ge=0.0, le=1.0)
    novelty_score: float = Field(ge=0.0, le=1.0)
    actionability_score: float = Field(ge=0.0, le=1.0)
    weakness_transparency_score: float = Field(ge=0.0, le=1.0)
    passes_quality_bar: bool = False
    readiness: ResearchBriefEvaluationReadiness = "needs_more_evidence"
    strengths: list[str] = Field(default_factory=list, max_length=20)
    weaknesses: list[str] = Field(default_factory=list, max_length=20)
    recommendations: list[str] = Field(default_factory=list, max_length=20)
    evidence: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchBriefEvaluationRecord(StrictBaseModel):
    evaluation_id: UUID = Field(default_factory=uuid4)
    brief_id: UUID
    agent_run_id: UUID | None = None
    topic: str
    source_key: str | None = None
    model_profile: str = "synthesis_quality_evaluator"
    overall_score: float = Field(ge=0.0, le=1.0)
    passes_quality_bar: bool = False
    readiness: ResearchBriefEvaluationReadiness = "needs_more_evidence"
    summary: dict[str, Any] = Field(default_factory=dict)
    result_payload: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)


ResearchBriefQualityStatus = Literal[
    "ready_for_validation",
    "ready_for_hypothesis_review",
    "needs_more_evidence",
    "needs_human_review",
    "blocked",
    "needs_evaluation",
    "needs_followup_research",
    "brief_failed",
]


class ResearchBriefQualityReportRequest(StrictBaseModel):
    status: ResearchBriefStatus | None = None
    source_key: str | None = None
    topic_query: str | None = None
    limit: int = Field(default=50, ge=1, le=500)
    include_evaluations: bool = True


class ResearchBriefQualityRow(StrictBaseModel):
    brief_id: UUID
    evaluation_id: UUID | None = None
    agent_run_id: UUID | None = None
    status: ResearchBriefStatus
    quality_status: ResearchBriefQualityStatus
    topic: str
    source_key: str | None = None
    brief_style: Literal["technical", "operator", "substack", "vet_partner"] = "technical"
    model_profile: str = "research_brief"
    review_mode: Literal[
        "external_required",
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "openrouter_required"
    review_models: list[str] = Field(default_factory=list)
    citation_count: int = Field(default=0, ge=0)
    finding_count: int = Field(default=0, ge=0)
    hypothesis_count: int = Field(default=0, ge=0)
    hard_error_count: int = Field(default=0, ge=0)
    evidence_limitation_count: int = Field(default=0, ge=0)
    error_count: int = Field(default=0, ge=0)
    passes_completion_bar: bool = False
    passes_quality_bar: bool | None = None
    readiness: ResearchBriefEvaluationReadiness | None = None
    overall_score: float | None = Field(default=None, ge=0.0, le=1.0)
    created_at: datetime
    updated_at: datetime


class ResearchBriefQualityReportResult(StrictBaseModel):
    brief_count: int = 0
    evaluated_count: int = 0
    ready_count: int = 0
    failed_count: int = 0
    followup_count: int = 0
    needs_evaluation_count: int = 0
    average_overall_score: float | None = Field(default=None, ge=0.0, le=1.0)
    status_counts: dict[str, int] = Field(default_factory=dict)
    quality_status_counts: dict[str, int] = Field(default_factory=dict)
    rows: list[ResearchBriefQualityRow] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchBriefFollowupQueueRequest(StrictBaseModel):
    brief_ids: list[UUID] = Field(default_factory=list, max_length=100)
    evaluation_ids: list[UUID] = Field(default_factory=list, max_length=100)
    status: ResearchBriefStatus | None = None
    source_key: str | None = None
    topic_query: str | None = None
    limit: int = Field(default=50, ge=1, le=500)
    include_evaluations: bool = True
    max_limitations_per_brief: int = Field(default=20, ge=1, le=50)
    force: bool = False
    dry_run: bool = False

    @model_validator(mode="after")
    def normalize_research_brief_followup_queue_request(self) -> "ResearchBriefFollowupQueueRequest":
        self.brief_ids = list(dict.fromkeys(self.brief_ids))
        self.evaluation_ids = list(dict.fromkeys(self.evaluation_ids))
        return self


class ResearchBriefFollowupQueueResult(StrictBaseModel):
    candidate_brief_count: int = 0
    limitation_count: int = 0
    queued_count: int = 0
    existing_count: int = 0
    skipped_count: int = 0
    dry_run: bool = False
    followup_leads: list[ResearchLeadRecord] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class XTopicLinkedSource(StrictBaseModel):
    url: str
    recommended_source_key: str | None = None
    identifier_type: XTopicIdentifierType = "unknown"
    identifier: str | None = None
    should_ingest: bool = False
    reason: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class XTopicReviewAction(StrictBaseModel):
    source_record_id: str
    query_name: str | None = None
    username: str | None = None
    action: XTopicReviewActionName
    severity: AgentSeverity
    reason: str
    ingestible_links: list[XTopicLinkedSource] = Field(default_factory=list)
    evidence_refs: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class XTopicReviewRequest(StrictBaseModel):
    provider_report: dict[str, Any] | None = None
    candidates: list[dict[str, Any]] = Field(default_factory=list)
    recent_run_limit: int = Field(default=5, ge=0, le=50)
    max_candidates: int = Field(default=20, ge=1, le=100)
    model_profile: str = "reviewer"
    review_mode: Literal[
        "external_required",
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "openrouter_required"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class XTopicReviewResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    agent_name: str = "x_topic_review_agent"
    model_profile: str = "reviewer"
    actions: list[XTopicReviewAction] = Field(default_factory=list)
    ingestion_candidate_count: int = 0
    needs_human_review_count: int = 0
    rejected_count: int = 0
    evidence: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class XLinkedArticleFollowupRequest(StrictBaseModel):
    urls: list[str] = Field(default_factory=list)
    recent_run_limit: int = Field(default=10, ge=0, le=100)
    max_urls: int = Field(default=10, ge=1, le=100)
    fetch: bool = True
    parse: bool = True
    approved_by: str | None = None
    approval_note: str | None = None
    robots_policy: Literal["unknown", "reviewed", "disallow", "manual_only"] = "reviewed"
    metadata: dict[str, Any] = Field(default_factory=dict)


class XLinkedArticleFollowupResult(StrictBaseModel):
    source_key: str = "x_linked_article"
    candidate_urls: list[str] = Field(default_factory=list)
    candidate_results: list[dict[str, Any]] = Field(default_factory=list)
    agent_run_ids: list[UUID] = Field(default_factory=list)
    fetched_pages: int = 0
    skipped_pages: int = 0
    artifact_ids: list[UUID] = Field(default_factory=list)
    parsed_records: int = 0
    review_ids: list[UUID] = Field(default_factory=list)
    primary_source_links: list[dict[str, Any]] = Field(default_factory=list)
    requires_fetch_approval: bool = False
    errors: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class XLinkedArticleReviewAction(StrictBaseModel):
    review_id: UUID
    source_record_id: str
    action: XLinkedArticleReviewActionName
    severity: AgentSeverity
    reason: str
    followup_links: list[XTopicLinkedSource] = Field(default_factory=list)
    evidence_refs: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class XLinkedArticleReviewRequest(StrictBaseModel):
    review_ids: list[UUID] = Field(default_factory=list)
    review_status: Literal["needs_review", "accepted", "rejected"] | None = "needs_review"
    limit: int = Field(default=50, ge=1, le=500)
    model_profile: str = "reviewer"
    review_mode: Literal[
        "external_required",
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "openrouter_required"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class XLinkedArticleReviewResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    agent_name: str = "x_linked_article_review_agent"
    model_profile: str = "reviewer"
    actions: list[XLinkedArticleReviewAction] = Field(default_factory=list)
    queue_candidate_count: int = 0
    needs_human_review_count: int = 0
    rejected_count: int = 0
    evidence: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class SourceFollowupQueueItem(StrictBaseModel):
    followup_id: UUID = Field(default_factory=uuid4)
    source_key: str
    identifier_type: XTopicIdentifierType
    identifier: str
    identity_key: str | None = None
    url: str | None = None
    title: str | None = None
    origin_source_key: str | None = None
    origin_review_id: UUID | None = None
    origin_artifact_id: UUID | None = None
    origin_agent_run_id: UUID | None = None
    reason: str | None = None
    status: SourceFollowupStatus = "queued"
    priority: int = Field(default=100, ge=0, le=1000)
    attempts: int = Field(default=0, ge=0)
    last_error: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_identifier(self) -> "SourceFollowupQueueItem":
        identifier = self.identifier.strip()
        if self.identifier_type == "doi":
            identifier = identifier.split("?", 1)[0].split("#", 1)[0].rstrip(").,;:&")
            for suffix in ("/full", "/abstract", "/pdf", "/epdf", "/html"):
                if identifier.lower().endswith(suffix):
                    identifier = identifier[: -len(suffix)]
                    break
            identifier = identifier.lower()
        elif self.identifier_type in {"pmcid", "nct", "chembl", "geo", "sra", "rcsb_pdb"}:
            identifier = identifier.upper()
        self.identifier = identifier
        self.identity_key = f"{self.source_key}:{self.identifier_type}:{identifier.lower()}"
        return self


class SourceFollowupQueueRequest(StrictBaseModel):
    source_key: str = "x_linked_article"
    review_ids: list[UUID] = Field(default_factory=list)
    review_status: Literal["needs_review", "accepted", "rejected"] | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    include_existing: bool = False
    include_agent_recommendations: bool = True
    agent_run_limit: int = Field(default=20, ge=0, le=100)


class DoiOpenAccessFollowupQueueRequest(StrictBaseModel):
    source_keys: list[str] = Field(default_factory=list)
    limit: int = Field(default=100, ge=1, le=1000)
    include_existing: bool = False


class SourceFollowupQueueResult(StrictBaseModel):
    source_key: str = "x_linked_article"
    reviewed_records: int = 0
    agent_runs_seen: int = 0
    agent_recommendations_seen: int = 0
    queued: int = 0
    skipped_existing: int = 0
    skipped_uningestible: int = 0
    items: list[SourceFollowupQueueItem] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class SourceFollowupIngestItemResult(StrictBaseModel):
    followup_id: UUID
    source_key: str
    identifier_type: XTopicIdentifierType
    identifier: str
    status: SourceFollowupStatus
    raw_records: int = 0
    research_objects: int = 0
    document_chunks: int = 0
    error: str | None = None
    fetch_run_id: UUID | None = None


class SourceFollowupIngestRequest(StrictBaseModel):
    followup_ids: list[UUID] = Field(default_factory=list, max_length=500)
    source_keys: list[str] = Field(default_factory=list)
    statuses: list[SourceFollowupStatus] = Field(default_factory=lambda: ["queued", "approved"])
    limit: int = Field(default=25, ge=1, le=500)
    approved_by: str | None = None
    run_claim_extraction: bool = True
    dry_run: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourceFollowupIngestResult(StrictBaseModel):
    queue_items_seen: int = 0
    attempted: int = 0
    ingested: int = 0
    failed: int = 0
    skipped: int = 0
    raw_records: int = 0
    research_objects: int = 0
    document_chunks: int = 0
    source_reports: list[dict[str, Any]] = Field(default_factory=list)
    items: list[SourceFollowupIngestItemResult] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class ResearchSource(StrictBaseModel):
    source_key: str = Field(description="Stable source key, e.g. openalex")
    display_name: str
    source_kind: SourceKind
    base_url: HttpUrl | None = None
    documentation_url: HttpUrl | None = None
    license_policy: str = "metadata_only"
    requires_api_key: bool = False
    enabled: bool = True
    priority: int = 100
    phase: int = 1
    rate_limit_per_minute: int | None = None
    capabilities: list[str] = Field(default_factory=list)
    notes: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourceQuery(StrictBaseModel):
    source_key: str
    query_name: str
    query_text: str
    query_params: dict[str, Any] = Field(default_factory=dict)
    track: str | None = None
    object_type: ResearchObjectType | None = None
    active: bool = True


class AgentFindingEscalationRequest(StrictBaseModel):
    review_ids: list[UUID] = Field(default_factory=list, max_length=100)
    agent_run_ids: list[UUID] = Field(default_factory=list, max_length=100)
    verdicts: list[AgentRunReviewVerdict] = Field(default_factory=lambda: ["bad", "needs_followup"], max_length=4)
    limit: int = Field(default=25, ge=1, le=200)
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    create_research_leads: bool = True
    create_source_queries: bool = True
    dry_run: bool = False
    operator: str = "command_center_operator"
    metadata: dict[str, Any] = Field(default_factory=dict)


class AgentFindingEscalationResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    agent_name: str = "agent_finding_escalation_agent"
    agent_version: str = "v1"
    dry_run: bool = False
    scanned_count: int = Field(default=0, ge=0)
    escalated_count: int = Field(default=0, ge=0)
    research_leads_created: int = Field(default=0, ge=0)
    source_queries_created: int = Field(default=0, ge=0)
    research_leads: list[ResearchLeadRecord] = Field(default_factory=list)
    source_queries: list[SourceQuery] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchFollowupRefinementRequest(StrictBaseModel):
    lead_ids: list[UUID] = Field(default_factory=list, max_length=100)
    review_ids: list[UUID] = Field(default_factory=list, max_length=100)
    verdicts: list[AgentRunReviewVerdict] = Field(default_factory=lambda: ["bad", "needs_followup"], max_length=4)
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    limit: int = Field(default=25, ge=1, le=200)
    max_queries_per_review: int = Field(default=4, ge=1, le=20)
    dry_run: bool = False
    operator: str = "command_center_operator"
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_research_followup_refinement_request(self) -> "ResearchFollowupRefinementRequest":
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        self.operator = self.operator.strip() or "command_center_operator"
        return self


class ResearchFollowupRefinementResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    agent_name: str = "research_followup_refinement_agent"
    agent_version: str = "v1"
    dry_run: bool = False
    scanned_count: int = Field(default=0, ge=0)
    lead_count: int = Field(default=0, ge=0)
    query_count: int = Field(default=0, ge=0)
    source_queries_created: int = Field(default=0, ge=0)
    source_queries_deactivated: int = Field(default=0, ge=0)
    source_queries: list[SourceQuery] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ValidationGapSourceQuery(StrictBaseModel):
    query_id: UUID = Field(default_factory=uuid4)
    lane: ValidationGapEvidenceLane
    source_key: str
    query_name: str
    query_text: str
    query_params: dict[str, Any] = Field(default_factory=dict)
    track: str = "validation_gap"
    object_type: ResearchObjectType | None = None
    active: bool = True
    priority: int = Field(default=100, ge=0, le=1000)
    reason: str
    required_terms: list[str] = Field(default_factory=list, max_length=25)
    excluded_terms: list[str] = Field(default_factory=list, max_length=25)
    lead_ids: list[UUID] = Field(default_factory=list, max_length=100)
    queue_item_ids: list[UUID] = Field(default_factory=list, max_length=100)
    evidence_refs: list[str] = Field(default_factory=list, max_length=50)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_validation_gap_source_query(self) -> "ValidationGapSourceQuery":
        self.source_key = self.source_key.strip().lower()
        self.query_name = self.query_name.strip()
        self.query_text = re.sub(r"\s+", " ", self.query_text).strip()
        self.required_terms = _normalized_unique_strings(self.required_terms)
        self.excluded_terms = _normalized_unique_strings(self.excluded_terms)
        self.evidence_refs = _normalized_unique_strings(self.evidence_refs)
        self.track = self.track.strip() or "validation_gap"
        self.reason = self.reason.strip()
        if not self.query_name:
            raise ValueError("query_name is required")
        if not self.query_text:
            raise ValueError("query_text is required")
        if not self.reason:
            raise ValueError("reason is required")
        return self

    def as_source_query(self) -> SourceQuery:
        return SourceQuery(
            source_key=self.source_key,
            query_name=self.query_name,
            query_text=self.query_text,
            query_params=self.query_params,
            track=self.track,
            object_type=self.object_type,
            active=self.active,
        )


class ValidationGapSourcePackRequest(StrictBaseModel):
    queue_item_ids: list[UUID] = Field(default_factory=list, max_length=100)
    lead_ids: list[UUID] = Field(default_factory=list, max_length=100)
    lead_statuses: list[ResearchLeadStatus] = Field(default_factory=lambda: ["new", "followup"], max_length=10)
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    lanes: list[ValidationGapEvidenceLane] = Field(default_factory=list, max_length=20)
    limit: int = Field(default=25, ge=1, le=200)
    max_queries_per_lane: int = Field(default=3, ge=1, le=20)
    persist_queries: bool = False
    active: bool = True
    dry_run: bool = True
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_validation_gap_source_pack_request(self) -> "ValidationGapSourcePackRequest":
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        self.lead_statuses = _dedupe_strings(self.lead_statuses)
        self.lanes = _dedupe_strings(self.lanes)
        return self


class ValidationGapSourcePackResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    agent_name: str = "validation_gap_source_pack_agent"
    model_profile: str = "deterministic_query_builder"
    source_pack_id: UUID = Field(default_factory=uuid4)
    lead_count: int = 0
    queue_item_count: int = 0
    query_count: int = 0
    persisted_query_count: int = 0
    skipped_count: int = 0
    dry_run: bool = True
    persist_queries: bool = False
    queries: list[ValidationGapSourceQuery] = Field(default_factory=list)
    source_queries: list[SourceQuery] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ValidationGapSourceIngestRequest(StrictBaseModel):
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    query_names: list[str] = Field(default_factory=list, max_length=100)
    followup_lane: str | None = None
    origin_review_ids: list[UUID] = Field(default_factory=list, max_length=100)
    origin_agent_run_ids: list[UUID] = Field(default_factory=list, max_length=100)
    limit_per_query: int = Field(default=5, ge=1, le=100)
    max_queries: int = Field(default=50, ge=1, le=500)
    dry_run: bool = True
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_validation_gap_source_ingest_request(self) -> "ValidationGapSourceIngestRequest":
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        self.query_names = _normalized_unique_strings(self.query_names)
        self.followup_lane = self.followup_lane.strip() if self.followup_lane and self.followup_lane.strip() else None
        return self


class ValidationGapSourceIngestResult(StrictBaseModel):
    dry_run: bool = True
    source_keys: list[str] = Field(default_factory=list)
    query_count: int = 0
    attempted_query_count: int = 0
    completed_query_count: int = 0
    failed_query_count: int = 0
    skipped_count: int = 0
    raw_records: int = 0
    research_objects: int = 0
    document_chunks: int = 0
    full_text_research_objects: int = 0
    source_queries: list[SourceQuery] = Field(default_factory=list)
    results: list[IngestionResult] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class EvidenceFitAssessment(StrictBaseModel):
    fit: EvidenceFitLevel = "weak"
    target_safety_fit: EvidenceFitLevel = "weak"
    disease_directness_fit: EvidenceFitLevel = "weak"
    actionability: EvidenceFitLevel = "weak"
    transfer_risk: EvidenceTransferRiskLevel = "unknown"
    overall_fit: EvidenceFitLevel = "weak"
    matched_terms: list[str] = Field(default_factory=list, max_length=50)
    missing_terms: list[str] = Field(default_factory=list, max_length=50)
    required_terms: list[str] = Field(default_factory=list, max_length=50)
    matched_required_count: int = Field(default=0, ge=0)
    total_required_count: int = Field(default=0, ge=0)
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    chunk_count: int = Field(default=0, ge=0)
    reason: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def normalize_evidence_fit_assessment(self) -> "EvidenceFitAssessment":
        self.matched_terms = _normalized_unique_strings(self.matched_terms)
        self.missing_terms = _normalized_unique_strings(self.missing_terms)
        self.required_terms = _normalized_unique_strings(self.required_terms)
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        if self.matched_required_count > self.total_required_count:
            raise ValueError("matched_required_count cannot exceed total_required_count")
        return self


class ResearchFollowupLoopRequest(StrictBaseModel):
    lead_id: UUID
    followup_lane: str = "agent_evaluator_followup"
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    query_names: list[str] = Field(default_factory=list, max_length=100)
    search_query_text: str | None = Field(default=None, max_length=500)
    limit_per_query: int = Field(default=2, ge=1, le=100)
    max_queries: int = Field(default=10, ge=1, le=100)
    ingest: bool = True
    resolve: bool = False
    evaluate: bool = False
    queue_identifier_followups: bool = True
    ingest_identifier_followups: bool = True
    run_claim_extraction: bool = True
    max_identifier_followups: int = Field(default=8, ge=1, le=50)
    dry_run: bool = True
    force_live_search: bool = True
    search_limit_per_source: int = Field(default=2, ge=1, le=25)
    min_evidence_chunks: int = Field(default=1, ge=1, le=25)
    model_profile: str = "agent_performance_evaluator"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    estimated_evaluator_cost_usd: float = Field(default=0.03, ge=0.0, le=10.0)
    operator: str = "command_center_operator"
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_research_followup_loop_request(self) -> "ResearchFollowupLoopRequest":
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        self.query_names = _normalized_unique_strings(self.query_names)
        self.followup_lane = self.followup_lane.strip() or "agent_evaluator_followup"
        if self.search_query_text is not None:
            self.search_query_text = re.sub(r"\s+", " ", self.search_query_text).strip() or None
        self.review_models = _dedupe_strings(self.review_models)
        self.operator = self.operator.strip() or "command_center_operator"
        return self


class ResearchFollowupLoopResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    agent_name: str = "research_followup_loop_agent"
    agent_version: str = "v1"
    lead_id: UUID
    lead_status_before: ResearchLeadStatus | None = None
    lead_status_after: ResearchLeadStatus | None = None
    dry_run: bool = True
    followup_lane: str = "agent_evaluator_followup"
    ingest_result: ValidationGapSourceIngestResult | None = None
    source_followup_result: SourceFollowupIngestResult | None = None
    resolver_result: ResearchFollowupResolverResult | None = None
    evaluation_result: AgentPerformanceEvaluationResult | None = None
    resolver_agent_run_id: UUID | None = None
    evaluator_agent_run_id: UUID | None = None
    query_count: int = Field(default=0, ge=0)
    raw_records: int = Field(default=0, ge=0)
    research_objects: int = Field(default=0, ge=0)
    document_chunks: int = Field(default=0, ge=0)
    source_followups_linked: int = Field(default=0, ge=0)
    source_followups_queued: int = Field(default=0, ge=0)
    source_followups_newly_queued: int = Field(default=0, ge=0)
    source_followups_preexisting: int = Field(default=0, ge=0)
    source_followups_already_ingested: int = Field(default=0, ge=0)
    source_followups_pending: int = Field(default=0, ge=0)
    source_followups_ingested: int = Field(default=0, ge=0)
    source_followups_ingested_this_run: int = Field(default=0, ge=0)
    source_followup_document_chunks: int = Field(default=0, ge=0)
    claim_chunks_seen: int = Field(default=0, ge=0)
    claims_extracted: int = Field(default=0, ge=0)
    claims_written: int = Field(default=0, ge=0)
    evidence_fit: EvidenceFitAssessment | None = None
    latest_evaluator_verdict: AgentRunReviewVerdict | None = None
    signal_status: str | None = None
    coverage_status: str | None = None
    best_signal: dict[str, Any] | None = None
    hunt_tasks_created: int = Field(default=0, ge=0)
    hunt_tasks_suppressed: int = Field(default=0, ge=0)
    hunt_tasks: list[dict[str, Any]] = Field(default_factory=list)
    suppressed_hunt_tasks: list[dict[str, Any]] = Field(default_factory=list)
    estimated_cost_usd: float = Field(default=0.0, ge=0.0)
    actual_cost_usd: float = Field(default=0.0, ge=0.0)
    status_transitions: list[dict[str, Any]] = Field(default_factory=list)
    claim_extraction_errors: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchHuntTaskRunRequest(StrictBaseModel):
    lead_ids: list[UUID] = Field(default_factory=list, max_length=100)
    task_ids: list[UUID] = Field(default_factory=list, max_length=100)
    task_types: list[str] = Field(default_factory=list, max_length=25)
    statuses: list[str] = Field(default_factory=lambda: ["open"], max_length=10)
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    limit: int = Field(default=5, ge=1, le=50)
    claim_chunk_limit: int = Field(default=25, ge=1, le=100)
    dry_run: bool = True
    evaluate: bool = True
    force_live_search: bool = True
    include_broad_tasks: bool = False
    allow_broad_task_fanout: bool = False
    search_limit_per_source: int = Field(default=1, ge=1, le=25)
    model_profile: str = "agent_performance_evaluator"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    operator: str = "research_hunt_executor"
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_research_hunt_task_run_request(self) -> "ResearchHuntTaskRunRequest":
        self.task_types = _dedupe_lower_tokens(self.task_types)
        self.statuses = _dedupe_strings(self.statuses)
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        self.review_models = _dedupe_strings(self.review_models)
        self.model_profile = self.model_profile.strip() or "agent_performance_evaluator"
        self.operator = self.operator.strip() or "research_hunt_executor"
        return self


class ResearchHuntTaskRunResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    agent_name: str = "research_hunt_task_executor_agent"
    agent_version: str = "v1"
    dry_run: bool = True
    scanned_count: int = Field(default=0, ge=0)
    selected_count: int = Field(default=0, ge=0)
    completed_count: int = Field(default=0, ge=0)
    failed_count: int = Field(default=0, ge=0)
    skipped_count: int = Field(default=0, ge=0)
    claim_chunks_seen: int = Field(default=0, ge=0)
    claims_written: int = Field(default=0, ge=0)
    loop_runs: int = Field(default=0, ge=0)
    items: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


ResearchHuntTaskClass = Literal["concrete", "broad", "passive", "unknown"]
ResearchHuntControlStatus = Literal[
    "ready_for_synthesis",
    "hunting",
    "watching",
    "blocked",
    "idle",
    "no_hunt_state",
]


class ResearchHuntQueueReportRequest(StrictBaseModel):
    lead_ids: list[UUID] = Field(default_factory=list, max_length=100)
    lead_statuses: list[ResearchLeadStatus] = Field(
        default_factory=lambda: ["new", "watching", "followup", "queued"],
        max_length=10,
    )
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    limit: int = Field(default=100, ge=1, le=1000)
    task_limit: int = Field(default=250, ge=1, le=2000)
    stale_after_hours: int = Field(default=72, ge=1, le=24 * 90)
    include_tasks: bool = True
    include_suppressed: bool = True

    @model_validator(mode="after")
    def normalize_research_hunt_queue_report_request(self) -> "ResearchHuntQueueReportRequest":
        self.lead_statuses = _dedupe_strings(self.lead_statuses)
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        return self


class ResearchHuntTaskQueueRow(StrictBaseModel):
    lead_id: UUID
    task_id: str | None = None
    task_type: str
    task_class: ResearchHuntTaskClass
    status: str
    priority: int | None = None
    action: str
    reason: str | None = None
    identity_key: str | None = None
    family_key: str | None = None
    suppression_reason: str | None = None
    age_hours: float | None = None
    stale: bool = False
    runnable_by_default: bool = False
    recommended_action: str


class ResearchHuntLeadQueueRow(StrictBaseModel):
    lead_id: UUID
    title: str | None = None
    status: ResearchLeadStatus
    source_key: str | None = None
    priority: int
    signal_status: str | None = None
    coverage_status: str | None = None
    control_status: ResearchHuntControlStatus
    open_task_count: int = Field(default=0, ge=0)
    open_concrete_count: int = Field(default=0, ge=0)
    open_broad_count: int = Field(default=0, ge=0)
    open_passive_count: int = Field(default=0, ge=0)
    blocked_task_count: int = Field(default=0, ge=0)
    stale_task_count: int = Field(default=0, ge=0)
    suppressed_task_count: int = Field(default=0, ge=0)
    completed_task_count: int = Field(default=0, ge=0)
    failed_task_count: int = Field(default=0, ge=0)
    best_signal_score: int = Field(default=0, ge=0, le=100)
    recommended_action: str


class ResearchHuntQueueReportResult(StrictBaseModel):
    scanned_lead_count: int = Field(default=0, ge=0)
    lead_count: int = Field(default=0, ge=0)
    executable_task_count: int = Field(default=0, ge=0)
    broad_task_count: int = Field(default=0, ge=0)
    passive_task_count: int = Field(default=0, ge=0)
    stale_task_count: int = Field(default=0, ge=0)
    suppressed_task_count: int = Field(default=0, ge=0)
    blocked_lead_count: int = Field(default=0, ge=0)
    ready_for_synthesis_count: int = Field(default=0, ge=0)
    watching_count: int = Field(default=0, ge=0)
    hunting_count: int = Field(default=0, ge=0)
    status_counts: dict[str, int] = Field(default_factory=dict)
    task_class_counts: dict[str, int] = Field(default_factory=dict)
    control_status_counts: dict[str, int] = Field(default_factory=dict)
    leads: list[ResearchHuntLeadQueueRow] = Field(default_factory=list)
    tasks: list[ResearchHuntTaskQueueRow] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


ResearchHuntQueueMaintenanceAction = Literal["suppress"]
ResearchHuntQueueMaintenanceReason = Literal[
    "stale_broad_or_passive",
    "duplicate_broad_family",
    "passive_monitoring_note",
]


class ResearchHuntQueueMaintenanceRequest(StrictBaseModel):
    action: ResearchHuntQueueMaintenanceAction = "suppress"
    lead_ids: list[UUID] = Field(default_factory=list, max_length=100)
    lead_statuses: list[ResearchLeadStatus] = Field(
        default_factory=lambda: ["new", "watching", "followup", "queued"],
        max_length=10,
    )
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    reasons: list[ResearchHuntQueueMaintenanceReason] = Field(
        default_factory=lambda: [
            "stale_broad_or_passive",
            "duplicate_broad_family",
            "passive_monitoring_note",
        ],
        max_length=10,
    )
    stale_after_hours: int = Field(default=72, ge=1, le=24 * 90)
    limit: int = Field(default=50, ge=1, le=500)
    dry_run: bool = True
    operator: str = "research_hunt_queue_maintenance"
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_research_hunt_queue_maintenance_request(self) -> "ResearchHuntQueueMaintenanceRequest":
        self.lead_statuses = _dedupe_strings(self.lead_statuses)
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        self.reasons = _dedupe_strings(self.reasons)
        self.operator = self.operator.strip() or "research_hunt_queue_maintenance"
        return self


class ResearchHuntQueueMaintenanceItem(StrictBaseModel):
    lead_id: UUID
    task_id: str
    task_type: str
    task_class: ResearchHuntTaskClass
    action: str
    previous_status: str
    new_status: str = "suppressed"
    suppression_reason: ResearchHuntQueueMaintenanceReason
    identity_key: str | None = None
    family_key: str | None = None
    age_hours: float | None = None
    dry_run: bool = True


class ResearchHuntQueueMaintenanceResult(StrictBaseModel):
    action: ResearchHuntQueueMaintenanceAction = "suppress"
    dry_run: bool = True
    candidate_count: int = Field(default=0, ge=0)
    suppressed_count: int = Field(default=0, ge=0)
    updated_lead_count: int = Field(default=0, ge=0)
    skipped_count: int = Field(default=0, ge=0)
    items: list[ResearchHuntQueueMaintenanceItem] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class SourceScoutRequest(StrictBaseModel):
    focus: Literal["all", "scholarly", "canine", "omics", "chemistry", "structure", "safety"] = "all"
    max_phase: int = Field(default=3, ge=1, le=5)
    max_recommendations: int = Field(default=12, ge=1, le=50)
    include_registered_sources: bool = True
    include_expansion_sources: bool = True
    model_profile: str = "long_context_reviewer"


class SourceRecommendation(StrictBaseModel):
    source_key: str
    display_name: str
    source_kind: SourceKind
    status: Literal["active_coverage", "coverage_gap", "not_registered"]
    phase: int
    priority_score: float = Field(ge=0.0, le=1.0)
    current_raw_records: int = 0
    current_research_objects: int = 0
    rationale: str
    recommended_queries: list[SourceQuery] = Field(default_factory=list)
    implementation_notes: list[str] = Field(default_factory=list)


class SourceScoutResult(StrictBaseModel):
    scout_name: str
    focus: str
    model_profile: str
    coverage: dict[str, Any] = Field(default_factory=dict)
    recommendations: list[SourceRecommendation] = Field(default_factory=list)
    next_actions: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    errors: list[str] = Field(default_factory=list)


class RawSourceRecord(StrictBaseModel):
    id: UUID | None = None
    source_key: str
    source_record_id: str | None = None
    source_url: str | None = None
    content_hash: str
    raw_payload: dict[str, Any]
    retrieved_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResearchObject(StrictBaseModel):
    id: UUID = Field(default_factory=uuid4)
    object_type: ResearchObjectType
    title: str | None = None
    abstract: str | None = None
    canonical_url: str | None = None
    publication_year: int | None = None
    published_at: str | None = None
    source_key: str | None = None
    raw_record_id: UUID | None = None
    dedupe_key: str | None = None
    identifiers: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DocumentChunk(StrictBaseModel):
    id: UUID = Field(default_factory=uuid4)
    research_object_id: UUID
    chunk_index: int = Field(ge=0)
    section_label: str | None = None
    text_content: str
    content_hash: str
    token_count: int | None = None
    char_start: int | None = None
    char_end: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class TextEmbedding(StrictBaseModel):
    embedding_id: UUID = Field(default_factory=uuid4)
    chunk_id: UUID
    research_object_id: UUID
    chunk_index: int = Field(ge=0)
    source_key: str | None = None
    object_type: ResearchObjectType | None = None
    content_hash: str
    embedding_model: str
    embedding_dimensions: int = Field(ge=1)
    embedding: list[float] = Field(min_length=1)
    text_preview: str | None = Field(default=None, max_length=500)
    embedded_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_embedding_dimensions(self) -> "TextEmbedding":
        if self.embedding_dimensions != len(self.embedding):
            raise ValueError("embedding_dimensions must match embedding length")
        return self


class TextEmbeddingSearchRequest(StrictBaseModel):
    query_embedding: list[float] = Field(min_length=1)
    embedding_model: str | None = None
    source_key: str | None = None
    research_object_id: UUID | None = None
    object_type: ResearchObjectType | None = None
    min_score: float | None = Field(default=None, ge=-1.0, le=1.0)
    limit: int = Field(default=10, ge=1, le=100)


class TextEmbeddingSearchResult(StrictBaseModel):
    embedding: TextEmbedding
    score: float = Field(ge=-1.0, le=1.0)


class ResearchChunkSearchRequest(StrictBaseModel):
    query: str = Field(min_length=1, max_length=1000)
    source_key: str | None = None
    research_object_id: UUID | None = None
    object_type: ResearchObjectType | None = None
    embedding_model: str | None = None
    min_score: float | None = Field(default=None, ge=-1.0, le=1.0)
    limit: int = Field(default=10, ge=1, le=50)
    max_chunk_chars: int = Field(default=2000, ge=200, le=12000)
    include_keyword_fallback: bool = True


class ResearchChunkSearchResult(StrictBaseModel):
    rank: int = Field(ge=1)
    chunk: DocumentChunk
    research_object: ResearchObject | None = None
    score: float | None = Field(default=None, ge=-1.0, le=1.0)
    match_type: Literal["embedding", "keyword"]
    text_truncated: bool = False


class ResearchChunkSearchResults(StrictBaseModel):
    results: list[ResearchChunkSearchResult]
    total: int = Field(ge=0)
    query: ResearchChunkSearchRequest
    search_mode: Literal["embedding", "keyword", "none"]
    embedding_model: str | None = None


class ChunkContextRequest(StrictBaseModel):
    chunk_id: UUID
    window: int = Field(default=1, ge=0, le=5)
    max_chunk_chars: int = Field(default=4000, ge=200, le=12000)
    include_entity_mentions: bool = True


class ChunkContextResult(StrictBaseModel):
    chunk: DocumentChunk
    research_object: ResearchObject | None = None
    before_chunks: list[DocumentChunk] = Field(default_factory=list)
    after_chunks: list[DocumentChunk] = Field(default_factory=list)
    entity_mentions: list[EntityMention] = Field(default_factory=list)


class ResearchObjectReadRequest(StrictBaseModel):
    research_object_id: UUID
    include_chunks: bool = True
    max_chunks: int = Field(default=20, ge=0, le=100)
    max_chunk_chars: int = Field(default=2000, ge=200, le=12000)


class ResearchObjectReadResult(StrictBaseModel):
    research_object: ResearchObject
    chunks: list[DocumentChunk] = Field(default_factory=list)


class RetrievalSmokeRequest(StrictBaseModel):
    query: str = Field(default="hemangiosarcoma angiogenesis", min_length=1, max_length=1000)
    source_key: str | None = None
    object_type: ResearchObjectType | None = None
    embedding_model: str | None = None
    limit: int = Field(default=3, ge=1, le=20)
    max_chunk_chars: int = Field(default=1200, ge=200, le=12000)
    context_window: int = Field(default=1, ge=0, le=5)
    include_entity_mentions: bool = True
    include_keyword_fallback: bool = True
    require_embedding: bool = False


class RetrievalSmokeResult(StrictBaseModel):
    request: RetrievalSmokeRequest
    passed: bool
    errors: list[str] = Field(default_factory=list)
    selected_chunk_id: UUID | None = None
    selected_research_object_id: UUID | None = None
    search: ResearchChunkSearchResults
    chunk_context: ChunkContextResult | None = None
    research_object: ResearchObjectReadResult | None = None


class EmbeddingCoverageSummary(StrictBaseModel):
    source_key: str | None = None
    object_type: str | None = None
    embedding_model: str | None = None
    total_chunks: int = Field(ge=0)
    embedded_chunks: int = Field(ge=0)
    missing_chunks: int = Field(ge=0)
    coverage_ratio: float = Field(ge=0.0, le=1.0)
    embedding_models: dict[str, int] = Field(default_factory=dict)


class HarvestedRecord(StrictBaseModel):
    raw_record: RawSourceRecord
    research_object: ResearchObject


class IngestionResult(StrictBaseModel):
    source_key: str
    query_name: str
    query_text: str
    fetch_run_id: UUID
    raw_records: int = 0
    research_objects: int = 0
    document_chunks: int = 0
    full_text_research_objects: int = 0
    section_chunk_counts: dict[str, int] = Field(default_factory=dict)
    status: RunStatus = RunStatus.COMPLETED
    errors: list[str] = Field(default_factory=list)


class FullTextTriageRequest(StrictBaseModel):
    source_key: str
    stage: Literal[
        "fetch",
        "parse",
        "normalize",
        "chunk",
        "qa",
        "entity_resolution",
        "claim_extraction",
        "claim_curation",
        "source_health",
        "dagster_run",
    ] = "qa"
    query_name: str | None = None
    error_message: str | None = None
    errors: list[str] = Field(default_factory=list)
    runtime_seconds: float | None = Field(default=None, ge=0.0)
    timeout_seconds: float | None = Field(default=None, ge=1.0)
    raw_records: int = Field(default=0, ge=0)
    research_objects: int = Field(default=0, ge=0)
    document_chunks: int = Field(default=0, ge=0)
    full_text_document_chunks: int = Field(default=0, ge=0)
    full_text_body_chars: int = Field(default=0, ge=0)
    claims: int = Field(default=0, ge=0)
    entity_mentions: int = Field(default=0, ge=0)
    current_failed_runs: list[str] = Field(default_factory=list)
    http_status: int | None = Field(default=None, ge=100, le=599)
    model_profile: str = "cheap_classifier"
    metadata: dict[str, Any] = Field(default_factory=dict)


class FullTextTriageResult(StrictBaseModel):
    triage_name: str = "full_text_triage_agent"
    source_key: str
    stage: str
    action: Literal[
        "no_action",
        "retry_later",
        "reduce_batch_size",
        "needs_parser_fix",
        "needs_license_review",
        "skip_record",
        "needs_human_review",
        "inspect_source_health",
    ]
    severity: Literal["info", "watch", "blocking"]
    should_retry: bool = False
    should_block_schedule: bool = False
    reasons: list[str] = Field(default_factory=list)
    recommended_next_actions: list[str] = Field(default_factory=list)
    model_profile: str = "cheap_classifier"
    metadata: dict[str, Any] = Field(default_factory=dict)


class BackfillResult(StrictBaseModel):
    source_key: str
    path: str
    raw_records: int = 0
    research_objects: int = 0
    document_chunks: int = 0
    skipped: int = 0
    errors: list[str] = Field(default_factory=list)


class ClaimExtractionResult(StrictBaseModel):
    extractor_name: str
    chunks_seen: int = 0
    chunks_with_claims: int = 0
    claims_extracted: int = 0
    claims_written: int = 0
    errors: list[str] = Field(default_factory=list)


class EntityRef(StrictBaseModel):
    entity_id: UUID | None = None
    entity_type: str
    canonical_name: str
    external_ids: dict[str, str] = Field(default_factory=dict)
    role: str | None = None


class ResolvedEntity(StrictBaseModel):
    entity_id: UUID = Field(default_factory=uuid4)
    entity_type: str
    canonical_name: str
    normalized_key: str
    external_ids: dict[str, str] = Field(default_factory=dict)
    resolver_name: str
    resolver_version: str
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class EntityAlias(StrictBaseModel):
    alias_id: UUID = Field(default_factory=uuid4)
    entity_id: UUID
    entity_type: str
    alias: str
    alias_normalized: str
    canonical_name: str
    normalized_key: str
    resolver_name: str
    resolver_version: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class EntityMention(StrictBaseModel):
    mention_id: UUID = Field(default_factory=uuid4)
    entity_id: UUID | None = None
    research_object_id: UUID
    chunk_id: UUID
    chunk_index: int
    section_label: str | None = None
    source_key: str | None = None
    entity_type: str
    canonical_name: str
    normalized_key: str
    matched_text: str
    matched_alias: str
    chunk_char_start: int = Field(ge=0)
    chunk_char_end: int = Field(ge=0)
    external_ids: dict[str, str] = Field(default_factory=dict)
    resolver_name: str
    resolver_version: str
    match_rule: str
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class EntityResolutionRequest(StrictBaseModel):
    source_key: str | None = None
    limit: int | None = Field(default=None, ge=1)
    resolver_profile: Literal["local", "pubtator", "local_plus_pubtator"] = "local"


class EntityResolutionResult(StrictBaseModel):
    resolver_name: str
    resolver_version: str
    resolver_profile: str
    chunks_seen: int = 0
    chunks_with_mentions: int = 0
    entities_upserted: int = 0
    aliases_upserted: int = 0
    mentions_upserted: int = 0
    errors: list[str] = Field(default_factory=list)


class ClaimSearchRequest(StrictBaseModel):
    query: str | None = None
    species: str | None = None
    targets: list[str] = Field(default_factory=list)
    compounds: list[str] = Field(default_factory=list)
    claim_types: list[ClaimType] = Field(default_factory=list)
    evidence_levels: list[EvidenceLevel] = Field(default_factory=list)
    min_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    include_drafts: bool = False
    include_conflicts: bool = True
    limit: int = Field(default=20, ge=1, le=100)


SearchClaimsRequest = ClaimSearchRequest


class ClaimSearchResult(StrictBaseModel):
    claim_id: UUID
    statement: str
    claim_type: ClaimType
    direction: ClaimDirection = ClaimDirection.UNKNOWN
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_level: EvidenceLevel = EvidenceLevel.UNKNOWN
    species: str | None = None
    entities: list[EntityRef] = Field(default_factory=list)
    source_object_id: UUID | None = None
    source_title: str | None = None
    source_url: str | None = None
    support_count: int = 0
    contradiction_count: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class ClaimSearchResults(StrictBaseModel):
    results: list[ClaimSearchResult]
    total: int
    query: ClaimSearchRequest


class ClaimCurationRequest(StrictBaseModel):
    source_key: str | None = None
    query: str | None = None
    limit: int = Field(default=100, ge=1, le=5000)
    min_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    promote_threshold: float = Field(default=0.5, ge=0.0, le=1.0)
    include_seed_claims: bool = False
    dry_run: bool = False
    model_profile: str = "reviewer"


class ClaimCurationItem(StrictBaseModel):
    claim_id: UUID
    statement: str
    decision: ClaimCurationDecision
    curation_score: float = Field(ge=0.0, le=1.0)
    original_confidence: float = Field(ge=0.0, le=1.0)
    curated_confidence: float = Field(ge=0.0, le=1.0)
    canonical_claim_id: UUID | None = None
    reasons: list[str] = Field(default_factory=list)


class ClaimCurationResult(StrictBaseModel):
    curator_name: str
    model_profile: str
    claims_seen: int = 0
    promoted: int = 0
    merged_duplicates: int = 0
    needs_review: int = 0
    rejected: int = 0
    dry_run: bool = False
    decisions: list[ClaimCurationItem] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class CandidateDossierRequest(StrictBaseModel):
    candidate_id: UUID | None = None
    candidate_name: str | None = None
    include_claims: bool = True
    include_artifacts: bool = True
    include_validation: bool = True


class ArtifactHandle(StrictBaseModel):
    artifact_id: UUID
    artifact_type: str
    uri: str
    legal_status: str = "unknown"
    mime_type: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ScrapeSourceProfile(StrictBaseModel):
    source_key: str
    display_name: str
    base_url: str
    allowed_url_patterns: list[str] = Field(default_factory=list)
    robots_policy: Literal["unknown", "reviewed", "disallow", "manual_only"] = "unknown"
    rate_limit_per_minute: int = Field(default=10, ge=1, le=120)
    parser: str = "generic_html"
    storage_policy: str = "link_and_metadata"
    approval_required: bool = True
    enabled: bool = False
    notes: str | None = None


class ScrapeSourceProfileReview(StrictBaseModel):
    source_key: str
    robots_policy: Literal["unknown", "reviewed", "disallow", "manual_only"] = "unknown"
    approved_for_fetch: bool = False
    reviewed_by: str
    review_note: str | None = None
    allowed_url_patterns: list[str] = Field(default_factory=list)
    storage_policy: str | None = None
    reviewed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)


class ScrapeProfileReviewRequest(StrictBaseModel):
    source_key: str
    robots_policy: Literal["unknown", "reviewed", "disallow", "manual_only"]
    approved_for_fetch: bool = False
    reviewed_by: str
    review_note: str | None = None
    allowed_url_patterns: list[str] = Field(default_factory=list)
    storage_policy: str | None = None


class ScrapeManifestItem(StrictBaseModel):
    url: str
    link_text: str | None = None
    discovered_from: str | None = None
    reason: str | None = None
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)


class ScrapeManifestRequest(StrictBaseModel):
    source_key: str
    seed_urls: list[str] = Field(default_factory=list)
    fetch_seed_pages: bool = False
    max_seed_pages: int = Field(default=5, ge=1, le=50)
    max_candidate_urls: int = Field(default=50, ge=1, le=500)
    approved_by: str | None = None
    approval_note: str | None = None


class ScrapeManifestResult(StrictBaseModel):
    source_key: str
    seed_artifacts_seen: int = 0
    fetched_seed_pages: int = 0
    candidate_urls: list[ScrapeManifestItem] = Field(default_factory=list)
    artifact_ids: list[UUID] = Field(default_factory=list)
    manifest_artifact_id: UUID | None = None
    skipped_urls: int = 0
    requires_review: bool = True
    errors: list[str] = Field(default_factory=list)


class ScrapeManifestFetchRequest(StrictBaseModel):
    source_key: str
    manifest_artifact_id: UUID
    max_pages: int = Field(default=10, ge=1, le=100)
    approved_by: str | None = None
    approval_note: str | None = None


class ScrapeFetchRequest(StrictBaseModel):
    source_key: str
    urls: list[str] = Field(default_factory=list)
    max_pages: int = Field(default=10, ge=1, le=100)
    approved_by: str | None = None
    approval_note: str | None = None


class ScrapeFetchResult(StrictBaseModel):
    source_key: str
    fetched_pages: int = 0
    skipped_pages: int = 0
    artifact_ids: list[UUID] = Field(default_factory=list)
    requires_review: bool = True
    errors: list[str] = Field(default_factory=list)


class ScrapeParsedRecord(StrictBaseModel):
    source_key: str
    source_record_id: str
    title: str | None = None
    canonical_url: str | None = None
    record_type: ResearchObjectType | None = None
    fields: dict[str, Any] = Field(default_factory=dict)
    parser_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    review_status: Literal["needs_review", "accepted", "rejected"] = "needs_review"
    artifact_id: UUID | None = None


class ScrapeParseResult(StrictBaseModel):
    source_key: str
    artifacts_seen: int = 0
    parsed_records: int = 0
    skipped_records: int = 0
    review_ids: list[UUID] = Field(default_factory=list)
    records: list[ScrapeParsedRecord] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class ScrapeReviewRecord(StrictBaseModel):
    review_id: UUID = Field(default_factory=uuid4)
    source_key: str
    artifact_id: UUID
    source_record_id: str
    title: str | None = None
    canonical_url: str | None = None
    record_type: ResearchObjectType | None = None
    fields: dict[str, Any] = Field(default_factory=dict)
    parser_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    review_status: Literal["needs_review", "accepted", "rejected"] = "needs_review"
    reviewer: str | None = None
    review_note: str | None = None
    parsed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    reviewed_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ScrapeReviewRequest(StrictBaseModel):
    source_key: str
    review_ids: list[UUID]
    decision: Literal["needs_review", "accepted", "rejected"]
    reviewed_by: str
    review_note: str | None = None


class ScrapeReviewResult(StrictBaseModel):
    source_key: str
    decision: str
    reviewed_records: int = 0
    skipped_records: int = 0
    records: list[ScrapeReviewRecord] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class ScrapeIngestRequest(StrictBaseModel):
    source_key: str
    review_ids: list[UUID] = Field(default_factory=list)
    artifact_ids: list[UUID] = Field(default_factory=list)
    limit: int | None = Field(default=None, ge=1, le=1000)
    min_parser_confidence: float = Field(default=0.3, ge=0.0, le=1.0)
    approved_by: str | None = None
    approval_note: str | None = None


class ScrapeIngestResult(StrictBaseModel):
    source_key: str
    fetch_run_id: UUID | None = None
    artifacts_seen: int = 0
    review_records_seen: int = 0
    parsed_records: int = 0
    promoted_records: int = 0
    raw_records: int = 0
    research_objects: int = 0
    document_chunks: int = 0
    skipped_records: int = 0
    errors: list[str] = Field(default_factory=list)


class CandidateDossier(StrictBaseModel):
    candidate_id: UUID
    name: str
    status: str = "investigating"
    summary: str | None = None
    evidence_claims: list[ClaimSearchResult] = Field(default_factory=list)
    validation_runs: list["AsyncRunHandle"] = Field(default_factory=list)
    artifacts: list[ArtifactHandle] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class HypothesisProposalRequest(StrictBaseModel):
    objective: str
    claim_ids: list[UUID] = Field(default_factory=list)
    candidate_name: str | None = None
    target_name: str | None = None
    species: str = "canine"
    model_profile: str = "hypothesis"
    commit: bool = False
    max_supporting_claims: int = Field(default=8, ge=1, le=25)


class HypothesisDraft(StrictBaseModel):
    hypothesis_id: UUID | None = None
    title: str
    hypothesis: str
    rationale: str
    status: Literal["draft", "proposed", "approved"] = "draft"
    supporting_claim_ids: list[UUID] = Field(default_factory=list)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    proposed_by: str = "mcp"
    metadata: dict[str, Any] = Field(default_factory=dict)


class CommitHypothesisRequest(StrictBaseModel):
    draft: HypothesisDraft
    approved_by: str
    approval_note: str | None = None


class BoltzRunRequest(StrictBaseModel):
    target_name: str
    ligand_smiles: str | None = None
    ligand_name: str | None = None
    protein_sequence: str | None = None
    candidate_id: UUID | None = None
    priority: int = Field(default=100, ge=1, le=1000)
    require_approval: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class ValidationAssayContext(StrictBaseModel):
    disease_context: str | None = Field(default=None, max_length=500)
    species: list[str] = Field(default_factory=list, max_length=10)
    model_system: str | None = Field(default=None, max_length=500)
    assay_type: str | None = Field(default=None, max_length=200)
    readout: str | None = Field(default=None, max_length=500)
    endpoint: str | None = Field(default=None, max_length=500)
    comparator_or_control: str | None = Field(default=None, max_length=500)
    sample_context: str | None = Field(default=None, max_length=500)
    dose_or_exposure_context: str | None = Field(default=None, max_length=500)
    safety_context: str | None = Field(default=None, max_length=500)
    evidence_refs: list[str] = Field(default_factory=list, max_length=25)
    negative_evidence_needs: list[str] = Field(default_factory=list, max_length=25)
    provenance_requirements: list[str] = Field(default_factory=list, max_length=25)

    @model_validator(mode="after")
    def normalize_assay_context(self) -> "ValidationAssayContext":
        for field_name in (
            "disease_context",
            "model_system",
            "assay_type",
            "readout",
            "endpoint",
            "comparator_or_control",
            "sample_context",
            "dose_or_exposure_context",
            "safety_context",
        ):
            value = getattr(self, field_name)
            if isinstance(value, str):
                normalized = value.strip()
                setattr(self, field_name, normalized or None)
        self.species = _normalized_unique_strings(self.species)
        self.evidence_refs = _normalized_unique_strings(self.evidence_refs)
        self.negative_evidence_needs = _normalized_unique_strings(self.negative_evidence_needs)
        self.provenance_requirements = _normalized_unique_strings(self.provenance_requirements)
        return self


class ValidationRequest(StrictBaseModel):
    validation_type: Literal[
        "boltz",
        "docking",
        "md",
        "admet",
        "homology",
        "safety",
        "expert_review",
        "wet_lab",
        "omics",
    ]
    candidate_id: UUID | None = None
    candidate_name: str | None = None
    target_name: str | None = None
    objective: str
    priority: int = Field(default=100, ge=1, le=1000)
    require_approval: bool = True
    assay_context: ValidationAssayContext | None = None
    quality_gates: list[str] = Field(default_factory=list, max_length=25)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_validation_request(self) -> "ValidationRequest":
        self.objective = self.objective.strip()
        if self.candidate_name:
            self.candidate_name = self.candidate_name.strip() or None
        if self.target_name:
            self.target_name = self.target_name.strip() or None
        self.quality_gates = _normalized_unique_strings(self.quality_gates)
        return self


class AsyncRunHandle(StrictBaseModel):
    run_id: UUID = Field(default_factory=uuid4)
    run_kind: Literal["dagster", "runpod", "mcp", "local", "external"] = "mcp"
    run_name: str
    status: RunStatus = RunStatus.QUEUED
    external_run_id: str | None = None
    dagster_run_id: str | None = None
    runpod_job_id: str | None = None
    cost_estimate_usd: float | None = None
    artifact_ids: list[UUID] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ValidationPlanTask(StrictBaseModel):
    task_id: UUID = Field(default_factory=uuid4)
    task_type: ValidationPlanTaskType
    title: str = Field(min_length=1, max_length=500)
    objective: str = Field(min_length=1, max_length=2000)
    rationale: str = Field(min_length=1, max_length=3000)
    validation_request: ValidationRequest | None = None
    required_inputs: list[str] = Field(default_factory=list, max_length=25)
    expected_outputs: list[str] = Field(default_factory=list, max_length=25)
    evidence_refs: list[str] = Field(default_factory=list, max_length=25)
    priority: int = Field(default=100, ge=1, le=1000)
    requires_human_approval: bool = True
    tool_hint: str | None = Field(default=None, max_length=200)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ValidationPlanRequest(StrictBaseModel):
    brief_id: UUID | None = None
    evaluation_id: UUID | None = None
    topic_query: str | None = None
    source_key: str | None = None
    require_ready_evaluation: bool = True
    max_tasks: int = Field(default=8, ge=1, le=25)
    model_profile: str = "validation_planner"
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ValidationPlanResult(StrictBaseModel):
    plan_id: UUID = Field(default_factory=uuid4)
    agent_run_id: UUID | None = None
    agent_name: str = "validation_planning_agent"
    model_profile: str = "validation_planner"
    brief_id: UUID
    evaluation_id: UUID | None = None
    topic: str
    source_key: str | None = None
    status: ValidationPlanStatus = "draft"
    readiness: ValidationPlanReadiness = "needs_better_synthesis"
    hypothesis_drafts: list[HypothesisDraft] = Field(default_factory=list, max_length=25)
    tasks: list[ValidationPlanTask] = Field(default_factory=list, max_length=25)
    evidence: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ValidationPlanRecord(StrictBaseModel):
    plan_id: UUID = Field(default_factory=uuid4)
    agent_run_id: UUID | None = None
    brief_id: UUID
    evaluation_id: UUID | None = None
    topic: str
    source_key: str | None = None
    model_profile: str = "validation_planner"
    status: ValidationPlanStatus = "draft"
    readiness: ValidationPlanReadiness = "needs_better_synthesis"
    task_count: int = Field(default=0, ge=0)
    hypothesis_count: int = Field(default=0, ge=0)
    summary: dict[str, Any] = Field(default_factory=dict)
    result_payload: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)


class ValidationRequestQueueItem(StrictBaseModel):
    queue_item_id: UUID = Field(default_factory=uuid4)
    identity_key: str | None = None
    status: ValidationRequestQueueStatus = "needs_approval"
    plan_id: UUID
    task_id: UUID
    brief_id: UUID
    evaluation_id: UUID | None = None
    source_key: str | None = None
    topic: str = Field(min_length=1, max_length=1000)
    task_type: ValidationPlanTaskType
    title: str = Field(min_length=1, max_length=500)
    objective: str = Field(min_length=1, max_length=2000)
    rationale: str = Field(min_length=1, max_length=3000)
    validation_request: ValidationRequest
    priority: int = Field(default=100, ge=1, le=1000)
    requires_human_approval: bool = True
    quality_gates: list[str] = Field(default_factory=list, max_length=25)
    dispatch_blockers: list[str] = Field(default_factory=list, max_length=25)
    last_run_id: UUID | None = None
    attempts: int = Field(default=0, ge=0)
    last_error: str | None = None
    approved_by: str | None = Field(default=None, max_length=200)
    approval_note: str | None = Field(default=None, max_length=1000)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_queue_item(self) -> "ValidationRequestQueueItem":
        self.topic = self.topic.strip()
        self.title = self.title.strip()
        self.objective = self.objective.strip()
        self.rationale = self.rationale.strip()
        self.quality_gates = _normalized_unique_strings(self.quality_gates)
        self.dispatch_blockers = _normalized_unique_strings(self.dispatch_blockers)
        if not self.identity_key:
            self.identity_key = f"validation_request_queue:{self.plan_id}:{self.task_id}"
        else:
            self.identity_key = self.identity_key.strip()
        return self


class ValidationRequestQueueRequest(StrictBaseModel):
    plan_id: UUID
    task_ids: list[UUID] = Field(default_factory=list, max_length=25)
    dry_run: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class ValidationRequestQueueResult(StrictBaseModel):
    plan_id: UUID
    candidate_task_count: int = 0
    queued_count: int = 0
    existing_count: int = 0
    skipped_count: int = 0
    dry_run: bool = True
    queue_items: list[ValidationRequestQueueItem] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ValidationPacketRequest(StrictBaseModel):
    candidate_id: str | None = Field(default=None, max_length=240)
    therapy_idea_id: UUID | None = None
    plan_id: UUID | None = None
    queue_item_id: UUID | None = None
    brief_id: UUID | None = None
    evaluation_id: UUID | None = None
    topic_query: str | None = None
    source_key: str | None = None
    include_queue_items: bool = True
    include_evidence_addendum: bool = True
    addendum_limit: int = Field(default=25, ge=0, le=200)
    queue_if_ready: bool = False
    dry_run: bool = True
    max_tasks: int = Field(default=8, ge=1, le=25)
    priority: int = Field(default=40, ge=1, le=1000)
    limit: int = Field(default=10, ge=1, le=100)
    model_profile: str = "validation_packet_builder"
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_validation_packet_request(self) -> "ValidationPacketRequest":
        if self.candidate_id:
            self.candidate_id = self.candidate_id.strip() or None
        if self.topic_query:
            self.topic_query = self.topic_query.strip() or None
        if self.source_key:
            self.source_key = self.source_key.strip().lower() or None
        self.model_profile = self.model_profile.strip() or "validation_packet_builder"
        return self


class ValidationPacketAddendumBrief(StrictBaseModel):
    queue_item_id: UUID
    topic: str = Field(min_length=3, max_length=1000)
    status: ResearchBriefQueueStatus
    source_key: str | None = None
    priority: int = Field(default=100, ge=0, le=1000)
    lead_id: UUID | None = None
    origin_queue_item_id: UUID | None = None
    plan_id: UUID | None = None
    lane: str | None = Field(default=None, max_length=100)
    task_type: str | None = Field(default=None, max_length=100)
    validation_type: str | None = Field(default=None, max_length=100)
    brief_id: UUID | None = None
    brief_agent_run_id: UUID | None = None
    evaluation_id: UUID | None = None
    evaluation_agent_run_id: UUID | None = None
    overall_score: float | None = Field(default=None, ge=0.0, le=1.0)
    passes_quality_bar: bool | None = None
    readiness: ResearchBriefEvaluationReadiness | None = None
    summary: dict[str, Any] = Field(default_factory=dict)
    key_strengths: list[str] = Field(default_factory=list, max_length=10)
    key_weaknesses: list[str] = Field(default_factory=list, max_length=10)
    recommendations: list[str] = Field(default_factory=list, max_length=10)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_addendum_brief(self) -> "ValidationPacketAddendumBrief":
        self.topic = self.topic.strip()
        if self.source_key:
            self.source_key = self.source_key.strip().lower() or None
        if self.lane:
            self.lane = self.lane.strip() or None
        if self.task_type:
            self.task_type = self.task_type.strip() or None
        if self.validation_type:
            self.validation_type = self.validation_type.strip() or None
        self.key_strengths = _dedupe_strings(self.key_strengths)
        self.key_weaknesses = _dedupe_strings(self.key_weaknesses)
        self.recommendations = _dedupe_strings(self.recommendations)
        return self


class ValidationPacketEvidenceAddendum(StrictBaseModel):
    follow_up_count: int = 0
    completed_follow_up_count: int = 0
    evaluated_follow_up_count: int = 0
    passing_follow_up_count: int = 0
    ready_for_hypothesis_review_count: int = 0
    needs_more_evidence_count: int = 0
    failed_follow_up_count: int = 0
    material_updates: list[str] = Field(default_factory=list, max_length=25)
    unresolved_blockers: list[str] = Field(default_factory=list, max_length=25)
    follow_up_briefs: list[ValidationPacketAddendumBrief] = Field(default_factory=list, max_length=200)
    latest_updated_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_evidence_addendum(self) -> "ValidationPacketEvidenceAddendum":
        self.material_updates = _dedupe_strings(self.material_updates)
        self.unresolved_blockers = _dedupe_strings(self.unresolved_blockers)
        return self


class ValidationPacket(StrictBaseModel):
    packet_id: str = Field(min_length=3, max_length=260)
    candidate_id: str = Field(min_length=3, max_length=240)
    source_type: HypothesisPromotionSourceType
    source_id: str = Field(min_length=1, max_length=200)
    therapy_idea_id: UUID | None = None
    brief_id: UUID | None = None
    evaluation_id: UUID | None = None
    committee_run_id: UUID | None = None
    agent_run_id: UUID | None = None
    therapy_idea: TherapyIdeaRecord | None = None
    promotion_candidate: HypothesisPromotionCandidate
    validation_plan: ValidationPlanRecord | None = None
    queue_items: list[ValidationRequestQueueItem] = Field(default_factory=list)
    validation_tasks: list[ValidationPlanTask] = Field(default_factory=list, max_length=25)
    title: str = Field(min_length=1, max_length=500)
    hypothesis: str = Field(min_length=1, max_length=3000)
    disease_scope: str = Field(default="canine hemangiosarcoma and human angiosarcoma", max_length=500)
    candidate_therapies: list[str] = Field(default_factory=list, max_length=50)
    targets: list[str] = Field(default_factory=list, max_length=50)
    biomarkers: list[str] = Field(default_factory=list, max_length=50)
    evidence_refs: list[str] = Field(default_factory=list, max_length=100)
    direct_evidence_refs: list[str] = Field(default_factory=list, max_length=100)
    analog_evidence_refs: list[str] = Field(default_factory=list, max_length=100)
    evidence_addendum: ValidationPacketEvidenceAddendum = Field(default_factory=ValidationPacketEvidenceAddendum)
    missing_evidence: list[str] = Field(default_factory=list, max_length=50)
    safety_risks: list[str] = Field(default_factory=list, max_length=50)
    contradictions: list[str] = Field(default_factory=list, max_length=50)
    next_experiments: list[str] = Field(default_factory=list, max_length=50)
    matched_tools: list[ValidationToolMatch] = Field(default_factory=list, max_length=10)
    required_inputs: list[str] = Field(default_factory=list, max_length=50)
    quality_gates: list[str] = Field(default_factory=list, max_length=50)
    dispatch_blockers: list[str] = Field(default_factory=list, max_length=50)
    promotion_state: HypothesisPromotionState | None = None
    status: ValidationPacketStatus = "draft"
    readiness: ValidationPacketReadiness = "needs_more_evidence"
    discovery_readiness: DiscoveryReadiness = "needs_more_evidence"
    validation_strategy_readiness: ValidationStrategyReadiness = "needs_more_evidence"
    protocol_readiness: ProtocolReadiness = "needs_protocol_inputs"
    risk_annotations: list[str] = Field(default_factory=list, max_length=50)
    protocol_blockers: list[str] = Field(default_factory=list, max_length=50)
    score: float = Field(default=0.0, ge=0.0, le=1.0)
    summary: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_validation_packet(self) -> "ValidationPacket":
        self.packet_id = self.packet_id.strip()
        self.candidate_id = self.candidate_id.strip()
        self.source_id = self.source_id.strip()
        self.title = self.title.strip()
        self.hypothesis = self.hypothesis.strip()
        self.disease_scope = self.disease_scope.strip()
        self.candidate_therapies = _dedupe_strings(self.candidate_therapies)
        self.targets = _dedupe_strings(self.targets)
        self.biomarkers = _dedupe_strings(self.biomarkers)
        self.evidence_refs = _dedupe_strings(self.evidence_refs)
        self.direct_evidence_refs = _dedupe_strings(self.direct_evidence_refs)
        self.analog_evidence_refs = _dedupe_strings(self.analog_evidence_refs)
        self.missing_evidence = _dedupe_strings(self.missing_evidence)
        self.safety_risks = _dedupe_strings(self.safety_risks)
        self.contradictions = _dedupe_strings(self.contradictions)
        self.next_experiments = _dedupe_strings(self.next_experiments)
        self.required_inputs = _dedupe_strings(self.required_inputs)
        self.quality_gates = _dedupe_strings(self.quality_gates)
        self.dispatch_blockers = _dedupe_strings(self.dispatch_blockers)
        self.risk_annotations = _dedupe_strings(self.risk_annotations)
        self.protocol_blockers = _dedupe_strings(self.protocol_blockers)
        return self


class ValidationPacketResult(StrictBaseModel):
    packet_count: int = 0
    ready_count: int = 0
    blocked_count: int = 0
    created_plan_count: int = 0
    queued_count: int = 0
    existing_queue_count: int = 0
    dry_run: bool = True
    packets: list[ValidationPacket] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchProgramQuestion(StrictBaseModel):
    question_id: UUID = Field(default_factory=uuid4)
    question: str = Field(min_length=3, max_length=1000)
    rationale: str = Field(default="", max_length=2000)
    metric_plan: list[str] = Field(default_factory=list, max_length=20)
    tool_hints: list[str] = Field(default_factory=list, max_length=20)
    confidence_increase_criteria: list[str] = Field(default_factory=list, max_length=20)
    confidence_decrease_criteria: list[str] = Field(default_factory=list, max_length=20)
    evidence_refs: list[str] = Field(default_factory=list, max_length=50)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_research_program_question(self) -> "ResearchProgramQuestion":
        self.question = self.question.strip()
        self.rationale = self.rationale.strip()
        self.metric_plan = _dedupe_strings(self.metric_plan)
        self.tool_hints = _dedupe_strings(self.tool_hints)
        self.confidence_increase_criteria = _dedupe_strings(self.confidence_increase_criteria)
        self.confidence_decrease_criteria = _dedupe_strings(self.confidence_decrease_criteria)
        self.evidence_refs = _dedupe_strings(self.evidence_refs)
        return self


class ResearchProgramEvidenceTask(StrictBaseModel):
    task_id: UUID = Field(default_factory=uuid4)
    question_id: UUID | None = None
    task_type: ResearchProgramEvidenceTaskType = "literature_search"
    title: str = Field(min_length=3, max_length=500)
    objective: str = Field(min_length=3, max_length=2000)
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    tool_hints: list[str] = Field(default_factory=list, max_length=20)
    metrics: list[str] = Field(default_factory=list, max_length=25)
    pass_values: list[str] = Field(default_factory=list, max_length=25)
    fail_values: list[str] = Field(default_factory=list, max_length=25)
    evidence_refs: list[str] = Field(default_factory=list, max_length=50)
    negative_coverage: list[str] = Field(default_factory=list, max_length=25)
    status: ResearchProgramEvidenceTaskStatus = "proposed"
    priority: int = Field(default=50, ge=1, le=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_research_program_evidence_task(self) -> "ResearchProgramEvidenceTask":
        self.title = self.title.strip()
        self.objective = self.objective.strip()
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        self.tool_hints = _dedupe_strings(self.tool_hints)
        self.metrics = _dedupe_strings(self.metrics)
        self.pass_values = _dedupe_strings(self.pass_values)
        self.fail_values = _dedupe_strings(self.fail_values)
        self.evidence_refs = _dedupe_strings(self.evidence_refs)
        self.negative_coverage = _dedupe_strings(self.negative_coverage)
        return self


class ResearchProgramRecord(StrictBaseModel):
    program_id: UUID = Field(default_factory=uuid4)
    agent_run_id: UUID | None = None
    title: str = Field(min_length=3, max_length=500)
    thesis: str = Field(min_length=3, max_length=3000)
    disease_model: str = Field(min_length=3, max_length=3000)
    disease_scope: str = Field(default="canine hemangiosarcoma and human angiosarcoma", max_length=500)
    thesis_area: str = Field(default="comparative_oncology", max_length=200)
    therapy_families: list[str] = Field(default_factory=list, max_length=50)
    modality_families: list[str] = Field(default_factory=list, max_length=50)
    decisive_questions: list[ResearchProgramQuestion] = Field(default_factory=list, min_length=2, max_length=4)
    evidence_tasks: list[ResearchProgramEvidenceTask] = Field(default_factory=list, max_length=25)
    metric_plan: list[str] = Field(default_factory=list, max_length=50)
    recommended_tools: list[str] = Field(default_factory=list, max_length=50)
    stop_criteria: list[str] = Field(default_factory=list, min_length=1, max_length=25)
    confidence_increase_criteria: list[str] = Field(default_factory=list, max_length=25)
    confidence_decrease_criteria: list[str] = Field(default_factory=list, max_length=25)
    downstream_therapy_opportunities: list[str] = Field(default_factory=list, max_length=50)
    status: ResearchProgramStatus = "proposed"
    gate_decision: ResearchProgramGateDecision = "needs_one_more_pass"
    biological_plausibility_score: float = Field(default=0.5, ge=0.0, le=1.0)
    cross_species_support_score: float = Field(default=0.5, ge=0.0, le=1.0)
    evidence_density_score: float = Field(default=0.5, ge=0.0, le=1.0)
    novelty_score: float = Field(default=0.5, ge=0.0, le=1.0)
    testability_score: float = Field(default=0.5, ge=0.0, le=1.0)
    therapeutic_leverage_score: float = Field(default=0.5, ge=0.0, le=1.0)
    failure_risk_score: float = Field(default=0.5, ge=0.0, le=1.0)
    confidence_score: float = Field(default=0.5, ge=0.0, le=1.0)
    max_evidence_loops: int = Field(default=2, ge=1, le=5)
    evidence_loop_count: int = Field(default=0, ge=0, le=5)
    source_query: str | None = Field(default=None, max_length=1000)
    source_packet_ids: list[str] = Field(default_factory=list, max_length=50)
    evidence_refs: list[str] = Field(default_factory=list, max_length=100)
    review_summary: str = Field(default="", max_length=3000)
    errors: list[str] = Field(default_factory=list, max_length=25)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_research_program_record(self) -> "ResearchProgramRecord":
        self.title = self.title.strip()
        self.thesis = self.thesis.strip()
        self.disease_model = self.disease_model.strip()
        self.disease_scope = self.disease_scope.strip()
        self.thesis_area = re.sub(r"\s+", "_", self.thesis_area.strip().lower()) or "comparative_oncology"
        self.therapy_families = _dedupe_strings(self.therapy_families)
        self.modality_families = _dedupe_strings(self.modality_families)
        self.metric_plan = _dedupe_strings(self.metric_plan)
        self.recommended_tools = _dedupe_strings(self.recommended_tools)
        self.stop_criteria = _dedupe_strings(self.stop_criteria)
        self.confidence_increase_criteria = _dedupe_strings(self.confidence_increase_criteria)
        self.confidence_decrease_criteria = _dedupe_strings(self.confidence_decrease_criteria)
        self.downstream_therapy_opportunities = _dedupe_strings(self.downstream_therapy_opportunities)
        self.source_packet_ids = _dedupe_strings(self.source_packet_ids)
        self.evidence_refs = _dedupe_strings(self.evidence_refs)
        self.errors = _dedupe_strings(self.errors)
        self.review_summary = self.review_summary.strip()
        if self.source_query:
            self.source_query = self.source_query.strip() or None
        if self.evidence_loop_count > self.max_evidence_loops:
            raise ValueError("evidence_loop_count cannot exceed max_evidence_loops")
        return self


class ResearchProgramReviewRequest(StrictBaseModel):
    program_id: UUID | None = None
    brief_ids: list[UUID] = Field(default_factory=list, max_length=25)
    evaluation_ids: list[UUID] = Field(default_factory=list, max_length=25)
    thesis_topic: str = Field(
        default="vascular injury / coagulation / angiogenesis ecology in canine HSA and human angiosarcoma",
        min_length=3,
        max_length=1000,
    )
    disease_scope: str = Field(default="canine hemangiosarcoma and human angiosarcoma", max_length=500)
    topic_query: str | None = None
    source_key: str | None = None
    include_validation_packets: bool = True
    include_evidence_addendum: bool = True
    max_packets: int = Field(default=5, ge=0, le=25)
    max_chunks: int = Field(default=20, ge=0, le=100)
    max_chunk_chars: int = Field(default=1800, ge=200, le=8000)
    max_programs: int = Field(default=1, ge=1, le=5)
    max_evidence_loops: int = Field(default=2, ge=1, le=5)
    review_mode: Literal["openrouter_required", "deterministic_only"] = "openrouter_required"
    review_models: list[str] = Field(default_factory=list, max_length=5)
    model_profile: str = "research_program_board"
    persist: bool = True
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_research_program_review_request(self) -> "ResearchProgramReviewRequest":
        self.thesis_topic = self.thesis_topic.strip()
        self.disease_scope = self.disease_scope.strip()
        if self.topic_query:
            self.topic_query = self.topic_query.strip() or None
        if self.source_key:
            self.source_key = self.source_key.strip().lower() or None
        self.brief_ids = list(dict.fromkeys(self.brief_ids))
        self.evaluation_ids = list(dict.fromkeys(self.evaluation_ids))
        self.review_models = _dedupe_strings(self.review_models)
        self.model_profile = self.model_profile.strip() or "research_program_board"
        return self


class ResearchProgramReviewResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    program_count: int = 0
    persisted_count: int = 0
    packet_count: int = 0
    evidence_chunk_count: int = 0
    programs: list[ResearchProgramRecord] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchProgramBoardRequest(StrictBaseModel):
    program_id: UUID | None = None
    status: ResearchProgramStatus | None = None
    gate_decision: ResearchProgramGateDecision | None = None
    thesis_query: str | None = None
    limit: int = Field(default=50, ge=1, le=500)

    @model_validator(mode="after")
    def normalize_research_program_board_request(self) -> "ResearchProgramBoardRequest":
        if self.thesis_query:
            self.thesis_query = self.thesis_query.strip() or None
        return self


class ResearchProgramBoardResult(StrictBaseModel):
    program_count: int = 0
    status_counts: dict[str, int] = Field(default_factory=dict)
    gate_counts: dict[str, int] = Field(default_factory=dict)
    programs: list[ResearchProgramRecord] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ResearchProgramEvidenceLoopRequest(StrictBaseModel):
    program_id: UUID | None = None
    thesis_query: str | None = None
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    max_tasks: int = Field(default=5, ge=1, le=25)
    max_source_queries: int = Field(default=20, ge=0, le=100)
    max_sources_per_task: int = Field(default=4, ge=0, le=25)
    queue_briefs: bool = True
    create_research_leads: bool = True
    create_source_queries: bool = True
    priority: int = Field(default=40, ge=0, le=1000)
    max_chunks_per_perspective: int = Field(default=10, ge=1, le=25)
    max_claims: int = Field(default=20, ge=0, le=50)
    max_chunk_chars: int = Field(default=2200, ge=500, le=12000)
    brief_style: Literal["technical", "operator", "substack", "vet_partner"] = "technical"
    model_profile: str = "research_brief"
    review_mode: Literal[
        "external_required",
        "openrouter_required",
        "openrouter_compare",
        "deterministic_only",
    ] = "openrouter_required"
    review_models: list[str] = Field(default_factory=list, max_length=10)
    dry_run: bool = False
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_research_program_evidence_loop_request(self) -> "ResearchProgramEvidenceLoopRequest":
        if self.thesis_query:
            self.thesis_query = self.thesis_query.strip() or None
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        self.review_models = _dedupe_strings(self.review_models)
        self.model_profile = self.model_profile.strip() or "research_brief"
        return self


class ResearchProgramEvidenceLoopTaskResult(StrictBaseModel):
    task_id: UUID
    title: str
    status_before: ResearchProgramEvidenceTaskStatus
    status_after: ResearchProgramEvidenceTaskStatus
    research_lead_id: UUID | None = None
    brief_queue_item_id: UUID | None = None
    source_query_names: list[str] = Field(default_factory=list)
    selected_source_keys: list[str] = Field(default_factory=list)
    skipped_reason: str | None = None
    errors: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResearchProgramEvidenceLoopResult(StrictBaseModel):
    program_id: UUID | None = None
    program_title: str | None = None
    dry_run: bool = False
    blocked: bool = False
    loop_count_before: int = 0
    loop_count_after: int = 0
    max_evidence_loops: int = 0
    task_count: int = 0
    selected_task_count: int = 0
    research_lead_count: int = 0
    source_query_count: int = 0
    brief_queue_count: int = 0
    task_results: list[ResearchProgramEvidenceLoopTaskResult] = Field(default_factory=list)
    research_leads: list[ResearchLeadRecord] = Field(default_factory=list)
    source_queries: list[SourceQuery] = Field(default_factory=list)
    brief_queue_items: list[ResearchBriefQueueItem] = Field(default_factory=list)
    program: ResearchProgramRecord | None = None
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class OmicsAccessionHit(StrictBaseModel):
    source_key: str
    accession: str
    identifier_type: Literal["geo", "sra", "bioproject", "biosample"]
    research_object_id: UUID | None = None
    title: str | None = None
    canonical_url: str | None = None
    organism: str | None = None
    sample_count: int | None = None
    library_strategy: str | None = None
    bioproject: str | None = None
    pmid: str | None = None
    matched_terms: list[str] = Field(default_factory=list)
    source_query_name: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class OmicsAccessionHuntRequest(StrictBaseModel):
    topic_query: str = (
        "canine hemangiosarcoma human angiosarcoma VIM vimentin transcriptome RNA-seq expression"
    )
    program_id: UUID | None = None
    disease_terms: list[str] = Field(
        default_factory=lambda: [
            "canine hemangiosarcoma",
            "dog hemangiosarcoma",
            "human angiosarcoma",
            "angiosarcoma",
        ],
        max_length=25,
    )
    gene_symbols: list[str] = Field(default_factory=lambda: ["VIM", "vimentin"], max_length=25)
    source_keys: list[str] = Field(default_factory=lambda: ["geo", "sra"], max_length=5)
    query_texts: list[str] = Field(default_factory=list, max_length=25)
    limit_per_query: int = Field(default=5, ge=1, le=50)
    max_queries: int = Field(default=8, ge=1, le=50)
    persist_queries: bool = True
    dry_run: bool = False
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_omics_accession_hunt_request(self) -> "OmicsAccessionHuntRequest":
        self.topic_query = self.topic_query.strip()
        self.disease_terms = _dedupe_strings(self.disease_terms)
        self.gene_symbols = _dedupe_strings(self.gene_symbols)
        self.source_keys = _dedupe_lower_tokens(self.source_keys) or ["geo", "sra"]
        self.query_texts = _dedupe_strings(self.query_texts)
        return self


class OmicsAccessionHuntResult(StrictBaseModel):
    program_id: UUID | None = None
    dry_run: bool = False
    query_count: int = 0
    source_query_count: int = 0
    raw_records: int = 0
    research_objects: int = 0
    document_chunks: int = 0
    accession_hit_count: int = 0
    source_queries: list[SourceQuery] = Field(default_factory=list)
    ingestion_results: list[IngestionResult] = Field(default_factory=list)
    accession_hits: list[OmicsAccessionHit] = Field(default_factory=list)
    negative_queries: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class OmicsEvidenceDataset(StrictBaseModel):
    source_key: str
    accession: str
    identifier_type: Literal["geo", "sra", "bioproject", "biosample"]
    research_object_id: UUID | None = None
    title: str | None = None
    canonical_url: str | None = None
    organism: str | None = None
    disease_context: Literal["canine_hsa", "human_angiosarcoma", "comparative_context", "unknown"] = "unknown"
    evidence_role: Literal["direct", "analog", "context"] = "context"
    sample_count: int | None = None
    library_strategy: str | None = None
    bioproject: str | None = None
    pmid: str | None = None
    run_accessions: list[str] = Field(default_factory=list, max_length=250)
    sample_accessions: list[str] = Field(default_factory=list, max_length=500)
    platform_accessions: list[str] = Field(default_factory=list, max_length=100)
    supplementary_file_types: list[str] = Field(default_factory=list, max_length=50)
    matched_terms: list[str] = Field(default_factory=list, max_length=50)
    readout_hints: list[str] = Field(default_factory=list, max_length=50)
    limitations: list[str] = Field(default_factory=list, max_length=50)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_omics_evidence_dataset(self) -> "OmicsEvidenceDataset":
        self.source_key = self.source_key.strip().lower()
        self.accession = self.accession.strip()
        self.run_accessions = _dedupe_strings(self.run_accessions)
        self.sample_accessions = _dedupe_strings(self.sample_accessions)
        self.platform_accessions = _dedupe_strings(self.platform_accessions)
        self.supplementary_file_types = _dedupe_strings(self.supplementary_file_types)
        self.matched_terms = _dedupe_strings(self.matched_terms)
        self.readout_hints = _dedupe_strings(self.readout_hints)
        self.limitations = _dedupe_strings(self.limitations)
        return self


class OmicsEvidencePacket(StrictBaseModel):
    packet_id: str = Field(min_length=3, max_length=260)
    packet_key: str = Field(min_length=3, max_length=120)
    program_id: UUID | None = None
    title: str = Field(min_length=3, max_length=500)
    topic_query: str = Field(min_length=3, max_length=1000)
    disease_scope: str = Field(default="canine hemangiosarcoma and human angiosarcoma", max_length=500)
    target_terms: list[str] = Field(default_factory=list, max_length=50)
    disease_terms: list[str] = Field(default_factory=list, max_length=50)
    source_keys: list[str] = Field(default_factory=list, max_length=10)
    datasets: list[OmicsEvidenceDataset] = Field(default_factory=list, max_length=250)
    dataset_count: int = 0
    direct_dataset_count: int = 0
    analog_dataset_count: int = 0
    total_sample_count: int | None = None
    accessions: list[str] = Field(default_factory=list, max_length=500)
    bioprojects: list[str] = Field(default_factory=list, max_length=200)
    pmids: list[str] = Field(default_factory=list, max_length=200)
    decisive_questions: list[str] = Field(default_factory=list, max_length=25)
    proposed_readouts: list[str] = Field(default_factory=list, max_length=50)
    quality_gates: list[str] = Field(default_factory=list, max_length=50)
    dispatch_blockers: list[str] = Field(default_factory=list, max_length=50)
    negative_coverage: list[str] = Field(default_factory=list, max_length=50)
    next_actions: list[str] = Field(default_factory=list, max_length=50)
    readiness: Literal["ready_for_omics_review", "needs_matrix_retrieval", "needs_more_accessions"] = "needs_matrix_retrieval"
    score: float = Field(default=0.0, ge=0.0, le=1.0)
    summary: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def normalize_omics_evidence_packet(self) -> "OmicsEvidencePacket":
        self.packet_id = self.packet_id.strip()
        self.packet_key = self.packet_key.strip()
        self.title = self.title.strip()
        self.topic_query = self.topic_query.strip()
        self.target_terms = _dedupe_strings(self.target_terms)
        self.disease_terms = _dedupe_strings(self.disease_terms)
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        self.accessions = _dedupe_strings(self.accessions)
        self.bioprojects = _dedupe_strings(self.bioprojects)
        self.pmids = _dedupe_strings(self.pmids)
        self.decisive_questions = _dedupe_strings(self.decisive_questions)
        self.proposed_readouts = _dedupe_strings(self.proposed_readouts)
        self.quality_gates = _dedupe_strings(self.quality_gates)
        self.dispatch_blockers = _dedupe_strings(self.dispatch_blockers)
        self.negative_coverage = _dedupe_strings(self.negative_coverage)
        self.next_actions = _dedupe_strings(self.next_actions)
        return self


class OmicsEvidencePacketRequest(StrictBaseModel):
    program_id: UUID | None = None
    topic_query: str = (
        "canine hemangiosarcoma human angiosarcoma VIM vimentin transcriptome RNA-seq expression"
    )
    disease_terms: list[str] = Field(
        default_factory=lambda: [
            "canine hemangiosarcoma",
            "dog hemangiosarcoma",
            "human angiosarcoma",
            "angiosarcoma",
        ],
        max_length=25,
    )
    gene_symbols: list[str] = Field(default_factory=lambda: ["VIM", "vimentin"], max_length=25)
    source_keys: list[str] = Field(default_factory=lambda: ["geo", "sra"], max_length=5)
    accessions: list[str] = Field(default_factory=list, max_length=200)
    limit: int = Field(default=100, ge=1, le=1000)
    min_datasets_per_packet: int = Field(default=1, ge=1, le=50)
    include_context_packet: bool = True
    dry_run: bool = False
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_omics_evidence_packet_request(self) -> "OmicsEvidencePacketRequest":
        self.topic_query = self.topic_query.strip()
        self.disease_terms = _dedupe_strings(self.disease_terms)
        self.gene_symbols = _dedupe_strings(self.gene_symbols)
        self.source_keys = _dedupe_lower_tokens(self.source_keys) or ["geo", "sra"]
        self.accessions = _dedupe_strings(self.accessions)
        return self


class OmicsEvidencePacketResult(StrictBaseModel):
    program_id: UUID | None = None
    dry_run: bool = False
    scanned_dataset_count: int = 0
    selected_dataset_count: int = 0
    packet_count: int = 0
    direct_dataset_count: int = 0
    analog_dataset_count: int = 0
    context_dataset_count: int = 0
    packets: list[OmicsEvidencePacket] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ValidationAutopilotRequest(StrictBaseModel):
    enabled: bool = True
    dry_run: bool = True
    force: bool = False
    manual_grace_period_hours: float = Field(default=6.0, ge=0.0, le=168.0)
    minimum_queue_age_hours: float = Field(default=1.0, ge=0.0, le=168.0)
    max_per_run: int = Field(default=2, ge=1, le=10)
    hourly_budget_usd: float = Field(default=0.25, ge=0.0, le=100.0)
    daily_budget_usd: float = Field(default=1.50, ge=0.0, le=1000.0)
    estimated_cost_per_item_usd: float = Field(default=0.03, ge=0.0, le=10.0)
    allowed_task_types: list[ValidationPlanTaskType] = Field(
        default_factory=lambda: ["expert_review", "target_validation", "omics"],
        max_length=20,
    )
    allowed_validation_types: list[str] = Field(
        default_factory=lambda: ["expert_review", "homology", "omics"],
        max_length=20,
    )
    source_keys: list[str] = Field(default_factory=list, max_length=25)
    model_profile: str = "openrouter_required"
    approved_by: str = Field(default="validation_autopilot", max_length=200)
    approval_note: str | None = Field(default=None, max_length=1000)
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_validation_autopilot_request(self) -> "ValidationAutopilotRequest":
        self.allowed_task_types = _dedupe_strings(self.allowed_task_types)
        self.allowed_validation_types = _dedupe_lower_tokens(self.allowed_validation_types)
        self.source_keys = _dedupe_lower_tokens(self.source_keys)
        self.model_profile = self.model_profile.strip() or "openrouter_required"
        self.approved_by = self.approved_by.strip() or "validation_autopilot"
        if self.approval_note:
            self.approval_note = self.approval_note.strip() or None
        return self


class ValidationAutopilotQueueRecord(StrictBaseModel):
    queue_item_id: UUID
    plan_id: UUID
    task_id: UUID
    status: ValidationRequestQueueStatus
    priority: int = Field(ge=1, le=1000)
    task_type: ValidationPlanTaskType
    validation_type: str
    title: str
    source_key: str | None = None
    reason: str
    decision: str | None = None
    agent_run_id: UUID | None = None
    cost_usd: float | None = Field(default=None, ge=0.0)
    last_error: str | None = None


class ValidationAutopilotResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    agent_name: str = "validation_autopilot_agent"
    policy_version: str = "v1"
    enabled: bool = True
    dry_run: bool = True
    force: bool = False
    model_profile: str = "openrouter_required"
    scanned_count: int = 0
    eligible_count: int = 0
    selected_count: int = 0
    dispatched_count: int = 0
    skipped_count: int = 0
    should_dispatch: bool = False
    blockers: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    last_manual_activity_at: datetime | None = None
    manual_grace_period_ends_at: datetime | None = None
    hourly_budget_usd: float = Field(default=0.25, ge=0.0)
    daily_budget_usd: float = Field(default=1.50, ge=0.0)
    hourly_spend_usd: float = Field(default=0.0, ge=0.0)
    daily_spend_usd: float = Field(default=0.0, ge=0.0)
    estimated_cost_usd: float = Field(default=0.0, ge=0.0)
    actual_cost_usd: float = Field(default=0.0, ge=0.0)
    selected: list[ValidationAutopilotQueueRecord] = Field(default_factory=list)
    dispatched: list[ValidationAutopilotQueueRecord] = Field(default_factory=list)
    skipped: list[ValidationAutopilotQueueRecord] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


ValidationAgentDecision = Literal["promote", "hold", "demote"]


class ValidationAgentResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    queue_item_id: UUID
    plan_id: UUID
    task_id: UUID
    task_type: ValidationPlanTaskType
    validation_type: str
    agent_name: str
    model_profile: str
    decision: ValidationAgentDecision = "hold"
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    summary: str = Field(min_length=1, max_length=3000)
    evidence_used: list[str] = Field(default_factory=list, max_length=50)
    missing_evidence: list[str] = Field(default_factory=list, max_length=50)
    risks: list[str] = Field(default_factory=list, max_length=50)
    next_actions: list[str] = Field(default_factory=list, max_length=50)
    errors: list[str] = Field(default_factory=list)
    raw_response: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def normalize_validation_agent_result(self) -> "ValidationAgentResult":
        self.summary = self.summary.strip()
        self.evidence_used = _normalized_unique_strings(self.evidence_used)
        self.missing_evidence = _normalized_unique_strings(self.missing_evidence)
        self.risks = _normalized_unique_strings(self.risks)
        self.next_actions = _normalized_unique_strings(self.next_actions)
        self.errors = _normalized_unique_strings(self.errors)
        return self


class EvidenceGapResolverRequest(StrictBaseModel):
    queue_item_ids: list[UUID] = Field(default_factory=list, max_length=100)
    plan_id: UUID | None = None
    statuses: list[ValidationRequestQueueStatus] = Field(default_factory=lambda: ["completed"], max_length=10)
    decisions: list[ValidationAgentDecision] = Field(default_factory=lambda: ["hold", "demote"], max_length=3)
    task_types: list[ValidationPlanTaskType] = Field(default_factory=list, max_length=20)
    gap_types: list[EvidenceGapType] = Field(
        default_factory=lambda: ["missing_evidence", "risk", "next_action"],
        max_length=3,
    )
    limit: int = Field(default=25, ge=1, le=200)
    max_gaps_per_item: int = Field(default=8, ge=1, le=50)
    priority: int = Field(default=30, ge=0, le=1000)
    dry_run: bool = True
    queue_research_briefs: bool = False
    dagster_run_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_evidence_gap_resolver_request(self) -> "EvidenceGapResolverRequest":
        self.statuses = _dedupe_strings(self.statuses)
        self.decisions = _dedupe_strings(self.decisions)
        self.task_types = _dedupe_strings(self.task_types)
        self.gap_types = _dedupe_strings(self.gap_types)
        return self


class EvidenceGapResolverResult(StrictBaseModel):
    agent_run_id: UUID | None = None
    agent_name: str = "evidence_gap_resolver_agent"
    model_profile: str = "deterministic_resolver"
    queue_items_seen: int = 0
    gap_count: int = 0
    leads_created: int = 0
    existing_leads: int = 0
    brief_queue_count: int = 0
    skipped_count: int = 0
    dry_run: bool = True
    research_leads: list[ResearchLeadRecord] = Field(default_factory=list)
    brief_queue_items: list[ResearchBriefQueueItem] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class TherapyCommitteeValidationQueueResult(StrictBaseModel):
    origin_agent_run_id: UUID | None = None
    committee_run_id: UUID | None = None
    plan_id: UUID = Field(default_factory=uuid4)
    candidate_idea_count: int = 0
    candidate_task_count: int = 0
    queued_count: int = 0
    existing_count: int = 0
    skipped_count: int = 0
    dry_run: bool = True
    queue_items: list[ValidationRequestQueueItem] = Field(default_factory=list)
    skipped: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class MCPToolCallLog(StrictBaseModel):
    tool_call_id: UUID = Field(default_factory=uuid4)
    tool_name: str
    mode: ToolMode
    input_hash: str | None = None
    sanitized_input: dict[str, Any] = Field(default_factory=dict)
    output_summary: str | None = None
    structured_output: dict[str, Any] = Field(default_factory=dict)
    status: RunStatus = RunStatus.COMPLETED
    latency_ms: int | None = None
    error_message: str | None = None
    created_by: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class ModelProfile(StrictBaseModel):
    profile_key: str
    provider: Literal["claude", "openai", "local", "other"]
    model_name: str | None = None
    purpose: str
    temperature: float | None = Field(default=None, ge=0.0, le=2.0)
    max_tokens: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
