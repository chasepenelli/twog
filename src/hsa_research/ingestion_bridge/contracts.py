"""Typed contracts for Ingestion Bridge v2.

These models are the shared contract between MCP tools, Dagster assets,
service functions, and future UI/API surfaces.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


class StrictBaseModel(BaseModel):
    """Base model that keeps contracts explicit but allows future metadata."""

    model_config = ConfigDict(extra="forbid", use_enum_values=True)


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
    status: RunStatus = RunStatus.COMPLETED
    errors: list[str] = Field(default_factory=list)


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


class ValidationRequest(StrictBaseModel):
    validation_type: Literal["boltz", "docking", "md", "admet", "homology", "safety", "expert_review"]
    candidate_id: UUID | None = None
    candidate_name: str | None = None
    target_name: str | None = None
    objective: str
    priority: int = Field(default=100, ge=1, le=1000)
    require_approval: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


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
