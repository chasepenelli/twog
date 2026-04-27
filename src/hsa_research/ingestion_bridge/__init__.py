"""Ingestion Bridge v2 contracts and service adapters."""

from .contracts import (
    ArtifactHandle,
    AsyncRunHandle,
    BackfillResult,
    CandidateDossier,
    ClaimCurationItem,
    ClaimCurationRequest,
    ClaimCurationResult,
    ClaimExtractionResult,
    ClaimSearchResult,
    ClaimSearchResults,
    CommitHypothesisRequest,
    DocumentChunk,
    HypothesisDraft,
    HypothesisProposalRequest,
    IngestionResult,
    ResearchSource,
    SearchClaimsRequest,
    SourceQuery,
    SourceRecommendation,
    SourceScoutRequest,
    SourceScoutResult,
    ValidationRequest,
)
from .claim_curator import ClaimCuratorAgent, curate_claims_for_repository
from .harvesters_v2 import (
    CrossrefHarvesterV2,
    EuropePMCHarvesterV2,
    OpenAlexHarvesterV2,
    PubMedHarvesterV2,
    get_harvester,
)
from .local_ingest import LocalIngestionPipeline
from .claim_extractor import LocalRuleClaimExtractor, extract_claims_for_repository
from .query_policy import build_scholarly_source_queries, comparative_required_query
from .service import HSAResearchService, get_service
from .local_store import SQLiteResearchRepository
from .postgres_store import PostgresResearchRepository
from .source_scout import SourceScoutAgent, scout_sources_for_repository
from .storage import build_research_repository, build_sql_repository

__all__ = [
    "ArtifactHandle",
    "AsyncRunHandle",
    "BackfillResult",
    "CandidateDossier",
    "ClaimCurationItem",
    "ClaimCurationRequest",
    "ClaimCurationResult",
    "ClaimExtractionResult",
    "ClaimSearchResult",
    "ClaimSearchResults",
    "CommitHypothesisRequest",
    "DocumentChunk",
    "HypothesisDraft",
    "HypothesisProposalRequest",
    "HSAResearchService",
    "IngestionResult",
    "ClaimCuratorAgent",
    "CrossrefHarvesterV2",
    "EuropePMCHarvesterV2",
    "LocalRuleClaimExtractor",
    "LocalIngestionPipeline",
    "OpenAlexHarvesterV2",
    "PubMedHarvesterV2",
    "PostgresResearchRepository",
    "ResearchSource",
    "SearchClaimsRequest",
    "SQLiteResearchRepository",
    "SourceQuery",
    "SourceRecommendation",
    "SourceScoutAgent",
    "SourceScoutRequest",
    "SourceScoutResult",
    "ValidationRequest",
    "build_scholarly_source_queries",
    "build_research_repository",
    "build_sql_repository",
    "comparative_required_query",
    "curate_claims_for_repository",
    "extract_claims_for_repository",
    "get_harvester",
    "get_service",
    "scout_sources_for_repository",
]
