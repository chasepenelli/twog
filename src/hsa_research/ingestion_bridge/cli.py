"""Command-line entrypoint for local-first Ingestion Bridge v2."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import sys
import time
from pathlib import Path
from typing import Any
from uuid import UUID

from .backfill import backfill_deep_dives, backfill_papers_json
from .candidate_contribution_intake import (
    TRIAGE_ACTIONS,
    build_candidate_contribution_intake_report,
    triage_candidate_contributions,
)
from .claim_curator import curate_claims_for_repository
from .claim_extractor import extract_claims_for_repository
from .contracts import (
    AgentFindingEscalationRequest,
    AgentPerformanceEvaluationRequest,
    AgentPerformanceReportRequest,
    ClaimCurationRequest,
    CommandCenterRequest,
    DoiOpenAccessFollowupQueueRequest,
    EvidenceRefRepairRequest,
    EvidenceGapResolverRequest,
    EntityResolutionRequest,
    FullTextTriageRequest,
    FullTextOpsRequest,
    HypothesisPromotionReportRequest,
    MDExpertAgentReviewRequest,
    OmicsAccessionHuntRequest,
    OmicsEvidencePacketRequest,
    OmicsFollowupRequest,
    OmicsLocusSignalRequest,
    OmicsReadoutRequest,
    PubMedIdentifierRepairRequest,
    PublicCandidateGenerateRequest,
    PublicCandidateIntegrityReportRequest,
    PublicCandidateLibraryRequest,
    ResearchBriefEvaluationRequest,
    ResearchBriefFollowupQueueRequest,
    ResearchBriefOperatorDocRequest,
    ResearchBriefQueueBatchRequest,
    ResearchBriefQueueMaintenanceRequest,
    ResearchBriefQueueRequest,
    ResearchBriefQueueRunRequest,
    ResearchBriefRequest,
    ResearchFollowupRefinementRequest,
    ResearchFollowupResolverRequest,
    ResearchFollowupLoopRequest,
    ResearchHuntTaskRunRequest,
    ResearchHuntQueueReportRequest,
    ResearchHuntQueueMaintenanceRequest,
    ResearchHuntSynthesisDocRequest,
    ResearchHuntSynthesisQueueRequest,
    ResearchLeadCollectRequest,
    ResearchProgramBoardRequest,
    ResearchProgramEvidenceLoopRequest,
    ResearchProgramReviewRequest,
    RewardEventSyncRequest,
    RewardReportRequest,
    RetrievalSmokeRequest,
    ScrapeFetchRequest,
    ScrapeIngestRequest,
    ScrapeManifestFetchRequest,
    ScrapeManifestRequest,
    ScrapeProfileReviewRequest,
    ScrapeReviewRequest,
    SourceFollowupIngestRequest,
    SourceFollowupQueueRequest,
    SourceQuery,
    SourceScoutRequest,
    TherapyCommitteeRequest,
    TherapyCommitteeValidationQueueRequest,
    TherapyIdeaLibraryRequest,
    ValidationAutopilotRequest,
    ValidationGapSourceIngestRequest,
    ValidationGapSourcePackRequest,
    ValidationDecisionReportRequest,
    ValidationPacketRequest,
    ValidationPlanRequest,
    ValidationRequestQueueRequest,
    ValidationToolCatalogRequest,
    ValidationToolMatchRequest,
    XLinkedArticleReviewRequest,
    XLinkedArticleFollowupRequest,
    XTopicReviewRequest,
)
from .embedding_bakeoff import DEFAULT_EMBEDDING_BAKEOFF_MODELS, run_embedding_bakeoff
from .embeddings import (
    default_embedding_model_for_environment,
    index_embeddings_for_repository,
    maintain_embedding_index,
)
from .entity_resolution import resolve_entities_for_repository
from .local_ingest import LocalIngestionPipeline
from .local_store import SQLiteResearchRepository
from .scraper_bridge import ScrapeBridge, list_scrape_profiles
from .service import HSAResearchService
from .source_scout import scout_sources_for_repository
from .source_health import build_source_health_report
from .storage import build_sql_repository
from .source_sets import ALL_API_SOURCE_KEYS, STRUCTURED_SOURCE_KEYS
from .structured_orchestration import (
    build_structured_source_count_report,
    run_structured_sources_pipeline,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="HSA Ingestion Bridge v2 local runner")
    parser.add_argument("--db", type=Path, default=None, help="SQLite DB path")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", help="Seed source registry and starter queries")
    subparsers.add_parser("coverage", help="Show local ingestion coverage")

    ingest = subparsers.add_parser("ingest", help="Run a single source query")
    ingest.add_argument("--source", required=True, help="Source key, e.g. openalex")
    ingest.add_argument("--query", required=True, help="Source query text")
    ingest.add_argument("--query-name", default="manual", help="Stable query name")
    ingest.add_argument("--limit", type=int, default=25, help="Maximum records to fetch")
    ingest.add_argument("--track", default="cross_cutting", help="Research track")

    ingest_source = subparsers.add_parser("ingest-source", help="Run all active queries for one source")
    ingest_source.add_argument("--source", required=True, help="Source key, e.g. openalex")
    ingest_source.add_argument("--limit", type=int, default=25, help="Maximum records per query")

    structured_pipeline = subparsers.add_parser(
        "structured-pipeline",
        help="Run structured source refresh, extraction, curation, and QA",
    )
    structured_pipeline.add_argument(
        "--source",
        action="append",
        default=[],
        help="Structured source key; repeat to run multiple. Defaults to all structured sources.",
    )
    structured_pipeline.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Override maximum records per active query for every selected source",
    )
    structured_pipeline.add_argument("--extract-limit", type=int, default=500, help="Maximum chunks to extract per source")
    structured_pipeline.add_argument("--curate-limit", type=int, default=1000, help="Maximum claims to curate per source")
    structured_pipeline.add_argument(
        "--promote-threshold",
        type=float,
        default=0.5,
        help="Curation score threshold for promotion",
    )
    structured_pipeline.add_argument("--no-init", action="store_true", help="Skip seeding source registry and queries")

    structured_report = subparsers.add_parser(
        "structured-report",
        help="Report persisted structured-source counts without harvesting",
    )
    structured_report.add_argument(
        "--source",
        action="append",
        default=[],
        help="Structured source key; repeat to report multiple. Defaults to all structured sources.",
    )
    structured_report.add_argument(
        "--sample-limit",
        type=int,
        default=5,
        help="Maximum sample claims per source",
    )
    structured_report.add_argument(
        "--no-require-claims",
        action="store_true",
        help="Require records, objects, and chunks, but do not require claims per source",
    )
    structured_report.add_argument(
        "--fail-on-failed-sources",
        action="store_true",
        help="Exit non-zero if the report has failed sources",
    )

    source_health = subparsers.add_parser(
        "source-health",
        help="Report persisted source health, risks, and recommended actions",
    )
    source_health.add_argument(
        "--source",
        action="append",
        default=[],
        help="Source key; repeat to report multiple. Defaults to all hosted API sources.",
    )
    source_health.add_argument("--sample-limit", type=int, default=5, help="Maximum sample claims per source")
    source_health.add_argument(
        "--metadata-claim-limit",
        type=int,
        default=500,
        help="Maximum claims per source to inspect for metadata status summaries",
    )
    source_health.add_argument(
        "--min-health-score",
        type=float,
        default=0.65,
        help="Minimum health score for the hard source-health bar",
    )
    source_health.add_argument(
        "--no-require-claims",
        action="store_true",
        help="Require records, objects, and chunks, but do not require claims per source",
    )
    source_health.add_argument(
        "--fail-on-failed-sources",
        action="store_true",
        help="Exit non-zero if the report has failed sources",
    )

    command_center = subparsers.add_parser(
        "command-center",
        help="Build the read-only TWOG command-center report",
    )
    command_center.add_argument(
        "--source",
        action="append",
        default=[],
        help="Optional source key for source-health scope; repeatable",
    )
    command_center.add_argument(
        "--no-source-health",
        action="store_true",
        help="Skip source health in the report",
    )
    command_center.add_argument(
        "--no-recent-agents",
        action="store_true",
        help="Skip recent agent runs in the report",
    )
    command_center.add_argument("--queue-limit", type=int, default=25, help="Maximum queue items to show")
    command_center.add_argument("--lead-limit", type=int, default=25, help="Maximum research leads to show")
    command_center.add_argument("--agent-run-limit", type=int, default=25, help="Maximum recent agent runs to show")
    command_center.add_argument(
        "--min-health-score",
        type=float,
        default=0.65,
        help="Minimum health score when source health is included",
    )
    command_center.add_argument(
        "--no-require-claims",
        action="store_true",
        help="Do not require claims for source-health scoring",
    )

    command_center_web = subparsers.add_parser(
        "command-center-web",
        help="Run the local TWOG command-center web UI",
    )
    command_center_web.add_argument("--host", default="127.0.0.1", help="Bind host")
    command_center_web.add_argument("--port", type=int, default=8787, help="Bind port")

    triage_full_text = subparsers.add_parser(
        "triage-full-text",
        help="Classify a full-text ingestion edge case into a bounded operational action",
    )
    triage_full_text.add_argument("--source", required=True, help="Source key, e.g. europe_pmc")
    triage_full_text.add_argument(
        "--stage",
        default="qa",
        choices=[
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
        ],
        help="Pipeline stage where the issue surfaced",
    )
    triage_full_text.add_argument("--query-name", default=None, help="Optional source query name")
    triage_full_text.add_argument("--error", action="append", default=[], help="Error text; repeatable")
    triage_full_text.add_argument("--runtime-seconds", type=float, default=None, help="Observed runtime")
    triage_full_text.add_argument("--timeout-seconds", type=float, default=None, help="Runtime cap")
    triage_full_text.add_argument("--raw-records", type=int, default=0)
    triage_full_text.add_argument("--research-objects", type=int, default=0)
    triage_full_text.add_argument("--document-chunks", type=int, default=0)
    triage_full_text.add_argument("--full-text-document-chunks", type=int, default=0)
    triage_full_text.add_argument("--full-text-body-chars", type=int, default=0)
    triage_full_text.add_argument("--claims", type=int, default=0)
    triage_full_text.add_argument("--entity-mentions", type=int, default=0)
    triage_full_text.add_argument("--current-failed-run", action="append", default=[])
    triage_full_text.add_argument("--http-status", type=int, default=None)
    triage_full_text.add_argument("--model-profile", default="cheap_classifier")
    triage_full_text.add_argument("--fail-on-blocking", action="store_true")

    full_text_ops = subparsers.add_parser(
        "full-text-ops",
        help="Run the recommend-only full-text ops agent",
    )
    full_text_ops.add_argument(
        "--source",
        action="append",
        default=[],
        help="Full-text source key; repeat to inspect multiple. Defaults to Europe PMC and PMC OA.",
    )
    full_text_ops.add_argument("--partition-date", default=None, help="Optional YYYY-MM-DD partition to validate")
    full_text_ops.add_argument("--recent-run-limit", type=int, default=10, help="Recent agent runs to include")
    full_text_ops.add_argument("--model-profile", default="reviewer", help="Logical model profile")
    full_text_ops.add_argument(
        "--review-mode",
        choices=("external_required", "openrouter_required", "openrouter_compare", "deterministic_only"),
        default="openrouter_required",
        help="Whether review is external, OpenRouter-backed, comparative, or deterministic only",
    )
    full_text_ops.add_argument(
        "--review-model",
        action="append",
        default=[],
        help="OpenRouter model id; repeat to compare multiple models",
    )
    full_text_ops.add_argument("--fail-on-blocking", action="store_true")

    research_brief = subparsers.add_parser(
        "research-brief",
        help="Run citation-first evidence, translational, skeptic, and synthesis agents",
    )
    research_brief.add_argument("--topic", required=True, help="Research question or topic to brief")
    research_brief.add_argument(
        "--disease-scope",
        default="canine hemangiosarcoma and human angiosarcoma",
        help="Disease/scope guardrail for retrieval and synthesis",
    )
    research_brief.add_argument("--source", default=None, help="Optional source key filter")
    research_brief.add_argument("--max-chunks", type=int, default=8, help="Chunks per perspective search")
    research_brief.add_argument("--max-claims", type=int, default=12, help="Claims to include in the evidence payload")
    research_brief.add_argument("--max-chunk-chars", type=int, default=1800, help="Maximum chars per cited chunk")
    research_brief.add_argument(
        "--brief-style",
        choices=("technical", "operator", "substack", "vet_partner"),
        default="technical",
    )
    research_brief.add_argument("--model-profile", default="research_brief", help="Logical model profile")
    research_brief.add_argument(
        "--review-mode",
        choices=("external_required", "openrouter_required", "openrouter_compare", "deterministic_only"),
        default="openrouter_required",
    )
    research_brief.add_argument(
        "--review-model",
        action="append",
        default=[],
        help="OpenRouter model id; repeat to compare multiple models",
    )

    research_briefs = subparsers.add_parser(
        "research-briefs",
        help="List or fetch persisted research briefs",
    )
    research_briefs.add_argument("--id", default=None, help="Optional brief_id to fetch")
    research_briefs.add_argument("--status", default=None, help="Brief status filter")
    research_briefs.add_argument("--source", default=None, help="Optional source key filter")
    research_briefs.add_argument("--topic-query", default=None, help="Case-insensitive topic/scope filter")
    research_briefs.add_argument("--limit", type=int, default=50, help="Maximum briefs to return")

    research_brief_operator_doc = subparsers.add_parser(
        "research-brief-operator-doc",
        help="Create plain-language operator docs from completed research briefs",
    )
    research_brief_operator_doc.add_argument("--brief-id", action="append", default=[], help="Specific brief ID")
    research_brief_operator_doc.add_argument("--status", default="completed", help="Brief status filter")
    research_brief_operator_doc.add_argument("--source", default=None, help="Optional source key filter")
    research_brief_operator_doc.add_argument("--topic-query", default=None, help="Case-insensitive topic/scope filter")
    research_brief_operator_doc.add_argument("--limit", type=int, default=10, help="Maximum docs to create")
    research_brief_operator_doc.add_argument("--max-hypotheses", type=int, default=5)
    research_brief_operator_doc.add_argument("--max-unresolved-questions", type=int, default=8)
    research_brief_operator_doc.add_argument("--max-evidence-limitations", type=int, default=8)
    research_brief_operator_doc.add_argument("--max-technical-footnotes", type=int, default=30)
    research_brief_operator_doc.add_argument("--no-technical-footnotes", action="store_true")
    research_brief_operator_doc.add_argument("--apply", action="store_true", help="Persist artifact records")
    research_brief_operator_doc.add_argument("--operator", default="cli_operator")

    queue_research_brief = subparsers.add_parser(
        "queue-research-brief",
        help="Queue a research brief request for later runner execution",
    )
    queue_research_brief.add_argument("--topic", required=True, help="Research question or topic to brief")
    queue_research_brief.add_argument(
        "--disease-scope",
        default="canine hemangiosarcoma and human angiosarcoma",
        help="Disease/scope guardrail for retrieval and synthesis",
    )
    queue_research_brief.add_argument("--source", default=None, help="Optional source key filter")
    queue_research_brief.add_argument("--priority", type=int, default=100, help="Lower values run first")
    queue_research_brief.add_argument("--max-chunks", type=int, default=8, help="Chunks per perspective search")
    queue_research_brief.add_argument("--max-claims", type=int, default=12, help="Claims to include")
    queue_research_brief.add_argument("--max-chunk-chars", type=int, default=1800, help="Maximum chars per cited chunk")
    queue_research_brief.add_argument(
        "--brief-style",
        choices=("technical", "operator", "substack", "vet_partner"),
        default="technical",
    )
    queue_research_brief.add_argument("--model-profile", default="research_brief", help="Logical model profile")
    queue_research_brief.add_argument(
        "--review-mode",
        choices=("external_required", "openrouter_required", "openrouter_compare", "deterministic_only"),
        default="openrouter_required",
    )
    queue_research_brief.add_argument(
        "--review-model",
        action="append",
        default=[],
        help="OpenRouter model id; repeat to compare multiple models",
    )

    queue_research_brief_batch = subparsers.add_parser(
        "queue-research-brief-batch",
        help="Queue research briefs from watchlist leads and source-health gaps",
    )
    queue_research_brief_batch.add_argument(
        "--mode",
        choices=("research_leads", "source_health", "both"),
        default="both",
        help="Batch source to queue from",
    )
    queue_research_brief_batch.add_argument(
        "--lead-status",
        action="append",
        default=[],
        help="Research lead status to queue; defaults to new and watching",
    )
    queue_research_brief_batch.add_argument(
        "--lead-type",
        action="append",
        default=[],
        help="Optional research lead type filter; repeatable",
    )
    queue_research_brief_batch.add_argument(
        "--source",
        action="append",
        default=[],
        help="Optional source key filter; repeatable",
    )
    queue_research_brief_batch.add_argument(
        "--source-health-status",
        action="append",
        default=[],
        help="Source health status to queue; defaults to failing, triage, and watch",
    )
    queue_research_brief_batch.add_argument(
        "--include-empty-sources",
        action="store_true",
        help="Queue source-health gaps even when no chunks exist yet",
    )
    queue_research_brief_batch.add_argument("--limit", type=int, default=25, help="Maximum queue items to create")
    queue_research_brief_batch.add_argument(
        "--disease-scope",
        default="canine hemangiosarcoma and human angiosarcoma",
        help="Disease/scope guardrail for retrieval and synthesis",
    )
    queue_research_brief_batch.add_argument("--priority", type=int, default=80, help="Default queue priority")
    queue_research_brief_batch.add_argument("--max-chunks", type=int, default=8, help="Chunks per perspective search")
    queue_research_brief_batch.add_argument("--max-claims", type=int, default=12, help="Claims to include")
    queue_research_brief_batch.add_argument(
        "--max-chunk-chars",
        type=int,
        default=1800,
        help="Maximum chars per cited chunk",
    )
    queue_research_brief_batch.add_argument(
        "--brief-style",
        choices=("technical", "operator", "substack", "vet_partner"),
        default="technical",
    )
    queue_research_brief_batch.add_argument("--model-profile", default="research_brief", help="Logical model profile")
    queue_research_brief_batch.add_argument(
        "--review-mode",
        choices=("external_required", "openrouter_required", "openrouter_compare", "deterministic_only"),
        default="openrouter_required",
    )
    queue_research_brief_batch.add_argument(
        "--review-model",
        action="append",
        default=[],
        help="OpenRouter model id; repeat to compare multiple models",
    )

    research_brief_queue = subparsers.add_parser(
        "research-brief-queue",
        help="List or fetch queued research brief requests",
    )
    research_brief_queue.add_argument("--id", default=None, help="Optional queue_item_id to fetch")
    research_brief_queue.add_argument("--status", default=None, help="Queue status filter")
    research_brief_queue.add_argument("--source", default=None, help="Optional source key filter")
    research_brief_queue.add_argument("--topic-query", default=None, help="Case-insensitive topic/scope filter")
    research_brief_queue.add_argument("--limit", type=int, default=50, help="Maximum queue items to return")

    requeue_research_brief_queue = subparsers.add_parser(
        "requeue-research-brief-queue-item",
        help="Move a failed research brief queue item back to queued",
    )
    requeue_research_brief_queue.add_argument("--id", required=True, help="queue_item_id to requeue")
    requeue_research_brief_queue.add_argument(
        "--priority",
        type=int,
        default=None,
        help="Optional replacement priority; lower values run first",
    )

    archive_research_brief_queue = subparsers.add_parser(
        "archive-research-brief-queue-item",
        help="Archive a completed research brief queue item",
    )
    archive_research_brief_queue.add_argument("--id", required=True, help="queue_item_id to archive")

    maintain_research_brief_queue = subparsers.add_parser(
        "maintain-research-brief-queue",
        help="Dry-run or apply safe research brief queue maintenance",
    )
    maintain_research_brief_queue.add_argument(
        "--id",
        action="append",
        default=[],
        help="Specific queue_item_id to inspect; repeat for multiple items",
    )
    maintain_research_brief_queue.add_argument(
        "--status",
        action="append",
        default=[],
        help="Eligible status; defaults to failed. Only failed/completed are supported.",
    )
    maintain_research_brief_queue.add_argument("--source", default=None, help="Optional source key filter")
    maintain_research_brief_queue.add_argument(
        "--topic-query",
        default=None,
        help="Case-insensitive topic/scope filter",
    )
    maintain_research_brief_queue.add_argument(
        "--min-attempts",
        type=int,
        default=1,
        help="Minimum attempts before a queue item is eligible",
    )
    maintain_research_brief_queue.add_argument(
        "--max-updated-age-hours",
        type=float,
        default=12.0,
        help="Only include items whose updated_at is at least this many hours old",
    )
    maintain_research_brief_queue.add_argument("--limit", type=int, default=50, help="Maximum items to archive")
    maintain_research_brief_queue.add_argument(
        "--reason",
        default="stale_research_brief_queue_cleanup",
        help="Reason recorded in queue metadata",
    )
    maintain_research_brief_queue.add_argument(
        "--apply",
        action="store_true",
        help="Actually archive matching items; without this flag the command is a dry run",
    )

    run_research_brief_queue = subparsers.add_parser(
        "run-research-brief-queue",
        help="Run the next queued research brief request",
    )
    run_research_brief_queue.add_argument(
        "--id",
        action="append",
        default=[],
        help="Specific queue_item_id to run; repeat for multiple items",
    )
    run_research_brief_queue.add_argument(
        "--status",
        action="append",
        default=[],
        help="Queue status to pull from; defaults to queued",
    )
    run_research_brief_queue.add_argument("--source", default=None, help="Optional source key filter")
    run_research_brief_queue.add_argument("--topic-query", default=None, help="Case-insensitive topic/scope filter")
    run_research_brief_queue.add_argument("--limit", type=int, default=1, help="Candidate queue items to inspect")

    candidate_contribution_intake = subparsers.add_parser(
        "candidate-contribution-intake",
        help="List public candidate contribution intake rows",
    )
    candidate_contribution_intake.add_argument(
        "--status",
        action="append",
        default=[],
        help="Intake status filter; repeatable. Defaults to pending statuses.",
    )
    candidate_contribution_intake.add_argument(
        "--candidate-id",
        action="append",
        default=[],
        help="Candidate ID filter; repeatable.",
    )
    candidate_contribution_intake.add_argument("--limit", type=int, default=50, help="Maximum rows to return")
    candidate_contribution_intake.add_argument(
        "--include-packet",
        action="store_true",
        help="Include submitted packet JSON in output.",
    )

    triage_candidate_contribution = subparsers.add_parser(
        "triage-candidate-contribution",
        help="Preview or apply an operator triage decision for public candidate contributions",
    )
    triage_candidate_contribution.add_argument(
        "--id",
        action="append",
        required=True,
        help="Contribution ID to triage; repeatable.",
    )
    triage_candidate_contribution.add_argument("--action", required=True, choices=TRIAGE_ACTIONS)
    triage_candidate_contribution.add_argument("--operator", default="cli", help="Operator identity for audit notes")
    triage_candidate_contribution.add_argument("--review-notes", default="", help="Operator review note")
    triage_candidate_contribution.add_argument(
        "--apply",
        action="store_true",
        help="Persist the triage decision. Without this flag the command is a dry run.",
    )

    research_brief_playground = subparsers.add_parser(
        "research-brief-playground-pack",
        help="Export playground-ready prompts for the research brief perspective agents",
    )
    research_brief_playground.add_argument("--topic", required=True, help="Research question or topic to brief")
    research_brief_playground.add_argument(
        "--disease-scope",
        default="canine hemangiosarcoma and human angiosarcoma",
        help="Disease/scope guardrail for retrieval and synthesis",
    )
    research_brief_playground.add_argument("--source", default=None, help="Optional source key filter")
    research_brief_playground.add_argument("--max-chunks", type=int, default=8, help="Chunks per perspective search")
    research_brief_playground.add_argument("--max-claims", type=int, default=12, help="Claims to include in the evidence payload")
    research_brief_playground.add_argument("--max-chunk-chars", type=int, default=1800, help="Maximum chars per cited chunk")
    research_brief_playground.add_argument(
        "--brief-style",
        choices=("technical", "operator", "substack", "vet_partner"),
        default="technical",
    )
    research_brief_playground.add_argument("--model-profile", default="research_brief", help="Logical model profile")

    therapy_committee = subparsers.add_parser(
        "therapy-committee",
        help="Run the cited therapy ideation committee",
    )
    therapy_committee.add_argument(
        "--topic",
        default="curative or disease-modifying therapy ideas for canine hemangiosarcoma",
        help="Therapy question or ideation topic",
    )
    therapy_committee.add_argument(
        "--disease-scope",
        default="canine hemangiosarcoma and human angiosarcoma",
        help="Disease/scope guardrail for retrieval and synthesis",
    )
    therapy_committee.add_argument("--source", default=None, help="Optional source key filter")
    therapy_committee.add_argument("--program-id", default=None, help="Run committee from a persisted research program")
    therapy_committee.add_argument("--max-chunks", type=int, default=10, help="Chunks per perspective search")
    therapy_committee.add_argument("--max-claims", type=int, default=20, help="Claims to include")
    therapy_committee.add_argument("--max-chunk-chars", type=int, default=2200, help="Maximum chars per cited chunk")
    therapy_committee.add_argument(
        "--max-ideas",
        type=int,
        default=None,
        help="Ideas per committee perspective; when explicit, also caps final ranked ideas unless --max-ranked-ideas is set",
    )
    therapy_committee.add_argument(
        "--max-ranked-ideas",
        type=int,
        default=None,
        help="Maximum final deduped ideas to persist from the committee run",
    )
    therapy_committee.add_argument("--model-profile", default="therapy_committee", help="Logical model profile")
    therapy_committee.add_argument(
        "--review-mode",
        choices=("external_required", "openrouter_required", "openrouter_compare", "deterministic_only"),
        default="openrouter_required",
    )
    therapy_committee.add_argument(
        "--review-model",
        action="append",
        default=[],
        help="OpenRouter model id; repeat to compare multiple models",
    )
    therapy_committee.add_argument("--brief-id", default=None, help="Run committee from a persisted evaluated brief")
    therapy_committee.add_argument("--evaluation-id", default=None, help="Run committee from a persisted brief evaluation")

    validation_tool_catalog = subparsers.add_parser(
        "validation-tool-catalog",
        help="List the in-repo recommend-only validation tool catalog",
    )
    validation_tool_catalog.add_argument("--category", default=None, help="Optional tool category")
    validation_tool_catalog.add_argument("--runner-status", default=None, help="Optional runner status")
    validation_tool_catalog.add_argument("--validation-type", default=None, help="Optional validation type filter")
    validation_tool_catalog.add_argument("--task-type", default=None, help="Optional validation plan task type filter")
    validation_tool_catalog.add_argument("--query", default=None, help="Text search over catalog descriptions")
    validation_tool_catalog.add_argument("--limit", type=int, default=50, help="Maximum tools to return")

    match_validation_tool = subparsers.add_parser(
        "match-validation-tool",
        help="Match validation tool catalog entries to a task context",
    )
    match_validation_tool.add_argument("--validation-type", default=None, help="Validation request type")
    match_validation_tool.add_argument("--task-type", default=None, help="Validation plan task type")
    match_validation_tool.add_argument("--objective", default=None, help="Task objective text")
    match_validation_tool.add_argument("--candidate-name", default=None, help="Candidate therapy/compound")
    match_validation_tool.add_argument("--target-name", default=None, help="Target or biomarker")
    match_validation_tool.add_argument("--species", action="append", default=[], help="Species context; repeatable")
    match_validation_tool.add_argument("--required-input", action="append", default=[], help="Available input; repeatable")
    match_validation_tool.add_argument("--runner-status", default="recommend_only", help="Runner status filter")
    match_validation_tool.add_argument("--limit", type=int, default=5, help="Maximum matches to return")

    therapy_ideas = subparsers.add_parser(
        "therapy-idea-library",
        help="List persisted therapy ideas",
    )
    therapy_ideas.add_argument("--id", default=None, help="Therapy idea ID")
    therapy_ideas.add_argument("--status", default=None, help="Therapy idea status")
    therapy_ideas.add_argument("--program-id", default=None, help="Source research program ID")
    therapy_ideas.add_argument("--brief-id", default=None, help="Source brief ID")
    therapy_ideas.add_argument("--evaluation-id", default=None, help="Source evaluation ID")
    therapy_ideas.add_argument("--committee-run-id", default=None, help="Committee run ID")
    therapy_ideas.add_argument("--query", default=None, help="Topic/title/hypothesis search")
    therapy_ideas.add_argument("--limit", type=int, default=50, help="Maximum ideas to return")

    public_candidate = subparsers.add_parser(
        "public-candidate-generate",
        help="Generate a persisted public-proof candidate snapshot from a therapy idea",
    )
    public_candidate.add_argument("--candidate-id", default=None, help="Existing or desired public candidate ID")
    public_candidate.add_argument("--therapy-idea-id", required=True, help="Therapy idea ID to project")
    public_candidate.add_argument("--display-id", default=None, help="Human-readable candidate display ID")
    public_candidate.add_argument(
        "--candidate-kind",
        choices=[
            "molecule",
            "peptide",
            "combination",
            "target_strategy",
            "research_program_child",
            "validation_packet",
        ],
        default=None,
        help="Override candidate kind",
    )
    public_candidate.add_argument(
        "--visibility",
        choices=["private", "draft_public", "public"],
        default="private",
        help="Candidate visibility",
    )
    public_candidate.add_argument(
        "--public-status",
        choices=[
            "draft",
            "proposed",
            "investigating",
            "evidence_supported",
            "compute_supported",
            "needs_review",
            "deprecated",
            "archived",
        ],
        default=None,
        help="Override public candidate status",
    )
    public_candidate.add_argument("--pipeline-version", default=None, help="Pipeline/methodology version label")
    public_candidate.add_argument("--commit-sha", default=None, help="Code commit SHA linked to this snapshot")
    public_candidate.add_argument("--no-compute-jobs", action="store_true", help="Do not attach matching compute jobs")
    public_candidate.add_argument("--no-decisions", action="store_true", help="Do not attach validation decisions")
    public_candidate.add_argument("--no-artifacts", action="store_true", help="Do not resolve compute artifacts")
    public_candidate.add_argument(
        "--allow-non-moonshot",
        action="store_true",
        help="Bypass the default moonshot-grade public candidate gate for admin previews",
    )
    public_candidate.add_argument(
        "--min-moonshot-score",
        type=float,
        default=0.8,
        help="Minimum therapy idea score required by the moonshot-grade public candidate gate",
    )
    public_candidate.add_argument("--no-persist", action="store_true", help="Preview without writing records")

    public_candidates = subparsers.add_parser(
        "public-candidates",
        help="List public-proof candidate records",
    )
    public_candidates.add_argument("--candidate-id", default=None, help="Public candidate ID")
    public_candidates.add_argument("--therapy-idea-id", default=None, help="Therapy idea ID")
    public_candidates.add_argument("--status", default=None, help="Public candidate status")
    public_candidates.add_argument("--visibility", default=None, help="Visibility filter")
    public_candidates.add_argument("--kind", default=None, help="Candidate kind filter")
    public_candidates.add_argument("--query", default=None, help="Title/rationale/target search")
    public_candidates.add_argument("--limit", type=int, default=50, help="Maximum candidates to return")

    public_candidate_integrity = subparsers.add_parser(
        "public-candidate-integrity",
        help="Check public candidate snapshot, manifest, and source therapy integrity",
    )
    public_candidate_integrity.add_argument("--candidate-id", action="append", default=[], help="Candidate ID to check")
    public_candidate_integrity.add_argument(
        "--therapy-idea-id",
        action="append",
        default=[],
        help="Therapy idea ID that should exist; repeatable",
    )
    public_candidate_integrity.add_argument(
        "--expected-pair",
        action="append",
        default=[],
        help="Expected candidate-to-therapy pair as candidate_id=therapy_idea_uuid",
    )
    public_candidate_integrity.add_argument("--visibility", default=None, help="Optional visibility sample filter")
    public_candidate_integrity.add_argument("--limit", type=int, default=100, help="Maximum sampled records")

    hypothesis_promotion = subparsers.add_parser(
        "hypothesis-promotion-report",
        help="Report evaluated brief and therapy idea promotion candidates",
    )
    hypothesis_promotion.add_argument("--brief-id", default=None, help="Brief ID")
    hypothesis_promotion.add_argument("--evaluation-id", default=None, help="Evaluation ID")
    hypothesis_promotion.add_argument("--therapy-idea-id", default=None, help="Therapy idea ID")
    hypothesis_promotion.add_argument("--query", default=None, help="Topic/title/hypothesis search")
    hypothesis_promotion.add_argument("--source", default=None, help="Source key filter")
    hypothesis_promotion.add_argument("--hide-blocked", action="store_true", help="Hide blocked/follow-up candidates")
    hypothesis_promotion.add_argument("--hide-ready-committee", action="store_true", help="Hide committee-ready candidates")
    hypothesis_promotion.add_argument("--hide-ready-validation", action="store_true", help="Hide validation-ready candidates")
    hypothesis_promotion.add_argument("--limit", type=int, default=50, help="Maximum candidates to return")

    validation_packets = subparsers.add_parser(
        "validation-packets",
        help="Build validation packet views from promotion candidates, plans, and queue items",
    )
    validation_packets.add_argument("--candidate-id", default=None, help="Promotion candidate ID")
    validation_packets.add_argument("--therapy-idea-id", default=None, help="Therapy idea ID")
    validation_packets.add_argument("--plan-id", default=None, help="Validation plan ID")
    validation_packets.add_argument("--queue-item-id", default=None, help="Validation request queue item ID")
    validation_packets.add_argument("--brief-id", default=None, help="Source brief ID")
    validation_packets.add_argument("--evaluation-id", default=None, help="Source evaluation ID")
    validation_packets.add_argument("--query", default=None, help="Topic/title/hypothesis search")
    validation_packets.add_argument("--source", default=None, help="Source key filter")
    validation_packets.add_argument("--hide-queue-items", action="store_true", help="Do not include queue item payloads")
    validation_packets.add_argument(
        "--hide-evidence-addendum",
        action="store_true",
        help="Do not include validation-gap follow-up brief/evaluation addendum",
    )
    validation_packets.add_argument(
        "--addendum-limit",
        type=int,
        default=25,
        help="Maximum follow-up brief/evaluation rows to include in the addendum",
    )
    validation_packets.add_argument("--queue-if-ready", action="store_true", help="Queue ready therapy idea packets")
    validation_packets.add_argument("--apply", action="store_true", help="Persist queue mutations when queueing")
    validation_packets.add_argument("--max-tasks", type=int, default=8, help="Maximum packet tasks")
    validation_packets.add_argument("--priority", type=int, default=40, help="Queue priority when queueing")
    validation_packets.add_argument("--limit", type=int, default=10, help="Maximum packets to return")

    validation_decisions = subparsers.add_parser(
        "validation-decision-report",
        help="Choose promote-broader, narrow, or archive outcomes for validation packets",
    )
    validation_decisions.add_argument("--candidate-id", default=None, help="Promotion candidate ID")
    validation_decisions.add_argument("--therapy-idea-id", default=None, help="Therapy idea ID")
    validation_decisions.add_argument("--plan-id", default=None, help="Validation plan ID")
    validation_decisions.add_argument("--queue-item-id", default=None, help="Validation request queue item ID")
    validation_decisions.add_argument("--brief-id", default=None, help="Source brief ID")
    validation_decisions.add_argument("--evaluation-id", default=None, help="Source evaluation ID")
    validation_decisions.add_argument("--query", default=None, help="Topic/title/hypothesis search")
    validation_decisions.add_argument("--source", default=None, help="Source key filter")
    validation_decisions.add_argument("--hide-queue-items", action="store_true", help="Do not include queue item payloads")
    validation_decisions.add_argument(
        "--hide-evidence-addendum",
        action="store_true",
        help="Do not include validation-gap follow-up brief/evaluation addendum",
    )
    validation_decisions.add_argument(
        "--include-source-packets",
        action="store_true",
        help="Include full source validation packet payloads in each decision row",
    )
    validation_decisions.add_argument("--no-persist", action="store_true", help="Do not persist decision records")
    validation_decisions.add_argument("--addendum-limit", type=int, default=25, help="Maximum follow-up rows")
    validation_decisions.add_argument("--limit", type=int, default=10, help="Maximum decisions to return")

    validation_decision_records = subparsers.add_parser(
        "validation-decisions",
        help="List persisted finite validation decision records",
    )
    validation_decision_records.add_argument("--id", default=None, help="Stable validation decision ID")
    validation_decision_records.add_argument("--outcome", default=None, help="Decision outcome filter")
    validation_decision_records.add_argument("--therapy-idea-id", default=None, help="Therapy idea ID")
    validation_decision_records.add_argument("--candidate-id", default=None, help="Promotion candidate ID")
    validation_decision_records.add_argument("--limit", type=int, default=50, help="Maximum records to return")

    evidence_ref_repair = subparsers.add_parser(
        "evidence-ref-repair-report",
        help="Audit validation packet and brief evidence refs for stale or unresolved citation tokens",
    )
    evidence_ref_repair.add_argument("--therapy-idea-id", default=None, help="Therapy idea ID")
    evidence_ref_repair.add_argument("--plan-id", default=None, help="Validation plan ID")
    evidence_ref_repair.add_argument("--queue-item-id", default=None, help="Validation request queue item ID")
    evidence_ref_repair.add_argument("--brief-id", default=None, help="Source brief ID")
    evidence_ref_repair.add_argument("--evaluation-id", default=None, help="Source evaluation ID")
    evidence_ref_repair.add_argument("--query", default=None, help="Topic/title/hypothesis search")
    evidence_ref_repair.add_argument("--source", default=None, help="Source key filter")
    evidence_ref_repair.add_argument(
        "--no-validation-packet",
        action="store_true",
        help="Audit only supplied brief/evaluation refs without building validation packets",
    )
    evidence_ref_repair.add_argument(
        "--no-text-refs",
        action="store_true",
        help="Do not scan free-text blockers/weaknesses for C# citation tokens",
    )
    evidence_ref_repair.add_argument("--addendum-limit", type=int, default=25, help="Maximum follow-up rows")
    evidence_ref_repair.add_argument("--limit", type=int, default=10, help="Maximum packets to inspect")

    research_program_board = subparsers.add_parser(
        "research-program-board",
        help="Run the finite big-idea Research Program Board",
    )
    research_program_board.add_argument(
        "--topic",
        default="vascular injury / coagulation / angiogenesis ecology in canine HSA and human angiosarcoma",
        help="Big research program thesis topic",
    )
    research_program_board.add_argument(
        "--disease-scope",
        default="canine hemangiosarcoma and human angiosarcoma",
        help="Disease/scope guardrail",
    )
    research_program_board.add_argument("--query", default=None, help="Evidence/packet search query")
    research_program_board.add_argument("--source", default=None, help="Optional source key filter")
    research_program_board.add_argument("--program-id", default=None, help="Existing research program ID to review/update")
    research_program_board.add_argument(
        "--brief-id",
        action="append",
        default=[],
        help="Research brief ID to pass directly to the board; repeatable",
    )
    research_program_board.add_argument(
        "--evaluation-id",
        action="append",
        default=[],
        help="Research brief evaluation ID to pass directly to the board; repeatable",
    )
    research_program_board.add_argument("--max-packets", type=int, default=5, help="Maximum validation packets to include")
    research_program_board.add_argument("--max-chunks", type=int, default=20, help="Maximum evidence chunks to include")
    research_program_board.add_argument("--max-programs", type=int, default=1, help="Maximum programs to return")
    research_program_board.add_argument("--max-evidence-loops", type=int, default=2, help="Maximum evidence loops per program")
    research_program_board.add_argument(
        "--review-mode",
        choices=("openrouter_required", "deterministic_only"),
        default="openrouter_required",
    )
    research_program_board.add_argument(
        "--review-model",
        action="append",
        default=[],
        help="OpenRouter model id; repeatable",
    )
    research_program_board.add_argument("--model-profile", default="research_program_board", help="Logical model profile")
    research_program_board.add_argument("--no-persist", action="store_true", help="Do not persist generated programs")

    research_programs = subparsers.add_parser(
        "research-programs",
        help="List persisted Research Program Board records",
    )
    research_programs.add_argument("--id", default=None, help="Research program ID")
    research_programs.add_argument("--status", default=None, help="Research program status")
    research_programs.add_argument("--gate-decision", default=None, help="Research program gate decision")
    research_programs.add_argument("--query", default=None, help="Thesis/title search")
    research_programs.add_argument("--limit", type=int, default=50, help="Maximum programs to return")

    research_program_evidence_loop = subparsers.add_parser(
        "research-program-evidence-loop",
        help="Run one capped evidence loop for a persisted research program",
    )
    research_program_evidence_loop.add_argument("--program-id", default=None, help="Research program ID")
    research_program_evidence_loop.add_argument("--query", default=None, help="Fallback thesis/title search")
    research_program_evidence_loop.add_argument(
        "--source",
        action="append",
        default=[],
        help="Override source key; repeatable. Defaults to each task's source hints.",
    )
    research_program_evidence_loop.add_argument("--max-tasks", type=int, default=5, help="Maximum evidence tasks to queue")
    research_program_evidence_loop.add_argument(
        "--max-source-queries",
        type=int,
        default=20,
        help="Maximum source queries to persist across the loop",
    )
    research_program_evidence_loop.add_argument(
        "--max-sources-per-task",
        type=int,
        default=4,
        help="Maximum source keys per task",
    )
    research_program_evidence_loop.add_argument("--priority", type=int, default=40, help="Queue priority")
    research_program_evidence_loop.add_argument("--no-briefs", action="store_true", help="Do not queue research briefs")
    research_program_evidence_loop.add_argument("--no-leads", action="store_true", help="Do not create research leads")
    research_program_evidence_loop.add_argument("--no-source-queries", action="store_true", help="Do not create source queries")
    research_program_evidence_loop.add_argument("--max-chunks-per-perspective", type=int, default=10)
    research_program_evidence_loop.add_argument("--max-claims", type=int, default=20)
    research_program_evidence_loop.add_argument("--max-chunk-chars", type=int, default=2200)
    research_program_evidence_loop.add_argument("--brief-style", default="technical")
    research_program_evidence_loop.add_argument("--model-profile", default="research_brief")
    research_program_evidence_loop.add_argument("--review-mode", default="openrouter_required")
    research_program_evidence_loop.add_argument(
        "--review-model",
        action="append",
        default=[],
        help="OpenRouter model id; repeatable",
    )
    research_program_evidence_loop.add_argument("--dry-run", action="store_true", help="Preview without persisting")

    omics_accession_hunt = subparsers.add_parser(
        "omics-accession-hunt",
        help="Run a bounded GEO/SRA accession hunt for omics evidence gaps",
    )
    omics_accession_hunt.add_argument("--program-id", default=None, help="Research program ID")
    omics_accession_hunt.add_argument(
        "--query",
        default="canine hemangiosarcoma human angiosarcoma VIM vimentin transcriptome RNA-seq expression",
        help="Fallback topic query",
    )
    omics_accession_hunt.add_argument("--source", action="append", default=[], help="Source key; repeatable")
    omics_accession_hunt.add_argument("--disease-term", action="append", default=[], help="Disease term; repeatable")
    omics_accession_hunt.add_argument("--gene", action="append", default=[], help="Gene/term; repeatable")
    omics_accession_hunt.add_argument("--query-text", action="append", default=[], help="Exact source query; repeatable")
    omics_accession_hunt.add_argument("--limit-per-query", type=int, default=5)
    omics_accession_hunt.add_argument("--max-queries", type=int, default=8)
    omics_accession_hunt.add_argument("--no-persist-queries", action="store_true")
    omics_accession_hunt.add_argument("--dry-run", action="store_true")

    omics_evidence_packets = subparsers.add_parser(
        "omics-evidence-packets",
        help="Build omics evidence packets from stored GEO/SRA accession metadata",
    )
    omics_evidence_packets.add_argument("--program-id", default=None, help="Research program ID")
    omics_evidence_packets.add_argument(
        "--query",
        default="canine hemangiosarcoma human angiosarcoma VIM vimentin transcriptome RNA-seq expression",
        help="Fallback topic query",
    )
    omics_evidence_packets.add_argument("--source", action="append", default=[], help="Source key; repeatable")
    omics_evidence_packets.add_argument("--disease-term", action="append", default=[], help="Disease term; repeatable")
    omics_evidence_packets.add_argument("--gene", action="append", default=[], help="Gene/term; repeatable")
    omics_evidence_packets.add_argument("--accession", action="append", default=[], help="Specific accession; repeatable")
    omics_evidence_packets.add_argument("--limit", type=int, default=100)
    omics_evidence_packets.add_argument("--min-datasets-per-packet", type=int, default=1)
    omics_evidence_packets.add_argument("--no-context-packet", action="store_true")
    omics_evidence_packets.add_argument("--dry-run", action="store_true")

    omics_readouts = subparsers.add_parser(
        "omics-readouts",
        help="Compute VIM and vascular-state readouts from processed omics matrices",
    )
    omics_readouts.add_argument("--program-id", default=None, help="Research program ID")
    omics_readouts.add_argument("--therapy-idea-id", default=None, help="Therapy idea ID")
    omics_readouts.add_argument("--packet-id", default=None, help="Specific omics evidence packet ID")
    omics_readouts.add_argument("--packet-key", default=None, help="Packet key, e.g. canine_hsa")
    omics_readouts.add_argument(
        "--query",
        default="canine hemangiosarcoma human angiosarcoma VIM vimentin transcriptome RNA-seq expression",
        help="Fallback topic query",
    )
    omics_readouts.add_argument("--source", action="append", default=[], help="Source key; repeatable")
    omics_readouts.add_argument("--disease-term", action="append", default=[], help="Disease term; repeatable")
    omics_readouts.add_argument("--gene", action="append", default=[], help="Gene/term; repeatable")
    omics_readouts.add_argument("--accession", action="append", default=[], help="Specific accession; repeatable")
    omics_readouts.add_argument(
        "--matrix-uri",
        action="append",
        default=[],
        help="Processed matrix URI as ACCESSION=URI; repeatable",
    )
    omics_readouts.add_argument(
        "--sample-groups-json",
        default=None,
        help="JSON mapping accession -> sample -> tumor/control group",
    )
    omics_readouts.add_argument("--limit", type=int, default=100)
    omics_readouts.add_argument("--max-datasets", type=int, default=5)
    omics_readouts.add_argument("--artifact-dir", default=None)
    omics_readouts.add_argument("--run-agent", action="store_true", help="Run omics_validation_agent after compute")
    omics_readouts.add_argument("--model-profile", default="openrouter_required")
    omics_readouts.add_argument("--dry-run", action="store_true")

    omics_locus_signals = subparsers.add_parser(
        "omics-locus-signals",
        help="Extract target-locus signal from ChRO-seq or related bigWig tracks",
    )
    omics_locus_signals.add_argument("--packet-id", default=None, help="Specific omics evidence packet ID")
    omics_locus_signals.add_argument("--packet-key", default=None, help="Packet key, e.g. canine_hsa")
    omics_locus_signals.add_argument(
        "--query",
        default="canine hemangiosarcoma human angiosarcoma VIM vimentin ChRO-seq bigWig locus signal",
    )
    omics_locus_signals.add_argument("--source", action="append", default=[], help="Source key; repeatable")
    omics_locus_signals.add_argument("--disease-term", action="append", default=[], help="Disease term; repeatable")
    omics_locus_signals.add_argument("--gene", action="append", default=[], help="Gene symbol; repeatable")
    omics_locus_signals.add_argument("--accession", action="append", default=[], help="Specific accession; repeatable")
    omics_locus_signals.add_argument("--limit", type=int, default=100)
    omics_locus_signals.add_argument("--max-datasets", type=int, default=5)
    omics_locus_signals.add_argument("--max-samples-per-group", type=int, default=2)
    omics_locus_signals.add_argument("--remote-extract-timeout-seconds", type=int, default=600)
    omics_locus_signals.add_argument("--artifact-dir", default=None)
    omics_locus_signals.add_argument(
        "--bigwig-json",
        default=None,
        help="JSON mapping sample -> {plus: URI, minus: URI}; optional override",
    )
    omics_locus_signals.add_argument(
        "--sample-groups-json",
        default=None,
        help="JSON mapping accession -> sample -> tumor/control group",
    )
    omics_locus_signals.add_argument(
        "--target-loci-json",
        default=None,
        help="JSON mapping gene -> locus object with chromosome/start/end/strand/genome_build",
    )
    omics_locus_signals.add_argument("--run-agent", action="store_true", help="Run omics_validation_agent after compute")
    omics_locus_signals.add_argument("--model-profile", default="openrouter_required")
    omics_locus_signals.add_argument("--dry-run", action="store_true")

    omics_followups = subparsers.add_parser(
        "omics-followups",
        help="Generate focused follow-up evidence tasks from omics readout gaps",
    )
    omics_followups.add_argument(
        "--query",
        default="canine hemangiosarcoma human angiosarcoma VIM vimentin omics follow-up evidence",
    )
    omics_followups.add_argument("--gene", action="append", default=[], help="Gene symbol; repeatable")
    omics_followups.add_argument("--accession", action="append", default=[], help="Specific accession; repeatable")
    omics_followups.add_argument("--source", action="append", default=[], help="Source key; repeatable")
    omics_followups.add_argument("--max-tasks", type=int, default=8)
    omics_followups.add_argument("--readout-report-json", default=None, help="JSON payload from omics-readouts")
    omics_followups.add_argument("--locus-report-json", default=None, help="JSON payload from omics-locus-signals")
    omics_followups.add_argument("--validation-agent-json", default=None, help="JSON validation_agent_result payload")
    omics_followups.add_argument("--include-locus-signals", action="store_true", help="Run locus signal compute first")
    omics_followups.add_argument("--locus-signal-request-json", default=None, help="Additional JSON args for locus signal compute")
    omics_followups.add_argument("--no-research-leads", action="store_true")
    omics_followups.add_argument("--no-source-queries", action="store_true")
    omics_followups.add_argument("--apply", action="store_true", help="Persist research leads/source queries")

    x_topic_monitor = subparsers.add_parser(
        "x-topic-monitor",
        help="Run TwitterAPI.io topic monitoring and the X topic review agent",
    )
    x_topic_monitor.add_argument("--query-name", default=None, help="Optional configured query name to run")
    x_topic_monitor.add_argument("--max-results", type=int, default=20, help="Maximum provider results per query")
    x_topic_monitor.add_argument("--max-candidates", type=int, default=20, help="Maximum candidates to review")
    x_topic_monitor.add_argument("--query-delay-seconds", type=float, default=6.0, help="Delay between provider queries")
    x_topic_monitor.add_argument(
        "--retention-mode",
        choices=("store_post_id_only", "store_metadata_only", "store_text"),
        default="store_metadata_only",
    )
    x_topic_monitor.add_argument("--model-profile", default="reviewer", help="Logical model profile")
    x_topic_monitor.add_argument(
        "--review-mode",
        choices=("external_required", "openrouter_required", "openrouter_compare", "deterministic_only"),
        default="openrouter_required",
        help="Whether review is external, OpenRouter-backed, comparative, or deterministic only",
    )
    x_topic_monitor.add_argument(
        "--review-model",
        action="append",
        default=[],
        help="OpenRouter model id; repeat to compare multiple models",
    )

    x_topic_review = subparsers.add_parser(
        "x-topic-review",
        help="Run the recommend-only X topic review agent over candidate JSON",
    )
    x_topic_review.add_argument(
        "--provider-report",
        default=None,
        help="Path to a JSON provider report from x_topic_monitor_review_report",
    )
    x_topic_review.add_argument(
        "--candidate-json",
        action="append",
        default=[],
        help="Candidate JSON object; repeat to review multiple candidates without a provider report",
    )
    x_topic_review.add_argument("--recent-run-limit", type=int, default=5, help="Recent agent runs to include")
    x_topic_review.add_argument("--max-candidates", type=int, default=20, help="Maximum candidates to review")
    x_topic_review.add_argument("--model-profile", default="reviewer", help="Logical model profile")
    x_topic_review.add_argument(
        "--review-mode",
        choices=("external_required", "openrouter_required", "openrouter_compare", "deterministic_only"),
        default="openrouter_required",
        help="Whether review is external, OpenRouter-backed, comparative, or deterministic only",
    )
    x_topic_review.add_argument(
        "--review-model",
        action="append",
        default=[],
        help="OpenRouter model id; repeat to compare multiple models",
    )

    x_linked_article_followup = subparsers.add_parser(
        "x-linked-article-followup",
        help="Fetch and parse article links queued by the X topic review agent",
    )
    x_linked_article_followup.add_argument("--url", action="append", default=[], help="Direct article URL to process")
    x_linked_article_followup.add_argument("--recent-run-limit", type=int, default=10, help="Recent X agent runs to inspect")
    x_linked_article_followup.add_argument("--max-urls", type=int, default=10, help="Maximum queued URLs to process")
    x_linked_article_followup.add_argument("--no-fetch", action="store_true", help="Only report queued article URLs")
    x_linked_article_followup.add_argument("--no-parse", action="store_true", help="Fetch only; skip parser review records")
    x_linked_article_followup.add_argument("--approved-by", default=None, help="Required for controlled page fetch")
    x_linked_article_followup.add_argument("--approval-note", default=None, help="Optional approval note")
    x_linked_article_followup.add_argument(
        "--robots-policy",
        choices=["unknown", "reviewed", "disallow", "manual_only"],
        default="reviewed",
        help="Reviewed robots/TOS policy for this controlled fetch",
    )

    x_linked_article_review = subparsers.add_parser(
        "x-linked-article-review",
        help="Run the linked-article review agent over parsed article records",
    )
    x_linked_article_review.add_argument("--review-id", action="append", default=[], help="Review record ID")
    x_linked_article_review.add_argument(
        "--review-status",
        choices=["needs_review", "accepted", "rejected"],
        default="needs_review",
        help="Parsed record review status filter",
    )
    x_linked_article_review.add_argument("--limit", type=int, default=50, help="Maximum records to review")
    x_linked_article_review.add_argument("--model-profile", default="reviewer", help="Logical model profile")
    x_linked_article_review.add_argument(
        "--review-mode",
        choices=("external_required", "openrouter_required", "openrouter_compare", "deterministic_only"),
        default="openrouter_required",
        help="Review mode for the linked-article agent",
    )
    x_linked_article_review.add_argument("--review-model", action="append", default=[], help="Review model id")

    queue_source_followups = subparsers.add_parser(
        "queue-source-followups",
        help="Queue primary-source follow-ups from parsed article review records",
    )
    queue_source_followups.add_argument("--source", default="x_linked_article", help="Scrape source key")
    queue_source_followups.add_argument("--review-id", action="append", default=[], help="Review record ID")
    queue_source_followups.add_argument(
        "--review-status",
        choices=["needs_review", "accepted", "rejected"],
        default=None,
        help="Optional parsed record review status filter",
    )
    queue_source_followups.add_argument("--limit", type=int, default=100, help="Maximum records to scan")
    queue_source_followups.add_argument("--include-existing", action="store_true", help="Return existing queue rows too")
    queue_source_followups.add_argument(
        "--no-agent-recommendations",
        action="store_true",
        help="Only queue deterministic parser links; ignore linked-article review agent recommendations",
    )
    queue_source_followups.add_argument(
        "--agent-run-limit",
        type=int,
        default=20,
        help="Maximum recent linked-article review agent runs to scan",
    )

    queue_unpaywall_doi_followups = subparsers.add_parser(
        "queue-unpaywall-doi-followups",
        help="Queue manual Unpaywall DOI open-access enrichment from stored research objects",
    )
    queue_unpaywall_doi_followups.add_argument(
        "--source",
        action="append",
        default=[],
        help="Research object source key to scan; repeatable",
    )
    queue_unpaywall_doi_followups.add_argument("--limit", type=int, default=100, help="Maximum research objects to scan")
    queue_unpaywall_doi_followups.add_argument(
        "--include-existing",
        action="store_true",
        help="Return existing Unpaywall DOI queue rows too",
    )

    list_source_followups = subparsers.add_parser("source-followups", help="List queued source follow-ups")
    list_source_followups.add_argument("--source", default=None, help="Target source key")
    list_source_followups.add_argument(
        "--status",
        choices=["queued", "approved", "ingested", "failed", "skipped", "rejected"],
        default=None,
        help="Queue item status",
    )
    list_source_followups.add_argument("--identifier-type", default=None, help="Identifier type filter")
    list_source_followups.add_argument("--limit", type=int, default=50, help="Maximum queue rows")

    collect_research_leads = subparsers.add_parser(
        "collect-research-leads",
        help="Collect watchlist leads from recent agent runs",
    )
    collect_research_leads.add_argument(
        "--agent-name",
        action="append",
        default=[],
        help="Agent name to scan; repeatable. Defaults to X topic and linked-article review agents.",
    )
    collect_research_leads.add_argument(
        "--status",
        action="append",
        default=[],
        help="Agent run status to scan; repeatable. Defaults to completed.",
    )
    collect_research_leads.add_argument("--limit", type=int, default=50, help="Maximum recent runs per agent/status")
    collect_research_leads.add_argument("--include-existing", action="store_true", help="Return existing leads too")

    research_leads = subparsers.add_parser("research-leads", help="List, fetch, or update watchlist research leads")
    research_leads.add_argument("--id", default=None, help="Optional lead_id to fetch")
    research_leads.add_argument("--status", default=None, help="Lead status filter or new status with --set-status")
    research_leads.add_argument("--lead-type", default=None, help="Lead type filter")
    research_leads.add_argument("--source", default=None, help="Source key filter")
    research_leads.add_argument("--limit", type=int, default=50, help="Maximum leads to return")
    research_leads.add_argument("--set-status", default=None, help="Update a lead status; requires --id")

    research_followup_resolver = subparsers.add_parser(
        "resolve-research-followups",
        help="Resolve evidence-light research leads into durable source evidence",
    )
    research_followup_resolver.add_argument("--lead-id", action="append", default=[], help="Specific lead ID")
    research_followup_resolver.add_argument(
        "--status",
        action="append",
        default=[],
        help="Lead status to resolve; defaults to followup",
    )
    research_followup_resolver.add_argument("--source", action="append", default=[], help="Lead source filter")
    research_followup_resolver.add_argument(
        "--search-source",
        action="append",
        default=[],
        help="Durable source to search when a lead has no identifier",
    )
    research_followup_resolver.add_argument("--limit", type=int, default=25, help="Maximum leads to inspect")
    research_followup_resolver.add_argument(
        "--no-ingest-source-followups",
        action="store_true",
        help="Do not ingest queued identifier follow-ups",
    )
    research_followup_resolver.add_argument(
        "--no-search-missing-identifiers",
        action="store_true",
        help="Do not search durable sources for leads without identifiers",
    )
    research_followup_resolver.add_argument(
        "--no-promote",
        action="store_true",
        help="Attach evidence but keep leads in followup",
    )
    research_followup_resolver.add_argument("--no-claim-extraction", action="store_true")
    research_followup_resolver.add_argument("--dry-run", action="store_true")
    research_followup_resolver.add_argument(
        "--force-live-search",
        action="store_true",
        help="Refresh durable sources even when stored chunks already satisfy the evidence threshold.",
    )
    research_followup_resolver.add_argument(
        "--no-evidence-inspection",
        action="store_true",
        help="Do not attach chunk/object inspection previews for selected evidence refs.",
    )
    research_followup_resolver.add_argument("--min-evidence-chunks", type=int, default=1)
    research_followup_resolver.add_argument("--evidence-inspection-limit", type=int, default=8)
    research_followup_resolver.add_argument("--search-limit-per-source", type=int, default=2)
    research_followup_resolver.add_argument("--max-search-terms", type=int, default=12)
    research_followup_resolver.add_argument("--approved-by", default=None)

    ingest_source_followups = subparsers.add_parser(
        "ingest-source-followups",
        help="Ingest queued primary-source follow-ups through API harvesters",
    )
    ingest_source_followups.add_argument("--source", action="append", default=[], help="Target source key")
    ingest_source_followups.add_argument("--followup-id", action="append", default=[], help="Specific follow-up row")
    ingest_source_followups.add_argument(
        "--status",
        action="append",
        default=[],
        choices=["queued", "approved", "ingested", "failed", "skipped", "rejected"],
        help="Queue status to process; repeatable",
    )
    ingest_source_followups.add_argument("--limit", type=int, default=25, help="Maximum queue rows to process")
    ingest_source_followups.add_argument("--approved-by", default=None, help="Operator approval identity")
    ingest_source_followups.add_argument("--no-claim-extraction", action="store_true", help="Skip enrichment after ingest")
    ingest_source_followups.add_argument("--dry-run", action="store_true", help="Report queued rows without ingesting")

    agent_runs = subparsers.add_parser("agent-runs", help="List or fetch persisted agent runs")
    agent_runs.add_argument("--id", default=None, help="Optional agent_run_id to fetch")
    agent_runs.add_argument("--agent-name", default=None, help="Optional agent name filter")
    agent_runs.add_argument("--status", default=None, help="Optional status filter")
    agent_runs.add_argument("--source", default=None, help="Optional source key filter")
    agent_runs.add_argument("--limit", type=int, default=50, help="Maximum runs to return")

    agent_performance = subparsers.add_parser(
        "agent-performance",
        help="Aggregate operator and evaluator reviews into agent/model/prompt performance rows",
    )
    agent_performance.add_argument("--agent-name", default=None, help="Optional agent name filter")
    agent_performance.add_argument("--status", default=None, help="Optional run status filter")
    agent_performance.add_argument("--source", default=None, help="Optional source key filter")
    agent_performance.add_argument("--limit", type=int, default=500, help="Recent agent runs to scan")
    agent_performance.add_argument("--min-sample-size", type=int, default=3, help="Runs required before sample is reliable")

    agent_performance_evaluate = subparsers.add_parser(
        "agent-performance-evaluate",
        help="Run OpenRouter specialist evaluators over recent reviewed agent runs",
    )
    agent_performance_evaluate.add_argument("--agent-name", default=None, help="Optional agent name filter")
    agent_performance_evaluate.add_argument("--agent-run-id", action="append", default=[], help="Specific agent run ID")
    agent_performance_evaluate.add_argument("--status", default="completed", help="Optional run status filter")
    agent_performance_evaluate.add_argument("--source", default=None, help="Optional source key filter")
    agent_performance_evaluate.add_argument("--limit", type=int, default=25, help="Recent reviewed runs to evaluate")
    agent_performance_evaluate.add_argument(
        "--include-unreviewed",
        action="store_true",
        help="Evaluate unreviewed runs too; default is reviewed runs only",
    )
    agent_performance_evaluate.add_argument(
        "--model-profile",
        default="agent_performance_evaluator",
        help="Logical evaluator profile recorded in the ledger",
    )
    agent_performance_evaluate.add_argument(
        "--review-model",
        action="append",
        default=[],
        help="OpenRouter evaluator model id; repeat to override the default",
    )

    reward_events = subparsers.add_parser(
        "reward-events",
        help="Aggregate durable reward events for the agent learning loop",
    )
    reward_events.add_argument("--agent-name", default=None, help="Optional agent name filter")
    reward_events.add_argument("--source", default=None, help="Optional source key filter")
    reward_events.add_argument(
        "--event-source",
        default=None,
        choices=["operator_review", "llm_evaluator_review", "system_review", "downstream_progress", "manual"],
        help="Optional reward event source filter",
    )
    reward_events.add_argument(
        "--group-by",
        default="agent_name",
        choices=["agent_name", "model_profile", "task_type", "source_key", "event_source"],
        help="Report grouping dimension",
    )
    reward_events.add_argument("--limit", type=int, default=500, help="Recent reward events to scan")
    reward_events.add_argument("--min-sample-size", type=int, default=3, help="Events required before sample is reliable")

    reward_events_sync = subparsers.add_parser(
        "reward-events-sync",
        help="Convert operator/evaluator agent run reviews into reward events",
    )
    reward_events_sync.add_argument("--agent-name", default=None, help="Optional agent name filter")
    reward_events_sync.add_argument("--source", default=None, help="Optional source key filter")
    reward_events_sync.add_argument(
        "--reviewer-type",
        default=None,
        choices=["operator", "llm_evaluator", "system"],
        help="Optional reviewer type filter",
    )
    reward_events_sync.add_argument("--limit", type=int, default=500, help="Recent reviews to scan")
    reward_events_sync.add_argument(
        "--include-existing",
        action="store_true",
        help="Regenerate reward events that already exist for a review",
    )
    reward_events_sync.add_argument("--created-by", default="reward_review_sync", help="Ledger creator identity")

    run_manifests = subparsers.add_parser(
        "run-manifests",
        help="List trace/run manifests that bind ledgers, models, methods, artifacts, and outputs",
    )
    run_manifests.add_argument("--trace-id", default=None, help="Optional trace_id filter")
    run_manifests.add_argument(
        "--manifest-type",
        default=None,
        choices=[
            "agent_run",
            "dagster_run",
            "cli_invocation",
            "mcp_invocation",
            "public_candidate_snapshot",
            "compute_job",
            "reward_sync",
            "manual",
        ],
        help="Optional manifest type filter",
    )
    run_manifests.add_argument(
        "--status",
        default=None,
        choices=["running", "completed", "failed", "cancelled", "blocked", "unknown"],
        help="Optional manifest status filter",
    )
    run_manifests.add_argument("--limit", type=int, default=50, help="Recent run manifests to list")

    agent_findings_escalate = subparsers.add_parser(
        "escalate-agent-findings",
        help="Create research leads and source queries from bad/needs-followup evaluator findings",
    )
    agent_findings_escalate.add_argument("--review-id", action="append", default=[], help="Specific evaluator review ID")
    agent_findings_escalate.add_argument("--agent-run-id", action="append", default=[], help="Specific agent run ID")
    agent_findings_escalate.add_argument(
        "--verdict",
        action="append",
        default=[],
        choices=["bad", "needs_followup", "unclear", "useful"],
        help="Evaluator verdict to escalate; defaults to bad and needs_followup",
    )
    agent_findings_escalate.add_argument("--source", action="append", default=[], help="Override target source key")
    agent_findings_escalate.add_argument("--limit", type=int, default=25, help="Maximum findings to inspect")
    agent_findings_escalate.add_argument("--no-research-leads", action="store_true", help="Do not create research leads")
    agent_findings_escalate.add_argument("--no-source-queries", action="store_true", help="Do not create SourceQuery rows")
    agent_findings_escalate.add_argument("--dry-run", action="store_true", help="Return planned work without persisting")
    agent_findings_escalate.add_argument("--operator", default="cli_operator", help="Operator identity recorded in metadata")

    refine_followups = subparsers.add_parser(
        "refine-research-followups",
        help="Create refined validation-gap SourceQuery rows from evaluator follow-up feedback",
    )
    refine_followups.add_argument("--lead-id", action="append", default=[], help="Specific research lead ID")
    refine_followups.add_argument("--review-id", action="append", default=[], help="Specific evaluator review ID")
    refine_followups.add_argument(
        "--verdict",
        action="append",
        default=[],
        choices=["bad", "needs_followup", "unclear", "useful"],
        help="Evaluator verdict to refine; defaults to bad and needs_followup",
    )
    refine_followups.add_argument("--source", action="append", default=[], help="Override target source key")
    refine_followups.add_argument("--limit", type=int, default=25, help="Maximum reviews to inspect")
    refine_followups.add_argument(
        "--max-queries-per-review",
        type=int,
        default=4,
        help="Maximum refined SourceQuery rows per evaluator review",
    )
    refine_followups.add_argument("--dry-run", action="store_true", help="Return planned queries without persisting")
    refine_followups.add_argument("--operator", default="cli_operator", help="Operator identity recorded in metadata")

    evaluate_research_brief = subparsers.add_parser(
        "evaluate-research-brief",
        help="Evaluate persisted research brief synthesis quality",
    )
    evaluate_research_brief.add_argument("--brief-id", default=None, help="Specific brief_id to evaluate")
    evaluate_research_brief.add_argument("--topic-query", default=None, help="Latest completed brief topic search")
    evaluate_research_brief.add_argument("--source", default=None, help="Optional source key filter")
    evaluate_research_brief.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Candidate brief search limit when --brief-id is omitted",
    )
    evaluate_research_brief.add_argument(
        "--minimum-overall-score",
        type=float,
        default=0.7,
        help="Minimum weighted score required to pass the quality bar",
    )
    evaluate_research_brief.add_argument(
        "--model-profile",
        default="synthesis_quality_evaluator",
        help="Evaluator model/profile label recorded in the ledger",
    )
    evaluate_research_brief.add_argument(
        "--review-mode",
        choices=("openrouter_required", "openrouter_compare", "deterministic_only"),
        default="openrouter_required",
        help="Whether evaluation is OpenRouter-backed or deterministic only",
    )
    evaluate_research_brief.add_argument(
        "--review-model",
        action="append",
        default=[],
        help="OpenRouter evaluator model id; repeat to compare multiple models",
    )

    research_brief_evaluations = subparsers.add_parser(
        "research-brief-evaluations",
        help="List or fetch persisted research brief evaluations",
    )
    research_brief_evaluations.add_argument("--id", default=None, help="Optional evaluation_id to fetch")
    research_brief_evaluations.add_argument("--brief-id", default=None, help="Optional brief_id filter")
    research_brief_evaluations.add_argument("--readiness", default=None, help="Optional readiness filter")
    research_brief_evaluations.add_argument(
        "--passes-quality-bar",
        choices=["true", "false"],
        default=None,
        help="Optional pass/fail quality-bar filter",
    )
    research_brief_evaluations.add_argument("--limit", type=int, default=50, help="Maximum evaluations to return")

    queue_research_brief_followups = subparsers.add_parser(
        "queue-research-brief-followups",
        help="Create durable follow-up research leads from weak research brief evaluations",
    )
    queue_research_brief_followups.add_argument("--brief-id", action="append", default=[], help="Specific brief ID")
    queue_research_brief_followups.add_argument(
        "--evaluation-id",
        action="append",
        default=[],
        help="Specific research brief evaluation ID",
    )
    queue_research_brief_followups.add_argument("--status", default=None, help="Brief status filter")
    queue_research_brief_followups.add_argument("--source", default=None, help="Optional source key filter")
    queue_research_brief_followups.add_argument("--topic-query", default=None, help="Case-insensitive topic/scope filter")
    queue_research_brief_followups.add_argument("--limit", type=int, default=50, help="Maximum briefs to inspect")
    queue_research_brief_followups.add_argument(
        "--max-limitations-per-brief",
        type=int,
        default=20,
        help="Maximum follow-up leads to create per brief",
    )
    queue_research_brief_followups.add_argument(
        "--no-evaluations",
        action="store_true",
        help="Use only brief-level limitations, not latest evaluation feedback",
    )
    queue_research_brief_followups.add_argument(
        "--force",
        action="store_true",
        help="Queue evidence-limit follow-ups even when the brief only needs evaluator review",
    )
    queue_research_brief_followups.add_argument("--dry-run", action="store_true", help="Preview without persisting")

    plan_validation = subparsers.add_parser(
        "plan-validation",
        help="Create a recommend-only validation plan from a persisted research brief",
    )
    plan_validation.add_argument("--brief-id", default=None, help="Specific brief_id to plan from")
    plan_validation.add_argument("--evaluation-id", default=None, help="Specific evaluation_id to plan from")
    plan_validation.add_argument("--topic-query", default=None, help="Latest completed brief topic search")
    plan_validation.add_argument("--source", default=None, help="Optional source key filter")
    plan_validation.add_argument(
        "--allow-unready",
        action="store_true",
        help="Allow planning even when the latest synthesis evaluation has not passed",
    )
    plan_validation.add_argument("--max-tasks", type=int, default=8, help="Maximum validation tasks to propose")
    plan_validation.add_argument(
        "--model-profile",
        default="validation_planner",
        help="Planner model/profile label recorded in the ledger",
    )

    validation_plans = subparsers.add_parser(
        "validation-plans",
        help="List or fetch persisted validation plans",
    )
    validation_plans.add_argument("--id", default=None, help="Optional plan_id to fetch")
    validation_plans.add_argument("--brief-id", default=None, help="Optional brief_id filter")
    validation_plans.add_argument("--evaluation-id", default=None, help="Optional evaluation_id filter")
    validation_plans.add_argument("--status", default=None, help="Optional status filter")
    validation_plans.add_argument("--readiness", default=None, help="Optional readiness filter")
    validation_plans.add_argument("--limit", type=int, default=50, help="Maximum plans to return")

    queue_validation_requests = subparsers.add_parser(
        "queue-validation-requests",
        help="Queue validation requests from a ready validation plan",
    )
    queue_validation_requests.add_argument("--plan-id", required=True, help="Validation plan to queue from")
    queue_validation_requests.add_argument(
        "--task-id",
        action="append",
        default=[],
        help="Optional task_id to queue; repeat for multiple tasks",
    )
    queue_validation_requests.add_argument(
        "--apply",
        action="store_true",
        help="Persist queue items. Without this flag the command is a dry run.",
    )

    queue_therapy_committee_validation_requests = subparsers.add_parser(
        "queue-therapy-committee-validation-requests",
        help="Queue validation requests from a completed therapy committee agent run",
    )
    queue_therapy_committee_validation_requests.add_argument(
        "--agent-run-id",
        default=None,
        help="Therapy committee agent_run_id. Defaults to the latest completed committee run.",
    )
    queue_therapy_committee_validation_requests.add_argument(
        "--idea-id",
        action="append",
        default=[],
        help="Specific therapy idea_id to queue; repeat for multiple ideas.",
    )
    queue_therapy_committee_validation_requests.add_argument("--max-ideas", type=int, default=1)
    queue_therapy_committee_validation_requests.add_argument("--priority", type=int, default=40)
    queue_therapy_committee_validation_requests.add_argument(
        "--apply",
        action="store_true",
        help="Persist queue items. Without this flag the command is a dry run.",
    )

    validation_request_queue = subparsers.add_parser(
        "validation-request-queue",
        help="List or fetch queued validation requests",
    )
    validation_request_queue.add_argument("--id", default=None, help="Optional queue_item_id to fetch")
    validation_request_queue.add_argument("--plan-id", default=None, help="Optional validation plan filter")
    validation_request_queue.add_argument("--status", default=None, help="Optional queue status filter")
    validation_request_queue.add_argument(
        "--status-any",
        action="append",
        default=[],
        help="Eligible queue status; repeat for multiple statuses",
    )
    validation_request_queue.add_argument("--source", default=None, help="Optional source key filter")
    validation_request_queue.add_argument("--task-type", default=None, help="Optional validation plan task type filter")
    validation_request_queue.add_argument("--topic-query", default=None, help="Case-insensitive topic/task filter")
    validation_request_queue.add_argument("--limit", type=int, default=50, help="Maximum queue items to return")

    approve_validation_request = subparsers.add_parser(
        "approve-validation-request",
        help="Approve a queued validation request for explicit dispatch",
    )
    approve_validation_request.add_argument("--id", required=True, help="queue_item_id to approve")
    approve_validation_request.add_argument("--approved-by", required=True, help="Person or system approving dispatch")
    approve_validation_request.add_argument("--approval-note", default=None, help="Optional approval note")

    dispatch_validation_request = subparsers.add_parser(
        "dispatch-validation-request",
        help="Dispatch an approved validation request through the validation agent layer",
    )
    dispatch_validation_request.add_argument("--id", required=True, help="queue_item_id to dispatch")
    dispatch_validation_request.add_argument(
        "--model-profile",
        default="openrouter_required",
        help="Validation agent model profile or OpenRouter model. Use deterministic_only for local tests.",
    )

    validation_autopilot = subparsers.add_parser(
        "validation-autopilot",
        help="Preview or run conservative automatic validation approval and dispatch",
    )
    validation_autopilot.add_argument("--apply", action="store_true", help="Approve and dispatch selected items")
    validation_autopilot.add_argument("--force", action="store_true", help="Bypass manual-grace and item-age windows")
    validation_autopilot.add_argument("--disabled", action="store_true", help="Return a disabled policy result")
    validation_autopilot.add_argument("--max-per-run", type=int, default=2)
    validation_autopilot.add_argument("--manual-grace-hours", type=float, default=6.0)
    validation_autopilot.add_argument("--minimum-queue-age-hours", type=float, default=1.0)
    validation_autopilot.add_argument("--hourly-budget-usd", type=float, default=0.25)
    validation_autopilot.add_argument("--daily-budget-usd", type=float, default=1.50)
    validation_autopilot.add_argument("--estimated-cost-per-item-usd", type=float, default=0.03)
    validation_autopilot.add_argument(
        "--allowed-task-type",
        action="append",
        default=[],
        help="Allowlisted task type; repeat for multiple.",
    )
    validation_autopilot.add_argument(
        "--allowed-validation-type",
        action="append",
        default=[],
        help="Allowlisted validation type; repeat for multiple.",
    )
    validation_autopilot.add_argument("--source", action="append", default=[], help="Optional source key filter")
    validation_autopilot.add_argument(
        "--model-profile",
        default="openrouter_required",
        help="Validation model profile to dispatch with.",
    )

    md_expert_packet = subparsers.add_parser(
        "md-expert-packet",
        help="Generate the MD expert review packet for a compute job",
    )
    md_expert_packet.add_argument("--compute-job-id", required=True, help="Compute job ID to package")
    md_expert_packet.add_argument("--endpoint-id", default="cbf4ffekmo36t9")
    md_expert_packet.add_argument("--endpoint-name", default="hsa-md-validation")
    md_expert_packet.add_argument("--template-name", default="hsa-md-openmm")
    md_expert_packet.add_argument("--no-persist", action="store_true", help="Render without persisting the packet")

    md_expert_agent_review = subparsers.add_parser(
        "md-expert-agent-review",
        help="Run the OpenRouter-backed MD expert agent over a packet",
    )
    md_expert_agent_review.add_argument("--packet-id", required=True, help="MD expert packet ID to review")
    md_expert_agent_review.add_argument(
        "--model-profile",
        default="openrouter_required",
        help="OpenRouter model/profile for the MD expert agent. Use deterministic_only for tests.",
    )
    md_expert_agent_review.add_argument(
        "--no-agent-approval",
        action="store_true",
        help="Do not persist an approval record when the agent returns approved.",
    )
    md_expert_agent_review.add_argument("--reviewer-name", default="md_expert_review_agent")
    md_expert_agent_review.add_argument("--reviewer-contact", default="agent://md_expert_review_agent")

    evidence_gap_resolver = subparsers.add_parser(
        "resolve-evidence-gaps",
        help="Convert validation-agent missing evidence, risks, and next actions into research leads",
    )
    evidence_gap_resolver.add_argument("--queue-item-id", action="append", default=[], help="Validation queue item id; repeat for multiple")
    evidence_gap_resolver.add_argument("--plan-id", default=None, help="Optional validation plan id filter")
    evidence_gap_resolver.add_argument("--status", action="append", default=[], help="Validation queue status; repeat for multiple")
    evidence_gap_resolver.add_argument("--decision", action="append", default=[], help="Validation decision to resolve; repeat for multiple")
    evidence_gap_resolver.add_argument("--task-type", action="append", default=[], help="Validation task type; repeat for multiple")
    evidence_gap_resolver.add_argument("--gap-type", action="append", default=[], help="missing_evidence, risk, or next_action")
    evidence_gap_resolver.add_argument("--limit", type=int, default=25)
    evidence_gap_resolver.add_argument("--max-gaps-per-item", type=int, default=8)
    evidence_gap_resolver.add_argument("--priority", type=int, default=30)
    evidence_gap_resolver.add_argument("--queue-briefs", action="store_true", help="Also queue research briefs for created leads")
    evidence_gap_resolver.add_argument("--apply", action="store_true", help="Persist leads. Without this flag the command is a dry run.")

    validation_gap_source_pack = subparsers.add_parser(
        "validation-gap-source-pack",
        help="Build targeted source queries for validation evidence gaps",
    )
    validation_gap_source_pack.add_argument(
        "--queue-item-id",
        action="append",
        default=[],
        help="Validation queue item id; repeat for multiple.",
    )
    validation_gap_source_pack.add_argument("--lead-id", action="append", default=[], help="Research lead id; repeat for multiple.")
    validation_gap_source_pack.add_argument(
        "--lead-status",
        action="append",
        default=[],
        help="Lead status to scan when no lead id is supplied. Defaults to new and followup.",
    )
    validation_gap_source_pack.add_argument("--source", action="append", default=[], help="Source key to target; repeat for multiple.")
    validation_gap_source_pack.add_argument("--lane", action="append", default=[], help="Evidence lane to include; repeat for multiple.")
    validation_gap_source_pack.add_argument("--limit", type=int, default=25)
    validation_gap_source_pack.add_argument("--max-queries-per-lane", type=int, default=3)
    validation_gap_source_pack.add_argument("--persist-queries", action="store_true", help="Persist generated SourceQuery rows.")
    validation_gap_source_pack.add_argument("--inactive", action="store_true", help="Persist generated SourceQuery rows as inactive.")
    validation_gap_source_pack.add_argument("--apply", action="store_true", help="Persist generated queries. Without this flag the command is a dry run.")

    validation_gap_ingest = subparsers.add_parser(
        "validation-gap-ingest",
        help="Ingest active validation-gap SourceQuery rows only",
    )
    validation_gap_ingest.add_argument("--source", action="append", default=[], help="Source key to ingest; repeat for multiple.")
    validation_gap_ingest.add_argument("--query-name", action="append", default=[], help="Specific query name; repeat for multiple.")
    validation_gap_ingest.add_argument(
        "--track",
        action="append",
        default=[],
        help="SourceQuery track to ingest; defaults to validation_gap. Repeat for multiple tracks.",
    )
    validation_gap_ingest.add_argument("--followup-lane", default=None, help="Internal follow-up lane filter.")
    validation_gap_ingest.add_argument("--origin-review-id", action="append", default=[], help="Origin evaluator review ID filter.")
    validation_gap_ingest.add_argument("--origin-agent-run-id", action="append", default=[], help="Origin agent run ID filter.")
    validation_gap_ingest.add_argument("--limit-per-query", type=int, default=5)
    validation_gap_ingest.add_argument("--max-queries", type=int, default=50)
    validation_gap_ingest.add_argument("--apply", action="store_true", help="Run ingestion. Without this flag the command is a dry run.")

    repair_pubmed_identifiers = subparsers.add_parser(
        "repair-pubmed-identifiers",
        help="Refresh PubMed DOI/PMCID metadata and move PubMed rows to PMID dedupe keys",
    )
    repair_pubmed_identifiers.add_argument("--pmid", action="append", default=[], help="Specific PMID; repeat for multiple.")
    repair_pubmed_identifiers.add_argument("--limit", type=int, default=250)
    repair_pubmed_identifiers.add_argument("--batch-size", type=int, default=100)
    repair_pubmed_identifiers.add_argument("--apply", action="store_true", help="Apply repairs. Without this flag the command is a dry run.")

    research_followup_loop = subparsers.add_parser(
        "research-followup-loop",
        help="Run the manual repair loop for one research lead: ingest, resolve, and optionally OpenRouter-evaluate.",
    )
    research_followup_loop.add_argument("--lead-id", required=True, help="Research lead ID")
    research_followup_loop.add_argument("--source", action="append", default=[], help="Source key to ingest; repeat for multiple.")
    research_followup_loop.add_argument("--query-name", action="append", default=[], help="Specific query name; repeat for multiple.")
    research_followup_loop.add_argument("--search-query-text", default=None, help="Optional resolver live-search query override.")
    research_followup_loop.add_argument("--followup-lane", default="agent_evaluator_followup")
    research_followup_loop.add_argument("--limit-per-query", type=int, default=2)
    research_followup_loop.add_argument("--max-queries", type=int, default=10)
    research_followup_loop.add_argument("--no-ingest", action="store_true")
    research_followup_loop.add_argument("--resolve", action="store_true", help="Re-run the follow-up resolver after ingestion.")
    research_followup_loop.add_argument("--evaluate", action="store_true", help="Run OpenRouter evaluator on the resolver run.")
    research_followup_loop.add_argument("--no-identifier-followups", action="store_true")
    research_followup_loop.add_argument("--no-ingest-identifier-followups", action="store_true")
    research_followup_loop.add_argument("--no-claim-extraction", action="store_true")
    research_followup_loop.add_argument("--max-identifier-followups", type=int, default=8)
    research_followup_loop.add_argument("--apply", action="store_true", help="Apply changes. Without this flag the command is a dry run.")
    research_followup_loop.add_argument("--no-force-live-search", action="store_true")
    research_followup_loop.add_argument("--search-limit-per-source", type=int, default=2)
    research_followup_loop.add_argument("--min-evidence-chunks", type=int, default=1)
    research_followup_loop.add_argument("--model-profile", default="agent_performance_evaluator")
    research_followup_loop.add_argument("--review-model", action="append", default=[])
    research_followup_loop.add_argument("--estimated-evaluator-cost-usd", type=float, default=0.03)
    research_followup_loop.add_argument("--operator", default="cli_operator")

    research_hunt_tasks = subparsers.add_parser(
        "research-hunt-tasks",
        help="Execute open research hunt tasks from lead metadata.",
    )
    research_hunt_tasks.add_argument("--lead-id", action="append", default=[], help="Research lead ID; repeat for multiple.")
    research_hunt_tasks.add_argument("--task-id", action="append", default=[], help="Specific hunt task ID; repeat for multiple.")
    research_hunt_tasks.add_argument("--task-type", action="append", default=[], help="Task type filter, e.g. claim_extract.")
    research_hunt_tasks.add_argument("--status", action="append", default=[], help="Task status filter. Defaults to open.")
    research_hunt_tasks.add_argument("--source", action="append", default=[], help="Source key override for resolver-style tasks.")
    research_hunt_tasks.add_argument("--limit", type=int, default=5)
    research_hunt_tasks.add_argument("--claim-chunk-limit", type=int, default=25)
    research_hunt_tasks.add_argument("--apply", action="store_true", help="Apply task execution. Without this flag the command is a dry run.")
    research_hunt_tasks.add_argument("--no-evaluate", action="store_true")
    research_hunt_tasks.add_argument("--no-force-live-search", action="store_true")
    research_hunt_tasks.add_argument("--include-broad-tasks", action="store_true", help="Include broad/meta hunt tasks when no explicit broad task type or task ID is provided.")
    research_hunt_tasks.add_argument("--allow-broad-task-fanout", action="store_true", help="Allow broad/meta tasks to create additional broad/meta tasks.")
    research_hunt_tasks.add_argument("--search-limit-per-source", type=int, default=1)
    research_hunt_tasks.add_argument("--model-profile", default="agent_performance_evaluator")
    research_hunt_tasks.add_argument("--review-model", action="append", default=[])
    research_hunt_tasks.add_argument("--operator", default="cli_operator")

    research_hunt_queue_report = subparsers.add_parser(
        "research-hunt-queue-report",
        help="Build a read-only research hunt queue control report.",
    )
    research_hunt_queue_report.add_argument("--lead-id", action="append", default=[], help="Research lead ID; repeat for multiple.")
    research_hunt_queue_report.add_argument("--lead-status", action="append", default=[], help="Lead status filter. Defaults to active hunt statuses.")
    research_hunt_queue_report.add_argument("--source", action="append", default=[], help="Source key filter.")
    research_hunt_queue_report.add_argument("--limit", type=int, default=100)
    research_hunt_queue_report.add_argument("--task-limit", type=int, default=250)
    research_hunt_queue_report.add_argument("--stale-after-hours", type=int, default=72)
    research_hunt_queue_report.add_argument("--no-tasks", action="store_true", help="Omit task rows from the report.")
    research_hunt_queue_report.add_argument("--no-suppressed", action="store_true", help="Omit suppressed task rows from the report.")

    research_hunt_queue_maintenance = subparsers.add_parser(
        "research-hunt-queue-maintenance",
        help="Dry-run or apply safe research hunt queue cleanup.",
    )
    research_hunt_queue_maintenance.add_argument("--lead-id", action="append", default=[], help="Research lead ID; repeat for multiple.")
    research_hunt_queue_maintenance.add_argument("--lead-status", action="append", default=[], help="Lead status filter. Defaults to active hunt statuses.")
    research_hunt_queue_maintenance.add_argument("--source", action="append", default=[], help="Source key filter.")
    research_hunt_queue_maintenance.add_argument(
        "--reason",
        action="append",
        default=[],
        help="Maintenance reason to allow; repeat. Defaults to all safe reasons.",
    )
    research_hunt_queue_maintenance.add_argument("--stale-after-hours", type=int, default=72)
    research_hunt_queue_maintenance.add_argument("--limit", type=int, default=50)
    research_hunt_queue_maintenance.add_argument("--apply", action="store_true", help="Apply suppressions. Without this flag the command is a dry run.")
    research_hunt_queue_maintenance.add_argument("--operator", default="cli_operator")

    queue_ready_hunt_synthesis = subparsers.add_parser(
        "queue-ready-research-hunt-synthesis",
        help="Queue ready research-hunt leads into the research brief synthesis lane.",
    )
    queue_ready_hunt_synthesis.add_argument("--lead-id", action="append", default=[], help="Research lead ID; repeat for multiple.")
    queue_ready_hunt_synthesis.add_argument("--lead-status", action="append", default=[], help="Lead status filter. Defaults to active hunt statuses.")
    queue_ready_hunt_synthesis.add_argument("--source", action="append", default=[], help="Source key filter.")
    queue_ready_hunt_synthesis.add_argument("--limit", type=int, default=10, help="Maximum ready leads to queue.")
    queue_ready_hunt_synthesis.add_argument(
        "--disease-scope",
        default="canine hemangiosarcoma and human angiosarcoma",
        help="Disease/scope guardrail for retrieval and synthesis.",
    )
    queue_ready_hunt_synthesis.add_argument("--priority", type=int, default=40, help="Default queue priority.")
    queue_ready_hunt_synthesis.add_argument("--max-chunks", type=int, default=10, help="Chunks per perspective search.")
    queue_ready_hunt_synthesis.add_argument("--max-claims", type=int, default=20, help="Claims to include.")
    queue_ready_hunt_synthesis.add_argument(
        "--max-chunk-chars",
        type=int,
        default=2200,
        help="Maximum chars per cited chunk.",
    )
    queue_ready_hunt_synthesis.add_argument(
        "--brief-style",
        choices=("technical", "operator", "substack", "vet_partner"),
        default="technical",
    )
    queue_ready_hunt_synthesis.add_argument("--model-profile", default="research_brief", help="Logical model profile.")
    queue_ready_hunt_synthesis.add_argument(
        "--review-mode",
        choices=("external_required", "openrouter_required", "openrouter_compare", "deterministic_only"),
        default="openrouter_required",
    )
    queue_ready_hunt_synthesis.add_argument(
        "--review-model",
        action="append",
        default=[],
        help="OpenRouter model id; repeat to compare multiple models.",
    )
    queue_ready_hunt_synthesis.add_argument(
        "--no-handoff-docs",
        action="store_true",
        help="Skip plain-language synthesis handoff document generation.",
    )
    queue_ready_hunt_synthesis.add_argument("--apply", action="store_true", help="Persist queue items. Without this flag the command is a dry run.")
    queue_ready_hunt_synthesis.add_argument("--no-transition-leads", action="store_true", help="Do not mark queued leads as queued.")
    queue_ready_hunt_synthesis.add_argument("--operator", default="cli_operator", help="Operator identity recorded in metadata.")

    research_hunt_synthesis_doc = subparsers.add_parser(
        "research-hunt-synthesis-doc",
        help="Create plain-language synthesis handoff docs for ready research-hunt leads.",
    )
    research_hunt_synthesis_doc.add_argument("--lead-id", action="append", default=[], help="Research lead ID; repeat for multiple.")
    research_hunt_synthesis_doc.add_argument("--lead-status", action="append", default=[], help="Lead status filter. Defaults to active/queued hunt statuses.")
    research_hunt_synthesis_doc.add_argument("--source", action="append", default=[], help="Source key filter.")
    research_hunt_synthesis_doc.add_argument("--limit", type=int, default=10, help="Maximum ready leads to document.")
    research_hunt_synthesis_doc.add_argument("--max-claims", type=int, default=16, help="Maximum claims to include.")
    research_hunt_synthesis_doc.add_argument("--max-chunks", type=int, default=12, help="Maximum cited chunks to footnote.")
    research_hunt_synthesis_doc.add_argument("--max-chunk-chars", type=int, default=900, help="Maximum chars per chunk footnote.")
    research_hunt_synthesis_doc.add_argument("--max-technical-footnotes", type=int, default=30, help="Maximum technical footnotes.")
    research_hunt_synthesis_doc.add_argument("--no-technical-footnotes", action="store_true", help="Create the plain-language doc without technical footnotes.")
    research_hunt_synthesis_doc.add_argument("--apply", action="store_true", help="Persist artifact records. Without this flag the command is a dry run.")
    research_hunt_synthesis_doc.add_argument("--operator", default="cli_operator", help="Operator identity recorded in metadata.")

    model_review_summary = subparsers.add_parser(
        "model-review-summary",
        help="Print compact model-review summaries from persisted agent runs",
    )
    model_review_summary.add_argument("--id", default=None, help="Optional agent_run_id to summarize")
    model_review_summary.add_argument("--agent-name", default="full_text_ops_agent", help="Agent name filter")
    model_review_summary.add_argument("--status", default=None, help="Optional status filter")
    model_review_summary.add_argument("--source", default=None, help="Optional source key filter")
    model_review_summary.add_argument("--limit", type=int, default=3, help="Maximum runs to summarize")

    retrieval_smoke = subparsers.add_parser(
        "retrieval-smoke",
        help="Smoke-test retrieval reads: search chunks, fetch context, fetch parent object",
    )
    retrieval_smoke.add_argument(
        "--query",
        default="hemangiosarcoma angiogenesis",
        help="Retrieval query to search against stored chunks",
    )
    retrieval_smoke.add_argument("--source", default=None, help="Optional source key filter")
    retrieval_smoke.add_argument("--object-type", default=None, help="Optional research object type filter")
    retrieval_smoke.add_argument("--embedding-model", default=None, help="Optional embedding model filter")
    retrieval_smoke.add_argument("--limit", type=int, default=3, help="Maximum chunk search hits")
    retrieval_smoke.add_argument("--max-chunk-chars", type=int, default=1200, help="Maximum returned chars per chunk")
    retrieval_smoke.add_argument("--context-window", type=int, default=1, help="Neighbor chunks to return on each side")
    retrieval_smoke.add_argument("--no-entity-mentions", action="store_true", help="Omit entity mentions from context")
    retrieval_smoke.add_argument("--no-keyword-fallback", action="store_true", help="Fail open only to embedding search")
    retrieval_smoke.add_argument("--require-embedding", action="store_true", help="Fail if search does not use embeddings")
    retrieval_smoke.add_argument("--fail-on-error", action="store_true", help="Exit non-zero if the smoke check fails")

    embedding_index = subparsers.add_parser(
        "embedding-index",
        help="Index stored document chunks for retrieval",
    )
    embedding_index.add_argument("--source", default=None, help="Optional source key filter")
    embedding_index.add_argument(
        "--embedding-model",
        default=None,
        help="Embedding model to write; defaults to HSA_EMBEDDING_MODEL, then OpenRouter large when configured",
    )
    embedding_index.add_argument("--limit", type=int, default=None, help="Optional maximum chunks to inspect")
    embedding_index.add_argument("--force", action="store_true", help="Rebuild existing embeddings")
    embedding_index.add_argument("--batch-size", type=int, default=32, help="Embedding request batch size")

    embedding_maintenance = subparsers.add_parser(
        "embedding-maintenance",
        help="Prune orphan embeddings and report active-model coverage",
    )
    embedding_maintenance.add_argument("--source", default=None, help="Optional source key filter")
    embedding_maintenance.add_argument("--object-type", default=None, help="Optional research object type filter")
    embedding_maintenance.add_argument(
        "--embedding-model",
        default=None,
        help="Active embedding model to enforce coverage against; defaults to HSA_EMBEDDING_MODEL, then OpenRouter large when configured",
    )
    embedding_maintenance.add_argument(
        "--prune-model",
        default=None,
        help="Optional embedding model to prune; defaults to all models",
    )
    embedding_maintenance.add_argument("--dry-run", action="store_true", help="Count orphan rows without deleting them")
    embedding_maintenance.add_argument("--fail-on-error", action="store_true", help="Exit non-zero if maintenance fails")

    embedding_bakeoff = subparsers.add_parser(
        "embedding-bakeoff",
        help="Compare embedding models against retrieval benchmarks",
    )
    embedding_bakeoff.add_argument(
        "--embedding-model",
        action="append",
        default=[],
        help="Embedding model to compare; repeat for multiple. Defaults to local hash plus OpenRouter small/large.",
    )
    embedding_bakeoff.add_argument("--source", default=None, help="Optional source key filter")
    embedding_bakeoff.add_argument("--limit", type=int, default=5, help="Search hits per benchmark")
    embedding_bakeoff.add_argument("--index-missing", action="store_true", help="Index missing embeddings before scoring")
    embedding_bakeoff.add_argument("--index-limit", type=int, default=None, help="Optional max chunks to index per model")
    embedding_bakeoff.add_argument("--force", action="store_true", help="Force reindex before scoring")
    embedding_bakeoff.add_argument("--batch-size", type=int, default=32, help="Embedding request batch size")

    resolve_entities = subparsers.add_parser(
        "resolve-entities",
        help="Run deterministic entity resolution over persisted chunks",
    )
    resolve_entities.add_argument("--source", default=None, help="Optional source key filter")
    resolve_entities.add_argument("--limit", type=int, default=None, help="Maximum chunks to resolve")
    resolve_entities.add_argument(
        "--resolver-profile",
        choices=["local", "pubtator", "local_plus_pubtator"],
        default="local",
        help="Deterministic resolver profile to run",
    )

    backfill_papers = subparsers.add_parser("backfill-papers", help="Backfill legacy papers JSON")
    backfill_papers.add_argument("--path", default="hsa_research/papers.json", help="Path to papers JSON")
    backfill_papers.add_argument("--limit", type=int, default=None, help="Optional record limit")
    backfill_papers.add_argument("--no-chunk", action="store_true", help="Skip chunk creation")

    backfill_knowledge = subparsers.add_parser("backfill-deep-dives", help="Backfill TWOG deep-dive markdown knowledge")
    backfill_knowledge.add_argument("--dir", default="docs/deep_dives", help="Directory of markdown files")
    backfill_knowledge.add_argument("--limit", type=int, default=None, help="Optional file limit")
    backfill_knowledge.add_argument("--no-chunk", action="store_true", help="Skip chunk creation")

    extract_claims = subparsers.add_parser("extract-claims", help="Extract draft claims from local chunks")
    extract_claims.add_argument("--source", default=None, help="Optional source key filter")
    extract_claims.add_argument("--object-type", default=None, help="Optional research object type filter")
    extract_claims.add_argument("--limit", type=int, default=None, help="Optional chunk limit")

    curate_claims = subparsers.add_parser("curate-claims", help="Curate, dedupe, and promote draft claims")
    curate_claims.add_argument("--source", default=None, help="Optional source key filter")
    curate_claims.add_argument("--query", default=None, help="Optional claim statement query filter")
    curate_claims.add_argument("--limit", type=int, default=100, help="Maximum draft claims to review")
    curate_claims.add_argument("--min-confidence", type=float, default=0.0, help="Minimum draft confidence")
    curate_claims.add_argument("--promote-threshold", type=float, default=0.5, help="Promotion score threshold")
    curate_claims.add_argument("--model-profile", default="reviewer", help="Logical model profile for this agent")
    curate_claims.add_argument("--include-seed-claims", action="store_true", help="Include seed claims")
    curate_claims.add_argument("--dry-run", action="store_true", help="Preview decisions without writing them")
    curate_claims.add_argument("--summary-only", action="store_true", help="Omit per-claim decisions from output")

    scout_sources = subparsers.add_parser("scout-sources", help="Prioritize ingestion source gaps")
    scout_sources.add_argument(
        "--focus",
        choices=["all", "scholarly", "canine", "omics", "chemistry", "structure", "safety"],
        default="all",
        help="Limit scouting to one source family",
    )
    scout_sources.add_argument("--max-phase", type=int, default=3, help="Maximum implementation phase to include")
    scout_sources.add_argument("--limit", type=int, default=12, help="Maximum recommendations")
    scout_sources.add_argument("--model-profile", default="long_context_reviewer", help="Logical model profile")
    scout_sources.add_argument("--no-registered", action="store_true", help="Skip already registered sources")
    scout_sources.add_argument("--no-expansion", action="store_true", help="Skip not-yet-registered expansion sources")
    scout_sources.add_argument("--summary-only", action="store_true", help="Omit per-source query details from output")

    subparsers.add_parser("scrape-profiles", help="List configured scraper source profiles")

    review_profile = subparsers.add_parser("review-scrape-profile", help="Record source profile review before fetch")
    review_profile.add_argument("--source", required=True, help="Scrape source key, e.g. avma_vctr")
    review_profile.add_argument(
        "--robots-policy",
        choices=["unknown", "reviewed", "disallow", "manual_only"],
        required=True,
        help="Reviewed robots/TOS policy",
    )
    review_profile.add_argument("--approve-fetch", action="store_true", help="Approve controlled fetches for this source")
    review_profile.add_argument("--reviewed-by", required=True, help="Reviewer identifier")
    review_profile.add_argument("--review-note", default=None, help="Optional review note")
    review_profile.add_argument("--allowed-url-pattern", action="append", default=[], help="Allowed URL pattern override")
    review_profile.add_argument("--storage-policy", default=None, help="Storage policy override")

    fetch_scrape = subparsers.add_parser("fetch-scrape", help="Fetch approved URLs through the scraper bridge")
    fetch_scrape.add_argument("--source", required=True, help="Scrape source key, e.g. avma_vctr")
    fetch_scrape.add_argument("--url", action="append", required=True, help="URL to fetch; can be repeated")
    fetch_scrape.add_argument("--max-pages", type=int, default=10, help="Maximum pages to fetch")
    fetch_scrape.add_argument("--approved-by", default=None, help="Required for approval-gated scrape profiles")
    fetch_scrape.add_argument("--approval-note", default=None, help="Optional approval note")

    parse_scrape = subparsers.add_parser("parse-scrape", help="Parse stored scrape artifacts into draft records")
    parse_scrape.add_argument("--source", required=True, help="Scrape source key, e.g. avma_vctr")
    parse_scrape.add_argument("--limit", type=int, default=None, help="Optional artifact limit")

    build_manifest = subparsers.add_parser("build-scrape-manifest", help="Discover candidate scrape URLs from seed pages")
    build_manifest.add_argument("--source", required=True, help="Scrape source key, e.g. avma_vctr")
    build_manifest.add_argument("--seed-url", action="append", default=[], help="Seed/list URL; can be repeated")
    build_manifest.add_argument("--fetch-seed-pages", action="store_true", help="Fetch seed pages before discovery")
    build_manifest.add_argument("--max-seed-pages", type=int, default=5, help="Maximum seed artifacts/pages")
    build_manifest.add_argument("--max-candidate-urls", type=int, default=50, help="Maximum candidate URLs")
    build_manifest.add_argument("--approved-by", default=None, help="Required if fetching seed pages")
    build_manifest.add_argument("--approval-note", default=None, help="Optional approval note")

    fetch_manifest = subparsers.add_parser("fetch-scrape-manifest", help="Fetch candidate URLs from a manifest artifact")
    fetch_manifest.add_argument("--source", required=True, help="Scrape source key, e.g. avma_vctr")
    fetch_manifest.add_argument("--manifest-artifact-id", required=True, help="Manifest artifact ID")
    fetch_manifest.add_argument("--max-pages", type=int, default=10, help="Maximum candidate pages to fetch")
    fetch_manifest.add_argument("--approved-by", default=None, help="Required for approval-gated profiles")
    fetch_manifest.add_argument("--approval-note", default=None, help="Optional approval note")

    list_scrape_reviews = subparsers.add_parser("list-scrape-reviews", help="List parsed scrape review records")
    list_scrape_reviews.add_argument("--source", required=True, help="Scrape source key, e.g. avma_vctr")
    list_scrape_reviews.add_argument(
        "--status",
        choices=["needs_review", "accepted", "rejected"],
        default=None,
        help="Optional review status filter",
    )
    list_scrape_reviews.add_argument("--limit", type=int, default=50, help="Maximum review records")

    review_scrape = subparsers.add_parser("review-scrape", help="Accept or reject parsed scrape records")
    review_scrape.add_argument("--source", required=True, help="Scrape source key, e.g. avma_vctr")
    review_scrape.add_argument("--review-id", action="append", required=True, help="Review record ID; can be repeated")
    review_scrape.add_argument(
        "--decision",
        choices=["needs_review", "accepted", "rejected"],
        required=True,
        help="Review decision",
    )
    review_scrape.add_argument("--reviewed-by", required=True, help="Reviewer identifier")
    review_scrape.add_argument("--review-note", default=None, help="Optional review note")

    ingest_scrape = subparsers.add_parser("ingest-scrape", help="Promote parsed scrape artifacts after review")
    ingest_scrape.add_argument("--source", required=True, help="Scrape source key, e.g. avma_vctr")
    ingest_scrape.add_argument("--review-id", action="append", default=[], help="Accepted review ID to ingest")
    ingest_scrape.add_argument("--artifact-id", action="append", default=[], help="Artifact ID to ingest; can be repeated")
    ingest_scrape.add_argument("--limit", type=int, default=None, help="Optional artifact limit when no IDs are supplied")
    ingest_scrape.add_argument("--min-parser-confidence", type=float, default=0.3, help="Minimum parser confidence")
    ingest_scrape.add_argument("--approved-by", default=None, help="Required to promote scrape records")
    ingest_scrape.add_argument("--approval-note", default=None, help="Optional approval note")

    args = parser.parse_args()
    repo = SQLiteResearchRepository(args.db) if args.db else build_sql_repository()
    pipeline = LocalIngestionPipeline(repo)

    if args.command == "init":
        output = pipeline.initialize()
    elif args.command == "coverage":
        output = pipeline.coverage()
    elif args.command == "ingest":
        query = SourceQuery(
            source_key=args.source,
            query_name=args.query_name,
            query_text=args.query,
            track=args.track,
        )
        output = pipeline.ingest_query(query, limit=args.limit).model_dump(mode="json")
    elif args.command == "ingest-source":
        output = [result.model_dump(mode="json") for result in pipeline.ingest_source(args.source, limit=args.limit)]
    elif args.command == "structured-pipeline":
        selected_sources = args.source or list(STRUCTURED_SOURCE_KEYS)
        source_limits = {source_key: args.limit for source_key in selected_sources} if args.limit is not None else None
        output = run_structured_sources_pipeline(
            repo,
            source_keys=selected_sources,
            source_limits=source_limits,
            extract_limit=args.extract_limit,
            curate_limit=args.curate_limit,
            promote_threshold=args.promote_threshold,
            initialize=not args.no_init,
        )
    elif args.command == "structured-report":
        selected_sources = args.source or list(STRUCTURED_SOURCE_KEYS)
        output = build_structured_source_count_report(
            repo,
            source_keys=selected_sources,
            sample_limit=args.sample_limit,
            require_claims=not args.no_require_claims,
        )
    elif args.command == "source-health":
        selected_sources = args.source or list(ALL_API_SOURCE_KEYS)
        output = build_source_health_report(
            repo,
            source_keys=selected_sources,
            sample_limit=args.sample_limit,
            metadata_claim_limit=args.metadata_claim_limit,
            min_health_score=args.min_health_score,
            require_claims=not args.no_require_claims,
        )
    elif args.command == "command-center":
        output = HSAResearchService(repo).build_command_center_report(
            CommandCenterRequest(
                source_keys=args.source,
                include_source_health=not args.no_source_health,
                include_recent_agents=not args.no_recent_agents,
                queue_limit=args.queue_limit,
                lead_limit=args.lead_limit,
                agent_run_limit=args.agent_run_limit,
                min_health_score=args.min_health_score,
                require_claims=not args.no_require_claims,
            )
        ).model_dump(mode="json")
    elif args.command == "command-center-web":
        from .command_center_web import run_server

        run_server(host=args.host, port=args.port, service_factory=lambda: HSAResearchService(repo))
        return
    elif args.command == "triage-full-text":
        output = HSAResearchService(repo).triage_full_text_issue(
            FullTextTriageRequest(
                source_key=args.source,
                stage=args.stage,
                query_name=args.query_name,
                error_message=args.error[0] if args.error else None,
                errors=args.error[1:],
                runtime_seconds=args.runtime_seconds,
                timeout_seconds=args.timeout_seconds,
                raw_records=args.raw_records,
                research_objects=args.research_objects,
                document_chunks=args.document_chunks,
                full_text_document_chunks=args.full_text_document_chunks,
                full_text_body_chars=args.full_text_body_chars,
                claims=args.claims,
                entity_mentions=args.entity_mentions,
                current_failed_runs=args.current_failed_run,
                http_status=args.http_status,
                model_profile=args.model_profile,
            )
        ).model_dump(mode="json")
    elif args.command == "full-text-ops":
        output = HSAResearchService(repo).run_full_text_ops(
            FullTextOpsRequest(
                source_keys=args.source,
                partition_date=args.partition_date,
                recent_run_limit=args.recent_run_limit,
                model_profile=args.model_profile,
                review_mode=args.review_mode,
                review_models=args.review_model,
            )
        ).model_dump(mode="json")
    elif args.command == "research-brief":
        output = HSAResearchService(repo).run_research_brief(
            ResearchBriefRequest(
                topic=args.topic,
                disease_scope=args.disease_scope,
                source_key=args.source,
                max_chunks_per_perspective=args.max_chunks,
                max_claims=args.max_claims,
                max_chunk_chars=args.max_chunk_chars,
                brief_style=args.brief_style,
                model_profile=args.model_profile,
                review_mode=args.review_mode,
                review_models=args.review_model,
            )
        ).model_dump(mode="json")
    elif args.command == "research-briefs":
        service = HSAResearchService(repo)
        if args.id:
            record = service.get_research_brief(UUID(args.id))
            output = {} if record is None else record.model_dump(mode="json")
        else:
            output = [
                record.model_dump(mode="json")
                for record in service.list_research_briefs(
                    status=args.status,
                    source_key=args.source,
                    topic_query=args.topic_query,
                    limit=args.limit,
                )
            ]
    elif args.command == "research-brief-operator-doc":
        output = HSAResearchService(repo).create_research_brief_operator_docs(
            ResearchBriefOperatorDocRequest(
                brief_ids=[UUID(value) for value in args.brief_id],
                status=args.status,
                source_key=args.source,
                topic_query=args.topic_query,
                limit=args.limit,
                max_hypotheses=args.max_hypotheses,
                max_unresolved_questions=args.max_unresolved_questions,
                max_evidence_limitations=args.max_evidence_limitations,
                max_technical_footnotes=args.max_technical_footnotes,
                include_technical_footnotes=not args.no_technical_footnotes,
                dry_run=not args.apply,
                operator=args.operator,
            )
        ).model_dump(mode="json")
    elif args.command == "queue-research-brief":
        output = HSAResearchService(repo).queue_research_brief(
            ResearchBriefQueueRequest(
                topic=args.topic,
                disease_scope=args.disease_scope,
                source_key=args.source,
                priority=args.priority,
                max_chunks_per_perspective=args.max_chunks,
                max_claims=args.max_claims,
                max_chunk_chars=args.max_chunk_chars,
                brief_style=args.brief_style,
                model_profile=args.model_profile,
                review_mode=args.review_mode,
                review_models=args.review_model,
            )
        ).model_dump(mode="json")
    elif args.command == "queue-research-brief-batch":
        output = HSAResearchService(repo).queue_research_brief_batch(
            ResearchBriefQueueBatchRequest(
                mode=args.mode,
                lead_statuses=args.lead_status or ["new", "watching"],
                lead_types=args.lead_type,
                source_keys=args.source,
                source_health_statuses=args.source_health_status or ["failing", "triage", "watch"],
                include_empty_sources=args.include_empty_sources,
                limit=args.limit,
                disease_scope=args.disease_scope,
                priority=args.priority,
                max_chunks_per_perspective=args.max_chunks,
                max_claims=args.max_claims,
                max_chunk_chars=args.max_chunk_chars,
                brief_style=args.brief_style,
                model_profile=args.model_profile,
                review_mode=args.review_mode,
                review_models=args.review_model,
            )
        ).model_dump(mode="json")
    elif args.command == "research-brief-queue":
        service = HSAResearchService(repo)
        if args.id:
            item = service.get_research_brief_queue_item(UUID(args.id))
            output = {} if item is None else item.model_dump(mode="json")
        else:
            output = [
                item.model_dump(mode="json")
                for item in service.list_research_brief_queue_items(
                    status=args.status,
                    source_key=args.source,
                    topic_query=args.topic_query,
                    limit=args.limit,
                )
            ]
    elif args.command == "requeue-research-brief-queue-item":
        item = HSAResearchService(repo).requeue_research_brief_queue_item(
            UUID(args.id),
            priority=args.priority,
        )
        output = {} if item is None else item.model_dump(mode="json")
    elif args.command == "archive-research-brief-queue-item":
        item = HSAResearchService(repo).archive_research_brief_queue_item(UUID(args.id))
        output = {} if item is None else item.model_dump(mode="json")
    elif args.command == "maintain-research-brief-queue":
        output = HSAResearchService(repo).maintain_research_brief_queue(
            ResearchBriefQueueMaintenanceRequest(
                queue_item_ids=[UUID(value) for value in args.id],
                statuses=args.status or ["failed"],
                source_key=args.source,
                topic_query=args.topic_query,
                min_attempts=args.min_attempts,
                max_updated_age_hours=args.max_updated_age_hours,
                limit=args.limit,
                dry_run=not args.apply,
                reason=args.reason,
            )
        ).model_dump(mode="json")
    elif args.command == "run-research-brief-queue":
        output = HSAResearchService(repo).run_next_research_brief_queue_item(
            ResearchBriefQueueRunRequest(
                queue_item_ids=[UUID(value) for value in args.id],
                statuses=args.status or ["queued"],
                source_key=args.source,
                topic_query=args.topic_query,
                limit=args.limit,
            )
        ).model_dump(mode="json")
    elif args.command == "candidate-contribution-intake":
        output = build_candidate_contribution_intake_report(
            statuses=args.status or None,
            candidate_ids=args.candidate_id,
            limit=args.limit,
            include_packet=args.include_packet,
        )
    elif args.command == "triage-candidate-contribution":
        output = triage_candidate_contributions(
            contribution_ids=args.id,
            action=args.action,
            operator=args.operator,
            review_notes=args.review_notes,
            dry_run=not args.apply,
        )
    elif args.command == "research-brief-playground-pack":
        output = HSAResearchService(repo).build_research_brief_playground_pack(
            ResearchBriefRequest(
                topic=args.topic,
                disease_scope=args.disease_scope,
                source_key=args.source,
                max_chunks_per_perspective=args.max_chunks,
                max_claims=args.max_claims,
                max_chunk_chars=args.max_chunk_chars,
                brief_style=args.brief_style,
                model_profile=args.model_profile,
                review_mode="external_required",
            )
        ).model_dump(mode="json")
    elif args.command == "therapy-committee":
        output = HSAResearchService(repo).run_therapy_committee(
            TherapyCommitteeRequest(
                topic=args.topic,
                disease_scope=args.disease_scope,
                source_key=args.source,
                max_chunks_per_perspective=args.max_chunks,
                max_claims=args.max_claims,
                max_chunk_chars=args.max_chunk_chars,
                max_ideas_per_perspective=args.max_ideas if args.max_ideas is not None else 4,
                max_ranked_ideas=(
                    args.max_ranked_ideas if args.max_ranked_ideas is not None else args.max_ideas
                ),
                model_profile=args.model_profile,
                review_mode=args.review_mode,
                review_models=args.review_model,
                program_id=UUID(args.program_id) if args.program_id else None,
                brief_id=UUID(args.brief_id) if args.brief_id else None,
                evaluation_id=UUID(args.evaluation_id) if args.evaluation_id else None,
            )
        ).model_dump(mode="json")
    elif args.command == "validation-tool-catalog":
        output = HSAResearchService(repo).list_validation_tool_catalog(
            ValidationToolCatalogRequest(
                category=args.category,
                runner_status=args.runner_status,
                validation_type=args.validation_type,
                task_type=args.task_type,
                query=args.query,
                limit=args.limit,
            )
        ).model_dump(mode="json")
    elif args.command == "match-validation-tool":
        output = HSAResearchService(repo).match_validation_tools(
            ValidationToolMatchRequest(
                validation_type=args.validation_type,
                task_type=args.task_type,
                objective=args.objective,
                candidate_name=args.candidate_name,
                target_name=args.target_name,
                species=args.species,
                required_inputs=args.required_input,
                runner_status=args.runner_status,
                limit=args.limit,
            )
        ).model_dump(mode="json")
    elif args.command == "therapy-idea-library":
        output = HSAResearchService(repo).list_therapy_ideas(
            TherapyIdeaLibraryRequest(
                therapy_idea_id=UUID(args.id) if args.id else None,
                status=args.status,
                source_program_id=UUID(args.program_id) if args.program_id else None,
                source_brief_id=UUID(args.brief_id) if args.brief_id else None,
                source_evaluation_id=UUID(args.evaluation_id) if args.evaluation_id else None,
                committee_run_id=UUID(args.committee_run_id) if args.committee_run_id else None,
                topic_query=args.query,
                limit=args.limit,
            )
        ).model_dump(mode="json")
    elif args.command == "public-candidate-generate":
        output = HSAResearchService(repo).generate_public_candidate_snapshot(
            PublicCandidateGenerateRequest(
                candidate_id=args.candidate_id,
                therapy_idea_id=UUID(args.therapy_idea_id),
                display_id=args.display_id,
                candidate_kind=args.candidate_kind,
                visibility=args.visibility,
                public_status=args.public_status,
                pipeline_version=args.pipeline_version,
                commit_sha=args.commit_sha,
                include_compute_jobs=not args.no_compute_jobs,
                include_decisions=not args.no_decisions,
                include_artifacts=not args.no_artifacts,
                require_moonshot_grade=not args.allow_non_moonshot,
                min_moonshot_score=args.min_moonshot_score,
                persist=not args.no_persist,
            )
        ).model_dump(mode="json")
    elif args.command == "public-candidates":
        output = HSAResearchService(repo).list_public_candidates(
            PublicCandidateLibraryRequest(
                candidate_id=args.candidate_id,
                therapy_idea_id=UUID(args.therapy_idea_id) if args.therapy_idea_id else None,
                public_status=args.status,
                visibility=args.visibility,
                candidate_kind=args.kind,
                query=args.query,
                limit=args.limit,
            )
        ).model_dump(mode="json")
    elif args.command == "public-candidate-integrity":
        expected_pairs = {}
        for pair in args.expected_pair:
            candidate_id, separator, therapy_idea_id = str(pair).partition("=")
            if not separator:
                raise SystemExit(f"Invalid --expected-pair {pair!r}; expected candidate_id=therapy_idea_uuid")
            expected_pairs[candidate_id.strip()] = UUID(therapy_idea_id.strip())
        output = HSAResearchService(repo).build_public_candidate_integrity_report(
            PublicCandidateIntegrityReportRequest(
                candidate_ids=args.candidate_id,
                therapy_idea_ids=[UUID(value) for value in args.therapy_idea_id],
                expected_candidate_therapy_ids=expected_pairs,
                visibility=args.visibility,
                limit=args.limit,
            )
        ).model_dump(mode="json")
    elif args.command == "hypothesis-promotion-report":
        output = HSAResearchService(repo).build_hypothesis_promotion_report(
            HypothesisPromotionReportRequest(
                brief_id=UUID(args.brief_id) if args.brief_id else None,
                evaluation_id=UUID(args.evaluation_id) if args.evaluation_id else None,
                therapy_idea_id=UUID(args.therapy_idea_id) if args.therapy_idea_id else None,
                topic_query=args.query,
                source_key=args.source,
                include_blocked=not args.hide_blocked,
                include_ready_for_committee=not args.hide_ready_committee,
                include_ready_for_validation=not args.hide_ready_validation,
                limit=args.limit,
            )
        ).model_dump(mode="json")
    elif args.command == "validation-packets":
        output = HSAResearchService(repo).build_validation_packets(
            ValidationPacketRequest(
                candidate_id=args.candidate_id,
                therapy_idea_id=UUID(args.therapy_idea_id) if args.therapy_idea_id else None,
                plan_id=UUID(args.plan_id) if args.plan_id else None,
                queue_item_id=UUID(args.queue_item_id) if args.queue_item_id else None,
                brief_id=UUID(args.brief_id) if args.brief_id else None,
                evaluation_id=UUID(args.evaluation_id) if args.evaluation_id else None,
                topic_query=args.query,
                source_key=args.source,
                include_queue_items=not args.hide_queue_items,
                include_evidence_addendum=not args.hide_evidence_addendum,
                addendum_limit=args.addendum_limit,
                queue_if_ready=args.queue_if_ready,
                dry_run=not args.apply,
                max_tasks=args.max_tasks,
                priority=args.priority,
                limit=args.limit,
            )
        ).model_dump(mode="json")
    elif args.command == "validation-decision-report":
        output = HSAResearchService(repo).build_validation_decision_report(
            ValidationDecisionReportRequest(
                candidate_id=args.candidate_id,
                therapy_idea_id=UUID(args.therapy_idea_id) if args.therapy_idea_id else None,
                plan_id=UUID(args.plan_id) if args.plan_id else None,
                queue_item_id=UUID(args.queue_item_id) if args.queue_item_id else None,
                brief_id=UUID(args.brief_id) if args.brief_id else None,
                evaluation_id=UUID(args.evaluation_id) if args.evaluation_id else None,
                topic_query=args.query,
                source_key=args.source,
                include_queue_items=not args.hide_queue_items,
                include_evidence_addendum=not args.hide_evidence_addendum,
                include_source_packets=args.include_source_packets,
                persist_decisions=not args.no_persist,
                addendum_limit=args.addendum_limit,
                limit=args.limit,
            )
        ).model_dump(mode="json")
    elif args.command == "validation-decisions":
        service = HSAResearchService(repo)
        if args.id:
            record = service.get_validation_decision(args.id)
            output = record.model_dump(mode="json") if record else {}
        else:
            output = [
                record.model_dump(mode="json")
                for record in service.list_validation_decisions(
                    outcome=args.outcome,
                    therapy_idea_id=UUID(args.therapy_idea_id) if args.therapy_idea_id else None,
                    candidate_id=args.candidate_id,
                    limit=args.limit,
                )
            ]
    elif args.command == "evidence-ref-repair-report":
        output = HSAResearchService(repo).build_evidence_ref_repair_report(
            EvidenceRefRepairRequest(
                therapy_idea_id=UUID(args.therapy_idea_id) if args.therapy_idea_id else None,
                plan_id=UUID(args.plan_id) if args.plan_id else None,
                queue_item_id=UUID(args.queue_item_id) if args.queue_item_id else None,
                brief_id=UUID(args.brief_id) if args.brief_id else None,
                evaluation_id=UUID(args.evaluation_id) if args.evaluation_id else None,
                topic_query=args.query,
                source_key=args.source,
                include_validation_packet=not args.no_validation_packet,
                include_text_refs=not args.no_text_refs,
                addendum_limit=args.addendum_limit,
                limit=args.limit,
            )
        ).model_dump(mode="json")
    elif args.command == "research-program-board":
        output = HSAResearchService(repo).run_research_program_board(
            ResearchProgramReviewRequest(
                program_id=UUID(args.program_id) if args.program_id else None,
                brief_ids=[UUID(value) for value in args.brief_id],
                evaluation_ids=[UUID(value) for value in args.evaluation_id],
                thesis_topic=args.topic,
                disease_scope=args.disease_scope,
                topic_query=args.query,
                source_key=args.source,
                max_packets=args.max_packets,
                max_chunks=args.max_chunks,
                max_programs=args.max_programs,
                max_evidence_loops=args.max_evidence_loops,
                review_mode=args.review_mode,
                review_models=args.review_model,
                model_profile=args.model_profile,
                persist=not args.no_persist,
            )
        ).model_dump(mode="json")
    elif args.command == "research-programs":
        service = HSAResearchService(repo)
        if args.id:
            record = service.get_research_program(UUID(args.id))
            output = {} if record is None else record.model_dump(mode="json")
        else:
            output = service.list_research_programs(
                ResearchProgramBoardRequest(
                    status=args.status,
                    gate_decision=args.gate_decision,
                    thesis_query=args.query,
                    limit=args.limit,
                )
            ).model_dump(mode="json")
    elif args.command == "research-program-evidence-loop":
        output = HSAResearchService(repo).run_research_program_evidence_loop(
            ResearchProgramEvidenceLoopRequest(
                program_id=UUID(args.program_id) if args.program_id else None,
                thesis_query=args.query,
                source_keys=args.source,
                max_tasks=args.max_tasks,
                max_source_queries=args.max_source_queries,
                max_sources_per_task=args.max_sources_per_task,
                queue_briefs=not args.no_briefs,
                create_research_leads=not args.no_leads,
                create_source_queries=not args.no_source_queries,
                priority=args.priority,
                max_chunks_per_perspective=args.max_chunks_per_perspective,
                max_claims=args.max_claims,
                max_chunk_chars=args.max_chunk_chars,
                brief_style=args.brief_style,
                model_profile=args.model_profile,
                review_mode=args.review_mode,
                review_models=args.review_model,
                dry_run=args.dry_run,
            )
        ).model_dump(mode="json")
    elif args.command == "omics-accession-hunt":
        output = HSAResearchService(repo).run_omics_accession_hunt(
            OmicsAccessionHuntRequest(
                program_id=UUID(args.program_id) if args.program_id else None,
                topic_query=args.query,
                source_keys=args.source or ["geo", "sra"],
                disease_terms=args.disease_term or [
                    "canine hemangiosarcoma",
                    "dog hemangiosarcoma",
                    "human angiosarcoma",
                    "angiosarcoma",
                ],
                gene_symbols=args.gene or ["VIM", "vimentin"],
                query_texts=args.query_text,
                limit_per_query=args.limit_per_query,
                max_queries=args.max_queries,
                persist_queries=not args.no_persist_queries,
                dry_run=args.dry_run,
            )
        ).model_dump(mode="json")
    elif args.command == "omics-evidence-packets":
        output = HSAResearchService(repo).build_omics_evidence_packets(
            OmicsEvidencePacketRequest(
                program_id=UUID(args.program_id) if args.program_id else None,
                topic_query=args.query,
                source_keys=args.source or ["geo", "sra"],
                disease_terms=args.disease_term or [
                    "canine hemangiosarcoma",
                    "dog hemangiosarcoma",
                    "human angiosarcoma",
                    "angiosarcoma",
                ],
                gene_symbols=args.gene or ["VIM", "vimentin"],
                accessions=args.accession,
                limit=args.limit,
                min_datasets_per_packet=args.min_datasets_per_packet,
                include_context_packet=not args.no_context_packet,
                dry_run=args.dry_run,
            )
        ).model_dump(mode="json")
    elif args.command == "omics-readouts":
        matrix_uri_by_accession = {}
        for item in args.matrix_uri:
            if "=" not in item:
                raise ValueError("--matrix-uri must be formatted as ACCESSION=URI")
            accession, uri = item.split("=", 1)
            matrix_uri_by_accession[accession.strip()] = uri.strip()
        sample_group_overrides = json.loads(args.sample_groups_json) if args.sample_groups_json else {}
        output = HSAResearchService(repo).build_omics_readouts(
            OmicsReadoutRequest(
                packet_id=args.packet_id,
                packet_key=args.packet_key,
                program_id=UUID(args.program_id) if args.program_id else None,
                therapy_idea_id=UUID(args.therapy_idea_id) if args.therapy_idea_id else None,
                topic_query=args.query,
                source_keys=args.source or ["geo", "sra"],
                disease_terms=args.disease_term or [
                    "canine hemangiosarcoma",
                    "dog hemangiosarcoma",
                    "human angiosarcoma",
                    "angiosarcoma",
                ],
                gene_symbols=args.gene or ["VIM", "vimentin"],
                accessions=args.accession,
                limit=args.limit,
                max_datasets=args.max_datasets,
                matrix_uri_by_accession=matrix_uri_by_accession,
                sample_group_overrides=sample_group_overrides,
                artifact_dir=args.artifact_dir,
                run_validation_agent=args.run_agent,
                model_profile=args.model_profile,
                dry_run=args.dry_run,
            )
        ).model_dump(mode="json")
    elif args.command == "omics-locus-signals":
        bigwig_uri_by_sample = json.loads(args.bigwig_json) if args.bigwig_json else {}
        sample_group_overrides = json.loads(args.sample_groups_json) if args.sample_groups_json else {}
        target_loci = json.loads(args.target_loci_json) if args.target_loci_json else {}
        output = HSAResearchService(repo).build_omics_locus_signals(
            OmicsLocusSignalRequest(
                packet_id=args.packet_id,
                packet_key=args.packet_key,
                topic_query=args.query,
                source_keys=args.source or ["geo"],
                disease_terms=args.disease_term or [
                    "canine hemangiosarcoma",
                    "dog hemangiosarcoma",
                    "human angiosarcoma",
                    "angiosarcoma",
                ],
                gene_symbols=args.gene or ["VIM"],
                accessions=args.accession,
                limit=args.limit,
                max_datasets=args.max_datasets,
                max_samples_per_group=args.max_samples_per_group,
                remote_extract_timeout_seconds=args.remote_extract_timeout_seconds,
                artifact_dir=args.artifact_dir,
                bigwig_uri_by_sample=bigwig_uri_by_sample,
                sample_group_overrides=sample_group_overrides,
                target_loci=target_loci,
                run_validation_agent=args.run_agent,
                model_profile=args.model_profile,
                dry_run=args.dry_run,
            )
        ).model_dump(mode="json")
    elif args.command == "omics-followups":
        output = HSAResearchService(repo).build_omics_followups(
            OmicsFollowupRequest(
                topic_query=args.query,
                gene_symbols=args.gene or ["VIM"],
                accessions=args.accession,
                source_keys=args.source or ["geo", "sra", "pubmed", "europe_pmc"],
                omics_readout_report=json.loads(args.readout_report_json) if args.readout_report_json else None,
                omics_locus_signal_report=json.loads(args.locus_report_json) if args.locus_report_json else None,
                validation_agent_result=json.loads(args.validation_agent_json) if args.validation_agent_json else None,
                include_locus_signal_report=args.include_locus_signals,
                locus_signal_request=json.loads(args.locus_signal_request_json) if args.locus_signal_request_json else {},
                max_tasks=args.max_tasks,
                create_research_leads=not args.no_research_leads,
                create_source_queries=not args.no_source_queries,
                dry_run=not args.apply,
            )
        ).model_dump(mode="json")
    elif args.command == "x-topic-monitor":
        from .x_topic_monitor import (
            TWITTERAPI_IO_MAX_SINGLE_PAGE_RESULTS,
            X_TOPIC_SOURCE_KEY,
            TwitterApiIoProvider,
            XTopicRequest,
            build_default_source_queries,
        )

        provider = TwitterApiIoProvider()
        max_results = min(args.max_results, TWITTERAPI_IO_MAX_SINGLE_PAGE_RESULTS)
        queries = [
            query
            for query in build_default_source_queries()
            if args.query_name is None or query.query_name == args.query_name
        ]
        query_results = []
        candidates = []
        errors = []
        raw_tweet_count = 0
        for query_index, query in enumerate(queries):
            if query_index > 0 and args.query_delay_seconds > 0:
                time.sleep(args.query_delay_seconds)
            try:
                result = provider.search(
                    XTopicRequest(
                        query=query.query_text,
                        query_name=query.query_name,
                        max_results=max_results,
                        retention_mode=args.retention_mode,
                    )
                )
            except Exception as exc:
                errors.append({"query_name": query.query_name, "error": str(exc)})
                continue
            raw_tweet_count += result.raw_tweet_count
            query_results.append(result.model_dump(mode="json"))
            for candidate in result.candidates:
                candidates.append(
                    {
                        **candidate.model_dump(mode="json"),
                        "query_name": query.query_name,
                        "post_id": candidate.source_record_id,
                    }
                )
        provider_report = {
            "source_key": X_TOPIC_SOURCE_KEY,
            "provider": provider.provider_name,
            "queries": [query.model_dump(mode="json") for query in queries],
            "query_delay_seconds": args.query_delay_seconds,
            "query_results": query_results,
            "raw_tweet_count": raw_tweet_count,
            "candidate_count": len(candidates),
            "candidates": candidates,
            "manual_review_required": True,
            "errors": errors,
        }
        agent_review = HSAResearchService(repo).run_x_topic_review(
            XTopicReviewRequest(
                provider_report=provider_report,
                recent_run_limit=5,
                max_candidates=args.max_candidates,
                model_profile=args.model_profile,
                review_mode=args.review_mode,
                review_models=args.review_model,
            )
        ).model_dump(mode="json")
        output = {**provider_report, "agent_review": agent_review}
    elif args.command == "x-topic-review":
        provider_report = None
        if args.provider_report:
            provider_report = json.loads(Path(args.provider_report).read_text())
        candidates = [json.loads(candidate) for candidate in args.candidate_json]
        output = HSAResearchService(repo).run_x_topic_review(
            XTopicReviewRequest(
                provider_report=provider_report,
                candidates=candidates,
                recent_run_limit=args.recent_run_limit,
                max_candidates=args.max_candidates,
                model_profile=args.model_profile,
                review_mode=args.review_mode,
                review_models=args.review_model,
            )
        ).model_dump(mode="json")
    elif args.command == "x-linked-article-followup":
        output = HSAResearchService(repo).run_x_linked_article_followup(
            XLinkedArticleFollowupRequest(
                urls=args.url,
                recent_run_limit=args.recent_run_limit,
                max_urls=args.max_urls,
                fetch=not args.no_fetch,
                parse=not args.no_parse,
                approved_by=args.approved_by,
                approval_note=args.approval_note,
                robots_policy=args.robots_policy,
            )
        ).model_dump(mode="json")
    elif args.command == "x-linked-article-review":
        output = HSAResearchService(repo).run_x_linked_article_review(
            XLinkedArticleReviewRequest(
                review_ids=[UUID(review_id) for review_id in args.review_id],
                review_status=args.review_status,
                limit=args.limit,
                model_profile=args.model_profile,
                review_mode=args.review_mode,
                review_models=args.review_model,
            )
        ).model_dump(mode="json")
    elif args.command == "queue-source-followups":
        output = HSAResearchService(repo).queue_source_followups(
            SourceFollowupQueueRequest(
                source_key=args.source,
                review_ids=[UUID(review_id) for review_id in args.review_id],
                review_status=args.review_status,
                limit=args.limit,
                include_existing=args.include_existing,
                include_agent_recommendations=not args.no_agent_recommendations,
                agent_run_limit=args.agent_run_limit,
            )
        ).model_dump(mode="json")
    elif args.command == "queue-unpaywall-doi-followups":
        output = HSAResearchService(repo).queue_unpaywall_doi_followups(
            DoiOpenAccessFollowupQueueRequest(
                source_keys=args.source,
                limit=args.limit,
                include_existing=args.include_existing,
            )
        ).model_dump(mode="json")
    elif args.command == "source-followups":
        output = [
            item.model_dump(mode="json")
            for item in HSAResearchService(repo).list_source_followups(
                source_key=args.source,
                status=args.status,
                identifier_type=args.identifier_type,
                limit=args.limit,
            )
        ]
    elif args.command == "collect-research-leads":
        output = HSAResearchService(repo).collect_research_leads(
            ResearchLeadCollectRequest(
                agent_names=args.agent_name or ["x_linked_article_review_agent", "x_topic_review_agent"],
                statuses=args.status or ["completed"],
                limit=args.limit,
                include_existing=args.include_existing,
            )
        ).model_dump(mode="json")
    elif args.command == "research-leads":
        service = HSAResearchService(repo)
        if args.id and args.set_status:
            lead = service.update_research_lead(UUID(args.id), status=args.set_status)
            output = {} if lead is None else lead.model_dump(mode="json")
        elif args.id:
            lead = service.get_research_lead(UUID(args.id))
            output = {} if lead is None else lead.model_dump(mode="json")
        else:
            output = [
                lead.model_dump(mode="json")
                for lead in service.list_research_leads(
                    status=args.status,
                    lead_type=args.lead_type,
                    source_key=args.source,
                    limit=args.limit,
                )
            ]
    elif args.command == "resolve-research-followups":
        output = HSAResearchService(repo).resolve_research_followups(
            ResearchFollowupResolverRequest(
                lead_ids=[UUID(lead_id) for lead_id in args.lead_id],
                statuses=args.status or ["followup"],
                source_keys=args.source,
                search_source_keys=args.search_source,
                limit=args.limit,
                ingest_source_followups=not args.no_ingest_source_followups,
                search_missing_identifiers=not args.no_search_missing_identifiers,
                promote_ready_leads=not args.no_promote,
                run_claim_extraction=not args.no_claim_extraction,
                dry_run=args.dry_run,
                force_live_search=args.force_live_search,
                inspect_evidence_refs=not args.no_evidence_inspection,
                min_evidence_chunks=args.min_evidence_chunks,
                evidence_inspection_limit=args.evidence_inspection_limit,
                search_limit_per_source=args.search_limit_per_source,
                max_search_terms=args.max_search_terms,
                approved_by=args.approved_by,
            )
        ).model_dump(mode="json")
    elif args.command == "ingest-source-followups":
        output = HSAResearchService(repo).ingest_source_followups(
            SourceFollowupIngestRequest(
                followup_ids=[UUID(followup_id) for followup_id in args.followup_id],
                source_keys=args.source,
                statuses=args.status or ["queued", "approved"],
                limit=args.limit,
                approved_by=args.approved_by,
                run_claim_extraction=not args.no_claim_extraction,
                dry_run=args.dry_run,
            )
        ).model_dump(mode="json")
    elif args.command == "agent-runs":
        service = HSAResearchService(repo)
        if args.id:
            record = service.get_agent_run(UUID(args.id))
            output = {} if record is None else record.model_dump(mode="json")
        else:
            output = [
                run.model_dump(mode="json")
                for run in service.list_agent_runs(
                    agent_name=args.agent_name,
                    status=args.status,
                    source_key=args.source,
                    limit=args.limit,
                )
            ]
    elif args.command == "agent-performance":
        output = HSAResearchService(repo).build_agent_performance_report(
            AgentPerformanceReportRequest(
                agent_name=args.agent_name,
                status=args.status,
                source_key=args.source,
                limit=args.limit,
                min_sample_size=args.min_sample_size,
            )
        ).model_dump(mode="json")
    elif args.command == "agent-performance-evaluate":
        output = HSAResearchService(repo).run_agent_performance_evaluation(
            AgentPerformanceEvaluationRequest(
                agent_run_ids=[UUID(value) for value in args.agent_run_id],
                agent_name=args.agent_name,
                status=args.status,
                source_key=args.source,
                limit=args.limit,
                reviewed_only=not args.include_unreviewed,
                model_profile=args.model_profile,
                review_models=args.review_model,
            )
        ).model_dump(mode="json")
    elif args.command == "reward-events":
        output = HSAResearchService(repo).build_reward_report(
            RewardReportRequest(
                agent_name=args.agent_name,
                source_key=args.source,
                event_source=args.event_source,
                group_by=args.group_by,
                limit=args.limit,
                min_sample_size=args.min_sample_size,
            )
        ).model_dump(mode="json")
    elif args.command == "reward-events-sync":
        output = HSAResearchService(repo).sync_reward_events_from_reviews(
            RewardEventSyncRequest(
                agent_name=args.agent_name,
                source_key=args.source,
                reviewer_type=args.reviewer_type,
                limit=args.limit,
                include_existing=args.include_existing,
                created_by=args.created_by,
            )
        ).model_dump(mode="json")
    elif args.command == "run-manifests":
        output = {
            "manifests": [
                record.model_dump(mode="json")
                for record in HSAResearchService(repo).list_run_manifests(
                    trace_id=UUID(args.trace_id) if args.trace_id else None,
                    manifest_type=args.manifest_type,
                    status=args.status,
                    limit=args.limit,
                )
            ]
        }
    elif args.command == "escalate-agent-findings":
        output = HSAResearchService(repo).escalate_agent_findings(
            AgentFindingEscalationRequest(
                review_ids=[UUID(value) for value in args.review_id],
                agent_run_ids=[UUID(value) for value in args.agent_run_id],
                verdicts=args.verdict or ["bad", "needs_followup"],
                limit=args.limit,
                source_keys=args.source,
                create_research_leads=not args.no_research_leads,
                create_source_queries=not args.no_source_queries,
                dry_run=args.dry_run,
                operator=args.operator,
            )
        ).model_dump(mode="json")
    elif args.command == "refine-research-followups":
        output = HSAResearchService(repo).refine_research_followups(
            ResearchFollowupRefinementRequest(
                lead_ids=[UUID(value) for value in args.lead_id],
                review_ids=[UUID(value) for value in args.review_id],
                verdicts=args.verdict or ["bad", "needs_followup"],
                source_keys=args.source,
                limit=args.limit,
                max_queries_per_review=args.max_queries_per_review,
                dry_run=args.dry_run,
                operator=args.operator,
            )
        ).model_dump(mode="json")
    elif args.command == "evaluate-research-brief":
        output = HSAResearchService(repo).evaluate_research_brief(
            ResearchBriefEvaluationRequest(
                brief_id=UUID(args.brief_id) if args.brief_id else None,
                topic_query=args.topic_query,
                source_key=args.source,
                limit=args.limit,
                minimum_overall_score=args.minimum_overall_score,
                model_profile=args.model_profile,
                review_mode=args.review_mode,
                review_models=args.review_model,
            )
        ).model_dump(mode="json")
    elif args.command == "research-brief-evaluations":
        service = HSAResearchService(repo)
        if args.id:
            record = service.get_research_brief_evaluation(UUID(args.id))
            output = {} if record is None else record.model_dump(mode="json")
        else:
            passes_quality_bar = None
            if args.passes_quality_bar is not None:
                passes_quality_bar = args.passes_quality_bar == "true"
            output = [
                evaluation.model_dump(mode="json")
                for evaluation in service.list_research_brief_evaluations(
                    brief_id=UUID(args.brief_id) if args.brief_id else None,
                    readiness=args.readiness,
                    passes_quality_bar=passes_quality_bar,
                    limit=args.limit,
                )
            ]
    elif args.command == "queue-research-brief-followups":
        output = HSAResearchService(repo).queue_research_brief_followups(
            ResearchBriefFollowupQueueRequest(
                brief_ids=[UUID(value) for value in args.brief_id],
                evaluation_ids=[UUID(value) for value in args.evaluation_id],
                status=args.status,
                source_key=args.source,
                topic_query=args.topic_query,
                limit=args.limit,
                include_evaluations=not args.no_evaluations,
                max_limitations_per_brief=args.max_limitations_per_brief,
                force=args.force,
                dry_run=args.dry_run,
            )
        ).model_dump(mode="json")
    elif args.command == "plan-validation":
        output = HSAResearchService(repo).plan_validation(
            ValidationPlanRequest(
                brief_id=UUID(args.brief_id) if args.brief_id else None,
                evaluation_id=UUID(args.evaluation_id) if args.evaluation_id else None,
                topic_query=args.topic_query,
                source_key=args.source,
                require_ready_evaluation=not args.allow_unready,
                max_tasks=args.max_tasks,
                model_profile=args.model_profile,
            )
        ).model_dump(mode="json")
    elif args.command == "validation-plans":
        service = HSAResearchService(repo)
        if args.id:
            record = service.get_validation_plan(UUID(args.id))
            output = {} if record is None else record.model_dump(mode="json")
        else:
            output = [
                plan.model_dump(mode="json")
                for plan in service.list_validation_plans(
                    brief_id=UUID(args.brief_id) if args.brief_id else None,
                    evaluation_id=UUID(args.evaluation_id) if args.evaluation_id else None,
                    status=args.status,
                    readiness=args.readiness,
                    limit=args.limit,
                )
            ]
    elif args.command == "queue-validation-requests":
        output = HSAResearchService(repo).queue_validation_requests_from_plan(
            ValidationRequestQueueRequest(
                plan_id=UUID(args.plan_id),
                task_ids=[UUID(value) for value in args.task_id],
                dry_run=not args.apply,
            )
        ).model_dump(mode="json")
    elif args.command == "queue-therapy-committee-validation-requests":
        output = HSAResearchService(repo).queue_therapy_committee_validation_requests(
            TherapyCommitteeValidationQueueRequest(
                agent_run_id=UUID(args.agent_run_id) if args.agent_run_id else None,
                idea_ids=[UUID(value) for value in args.idea_id],
                max_ideas=args.max_ideas,
                priority=args.priority,
                dry_run=not args.apply,
            )
        ).model_dump(mode="json")
    elif args.command == "validation-request-queue":
        service = HSAResearchService(repo)
        if args.id:
            item = service.get_validation_request_queue_item(UUID(args.id))
            output = {} if item is None else item.model_dump(mode="json")
        else:
            output = [
                item.model_dump(mode="json")
                for item in service.list_validation_request_queue_items(
                    plan_id=UUID(args.plan_id) if args.plan_id else None,
                    status=args.status,
                    statuses=args.status_any or None,
                    source_key=args.source,
                    task_type=args.task_type,
                    topic_query=args.topic_query,
                    limit=args.limit,
                )
            ]
    elif args.command == "approve-validation-request":
        item = HSAResearchService(repo).approve_validation_request_queue_item(
            UUID(args.id),
            approved_by=args.approved_by,
            approval_note=args.approval_note,
        )
        output = {} if item is None else item.model_dump(mode="json")
    elif args.command == "dispatch-validation-request":
        item = HSAResearchService(repo).dispatch_validation_request_queue_item(
            UUID(args.id),
            model_profile=args.model_profile,
        )
        output = {} if item is None else item.model_dump(mode="json")
    elif args.command == "validation-autopilot":
        service = HSAResearchService(repo)
        request = ValidationAutopilotRequest(
            enabled=not args.disabled,
            dry_run=not args.apply,
            force=args.force,
            manual_grace_period_hours=args.manual_grace_hours,
            minimum_queue_age_hours=args.minimum_queue_age_hours,
            max_per_run=args.max_per_run,
            hourly_budget_usd=args.hourly_budget_usd,
            daily_budget_usd=args.daily_budget_usd,
            estimated_cost_per_item_usd=args.estimated_cost_per_item_usd,
            allowed_task_types=args.allowed_task_type or ["expert_review", "target_validation", "omics"],
            allowed_validation_types=args.allowed_validation_type or ["expert_review", "homology", "omics"],
            source_keys=args.source,
            model_profile=args.model_profile,
        )
        if args.apply:
            output = service.run_validation_autopilot(request).model_dump(mode="json")
        else:
            output = service.preview_validation_autopilot(request).model_dump(mode="json")
    elif args.command == "md-expert-packet":
        packet = HSAResearchService(repo).create_md_expert_review_packet(
            UUID(args.compute_job_id),
            endpoint_id=args.endpoint_id,
            endpoint_name=args.endpoint_name,
            template_name=args.template_name,
            persist=not args.no_persist,
        )
        output = {} if packet is None else packet.model_dump(mode="json")
    elif args.command == "md-expert-agent-review":
        result = HSAResearchService(repo).run_md_expert_review_agent(
            MDExpertAgentReviewRequest(
                packet_id=UUID(args.packet_id),
                model_profile=args.model_profile,
                approve_on_agent_approved=not args.no_agent_approval,
                reviewer_name=args.reviewer_name,
                reviewer_contact=args.reviewer_contact,
            )
        )
        output = {} if result is None else result.model_dump(mode="json")
    elif args.command == "resolve-evidence-gaps":
        output = HSAResearchService(repo).resolve_evidence_gaps(
            EvidenceGapResolverRequest(
                queue_item_ids=[UUID(value) for value in args.queue_item_id],
                plan_id=UUID(args.plan_id) if args.plan_id else None,
                statuses=args.status or ["completed"],
                decisions=args.decision or ["hold", "demote"],
                task_types=args.task_type,
                gap_types=args.gap_type or ["missing_evidence", "risk", "next_action"],
                limit=args.limit,
                max_gaps_per_item=args.max_gaps_per_item,
                priority=args.priority,
                dry_run=not args.apply,
                queue_research_briefs=args.queue_briefs,
            )
        ).model_dump(mode="json")
    elif args.command == "validation-gap-source-pack":
        output = HSAResearchService(repo).build_validation_gap_source_pack(
            ValidationGapSourcePackRequest(
                queue_item_ids=[UUID(value) for value in args.queue_item_id],
                lead_ids=[UUID(value) for value in args.lead_id],
                lead_statuses=args.lead_status or ["new", "followup"],
                source_keys=args.source,
                lanes=args.lane,
                limit=args.limit,
                max_queries_per_lane=args.max_queries_per_lane,
                persist_queries=args.persist_queries or args.apply,
                active=not args.inactive,
                dry_run=not args.apply,
            )
        ).model_dump(mode="json")
    elif args.command == "validation-gap-ingest":
        output = HSAResearchService(repo).ingest_validation_gap_source_queries(
            ValidationGapSourceIngestRequest(
                source_keys=args.source,
                query_names=args.query_name,
                tracks=args.track or ["validation_gap"],
                followup_lane=args.followup_lane,
                origin_review_ids=[UUID(value) for value in args.origin_review_id],
                origin_agent_run_ids=[UUID(value) for value in args.origin_agent_run_id],
                limit_per_query=args.limit_per_query,
                max_queries=args.max_queries,
                dry_run=not args.apply,
            )
        ).model_dump(mode="json")
    elif args.command == "repair-pubmed-identifiers":
        output = HSAResearchService(repo).repair_pubmed_identifiers(
            PubMedIdentifierRepairRequest(
                pmids=args.pmid,
                limit=args.limit,
                batch_size=args.batch_size,
                dry_run=not args.apply,
            )
        ).model_dump(mode="json")
    elif args.command == "research-followup-loop":
        output = HSAResearchService(repo).run_research_followup_loop(
            ResearchFollowupLoopRequest(
                lead_id=UUID(args.lead_id),
                followup_lane=args.followup_lane,
                source_keys=args.source,
                query_names=args.query_name,
                search_query_text=args.search_query_text,
                limit_per_query=args.limit_per_query,
                max_queries=args.max_queries,
                ingest=not args.no_ingest,
                resolve=args.resolve,
                evaluate=args.evaluate,
                queue_identifier_followups=not args.no_identifier_followups,
                ingest_identifier_followups=not args.no_ingest_identifier_followups,
                run_claim_extraction=not args.no_claim_extraction,
                max_identifier_followups=args.max_identifier_followups,
                dry_run=not args.apply,
                force_live_search=not args.no_force_live_search,
                search_limit_per_source=args.search_limit_per_source,
                min_evidence_chunks=args.min_evidence_chunks,
                model_profile=args.model_profile,
                review_models=args.review_model,
                estimated_evaluator_cost_usd=args.estimated_evaluator_cost_usd,
                operator=args.operator,
            )
        ).model_dump(mode="json")
    elif args.command == "research-hunt-tasks":
        output = HSAResearchService(repo).run_research_hunt_tasks(
            ResearchHuntTaskRunRequest(
                lead_ids=[UUID(value) for value in args.lead_id],
                task_ids=[UUID(value) for value in args.task_id],
                task_types=args.task_type,
                statuses=args.status or ["open"],
                source_keys=args.source,
                limit=args.limit,
                claim_chunk_limit=args.claim_chunk_limit,
                dry_run=not args.apply,
                evaluate=not args.no_evaluate,
                force_live_search=not args.no_force_live_search,
                include_broad_tasks=args.include_broad_tasks,
                allow_broad_task_fanout=args.allow_broad_task_fanout,
                search_limit_per_source=args.search_limit_per_source,
                model_profile=args.model_profile,
                review_models=args.review_model,
                operator=args.operator,
            )
        ).model_dump(mode="json")
    elif args.command == "research-hunt-queue-report":
        output = HSAResearchService(repo).build_research_hunt_queue_report(
            ResearchHuntQueueReportRequest(
                lead_ids=[UUID(value) for value in args.lead_id],
                lead_statuses=args.lead_status or ["new", "watching", "followup", "queued"],
                source_keys=args.source,
                limit=args.limit,
                task_limit=args.task_limit,
                stale_after_hours=args.stale_after_hours,
                include_tasks=not args.no_tasks,
                include_suppressed=not args.no_suppressed,
            )
        ).model_dump(mode="json")
    elif args.command == "research-hunt-queue-maintenance":
        output = HSAResearchService(repo).maintain_research_hunt_queue(
            ResearchHuntQueueMaintenanceRequest(
                lead_ids=[UUID(value) for value in args.lead_id],
                lead_statuses=args.lead_status or ["new", "watching", "followup", "queued"],
                source_keys=args.source,
                reasons=args.reason or [
                    "stale_broad_or_passive",
                    "duplicate_broad_family",
                    "passive_monitoring_note",
                ],
                stale_after_hours=args.stale_after_hours,
                limit=args.limit,
                dry_run=not args.apply,
                operator=args.operator,
            )
        ).model_dump(mode="json")
    elif args.command == "queue-ready-research-hunt-synthesis":
        output = HSAResearchService(repo).queue_ready_research_hunt_synthesis(
            ResearchHuntSynthesisQueueRequest(
                lead_ids=[UUID(value) for value in args.lead_id],
                lead_statuses=args.lead_status or ["new", "watching", "followup"],
                source_keys=args.source,
                limit=args.limit,
                disease_scope=args.disease_scope,
                priority=args.priority,
                max_chunks_per_perspective=args.max_chunks,
                max_claims=args.max_claims,
                max_chunk_chars=args.max_chunk_chars,
                brief_style=args.brief_style,
                model_profile=args.model_profile,
                review_mode=args.review_mode,
                review_models=args.review_model,
                create_handoff_docs=not args.no_handoff_docs,
                dry_run=not args.apply,
                transition_leads=not args.no_transition_leads,
                operator=args.operator,
            )
        ).model_dump(mode="json")
    elif args.command == "research-hunt-synthesis-doc":
        output = HSAResearchService(repo).create_ready_research_hunt_synthesis_docs(
            ResearchHuntSynthesisDocRequest(
                lead_ids=[UUID(value) for value in args.lead_id],
                lead_statuses=args.lead_status or ["new", "watching", "followup", "queued"],
                source_keys=args.source,
                limit=args.limit,
                max_claims=args.max_claims,
                max_chunks=args.max_chunks,
                max_chunk_chars=args.max_chunk_chars,
                max_technical_footnotes=args.max_technical_footnotes,
                include_technical_footnotes=not args.no_technical_footnotes,
                dry_run=not args.apply,
                operator=args.operator,
            )
        ).model_dump(mode="json")
    elif args.command == "model-review-summary":
        service = HSAResearchService(repo)
        if args.id:
            record = service.get_agent_run(UUID(args.id))
            output = {} if record is None else _model_review_summary(record.model_dump(mode="json"))
        else:
            output = [
                _model_review_summary(run.model_dump(mode="json"))
                for run in service.list_agent_runs(
                    agent_name=args.agent_name,
                    status=args.status,
                    source_key=args.source,
                    limit=args.limit,
                )
            ]
    elif args.command == "retrieval-smoke":
        output = HSAResearchService(repo).run_retrieval_smoke(
            RetrievalSmokeRequest(
                query=args.query,
                source_key=args.source,
                object_type=args.object_type,
                embedding_model=args.embedding_model,
                limit=args.limit,
                max_chunk_chars=args.max_chunk_chars,
                context_window=args.context_window,
                include_entity_mentions=not args.no_entity_mentions,
                include_keyword_fallback=not args.no_keyword_fallback,
                require_embedding=args.require_embedding,
            )
        ).model_dump(mode="json")
    elif args.command == "embedding-index":
        embedding_model = args.embedding_model or default_embedding_model_for_environment()
        output = asdict(
            index_embeddings_for_repository(
                repo,
                embedding_model=embedding_model,
                source_key=args.source,
                limit=args.limit,
                force=args.force,
                batch_size=args.batch_size,
            )
        )
    elif args.command == "embedding-maintenance":
        embedding_model = args.embedding_model or default_embedding_model_for_environment()
        output = maintain_embedding_index(
            repo,
            embedding_model=embedding_model,
            prune_embedding_model=args.prune_model,
            source_key=args.source,
            object_type=args.object_type,
            prune_orphans=not args.dry_run,
        ).to_report()
    elif args.command == "embedding-bakeoff":
        output = run_embedding_bakeoff(
            repo,
            embedding_models=tuple(args.embedding_model) or DEFAULT_EMBEDDING_BAKEOFF_MODELS,
            source_key=args.source,
            limit=args.limit,
            index_missing=args.index_missing,
            index_limit=args.index_limit,
            force=args.force,
            batch_size=args.batch_size,
        )
    elif args.command == "resolve-entities":
        request = EntityResolutionRequest(
            source_key=args.source,
            limit=args.limit,
            resolver_profile=args.resolver_profile,
        )
        output = resolve_entities_for_repository(
            repo,
            source_key=request.source_key,
            limit=request.limit,
            resolver_profile=request.resolver_profile,
        ).model_dump(mode="json")
    elif args.command == "backfill-papers":
        output = backfill_papers_json(
            repo,
            args.path,
            limit=args.limit,
            chunk=not args.no_chunk,
        ).model_dump(mode="json")
    elif args.command == "backfill-deep-dives":
        output = backfill_deep_dives(
            repo,
            args.dir,
            limit=args.limit,
            chunk=not args.no_chunk,
        ).model_dump(mode="json")
    elif args.command == "extract-claims":
        output = extract_claims_for_repository(
            repo,
            source_key=args.source,
            object_type=args.object_type,
            limit=args.limit,
        ).model_dump(mode="json")
    elif args.command == "curate-claims":
        curation = HSAResearchService(repo).curate_claims(
            ClaimCurationRequest(
                source_key=args.source,
                query=args.query,
                limit=args.limit,
                min_confidence=args.min_confidence,
                promote_threshold=args.promote_threshold,
                include_seed_claims=args.include_seed_claims,
                dry_run=args.dry_run,
                model_profile=args.model_profile,
            ),
        ).model_dump(mode="json")
        if args.summary_only:
            curation.pop("decisions", None)
        output = curation
    elif args.command == "scout-sources":
        scout = HSAResearchService(repo).scout_sources(
            SourceScoutRequest(
                focus=args.focus,
                max_phase=args.max_phase,
                max_recommendations=args.limit,
                include_registered_sources=not args.no_registered,
                include_expansion_sources=not args.no_expansion,
                model_profile=args.model_profile,
            ),
        ).model_dump(mode="json")
        if args.summary_only:
            for recommendation in scout["recommendations"]:
                recommendation.pop("recommended_queries", None)
                recommendation.pop("implementation_notes", None)
        output = scout
    elif args.command == "scrape-profiles":
        output = [profile.model_dump(mode="json") for profile in list_scrape_profiles()]
    elif args.command == "review-scrape-profile":
        output = ScrapeBridge(repo).review_profile(
            ScrapeProfileReviewRequest(
                source_key=args.source,
                robots_policy=args.robots_policy,
                approved_for_fetch=args.approve_fetch,
                reviewed_by=args.reviewed_by,
                review_note=args.review_note,
                allowed_url_patterns=args.allowed_url_pattern,
                storage_policy=args.storage_policy,
            )
        ).model_dump(mode="json")
    elif args.command == "fetch-scrape":
        output = ScrapeBridge(repo).fetch(
            ScrapeFetchRequest(
                source_key=args.source,
                urls=args.url,
                max_pages=args.max_pages,
                approved_by=args.approved_by,
                approval_note=args.approval_note,
            )
        ).model_dump(mode="json")
    elif args.command == "parse-scrape":
        output = ScrapeBridge(repo).parse(args.source, limit=args.limit).model_dump(mode="json")
    elif args.command == "build-scrape-manifest":
        output = ScrapeBridge(repo).build_manifest(
            ScrapeManifestRequest(
                source_key=args.source,
                seed_urls=args.seed_url,
                fetch_seed_pages=args.fetch_seed_pages,
                max_seed_pages=args.max_seed_pages,
                max_candidate_urls=args.max_candidate_urls,
                approved_by=args.approved_by,
                approval_note=args.approval_note,
            )
        ).model_dump(mode="json")
    elif args.command == "fetch-scrape-manifest":
        output = ScrapeBridge(repo).fetch_manifest(
            ScrapeManifestFetchRequest(
                source_key=args.source,
                manifest_artifact_id=args.manifest_artifact_id,
                max_pages=args.max_pages,
                approved_by=args.approved_by,
                approval_note=args.approval_note,
            )
        ).model_dump(mode="json")
    elif args.command == "list-scrape-reviews":
        output = [
            record.model_dump(mode="json")
            for record in ScrapeBridge(repo).list_reviews(args.source, review_status=args.status, limit=args.limit)
        ]
    elif args.command == "review-scrape":
        output = ScrapeBridge(repo).review(
            ScrapeReviewRequest(
                source_key=args.source,
                review_ids=args.review_id,
                decision=args.decision,
                reviewed_by=args.reviewed_by,
                review_note=args.review_note,
            )
        ).model_dump(mode="json")
    elif args.command == "ingest-scrape":
        output = ScrapeBridge(repo).ingest(
            ScrapeIngestRequest(
                source_key=args.source,
                review_ids=args.review_id,
                artifact_ids=args.artifact_id,
                limit=args.limit,
                min_parser_confidence=args.min_parser_confidence,
                approved_by=args.approved_by,
                approval_note=args.approval_note,
            )
        ).model_dump(mode="json")
    else:  # pragma: no cover - argparse prevents this path
        raise ValueError(f"Unsupported command: {args.command}")

    print(json.dumps(output, indent=2, sort_keys=True))
    if getattr(args, "fail_on_failed_sources", False) and output.get("failed_sources"):
        print(f"Failed sources: {', '.join(output['failed_sources'])}", file=sys.stderr)
        raise SystemExit(1)
    if getattr(args, "fail_on_error", False) and output.get("passed") is False:
        print(f"Retrieval smoke failed: {', '.join(output.get('errors', []))}", file=sys.stderr)
        raise SystemExit(1)
    if getattr(args, "fail_on_blocking", False) and output.get("should_block_schedule"):
        print(f"Blocking full-text triage: {output.get('action')}", file=sys.stderr)
        raise SystemExit(1)


def _model_review_summary(run: dict[str, Any]) -> dict[str, Any]:
    output_payload = run.get("output_payload") or {}
    evidence = output_payload.get("evidence") or {}
    ranked_ideas = output_payload.get("ranked_ideas") or []
    summary = {
        "agent_run_id": run.get("agent_run_id"),
        "agent_name": run.get("agent_name"),
        "status": run.get("status"),
        "source_key": run.get("source_key"),
        "partition_date": run.get("partition_date"),
        "completed_at": run.get("completed_at"),
        "summary": run.get("summary"),
        "schedule_readiness": output_payload.get("schedule_readiness"),
        "should_block_schedule": output_payload.get("should_block_schedule"),
        "selected_model": evidence.get("selected_model"),
        "committee_run_id": output_payload.get("committee_run_id"),
        "decision_summary": output_payload.get("decision_summary"),
        "idea_count": len(ranked_ideas),
        "top_ideas": [
            {
                "idea_id": idea.get("idea_id"),
                "title": idea.get("title"),
                "priority_score": idea.get("priority_score"),
                "evidence_strength": idea.get("evidence_strength"),
            }
            for idea in ranked_ideas[:5]
            if isinstance(idea, dict)
        ],
        "errors": (output_payload.get("errors") or run.get("errors") or [])[:5],
        "actions": [
            {
                "source_key": action.get("source_key"),
                "action": action.get("action"),
                "severity": action.get("severity"),
                "reason": action.get("reason"),
            }
            for action in (output_payload.get("actions") or [])[:10]
        ],
        "model_reviews": [],
    }
    for review in (evidence.get("model_reviews") or [])[:10]:
        metadata = review.get("metadata") or {}
        usage = metadata.get("usage") or {}
        result = review.get("result") or {}
        summary["model_reviews"].append(
            {
                "model_name": review.get("model_name"),
                "status": review.get("status"),
                "requested_model": metadata.get("requested_model"),
                "resolved_model": metadata.get("model_name"),
                "schedule_readiness": result.get("schedule_readiness"),
                "should_block_schedule": result.get("should_block_schedule"),
                "actions": [action.get("action") for action in (result.get("actions") or [])],
                "usage": {
                    key: usage[key]
                    for key in ("prompt_tokens", "completion_tokens", "total_tokens", "cost")
                    if key in usage
                },
                "error": review.get("error"),
            }
        )
    if not summary["model_reviews"]:
        for report in (output_payload.get("reports") or [])[:10]:
            if not isinstance(report, dict):
                continue
            metadata = ((report.get("evidence") or {}).get("model_review") or {})
            if not isinstance(metadata, dict):
                continue
            usage = metadata.get("usage") or {}
            original_review = metadata.get("original_review") or {}
            summary["model_reviews"].append(
                {
                    "perspective": report.get("perspective"),
                    "requested_model": metadata.get("requested_model"),
                    "resolved_model": metadata.get("model_name"),
                    "json_repair_attempted": bool(metadata.get("json_repair_attempted")),
                    "original_requested_model": original_review.get("requested_model"),
                    "original_resolved_model": original_review.get("model_name"),
                    "usage": {
                        key: usage[key]
                        for key in ("prompt_tokens", "completion_tokens", "total_tokens", "cost")
                        if key in usage
                    },
                }
            )
    return summary


if __name__ == "__main__":
    main()
