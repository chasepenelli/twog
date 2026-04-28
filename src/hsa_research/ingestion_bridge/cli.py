"""Command-line entrypoint for local-first Ingestion Bridge v2."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from uuid import UUID

from .backfill import backfill_deep_dives, backfill_papers_json
from .claim_curator import curate_claims_for_repository
from .claim_extractor import extract_claims_for_repository
from .contracts import (
    ClaimCurationRequest,
    EntityResolutionRequest,
    FullTextTriageRequest,
    FullTextOpsRequest,
    RetrievalSmokeRequest,
    ScrapeFetchRequest,
    ScrapeIngestRequest,
    ScrapeManifestFetchRequest,
    ScrapeManifestRequest,
    ScrapeProfileReviewRequest,
    ScrapeReviewRequest,
    SourceQuery,
    SourceScoutRequest,
    XLinkedArticleFollowupRequest,
    XTopicReviewRequest,
)
from .embeddings import LOCAL_HASH_EMBEDDING_MODEL, maintain_embedding_index
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
        default="external_required",
        help="Whether review is external, OpenRouter-backed, comparative, or deterministic only",
    )
    full_text_ops.add_argument(
        "--review-model",
        action="append",
        default=[],
        help="OpenRouter model id; repeat to compare multiple models",
    )
    full_text_ops.add_argument("--fail-on-blocking", action="store_true")

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

    agent_runs = subparsers.add_parser("agent-runs", help="List or fetch persisted agent runs")
    agent_runs.add_argument("--id", default=None, help="Optional agent_run_id to fetch")
    agent_runs.add_argument("--agent-name", default=None, help="Optional agent name filter")
    agent_runs.add_argument("--status", default=None, help="Optional status filter")
    agent_runs.add_argument("--source", default=None, help="Optional source key filter")
    agent_runs.add_argument("--limit", type=int, default=50, help="Maximum runs to return")

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

    embedding_maintenance = subparsers.add_parser(
        "embedding-maintenance",
        help="Prune orphan embeddings and report active-model coverage",
    )
    embedding_maintenance.add_argument("--source", default=None, help="Optional source key filter")
    embedding_maintenance.add_argument("--object-type", default=None, help="Optional research object type filter")
    embedding_maintenance.add_argument(
        "--embedding-model",
        default=LOCAL_HASH_EMBEDDING_MODEL,
        help="Active embedding model to enforce coverage against",
    )
    embedding_maintenance.add_argument(
        "--prune-model",
        default=None,
        help="Optional embedding model to prune; defaults to all models",
    )
    embedding_maintenance.add_argument("--dry-run", action="store_true", help="Count orphan rows without deleting them")
    embedding_maintenance.add_argument("--fail-on-error", action="store_true", help="Exit non-zero if maintenance fails")

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
    elif args.command == "embedding-maintenance":
        output = maintain_embedding_index(
            repo,
            embedding_model=args.embedding_model,
            prune_embedding_model=args.prune_model,
            source_key=args.source,
            object_type=args.object_type,
            prune_orphans=not args.dry_run,
        ).to_report()
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
    summary = {
        "agent_run_id": run.get("agent_run_id"),
        "agent_name": run.get("agent_name"),
        "status": run.get("status"),
        "source_key": run.get("source_key"),
        "partition_date": run.get("partition_date"),
        "completed_at": run.get("completed_at"),
        "schedule_readiness": output_payload.get("schedule_readiness"),
        "should_block_schedule": output_payload.get("should_block_schedule"),
        "selected_model": evidence.get("selected_model"),
        "errors": (output_payload.get("errors") or [])[:5],
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
    return summary


if __name__ == "__main__":
    main()
