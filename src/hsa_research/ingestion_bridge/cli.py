"""Command-line entrypoint for local-first Ingestion Bridge v2."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .backfill import backfill_deep_dives, backfill_papers_json
from .claim_curator import curate_claims_for_repository
from .claim_extractor import extract_claims_for_repository
from .contracts import (
    ClaimCurationRequest,
    ScrapeFetchRequest,
    ScrapeIngestRequest,
    ScrapeManifestFetchRequest,
    ScrapeManifestRequest,
    ScrapeProfileReviewRequest,
    ScrapeReviewRequest,
    SourceQuery,
    SourceScoutRequest,
)
from .local_ingest import LocalIngestionPipeline
from .local_store import SQLiteResearchRepository
from .scraper_bridge import ScrapeBridge, list_scrape_profiles
from .source_scout import scout_sources_for_repository
from .storage import build_sql_repository
from .structured_orchestration import (
    STRUCTURED_SOURCE_KEYS,
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
        curation = curate_claims_for_repository(
            repo,
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
        scout = scout_sources_for_repository(
            repo,
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


if __name__ == "__main__":
    main()
