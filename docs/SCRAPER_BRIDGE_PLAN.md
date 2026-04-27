# Scraper Bridge Plan

Status: phase 1 foundation implemented
Owner: TWOG / HSA AutoResearch
Date: 2026-04-26

## Purpose

The scraper bridge is for sources without stable APIs. It should not be mixed
into the API harvester layer. API harvesters fetch typed records from documented
interfaces. Scraper jobs collect web pages or downloadable files under stricter
controls, then hand normalized raw records to the same ingestion pipeline.

First candidate sources:

- AVMA Veterinary Clinical Trials Registry detail pages, only when API metadata is insufficient
- NCI COTC pages if no structured endpoint is available
- veterinary school trial pages
- foundations or hospital pages with canine oncology trial listings
- static tables or PDFs with safety, trial, or protocol metadata

Live testing on 2026-04-26 found a public AVMA/Studypages JSON search endpoint:
`https://veterinaryclinicaltrials.org/avma/studies/search/json/`. AVMA VCTR is
therefore an API-backed v2 harvester for trial-card metadata. This scraper plan
now treats AVMA scraping as optional detail/provenance capture, not the primary
ingestion route.

## Design Rules

1. Scraping is opt-in per source.
2. Every source needs a source profile before crawling.
3. The source profile stores robots/TOS notes, allowed URL patterns, rate limits,
   parser type, and license/storage policy.
4. Fetching and parsing are separate stages.
5. Raw HTML/PDF snapshots are stored as artifacts with content hashes.
6. Parsed records are reviewed before they become canonical research objects.
7. Crawlers do not execute expensive downloads without approval.
8. Browser automation is a fallback, not the default.

## Implemented Phase 1 Contracts

Source profile:

```json
{
  "source_key": "avma_vctr",
  "display_name": "AVMA Veterinary Clinical Trials Registry",
  "base_url": "https://veterinaryclinicaltrials.org/",
  "allowed_url_patterns": ["https://veterinaryclinicaltrials.org/*"],
  "robots_policy": "reviewed",
  "rate_limit_per_minute": 10,
  "parser": "html_list_detail",
  "storage_policy": "link_and_registry_metadata",
  "approval_required": true
}
```

Scrape fetch output:

```json
{
  "source_key": "avma_vctr",
  "fetched_pages": 12,
  "skipped_pages": 4,
  "artifact_ids": ["..."],
  "requires_review": true
}
```

Parsed record:

```json
{
  "source_key": "avma_vctr",
  "source_record_id": "stable-detail-url-or-hash",
  "title": "Trial title",
  "canonical_url": "https://...",
  "record_type": "veterinary_trial",
  "fields": {
    "condition": "hemangiosarcoma",
    "species": "dog",
    "status": "recruiting",
    "location": "..."
  },
  "parser_confidence": 0.82,
  "review_status": "needs_review"
}
```

Approved ingest output:

```json
{
  "source_key": "avma_vctr",
  "fetch_run_id": "...",
  "artifacts_seen": 8,
  "parsed_records": 8,
  "promoted_records": 7,
  "raw_records": 7,
  "research_objects": 7,
  "document_chunks": 7,
  "skipped_records": 1
}
```

Current CLI surface:

```bash
python -m hsa_research.ingestion_bridge.cli scrape-profiles
python -m hsa_research.ingestion_bridge.cli review-scrape-profile --source avma_vctr --robots-policy reviewed --approve-fetch --reviewed-by poppa --review-note "robots/TOS/storage reviewed"
python -m hsa_research.ingestion_bridge.cli fetch-scrape --source avma_vctr --url https://veterinaryclinicaltrials.org/ --approved-by poppa
python -m hsa_research.ingestion_bridge.cli build-scrape-manifest --source avma_vctr
python -m hsa_research.ingestion_bridge.cli fetch-scrape-manifest --source avma_vctr --manifest-artifact-id "$MANIFEST_ID" --max-pages 10 --approved-by poppa
python -m hsa_research.ingestion_bridge.cli parse-scrape --source avma_vctr
python -m hsa_research.ingestion_bridge.cli list-scrape-reviews --source avma_vctr --status needs_review
python -m hsa_research.ingestion_bridge.cli review-scrape --source avma_vctr --review-id "$REVIEW_ID" --decision accepted --reviewed-by poppa
python -m hsa_research.ingestion_bridge.cli ingest-scrape --source avma_vctr --review-id "$REVIEW_ID" --approved-by poppa --approval-note "reviewed parsed records"
```

`review-scrape-profile` records source-level robots/TOS/storage review. Disabled
sources such as AVMA cannot be fetched until profile review approves controlled
fetching. `fetch-scrape` stores immutable `scrape_snapshot` artifacts.
`build-scrape-manifest` discovers likely candidate detail URLs from stored seed
pages and stores a `scrape_manifest` artifact. `fetch-scrape-manifest` fetches
candidate URLs from that manifest. `parse-scrape` stores durable
`scrape_review_records` and returns review IDs.
`review-scrape` accepts or rejects those parsed records. `ingest-scrape` is the
only command that creates canonical `raw_source_records`, `research_objects`,
and `document_chunks`, and it only promotes accepted review records with
explicit approval.

The AVMA VCTR source uses the source-specific `avma_vctr` parser only for
optional detail snapshots. It extracts conservative trial metadata from HTML
labels, visible text, and embedded study JSON, including title, condition,
species, study type, intervention, funding, status, institution, location,
investigator, eligibility, contact, summary, VCT code, outcomes, risks,
benefits, and links. Sparse pages stay low confidence and remain review-only.

## Minimal Architecture

```text
scrape_source_profile
  -> crawl_manifest
  -> fetched_web_artifacts
  -> parsed_scrape_records
  -> review_queue
  -> raw_source_records
  -> canonical_research_objects
  -> chunks
  -> claims
```

## Implementation Phases

Phase 1: static HTML and PDF discovery

- Add source profile contract. Done.
- Add polite HTTP fetcher with rate limits and content hashing. Done.
- Store immutable scrape artifacts in local artifact storage. Done.
- Add generic deterministic HTML title/link parser. Done.
- Add review status fields before promotion to canonical objects. Done.
- Add approval-gated promotion into raw records, research objects, and chunks. Done.
- Add durable `scrape_review_records` queue. Done.
- Add AVMA VCTR parser scaffold. Done.
- Add source profile review gate for disabled scrape sources. Done.
- Add manifest discovery and manifest-backed fetch. Done.
- Run live AVMA VCTR detail manifests only after source profile/robots review.

Phase 2: browser-backed extraction

- Add Playwright only for pages that require JavaScript rendering.
- Save screenshots and HTML snapshots for parser debugging.
- Keep browser jobs approval-gated because they are more fragile and expensive.

Phase 3: monitoring

- Add change detection by URL and content hash.
- Add source health checks.
- Add parser drift detection when expected fields disappear.

## Initial MCP Tools

- `scout_scrape_source`: inspect a source URL and draft a source profile.
- `fetch_scrape_manifest`: fetch allowed list/detail pages and store artifacts.
- `parse_scrape_artifacts`: parse stored artifacts into draft records.
- `review_scrape_records`: accept/reject parsed records before ingestion.

## Do Not Build Yet

- No generic unconstrained web crawler.
- No auto-clicking arbitrary pages.
- No raw bulk downloads without source-specific approval.
- No model-only parsing as the primary parser. Models can assist, but the durable
  parser should be deterministic where possible.
