# X/Twitter Topic Monitoring Source Spec

Status: planning spec only
Owner: TWOG / HSA AutoResearch
Date: 2026-04-28

## Purpose

Add a compliant X/Twitter topic-monitoring lane that can surface timely HSA,
canine oncology, comparative oncology, vascular sarcoma, drug, target, trial,
and safety discussions for human review. This lane is not a scholarly evidence
source and must not auto-promote social posts into biological claims.

The first implementation should stay isolated from current full-text parser
work. Do not edit `harvesters_v2.py`, `source_registry.py`, `source_sets.py`,
`dagster_assets.py`, or tests until the parser work settles.

## Compliance Boundary

Use the official X API as the primary and only default collection path.
Official docs currently expose:

- Recent Search: `GET /2/tweets/search/recent`, covering posts from the last 7
  days for all developers.
- Full-Archive Search: complete archive access for pay-per-use or Enterprise
  customers.
- Filtered Stream: `GET /2/tweets/search/stream` plus stream rule management at
  `/2/tweets/search/stream/rules`, for near-real-time matching.
- Compliance APIs/streams for downstream delete, withheld, and account-status
  handling.

References:

- https://docs.x.com/x-api/posts/search/introduction
- https://docs.x.com/x-api/posts/filtered-stream/introduction
- https://docs.x.com/x-api/overview
- https://developer.x.com/overview/terms/agreement

TwitterAPI.io provider option:

- Use `GET https://api.twitterapi.io/twitter/tweet/advanced_search` with the
  `x-api-key` header when the operator chooses not to sign up for official X
  API access.
- Treat TwitterAPI.io output as a third-party read-only provider. Keep the same
  retention modes, review queue, dedupe keys, and "not scientific evidence"
  rule.
- Keep requests small. Their public docs state that each page returns up to
  roughly 20 tweets and recommend time-windowing rather than deep pagination.
- Required env var: `TWITTERAPI_IO_KEY`.

Scraping constraints:

- Do not scrape X web pages, timelines, search results, embedded pages, or
  JavaScript-rendered views as a substitute for API access.
- Do not use browser automation to bypass login, rate limits, paywalls, API
  tier limits, robots/TOS controls, or deleted/withheld-content restrictions.
- If an operator asks for non-API capture, route it through the existing
  scraper bridge only as a manual compliance review item, with source-profile
  review set to `manual_only` or equivalent. It should not fetch from X until
  legal/TOS review explicitly approves the exact use case.
- Store only fields allowed by the active X Developer Agreement and configured
  plan. Keep a policy toggle for text-retention mode: `store_text`,
  `store_metadata_only`, or `store_post_id_only`.

## Source And Query Model

Planned source key: `x_topic_monitor`.

Suggested `ResearchSource` shape when code work begins:

```json
{
  "source_key": "x_topic_monitor",
  "display_name": "X Topic Monitor",
  "source_kind": "internal",
  "base_url": "https://x.com/",
  "documentation_url": "https://docs.x.com/x-api",
  "license_policy": "x_api_plan_and_developer_agreement",
  "requires_api_key": true,
  "priority": 300,
  "phase": 4,
  "capabilities": [
    "recent_search",
    "filtered_stream",
    "topic_monitoring",
    "compliance_replay"
  ]
}
```

The current `SourceKind` enum has no social/news value. Keep the first source
registration as `INTERNAL` unless a small, deliberate contract change adds
`SOCIAL_MONITORING`.

Source queries should live in `SourceQuery` rows and use rule groups rather
than one broad query. Initial tracks:

- `disease_monitoring`: `"canine hemangiosarcoma" OR "dog hemangiosarcoma" OR
  "canine haemangiosarcoma" OR "angiosarcoma" OR "vascular sarcoma"`
- `trial_monitoring`: disease terms plus `trial OR study OR recruiting OR
  enrollment OR veterinary clinical trial`
- `therapy_target_monitoring`: disease terms plus priority compounds and
  targets from `query_policy.py`, including doxorubicin, propranolol,
  toceranib, sirolimus, paclitaxel, VEGF, VEGFR, KIT, PI3K, AKT, MTOR, CD47
- `safety_monitoring`: drug terms plus `dog OR canine` and `adverse OR toxicity
  OR side effect OR death OR bleeding OR cardiotoxicity`
- `expert_watchlist`: posts from approved accounts, lists, or institutions
  after manual allowlist review

Default query params:

```json
{
  "api_mode": "recent_search",
  "language": "en",
  "exclude_retweets": true,
  "exclude_replies": false,
  "max_results": 100,
  "retention_mode": "store_metadata_only",
  "manual_review_required": true,
  "comparative_policy": "required",
  "compliance_sync_required": true
}
```

## Normalized Objects

Social posts should become monitored signals, not publications. Until the
contract grows a dedicated object type, normalize accepted records as
`ResearchObjectType.KNOWLEDGE_ENTRY` with:

- `source_key`: `x_topic_monitor`
- `source_record_id`: X post ID
- `canonical_url`: `https://x.com/{username}/status/{post_id}` when username is
  available
- `title`: short deterministic summary such as `X post by @user on YYYY-MM-DD`
- `published_at`: post creation timestamp
- `dedupe_key`: `x_topic_monitor:post:{post_id}`
- `identifiers`: `{ "x_post_id": "...", "x_author_id": "..." }`
- `metadata`: matched rule tag, query name, author metrics if permitted,
  public engagement metrics if permitted, conversation ID, referenced post IDs,
  language, retention mode, compliance status, and review status

Create a single `DocumentChunk` only when retention policy permits storing post
text. Otherwise store a compact metadata-only chunk such as:
`X post metadata signal: matched disease_monitoring rule; text withheld by
retention policy.`

## Dedupe And Provenance

Deduplicate in three layers:

1. Raw record: use `content_hash` over stable JSON containing post ID, edit
   history IDs, query/rule tag, retrieval timestamp bucket, and permitted
   payload fields.
2. Research object: unique `dedupe_key = x_topic_monitor:post:{post_id}`.
3. Signal cluster: metadata field `signal_cluster_key`, built from normalized
   URL, DOI, PMID, NCT ID, drug/target/disease mentions, or quoted link when
   present.

Every promoted object must preserve:

- API endpoint and query/rule tag.
- Retrieval time and fetch run ID.
- X post ID, author ID, conversation ID, and edit history IDs when supplied.
- Retention mode and allowed fields policy.
- Compliance status: `active`, `deleted`, `withheld`, `author_suspended`, or
  `unknown`.

Agent-created summaries, tags, or review decisions must point back to the
post object and, when text is retained, the source chunk.

## Content Quality Filters

Filtering should happen before review queue insertion, but conservatively.
Reject or downrank:

- Non-English posts unless a query is explicitly multilingual.
- Retweets by default; keep quote posts only when original context is useful.
- Posts with no disease, compound, target, trial, institution, paper, dataset,
  or safety term after URL expansion metadata is considered.
- Spam-like posts: repeated promotional language, unrelated crypto/finance
  tags, excessive hashtags, engagement bait, or known blocked domains.
- Low-context posts under a minimum useful length unless from an approved
  expert/institution account or containing a high-value link.
- Duplicate link-only posts already represented by a publication, trial, or
  dataset object.

Quality score inputs:

- Topic match specificity.
- Author type: institution, clinician/researcher, journal, trial registry,
  patient/owner anecdote, commercial account, unknown.
- Link quality: DOI, PubMed, PMC, ClinicalTrials.gov, AVMA VCTR, university,
  FDA, company press release, generic media, unknown.
- Engagement velocity is a watch signal only, not evidence quality.
- Safety/anecdote terms should increase review priority but keep evidence level
  `UNKNOWN` or `REVIEW`.

## Manual Review Queue

Reuse the scrape-review pattern conceptually, but create social-specific
contracts when implementation begins rather than overloading
`ScrapeReviewRecord`.

Suggested review statuses:

- `needs_review`
- `accepted_signal`
- `rejected_noise`
- `needs_followup_source`
- `compliance_hold`
- `expired_or_deleted`

Review form fields:

- Review decision and reviewer.
- Topic tags.
- Whether the post points to a durable source that should be harvested by an
  existing API source.
- Whether any claim extraction is allowed.
- Whether text must be redacted or metadata-only retention is required.
- Notes for follow-up agents.

Promotion rules:

- Posts never auto-promote to durable scientific claims.
- Accepted posts may create or tag a `knowledge_entry` object as a monitoring
  signal.
- If a post links a paper, trial, dataset, safety report, label, or source
  artifact, queue the durable source harvester and attach the X post only as
  discovery provenance.
- Anecdotes and safety reports may create `SAFETY_SIGNAL` draft claims only
  with `evidence_level=UNKNOWN`, `curation_status=needs_review`, and explicit
  wording that the source is an unverified social report.

## Storage And Tagging

Use existing storage concepts:

- `raw_source_records`: API payload or permitted metadata payload.
- `research_objects`: accepted monitoring signal as `knowledge_entry`.
- `document_chunks`: retained text or metadata-only signal chunk.
- `claims`: only review-only draft claims when explicitly allowed.
- `tag_assignments`: topic, source, quality, safety, and follow-up tags.
- `agent_runs`, `approval_events`, `async_runs`: review and compliance audit.

Recommended tag sets:

- `x_topic`: `disease`, `trial`, `therapy`, `target`, `safety`, `paper`,
  `dataset`, `expert`, `patient_owner_anecdote`, `commercial`
- `x_quality`: `high_signal`, `needs_source_followup`, `duplicate_link`,
  `low_context`, `spam_like`, `compliance_hold`
- `hsa_scope`: reuse comparative ontology concepts such as `canine_hsa`,
  `human_angiosarcoma`, `vascular_sarcoma_analog`,
  `comparative_oncology`

Do not store private data, DMs, protected-account content, deleted text, or
content outside the active API plan.

## Agent Review Workflow

1. `x_topic_fetcher` reads active `SourceQuery` rows and calls Recent Search or
   Filtered Stream with configured expansions and fields.
2. `x_topic_filter_agent` applies deterministic topic, spam, source-link, and
   retention filters. It emits review candidates, not claims.
3. `x_topic_review_agent` drafts review notes, suggested tags, likely durable
   follow-up sources, ingestion flags, and compliance warnings. In hosted
   Dagster this attempts OpenRouter by default; deterministic review remains
   available for local tests and as the guardrail fallback.
4. Human reviewer accepts, rejects, or places candidates on compliance hold.
5. `x_topic_promoter` creates `RawSourceRecord`, `ResearchObject`,
   `DocumentChunk`, and tag assignments for accepted signals only.
6. `source_followup_agent` queues existing source harvesters for linked DOI,
   PMID, PMCID, NCT, AVMA VCTR, FDA, PubChem, ChEMBL, UniProt, RCSB, GEO, or
   SRA identifiers.
7. `x_compliance_sync` periodically checks API compliance state and marks
   stored records deleted/withheld or removes retained text according to policy.

## First Implementation Steps

Keep the first PR code-light and isolated from full-text parser files.

1. Add social-monitoring contracts in a new module such as
   `src/hsa_research/ingestion_bridge/x_topic_monitor.py`. Done.
2. Add tests for query building, TwitterAPI.io request normalization, dedupe
   keys, retention modes, and review gating. Done.
3. Add a feature flag: `HSA_X_TOPIC_MONITOR_ENABLED=false` by default.
4. Add environment variables: `X_BEARER_TOKEN` or `TWITTERAPI_IO_KEY`,
   `HSA_X_RETENTION_MODE`, and `HSA_X_COMPLIANCE_REQUIRED=true`.
5. Add a dry-run CLI command that builds API requests and normalizes fixture
   payloads without writing to the repository.
6. Add repository persistence only after the review queue contract is settled.
7. Register `x_topic_monitor` in `source_registry.py` as disabled by default.
   Done.
8. Add Dagster manual review asset/job after local dry-run and fixture tests.
   Done as `x_topic_monitor_review_report` / `x_topic_monitor_review_job`.
   The job now fetches candidates, runs `x_topic_review_agent`, and emits an
   ingestion-recommendation table for durable linked sources. Do not add
   schedules until persistence, compliance sync, and manual review promotion
   are working.

Initial acceptance criteria:

- No scraping of X pages.
- No writes without `manual_review_required`.
- No social post promoted as scientific evidence without a durable source.
- Dedupe by post ID and cluster related posts by durable linked source.
- Compliance status can remove/redact stored text.
- All agent outputs include provenance and reviewer-visible rationale.

## Implementation Started

Created `src/hsa_research/ingestion_bridge/x_topic_monitor.py` as an isolated
request, provider, and normalization module. It builds official X Recent Search
request shapes, builds and runs bounded TwitterAPI.io Advanced Search requests
when `TWITTERAPI_IO_KEY` is configured, builds Filtered Stream rule payloads,
defines local retention/review models, scores candidate posts conservatively,
and converts accepted review candidates into `RawSourceRecord`,
`ResearchObject`, and `DocumentChunk` contracts without writing to the
repository. Dagster exposes a manual-only `x_topic_monitor_review_job` that
returns review candidates, persists an `x_topic_review_agent` ledger row, and
surfaces source-ingestion recommendations for linked DOI/PubMed/PMC/NCT/PubChem/
ChEMBL/UniProt/RCSB/GEO/SRA records. It does not persist posts or promote them
as evidence.
