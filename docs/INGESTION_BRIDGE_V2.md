# Ingestion Bridge v2

Status: agreed foundation
Owner: TWOG / HSA AutoResearch
Date: 2026-04-25

## Purpose

Ingestion Bridge v2 rebuilds the research intake layer around durable research objects and provenance-backed claims. The old bridge is useful, but it is too paper-centered. The new bridge treats papers, trials, datasets, chemical records, safety reports, structures, and validation outputs as first-class research objects.

One policy is non-negotiable: scholarly ingestion must always include human angiosarcoma and close vascular sarcoma analogs alongside canine HSA. Canine HSA is part of a comparative oncology surface, not a silo.

The core flow is:

```text
source -> raw record -> canonical research object -> legal artifact/full text
-> chunks -> entities -> claims -> tags -> evidence graph -> candidate decisions
```

This is the foundation for the frontier system. Dagster runs the durable data and compute flows. MCP exposes clean typed tools to Claude, OpenAI, vet partners, and future contributors. RunPod/Docker handles expensive GPU work.

## Lean v1 Stack

The first build avoids extra platform weight.

```text
Claude / OpenAI
      |
MCP server
      |
Typed HSA service layer
      |
Dagster + local SQLite/artifact storage + RunPod
      |
TWOG lightweight dashboard
```

In v1 we are intentionally not adding LiteLLM or Langfuse. Model switching can be handled by simple model profiles and operator instructions. Observability is handled by internal tables for tool calls, model calls, agent runs, Dagster links, artifact links, and approvals.

The storage posture is local-first. The default v2 service uses SQLite at `var/hsa_research/ingestion_bridge.sqlite3` plus filesystem artifacts. Postgres/Supabase remains a future deployment adapter, not a dependency for building the new system.

Local bootstrap:

```bash
python -m hsa_research.ingestion_bridge.cli init
python -m hsa_research.ingestion_bridge.cli coverage
python -m hsa_research.ingestion_bridge.cli backfill-papers --path hsa_research/papers.json
python -m hsa_research.ingestion_bridge.cli backfill-deep-dives --dir docs/deep_dives
python -m hsa_research.ingestion_bridge.cli ingest --source openalex --query '"canine hemangiosarcoma"' --limit 25
```

## Design Principles

1. Harvesters collect; they do not reason.
2. Resolvers normalize; they do not invent.
3. Agents propose claims and hypotheses; they do not silently mutate durable knowledge.
4. Every durable claim must have provenance.
5. Every tag assignment must identify what assigned it, why, and with what confidence.
6. Expensive compute is async by default.
7. MCP tools are the public contract for humans and LLMs.
8. Dagster assets are the durable operating graph.

## Source Classes

Phase 1 sources:

| Class | Sources | Purpose |
| --- | --- | --- |
| Scholarly backbone | PubMed, Europe PMC, OpenAlex, Crossref | Metadata, identifiers, citations, journal coverage |
| Open access text | Europe PMC OA, PMC OA, Unpaywall, DOAJ | Legal full text and artifact discovery |
| Current repo bridge | Existing `papers`, `corpus_knowledge_entries` | Backfill current knowledge into the new model |

Phase 2 sources:

| Class | Sources | Purpose |
| --- | --- | --- |
| Canine oncology | ICDC, NCI COTC, AVMA VCTR, ClinicalTrials.gov | Comparative oncology, canine trials, outcomes |
| Omics | GEO, SRA, ENA, Expression Atlas | Expression, sequencing, dataset evidence |
| Chemistry | PubChem, ChEMBL, BindingDB, TTD, canSAR | Compounds, assays, targets, bioactivity |
| Targets and structures | UniProt, Ensembl dog, NCBI Gene, RCSB PDB, AlphaFold DB | Species mapping, target structures, homology |
| Veterinary safety | FDA Green Book, openFDA animal adverse events, DailyMed | Canine and translational safety context |

## Research Objects

A research object is any durable source-derived object that can produce evidence:

- `publication`
- `preprint`
- `clinical_trial`
- `veterinary_trial`
- `dataset`
- `compound_record`
- `bioactivity_assay`
- `safety_report`
- `drug_label`
- `structure`
- `validation_run`
- `knowledge_entry`

Objects can have many identifiers. The resolver links DOI, PMID, PMCID, OpenAlex ID, NCT ID, AVMA study ID, GEO/SRA accession, PubChem CID, ChEMBL ID, UniProt accession, PDB ID, AlphaFold ID, and internal IDs.

Full-text sources chunk title/abstract text and licensed article body text as
separate sections. `title_abstract` chunks stay useful for metadata triage, but
`full_text` chunks must come from body text only. Full-text QA requires both
persisted body chunks and current-run body chunks, and object refreshes replace
old chunk rows plus derived chunk-level embeddings and entity mentions.
The hosted full-text lane is runtime-bounded with short body-fetch timeouts,
source-level fetch budgets, and a capped PMC OA candidate scan so API slowness
fails as source health signal instead of occupying the whole orchestration
window.

## Retrieval Foundation

Retrieval starts as a bounded storage contract, not a user-facing tool. The
`TextEmbedding` contract stores one embedding for one `DocumentChunk` and one
embedding model. SQLite and Postgres both persist it in `text_embeddings` with
query-friendly columns plus the typed payload and JSON vector. There is no
`pgvector` dependency yet; search computes cosine similarity in Python over the
stored JSON vectors.

The first indexer is local and deterministic:

- `LocalDeterministicEmbeddingProvider` creates dependency-free
  `local-hash-v1` vectors with stable SHA-256 token hashing.
- `build_chunk_embedding_text` combines `DocumentChunk` text with
  `ResearchObject` title/source/identifier context and canonical
  `EntityMention` labels.
- `index_embeddings_for_repository(repository, source_key=None, limit=None,
  embedding_model="local-hash-v1", force=False)` writes `TextEmbedding`
  records through the existing repository methods. It skips rows whose current
  embedding input hash is already stored, rebuilds when the chunk/context text
  changes, and `force=True` refreshes matching rows while preserving the
  `(chunk_id, embedding_model)` upsert identity.

Repository adapters expose:

- `upsert_text_embedding`
- `get_text_embedding`
- `list_text_embeddings`
- `search_text_embeddings`
- `embedding_coverage`
- `count_orphan_text_embeddings`
- `delete_orphan_text_embeddings`

`embedding_coverage` compares stored embeddings against durable
`document_chunks`, optionally scoped by source, object type, or model. This
lets the ingestion layer measure retrieval readiness for CLI, Dagster, and MCP
read tools.

`maintain_embedding_index(repository, embedding_model="local-hash-v1")`
deletes orphan embedding rows whose `chunk_id` no longer exists in
`document_chunks`, then reports active-model coverage. This keeps retrieval
search from carrying stale vectors after chunk ID or chunking logic changes.

Retrieval read contracts expose chunks and object context without exposing raw
vectors:

- `search_research_chunks` embeds the query with an available stored embedding
  model, calls `search_text_embeddings`, hydrates matching chunks and research
  objects, and falls back to bounded keyword search only when semantic search
  returns no hits.
- `get_chunk_context` returns one chunk, nearby sibling chunks, its canonical
  research object, and resolved entity mentions.
- `get_research_object` returns a canonical object plus a bounded chunk list.
- `run_retrieval_smoke` chains the three read tools and reports whether the
  retrieval path is usable for one query.

All retrieval tools cap result counts and returned chunk text length. Raw
embedding vectors remain an internal storage/search detail.

Dagster now exposes this local retrieval foundation through
`embedding_index_report`, which builds the configured repository resource and
runs `index_embeddings_for_repository` over stored chunks using the
deterministic `local-hash-v1` model. `embedding_index_job` materializes only
that report. Its asset check passes for an empty local store, but once chunks
exist it requires at least one readable embedding and a clean error list.

`embedding_maintenance_report` and `embedding_maintenance_job` prune orphan
embedding rows for all models, then require the active `local-hash-v1` model to
cover every live `document_chunk`. Its metadata surfaces orphan rows seen,
deleted rows, active coverage, missing chunks, and the repository coverage
snapshot. Run this after `embedding_index_job` before using scheduled retrieval
or RAG workflows.

Dagster schedules use `America/Denver` and intentionally stay staggered rather
than chained. The initial production cadence is:

- `literature_corpus_daily_schedule`: `0 1 * * *`, running.
- `structured_source_pipeline_weekly_schedule`: `0 2 * * 1`, running.
- `literature_full_text_weekly_schedule`: `0 2 * * 0`, stopped until the
  full-text lane has a clean hosted run.
- `literature_full_text_source_date_daily_schedule`: `30 2 * * *`, stopped
  until source/date partitions are reviewed in Dagster+.
- `all_api_smoke_weekly_schedule`: `0 3 * * 2`, running.
- `embedding_index_daily_schedule`: `0 5 * * *`, running.
- `embedding_maintenance_daily_schedule`: `45 5 * * *`, running.
- `source_health_daily_schedule`: `15 6 * * *`, running.

Full-text hosted debugging also exposes pull-only and end-to-end jobs. Start
with `literature_full_text_ingest_smoke_job`,
`europe_pmc_full_text_ingest_job`, and `pmc_oa_full_text_ingest_job` to isolate
fetch, normalization, persistence, chunking, and full-text QA from entity
resolution, claim extraction, and curation. Then run
`literature_full_text_smoke_job`, `europe_pmc_full_text_refresh_job`, and
`pmc_oa_full_text_refresh_job` before the combined weekly refresh when a
source-specific parser, API, or runtime issue needs isolation. The
`literature_full_text_source_date_job` is the first partitioned lane: it uses a
multi-partition key with `source` and `date`, applies source-native publication
date filters, and treats zero-record days as clean empty partitions rather than
source failures. Keep the daily schedule stopped until at least one hosted
source/date partition is reviewed.

No sensors or chained graph job are part of this first schedule pass.

CLI smoke check:

```bash
.venv/bin/python -m hsa_research.ingestion_bridge.cli retrieval-smoke \
  --query "hemangiosarcoma angiogenesis VEGFA" \
  --require-embedding \
  --fail-on-error
```

CLI maintenance check:

```bash
.venv/bin/python -m hsa_research.ingestion_bridge.cli embedding-maintenance \
  --fail-on-error
```

The hosted GitHub Actions smoke workflow runs this command after
`embedding_index_job`, so a green run proves the indexer wrote embeddings and
the MCP read path can search a chunk, retrieve its context, and fetch the
parent research object.

## Claim Types

The first claim set should stay small:

| Claim type | Example |
| --- | --- |
| `target_associated_with_disease` | VEGFA is associated with canine HSA angiogenesis |
| `compound_modulates_target` | Toceranib inhibits KIT/VEGFR/PDGFR signaling |
| `compound_affects_outcome` | Propranolol plus doxorubicin affects survival endpoint |
| `pathway_active_in_disease` | PI3K/AKT/mTOR pathway is active in HSA subset |
| `biomarker_predicts_state` | CD31 expression marks endothelial phenotype |
| `safety_signal` | Drug has canine adverse event signal |
| `species_translation` | Human target maps to canine ortholog with high identity |
| `validation_result` | Boltz/MD/docking supports or weakens candidate binding hypothesis |

Claims are not summaries. A claim is a structured assertion with direction, species, entities, confidence, evidence level, extraction metadata, and source spans.

## Comparative Query Policy

All new scholarly harvesters use the shared comparative query policy in `query_policy.py`.

Required disease surface:

- canine hemangiosarcoma and haemangiosarcoma
- dog/canine HSA wording
- human angiosarcoma
- cutaneous, cardiac, hepatic, and radiation-associated angiosarcoma
- epithelioid hemangioendothelioma / haemangioendothelioma
- vascular sarcoma and endothelial sarcoma
- comparative and translational oncology language

Source-specific starter queries can add journal, therapy, target, or full-text filters, but they should not remove this comparative disease surface unless a one-off operator explicitly disables the policy for debugging.

## Harvester v2

The fresh harvester layer is `harvesters_v2.py`. The previous `harvesters.py` file is legacy scaffold code and should not be expanded for new work.

Harvester v2 rules:

- preserve the raw source payload before normalization
- normalize into `ResearchObject`
- attach `metadata.ingestion_policy` with matched comparative concepts
- expand scholarly queries through `query_policy.py`
- keep claim extraction and scientific judgment out of harvesters

Current API-backed v2 harvesters include PubMed, Europe PMC, OpenAlex, Crossref,
PMC OA, ClinicalTrials.gov, AVMA VCTR, ICDC, GEO, and SRA. AVMA VCTR uses the
public Studypages JSON search endpoint for veterinary trial cards and stores
registry metadata plus links; detail-page snapshots remain scraper-gated when
richer fields or provenance are needed.

## MCP Tool Surface

MCP is the clean conversational interface. These tools are the v0 contract:

| Tool | Mode | Purpose |
| --- | --- | --- |
| `search_claims` | read | Search evidence-backed claims by query, target, compound, species, evidence type, confidence |
| `search_research_chunks` | read | Search chunk retrieval context using embeddings first, with bounded keyword fallback |
| `get_chunk_context` | read | Return a chunk with nearby sibling chunks and resolved entity mentions |
| `get_research_object` | read | Return a canonical research object and bounded stored chunks |
| `run_retrieval_smoke` | read | Verify search, context, and object reads work together for one retrieval query |
| `curate_claims` | write | Run the claim curator agent to dedupe, review, and promote draft claims |
| `scout_sources` | read | Run the source scout agent to prioritize ingestion gaps and starter source queries |
| `get_candidate` | read | Return a candidate dossier with evidence, lineage, validation state, and risk flags |
| `propose_hypothesis` | draft | Draft a hypothesis from claims and gaps without committing durable state |
| `commit_hypothesis` | write | Save a human-approved hypothesis |
| `run_boltz` | async compute | Queue a structure or binding prediction job |
| `request_validation` | async compute | Queue validation such as MD, docking, ADMET, homology, safety, or expert review |
| `get_run_status` | read | Check Dagster/RunPod/MCP job status |
| `get_artifact` | read | Fetch artifact metadata and links |
| `list_scrape_profiles` | read | Inspect approved non-API scrape profiles and controls |
| `review_scrape_profile` | write | Record robots/TOS/storage review before disabled sources can fetch |
| `fetch_scrape_manifest` | draft | Fetch approved scrape URLs into immutable artifacts |
| `build_scrape_manifest` | draft | Discover likely candidate detail URLs from stored seed pages |
| `fetch_scrape_manifest_candidates` | draft | Fetch candidate URLs from a reviewed manifest artifact |
| `parse_scrape_artifacts` | draft | Parse stored scrape artifacts into durable review records |
| `list_scrape_reviews` | read | Inspect parsed scrape records awaiting accept/reject decisions |
| `review_scrape_records` | write | Accept or reject parsed scrape records before ingestion |
| `ingest_scrape_records` | write | Promote reviewed scrape records into canonical local ingestion objects |

MCP resources:

```text
claim://{claim_id}
chunk://{chunk_id}
research-object://{research_object_id}
candidate://{candidate_id}
hypothesis://{hypothesis_id}
run://{run_id}
artifact://{artifact_id}
source://{source_id}
```

Sensitive or expensive tools must return a run handle and should require approval in the dashboard before execution outside local development.

## Dagster Asset Graph

The v1 Dagster assets are:

```text
source_registry
  -> source_scout_plan
  -> source_queries
  -> raw_source_records
  -> canonical_research_objects
  -> identifier_links
  -> legal_artifacts
  -> document_chunks
  -> entity_mentions
  -> normalized_entities
  -> claims
  -> curated_claims
  -> tag_assignments
  -> coverage_snapshot
```

`embedding_index_report` is an executable side report over persisted chunks and
available entity mentions; it does not block claim extraction.

Initial partitions:

- `source`
- `date`
- `object_type`
- `track`

Later partitions:

- `journal`
- `species`
- `target_gene`
- `compound`
- `evidence_type`

Asset checks should enforce:

- required identifiers are present when available
- raw records have stable content hashes
- canonical objects dedupe correctly
- no claim exists without evidence
- no tag exists without provenance
- source coverage does not silently regress

Current executable scaffold:

- `structured_source_pipeline_report`: runs structured source refresh,
  extraction, curation, and QA through the configured repository resource.
- `structured_source_pipeline_job`: Dagster job for the structured-source
  pipeline.
- `structured_source_pipeline_has_minimum_outputs`: asset check that fails when
  a structured source produces no objects or no claims, or when extraction or
  curation reports errors.
- `embedding_index_report`: runs the deterministic local embedding indexer over
  persisted chunks and reports embedding coverage.
- `embedding_index_job`: Dagster job for the local embedding index report.
- `embedding_index_has_minimum_outputs`: asset check that accepts an empty
  store, but requires at least one embedding once chunks exist.
- `embedding_maintenance_report`: prunes orphan embedding rows and reports
  active-model coverage.
- `embedding_maintenance_job`: Dagster job for embedding maintenance.
- `embedding_maintenance_has_clean_coverage`: asset check that accepts an empty
  store, but requires active-model embeddings for every live chunk once chunks
  exist.
- `structured_source_pipeline_weekly_schedule`: weekly structured API refresh.
- `literature_corpus_daily_schedule`: daily metadata/abstract literature
  corpus harvest.
- `literature_full_text_weekly_schedule`: stopped weekly full-text lane until a
  hosted full-text run is clean.
- `all_api_smoke_weekly_schedule`: weekly all-source API heartbeat.
- `embedding_index_daily_schedule`: daily deterministic embedding refresh.
- `embedding_maintenance_daily_schedule`: daily orphan cleanup and active-model
  coverage gate.
- `source_health_daily_schedule`: daily post-maintenance source health report.

Dagster executable assets receive a `research_repository` resource instead of
constructing storage directly. The resource reads:

- `HSA_STORAGE_BACKEND`: `sqlite` by default, or `postgres` for hosted durable
  runs.
- `HSA_SQLITE_PATH`: optional local SQLite path for Dagster assets.
- `HSA_DATABASE_URL`: required when `HSA_STORAGE_BACKEND=postgres`.

The executable scaffold intentionally calls the same orchestration function as
the CLI command after the repository resource has built the storage adapter:

```bash
.venv/bin/python -m hsa_research.ingestion_bridge.cli structured-pipeline
```

First-pass structured Dagster metadata is limited to the low-risk reporting
assets that already return stable dictionaries:

- `structured_source_count_report`
- `source_health_report`
- `entity_resolution_report`
- `embedding_index_report`

Each asset materializes with the full report preserved as the Dagster value so
existing asset checks can continue reading the same contract. The attached
metadata adds compact totals, source key lists, coverage summaries, and
per-source Dagster tables. Table cells must stay scalar; nested lists and
dictionaries are JSON-encoded into strings before being attached to the table
metadata.

Dagster+ deployment setup lives in `docs/DAGSTER_PLUS_SETUP.md`. Hosted runs
must use `HSA_STORAGE_BACKEND=postgres` and `HSA_DATABASE_URL`; local SQLite is
not durable inside Dagster+ Serverless workers.

## Internal Observability v1

Instead of Langfuse, v1 logs enough to inspect, replay, and audit:

- `mcp_tool_calls`
- `agent_runs`
- `model_calls`
- `approval_events`
- `artifacts`
- `source_fetch_runs`

Each tool or model call should log:

- tool or agent name
- input hash
- sanitized input
- prompt version, if any
- model profile and model name, if any
- output summary
- linked claim/candidate/hypothesis IDs
- status, latency, error
- Dagster run ID or RunPod job ID
- created by and created at

`agent_runs` is the durable ledger for agent execution. Each service-level agent
call writes the typed input payload, terminal output payload, summary, status,
source key, partition date, Dagster run id when supplied, and errors. The first
implementation is recommend-only and deterministic: no Claude/OpenAI calls are
made by the ledger or full-text ops agent, and agents do not mutate schedules or
launch retries.

## Model Profiles

No model gateway is required in v1. Agents should reference simple logical profiles:

```yaml
models:
  extractor: claude
  hypothesis: openai
  reviewer: claude
  cheap_classifier: openai
  long_context_reviewer: claude
```

The actual model can be changed by config or operator instruction. If model routing, budgets, or fallback chains become painful later, add LiteLLM then.

## First Agent: Claim Curator

The first agent is the claim curator. It reviews draft claims, merges duplicates, assigns a curation score, and promotes only claims that have enough provenance and support. The first implementation is deterministic and local so the storage contract can stabilize before Claude/OpenAI are attached.

Claim curator decisions:

- `promote`: claim can be used by normal search and hypothesis drafting.
- `merge_duplicate`: claim is retained for audit but points to a canonical claim.
- `needs_review`: claim has useful signal but should stay hidden from default search.
- `reject`: claim is too weak, underspecified, or lacks provenance.

Dagster does not need to run yet for this agent. The curator should first be callable from Python, CLI, and MCP; Dagster can schedule the same `curate_claims` contract once the local behavior is trusted.

## Second Agent: Source Scout

The source scout inspects local coverage and proposes the next ingestion bridges. Its first version compares registered sources against local raw/object counts, then adds a small set of not-yet-registered expansion targets:

- GEO and SRA for canine HSA and comparative oncology omics metadata.
- BindingDB for compound-target binding evidence.
- AlphaFold for canine ortholog structure coverage before GPU validation.
- FDA Green Book for veterinary drug label context.

The scout returns prioritized source recommendations, starter `SourceQuery` contracts, and implementation notes. This should drive source-by-source harvester work without requiring a full Dagster deployment yet.

## Full-Text Ops Agent

The full-text ops agent reviews Europe PMC and PMC OA full-text health,
triage, optional source/date partition reports, and recent related agent runs.
It returns structured recommendations such as `run_ingest_smoke`,
`run_source_date_partition`, `inspect_parser`, `inspect_license`, or
`ready_to_enable_schedule`. Its Dagster surface is
`full_text_ops_agent_report` / `full_text_ops_agent_job`, intentionally manual
only until the recommendations are trusted in hosted runs.

Full-text ops is external-review backed by default. `review_mode=external_required`
creates a typed review packet containing source health, full-text report, recent
agent runs, and the deterministic guardrail result. A ChatGPT Pro/Codex session
reviews that packet outside the hosted Dagster worker, so no API key or per-token
API spend is required. The hosted job keeps the schedule stopped until that
external review is performed. `deterministic_only` exists for tests and
break-glass debugging, not normal hosted operations.

For automated hosted review, `review_mode=openrouter_required` sends the review
packet to OpenRouter and stores the typed recommendation in the agent ledger. The
default hosted model is `~anthropic/claude-sonnet-latest`, keeping the production
lane on the latest available Claude Sonnet release without hard-coding a stale
version. `review_mode=openrouter_compare` remains available for intentional
benchmarks; pass `review_models` explicitly when comparing GPT, Sonnet, Opus, or
other candidates. The deterministic guardrail is still applied to every model
result, so an unvalidated or blocking lane cannot be marked ready by a single
model.

`full_text_source_date_ops_job` is the manual hosted bridge for source/date
validation while the Dagster Cloud CLI lacks partition-key launch support. It
accepts `source_key` and `partition_date` run config, executes one source/date
full-text slice, and passes that report directly into `FullTextOpsAgent` so the
recommendation is backed by current partition evidence. It is manual only and
does not replace the partitioned `literature_full_text_source_date_job` or its
stopped daily schedule.

## Build Phases

### Phase 0: Foundation

- Add schema migration for sources, raw records, research objects, chunks, entities, claims, tags, MCP logs, and run logs.
- Add local-first SQLite storage for the service layer.
- Add typed Python contracts.
- Add source registry.
- Add MCP server scaffold with read/draft/async tool contracts.
- Add Dagster asset scaffold.

### Phase 1: Scholarly Bridge

- Implement OpenAlex, Crossref, PubMed, and Europe PMC harvesters.
- Normalize identifiers into `research_objects` and `identifier_links`.
- Backfill current `papers` and `corpus_knowledge_entries`.
- Add legal artifact handling for OA full text.
- Add coverage report.

### Phase 2: Claim Layer

- Chunk legal text and abstracts.
- Resolve deterministic entity mentions against local and external vocabularies.
- Extract first claim types.
- Add claim review workflow and QA checks.

### Phase 2.5: Deterministic Entity Resolution

- Persist `resolved_entities`, `entity_aliases`, and `entity_mentions`.
- Run the local deterministic resolver before claim extraction.
- Keep PubTator BioC JSON as an opt-in external deterministic resolver profile.
- Store resolver name, resolver version, normalized ID, match rule, span offsets,
  source chunk, and payload hash for auditability.
- Leave ambiguous or unresolved mentions reviewable instead of guessing.

### Phase 3: Canine and Chemistry Expansion

- Add ICDC, AVMA VCTR, ClinicalTrials.gov.
- Add GEO/SRA metadata bridge.
- Add PubChem, ChEMBL, BindingDB, TTD, canSAR.
- Add FDA Green Book and openFDA animal adverse events.

### Phase 4: Validation and GPU

- Wire `run_boltz` and `request_validation` into Dagster and RunPod.
- Store structured model outputs as artifacts.
- Promote validation outputs into `validation_result` claims only after checks pass.

## First Build Target

The first code should prove these contracts, not solve all ingestion:

- `db/migrations/005_ingestion_bridge_v2.sql`
- `hsa_research/ingestion_bridge/contracts.py`
- `hsa_research/ingestion_bridge/source_registry.py`
- `hsa_research/ingestion_bridge/local_store.py`
- `hsa_research/ingestion_bridge/local_ingest.py`
- `hsa_research/ingestion_bridge/harvesters.py`
- `hsa_research/ingestion_bridge/cli.py`
- `hsa_research/ingestion_bridge/service.py`
- `hsa_research/ingestion_bridge/mcp_server.py`
- `hsa_research/ingestion_bridge/dagster_assets.py`

Once this is merged, harvester implementation can proceed source by source without changing the core shape.
