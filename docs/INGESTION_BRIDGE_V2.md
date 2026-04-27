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
