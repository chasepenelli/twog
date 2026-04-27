# Structured Source SOPs

These SOPs define how structured API sources are allowed to enter the ingestion
bridge, what claims they may support, and how to QA them before expanding the
framework. The goal is consistency: every source gets a typed contract, an
evidence boundary, a test, and a live sample check.

## Universal Rules

1. A structured source must not be treated as free prose.
2. Every normalized object must include stable identifiers, source URL, source
   key, object type, dedupe key, license/responsible-use metadata, and a chunk
   section label.
3. Every claim extractor path must state what the source can prove and what it
   cannot prove.
4. Claim statements must include the measured identifier or observable signal
   that makes the claim auditable.
5. Curation may promote structured support claims, but promotion does not
   upgrade the source beyond its evidence boundary.
6. Live refreshes must inspect sample rows before moving on.

## Source Boundaries

### PubChem

Allowed use:
- Compound identity support.
- CID, InChIKey, formula, SMILES, IUPAC name, synonyms, and basic properties.

Not allowed:
- Treatment efficacy.
- Target modulation.
- Safety inference.

Required gates:
- `require_exact_match=true`.
- One preferred record per query term unless explicitly widened.
- Identity must match exact title or exact synonym.

Allowed claim:
- `OTHER`: compound has PubChem identity CID/InChIKey.

QA checks:
- Query term exactly matches title or synonym.
- CID and InChIKey are present.
- No salts/analogs are accepted unless the query explicitly asks for them.

### ChEMBL

Allowed use:
- Compound-target bioactivity.
- Constrained disease-relevant cell-line functional assays.

Not allowed:
- Direct HSA/angiosarcoma efficacy.
- Incidence or clinical outcome claims.
- Broad unrelated target rows.

Required gates:
- Exact molecule resolution.
- HSA/canine-relevant target gate or constrained disease-relevant cell-line
  gate.
- `Homo sapiens` or `Canis lupus familiaris` target organism where applicable.
- Standard types: `IC50`, `Ki`, `Kd`, `EC50`.
- Assay types: binding or functional.
- pChEMBL present and above the configured minimum.

Allowed claims:
- `COMPOUND_MODULATES_TARGET`: measured target bioactivity.
- `COMPOUND_AFFECTS_OUTCOME`: constrained cell-line functional activity.

QA checks:
- No viral/parasitic/off-target rows.
- No substring false positives such as `dog` inside another word.
- Statement includes standard type, value, units, and pChEMBL.

### UniProt

Allowed use:
- Target identity, protein metadata, species scope, gene/protein naming, and
  AlphaFold cross references.

Not allowed:
- Disease association by itself.
- Drug response or therapeutic effect.

Required gates:
- Query gene must exactly match a UniProt gene name.
- Human/canine organism IDs only unless explicitly widened.
- Deduplicate by gene and organism.

Allowed claim:
- `OTHER`: target has UniProtKB metadata for a species/accession.

QA checks:
- Gene match is verified.
- Organism is human or canine.
- Accession is present.
- AlphaFold IDs are recorded when available.

### RCSB PDB

Allowed use:
- Experimental structure support for a priority target.

Not allowed:
- Disease biology claims.
- Compound efficacy claims.
- Structures that only match broad full-text noise.

Required gates:
- Structure title must match a configured target alias.
- Experimental method must be recorded when available.
- PDB ID must be present.

Allowed claim:
- `OTHER`: RCSB contains an experimental structure supporting target structure
  context.

QA checks:
- No full-text false positives.
- Title contains the target alias.
- PDB ID and method are present.

### openFDA Animal Events

Allowed use:
- Veterinary safety signal generation.
- Matched drug, dog species, reported reaction terms, seriousness, outcome.

Not allowed:
- Incidence.
- Causality.
- Efficacy or comparative treatment claims.

Required gates:
- Matched drug must correspond to the query term.
- Species must be dog for the default safety query.
- Reaction names must be separated from reaction codes.
- Statement must explicitly say signal only, not incidence or causality.

Allowed claim:
- `SAFETY_SIGNAL`: openFDA animal adverse event signal report.

QA checks:
- No numeric reaction codes in the reaction term list.
- Matched drug appears in title/metadata.
- Safety statement includes the responsible-use limitation.

### Scholarly Metadata APIs

Sources:
- `openalex`
- `pubmed`
- `europe_pmc`
- `crossref`
- `pmc_oa`

Allowed use:
- Publication metadata, abstracts where available, and legal full-text chunks
  where licensing allows it.
- HSA, human angiosarcoma, vascular sarcoma, and comparative oncology source
  triage.
- Low-confidence source-context claims when a relevant sparse record has no
  target, compound, pathway, or biomarker terms.

Not allowed:
- Treating a title-only metadata record as proof of mechanism or efficacy.
- Promoting sparse source-context claims into biological claims without deeper
  extraction.

Required gates:
- Comparative oncology policy must remain enabled by default.
- Normalized objects must retain DOI, PMID, PMCID, source URL, and source
  policy metadata when present.
- PMC OA and Europe PMC may pass relevance on licensed/open-access body text
  even when title/abstract metadata is sparse.
- Every hosted smoke source must produce raw records, research objects, chunks,
  and at least one claim.

Allowed claims:
- Source-context `OTHER` claims for sparse but relevant scholarly records.
- Typed biological claims only when the chunk text contains the required
  compound, target, biomarker, pathway, safety, or translation terms.
- Source-context claims are review-only triage claims; they should not be
  auto-promoted as biological evidence.

QA checks:
- Europe PMC and Crossref must not pass hosted smoke validation with zero
  claims.
- Sparse source-context samples must clearly state they are triage context, not
  efficacy or mechanism.

## Task SOPs

### Add Or Modify A Structured Harvester

1. Define source boundary before writing code.
2. Add or update query parameters in `query_policy.py`.
3. Normalize stable identifiers and source-specific metadata.
4. Add quality gates in `fetch`; avoid relying only on downstream filtering.
5. Add chunk text that contains exactly the fields needed for claim extraction.
6. Add tests for normalizer behavior and at least one quality-gate failure mode.
7. Run focused tests, then full contract tests.

Commands:

```bash
.venv/bin/python -m pytest tests/test_ingestion_bridge_contracts.py -k "source_key_or_feature"
.venv/bin/python -m pytest tests/test_ingestion_bridge_contracts.py
```

### Refresh A Structured Source

1. Run `init` so stored source query params match code.
2. Remove existing rows for only the source being refreshed.
3. Run live ingestion.
4. Inspect stored objects and chunks.

Commands:

```bash
.venv/bin/python -m hsa_research.ingestion_bridge.cli init
.venv/bin/python -m hsa_research.ingestion_bridge.cli ingest-source --source chembl --limit 50
.venv/bin/python -m hsa_research.ingestion_bridge.cli coverage
```

### Run The Structured Pipeline

Use this when the source has already passed its individual harvester SOP and
you want the standard refresh to run end to end. The pipeline runs ingestion,
claim extraction, claim curation, and QA for the selected structured sources.

Default structured sources:
- `pubchem`
- `chembl`
- `uniprot`
- `rcsb_pdb`
- `openfda_animal_events`

Command:

```bash
.venv/bin/python -m hsa_research.ingestion_bridge.cli structured-pipeline
```

Run one source while debugging:

```bash
.venv/bin/python -m hsa_research.ingestion_bridge.cli structured-pipeline --source chembl --limit 50
```

Expected output:
- Per-source ingestion result.
- Per-source extraction result.
- Per-source curation summary without per-claim decision noise.
- QA counts for raw records, research objects, chunks, claims, curation status,
  claim types, and sample claims.

Stop conditions:
- Any source returns `passes_minimum_bar=false`.
- Any extraction or curation errors appear.
- Claim types fall outside the source boundary listed above.
- Sample claims overstate what the source can prove.

### Run The Dagster Structured Job

Dagster wraps the same local pipeline through `structured_source_pipeline_report`.
Use this after the CLI path works and you want schedule visibility, asset checks,
and run history.

Command:

```bash
.venv/bin/dagster dev -m hsa_research.ingestion_bridge.dagster_assets
```

Dagster job:
- `structured_source_pipeline_job`

Dagster schedule:
- `structured_source_pipeline_weekly_schedule`
- Cron: `0 2 * * 1`
- Timezone: `America/Denver`
- Default status: running.
- Purpose: refresh PubChem, ChEMBL, UniProt, RCSB PDB, and OpenFDA animal
  event sources weekly without mixing them into the daily literature corpus.

Dagster asset check:
- `structured_source_pipeline_has_minimum_outputs`

### Run The Hosted All-API Smoke

Use this before broadening the ingestion framework. It validates every current
API harvester under one Dagster job and then checks persisted source counts.

GitHub Actions workflow:

```text
Launch Dagster Smoke Job -> all_api_smoke_job
```

Required QA output:
- Every source in `ALL_API_SMOKE_KEYS` has at least one raw record, research
  object, document chunk, and claim.
- Sparse scholarly source-context claims remain `needs_review`.
- Licensed full-text sources can produce `full_text` chunks and typed claims.

Dagster schedule:
- `all_api_smoke_weekly_schedule`
- Cron: `0 3 * * 2`
- Timezone: `America/Denver`
- Default status: running.
- Purpose: lightweight API reachability across every implemented source,
  including clinical, omics, and canine sources that do not yet have deeper
  scheduled refresh lanes.

### Run The Literature Corpus Harvest

Use this when you want to prove the system can build an organized paper corpus
instead of only validating source connectivity. This job targets hundreds of
persisted literature records from metadata and abstract sources only. Legal
full-text harvests run in a separate slower lane.

GitHub Actions workflow:

```text
Launch Dagster Smoke Job -> literature_corpus_harvest_job
```

Sources and per-query limits:
- `openalex`: 100
- `pubmed`: 100
- `crossref`: 100

Required QA output:
- At least 200 raw records.
- At least 100 canonical research objects.
- At least 100 document chunks.
- At least 50 claims.
- No ingestion, extraction, or curation errors.

Organization standard:
- Every persisted object must keep `source_key`, canonical URL or source URL,
  identifiers, source query metadata, and policy match metadata.
- Full-text records must use `section_label=full_text`.
- Crossref remains a triage source until the specialized review agent promotes
  source-context claims into evidence.

Dagster schedule:
- `literature_corpus_daily_schedule`
- Cron: `0 1 * * *`
- Timezone: `America/Denver`
- Default status: running after the first hosted corpus run completed cleanly.

### Run The Full-Text Literature Refresh

Use this for slower licensed full-text ingestion. Keep this smaller than the
metadata corpus job because Europe PMC and PMC OA perform additional full-text
fetches and parsing.

GitHub Actions workflow:

```text
Launch Dagster Smoke Job -> literature_full_text_refresh_job
```

For faster diagnosis, run the single-source lanes first:

```text
Launch Dagster Smoke Job -> literature_full_text_smoke_job
Launch Dagster Smoke Job -> europe_pmc_full_text_refresh_job
Launch Dagster Smoke Job -> pmc_oa_full_text_refresh_job
```

Sources and per-query limits:
- `europe_pmc`: 10
- `pmc_oa`: 3

Single-source full-text jobs use the same source-specific limits as the combined
refresh. `literature_full_text_smoke_job` uses one record per source and is the
preferred hosted readiness check before the combined refresh.

Required QA output:
- Every full-text source has raw records, research objects, document chunks, and
  claims.
- Title/abstract text is chunked as `section_label=title_abstract`; licensed
  body text is chunked separately as `section_label=full_text`.
- `full_text_qa.passes_full_text_bar` is true, with at least one persisted
  `full_text` body chunk and at least one current-run `full_text` body chunk.
- No ingestion, extraction, or curation errors.
- Re-ingesting a refreshed object replaces its prior chunks and clears derived
  chunk-level embeddings and entity mentions, so stale body chunks cannot make a
  failed refresh look healthy.
- Full-text body fetches use a shorter timeout and a bounded candidate scan.
  Tune `HSA_FULL_TEXT_REQUEST_TIMEOUT_SECONDS`,
  `HSA_FULL_TEXT_FETCH_TIME_BUDGET_SECONDS`, and
  `HSA_PMC_OA_MAX_CANDIDATE_RECORDS` only when a hosted run proves the default
  lane is too conservative.

Dagster schedule:
- `literature_full_text_weekly_schedule`
- Cron: `0 2 * * 0`
- Timezone: `America/Denver`
- Default status: stopped until the full-text lane has a clean hosted run.

### Run Source Health

Use this after a smoke run or scheduled refresh. It is stricter than coverage:
every source gets a health status, score, risks, and recommended actions.

Command:

```bash
.venv/bin/python -m hsa_research.ingestion_bridge.cli source-health --fail-on-failed-sources
```

Dagster job:
- `source_health_report_job`

Dagster schedule:
- `source_health_daily_schedule`
- Cron: `15 6 * * *`
- Timezone: `America/Denver`
- Default status: running after embedding maintenance.

Health status:
- `healthy`: required records, objects, chunks, claims, samples, and promoted
  claim signals are present.
- `triage`: required persisted outputs are present, and the source is
  intentionally routed to the specialized triage agent before claims become
  evidence. Current triage-only sources are `sra` and `crossref`.
- `watch`: hard persisted outputs are present, but QA found review-heavy,
  context-only, or otherwise weak evidence signals.
- `failing`: required persisted outputs are missing or the score is below the
  minimum health bar.

Stop conditions:
- Any source appears in `failed_sources`.
- Any source that should produce typed evidence only appears in `watch` because
  all sampled claims are source-context triage claims.
- Recommended actions point to missing raw records, normalized objects, chunks,
  or extraction/curation outputs.

### Run The Deterministic Entity Resolver

Use this after chunks exist and before model-backed enrichment. The resolver
normalizes entity mentions; it does not infer biology, promote evidence, or
change source boundaries.

Command:

```bash
.venv/bin/python -m hsa_research.ingestion_bridge.cli resolve-entities --source pubmed --limit 1000
```

Resolver profiles:
- `local`: high-precision local vocabulary from the existing compound, target,
  biomarker, pathway, and disease dictionaries.
- `pubtator`: PubTator BioC JSON annotations for PMID-backed publications.
- `local_plus_pubtator`: run both deterministic sources.

Dagster job:
- `entity_resolution_job`

Required output:
- `resolved_entities` has canonical entity rows with `entity_type`,
  `canonical_name`, and `normalized_key`.
- `entity_aliases` keeps deterministic aliases for each canonical entity.
- `entity_mentions` keeps chunk-level spans with `chunk_id`,
  `research_object_id`, `chunk_index`, `section_label`, resolver provenance, and
  match rule.

Stop conditions:
- A stable external ID maps to more than one canonical entity.
- A mention is linked to multiple entities.
- The resolver drops source chunk provenance.
- A resolver profile changes claim curation status or source boundaries.

### Extract Claims

1. Confirm the source has chunks.
2. Run `resolve-entities` or verify the structured pipeline has produced
   `entity_mentions`.
3. Run source-specific extraction.
4. Expect structured sources to produce only their allowed claim types.
5. If extraction returns zero for a structured source, inspect whether the
   source needs a typed extractor path.

Command:

```bash
.venv/bin/python -m hsa_research.ingestion_bridge.cli extract-claims --source chembl --limit 100
```

### Curate Claims

1. Use source filter.
2. Use summary mode first.
3. Inspect promoted and merged examples.
4. Keep source boundaries in the statement text.

Command:

```bash
.venv/bin/python -m hsa_research.ingestion_bridge.cli curate-claims --source chembl --limit 100 --promote-threshold 0.5 --summary-only
```

### QA Sample Rows

Run SQL checks after every live refresh. Replace the source key and fields as
needed.

```bash
sqlite3 var/hsa_research/ingestion_bridge.sqlite3 \
  "select title, json_extract(payload,'$.metadata.target_gene'), json_extract(payload,'$.metadata.target_category') from research_objects where source_key='chembl' limit 20;"
```

Required QA output:
- Object count and chunk count.
- Representative rows by source.
- Claim extraction count.
- Curation summary.
- At least three promoted claim examples when claims are expected.

## Stop Conditions

Stop and fix before moving on when:
- A source produces broad off-topic rows.
- A source produces zero claims where typed claims are expected.
- Claims exceed the source boundary.
- Metadata mixes labels and codes in the same field.
- Query parameters in SQLite do not match code.
- Live samples include false positives that tests do not cover.
