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
- PMC OA may pass relevance on licensed body text even when title/abstract
  metadata is sparse.
- Every hosted smoke source must produce raw records, research objects, chunks,
  and at least one claim.

Allowed claims:
- Source-context `OTHER` claims for sparse but relevant scholarly records.
- Typed biological claims only when the chunk text contains the required
  compound, target, biomarker, pathway, safety, or translation terms.

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

Dagster asset check:
- `structured_source_pipeline_has_minimum_outputs`

### Extract Claims

1. Confirm the source has chunks.
2. Run source-specific extraction.
3. Expect structured sources to produce only their allowed claim types.
4. If extraction returns zero for a structured source, inspect whether the
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
