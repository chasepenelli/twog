# HSA AutoResearch v2

Dagster+ deployed ingestion, curation, and orchestration system for canine
hemangiosarcoma and human angiosarcoma comparative oncology research.

This repository is the active Dagster+ project for v2. Dagster+ created the
deployment repo and GitHub Actions workflow; the HSA research system now lives
inside that generated project layout.

## Current System

- Typed research contracts.
- Local SQLite development storage.
- Hosted Postgres runtime adapter for Dagster+.
- Structured API harvesters for PubChem, ChEMBL, UniProt, RCSB PDB, and openFDA
  animal adverse events.
- Source-specific claim extraction and curation.
- Dagster asset graph and executable structured-source job.
- MCP-ready service boundary.

## Local Setup

```bash
uv sync
uv run pytest tests/test_ingestion_bridge_contracts.py
```

If using pip instead:

```bash
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest tests/test_ingestion_bridge_contracts.py
```

## Dagster

Validate definitions:

```bash
uv run dg check defs
```

Run locally:

```bash
uv run dg dev
```

Production deploys are handled by the existing Dagster+ GitHub Actions workflow
on pushes to `main`.

## Current Job

Dagster job:

```text
structured_source_pipeline_job
```

This runs:

```text
structured source refresh -> claim extraction -> claim curation -> QA report
```

## Local Structured Pipeline

```bash
uv run hsa-ingestion-bridge structured-pipeline --source pubchem --limit 1
```

## Source Standards

Structured source SOPs live in `docs/STRUCTURED_SOURCE_SOPS.md`. Each source has
an explicit evidence boundary so the system does not overstate what an API
record can prove.
