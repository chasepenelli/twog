# Codex Project Instructions

This repository is the active HSA AutoResearch v2 / TWOG Bio Dagster+ project.
Future Codex sessions should treat this file as the fastest orientation layer.

## Repository

- GitHub repo: `chasepenelli/hsa-dagster`
- Production branch: `main`
- Hosted orchestration: Dagster+ organization `twogbio`, deployment `prod`
- Primary Dagster definitions: `src/hsa_research/ingestion_bridge/dagster_assets.py`
- Local CLI entrypoint: `src/hsa_research/ingestion_bridge/cli.py`
- Shared service boundary: `src/hsa_research/ingestion_bridge/service.py`
- Typed contracts: `src/hsa_research/ingestion_bridge/contracts.py`

## Operating Rules

- Do not commit secrets. Secrets belong in GitHub Actions secrets and Dagster+
  environment variables.
- Do not touch untracked `scribbles/` unless the user explicitly asks.
- Use `apply_patch` for manual edits.
- Keep changes scoped and test with the existing contract suite.
- Hosted Dagster+ deploys are triggered by pushes to `main`.

## Current Runtime Posture

- Local development storage defaults to SQLite.
- Hosted runtime uses Postgres through `HSA_STORAGE_BACKEND=postgres` and
  `HSA_DATABASE_URL`.
- OpenRouter is used only when requested by review mode/config. Deterministic
  smoke tests should use `review_mode="deterministic_only"` to avoid spend.
- X/Twitter monitoring uses TwitterAPI.io when `TWITTERAPI_IO_KEY` is present.

## Current Major Lanes

- Structured API ingestion and source health.
- Literature corpus and full-text ingestion.
- Source follow-up queues for API-resolvable leads.
- X/Twitter monitoring and linked-article triage.
- Durable research leads/watchlist for important non-ingestible items.
- Deterministic local embeddings and retrieval smoke.
- Agent run ledger.
- Citation-first research brief agents.
- Durable research brief ledger.
- Research brief queue, queue seed, and queue runner jobs.

## Validation Commands

Run these before committing meaningful changes:

```bash
git diff --check
.venv/bin/python -m compileall src tests .github/scripts
.venv/bin/pytest tests/test_ingestion_bridge_contracts.py -q
DAGSTER_HOME=/tmp/dagster-home DAGSTER_DISABLE_TELEMETRY=1 .venv/bin/dg check defs
```

## Hosted Smoke Workflow

Use GitHub Actions workflow `Launch Dagster Smoke Job` for hosted manual tests.
Useful jobs include:

- `research_brief_agent_job`
- `research_brief_library_job`
- `research_brief_queue_seed_job`
- `research_brief_queue_runner_job`
- `research_leads_job`
- `embedding_index_job`
- `embedding_maintenance_job`
- `source_health_report_job`

## More Context

Read `docs/CODEX_PROJECT_CONTEXT.md` for project history, architecture, current
state, deployed commits, hosted smoke runs, and likely next steps.
