# Codex Project Context

Status: active build context  
Project: HSA AutoResearch v2 / TWOG Bio research system  
Repo: `chasepenelli/hsa-dagster`  
Last updated: 2026-04-30

## Purpose

This project is rebuilding the original HSA autoresearch system as a cleaner
Dagster+ orchestrated research platform for canine hemangiosarcoma and human
angiosarcoma comparative oncology. The goal is to ingest, organize, retrieve,
triage, and synthesize scientific evidence across literature, structured APIs,
full text, X/Twitter-discovered leads, and later GPU validation lanes.

The system should favor typed contracts, durable storage, reproducible
pipelines, and explicit provenance. Agents can recommend, synthesize, triage,
and queue work, but durable evidence still comes from stored research objects,
chunks, claims, and citations.

## Current Architecture

Core layers:

- Dagster+ for orchestration and hosted visibility.
- SQLite for local development.
- Managed Postgres for hosted Dagster+ runtime.
- Typed Pydantic contracts in `contracts.py`.
- Repository adapters in `repository.py`, `local_store.py`, and
  `postgres_store.py`.
- Service boundary in `service.py`.
- MCP server in `mcp_server.py`.
- CLI in `cli.py`.
- Dagster assets/jobs/schedules in `dagster_assets.py`.

Important principles:

- Harvesters collect; they do not reason.
- Resolvers normalize; they do not invent.
- Agents propose, review, queue, or synthesize; they do not silently mutate
  durable knowledge unless called through an explicit write path.
- Every final research brief must be citation-first and grounded in stored
  chunks/claims.
- Research leads are watchlist context only; they cannot satisfy citation
  requirements.

## Storage State

The hosted runtime uses a payload-oriented Postgres schema that mirrors the
local SQLite contract. This is intentional for the current build phase.

Major durable tables/contracts now include:

- `agent_runs`
- `research_leads`
- `research_briefs`
- `research_brief_queue`
- `source_followup_queue`
- `text_embeddings`
- raw records, research objects, document chunks, claims, entities, and
  artifacts.

Do not commit or paste secrets into repo files. Required hosted secrets/env vars
are documented in `docs/DAGSTER_PLUS_SETUP.md`.

## Agent Layer

The current agent layer includes:

- Full-text ops agent.
- X/Twitter topic review agent.
- X linked-article review/follow-up agents.
- Evidence scout, translational hypothesis, skeptic validation, and synthesis
  editor agents for research briefs.
- Agent run ledger for durable tracking.
- Research lead collector for non-ingestible but important evidence.

Default low-cost smoke mode:

```text
review_mode = deterministic_only
```

OpenRouter-backed modes exist but should be used intentionally:

```text
openrouter_required
openrouter_compare
```

## Research Brief Lane

The brief lane is now a durable system rather than only one-off output.

Implemented:

- `ResearchBriefResult`
- `ResearchBriefRecord`
- `research_briefs` table
- `run_research_brief`
- `get_research_brief`
- `list_research_briefs`
- MCP tools/resources for persisted briefs
- CLI `research-briefs`
- Dagster `research_brief_agent_job`
- Dagster `research_brief_library_job`

The queue layer is also implemented:

- `ResearchBriefQueueItem`
- `ResearchBriefQueueRequest`
- `ResearchBriefQueueRunRequest`
- `ResearchBriefQueueRunResult`
- `research_brief_queue` table
- Service methods to queue, fetch, list, and run queued items
- MCP tools/resources for the queue
- CLI queue commands
- Dagster `research_brief_queue_job`
- Dagster `research_brief_queue_seed_job`
- Dagster `research_brief_queue_runner_job`

The synthesis evaluation layer is implemented:

- `ResearchBriefEvaluationRequest`
- `ResearchBriefEvaluationResult`
- `ResearchBriefEvaluationRecord`
- `research_brief_evaluations` table
- Service methods to evaluate, fetch, and list brief evaluations
- MCP tools/resources for persisted evaluations
- CLI `evaluate-research-brief`
- CLI `research-brief-evaluations`
- Dagster `research_brief_evaluation_job`
- Dagster `research_brief_evaluation_library_job`

Manual queue flow:

1. Use `research_brief_queue_seed_job` to queue a topic.
2. Use `research_brief_queue_runner_job` to run the next queued topic.
3. Use `research_brief_library_job` to confirm the persisted brief.
4. Use `research_brief_evaluation_job` to score synthesis readiness before
   promoting it into hypothesis review.

## Recent Commits

Recent relevant commits:

- `6b07512` - Add research lead watchlist lane
- `a0cc467` - Expose research lead jobs in smoke launcher
- `ed17d4e` - Persist research brief outputs
- `5a47c1e` - Add research brief queue runner
- `791410b` - Add Dagster research brief queue seed job

## Hosted Smoke Runs

Known successful Dagster+ runs:

- Research leads job:
  `https://twogbio.dagster.cloud/prod/runs/9c4ecd31-593e-42ce-8c11-e18b742c8c4b`
- Research brief library job:
  `https://twogbio.dagster.cloud/prod/runs/fa5faf11-d335-433e-bd14-12ae1c1ddea6`
- Direct deterministic brief smoke:
  `https://twogbio.dagster.cloud/prod/runs/a0abe371-8016-4f3e-b763-3299cc2dfb9c`
- Brief library readback:
  `https://twogbio.dagster.cloud/prod/runs/d3ad5f8f-2563-4b1e-8860-06db8a3c54aa`
- Queue seed:
  `https://twogbio.dagster.cloud/prod/runs/94d3b8d8-4c40-4257-8fa7-6de2ec5c2f68`
- Queue runner:
  `https://twogbio.dagster.cloud/prod/runs/0e106603-2c71-41d5-ad87-fbb015ac5ccf`
- Final library readback for `queue smoke`:
  `https://twogbio.dagster.cloud/prod/runs/c1262ccd-d749-4d6b-9e74-9c46d0436dad`

## Current Hosted Jobs To Know

Useful manual GitHub smoke jobs:

- `research_brief_agent_job`
- `research_brief_library_job`
- `research_brief_queue_job`
- `research_brief_queue_seed_job`
- `research_brief_queue_runner_job`
- `research_leads_job`
- `x_topic_monitor_review_job`
- `x_linked_article_followup_job`
- `x_linked_article_review_job`
- `source_followup_queue_job`
- `source_followup_ingest_job`
- `embedding_index_job`
- `embedding_maintenance_job`
- `source_health_report_job`

## Validation Commands

Use this validation set before commits:

```bash
git diff --check
.venv/bin/python -m compileall src tests .github/scripts
.venv/bin/pytest tests/test_ingestion_bridge_contracts.py -q
DAGSTER_HOME=/tmp/dagster-home DAGSTER_DISABLE_TELEMETRY=1 .venv/bin/dg check defs
```

Expected current contract test count after the queue work:

```text
215 passed
```

## Current Working Tree Note

There may be an untracked `scribbles/` directory. Treat it as user-owned and do
not modify it unless explicitly instructed.

## Recommended Next Work

Good next lanes:

1. Queue batches from source health gaps or watchlist leads. Basic queue
   controls now cover failed-item requeue and completed-item archive.
2. Add a model comparison run path for one queued brief using OpenRouter with
   controlled cost metadata.
3. Add a small dashboard/control-panel view for brief queue, lead queue, and
   source health.
4. Continue full-text parser hardening and source/date partition coverage.

Use deterministic mode for smoke tests and only enable OpenRouter-backed runs
when the user explicitly approves model spend.
