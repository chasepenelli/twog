# Dagster+ Setup

This is the hosted orchestration path for the Ingestion Bridge v2 structured
source pipeline.

## Repository

Dagster+ is connected to:

```text
chasepenelli/hsa-dagster
```

Use `main` for production deploys. The generated GitHub Actions workflow already
contains the Dagster+ organization and deployment target, and the repository
already has `DAGSTER_CLOUD_API_TOKEN` stored as a GitHub Actions secret for code
deployments.

## Code Location

This project uses the current Dagster `dg` project layout:

```text
src/hsa_dagster/definitions.py
```

That entrypoint loads the HSA ingestion bridge definitions from:

```text
src/hsa_research/ingestion_bridge/dagster_assets.py
```

The loaded Dagster definitions expose:
- `structured_source_pipeline_report`
- `structured_source_pipeline_job`
- `structured_source_pipeline_has_minimum_outputs`

## Required Dagster+ Environment Variables

Set these in Dagster+ under Deployment -> Environment variables:

```text
HSA_STORAGE_BACKEND=postgres
HSA_DATABASE_URL=<Neon or hosted Postgres connection string>
HSA_CONTACT_EMAIL=poppa@bradyandgraffiti.com
HSA_FULL_TEXT_REQUEST_TIMEOUT_SECONDS=8
HSA_FULL_TEXT_REQUEST_ATTEMPTS=1
HSA_FULL_TEXT_FETCH_TIME_BUDGET_SECONDS=120
HSA_PMC_OA_MAX_CANDIDATE_RECORDS=20
OPENROUTER_API_KEY=<required for hosted OpenRouter review modes>
```

Do not commit or paste `HSA_DATABASE_URL` into chat. Store it directly in
Dagster+.

The preferred automated path is:

1. Store the managed Postgres URL as the GitHub Actions secret
   `HSA_DATABASE_URL`.
2. Store a Dagster+ token from a user with Editor, Admin, or Organization Admin
   permissions as the GitHub Actions secret `DAGSTER_PLUS_ENV_API_TOKEN`.
3. Store the OpenRouter key as the GitHub Actions secret `OPENROUTER_API_KEY`
   if hosted model-review comparison should run.
4. Run the `Configure Dagster Plus Environment` GitHub Actions workflow.

That workflow uses `DAGSTER_PLUS_ENV_API_TOKEN` to write the production Dagster+
environment variables with:

```bash
uv run dg plus create env HSA_STORAGE_BACKEND postgres --global --scope full --yes
uv run dg plus create env HSA_DATABASE_URL --from-local-env --global --scope full --yes
uv run dg plus create env HSA_CONTACT_EMAIL poppa@bradyandgraffiti.com --global --scope full --yes
uv run dg plus create env HSA_FULL_TEXT_REQUEST_TIMEOUT_SECONDS 8 --global --scope full --yes
uv run dg plus create env HSA_FULL_TEXT_REQUEST_ATTEMPTS 1 --global --scope full --yes
uv run dg plus create env HSA_FULL_TEXT_FETCH_TIME_BUDGET_SECONDS 120 --global --scope full --yes
uv run dg plus create env HSA_PMC_OA_MAX_CANDIDATE_RECORDS 20 --global --scope full --yes
uv run dg plus create env OPENROUTER_API_KEY --from-local-env --global --scope full --yes
```

## GitHub Actions Settings

No manual GitHub Actions setup should be required for the initial Dagster+
deployment repo. Dagster+ created `.github/workflows/dagster-plus-deploy.yml`
and stored the deploy token as `DAGSTER_CLOUD_API_TOKEN`.

Environment-variable writes require a separate Dagster+ token with user-level
deployment permissions. The deploy token created for GitHub Actions can deploy
code but may not have permission to create or update Dagster+ environment
variables.

## Optional Environment Variables

These improve rate limits or unlock later compute paths:

```text
NCBI_API_KEY=<free NCBI key>
OPENFDA_API_KEY=<free openFDA key>
RUNPOD_API_KEY=<later GPU jobs>
OPENROUTER_API_KEY=<optional hosted model-review comparison>
```

## Hosted Database

Recommended first hosted database:

```text
Neon Postgres
```

The current hosted runtime adapter creates a payload-oriented Postgres schema
that mirrors the proven local SQLite contract. This is separate from the richer
normalized Postgres migration in `db/migrations/005_ingestion_bridge_v2.sql`.
Do not run that migration for the first Dagster+ runtime unless the adapter has
been upgraded to the normalized schema.

## Local Validation

With local SQLite:

```bash
.venv/bin/python -m hsa_research.ingestion_bridge.cli structured-pipeline --source pubchem --limit 1
```

With hosted Postgres:

```bash
HSA_STORAGE_BACKEND=postgres \
HSA_DATABASE_URL="postgres://..." \
.venv/bin/python -m hsa_research.ingestion_bridge.cli structured-pipeline --source pubchem --limit 1
```

## Dagster+ Validation

After the code location loads, run:

```text
structured_source_pipeline_job
```

Check:
- The job run succeeds.
- `structured_source_pipeline_has_minimum_outputs` passes.
- `structured_source_pipeline_report` shows source QA counts.
- The Neon database contains persisted rows after the run.

Before the Dagster+ environment variables are wired, run the manual GitHub
Actions workflow `Validate Hosted Postgres`. It uses the `HSA_DATABASE_URL`
GitHub secret directly and runs a small structured-source pipeline against Neon.

## Hosted Smoke Launch Workflow

Use the manual GitHub Actions workflow `Launch Dagster Smoke Job` to launch a
hosted Dagster+ smoke job from Actions.

The local `dagster-cloud` CLI currently exposes `dagster-cloud job launch
--wait`, but the installed 1.13.2 implementation has no wait timeout. It polls
until Dagster+ returns `SUCCESS`, `FAILURE`, or `CANCELED`, so a hosted run that
stays queued or a status endpoint that keeps erroring can leave
`workflow_dispatch` runs waiting until the outer GitHub Actions timeout.

The smoke workflow avoids the unbounded waiter:

1. Launch the job without `--wait`.
2. Capture and print the Dagster run id and Dagster+ run URL.
3. Poll `dagster-cloud run status` with a per-query timeout.
4. Stop after `DAGSTER_CLOUD_RUN_TIMEOUT_SECONDS`, request `SAFE_TERMINATE` for
   the accepted Dagster run, and fail with the last known status.

Default timeout controls live in `.github/workflows/launch-dagster-smoke.yml`:

```text
DAGSTER_CLOUD_RUN_TIMEOUT_SECONDS=2700
DAGSTER_CLOUD_RUN_STATUS_INTERVAL_SECONDS=15
DAGSTER_CLOUD_RUN_STATUS_QUERY_TIMEOUT_SECONDS=60
DAGSTER_CLOUD_TERMINATE_POLICY_ON_TIMEOUT=SAFE_TERMINATE
```

The workflow also exposes a `timeout_seconds` dispatch input so long-running
jobs can be tested without changing the file. For the full-text lane, prefer a
shorter manual timeout while hardening the hosted path.

Partitioned full-text source/date assets should still be launched from Dagster+
directly or by the stopped `literature_full_text_source_date_daily_schedule`.
The current GitHub smoke workflow uses `dagster-cloud job launch`, which does
not expose a partition-key option in the installed CLI.

For agent-reviewed manual validation from GitHub Actions, launch
`full_text_source_date_ops_job` with `config_json`. This is a config-driven
manual job, not the partitioned asset run. It runs one source/date full-text
slice, persists the ingested data, then passes that report into
`FullTextOpsAgent` so the agent can recommend whether the lane is ready.

Example `config_json`:

```json
{
  "ops": {
    "full_text_source_date_ops": {
      "config": {
        "source_key": "europe_pmc",
        "partition_date": "2026-04-27",
        "source_limit": 25,
        "extract_limit": 100,
        "curate_limit": 100,
        "review_mode": "external_required"
      }
    }
  }
}
```

External-review full-text ops does not require an OpenAI API key. The hosted job
persists a typed review packet in `agent_runs.output_payload.evidence.review_packet`.
Review that packet from a ChatGPT Pro/Codex session, compare it to
`deterministic_guardrail_result`, and only then decide whether to enable the
stopped schedule.

To compare hosted model reviewers through OpenRouter, set the GitHub Actions
secret `OPENROUTER_API_KEY` and use `review_mode=openrouter_compare`:

```json
{
  "ops": {
    "full_text_source_date_ops": {
      "config": {
        "source_key": "pmc_oa",
        "partition_date": "2026-04-27",
        "source_limit": 25,
        "extract_limit": 100,
        "curate_limit": 100,
        "review_mode": "openrouter_compare",
        "review_models": [
          "openai/gpt-5.1",
          "anthropic/claude-sonnet-4.5",
          "anthropic/claude-opus-4.5"
        ]
      }
    }
  }
}
```

Each model's structured result is stored in
`agent_runs.output_payload.evidence.model_reviews` alongside the deterministic
guardrail result.

If a run is already stuck in Dagster+, use the manual GitHub Actions workflow
`Terminate Dagster Runs`. It calls Dagster Cloud GraphQL `terminateRuns` with the
GitHub secret `DAGSTER_PLUS_ENV_API_TOKEN`. Start with `SAFE_TERMINATE`. Use
`MARK_AS_CANCELED_IMMEDIATELY` only when the UI still shows a stale run after a
safe termination request and the run must be cleared from orchestration state.

## Notes

- Dagster+ Serverless is the preferred first deployment mode.
- Heavy GPU tasks should stay outside Dagster+ workers and run through RunPod.
  Dagster should submit those jobs and track handles, not execute GPU work
  directly.
- SQLite remains the local development backend.
