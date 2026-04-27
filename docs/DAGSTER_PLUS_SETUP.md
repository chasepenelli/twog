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
```

Do not commit or paste `HSA_DATABASE_URL` into chat. Store it directly in
Dagster+.

The preferred automated path is:

1. Store the managed Postgres URL as the GitHub Actions secret
   `HSA_DATABASE_URL`.
2. Store a Dagster+ token from a user with Editor, Admin, or Organization Admin
   permissions as the GitHub Actions secret `DAGSTER_PLUS_ENV_API_TOKEN`.
3. Run the `Configure Dagster Plus Environment` GitHub Actions workflow.

That workflow uses `DAGSTER_PLUS_ENV_API_TOKEN` to write the production Dagster+
environment variables with:

```bash
uv run dg plus create env HSA_STORAGE_BACKEND postgres --global --scope full --yes
uv run dg plus create env HSA_DATABASE_URL --from-local-env --global --scope full --yes
uv run dg plus create env HSA_CONTACT_EMAIL poppa@bradyandgraffiti.com --global --scope full --yes
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
OPENAI_API_KEY=<later hosted LLM agents>
ANTHROPIC_API_KEY=<later hosted LLM agents>
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

## Notes

- Dagster+ Serverless is the preferred first deployment mode.
- Heavy GPU tasks should stay outside Dagster+ workers and run through RunPod.
  Dagster should submit those jobs and track handles, not execute GPU work
  directly.
- SQLite remains the local development backend.
