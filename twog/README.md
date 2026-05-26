# TWOG Public Site

This is the public-facing Next.js app for TWOG. It presents the mission, public candidate records, method pages, and the first public proof/check-in surface.

## What This App Does

- Renders the mission-first homepage at `/`.
- Publishes static candidate pages at `/candidates`.
- Exposes machine-readable candidate payloads under `/api/public-candidates`.
- Exposes public-safe evidence bundles for candidate checkout.
- Documents candidate record methodology under `/methods/candidate-record-v1`.
- Accepts structured public contribution packets into Neon/Postgres when storage is configured.
- Returns contribution receipts and public status URLs.
- Keeps public submissions behind an operator-reviewed intake queue instead of mutating candidate pages directly.

## Core Routes

```text
/                                           Homepage
/candidates                                 Candidate index
/candidates/twog-15f50d                     Example public candidate
/methods/candidate-record-v1                Candidate record method
/api/public-candidates                      Public candidate payload index
/api/public-candidates/{candidate_id}        One public candidate payload
/api/public-candidates/{candidate_id}/evidence-bundle
/api/public-candidates/{candidate_id}/contribution-template
/api/public-candidates/{candidate_id}/contributions
/api/contributions/{contribution_id}/status
```

## Data Strategy

Candidate pages currently render from static exported JSON in:

```text
data/public-candidates.json
```

Refresh that export from the local command center:

```bash
npm run sync:candidates
```

By default, the sync script reads from `http://127.0.0.1:8792`. Override with:

```bash
TWOG_COMMAND_CENTER_URL=http://127.0.0.1:8792 npm run sync:candidates
```

Override the output path with `TWOG_PUBLIC_CANDIDATES_OUT`.

## Neon Contribution Intake

The public checkout routes are static/read-only. The check-in route writes to Neon/Postgres:

```text
POST /api/public-candidates/{candidate_id}/contributions
```

Set one database URL:

```text
NEON_DATABASE_URL=<postgres connection string>
```

Supported aliases:

- `DATABASE_URL`
- `POSTGRES_URL`
- `HSA_DATABASE_URL`

Apply the schema:

```bash
npm run db:migrate
```

The API also lazily ensures the table exists on first write, but explicit migration is preferred for production and handoff.

## Optional Runtime Integrations

The public candidate pages build without live database or model credentials. Optional interactive routes use these environment variables when enabled:

- `NEON_DATABASE_URL`, `DATABASE_URL`, `POSTGRES_URL`, or `HSA_DATABASE_URL` for public contribution intake.
- `NEXT_PUBLIC_SUPABASE_URL` and `NEXT_PUBLIC_SUPABASE_ANON_KEY` for browser-side Supabase reads.
- `SUPABASE_URL` plus `SUPABASE_SERVICE_ROLE_KEY` or `SUPABASE_ANON_KEY` for server-side design-lab APIs.
- `OPENROUTER_API_KEY` for design-lab RAG chat/search.

## Check-Out / Check-In Flow

The intended public loop is:

1. Open a candidate page.
2. Open the JSON payload and evidence bundle for the exact public snapshot.
3. Do outside work against that snapshot: evidence addition, citation repair, claim critique, replication, omics note, artifact generation, validation proposal, safety/translation note, or demotion case.
4. Submit a contribution packet through the page form or `POST /api/public-candidates/{candidate_id}/contributions`.
5. Receive a contribution receipt with a status URL.
6. TWOG reviews the packet in the Command Center and explicitly routes it to evidence review, validation planning, compute review, request-more-information, rejection, or archive.

The public app writes intake records only. Operator triage lives in the Python/Dagster command layer.

## Local Development

```bash
npm install
npm run build
npm start -- --port 3000
```

Use production preview for this app when testing public candidate pages and API routes.

## Validation

Useful checks for this slice:

```bash
npx eslint 'app/api/public-candidates/**/*.ts' 'app/methods/[methodId]/page.tsx' 'app/candidates/[candidateId]/page.tsx' lib/*.ts
npm run build
```

Full `npm run lint` currently surfaces unrelated older lint debt outside the public candidate slice.

## Safety Boundary

TWOG public records are research artifacts. They are not medical advice, veterinary advice, treatment instructions, or efficacy claims. The site exposes evidence, uncertainty, methods, and decision history so the work can be inspected and challenged.
