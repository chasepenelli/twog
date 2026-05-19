# TWOG Public Site

This is the public-facing Next.js app for TWOG. It presents the mission, public candidate records, method pages, and the first public contribution-intake surface.

## What This App Does

- Renders the mission-first homepage at `/`.
- Publishes static candidate pages at `/candidates`.
- Exposes machine-readable candidate payloads under `/api/public-candidates`.
- Documents candidate record methodology under `/methods/candidate-record-v1`.
- Accepts structured public contribution packets into Neon/Postgres when storage is configured.

## Core Routes

```text
/                                           Homepage
/candidates                                 Candidate index
/candidates/twog-15f50d                     Example public candidate
/methods/candidate-record-v1                Candidate record method
/api/public-candidates                      Public candidate payload index
/api/public-candidates/{candidate_id}        One public candidate payload
/api/public-candidates/{candidate_id}/contribution-template
/api/public-candidates/{candidate_id}/contributions
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
TWOG_PUBLIC_CANDIDATES_SOURCE=http://127.0.0.1:8792 npm run sync:candidates
```

## Neon Contribution Intake

The public checkout routes are static/read-only. The check-in route writes to Neon/Postgres:

```text
POST /api/public-candidates/{candidate_id}/contributions
```

Set one database URL:

```bash
NEON_DATABASE_URL=postgresql://...
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
npx eslint 'app/api/public-candidates/**/*.ts' 'app/methods/[methodId]/page.tsx' 'app/candidates/[candidateId]/page.tsx' lib/public-candidates.ts lib/candidate-contributions.ts
npx next build --webpack
```

Full `npm run lint` currently surfaces unrelated older lint debt outside the public candidate slice.

## Safety Boundary

TWOG public records are research artifacts. They are not medical advice, veterinary advice, treatment instructions, or efficacy claims. The site exposes evidence, uncertainty, methods, and decision history so the work can be inspected and challenged.
