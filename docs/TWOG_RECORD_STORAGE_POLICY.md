# TWOG Record Storage Policy

This project should persist scientific state that we may need to audit, replay, compare, or build on later. Short-lived working context can stay ephemeral.

## Durable Records

Persist records when they answer any of these questions:

- Why did the system make or reject a scientific bet?
- What evidence was attached to a claim, program, therapy idea, or validation packet?
- Which agent, model, prompt, or operator produced a result?
- What source query, lead, queue item, artifact, or run should be replayable later?
- What decision changed the downstream workflow?

Durable record classes include:

- Research objects, raw records, document chunks, claims, entity mentions, and embeddings.
- Research programs, decisive questions, evidence tasks, loop counts, and gate decisions.
- Source queries, research leads, research brief queue items, briefs, evaluations, and follow-up tasks.
- Therapy ideas, committee outputs, promotion candidates, validation plans, validation queue items, and validation decisions.
- Agent runs, agent reviews, model metadata, prompt/version metadata, cost/usage metadata, and output payloads.
- Artifacts such as processed omics matrices, computed readouts, reports, markdown handoffs, CSVs, and JSON outputs.

## Ephemeral Context

Do not persist by default when the information is only useful inside the current operation:

- Prompt scratch notes or hidden reasoning traces.
- Temporary UI state, filters, sort order, and page selections.
- Intermediate parsing attempts that produce no scientific or operational signal.
- Duplicate snippets that are already represented by a persisted citation, chunk, artifact, or source query.
- One-off command context that does not affect downstream scientific state.

## Boundary Rule

If a human or agent might later ask, "Why did we do this, and what evidence supported it?", persist it. If the answer is only, "This helped one call finish," keep it ephemeral.

## Current Storage Backends

Local development defaults to SQLite at `var/hsa_research/ingestion_bridge.sqlite3`. Hosted execution should use Postgres through `HSA_STORAGE_BACKEND=postgres` and `HSA_DATABASE_URL`.

Memory storage is appropriate for tests, demos, isolated simulations, and temporary agent scratch. It is not appropriate for evidence, decisions, or work queues.
