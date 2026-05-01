# Research Follow-Up Resolver SOP

This SOP is the operating reference for the person or agent responsible for
moving evidence-light research briefs back into the durable evidence pipeline.
It is a repo-native skill reference, not an installed Codex skill package.

## Purpose

The research brief quality lane can label a brief as
`needs_followup_research` when the synthesis is directionally useful but too
light on durable evidence. This SOP defines how to queue those limitations,
resolve them against stored scholarly evidence, and decide whether the lead is
safe to promote back into synthesis.

The resolver does not replace scientific review. Its job is to make sure a
lead has durable source support before it is used again by the brief agents.

## Required Operator Skills

- Citation-first scientific review.
- Identifier triage for DOI, PMID, PMCID, and NCT IDs.
- Familiarity with durable source boundaries:
  - PubMed.
  - Crossref.
  - OpenAlex.
  - Europe PMC.
  - PMC OA.
  - ClinicalTrials.gov.
- Comparative oncology vocabulary, especially canine hemangiosarcoma, human
  angiosarcoma, vascular sarcoma, translational oncology, biomarkers,
  targeted therapy, immunotherapy, and metastasis biology.
- Ability to read Dagster materialization metadata, GitHub Actions summaries,
  and agent-run ledger outputs.
- Discipline to keep social, conference, institutional, or narrative sources
  as watchlist context unless durable scholarly chunks are attached.

## Guardrails

1. Do not invent citations, identifiers, claims, or source links.
2. Treat `research_leads` as watchlist context until durable chunks or source
   follow-ups support them.
3. Promote a lead only when it has stored evidence refs from durable sources.
4. If durable evidence is absent, keep the lead in `followup` or mark it for
   manual review.
5. Do not mutate Dagster schedules from this lane.
6. Do not use X/Twitter, institutional pages, or conference pages as final
   evidence for a scientific brief. They can only seed follow-up searches.
7. Prefer narrow resolver runs over broad runs when debugging.

## Standard Workflow

### 1. Run Quality First

Run `research_brief_quality_job` and inspect:

- `failed_count`.
- `followup_count`.
- `quality_status_counts`.
- hard errors versus evidence limitations.

Stop if `brief_failed` or hard errors are present. Fix hard failures before
queuing follow-up work.

### 2. Dry-Run The Follow-Up Queue

Run `research_brief_followup_queue_job` with `dry_run=true`.

Inspect:

- `candidate_brief_count`.
- `limitation_count`.
- `existing_count`.
- `skipped_count`.
- `errors`.

Proceed only if the dry-run output is explainable. A high `existing_count`
usually means the lane is idempotent and already has durable follow-up leads
for those limitations.

### 3. Queue Follow-Up Leads

Run `research_brief_followup_queue_job` with `dry_run=false`.

Expected result:

- Evidence limitations become durable `ResearchLeadRecord` rows with
  `status="followup"`.
- Re-running the same queue should not duplicate existing follow-up leads.

### 4. Resolve The Follow-Up Leads

Run `research_followup_resolver_job` scoped to the queue source:

- `statuses=["followup"]`.
- `source_keys=["research_brief_quality"]`.
- `search_source_keys` should include the durable scholarly sources needed for
  fallback search.
- Use `dry_run=true` first if the source scope, lead count, or search terms are
  unfamiliar.

The resolver can:

- Attach queued identifier follow-ups.
- Ingest source follow-ups when identifiers are present.
- Search durable sources when identifiers are missing.
- Promote leads back to `watching` only when evidence requirements are met.
- Keep unresolved leads in `followup`.
- Mark leads for manual research.

### 5. Interpret Resolver Output

Key counters:

- `leads_seen`: total inspected leads.
- `promoted_leads`: leads now safe to feed back into synthesis.
- `kept_in_followup`: leads that still lack enough durable evidence.
- `manual_research_required`: leads needing a human/source specialist.
- `failed_leads`: resolver failures that need debugging.
- `source_followups_queued`: identifier follow-ups created.
- `source_followups_ingested`: identifier follow-ups ingested.
- `durable_source_searches`: fallback searches run against durable sources.

For each promoted lead, verify that the lead result includes at least one
`evidence_refs` item or `durable_source_keys` item.

### 6. Feed Resolved Leads Back Into Synthesis

After promotion, run `research_brief_queue_batch_job` conservatively. Prefer
small batches until the quality report confirms the resolver improved evidence
coverage. The current batch queue can filter research leads by status and lead
type, but it does not yet filter by `origin_source_key`, so keep limits low and
inspect queued item metadata before launching synthesis.

## Hosted Config Examples

### Quality Report

```json
{
  "ops": {
    "research_brief_quality_report": {
      "config": {
        "limit": 25,
        "include_evaluations": true
      }
    }
  }
}
```

### Follow-Up Queue Dry Run

```json
{
  "ops": {
    "research_brief_followup_queue_report": {
      "config": {
        "limit": 25,
        "include_evaluations": true,
        "max_limitations_per_brief": 20,
        "dry_run": true
      }
    }
  }
}
```

### Follow-Up Queue Write

```json
{
  "ops": {
    "research_brief_followup_queue_report": {
      "config": {
        "limit": 25,
        "include_evaluations": true,
        "max_limitations_per_brief": 20,
        "dry_run": false
      }
    }
  }
}
```

### Resolver Run

```json
{
  "ops": {
    "research_followup_resolver_report": {
      "config": {
        "lead_ids": [],
        "statuses": ["followup"],
        "source_keys": ["research_brief_quality"],
        "search_source_keys": [
          "pubmed",
          "crossref",
          "openalex",
          "europe_pmc",
          "pmc_oa",
          "clinicaltrials_gov"
        ],
        "limit": 25,
        "ingest_source_followups": true,
        "search_missing_identifiers": true,
        "promote_ready_leads": true,
        "run_claim_extraction": true,
        "dry_run": false,
        "min_evidence_chunks": 1,
        "search_limit_per_source": 2,
        "max_search_terms": 12,
        "approved_by": "github-actions"
      }
    }
  }
}
```

### Conservative Synthesis Queue

```json
{
  "ops": {
    "research_brief_queue_batch_report": {
      "config": {
        "mode": "research_leads",
        "lead_statuses": ["watching"],
        "lead_types": [],
        "source_keys": [],
        "source_health_statuses": [],
        "include_empty_sources": false,
        "limit": 5,
        "disease_scope": "canine_hsa_human_angiosarcoma",
        "priority": 3,
        "max_chunks_per_perspective": 6,
        "max_claims": 12,
        "max_chunk_chars": 1200,
        "brief_style": "technical",
        "model_profile": "research_brief",
        "review_mode": "deterministic_only",
        "review_models": []
      }
    }
  }
}
```

## Stop Conditions

Stop and inspect before proceeding if any of these are true:

- Quality report has `failed_count > 0`.
- Quality status includes `brief_failed`.
- Resolver has `failed_leads > 0`.
- Resolver has non-empty `errors`.
- Lead result has `promoted=true` but no durable evidence refs.
- Source follow-up ingest reports failed queue items.
- The same unresolved limitation returns repeatedly without new identifiers or
  durable source hits.

## Output Requirements

Every operator or agent run should record:

- GitHub Actions run ID or Dagster run ID.
- Job name.
- Config used.
- `candidate_brief_count`, `limitation_count`, and `existing_count`.
- `leads_seen`, `promoted_leads`, `kept_in_followup`,
  `manual_research_required`, and `failed_leads`.
- Any lead IDs requiring manual review.
- Any source follow-up IDs created or ingested.
- The next recommended job, if any.

## Agent Prompt Card

Use this prompt when assigning the lane to a human-assisted agent:

```text
You are the TWOG research follow-up resolver operator. Your task is to move
evidence-light research brief limitations into durable evidence, not to invent
or overstate findings. Use only stored durable sources and resolver outputs as
scientific evidence. Treat X/Twitter, institutional pages, and conference pages
as watchlist context only unless they lead to DOI, PMID, PMCID, NCT, Europe PMC,
PMC OA, PubMed, Crossref, OpenAlex, or ClinicalTrials.gov evidence. Run the
quality job first, dry-run the follow-up queue, queue only explainable
limitations, resolve scoped follow-up leads, and promote only when durable
evidence refs are attached. Report run IDs, counts, unresolved lead IDs, and
the next recommended action.
```

## Last Known Good Hosted Pattern

The lane was validated on hosted Dagster+ with this pattern:

1. `research_brief_quality_job`.
2. `research_brief_followup_queue_job` dry run.
3. `research_brief_followup_queue_job` write run.
4. `research_followup_resolver_job` scoped to
   `source_keys=["research_brief_quality"]`.

The resolver successfully promoted all queued follow-up leads in that run
because stored durable chunks already satisfied the evidence requirements.
