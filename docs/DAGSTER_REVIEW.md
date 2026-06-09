# Dagster Ingest Review — Keep, Refactor, or Rebuild?

> Review of how the Dagster ingestion works end-to-end and whether to keep/refactor the
> current setup or rebuild / stand up a fresh Dagster instance. Written 2026-06-09 from a
> three-part deep read (asset graph · harvesters/sources · deployment/ops).

## Verdict (short)

**Keep both the code and the Dagster+ instance. A rebuild is not warranted.** The work is
targeted cleanup + opportunistic refactor — not a do-over. The only thing that would justify a
real re-architecture is a decision to make the platform **disease-agnostic** (see §6).

## 1. How the ingest actually works

```
Dagster schedule (daily/weekly)
  → asset/job → service call → harvester.fetch() over an external API
  → normalize() to typed records (RawSourceRecord + ResearchObject)
  → upsert by dedupe_key (idempotent) → document chunks
  → entity resolution → claim extraction (rule-based drafts) → claim curation
  → coverage snapshot
Storage: Neon Postgres (hosted) via repository abstraction; SQLite for local dev.
```

- **27 source integrations, all implemented, no stubs**: PubMed, Europe PMC, Crossref, OpenAlex,
  PMC OA, Unpaywall, ClinicalTrials.gov, AVMA VCTR, ChEMBL, PubChem, UniProt, RCSB PDB, OpenFDA,
  GEO, SRA, ICDC, HGNC/VGNC, NCBI Gene, Ensembl, MONDO/DOID, Reactome, WikiPathways, UniChem, OMA.
- **Solid robustness**: 3-retry exponential backoff on 429/5xx, partial-fetch tolerance, full-text
  time budgets, idempotent re-runs via `dedupe_key`.
- **Approval gating**: API sources ingest directly (trusted); **scrape sources** go
  fetch → parse → human review → ingest, with an explicit `approved_by` gate (tested).
- **Claim extraction is intentionally rule-based** (confidence 0.30–0.54) — drafts to exercise
  the storage/search loop before frontier models. Not the real scientific claim engine yet.

## 2. The asset graph

- **127 assets, 97 jobs, 16 schedules, 20 asset checks.** Modern Dagster (`1.13.2`, `@asset`,
  `Definitions`, `ConfigurableResource`, multi-partitions, asset checks). **No legacy idioms.**
- **Only ~10 assets form a real DAG** (`ingestion_bridge_v2`: source_registry → queries → raw
  records → objects → chunks → entities → claims → curated → coverage). **The other ~117 are
  thin imperative wrappers** that each call a `HSAResearchService` method and return a result —
  they don't depend on each other.
- **Implication (the honest critique):** Dagster here is a **scheduler + run-history + UI**
  layer, not a lineage graph. That's *fine* for this workload — but name it. The value you get is
  "things run on a cadence, unattended, with a dashboard," not asset lineage. If you ever want
  real lineage (candidate/claim graph as assets), that's a modeling project — not a reason to
  rebuild the framework.

## 3. The Dagster+ deployment

- **One clean, coherent instance**: org `twogbio`, deployment `prod`, single code location
  `twog`, Serverless + PEX deploys, ephemeral branch deployments on PR. Env/secrets centrally
  managed via `configure-dagster-env.yml`. **No ghost instances, no orphaned locations.**
- Connects to Neon via `HSA_DATABASE_URL` + `ResearchRepositoryResource`.
- A fresh instance would take ~30 min to recreate and deliver **zero new capability.**

## 4. The real problems (none are "rebuild" problems)

| Problem | Severity | Fix |
|---|---|---|
| ~~RunPod endpoint ID half-migrated~~ | **RESOLVED 2026-06-09** | RunPod execution layer removed entirely (it never worked). `compute_runners.py` is now a provider seam (`ComputeRunner` protocol + registry); the dead `cbf4ffekmo36t9` endpoint is gone. Next compute tool registers via `register_compute_runner()` — ROADMAP P3. |
| **`launch-dagster-smoke.yml` is 122KB / 2,535 lines** of hand-maintained per-job boilerplate (84 jobs × dropdown+validation+report) | Maintainability | Generate from a job manifest; target ~500 lines. |
| **`dagster_assets.py` 9,216 lines** — 97 boilerplate jobs, ~50 metadata extractors, formulaic asset wrappers | Maintainability | Asset/job factories → ~2,500 lines, zero behavior change. Ties into the `service.py` decoupling. |
| **`harvesters_v2.py` 4,275 lines** — 20–30% copy-paste across `normalize()` methods, no base class / plugin system | Maintainability | Extract `APIHarvesterV2` base + split into modules by source family. |
| **Schedule enable-state unknown** | Verify | Confirm in the Dagster+ UI which of the 16 schedules are actually enabled. |

## 5. Why NOT rebuild

1. Modern idioms — nothing legacy to escape.
2. 27 working source integrations + retry/dedup/approval = months of real work; a rewrite is
   estimated **6–8 weeks for feature parity** with pure regression risk.
3. The instance is clean and singular; a fresh one is downtime for no gain.
4. The defects are **sprawl and a half-finished migration** — cosmetic/maintainability, not
   architectural rot.

## 6. The ONE thing that would change this answer: domain entanglement

The framework is **extremely HSA-baked** — disease terms, priority targets, and the
compound/target vocabularies are hardcoded across `query_policy.py`, `harvesters_v2.py`, and
`claim_extractor.py` (reusability for another disease scored ~2/10 by the review).

- If **twog stays canine-HSA forever** → this is a non-issue; keep as-is.
- If you ever want it **disease-agnostic** (ingest other cancers / a platform others reuse) →
  that's the real re-architecture: lift the domain terms into a config/profile interface. Even
  then it's a *refactor of 3 files behind a config seam*, not a framework rebuild.

**This is the only strategic fork worth a decision.** Everything else is cleanup.

## 7. Recommended sequence

1. **Endpoint hygiene** (cheap, do soon): make RunPod endpoint env/config-driven, kill the stale
   hardcoded default, confirm the live ID. Removes a whole class of footgun.
2. **Verify schedules enabled** in the Dagster+ UI; record the enabled set.
3. **Opportunistic refactor** (rides with `service.py` decoupling): asset/job factories to shrink
   `dagster_assets.py`; `APIHarvesterV2` base to de-dupe harvesters; data-drive the smoke
   workflow. Each behind the green test suite.
4. **Defer** the disease-agnostic config seam until/unless §6 is a real goal.
