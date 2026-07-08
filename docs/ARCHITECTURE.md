# twog — Architecture & Onboarding

> **twog is an autonomous comparative-oncology FALSIFICATION engine** (canine hemangiosarcoma × human angiosarcoma).
> Tagline: *"An engine that tries to be wrong."*
> It autonomously proposes hypotheses, runs real GPU compute (Modal) to try to **disprove** them, **refuses inputs it can't verify**, and emits signed, re-checkable **proof capsules**. **Nothing is ever auto-promoted** — a human holds the terminal write-gate.

This document is a **map + a per-area guide**. You should be able to work safely on *any one area* below without reading the whole system first. Each area section follows the same shape: **What it does · Key files · How it works / data flow · How to run or extend · Gotchas.**

Repo root: `/Users/home/twog-cleanup`. Backend: Python under `src/hsa_research/ingestion_bridge/`. Front-end: `web/` (Next.js 14 App Router + TypeScript). Data: Neon Postgres (prod) / SQLite (tests). Compute: Modal.

---

## 1. System map

The **engine** (a deterministic falsification loop) reads a candidate's signed proof-capsule ledger from **Neon**, proposes the next cheapest test that could kill the leading hypothesis, pre-registers a kill-criterion, dispatches real compute on **Modal** (docking / co-folding / MD / omics), reads the result back against the pre-registration, and writes a new **proof capsule** to Neon. A **JSON REST API** (`web_api.dispatch`) projects those records through presenters and serves them to the **Next.js website** (public STATE / EVIDENCE / RUNS pages, no auth) plus gated operator/contribute surfaces (WorkOS, planned). The whole thing is a single in-process orchestrator (`HSAResearchService`) over a `ResearchRepository` interface — there is no microservice fan-out.

```
        ┌─────────────────────────────────────────────────────────────┐
        │  ENGINE  (HSAResearchService — the orchestrator god-object)  │
        │                                                             │
        │   propose_next_falsification   (falsification_planner.py)   │
        │        │  deterministic, NOT LLM-backed                     │
        │        ▼                                                    │
        │   register_falsification_test  (pre-register kill-crux)     │
        │        │  resolve real lane inputs OR refuse (spend gate)   │
        │        ▼                                                    │
        │   run_compute_validation_flow ───────────────┐             │
        │        │                                      │ dispatch    │
        │        ▼                                      ▼             │
        │   read result vs pre-registration       MODAL (GPU/CPU)     │
        │        │   confound + provenance gates   gnina · Boltz-2    │
        │        ▼                                  OpenMM MD · omics  │
        │   write ProofCapsule  ── HUMAN write-gate (accept/promote)  │
        └─────────┬──────────────────────────────────────┬──────────┘
                  │ reads/writes                          │
                  ▼                                       │
        ┌──────────────────┐                             │
        │  NEON Postgres    │  (SQLite in tests)          │
        │  candidates       │                             │
        │  proof capsules   │                             │
        │  run manifests    │                             │
        │  collaborators    │                             │
        └─────────┬─────────┘                             │
                  │ reads                                 │
                  ▼                                       │
        ┌──────────────────────────────────────┐         │
        │  API  web_api.dispatch / run_api_server│◀────────┘
        │   /public/*  (presented, no auth)      │
        │   operator/contribute  (WorkOS-gated)  │
        └─────────┬──────────────────────────────┘
                  │ HTTPS/JSON
                  ▼
        ┌──────────────────────────────────────┐
        │  WEB  Next.js 14 App Router            │
        │   /(STATE) /evidence /runs  (public)   │
        │   /operate /contribute      (gated)    │
        │   NEXT_PUBLIC_USE_MOCKS → mocks | /public/* │
        └──────────────────────────────────────┘
```

**Status (per `threadnotes/launch-checklist.md`):** engine, Neon, Modal, the API logic, and the public website are **built and tested** (~720 backend tests). The last mile (turn the loop on with budget caps, deploy API + web, stand up WorkOS) is the remaining work. The autonomous Dagster schedule is **currently STOPPED** by design.

> **Note on the repo:** `src/hsa_research/ingestion_bridge/` is a large, mature module (the falsification engine grew inside an older "ingestion bridge" research-harvesting codebase). Many files (`harvesters*.py`, `claim_*`, `source_*`, `omics_*`, `research_*`, `x_topic_*`) belong to that older literature-ingestion pipeline and are **not** part of the falsification engine described here. The areas below are the load-bearing falsification-engine modules.

---

## 2. Domain model & contracts

**What it does.** Defines every typed record and request/result the system passes around. All models are Pydantic (`StrictBaseModel`), with `@model_validator` normalizers. This is the single source of truth the front-end `web/lib/types/domain.ts` mirrors.

**Key files / entrypoints.**
- `src/hsa_research/ingestion_bridge/contracts.py` (~7,200 lines — the whole vocabulary).

**The records that matter for the falsification engine** (line numbers approximate; the file is large):
- `PublicCandidateRecord` (~5443) — a hypothesis/candidate (`candidate_id`, `title`, `targets`, `candidate_therapies`, `evidence_refs`, `validation_ready`, `metadata`). The unit the loop operates on. Curated lane inputs live at `metadata["lane_inputs"][<lane>]`.
- `ProofCapsuleRecord` (~5229) — the sealed evidence unit. Composed of:
  - `producer` (`ProofCapsuleProducer`) — who/what produced it (Person or software).
  - `target` (`ProofCapsuleTarget`) — has a `.section` (the lane, e.g. `docking`).
  - `summary` (`ProofCapsuleSummary`) — `.title` (the claim), `.finding` (the readout), `.why_it_matters`, `.limitations`.
  - `payload` (free dict) — carries `signal` (`supports|refutes|neutral|none`), `confidence`, `validation_type`, `compute_job_id`, `falsification_preregistration`, `controls_confound`, `provenance_flag`, `redock_rmsd`, …
  - `content_hash` — a deterministic hash over the **scientific content only** (signature/lineage/`submitted_by` are excluded). `parent_content_hash` + `lineage_index` form a Merkle edit chain; `signature` is an Ed25519 sig of `content_hash`.
  - `status` (`ProofCapsuleStatus`), `submitted_by`, `reviewed_by`.
- `RunManifestRecord` (~659) — durable aggregate report of a run. `manifest_type="falsification_campaign"` is the campaign report; rollup/rows live verbatim in `output_refs`. `manifest_type="agent_run"` wraps single agent runs.
- `CollaboratorRecord` (~4812) — a trusted principal: `principal` (stable actor slug), `role` (`operator|collaborator`), `scopes`, `status` (`pending|active|revoked`), `public_key` (Ed25519 hex), `auth_subject` (WorkOS user id). `has_scope()` requires `status=="active"`.
- Falsification value objects (~6927–7180): `KillCriterion`, `ConfoundFlag`, `BeliefState`, `FalsificationPlan`, `FalsificationPlannerResult`, `FalsificationLoopResult`, `LaneInputResolution`, `FailureCorpusEntry`.
- `ComputeJobRecord` (~4104) — a dispatched compute job (`runner_kind`, `container_image`, `validation_type`, `status`, `checkout_manifest_hash`, …). The provenance auditor checks capsules against this.

**Scopes** (~344):
```
CollaboratorScope = lease_workspace | submit_compute | submit_capsule
                  | accept_capsule    (operator only — write gate)
                  | promote_candidate (operator only — write gate)
COLLABORATOR_SCOPES = first three;  OPERATOR_SCOPES = all five.
```
A non-operator record is **silently stripped** of the two write-gate scopes by the normalizer — you cannot give a collaborator `accept_capsule`.

**How to extend.** Add fields to the Pydantic model, add a normalizer clause if the field needs trimming/dedup, and mirror the change in `web/lib/types/domain.ts`. Persisted models also need a column or `payload`/`metadata` slot in the stores (Area 4) — most free-form data rides in `payload`/`metadata` JSON, so new analysis fields rarely need a migration.

**Gotchas.**
- Signal vocabulary is **`supports | refutes | neutral | none`** — it is **`refutes`**, not `refuted`. (`refuted` is a *rollup* status, not a capsule signal. This exact confusion was a real bug in `rocrate_export.py`; see Area 8 and `threadnotes/rocrate-export-review.md`.)
- `content_hash` deliberately excludes provenance wrappers — never fold a signature into it or external re-signing breaks.

---

## 3. The service layer (the orchestrator)

**What it does.** `HSAResearchService` is the **central god-object**: one class, ~15,000 lines, holding nearly every use-case as a method over a single `ResearchRepository`. The web API, CLI, and Dagster assets all call into it; it owns authorization, the falsification loop, the gates, and the write-gate.

**Key files / entrypoints.**
- `src/hsa_research/ingestion_bridge/service.py`
  - Class `HSAResearchService` (~1114). `__init__(repository=None, model_profiles=None)` — defaults to `build_default_repository()`; reads `TWOG_REQUIRE_REGISTERED_PRINCIPALS` env (deny-unknown auth, default off).
  - Exceptions: `CollaboratorAccessError` (~1106, → HTTP 403), `WorkspaceLeaseError` (~1110, → HTTP 409).

**Method families (the map — not exhaustive):**
- **Candidates / proposals:** `get_public_candidate` (~1460), `list_public_candidates` (~1591), `submit_candidate_proposal` (~1516), `list_candidate_proposals` (~1548), `decide_candidate_proposal` (~1556).
- **Capsules:** `list_proof_capsules` (~2215), `submit_external_proof_capsule` (~2145, BYOC), and the **write-gate** `accept_proof_capsule` (~2784) / `promote_proof_capsule_to_candidate` (~3093).
- **The falsification loop:** `propose_next_falsification` (~2247), `register_falsification_test` (~2334), `run_falsification_round` (~2422), `run_falsification_loop` (~2488), `run_falsification_campaign` (~2557), `get_failure_corpus` (~2296), `resolve_lane_inputs` (~2308). See Area 6.
- **Collaborators / access:** `request_collaborator_access` (~1871), `approve_collaborator` (~1925), `revoke_collaborator` (~1863), `list_collaborators` (~1854), `resolve_collaborator_by_auth_subject` (~1913), `_authorize` (~1951, the scope gate), `lease_workspace` (~1979), `open_collaborator_sandbox` (~2104).
- **Gates / validation helpers:** `_confound_audit_dict`, `_provenance_audit_dict`, `verify_capsule_provenance`, `dispatch_validation_request_queue_item` (~4401), `run_compute_validation_flow`.
- **Manifests:** `get_run_manifest` (~7378), `list_run_manifests` (~7381).

**How to run or extend.** Instantiate with a repo: `HSAResearchService(SQLiteResearchRepository(path))` for tests, `HSAResearchService(PostgresResearchRepository(url))` for prod. Add a new use-case as a method; if it's a write-gate point, call `self._authorize(principal, scope)` first. Heavy/optional collaborators (planners, auditors, runners) are imported lazily inside methods to keep import cost down.

**Gotchas.**
- It is a god-object on purpose (fast iteration, single transaction surface). Resist adding cross-cutting state to `__init__`; pass dependencies through method args or attach optional seams (e.g. `self.input_resolvers`, read via `getattr(self, "input_resolvers", None)`).
- `_authorize` is **default-trust**: an *unregistered* principal passes unless `TWOG_REQUIRE_REGISTERED_PRINCIPALS=true`. This preserves the solo-operator model; flip it on for multi-tenant prod.

---

## 4. Persistence

**What it does.** Stores all records behind one `Protocol` interface, with two implementations: Postgres (Neon, prod) and SQLite (tests).

**Key files / entrypoints.**
- `repository.py` — `ResearchRepository` (`Protocol`, ~73). The contract every store implements (`upsert_proof_capsule`, `list_proof_capsules`, `get_public_candidate`, `acquire_workspace_lease`, `upsert_run_manifest`, `get_collaborator_by_principal`, …).
- `postgres_store.py` — `PostgresResearchRepository(database_url, seed=True)` (~79). Uses `psycopg2` + `RealDictCursor`. Calls `_init_schema()` in `__init__` — the schema is **self-bootstrapping** (idempotent `create table if not exists`), so pointing it at an empty Neon DB just works.
- `local_store.py` — `SQLiteResearchRepository(ResearchRepository)` (~80). The test/dev backend; accepts a file path or `:memory:`-style use.

**How it works / data flow.** The service holds one repository instance and calls typed methods; the stores serialize Pydantic models (mostly to JSON columns) and back. `build_default_repository()` picks the backend (env-driven, `HSA_STORAGE_BACKEND`).

**Env.** `NEON_DATABASE_URL` (also accepts `DATABASE_URL`/`POSTGRES_URL`/`HSA_DATABASE_URL`). Lives in the gitignored `.env`. The Neon project is `twog-v2-1` (pooled string).

**How to extend.** Add the method to the `ResearchRepository` Protocol, then implement in **both** stores (SQLite first — it's what tests run). For new columns add an idempotent `alter/create` clause in the Postgres `_init_schema`; prefer stashing analysis data in existing `payload`/`metadata` JSON to avoid migrations.

**Gotchas.**
- The Postgres store opens a single connection and the read API server (`run_web_api.py`) shares one service instance — it's single-threaded (stdlib `HTTPServer`), so that's safe but won't scale to concurrent writers without rework.
- Any new method must exist in both stores or tests (SQLite) diverge from prod (Postgres).

---

## 5. Compute lanes & the spend gate

**What it does.** Runs the real science on Modal (GPU/CPU) — and, crucially, **refuses to spend money on inputs it can't verify**. This refuse-on-unverified behavior is the moat.

**Key files / entrypoints.**
- `src/hsa_research/modal_app.py` — the Modal app (`modal.App("twog-compute")`). Functions: `run_gnina_remote` (gnina docking, A100), `run_boltz_remote` (Boltz-2 co-folding, A100), `run_md_checkpoint_remote` / `run_md_prep` / `run_md_production` (OpenMM MD, T4/A100, durable checkpoints on a Modal Volume), `run_omics_review_remote` (CPU). Plus `local_entrypoint`s for smokes (`dock`, `dock_pi3k`, `cofold`, `md_prep`, `md_production`, `md_checkpoint`, `main`, `provision`). *(There is a 20-line shim at `src/hsa_research/ingestion_bridge/modal_app.py`; the real app is the top-level one.)*
- `lane_inputs.py` — `resolve(candidate, lane, *, resolvers, target_library)` → `LaneInputResolution`. Maps each lane to its config key + required key-sets. Curated per-candidate inputs (`candidate.metadata["lane_inputs"][lane]`) always win.
- `input_resolvers.py` — `NetworkInputResolvers` (PubChem SMILES, RCSB structures, UniProt sequences). **Network access lives ONLY here**, behind the lane_inputs seam; everything else stays offline/deterministic so CI never touches the network. Any failure returns `None` — never fabricated.
- `target_library.py` + `data/target_library.json` — the curated, **redock-verified** target library. `curated_docking_config()` returns a config only if a verified entry with a prepared receptor + box exists, else `None` (refuse).
- `compute_runners.py` — the runner registry: `MockComputeRunner` (`mock`, CI), `LocalOmicsComputeRunner` (`local`, in-process omics), `CheckpointingComputeRunner` (`checkpoint`), `ContainerComputeRunner` (`container`, BYOC), and the Modal path (`modal`). `available_compute_runners()` returns the registered kinds.
- `scripts/verify_target_library.py` — the one-time job that redocks a target's native ligand and records the QC verdict (RMSD ≤ 2 Å + PoseBusters-valid).

**How it works / data flow (the spend gate).** When the loop builds a test, it calls `lane_inputs.resolve`. For **docking**, resolution goes *only* through the curated target library (`_resolve_docking_from_library`): no verified entry → `resolved=False`, no GPU spent. There is deliberately **no dirty RCSB full-text fallback for docking** (`_NETWORK_STRUCTURE_LANES` is empty). Sequence lanes (cofolding) may resolve a UniProt sequence + PubChem SMILES from the network. A plan whose inputs are unresolved is still pre-registered (honesty) but flagged `inputs_ready=False` and ranked last; on a **real** runner the round terminates rather than burn cost producing nothing.

**How to run or extend.**
- Auth Modal once: `python -m modal setup`. Deploy: `PYTHONPATH=src python -m modal deploy src/hsa_research/modal_app.py`. Smoke: `PYTHONPATH=src python -m modal run src/hsa_research/modal_app.py::dock`.
- **Add a lane:** add the validation_type → config-key mapping + required key-set in `lane_inputs.py`, a Modal function in `modal_app.py`, a parser/`build_*_result` helper (like `docking.py`/`cofolding.py`), a kill-criterion template in `falsification_planner._generic_kill_criterion`, and a confound class in `confound_auditor.CONFOUND_TAXONOMY`.
- **Add a docking target:** add an entry to `data/target_library.json`, then run `scripts/verify_target_library.py` to flip `verified=true`.

**Gotchas.**
- The gnina and Boltz-2 invocations are flagged SCAFFOLD in the code — the parse/signal logic is real and tested, but the binding box / image tag / output paths must be confirmed on the first real GPU run. (Docking + MD have since been run on real GPU per the launch checklist.)
- **Always confirm spend with Chase before flipping on any path that costs Modal GPU money.**
- The MD `run_md_checkpoint_remote` harmonic-well harness proves the GPU + checkpoint *infrastructure*, not a scientific MD result (it says so in its `limitations`).

---

## 6. The autonomous falsification loop

**What it does.** twog's discovery engine: propose → pre-register → resolve-inputs (or refuse) → dispatch → read vs pre-registration → write capsule → repeat, terminating without ever auto-promoting.

**Key files / entrypoints.**
- `falsification_planner.py` — **pure, deterministic, NO I/O.** `distill_belief_state()` reads the signed ledger into a `BeliefState`; `rank_falsification_tests()` generates + ranks tests by value-of-information-per-dollar; `propose()` composes them into a read-only `FalsificationPlannerResult`. **Generation is deterministic, NOT LLM-backed.**
- `service.py` — `propose_next_falsification` (wraps the planner in `AgentRunner` for durable provenance; zero writes to candidates/capsules), `run_falsification_round`, `run_falsification_loop`, `run_falsification_campaign`.
- `failure_corpus.py` — `derive()` / `ruled_out_lanes()`: the queryable record of negative knowledge, derived purely from the capsule ledger. The planner reads `ruled_out` to apply a **novelty penalty** (×0.5 VOI) to already-settled lanes.
- `agent_runner.py` — `AgentRunner.run()`: persists an `AgentRunRecord` + `RunManifestRecord` before and after any agent callable (durable provenance ledger), recording running/completed/failed.

**How it works / data flow.**
1. `propose_next_falsification` reads the candidate + its capsules + decisions, computes `runnable` lanes, `ruled_out` (failure corpus), and `inputs_unresolved` lanes, then calls `falsification_planner.propose`. Confidence is read **only** from `compute_artifact` capsules that actually carry a signal; a thin signal base (<2 signalful capsules) caps confidence at 0.3 (`_THIN_SIGNAL_CONFIDENCE_CAP`) rather than fabricating certainty.
2. `register_falsification_test` content-hashes the kill-criterion **before** dispatch (the pre-registration lock), resolves real lane inputs (or records the gap), and creates+approves a validation-queue item.
3. `run_falsification_round` dispatches (`mock` in CI, `modal` on GPU), reads the resulting capsule's `signal`, and evaluates `kill_criterion_met` (signal == `observed_signal_kills`).
4. `run_falsification_loop` chains rounds until: kill-criterion met (`refuted`), no runnable proposal with `rounds_run>0` (`standing`), budget exhausted / max rounds / never ran (`underpowered`). It **NEVER auto-promotes** — a refuting/neutral outcome terminates without promotion (the "Megquier shape"). Only `_autonomously_runnable_lanes` (ungated) are eligible; expert-gated lanes like MD are surfaced separately.
5. `run_falsification_campaign` runs the loop across a roster (default: all `validation_ready` candidates) under budget caps (`$0.50/candidate`, `$2.0/campaign` default) and persists a `RunManifestRecord(manifest_type="falsification_campaign")`.

**How to run.** `service.run_falsification_loop("CAND-ID", runner_kind="mock")` for a dry exercise; `runner_kind="modal"` for real compute (budget-cap it). The campaign is what the Dagster schedule ticks (Area 9).

**Gotchas.**
- The planner is pure — don't add I/O to it; the service supplies the runnable-lane set, cost function, and resolvers.
- Kill-criterion evaluation is currently **signal-based** (`observed_signal == observed_signal_kills`); metric-threshold evaluation against real lane metrics lands once lanes emit them.
- On a real runner, an inputs-unready best proposal terminates the round cleanly (it used to burn proposal cost producing no capsule — the bug that made everything read `underpowered/max_rounds`).

---

## 7. Gates

**What it does.** Three independent gates keep bad evidence from promoting; the third is a **human**.

**Key files / entrypoints.**
- `confound_auditor.py` — `audit(capsule, ledger)` (validity pre-gate). For a `supports` capsule, enumerates known confounds for its lane (`CONFOUND_TAXONOMY`: omics→`tumor_purity, batch, cell_composition, normalization`; docking→`pose_instability`; md→`forcefield_artifact`) and checks the ledger for a surviving control capsule (`payload.controls_confound == X`). Best attainable status is **`survived_known_confounds`** — never "true". Missing control → `unauditable` (cannot accept); a refuting control → `refuted`.
- `provenance_auditor.py` — `audit(capsule, compute_job)` (integrity pre-gate). Verifies the capsule's claimed run matches the linked compute job (`compute_job_id`, `candidate_id`, hashes, `validation_type`, `completed`). For foreign/BYOC jobs (`runner_kind=="container"`) it additionally requires a digest-pinned image and matching runner principal. A capsule making no claim verifies trivially. Integrity, **not** validity.
- `target_library.py` / `lane_inputs.py` — the **docking spend-gate** (Area 5): refuse-on-unverified inputs.
- `service.accept_proof_capsule` / `promote_proof_capsule_to_candidate` — the **terminal human write-gate**.

**How it works / data flow.** In `accept_proof_capsule` (~2784): the provenance gate runs first (foreign capsules additionally require a valid signature), then the confound gate (only for `supports` signals). A failed gate does **not** raise — it stamps a `provenance_gate`/`confound_gate` verdict into the capsule's `metadata` and leaves it `submitted`, so the operator cannot launder a bad capsule. Only a clean capsule transitions to an accepted status. `_authorize(reviewer, "accept_capsule")` enforces operator scope before any of this.

**How to extend.** Add a confound class to `CONFOUND_TAXONOMY` for a lane; add a new integrity check inside `provenance_auditor.audit`. Both auditors are **pure** (no I/O) — the service resolves the ledger/job and supplies it, which is why the RO-Crate exporter can recompute provenance verdicts offline.

**Gotchas.**
- The gates can be disabled per-call (`enforce_confound_gate=False`, `enforce_provenance_gate=False`) — used in tests; never disable in the autonomous path.
- A passing confound audit is "survived known confounds," not correctness. Don't let UI copy overstate it.

---

## 8. Proof capsules & provenance

**What it does.** Turns each result into a sealed, tamper-evident, signed, re-checkable unit, and (optionally) packages a candidate's whole dossier as a portable RO-Crate.

**Key files / entrypoints.**
- `proof_capsules.py` — `submit_proof_capsule()` (validates a workspace check-in; `external_submission=True` enforces the BYOC gate, Area 11), `build_proof_capsule_library()`, `capsule_content_hash()` (the hash a BYOC submitter signs), `_capsule_content_hash()` (canonical hash over scientific content only). Also a raw-secret scrubber (`_SECRET_PATTERNS`) and a 1 MB external-payload cap.
- `provenance.py` — the crypto primitives: `generate_keypair()`, `sign()`, `verify()` (Ed25519 over `content_hash`); `verify_lineage()` / `next_lineage()` (the hash-linked Merkle DAG = edit history); `merkle_root()` (what you'd anchor to a public ledger). Honest limit, stated in the module: proves **integrity**, not **validity**.
- `rocrate_export.py` — `candidate_to_crate()`: exports every capsule for a `candidate_id` as a Process Run Crate (JSON-LD provenance graph) for WorkflowHub/Galaxy/nf-core/runcrate. Write-only, off the hot path, `rocrate` is a lazily-imported optional extra.

**How it works / data flow.** A capsule's `content_hash` is a pure function of its scientific content; `signature` (Ed25519 over the hash) and `parent_content_hash`/`lineage_index` wrap around it and are excluded from it. Anyone with a principal's `public_key` can re-verify. The RO-Crate maps candidate→root Dataset, capsule→result CreativeWork (`content_hash`=identifier), compute job→CreateAction, prereg block→CreativeWork, and stamps a recomputed `provenance_auditor` verdict per CreateAction.

**Gotchas (from `threadnotes/rocrate-export-review.md` + the response).**
- A real bug was the rollup checking `"refuted"` instead of the actual signal `"refutes"` — **fixed**; the rollup now faithfully derives the three-state `standing | refuted | underpowered` (reading the pre-registered `observed_signal_kills`). Watch for this `refutes`/`refuted` confusion anywhere you read signals.
- Gate-verdict stamping (provenance) is in v1; **confound verdict is not yet in the crate** (it needs lane config to recompute) — a known fast-follow.
- `runcrate` CI validation is still a follow-up, so `conformsTo` is declared but unproven. Run tests with `uv run --extra rocrate pytest tests/test_rocrate_export.py`.

---

## 9. Orchestration (Dagster)

**What it does.** Schedules the autonomous engine ticks (and a large set of older ingestion pipelines).

**Key files / entrypoints.**
- `dagster_assets.py` — defines assets, jobs, and schedules. The relevant one: the `falsification_loop_report` op (~5754) and `falsification_loop_hourly_schedule` (~9064).
- `dagster_resources.py`, `src/hsa_dagster/definitions.py` — Dagster wiring/resources.

**How it works / data flow.** `falsification_loop_report` runs `service.run_falsification_campaign` for validation-ready candidates, stamping `submitted_by=f"falsification_loop_scheduler:{run_id}"`. It **defaults `dry_run=True`** (the op-level default in `falsification_loop_report`); the schedule's `run_config` overrides it to `dry_run=False, runner_kind="modal", max_candidates_per_tick=3, budget_usd_per_candidate=0.50, tick_budget_usd=2.0`.

**The schedule is currently STOPPED:** `default_status=dg.DefaultScheduleStatus.STOPPED` (cron `0 * * * *`). Turning the engine on = enabling this schedule (a launch-checklist Tier-1, spend-gated decision — confirm with Chase).

**Gotchas.**
- It never promotes — the loop is terminal-gated regardless of the schedule.
- The dry-run default lives on the op, the live config lives on the schedule — to test the op directly you'll get a dry run unless you pass config.

---

## 10. Web API

**What it does.** The thin JSON boundary the Next.js app calls. It maps identity in and exceptions to HTTP status out — it does **not** re-implement authz (the service's `_authorize` + the gates do that).

**Key files / entrypoints.**
- `web_api.py` — `dispatch(service, *, method, path, principal, body) -> (status, json)` is a **pure router** (no HTTP, fully unit-testable). `run_api_server()` is the stdlib HTTP wrapper. `resolve_principal_from_token()` is the WorkOS seam.
- `web_presenters.py` — pure functions projecting raw records → the front-end display shapes (`present_capsule`, `present_candidate`, `present_manifest`, `present_engine_state`).
- `scripts/run_web_api.py` — the servable entrypoint (resolves `NEON_DATABASE_URL`, builds one service, serves).

**How it works / data flow.**
- **PUBLIC read surface (no auth):** `GET /public/state`, `/public/capsules[/:id]`, `/public/campaigns[/:id]`, `/public/candidates[/:id]`. These return **presented** (display-shaped) data via `web_presenters`, never raw records. This powers STATE/EVIDENCE/RUNS.
- **Gated routes** (require a WorkOS-verified principal): `/me`, collaborator lifecycle, `/candidates`, `/campaigns`, `/target-library`, `/sandbox/open`, `/contributions`, the operator capsule write-gate (`/capsules/:id/accept|promote`), and proposal review. These return raw records and let the service enforce scope.
- Exceptions map cleanly: `ApiError`→its status, `CollaboratorAccessError`→403, `WorkspaceLeaseError`→409, `(KeyError|ValueError|TypeError)`→400.

**How to run.** `NEON_DATABASE_URL=... PYTHONPATH=src python scripts/run_web_api.py --host 0.0.0.0 --port 8000`. Public reads work immediately; gated routes 401 until WorkOS is wired (the verifier is stubbed to return `None`).

**Gotchas.**
- The real-backend transport in the front-end (`web/lib/api/config.ts`) is wired but only fires when `NEXT_PUBLIC_USE_MOCKS=false`.
- Presenters degrade missing fields gracefully (invalid signal → `neutral`, missing provenance flag → omitted) — keep that defensiveness when adding fields.

---

## 11. Web front-end

**What it does.** Next.js 14 App Router (TypeScript) site: the public Phase-1 surface (watch the engine work) plus gated operator/contribute surfaces.

**Key files / entrypoints.**
- `web/app/page.tsx` — **STATE** (the live hero: tiles, lanes, latest capsules, the live-think stream).
- `web/app/evidence/` (+ `[capsuleId]`) — **EVIDENCE** (proof capsules). `web/app/runs/` (+ `[manifestId]`) — **RUNS** (campaigns). These three are the public surface.
- `web/app/operate/*` (write-gate, candidates, collaborators, targets, applicants, campaigns) and `web/app/contribute/*` (apply, submit, tracker, sandbox) — the gated surfaces.
- `web/app/globals.css` — the design system (ported from twog.bio: IBM Plex, B&W editorial, electric-blue accent).
- `web/components/shell/brand-shell.tsx` — the brand chrome. `web/components/state/live-think.tsx` — the "watch it think" stream (propose → resolve → refuse → dock → verdict → seal+sign).
- `web/lib/api/*` — the client. A single switch `NEXT_PUBLIC_USE_MOCKS` (`web/lib/api/config.ts`) routes every call to bundled mocks (`web/lib/mocks/*`) or the real `/public/*` API.
- `web/lib/types/domain.ts` — the display contract (mirrors `contracts.py`; do not invent behavior beyond it).

**How it works / data flow.** Server components `await api.engine.state()` etc.; with mocks on (default) they resolve fixtures after a simulated delay; with mocks off they `fetch` `${NEXT_PUBLIC_API_BASE_URL}/public/*`.

**How to run.** `cd web && npm install && npm run dev`. For real data: set `NEXT_PUBLIC_USE_MOCKS=false` + `NEXT_PUBLIC_API_BASE_URL` in `web/.env.local` and run the API (Area 10). `DEV_AUTH_BYPASS=1` renders gated UI locally without WorkOS.

**Gotchas.**
- The `/contribute` + `/operate` pages were flagged stale (old brand) in the launch checklist — rebuild on the current design system before exposing them.
- Engine metadata (`testsPassing`, `coverage`) in `web_presenters.ENGINE_*` are marked constants, not DB-derived.

---

## 12. Auth (WorkOS AuthKit — planned/headless)

**What it does.** Authentication for the gated surfaces. **Planned** — the seams exist, the provider isn't stood up.

**Key files / entrypoints.**
- `web_api.py` — `resolve_principal_from_token(service, token, *, verify_token)`: `verify_token` (the WorkOS seam, injected) yields a stable `auth_subject` (WorkOS user id), which maps to a `CollaboratorRecord` via `service.resolve_collaborator_by_auth_subject`.
- `contracts.py` — `CollaboratorRecord.auth_subject` is the mapping field.
- `web/middleware.ts` — `authkitMiddleware` (enforcing; public allowlist `/`, `/login`, `/callback`; `DEV_AUTH_BYPASS=1` skips it). `web/lib/auth/*`, `web/app/login/route.ts`, `web/app/callback/route.ts`.
- `web/.env.example` — the WorkOS keys (`WORKOS_API_KEY`, `WORKOS_CLIENT_ID`, `WORKOS_COOKIE_PASSWORD`, `WORKOS_REDIRECT_URI`).

**How it works / data flow.** Middleware enforces *being signed in*; per-surface guards enforce *scope* (defense in depth). Token → `verify_token` → `auth_subject` → collaborator → `principal` (the actor string the service authorizes on).

**Gotchas.**
- `verify_token` is **stubbed to return `None`** in `scripts/run_web_api.py` — gated routes 401 until you implement JWKS verification. That's fine for the Tier-1 public launch.
- The **custodial Ed25519 key vault is NOT built** (platform-held keys, encrypted at rest). Self-held keys are the supported path today; custodial is a Tier-2 item.

---

## 13. Collaborator boundary / BYOC

**What it does.** Lets an *outside* collaborator run compute on their own backend and submit a capsule — safely. The authenticated boundary proves *who* produced it and that it's bound to a leased sandbox; validity stays the auditors' + human's job.

**Key files / entrypoints.**
- `service.py` — `open_collaborator_sandbox` (~2104), `lease_workspace` (~1979, atomic acquire), `submit_external_proof_capsule` (~2145).
- `proof_capsules.py` — `_enforce_external_submit_gate()`: an external capsule is admitted only if the submitter is (1) a registered **active** collaborator, (2) holds the `submit_capsule` scope, (3) currently holds the workspace's lease, and (4) signed the `content_hash` with the key matching their registered `public_key`. Plus a 1 MB payload cap and content-hash idempotency (re-submits return the existing capsule, so nobody can flood the ledger).
- `compute_runners.py` — `ContainerComputeRunner` (~453, `runner_kind="container"`): runs the job's **digest-pinned** container; the provenance auditor then verifies the image is `@sha256:`-pinned and the runner principal matches.

**How it works / data flow.** A non-operator lease flips the workspace `gate_policy` to `external_collaborator`; capsules from it require the external submit gate on entry and the operator accept/promote write-gate before they count.

**Gotchas.**
- The external submit gate is **always enforced** regardless of `TWOG_REQUIRE_REGISTERED_PRINCIPALS` — that flag only governs the internal `_authorize` default-trust.
- An external submitter signs the **request's own** values (`capsule_content_hash`) with no workspace fallback; the gate recomputes exactly that. Don't apply the workspace fallback to external submissions or the hash diverges and a legit submit is falsely rejected.

---

## 14. CLI

**What it does.** A local runner for the (older) ingestion pipelines and operational tasks.

**Key files / entrypoints.**
- `cli.py` — `main()` (~135), an `argparse` app with subcommands (`init`, `coverage`, `ingest`, `ingest-source`, `structured-pipeline`, `structured-report`, …). `--db` selects a SQLite path.

**Gotchas.**
- The CLI is oriented at the literature-ingestion side, not the falsification loop (which is driven via Dagster / direct service calls). RO-Crate export currently runs via `python -m ...rocrate_export`; wiring it as a CLI subcommand is a known follow-up.

---

## 15. Deployment & ops

**What it does.** Stands the system up: engine on a cadence, API + web deployed, Neon + Modal connected.

**Key files / entrypoints.**
- `scripts/run_web_api.py` — serve the API against Neon.
- `src/hsa_research/modal_app.py` — deploy/run compute (`python -m modal deploy ...`).
- `dagster_assets.py` — enable `falsification_loop_hourly_schedule` to turn the engine on.

**Env vars.**
- `.env` (gitignored): `NEON_DATABASE_URL`, `HSA_STORAGE_BACKEND`.
- `web/.env.local`: `NEXT_PUBLIC_USE_MOCKS`, `NEXT_PUBLIC_API_BASE_URL`, `DEV_AUTH_BYPASS` (dev only).
- `web/.env.example`: the WorkOS keys (Tier-2).
- Service flags: `TWOG_REQUIRE_REGISTERED_PRINCIPALS` (deny-unknown authz), `TWOG_API_HOST`/`TWOG_API_PORT`/`TWOG_API_ALLOW_ORIGIN`.

**Launch plan (see `threadnotes/launch-checklist.md`).**
- **Tier 1 ("It's alive", no auth):** turn on the budget-capped engine schedule (confirm spend) → deploy the API to a host → point web at `/public/*` (done locally) → deploy web + domain (`twog.bio` marketing → app at e.g. `app.twog.bio`). Built + working locally; remaining is deploy + flip the schedule.
- **Tier 2 (collaborators):** stand up WorkOS + implement `verify_token`; build the custodial key vault; flip `TWOG_REQUIRE_REGISTERED_PRINCIPALS=true`; rebuild the contribute/operate surfaces.

**Gotchas.**
- Anything that spends Modal GPU/cloud money: confirm with Chase first.
- The API server is single-threaded stdlib HTTP — fine for read-only public traffic, not a high-write production server.

---

## 16. Testing

**What it does.** ~720 tests pin the contracts, gates, loop, presenters, and routes.

**Key files / entrypoints.**
- `tests/` — ~41 test modules, named per area (`test_falsification_loop.py`, `test_falsification_planner.py`, `test_confound_auditor.py`, `test_provenance_auditor.py`, `test_target_library.py`, `test_lane_inputs.py`, `test_web_api.py`, `test_web_presenters.py`, `test_phase_b_submit_gate.py`, `test_foreign_provenance.py`, `test_rocrate_export.py`, …).
- `tests/_helpers.py` — fixtures + the SQLite pattern: `make_service(tmp_path)` returns `HSAResearchService(SQLiteResearchRepository(tmp_path / "hsa.sqlite3"))`. Network resolvers are mocked; the loop runs with `runner_kind="mock"`.

**How to run.** `pytest` (or `uv run pytest`). RO-Crate tests need the extra: `uv run --extra rocrate pytest tests/test_rocrate_export.py`.

**Gotchas.**
- **CI is offline + deterministic.** Tests use the `mock` runner and never hit the network (PubChem/RCSB/UniProt live only in `input_resolvers.py`, mocked in tests) or Modal. Keep new logic pure/injectable so it stays testable without GPU or network.
- A test that seeds a value the engine never emits (e.g. signal `"refuted"`) gives false confidence — assert against the real vocabulary (`refutes`). This was the exact RO-Crate trap.

---

## 17. Current state (2026-07) — the live twog.bio app, contribute lane, triage board

> §11 above describes the **older `web/` app (Next 14)** — the gated operator/collaborator *reference* app, which is **localhost-only, never deployed**. Since then, the **public product moved to a new app: `twog/` (Next 16 + React 19), which IS what serves `twog.bio`.** This section is the source of truth for what's live.

**Two front-ends, one Neon.** `web/` = the older Next 14 app (operator console, WorkOS scaffold — not deployed). `twog/` = the live Next 16 "v4" dark proof-engine site on twog.bio. Both read the **same Neon DB**. The `twog/` app reads Neon **directly via `pg`** (`twog/lib/neon.ts` `neonRows`/`neonWrite`, graceful []-on-failure), NOT via the Railway engine API — so the data surfaces are live even though the Railway API is unshipped.

**Live public surfaces (twog.bio, `twog/app/`):**
- `/` → the v4 "PROOF" landing (`app/v4/page.tsx`, promoted to home; old landing at `/legacy`). Hero HUD, live ledger, marquees.
- `/candidates` (+ `/[candidateId]`) → every idea (drug × target) read live from Neon `public_candidates` (`lib/public-candidates-live.ts`), verdict from capsule signals, cross-linked to evidence + runs.
- `/evidence` (+ `/[capsuleId]`) → proof capsules (`lib/public-capsules.ts`); **two-tier**: public teaser always, deep tier (rubric + provenance) behind an **email gate** (`twog_email` cookie via `/api/subscribe`, `lib/gate.ts`).
- `/runs` (+ `/[manifestId]`) → falsification campaigns + rollups (`lib/public-runs.ts`, `findRunForCandidate`).
- `/involved` → the Get-Involved ladder + Suggest form (`/api/suggestions` → Neon `public_suggestions`).

**Verdict model (`lib/verdict.ts`):** capsule signal `refutes`→`ruled-out`, `supports`→`still-standing`, else `needs-more`. Ceiling is "still standing" — never "proven".

**The Contribute lane (the hands-on rung):**
- Public intake: `ContributeForm` (`components/v4/ContributeForm.tsx`) → `POST /api/public-candidates/[id]/contributions` → Neon `candidate_contribution_intake` (`lib/candidate-contributions.ts`). Validates against **live** candidates (`asPublicCandidateDetail` adapter). **Env-gated `TWOG_CONTRIBUTIONS_OPEN` (default PAUSED → 503).** Pre-go-live gate: add per-IP rate limiting.
- Operator triage board: **`/operate`** (`app/operate/`), **passphrase-gated** (`TWOG_OPERATOR_TOKEN`, `lib/operator-gate.ts`, constant-time compare, httpOnly cookie — a **stopgap until WorkOS**). `lib/contribution-triage.ts` is a **faithful TS port** of the engine's `src/hsa_research/ingestion_bridge/candidate_contribution_intake.py` (same statuses, same `promoted_queue_id` format `candidate_contribution:{id}:{route}`, same review-note append) — so the twog board and the engine/Dagster path write identical shapes. Board **only routes, never publishes**. The canonical home is the `web/` `/operate` console + engine `triage_candidate_contributions()`; same Neon table → no data migration when it migrates.
- Smoke test: `twog/scripts/smoke-contribution-lane.mjs` (`npm run smoke:contrib`) — intake + gate + all 7 triage transitions vs the engine contract; env-driven, self-cleaning.

**Deploy:** `twog.bio` = the **`twog` Vercel project** (rootDir `twog`), deployed **manually** via `vercel build --prod` + `vercel deploy --prebuilt --prod` from the `moonshot-rubric` working tree (NOT git auto-deploy). Prod env vars set: `NEON_DATABASE_URL`, `TWOG_OPERATOR_TOKEN`. The full launch (v4 site + contribute lane + triage + inc8 engine spine) is in **open PR #29 (`moonshot-rubric` → `main`)**, which diverges from `origin/main` (a lighter v4 landed earlier via PR #28) — landing it needs a rebase.

**Known seams (honest gaps):**
- **Railway engine API not deployed** → the homepage "watch the engine work" ledger polls `/api/ledger` → engine `/public/activity`, which is unreachable in prod → falls back to **REPLAY** (`source:"unreachable"`). Deploying Railway makes it truly live.
- **WorkOS stubbed** → operator/collaborator write routes 401; the twog `/operate` passphrase is the interim gate.
- **Boltz-2 co-folding + OpenMM MD are scaffolds** (in-code warnings) — only **gnina docking + omics** carry defensible signal; keep the others out of any confidence framing.
- Public intake + the operator board are both **env-gated OFF by default**.

**In progress — the Research Director Agent (Option 3, 2026-07):** a net-new engine agent (`ResearchDirectorAgent`, reusing `agent_runner.py` + `frontier_research_policy.py`) that reads the proof ledger + validation-ready roster + failure corpus + confound verdicts and emits grounded, re-derivable **`DecisionLog`** records (which candidate next, why, confidence, cost) — every claim **must cite a real `capsule_id`/target** or be rejected. Surfaced on a new live **`/orchestration`** reasoning feed. Hard **$20 API cost gate** built before the first paid call (per always-confirm-spend). This is the "watch a frontier model direct the science, grounded" surface.

**Key `twog/` file map:** `lib/{neon,verdict,gate,public-runs,public-capsules,public-candidates-live,candidate-contributions,contribution-triage,operator-gate}.ts` · `app/{v4,candidates,evidence,runs,involved,operate}/` · `app/api/{subscribe,suggestions,ledger,public-candidates/*,operator/*}/` · `components/v4/*` · `app/detail.css` (the v4-dark design system) · `scripts/*.mjs`.
