# Phase 3 — Pluggable expert-gated compute lanes + first real provider

> Builds on the Phase-2 loop (compute → capsule → promotion). Goal: turn the single hardcoded MD
> lane into a **pluggable lane pattern** behind the gate, register a **real compute provider**, and
> stand up the **first real scientific lanes**. Design was pressure-tested across gate generality,
> a 5-lane shape test, multi-step chaining, autonomy, and a two-shape checkpoint model — see
> `FRONTIER_SCAN.md` for the verified tooling this references.

## Split: 3a (in-repo, buildable now) vs 3b (cloud/GPU, needs accounts + payload pick)

- **3a — the abstraction.** Lane registry, generalized gate, `parse_result`, autonomy policy,
  pipeline/checkpoint *affordances*, and a CPU/mock lane proving pluggability. **Fully testable
  today with the mock provider; no cloud, no GPU.** This is the safe strangler refactor.
- **3b — the reality.** Register **Modal** (or RunPod+Cedana) as a real `ComputeRunner`; build the
  **ADMET → gnina → Boltz-2** lane containers; run real GPU jobs. Needs cloud accounts, GPU budget,
  container builds, and the bio dept's first-payload priority. Do **not** block 3a on 3b.

---

## Locked design decisions (from the exploration rounds)

1. **Lanes are atomic; chaining is a pipeline *of* lanes, never a lane *with* stages.** Three
   layers: **Provider** (where one compute runs — built) → **Lane** (what one atomic compute is +
   its gate + its parser) → **Pipeline** (how lanes chain — *deferred*, affordances baked now).
2. **The gate stays the crown jewel, generalized not weakened.** `_md_live_submit_gate` is already
   lane-agnostic in spirit — only the input-packet builder and the agent identity are MD-specific.
   Generalize storage `MDExpertReviewPacketRecord → ExpertReviewPacketRecord(lane_key, …)` (blob-
   backed, low-migration: add `lane_key` default `"md"`). `LaneGate` is **config-driven**:
   `{agent_name, checklist, bounds_required, input_review}` or `None` (ungated lanes like ADMET).
3. **`parse_result` returns a directional signal.** `{findings, limitations, source_refs,
   artifacts, signal: supports|neutral|refutes, confidence}`. This both enriches the capsule (the
   candidate gets *directional* evidence) and doubles as the pipeline's `continue_if` branch.
4. **LLM-only review is NOT a lane** (the abstraction's edge). A lane runs on an external compute
   provider behind a gate; pure-LLM validation is the existing agent dispatch. Don't generalize it
   into `LaneSpec`.
5. **Autonomous execution; the gate sits only at the WRITE.** The compute was never the write — the
   *promotion* is. So compute chains run autonomously; the inter-stage human gates become automated
   guardrails: a **budget ceiling per run**, **fan-out caps**, and the **`continue_if` thresholds**
   (which become load-bearing — they must kill dead chains by themselves). One human stamp at
   promotion. Make it **origin-policy-driven**: `gate_policy = trusted_operator` (your runs:
   autonomous → single end-review) vs `external_collaborator` (Phase 4: review stays at submission).
6. **Checkpointing has two shapes** (corrected for real hours-long GPU docking):
   - **Work-queue** (a screen of N independent dockings): persist each completed result + a cursor.
     Easy, clean, parallel, hands off trivially — maps onto pipeline fan-out.
   - **Single-long-run** (one exhaustive docking / an MD trajectory): engine restart file
     (OpenMM `.chk`) or process/GPU checkpoint (cuda-checkpoint/CRIUgpu/Cedana). Harder.
   Bake affordances now; full resume implementation is 3b.

---

## The provider pick (3b)

**First: Modal.** Fastest path to a real `ComputeRunner` — Python-native Sandbox API, genuine GPU,
durable filesystem/directory snapshots. **Caveat (verified):** GPU and live-memory-snapshot do NOT
compose on Modal — so implement checkpoints as **artifacts written to durable storage at stage
boundaries**, not live GPU pause. That's the work-queue shape, which is what screening wants anyway.

**Later, for true single-run GPU pause/resume: RunPod + Cedana.** Cheaper H100/A100 + Cedana
(cuda-checkpoint/CRIUgpu) adds genuine mid-job GPU checkpoint/resume/migration that Modal/RunPod
alone lack. Add this when a lane has long *non-decomposable* runs that must survive a pause.

**Self-host option (own the provenance): Daytona or Northflank** — defer unless owning the stack
matters more than speed-to-first-run.

Decision needed from you: commit Modal spend for the first real provider, or wait. 3a doesn't need it.

## The first 3 lanes (3b) — a provenance-friendly screening cascade

All MIT/Apache-2.0, all hit twog's targets (VEGFR2/KDR, KIT, PI3K/AKT/mTOR, β-adrenergic):

| order | lane | provider/profile | gate? | checkpoint shape | role |
|---|---|---|---|---|---|
| 1 | **ADMET-AI** | CPU, cheap | **none** (ungated) | n/a (fast) | early triage — tox/ADMET profile before burning GPU |
| 2 | **gnina docking** | GPU | light (cost/input) | **work-queue** (screen = N ligands + cursor) | hit ranking; the upgrade to the existing Vina lane |
| 3 | **Boltz-2 co-fold+affinity** | GPU | yes (cost/input) | single-run or batched | structure + affinity: "does this ligand engage this target?" |

ADMET is the **proof-of-pluggability lane** (ungated, CPU, no GPU/container complexity) — building it
end-to-end is the acceptance test that the lane pattern actually works. gnina + Boltz-2 are the real
science, gated by 3b infra readiness. Defer OpenFE/FEP (physics confirmation, heaviest) and
REINVENT4 (generative front-end) to later lanes.

---

## Build sequence (3a — strangler, each increment ships green)

1. **`LaneSpec` + registry.** `register_lane`/`get_lane`. Register **MD as the first instance**,
   whose `build_input_packet`/`gate`/`parse_result` call the *existing* `_md_*` functions. Generic
   path, identical behavior. The 33 gate tests stay green.
2. **Dispatch through the registry.** `submit_compute_job` resolves `get_lane(record.validation_type)`
   and runs the lane's gate instead of the hardcoded `if validation_type == "md"`. `_md_*` become
   the MD lane's internals; public names stay as thin adapters. (33 gate tests unchanged.)
3. **Generalize the gate storage.** `ExpertReviewPacketRecord(lane_key, input_packet: dict, …)` +
   `get_expert_review_packet_by_hash`; MD wraps it with `lane_key="md"`. Blob-backed, no migration.
4. **`parse_result` + the signal.** Add the `{…, signal, confidence}` return; wire it into the
   Phase-2 `build_proof_capsule_from_completed_compute_job` so capsules carry directional evidence,
   and into the promotion so candidate evidence gains direction.
5. **Autonomy policy.** `gate_policy` on the pipeline/flow; budget ceiling + fan-out cap fields;
   `continue_if` hook on `parse_result.signal/confidence`. (Operator-origin autonomous; external
   gated — sets up Phase 4.)
6. **Pipeline + checkpoint affordances** (fields only, not the orchestrator): `parent_compute_job_id`
   on `ComputeJobRecord`; `progress_fraction`, `checkpoint_artifact_id`, a `paused` status,
   `resume_from_checkpoint` input support; `leased_by`/`lease_expires_at` on the workspace;
   `supports_checkpointing` on `LaneSpec`; `checkpoint_uri` in the `ComputeRunner` contract.
7. **Second lane as the proof.** Register an ADMET-style lane (CPU, ungated) — even stubbed against
   the mock provider — and run it through the *same* loop end-to-end. **This is the acceptance test:
   if the second lane is hard, the abstraction is wrong.**

## Build sequence (3b — cloud, when ready)

8. **Register Modal** as a `ComputeRunner` (`register_compute_runner("modal", …)`), checkpoints as
   stage-boundary artifacts.
9. **Build the ADMET container/lane** for real (CPU on Modal) — cheapest real lane, proves the
   provider end-to-end.
10. **gnina + Boltz-2 lanes** (GPU containers) — the real science; gated; the screening cascade.
11. **RunPod + Cedana** only if/when a non-decomposable long run needs true GPU pause/resume.

## Explicitly deferred (not Phase 3)
- The `ComputePipeline` orchestrator (the resumable gated advancer) — affordances baked, orchestrator later.
- Full checkpoint/resume *implementation* — fields now, mechanism in 3b per provider.
- OpenFE/FEP and REINVENT4 lanes.

## Verification
- 3a: `pytest -n auto --cov-fail-under=75` green after each increment; the **second-lane e2e test**
  (step 7) is the pluggability proof, runnable with the mock provider — no GPU.
- 3b: each real lane gets a CI smoke test (like the old worker-image CI) + one real run reviewed.
- Nothing pushed; commits land on `chore/cleanup-pass`.

## Open decisions for you
1. **Provider spend** — commit Modal now, or stay mock-only until a lane is ready? (3a needs neither.)
2. **First real payload** — the bio dept's priority among ADMET/gnina/Boltz decides 3b order; the
   default cascade (ADMET → gnina → Boltz-2) is the recommendation absent input.
3. **Autonomy default** — confirm operator-origin runs go fully autonomous to a single end-review
   (vs. keeping an optional manual checkpoint you can toggle per run).
