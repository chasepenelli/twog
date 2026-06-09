# TWOG — Roadmap to External Heavy-Compute Collaboration

> **The destination.** Trusted external collaborators (NVIDIA bio dept, on free GPU compute)
> check out a research candidate, run sandboxed heavy-compute tasks, and submit **proof
> capsules** that — after operator review — promote evidence on a candidate. TWOG stays the
> approval gate; collaborators add horsepower, not unreviewed writes.
>
> Companion to `CURRENT_STATE.md` (what exists today). This file is the sequenced plan to get
> from here to there. Written 2026-06-09.

---

## Two decisions that shape this plan (locked)

1. **Access = trusted-collaborator, not multi-tenancy.** A known, trusted group of scientists.
   Per-collaborator identity + scoped checkout + capsule verification riding the *existing*
   checkout→capsule→operator-approval design. Full SaaS multi-tenancy (orgs, quotas, billing,
   self-serve) is deferred to Phase 5 and built only if demand proves out.
2. **Compute payload = pluggable lanes, not one bet.** We don't yet know whether the bio dept
   most wants real MD (OpenMM), docking-at-scale, or Boltz/cofolding. Phase 3 builds the lane
   *pattern* (one expert-gated lane already works) so any payload plugs in behind the same gate.

---

## Why this order (the dependency spine)

```
P0 cleanup ─▶ P1 data intake ─▶ P2 close the loop (solo) ─▶ P4 open to collaborators
                                          │
                                          └▶ P3 graduate compute (parallel) ─┘
                                                                              ▼
                                                                     P5 scale (later)
```

You cannot safely open to collaborators (P4) until the loop closes end-to-end (P2), and the
loop is only worth running if there's real candidate data (P1) and a repo that's safe to change
(P0). Compute graduation (P3) is the scientific value that makes collaboration worth a
scientist's time — it runs parallel to P2 because both sit behind the same expert gate.

**Guardrail carried through every phase:** *LLMs and collaborators argue and produce; operator
approval is the write gate.* No phase weakens it. P4 specifically must not.

---

## Phase 0 — Finish the cleanup, lock the spine

**Goal:** the repo is safe to change and costs ~$0 idle. (Prerequisite you named.)

**Deliverables**
- Commit `CURRENT_STATE.md` + this `ROADMAP.md` to `origin/main`.
- Back up the unpushed June-4 frontend work (`~/Documents/Codex/.../twog-system-tightening`).
- Confirm Dagster+ schedules are the ones you want enabled and Neon usage cost is as expected
  (CURRENT_STATE §7). Both are kept — they run the ingestion heartbeat; the goal is "right
  cadence at a modest, known cost," not zero.
- Split the 502-test megatest into per-area files (CURRENT_STATE §6) — needs a working venv to
  verify green before/after.
- Begin `service.py` decoupling **step 1 only**: map its public surface and callers
  (CURRENT_STATE §5). Do not move files yet.

**Exit criteria:** docs committed; idle cost ≈ $0 confirmed on dashboards; test suite splittable
and green; `service.py` surface mapped.

---

## Phase 1 — Get data intake "in the right place"

**Goal:** a trustworthy, well-shaped candidate is the unit of work a collaborator checks out.

**What exists:** ingest → typed records → claims → public candidates already runs; the
checkout manifest already bundles method refs, open questions, evidence-bundle hash.

**Deliverables**
- Define and implement a **"validation-ready candidate" state** — the explicit gate a candidate
  must pass before it's eligible for external checkout (evidence bundle complete, claims typed,
  open questions enumerated, snapshot hashed). This is the contract collaborators rely on.
- Ensure the **candidate snapshot hash** is stable and reproducible (it's already referenced by
  proof_capsules.py and checkout manifests — make it load-bearing).
- Make the **evidence index** the thing a checkout manifest points at, so a collaborator sees
  exactly what's known and what's open.

**Exit criteria:** you can take a real HSA candidate, mark it validation-ready, and produce a
checkout manifest a stranger could understand without a call.

---

## Phase 2 — Close the loop end-to-end (you as the only user)

**Goal:** prove compute result → proof capsule → operator review → candidate promotion works
fully, with zero multi-tenancy. This is the highest-leverage missing piece.

**The three gaps to fill (CURRENT_STATE / research findings):**
1. **Compute result → auto proof-capsule.** When a RunPod job reaches `completed`, parse its
   `output_payload` + artifact hashes and auto-construct a `compute_artifact` proof capsule with
   `requested_action = docking_or_md_review`. (Today the output just sits in the payload.)
2. **Capsule status advancement.** Implement the operator-gated transitions that already exist
   as enums but not as workflow: `submitted → accepted_for_compute_review → accepted_for_validation_queue
   → (promotion)`. Each transition is an explicit operator action, logged.
3. **Capsule → candidate promotion.** On operator approval, transclude capsule artifacts +
   source_refs into the candidate's evidence and advance the candidate. The promotion is the
   only step that mutates a candidate, and it requires the human stamp.

**Also:** wire `validation_queue (approved "md"/"docking" item) → create compute job → submit`
as one tracked flow (today it's manual workflow_dispatch steps).

**Exit criteria:** you click "run validation" on a validation-ready candidate, a GPU job runs
behind the expert gate, its result becomes a proof capsule, you review and approve it, and the
candidate's evidence updates — all logged in the ledger, no manual JSON shuffling.

---

## Phase 3 — Graduate the compute (smoke → science) — parallel with P2

**Goal:** real scientific payloads behind the existing expert gate. Build the **lane pattern**,
then turn on the first real lane the bio dept asks for.

**What exists (2026-06-09):** the RunPod execution provider + smoke worker were **removed**
(never worked). What remains is the provider *seam* (`compute_runners.py`: a `ComputeRunner`
protocol + `register_compute_runner()` registry) and the provider-agnostic machinery around it —
the expert gate, validation queue, proof-capsule model, and compute-job ledger, all green-tested.
No provider is registered, so `get_compute_runner()` blocks submission safely.

**Deliverables**
- **First: register a working provider** behind the seam — a rebuilt worker or a different tool
  (Modal / E2B / NVIDIA GPU env). Implement `ComputeRunner.submit/poll/cancel` and
  `register_compute_runner("<kind>", factory)`. The gate already runs before the provider.
- **Generalize the lane** into a pluggable contract: `{lane_key, input_packet_schema,
  expert_gate, expected_outputs, result_parser}`. Each new lane reuses the MD gate machinery, a
  `result_parser` feeding Phase-2's auto-capsule step, and a CI smoke test.
- **Candidate first payloads (decide with bio dept):** OpenMM MD · docking-at-scale ·
  Boltz/cofolding. Build the *pattern* now; commit to a payload when they tell us.
- **NVIDIA compute:** their GPU env is one provider implementation of the seam.

**Exit criteria:** a second compute lane runs end-to-end behind its own expert gate and produces
a proof capsule via the same Phase-2 path — proving the pattern, not just one worker.

---

## Phase 4 — Open the door to trusted collaborators

**Goal:** NVIDIA scientists can check out candidates, run lanes on their compute, and submit
capsules — within scoped, revocable, logged access. You remain the promotion gate.

**Deliverables (deliberately lightweight)**
- **Per-collaborator identity:** an API key / signed token per collaborator (not full OAuth).
  Attach `collaborator_id` to workspaces, checkouts, capsules, compute jobs.
- **Scoped checkout:** a collaborator sees only workspaces checked out to them; capsule
  submission verifies `collaborator_id` + manifest hash + candidate snapshot hash (most of this
  validation already exists in `proof_capsules.py` — add identity to it).
- **Submitter verification:** sign the capsule producer identity so "who submitted this" is
  provable, not plaintext.
- **Revocation + lease:** explicit "checkout complete / access withdrawn" that revokes the Neon
  branch and blocks further capsule check-ins (today it's TTL-only).
- **A thin collaborator surface:** "here are candidates open for validation," "here's your run
  history," "here's what your last capsule needs." Minimal — not a dashboard product.
- **Expert gate on ALL capsule check-ins**, not just MD compute — an external capsule never
  promotes without operator review.

**Explicitly NOT in P4:** org hierarchies, billing, self-serve signup, per-user quota
enforcement. Those are P5, built only if needed.

**Exit criteria:** a real NVIDIA collaborator, with their own key, checks out a real candidate,
runs a real compute lane on their GPUs, submits a proof capsule, and you approve it into the
candidate — without them ever touching production credentials or another collaborator's work.

---

## Phase 5 — Scale (only if demand proves it)

Real multi-tenancy (orgs, isolation), per-collaborator GPU quotas + cost tracking, result
caching/memoization, a proper collaborator dashboard, and queue maintenance at volume. Triggered
by evidence that the trusted-collaborator model is oversubscribed — not by anticipation.

---

## Cross-cutting principles (true in every phase)

- **The expert gate is sacred.** Every compute submit and every capsule promotion passes a
  recorded human (or gated agent) approval tied to an exact hash. This is the moat; never trade
  it for convenience.
- **Public-safe by construction.** Workspaces and manifests never carry production credentials
  (already enforced); keep it that way as collaborators arrive.
- **Append-only provenance.** Every run, capsule, approval, and promotion is a ledger row. The
  audit trail is the product (the README's own thesis).
- **Survivable across absences.** Idle ≈ $0; one-evening re-entry; compute as expeditions. A
  collaboration that bills while you're heads-down on Bridge is a failed design.

---

## Immediate next actions

1. Commit `CURRENT_STATE.md` + `ROADMAP.md` to `origin/main`.
2. Finish Phase 0 (back up frontend work; confirm idle cost ≈ $0; split tests; map `service.py`).
3. Start Phase 1: define the "validation-ready candidate" state.
4. Share this roadmap with the NVIDIA bio contacts to pull the Phase-3 payload decision forward.
