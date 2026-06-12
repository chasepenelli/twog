# Changelog

## v2.2.0 — The autonomous discovery loop

TWOG now drives its own scientific crux. Given a candidate, the system proposes the next test most
likely to KILL the leading hypothesis, fetches its own inputs, runs real GPU compute, audits the
result, and decides what to do next — falsification-first, with the human write-gate strictly
terminal. Nothing it concludes is ever auto-promoted.

### The loop
- **Active Falsification Planner** — reads a candidate's signed evidence ledger and proposes the
  cheapest test that could refute the leading hypothesis, pre-registering an explicit kill-criterion.
- **Pre-registration lock** — the kill-criterion is content-hashed BEFORE any compute runs and rides
  unchanged onto the produced proof capsule (anti p-hacking).
- **Confound + Provenance pre-gates** — a `supports` capsule cannot be accepted until its known
  confounds each survive a control (the tumor-purity catch, made mandatory), and a capsule's claimed
  run must match its linked compute job ("claimed V100, ran H100"). Verdicts are never "true".
- **Multi-round controller** — chains rounds until the kill-criterion is met, the test space is
  exhausted, or the budget / round cap is hit; a refuting outcome terminates WITHOUT promotion.
- **Failure Corpus** — accumulating, queryable negative knowledge; the planner deprioritizes
  already-settled approaches.
- **Input-aware + self-supplying** — the planner prefers lanes it can actually run, and resolves real
  inputs from a candidate's named target + therapy (PubChem SMILES + RCSB structure) — no curation.
- **Scheduler** — a STOPPED-by-default Dagster schedule ticks the loop for validation-ready candidates
  between operator sessions, budget-capped, never promoting.

### Proven on real science
Pointed at the alpelisib → PI3Kα hypothesis (named target + therapy only), the loop autonomously
fetched the PI3Kα structure + alpelisib SMILES, dispatched real gnina docking on a Modal A100, and
found strong engagement (−9.8 kcal/mol, CNN pose 0.99) → the hypothesis SURVIVED its own falsification
attempt, and nothing was promoted. Real Modal cost ~$0.10/round.

### Also
- Fixed the adapter→Modal path (the lane functions dragged the heavy package onto the slim GPU images);
  `runner_kind="modal"` now runs real compute end-to-end.
- Calibrated per-lane cost from a real Modal baseline (omics CPU ~$0.0001, docking A100 ~$0.07,
  cofolding A100 ~$0.23).
- 640 tests; the loop runs entirely on the in-process mock provider in CI (no GPU, no network).

## v2.1.0 — Reproducible GPU compute, a validated in-silico chain, and a rigor spine

This release matures TWOG v2 from "structured proof of concept" into a system that carries a
hypothesis from messy evidence through **reproducible, GPU-accelerated structural validation** — and
that is built to falsify itself before it can mislead. Everything here is in-silico and
hypothesis-generating; the bar is integrity and reproducibility, not a validated treatment.

### Compute — the layer that grew up
- **Provider-agnostic compute seam.** Removed the original non-functional hosted-GPU path; a `ComputeRunner`
  protocol + lane registry means a lane (docking / co-folding / MD / omics) is a pluggable unit that
  can target any GPU backend without touching the science code. Currently runs on Modal (A100/T4).
- **Real GPU lanes, verified end-to-end:** gnina CNN docking, Boltz-2 co-folding (structure +
  affinity), OpenMM molecular dynamics, and a CPU omics/TME lane.
- **Reproducible environments** — exact pinned stacks captured at build (`docs/ENVIRONMENT_LOCK.md`).
- **Checkpoint / resume to durable storage** — a long GPU run can pause at 40% and be picked back up
  by the same or a different operator (lease/handoff model).

### Science — one honest, end-to-end run (canine HSA ↔ human angiosarcoma)
- **Falsification-first crux:** the initial "PIK3CA-mutant tumors are immunosuppressed" hypothesis
  was *refuted* — the signal was a tumor-purity artifact, caught by running orthogonal controls.
- **Reframed + replicated cross-species:** a PI3Kα / endothelial-vascular axis, replicated in a
  98-sample human angiosarcoma cohort (p = 0.001).
- **Alpelisib translational thesis, in-silico chain:** docking redock RMSD **1.8 Å** → Boltz-2
  co-fold iptm **0.99**, predicted **~11 nM** vs measured 4.6 nM → PI3Kα pocket **100% conserved**
  dog↔human → ~350k-atom solvated **MD-stable** (ligand RMSD **1.25 Å**).

### Rigor spine — "not a black box"
- **Proof capsules:** portable, **Ed25519-signed**, content-hashed units of evidence (what ran,
  against what, the directional signal, the limitations) with **tamper-evident lineage** (hash-linked
  Merkle DAG). Proves *integrity*, not *validity* — by design.
- **Operator write-gate** + **trusted-collaborator access layer** (principals, scoped roles,
  workspace leasing) — agents propose; promotion to a claim is gated.

### Open contributions (the flywheel)
- Harmonized **cross-species cohort** (canine HSA ↔ human AS, genotype × expression).
- **Canine TME signature registry** (16 panels, 85 markers, both genome assemblies).
- **v0 cell-type deconvolver** (immune lineages; validated on a real canine atlas reference).

### Architecture + design
- Pluggable **lanes** + **sandbox provisioning** (open-a-sandbox-and-it's-ready manifests).
- `docs/AGENT_PRIMITIVES.md` — sourced research + proposed next-generation agent primitives.
- Public **site + dashboard** (`web/`) with a one-click GitHub Pages deploy.

### Notes
- 580 tests passing; ~77% coverage.
- License: Apache-2.0.

## v2.0 — Structured rebuild (prior)
Deterministic ingestion, typed research records, citation-tracked synthesis, public candidate
records, method versioning, compute ledgers, and human approval gates.
