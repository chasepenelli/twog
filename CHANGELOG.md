# Changelog

## v2.1.0 — Reproducible GPU compute, a validated in-silico chain, and a rigor spine

This release matures TWOG v2 from "structured proof of concept" into a system that carries a
hypothesis from messy evidence through **reproducible, GPU-accelerated structural validation** — and
that is built to falsify itself before it can mislead. Everything here is in-silico and
hypothesis-generating; the bar is integrity and reproducibility, not a validated treatment.

### Compute — the layer that grew up
- **Provider-agnostic compute seam.** Removed the non-functional RunPod path; a `ComputeRunner`
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
