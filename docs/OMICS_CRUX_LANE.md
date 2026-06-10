# Omics-review crux lane — spec

> The first *real* non-docking/non-MD compute lane. It answers the developed hypothesis's
> cheapest-first crux and doubles as the **pluggability proof** for Phase 3: if the `LaneSpec`
> abstraction (built for GPU docking/MD) also cleanly expresses a CPU omics-stats analysis, the
> abstraction is validated. Data access is in `DATA_SOURCES.md`; lane architecture in `PHASE3_PLAN.md`.

## What it answers

**Crux:** does the **PIK3CA-mutant** subset of canine splenic HSA carry an **immunosuppressive
TME signature** (Tregs / M2-TAMs / immunosuppressive cytokines)? If yes → the v2 hypothesis's
local-IL-12-relief rationale has a target. If no → the IL-12 arm loses its premise (a real,
valuable *refute* outcome).

## The computation (6 steps)

1. **Load** canine HSA bulk RNA-seq — Megquier 2019 (`PRJNA562916`, n≈23) — with matched
   per-sample mutation calls (WES `PRJNA552034`; calls in paper supplements).
2. **Stratify** samples: PIK3CA-mutant vs WT (secondary: TP53 strata). ⚠ ~30% mutant of n≈23 ⇒
   ~7 mutant — **underpowered**; this must flow into the confidence, not be hidden.
3. **Score immune composition** per sample using the **Ammons canine leukocyte atlas**
   (`GSE225599`) cell-type marker signatures. **Method (v1): open marker-gene set scoring
   (ssGSEA-style)** — no licensing, interpretable. *Gold-standard upgrade:* CIBERSORTx
   (web-portal/licensed — flag the access caveat; not freely redistributable).
4. **Quantify the immunosuppression signature:** Treg fraction, M2-TAM/macrophage fraction,
   immunosuppressive cytokines (IL10, TGFB1), exhaustion markers, and the **Banerjee 2024
   PIK3CA→immune program** (IL6/CXCL8/CCL2). (Banerjee, PMID 39709507, is the mechanistic anchor.)
5. **Test** differential composition: mutant vs WT (Mann-Whitney on fractions/scores; effect size +
   p; BH multiple-testing correction). **Replicate direction** in Wang (`PRJNA526923`) if loaded.
6. **Emit a directional signal** (see below) + artifacts.

## Inputs — `OmicsReviewInputPacket` (the lane's typed packet)

```python
class OmicsReviewInputPacket(LaneInputPacket):
    datasets: list[str]            # ["PRJNA562916", "PRJNA552034"]  (RNA-seq + WES strata)
    replication_datasets: list[str] = []   # ["PRJNA526923"]
    reference_atlas: str           # "GSE225599" (Ammons canine leukocyte atlas)
    stratify_by: str               # "PIK3CA_mutation_status"
    signature: dict[str, list[str]]  # {"Treg": [...markers], "M2_TAM": [...], "immunosupp_cytokines": [...]}
    method: Literal["ssgsea_markers", "cibersortx"] = "ssgsea_markers"
    test: Literal["mannwhitney"] = "mannwhitney"
    direction_hypothesis: str = "immunosuppression_higher_in_mutant"  # PRE-REGISTERED
```

The signature/method/test/direction are **pinned up front** — a pre-registered analysis. This kills
the "garden of forking paths" (analyst degrees of freedom) that makes omics claims unreliable.

## The gate — input-review (demonstrates the gate generalizing)

The MD gate means *safety+cost*; here the gate means **"is the analysis pre-specified correctly?"** —
an `input_review` gate, **`bounds_required=False`** (it's cheap CPU, no GPU budget to guard).

```python
gate = LaneGate(
    agent_name="omics_review_agent",
    checklist=[
        "signature genes are valid canine symbols present in the reference atlas",
        "strata are defined from the WES calls, not derived post-hoc",
        "direction_hypothesis + test are pre-registered (not chosen after seeing data)",
        "sample n per stratum is reported and the power limitation acknowledged",
    ],
    bounds_required=False,
    input_review=True,
)
```

This is the payoff for the pressure-test: the *same* generic gate machinery enforces "exact
input-packet hash + approval," while the lane supplies a checklist about **scientific validity**
instead of GPU cost. The crown jewel generalizes with zero weakening.

## The signal — `LaneResult` (feeds the Phase-2 capsule)

```python
LaneResult(
    findings=("In Megquier HSA (n=23; mutant n=7), PIK3CA-mutant tumors showed {higher|no} "
              "Treg+M2-TAM enrichment (effect={d}, p={p}); direction {replicated|not} in Wang."),
    signal="supports" | "neutral" | "refutes",   # vs the immunosuppression hypothesis
    confidence=...,        # down-weighted by small n, no-replication, deconvolution uncertainty
    artifacts=[deconvolution_table, differential_composition_plot, per_sample_composition_matrix],
    source_refs=["PRJNA562916", "GSE225599", "PRJNA526923", "PMID:39709507"],
    limitations=[
        "golden-retriever-enriched cohort (frequency/bias caveat)",
        "underpowered: ~7 PIK3CA-mutant RNA-seq samples",
        "bulk deconvolution has compositional uncertainty",
        "RNA-seq:WES sample matching must be verified in the data",
    ],
)
```

**Signal logic** (pre-registered): mutant-higher + significant + replicated → `supports`; mutant-higher
but underpowered/unreplicated → `neutral` (low confidence); no difference or opposite → `refutes`.
The directional signal is exactly the enrichment the lane pressure-test demanded — and here it maps
straight onto "does the IL-12-relief arm have a target?"

## How it slots into Phase 3 and Phase 2

- **Phase 3 (the pluggability proof):** register it as `LaneSpec(lane_key="omics_review",
  validation_type="omics", input_packet_type=OmicsReviewInputPacket, gate=<input_review>,
  parse_result=<above>, default_compute_profile="cpu")`. **CPU, no GPU, no Modal** → buildable in
  **3a** and runnable on the **mock provider**. It's the second lane (after MD) and the first
  non-3D-compute lane — if it registers and runs through the *same* dispatch as MD, the abstraction
  holds.
- **Phase 2 (the loop):** omics lane → `compute_artifact` proof capsule → operator reviews →
  `promote_proof_capsule_to_candidate` → the v2 therapy candidate gains **directional evidence**
  ("PIK3CA-mutant HSA is/ isn't immunosuppressive"), and its validation-ready gate re-assesses. The
  crux experiment literally becomes a reviewed capsule on the candidate.

## 3a (now, mock-testable) vs 3b (real bioinformatics)

- **3a — design + stub (buildable today):** the `OmicsReviewInputPacket`, the `LaneGate`, the
  `LaneResult` shape, the `LaneSpec` registration, and a **stub `compute` that returns a seeded
  result** through the mock provider. Proves the lane plugs into dispatch + capsule + promotion,
  green-tested. No data download.
- **3b — real implementation:** the actual data pull (`pysradb`/`GEOparse` over PRJNA562916/GSE225599),
  the marker-gene scoring (scanpy/ssGSEA), the Mann-Whitney + BH, the plot/table artifacts, and the
  `omics_review_agent` to run the input-review gate. This is real (open, mostly free) bioinformatics —
  the first lane that produces a *true scientific result*, on a server (CPU is fine; no GPU).

## Honest caveats (baked into the lane, not hidden)
- **Underpowered** (~7 mutant RNA-seq) — the headline limitation; confidence must reflect it, and a
  `neutral` outcome is the most likely honest result from Megquier alone. Replication (Wang) and the
  human cross-check (Angiosarcoma Project) are how confidence grows.
- **Deconvolution method** — ssGSEA marker scoring is open and defensible but coarser than CIBERSORTx
  (which is access-gated). Report the method as part of provenance.
- **RNA-seq↔WES matching** in Megquier must be verified before trusting per-sample strata.
- This lane tests **one** crux (the IL-12-relief premise). The mTOR-engagement crux is the *docking*
  lane; the synergy/antagonism crux is in-vitro (out of scope for compute).
