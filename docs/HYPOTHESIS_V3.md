# Hypothesis v3 — reframed from the real omics crux (PIK3CA in canine HSA)

**Status:** hypothesis-generating, n=14 public cohort. Supersedes the v2 immunosuppression framing.
**Date:** 2026-06-10. **Data:** Megquier 2019 genotypes (PMC7067513 Tables S4+S5) × GSE95183 FPKM.
All analysis local + free; reproducible via `scripts/build_megquier_cohort.py` + the omics engine.

## What we tested, and what the data said

A pre-registered directional crux (5 PIK3CA-mutant vs 9 WT canine HSA, matched WES↔RNA-seq), then
two follow-up reframes. **All three immune framings failed:**

| Framing tested | Prediction | Result (AUC = P(mutant > WT)) | Verdict |
|---|---|---|---|
| **v2: immunosuppressed** | Treg/M2/cytokines ↑ in mutant | all AUC 0.27–0.44 (↓) | **not supported** |
| **reframe A: "hotter"** | effector/IFN-γ ↑ in mutant | effector 0.33, IFN-γ 0.24, Th1 0.38 (all ↓) | **not supported** |
| **reframe B: immune-cold/excluded** | immune ↓ *specifically* | immune ↓ (0.31–0.38) **but stroma ↓ too (0.42), tumor/endothelial ↑ (0.71)** | **confounded by purity** |

The decisive observation: immune **and** stroma both fall in mutant while the **endothelial/tumor**
compartment (PECAM1/VWF/CDH5/KDR, AUC 0.71) **rises**. Since HSA is an endothelial malignancy,
endothelial markers proxy tumor content. The most parsimonious explanation for the global immune
downshift is **higher tumor purity in PIK3CA-mutant samples** (less immune+stromal dilution) — not a
specific immune-exclusion biology. **No clean immune phenotype can be attributed to PIK3CA here
without purity adjustment.**

## The defensible reframe (v3)

> **PIK3CA mutation in canine HSA associates with a more tumor-cell-dense, endothelially-driven
> tumor (↑ endothelial/KDR-VEGFR2 content), consistent with PI3K-AKT amplification of the
> angiogenic program in a vascular malignancy — rather than with any specific immune phenotype.**

Why this is grounded, not a story:
- PI3K-AKT is a canonical driver of endothelial proliferation and VEGF/VEGFR2 (KDR) signaling; HSA
  is a tumor *of* endothelium. ↑ endothelial-tumor content + ↑ KDR in the mutant stratum is
  mechanistically coherent.
- It connects directly to work already validated in this repo: the gnina GPU docking against
  **VEGFR2/KDR (PDB 3VO3)** — the same axis this reframe implicates.
- **Therapeutic implication flips away from immunotherapy:** the rational angle for PIK3CA-mutant
  HSA is **PI3K/mTOR + VEGFR2 co-targeting** (anti-angiogenic + pathway), not checkpoint/IL-12.
  (Local IL-12 is *not* discarded — it's simply no longer justified by *this* cohort's immune data.)

## Purity-adjusted crux — RESULT (2026-06-10, free)

Ran the decisive test below. Per-sample ESTIMATE RNA-seq purity parsed from Table S5; immune
signatures residualized on purity two independent ways (paper's ESTIMATE score, n=11; and the
endothelial-tumor-content proxy, n=14).

- **Mutant is more tumor-pure** (ESTIMATE 0.63 vs WT 0.57, AUC 0.70, p=0.27) — confound is real.
- **The broad immune downshift is a purity artifact** — it collapses to neutral after adjustment:

  | signature | raw AUC | ESTIMATE-adj | endo-adj |
  |---|---|---|---|
  | Immune (broad) | 0.30 | 0.60 | 0.51 |
  | M2-TAM | 0.33 | 0.57 | 0.40 |
  | Cytotoxic effector | 0.33 | 0.57 | 0.56 |
  | **IFN-γ (IFNG/CXCL10)** | 0.27 | **0.30** | **0.27** |

- **One purity-independent residual: IFN-γ response stays down in mutant** (p≈0.13–0.16 both ways).
  Mechanistically plausible (PI3K-AKT dampens IFN/STAT1 in human cancers). A *lead*, not a finding:
  thin 2-gene panel, n=11–14, not significant.

**Net:** broad immune-phenotype claims are dead (artifact). v3's tumor-density/endothelial core
stands (it explains the purity gap). The live, narrowed immune lead is **PI3K → reduced IFN-γ
response** — to be confirmed, not asserted.

## What's next (cheap, free, before any spend)

1. **Firm up the IFN-γ lead.** Re-test with the full IFN-γ hallmark (add STAT1, IRF1, GBP1, CXCL9/11,
   HLA/DLA-class-I) instead of 2 genes; still purity-adjusted. Does the residual hold or evaporate?
2. **Confirm the endothelial/proliferation signal** with a proliferation panel (MKI67, PCNA, TOP2A)
   and the angiogenic program (VEGFA, KDR, ANGPT2, DLL4) — does PIK3CA-mutant track proliferation?
3. **Independent replication:** human angiosarcoma PI3K-pathway ↔ angiogenesis/immune via
   gget/OpenTargets/COSMIC/cellxgene (free) — is the endothelial-density association cross-species?
4. **Power:** the salmon-on-Modal pipeline adds 5 matched raw samples (→ ~7 vs 12) — *spend-gated*,
   and now better motivated to confirm the purity/endothelial finding, not the dead immune one.

## Human cross-check (gget / Open Targets, free) — corroborates the endothelial axis

PIK3CA's top human disease associations (Open Targets) are dominated by **vascular/endothelial
overgrowth syndromes** caused by somatic PIK3CA mutations: megalencephaly-capillary malformation
(0.85), CLOVE/CLOVES (0.79), PIK3CA-related overgrowth spectrum / PROS (0.77), CLAPO (0.72) — above
the classic carcinomas (breast 0.75, ovarian 0.73). Independent, cross-species support that PIK3CA
drives **endothelial/vascular proliferation** — the same axis as the one robust canine signal.
Therapeutic: **alpelisib (PI3Kα inhibitor)** is approved for PIK3CA-mut breast cancer and used for
PIK3CA-driven vascular malformations (PROS) — a repurposable agent for this exact axis.

## Cross-species replication — human angiosarcoma (Angiosarcoma Project, n=98, free)

Ran the same PIK3CA-stratified crux on the Angiosarcoma Project "Count Me In" cohort (cBioPortal
`angs_painter_2025`: TPM expression + WES mutations), 5 PIK3CA-mut vs 93 WT.

- **Endothelial content: AUC 0.96, p=0.001** — PIK3CA-mut human AS are dramatically more
  endothelial/vascular-rich. Same direction as canine (0.71), far stronger, significant, survives
  multiple-testing correction. **The endothelial axis replicates across species.**
- Angiogenesis 0.85 (p=0.008) but endo-adj 0.51 → = endothelial content, not independent (as in dog).
- Immune lower in mutant even after endo-adjustment (broad 0.09 p=0.002; cytotoxic 0.20 p=0.024) —
  stronger immune-cold residual than canine, but n=5/VUS-fragile.

**Two sharp caveats:** (1) human PIK3CA mutations here are **non-hotspot VUS** (T957P, M1043V/I,
M1004V, R88Q — not E545K/H1047R), and PIK3CA is **rare in human AS (~4%) vs canine HSA (~30%)**.
(2) n=5 mutant. So the endothelial association is **real and replicated**, but its **cause is
unproven** — it may mark a well-differentiated/vascular AS subtype rather than PI3K oncogenic
signaling. Establishing PI3K causality requires functional work (→ alpelisib/PI3Kα docking + MD,
in-vitro), not more association mining.

## Converged thesis (what survived everything)

> PIK3CA-mutant canine HSA sits on the **PI3Kα-driven endothelial/vascular-overgrowth axis** seen in
> human PROS/CLOVES — not on an immune axis. The rational, translationally-grounded intervention is
> **PI3Kα inhibition (alpelisib) ± VEGFR2 targeting** (ties to the gnina 3VO3 docking), NOT
> checkpoint/IL-12 immunotherapy. Testable functionally (docking/MD/in-vitro), and alpelisib is an
> already-approved, repurposable probe.

## Functional probe — alpelisib docking (gnina, Modal A100, 2026-06-10; parser-corrected)

Redocked alpelisib (native ligand 1LT) into PI3Kα/4JPS and, as a specificity control, into
VEGFR2/3VO3 (modal_app.py::dock_pi3k). NB: a first run was corrupted by a docking.py parser bug
(gnina's 5-column output incl. an `intramol` column was misread, and poses were ranked by Vina
affinity instead of gnina's CNN score). Fixed + tested (tests/test_docking.py); corrected results:

| target | recommended pose | best Vina | CNN pose | CNN aff (pK) | pose RMSD | signal |
|---|---|---|---|---|---|---|
| **PI3Kα/4JPS** (on-target) | **−8.92** | −8.95 | **0.986** | 8.12 | **1.80 Å** | supports |
| VEGFR2/3VO3 (off-target) | −4.86 | −8.03 | 0.84 | 7.92 | — | refutes |

- **PI3Kα: validated, unambiguous.** All scores agree (strong affinity, CNN pose 0.986, CNN
  affinity pK 8.1 ≈ nM) AND the docked pose reproduces the crystal at **RMSD 1.80 Å (<2 Å = a
  successful native redock)**. alpelisib genuinely engages the PI3Kα pocket.
- **Selectivity emerges only under CNN ranking.** Raw Vina affinity is ~−8 for BOTH targets (a
  promiscuous discriminator). But gnina's CNN-recommended VEGFR2 pose is weak (−4.86) → the
  corrected analysis favors PI3Kα selectivity, which the buggy Vina-only read had hidden.
- **Honest caveat:** the VEGFR2 result has internal score disagreement (a strong Vina pose exists at
  −8.0 even though the CNN-favored pose is weak) → suggestive of selectivity, not a clean refute.
- **To strengthen (small Modal runs):** MD/free-energy on the PI3Kα complex; dock a panel of
  PI3Kα-selective vs pan-kinase inhibitors to calibrate the CNN-selectivity readout.

## Honest limitations

n=5 vs 9; bulk RNA-seq with coarse mean-z scoring; nothing significant at q<0.05 (all signals are
trends); variable purity and some low-VAF PIK3CA calls (0.06–0.25 VAF, Table S5); possible WES↔RNA-seq
tumor-site mismatch for ≤2 dogs. v3 is a direction to test, not a conclusion.
