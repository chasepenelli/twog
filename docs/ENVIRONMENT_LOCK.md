# Environment lock — pinned inventory for the structure/MD/deconvolution work

Phase 0 of the four-item pass. Every external dependency: source, version, license, fetch method.
No hand-waving. Lead with the coverage gaps. Reference upstream data; do not re-host.

## Coverage gaps & limitations (read first)
- **No canine hemangiosarcoma scRNA atlas exists** (confirmed scan, June 2026) — only bulk HSA
  expression profiling + a healthy canine lung scRNA atlas. So a real HSA *tumor-purity* model has
  no off-the-shelf reference. See "Single-cell reference" below for the honest path.
- **HSA tumor cells ARE endothelial** — so any endothelial reference conflates HSA-tumor with normal
  endothelium. The published HSA-vs-normal-endothelium bulk signature (Tamburini/Modiano) is the
  only separator we have, and it's bulk. v0 deconvolution is therefore **immune-only**.
- **4JPS has 75 missing residues (chain A) in 6 loops** — but **0 are within 15 Å of the alpelisib
  pocket** (nearest 17.5 Å). So binding-site MD is justified with capping, NOT loop modeling. This
  is a pocket-local result, not a basis for global/allosteric claims.
- **Boltz checkpoint download has a known corruption issue** (jwohlwend/boltz #664) — delete the
  cache Volume to re-fetch if weights load fails.

## 1. Boltz-2 (cofolding lane)
- **Source:** `jwohlwend/boltz` (PyPI `boltz`). **Version: pinned `boltz==2.2.1`.**
- **License: MIT** — free for academic *and* commercial use.
- **Weights:** `boltz2_conf.ckpt` (structure) + `boltz2_aff.ckpt` (affinity) **auto-download on first
  run** to `~/.boltz` or `--cache <dir>`. We point `--cache /cache` at a persistent Modal Volume so
  the multi-GB download happens once.
- **Affinity schema `cofolding.py` parses:** `confidence_*.json` → `confidence_score`
  (= 0.8·complex_plddt + 0.2·iptm), `iptm`, `ptm`, `ligand_iptm`, `complex_plddt`; `affinity_*.json`
  → `affinity_pred_value` (log₁₀(IC50 µM), lower = stronger) + `affinity_probability_binary`
  (P(binder)). Convert affinity→kcal/mol: `(6 − affinity_pred_value)·1.364`.
- **Scaffold risk to verify in the smoke:** output dir layout (`out_dir/predictions/<input>/` vs a
  `boltz_results_*` wrapper) and whether the affinity JSON name uses `_` or `-`. The parse logic is
  correct; the globbing is what the smoke confirms.

## 2. MD toolchain (real protein-ligand MD) — **CONFIRMED at build (2026-06)**
Co-resolution verified by an actual micromamba image build (Modal). **Resolved + pinned:**
- `openmm=8.5.2` — engine (resolved from unpinned; ≥8.5.1 as openmmforcefields requires).
- `openmmforcefields=0.16.0` — SMIRNOFF/AMBER small-molecule + biopolymer FF management.
- `openff-toolkit` — SMIRNOFF ligand parametrization (RDKit backend).
- `pdbfixer` — add missing atoms, protonate, solvate.
- **`cuda-version` is NOT pinned.** The strict `cuda-version=12.4` pin was the conflict (it has no
  openmm 8.5.x build) — *that was the Phase-0 open question, and the build answered it.* The CUDA
  build is only needed for the **3b GPU run** and is resolved at that image's build (a CPU prep image
  resolves the CPU platform fine for 3a).

## 3. Loop modeling decision (the 148 missing residues; 75 on chain A)
**Decision: binding-site-restrained capping. No loop modeling.** Justified by the geometry below —
every gap is ≥17.5 Å from the ligand. PDBFixer adds the 53 missing *atoms* + hydrogens (pH 7);
chain breaks at the large far gaps are capped (ACE/NME) with positional restraints on flanking Cα.
**Tradeoff:** fast and valid for *pocket* dynamics; **invalid for global/allosteric claims.**

### Modeled-vs-capped inventory (chain A, alpelisib centroid −1.3,−9.5,16.9)
| gap (residues) | length | min flank-Cα dist to ligand | handling |
|---|---|---|---|
| 1 | 1 | 35.4 Å | cap (N-term) |
| 228–243 | 16 | 43.9 Å | cap + restrain |
| 314–323 | 10 | 53.7 Å | cap + restrain |
| 498–524 | 27 | 58.8 Å | cap + restrain (largest gap, far) |
| 864–871 | 8 | **17.5 Å** | cap + restrain (closest; still outside pocket) |
| 1062–1074 | 13 | 42.8 Å | cap (C-term) |
**0 / 6 gaps within 15 Å of the pocket.** Alternative if global claims are ever needed:
AlphaFold/ESMFold full-structure fill then graft (heavier; out of scope now).

## 4. Single-cell reference (.rds → h5ad) for deconvolution
- **v0 immune reference:** GSE225599 (Ammons canine *immune* atlas). Ships as **Seurat `.rds`**
  (`GSE225599_final_dataSet_H.rds.gz` + lineage subsets). Cell-type labels live in Seurat metadata.
- **Converter:** **`sceasy`** (Seurat→AnnData; most robust for Seurat-5) in a **one-shot Modal R
  container** (`UTILITY_IMAGE_PLANS["rds_convert"]`: micromamba `r-base r-seurat r-sceasy anndata`,
  channels conda-forge+bioconda) that emits `h5ad` once to a Volume. Fallback: `zellkonverter`.
- **Label fields to extract:** Ammons convention — `celltype.l1` / `celltype.l2` / `majorID` /
  `clusterID` from `obj@meta.data`. **Exact field names confirmed on first read of the object**
  (documented here as the convention; not assumed silently).
- **Full-TME reference (for a future tumor-purity model):** **`dyammons/canine_osteosarcoma_atlas`**
  (PMC10441479, *Comms Biol* 2024) — canine OSA, 35,310 cells, 41 types incl. **9 tumor, 1
  fibroblast, 1 endothelial, 30 immune**. Same species (no ortholog mapping). *Caveat:* OSA, and its
  endothelial cluster ≠ HSA-tumor-endothelium. **No canine HSA scRNA atlas exists** → an HSA
  tumor-purity model needs a new HSA dataset (generate) or a proxy assembly (OSA immune/stroma +
  healthy-lung normal-endothelium + the bulk HSA-vs-normal-endothelium signature). Flagged, not assumed away.

## 5. Validation anchor (alpelisib–PI3Kα)
- **Experimental p110α IC50 = 4.6 nM** (Fritsch et al., *Mol Cancer Ther* 2014;13(5):1117-29,
  "Characterization of NVP-BYL719"). → pIC50 ≈ 8.34; ΔG ≈ −11.4 kcal/mol; Boltz-scale
  `log₁₀(IC50 µM)` ≈ −2.34.
- **Selectivity (off-target context):** p110β 1156, p110δ 290, p110γ 250 nM (≥50× selective);
  not a VEGFR2 inhibitor → the 3VO3 VEGFR2 dock is the specificity control (expect weak).

## 6. Modal image manifest → concrete builders
`lanes.lane_image_plan(validation_type)` translates each lane's pinned `LaneEnvironment` into a
concrete, inspectable build plan (builder/base/pip/conda/channels/gpu) — unit-tested
(`test_lane_image_plans_are_concrete_and_pinned`) without modal:
- **cofolding** → `debian_slim` + `pip boltz==2.2.1`, A100
- **md** → `micromamba` + `openmm=8.5.1 openmmforcefields=0.16.0 openff-toolkit pdbfixer cuda-version=12.4`, GPU
- **docking** → `registry gnina/gnina:v1.3.1` + `pip rdkit`, A100
- **rds_convert** (utility) → `micromamba` + `r-base r-seurat r-sceasy anndata` (conda-forge+bioconda)
`modal_app` consumes these plans to construct real `modal.Image`s.

## 7. Git / HF housekeeping (reference, don't re-host)
**Vendor/pin into the repo:** pinned version specs (this doc), `lane_image_plan` builders, the
modeled-vs-capped inventory, signature matrices/registry, the harmonization tables.
**Fetch at runtime:** Boltz weights (→ Volume), OpenFF/Amber force fields (package-provided),
GEO/GSE `.rds` (download + one-shot convert → Volume), Ensembl ortholog maps (cached JSON),
cBioPortal (live API). Nothing upstream is re-hosted; we publish the harmonization + derived artifacts.

## Sources
- Boltz: <https://pypi.org/project/boltz/>, <https://github.com/jwohlwend/boltz>, output schema <https://deepwiki.com/jwohlwend/boltz/2.3-output-formats-and-interpretation>, paper <https://www.ncbi.nlm.nih.gov/pmc/articles/PMC12262699/>
- MD stack: <https://github.com/openmm/openmmforcefields>, <https://anaconda.org/conda-forge/openmm>, <https://anaconda.org/conda-forge/pdbfixer>
- Affinity anchor: Fritsch 2014, *Mol Cancer Ther* 13(5):1117-29
- scRNA refs: <https://pmc.ncbi.nlm.nih.gov/articles/PMC10441479/> (canine OSA atlas), <https://github.com/dyammons/canine_osteosarcoma_atlas>, healthy canine lung atlas <https://www.frontiersin.org/journals/immunology/articles/10.3389/fimmu.2025.1501603/full>
