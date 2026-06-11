# Scope — canine tumor TME deconvolution model (first trained HF contribution)

**Status:** scoping. **Goal:** a public model that estimates immune/stromal/tumor-cell fractions
(and tumor purity) from a **bulk canine tumor RNA-seq profile** — the dog equivalent of
CIBERSORTx/ESTIMATE, which are human/mouse-only. Directly fixes the **purity confound** that
neutralized our PIK3CA omics crux, and upgrades the omics lane from coarse mean-z scoring to
calibrated fractions.

## Why this is the right trained contribution
- **Real gap:** no validated canine TME-deconvolution tool exists. Vet-onc researchers need it.
- **Flywheel:** fixes our own confounder → better crux → more (honest) findings → more capsules;
  others adopt it → more canine TME data → better model. Positive-sum (3,3).
- **Tractable data:** ground-truth cell fractions are obtainable via **pseudobulk** from public
  canine single-cell atlases — the usual deconvolution-training trick — so we don't need labeled
  bulk samples.

## Approach (ranked)
1. **Pseudobulk-supervised regression (recommended).** From canine tumor/immune scRNA-seq, sum
   cells into pseudobulk profiles with *known* cell-type fractions; train a model (NNLS-style linear
   deconvolution, or gradient-boosted / small MLP) mapping bulk expression → fractions + purity.
   Ground truth is exact (we set the mixing). Output: per-sample fractions for the registry's cell
   types + a tumor-purity scalar.
2. **Reference signature-matrix (CIBERSORTx-style).** Build a canine cell-type signature matrix from
   the scRNA atlas, solve fractions by constrained regression at inference. Simpler, no training, but
   less robust to platform shift. Good v0 baseline.
3. **Marker mean-z (current).** What the omics lane does now — keep as the dependency-free fallback.

Plan: ship **(2) as v0** (no training, immediate, strong baseline) and **(1) as v1** (the trained HF
model) once the pseudobulk pipeline is validated.

## Data sources (public)
- **Single-cell reference (ground truth for pseudobulk):** Ammons et al. canine immune atlas
  (GSE225599, already referenced); canine tumor scRNA-seq (HSA / osteosarcoma / lymphoma series on
  GEO). Need cell-type annotations — use the atlas's, or annotate with the signature registry.
- **Bulk validation/anchoring:** ARCHS4-scale uniformly-processed canine RNA-seq (thousands of
  samples) for distribution anchoring + sanity checks; GSE95183 (our HSA cohort) as a real test set.
- **Genes / orthologs:** `canine_tme_signature_registry.json` (both assemblies) — the feature space.

## Model + eval
- **Inputs:** bulk expression vector over the registry genes (assembly-aware via the registry).
- **Outputs:** cell-type fraction vector (T/CD8/Treg/NK/B/M1/M2/DC/fibroblast/endothelial) + tumor
  purity (1 − immune − stroma).
- **Eval:** held-out pseudobulk (R²/CCC per cell type); correlation with ESTIMATE-style ImmuneScore
  on shared markers; **the purity-confound benchmark** (re-run the PIK3CA crux with model-derived
  purity as the covariate — does the immune signal still wash out?).
- **Honesty / limitations:** pseudobulk ≠ real bulk (no ambient RNA / capture bias); canine cell-type
  annotations are imperfect; report confidence + the train/test domain gap. Hypothesis-generating.

## Compute + deliverable
- Training is modest (CPU or single small GPU); pseudobulk generation is the main cost (manageable).
  Can run locally or on Modal (flag spend if GPU).
- **Deliverable:** a HF **model** (weights + signature matrix + inference code) and a HF **Space**
  (upload bulk canine expression → fractions + purity), citing the scRNA sources. Pairs with the
  `canine_hsa_comparative` dataset.

## First concrete steps (free)
1. Pull the canine scRNA atlas (GSE225599) cell-type annotations; confirm coverage of the registry markers.
2. Build the **v0 signature matrix** + a constrained-regression deconvolver (no training) — validate
   on GSE95183 pseudobulk.
3. If v0 holds, build the pseudobulk-supervised v1 trainer.
