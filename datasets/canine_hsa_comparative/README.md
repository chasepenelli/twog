---
license: cc-by-4.0
language:
  - en
tags:
  - comparative-oncology
  - canine
  - hemangiosarcoma
  - angiosarcoma
  - PIK3CA
  - tumor-microenvironment
  - bioinformatics
pretty_name: Canine Hemangiosarcoma ↔ Human Angiosarcoma (harmonized, PIK3CA-stratified)
size_categories:
  - n<1K
---

# Canine Hemangiosarcoma ↔ Human Angiosarcoma — harmonized PIK3CA-stratified cohort

A small, **harmonized comparative-oncology resource** linking canine hemangiosarcoma (HSA) and human
angiosarcoma (AS) by PIK3CA/TP53 genotype, with reusable immune/endothelial **signature definitions
carrying canine Ensembl ortholog IDs**, and a fully **reproducible analysis pipeline**.

The contributed work is the **harmonization** — per-sample genotype↔expression↔archive mapping,
cross-species ortholog resolution, and the reproducible crux + purity-confound analysis. Raw
expression is **referenced, not re-hosted** (see Sources), so this dataset is license-clean.

## Why this exists
Canine HSA is a leading natural model for human AS, but public data is fragmented across a paper
supplement (genotypes), GEO (canine expression), and cBioPortal (human). Stitching them into a
single analysis-ready cohort is non-trivial and not otherwise available. This packages that work so
others can build on it.

## Files
| file | what it is |
|---|---|
| `canine_samples.csv` | per-sample harmonized table: exome_id ↔ rnaseq_id ↔ PIK3CA/TP53 status ↔ archive IDs ↔ expression source |
| `signatures.json` | 7 immune/endothelial signature panels (Treg, M2-TAM, cytotoxic, IFN-γ, endothelial, angiogenesis, cytokines) with **canine CanFam3.1 Ensembl IDs** per human symbol |
| `build_dataset.py` | rebuilds the artifacts from the upstream sources (reproducible) |

`canine_samples.csv` columns: `exome_id` (HSA_n, Megquier WES), `rnaseq_id` (GSE95183 matrix column;
empty = raw-SRA-only), `pik3ca`/`tp53` (`mutant`/`wt` from exome calls), `mutated_examined_genes`,
`has_public_expression`, `biosamples`, `expression_source`.

## Sources (raw data referenced, not re-hosted)
- **Genotypes:** Megquier et al. 2019, *Mol Cancer Res* (PMC7067513), Supplementary Tables S4+S5.
- **Canine expression:** GEO **GSE95183** (public FPKM matrix; columns = `rnaseq_id`).
- **Human angiosarcoma:** cBioPortal **Angiosarcoma Project** (`angs_painter_2025`, Count Me In) —
  TPM expression + mutations, fetched live via the cBioPortal API.
- **Orthologs:** Ensembl release 104 (CanFam3.1) symbol→gene xrefs.

## Headline finding (reproducible)
Across both species, **PIK3CA-mutant tumors are more endothelial/tumor-dense**, and bulk immune
differences are largely a **tumor-purity artifact** (they wash out after purity adjustment):
- Canine HSA (n=5 mut vs 9 wt): endothelial-content AUC **0.71**; immune signal neutral after purity adjustment.
- Human AS (n=5 mut vs 93 wt): endothelial-content AUC **0.96, p=0.001**.
- Caveat: human PIK3CA muts are rare (~4%) and **non-hotspot/VUS** — association replicates, causation unproven.

## Reproduce
```bash
PYTHONPATH=src python scripts/build_megquier_cohort.py <Megquier_supplement.pdf>   # genotypes
PYTHONPATH=src python scripts/run_megquier_crux.py <supplement.pdf>                # canine crux + purity test
PYTHONPATH=src python scripts/run_human_as_crux.py                                 # human cross-species crux
PYTHONPATH=src python datasets/canine_hsa_comparative/build_dataset.py             # rebuild these artifacts
```

## Limitations
Small n (canine 14, human 98); bulk RNA-seq with coarse signature scoring; genotype strata from
exome calls (variable tumor purity / VAF); CanFam3.1 IDs do **not** join the newer ROS_Cfam_1.0
(`ENSCAFG00845…`) assembly. Hypothesis-generating, not clinical.

## Citation
Please cite the upstream sources (Megquier 2019; GSE95183; The Angiosarcoma Project) alongside this
harmonization. Produced by the twog comparative-oncology research engine.
