# Data Sources & Access — for twog

> Verified June 2026 by three scouting passes (canine/comparative-oncology data · Hugging Face
> bio/chem hubs · GitHub data-access tooling + benchmarks). Accessions/repos checked live where
> noted; items that couldn't be verified are marked UNVERIFIED. Honest license/access-gating calls
> included. Companion to `FRONTIER_SCAN.md` (tools/models) and `PHASE3_PLAN.md` (lanes).

## ★ The headline: the hypothesis crux is resolvable with OPEN data

The developed hypothesis (PIK3CA-mutant canine splenic HSA carries an immunosuppressive TME that
local IL-12 would relieve) has one cheapest-first crux: *does the PIK3CA-mutant/TP53 subset carry
the immunosuppressive signature?* It is now a concrete, mostly-free analysis:

| dataset | accession | access | role |
|---|---|---|---|
| **Megquier 2019 (Broad/UMN)** RNA-seq + matched WES | **PRJNA562916** (RNA-seq, 23) + **PRJNA552034** (WES, 47) | open SRA, no dbGaP | the linchpin — only open canine HSA data linking mutation status (PIK3CA 29.8%, TP53 59.6%) to transcriptomes |
| **Ammons 2023 canine leukocyte atlas** | **GSE225599** (74k cells, 36 populations) | open GEO + UCSC Cell Browser | the **deconvolution reference** (CIBERSORTx/TIMER) to score Tregs/TAMs/immunosuppression in bulk HSA RNA-seq |
| **Wang/UPenn** canine HSA | **PRJNA526923** (50 HSA) | open SRA | independent replication cohort |
| **Angiosarcoma Project** (human) | cBioPortal `angs_project_painter_2018` (open) / dbGaP phs001931 (controlled) | cBioPortal API open | human cross-species confirmation (KDR/TP53/PIK3CA) |
| **Banerjee 2024** PIK3CA→immune | PMID 39709507 (reuses GSE157858) | open | ★ direct mechanistic anchor: PIK3CA mutation → IL-6/IL-8/MCP-1 immune-cytokine program in vascular cancers + alpelisib resistance |

**The recipe:** pull Megquier RNA-seq → stratify by PIK3CA/TP53 (WES calls in paper supplements)
→ deconvolve with the Ammons atlas → test for Treg/TAM/immunosuppression enrichment in the
PIK3CA-mutant subset → replicate in Wang → cross-check human via the Angiosarcoma Project. The
**Banerjee 2024** paper already reports PIK3CA→immune-cytokine enrichment — so the v2 hypothesis's
core mechanism gained independent support from this scan.

**Honest negatives:** ICDC has **no** HSA study (verified via its live GraphQL API; closest is
STS01 soft-tissue sarcoma). FidoCure's Estabrooks 2023 (109 splenic HSA, largest) is a proprietary
panel with **no public deposition**. Brachelente 2024 TME data is **IHC only** — no omics accession.

---

## A — Canine / comparative-oncology data

- **Megquier 2019** — RNA-seq **PRJNA562916**, WES **PRJNA552034**; PMC7067513. Open SRA. *Crux dataset.*
- **Wang/UPenn** "Molecular subtypes in canine HSA" — **PRJNA526923** (50 HSA); PMC7094861. Open SRA. Replication.
- **Wong/Sanger** canine+feline+human — ENA **ERP119497** (canine) / **ERP119871** (feline); PMC8319545. ~1000-gene panel; TP53/PIK3CA/ATRX recurrent. Open via ENA. Cross-species mutation-frequency reference.
- **Ammons 2023 canine leukocyte scRNA atlas** — **GSE225599**; UCSC Cell Browser https://canine-leukocyte-atlas.cells.ucsc.edu; code github.com/dyammons/Canine_Leukocyte_scRNA. Open. *The deconvolution reference panel.*
- **DoGA expression atlas** — https://www.doggenomeannotation.org (Nat Commun 2024, PMC11494170). 100 canine tissues, CanFam4. Open. Tissue baselines for VEGFR2/KDR/KIT/PIK3CA/MTOR (incl. normal spleen).
- **ICDC (Integrated Canine Data Commons)** — https://caninecommons.cancer.gov; GraphQL `POST /v1/graphql/` (18 studies/1029 cases). **No HSA study.** Use as comparative-oncology schema reference + STS01; re-query periodically.
- **Angiosarcoma Project (Count Me In/Broad)** — cBioPortal `angs_project_painter_2018` (open, KDR/TP53/PIK3CA); raw reads dbGaP **phs001931** (controlled). Human reference.
- **TCGA-SARC** — GDC + cBioPortal `sarc_tcga`; few/no pure angiosarcomas — broad sarcoma context only.
- **Banerjee/Dickerson 2024** "PIK3CA mutation fortifies immune signaling in vascular cancers" — PMID 39709507 (reuses GSE157858). ★ mechanistic anchor for the v2 hypothesis.
- Low relevance to the crux: Dog Aging Project / Darwin's Ark (aging/behavior, not tumor omics).

twog use: these slot into the existing GEO/SRA/ICDC harvesters; the crux is a target-expression +
TME-deconvolution analysis — a natural **omics-review lane** (Phase 3) over PRJNA562916 × GSE225599.

## B — Hugging Face (models + datasets)

**Compute-lane models** (pull order): `boltz-community/boltz-2` (**MIT**, co-fold+affinity keystone) ·
`chaidiscovery/chai-1` (**Apache-2.0**, 2nd co-fold engine) · `facebook/esm2_t33_650M_UR50D` (**MIT**,
embeddings) · `facebook/esmfold_v1` (**MIT**) · `ibm-research/MoLFormer-XL-both-10pct` (**Apache-2.0**)
+ `DeepChem/ChemBERTa-77M-MTR` (ADMET backbones).
**⚠ License landmine:** `EvolutionaryScale/esm3-sm-open-v1` + ESMC are **gated, NON-COMMERCIAL** — use
ESM-2 instead. No commercial AF3 weights — Boltz-2/Chai-1 are the AF3-class substitutes.

**Datasets:** `maomlab/TDC` (**MIT** mirror of TDC ADMET/Tox/HTS, ~46M points) · `jglaser/binding_affinity`
(~3.67M protein+ligand+affinity from BindingDB/PDBbind/BioLIP — license UNVERIFIED, confirm before
commercial) · `ibm-research/otter_uniprot_bindingdb_chembl` (6.2M KG triples) · `hyskova-anna/proteins`
(PDB benchmark set).

**Evidence-synthesis / RAG:** `ncbi/MedCPT-Query-Encoder` + `ncbi/MedCPT-Article-Encoder` (top biomedical
retriever) · `NeuML/pubmedbert-base-embeddings` (**Apache-2.0**, RAG embeddings) ·
`microsoft/BiomedNLP-PubMedBERT-base-uncased-abstract` (**MIT**, NER) · `dmis-lab/biobert-v1.1` ·
`allenai/scibert_scivocab_uncased`. (Prefer a modern general LLM + MedCPT retrieval over the legacy
`microsoft/biogpt` for synthesis.)

**Honest gap:** effectively **no usable canine/veterinary-oncology dataset on HF** — that ingestion is
custom from GEO/SRA/literature (see §A).

## C — GitHub tooling + benchmarks

**Power/benchmark the compute lanes (wire first):**
1. **PyTDC** — `mims-harvard/TDC` (**MIT**, `pip install PyTDC`) — one-line ADMET + DAVIS/KIBA/BindingDB
   benchmarks with canonical scaffold splits. Highest leverage.
2. **PoseBusters** — `maabuu/posebusters` (**MIT**) — RDKit pose-validity benchmark. **Non-negotiable QC
   gate** on every docking/co-fold pose before it reaches the ledger.
3. **LP-PDBBind** — `THGLab/LP-PDBBind` (**CC-BY-4.0**) — leak-free PDBbind split labels (avoids the
   restricted raw PDBbind; pull structures from RCSB instead).
4. **rcsb-api** — `rcsb/py-rcsb-api` (**MIT**) + **gget** — `scverse/gget` (**BSD-2**, note org moved from
   pachterlab) — structure search/fetch (PDB + AlphaFold) for target prep.
5. **datamol** (`datamol-io/datamol`, Apache-2.0) + **PubChemPy** (`mcs07/PubChemPy`, MIT) +
   **chembl_webresource_client** (EBI official) — molecule prep/lookup feeding docking. RDKit underneath.
6. **LIT-PCBA** (drugdesign.unistra.fr/LIT-PCBA) over the biased DUD-E — screening-power benchmark.

**Enrich the claim/evidence ledger:**
1. **OAK / oaklib** — `INCATools/ontology-access-kit` (**Apache-2.0**) — one library for MONDO + DOID + GO
   term normalization/traversal. Best single entry point.
2. **Open Targets** — GraphQL / BigQuery / Parquet (⚠ the old `opentargets-py` client is **dead/archived** —
   do not use). Target–disease evidence scores.
3. **PrimeKG** — `mims-harvard/PrimeKG` (**MIT**) — prebuilt precision-medicine KG (17k diseases, 4M edges)
   to seed the claim graph.
4. **mygene/BioThings** (`biothings/mygene.py`) — gene/chem/disease ID normalization.
5. **bioservices** (`cokelaer/bioservices`) — uniform Reactome/UniProt/KEGG fallback. **pysradb**
   (`saketkc/pysradb`) + **GEOparse** for the SRA/GEO harvesters.

## License / access gotchas (clear before shipping anything paid)
- **PDBbind / CASF / Binding MOAD** — academic registration, post-2020 not free, no redistribution → use
  LP-PDBBind split labels + RCSB structures.
- **BindingDB** — CC-BY-4.0 overall, but **ChEMBL-sourced rows are CC-BY-SA-3.0 (share-alike propagates)**.
- **ESM3/ESMC** — non-commercial; **AF3 weights** — not commercially licensed → Boltz-2/Chai-1.
- **Open Babel (GPL-2.0)**, **pypath (GPL-3.0)** — copyleft; isolate behind a CLI/process boundary.
- **dbGaP** (Angiosarcoma Project raw reads, TCGA controlled tier) — Data Access Request required; the
  cBioPortal/GDC *open* tiers cover most needs without it.

## How this maps to twog
- **Harvesters** already cover GEO/SRA/ICDC/ChEMBL/PubChem/UniProt/RCSB — §A datasets ingest through them.
- **Compute lanes** (Phase 3): Boltz-2/Chai-1 (co-fold), gnina (docking), ADMET-AI/ChemBERTa/MoLFormer
  (ADMET) — benchmarked by PyTDC + LP-PDBBind, QC'd by PoseBusters, fed structures by rcsb-api/gget.
- **The crux** is an **omics-review lane**: PRJNA562916 × GSE225599 deconvolution → a directional
  `parse_result` signal → a proof capsule on the candidate. The first *real* non-docking lane to build.
- **Claim ledger**: OAK (MONDO/DOID) + Open Targets + PrimeKG enrich provenance; MedCPT + PubMedBERT
  power citation-grounded RAG (the PaperQA2/Kosmos patterns from `FRONTIER_SCAN.md`).
