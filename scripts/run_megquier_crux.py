"""Reproduce the canine PIK3CA omics crux + purity-confound test (Megquier 2019 / GSE95183).

Pipeline (all public data, no spend): genotype strata from data/megquier_cohort.json (built by
build_megquier_cohort.py) + GSE95183 public FPKM matrix + cached canine symbol->Ensembl map.
Runs immune/endothelial/IFN signatures by PIK3CA status, raw and tumor-purity-adjusted (endothelial
content proxy; and ESTIMATE RNA-seq purity if the supplement PDF is given). See docs/HYPOTHESIS_V3.md.

    PYTHONPATH=src python scripts/run_megquier_crux.py [supplement.pdf]
"""
from __future__ import annotations

import gzip
import json
import math
import os
import re
import sys
import urllib.request

import numpy as np

from hsa_research.ingestion_bridge.omics_review import mann_whitney

GSE_URL = ("https://ftp.ncbi.nlm.nih.gov/geo/series/GSE95nnn/GSE95183/suppl/"
           "GSE95183_fpkms_hemangiosarcoma.txt.gz")
MATRIX = "data/GSE95183_fpkms.txt.gz"

PANELS = {
    "Immune_broad": ["PTPRC", "CD3E", "CD8A", "CD68", "CD163", "FOXP3", "IFNG", "GZMB"],
    "M2_TAM": ["CD163", "MRC1", "MSR1", "CSF1R"],
    "Cytotoxic_effector": ["CD8A", "PRF1", "GZMK", "GZMA"],
    "IFNg_hallmark": ["IFNG", "STAT1", "IRF1", "GBP1", "CXCL10", "IDO1", "TAP1", "B2M", "JAK2"],
    "Proliferation": ["MKI67", "PCNA", "TOP2A", "CCNB1", "CCNA2", "BUB1", "FOXM1", "CDK1"],
    "Angiogenesis": ["VEGFA", "KDR", "FLT1", "ANGPT2", "DLL4", "TEK", "ESM1", "CD34", "NOTCH4"],
}
ENDO = ["PECAM1", "VWF", "CDH5", "KDR"]


def ensure_matrix() -> None:
    if not os.path.exists(MATRIX):
        os.makedirs("data", exist_ok=True)
        urllib.request.urlretrieve(GSE_URL, MATRIX)


def load_expression(strata: dict, ens2sym: dict) -> tuple[list[str], dict, set]:
    with gzip.open(MATRIX, "rt") as fh:
        header = fh.readline().rstrip("\n").split("\t")
        ci = {c: i for i, c in enumerate(header)}
        samples = [s for s in strata if s in ci]
        expr = {s: {} for s in samples}
        found: set[str] = set()
        for line in fh:
            p = line.rstrip("\n").split("\t")
            if p[0] in ens2sym:
                sym = ens2sym[p[0]]
                found.add(sym)
                for s in samples:
                    expr[s][sym] = math.log2(float(p[ci[s]]) + 1.0)
    return samples, expr, found


def parse_estimate_purity(pdf_path: str) -> dict:
    """Per-sample ESTIMATE RNA-seq purity from Megquier Table S5 (page 9)."""
    from hsa_research.ingestion_bridge.supplement_parser import read_pdf_pages

    s5 = read_pdf_pages(pdf_path)[8]
    row = re.compile(r"\bHSA_(\d+)\b.*?\b(?:fixed|frozen)\s+[\d.]+\s+(?:NA|[\d.]+)\s+([\d.]+)\s+(?:Yes|No)")
    purity: dict[str, float] = {}
    for line in s5.splitlines():
        m = row.search(line)
        if m:
            purity.setdefault(f"HSA_{m.group(1)}", float(m.group(2)))
    return purity


def main() -> None:
    cohort_doc = json.load(open("data/megquier_cohort.json"))
    strata = cohort_doc["crux_strata_pik3ca_public"]
    sym2ens = json.load(open("data/canine_symbol_to_ensembl.json"))
    ens2sym = {v: k for k, v in sym2ens.items() if str(v).startswith("ENSCAFG00000")}
    ensure_matrix()
    samples, expr, found = load_expression(strata, ens2sym)

    def mscore(panel, samps):
        genes = [g for g in panel if g in found]
        M = np.array([[expr[s][g] for g in genes] for s in samps])
        Z = (M - M.mean(0)) / (M.std(0) + 1e-9)
        return Z.mean(1)

    def mw(a, b):
        r = mann_whitney(list(a), list(b))
        return r["auc"], r["p"]

    g = np.array([strata[s] for s in samples])
    endo = mscore(ENDO, samples)
    nm = int((g == "mutant").sum())
    nw = int((g == "wt").sum())
    print(f"Canine HSA PIK3CA crux: n={len(samples)} ({nm} mutant vs {nw} WT)\n")
    print(f"{'panel':20} {'raw AUC':>8} {'endo-adj':>9}   (AUC>0.5 = higher in mutant)")
    for name, panel in {**PANELS, "Endothelial_content": ENDO}.items():
        sc = mscore(panel, samples)
        raw = mw(sc[g == "mutant"], sc[g == "wt"])
        if name == "Endothelial_content":
            print(f"{name:20} {raw[0]:8.2f} {'—':>9}")
            continue
        b = np.polyfit(endo, sc, 1)
        res = sc - np.polyval(b, endo)
        adj = mw(res[g == "mutant"], res[g == "wt"])
        print(f"{name:20} {raw[0]:8.2f} {adj[0]:9.2f}")

    # optional ESTIMATE-purity adjustment if the supplement PDF is provided
    pdf = sys.argv[1] if len(sys.argv) > 1 else None
    if pdf and os.path.exists(pdf):
        purH = parse_estimate_purity(pdf)
        rid_pur = {}
        for hsa, c in cohort_doc["cohort"].items():
            for rid in c.get("rnaseq_ids_public", []):
                if hsa in purH:
                    rid_pur[rid] = purH[hsa]
        ps = [s for s in samples if s in rid_pur]
        if len(ps) >= 8:
            pur = np.array([rid_pur[s] for s in ps])
            gp = np.array([strata[s] for s in ps])
            print(f"\nESTIMATE-purity-adjusted (n={len(ps)}):")
            for name, panel in PANELS.items():
                sc = mscore(panel, ps)
                b = np.polyfit(pur, sc, 1)
                res = sc - np.polyval(b, pur)
                print(f"  {name:20} adj AUC={mw(res[gp=='mutant'], res[gp=='wt'])[0]:.2f}")


if __name__ == "__main__":
    main()
