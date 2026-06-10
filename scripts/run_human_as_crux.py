"""Reproduce the human angiosarcoma cross-species crux (Angiosarcoma Project, cBioPortal).

Pulls PIK3CA mutation status + TPM expression from the public Angiosarcoma Project "Count Me In"
study (angs_painter_2025) and runs the same immune/endothelial panels by PIK3CA status, raw and
endothelial-content-adjusted. Free (public API). See docs/HYPOTHESIS_V3.md.

    PYTHONPATH=src python scripts/run_human_as_crux.py
"""
from __future__ import annotations

import json
import math
import urllib.request

import numpy as np

from hsa_research.ingestion_bridge.omics_review import mann_whitney

BASE = "https://www.cbioportal.org/api"
STUDY = "angs_painter_2025"
PIK3CA_ENTREZ = 5290

PANELS = {
    "Immune_broad": ["PTPRC", "CD3E", "CD8A", "CD68", "CD163", "FOXP3", "IFNG", "GZMB"],
    "M2_TAM": ["CD163", "MRC1", "MSR1", "CSF1R"],
    "Cytotoxic_effector": ["CD8A", "PRF1", "GZMK", "GZMA", "NKG7"],
    "IFNg_hallmark": ["IFNG", "STAT1", "IRF1", "GBP1", "CXCL9", "CXCL10", "CXCL11", "IDO1", "TAP1", "PSMB9", "B2M"],
    "Angiogenesis": ["VEGFA", "KDR", "FLT1", "ANGPT2", "DLL4", "TEK", "ESM1", "CD34", "NOTCH4"],
}
ENDO = ["PECAM1", "VWF", "CDH5", "KDR"]


def get(url):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.load(r)


def post(url, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                 headers={"Accept": "application/json", "Content-Type": "application/json"},
                                 method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.load(r)


def main() -> None:
    syms = sorted({g for v in PANELS.values() for g in v} | set(ENDO))
    genes = post(f"{BASE}/genes/fetch?geneIdType=HUGO_GENE_SYMBOL", syms)
    sym2ent = {g["hugoGeneSymbol"]: g["entrezGeneId"] for g in genes}
    ent2sym = {v: k for k, v in sym2ent.items()}

    expr_s = set(get(f"{BASE}/sample-lists/{STUDY}_rna_seq_mrna/sample-ids"))
    seq_s = set(get(f"{BASE}/sample-lists/{STUDY}_sequenced/sample-ids"))
    both = sorted(expr_s & seq_s)
    muts = post(f"{BASE}/molecular-profiles/{STUDY}_mutations/mutations/fetch?projection=SUMMARY",
                {"sampleListId": f"{STUDY}_sequenced", "entrezGeneIds": [PIK3CA_ENTREZ]})
    pikmut = {m["sampleId"] for m in muts}

    data = post(f"{BASE}/molecular-profiles/{STUDY}_mrna_seq_tpm/molecular-data/fetch?projection=SUMMARY",
                {"sampleIds": both, "entrezGeneIds": list(sym2ent.values())})
    expr = {s: {} for s in both}
    for d in data:
        sid, e, v = d["sampleId"], d["entrezGeneId"], d.get("value")
        if sid in expr and e in ent2sym and v is not None:
            try:
                expr[sid][ent2sym[e]] = math.log2(float(v) + 1.0)
            except ValueError:
                pass
    samples = [s for s in both if len(expr[s]) >= len(syms) * 0.8]
    strata = {s: ("mutant" if s in pikmut else "wt") for s in samples}
    found = set().union(*[set(expr[s]) for s in samples]) if samples else set()
    nm = sum(v == "mutant" for v in strata.values())
    print(f"Human angiosarcoma crux ({STUDY}): n={len(samples)} ({nm} PIK3CA-mut vs {len(samples)-nm} WT)")
    print(f"PIK3CA mutation rate in cohort: {len(pikmut)}/{len(seq_s)} (~{100*len(pikmut)/len(seq_s):.0f}%); "
          "NB human AS PIK3CA muts are mostly non-hotspot VUS — see docs/HYPOTHESIS_V3.md\n")

    def mscore(panel, samps):
        gs = [g for g in panel if g in found]
        M = np.array([[expr[s].get(g, np.nan) for g in gs] for s in samps], float)
        cm = np.nanmean(M, 0)
        idx = np.where(np.isnan(M))
        M[idx] = np.take(cm, idx[1])
        Z = (M - M.mean(0)) / (M.std(0) + 1e-9)
        return Z.mean(1)

    def mw(a, b):
        r = mann_whitney(list(a), list(b))
        return r["auc"], r["p"]

    g = np.array([strata[s] for s in samples])
    endo = mscore(ENDO, samples)
    print(f"{'panel':20} {'raw AUC':>8} {'p':>7} {'endo-adj':>9} {'p':>7}")
    for name, panel in {**PANELS, "Endothelial_content": ENDO}.items():
        sc = mscore(panel, samples)
        raw = mw(sc[g == "mutant"], sc[g == "wt"])
        if name == "Endothelial_content":
            print(f"{name:20} {raw[0]:8.2f} {raw[1]:7.3f} {'—':>9}")
            continue
        b = np.polyfit(endo, sc, 1)
        res = sc - np.polyval(b, endo)
        adj = mw(res[g == "mutant"], res[g == "wt"])
        print(f"{name:20} {raw[0]:8.2f} {raw[1]:7.3f} {adj[0]:9.2f} {adj[1]:7.3f}")


if __name__ == "__main__":
    main()
