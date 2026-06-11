"""Build the harmonized canine-HSA comparative-oncology dataset artifacts.

License-safe by design: we publish the HARMONIZATION (sample <-> genotype <-> archive map), the
derived genotype strata, and the signature definitions + reproducible pipeline — and REFERENCE
GEO (GSE95183) / cBioPortal (Angiosarcoma Project) for raw expression rather than re-hosting it.
The harmonization is the contributed work.

    PYTHONPATH=src python datasets/canine_hsa_comparative/build_dataset.py
"""
from __future__ import annotations

import csv
import json
import os

HERE = os.path.dirname(__file__)
COHORT = "data/megquier_cohort.json"
SYMS = "data/canine_symbol_to_ensembl.json"

# the signature panels used by the omics crux (human symbol -> role)
PANELS = {
    "Treg": ["FOXP3", "CTLA4", "IL2RA", "IKZF2"],
    "M2_TAM": ["CD163", "MRC1", "MSR1", "CSF1R"],
    "immunosuppressive_cytokines": ["IL10", "TGFB1", "CCL2"],
    "cytotoxic_effector": ["CD8A", "PRF1", "GZMK", "GZMA", "NKG7"],
    "IFNg_response": ["IFNG", "STAT1", "IRF1", "GBP1", "CXCL10", "IDO1", "TAP1", "B2M", "JAK2"],
    "endothelial_tumor": ["PECAM1", "VWF", "CDH5", "KDR"],
    "angiogenesis": ["VEGFA", "KDR", "FLT1", "ANGPT2", "DLL4"],
}


def build() -> None:
    cohort = json.load(open(COHORT))["cohort"]
    syms = json.load(open(SYMS))

    # 1) per-sample harmonized table (canine RNA-seq sample <-> exome <-> genotype <-> archive)
    rows = []
    for hsa, c in sorted(cohort.items(), key=lambda kv: int(kv[0].split("_")[1])):
        rnaseq_ids = c.get("rnaseq_ids_public") or [None]
        for rid in rnaseq_ids:
            rows.append({
                "exome_id": hsa,
                "rnaseq_id": rid or "",  # GSE95183 expression-matrix column (empty = raw-only)
                "pik3ca": c["pik3ca"],
                "tp53": c["tp53"],
                "mutated_examined_genes": ";".join(c.get("mutated_examined_genes", [])),
                "has_public_expression": c["has_public_expression"],
                "biosamples": ";".join(c.get("biosamples", [])),
                "expression_source": "GSE95183" if rid else "PRJNA562916 (raw SRA)",
            })
    with open(os.path.join(HERE, "canine_samples.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    # 2) signature definitions with canine CanFam3.1 Ensembl IDs (the cross-species ortholog work)
    sig_out = {
        name: [{"symbol": s, "canine_ensembl_canfam3_1": syms.get(s)} for s in genes]
        for name, genes in PANELS.items()
    }
    json.dump(
        {"assembly": "CanFam3.1 (Ensembl r104)", "signatures": sig_out},
        open(os.path.join(HERE, "signatures.json"), "w"),
        indent=2,
    )

    n_mut = sum(r["pik3ca"] == "mutant" and r["has_public_expression"] for r in rows)
    n_wt = sum(r["pik3ca"] == "wt" and r["has_public_expression"] for r in rows)
    print(f"wrote canine_samples.csv ({len(rows)} rows; public PIK3CA {n_mut} mut / {n_wt} wt)")
    print(f"wrote signatures.json ({len(PANELS)} panels)")


if __name__ == "__main__":
    build()
