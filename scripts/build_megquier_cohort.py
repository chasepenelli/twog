"""Build the Megquier 2019 PIK3CA/TP53-stratified crux cohort from the supplementary PDF.

Thin CLI wrapper over the tested parser in hsa_research.ingestion_bridge.supplement_parser
(Tables S4 pp.6-8 + S5 p.9). Run with the package importable:
    PYTHONPATH=src python scripts/build_megquier_cohort.py <supplement.pdf> [out.json]
"""
from __future__ import annotations

import json
import os
import sys

from hsa_research.ingestion_bridge.supplement_parser import build_cohort, read_pdf_pages

if __name__ == "__main__":
    pdf_path = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else "data/megquier_cohort.json"
    pages = read_pdf_pages(pdf_path)
    parsed = build_cohort(pages[5:8], pages[8], stratify_gene="PIK3CA")  # S4 = pp.6-8, S5 = p.9

    # legacy-compatible output schema (readers use crux_strata_pik3ca_public + cohort[h].pik3ca)
    cohort = {
        h: {
            "pik3ca": c["genotype"]["PIK3CA"],
            "tp53": c["genotype"]["TP53"],
            "mutated_examined_genes": c["mutated_examined_genes"],
            "has_public_expression": c["has_public_expression"],
            "rnaseq_ids_public": c["rnaseq_ids_public"],
            "biosamples": c["biosamples"],
        }
        for h, c in parsed["cohort"].items()
    }
    result = {
        "source": "Megquier 2019 (PMC7067513) Supplementary Tables S4+S5",
        "expression_source": "GSE95183 (public FPKM matrix, columns = RNA-seq IDs)",
        "cohort": cohort,
        "crux_strata_pik3ca_public": parsed["crux_strata_public"],
        "counts": {
            "pik3ca_mutant_public": parsed["counts"]["mutant_public"],
            "pik3ca_wt_public": parsed["counts"]["wt_public"],
            "total_exome_matched_rnaseq": parsed["counts"]["total_exome_matched_rnaseq"],
        },
    }
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as fh:
        json.dump(result, fh, indent=2)
    c = result["counts"]
    print(f"exome-matched RNA-seq samples: {c['total_exome_matched_rnaseq']}")
    print(f"PIK3CA crux (public expression): {c['pik3ca_mutant_public']} mutant vs {c['pik3ca_wt_public']} WT")
    print(f"wrote {out}")
