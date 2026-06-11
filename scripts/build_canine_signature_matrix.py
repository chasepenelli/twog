"""Build a canine IMMUNE signature matrix from the Ammons canine-leukocyte atlas (UCSC Cell Browser).

Python-native (no R): streams the atlas exprMatrix.tsv.gz, keeps only the TME-registry marker genes,
and computes per-cell-type (celltype.l1) mean expression -> signature matrix for v0 deconvolution.
IMMUNE-ONLY by construction (the atlas has no endothelial/tumor/stromal cells) — stated, not hidden.

    python scripts/build_canine_signature_matrix.py
"""
from __future__ import annotations

import csv
import gzip
import json
import os
import urllib.request

BASE = "https://cells.ucsc.edu/canine-leukocyte-atlas/healthy-os-combined"
META = "/tmp/cla_metaHO.tsv"
EXPR = "/tmp/cla_expr.tsv.gz"
REG = "datasets/canine_hsa_comparative/canine_tme_signature_registry.json"
OUT = "datasets/canine_hsa_comparative/canine_immune_signature_matrix.json"
CELLTYPE = "celltype.l1"


def main() -> None:
    markers = sorted({g["symbol"] for panel in json.load(open(REG))["panels"].values() for g in panel})
    if not os.path.exists(META):
        urllib.request.urlretrieve(f"{BASE}/meta.tsv", META)
    if not os.path.exists(EXPR):
        urllib.request.urlretrieve(f"{BASE}/exprMatrix.tsv.gz", EXPR)

    # cell barcode -> cell type
    cell2type = {r["Cell"]: r[CELLTYPE] for r in csv.DictReader(open(META), delimiter="\t")}

    with gzip.open(EXPR, "rt") as fh:
        header = fh.readline().rstrip("\n").split("\t")
        cells = header[1:]
        col_type = [cell2type.get(c) for c in cells]
        types = sorted({t for t in col_type if t})
        sums = {t: {} for t in types}
        counts = {t: 0 for t in types}
        for t in types:
            counts[t] = sum(1 for ct in col_type if ct == t)
        found = []
        marker_set = set(markers)
        for line in fh:
            tab = line.find("\t")
            gene = line[:tab]
            if gene not in marker_set:
                continue
            found.append(gene)
            vals = line[tab + 1:].rstrip("\n").split("\t")
            acc = {t: 0.0 for t in types}
            for v, ct in zip(vals, col_type):
                if ct:
                    acc[ct] += float(v)
            for t in types:
                sums[t][gene] = acc[t] / counts[t] if counts[t] else 0.0

    profiles = {t: {g: round(sums[t].get(g, 0.0), 4) for g in found} for t in types}
    json.dump(
        {
            "source": "Ammons canine-leukocyte atlas (GSE225599) via UCSC Cell Browser",
            "cell_type_field": CELLTYPE,
            "scope": "IMMUNE-ONLY (no endothelial/tumor/stromal cells in this atlas)",
            "cell_types": types,
            "cells_per_type": counts,
            "markers_found": found,
            "markers_missing": sorted(marker_set - set(found)),
            "profiles": profiles,  # cell_type -> {marker -> mean expression}
        },
        open(OUT, "w"),
        indent=1,
    )
    print(f"cell types: {len(types)} | markers matched: {len(found)}/{len(markers)} -> {OUT}")
    print("types:", types)
    if len(found) < len(markers):
        print("missing (not in atlas gene space):", sorted(marker_set - set(found))[:20])


if __name__ == "__main__":
    main()
