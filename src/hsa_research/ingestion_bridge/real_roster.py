"""Real campaign roster + the bridge from the offline Megquier real-data pipeline onto candidates.

The omics engine (omics_review.run_omics_review) and the cohort are real (GSE95183 public FPKM matrix
+ data/megquier_cohort.json genotype strata). This module materializes INLINE `expression` + `strata`
so the omics lane runs the REAL engine — including on Modal, where a local matrix_path would not exist.
The Megquier load helpers live here (the canonical home); scripts/run_megquier_crux.py imports them.
Paths resolve from the repo root via __file__, so it works regardless of cwd.
"""

from __future__ import annotations

import gzip
import json
import math
import pathlib
import urllib.request
from typing import Any

_DATA = pathlib.Path(__file__).resolve().parents[3] / "data"
GSE_URL = (
    "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE95nnn/GSE95183/suppl/"
    "GSE95183_fpkms_hemangiosarcoma.txt.gz"
)
MATRIX = _DATA / "GSE95183_fpkms.txt.gz"
COHORT = _DATA / "megquier_cohort.json"
SYM2ENS = _DATA / "canine_symbol_to_ensembl.json"
ROSTER = _DATA / "real_roster.json"

# Immune / TME / angiogenesis signature panels (canine HSA PIK3CA crux), gene symbols.
PANELS: dict[str, list[str]] = {
    "Immune_broad": ["PTPRC", "CD3E", "CD8A", "CD68", "CD163", "FOXP3", "IFNG", "GZMB"],
    "M2_TAM": ["CD163", "MRC1", "MSR1", "CSF1R"],
    "Cytotoxic_effector": ["CD8A", "PRF1", "GZMK", "GZMA"],
    "IFNg_hallmark": ["IFNG", "STAT1", "IRF1", "GBP1", "CXCL10", "IDO1", "TAP1", "B2M", "JAK2"],
    "Proliferation": ["MKI67", "PCNA", "TOP2A", "CCNB1", "CCNA2", "BUB1", "FOXM1", "CDK1"],
    "Angiogenesis": ["VEGFA", "KDR", "FLT1", "ANGPT2", "DLL4", "TEK", "ESM1", "CD34", "NOTCH4"],
}
ENDO = ["PECAM1", "VWF", "CDH5", "KDR"]


def matrix_available() -> bool:
    return MATRIX.exists()


def ensure_matrix() -> None:
    if not MATRIX.exists():
        MATRIX.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(GSE_URL, str(MATRIX))


def load_expression(strata: dict, ens2sym: dict) -> tuple[list[str], dict, set]:
    """Return (samples, expr[sample][symbol]=log2(fpkm+1), found_symbols) for the GSE95183 matrix."""
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


def build_megquier_omics_inputs() -> dict[str, Any]:
    """Materialize inline omics-lane inputs (expression + strata + signatures) from the real Megquier
    cohort. Fetches the GSE95183 matrix if absent (network). The result drops straight into a candidate's
    metadata['lane_inputs']['omics'] and runs the REAL run_omics_review engine (not the stub)."""
    cohort = json.loads(COHORT.read_text())
    strata = cohort["crux_strata_pik3ca_public"]
    sym2ens = json.loads(SYM2ENS.read_text())
    ens2sym = {v: k for k, v in sym2ens.items() if str(v).startswith("ENSCAFG00000")}
    ensure_matrix()
    samples, expr, _found = load_expression(strata, ens2sym)
    return {
        "expression": expr,
        "strata": {s: strata[s] for s in samples},
        "signatures": {name: list(genes) for name, genes in {**PANELS, "Endothelial_content": ENDO}.items()},
        "direction_hypothesis": "immunosuppression_higher_in_mutant",
        "min_n_per_stratum": 4,
        "source_refs": ["GSE95183", "PRJNA562916", "PMC7067513"],
    }


def load_roster(path: str | pathlib.Path | None = None) -> list[dict[str, Any]]:
    """Load the real campaign roster (list of candidate specs). Returns [] if absent."""
    p = pathlib.Path(path) if path is not None else ROSTER
    if not p.exists():
        return []
    return json.loads(p.read_text()).get("candidates", [])
