"""v0 deconvolver — constrained-regression cell-type fraction estimation.

Validates the METHOD on synthetic pseudobulk with KNOWN fractions (recovery R²/CCC above a stated
threshold). The real canine immune-atlas reference + GSE95183 validation come once the .rds atlas is
converted; this proves the solver is sound and honest about what it recovers.
"""

from __future__ import annotations

import numpy as np

from hsa_research.ingestion_bridge.deconvolve import (
    build_signature_matrix,
    concordance_cc,
    deconvolve,
    deconvolve_sample,
    make_pseudobulk,
)

# 4 cell types, each with a distinct marker-high profile over 8 genes (immune-style)
_PROFILES = {
    "T_cell": {"CD3E": 50, "CD8A": 40, "FOXP3": 5, "CD163": 1, "MS4A1": 1, "CSF1R": 1, "NKG7": 8, "VWF": 1},
    "Treg": {"CD3E": 30, "CD8A": 3, "FOXP3": 45, "CD163": 1, "MS4A1": 1, "CSF1R": 1, "NKG7": 2, "VWF": 1},
    "Macrophage": {"CD3E": 1, "CD8A": 1, "FOXP3": 1, "CD163": 55, "MS4A1": 1, "CSF1R": 40, "NKG7": 1, "VWF": 2},
    "B_cell": {"CD3E": 2, "CD8A": 1, "FOXP3": 1, "CD163": 1, "MS4A1": 60, "CSF1R": 1, "NKG7": 1, "VWF": 1},
}


def test_signature_matrix_shape():
    genes, cts, S = build_signature_matrix(_PROFILES)
    assert S.shape == (len(genes), len(cts))
    assert set(cts) == set(_PROFILES)


def test_recovers_known_fractions_on_pseudobulk():
    genes, cts, S = build_signature_matrix(_PROFILES)
    true = np.array([0.5, 0.2, 0.2, 0.1])  # T / Treg / Macro / B
    order = [cts.index(c) for c in ["T_cell", "Treg", "Macrophage", "B_cell"]]
    true_ordered = np.zeros(len(cts))
    for frac, idx in zip(true, order):
        true_ordered[idx] = frac
    bulk = make_pseudobulk(S, true_ordered)
    est = deconvolve(bulk, S)
    # near-exact recovery on clean pseudobulk
    assert np.allclose(est, true_ordered, atol=0.02)
    assert concordance_cc(true_ordered, est) > 0.99


def test_recovers_under_noise():
    rng = np.random.default_rng(0)
    genes, cts, S = build_signature_matrix(_PROFILES)
    cccs = []
    for _ in range(20):
        f = rng.dirichlet(np.ones(len(cts)))
        bulk = make_pseudobulk(S, f) * rng.lognormal(0, 0.08, size=S.shape[0])  # multiplicative noise
        est = deconvolve(bulk, S)
        cccs.append(concordance_cc(f, est))
    mean_ccc = float(np.mean(cccs))
    assert mean_ccc > 0.9, f"mean CCC under noise too low: {mean_ccc:.3f}"


def test_fractions_are_valid_simplex():
    genes, cts, S = build_signature_matrix(_PROFILES)
    out = deconvolve_sample({"CD3E": 50, "CD8A": 40, "NKG7": 8}, genes, cts, S)
    vals = list(out.values())
    assert all(v >= 0 for v in vals)
    assert abs(sum(vals) - 1.0) < 1e-3
