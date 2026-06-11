"""Constrained-regression bulk RNA-seq deconvolution (v0) — estimate cell-type fractions.

Given a reference signature matrix (marker genes × cell types, mean expression) and a bulk
expression vector, solve for non-negative cell-type fractions that sum to 1 (Euclidean projection
onto the probability simplex via projected gradient — no scipy dependency).

SCOPE (honest): the METHOD here is general, but v0's only available canine reference (GSE225599,
Ammons) is an IMMUNE atlas — so v0 estimates IMMUNE-cell fractions only. It CANNOT estimate the
endothelial/tumor fraction (no canine HSA scRNA reference exists; HSA tumor cells are themselves
endothelial). A tumor-purity model needs a full-TME reference (e.g. the canine OSA atlas) or a new
HSA dataset — see docs/ENVIRONMENT_LOCK.md. Reference quality, not the solver, is the limiting gap.
"""

from __future__ import annotations

import numpy as np


def build_signature_matrix(
    profiles: dict[str, dict[str, float]],
    genes: list[str] | None = None,
) -> tuple[list[str], list[str], np.ndarray]:
    """profiles: cell_type -> {gene -> mean expression}. Returns (genes, cell_types, S) where
    S[g, c] is cell type c's mean expression of gene g. Genes default to the union across profiles."""
    cell_types = list(profiles)
    if genes is None:
        genes = sorted({g for p in profiles.values() for g in p})
    S = np.array([[profiles[c].get(g, 0.0) for c in cell_types] for g in genes], dtype=float)
    return genes, cell_types, S


def _project_simplex(v: np.ndarray) -> np.ndarray:
    """Euclidean projection of v onto the probability simplex {x >= 0, sum(x) = 1}."""
    u = np.sort(v)[::-1]
    css = np.cumsum(u) - 1.0
    ind = np.arange(1, len(v) + 1)
    cond = u - css / ind > 0
    rho = np.nonzero(cond)[0][-1]
    theta = css[rho] / (rho + 1.0)
    return np.maximum(v - theta, 0.0)


def deconvolve(
    bulk: np.ndarray,
    signature: np.ndarray,
    *,
    iters: int = 8000,
    tol: float = 1e-10,
) -> np.ndarray:
    """Solve argmin_f ||S f - b||^2 s.t. f >= 0, sum(f) = 1 (cell-type fractions) via projected
    gradient descent. bulk: (genes,), signature S: (genes, cell_types). Returns fractions (cell_types,)."""
    S = np.asarray(signature, dtype=float)
    b = np.asarray(bulk, dtype=float)
    n = S.shape[1]
    f = np.full(n, 1.0 / n)
    StS = S.T @ S
    Stb = S.T @ b
    lip = float(np.linalg.norm(S, 2) ** 2) + 1e-9  # gradient step = 1/Lipschitz
    for _ in range(iters):
        grad = StS @ f - Stb
        f_new = _project_simplex(f - grad / lip)
        if np.linalg.norm(f_new - f) < tol:
            f = f_new
            break
        f = f_new
    return f


def deconvolve_sample(
    bulk: dict[str, float],
    genes: list[str],
    cell_types: list[str],
    signature: np.ndarray,
) -> dict[str, float]:
    """Deconvolve one bulk sample (dict gene->value) against a prebuilt signature; returns
    {cell_type: fraction}. Genes absent from the bulk are treated as 0."""
    b = np.array([float(bulk.get(g, 0.0)) for g in genes], dtype=float)
    frac = deconvolve(b, signature)
    return {c: float(round(frac[i], 4)) for i, c in enumerate(cell_types)}


def make_pseudobulk(signature: np.ndarray, fractions: np.ndarray) -> np.ndarray:
    """Synthesize a bulk profile as the fraction-weighted sum of cell-type profiles (S @ f)."""
    return np.asarray(signature, dtype=float) @ np.asarray(fractions, dtype=float)


def concordance_cc(a: np.ndarray, b: np.ndarray) -> float:
    """Lin's concordance correlation coefficient between true (a) and predicted (b) fractions."""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    va, vb = a.var(), b.var()
    cov = ((a - a.mean()) * (b - b.mean())).mean()
    denom = va + vb + (a.mean() - b.mean()) ** 2
    return float(2 * cov / denom) if denom > 0 else 1.0
