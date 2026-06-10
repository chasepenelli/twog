"""Omics-review analysis engine — the PIK3CA-mutant immunosuppression crux lane.

Resolves the developed hypothesis's cheapest-first crux: does the PIK3CA-mutant subset of canine
splenic HSA carry an immunosuppressive TME signature (Tregs / M2-TAMs / immunosuppressive
cytokines + the Banerjee PIK3CA→immune program)? See docs/OMICS_CRUX_LANE.md and DATA_SOURCES.md.

Design: the ANALYSIS is real and fully tested here, operating on *provided* expression + strata.
The multi-GB SRA/GEO data pull (Megquier PRJNA562916 + Ammons GSE225599) is a clearly-marked seam
(``load_omics_dataset``) you wire at deploy time — the engine itself never downloads anything.

Method (v1, open, no licensing): per-gene z-score across samples, then a mean-z "module score" per
signature (ssGSEA-style); Mann-Whitney U (mutant vs WT) per signature with Benjamini-Hochberg
correction; a PRE-REGISTERED directional signal. ~7-mutant underpowering is honestly reflected in
the confidence and can yield a `neutral` ("can't tell yet") verdict — which is the correct outcome.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np

# Default canine HSA immunosuppression signatures (real gene symbols). The PIK3CA_immune_program
# set is the Banerjee 2024 (PMID 39709507) PIK3CA->immune-cytokine program (CXCL8 = IL-8).
DEFAULT_IMMUNOSUPPRESSION_SIGNATURES: dict[str, list[str]] = {
    "Treg": ["FOXP3", "CTLA4", "IL2RA", "IKZF2"],
    "M2_TAM": ["CD163", "MRC1", "MSR1", "CSF1R"],
    "immunosuppressive_cytokines": ["IL10", "TGFB1", "CCL2"],
    "PIK3CA_immune_program": ["IL6", "CXCL8", "CCL2"],
}
# A contrast set (not immunosuppression) — useful to show direction specificity, not scored as suppression.
DEFAULT_CONTRAST_SIGNATURES: dict[str, list[str]] = {
    "cytotoxic_T": ["CD8A", "GZMB", "PRF1", "IFNG"],
}


def _zscore_by_gene(
    expression: dict[str, dict[str, float]], genes: list[str], samples: list[str]
) -> dict[str, dict[str, float]]:
    z: dict[str, dict[str, float]] = {s: {} for s in samples}
    for gene in genes:
        vals = np.array([float(expression[s].get(gene, 0.0)) for s in samples], dtype=float)
        sd = vals.std()
        zr = np.zeros_like(vals) if sd == 0 else (vals - vals.mean()) / sd
        for sample, value in zip(samples, zr):
            z[sample][gene] = float(value)
    return z


def score_signatures(
    expression: dict[str, dict[str, float]], signatures: dict[str, list[str]]
) -> tuple[dict[str, dict[str, float]], dict[str, list[str]]]:
    """Mean-z module score per signature per sample. Returns (scores, genes_used_per_signature)."""
    samples = list(expression)
    present = {
        gene
        for sig in signatures.values()
        for gene in sig
        if any(gene in expression[s] for s in samples)
    }
    z = _zscore_by_gene(expression, sorted(present), samples)
    used = {name: [g for g in sig if g in present] for name, sig in signatures.items()}
    scores: dict[str, dict[str, float]] = {s: {} for s in samples}
    for name, genes in used.items():
        for sample in samples:
            scores[sample][name] = float(np.mean([z[sample][g] for g in genes])) if genes else 0.0
    return scores, used


def mann_whitney(group1: list[float], group2: list[float]) -> dict[str, float]:
    """Two-sided Mann-Whitney U (normal approx, tie-corrected). auc>0.5 ⇒ group1 tends higher."""
    n1, n2 = len(group1), len(group2)
    if n1 == 0 or n2 == 0:
        return {"u": float("nan"), "p": 1.0, "auc": 0.5, "n1": n1, "n2": n2, "z": 0.0}
    combined = np.array(list(group1) + list(group2), dtype=float)
    order = combined.argsort(kind="mergesort")
    ranks = np.empty(len(combined), dtype=float)
    ranks[order] = np.arange(1, len(combined) + 1, dtype=float)
    # average ranks within ties
    sorted_vals = combined[order]
    i = 0
    while i < len(sorted_vals):
        j = i
        while j + 1 < len(sorted_vals) and sorted_vals[j + 1] == sorted_vals[i]:
            j += 1
        if j > i:
            avg = (i + 1 + j + 1) / 2.0
            for k in range(i, j + 1):
                ranks[order[k]] = avg
        i = j + 1
    r1 = float(ranks[:n1].sum())
    u1 = r1 - n1 * (n1 + 1) / 2.0
    mu = n1 * n2 / 2.0
    # tie-corrected standard deviation
    _, counts = np.unique(combined, return_counts=True)
    tie_term = float(np.sum(counts**3 - counts))
    n = n1 + n2
    sigma_sq = (n1 * n2 / 12.0) * ((n + 1) - tie_term / (n * (n - 1))) if n > 1 else 0.0
    sigma = math.sqrt(sigma_sq) if sigma_sq > 0 else 0.0
    z = (u1 - mu) / sigma if sigma > 0 else 0.0
    p = 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(z) / math.sqrt(2.0))))
    return {"u": u1, "p": min(1.0, p), "auc": u1 / (n1 * n2), "n1": n1, "n2": n2, "z": z}


def benjamini_hochberg(pvals: list[float]) -> list[float]:
    if not pvals:
        return []
    p = np.array(pvals, dtype=float)
    n = len(p)
    order = p.argsort()
    adj = p[order] * n / np.arange(1, n + 1, dtype=float)
    adj = np.minimum.accumulate(adj[::-1])[::-1]
    out = np.empty(n, dtype=float)
    out[order] = np.clip(adj, 0.0, 1.0)
    return out.tolist()


def run_omics_review(
    *,
    expression: dict[str, dict[str, float]],
    strata: dict[str, str],
    signatures: dict[str, list[str]] | None = None,
    direction_hypothesis: str = "immunosuppression_higher_in_mutant",
    min_n_per_stratum: int = 5,
    alpha: float = 0.05,
    source_refs: list[str] | None = None,
) -> dict[str, Any]:
    """Run the pre-registered differential-composition analysis and emit a directional result
    ({findings, signal, confidence, source_refs, limitations, metrics}) ready for the capsule.

    strata maps sample_id -> "mutant" | "wt". Pre-registered: immunosuppression signatures are
    expected HIGHER in the mutant stratum; the signal/confidence logic below is fixed up front.
    """
    signatures = signatures or DEFAULT_IMMUNOSUPPRESSION_SIGNATURES
    scores, used = score_signatures(expression, signatures)
    mutant = [s for s, v in strata.items() if v == "mutant" and s in scores]
    wt = [s for s, v in strata.items() if v == "wt" and s in scores]
    n_mut, n_wt = len(mutant), len(wt)

    per_sig: dict[str, dict[str, float]] = {}
    names, pvals = [], []
    for name in signatures:
        res = mann_whitney([scores[s][name] for s in mutant], [scores[s][name] for s in wt])
        per_sig[name] = res
        names.append(name)
        pvals.append(res["p"])
    for name, q in zip(names, benjamini_hochberg(pvals)):
        per_sig[name]["q"] = q

    underpowered = n_mut < min_n_per_stratum or n_wt < min_n_per_stratum
    higher = [n for n in signatures if per_sig[n]["auc"] > 0.5]
    sig_higher = [n for n in higher if per_sig[n].get("q", 1.0) < alpha]
    sig_lower = [n for n in signatures if per_sig[n]["auc"] < 0.5 and per_sig[n].get("q", 1.0) < alpha]

    # pre-registered signal logic
    if sig_higher and not underpowered:
        signal, conf_base = "supports", 0.7
    elif sig_lower and not underpowered:
        signal, conf_base = "refutes", 0.6
    else:
        signal, conf_base = "neutral", 0.25  # underpowered, or no significant difference
    # confidence: scale by power and best q; capped (single cohort, bulk deconvolution)
    power = min(1.0, min(n_mut, n_wt) / max(1, min_n_per_stratum))
    best_q = min((per_sig[n].get("q", 1.0) for n in signatures), default=1.0)
    confidence = round(min(0.9, conf_base * power * (1.0 - min(best_q, 1.0))), 3)
    if signal == "neutral":
        confidence = round(min(confidence, 0.3), 3)

    direction = "higher" if higher else "not higher"
    findings = (
        f"PIK3CA-mutant (n={n_mut}) vs WT (n={n_wt}) HSA: immunosuppression signatures are "
        f"{direction} in the mutant stratum "
        f"({len(sig_higher)}/{len(signatures)} significant at q<{alpha}). Signal: {signal}."
    )
    limitations = [
        "single bulk RNA-seq cohort; deconvolution by mean-z marker scoring (coarser than CIBERSORTx)",
        f"underpowered: n_mutant={n_mut}, n_wt={n_wt} (min per stratum {min_n_per_stratum})"
        if underpowered
        else "power adequate for a first pass; replication still recommended",
        "RNA-seq:WES sample matching must be verified in the source data",
    ]
    return {
        "findings": findings,
        "signal": signal,
        "confidence": confidence,
        "source_refs": source_refs or [],
        "limitations": limitations,
        "metrics": {
            "method": "ssgsea_meanz",
            "test": "mann_whitney_bh",
            "direction_hypothesis": direction_hypothesis,
            "n_mutant": n_mut,
            "n_wt": n_wt,
            "underpowered": underpowered,
            "signatures_used": used,
            "per_signature": per_sig,
            "significant_higher": sig_higher,
        },
    }


def load_omics_dataset(accessions: list[str]) -> tuple[dict[str, dict[str, float]], dict[str, str]]:
    """SEAM (deploy-time, 3b): pull real expression + mutation strata from SRA/GEO.

    Not wired in-repo — fetching multi-GB SRA (e.g. PRJNA562916) + the Ammons reference (GSE225599)
    must run where there's network + disk (your machine / a CPU instance), via pysradb/GEOparse.
    The engine above runs on the returned (expression, strata); tests feed fixtures directly.
    """
    raise NotImplementedError(
        "Real omics data pull is not wired in-repo. Provide `expression`+`strata` inline (the engine "
        "runs on them), or implement this loader with pysradb/GEOparse for "
        f"{accessions} where network+disk are available. See docs/DATA_SOURCES.md."
    )
