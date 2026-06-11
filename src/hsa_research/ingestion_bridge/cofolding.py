"""Co-folding lane engine — parse Boltz-2 output into a directional binding result.

Boltz-2 co-folds a protein + ligand into a predicted complex AND predicts binding affinity. The
heavy model runs remotely on GPU (see modal_app.run_boltz_remote); this module holds the
dependency-free, tested parts: reading Boltz's confidence + affinity JSON and mapping them to a
directional signal (supports/neutral/refutes the binding hypothesis).

Boltz confidence JSON carries `confidence_score`, `ptm`, `iptm` (interface pTM — the key metric for
a complex), `ligand_iptm`, `complex_plddt`. Boltz-2 also emits an affinity JSON with
`affinity_pred_value` (predicted log(IC50) in µM units; LOWER = stronger) and
`affinity_probability_binary` (P(binder), 0-1). Co-folding is a structure/affinity ESTIMATE, framed
accordingly.
"""

from __future__ import annotations

from typing import Any


def _f(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_boltz_outputs(confidence: dict[str, Any], affinity: dict[str, Any] | None = None) -> dict[str, Any]:
    """Pull the scores we score on from Boltz's confidence (+ optional Boltz-2 affinity) JSON."""
    scores: dict[str, Any] = {
        "confidence_score": _f(confidence.get("confidence_score")),
        "iptm": _f(confidence.get("iptm")),
        "ptm": _f(confidence.get("ptm")),
        "ligand_iptm": _f(confidence.get("ligand_iptm")),
        "complex_plddt": _f(confidence.get("complex_plddt")),
    }
    if affinity:
        scores["affinity_pred_value"] = _f(affinity.get("affinity_pred_value"))
        scores["affinity_probability_binary"] = _f(affinity.get("affinity_probability_binary"))
    return scores


def build_cofolding_result(
    scores: dict[str, Any],
    *,
    target: str,
    ligand: str,
    iptm_strong: float = 0.6,
    iptm_weak: float = 0.4,
    binder_prob_strong: float = 0.5,
    binder_prob_weak: float = 0.3,
    source_refs: list[str] | None = None,
) -> dict[str, Any]:
    """Map Boltz scores to a directional result. Pre-registered: a confident interface
    (iptm >= iptm_strong) with a likely binder (P(binder) >= binder_prob_strong) SUPPORTS the
    binding hypothesis; a poor interface (iptm <= iptm_weak) or unlikely binder REFUTES it."""
    # interface confidence is the primary structural metric; fall back to the overall score
    struct = scores.get("iptm")
    if struct is None:
        struct = scores.get("confidence_score")
    binder_prob = scores.get("affinity_probability_binary")

    if struct is None:
        return {
            "findings": f"Boltz produced no parseable confidence for {ligand} vs {target}.",
            "signal": "neutral",
            "confidence": 0.0,
            "source_refs": source_refs or [],
            "limitations": ["no confidence parsed — check Boltz inputs/output"],
            "metrics": {"method": "boltz2_cofolding", "scores": scores},
        }

    likely_binder = binder_prob is None or binder_prob >= binder_prob_strong
    unlikely_binder = binder_prob is not None and binder_prob < binder_prob_weak
    if struct >= iptm_strong and likely_binder:
        signal = "supports"
    elif struct <= iptm_weak or unlikely_binder:
        signal = "refutes"
    else:
        signal = "neutral"

    # confidence: interface strength gated by binder probability (capped, like the docking lane)
    strength = max(0.0, min(1.0, (struct - iptm_weak) / (1.0 - iptm_weak))) if struct > iptm_weak else 0.0
    prob_gate = binder_prob if binder_prob is not None else 1.0
    confidence = round(min(0.85, strength * max(0.0, min(1.0, prob_gate))), 3)

    aff = scores.get("affinity_pred_value")
    aff_txt = f", affinity_pred {aff:.2f} (log µM, lower=stronger)" if aff is not None else ""
    prob_txt = f", P(binder)={binder_prob:.2f}" if binder_prob is not None else ""
    return {
        "findings": (
            f"Boltz-2 co-folded {ligand} with {target}: interface iptm "
            f"{struct:.2f}{prob_txt}{aff_txt}. Signal: {signal}."
        ),
        "signal": signal,
        "confidence": confidence,
        "source_refs": source_refs or [],
        "limitations": [
            "co-folding is a structure/affinity ESTIMATE, not measured binding",
            "iptm/affinity are model confidences; high confidence is not proof of binding",
            "single predicted complex; no conformational ensemble or experimental validation",
        ],
        "metrics": {
            "method": "boltz2_cofolding",
            "iptm": scores.get("iptm"),
            "confidence_score": scores.get("confidence_score"),
            "ligand_iptm": scores.get("ligand_iptm"),
            "affinity_pred_value": aff,
            "affinity_probability_binary": binder_prob,
            "scores": scores,
        },
    }
