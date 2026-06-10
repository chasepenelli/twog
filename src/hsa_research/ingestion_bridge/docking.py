"""Docking lane engine — parse gnina output into a directional result.

The gnina BINARY runs remotely on Modal GPU (see modal_app.run_gnina_remote); this module holds
the dependency-free, fully-tested parts: parsing gnina's stdout into per-mode affinities/CNN
scores, and mapping the best pose to a directional signal (supports/neutral/refutes the candidate's
binding hypothesis). gnina reports affinity in kcal/mol (more negative = stronger predicted binding)
plus a CNN pose score and CNN-predicted affinity. Docking is an ESTIMATE, not measured binding —
the result is framed accordingly.
"""

from __future__ import annotations

import re
from typing import Any

# a gnina results row: leading int (mode), then affinity, CNN pose score, CNN affinity
_MODE_ROW = re.compile(r"^\s*(\d+)\s+(-?\d+\.?\d*)\s+(-?\d+\.?\d*)\s+(-?\d+\.?\d*)")


def parse_gnina_output(stdout: str) -> list[dict[str, float]]:
    """Parse gnina stdout into a list of pose modes (best first), each with affinity + CNN scores."""
    modes: list[dict[str, float]] = []
    for line in stdout.replace("\r\n", "\n").split("\n"):
        m = _MODE_ROW.match(line)
        if not m:
            continue
        modes.append(
            {
                "mode": int(m.group(1)),
                "affinity": float(m.group(2)),  # kcal/mol, more negative = stronger
                "cnn_pose_score": float(m.group(3)),
                "cnn_affinity": float(m.group(4)),
            }
        )
    modes.sort(key=lambda d: d["mode"])
    return modes


def build_docking_result(
    modes: list[dict[str, float]],
    *,
    target: str,
    ligand: str,
    strong_threshold: float = -7.0,
    weak_threshold: float = -5.0,
    source_refs: list[str] | None = None,
) -> dict[str, Any]:
    """Map the best docking pose to a directional result ({findings, signal, confidence, ...}).

    Pre-registered: a strong predicted binding (best affinity <= strong_threshold) SUPPORTS the
    candidate's engagement hypothesis; weak (>= weak_threshold) REFUTES it; in between is neutral.
    """
    if not modes:
        return {
            "findings": f"gnina produced no parseable poses for {ligand} vs {target}.",
            "signal": "neutral",
            "confidence": 0.0,
            "source_refs": source_refs or [],
            "limitations": ["no poses parsed — check gnina inputs/output"],
            "metrics": {"method": "gnina_cnn", "modes": []},
        }
    best = min(modes, key=lambda d: d["affinity"])
    affinity = best["affinity"]
    cnn = best.get("cnn_pose_score", 0.0)
    if affinity <= strong_threshold:
        signal = "supports"
    elif affinity >= weak_threshold:
        signal = "refutes"
    else:
        signal = "neutral"
    # confidence: strength of binding (capped) gated by CNN pose plausibility
    strength = max(0.0, min(1.0, (weak_threshold - affinity) / 5.0)) if affinity < weak_threshold else 0.0
    confidence = round(min(0.85, strength * max(0.0, min(1.0, cnn))), 3)
    return {
        "findings": (
            f"gnina docked {ligand} into {target}: best affinity {affinity:.1f} kcal/mol "
            f"(CNN pose {cnn:.2f}, CNN affinity {best.get('cnn_affinity', float('nan')):.1f}). Signal: {signal}."
        ),
        "signal": signal,
        "confidence": confidence,
        "source_refs": source_refs or [],
        "limitations": [
            "docking is a pose/affinity ESTIMATE, not measured binding",
            "receptor-conformation dependent; single rigid docking run",
            "CNN scoring (gnina) improves ranking but is not ground truth",
        ],
        "metrics": {
            "method": "gnina_cnn",
            "best_affinity_kcal_mol": affinity,
            "best_cnn_pose_score": cnn,
            "n_modes": len(modes),
            "modes": modes,
        },
    }
