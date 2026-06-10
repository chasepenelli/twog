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

# A gnina (v1.3) results row is: mode | affinity (kcal/mol) | intramol (kcal/mol) | CNN pose score |
# CNN affinity. Older gnina/smina omit the intramol column. Parse robustly by position regardless of
# column count: affinity is the FIRST float, CNN pose score and CNN affinity are the LAST two.
_MODE_LEAD = re.compile(r"^(\d+)\s+(.*)$")
_NUM = re.compile(r"-?\d+\.\d+|-?\d+")


def parse_gnina_output(stdout: str) -> list[dict[str, float]]:
    """Parse gnina stdout into a list of pose modes (ordered by gnina's mode index = CNN ranking),
    each with affinity + CNN scores. Robust to the presence/absence of the `intramol` column."""
    modes: list[dict[str, float]] = []
    for line in stdout.replace("\r\n", "\n").split("\n"):
        m = _MODE_LEAD.match(line.strip())
        if not m:
            continue
        nums = [float(x) for x in _NUM.findall(m.group(2))]
        if len(nums) < 3:  # need at least affinity + CNN pose + CNN affinity
            continue
        modes.append(
            {
                "mode": int(m.group(1)),
                "affinity": nums[0],  # kcal/mol, more negative = stronger (Vina/smina)
                "intramol": nums[1] if len(nums) >= 4 else None,
                "cnn_pose_score": nums[-2],  # gnina CNN pose score, ~[0,1] (higher = more plausible)
                "cnn_affinity": nums[-1],  # gnina CNN-predicted affinity (pK units, higher = stronger)
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
    # gnina ranks poses by CNN pose score; the recommended pose is the highest-CNN one (usually mode 1).
    # Select by CNN pose score (robust to output order); report the best Vina affinity separately.
    primary = max(modes, key=lambda d: d.get("cnn_pose_score", 0.0))
    affinity = primary["affinity"]
    best_vina_affinity = min(d["affinity"] for d in modes)
    cnn = primary.get("cnn_pose_score", 0.0)
    cnn_aff = primary.get("cnn_affinity", float("nan"))
    if affinity <= strong_threshold:
        signal = "supports"
    elif affinity >= weak_threshold:
        signal = "refutes"
    else:
        signal = "neutral"
    # confidence: strength of binding (capped) gated by CNN pose plausibility of the recommended pose
    strength = max(0.0, min(1.0, (weak_threshold - affinity) / 5.0)) if affinity < weak_threshold else 0.0
    confidence = round(min(0.85, strength * max(0.0, min(1.0, cnn))), 3)
    return {
        "findings": (
            f"gnina docked {ligand} into {target}: recommended pose {affinity:.1f} kcal/mol "
            f"(CNN pose {cnn:.2f}, CNN affinity {cnn_aff:.1f}); best Vina affinity "
            f"{best_vina_affinity:.1f} kcal/mol. Signal: {signal}."
        ),
        "signal": signal,
        "confidence": confidence,
        "source_refs": source_refs or [],
        "limitations": [
            "docking is a pose/affinity ESTIMATE, not measured binding",
            "receptor-conformation dependent; single rigid docking run",
            "raw affinity is a weak selectivity discriminator; CNN scoring improves ranking but is not ground truth",
        ],
        "metrics": {
            "method": "gnina_cnn",
            "best_affinity_kcal_mol": affinity,  # recommended (CNN-top) pose affinity
            "best_vina_affinity_kcal_mol": best_vina_affinity,  # most favorable Vina across modes
            "best_cnn_pose_score": cnn,
            "best_cnn_affinity": cnn_aff,
            "n_modes": len(modes),
            "modes": modes,
        },
    }
