"""Confound Auditor (increment 3) — the adversarial pre-gate on supporting evidence.

Pure and deterministic: for a `supports` proof capsule, enumerate the known confound classes for its
lane and check the candidate's ledger for a control capsule that addresses each. The verdict is NEVER
"true" — the best attainable status is "survived known confounds". Missing controls => "unauditable"
(cannot accept until run, NOT "clean"); a control that shows the signal vanishes => "refuted".

This is the (3,3) standard made executable: the more autonomously twog runs, the MORE every positive
signal gets adversarially challenged before it can promote. NO I/O here — the service supplies the
capsule + ledger and persists the verdict.
"""

from __future__ import annotations

from typing import Any

# Known confound classes per science lane. Honest by construction: a lane with no well-characterized
# confound set (absent / empty) yields "survived known confounds" vacuously — i.e. "no KNOWN confound
# for this lane", never a claim of correctness. The omics set is the one we have real experience with
# (the tumor-purity catch the whole project turns on).
CONFOUND_TAXONOMY: dict[str, list[str]] = {
    "omics": ["tumor_purity", "batch", "cell_composition", "normalization"],
    "docking": ["pose_instability"],
    "md": ["forcefield_artifact"],
}

_CONTROL_REFUTING_SIGNALS = {"neutral", "refutes"}


def _payload(capsule: Any) -> dict[str, Any]:
    payload = getattr(capsule, "payload", {})
    return payload if isinstance(payload, dict) else {}


def _capsule_lane(capsule: Any) -> str | None:
    payload = _payload(capsule)
    vt = payload.get("validation_type")
    if isinstance(vt, str) and vt:
        return vt
    section = getattr(getattr(capsule, "target", None), "section", None)
    return section if isinstance(section, str) else None


def audit(capsule: Any, ledger_capsules: list[Any], *, taxonomy: dict[str, list[str]] | None = None) -> dict[str, Any]:
    """Return an audit dict {status, applicable, checked, missing, refuted_by, rationale} for a capsule.

    A capsule "controls" confound X iff its payload carries ``controls_confound == X``. A control whose
    own signal is neutral/refutes means the effect vanished under that confound (refuting); a control
    whose signal still supports means the effect survived that confound.
    """
    taxonomy = taxonomy if taxonomy is not None else CONFOUND_TAXONOMY
    payload = _payload(capsule)
    if payload.get("signal") != "supports":
        return {
            "status": "not_required",
            "applicable": [],
            "checked": [],
            "missing": [],
            "refuted_by": [],
            "rationale": "Confound audit applies only to supporting signals.",
        }

    lane = _capsule_lane(capsule)
    applicable = list(taxonomy.get(lane or "", []))
    checked: list[str] = []
    missing: list[str] = []
    refuting: list[str] = []

    for confound in applicable:
        controls = [c for c in ledger_capsules if _payload(c).get("controls_confound") == confound]
        if not controls:
            missing.append(confound)
            continue
        checked.append(confound)
        if any(_payload(c).get("signal") in _CONTROL_REFUTING_SIGNALS for c in controls):
            refuting.append(confound)

    if refuting:
        status = "refuted"
        rationale = f"Controls show the signal vanishes under: {', '.join(refuting)} — likely an artifact, not biology."
    elif missing:
        status = "unauditable"
        rationale = f"Missing controls for: {', '.join(missing)} — cannot accept until these are run."
    else:
        status = "survived_known_confounds"
        rationale = (
            "Survived a surviving control for every known confound of this lane."
            if applicable
            else f"No known confound class for lane '{lane}'; nothing to audit (not a claim of correctness)."
        )

    return {
        "status": status,
        "applicable": applicable,
        "checked": checked,
        "missing": missing,
        "refuted_by": refuting,
        "rationale": rationale,
    }
