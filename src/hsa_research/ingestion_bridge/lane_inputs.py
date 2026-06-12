"""Lane input resolution (increment 7) — turn a candidate's curated inputs into real lane config.

Pure and deterministic. A falsification test the planner proposes can only run REAL compute if it
carries the inputs its lane needs (receptor_pdb + ligand_smiles for docking, expression + strata for
omics, a protein sequence + ligand for co-folding). This resolver reads those from operator-curated
inputs attached to the candidate (``candidate.metadata['lane_inputs'][<lane>]``) and returns the lane
config to ride under ``validation_request.metadata[<config_key>]``.

Honest by construction: a candidate with no real inputs resolves to ``resolved=False`` (the test is
still pre-registered, but a real provider will not fabricate a result — it surfaces the gap). Network
resolvers (RCSB structures, PubChem SMILES, the omics SRA seam) are a future source behind this same
interface; nothing here invents data.
"""

from __future__ import annotations

from typing import Any

from .contracts import LaneInputResolution

# validation_type -> the metadata key the lane reads its config from (mirrors _MODAL_LANES / the
# local omics runner). docking/cofolding/omics are the autonomously-runnable lanes today.
LANE_CONFIG_KEY: dict[str, str] = {
    "omics": "omics_review",
    "docking": "docking",
    "cofolding": "cofolding",
    "md": "compute_input",
}

# A lane is resolved if its config carries ANY ONE of these required key-sets.
_REQUIRED_KEY_SETS: dict[str, list[tuple[str, ...]]] = {
    "omics": [("expression", "strata"), ("matrix_path", "strata_path"), ("datasets",)],
    "docking": [("receptor_pdb", "ligand_smiles")],
    "cofolding": [("protein_sequence", "ligand_smiles")],
    "md": [("protein_pdb", "compound_smiles")],
}


def _candidate_lane_inputs(candidate: Any, lane: str) -> dict[str, Any] | None:
    metadata = getattr(candidate, "metadata", {}) or {}
    bag = metadata.get("lane_inputs")
    if isinstance(bag, dict) and isinstance(bag.get(lane), dict):
        return bag[lane]
    return None


def resolve(candidate: Any, lane: str) -> LaneInputResolution:
    """Resolve real lane inputs for a candidate, or report what's missing."""
    config_key = LANE_CONFIG_KEY.get(lane, lane)
    cfg = _candidate_lane_inputs(candidate, lane)
    required_sets = _REQUIRED_KEY_SETS.get(lane, [])

    if cfg is None:
        return LaneInputResolution(
            lane=lane,
            config_key=config_key,
            resolved=False,
            missing=[f"candidate.metadata['lane_inputs']['{lane}']"],
            source="candidate.metadata",
        )

    for required in required_sets:
        if all(cfg.get(key) not in (None, "", [], {}) for key in required):
            return LaneInputResolution(
                lane=lane,
                config_key=config_key,
                resolved=True,
                config=dict(cfg),
                source="candidate.metadata",
            )

    # Inputs are attached but incomplete — report the first required set's gaps.
    first = required_sets[0] if required_sets else ()
    missing = [key for key in first if cfg.get(key) in (None, "", [], {})]
    return LaneInputResolution(
        lane=lane,
        config_key=config_key,
        resolved=False,
        config=dict(cfg),
        missing=missing,
        source="candidate.metadata",
    )
