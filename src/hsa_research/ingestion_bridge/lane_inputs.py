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


# Lanes whose inputs can be auto-resolved from the network (target structure + ligand SMILES).
_NETWORK_STRUCTURE_LANES = {"docking"}


def _candidate_lane_inputs(candidate: Any, lane: str) -> dict[str, Any] | None:
    metadata = getattr(candidate, "metadata", {}) or {}
    bag = metadata.get("lane_inputs")
    if isinstance(bag, dict) and isinstance(bag.get(lane), dict):
        return bag[lane]
    return None


def _complete(lane: str, cfg: dict[str, Any]) -> bool:
    for required in _REQUIRED_KEY_SETS.get(lane, []):
        if all(cfg.get(key) not in (None, "", [], {}) for key in required):
            return True
    return False


def _resolve_from_network(candidate: Any, lane: str, resolvers: Any) -> dict[str, Any] | None:
    """For a structure lane, build the lane config from a candidate's NAMED target + therapy via the
    injected resolvers (PubChem SMILES + RCSB structure). Returns None on any miss — never fabricates."""
    if lane not in _NETWORK_STRUCTURE_LANES:
        return None
    targets = getattr(candidate, "targets", []) or []
    therapies = getattr(candidate, "candidate_therapies", []) or []
    if not targets or not therapies:
        return None
    smiles = resolvers.compound_smiles(therapies[0])
    structure = resolvers.target_structure(targets[0])
    if not smiles or not structure or not structure.get("receptor_pdb"):
        return None
    return {**structure, "ligand_smiles": smiles, "target": targets[0], "ligand_name": therapies[0]}


def resolve(candidate: Any, lane: str, *, resolvers: Any = None) -> LaneInputResolution:
    """Resolve real lane inputs for a candidate, or report what's missing. Curated inputs
    (candidate.metadata['lane_inputs'][lane]) always win; if absent/incomplete and ``resolvers`` is
    injected, fall back to network resolution from the candidate's named target + therapy."""
    config_key = LANE_CONFIG_KEY.get(lane, lane)
    cfg = _candidate_lane_inputs(candidate, lane)

    if cfg is not None and _complete(lane, cfg):
        return LaneInputResolution(
            lane=lane, config_key=config_key, resolved=True, config=dict(cfg), source="candidate.metadata"
        )

    if resolvers is not None:
        network_cfg = _resolve_from_network(candidate, lane, resolvers)
        if network_cfg is not None:
            return LaneInputResolution(
                lane=lane, config_key=config_key, resolved=True, config=network_cfg, source="network"
            )

    if cfg is None:
        return LaneInputResolution(
            lane=lane,
            config_key=config_key,
            resolved=False,
            missing=[f"candidate.metadata['lane_inputs']['{lane}']"],
            source="candidate.metadata",
        )

    first = _REQUIRED_KEY_SETS.get(lane, [()])[0]
    missing = [key for key in first if cfg.get(key) in (None, "", [], {})]
    return LaneInputResolution(
        lane=lane, config_key=config_key, resolved=False, config=dict(cfg), missing=missing, source="candidate.metadata"
    )
