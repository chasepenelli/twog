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
# Stage-0 frontier/design lanes: first-class modality lanes whose inputs do not exist yet. They are NOT
# registered as compute lanes (no LaneSpec), so they can never dispatch; the resolver refuses with an
# honest input checklist until real inputs are attached (Stage 1). The required key-sets below are the
# REAL scientific inputs each modality needs — published on the site as the "what it would take" checklist.
_DESIGN_STAGE0_LANES: frozenset[str] = frozenset(
    {"binder_design", "degrader_design", "genome_edit", "cell_therapy", "mrna_vaccine"}
)

LANE_CONFIG_KEY: dict[str, str] = {
    "omics": "omics_review",
    "docking": "docking",
    "cofolding": "cofolding",
    "md": "compute_input",
    # Stage-0 design lanes read their (future) config from an identically-named key.
    "binder_design": "binder_design",
    "degrader_design": "degrader_design",
    "genome_edit": "genome_edit",
    "cell_therapy": "cell_therapy",
    "mrna_vaccine": "mrna_vaccine",
}

# A lane is resolved if its config carries ANY ONE of these required key-sets.
_REQUIRED_KEY_SETS: dict[str, list[tuple[str, ...]]] = {
    "omics": [("expression", "strata"), ("matrix_path", "strata_path"), ("datasets",)],
    "docking": [("receptor_pdb", "ligand_smiles")],
    "cofolding": [("protein_sequence", "ligand_smiles")],
    "md": [("protein_pdb", "compound_smiles")],
    # Stage-0 design-lane input contracts (the honest "what a real test would need"):
    # binder_design — stapled peptides, de-novo binders, the ADC target-engagement arm.
    "binder_design": [("target_structure", "binder_sequence", "interface_hotspots")],
    # degrader_design — PROTAC / peptide-PROTAC / molecular glue (also the ADC payload+linker later).
    "degrader_design": [("target_warhead_smiles", "e3_ligase", "linker", "ternary_target_pose")],
    # genome_edit — base / prime editing at a specific locus.
    "genome_edit": [("target_locus", "guide_spacer", "pam", "edit_window", "edit_type")],
    # cell_therapy — CAR-T / TCR-T against a tumor-selective antigen.
    "cell_therapy": [("target_antigen", "antigen_expression_tumor_vs_normal", "binder_sequence", "construct")],
    # mrna_vaccine — personalized neoantigen vaccine.
    "mrna_vaccine": [("neoantigen_peptides", "hla_alleles", "predicted_binding", "tumor_expression_clonality")],
}


# Lanes auto-resolved from the network. Docking is NOT here: it is gated on the curated, redock-verified
# target library (the spend gate — no dirty RCSB full-text fallback for real GPU). Sequence lanes only.
_NETWORK_STRUCTURE_LANES: set[str] = set()
_NETWORK_SEQUENCE_LANES = {"cofolding"}  # target sequence (UniProt) + ligand SMILES (PubChem)


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
    """Build a lane config from a candidate's NAMED target + therapy via the injected resolvers —
    structure lanes get an RCSB receptor, sequence lanes a UniProt sequence, both a PubChem SMILES.
    Returns None on any miss — never fabricates."""
    if lane not in _NETWORK_STRUCTURE_LANES and lane not in _NETWORK_SEQUENCE_LANES:
        return None
    targets = getattr(candidate, "targets", []) or []
    therapies = getattr(candidate, "candidate_therapies", []) or []
    if not targets or not therapies:
        return None
    smiles = resolvers.compound_smiles(therapies[0])
    if not smiles:
        return None
    common = {"ligand_smiles": smiles, "target": targets[0], "ligand_name": therapies[0]}
    if lane in _NETWORK_STRUCTURE_LANES:
        structure = resolvers.target_structure(targets[0])
        if not structure or not structure.get("receptor_pdb"):
            return None
        return {**structure, **common}
    sequence = resolvers.protein_sequence(targets[0])
    if not sequence:
        return None
    return {"protein_sequence": sequence, **common}


def _resolve_docking_from_library(
    candidate: Any, config_key: str, resolvers: Any, target_library: Any
) -> LaneInputResolution:
    """Docking spend gate: resolve the receptor + box from the curated, redock-VERIFIED target library
    (by candidate.targets[0]) + the candidate's therapy SMILES (PubChem). Refuse (resolved=False) if the
    target is missing/unverified or the ligand can't be resolved — never a dirty full-text fallback."""
    from . import target_library as _tl

    lib = target_library if target_library is not None else _tl.load_target_library()
    targets = getattr(candidate, "targets", []) or []
    therapies = getattr(candidate, "candidate_therapies", []) or []

    def refuse(reason: str, missing: list[str]) -> LaneInputResolution:
        # `missing` is config-KEY NAMES only (a clean checklist); the human explanation rides `notes`.
        return LaneInputResolution(
            lane="docking", config_key=config_key, resolved=False, missing=missing,
            notes=reason, source="curated_library",
        )

    if not targets:
        return refuse("candidate.targets is empty", ["target"])
    target = targets[0]
    entry = _tl.get_entry(lib, target)
    if not entry or not entry.get("verified"):
        return refuse(f"no redock-verified target_library entry for '{target}'", ["receptor_pdb"])
    if not therapies:
        return refuse("candidate.candidate_therapies is empty", ["ligand_smiles"])
    if resolvers is None:
        return refuse("no SMILES resolver (PubChem) injected", ["ligand_smiles"])
    smiles = resolvers.compound_smiles(therapies[0])
    if not smiles:
        return refuse(f"PubChem SMILES not found for therapy '{therapies[0]}'", ["ligand_smiles"])
    config = _tl.curated_docking_config(lib, target, ligand_smiles=smiles, ligand_name=therapies[0])
    if config is None:
        return refuse(f"verified library entry for '{target}' is missing a prepared receptor/box", ["receptor_pdb"])
    return LaneInputResolution(
        lane="docking", config_key=config_key, resolved=True, config=config, source="curated_library"
    )


def resolve(candidate: Any, lane: str, *, resolvers: Any = None, target_library: Any = None) -> LaneInputResolution:
    """Resolve real lane inputs for a candidate, or report what's missing. Curated per-candidate inputs
    (candidate.metadata['lane_inputs'][lane]) always win. DOCKING then resolves ONLY from the curated,
    redock-verified target library (the spend gate — refuse if unverified). Sequence lanes (cofolding)
    fall back to network resolution from the candidate's named target + therapy."""
    config_key = LANE_CONFIG_KEY.get(lane, lane)
    cfg = _candidate_lane_inputs(candidate, lane)

    if cfg is not None and _complete(lane, cfg):
        return LaneInputResolution(
            lane=lane, config_key=config_key, resolved=True, config=dict(cfg), source="candidate.metadata"
        )

    # Stage-0 frontier/design lanes: refuse (resolved=False) with the exact real-input checklist and an
    # honest note. The lane is not registered as a runner, so even a hypothetical resolved=True never
    # dispatches — this branch exists so the gap reads as "design concept, not tested", never as evidence.
    if lane in _DESIGN_STAGE0_LANES:
        required = _REQUIRED_KEY_SETS[lane][0]
        present = dict(cfg) if cfg else {}
        missing = [key for key in required if present.get(key) in (None, "", [], {})]
        return LaneInputResolution(
            lane=lane,
            config_key=config_key,
            resolved=False,
            config=present,
            missing=missing,
            notes=(
                f"Stage-0 design concept: no real {lane} inputs attached yet — not tested, not evidence. "
                f"Attach the listed inputs to make it runnable (Stage 1)."
            ),
            source="frontier_design_stage0",
        )

    if lane == "docking":
        return _resolve_docking_from_library(candidate, config_key, resolvers, target_library)

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

    # Report the keyset CLOSEST to satisfied (fewest missing keys) so the operator sees the easiest
    # completion path — never blindly the first keyset (omics has 3 alternatives: expression+strata,
    # matrix_path+strata_path, or datasets).
    keysets = _REQUIRED_KEY_SETS.get(lane) or [()]
    missing = min(
        ([key for key in ks if cfg.get(key) in (None, "", [], {})] for ks in keysets),
        key=len,
    )
    return LaneInputResolution(
        lane=lane, config_key=config_key, resolved=False, config=dict(cfg), missing=missing, source="candidate.metadata"
    )
