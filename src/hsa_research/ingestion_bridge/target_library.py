"""Curated, redock-verified target library (Phase A, Step 0) — the docking-input spend gate.

Real GPU money is never spent docking against dirty inputs. This module holds a CURATED library of
structures (one verified entry per target) and a PRECISE receptor-prep that, unlike the crude dynamic
resolver (input_resolvers.receptor_and_box: "biggest HETATM"), works from a NAMED chain + co-crystal
ligand. An entry is only usable once `scripts/verify_target_library.py` has redocked its native ligand
and recorded a passing QC verdict (symmetry-corrected RMSD ≤ 2 Å + PoseBusters-valid). The campaign's
docking lane resolves from this library and REFUSES (`resolved=False`) any target that is missing or
unverified — so every docked structure has a proven pocket+protocol. This is the input analog of the
confound/provenance gates. Pure + offline (no network); the one-time verification runs separately.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

from .input_resolvers import _NON_LIGAND_HET

_DEFAULT_PATH = pathlib.Path(__file__).resolve().parents[3] / "data" / "target_library.json"


def load_target_library(path: str | pathlib.Path | None = None) -> dict[str, Any]:
    """Load the target library JSON (entries keyed by UPPER-CASE target name). Returns {} if absent."""
    p = pathlib.Path(path) if path is not None else _DEFAULT_PATH
    if not p.exists():
        return {"version": "target-library-v1", "entries": {}}
    data = json.loads(p.read_text())
    entries = {str(k).upper(): v for k, v in (data.get("entries") or {}).items()}
    return {"version": data.get("version", "target-library-v1"), "entries": entries}


def _selected_box(coords: list[tuple[float, float, float]], pad: float = 8.0) -> dict[str, float]:
    """Docking box = ligand centroid + (extent per axis + 2*pad), clamped to a sane [16, 30] Å."""
    n = len(coords)
    center = [round(sum(c[i] for c in coords) / n, 3) for i in range(3)]
    size = []
    for i in range(3):
        lo = min(c[i] for c in coords)
        hi = max(c[i] for c in coords)
        size.append(round(min(30.0, max(16.0, (hi - lo) + 2 * pad)), 1))
    return {
        "center_x": center[0], "center_y": center[1], "center_z": center[2],
        "size_x": size[0], "size_y": size[1], "size_z": size[2],
    }


def prepare_receptor(
    pdb_text: str, chain: str, ligand_code: str | None = None
) -> tuple[str, dict[str, float], str]:
    """Prepare a dock-ready apo receptor + box + the co-crystal ligand block, PRECISELY.

    Keep only `chain`'s polymer atoms (strip waters / ions / other heteroatoms); derive the docking box
    from the NAMED `cocrystal_ligand_code` (or the largest drug-like HET in the chain if None); return
    the co-crystal ligand's atoms as a PDB block for the redock reference. Returns
    (apo_receptor_pdb, box, cocrystal_pdb_block). Raises ValueError if no usable co-crystal ligand.
    """
    receptor_lines: list[str] = []
    het_groups: dict[tuple, list[str]] = {}  # (resn, chain, resseq) -> raw HETATM lines
    het_coords: dict[tuple, list[tuple[float, float, float]]] = {}
    for line in pdb_text.splitlines():
        rec = line[:6].strip()
        ch = line[21:22]
        if rec == "ATOM" and ch == chain:
            receptor_lines.append(line)
        elif rec == "TER" and ch == chain:
            receptor_lines.append(line)
        elif rec == "HETATM":
            resn = line[17:20].strip()
            if resn in _NON_LIGAND_HET:
                continue
            try:
                xyz = (float(line[30:38]), float(line[38:46]), float(line[46:54]))
            except ValueError:
                continue
            key = (resn, ch, line[22:26])
            het_groups.setdefault(key, []).append(line)
            het_coords.setdefault(key, []).append(xyz)

    if not het_groups:
        raise ValueError(f"no drug-like co-crystal ligand found for chain {chain}")

    if ligand_code:
        code = ligand_code.strip().upper()
        candidates = [k for k in het_groups if k[0].upper() == code]
        if not candidates:
            raise ValueError(f"co-crystal ligand '{ligand_code}' not found in structure")
        # prefer the copy in the requested chain, else any
        key = next((k for k in candidates if k[1] == chain), candidates[0])
    else:
        key = max(het_coords, key=lambda k: len(het_coords[k]))  # largest drug-like HET

    box = _selected_box(het_coords[key])
    cocrystal_block = "\n".join(het_groups[key]) + "\nEND\n"
    receptor_pdb = "\n".join(receptor_lines) + "\nEND\n"
    return receptor_pdb, box, cocrystal_block


def get_entry(library: dict[str, Any], target: str) -> dict[str, Any] | None:
    return (library.get("entries") or {}).get(str(target).upper())


def curated_docking_config(
    library: dict[str, Any], target: str, *, ligand_smiles: str, ligand_name: str
) -> dict[str, Any] | None:
    """The docking lane config for a target from the curated library — ONLY if a verified entry with a
    prepared receptor exists. Returns None (refuse) otherwise. The receptor + box are the verified
    structure; the ligand is the candidate's therapy."""
    entry = get_entry(library, target)
    if not entry or not entry.get("verified") or not entry.get("receptor_pdb") or not entry.get("box"):
        return None
    return {
        "receptor_pdb": entry["receptor_pdb"],
        **entry["box"],
        "ligand_smiles": ligand_smiles,
        "ligand_name": ligand_name,
        "target": target,
        "source_pdb": entry.get("pdb_id"),
        "redock_rmsd": entry.get("redock_rmsd"),
    }
