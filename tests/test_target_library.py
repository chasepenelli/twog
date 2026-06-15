"""Curated, redock-verified target library (Phase A, Step 0) — the docking-input spend gate.

Offline: precise receptor prep (right chain, named co-crystal ligand → box, waters stripped) and the
curated_docking_config gate (verified entry → config; missing/unverified → None). The REAL redock
verification runs only in scripts/verify_target_library.py, never in CI.
"""

from __future__ import annotations

import pytest

from hsa_research.ingestion_bridge import target_library as tl


def _line(rec, serial, name, resn, chain, resseq, x, y, z, elem):
    """Format one PDB ATOM/HETATM record with exact columns (resName 18-20, chain 22, xyz 31-54)."""
    return (
        f"{rec:<6}{serial:>5} {name:<4}{'':1}{resn:>3} {chain}{resseq:>4}{'':4}"
        f"{x:>8.3f}{y:>8.3f}{z:>8.3f}{1.0:>6.2f}{20.0:>6.2f}{'':10}{elem:>2}"
    )


# chain A protein (2 atoms) + a drug-like ligand LIG (3 atoms, centroid ~ (10,10,10)) + a water + a
# chain B protein atom (must be dropped when we ask for chain A).
_FIXTURE_PDB = "\n".join([
    _line("ATOM", 1, "N", "ALA", "A", 1, 11.0, 13.0, 8.0, "N"),
    _line("ATOM", 2, "CA", "ALA", "A", 1, 12.5, 13.2, 8.4, "C"),
    _line("ATOM", 3, "CA", "GLY", "B", 1, 40.0, 40.0, 40.0, "C"),
    _line("HETATM", 100, "C1", "LIG", "A", 500, 9.0, 9.0, 9.0, "C"),
    _line("HETATM", 101, "C2", "LIG", "A", 500, 10.0, 10.0, 10.0, "C"),
    _line("HETATM", 102, "O1", "LIG", "A", 500, 11.0, 11.0, 11.0, "O"),
    _line("HETATM", 200, "O", "HOH", "A", 600, 50.0, 50.0, 50.0, "O"),
]) + "\n"


def test_prepare_receptor_strips_chain_waters_and_boxes_the_named_ligand():
    receptor, box, cocrystal = tl.prepare_receptor(_FIXTURE_PDB, chain="A", ligand_code="LIG")
    # receptor keeps only chain-A protein, no HETATM/water, no chain B
    assert "ALA A" in receptor and "GLY B" not in receptor
    assert "HOH" not in receptor and "LIG" not in receptor
    # box centered on the LIG centroid (~10,10,10), clamped size
    assert box["center_x"] == pytest.approx(10.0, abs=0.1)
    assert box["center_y"] == pytest.approx(10.0, abs=0.1)
    assert 16.0 <= box["size_x"] <= 30.0
    # co-crystal block holds the ligand atoms (for the redock reference)
    assert cocrystal.count("HETATM") == 3 and "LIG" in cocrystal


def test_prepare_receptor_auto_picks_largest_druglike_when_code_omitted():
    receptor, box, cocrystal = tl.prepare_receptor(_FIXTURE_PDB, chain="A")  # no ligand_code
    assert cocrystal.count("HETATM") == 3 and "LIG" in cocrystal  # LIG is the only/largest drug-like HET


def test_prepare_receptor_raises_when_named_ligand_absent():
    with pytest.raises(ValueError):
        tl.prepare_receptor(_FIXTURE_PDB, chain="A", ligand_code="ZZZ")


def test_curated_docking_config_gate():
    verified = {"entries": {"PIK3CA": {
        "pdb_id": "4JPS", "verified": True, "redock_rmsd": 1.6,
        "receptor_pdb": "ATOM ...\nEND\n",
        "box": {"center_x": 1.0, "center_y": 2.0, "center_z": 3.0, "size_x": 20, "size_y": 20, "size_z": 20},
    }}}
    cfg = tl.curated_docking_config(verified, "pik3ca", ligand_smiles="CCO", ligand_name="alpelisib")
    assert cfg is not None
    assert cfg["ligand_smiles"] == "CCO" and cfg["source_pdb"] == "4JPS" and cfg["center_x"] == 1.0

    # unverified / missing -> None (refuse)
    unver = {"entries": {"PIK3CA": {"verified": False, "receptor_pdb": "x", "box": {}}}}
    assert tl.curated_docking_config(unver, "PIK3CA", ligand_smiles="CCO", ligand_name="x") is None
    assert tl.curated_docking_config(verified, "MTOR", ligand_smiles="CCO", ligand_name="x") is None


def test_shipped_library_loads_and_entries_start_unverified():
    lib = tl.load_target_library()  # the real data/target_library.json
    assert "PIK3CA" in lib["entries"]
    # entries ship unverified (verifier fills them) -> the gate refuses until a real redock passes
    assert lib["entries"]["PIK3CA"]["verified"] is False
