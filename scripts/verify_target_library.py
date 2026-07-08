"""Verify the curated docking target library — the one-time docking-input SPEND GATE (Phase A, Step 0c).

For each target in data/target_library.json this:
  1. fetches the curated PDB from RCSB,
  2. prepares the receptor PRECISELY (named chain, waters/ions stripped, box from the NAMED co-crystal
     ligand) via target_library.prepare_receptor,
  3. resolves the NATIVE co-crystal ligand SMILES (from the RCSB CCD ideal SDF, by ligand code),
  4. redocks that native ligand into its own pocket on Modal GPU (reuses compute_runners._call_modal_gnina),
  5. runs the QC GATE locally: symmetry-corrected RMSD (spyrmsd) <= 2.0 A AND PoseBusters-valid,
  6. on PASS, writes back verified=true + redock_rmsd + the prepared receptor_pdb/box/ligand code+SMILES
     so the campaign resolver can use the entry; on FAIL, leaves verified=false (re-curate the PDB).

This costs real GPU money (~$0.10/target on A100) — CONFIRM before running. It is NOT run in CI.
Deps (local QC): pip install spyrmsd posebusters

    PYTHONPATH=src python scripts/verify_target_library.py [TARGET ...]   # default: all entries
"""

from __future__ import annotations

import json
import pathlib
import sys
import tempfile
import urllib.request

from hsa_research.ingestion_bridge import target_library as tl
from hsa_research.ingestion_bridge.compute_runners import _call_modal_gnina
from hsa_research.ingestion_bridge.input_resolvers import _NON_LIGAND_HET

_LIB_PATH = pathlib.Path(__file__).resolve().parents[1] / "data" / "target_library.json"


def _fetch(url: str, *, binary: bool = False) -> str | bytes:
    with urllib.request.urlopen(url, timeout=60) as fh:
        return fh.read() if binary else fh.read().decode("utf-8")


def _fetch_pdb(pdb_id: str) -> str:
    return _fetch(f"https://files.rcsb.org/download/{pdb_id}.pdb")


def _detect_cocrystal_code(pdb_text: str, chain: str) -> str:
    """The native co-crystal ligand = the largest drug-like HET group in `chain` (matches the box that
    prepare_receptor builds when ligand_code is None). Returns its 3-letter PDB chemical-component code."""
    groups: dict[tuple, int] = {}
    for line in pdb_text.splitlines():
        if line[:6].strip() != "HETATM":
            continue
        resn = line[17:20].strip()
        if resn in _NON_LIGAND_HET or line[21:22] != chain:
            continue
        groups[(resn, line[22:26])] = groups.get((resn, line[22:26]), 0) + 1
    if not groups:
        raise ValueError(f"no drug-like co-crystal ligand in chain {chain}")
    return max(groups, key=lambda k: groups[k])[0]


def _ccd_smiles(code: str) -> str:
    """Native ligand SMILES (with correct bond orders) from the RCSB CCD ideal SDF, by component code."""
    from rdkit import Chem

    sdf = _fetch(f"https://files.rcsb.org/ligands/download/{code}_ideal.sdf")
    mol = Chem.MolFromMolBlock(sdf, removeHs=True)
    if mol is None:
        raise ValueError(f"could not parse CCD ideal SDF for ligand {code}")
    return Chem.MolToSmiles(mol)


def _redock_qc(receptor_pdb: str, cocrystal_block: str, ligand_smiles: str, poses_sdf: str) -> dict:
    """Local QC on the best docked pose: symmetry-corrected RMSD to the crystal pose (spyrmsd) AND
    PoseBusters validity. Returns {rmsd, posebusters_ok, passed, detail}."""
    from posebusters import PoseBusters
    from rdkit import Chem
    from rdkit.Chem import AllChem
    from spyrmsd import rmsd as spy_rmsd
    from spyrmsd.molecule import Molecule

    # crystal reference: assign bond orders from the native SMILES so atoms match the docked mol
    ref = Chem.MolFromPDBBlock(cocrystal_block, removeHs=True)
    ref = AllChem.AssignBondOrdersFromTemplate(Chem.MolFromSmiles(ligand_smiles), ref)

    # SDMolSupplier needs a file; write the poses + take mode-1 (best) pose, then run the QC checks
    with tempfile.TemporaryDirectory() as d:
        pose_path = pathlib.Path(d) / "poses.sdf"
        pose_path.write_text(poses_sdf)
        docked = next(iter(Chem.SDMolSupplier(str(pose_path), removeHs=True)))
        best_path = pathlib.Path(d) / "best.sdf"
        w = Chem.SDWriter(str(best_path))
        w.write(docked)
        w.close()

        sr = Molecule.from_rdkit(docked)
        sm = Molecule.from_rdkit(ref)
        symm = spy_rmsd.symmrmsd(
            sr.coordinates, sm.coordinates, sr.atomicnums, sm.atomicnums, sr.adjacency_matrix, sm.adjacency_matrix
        )
        rmsd_val = round(float(symm), 3)

        buster = PoseBusters(config="mol")  # intramolecular validity of the docked pose (geometry, clashes)
        df = buster.bust([str(best_path)], None, None, full_report=False)
        pb_ok = bool(df.all(axis=1).iloc[0]) if not df.empty else False

    passed = rmsd_val <= 2.0 and pb_ok
    return {"rmsd": rmsd_val, "posebusters_ok": pb_ok, "passed": passed}


def main() -> None:
    library = tl.load_target_library(_LIB_PATH)
    entries = library.get("entries") or {}
    wanted = [t.upper() for t in sys.argv[1:]] or list(entries)

    for target in wanted:
        entry = entries.get(target)
        if entry is None:
            print(f"!! {target}: not in library — skip")
            continue
        pdb_id, chain = entry["pdb_id"], entry["chain"]
        print(f"\n=== {target} ({pdb_id}, chain {chain}) ===", flush=True)
        try:
            pdb_text = _fetch_pdb(pdb_id)
            code = entry.get("cocrystal_ligand_code") or _detect_cocrystal_code(pdb_text, chain)
            receptor, box, cocrystal_block = tl.prepare_receptor(pdb_text, chain, code)
            smiles = entry.get("cocrystal_smiles") or _ccd_smiles(code)
            print(f"  native ligand: {code}  SMILES={smiles}")
            print(f"  receptor: {receptor.count(chr(10))} lines  box center=({box['center_x']},{box['center_y']},{box['center_z']})")
        except Exception as exc:  # curation/prep failure — leave unverified
            print(f"  PREP FAILED: {exc} -> verified stays false")
            continue

        config = {
            "receptor_pdb": receptor, "ligand_smiles": smiles, **box,
            "target": target, "ligand_name": f"{code} (native redock)",
            "reference_pdb_block": cocrystal_block, "source_refs": [f"PDB:{pdb_id}"],
        }
        print("  redocking native ligand on Modal A100 ...", flush=True)
        result = _call_modal_gnina(config)
        gnina_rmsd = result.get("metrics", {}).get("pose_rmsd_to_crystal")
        print(f"  gnina pose_rmsd_to_crystal = {gnina_rmsd} A; running spyrmsd + PoseBusters QC ...", flush=True)

        qc = _redock_qc(receptor, cocrystal_block, smiles, result.get("poses_sdf", ""))
        verdict = "PASS" if qc["passed"] else "FAIL"
        print(f"  QC {verdict}: symm_rmsd={qc['rmsd']} A (<=2.0)  posebusters_ok={qc['posebusters_ok']}")

        entry.update({
            "cocrystal_ligand_code": code, "cocrystal_smiles": smiles,
            "box": box, "receptor_pdb": receptor if qc["passed"] else None,
            "redock_rmsd": qc["rmsd"], "verified": bool(qc["passed"]),
        })
        if not qc["passed"]:
            print(f"  -> verified=false (re-curate {pdb_id} or pick another structure for {target})")

    _LIB_PATH.write_text(json.dumps({"version": library.get("version", "target-library-v1"),
                                     "_doc": json.loads(_LIB_PATH.read_text()).get("_doc", ""),
                                     "entries": entries}, indent=2) + "\n")
    n_ok = sum(1 for e in entries.values() if e.get("verified"))
    print(f"\nWrote {_LIB_PATH.name}: {n_ok}/{len(entries)} targets verified.")


if __name__ == "__main__":
    main()
