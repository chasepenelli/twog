"""Modal app for twog compute lanes — first lane: the omics-crux CPU analysis.

Runs the omics-review engine on Modal cloud CPU (fits easily in the $30/mo free tier). If no
inline expression is provided, the heavy SRA/GEO pull (load_omics_dataset) runs *remotely* on
Modal where there's network + disk — that's the natural home for it.

Auth: `python -m modal setup` (done; token in ~/.modal.toml, profile chasepenelli).

Run a smoke (first real cloud execution) from the repo root with the package importable:
    PYTHONPATH=src python -m modal run src/hsa_research/ingestion_bridge/modal_app.py

Deploy it so the ModalComputeRunner adapter can call it by name:
    PYTHONPATH=src python -m modal deploy src/hsa_research/ingestion_bridge/modal_app.py

Note: the local-source inclusion (add_local_python_source) is verified on the first `modal run`;
if the remote import of hsa_research fails, run with PYTHONPATH=src as above (the package must be
importable locally for Modal to ship it).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    import modal
except ImportError:  # modal is an optional dep — the rest of twog never imports this module eagerly
    modal = None  # type: ignore[assignment]


if modal is not None:
    # Ship ONLY the self-contained omics_review module (numpy-only) as a flat top-level module —
    # importing it through the hsa_research package would drag in contracts/pydantic/etc., which the
    # slim image doesn't have. The engine has zero twog dependencies, so this is clean and minimal.
    _OMICS_MODULE = Path(__file__).resolve().parent / "omics_review.py"
    image = (
        modal.Image.debian_slim()
        .pip_install("numpy>=1.26,<3")
        .add_local_file(str(_OMICS_MODULE), "/root/omics_review.py")
    )
    app = modal.App("twog-compute")

    @app.function(image=image, cpu=1.0, timeout=900)
    def run_omics_review_remote(config: dict[str, Any]) -> dict[str, Any]:
        """Remote omics-review: runs the real analysis engine on Modal CPU."""
        import sys

        sys.path.insert(0, "/root")
        from omics_review import load_omics_dataset, run_omics_review

        expression = config.get("expression")
        strata = config.get("strata")
        if not expression or not strata:
            expression, strata = load_omics_dataset(config.get("datasets") or [])
        return run_omics_review(
            expression=expression,
            strata=strata,
            signatures=config.get("signatures"),
            direction_hypothesis=config.get("direction_hypothesis", "immunosuppression_higher_in_mutant"),
            min_n_per_stratum=int(config.get("min_n_per_stratum", 5)),
            source_refs=config.get("source_refs"),
        )

    # --- GPU docking lane (gnina) ---------------------------------------------------------------
    # SCAFFOLD: the parse/signal logic (docking.py) is real + tested; the gnina invocation here is
    # best-effort and MUST be verified on the first real GPU run — the binding box, ligand prep, and
    # gnina image tag are the parts to confirm. gnina runs on a modest GPU (T4 is enough for a smoke).
    _DOCKING_MODULE = Path(__file__).resolve().parent / "docking.py"
    gnina_image = (
        # gnina image lacks a Modal-compatible Python -> add_python so Modal can install its client + rdkit
        modal.Image.from_registry("gnina/gnina:v1.3.1", add_python="3.11")  # gnina binary + CUDA (pinned)
        .pip_install("rdkit")  # SMILES -> 3D SDF ligand prep (avoids the old PDB-intermediate failure)
        .add_local_file(str(_DOCKING_MODULE), "/root/docking.py")
    )

    @app.function(image=gnina_image, gpu="A100", timeout=1800)
    def run_gnina_remote(config: dict[str, Any]) -> dict[str, Any]:
        """Remote gnina docking on GPU. config: receptor_pdb, ligand_smiles, target, ligand_name,
        and box params (center_x/y/z, size_x/y/z) OR autobox_ligand_pdb for the search box."""
        import os
        import subprocess
        import sys
        import tempfile

        sys.path.insert(0, "/root")
        from docking import build_docking_result, parse_gnina_output
        from rdkit import Chem
        from rdkit.Chem import AllChem

        workdir = tempfile.mkdtemp()
        receptor = os.path.join(workdir, "receptor.pdb")
        with open(receptor, "w") as fh:
            fh.write(config["receptor_pdb"])
        # ligand prep: SMILES -> 3D SDF with bond orders preserved (RDKit), not a PDB intermediate
        mol = Chem.AddHs(Chem.MolFromSmiles(config["ligand_smiles"]))
        AllChem.EmbedMolecule(mol, randomSeed=0xF00D)
        AllChem.MMFFOptimizeMolecule(mol)
        ligand = os.path.join(workdir, "ligand.sdf")
        writer = Chem.SDWriter(ligand)
        writer.write(mol)
        writer.close()
        cmd = ["gnina", "-r", receptor, "-l", ligand, "-o", os.path.join(workdir, "out.sdf"), "--seed", "0"]
        if all(k in config for k in ("center_x", "center_y", "center_z")):
            cmd += ["--center_x", str(config["center_x"]), "--center_y", str(config["center_y"]),
                    "--center_z", str(config["center_z"]),
                    "--size_x", str(config.get("size_x", 20)), "--size_y", str(config.get("size_y", 20)),
                    "--size_z", str(config.get("size_z", 20))]
        else:
            cmd += ["--autobox_ligand", receptor]  # fallback (whole-receptor box) — refine for real runs
        out_sdf = os.path.join(workdir, "out.sdf")
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=1500)
        modes = parse_gnina_output(proc.stdout)
        result = build_docking_result(
            modes,
            target=config.get("target", "target"),
            ligand=config.get("ligand_name", config.get("ligand_smiles", "ligand")),
            source_refs=config.get("source_refs"),
        )
        # raw artifacts so the parser can be verified against ground truth + poses inspected
        try:
            poses_sdf = open(out_sdf).read()
        except OSError:
            poses_sdf = ""
        result["raw_stdout"] = proc.stdout[-6000:]
        result["poses_sdf"] = poses_sdf
        # native-redock validation: heavy-atom RMSD of best docked pose vs the crystal ligand
        if config.get("reference_pdb_block"):
            try:
                ref = Chem.MolFromPDBBlock(config["reference_pdb_block"], removeHs=True)
                ref = AllChem.AssignBondOrdersFromTemplate(Chem.MolFromSmiles(config["ligand_smiles"]), ref)
                docked = next(iter(Chem.SDMolSupplier(out_sdf, removeHs=True)))  # best pose (mode 1)
                result["metrics"]["pose_rmsd_to_crystal"] = round(AllChem.GetBestRMS(docked, ref), 3)
            except Exception as exc:  # RMSD is best-effort; never fail the dock on it
                result["metrics"]["pose_rmsd_error"] = str(exc)[:200]
        return result

    @app.local_entrypoint()
    def dock() -> None:
        """First real gnina GPU smoke: redock VEGFR2/KDR native ligand 0KF (PDB 3VO3) — gold-standard
        validation (does gnina recover a strong affinity for the known binder?). The receptor is
        prepped locally (protein-only 3VO3) at /tmp/3VO3_receptor.pdb.
        Run: python -m modal run src/hsa_research/ingestion_bridge/modal_app.py::dock"""
        result = run_gnina_remote.remote(
            {
                "receptor_pdb": Path("/tmp/3VO3_receptor.pdb").read_text(),
                "ligand_smiles": "Cc1cc(n(n1)C)C(=O)Nc2cccc(c2)Oc3ccc4nc(cn4n3)NC(=O)C5CC5",
                "ligand_name": "0KF (native VEGFR2 inhibitor)",
                "target": "VEGFR2/KDR (PDB 3VO3)",
                "center_x": 25.61, "center_y": -27.71, "center_z": -13.53,
                "size_x": 22, "size_y": 22, "size_z": 22,
                "source_refs": ["PDB:3VO3"],
            }
        )
        print(f"signal={result['signal']} confidence={result['confidence']}")
        print(result["findings"])
        print("best affinity (kcal/mol):", result.get("metrics", {}).get("best_affinity_kcal_mol"))

    @app.local_entrypoint()
    def dock_pi3k() -> None:
        """gnina GPU dock (A100): test the converged v3 thesis on BOTH targets IN PARALLEL —
        (1) alpelisib redocked into PI3Kα/4JPS = gold-standard native redock + on-target validation;
        (2) alpelisib into VEGFR2/3VO3 = SPECIFICITY CONTROL (alpelisib is PI3Kα-selective, so it
        should bind PI3Kα strongly and VEGFR2 weakly). Receptors prepped locally.
        Run: python -m modal run src/hsa_research/ingestion_bridge/modal_app.py::dock_pi3k"""
        ALPELISIB = "Cc1c(sc(n1)NC(=O)N2CCC[C@H]2C(=O)N)c3ccnc(c3)C(C)(C)C(F)(F)F"
        targets = {
            "PI3Kα/4JPS (on-target, native redock)": {
                "receptor_pdb": Path("/tmp/4JPS_receptor.pdb").read_text(),
                "ligand_smiles": ALPELISIB, "ligand_name": "alpelisib (1LT, native)",
                "target": "PI3Kα / p110α (PDB 4JPS)",
                "center_x": -1.32, "center_y": -9.51, "center_z": 16.95,
                "size_x": 18, "size_y": 18, "size_z": 21,
                "reference_pdb_block": Path("/tmp/4JPS_ligand.pdb").read_text(),  # crystal -> RMSD
                "source_refs": ["PDB:4JPS"],
            },
            "VEGFR2/3VO3 (off-target specificity control)": {
                "receptor_pdb": Path("/tmp/3VO3_receptor.pdb").read_text(),
                "ligand_smiles": ALPELISIB, "ligand_name": "alpelisib (off-target probe)",
                "target": "VEGFR2 / KDR (PDB 3VO3)",
                "center_x": 25.61, "center_y": -27.71, "center_z": -13.53,
                "size_x": 22, "size_y": 22, "size_z": 22,
                "source_refs": ["PDB:3VO3", "alpelisib specificity control"],
            },
        }
        calls = {label: run_gnina_remote.spawn(cfg) for label, cfg in targets.items()}
        for label, call in calls.items():
            r = call.get(); m = r.get("metrics", {})
            print(f"\n=== {label} ===")
            print(f"  signal={r['signal']} confidence={r['confidence']}")
            print(f"  recommended pose affinity={m.get('best_affinity_kcal_mol')} kcal/mol | "
                  f"best Vina={m.get('best_vina_affinity_kcal_mol')} kcal/mol")
            print(f"  CNN pose score={m.get('best_cnn_pose_score')} | CNN affinity(pK)={m.get('best_cnn_affinity')}")
            print(f"  pose_rmsd_to_crystal={m.get('pose_rmsd_to_crystal')} A (err={m.get('pose_rmsd_error')})")

    @app.local_entrypoint()
    def main() -> None:
        """Tiny smoke: a fixture where the PIK3CA-mutant stratum IS immunosuppressed -> 'supports'."""
        genes = [
            "FOXP3", "CTLA4", "IL2RA", "IKZF2", "CD163", "MRC1", "MSR1", "CSF1R",
            "IL10", "TGFB1", "CCL2", "IL6", "CXCL8",
        ]
        mut = [f"m{i}" for i in range(5)]
        wt = [f"w{i}" for i in range(5)]
        expression = {s: {g: 3.0 for g in genes} for s in mut}
        expression.update({s: {g: 1.0 for g in genes} for s in wt})
        strata = {**{s: "mutant" for s in mut}, **{s: "wt" for s in wt}}
        result = run_omics_review_remote.remote(
            {"expression": expression, "strata": strata, "source_refs": ["PRJNA562916", "GSE225599"]}
        )
        print(f"Modal omics result -> signal={result['signal']} confidence={result['confidence']}")
        print(f"  n_mutant={result['metrics']['n_mutant']} n_wt={result['metrics']['n_wt']}")
