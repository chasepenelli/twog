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

    # --- Real-GPU checkpointing lane: OpenMM MD with durable checkpoint to a Modal Volume ---------
    # MD is the canonical hours-long GPU job (and our MD lane). OpenMM has true binary checkpoint/
    # restart; a persistent Modal Volume gives durable cross-invocation state — so a job can pause at
    # 40%, be handed off, and resume from the exact checkpoint. The system here is a deterministic
    # harmonic-well harness (no force-field parametrization) — it proves the GPU+checkpoint
    # INFRASTRUCTURE for real, not a scientific MD result.
    md_checkpoint_volume = modal.Volume.from_name("twog-md-checkpoints", create_if_missing=True)
    md_image = (
        modal.Image.from_registry("nvidia/cuda:12.4.1-runtime-ubuntu22.04", add_python="3.11")
        .pip_install("openmm")
    )

    @app.function(image=md_image, gpu="T4", timeout=1800, volumes={"/ckpt": md_checkpoint_volume})
    def run_md_checkpoint_remote(config: dict[str, Any]) -> dict[str, Any]:
        """Run a bounded chunk of GPU MD (CUDA platform), persisting an OpenMM checkpoint to the
        Volume. Returns 'paused' (progress<1) or 'completed'. resume=True loads the durable
        checkpoint and continues — true cross-invocation pause/resume. config: job_id, total_steps,
        steps_per_chunk, n_particles, resume."""
        import json
        import math
        import os

        import openmm as mm
        from openmm import unit

        job_id = str(config["job_id"])
        total_steps = int(config.get("total_steps", 30000))
        chunk = int(config.get("steps_per_chunk", 10000))
        n = int(config.get("n_particles", 2000))
        ckpt_dir = f"/ckpt/{job_id}"
        os.makedirs(ckpt_dir, exist_ok=True)
        ckpt_path = os.path.join(ckpt_dir, "state.chk")
        meta_path = os.path.join(ckpt_dir, "meta.json")

        # deterministic harmonic-well system of n particles (real MD physics, no parametrization)
        system = mm.System()
        for _ in range(n):
            system.addParticle(39.948 * unit.amu)
        force = mm.CustomExternalForce("0.5*k*((x-x0)^2+(y-y0)^2+(z-z0)^2)")
        force.addGlobalParameter("k", 100.0)
        for p in ("x0", "y0", "z0"):
            force.addPerParticleParameter(p)
        side = math.ceil(n ** (1 / 3))
        positions = []
        for i in range(n):
            x = (i % side) * 0.3
            y = ((i // side) % side) * 0.3
            z = (i // (side * side)) * 0.3
            force.addParticle(i, [x, y, z])
            positions.append(mm.Vec3(x, y, z) * unit.nanometer)
        system.addForce(force)
        integrator = mm.LangevinMiddleIntegrator(
            300 * unit.kelvin, 1.0 / unit.picosecond, 0.002 * unit.picoseconds
        )
        platform = mm.Platform.getPlatformByName("CUDA")  # require a real GPU platform
        context = mm.Context(system, integrator, platform)

        md_checkpoint_volume.reload()
        resume = bool(config.get("resume")) and os.path.exists(ckpt_path)
        if resume:
            with open(ckpt_path, "rb") as fh:
                context.loadCheckpoint(fh.read())
            steps_done = int(json.load(open(meta_path)).get("steps_done", 0))
        else:
            context.setPositions(positions)
            context.setVelocitiesToTemperature(300 * unit.kelvin, 12345)
            steps_done = 0

        to_run = max(0, min(chunk, total_steps - steps_done))
        integrator.step(to_run)
        steps_done += to_run
        pe = context.getState(getEnergy=True).getPotentialEnergy().value_in_unit(
            unit.kilojoule_per_mole
        )
        with open(ckpt_path, "wb") as fh:
            fh.write(context.createCheckpoint())
        with open(meta_path, "w") as fh:
            json.dump({"steps_done": steps_done, "total_steps": total_steps}, fh)
        md_checkpoint_volume.commit()

        progress = round(steps_done / total_steps, 4) if total_steps else 1.0
        done = steps_done >= total_steps
        return {
            "status": "completed" if done else "paused",
            "external_run_id": f"modal_md:{job_id}",
            "runpod_job_id": f"modal_md:{job_id}",
            "progress_fraction": progress,
            "checkpoint_uri": f"modal-volume://twog-md-checkpoints/{job_id}/state.chk",
            "output_payload": {
                "provider": "modal_md_checkpoint",
                "platform": platform.getName(),
                "steps_done": steps_done,
                "total_steps": total_steps,
                "findings": (
                    f"GPU MD ({platform.getName()}) completed {steps_done}/{total_steps} steps; "
                    f"PE={pe:.1f} kJ/mol."
                    if done
                    else f"GPU MD paused at {steps_done}/{total_steps} steps (checkpointed)."
                ),
                "limitations": [
                    "harmonic-well harness proves GPU + durable checkpoint infra, not a scientific MD result"
                ],
                "source_refs": [],
                "metrics": {"potential_energy_kj_mol": pe, "platform": platform.getName()},
                "signal": "neutral",
                "confidence": 0.0,
            },
            "metadata": {"provider": "modal_md_checkpoint", "platform": platform.getName()},
        }

    @app.local_entrypoint()
    def md_checkpoint() -> None:
        """Smoke the real-GPU checkpoint/resume loop across SEPARATE invocations (durable Volume):
        30k steps in 3 chunks -> paused 0.33, paused 0.67, completed 1.0.
        Run: python -m modal run src/hsa_research/ingestion_bridge/modal_app.py::md_checkpoint"""
        cfg = {"job_id": "smoke-md-ckpt", "total_steps": 30000, "steps_per_chunk": 10000, "n_particles": 2000}
        r1 = run_md_checkpoint_remote.remote({**cfg, "resume": False})
        print(f"chunk1: status={r1['status']} progress={r1['progress_fraction']} platform={r1['output_payload']['platform']}")
        r2 = run_md_checkpoint_remote.remote({**cfg, "resume": True})
        print(f"chunk2: status={r2['status']} progress={r2['progress_fraction']}")
        r3 = run_md_checkpoint_remote.remote({**cfg, "resume": True})
        print(f"chunk3: status={r3['status']} progress={r3['progress_fraction']} | {r3['output_payload']['findings']}")

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
