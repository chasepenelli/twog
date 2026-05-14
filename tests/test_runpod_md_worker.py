from pathlib import Path

from runpod_workers.md_smoke.src import handler as md_worker


_MINIMAL_PDB = (
    "ATOM      1  N   ALA A   1      11.104  13.207   8.678  1.00 20.00           N\n"
    "ATOM      2  CA  ALA A   1      12.560  13.235   8.421  1.00 20.00           C\n"
    "TER\n"
    "END\n"
)


def _worker_payload(**overrides):
    payload = {
        "protein_pdb": _MINIMAL_PDB,
        "compound_smiles": "CCO",
        "target_name": "KDR",
        "compound_name": "ethanol smoke ligand",
        "simulation_steps": 10,
        "temperature": 300.0,
        "protein_source": "unit-test minimal PDB smoke fixture",
        "ligand_source": "unit-test SMILES fixture",
        "preparation_method": "unprepared smoke input for contract validation only",
    }
    payload.update(overrides)
    return payload


def test_md_worker_rejects_malformed_pdb_without_chemistry_dependencies():
    result = md_worker.handler({"input": _worker_payload(protein_pdb="HEADER only\nEND\n")})

    assert result["status"] == "failed"
    assert result["errors"][0]["stage"] == "input_validation"
    assert "protein_pdb" in result["errors"][0]["message"]


def test_md_worker_ligand_pdbqt_uses_sdf_not_pdb(monkeypatch):
    commands_seen = []

    def fake_prepare_ligand_3d(smiles, compound_name, workdir):
        sdf_path = Path(workdir) / "ligand.sdf"
        mol_path = Path(workdir) / "ligand.mol"
        sdf_path.write_text("sdf fixture\n", encoding="utf-8")
        mol_path.write_text("mol fixture\n", encoding="utf-8")
        return {
            "sdf_path": sdf_path,
            "mol_path": mol_path,
            "stage_details": {
                "compound_name": compound_name,
                "atom_count": 9,
                "conformer_count": 1,
                "optimization_method": "MMFF94",
                "optimization_code": 0,
                "intermediate_format": "sdf",
            },
        }

    def fake_run_subprocess(command, *, stage):
        commands_seen.append(command)
        assert stage == "ligand_pdbqt"
        input_path = command[command.index("-i") + 1]
        output_path = command[command.index("-o") + 1]
        assert input_path.endswith("ligand.sdf")
        assert not input_path.endswith("ligand.pdb")
        Path(output_path).write_text("pdbqt fixture\n", encoding="utf-8")
        return {
            "command": command,
            "return_code": 0,
            "stdout_tail": "",
            "stderr_tail": "",
        }

    monkeypatch.setattr(md_worker, "_prepare_ligand_3d", fake_prepare_ligand_3d)
    monkeypatch.setattr(md_worker, "_find_command", lambda name: "mk_prepare_ligand.py" if name == "mk_prepare_ligand.py" else None)
    monkeypatch.setattr(md_worker, "_run_subprocess", fake_run_subprocess)

    result = md_worker.handler({"input": _worker_payload()})

    assert result["status"] == "completed"
    assert commands_seen
    stages = {stage["stage"]: stage for stage in result["stages"]}
    assert stages["ligand_3d"]["status"] == "completed"
    assert stages["ligand_pdbqt"]["status"] == "completed"
    assert stages["ligand_pdbqt"]["input_format"] == "sdf"
    assert stages["docking"]["status"] == "skipped"
    assert stages["md_smoke"]["status"] == "skipped"


def test_md_worker_docking_enabled_reports_missing_vina_as_failure(monkeypatch):
    def fake_prepare_ligand_3d(smiles, compound_name, workdir):
        sdf_path = Path(workdir) / "ligand.sdf"
        mol_path = Path(workdir) / "ligand.mol"
        sdf_path.write_text("sdf fixture\n", encoding="utf-8")
        mol_path.write_text("mol fixture\n", encoding="utf-8")
        return {
            "sdf_path": sdf_path,
            "mol_path": mol_path,
            "stage_details": {
                "compound_name": compound_name,
                "atom_count": 9,
                "conformer_count": 1,
                "optimization_method": "MMFF94",
                "optimization_code": 0,
                "intermediate_format": "sdf",
            },
        }

    def fake_run_subprocess(command, *, stage):
        output_path = command[command.index("-o") + 1]
        Path(output_path).write_text("pdbqt fixture\n", encoding="utf-8")
        return {
            "command": command,
            "return_code": 0,
            "stdout_tail": "",
            "stderr_tail": "",
        }

    def fake_find_command(name):
        return "mk_prepare_ligand.py" if name == "mk_prepare_ligand.py" else None

    monkeypatch.setattr(md_worker, "_prepare_ligand_3d", fake_prepare_ligand_3d)
    monkeypatch.setattr(md_worker, "_find_command", fake_find_command)
    monkeypatch.setattr(md_worker, "_run_subprocess", fake_run_subprocess)

    result = md_worker.handler({"input": _worker_payload(enable_docking=True)})

    assert result["status"] == "failed"
    stages = {stage["stage"]: stage for stage in result["stages"]}
    assert stages["ligand_pdbqt"]["status"] == "completed"
    assert stages["docking"]["status"] == "failed"
    assert result["errors"][0]["stage"] == "docking"
    assert "vina" in result["errors"][0]["message"]


def test_md_worker_returns_structured_ligand_failure(monkeypatch):
    def fake_prepare_ligand_3d(smiles, compound_name, workdir):
        raise md_worker.StageFailure("ligand_3d", "compound_smiles could not be parsed by RDKit.", {"compound_smiles": smiles})

    monkeypatch.setattr(md_worker, "_prepare_ligand_3d", fake_prepare_ligand_3d)

    result = md_worker.handler({"input": _worker_payload(compound_smiles="not-a-smiles")})

    assert result["status"] == "failed"
    assert result["errors"][0]["stage"] == "ligand_3d"
    assert result["errors"][0]["compound_smiles"] == "not-a-smiles"
