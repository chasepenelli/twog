"""Unit tests for the docking lane engine (gnina output parsing + directional signal)."""

from __future__ import annotations

from hsa_research.ingestion_bridge.docking import build_docking_result, parse_gnina_output

_GNINA_STDOUT = """
mode |  affinity | CNN pose | CNN affinity
-----+-----------+----------+--------------
    1      -8.5       0.90        6.5
    2      -7.9       0.85        6.1
    3      -7.2       0.70        5.8
"""


def test_parse_gnina_output_extracts_modes():
    modes = parse_gnina_output(_GNINA_STDOUT)
    assert len(modes) == 3
    assert modes[0]["mode"] == 1
    assert modes[0]["affinity"] == -8.5
    assert modes[0]["cnn_pose_score"] == 0.90


def test_build_docking_result_supports_strong_binder():
    result = build_docking_result(
        parse_gnina_output(_GNINA_STDOUT), target="MTOR", ligand="RMC-5552-like", source_refs=["RCSB:xxxx"]
    )
    assert result["signal"] == "supports"  # -8.5 kcal/mol is strong
    assert result["confidence"] > 0.0
    assert result["metrics"]["best_affinity_kcal_mol"] == -8.5


def test_build_docking_result_refutes_weak_binder():
    weak = "    1      -3.5       0.40        4.0\n"
    result = build_docking_result(parse_gnina_output(weak), target="MTOR", ligand="weak")
    assert result["signal"] == "refutes"


def test_build_docking_result_neutral_midrange():
    mid = "    1      -6.0       0.60        5.0\n"
    result = build_docking_result(parse_gnina_output(mid), target="MTOR", ligand="mid")
    assert result["signal"] == "neutral"


def test_build_docking_result_no_poses_is_neutral():
    result = build_docking_result([], target="MTOR", ligand="none")
    assert result["signal"] == "neutral"
    assert result["confidence"] == 0.0
