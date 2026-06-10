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


# Real gnina v1.3 output has a 5-column table including an `intramol` column (captured from an
# actual alpelisib->PI3Kα run). The parser must handle it and not mistake intramol for the CNN score.
_GNINA_REAL = """
mode |  affinity  |  intramol  |    CNN     |   CNN
     | (kcal/mol) | (kcal/mol) | pose score | affinity
-----+------------+------------+------------+----------
    1       -8.92       -0.14       0.9861      8.124
    2       -8.13        0.00       0.9497      8.063
    5       -8.95       -0.25       0.6369      7.472
"""


def test_parse_handles_real_gnina_intramol_column():
    modes = parse_gnina_output(_GNINA_REAL)
    assert len(modes) == 3
    m1 = modes[0]
    assert m1["mode"] == 1
    assert m1["affinity"] == -8.92
    assert m1["intramol"] == -0.14
    assert m1["cnn_pose_score"] == 0.9861  # NOT the intramol -0.14
    assert m1["cnn_affinity"] == 8.124


def test_build_uses_cnn_top_pose_not_min_affinity():
    # CNN-top pose (mode 1) differs from the most-negative Vina affinity (mode 5)
    result = build_docking_result(parse_gnina_output(_GNINA_REAL), target="PI3Ka", ligand="alpelisib")
    assert result["signal"] == "supports"
    assert result["metrics"]["best_affinity_kcal_mol"] == -8.92  # recommended (CNN-top) pose
    assert result["metrics"]["best_vina_affinity_kcal_mol"] == -8.95  # most favorable Vina
    assert result["metrics"]["best_cnn_pose_score"] == 0.9861
    assert result["confidence"] > 0.5  # strong + high CNN pose confidence


def test_parser_ignores_non_data_lines():
    noise = "gnina v1.3.1 master:5bd63bd\nUsing random seed: 0\n0%   10   20   30\n" + _GNINA_REAL
    assert len(parse_gnina_output(noise)) == 3  # header/banner/progress lines excluded
