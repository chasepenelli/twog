"""Unit tests for the omics-review analysis engine (the PIK3CA-mutant immunosuppression crux)."""

from __future__ import annotations

from hsa_research.ingestion_bridge.omics_review import (
    DEFAULT_IMMUNOSUPPRESSION_SIGNATURES,
    benjamini_hochberg,
    load_omics_dataset,
    mann_whitney,
    parse_expression_matrix,
    parse_strata_table,
    run_omics_review,
    score_signatures,
)

_IMMUNO_GENES = sorted({g for sig in DEFAULT_IMMUNOSUPPRESSION_SIGNATURES.values() for g in sig})


def _expression(high: list[str], low: list[str], hi: float = 3.0, lo: float = 1.0):
    expr = {}
    for s in high:
        expr[s] = {g: hi for g in _IMMUNO_GENES}
    for s in low:
        expr[s] = {g: lo for g in _IMMUNO_GENES}
    return expr


def test_mann_whitney_separates_clearly():
    res = mann_whitney([3, 3, 3, 3, 3], [1, 1, 1, 1, 1])
    assert res["auc"] == 1.0  # group1 entirely higher
    assert res["p"] < 0.05


def test_benjamini_hochberg_monotone_and_bounded():
    q = benjamini_hochberg([0.001, 0.01, 0.5, 0.9])
    assert all(0.0 <= v <= 1.0 for v in q)
    assert q == sorted(q)  # BH-adjusted values are monotone in the sorted p order here


def test_score_signatures_higher_for_high_expression():
    expr = _expression(["m1", "m2"], ["w1", "w2"])
    scores, used = score_signatures(expr, DEFAULT_IMMUNOSUPPRESSION_SIGNATURES)
    for sig in DEFAULT_IMMUNOSUPPRESSION_SIGNATURES:
        assert scores["m1"][sig] > scores["w1"][sig]
        assert used[sig]  # genes were found


def test_run_omics_review_supports_when_mutant_immunosuppressed_and_powered():
    mut = [f"m{i}" for i in range(5)]
    wt = [f"w{i}" for i in range(5)]
    result = run_omics_review(
        expression=_expression(mut, wt),
        strata={**{s: "mutant" for s in mut}, **{s: "wt" for s in wt}},
        source_refs=["PRJNA562916", "GSE225599"],
    )
    assert result["signal"] == "supports"
    assert result["confidence"] > 0.0
    assert result["metrics"]["n_mutant"] == 5 and result["metrics"]["n_wt"] == 5
    assert result["metrics"]["significant_higher"]


def test_run_omics_review_neutral_when_no_difference():
    mut = [f"m{i}" for i in range(5)]
    wt = [f"w{i}" for i in range(5)]
    # both strata identical → no differential composition
    result = run_omics_review(
        expression=_expression(mut, wt, hi=1.0, lo=1.0),
        strata={**{s: "mutant" for s in mut}, **{s: "wt" for s in wt}},
    )
    assert result["signal"] == "neutral"
    assert result["confidence"] <= 0.3


def test_run_omics_review_neutral_when_underpowered_even_if_higher():
    mut = ["m0", "m1"]  # only 2 mutant → underpowered
    wt = [f"w{i}" for i in range(5)]
    result = run_omics_review(
        expression=_expression(mut, wt),
        strata={**{s: "mutant" for s in mut}, **{s: "wt" for s in wt}},
        min_n_per_stratum=5,
    )
    assert result["metrics"]["underpowered"] is True
    assert result["signal"] == "neutral"  # honest: can't conclude from 2 samples


def test_load_omics_dataset_is_an_unwired_seam():
    import pytest

    with pytest.raises(NotImplementedError):
        load_omics_dataset(["PRJNA562916"])


def test_parse_expression_matrix_genes_in_rows():
    text = "gene\tm1\tm2\tw1\nFOXP3\t3.0\t3.2\t1.0\nCD163\t2.5\t2.7\t0.9\n"
    expr = parse_expression_matrix(text)
    assert set(expr) == {"m1", "m2", "w1"}
    assert expr["m1"]["FOXP3"] == 3.0
    assert expr["w1"]["CD163"] == 0.9


def test_parse_strata_table_maps_mutant_and_wt():
    text = "sample,pik3ca_status\nm1,mutant\nm2,1\nw1,wt\nw2,0\n"
    strata = parse_strata_table(text)
    assert strata == {"m1": "mutant", "m2": "mutant", "w1": "wt", "w2": "wt"}


def test_load_omics_dataset_from_matrix_and_strata_files(tmp_path):
    genes = sorted({g for sig in DEFAULT_IMMUNOSUPPRESSION_SIGNATURES.values() for g in sig})
    header = "gene\t" + "\t".join(["m1", "m2", "m3", "m4", "m5", "w1", "w2", "w3", "w4", "w5"])
    lines = [header]
    for g in genes:
        lines.append(g + "\t" + "\t".join(["3.0"] * 5 + ["1.0"] * 5))
    matrix = tmp_path / "matrix.tsv"
    matrix.write_text("\n".join(lines) + "\n")
    strata_f = tmp_path / "strata.csv"
    strata_f.write_text(
        "sample,pik3ca_status\n" + "\n".join([f"m{i},mutant" for i in range(1, 6)] + [f"w{i},wt" for i in range(1, 6)]) + "\n"
    )

    expression, strata = load_omics_dataset([], matrix_path=str(matrix), strata_path=str(strata_f))
    result = run_omics_review(expression=expression, strata=strata, source_refs=["fixture"])
    assert result["signal"] == "supports"  # the parsed real-format files flow through the engine


def test_load_omics_dataset_without_files_surfaces_the_pipeline_gap():
    import pytest

    with pytest.raises(NotImplementedError):
        load_omics_dataset(["PRJNA562916"])
