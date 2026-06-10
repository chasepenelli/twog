"""Unit tests for the omics-review analysis engine (the PIK3CA-mutant immunosuppression crux)."""

from __future__ import annotations

from hsa_research.ingestion_bridge.omics_review import (
    DEFAULT_IMMUNOSUPPRESSION_SIGNATURES,
    benjamini_hochberg,
    load_omics_dataset,
    mann_whitney,
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
