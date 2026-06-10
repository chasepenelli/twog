"""Unit tests for the supplementary-table parser (Megquier S4+S5 -> stratified cohort)."""

from __future__ import annotations

from hsa_research.ingestion_bridge.supplement_parser import (
    build_cohort,
    parse_s4_map,
    parse_s5_genotypes,
)

# Synthetic Table S5: HSA_4 = PIK3CA+TP53 mutant; HSA_20 = TP53 only (PIK3CA wt).
_S5 = """Table S5 - RNA-seq validation
HSA_4 chr34:12675674:PIK3CA:A:T same Missense frozen 0.25 0.05 0.72 No 67|0
HSA_4 chr5:32563923:TP53:C:T same Missense frozen 0.32 0.05 0.72 No 49|0
HSA_20 chr5:32563698:TP53:A:T same Missense frozen 0.26 0.35 0.37 No 40|0
"""

# Synthetic Table S4: HSA_4/HSA_20 have GSM (public expression); HSA_17 is raw-SRA only (no GSM).
_S4 = """Table S4 - Summary of canine RNA-seq data
DHSA-0502 HSA_4 Golden Retriever Spleen GSM2498359 SAMN06368903 Yes
DHSA-0904 HSA_20 Golden Retriever Heart GSM2498372 SAMN06368890 Yes
DHSA-1101 HSA_17 Golden Retriever Heart SAMN12659342 No
"""


def test_parse_s5_genotypes():
    geno = parse_s5_genotypes(_S5)
    assert geno["HSA_4"] == {"PIK3CA", "TP53"}
    assert geno["HSA_20"] == {"TP53"}  # PIK3CA absent -> wild-type


def test_parse_s4_map_captures_archive_ids():
    s4 = parse_s4_map([_S4])
    assert s4["HSA_4"][0]["rnaseq_id"] == "DHSA-0502"
    assert s4["HSA_4"][0]["gsm"] == "GSM2498359"
    assert s4["HSA_17"][0]["gsm"] is None  # raw-only, no public expression


def test_build_cohort_strata_and_counts():
    result = build_cohort([_S4], _S5, stratify_gene="PIK3CA")
    cohort = result["cohort"]
    assert cohort["HSA_4"]["genotype"]["PIK3CA"] == "mutant"
    assert cohort["HSA_4"]["genotype"]["TP53"] == "mutant"
    assert cohort["HSA_20"]["genotype"]["PIK3CA"] == "wt"
    assert cohort["HSA_17"]["has_public_expression"] is False  # raw-only excluded from crux
    # strata keyed by RNA-seq ID (= expression-matrix column), public-expression only
    assert result["crux_strata_public"] == {"DHSA-0502": "mutant", "DHSA-0904": "wt"}
    assert result["counts"] == {"mutant_public": 1, "wt_public": 1, "total_exome_matched_rnaseq": 3}
