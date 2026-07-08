"""Frontier modality weighting for TWOG moonshot research lanes."""

from __future__ import annotations

import re
from typing import Any


FRONTIER_RESEARCH_POLICY_VERSION = "frontier_moonshot_policy_v1"
FRONTIER_MODALITY_WEIGHT = 0.9
FOUNDATIONAL_EVIDENCE_WEIGHT = 0.1

FRONTIER_MODALITY_TERMS: dict[str, tuple[str, ...]] = {
    "mrna_personalized_vaccine": (
        "mrna",
        "rna vaccine",
        "personalized vaccine",
        "neoantigen",
        "neoantigen vaccine",
        "cancer vaccine",
        "tumor antigen vaccine",
    ),
    "cellular_therapy": (
        "car-t",
        "car t",
        "cart",
        "tcr-t",
        "tcr t",
        "engineered t cell",
        "engineered t-cell",
        "cellular therapy",
        "nk cell therapy",
        "adoptive cell therapy",
    ),
    "antibody_drug_conjugate": (
        "antibody-drug conjugate",
        "antibody drug conjugate",
        "adc",
        "targeting antibody",
        "payload",
        "cleavable linker",
        "bystander effect",
    ),
    "stapled_peptide_degrader": (
        "stapled peptide",
        "hydrocarbon stapled",
        "cell-penetrating peptide",
        "peptide strategy",
        "targeting peptide",
        "directed peptide",
        "vimentin-targeting peptide",
        "vimentin-directed peptide",
        "peptide protac",
        "protac",
        "molecular glue",
        "targeted protein degradation",
        "degrader",
    ),
    "frontier_targeted_small_molecule": (
        "allele-specific",
        "mutation-selective",
        "mutation selective",
        "allosteric inhibitor",
        "covalent inhibitor",
        "synthetic lethality",
        "synthetic lethal",
        "ras inhibitor",
        "kras inhibitor",
        "pan-ras",
        "daraxonrasib",
    ),
}

FRONTIER_SEARCH_TERMS: tuple[str, ...] = (
    '"mRNA vaccine"',
    '"personalized neoantigen"',
    '"cancer vaccine"',
    '"CAR-T"',
    '"engineered T cell"',
    '"cellular therapy"',
    '"antibody-drug conjugate"',
    '"ADC"',
    '"stapled peptide"',
    '"peptide PROTAC"',
    '"targeted protein degradation"',
    '"molecular glue"',
    '"mutation-selective"',
    '"allosteric inhibitor"',
    '"synthetic lethality"',
)


def frontier_modality_matches(text: str) -> dict[str, list[str]]:
    """Return frontier modality labels and matched terms from normalized text."""

    normalized = _normalize(text)
    matches: dict[str, list[str]] = {}
    for modality, terms in FRONTIER_MODALITY_TERMS.items():
        matched = [term for term in terms if _term_in_text(term, normalized)]
        if matched:
            matches[modality] = matched
    return matches


# Map a candidate's text to the Stage-0 DESIGN lanes it implies (lane_inputs._DESIGN_STAGE0_LANES).
# Grouped by the real INPUT SHAPE a test would need, not by marketing label: PROTAC/glue/degrader → a
# ternary-complex design (degrader_design); peptides / de-novo binders / the ADC target-engagement arm →
# binder_design; base/prime edits → genome_edit; CAR-T/TCR-T → cell_therapy; neoantigen vaccines →
# mrna_vaccine. Self-contained term set (the policy's FRONTIER_MODALITY_TERMS has no gene-edit cluster).
_DESIGN_LANE_TERMS: dict[str, tuple[str, ...]] = {
    "degrader_design": (
        "protac", "peptide protac", "molecular glue", "targeted protein degradation", "degrader",
    ),
    "binder_design": (
        "stapled peptide", "hydrocarbon stapled", "cell-penetrating peptide", "targeting peptide",
        "directed peptide", "de novo binder", "de-novo binder", "minibinder", "peptide strategy",
        "antibody-drug conjugate", "antibody drug conjugate", "adc", "targeting antibody",
    ),
    "genome_edit": (
        "base edit", "base editing", "prime edit", "prime editing", "gene edit", "gene editing",
        "genome edit", "genome editing", "crispr", "cas9", "cas12", "pegrna",
    ),
    "cell_therapy": (
        "car-t", "car t", "cart", "tcr-t", "tcr t", "engineered t cell", "engineered t-cell",
        "cellular therapy", "nk cell therapy", "adoptive cell therapy",
    ),
    "mrna_vaccine": (
        "mrna", "rna vaccine", "personalized vaccine", "neoantigen", "cancer vaccine",
        "tumor antigen vaccine",
    ),
}


def design_lanes_for_text(text: str) -> set[str]:
    """The Stage-0 frontier/design lanes a candidate's text implies. Pure + deterministic; the Research
    Director uses this to attach the right design lane(s) to a frontier bet. Returns an empty set for a
    non-frontier candidate (e.g. a conventional small molecule → no design lane, it routes to docking)."""
    normalized = _normalize(text)
    lanes: set[str] = set()
    for lane, terms in _DESIGN_LANE_TERMS.items():
        if any(_term_in_text(term, normalized) for term in terms):
            lanes.add(lane)
    return lanes


def frontier_modality_profile(
    text: str,
    *,
    conventional_score: float,
) -> dict[str, Any]:
    """Score whether an idea should be treated as a frontier moonshot.

    The policy intentionally gives most weight to frontier modality fit while
    preserving a small conventional evidence contribution. This keeps the
    public candidate lane biased toward big bets without letting citation-light
    ideas bypass the normal provenance and validation-shape gates.
    """

    conventional = max(0.0, min(float(conventional_score), 1.0))
    matches = frontier_modality_matches(text)
    matched_term_count = sum(len(terms) for terms in matches.values())
    matched_modality_count = len(matches)
    if matched_modality_count == 0:
        frontier_score = 0.0
    else:
        frontier_score = min(1.0, 0.62 + (matched_modality_count * 0.14) + (matched_term_count * 0.04))
    weighted = (FRONTIER_MODALITY_WEIGHT * frontier_score) + (FOUNDATIONAL_EVIDENCE_WEIGHT * conventional)
    return {
        "policy": FRONTIER_RESEARCH_POLICY_VERSION,
        "frontier_modality_weight": FRONTIER_MODALITY_WEIGHT,
        "foundational_evidence_weight": FOUNDATIONAL_EVIDENCE_WEIGHT,
        "frontier_modality_score": round(frontier_score, 3),
        "foundational_evidence_score": round(conventional, 3),
        "weighted_score": round(weighted, 3),
        "matched_modalities": sorted(matches),
        "matched_terms": {key: sorted(value) for key, value in sorted(matches.items())},
    }


def frontier_search_or_group(*, max_terms: int = 12) -> str:
    """Return a compact OR group suitable for source query expansion."""

    terms = list(FRONTIER_SEARCH_TERMS[: max(1, max_terms)])
    return "(" + " OR ".join(terms) + ")"


def frontier_policy_note() -> str:
    return (
        "Apply TWOG frontier moonshot policy: weight frontier modality fit at 90% and "
        "conventional prior evidence at 10%, while preserving citation provenance and "
        "explicit stop criteria."
    )


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", str(text).casefold()).strip()


def _term_in_text(term: str, text: str) -> bool:
    normalized = _normalize(term)
    if not normalized:
        return False
    if re.fullmatch(r"[a-z0-9]+", normalized):
        return re.search(rf"\b{re.escape(normalized)}\b", text) is not None
    return normalized in text
