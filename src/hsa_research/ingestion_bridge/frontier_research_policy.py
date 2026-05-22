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
