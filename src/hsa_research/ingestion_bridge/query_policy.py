"""Comparative oncology query policy for HSA ingestion.

Canine hemangiosarcoma research should not be isolated from human vascular
sarcoma work. This module centralizes that rule so every scholarly harvester
and starter query includes human angiosarcoma and close vascular sarcoma
analogs by default.
"""

from __future__ import annotations

import re
from typing import Literal

from .contracts import ResearchObjectType, SourceQuery

ComparativeQueryStyle = Literal["generic", "pubmed", "europe_pmc", "openalex", "crossref", "pmc"]

CANINE_HSA_TERMS = (
    '"canine hemangiosarcoma"',
    '"canine haemangiosarcoma"',
    '"dog hemangiosarcoma"',
    '"dog haemangiosarcoma"',
    '(hemangiosarcoma AND (canine OR dog OR dogs))',
    '(haemangiosarcoma AND (canine OR dog OR dogs))',
)

HUMAN_VASCULAR_SARCOMA_TERMS = (
    '"human angiosarcoma"',
    '"angiosarcoma"',
    '"cutaneous angiosarcoma"',
    '"cardiac angiosarcoma"',
    '"hepatic angiosarcoma"',
    '"radiation-associated angiosarcoma"',
    '"epithelioid hemangioendothelioma"',
    '"epithelioid haemangioendothelioma"',
    '"hemangioendothelioma"',
    '"haemangioendothelioma"',
    '"vascular sarcoma"',
    '"endothelial sarcoma"',
)

PMC_CANINE_HSA_TIAB_TERMS = (
    '"canine hemangiosarcoma"[tiab]',
    '"canine haemangiosarcoma"[tiab]',
    '"dog hemangiosarcoma"[tiab]',
    '"dog haemangiosarcoma"[tiab]',
    '(hemangiosarcoma[tiab] AND (canine[tiab] OR dog[tiab] OR dogs[tiab]))',
    '(haemangiosarcoma[tiab] AND (canine[tiab] OR dog[tiab] OR dogs[tiab]))',
)

PMC_HUMAN_VASCULAR_SARCOMA_TIAB_TERMS = (
    '"human angiosarcoma"[tiab]',
    'angiosarcoma[tiab]',
    '"cutaneous angiosarcoma"[tiab]',
    '"cardiac angiosarcoma"[tiab]',
    '"hepatic angiosarcoma"[tiab]',
    '"radiation-associated angiosarcoma"[tiab]',
    '"epithelioid hemangioendothelioma"[tiab]',
    '"epithelioid haemangioendothelioma"[tiab]',
    'hemangioendothelioma[tiab]',
    'haemangioendothelioma[tiab]',
    '"vascular sarcoma"[tiab]',
    '"endothelial sarcoma"[tiab]',
)

PUBMED_COMPARATIVE_ONCOLOGY_TIAB_TERMS = (
    '"comparative oncology"[tiab]',
    '"spontaneous canine tumor"[tiab]',
    '"spontaneous canine tumour"[tiab]',
    '("canine model"[tiab] AND human[tiab])',
    '("dog model"[tiab] AND human[tiab])',
    '"translational oncology"[tiab]',
)

COMPARATIVE_ONCOLOGY_TERMS = (
    '"comparative oncology"',
    '"spontaneous canine tumor"',
    '"spontaneous canine tumour"',
    '"canine model" AND human',
    '"dog model" AND human',
    '"translational oncology"',
)

THERAPY_TERMS = (
    "therapy",
    "therapeutic",
    "treatment",
    "drug",
    "target",
    "VEGF",
    "VEGFR",
    "KIT",
    "PI3K",
    "AKT",
    "MTOR",
    "CD47",
    "doxorubicin",
    "propranolol",
    "sirolimus",
    "toceranib",
    "pazopanib",
    "paclitaxel",
)


def comparative_required_query(style: ComparativeQueryStyle = "generic") -> str:
    """Return the required broad disease/analog query for scholarly sources."""

    if style == "pubmed":
        return _or_group(
            (
                _or_group(PMC_CANINE_HSA_TIAB_TERMS),
                _or_group(PMC_HUMAN_VASCULAR_SARCOMA_TIAB_TERMS),
                _or_group(PUBMED_COMPARATIVE_ONCOLOGY_TIAB_TERMS),
            )
        )
    groups = (
        _or_group(CANINE_HSA_TERMS),
        _or_group(HUMAN_VASCULAR_SARCOMA_TERMS),
        _or_group(COMPARATIVE_ONCOLOGY_TERMS),
    )
    return _or_group(groups)


def disease_analog_query(style: ComparativeQueryStyle = "generic") -> str:
    """Return disease-specific canine HSA plus human vascular sarcoma analog terms."""

    if style == "pmc":
        return _or_group((_or_group(PMC_CANINE_HSA_TIAB_TERMS), _or_group(PMC_HUMAN_VASCULAR_SARCOMA_TIAB_TERMS)))
    return _or_group((_or_group(CANINE_HSA_TERMS), _or_group(HUMAN_VASCULAR_SARCOMA_TERMS)))


def clinical_trials_analog_query() -> str:
    """Return a focused ClinicalTrials.gov query for human vascular sarcoma analog trials."""

    return _or_group(
        (
            "angiosarcoma",
            '"vascular sarcoma"',
            "hemangioendothelioma",
            "haemangioendothelioma",
            "hemangiosarcoma",
            "haemangiosarcoma",
        )
    )


def therapy_comparative_query(style: ComparativeQueryStyle = "generic") -> str:
    """Return a therapy/target-centered comparative oncology query."""

    return f"({comparative_required_query(style)}) AND ({_or_group(THERAPY_TERMS)})"


def expand_with_comparative_policy(query_text: str, style: ComparativeQueryStyle = "generic") -> str:
    """Ensure a scholarly query includes human angiosarcoma and analog coverage."""

    if _has_required_analog_terms(query_text):
        return query_text
    return f"({query_text}) OR ({comparative_required_query(style)})"


def build_scholarly_source_queries() -> list[SourceQuery]:
    """Create starter queries that always include human analog coverage."""

    unpaywall_disease_query = (
        '"canine hemangiosarcoma" OR "canine haemangiosarcoma" OR "dog hemangiosarcoma" OR '
        '"dog haemangiosarcoma" OR "human angiosarcoma" OR angiosarcoma OR hemangiosarcoma OR '
        'haemangiosarcoma OR hemangioendothelioma OR haemangioendothelioma OR "vascular sarcoma"'
    )
    return [
        SourceQuery(
            source_key="pubmed",
            query_name="comparative_hsa_required",
            query_text=comparative_required_query("pubmed"),
            query_params={"comparative_policy": "required"},
            track="comparative_oncology",
        ),
        SourceQuery(
            source_key="pubmed",
            query_name="comparative_hsa_therapy_targets",
            query_text=therapy_comparative_query("pubmed"),
            query_params={"comparative_policy": "required"},
            track="treatment",
        ),
        SourceQuery(
            source_key="europe_pmc",
            query_name="comparative_hsa_open_access",
            query_text=comparative_required_query("europe_pmc"),
            query_params={"comparative_policy": "required", "open_access": True},
            track="comparative_oncology",
        ),
        SourceQuery(
            source_key="europe_pmc",
            query_name="comparative_hsa_therapy_targets",
            query_text=therapy_comparative_query("europe_pmc"),
            query_params={"comparative_policy": "required", "open_access": True},
            track="treatment",
        ),
        SourceQuery(
            source_key="openalex",
            query_name="comparative_hsa_required",
            query_text=comparative_required_query("openalex"),
            query_params={"comparative_policy": "required"},
            track="comparative_oncology",
        ),
        SourceQuery(
            source_key="openalex",
            query_name="comparative_hsa_citation_neighborhood",
            query_text=f"({comparative_required_query('openalex')}) AND (angiogenesis OR metastasis OR survival OR splenic OR endothelial)",
            query_params={"comparative_policy": "required"},
            track="citation_graph",
        ),
        SourceQuery(
            source_key="crossref",
            query_name="comparative_hsa_journal_backbone",
            query_text=comparative_required_query("crossref"),
            query_params={"comparative_policy": "required"},
            track="journal_backbone",
        ),
        SourceQuery(
            source_key="pmc_oa",
            query_name="licensed_full_text_hsa",
            query_text=disease_analog_query("pmc"),
            query_params={"comparative_policy": "required", "license_required": True},
            track="legal_full_text",
        ),
        SourceQuery(
            source_key="unpaywall",
            query_name="oa_discovery_hsa_titles",
            query_text=unpaywall_disease_query,
            query_params={"is_oa": True},
            track="open_access_discovery",
            active=False,
        ),
    ]


def build_clinical_trial_source_queries() -> list[SourceQuery]:
    """Create starter queries for human clinical trial analog evidence."""

    return [
        SourceQuery(
            source_key="clinicaltrials_gov",
            query_name="human_vascular_sarcoma_trials",
            query_text=clinical_trials_analog_query(),
            query_params={"search_area": "term"},
            track="trials",
        )
    ]


def build_canine_data_source_queries() -> list[SourceQuery]:
    """Create starter queries for API-backed canine comparative oncology data."""

    return [
        SourceQuery(
            source_key="avma_vctr",
            query_name="canine_hsa_trials",
            query_text="hemangiosarcoma",
            query_params={
                "sort_by": "score",
                "skip_similar_studies": True,
                "extra_aggregations": [],
            },
            track="veterinary_trials",
            object_type=ResearchObjectType.VETERINARY_TRIAL,
        ),
        SourceQuery(
            source_key="icdc",
            query_name="canine_hsa_cases",
            query_text="Hemangiosarcoma",
            query_params={"diagnosis": ["Hemangiosarcoma"]},
            track="canine_omics",
        )
    ]


def build_omics_source_queries() -> list[SourceQuery]:
    """Create starter queries for API-backed omics dataset metadata."""

    canine_hsa = '("canine hemangiosarcoma" OR "dog hemangiosarcoma" OR (hemangiosarcoma AND canine) OR (hemangiosarcoma AND dog))'
    return [
        SourceQuery(
            source_key="geo",
            query_name="canine_hsa_expression",
            query_text=canine_hsa,
            query_params={"db": "gds"},
            track="omics",
        ),
        SourceQuery(
            source_key="sra",
            query_name="canine_hsa_sequence_runs",
            query_text=canine_hsa,
            query_params={"db": "sra"},
            track="omics",
        ),
    ]


def build_chemistry_source_queries() -> list[SourceQuery]:
    """Create starter queries for API-backed compound and bioactivity metadata."""

    priority_compounds = "propranolol OR doxorubicin OR toceranib OR sirolimus OR vorinostat OR valproic acid OR paclitaxel OR cyclophosphamide"
    priority_target_ids = [
        "CHEMBL213",
        "CHEMBL210",
        "CHEMBL2289",
        "CHEMBL279",
        "CHEMBL1955",
        "CHEMBL2095227",
        "CHEMBL1936",
        "CHEMBL5303563",
        "CHEMBL2007",
        "CHEMBL1913",
        "CHEMBL5303562",
        "CHEMBL1974",
        "CHEMBL1844",
        "CHEMBL2842",
        "CHEMBL325",
        "CHEMBL1937",
        "CHEMBL1829",
        "CHEMBL1865",
        "CHEMBL3192",
        "CHEMBL1806",
        "CHEMBL3396",
        "CHEMBL2094255",
        "CHEMBL3832941",
        "CHEMBL3832942",
    ]
    return [
        SourceQuery(
            source_key="pubchem",
            query_name="priority_compounds",
            query_text=priority_compounds,
            query_params={"records_per_term": 1, "require_exact_match": True},
            track="chemistry",
            object_type=ResearchObjectType.COMPOUND_RECORD,
        ),
        SourceQuery(
            source_key="chembl",
            query_name="priority_compound_bioactivities",
            query_text=priority_compounds,
            query_params={
                "molecules_per_term": 1,
                "activities_per_molecule": 5,
                "target_chembl_ids": priority_target_ids,
                "target_organisms": ["Homo sapiens", "Canis lupus familiaris"],
                "standard_types": ["IC50", "Ki", "Kd", "EC50"],
                "assay_types": ["B", "F"],
                "min_pchembl": 4.0,
                "include_cell_line_assays": True,
                "cell_line_terms": [
                    "angiosarcoma",
                    "hemangiosarcoma",
                    "haemangiosarcoma",
                    "sarcoma",
                    "endothelial",
                    "vascular",
                    "canine",
                    "dog",
                ],
                "cell_line_standard_types": ["IC50", "EC50"],
                "cell_line_scan_limit": 50,
                "cell_line_records_per_molecule": 2,
            },
            track="bioactivity",
            object_type=ResearchObjectType.BIOACTIVITY_ASSAY,
        ),
    ]


def build_target_structure_source_queries() -> list[SourceQuery]:
    """Create starter queries for target/protein and structure metadata."""

    priority_targets = "VEGFA OR KDR OR FLT4 OR KIT OR MTOR OR CD47 OR SIRPA OR TP53"
    return [
        SourceQuery(
            source_key="uniprot",
            query_name="canine_human_priority_targets",
            query_text=priority_targets,
            query_params={"organism_ids": ["9615", "9606"], "require_gene_match": True, "dedupe_gene_organism": True},
            track="target_structure",
            object_type=ResearchObjectType.STRUCTURE,
        ),
        SourceQuery(
            source_key="rcsb_pdb",
            query_name="priority_target_structures",
            query_text=priority_targets,
            query_params={"rows_per_term": 3, "require_target_match": True, "require_protein_entity": True},
            track="target_structure",
            object_type=ResearchObjectType.STRUCTURE,
        ),
    ]


def build_safety_source_queries() -> list[SourceQuery]:
    """Create starter queries for veterinary safety signal metadata."""

    return [
        SourceQuery(
            source_key="openfda_animal_events",
            query_name="priority_drug_safety",
            query_text="propranolol OR doxorubicin OR toceranib OR sirolimus OR cyclophosphamide",
            query_params={"species": "Dog"},
            track="safety",
            object_type=ResearchObjectType.SAFETY_REPORT,
        )
    ]


def infer_comparative_scope(title: str | None, abstract: str | None = None) -> dict[str, object]:
    """Infer broad comparative context for a normalized research object."""

    text = f"{title or ''} {abstract or ''}".lower()
    matched: list[str] = []
    if _contains_any(text, ("canine hemangiosarcoma", "canine haemangiosarcoma", "dog hemangiosarcoma")):
        matched.append("canine_hsa")
    elif _contains_any(text, ("hemangiosarcoma", "haemangiosarcoma")) and _contains_any(text, ("canine", "dog", "dogs")):
        matched.append("canine_hsa")

    if _contains_any(
        text,
        (
            "angiosarcoma",
            "angiosarcomas",
            "cutaneous angiosarcoma",
            "cardiac angiosarcoma",
            "hepatic angiosarcoma",
            "radiation-associated angiosarcoma",
        ),
    ):
        matched.append("human_angiosarcoma")
    if _contains_any(text, ("hemangioendothelioma", "haemangioendothelioma", "vascular sarcoma", "endothelial sarcoma")):
        matched.append("vascular_sarcoma_analog")
    if _contains_any(text, ("comparative oncology", "translational oncology")):
        matched.append("comparative_oncology")

    return {
        "comparative_oncology_required": True,
        "matched_concepts": sorted(set(matched)),
    }


def _or_group(terms: tuple[str, ...]) -> str:
    return "(" + " OR ".join(terms) + ")"


def _has_required_analog_terms(query_text: str) -> bool:
    query = query_text.lower()
    has_canine = "hemangiosarcoma" in query or "haemangiosarcoma" in query
    has_human_analog = "angiosarcoma" in query or "hemangioendothelioma" in query or "vascular sarcoma" in query
    return has_canine and has_human_analog


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(re.search(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", text) for term in terms)
