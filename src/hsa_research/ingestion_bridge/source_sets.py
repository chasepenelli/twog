"""Shared source-key sets for hosted API reporting."""

from __future__ import annotations


STRUCTURED_SOURCE_KEYS = (
    "pubchem",
    "chembl",
    "uniprot",
    "rcsb_pdb",
    "openfda_animal_events",
)
CANINE_DATA_OMICS_SOURCE_KEYS = (
    "icdc",
    "geo",
    "sra",
)
LITERATURE_CLINICAL_SOURCE_KEYS = (
    "openalex",
    "pubmed",
    "europe_pmc",
    "crossref",
    "pmc_oa",
    "clinicaltrials_gov",
)
OPEN_ACCESS_DISCOVERY_SOURCE_KEYS = (
    "unpaywall",
)
LITERATURE_CORPUS_SOURCE_KEYS = (
    "openalex",
    "pubmed",
    "crossref",
)
LITERATURE_CORPUS_SOURCE_LIMITS = {
    "openalex": 100,
    "pubmed": 100,
    "crossref": 100,
}
LITERATURE_FULL_TEXT_SOURCE_KEYS = (
    "europe_pmc",
    "pmc_oa",
)
LITERATURE_FULL_TEXT_SOURCE_LIMITS = {
    "europe_pmc": 10,
    "pmc_oa": 3,
}
ALL_API_SOURCE_KEYS = (
    *STRUCTURED_SOURCE_KEYS,
    *CANINE_DATA_OMICS_SOURCE_KEYS,
    *LITERATURE_CLINICAL_SOURCE_KEYS,
    *OPEN_ACCESS_DISCOVERY_SOURCE_KEYS,
)
TRIAGE_ONLY_SOURCE_KEYS = (
    "sra",
    "crossref",
    "unpaywall",
)
