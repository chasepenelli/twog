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
ALL_API_SOURCE_KEYS = (
    *STRUCTURED_SOURCE_KEYS,
    *CANINE_DATA_OMICS_SOURCE_KEYS,
    *LITERATURE_CLINICAL_SOURCE_KEYS,
)
