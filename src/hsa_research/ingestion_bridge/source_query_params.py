"""Helpers for separating internal source-query metadata from API parameters."""

from __future__ import annotations

from typing import Any

from .contracts import SourceQuery

_COMMON_SCHOLARLY_QUERY_PARAMS = {
    "comparative_policy",
    "require_policy_match",
    "published_after",
    "published_before",
}

_SOURCE_QUERY_PARAM_ALLOWLISTS: dict[str, set[str]] = {
    "openalex": _COMMON_SCHOLARLY_QUERY_PARAMS
    | {
        "cursor",
        "filter",
        "from_publication_date",
        "mailto",
        "sample",
        "seed",
        "select",
        "sort",
        "to_publication_date",
    },
    "crossref": _COMMON_SCHOLARLY_QUERY_PARAMS
    | {
        "filter",
        "mailto",
        "order",
        "select",
        "sort",
    },
    "europe_pmc": _COMMON_SCHOLARLY_QUERY_PARAMS
    | {
        "fetch_full_text",
        "format",
        "full_text_attempts",
        "full_text_time_budget_seconds",
        "full_text_timeout_seconds",
        "open_access",
        "resultType",
    },
    "pubmed": _COMMON_SCHOLARLY_QUERY_PARAMS
    | {
        "api_key",
        "datetype",
        "email",
        "field",
        "maxdate",
        "mindate",
        "reldate",
        "retstart",
        "sort",
        "tool",
    },
    "pmc_oa": _COMMON_SCHOLARLY_QUERY_PARAMS
    | {
        "api_key",
        "datetype",
        "email",
        "field",
        "full_text_attempts",
        "full_text_time_budget_seconds",
        "full_text_timeout_seconds",
        "license_required",
        "max_candidate_records",
        "maxdate",
        "mindate",
        "reldate",
        "retstart",
        "skip_retracted",
        "sort",
        "tool",
    },
    "unpaywall": {
        "email",
        "is_oa",
    },
    "clinicaltrials_gov": {
        "require_policy_match",
        "search_area",
    },
    "pubchem": {
        "records_per_term",
        "require_exact_match",
    },
    "chembl": {
        "activities_per_molecule",
        "assay_types",
        "cell_line_records_per_molecule",
        "cell_line_scan_limit",
        "cell_line_standard_types",
        "cell_line_terms",
        "include_cell_line_assays",
        "min_pchembl",
        "molecules_per_term",
        "standard_types",
        "target_chembl_ids",
        "target_organisms",
    },
    "openfda_animal_events": {
        "per_term",
        "search",
        "species",
    },
    "geo": {
        "api_key",
        "db",
        "email",
        "retstart",
        "sort",
        "tool",
    },
    "sra": {
        "api_key",
        "db",
        "email",
        "retstart",
        "sort",
        "tool",
    },
    "uniprot": {
        "dedupe_gene_organism",
        "organism_ids",
        "require_gene_match",
        "reviewed",
        "size_per_term",
    },
    "rcsb_pdb": {
        "require_protein_entity",
        "require_target_match",
        "rows_per_term",
    },
}

_INTERNAL_QUERY_PARAM_KEYS = {
    "evidence_refs",
    "followup_lane",
    "include_human_angiosarcoma",
    "lane",
    "lead_id",
    "operator",
    "origin_agent_run_id",
    "origin_evaluator_agent_run_id",
    "origin_evaluator_review_id",
    "origin_review_id",
    "priority",
    "queue_item_id",
    "required_terms",
    "refinement_attempt",
    "refinement_source",
    "source_pack_request",
    "validation_gap",
    "why_this_query_exists",
}


def with_source_safe_query_params(query: SourceQuery) -> SourceQuery:
    """Return a query whose params are safe to pass to the external source API."""

    if query.track not in {"validation_gap", "research_followup"}:
        return query
    safe_params = source_safe_query_params(query)
    if safe_params == query.query_params:
        return query
    return query.model_copy(update={"query_params": safe_params})


def source_safe_query_params(query: SourceQuery) -> dict[str, Any]:
    params = {key: value for key, value in (query.query_params or {}).items() if value is not None}
    allowlist = _SOURCE_QUERY_PARAM_ALLOWLISTS.get(query.source_key)
    if allowlist is None:
        return {key: value for key, value in params.items() if key not in _INTERNAL_QUERY_PARAM_KEYS}
    return {key: value for key, value in params.items() if key in allowlist}
