"""Bounded omics accession hunter for program evidence gaps."""

from __future__ import annotations

import re
from typing import Any

from .contracts import (
    IngestionResult,
    OmicsAccessionHit,
    OmicsAccessionHuntRequest,
    OmicsAccessionHuntResult,
    ResearchObject,
    ResearchObjectType,
    SourceQuery,
)
from .local_ingest import LocalIngestionPipeline
from .repository import ResearchRepository


DEFAULT_OMICS_SOURCES = ("geo", "sra")


def run_omics_accession_hunt(
    repository: ResearchRepository,
    request: OmicsAccessionHuntRequest,
) -> OmicsAccessionHuntResult:
    """Search GEO/SRA-style sources for accession-level omics evidence.

    The goal is not to reanalyze expression yet. This lane proves whether there
    are durable public accessions worth a downstream omics agent pass.
    """

    queries = _build_source_queries(request)
    selected_queries = queries[: request.max_queries]
    result = OmicsAccessionHuntResult(
        program_id=request.program_id,
        dry_run=request.dry_run,
        query_count=len(selected_queries),
        source_query_count=len(selected_queries),
        source_queries=selected_queries,
    )
    if request.dry_run:
        return result

    pipeline = LocalIngestionPipeline(repository)  # type: ignore[arg-type]
    before_keys = _current_accession_keys(repository, request.source_keys)
    for query in selected_queries:
        try:
            ingestion = pipeline.ingest_query(
                query,
                limit=request.limit_per_query,
                persist_query=request.persist_queries,
            )
        except Exception as exc:
            result.errors.append(f"{query.source_key}:{query.query_name}: {exc}")
            result.negative_queries.append(
                {
                    "source_key": query.source_key,
                    "query_name": query.query_name,
                    "query_text": query.query_text,
                    "error": str(exc),
                }
            )
            continue
        result.ingestion_results.append(ingestion)
        result.raw_records += ingestion.raw_records
        result.research_objects += ingestion.research_objects
        result.document_chunks += ingestion.document_chunks
        if ingestion.errors or ingestion.research_objects < 1:
            result.negative_queries.append(
                {
                    "source_key": query.source_key,
                    "query_name": query.query_name,
                    "query_text": query.query_text,
                    "raw_records": ingestion.raw_records,
                    "research_objects": ingestion.research_objects,
                    "errors": ingestion.errors,
                }
            )

    hits = _collect_accession_hits(repository, request, before_keys)
    result.accession_hits = hits
    result.accession_hit_count = len(hits)
    return result


def _build_source_queries(request: OmicsAccessionHuntRequest) -> list[SourceQuery]:
    query_texts = request.query_texts or _default_query_texts(request)
    source_keys = [
        source_key
        for source_key in (request.source_keys or list(DEFAULT_OMICS_SOURCES))
        if source_key in DEFAULT_OMICS_SOURCES
    ]
    queries: list[SourceQuery] = []
    for index, query_text in enumerate(query_texts, start=1):
        for source_key in source_keys:
            queries.append(
                SourceQuery(
                    source_key=source_key,
                    query_name=f"omics_accession_hunt_{source_key}_{index}_{_slug(query_text)}",
                    query_text=query_text,
                    query_params={
                        "require_policy_match": False,
                        "research_program_id": str(request.program_id) if request.program_id else None,
                        "gene_symbols": request.gene_symbols,
                        "disease_terms": request.disease_terms,
                        "dagster_run_id": request.dagster_run_id,
                        "metadata": request.metadata,
                    },
                    track="omics_accession_hunt",
                    object_type=ResearchObjectType.DATASET,
                    active=False,
                )
            )
    return queries


def _default_query_texts(request: OmicsAccessionHuntRequest) -> list[str]:
    disease_terms = request.disease_terms or ["canine hemangiosarcoma", "human angiosarcoma"]
    gene_terms = request.gene_symbols or ["VIM", "vimentin"]
    omics_terms = ("RNA-seq", "transcriptome", "expression", "ChRO-seq")
    queries: list[str] = []
    for disease in disease_terms:
        queries.append(f'{disease} ("RNA-seq" OR transcriptome OR expression)')
        queries.append(f'{disease} ({" OR ".join(gene_terms)})')
    queries.append(request.topic_query)
    queries.append(f'({" OR ".join(disease_terms[:4])}) ({" OR ".join(omics_terms)})')
    return _dedupe_strings(queries)


def _collect_accession_hits(
    repository: ResearchRepository,
    request: OmicsAccessionHuntRequest,
    before_keys: set[str],
) -> list[OmicsAccessionHit]:
    hits: list[OmicsAccessionHit] = []
    seen: set[str] = set()
    for source_key in request.source_keys:
        for obj in repository.list_research_objects(source_key=source_key, limit=1000):
            accession, identifier_type = _primary_accession(obj)
            if accession is None:
                continue
            hit_key = f"{source_key}:{identifier_type}:{accession.lower()}"
            if hit_key in seen:
                continue
            if before_keys and hit_key in before_keys and not _object_matches_request(obj, request):
                continue
            seen.add(hit_key)
            hits.append(_hit_from_object(obj, accession, identifier_type, request))
    hits.sort(key=lambda hit: (hit.source_key, hit.accession))
    return hits


def _current_accession_keys(repository: ResearchRepository, source_keys: list[str]) -> set[str]:
    keys: set[str] = set()
    for source_key in source_keys:
        for obj in repository.list_research_objects(source_key=source_key, limit=1000):
            accession, identifier_type = _primary_accession(obj)
            if accession:
                keys.add(f"{source_key}:{identifier_type}:{accession.lower()}")
    return keys


def _primary_accession(obj: ResearchObject) -> tuple[str | None, str]:
    identifiers = obj.identifiers or {}
    if obj.source_key == "geo":
        for key in ("geo_accession", "gse", "gds"):
            value = identifiers.get(key)
            if value:
                return str(value), "geo"
    if obj.source_key == "sra":
        for key in ("sra_experiment", "sra_study", "sra_run", "bioproject", "biosample"):
            value = identifiers.get(key)
            if value:
                if key == "bioproject":
                    return str(value), "bioproject"
                if key == "biosample":
                    return str(value), "biosample"
                return str(value), "sra"
    return None, "geo"


def _hit_from_object(
    obj: ResearchObject,
    accession: str,
    identifier_type: str,
    request: OmicsAccessionHuntRequest,
) -> OmicsAccessionHit:
    metadata = obj.metadata or {}
    return OmicsAccessionHit(
        source_key=obj.source_key,
        accession=accession,
        identifier_type=identifier_type,  # type: ignore[arg-type]
        research_object_id=obj.id,
        title=obj.title,
        canonical_url=obj.canonical_url,
        organism=_first_text(metadata.get("organism"), metadata.get("taxon")),
        sample_count=_int_or_none(metadata.get("sample_count")),
        library_strategy=_first_text(metadata.get("library_strategy"), metadata.get("dataset_type")),
        bioproject=obj.identifiers.get("bioproject") if obj.identifiers else None,
        pmid=obj.identifiers.get("pmid") if obj.identifiers else None,
        matched_terms=_matched_terms(obj, request),
        source_query_name=_source_query_name_from_object(obj),
        metadata={
            "source_key": obj.source_key,
            "dedupe_key": obj.dedupe_key,
            "run_accessions": metadata.get("run_accessions", []),
            "sample_accessions": metadata.get("sample_accessions", []),
            "platform_accessions": metadata.get("platform_accessions", []),
            "supplementary_file_types": metadata.get("supplementary_file_types", []),
        },
    )


def _object_matches_request(obj: ResearchObject, request: OmicsAccessionHuntRequest) -> bool:
    return bool(_matched_terms(obj, request))


def _matched_terms(obj: ResearchObject, request: OmicsAccessionHuntRequest) -> list[str]:
    haystack = _object_text(obj).casefold()
    terms: list[str] = []
    for term in [*request.disease_terms, *request.gene_symbols, "rna-seq", "transcriptome", "expression"]:
        normalized = str(term).strip()
        if normalized and normalized.casefold() in haystack:
            terms.append(normalized)
    return _dedupe_strings(terms)


def _object_text(obj: ResearchObject) -> str:
    metadata = obj.metadata or {}
    metadata_bits = []
    for key in (
        "organism",
        "taxon",
        "dataset_type",
        "library_strategy",
        "library_source",
        "sample_titles",
        "study_name",
    ):
        value = metadata.get(key)
        if isinstance(value, list):
            metadata_bits.extend(str(item) for item in value)
        elif value:
            metadata_bits.append(str(value))
    return " ".join([obj.title or "", obj.abstract or "", *metadata_bits])


def _source_query_name_from_object(obj: ResearchObject) -> str | None:
    metadata = obj.metadata or {}
    if isinstance(metadata.get("source_query"), str):
        return metadata["source_query"]
    return None


def _dedupe_strings(values: list[str] | tuple[str, ...]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        key = text.casefold()
        if text and key not in seen:
            deduped.append(text)
            seen.add(key)
    return deduped


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")[:48] or "query"


def _first_text(*values: Any) -> str | None:
    for value in values:
        if value:
            return str(value)
    return None


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
