"""TWOG-owned research primitive services.

This module is intentionally small for the first slice: it adds the traceable
entity lookup primitive without replacing the existing chunk-level entity
resolution job.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import re
import unicodedata
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import (
    EntityAlias,
    EntityLookupIndexRequest,
    EntityLookupIndexResult,
    EntityLookupIndexSourceSummary,
    EntityLookupFailure,
    EntityLookupRequest,
    EntityLookupResponse,
    EntityLookupResult,
    PrimitiveCallEvent,
    ResearchObject,
    ResolvedEntity,
    SourceVersionRecord,
)
from .local_store import stable_json_hash
from .repository import ResearchRepository


_GREEK_NORMALIZATIONS = {
    "α": "alpha",
    "β": "beta",
    "γ": "gamma",
    "δ": "delta",
    "κ": "kappa",
}

DEFAULT_ENTITY_LOOKUP_INDEX_SOURCE_KEYS = (
    "hgnc",
    "vgnc",
    "ncbi_gene",
    "ensembl_xrefs",
    "uniprot",
    "pubchem",
    "chembl",
    "unichem",
    "mondo",
    "doid",
    "reactome",
    "wikipathways",
)


@dataclass(frozen=True)
class _EntityIndexSpec:
    entity_type: str
    canonical_name: str
    normalized_key: str
    aliases: tuple[str, ...]
    external_ids: dict[str, str] = field(default_factory=dict)
    organism_taxid: str | None = None
    source_key: str | None = None
    source_version: str = "unknown"
    metadata: dict[str, Any] = field(default_factory=dict)


def materialize_entity_lookup_index(
    repository: ResearchRepository,
    request: EntityLookupIndexRequest | None = None,
) -> EntityLookupIndexResult:
    """Materialize source-derived entity aliases into TWOG's lookup index."""

    request = request or EntityLookupIndexRequest()
    source_keys = request.source_keys or list(DEFAULT_ENTITY_LOOKUP_INDEX_SOURCE_KEYS)
    summaries: list[EntityLookupIndexSourceSummary] = []
    errors: list[str] = []
    total_entities = 0
    total_aliases = 0
    source_versions_upserted = 0
    total_records = 0

    for source_key in source_keys:
        source_summary = EntityLookupIndexSourceSummary(source_key=source_key)
        source_versions: set[str] = set()
        seen_entities: set[tuple[str, str]] = set()
        seen_aliases: set[tuple[str, str, str]] = set()
        try:
            objects = repository.list_research_objects(source_key=source_key, limit=request.limit_per_source)
            source_summary.records_seen = len(objects)
            total_records += len(objects)
            for obj in objects:
                source_version = _source_version_for_object(obj)
                source_versions.add(source_version)
                for spec in _entity_specs_from_object(obj, source_version=source_version):
                    entity = _upsert_index_entity(repository, spec)
                    entity_key = (entity.entity_type, entity.normalized_key)
                    if entity_key not in seen_entities:
                        seen_entities.add(entity_key)
                        source_summary.entities_upserted += 1
                    for alias_text in _unique_aliases(spec.aliases):
                        alias = repository.upsert_entity_alias(
                            EntityAlias(
                                alias_id=_stable_uuid("entity-alias", entity.entity_type, alias_text, entity.normalized_key),
                                entity_id=entity.entity_id,
                                entity_type=entity.entity_type,
                                alias=alias_text,
                                alias_normalized=normalize_entity_query(alias_text),
                                canonical_name=entity.canonical_name,
                                normalized_key=entity.normalized_key,
                                resolver_name=spec.source_key or source_key,
                                resolver_version=spec.source_version,
                                metadata={
                                    "source_key": spec.source_key or source_key,
                                    "source_version": spec.source_version,
                                    "organism_taxid": spec.organism_taxid,
                                    **spec.external_ids,
                                },
                            )
                        )
                        alias_key = (alias.entity_type, alias.alias_normalized, alias.normalized_key)
                        if alias_key not in seen_aliases:
                            seen_aliases.add(alias_key)
                            source_summary.aliases_upserted += 1
        except Exception as exc:
            message = f"{source_key}: {exc}"
            source_summary.errors.append(message)
            errors.append(message)

        source_summary.source_version = _summarize_source_versions(source_versions)
        if source_versions:
            repository.upsert_source_version(
                SourceVersionRecord(
                    source_key=source_key,
                    source_version=source_summary.source_version or "unknown",
                    materialized_at=datetime.now(UTC),
                    metadata={
                        "source_versions": sorted(source_versions),
                        "records_seen": source_summary.records_seen,
                        "entities_upserted": source_summary.entities_upserted,
                        "aliases_upserted": source_summary.aliases_upserted,
                        "primitive": "entity_lookup_index",
                    },
                )
            )
            source_versions_upserted += 1
        total_entities += source_summary.entities_upserted
        total_aliases += source_summary.aliases_upserted
        summaries.append(source_summary)

    return EntityLookupIndexResult(
        request=request,
        source_summaries=summaries,
        records_seen=total_records,
        entities_upserted=total_entities,
        aliases_upserted=total_aliases,
        source_versions_upserted=source_versions_upserted,
        errors=errors,
    )


def resolve_entity_lookup(repository: ResearchRepository, request: EntityLookupRequest) -> EntityLookupResponse:
    """Resolve one biomedical entity against TWOG-owned aliases and provenance."""

    request_payload = request.model_dump(mode="json")
    request_hash = stable_json_hash(request_payload)
    source_versions = {
        record.source_key: record.source_version
        for record in repository.list_source_versions(limit=None)
    }
    try:
        response = _resolve_entity_lookup(repository, request)
        event = repository.create_primitive_call_event(
            PrimitiveCallEvent(
                primitive_name="entity_lookup",
                status="completed",
                request_hash=request_hash,
                result_hash=stable_json_hash(response.model_dump(mode="json")),
                agent_run_id=request.agent_run_id,
                source_versions=source_versions,
                input_payload=request_payload,
                output_payload=response.model_dump(mode="json"),
            )
        )
        return response.model_copy(update={"event_id": event.event_id})
    except Exception as exc:
        failure = EntityLookupFailure(
            query=request.query,
            category=request.category,
            organism=request.organism,
            reason="no_match",
            candidates=[],
        )
        response = EntityLookupResponse(request=request, failure=failure)
        event = repository.create_primitive_call_event(
            PrimitiveCallEvent(
                primitive_name="entity_lookup",
                status="failed",
                request_hash=request_hash,
                agent_run_id=request.agent_run_id,
                source_versions=source_versions,
                input_payload=request_payload,
                output_payload=response.model_dump(mode="json"),
                errors=[str(exc)],
            )
        )
        return response.model_copy(update={"event_id": event.event_id})


def resolve_entity_lookup_bulk(
    repository: ResearchRepository,
    requests: list[EntityLookupRequest],
) -> list[EntityLookupResponse]:
    """Resolve many biomedical entities with one stable service boundary."""

    return [resolve_entity_lookup(repository, request) for request in requests]


def _entity_specs_from_object(obj: ResearchObject, *, source_version: str) -> list[_EntityIndexSpec]:
    source_key = obj.source_key or "unknown"
    if source_key in {"hgnc", "vgnc", "ncbi_gene", "ensembl_xrefs", "uniprot"}:
        return _target_specs_from_object(obj, source_version=source_version)
    if source_key in {"pubchem", "unichem"}:
        return _compound_specs_from_object(obj, source_version=source_version)
    if source_key == "chembl":
        specs = _compound_specs_from_object(obj, source_version=source_version)
        specs.extend(_target_specs_from_object(obj, source_version=source_version))
        return specs
    if source_key in {"mondo", "doid"}:
        return _disease_specs_from_object(obj, source_version=source_version)
    if source_key in {"reactome", "wikipathways"}:
        return _pathway_specs_from_object(obj, source_version=source_version)
    return []


def _target_specs_from_object(obj: ResearchObject, *, source_version: str) -> list[_EntityIndexSpec]:
    metadata = obj.metadata
    identifiers = obj.identifiers
    symbol = _first_text(
        metadata.get("symbol"),
        identifiers.get("gene_symbol"),
        metadata.get("target_gene"),
        _first_list_value(metadata.get("gene_names")),
        metadata.get("query_term"),
        metadata.get("source_query"),
    )
    if not symbol:
        return []
    organism_taxid = _organism_from_metadata({**identifiers, **metadata})
    canonical_name = symbol.upper() if len(symbol) <= 12 and not symbol.startswith("ENS") else symbol
    normalized_key = f"target:{organism_taxid or 'unknown'}:{normalize_entity_query(canonical_name)}"
    cross_refs = metadata.get("cross_references") if isinstance(metadata.get("cross_references"), dict) else {}
    external_ids = _canonical_external_ids(identifiers | cross_refs)
    aliases = _unique_aliases(
        (
            canonical_name,
            symbol,
            obj.title,
            obj.abstract,
            metadata.get("name"),
            metadata.get("protein_name"),
            metadata.get("target_pref_name"),
            metadata.get("query_term"),
            metadata.get("source_query"),
            *(_list_texts(metadata.get("aliases"))),
            *(_list_texts(metadata.get("gene_names"))),
            *(_list_texts(cross_refs.get("uniprot_ids"))),
            *external_ids.values(),
        )
    )
    return [
        _EntityIndexSpec(
            entity_type="target",
            canonical_name=canonical_name,
            normalized_key=normalized_key,
            aliases=aliases,
            external_ids=external_ids,
            organism_taxid=organism_taxid,
            source_key=obj.source_key,
            source_version=source_version,
            metadata={
                "source_object_id": str(obj.id),
                "source_key": obj.source_key,
                "source_version": source_version,
                "organism_taxid": organism_taxid,
            },
        )
    ]


def _compound_specs_from_object(obj: ResearchObject, *, source_version: str) -> list[_EntityIndexSpec]:
    metadata = obj.metadata
    identifiers = obj.identifiers
    compound_name = _first_text(
        metadata.get("molecule_pref_name"),
        metadata.get("query_term"),
        obj.title,
        identifiers.get("chembl_molecule_id"),
        identifiers.get("pubchem_cid"),
        identifiers.get("source_compound_id"),
    )
    if not compound_name:
        return []
    external_ids = _canonical_external_ids(identifiers | metadata)
    stable_identity = (
        external_ids.get("inchikey")
        or external_ids.get("chembl")
        or external_ids.get("pubchem")
        or identifiers.get("source_compound_id")
        or normalize_entity_query(compound_name)
    )
    aliases = _unique_aliases(
        (
            compound_name,
            obj.title,
            metadata.get("query_term"),
            metadata.get("iupac_name"),
            metadata.get("canonical_smiles"),
            identifiers.get("chembl_molecule_id"),
            identifiers.get("pubchem_cid"),
            identifiers.get("source_compound_id"),
            *(_list_texts(metadata.get("synonyms"))),
        )
    )
    return [
        _EntityIndexSpec(
            entity_type="compound",
            canonical_name=str(compound_name),
            normalized_key=f"compound:{normalize_entity_query(str(stable_identity))}",
            aliases=aliases,
            external_ids=external_ids,
            source_key=obj.source_key,
            source_version=source_version,
            metadata={
                "source_object_id": str(obj.id),
                "source_key": obj.source_key,
                "source_version": source_version,
            },
        )
    ]


def _disease_specs_from_object(obj: ResearchObject, *, source_version: str) -> list[_EntityIndexSpec]:
    metadata = obj.metadata
    identifiers = obj.identifiers
    label = _first_text(metadata.get("label"), obj.title, metadata.get("query_term"))
    if not label:
        return []
    label = re.sub(r"\s+ontology term$", "", label, flags=re.IGNORECASE).strip()
    external_ids = _canonical_external_ids(identifiers | metadata)
    stable_identity = external_ids.get("ontology") or identifiers.get("ontology_id") or normalize_entity_query(label)
    aliases = _unique_aliases((label, obj.title, metadata.get("query_term"), *(_list_texts(metadata.get("synonyms")))))
    return [
        _EntityIndexSpec(
            entity_type="disease",
            canonical_name=label,
            normalized_key=f"disease:{normalize_entity_query(str(stable_identity))}",
            aliases=aliases,
            external_ids=external_ids,
            source_key=obj.source_key,
            source_version=source_version,
            metadata={
                "source_object_id": str(obj.id),
                "source_key": obj.source_key,
                "source_version": source_version,
            },
        )
    ]


def _pathway_specs_from_object(obj: ResearchObject, *, source_version: str) -> list[_EntityIndexSpec]:
    metadata = obj.metadata
    identifiers = obj.identifiers
    pathway_name = _first_text(metadata.get("pathway_name"), obj.title, metadata.get("query_term"))
    if not pathway_name:
        return []
    external_ids = _canonical_external_ids(identifiers | metadata)
    stable_identity = (
        external_ids.get("reactome")
        or external_ids.get("wikipathways")
        or identifiers.get("source_id")
        or normalize_entity_query(pathway_name)
    )
    aliases = _unique_aliases((pathway_name, obj.title, metadata.get("query_term")))
    return [
        _EntityIndexSpec(
            entity_type="pathway",
            canonical_name=pathway_name,
            normalized_key=f"pathway:{normalize_entity_query(str(stable_identity))}",
            aliases=aliases,
            external_ids=external_ids,
            source_key=obj.source_key,
            source_version=source_version,
            metadata={
                "source_object_id": str(obj.id),
                "source_key": obj.source_key,
                "source_version": source_version,
            },
        )
    ]


def _upsert_index_entity(repository: ResearchRepository, spec: _EntityIndexSpec) -> ResolvedEntity:
    existing = next(
        (
            candidate
            for candidate in repository.list_entities(entity_type=spec.entity_type, query=spec.normalized_key, limit=20)
            if candidate.normalized_key == spec.normalized_key
        ),
        None,
    )
    external_ids = dict(existing.external_ids if existing else {})
    external_ids.update({key: value for key, value in spec.external_ids.items() if value})
    metadata = dict(existing.metadata if existing else {})
    source_keys = set(_list_texts(metadata.get("source_keys")))
    if spec.source_key:
        source_keys.add(spec.source_key)
    metadata.update(spec.metadata)
    metadata["source_keys"] = sorted(source_keys)
    if spec.organism_taxid:
        metadata["organism_taxid"] = spec.organism_taxid
    return repository.upsert_entity(
        ResolvedEntity(
            entity_id=existing.entity_id if existing else _stable_uuid("entity", spec.entity_type, spec.normalized_key),
            entity_type=spec.entity_type,
            canonical_name=existing.canonical_name if existing else spec.canonical_name,
            normalized_key=spec.normalized_key,
            external_ids=external_ids,
            resolver_name="entity_lookup_index",
            resolver_version=spec.source_version,
            confidence=1.0,
            metadata=metadata,
        )
    )


def _source_version_for_object(obj: ResearchObject) -> str:
    value = obj.metadata.get("source_version")
    if value:
        return str(value)
    if obj.source_key in {"hgnc", "vgnc"}:
        return "genenames-rest-current"
    if obj.source_key in {"ncbi_gene"}:
        return "eutils-current"
    if obj.source_key in {"ensembl_xrefs", "ensembl_compara"}:
        return "ensembl-rest-current"
    if obj.source_key in {"reactome"}:
        return "content-service-current"
    if obj.source_key in {"mondo", "doid"}:
        return "ols-current"
    return f"{obj.source_key or 'source'}-current"


def _summarize_source_versions(source_versions: set[str]) -> str | None:
    if not source_versions:
        return None
    versions = sorted(source_versions)
    if len(versions) == 1:
        return versions[0]
    return "mixed:" + ",".join(versions[:5])


def _canonical_external_ids(values: dict[str, Any]) -> dict[str, str]:
    mapped: dict[str, str] = {}
    key_map = {
        "hgnc_id": "hgnc",
        "vgnc_id": "vgnc",
        "entrez_id": "entrezgene",
        "ncbi_gene_id": "entrezgene",
        "geneid": "entrezgene",
        "ensembl_gene_id": "ensembl",
        "uniprot_accession": "uniprot",
        "pubchem_cid": "pubchem",
        "chembl_molecule_id": "chembl",
        "chembl_target_id": "chembl_target",
        "inchikey": "inchikey",
        "ontology_id": "ontology",
        "reactome_id": "reactome",
        "wikipathways_id": "wikipathways",
    }
    for key, value in values.items():
        if value is None:
            continue
        if isinstance(value, list):
            if not value:
                continue
            value = value[0]
        if isinstance(value, dict):
            continue
        mapped_key = key_map.get(key)
        if mapped_key:
            mapped[mapped_key] = str(value)
    return mapped


def _stable_uuid(*parts: object) -> UUID:
    return uuid5(NAMESPACE_URL, "twog:research-primitives:" + ":".join(str(part) for part in parts))


def _first_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, list | tuple):
            nested = _first_list_value(value)
            if nested:
                return nested
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _first_list_value(value: Any) -> str | None:
    for item in _list_texts(value):
        return item
    return None


def _list_texts(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str | int | float):
        text = str(value).strip()
        return [text] if text else []
    if isinstance(value, dict):
        return []
    if isinstance(value, list | tuple | set):
        result: list[str] = []
        for item in value:
            result.extend(_list_texts(item))
        return result
    text = str(value).strip()
    return [text] if text else []


def _unique_aliases(values: tuple[Any, ...] | list[Any]) -> tuple[str, ...]:
    aliases: list[str] = []
    seen: set[str] = set()
    for value in values:
        for alias in _list_texts(value):
            alias = _clean_alias(alias)
            normalized = normalize_entity_query(alias)
            if not alias or not normalized or len(alias) > 250 or normalized in seen:
                continue
            seen.add(normalized)
            aliases.append(alias)
    return tuple(aliases)


def _clean_alias(value: str) -> str:
    text = re.sub(r"\s+", " ", value).strip()
    for suffix in (
        " gene nomenclature",
        " NCBI Gene record",
        " Ensembl xref",
        " ontology term",
    ):
        if text.lower().endswith(suffix.lower()):
            text = text[: -len(suffix)].strip()
    return text


def _resolve_entity_lookup(repository: ResearchRepository, request: EntityLookupRequest) -> EntityLookupResponse:
    normalized = normalize_entity_query(request.query)
    entity_type = request.category
    aliases = repository.list_entity_aliases(entity_type=entity_type, query=normalized, limit=100)
    exact = [
        alias
        for alias in aliases
        if alias.alias_normalized == normalized or alias.normalized_key == normalized
    ]
    if exact:
        return _response_from_aliases(repository, request, exact, match_type="exact", confidence=1.0)

    cross_ref = _cross_reference_matches(repository, request, normalized)
    if cross_ref:
        return _response_from_aliases(repository, request, cross_ref, match_type="cross_ref", confidence=1.0)

    fuzzy_candidates = _fuzzy_alias_matches(repository, request, normalized)
    if fuzzy_candidates:
        best_distance = min(distance for _, distance in fuzzy_candidates)
        best = [alias for alias, distance in fuzzy_candidates if distance == best_distance]
        confidence = max(0.0, min(0.85, 1.0 - (best_distance * 0.1)))
        if confidence < request.min_confidence:
            candidates = [
                _result_from_alias(repository, request, alias, match_type="fuzzy", confidence=confidence)
                for alias in best
            ]
            return EntityLookupResponse(
                request=request,
                failure=EntityLookupFailure(
                    query=request.query,
                    category=request.category,
                    organism=request.organism,
                    reason="low_confidence",
                    candidates=candidates,
                ),
            )
        return _response_from_aliases(repository, request, best, match_type="fuzzy", confidence=confidence)

    return EntityLookupResponse(
        request=request,
        failure=EntityLookupFailure(
            query=request.query,
            category=request.category,
            organism=request.organism,
            reason="no_match",
        ),
    )


def _response_from_aliases(
    repository: ResearchRepository,
    request: EntityLookupRequest,
    aliases: list[EntityAlias],
    *,
    match_type: str,
    confidence: float,
) -> EntityLookupResponse:
    source_backed_aliases = [alias for alias in aliases if alias.resolver_name != "local_deterministic_entity_resolver"]
    if source_backed_aliases:
        aliases = source_backed_aliases
    by_key: dict[str, EntityAlias] = {}
    for alias in aliases:
        if _organism_matches(alias, request.organism):
            by_key.setdefault(alias.normalized_key, alias)
    if not by_key:
        by_key = {alias.normalized_key: alias for alias in aliases}
    results = [
        _result_from_alias(repository, request, alias, match_type=match_type, confidence=confidence)
        for alias in by_key.values()
    ]
    if len(results) == 1:
        result = results[0]
        if result.match_confidence < request.min_confidence:
            return EntityLookupResponse(
                request=request,
                failure=EntityLookupFailure(
                    query=request.query,
                    category=request.category,
                    organism=request.organism,
                    reason="low_confidence",
                    candidates=results,
                ),
            )
        return EntityLookupResponse(request=request, result=result)
    return EntityLookupResponse(
        request=request,
        failure=EntityLookupFailure(
            query=request.query,
            category=request.category,
            organism=request.organism,
            reason="ambiguous",
            candidates=results,
        ),
    )


def _result_from_alias(
    repository: ResearchRepository,
    request: EntityLookupRequest,
    alias: EntityAlias,
    *,
    match_type: str,
    confidence: float,
) -> EntityLookupResult:
    entity = next(
        (
            candidate
            for candidate in repository.list_entities(entity_type=alias.entity_type, query=alias.normalized_key, limit=10)
            if candidate.normalized_key == alias.normalized_key
        ),
        None,
    )
    metadata = alias.metadata | (entity.metadata if entity else {})
    alternate_ids = dict(entity.external_ids if entity else {})
    for key, value in metadata.items():
        if key.endswith("_id") or key in {"uniprot", "hgnc", "vgnc", "ensembl", "ncbi_gene", "chembl", "pubchem"}:
            if value is not None and isinstance(value, str | int | float):
                alternate_ids[key] = str(value)
    return EntityLookupResult(
        query=request.query,
        canonical_id=_canonical_id(alias, alternate_ids),
        canonical_title=alias.canonical_name,
        category=_category_for_entity_type(alias.entity_type),
        organism=_organism_from_metadata(metadata) or request.organism,
        match_type=match_type,  # type: ignore[arg-type]
        match_confidence=confidence,
        source_table=alias.resolver_name,
        source_version=alias.resolver_version,
        alternate_ids=alternate_ids,
        resolved_at=datetime.now(UTC),
    )


def _cross_reference_matches(
    repository: ResearchRepository,
    request: EntityLookupRequest,
    normalized: str,
) -> list[EntityAlias]:
    if not re.match(r"^(chembl|pubchem|uniprot|hgnc|vgnc|ensembl|entrezgene|ncbi)[_: -]?[a-z0-9_.-]+$", normalized):
        return []
    compact = re.sub(r"[^a-z0-9]+", "", normalized)
    matches: list[EntityAlias] = []
    for alias in repository.list_entity_aliases(entity_type=request.category, limit=5000):
        values = [alias.alias_normalized, alias.normalized_key]
        values.extend(str(value).lower() for value in alias.metadata.values() if isinstance(value, str | int | float))
        if any(re.sub(r"[^a-z0-9]+", "", value) == compact for value in values):
            matches.append(alias)
    return matches


def _fuzzy_alias_matches(
    repository: ResearchRepository,
    request: EntityLookupRequest,
    normalized: str,
) -> list[tuple[EntityAlias, int]]:
    candidates: list[tuple[EntityAlias, int]] = []
    for alias in repository.list_entity_aliases(entity_type=request.category, limit=5000):
        if abs(len(alias.alias_normalized) - len(normalized)) > 2:
            continue
        distance = _levenshtein_distance(normalized, alias.alias_normalized, max_distance=2)
        if distance <= 2:
            candidates.append((alias, distance))
    candidates.sort(key=lambda item: (item[1], item[0].canonical_name))
    return candidates[:10]


def normalize_entity_query(query: str) -> str:
    text = unicodedata.normalize("NFKD", query.strip().lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    for source, target in _GREEK_NORMALIZATIONS.items():
        text = text.replace(source, target)
    text = re.sub(r"[\u2010-\u2015]", "-", text)
    text = re.sub(r"[^a-z0-9:_+.-]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _levenshtein_distance(left: str, right: str, *, max_distance: int) -> int:
    if abs(len(left) - len(right)) > max_distance:
        return max_distance + 1
    previous = list(range(len(right) + 1))
    for i, left_char in enumerate(left, start=1):
        current = [i]
        row_min = i
        for j, right_char in enumerate(right, start=1):
            cost = 0 if left_char == right_char else 1
            value = min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + cost)
            current.append(value)
            row_min = min(row_min, value)
        if row_min > max_distance:
            return max_distance + 1
        previous = current
    return previous[-1]


def _category_for_entity_type(entity_type: str) -> str:
    if entity_type in {"compound", "target", "disease", "pathway"}:
        return entity_type
    if entity_type in {"biomarker", "gene", "protein"}:
        return "target"
    return "pathway"


def _canonical_id(alias: EntityAlias, alternate_ids: dict[str, str]) -> str:
    for key in (
        "entrezgene",
        "ncbi_gene",
        "hgnc",
        "vgnc",
        "uniprot",
        "chembl",
        "pubchem",
        "ensembl",
        "ontology",
        "reactome",
        "wikipathways",
    ):
        value = alternate_ids.get(key)
        if value:
            return f"{key}:{value}" if ":" not in value else value
    if alias.normalized_key.startswith(f"{alias.entity_type}:"):
        return alias.normalized_key
    return f"{alias.entity_type}:{alias.normalized_key}"


def _organism_from_metadata(metadata: dict[str, object]) -> str | None:
    for key in ("organism_taxid", "taxonomy_id", "taxid"):
        value = metadata.get(key)
        if value is not None:
            return str(value)
    organism = str(metadata.get("organism") or "").lower()
    if "homo sapiens" in organism or organism == "human":
        return "9606"
    if "canis" in organism or "dog" in organism or "canine" in organism:
        return "9615"
    return None


def _organism_matches(alias: EntityAlias, organism: str | None) -> bool:
    if not organism:
        return True
    return _organism_from_metadata(alias.metadata) in {None, organism}
