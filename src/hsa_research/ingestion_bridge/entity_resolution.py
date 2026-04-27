"""Deterministic entity resolution for chunked ingestion text.

This layer is intentionally reproducible: it persists canonical entities,
aliases, and chunk-level mentions before model-backed enrichment enters the
system. External resolver APIs such as PubTator are treated as deterministic
annotation sources with resolver provenance, not as reasoning agents.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
import urllib.parse
import urllib.request
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from .claim_extractor import BIOMARKERS, COMPOUNDS, HUMAN_ANALOG_TERMS, PATHWAY_TERMS, TARGETS
from .contracts import (
    DocumentChunk,
    EntityAlias,
    EntityMention,
    EntityResolutionResult,
    ResolvedEntity,
    ResearchObject,
)
from .local_store import SQLiteResearchRepository, stable_json_hash


LOCAL_RESOLVER_NAME = "local_deterministic_entity_resolver"
LOCAL_RESOLVER_VERSION = "0.1"
PUBTATOR_RESOLVER_NAME = "pubtator_biocjson_resolver"
PUBTATOR_RESOLVER_VERSION = "3-api"
PUBTATOR_ENDPOINT = "https://www.ncbi.nlm.nih.gov/research/pubtator3-api/publications/export/biocjson"

STABLE_ID_ORDER = (
    "pubchem_cid",
    "chembl_molecule_id",
    "chembl_target_id",
    "uniprot_accession",
    "pdb_id",
    "ncbi_gene_id",
    "mesh_id",
    "umls_cui",
    "pubtator_identifier",
)

COMPOUND_EXTERNAL_IDS = {
    "propranolol": {"pubchem_cid": "4946"},
    "doxorubicin": {"pubchem_cid": "31703"},
    "paclitaxel": {"pubchem_cid": "36314"},
    "sirolimus": {"pubchem_cid": "5284616"},
}

TARGET_EXTERNAL_IDS = {
    "VEGFA": {"ncbi_gene_id": "7422", "uniprot_accession": "P15692"},
    "KDR": {"ncbi_gene_id": "3791", "uniprot_accession": "P35968"},
    "KIT": {"ncbi_gene_id": "3815", "uniprot_accession": "P10721"},
    "TP53": {"ncbi_gene_id": "7157", "uniprot_accession": "P04637"},
}


@dataclass(frozen=True)
class VocabularyEntry:
    entity_type: str
    canonical_name: str
    aliases: tuple[str, ...]
    external_ids: dict[str, str]
    metadata: dict[str, Any]


def resolve_entities_for_repository(
    repository: SQLiteResearchRepository,
    *,
    source_key: str | None = None,
    limit: int | None = None,
    resolver_profile: str = "local",
) -> EntityResolutionResult:
    """Resolve and persist deterministic entities for repository chunks."""

    result = EntityResolutionResult(
        resolver_name=LOCAL_RESOLVER_NAME,
        resolver_version=LOCAL_RESOLVER_VERSION,
        resolver_profile=resolver_profile,
    )
    chunks = repository.list_document_chunks(source_key=source_key, limit=limit)
    objects = {
        chunk.research_object_id: repository.get_research_object(chunk.research_object_id)
        for chunk in chunks
    }
    pubtator_annotations: dict[str, list[dict[str, Any]]] = {}
    if resolver_profile in {"pubtator", "local_plus_pubtator"}:
        pmids = sorted(
            {
                obj.identifiers["pmid"]
                for obj in objects.values()
                if obj and obj.identifiers.get("pmid")
            }
        )
        try:
            pubtator_annotations = fetch_pubtator_annotations(pmids)
        except Exception as exc:
            result.errors.append(f"pubtator: {exc}")

    for chunk in chunks:
        result.chunks_seen += 1
        obj = objects.get(chunk.research_object_id)
        if obj is None:
            result.errors.append(f"{chunk.id}: missing research object {chunk.research_object_id}")
            continue
        try:
            mentions: list[EntityMention] = []
            if resolver_profile in {"local", "local_plus_pubtator"}:
                mentions.extend(resolve_chunk_with_local_vocabulary(chunk, obj))
            if resolver_profile in {"pubtator", "local_plus_pubtator"} and obj.identifiers.get("pmid"):
                mentions.extend(
                    resolve_chunk_with_pubtator_annotations(
                        chunk,
                        obj,
                        pubtator_annotations.get(obj.identifiers["pmid"], []),
                    )
                )
            if mentions:
                result.chunks_with_mentions += 1
            seen_entities: set[tuple[str, str]] = set()
            seen_aliases: set[tuple[str, str, str]] = set()
            for mention in mentions:
                entity = repository.upsert_entity(
                    ResolvedEntity(
                        entity_id=mention.entity_id or _entity_id(mention.entity_type, mention.normalized_key),
                        entity_type=mention.entity_type,
                        canonical_name=mention.canonical_name,
                        normalized_key=mention.normalized_key,
                        external_ids=mention.external_ids,
                        resolver_name=mention.resolver_name,
                        resolver_version=mention.resolver_version,
                        confidence=mention.confidence,
                        metadata=mention.metadata.get("entity_metadata", {}),
                    )
                )
                result.entities_upserted += int((entity.entity_type, entity.normalized_key) not in seen_entities)
                seen_entities.add((entity.entity_type, entity.normalized_key))
                alias = repository.upsert_entity_alias(
                    EntityAlias(
                        alias_id=_alias_id(entity.entity_type, mention.matched_alias, entity.normalized_key),
                        entity_id=entity.entity_id,
                        entity_type=entity.entity_type,
                        alias=mention.matched_alias,
                        alias_normalized=normalize_text_key(mention.matched_alias),
                        canonical_name=entity.canonical_name,
                        normalized_key=entity.normalized_key,
                        resolver_name=mention.resolver_name,
                        resolver_version=mention.resolver_version,
                        metadata={"source": "entity_resolution"},
                    )
                )
                result.aliases_upserted += int(
                    (alias.entity_type, alias.alias_normalized, alias.normalized_key) not in seen_aliases
                )
                seen_aliases.add((alias.entity_type, alias.alias_normalized, alias.normalized_key))
                repository.upsert_entity_mention(mention.model_copy(update={"entity_id": entity.entity_id}))
                result.mentions_upserted += 1
        except Exception as exc:
            result.errors.append(f"{chunk.id}: {exc}")

    return result


def resolve_chunk_with_local_vocabulary(chunk: DocumentChunk, obj: ResearchObject) -> list[EntityMention]:
    """Return longest-match local deterministic mentions for a chunk."""

    entries = local_vocabulary()
    spans: list[tuple[int, int, VocabularyEntry, str]] = []
    for entry in entries:
        for alias in sorted(set(entry.aliases), key=len, reverse=True):
            if not alias:
                continue
            for match in _iter_alias_matches(chunk.text_content, alias):
                spans.append((match.start(), match.end(), entry, match.group(0)))
    spans.sort(key=lambda item: (item[0], -(item[1] - item[0])))

    accepted: list[tuple[int, int, VocabularyEntry, str]] = []
    occupied: list[tuple[int, int]] = []
    for start, end, entry, matched_text in spans:
        if any(start < used_end and end > used_start for used_start, used_end in occupied):
            continue
        accepted.append((start, end, entry, matched_text))
        occupied.append((start, end))

    return [
        _mention_from_entry(
            chunk,
            obj,
            entry,
            matched_text=matched_text,
            matched_alias=matched_text,
            start=start,
            end=end,
            resolver_name=LOCAL_RESOLVER_NAME,
            resolver_version=LOCAL_RESOLVER_VERSION,
            match_rule="local_dictionary_longest_exact",
            confidence=1.0,
        )
        for start, end, entry, matched_text in accepted
    ]


def fetch_pubtator_annotations(pmids: list[str]) -> dict[str, list[dict[str, Any]]]:
    """Fetch PubTator BioC JSON annotations keyed by PMID."""

    annotations: dict[str, list[dict[str, Any]]] = {}
    for start in range(0, len(pmids), 100):
        batch = pmids[start : start + 100]
        if not batch:
            continue
        query = urllib.parse.urlencode({"pmids": ",".join(batch)})
        request = urllib.request.Request(f"{PUBTATOR_ENDPOINT}?{query}", headers={"User-Agent": "hsa-entity-resolver/0.1"})
        with urllib.request.urlopen(request, timeout=45) as response:
            payload = json.loads(response.read().decode("utf-8"))
        for document in payload.get("documents", []):
            pmid = str(document.get("id") or "")
            document_annotations: list[dict[str, Any]] = []
            for passage in document.get("passages", []):
                for annotation in passage.get("annotations", []):
                    document_annotations.append(annotation)
            if pmid:
                annotations[pmid] = document_annotations
    return annotations


def resolve_chunk_with_pubtator_annotations(
    chunk: DocumentChunk,
    obj: ResearchObject,
    annotations: list[dict[str, Any]],
) -> list[EntityMention]:
    mentions: list[EntityMention] = []
    for annotation in annotations:
        text = str(annotation.get("text") or "").strip()
        if not text:
            continue
        entity_type = _pubtator_entity_type((annotation.get("infons") or {}).get("type"))
        if entity_type is None:
            continue
        for match in _iter_alias_matches(chunk.text_content, text):
            external_ids = _pubtator_external_ids(annotation)
            entry = VocabularyEntry(
                entity_type=entity_type,
                canonical_name=text,
                aliases=(text,),
                external_ids=external_ids,
                metadata={"resolver_source": "pubtator", "payload_hash": stable_json_hash(annotation)},
            )
            mentions.append(
                _mention_from_entry(
                    chunk,
                    obj,
                    entry,
                    matched_text=match.group(0),
                    matched_alias=text,
                    start=match.start(),
                    end=match.end(),
                    resolver_name=PUBTATOR_RESOLVER_NAME,
                    resolver_version=PUBTATOR_RESOLVER_VERSION,
                    match_rule="pubtator_annotation_exact_text",
                    confidence=0.95,
                )
            )
    return mentions


def local_vocabulary() -> list[VocabularyEntry]:
    entries: list[VocabularyEntry] = []
    entries.extend(
        VocabularyEntry(
            entity_type="compound",
            canonical_name=canonical,
            aliases=aliases,
            external_ids=COMPOUND_EXTERNAL_IDS.get(canonical, {}),
            metadata={"source": "claim_extractor.COMPOUNDS"},
        )
        for canonical, aliases in COMPOUNDS.items()
    )
    entries.extend(
        VocabularyEntry(
            entity_type="target",
            canonical_name=canonical,
            aliases=aliases,
            external_ids=TARGET_EXTERNAL_IDS.get(canonical, {}),
            metadata={"source": "claim_extractor.TARGETS"},
        )
        for canonical, aliases in TARGETS.items()
    )
    entries.extend(
        VocabularyEntry(
            entity_type="biomarker",
            canonical_name=canonical,
            aliases=aliases,
            external_ids={},
            metadata={"source": "claim_extractor.BIOMARKERS"},
        )
        for canonical, aliases in BIOMARKERS.items()
    )
    entries.extend(
        VocabularyEntry(
            entity_type="pathway",
            canonical_name=canonical,
            aliases=aliases,
            external_ids={},
            metadata={"source": "claim_extractor.PATHWAY_TERMS"},
        )
        for canonical, aliases in PATHWAY_TERMS.items()
    )
    entries.extend(
        VocabularyEntry(
            entity_type="disease",
            canonical_name=_canonical_disease_name(alias),
            aliases=(alias.strip('"'),),
            external_ids={},
            metadata={"source": "query_policy.HUMAN_VASCULAR_SARCOMA_TERMS"},
        )
        for alias in HUMAN_ANALOG_TERMS
    )
    return entries


def normalize_entity_key(entity_type: str, canonical_name: str, external_ids: dict[str, str] | None = None) -> str:
    ids = external_ids or {}
    for key in STABLE_ID_ORDER:
        value = ids.get(key)
        if value:
            return f"{key}:{str(value).lower()}"
    return f"{entity_type}:{normalize_text_key(canonical_name)}"


def normalize_text_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _mention_from_entry(
    chunk: DocumentChunk,
    obj: ResearchObject,
    entry: VocabularyEntry,
    *,
    matched_text: str,
    matched_alias: str,
    start: int,
    end: int,
    resolver_name: str,
    resolver_version: str,
    match_rule: str,
    confidence: float,
) -> EntityMention:
    normalized_key = normalize_entity_key(entry.entity_type, entry.canonical_name, entry.external_ids)
    entity_id = _entity_id(entry.entity_type, normalized_key)
    return EntityMention(
        mention_id=_mention_id(chunk.id, entry.entity_type, normalized_key, start, end, resolver_name),
        entity_id=entity_id,
        research_object_id=chunk.research_object_id,
        chunk_id=chunk.id,
        chunk_index=chunk.chunk_index,
        section_label=chunk.section_label,
        source_key=obj.source_key,
        entity_type=entry.entity_type,
        canonical_name=entry.canonical_name,
        normalized_key=normalized_key,
        matched_text=matched_text,
        matched_alias=matched_alias,
        chunk_char_start=start,
        chunk_char_end=end,
        external_ids=entry.external_ids,
        resolver_name=resolver_name,
        resolver_version=resolver_version,
        match_rule=match_rule,
        confidence=confidence,
        metadata={
            "content_hash": chunk.content_hash,
            "entity_metadata": entry.metadata,
        },
    )


def _iter_alias_matches(text: str, alias: str) -> list[re.Match[str]]:
    pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(alias)}(?![A-Za-z0-9])", flags=re.I)
    return list(pattern.finditer(text))


def _entity_id(entity_type: str, normalized_key: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"hsa:entity:{entity_type}:{normalized_key}")


def _alias_id(entity_type: str, alias: str, normalized_key: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"hsa:entity-alias:{entity_type}:{normalize_text_key(alias)}:{normalized_key}")


def _mention_id(
    chunk_id: UUID,
    entity_type: str,
    normalized_key: str,
    start: int,
    end: int,
    resolver_name: str,
) -> UUID:
    return uuid5(NAMESPACE_URL, f"hsa:entity-mention:{chunk_id}:{entity_type}:{normalized_key}:{start}:{end}:{resolver_name}")


def _canonical_disease_name(alias: str) -> str:
    cleaned = alias.strip().strip('"')
    if "angiosarcoma" in cleaned.lower():
        return "human angiosarcoma or vascular sarcoma analog"
    return cleaned


def _pubtator_entity_type(value: Any) -> str | None:
    mapping = {
        "gene": "target",
        "chemical": "compound",
        "disease": "disease",
        "species": "species",
        "mutation": "genetic_variant",
        "cellline": "cell_line",
        "cell line": "cell_line",
    }
    return mapping.get(str(value or "").lower())


def _pubtator_external_ids(annotation: dict[str, Any]) -> dict[str, str]:
    infons = annotation.get("infons") or {}
    identifier = infons.get("identifier") or annotation.get("id")
    if not identifier:
        return {}
    return {"pubtator_identifier": str(identifier)}
