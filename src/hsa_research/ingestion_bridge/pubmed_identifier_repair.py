"""Repair PubMed identifier metadata after harvester identifier-scope fixes."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, datetime
import xml.etree.ElementTree as ET

from .contracts import (
    PubMedIdentifierRepairItem,
    PubMedIdentifierRepairRequest,
    PubMedIdentifierRepairResult,
    ResearchObject,
)
from .harvesters_v2 import PubMedHarvesterV2, _get_text
from .repository import ResearchRepository


IdentifierFetcher = Callable[[list[str]], dict[str, dict[str, str]]]


def repair_pubmed_identifier_metadata(
    repository: ResearchRepository,
    request: PubMedIdentifierRepairRequest,
    *,
    identifier_fetcher: IdentifierFetcher | None = None,
) -> PubMedIdentifierRepairResult:
    """Refresh PubMed DOI/PMCID payloads and move PubMed rows to PMID dedupe keys."""

    result = PubMedIdentifierRepairResult(dry_run=request.dry_run)
    objects = _candidate_pubmed_objects(repository, request)
    result.scanned_objects = len(objects)
    pmids = _dedupe_strings([obj.identifiers.get("pmid", "") for obj in objects if obj.identifiers.get("pmid")])
    fetcher = identifier_fetcher or (lambda batch: _fetch_pubmed_identifier_map(batch))
    fetched: dict[str, dict[str, str]] = {}
    for batch in _batches(pmids, request.batch_size):
        try:
            fetched.update(fetcher(batch))
        except Exception as exc:
            message = f"PubMed identifier fetch failed for {','.join(batch)}: {exc}"
            result.errors.append(message)
    result.fetched_pmids = len(fetched)

    for obj in objects:
        item = _repair_one(repository, obj, fetched, request)
        result.items.append(item)
        if item.status == "clean":
            result.clean += 1
        elif item.status == "repaired":
            result.repaired += 1
        elif item.status == "would_repair":
            result.would_repair += 1
        elif item.status == "conflict":
            result.conflicts += 1
            if item.error:
                result.errors.append(item.error)
        elif item.status == "failed":
            result.failed += 1
            if item.error:
                result.errors.append(item.error)
        else:
            result.skipped += 1
    return result


def _candidate_pubmed_objects(
    repository: ResearchRepository,
    request: PubMedIdentifierRepairRequest,
) -> list[ResearchObject]:
    requested_pmids = {pmid.strip() for pmid in request.pmids if pmid.strip()}
    candidates = repository.list_research_objects(source_key="pubmed", limit=request.limit)
    if requested_pmids:
        candidates = [obj for obj in candidates if obj.identifiers.get("pmid") in requested_pmids]
    return candidates


def _repair_one(
    repository: ResearchRepository,
    obj: ResearchObject,
    fetched: dict[str, dict[str, str]],
    request: PubMedIdentifierRepairRequest,
) -> PubMedIdentifierRepairItem:
    old_identifiers = {key: str(value) for key, value in obj.identifiers.items() if value}
    pmid = old_identifiers.get("pmid")
    if not pmid:
        return PubMedIdentifierRepairItem(
            object_id=obj.id,
            pmid="",
            status="skipped",
            old_dedupe_key=obj.dedupe_key,
            old_identifiers=old_identifiers,
            error="Missing PMID.",
        )
    canonical = fetched.get(pmid)
    if not canonical:
        return PubMedIdentifierRepairItem(
            object_id=obj.id,
            pmid=pmid,
            status="skipped",
            old_dedupe_key=obj.dedupe_key,
            old_identifiers=old_identifiers,
            error="No PubMed metadata was fetched for PMID.",
        )

    new_identifiers = _merged_pubmed_identifiers(old_identifiers, canonical)
    new_dedupe_key = f"pmid:{pmid.lower()}"
    changed = old_identifiers != new_identifiers or obj.dedupe_key != new_dedupe_key
    if not changed:
        return PubMedIdentifierRepairItem(
            object_id=obj.id,
            pmid=pmid,
            status="clean",
            old_dedupe_key=obj.dedupe_key,
            new_dedupe_key=new_dedupe_key,
            old_identifiers=old_identifiers,
            new_identifiers=new_identifiers,
        )
    if request.dry_run:
        return PubMedIdentifierRepairItem(
            object_id=obj.id,
            pmid=pmid,
            status="would_repair",
            old_dedupe_key=obj.dedupe_key,
            new_dedupe_key=new_dedupe_key,
            old_identifiers=old_identifiers,
            new_identifiers=new_identifiers,
        )

    metadata = {
        **obj.metadata,
        "pubmed_identifier_repair": {
            "repaired_at": datetime.now(UTC).isoformat(),
            "old_identifiers": old_identifiers,
            "new_identifiers": new_identifiers,
            "old_dedupe_key": obj.dedupe_key,
            "new_dedupe_key": new_dedupe_key,
        },
    }
    updated = obj.model_copy(
        update={
            "identifiers": new_identifiers,
            "dedupe_key": new_dedupe_key,
            "metadata": metadata,
        }
    )
    try:
        saved = repository.update_research_object(updated)
    except ValueError as exc:
        fallback = updated.model_copy(
            update={
                "dedupe_key": obj.dedupe_key,
                "metadata": {
                    **metadata,
                    "pubmed_identifier_repair": {
                        **metadata["pubmed_identifier_repair"],
                        "dedupe_key_conflict": str(exc),
                        "dedupe_key_moved": False,
                    },
                },
            }
        )
        try:
            saved = repository.update_research_object(fallback)
        except Exception as fallback_exc:
            return PubMedIdentifierRepairItem(
                object_id=obj.id,
                pmid=pmid,
                status="conflict",
                old_dedupe_key=obj.dedupe_key,
                new_dedupe_key=new_dedupe_key,
                old_identifiers=old_identifiers,
                new_identifiers=new_identifiers,
                error=f"{exc}; fallback identifier repair failed: {fallback_exc}",
            )
        if saved is not None:
            return PubMedIdentifierRepairItem(
                object_id=obj.id,
                pmid=pmid,
                status="repaired",
                old_dedupe_key=obj.dedupe_key,
                new_dedupe_key=obj.dedupe_key,
                old_identifiers=old_identifiers,
                new_identifiers=new_identifiers,
                error=f"Dedupe key conflict; identifiers repaired without dedupe move: {exc}",
            )
        return PubMedIdentifierRepairItem(
            object_id=obj.id,
            pmid=pmid,
            status="conflict",
            old_dedupe_key=obj.dedupe_key,
            new_dedupe_key=new_dedupe_key,
            old_identifiers=old_identifiers,
            new_identifiers=new_identifiers,
            error=str(exc),
        )
    except Exception as exc:
        return PubMedIdentifierRepairItem(
            object_id=obj.id,
            pmid=pmid,
            status="failed",
            old_dedupe_key=obj.dedupe_key,
            new_dedupe_key=new_dedupe_key,
            old_identifiers=old_identifiers,
            new_identifiers=new_identifiers,
            error=str(exc),
        )
    if saved is None:
        return PubMedIdentifierRepairItem(
            object_id=obj.id,
            pmid=pmid,
            status="failed",
            old_dedupe_key=obj.dedupe_key,
            new_dedupe_key=new_dedupe_key,
            old_identifiers=old_identifiers,
            new_identifiers=new_identifiers,
            error="Research object disappeared before update.",
        )
    return PubMedIdentifierRepairItem(
        object_id=obj.id,
        pmid=pmid,
        status="repaired",
        old_dedupe_key=obj.dedupe_key,
        new_dedupe_key=new_dedupe_key,
        old_identifiers=old_identifiers,
        new_identifiers=new_identifiers,
    )


def _merged_pubmed_identifiers(
    old_identifiers: dict[str, str],
    canonical: dict[str, str],
) -> dict[str, str]:
    merged = {
        key: value
        for key, value in old_identifiers.items()
        if key not in {"doi", "pmcid", "source_id", "pmid"} and value
    }
    for key in ("pmid", "doi", "pmcid", "source_id"):
        value = canonical.get(key)
        if value:
            merged[key] = value
    return merged


def _fetch_pubmed_identifier_map(pmids: list[str]) -> dict[str, dict[str, str]]:
    if not pmids:
        return {}
    xml = _get_text(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
        {"db": "pubmed", "id": ",".join(pmids), "retmode": "xml"},
    )
    harvester = PubMedHarvesterV2()
    records: dict[str, dict[str, str]] = {}
    for article in ET.fromstring(xml).findall(".//PubmedArticle"):
        record = harvester.normalize(article)
        identifiers = record.research_object.identifiers
        pmid = identifiers.get("pmid")
        if pmid:
            records[pmid] = {key: str(value) for key, value in identifiers.items() if value}
    return records


def _batches(values: list[str], batch_size: int) -> Iterable[list[str]]:
    for index in range(0, len(values), batch_size):
        yield values[index : index + batch_size]


def _dedupe_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        deduped.append(normalized)
        seen.add(normalized)
    return deduped
