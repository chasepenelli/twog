"""Backfill legacy local artifacts into Ingestion Bridge v2 storage."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .chunker import chunk_text
from .contracts import BackfillResult, RawSourceRecord, ResearchObject, ResearchObjectType
from .local_store import SQLiteResearchRepository, stable_json_hash

TRACK_PREFIXES = {
    "treatment_": "treatment",
    "early_detection_": "early_detection",
    "supplements_": "supplements",
    "breed_screening_": "breed_screening",
    "cross_cutting_": "cross_cutting",
}


def backfill_papers_json(
    repository: SQLiteResearchRepository,
    path: str | Path = "hsa_research/papers.json",
    *,
    source_key: str = "current_papers",
    limit: int | None = None,
    chunk: bool = True,
) -> BackfillResult:
    """Backfill the legacy papers JSON into raw records and research objects."""

    json_path = Path(path)
    records = json.loads(json_path.read_text())
    if not isinstance(records, list):
        raise ValueError(f"Expected a list of paper records in {json_path}")

    result = BackfillResult(source_key=source_key, path=str(json_path))
    fetch_run_id = repository.create_fetch_run(source_key, json_path.name)
    selected = records[:limit] if limit is not None else records

    for paper in selected:
        if not isinstance(paper, dict):
            result.skipped += 1
            continue
        try:
            raw = _paper_raw_record(paper, source_key)
            raw_id = repository.upsert_raw_record(raw, fetch_run_id)
            obj = _paper_research_object(paper, source_key, raw_id)
            object_id = repository.upsert_research_object(obj, raw_id)
            result.raw_records += 1
            result.research_objects += 1

            if chunk:
                chunk_text_value = _paper_chunk_text(paper)
                for doc_chunk in chunk_text(
                    object_id,
                    chunk_text_value,
                    section_label="title_abstract",
                    metadata={"backfill_source": str(json_path), "source_key": source_key},
                ):
                    repository.upsert_document_chunk(doc_chunk)
                    result.document_chunks += 1
        except Exception as exc:
            result.errors.append(f"{paper.get('pmid') or paper.get('doi') or paper.get('title')}: {exc}")

    repository.finish_fetch_run(
        fetch_run_id,
        "completed" if not result.errors else "completed",
        records_found=len(selected),
        records_inserted=result.raw_records,
    )
    return result


def backfill_deep_dives(
    repository: SQLiteResearchRepository,
    directory: str | Path = "docs/deep_dives",
    *,
    source_key: str = "current_knowledge_entries",
    limit: int | None = None,
    chunk: bool = True,
) -> BackfillResult:
    """Backfill TWOG deep-dive markdown files as knowledge entries."""

    root = Path(directory)
    paths = sorted(path for path in root.glob("*.md") if path.is_file())
    selected = paths[:limit] if limit is not None else paths
    result = BackfillResult(source_key=source_key, path=str(root))
    fetch_run_id = repository.create_fetch_run(source_key, root.name)

    for md_path in selected:
        try:
            content = md_path.read_text()
            title = _markdown_title(content, md_path.stem.replace("_", " ").title())
            summary = _markdown_summary(content)
            track = _track_from_slug(md_path.stem)
            payload = {
                "source_type": "twog_deep_dive",
                "source_name": "TWOG deep-dive analyst",
                "source_url": str(md_path),
                "entry_type": "deep_dive",
                "entity_name": md_path.stem,
                "title": title,
                "content": content,
                "summary": summary,
                "metadata": {"track": track, "file_path": str(md_path)},
            }
            raw = RawSourceRecord(
                source_key=source_key,
                source_record_id=md_path.stem,
                source_url=str(md_path),
                content_hash=stable_json_hash(payload),
                raw_payload=payload,
            )
            raw_id = repository.upsert_raw_record(raw, fetch_run_id)
            obj = ResearchObject(
                object_type=ResearchObjectType.KNOWLEDGE_ENTRY,
                title=title,
                abstract=summary,
                canonical_url=str(md_path),
                source_key=source_key,
                raw_record_id=raw_id,
                dedupe_key=f"deep_dive:{md_path.stem}",
                identifiers={"source_id": md_path.stem},
                metadata={"track": track, "entry_type": "deep_dive", "file_path": str(md_path)},
            )
            object_id = repository.upsert_research_object(obj, raw_id)
            result.raw_records += 1
            result.research_objects += 1

            if chunk:
                for doc_chunk in chunk_text(
                    object_id,
                    content,
                    section_label="markdown",
                    metadata={"backfill_source": str(md_path), "track": track, "source_key": source_key},
                ):
                    repository.upsert_document_chunk(doc_chunk)
                    result.document_chunks += 1
        except Exception as exc:
            result.errors.append(f"{md_path}: {exc}")

    repository.finish_fetch_run(
        fetch_run_id,
        "completed" if not result.errors else "completed",
        records_found=len(selected),
        records_inserted=result.raw_records,
    )
    return result


def _paper_raw_record(paper: dict[str, Any], source_key: str) -> RawSourceRecord:
    source_record_id = str(paper.get("pmid") or paper.get("doi") or paper.get("_dedup_key") or paper.get("title") or "")
    return RawSourceRecord(
        source_key=source_key,
        source_record_id=source_record_id or None,
        source_url=paper.get("url") or paper.get("full_text_url"),
        content_hash=stable_json_hash(paper),
        raw_payload=paper,
    )


def _paper_research_object(paper: dict[str, Any], source_key: str, raw_id) -> ResearchObject:
    pmid = _clean_identifier(paper.get("pmid"))
    doi = _normalize_doi(paper.get("doi"))
    source_id = _clean_identifier(paper.get("source_id")) or pmid or doi
    identifiers = {
        "pmid": pmid,
        "doi": doi,
        "source_id": source_id,
    }
    identifiers = {key: value for key, value in identifiers.items() if value}
    year = _safe_int(paper.get("year"))
    return ResearchObject(
        object_type=ResearchObjectType.PUBLICATION,
        title=_clean_string(paper.get("title")),
        abstract=_clean_string(paper.get("abstract")),
        canonical_url=_clean_string(paper.get("url") or paper.get("full_text_url")),
        publication_year=year,
        published_at=_clean_string(paper.get("pub_date")),
        source_key=source_key,
        raw_record_id=raw_id,
        dedupe_key=_paper_dedupe_key(identifiers, paper),
        identifiers=identifiers,
        metadata={
            "legacy_source": paper.get("source"),
            "journal": paper.get("journal"),
            "authors": _parse_jsonish(paper.get("authors")),
            "keywords": _parse_jsonish(paper.get("keywords")),
            "raw_query": paper.get("raw_query"),
        },
    )


def _paper_chunk_text(paper: dict[str, Any]) -> str:
    title = _clean_string(paper.get("title")) or ""
    abstract = _clean_string(paper.get("abstract")) or ""
    return f"{title}\n\n{abstract}".strip()


def _paper_dedupe_key(identifiers: dict[str, str], paper: dict[str, Any]) -> str:
    if identifiers.get("doi"):
        return f"doi:{identifiers['doi'].lower()}"
    if identifiers.get("pmid"):
        return f"pmid:{identifiers['pmid'].lower()}"
    title = _clean_string(paper.get("title")) or stable_json_hash(paper)
    return f"legacy_paper:{title.lower()}"


def _markdown_title(content: str, fallback: str) -> str:
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("# ") and not line.startswith("## "):
            return line[2:].strip()
    return fallback


def _markdown_summary(content: str, limit: int = 1200) -> str:
    match = re.search(r"(?:^|\n)## *TL;DR\s*\n(.*?)(?=\n## )", content, re.DOTALL)
    block = match.group(1) if match else content
    block = re.sub(r"\s+", " ", block).strip()
    return block[:limit].rstrip() + ("..." if len(block) > limit else "")


def _track_from_slug(slug: str) -> str:
    for prefix, track in TRACK_PREFIXES.items():
        if slug.startswith(prefix):
            return track
    return "cross_cutting"


def _parse_jsonish(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _normalize_doi(value: Any) -> str | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    cleaned = re.sub(r"^https?://(dx\.)?doi\.org/", "", cleaned, flags=re.I)
    return cleaned.lower()


def _clean_identifier(value: Any) -> str | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    return cleaned


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
