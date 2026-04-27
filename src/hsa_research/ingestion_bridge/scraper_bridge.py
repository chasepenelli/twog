"""Controlled scraper bridge for non-API sources.

This module is intentionally narrower than a crawler. It fetches approved URLs
from source profiles, stores immutable artifacts, and parses only deterministic
metadata until a source-specific parser is implemented.
"""

from __future__ import annotations

import fnmatch
import json
import mimetypes
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from .chunker import chunk_text
from .contracts import (
    ArtifactHandle,
    RawSourceRecord,
    ResearchObject,
    ResearchObjectType,
    ScrapeFetchRequest,
    ScrapeFetchResult,
    ScrapeIngestRequest,
    ScrapeIngestResult,
    ScrapeManifestFetchRequest,
    ScrapeManifestItem,
    ScrapeManifestRequest,
    ScrapeManifestResult,
    ScrapeParsedRecord,
    ScrapeParseResult,
    ScrapeProfileReviewRequest,
    ScrapeReviewRecord,
    ScrapeReviewRequest,
    ScrapeReviewResult,
    ScrapeSourceProfileReview,
    ScrapeSourceProfile,
)
from .harvesters_v2 import USER_AGENT
from .local_store import SQLiteResearchRepository, stable_json_hash
from .scrape_parsers import discover_manifest_candidates, parse_scrape_html

DEFAULT_SCRAPE_ARTIFACT_ROOT = Path("var/hsa_research/artifacts/scrape")


SCRAPE_SOURCE_PROFILES: tuple[ScrapeSourceProfile, ...] = (
    ScrapeSourceProfile(
        source_key="avma_vctr",
        display_name="AVMA Veterinary Clinical Trials Registry",
        base_url="https://veterinaryclinicaltrials.org/",
        allowed_url_patterns=["https://veterinaryclinicaltrials.org/*"],
        robots_policy="unknown",
        rate_limit_per_minute=10,
        parser="avma_vctr",
        storage_policy="link_and_registry_metadata",
        approval_required=True,
        enabled=False,
        notes="API not found. Use controlled URL manifests and human review before ingestion.",
    ),
)


def list_scrape_profiles() -> list[ScrapeSourceProfile]:
    """Return configured scrape profiles."""

    return [profile.model_copy(deep=True) for profile in SCRAPE_SOURCE_PROFILES]


def get_scrape_profile(source_key: str) -> ScrapeSourceProfile | None:
    """Look up a scrape profile by source key."""

    for profile in SCRAPE_SOURCE_PROFILES:
        if profile.source_key == source_key:
            return profile.model_copy(deep=True)
    return None


@dataclass(frozen=True)
class FetchedPage:
    url: str
    status_code: int
    mime_type: str | None
    content: bytes


class ScrapeBridge:
    """Fetch and parse non-API web artifacts under source profile controls."""

    def __init__(
        self,
        repository: SQLiteResearchRepository,
        *,
        artifact_root: Path | str = DEFAULT_SCRAPE_ARTIFACT_ROOT,
    ) -> None:
        self.repository = repository
        self.artifact_root = Path(artifact_root)

    def fetch(self, request: ScrapeFetchRequest) -> ScrapeFetchResult:
        profile = get_scrape_profile(request.source_key)
        if profile is None:
            return ScrapeFetchResult(source_key=request.source_key, errors=[f"No scrape profile: {request.source_key}"])
        if profile.approval_required and not request.approved_by:
            return ScrapeFetchResult(
                source_key=request.source_key,
                errors=[f"Scrape source `{request.source_key}` requires explicit approval."],
            )
        gate_error = self._profile_gate_error(profile)
        if gate_error:
            return ScrapeFetchResult(source_key=request.source_key, errors=[gate_error])

        result = ScrapeFetchResult(source_key=request.source_key)
        urls = request.urls[: request.max_pages]
        delay_seconds = 60 / profile.rate_limit_per_minute
        for index, url in enumerate(urls):
            if not _url_allowed(profile, url):
                result.skipped_pages += 1
                result.errors.append(f"URL outside allowed patterns: {url}")
                continue
            if index:
                time.sleep(delay_seconds)
            try:
                page = _fetch_url(url)
                artifact = self._store_page(profile, page, request)
                self.repository.upsert_artifact(artifact)
                result.artifact_ids.append(artifact.artifact_id)
                result.fetched_pages += 1
            except Exception as exc:
                result.skipped_pages += 1
                result.errors.append(f"{url}: {exc}")
        return result

    def review_profile(self, request: ScrapeProfileReviewRequest) -> ScrapeSourceProfileReview:
        profile = get_scrape_profile(request.source_key)
        allowed_url_patterns = request.allowed_url_patterns or (profile.allowed_url_patterns if profile else [])
        storage_policy = request.storage_policy or (profile.storage_policy if profile else None)
        review = ScrapeSourceProfileReview(
            source_key=request.source_key,
            robots_policy=request.robots_policy,
            approved_for_fetch=request.approved_for_fetch,
            reviewed_by=request.reviewed_by,
            review_note=request.review_note,
            allowed_url_patterns=allowed_url_patterns,
            storage_policy=storage_policy,
            metadata={"profile_found": profile is not None},
        )
        return self.repository.upsert_scrape_profile_review(review)

    def get_profile_review(self, source_key: str) -> ScrapeSourceProfileReview | None:
        return self.repository.get_scrape_profile_review(source_key)

    def build_manifest(self, request: ScrapeManifestRequest) -> ScrapeManifestResult:
        profile = get_scrape_profile(request.source_key)
        if profile is None:
            return ScrapeManifestResult(source_key=request.source_key, errors=[f"No scrape profile: {request.source_key}"])

        result = ScrapeManifestResult(source_key=request.source_key)
        if request.fetch_seed_pages:
            seed_urls = request.seed_urls or [profile.base_url]
            fetch_result = self.fetch(
                ScrapeFetchRequest(
                    source_key=request.source_key,
                    urls=seed_urls,
                    max_pages=request.max_seed_pages,
                    approved_by=request.approved_by,
                    approval_note=request.approval_note,
                )
            )
            result.fetched_seed_pages = fetch_result.fetched_pages
            result.artifact_ids.extend(fetch_result.artifact_ids)
            result.skipped_urls += fetch_result.skipped_pages
            result.errors.extend(fetch_result.errors)
            if fetch_result.errors and not fetch_result.artifact_ids:
                return result

        artifacts = self.repository.list_artifacts(
            artifact_type="scrape_snapshot",
            source_key=request.source_key,
            limit=request.max_seed_pages,
        )
        result.seed_artifacts_seen = len(artifacts)
        candidates: list[ScrapeManifestItem] = []
        for artifact in artifacts:
            try:
                candidates.extend(self._discover_candidates(profile, artifact))
            except Exception as exc:
                result.errors.append(f"{artifact.artifact_id}: {exc}")
        deduped = _dedupe_manifest_items(candidates)[: request.max_candidate_urls]
        result.candidate_urls = deduped
        if deduped:
            manifest = self._store_manifest(profile, deduped, artifacts, request)
            self.repository.upsert_artifact(manifest)
            result.manifest_artifact_id = manifest.artifact_id
            result.artifact_ids.append(manifest.artifact_id)
        return result

    def fetch_manifest(self, request: ScrapeManifestFetchRequest) -> ScrapeFetchResult:
        artifact = self.repository.get_artifact(request.manifest_artifact_id)
        if artifact is None:
            return ScrapeFetchResult(source_key=request.source_key, errors=["No manifest artifact found."])
        if artifact.artifact_type != "scrape_manifest":
            return ScrapeFetchResult(source_key=request.source_key, errors=["Artifact is not a scrape manifest."])
        if artifact.metadata.get("source_key") != request.source_key:
            return ScrapeFetchResult(source_key=request.source_key, errors=["Manifest source does not match request."])
        manifest = json.loads(Path(artifact.uri).read_text(encoding="utf-8"))
        urls = [item["url"] for item in manifest.get("candidate_urls", []) if item.get("url")]
        return self.fetch(
            ScrapeFetchRequest(
                source_key=request.source_key,
                urls=urls[: request.max_pages],
                max_pages=request.max_pages,
                approved_by=request.approved_by,
                approval_note=request.approval_note,
            )
        )

    def parse(self, source_key: str, *, limit: int | None = None) -> ScrapeParseResult:
        profile = get_scrape_profile(source_key)
        if profile is None:
            return ScrapeParseResult(source_key=source_key, errors=[f"No scrape profile: {source_key}"])
        artifacts = self.repository.list_artifacts(
            artifact_type="scrape_snapshot",
            source_key=source_key,
            limit=limit,
        )
        result = ScrapeParseResult(source_key=source_key, artifacts_seen=len(artifacts))
        for artifact in artifacts:
            try:
                record = self._parse_artifact(profile, artifact)
            except Exception as exc:
                result.skipped_records += 1
                result.errors.append(f"{artifact.artifact_id}: {exc}")
                continue
            if record is None:
                result.skipped_records += 1
                continue
            review_record = self.repository.upsert_scrape_review(_to_review_record(record, profile))
            result.review_ids.append(review_record.review_id)
            result.records.append(_parsed_from_review_record(review_record))
            result.parsed_records += 1
        return result

    def list_reviews(
        self,
        source_key: str,
        *,
        review_status: str | None = None,
        limit: int | None = None,
    ) -> list[ScrapeReviewRecord]:
        return self.repository.list_scrape_reviews(
            source_key=source_key,
            review_status=review_status,
            limit=limit,
        )

    def review(self, request: ScrapeReviewRequest) -> ScrapeReviewResult:
        result = ScrapeReviewResult(source_key=request.source_key, decision=request.decision)
        for review_id in request.review_ids:
            record = self.repository.update_scrape_review(
                review_id,
                review_status=request.decision,
                reviewed_by=request.reviewed_by,
                review_note=request.review_note,
            )
            if record is None or record.source_key != request.source_key:
                result.skipped_records += 1
                result.errors.append(f"No review record for {request.source_key}: {review_id}")
                continue
            result.records.append(record)
            result.reviewed_records += 1
        return result

    def ingest(self, request: ScrapeIngestRequest) -> ScrapeIngestResult:
        """Promote parsed scrape artifacts into canonical records after approval."""

        profile = get_scrape_profile(request.source_key)
        if profile is None:
            return ScrapeIngestResult(source_key=request.source_key, errors=[f"No scrape profile: {request.source_key}"])
        if not request.approved_by:
            return ScrapeIngestResult(
                source_key=request.source_key,
                errors=[f"Scrape ingest for `{request.source_key}` requires explicit approval."],
            )

        review_records = self._select_review_records(request)
        result = ScrapeIngestResult(
            source_key=request.source_key,
            artifacts_seen=len({record.artifact_id for record in review_records}),
            review_records_seen=len(review_records),
        )
        if not review_records:
            result.errors.append(f"No accepted scrape review records found for `{request.source_key}`.")
            return result
        fetch_run_id = self.repository.create_fetch_run(request.source_key, "scrape_ingest")
        result.fetch_run_id = fetch_run_id
        try:
            for review_record in review_records:
                record = _parsed_from_review_record(review_record)
                result.parsed_records += 1
                if record.parser_confidence < request.min_parser_confidence:
                    result.skipped_records += 1
                    result.errors.append(
                        f"{review_record.review_id}: parser confidence {record.parser_confidence:.2f} below threshold"
                    )
                    continue
                raw, obj = _to_harvested_scrape_record(record, profile, request, review_record)
                raw_id = self.repository.upsert_raw_record(raw, fetch_run_id)
                object_id = self.repository.upsert_research_object(obj, raw_id)
                for doc_chunk in chunk_text(
                    object_id,
                    _text_for_parsed_record(record),
                    section_label="scrape_metadata",
                    metadata={
                        "source_key": request.source_key,
                        "harvester": "scrape_bridge",
                        "artifact_id": str(review_record.artifact_id),
                        "review_id": str(review_record.review_id),
                        "approved_by": request.approved_by,
                    },
                ):
                    self.repository.upsert_document_chunk(doc_chunk)
                    result.document_chunks += 1
                result.raw_records += 1
                result.research_objects += 1
                result.promoted_records += 1
            self.repository.finish_fetch_run(
                fetch_run_id,
                "completed",
                records_found=result.parsed_records,
                records_inserted=result.promoted_records,
            )
        except Exception as exc:
            self.repository.finish_fetch_run(fetch_run_id, "failed", error_message=str(exc))
            result.errors.append(str(exc))
        return result

    def _store_page(
        self,
        profile: ScrapeSourceProfile,
        page: FetchedPage,
        request: ScrapeFetchRequest,
    ) -> ArtifactHandle:
        digest = sha256(page.content).hexdigest()
        suffix = _extension_for_mime(page.mime_type, page.url)
        relative_path = Path(profile.source_key) / f"{digest}{suffix}"
        target = self.artifact_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(page.content)
        return ArtifactHandle(
            artifact_id=uuid4(),
            artifact_type="scrape_snapshot",
            uri=str(target),
            legal_status=profile.storage_policy,
            mime_type=page.mime_type,
            metadata={
                "source_key": profile.source_key,
                "source_url": page.url,
                "content_hash": digest,
                "http_status": page.status_code,
                "parser": profile.parser,
                "requires_review": True,
                "approved_by": request.approved_by,
                "approval_note": request.approval_note,
            },
        )

    def _parse_artifact(
        self,
        profile: ScrapeSourceProfile,
        artifact: ArtifactHandle,
    ) -> ScrapeParsedRecord | None:
        path = Path(artifact.uri)
        if not path.exists():
            raise FileNotFoundError(path)
        if artifact.mime_type and "html" not in artifact.mime_type:
            return None
        html = path.read_text(encoding="utf-8", errors="replace")
        return parse_scrape_html(profile, artifact, html)

    def _discover_candidates(
        self,
        profile: ScrapeSourceProfile,
        artifact: ArtifactHandle,
    ) -> list[ScrapeManifestItem]:
        path = Path(artifact.uri)
        if not path.exists():
            raise FileNotFoundError(path)
        if artifact.mime_type and "html" not in artifact.mime_type:
            return []
        html = path.read_text(encoding="utf-8", errors="replace")
        return discover_manifest_candidates(profile, artifact, html)

    def _select_review_records(self, request: ScrapeIngestRequest) -> list[ScrapeReviewRecord]:
        return self.repository.list_scrape_reviews(
            source_key=request.source_key,
            review_status="accepted",
            review_ids=request.review_ids or None,
            artifact_ids=request.artifact_ids or None,
            limit=request.limit,
        )

    def _store_manifest(
        self,
        profile: ScrapeSourceProfile,
        candidates: list[ScrapeManifestItem],
        seed_artifacts: list[ArtifactHandle],
        request: ScrapeManifestRequest,
    ) -> ArtifactHandle:
        payload = {
            "source_key": profile.source_key,
            "candidate_urls": [candidate.model_dump(mode="json") for candidate in candidates],
            "seed_artifact_ids": [str(artifact.artifact_id) for artifact in seed_artifacts],
            "request": request.model_dump(mode="json"),
        }
        digest = stable_json_hash(payload)
        target = self.artifact_root / profile.source_key / f"manifest-{digest}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return ArtifactHandle(
            artifact_id=uuid4(),
            artifact_type="scrape_manifest",
            uri=str(target),
            legal_status=profile.storage_policy,
            mime_type="application/json",
            metadata={
                "source_key": profile.source_key,
                "content_hash": digest,
                "candidate_count": len(candidates),
                "seed_artifact_ids": [str(artifact.artifact_id) for artifact in seed_artifacts],
                "requires_review": True,
                "approved_by": request.approved_by,
                "approval_note": request.approval_note,
            },
        )

    def _profile_gate_error(self, profile: ScrapeSourceProfile) -> str | None:
        if profile.enabled:
            return None
        review = self.repository.get_scrape_profile_review(profile.source_key)
        if review is None:
            return f"Scrape source `{profile.source_key}` requires source profile review before fetch."
        if not review.approved_for_fetch:
            return f"Scrape source `{profile.source_key}` has not been approved for fetch."
        if review.robots_policy != "reviewed":
            return f"Scrape source `{profile.source_key}` robots/TOS policy is `{review.robots_policy}`, not reviewed."
        return None


def _fetch_url(url: str) -> FetchedPage:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/pdf,*/*"})
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            content = response.read()
            return FetchedPage(
                url=response.geturl(),
                status_code=getattr(response, "status", 200),
                mime_type=(response.headers.get_content_type() if response.headers else None),
                content=content,
            )
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, socket.timeout) as exc:
        raise RuntimeError(f"Fetch failed: {exc}") from exc


def _url_allowed(profile: ScrapeSourceProfile, url: str) -> bool:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https", "file"}:
        return False
    return any(fnmatch.fnmatch(url, pattern) for pattern in profile.allowed_url_patterns)


def _extension_for_mime(mime_type: str | None, url: str) -> str:
    path_suffix = Path(urllib.parse.urlparse(url).path).suffix
    if path_suffix:
        return path_suffix
    if mime_type == "text/html":
        return ".html"
    return mimetypes.guess_extension(mime_type or "") or ".bin"


def _dedupe_manifest_items(items: list[ScrapeManifestItem]) -> list[ScrapeManifestItem]:
    deduped: dict[str, ScrapeManifestItem] = {}
    for item in items:
        existing = deduped.get(item.url)
        if existing is None or item.confidence > existing.confidence:
            deduped[item.url] = item
    return sorted(deduped.values(), key=lambda item: (-item.confidence, item.url))


def _to_harvested_scrape_record(
    record: ScrapeParsedRecord,
    profile: ScrapeSourceProfile,
    request: ScrapeIngestRequest,
    review_record: ScrapeReviewRecord,
) -> tuple[RawSourceRecord, ResearchObject]:
    payload = {
        "scrape_record": record.model_dump(mode="json"),
        "review_record": review_record.model_dump(mode="json"),
        "source_profile": profile.model_dump(mode="json"),
        "approval": {
            "approved_by": request.approved_by,
            "approval_note": request.approval_note,
        },
    }
    raw = RawSourceRecord(
        source_key=record.source_key,
        source_record_id=record.source_record_id,
        source_url=record.canonical_url,
        content_hash=stable_json_hash(payload),
        raw_payload=payload,
        metadata={
            "harvester": "scrape_bridge",
            "artifact_id": str(record.artifact_id) if record.artifact_id else None,
            "review_id": str(review_record.review_id),
            "parser_confidence": record.parser_confidence,
        },
    )
    obj = ResearchObject(
        object_type=record.record_type or ResearchObjectType.VETERINARY_TRIAL,
        title=record.title,
        canonical_url=record.canonical_url,
        source_key=record.source_key,
        dedupe_key=f"scrape:{record.source_key}:{record.source_record_id}",
        identifiers={"source_id": record.source_record_id},
        metadata={
            "scrape_fields": record.fields,
            "parser_confidence": record.parser_confidence,
            "review_status": "accepted",
            "approved_by": request.approved_by,
            "approval_note": request.approval_note,
            "artifact_id": str(record.artifact_id) if record.artifact_id else None,
            "review_id": str(review_record.review_id),
            "storage_policy": profile.storage_policy,
            "provenance": "scrape_bridge",
        },
    )
    return raw, obj


def _to_review_record(record: ScrapeParsedRecord, profile: ScrapeSourceProfile) -> ScrapeReviewRecord:
    if record.artifact_id is None:
        raise ValueError("Parsed scrape record is missing artifact_id")
    return ScrapeReviewRecord(
        source_key=record.source_key,
        artifact_id=record.artifact_id,
        source_record_id=record.source_record_id,
        title=record.title,
        canonical_url=record.canonical_url,
        record_type=record.record_type,
        fields=record.fields,
        parser_confidence=record.parser_confidence,
        review_status=record.review_status,
        metadata={
            "parser": profile.parser,
            "storage_policy": profile.storage_policy,
        },
    )


def _parsed_from_review_record(record: ScrapeReviewRecord) -> ScrapeParsedRecord:
    return ScrapeParsedRecord(
        source_key=record.source_key,
        source_record_id=record.source_record_id,
        title=record.title,
        canonical_url=record.canonical_url,
        record_type=record.record_type,
        fields=record.fields,
        parser_confidence=record.parser_confidence,
        review_status=record.review_status,
        artifact_id=record.artifact_id,
    )


def _text_for_parsed_record(record: ScrapeParsedRecord) -> str:
    parts = [record.title or "", record.canonical_url or ""]
    for key, value in record.fields.items():
        if key == "links":
            continue
        parts.append(f"{key}: {value}")
    links = record.fields.get("links")
    if isinstance(links, list):
        parts.extend(
            f"Link: {link.get('text', '')} {link.get('href', '')}"
            for link in links[:25]
            if isinstance(link, dict)
        )
    return "\n".join(part for part in parts if part)
