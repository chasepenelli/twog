"""Compliant X/Twitter topic monitoring primitives.

This module intentionally stops short of repository writes. It builds official
X API request shapes, runs bounded TwitterAPI.io searches when configured,
normalizes permitted API payloads into the ingestion bridge contracts, and
produces review candidates that must be accepted before durable storage.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime
from enum import Enum
from hashlib import sha256
from typing import Any, Callable
from urllib.parse import quote, urlencode, urlparse
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from .contracts import DocumentChunk, RawSourceRecord, ResearchObject, ResearchObjectType, SourceQuery

X_TOPIC_SOURCE_KEY = "x_topic_monitor"
X_API_BASE_URL = "https://api.x.com/2"
X_RECENT_SEARCH_ENDPOINT = "/tweets/search/recent"
X_FILTERED_STREAM_ENDPOINT = "/tweets/search/stream"
TWITTERAPI_IO_BASE_URL = "https://api.twitterapi.io"
TWITTERAPI_IO_ADVANCED_SEARCH_ENDPOINT = "/twitter/tweet/advanced_search"
X_MAX_RECENT_SEARCH_RESULTS = 100
X_MAX_RECENT_SEARCH_QUERY_LENGTH = 512
X_MAX_FILTERED_STREAM_RULE_LENGTH = 1024
TWITTERAPI_IO_MAX_SINGLE_PAGE_RESULTS = 20


DISEASE_TERMS = (
    '"canine hemangiosarcoma"',
    '"dog hemangiosarcoma"',
    '"canine haemangiosarcoma"',
    '"dog haemangiosarcoma"',
    "angiosarcoma",
    '"vascular sarcoma"',
    "hemangioendothelioma",
    "haemangioendothelioma",
)

TRIAL_TERMS = ("trial", "study", "recruiting", "enrollment", '"veterinary clinical trial"')

THERAPY_TARGET_TERMS = (
    "doxorubicin",
    "propranolol",
    "toceranib",
    "sirolimus",
    "paclitaxel",
    "VEGF",
    "VEGFR",
    "KIT",
    "PI3K",
    "AKT",
    "MTOR",
    "CD47",
)

SAFETY_TERMS = ("adverse", "toxicity", '"side effect"', "death", "bleeding", "cardiotoxicity")

SPAM_TERMS = (
    "crypto",
    "airdrop",
    "forex",
    "casino",
    "nft",
    "giveaway",
    "followers",
)

HIGH_QUALITY_DOMAINS = (
    "doi.org",
    "pubmed.ncbi.nlm.nih.gov",
    "pmc.ncbi.nlm.nih.gov",
    "clinicaltrials.gov",
    "veterinaryclinicaltrials.org",
    "fda.gov",
    "nih.gov",
    "nature.com",
    "science.org",
    "cell.com",
    "aacrjournals.org",
    "ascopubs.org",
    "frontiersin.org",
    "mdpi.com",
    "springer.com",
    "biomedcentral.com",
    "wiley.com",
    "tandfonline.com",
    "sciencedirect.com",
    "nejm.org",
    "jamanetwork.com",
    "biorxiv.org",
    "medrxiv.org",
    "peerj.com",
    "plos.org",
    "bmj.com",
    "edu",
)

_URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
_DOI_RE = re.compile(r"\b10\.\d{4,9}/[^\s\"'<>]+", re.IGNORECASE)
_PMID_RE = re.compile(r"\bPMID\s*:?\s*(\d{5,})\b", re.IGNORECASE)
_PMCID_RE = re.compile(r"\bPMC(?:ID)?\s*:?\s*(PMC\d+)\b", re.IGNORECASE)
_NCT_RE = re.compile(r"\bNCT\d{8}\b", re.IGNORECASE)


class XRetentionMode(str, Enum):
    """Allowed storage modes for post text and metadata."""

    STORE_TEXT = "store_text"
    STORE_METADATA_ONLY = "store_metadata_only"
    STORE_POST_ID_ONLY = "store_post_id_only"


class XReviewStatus(str, Enum):
    """Manual review states for social monitoring candidates."""

    NEEDS_REVIEW = "needs_review"
    ACCEPTED_SIGNAL = "accepted_signal"
    REJECTED_NOISE = "rejected_noise"
    NEEDS_FOLLOWUP_SOURCE = "needs_followup_source"
    COMPLIANCE_HOLD = "compliance_hold"
    EXPIRED_OR_DELETED = "expired_or_deleted"


class XTopicBaseModel(BaseModel):
    """Strict local model base for X topic monitoring."""

    model_config = ConfigDict(extra="forbid")


class XTopicRequest(XTopicBaseModel):
    """Dry-run request for official X API search or stream monitoring."""

    query: str
    query_name: str = "manual_x_topic_monitor"
    api_mode: str = "recent_search"
    language: str = "en"
    exclude_retweets: bool = True
    exclude_replies: bool = False
    max_results: int = Field(default=25, ge=10, le=X_MAX_RECENT_SEARCH_RESULTS)
    retention_mode: XRetentionMode = XRetentionMode.STORE_METADATA_ONLY
    manual_review_required: bool = True
    compliance_sync_required: bool = True


class XTopicApiRequest(XTopicBaseModel):
    """A request shape ready for the official X API."""

    method: str = "GET"
    url: str
    params: dict[str, Any]
    headers: dict[str, str]
    billable: bool
    notes: list[str] = Field(default_factory=list)


class XTopicReviewCandidate(XTopicBaseModel):
    """A normalized post candidate that still requires review."""

    review_id: str = Field(default_factory=lambda: str(uuid4()))
    source_key: str = X_TOPIC_SOURCE_KEY
    source_record_id: str
    canonical_url: str | None = None
    author_id: str | None = None
    username: str | None = None
    conversation_id: str | None = None
    created_at: str | None = None
    matched_query_name: str
    matched_terms: list[str] = Field(default_factory=list)
    durable_links: list[str] = Field(default_factory=list)
    quality_score: float = Field(default=0.0, ge=0.0, le=1.0)
    review_status: XReviewStatus = XReviewStatus.NEEDS_REVIEW
    retention_mode: XRetentionMode = XRetentionMode.STORE_METADATA_ONLY
    text_preview: str | None = None
    review_reasons: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class XTopicProviderResult(XTopicBaseModel):
    """Normalized provider search output for manual review."""

    provider: str
    query_name: str
    candidates: list[XTopicReviewCandidate] = Field(default_factory=list)
    raw_tweet_count: int = 0
    has_next_page: bool = False
    next_cursor: str | None = None
    errors: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class XTopicNormalizedRecord(XTopicBaseModel):
    """Research-object-compatible representation of an accepted signal."""

    raw_record: RawSourceRecord
    research_object: ResearchObject
    document_chunk: DocumentChunk


TwitterApiIoTransport = Callable[[str, dict[str, Any], dict[str, str], float], dict[str, Any]]


class TwitterApiIoProvider:
    """Read-only TwitterAPI.io provider for manual topic-monitoring review."""

    provider_name = "twitterapi_io"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        transport: TwitterApiIoTransport | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.api_key = api_key or os.getenv("TWITTERAPI_IO_KEY")
        self.transport = transport or _http_get_json
        self.timeout_seconds = timeout_seconds

    def search(self, request: XTopicRequest, *, cursor: str | None = None) -> XTopicProviderResult:
        """Run one bounded TwitterAPI.io Advanced Search page and normalize candidates."""

        if not self.api_key:
            raise ValueError("TWITTERAPI_IO_KEY is required for TwitterAPI.io searches")
        query = build_twitterapi_io_query(request)
        data = self.transport(
            f"{TWITTERAPI_IO_BASE_URL}{TWITTERAPI_IO_ADVANCED_SEARCH_ENDPOINT}",
            {
                "query": query,
                "queryType": "Latest",
                "cursor": cursor or "",
            },
            {"x-api-key": self.api_key},
            self.timeout_seconds,
        )
        tweets = data.get("tweets") if isinstance(data.get("tweets"), list) else []
        limited_tweets = tweets[: min(request.max_results, TWITTERAPI_IO_MAX_SINGLE_PAGE_RESULTS)]
        candidates = [
            normalize_post_payload(
                _twitterapi_io_tweet_to_post_payload(tweet),
                query_name=request.query_name,
                retention_mode=request.retention_mode,
            )
            for tweet in limited_tweets
            if isinstance(tweet, dict)
        ]
        return XTopicProviderResult(
            provider=self.provider_name,
            query_name=request.query_name,
            candidates=candidates,
            raw_tweet_count=len(tweets),
            has_next_page=bool(data.get("has_next_page")),
            next_cursor=_optional_str(data.get("next_cursor")),
            metadata={
                "endpoint": TWITTERAPI_IO_ADVANCED_SEARCH_ENDPOINT,
                "query": query,
                "query_type": "Latest",
                "max_single_page_results": TWITTERAPI_IO_MAX_SINGLE_PAGE_RESULTS,
                "billing": "TwitterAPI.io charges per returned tweet with a minimum request charge.",
            },
        )


def build_default_source_queries() -> list[SourceQuery]:
    """Return starter social-monitoring query groups without registry writes."""

    disease_query = _or_group(DISEASE_TERMS)
    therapy_query = f"{_or_group(DISEASE_TERMS)} AND {_or_group(THERAPY_TARGET_TERMS)}"
    trial_query = f"{_or_group(DISEASE_TERMS)} AND {_or_group(TRIAL_TERMS)}"
    safety_query = f"{_or_group(THERAPY_TARGET_TERMS)} AND (dog OR canine) AND {_or_group(SAFETY_TERMS)}"
    common_params = {
        "api_mode": "recent_search",
        "language": "en",
        "exclude_retweets": True,
        "retention_mode": XRetentionMode.STORE_METADATA_ONLY.value,
        "manual_review_required": True,
        "compliance_sync_required": True,
    }
    return [
        SourceQuery(
            source_key=X_TOPIC_SOURCE_KEY,
            query_name="x_disease_monitoring",
            query_text=disease_query,
            query_params=common_params,
            track="disease_monitoring",
            object_type=ResearchObjectType.KNOWLEDGE_ENTRY,
        ),
        SourceQuery(
            source_key=X_TOPIC_SOURCE_KEY,
            query_name="x_trial_monitoring",
            query_text=trial_query,
            query_params=common_params,
            track="trial_monitoring",
            object_type=ResearchObjectType.KNOWLEDGE_ENTRY,
        ),
        SourceQuery(
            source_key=X_TOPIC_SOURCE_KEY,
            query_name="x_therapy_target_monitoring",
            query_text=therapy_query,
            query_params=common_params,
            track="therapy_target_monitoring",
            object_type=ResearchObjectType.KNOWLEDGE_ENTRY,
        ),
        SourceQuery(
            source_key=X_TOPIC_SOURCE_KEY,
            query_name="x_safety_monitoring",
            query_text=safety_query,
            query_params=common_params,
            track="safety_monitoring",
            object_type=ResearchObjectType.KNOWLEDGE_ENTRY,
        ),
    ]


def build_recent_search_request(request: XTopicRequest, *, bearer_token: str | None = None) -> XTopicApiRequest:
    """Build an official X Recent Search request without sending it."""

    query = _apply_query_guards(request)
    if len(query) > X_MAX_RECENT_SEARCH_QUERY_LENGTH:
        raise ValueError(
            f"Recent Search query is {len(query)} chars; max is {X_MAX_RECENT_SEARCH_QUERY_LENGTH}. "
            "Split it into narrower SourceQuery rows."
        )
    params = {
        "query": query,
        "max_results": request.max_results,
        "tweet.fields": ",".join(
            (
                "id",
                "text",
                "author_id",
                "conversation_id",
                "created_at",
                "lang",
                "public_metrics",
                "referenced_tweets",
                "entities",
                "edit_history_tweet_ids",
            )
        ),
        "user.fields": "id,username,name,verified,verified_type",
        "expansions": "author_id,referenced_tweets.id",
    }
    headers = {"Authorization": "Bearer <X_BEARER_TOKEN>"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    return XTopicApiRequest(
        url=f"{X_API_BASE_URL}{X_RECENT_SEARCH_ENDPOINT}?{urlencode(params)}",
        params=params,
        headers=headers,
        billable=True,
        notes=[
            "Official X API request only; no scraping.",
            "Successful returned posts may consume prepaid X API credits.",
            f"retention_mode={_retention_value(request.retention_mode)}",
        ],
    )


def build_twitterapi_io_query(request: XTopicRequest) -> str:
    """Build a TwitterAPI.io advanced-search query from the shared request."""

    query = request.query.strip()
    guards: list[str] = []
    if request.language:
        guards.append(f"lang:{request.language}")
    if request.exclude_retweets:
        guards.append("-filter:retweets")
    if request.exclude_replies:
        guards.append("-filter:replies")
    return " ".join([query, *guards]).strip()


def build_twitterapi_io_search_request(request: XTopicRequest) -> XTopicApiRequest:
    """Build a redacted TwitterAPI.io Advanced Search request without sending it."""

    query = build_twitterapi_io_query(request)
    params = {
        "query": query,
        "queryType": "Latest",
        "cursor": "",
    }
    return XTopicApiRequest(
        url=f"{TWITTERAPI_IO_BASE_URL}{TWITTERAPI_IO_ADVANCED_SEARCH_ENDPOINT}?{urlencode(params)}",
        params=params,
        headers={"x-api-key": "<TWITTERAPI_IO_KEY>"},
        billable=True,
        notes=[
            "TwitterAPI.io read-only advanced search request.",
            "Provider charges per returned tweet with a minimum request charge.",
            f"retention_mode={_retention_value(request.retention_mode)}",
        ],
    )


def build_filtered_stream_rule(query: SourceQuery) -> dict[str, str]:
    """Build a Filtered Stream rule payload entry from a source query."""

    request = XTopicRequest(
        query=query.query_text,
        query_name=query.query_name,
        api_mode="filtered_stream",
        max_results=10,
        retention_mode=query.query_params.get("retention_mode", XRetentionMode.STORE_METADATA_ONLY.value),
    )
    rule_value = _apply_query_guards(request)
    if len(rule_value) > X_MAX_FILTERED_STREAM_RULE_LENGTH:
        raise ValueError(
            f"Filtered Stream rule is {len(rule_value)} chars; max is {X_MAX_FILTERED_STREAM_RULE_LENGTH}. "
            "Split it into narrower stream rules."
        )
    return {"value": rule_value, "tag": query.query_name}


def normalize_post_payload(
    payload: dict[str, Any],
    *,
    query_name: str,
    retention_mode: XRetentionMode = XRetentionMode.STORE_METADATA_ONLY,
) -> XTopicReviewCandidate:
    """Normalize one X API post payload into a review candidate."""

    post_id = str(payload.get("id") or "")
    if not post_id:
        raise ValueError("X post payload is missing `id`")
    author_id = _optional_str(payload.get("author_id"))
    username = _extract_username(payload)
    text = _optional_str(payload.get("text"))
    canonical_url = _optional_str(payload.get("url")) or (
        f"https://x.com/{username}/status/{post_id}" if username else f"https://x.com/i/web/status/{post_id}"
    )
    matched_terms = _matched_terms(text or "")
    durable_links = _durable_links(payload)
    quality_score, reasons = score_candidate(payload, matched_terms=matched_terms, durable_links=durable_links)
    review_status = XReviewStatus.NEEDS_REVIEW
    if quality_score < 0.2:
        review_status = XReviewStatus.REJECTED_NOISE
    retention = _retention_value(retention_mode)
    if retention == XRetentionMode.STORE_TEXT.value:
        text_preview = _truncate(text, 500)
    elif retention == XRetentionMode.STORE_METADATA_ONLY.value:
        text_preview = _truncate(text, 160) if text else None
    else:
        text_preview = None
    return XTopicReviewCandidate(
        source_record_id=post_id,
        canonical_url=canonical_url,
        author_id=author_id,
        username=username,
        conversation_id=_optional_str(payload.get("conversation_id")),
        created_at=_optional_str(payload.get("created_at")),
        matched_query_name=query_name,
        matched_terms=matched_terms,
        durable_links=durable_links,
        quality_score=quality_score,
        review_status=review_status,
        retention_mode=retention_mode,
        text_preview=text_preview,
        review_reasons=reasons,
        metadata={
            "lang": payload.get("lang"),
            "edit_history_tweet_ids": payload.get("edit_history_tweet_ids") or [],
            "referenced_tweets": payload.get("referenced_tweets") or [],
            "public_metrics": payload.get("public_metrics") or {},
            "provider_payload": payload.get("provider_payload") or {},
            "compliance_status": "unknown",
        },
    )


def score_candidate(
    payload: dict[str, Any],
    *,
    matched_terms: list[str] | None = None,
    durable_links: list[str] | None = None,
) -> tuple[float, list[str]]:
    """Return a conservative topic quality score and reviewer-facing reasons."""

    text = (_optional_str(payload.get("text")) or "").lower()
    matched_terms = matched_terms if matched_terms is not None else _matched_terms(text)
    durable_links = durable_links if durable_links is not None else _durable_links(payload)
    score = 0.0
    reasons: list[str] = []
    if payload.get("lang") not in (None, "en"):
        reasons.append("non-English post")
        return 0.0, reasons
    if matched_terms:
        score += min(0.45, 0.12 * len(matched_terms))
        reasons.append(f"matched topic terms: {', '.join(matched_terms[:5])}")
    if durable_links:
        score += 0.25
        reasons.append("contains durable source link")
    if any(term in text for term in ("trial", "recruiting", "enrollment", "study")):
        score += 0.12
        reasons.append("trial/study signal")
    if any(term in text for term in ("adverse", "toxicity", "side effect", "death")):
        score += 0.08
        reasons.append("safety/anecdote signal requiring review")
    if len(text) < 40 and not durable_links:
        score -= 0.15
        reasons.append("low-context short post")
    if any(term in text for term in SPAM_TERMS):
        score -= 0.35
        reasons.append("spam-like term present")
    if "referenced_tweets" in payload:
        referenced = payload.get("referenced_tweets") or []
        if any(item.get("type") == "retweeted" for item in referenced if isinstance(item, dict)):
            score -= 0.2
            reasons.append("retweet-style referenced post")
    return max(0.0, min(1.0, round(score, 4))), reasons or ["no strong topic signal"]


def to_research_record(
    candidate: XTopicReviewCandidate,
    payload: dict[str, Any],
    *,
    accepted_by: str,
) -> XTopicNormalizedRecord:
    """Convert an accepted review candidate into bridge storage contracts."""

    if _review_status_value(candidate.review_status) not in {
        XReviewStatus.ACCEPTED_SIGNAL.value,
        XReviewStatus.NEEDS_FOLLOWUP_SOURCE.value,
    }:
        raise ValueError("Only accepted X topic candidates can be normalized for storage")
    retained_payload = _retained_payload(payload, candidate.retention_mode)
    content_hash = _stable_hash(
        {
            "source_key": X_TOPIC_SOURCE_KEY,
            "source_record_id": candidate.source_record_id,
            "retained_payload": retained_payload,
            "retention_mode": _retention_value(candidate.retention_mode),
        }
    )
    raw_record = RawSourceRecord(
        source_key=X_TOPIC_SOURCE_KEY,
        source_record_id=candidate.source_record_id,
        source_url=candidate.canonical_url,
        content_hash=content_hash,
        raw_payload=retained_payload,
        metadata={
            "retention_mode": _retention_value(candidate.retention_mode),
            "matched_query_name": candidate.matched_query_name,
            "manual_review_required": True,
            "review_status": _review_status_value(candidate.review_status),
            "accepted_by": accepted_by,
        },
    )
    research_object = ResearchObject(
        object_type=ResearchObjectType.KNOWLEDGE_ENTRY,
        title=_candidate_title(candidate),
        canonical_url=candidate.canonical_url,
        published_at=candidate.created_at,
        source_key=X_TOPIC_SOURCE_KEY,
        raw_record_id=raw_record.id,
        dedupe_key=f"{X_TOPIC_SOURCE_KEY}:post:{candidate.source_record_id}",
        identifiers={
            "x_post_id": candidate.source_record_id,
            **({"x_author_id": candidate.author_id} if candidate.author_id else {}),
        },
        metadata={
            "matched_query_name": candidate.matched_query_name,
            "matched_terms": candidate.matched_terms,
            "durable_links": candidate.durable_links,
            "quality_score": candidate.quality_score,
            "retention_mode": _retention_value(candidate.retention_mode),
            "review_status": _review_status_value(candidate.review_status),
            "compliance_status": candidate.metadata.get("compliance_status", "unknown"),
        },
    )
    chunk_text = _chunk_text_for_candidate(candidate, payload)
    document_chunk = DocumentChunk(
        research_object_id=research_object.id,
        chunk_index=0,
        section_label="x_topic_signal",
        text_content=chunk_text,
        content_hash=_stable_hash({"post_id": candidate.source_record_id, "chunk_text": chunk_text}),
        metadata={
            "source_key": X_TOPIC_SOURCE_KEY,
            "retention_mode": _retention_value(candidate.retention_mode),
            "manual_review_required": True,
            "accepted_by": accepted_by,
        },
    )
    return XTopicNormalizedRecord(raw_record=raw_record, research_object=research_object, document_chunk=document_chunk)


def dry_run_plan() -> dict[str, Any]:
    """Return starter queries and API request shapes for operator review."""

    queries = build_default_source_queries()
    official_requests = [
        build_recent_search_request(
            XTopicRequest(
                query=query.query_text,
                query_name=query.query_name,
                max_results=10,
                retention_mode=query.query_params.get("retention_mode", XRetentionMode.STORE_METADATA_ONLY.value),
            )
        ).model_dump(mode="json")
        for query in queries
    ]
    twitterapi_io_requests = [
        build_twitterapi_io_search_request(
            XTopicRequest(
                query=query.query_text,
                query_name=query.query_name,
                max_results=10,
                retention_mode=query.query_params.get("retention_mode", XRetentionMode.STORE_METADATA_ONLY.value),
            )
        ).model_dump(mode="json")
        for query in queries
    ]
    stream_rules = [build_filtered_stream_rule(query) for query in queries]
    return {
        "source_key": X_TOPIC_SOURCE_KEY,
        "enabled_by_default": False,
        "requires_env": ["X_BEARER_TOKEN or TWITTERAPI_IO_KEY"],
        "billing_note": "Official X API and TwitterAPI.io both bill successful reads; keep early runs small.",
        "source_queries": [query.model_dump(mode="json") for query in queries],
        "recent_search_requests": official_requests,
        "twitterapi_io_search_requests": twitterapi_io_requests,
        "filtered_stream_rules": stream_rules,
    }


def main() -> None:
    """Small module-local dry-run entrypoint to avoid CLI wiring conflicts."""

    parser = argparse.ArgumentParser(description="Dry-run X topic monitor request planning")
    parser.add_argument("--json", action="store_true", help="Emit JSON")
    args = parser.parse_args()
    plan = dry_run_plan()
    if args.json:
        print(json.dumps(plan, indent=2, sort_keys=True))
        return
    print(f"source_key: {plan['source_key']}")
    print("requires: X_BEARER_TOKEN or TWITTERAPI_IO_KEY")
    for query in plan["source_queries"]:
        print(f"- {query['query_name']}: {query['query_text']}")


def _apply_query_guards(request: XTopicRequest) -> str:
    query = request.query.strip()
    guards: list[str] = []
    if request.language:
        guards.append(f"lang:{request.language}")
    if request.exclude_retweets:
        guards.append("-is:retweet")
    if request.exclude_replies:
        guards.append("-is:reply")
    return " ".join([query, *guards]).strip()


def _retained_payload(payload: dict[str, Any], retention_mode: XRetentionMode | str) -> dict[str, Any]:
    common = {
        "id": payload.get("id"),
        "author_id": payload.get("author_id"),
        "conversation_id": payload.get("conversation_id"),
        "created_at": payload.get("created_at"),
        "lang": payload.get("lang"),
        "edit_history_tweet_ids": payload.get("edit_history_tweet_ids") or [],
        "referenced_tweets": payload.get("referenced_tweets") or [],
    }
    retention = _retention_value(retention_mode)
    if retention == XRetentionMode.STORE_POST_ID_ONLY.value:
        return {"id": payload.get("id")}
    if retention == XRetentionMode.STORE_METADATA_ONLY.value:
        return common | {"entities": payload.get("entities") or {}, "public_metrics": payload.get("public_metrics") or {}}
    return payload


def _chunk_text_for_candidate(candidate: XTopicReviewCandidate, payload: dict[str, Any]) -> str:
    if _retention_value(candidate.retention_mode) == XRetentionMode.STORE_TEXT.value:
        return _optional_str(payload.get("text")) or ""
    terms = ", ".join(candidate.matched_terms) if candidate.matched_terms else "no exact topic terms recorded"
    return (
        f"X topic monitoring signal for {candidate.matched_query_name}; "
        f"post_id={candidate.source_record_id}; matched_terms={terms}; "
        f"text retention mode={_retention_value(candidate.retention_mode)}."
    )


def _candidate_title(candidate: XTopicReviewCandidate) -> str:
    created = candidate.created_at or datetime.utcnow().date().isoformat()
    author = f"@{candidate.username}" if candidate.username else candidate.author_id or "unknown author"
    return f"X post by {author} on {created[:10]}"


def _matched_terms(text: str) -> list[str]:
    lower = text.lower()
    terms = [term.strip('"') for term in (*DISEASE_TERMS, *TRIAL_TERMS, *THERAPY_TARGET_TERMS, *SAFETY_TERMS)]
    return sorted({term for term in terms if term.lower() in lower})


def _durable_links(payload: dict[str, Any]) -> list[str]:
    links: list[str] = []
    candidate_urls: list[str] = []
    entities = payload.get("entities") or {}
    for url_item in entities.get("urls") or []:
        if not isinstance(url_item, dict):
            continue
        candidate_urls.extend(
            str(value)
            for value in (
                url_item.get("expanded_url"),
                url_item.get("expandedUrl"),
                url_item.get("unwound_url"),
                url_item.get("unwoundUrl"),
                url_item.get("url"),
            )
            if value
        )

    text = _optional_str(payload.get("text")) or ""
    candidate_urls.extend(match.group(0) for match in _URL_RE.finditer(text))
    for candidate_url in candidate_urls:
        cleaned = _clean_url(candidate_url)
        if cleaned and _is_high_quality_link(cleaned):
            links.append(cleaned)

    links.extend(_identifier_links_from_text(text))
    return sorted(set(links))


def _extract_username(payload: dict[str, Any]) -> str | None:
    username = payload.get("username")
    if isinstance(username, str) and username:
        return username
    author = payload.get("author")
    if isinstance(author, dict):
        return _optional_str(author.get("username"))
    includes = payload.get("includes")
    author_id = _optional_str(payload.get("author_id"))
    if isinstance(includes, dict) and author_id:
        users = includes.get("users") or []
        for user in users:
            if isinstance(user, dict) and str(user.get("id")) == author_id:
                return _optional_str(user.get("username"))
    return None


def _twitterapi_io_tweet_to_post_payload(tweet: dict[str, Any]) -> dict[str, Any]:
    author = tweet.get("author") if isinstance(tweet.get("author"), dict) else {}
    entities = tweet.get("entities") if isinstance(tweet.get("entities"), dict) else {}
    urls = []
    for url_item in entities.get("urls") or []:
        if not isinstance(url_item, dict):
            continue
        urls.append(
            {
                "url": url_item.get("url"),
                "expanded_url": url_item.get("expanded_url") or url_item.get("expandedUrl"),
                "unwound_url": url_item.get("unwound_url") or url_item.get("unwoundUrl"),
                "display_url": url_item.get("display_url") or url_item.get("displayUrl"),
            }
        )
    for url_item in tweet.get("urls") or []:
        if not isinstance(url_item, dict):
            continue
        urls.append(
            {
                "url": url_item.get("url"),
                "expanded_url": url_item.get("expanded_url") or url_item.get("expandedUrl"),
                "unwound_url": url_item.get("unwound_url") or url_item.get("unwoundUrl"),
                "display_url": url_item.get("display_url") or url_item.get("displayUrl"),
            }
        )
    return {
        "id": tweet.get("id"),
        "url": tweet.get("url"),
        "text": tweet.get("text"),
        "author_id": author.get("id"),
        "author": {
            "username": author.get("userName"),
            "name": author.get("name"),
            "verified": author.get("isBlueVerified"),
            "verified_type": author.get("verifiedType"),
        },
        "conversation_id": tweet.get("conversationId"),
        "created_at": tweet.get("createdAt"),
        "lang": tweet.get("lang"),
        "public_metrics": {
            "retweet_count": tweet.get("retweetCount"),
            "reply_count": tweet.get("replyCount"),
            "like_count": tweet.get("likeCount"),
            "quote_count": tweet.get("quoteCount"),
            "view_count": tweet.get("viewCount"),
            "bookmark_count": tweet.get("bookmarkCount"),
        },
        "referenced_tweets": _twitterapi_io_referenced_tweets(tweet),
        "entities": {**entities, "urls": urls},
        "provider_payload": {
            "provider": "twitterapi_io",
            "source": tweet.get("source"),
            "is_reply": tweet.get("isReply"),
            "in_reply_to_id": tweet.get("inReplyToId"),
            "in_reply_to_username": tweet.get("inReplyToUsername"),
        },
    }


def _twitterapi_io_referenced_tweets(tweet: dict[str, Any]) -> list[dict[str, str]]:
    references: list[dict[str, str]] = []
    if tweet.get("retweeted_tweet"):
        references.append({"type": "retweeted", "id": str(tweet["retweeted_tweet"])})
    if tweet.get("quoted_tweet"):
        references.append({"type": "quoted", "id": str(tweet["quoted_tweet"])})
    if tweet.get("inReplyToId"):
        references.append({"type": "replied_to", "id": str(tweet["inReplyToId"])})
    return references


def _http_get_json(
    url: str,
    params: dict[str, Any],
    headers: dict[str, str],
    timeout_seconds: float,
) -> dict[str, Any]:
    query = urlencode({key: value for key, value in params.items() if value is not None})
    request = urllib.request.Request(f"{url}?{query}", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc


def _identifier_links_from_text(text: str) -> list[str]:
    links: list[str] = []
    for match in _DOI_RE.finditer(text):
        doi = _clean_identifier(match.group(0))
        if doi:
            links.append(f"https://doi.org/{quote(doi, safe='/')}")
    for match in _PMID_RE.finditer(text):
        links.append(f"https://pubmed.ncbi.nlm.nih.gov/{match.group(1)}/")
    for match in _PMCID_RE.finditer(text):
        links.append(f"https://pmc.ncbi.nlm.nih.gov/articles/{match.group(1).upper()}/")
    for match in _NCT_RE.finditer(text):
        links.append(f"https://clinicaltrials.gov/study/{match.group(0).upper()}")
    return links


def _clean_url(url: str) -> str | None:
    cleaned = url.strip().rstrip(").,;]'\"")
    parsed = urlparse(cleaned)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return cleaned


def _clean_identifier(value: str) -> str:
    identifier = value.strip().split("?", 1)[0].split("#", 1)[0].rstrip(").,;:]'\"")
    for suffix in ("/full", "/abstract", "/pdf", "/epdf", "/html"):
        if identifier.lower().endswith(suffix):
            identifier = identifier[: -len(suffix)]
            break
    return identifier


def _is_high_quality_link(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if not host:
        return False
    for domain in HIGH_QUALITY_DOMAINS:
        marker = domain.lower()
        if marker == "edu":
            if host.endswith(".edu"):
                return True
            continue
        if host == marker or host.endswith(f".{marker}"):
            return True
    return False


def _stable_hash(payload: dict[str, Any]) -> str:
    return sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()


def _or_group(terms: tuple[str, ...]) -> str:
    return "(" + " OR ".join(terms) + ")"


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _truncate(text: str | None, limit: int) -> str | None:
    if text is None or len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _retention_value(value: XRetentionMode | str) -> str:
    if isinstance(value, XRetentionMode):
        return value.value
    return str(value)


def _review_status_value(value: XReviewStatus | str) -> str:
    if isinstance(value, XReviewStatus):
        return value.value
    return str(value)


if __name__ == "__main__":
    main()
