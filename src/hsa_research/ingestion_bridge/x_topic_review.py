"""Recommend-only agent for X/Twitter monitoring candidates.

The agent reviews normalized social-monitoring candidates and flags durable
linked sources for primary-source ingestion. It does not persist tweets, scrape
X pages, or promote social posts as scientific evidence.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
import os
import re
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

from .contracts import (
    XTopicLinkedSource,
    XTopicReviewAction,
    XTopicReviewRequest,
    XTopicReviewResult,
)


X_TOPIC_REVIEW_AGENT_NAME = "x_topic_review_agent"
X_TOPIC_REVIEW_AGENT_VERSION = "v1"
DEFAULT_X_TOPIC_REVIEW_MODEL = "~anthropic/claude-sonnet-latest"
DEFAULT_X_TOPIC_COMPARE_MODELS = (DEFAULT_X_TOPIC_REVIEW_MODEL,)
_ALLOWED_ACTIONS = {
    "flag_for_ingestion",
    "queue_source_followup",
    "needs_link_review",
    "needs_human_review",
    "reject_noise",
    "compliance_hold",
    "skip_no_durable_source",
}
_ALLOWED_SEVERITIES = {"info", "watch", "blocking"}
_ALLOWED_IDENTIFIER_TYPES = {
    "doi",
    "pmid",
    "pmcid",
    "nct",
    "pubchem",
    "chembl",
    "uniprot",
    "rcsb_pdb",
    "geo",
    "sra",
    "unknown",
}

_SOURCE_PATTERNS = (
    ("pubmed", "pmid", "pubmed.ncbi.nlm.nih.gov", re.compile(r"/(\d{5,})/?(?:$|[?#])")),
    ("pmc_oa", "pmcid", "pmc.ncbi.nlm.nih.gov", re.compile(r"/articles/(PMC\d+)/?", re.IGNORECASE)),
    ("clinicaltrials_gov", "nct", "clinicaltrials.gov", re.compile(r"(NCT\d{8})", re.IGNORECASE)),
    ("pubchem", "pubchem", "pubchem.ncbi.nlm.nih.gov", re.compile(r"/compound/([^/?#]+)", re.IGNORECASE)),
    ("chembl", "chembl", "ebi.ac.uk/chembl", re.compile(r"(CHEMBL\d+)", re.IGNORECASE)),
    ("uniprot", "uniprot", "uniprot.org", re.compile(r"/uniprotkb/([^/?#]+)", re.IGNORECASE)),
    ("rcsb_pdb", "rcsb_pdb", "rcsb.org", re.compile(r"/structure/([A-Za-z0-9]{4})", re.IGNORECASE)),
    ("geo", "geo", "ncbi.nlm.nih.gov/geo", re.compile(r"(GSE\d+|GSM\d+)", re.IGNORECASE)),
    ("sra", "sra", "ncbi.nlm.nih.gov/sra", re.compile(r"(SRP\d+|SRR\d+|SRS\d+)", re.IGNORECASE)),
)

_DOI_RE = re.compile(r"10\.\d{4,9}/[^\s\"'<>]+", re.IGNORECASE)
_RESOLVABLE_SHORT_LINK_HOSTS = (
    "go.ufl.edu",
    "bit.ly",
    "tinyurl.com",
    "t.co",
    "x.com",
)


class XTopicReviewAgent:
    """Review social-monitoring candidates and recommend ingestion follow-up."""

    agent_name = X_TOPIC_REVIEW_AGENT_NAME
    agent_version = X_TOPIC_REVIEW_AGENT_VERSION

    def run(self, request: XTopicReviewRequest) -> XTopicReviewResult:
        candidates = _candidate_payloads(request)[: request.max_candidates]
        deterministic_result = _deterministic_review(request, candidates)
        if request.review_mode == "deterministic_only":
            return deterministic_result

        review_payload = _build_review_payload(request, candidates, deterministic_result)
        if request.review_mode in {"openrouter_required", "openrouter_compare"}:
            return _run_openrouter_reviews(request, deterministic_result, review_payload)

        return _external_review_required_result(request, deterministic_result, review_payload)


def _candidate_payloads(request: XTopicReviewRequest) -> list[dict[str, Any]]:
    if request.candidates:
        return list(request.candidates)
    provider_report = request.provider_report or {}
    candidates = provider_report.get("candidates", [])
    return [candidate for candidate in candidates if isinstance(candidate, dict)]


def _deterministic_review(
    request: XTopicReviewRequest,
    candidates: Sequence[Mapping[str, Any]],
) -> XTopicReviewResult:
    actions = [_review_candidate(candidate) for candidate in candidates]
    return _finalize_result(
        XTopicReviewResult(
            model_profile=request.model_profile,
            actions=actions,
            evidence={
                "review_mode": request.review_mode,
                "deterministic_review": True,
                "candidate_count": len(candidates),
                "provider": (request.provider_report or {}).get("provider"),
            },
        )
    )


def _review_candidate(candidate: Mapping[str, Any]) -> XTopicReviewAction:
    post_id = str(candidate.get("post_id") or candidate.get("source_record_id") or "")
    query_name = _optional_str(candidate.get("query_name") or candidate.get("matched_query_name"))
    username = _optional_str(candidate.get("username"))
    quality_score = _float_or_zero(candidate.get("quality_score"))
    links = [
        _classify_link(str(link))
        for link in candidate.get("durable_links", [])
        if isinstance(link, str) and link.strip()
    ]
    ingestible_links = [link for link in links if link.should_ingest]
    evidence_refs = [f"candidate:{post_id}"] if post_id else []

    if quality_score < 0.2:
        return XTopicReviewAction(
            source_record_id=post_id,
            query_name=query_name,
            username=username,
            action="reject_noise",
            severity="info",
            reason="Candidate scored below the social-monitoring quality bar.",
            ingestible_links=links,
            evidence_refs=evidence_refs,
        )

    if ingestible_links:
        return XTopicReviewAction(
            source_record_id=post_id,
            query_name=query_name,
            username=username,
            action="flag_for_ingestion",
            severity="watch",
            reason="Candidate links to one or more durable primary-source records that should be harvested.",
            ingestible_links=ingestible_links,
            evidence_refs=evidence_refs + [link.url for link in ingestible_links],
        )

    if links:
        return XTopicReviewAction(
            source_record_id=post_id,
            query_name=query_name,
            username=username,
            action="needs_link_review",
            severity="watch",
            reason="Candidate has durable-looking links, but none map cleanly to an implemented harvester.",
            ingestible_links=links,
            evidence_refs=evidence_refs + [link.url for link in links],
        )

    return XTopicReviewAction(
        source_record_id=post_id,
        query_name=query_name,
        username=username,
        action="skip_no_durable_source",
        severity="info",
        reason="Candidate has no durable source link; do not ingest the social post as evidence.",
        ingestible_links=[],
        evidence_refs=evidence_refs,
    )


def _classify_link(url: str) -> XTopicLinkedSource:
    original_url = url.strip()
    normalized, resolution_metadata = _resolve_review_link(original_url)
    parsed = urllib.parse.urlparse(normalized)
    host_path = f"{parsed.netloc}{parsed.path}".lower()

    doi = _extract_doi(normalized)
    if doi:
        return XTopicLinkedSource(
            url=normalized,
            recommended_source_key="crossref",
            identifier_type="doi",
            identifier=doi,
            should_ingest=True,
            reason="DOI link can be followed through Crossref/OpenAlex and, when open, full-text sources.",
            metadata=resolution_metadata,
        )

    for source_key, identifier_type, marker, pattern in _SOURCE_PATTERNS:
        if marker not in host_path:
            continue
        match = pattern.search(normalized)
        identifier = match.group(1) if match else None
        return XTopicLinkedSource(
            url=normalized,
            recommended_source_key=source_key,
            identifier_type=identifier_type,  # type: ignore[arg-type]
            identifier=identifier.upper() if identifier_type in {"pmcid", "nct", "chembl", "geo", "sra"} and identifier else identifier,
            should_ingest=True,
            reason=f"URL maps to implemented source `{source_key}`.",
            metadata=resolution_metadata,
        )

    if any(marker in host_path for marker in ("nih.gov", "ncbi.nlm.nih.gov", "fda.gov", ".edu")):
        return XTopicLinkedSource(
            url=normalized,
            recommended_source_key=None,
            identifier_type="unknown",
            identifier=None,
            should_ingest=False,
            reason="Credible domain but no implemented source identifier was detected; human link review needed.",
            metadata=resolution_metadata,
        )

    return XTopicLinkedSource(
        url=normalized,
        recommended_source_key=None,
        identifier_type="unknown",
        identifier=None,
        should_ingest=False,
        reason="Link does not map to an implemented ingestion source.",
        metadata=resolution_metadata,
    )


def _resolve_review_link(url: str) -> tuple[str, dict[str, Any]]:
    metadata: dict[str, Any] = {"original_url": url, "resolved": False}
    if os.getenv("HSA_X_TOPIC_RESOLVE_LINKS", "true").strip().lower() not in {"1", "true", "yes"}:
        metadata["resolution_status"] = "disabled"
        return url, metadata
    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc.lower()
    should_resolve = any(host == item or host.endswith(f".{item}") for item in _RESOLVABLE_SHORT_LINK_HOSTS)
    if not should_resolve:
        metadata["resolution_status"] = "not_short_link"
        return url, metadata
    try:
        resolved_url = _follow_redirects(url)
    except Exception as exc:
        metadata["resolution_status"] = "failed"
        metadata["resolution_error"] = str(exc)
        return url, metadata
    metadata["resolved_url"] = resolved_url
    metadata["resolved"] = resolved_url != url
    metadata["resolution_status"] = "resolved" if resolved_url != url else "unchanged"
    return resolved_url, metadata


def _follow_redirects(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": os.getenv(
                "HSA_X_TOPIC_LINK_RESOLVER_USER_AGENT",
                "hsa-dagster/0.1 link resolver; contact poppa@bradyandgraffiti.com",
            )
        },
        method="GET",
    )
    with urllib.request.urlopen(
        request,
        timeout=float(os.getenv("HSA_X_TOPIC_LINK_RESOLVE_TIMEOUT_SECONDS", "10")),
    ) as response:
        return response.geturl()


def _extract_doi(url: str) -> str | None:
    parsed = urllib.parse.urlparse(url)
    if "doi.org" in parsed.netloc.lower():
        path = urllib.parse.unquote(parsed.path.lstrip("/"))
        return path or None
    match = _DOI_RE.search(url)
    if not match:
        return None
    return match.group(0).rstrip(").,;")


def _build_review_payload(
    request: XTopicReviewRequest,
    candidates: Sequence[Mapping[str, Any]],
    deterministic_result: XTopicReviewResult,
) -> dict[str, Any]:
    return {
        "task": "Review X/Twitter topic-monitoring candidates and flag linked primary sources for ingestion.",
        "rules": [
            "Never treat a social post as scientific evidence.",
            "Flag only durable linked sources for ingestion into primary API harvesters.",
            "Use needs_human_review when a link may matter but does not map cleanly.",
            "Use compliance_hold for protected, deleted, private, or policy-sensitive content.",
            "Return JSON only.",
        ],
        "allowed_actions": [
            "flag_for_ingestion",
            "queue_source_followup",
            "needs_link_review",
            "needs_human_review",
            "reject_noise",
            "compliance_hold",
            "skip_no_durable_source",
        ],
        "allowed_identifier_types": [
            "doi",
            "pmid",
            "pmcid",
            "nct",
            "pubchem",
            "chembl",
            "uniprot",
            "rcsb_pdb",
            "geo",
            "sra",
            "unknown",
        ],
        "output_shape": {
            "actions": [
                {
                    "source_record_id": "tweet/post id",
                    "query_name": "query name or null",
                    "username": "username or null",
                    "action": "flag_for_ingestion",
                    "severity": "info|watch|blocking",
                    "reason": "short reviewer-facing reason",
                    "ingestible_links": [
                        {
                            "url": "durable URL",
                            "recommended_source_key": "pubmed|pmc_oa|crossref|clinicaltrials_gov|...",
                            "identifier_type": "doi|pmid|pmcid|nct|unknown",
                            "identifier": "identifier or null",
                            "should_ingest": True,
                            "reason": "why this should be harvested",
                            "metadata": {},
                        }
                    ],
                    "evidence_refs": ["candidate:<post id>", "linked URL"],
                    "metadata": {},
                }
            ],
            "evidence": {"review_summary": "short summary"},
            "errors": [],
        },
        "candidates": [_compact_candidate(candidate) for candidate in candidates],
        "deterministic_result": deterministic_result.model_dump(mode="json"),
        "metadata": request.metadata,
    }


def _compact_candidate(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "post_id": candidate.get("post_id") or candidate.get("source_record_id"),
        "query_name": candidate.get("query_name") or candidate.get("matched_query_name"),
        "username": candidate.get("username"),
        "quality_score": candidate.get("quality_score"),
        "review_status": candidate.get("review_status"),
        "matched_terms": candidate.get("matched_terms", []),
        "durable_links": candidate.get("durable_links", []),
        "text_preview": candidate.get("text_preview"),
        "review_reasons": candidate.get("review_reasons", []),
    }


def _external_review_required_result(
    request: XTopicReviewRequest,
    deterministic_result: XTopicReviewResult,
    review_payload: dict[str, Any],
) -> XTopicReviewResult:
    actions = list(deterministic_result.actions)
    actions.append(
        XTopicReviewAction(
            source_record_id="external_review",
            action="needs_human_review",
            severity="watch",
            reason="External model review is required before any linked source is queued for ingestion.",
            evidence_refs=["review_payload"],
            metadata={"review_payload": review_payload},
        )
    )
    return _finalize_result(
        deterministic_result.model_copy(
            update={
                "actions": actions,
                "evidence": {
                    **deterministic_result.evidence,
                    "review_mode": request.review_mode,
                    "external_review_required": True,
                    "review_payload": review_payload,
                },
            }
        )
    )


def _run_openrouter_reviews(
    request: XTopicReviewRequest,
    deterministic_result: XTopicReviewResult,
    review_payload: dict[str, Any],
) -> XTopicReviewResult:
    reviews = []
    errors = []
    selected: XTopicReviewResult | None = None

    for model_name in _review_models(request):
        try:
            review = _openrouter_review_model(model_name, review_payload)
            raw_payload = _parse_json_object(review["text"])
            result = _result_from_payload(request, raw_payload)
            result = _apply_deterministic_guardrails(result, deterministic_result)
            selected = result
            reviews.append(
                {
                    "model_name": model_name,
                    "status": "completed",
                    "resolved_model": review["metadata"].get("model_name"),
                    "usage": review["metadata"].get("usage", {}),
                    "action_count": len(result.actions),
                    "ingestion_candidate_count": result.ingestion_candidate_count,
                }
            )
            if request.review_mode == "openrouter_required":
                break
        except Exception as exc:
            errors.append(f"{model_name}: {exc}")
            reviews.append({"model_name": model_name, "status": "failed", "error": str(exc)})

    if selected is None:
        if request.review_mode == "openrouter_required":
            raise RuntimeError(f"OpenRouter X topic review failed for all models: {errors}")
        selected = deterministic_result

    evidence = {
        **selected.evidence,
        "review_mode": request.review_mode,
        "model_reviews": reviews,
        "openrouter_errors": errors,
    }
    return _finalize_result(selected.model_copy(update={"evidence": evidence, "errors": list(selected.errors) + errors}))


def _review_models(request: XTopicReviewRequest) -> list[str]:
    if request.review_models:
        return request.review_models
    configured = os.getenv("HSA_X_TOPIC_REVIEW_MODELS")
    if configured:
        return [model.strip() for model in configured.split(",") if model.strip()]
    if request.review_mode == "openrouter_compare":
        return list(DEFAULT_X_TOPIC_COMPARE_MODELS)
    return [os.getenv("HSA_X_TOPIC_REVIEW_MODEL", DEFAULT_X_TOPIC_REVIEW_MODEL)]


def _openrouter_review_model(model_name: str, review_payload: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for OpenRouter X topic review.")
    payload = {
        "model": model_name,
        "temperature": float(os.getenv("HSA_X_TOPIC_REVIEW_TEMPERATURE", "0")),
        "max_tokens": int(os.getenv("HSA_X_TOPIC_REVIEW_MAX_TOKENS", "2500")),
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _X_TOPIC_REVIEW_SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(review_payload, sort_keys=True, default=str)},
        ],
    }
    http_request = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "HTTP-Referer": os.getenv("OPENROUTER_HTTP_REFERER", "https://github.com/chasepenelli/hsa-dagster"),
            "X-Title": os.getenv("OPENROUTER_APP_TITLE", "hsa-dagster"),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(
            http_request,
            timeout=float(os.getenv("HSA_X_TOPIC_REVIEW_TIMEOUT_SECONDS", "120")),
        ) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenRouter HTTP {error.code}: {body}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"OpenRouter request failed: {error}") from error

    choices = response_payload.get("choices") or []
    if not choices:
        raise RuntimeError(f"OpenRouter response had no choices: {response_payload}")
    message = choices[0].get("message") or {}
    text = message.get("content") or ""
    if not text:
        raise RuntimeError(f"OpenRouter response had no text content: {response_payload}")
    return {
        "text": text,
        "metadata": {
            "provider": "openrouter",
            "model_name": response_payload.get("model", model_name),
            "requested_model": model_name,
            "request_id": response_payload.get("id"),
            "usage": response_payload.get("usage", {}),
        },
    }


def _parse_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", stripped, flags=re.DOTALL)
    if fenced:
        stripped = fenced.group(1).strip()
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end <= start:
            raise
        parsed = json.loads(stripped[start : end + 1])
    if not isinstance(parsed, dict):
        raise RuntimeError("OpenRouter model response must be a JSON object.")
    return parsed


def _result_from_payload(request: XTopicReviewRequest, payload: Mapping[str, Any]) -> XTopicReviewResult:
    actions = (
        payload.get("actions")
        or payload.get("recommendations")
        or payload.get("ingestion_recommendations")
        or []
    )
    if not isinstance(actions, list):
        raise RuntimeError("X topic review response must include an actions list.")
    parsed_actions = [
        action
        for raw_action in actions
        if isinstance(raw_action, dict)
        if (action := _sanitize_model_action(raw_action)) is not None
    ]
    result = XTopicReviewResult(
        model_profile=request.model_profile,
        actions=parsed_actions,
        evidence=payload.get("evidence", {}) if isinstance(payload.get("evidence"), dict) else {},
        errors=list(payload.get("errors", [])) if isinstance(payload.get("errors"), list) else [],
    )
    return _finalize_result(result)


def _sanitize_model_action(raw_action: Mapping[str, Any]) -> XTopicReviewAction | None:
    post_id = _optional_str(raw_action.get("source_record_id") or raw_action.get("post_id"))
    if post_id is None:
        return None
    action = _allowed_value(raw_action.get("action"), _ALLOWED_ACTIONS, "needs_human_review")
    severity = _allowed_value(raw_action.get("severity"), _ALLOWED_SEVERITIES, "watch")
    links = []
    raw_links = raw_action.get("ingestible_links") or raw_action.get("links") or []
    if isinstance(raw_links, list):
        links = [
            link
            for raw_link in raw_links
            if (link := _sanitize_model_link(raw_link)) is not None
        ]
    evidence_refs = [
        str(ref)
        for ref in raw_action.get("evidence_refs", [])
        if isinstance(ref, str | int | float)
    ]
    metadata = raw_action.get("metadata") if isinstance(raw_action.get("metadata"), dict) else {}
    return XTopicReviewAction(
        source_record_id=post_id,
        query_name=_optional_str(raw_action.get("query_name")),
        username=_optional_str(raw_action.get("username")),
        action=action,  # type: ignore[arg-type]
        severity=severity,  # type: ignore[arg-type]
        reason=_optional_str(raw_action.get("reason") or raw_action.get("rationale"))
        or "Model review did not provide a reason.",
        ingestible_links=links,
        evidence_refs=evidence_refs,
        metadata=metadata,
    )


def _sanitize_model_link(raw_link: Any) -> XTopicLinkedSource | None:
    if isinstance(raw_link, str):
        return _classify_link(raw_link)
    if not isinstance(raw_link, Mapping):
        return None
    url = _optional_str(raw_link.get("url"))
    if url is None:
        return None
    classified = _classify_link(url)
    identifier_type = _allowed_value(
        raw_link.get("identifier_type"),
        _ALLOWED_IDENTIFIER_TYPES,
        classified.identifier_type,
    )
    should_ingest = raw_link.get("should_ingest")
    if isinstance(should_ingest, str):
        should_ingest = should_ingest.strip().lower() in {"1", "true", "yes"}
    elif not isinstance(should_ingest, bool):
        should_ingest = classified.should_ingest
    metadata = raw_link.get("metadata") if isinstance(raw_link.get("metadata"), dict) else {}
    return XTopicLinkedSource(
        url=url,
        recommended_source_key=_optional_str(
            raw_link.get("recommended_source_key") or classified.recommended_source_key
        ),
        identifier_type=identifier_type,  # type: ignore[arg-type]
        identifier=_optional_str(raw_link.get("identifier") or classified.identifier),
        should_ingest=should_ingest,
        reason=_optional_str(raw_link.get("reason")) or classified.reason,
        metadata=metadata,
    )


def _apply_deterministic_guardrails(
    model_result: XTopicReviewResult,
    deterministic_result: XTopicReviewResult,
) -> XTopicReviewResult:
    actions = list(model_result.actions)
    existing = {
        (
            action.source_record_id,
            action.action,
            tuple(sorted(link.url for link in action.ingestible_links)),
        )
        for action in actions
    }
    for action in deterministic_result.actions:
        if action.action not in {"flag_for_ingestion", "compliance_hold"}:
            continue
        key = (
            action.source_record_id,
            action.action,
            tuple(sorted(link.url for link in action.ingestible_links)),
        )
        if key not in existing:
            actions.append(action)
            existing.add(key)
    return _finalize_result(model_result.model_copy(update={"actions": actions}))


def _finalize_result(result: XTopicReviewResult) -> XTopicReviewResult:
    ingestion_keys: set[tuple[str, str]] = set()
    for action in result.actions:
        for link in action.ingestible_links:
            if link.should_ingest:
                ingestion_keys.add((action.source_record_id, link.url))
        if action.action in {"flag_for_ingestion", "queue_source_followup"} and not action.ingestible_links:
            ingestion_keys.add((action.source_record_id, action.action))
    ingestion_candidate_count = len(ingestion_keys)
    needs_human_review_count = sum(
        1
        for action in result.actions
        if action.action in {"needs_link_review", "needs_human_review", "compliance_hold"}
    )
    rejected_count = sum(
        1
        for action in result.actions
        if action.action in {"reject_noise", "skip_no_durable_source"}
    )
    return result.model_copy(
        update={
            "ingestion_candidate_count": ingestion_candidate_count,
            "needs_human_review_count": needs_human_review_count,
            "rejected_count": rejected_count,
        }
    )


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _float_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _allowed_value(value: Any, allowed: set[str], default: str) -> str:
    text = str(value or "").strip()
    return text if text in allowed else default


_X_TOPIC_REVIEW_SYSTEM_PROMPT = """You are a scientific ingestion-review agent for HSA comparative oncology monitoring.

Your job is to review normalized X/Twitter candidates and decide whether any
linked article, DOI, PubMed page, PMC page, clinical trial, dataset, compound,
protein, structure, or safety source should be ingested through the primary
database/API harvesters.

Rules:
- Social posts are discovery signals only, never scientific evidence.
- Do not recommend storing or citing a social post as a claim source.
- Flag durable linked sources for ingestion when they map to implemented
  sources: pubmed, pmc_oa, crossref, clinicaltrials_gov, pubchem, chembl,
  uniprot, rcsb_pdb, geo, or sra.
- If a link may be important but does not map cleanly, use needs_link_review.
- If there is no durable source, use skip_no_durable_source or needs_human_review.
- If content appears private, deleted, protected, or compliance-sensitive, use
  compliance_hold.
- Return only one valid JSON object matching the requested output shape.
"""
