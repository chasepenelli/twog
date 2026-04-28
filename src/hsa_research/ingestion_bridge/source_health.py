"""Source health reporting for the ingestion bridge."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from typing import Any

from .source_sets import ALL_API_SOURCE_KEYS, LITERATURE_FULL_TEXT_SOURCE_KEYS, TRIAGE_ONLY_SOURCE_KEYS
from .structured_orchestration import full_text_source_qa, full_text_triage_summary, structured_source_qa


COUNT_FIELDS = ("raw_records", "research_objects", "document_chunks", "claims")
HARD_COUNT_FIELDS = ("raw_records", "research_objects", "document_chunks")


def build_source_health_report(
    repository: Any,
    *,
    source_keys: Sequence[str] | None = None,
    sample_limit: int = 5,
    metadata_claim_limit: int = 500,
    min_health_score: float = 0.65,
    require_claims: bool = True,
) -> dict[str, Any]:
    """Return source-scoped health, risks, and recommended follow-up actions."""

    selected_sources = list(ALL_API_SOURCE_KEYS if source_keys is None else source_keys)
    source_reports = [
        build_source_health(
            repository,
            source_key,
            sample_limit=sample_limit,
            metadata_claim_limit=metadata_claim_limit,
            min_health_score=min_health_score,
            require_claims=require_claims,
        )
        for source_key in selected_sources
    ]
    status_counts = Counter(report["health_status"] for report in source_reports)
    failed_sources = [report["source_key"] for report in source_reports if not report["passes_minimum_bar"]]
    watch_sources = [report["source_key"] for report in source_reports if report["health_status"] == "watch"]
    triage_sources = [report["source_key"] for report in source_reports if report["health_status"] == "triage"]
    full_text_triage = [
        report["full_text_qa"]["triage"]
        for report in source_reports
        if report.get("full_text_qa", {}).get("triage")
    ]
    full_text_blocking_sources = [
        report["source_key"]
        for report in source_reports
        if report.get("full_text_triage_should_block_schedule")
    ]
    return {
        "source_keys": selected_sources,
        "sources": source_reports,
        "totals": _sum_runtime_summaries(source_reports),
        "summary": {
            "sources": len(source_reports),
            "healthy": status_counts.get("healthy", 0),
            "triage": status_counts.get("triage", 0),
            "watch": status_counts.get("watch", 0),
            "failing": status_counts.get("failing", 0),
        },
        "failed_sources": failed_sources,
        "triage_sources": triage_sources,
        "watch_sources": watch_sources,
        "full_text_triage": full_text_triage,
        "full_text_blocking_sources": full_text_blocking_sources,
        "passes_minimum_bar": not failed_sources,
        "minimum_bar": {
            "require_claims": require_claims,
            "min_health_score": min_health_score,
            "required_counts": list((*HARD_COUNT_FIELDS, "claims") if require_claims else HARD_COUNT_FIELDS),
            "triage_only_sources": list(TRIAGE_ONLY_SOURCE_KEYS),
        },
        "coverage": repository.coverage_summary(),
    }


def build_source_health(
    repository: Any,
    source_key: str,
    *,
    sample_limit: int = 5,
    metadata_claim_limit: int = 500,
    min_health_score: float = 0.65,
    require_claims: bool = True,
) -> dict[str, Any]:
    """Return one source health row."""

    triage_only = source_key in TRIAGE_ONLY_SOURCE_KEYS
    runtime = structured_source_qa(repository, source_key, sample_limit=sample_limit)
    if source_key in LITERATURE_FULL_TEXT_SOURCE_KEYS:
        full_text_qa = full_text_source_qa(repository, source_key, runtime_summary=runtime)
        runtime = {
            **runtime,
            "full_text_qa": full_text_qa,
            **full_text_triage_summary(full_text_qa),
        }
    claim_metadata = _claim_metadata_summary(repository, source_key, limit=metadata_claim_limit)
    health_score, signals, risks, recommended_actions = _score_source(
        runtime,
        claim_metadata=claim_metadata,
        require_claims=require_claims,
        triage_only=triage_only,
    )
    has_required_counts = _has_required_counts(runtime, require_claims=require_claims)
    full_text_required_passes = _full_text_required_passes(runtime)
    passes_minimum_bar = has_required_counts and full_text_required_passes and health_score >= min_health_score
    health_status = _health_status(
        passes_minimum_bar=passes_minimum_bar,
        risks=risks,
        triage_only=triage_only,
    )
    return {
        **runtime,
        "source_role": "triage" if triage_only else "evidence",
        "health_status": health_status,
        "health_score": health_score,
        "signals": signals,
        "risks": risks,
        "recommended_actions": recommended_actions,
        "claim_metadata": claim_metadata,
        "passes_minimum_bar": passes_minimum_bar,
        "minimum_bar": {
            "require_claims": require_claims,
            "min_health_score": min_health_score,
            "has_required_counts": has_required_counts,
            "full_text_required_passes": full_text_required_passes,
        },
    }


def _claim_metadata_summary(repository: Any, source_key: str, *, limit: int) -> dict[str, Any]:
    if not hasattr(repository, "list_claims"):
        return {
            "claims_sampled": 0,
            "extraction_status": {},
            "curation_status": {},
            "source_context_claims": 0,
        }

    claims = repository.list_claims(source_key=source_key, include_seed_claims=True, limit=limit)
    extraction_status = Counter()
    curation_status = Counter()
    source_context_claims = 0
    for claim in claims:
        metadata = claim.metadata
        extraction = str(metadata.get("extraction_status") or "unknown")
        curation = str(metadata.get("curation_status") or ("seed" if metadata.get("seed") else "uncurated"))
        extraction_status[extraction] += 1
        curation_status[curation] += 1
        if extraction == "source_context":
            source_context_claims += 1
    return {
        "claims_sampled": len(claims),
        "extraction_status": dict(sorted(extraction_status.items())),
        "curation_status": dict(sorted(curation_status.items())),
        "source_context_claims": source_context_claims,
    }


def _score_source(
    runtime: dict[str, Any],
    *,
    claim_metadata: dict[str, Any],
    require_claims: bool,
    triage_only: bool,
) -> tuple[float, list[str], list[str], list[str]]:
    signals: list[str] = []
    risks: list[str] = []
    recommended_actions: list[str] = []
    score = 0.0

    score += _score_count(runtime, "raw_records", 0.2, signals, risks, recommended_actions)
    score += _score_count(runtime, "research_objects", 0.2, signals, risks, recommended_actions)
    score += _score_count(runtime, "document_chunks", 0.2, signals, risks, recommended_actions)
    if require_claims:
        score += _score_count(runtime, "claims", 0.2, signals, risks, recommended_actions)
    else:
        score += 0.2
        signals.append("claims_optional_for_this_report")
        if runtime.get("claims", 0) < 1:
            risks.append("No claims are present; extraction and curation are not validated for this source yet.")
            recommended_actions.append("Run source-specific extraction and curation before using this source for evidence.")

    claim_status = runtime.get("claim_status", {})
    promoted_claims = int(claim_status.get("promote", 0))
    if promoted_claims > 0:
        score += 0.1
        signals.append("promoted_claims_present")
    elif runtime.get("claims", 0) > 0 and triage_only:
        score += 0.05
        signals.append("triage_only_source")
        recommended_actions.append("Route claims to the specialized triage agent before using them as evidence.")
    elif runtime.get("claims", 0) > 0:
        score += 0.05
        risks.append("No promoted claims are present; the source is currently review-heavy or context-only.")
        recommended_actions.append("Inspect curator decisions and source boundaries before treating claims as evidence.")

    if runtime.get("sample_claims"):
        score += 0.1
        signals.append("sample_claims_available")
    elif runtime.get("claims", 0) > 0:
        risks.append("Claims exist, but no sample claims were returned for QA.")
        recommended_actions.append("Inspect source runtime summary sampling for this source.")

    claims_sampled = int(claim_metadata.get("claims_sampled", 0))
    source_context_claims = int(claim_metadata.get("source_context_claims", 0))
    if claims_sampled > 0 and source_context_claims == claims_sampled:
        if triage_only:
            signals.append("source_context_claims_expected_for_triage")
            recommended_actions.append("Keep source-context claims review-only until the triage agent promotes them.")
        else:
            risks.append("All sampled claims are source-context triage claims, not typed biological evidence.")
            recommended_actions.append("Leave source-context claims review-only unless a typed extractor path is added.")

    full_text_qa = runtime.get("full_text_qa")
    if full_text_qa:
        triage = full_text_qa.get("triage") or {}
        if full_text_qa.get("passes_full_text_bar"):
            score += 0.1
            signals.append("full_text_body_chunks_present")
            if triage.get("action") == "no_action":
                signals.append("full_text_triage_clear")
        else:
            risks.append("Full-text source lacks persisted body chunks or current-run full-text output.")
            recommended_actions.append("Run the full-text refresh and inspect XML parsing plus section-labeled chunk output.")
            if triage:
                risks.append(
                    f"Full-text triage action: {triage.get('action')} "
                    f"({triage.get('severity')})."
                )
                recommended_actions.extend(triage.get("recommended_next_actions", []))

    return round(min(score, 1.0), 3), _dedupe(signals), _dedupe(risks), _dedupe(recommended_actions)


def _score_count(
    runtime: dict[str, Any],
    field: str,
    weight: float,
    signals: list[str],
    risks: list[str],
    recommended_actions: list[str],
) -> float:
    if runtime.get(field, 0) >= 1:
        signals.append(f"{field}_present")
        return weight

    risks.append(f"No {field.replace('_', ' ')} persisted for this source.")
    recommended_actions.append(_count_action(field))
    return 0.0


def _count_action(field: str) -> str:
    return {
        "raw_records": "Run the source harvester and confirm source query parameters are current.",
        "research_objects": "Inspect source normalization and dedupe key generation.",
        "document_chunks": "Inspect chunk text generation for the source harvester.",
        "claims": "Run claim extraction and curation; tune extractor rules if the source still returns zero claims.",
    }[field]


def _has_required_counts(runtime: dict[str, Any], *, require_claims: bool) -> bool:
    required_fields = (*HARD_COUNT_FIELDS, "claims") if require_claims else HARD_COUNT_FIELDS
    return all(runtime.get(field, 0) >= 1 for field in required_fields)


def _full_text_required_passes(runtime: dict[str, Any]) -> bool:
    full_text_qa = runtime.get("full_text_qa")
    return full_text_qa is None or bool(full_text_qa.get("passes_full_text_bar"))


def _health_status(*, passes_minimum_bar: bool, risks: Sequence[str], triage_only: bool) -> str:
    if not passes_minimum_bar:
        return "failing"
    if triage_only:
        return "triage"
    if risks:
        return "watch"
    return "healthy"


def _sum_runtime_summaries(reports: Sequence[dict[str, Any]]) -> dict[str, int]:
    return {field: sum(int(report.get(field, 0)) for report in reports) for field in COUNT_FIELDS}


def _dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(values))
