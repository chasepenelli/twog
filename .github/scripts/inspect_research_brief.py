from __future__ import annotations

import json
import os
from typing import Any
from uuid import UUID

from hsa_research.ingestion_bridge.service import HSAResearchService
from hsa_research.ingestion_bridge.storage import build_research_repository


def _citation_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in payload.get("citations") or []:
        if not isinstance(item, dict):
            continue
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        identifiers = metadata.get("identifiers") if isinstance(metadata.get("identifiers"), dict) else {}
        rows.append(
            {
                "citation_id": item.get("citation_id"),
                "title": item.get("title"),
                "source_key": item.get("source_key"),
                "source_url": item.get("source_url"),
                "doi": identifiers.get("doi"),
                "pmid": identifiers.get("pmid"),
                "pmcid": identifiers.get("pmcid"),
            }
        )
    return rows


def main() -> None:
    source_brief_id = os.environ.get("SOURCE_BRIEF_ID")
    if not source_brief_id:
        raise SystemExit("SOURCE_BRIEF_ID is required.")

    service = HSAResearchService(build_research_repository())
    brief_id = UUID(source_brief_id)
    brief = service.get_research_brief(brief_id)
    if brief is None:
        raise SystemExit(f"Research brief not found: {brief_id}")

    source_evaluation_id = os.environ.get("SOURCE_EVALUATION_ID") or ""
    if source_evaluation_id:
        evaluation = service.get_research_brief_evaluation(UUID(source_evaluation_id))
    else:
        evaluations = service.list_research_brief_evaluations(brief_id=brief_id, limit=1)
        evaluation = evaluations[0] if evaluations else None

    result_payload = brief.result_payload or {}
    evaluation_payload = evaluation.result_payload if evaluation else {}
    report: dict[str, Any] = {
        "brief": {
            "brief_id": str(brief.brief_id),
            "agent_run_id": str(brief.agent_run_id) if brief.agent_run_id else None,
            "agent_run_ids": [str(value) for value in brief.agent_run_ids],
            "topic": brief.topic,
            "disease_scope": brief.disease_scope,
            "source_key": brief.source_key,
            "brief_style": brief.brief_style,
            "model_profile": brief.model_profile,
            "review_mode": brief.review_mode,
            "status": brief.status,
            "citation_count": brief.citation_count,
            "finding_count": brief.finding_count,
            "hypothesis_count": brief.hypothesis_count,
            "unresolved_question_count": brief.unresolved_question_count,
            "hard_error_count": brief.hard_error_count,
            "evidence_limitation_count": brief.evidence_limitation_count,
            "error_count": brief.error_count,
            "created_at": brief.created_at.isoformat(),
            "final_brief_excerpt": brief.final_brief[:5000],
            "summary": brief.summary,
            "citations": _citation_rows(result_payload)[:25],
            "ranked_hypotheses": list(result_payload.get("ranked_hypotheses") or [])[:5],
            "unresolved_questions": list(result_payload.get("unresolved_questions") or [])[:10],
            "errors": list(result_payload.get("errors") or [])[:10],
        },
        "evaluation": None,
    }
    if evaluation is not None:
        report["evaluation"] = {
            "evaluation_id": str(evaluation.evaluation_id),
            "brief_id": str(evaluation.brief_id),
            "agent_run_id": str(evaluation.agent_run_id) if evaluation.agent_run_id else None,
            "topic": evaluation.topic,
            "source_key": evaluation.source_key,
            "overall_score": evaluation.overall_score,
            "passes_quality_bar": evaluation.passes_quality_bar,
            "readiness": evaluation.readiness,
            "summary": evaluation.summary,
            "strengths": list(evaluation_payload.get("strengths") or [])[:10],
            "weaknesses": list(evaluation_payload.get("weaknesses") or [])[:10],
            "recommendations": list(evaluation_payload.get("recommendations") or [])[:10],
            "errors": list(evaluation.errors)[:10],
            "created_at": evaluation.created_at.isoformat(),
        }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
