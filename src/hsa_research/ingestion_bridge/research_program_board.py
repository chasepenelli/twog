"""Research Program Board agent for big scientific bets."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
import os
import re
from typing import Any
import urllib.error
import urllib.request
from uuid import NAMESPACE_URL, uuid5

from .contracts import (
    DocumentChunk,
    ResearchProgramEvidenceTask,
    ResearchProgramQuestion,
    ResearchProgramRecord,
    ResearchProgramReviewRequest,
    ResearchProgramReviewResult,
    ValidationPacket,
)
from .model_policy import big_idea_openrouter_model
from .repository import ResearchRepository, keyword_terms


RESEARCH_PROGRAM_BOARD_AGENT_NAME = "research_program_board_agent"
RESEARCH_PROGRAM_BOARD_AGENT_VERSION = "v1"
DEFAULT_RESEARCH_PROGRAM_BOARD_MODEL = big_idea_openrouter_model()

_SYSTEM_PROMPT = """You are the TWOG Research Program Board agent.

Your job is to manage finite big-bet scientific programs, not to produce endless ideas.
Use only supplied evidence references. Do not invent papers, outcomes, assays, doses, or schedules.

Return strict JSON only. Every program must define:
- big thesis and disease model
- therapy or modality families
- exactly 2 to 4 decisive questions
- capped evidence tasks that answer those questions
- tools, metrics, and values that would change confidence
- stop criteria
- gate decision

Separate discovery merit from execution readiness. Missing dose, PK/PD, tolerance, or protocol details
can be blockers for execution without killing the research program.
"""


def run_research_program_board_agent(
    repository: ResearchRepository,
    request: ResearchProgramReviewRequest,
    *,
    validation_packets: Sequence[ValidationPacket] = (),
) -> ResearchProgramReviewResult:
    """Run the finite research program board against stored evidence."""

    evidence = _build_evidence_payload(repository, request, validation_packets)
    if request.review_mode == "deterministic_only":
        return _deterministic_program_result(request, evidence)
    review = _run_openrouter_program_review(request, evidence)
    return _result_from_model(request, evidence, review)


def summarize_research_program_board(result: ResearchProgramReviewResult) -> dict[str, Any]:
    return {
        "program_count": result.program_count,
        "persisted_count": result.persisted_count,
        "packet_count": result.packet_count,
        "evidence_chunk_count": result.evidence_chunk_count,
        "gate_counts": {
            gate: sum(1 for program in result.programs if program.gate_decision == gate)
            for gate in sorted({program.gate_decision for program in result.programs})
        },
        "selected_model": result.programs[0].metadata.get("model_name") if result.programs else None,
    }


def _build_evidence_payload(
    repository: ResearchRepository,
    request: ResearchProgramReviewRequest,
    validation_packets: Sequence[ValidationPacket],
) -> dict[str, Any]:
    query = request.topic_query or request.thesis_topic
    chunks = _select_evidence_chunks(repository, request, query)
    return {
        "thesis_topic": request.thesis_topic,
        "disease_scope": request.disease_scope,
        "topic_query": query,
        "source_key": request.source_key,
        "max_evidence_loops": request.max_evidence_loops,
        "validation_packets": [
            _packet_payload(packet, index)
            for index, packet in enumerate(validation_packets[: request.max_packets], start=1)
        ],
        "evidence_chunks": [
            _chunk_payload(repository, chunk, index, request.max_chunk_chars)
            for index, chunk in enumerate(chunks[: request.max_chunks], start=1)
        ],
    }


def _select_evidence_chunks(
    repository: ResearchRepository,
    request: ResearchProgramReviewRequest,
    query: str,
) -> list[DocumentChunk]:
    if request.max_chunks <= 0:
        return []
    candidates = repository.list_document_chunks(source_key=request.source_key, limit=max(request.max_chunks * 20, 100))
    terms = set(keyword_terms(query))
    if not terms:
        return candidates[: request.max_chunks]

    scored: list[tuple[int, DocumentChunk]] = []
    for chunk in candidates:
        text = chunk.text_content.lower()
        score = sum(1 for term in terms if term in text)
        if score:
            scored.append((score, chunk))
    scored.sort(key=lambda item: (item[0], -item[1].chunk_index), reverse=True)
    return [chunk for _score, chunk in scored[: request.max_chunks]]


def _packet_payload(packet: ValidationPacket, index: int) -> dict[str, Any]:
    return {
        "ref": f"packet:{index}",
        "packet_id": packet.packet_id,
        "title": packet.title,
        "hypothesis": packet.hypothesis,
        "candidate_therapies": packet.candidate_therapies,
        "targets": packet.targets,
        "biomarkers": packet.biomarkers,
        "direct_evidence_refs": packet.direct_evidence_refs,
        "analog_evidence_refs": packet.analog_evidence_refs,
        "missing_evidence": packet.missing_evidence,
        "material_updates": packet.evidence_addendum.material_updates,
        "risk_annotations": packet.risk_annotations,
        "protocol_blockers": packet.protocol_blockers,
        "discovery_readiness": packet.discovery_readiness,
        "validation_strategy_readiness": packet.validation_strategy_readiness,
        "protocol_readiness": packet.protocol_readiness,
        "score": packet.score,
    }


def _chunk_payload(
    repository: ResearchRepository,
    chunk: DocumentChunk,
    index: int,
    max_chars: int,
) -> dict[str, Any]:
    obj = repository.get_research_object(chunk.research_object_id)
    return {
        "ref": f"chunk:{index}",
        "chunk_id": str(chunk.id),
        "research_object_id": str(chunk.research_object_id),
        "source_key": obj.source_key if obj else None,
        "title": obj.title if obj else "",
        "object_type": obj.object_type if obj else None,
        "identifiers": obj.identifiers if obj else {},
        "section_label": chunk.section_label,
        "text": chunk.text_content[:max_chars],
    }


def _deterministic_program_result(
    request: ResearchProgramReviewRequest,
    evidence: Mapping[str, Any],
) -> ResearchProgramReviewResult:
    refs = _available_refs(evidence)
    packet_count = len(evidence.get("validation_packets") or [])
    chunk_count = len(evidence.get("evidence_chunks") or [])
    confidence = 0.62 if refs else 0.45
    gate = "ready_for_therapy_ideas" if packet_count >= 1 and chunk_count >= 2 else "needs_one_more_pass"
    status = "ready_for_therapy_ideas" if gate == "ready_for_therapy_ideas" else "needs_one_more_pass"
    program = ResearchProgramRecord(
        program_id=_program_id(request.thesis_topic, "vascular_ecology"),
        title="Vascular injury, coagulation, and angiogenesis ecology program",
        thesis=(
            "Canine HSA may be more tractable as a vascular-injury, coagulation, and angiogenesis ecology "
            "than as a single-target kinase problem."
        ),
        disease_model=(
            "The disease model links endothelial tumor biology, bleeding/coagulation signals, VEGF/KDR activity, "
            "and cross-species angiosarcoma analog evidence."
        ),
        thesis_area="vascular_coagulation_angiogenesis",
        therapy_families=["vascular signaling modulation", "coagulation-aware combinations", "immune-vascular strategies"],
        modality_families=["small molecule", "biomarker-gated combination", "peptide or targeted delivery review"],
        decisive_questions=[
            ResearchProgramQuestion(
                question="Is coagulation or vascular-injury biology consistently linked to HSA progression or response?",
                rationale="This determines whether the program is broader than VEGFR monotherapy.",
                metric_plan=["independent cited links to coagulation, bleeding, endothelial injury, or outcomes"],
                tool_hints=["literature_review", "safety_translational_risk_review"],
                confidence_increase_criteria=["multiple independent canine or comparative oncology sources support the link"],
                confidence_decrease_criteria=["evidence is limited to nonspecific bleeding complications"],
                evidence_refs=refs[:4],
            ),
            ResearchProgramQuestion(
                question="Which measurable biomarkers can stratify a vascular-ecology strategy?",
                rationale="A program should spawn therapy ideas only when it can specify measurable biology.",
                metric_plan=["KDR/PIK3CA expression or pathway activity", "tumor versus normal contrast", "assayable endpoint"],
                tool_hints=["omics_expression_review", "target_expression_review"],
                confidence_increase_criteria=["biomarkers map to targetable pathways and available assays"],
                confidence_decrease_criteria=["markers are nonspecific or lack assay context"],
                evidence_refs=refs[:4],
            ),
        ],
        evidence_tasks=[
            ResearchProgramEvidenceTask(
                title="Map coagulation and vascular-injury evidence",
                objective="Acquire cited evidence linking coagulation, hemorrhage, endothelial injury, and HSA outcomes.",
                source_keys=["pubmed", "europe_pmc", "pmc_oa"],
                tool_hints=["literature_review"],
                metrics=["unique source count", "direct canine evidence count", "contradiction count"],
                pass_values=["at least three independent direct or strong analog sources"],
                fail_values=["only nonspecific adverse-event or complication references"],
                evidence_refs=refs[:4],
            ),
            ResearchProgramEvidenceTask(
                task_type="omics_lookup",
                title="Check vascular pathway biomarker support",
                objective="Review KDR, PIK3CA, VEGF-axis, endothelial, and coagulation-related markers in canine HSA.",
                source_keys=["geo", "arrayexpress", "pubmed"],
                tool_hints=["omics_expression_review"],
                metrics=["expression support", "pathway activation evidence", "tumor-normal contrast"],
                pass_values=["biomarker signal can gate a downstream therapy idea"],
                fail_values=["no disease-relevant or assayable biomarker signal"],
                evidence_refs=refs[:4],
            ),
        ],
        metric_plan=[
            "biological plausibility",
            "cross-species support",
            "evidence density",
            "testability",
            "therapeutic leverage",
        ],
        recommended_tools=["literature_review", "omics_expression_review", "safety_translational_risk_review"],
        stop_criteria=[
            "Archive if vascular/coagulation evidence is only nonspecific complication management.",
            "Archive if no measurable biomarker or assayable endpoint emerges after two evidence loops.",
        ],
        confidence_increase_criteria=[
            "Direct canine HSA evidence supports vascular/coagulation biology as outcome-relevant.",
            "Human angiosarcoma analog evidence converges on the same pathway ecology.",
        ],
        confidence_decrease_criteria=[
            "Evidence is sparse, contradictory, or not linked to tumor biology.",
            "No assayable biomarker or model system can be specified.",
        ],
        downstream_therapy_opportunities=[
            "VEGFR/PI3K biomarker-gated combinations",
            "coagulation-aware vascular normalization strategy",
            "immune-vascular combination strategy",
            "peptide or targeted delivery modality review",
        ],
        status=status,
        gate_decision=gate,
        biological_plausibility_score=0.7,
        cross_species_support_score=0.62,
        evidence_density_score=0.58 if refs else 0.35,
        novelty_score=0.76,
        testability_score=0.64,
        therapeutic_leverage_score=0.68,
        failure_risk_score=0.42,
        confidence_score=confidence,
        max_evidence_loops=request.max_evidence_loops,
        source_query=evidence.get("topic_query"),
        source_packet_ids=[packet.get("packet_id") for packet in evidence.get("validation_packets", []) if packet.get("packet_id")],
        evidence_refs=refs[:10],
        review_summary="Deterministic board pass created one bounded research program with two decisive questions.",
        metadata={"review_mode": request.review_mode, "evidence": _evidence_summary(evidence)},
    )
    return ResearchProgramReviewResult(
        program_count=1,
        persisted_count=0,
        packet_count=packet_count,
        evidence_chunk_count=chunk_count,
        programs=[program],
    )


def _run_openrouter_program_review(
    request: ResearchProgramReviewRequest,
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    errors: list[str] = []
    for model_name in _select_models(request):
        try:
            return _openrouter_review_model(model_name, _review_payload(request, evidence))
        except Exception as exc:
            errors.append(f"{model_name}: {exc}")
            break
    raise RuntimeError("; ".join(errors) or "OpenRouter research program board failed")


def _result_from_model(
    request: ResearchProgramReviewRequest,
    evidence: Mapping[str, Any],
    review: Mapping[str, Any],
) -> ResearchProgramReviewResult:
    payload = _load_json_object(str(review.get("text") or ""))
    programs = [
        program
        for raw in payload.get("programs", [])
        if isinstance(raw, Mapping)
        if (program := _program_from_payload(raw, request, evidence, review)) is not None
    ][: request.max_programs]
    return ResearchProgramReviewResult(
        program_count=len(programs),
        persisted_count=0,
        packet_count=len(evidence.get("validation_packets") or []),
        evidence_chunk_count=len(evidence.get("evidence_chunks") or []),
        programs=programs,
        errors=[str(item).strip() for item in payload.get("errors", []) if str(item).strip()],
    )


def _program_from_payload(
    raw: Mapping[str, Any],
    request: ResearchProgramReviewRequest,
    evidence: Mapping[str, Any],
    review: Mapping[str, Any],
) -> ResearchProgramRecord | None:
    title = str(raw.get("title") or request.thesis_topic).strip()
    thesis = str(raw.get("thesis") or raw.get("hypothesis") or title).strip()
    if not title or not thesis:
        return None
    questions = [
        ResearchProgramQuestion(
            question=str(item.get("question") or item.get("title") or "").strip(),
            rationale=str(item.get("rationale") or "").strip(),
            metric_plan=_string_list(item.get("metric_plan") or item.get("metrics")),
            tool_hints=_string_list(item.get("tool_hints")),
            confidence_increase_criteria=_string_list(item.get("confidence_increase_criteria")),
            confidence_decrease_criteria=_string_list(item.get("confidence_decrease_criteria")),
            evidence_refs=_valid_refs(item.get("evidence_refs"), evidence),
        )
        for item in raw.get("decisive_questions", [])
        if isinstance(item, Mapping)
    ][:4]
    while len(questions) < 2:
        questions.append(
            ResearchProgramQuestion(
                question=f"What evidence would materially change confidence in {title}?",
                rationale="Fallback question added to preserve finite board contract.",
                metric_plan=["confidence shift"],
                tool_hints=["literature_review"],
            )
        )
    tasks = [
        ResearchProgramEvidenceTask(
            task_type=_task_type(item.get("task_type")),
            title=str(item.get("title") or item.get("objective") or "Evidence acquisition task").strip(),
            objective=str(item.get("objective") or item.get("title") or "Acquire decisive evidence.").strip(),
            source_keys=_string_list(item.get("source_keys")),
            tool_hints=_string_list(item.get("tool_hints")),
            metrics=_string_list(item.get("metrics")),
            pass_values=_string_list(item.get("pass_values") or item.get("success_values")),
            fail_values=_string_list(item.get("fail_values") or item.get("failure_values")),
            evidence_refs=_valid_refs(item.get("evidence_refs"), evidence),
        )
        for item in raw.get("evidence_tasks", [])
        if isinstance(item, Mapping)
    ][:25]
    gate = _gate_decision(raw.get("gate_decision"))
    status = _status_from_gate(raw.get("status"), gate)
    model_metadata = review.get("metadata") if isinstance(review.get("metadata"), Mapping) else {}
    return ResearchProgramRecord(
        program_id=_program_id(title, thesis),
        title=title,
        thesis=thesis,
        disease_model=str(raw.get("disease_model") or "No disease model supplied.").strip(),
        disease_scope=str(raw.get("disease_scope") or request.disease_scope).strip(),
        thesis_area=str(raw.get("thesis_area") or "comparative_oncology"),
        therapy_families=_string_list(raw.get("therapy_families")),
        modality_families=_string_list(raw.get("modality_families")),
        decisive_questions=questions,
        evidence_tasks=tasks,
        metric_plan=_string_list(raw.get("metric_plan")),
        recommended_tools=_string_list(raw.get("recommended_tools")),
        stop_criteria=_string_list(raw.get("stop_criteria")) or [
            "Archive if decisive evidence does not support the program after the capped evidence loops."
        ],
        confidence_increase_criteria=_string_list(raw.get("confidence_increase_criteria")),
        confidence_decrease_criteria=_string_list(raw.get("confidence_decrease_criteria")),
        downstream_therapy_opportunities=_string_list(raw.get("downstream_therapy_opportunities")),
        status=status,
        gate_decision=gate,
        biological_plausibility_score=_score(raw, "biological_plausibility_score", 0.5),
        cross_species_support_score=_score(raw, "cross_species_support_score", 0.5),
        evidence_density_score=_score(raw, "evidence_density_score", 0.5),
        novelty_score=_score(raw, "novelty_score", 0.5),
        testability_score=_score(raw, "testability_score", 0.5),
        therapeutic_leverage_score=_score(raw, "therapeutic_leverage_score", 0.5),
        failure_risk_score=_score(raw, "failure_risk_score", 0.5),
        confidence_score=_score(raw, "confidence_score", 0.5),
        max_evidence_loops=request.max_evidence_loops,
        evidence_loop_count=min(int(raw.get("evidence_loop_count") or 0), request.max_evidence_loops),
        source_query=evidence.get("topic_query"),
        source_packet_ids=[
            packet.get("packet_id") for packet in evidence.get("validation_packets", []) if packet.get("packet_id")
        ],
        evidence_refs=_valid_refs(raw.get("evidence_refs"), evidence),
        review_summary=str(raw.get("review_summary") or raw.get("summary") or "").strip(),
        errors=_string_list(raw.get("errors")),
        metadata={
            "review_mode": request.review_mode,
            "requested_model": model_metadata.get("requested_model"),
            "model_name": model_metadata.get("model_name"),
            "usage": model_metadata.get("usage", {}),
            "evidence": _evidence_summary(evidence),
        },
    )


def _review_payload(request: ResearchProgramReviewRequest, evidence: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "instructions": [
            "Return JSON only.",
            "Use only supplied evidence refs.",
            "Generate finite research programs, not open-ended idea lists.",
            "Each program must have 2 to 4 decisive questions.",
            "Each program must include stop criteria and a gate decision.",
            "Do not recommend validation dispatch or treatment use.",
        ],
        "response_contract": _response_contract(),
        "request": request.model_dump(mode="json"),
        "evidence_payload": evidence,
    }


def _response_contract() -> dict[str, Any]:
    return {
        "programs": [
            {
                "title": "string",
                "thesis": "string",
                "disease_model": "string",
                "thesis_area": "string",
                "therapy_families": ["string"],
                "modality_families": ["string"],
                "decisive_questions": [
                    {
                        "question": "string",
                        "rationale": "string",
                        "metric_plan": ["string"],
                        "tool_hints": ["string"],
                        "confidence_increase_criteria": ["string"],
                        "confidence_decrease_criteria": ["string"],
                        "evidence_refs": ["packet:1 or chunk:1"],
                    }
                ],
                "evidence_tasks": [
                    {
                        "task_type": "literature_search",
                        "title": "string",
                        "objective": "string",
                        "source_keys": ["pubmed"],
                        "tool_hints": ["literature_review"],
                        "metrics": ["string"],
                        "pass_values": ["string"],
                        "fail_values": ["string"],
                        "evidence_refs": ["packet:1 or chunk:1"],
                    }
                ],
                "metric_plan": ["string"],
                "recommended_tools": ["string"],
                "stop_criteria": ["string"],
                "confidence_increase_criteria": ["string"],
                "confidence_decrease_criteria": ["string"],
                "downstream_therapy_opportunities": ["string"],
                "status": "active",
                "gate_decision": "needs_one_more_pass",
                "biological_plausibility_score": 0.0,
                "cross_species_support_score": 0.0,
                "evidence_density_score": 0.0,
                "novelty_score": 0.0,
                "testability_score": 0.0,
                "therapeutic_leverage_score": 0.0,
                "failure_risk_score": 0.0,
                "confidence_score": 0.0,
                "review_summary": "string",
                "evidence_refs": ["packet:1 or chunk:1"],
                "errors": ["string"],
            }
        ],
        "errors": ["string"],
    }


def _openrouter_review_model(model_name: str, review_payload: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for research program board review.")
    payload = {
        "model": model_name,
        "temperature": float(os.getenv("HSA_RESEARCH_PROGRAM_BOARD_TEMPERATURE", "0.25")),
        "max_tokens": int(os.getenv("HSA_RESEARCH_PROGRAM_BOARD_MAX_TOKENS", "7000")),
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(review_payload, sort_keys=True, default=str)},
        ],
    }
    request = urllib.request.Request(
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
            request,
            timeout=float(os.getenv("HSA_RESEARCH_PROGRAM_BOARD_TIMEOUT_SECONDS", "120")),
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
    text = ((choices[0].get("message") or {}).get("content")) or ""
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
            "finish_reason": choices[0].get("finish_reason"),
        },
    }


def _select_models(request: ResearchProgramReviewRequest) -> list[str]:
    if request.review_models:
        return list(request.review_models)
    return [os.getenv("HSA_RESEARCH_PROGRAM_BOARD_MODEL", big_idea_openrouter_model())]


def _load_json_object(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?", "", cleaned).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid research program board JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("Research program board response must be a JSON object")
    return payload


def _available_refs(evidence: Mapping[str, Any]) -> list[str]:
    refs = [str(packet.get("ref")) for packet in evidence.get("validation_packets", []) if packet.get("ref")]
    refs.extend(str(chunk.get("ref")) for chunk in evidence.get("evidence_chunks", []) if chunk.get("ref"))
    return refs


def _valid_refs(value: Any, evidence: Mapping[str, Any]) -> list[str]:
    valid = set(_available_refs(evidence))
    return [ref for ref in _string_list(value) if ref in valid]


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if not isinstance(value, list):
        return []
    deduped: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item).strip()
        if text and text not in seen:
            deduped.append(text)
            seen.add(text)
    return deduped


def _score(raw: Mapping[str, Any], key: str, default: float) -> float:
    try:
        return max(0.0, min(1.0, float(raw.get(key, default))))
    except (TypeError, ValueError):
        return default


def _gate_decision(value: Any) -> str:
    text = str(value or "needs_one_more_pass").strip()
    if text in {"archive", "needs_one_more_pass", "ready_for_therapy_ideas", "ready_for_validation_strategy"}:
        return text
    return "needs_one_more_pass"


def _status_from_gate(status_value: Any, gate: str) -> str:
    status = str(status_value or "").strip()
    allowed = {
        "proposed",
        "active",
        "needs_one_more_pass",
        "ready_for_therapy_ideas",
        "ready_for_validation_strategy",
        "archived",
    }
    if status in allowed:
        return status
    return {
        "archive": "archived",
        "needs_one_more_pass": "needs_one_more_pass",
        "ready_for_therapy_ideas": "ready_for_therapy_ideas",
        "ready_for_validation_strategy": "ready_for_validation_strategy",
    }[gate]


def _task_type(value: Any) -> str:
    text = str(value or "literature_search").strip()
    allowed = {
        "literature_search",
        "full_text_review",
        "omics_lookup",
        "clinical_trial_lookup",
        "drug_target_lookup",
        "safety_lookup",
        "expert_review",
        "x_topic_scan",
        "other",
    }
    return text if text in allowed else "other"


def _program_id(title: str, thesis: str) -> Any:
    return uuid5(NAMESPACE_URL, f"research_program:{title.strip().lower()}:{thesis.strip().lower()}")


def _evidence_summary(evidence: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "packet_count": len(evidence.get("validation_packets") or []),
        "evidence_chunk_count": len(evidence.get("evidence_chunks") or []),
        "refs": _available_refs(evidence)[:25],
    }
