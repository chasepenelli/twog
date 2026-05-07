"""Validation queue execution agents.

These agents review approved validation queue items and persist a structured
decision. They are recommend-only: they do not launch wet lab work, schedule
jobs, or mutate therapy ideas directly.
"""

from __future__ import annotations

from collections.abc import Mapping
import json
import os
import re
from typing import Any
import urllib.error
import urllib.request

from .contracts import ValidationAgentResult, ValidationRequestQueueItem
from .model_policy import default_openrouter_model


VALIDATION_AGENT_VERSION = "v1"
DEFAULT_VALIDATION_AGENT_MODEL = default_openrouter_model()
VALIDATION_AGENT_MODEL_PROFILE = "openrouter_required"

_AGENT_NAMES = {
    "expert_review": "evidence_review_validation_agent",
    "literature_review": "evidence_review_validation_agent",
    "wet_lab": "assay_design_validation_agent",
    "target_validation": "mutation_function_validation_agent",
    "omics": "omics_validation_agent",
    "safety": "safety_review_validation_agent",
    "admet": "safety_review_validation_agent",
}

_PEPTIDE_TERMS = {
    "cyclic peptide",
    "epitope",
    "neoantigen",
    "peptide",
    "peptide vaccine",
    "peptidomimetic",
    "stapled peptide",
}

_SPECIALIST_RUBRICS = {
    "assay_design_validation_agent": [
        "Check whether model system, controls, readouts, replicates, and stop criteria are explicit.",
        "Hold if the proposed assay cannot be reproduced from the supplied context.",
        "Separate protocol design gaps from biological evidence gaps.",
    ],
    "evidence_review_validation_agent": [
        "Check source traceability, direct-vs-analog evidence, contradictions, and independent support.",
        "Hold if the evidence packet lacks enough cited support for the next validation step.",
        "Demote only when the supplied evidence contradicts the core premise or the risk is unacceptable.",
    ],
    "mutation_function_validation_agent": [
        "Check target identity, variant identity, conservation, functional consequence, and pathway relevance.",
        "Hold if gene/protein naming, variant annotation, or species conservation is ambiguous.",
        "Require source-traceable functional evidence before promotion.",
    ],
    "omics_validation_agent": [
        "Check dataset identity, sample metadata, disease scope, expression signal, and cross-species comparator fit.",
        "Hold if omics support lacks sample metadata or negative/null dataset review.",
        "Distinguish target-expression support from therapy-response support.",
    ],
    "peptide_specialist_validation_agent": [
        "Check peptide sequence or modality identity, target engagement plausibility, and mechanism fit.",
        "Review protease stability, delivery route/formulation, exposure, immunogenicity, and manufacturability risks.",
        "Hold if sequence/modality, delivery context, stability context, or immunogenicity review is missing.",
        "Demote if the modality is incompatible with the target biology or the supplied evidence shows unacceptable risk.",
    ],
    "safety_review_validation_agent": [
        "Check dose, route, PK/PD, adverse events, contraindications, and species-specific safety constraints.",
        "At discovery and validation-strategy stage, annotate dose/PK/safety gaps as protocol-stage risks rather than vetoing biological hypothesis exploration.",
        "Hold only when the reviewed item is being promoted into protocol, animal-facing work, dosing assumptions, or wet-lab/clinical dispatch without sufficient safety context.",
        "Treat hemorrhage, coagulation, immune, and cardiotoxicity signals as material risks.",
    ],
}

_SYSTEM_PROMPT = """You are a validation review agent for a translational oncology research system.
You review one approved validation queue item at a time.
Use only the supplied queue item, validation request, evidence refs, risks, required inputs, expected outputs, and assay context.
Do not invent papers, trial results, experiments that were already completed, or clinical claims.
Return strict JSON only.
Follow the specialist rubric supplied in the review payload.
Your decision must be one of: promote, hold, demote.
Use promote only when the supplied evidence and context are sufficient to move the item to the next validation step.
Use hold when the idea is plausible but needs more evidence, protocol detail, safety context, or expert review.
Use demote when the supplied evidence has a major contradiction, missing core premise, or unacceptable risk for this stage.
For discovery-stage therapy ideas, safety and PK/PD gaps should usually become risk annotations or protocol-stage blockers, not automatic demotion.
Do not block early combination ideation solely because dose, route, schedule, tolerability, or monitoring thresholds are not yet known.
Only apply a hard safety hold when the queue item is explicitly asking to advance into protocol execution, animal-facing validation, dosing recommendations, or wet-lab/clinical dispatch."""


def run_validation_agent(
    item: ValidationRequestQueueItem,
    *,
    model_profile: str = VALIDATION_AGENT_MODEL_PROFILE,
) -> ValidationAgentResult:
    """Run the appropriate validation review agent for one queue item."""

    agent_name = validation_agent_name(item)
    if model_profile == "deterministic_only":
        return _deterministic_validation_result(item, agent_name=agent_name, model_profile=model_profile)
    if model_profile == "external_required":
        return _external_required_result(item, agent_name=agent_name, model_profile=model_profile)
    review = _openrouter_review_model(_model_name(model_profile), _review_payload(item, agent_name))
    return _result_from_model(item, review, agent_name=agent_name, model_profile=model_profile)


def validation_agent_name(item: ValidationRequestQueueItem) -> str:
    catalog = _catalog_metadata(item)
    recommended_agent = str(catalog.get("recommended_agent_name") or "").strip()
    if recommended_agent:
        return recommended_agent

    task_type = str(item.task_type)
    validation_type = str(item.validation_request.validation_type)
    tool_hint = _tool_hint(item).casefold()
    text = _routing_text(item)
    if "peptide_specialist_review" in tool_hint or _contains_any(text, _PEPTIDE_TERMS):
        return "peptide_specialist_validation_agent"
    if "mutation_function_review" in tool_hint or validation_type == "homology":
        return "mutation_function_validation_agent"
    if validation_type in {"safety", "admet"} or task_type in {"safety", "admet"}:
        return "safety_review_validation_agent"
    if validation_type == "omics" or task_type == "omics":
        return "omics_validation_agent"
    if task_type == "wet_lab" or validation_type == "wet_lab":
        return "assay_design_validation_agent"
    return _AGENT_NAMES.get(task_type, "general_validation_review_agent")


def _catalog_metadata(item: ValidationRequestQueueItem) -> dict[str, Any]:
    for payload in (
        item.metadata,
        item.validation_request.metadata,
    ):
        if not isinstance(payload, Mapping):
            continue
        catalog = payload.get("validation_tool_catalog")
        if isinstance(catalog, Mapping):
            return dict(catalog)
    return {}


def _tool_hint(item: ValidationRequestQueueItem) -> str:
    catalog = _catalog_metadata(item)
    hints = [
        catalog.get("tool_hint"),
        catalog.get("tool_key"),
        item.metadata.get("tool_hint") if isinstance(item.metadata, Mapping) else None,
    ]
    return " ".join(str(hint).strip() for hint in hints if str(hint or "").strip())


def _routing_text(item: ValidationRequestQueueItem) -> str:
    request = item.validation_request
    item_required_inputs = []
    if isinstance(item.metadata, Mapping):
        raw_required_inputs = item.metadata.get("required_inputs") or []
        item_required_inputs = raw_required_inputs if isinstance(raw_required_inputs, list) else [raw_required_inputs]
    payload = [
        item.topic,
        item.title,
        item.objective,
        item.rationale,
        request.objective,
        request.candidate_name,
        request.target_name,
        " ".join(str(value) for value in item_required_inputs),
        json.dumps(item.metadata, sort_keys=True, default=str),
        json.dumps(request.metadata, sort_keys=True, default=str),
    ]
    if request.assay_context:
        payload.append(
            json.dumps(request.assay_context.model_dump(mode="json"), sort_keys=True, default=str)
        )
    return " ".join(str(value or "") for value in payload).casefold()


def _contains_any(text: str, terms: set[str]) -> bool:
    return any(re.search(rf"\b{re.escape(term)}\b", text) for term in terms)


def _specialist_rubric(agent_name: str) -> list[str]:
    return _SPECIALIST_RUBRICS.get(
        agent_name,
        [
            "Check evidence traceability, missing context, safety risks, and next-step readiness.",
            "Hold when the supplied context is insufficient for a conservative promotion decision.",
        ],
    )


def summarize_validation_agent_result(result: ValidationAgentResult) -> dict[str, Any]:
    return {
        "queue_item_id": str(result.queue_item_id),
        "task_type": result.task_type,
        "validation_type": result.validation_type,
        "decision": result.decision,
        "confidence": result.confidence,
        "missing_evidence_count": len(result.missing_evidence),
        "risk_count": len(result.risks),
        "next_action_count": len(result.next_actions),
    }


def _deterministic_validation_result(
    item: ValidationRequestQueueItem,
    *,
    agent_name: str,
    model_profile: str,
) -> ValidationAgentResult:
    request = item.validation_request
    metadata = item.metadata or {}
    evidence_refs = _string_list(metadata.get("evidence_refs")) or _string_list(
        (request.assay_context.evidence_refs if request.assay_context else [])
    )
    expected_outputs = _string_list(metadata.get("expected_outputs"))
    risks = _string_list(request.assay_context.negative_evidence_needs if request.assay_context else [])
    missing: list[str] = []
    if len(evidence_refs) < 2:
        missing.append("At least two source-traceable evidence refs should be reviewed before promotion.")
    if not request.assay_context:
        missing.append("Assay context is required before validation execution.")
    if not expected_outputs:
        missing.append("Expected outputs should be explicit before dispatch.")

    decision = "hold"
    confidence = 0.62
    if missing:
        confidence = 0.48
    elif item.task_type == "safety" and risks:
        decision = "hold"
        confidence = 0.7
    elif evidence_refs:
        decision = "promote"
        confidence = 0.72

    return ValidationAgentResult(
        queue_item_id=item.queue_item_id,
        plan_id=item.plan_id,
        task_id=item.task_id,
        task_type=item.task_type,
        validation_type=request.validation_type,
        agent_name=agent_name,
        model_profile=model_profile,
        decision=decision,
        confidence=confidence,
        summary=(
            f"{agent_name} reviewed {item.task_type} task '{item.title}'. "
            f"Decision: {decision}. This deterministic result is for local tests and fallback only."
        ),
        evidence_used=evidence_refs,
        missing_evidence=missing,
        risks=risks,
        next_actions=expected_outputs or ["Review the queue item manually before further promotion."],
    )


def _external_required_result(
    item: ValidationRequestQueueItem,
    *,
    agent_name: str,
    model_profile: str,
) -> ValidationAgentResult:
    return ValidationAgentResult(
        queue_item_id=item.queue_item_id,
        plan_id=item.plan_id,
        task_id=item.task_id,
        task_type=item.task_type,
        validation_type=item.validation_request.validation_type,
        agent_name=agent_name,
        model_profile=model_profile,
        decision="hold",
        confidence=0.0,
        summary="External model review is required. Use raw_response.prompt_payload with a human-operated LLM.",
        missing_evidence=["No model review was executed inside the system."],
        next_actions=["Run the prompt payload through the selected external reviewer and paste back the result."],
        raw_response={"prompt_payload": _review_payload(item, agent_name)},
    )


def _result_from_model(
    item: ValidationRequestQueueItem,
    review: Mapping[str, Any],
    *,
    agent_name: str,
    model_profile: str,
) -> ValidationAgentResult:
    payload = _load_json_object(str(review.get("text") or ""))
    decision = str(payload.get("decision") or "hold").strip().lower()
    if decision not in {"promote", "hold", "demote"}:
        decision = "hold"
    return ValidationAgentResult(
        queue_item_id=item.queue_item_id,
        plan_id=item.plan_id,
        task_id=item.task_id,
        task_type=item.task_type,
        validation_type=item.validation_request.validation_type,
        agent_name=agent_name,
        model_profile=model_profile,
        decision=decision,
        confidence=_float_between(payload.get("confidence"), default=0.5),
        summary=str(payload.get("summary") or "Validation review completed."),
        evidence_used=_string_list(payload.get("evidence_used")),
        missing_evidence=_string_list(payload.get("missing_evidence")),
        risks=_string_list(payload.get("risks")),
        next_actions=_string_list(payload.get("next_actions")),
        errors=_string_list(payload.get("errors")),
        raw_response={
            "provider_metadata": review.get("metadata", {}),
            "model_payload": payload,
        },
    )


def _review_payload(item: ValidationRequestQueueItem, agent_name: str) -> dict[str, Any]:
    request = item.validation_request
    return {
        "agent_name": agent_name,
        "specialist_rubric": _specialist_rubric(agent_name),
        "queue_item": {
            "queue_item_id": str(item.queue_item_id),
            "plan_id": str(item.plan_id),
            "task_id": str(item.task_id),
            "status": item.status,
            "topic": item.topic,
            "task_type": item.task_type,
            "title": item.title,
            "objective": item.objective,
            "rationale": item.rationale,
            "priority": item.priority,
            "quality_gates": item.quality_gates,
            "dispatch_blockers": item.dispatch_blockers,
            "metadata": item.metadata,
        },
        "validation_request": request.model_dump(mode="json"),
        "required_contract": {
            "decision": "promote|hold|demote",
            "confidence": "number between 0 and 1",
            "summary": "string",
            "evidence_used": ["citation IDs, evidence refs, or supplied queue evidence"],
            "missing_evidence": ["string"],
            "risks": ["string"],
            "next_actions": ["string"],
            "errors": ["string"],
        },
    }


def _openrouter_review_model(model_name: str, review_payload: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for live validation agent dispatch.")
    payload = {
        "model": model_name,
        "temperature": float(os.getenv("HSA_VALIDATION_AGENT_TEMPERATURE", "0.15")),
        "max_tokens": int(os.getenv("HSA_VALIDATION_AGENT_MAX_TOKENS", "4000")),
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "instructions": [
                            "Return JSON only.",
                            "Do not invent citations or experimental results.",
                            "Follow the specialist_rubric in the review payload exactly.",
                            "Make the decision conservative and explain what is still missing.",
                        ],
                        "review_payload": review_payload,
                    },
                    sort_keys=True,
                    default=str,
                ),
            },
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
            timeout=float(os.getenv("HSA_VALIDATION_AGENT_TIMEOUT_SECONDS", "150")),
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
        },
    }


def _model_name(model_profile: str) -> str:
    if "/" in model_profile or model_profile.startswith("~"):
        return model_profile
    return os.getenv("HSA_VALIDATION_AGENT_MODEL", default_openrouter_model())


def _load_json_object(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.I)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        cleaned = cleaned[start : end + 1]
    payload = json.loads(cleaned)
    if not isinstance(payload, dict):
        raise ValueError("model response JSON must be an object")
    return payload


def _float_between(value: Any, *, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return min(max(number, 0.0), 1.0)


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        value = [value]
    return [str(item).strip() for item in value if str(item).strip()]
