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


VALIDATION_AGENT_VERSION = "v1"
DEFAULT_VALIDATION_AGENT_MODEL = "~anthropic/claude-sonnet-latest"
VALIDATION_AGENT_MODEL_PROFILE = "openrouter_required"

_AGENT_NAMES = {
    "expert_review": "evidence_review_validation_agent",
    "literature_review": "evidence_review_validation_agent",
    "wet_lab": "assay_design_validation_agent",
    "target_validation": "assay_design_validation_agent",
    "omics": "omics_validation_agent",
    "safety": "safety_review_validation_agent",
    "admet": "safety_review_validation_agent",
}

_SYSTEM_PROMPT = """You are a validation review agent for a translational oncology research system.
You review one approved validation queue item at a time.
Use only the supplied queue item, validation request, evidence refs, risks, required inputs, expected outputs, and assay context.
Do not invent papers, trial results, experiments that were already completed, or clinical claims.
Return strict JSON only.
Your decision must be one of: promote, hold, demote.
Use promote only when the supplied evidence and context are sufficient to move the item to the next validation step.
Use hold when the idea is plausible but needs more evidence, protocol detail, safety context, or expert review.
Use demote when the supplied evidence has a major contradiction, missing core premise, or unacceptable risk for this stage."""


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
    return _AGENT_NAMES.get(str(item.task_type), "general_validation_review_agent")


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
    return os.getenv("HSA_VALIDATION_AGENT_MODEL", DEFAULT_VALIDATION_AGENT_MODEL)


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
