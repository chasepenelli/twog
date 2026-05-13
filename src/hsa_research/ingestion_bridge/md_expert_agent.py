"""MD expert review agent for approval-first RunPod compute packets."""

from __future__ import annotations

import json
import os
from typing import Any
import urllib.error
import urllib.request

from .contracts import MDExpertAgentReviewResult, MDExpertReviewPacketRecord
from .model_policy import default_openrouter_model


MD_EXPERT_REVIEW_AGENT_NAME = "md_expert_review_agent"
MD_EXPERT_REVIEW_AGENT_VERSION = "v1"
DEFAULT_MD_EXPERT_REVIEW_MODEL = default_openrouter_model()

_SYSTEM_PROMPT = """You are an MD/docking/structure-computation expert agent.
You review one TWOG MD expert review packet before any live RunPod MD job is submitted.
Your job is not to judge therapeutic efficacy. Your job is to decide whether this exact packet is suitable for one smoke-scale worker-contract test.
Use only the supplied packet, input schema, worker history, cost bounds, and checklist.
Return strict JSON only.

Decision rules:
- approved: the protein PDB, ligand SMILES, provenance, preparation method, simulation settings, endpoint, expected outputs, and safety/cost bounds are adequate for exactly one small smoke test.
- needs_changes: the packet is plausible but missing fields, provenance, preparation detail, contract detail, or worker artifact expectations.
- rejected: the packet is scientifically or operationally unsuitable for the smoke test.

If decision is approved, required_changes must be empty.
Do not invent protein preparation, ligand preparation, docking, MD, or clinical results.
Do not approve if the packet is only a placeholder or lacks protein/ligand provenance."""


def run_md_expert_review_agent(
    packet: MDExpertReviewPacketRecord,
    *,
    model_profile: str = "openrouter_required",
) -> MDExpertAgentReviewResult:
    """Review an MD packet with OpenRouter or deterministic test fallback."""

    if model_profile == "deterministic_only":
        return _deterministic_review(packet, model_profile=model_profile)
    review = _openrouter_review_model(_model_name(model_profile), _review_payload(packet))
    result = _result_from_model(packet, review, model_profile=model_profile)
    return result


def summarize_md_expert_review(result: MDExpertAgentReviewResult) -> dict[str, Any]:
    return {
        "packet_id": str(result.packet_id),
        "packet_hash": result.packet_hash,
        "decision": result.decision,
        "confidence": result.confidence,
        "required_change_count": len(result.required_changes),
        "risk_flag_count": len(result.risk_flags),
        "model_profile": result.model_profile,
        "approval_id": str(result.approval_record.approval_id) if result.approval_record else None,
    }


def _deterministic_review(
    packet: MDExpertReviewPacketRecord,
    *,
    model_profile: str,
) -> MDExpertAgentReviewResult:
    missing: list[str] = []
    if not packet.input_packet.protein_source:
        missing.append("protein_source is required.")
    if not packet.input_packet.ligand_source:
        missing.append("ligand_source is required.")
    if not packet.input_packet.preparation_method:
        missing.append("preparation_method is required.")
    if packet.input_packet.simulation_steps > 1000:
        missing.append("simulation_steps exceeds first smoke-test bounds.")
    if not packet.safety_cost_bounds:
        missing.append("safety_cost_bounds are required.")
    decision = "approved" if not missing else "needs_changes"
    return MDExpertAgentReviewResult(
        packet_id=packet.packet_id,
        packet_hash=packet.packet_hash,
        decision=decision,
        confidence=0.7 if decision == "approved" else 0.5,
        summary=(
            "Deterministic MD packet review found the smoke-test contract complete."
            if decision == "approved"
            else "Deterministic MD packet review found missing smoke-test contract fields."
        ),
        rationale="Deterministic fallback checks only required local fields and smoke-test bounds.",
        required_changes=missing,
        checklist_assessment=["Deterministic fallback; use OpenRouter for real MD expert review."],
        risk_flags=[] if decision == "approved" else ["local_fallback_not_sufficient_for_live_science_review"],
        model_profile=model_profile,
        raw_response={"mode": "deterministic_only"},
    )


def _result_from_model(
    packet: MDExpertReviewPacketRecord,
    review: dict[str, Any],
    *,
    model_profile: str,
) -> MDExpertAgentReviewResult:
    payload = _load_json_object(str(review.get("text") or ""))
    decision = str(payload.get("decision") or "").strip()
    if decision not in {"approved", "needs_changes", "rejected"}:
        raise RuntimeError("MD expert agent response is missing a valid decision.")
    return MDExpertAgentReviewResult(
        packet_id=packet.packet_id,
        packet_hash=packet.packet_hash,
        decision=decision,  # type: ignore[arg-type]
        confidence=float(payload.get("confidence") or 0.0),
        summary=str(payload.get("summary") or "").strip(),
        rationale=str(payload.get("rationale") or "").strip(),
        required_changes=_string_list(payload.get("required_changes")),
        checklist_assessment=_string_list(payload.get("checklist_assessment")),
        risk_flags=_string_list(payload.get("risk_flags")),
        model_profile=model_profile,
        raw_response={
            "model_review": payload,
            "provider_metadata": review.get("metadata", {}),
        },
        metadata={
            "selected_model": (review.get("metadata") or {}).get("model_name"),
            "requested_model": (review.get("metadata") or {}).get("requested_model"),
        },
    )


def _review_payload(packet: MDExpertReviewPacketRecord) -> dict[str, Any]:
    return {
        "packet": packet.model_dump(mode="json"),
        "review_contract": {
            "decision": "approved|needs_changes|rejected",
            "confidence": "number between 0 and 1",
            "summary": "string",
            "rationale": "string",
            "required_changes": ["string"],
            "checklist_assessment": ["string"],
            "risk_flags": ["string"],
        },
        "approval_constraint": "Approve only for exactly one smoke-scale worker-contract test.",
    }


def _openrouter_review_model(model_name: str, review_payload: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for MD expert agent review.")
    payload = {
        "model": model_name,
        "temperature": float(os.getenv("HSA_MD_EXPERT_AGENT_TEMPERATURE", "0.1")),
        "max_tokens": int(os.getenv("HSA_MD_EXPERT_AGENT_MAX_TOKENS", "3500")),
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "instructions": [
                            "Return JSON only.",
                            "Review the packet as a compute smoke-test approval gate.",
                            "Do not invent input preparation details or scientific results.",
                            "If any required input or provenance is missing, choose needs_changes or rejected.",
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
            timeout=float(os.getenv("HSA_MD_EXPERT_AGENT_TIMEOUT_SECONDS", "150")),
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
    return os.getenv("HSA_MD_EXPERT_AGENT_MODEL", DEFAULT_MD_EXPERT_REVIEW_MODEL)


def _load_json_object(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:].strip()
    try:
        loaded = json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        loaded = json.loads(cleaned[start : end + 1])
    if not isinstance(loaded, dict):
        raise RuntimeError("OpenRouter MD expert response must be a JSON object.")
    return loaded


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []
