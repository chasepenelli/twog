"""Recommend-only validation planning from evaluated research briefs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import re
from typing import Any
from uuid import uuid4

from .contracts import (
    HypothesisDraft,
    ResearchBriefEvaluationRecord,
    ResearchBriefFinding,
    ResearchBriefRecord,
    ResearchBriefResult,
    ValidationAssayContext,
    ValidationPlanReadiness,
    ValidationPlanRecord,
    ValidationPlanRequest,
    ValidationPlanResult,
    ValidationPlanTask,
    ValidationPlanTaskType,
    ValidationRequest,
)
from .repository import ResearchRepository


VALIDATION_PLANNING_AGENT_NAME = "validation_planning_agent"
VALIDATION_PLANNING_AGENT_VERSION = "v1"

_READY_READINESS = "ready_for_hypothesis_review"
_EVIDENCE_STRENGTH_CONFIDENCE = {
    "high": 0.82,
    "medium": 0.62,
    "low": 0.38,
    "unknown": 0.25,
}
_TARGET_TERMS = {
    "angpt2": "ANGPT2",
    "egfr": "EGFR",
    "flt4": "FLT4",
    "kdr": "KDR",
    "met": "MET",
    "mtor": "MTOR",
    "pdgfrb": "PDGFRB",
    "pik3ca": "PIK3CA",
    "pik3cb": "PIK3CB",
    "pik3cd": "PIK3CD",
    "pik3cg": "PIK3CG",
    "vegf": "VEGF",
    "vegfa": "VEGFA",
    "vegfr": "VEGFR",
    "vegfr2": "KDR",
    "vegfr3": "FLT4",
}
_CANDIDATE_TERMS = {
    "dasatinib": "dasatinib",
    "doxorubicin": "doxorubicin",
    "pazopanib": "pazopanib",
    "propranolol": "propranolol",
    "rapamycin": "rapamycin",
    "sirolimus": "sirolimus",
    "sorafenib": "sorafenib",
    "toceranib": "toceranib",
    "trametinib": "trametinib",
}
_COMPOUND_TERMS = {
    "compound",
    "drug",
    "inhibitor",
    "ligand",
    "therapy",
    "treatment",
}
_STRUCTURE_TERMS = {"binding", "docking", "kinase", "ligand", "protein", "structure", "target"}
_SAFETY_TERMS = {"admet", "dose", "safety", "toxicity", "toxicology"}
_OMICS_TERMS = {"biomarker", "expression", "gene", "genomic", "omics", "rna", "transcriptomic"}
_TRANSLATION_TERMS = {"angiosarcoma", "canine", "comparative", "human", "species", "translation"}


def plan_validation_from_research_brief(
    repository: ResearchRepository,
    request: ValidationPlanRequest,
) -> ValidationPlanResult:
    """Build a source-traceable validation plan without launching work."""

    brief, evaluation = _select_brief_and_evaluation(repository, request)
    if brief is None:
        raise ValueError("No persisted research brief matched the validation planning request.")

    payload, parse_errors = _brief_payload(brief)
    evaluation_errors = list(evaluation.errors) if evaluation else []
    ready = _evaluation_is_ready(evaluation)
    errors = parse_errors + evaluation_errors

    evidence = {
        "brief_id": str(brief.brief_id),
        "evaluation_id": str(evaluation.evaluation_id) if evaluation else None,
        "evaluation_readiness": evaluation.readiness if evaluation else None,
        "evaluation_passes_quality_bar": evaluation.passes_quality_bar if evaluation else None,
        "evaluation_overall_score": evaluation.overall_score if evaluation else None,
        "citation_count": len(_as_list(payload.get("citations"))),
        "ranked_hypothesis_count": len(_as_list(payload.get("ranked_hypotheses"))),
        "unresolved_question_count": len(_as_list(payload.get("unresolved_questions"))),
        "recommend_only": True,
    }

    if request.require_ready_evaluation and not ready:
        block_errors = list(errors)
        if evaluation is None:
            block_errors.append("No research brief evaluation is available.")
        elif evaluation.readiness != _READY_READINESS or not evaluation.passes_quality_bar:
            block_errors.append(
                "Research brief evaluation is not ready for hypothesis review."
            )
        result = ValidationPlanResult(
            model_profile=request.model_profile,
            brief_id=brief.brief_id,
            evaluation_id=evaluation.evaluation_id if evaluation else None,
            topic=brief.topic,
            source_key=brief.source_key,
            status="blocked",
            readiness="needs_better_synthesis",
            tasks=[
                _review_task(
                    brief=brief,
                    evaluation=evaluation,
                    reason="Resolve synthesis quality gaps before promoting hypotheses into validation planning.",
                    priority=50,
                )
            ],
            evidence=evidence,
            errors=block_errors,
        )
        return result

    findings = _ranked_findings(payload)
    hypotheses = [_hypothesis_from_finding(brief, evaluation, finding) for finding in findings]
    tasks = _tasks_from_findings(
        brief=brief,
        evaluation=evaluation,
        findings=findings,
        max_tasks=request.max_tasks,
    )
    if not tasks:
        tasks = [
            _review_task(
                brief=brief,
                evaluation=evaluation,
                reason="Review the brief and select one hypothesis for the first validation path.",
                priority=80,
            )
        ]

    readiness: ValidationPlanReadiness = "ready_for_expert_review" if not errors else "needs_better_synthesis"
    status = "ready_for_review" if readiness == "ready_for_expert_review" else "draft"
    return ValidationPlanResult(
        model_profile=request.model_profile,
        brief_id=brief.brief_id,
        evaluation_id=evaluation.evaluation_id if evaluation else None,
        topic=brief.topic,
        source_key=brief.source_key,
        status=status,
        readiness=readiness,
        hypothesis_drafts=hypotheses,
        tasks=tasks,
        evidence=evidence,
        errors=errors,
    )


def summarize_validation_plan(result: ValidationPlanResult) -> dict[str, Any]:
    return {
        "plan_id": str(result.plan_id),
        "brief_id": str(result.brief_id),
        "evaluation_id": str(result.evaluation_id) if result.evaluation_id else None,
        "status": result.status,
        "readiness": result.readiness,
        "hypothesis_count": len(result.hypothesis_drafts),
        "task_count": len(result.tasks),
        "error_count": len(result.errors),
    }


def validation_plan_record_from_result(
    result: ValidationPlanResult,
    request: ValidationPlanRequest,
) -> ValidationPlanRecord:
    payload = result.model_dump(mode="json")
    return ValidationPlanRecord(
        plan_id=result.plan_id,
        agent_run_id=result.agent_run_id,
        brief_id=result.brief_id,
        evaluation_id=result.evaluation_id,
        topic=result.topic,
        source_key=result.source_key,
        model_profile=result.model_profile,
        status=result.status,
        readiness=result.readiness,
        task_count=len(result.tasks),
        hypothesis_count=len(result.hypothesis_drafts),
        summary=summarize_validation_plan(result),
        result_payload=payload,
        errors=result.errors,
        metadata=request.metadata,
    )


def _select_brief_and_evaluation(
    repository: ResearchRepository,
    request: ValidationPlanRequest,
) -> tuple[ResearchBriefRecord | None, ResearchBriefEvaluationRecord | None]:
    if request.evaluation_id:
        evaluation = repository.get_research_brief_evaluation(request.evaluation_id)
        if evaluation is None:
            return None, None
        return repository.get_research_brief(evaluation.brief_id), evaluation

    if request.brief_id:
        brief = repository.get_research_brief(request.brief_id)
        evaluation = _latest_evaluation(repository, request.brief_id)
        return brief, evaluation

    briefs = repository.list_research_briefs(
        status="completed",
        source_key=request.source_key,
        topic_query=request.topic_query,
        limit=1,
    )
    if not briefs:
        return None, None
    brief = briefs[0]
    return brief, _latest_evaluation(repository, brief.brief_id)


def _latest_evaluation(
    repository: ResearchRepository,
    brief_id,
) -> ResearchBriefEvaluationRecord | None:
    evaluations = repository.list_research_brief_evaluations(brief_id=brief_id, limit=1)
    return evaluations[0] if evaluations else None


def _evaluation_is_ready(evaluation: ResearchBriefEvaluationRecord | None) -> bool:
    return bool(
        evaluation
        and evaluation.passes_quality_bar
        and evaluation.readiness == _READY_READINESS
    )


def _brief_payload(brief: ResearchBriefRecord) -> tuple[dict[str, Any], list[str]]:
    payload = dict(brief.result_payload or {})
    payload.setdefault("brief_id", str(brief.brief_id))
    payload.setdefault("topic", brief.topic)
    payload.setdefault("disease_scope", brief.disease_scope)
    payload.setdefault("brief_style", brief.brief_style)
    payload.setdefault("model_profile", brief.model_profile)
    payload.setdefault("final_brief", brief.final_brief)
    payload.setdefault("perspective_reports", [])
    payload.setdefault("ranked_hypotheses", [])
    payload.setdefault("unresolved_questions", [])
    payload.setdefault("citations", [])
    payload.setdefault("evidence", {})
    payload.setdefault("errors", [])
    try:
        typed = ResearchBriefResult.model_validate(payload)
    except Exception as exc:
        return payload, [f"Could not validate stored research brief payload: {exc}"]
    return typed.model_dump(mode="json"), []


def _ranked_findings(payload: Mapping[str, Any]) -> list[ResearchBriefFinding]:
    findings: list[ResearchBriefFinding] = []
    for item in _as_list(payload.get("ranked_hypotheses")):
        if not isinstance(item, Mapping):
            continue
        try:
            findings.append(ResearchBriefFinding.model_validate(dict(item)))
        except Exception:
            continue
    return findings


def _hypothesis_from_finding(
    brief: ResearchBriefRecord,
    evaluation: ResearchBriefEvaluationRecord | None,
    finding: ResearchBriefFinding,
) -> HypothesisDraft:
    confidence = _EVIDENCE_STRENGTH_CONFIDENCE.get(finding.evidence_strength, 0.25)
    return HypothesisDraft(
        title=_title_from_claim(finding.claim),
        hypothesis=finding.claim,
        rationale=finding.reasoning,
        status="draft",
        confidence=confidence,
        proposed_by=VALIDATION_PLANNING_AGENT_NAME,
        metadata={
            "brief_id": str(brief.brief_id),
            "evaluation_id": str(evaluation.evaluation_id) if evaluation else None,
            "topic": brief.topic,
            "source_key": brief.source_key,
            "stance": finding.stance,
            "evidence_strength": finding.evidence_strength,
            "citations": finding.citations,
            "open_questions": finding.open_questions,
        },
    )


def _tasks_from_findings(
    *,
    brief: ResearchBriefRecord,
    evaluation: ResearchBriefEvaluationRecord | None,
    findings: Sequence[ResearchBriefFinding],
    max_tasks: int,
) -> list[ValidationPlanTask]:
    tasks: list[ValidationPlanTask] = []
    seen: set[tuple[str, str]] = set()
    for index, finding in enumerate(findings, start=1):
        text = f"{finding.claim} {finding.reasoning} {' '.join(finding.open_questions)}"
        target_name = _extract_known_term(text, _TARGET_TERMS)
        candidate_name = _extract_known_term(text, _CANDIDATE_TERMS)

        _append_task(
            tasks,
            seen,
            _task(
                task_type="expert_review",
                title=f"Expert review: {_title_from_claim(finding.claim)}",
                objective="Have a qualified scientific reviewer assess whether this hypothesis should enter validation.",
                rationale=finding.reasoning,
                brief=brief,
                evaluation=evaluation,
                finding=finding,
                priority=50 + index,
                validation_type="expert_review",
                candidate_name=candidate_name,
                target_name=target_name,
                tool_hint="human_review",
                required_inputs=["research brief", "citation set", "synthesis evaluation"],
                expected_outputs=["go/no-go decision", "validation scope", "known risks"],
            ),
            max_tasks,
        )
        if len(tasks) >= max_tasks:
            break

        if target_name or _contains_any(text, _STRUCTURE_TERMS):
            _append_task(
                tasks,
                seen,
                _task(
                    task_type="target_validation",
                    title=f"Target validation: {target_name or 'priority target'}",
                    objective="Check species conservation, disease relevance, and available structural context.",
                    rationale=finding.reasoning,
                    brief=brief,
                    evaluation=evaluation,
                    finding=finding,
                    priority=100 + index,
                    validation_type="homology",
                    target_name=target_name,
                    tool_hint="uniprot_rcsb_homology",
                    required_inputs=["target identifier", "canine/human sequence evidence", "citation set"],
                    expected_outputs=["orthology assessment", "structure availability", "translation risk flags"],
                ),
                max_tasks,
            )
        if len(tasks) >= max_tasks:
            break

        if candidate_name or _contains_any(text, _COMPOUND_TERMS):
            _append_task(
                tasks,
                seen,
                _task(
                    task_type="docking",
                    title=f"Docking screen: {candidate_name or 'candidate compound'}",
                    objective="Define a bounded docking or binding plausibility screen before heavier GPU work.",
                    rationale=finding.reasoning,
                    brief=brief,
                    evaluation=evaluation,
                    finding=finding,
                    priority=150 + index,
                    validation_type="docking",
                    candidate_name=candidate_name,
                    target_name=target_name,
                    tool_hint="docking_or_boltz_lane",
                    required_inputs=["candidate identity", "target structure or model", "binding-site rationale"],
                    expected_outputs=["ranked pose or binding plausibility report", "artifact links"],
                ),
                max_tasks,
            )
        if len(tasks) >= max_tasks:
            break

        if candidate_name or _contains_any(text, _SAFETY_TERMS):
            _append_task(
                tasks,
                seen,
                _task(
                    task_type="safety",
                    title=f"Safety screen: {candidate_name or 'candidate'}",
                    objective="Check canine safety, dosing constraints, and known adverse-event evidence.",
                    rationale=finding.reasoning,
                    brief=brief,
                    evaluation=evaluation,
                    finding=finding,
                    priority=200 + index,
                    validation_type="safety",
                    candidate_name=candidate_name,
                    target_name=target_name,
                    tool_hint="openfda_pubchem_admet",
                    required_inputs=["candidate identity", "species context", "adverse-event and ADMET records"],
                    expected_outputs=["safety risk flags", "dose-context gaps", "go/no-go constraints"],
                ),
                max_tasks,
            )
        if len(tasks) >= max_tasks:
            break

        if _contains_any(text, _OMICS_TERMS):
            _append_task(
                tasks,
                seen,
                _task(
                    task_type="omics",
                    title="Omics support check",
                    objective="Check whether expression or biomarker evidence supports the hypothesis.",
                    rationale=finding.reasoning,
                    brief=brief,
                    evaluation=evaluation,
                    finding=finding,
                    priority=250 + index,
                    validation_type=None,
                    candidate_name=candidate_name,
                    target_name=target_name,
                    tool_hint="geo_sra_expression_review",
                    required_inputs=["gene or biomarker terms", "canine HSA datasets", "human angiosarcoma datasets"],
                    expected_outputs=["dataset support", "species translation notes", "negative-evidence gaps"],
                ),
                max_tasks,
            )
        if len(tasks) >= max_tasks:
            break

        if _contains_any(text, _TRANSLATION_TERMS):
            _append_task(
                tasks,
                seen,
                _task(
                    task_type="partner_review",
                    title="Translational review",
                    objective="Assess whether the human/canine translational bridge is strong enough to prioritize.",
                    rationale=finding.reasoning,
                    brief=brief,
                    evaluation=evaluation,
                    finding=finding,
                    priority=300 + index,
                    validation_type="expert_review",
                    candidate_name=candidate_name,
                    target_name=target_name,
                    tool_hint="vet_partner_review",
                    required_inputs=["canine evidence", "human evidence", "clinical feasibility notes"],
                    expected_outputs=["translation risk rating", "partner questions", "next evidence target"],
                ),
                max_tasks,
            )
        if len(tasks) >= max_tasks:
            break

    return tasks


def _task(
    *,
    task_type: ValidationPlanTaskType,
    title: str,
    objective: str,
    rationale: str,
    brief: ResearchBriefRecord,
    evaluation: ResearchBriefEvaluationRecord | None,
    finding: ResearchBriefFinding,
    priority: int,
    validation_type: str | None,
    candidate_name: str | None = None,
    target_name: str | None = None,
    tool_hint: str | None = None,
    required_inputs: list[str] | None = None,
    expected_outputs: list[str] | None = None,
) -> ValidationPlanTask:
    evidence_refs = _evidence_refs(brief, evaluation, finding)
    assay_context = _validation_assay_context(
        brief=brief,
        finding=finding,
        task_type=task_type,
        validation_type=validation_type,
        evidence_refs=evidence_refs,
    )
    quality_gates = _validation_quality_gates_for_task(
        task_type=task_type,
        validation_type=validation_type,
    )
    validation_request = None
    if validation_type:
        validation_request = ValidationRequest(
            validation_type=validation_type,  # type: ignore[arg-type]
            candidate_name=candidate_name,
            target_name=target_name,
            objective=objective,
            priority=priority,
            require_approval=True,
            assay_context=assay_context,
            quality_gates=quality_gates,
            metadata={
                "brief_id": str(brief.brief_id),
                "evaluation_id": str(evaluation.evaluation_id) if evaluation else None,
                "citation_refs": finding.citations,
                "recommend_only": True,
            },
        )
    return ValidationPlanTask(
        task_type=task_type,
        title=title,
        objective=objective,
        rationale=rationale,
        validation_request=validation_request,
        required_inputs=required_inputs or [],
        expected_outputs=expected_outputs or [],
        evidence_refs=evidence_refs,
        priority=priority,
        requires_human_approval=True,
        tool_hint=tool_hint,
        metadata={
            "stance": finding.stance,
            "evidence_strength": finding.evidence_strength,
            "open_questions": finding.open_questions,
        },
    )


def _review_task(
    *,
    brief: ResearchBriefRecord,
    evaluation: ResearchBriefEvaluationRecord | None,
    reason: str,
    priority: int,
) -> ValidationPlanTask:
    finding = ResearchBriefFinding(
        claim=brief.topic,
        stance="uncertain",
        citations=["brief"],
        evidence_strength="unknown",
        reasoning=reason,
    )
    return _task(
        task_type="expert_review",
        title="Synthesis review before validation",
        objective="Resolve synthesis readiness before promoting a validation plan.",
        rationale=reason,
        brief=brief,
        evaluation=evaluation,
        finding=finding,
        priority=priority,
        validation_type="expert_review",
        tool_hint="human_review",
        required_inputs=["persisted research brief", "synthesis evaluation"],
        expected_outputs=["ready/not-ready decision", "required synthesis fixes"],
    )


def _append_task(
    tasks: list[ValidationPlanTask],
    seen: set[tuple[str, str]],
    task: ValidationPlanTask,
    max_tasks: int,
) -> None:
    if len(tasks) >= max_tasks:
        return
    key = (task.task_type, task.title.lower())
    if key in seen:
        return
    tasks.append(task)
    seen.add(key)


def _evidence_refs(
    brief: ResearchBriefRecord,
    evaluation: ResearchBriefEvaluationRecord | None,
    finding: ResearchBriefFinding,
) -> list[str]:
    refs = [f"brief:{brief.brief_id}"]
    if evaluation:
        refs.append(f"evaluation:{evaluation.evaluation_id}")
    refs.extend(finding.citations)
    return refs


def _validation_assay_context(
    *,
    brief: ResearchBriefRecord,
    finding: ResearchBriefFinding,
    task_type: ValidationPlanTaskType,
    validation_type: str | None,
    evidence_refs: list[str],
) -> ValidationAssayContext:
    combined_text = " ".join(
        [
            brief.topic,
            brief.disease_scope,
            brief.final_brief,
            finding.claim,
            finding.reasoning,
            " ".join(finding.open_questions),
        ]
    ).lower()
    species: list[str] = []
    if any(term in combined_text for term in ("canine", "dog", "dogs")):
        species.append("canine")
    if any(term in combined_text for term in ("human", "angiosarcoma")):
        species.append("human")
    if not species:
        species = ["canine", "human"]

    assay_type = _assay_type_for_task(task_type, validation_type)
    model_system = _model_system_for_task(task_type, validation_type)
    safety_context = None
    if validation_type in {"admet", "safety"} or task_type in {"admet", "safety", "wet_lab"}:
        safety_context = "Canine translational safety, adverse-event, dose-context, and ADMET relevance."

    return ValidationAssayContext(
        disease_context=brief.disease_scope,
        species=species,
        model_system=model_system,
        assay_type=assay_type,
        readout=_readout_for_task(task_type, validation_type),
        endpoint=_endpoint_for_task(task_type, validation_type),
        comparator_or_control="Require explicit comparator, negative control, or baseline literature comparator before execution.",
        sample_context="Source-traceable literature context until a partner supplies assay samples or datasets.",
        dose_or_exposure_context="Record dose, exposure, and timepoint before wet-lab, ADMET, or safety execution.",
        safety_context=safety_context,
        evidence_refs=evidence_refs,
        negative_evidence_needs=[
            "Search for negative, null, or contradictory findings before execution.",
            "Check species mismatch and translational relevance before claiming validation.",
        ],
        provenance_requirements=[
            "Preserve source citations and brief/evaluation identifiers.",
            "Attach assay protocol, model version, and parameter provenance before execution.",
        ],
    )


def _validation_quality_gates_for_task(
    *,
    task_type: ValidationPlanTaskType,
    validation_type: str | None,
) -> list[str]:
    gates = ["approval_required", "source_traceability_required", "negative_evidence_check_required"]
    if validation_type and validation_type != "expert_review":
        gates.extend(["assay_context_required", "species_context_required", "disease_context_required"])
    if task_type in {"docking", "boltz", "md", "protein_structure"}:
        gates.extend(["target_identity_required", "structure_or_sequence_context_required"])
    if task_type in {"compound_screen", "docking", "admet", "safety"}:
        gates.append("candidate_identity_required")
    if task_type in {"admet", "safety", "wet_lab"}:
        gates.append("safety_context_required")
    return _dedupe_labels(gates)


def _assay_type_for_task(task_type: ValidationPlanTaskType, validation_type: str | None) -> str:
    if validation_type == "expert_review" or task_type in {"expert_review", "partner_review", "literature_review"}:
        return "structured expert evidence review"
    if task_type in {"docking", "boltz", "md", "protein_structure"}:
        return "in silico structural validation"
    if task_type in {"compound_screen", "admet", "safety"}:
        return "candidate bioactivity or safety triage"
    if task_type == "omics":
        return "omics evidence review"
    if task_type == "wet_lab":
        return "wet-lab assay design review"
    return "target validation review"


def _model_system_for_task(task_type: ValidationPlanTaskType, validation_type: str | None) -> str:
    if validation_type == "expert_review" or task_type in {"expert_review", "partner_review", "literature_review"}:
        return "Human-reviewed literature and translational evidence packet."
    if task_type in {"docking", "boltz", "md", "protein_structure"}:
        return "Computational target or structure model with explicit source provenance."
    if task_type in {"compound_screen", "admet", "safety"}:
        return "Candidate-centered translational safety or activity screen."
    if task_type == "omics":
        return "Comparative canine and human molecular dataset review."
    return "Source-traceable validation planning context."


def _readout_for_task(task_type: ValidationPlanTaskType, validation_type: str | None) -> str:
    if validation_type == "expert_review" or task_type in {"expert_review", "partner_review", "literature_review"}:
        return "ready/not-ready decision with cited gaps and next validation lane."
    if task_type in {"docking", "boltz", "md", "protein_structure"}:
        return "model confidence, binding or structural rationale, and failure modes."
    if task_type in {"compound_screen", "admet", "safety"}:
        return "activity, toxicity, exposure, and translational risk flags."
    if task_type == "omics":
        return "species-conserved signal, expression context, and dataset caveats."
    return "go/no-go validation recommendation."


def _endpoint_for_task(task_type: ValidationPlanTaskType, validation_type: str | None) -> str:
    if validation_type == "expert_review" or task_type in {"expert_review", "partner_review", "literature_review"}:
        return "expert validation readiness."
    if task_type in {"docking", "boltz", "md", "protein_structure"}:
        return "computational plausibility and structural prioritization."
    if task_type in {"compound_screen", "admet", "safety"}:
        return "candidate prioritization and safety gating."
    if task_type == "omics":
        return "translational molecular support."
    return "validation path selection."


def _dedupe_labels(labels: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for label in labels:
        normalized = str(label).strip()
        key = normalized.casefold()
        if normalized and key not in seen:
            deduped.append(normalized)
            seen.add(key)
    return deduped


def _extract_known_term(text: str, terms: Mapping[str, str]) -> str | None:
    normalized = text.lower()
    for term, canonical in sorted(terms.items(), key=lambda item: (-len(item[0]), item[0])):
        if re.search(rf"\b{re.escape(term)}\b", normalized):
            return canonical
    return None


def _contains_any(text: str, terms: set[str]) -> bool:
    normalized = text.lower()
    return any(re.search(rf"\b{re.escape(term)}\b", normalized) for term in terms)


def _title_from_claim(claim: str) -> str:
    compact = re.sub(r"\s+", " ", claim).strip()
    if len(compact) <= 110:
        return compact
    return compact[:107].rstrip() + "..."


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []
