"""Recommend-only validation tool catalog.

This module intentionally stays isolated from service, Dagster, MCP, and CLI
registration. It describes validation lanes that can be recommended by planners
while reusing the existing validation request and task contracts.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import re
from typing import Any, Literal

from .contracts import (
    StrictBaseModel,
    ToolMode,
    ValidationAssayContext,
    ValidationToolCapability,
    ValidationToolCatalogRequest,
    ValidationToolCatalogResult,
    ValidationToolMatch,
    ValidationToolMatchRequest,
    ValidationToolMatchResult,
    ValidationPlanTask,
    ValidationPlanTaskType,
    ValidationRequest,
)


VALIDATION_TOOL_CATALOG_VERSION = "v1"

ValidationToolKey = Literal[
    "expert_review",
    "assay_design_review",
    "target_expression_review",
    "biomarker_response_assay_design",
    "omics_expression_review",
    "mutation_function_review",
    "peptide_specialist_review",
    "safety_translational_risk_review",
]
ValidationToolRunnerStatus = Literal["recommend_only"]
ValidationToolValidationType = Literal[
    "boltz",
    "docking",
    "md",
    "admet",
    "homology",
    "safety",
    "expert_review",
    "wet_lab",
    "omics",
]


class ValidationToolCatalogEntry(StrictBaseModel):
    tool_key: ValidationToolKey
    display_name: str
    description: str
    task_type: ValidationPlanTaskType
    validation_type: ValidationToolValidationType
    tool_hint: str
    recommended_agent_name: str
    mode: ToolMode = ToolMode.DRAFT
    runner_status: ValidationToolRunnerStatus = "recommend_only"
    default_objective: str
    assay_context_template: ValidationAssayContext
    quality_gates: list[str]
    required_inputs: list[str]
    expected_outputs: list[str]

    def as_validation_request(
        self,
        *,
        objective: str | None = None,
        candidate_name: str | None = None,
        target_name: str | None = None,
        priority: int = 100,
        evidence_refs: Iterable[str] | None = None,
        assay_context_overrides: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> ValidationRequest:
        objective_text = (objective or self.default_objective).strip()
        return ValidationRequest(
            validation_type=self.validation_type,
            candidate_name=_blank_to_none(candidate_name),
            target_name=_blank_to_none(target_name),
            objective=objective_text,
            priority=priority,
            require_approval=True,
            assay_context=self.assay_context(
                evidence_refs=evidence_refs,
                overrides=assay_context_overrides,
            ),
            quality_gates=list(self.quality_gates),
            metadata=_metadata_payload(self, metadata),
        )

    def as_plan_task(
        self,
        *,
        title: str | None = None,
        objective: str | None = None,
        rationale: str,
        candidate_name: str | None = None,
        target_name: str | None = None,
        priority: int = 100,
        evidence_refs: Iterable[str] | None = None,
        assay_context_overrides: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> ValidationPlanTask:
        objective_text = (objective or self.default_objective).strip()
        refs = _dedupe_strings(evidence_refs or [])
        merged_metadata = _metadata_payload(self, metadata)
        return ValidationPlanTask(
            task_type=self.task_type,
            title=(title or self.display_name).strip(),
            objective=objective_text,
            rationale=rationale.strip(),
            validation_request=self.as_validation_request(
                objective=objective_text,
                candidate_name=candidate_name,
                target_name=target_name,
                priority=priority,
                evidence_refs=refs,
                assay_context_overrides=assay_context_overrides,
                metadata=metadata,
            ),
            required_inputs=list(self.required_inputs),
            expected_outputs=list(self.expected_outputs),
            evidence_refs=refs,
            priority=priority,
            requires_human_approval=True,
            tool_hint=self.tool_hint,
            metadata=merged_metadata,
        )

    def assay_context(
        self,
        *,
        evidence_refs: Iterable[str] | None = None,
        overrides: Mapping[str, Any] | None = None,
    ) -> ValidationAssayContext:
        payload = self.assay_context_template.model_dump(mode="python")
        override_refs: Iterable[str] = []
        if overrides:
            for key, value in overrides.items():
                if key == "evidence_refs":
                    override_refs = value or []
                    continue
                payload[key] = value
        payload["evidence_refs"] = _dedupe_strings(
            [
                *payload.get("evidence_refs", []),
                *override_refs,
                *(evidence_refs or []),
            ]
        )
        return ValidationAssayContext.model_validate(payload)


def list_validation_tool_catalog() -> list[ValidationToolCatalogEntry]:
    """Return copies of the catalog entries so callers cannot mutate constants."""

    return [entry.model_copy(deep=True) for entry in VALIDATION_TOOL_CATALOG]


def build_validation_tool_catalog_report(
    request: ValidationToolCatalogRequest | None = None,
) -> ValidationToolCatalogResult:
    """Return the first-class typed catalog report used by service surfaces."""

    request = request or ValidationToolCatalogRequest()
    tools = [_capability_from_entry(entry) for entry in VALIDATION_TOOL_CATALOG]
    if request.category:
        tools = [tool for tool in tools if tool.category == request.category]
    if request.runner_status:
        tools = [tool for tool in tools if tool.runner_status == request.runner_status]
    if request.validation_type:
        tools = [
            tool
            for tool in tools
            if request.validation_type in tool.compatible_validation_types
        ]
    if request.task_type:
        tools = [
            tool
            for tool in tools
            if request.task_type in tool.compatible_task_types
        ]
    if request.query:
        normalized = request.query.casefold()
        tools = [
            tool
            for tool in tools
            if normalized in tool.tool_key.casefold()
            or normalized in tool.display_name.casefold()
            or normalized in tool.description.casefold()
            or any(normalized in item.casefold() for item in [*tool.required_inputs, *tool.outputs])
        ]
    tools = tools[: request.limit]
    return ValidationToolCatalogResult(
        tool_count=len(tools),
        runner_status_counts=_count_by(tools, "runner_status"),
        category_counts=_count_by(tools, "category"),
        tools=tools,
    )


def match_validation_tools(request: ValidationToolMatchRequest) -> ValidationToolMatchResult:
    """Rank recommend-only catalog tools for a validation planning request."""

    candidates = build_validation_tool_catalog_report(
        ValidationToolCatalogRequest(
            runner_status=request.runner_status,
            validation_type=request.validation_type,
            task_type=request.task_type,
            limit=50,
        )
    ).tools
    matches: list[ValidationToolMatch] = []
    context = " ".join(
        value
        for value in [
            request.objective,
            request.candidate_name,
            request.target_name,
            " ".join(request.species),
            " ".join(request.required_inputs),
        ]
        if value
    ).casefold()
    supplied_inputs = {item.casefold() for item in request.required_inputs}
    for tool in candidates:
        score = 0.12
        reasons: list[str] = []
        if request.validation_type and request.validation_type in tool.compatible_validation_types:
            score += 0.34
            reasons.append(f"matches validation_type={request.validation_type}")
        if request.task_type and request.task_type in tool.compatible_task_types:
            score += 0.28
            reasons.append(f"matches task_type={request.task_type}")
        keyword_hits = [
            token
            for token in _tool_keywords(tool)
            if token and token in context
        ]
        if keyword_hits:
            score += min(0.2, 0.04 * len(set(keyword_hits)))
            reasons.append("matches objective/context keywords")
        missing_inputs = [
            item
            for item in tool.required_inputs
            if item.casefold() not in supplied_inputs
        ]
        if missing_inputs:
            score -= min(0.1, 0.02 * len(missing_inputs))
        matches.append(
            ValidationToolMatch(
                tool=tool,
                score=max(0.0, min(round(score, 3), 1.0)),
                reasons=reasons or ["general validation catalog fallback"],
                missing_inputs=missing_inputs,
                dispatch_blockers=tool.dispatch_blockers,
            )
        )
    matches.sort(key=lambda match: match.score, reverse=True)
    matches = matches[: request.limit]
    return ValidationToolMatchResult(match_count=len(matches), matches=matches)


def get_validation_tool(tool_key: str) -> ValidationToolCatalogEntry:
    for entry in VALIDATION_TOOL_CATALOG:
        if entry.tool_key == tool_key:
            return entry.model_copy(deep=True)
    raise KeyError(f"Unknown validation tool: {tool_key}")


def build_validation_tool_task(
    tool_key: str,
    *,
    title: str | None = None,
    objective: str | None = None,
    rationale: str,
    candidate_name: str | None = None,
    target_name: str | None = None,
    priority: int = 100,
    evidence_refs: Iterable[str] | None = None,
    assay_context_overrides: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> ValidationPlanTask:
    """Build an existing ValidationPlanTask contract from a catalog entry."""

    return get_validation_tool(tool_key).as_plan_task(
        title=title,
        objective=objective,
        rationale=rationale,
        candidate_name=candidate_name,
        target_name=target_name,
        priority=priority,
        evidence_refs=evidence_refs,
        assay_context_overrides=assay_context_overrides,
        metadata=metadata,
    )


def _dedupe_strings(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        key = normalized.casefold()
        if not normalized or key in seen:
            continue
        deduped.append(normalized)
        seen.add(key)
    return deduped


def _capability_from_entry(entry: ValidationToolCatalogEntry) -> ValidationToolCapability:
    return ValidationToolCapability(
        tool_key=entry.tool_key,
        display_name=entry.display_name,
        category=_category_for_tool(entry.tool_key),
        description=entry.description,
        compatible_validation_types=[entry.validation_type],
        compatible_task_types=[entry.task_type],
        required_inputs=entry.required_inputs,
        optional_inputs=["operator notes", "additional cited evidence", "negative evidence"],
        outputs=entry.expected_outputs,
        artifacts=["structured recommendation", "evidence gap list"],
        quality_gates=entry.quality_gates,
        dispatch_blockers=["recommend_only_runner", *entry.quality_gates[:1]],
        estimated_runtime_minutes=_estimated_runtime(entry.tool_key),
        estimated_cost_usd=0.0,
        compute_profile=_compute_profile_for_tool(entry.tool_key),
        runner_status=entry.runner_status,
        sop_path=f"docs/sops/{entry.tool_key}.md",
        tool_hint=entry.tool_hint,
        metadata={
            "catalog_version": VALIDATION_TOOL_CATALOG_VERSION,
            "recommended_agent_name": entry.recommended_agent_name,
            "mode": _mode_value(entry.mode),
            "validation_type": entry.validation_type,
            "task_type": entry.task_type,
        },
    )


def _category_for_tool(tool_key: str) -> str:
    return {
        "expert_review": "expert_review",
        "assay_design_review": "assay_design",
        "target_expression_review": "target_expression",
        "biomarker_response_assay_design": "biomarker_response",
        "omics_expression_review": "omics_expression",
        "mutation_function_review": "mutation_function",
        "peptide_specialist_review": "peptide_specialist",
        "safety_translational_risk_review": "safety_translational_risk",
    }.get(tool_key, "expert_review")


def _compute_profile_for_tool(tool_key: str) -> str:
    if tool_key in {"assay_design_review", "biomarker_response_assay_design"}:
        return "wet_lab_planning"
    if tool_key in {"omics_expression_review", "target_expression_review"}:
        return "local_cpu"
    if tool_key == "peptide_specialist_review":
        return "llm_review"
    return "manual_review"


def _estimated_runtime(tool_key: str) -> int:
    if tool_key in {"assay_design_review", "biomarker_response_assay_design"}:
        return 90
    if tool_key in {"omics_expression_review", "target_expression_review"}:
        return 60
    return 45


def _tool_keywords(tool: ValidationToolCapability) -> list[str]:
    text = " ".join(
        [
            tool.tool_key,
            tool.display_name,
            tool.description,
            tool.category,
            tool.tool_hint,
            " ".join(tool.compatible_validation_types),
            " ".join(tool.compatible_task_types),
            " ".join(tool.required_inputs),
            " ".join(tool.outputs),
        ]
    ).casefold()
    return [
        token
        for token in re.findall(r"[a-z0-9]+", text)
        if len(token) >= 4
    ]


def _count_by(tools: list[ValidationToolCapability], field_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for tool in tools:
        value = str(getattr(tool, field_name))
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _entry(
    *,
    tool_key: ValidationToolKey,
    display_name: str,
    description: str,
    task_type: ValidationPlanTaskType,
    validation_type: ValidationToolValidationType,
    tool_hint: str,
    recommended_agent_name: str,
    default_objective: str,
    assay_type: str,
    readout: str,
    endpoint: str,
    model_system: str,
    quality_gates: list[str],
    required_inputs: list[str],
    expected_outputs: list[str],
    safety_context: str | None = None,
    provenance_requirements: list[str] | None = None,
    negative_evidence_needs: list[str] | None = None,
) -> ValidationToolCatalogEntry:
    return ValidationToolCatalogEntry(
        tool_key=tool_key,
        display_name=display_name,
        description=description,
        task_type=task_type,
        validation_type=validation_type,
        tool_hint=tool_hint,
        recommended_agent_name=recommended_agent_name,
        default_objective=default_objective,
        assay_context_template=ValidationAssayContext(
            disease_context="canine hemangiosarcoma with human angiosarcoma comparator",
            species=["canine", "human"],
            model_system=model_system,
            assay_type=assay_type,
            readout=readout,
            endpoint=endpoint,
            safety_context=safety_context,
            provenance_requirements=provenance_requirements or [
                "source traceability",
                "negative evidence review",
                "species scope preserved",
            ],
            negative_evidence_needs=negative_evidence_needs or [],
        ),
        quality_gates=_dedupe_strings(
            [
                "human_approval_required",
                "source_traceability_required",
                *quality_gates,
            ]
        ),
        required_inputs=required_inputs,
        expected_outputs=expected_outputs,
    )


VALIDATION_TOOL_CATALOG: tuple[ValidationToolCatalogEntry, ...] = (
    _entry(
        tool_key="expert_review",
        display_name="Expert review",
        description="Qualified scientific review of cited support, contradictions, and promotion criteria.",
        task_type="expert_review",
        validation_type="expert_review",
        tool_hint="expert_review",
        recommended_agent_name="evidence_review_validation_agent",
        default_objective=(
            "Review the cited evidence, limitations, contradictions, and promotion criteria before validation."
        ),
        assay_type="expert evidence review",
        readout="go/no-go validation readiness, missing evidence, and contradiction map",
        endpoint="approved validation scope or demotion/follow-up decision",
        model_system="source-traceable literature and committee evidence packet",
        quality_gates=["contradiction_review_required"],
        required_inputs=["research brief", "citation set", "negative evidence needs"],
        expected_outputs=["go/no-go decision", "validation scope", "known risks"],
    ),
    _entry(
        tool_key="assay_design_review",
        display_name="Assay design/review",
        description="Conservative protocol design and review before any wet-lab validation dispatch.",
        task_type="wet_lab",
        validation_type="wet_lab",
        tool_hint="wet_lab_protocol_review",
        recommended_agent_name="assay_design_validation_agent",
        default_objective="Design or review a bounded assay plan with controls, readouts, and stop criteria.",
        assay_type="assay design and protocol review",
        readout="mechanism, viability, pathway, and control strategy",
        endpoint="assay-ready protocol with controls, stop criteria, and failure criteria",
        model_system="canine HSA model system with human angiosarcoma comparator where available",
        quality_gates=[
            "assay_context_present",
            "controls_required",
            "stop_criteria_required",
            "species_and_disease_context_required",
        ],
        required_inputs=["target or biomarker list", "model system availability", "positive and negative controls"],
        expected_outputs=["assay design", "readout panel", "control strategy", "failure criteria"],
    ),
    _entry(
        tool_key="target_expression_review",
        display_name="Target expression review",
        description="Review target expression support in canine HSA and translational comparator evidence.",
        task_type="target_validation",
        validation_type="omics",
        tool_hint="target_expression_review",
        recommended_agent_name="omics_validation_agent",
        default_objective="Review whether target expression evidence supports validation prioritization.",
        assay_type="target expression evidence review",
        readout="canine HSA target expression, human comparator expression, and dataset limitations",
        endpoint="target-expression support rating and dataset gap list",
        model_system="canine HSA tissue, cell, or cohort expression evidence with human comparator",
        quality_gates=["target_identity_required", "omics_dataset_context_required"],
        required_inputs=["target identifier", "canine HSA expression evidence", "human comparator evidence"],
        expected_outputs=["expression support rating", "dataset limitations", "translation risk flags"],
    ),
    _entry(
        tool_key="biomarker_response_assay_design",
        display_name="Biomarker-response assay design",
        description="Design a response assay stratified by biomarkers and source-traceable controls.",
        task_type="wet_lab",
        validation_type="wet_lab",
        tool_hint="biomarker_response_assay_design",
        recommended_agent_name="assay_design_validation_agent",
        default_objective="Design a biomarker-response assay plan with response correlation and controls.",
        assay_type="biomarker-response assay design",
        readout="biomarker stratification, response correlation, pathway suppression, and viability",
        endpoint="assay design with biomarker panel, response metrics, controls, and stop criteria",
        model_system="canine HSA samples or models matched to the proposed biomarker panel",
        quality_gates=[
            "assay_context_present",
            "biomarker_panel_required",
            "controls_required",
            "response_readout_required",
            "stop_criteria_required",
        ],
        required_inputs=["biomarker panel", "candidate therapy list", "model system availability", "control strategy"],
        expected_outputs=["biomarker-response design", "readout panel", "sample inclusion rules", "failure criteria"],
    ),
    _entry(
        tool_key="omics_expression_review",
        display_name="Omics expression review",
        description="Review GEO/SRA/ICDC expression evidence for disease, target, or biomarker support.",
        task_type="omics",
        validation_type="omics",
        tool_hint="geo_sra_expression_review",
        recommended_agent_name="omics_validation_agent",
        default_objective="Review omics expression evidence and dataset limitations for validation relevance.",
        assay_type="omics expression evidence review",
        readout="dataset support, sample metadata quality, differential expression, and species comparison",
        endpoint="omics support rating with dataset gaps and negative evidence needs",
        model_system="GEO, SRA, ICDC, or comparable expression datasets with sample metadata review",
        quality_gates=[
            "omics_dataset_context_required",
            "sample_metadata_required",
            "cross_species_comparator_required",
        ],
        required_inputs=["gene or biomarker terms", "canine HSA datasets", "human angiosarcoma datasets"],
        expected_outputs=["dataset support", "sample metadata limitations", "species translation notes"],
    ),
    _entry(
        tool_key="mutation_function_review",
        display_name="Mutation-function review",
        description="Review mutation consequence, conservation, and functional evidence before promotion.",
        task_type="target_validation",
        validation_type="homology",
        tool_hint="mutation_function_review",
        recommended_agent_name="mutation_function_validation_agent",
        default_objective="Review whether mutation-function evidence supports target or pathway validation.",
        assay_type="mutation-function evidence review",
        readout="mutation consequence, pathway impact, conservation, and functional assay support",
        endpoint="functional relevance rating and evidence gap list",
        model_system="variant annotations, conservation evidence, functional assays, and disease context",
        quality_gates=[
            "target_identity_required",
            "variant_annotation_required",
            "functional_evidence_required",
        ],
        required_inputs=["gene or protein target", "variant list", "functional annotation", "species conservation evidence"],
        expected_outputs=["functional relevance rating", "pathway impact summary", "negative evidence gaps"],
    ),
    _entry(
        tool_key="peptide_specialist_review",
        display_name="Peptide specialist review",
        description=(
            "Specialist review of peptide modality feasibility, target engagement, delivery, "
            "stability, immunogenicity, and translational risk."
        ),
        task_type="expert_review",
        validation_type="expert_review",
        tool_hint="peptide_specialist_review",
        recommended_agent_name="peptide_specialist_validation_agent",
        default_objective=(
            "Review peptide modality feasibility, target engagement plausibility, delivery, "
            "stability, immunogenicity, and translational risk before assay planning."
        ),
        assay_type="peptide modality expert review",
        readout="target engagement plausibility, sequence/developability risks, delivery/stability constraints",
        endpoint="peptide go/no-go review before assay or partner validation",
        model_system=(
            "peptide, peptidomimetic, cyclic peptide, vaccine peptide, or biologic-style modality evidence packet"
        ),
        safety_context=(
            "Review immunogenicity, protease stability, delivery route, formulation, and species-specific safety gaps."
        ),
        quality_gates=[
            "peptide_identity_required",
            "delivery_context_required",
            "stability_or_protease_risk_review_required",
            "immunogenicity_review_required",
        ],
        required_inputs=[
            "peptide sequence or modality",
            "target/pathway rationale",
            "delivery route or formulation context",
            "stability or protease risk context",
        ],
        expected_outputs=[
            "peptide feasibility rating",
            "target engagement caveats",
            "delivery and stability risks",
            "immunogenicity/manufacturability gaps",
        ],
    ),
    _entry(
        tool_key="safety_translational_risk_review",
        display_name="Safety/translational risk review",
        description="Review safety, toxicity, PK/PD, and species-translation risk before validation dispatch.",
        task_type="safety",
        validation_type="safety",
        tool_hint="safety_translational_risk_review",
        recommended_agent_name="safety_review_validation_agent",
        default_objective="Review safety and translational risk before any animal-facing validation step.",
        assay_type="safety and translational risk review",
        readout="toxicity flags, coagulation risk, species PK/PD gaps, and contraindications",
        endpoint="safety gate decision before animal-facing validation",
        model_system="canine safety evidence with human translational comparator where available",
        safety_context=(
            "Review toxicity, coagulation or hemorrhage, PK/PD, dose, and contraindication evidence before dispatch."
        ),
        quality_gates=[
            "safety_context_required",
            "species_translation_review_required",
            "stop_criteria_required",
        ],
        required_inputs=["candidate therapy list", "known safety risks", "species-specific PK/PD evidence"],
        expected_outputs=["safety gate decision", "monitoring requirements", "contraindications and stop criteria"],
    ),
)


def _metadata_payload(
    entry: ValidationToolCatalogEntry,
    metadata: Mapping[str, Any] | None,
) -> dict[str, Any]:
    payload = dict(metadata or {})
    payload["recommend_only"] = True
    payload["validation_tool_catalog"] = {
        "version": VALIDATION_TOOL_CATALOG_VERSION,
        "tool_key": entry.tool_key,
        "display_name": entry.display_name,
        "runner_status": entry.runner_status,
        "mode": _mode_value(entry.mode),
        "recommended_agent_name": entry.recommended_agent_name,
        "tool_hint": entry.tool_hint,
    }
    return payload


def _mode_value(mode: ToolMode | str) -> str:
    if isinstance(mode, ToolMode):
        return mode.value
    return str(mode)


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


__all__ = [
    "VALIDATION_TOOL_CATALOG",
    "VALIDATION_TOOL_CATALOG_VERSION",
    "ValidationToolCatalogEntry",
    "ValidationToolKey",
    "ValidationToolRunnerStatus",
    "ValidationToolValidationType",
    "build_validation_tool_task",
    "build_validation_tool_catalog_report",
    "get_validation_tool",
    "list_validation_tool_catalog",
    "match_validation_tools",
]
