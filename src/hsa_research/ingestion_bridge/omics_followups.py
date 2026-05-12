"""Follow-up task generation for omics validation gaps."""

from __future__ import annotations

import hashlib
import re
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from .contracts import (
    OmicsFollowupRequest,
    OmicsFollowupResult,
    OmicsFollowupTask,
    OmicsLocusSignalRequest,
    ResearchLeadRecord,
    SourceQuery,
)
from .omics_locus_signals import build_omics_locus_signals
from .repository import ResearchRepository


OMICS_FOLLOWUP_AGENT_VERSION = "v1"
OMICS_FOLLOWUP_TRACK = "omics_followup"


def build_omics_followups(
    repository: ResearchRepository,
    request: OmicsFollowupRequest,
) -> OmicsFollowupResult:
    """Create bounded evidence tasks from omics readout and review gaps."""

    errors: list[str] = []
    locus_report = dict(request.omics_locus_signal_report or {})
    readout_report = dict(request.omics_readout_report or {})

    if request.include_locus_signal_report and not locus_report:
        try:
            locus_payload = {
                "topic_query": request.topic_query,
                "gene_symbols": request.gene_symbols,
                "accessions": request.accessions,
                **request.locus_signal_request,
                "run_validation_agent": False,
            }
            locus_report = build_omics_locus_signals(repository, OmicsLocusSignalRequest(**locus_payload)).model_dump(
                mode="json"
            )
        except Exception as exc:
            errors.append(f"locus_signal_report_failed:{exc}")

    context = _followup_context(request, readout_report, locus_report)
    tasks = _dedupe_tasks(_candidate_tasks(context))[: request.max_tasks]
    leads: list[ResearchLeadRecord] = []
    queries: list[SourceQuery] = []

    if not request.dry_run:
        if request.create_research_leads:
            for task in tasks:
                lead = repository.upsert_research_lead(_lead_from_task(task, request))
                leads.append(lead)
        if request.create_source_queries:
            for task in tasks:
                for query in _source_queries_from_task(task):
                    queries.append(repository.upsert_source_query(query))

    return OmicsFollowupResult(
        dry_run=request.dry_run,
        scanned_dataset_count=context["dataset_count"],
        generated_task_count=len(tasks),
        persisted_research_lead_count=len(leads),
        persisted_source_query_count=len(queries),
        tasks=tasks,
        research_leads=leads,
        source_queries=queries,
        errors=errors,
        metadata={
            "agent_name": "omics_followup_generator",
            "agent_version": OMICS_FOLLOWUP_AGENT_VERSION,
            "dagster_run_id": request.dagster_run_id,
            "accessions": context["accessions"],
            "genes": context["genes"],
            "support_levels": context["support_levels"],
            "limitations": context["limitations"],
        },
    )


def _followup_context(
    request: OmicsFollowupRequest,
    readout_report: dict[str, Any],
    locus_report: dict[str, Any],
) -> dict[str, Any]:
    datasets = list(readout_report.get("datasets") or []) + list(locus_report.get("datasets") or [])
    accessions = set(request.accessions)
    support_levels: set[str] = set()
    limitations: set[str] = set()
    source_keys: set[str] = set(request.source_keys)
    evidence_refs: list[str] = []
    normalization_unverified = False
    missing_samples = False
    null_or_weak = False
    chro_seq = False
    comparator_unclear = False
    sample_metadata_needed = False

    for item in datasets:
        dataset = item.get("dataset") or {}
        accession = str(dataset.get("accession") or "").strip()
        if accession:
            accessions.add(accession)
            evidence_refs.append(accession)
        source_key = str(dataset.get("source_key") or "").strip()
        if source_key:
            source_keys.add(source_key)
        support = str(item.get("support_level") or "").strip()
        if support:
            support_levels.add(support)
        if support in {"differential_null", "not_detected", "insufficient_labels", "descriptive_presence"}:
            null_or_weak = True
        item_limitations = [str(value) for value in item.get("limitations") or []]
        limitations.update(item_limitations)
        if item.get("normalization_status") == "not_verified" or "bigwig_normalization_not_verified" in item_limitations:
            normalization_unverified = True
        if "some_samples_missing_locus_signal" in item_limitations:
            missing_samples = True
        if "chro_seq_signal_not_steady_state_mrna" in item_limitations:
            chro_seq = True
        metadata = item.get("metadata") or {}
        comparison_design = str(metadata.get("comparison_design") or "")
        if "normal_tissue_control" in comparison_design or "primary_tumor_vs_normal_tissue" in comparison_design:
            comparator_unclear = True
        sample_count = int(item.get("sample_count") or 0)
        manifest_sample_count = int((metadata.get("manifest_sample_count") or 0))
        if manifest_sample_count and sample_count and sample_count < manifest_sample_count:
            missing_samples = True
        if metadata.get("sample_groups") or metadata.get("sample_roles"):
            sample_metadata_needed = True

    validation = dict(request.validation_agent_result or locus_report.get("validation_agent_result") or readout_report.get("validation_agent_result") or {})
    for field in ("missing_evidence", "next_actions", "risks", "errors"):
        for value in validation.get(field) or []:
            text = str(value).casefold()
            if "normalization" in text or "library-size" in text or "library size" in text:
                normalization_unverified = True
            if "steady-state" in text or "mrna" in text or "protein" in text or "ihc" in text:
                chro_seq = True
            if "human angiosarcoma" in text or "cross-species" in text:
                null_or_weak = True
            if "control" in text and ("tissue" in text or "annotation" in text):
                comparator_unclear = True

    return {
        "topic": request.topic_query,
        "genes": request.gene_symbols,
        "accessions": sorted(accessions),
        "source_keys": sorted(source_keys),
        "support_levels": sorted(support_levels),
        "limitations": sorted(limitations),
        "evidence_refs": _dedupe_strings(evidence_refs),
        "dataset_count": len(datasets),
        "normalization_unverified": normalization_unverified,
        "missing_samples": missing_samples,
        "null_or_weak": null_or_weak or not datasets,
        "chro_seq": chro_seq,
        "comparator_unclear": comparator_unclear,
        "sample_metadata_needed": sample_metadata_needed,
    }


def _candidate_tasks(context: dict[str, Any]) -> list[OmicsFollowupTask]:
    genes = context["genes"] or ["VIM"]
    accessions = context["accessions"]
    gene_text = " ".join(genes)
    disease_text = "canine hemangiosarcoma human angiosarcoma"
    tasks: list[OmicsFollowupTask] = []

    if context["chro_seq"] or context["null_or_weak"]:
        tasks.append(
            _task(
                "steady_state_expression",
                f"Find steady-state RNA expression evidence for {'/'.join(genes)}",
                "Find RNA-seq, microarray, or processed expression matrices that measure steady-state transcript abundance.",
                "ChRO-seq locus signal does not establish steady-state mRNA abundance; downstream therapy logic needs expression corroboration.",
                f"{disease_text} {gene_text} RNA-seq microarray expression tumor normal processed matrix",
                ["geo", "sra", "pubmed", "europe_pmc"],
                genes,
                accessions,
                context["evidence_refs"],
                20,
                {"gap": "steady_state_expression"},
            )
        )
        tasks.append(
            _task(
                "protein_expression",
                f"Find protein/IHC evidence for {'/'.join(genes)} in HSA",
                "Find vimentin protein, IHC, proteomics, or pathology evidence in canine HSA and human angiosarcoma.",
                "A transcript or locus signal is not enough to support target availability; protein-level evidence changes confidence.",
                f"{disease_text} vimentin VIM immunohistochemistry IHC proteomics protein expression",
                ["pubmed", "europe_pmc"],
                genes,
                accessions,
                context["evidence_refs"],
                25,
                {"gap": "protein_expression"},
            )
        )

    if context["normalization_unverified"]:
        tasks.append(
            _task(
                "normalization_review",
                "Verify bigWig normalization and ChRO-seq scaling",
                "Find method details for bigWig generation, library-size scaling, RPM/RPKM handling, and track comparability.",
                "The locus delta should remain screening-only until cross-sample bigWig normalization is verified.",
                f"{' '.join(accessions)} ChRO-seq bigWig normalization library size RPM RPKM methods",
                ["pubmed", "europe_pmc", "geo"],
                genes,
                accessions,
                context["evidence_refs"],
                30,
                {"gap": "normalization"},
            )
        )

    if context["missing_samples"] or context["sample_metadata_needed"] or context["comparator_unclear"]:
        tasks.append(
            _task(
                "sample_metadata_review",
                "Resolve omics sample labels, controls, and exclusions",
                "Confirm tumor/control labels, NM tissue meaning, comparator tissue type, and why any manifest samples were excluded.",
                "Control identity and sample inclusion decide whether the comparison is biologically meaningful.",
                f"{' '.join(accessions)} sample metadata tumor normal NM tissue canine hemangiosarcoma ChRO-seq",
                ["geo", "sra", "pubmed", "europe_pmc"],
                genes,
                accessions,
                context["evidence_refs"],
                35,
                {"gap": "sample_metadata"},
            )
        )

    tasks.append(
        _task(
            "cross_species_comparator",
            f"Find human angiosarcoma comparator expression for {'/'.join(genes)}",
            "Find human angiosarcoma expression, protein, or single-cell evidence for the same gene/marker axis.",
            "The program needs cross-species support before treating canine HSA findings as translational evidence.",
            f"human angiosarcoma {gene_text} vimentin expression RNA-seq IHC proteomics",
            ["geo", "sra", "pubmed", "europe_pmc"],
            genes,
            accessions,
            context["evidence_refs"],
            40,
            {"gap": "cross_species_comparator"},
        )
    )
    tasks.append(
        _task(
            "gene_set_context",
            "Score pathway context around VIM instead of one locus only",
            "Compute or find mesenchymal/ECM, angiogenesis/endothelial, and coagulation/vascular injury signatures.",
            "A single VIM locus can be null while the broader vascular injury or mesenchymal program remains informative.",
            f"{disease_text} mesenchymal ECM angiogenesis endothelial coagulation vascular injury expression signature",
            ["geo", "sra", "pubmed", "europe_pmc"],
            genes,
            accessions,
            context["evidence_refs"],
            45,
            {"gap": "gene_set_context"},
        )
    )
    tasks.append(
        _task(
            "negative_control_locus",
            "Add negative-control locus extraction for bigWig background",
            "Run a control-locus or housekeeping-locus comparison to estimate background and track behavior.",
            "Negative controls make locus-level signal extraction easier to interpret and debug.",
            f"{' '.join(accessions)} ChRO-seq bigWig control locus housekeeping background signal",
            ["geo"],
            genes,
            accessions,
            context["evidence_refs"],
            55,
            {"gap": "negative_control_locus"},
        )
    )
    return tasks


def _task(
    task_type: str,
    title: str,
    objective: str,
    rationale: str,
    query_text: str,
    source_keys: list[str],
    genes: list[str],
    accessions: list[str],
    evidence_refs: list[str],
    priority: int,
    metadata: dict[str, Any],
) -> OmicsFollowupTask:
    identity = _identity_key(task_type, query_text)
    return OmicsFollowupTask(
        identity_key=identity,
        task_id=uuid5(NAMESPACE_URL, f"twog:{identity}"),
        task_type=task_type,  # type: ignore[arg-type]
        title=title,
        objective=objective,
        rationale=rationale,
        query_text=query_text,
        source_keys=source_keys,
        target_genes=genes,
        accessions=accessions,
        evidence_refs=evidence_refs,
        priority=priority,
        metadata=metadata,
    )


def _lead_from_task(task: OmicsFollowupTask, request: OmicsFollowupRequest) -> ResearchLeadRecord:
    return ResearchLeadRecord(
        lead_id=uuid5(NAMESPACE_URL, f"twog:research-lead:{task.identity_key}"),
        identity_key=f"research_lead:{task.identity_key}",
        title=task.title,
        lead_type="unknown",
        status="followup",
        priority=task.priority,
        source_key=(task.source_keys[0] if task.source_keys else None),
        origin_source_key="omics_followup_generator",
        reason=task.rationale,
        summary=task.objective,
        evidence_refs=task.evidence_refs,
        topic_tags=["omics_followup", task.task_type, *[gene.lower() for gene in task.target_genes]],
        identifiers={"accessions": ",".join(task.accessions)} if task.accessions else {},
        suggested_sources=task.source_keys,
        metadata={
            "operator": request.operator,
            "query_text": task.query_text,
            "task_id": str(task.task_id),
            "task_type": task.task_type,
            "track": OMICS_FOLLOWUP_TRACK,
            **task.metadata,
        },
    )


def _source_queries_from_task(task: OmicsFollowupTask) -> list[SourceQuery]:
    queries: list[SourceQuery] = []
    slug = _slug(task.task_type)
    for source_key in task.source_keys:
        queries.append(
            SourceQuery(
                source_key=source_key,
                query_name=f"omics_followup_{slug}_{_digest(task.query_text)[:10]}",
                query_text=task.query_text,
                query_params={
                    "task_id": str(task.task_id),
                    "task_type": task.task_type,
                    "target_genes": task.target_genes,
                    "accessions": task.accessions,
                    "priority": task.priority,
                    "evidence_refs": task.evidence_refs,
                },
                track=OMICS_FOLLOWUP_TRACK,
                active=True,
            )
        )
    return queries


def _dedupe_tasks(tasks: list[OmicsFollowupTask]) -> list[OmicsFollowupTask]:
    deduped: list[OmicsFollowupTask] = []
    seen: set[str] = set()
    for task in sorted(tasks, key=lambda item: item.priority):
        key = task.identity_key or task.query_text
        if key in seen:
            continue
        seen.add(key)
        deduped.append(task)
    return deduped


def _dedupe_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        deduped.append(normalized)
        seen.add(normalized)
    return deduped


def _identity_key(task_type: str, query_text: str) -> str:
    return f"omics_followup:{_slug(task_type)}:{_digest(query_text)[:16]}"


def _digest(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") or "task"
