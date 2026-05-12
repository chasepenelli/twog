"""Locus-level signal extraction from public omics bigWig tracks."""

from __future__ import annotations

from dataclasses import dataclass
import importlib
import math
import os
from pathlib import Path
import re
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
from uuid import NAMESPACE_URL, uuid5

from .agent_runner import AgentRunner
from .contracts import (
    OmicsEvidenceDataset,
    OmicsEvidencePacketRequest,
    OmicsLocusSignalDatasetResult,
    OmicsLocusSignalRequest,
    OmicsLocusSignalResult,
    OmicsLocusSignalSampleResult,
    OmicsLocusTarget,
    ValidationAssayContext,
    ValidationRequest,
    ValidationRequestQueueItem,
)
from .omics_evidence_packets import build_omics_evidence_packets
from .omics_readouts import (
    _cohen_d,
    _comparison_design,
    _dedupe_strings,
    _geo_series_base_uri,
    _is_chro_seq_bigwig_dataset,
    _mean,
    _round,
    _sample_groups,
    _sample_roles,
    _support_level,
)
from .repository import ResearchRepository
from .validation_agents import (
    VALIDATION_AGENT_VERSION,
    run_validation_agent,
    summarize_validation_agent_result,
    validation_agent_name,
)


DEFAULT_REMOTE_EXTRACT_TIMEOUT_SECONDS = 600


DEFAULT_LOCUS_TARGETS: dict[str, OmicsLocusTarget] = {
    "VIM": OmicsLocusTarget(
        gene_symbol="VIM",
        chromosome="2",
        start=19_671_316,
        end=19_679_466,
        strand="-",
        genome_build="CanFam3.1",
        metadata={
            "source": "Ensembl archive may2021",
            "ensembl_gene_id": "ENSCAFG00000004529",
        },
    )
}


@dataclass(frozen=True)
class _BigWigRef:
    sample_id: str
    strand: str
    uri: str | None = None
    tar_member: str | None = None
    size_bytes: int | None = None


def build_omics_locus_signals(
    repository: ResearchRepository,
    request: OmicsLocusSignalRequest,
) -> OmicsLocusSignalResult:
    """Extract target-locus bigWig signal for ChRO-seq or related track datasets."""

    packet_result = build_omics_evidence_packets(
        repository,
        OmicsEvidencePacketRequest(
            topic_query=request.topic_query,
            disease_terms=request.disease_terms,
            gene_symbols=request.gene_symbols,
            source_keys=request.source_keys,
            accessions=request.accessions,
            limit=request.limit,
            min_datasets_per_packet=1,
            include_context_packet=True,
            dry_run=request.dry_run,
            dagster_run_id=request.dagster_run_id,
            metadata=request.metadata,
        ),
    )
    datasets = _selected_datasets(packet_result.packets, request)
    datasets = datasets[: request.max_datasets]
    result = OmicsLocusSignalResult(
        dry_run=request.dry_run,
        dataset_count=len(datasets),
        metadata={
            **request.metadata,
            "dagster_run_id": request.dagster_run_id,
            "packet_builder_selected_dataset_count": packet_result.selected_dataset_count,
        },
    )
    dataset_results = [
        _compute_locus_signal_dataset(repository, dataset, request)
        for dataset in datasets
    ]
    result = result.model_copy(
        update={
            "computed_count": sum(1 for item in dataset_results if item.status == "computed"),
            "skipped_count": sum(1 for item in dataset_results if item.status == "skipped"),
            "failed_count": sum(1 for item in dataset_results if item.status == "failed"),
            "datasets": dataset_results,
        }
    )
    if request.run_validation_agent and result.computed_count:
        result = result.model_copy(
            update={
                "validation_agent_result": _run_locus_signal_validation_agent(
                    repository,
                    request,
                    result,
                )
            }
        )
    return result


def _selected_datasets(packets, request: OmicsLocusSignalRequest) -> list[OmicsEvidenceDataset]:
    selected = []
    seen = set()
    for packet in packets:
        if request.packet_id and packet.packet_id != request.packet_id:
            continue
        if request.packet_key and packet.packet_key != request.packet_key:
            continue
        for dataset in packet.datasets:
            key = f"{dataset.source_key}:{dataset.accession}".casefold()
            if key in seen:
                continue
            seen.add(key)
            selected.append(dataset)
    return selected


def _compute_locus_signal_dataset(
    repository: ResearchRepository,
    dataset: OmicsEvidenceDataset,
    request: OmicsLocusSignalRequest,
) -> OmicsLocusSignalDatasetResult:
    try:
        if not _is_chro_seq_bigwig_dataset(dataset) and dataset.accession not in request.bigwig_uri_by_sample:
            return OmicsLocusSignalDatasetResult(
                dataset=dataset,
                status="skipped",
                skipped_reason="not_bigwig_locus_signal_dataset",
                limitations=["bigwig_signal_dataset_required"],
            )
        target = _target_for_request(request)
        sample_groups = _sample_groups(dataset, dataset.sample_accessions, _readout_like_request(request))
        sample_roles = _sample_roles(dataset, dataset.sample_accessions)
        manifest = _resolve_bigwig_manifest(dataset, request)
        selected_samples = _select_samples(manifest, sample_groups, request.max_samples_per_group)
        if not selected_samples:
            return OmicsLocusSignalDatasetResult(
                dataset=dataset,
                status="skipped",
                skipped_reason="no_bigwig_samples_selected",
                target=target,
                sample_count=len(dataset.sample_accessions),
                tumor_sample_count=sum(1 for group in sample_groups.values() if group == "tumor"),
                control_sample_count=sum(1 for group in sample_groups.values() if group == "control"),
                limitations=["bigwig_manifest_missing_or_unusable"],
                metadata={"sample_roles": sample_roles, "comparison_design": _comparison_design(sample_roles)},
            )
        pybigwig = _load_pybigwig()
        if pybigwig is None:
            return OmicsLocusSignalDatasetResult(
                dataset=dataset,
                status="skipped",
                skipped_reason="pybigwig_missing",
                target=target,
                sample_count=len(selected_samples),
                tumor_sample_count=sum(1 for sample in selected_samples if sample_groups.get(sample) == "tumor"),
                control_sample_count=sum(1 for sample in selected_samples if sample_groups.get(sample) == "control"),
                limitations=["pybigwig_dependency_required"],
                metadata={
                    "install_hint": "pip install pyBigWig",
                    "selected_samples": selected_samples,
                    "sample_roles": sample_roles,
                    "comparison_design": _comparison_design(sample_roles),
                },
            )
        sample_results = [
            _compute_sample_signal(pybigwig, dataset, sample_id, manifest[sample_id], sample_groups, sample_roles, target, request)
            for sample_id in selected_samples
        ]
        computed_values = {
            item.sample_id: item.target_strand_mean
            for item in sample_results
            if item.status == "computed" and item.target_strand_mean is not None
        }
        stats = _signal_stats(computed_values, sample_groups)
        normalization = _normalization_metadata(dataset)
        support = _support_level(bool(computed_values), stats)
        status = "computed" if computed_values else "skipped"
        limitations = _dataset_limitations(dataset, sample_results, sample_groups, normalization)
        return OmicsLocusSignalDatasetResult(
            dataset=dataset,
            status=status,
            skipped_reason=None if status == "computed" else "no_locus_signal_values_computed",
            target=target,
            sample_count=len(sample_results),
            computed_sample_count=len(computed_values),
            tumor_sample_count=stats["tumor_n"],
            control_sample_count=stats["control_n"],
            tumor_mean=_round(stats["tumor_mean"]),
            control_mean=_round(stats["control_mean"]),
            tumor_standard_deviation=_round(stats["tumor_standard_deviation"]),
            control_standard_deviation=_round(stats["control_standard_deviation"]),
            tumor_control_delta=_round(stats["delta"]),
            effect_size=_round(stats["effect_size"]),
            comparison_statistic=_round(stats["comparison_statistic"]),
            comparison_p_value=_round(stats["comparison_p_value"]),
            comparison_method=stats["comparison_method"],
            normalization_method=normalization["method"],
            normalization_status=normalization["status"],
            support_level=support,
            sample_results=sample_results,
            limitations=limitations,
            metadata={
                "sample_roles": {sample: sample_roles.get(sample, "unknown") for sample in selected_samples},
                "sample_groups": {sample: sample_groups.get(sample, "unknown") for sample in selected_samples},
                "comparison_design": _comparison_design({sample: sample_roles.get(sample, "unknown") for sample in selected_samples}),
                "selected_samples": selected_samples,
                "manifest_sample_count": len(manifest),
                "remote_extract_timeout_seconds": request.remote_extract_timeout_seconds,
                "normalization": normalization,
                "statistical_test": stats["statistical_test"],
            },
        )
    except Exception as exc:
        return OmicsLocusSignalDatasetResult(
            dataset=dataset,
            status="failed",
            skipped_reason="locus_signal_extraction_failed",
            errors=[str(exc)],
            limitations=["locus_signal_exception"],
        )


def _readout_like_request(request: OmicsLocusSignalRequest):
    from .contracts import OmicsReadoutRequest

    return OmicsReadoutRequest(
        accessions=request.accessions,
        sample_group_overrides=request.sample_group_overrides,
        dry_run=True,
    )


def _target_for_request(request: OmicsLocusSignalRequest) -> OmicsLocusTarget:
    for gene in request.gene_symbols:
        key = gene.upper()
        if key in request.target_loci:
            return request.target_loci[key]
        if key in DEFAULT_LOCUS_TARGETS:
            target = DEFAULT_LOCUS_TARGETS[key]
            if target.flank_bp == request.metadata.get("flank_bp"):
                return target
            flank = int(request.metadata.get("flank_bp") or target.flank_bp)
            return target.model_copy(update={"flank_bp": flank})
    return next(iter(DEFAULT_LOCUS_TARGETS.values()))


def _resolve_bigwig_manifest(
    dataset: OmicsEvidenceDataset,
    request: OmicsLocusSignalRequest,
) -> dict[str, dict[str, _BigWigRef]]:
    manifest: dict[str, dict[str, _BigWigRef]] = {}
    for sample_id, strands in request.bigwig_uri_by_sample.items():
        for strand, uri in strands.items():
            normalized = _normalize_strand(strand)
            if normalized:
                manifest.setdefault(sample_id, {})[normalized] = _BigWigRef(sample_id=sample_id, strand=normalized, uri=uri)
    if manifest:
        return manifest
    if dataset.identifier_type != "geo" or not dataset.accession.upper().startswith("GSE"):
        return manifest
    for row in _geo_filelist_rows(dataset.accession, request.artifact_dir):
        file_name = row.get("name", "")
        if not file_name.casefold().endswith(".bw"):
            continue
        sample_id = file_name.split("_", 1)[0]
        strand = _normalize_strand(file_name)
        if not sample_id or not strand:
            continue
        manifest.setdefault(sample_id, {})[strand] = _BigWigRef(
            sample_id=sample_id,
            strand=strand,
            tar_member=file_name,
            size_bytes=_int_or_none(row.get("size")),
        )
    return manifest


def _geo_filelist_rows(accession: str, artifact_dir: str | None) -> list[dict[str, str]]:
    uri = f"{_geo_series_base_uri(accession.upper())}suppl/filelist.txt"
    data = _read_bytes_cached(uri, artifact_dir, accession, "filelist.txt")
    text = data.decode("utf-8", errors="replace")
    rows = []
    headers: list[str] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if line.startswith("#"):
            headers = [part.strip("#").strip().lower().replace("/", "_") for part in parts]
            continue
        if headers and len(parts) == len(headers):
            rows.append({header: value.strip() for header, value in zip(headers, parts, strict=False)})
    return rows


def _select_samples(
    manifest: dict[str, dict[str, _BigWigRef]],
    sample_groups: dict[str, str],
    max_samples_per_group: int,
) -> list[str]:
    selected: list[str] = []
    counts = {"tumor": 0, "control": 0, "unknown": 0}
    for sample_id in sorted(manifest):
        group = sample_groups.get(sample_id, "unknown")
        group_key = group if group in counts else "unknown"
        if group_key in {"tumor", "control"} and counts[group_key] >= max_samples_per_group:
            continue
        if group_key == "unknown" and counts[group_key] >= max_samples_per_group:
            continue
        if "plus" not in manifest[sample_id] and "minus" not in manifest[sample_id]:
            continue
        selected.append(sample_id)
        counts[group_key] += 1
    return selected


def _compute_sample_signal(
    pybigwig,
    dataset: OmicsEvidenceDataset,
    sample_id: str,
    refs: dict[str, _BigWigRef],
    sample_groups: dict[str, str],
    sample_roles: dict[str, str],
    target: OmicsLocusTarget,
    request: OmicsLocusSignalRequest,
) -> OmicsLocusSignalSampleResult:
    try:
        uris = _materialize_bigwig_refs(dataset, refs, request)
        plus_uri = uris.get("plus")
        minus_uri = uris.get("minus")
        plus_mean = _bigwig_mean(pybigwig, plus_uri, target) if plus_uri else None
        minus_mean = _bigwig_mean(pybigwig, minus_uri, target) if minus_uri else None
        if plus_mean is None and minus_mean is None:
            return OmicsLocusSignalSampleResult(
                sample_id=sample_id,
                group=sample_groups.get(sample_id, "unknown"),
                role=sample_roles.get(sample_id, "unknown"),
                plus_uri=plus_uri,
                minus_uri=minus_uri,
                status="skipped",
                errors=["no_signal_values_in_target_locus"],
            )
        combined = abs(plus_mean or 0.0) + abs(minus_mean or 0.0)
        signed_target_strand = minus_mean if target.strand == "-" else plus_mean
        target_strand = abs(signed_target_strand) if signed_target_strand is not None else None
        if target_strand is None:
            target_strand = combined
        return OmicsLocusSignalSampleResult(
            sample_id=sample_id,
            group=sample_groups.get(sample_id, "unknown"),
            role=sample_roles.get(sample_id, "unknown"),
            plus_uri=plus_uri,
            minus_uri=minus_uri,
            plus_mean=_round(plus_mean),
            minus_mean=_round(minus_mean),
            combined_mean=_round(combined),
            target_strand_mean=_round(target_strand),
            status="computed",
            metadata={
                "derived_signal": "strand_magnitude",
                "target_strand": target.strand,
                "signed_target_strand_mean": _round(signed_target_strand),
            },
        )
    except Exception as exc:
        return OmicsLocusSignalSampleResult(
            sample_id=sample_id,
            group=sample_groups.get(sample_id, "unknown"),
            role=sample_roles.get(sample_id, "unknown"),
            status="failed",
            errors=[str(exc)],
        )


def _materialize_bigwig_refs(
    dataset: OmicsEvidenceDataset,
    refs: dict[str, _BigWigRef],
    request: OmicsLocusSignalRequest,
) -> dict[str, str]:
    uris: dict[str, str] = {}
    targets: dict[str, Path] = {}
    destinations: dict[str, Path] = {}
    for strand in ("plus", "minus"):
        ref = refs.get(strand)
        if ref is None:
            continue
        if ref.uri:
            uris[strand] = ref.uri
            continue
        if not ref.tar_member:
            continue
        destination = _bigwig_destination(dataset, ref, request)
        destinations[strand] = destination
        if destination.exists() and destination.stat().st_size:
            uris[strand] = destination.as_uri()
        else:
            targets[ref.tar_member] = destination
    if targets:
        raw_tar_uri = f"{_geo_series_base_uri(dataset.accession.upper())}suppl/{dataset.accession.upper()}_RAW.tar"
        _extract_tar_members(raw_tar_uri, targets, request.remote_extract_timeout_seconds)
        for strand, destination in destinations.items():
            if destination.exists() and destination.stat().st_size:
                uris[strand] = destination.as_uri()
    return uris


def _bigwig_destination(
    dataset: OmicsEvidenceDataset,
    ref: _BigWigRef,
    request: OmicsLocusSignalRequest,
) -> Path:
    directory = Path(request.artifact_dir or os.getenv("HSA_OMICS_ARTIFACT_DIR", "/tmp/twog_omics_artifacts"))
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{dataset.accession}_{Path(ref.tar_member).name}"


def _extract_tar_members(raw_tar_uri: str, targets: dict[str, Path], max_seconds: int) -> None:
    started_at = time.monotonic()
    with urllib.request.urlopen(raw_tar_uri, timeout=300) as response:
        with tarfile.open(fileobj=response, mode="r|") as archive:
            pending = dict(targets)
            for member in archive:
                if time.monotonic() - started_at > max_seconds:
                    pending_names = ", ".join(sorted(pending)[:5])
                    raise TimeoutError(
                        f"remote_bigwig_extract_timeout after {max_seconds}s; pending={pending_names}"
                    )
                name = Path(member.name).name
                if name not in pending:
                    continue
                output = pending.pop(name)
                extracted = archive.extractfile(member)
                if extracted is None:
                    continue
                output.write_bytes(extracted.read())
                if not pending:
                    break


def _bigwig_mean(pybigwig, uri: str, target: OmicsLocusTarget) -> float | None:
    path = uri.removeprefix("file://") if uri.startswith("file://") else uri
    bw = pybigwig.open(path)
    try:
        chroms = set((bw.chroms() or {}).keys())
        chrom = _matching_chromosome(target.chromosome, chroms)
        if chrom is None:
            raise ValueError(f"chromosome {target.chromosome} not found in bigWig")
        start = max(0, target.start - 1 - target.flank_bp)
        end = target.end + target.flank_bp
        values = bw.stats(chrom, start, end, type="mean", exact=True)
        value = values[0] if values else None
        return float(value) if value is not None else None
    finally:
        bw.close()


def _matching_chromosome(chromosome: str, chroms: set[str]) -> str | None:
    candidates = [chromosome, f"chr{chromosome}", chromosome.removeprefix("chr")]
    for candidate in candidates:
        if candidate in chroms:
            return candidate
    return None


def _signal_stats(values: dict[str, float], sample_groups: dict[str, str]) -> dict[str, object]:
    tumor_items = [(sample, value) for sample, value in values.items() if sample_groups.get(sample) == "tumor"]
    control_items = [(sample, value) for sample, value in values.items() if sample_groups.get(sample) == "control"]
    tumor = [value for _, value in tumor_items]
    control = [value for _, value in control_items]
    tumor_mean = _mean(tumor) if tumor else None
    control_mean = _mean(control) if control else None
    delta = tumor_mean - control_mean if tumor_mean is not None and control_mean is not None else None
    tumor_sd = _sample_standard_deviation(tumor)
    control_sd = _sample_standard_deviation(control)
    comparison = _welch_normal_approximation(tumor, control)
    return {
        "tumor_n": len(tumor),
        "control_n": len(control),
        "tumor_mean": tumor_mean,
        "control_mean": control_mean,
        "tumor_standard_deviation": tumor_sd,
        "control_standard_deviation": control_sd,
        "delta": delta,
        "effect_size": _cohen_d(tumor, control) if tumor and control else None,
        "comparison_statistic": comparison["statistic"],
        "comparison_p_value": comparison["p_value"],
        "comparison_method": comparison["method"],
        "statistical_test": {
            **comparison,
            "tumor_sample_ids": [sample for sample, _ in tumor_items],
            "control_sample_ids": [sample for sample, _ in control_items],
            "tumor_values": [_round(value) for value in tumor],
            "control_values": [_round(value) for value in control],
        },
    }


def _sample_standard_deviation(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def _welch_normal_approximation(tumor: list[float], control: list[float]) -> dict[str, object]:
    method = "welch_t_normal_approximation"
    if len(tumor) < 2 or len(control) < 2:
        return {
            "method": method,
            "statistic": None,
            "p_value": None,
            "degrees_of_freedom": None,
            "status": "insufficient_sample_count",
        }
    tumor_sd = _sample_standard_deviation(tumor)
    control_sd = _sample_standard_deviation(control)
    if tumor_sd is None or control_sd is None:
        return {"method": method, "statistic": None, "p_value": None, "degrees_of_freedom": None, "status": "not_computable"}
    tumor_var_term = (tumor_sd**2) / len(tumor)
    control_var_term = (control_sd**2) / len(control)
    standard_error = math.sqrt(tumor_var_term + control_var_term)
    if standard_error == 0:
        return {"method": method, "statistic": None, "p_value": None, "degrees_of_freedom": None, "status": "zero_variance"}
    statistic = ((_mean(tumor) or 0.0) - (_mean(control) or 0.0)) / standard_error
    numerator = (tumor_var_term + control_var_term) ** 2
    denominator = 0.0
    if len(tumor) > 1:
        denominator += (tumor_var_term**2) / (len(tumor) - 1)
    if len(control) > 1:
        denominator += (control_var_term**2) / (len(control) - 1)
    degrees_of_freedom = numerator / denominator if denominator else None
    p_value = math.erfc(abs(statistic) / math.sqrt(2.0))
    return {
        "method": method,
        "statistic": statistic,
        "p_value": p_value,
        "degrees_of_freedom": degrees_of_freedom,
        "status": "computed",
    }


def _normalization_metadata(dataset: OmicsEvidenceDataset) -> dict[str, str | list[str]]:
    metadata = dataset.metadata or {}
    declared = metadata.get("normalization_method") or metadata.get("normalization") or metadata.get("bigwig_normalization")
    if declared:
        return {
            "method": str(declared),
            "status": "declared",
            "notes": ["normalization_declared_by_source_metadata"],
        }
    return {
        "method": "bigwig_target_locus_mean_signal",
        "status": "not_verified",
        "notes": [
            "bigwig_cross_sample_normalization_not_declared",
            "treat_delta_as_screening_signal_until_library_size_or_track_scaling_is_confirmed",
        ],
    }


def _dataset_limitations(
    dataset: OmicsEvidenceDataset,
    sample_results: list[OmicsLocusSignalSampleResult],
    sample_groups: dict[str, str],
    normalization: dict[str, str | list[str]],
) -> list[str]:
    limitations = ["bigwig_locus_signal_extractor_first_pass"]
    if any(item.status != "computed" for item in sample_results):
        limitations.append("some_samples_missing_locus_signal")
    if not any(sample_groups.get(item.sample_id) == "tumor" for item in sample_results):
        limitations.append("tumor_signal_missing")
    if not any(sample_groups.get(item.sample_id) == "control" for item in sample_results):
        limitations.append("control_signal_missing")
    if normalization.get("status") != "declared":
        limitations.append("bigwig_normalization_not_verified")
    if dataset.accession.upper() == "GSE150705":
        limitations.append("chro_seq_signal_not_steady_state_mrna")
    return _dedupe_strings(limitations)


def _run_locus_signal_validation_agent(
    repository: ResearchRepository,
    request: OmicsLocusSignalRequest,
    result: OmicsLocusSignalResult,
):
    queue_item_id = uuid5(NAMESPACE_URL, "twog:omics-locus-signal-validation")
    plan_id = uuid5(NAMESPACE_URL, "twog:omics-locus-signal-validation-plan")
    task_id = uuid5(NAMESPACE_URL, "twog:omics-locus-signal-validation-task")
    brief_id = uuid5(NAMESPACE_URL, "twog:omics-locus-signal-validation-brief")
    item = ValidationRequestQueueItem(
        queue_item_id=queue_item_id,
        status="approved",
        plan_id=plan_id,
        task_id=task_id,
        brief_id=brief_id,
        source_key="omics_locus_signals",
        topic=request.topic_query,
        task_type="omics",
        title="Review VIM locus-level ChRO-seq signal readouts",
        objective=(
            "Assess whether VIM locus-level bigWig signal supports transcriptional activity "
            "in canine hemangiosarcoma versus available normal controls."
        ),
        rationale="Locus-level signal extraction completed and requires omics interpretation.",
        validation_request=ValidationRequest(
            validation_type="omics",
            target_name="VIM",
            objective="Review computed VIM locus signal from bigWig tracks.",
            require_approval=False,
            assay_context=ValidationAssayContext(
                disease_context="canine hemangiosarcoma",
                species=["canine"],
                assay_type="ChRO-seq bigWig locus signal review",
                readout="VIM target-strand mean signal and tumor/control delta",
                endpoint="omics support rating with genome-build and track limitations",
                evidence_refs=[],
                negative_evidence_needs=[
                    "Separate transcriptional activity signal from steady-state RNA expression.",
                    "Record missing or null signal samples.",
                ],
                provenance_requirements=["bigWig file names", "target locus coordinates", "sample-group labels"],
            ),
            quality_gates=[
                "target_locus_coordinates_declared",
                "sample_metadata_group_labels_extracted",
                "tumor_control_locus_signal_delta_computed",
            ],
            metadata={"omics_locus_signal_result": result.model_dump(mode="json", exclude={"validation_agent_result"})},
        ),
        priority=35,
        requires_human_approval=False,
        quality_gates=[
            "target_locus_coordinates_declared",
            "sample_metadata_group_labels_extracted",
            "tumor_control_locus_signal_delta_computed",
        ],
        metadata={
            "tool_hint": "omics_locus_signal_review",
            "omics_locus_signal_summary": _result_summary(result),
        },
    )
    return AgentRunner(repository).run(
        agent_name=validation_agent_name(item),
        agent_version=VALIDATION_AGENT_VERSION,
        model_profile=request.model_profile,
        input_payload=item.model_dump(mode="json"),
        source_key="omics_locus_signals",
        dagster_run_id=request.dagster_run_id,
        metadata={"accessions": request.accessions, **request.metadata},
        execute=lambda: run_validation_agent(item, model_profile=request.model_profile),
        summarize=summarize_validation_agent_result,
    )


def _result_summary(result: OmicsLocusSignalResult) -> dict[str, object]:
    return {
        "dataset_count": result.dataset_count,
        "computed_count": result.computed_count,
        "skipped_count": result.skipped_count,
        "failed_count": result.failed_count,
        "top_datasets": [
            {
                "accession": item.dataset.accession,
                "status": item.status,
                "support_level": item.support_level,
                "effect_size": item.effect_size,
                "tumor_control_delta": item.tumor_control_delta,
                "comparison_p_value": item.comparison_p_value,
                "normalization_status": item.normalization_status,
                "limitations": item.limitations,
            }
            for item in result.datasets[:5]
        ],
    }


def _read_bytes_cached(uri: str, artifact_dir: str | None, accession: str, suffix: str) -> bytes:
    directory = Path(artifact_dir or os.getenv("HSA_OMICS_ARTIFACT_DIR", "/tmp/twog_omics_artifacts"))
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{accession}_{suffix}"
    if path.exists() and path.stat().st_size:
        return path.read_bytes()
    with urllib.request.urlopen(uri, timeout=30) as response:
        data = response.read()
    path.write_bytes(data)
    return data


def _normalize_strand(value: str) -> str | None:
    lowered = value.casefold()
    if "plus" in lowered or lowered.endswith("+") or lowered == "plus":
        return "plus"
    if "minus" in lowered or lowered.endswith("-") or lowered == "minus":
        return "minus"
    return None


def _load_pybigwig():
    try:
        return importlib.import_module("pyBigWig")
    except ImportError:
        return None


def _int_or_none(value: object) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None
