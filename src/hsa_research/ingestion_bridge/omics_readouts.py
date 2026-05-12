"""Processed-matrix omics readouts for VIM and vascular-state programs."""

from __future__ import annotations

from collections import defaultdict
import csv
from dataclasses import dataclass
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from uuid import NAMESPACE_URL, UUID, uuid5

from .agent_runner import AgentRunner
from .contracts import (
    ArtifactHandle,
    OmicsEvidenceDataset,
    OmicsEvidencePacket,
    OmicsEvidencePacketRequest,
    OmicsGeneSetScore,
    OmicsReadoutDatasetResult,
    OmicsReadoutRequest,
    OmicsReadoutResult,
    OmicsTargetExpressionScore,
    ValidationAssayContext,
    ValidationRequest,
    ValidationRequestQueueItem,
)
from .omics_evidence_packets import build_omics_evidence_packets
from .repository import ResearchRepository
from .validation_agents import (
    VALIDATION_AGENT_VERSION,
    run_validation_agent,
    summarize_validation_agent_result,
    validation_agent_name,
)


GENE_SETS: dict[str, list[str]] = {
    "vimentin_target": ["VIM"],
    "mesenchymal_ecm": [
        "VIM",
        "FN1",
        "COL1A1",
        "COL1A2",
        "COL3A1",
        "POSTN",
        "SPARC",
        "MMP2",
        "MMP9",
        "SNAI1",
        "SNAI2",
        "ZEB1",
        "TWIST1",
    ],
    "angiogenesis_endothelial": [
        "VEGFA",
        "KDR",
        "FLT1",
        "ANGPT2",
        "TEK",
        "PECAM1",
        "VWF",
        "ENG",
        "ESAM",
        "CDH5",
        "NOTCH1",
        "DLL4",
    ],
    "coagulation_vascular_injury": [
        "F3",
        "SERPINE1",
        "THBD",
        "PROCR",
        "PLAT",
        "PLAUR",
        "SELE",
        "ICAM1",
        "VCAM1",
        "ITGA2B",
        "GP1BA",
    ],
}

_PROCESSED_FILE_EXTENSIONS = (".txt", ".tsv", ".csv", ".xls", ".xlsx", ".h5", ".hdf5", ".mtx")
_RAW_FILE_EXTENSIONS = (".cel", ".fastq", ".fq", ".bam", ".sam", ".sra")
_GENE_COLUMN_CANDIDATES = (
    "gene",
    "gene_symbol",
    "genesymbol",
    "symbol",
    "hgnc_symbol",
    "external_gene_name",
    "gene name",
    "gene_assignment",
    "id_ref",
    "target_id",
    "probe",
    "probe_id",
)
_NON_SAMPLE_COLUMN_HEADERS = {
    "entrez_gene_id",
    "ncbi_gene",
    "ncbi_name",
    "ncbi_accession",
    "gene_function",
    "mapped_reads",
    "on_target",
    "targets_detected",
    "targets_1_reads",
    "target_2_reads",
}
_CONTROL_TERMS = ("control", "normal", "healthy", "benign", "reference", "untreated")
_TUMOR_TERMS = (
    "hemangiosarcoma",
    "angiosarcoma",
    "hsa",
    "tumor",
    "tumour",
    "cancer",
    "sarcoma",
    "malignant",
)
_DISEASE_COMPARATOR_TERMS = (
    "osteosarcoma",
    "lymphoma",
    "leukemia",
    "splenic hematoma",
    "hematoma",
)
_GENE_SYMBOL_MAP_KEYS = ("gene_symbol_map", "gene_identifier_map", "ensembl_gene_symbol_map")
_CANINE_LEGACY_ENSEMBL_BIOMART_URI = "https://may2021.archive.ensembl.org/biomart/martservice"


@dataclass(frozen=True)
class _MatrixPayload:
    text: str
    uri: str
    artifact_id: UUID | None
    metadata: dict


@dataclass(frozen=True)
class _ParsedMatrix:
    values: dict[str, dict[str, float]]
    samples: list[str]
    count_like: bool
    skipped_rows: int


def build_omics_readouts(
    repository: ResearchRepository,
    request: OmicsReadoutRequest,
) -> OmicsReadoutResult:
    """Compute deterministic VIM and gene-set readouts from processed matrices."""

    packet_result = build_omics_evidence_packets(
        repository,
        OmicsEvidencePacketRequest(
            program_id=request.program_id,
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
    packets = _select_packets(packet_result.packets, request)
    result = OmicsReadoutResult(
        program_id=request.program_id,
        therapy_idea_id=request.therapy_idea_id,
        packet_id=packets[0].packet_id if len(packets) == 1 else request.packet_id,
        packet_key=packets[0].packet_key if len(packets) == 1 else request.packet_key,
        dry_run=request.dry_run,
        metadata={
            **request.metadata,
            "dagster_run_id": request.dagster_run_id,
            "packet_count": len(packets),
            "packet_builder_selected_dataset_count": packet_result.selected_dataset_count,
        },
    )
    if not packets:
        return result.model_copy(update={"errors": ["No omics evidence packets matched the readout request."]})

    datasets = _dedupe_datasets([dataset for packet in packets for dataset in packet.datasets])
    datasets = datasets[: request.max_datasets]
    dataset_results = [
        _compute_dataset_readout(repository, dataset, request, packets[0])
        for dataset in datasets
    ]
    artifact_ids = [
        artifact_id
        for item in dataset_results
        for artifact_id in (item.matrix_artifact_id, item.result_artifact_id)
        if artifact_id is not None
    ]
    result = result.model_copy(
        update={
            "dataset_count": len(dataset_results),
            "computed_count": sum(1 for item in dataset_results if item.status == "computed"),
            "skipped_count": sum(1 for item in dataset_results if item.status == "skipped"),
            "failed_count": sum(1 for item in dataset_results if item.status == "failed"),
            "artifact_ids": artifact_ids,
            "datasets": dataset_results,
        }
    )
    if request.run_validation_agent and result.computed_count:
        result = result.model_copy(
            update={
                "validation_agent_result": _run_omics_validation_agent(
                    repository,
                    request,
                    result,
                )
            }
        )
    return result


def _select_packets(
    packets: list[OmicsEvidencePacket],
    request: OmicsReadoutRequest,
) -> list[OmicsEvidencePacket]:
    selected = packets
    if request.packet_id:
        selected = [packet for packet in selected if packet.packet_id == request.packet_id]
    if request.packet_key:
        selected = [packet for packet in selected if packet.packet_key == request.packet_key]
    return selected


def _compute_dataset_readout(
    repository: ResearchRepository,
    dataset: OmicsEvidenceDataset,
    request: OmicsReadoutRequest,
    packet: OmicsEvidencePacket,
) -> OmicsReadoutDatasetResult:
    try:
        if _requires_locus_signal_extractor(dataset, request):
            sample_groups = _sample_groups(dataset, dataset.sample_accessions, request)
            sample_roles = _sample_roles(dataset, dataset.sample_accessions)
            labeled_sample_count = sum(1 for group in sample_groups.values() if group in {"tumor", "control"})
            return OmicsReadoutDatasetResult(
                dataset=dataset,
                status="skipped",
                skipped_reason="chro_seq_bigwig_locus_extraction_required",
                sample_count=len(dataset.sample_accessions),
                labeled_sample_count=labeled_sample_count,
                tumor_sample_count=sum(1 for group in sample_groups.values() if group == "tumor"),
                control_sample_count=sum(1 for group in sample_groups.values() if group == "control"),
                sample_groups=sample_groups,
                limitations=[
                    *_raw_limitations(dataset),
                    "chro_seq_bigwig_locus_extraction_required",
                    "processed_expression_matrix_not_expected_for_bigwig_signal_files",
                ],
                metadata={
                    "recommended_next_path": "bigwig_locus_signal_extractor",
                    "target_loci": request.gene_symbols,
                    "sample_roles": sample_roles,
                    "comparison_design": _comparison_design(sample_roles),
                    "locus_signal_metadata": _locus_signal_metadata(dataset),
                },
            )
        matrix = _resolve_matrix_payload(repository, dataset, request)
        if matrix is None:
            return OmicsReadoutDatasetResult(
                dataset=dataset,
                status="skipped",
                skipped_reason=_skip_reason(dataset),
                limitations=[*_raw_limitations(dataset), "processed_matrix_not_found"],
            )
        probe_mapping = _platform_probe_mapping(repository, dataset, request)
        parsed = _parse_matrix(matrix.text, gene_symbol_lookup=probe_mapping)
        if not parsed.samples or not parsed.values:
            return OmicsReadoutDatasetResult(
                dataset=dataset,
                status="skipped",
                skipped_reason="processed_matrix_had_no_sample_by_gene_values",
                matrix_uri=matrix.uri,
                matrix_artifact_id=matrix.artifact_id,
                limitations=["matrix_parse_empty"],
                metadata={"parser": matrix.metadata},
            )
        normalized = _normalize_matrix(parsed)
        sample_groups = _sample_groups(dataset, parsed.samples, request)
        sample_roles = _sample_roles(dataset, parsed.samples)
        target_expression = _target_expression_score(
            normalized.values,
            parsed.samples,
            sample_groups,
            request.gene_symbols,
        )
        gene_set_scores = [
            _gene_set_score(key, genes, normalized.values, parsed.samples, sample_groups)
            for key, genes in GENE_SETS.items()
        ]
        labeled_sample_count = sum(1 for group in sample_groups.values() if group in {"tumor", "control"})
        result = OmicsReadoutDatasetResult(
            dataset=dataset,
            status="computed",
            matrix_uri=matrix.uri,
            matrix_artifact_id=matrix.artifact_id,
            normalized_kind=normalized.kind,
            sample_count=len(parsed.samples),
            gene_count=len(normalized.values),
            labeled_sample_count=labeled_sample_count,
            tumor_sample_count=sum(1 for group in sample_groups.values() if group == "tumor"),
            control_sample_count=sum(1 for group in sample_groups.values() if group == "control"),
            sample_groups=sample_groups,
            target_expression=target_expression,
            gene_set_scores=gene_set_scores,
            limitations=_computed_limitations(
                dataset,
                target_expression,
                gene_set_scores,
                labeled_sample_count,
                parsed,
                sample_groups,
                sample_roles,
            ),
            metadata={
                "packet_id": packet.packet_id,
                "packet_key": packet.packet_key,
                "parser_skipped_rows": parsed.skipped_rows,
                "matrix_metadata": matrix.metadata,
                "platform_probe_mapping_count": len(probe_mapping),
                "gene_identifier_mapping_sources": _mapping_sources(probe_mapping),
                "sample_roles": sample_roles,
                "comparison_design": _comparison_design(sample_roles),
            },
        )
        if not request.dry_run:
            artifact = _persist_result_artifact(repository, result, request)
            result = result.model_copy(update={"result_artifact_id": artifact.artifact_id})
        return result
    except Exception as exc:
        return OmicsReadoutDatasetResult(
            dataset=dataset,
            status="failed",
            skipped_reason="omics_readout_failed",
            errors=[str(exc)],
            limitations=["readout_exception"],
        )


@dataclass(frozen=True)
class _NormalizedMatrix:
    values: dict[str, dict[str, float]]
    kind: str


def _resolve_matrix_payload(
    repository: ResearchRepository,
    dataset: OmicsEvidenceDataset,
    request: OmicsReadoutRequest,
) -> _MatrixPayload | None:
    accession = dataset.accession
    content = _metadata_text(dataset.metadata)
    if content:
        return _matrix_payload_from_text(repository, dataset, request, content, "metadata:matrix_text")

    if accession in request.matrix_uri_by_accession:
        return _matrix_payload_from_uri(
            repository,
            dataset,
            request,
            request.matrix_uri_by_accession[accession],
            source="request.matrix_uri_by_accession",
        )

    obj = repository.get_research_object(dataset.research_object_id) if dataset.research_object_id else None
    metadata = {**dataset.metadata, **((obj.metadata if obj else {}) or {})}

    content = _metadata_text(metadata)
    if content:
        return _matrix_payload_from_text(repository, dataset, request, content, "research_object.metadata:matrix_text")

    artifact_id = _artifact_id_from_metadata(metadata)
    if artifact_id:
        artifact = repository.get_artifact(artifact_id)
        if artifact:
            artifact_content = _metadata_text(artifact.metadata)
            if artifact_content:
                return _MatrixPayload(
                    text=artifact_content,
                    uri=artifact.uri,
                    artifact_id=artifact.artifact_id,
                    metadata={"source": "artifact_metadata", "artifact_type": artifact.artifact_type},
                )
            return _matrix_payload_from_uri(
                repository,
                dataset,
                request,
                artifact.uri,
                source="artifact_uri",
                existing_artifact_id=artifact.artifact_id,
            )

    for uri in _matrix_uris_from_metadata(metadata):
        payload = _matrix_payload_from_uri(repository, dataset, request, uri, source="metadata_uri")
        if payload:
            return payload

    if dataset.identifier_type == "geo" and dataset.accession.upper().startswith("GSE"):
        for uri in _geo_supplementary_matrix_uris(dataset.accession):
            payload = _matrix_payload_from_uri(
                repository,
                dataset,
                request,
                uri,
                source="geo_supplementary_matrix",
            )
            if payload:
                return payload
        return _matrix_payload_from_uri(
            repository,
            dataset,
            request,
            _geo_series_matrix_uri(dataset.accession),
            source="geo_series_matrix",
        )
    return None


def _matrix_payload_from_text(
    repository: ResearchRepository,
    dataset: OmicsEvidenceDataset,
    request: OmicsReadoutRequest,
    text: str,
    source: str,
) -> _MatrixPayload:
    artifact_id = uuid5(NAMESPACE_URL, f"twog:omics-matrix:{dataset.source_key}:{dataset.accession}:{source}")
    artifact_uri = f"twog://omics/matrix/{artifact_id}"
    if not request.dry_run:
        repository.upsert_artifact(
            ArtifactHandle(
                artifact_id=artifact_id,
                artifact_type="omics_processed_matrix",
                uri=artifact_uri,
                legal_status="derived_from_local_research_metadata",
                mime_type="text/tab-separated-values",
                metadata={
                    "source_key": dataset.source_key,
                    "accession": dataset.accession,
                    "source": source,
                    "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                    "size_bytes": len(text.encode("utf-8")),
                    "content": text,
                },
            )
        )
    return _MatrixPayload(
        text=text,
        uri=artifact_uri,
        artifact_id=None if request.dry_run else artifact_id,
        metadata={"source": source, "size_bytes": len(text.encode("utf-8"))},
    )


def _matrix_payload_from_uri(
    repository: ResearchRepository,
    dataset: OmicsEvidenceDataset,
    request: OmicsReadoutRequest,
    uri: str,
    *,
    source: str,
    existing_artifact_id: UUID | None = None,
) -> _MatrixPayload | None:
    try:
        text, persisted_uri, mime_type = _read_matrix_uri(uri, request.artifact_dir, dataset.accession)
    except (OSError, UnicodeDecodeError, urllib.error.URLError, TimeoutError):
        return None
    artifact_id = existing_artifact_id or uuid5(
        NAMESPACE_URL,
        f"twog:omics-matrix-uri:{dataset.source_key}:{dataset.accession}:{uri}",
    )
    if not request.dry_run and existing_artifact_id is None:
        repository.upsert_artifact(
            ArtifactHandle(
                artifact_id=artifact_id,
                artifact_type="omics_processed_matrix",
                uri=persisted_uri,
                legal_status="public_metadata_or_open_processed_matrix",
                mime_type=mime_type,
                metadata={
                    "source_key": dataset.source_key,
                    "accession": dataset.accession,
                    "source": source,
                    "original_uri": uri,
                    "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                    "size_bytes": len(text.encode("utf-8")),
                },
            )
        )
    return _MatrixPayload(
        text=text,
        uri=persisted_uri,
        artifact_id=None if request.dry_run else artifact_id,
        metadata={"source": source, "original_uri": uri, "size_bytes": len(text.encode("utf-8"))},
    )


def _read_matrix_uri(uri: str, artifact_dir: str | None, accession: str) -> tuple[str, str, str]:
    data, persisted_uri, file_name = _read_uri_bytes(uri, artifact_dir, accession)
    return _decode_matrix_bytes(data, file_name), persisted_uri, _mime_type(file_name)


def _read_uri_bytes(uri: str, artifact_dir: str | None, accession: str) -> tuple[bytes, str, str]:
    if uri.startswith("twog://"):
        raise OSError("twog artifact URI must be resolved through repository metadata")
    if uri.startswith("file://"):
        path = Path(uri.removeprefix("file://"))
        return path.read_bytes(), uri, path.name
    if uri.startswith(("http://", "https://")):
        with urllib.request.urlopen(uri, timeout=30) as response:
            data = response.read()
        directory = Path(artifact_dir or os.getenv("HSA_OMICS_ARTIFACT_DIR", "/tmp/twog_omics_artifacts"))
        directory.mkdir(parents=True, exist_ok=True)
        file_name = _safe_file_name(accession, uri)
        path = directory / file_name
        path.write_bytes(data)
        return data, path.as_uri(), file_name
    path = Path(uri).expanduser()
    if not path.is_absolute():
        path = path.resolve()
    return path.read_bytes(), path.as_uri(), path.name


def _decode_matrix_bytes(data: bytes, file_name: str) -> str:
    lower = file_name.casefold()
    if lower.endswith(".gz"):
        data = gzip.decompress(data)
        lower = lower[:-3]
    if lower.endswith(".xlsx"):
        return _xlsx_to_tsv(data)
    return data.decode("utf-8")


def _parse_matrix(
    text: str,
    *,
    gene_symbol_lookup: dict[str, list[str]] | None = None,
) -> _ParsedMatrix:
    gene_symbol_lookup = gene_symbol_lookup or {}
    table_text = _series_matrix_table(text)
    first_line = next((line for line in table_text.splitlines() if line.strip()), "")
    if not first_line:
        return _ParsedMatrix(values={}, samples=[], count_like=False, skipped_rows=0)
    delimiter = "\t" if "\t" in first_line else ","
    rows = [
        [_clean_cell(cell) for cell in row]
        for row in csv.reader(io.StringIO(table_text), delimiter=delimiter)
        if row and any(str(cell).strip() for cell in row)
    ]
    header_index = _header_index(rows)
    if header_index is None:
        return _ParsedMatrix(values={}, samples=[], count_like=False, skipped_rows=len(rows))
    header = rows[header_index]
    gene_col = _gene_column_index(header)
    numeric_cols = _numeric_columns(rows[header_index + 1 :], gene_col, header)
    samples = [header[index] for index in numeric_cols]
    gene_values: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    skipped = 0
    for row in rows[header_index + 1 :]:
        if len(row) <= max([gene_col, *numeric_cols], default=gene_col):
            skipped += 1
            continue
        raw_genes = _gene_symbols(row[gene_col])
        genes = _mapped_gene_symbols(raw_genes, gene_symbol_lookup)
        if not genes:
            skipped += 1
            continue
        for col in numeric_cols:
            value = _float_or_none(row[col] if col < len(row) else None)
            if value is None:
                continue
            for gene in genes:
                gene_values[gene][header[col]].append(value)
    values = {
        gene: {
            sample: sum(sample_values) / len(sample_values)
            for sample, sample_values in by_sample.items()
            if sample_values
        }
        for gene, by_sample in gene_values.items()
    }
    numeric_values = [value for by_sample in values.values() for value in by_sample.values()]
    return _ParsedMatrix(
        values=values,
        samples=samples,
        count_like=_looks_count_like(numeric_values),
        skipped_rows=skipped,
    )


def _normalize_matrix(parsed: _ParsedMatrix) -> _NormalizedMatrix:
    if not parsed.count_like:
        return _NormalizedMatrix(values=parsed.values, kind="processed_expression")
    library_sizes = {
        sample: sum(max(0.0, by_sample.get(sample, 0.0)) for by_sample in parsed.values.values())
        for sample in parsed.samples
    }
    normalized: dict[str, dict[str, float]] = {}
    for gene, by_sample in parsed.values.items():
        normalized[gene] = {}
        for sample in parsed.samples:
            raw = max(0.0, by_sample.get(sample, 0.0))
            library_size = library_sizes.get(sample) or 0.0
            cpm = (raw / library_size * 1_000_000.0) if library_size else 0.0
            normalized[gene][sample] = math.log2(cpm + 1.0)
    return _NormalizedMatrix(values=normalized, kind="counts_cpm_log1p")


def _target_expression_score(
    values: dict[str, dict[str, float]],
    samples: list[str],
    sample_groups: dict[str, str],
    target_terms: list[str],
) -> OmicsTargetExpressionScore:
    target_symbols = _target_symbols(target_terms)
    detected = [gene for gene in target_symbols if gene in values]
    if not detected:
        return OmicsTargetExpressionScore(
            target="VIM",
            detected=False,
            sample_count=len(samples),
            support_level="not_detected",
            interpretation="VIM/vimentin was not detected in the parsed matrix gene identifiers.",
        )
    sample_values = _mean_by_sample([values[gene] for gene in detected], samples)
    stats = _comparison_stats(sample_values, sample_groups)
    support_level = _support_level(detected=True, stats=stats)
    return OmicsTargetExpressionScore(
        target="VIM",
        detected=True,
        detected_gene_symbols=detected,
        sample_count=len(sample_values),
        tumor_sample_count=stats["tumor_n"],
        control_sample_count=stats["control_n"],
        normalized_mean=_round(_mean(list(sample_values.values()))),
        tumor_mean=_round(stats["tumor_mean"]),
        control_mean=_round(stats["control_mean"]),
        tumor_control_delta=_round(stats["delta"]),
        effect_size=_round(stats["effect_size"]),
        support_level=support_level,
        interpretation=_target_interpretation(support_level, stats),
    )


def _gene_set_score(
    gene_set_key: str,
    genes: list[str],
    values: dict[str, dict[str, float]],
    samples: list[str],
    sample_groups: dict[str, str],
) -> OmicsGeneSetScore:
    detected = [gene for gene in genes if gene in values]
    if not detected:
        return OmicsGeneSetScore(
            gene_set_key=gene_set_key,  # type: ignore[arg-type]
            gene_count=len(genes),
            detected_gene_count=0,
            sample_count=len(samples),
            support_level="not_detected",
            interpretation=f"No genes from {gene_set_key} were detected in the parsed matrix.",
        )
    z_values = [_zscore_by_sample(values[gene], samples) for gene in detected]
    sample_scores = _mean_by_sample(z_values, samples)
    stats = _comparison_stats(sample_scores, sample_groups)
    support_level = _support_level(detected=True, stats=stats)
    return OmicsGeneSetScore(
        gene_set_key=gene_set_key,  # type: ignore[arg-type]
        detected_gene_symbols=detected,
        gene_count=len(genes),
        detected_gene_count=len(detected),
        coverage_ratio=round(len(detected) / len(genes), 3) if genes else 0.0,
        sample_count=len(sample_scores),
        tumor_sample_count=stats["tumor_n"],
        control_sample_count=stats["control_n"],
        mean_score=_round(_mean(list(sample_scores.values()))),
        tumor_mean=_round(stats["tumor_mean"]),
        control_mean=_round(stats["control_mean"]),
        tumor_control_delta=_round(stats["delta"]),
        effect_size=_round(stats["effect_size"]),
        support_level=support_level,
        interpretation=_gene_set_interpretation(gene_set_key, support_level, stats),
    )


def _run_omics_validation_agent(
    repository: ResearchRepository,
    request: OmicsReadoutRequest,
    result: OmicsReadoutResult,
):
    queue_item_id = uuid5(NAMESPACE_URL, f"twog:omics-validation:{result.packet_id}:{result.packet_key}")
    plan_id = uuid5(NAMESPACE_URL, f"twog:omics-validation-plan:{result.packet_id}:{result.packet_key}")
    task_id = uuid5(NAMESPACE_URL, f"twog:omics-validation-task:{result.packet_id}:{result.packet_key}")
    evidence_refs = [
        f"artifact:{artifact_id}"
        for artifact_id in result.artifact_ids
    ]
    item = ValidationRequestQueueItem(
        queue_item_id=queue_item_id,
        status="approved",
        plan_id=plan_id,
        task_id=task_id,
        brief_id=uuid5(NAMESPACE_URL, f"twog:omics-readout-brief:{result.packet_id}:{result.packet_key}"),
        source_key="omics_readouts",
        topic=request.topic_query,
        task_type="omics",
        title="Review computed VIM and vascular-state omics readouts",
        objective=(
            "Assess whether computed VIM/vimentin expression and vascular-state gene-set readouts "
            "support the peptide/vimentin child packet or expose material evidence gaps."
        ),
        rationale="Processed public matrix readouts are available and require specialist interpretation.",
        validation_request=ValidationRequest(
            validation_type="omics",
            target_name="VIM",
            objective="Review computed omics readouts for target-expression and gene-set support.",
            require_approval=False,
            assay_context=ValidationAssayContext(
                disease_context="canine hemangiosarcoma and human angiosarcoma",
                species=["canine", "human"],
                assay_type="processed transcriptomic matrix review",
                readout="VIM expression, mesenchymal/ECM, angiogenesis, and coagulation/vascular injury scores",
                endpoint="omics support rating with dataset limitations and null evidence",
                evidence_refs=evidence_refs,
                negative_evidence_needs=[
                    "Record datasets where VIM or requested gene sets are absent.",
                    "Separate descriptive presence from tumor/control differential support.",
                ],
                provenance_requirements=["matrix artifact IDs", "dataset accessions", "sample-group labels"],
            ),
            quality_gates=[
                "sample_metadata_group_labels_extracted",
                "VIM_or_vimentin_readout_computed_with_direction_and_effect_size",
                "negative_or_null_expression_results_recorded",
            ],
            metadata={
                "omics_readout_result": result.model_dump(mode="json", exclude={"validation_agent_result"}),
                "validation_tool_catalog": {
                    "tool_key": "omics_expression_review",
                    "recommended_agent_name": "omics_validation_agent",
                    "tool_hint": "omics_expression_review",
                },
            },
        ),
        priority=35,
        requires_human_approval=False,
        quality_gates=[
            "sample_metadata_group_labels_extracted",
            "VIM_or_vimentin_readout_computed_with_direction_and_effect_size",
            "negative_or_null_expression_results_recorded",
        ],
        metadata={
            "evidence_refs": evidence_refs,
            "expected_outputs": [
                "promotion/hold/demote decision",
                "dataset limitations",
                "whether target-expression support is distinct from therapy-response support",
            ],
            "tool_hint": "omics_expression_review",
            "omics_readout_summary": _result_summary(result),
        },
    )
    return AgentRunner(repository).run(
        agent_name=validation_agent_name(item),
        agent_version=VALIDATION_AGENT_VERSION,
        model_profile=request.model_profile,
        input_payload=item.model_dump(mode="json"),
        source_key="omics_readouts",
        dagster_run_id=request.dagster_run_id,
        metadata={"packet_id": result.packet_id, "packet_key": result.packet_key, **request.metadata},
        execute=lambda: run_validation_agent(item, model_profile=request.model_profile),
        summarize=summarize_validation_agent_result,
    )


def _persist_result_artifact(
    repository: ResearchRepository,
    result: OmicsReadoutDatasetResult,
    request: OmicsReadoutRequest,
) -> ArtifactHandle:
    artifact_id = uuid5(
        NAMESPACE_URL,
        f"twog:omics-readout-result:{result.dataset.source_key}:{result.dataset.accession}:{request.program_id}",
    )
    artifact = ArtifactHandle(
        artifact_id=artifact_id,
        artifact_type="omics_readout_result",
        uri=f"twog://omics/readout/{artifact_id}",
        legal_status="derived_from_processed_public_matrix",
        mime_type="application/json",
        metadata={
            "source_key": result.dataset.source_key,
            "accession": result.dataset.accession,
            "program_id": str(request.program_id) if request.program_id else None,
            "therapy_idea_id": str(request.therapy_idea_id) if request.therapy_idea_id else None,
            "dagster_run_id": request.dagster_run_id,
            "result": result.model_dump(mode="json", exclude={"result_artifact_id"}),
        },
    )
    repository.upsert_artifact(artifact)
    return artifact


def _computed_limitations(
    dataset: OmicsEvidenceDataset,
    target: OmicsTargetExpressionScore,
    gene_sets: list[OmicsGeneSetScore],
    labeled_sample_count: int,
    parsed: _ParsedMatrix,
    sample_groups: dict[str, str],
    sample_roles: dict[str, str],
) -> list[str]:
    limitations: list[str] = []
    if labeled_sample_count == 0:
        limitations.append("sample_group_labels_missing_or_unresolved")
    elif not any(score.tumor_sample_count and score.control_sample_count for score in gene_sets):
        limitations.append("tumor_control_comparison_unavailable")
    tumor_count = sum(1 for group in sample_groups.values() if group == "tumor")
    control_count = sum(1 for group in sample_groups.values() if group == "control")
    if 0 < tumor_count < 3:
        limitations.append("tumor_sample_count_low")
    if 0 < control_count < 3:
        limitations.append("control_sample_count_low")
    role_values = set(sample_roles.values())
    if any("knockdown" in role for role in role_values):
        limitations.append("perturbation_context_present_not_primary_tumor_normal")
    if any("_cell_line" in role for role in role_values):
        limitations.append("cell_line_expression_context_not_primary_tissue")
    if dataset.library_strategy and "chro" in dataset.library_strategy.casefold():
        limitations.append("chro_seq_signal_not_steady_state_mrna")
    if any("disease_comparator" in role for role in role_values):
        limitations.append("disease_comparator_not_normal_control")
    if any("glucose_deprivation" in role for role in role_values):
        limitations.append("metabolic_perturbation_context_not_primary_tumor_normal")
    if not target.detected:
        limitations.append("VIM_not_detected_in_matrix_gene_identifiers")
    for score in gene_sets:
        if score.coverage_ratio < 0.5:
            limitations.append(f"{score.gene_set_key}_low_gene_coverage")
    if parsed.skipped_rows:
        limitations.append("some_matrix_rows_skipped_during_parse")
    return limitations


def _comparison_stats(values: dict[str, float], sample_groups: dict[str, str]) -> dict[str, float | int | None]:
    tumor = [value for sample, value in values.items() if sample_groups.get(sample) == "tumor"]
    control = [value for sample, value in values.items() if sample_groups.get(sample) == "control"]
    tumor_mean = _mean(tumor) if tumor else None
    control_mean = _mean(control) if control else None
    delta = tumor_mean - control_mean if tumor_mean is not None and control_mean is not None else None
    effect_size = _cohen_d(tumor, control) if tumor and control else None
    return {
        "tumor_n": len(tumor),
        "control_n": len(control),
        "tumor_mean": tumor_mean,
        "control_mean": control_mean,
        "delta": delta,
        "effect_size": effect_size,
    }


def _support_level(detected: bool, stats: dict[str, float | int | None]) -> str:
    if not detected:
        return "not_detected"
    if not stats["tumor_n"] or not stats["control_n"]:
        return "descriptive_presence"
    if int(stats["tumor_n"]) < 2 or int(stats["control_n"]) < 2:
        return "insufficient_labels"
    delta = stats["delta"]
    if delta is None or abs(float(delta)) < 0.2:
        return "differential_null"
    return "differential_support" if float(delta) > 0 else "differential_null"


def _target_interpretation(support_level: str, stats: dict[str, float | int | None]) -> str:
    if support_level == "differential_support":
        return "VIM is higher in tumor-labeled samples than control-labeled samples in this matrix."
    if support_level == "differential_null":
        return "VIM was detected, but tumor/control separation is weak or negative in this matrix."
    if support_level == "insufficient_labels":
        return "VIM was detected, but comparator group size is too small for a differential claim."
    if support_level == "descriptive_presence":
        return "VIM was detected, but sample labels do not support a tumor/control differential claim."
    return "VIM/vimentin was not detected in the parsed matrix gene identifiers."


def _gene_set_interpretation(gene_set_key: str, support_level: str, stats: dict[str, float | int | None]) -> str:
    if support_level == "differential_support":
        return f"{gene_set_key} is higher in tumor-labeled samples than control-labeled samples."
    if support_level == "differential_null":
        return f"{gene_set_key} was detected, but tumor/control separation is weak or negative."
    if support_level == "insufficient_labels":
        return f"{gene_set_key} genes were detected, but comparator group size is too small for a differential claim."
    if support_level == "descriptive_presence":
        return f"{gene_set_key} genes were detected, but labels do not support a differential claim."
    return f"{gene_set_key} genes were not detected in the parsed matrix."


def _sample_groups(
    dataset: OmicsEvidenceDataset,
    samples: list[str],
    request: OmicsReadoutRequest,
) -> dict[str, str]:
    overrides = request.sample_group_overrides.get(dataset.accession, {})
    metadata_groups = dataset.metadata.get("sample_groups") if isinstance(dataset.metadata, dict) else None
    if isinstance(metadata_groups, dict):
        overrides = {
            **{str(key): str(value) for key, value in metadata_groups.items()},
            **overrides,
        }
    groups: dict[str, str] = {}
    sample_titles = dataset.metadata.get("sample_titles") if isinstance(dataset.metadata, dict) else None
    title_by_sample = {}
    if isinstance(sample_titles, dict):
        title_by_sample = {str(key): str(value) for key, value in sample_titles.items()}
    elif isinstance(sample_titles, list):
        titles = [str(value) for value in sample_titles]
        if len(dataset.sample_accessions) == len(titles):
            title_by_sample.update(dict(zip(dataset.sample_accessions, titles, strict=False)))
    for sample in samples:
        if sample in overrides:
            groups[sample] = _normalize_group(overrides[sample])
            continue
        text = f"{sample} {title_by_sample.get(sample, '')}".casefold()
        group = _infer_group(text)
        if group == "unknown":
            group = _infer_group(_fallback_sample_role(dataset, sample))
        groups[sample] = group
    return groups


def _sample_roles(dataset: OmicsEvidenceDataset, samples: list[str]) -> dict[str, str]:
    sample_titles = dataset.metadata.get("sample_titles") if isinstance(dataset.metadata, dict) else None
    title_by_sample = {}
    if isinstance(sample_titles, dict):
        title_by_sample = {str(key): str(value) for key, value in sample_titles.items()}
    elif isinstance(sample_titles, list) and len(dataset.sample_accessions) == len(sample_titles):
        title_by_sample = dict(zip(dataset.sample_accessions, [str(value) for value in sample_titles], strict=False))
    roles: dict[str, str] = {}
    for sample in samples:
        role = _infer_role(f"{sample} {title_by_sample.get(sample, '')}".casefold())
        if role == "unknown":
            role = _fallback_sample_role(dataset, sample)
        roles[sample] = role
    return roles


def _fallback_sample_role(dataset: OmicsEvidenceDataset, sample: str) -> str:
    accession = dataset.accession.upper()
    sample_text = sample.casefold()
    if accession == "GSE203215":
        return "human_angiosarcoma_tumor_context"
    if dataset.disease_context == "human_angiosarcoma":
        return "human_angiosarcoma_tumor_context"
    if dataset.disease_context == "canine_hsa" and not re.search(r"\bnm\b", sample_text):
        return "hsa_tumor_context"
    return "unknown"


def _infer_role(text: str) -> str:
    if re.search(r"\b[a-z]\d+nm\b", text) or re.search(r"\bnm\b", text) or "normal tissue" in text:
        return "normal_tissue_control"
    if re.search(r"\bec\b", text) or "endothelial" in text or "normal" in text or "healthy" in text:
        return "normal_endothelial_control"
    if "0mmglc" in text or "0mm glc" in text or "without glucose" in text or "glucose deprivation" in text:
        return "hsa_glucose_deprivation_cell_line"
    if "25mmglc" in text or "25mm glc" in text or "with glucose" in text:
        return "hsa_glucose_control_cell_line"
    if "scr" in text:
        return "hsa_scramble_control_cell_line"
    if "sha" in text or "knockdown" in text or "shrna" in text:
        return "hsa_knockdown_cell_line"
    if "jub" in text:
        return "hsa_cell_line_or_tumor"
    if "human_angiosarcoma_tumor_context" in text:
        return "human_angiosarcoma_tumor_context"
    if any(term in text for term in _DISEASE_COMPARATOR_TERMS):
        return "disease_comparator_control"
    if any(term in text for term in _TUMOR_TERMS):
        return "hsa_tumor_context"
    return "unknown"


def _comparison_design(sample_roles: dict[str, str]) -> str:
    roles = set(sample_roles.values())
    if any("knockdown" in role for role in roles) and any("scramble" in role for role in roles):
        return "perturbation_cell_line_vs_scramble_and_endothelial_context"
    if any("glucose_deprivation" in role for role in roles) and any("glucose_control" in role for role in roles):
        return "metabolic_perturbation_hsa_cell_line_context"
    if "disease_comparator_control" in roles and "hsa_tumor_context" in roles:
        return "hsa_vs_other_disease_comparator"
    if roles == {"human_angiosarcoma_tumor_context"}:
        return "human_angiosarcoma_descriptive_tumor_only"
    if any("_cell_line" in role for role in roles) and "normal_endothelial_control" in roles:
        return "hsa_cell_line_vs_endothelial_control"
    if "normal_tissue_control" in roles and "hsa_tumor_context" in roles:
        return "primary_tumor_vs_normal_tissue"
    if "normal_endothelial_control" in roles:
        return "tumor_or_cell_line_vs_endothelial_control"
    return "descriptive_or_unresolved_sample_context"


def _infer_group(text: str) -> str:
    role = _infer_role(text)
    if role in {"normal_endothelial_control", "normal_tissue_control", "disease_comparator_control"}:
        return "control"
    if any(term in text for term in _CONTROL_TERMS):
        return "control"
    if role in {
        "hsa_scramble_control_cell_line",
        "hsa_knockdown_cell_line",
        "hsa_cell_line_or_tumor",
        "hsa_tumor_context",
        "human_angiosarcoma_tumor_context",
        "hsa_glucose_deprivation_cell_line",
        "hsa_glucose_control_cell_line",
    }:
        return "tumor"
    if any(term in text for term in _TUMOR_TERMS):
        return "tumor"
    return "unknown"


def _normalize_group(value: str) -> str:
    normalized = value.strip().casefold()
    if normalized in {"tumor", "case", "disease", "hsa", "angiosarcoma"}:
        return "tumor"
    if normalized in {"control", "normal", "healthy", "reference"}:
        return "control"
    return normalized or "unknown"


def _mean_by_sample(value_maps: list[dict[str, float]], samples: list[str]) -> dict[str, float]:
    sample_values: dict[str, float] = {}
    for sample in samples:
        values = [value_map[sample] for value_map in value_maps if sample in value_map]
        if values:
            sample_values[sample] = _mean(values)
    return sample_values


def _zscore_by_sample(values: dict[str, float], samples: list[str]) -> dict[str, float]:
    present = [values[sample] for sample in samples if sample in values]
    mean = _mean(present) if present else 0.0
    sd = _std(present)
    if sd == 0.0:
        return {sample: 0.0 for sample in samples if sample in values}
    return {sample: (values[sample] - mean) / sd for sample in samples if sample in values}


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = _mean(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / (len(values) - 1))


def _cohen_d(left: list[float], right: list[float]) -> float | None:
    if len(left) < 2 or len(right) < 2:
        return None
    pooled_n = len(left) + len(right) - 2
    if pooled_n <= 0:
        return None
    left_var = _std(left) ** 2
    right_var = _std(right) ** 2
    pooled = math.sqrt(((len(left) - 1) * left_var + (len(right) - 1) * right_var) / pooled_n)
    if pooled == 0.0:
        return None
    return (_mean(left) - _mean(right)) / pooled


def _round(value: float | int | None) -> float | None:
    return round(float(value), 4) if value is not None else None


def _target_symbols(target_terms: list[str]) -> list[str]:
    symbols = ["VIM"]
    for term in target_terms:
        normalized = _gene_symbol(term)
        if normalized in {"VIM", "VIMENTIN"}:
            symbols.append("VIM")
        elif normalized:
            symbols.append(normalized)
    return _dedupe_symbols(symbols)


def _series_matrix_table(text: str) -> str:
    lines = text.splitlines()
    begin = None
    end = None
    for index, line in enumerate(lines):
        if line.strip() == "!series_matrix_table_begin":
            begin = index + 1
        elif line.strip() == "!series_matrix_table_end":
            end = index
            break
    if begin is not None:
        return "\n".join(lines[begin:end])
    return text


def _header_index(rows: list[list[str]]) -> int | None:
    for index, row in enumerate(rows):
        if len(row) < 2:
            continue
        normalized = [_normalize_header(cell) for cell in row]
        if any(cell in _GENE_COLUMN_CANDIDATES for cell in normalized):
            return index
        if sum(1 for cell in row[1:] if cell) >= 2:
            numeric_cols = _numeric_columns(rows[index + 1 :], 0, row)
            if numeric_cols:
                return index
    return None


def _gene_column_index(header: list[str]) -> int:
    normalized = [_normalize_header(cell) for cell in header]
    for candidate in _GENE_COLUMN_CANDIDATES:
        if candidate in normalized:
            return normalized.index(candidate)
    return 0


def _numeric_columns(rows: list[list[str]], gene_col: int, header: list[str] | None = None) -> list[int]:
    if not rows:
        return []
    width = max(len(row) for row in rows)
    normalized_header = [_normalize_header(cell) for cell in header] if header else []
    columns: list[int] = []
    for index in range(width):
        if index == gene_col:
            continue
        if index < len(normalized_header) and normalized_header[index] in _NON_SAMPLE_COLUMN_HEADERS:
            continue
        checked = 0
        numeric = 0
        for row in rows[:100]:
            if index >= len(row) or not row[index]:
                continue
            checked += 1
            if _float_or_none(row[index]) is not None:
                numeric += 1
        if checked and numeric / checked >= 0.6:
            columns.append(index)
    return columns


def _gene_symbols(value: str) -> list[str]:
    symbols = [_gene_symbol(part) for part in re.split(r"///|//|;|,|\|", value)]
    return _dedupe_symbols([symbol for symbol in symbols if symbol])


def _mapped_gene_symbols(
    raw_genes: list[str],
    gene_symbol_lookup: dict[str, list[str]],
) -> list[str]:
    mapped: list[str] = []
    for gene in raw_genes:
        mapped.extend(gene_symbol_lookup.get(gene, []))
        if gene not in gene_symbol_lookup:
            mapped.append(gene)
    return _dedupe_symbols(mapped)


def _gene_symbol(value: str) -> str:
    text = re.sub(r"\s+", " ", str(value)).strip().upper()
    text = re.sub(r"[^A-Z0-9_.-]", "", text)
    if not text or text in {"NA", "N/A", "NULL", "---", "ID_REF"}:
        return ""
    if text == "VIMENTIN":
        return "VIM"
    return text


def _dedupe_symbols(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value and value not in seen:
            deduped.append(value)
            seen.add(value)
    return deduped


def _looks_count_like(values: list[float]) -> bool:
    if not values:
        return False
    nonnegative = all(value >= 0 for value in values)
    integer_like = sum(1 for value in values if abs(value - round(value)) < 1e-6) / len(values)
    return nonnegative and integer_like >= 0.85 and max(values) > 50


def _clean_cell(value: str) -> str:
    return str(value).strip().strip('"').strip("'")


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", _clean_cell(value).casefold()).strip("_")


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() in {"NA", "NAN", "NULL", "INF", "-INF"}:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _metadata_text(metadata: dict) -> str | None:
    for key in ("matrix_text", "processed_matrix_text", "expression_matrix_text", "content"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip("\n\r ")
    return None


def _platform_probe_mapping(
    repository: ResearchRepository,
    dataset: OmicsEvidenceDataset,
    request: OmicsReadoutRequest,
) -> dict[str, list[str]]:
    obj = repository.get_research_object(dataset.research_object_id) if dataset.research_object_id else None
    metadata = {**dataset.metadata, **((obj.metadata if obj else {}) or {})}
    merged_mapping: dict[str, list[str]] = {}
    for key in _GENE_SYMBOL_MAP_KEYS:
        explicit_gene_map = metadata.get(key)
        if isinstance(explicit_gene_map, dict):
            merged_mapping.update(_normalize_probe_map(explicit_gene_map))
    explicit_map = metadata.get("platform_probe_map")
    if isinstance(explicit_map, dict):
        merged_mapping.update(_normalize_probe_map(explicit_map))
    annotation_text = metadata.get("platform_annotation_text")
    if isinstance(annotation_text, str) and annotation_text.strip():
        merged_mapping.update(_parse_platform_annotation(annotation_text))
    platform_accessions = _string_list(metadata.get("platform_accessions"))
    for platform in platform_accessions:
        if not platform.upper().startswith("GPL"):
            platform = f"GPL{platform}"
        mapping = _download_platform_mapping(repository, platform, request)
        if mapping:
            merged_mapping.update(mapping)
            break
    if _should_attempt_canine_legacy_mapping(dataset, metadata):
        legacy_mapping = _download_canine_legacy_ensembl_mapping(repository, request)
        if legacy_mapping:
            merged_mapping.update({key: value for key, value in legacy_mapping.items() if key not in merged_mapping})
    return merged_mapping


def _mapping_sources(mapping: dict[str, list[str]]) -> list[str]:
    sources: list[str] = []
    if any(key.startswith("ENSCAFG") for key in mapping):
        sources.append("ensembl_or_platform_gene_id")
    if mapping:
        sources.append("explicit_or_downloaded_gene_symbol_map")
    return _dedupe_strings(sources)


def _download_platform_mapping(
    repository: ResearchRepository,
    platform_accession: str,
    request: OmicsReadoutRequest,
) -> dict[str, list[str]]:
    uri = _geo_platform_annotation_uri(platform_accession)
    try:
        data, persisted_uri, file_name = _read_uri_bytes(uri, request.artifact_dir, platform_accession)
        text = _decode_matrix_bytes(data, file_name)
    except (OSError, UnicodeDecodeError, urllib.error.URLError, TimeoutError, gzip.BadGzipFile):
        return {}
    mapping = _parse_platform_annotation(text)
    if mapping and not request.dry_run:
        artifact_id = uuid5(NAMESPACE_URL, f"twog:omics-platform-annotation:{platform_accession}:{uri}")
        repository.upsert_artifact(
            ArtifactHandle(
                artifact_id=artifact_id,
                artifact_type="omics_platform_annotation",
                uri=persisted_uri,
                legal_status="public_geo_platform_annotation",
                mime_type="text/tab-separated-values",
                metadata={
                    "platform_accession": platform_accession,
                    "source": "geo_platform_annotation",
                    "original_uri": uri,
                    "probe_mapping_count": len(mapping),
                    "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                    "size_bytes": len(text.encode("utf-8")),
                },
            )
        )
    return mapping


def _download_canine_legacy_ensembl_mapping(
    repository: ResearchRepository,
    request: OmicsReadoutRequest,
) -> dict[str, list[str]]:
    query = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Query>
<Query virtualSchemaName="default" formatter="TSV" header="1" uniqueRows="1" count="" datasetConfigVersion="0.6">
  <Dataset name="clfamiliaris_gene_ensembl" interface="default">
    <Attribute name="ensembl_gene_id" />
    <Attribute name="external_gene_name" />
    <Attribute name="entrezgene_id" />
  </Dataset>
</Query>"""
    uri = f"{_CANINE_LEGACY_ENSEMBL_BIOMART_URI}?query={urllib.parse.quote(query)}"
    try:
        data, persisted_uri, file_name = _read_uri_bytes(uri, request.artifact_dir, "canine_legacy_ensembl")
        text = _decode_matrix_bytes(data, file_name)
    except (OSError, UnicodeDecodeError, urllib.error.URLError, TimeoutError):
        return {}
    mapping = _parse_ensembl_symbol_table(text)
    if mapping and not request.dry_run:
        artifact_id = uuid5(NAMESPACE_URL, f"twog:omics-gene-map:canine-legacy-ensembl:{_CANINE_LEGACY_ENSEMBL_BIOMART_URI}")
        repository.upsert_artifact(
            ArtifactHandle(
                artifact_id=artifact_id,
                artifact_type="omics_gene_identifier_map",
                uri=persisted_uri,
                legal_status="public_ensembl_archive_biomart",
                mime_type="text/tab-separated-values",
                metadata={
                    "species": "Canis lupus familiaris",
                    "source": "ensembl_archive_biomart",
                    "original_uri": _CANINE_LEGACY_ENSEMBL_BIOMART_URI,
                    "gene_mapping_count": len(mapping),
                    "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                    "size_bytes": len(text.encode("utf-8")),
                },
            )
        )
    return mapping


def _parse_ensembl_symbol_table(text: str) -> dict[str, list[str]]:
    rows = [
        [_clean_cell(cell) for cell in row]
        for row in csv.reader(io.StringIO(text), delimiter="\t")
        if row and any(str(cell).strip() for cell in row)
    ]
    if not rows:
        return {}
    header = [_normalize_header(cell) for cell in rows[0]]
    gene_id_col = _first_header_index(header, {"gene_stable_id", "ensembl_gene_id", "id"})
    symbol_col = _first_header_index(header, {"gene_name", "external_gene_name", "gene_symbol", "symbol"})
    if gene_id_col is None or symbol_col is None:
        return {}
    mapping: dict[str, list[str]] = {}
    for row in rows[1:]:
        if len(row) <= max(gene_id_col, symbol_col):
            continue
        gene_id = _gene_symbol(row[gene_id_col])
        symbols = _gene_symbols(row[symbol_col])
        if gene_id and symbols:
            mapping[gene_id] = symbols
    return mapping


def _first_header_index(header: list[str], candidates: set[str]) -> int | None:
    for index, value in enumerate(header):
        if value in candidates:
            return index
    return None


def _parse_platform_annotation(text: str) -> dict[str, list[str]]:
    rows = [
        [_clean_cell(cell) for cell in row]
        for row in csv.reader(io.StringIO(text), delimiter="\t")
        if row and not str(row[0]).startswith(("#", "!", "^"))
    ]
    header_index = None
    id_col = None
    symbol_col = None
    for index, row in enumerate(rows[:200]):
        normalized = [_normalize_header(cell) for cell in row]
        if "id" not in normalized:
            continue
        symbol_candidates = [
            column
            for column, header in enumerate(normalized)
            if header in {"gene_symbol", "gene_symbols", "symbol", "gene_assignment", "gene_symbol_s"}
            or ("gene" in header and "symbol" in header)
        ]
        if symbol_candidates:
            header_index = index
            id_col = normalized.index("id")
            symbol_col = symbol_candidates[0]
            break
    if header_index is None or id_col is None or symbol_col is None:
        return {}
    mapping: dict[str, list[str]] = {}
    for row in rows[header_index + 1 :]:
        if len(row) <= max(id_col, symbol_col):
            continue
        probe_id = _gene_symbol(row[id_col])
        symbols = _gene_symbols(row[symbol_col])
        if probe_id and symbols:
            mapping[probe_id] = symbols
    return mapping


def _normalize_probe_map(raw: dict) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for key, value in raw.items():
        probe_id = _gene_symbol(str(key))
        if not probe_id:
            continue
        values = value if isinstance(value, list) else [value]
        symbols = _dedupe_symbols([symbol for item in values for symbol in _gene_symbols(str(item))])
        if symbols:
            mapping[probe_id] = symbols
    return mapping


def _is_canine_dataset(dataset: OmicsEvidenceDataset) -> bool:
    organism = " ".join(
        [
            dataset.organism or "",
            str(dataset.metadata.get("organism") or ""),
            str(dataset.metadata.get("taxon") or ""),
        ]
    ).casefold()
    return "canis" in organism or "canine" in organism or "dog" in organism


def _should_attempt_canine_legacy_mapping(dataset: OmicsEvidenceDataset, metadata: dict) -> bool:
    text = " ".join(
        [
            dataset.library_strategy or "",
            str(metadata.get("dataset_type") or ""),
            dataset.title or "",
        ]
    ).casefold()
    if not _is_canine_dataset(dataset):
        return False
    return "rna" in text or "sequencing" in text or "high throughput" in text


def _artifact_id_from_metadata(metadata: dict) -> UUID | None:
    for key in ("matrix_artifact_id", "processed_matrix_artifact_id", "artifact_id"):
        value = metadata.get(key)
        if not value:
            continue
        try:
            return UUID(str(value))
        except ValueError:
            continue
    return None


def _matrix_uris_from_metadata(metadata: dict) -> list[str]:
    values: list[str] = []
    for key in ("matrix_uri", "processed_matrix_uri", "expression_matrix_uri", "matrix_url", "processed_matrix_url"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    supplementary = metadata.get("supplementary_files")
    if isinstance(supplementary, list):
        for item in supplementary:
            if isinstance(item, dict):
                uri = str(item.get("url") or item.get("uri") or "").strip()
                file_name = str(item.get("name") or uri).casefold()
            else:
                uri = str(item).strip()
                file_name = uri.casefold()
            if uri and _is_processed_matrix_uri(file_name):
                values.append(uri)
    return _rank_matrix_uris(_dedupe_strings(values))


def _geo_supplementary_matrix_uris(accession: str) -> list[str]:
    normalized = accession.upper()
    base_uri = _geo_series_base_uri(normalized) + "suppl/"
    try:
        with urllib.request.urlopen(base_uri, timeout=20) as response:
            html = response.read().decode("utf-8", errors="replace")
    except (OSError, urllib.error.URLError, TimeoutError):
        return []
    uris: list[str] = []
    for href in re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE):
        decoded = urllib.parse.unquote(href)
        if decoded.startswith(("../", "/")):
            continue
        lower = decoded.casefold()
        if _is_processed_matrix_uri(lower):
            uris.append(urllib.parse.urljoin(base_uri, href))
    return _rank_matrix_uris(_dedupe_strings(uris))


def _is_processed_matrix_uri(value: str) -> bool:
    lower = value.casefold()
    name = lower.rsplit("/", 1)[-1]
    if (
        name in {"filelist.txt", "md5sum.txt", "readme.txt"}
        or name.startswith("filelist.")
        or name.startswith("md5")
        or "readme" in name
        or "summary" in name
    ):
        return False
    return lower.endswith(_PROCESSED_FILE_EXTENSIONS) or lower.endswith(tuple(ext + ".gz" for ext in _PROCESSED_FILE_EXTENSIONS))


def _rank_matrix_uris(uris: list[str]) -> list[str]:
    return sorted(uris, key=lambda uri: (-_matrix_uri_score(uri), uri))


def _matrix_uri_score(uri: str) -> int:
    name = urllib.parse.unquote(uri).rsplit("/", 1)[-1].casefold()
    score = 0
    if "gsimport" in name:
        score += 60
    if "rpm" in name or "tpm" in name:
        score += 25
    if "bcmatrix" in name or "matrix" in name:
        score += 20
    if "geneexpression" in name or "expression" in name:
        score += 15
    if "raw" in name or "count" in name:
        score += 5
    if "summary" in name:
        score -= 100
    return score


def _raw_limitations(dataset: OmicsEvidenceDataset) -> list[str]:
    limitations = list(dataset.limitations)
    file_types = {item.casefold() for item in dataset.supplementary_file_types}
    if dataset.source_key == "sra" or dataset.run_accessions:
        limitations.append("raw_sra_reprocessing_required")
    if any(file_type.casefold() in {"cel", "fastq", "bam", "sra"} for file_type in file_types):
        limitations.append("raw_or_probe_level_file_requires_later_reprocessing_lane")
    if _is_chro_seq_bigwig_dataset(dataset):
        limitations.append("chro_seq_bigwig_locus_extraction_required")
    return _dedupe_strings(limitations)


def _skip_reason(dataset: OmicsEvidenceDataset) -> str:
    file_types = {item.casefold() for item in dataset.supplementary_file_types}
    if dataset.source_key == "sra" or dataset.run_accessions:
        return "raw_sra_reprocessing_required"
    if any(file_type in {ext.strip(".") for ext in _RAW_FILE_EXTENSIONS} for file_type in file_types):
        return "raw_or_probe_level_reprocessing_required"
    if _is_chro_seq_bigwig_dataset(dataset):
        return "chro_seq_bigwig_locus_extraction_required"
    return "processed_matrix_not_found"


def _requires_locus_signal_extractor(
    dataset: OmicsEvidenceDataset,
    request: OmicsReadoutRequest,
) -> bool:
    if dataset.accession in request.matrix_uri_by_accession:
        return False
    if _metadata_text(dataset.metadata):
        return False
    return _is_chro_seq_bigwig_dataset(dataset)


def _locus_signal_metadata(dataset: OmicsEvidenceDataset) -> dict:
    sample_roles = _sample_roles(dataset, dataset.sample_accessions)
    sample_groups = _sample_groups(dataset, dataset.sample_accessions, OmicsReadoutRequest(dry_run=True))
    return {
        "runner_status": "recommend_only",
        "recommended_next_path": "bigwig_locus_signal_extractor",
        "required_inputs": [
            "bigWig files or indexed signal tracks",
            "target gene genomic locus for the matching canine genome build",
            "sample-to-group labels",
        ],
        "target_loci": {
            "VIM": {
                "status": "requires_genome_build_specific_lookup",
                "accepted_builds": ["CanFam3.1", "ROS_Cfam_1.0", "Dog10K_Boxer_Tasha"],
            }
        },
        "sample_count": len(dataset.sample_accessions),
        "labeled_sample_count": sum(1 for group in sample_groups.values() if group in {"tumor", "control"}),
        "tumor_sample_count": sum(1 for group in sample_groups.values() if group == "tumor"),
        "control_sample_count": sum(1 for group in sample_groups.values() if group == "control"),
        "sample_roles": sample_roles,
        "comparison_design": _comparison_design(sample_roles),
    }


def _is_chro_seq_bigwig_dataset(dataset: OmicsEvidenceDataset) -> bool:
    text = " ".join(
        [
            dataset.library_strategy or "",
            dataset.title or "",
            " ".join(dataset.readout_hints),
            " ".join(dataset.supplementary_file_types),
        ]
    ).casefold()
    return ("chro" in text or "bigwig" in text or " bw" in f" {text}") and (
        "bw" in {item.casefold() for item in dataset.supplementary_file_types}
        or "bigwig" in text
        or "chro" in text
    )


def _geo_series_matrix_uri(accession: str) -> str:
    normalized = accession.upper()
    return f"{_geo_series_base_uri(normalized)}matrix/{normalized}_series_matrix.txt.gz"


def _geo_series_base_uri(accession: str) -> str:
    normalized = accession.upper()
    series_bucket = f"{normalized[:-3]}nnn" if len(normalized) > 3 else normalized
    return f"https://ftp.ncbi.nlm.nih.gov/geo/series/{series_bucket}/{normalized}/"


def _geo_platform_annotation_uri(platform_accession: str) -> str:
    normalized = platform_accession.upper()
    platform_bucket = f"{normalized[:-3]}nnn" if len(normalized) > 3 else normalized
    return f"https://ftp.ncbi.nlm.nih.gov/geo/platforms/{platform_bucket}/{normalized}/annot/{normalized}.annot.gz"


def _safe_file_name(accession: str, uri: str) -> str:
    raw_name = uri.rsplit("/", 1)[-1] or f"{accession}_matrix.txt"
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", raw_name)[:180]
    if accession.upper() not in safe_name.upper():
        safe_name = f"{accession}_{safe_name}"
    return safe_name


def _mime_type(file_name: str) -> str:
    lower = file_name.casefold()
    if lower.endswith(".csv"):
        return "text/csv"
    if lower.endswith((".tsv", ".txt", ".gz")):
        return "text/tab-separated-values"
    return "application/octet-stream"


def _xlsx_to_tsv(data: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(data)) as workbook:
        shared_strings = _xlsx_shared_strings(workbook)
        sheet_names = sorted(
            name
            for name in workbook.namelist()
            if re.match(r"xl/worksheets/sheet\d+\.xml$", name)
        )
        best_rows: list[list[str]] = []
        best_score = -1
        for sheet_name in sheet_names:
            rows = _xlsx_sheet_rows(workbook, sheet_name, shared_strings)
            score = _matrix_row_score(rows)
            if score > best_score:
                best_score = score
                best_rows = rows
    return "\n".join("\t".join(row) for row in best_rows)


def _xlsx_shared_strings(workbook: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in workbook.namelist():
        return []
    root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
    shared: list[str] = []
    for item in root:
        texts = [
            node.text or ""
            for node in item.iter()
            if node.tag.endswith("}t") or node.tag == "t"
        ]
        shared.append("".join(texts))
    return shared


def _xlsx_sheet_rows(
    workbook: zipfile.ZipFile,
    sheet_name: str,
    shared_strings: list[str],
) -> list[list[str]]:
    root = ET.fromstring(workbook.read(sheet_name))
    rows: list[list[str]] = []
    for row_node in root.iter():
        if not row_node.tag.endswith("}row") and row_node.tag != "row":
            continue
        cells: dict[int, str] = {}
        for cell_node in row_node:
            if not cell_node.tag.endswith("}c") and cell_node.tag != "c":
                continue
            column = _xlsx_column_index(cell_node.attrib.get("r", ""))
            if column is None:
                column = len(cells)
            cells[column] = _xlsx_cell_value(cell_node, shared_strings)
        if cells:
            rows.append([cells.get(index, "") for index in range(max(cells) + 1)])
    return rows


def _xlsx_cell_value(cell_node: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell_node.attrib.get("t")
    if cell_type == "inlineStr":
        texts = [
            node.text or ""
            for node in cell_node.iter()
            if node.tag.endswith("}t") or node.tag == "t"
        ]
        return "".join(texts).strip()
    value_node = next(
        (
            node
            for node in cell_node
            if node.tag.endswith("}v") or node.tag == "v"
        ),
        None,
    )
    if value_node is None or value_node.text is None:
        return ""
    value = value_node.text.strip()
    if cell_type == "s":
        try:
            return shared_strings[int(value)].strip()
        except (ValueError, IndexError):
            return ""
    return value


def _xlsx_column_index(reference: str) -> int | None:
    match = re.match(r"([A-Za-z]+)", reference)
    if not match:
        return None
    index = 0
    for char in match.group(1).upper():
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index - 1


def _matrix_row_score(rows: list[list[str]]) -> int:
    score = 0
    for row in rows[:500]:
        normalized = {_normalize_header(cell) for cell in row}
        if normalized & set(_GENE_COLUMN_CANDIDATES):
            score += 200
        score += sum(1 for cell in row if _float_or_none(cell) is not None)
    return score


def _dedupe_datasets(datasets: list[OmicsEvidenceDataset]) -> list[OmicsEvidenceDataset]:
    seen: set[str] = set()
    deduped: list[OmicsEvidenceDataset] = []
    for dataset in datasets:
        key = f"{dataset.source_key}:{dataset.identifier_type}:{dataset.accession}".casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dataset)
    return deduped


def _dedupe_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = re.sub(r"\s+", " ", str(value)).strip()
        key = text.casefold()
        if text and key not in seen:
            deduped.append(text)
            seen.add(key)
    return deduped


def _string_list(value: object) -> list[str]:
    if isinstance(value, list):
        return _dedupe_strings([str(item) for item in value if str(item).strip()])
    if value:
        return [str(value)]
    return []


def _result_summary(result: OmicsReadoutResult) -> dict[str, object]:
    return {
        "dataset_count": result.dataset_count,
        "computed_count": result.computed_count,
        "skipped_count": result.skipped_count,
        "failed_count": result.failed_count,
        "artifact_ids": [str(artifact_id) for artifact_id in result.artifact_ids],
        "datasets": [
            {
                "accession": item.dataset.accession,
                "status": item.status,
                "target_support": item.target_expression.support_level if item.target_expression else None,
                "target_effect_size": item.target_expression.effect_size if item.target_expression else None,
                "limitations": item.limitations,
            }
            for item in result.datasets
        ],
    }
