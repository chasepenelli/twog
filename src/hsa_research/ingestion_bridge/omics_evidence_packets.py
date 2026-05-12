"""Build omics evidence packets from accession-level GEO/SRA metadata."""

from __future__ import annotations

import re
from uuid import NAMESPACE_URL, uuid5

from .contracts import (
    OmicsEvidenceDataset,
    OmicsEvidencePacket,
    OmicsEvidencePacketRequest,
    OmicsEvidencePacketResult,
    ResearchObject,
)
from .repository import ResearchRepository


def build_omics_evidence_packets(
    repository: ResearchRepository,
    request: OmicsEvidencePacketRequest,
) -> OmicsEvidencePacketResult:
    """Package stored GEO/SRA accessions into finite omics review packets."""

    result = OmicsEvidencePacketResult(program_id=request.program_id, dry_run=request.dry_run)
    datasets: list[OmicsEvidenceDataset] = []
    accession_filter = {accession.casefold() for accession in request.accessions}

    for source_key in request.source_keys:
        for obj in repository.list_research_objects(source_key=source_key, limit=request.limit):
            result.scanned_dataset_count += 1
            accession, identifier_type = _primary_accession(obj)
            if not accession:
                result.skipped.append(
                    {
                        "source_key": source_key,
                        "research_object_id": str(obj.id),
                        "reason": "missing_primary_accession",
                        "title": obj.title,
                    }
                )
                continue
            if accession_filter and accession.casefold() not in accession_filter:
                continue
            dataset = _dataset_from_object(obj, accession, identifier_type, request)
            if not dataset.matched_terms:
                result.skipped.append(
                    {
                        "source_key": source_key,
                        "accession": accession,
                        "reason": "no_request_term_match",
                        "title": obj.title,
                    }
                )
                continue
            datasets.append(dataset)

    datasets = _dedupe_datasets(datasets)
    result.selected_dataset_count = len(datasets)
    result.direct_dataset_count = sum(1 for dataset in datasets if dataset.evidence_role == "direct")
    result.analog_dataset_count = sum(1 for dataset in datasets if dataset.evidence_role == "analog")
    result.context_dataset_count = sum(1 for dataset in datasets if dataset.evidence_role == "context")

    packets = _build_packets(datasets, request)
    result.packets = packets
    result.packet_count = len(packets)
    return result


def _build_packets(
    datasets: list[OmicsEvidenceDataset],
    request: OmicsEvidencePacketRequest,
) -> list[OmicsEvidencePacket]:
    grouped: dict[str, list[OmicsEvidenceDataset]] = {
        "canine_hsa": [dataset for dataset in datasets if dataset.disease_context == "canine_hsa"],
        "human_angiosarcoma": [
            dataset for dataset in datasets if dataset.disease_context == "human_angiosarcoma"
        ],
    }
    if request.include_context_packet:
        grouped["comparative_context"] = [
            dataset for dataset in datasets if dataset.disease_context == "comparative_context"
        ]

    packets: list[OmicsEvidencePacket] = []
    for packet_key, packet_datasets in grouped.items():
        if len(packet_datasets) < request.min_datasets_per_packet:
            continue
        packets.append(_packet_from_datasets(packet_key, packet_datasets, request))
    return packets


def _packet_from_datasets(
    packet_key: str,
    datasets: list[OmicsEvidenceDataset],
    request: OmicsEvidencePacketRequest,
) -> OmicsEvidencePacket:
    accessions = [dataset.accession for dataset in datasets]
    total_sample_count = sum(
        dataset.sample_count for dataset in datasets if dataset.sample_count is not None
    )
    source_keys = sorted({dataset.source_key for dataset in datasets})
    direct_count = sum(1 for dataset in datasets if dataset.evidence_role == "direct")
    analog_count = sum(1 for dataset in datasets if dataset.evidence_role == "analog")
    matrix_ready_count = sum(1 for dataset in datasets if _has_matrix_hint(dataset))
    exact_target_hits = [
        dataset.accession
        for dataset in datasets
        if any(term.casefold() in {"vim", "vimentin"} for term in dataset.matched_terms)
    ]
    readiness = "ready_for_omics_review" if matrix_ready_count else "needs_matrix_retrieval"
    if not datasets:
        readiness = "needs_more_accessions"
    score = min(
        1.0,
        0.25
        + min(len(datasets), 10) * 0.05
        + min(matrix_ready_count, 6) * 0.06
        + (0.12 if exact_target_hits else 0.0)
        + (0.08 if direct_count else 0.0),
    )
    title = {
        "canine_hsa": "Canine HSA omics packet for VIM/vimentin evidence review",
        "human_angiosarcoma": "Human angiosarcoma analog omics packet for VIM/vimentin transfer review",
        "comparative_context": "Comparative omics context packet for vascular injury and mesenchymal state review",
    }.get(packet_key, f"Omics evidence packet: {packet_key}")

    negative_coverage = []
    if not exact_target_hits:
        negative_coverage.append(
            "No accession metadata matched VIM/vimentin text directly; expression matrix review is required."
        )
    if matrix_ready_count < len(datasets):
        negative_coverage.append(
            "Some datasets lack obvious matrix/raw file hints in metadata and need manual accession inspection."
        )

    blockers = [
        "expression_matrix_or_raw_counts_required",
        "sample_group_labels_required",
        "species_gene_identifier_mapping_required",
    ]
    if packet_key == "human_angiosarcoma":
        blockers.append("cross_species_transfer_assumption_must_be_explicit")

    return OmicsEvidencePacket(
        packet_id=_packet_id(packet_key, accessions, request),
        packet_key=packet_key,
        program_id=request.program_id,
        title=title,
        topic_query=request.topic_query,
        target_terms=request.gene_symbols,
        disease_terms=request.disease_terms,
        source_keys=source_keys,
        datasets=datasets,
        dataset_count=len(datasets),
        direct_dataset_count=direct_count,
        analog_dataset_count=analog_count,
        total_sample_count=total_sample_count or None,
        accessions=accessions,
        bioprojects=[dataset.bioproject for dataset in datasets if dataset.bioproject],
        pmids=[dataset.pmid for dataset in datasets if dataset.pmid],
        decisive_questions=_decisive_questions(packet_key),
        proposed_readouts=_proposed_readouts(packet_key, request.gene_symbols),
        quality_gates=[
            "accession_resolves_to_public_metadata",
            "sample_metadata_group_labels_extracted",
            "expression_matrix_or_raw_counts_available",
            "VIM_or_vimentin_readout_computed_with_direction_and_effect_size",
            "negative_or_null_expression_results_recorded",
        ],
        dispatch_blockers=blockers,
        negative_coverage=negative_coverage,
        next_actions=[
            "Retrieve expression matrices or raw count files for each accession.",
            "Extract sample labels into tumor/control, tissue/site, treatment, and species columns.",
            "Compute VIM expression, epithelial/mesenchymal and vascular injury gene-set scores.",
            "Route computed readouts to omics_validation_agent with null findings included.",
        ],
        readiness=readiness,
        score=round(score, 3),
        summary={
            "matrix_ready_dataset_count": matrix_ready_count,
            "exact_target_metadata_hit_count": len(exact_target_hits),
            "exact_target_metadata_accessions": exact_target_hits,
            "top_accessions": accessions[:10],
        },
        metadata={"dagster_run_id": request.dagster_run_id, **request.metadata},
    )


def _dataset_from_object(
    obj: ResearchObject,
    accession: str,
    identifier_type: str,
    request: OmicsEvidencePacketRequest,
) -> OmicsEvidenceDataset:
    metadata = obj.metadata or {}
    disease_context = _disease_context(obj)
    evidence_role = {
        "canine_hsa": "direct",
        "human_angiosarcoma": "analog",
    }.get(disease_context, "context")
    limitations = _dataset_limitations(metadata)
    return OmicsEvidenceDataset(
        source_key=obj.source_key or "",
        accession=accession,
        identifier_type=identifier_type,  # type: ignore[arg-type]
        research_object_id=obj.id,
        title=obj.title,
        canonical_url=obj.canonical_url,
        organism=_first_text(metadata.get("organism"), metadata.get("taxon")),
        disease_context=disease_context,
        evidence_role=evidence_role,  # type: ignore[arg-type]
        sample_count=_int_or_none(metadata.get("sample_count")),
        library_strategy=_first_text(metadata.get("library_strategy"), metadata.get("dataset_type")),
        bioproject=obj.identifiers.get("bioproject") if obj.identifiers else None,
        pmid=obj.identifiers.get("pmid") if obj.identifiers else None,
        run_accessions=_string_list(metadata.get("run_accessions")),
        sample_accessions=_string_list(metadata.get("sample_accessions")),
        platform_accessions=_string_list(metadata.get("platform_accessions")),
        supplementary_file_types=_string_list(metadata.get("supplementary_file_types")),
        matched_terms=_matched_terms(obj, request),
        readout_hints=_readout_hints(obj, request),
        limitations=limitations,
        metadata={
            "dedupe_key": obj.dedupe_key,
            "source_query": metadata.get("source_query"),
            "object_type": str(obj.object_type),
            "sample_titles": metadata.get("sample_titles"),
            "sample_groups": metadata.get("sample_groups"),
            "matrix_uri": metadata.get("matrix_uri"),
            "processed_matrix_uri": metadata.get("processed_matrix_uri"),
            "matrix_url": metadata.get("matrix_url"),
            "processed_matrix_url": metadata.get("processed_matrix_url"),
            "matrix_artifact_id": metadata.get("matrix_artifact_id"),
            "processed_matrix_artifact_id": metadata.get("processed_matrix_artifact_id"),
            "supplementary_files": metadata.get("supplementary_files"),
            "matrix_text": metadata.get("matrix_text"),
        },
    )


def _primary_accession(obj: ResearchObject) -> tuple[str | None, str]:
    identifiers = obj.identifiers or {}
    if obj.source_key == "geo":
        for key in ("geo_accession", "gse", "gds"):
            value = identifiers.get(key)
            if value:
                return str(value), "geo"
    if obj.source_key == "sra":
        for key in ("sra_experiment", "sra_study", "sra_run", "bioproject", "biosample"):
            value = identifiers.get(key)
            if value:
                if key == "bioproject":
                    return str(value), "bioproject"
                if key == "biosample":
                    return str(value), "biosample"
                return str(value), "sra"
    return None, "geo"


def _disease_context(obj: ResearchObject) -> str:
    text = _object_text(obj).casefold()
    title = str(obj.title or "").casefold()
    organism = str((obj.metadata or {}).get("organism") or (obj.metadata or {}).get("taxon") or "").casefold()
    is_canid = any(term in organism for term in ("canis", "canine", "dog"))
    is_human = any(term in organism for term in ("homo sapiens", "human"))
    has_hsa = "hemangiosarcoma" in text
    has_angiosarcoma = "angiosarcoma" in text
    title_has_disease = "hemangiosarcoma" in title or "angiosarcoma" in title
    if has_hsa and (is_canid or (not organism and any(term in text for term in ("canine", "dog")))):
        return "canine_hsa"
    if has_angiosarcoma and (is_human or (not organism and "human" in text)) and (
        not has_hsa or title_has_disease
    ):
        return "human_angiosarcoma"
    if (has_angiosarcoma or has_hsa) and (title_has_disease or not organism):
        return "comparative_context"
    return "unknown"


def _matched_terms(obj: ResearchObject, request: OmicsEvidencePacketRequest) -> list[str]:
    haystack = _object_text(obj).casefold()
    terms: list[str] = []
    for term in [*request.disease_terms, *request.gene_symbols, "rna-seq", "transcriptome", "expression", "chro-seq"]:
        normalized = str(term).strip()
        if normalized and normalized.casefold() in haystack:
            terms.append(normalized)
    return _dedupe_strings(terms)


def _readout_hints(obj: ResearchObject, request: OmicsEvidencePacketRequest) -> list[str]:
    text = _object_text(obj).casefold()
    hints = [f"{gene} expression abundance" for gene in request.gene_symbols]
    if "rna-seq" in text or "sequencing" in text:
        hints.extend(["differential expression", "gene-set scoring"])
    if "array" in text:
        hints.append("microarray probe-level expression review")
    if "spatial" in text:
        hints.append("spatial compartment expression review")
    if "chro-seq" in text:
        hints.append("transcriptional activity and enhancer-state review")
    if "cell" in text:
        hints.append("tumor primary cell versus comparator expression review")
    return _dedupe_strings(hints)


def _dataset_limitations(metadata: dict) -> list[str]:
    limitations: list[str] = []
    if _int_or_none(metadata.get("sample_count")) is None:
        limitations.append("sample_count_missing")
    if not _string_list(metadata.get("sample_accessions")):
        limitations.append("sample_accessions_missing")
    if not (
        _string_list(metadata.get("supplementary_file_types"))
        or _string_list(metadata.get("run_accessions"))
    ):
        limitations.append("matrix_or_raw_file_hint_missing")
    return limitations


def _has_matrix_hint(dataset: OmicsEvidenceDataset) -> bool:
    file_types = {item.upper() for item in dataset.supplementary_file_types}
    if file_types & {"CEL", "TXT", "TSV", "XLS", "XLSX", "H5", "MTX"}:
        return True
    return bool(dataset.run_accessions)


def _decisive_questions(packet_key: str) -> list[str]:
    questions = [
        "Is VIM/vimentin elevated in tumor samples versus available controls or lower-risk comparators?",
        "Does VIM track with angiogenesis, ECM remodeling, coagulation, hypoxia, or vascular injury programs?",
        "Are high-VIM samples separable enough to define a biomarker-positive subgroup?",
    ]
    if packet_key == "human_angiosarcoma":
        questions.append("Does the human angiosarcoma signal transfer directionally to canine HSA?")
    else:
        questions.append("Do canine HSA datasets provide direct evidence strong enough for a therapy idea child packet?")
    return questions


def _proposed_readouts(packet_key: str, gene_symbols: list[str]) -> list[str]:
    genes = " / ".join(gene_symbols or ["VIM", "vimentin"])
    readouts = [
        f"{genes} expression effect size, direction, and FDR where comparator labels exist",
        "Per-sample mesenchymal/ECM score",
        "VEGF/angiogenesis pathway score",
        "Coagulation/vascular injury gene-set score",
        "Dataset-level confidence: sample count, comparator quality, matrix availability, and species mapping risk",
    ]
    if packet_key == "human_angiosarcoma":
        readouts.append("Cross-species ortholog mapping and direction-of-effect concordance")
    return readouts


def _object_text(obj: ResearchObject) -> str:
    metadata = obj.metadata or {}
    metadata_bits: list[str] = []
    for key in (
        "organism",
        "taxon",
        "dataset_type",
        "library_strategy",
        "library_source",
        "sample_titles",
        "study_name",
    ):
        value = metadata.get(key)
        if isinstance(value, list):
            metadata_bits.extend(str(item) for item in value)
        elif value:
            metadata_bits.append(str(value))
    return " ".join([obj.title or "", obj.abstract or "", *metadata_bits])


def _first_text(*values: object) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _string_list(value: object) -> list[str]:
    if isinstance(value, list):
        return _dedupe_strings([str(item) for item in value if str(item).strip()])
    if value:
        return [str(value)]
    return []


def _dedupe_datasets(datasets: list[OmicsEvidenceDataset]) -> list[OmicsEvidenceDataset]:
    deduped: list[OmicsEvidenceDataset] = []
    seen: set[str] = set()
    for dataset in datasets:
        key = f"{dataset.source_key}:{dataset.identifier_type}:{dataset.accession}".casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dataset)
    deduped.sort(
        key=lambda dataset: (
            {"direct": 0, "analog": 1, "context": 2}.get(dataset.evidence_role, 3),
            dataset.source_key,
            dataset.accession,
        )
    )
    return deduped


def _packet_id(
    packet_key: str,
    accessions: list[str],
    request: OmicsEvidencePacketRequest,
) -> str:
    basis = "|".join([str(request.program_id or "none"), packet_key, *sorted(accessions)])
    return f"omics_packet:{uuid5(NAMESPACE_URL, basis)}"


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
