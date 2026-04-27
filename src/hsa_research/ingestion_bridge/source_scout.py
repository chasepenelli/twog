"""Source Scout agent for ingestion coverage planning.

This agent inspects local coverage and produces a prioritized bridge plan. It
is deterministic for now, but the contract is shaped so a model-backed scout can
later critique coverage, suggest new journals, and draft richer queries.
"""

from __future__ import annotations

from dataclasses import dataclass

from .contracts import (
    ResearchObjectType,
    ResearchSource,
    SourceKind,
    SourceQuery,
    SourceRecommendation,
    SourceScoutRequest,
    SourceScoutResult,
)
from .harvesters_v2 import HARVESTERS
from .local_store import SQLiteResearchRepository
from .query_policy import (
    build_canine_data_source_queries,
    build_chemistry_source_queries,
    build_clinical_trial_source_queries,
    build_omics_source_queries,
    build_safety_source_queries,
    build_scholarly_source_queries,
    build_target_structure_source_queries,
    disease_analog_query,
)

SCOUT_NAME = "local_source_scout_agent"
SCOUT_VERSION = "0.1"

FOCUS_KINDS: dict[str, set[SourceKind]] = {
    "scholarly": {SourceKind.SCHOLARLY_METADATA, SourceKind.OPEN_ACCESS_FULL_TEXT},
    "canine": {SourceKind.CANINE_ONCOLOGY, SourceKind.VETERINARY_TRIAL, SourceKind.CLINICAL_TRIAL},
    "omics": {SourceKind.OMICS, SourceKind.CANINE_ONCOLOGY},
    "chemistry": {SourceKind.CHEMISTRY},
    "structure": {SourceKind.TARGET_STRUCTURE},
    "safety": {SourceKind.SAFETY, SourceKind.CLINICAL_TRIAL, SourceKind.VETERINARY_TRIAL},
}

DISEASE_ANALOG_QUERY_TEXT = disease_analog_query("pmc")
SCHOLARLY_SOURCE_QUERIES = build_scholarly_source_queries()
CLINICAL_TRIAL_SOURCE_QUERIES = build_clinical_trial_source_queries()
CANINE_DATA_SOURCE_QUERIES = build_canine_data_source_queries()
OMICS_SOURCE_QUERIES = build_omics_source_queries()
CHEMISTRY_SOURCE_QUERIES = build_chemistry_source_queries()
TARGET_STRUCTURE_SOURCE_QUERIES = build_target_structure_source_queries()
SAFETY_SOURCE_QUERIES = build_safety_source_queries()


def _queries_for_source(source_key: str) -> list[SourceQuery]:
    return [query for query in SCHOLARLY_SOURCE_QUERIES if query.source_key == source_key]


REGISTERED_SOURCE_QUERIES: dict[str, list[SourceQuery]] = {
    "pubmed": _queries_for_source("pubmed"),
    "europe_pmc": _queries_for_source("europe_pmc"),
    "openalex": _queries_for_source("openalex"),
    "crossref": _queries_for_source("crossref"),
    "pmc_oa": [
        SourceQuery(
            source_key="pmc_oa",
            query_name="licensed_full_text_hsa",
            query_text=DISEASE_ANALOG_QUERY_TEXT,
            query_params={"comparative_policy": "required", "license_required": True},
            track="legal_full_text",
        )
    ],
    "clinicaltrials_gov": [
        query.model_copy(update={"object_type": ResearchObjectType.CLINICAL_TRIAL})
        for query in CLINICAL_TRIAL_SOURCE_QUERIES
    ],
    "avma_vctr": [
        query.model_copy(update={"object_type": ResearchObjectType.VETERINARY_TRIAL})
        for query in CANINE_DATA_SOURCE_QUERIES
        if query.source_key == "avma_vctr"
    ],
    "icdc": [
        query.model_copy(update={"object_type": ResearchObjectType.DATASET})
        for query in CANINE_DATA_SOURCE_QUERIES
        if query.source_key == "icdc"
    ],
    "geo": [
        query.model_copy(update={"object_type": ResearchObjectType.DATASET})
        for query in OMICS_SOURCE_QUERIES
        if query.source_key == "geo"
    ],
    "sra": [
        query.model_copy(update={"object_type": ResearchObjectType.DATASET})
        for query in OMICS_SOURCE_QUERIES
        if query.source_key == "sra"
    ],
    "pubchem": [
        query.model_copy(update={"object_type": ResearchObjectType.COMPOUND_RECORD})
        for query in CHEMISTRY_SOURCE_QUERIES
        if query.source_key == "pubchem"
    ],
    "chembl": [
        query.model_copy(update={"object_type": ResearchObjectType.BIOACTIVITY_ASSAY})
        for query in CHEMISTRY_SOURCE_QUERIES
        if query.source_key == "chembl"
    ],
    "uniprot": [
        query.model_copy(update={"object_type": ResearchObjectType.STRUCTURE})
        for query in TARGET_STRUCTURE_SOURCE_QUERIES
        if query.source_key == "uniprot"
    ],
    "rcsb_pdb": [
        query.model_copy(update={"object_type": ResearchObjectType.STRUCTURE})
        for query in TARGET_STRUCTURE_SOURCE_QUERIES
        if query.source_key == "rcsb_pdb"
    ],
    "openfda_animal_events": [
        query.model_copy(update={"object_type": ResearchObjectType.SAFETY_REPORT})
        for query in SAFETY_SOURCE_QUERIES
        if query.source_key == "openfda_animal_events"
    ],
}


@dataclass(frozen=True)
class ExpansionSource:
    source_key: str
    display_name: str
    source_kind: SourceKind
    phase: int
    priority: int
    rationale: str
    queries: tuple[SourceQuery, ...]
    notes: tuple[str, ...]


EXPANSION_SOURCES: tuple[ExpansionSource, ...] = (
    ExpansionSource(
        source_key="geo",
        display_name="NCBI Gene Expression Omnibus",
        source_kind=SourceKind.OMICS,
        phase=2,
        priority=135,
        rationale="GEO can surface canine HSA expression datasets and angiosarcoma comparators for target/pathway grounding.",
        queries=(
            SourceQuery(
                source_key="geo",
                query_name="canine_hsa_expression",
                query_text='("canine hemangiosarcoma" OR "dog hemangiosarcoma" OR "canine angiosarcoma")',
                object_type=ResearchObjectType.DATASET,
                track="omics",
            ),
        ),
        notes=("Use NCBI E-utilities first; store dataset metadata and links before downloading matrices.",),
    ),
    ExpansionSource(
        source_key="sra",
        display_name="NCBI Sequence Read Archive",
        source_kind=SourceKind.OMICS,
        phase=2,
        priority=140,
        rationale="SRA can add sequence-level evidence for canine HSA cohorts and comparative oncology datasets.",
        queries=(
            SourceQuery(
                source_key="sra",
                query_name="canine_hsa_sequence_runs",
                query_text='("canine hemangiosarcoma" OR "dog hemangiosarcoma")',
                object_type=ResearchObjectType.DATASET,
                track="omics",
            ),
        ),
        notes=("Ingest run/project metadata first; raw sequence download should remain approval gated.",),
    ),
    ExpansionSource(
        source_key="bindingdb",
        display_name="BindingDB",
        source_kind=SourceKind.CHEMISTRY,
        phase=3,
        priority=225,
        rationale="BindingDB adds target-ligand binding evidence that ChEMBL/PubChem may not fully cover.",
        queries=(
            SourceQuery(
                source_key="bindingdb",
                query_name="priority_target_ligands",
                query_text="KDR OR KIT OR MTOR OR HDAC OR EGFR OR MET",
                object_type=ResearchObjectType.BIOACTIVITY_ASSAY,
                track="bioactivity",
            ),
        ),
        notes=("Resolve human target data to canine orthologs through UniProt before making translation claims.",),
    ),
    ExpansionSource(
        source_key="alphafold",
        display_name="AlphaFold Protein Structure Database",
        source_kind=SourceKind.TARGET_STRUCTURE,
        phase=3,
        priority=235,
        rationale="AlphaFold can fill structure gaps for canine orthologs before Boltz/docking jobs are queued.",
        queries=(
            SourceQuery(
                source_key="alphafold",
                query_name="canine_priority_structures",
                query_text="canis lupus familiaris VEGFA KDR FLT4 KIT MTOR CD47 SIRPA TP53",
                object_type=ResearchObjectType.STRUCTURE,
                track="target_structure",
            ),
        ),
        notes=("Store structure metadata and artifact URI; protein downloads should be artifact-managed.",),
    ),
    ExpansionSource(
        source_key="fda_green_book",
        display_name="FDA Green Book",
        source_kind=SourceKind.SAFETY,
        phase=3,
        priority=255,
        rationale="Green Book records help distinguish approved veterinary products, labels, and species-specific constraints.",
        queries=(
            SourceQuery(
                source_key="fda_green_book",
                query_name="veterinary_drug_labels",
                query_text="doxorubicin OR toceranib OR propranolol OR cyclophosphamide",
                object_type=ResearchObjectType.DRUG_LABEL,
                track="safety",
            ),
        ),
        notes=("Treat label facts separately from adverse-event reports; both need provenance.",),
    ),
)


class SourceScoutAgent:
    """Prioritize ingestion sources and draft starter source queries."""

    def __init__(self, repository: SQLiteResearchRepository) -> None:
        self.repository = repository

    def scout(self, request: SourceScoutRequest) -> SourceScoutResult:
        coverage = self.repository.coverage_summary()
        by_source = {entry["source_key"]: entry for entry in coverage.get("by_source", [])}
        registered = {source.source_key: source for source in self.repository.list_sources(enabled_only=False)}
        result = SourceScoutResult(
            scout_name=f"{SCOUT_NAME}:{SCOUT_VERSION}",
            focus=request.focus,
            model_profile=request.model_profile,
            coverage=coverage,
        )

        recommendations: list[SourceRecommendation] = []
        if request.include_registered_sources:
            recommendations.extend(
                self._registered_recommendations(registered.values(), by_source, request)
            )
        if request.include_expansion_sources:
            recommendations.extend(self._expansion_recommendations(registered, request))

        recommendations.sort(key=lambda item: (-item.priority_score, item.phase, item.source_key))
        result.recommendations = recommendations[: request.max_recommendations]
        result.next_actions = _next_actions(result.recommendations)
        return result

    def _registered_recommendations(
        self,
        sources: list[ResearchSource] | tuple[ResearchSource, ...] | object,
        by_source: dict[str, dict],
        request: SourceScoutRequest,
    ) -> list[SourceRecommendation]:
        recommendations: list[SourceRecommendation] = []
        for source in sources:
            if not isinstance(source, ResearchSource):
                continue
            if not source.enabled:
                continue
            if source.source_kind == SourceKind.INTERNAL or source.phase > request.max_phase:
                continue
            if not _matches_focus(source.source_kind, request.focus):
                continue
            coverage = by_source.get(source.source_key, {})
            raw_records = int(coverage.get("raw_records", 0) or 0)
            research_objects = int(coverage.get("research_objects", 0) or 0)
            if raw_records or research_objects:
                status = "active_coverage"
                rationale = f"{source.display_name} already has local coverage; deepen or refresh it after zero-coverage sources."
            else:
                status = "coverage_gap"
                rationale = _registered_rationale(source)
            recommendations.append(
                SourceRecommendation(
                    source_key=source.source_key,
                    display_name=source.display_name,
                    source_kind=source.source_kind,
                    status=status,
                    phase=source.phase,
                    priority_score=_registered_priority(source, raw_records, research_objects),
                    current_raw_records=raw_records,
                    current_research_objects=research_objects,
                    rationale=rationale,
                    recommended_queries=REGISTERED_SOURCE_QUERIES.get(source.source_key, []),
                    implementation_notes=_implementation_notes(source),
                )
            )
        return recommendations

    def _expansion_recommendations(
        self,
        registered: dict[str, ResearchSource],
        request: SourceScoutRequest,
    ) -> list[SourceRecommendation]:
        recommendations: list[SourceRecommendation] = []
        for source in EXPANSION_SOURCES:
            if source.source_key in registered or source.phase > request.max_phase:
                continue
            if not _matches_focus(source.source_kind, request.focus):
                continue
            recommendations.append(
                SourceRecommendation(
                    source_key=source.source_key,
                    display_name=source.display_name,
                    source_kind=source.source_kind,
                    status="not_registered",
                    phase=source.phase,
                    priority_score=_expansion_priority(source),
                    rationale=source.rationale,
                    recommended_queries=list(source.queries),
                    implementation_notes=list(source.notes),
                )
            )
        return recommendations


def scout_sources_for_repository(
    repository: SQLiteResearchRepository,
    request: SourceScoutRequest | None = None,
) -> SourceScoutResult:
    """Run the local source scout against a SQLite repository."""

    return SourceScoutAgent(repository).scout(request or SourceScoutRequest())


def _matches_focus(source_kind: SourceKind, focus: str) -> bool:
    if focus == "all":
        return True
    return source_kind in FOCUS_KINDS.get(focus, set())


def _registered_priority(source: ResearchSource, raw_records: int, research_objects: int) -> float:
    phase_bonus = {1: 0.95, 2: 0.82, 3: 0.72}.get(source.phase, 0.55)
    priority_penalty = min(0.18, source.priority / 2000)
    coverage_penalty = 0.28 if raw_records or research_objects else 0.0
    return max(0.1, min(0.99, phase_bonus - priority_penalty - coverage_penalty))


def _expansion_priority(source: ExpansionSource) -> float:
    phase_bonus = {2: 0.74, 3: 0.62}.get(source.phase, 0.5)
    priority_penalty = min(0.12, source.priority / 3000)
    return max(0.1, min(0.95, phase_bonus - priority_penalty))


def _registered_rationale(source: ResearchSource) -> str:
    if source.source_kind == SourceKind.SCHOLARLY_METADATA:
        return f"{source.display_name} is a zero-coverage scholarly source; it should be bridged before adding more downstream agents."
    if source.source_kind == SourceKind.OPEN_ACCESS_FULL_TEXT:
        return f"{source.display_name} can move us from abstracts to legal full-text chunks for stronger claim provenance."
    if source.source_kind == SourceKind.CANINE_ONCOLOGY:
        return f"{source.display_name} is canine-specific and should close the biggest translation gap in the corpus."
    if source.source_kind in {SourceKind.CLINICAL_TRIAL, SourceKind.VETERINARY_TRIAL}:
        return f"{source.display_name} adds trial status, interventions, and outcome context beyond papers."
    if source.source_kind == SourceKind.CHEMISTRY:
        return f"{source.display_name} adds compound identifiers, assays, and bioactivity context for candidate scoring."
    if source.source_kind == SourceKind.TARGET_STRUCTURE:
        return f"{source.display_name} adds protein/structure context needed before GPU validation jobs."
    if source.source_kind == SourceKind.SAFETY:
        return f"{source.display_name} adds safety evidence that should gate candidate prioritization."
    return f"{source.display_name} is registered but has no local coverage yet."


def _implementation_notes(source: ResearchSource) -> list[str]:
    notes = [f"Use source key `{source.source_key}` and preserve raw payloads before normalization."]
    if source.source_key in HARVESTERS:
        notes.append("A local harvester class already exists; prioritize running it, hardening pagination, and adding tests.")
    else:
        notes.append("No local harvester class exists yet; implement source-specific fetch and normalization first.")
    if source.requires_api_key:
        notes.append("Requires API key configuration before scheduled ingestion.")
    if source.license_policy:
        notes.append(f"License policy: {source.license_policy}.")
    if "legal_full_text" in source.capabilities or source.source_kind == SourceKind.OPEN_ACCESS_FULL_TEXT:
        notes.append("Only store full text when the license explicitly permits local storage.")
    if source.source_kind == SourceKind.TARGET_STRUCTURE:
        notes.append("Link targets through UniProt/canine ortholog resolution before validation jobs.")
    return notes


def _next_actions(recommendations: list[SourceRecommendation]) -> list[str]:
    if not recommendations:
        return ["No matching source gaps found for this focus."]
    actions = []
    for rec in recommendations[:5]:
        query_names = ", ".join(query.query_name for query in rec.recommended_queries[:3])
        verb = "Run and harden" if rec.source_key in HARVESTERS else "Implement"
        if query_names:
            actions.append(f"{verb} `{rec.source_key}` harvester and seed queries: {query_names}.")
        else:
            actions.append(f"{verb} `{rec.source_key}` harvester and add source-specific starter queries.")
    return actions
