"""Dagster asset scaffold for Ingestion Bridge v2.

These assets define the durable graph before each harvester is implemented.
They are intentionally lightweight and deterministic.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
import json
import os
import time
from typing import Any
from uuid import UUID

from .contracts import (
    AgentPerformanceEvaluationRequest,
    AgentPerformanceReportRequest,
    ClaimCurationRequest,
    ClaimSearchRequest,
    CommandCenterRequest,
    ComputeJobReportRequest,
    EvidenceGapResolverRequest,
    EntityLookupIndexRequest,
    FullTextOpsRequest,
    HypothesisPromotionReportRequest,
    MDExpertAgentReviewRequest,
    OmicsAccessionHuntRequest,
    OmicsEvidencePacketRequest,
    OmicsFollowupRequest,
    OmicsLocusSignalRequest,
    OmicsReadoutRequest,
    PubMedIdentifierRepairRequest,
    PublicCandidateGenerateRequest,
    PublicCandidateIntegrityReportRequest,
    ResearchBriefEvaluationRequest,
    ResearchBriefFollowupQueueRequest,
    ResearchBriefQualityReportRequest,
    ResearchBriefQueueBatchRequest,
    ResearchBriefQueueMaintenanceRequest,
    ResearchBriefQueueRequest,
    ResearchBriefQueueRunRequest,
    ResearchBriefRequest,
    ResearchFollowupResolverRequest,
    ResearchHuntQueueReportRequest,
    ResearchHuntQueueMaintenanceRequest,
    ResearchHuntSynthesisDocRequest,
    ResearchHuntSynthesisQueueRequest,
    ResearchLeadCollectRequest,
    ResearchProgramBoardRequest,
    ResearchProgramEvidenceLoopRequest,
    ResearchProgramReviewRequest,
    RewardEventSyncRequest,
    RewardReportRequest,
    ResearchObject,
    SourceFollowupIngestRequest,
    SourceFollowupQueueRequest,
    SourceQuery,
    SourceScoutRequest,
    TherapyCommitteeRequest,
    TherapyCommitteeValidationQueueRequest,
    TherapyIdeaLibraryRequest,
    ValidationAutopilotRequest,
    ValidationGapSourceIngestRequest,
    ValidationGapSourcePackRequest,
    ValidationDecisionReportRequest,
    ValidationPacketRequest,
    ValidationPlanRequest,
    ValidationRequestQueueRequest,
    ValidationToolCatalogRequest,
    ValidationToolMatchRequest,
    XLinkedArticleReviewRequest,
    XLinkedArticleFollowupRequest,
    XTopicReviewRequest,
)
from .query_policy import (
    build_canine_data_source_queries,
    build_chemistry_source_queries,
    build_clinical_trial_source_queries,
    build_omics_source_queries,
    build_research_primitive_source_queries,
    build_safety_source_queries,
    build_scholarly_source_queries,
    build_target_structure_source_queries,
)
from .source_registry import get_initial_sources
from .source_sets import (
    ALL_API_SOURCE_KEYS,
    CANINE_DATA_OMICS_SOURCE_KEYS,
    LITERATURE_CLINICAL_SOURCE_KEYS,
    LITERATURE_CORPUS_SOURCE_KEYS,
    LITERATURE_CORPUS_SOURCE_LIMITS,
    LITERATURE_FULL_TEXT_SOURCE_KEYS,
    LITERATURE_FULL_TEXT_SOURCE_LIMITS,
    PRIMITIVE_SOURCE_KEYS,
    STRUCTURED_SOURCE_KEYS,
)

try:
    import dagster as dg
except ImportError:  # pragma: no cover - Dagster is optional until the orchestration env is installed
    dg = None  # type: ignore[assignment]


STRUCTURED_SOURCE_SMOKE_KEYS = ("pubchem",)
STRUCTURED_SOURCE_MULTISOURCE_SMOKE_KEYS = STRUCTURED_SOURCE_KEYS
CANINE_DATA_OMICS_SMOKE_KEYS = CANINE_DATA_OMICS_SOURCE_KEYS
LITERATURE_CLINICAL_SMOKE_KEYS = LITERATURE_CLINICAL_SOURCE_KEYS
ALL_API_SMOKE_KEYS = ALL_API_SOURCE_KEYS
HOSTED_API_REPORT_KEYS = ALL_API_SMOKE_KEYS
ENTITY_LOOKUP_INDEX_SOURCE_KEYS = (*PRIMITIVE_SOURCE_KEYS, "pubchem", "chembl", "uniprot")
RESEARCH_PRIMITIVE_SOURCE_SMOKE_KEYS = ("hgnc", "ncbi_gene", "ensembl_xrefs", "pubchem", "reactome")
LITERATURE_FULL_TEXT_SMOKE_LIMITS = {
    source_key: 1 for source_key in LITERATURE_FULL_TEXT_SOURCE_KEYS
}
SOURCE_FOLLOWUP_LANE_LIMITS = {
    "pubmed": 25,
    "crossref": 25,
    "pmc_oa": 10,
    "clinicaltrials_gov": 10,
    "unpaywall": 25,
}
SCHEDULE_TIMEZONE = "America/Denver"
LITERATURE_CORPUS_PARTITION_START_DATE = "2026-01-01"
LITERATURE_FULL_TEXT_PARTITION_START_DATE = "2026-01-01"

_STRUCTURED_SOURCE_COUNT_TABLE_COLUMNS = (
    "source_key",
    "raw_records",
    "research_objects",
    "document_chunks",
    "entity_mentions",
    "claims",
    "passes_minimum_bar",
    "claim_status",
    "claim_types",
    "full_text_triage_action",
    "full_text_triage_severity",
    "full_text_triage_should_retry",
    "full_text_triage_should_block_schedule",
    "full_text_qa",
)
_SOURCE_HEALTH_TABLE_COLUMNS = (
    "source_key",
    "source_role",
    "health_status",
    "health_score",
    "raw_records",
    "research_objects",
    "document_chunks",
    "entity_mentions",
    "claims",
    "embedding_coverage_ratio",
    "missing_embeddings",
    "source_followup_failed",
    "source_followup_pending",
    "active_source_queries",
    "passes_minimum_bar",
    "signals",
    "risks",
    "recommended_actions",
    "claim_metadata",
    "embedding_health",
    "source_followup_health",
    "source_query_health",
    "full_text_triage_action",
    "full_text_triage_severity",
    "full_text_triage_should_retry",
    "full_text_triage_should_block_schedule",
    "full_text_qa",
)
_COMMAND_CENTER_RECOMMENDATION_TABLE_COLUMNS = (
    "severity",
    "area",
    "action",
    "job_name",
    "reason",
)
_CANDIDATE_CONTRIBUTION_INTAKE_TABLE_COLUMNS = (
    "contribution_id",
    "display_id",
    "status",
    "contribution_type",
    "contribution_content_hash",
    "status_url",
    "requested_system_action",
    "recommended_route",
    "evidence_count",
    "artifact_count",
    "created_at",
    "route_reason",
)
_CANDIDATE_CONTRIBUTION_TRIAGE_TABLE_COLUMNS = (
    "contribution_id",
    "display_id",
    "old_status",
    "new_status",
    "contribution_content_hash",
    "status_url",
    "action",
    "operator",
    "promoted_queue_id",
    "would_update",
)
_WORK_PACKET_SEED_TABLE_COLUMNS = (
    "work_packet_id",
    "candidate_id",
    "packet_type",
    "title",
    "difficulty",
    "status",
    "notebook_recommended",
    "reward_hint",
)
_PROOF_CAPSULE_REWARD_SYNC_COLUMNS = (
    "scanned_review_count",
    "eligible_review_count",
    "created_count",
    "skipped_existing_count",
)
_AGENT_PERFORMANCE_TABLE_COLUMNS = (
    "group_type",
    "group_value",
    "run_count",
    "reviewed_run_count",
    "performance_score",
    "useful_rate",
    "followup_rate",
    "bad_rate",
    "unclear_rate",
    "disagreement_count",
    "low_sample",
)
_AGENT_PERFORMANCE_EVALUATION_TABLE_COLUMNS = (
    "agent_run_id",
    "agent_name",
    "specialist",
    "model_name",
    "verdict",
    "confidence",
    "review_id",
    "rationale",
)
_REWARD_REPORT_TABLE_COLUMNS = (
    "group_type",
    "group_value",
    "event_count",
    "reward_score",
    "average_score",
    "positive_signal_count",
    "actionable_followup_count",
    "low_value_churn_count",
    "negative_signal_count",
    "actionable_followup_rate",
    "low_value_churn_rate",
    "low_sample",
)
_PUBMED_IDENTIFIER_REPAIR_TABLE_COLUMNS = (
    "pmid",
    "status",
    "old_dedupe_key",
    "new_dedupe_key",
    "old_doi",
    "new_doi",
    "old_pmcid",
    "new_pmcid",
    "error",
)
_FULL_TEXT_TRIAGE_TABLE_COLUMNS = (
    "source_key",
    "mode",
    "passes_full_text_bar",
    "triage_action",
    "triage_severity",
    "should_retry",
    "should_block_schedule",
    "reasons",
    "recommended_next_actions",
)
_FULL_TEXT_OPS_ACTION_TABLE_COLUMNS = (
    "source_key",
    "action",
    "severity",
    "reason",
    "dagster_job_name",
    "partition_date",
    "evidence_refs",
)
_RESEARCH_BRIEF_PERSPECTIVE_TABLE_COLUMNS = (
    "perspective",
    "agent_name",
    "finding_count",
    "citation_count",
    "hard_error_count",
    "evidence_limitation_count",
    "error_count",
    "summary",
)
_RESEARCH_BRIEF_CITATION_TABLE_COLUMNS = (
    "citation_id",
    "source_key",
    "title",
    "section_label",
    "match_type",
    "score",
    "relevance",
)
_THERAPY_IDEA_TABLE_COLUMNS = (
    "title",
    "priority_score",
    "evidence_strength",
    "candidate_therapies",
    "targets",
    "biomarkers",
    "evidence_refs",
)
_THERAPY_COMMITTEE_REPORT_TABLE_COLUMNS = (
    "perspective",
    "agent_name",
    "idea_count",
    "evidence_limitation_count",
    "error_count",
    "summary",
)
_VALIDATION_TOOL_CATALOG_TABLE_COLUMNS = (
    "tool_key",
    "category",
    "runner_status",
    "tool_hint",
    "validation_types",
    "task_types",
    "quality_gates",
)
_THERAPY_IDEA_LIBRARY_TABLE_COLUMNS = (
    "therapy_idea_id",
    "status",
    "promotion_state",
    "score",
    "title",
    "candidate_therapies",
    "targets",
    "evidence_refs",
)
_PUBLIC_CANDIDATE_GATE_TABLE_COLUMNS = (
    "passed",
    "priority_score",
    "min_priority_score",
    "has_program_lineage",
    "evidence_ref_count",
    "reasons",
    "blockers",
)
_PUBLIC_CANDIDATE_INTEGRITY_TABLE_COLUMNS = (
    "candidate_id",
    "candidate_found",
    "therapy_idea_found",
    "latest_snapshot_found",
    "trace_id",
    "run_manifest_id",
    "run_manifest_found",
    "strict_export_ready",
    "problems",
)
_HYPOTHESIS_PROMOTION_TABLE_COLUMNS = (
    "candidate_id",
    "source_type",
    "promotion_state",
    "score",
    "title",
    "recommended_job_name",
    "blockers",
    "matched_tools",
)
_VALIDATION_PACKET_TABLE_COLUMNS = (
    "packet_id",
    "status",
    "readiness",
    "discovery_readiness",
    "validation_strategy_readiness",
    "protocol_readiness",
    "score",
    "source_type",
    "title",
    "candidate_therapies",
    "targets",
    "matched_tools",
    "task_count",
    "queue_item_count",
    "dispatch_blocker_count",
    "risk_annotation_count",
    "protocol_blocker_count",
    "follow_up_count",
    "evaluated_follow_up_count",
    "passing_follow_up_count",
)
_VALIDATION_DECISION_TABLE_COLUMNS = (
    "decision_id",
    "packet_id",
    "outcome",
    "confidence",
    "validation_ready",
    "specific_claim_viability",
    "broader_program_signal",
    "title",
    "recommended_downstream_action",
    "blocking_reason_count",
)
_RESEARCH_PROGRAM_TABLE_COLUMNS = (
    "program_id",
    "status",
    "gate_decision",
    "confidence_score",
    "evidence_loop_count",
    "max_evidence_loops",
    "title",
    "thesis_area",
    "question_count",
    "evidence_task_count",
    "therapy_families",
    "recommended_tools",
    "stop_criteria",
)
_RESEARCH_PROGRAM_EVIDENCE_LOOP_TABLE_COLUMNS = (
    "task_id",
    "title",
    "status_before",
    "status_after",
    "research_lead_id",
    "brief_queue_item_id",
    "selected_source_keys",
    "source_query_count",
    "errors",
)
_OMICS_ACCESSION_HUNT_TABLE_COLUMNS = (
    "source_key",
    "accession",
    "identifier_type",
    "organism",
    "sample_count",
    "library_strategy",
    "bioproject",
    "pmid",
    "matched_terms",
    "title",
)
_OMICS_EVIDENCE_PACKET_TABLE_COLUMNS = (
    "packet_key",
    "readiness",
    "score",
    "dataset_count",
    "direct_dataset_count",
    "analog_dataset_count",
    "total_sample_count",
    "source_keys",
    "accessions",
    "dispatch_blockers",
    "title",
)
_OMICS_READOUT_TABLE_COLUMNS = (
    "accession",
    "status",
    "sample_count",
    "gene_count",
    "tumor_sample_count",
    "control_sample_count",
    "target_support",
    "target_effect_size",
    "matrix_uri",
    "limitations",
)
_OMICS_LOCUS_SIGNAL_TABLE_COLUMNS = (
    "accession",
    "status",
    "sample_count",
    "computed_sample_count",
    "tumor_sample_count",
    "control_sample_count",
    "support_level",
    "effect_size",
    "tumor_control_delta",
    "comparison_p_value",
    "normalization_status",
    "limitations",
)
_OMICS_FOLLOWUP_TASK_TABLE_COLUMNS = (
    "task_type",
    "priority",
    "title",
    "source_keys",
    "target_genes",
    "accessions",
    "query_text",
)
_RESEARCH_BRIEF_LIBRARY_TABLE_COLUMNS = (
    "brief_id",
    "agent_run_id",
    "status",
    "topic",
    "source_key",
    "brief_style",
    "model_profile",
    "finding_count",
    "citation_count",
    "research_lead_count",
    "hard_error_count",
    "evidence_limitation_count",
    "error_count",
    "created_at",
)
_RESEARCH_BRIEF_EVALUATION_TABLE_COLUMNS = (
    "evaluation_id",
    "brief_id",
    "agent_run_id",
    "topic",
    "source_key",
    "overall_score",
    "passes_quality_bar",
    "readiness",
    "recommendation_count",
    "error_count",
    "created_at",
)
_RESEARCH_BRIEF_QUALITY_TABLE_COLUMNS = (
    "brief_id",
    "evaluation_id",
    "status",
    "quality_status",
    "topic",
    "source_key",
    "review_mode",
    "review_models",
    "citation_count",
    "finding_count",
    "hypothesis_count",
    "hard_error_count",
    "evidence_limitation_count",
    "error_count",
    "overall_score",
    "passes_quality_bar",
    "readiness",
    "created_at",
)
_RESEARCH_BRIEF_FOLLOWUP_QUEUE_TABLE_COLUMNS = (
    "lead_id",
    "status",
    "source_key",
    "origin_record_id",
    "title",
    "reason",
    "evidence_refs",
    "created_at",
)
_VALIDATION_PLAN_TASK_TABLE_COLUMNS = (
    "task_id",
    "task_type",
    "title",
    "priority",
    "validation_type",
    "target_name",
    "candidate_name",
    "objective",
    "rationale",
    "tool_hint",
    "evidence_refs",
)
_VALIDATION_PLAN_LIBRARY_TABLE_COLUMNS = (
    "plan_id",
    "brief_id",
    "evaluation_id",
    "agent_run_id",
    "topic",
    "source_key",
    "status",
    "readiness",
    "task_count",
    "hypothesis_count",
    "task_titles",
    "validation_types",
    "created_at",
)
_VALIDATION_REQUEST_QUEUE_TABLE_COLUMNS = (
    "queue_item_id",
    "status",
    "priority",
    "plan_id",
    "task_id",
    "task_type",
    "title",
    "validation_type",
    "target_name",
    "candidate_name",
    "source_key",
    "quality_gate_count",
    "dispatch_blocker_count",
    "attempts",
    "last_run_id",
    "last_error",
    "created_at",
)
_VALIDATION_AUTOPILOT_TABLE_COLUMNS = (
    "queue_item_id",
    "status",
    "priority",
    "task_type",
    "validation_type",
    "title",
    "source_key",
    "reason",
    "decision",
    "agent_run_id",
    "cost_usd",
    "last_error",
)
_RESEARCH_BRIEF_QUEUE_TABLE_COLUMNS = (
    "queue_item_id",
    "status",
    "priority",
    "topic",
    "source_key",
    "brief_style",
    "model_profile",
    "review_mode",
    "attempts",
    "last_brief_id",
    "last_error",
    "created_at",
)
_RESEARCH_HUNT_SYNTHESIS_DOC_TABLE_COLUMNS = (
    "lead_id",
    "title",
    "control_status",
    "recommended_action",
    "artifact_id",
    "claim_count",
    "chunk_count",
    "research_object_count",
    "technical_footnote_count",
    "created_at",
)
_RESEARCH_LEAD_TABLE_COLUMNS = (
    "lead_id",
    "lead_type",
    "status",
    "priority",
    "source_key",
    "title",
    "url",
    "topic_tags",
    "suggested_sources",
    "reason",
)
_RESEARCH_HUNT_LEAD_TABLE_COLUMNS = (
    "lead_id",
    "status",
    "control_status",
    "open_concrete_count",
    "open_broad_count",
    "open_passive_count",
    "stale_task_count",
    "suppressed_task_count",
    "best_signal_score",
    "recommended_action",
    "title",
)
_RESEARCH_HUNT_TASK_TABLE_COLUMNS = (
    "lead_id",
    "task_id",
    "task_type",
    "task_class",
    "status",
    "priority",
    "stale",
    "runnable_by_default",
    "recommended_action",
    "suppression_reason",
    "age_hours",
    "action",
)
_RESEARCH_HUNT_MAINTENANCE_TABLE_COLUMNS = (
    "lead_id",
    "task_id",
    "task_type",
    "task_class",
    "previous_status",
    "new_status",
    "suppression_reason",
    "age_hours",
    "dry_run",
    "action",
)
_RESEARCH_FOLLOWUP_RESOLVER_TABLE_COLUMNS = (
    "lead_id",
    "status_before",
    "status_after",
    "actions",
    "source_followup_ids",
    "durable_source_keys",
    "evidence_refs",
    "evidence_inspection_count",
    "manual_research_required",
    "promoted",
    "errors",
)
_VALIDATION_GAP_SOURCE_PACK_TABLE_COLUMNS = (
    "source_key",
    "lane",
    "query_name",
    "query_text",
    "priority",
    "active",
    "lead_ids",
    "queue_item_ids",
    "required_terms",
)
_VALIDATION_GAP_SOURCE_INGEST_TABLE_COLUMNS = (
    "source_key",
    "query_name",
    "status",
    "raw_records",
    "research_objects",
    "document_chunks",
    "full_text_research_objects",
    "errors",
)
_VALIDATION_GAP_COMPLETION_BRIEF_TABLE_COLUMNS = (
    "queue_item_id",
    "status",
    "topic",
    "source_key",
    "brief_id",
    "agent_run_id",
    "citation_count",
    "error_count",
    "last_error",
)
_RESEARCH_BRIEF_PLAYGROUND_PROMPT_TABLE_COLUMNS = (
    "perspective",
    "agent_name",
    "recommended_model",
    "user_prompt_chars",
    "citation_count",
)
_X_TOPIC_CANDIDATE_TABLE_COLUMNS = (
    "query_name",
    "post_id",
    "username",
    "quality_score",
    "review_status",
    "matched_terms",
    "durable_links",
)
_X_TOPIC_REVIEW_TABLE_COLUMNS = (
    "post_id",
    "query_name",
    "action",
    "severity",
    "recommended_sources",
    "identifiers",
    "links",
    "reason",
)
_X_LINKED_ARTICLE_SOURCE_LINK_COLUMNS = (
    "recommended_source_key",
    "identifier_type",
    "identifier",
    "url",
    "should_ingest",
    "reason",
)
_X_LINKED_ARTICLE_REVIEW_TABLE_COLUMNS = (
    "review_id",
    "source_record_id",
    "action",
    "severity",
    "followup_sources",
    "identifiers",
    "reason",
)
_SOURCE_FOLLOWUP_QUEUE_TABLE_COLUMNS = (
    "followup_id",
    "source_key",
    "identifier_type",
    "identifier",
    "status",
    "priority",
    "attempts",
    "origin_source_key",
    "origin_review_id",
    "reason",
)
_SOURCE_FOLLOWUP_INGEST_TABLE_COLUMNS = (
    "followup_id",
    "source_key",
    "identifier_type",
    "identifier",
    "status",
    "raw_records",
    "research_objects",
    "document_chunks",
    "error",
)
_ENTITY_RESOLUTION_TABLE_COLUMNS = (
    "source_key",
    "chunks_seen",
    "chunks_with_mentions",
    "entities_upserted",
    "aliases_upserted",
    "mentions_upserted",
    "entity_mentions",
    "claims",
    "passes_minimum_bar",
    "errors",
)
_ENTITY_LOOKUP_INDEX_TABLE_COLUMNS = (
    "source_key",
    "records_seen",
    "entities_upserted",
    "aliases_upserted",
    "source_version",
    "errors",
)
_COMPUTE_JOB_TABLE_COLUMNS = (
    "compute_job_id",
    "queue_item_id",
    "status",
    "runner_kind",
    "compute_profile",
    "validation_type",
    "title",
    "runpod_job_id",
    "dagster_run_id",
    "last_error",
    "updated_at",
)


def _metadata_table_scalar(value: Any) -> str | int | float | bool | None:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return json.dumps(value, sort_keys=True, default=str)


def _compact_table_rows(
    rows: Sequence[Mapping[str, Any]],
    columns: Sequence[str],
) -> list[dict[str, str | int | float | bool | None]]:
    return [
        {column: _metadata_table_scalar(row.get(column)) for column in columns}
        for row in rows
    ]


def build_source_queries() -> list[SourceQuery]:
    """Create the starter query set for implemented ingestion sources."""

    return [
        *build_scholarly_source_queries(),
        *build_clinical_trial_source_queries(),
        *build_canine_data_source_queries(),
        *build_omics_source_queries(),
        *build_chemistry_source_queries(),
        *build_target_structure_source_queries(),
        *build_research_primitive_source_queries(),
        *build_safety_source_queries(),
    ]


if dg is not None:
    from .dagster_resources import ResearchRepositoryResource

    LITERATURE_CORPUS_SOURCE_PARTITIONS = dg.StaticPartitionsDefinition(
        list(LITERATURE_CORPUS_SOURCE_KEYS)
    )
    LITERATURE_CORPUS_DATE_PARTITIONS = dg.DailyPartitionsDefinition(
        start_date=LITERATURE_CORPUS_PARTITION_START_DATE,
    )
    LITERATURE_CORPUS_SOURCE_DATE_PARTITIONS = dg.MultiPartitionsDefinition(
        {
            "source": LITERATURE_CORPUS_SOURCE_PARTITIONS,
            "date": LITERATURE_CORPUS_DATE_PARTITIONS,
        }
    )
    LITERATURE_FULL_TEXT_SOURCE_PARTITIONS = dg.StaticPartitionsDefinition(
        list(LITERATURE_FULL_TEXT_SOURCE_KEYS)
    )
    LITERATURE_FULL_TEXT_DATE_PARTITIONS = dg.DailyPartitionsDefinition(
        start_date=LITERATURE_FULL_TEXT_PARTITION_START_DATE,
    )
    LITERATURE_FULL_TEXT_SOURCE_DATE_PARTITIONS = dg.MultiPartitionsDefinition(
        {
            "source": LITERATURE_FULL_TEXT_SOURCE_PARTITIONS,
            "date": LITERATURE_FULL_TEXT_DATE_PARTITIONS,
        }
    )

    def _full_text_source_date_daily_schedule_requests(
        context: dg.ScheduleEvaluationContext,
    ) -> list[dg.RunRequest]:
        scheduled_time = context.scheduled_execution_time
        if scheduled_time is None:
            return []
        partition_date = (scheduled_time.date() - timedelta(days=1)).isoformat()
        return [
            dg.RunRequest(
                run_key=f"{source_key}:{partition_date}",
                partition_key=dg.MultiPartitionKey(
                    {
                        "source": source_key,
                        "date": partition_date,
                    }
                ),
            )
            for source_key in LITERATURE_FULL_TEXT_SOURCE_KEYS
        ]

    def _metadata_table(
        rows: Sequence[Mapping[str, Any]],
        columns: Sequence[str],
    ) -> dg.TableMetadataValue:
        compact_rows = _compact_table_rows(rows, columns)
        records = [dg.TableRecord(data=row) for row in compact_rows]
        if records:
            return dg.MetadataValue.table(records=records)
        return dg.MetadataValue.table(
            records=[],
            schema=dg.TableSchema(columns=[dg.TableColumn(name=column) for column in columns]),
        )

    def _uuid_or_none(value: str | None) -> UUID | None:
        return UUID(value) if value else None

    def _csv_values(value: Any) -> list[str]:
        return [item.strip() for item in str(value or "").split(",") if item.strip()]

    def _uuid_csv_values(value: Any) -> list[UUID]:
        parsed: list[UUID] = []
        for item in _csv_values(value):
            try:
                parsed.append(UUID(item))
            except ValueError:
                continue
        return parsed

    def _expected_public_candidate_pairs(value: Any) -> dict[str, UUID]:
        pairs: dict[str, UUID] = {}
        for item in _csv_values(value):
            candidate_id, separator, therapy_idea_id = item.partition("=")
            if not separator:
                continue
            try:
                pairs[candidate_id.strip()] = UUID(therapy_idea_id.strip())
            except ValueError:
                continue
        return pairs

    def _base_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "source_count": len(report.get("sources", [])),
            "source_keys": dg.MetadataValue.json(report.get("source_keys", [])),
            "totals": dg.MetadataValue.json(report.get("totals", {})),
            "coverage": dg.MetadataValue.json(report.get("coverage", {})),
        }

    def _structured_source_count_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            **_base_report_metadata(report),
            "failed_source_count": len(report.get("failed_sources", [])),
            "failed_sources": dg.MetadataValue.json(report.get("failed_sources", [])),
            "minimum_bar": dg.MetadataValue.json(report.get("minimum_bar", {})),
            "passes_minimum_bar": bool(report.get("passes_minimum_bar", False)),
            "source_count_table": _metadata_table(
                report.get("sources", []),
                _STRUCTURED_SOURCE_COUNT_TABLE_COLUMNS,
            ),
        }

    def _source_health_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            **_base_report_metadata(report),
            "failed_source_count": len(report.get("failed_sources", [])),
            "failed_sources": dg.MetadataValue.json(report.get("failed_sources", [])),
            "minimum_bar": dg.MetadataValue.json(report.get("minimum_bar", {})),
            "passes_minimum_bar": bool(report.get("passes_minimum_bar", False)),
            "summary": dg.MetadataValue.json(report.get("summary", {})),
            "triage_source_count": len(report.get("triage_sources", [])),
            "triage_sources": dg.MetadataValue.json(report.get("triage_sources", [])),
            "watch_source_count": len(report.get("watch_sources", [])),
            "watch_sources": dg.MetadataValue.json(report.get("watch_sources", [])),
            "embedding_missing_source_count": len(report.get("embedding_missing_sources", [])),
            "embedding_missing_sources": dg.MetadataValue.json(report.get("embedding_missing_sources", [])),
            "source_followup_failed_source_count": len(report.get("source_followup_failed_sources", [])),
            "source_followup_failed_sources": dg.MetadataValue.json(report.get("source_followup_failed_sources", [])),
            "source_followup_pending_source_count": len(report.get("source_followup_pending_sources", [])),
            "source_followup_pending_sources": dg.MetadataValue.json(report.get("source_followup_pending_sources", [])),
            "sources_without_active_queries_count": len(report.get("sources_without_active_queries", [])),
            "sources_without_active_queries": dg.MetadataValue.json(report.get("sources_without_active_queries", [])),
            "full_text_blocking_sources": dg.MetadataValue.json(report.get("full_text_blocking_sources", [])),
            "full_text_triage": dg.MetadataValue.json(report.get("full_text_triage", [])),
            "source_health_table": _metadata_table(
                report.get("sources", []),
                _SOURCE_HEALTH_TABLE_COLUMNS,
            ),
        }

    def _candidate_contribution_intake_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        summary = report.get("summary", {})
        return {
            "storage_configured": bool(report.get("storage_configured", False)),
            "table_available": bool(report.get("table_available", False)),
            "row_count": dg.MetadataValue.int(int(summary.get("row_count") or 0)),
            "queued_for_intake": dg.MetadataValue.int(int(summary.get("queued_for_intake") or 0)),
            "triage_in_progress": dg.MetadataValue.int(int(summary.get("triage_in_progress") or 0)),
            "needs_more_information": dg.MetadataValue.int(int(summary.get("needs_more_information") or 0)),
            "actionable_count": dg.MetadataValue.int(int(summary.get("actionable_count") or 0)),
            "no_action_count": dg.MetadataValue.int(int(summary.get("no_action_count") or 0)),
            "status_counts": dg.MetadataValue.json(report.get("status_counts", {})),
            "requested_action_counts": dg.MetadataValue.json(report.get("requested_action_counts", {})),
            "recommended_route_counts": dg.MetadataValue.json(report.get("recommended_route_counts", {})),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "intake_rows": _metadata_table(
                report.get("rows", []),
                _CANDIDATE_CONTRIBUTION_INTAKE_TABLE_COLUMNS,
            ),
        }

    def _work_packet_seed_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "dry_run": bool(report.get("dry_run", True)),
            "storage_configured": bool(report.get("storage_configured", False)),
            "table_available": bool(report.get("table_available", True)),
            "planned_count": dg.MetadataValue.int(int(report.get("planned_count") or 0)),
            "inserted_count": dg.MetadataValue.int(int(report.get("inserted_count") or 0)),
            "skipped_count": dg.MetadataValue.int(int(report.get("skipped_count") or 0)),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "rows": _metadata_table(
                report.get("rows", []),
                _WORK_PACKET_SEED_TABLE_COLUMNS,
            ),
        }

    def _proof_capsule_reward_sync_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "scanned_review_count": dg.MetadataValue.int(int(report.get("scanned_review_count") or 0)),
            "eligible_review_count": dg.MetadataValue.int(int(report.get("eligible_review_count") or 0)),
            "created_count": dg.MetadataValue.int(int(report.get("created_count") or 0)),
            "skipped_existing_count": dg.MetadataValue.int(int(report.get("skipped_existing_count") or 0)),
            "reward_event_ids": dg.MetadataValue.json(report.get("reward_event_ids", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
        }

    def _candidate_contribution_triage_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        summary = report.get("summary", {})
        return {
            "dry_run": bool(report.get("dry_run", True)),
            "action": str(report.get("action") or ""),
            "target_status": str(report.get("target_status") or ""),
            "operator": str(report.get("operator") or ""),
            "requested_count": dg.MetadataValue.int(int(summary.get("requested_count") or 0)),
            "selected_count": dg.MetadataValue.int(int(summary.get("selected_count") or 0)),
            "missing_count": dg.MetadataValue.int(int(summary.get("missing_count") or 0)),
            "updated_count": dg.MetadataValue.int(int(summary.get("updated_count") or 0)),
            "missing_contribution_ids": dg.MetadataValue.json(report.get("missing_contribution_ids", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "triage_rows": _metadata_table(
                report.get("rows", []),
                _CANDIDATE_CONTRIBUTION_TRIAGE_TABLE_COLUMNS,
            ),
        }

    def _command_center_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        summary = report.get("summary", {})
        recommendations = report.get("recommendations", [])
        recommendation_rows = [
            {
                "severity": item.get("severity"),
                "area": item.get("area"),
                "action": item.get("action"),
                "job_name": item.get("job_name"),
                "reason": str(item.get("reason") or "")[:500],
            }
            for item in recommendations
        ]
        queue = report.get("research_brief_queue", {})
        leads = report.get("research_leads", {})
        return {
            "brief_queue_ready": dg.MetadataValue.int(int(summary.get("brief_queue_ready") or 0)),
            "brief_queue_failed": dg.MetadataValue.int(int(summary.get("brief_queue_failed") or 0)),
            "research_leads_actionable": dg.MetadataValue.int(int(summary.get("research_leads_actionable") or 0)),
            "recent_agent_failures": dg.MetadataValue.int(int(summary.get("recent_agent_failures") or 0)),
            "recommendation_count": dg.MetadataValue.int(int(summary.get("recommendation_count") or 0)),
            "blocking_recommendations": dg.MetadataValue.int(int(summary.get("blocking_recommendations") or 0)),
            "summary": dg.MetadataValue.json(summary),
            "queue_status_counts": dg.MetadataValue.json(queue.get("status_counts", {})),
            "lead_status_counts": dg.MetadataValue.json(leads.get("status_counts", {})),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "recommendations": _metadata_table(
                recommendation_rows,
                _COMMAND_CENTER_RECOMMENDATION_TABLE_COLUMNS,
            ),
        }

    def _research_hunt_queue_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "lead_count": dg.MetadataValue.int(int(report.get("lead_count") or 0)),
            "scanned_lead_count": dg.MetadataValue.int(int(report.get("scanned_lead_count") or 0)),
            "executable_task_count": dg.MetadataValue.int(int(report.get("executable_task_count") or 0)),
            "broad_task_count": dg.MetadataValue.int(int(report.get("broad_task_count") or 0)),
            "passive_task_count": dg.MetadataValue.int(int(report.get("passive_task_count") or 0)),
            "stale_task_count": dg.MetadataValue.int(int(report.get("stale_task_count") or 0)),
            "suppressed_task_count": dg.MetadataValue.int(int(report.get("suppressed_task_count") or 0)),
            "blocked_lead_count": dg.MetadataValue.int(int(report.get("blocked_lead_count") or 0)),
            "ready_for_synthesis_count": dg.MetadataValue.int(
                int(report.get("ready_for_synthesis_count") or 0)
            ),
            "hunting_count": dg.MetadataValue.int(int(report.get("hunting_count") or 0)),
            "watching_count": dg.MetadataValue.int(int(report.get("watching_count") or 0)),
            "status_counts": dg.MetadataValue.json(report.get("status_counts", {})),
            "task_class_counts": dg.MetadataValue.json(report.get("task_class_counts", {})),
            "control_status_counts": dg.MetadataValue.json(report.get("control_status_counts", {})),
            "lead_table": _metadata_table(report.get("leads", []), _RESEARCH_HUNT_LEAD_TABLE_COLUMNS),
            "task_table": _metadata_table(report.get("tasks", []), _RESEARCH_HUNT_TASK_TABLE_COLUMNS),
        }

    def _research_hunt_queue_maintenance_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "action": report.get("action"),
            "dry_run": bool(report.get("dry_run", True)),
            "candidate_count": dg.MetadataValue.int(int(report.get("candidate_count") or 0)),
            "suppressed_count": dg.MetadataValue.int(int(report.get("suppressed_count") or 0)),
            "updated_lead_count": dg.MetadataValue.int(int(report.get("updated_lead_count") or 0)),
            "skipped_count": dg.MetadataValue.int(int(report.get("skipped_count") or 0)),
            "error_count": dg.MetadataValue.int(len(report.get("errors", []))),
            "items": _metadata_table(report.get("items", []), _RESEARCH_HUNT_MAINTENANCE_TABLE_COLUMNS),
            "skipped": dg.MetadataValue.json(report.get("skipped", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
        }

    def _agent_performance_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "agent_run_count": dg.MetadataValue.int(int(report.get("agent_run_count") or 0)),
            "reviewed_run_count": dg.MetadataValue.int(int(report.get("reviewed_run_count") or 0)),
            "unreviewed_run_count": dg.MetadataValue.int(int(report.get("unreviewed_run_count") or 0)),
            "operator_reviewed_count": dg.MetadataValue.int(int(report.get("operator_reviewed_count") or 0)),
            "evaluator_reviewed_count": dg.MetadataValue.int(int(report.get("evaluator_reviewed_count") or 0)),
            "disagreement_count": dg.MetadataValue.int(int(report.get("disagreement_count") or 0)),
            "review_coverage": float(report.get("review_coverage") or 0.0),
            "verdict_counts": dg.MetadataValue.json(report.get("verdict_counts", {})),
            "reviewer_type_counts": dg.MetadataValue.json(report.get("reviewer_type_counts", {})),
            "top_rows": _metadata_table(report.get("top_rows", []), _AGENT_PERFORMANCE_TABLE_COLUMNS),
            "bottom_rows": _metadata_table(report.get("bottom_rows", []), _AGENT_PERFORMANCE_TABLE_COLUMNS),
        }

    def _agent_performance_evaluation_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "agent_run_id": str(report.get("agent_run_id") or ""),
            "scanned_count": dg.MetadataValue.int(int(report.get("scanned_count") or 0)),
            "candidate_count": dg.MetadataValue.int(int(report.get("candidate_count") or 0)),
            "evaluated_count": dg.MetadataValue.int(int(report.get("evaluated_count") or 0)),
            "review_created_count": dg.MetadataValue.int(int(report.get("review_created_count") or 0)),
            "failed_count": dg.MetadataValue.int(int(report.get("failed_count") or 0)),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "evaluations": _metadata_table(
                report.get("evaluations", []),
                _AGENT_PERFORMANCE_EVALUATION_TABLE_COLUMNS,
            ),
        }

    def _reward_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "event_count": dg.MetadataValue.int(int(report.get("event_count") or 0)),
            "reward_score": dg.MetadataValue.int(int(report.get("reward_score") or 0)),
            "average_score": float(report.get("average_score") or 0.0),
            "event_source_counts": dg.MetadataValue.json(report.get("event_source_counts", {})),
            "verdict_counts": dg.MetadataValue.json(report.get("verdict_counts", {})),
            "outcome_counts": dg.MetadataValue.json(report.get("outcome_counts", {})),
            "actionable_followup_count": dg.MetadataValue.int(int(report.get("actionable_followup_count") or 0)),
            "low_value_churn_count": dg.MetadataValue.int(int(report.get("low_value_churn_count") or 0)),
            "top_rows": _metadata_table(report.get("top_rows", []), _REWARD_REPORT_TABLE_COLUMNS),
            "bottom_rows": _metadata_table(report.get("bottom_rows", []), _REWARD_REPORT_TABLE_COLUMNS),
        }

    def _reward_sync_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "scanned_review_count": dg.MetadataValue.int(int(report.get("scanned_review_count") or 0)),
            "eligible_review_count": dg.MetadataValue.int(int(report.get("eligible_review_count") or 0)),
            "created_count": dg.MetadataValue.int(int(report.get("created_count") or 0)),
            "skipped_existing_count": dg.MetadataValue.int(int(report.get("skipped_existing_count") or 0)),
            "missing_run_count": dg.MetadataValue.int(int(report.get("missing_run_count") or 0)),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
        }

    def _entity_resolution_source_rows(report: Mapping[str, Any]) -> list[dict[str, Any]]:
        rows = []
        for source_report in report.get("sources", []):
            resolution = source_report.get("resolution", {})
            qa = source_report.get("qa", {})
            rows.append(
                {
                    "source_key": source_report.get("source_key"),
                    "chunks_seen": resolution.get("chunks_seen", 0),
                    "chunks_with_mentions": resolution.get("chunks_with_mentions", 0),
                    "entities_upserted": resolution.get("entities_upserted", 0),
                    "aliases_upserted": resolution.get("aliases_upserted", 0),
                    "mentions_upserted": resolution.get("mentions_upserted", 0),
                    "entity_mentions": qa.get("entity_mentions", 0),
                    "claims": qa.get("claims", 0),
                    "passes_minimum_bar": qa.get("passes_minimum_bar"),
                    "errors": resolution.get("errors", []),
                }
            )
        return rows

    def _entity_resolution_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        errors = report.get("errors", [])
        totals = report.get("totals", {})
        return {
            **_base_report_metadata(report),
            "error_count": len(errors),
            "errors": dg.MetadataValue.json(errors),
            "minimum_entity_mentions": 1,
            "passes_minimum_bar": not errors and totals.get("entity_mentions", 0) >= 1,
            "entity_resolution_table": _metadata_table(
                _entity_resolution_source_rows(report),
                _ENTITY_RESOLUTION_TABLE_COLUMNS,
            ),
        }

    def _entity_lookup_index_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        errors = report.get("errors", [])
        totals = report.get("totals", {})
        return {
            "source_count": len(report.get("source_keys", [])),
            "source_keys": dg.MetadataValue.json(report.get("source_keys", [])),
            "records_seen": dg.MetadataValue.int(int(totals.get("records_seen") or 0)),
            "entities_upserted": dg.MetadataValue.int(int(totals.get("entities_upserted") or 0)),
            "aliases_upserted": dg.MetadataValue.int(int(totals.get("aliases_upserted") or 0)),
            "source_versions_upserted": dg.MetadataValue.int(int(totals.get("source_versions_upserted") or 0)),
            "error_count": len(errors),
            "errors": dg.MetadataValue.json(errors),
            "coverage": dg.MetadataValue.json(report.get("coverage", {})),
            "source_table": _metadata_table(
                report.get("sources", []),
                _ENTITY_LOOKUP_INDEX_TABLE_COLUMNS,
            ),
        }

    def _embedding_index_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        embedding_coverage = report.get("embedding_coverage", {})
        totals = report.get("totals", {})
        errors = report.get("errors", [])
        return {
            "embedding_model": report.get("embedding_model"),
            "source_key": report.get("source_key"),
            "chunks_seen": totals.get("chunks_seen", 0),
            "embeddings_created": totals.get("embeddings_created", 0),
            "embeddings_updated": totals.get("embeddings_updated", 0),
            "embeddings_skipped": totals.get("embeddings_skipped", 0),
            "error_count": len(errors),
            "errors": dg.MetadataValue.json(errors),
            "total_chunks": embedding_coverage.get("total_chunks", 0),
            "embedded_chunks": embedding_coverage.get("embedded_chunks", 0),
            "missing_chunks": embedding_coverage.get("missing_chunks", 0),
            "coverage_ratio": embedding_coverage.get("coverage_ratio", 0.0),
            "embedding_models": dg.MetadataValue.json(embedding_coverage.get("embedding_models", {})),
            "coverage": dg.MetadataValue.json(report.get("coverage", {})),
            "passes_minimum_bar": bool(report.get("passes_minimum_bar", False)),
        }

    def _embedding_maintenance_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        embedding_coverage = report.get("embedding_coverage", {})
        orphan_embeddings = report.get("orphan_embeddings", {})
        errors = report.get("errors", [])
        return {
            "embedding_model": report.get("embedding_model"),
            "prune_embedding_model": orphan_embeddings.get("embedding_model", "all"),
            "orphan_embeddings_seen": orphan_embeddings.get("seen", 0),
            "orphan_embeddings_deleted": orphan_embeddings.get("deleted", 0),
            "prune_enabled": bool(orphan_embeddings.get("prune_enabled", False)),
            "error_count": len(errors),
            "errors": dg.MetadataValue.json(errors),
            "total_chunks": embedding_coverage.get("total_chunks", 0),
            "embedded_chunks": embedding_coverage.get("embedded_chunks", 0),
            "missing_chunks": embedding_coverage.get("missing_chunks", 0),
            "coverage_ratio": embedding_coverage.get("coverage_ratio", 0.0),
            "embedding_models": dg.MetadataValue.json(embedding_coverage.get("embedding_models", {})),
            "coverage": dg.MetadataValue.json(report.get("coverage", {})),
            "passes_minimum_bar": bool(report.get("passes_minimum_bar", False)),
        }

    def _full_text_ops_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        actions = report.get("actions", [])
        blocking_actions = [
            action
            for action in actions
            if action.get("severity") == "blocking" or action.get("action") == "keep_schedule_stopped"
        ]
        return {
            "agent_run_id": report.get("agent_run_id"),
            "action_count": len(actions),
            "blocking_action_count": len(blocking_actions),
            "should_block_schedule": bool(report.get("should_block_schedule", False)),
            "schedule_readiness": report.get("schedule_readiness"),
            "error_count": len(report.get("errors", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "evidence": dg.MetadataValue.json(report.get("evidence", {})),
            "actions": _metadata_table(actions, _FULL_TEXT_OPS_ACTION_TABLE_COLUMNS),
        }

    def _research_brief_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        perspective_reports = report.get("perspective_reports", [])
        finding_count = sum(len(item.get("findings", [])) for item in perspective_reports)
        evidence = report.get("evidence", {})
        perspective_rows = [
            {
                "perspective": item.get("perspective"),
                "agent_name": item.get("agent_name"),
                "finding_count": len(item.get("findings", [])),
                "citation_count": len(item.get("citations", [])),
                "error_count": len(item.get("errors", [])),
                "summary": str(item.get("summary") or "")[:500],
            }
            for item in perspective_reports
        ]
        citation_rows = [
            {
                "citation_id": citation.get("citation_id"),
                "source_key": citation.get("source_key"),
                "title": str(citation.get("title") or "")[:300],
                "section_label": citation.get("section_label"),
                "match_type": (citation.get("metadata") or {}).get("match_type"),
                "score": (citation.get("metadata") or {}).get("score"),
                "relevance": str(citation.get("relevance") or "")[:500],
            }
            for citation in report.get("citations", [])[:50]
        ]
        return {
            "brief_id": report.get("brief_id"),
            "agent_run_id": report.get("agent_run_id"),
            "agent_run_ids": dg.MetadataValue.json(report.get("agent_run_ids", [])),
            "topic": report.get("topic"),
            "brief_style": report.get("brief_style"),
            "review_mode": evidence.get("review_mode"),
            "retrieval_strategy": evidence.get("retrieval_strategy"),
            "search_query_count": dg.MetadataValue.int(int(evidence.get("search_query_count", 0))),
            "research_lead_count": dg.MetadataValue.int(int(evidence.get("research_lead_count", 0))),
            "perspective_count": len(perspective_reports),
            "finding_count": dg.MetadataValue.int(int(finding_count)),
            "hypothesis_count": dg.MetadataValue.int(len(report.get("ranked_hypotheses", []))),
            "citation_count": dg.MetadataValue.int(len(report.get("citations", []))),
            "error_count": len(report.get("errors", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "evidence": dg.MetadataValue.json(evidence),
            "perspectives": _metadata_table(
                perspective_rows,
                _RESEARCH_BRIEF_PERSPECTIVE_TABLE_COLUMNS,
            ),
            "citations": _metadata_table(citation_rows, _RESEARCH_BRIEF_CITATION_TABLE_COLUMNS),
            "brief_preview": dg.MetadataValue.md(str(report.get("final_brief") or "")[:4000]),
        }

    def _therapy_committee_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        reports = report.get("reports", [])
        ideas = report.get("ranked_ideas", [])
        report_rows = [
            {
                "perspective": item.get("perspective"),
                "agent_name": item.get("agent_name"),
                "idea_count": len(item.get("ideas", [])),
                "evidence_limitation_count": len(item.get("evidence_limitations", [])),
                "error_count": len(item.get("errors", [])),
                "summary": str(item.get("summary") or "")[:500],
            }
            for item in reports
        ]
        idea_rows = [
            {
                "title": str(idea.get("title") or "")[:300],
                "priority_score": idea.get("priority_score"),
                "evidence_strength": idea.get("evidence_strength"),
                "candidate_therapies": ", ".join(idea.get("candidate_therapies", [])[:6]),
                "targets": ", ".join(idea.get("targets", [])[:6]),
                "biomarkers": ", ".join(idea.get("biomarkers", [])[:6]),
                "evidence_refs": ", ".join(idea.get("evidence_refs", [])[:8]),
            }
            for idea in ideas
        ]
        return {
            "committee_run_id": report.get("committee_run_id"),
            "agent_run_id": report.get("agent_run_id"),
            "source_program_id": report.get("source_program_id"),
            "topic": report.get("topic"),
            "review_mode": report.get("review_mode"),
            "perspective_count": len(reports),
            "idea_count": dg.MetadataValue.int(len(ideas)),
            "error_count": len(report.get("errors", [])),
            "evidence": dg.MetadataValue.json(report.get("evidence", {})),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "committee_reports": _metadata_table(report_rows, _THERAPY_COMMITTEE_REPORT_TABLE_COLUMNS),
            "ranked_ideas": _metadata_table(idea_rows, _THERAPY_IDEA_TABLE_COLUMNS),
            "decision_summary": dg.MetadataValue.md(str(report.get("decision_summary") or "")[:4000]),
        }

    def _validation_tool_catalog_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = [
            {
                "tool_key": tool.get("tool_key"),
                "category": tool.get("category"),
                "runner_status": tool.get("runner_status"),
                "tool_hint": tool.get("tool_hint"),
                "validation_types": ", ".join(tool.get("compatible_validation_types") or []),
                "task_types": ", ".join(tool.get("compatible_task_types") or []),
                "quality_gates": ", ".join((tool.get("quality_gates") or [])[:5]),
            }
            for tool in report.get("tools", [])
        ]
        return {
            "tool_count": dg.MetadataValue.int(int(report.get("tool_count", 0))),
            "runner_status_counts": dg.MetadataValue.json(report.get("runner_status_counts", {})),
            "category_counts": dg.MetadataValue.json(report.get("category_counts", {})),
            "tools": _metadata_table(rows, _VALIDATION_TOOL_CATALOG_TABLE_COLUMNS),
        }

    def _therapy_idea_library_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = []
        for record in report.get("ideas", []):
            idea = record.get("idea") or {}
            rows.append(
                {
                    "therapy_idea_id": record.get("therapy_idea_id"),
                    "status": record.get("status"),
                    "promotion_state": record.get("promotion_state"),
                    "score": record.get("score"),
                    "title": str(idea.get("title") or "")[:300],
                    "candidate_therapies": ", ".join(record.get("candidate_therapies") or []),
                    "targets": ", ".join(record.get("targets") or []),
                    "evidence_refs": ", ".join(record.get("evidence_refs") or []),
                }
            )
        return {
            "idea_count": dg.MetadataValue.int(int(report.get("idea_count", 0))),
            "status_counts": dg.MetadataValue.json(report.get("status_counts", {})),
            "ideas": _metadata_table(rows, _THERAPY_IDEA_LIBRARY_TABLE_COLUMNS),
        }

    def _public_candidate_snapshot_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        candidate = report.get("candidate") if isinstance(report.get("candidate"), Mapping) else {}
        snapshot = report.get("snapshot") if isinstance(report.get("snapshot"), Mapping) else {}
        gate = report.get("moonshot_gate") if isinstance(report.get("moonshot_gate"), Mapping) else {}
        gate_rows = [
            {
                "passed": gate.get("passed"),
                "priority_score": gate.get("priority_score"),
                "min_priority_score": gate.get("min_priority_score"),
                "has_program_lineage": gate.get("has_program_lineage"),
                "evidence_ref_count": gate.get("evidence_ref_count"),
                "reasons": ", ".join(gate.get("reasons") or []),
                "blockers": ", ".join(gate.get("blockers") or []),
            }
        ]
        return {
            "candidate_id": dg.MetadataValue.text(str(candidate.get("candidate_id") or "")),
            "trace_id": dg.MetadataValue.text(str(snapshot.get("trace_id") or candidate.get("trace_id") or "")),
            "run_manifest_id": dg.MetadataValue.text(
                str((snapshot.get("metadata") or {}).get("run_manifest_id") or "")
            ),
            "display_id": dg.MetadataValue.text(str(candidate.get("display_id") or "")),
            "public_status": dg.MetadataValue.text(str(candidate.get("public_status") or "")),
            "visibility": dg.MetadataValue.text(str(candidate.get("visibility") or "")),
            "content_hash": dg.MetadataValue.text(str(snapshot.get("content_hash") or "")),
            "snapshot_version": dg.MetadataValue.int(int(snapshot.get("snapshot_version") or 0)),
            "moonshot_gate_passed": dg.MetadataValue.bool(bool(gate.get("passed"))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "moonshot_gate": _metadata_table(gate_rows, _PUBLIC_CANDIDATE_GATE_TABLE_COLUMNS),
        }

    def _public_candidate_integrity_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        checks = report.get("checks") if isinstance(report.get("checks"), Sequence) else []
        rows = []
        for check in checks:
            if not isinstance(check, Mapping):
                continue
            rows.append(
                {
                    "candidate_id": check.get("candidate_id"),
                    "candidate_found": check.get("candidate_found"),
                    "therapy_idea_found": check.get("therapy_idea_found"),
                    "latest_snapshot_found": check.get("latest_snapshot_found"),
                    "trace_id": check.get("trace_id"),
                    "run_manifest_id": check.get("run_manifest_id"),
                    "run_manifest_found": check.get("run_manifest_found"),
                    "strict_export_ready": check.get("strict_export_ready"),
                    "problems": ", ".join(check.get("problems") or []),
                }
            )
        return {
            "repository_type": dg.MetadataValue.text(str(report.get("repository_type") or "")),
            "strict_export_ready": dg.MetadataValue.bool(bool(report.get("strict_export_ready"))),
            "candidate_sample_count": dg.MetadataValue.int(int(report.get("candidate_sample_count") or 0)),
            "snapshot_sample_count": dg.MetadataValue.int(int(report.get("snapshot_sample_count") or 0)),
            "therapy_idea_sample_count": dg.MetadataValue.int(int(report.get("therapy_idea_sample_count") or 0)),
            "run_manifest_sample_count": dg.MetadataValue.int(int(report.get("run_manifest_sample_count") or 0)),
            "missing_candidate_ids": dg.MetadataValue.json(report.get("missing_candidate_ids", [])),
            "missing_therapy_idea_ids": dg.MetadataValue.json(report.get("missing_therapy_idea_ids", [])),
            "candidates_missing_manifest_receipt": dg.MetadataValue.json(
                report.get("candidates_missing_manifest_receipt", [])
            ),
            "checks": _metadata_table(rows, _PUBLIC_CANDIDATE_INTEGRITY_TABLE_COLUMNS),
        }

    def _hypothesis_promotion_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = [
            {
                "candidate_id": candidate.get("candidate_id"),
                "source_type": candidate.get("source_type"),
                "promotion_state": candidate.get("promotion_state"),
                "score": candidate.get("score"),
                "title": str(candidate.get("title") or "")[:300],
                "recommended_job_name": candidate.get("recommended_job_name"),
                "blockers": ", ".join(candidate.get("blockers") or []),
                "matched_tools": ", ".join(
                    [
                        ((match.get("tool") or {}).get("tool_key") or "")
                        for match in candidate.get("matched_tools", [])
                    ]
                ),
            }
            for candidate in report.get("candidates", [])
        ]
        return {
            "candidate_count": dg.MetadataValue.int(int(report.get("candidate_count", 0))),
            "state_counts": dg.MetadataValue.json(report.get("state_counts", {})),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "candidates": _metadata_table(rows, _HYPOTHESIS_PROMOTION_TABLE_COLUMNS),
        }

    def _validation_packet_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = []
        for packet in report.get("packets", []):
            summary = packet.get("summary") if isinstance(packet.get("summary"), dict) else {}
            rows.append(
                {
                    "packet_id": packet.get("packet_id"),
                    "status": packet.get("status"),
                    "readiness": packet.get("readiness"),
                    "discovery_readiness": packet.get("discovery_readiness"),
                    "validation_strategy_readiness": packet.get("validation_strategy_readiness"),
                    "protocol_readiness": packet.get("protocol_readiness"),
                    "score": packet.get("score"),
                    "source_type": packet.get("source_type"),
                    "title": str(packet.get("title") or "")[:300],
                    "candidate_therapies": ", ".join(packet.get("candidate_therapies") or []),
                    "targets": ", ".join(packet.get("targets") or []),
                    "matched_tools": ", ".join(
                        [
                            ((match.get("tool") or {}).get("tool_key") or "")
                            for match in packet.get("matched_tools", [])
                        ]
                    ),
                    "task_count": summary.get("task_count", len(packet.get("validation_tasks") or [])),
                    "queue_item_count": summary.get("queue_item_count", len(packet.get("queue_items") or [])),
                    "dispatch_blocker_count": summary.get(
                        "dispatch_blocker_count",
                        len(packet.get("dispatch_blockers") or []),
                    ),
                    "risk_annotation_count": summary.get(
                        "risk_annotation_count",
                        len(packet.get("risk_annotations") or []),
                    ),
                    "protocol_blocker_count": summary.get(
                        "protocol_blocker_count",
                        len(packet.get("protocol_blockers") or []),
                    ),
                    "follow_up_count": summary.get(
                        "follow_up_count",
                        (packet.get("evidence_addendum") or {}).get("follow_up_count", 0),
                    ),
                    "evaluated_follow_up_count": summary.get(
                        "evaluated_follow_up_count",
                        (packet.get("evidence_addendum") or {}).get("evaluated_follow_up_count", 0),
                    ),
                    "passing_follow_up_count": summary.get(
                        "passing_follow_up_count",
                        (packet.get("evidence_addendum") or {}).get("passing_follow_up_count", 0),
                    ),
                }
            )
        return {
            "packet_count": dg.MetadataValue.int(int(report.get("packet_count", 0))),
            "ready_count": dg.MetadataValue.int(int(report.get("ready_count", 0))),
            "blocked_count": dg.MetadataValue.int(int(report.get("blocked_count", 0))),
            "queued_count": dg.MetadataValue.int(int(report.get("queued_count", 0))),
            "existing_queue_count": dg.MetadataValue.int(int(report.get("existing_queue_count", 0))),
            "dry_run": bool(report.get("dry_run", True)),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "packets": _metadata_table(rows, _VALIDATION_PACKET_TABLE_COLUMNS),
        }

    def _validation_decision_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = [
            {
                "decision_id": decision.get("decision_id"),
                "packet_id": decision.get("packet_id"),
                "outcome": decision.get("outcome"),
                "confidence": decision.get("confidence"),
                "validation_ready": decision.get("validation_ready"),
                "specific_claim_viability": decision.get("specific_claim_viability"),
                "broader_program_signal": decision.get("broader_program_signal"),
                "title": str(decision.get("title") or "")[:300],
                "recommended_downstream_action": str(decision.get("recommended_downstream_action") or "")[:500],
                "blocking_reason_count": len(decision.get("blocking_reasons") or []),
            }
            for decision in report.get("decisions", [])
        ]
        return {
            "decision_count": dg.MetadataValue.int(int(report.get("decision_count", 0))),
            "packet_count": dg.MetadataValue.int(int(report.get("packet_count", 0))),
            "persisted_decision_count": dg.MetadataValue.int(int(report.get("persisted_decision_count", 0))),
            "validation_ready_count": dg.MetadataValue.int(int(report.get("validation_ready_count", 0))),
            "outcome_counts": dg.MetadataValue.json(report.get("outcome_counts", {})),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "decisions": _metadata_table(rows, _VALIDATION_DECISION_TABLE_COLUMNS),
        }

    def _research_program_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = []
        for program in report.get("programs", []):
            rows.append(
                {
                    "program_id": program.get("program_id"),
                    "status": program.get("status"),
                    "gate_decision": program.get("gate_decision"),
                    "confidence_score": program.get("confidence_score"),
                    "evidence_loop_count": program.get("evidence_loop_count"),
                    "max_evidence_loops": program.get("max_evidence_loops"),
                    "title": str(program.get("title") or "")[:300],
                    "thesis_area": program.get("thesis_area"),
                    "question_count": len(program.get("decisive_questions") or []),
                    "evidence_task_count": len(program.get("evidence_tasks") or []),
                    "therapy_families": ", ".join(program.get("therapy_families") or []),
                    "recommended_tools": ", ".join(program.get("recommended_tools") or []),
                    "stop_criteria": " | ".join((program.get("stop_criteria") or [])[:3]),
                }
            )
        metadata = {
            "program_count": dg.MetadataValue.int(int(report.get("program_count", 0))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "programs": _metadata_table(rows, _RESEARCH_PROGRAM_TABLE_COLUMNS),
        }
        if "persisted_count" in report:
            metadata["persisted_count"] = dg.MetadataValue.int(int(report.get("persisted_count", 0)))
        if "packet_count" in report:
            metadata["packet_count"] = dg.MetadataValue.int(int(report.get("packet_count", 0)))
        if "evidence_chunk_count" in report:
            metadata["evidence_chunk_count"] = dg.MetadataValue.int(int(report.get("evidence_chunk_count", 0)))
        if "status_counts" in report:
            metadata["status_counts"] = dg.MetadataValue.json(report.get("status_counts", {}))
        if "gate_counts" in report:
            metadata["gate_counts"] = dg.MetadataValue.json(report.get("gate_counts", {}))
        return metadata

    def _research_program_evidence_loop_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = []
        for task in report.get("task_results", []):
            rows.append(
                {
                    "task_id": task.get("task_id"),
                    "title": str(task.get("title") or "")[:300],
                    "status_before": task.get("status_before"),
                    "status_after": task.get("status_after"),
                    "research_lead_id": task.get("research_lead_id"),
                    "brief_queue_item_id": task.get("brief_queue_item_id"),
                    "selected_source_keys": ", ".join(task.get("selected_source_keys") or []),
                    "source_query_count": len(task.get("source_query_names") or []),
                    "errors": " | ".join(task.get("errors") or []),
                }
            )
        return {
            "program_id": report.get("program_id"),
            "program_title": report.get("program_title"),
            "dry_run": bool(report.get("dry_run", False)),
            "blocked": bool(report.get("blocked", False)),
            "loop_count_before": dg.MetadataValue.int(int(report.get("loop_count_before", 0))),
            "loop_count_after": dg.MetadataValue.int(int(report.get("loop_count_after", 0))),
            "max_evidence_loops": dg.MetadataValue.int(int(report.get("max_evidence_loops", 0))),
            "selected_task_count": dg.MetadataValue.int(int(report.get("selected_task_count", 0))),
            "research_lead_count": dg.MetadataValue.int(int(report.get("research_lead_count", 0))),
            "source_query_count": dg.MetadataValue.int(int(report.get("source_query_count", 0))),
            "brief_queue_count": dg.MetadataValue.int(int(report.get("brief_queue_count", 0))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "task_results": _metadata_table(rows, _RESEARCH_PROGRAM_EVIDENCE_LOOP_TABLE_COLUMNS),
        }

    def _omics_accession_hunt_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = []
        for hit in report.get("accession_hits", []):
            rows.append(
                {
                    "source_key": hit.get("source_key"),
                    "accession": hit.get("accession"),
                    "identifier_type": hit.get("identifier_type"),
                    "organism": hit.get("organism"),
                    "sample_count": hit.get("sample_count"),
                    "library_strategy": hit.get("library_strategy"),
                    "bioproject": hit.get("bioproject"),
                    "pmid": hit.get("pmid"),
                    "matched_terms": ", ".join(hit.get("matched_terms") or []),
                    "title": str(hit.get("title") or "")[:300],
                }
            )
        return {
            "program_id": report.get("program_id"),
            "dry_run": bool(report.get("dry_run", False)),
            "query_count": dg.MetadataValue.int(int(report.get("query_count", 0))),
            "raw_records": dg.MetadataValue.int(int(report.get("raw_records", 0))),
            "research_objects": dg.MetadataValue.int(int(report.get("research_objects", 0))),
            "document_chunks": dg.MetadataValue.int(int(report.get("document_chunks", 0))),
            "accession_hit_count": dg.MetadataValue.int(int(report.get("accession_hit_count", 0))),
            "negative_query_count": dg.MetadataValue.int(len(report.get("negative_queries", []))),
            "negative_queries": dg.MetadataValue.json(report.get("negative_queries", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "accession_hits": _metadata_table(rows, _OMICS_ACCESSION_HUNT_TABLE_COLUMNS),
        }

    def _omics_evidence_packet_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = []
        for packet in report.get("packets", []):
            rows.append(
                {
                    "packet_key": packet.get("packet_key"),
                    "readiness": packet.get("readiness"),
                    "score": packet.get("score"),
                    "dataset_count": packet.get("dataset_count", 0),
                    "direct_dataset_count": packet.get("direct_dataset_count", 0),
                    "analog_dataset_count": packet.get("analog_dataset_count", 0),
                    "total_sample_count": packet.get("total_sample_count"),
                    "source_keys": ", ".join(packet.get("source_keys") or []),
                    "accessions": ", ".join((packet.get("accessions") or [])[:12]),
                    "dispatch_blockers": ", ".join((packet.get("dispatch_blockers") or [])[:8]),
                    "title": str(packet.get("title") or "")[:300],
                }
            )
        return {
            "program_id": report.get("program_id"),
            "dry_run": bool(report.get("dry_run", False)),
            "packet_count": dg.MetadataValue.int(int(report.get("packet_count", 0))),
            "scanned_dataset_count": dg.MetadataValue.int(int(report.get("scanned_dataset_count", 0))),
            "selected_dataset_count": dg.MetadataValue.int(int(report.get("selected_dataset_count", 0))),
            "direct_dataset_count": dg.MetadataValue.int(int(report.get("direct_dataset_count", 0))),
            "analog_dataset_count": dg.MetadataValue.int(int(report.get("analog_dataset_count", 0))),
            "context_dataset_count": dg.MetadataValue.int(int(report.get("context_dataset_count", 0))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "skipped": dg.MetadataValue.json((report.get("skipped") or [])[:25]),
            "packets": _metadata_table(rows, _OMICS_EVIDENCE_PACKET_TABLE_COLUMNS),
        }

    def _omics_readout_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = []
        for item in report.get("datasets", []):
            dataset = item.get("dataset") or {}
            target = item.get("target_expression") or {}
            rows.append(
                {
                    "accession": dataset.get("accession"),
                    "status": item.get("status"),
                    "sample_count": item.get("sample_count", 0),
                    "gene_count": item.get("gene_count", 0),
                    "tumor_sample_count": item.get("tumor_sample_count", 0),
                    "control_sample_count": item.get("control_sample_count", 0),
                    "target_support": target.get("support_level"),
                    "target_effect_size": target.get("effect_size"),
                    "matrix_uri": str(item.get("matrix_uri") or "")[:300],
                    "limitations": ", ".join((item.get("limitations") or [])[:8]),
                }
            )
        validation = report.get("validation_agent_result") or {}
        return {
            "program_id": report.get("program_id"),
            "therapy_idea_id": report.get("therapy_idea_id"),
            "packet_id": report.get("packet_id"),
            "packet_key": report.get("packet_key"),
            "dry_run": bool(report.get("dry_run", False)),
            "dataset_count": dg.MetadataValue.int(int(report.get("dataset_count", 0))),
            "computed_count": dg.MetadataValue.int(int(report.get("computed_count", 0))),
            "skipped_count": dg.MetadataValue.int(int(report.get("skipped_count", 0))),
            "failed_count": dg.MetadataValue.int(int(report.get("failed_count", 0))),
            "artifact_ids": dg.MetadataValue.json(report.get("artifact_ids", [])),
            "validation_agent_decision": validation.get("decision"),
            "validation_agent_confidence": validation.get("confidence"),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "datasets": _metadata_table(rows, _OMICS_READOUT_TABLE_COLUMNS),
        }

    def _omics_locus_signal_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = []
        for item in report.get("datasets", []):
            dataset = item.get("dataset") or {}
            rows.append(
                {
                    "accession": dataset.get("accession"),
                    "status": item.get("status"),
                    "sample_count": item.get("sample_count", 0),
                    "computed_sample_count": item.get("computed_sample_count", 0),
                    "tumor_sample_count": item.get("tumor_sample_count", 0),
                    "control_sample_count": item.get("control_sample_count", 0),
                    "support_level": item.get("support_level"),
                    "effect_size": item.get("effect_size"),
                    "tumor_control_delta": item.get("tumor_control_delta"),
                    "comparison_p_value": item.get("comparison_p_value"),
                    "normalization_status": item.get("normalization_status"),
                    "limitations": ", ".join((item.get("limitations") or [])[:8]),
                }
            )
        validation = report.get("validation_agent_result") or {}
        return {
            "dry_run": bool(report.get("dry_run", False)),
            "dataset_count": dg.MetadataValue.int(int(report.get("dataset_count", 0))),
            "computed_count": dg.MetadataValue.int(int(report.get("computed_count", 0))),
            "skipped_count": dg.MetadataValue.int(int(report.get("skipped_count", 0))),
            "failed_count": dg.MetadataValue.int(int(report.get("failed_count", 0))),
            "validation_agent_decision": validation.get("decision"),
            "validation_agent_confidence": validation.get("confidence"),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "datasets": _metadata_table(rows, _OMICS_LOCUS_SIGNAL_TABLE_COLUMNS),
        }

    def _omics_followup_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = []
        for item in report.get("tasks", []):
            rows.append(
                {
                    "task_type": item.get("task_type"),
                    "priority": item.get("priority"),
                    "title": item.get("title"),
                    "source_keys": ", ".join(item.get("source_keys") or []),
                    "target_genes": ", ".join(item.get("target_genes") or []),
                    "accessions": ", ".join((item.get("accessions") or [])[:5]),
                    "query_text": str(item.get("query_text") or "")[:300],
                }
            )
        return {
            "dry_run": bool(report.get("dry_run", True)),
            "scanned_dataset_count": dg.MetadataValue.int(int(report.get("scanned_dataset_count", 0))),
            "generated_task_count": dg.MetadataValue.int(int(report.get("generated_task_count", 0))),
            "persisted_research_lead_count": dg.MetadataValue.int(
                int(report.get("persisted_research_lead_count", 0))
            ),
            "persisted_source_query_count": dg.MetadataValue.int(int(report.get("persisted_source_query_count", 0))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "tasks": _metadata_table(rows, _OMICS_FOLLOWUP_TASK_TABLE_COLUMNS),
        }

    def _research_brief_library_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = report.get("briefs", [])
        return {
            "brief_count": dg.MetadataValue.int(int(report.get("brief_count", 0))),
            "status": report.get("status"),
            "source_key": report.get("source_key"),
            "topic_query": report.get("topic_query"),
            "briefs": _metadata_table(rows, _RESEARCH_BRIEF_LIBRARY_TABLE_COLUMNS),
        }

    def _research_brief_evaluation_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        scores = report.get("scores", {})
        return {
            "evaluation_id": report.get("evaluation_id"),
            "brief_id": report.get("brief_id"),
            "agent_run_id": report.get("agent_run_id"),
            "topic": report.get("topic"),
            "source_key": report.get("source_key"),
            "overall_score": float(report.get("overall_score", 0.0)),
            "passes_quality_bar": bool(report.get("passes_quality_bar", False)),
            "readiness": report.get("readiness"),
            "recommendation_count": len(report.get("recommendations", [])),
            "error_count": len(report.get("errors", [])),
            "scores": dg.MetadataValue.json(scores),
            "evidence": dg.MetadataValue.json(report.get("evidence", {})),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "strengths": dg.MetadataValue.json(report.get("strengths", [])),
            "weaknesses": dg.MetadataValue.json(report.get("weaknesses", [])),
            "recommendations": dg.MetadataValue.json(report.get("recommendations", [])),
        }

    def _research_brief_evaluation_library_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = report.get("evaluations", [])
        return {
            "evaluation_count": dg.MetadataValue.int(int(report.get("evaluation_count", 0))),
            "brief_id": report.get("brief_id"),
            "readiness": report.get("readiness"),
            "passes_quality_bar": report.get("passes_quality_bar"),
            "evaluations": _metadata_table(rows, _RESEARCH_BRIEF_EVALUATION_TABLE_COLUMNS),
        }

    def _research_brief_quality_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = [
            {
                **row,
                "topic": str(row.get("topic") or "")[:300],
                "review_models": ", ".join(row.get("review_models") or []),
            }
            for row in report.get("rows", [])
        ]
        return {
            "brief_count": dg.MetadataValue.int(int(report.get("brief_count", 0))),
            "evaluated_count": dg.MetadataValue.int(int(report.get("evaluated_count", 0))),
            "ready_count": dg.MetadataValue.int(int(report.get("ready_count", 0))),
            "failed_count": dg.MetadataValue.int(int(report.get("failed_count", 0))),
            "followup_count": dg.MetadataValue.int(int(report.get("followup_count", 0))),
            "needs_evaluation_count": dg.MetadataValue.int(int(report.get("needs_evaluation_count", 0))),
            "average_overall_score": (
                float(report["average_overall_score"])
                if report.get("average_overall_score") is not None
                else None
            ),
            "status_counts": dg.MetadataValue.json(report.get("status_counts", {})),
            "quality_status_counts": dg.MetadataValue.json(report.get("quality_status_counts", {})),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "briefs": _metadata_table(rows, _RESEARCH_BRIEF_QUALITY_TABLE_COLUMNS),
        }

    def _research_brief_followup_queue_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = [
            {
                **lead,
                "title": str(lead.get("title") or "")[:300],
                "reason": str(lead.get("reason") or "")[:300],
                "evidence_refs": ", ".join(lead.get("evidence_refs") or []),
            }
            for lead in report.get("followup_leads", [])
        ]
        return {
            "candidate_brief_count": dg.MetadataValue.int(int(report.get("candidate_brief_count", 0))),
            "limitation_count": dg.MetadataValue.int(int(report.get("limitation_count", 0))),
            "queued_count": dg.MetadataValue.int(int(report.get("queued_count", 0))),
            "existing_count": dg.MetadataValue.int(int(report.get("existing_count", 0))),
            "skipped_count": dg.MetadataValue.int(int(report.get("skipped_count", 0))),
            "dry_run": bool(report.get("dry_run", False)),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "skipped": dg.MetadataValue.json(report.get("skipped", [])),
            "followup_leads": _metadata_table(rows, _RESEARCH_BRIEF_FOLLOWUP_QUEUE_TABLE_COLUMNS),
        }

    def _validation_plan_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        task_rows = []
        for task in report.get("tasks", []):
            validation_request = task.get("validation_request") or {}
            task_rows.append(
                {
                    "task_id": task.get("task_id"),
                    "task_type": task.get("task_type"),
                    "title": str(task.get("title") or "")[:300],
                    "priority": task.get("priority"),
                    "validation_type": validation_request.get("validation_type"),
                    "target_name": validation_request.get("target_name"),
                    "candidate_name": validation_request.get("candidate_name"),
                    "objective": str(task.get("objective") or "")[:500],
                    "rationale": str(task.get("rationale") or "")[:500],
                    "tool_hint": task.get("tool_hint"),
                    "evidence_refs": task.get("evidence_refs", []),
                }
            )
        return {
            "plan_id": report.get("plan_id"),
            "brief_id": report.get("brief_id"),
            "evaluation_id": report.get("evaluation_id"),
            "agent_run_id": report.get("agent_run_id"),
            "topic": report.get("topic"),
            "source_key": report.get("source_key"),
            "status": report.get("status"),
            "readiness": report.get("readiness"),
            "hypothesis_count": dg.MetadataValue.int(len(report.get("hypothesis_drafts", []))),
            "task_count": dg.MetadataValue.int(len(report.get("tasks", []))),
            "error_count": dg.MetadataValue.int(len(report.get("errors", []))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "evidence": dg.MetadataValue.json(report.get("evidence", {})),
            "tasks": _metadata_table(task_rows, _VALIDATION_PLAN_TASK_TABLE_COLUMNS),
        }

    def _validation_plan_task_summary(record: Mapping[str, Any]) -> tuple[str, str]:
        tasks = record.get("tasks") or (record.get("result_payload") or {}).get("tasks") or []
        titles: list[str] = []
        validation_types: list[str] = []
        for task in tasks[:5]:
            title = str(task.get("title") or "").strip()
            if title:
                titles.append(title[:120])
            validation_request = task.get("validation_request") or {}
            validation_type = str(validation_request.get("validation_type") or "").strip()
            if validation_type:
                validation_types.append(validation_type)
        return "; ".join(titles), ", ".join(dict.fromkeys(validation_types))

    def _validation_plan_library_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = report.get("plans", [])
        return {
            "plan_count": dg.MetadataValue.int(int(report.get("plan_count", 0))),
            "brief_id": report.get("brief_id"),
            "evaluation_id": report.get("evaluation_id"),
            "status": report.get("status"),
            "readiness": report.get("readiness"),
            "plans": _metadata_table(rows, _VALIDATION_PLAN_LIBRARY_TABLE_COLUMNS),
        }

    def _validation_request_queue_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = [_validation_request_queue_row(item) for item in report.get("queue_items", [])]
        return {
            "queue_item_count": dg.MetadataValue.int(
                int(report.get("queue_item_count", report.get("queued_count", 0)))
            ),
            "candidate_task_count": dg.MetadataValue.int(int(report.get("candidate_task_count", 0))),
            "queued_count": dg.MetadataValue.int(int(report.get("queued_count", 0))),
            "existing_count": dg.MetadataValue.int(int(report.get("existing_count", 0))),
            "skipped_count": dg.MetadataValue.int(int(report.get("skipped_count", 0))),
            "dry_run": bool(report.get("dry_run", False)),
            "plan_id": report.get("plan_id"),
            "status": report.get("status"),
            "statuses": dg.MetadataValue.json(report.get("statuses", [])),
            "source_key": report.get("source_key"),
            "task_type": report.get("task_type"),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "skipped": dg.MetadataValue.json(report.get("skipped", [])),
            "queue_items": _metadata_table(rows, _VALIDATION_REQUEST_QUEUE_TABLE_COLUMNS),
        }

    def _validation_request_queue_row(item: Mapping[str, Any]) -> dict[str, Any]:
        validation_request = item.get("validation_request") or {}
        return {
            "queue_item_id": item.get("queue_item_id"),
            "status": item.get("status"),
            "priority": item.get("priority"),
            "plan_id": item.get("plan_id"),
            "task_id": item.get("task_id"),
            "task_type": item.get("task_type"),
            "title": str(item.get("title") or "")[:300],
            "validation_type": validation_request.get("validation_type"),
            "target_name": validation_request.get("target_name"),
            "candidate_name": validation_request.get("candidate_name"),
            "source_key": item.get("source_key"),
            "quality_gate_count": len(item.get("quality_gates") or []),
            "dispatch_blocker_count": len(item.get("dispatch_blockers") or []),
            "attempts": item.get("attempts"),
            "last_run_id": item.get("last_run_id"),
            "last_error": str(item.get("last_error") or "")[:300],
            "created_at": item.get("created_at"),
        }

    def _compute_job_row(item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "compute_job_id": item.get("compute_job_id"),
            "queue_item_id": item.get("queue_item_id"),
            "status": item.get("status"),
            "runner_kind": item.get("runner_kind"),
            "compute_profile": item.get("compute_profile"),
            "validation_type": item.get("validation_type"),
            "title": str(item.get("title") or "")[:300],
            "runpod_job_id": item.get("runpod_job_id"),
            "dagster_run_id": item.get("dagster_run_id"),
            "last_error": str(item.get("last_error") or "")[:300],
            "updated_at": item.get("updated_at"),
        }

    def _compute_job_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = [_compute_job_row(item) for item in report.get("jobs", [])]
        created_job = report.get("created_job") or {}
        return {
            "job_count": dg.MetadataValue.int(int(report.get("job_count", 0))),
            "created_count": dg.MetadataValue.int(int(report.get("created_count", 0))),
            "submitted_count": dg.MetadataValue.int(int(report.get("submitted_count", 0))),
            "blocked_count": dg.MetadataValue.int(int(report.get("blocked_count", 0))),
            "created_compute_job_id": created_job.get("compute_job_id"),
            "created_trace_id": created_job.get("trace_id"),
            "created_run_manifest_id": (created_job.get("metadata") or {}).get("run_manifest_id"),
            "created_compute_job_status": created_job.get("status"),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "jobs": _metadata_table(rows, _COMPUTE_JOB_TABLE_COLUMNS),
        }

    def _md_smoke_compute_job_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        compute_job = report.get("compute_job") if isinstance(report.get("compute_job"), Mapping) else {}
        return {
            "queue_item_id": report.get("queue_item_id"),
            "queue_item_status": report.get("queue_item_status"),
            "compute_job_id": report.get("compute_job_id"),
            "compute_job_status": report.get("compute_job_status"),
            "pdb_id": report.get("pdb_id"),
            "compound_name": report.get("compound_name"),
            "target_name": report.get("target_name"),
            "simulation_steps": dg.MetadataValue.int(int(report.get("simulation_steps", 0))),
            "protein_pdb_line_count": dg.MetadataValue.int(int(report.get("protein_pdb_line_count", 0))),
            "protein_pdb_sha256": report.get("protein_pdb_sha256"),
            "api_sources": dg.MetadataValue.json(report.get("api_sources", {})),
            "compute_job": dg.MetadataValue.json(compute_job),
        }

    def _md_expert_review_packet_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        input_packet = report.get("input_packet") if isinstance(report.get("input_packet"), Mapping) else {}
        return {
            "packet_id": report.get("packet_id"),
            "packet_hash": report.get("packet_hash"),
            "status": report.get("status"),
            "compute_job_id": report.get("compute_job_id"),
            "queue_item_id": report.get("queue_item_id"),
            "endpoint_id": report.get("endpoint_id"),
            "target_name": input_packet.get("target_name"),
            "compound_name": input_packet.get("compound_name"),
            "simulation_steps": dg.MetadataValue.int(int(input_packet.get("simulation_steps", 0))),
            "worker_error_count": dg.MetadataValue.int(len(report.get("worker_error_history", []))),
            "review_document": dg.MetadataValue.md(str(report.get("review_document") or "")),
        }

    def _md_expert_agent_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        approval = report.get("approval_record") if isinstance(report.get("approval_record"), Mapping) else {}
        return {
            "agent_run_id": report.get("agent_run_id"),
            "packet_id": report.get("packet_id"),
            "packet_hash": report.get("packet_hash"),
            "decision": report.get("decision"),
            "confidence": float(report.get("confidence", 0.0)),
            "model_profile": report.get("model_profile"),
            "approval_id": approval.get("approval_id"),
            "reviewer_type": approval.get("reviewer_type"),
            "required_change_count": dg.MetadataValue.int(len(report.get("required_changes", []))),
            "risk_flag_count": dg.MetadataValue.int(len(report.get("risk_flags", []))),
            "summary": dg.MetadataValue.md(str(report.get("summary") or "")),
            "required_changes": dg.MetadataValue.json(report.get("required_changes", [])),
            "risk_flags": dg.MetadataValue.json(report.get("risk_flags", [])),
        }

    def _validation_autopilot_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        selected_rows = report.get("selected", [])
        dispatched_rows = report.get("dispatched", [])
        skipped_rows = report.get("skipped", [])
        return {
            "agent_run_id": report.get("agent_run_id"),
            "dry_run": bool(report.get("dry_run", False)),
            "enabled": bool(report.get("enabled", False)),
            "force": bool(report.get("force", False)),
            "model_profile": report.get("model_profile"),
            "scanned_count": dg.MetadataValue.int(int(report.get("scanned_count", 0))),
            "eligible_count": dg.MetadataValue.int(int(report.get("eligible_count", 0))),
            "selected_count": dg.MetadataValue.int(int(report.get("selected_count", 0))),
            "dispatched_count": dg.MetadataValue.int(int(report.get("dispatched_count", 0))),
            "skipped_count": dg.MetadataValue.int(int(report.get("skipped_count", 0))),
            "estimated_cost_usd": dg.MetadataValue.float(float(report.get("estimated_cost_usd", 0.0))),
            "actual_cost_usd": dg.MetadataValue.float(float(report.get("actual_cost_usd", 0.0))),
            "hourly_spend_usd": dg.MetadataValue.float(float(report.get("hourly_spend_usd", 0.0))),
            "daily_spend_usd": dg.MetadataValue.float(float(report.get("daily_spend_usd", 0.0))),
            "blockers": dg.MetadataValue.json(report.get("blockers", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "selected": _metadata_table(selected_rows, _VALIDATION_AUTOPILOT_TABLE_COLUMNS),
            "dispatched": _metadata_table(dispatched_rows, _VALIDATION_AUTOPILOT_TABLE_COLUMNS),
            "skipped": _metadata_table(skipped_rows[:25], _VALIDATION_AUTOPILOT_TABLE_COLUMNS),
        }

    def _research_brief_queue_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = report.get("queue_items", [])
        return {
            "queue_item_count": dg.MetadataValue.int(int(report.get("queue_item_count", 0))),
            "status": report.get("status"),
            "statuses": dg.MetadataValue.json(report.get("statuses", [])),
            "source_key": report.get("source_key"),
            "topic_query": report.get("topic_query"),
            "queue_items": _metadata_table(rows, _RESEARCH_BRIEF_QUEUE_TABLE_COLUMNS),
        }

    def _research_brief_queue_batch_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        queue_rows = [
            {
                "queue_item_id": item.get("queue_item_id"),
                "status": item.get("status"),
                "priority": item.get("priority"),
                "topic": str(item.get("topic") or "")[:300],
                "source_key": item.get("source_key"),
                "brief_style": item.get("brief_style"),
                "model_profile": item.get("model_profile"),
                "review_mode": item.get("review_mode"),
                "attempts": item.get("attempts"),
                "last_brief_id": item.get("last_brief_id"),
                "last_error": str(item.get("last_error") or "")[:300],
                "created_at": item.get("created_at"),
            }
            for item in report.get("queue_items", [])
        ]
        return {
            "mode": report.get("mode"),
            "queued_count": dg.MetadataValue.int(int(report.get("queued_count", 0))),
            "lead_count": dg.MetadataValue.int(int(report.get("lead_count", 0))),
            "research_followup_count": dg.MetadataValue.int(int(report.get("research_followup_count", 0))),
            "source_health_count": dg.MetadataValue.int(int(report.get("source_health_count", 0))),
            "skipped_count": dg.MetadataValue.int(int(report.get("skipped_count", 0))),
            "skipped": dg.MetadataValue.json(report.get("skipped", [])),
            "error_count": dg.MetadataValue.int(len(report.get("errors", []))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "queue_items": _metadata_table(queue_rows, _RESEARCH_BRIEF_QUEUE_TABLE_COLUMNS),
        }

    def _research_hunt_synthesis_queue_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        queue_rows = [
            {
                "queue_item_id": item.get("queue_item_id"),
                "status": item.get("status"),
                "priority": item.get("priority"),
                "topic": str(item.get("topic") or "")[:300],
                "source_key": item.get("source_key"),
                "brief_style": item.get("brief_style"),
                "model_profile": item.get("model_profile"),
                "review_mode": item.get("review_mode"),
                "attempts": item.get("attempts"),
                "last_brief_id": item.get("last_brief_id"),
                "last_error": str(item.get("last_error") or "")[:300],
                "created_at": item.get("created_at"),
            }
            for item in report.get("queue_items", [])
        ]
        return {
            "dry_run": bool(report.get("dry_run", True)),
            "candidate_count": dg.MetadataValue.int(int(report.get("candidate_count", 0))),
            "queued_count": dg.MetadataValue.int(int(report.get("queued_count", 0))),
            "preexisting_count": dg.MetadataValue.int(int(report.get("preexisting_count", 0))),
            "updated_lead_count": dg.MetadataValue.int(int(report.get("updated_lead_count", 0))),
            "handoff_document_count": dg.MetadataValue.int(int(report.get("handoff_document_count", 0))),
            "handoff_artifact_count": dg.MetadataValue.int(int(report.get("handoff_artifact_count", 0))),
            "skipped_count": dg.MetadataValue.int(int(report.get("skipped_count", 0))),
            "skipped": dg.MetadataValue.json(report.get("skipped", [])[:50]),
            "error_count": dg.MetadataValue.int(len(report.get("errors", []))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "queue_items": _metadata_table(queue_rows, _RESEARCH_BRIEF_QUEUE_TABLE_COLUMNS),
        }

    def _research_hunt_synthesis_doc_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        doc_rows = [
            {
                "lead_id": doc.get("lead_id"),
                "title": str(doc.get("title") or "")[:300],
                "control_status": doc.get("control_status"),
                "recommended_action": doc.get("recommended_action"),
                "artifact_id": doc.get("artifact_id"),
                "claim_count": doc.get("claim_count"),
                "chunk_count": doc.get("chunk_count"),
                "research_object_count": doc.get("research_object_count"),
                "technical_footnote_count": doc.get("technical_footnote_count"),
                "created_at": doc.get("created_at"),
            }
            for doc in report.get("documents", [])
        ]
        preview = ""
        documents = report.get("documents", [])
        if documents:
            preview = str(documents[0].get("markdown") or "")[:8000]
        return {
            "dry_run": bool(report.get("dry_run", True)),
            "candidate_count": dg.MetadataValue.int(int(report.get("candidate_count", 0))),
            "document_count": dg.MetadataValue.int(int(report.get("document_count", 0))),
            "artifact_count": dg.MetadataValue.int(int(report.get("artifact_count", 0))),
            "updated_lead_count": dg.MetadataValue.int(int(report.get("updated_lead_count", 0))),
            "skipped_count": dg.MetadataValue.int(int(report.get("skipped_count", 0))),
            "error_count": dg.MetadataValue.int(len(report.get("errors", []))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "documents": _metadata_table(doc_rows, _RESEARCH_HUNT_SYNTHESIS_DOC_TABLE_COLUMNS),
            "preview": dg.MetadataValue.md(preview or "No synthesis handoff document generated."),
        }

    def _research_brief_queue_runner_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        if "runs" in report:
            rows = []
            for run in report.get("runs", []):
                brief = run.get("brief") or {}
                queue_item = run.get("queue_item") or {}
                rows.append(
                    {
                        "queue_item_id": queue_item.get("queue_item_id"),
                        "status": queue_item.get("status"),
                        "topic": str(queue_item.get("topic") or brief.get("topic") or "")[:300],
                        "source_key": queue_item.get("source_key") or brief.get("source_key"),
                        "brief_id": brief.get("brief_id"),
                        "agent_run_id": brief.get("agent_run_id"),
                        "citation_count": len(brief.get("citations", [])),
                        "error_count": len(run.get("errors", [])),
                        "last_error": str(queue_item.get("last_error") or "")[:300],
                    }
                )
            return {
                "ran_count": dg.MetadataValue.int(int(report.get("ran_count", 0))),
                "completed_count": dg.MetadataValue.int(int(report.get("completed_count", 0))),
                "failed_count": dg.MetadataValue.int(int(report.get("failed_count", 0))),
                "requested_max_runs": dg.MetadataValue.int(int(report.get("requested_max_runs", 0))),
                "queue_item_ids": dg.MetadataValue.json(report.get("queue_item_ids", [])),
                "error_count": dg.MetadataValue.int(len(report.get("errors", []))),
                "errors": dg.MetadataValue.json(report.get("errors", [])),
                "brief_runs": _metadata_table(rows, _VALIDATION_GAP_COMPLETION_BRIEF_TABLE_COLUMNS),
            }

        brief = report.get("brief") or {}
        queue_item = report.get("queue_item") or {}
        return {
            "ran": bool(report.get("ran", False)),
            "queue_item_id": queue_item.get("queue_item_id"),
            "queue_status": queue_item.get("status"),
            "brief_id": brief.get("brief_id"),
            "agent_run_id": brief.get("agent_run_id"),
            "topic": brief.get("topic") or queue_item.get("topic"),
            "citation_count": dg.MetadataValue.int(len(brief.get("citations", []))),
            "finding_count": dg.MetadataValue.int(
                sum(len(item.get("findings", [])) for item in brief.get("perspective_reports", []))
            ),
            "error_count": dg.MetadataValue.int(len(report.get("errors", []))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "brief_preview": dg.MetadataValue.md(str(brief.get("final_brief") or "")[:4000]),
        }

    def _research_brief_queue_maintenance_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        queue_rows = [
            {
                "queue_item_id": item.get("queue_item_id"),
                "status": item.get("status"),
                "priority": item.get("priority"),
                "topic": str(item.get("topic") or "")[:300],
                "source_key": item.get("source_key"),
                "brief_style": item.get("brief_style"),
                "model_profile": item.get("model_profile"),
                "review_mode": item.get("review_mode"),
                "attempts": item.get("attempts"),
                "last_brief_id": item.get("last_brief_id"),
                "last_error": str(item.get("last_error") or "")[:300],
                "created_at": item.get("created_at"),
            }
            for item in report.get("queue_items", [])
        ]
        return {
            "action": report.get("action"),
            "dry_run": bool(report.get("dry_run", True)),
            "candidate_count": dg.MetadataValue.int(int(report.get("candidate_count", 0))),
            "archived_count": dg.MetadataValue.int(int(report.get("archived_count", 0))),
            "skipped_count": dg.MetadataValue.int(int(report.get("skipped_count", 0))),
            "error_count": dg.MetadataValue.int(len(report.get("errors", []))),
            "queue_items": _metadata_table(queue_rows, _RESEARCH_BRIEF_QUEUE_TABLE_COLUMNS),
            "skipped": dg.MetadataValue.json(report.get("skipped", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
        }

    def _research_brief_playground_pack_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        evidence = report.get("evidence", {})
        prompt_rows = [
            {
                "perspective": prompt.get("perspective"),
                "agent_name": prompt.get("agent_name"),
                "recommended_model": prompt.get("recommended_model"),
                "user_prompt_chars": len(str(prompt.get("user_prompt") or "")),
                "citation_count": len((prompt.get("prompt_payload") or {}).get("citations", [])),
            }
            for prompt in report.get("prompts", [])
        ]
        return {
            "topic": report.get("topic"),
            "brief_style": report.get("brief_style"),
            "model_profile": report.get("model_profile"),
            "prompt_count": dg.MetadataValue.int(len(report.get("prompts", []))),
            "citation_count": dg.MetadataValue.int(len(report.get("citations", []))),
            "retrieval_strategy": evidence.get("retrieval_strategy"),
            "search_query_count": dg.MetadataValue.int(int(evidence.get("search_query_count", 0))),
            "research_lead_count": dg.MetadataValue.int(int(evidence.get("research_lead_count", 0))),
            "error_count": len(report.get("errors", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "evidence": dg.MetadataValue.json(evidence),
            "prompts": _metadata_table(
                prompt_rows,
                _RESEARCH_BRIEF_PLAYGROUND_PROMPT_TABLE_COLUMNS,
            ),
        }

    def _research_leads_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        leads = report.get("items", [])
        rows = [
            {
                "lead_id": lead.get("lead_id"),
                "lead_type": lead.get("lead_type"),
                "status": lead.get("status"),
                "priority": lead.get("priority"),
                "source_key": lead.get("source_key"),
                "title": str(lead.get("title") or "")[:300],
                "url": lead.get("url"),
                "topic_tags": lead.get("topic_tags", []),
                "suggested_sources": lead.get("suggested_sources", []),
                "reason": str(lead.get("reason") or "")[:500],
            }
            for lead in leads[:100]
        ]
        return {
            "agent_runs_seen": dg.MetadataValue.int(int(report.get("agent_runs_seen", 0))),
            "leads_created": dg.MetadataValue.int(int(report.get("leads_created", 0))),
            "skipped_existing": dg.MetadataValue.int(int(report.get("skipped_existing", 0))),
            "lead_count": dg.MetadataValue.int(len(leads)),
            "error_count": len(report.get("errors", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "research_leads": _metadata_table(rows, _RESEARCH_LEAD_TABLE_COLUMNS),
        }

    def _evidence_gap_resolver_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        leads = report.get("research_leads", [])
        rows = [
            {
                "lead_id": lead.get("lead_id"),
                "lead_type": lead.get("lead_type"),
                "status": lead.get("status"),
                "priority": lead.get("priority"),
                "source_key": lead.get("source_key"),
                "title": str(lead.get("title") or "")[:300],
                "url": lead.get("url"),
                "topic_tags": lead.get("topic_tags", []),
                "suggested_sources": lead.get("suggested_sources", []),
                "reason": str(lead.get("reason") or "")[:500],
            }
            for lead in leads[:100]
        ]
        return {
            "queue_items_seen": dg.MetadataValue.int(int(report.get("queue_items_seen", 0))),
            "gap_count": dg.MetadataValue.int(int(report.get("gap_count", 0))),
            "leads_created": dg.MetadataValue.int(int(report.get("leads_created", 0))),
            "existing_leads": dg.MetadataValue.int(int(report.get("existing_leads", 0))),
            "brief_queue_count": dg.MetadataValue.int(int(report.get("brief_queue_count", 0))),
            "skipped_count": dg.MetadataValue.int(int(report.get("skipped_count", 0))),
            "dry_run": bool(report.get("dry_run", True)),
            "error_count": len(report.get("errors", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "research_leads": _metadata_table(rows, _RESEARCH_LEAD_TABLE_COLUMNS),
            "skipped": dg.MetadataValue.json(report.get("skipped", [])[:100]),
        }

    def _research_followup_resolver_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = [
            {
                "lead_id": item.get("lead_id"),
                "status_before": item.get("status_before"),
                "status_after": item.get("status_after"),
                "actions": item.get("actions", []),
                "source_followup_ids": item.get("source_followup_ids", []),
                "durable_source_keys": item.get("durable_source_keys", []),
                "evidence_refs": item.get("evidence_refs", [])[:10],
                "evidence_inspection_count": (item.get("metadata", {}).get("evidence_inspection", {}) or {}).get(
                    "inspected_count", 0
                ),
                "manual_research_required": item.get("manual_research_required"),
                "promoted": item.get("promoted"),
                "errors": item.get("errors", []),
            }
            for item in report.get("lead_results", [])
        ]
        return {
            "agent_run_id": report.get("agent_run_id"),
            "dry_run": bool(report.get("dry_run", False)),
            "force_live_search": bool(report.get("force_live_search", False)),
            "blocked": bool(report.get("blocked", False)),
            "leads_seen": dg.MetadataValue.int(int(report.get("leads_seen", 0))),
            "skipped_leads": dg.MetadataValue.int(int(report.get("skipped_leads", 0))),
            "unresolved_lead_ids": dg.MetadataValue.json(report.get("unresolved_lead_ids", [])),
            "skip_reasons": dg.MetadataValue.json(report.get("skip_reasons", [])[:100]),
            "source_followups_queued": dg.MetadataValue.int(int(report.get("source_followups_queued", 0))),
            "source_followups_ingested": dg.MetadataValue.int(int(report.get("source_followups_ingested", 0))),
            "durable_source_searches": dg.MetadataValue.int(int(report.get("durable_source_searches", 0))),
            "evidence_inspections": dg.MetadataValue.int(int(report.get("evidence_inspections", 0))),
            "promoted_leads": dg.MetadataValue.int(int(report.get("promoted_leads", 0))),
            "manual_research_required": dg.MetadataValue.int(int(report.get("manual_research_required", 0))),
            "kept_in_followup": dg.MetadataValue.int(int(report.get("kept_in_followup", 0))),
            "failed_leads": dg.MetadataValue.int(int(report.get("failed_leads", 0))),
            "error_count": dg.MetadataValue.int(len(report.get("errors", []))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "lead_results": _metadata_table(rows, _RESEARCH_FOLLOWUP_RESOLVER_TABLE_COLUMNS),
        }

    def _validation_gap_source_pack_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = [
            {
                "source_key": query.get("source_key"),
                "lane": query.get("lane"),
                "query_name": query.get("query_name"),
                "query_text": str(query.get("query_text") or "")[:500],
                "priority": query.get("priority"),
                "active": query.get("active"),
                "lead_ids": query.get("lead_ids", []),
                "queue_item_ids": query.get("queue_item_ids", []),
                "required_terms": query.get("required_terms", []),
            }
            for query in report.get("queries", [])
        ]
        return {
            "agent_run_id": report.get("agent_run_id"),
            "source_pack_id": report.get("source_pack_id"),
            "lead_count": dg.MetadataValue.int(int(report.get("lead_count", 0))),
            "queue_item_count": dg.MetadataValue.int(int(report.get("queue_item_count", 0))),
            "query_count": dg.MetadataValue.int(int(report.get("query_count", 0))),
            "persisted_query_count": dg.MetadataValue.int(int(report.get("persisted_query_count", 0))),
            "skipped_count": dg.MetadataValue.int(int(report.get("skipped_count", 0))),
            "dry_run": bool(report.get("dry_run", True)),
            "persist_queries": bool(report.get("persist_queries", False)),
            "error_count": dg.MetadataValue.int(len(report.get("errors", []))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "skipped": dg.MetadataValue.json(report.get("skipped", [])[:100]),
            "queries": _metadata_table(rows, _VALIDATION_GAP_SOURCE_PACK_TABLE_COLUMNS),
        }

    def _pubmed_identifier_repair_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = [
            {
                "pmid": item.get("pmid"),
                "status": item.get("status"),
                "old_dedupe_key": item.get("old_dedupe_key"),
                "new_dedupe_key": item.get("new_dedupe_key"),
                "old_doi": (item.get("old_identifiers") or {}).get("doi"),
                "new_doi": (item.get("new_identifiers") or {}).get("doi"),
                "old_pmcid": (item.get("old_identifiers") or {}).get("pmcid"),
                "new_pmcid": (item.get("new_identifiers") or {}).get("pmcid"),
                "error": item.get("error"),
            }
            for item in report.get("items", [])[:50]
            if isinstance(item, Mapping)
        ]
        return {
            "dry_run": bool(report.get("dry_run", True)),
            "scanned_objects": dg.MetadataValue.int(int(report.get("scanned_objects", 0))),
            "fetched_pmids": dg.MetadataValue.int(int(report.get("fetched_pmids", 0))),
            "clean": dg.MetadataValue.int(int(report.get("clean", 0))),
            "repaired": dg.MetadataValue.int(int(report.get("repaired", 0))),
            "would_repair": dg.MetadataValue.int(int(report.get("would_repair", 0))),
            "skipped": dg.MetadataValue.int(int(report.get("skipped", 0))),
            "conflicts": dg.MetadataValue.int(int(report.get("conflicts", 0))),
            "failed": dg.MetadataValue.int(int(report.get("failed", 0))),
            "error_count": dg.MetadataValue.int(len(report.get("errors", []))),
            "errors": dg.MetadataValue.json(report.get("errors", [])[:20]),
            "items": _metadata_table(rows, _PUBMED_IDENTIFIER_REPAIR_TABLE_COLUMNS),
        }

    def _validation_gap_source_ingest_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = [
            {
                "source_key": item.get("source_key"),
                "query_name": item.get("query_name"),
                "status": item.get("status"),
                "raw_records": item.get("raw_records", 0),
                "research_objects": item.get("research_objects", 0),
                "document_chunks": item.get("document_chunks", 0),
                "full_text_research_objects": item.get("full_text_research_objects", 0),
                "errors": item.get("errors", []),
            }
            for item in report.get("results", [])
        ]
        dry_run_rows = [
            {
                "source_key": query.get("source_key"),
                "query_name": query.get("query_name"),
                "status": "dry_run",
                "raw_records": 0,
                "research_objects": 0,
                "document_chunks": 0,
                "full_text_research_objects": 0,
                "errors": [],
            }
            for query in report.get("source_queries", [])
        ]
        return {
            "dry_run": bool(report.get("dry_run", True)),
            "source_keys": dg.MetadataValue.json(report.get("source_keys", [])),
            "query_count": dg.MetadataValue.int(int(report.get("query_count", 0))),
            "attempted_query_count": dg.MetadataValue.int(int(report.get("attempted_query_count", 0))),
            "completed_query_count": dg.MetadataValue.int(int(report.get("completed_query_count", 0))),
            "failed_query_count": dg.MetadataValue.int(int(report.get("failed_query_count", 0))),
            "raw_records": dg.MetadataValue.int(int(report.get("raw_records", 0))),
            "research_objects": dg.MetadataValue.int(int(report.get("research_objects", 0))),
            "document_chunks": dg.MetadataValue.int(int(report.get("document_chunks", 0))),
            "error_count": dg.MetadataValue.int(len(report.get("errors", []))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "results": _metadata_table(rows or dry_run_rows, _VALIDATION_GAP_SOURCE_INGEST_TABLE_COLUMNS),
        }

    def _validation_gap_completion_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        summary = report.get("summary", {})
        brief_rows = []
        for run in report.get("brief_runs", []):
            queue_item = run.get("queue_item") or {}
            brief = run.get("brief") or {}
            brief_rows.append(
                {
                    "queue_item_id": queue_item.get("queue_item_id"),
                    "status": queue_item.get("status"),
                    "topic": str(queue_item.get("topic") or brief.get("topic") or "")[:300],
                    "source_key": queue_item.get("source_key") or brief.get("source_key"),
                    "brief_id": brief.get("brief_id"),
                    "agent_run_id": brief.get("agent_run_id"),
                    "citation_count": len(brief.get("citations", [])),
                    "error_count": len(run.get("errors", [])),
                    "last_error": str(queue_item.get("last_error") or "")[:300],
                }
            )
        return {
            "dry_run": bool(summary.get("dry_run", True)),
            "queue_item_count": dg.MetadataValue.int(int(summary.get("queue_item_count", 0))),
            "gap_count": dg.MetadataValue.int(int(summary.get("gap_count", 0))),
            "leads_created": dg.MetadataValue.int(int(summary.get("leads_created", 0))),
            "existing_leads": dg.MetadataValue.int(int(summary.get("existing_leads", 0))),
            "brief_queue_count": dg.MetadataValue.int(int(summary.get("brief_queue_count", 0))),
            "source_query_count": dg.MetadataValue.int(int(summary.get("source_query_count", 0))),
            "persisted_query_count": dg.MetadataValue.int(int(summary.get("persisted_query_count", 0))),
            "ingested_query_count": dg.MetadataValue.int(int(summary.get("ingested_query_count", 0))),
            "raw_records": dg.MetadataValue.int(int(summary.get("raw_records", 0))),
            "research_objects": dg.MetadataValue.int(int(summary.get("research_objects", 0))),
            "document_chunks": dg.MetadataValue.int(int(summary.get("document_chunks", 0))),
            "brief_runs_requested": dg.MetadataValue.int(int(summary.get("brief_runs_requested", 0))),
            "brief_runs_attempted": dg.MetadataValue.int(int(summary.get("brief_runs_attempted", 0))),
            "brief_runs_completed": dg.MetadataValue.int(int(summary.get("brief_runs_completed", 0))),
            "brief_runs_failed": dg.MetadataValue.int(int(summary.get("brief_runs_failed", 0))),
            "error_count": dg.MetadataValue.int(len(report.get("errors", []))),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "brief_runs": _metadata_table(brief_rows, _VALIDATION_GAP_COMPLETION_BRIEF_TABLE_COLUMNS),
        }

    def _x_topic_monitor_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        errors = report.get("errors", [])
        agent_review = report.get("agent_review") or {}
        return {
            "source_key": report.get("source_key"),
            "provider": report.get("provider"),
            "query_count": len(report.get("queries", [])),
            "raw_tweet_count": dg.MetadataValue.int(int(report.get("raw_tweet_count", 0))),
            "candidate_count": dg.MetadataValue.int(int(report.get("candidate_count", 0))),
            "agent_run_id": agent_review.get("agent_run_id"),
            "review_action_count": len(agent_review.get("actions", [])),
            "ingestion_candidate_count": dg.MetadataValue.int(
                int(agent_review.get("ingestion_candidate_count", 0))
            ),
            "needs_human_review_count": dg.MetadataValue.int(
                int(agent_review.get("needs_human_review_count", 0))
            ),
            "rejected_count": dg.MetadataValue.int(int(agent_review.get("rejected_count", 0))),
            "manual_review_required": bool(report.get("manual_review_required", True)),
            "error_count": len(errors),
            "errors": dg.MetadataValue.json(errors),
            "candidates": _metadata_table(
                report.get("candidates", []),
                _X_TOPIC_CANDIDATE_TABLE_COLUMNS,
            ),
            "ingestion_recommendations": _metadata_table(
                _x_topic_review_action_rows(agent_review.get("actions", [])),
                _X_TOPIC_REVIEW_TABLE_COLUMNS,
            ),
        }

    def _x_topic_review_action_rows(actions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for action in actions:
            links = action.get("ingestible_links", [])
            rows.append(
                {
                    "post_id": action.get("source_record_id"),
                    "query_name": action.get("query_name"),
                    "action": action.get("action"),
                    "severity": action.get("severity"),
                    "recommended_sources": sorted(
                        {
                            link.get("recommended_source_key")
                            for link in links
                            if isinstance(link, Mapping) and link.get("recommended_source_key")
                        }
                    ),
                    "identifiers": [
                        {
                            "type": link.get("identifier_type"),
                            "value": link.get("identifier"),
                        }
                        for link in links
                        if isinstance(link, Mapping) and link.get("identifier")
                    ],
                    "links": [
                        link.get("url")
                        for link in links
                        if isinstance(link, Mapping) and link.get("url")
                    ],
                    "reason": action.get("reason"),
                }
            )
        return rows

    def _x_linked_article_followup_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        errors = report.get("errors", [])
        primary_source_links = report.get("primary_source_links", [])
        return {
            "source_key": report.get("source_key"),
            "candidate_url_count": len(report.get("candidate_urls", [])),
            "candidate_urls": dg.MetadataValue.json(report.get("candidate_urls", [])),
            "agent_run_ids": dg.MetadataValue.json(report.get("agent_run_ids", [])),
            "fetched_pages": dg.MetadataValue.int(int(report.get("fetched_pages", 0))),
            "skipped_pages": dg.MetadataValue.int(int(report.get("skipped_pages", 0))),
            "artifact_count": len(report.get("artifact_ids", [])),
            "review_record_count": len(report.get("review_ids", [])),
            "parsed_records": dg.MetadataValue.int(int(report.get("parsed_records", 0))),
            "primary_source_link_count": len(primary_source_links),
            "requires_fetch_approval": bool(report.get("requires_fetch_approval", False)),
            "error_count": len(errors),
            "errors": dg.MetadataValue.json(errors),
            "primary_source_links": _metadata_table(
                primary_source_links,
                _X_LINKED_ARTICLE_SOURCE_LINK_COLUMNS,
            ),
        }

    def _x_linked_article_review_action_rows(actions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for action in actions:
            links = action.get("followup_links", [])
            rows.append(
                {
                    "review_id": action.get("review_id"),
                    "source_record_id": action.get("source_record_id"),
                    "action": action.get("action"),
                    "severity": action.get("severity"),
                    "followup_sources": sorted(
                        {
                            link.get("recommended_source_key")
                            for link in links
                            if isinstance(link, Mapping) and link.get("recommended_source_key")
                        }
                    ),
                    "identifiers": [
                        {
                            "type": link.get("identifier_type"),
                            "value": link.get("identifier"),
                        }
                        for link in links
                        if isinstance(link, Mapping) and link.get("identifier")
                    ],
                    "reason": action.get("reason"),
                }
            )
        return rows

    def _x_linked_article_review_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "agent_run_id": report.get("agent_run_id"),
            "action_count": len(report.get("actions", [])),
            "queue_candidate_count": int(report.get("queue_candidate_count", 0)),
            "needs_human_review_count": int(report.get("needs_human_review_count", 0)),
            "rejected_count": int(report.get("rejected_count", 0)),
            "error_count": len(report.get("errors", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "evidence": dg.MetadataValue.json(report.get("evidence", {})),
            "recommendations": _metadata_table(
                _x_linked_article_review_action_rows(report.get("actions", [])),
                _X_LINKED_ARTICLE_REVIEW_TABLE_COLUMNS,
            ),
        }

    def _source_followup_queue_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "source_key": report.get("source_key"),
            "reviewed_records": int(report.get("reviewed_records", 0)),
            "queued": int(report.get("queued", 0)),
            "skipped_existing": int(report.get("skipped_existing", 0)),
            "skipped_uningestible": int(report.get("skipped_uningestible", 0)),
            "agent_runs_seen": int(report.get("agent_runs_seen", 0)),
            "agent_recommendations_seen": int(report.get("agent_recommendations_seen", 0)),
            "error_count": len(report.get("errors", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "queue_items": _metadata_table(
                report.get("items", []),
                _SOURCE_FOLLOWUP_QUEUE_TABLE_COLUMNS,
            ),
        }

    def _source_followup_ingest_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "lane_name": report.get("lane_name"),
            "source_keys": dg.MetadataValue.json(report.get("source_keys", [])),
            "statuses": dg.MetadataValue.json(report.get("statuses", [])),
            "queue_items_seen": int(report.get("queue_items_seen", 0)),
            "attempted": int(report.get("attempted", 0)),
            "ingested": int(report.get("ingested", 0)),
            "failed": int(report.get("failed", 0)),
            "skipped": int(report.get("skipped", 0)),
            "raw_records": int(report.get("raw_records", 0)),
            "research_objects": int(report.get("research_objects", 0)),
            "document_chunks": int(report.get("document_chunks", 0)),
            "error_count": len(report.get("errors", [])),
            "errors": dg.MetadataValue.json(report.get("errors", [])),
            "items": _metadata_table(
                report.get("items", []),
                _SOURCE_FOLLOWUP_INGEST_TABLE_COLUMNS,
            ),
        }

    _SOURCE_FOLLOWUP_INGEST_CONFIG_SCHEMA = {
        "source_keys": dg.Field([str], is_required=False, description="Target source keys to process."),
        "statuses": dg.Field([str], is_required=False, description="Queue statuses to process."),
        "limit": dg.Field(int, is_required=False, description="Maximum queue rows to process."),
        "approved_by": dg.Field(str, is_required=False, description="Operator approval identity."),
        "run_claim_extraction": dg.Field(
            bool,
            is_required=False,
            description="Run entity resolution, claim extraction, and curation after ingest.",
        ),
        "dry_run": dg.Field(bool, is_required=False, description="List queue rows without ingesting."),
    }
    _SOURCE_FOLLOWUP_LANE_CONFIG_SCHEMA = {
        key: value for key, value in _SOURCE_FOLLOWUP_INGEST_CONFIG_SCHEMA.items() if key != "source_keys"
    }

    def _run_source_followup_ingest_asset(
        context,
        research_repository: ResearchRepositoryResource,
        *,
        lane_name: str,
        default_source_keys: Sequence[str] | None = None,
        default_limit: int = 25,
    ) -> dg.MaterializeResult:
        from .service import HSAResearchService

        config = context.op_config or {}
        repository = research_repository.build_repository()
        if default_source_keys is None:
            source_keys = config.get("source_keys") or _parse_delimited_string_list(
                os.getenv("HSA_SOURCE_FOLLOWUP_SOURCE_KEYS")
            )
        else:
            source_keys = list(default_source_keys)
        statuses = config.get("statuses") or _parse_delimited_string_list(
            os.getenv("HSA_SOURCE_FOLLOWUP_STATUSES")
        ) or ["queued", "approved"]
        limit = int(_config_or_env(config, "limit", "HSA_SOURCE_FOLLOWUP_INGEST_LIMIT", default_limit))
        approved_by = _config_or_env(config, "approved_by", "HSA_SOURCE_FOLLOWUP_APPROVED_BY", None)
        run_claim_extraction = _config_or_env_bool(
            config,
            "run_claim_extraction",
            "HSA_SOURCE_FOLLOWUP_RUN_CLAIM_EXTRACTION",
            True,
        )
        dry_run = _config_or_env_bool(config, "dry_run", "HSA_SOURCE_FOLLOWUP_DRY_RUN", False)
        result = HSAResearchService(repository).ingest_source_followups(
            SourceFollowupIngestRequest(
                source_keys=source_keys,
                statuses=statuses,
                limit=limit,
                approved_by=approved_by,
                run_claim_extraction=run_claim_extraction,
                dry_run=dry_run,
                metadata={
                    "dagster_run_id": context.run_id,
                    "lane_name": lane_name,
                },
            )
        )
        report = result.model_dump(mode="json")
        report.update(
            {
                "lane_name": lane_name,
                "source_keys": source_keys,
                "statuses": statuses,
                "limit": limit,
                "approved_by": approved_by,
                "run_claim_extraction": run_claim_extraction,
                "dry_run": dry_run,
            }
        )
        return dg.MaterializeResult(value=report, metadata=_source_followup_ingest_metadata(report))

    def _run_literature_full_text_refresh(
        research_repository: ResearchRepositoryResource,
        *,
        source_keys: Sequence[str],
        source_limits: Mapping[str, int],
        extract_limit: int,
        curate_limit: int,
    ) -> dict:
        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        report = run_structured_sources_pipeline(
            repository,
            source_keys=source_keys,
            source_limits=source_limits,
            extract_limit=extract_limit,
            curate_limit=curate_limit,
        )
        return _annotate_full_text_report(report, mode="refresh")

    def _run_literature_full_text_ingestion(
        research_repository: ResearchRepositoryResource,
        *,
        source_keys: Sequence[str],
        source_limits: Mapping[str, int],
    ) -> dict:
        from .structured_orchestration import run_structured_sources_ingestion_pipeline

        repository = research_repository.build_repository()
        report = run_structured_sources_ingestion_pipeline(
            repository,
            source_keys=source_keys,
            source_limits=source_limits,
        )
        return _annotate_full_text_report(report, mode="ingestion_only")

    def _run_literature_corpus_source_date_partition(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        from .structured_orchestration import run_structured_sources_pipeline

        keys = context.multi_partition_key.keys_by_dimension
        source_key = keys["source"]
        partition_date = keys["date"]
        source_limits = {source_key: LITERATURE_CORPUS_SOURCE_LIMITS[source_key]}
        repository = research_repository.build_repository()
        report = run_structured_sources_pipeline(
            repository,
            source_keys=(source_key,),
            source_limits=source_limits,
            extract_limit=5000,
            curate_limit=5000,
            partition_date=partition_date,
        )
        report["mode"] = "source_date_partition"
        report["date_filter_status"] = "orchestration_metadata_only"
        report["date_filter_note"] = (
            "Literature corpus harvesters are source-filtered for this partition; "
            "the date partition is orchestration metadata until source-specific "
            "publication-date filters are added."
        )
        return dg.MaterializeResult(
            value=report,
            metadata={
                "source_key": source_key,
                "partition_date": partition_date,
                "date_filter_status": report["date_filter_status"],
                "date_filter_note": report["date_filter_note"],
                "source_limits": dg.MetadataValue.json(dict(source_limits)),
                "totals": dg.MetadataValue.json(report.get("totals", {})),
                "errors": dg.MetadataValue.json(report.get("errors", [])),
            },
        )

    def _run_literature_full_text_source_date_partition(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        from .structured_orchestration import run_structured_sources_pipeline

        keys = context.multi_partition_key.keys_by_dimension
        source_key = keys["source"]
        partition_date = keys["date"]
        source_limits = {source_key: LITERATURE_FULL_TEXT_SOURCE_LIMITS[source_key]}
        repository = research_repository.build_repository()
        report = run_structured_sources_pipeline(
            repository,
            source_keys=(source_key,),
            source_limits=source_limits,
            extract_limit=500,
            curate_limit=500,
            partition_date=partition_date,
        )
        report = _annotate_full_text_report(report, mode="source_date_partition")
        return dg.MaterializeResult(
            value=report,
            metadata={
                "source_key": source_key,
                "partition_date": partition_date,
                "source_limits": dg.MetadataValue.json(dict(source_limits)),
                "totals": dg.MetadataValue.json(report.get("totals", {})),
                "errors": dg.MetadataValue.json(report.get("errors", [])),
                "full_text_blocking_sources": dg.MetadataValue.json(
                    report.get("full_text_blocking_sources", [])
                ),
                "full_text_triage": _metadata_table(
                    report.get("full_text_triage", []),
                    _FULL_TEXT_TRIAGE_TABLE_COLUMNS,
                ),
            },
        )

    def _run_literature_full_text_source_date_report(
        research_repository: ResearchRepositoryResource,
        *,
        source_key: str,
        partition_date: str,
        source_limit: int,
        extract_limit: int,
        curate_limit: int,
    ) -> dict:
        from .structured_orchestration import run_structured_sources_pipeline

        if source_key not in LITERATURE_FULL_TEXT_SOURCE_KEYS:
            valid_sources = ", ".join(LITERATURE_FULL_TEXT_SOURCE_KEYS)
            raise ValueError(f"source_key must be one of {valid_sources}; got {source_key!r}")
        if source_limit < 1:
            raise ValueError(f"source_limit must be positive; got {source_limit!r}")
        if extract_limit < 1:
            raise ValueError(f"extract_limit must be positive; got {extract_limit!r}")
        if curate_limit < 1:
            raise ValueError(f"curate_limit must be positive; got {curate_limit!r}")

        repository = research_repository.build_repository()
        report = run_structured_sources_pipeline(
            repository,
            source_keys=(source_key,),
            source_limits={source_key: source_limit},
            extract_limit=extract_limit,
            curate_limit=curate_limit,
            partition_date=partition_date,
        )
        report = _annotate_full_text_report(report, mode="source_date_partition")
        report["partition_date"] = partition_date
        return report

    def _annotate_full_text_report(report: dict, *, mode: str) -> dict:
        report["mode"] = mode
        report["full_text_runtime_config"] = _full_text_runtime_config()
        report["full_text_triage"] = _full_text_triage_rows(report, mode=mode)
        report["full_text_blocking_sources"] = [
            row["source_key"]
            for row in report["full_text_triage"]
            if row.get("should_block_schedule")
        ]
        return report

    def _full_text_runtime_config() -> dict[str, str | None]:
        env_names = (
            "HSA_FULL_TEXT_REQUEST_TIMEOUT_SECONDS",
            "HSA_FULL_TEXT_REQUEST_ATTEMPTS",
            "HSA_FULL_TEXT_FETCH_TIME_BUDGET_SECONDS",
            "HSA_PMC_OA_MAX_CANDIDATE_RECORDS",
        )
        return {name: os.getenv(name) for name in env_names}

    def _parse_delimited_string_list(value: str | None) -> list[str]:
        if value is None:
            return []
        raw_value = value.strip()
        if not raw_value:
            return []
        if raw_value.startswith("["):
            try:
                parsed = json.loads(raw_value)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        normalized = raw_value.replace("\n", ",").replace("\r", ",")
        return [item.strip() for item in normalized.split(",") if item.strip()]

    def _config_or_env(
        config: Mapping[str, Any],
        key: str,
        env_name: str,
        default: Any,
    ) -> Any:
        value = config.get(key)
        if value is not None:
            return value
        env_value = os.getenv(env_name)
        if env_value is None or not env_value.strip():
            return default
        return env_value

    def _config_or_env_bool(
        config: Mapping[str, Any],
        key: str,
        env_name: str,
        default: bool,
    ) -> bool:
        value = _config_or_env(config, key, env_name, default)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes"}

    def _full_text_triage_rows(report: Mapping[str, Any], *, mode: str) -> list[dict[str, Any]]:
        rows = []
        for source_report in report.get("sources", []):
            full_text_qa = source_report.get("full_text_qa") or {}
            triage = full_text_qa.get("triage") or {}
            if not triage:
                continue
            rows.append(
                {
                    "source_key": source_report.get("source_key"),
                    "mode": mode,
                    "passes_full_text_bar": full_text_qa.get("passes_full_text_bar"),
                    "triage_action": triage.get("action"),
                    "triage_severity": triage.get("severity"),
                    "should_retry": triage.get("should_retry"),
                    "should_block_schedule": triage.get("should_block_schedule"),
                    "reasons": triage.get("reasons", []),
                    "recommended_next_actions": triage.get("recommended_next_actions", []),
                }
            )
        return rows

    def _full_text_check_result(
        report: Mapping[str, Any],
        *,
        source_limits: Mapping[str, int],
    ) -> dg.AssetCheckResult:
        source_reports = report.get("sources", [])
        failed_sources = [
            source_report["source_key"]
            for source_report in source_reports
            if (
                not _has_minimum_ingested_source_outputs(source_report)
                or not _has_required_full_text_outputs(source_report)
            )
        ]
        errors = report.get("errors", [])
        return dg.AssetCheckResult(
            passed=not failed_sources and not errors,
            metadata={
                "failed_sources": failed_sources,
                "errors": errors,
                "source_keys": report.get("source_keys", []),
                "source_limits": dict(source_limits),
                "full_text_blocking_sources": report.get("full_text_blocking_sources", []),
                "full_text_triage": _metadata_table(
                    report.get("full_text_triage", []),
                    _FULL_TEXT_TRIAGE_TABLE_COLUMNS,
                ),
                "totals": report.get("totals", {}),
            },
        )

    def _full_text_ingestion_check_result(
        report: Mapping[str, Any],
        *,
        source_limits: Mapping[str, int],
    ) -> dg.AssetCheckResult:
        source_reports = report.get("sources", [])
        failed_sources = [
            source_report["source_key"]
            for source_report in source_reports
            if (
                not _has_minimum_source_ingestion_outputs(source_report)
                or not _has_required_full_text_outputs(source_report)
            )
        ]
        errors = report.get("errors", [])
        return dg.AssetCheckResult(
            passed=not failed_sources and not errors,
            metadata={
                "mode": report.get("mode"),
                "failed_sources": failed_sources,
                "errors": errors,
                "source_keys": report.get("source_keys", []),
                "source_limits": dict(source_limits),
                "full_text_runtime_config": report.get("full_text_runtime_config", {}),
                "full_text_blocking_sources": report.get("full_text_blocking_sources", []),
                "full_text_triage": _metadata_table(
                    report.get("full_text_triage", []),
                    _FULL_TEXT_TRIAGE_TABLE_COLUMNS,
                ),
                "totals": report.get("totals", {}),
            },
        )

    def _full_text_partition_check_result(report: Mapping[str, Any]) -> dg.AssetCheckResult:
        source_reports = report.get("sources", [])
        failed_sources = []
        empty_sources = []
        for source_report in source_reports:
            full_text_qa = source_report.get("full_text_qa") or {}
            if full_text_qa.get("current_empty_passes"):
                empty_sources.append(source_report["source_key"])
                continue
            if (
                not _has_minimum_source_ingestion_outputs(source_report)
                or not _has_required_full_text_outputs(source_report)
            ):
                failed_sources.append(source_report["source_key"])
        errors = report.get("errors", [])
        return dg.AssetCheckResult(
            passed=not failed_sources and not errors,
            metadata={
                "mode": report.get("mode"),
                "partition_date": report.get("partition_date"),
                "failed_sources": failed_sources,
                "empty_sources": empty_sources,
                "errors": errors,
                "source_keys": report.get("source_keys", []),
                "full_text_blocking_sources": report.get("full_text_blocking_sources", []),
                "full_text_triage": _metadata_table(
                    report.get("full_text_triage", []),
                    _FULL_TEXT_TRIAGE_TABLE_COLUMNS,
                ),
                "totals": report.get("totals", {}),
            },
        )

    @dg.asset(group_name="ingestion_bridge_v2")
    def source_registry() -> list[dict]:
        """Canonical source registry for ingestion."""

        return [source.model_dump(mode="json") for source in get_initial_sources()]

    @dg.asset(group_name="ingestion_bridge_v2")
    def source_queries(source_registry: list[dict]) -> list[dict]:
        """Starter source-specific queries."""

        enabled_sources = {source["source_key"] for source in source_registry if source["enabled"]}
        queries = [query for query in build_source_queries() if query.source_key in enabled_sources]
        return [query.model_dump(mode="json") for query in queries]

    @dg.asset(group_name="ingestion_bridge_v2")
    def source_scout_plan(source_registry: list[dict]) -> dict:
        """Placeholder for the Source Scout agent's ingestion gap plan."""

        request = SourceScoutRequest(focus="all", max_phase=3, max_recommendations=12)
        return {
            "status": "pending_source_scout_agent",
            "registered_sources": len(source_registry),
            "default_scout_contract": request.model_dump(mode="json"),
        }

    @dg.asset(group_name="ingestion_bridge_v2")
    def raw_source_records(source_queries: list[dict], source_scout_plan: dict) -> list[dict]:
        """Placeholder for raw records harvested by source-specific workers."""

        return [
            {
                "source_key": query["source_key"],
                "query_name": query["query_name"],
                "status": "pending_harvester",
                "scout_status": source_scout_plan["status"],
            }
            for query in source_queries
        ]

    @dg.asset(group_name="ingestion_bridge_v2")
    def canonical_research_objects(raw_source_records: list[dict]) -> list[dict]:
        """Placeholder for resolver-normalized research objects."""

        objects = [
            ResearchObject(
                object_type="publication",
                title=f"Pending resolver output for {record['source_key']}:{record['query_name']}",
                source_key=record["source_key"],
                metadata={"status": record["status"]},
            )
            for record in raw_source_records
        ]
        return [obj.model_dump(mode="json") for obj in objects]

    @dg.asset(group_name="ingestion_bridge_v2")
    def document_chunks(canonical_research_objects: list[dict]) -> list[dict]:
        """Placeholder for legal text and abstract chunks."""

        return [
            {
                "research_object_id": obj["id"],
                "chunk_index": 0,
                "status": "pending_chunker",
                "title": obj["title"],
            }
            for obj in canonical_research_objects
        ]

    @dg.asset(group_name="ingestion_bridge_v2")
    def normalized_entities(document_chunks: list[dict]) -> list[dict]:
        """Placeholder for entity mentions and normalized entities."""

        return [
            {
                "research_object_id": chunk["research_object_id"],
                "status": "pending_entity_mapper",
            }
            for chunk in document_chunks
        ]

    @dg.asset(group_name="ingestion_bridge_v2")
    def claims(normalized_entities: list[dict]) -> dict:
        """Placeholder for claim extraction output."""

        request = ClaimSearchRequest(query="canine hemangiosarcoma", species="canine", limit=20)
        return {
            "status": "pending_claim_extractor",
            "entity_batches": len(normalized_entities),
            "default_search_contract": request.model_dump(mode="json"),
        }

    @dg.asset(group_name="ingestion_bridge_v2")
    def curated_claims(claims: dict) -> dict:
        """Placeholder for the claim curator agent output."""

        request = ClaimCurationRequest(limit=250, model_profile="reviewer")
        return {
            "status": "pending_claim_curator_agent",
            "upstream_status": claims["status"],
            "default_curation_contract": request.model_dump(mode="json"),
        }

    @dg.asset(group_name="ingestion_bridge_v2")
    def coverage_snapshot(source_registry: list[dict], curated_claims: dict) -> dict:
        """Coverage report placeholder."""

        return {
            "source_count": len(source_registry),
            "claim_status": curated_claims["status"],
            "phase": 0,
        }

    @dg.asset(group_name="structured_source_refresh")
    def structured_source_pipeline_report(research_repository: ResearchRepositoryResource) -> dict:
        """Executable structured-source refresh, extraction, curation, and QA report."""

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(repository)

    @dg.asset(group_name="structured_source_refresh")
    def structured_source_smoke_report(research_repository: ResearchRepositoryResource) -> dict:
        """Small hosted-runtime validation run against a single structured source."""

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=STRUCTURED_SOURCE_SMOKE_KEYS,
            source_limits={"pubchem": 1},
            extract_limit=50,
            curate_limit=50,
        )

    @dg.asset(group_name="structured_source_refresh")
    def structured_source_multisource_smoke_report(research_repository: ResearchRepositoryResource) -> dict:
        """Small hosted-runtime validation run across all structured API harvesters."""

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=STRUCTURED_SOURCE_MULTISOURCE_SMOKE_KEYS,
            source_limits={source_key: 1 for source_key in STRUCTURED_SOURCE_MULTISOURCE_SMOKE_KEYS},
            extract_limit=250,
            curate_limit=250,
        )

    @dg.asset(group_name="research_primitives")
    def research_primitive_source_smoke_report(research_repository: ResearchRepositoryResource) -> dict:
        """Small hosted-runtime validation run for primitive source ingestion."""

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=RESEARCH_PRIMITIVE_SOURCE_SMOKE_KEYS,
            source_limits={source_key: 2 for source_key in RESEARCH_PRIMITIVE_SOURCE_SMOKE_KEYS},
            extract_limit=50,
            curate_limit=50,
        )

    @dg.asset(group_name="research_primitives")
    def research_primitive_source_pipeline_report(research_repository: ResearchRepositoryResource) -> dict:
        """Manual primitive-source refresh for entity, compound, pathway, and ontology lookup data."""

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=ENTITY_LOOKUP_INDEX_SOURCE_KEYS,
            source_limits={source_key: 5 for source_key in ENTITY_LOOKUP_INDEX_SOURCE_KEYS},
            extract_limit=500,
            curate_limit=500,
        )

    @dg.asset(group_name="literature_clinical_refresh")
    def literature_clinical_smoke_report(research_repository: ResearchRepositoryResource) -> dict:
        """Small hosted-runtime validation run across literature and clinical APIs."""

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=LITERATURE_CLINICAL_SMOKE_KEYS,
            source_limits={source_key: 1 for source_key in LITERATURE_CLINICAL_SMOKE_KEYS},
            extract_limit=250,
            curate_limit=250,
        )

    @dg.asset(group_name="hosted_api_refresh")
    def all_api_smoke_report(research_repository: ResearchRepositoryResource) -> dict:
        """Small hosted-runtime API heartbeat across every implemented harvester."""

        from .structured_orchestration import run_structured_sources_ingestion_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_ingestion_pipeline(
            repository,
            source_keys=ALL_API_SMOKE_KEYS,
            source_limits={source_key: 1 for source_key in ALL_API_SMOKE_KEYS},
        )

    @dg.asset(group_name="literature_corpus_harvest")
    def literature_corpus_harvest_report(research_repository: ResearchRepositoryResource) -> dict:
        """Hundreds-scale hosted literature ingestion across metadata and abstract sources."""

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=LITERATURE_CORPUS_SOURCE_KEYS,
            source_limits=LITERATURE_CORPUS_SOURCE_LIMITS,
            extract_limit=5000,
            curate_limit=5000,
        )

    @dg.asset(
        group_name="literature_corpus_partitions",
        partitions_def=LITERATURE_CORPUS_SOURCE_DATE_PARTITIONS,
    )
    def literature_corpus_source_date_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Source/date literature corpus harvest partition for rerunnable daily slices."""

        return _run_literature_corpus_source_date_partition(context, research_repository)

    @dg.asset(group_name="literature_full_text_refresh")
    def literature_full_text_refresh_report(research_repository: ResearchRepositoryResource) -> dict:
        """Bounded hosted full-text ingestion for licensed open-access sources."""

        return _run_literature_full_text_refresh(
            research_repository,
            source_keys=LITERATURE_FULL_TEXT_SOURCE_KEYS,
            source_limits=LITERATURE_FULL_TEXT_SOURCE_LIMITS,
            extract_limit=1000,
            curate_limit=1000,
        )

    @dg.asset(group_name="literature_full_text_refresh")
    def europe_pmc_full_text_refresh_report(research_repository: ResearchRepositoryResource) -> dict:
        """Single-source hosted full-text refresh for Europe PMC."""

        return _run_literature_full_text_refresh(
            research_repository,
            source_keys=("europe_pmc",),
            source_limits={"europe_pmc": LITERATURE_FULL_TEXT_SOURCE_LIMITS["europe_pmc"]},
            extract_limit=500,
            curate_limit=500,
        )

    @dg.asset(group_name="literature_full_text_refresh")
    def pmc_oa_full_text_refresh_report(research_repository: ResearchRepositoryResource) -> dict:
        """Single-source hosted full-text refresh for PMC OA."""

        return _run_literature_full_text_refresh(
            research_repository,
            source_keys=("pmc_oa",),
            source_limits={"pmc_oa": LITERATURE_FULL_TEXT_SOURCE_LIMITS["pmc_oa"]},
            extract_limit=500,
            curate_limit=500,
        )

    @dg.asset(group_name="literature_full_text_ingestion")
    def literature_full_text_ingest_smoke_report(research_repository: ResearchRepositoryResource) -> dict:
        """Fast hosted full-text pull validation without extraction or curation."""

        return _run_literature_full_text_ingestion(
            research_repository,
            source_keys=LITERATURE_FULL_TEXT_SOURCE_KEYS,
            source_limits=LITERATURE_FULL_TEXT_SMOKE_LIMITS,
        )

    @dg.asset(group_name="literature_full_text_ingestion")
    def europe_pmc_full_text_ingest_report(research_repository: ResearchRepositoryResource) -> dict:
        """Europe PMC full-text pull and persistence path without extraction."""

        return _run_literature_full_text_ingestion(
            research_repository,
            source_keys=("europe_pmc",),
            source_limits={"europe_pmc": LITERATURE_FULL_TEXT_SOURCE_LIMITS["europe_pmc"]},
        )

    @dg.asset(group_name="literature_full_text_ingestion")
    def pmc_oa_full_text_ingest_report(research_repository: ResearchRepositoryResource) -> dict:
        """PMC OA full-text pull and persistence path without extraction."""

        return _run_literature_full_text_ingestion(
            research_repository,
            source_keys=("pmc_oa",),
            source_limits={"pmc_oa": LITERATURE_FULL_TEXT_SOURCE_LIMITS["pmc_oa"]},
        )

    @dg.asset(group_name="literature_full_text_refresh")
    def literature_full_text_smoke_report(research_repository: ResearchRepositoryResource) -> dict:
        """Fast hosted full-text validation with one record per full-text source."""

        return _run_literature_full_text_refresh(
            research_repository,
            source_keys=LITERATURE_FULL_TEXT_SOURCE_KEYS,
            source_limits=LITERATURE_FULL_TEXT_SMOKE_LIMITS,
            extract_limit=25,
            curate_limit=25,
        )

    @dg.asset(
        group_name="literature_full_text_partitions",
        partitions_def=LITERATURE_FULL_TEXT_SOURCE_DATE_PARTITIONS,
    )
    def literature_full_text_source_date_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Source/date full-text refresh partition for rerunnable daily slices."""

        return _run_literature_full_text_source_date_partition(context, research_repository)

    @dg.asset(group_name="structured_source_refresh")
    def structured_source_count_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Persisted count report for hosted API source coverage."""

        from .structured_orchestration import build_structured_source_count_report

        repository = research_repository.build_repository()
        report = build_structured_source_count_report(
            repository,
            source_keys=HOSTED_API_REPORT_KEYS,
            sample_limit=3,
            require_claims=True,
        )
        return dg.MaterializeResult(value=report, metadata=_structured_source_count_report_metadata(report))

    @dg.asset(group_name="control_panel")
    def source_health_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Persisted source health report for hosted API source coverage."""

        from .source_health import build_source_health_report

        repository = research_repository.build_repository()
        report = build_source_health_report(
            repository,
            source_keys=HOSTED_API_REPORT_KEYS,
            sample_limit=3,
            require_claims=True,
        )
        return dg.MaterializeResult(value=report, metadata=_source_health_report_metadata(report))

    @dg.asset(
        group_name="control_panel",
        config_schema={
            "statuses": dg.Field(
                [str],
                default_value=["queued_for_intake", "triage_in_progress", "needs_more_information"],
                description="Candidate contribution intake statuses to include.",
            ),
            "candidate_ids": dg.Field(
                [str],
                default_value=[],
                description="Optional public candidate ids to filter.",
            ),
            "limit": dg.Field(int, default_value=50, description="Maximum intake rows to include."),
            "include_packet": dg.Field(
                bool,
                default_value=False,
                description="Include full submitted packet JSON in asset value.",
            ),
        },
    )
    def candidate_contribution_intake_report(context) -> dg.MaterializeResult:
        """Read-only report for public candidate contribution intake rows in Neon/Postgres."""

        from .candidate_contribution_intake import build_candidate_contribution_intake_report

        config = context.op_config
        report = build_candidate_contribution_intake_report(
            statuses=config["statuses"],
            candidate_ids=config["candidate_ids"],
            limit=config["limit"],
            include_packet=config["include_packet"],
        )
        return dg.MaterializeResult(value=report, metadata=_candidate_contribution_intake_report_metadata(report))

    @dg.asset(
        group_name="control_panel",
        config_schema={
            "contribution_ids": dg.Field(
                [str],
                default_value=[],
                description="Public contribution IDs to triage. Required for a real update.",
            ),
            "action": dg.Field(
                str,
                default_value="start_triage",
                description=(
                    "One of start_triage, request_more_information, reject, accept_for_evidence_review, "
                    "accept_for_validation_queue, accept_for_compute_review, archive."
                ),
            ),
            "operator": dg.Field(str, default_value="dagster", description="Operator identity recorded in notes."),
            "review_notes": dg.Field(str, default_value="", description="Operator note appended to the intake row."),
            "dry_run": dg.Field(bool, default_value=True, description="Preview only unless explicitly set false."),
        },
    )
    def candidate_contribution_triage_report(context) -> dg.MaterializeResult:
        """Apply or preview an explicit operator triage decision for public contribution intake."""

        from .candidate_contribution_intake import triage_candidate_contributions

        config = context.op_config
        report = triage_candidate_contributions(
            contribution_ids=config["contribution_ids"],
            action=config["action"],
            operator=config["operator"],
            review_notes=config["review_notes"],
            dry_run=config["dry_run"],
        )
        return dg.MaterializeResult(value=report, metadata=_candidate_contribution_triage_report_metadata(report))

    @dg.asset(
        group_name="control_panel",
        config_schema={
            "candidate_ids": dg.Field(
                [str],
                default_value=[],
                description="Public candidate IDs to seed curated work packets for.",
            ),
            "dry_run": dg.Field(
                bool,
                default_value=True,
                description="Preview the seed. Set false to persist work_packet rows.",
            ),
        },
    )
    def work_packet_seed_report(context) -> dg.MaterializeResult:
        """Idempotently seed Proof Network work packets for one or more candidates.

        Each candidate gets the curated starter set (citation_repair,
        claim_critique, evidence_addition). Inserts are ``ON CONFLICT
        DO NOTHING`` on work_packet_id, so reruns are safe.
        """

        from .work_packets import curated_work_packets_for_candidate, seed_work_packets

        config = context.op_config
        candidate_ids = [str(value).strip() for value in (config.get("candidate_ids") or []) if str(value).strip()]
        records = []
        for candidate_id in candidate_ids:
            records.extend(curated_work_packets_for_candidate(candidate_id))
        report = seed_work_packets(records, dry_run=bool(config["dry_run"]))
        return dg.MaterializeResult(
            value=report,
            metadata=_work_packet_seed_report_metadata(report),
        )

    @dg.asset(
        group_name="agent_ops",
        config_schema={
            "reviewer_type": dg.Field(
                str,
                is_required=False,
                description="Optional reviewer_type filter (operator | llm_evaluator | system | external_expert).",
            ),
            "limit": dg.Field(int, default_value=500, description="Maximum proof capsule reviews to scan."),
            "include_existing": dg.Field(
                bool,
                default_value=False,
                description="Regenerate existing reward events instead of skipping them.",
            ),
            "created_by": dg.Field(
                str,
                default_value="dagster_proof_capsule_review_sync",
                description="Ledger creator identity.",
            ),
        },
    )
    def proof_capsule_reward_sync_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Convert proof_capsule_reviews into reward events through the existing ledger.

        Idempotent on review_id via uuid5 over a deterministic identity key.
        Reruns are safe; the sync skips reviews that already have a reward
        event unless include_existing is true.
        """

        from .proof_capsules import sync_reward_events_from_proof_capsule_reviews

        config = context.op_config
        repository = research_repository.build_repository()
        report = sync_reward_events_from_proof_capsule_reviews(
            repository,
            reviewer_type=config.get("reviewer_type"),
            limit=config["limit"],
            include_existing=config["include_existing"],
            created_by=config["created_by"],
        )
        return dg.MaterializeResult(
            value=report,
            metadata=_proof_capsule_reward_sync_report_metadata(report),
        )

    @dg.asset(
        group_name="control_panel",
        config_schema={
            "source_keys": dg.Field(
                [str],
                default_value=[],
                description="Optional source keys for source-health scope.",
            ),
            "include_source_health": dg.Field(
                bool,
                default_value=True,
                description="Include source health in the command-center report.",
            ),
            "include_recent_agents": dg.Field(
                bool,
                default_value=True,
                description="Include recent agent runs in the command-center report.",
            ),
            "queue_limit": dg.Field(int, default_value=25, description="Maximum queue items to show."),
            "lead_limit": dg.Field(int, default_value=25, description="Maximum research leads to show."),
            "agent_run_limit": dg.Field(int, default_value=25, description="Maximum recent agent runs to show."),
            "min_health_score": dg.Field(float, default_value=0.65, description="Minimum source health score."),
            "require_claims": dg.Field(bool, default_value=True, description="Require claims for source health."),
        },
    )
    def command_center_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Read-only command-center snapshot for TWOG operations."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).build_command_center_report(
            CommandCenterRequest(
                source_keys=config["source_keys"],
                include_source_health=config["include_source_health"],
                include_recent_agents=config["include_recent_agents"],
                queue_limit=config["queue_limit"],
                lead_limit=config["lead_limit"],
                agent_run_limit=config["agent_run_limit"],
                min_health_score=config["min_health_score"],
                require_claims=config["require_claims"],
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_command_center_report_metadata(report))

    @dg.asset(
        group_name="control_panel",
        config_schema={
            "lead_ids": dg.Field(
                [str],
                default_value=[],
                description="Optional research lead ids to inspect.",
            ),
            "lead_statuses": dg.Field(
                [str],
                default_value=["new", "watching", "followup", "queued"],
                description="Research lead statuses to scan.",
            ),
            "source_keys": dg.Field(
                [str],
                default_value=[],
                description="Optional source key scope.",
            ),
            "limit": dg.Field(int, default_value=100, description="Maximum leads to include."),
            "task_limit": dg.Field(int, default_value=250, description="Maximum task rows to include."),
            "stale_after_hours": dg.Field(int, default_value=72, description="Age threshold for stale open tasks."),
            "include_tasks": dg.Field(bool, default_value=True, description="Include task-level rows."),
            "include_suppressed": dg.Field(bool, default_value=True, description="Include suppressed task rows."),
        },
    )
    def research_hunt_queue_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Read-only control report for research hunt queue hygiene."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).build_research_hunt_queue_report(
            ResearchHuntQueueReportRequest(
                lead_ids=[UUID(value) for value in config["lead_ids"]],
                lead_statuses=config["lead_statuses"],
                source_keys=config["source_keys"],
                limit=config["limit"],
                task_limit=config["task_limit"],
                stale_after_hours=config["stale_after_hours"],
                include_tasks=config["include_tasks"],
                include_suppressed=config["include_suppressed"],
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_research_hunt_queue_report_metadata(report))

    @dg.asset(
        group_name="control_panel",
        config_schema={
            "lead_ids": dg.Field(
                [str],
                default_value=[],
                description="Optional research lead ids to clean.",
            ),
            "lead_statuses": dg.Field(
                [str],
                default_value=["new", "watching", "followup", "queued"],
                description="Research lead statuses to scan.",
            ),
            "source_keys": dg.Field(
                [str],
                default_value=[],
                description="Optional source key scope.",
            ),
            "reasons": dg.Field(
                [str],
                default_value=[
                    "stale_broad_or_passive",
                    "duplicate_broad_family",
                    "passive_monitoring_note",
                ],
                description="Safe maintenance reasons to apply.",
            ),
            "stale_after_hours": dg.Field(int, default_value=72, description="Age threshold for stale open tasks."),
            "limit": dg.Field(int, default_value=50, description="Maximum tasks to suppress."),
            "dry_run": dg.Field(bool, default_value=True, description="Preview changes without mutating leads."),
        },
    )
    def research_hunt_queue_maintenance_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Dry-run or apply safe research hunt queue cleanup."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).maintain_research_hunt_queue(
            ResearchHuntQueueMaintenanceRequest(
                lead_ids=[UUID(value) for value in config["lead_ids"]],
                lead_statuses=config["lead_statuses"],
                source_keys=config["source_keys"],
                reasons=config["reasons"],
                stale_after_hours=config["stale_after_hours"],
                limit=config["limit"],
                dry_run=config["dry_run"],
                operator="dagster",
                dagster_run_id=context.run_id,
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_research_hunt_queue_maintenance_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "lead_ids": dg.Field(
                [str],
                default_value=[],
                description="Optional ready research lead ids to queue.",
            ),
            "lead_statuses": dg.Field(
                [str],
                default_value=["new", "watching", "followup"],
                description="Research lead statuses to scan.",
            ),
            "source_keys": dg.Field(
                [str],
                default_value=[],
                description="Optional source key scope.",
            ),
            "limit": dg.Field(int, default_value=10, description="Maximum ready leads to queue."),
            "disease_scope": dg.Field(
                str,
                default_value="canine hemangiosarcoma and human angiosarcoma",
                description="Disease/scope guardrail for retrieval and synthesis.",
            ),
            "priority": dg.Field(int, default_value=40, description="Default queue priority."),
            "max_chunks_per_perspective": dg.Field(
                int,
                default_value=10,
                description="Maximum chunks to retrieve per perspective query.",
            ),
            "max_claims": dg.Field(int, default_value=20, description="Maximum claims to include."),
            "max_chunk_chars": dg.Field(
                int,
                default_value=2200,
                description="Maximum characters per cited chunk sent to the reviewer.",
            ),
            "brief_style": dg.Field(str, default_value="technical", description="Research brief style."),
            "model_profile": dg.Field(str, default_value="research_brief", description="Logical model profile."),
            "review_mode": dg.Field(str, default_value="openrouter_required", description="Review mode."),
            "review_models": dg.Field(
                [str],
                default_value=[],
                description="OpenRouter model ids for OpenRouter-backed review modes.",
            ),
            "dry_run": dg.Field(bool, default_value=True, description="Preview queueing without mutating records."),
            "transition_leads": dg.Field(bool, default_value=True, description="Mark newly queued leads as queued."),
            "create_handoff_docs": dg.Field(bool, default_value=True, description="Create plain-language synthesis handoff docs for ready leads."),
        },
    )
    def research_hunt_synthesis_queue_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Queue ready research-hunt leads into the research brief synthesis lane."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).queue_ready_research_hunt_synthesis(
            ResearchHuntSynthesisQueueRequest(
                lead_ids=[UUID(value) for value in config["lead_ids"]],
                lead_statuses=config["lead_statuses"],
                source_keys=config["source_keys"],
                limit=config["limit"],
                disease_scope=config["disease_scope"],
                priority=config["priority"],
                max_chunks_per_perspective=config["max_chunks_per_perspective"],
                max_claims=config["max_claims"],
                max_chunk_chars=config["max_chunk_chars"],
                brief_style=config["brief_style"],
                model_profile=config["model_profile"],
                review_mode=config["review_mode"],
                review_models=config["review_models"],
                create_handoff_docs=config.get("create_handoff_docs", True),
                dry_run=config["dry_run"],
                transition_leads=config["transition_leads"],
                operator="dagster",
                dagster_run_id=context.run_id,
                metadata={"dagster_synthesis_queue_run_id": context.run_id},
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_research_hunt_synthesis_queue_metadata(report))

    @dg.asset(
        group_name="agent_ops",
        config_schema={
            "lead_ids": dg.Field([str], default_value=[], description="Optional lead IDs to document."),
            "lead_statuses": dg.Field(
                [str],
                default_value=["new", "watching", "followup", "queued"],
                description="Lead statuses to scan.",
            ),
            "source_keys": dg.Field([str], default_value=[], description="Optional source keys to filter."),
            "limit": dg.Field(int, default_value=10, description="Maximum ready leads to document."),
            "max_claims": dg.Field(int, default_value=16, description="Maximum claims to include."),
            "max_chunks": dg.Field(int, default_value=12, description="Maximum chunks to footnote."),
            "max_chunk_chars": dg.Field(int, default_value=900, description="Maximum chars per chunk footnote."),
            "max_technical_footnotes": dg.Field(int, default_value=30, description="Maximum technical footnotes."),
            "include_technical_footnotes": dg.Field(bool, default_value=True, description="Include technical provenance footnotes."),
            "dry_run": dg.Field(bool, default_value=True, description="Preview docs without persisting artifacts."),
        },
    )
    def research_hunt_synthesis_doc_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Create plain-language synthesis handoff docs for ready research-hunt leads."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).create_ready_research_hunt_synthesis_docs(
            ResearchHuntSynthesisDocRequest(
                lead_ids=[UUID(value) for value in config["lead_ids"]],
                lead_statuses=config["lead_statuses"],
                source_keys=config["source_keys"],
                limit=config["limit"],
                max_claims=config["max_claims"],
                max_chunks=config["max_chunks"],
                max_chunk_chars=config["max_chunk_chars"],
                max_technical_footnotes=config["max_technical_footnotes"],
                include_technical_footnotes=config["include_technical_footnotes"],
                dry_run=config["dry_run"],
                operator="dagster",
                dagster_run_id=context.run_id,
                metadata={"dagster_synthesis_doc_run_id": context.run_id},
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_research_hunt_synthesis_doc_metadata(report))

    @dg.asset(group_name="agent_ops")
    def full_text_ops_agent_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Recommend-only full-text ops agent report over persisted hosted state."""

        from .service import HSAResearchService

        repository = research_repository.build_repository()
        result = HSAResearchService(repository).run_full_text_ops(FullTextOpsRequest())
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_full_text_ops_report_metadata(report))

    @dg.asset(
        group_name="agent_ops",
        config_schema={
            "agent_name": dg.Field(str, is_required=False, description="Optional agent name filter."),
            "status": dg.Field(str, is_required=False, description="Optional run status filter."),
            "source_key": dg.Field(str, is_required=False, description="Optional source key filter."),
            "limit": dg.Field(int, default_value=500, description="Recent agent runs to scan."),
            "min_sample_size": dg.Field(int, default_value=3, description="Runs required before sample is reliable."),
        },
    )
    def agent_performance_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Hybrid operator/evaluator performance report for the agent ledger."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).build_agent_performance_report(
            AgentPerformanceReportRequest(
                agent_name=config.get("agent_name"),
                status=config.get("status"),
                source_key=config.get("source_key"),
                limit=config["limit"],
                min_sample_size=config["min_sample_size"],
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_agent_performance_report_metadata(report))

    @dg.asset(
        group_name="agent_ops",
        config_schema={
            "agent_name": dg.Field(str, is_required=False, description="Optional agent name filter."),
            "status": dg.Field(str, default_value="completed", description="Optional run status filter."),
            "source_key": dg.Field(str, is_required=False, description="Optional source key filter."),
            "limit": dg.Field(int, default_value=25, description="Recent reviewed runs to evaluate."),
            "reviewed_only": dg.Field(bool, default_value=True, description="Evaluate only operator-reviewed runs."),
            "model_profile": dg.Field(
                str,
                default_value="agent_performance_evaluator",
                description="Logical evaluator profile recorded in the ledger.",
            ),
            "review_models": dg.Field(
                [str],
                default_value=[],
                description="OpenRouter evaluator model ids to use.",
            ),
        },
    )
    def agent_performance_evaluation_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual OpenRouter specialist evaluator pass over recent reviewed agent runs."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).run_agent_performance_evaluation(
            AgentPerformanceEvaluationRequest(
                agent_name=config.get("agent_name"),
                status=config.get("status"),
                source_key=config.get("source_key"),
                limit=config["limit"],
                reviewed_only=config["reviewed_only"],
                model_profile=config.get("model_profile") or "agent_performance_evaluator",
                review_models=config.get("review_models") or [],
                dagster_run_id=context.run_id,
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_agent_performance_evaluation_metadata(report))

    @dg.asset(
        group_name="agent_ops",
        config_schema={
            "agent_name": dg.Field(str, is_required=False, description="Optional agent name filter."),
            "source_key": dg.Field(str, is_required=False, description="Optional source key filter."),
            "event_source": dg.Field(str, is_required=False, description="Optional reward event source filter."),
            "group_by": dg.Field(
                str,
                default_value="agent_name",
                description="Reward report group: agent_name, model_profile, task_type, source_key, or event_source.",
            ),
            "limit": dg.Field(int, default_value=500, description="Recent reward events to scan."),
            "min_sample_size": dg.Field(int, default_value=3, description="Events required before sample is reliable."),
        },
    )
    def reward_event_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Learning-loop reward report over persisted reward events."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).build_reward_report(
            RewardReportRequest(
                agent_name=config.get("agent_name"),
                source_key=config.get("source_key"),
                event_source=config.get("event_source"),
                group_by=config["group_by"],
                limit=config["limit"],
                min_sample_size=config["min_sample_size"],
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_reward_report_metadata(report))

    @dg.asset(
        group_name="agent_ops",
        config_schema={
            "agent_name": dg.Field(str, is_required=False, description="Optional agent name filter."),
            "source_key": dg.Field(str, is_required=False, description="Optional source key filter."),
            "reviewer_type": dg.Field(str, is_required=False, description="operator, llm_evaluator, or system."),
            "limit": dg.Field(int, default_value=500, description="Recent reviews to scan."),
            "include_existing": dg.Field(bool, default_value=False, description="Regenerate existing review reward events."),
            "created_by": dg.Field(str, default_value="dagster_reward_review_sync", description="Ledger creator identity."),
        },
    )
    def reward_event_sync_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Derive reward events from operator and evaluator agent-run reviews."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).sync_reward_events_from_reviews(
            RewardEventSyncRequest(
                agent_name=config.get("agent_name"),
                source_key=config.get("source_key"),
                reviewer_type=config.get("reviewer_type"),
                limit=config["limit"],
                include_existing=config["include_existing"],
                created_by=config["created_by"],
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_reward_sync_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "limit": dg.Field(
                int,
                default_value=20,
                description="Maximum unreviewed capsules to grade per pass.",
            ),
            "model_name": dg.Field(
                str,
                is_required=False,
                description="OpenRouter model identifier (defaults to claude-haiku-4-5-20251001).",
            ),
            "dry_run": dg.Field(
                bool,
                default_value=False,
                description="Skip persisting reviews; print scores only.",
            ),
        },
    )
    def capsule_llm_grade_report(context) -> dg.MaterializeResult:
        """LLM-as-judge pre-grade for proof capsules.

        Finds capsules without any operator-grade review and asks an LLM
        to score them across the 7 rubric dimensions. Stores results as
        proof_capsule_reviews rows with reviewer_type="llm_evaluator" —
        these are advisory recommendations only and do not award proof
        points (sync_reward_events_from_proof_capsule_reviews skips
        reviewer_type=llm_evaluator). Operators can adopt or override
        the LLM grade when they triage their queue.
        """

        from .llm_capsule_grader import grade_pending_capsules

        config = context.op_config
        result = grade_pending_capsules(
            limit=config["limit"],
            model_name=config.get("model_name"),
            dry_run=config["dry_run"],
        )
        metadata: dict[str, Any] = {
            "scanned": dg.MetadataValue.int(result.get("scanned", 0)),
            "graded": dg.MetadataValue.int(result.get("graded", 0)),
            "cached_hits": dg.MetadataValue.int(result.get("cached_hits", 0)),
            "error_count": dg.MetadataValue.int(len(result.get("errors") or [])),
            "dry_run": dg.MetadataValue.bool(bool(result.get("dry_run"))),
        }
        if result.get("errors"):
            metadata["errors"] = dg.MetadataValue.json(result["errors"])
        return dg.MaterializeResult(value=result, metadata=metadata)

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "topic": dg.Field(
                str,
                default_value="canine hemangiosarcoma translational therapy",
                description="Research topic or question to brief.",
            ),
            "disease_scope": dg.Field(
                str,
                default_value="canine hemangiosarcoma and human angiosarcoma",
                description="Disease/scope guardrail for retrieval and synthesis.",
            ),
            "source_key": dg.Field(
                str,
                is_required=False,
                description="Optional source key filter for chunk retrieval.",
            ),
            "max_chunks_per_perspective": dg.Field(
                int,
                default_value=8,
                description="Maximum chunks to retrieve per perspective query.",
            ),
            "max_claims": dg.Field(
                int,
                default_value=12,
                description="Maximum stored claims to include in the evidence payload.",
            ),
            "max_chunk_chars": dg.Field(
                int,
                default_value=1800,
                description="Maximum characters per cited chunk sent to the reviewer.",
            ),
            "brief_style": dg.Field(
                str,
                default_value="technical",
                description="Brief style: technical, operator, substack, or vet_partner.",
            ),
            "review_mode": dg.Field(
                str,
                default_value="openrouter_required",
                description="Research brief review mode: openrouter_required, openrouter_compare, external_required, or deterministic_only.",
            ),
            "review_models": dg.Field(
                [str],
                default_value=[],
                description="OpenRouter model ids to use when using an OpenRouter review mode.",
            ),
        },
    )
    def research_brief_agent_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual citation-first research brief from evidence, translational, and skeptic agents."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).run_research_brief(
            ResearchBriefRequest(
                topic=config["topic"],
                disease_scope=config["disease_scope"],
                source_key=config.get("source_key"),
                max_chunks_per_perspective=config["max_chunks_per_perspective"],
                max_claims=config["max_claims"],
                max_chunk_chars=config["max_chunk_chars"],
                brief_style=config["brief_style"],
                review_mode=config["review_mode"],
                review_models=config["review_models"],
                dagster_run_id=context.run_id,
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_research_brief_report_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "topic": dg.Field(
                str,
                default_value="curative or disease-modifying therapy ideas for canine hemangiosarcoma",
                description="Therapy committee topic.",
            ),
            "disease_scope": dg.Field(
                str,
                default_value="canine hemangiosarcoma and human angiosarcoma",
                description="Disease/scope guardrail for retrieval and synthesis.",
            ),
            "source_key": dg.Field(
                str,
                is_required=False,
                description="Optional source key filter for chunk retrieval.",
            ),
            "max_chunks_per_perspective": dg.Field(int, default_value=10),
            "max_claims": dg.Field(int, default_value=20),
            "max_chunk_chars": dg.Field(int, default_value=2200),
            "max_ideas_per_perspective": dg.Field(int, default_value=4),
            "max_ranked_ideas": dg.Field(int, is_required=False),
            "review_mode": dg.Field(
                str,
                default_value="openrouter_required",
                description="Committee review mode: openrouter_required, openrouter_compare, external_required, or deterministic_only.",
            ),
            "review_models": dg.Field([str], default_value=[]),
            "program_id": dg.Field(str, is_required=False),
            "brief_id": dg.Field(str, is_required=False),
            "evaluation_id": dg.Field(str, is_required=False),
        },
    )
    def therapy_committee_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual cited therapy ideation committee over stored evidence."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).run_therapy_committee(
            TherapyCommitteeRequest(
                topic=config["topic"],
                disease_scope=config["disease_scope"],
                source_key=config.get("source_key"),
                max_chunks_per_perspective=config["max_chunks_per_perspective"],
                max_claims=config["max_claims"],
                max_chunk_chars=config["max_chunk_chars"],
                max_ideas_per_perspective=config["max_ideas_per_perspective"],
                max_ranked_ideas=config.get("max_ranked_ideas"),
                review_mode=config["review_mode"],
                review_models=config["review_models"],
                program_id=_uuid_or_none(config.get("program_id")),
                brief_id=_uuid_or_none(config.get("brief_id")),
                evaluation_id=_uuid_or_none(config.get("evaluation_id")),
                dagster_run_id=context.run_id,
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_therapy_committee_metadata(report))

    @dg.asset(group_name="ai_research")
    def validation_tool_catalog_report(
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual read-only report of recommend-only validation tool capabilities."""

        from .service import HSAResearchService

        repository = research_repository.build_repository()
        report = HSAResearchService(repository).list_validation_tool_catalog(
            ValidationToolCatalogRequest()
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_validation_tool_catalog_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "validation_type": dg.Field(str, default_value="expert_review"),
            "task_type": dg.Field(str, default_value="expert_review"),
            "objective": dg.Field(str, default_value="Review a cited therapy hypothesis for validation readiness."),
            "candidate_name": dg.Field(str, is_required=False),
            "target_name": dg.Field(str, is_required=False),
            "limit": dg.Field(int, default_value=5),
        },
    )
    def validation_tool_match_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual read-only validation tool match report for a task context."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).match_validation_tools(
            ValidationToolMatchRequest(
                validation_type=config.get("validation_type"),
                task_type=config.get("task_type"),
                objective=config.get("objective"),
                candidate_name=config.get("candidate_name"),
                target_name=config.get("target_name"),
                limit=config.get("limit", 5),
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata={
                "match_count": dg.MetadataValue.int(int(report.get("match_count", 0))),
                "matches": dg.MetadataValue.json(report.get("matches", [])),
            },
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "status": dg.Field(str, is_required=False),
            "program_id": dg.Field(str, is_required=False),
            "topic_query": dg.Field(str, is_required=False),
            "limit": dg.Field(int, default_value=50),
        },
    )
    def therapy_idea_library_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual read-only report of persisted therapy ideas."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).list_therapy_ideas(
            TherapyIdeaLibraryRequest(
                status=config.get("status"),
                source_program_id=_uuid_or_none(config.get("program_id")),
                topic_query=config.get("topic_query"),
                limit=config.get("limit", 50),
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_therapy_idea_library_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "therapy_idea_id": dg.Field(str, description="Therapy idea ID to project into a public candidate."),
            "candidate_id": dg.Field(str, is_required=False),
            "display_id": dg.Field(str, is_required=False),
            "candidate_kind": dg.Field(str, is_required=False),
            "visibility": dg.Field(str, default_value="private"),
            "public_status": dg.Field(str, is_required=False),
            "pipeline_version": dg.Field(str, is_required=False),
            "commit_sha": dg.Field(str, is_required=False),
            "include_compute_jobs": dg.Field(bool, default_value=True),
            "include_decisions": dg.Field(bool, default_value=True),
            "include_artifacts": dg.Field(bool, default_value=True),
            "require_moonshot_grade": dg.Field(bool, default_value=True),
            "min_moonshot_score": dg.Field(float, default_value=0.8),
            "persist": dg.Field(bool, default_value=True),
        },
    )
    def public_candidate_snapshot_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual public-proof snapshot generator for moonshot-grade therapy ideas."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).generate_public_candidate_snapshot(
            PublicCandidateGenerateRequest(
                candidate_id=config.get("candidate_id"),
                therapy_idea_id=_uuid_or_none(config.get("therapy_idea_id")),
                display_id=config.get("display_id"),
                candidate_kind=config.get("candidate_kind"),
                visibility=config.get("visibility", "private"),
                public_status=config.get("public_status"),
                pipeline_version=config.get("pipeline_version"),
                commit_sha=config.get("commit_sha"),
                include_compute_jobs=config.get("include_compute_jobs", True),
                include_decisions=config.get("include_decisions", True),
                include_artifacts=config.get("include_artifacts", True),
                require_moonshot_grade=config.get("require_moonshot_grade", True),
                min_moonshot_score=config.get("min_moonshot_score", 0.8),
                persist=config.get("persist", True),
                metadata={"dagster_run_id": context.run_id},
            )
        ).model_dump(mode="json")
        metadata = _public_candidate_snapshot_metadata(report)
        if report.get("errors") or not report.get("candidate") or not report.get("snapshot"):
            candidate = report.get("candidate") if isinstance(report.get("candidate"), Mapping) else {}
            gate = report.get("moonshot_gate") if isinstance(report.get("moonshot_gate"), Mapping) else {}
            errors = report.get("errors") if isinstance(report.get("errors"), Sequence) else []
            blockers = gate.get("blockers") if isinstance(gate.get("blockers"), Sequence) else []
            reason = {
                "candidate_id": candidate.get("candidate_id") or config.get("candidate_id") or "",
                "therapy_idea_id": config.get("therapy_idea_id") or "",
                "has_candidate": bool(report.get("candidate")),
                "has_snapshot": bool(report.get("snapshot")),
                "errors": list(errors),
                "moonshot_gate_passed": gate.get("passed"),
                "moonshot_gate_blockers": list(blockers),
            }
            raise dg.Failure(
                description=(
                    "Public candidate snapshot generation returned no persisted snapshot: "
                    f"{json.dumps(reason, sort_keys=True, default=str)}"
                ),
                metadata=metadata,
            )
        return dg.MaterializeResult(value=report, metadata=metadata)

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "candidate_ids": dg.Field(
                str,
                default_value="",
                description="Comma-separated public candidate IDs to check.",
            ),
            "therapy_idea_ids": dg.Field(
                str,
                default_value="",
                description="Comma-separated therapy idea IDs that should exist.",
            ),
            "expected_pairs": dg.Field(
                str,
                default_value="",
                description="Comma-separated candidate_id=therapy_idea_uuid expectations.",
            ),
            "visibility": dg.Field(str, is_required=False),
            "limit": dg.Field(int, default_value=100),
            "fail_on_not_ready": dg.Field(
                bool,
                default_value=False,
                description="Fail the Dagster run when checked candidates are not strict-export ready.",
            ),
        },
    )
    def public_candidate_integrity_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Read-only integrity report for public candidate export readiness."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).build_public_candidate_integrity_report(
            PublicCandidateIntegrityReportRequest(
                candidate_ids=_csv_values(config.get("candidate_ids")),
                therapy_idea_ids=_uuid_csv_values(config.get("therapy_idea_ids")),
                expected_candidate_therapy_ids=_expected_public_candidate_pairs(config.get("expected_pairs")),
                visibility=config.get("visibility"),
                limit=config.get("limit", 100),
            )
        ).model_dump(mode="json")
        metadata = _public_candidate_integrity_metadata(report)
        if config.get("fail_on_not_ready", False) and not report.get("strict_export_ready"):
            reason = {
                "strict_export_ready": report.get("strict_export_ready"),
                "missing_candidate_ids": report.get("missing_candidate_ids", []),
                "missing_therapy_idea_ids": report.get("missing_therapy_idea_ids", []),
                "candidates_missing_manifest_receipt": report.get("candidates_missing_manifest_receipt", []),
                "checks": report.get("checks", []),
            }
            raise dg.Failure(
                description=f"Public candidate integrity check failed: {json.dumps(reason, sort_keys=True, default=str)}",
                metadata=metadata,
            )
        return dg.MaterializeResult(value=report, metadata=metadata)

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "brief_id": dg.Field(str, is_required=False),
            "evaluation_id": dg.Field(str, is_required=False),
            "therapy_idea_id": dg.Field(str, is_required=False),
            "topic_query": dg.Field(str, is_required=False),
            "source_key": dg.Field(str, is_required=False),
            "limit": dg.Field(int, default_value=50),
        },
    )
    def hypothesis_promotion_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual report of brief and therapy idea promotion candidates."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).build_hypothesis_promotion_report(
            HypothesisPromotionReportRequest(
                brief_id=_uuid_or_none(config.get("brief_id")),
                evaluation_id=_uuid_or_none(config.get("evaluation_id")),
                therapy_idea_id=_uuid_or_none(config.get("therapy_idea_id")),
                topic_query=config.get("topic_query"),
                source_key=config.get("source_key"),
                limit=config.get("limit", 50),
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_hypothesis_promotion_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "candidate_id": dg.Field(str, is_required=False),
            "therapy_idea_id": dg.Field(str, is_required=False),
            "plan_id": dg.Field(str, is_required=False),
            "queue_item_id": dg.Field(str, is_required=False),
            "brief_id": dg.Field(str, is_required=False),
            "evaluation_id": dg.Field(str, is_required=False),
            "topic_query": dg.Field(str, is_required=False),
            "source_key": dg.Field(str, is_required=False),
            "include_queue_items": dg.Field(bool, default_value=True),
            "include_evidence_addendum": dg.Field(bool, default_value=True),
            "addendum_limit": dg.Field(int, default_value=25),
            "queue_if_ready": dg.Field(bool, default_value=False),
            "dry_run": dg.Field(bool, default_value=True),
            "max_tasks": dg.Field(int, default_value=8),
            "priority": dg.Field(int, default_value=40),
            "limit": dg.Field(int, default_value=10),
            "model_profile": dg.Field(str, default_value="validation_packet_builder"),
        },
    )
    def validation_packet_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual validation packet report tying ideas to plans, queue items, and tool gates."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).build_validation_packets(
            ValidationPacketRequest(
                candidate_id=config.get("candidate_id"),
                therapy_idea_id=_uuid_or_none(config.get("therapy_idea_id")),
                plan_id=_uuid_or_none(config.get("plan_id")),
                queue_item_id=_uuid_or_none(config.get("queue_item_id")),
                brief_id=_uuid_or_none(config.get("brief_id")),
                evaluation_id=_uuid_or_none(config.get("evaluation_id")),
                topic_query=config.get("topic_query"),
                source_key=config.get("source_key"),
                include_queue_items=config.get("include_queue_items", True),
                include_evidence_addendum=config.get("include_evidence_addendum", True),
                addendum_limit=config.get("addendum_limit", 25),
                queue_if_ready=config.get("queue_if_ready", False),
                dry_run=config.get("dry_run", True),
                max_tasks=config.get("max_tasks", 8),
                priority=config.get("priority", 40),
                limit=config.get("limit", 10),
                model_profile=config.get("model_profile", "validation_packet_builder"),
                dagster_run_id=context.run_id,
                metadata={"dagster_packet_run_id": context.run_id},
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_validation_packet_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "candidate_id": dg.Field(str, is_required=False),
            "therapy_idea_id": dg.Field(str, is_required=False),
            "plan_id": dg.Field(str, is_required=False),
            "queue_item_id": dg.Field(str, is_required=False),
            "brief_id": dg.Field(str, is_required=False),
            "evaluation_id": dg.Field(str, is_required=False),
            "topic_query": dg.Field(str, is_required=False),
            "source_key": dg.Field(str, is_required=False),
            "include_queue_items": dg.Field(bool, default_value=True),
            "include_evidence_addendum": dg.Field(bool, default_value=True),
            "include_source_packets": dg.Field(bool, default_value=False),
            "persist_decisions": dg.Field(bool, default_value=True),
            "addendum_limit": dg.Field(int, default_value=25),
            "limit": dg.Field(int, default_value=10),
        },
    )
    def validation_decision_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual finite decision report for blocked or uncertain validation packets."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).build_validation_decision_report(
            ValidationDecisionReportRequest(
                candidate_id=config.get("candidate_id"),
                therapy_idea_id=_uuid_or_none(config.get("therapy_idea_id")),
                plan_id=_uuid_or_none(config.get("plan_id")),
                queue_item_id=_uuid_or_none(config.get("queue_item_id")),
                brief_id=_uuid_or_none(config.get("brief_id")),
                evaluation_id=_uuid_or_none(config.get("evaluation_id")),
                topic_query=config.get("topic_query"),
                source_key=config.get("source_key"),
                include_queue_items=config.get("include_queue_items", True),
                include_evidence_addendum=config.get("include_evidence_addendum", True),
                include_source_packets=config.get("include_source_packets", False),
                persist_decisions=config.get("persist_decisions", True),
                addendum_limit=config.get("addendum_limit", 25),
                limit=config.get("limit", 10),
                metadata={"dagster_decision_run_id": context.run_id},
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_validation_decision_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "thesis_topic": dg.Field(
                str,
                default_value="vascular injury / coagulation / angiogenesis ecology in canine HSA and human angiosarcoma",
            ),
            "disease_scope": dg.Field(str, default_value="canine hemangiosarcoma and human angiosarcoma"),
            "topic_query": dg.Field(str, is_required=False),
            "source_key": dg.Field(str, is_required=False),
            "program_id": dg.Field(str, is_required=False),
            "brief_ids": dg.Field([str], default_value=[]),
            "evaluation_ids": dg.Field([str], default_value=[]),
            "max_packets": dg.Field(int, default_value=5),
            "max_chunks": dg.Field(int, default_value=20),
            "max_programs": dg.Field(int, default_value=1),
            "max_evidence_loops": dg.Field(int, default_value=2),
            "review_mode": dg.Field(str, default_value="openrouter_required"),
            "review_models": dg.Field([str], default_value=[]),
            "model_profile": dg.Field(str, default_value="research_program_board"),
            "persist": dg.Field(bool, default_value=True),
        },
    )
    def research_program_board_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual big-idea research program board run."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).run_research_program_board(
            ResearchProgramReviewRequest(
                program_id=UUID(config["program_id"]) if config.get("program_id") else None,
                brief_ids=[UUID(value) for value in config.get("brief_ids", [])],
                evaluation_ids=[UUID(value) for value in config.get("evaluation_ids", [])],
                thesis_topic=config.get("thesis_topic"),
                disease_scope=config.get("disease_scope"),
                topic_query=config.get("topic_query"),
                source_key=config.get("source_key"),
                max_packets=config.get("max_packets", 5),
                max_chunks=config.get("max_chunks", 20),
                max_programs=config.get("max_programs", 1),
                max_evidence_loops=config.get("max_evidence_loops", 2),
                review_mode=config.get("review_mode", "openrouter_required"),
                review_models=config.get("review_models") or [],
                model_profile=config.get("model_profile", "research_program_board"),
                persist=config.get("persist", True),
                dagster_run_id=context.run_id,
                metadata={"dagster_program_board_run_id": context.run_id},
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_research_program_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "status": dg.Field(str, is_required=False),
            "gate_decision": dg.Field(str, is_required=False),
            "thesis_query": dg.Field(str, is_required=False),
            "limit": dg.Field(int, default_value=50),
        },
    )
    def research_program_library_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual read-only report of persisted research programs."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).list_research_programs(
            ResearchProgramBoardRequest(
                status=config.get("status"),
                gate_decision=config.get("gate_decision"),
                thesis_query=config.get("thesis_query"),
                limit=config.get("limit", 50),
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_research_program_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "program_id": dg.Field(str, is_required=False),
            "thesis_query": dg.Field(str, is_required=False),
            "source_keys": dg.Field([str], default_value=[]),
            "max_tasks": dg.Field(int, default_value=5),
            "max_source_queries": dg.Field(int, default_value=20),
            "max_sources_per_task": dg.Field(int, default_value=4),
            "queue_briefs": dg.Field(bool, default_value=True),
            "create_research_leads": dg.Field(bool, default_value=True),
            "create_source_queries": dg.Field(bool, default_value=True),
            "priority": dg.Field(int, default_value=40),
            "max_chunks_per_perspective": dg.Field(int, default_value=10),
            "max_claims": dg.Field(int, default_value=20),
            "max_chunk_chars": dg.Field(int, default_value=2200),
            "brief_style": dg.Field(str, default_value="technical"),
            "model_profile": dg.Field(str, default_value="research_brief"),
            "review_mode": dg.Field(str, default_value="openrouter_required"),
            "review_models": dg.Field([str], default_value=[]),
            "dry_run": dg.Field(bool, default_value=False),
        },
    )
    def research_program_evidence_loop_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Run one capped evidence loop for a persisted Research Program Board record."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        program_id = UUID(config["program_id"]) if config.get("program_id") else None
        report = HSAResearchService(repository).run_research_program_evidence_loop(
            ResearchProgramEvidenceLoopRequest(
                program_id=program_id,
                thesis_query=config.get("thesis_query"),
                source_keys=config["source_keys"],
                max_tasks=config["max_tasks"],
                max_source_queries=config["max_source_queries"],
                max_sources_per_task=config["max_sources_per_task"],
                queue_briefs=config["queue_briefs"],
                create_research_leads=config["create_research_leads"],
                create_source_queries=config["create_source_queries"],
                priority=config["priority"],
                max_chunks_per_perspective=config["max_chunks_per_perspective"],
                max_claims=config["max_claims"],
                max_chunk_chars=config["max_chunk_chars"],
                brief_style=config["brief_style"],
                model_profile=config["model_profile"],
                review_mode=config["review_mode"],
                review_models=config["review_models"],
                dry_run=config["dry_run"],
                dagster_run_id=context.run_id,
                metadata={"dagster_program_evidence_loop_run_id": context.run_id},
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_research_program_evidence_loop_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "program_id": dg.Field(str, is_required=False),
            "topic_query": dg.Field(
                str,
                default_value=(
                    "canine hemangiosarcoma human angiosarcoma VIM vimentin "
                    "transcriptome RNA-seq expression"
                ),
            ),
            "disease_terms": dg.Field([str], default_value=[]),
            "gene_symbols": dg.Field([str], default_value=[]),
            "source_keys": dg.Field([str], default_value=["geo", "sra"]),
            "query_texts": dg.Field([str], default_value=[]),
            "limit_per_query": dg.Field(int, default_value=5),
            "max_queries": dg.Field(int, default_value=8),
            "persist_queries": dg.Field(bool, default_value=True),
            "dry_run": dg.Field(bool, default_value=False),
        },
    )
    def omics_accession_hunt_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual bounded GEO/SRA accession hunt for omics evidence gaps."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).run_omics_accession_hunt(
            OmicsAccessionHuntRequest(
                program_id=UUID(config["program_id"]) if config.get("program_id") else None,
                topic_query=config["topic_query"],
                disease_terms=config.get("disease_terms") or [
                    "canine hemangiosarcoma",
                    "dog hemangiosarcoma",
                    "human angiosarcoma",
                    "angiosarcoma",
                ],
                gene_symbols=config.get("gene_symbols") or ["VIM", "vimentin"],
                source_keys=config.get("source_keys") or ["geo", "sra"],
                query_texts=config.get("query_texts") or [],
                limit_per_query=config["limit_per_query"],
                max_queries=config["max_queries"],
                persist_queries=config["persist_queries"],
                dry_run=config["dry_run"],
                dagster_run_id=context.run_id,
                metadata={"dagster_omics_accession_hunt_run_id": context.run_id},
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_omics_accession_hunt_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "program_id": dg.Field(str, is_required=False),
            "topic_query": dg.Field(
                str,
                default_value=(
                    "canine hemangiosarcoma human angiosarcoma VIM vimentin "
                    "transcriptome RNA-seq expression"
                ),
            ),
            "disease_terms": dg.Field([str], default_value=[]),
            "gene_symbols": dg.Field([str], default_value=[]),
            "source_keys": dg.Field([str], default_value=["geo", "sra"]),
            "accessions": dg.Field([str], default_value=[]),
            "limit": dg.Field(int, default_value=100),
            "min_datasets_per_packet": dg.Field(int, default_value=1),
            "include_context_packet": dg.Field(bool, default_value=True),
            "dry_run": dg.Field(bool, default_value=False),
        },
    )
    def omics_evidence_packet_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual packet builder from stored GEO/SRA omics accessions."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).build_omics_evidence_packets(
            OmicsEvidencePacketRequest(
                program_id=UUID(config["program_id"]) if config.get("program_id") else None,
                topic_query=config["topic_query"],
                disease_terms=config.get("disease_terms") or [
                    "canine hemangiosarcoma",
                    "dog hemangiosarcoma",
                    "human angiosarcoma",
                    "angiosarcoma",
                ],
                gene_symbols=config.get("gene_symbols") or ["VIM", "vimentin"],
                source_keys=config.get("source_keys") or ["geo", "sra"],
                accessions=config.get("accessions") or [],
                limit=config["limit"],
                min_datasets_per_packet=config["min_datasets_per_packet"],
                include_context_packet=config["include_context_packet"],
                dry_run=config["dry_run"],
                dagster_run_id=context.run_id,
                metadata={"dagster_omics_evidence_packet_run_id": context.run_id},
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_omics_evidence_packet_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "program_id": dg.Field(str, is_required=False),
            "therapy_idea_id": dg.Field(str, is_required=False),
            "packet_id": dg.Field(str, is_required=False),
            "packet_key": dg.Field(str, is_required=False),
            "topic_query": dg.Field(
                str,
                default_value=(
                    "canine hemangiosarcoma human angiosarcoma VIM vimentin "
                    "transcriptome RNA-seq expression"
                ),
            ),
            "disease_terms": dg.Field([str], default_value=[]),
            "gene_symbols": dg.Field([str], default_value=[]),
            "source_keys": dg.Field([str], default_value=["geo", "sra"]),
            "accessions": dg.Field([str], default_value=[]),
            "limit": dg.Field(int, default_value=100),
            "max_datasets": dg.Field(int, default_value=5),
            "matrix_uri_by_accession": dg.Field(dict, default_value={}),
            "sample_group_overrides": dg.Field(dict, default_value={}),
            "artifact_dir": dg.Field(str, is_required=False),
            "run_validation_agent": dg.Field(bool, default_value=False),
            "model_profile": dg.Field(str, default_value="openrouter_required"),
            "dry_run": dg.Field(bool, default_value=False),
        },
    )
    def omics_readout_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual processed-matrix readout compute for VIM and vascular-state programs."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).build_omics_readouts(
            OmicsReadoutRequest(
                packet_id=config.get("packet_id") or None,
                packet_key=config.get("packet_key") or None,
                program_id=UUID(config["program_id"]) if config.get("program_id") else None,
                therapy_idea_id=UUID(config["therapy_idea_id"]) if config.get("therapy_idea_id") else None,
                topic_query=config["topic_query"],
                disease_terms=config.get("disease_terms") or [
                    "canine hemangiosarcoma",
                    "dog hemangiosarcoma",
                    "human angiosarcoma",
                    "angiosarcoma",
                ],
                gene_symbols=config.get("gene_symbols") or ["VIM", "vimentin"],
                source_keys=config.get("source_keys") or ["geo", "sra"],
                accessions=config.get("accessions") or [],
                limit=config["limit"],
                max_datasets=config["max_datasets"],
                matrix_uri_by_accession=config.get("matrix_uri_by_accession") or {},
                sample_group_overrides=config.get("sample_group_overrides") or {},
                artifact_dir=config.get("artifact_dir") or None,
                run_validation_agent=config["run_validation_agent"],
                model_profile=config["model_profile"],
                dry_run=config["dry_run"],
                dagster_run_id=context.run_id,
                metadata={"dagster_omics_readout_run_id": context.run_id},
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_omics_readout_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "packet_id": dg.Field(str, is_required=False),
            "packet_key": dg.Field(str, is_required=False),
            "topic_query": dg.Field(
                str,
                default_value=(
                    "canine hemangiosarcoma human angiosarcoma VIM vimentin "
                    "ChRO-seq bigWig locus signal"
                ),
            ),
            "disease_terms": dg.Field([str], default_value=[]),
            "gene_symbols": dg.Field([str], default_value=["VIM"]),
            "source_keys": dg.Field([str], default_value=["geo"]),
            "accessions": dg.Field([str], default_value=[]),
            "limit": dg.Field(int, default_value=100),
            "max_datasets": dg.Field(int, default_value=5),
            "max_samples_per_group": dg.Field(int, default_value=2),
            "remote_extract_timeout_seconds": dg.Field(int, default_value=600),
            "artifact_dir": dg.Field(str, is_required=False),
            "target_loci": dg.Field(dict, default_value={}),
            "bigwig_uri_by_sample": dg.Field(dict, default_value={}),
            "sample_group_overrides": dg.Field(dict, default_value={}),
            "run_validation_agent": dg.Field(bool, default_value=False),
            "model_profile": dg.Field(str, default_value="openrouter_required"),
            "dry_run": dg.Field(bool, default_value=False),
        },
    )
    def omics_locus_signal_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual VIM locus signal extraction from ChRO-seq bigWig tracks."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).build_omics_locus_signals(
            OmicsLocusSignalRequest(
                packet_id=config.get("packet_id") or None,
                packet_key=config.get("packet_key") or None,
                topic_query=config["topic_query"],
                disease_terms=config.get("disease_terms") or [
                    "canine hemangiosarcoma",
                    "dog hemangiosarcoma",
                    "human angiosarcoma",
                    "angiosarcoma",
                ],
                gene_symbols=config.get("gene_symbols") or ["VIM"],
                source_keys=config.get("source_keys") or ["geo"],
                accessions=config.get("accessions") or [],
                limit=config["limit"],
                max_datasets=config["max_datasets"],
                max_samples_per_group=config["max_samples_per_group"],
                remote_extract_timeout_seconds=config["remote_extract_timeout_seconds"],
                artifact_dir=config.get("artifact_dir") or None,
                target_loci=config.get("target_loci") or {},
                bigwig_uri_by_sample=config.get("bigwig_uri_by_sample") or {},
                sample_group_overrides=config.get("sample_group_overrides") or {},
                run_validation_agent=config["run_validation_agent"],
                model_profile=config["model_profile"],
                dry_run=config["dry_run"],
                dagster_run_id=context.run_id,
                metadata={"dagster_omics_locus_signal_run_id": context.run_id},
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_omics_locus_signal_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "topic_query": dg.Field(
                str,
                default_value="canine hemangiosarcoma human angiosarcoma VIM vimentin omics follow-up evidence",
            ),
            "gene_symbols": dg.Field([str], default_value=["VIM"]),
            "accessions": dg.Field([str], default_value=[]),
            "source_keys": dg.Field([str], default_value=["geo", "sra", "pubmed", "europe_pmc"]),
            "omics_readout_report": dg.Field(dict, default_value={}),
            "omics_locus_signal_report": dg.Field(dict, default_value={}),
            "validation_agent_result": dg.Field(dict, default_value={}),
            "include_locus_signal_report": dg.Field(bool, default_value=False),
            "locus_signal_request": dg.Field(dict, default_value={}),
            "max_tasks": dg.Field(int, default_value=8),
            "create_research_leads": dg.Field(bool, default_value=True),
            "create_source_queries": dg.Field(bool, default_value=True),
            "dry_run": dg.Field(bool, default_value=True),
        },
    )
    def omics_followup_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Generate bounded omics follow-up tasks from null/held readouts."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).build_omics_followups(
            OmicsFollowupRequest(
                topic_query=config["topic_query"],
                gene_symbols=config.get("gene_symbols") or ["VIM"],
                accessions=config.get("accessions") or [],
                source_keys=config.get("source_keys") or ["geo", "sra", "pubmed", "europe_pmc"],
                omics_readout_report=config.get("omics_readout_report") or None,
                omics_locus_signal_report=config.get("omics_locus_signal_report") or None,
                validation_agent_result=config.get("validation_agent_result") or None,
                include_locus_signal_report=config["include_locus_signal_report"],
                locus_signal_request=config.get("locus_signal_request") or {},
                max_tasks=config["max_tasks"],
                create_research_leads=config["create_research_leads"],
                create_source_queries=config["create_source_queries"],
                dry_run=config["dry_run"],
                dagster_run_id=context.run_id,
                metadata={"dagster_omics_followup_run_id": context.run_id},
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_omics_followup_metadata(report))

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "agent_run_id": dg.Field(
                str,
                is_required=False,
                description="Therapy committee agent_run_id. Defaults to latest completed committee run.",
            ),
            "idea_ids": dg.Field(
                [str],
                default_value=[],
                description="Specific therapy idea IDs to queue. Empty selects top-ranked ideas.",
            ),
            "max_ideas": dg.Field(int, default_value=1),
            "priority": dg.Field(int, default_value=40),
            "dry_run": dg.Field(
                bool,
                default_value=True,
                description="When true, preview queue items without persisting.",
            ),
        },
    )
    def therapy_committee_validation_queue_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Queue validation requests from a completed therapy committee run."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).queue_therapy_committee_validation_requests(
            TherapyCommitteeValidationQueueRequest(
                agent_run_id=config.get("agent_run_id"),
                idea_ids=config["idea_ids"],
                max_ideas=config["max_ideas"],
                priority=config["priority"],
                dry_run=config["dry_run"],
                metadata={"dagster_queue_run_id": context.run_id},
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_validation_request_queue_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "pdb_id": dg.Field(
                str,
                default_value="3VHE",
                description="RCSB PDB identifier used to fetch protein PDB text.",
            ),
            "compound_name": dg.Field(
                str,
                default_value="pazopanib",
                description="PubChem compound name used to fetch canonical SMILES.",
            ),
            "target_name": dg.Field(str, default_value="VEGFR2/KDR"),
            "simulation_steps": dg.Field(int, default_value=10),
            "temperature": dg.Field(float, default_value=300.0),
            "enable_docking": dg.Field(
                bool,
                default_value=False,
                description="Enable the worker docking stage for a fresh MD smoke packet.",
            ),
            "priority": dg.Field(int, default_value=40),
            "approve_queue_item": dg.Field(bool, default_value=True),
            "create_compute_job": dg.Field(bool, default_value=True),
            "force_new_compute_job": dg.Field(
                bool,
                default_value=False,
                description=(
                    "Create a fresh compute job for an existing MD smoke queue item instead of reusing "
                    "the latest job for that queue item."
                ),
            ),
            "approved_by": dg.Field(str, default_value="dagster-md-smoke"),
            "approval_note": dg.Field(str, is_required=False),
            "timeout_seconds": dg.Field(int, default_value=45),
        },
    )
    def md_smoke_compute_job_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Seed one real MD validation queue item from RCSB/PubChem APIs."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).seed_md_smoke_compute_job(
            pdb_id=config["pdb_id"],
            compound_name=config["compound_name"],
            target_name=config["target_name"],
            simulation_steps=config["simulation_steps"],
            temperature=config["temperature"],
            enable_docking=config["enable_docking"],
            priority=config["priority"],
            approve_queue_item=config["approve_queue_item"],
            create_compute_job=config["create_compute_job"],
            force_new_compute_job=config["force_new_compute_job"],
            approved_by=config["approved_by"],
            approval_note=config.get("approval_note"),
            timeout_seconds=config["timeout_seconds"],
            dagster_run_id=context.run_id,
        )
        return dg.MaterializeResult(
            value=report,
            metadata=_md_smoke_compute_job_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "compute_job_id": dg.Field(
                str,
                is_required=False,
                description="Optional existing compute job to submit, poll, cancel, or repair.",
            ),
            "queue_item_id": dg.Field(
                str,
                is_required=False,
                description="Optional validation queue item to create/list compute jobs for.",
            ),
            "status": dg.Field(str, is_required=False, description="Optional compute job status filter."),
            "runner_kind": dg.Field(
                str,
                default_value="runpod",
                description="Compute runner kind for new jobs and optional list filter.",
            ),
            "compute_profile": dg.Field(
                str,
                default_value="gpu",
                description="Requested compute profile for new jobs.",
            ),
            "limit": dg.Field(int, default_value=50, description="Maximum compute jobs to list."),
            "create_from_queue_item": dg.Field(
                bool,
                default_value=False,
                description="Create a durable compute job from the selected validation queue item.",
            ),
            "force_new_compute_job": dg.Field(
                bool,
                default_value=False,
                description=(
                    "When creating from a queue item, create a fresh compute job instead of reusing "
                    "the latest job for that queue item."
                ),
            ),
            "submit": dg.Field(
                bool,
                default_value=False,
                description="Attempt submission. Dry-run submission records a handle only.",
            ),
            "poll": dg.Field(
                bool,
                default_value=False,
                description="Poll the created/submitted RunPod job after submission.",
            ),
            "cancel": dg.Field(
                bool,
                default_value=False,
                description="Cancel the created/submitted RunPod job after submission/polling.",
            ),
            "dry_run": dg.Field(
                bool,
                default_value=True,
                description="Keep submission in dry-run mode. Live RunPod submission is blocked until configured.",
            ),
            "recover_runpod_job_id": dg.Field(
                str,
                is_required=False,
                description="Optional RunPod job ID to restore before polling an existing compute job.",
            ),
            "approved_by": dg.Field(str, is_required=False, description="Optional operator approval identity."),
            "approval_note": dg.Field(str, is_required=False, description="Optional operator approval note."),
        },
    )
    def compute_job_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual control-plane view for approval-first CPU/GPU compute jobs."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).build_compute_job_report(
            ComputeJobReportRequest(
                compute_job_id=UUID(config["compute_job_id"]) if config.get("compute_job_id") else None,
                queue_item_id=UUID(config["queue_item_id"]) if config.get("queue_item_id") else None,
                status=config.get("status"),
                runner_kind=config.get("runner_kind") or None,
                compute_profile=config.get("compute_profile") or "gpu",
                limit=config["limit"],
                create_from_queue_item=config["create_from_queue_item"],
                force_new_compute_job=config["force_new_compute_job"],
                submit=config["submit"],
                poll=config["poll"],
                cancel=config["cancel"],
                dry_run=config["dry_run"],
                recover_runpod_job_id=config.get("recover_runpod_job_id"),
                approved_by=config.get("approved_by"),
                approval_note=config.get("approval_note"),
                dagster_run_id=context.run_id,
                metadata={"dagster_run_id": context.run_id},
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_compute_job_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "compute_job_id": dg.Field(str, description="Compute job ID to package for MD expert review."),
            "endpoint_id": dg.Field(str, default_value="cbf4ffekmo36t9"),
            "endpoint_name": dg.Field(str, default_value="hsa-md-validation"),
            "template_name": dg.Field(str, default_value="hsa-md-openmm"),
            "persist": dg.Field(bool, default_value=True),
        },
    )
    def md_expert_review_packet_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Generate the pre-submit MD expert review packet for one compute job."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        packet = HSAResearchService(repository).create_md_expert_review_packet(
            UUID(config["compute_job_id"]),
            endpoint_id=config["endpoint_id"],
            endpoint_name=config["endpoint_name"],
            template_name=config["template_name"],
            persist=config["persist"],
        )
        if packet is None:
            raise RuntimeError(f"Compute job not found: {config['compute_job_id']}")
        report = packet.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_md_expert_review_packet_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "packet_id": dg.Field(str, description="MD expert review packet ID to review."),
            "model_profile": dg.Field(
                str,
                default_value="openrouter_required",
                description="OpenRouter model/profile for the MD expert agent. Use deterministic_only for tests.",
            ),
            "approve_on_agent_approved": dg.Field(
                bool,
                default_value=True,
                description="Persist an approval record when the agent returns approved.",
            ),
            "reviewer_name": dg.Field(str, default_value="md_expert_review_agent"),
            "reviewer_contact": dg.Field(str, default_value="agent://md_expert_review_agent"),
        },
    )
    def md_expert_agent_review_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Run the MD expert agent over a generated review packet."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).run_md_expert_review_agent(
            MDExpertAgentReviewRequest(
                packet_id=UUID(config["packet_id"]),
                model_profile=config["model_profile"],
                approve_on_agent_approved=config["approve_on_agent_approved"],
                reviewer_name=config["reviewer_name"],
                reviewer_contact=config["reviewer_contact"],
                metadata={"dagster_run_id": context.run_id},
            ),
            dagster_run_id=context.run_id,
        )
        if result is None:
            raise RuntimeError(f"MD expert review packet not found: {config['packet_id']}")
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_md_expert_agent_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "enabled": dg.Field(bool, default_value=True, description="Allow live dispatch when dry_run is false."),
            "dry_run": dg.Field(bool, default_value=True, description="Preview selected items without mutating queue state."),
            "force": dg.Field(bool, default_value=False, description="Bypass manual-grace and item-age windows."),
            "manual_grace_period_hours": dg.Field(float, default_value=6.0),
            "minimum_queue_age_hours": dg.Field(float, default_value=1.0),
            "max_per_run": dg.Field(int, default_value=2),
            "hourly_budget_usd": dg.Field(float, default_value=0.25),
            "daily_budget_usd": dg.Field(float, default_value=1.50),
            "estimated_cost_per_item_usd": dg.Field(float, default_value=0.03),
            "allowed_task_types": dg.Field([str], default_value=["expert_review", "target_validation", "omics"]),
            "allowed_validation_types": dg.Field([str], default_value=["expert_review", "homology", "omics"]),
            "source_keys": dg.Field([str], default_value=[]),
            "model_profile": dg.Field(str, default_value="openrouter_required"),
        },
    )
    def validation_autopilot_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual or scheduled validation autopilot run with explicit caps and blockers."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        service = HSAResearchService(repository)
        request = ValidationAutopilotRequest(
            enabled=bool(config.get("enabled", True)),
            dry_run=bool(config.get("dry_run", True)),
            force=bool(config.get("force", False)),
            manual_grace_period_hours=float(config.get("manual_grace_period_hours", 6.0)),
            minimum_queue_age_hours=float(config.get("minimum_queue_age_hours", 1.0)),
            max_per_run=int(config.get("max_per_run", 2)),
            hourly_budget_usd=float(config.get("hourly_budget_usd", 0.25)),
            daily_budget_usd=float(config.get("daily_budget_usd", 1.50)),
            estimated_cost_per_item_usd=float(config.get("estimated_cost_per_item_usd", 0.03)),
            allowed_task_types=config.get("allowed_task_types") or ["expert_review", "target_validation", "omics"],
            allowed_validation_types=config.get("allowed_validation_types") or ["expert_review", "homology", "omics"],
            source_keys=config.get("source_keys") or [],
            model_profile=config.get("model_profile") or "openrouter_required",
            dagster_run_id=context.run_id,
        )
        result = service.run_validation_autopilot(request)
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_validation_autopilot_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "brief_ids": dg.Field(
                [str],
                default_value=[],
                description="Optional specific brief IDs to inspect.",
            ),
            "evaluation_ids": dg.Field(
                [str],
                default_value=[],
                description="Optional specific research brief evaluation IDs to route.",
            ),
            "status": dg.Field(
                str,
                is_required=False,
                description="Optional persisted brief status filter.",
            ),
            "source_key": dg.Field(
                str,
                is_required=False,
                description="Optional source key filter.",
            ),
            "topic_query": dg.Field(
                str,
                is_required=False,
                description="Optional case-insensitive topic/scope search filter.",
            ),
            "limit": dg.Field(
                int,
                default_value=50,
                description="Maximum persisted briefs to show.",
            ),
        },
    )
    def research_brief_library_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Control-panel view of persisted research brief outputs."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        service = HSAResearchService(repository)
        records = service.list_research_briefs(
            status=config.get("status"),
            source_key=config.get("source_key"),
            topic_query=config.get("topic_query"),
            limit=config["limit"],
        )
        rows = [
            {
                "brief_id": str(record.brief_id),
                "agent_run_id": str(record.agent_run_id) if record.agent_run_id else None,
                "status": record.status,
                "topic": record.topic[:300],
                "source_key": record.source_key,
                "brief_style": record.brief_style,
                "model_profile": record.model_profile,
                "finding_count": record.finding_count,
                "citation_count": record.citation_count,
                "research_lead_count": record.research_lead_count,
                "hard_error_count": record.hard_error_count,
                "evidence_limitation_count": record.evidence_limitation_count,
                "error_count": record.error_count,
                "created_at": record.created_at.isoformat(),
            }
            for record in records
        ]
        report = {
            "brief_count": len(records),
            "status": config.get("status"),
            "source_key": config.get("source_key"),
            "topic_query": config.get("topic_query"),
            "briefs": rows,
        }
        return dg.MaterializeResult(
            value=report,
            metadata=_research_brief_library_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "brief_id": dg.Field(
                str,
                is_required=False,
                description="Specific persisted brief ID to evaluate.",
            ),
            "topic_query": dg.Field(
                str,
                is_required=False,
                description="Latest completed brief topic/scope search when brief_id is omitted.",
            ),
            "source_key": dg.Field(
                str,
                is_required=False,
                description="Optional source key filter when brief_id is omitted.",
            ),
            "limit": dg.Field(
                int,
                default_value=1,
                description="Candidate brief search limit when brief_id is omitted.",
            ),
            "minimum_overall_score": dg.Field(
                float,
                default_value=0.7,
                description="Minimum weighted score required to pass the synthesis quality bar.",
            ),
            "review_mode": dg.Field(
                str,
                default_value="openrouter_required",
                description="Evaluation review mode: openrouter_required, openrouter_compare, or deterministic_only.",
            ),
            "review_models": dg.Field(
                [str],
                default_value=[],
                description="OpenRouter model ids to use for live synthesis-quality evaluation.",
            ),
            "model_profile": dg.Field(
                str,
                default_value="synthesis_quality_evaluator",
                description="Logical model/profile label recorded in the agent ledger.",
            ),
        },
    )
    def research_brief_evaluation_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual synthesis-quality evaluation for a persisted research brief."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).evaluate_research_brief(
            ResearchBriefEvaluationRequest(
                brief_id=UUID(config["brief_id"]) if config.get("brief_id") else None,
                topic_query=config.get("topic_query"),
                source_key=config.get("source_key"),
                limit=config["limit"],
                minimum_overall_score=config["minimum_overall_score"],
                model_profile=config.get("model_profile") or "synthesis_quality_evaluator",
                review_mode=config.get("review_mode") or "openrouter_required",
                review_models=config.get("review_models") or [],
                dagster_run_id=context.run_id,
            )
        )
        report = result.model_dump(mode="json")
        report["scores"] = {
            "citation_coverage": report.get("citation_coverage_score"),
            "perspective_balance": report.get("perspective_balance_score"),
            "contradiction_handling": report.get("contradiction_handling_score"),
            "novelty": report.get("novelty_score"),
            "actionability": report.get("actionability_score"),
            "weakness_transparency": report.get("weakness_transparency_score"),
        }
        return dg.MaterializeResult(
            value=report,
            metadata=_research_brief_evaluation_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "brief_id": dg.Field(str, is_required=False, description="Optional brief ID filter."),
            "readiness": dg.Field(str, is_required=False, description="Optional readiness filter."),
            "passes_quality_bar": dg.Field(
                bool,
                is_required=False,
                description="Optional quality-bar pass/fail filter.",
            ),
            "limit": dg.Field(
                int,
                default_value=50,
                description="Maximum persisted evaluations to show.",
            ),
        },
    )
    def research_brief_evaluation_library_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Control-panel view of persisted research brief synthesis evaluations."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        service = HSAResearchService(repository)
        evaluations = service.list_research_brief_evaluations(
            brief_id=UUID(config["brief_id"]) if config.get("brief_id") else None,
            readiness=config.get("readiness"),
            passes_quality_bar=config.get("passes_quality_bar"),
            limit=config["limit"],
        )
        rows = [
            {
                "evaluation_id": str(record.evaluation_id),
                "brief_id": str(record.brief_id),
                "agent_run_id": str(record.agent_run_id) if record.agent_run_id else None,
                "topic": record.topic[:300],
                "source_key": record.source_key,
                "overall_score": record.overall_score,
                "passes_quality_bar": record.passes_quality_bar,
                "readiness": record.readiness,
                "recommendation_count": len((record.result_payload or {}).get("recommendations", [])),
                "error_count": len(record.errors),
                "created_at": record.created_at.isoformat(),
            }
            for record in evaluations
        ]
        report = {
            "evaluation_count": len(evaluations),
            "brief_id": config.get("brief_id"),
            "readiness": config.get("readiness"),
            "passes_quality_bar": config.get("passes_quality_bar"),
            "evaluations": rows,
        }
        return dg.MaterializeResult(
            value=report,
            metadata=_research_brief_evaluation_library_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "status": dg.Field(
                str,
                is_required=False,
                description="Optional persisted brief status filter.",
            ),
            "source_key": dg.Field(
                str,
                is_required=False,
                description="Optional source key filter.",
            ),
            "topic_query": dg.Field(
                str,
                is_required=False,
                description="Optional case-insensitive topic/scope search filter.",
            ),
            "limit": dg.Field(
                int,
                default_value=50,
                description="Maximum persisted briefs to score.",
            ),
            "include_evaluations": dg.Field(
                bool,
                default_value=True,
                description="Join each brief to its latest persisted synthesis evaluation.",
            ),
        },
    )
    def research_brief_quality_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Control-panel quality view for persisted research briefs."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).build_research_brief_quality_report(
            ResearchBriefQualityReportRequest(
                status=config.get("status"),
                source_key=config.get("source_key"),
                topic_query=config.get("topic_query"),
                limit=config["limit"],
                include_evaluations=config["include_evaluations"],
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_research_brief_quality_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "status": dg.Field(
                str,
                is_required=False,
                description="Optional persisted brief status filter.",
            ),
            "source_key": dg.Field(
                str,
                is_required=False,
                description="Optional source key filter.",
            ),
            "topic_query": dg.Field(
                str,
                is_required=False,
                description="Optional case-insensitive topic/scope search filter.",
            ),
            "limit": dg.Field(
                int,
                default_value=50,
                description="Maximum persisted briefs to inspect.",
            ),
            "include_evaluations": dg.Field(
                bool,
                default_value=True,
                description="Join each brief to its latest persisted synthesis evaluation.",
            ),
            "max_limitations_per_brief": dg.Field(
                int,
                default_value=20,
                description="Maximum evidence limitations to queue per brief.",
            ),
            "dry_run": dg.Field(
                bool,
                default_value=False,
                description="Preview follow-up leads without writing them.",
            ),
        },
    )
    def research_brief_followup_queue_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Queue evidence-limited research briefs into the follow-up research lane."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        report = HSAResearchService(repository).queue_research_brief_followups(
            ResearchBriefFollowupQueueRequest(
                brief_ids=[UUID(value) for value in config["brief_ids"]],
                evaluation_ids=[UUID(value) for value in config["evaluation_ids"]],
                status=config.get("status"),
                source_key=config.get("source_key"),
                topic_query=config.get("topic_query"),
                limit=config["limit"],
                include_evaluations=config["include_evaluations"],
                max_limitations_per_brief=config["max_limitations_per_brief"],
                dry_run=config["dry_run"],
            )
        ).model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_research_brief_followup_queue_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "brief_id": dg.Field(
                str,
                is_required=False,
                description="Specific persisted brief ID to plan from.",
            ),
            "evaluation_id": dg.Field(
                str,
                is_required=False,
                description="Specific persisted evaluation ID to plan from.",
            ),
            "topic_query": dg.Field(
                str,
                is_required=False,
                description="Latest completed brief topic/scope search when IDs are omitted.",
            ),
            "source_key": dg.Field(
                str,
                is_required=False,
                description="Optional source key filter when IDs are omitted.",
            ),
            "require_ready_evaluation": dg.Field(
                bool,
                default_value=True,
                description="Require a passing synthesis evaluation before proposing validation tasks.",
            ),
            "max_tasks": dg.Field(
                int,
                default_value=8,
                description="Maximum validation tasks to propose.",
            ),
        },
    )
    def validation_plan_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual recommend-only validation plan from a persisted research brief."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).plan_validation(
            ValidationPlanRequest(
                brief_id=UUID(config["brief_id"]) if config.get("brief_id") else None,
                evaluation_id=UUID(config["evaluation_id"]) if config.get("evaluation_id") else None,
                topic_query=config.get("topic_query"),
                source_key=config.get("source_key"),
                require_ready_evaluation=config["require_ready_evaluation"],
                max_tasks=config["max_tasks"],
                dagster_run_id=context.run_id,
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_validation_plan_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "brief_id": dg.Field(str, is_required=False, description="Optional brief ID filter."),
            "evaluation_id": dg.Field(str, is_required=False, description="Optional evaluation ID filter."),
            "status": dg.Field(str, is_required=False, description="Optional plan status filter."),
            "readiness": dg.Field(str, is_required=False, description="Optional readiness filter."),
            "limit": dg.Field(
                int,
                default_value=50,
                description="Maximum persisted validation plans to show.",
            ),
        },
    )
    def validation_plan_library_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Control-panel view of persisted recommend-only validation plans."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        service = HSAResearchService(repository)
        plans = service.list_validation_plans(
            brief_id=UUID(config["brief_id"]) if config.get("brief_id") else None,
            evaluation_id=UUID(config["evaluation_id"]) if config.get("evaluation_id") else None,
            status=config.get("status"),
            readiness=config.get("readiness"),
            limit=config["limit"],
        )
        rows = []
        for record in plans:
            task_titles, validation_types = _validation_plan_task_summary(record.model_dump(mode="json"))
            rows.append(
                {
                    "plan_id": str(record.plan_id),
                    "brief_id": str(record.brief_id),
                    "evaluation_id": str(record.evaluation_id) if record.evaluation_id else None,
                    "agent_run_id": str(record.agent_run_id) if record.agent_run_id else None,
                    "topic": record.topic[:300],
                    "source_key": record.source_key,
                    "status": record.status,
                    "readiness": record.readiness,
                    "task_count": record.task_count,
                    "hypothesis_count": record.hypothesis_count,
                    "task_titles": task_titles,
                    "validation_types": validation_types,
                    "created_at": record.created_at.isoformat(),
                }
            )
        report = {
            "plan_count": len(plans),
            "brief_id": config.get("brief_id"),
            "evaluation_id": config.get("evaluation_id"),
            "status": config.get("status"),
            "readiness": config.get("readiness"),
            "plans": rows,
        }
        return dg.MaterializeResult(
            value=report,
            metadata=_validation_plan_library_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "plan_id": dg.Field(str, description="Ready validation plan to queue requests from."),
            "task_ids": dg.Field(
                [str],
                default_value=[],
                description="Optional validation plan task IDs to queue.",
            ),
            "dry_run": dg.Field(
                bool,
                default_value=True,
                description="Preview queue items without persisting them.",
            ),
        },
    )
    def validation_request_queue_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Queue validation requests from a ready plan, or preview them as a dry run."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).queue_validation_requests_from_plan(
            ValidationRequestQueueRequest(
                plan_id=UUID(config["plan_id"]),
                task_ids=[UUID(value) for value in config["task_ids"]],
                dry_run=config["dry_run"],
                metadata={"dagster_run_id": context.run_id},
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_validation_request_queue_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "plan_id": dg.Field(str, is_required=False, description="Optional validation plan filter."),
            "status": dg.Field(str, is_required=False, description="Optional queue status filter."),
            "statuses": dg.Field([str], default_value=[], description="Optional queue statuses filter."),
            "source_key": dg.Field(str, is_required=False, description="Optional source key filter."),
            "task_type": dg.Field(str, is_required=False, description="Optional validation plan task type filter."),
            "topic_query": dg.Field(str, is_required=False, description="Optional topic/task search filter."),
            "limit": dg.Field(
                int,
                default_value=50,
                description="Maximum persisted validation request queue items to show.",
            ),
        },
    )
    def validation_request_queue_library_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Control-panel view of queued validation requests."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        service = HSAResearchService(repository)
        items = service.list_validation_request_queue_items(
            plan_id=UUID(config["plan_id"]) if config.get("plan_id") else None,
            status=config.get("status"),
            statuses=config.get("statuses") or None,
            source_key=config.get("source_key"),
            task_type=config.get("task_type"),
            topic_query=config.get("topic_query"),
            limit=config["limit"],
        )
        report = {
            "queue_item_count": len(items),
            "plan_id": config.get("plan_id"),
            "status": config.get("status"),
            "statuses": config.get("statuses") or [],
            "source_key": config.get("source_key"),
            "task_type": config.get("task_type"),
            "topic_query": config.get("topic_query"),
            "queue_items": [item.model_dump(mode="json") for item in items],
            "errors": [],
            "skipped": [],
        }
        return dg.MaterializeResult(
            value=report,
            metadata=_validation_request_queue_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "status": dg.Field(
                str,
                is_required=False,
                description="Optional queue status filter.",
            ),
            "source_key": dg.Field(
                str,
                is_required=False,
                description="Optional source key filter.",
            ),
            "topic_query": dg.Field(
                str,
                is_required=False,
                description="Optional case-insensitive topic/scope search filter.",
            ),
            "limit": dg.Field(
                int,
                default_value=50,
                description="Maximum queued brief requests to show.",
            ),
        },
    )
    def research_brief_queue_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Control-panel view of queued research brief requests."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        service = HSAResearchService(repository)
        items = service.list_research_brief_queue_items(
            status=config.get("status"),
            source_key=config.get("source_key"),
            topic_query=config.get("topic_query"),
            limit=config["limit"],
        )
        rows = [
            {
                "queue_item_id": str(item.queue_item_id),
                "status": item.status,
                "priority": item.priority,
                "topic": item.topic[:300],
                "source_key": item.source_key,
                "brief_style": item.brief_style,
                "model_profile": item.model_profile,
                "review_mode": item.review_mode,
                "attempts": item.attempts,
                "last_brief_id": str(item.last_brief_id) if item.last_brief_id else None,
                "last_error": str(item.last_error or "")[:300],
                "created_at": item.created_at.isoformat(),
            }
            for item in items
        ]
        report = {
            "queue_item_count": len(items),
            "status": config.get("status"),
            "statuses": [config["status"]] if config.get("status") else [],
            "source_key": config.get("source_key"),
            "topic_query": config.get("topic_query"),
            "queue_items": rows,
        }
        return dg.MaterializeResult(
            value=report,
            metadata=_research_brief_queue_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "mode": dg.Field(
                str,
                default_value="both",
                description="Batch source: research_leads, source_health, or both.",
            ),
            "lead_statuses": dg.Field(
                [str],
                default_value=["new", "watching"],
                description="Research lead statuses to convert into brief queue items.",
            ),
            "lead_types": dg.Field(
                [str],
                default_value=[],
                description="Optional research lead type filters.",
            ),
            "source_keys": dg.Field(
                [str],
                default_value=[],
                description="Optional source keys for source-health gap queueing.",
            ),
            "source_health_statuses": dg.Field(
                [str],
                default_value=["failing", "triage", "watch"],
                description="Source health statuses eligible for batch queueing.",
            ),
            "include_empty_sources": dg.Field(
                bool,
                default_value=False,
                description="Queue source-health gaps even when a source has no chunks yet.",
            ),
            "limit": dg.Field(int, default_value=25, description="Maximum queue items to create."),
            "disease_scope": dg.Field(
                str,
                default_value="canine hemangiosarcoma and human angiosarcoma",
                description="Disease/scope guardrail for retrieval and synthesis.",
            ),
            "priority": dg.Field(int, default_value=80, description="Default queue priority."),
            "max_chunks_per_perspective": dg.Field(
                int,
                default_value=8,
                description="Maximum chunks to retrieve per perspective query.",
            ),
            "max_claims": dg.Field(int, default_value=12, description="Maximum claims to include."),
            "max_chunk_chars": dg.Field(
                int,
                default_value=1800,
                description="Maximum characters per cited chunk sent to the reviewer.",
            ),
            "brief_style": dg.Field(str, default_value="technical", description="Research brief style."),
            "model_profile": dg.Field(str, default_value="research_brief", description="Logical model profile."),
            "review_mode": dg.Field(str, default_value="openrouter_required", description="Review mode."),
            "review_models": dg.Field(
                [str],
                default_value=[],
                description="OpenRouter model ids for OpenRouter-backed review modes.",
            ),
        },
    )
    def research_brief_queue_batch_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Queue research brief batches from watchlist leads and source-health gaps."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).queue_research_brief_batch(
            ResearchBriefQueueBatchRequest(
                mode=config["mode"],
                lead_statuses=config["lead_statuses"],
                lead_types=config["lead_types"],
                source_keys=config["source_keys"],
                source_health_statuses=config["source_health_statuses"],
                include_empty_sources=config["include_empty_sources"],
                limit=config["limit"],
                disease_scope=config["disease_scope"],
                priority=config["priority"],
                max_chunks_per_perspective=config["max_chunks_per_perspective"],
                max_claims=config["max_claims"],
                max_chunk_chars=config["max_chunk_chars"],
                brief_style=config["brief_style"],
                model_profile=config["model_profile"],
                review_mode=config["review_mode"],
                review_models=config["review_models"],
                metadata={"dagster_batch_run_id": context.run_id},
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_research_brief_queue_batch_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "topic": dg.Field(
                str,
                default_value="canine hemangiosarcoma translational therapy",
                description="Research topic or question to queue.",
            ),
            "disease_scope": dg.Field(
                str,
                default_value="canine hemangiosarcoma and human angiosarcoma",
                description="Disease/scope guardrail for retrieval and synthesis.",
            ),
            "source_key": dg.Field(
                str,
                is_required=False,
                description="Optional source key filter.",
            ),
            "priority": dg.Field(
                int,
                default_value=100,
                description="Lower values run first.",
            ),
            "max_chunks_per_perspective": dg.Field(
                int,
                default_value=8,
                description="Maximum chunks to retrieve per perspective query.",
            ),
            "max_claims": dg.Field(
                int,
                default_value=12,
                description="Maximum stored claims to include in the evidence payload.",
            ),
            "max_chunk_chars": dg.Field(
                int,
                default_value=1800,
                description="Maximum characters per cited chunk sent to the reviewer.",
            ),
            "brief_style": dg.Field(
                str,
                default_value="technical",
                description="Brief style: technical, operator, substack, or vet_partner.",
            ),
            "review_mode": dg.Field(
                str,
                default_value="openrouter_required",
                description="Research brief review mode.",
            ),
            "review_models": dg.Field(
                [str],
                default_value=[],
                description="OpenRouter model ids to use when using an OpenRouter review mode.",
            ),
        },
    )
    def research_brief_queue_seed_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manually queue a research brief request from Dagster."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        item = HSAResearchService(repository).queue_research_brief(
            ResearchBriefQueueRequest(
                topic=config["topic"],
                disease_scope=config["disease_scope"],
                source_key=config.get("source_key"),
                priority=config["priority"],
                max_chunks_per_perspective=config["max_chunks_per_perspective"],
                max_claims=config["max_claims"],
                max_chunk_chars=config["max_chunk_chars"],
                brief_style=config["brief_style"],
                review_mode=config["review_mode"],
                review_models=config["review_models"],
                metadata={"dagster_seed_run_id": context.run_id},
            )
        )
        row = {
            "queue_item_id": str(item.queue_item_id),
            "status": item.status,
            "priority": item.priority,
            "topic": item.topic[:300],
            "source_key": item.source_key,
            "brief_style": item.brief_style,
            "model_profile": item.model_profile,
            "review_mode": item.review_mode,
            "attempts": item.attempts,
            "last_brief_id": str(item.last_brief_id) if item.last_brief_id else None,
            "last_error": str(item.last_error or "")[:300],
            "created_at": item.created_at.isoformat(),
        }
        report = {
            "queue_item_count": 1,
            "status": item.status,
            "statuses": [item.status],
            "source_key": item.source_key,
            "topic_query": item.topic,
            "queue_items": [row],
        }
        return dg.MaterializeResult(
            value=item.model_dump(mode="json"),
            metadata={
                **_research_brief_queue_metadata(report),
                "queue_item_id": str(item.queue_item_id),
                "identity_key": item.identity_key,
            },
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "queue_item_ids": dg.Field(
                [str],
                default_value=[],
                description="Specific research brief queue item IDs to run.",
            ),
            "statuses": dg.Field(
                [str],
                default_value=["queued"],
                description="Queue statuses eligible for execution.",
            ),
            "source_key": dg.Field(
                str,
                is_required=False,
                description="Optional source key filter.",
            ),
            "topic_query": dg.Field(
                str,
                is_required=False,
                description="Optional case-insensitive topic/scope search filter.",
            ),
            "limit": dg.Field(
                int,
                default_value=1,
                description="Candidate queued brief requests to inspect.",
            ),
            "max_runs": dg.Field(
                int,
                default_value=1,
                description="Maximum queue items to execute in this materialization.",
            ),
        },
    )
    def research_brief_queue_runner_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Run the next queued research brief request and persist the output."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        service = HSAResearchService(repository)
        queue_item_ids = [UUID(value) for value in config["queue_item_ids"]]
        runs = []
        for _ in range(max(int(config["max_runs"]), 0)):
            result = service.run_next_research_brief_queue_item(
                ResearchBriefQueueRunRequest(
                    queue_item_ids=queue_item_ids,
                    statuses=config["statuses"],
                    source_key=config.get("source_key"),
                    topic_query=config.get("topic_query"),
                    limit=config["limit"],
                    dagster_run_id=context.run_id,
                )
            )
            run_report = result.model_dump(mode="json")
            runs.append(run_report)
            if not result.ran:
                break
        errors = [error for run in runs for error in run.get("errors", [])]
        completed_count = sum(
            1 for run in runs if (run.get("queue_item") or {}).get("status") == "completed"
        )
        failed_count = sum(1 for run in runs if (run.get("queue_item") or {}).get("status") == "failed")
        report = {
            "runs": runs,
            "ran_count": sum(1 for run in runs if run.get("ran")),
            "completed_count": completed_count,
            "failed_count": failed_count,
            "requested_max_runs": int(config["max_runs"]),
            "queue_item_ids": [str(value) for value in queue_item_ids],
            "statuses": config["statuses"],
            "source_key": config.get("source_key"),
            "topic_query": config.get("topic_query"),
            "errors": errors,
        }
        return dg.MaterializeResult(
            value=report,
            metadata=_research_brief_queue_runner_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "queue_item_ids": dg.Field(
                [str],
                default_value=[],
                description="Specific queue item ids to inspect; leave empty to use filters.",
            ),
            "statuses": dg.Field(
                [str],
                default_value=["failed"],
                description="Eligible statuses. Only failed/completed are supported.",
            ),
            "source_key": dg.Field(
                str,
                is_required=False,
                description="Optional source key filter.",
            ),
            "topic_query": dg.Field(
                str,
                is_required=False,
                description="Optional case-insensitive topic/scope filter.",
            ),
            "min_attempts": dg.Field(
                int,
                default_value=1,
                description="Minimum attempts before a queue item is eligible.",
            ),
            "max_updated_age_hours": dg.Field(
                float,
                default_value=12.0,
                description="Only include items whose updated_at is at least this many hours old.",
            ),
            "limit": dg.Field(
                int,
                default_value=50,
                description="Maximum eligible queue items to archive.",
            ),
            "dry_run": dg.Field(
                bool,
                default_value=True,
                description="Preview matching items without archiving them.",
            ),
            "reason": dg.Field(
                str,
                default_value="stale_research_brief_queue_cleanup",
                description="Reason recorded in queue metadata when archiving.",
            ),
        },
    )
    def research_brief_queue_maintenance_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Dry-run or apply safe research brief queue maintenance."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).maintain_research_brief_queue(
            ResearchBriefQueueMaintenanceRequest(
                queue_item_ids=[UUID(value) for value in config["queue_item_ids"]],
                statuses=config["statuses"],
                source_key=config.get("source_key"),
                topic_query=config.get("topic_query"),
                min_attempts=config["min_attempts"],
                max_updated_age_hours=config["max_updated_age_hours"],
                limit=config["limit"],
                dry_run=config["dry_run"],
                reason=config["reason"],
                dagster_run_id=context.run_id,
                metadata={"dagster_maintenance_run_id": context.run_id},
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_research_brief_queue_maintenance_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "topic": dg.Field(
                str,
                default_value="canine hemangiosarcoma translational therapy",
                description="Research topic or question to brief.",
            ),
            "disease_scope": dg.Field(
                str,
                default_value="canine hemangiosarcoma and human angiosarcoma",
                description="Disease/scope guardrail for retrieval and synthesis.",
            ),
            "source_key": dg.Field(
                str,
                is_required=False,
                description="Optional source key filter for chunk retrieval.",
            ),
            "max_chunks_per_perspective": dg.Field(
                int,
                default_value=8,
                description="Maximum chunks to retrieve per perspective query.",
            ),
            "max_claims": dg.Field(
                int,
                default_value=12,
                description="Maximum stored claims to include in the evidence payload.",
            ),
            "max_chunk_chars": dg.Field(
                int,
                default_value=1800,
                description="Maximum characters per cited chunk sent to the reviewer.",
            ),
            "brief_style": dg.Field(
                str,
                default_value="technical",
                description="Brief style: technical, operator, substack, or vet_partner.",
            ),
        },
    )
    def research_brief_playground_pack_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual prompt pack for running research brief agents in a model playground."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).build_research_brief_playground_pack(
            ResearchBriefRequest(
                topic=config["topic"],
                disease_scope=config["disease_scope"],
                source_key=config.get("source_key"),
                max_chunks_per_perspective=config["max_chunks_per_perspective"],
                max_claims=config["max_claims"],
                max_chunk_chars=config["max_chunk_chars"],
                brief_style=config["brief_style"],
                review_mode="external_required",
                dagster_run_id=context.run_id,
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_research_brief_playground_pack_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "agent_names": dg.Field(
                [str],
                default_value=["x_linked_article_review_agent", "x_topic_review_agent"],
                description="Agent names to scan for non-ingestible research leads.",
            ),
            "statuses": dg.Field(
                [str],
                default_value=["completed"],
                description="Agent run statuses to scan.",
            ),
            "limit": dg.Field(
                int,
                default_value=50,
                description="Maximum recent runs to scan per agent/status.",
            ),
            "include_existing": dg.Field(
                bool,
                default_value=False,
                description="Include already persisted leads in the materialization output.",
            ),
        },
    )
    def research_leads_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Collect watchlist leads from review-agent runs for future synthesis."""

        from .service import HSAResearchService

        config = context.op_config or {}
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).collect_research_leads(
            ResearchLeadCollectRequest(
                agent_names=config.get("agent_names") or ["x_linked_article_review_agent", "x_topic_review_agent"],
                statuses=config.get("statuses") or ["completed"],
                limit=int(config.get("limit") or 50),
                include_existing=bool(config.get("include_existing", False)),
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_research_leads_report_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "queue_item_ids": dg.Field([str], default_value=[], description="Specific validation queue item IDs."),
            "plan_id": dg.Field(str, default_value="", description="Optional validation plan ID."),
            "statuses": dg.Field([str], default_value=["completed"], description="Validation queue statuses to inspect."),
            "decisions": dg.Field([str], default_value=["hold", "demote"], description="Validation decisions to resolve."),
            "task_types": dg.Field([str], default_value=[], description="Optional validation task-type filters."),
            "gap_types": dg.Field(
                [str],
                default_value=["missing_evidence", "risk", "next_action"],
                description="Gap types to convert into research leads.",
            ),
            "limit": dg.Field(int, default_value=25, description="Maximum validation queue items to scan."),
            "max_gaps_per_item": dg.Field(int, default_value=8, description="Maximum gaps to convert per item."),
            "priority": dg.Field(int, default_value=30, description="Default priority cap for generated leads."),
            "dry_run": dg.Field(bool, default_value=True, description="Preview leads without persisting them."),
            "queue_research_briefs": dg.Field(
                bool,
                default_value=False,
                description="Also queue research briefs for generated leads when dry_run is false.",
            ),
        },
    )
    def evidence_gap_resolver_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Convert validation-agent evidence gaps into durable research leads."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config or {}
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).resolve_evidence_gaps(
            EvidenceGapResolverRequest(
                queue_item_ids=[UUID(value) for value in config["queue_item_ids"]],
                plan_id=UUID(config["plan_id"]) if config.get("plan_id") else None,
                statuses=config.get("statuses") or ["completed"],
                decisions=config.get("decisions") or ["hold", "demote"],
                task_types=config.get("task_types") or [],
                gap_types=config.get("gap_types") or ["missing_evidence", "risk", "next_action"],
                limit=int(config.get("limit") or 25),
                max_gaps_per_item=int(config.get("max_gaps_per_item") or 8),
                priority=int(config.get("priority") or 30),
                dry_run=bool(config.get("dry_run", True)),
                queue_research_briefs=bool(config.get("queue_research_briefs", False)),
                dagster_run_id=context.run_id,
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_evidence_gap_resolver_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "queue_item_ids": dg.Field(
                [str],
                default_value=[],
                description="Specific validation queue item IDs to convert into source queries.",
            ),
            "lead_ids": dg.Field(
                [str],
                default_value=[],
                description="Specific validation-gap research lead IDs to convert into source queries.",
            ),
            "lead_statuses": dg.Field(
                [str],
                default_value=["new", "followup"],
                description="Lead statuses to scan when no lead IDs are supplied.",
            ),
            "source_keys": dg.Field([str], default_value=[], description="Optional source keys to target."),
            "lanes": dg.Field([str], default_value=[], description="Optional evidence lanes to include."),
            "limit": dg.Field(int, default_value=25, description="Maximum validation-gap leads/items to inspect."),
            "max_queries_per_lane": dg.Field(int, default_value=3, description="Maximum generated queries per lane."),
            "persist_queries": dg.Field(bool, default_value=False, description="Persist generated SourceQuery rows."),
            "active": dg.Field(bool, default_value=True, description="Persist generated SourceQuery rows as active."),
            "dry_run": dg.Field(bool, default_value=True, description="Preview source queries without persistence."),
        },
    )
    def validation_gap_source_pack_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Build targeted source-query packs for validation evidence gaps."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config or {}
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).build_validation_gap_source_pack(
            ValidationGapSourcePackRequest(
                queue_item_ids=[UUID(value) for value in config["queue_item_ids"]],
                lead_ids=[UUID(value) for value in config["lead_ids"]],
                lead_statuses=config["lead_statuses"],
                source_keys=config["source_keys"],
                lanes=config["lanes"],
                limit=int(config["limit"]),
                max_queries_per_lane=int(config["max_queries_per_lane"]),
                persist_queries=bool(config["persist_queries"]),
                active=bool(config["active"]),
                dry_run=bool(config["dry_run"]),
                dagster_run_id=context.run_id,
                metadata={"dagster_asset": "validation_gap_source_pack_report"},
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_validation_gap_source_pack_metadata(report),
        )

    @dg.asset(
        group_name="ingestion_maintenance",
        config_schema={
            "pmids": dg.Field([str], default_value=[], description="Optional PMIDs to repair."),
            "limit": dg.Field(int, default_value=250, description="Maximum PubMed objects to inspect."),
            "batch_size": dg.Field(int, default_value=100, description="PubMed EFetch batch size."),
            "dry_run": dg.Field(bool, default_value=True, description="Preview repairs without updating rows."),
        },
    )
    def pubmed_identifier_repair_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Repair PubMed DOI/PMCID payloads and PubMed dedupe keys."""

        from .service import HSAResearchService

        config = context.op_config or {}
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).repair_pubmed_identifiers(
            PubMedIdentifierRepairRequest(
                pmids=config["pmids"],
                limit=int(config["limit"]),
                batch_size=int(config["batch_size"]),
                dry_run=bool(config["dry_run"]),
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_pubmed_identifier_repair_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "source_keys": dg.Field([str], default_value=[], description="Optional validation-gap source keys."),
            "query_names": dg.Field([str], default_value=[], description="Optional validation-gap query names."),
            "tracks": dg.Field([str], default_value=["validation_gap"], description="SourceQuery tracks to ingest."),
            "followup_lane": dg.Field(str, default_value="", description="Optional internal follow-up lane filter."),
            "origin_review_ids": dg.Field([str], default_value=[], description="Optional origin evaluator review IDs."),
            "origin_agent_run_ids": dg.Field([str], default_value=[], description="Optional origin agent run IDs."),
            "limit_per_query": dg.Field(int, default_value=5, description="Maximum records per query."),
            "max_queries": dg.Field(int, default_value=50, description="Maximum validation-gap queries to run."),
            "dry_run": dg.Field(bool, default_value=True, description="Preview selected queries without API calls."),
        },
    )
    def validation_gap_source_ingest_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Ingest only active validation-gap source queries."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config or {}
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).ingest_validation_gap_source_queries(
            ValidationGapSourceIngestRequest(
                source_keys=config["source_keys"],
                query_names=config["query_names"],
                tracks=config.get("tracks") or ["validation_gap"],
                followup_lane=config.get("followup_lane") or None,
                origin_review_ids=[UUID(value) for value in config.get("origin_review_ids", [])],
                origin_agent_run_ids=[UUID(value) for value in config.get("origin_agent_run_ids", [])],
                limit_per_query=int(config["limit_per_query"]),
                max_queries=int(config["max_queries"]),
                dry_run=bool(config["dry_run"]),
                dagster_run_id=context.run_id,
                metadata={"dagster_asset": "validation_gap_source_ingest_report"},
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_validation_gap_source_ingest_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "queue_item_ids": dg.Field(
                [str],
                default_value=[],
                description="Specific validation queue item IDs to complete.",
            ),
            "statuses": dg.Field(
                [str],
                default_value=["completed"],
                description="Validation queue statuses eligible for gap resolution.",
            ),
            "decisions": dg.Field(
                [str],
                default_value=["hold", "demote"],
                description="Validation decisions to convert into follow-up work.",
            ),
            "task_types": dg.Field([str], default_value=[], description="Optional validation task-type filters."),
            "gap_types": dg.Field(
                [str],
                default_value=["missing_evidence", "next_action"],
                description="Gap types to resolve. Risks stay human-facing by default.",
            ),
            "source_keys": dg.Field(
                [str],
                default_value=[
                    "pubmed",
                    "europe_pmc",
                    "pmc_oa",
                    "openalex",
                    "clinicaltrials_gov",
                    "chembl",
                    "openfda_animal_events",
                ],
                description="Sources to target for generated validation-gap searches.",
            ),
            "lanes": dg.Field([str], default_value=[], description="Optional evidence lanes to include."),
            "limit": dg.Field(int, default_value=10, description="Maximum validation items/leads to scan."),
            "max_gaps_per_item": dg.Field(int, default_value=5, description="Maximum gaps to convert per item."),
            "priority": dg.Field(int, default_value=30, description="Default priority cap for generated leads."),
            "max_queries_per_lane": dg.Field(int, default_value=2, description="Maximum queries per evidence lane."),
            "limit_per_query": dg.Field(int, default_value=3, description="Maximum records to ingest per query."),
            "max_ingest_queries": dg.Field(int, default_value=20, description="Maximum generated queries to ingest."),
            "queue_research_briefs": dg.Field(
                bool,
                default_value=True,
                description="Queue research briefs for generated evidence-gap leads.",
            ),
            "brief_runs": dg.Field(
                int,
                default_value=2,
                description="Maximum generated research brief queue items to execute after ingest.",
            ),
            "dry_run": dg.Field(bool, default_value=True, description="Preview without writing or API ingestion."),
        },
    )
    def validation_gap_completion_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Resolve validation gaps, ingest targeted source evidence, and run bounded follow-up briefs."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config or {}
        dry_run = bool(config.get("dry_run", True))
        repository = research_repository.build_repository()
        service = HSAResearchService(repository)
        queue_item_ids = [UUID(value) for value in config["queue_item_ids"]]

        gap_result = service.resolve_evidence_gaps(
            EvidenceGapResolverRequest(
                queue_item_ids=queue_item_ids,
                statuses=config.get("statuses") or ["completed"],
                decisions=config.get("decisions") or ["hold", "demote"],
                task_types=config.get("task_types") or [],
                gap_types=config.get("gap_types") or ["missing_evidence", "next_action"],
                limit=int(config.get("limit") or 10),
                max_gaps_per_item=int(config.get("max_gaps_per_item") or 5),
                priority=int(config.get("priority") or 30),
                dry_run=dry_run,
                queue_research_briefs=bool(config.get("queue_research_briefs", True)),
                dagster_run_id=context.run_id,
                metadata={"dagster_asset": "validation_gap_completion_report"},
            )
        )

        lead_ids = [lead.lead_id for lead in gap_result.research_leads]
        source_pack_result = service.build_validation_gap_source_pack(
            ValidationGapSourcePackRequest(
                queue_item_ids=queue_item_ids,
                lead_ids=lead_ids,
                lead_statuses=["new", "followup"],
                source_keys=config.get("source_keys") or [],
                lanes=config.get("lanes") or [],
                limit=int(config.get("limit") or 10),
                max_queries_per_lane=int(config.get("max_queries_per_lane") or 2),
                persist_queries=not dry_run,
                active=True,
                dry_run=dry_run,
                dagster_run_id=context.run_id,
                metadata={"dagster_asset": "validation_gap_completion_report"},
            )
        )

        query_names = [query.query_name for query in source_pack_result.queries]
        ingest_result = service.ingest_validation_gap_source_queries(
            ValidationGapSourceIngestRequest(
                source_keys=config.get("source_keys") or [],
                query_names=query_names,
                limit_per_query=int(config.get("limit_per_query") or 3),
                max_queries=int(config.get("max_ingest_queries") or 20),
                dry_run=dry_run,
                dagster_run_id=context.run_id,
                metadata={"dagster_asset": "validation_gap_completion_report"},
            )
        )

        brief_runs = []
        generated_brief_ids = [item.queue_item_id for item in gap_result.brief_queue_items]
        if not dry_run and generated_brief_ids:
            for _ in range(max(int(config.get("brief_runs") or 0), 0)):
                run_result = service.run_next_research_brief_queue_item(
                    ResearchBriefQueueRunRequest(
                        queue_item_ids=generated_brief_ids,
                        statuses=["queued"],
                        limit=min(max(len(generated_brief_ids), 1), 10),
                        dagster_run_id=context.run_id,
                    )
                )
                brief_runs.append(run_result.model_dump(mode="json"))
                if not run_result.ran:
                    break

        errors = [
            *gap_result.errors,
            *source_pack_result.errors,
            *ingest_result.errors,
            *(error for run in brief_runs for error in run.get("errors", [])),
        ]
        brief_runs_completed = sum(
            1 for run in brief_runs if (run.get("queue_item") or {}).get("status") == "completed"
        )
        brief_runs_failed = sum(1 for run in brief_runs if (run.get("queue_item") or {}).get("status") == "failed")
        report = {
            "summary": {
                "dry_run": dry_run,
                "queue_item_count": len(queue_item_ids) or gap_result.queue_items_seen,
                "gap_count": gap_result.gap_count,
                "leads_created": gap_result.leads_created,
                "existing_leads": gap_result.existing_leads,
                "brief_queue_count": gap_result.brief_queue_count,
                "source_query_count": source_pack_result.query_count,
                "persisted_query_count": source_pack_result.persisted_query_count,
                "ingested_query_count": ingest_result.completed_query_count,
                "raw_records": ingest_result.raw_records,
                "research_objects": ingest_result.research_objects,
                "document_chunks": ingest_result.document_chunks,
                "brief_runs_requested": int(config.get("brief_runs") or 0),
                "brief_runs_attempted": sum(1 for run in brief_runs if run.get("ran")),
                "brief_runs_completed": brief_runs_completed,
                "brief_runs_failed": brief_runs_failed,
            },
            "evidence_gap_resolver": gap_result.model_dump(mode="json"),
            "source_pack": source_pack_result.model_dump(mode="json"),
            "source_ingest": ingest_result.model_dump(mode="json"),
            "brief_runs": brief_runs,
            "errors": errors,
        }
        return dg.MaterializeResult(
            value=report,
            metadata=_validation_gap_completion_metadata(report),
        )

    @dg.asset(
        group_name="ai_research",
        config_schema={
            "lead_ids": dg.Field([str], default_value=[], description="Specific follow-up lead IDs."),
            "statuses": dg.Field([str], default_value=["followup"], description="Lead statuses to inspect."),
            "source_keys": dg.Field([str], default_value=[], description="Optional lead source filters."),
            "search_source_keys": dg.Field(
                [str],
                default_value=[],
                description="Durable sources to search when a lead lacks identifiers.",
            ),
            "limit": dg.Field(int, default_value=25, description="Maximum leads to resolve."),
            "ingest_source_followups": dg.Field(
                bool,
                default_value=True,
                description="Ingest queued identifier follow-ups before promotion checks.",
            ),
            "search_missing_identifiers": dg.Field(
                bool,
                default_value=True,
                description="Search durable sources for leads without DOI/PMID/PMCID/NCT identifiers.",
            ),
            "promote_ready_leads": dg.Field(
                bool,
                default_value=True,
                description="Move leads back to watching only after durable evidence is attached.",
            ),
            "run_claim_extraction": dg.Field(
                bool,
                default_value=True,
                description="Run entity, claim extraction, and curation for identifier follow-up ingest.",
            ),
            "dry_run": dg.Field(bool, default_value=False, description="Plan work without mutating lead state."),
            "force_live_search": dg.Field(
                bool,
                default_value=False,
                description="Refresh durable sources even when stored chunks already satisfy the evidence threshold.",
            ),
            "inspect_evidence_refs": dg.Field(
                bool,
                default_value=True,
                description="Attach chunk/object inspection previews for selected evidence refs.",
            ),
            "min_evidence_chunks": dg.Field(int, default_value=1, description="Minimum evidence chunks to promote."),
            "evidence_inspection_limit": dg.Field(
                int,
                default_value=8,
                description="Maximum chunk/object evidence refs to inspect in metadata.",
            ),
            "search_limit_per_source": dg.Field(int, default_value=2, description="Search ingest limit per source."),
            "max_search_terms": dg.Field(int, default_value=12, description="Maximum terms in generated source query."),
            "approved_by": dg.Field(str, is_required=False, description="Operator identity for follow-up ingest."),
        },
    )
    def research_followup_resolver_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Resolve evidence-light research leads before they enter synthesis."""

        from uuid import UUID

        from .service import HSAResearchService

        config = context.op_config or {}
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).resolve_research_followups(
            ResearchFollowupResolverRequest(
                lead_ids=[UUID(lead_id) for lead_id in config["lead_ids"]],
                statuses=config["statuses"],
                source_keys=config["source_keys"],
                search_source_keys=config["search_source_keys"],
                limit=config["limit"],
                ingest_source_followups=config["ingest_source_followups"],
                search_missing_identifiers=config["search_missing_identifiers"],
                promote_ready_leads=config["promote_ready_leads"],
                run_claim_extraction=config["run_claim_extraction"],
                dry_run=config["dry_run"],
                force_live_search=bool(config.get("force_live_search", False)),
                inspect_evidence_refs=bool(config.get("inspect_evidence_refs", True)),
                min_evidence_chunks=config["min_evidence_chunks"],
                evidence_inspection_limit=int(config.get("evidence_inspection_limit", 8)),
                search_limit_per_source=config["search_limit_per_source"],
                max_search_terms=config["max_search_terms"],
                approved_by=config.get("approved_by"),
                dagster_run_id=context.run_id,
                metadata={"dagster_asset": "research_followup_resolver_report"},
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_research_followup_resolver_metadata(report),
        )

    @dg.asset(group_name="social_monitoring")
    def x_topic_monitor_review_report(
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual TwitterAPI.io topic monitoring plus agent source-review report."""

        from .service import HSAResearchService
        from .x_topic_monitor import (
            TWITTERAPI_IO_MAX_SINGLE_PAGE_RESULTS,
            X_TOPIC_SOURCE_KEY,
            TwitterApiIoProvider,
            XTopicRequest,
            build_default_source_queries,
        )

        provider_name = os.getenv("X_TOPIC_PROVIDER", "twitterapi_io")
        max_results = int(os.getenv("HSA_X_TOPIC_MAX_RESULTS", str(TWITTERAPI_IO_MAX_SINGLE_PAGE_RESULTS)))
        query_name_filter = os.getenv("HSA_X_TOPIC_QUERY_NAME")
        query_delay_seconds = max(0.0, float(os.getenv("HSA_X_TOPIC_QUERY_DELAY_SECONDS", "6")))
        retention_mode = os.getenv("HSA_X_RETENTION_MODE", "store_metadata_only")
        review_mode = os.getenv("HSA_X_TOPIC_REVIEW_MODE", "openrouter_required")
        review_models = [
            model.strip()
            for model in os.getenv("HSA_X_TOPIC_REVIEW_MODELS", "").split(",")
            if model.strip()
        ]
        max_review_candidates = int(os.getenv("HSA_X_TOPIC_REVIEW_MAX_CANDIDATES", "20"))
        queries = [
            query
            for query in build_default_source_queries()
            if query_name_filter is None or query.query_name == query_name_filter
        ]

        query_results: list[dict[str, Any]] = []
        candidate_rows: list[dict[str, Any]] = []
        errors: list[dict[str, Any] | str] = []
        raw_tweet_count = 0

        if provider_name != "twitterapi_io":
            errors.append(f"Unsupported X topic provider: {provider_name}")
        else:
            provider = TwitterApiIoProvider()
            for query_index, query in enumerate(queries):
                if query_index > 0 and query_delay_seconds > 0:
                    time.sleep(query_delay_seconds)
                try:
                    result = provider.search(
                        XTopicRequest(
                            query=query.query_text,
                            query_name=query.query_name,
                            max_results=max_results,
                            retention_mode=retention_mode,
                        )
                    )
                except Exception as exc:
                    errors.append({"query_name": query.query_name, "error": str(exc)})
                    continue
                raw_tweet_count += result.raw_tweet_count
                query_results.append(result.model_dump(mode="json"))
                for candidate in result.candidates:
                    review_status = (
                        candidate.review_status.value
                        if hasattr(candidate.review_status, "value")
                        else str(candidate.review_status)
                    )
                    candidate_rows.append(
                        {
                            "query_name": query.query_name,
                            "post_id": candidate.source_record_id,
                            "username": candidate.username,
                            "quality_score": candidate.quality_score,
                            "review_status": review_status,
                            "matched_terms": candidate.matched_terms,
                            "durable_links": candidate.durable_links,
                        }
                    )

        report = {
            "source_key": X_TOPIC_SOURCE_KEY,
            "provider": provider_name,
            "queries": [query.model_dump(mode="json") for query in queries],
            "query_delay_seconds": query_delay_seconds,
            "query_results": query_results,
            "raw_tweet_count": raw_tweet_count,
            "candidate_count": len(candidate_rows),
            "candidates": candidate_rows,
            "manual_review_required": True,
            "errors": errors,
        }
        repository = research_repository.build_repository()
        agent_review = HSAResearchService(repository).run_x_topic_review(
            XTopicReviewRequest(
                provider_report=report,
                recent_run_limit=5,
                max_candidates=max_review_candidates,
                review_mode=review_mode,  # type: ignore[arg-type]
                review_models=review_models,
                metadata={
                    "provider": provider_name,
                    "query_name_filter": query_name_filter,
                    "query_delay_seconds": query_delay_seconds,
                    "retention_mode": retention_mode,
                },
            )
        )
        report["agent_review"] = agent_review.model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_x_topic_monitor_report_metadata(report))

    @dg.asset(
        group_name="social_monitoring",
        config_schema={
            "urls": dg.Field(
                [str],
                is_required=False,
                description="Direct x_linked_article URLs to fetch and parse for manual validation.",
            ),
            "recent_run_limit": dg.Field(
                int,
                is_required=False,
                description="Recent X topic review agent runs to inspect for queued article URLs.",
            ),
            "max_urls": dg.Field(
                int,
                is_required=False,
                description="Maximum candidate URLs to process.",
            ),
            "fetch": dg.Field(
                bool,
                is_required=False,
                description="Whether to fetch candidate URLs after approval.",
            ),
            "parse": dg.Field(
                bool,
                is_required=False,
                description="Whether to parse fetched snapshots into review records.",
            ),
            "approved_by": dg.Field(
                str,
                is_required=False,
                description="Operator approval identity required for controlled fetch.",
            ),
            "approval_note": dg.Field(
                str,
                is_required=False,
                description="Operator approval note for the controlled fetch.",
            ),
            "robots_policy": dg.Field(
                str,
                is_required=False,
                description="Reviewed robots/TOS policy: unknown, reviewed, disallow, or manual_only.",
            ),
        },
    )
    def x_linked_article_followup_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Controlled scraper follow-up for article links queued by X topic review."""

        from .service import HSAResearchService

        config = context.op_config or {}
        repository = research_repository.build_repository()
        direct_urls = config.get("urls") or _parse_delimited_string_list(
            os.getenv("HSA_X_LINKED_ARTICLE_URLS")
        )
        recent_run_limit = int(
            _config_or_env(config, "recent_run_limit", "HSA_X_LINKED_ARTICLE_RECENT_RUN_LIMIT", 10)
        )
        max_urls = int(_config_or_env(config, "max_urls", "HSA_X_LINKED_ARTICLE_MAX_URLS", 10))
        approved_by = _config_or_env(
            config, "approved_by", "HSA_X_LINKED_ARTICLE_APPROVED_BY", None
        )
        approval_note = _config_or_env(
            config, "approval_note", "HSA_X_LINKED_ARTICLE_APPROVAL_NOTE", None
        )
        robots_policy = _config_or_env(
            config, "robots_policy", "HSA_X_LINKED_ARTICLE_ROBOTS_POLICY", "reviewed"
        )
        result = HSAResearchService(repository).run_x_linked_article_followup(
            XLinkedArticleFollowupRequest(
                urls=direct_urls,
                recent_run_limit=recent_run_limit,
                max_urls=max_urls,
                approved_by=approved_by,
                approval_note=approval_note,
                robots_policy=robots_policy,  # type: ignore[arg-type]
                fetch=_config_or_env_bool(config, "fetch", "HSA_X_LINKED_ARTICLE_FETCH", True),
                parse=_config_or_env_bool(config, "parse", "HSA_X_LINKED_ARTICLE_PARSE", True),
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_x_linked_article_followup_metadata(report))

    @dg.asset(
        group_name="social_monitoring",
        config_schema={
            "review_ids": dg.Field([str], is_required=False, description="Specific scrape review IDs to review."),
            "review_status": dg.Field(
                str,
                is_required=False,
                description="Review status filter: needs_review, accepted, rejected, or empty for all.",
            ),
            "limit": dg.Field(int, is_required=False, description="Maximum parsed article records to review."),
            "review_mode": dg.Field(
                str,
                is_required=False,
                description="Review mode: deterministic_only, external_required, openrouter_required, or openrouter_compare.",
            ),
            "review_models": dg.Field([str], is_required=False, description="Optional model ids for external review."),
        },
    )
    def x_linked_article_review_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Recommend-only agent review for parsed linked article records."""

        from .service import HSAResearchService

        config = context.op_config or {}
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).run_x_linked_article_review(
            XLinkedArticleReviewRequest(
                review_ids=config.get("review_ids") or [],
                review_status=config.get("review_status", "needs_review") or None,
                limit=int(_config_or_env(config, "limit", "HSA_X_LINKED_ARTICLE_REVIEW_LIMIT", 50)),
                review_mode=_config_or_env(
                    config,
                    "review_mode",
                    "HSA_X_LINKED_ARTICLE_REVIEW_MODE",
                    "openrouter_required",
                ),
                review_models=config.get("review_models") or _parse_delimited_string_list(
                    os.getenv("HSA_X_LINKED_ARTICLE_REVIEW_MODELS")
                ),
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_x_linked_article_review_metadata(report))

    @dg.asset(
        group_name="source_followup",
        config_schema={
            "source_key": dg.Field(str, is_required=False, description="Scrape source key to scan."),
            "review_ids": dg.Field([str], is_required=False, description="Specific scrape review IDs to queue."),
            "review_status": dg.Field(
                str,
                is_required=False,
                description="Optional scrape review status filter.",
            ),
            "limit": dg.Field(int, is_required=False, description="Maximum scrape review records to scan."),
            "include_existing": dg.Field(bool, is_required=False, description="Include existing queue rows in output."),
            "include_agent_recommendations": dg.Field(
                bool,
                is_required=False,
                description="Also queue validated recommendations from recent linked-article review agent runs.",
            ),
            "agent_run_limit": dg.Field(
                int,
                is_required=False,
                description="Maximum recent linked-article review agent runs to scan.",
            ),
        },
    )
    def source_followup_queue_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Queue primary-source follow-ups from parsed linked-article records."""

        from .service import HSAResearchService

        config = context.op_config or {}
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).queue_source_followups(
            SourceFollowupQueueRequest(
                source_key=_config_or_env(config, "source_key", "HSA_SOURCE_FOLLOWUP_SOURCE_KEY", "x_linked_article"),
                review_ids=config.get("review_ids") or [],
                review_status=config.get("review_status"),
                limit=int(_config_or_env(config, "limit", "HSA_SOURCE_FOLLOWUP_QUEUE_LIMIT", 100)),
                include_existing=_config_or_env_bool(
                    config,
                    "include_existing",
                    "HSA_SOURCE_FOLLOWUP_INCLUDE_EXISTING",
                    False,
                ),
                include_agent_recommendations=_config_or_env_bool(
                    config,
                    "include_agent_recommendations",
                    "HSA_SOURCE_FOLLOWUP_INCLUDE_AGENT_RECOMMENDATIONS",
                    True,
                ),
                agent_run_limit=int(
                    _config_or_env(config, "agent_run_limit", "HSA_SOURCE_FOLLOWUP_AGENT_RUN_LIMIT", 20)
                ),
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_source_followup_queue_metadata(report))

    @dg.asset(
        group_name="source_followup",
        config_schema=_SOURCE_FOLLOWUP_INGEST_CONFIG_SCHEMA,
    )
    def source_followup_ingest_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Manual primary-source follow-up ingestion through API harvesters."""

        return _run_source_followup_ingest_asset(
            context,
            research_repository,
            lane_name="source_followup_ingest",
        )

    @dg.asset(group_name="source_followup", config_schema=_SOURCE_FOLLOWUP_LANE_CONFIG_SCHEMA)
    def pubmed_source_followup_ingest_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """PubMed-only primary-source follow-up ingestion lane."""

        return _run_source_followup_ingest_asset(
            context,
            research_repository,
            lane_name="pubmed_source_followup_ingest",
            default_source_keys=("pubmed",),
            default_limit=SOURCE_FOLLOWUP_LANE_LIMITS["pubmed"],
        )

    @dg.asset(group_name="source_followup", config_schema=_SOURCE_FOLLOWUP_LANE_CONFIG_SCHEMA)
    def crossref_source_followup_ingest_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Crossref-only primary-source follow-up ingestion lane."""

        return _run_source_followup_ingest_asset(
            context,
            research_repository,
            lane_name="crossref_source_followup_ingest",
            default_source_keys=("crossref",),
            default_limit=SOURCE_FOLLOWUP_LANE_LIMITS["crossref"],
        )

    @dg.asset(group_name="source_followup", config_schema=_SOURCE_FOLLOWUP_LANE_CONFIG_SCHEMA)
    def pmc_oa_source_followup_ingest_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """PMC OA-only primary-source follow-up ingestion lane."""

        return _run_source_followup_ingest_asset(
            context,
            research_repository,
            lane_name="pmc_oa_source_followup_ingest",
            default_source_keys=("pmc_oa",),
            default_limit=SOURCE_FOLLOWUP_LANE_LIMITS["pmc_oa"],
        )

    @dg.asset(group_name="source_followup", config_schema=_SOURCE_FOLLOWUP_LANE_CONFIG_SCHEMA)
    def clinicaltrials_gov_source_followup_ingest_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """ClinicalTrials.gov-only primary-source follow-up ingestion lane."""

        return _run_source_followup_ingest_asset(
            context,
            research_repository,
            lane_name="clinicaltrials_gov_source_followup_ingest",
            default_source_keys=("clinicaltrials_gov",),
            default_limit=SOURCE_FOLLOWUP_LANE_LIMITS["clinicaltrials_gov"],
        )

    @dg.asset(group_name="source_followup", config_schema=_SOURCE_FOLLOWUP_LANE_CONFIG_SCHEMA)
    def unpaywall_source_followup_ingest_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Unpaywall-only DOI enrichment follow-up ingestion lane."""

        return _run_source_followup_ingest_asset(
            context,
            research_repository,
            lane_name="unpaywall_source_followup_ingest",
            default_source_keys=("unpaywall",),
            default_limit=SOURCE_FOLLOWUP_LANE_LIMITS["unpaywall"],
        )

    @dg.op(
        required_resource_keys={"research_repository"},
        config_schema={
            "source_key": dg.Field(
                str,
                default_value="europe_pmc",
                description="Full-text source to validate.",
            ),
            "partition_date": dg.Field(
                str,
                description="Publication-date partition to validate as YYYY-MM-DD.",
            ),
            "source_limit": dg.Field(
                int,
                default_value=25,
                description="Maximum source records to fetch for the manual validation run.",
            ),
            "extract_limit": dg.Field(
                int,
                default_value=100,
                description="Maximum chunks to extract claims from.",
            ),
            "curate_limit": dg.Field(
                int,
                default_value=100,
                description="Maximum claims to curate.",
            ),
            "review_mode": dg.Field(
                str,
                default_value="openrouter_required",
                description="FullTextOps review mode: external_required, openrouter_required, openrouter_compare, or deterministic_only.",
            ),
            "review_models": dg.Field(
                [str],
                default_value=[],
                description="OpenRouter model ids to review with when using an OpenRouter review mode.",
            ),
        },
    )
    def full_text_source_date_ops(context) -> dict:
        """Run one full-text source/date slice and feed the report into FullTextOpsAgent."""

        from .service import HSAResearchService

        config = context.op_config
        source_key = config["source_key"]
        partition_date = config["partition_date"]
        research_repository = context.resources.research_repository
        partition_report = _run_literature_full_text_source_date_report(
            research_repository,
            source_key=source_key,
            partition_date=partition_date,
            source_limit=config["source_limit"],
            extract_limit=config["extract_limit"],
            curate_limit=config["curate_limit"],
        )
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).run_full_text_ops(
            FullTextOpsRequest(
                source_keys=(source_key,),
                partition_date=partition_date,
                full_text_report=partition_report,
                review_mode=config["review_mode"],
                review_models=config["review_models"],
            )
        )
        ops_report = result.model_dump(mode="json")
        context.add_output_metadata(
            {
                "source_key": source_key,
                "partition_date": partition_date,
                "agent_run_id": ops_report.get("agent_run_id"),
                "action_count": len(ops_report.get("actions", [])),
                "blocking_count": sum(
                    1
                    for action in ops_report.get("actions", [])
                    if action.get("severity") in {"warning", "critical"}
                ),
                "schedule_readiness": ops_report.get("schedule_readiness"),
                "should_block_schedule": ops_report.get("should_block_schedule"),
                "full_text_triage": _metadata_table(
                    partition_report.get("full_text_triage", []),
                    _FULL_TEXT_TRIAGE_TABLE_COLUMNS,
                ),
                "recommendations": _metadata_table(
                    ops_report.get("actions", []),
                    _FULL_TEXT_OPS_ACTION_TABLE_COLUMNS,
                ),
            }
        )
        return {
            "partition_report": partition_report,
            "ops_report": ops_report,
        }

    @dg.asset(group_name="entity_resolution")
    def entity_resolution_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Deterministic entity resolution over persisted hosted API chunks."""

        from .entity_resolution import resolve_entities_for_repository
        from .structured_orchestration import structured_source_qa

        repository = research_repository.build_repository()
        reports = []
        for source_key in HOSTED_API_REPORT_KEYS:
            resolution = resolve_entities_for_repository(
                repository,
                source_key=source_key,
                limit=1000,
            ).model_dump(mode="json")
            reports.append(
                {
                    "source_key": source_key,
                    "resolution": resolution,
                    "qa": structured_source_qa(repository, source_key, sample_limit=2),
                }
            )
        report = {
            "source_keys": list(HOSTED_API_REPORT_KEYS),
            "sources": reports,
            "totals": {
                "chunks_seen": sum(report["resolution"].get("chunks_seen", 0) for report in reports),
                "entities_upserted": sum(report["resolution"].get("entities_upserted", 0) for report in reports),
                "aliases_upserted": sum(report["resolution"].get("aliases_upserted", 0) for report in reports),
                "mentions_upserted": sum(report["resolution"].get("mentions_upserted", 0) for report in reports),
                "entity_mentions": sum(report["qa"].get("entity_mentions", 0) for report in reports),
            },
            "errors": [
                f"{report['source_key']}: {error}"
                for report in reports
                for error in report["resolution"].get("errors", [])
            ],
            "coverage": repository.coverage_summary(),
        }
        return dg.MaterializeResult(value=report, metadata=_entity_resolution_report_metadata(report))

    @dg.asset(group_name="research_primitives")
    def entity_lookup_index_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Materialize source-derived entity aliases for TWOG-owned primitive lookup."""

        from .research_primitives import materialize_entity_lookup_index

        repository = research_repository.build_repository()
        result = materialize_entity_lookup_index(
            repository,
            EntityLookupIndexRequest(
                source_keys=list(ENTITY_LOOKUP_INDEX_SOURCE_KEYS),
                limit_per_source=1000,
            ),
        )
        report = {
            "source_keys": list(ENTITY_LOOKUP_INDEX_SOURCE_KEYS),
            "sources": [summary.model_dump(mode="json") for summary in result.source_summaries],
            "totals": {
                "records_seen": result.records_seen,
                "entities_upserted": result.entities_upserted,
                "aliases_upserted": result.aliases_upserted,
                "source_versions_upserted": result.source_versions_upserted,
            },
            "errors": list(result.errors),
            "coverage": repository.coverage_summary(),
        }
        return dg.MaterializeResult(value=report, metadata=_entity_lookup_index_metadata(report))

    @dg.asset(group_name="embedding_index")
    def embedding_index_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Deterministic local embedding index over persisted document chunks."""

        from .embeddings import default_embedding_model_for_environment, index_embeddings_for_repository

        repository = research_repository.build_repository()
        embedding_model = default_embedding_model_for_environment()
        result = index_embeddings_for_repository(repository, embedding_model=embedding_model)
        embedding_coverage = repository.embedding_coverage(embedding_model=result.embedding_model)
        coverage = embedding_coverage.model_dump(mode="json")
        totals = {
            "chunks_seen": result.chunks_seen,
            "embeddings_created": result.embeddings_created,
            "embeddings_updated": result.embeddings_updated,
            "embeddings_skipped": result.embeddings_skipped,
            "total_chunks": coverage["total_chunks"],
            "embedded_chunks": coverage["embedded_chunks"],
            "missing_chunks": coverage["missing_chunks"],
        }
        errors = list(result.errors)
        passes_minimum_bar = not errors and (
            (coverage["total_chunks"] == 0 and result.chunks_seen == 0 and coverage["embedded_chunks"] == 0)
            or (coverage["total_chunks"] > 0 and result.chunks_seen >= 1 and coverage["embedded_chunks"] >= 1)
        )
        report = {
            "embedding_model": result.embedding_model,
            "source_key": result.source_key,
            "totals": totals,
            "errors": errors,
            "embedding_coverage": coverage,
            "coverage": repository.coverage_summary(),
            "passes_minimum_bar": passes_minimum_bar,
        }
        return dg.MaterializeResult(value=report, metadata=_embedding_index_report_metadata(report))

    @dg.asset(group_name="embedding_index")
    def embedding_maintenance_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Prune orphan embedding rows and verify active-model coverage."""

        from .embeddings import default_embedding_model_for_environment, maintain_embedding_index

        repository = research_repository.build_repository()
        embedding_model = default_embedding_model_for_environment()
        report = maintain_embedding_index(repository, embedding_model=embedding_model).to_report()
        return dg.MaterializeResult(value=report, metadata=_embedding_maintenance_report_metadata(report))

    @dg.asset_check(asset=source_registry)
    def source_registry_has_phase_one_sources(source_registry: list[dict]) -> dg.AssetCheckResult:
        """Ensure the first bridge has the minimum source backbone."""

        source_keys = {source["source_key"] for source in source_registry}
        required = {"pubmed", "europe_pmc", "openalex", "crossref", "pmc_oa", "unpaywall"}
        missing = sorted(required - source_keys)
        return dg.AssetCheckResult(
            passed=not missing,
            metadata={"missing": missing, "required": sorted(required)},
        )

    @dg.asset_check(asset=structured_source_pipeline_report)
    def structured_source_pipeline_has_minimum_outputs(
        structured_source_pipeline_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure each structured source produced source objects and claims."""

        source_reports = structured_source_pipeline_report.get("sources", [])
        failed_sources = [
            report["source_key"]
            for report in source_reports
            if not report.get("qa", {}).get("passes_minimum_bar", False)
        ]
        errors = structured_source_pipeline_report.get("errors", [])
        return dg.AssetCheckResult(
            passed=not failed_sources and not errors,
            metadata={
                "failed_sources": failed_sources,
                "errors": errors,
                "totals": structured_source_pipeline_report.get("totals", {}),
            },
        )

    @dg.asset_check(asset=structured_source_smoke_report)
    def structured_source_smoke_has_minimum_outputs(
        structured_source_smoke_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the hosted-runtime smoke run writes at least one PubChem result."""

        totals = structured_source_smoke_report.get("totals", {})
        errors = structured_source_smoke_report.get("errors", [])
        passed = not errors and all(totals.get(field, 0) >= 1 for field in ("raw_records", "research_objects", "claims"))
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "errors": errors,
                "source_keys": structured_source_smoke_report.get("source_keys", []),
                "totals": totals,
            },
        )

    @dg.asset_check(asset=structured_source_multisource_smoke_report)
    def structured_source_multisource_smoke_has_minimum_outputs(
        structured_source_multisource_smoke_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure each structured API source can write records, chunks, and claims."""

        source_reports = structured_source_multisource_smoke_report.get("sources", [])
        failed_sources = [
            report["source_key"]
            for report in source_reports
            if not report.get("qa", {}).get("passes_minimum_bar", False)
        ]
        errors = structured_source_multisource_smoke_report.get("errors", [])
        passed = not failed_sources and not errors
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "failed_sources": failed_sources,
                "errors": errors,
                "source_keys": structured_source_multisource_smoke_report.get("source_keys", []),
                "totals": structured_source_multisource_smoke_report.get("totals", {}),
            },
        )

    @dg.asset_check(asset=literature_clinical_smoke_report)
    def literature_clinical_smoke_has_minimum_outputs(
        literature_clinical_smoke_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure literature and clinical API sources can write records and produce claims."""

        source_reports = literature_clinical_smoke_report.get("sources", [])
        failed_sources = [
            report["source_key"]
            for report in source_reports
            if not _has_minimum_ingested_source_outputs(report)
        ]
        errors = literature_clinical_smoke_report.get("errors", [])
        totals = literature_clinical_smoke_report.get("totals", {})
        passed = not failed_sources and not errors and totals.get("claims", 0) >= 1
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "failed_sources": failed_sources,
                "errors": errors,
                "minimum_total_claims": 1,
                "source_keys": literature_clinical_smoke_report.get("source_keys", []),
                "totals": totals,
            },
        )

    @dg.asset_check(asset=all_api_smoke_report)
    def all_api_smoke_has_minimum_outputs(
        all_api_smoke_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure every implemented API source can write records and chunks."""

        source_reports = all_api_smoke_report.get("sources", [])
        failed_sources = [
            report["source_key"]
            for report in source_reports
            if not _has_minimum_source_ingestion_outputs(report)
        ]
        errors = all_api_smoke_report.get("errors", [])
        passed = not failed_sources and not errors
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "mode": all_api_smoke_report.get("mode"),
                "failed_sources": failed_sources,
                "errors": errors,
                "source_keys": list(all_api_smoke_report.get("source_keys", [])),
                "totals": all_api_smoke_report.get("totals", {}),
            },
        )

    @dg.asset_check(asset=literature_corpus_harvest_report)
    def literature_corpus_harvest_has_hundreds_of_records(
        literature_corpus_harvest_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the corpus run produces a meaningful persisted literature set."""

        errors = literature_corpus_harvest_report.get("errors", [])
        totals = literature_corpus_harvest_report.get("totals", {})
        passed = (
            not errors
            and totals.get("raw_records", 0) >= 200
            and totals.get("research_objects", 0) >= 100
            and totals.get("document_chunks", 0) >= 100
            and totals.get("claims", 0) >= 50
        )
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "errors": errors,
                "minimum_totals": {
                    "raw_records": 200,
                    "research_objects": 100,
                    "document_chunks": 100,
                    "claims": 50,
                },
                "source_keys": literature_corpus_harvest_report.get("source_keys", []),
                "source_limits": LITERATURE_CORPUS_SOURCE_LIMITS,
                "totals": totals,
            },
        )

    @dg.asset_check(asset=literature_full_text_refresh_report)
    def literature_full_text_refresh_has_outputs(
        literature_full_text_refresh_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the bounded full-text lane writes persisted outputs."""

        return _full_text_check_result(
            literature_full_text_refresh_report,
            source_limits=LITERATURE_FULL_TEXT_SOURCE_LIMITS,
        )

    @dg.asset_check(asset=europe_pmc_full_text_refresh_report)
    def europe_pmc_full_text_refresh_has_outputs(
        europe_pmc_full_text_refresh_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the Europe PMC full-text lane writes persisted outputs."""

        return _full_text_check_result(
            europe_pmc_full_text_refresh_report,
            source_limits={"europe_pmc": LITERATURE_FULL_TEXT_SOURCE_LIMITS["europe_pmc"]},
        )

    @dg.asset_check(asset=pmc_oa_full_text_refresh_report)
    def pmc_oa_full_text_refresh_has_outputs(
        pmc_oa_full_text_refresh_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the PMC OA full-text lane writes persisted outputs."""

        return _full_text_check_result(
            pmc_oa_full_text_refresh_report,
            source_limits={"pmc_oa": LITERATURE_FULL_TEXT_SOURCE_LIMITS["pmc_oa"]},
        )

    @dg.asset_check(asset=literature_full_text_ingest_smoke_report)
    def literature_full_text_ingest_smoke_has_outputs(
        literature_full_text_ingest_smoke_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the pull-only full-text smoke lane writes persisted body chunks."""

        return _full_text_ingestion_check_result(
            literature_full_text_ingest_smoke_report,
            source_limits=LITERATURE_FULL_TEXT_SMOKE_LIMITS,
        )

    @dg.asset_check(asset=europe_pmc_full_text_ingest_report)
    def europe_pmc_full_text_ingest_has_outputs(
        europe_pmc_full_text_ingest_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the Europe PMC pull-only lane writes persisted body chunks."""

        return _full_text_ingestion_check_result(
            europe_pmc_full_text_ingest_report,
            source_limits={"europe_pmc": LITERATURE_FULL_TEXT_SOURCE_LIMITS["europe_pmc"]},
        )

    @dg.asset_check(asset=pmc_oa_full_text_ingest_report)
    def pmc_oa_full_text_ingest_has_outputs(
        pmc_oa_full_text_ingest_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the PMC OA pull-only lane writes persisted body chunks."""

        return _full_text_ingestion_check_result(
            pmc_oa_full_text_ingest_report,
            source_limits={"pmc_oa": LITERATURE_FULL_TEXT_SOURCE_LIMITS["pmc_oa"]},
        )

    @dg.asset_check(asset=literature_full_text_smoke_report)
    def literature_full_text_smoke_has_outputs(
        literature_full_text_smoke_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the fast full-text smoke lane writes persisted outputs."""

        return _full_text_check_result(
            literature_full_text_smoke_report,
            source_limits=LITERATURE_FULL_TEXT_SMOKE_LIMITS,
        )

    @dg.asset_check(asset=literature_full_text_source_date_report)
    def literature_full_text_source_date_has_outputs(
        literature_full_text_source_date_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure a full-text source/date partition either writes body chunks or is cleanly empty."""

        return _full_text_partition_check_result(literature_full_text_source_date_report)

    @dg.asset_check(asset=structured_source_count_report)
    def structured_source_count_report_has_minimum_outputs(
        structured_source_count_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure persisted structured-source counts are healthy after hosted runs."""

        failed_sources = structured_source_count_report.get("failed_sources", [])
        return dg.AssetCheckResult(
            passed=not failed_sources,
            metadata={
                "failed_sources": failed_sources,
                "source_keys": structured_source_count_report.get("source_keys", []),
                "totals": structured_source_count_report.get("totals", {}),
            },
        )

    @dg.asset_check(asset=source_health_report)
    def source_health_report_has_no_failed_sources(
        source_health_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure persisted source health has no hard source failures."""

        failed_sources = source_health_report.get("failed_sources", [])
        return dg.AssetCheckResult(
            passed=not failed_sources,
            metadata={
                "failed_sources": failed_sources,
                "triage_sources": source_health_report.get("triage_sources", []),
                "watch_sources": source_health_report.get("watch_sources", []),
                "summary": source_health_report.get("summary", {}),
                "totals": source_health_report.get("totals", {}),
            },
        )

    @dg.asset_check(asset=entity_resolution_report)
    def entity_resolution_has_minimum_outputs(
        entity_resolution_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure deterministic entity resolution writes first-class mentions."""

        errors = entity_resolution_report.get("errors", [])
        totals = entity_resolution_report.get("totals", {})
        return dg.AssetCheckResult(
            passed=not errors and totals.get("entity_mentions", 0) >= 1,
            metadata={
                "errors": errors,
                "minimum_entity_mentions": 1,
                "totals": totals,
            },
        )

    @dg.asset_check(asset=embedding_index_report)
    def embedding_index_has_minimum_outputs(
        embedding_index_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure stored chunks can produce at least one deterministic embedding."""

        errors = embedding_index_report.get("errors", [])
        totals = embedding_index_report.get("totals", {})
        embedding_coverage = embedding_index_report.get("embedding_coverage", {})
        total_chunks = embedding_coverage.get("total_chunks", 0)
        embedded_chunks = embedding_coverage.get("embedded_chunks", 0)
        passed = not errors and (
            (total_chunks == 0 and totals.get("chunks_seen", 0) == 0 and embedded_chunks == 0)
            or (total_chunks > 0 and totals.get("chunks_seen", 0) >= 1 and embedded_chunks >= 1)
        )
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "errors": errors,
                "minimum_contract": {
                    "when_chunks_exist": "at_least_one_embedding",
                    "when_no_chunks_exist": "zero_chunks_zero_embeddings",
                },
                "totals": totals,
                "embedding_coverage": embedding_coverage,
            },
        )

    @dg.asset_check(asset=embedding_maintenance_report)
    def embedding_maintenance_has_clean_coverage(
        embedding_maintenance_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure active embeddings have no orphan rows and full chunk coverage."""

        errors = embedding_maintenance_report.get("errors", [])
        embedding_coverage = embedding_maintenance_report.get("embedding_coverage", {})
        total_chunks = embedding_coverage.get("total_chunks", 0)
        embedded_chunks = embedding_coverage.get("embedded_chunks", 0)
        missing_chunks = embedding_coverage.get("missing_chunks", 0)
        passed = not errors and (
            (total_chunks == 0 and embedded_chunks == 0)
            or (total_chunks > 0 and missing_chunks == 0)
        )
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "errors": errors,
                "minimum_contract": {
                    "when_chunks_exist": "active_embedding_model_covers_every_chunk",
                    "when_no_chunks_exist": "zero_chunks_zero_embeddings",
                },
                "orphan_embeddings": embedding_maintenance_report.get("orphan_embeddings", {}),
                "embedding_coverage": embedding_coverage,
            },
        )

    ingestion_bridge_assets = [
        source_registry,
        source_scout_plan,
        source_queries,
        raw_source_records,
        canonical_research_objects,
        document_chunks,
        normalized_entities,
        claims,
        curated_claims,
        coverage_snapshot,
        structured_source_pipeline_report,
        structured_source_smoke_report,
        structured_source_multisource_smoke_report,
        research_primitive_source_smoke_report,
        research_primitive_source_pipeline_report,
        literature_clinical_smoke_report,
        all_api_smoke_report,
        literature_corpus_harvest_report,
        literature_corpus_source_date_report,
        literature_full_text_refresh_report,
        europe_pmc_full_text_refresh_report,
        pmc_oa_full_text_refresh_report,
        literature_full_text_ingest_smoke_report,
        europe_pmc_full_text_ingest_report,
        pmc_oa_full_text_ingest_report,
        literature_full_text_smoke_report,
        literature_full_text_source_date_report,
        structured_source_count_report,
        source_health_report,
        candidate_contribution_intake_report,
        candidate_contribution_triage_report,
        work_packet_seed_report,
        proof_capsule_reward_sync_report,
        command_center_report,
        research_hunt_queue_report,
        research_hunt_queue_maintenance_report,
        research_hunt_synthesis_queue_report,
        research_hunt_synthesis_doc_report,
        full_text_ops_agent_report,
        agent_performance_report,
        agent_performance_evaluation_report,
        reward_event_report,
        reward_event_sync_report,
        capsule_llm_grade_report,
        research_brief_agent_report,
        therapy_committee_report,
        validation_tool_catalog_report,
        validation_tool_match_report,
        therapy_idea_library_report,
        public_candidate_snapshot_report,
        public_candidate_integrity_report,
        hypothesis_promotion_report,
        validation_packet_report,
        validation_decision_report,
        research_program_board_report,
        research_program_library_report,
        research_program_evidence_loop_report,
        omics_accession_hunt_report,
        omics_evidence_packet_report,
        omics_readout_report,
        omics_locus_signal_report,
        omics_followup_report,
        therapy_committee_validation_queue_report,
        research_brief_library_report,
        research_brief_evaluation_report,
        research_brief_evaluation_library_report,
        research_brief_quality_report,
        research_brief_followup_queue_report,
        validation_plan_report,
        validation_plan_library_report,
        validation_request_queue_report,
        validation_request_queue_library_report,
        md_smoke_compute_job_report,
        compute_job_report,
        md_expert_review_packet_report,
        md_expert_agent_review_report,
        validation_autopilot_report,
        research_brief_queue_report,
        research_brief_queue_batch_report,
        research_brief_queue_seed_report,
        research_brief_queue_runner_report,
        research_brief_queue_maintenance_report,
        research_brief_playground_pack_report,
        research_leads_report,
        evidence_gap_resolver_report,
        validation_gap_source_pack_report,
        pubmed_identifier_repair_report,
        validation_gap_source_ingest_report,
        validation_gap_completion_report,
        research_followup_resolver_report,
        x_topic_monitor_review_report,
        x_linked_article_followup_report,
        x_linked_article_review_report,
        source_followup_queue_report,
        source_followup_ingest_report,
        pubmed_source_followup_ingest_report,
        crossref_source_followup_ingest_report,
        pmc_oa_source_followup_ingest_report,
        clinicaltrials_gov_source_followup_ingest_report,
        unpaywall_source_followup_ingest_report,
        entity_resolution_report,
        entity_lookup_index_report,
        embedding_index_report,
        embedding_maintenance_report,
    ]

    structured_source_pipeline_job = dg.define_asset_job(
        "structured_source_pipeline_job",
        selection=dg.AssetSelection.assets(structured_source_pipeline_report),
    )
    structured_source_smoke_job = dg.define_asset_job(
        "structured_source_smoke_job",
        selection=dg.AssetSelection.assets(structured_source_smoke_report),
    )
    structured_source_multisource_smoke_job = dg.define_asset_job(
        "structured_source_multisource_smoke_job",
        selection=dg.AssetSelection.assets(structured_source_multisource_smoke_report),
    )
    research_primitive_source_smoke_job = dg.define_asset_job(
        "research_primitive_source_smoke_job",
        selection=dg.AssetSelection.assets(research_primitive_source_smoke_report),
    )
    research_primitive_source_pipeline_job = dg.define_asset_job(
        "research_primitive_source_pipeline_job",
        selection=dg.AssetSelection.assets(research_primitive_source_pipeline_report),
    )
    literature_clinical_smoke_job = dg.define_asset_job(
        "literature_clinical_smoke_job",
        selection=dg.AssetSelection.assets(literature_clinical_smoke_report),
    )
    all_api_smoke_job = dg.define_asset_job(
        "all_api_smoke_job",
        selection=dg.AssetSelection.assets(all_api_smoke_report),
    )
    literature_corpus_harvest_job = dg.define_asset_job(
        "literature_corpus_harvest_job",
        selection=dg.AssetSelection.assets(literature_corpus_harvest_report),
    )
    literature_corpus_source_date_job = dg.define_asset_job(
        "literature_corpus_source_date_job",
        selection=dg.AssetSelection.assets(literature_corpus_source_date_report),
    )
    literature_full_text_refresh_job = dg.define_asset_job(
        "literature_full_text_refresh_job",
        selection=dg.AssetSelection.assets(literature_full_text_refresh_report),
    )
    europe_pmc_full_text_refresh_job = dg.define_asset_job(
        "europe_pmc_full_text_refresh_job",
        selection=dg.AssetSelection.assets(europe_pmc_full_text_refresh_report),
    )
    pmc_oa_full_text_refresh_job = dg.define_asset_job(
        "pmc_oa_full_text_refresh_job",
        selection=dg.AssetSelection.assets(pmc_oa_full_text_refresh_report),
    )
    literature_full_text_ingest_smoke_job = dg.define_asset_job(
        "literature_full_text_ingest_smoke_job",
        selection=dg.AssetSelection.assets(literature_full_text_ingest_smoke_report),
    )
    europe_pmc_full_text_ingest_job = dg.define_asset_job(
        "europe_pmc_full_text_ingest_job",
        selection=dg.AssetSelection.assets(europe_pmc_full_text_ingest_report),
    )
    pmc_oa_full_text_ingest_job = dg.define_asset_job(
        "pmc_oa_full_text_ingest_job",
        selection=dg.AssetSelection.assets(pmc_oa_full_text_ingest_report),
    )
    literature_full_text_smoke_job = dg.define_asset_job(
        "literature_full_text_smoke_job",
        selection=dg.AssetSelection.assets(literature_full_text_smoke_report),
    )
    literature_full_text_source_date_job = dg.define_asset_job(
        "literature_full_text_source_date_job",
        selection=dg.AssetSelection.assets(literature_full_text_source_date_report),
    )
    structured_source_count_report_job = dg.define_asset_job(
        "structured_source_count_report_job",
        selection=dg.AssetSelection.assets(structured_source_count_report),
    )
    source_health_report_job = dg.define_asset_job(
        "source_health_report_job",
        selection=dg.AssetSelection.assets(source_health_report),
    )
    candidate_contribution_intake_report_job = dg.define_asset_job(
        "candidate_contribution_intake_report_job",
        selection=dg.AssetSelection.assets(candidate_contribution_intake_report),
    )
    candidate_contribution_triage_job = dg.define_asset_job(
        "candidate_contribution_triage_job",
        selection=dg.AssetSelection.assets(candidate_contribution_triage_report),
    )
    work_packet_seed_job = dg.define_asset_job(
        "work_packet_seed_job",
        selection=dg.AssetSelection.assets(work_packet_seed_report),
    )
    proof_capsule_reward_sync_job = dg.define_asset_job(
        "proof_capsule_reward_sync_job",
        selection=dg.AssetSelection.assets(proof_capsule_reward_sync_report),
    )
    command_center_job = dg.define_asset_job(
        "command_center_job",
        selection=dg.AssetSelection.assets(command_center_report),
    )
    research_hunt_queue_report_job = dg.define_asset_job(
        "research_hunt_queue_report_job",
        selection=dg.AssetSelection.assets(research_hunt_queue_report),
    )
    research_hunt_queue_maintenance_job = dg.define_asset_job(
        "research_hunt_queue_maintenance_job",
        selection=dg.AssetSelection.assets(research_hunt_queue_maintenance_report),
    )
    research_hunt_synthesis_queue_job = dg.define_asset_job(
        "research_hunt_synthesis_queue_job",
        selection=dg.AssetSelection.assets(research_hunt_synthesis_queue_report),
    )
    research_hunt_synthesis_doc_job = dg.define_asset_job(
        "research_hunt_synthesis_doc_job",
        selection=dg.AssetSelection.assets(research_hunt_synthesis_doc_report),
    )
    full_text_ops_agent_job = dg.define_asset_job(
        "full_text_ops_agent_job",
        selection=dg.AssetSelection.assets(full_text_ops_agent_report),
    )
    agent_performance_report_job = dg.define_asset_job(
        "agent_performance_report_job",
        selection=dg.AssetSelection.assets(agent_performance_report),
    )
    agent_performance_evaluation_job = dg.define_asset_job(
        "agent_performance_evaluation_job",
        selection=dg.AssetSelection.assets(agent_performance_evaluation_report),
    )
    reward_event_report_job = dg.define_asset_job(
        "reward_event_report_job",
        selection=dg.AssetSelection.assets(reward_event_report),
    )
    reward_event_sync_job = dg.define_asset_job(
        "reward_event_sync_job",
        selection=dg.AssetSelection.assets(reward_event_sync_report),
    )
    capsule_llm_grade_job = dg.define_asset_job(
        "capsule_llm_grade_job",
        selection=dg.AssetSelection.assets(capsule_llm_grade_report),
    )
    research_brief_agent_job = dg.define_asset_job(
        "research_brief_agent_job",
        selection=dg.AssetSelection.assets(research_brief_agent_report),
    )
    therapy_committee_job = dg.define_asset_job(
        "therapy_committee_job",
        selection=dg.AssetSelection.assets(therapy_committee_report),
    )
    validation_tool_catalog_job = dg.define_asset_job(
        "validation_tool_catalog_job",
        selection=dg.AssetSelection.assets(validation_tool_catalog_report),
    )
    validation_tool_match_job = dg.define_asset_job(
        "validation_tool_match_job",
        selection=dg.AssetSelection.assets(validation_tool_match_report),
    )
    therapy_idea_library_job = dg.define_asset_job(
        "therapy_idea_library_job",
        selection=dg.AssetSelection.assets(therapy_idea_library_report),
    )
    public_candidate_snapshot_job = dg.define_asset_job(
        "public_candidate_snapshot_job",
        selection=dg.AssetSelection.assets(public_candidate_snapshot_report),
    )
    public_candidate_integrity_job = dg.define_asset_job(
        "public_candidate_integrity_job",
        selection=dg.AssetSelection.assets(public_candidate_integrity_report),
    )
    hypothesis_promotion_job = dg.define_asset_job(
        "hypothesis_promotion_job",
        selection=dg.AssetSelection.assets(hypothesis_promotion_report),
    )
    validation_packet_job = dg.define_asset_job(
        "validation_packet_job",
        selection=dg.AssetSelection.assets(validation_packet_report),
    )
    validation_decision_job = dg.define_asset_job(
        "validation_decision_job",
        selection=dg.AssetSelection.assets(validation_decision_report),
    )
    research_program_board_job = dg.define_asset_job(
        "research_program_board_job",
        selection=dg.AssetSelection.assets(research_program_board_report),
    )
    research_program_library_job = dg.define_asset_job(
        "research_program_library_job",
        selection=dg.AssetSelection.assets(research_program_library_report),
    )
    research_program_evidence_loop_job = dg.define_asset_job(
        "research_program_evidence_loop_job",
        selection=dg.AssetSelection.assets(research_program_evidence_loop_report),
    )
    omics_accession_hunt_job = dg.define_asset_job(
        "omics_accession_hunt_job",
        selection=dg.AssetSelection.assets(omics_accession_hunt_report),
    )
    omics_evidence_packet_job = dg.define_asset_job(
        "omics_evidence_packet_job",
        selection=dg.AssetSelection.assets(omics_evidence_packet_report),
    )
    omics_readout_job = dg.define_asset_job(
        "omics_readout_job",
        selection=dg.AssetSelection.assets(omics_readout_report),
    )
    omics_locus_signal_job = dg.define_asset_job(
        "omics_locus_signal_job",
        selection=dg.AssetSelection.assets(omics_locus_signal_report),
    )
    omics_followup_job = dg.define_asset_job(
        "omics_followup_job",
        selection=dg.AssetSelection.assets(omics_followup_report),
    )
    therapy_committee_validation_queue_job = dg.define_asset_job(
        "therapy_committee_validation_queue_job",
        selection=dg.AssetSelection.assets(therapy_committee_validation_queue_report),
    )
    research_brief_library_job = dg.define_asset_job(
        "research_brief_library_job",
        selection=dg.AssetSelection.assets(research_brief_library_report),
    )
    research_brief_evaluation_job = dg.define_asset_job(
        "research_brief_evaluation_job",
        selection=dg.AssetSelection.assets(research_brief_evaluation_report),
    )
    research_brief_evaluation_library_job = dg.define_asset_job(
        "research_brief_evaluation_library_job",
        selection=dg.AssetSelection.assets(research_brief_evaluation_library_report),
    )
    research_brief_quality_job = dg.define_asset_job(
        "research_brief_quality_job",
        selection=dg.AssetSelection.assets(research_brief_quality_report),
    )
    research_brief_followup_queue_job = dg.define_asset_job(
        "research_brief_followup_queue_job",
        selection=dg.AssetSelection.assets(research_brief_followup_queue_report),
    )
    validation_plan_job = dg.define_asset_job(
        "validation_plan_job",
        selection=dg.AssetSelection.assets(validation_plan_report),
    )
    validation_plan_library_job = dg.define_asset_job(
        "validation_plan_library_job",
        selection=dg.AssetSelection.assets(validation_plan_library_report),
    )
    validation_request_queue_job = dg.define_asset_job(
        "validation_request_queue_job",
        selection=dg.AssetSelection.assets(validation_request_queue_report),
    )
    validation_request_queue_library_job = dg.define_asset_job(
        "validation_request_queue_library_job",
        selection=dg.AssetSelection.assets(validation_request_queue_library_report),
    )
    md_smoke_compute_job_job = dg.define_asset_job(
        "md_smoke_compute_job_job",
        selection=dg.AssetSelection.assets(md_smoke_compute_job_report),
    )
    compute_job_job = dg.define_asset_job(
        "compute_job_job",
        selection=dg.AssetSelection.assets(compute_job_report),
    )
    md_expert_review_packet_job = dg.define_asset_job(
        "md_expert_review_packet_job",
        selection=dg.AssetSelection.assets(md_expert_review_packet_report),
    )
    md_expert_agent_review_job = dg.define_asset_job(
        "md_expert_agent_review_job",
        selection=dg.AssetSelection.assets(md_expert_agent_review_report),
    )
    validation_autopilot_job = dg.define_asset_job(
        "validation_autopilot_job",
        selection=dg.AssetSelection.assets(validation_autopilot_report),
    )
    research_brief_queue_job = dg.define_asset_job(
        "research_brief_queue_job",
        selection=dg.AssetSelection.assets(research_brief_queue_report),
    )
    research_brief_queue_batch_job = dg.define_asset_job(
        "research_brief_queue_batch_job",
        selection=dg.AssetSelection.assets(research_brief_queue_batch_report),
    )
    research_brief_queue_seed_job = dg.define_asset_job(
        "research_brief_queue_seed_job",
        selection=dg.AssetSelection.assets(research_brief_queue_seed_report),
    )
    research_brief_queue_runner_job = dg.define_asset_job(
        "research_brief_queue_runner_job",
        selection=dg.AssetSelection.assets(research_brief_queue_runner_report),
    )
    research_brief_queue_maintenance_job = dg.define_asset_job(
        "research_brief_queue_maintenance_job",
        selection=dg.AssetSelection.assets(research_brief_queue_maintenance_report),
    )
    research_brief_playground_pack_job = dg.define_asset_job(
        "research_brief_playground_pack_job",
        selection=dg.AssetSelection.assets(research_brief_playground_pack_report),
    )
    research_leads_job = dg.define_asset_job(
        "research_leads_job",
        selection=dg.AssetSelection.assets(research_leads_report),
    )
    evidence_gap_resolver_job = dg.define_asset_job(
        "evidence_gap_resolver_job",
        selection=dg.AssetSelection.assets(evidence_gap_resolver_report),
    )
    validation_gap_source_pack_job = dg.define_asset_job(
        "validation_gap_source_pack_job",
        selection=dg.AssetSelection.assets(validation_gap_source_pack_report),
    )
    pubmed_identifier_repair_job = dg.define_asset_job(
        "pubmed_identifier_repair_job",
        selection=dg.AssetSelection.assets(pubmed_identifier_repair_report),
    )
    validation_gap_source_ingest_job = dg.define_asset_job(
        "validation_gap_source_ingest_job",
        selection=dg.AssetSelection.assets(validation_gap_source_ingest_report),
    )
    validation_gap_completion_job = dg.define_asset_job(
        "validation_gap_completion_job",
        selection=dg.AssetSelection.assets(validation_gap_completion_report),
    )
    research_followup_resolver_job = dg.define_asset_job(
        "research_followup_resolver_job",
        selection=dg.AssetSelection.assets(research_followup_resolver_report),
    )
    x_topic_monitor_review_job = dg.define_asset_job(
        "x_topic_monitor_review_job",
        selection=dg.AssetSelection.assets(x_topic_monitor_review_report),
    )
    x_linked_article_followup_job = dg.define_asset_job(
        "x_linked_article_followup_job",
        selection=dg.AssetSelection.assets(x_linked_article_followup_report),
    )
    x_linked_article_review_job = dg.define_asset_job(
        "x_linked_article_review_job",
        selection=dg.AssetSelection.assets(x_linked_article_review_report),
    )
    source_followup_queue_job = dg.define_asset_job(
        "source_followup_queue_job",
        selection=dg.AssetSelection.assets(source_followup_queue_report),
    )
    source_followup_ingest_job = dg.define_asset_job(
        "source_followup_ingest_job",
        selection=dg.AssetSelection.assets(source_followup_ingest_report),
    )
    pubmed_source_followup_ingest_job = dg.define_asset_job(
        "pubmed_source_followup_ingest_job",
        selection=dg.AssetSelection.assets(pubmed_source_followup_ingest_report),
    )
    crossref_source_followup_ingest_job = dg.define_asset_job(
        "crossref_source_followup_ingest_job",
        selection=dg.AssetSelection.assets(crossref_source_followup_ingest_report),
    )
    pmc_oa_source_followup_ingest_job = dg.define_asset_job(
        "pmc_oa_source_followup_ingest_job",
        selection=dg.AssetSelection.assets(pmc_oa_source_followup_ingest_report),
    )
    clinicaltrials_gov_source_followup_ingest_job = dg.define_asset_job(
        "clinicaltrials_gov_source_followup_ingest_job",
        selection=dg.AssetSelection.assets(clinicaltrials_gov_source_followup_ingest_report),
    )
    unpaywall_source_followup_ingest_job = dg.define_asset_job(
        "unpaywall_source_followup_ingest_job",
        selection=dg.AssetSelection.assets(unpaywall_source_followup_ingest_report),
    )
    full_text_source_date_ops_job = dg.JobDefinition(
        name="full_text_source_date_ops_job",
        graph_def=dg.GraphDefinition(
            name="full_text_source_date_ops_graph",
            node_defs=[full_text_source_date_ops],
        ),
    )
    entity_resolution_job = dg.define_asset_job(
        "entity_resolution_job",
        selection=dg.AssetSelection.assets(entity_resolution_report),
    )
    entity_lookup_index_job = dg.define_asset_job(
        "entity_lookup_index_job",
        selection=dg.AssetSelection.assets(entity_lookup_index_report),
    )
    embedding_index_job = dg.define_asset_job(
        "embedding_index_job",
        selection=dg.AssetSelection.assets(embedding_index_report),
    )
    embedding_maintenance_job = dg.define_asset_job(
        "embedding_maintenance_job",
        selection=dg.AssetSelection.assets(embedding_maintenance_report),
    )
    structured_source_pipeline_weekly_schedule = dg.ScheduleDefinition(
        name="structured_source_pipeline_weekly_schedule",
        job=structured_source_pipeline_job,
        cron_schedule="0 2 * * 1",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    literature_corpus_daily_schedule = dg.ScheduleDefinition(
        name="literature_corpus_daily_schedule",
        job=literature_corpus_harvest_job,
        cron_schedule="0 1 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    literature_full_text_weekly_schedule = dg.ScheduleDefinition(
        name="literature_full_text_weekly_schedule",
        job=literature_full_text_refresh_job,
        cron_schedule="0 2 * * 0",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.STOPPED,
    )
    all_api_smoke_weekly_schedule = dg.ScheduleDefinition(
        name="all_api_smoke_weekly_schedule",
        job=all_api_smoke_job,
        cron_schedule="0 3 * * 2",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    source_followup_queue_daily_schedule = dg.ScheduleDefinition(
        name="source_followup_queue_daily_schedule",
        job=source_followup_queue_job,
        cron_schedule="5 3 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    pubmed_source_followup_ingest_daily_schedule = dg.ScheduleDefinition(
        name="pubmed_source_followup_ingest_daily_schedule",
        job=pubmed_source_followup_ingest_job,
        cron_schedule="20 3 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    crossref_source_followup_ingest_daily_schedule = dg.ScheduleDefinition(
        name="crossref_source_followup_ingest_daily_schedule",
        job=crossref_source_followup_ingest_job,
        cron_schedule="35 3 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    pmc_oa_source_followup_ingest_daily_schedule = dg.ScheduleDefinition(
        name="pmc_oa_source_followup_ingest_daily_schedule",
        job=pmc_oa_source_followup_ingest_job,
        cron_schedule="50 3 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    clinicaltrials_gov_source_followup_ingest_daily_schedule = dg.ScheduleDefinition(
        name="clinicaltrials_gov_source_followup_ingest_daily_schedule",
        job=clinicaltrials_gov_source_followup_ingest_job,
        cron_schedule="5 4 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    unpaywall_source_followup_ingest_daily_schedule = dg.ScheduleDefinition(
        name="unpaywall_source_followup_ingest_daily_schedule",
        job=unpaywall_source_followup_ingest_job,
        cron_schedule="20 4 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    research_leads_daily_schedule = dg.ScheduleDefinition(
        name="research_leads_daily_schedule",
        job=research_leads_job,
        cron_schedule="35 4 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    embedding_index_daily_schedule = dg.ScheduleDefinition(
        name="embedding_index_daily_schedule",
        job=embedding_index_job,
        cron_schedule="0 5 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    embedding_maintenance_daily_schedule = dg.ScheduleDefinition(
        name="embedding_maintenance_daily_schedule",
        job=embedding_maintenance_job,
        cron_schedule="45 5 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    source_health_daily_schedule = dg.ScheduleDefinition(
        name="source_health_daily_schedule",
        job=source_health_report_job,
        cron_schedule="15 6 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    validation_autopilot_hourly_schedule = dg.ScheduleDefinition(
        name="validation_autopilot_hourly_schedule",
        job=validation_autopilot_job,
        cron_schedule="0 * * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.STOPPED,
        run_config={
            "ops": {
                "validation_autopilot_report": {
                    "config": {
                        "enabled": True,
                        "dry_run": False,
                        "force": False,
                        "manual_grace_period_hours": 6.0,
                        "minimum_queue_age_hours": 1.0,
                        "max_per_run": 2,
                        "hourly_budget_usd": 0.25,
                        "daily_budget_usd": 1.50,
                        "estimated_cost_per_item_usd": 0.03,
                        "allowed_task_types": ["expert_review", "target_validation", "omics"],
                        "allowed_validation_types": ["expert_review", "homology", "omics"],
                        "source_keys": [],
                        "model_profile": "openrouter_required",
                    }
                }
            }
        },
    )
    literature_full_text_source_date_daily_schedule = dg.ScheduleDefinition(
        name="literature_full_text_source_date_daily_schedule",
        job=literature_full_text_source_date_job,
        cron_schedule="30 2 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
        execution_fn=_full_text_source_date_daily_schedule_requests,
    )

    defs = dg.Definitions(
        assets=ingestion_bridge_assets,
        asset_checks=[
            source_registry_has_phase_one_sources,
            structured_source_pipeline_has_minimum_outputs,
            structured_source_smoke_has_minimum_outputs,
            structured_source_multisource_smoke_has_minimum_outputs,
            literature_clinical_smoke_has_minimum_outputs,
            all_api_smoke_has_minimum_outputs,
            literature_corpus_harvest_has_hundreds_of_records,
            literature_full_text_refresh_has_outputs,
            europe_pmc_full_text_refresh_has_outputs,
            pmc_oa_full_text_refresh_has_outputs,
            literature_full_text_ingest_smoke_has_outputs,
            europe_pmc_full_text_ingest_has_outputs,
            pmc_oa_full_text_ingest_has_outputs,
            literature_full_text_smoke_has_outputs,
            literature_full_text_source_date_has_outputs,
            structured_source_count_report_has_minimum_outputs,
            source_health_report_has_no_failed_sources,
            entity_resolution_has_minimum_outputs,
            embedding_index_has_minimum_outputs,
            embedding_maintenance_has_clean_coverage,
        ],
        jobs=[
            structured_source_pipeline_job,
            structured_source_smoke_job,
            structured_source_multisource_smoke_job,
            research_primitive_source_smoke_job,
            research_primitive_source_pipeline_job,
            literature_clinical_smoke_job,
            all_api_smoke_job,
            literature_corpus_harvest_job,
            literature_corpus_source_date_job,
            literature_full_text_refresh_job,
            europe_pmc_full_text_refresh_job,
            pmc_oa_full_text_refresh_job,
            literature_full_text_ingest_smoke_job,
            europe_pmc_full_text_ingest_job,
            pmc_oa_full_text_ingest_job,
            literature_full_text_smoke_job,
            literature_full_text_source_date_job,
            structured_source_count_report_job,
            source_health_report_job,
            candidate_contribution_intake_report_job,
            candidate_contribution_triage_job,
            work_packet_seed_job,
            proof_capsule_reward_sync_job,
            command_center_job,
            research_hunt_queue_report_job,
            research_hunt_queue_maintenance_job,
            research_hunt_synthesis_queue_job,
            research_hunt_synthesis_doc_job,
            full_text_ops_agent_job,
            agent_performance_report_job,
            agent_performance_evaluation_job,
            reward_event_report_job,
            reward_event_sync_job,
            capsule_llm_grade_job,
            research_brief_agent_job,
            therapy_committee_job,
            validation_tool_catalog_job,
            validation_tool_match_job,
            therapy_idea_library_job,
            public_candidate_snapshot_job,
            public_candidate_integrity_job,
            hypothesis_promotion_job,
            validation_packet_job,
            validation_decision_job,
            research_program_board_job,
            research_program_library_job,
            research_program_evidence_loop_job,
            omics_accession_hunt_job,
            omics_evidence_packet_job,
            omics_readout_job,
            omics_locus_signal_job,
            omics_followup_job,
            therapy_committee_validation_queue_job,
            research_brief_library_job,
            research_brief_evaluation_job,
            research_brief_evaluation_library_job,
            research_brief_quality_job,
            research_brief_followup_queue_job,
            validation_plan_job,
            validation_plan_library_job,
            validation_request_queue_job,
            validation_request_queue_library_job,
            md_smoke_compute_job_job,
            compute_job_job,
            md_expert_review_packet_job,
            md_expert_agent_review_job,
            validation_autopilot_job,
            research_brief_queue_job,
            research_brief_queue_batch_job,
            research_brief_queue_seed_job,
            research_brief_queue_runner_job,
            research_brief_queue_maintenance_job,
            research_brief_playground_pack_job,
            research_leads_job,
            evidence_gap_resolver_job,
            validation_gap_source_pack_job,
            pubmed_identifier_repair_job,
            validation_gap_source_ingest_job,
            validation_gap_completion_job,
            research_followup_resolver_job,
            x_topic_monitor_review_job,
            x_linked_article_followup_job,
            x_linked_article_review_job,
            source_followup_queue_job,
            source_followup_ingest_job,
            pubmed_source_followup_ingest_job,
            crossref_source_followup_ingest_job,
            pmc_oa_source_followup_ingest_job,
            clinicaltrials_gov_source_followup_ingest_job,
            unpaywall_source_followup_ingest_job,
            full_text_source_date_ops_job,
            entity_resolution_job,
            entity_lookup_index_job,
            embedding_index_job,
            embedding_maintenance_job,
        ],
        schedules=[
            structured_source_pipeline_weekly_schedule,
            literature_corpus_daily_schedule,
            literature_full_text_weekly_schedule,
            all_api_smoke_weekly_schedule,
            source_followup_queue_daily_schedule,
            pubmed_source_followup_ingest_daily_schedule,
            crossref_source_followup_ingest_daily_schedule,
            pmc_oa_source_followup_ingest_daily_schedule,
            clinicaltrials_gov_source_followup_ingest_daily_schedule,
            unpaywall_source_followup_ingest_daily_schedule,
            research_leads_daily_schedule,
            embedding_index_daily_schedule,
            embedding_maintenance_daily_schedule,
            source_health_daily_schedule,
            validation_autopilot_hourly_schedule,
            literature_full_text_source_date_daily_schedule,
        ],
        resources={
            "research_repository": ResearchRepositoryResource(),
        },
    )

else:
    ingestion_bridge_assets = []
    structured_source_pipeline_job = None
    structured_source_smoke_job = None
    structured_source_multisource_smoke_job = None
    research_primitive_source_smoke_job = None
    research_primitive_source_pipeline_job = None
    literature_clinical_smoke_job = None
    all_api_smoke_job = None
    literature_corpus_harvest_job = None
    literature_corpus_source_date_job = None
    literature_full_text_refresh_job = None
    europe_pmc_full_text_refresh_job = None
    pmc_oa_full_text_refresh_job = None
    literature_full_text_ingest_smoke_job = None
    europe_pmc_full_text_ingest_job = None
    pmc_oa_full_text_ingest_job = None
    literature_full_text_smoke_job = None
    literature_full_text_source_date_job = None
    structured_source_count_report_job = None
    source_health_report_job = None
    candidate_contribution_intake_report_job = None
    candidate_contribution_triage_job = None
    work_packet_seed_job = None
    proof_capsule_reward_sync_job = None
    command_center_job = None
    research_hunt_queue_report_job = None
    research_hunt_queue_maintenance_job = None
    research_hunt_synthesis_queue_job = None
    research_hunt_synthesis_doc_job = None
    full_text_ops_agent_job = None
    agent_performance_report_job = None
    agent_performance_evaluation_job = None
    reward_event_report_job = None
    reward_event_sync_job = None
    research_brief_agent_job = None
    therapy_committee_job = None
    validation_tool_catalog_job = None
    validation_tool_match_job = None
    therapy_idea_library_job = None
    public_candidate_snapshot_job = None
    public_candidate_integrity_job = None
    hypothesis_promotion_job = None
    validation_packet_job = None
    validation_decision_job = None
    research_program_board_job = None
    research_program_library_job = None
    research_program_evidence_loop_job = None
    omics_accession_hunt_job = None
    omics_evidence_packet_job = None
    omics_readout_job = None
    omics_locus_signal_job = None
    omics_followup_job = None
    therapy_committee_validation_queue_job = None
    research_brief_library_job = None
    research_brief_evaluation_job = None
    research_brief_evaluation_library_job = None
    research_brief_quality_job = None
    research_brief_followup_queue_job = None
    validation_plan_job = None
    validation_plan_library_job = None
    validation_request_queue_job = None
    validation_request_queue_library_job = None
    md_smoke_compute_job_job = None
    compute_job_job = None
    md_expert_review_packet_job = None
    md_expert_agent_review_job = None
    validation_autopilot_job = None
    research_brief_queue_job = None
    research_brief_queue_batch_job = None
    research_brief_queue_seed_job = None
    research_brief_queue_runner_job = None
    research_brief_queue_maintenance_job = None
    research_brief_playground_pack_job = None
    research_leads_job = None
    evidence_gap_resolver_job = None
    validation_gap_source_pack_job = None
    pubmed_identifier_repair_job = None
    validation_gap_source_ingest_job = None
    validation_gap_completion_job = None
    research_followup_resolver_job = None
    x_topic_monitor_review_job = None
    x_linked_article_followup_job = None
    x_linked_article_review_job = None
    source_followup_queue_job = None
    source_followup_ingest_job = None
    pubmed_source_followup_ingest_job = None
    crossref_source_followup_ingest_job = None
    pmc_oa_source_followup_ingest_job = None
    clinicaltrials_gov_source_followup_ingest_job = None
    unpaywall_source_followup_ingest_job = None
    full_text_source_date_ops_job = None
    entity_resolution_job = None
    entity_lookup_index_job = None
    embedding_index_job = None
    embedding_maintenance_job = None
    structured_source_pipeline_weekly_schedule = None
    literature_corpus_daily_schedule = None
    literature_full_text_weekly_schedule = None
    literature_full_text_source_date_daily_schedule = None
    all_api_smoke_weekly_schedule = None
    source_followup_queue_daily_schedule = None
    pubmed_source_followup_ingest_daily_schedule = None
    crossref_source_followup_ingest_daily_schedule = None
    pmc_oa_source_followup_ingest_daily_schedule = None
    clinicaltrials_gov_source_followup_ingest_daily_schedule = None
    unpaywall_source_followup_ingest_daily_schedule = None
    research_leads_daily_schedule = None
    embedding_index_daily_schedule = None
    embedding_maintenance_daily_schedule = None
    source_health_daily_schedule = None
    validation_autopilot_hourly_schedule = None
    defs = None


def _has_minimum_ingested_source_outputs(report: dict) -> bool:
    qa = report.get("qa", {})
    return all(qa.get(field, 0) >= 1 for field in ("raw_records", "research_objects", "document_chunks", "claims"))


def _has_minimum_source_ingestion_outputs(report: dict) -> bool:
    qa = report.get("qa", {})
    return all(qa.get(field, 0) >= 1 for field in ("raw_records", "research_objects", "document_chunks"))


def _has_required_full_text_outputs(report: dict) -> bool:
    full_text_qa = report.get("full_text_qa")
    return full_text_qa is None or bool(full_text_qa.get("passes_full_text_bar"))
