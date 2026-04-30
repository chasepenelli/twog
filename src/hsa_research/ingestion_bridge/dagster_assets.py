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

from .contracts import (
    ClaimCurationRequest,
    ClaimSearchRequest,
    FullTextOpsRequest,
    ResearchBriefQueueRunRequest,
    ResearchBriefRequest,
    ResearchLeadCollectRequest,
    ResearchObject,
    SourceFollowupIngestRequest,
    SourceFollowupQueueRequest,
    SourceQuery,
    SourceScoutRequest,
    XLinkedArticleReviewRequest,
    XLinkedArticleFollowupRequest,
    XTopicReviewRequest,
)
from .query_policy import (
    build_canine_data_source_queries,
    build_chemistry_source_queries,
    build_clinical_trial_source_queries,
    build_omics_source_queries,
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
    "error_count",
    "created_at",
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

    def _research_brief_library_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        rows = report.get("briefs", [])
        return {
            "brief_count": dg.MetadataValue.int(int(report.get("brief_count", 0))),
            "status": report.get("status"),
            "source_key": report.get("source_key"),
            "topic_query": report.get("topic_query"),
            "briefs": _metadata_table(rows, _RESEARCH_BRIEF_LIBRARY_TABLE_COLUMNS),
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

    def _research_brief_queue_runner_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
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

    @dg.asset(group_name="agent_ops")
    def full_text_ops_agent_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Recommend-only full-text ops agent report over persisted hosted state."""

        from .service import HSAResearchService

        repository = research_repository.build_repository()
        result = HSAResearchService(repository).run_full_text_ops(FullTextOpsRequest())
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(value=report, metadata=_full_text_ops_report_metadata(report))

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
        },
    )
    def research_brief_queue_runner_report(
        context,
        research_repository: ResearchRepositoryResource,
    ) -> dg.MaterializeResult:
        """Run the next queued research brief request and persist the output."""

        from .service import HSAResearchService

        config = context.op_config
        repository = research_repository.build_repository()
        result = HSAResearchService(repository).run_next_research_brief_queue_item(
            ResearchBriefQueueRunRequest(
                statuses=config["statuses"],
                source_key=config.get("source_key"),
                topic_query=config.get("topic_query"),
                limit=config["limit"],
                dagster_run_id=context.run_id,
            )
        )
        report = result.model_dump(mode="json")
        return dg.MaterializeResult(
            value=report,
            metadata=_research_brief_queue_runner_metadata(report),
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
                    "deterministic_only",
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
                default_value="external_required",
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

    @dg.asset(group_name="embedding_index")
    def embedding_index_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Deterministic local embedding index over persisted document chunks."""

        from .embeddings import index_embeddings_for_repository

        repository = research_repository.build_repository()
        result = index_embeddings_for_repository(repository)
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

        from .embeddings import maintain_embedding_index

        repository = research_repository.build_repository()
        report = maintain_embedding_index(repository).to_report()
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
        full_text_ops_agent_report,
        research_brief_agent_report,
        research_brief_library_report,
        research_brief_queue_report,
        research_brief_queue_runner_report,
        research_brief_playground_pack_report,
        research_leads_report,
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
    full_text_ops_agent_job = dg.define_asset_job(
        "full_text_ops_agent_job",
        selection=dg.AssetSelection.assets(full_text_ops_agent_report),
    )
    research_brief_agent_job = dg.define_asset_job(
        "research_brief_agent_job",
        selection=dg.AssetSelection.assets(research_brief_agent_report),
    )
    research_brief_library_job = dg.define_asset_job(
        "research_brief_library_job",
        selection=dg.AssetSelection.assets(research_brief_library_report),
    )
    research_brief_queue_job = dg.define_asset_job(
        "research_brief_queue_job",
        selection=dg.AssetSelection.assets(research_brief_queue_report),
    )
    research_brief_queue_runner_job = dg.define_asset_job(
        "research_brief_queue_runner_job",
        selection=dg.AssetSelection.assets(research_brief_queue_runner_report),
    )
    research_brief_playground_pack_job = dg.define_asset_job(
        "research_brief_playground_pack_job",
        selection=dg.AssetSelection.assets(research_brief_playground_pack_report),
    )
    research_leads_job = dg.define_asset_job(
        "research_leads_job",
        selection=dg.AssetSelection.assets(research_leads_report),
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
            full_text_ops_agent_job,
            research_brief_agent_job,
            research_brief_library_job,
            research_brief_queue_job,
            research_brief_queue_runner_job,
            research_brief_playground_pack_job,
            research_leads_job,
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
    full_text_ops_agent_job = None
    research_brief_agent_job = None
    research_brief_library_job = None
    research_brief_queue_job = None
    research_brief_queue_runner_job = None
    research_brief_playground_pack_job = None
    research_leads_job = None
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
