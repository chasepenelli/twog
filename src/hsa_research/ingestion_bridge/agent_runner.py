"""Durable execution wrapper for deterministic and future model-backed agents."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel

from .contracts import AgentRunRecord, RunManifestRecord, RunStatus
from .repository import ResearchRepository


T = TypeVar("T")


class AgentRunner:
    """Persist an agent run before and after executing a typed agent callable."""

    def __init__(self, repository: ResearchRepository) -> None:
        self.repository = repository

    def run(
        self,
        *,
        agent_name: str,
        agent_version: str = "v1",
        model_profile: str = "deterministic",
        input_payload: dict[str, Any],
        execute: Callable[[], T],
        source_key: str | None = None,
        partition_date: str | None = None,
        dagster_run_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        trace_id: UUID | str | None = None,
        summarize: Callable[[T], dict[str, Any]] | None = None,
    ) -> T:
        normalized_metadata = metadata or {}
        normalized_trace_id = _select_trace_id(
            trace_id,
            normalized_metadata.get("trace_id"),
            input_payload.get("trace_id"),
        )
        record = AgentRunRecord(
            trace_id=normalized_trace_id,
            agent_name=agent_name,
            agent_version=agent_version,
            model_profile=model_profile,
            status=RunStatus.RUNNING,
            source_key=source_key,
            partition_date=partition_date,
            dagster_run_id=dagster_run_id,
            input_payload=input_payload,
            metadata={**normalized_metadata, "trace_id": str(normalized_trace_id)},
        )
        self.repository.create_agent_run(record)
        manifest = RunManifestRecord(
            trace_id=normalized_trace_id,
            manifest_type="agent_run",
            status="running",
            title=f"{agent_name} {agent_version}",
            created_by="agent_runner",
            dagster_run_id=dagster_run_id,
            agent_run_ids=[record.agent_run_id],
            model_profiles=[model_profile],
            input_refs={
                "source_key": source_key,
                "partition_date": partition_date,
            },
            metadata={
                "agent_name": agent_name,
                "agent_version": agent_version,
            },
        )
        self.repository.upsert_run_manifest(manifest)
        try:
            result = execute()
            result = _attach_agent_run_id(result, record.agent_run_id)
            output_payload = _payload(result)
            summary = summarize(result) if summarize else _default_summary(result)
            self.repository.finish_agent_run(
                record.agent_run_id,
                status=RunStatus.COMPLETED.value,
                output_payload=output_payload,
                summary=summary,
                errors=[],
            )
            self.repository.upsert_run_manifest(
                manifest.model_copy(
                    update={
                        "status": "completed",
                        "updated_at": datetime.now(UTC),
                        "output_refs": _manifest_output_refs(output_payload, summary),
                    }
                )
            )
            return result
        except Exception as exc:
            self.repository.finish_agent_run(
                record.agent_run_id,
                status=RunStatus.FAILED.value,
                output_payload={},
                summary={"error_type": type(exc).__name__},
                errors=[str(exc)],
            )
            self.repository.upsert_run_manifest(
                manifest.model_copy(
                    update={
                        "status": "failed",
                        "updated_at": datetime.now(UTC),
                        "errors": [str(exc)],
                        "output_refs": {"error_type": type(exc).__name__},
                    }
                )
            )
            raise


def _payload(value: Any) -> dict[str, Any]:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return value
    return {"value": value}


def _attach_agent_run_id(value: T, agent_run_id) -> T:
    if isinstance(value, BaseModel) and "agent_run_id" in value.__class__.model_fields:
        return value.model_copy(update={"agent_run_id": agent_run_id})  # type: ignore[return-value]
    return value


def _default_summary(value: Any) -> dict[str, Any]:
    payload = _payload(value)
    return {
        key: payload[key]
        for key in ("agent_name", "source_key", "action", "severity", "should_block_schedule")
        if key in payload
    }


def _select_trace_id(*values: Any) -> UUID:
    for value in values:
        if isinstance(value, UUID):
            return value
        if isinstance(value, str) and value.strip():
            try:
                return UUID(value.strip())
            except ValueError:
                continue
    return uuid4()


def _manifest_output_refs(output_payload: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    refs: dict[str, Any] = {}
    for key in (
        "agent_run_id",
        "brief_id",
        "evaluation_id",
        "therapy_idea_id",
        "program_id",
        "candidate_id",
        "compute_job_id",
        "created_count",
        "errors",
    ):
        if key in output_payload:
            refs[key] = output_payload[key]
        elif key in summary:
            refs[key] = summary[key]
    return refs
