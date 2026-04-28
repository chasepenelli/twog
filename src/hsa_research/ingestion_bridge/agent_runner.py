"""Durable execution wrapper for deterministic and future model-backed agents."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from pydantic import BaseModel

from .contracts import AgentRunRecord, RunStatus
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
        summarize: Callable[[T], dict[str, Any]] | None = None,
    ) -> T:
        record = AgentRunRecord(
            agent_name=agent_name,
            agent_version=agent_version,
            model_profile=model_profile,
            status=RunStatus.RUNNING,
            source_key=source_key,
            partition_date=partition_date,
            dagster_run_id=dagster_run_id,
            input_payload=input_payload,
            metadata=metadata or {},
        )
        self.repository.create_agent_run(record)
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
            return result
        except Exception as exc:
            self.repository.finish_agent_run(
                record.agent_run_id,
                status=RunStatus.FAILED.value,
                output_payload={},
                summary={"error_type": type(exc).__name__},
                errors=[str(exc)],
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
