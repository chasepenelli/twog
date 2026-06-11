"""Compute runner seam for approval-first validation jobs.

The RunPod execution provider was REMOVED — it never worked reliably (the worker
failed in ligand preparation; see docs/DAGSTER_REVIEW.md §4 and ROADMAP.md P3).
A replacement will be built from scratch or use a different tool.

What remains here is the provider-agnostic *seam*: a ComputeRunner protocol plus a
registry. The expert gate, validation queue, proof-capsule model, and compute-job
ledger are unchanged and provider-independent. Until a provider is registered,
get_compute_runner() raises ComputeRunnerConfigError, so no job can silently
execute — submission is blocked safely, not run against a dead endpoint.

To add a provider (ROADMAP P3): implement ComputeRunner and call
register_compute_runner("<kind>", factory).
"""

from __future__ import annotations

from typing import Any, Callable, Protocol, runtime_checkable

from .contracts import ComputeJobRecord


class ComputeRunnerConfigError(RuntimeError):
    """Raised when no compute provider is configured (or a provider lacks config)."""


class ComputeRunnerRequestError(RuntimeError):
    """Raised when a registered compute provider's request fails."""


@runtime_checkable
class ComputeRunner(Protocol):
    """Provider seam. Each method returns a dict carrying at least a ``status`` key.

    submit(record) -> {status, external_run_id, runpod_job_id?, output_payload, metadata}
    poll(record)   -> {status, output_payload, last_error, metadata}
    cancel(record) -> {status, output_payload, metadata}

    Checkpointing (Phase 3a affordance): for long lanes (record.checkpoint_uri set), a provider
    should periodically write restart state to record.checkpoint_uri (durable storage, not the
    ephemeral GPU disk) and may report record.progress_fraction; on resume (record.resume_from_
    checkpoint) it continues from that checkpoint. submit/poll may return status "paused".
    """

    def submit(self, record: ComputeJobRecord) -> dict[str, Any]: ...
    def poll(self, record: ComputeJobRecord) -> dict[str, Any]: ...
    def cancel(self, record: ComputeJobRecord) -> dict[str, Any]: ...


ComputeRunnerFactory = Callable[[], ComputeRunner]

_PROVIDERS: dict[str, ComputeRunnerFactory] = {}


def register_compute_runner(runner_kind: str, factory: ComputeRunnerFactory) -> None:
    """Register a compute provider factory for a given ``runner_kind``."""
    _PROVIDERS[runner_kind] = factory


def available_compute_runners() -> tuple[str, ...]:
    """Return the runner_kinds with a registered provider (empty until P3)."""
    return tuple(sorted(_PROVIDERS))


def get_compute_runner(record: ComputeJobRecord) -> ComputeRunner:
    """Resolve the provider for ``record.runner_kind`` or raise if none is registered."""
    factory = _PROVIDERS.get(record.runner_kind)
    if factory is None:
        raise ComputeRunnerConfigError(
            f"compute_provider_not_configured: no '{record.runner_kind}' compute provider is "
            "registered. The RunPod provider was removed (non-functional); implement a "
            "ComputeRunner and register it via register_compute_runner() — see ROADMAP.md P3."
        )
    return factory()


class MockComputeRunner:
    """Deterministic in-process provider for proving the Phase-2 loop without any GPU.

    Selected explicitly via runner_kind="mock". submit() returns a 'completed' result whose
    output_payload conforms to the compute-artifact shape the capsule builder reads. It does NOT
    run any real computation — never use it for scientific results."""

    def submit(self, record: ComputeJobRecord) -> dict[str, Any]:
        run_id = f"mock:{record.compute_job_id}"
        return {
            "status": "completed",
            "external_run_id": run_id,
            "runpod_job_id": run_id,
            "output_payload": {
                "provider": "mock",
                "findings": f"Mock compute completed for '{record.title}' ({record.validation_type or 'compute'}).",
                "limitations": ["Mock provider output; not scientifically meaningful."],
                "source_refs": [],
                "metrics": {"mock": True},
                # Honest mock: no real computation, so the directional signal is neutral/low-confidence.
                "signal": "neutral",
                "confidence": 0.0,
            },
            "metadata": {"provider": "mock"},
        }

    def poll(self, record: ComputeJobRecord) -> dict[str, Any]:
        return {
            "status": "completed",
            "output_payload": record.output_payload or {"provider": "mock"},
            "last_error": None,
            "metadata": {"provider": "mock"},
        }

    def cancel(self, record: ComputeJobRecord) -> dict[str, Any]:
        return {"status": "cancelled", "output_payload": {}, "metadata": {"provider": "mock"}}


register_compute_runner("mock", lambda: MockComputeRunner())


def _extract_lane_config(record: ComputeJobRecord, key: str) -> dict[str, Any] | None:
    """Find a lane's config on a compute job (it rides with the validation request under `key`)."""
    payload = record.input_payload if isinstance(record.input_payload, dict) else {}
    candidates = [
        payload.get(key),
        (payload.get("validation_request") or {}).get("metadata", {}).get(key)
        if isinstance(payload.get("validation_request"), dict)
        else None,
        record.metadata.get(key) if isinstance(record.metadata, dict) else None,
    ]
    for cfg in candidates:
        if isinstance(cfg, dict):
            return cfg
    return None


def _extract_omics_config(record: ComputeJobRecord) -> dict[str, Any] | None:
    return _extract_lane_config(record, "omics_review")


class LocalOmicsComputeRunner:
    """In-process CPU provider for the omics-review lane (runner_kind="local"). Runs the real
    analysis engine (omics_review.run_omics_review) on the job's provided expression+strata. The
    multi-GB SRA pull is a separate deploy-time seam (omics_review.load_omics_dataset)."""

    def submit(self, record: ComputeJobRecord) -> dict[str, Any]:
        from .omics_review import load_omics_dataset, run_omics_review

        run_id = f"local:{record.compute_job_id}"
        if record.validation_type != "omics":
            raise ComputeRunnerConfigError(
                f"local runner only handles validation_type='omics', got '{record.validation_type}'."
            )
        config = _extract_omics_config(record)

        def _fail(error: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
            return {
                "status": "failed",
                "external_run_id": run_id,
                "runpod_job_id": run_id,
                "output_payload": {"provider": "local_omics", "error": error, **(extra or {})},
                "metadata": {"provider": "local_omics"},
            }

        if config is None:
            return _fail("omics_review_config_missing")
        expression = config.get("expression")
        strata = config.get("strata")
        if not expression or not strata:
            # No inline data → load from a real matrix+strata file if provided, else surface the
            # honest gap (Megquier raw-SRA needs an alignment pipeline + PDF genotype parse).
            try:
                expression, strata = load_omics_dataset(
                    config.get("datasets") or [],
                    matrix_path=config.get("matrix_path"),
                    strata_path=config.get("strata_path"),
                )
            except (NotImplementedError, OSError) as exc:
                return _fail("real_data_pull_not_wired", {"detail": str(exc), "datasets": config.get("datasets")})

        result = run_omics_review(
            expression=expression,
            strata=strata,
            signatures=config.get("signatures"),
            direction_hypothesis=config.get("direction_hypothesis", "immunosuppression_higher_in_mutant"),
            min_n_per_stratum=int(config.get("min_n_per_stratum", 5)),
            source_refs=config.get("source_refs"),
        )
        return {
            "status": "completed",
            "external_run_id": run_id,
            "runpod_job_id": run_id,
            "output_payload": {"provider": "local_omics", **result},
            "metadata": {"provider": "local_omics"},
        }

    def poll(self, record: ComputeJobRecord) -> dict[str, Any]:
        return {
            "status": "completed",
            "output_payload": record.output_payload or {"provider": "local_omics"},
            "last_error": None,
            "metadata": {"provider": "local_omics"},
        }

    def cancel(self, record: ComputeJobRecord) -> dict[str, Any]:
        return {"status": "cancelled", "output_payload": {}, "metadata": {"provider": "local_omics"}}


register_compute_runner("local", lambda: LocalOmicsComputeRunner())


class CheckpointingComputeRunner:
    """Deterministic provider that proves the checkpoint/pause/resume loop (runner_kind="checkpoint")
    WITHOUT a GPU — the analogue of MockComputeRunner for long, resumable jobs.

    A long job advances by a fixed fraction per submit (config 'checkpoint' -> {"step": 0.34}). While
    progress < 1.0 it returns status="paused" plus a checkpoint (progress_fraction + checkpoint_uri +
    inline restart state, simulating durable storage). On the next submit, if resume_from_checkpoint
    is set it continues from record.progress_fraction; otherwise it starts fresh at 0. When progress
    reaches 1.0 it returns a "completed" compute-artifact result. No real computation — this exists to
    prove the mechanism (and the lease-handoff "resume a 40% session" model) end-to-end."""

    def submit(self, record: ComputeJobRecord) -> dict[str, Any]:
        config = _extract_lane_config(record, "checkpoint") or {}
        step = float(config.get("step", 0.34))
        run_id = f"checkpoint:{record.compute_job_id}"
        prev = (record.progress_fraction or 0.0) if record.resume_from_checkpoint else 0.0
        progress = round(min(1.0, prev + step), 4)
        if progress < 1.0:
            return {
                "status": "paused",
                "external_run_id": run_id,
                "runpod_job_id": run_id,
                "output_payload": {"provider": "checkpoint", "progress": progress, "partial": True},
                "progress_fraction": progress,
                "checkpoint_uri": f"memory://checkpoint/{record.compute_job_id}/{progress}",
                "checkpoint_state": {"progress": progress, "resumed_from": prev},
                "metadata": {"provider": "checkpoint", "paused_at_fraction": progress},
            }
        return {
            "status": "completed",
            "external_run_id": run_id,
            "runpod_job_id": run_id,
            "output_payload": {
                "provider": "checkpoint",
                "findings": f"Checkpointed job '{record.title}' completed after resuming from {prev:.2f}.",
                "limitations": ["Checkpoint mock provider; not scientifically meaningful."],
                "source_refs": [],
                "metrics": {"checkpointed": True, "final_progress": 1.0},
                "signal": "neutral",
                "confidence": 0.0,
            },
            "progress_fraction": 1.0,
            "metadata": {"provider": "checkpoint"},
        }

    def poll(self, record: ComputeJobRecord) -> dict[str, Any]:
        return {
            "status": record.status,
            "output_payload": record.output_payload or {"provider": "checkpoint"},
            "last_error": None,
            "metadata": {"provider": "checkpoint"},
        }

    def cancel(self, record: ComputeJobRecord) -> dict[str, Any]:
        return {"status": "cancelled", "output_payload": {}, "metadata": {"provider": "checkpoint"}}


register_compute_runner("checkpoint", lambda: CheckpointingComputeRunner())


def _call_modal_omics(config: dict[str, Any]) -> dict[str, Any]:
    """Run the omics analysis on Modal cloud CPU (imports modal + the app lazily, ephemeral run).

    Isolated so tests can monkeypatch it — the adapter logic is verified without billing Modal;
    the real cloud execution is verified by running modal_app.py / the flow with runner_kind="modal".
    """
    from .modal_app import app, run_omics_review_remote

    with app.run():
        return run_omics_review_remote.remote(config)


def _call_modal_gnina(config: dict[str, Any]) -> dict[str, Any]:
    """Run gnina docking on Modal cloud GPU (lazy import; ephemeral run). Mockable in tests."""
    from .modal_app import app, run_gnina_remote

    with app.run():
        return run_gnina_remote.remote(config)


def _call_modal_md_checkpoint(config: dict[str, Any]) -> dict[str, Any]:
    """Run one bounded chunk of GPU MD with durable checkpointing on Modal (lazy import; ephemeral
    run). Returns a paused/completed result with progress + checkpoint pointer. Mockable in tests."""
    from .modal_app import app, run_md_checkpoint_remote

    with app.run():
        return run_md_checkpoint_remote.remote(config)


# validation_type -> (config key, the _call_modal_* function NAME). The name is resolved against
# the module globals at CALL TIME (not captured here) so tests can monkeypatch the call, and so a
# bug can't silently bind the real cloud call.
_MODAL_LANES: dict[str, tuple[str, str]] = {
    "omics": ("omics_review", "_call_modal_omics"),
    "docking": ("docking", "_call_modal_gnina"),
}


class ModalComputeRunner:
    """Modal cloud provider (runner_kind="modal"). Dispatches by validation_type: omics -> CPU
    analysis, docking -> gnina on GPU. New lanes register in _MODAL_LANES. The modal SDK is imported
    lazily (via the per-lane _call_modal_* fns) so twog never hard-depends on it."""

    def submit(self, record: ComputeJobRecord) -> dict[str, Any]:
        run_id = f"modal:{record.compute_job_id}"

        def _fail(error: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
            return {
                "status": "failed",
                "external_run_id": run_id,
                "runpod_job_id": run_id,
                "output_payload": {"provider": "modal", "error": error, **(extra or {})},
                "metadata": {"provider": "modal"},
            }

        lane = _MODAL_LANES.get(record.validation_type or "")
        if lane is None:
            raise ComputeRunnerConfigError(
                f"modal runner has no lane for validation_type='{record.validation_type}' "
                f"(have: {sorted(_MODAL_LANES)})."
            )
        config_key, call_name = lane
        config = _extract_lane_config(record, config_key)
        if config is None:
            return _fail(f"modal_{record.validation_type}_config_missing")
        call = globals()[call_name]  # resolve at call time -> monkeypatch-able; never auto-binds real cloud
        try:
            result = call(config)
        except Exception as exc:  # network/auth/import/remote errors surface as a failed job
            return _fail("modal_run_failed", {"detail": str(exc)[:500]})
        return {
            "status": "completed",
            "external_run_id": run_id,
            "runpod_job_id": run_id,
            "output_payload": {"provider": "modal", **result},
            "metadata": {"provider": "modal"},
        }

    def poll(self, record: ComputeJobRecord) -> dict[str, Any]:
        return {
            "status": "completed",
            "output_payload": record.output_payload or {"provider": "modal"},
            "last_error": None,
            "metadata": {"provider": "modal"},
        }

    def cancel(self, record: ComputeJobRecord) -> dict[str, Any]:
        return {"status": "cancelled", "output_payload": {}, "metadata": {"provider": "modal"}}


register_compute_runner("modal", lambda: ModalComputeRunner())


class ModalCheckpointComputeRunner:
    """Real-GPU checkpointing provider (runner_kind="modal_checkpoint"). Runs a bounded chunk of GPU
    MD on Modal, persisting an OpenMM checkpoint to a persistent Modal Volume; returns status="paused"
    until the step budget is reached, then "completed". On resume (record.resume_from_checkpoint) the
    remote loads the durable checkpoint and continues from where it stopped — true cross-invocation
    pause/resume that the service's resume_compute_job + workspace lease drive. The modal call is
    resolved at call time so tests can monkeypatch it without billing GPU."""

    def submit(self, record: ComputeJobRecord) -> dict[str, Any]:
        run_id = f"modal_md:{record.compute_job_id}"
        cfg = _extract_lane_config(record, "md_checkpoint") or {}
        config = {
            **cfg,
            "job_id": str(record.compute_job_id),
            "resume": bool(record.resume_from_checkpoint),
        }
        call = globals()["_call_modal_md_checkpoint"]  # resolved at call time -> monkeypatch-able
        try:
            result = call(config)
        except Exception as exc:
            return {
                "status": "failed",
                "external_run_id": run_id,
                "runpod_job_id": run_id,
                "output_payload": {"provider": "modal_md_checkpoint", "error": "modal_md_failed",
                                   "detail": str(exc)[:500]},
                "metadata": {"provider": "modal_md_checkpoint"},
            }
        # the remote returns the full submit-shape (status + progress_fraction + checkpoint_uri +
        # output_payload + metadata); submit_compute_job persists the checkpoint affordances.
        result.setdefault("external_run_id", run_id)
        result.setdefault("runpod_job_id", run_id)
        return result

    def poll(self, record: ComputeJobRecord) -> dict[str, Any]:
        return {
            "status": record.status,
            "output_payload": record.output_payload or {"provider": "modal_md_checkpoint"},
            "last_error": None,
            "metadata": {"provider": "modal_md_checkpoint"},
        }

    def cancel(self, record: ComputeJobRecord) -> dict[str, Any]:
        return {"status": "cancelled", "output_payload": {}, "metadata": {"provider": "modal_md_checkpoint"}}


register_compute_runner("modal_checkpoint", lambda: ModalCheckpointComputeRunner())
