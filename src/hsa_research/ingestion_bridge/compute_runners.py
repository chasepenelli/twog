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


def _extract_omics_config(record: ComputeJobRecord) -> dict[str, Any] | None:
    """Find the omics-review config on a compute job (it rides with the validation request)."""
    payload = record.input_payload if isinstance(record.input_payload, dict) else {}
    candidates = [
        payload.get("omics_review"),
        (payload.get("validation_request") or {}).get("metadata", {}).get("omics_review")
        if isinstance(payload.get("validation_request"), dict)
        else None,
        record.metadata.get("omics_review") if isinstance(record.metadata, dict) else None,
    ]
    for cfg in candidates:
        if isinstance(cfg, dict):
            return cfg
    return None


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


def _call_modal_omics(config: dict[str, Any]) -> dict[str, Any]:
    """Run the omics analysis on Modal cloud CPU (imports modal + the app lazily, ephemeral run).

    Isolated so tests can monkeypatch it — the adapter logic is verified without billing Modal;
    the real cloud execution is verified by running modal_app.py / the flow with runner_kind="modal".
    """
    from .modal_app import app, run_omics_review_remote

    with app.run():
        return run_omics_review_remote.remote(config)


class ModalComputeRunner:
    """Modal cloud provider (runner_kind="modal"). v1 handles the omics-review CPU lane; GPU lanes
    (gnina/Boltz) plug in the same way as gpu=... Modal functions. The modal SDK is imported
    lazily (via _call_modal_omics) so twog never hard-depends on it."""

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

        if record.validation_type != "omics":
            raise ComputeRunnerConfigError(
                f"modal runner v1 handles validation_type='omics' only, got '{record.validation_type}'."
            )
        config = _extract_omics_config(record)
        if config is None:
            return _fail("modal_omics_config_missing")
        try:
            result = _call_modal_omics(config)
        except Exception as exc:  # network/auth/import/remote errors surface as a blocked-style failure
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
