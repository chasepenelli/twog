"""Phase B / B3 — the generic BYOC container compute runner.

A vetted collaborator runs the SAME lane logic as a DIGEST-PINNED container on THEIR OWN backend with
THEIR OWN credentials. The runner is provider-agnostic (register a ContainerBackend), enforces
digest-pinned images, passes the collaborator's credentials through (never persisting them), and wraps
the lane result with provenance breadcrumbs (image digest, backend, runner principal) for B4. Offline.
"""

from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403

from hsa_research.ingestion_bridge import compute_runners
from hsa_research.ingestion_bridge.compute_runners import (
    available_container_backends,
    get_compute_runner,
    register_container_backend,
)
from hsa_research.ingestion_bridge.contracts import ComputeJobRecord

DIGEST = "ghcr.io/twog/gnina@sha256:" + "a" * 64
MUTABLE = "ghcr.io/twog/gnina:latest"


class _FakeBackend:
    """A test container backend: records what it received and returns a docking-shaped lane result."""

    last_spec: dict | None = None

    def run(self, spec):
        _FakeBackend.last_spec = spec
        return {
            "status": "completed",
            "provider_job_id": "fake-backend-job-1",
            "output_payload": {
                "signal": "supports",
                "validation_type": spec.get("lane"),
                "metrics": {"best_affinity_kcal_mol": -8.8},
                "findings": "ligand engages the pocket (BYOC container run)",
            },
        }


def _job(**over) -> ComputeJobRecord:
    fields = dict(
        status="approved",
        runner_kind="container",
        compute_profile="gpu_a100",
        validation_type="docking",
        title="BYOC dock",
        objective="Dock a ligand in a collaborator-run container.",
        container_image=DIGEST,
        entrypoint=["python", "-m", "twog_lane.docking"],
        submitted_by="dr.vet",
        input_payload={
            "container": {
                "backend": "fake",
                "credentials": {"token": "COLLAB_SECRET_TOKEN"},
                "config": {"receptor_pdb": "...", "ligand_smiles": "C1=CC=CC=C1"},
            }
        },
    )
    fields.update(over)
    return ComputeJobRecord(**fields)


def _run(record):
    return get_compute_runner(record).submit(record)


def test_container_runner_is_registered():
    assert "container" in compute_runners.available_compute_runners()


def test_byoc_run_executes_on_backend_with_provenance_breadcrumbs():
    register_container_backend("fake", lambda: _FakeBackend())
    assert "fake" in available_container_backends()
    out = _run(_job())
    assert out["status"] == "completed"
    payload = out["output_payload"]
    # the lane result is carried through...
    assert payload["signal"] == "supports" and payload["metrics"]["best_affinity_kcal_mol"] == -8.8
    # ...wrapped with provenance breadcrumbs B4 will verify
    assert payload["provider"] == "container"
    assert payload["container_image"] == DIGEST
    assert payload["container_backend"] == "fake"
    assert payload["runner_principal"] == "dr.vet"
    assert out["provider_job_id"] == "fake-backend-job-1"
    # the collaborator's own credentials + lane config reached the backend
    assert _FakeBackend.last_spec["credentials"] == {"token": "COLLAB_SECRET_TOKEN"}
    assert _FakeBackend.last_spec["config"]["ligand_smiles"] == "C1=CC=CC=C1"
    # ...but credentials are NOT echoed into the result payload/metadata (not persisted by twog)
    import json as _json

    blob = _json.dumps(out)
    assert "COLLAB_SECRET_TOKEN" not in blob


def test_mutable_image_tag_is_refused():
    register_container_backend("fake", lambda: _FakeBackend())
    out = _run(_job(container_image=MUTABLE))
    assert out["status"] == "failed"
    assert out["output_payload"]["error"] == "container_image_must_be_digest_pinned"


def test_missing_image_is_refused():
    out = _run(_job(container_image=None))
    assert out["status"] == "failed"
    assert out["output_payload"]["error"] == "container_image_required"


def test_unregistered_backend_is_refused():
    out = _run(
        _job(input_payload={"container": {"backend": "nope-not-registered", "config": {}}})
    )
    assert out["status"] == "failed"
    assert out["output_payload"]["error"] == "container_backend_not_registered"


def test_backend_exception_surfaces_as_failed_job():
    class _Boom:
        def run(self, spec):
            raise RuntimeError("backend auth rejected")

    register_container_backend("boom", lambda: _Boom())
    out = _run(_job(input_payload={"container": {"backend": "boom", "config": {}}}))
    assert out["status"] == "failed"
    assert out["output_payload"]["error"] == "container_run_failed"
