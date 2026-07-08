"""Provenance Auditor (increment 4) — the integrity pre-gate on claimed runs.

Pure and deterministic: verify that a proof capsule's CLAIMED provenance (the compute job it says it
came from, plus the workspace/snapshot hashes and validation type bound to it) matches the actual
linked compute job. This is the machine-verifiable answer to the AI-Scientist failure mode ("claimed
it ran on a V100, actually ran on an H100"): what the agent SAYS it did must equal what it did.

Integrity, not validity — a correctly-provenanced capsule can still carry a wrong conclusion (that is
the Confound Auditor's job). A capsule that makes no provenance claim has nothing to refute here.
NO I/O — the service resolves the linked compute job and supplies it.
"""

from __future__ import annotations

from typing import Any


def _payload(capsule: Any) -> dict[str, Any]:
    payload = getattr(capsule, "payload", {})
    return payload if isinstance(payload, dict) else {}


def audit(capsule: Any, compute_job: Any) -> dict[str, Any]:
    """Return {ok, status, checks_passed, mismatches, rationale}. ``compute_job`` is the resolved job
    the capsule claims (or None if it claimed one that does not exist)."""
    payload = _payload(capsule)
    claimed_job_id = payload.get("compute_job_id")

    if not claimed_job_id:
        return {
            "ok": True,
            "status": "no_claim",
            "checks_passed": [],
            "mismatches": [],
            "rationale": "Capsule makes no compute-provenance claim to verify.",
        }

    if compute_job is None:
        return {
            "ok": False,
            "status": "claim_unresolved",
            "checks_passed": [],
            "mismatches": ["claimed_compute_job_not_found"],
            "rationale": f"Capsule claims compute job {claimed_job_id}, which does not exist.",
        }

    checks_passed: list[str] = []
    mismatches: list[str] = []

    def _check(name: str, claimed: Any, actual: Any) -> None:
        if claimed == actual:
            checks_passed.append(name)
        else:
            mismatches.append(f"{name}:{claimed!r}!={actual!r}")

    _check("compute_job_id", str(claimed_job_id), str(getattr(compute_job, "compute_job_id", None)))
    _check("candidate_id", getattr(capsule, "candidate_id", None), getattr(compute_job, "candidate_id", None))
    _check(
        "checkout_manifest_hash",
        getattr(capsule, "checkout_manifest_hash", None),
        getattr(compute_job, "checkout_manifest_hash", None),
    )
    _check(
        "candidate_snapshot_hash",
        getattr(capsule, "candidate_snapshot_hash", None),
        getattr(compute_job, "candidate_snapshot_hash", None),
    )
    _check("validation_type", payload.get("validation_type"), getattr(compute_job, "validation_type", None))

    # Foreign compute (Phase B BYOC, runner_kind="container"): the capsule was produced on a
    # collaborator's own backend, so integrity rests on the container identity, not our infra. Verify
    # the image is digest-pinned (immutable + reproducible) and that the capsule's CLAIMED image + the
    # runner principal match the linked job — i.e. it ran the container it says it did, as who it says.
    if getattr(compute_job, "runner_kind", None) == "container":
        image = getattr(compute_job, "container_image", None)
        if image and "@sha256:" in str(image):
            checks_passed.append("container_image_digest_pinned")
        else:
            mismatches.append(f"container_image_not_digest_pinned:{image!r}")
        _check("container_image", payload.get("container_image"), image)
        _check("runner_principal", payload.get("runner_principal"), getattr(compute_job, "submitted_by", None))

    # You cannot claim evidence from a job that never completed.
    if getattr(compute_job, "status", None) == "completed":
        checks_passed.append("compute_job_completed")
    else:
        mismatches.append(f"compute_job_status:{getattr(compute_job, 'status', None)!r}")

    ok = not mismatches
    return {
        "ok": ok,
        "status": "verified" if ok else "mismatch",
        "checks_passed": checks_passed,
        "mismatches": mismatches,
        "rationale": (
            "Claimed provenance matches the linked compute job."
            if ok
            else "Claimed provenance does not match the linked compute job."
        ),
    }
