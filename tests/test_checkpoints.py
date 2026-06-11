"""Phase 5 — compute checkpoint / pause / resume execution (with lease handoff)."""

from __future__ import annotations

import pytest

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository

from hsa_research.ingestion_bridge.contracts import ComputeJobRecord, ResearchWorkspaceRecord
from hsa_research.ingestion_bridge.service import CollaboratorAccessError, WorkspaceLeaseError


def _ckpt_job(repo, *, step=0.34, **kw):
    job = ComputeJobRecord(
        runner_kind="checkpoint", compute_profile="gpu", validation_type="omics",
        status="approved", title="long compute run", objective="prove checkpoint/resume",
        input_payload={"checkpoint": {"step": step}}, **kw,
    )
    return repo.upsert_compute_job(job)


def test_submit_pause_resume_until_complete(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "c.sqlite3", seed=False)
    service = HSAResearchService(repo)
    jid = _ckpt_job(repo).compute_job_id
    r1 = service.submit_compute_job(jid, dry_run=False)
    assert r1.status == "paused" and round(r1.progress_fraction, 2) == 0.34 and r1.checkpoint_uri
    r2 = service.resume_compute_job(jid)
    assert r2.status == "paused" and round(r2.progress_fraction, 2) == 0.68
    r3 = service.resume_compute_job(jid)
    assert r3.status == "completed" and r3.progress_fraction == 1.0
    assert "completed after resuming" in r3.output_payload["findings"]


def test_resume_requires_paused_status(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "c2.sqlite3", seed=False)
    service = HSAResearchService(repo)
    job = _ckpt_job(repo)  # status "approved", never submitted
    with pytest.raises(ValueError, match="compute_job_not_paused"):
        service.resume_compute_job(job.compute_job_id)


def test_manual_pause_records_checkpoint(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "c3.sqlite3", seed=False)
    service = HSAResearchService(repo)
    job = _ckpt_job(repo)
    paused = service.pause_compute_job(
        job.compute_job_id, progress_fraction=0.4, checkpoint_uri="s3://ck/40",
        checkpoint_state={"phase": "minimization"},
    )
    assert paused.status == "paused" and paused.progress_fraction == 0.4
    assert paused.checkpoint_uri == "s3://ck/40"
    assert paused.metadata["checkpoint_state"] == {"phase": "minimization"}
    resumed = service.resume_compute_job(job.compute_job_id)  # continues from 0.4
    assert resumed.status in {"paused", "completed"}
    assert resumed.progress_fraction > 0.4


def test_checkpoint_lease_handoff(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "c4.sqlite3", seed=False)
    service = HSAResearchService(repo)
    service.register_collaborator(principal="vet1", name="V1", role="collaborator")
    service.register_collaborator(principal="vet2", name="V2", role="collaborator")
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    ws = repo.upsert_research_workspace(
        ResearchWorkspaceRecord(candidate_id="vr-ckpt", provider="manual", status="ready")
    )
    service.lease_workspace(ws.workspace_id, "vet1")  # external_collaborator, held by vet1
    job = _ckpt_job(repo, workspace_id=ws.workspace_id)
    service.submit_compute_job(job.compute_job_id, dry_run=False)  # -> paused at 0.34

    # a different collaborator cannot pick up the paused job without the lease
    with pytest.raises(WorkspaceLeaseError):
        service.resume_compute_job(job.compute_job_id, principal="vet2")
    # the lease holder can resume
    assert service.resume_compute_job(job.compute_job_id, principal="vet1").progress_fraction > 0.34
    # an operator can resume without holding the lease
    assert service.resume_compute_job(job.compute_job_id, principal="chase") is not None


def test_revoked_principal_cannot_resume(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "c5.sqlite3", seed=False)
    service = HSAResearchService(repo)
    collab = service.register_collaborator(principal="vet1", name="V1", role="collaborator")
    job = _ckpt_job(repo)
    service.submit_compute_job(job.compute_job_id, dry_run=False)  # paused
    service.revoke_collaborator(collab.collaborator_id)
    with pytest.raises(CollaboratorAccessError):
        service.resume_compute_job(job.compute_job_id, principal="vet1")
