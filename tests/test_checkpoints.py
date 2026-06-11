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


def test_modal_checkpoint_runner_adapter_paused_then_completed(tmp_path, monkeypatch):
    """The real-GPU provider adapter (runner_kind='modal_checkpoint') drives the pause/resume
    contract. The Modal call is monkeypatched to simulate the remote OpenMM checkpoint loop, so the
    adapter + submit/resume + checkpoint persistence are verified WITHOUT billing GPU."""
    from hsa_research.ingestion_bridge import compute_runners

    calls = {"n": 0}

    def fake_modal_md(config):
        # simulate the remote: advance 0.5 per (resume) call, durable progress comes from `resume`
        calls["n"] += 1
        progress = 0.5 if not config.get("resume") else 1.0
        done = progress >= 1.0
        return {
            "status": "completed" if done else "paused",
            "external_run_id": f"modal_md:{config['job_id']}",
            "runpod_job_id": f"modal_md:{config['job_id']}",
            "progress_fraction": progress,
            "checkpoint_uri": f"modal-volume://twog-md-checkpoints/{config['job_id']}/state.chk",
            "output_payload": {
                "provider": "modal_md_checkpoint", "platform": "CUDA", "progress": progress,
                "findings": "GPU MD completed." if done else "",
                "limitations": ["mock"], "source_refs": [], "metrics": {"platform": "CUDA"},
                "signal": "neutral", "confidence": 0.0,
            },
            "metadata": {"provider": "modal_md_checkpoint", "platform": "CUDA", "resume_seen": config.get("resume")},
        }

    monkeypatch.setattr(compute_runners, "_call_modal_md_checkpoint", fake_modal_md)

    repo = SQLiteResearchRepository(tmp_path / "mc.sqlite3", seed=False)
    service = HSAResearchService(repo)
    job = ComputeJobRecord(
        runner_kind="modal_checkpoint", compute_profile="gpu", validation_type="md",
        status="approved", title="GPU MD run", objective="real-provider checkpoint",
        input_payload={"md_checkpoint": {"total_steps": 30000, "steps_per_chunk": 15000}},
        # md lane is gated; mark it as already expert-approved-bypassed by using a non-md type instead:
    )
    # use an ungated validation_type so the md expert gate doesn't block this adapter test
    job = job.model_copy(update={"validation_type": "omics"})
    repo.upsert_compute_job(job)
    jid = job.compute_job_id

    r1 = service.submit_compute_job(jid, dry_run=False)
    assert r1.status == "paused" and r1.progress_fraction == 0.5
    assert r1.checkpoint_uri and r1.checkpoint_uri.startswith("modal-volume://")
    assert r1.metadata.get("resume_seen") is False  # first call is not a resume

    r2 = service.resume_compute_job(jid)
    assert r2.status == "completed" and r2.progress_fraction == 1.0
    assert r2.metadata.get("resume_seen") is True  # resume propagated to the provider
    assert calls["n"] == 2


def test_revoked_principal_cannot_resume(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "c5.sqlite3", seed=False)
    service = HSAResearchService(repo)
    collab = service.register_collaborator(principal="vet1", name="V1", role="collaborator")
    job = _ckpt_job(repo)
    service.submit_compute_job(job.compute_job_id, dry_run=False)  # paused
    service.revoke_collaborator(collab.collaborator_id)
    with pytest.raises(CollaboratorAccessError):
        service.resume_compute_job(job.compute_job_id, principal="vet1")
