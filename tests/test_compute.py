"""Split from test_ingestion_bridge_contracts.py — see scripts/split_contract_tests.py.
Shared imports/helpers live in tests/_helpers.py."""
from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import (  # noqa: F401
    _FailingNeonBranchClient,
    _FakeNeonBranchClient,
    _MINIMAL_MD_PDB,
    _avma_vctr_study_card,
    _cleanup_workspace,
    _contains_key,
    _md_queue_item,
    _md_compute_input,
    _ready_for_therapy_ideas_program,
    _research_program_fixture,
    _seed_evaluated_brief,
    _seed_full_text_source_claim,
    _seed_minimal_source_claim,
    _seed_program_committee_corpus,
    _write_minimal_xlsx,
    _xlsx_column_name,
)

def test_public_candidate_snapshot_generation_links_therapy_decision_compute_and_artifact(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "public-candidate-generation.sqlite3", seed=False)
    service = HSAResearchService(repo)
    artifact_id = uuid4()
    brief_id = uuid4()
    chunk_id = uuid4()
    research_object_id = uuid4()
    repo.upsert_research_brief(
        ResearchBriefRecord(
            brief_id=brief_id,
            topic="VIM peptide program child",
            final_brief="Synthetic brief with source-traceable citations.",
            result_payload={
                "citations": [
                    {
                        "citation_id": "C1",
                        "chunk_id": str(chunk_id),
                        "research_object_id": str(research_object_id),
                        "source_key": "europe_pmc",
                        "title": "Vimentin expression in canine hemangiosarcoma",
                        "source_url": "https://example.test/vim",
                        "section_label": "abstract",
                        "quote": "VIM expression was observed in canine HSA.",
                        "metadata": {
                            "identifiers": {"doi": "10.1000/vim", "pmid": "123"},
                            "publication_year": 2026,
                            "provenance": {
                                "source_keys": ["europe_pmc"],
                                "source_urls": ["https://example.test/vim"],
                                "titles": ["Vimentin expression in canine hemangiosarcoma"],
                                "research_object_ids": [str(research_object_id)],
                                "chunk_ids": [str(chunk_id)],
                                "section_labels": ["abstract"],
                            },
                        },
                    }
                ]
            },
            citation_count=1,
        )
    )
    idea = TherapyIdea(
        title="Vimentin peptide blockade strategy",
        hypothesis="A vimentin-directed peptide strategy may disrupt vascular HSA invasion programs.",
        rationale="C1 indicates VIM sits at a plausible tumor ecology interface and needs inspectable validation.",
        candidate_therapies=["vimentin-targeting peptide"],
        targets=["VIM"],
        biomarkers=["VIM expression"],
        evidence_refs=["C1", "PMID:123"],
        evidence_strength="medium",
        risks=["direct canine peptide evidence remains sparse"],
        next_experiments=["Run processed omics VIM expression readout."],
        priority_score=0.82,
    )
    repo.upsert_therapy_idea(
        TherapyIdeaRecord(
            idea=idea,
            source_brief_id=brief_id,
            topic="VIM peptide program child",
            status="ready_for_promotion",
            score=0.82,
        )
    )
    decision = ValidationDecisionPacket(
        decision_id="validation_decision:public-candidate",
        packet_id="validation_packet:public-candidate",
        candidate_id=f"therapy_idea:{idea.idea_id}",
        source_type="therapy_idea",
        source_id=str(idea.idea_id),
        therapy_idea_id=idea.idea_id,
        title="Public candidate decision",
        outcome="promote_broader_program",
        confidence=0.72,
        validation_ready=True,
        specific_claim_viability="uncertain",
        broader_program_signal="strong",
        rationale="The broader VIM peptide program has enough signal for a public proof snapshot.",
        recommended_downstream_action="Create an inspectable candidate record and keep validation recommend-only.",
        decisive_questions=["Does VIM expression enrich in canine HSA cohorts?"],
        evidence_tasks=["Attach processed omics readout and assay strategy."],
        evidence_summary={"evidence_refs": ["PMID:123"]},
    )
    repo.upsert_validation_decision(ValidationDecisionRecord.from_decision(decision))
    repo.upsert_artifact(
        ArtifactHandle(
            artifact_id=artifact_id,
            artifact_type="md_pose",
            uri="twog://artifacts/pose.pdbqt",
            mime_type="chemical/x-pdbqt",
        )
    )
    repo.upsert_compute_job(
        ComputeJobRecord(
            status="completed",
            runner_kind="local",
            compute_profile="cpu",
            validation_type="md_smoke",
            title="VIM peptide MD smoke",
            objective="Run a smoke compute study for the VIM peptide public candidate.",
            output_payload={"summary": {"stage": "md_smoke", "status": "completed"}},
            artifact_ids=[artifact_id],
            metadata={"method_ref": "md-smoke-v1"},
        )
    )

    result = service.generate_public_candidate_snapshot(
        PublicCandidateGenerateRequest(
            therapy_idea_id=idea.idea_id,
            visibility="draft_public",
            pipeline_version="test-v1",
            commit_sha="abc123",
        )
    )

    assert result.errors == []
    assert result.candidate is not None
    assert result.snapshot is not None
    assert result.candidate.public_status == "compute_supported"
    assert result.candidate.candidate_kind == "peptide"
    assert result.candidate.latest_snapshot_id == result.snapshot.snapshot_id
    assert result.candidate.content_hash == result.snapshot.content_hash
    assert result.candidate.trace_id is not None
    assert result.snapshot.trace_id == result.candidate.trace_id
    assert result.snapshot.pipeline_version == "test-v1"
    assert str(artifact_id) in [str(value) for value in result.snapshot.artifact_ids]
    assert result.snapshot.payload["computational_evidence"][0]["summary"]["stage"] == "md_smoke"
    assert "PMID:123" in result.snapshot.citation_refs
    assert result.snapshot.payload["literature"][0]["title"] == "Vimentin expression in canine hemangiosarcoma"
    assert result.snapshot.payload["literature"][0]["identifiers"]["doi"] == "10.1000/vim"
    assert result.snapshot.payload["literature"][0]["supports"].startswith("C1 indicates")
    assert {event.action for event in result.decision_events} >= {"proposed", "evidence_added", "snapshot_generated"}
    assert {event.trace_id for event in result.decision_events} == {result.snapshot.trace_id}
    manifest_id = UUID(result.snapshot.metadata["run_manifest_id"])
    manifest = repo.get_run_manifest(manifest_id)
    assert manifest is not None
    assert manifest.manifest_type == "public_candidate_snapshot"
    assert manifest.status == "completed"
    assert manifest.trace_id == result.snapshot.trace_id
    assert manifest.candidate_ids == [result.candidate.candidate_id]
    assert manifest.content_hashes["public_candidate_snapshot"] == result.snapshot.content_hash
    assert manifest.compute_job_ids == result.snapshot.compute_job_ids
    assert manifest.artifact_ids == result.snapshot.artifact_ids
    listed = service.list_public_candidates(PublicCandidateLibraryRequest(query="vimentin", limit=10))
    assert listed.candidate_count == 1
    assert listed.candidates[0].therapy_idea_id == idea.idea_id
    api_list = command_center_web.public_candidates_payload(service, {"query": ["vimentin"]})
    assert api_list["candidate_count"] == 1
    api_detail = command_center_web.public_candidate_payload(service, result.candidate.candidate_id)
    assert api_detail["candidate"]["candidate_id"] == result.candidate.candidate_id
    assert api_detail["latest_snapshot"]["content_hash"] == result.snapshot.content_hash
    candidate_html = command_center_web._public_candidate_html(api_detail)
    assert "<h2>Evidence</h2>" in candidate_html
    assert "<strong>C1</strong> =" in candidate_html
    assert "Vimentin expression in canine hemangiosarcoma" in candidate_html


def test_neon_branch_workspace_dry_run_records_skill_preset():
    repo = InMemoryResearchRepository()
    result = provision_neon_branch_workspace(
        repo,
        NeonBranchWorkspaceRequest(
            candidate_id="twog-candidate-447eb8089965",
            work_packet_id="wp-database-1",
            project_id="project-test",
            branch_name="twog-wp-database-1",
            skill_profile="database_lookup",
            dry_run=True,
        ),
    )

    assert isinstance(result, NeonBranchWorkspaceResult)
    assert result.dry_run is True
    assert result.branch_created is False
    assert result.workspace.status == "requested"
    assert result.workspace.provider == "neon"
    assert result.workspace.neon_branch_name == "twog-wp-database-1"
    assert result.workspace.database_secret_ref == "neon://project-test/twog-wp-database-1/neondb/neondb_owner"
    assert "K-Dense-AI/scientific-agent-skills:database-lookup" in result.workspace.installed_skill_refs
    assert repo.get_research_workspace(result.workspace.workspace_id) is not None
    assert repo.list_research_workspaces(provider="neon", limit=10)


def test_neon_branch_workspace_reuses_existing_branch_without_storing_dsn():
    repo = InMemoryResearchRepository()
    client = _FakeNeonBranchClient(
        existing_branches=[{"id": "br-existing", "name": "twog-existing"}],
    )
    result = provision_neon_branch_workspace(
        repo,
        NeonBranchWorkspaceRequest(
            candidate_id="twog-candidate-447eb8089965",
            project_id="project-test",
            branch_name="twog-existing",
            skill_profile="md_review",
            dry_run=False,
        ),
        client=client,
    )

    assert result.branch_reused is True
    assert result.branch_created is False
    assert result.branch_id == "br-existing"
    assert result.workspace.status == "ready"
    assert result.workspace.provider == "neon"
    assert result.workspace.provider_workspace_id == "br-existing"
    assert result.workspace.neon_branch_id == "br-existing"
    assert result.workspace.database_secret_ref == "neon://project-test/br-existing/neondb/neondb_owner"
    assert "K-Dense-AI/scientific-agent-skills:molecular-dynamics" in result.workspace.installed_skill_refs
    assert client.created == []
    assert "postgres://secret.example" not in result.workspace.model_dump_json()


def test_neon_branch_workspace_creates_branch_with_endpoint_and_parent_resolution():
    repo = InMemoryResearchRepository()
    client = _FakeNeonBranchClient(
        existing_branches=[{"id": "br-parent", "name": "main"}],
    )
    result = provision_neon_branch_workspace(
        repo,
        NeonBranchWorkspaceRequest(
            candidate_id="twog-candidate-447eb8089965",
            work_packet_id="wp-citation-1",
            project_id="project-test",
            parent_branch_name="main",
            branch_name="twog-new-branch",
            suspend_timeout_seconds=120,
            skill_profile="literature_and_citation",
            dry_run=False,
        ),
        client=client,
    )

    assert result.branch_created is True
    assert result.branch_reused is False
    assert result.branch_id == "br-created"
    assert result.endpoint_id == "ep-created"
    assert result.operation_ids == ["op-created"]
    assert client.created[0]["branch"]["parent_id"] == "br-parent"
    assert client.created[0]["endpoints"] == [{"type": "read_write", "suspend_timeout_seconds": 120}]
    assert client.connection_requests[0]["endpoint_id"] == "ep-created"
    assert repo.get_research_workspace(result.workspace.workspace_id).status == "ready"


def test_neon_workspace_service_live_requires_explicit_secrets(monkeypatch):
    repo = InMemoryResearchRepository()
    monkeypatch.delenv("NEON_API_KEY", raising=False)
    monkeypatch.delenv("NEON_PROJECT_ID", raising=False)

    result = HSAResearchService(repo).provision_neon_research_workspace(
        NeonBranchWorkspaceRequest(
            candidate_id="twog-candidate-447eb8089965",
            branch_name="twog-live-blocked",
            dry_run=False,
        )
    )

    assert result.dry_run is False
    assert result.workspace.status == "failed"
    assert result.workspace.database_secret_ref is None
    assert "NEON_PROJECT_ID is required when dry_run is false." in result.errors
    assert "NEON_API_KEY is required when dry_run is false." in result.errors
    assert repo.get_research_workspace(result.workspace.workspace_id).status == "failed"


def test_research_workspace_service_library_filters():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    result = service.provision_neon_research_workspace(
        NeonBranchWorkspaceRequest(
            candidate_id="twog-candidate-447eb8089965",
            work_packet_id="wp-library-1",
            project_id="project-test",
            branch_name="twog-library",
            skill_profile="chemistry",
            dry_run=True,
        )
    )
    library = service.list_research_workspaces(
        ResearchWorkspaceLibraryRequest(
            candidate_id="twog-candidate-447eb8089965",
            work_packet_id="wp-library-1",
            provider="neon",
            status="requested",
            skill_profile="chemistry",
            limit=10,
        )
    )

    assert isinstance(library, ResearchWorkspaceLibraryResult)
    assert library.workspace_count == 1
    assert library.workspaces[0].workspace_id == result.workspace.workspace_id
    assert library.provider_counts == {"neon": 1}
    assert library.status_counts == {"requested": 1}


def test_research_workspace_checkout_manifest_requires_candidate_or_workspace():
    with pytest.raises(ValueError):
        ResearchWorkspaceCheckoutManifestRequest()


def test_research_workspace_checkout_manifest_attaches_to_workspace():
    repo = InMemoryResearchRepository()
    workspace = repo.upsert_research_workspace(
        ResearchWorkspaceRecord(
            candidate_id="twog-candidate-447eb8089965",
            work_packet_id="wp-proof-1",
            candidate_snapshot_hash="sha256:snapshot",
            evidence_bundle_hash="sha256:evidence",
            provider="neon",
            neon_branch_id="br-proof",
            neon_branch_name="twog-proof",
            database_secret_ref="neon://project-test/br-proof/neondb/neondb_owner",
            status="requested",
            skill_profile="k_dense_biomed",
            recommended_source_refs=["pubmed:123"],
        )
    )

    result = build_research_workspace_checkout_manifest(
        repo,
        ResearchWorkspaceCheckoutManifestRequest(
            workspace_id=workspace.workspace_id,
            open_questions=["Does this repair the KDR citation trail?"],
            artifact_refs=["artifact://candidate/twog-candidate-447eb8089965/evidence-bundle.json"],
        ),
    )

    assert isinstance(result, ResearchWorkspaceCheckoutManifestResult)
    assert result.persisted is True
    assert result.errors == []
    assert result.manifest.candidate_id == workspace.candidate_id
    assert result.manifest.work_packet_id == "wp-proof-1"
    assert result.manifest.candidate_snapshot_hash == "sha256:snapshot"
    assert result.manifest.evidence_bundle_hash == "sha256:evidence"
    assert result.manifest.content_hash.startswith("sha256:")
    assert "citation_repair" in result.manifest.allowed_task_types
    assert "K-Dense-AI/scientific-agent-skills:database-lookup" in result.manifest.installed_skill_refs
    stored = repo.get_research_workspace(workspace.workspace_id)
    assert stored.checkout_manifest_hash == result.manifest.content_hash
    assert stored.checkout_manifest["content_hash"] == result.manifest.content_hash
    assert "postgres://" not in json.dumps(result.model_dump(mode="json"))


def test_research_workspace_checkout_manifest_cli_attaches_to_workspace(monkeypatch, capsys, tmp_path):
    db_path = tmp_path / "workspace-manifest-cli.sqlite3"
    workspace = SQLiteResearchRepository(db_path, seed=False).upsert_research_workspace(
        ResearchWorkspaceRecord(
            candidate_id="twog-candidate-447eb8089965",
            work_packet_id="wp-cli-manifest",
            provider="neon",
            neon_branch_id="br-cli-manifest",
            neon_branch_name="twog-cli-manifest",
            database_secret_ref="neon://project-test/br-cli-manifest/neondb/neondb_owner",
            skill_profile="literature_and_citation",
            status="requested",
        )
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "hsa-ingestion",
            "--db",
            str(db_path),
            "research-workspace-checkout-manifest",
            "--workspace-id",
            str(workspace.workspace_id),
            "--open-question",
            "Can the external reviewer reproduce the citation trail?",
        ],
    )

    cli_module.main()
    output = json.loads(capsys.readouterr().out)

    assert output["persisted"] is True
    assert output["manifest"]["candidate_id"] == "twog-candidate-447eb8089965"
    assert output["manifest"]["content_hash"].startswith("sha256:")
    stored = SQLiteResearchRepository(db_path, seed=False).get_research_workspace(workspace.workspace_id)
    assert stored.checkout_manifest_hash == output["manifest"]["content_hash"]


def test_proof_capsule_contracts_require_target_source_and_limitations():
    with pytest.raises(ValidationError):
        ProofCapsuleTarget()
    with pytest.raises(ValidationError):
        ProofCapsuleSourceRef()
    with pytest.raises(ValidationError):
        ProofCapsuleSummary(
            title="Citation repair",
            finding="The citation should be replaced.",
            why_it_matters="It anchors the candidate rationale.",
            limitations=[],
        )


def test_proof_capsule_submit_persists_and_marks_workspace_submitted():
    repo = InMemoryResearchRepository()
    workspace = repo.upsert_research_workspace(
        ResearchWorkspaceRecord(
            candidate_id="twog-candidate-447eb8089965",
            work_packet_id="wp-proof-capsule",
            candidate_snapshot_hash="sha256:snapshot",
            provider="neon",
            neon_branch_id="br-proof-capsule",
            neon_branch_name="twog-proof-capsule",
            database_secret_ref="neon://project/br-proof-capsule/neondb/neondb_owner",
            status="requested",
        )
    )
    manifest_result = build_research_workspace_checkout_manifest(
        repo,
        ResearchWorkspaceCheckoutManifestRequest(workspace_id=workspace.workspace_id),
    )

    result = submit_proof_capsule(
        repo,
        ProofCapsuleSubmitRequest(
            workspace_id=workspace.workspace_id,
            checkout_manifest_hash=manifest_result.manifest.content_hash,
            candidate_id=workspace.candidate_id,
            candidate_snapshot_hash="sha256:snapshot",
            work_packet_id="wp-proof-capsule",
            packet_type="citation_repair",
            requested_action="citation_repair",
            target=ProofCapsuleTarget(section="Literature audit", evidence_ref="C1"),
            summary=ProofCapsuleSummary(
                title="Repair KDR evidence citation",
                finding="The cited claim should point at a canine-specific KDR source.",
                why_it_matters="The candidate record needs source-level provenance before promotion.",
                limitations=["This does not evaluate therapeutic efficacy."],
            ),
            payload={"method_notes": "Checked the candidate snapshot and evidence bundle."},
            source_refs=[
                ProofCapsuleSourceRef(
                    title="Canine vascular marker review",
                    doi="10.0000/example",
                    claim_supported="KDR evidence should be canine-specific.",
                )
            ],
            limitations=["Needs operator review before public record mutation."],
        ),
    )

    assert isinstance(result, ProofCapsuleSubmitResult)
    assert result.accepted is True
    assert result.persisted is True
    assert result.capsule is not None
    assert result.capsule.content_hash.startswith("sha256:")
    assert repo.get_proof_capsule(result.capsule.capsule_id).packet_type == "citation_repair"
    stored_workspace = repo.get_research_workspace(workspace.workspace_id)
    assert stored_workspace.status == "submitted"
    assert stored_workspace.submitted_proof_capsule_id == result.capsule.capsule_id

    library = build_proof_capsule_library(
        repo,
        ProofCapsuleLibraryRequest(candidate_id=workspace.candidate_id, status="submitted"),
    )
    assert isinstance(library, ProofCapsuleLibraryResult)
    assert library.capsule_count == 1
    assert library.packet_type_counts == {"citation_repair": 1}


def test_proof_capsule_rejects_manifest_mismatch_and_raw_secrets():
    repo = InMemoryResearchRepository()
    workspace = repo.upsert_research_workspace(
        ResearchWorkspaceRecord(
            candidate_id="twog-candidate-447eb8089965",
            work_packet_id="wp-proof-reject",
            checkout_manifest_hash="sha256:manifest",
            checkout_manifest={"allowed_task_types": ["citation_repair"]},
            status="requested",
        )
    )
    base_request = {
        "workspace_id": workspace.workspace_id,
        "candidate_id": workspace.candidate_id,
        "work_packet_id": workspace.work_packet_id,
        "packet_type": "citation_repair",
        "requested_action": "citation_repair",
        "target": ProofCapsuleTarget(section="Rationale"),
        "summary": ProofCapsuleSummary(
            title="Repair citation",
            finding="A citation is stale.",
            why_it_matters="The public proof record must stay inspectable.",
            limitations=["Operator still needs to check the replacement."],
        ),
    }

    mismatch = submit_proof_capsule(
        repo,
        ProofCapsuleSubmitRequest(
            **base_request,
            checkout_manifest_hash="sha256:other-manifest",
            payload={"note": "manifest mismatch"},
        ),
    )
    assert mismatch.accepted is False
    assert any("checkout_manifest_hash mismatch" in error for error in mismatch.errors)

    secret = submit_proof_capsule(
        repo,
        ProofCapsuleSubmitRequest(
            **base_request,
            checkout_manifest_hash="sha256:manifest",
            payload={"bad": "postgresql://user:pass@example.invalid/neondb"},
        ),
    )
    assert secret.accepted is False
    assert any("raw secret" in error for error in secret.errors)


def test_research_workspace_cleanup_contract_defaults_and_rejects_invalid_provider():
    request = ResearchWorkspaceCleanupRequest()

    assert request.provider == "neon"
    assert request.dry_run is True
    assert request.reason == "operator_workspace_cleanup"

    with pytest.raises(ValidationError):
        ResearchWorkspaceCleanupRequest(provider="prod-db")


def test_research_workspace_cleanup_dry_run_selects_expired_and_skips_active():
    repo = InMemoryResearchRepository()
    expired = repo.upsert_research_workspace(_cleanup_workspace(branch_id="br-expired"))
    active = repo.upsert_research_workspace(
        _cleanup_workspace(
            branch_id="br-active",
            expires_at=datetime.now(UTC) + timedelta(hours=2),
            work_packet_id="wp-cleanup-2",
        )
    )

    result = cleanup_neon_research_workspaces(
        repo,
        ResearchWorkspaceCleanupRequest(candidate_id=expired.candidate_id, dry_run=True, limit=10),
        project_id="project-test",
    )

    assert isinstance(result, ResearchWorkspaceCleanupResult)
    assert result.dry_run is True
    assert [candidate.workspace.workspace_id for candidate in result.candidates] == [expired.workspace_id]
    assert [candidate.workspace.workspace_id for candidate in result.skipped] == [active.workspace_id]
    assert result.skipped[0].reason == "workspace_not_expired"
    assert repo.get_research_workspace(expired.workspace_id).database_secret_ref is not None


def test_research_workspace_cleanup_explicit_workspace_can_select_non_expired():
    repo = InMemoryResearchRepository()
    active = repo.upsert_research_workspace(
        _cleanup_workspace(branch_id="br-active", expires_at=datetime.now(UTC) + timedelta(hours=2))
    )

    result = cleanup_neon_research_workspaces(
        repo,
        ResearchWorkspaceCleanupRequest(workspace_id=active.workspace_id, dry_run=True),
        project_id="project-test",
    )

    assert result.candidate_count == 1
    assert result.candidates[0].workspace.workspace_id == active.workspace_id


def test_research_workspace_cleanup_skips_configured_parent_branch():
    repo = InMemoryResearchRepository()
    parent = repo.upsert_research_workspace(_cleanup_workspace(branch_id="br-parent"))

    result = cleanup_neon_research_workspaces(
        repo,
        ResearchWorkspaceCleanupRequest(workspace_id=parent.workspace_id, dry_run=True),
        project_id="project-test",
        protected_branch_ids={"br-parent"},
    )

    assert result.candidate_count == 0
    assert result.skipped_count == 1
    assert result.skipped[0].reason == "workspace_branch_is_configured_parent_branch"


def test_research_workspace_cleanup_live_missing_env_fails_without_mutation(monkeypatch):
    repo = InMemoryResearchRepository()
    workspace = repo.upsert_research_workspace(_cleanup_workspace())
    monkeypatch.delenv("NEON_API_KEY", raising=False)
    monkeypatch.delenv("NEON_PROJECT_ID", raising=False)

    result = HSAResearchService(repo).cleanup_research_workspaces(
        ResearchWorkspaceCleanupRequest(workspace_id=workspace.workspace_id, dry_run=False)
    )

    stored = repo.get_research_workspace(workspace.workspace_id)
    assert result.dry_run is False
    assert result.deleted_count == 0
    assert "NEON_PROJECT_ID is required when dry_run is false." in result.errors
    assert "NEON_API_KEY is required when dry_run is false." in result.errors
    assert stored.status == "ready"
    assert stored.database_secret_ref is not None


def test_research_workspace_cleanup_fake_delete_success_clears_secret_ref():
    repo = InMemoryResearchRepository()
    workspace = repo.upsert_research_workspace(_cleanup_workspace())
    client = _FakeNeonBranchClient()

    result = cleanup_neon_research_workspaces(
        repo,
        ResearchWorkspaceCleanupRequest(workspace_id=workspace.workspace_id, dry_run=False, reason="ttl_cleanup"),
        project_id="project-test",
        client=client,
    )

    stored = repo.get_research_workspace(workspace.workspace_id)
    assert result.deleted_count == 1
    assert result.updated_count == 1
    assert result.deleted_branch_ids == ["br-expired"]
    assert client.deleted == [{"project_id": "project-test", "branch_id": "br-expired"}]
    assert stored.status == "expired"
    assert stored.database_secret_ref is None
    assert stored.metadata["cleanup"]["deleted"] is True
    assert stored.metadata["cleanup"]["reason"] == "ttl_cleanup"

    repeated = cleanup_neon_research_workspaces(
        repo,
        ResearchWorkspaceCleanupRequest(workspace_id=workspace.workspace_id, dry_run=True),
        project_id="project-test",
    )
    assert repeated.candidate_count == 0
    assert repeated.skipped[0].reason == "workspace_branch_already_cleaned"


def test_research_workspace_cleanup_fake_delete_failure_preserves_secret_ref():
    repo = InMemoryResearchRepository()
    workspace = repo.upsert_research_workspace(_cleanup_workspace())
    client = _FailingNeonBranchClient()

    result = cleanup_neon_research_workspaces(
        repo,
        ResearchWorkspaceCleanupRequest(workspace_id=workspace.workspace_id, dry_run=False),
        project_id="project-test",
        client=client,
    )

    stored = repo.get_research_workspace(workspace.workspace_id)
    assert result.deleted_count == 0
    assert result.updated_count == 1
    assert "Neon branch cleanup failed for br-expired: delete blocked" in result.errors
    assert stored.status == "ready"
    assert stored.database_secret_ref is not None
    assert stored.metadata["cleanup"]["deleted"] is False


def test_research_workspace_cli_dry_run_persists_without_neon(monkeypatch, capsys, tmp_path):
    db_path = tmp_path / "workspace-cli.sqlite3"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "hsa-ingestion",
            "--db",
            str(db_path),
            "research-workspace-neon",
            "--candidate-id",
            "twog-candidate-447eb8089965",
            "--work-packet-id",
            "wp-cli-1",
            "--branch-name",
            "twog-cli-dry-run",
            "--skill-profile",
            "database_lookup",
        ],
    )
    monkeypatch.delenv("NEON_API_KEY", raising=False)
    monkeypatch.delenv("NEON_PROJECT_ID", raising=False)

    cli_module.main()
    output = json.loads(capsys.readouterr().out)

    assert output["dry_run"] is True
    assert output["workspace"]["status"] == "requested"
    assert output["workspace"]["provider"] == "neon"
    assert output["workspace"]["neon_branch_name"] == "twog-cli-dry-run"
    assert "K-Dense-AI/scientific-agent-skills:database-lookup" in output["workspace"]["installed_skill_refs"]


def test_research_workspace_cli_execute_missing_env_fails_cleanly(monkeypatch, capsys, tmp_path):
    db_path = tmp_path / "workspace-cli-live.sqlite3"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "hsa-ingestion",
            "--db",
            str(db_path),
            "research-workspace-neon",
            "--candidate-id",
            "twog-candidate-447eb8089965",
            "--branch-name",
            "twog-cli-live-blocked",
            "--execute",
        ],
    )
    monkeypatch.delenv("NEON_API_KEY", raising=False)
    monkeypatch.delenv("NEON_PROJECT_ID", raising=False)

    cli_module.main()
    output = json.loads(capsys.readouterr().out)

    assert output["dry_run"] is False
    assert output["workspace"]["status"] == "failed"
    assert output["workspace"]["database_secret_ref"] is None
    assert "NEON_API_KEY is required when dry_run is false." in output["errors"]


def test_research_workspace_cleanup_cli_defaults_to_dry_run(monkeypatch, capsys, tmp_path):
    db_path = tmp_path / "workspace-cleanup-cli.sqlite3"
    workspace = SQLiteResearchRepository(db_path, seed=False).upsert_research_workspace(_cleanup_workspace())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "hsa-ingestion",
            "--db",
            str(db_path),
            "research-workspace-cleanup",
            "--workspace-id",
            str(workspace.workspace_id),
        ],
    )
    monkeypatch.delenv("NEON_API_KEY", raising=False)
    monkeypatch.delenv("NEON_PROJECT_ID", raising=False)

    cli_module.main()
    output = json.loads(capsys.readouterr().out)

    assert output["dry_run"] is True
    assert output["candidate_count"] == 1
    assert output["deleted_count"] == 0
    assert output["candidates"][0]["workspace"]["workspace_id"] == str(workspace.workspace_id)


def test_research_workspace_cleanup_cli_apply_missing_env_fails_cleanly(monkeypatch, capsys, tmp_path):
    db_path = tmp_path / "workspace-cleanup-cli-apply.sqlite3"
    workspace = SQLiteResearchRepository(db_path, seed=False).upsert_research_workspace(_cleanup_workspace())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "hsa-ingestion",
            "--db",
            str(db_path),
            "research-workspace-cleanup",
            "--workspace-id",
            str(workspace.workspace_id),
            "--apply",
        ],
    )
    monkeypatch.delenv("NEON_API_KEY", raising=False)
    monkeypatch.delenv("NEON_PROJECT_ID", raising=False)

    cli_module.main()
    output = json.loads(capsys.readouterr().out)

    assert output["dry_run"] is False
    assert output["candidate_count"] == 1
    assert output["deleted_count"] == 0
    assert "NEON_API_KEY is required when dry_run is false." in output["errors"]


def test_daytona_workspace_stub_is_non_live_and_secret_ref_only():
    repo = InMemoryResearchRepository()
    result = plan_daytona_workspace(
        repo,
        DaytonaWorkspaceRequest(
            candidate_id="twog-candidate-447eb8089965",
            work_packet_id="wp-daytona-1",
            database_secret_ref="neon://project/br-workspace/neondb/neondb_owner",
            checkout_manifest_hash="sha256:manifest",
            git_repo="https://github.com/chasepenelli/twog",
            git_ref="main",
            skill_profile="md_review",
            dry_run=True,
        ),
    )

    assert isinstance(result, DaytonaWorkspaceResult)
    assert result.workspace.provider == "daytona"
    assert result.workspace.status == "requested"
    assert result.ready_for_provider_dispatch is True
    assert result.provider_payload["checkout_manifest_hash"] == "sha256:manifest"
    assert result.provider_payload["forbidden_secrets"] == [
        "OPENROUTER_API_KEY",
        "MODAL_TOKEN_SECRET",
        "GH_TOKEN",
        "production_database_url",
    ]
    assert "K-Dense-AI/scientific-agent-skills:molecular-dynamics" in result.workspace.installed_skill_refs
    assert "postgres://" not in result.workspace.model_dump_json()


def test_daytona_workspace_live_request_fails_closed_without_provider_client():
    repo = InMemoryResearchRepository()
    result = plan_daytona_workspace(
        repo,
        DaytonaWorkspaceRequest(
            candidate_id="twog-candidate-447eb8089965",
            work_packet_id="wp-daytona-live",
            dry_run=False,
        ),
    )

    assert result.dry_run is False
    assert result.ready_for_provider_dispatch is False
    assert result.workspace.status == "failed"
    assert "Daytona provider client is not configured; live provisioning remains gated." in result.errors
    assert "database_secret_ref is required before live Daytona provisioning." in result.errors


def test_omics_readouts_compute_vim_and_gene_set_scores_from_processed_matrix(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "omics-readouts.sqlite3", seed=False)
    matrix_text = "\n".join(
        [
            "gene\tcontrol_1\tcontrol_2\thsa_tumor_1\thsa_tumor_2",
            "VIM\t4.1\t4.0\t8.2\t8.4",
            "FN1\t4.4\t4.2\t7.8\t8.0",
            "COL1A1\t3.9\t4.1\t7.4\t7.5",
            "VEGFA\t3.0\t3.1\t6.5\t6.7",
            "KDR\t3.2\t3.0\t6.1\t6.4",
            "F3\t2.8\t2.9\t5.9\t6.1",
            "SERPINE1\t2.7\t2.8\t5.7\t5.8",
        ]
    )
    raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="geo",
            source_record_id="GSEVIMREADOUT",
            content_hash="omics-readout-raw",
            raw_payload={"accession": "GSEVIMREADOUT"},
        )
    )
    repo.upsert_research_object(
        ResearchObject(
            object_type=ResearchObjectType.DATASET,
            title="Canine hemangiosarcoma processed RNA-seq VIM expression matrix",
            abstract="Canine hemangiosarcoma transcriptome evidence for VIM/vimentin and angiogenesis.",
            source_key="geo",
            raw_record_id=raw_id,
            dedupe_key="geo_accession:gsevimreadout",
            identifiers={"geo_accession": "GSEVIMREADOUT"},
            metadata={
                "organism": "Canis lupus familiaris",
                "sample_count": 4,
                "library_strategy": "RNA-seq",
                "sample_accessions": ["control_1", "control_2", "hsa_tumor_1", "hsa_tumor_2"],
                "supplementary_file_types": ["TSV"],
                "matrix_text": matrix_text,
            },
        ),
        raw_id,
    )

    result = HSAResearchService(repo).build_omics_readouts(
        OmicsReadoutRequest(packet_key="canine_hsa", accessions=["GSEVIMREADOUT"], max_datasets=1)
    )

    assert isinstance(result, OmicsReadoutResult)
    assert result.computed_count == 1
    assert result.skipped_count == 0
    dataset_result = result.datasets[0]
    assert dataset_result.status == "computed"
    assert dataset_result.normalized_kind == "processed_expression"
    assert dataset_result.target_expression is not None
    assert dataset_result.target_expression.detected is True
    assert dataset_result.target_expression.support_level == "differential_support"
    assert dataset_result.target_expression.tumor_control_delta is not None
    assert dataset_result.target_expression.tumor_control_delta > 0
    gene_sets = {score.gene_set_key: score for score in dataset_result.gene_set_scores}
    assert gene_sets["mesenchymal_ecm"].support_level == "differential_support"
    assert gene_sets["angiogenesis_endothelial"].detected_gene_count == 2
    assert gene_sets["coagulation_vascular_injury"].detected_gene_count == 2
    assert dataset_result.result_artifact_id is not None
    assert repo.get_artifact(dataset_result.result_artifact_id) is not None


def test_omics_locus_signals_compute_bigwig_target_signal(monkeypatch, tmp_path):
    from hsa_research.ingestion_bridge import omics_locus_signals

    class FakeBigWig:
        def __init__(self, path):
            self.path = str(path)

        def chroms(self):
            return {"chr2": 1000000}

        def stats(self, chrom, start, end, type="mean", exact=True):
            if "tumor_1_minus" in self.path:
                return [-10.0]
            if "tumor_2_minus" in self.path:
                return [-12.0]
            if "normal_1_minus" in self.path:
                return [-3.0]
            if "normal_2_minus" in self.path:
                return [-4.0]
            return [1.0]

        def close(self):
            return None

    class FakePyBigWig:
        @staticmethod
        def open(path):
            return FakeBigWig(path)

    captured = {}

    def fake_run_validation_agent(item, *, model_profile):
        captured["brief_id"] = item.brief_id
        captured["model_profile"] = model_profile
        return ValidationAgentResult(
            queue_item_id=item.queue_item_id,
            plan_id=item.plan_id,
            task_id=item.task_id,
            task_type=item.task_type,
            validation_type=item.validation_request.validation_type,
            agent_name="omics_validation_agent",
            model_profile=model_profile,
            decision="hold",
            confidence=0.7,
            summary="VIM locus signal was computed and should be interpreted with ChRO-seq limitations.",
            missing_evidence=["independent RNA expression matrix"],
        )

    monkeypatch.setattr(omics_locus_signals, "_load_pybigwig", lambda: FakePyBigWig)
    monkeypatch.setattr(omics_locus_signals, "run_validation_agent", fake_run_validation_agent)
    repo = InMemoryResearchRepository()
    obj = ResearchObject(
        object_type=ResearchObjectType.DATASET,
        title="Canine hemangiosarcoma ChRO-seq bigWig VIM signal dataset",
        abstract="Canine hemangiosarcoma ChRO-seq signal tracks for tumor and normal tissue.",
        source_key="geo",
        dedupe_key="geo_accession:gsebigwigsignal",
        identifiers={"geo_accession": "GSEBIGWIGSIGNAL"},
        metadata={
            "organism": "Canis lupus familiaris",
            "sample_count": 4,
            "library_strategy": "ChRO-seq",
            "sample_accessions": ["tumor_1", "tumor_2", "normal_1", "normal_2"],
            "sample_titles": {
                "tumor_1": "canine HSA tumor",
                "tumor_2": "canine HSA tumor",
                "normal_1": "canine normal tissue",
                "normal_2": "canine normal tissue",
            },
            "supplementary_file_types": ["BW"],
        },
    )
    repo.research_objects[obj.id] = obj
    bigwig_uri_by_sample = {
        sample: {
            "plus": (tmp_path / f"{sample}_plus.bw").as_uri(),
            "minus": (tmp_path / f"{sample}_minus.bw").as_uri(),
        }
        for sample in ["tumor_1", "tumor_2", "normal_1", "normal_2"]
    }

    result = HSAResearchService(repo).build_omics_locus_signals(
        OmicsLocusSignalRequest(
            accessions=["GSEBIGWIGSIGNAL"],
            bigwig_uri_by_sample=bigwig_uri_by_sample,
            max_samples_per_group=2,
            run_validation_agent=True,
        )
    )

    assert isinstance(result, OmicsLocusSignalResult)
    assert result.computed_count == 1
    dataset_result = result.datasets[0]
    assert dataset_result.status == "computed"
    assert dataset_result.tumor_sample_count == 2
    assert dataset_result.control_sample_count == 2
    assert dataset_result.tumor_control_delta is not None
    assert dataset_result.tumor_control_delta > 0
    assert dataset_result.effect_size is not None
    assert dataset_result.tumor_standard_deviation is not None
    assert dataset_result.control_standard_deviation is not None
    assert dataset_result.comparison_method == "welch_t_normal_approximation"
    assert dataset_result.comparison_p_value is not None
    assert dataset_result.comparison_p_value < 0.1
    assert dataset_result.normalization_method == "bigwig_target_locus_mean_signal"
    assert dataset_result.normalization_status == "not_verified"
    assert "bigwig_normalization_not_verified" in dataset_result.limitations
    assert dataset_result.metadata["statistical_test"]["status"] == "computed"
    assert dataset_result.metadata["normalization"]["status"] == "not_verified"
    assert dataset_result.support_level == "differential_support"
    assert dataset_result.sample_results[0].target_strand_mean is not None
    assert dataset_result.sample_results[0].target_strand_mean > 0
    assert dataset_result.sample_results[0].minus_mean is not None
    assert dataset_result.sample_results[0].minus_mean < 0
    assert dataset_result.sample_results[0].metadata["derived_signal"] == "strand_magnitude"
    assert captured["brief_id"] is not None
    assert captured["model_profile"] == "openrouter_required"
    assert result.validation_agent_result is not None
    assert result.validation_agent_result.agent_name == "omics_validation_agent"
    assert OmicsLocusSignalRequest().remote_extract_timeout_seconds == 600


def test_omics_readouts_can_route_computed_packet_to_deterministic_omics_agent():
    repo = InMemoryResearchRepository()
    matrix_text = "\n".join(
        [
            "gene\tcontrol_sample\thsa_tumor_sample",
            "VIM\t4.0\t8.0",
            "FN1\t4.0\t8.0",
            "VEGFA\t3.0\t7.0",
            "F3\t2.0\t6.0",
        ]
    )
    obj = ResearchObject(
        object_type=ResearchObjectType.DATASET,
        title="Canine hemangiosarcoma VIM processed matrix",
        abstract="Canine hemangiosarcoma transcriptome evidence for VIM/vimentin expression.",
        source_key="geo",
        dedupe_key="geo_accession:gseagentreadout",
        identifiers={"geo_accession": "GSEAGENTREADOUT"},
        metadata={
            "organism": "Canis lupus familiaris",
            "sample_count": 2,
            "sample_accessions": ["control_sample", "hsa_tumor_sample"],
            "supplementary_file_types": ["TSV"],
            "matrix_text": matrix_text,
        },
    )
    repo.research_objects[obj.id] = obj

    result = HSAResearchService(repo).build_omics_readouts(
        OmicsReadoutRequest(
            packet_key="canine_hsa",
            accessions=["GSEAGENTREADOUT"],
            max_datasets=1,
            run_validation_agent=True,
            model_profile="deterministic_only",
        )
    )

    assert result.validation_agent_result is not None
    assert result.validation_agent_result.agent_name == "omics_validation_agent"
    assert result.validation_agent_result.decision in {"promote", "hold"}
    agent_runs = repo.list_agent_runs(agent_name="omics_validation_agent", limit=10)
    assert len(agent_runs) == 1
    assert agent_runs[0].status == "completed"


def test_live_compute_validation_lanes_stay_blocked_until_runner_exists(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-request-live-compute-blocked.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Docking queue compute guardrail",
            task_type="docking",
            title="Dock candidate A against KDR",
            objective="Run docking only after a live compute runner is enabled.",
            rationale="The current validation layer is recommend-only.",
            validation_request=ValidationRequest(
                validation_type="docking",
                target_name="KDR",
                candidate_name="candidate A",
                objective="Dock candidate A against KDR.",
                require_approval=True,
                metadata={"compute_input": {"protein_pdb": "ATOM test"}},
                assay_context=ValidationAssayContext(
                    disease_context="canine hemangiosarcoma and human angiosarcoma",
                    species=["canine", "human"],
                    model_system="Computational target or structure model with explicit source provenance.",
                    assay_type="in silico structural validation",
                    readout="binding plausibility and failure modes",
                    endpoint="computational plausibility",
                ),
            ),
        )
    )

    service.approve_validation_request_queue_item(
        item.queue_item_id,
        approved_by="unit-test",
        approval_note="Approval should not enable live compute.",
    )
    blocked = service.dispatch_validation_request_queue_item(
        item.queue_item_id,
        model_profile="deterministic_only",
    )

    assert blocked is not None
    assert blocked.status == "blocked"
    assert "live_compute_runner_not_enabled" in blocked.dispatch_blockers
    assert blocked.last_run_id is None


def test_compute_job_contract_rejects_invalid_status():
    with pytest.raises(ValidationError):
        ComputeJobRecord(
            status="waiting",
            title="Dock candidate A",
            objective="Run a controlled docking job.",
        )




def test_md_smoke_seed_fetches_live_api_inputs_and_creates_compute_job(tmp_path, monkeypatch):
    repo = SQLiteResearchRepository(tmp_path / "md-smoke-seed.sqlite3", seed=False)
    service = HSAResearchService(repo)

    monkeypatch.setattr(service_module, "_fetch_rcsb_pdb_text", lambda pdb_id, timeout_seconds=45: _MINIMAL_MD_PDB)
    monkeypatch.setattr(service_module, "_fetch_pubchem_canonical_smiles", lambda name, timeout_seconds=45: "CCO")

    report = service.seed_md_smoke_compute_job(
        pdb_id="1abc",
        compound_name="ethanol",
        target_name="KDR",
        simulation_steps=10,
        approved_by="unit-test",
    )

    queue_item_id = report["queue_item_id"]
    compute_job_id = report["compute_job_id"]
    item = repo.get_validation_request_queue_item(UUID(queue_item_id))
    job = repo.get_compute_job(UUID(compute_job_id))

    assert item is not None
    assert item.status == "approved"
    assert item.task_type == "md"
    assert item.validation_request.validation_type == "md"
    assert item.validation_request.metadata["compute_input"]["protein_source"].startswith("RCSB PDB 1ABC")
    assert item.validation_request.metadata["compute_input"]["compound_smiles"] == "CCO"
    assert item.validation_request.metadata["compute_input"]["ph"] == 7.4
    assert item.validation_request.metadata["compute_input"]["box_padding"] == 10.0
    assert item.validation_request.metadata["compute_input"]["force_field"]
    assert item.validation_request.metadata["compute_input"]["solvent_model"] == "tip3p"
    assert item.validation_request.metadata["compute_input"]["enable_docking"] is False
    assert item.validation_request.metadata["compute_input"]["metadata"]["pdb_preparation"]["retained_records"] == [
        "ATOM",
        "TER",
        "END",
    ]
    assert job is not None
    assert job.validation_type == "md"
    assert job.status == "approved"
    assert report["queue_item"]["validation_request"]["metadata"]["compute_input"]["protein_pdb_sha256"]
    assert "protein_pdb" not in report["queue_item"]["validation_request"]["metadata"]["compute_input"]
    assert report["pdb_preparation"]["preparation"] == "protein_only_strip_hetatm_waters_ligands"


def test_md_smoke_seed_can_create_distinct_docking_enabled_packet(tmp_path, monkeypatch):
    repo = SQLiteResearchRepository(tmp_path / "md-smoke-seed-docking.sqlite3", seed=False)
    service = HSAResearchService(repo)

    monkeypatch.setattr(service_module, "_fetch_rcsb_pdb_text", lambda pdb_id, timeout_seconds=45: _MINIMAL_MD_PDB)
    monkeypatch.setattr(service_module, "_fetch_pubchem_canonical_smiles", lambda name, timeout_seconds=45: "CCO")

    prep_report = service.seed_md_smoke_compute_job(
        pdb_id="1abc",
        compound_name="ethanol",
        target_name="KDR",
        simulation_steps=10,
        approved_by="unit-test",
    )
    docking_report = service.seed_md_smoke_compute_job(
        pdb_id="1abc",
        compound_name="ethanol",
        target_name="KDR",
        simulation_steps=10,
        enable_docking=True,
        approved_by="unit-test",
    )

    assert docking_report["queue_item_id"] != prep_report["queue_item_id"]
    docking_item = repo.get_validation_request_queue_item(UUID(docking_report["queue_item_id"]))
    assert docking_item is not None
    assert docking_item.validation_request.metadata["compute_input"]["enable_docking"] is True
    docking_job = repo.get_compute_job(UUID(docking_report["compute_job_id"]))
    assert docking_job is not None
    packet = service.create_md_expert_review_packet(docking_job.compute_job_id, endpoint_id="endpoint-test")
    assert packet is not None
    assert packet.input_packet.enable_docking is True
    assert "Docking enabled: True" in packet.review_document


def test_md_input_packet_rejects_missing_or_malformed_inputs():
    valid = MDInputPacket(**_md_compute_input())
    assert valid.simulation_steps == 10
    assert valid.compound_smiles == "CCO"

    with pytest.raises(ValidationError):
        MDInputPacket(**_md_compute_input(protein_pdb="HEADER only\nEND\n"))
    with pytest.raises(ValidationError):
        MDInputPacket(**_md_compute_input(compound_smiles="C C"))
    with pytest.raises(ValidationError):
        MDInputPacket(**_md_compute_input(compound_smiles=""))


def test_md_expert_review_packet_round_trip_and_approval_status(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "md-expert-packets.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(_md_queue_item())
    service.approve_validation_request_queue_item(item.queue_item_id, approved_by="unit-test")
    created = service.build_compute_job_report(
        ComputeJobReportRequest(
            queue_item_id=item.queue_item_id,
            create_from_queue_item=True,
            approved_by="unit-test",
        )
    ).created_job
    assert created is not None

    packet = service.create_md_expert_review_packet(created.compute_job_id)
    assert isinstance(packet, MDExpertReviewPacketRecord)
    assert packet.status == "needs_review"
    assert packet.input_packet.target_name == "KDR"
    assert packet.packet_hash
    assert repo.get_md_expert_review_packet_by_hash(packet.packet_hash) == packet

    approval = service.record_md_expert_approval(
        packet.packet_id,
        decision="approved",
        reviewer_name="Dr. Test",
        reviewer_contact="expert@example.com",
        comments="Approved for one smoke test only.",
    )
    assert isinstance(approval, MDExpertApprovalRecord)
    assert approval.decision == "approved"
    assert repo.get_md_expert_review_packet(packet.packet_id).status == "approved"


def test_md_live_submit_blocks_until_exact_expert_approval_exists(tmp_path, monkeypatch):
    repo = SQLiteResearchRepository(tmp_path / "md-live-submit-gate.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(_md_queue_item())
    service.approve_validation_request_queue_item(item.queue_item_id, approved_by="unit-test")
    created = service.build_compute_job_report(
        ComputeJobReportRequest(
            queue_item_id=item.queue_item_id,
            create_from_queue_item=True,
            approved_by="unit-test",
        )
    ).created_job
    assert created is not None

    monkeypatch.setenv("HSA_MD_ENDPOINT_ID", "endpoint-test")

    blocked_without_packet = service.submit_compute_job(created.compute_job_id, dry_run=False)
    assert blocked_without_packet is not None
    assert blocked_without_packet.status == "blocked"
    assert blocked_without_packet.last_error == "md_expert_review_packet_required"

    packet = service.create_md_expert_review_packet(created.compute_job_id, endpoint_id="endpoint-test")
    assert packet is not None
    blocked_without_approval = service.submit_compute_job(created.compute_job_id, dry_run=False)
    assert blocked_without_approval is not None
    assert blocked_without_approval.status == "blocked"
    assert blocked_without_approval.last_error == "md_expert_approval_required"


def test_md_live_submit_allows_approved_packet_and_sends_worker_fields(tmp_path, monkeypatch):
    repo = SQLiteResearchRepository(tmp_path / "md-live-submit-approved.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(_md_queue_item())
    service.approve_validation_request_queue_item(item.queue_item_id, approved_by="unit-test")
    created = service.build_compute_job_report(
        ComputeJobReportRequest(
            queue_item_id=item.queue_item_id,
            create_from_queue_item=True,
            approved_by="unit-test",
        )
    ).created_job
    assert created is not None

    monkeypatch.setenv("HSA_MD_ENDPOINT_ID", "endpoint-test")
    packet = service.create_md_expert_review_packet(created.compute_job_id, endpoint_id="endpoint-test")
    assert packet is not None
    approval = service.record_md_expert_approval(
        packet.packet_id,
        decision="approved",
        reviewer_name="Dr. Test",
        reviewer_contact="expert@example.com",
    )
    assert approval is not None

    # Register a fake provider via the seam to prove the expert gate
    # passes after approval and the job reaches the provider (see compute_runners).
    class _FakeRunner:
        def submit(self, record):
            return {
                "status": "submitted",
                "external_run_id": "fake-md-run",
                "provider_job_id": "fake-md-run",
                "output_payload": {"provider": "fake"},
                "metadata": {"provider": "fake"},
            }

        def poll(self, record):  # pragma: no cover - not exercised here
            return {"status": "completed", "output_payload": {}, "last_error": None, "metadata": {}}

        def cancel(self, record):  # pragma: no cover - not exercised here
            return {"status": "cancelled", "output_payload": {}, "metadata": {}}

    monkeypatch.setitem(compute_runners._PROVIDERS, "external", lambda: _FakeRunner())
    submitted = service.submit_compute_job(created.compute_job_id, dry_run=False)

    assert submitted is not None
    assert submitted.status == "submitted"
    assert submitted.provider_job_id == "fake-md-run"
    # gate metadata (the approval id) is merged into the live submission metadata
    assert submitted.metadata["md_expert_approval_id"] == str(approval.approval_id)


def test_force_new_compute_job_does_not_reuse_failed_provider_state(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "md-force-new-compute-job.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(_md_queue_item())
    service.approve_validation_request_queue_item(item.queue_item_id, approved_by="unit-test")
    first = service.build_compute_job_report(
        ComputeJobReportRequest(
            queue_item_id=item.queue_item_id,
            create_from_queue_item=True,
            approved_by="unit-test",
        )
    ).created_job
    assert first is not None
    failed = repo.update_compute_job(
        first.compute_job_id,
        status="failed",
        external_run_id="old-provider-job",
        provider_job_id="old-provider-job",
        output_payload={"provider_status_response": {"status": "FAILED"}},
        last_error="worker_ligand_prep_failed",
    )
    assert failed is not None

    report = service.build_compute_job_report(
        ComputeJobReportRequest(
            queue_item_id=item.queue_item_id,
            create_from_queue_item=True,
            force_new_compute_job=True,
            approved_by="unit-test",
            metadata={"test_case": "force_new"},
        )
    )

    fresh = report.created_job
    assert fresh is not None
    assert fresh.compute_job_id != failed.compute_job_id
    assert fresh.status == "approved"
    assert fresh.external_run_id is None
    assert fresh.provider_job_id is None
    assert fresh.output_payload == {}
    assert fresh.last_error is None
    assert fresh.metadata["force_new_compute_job"] is True
    assert fresh.metadata["supersedes_compute_job_id"] == str(failed.compute_job_id)
    assert repo.get_compute_job(failed.compute_job_id).status == "failed"  # type: ignore[union-attr]
    assert len(repo.list_compute_jobs(queue_item_id=item.queue_item_id, limit=None)) == 2


def test_compute_job_report_creates_dry_run_and_blocks_live_submit(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "compute-job-report.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Docking queue compute job",
            task_type="docking",
            title="Dock candidate A against KDR",
            objective="Run docking after a live compute runner is configured.",
            rationale="The first GPU lane needs durable approval and artifact tracking.",
            validation_request=ValidationRequest(
                validation_type="docking",
                target_name="KDR",
                candidate_name="candidate A",
                objective="Dock candidate A against KDR.",
                require_approval=True,
                metadata={"compute_input": {"protein_pdb": "ATOM test"}},
                assay_context=ValidationAssayContext(
                    disease_context="canine hemangiosarcoma and human angiosarcoma",
                    species=["canine", "human"],
                    model_system="Computational target or structure model with explicit source provenance.",
                    assay_type="in silico structural validation",
                    readout="binding plausibility and failure modes",
                    endpoint="computational plausibility",
                ),
            ),
        )
    )
    service.approve_validation_request_queue_item(item.queue_item_id, approved_by="unit-test")

    report = service.build_compute_job_report(
        ComputeJobReportRequest(
            queue_item_id=item.queue_item_id,
            create_from_queue_item=True,
            submit=True,
            dry_run=True,
            approved_by="unit-test",
        )
    )
    assert isinstance(report, ComputeJobReportResult)
    assert report.created_job is not None
    assert report.created_job.status == "submitted"
    assert report.created_job.trace_id is not None
    assert "run_manifest_id" in report.created_job.metadata
    assert report.created_job.external_run_id.startswith("dry-run:")
    assert report.submitted_count == 1
    manifest = repo.get_run_manifest(UUID(report.created_job.metadata["run_manifest_id"]))
    assert manifest is not None
    assert manifest.manifest_type == "compute_job"
    assert manifest.status == "running"
    assert manifest.trace_id == report.created_job.trace_id
    assert manifest.compute_job_ids == [report.created_job.compute_job_id]
    assert manifest.output_refs["external_run_id"].startswith("dry-run:")

    live_attempt = service.submit_compute_job(report.created_job.compute_job_id, dry_run=False)
    assert live_attempt is not None
    assert live_attempt.status == "blocked"
    assert "compute_provider_not_configured" in live_attempt.last_error
    blocked_manifest = repo.get_run_manifest(UUID(live_attempt.metadata["run_manifest_id"]))
    assert blocked_manifest is not None
    assert blocked_manifest.status == "blocked"
    assert any("compute_provider_not_configured" in error for error in blocked_manifest.errors)






