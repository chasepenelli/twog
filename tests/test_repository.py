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

def test_research_repository_resource_uses_hsa_sqlite_path(monkeypatch, tmp_path):
    db_path = tmp_path / "dagster-resource.sqlite3"
    monkeypatch.setenv("HSA_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("HSA_SQLITE_PATH", str(db_path))
    monkeypatch.delenv("HSA_DATABASE_URL", raising=False)

    repo = ResearchRepositoryResource().build_repository()
    try:
        assert isinstance(repo, SQLiteResearchRepository)
        assert repo.db_path == db_path
        assert repo.coverage_summary()["claims"] >= 1
    finally:
        repo.conn.close()


def test_research_repository_resource_rejects_memory_backend():
    with pytest.raises(RuntimeError, match="sqlite or postgres storage"):
        ResearchRepositoryResource(storage_backend="memory").build_repository()


def test_therapy_idea_source_program_filter_round_trips_in_memory_and_sqlite(tmp_path):
    for repo in [
        InMemoryResearchRepository(),
        SQLiteResearchRepository(tmp_path / "program-filter-ideas.sqlite3", seed=False),
    ]:
        program = repo.upsert_research_program(_ready_for_therapy_ideas_program())
        other_program = repo.upsert_research_program(
            _ready_for_therapy_ideas_program().model_copy(update={"title": "Other program"})
        )
        idea = TherapyIdea(
            title="Program-linked vascular therapy idea",
            hypothesis="A high-level vascular ecology strategy should stay linked to its source program.",
            rationale="The bridge must support durable program provenance.",
            evidence_refs=["C1", "C2"],
            evidence_strength="medium",
            priority_score=0.75,
        )
        repo.upsert_therapy_idea(
            TherapyIdeaRecord(
                idea=idea,
                source_program_id=program.program_id,
                topic="program-linked therapy idea",
            )
        )

        assert HSAResearchService(repo).list_therapy_ideas(
            TherapyIdeaLibraryRequest(source_program_id=program.program_id)
        ).idea_count == 1
        assert HSAResearchService(repo).list_therapy_ideas(
            TherapyIdeaLibraryRequest(source_program_id=other_program.program_id)
        ).idea_count == 0


def test_sqlite_therapy_ideas_schema_adds_source_program_id_to_existing_table(tmp_path):
    db_path = tmp_path / "legacy-therapy-ideas.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            create table therapy_ideas (
              therapy_idea_id text primary key,
              committee_run_id text,
              agent_run_id text,
              source_brief_id text,
              source_evaluation_id text,
              topic text not null,
              source_key text,
              status text not null,
              promotion_state text,
              score real not null default 0.5,
              payload text not null,
              created_at text not null default current_timestamp,
              updated_at text not null default current_timestamp
            )
            """
        )

    repo = SQLiteResearchRepository(db_path, seed=False)
    columns = {
        str(row["name"])
        for row in repo.conn.execute("pragma table_info(therapy_ideas)").fetchall()
    }

    assert "source_program_id" in columns


def test_validation_decision_record_round_trips_in_memory_and_sqlite(tmp_path):
    decision = ValidationDecisionPacket(
        decision_id="validation_decision:record-test",
        packet_id="validation_packet:record-test",
        candidate_id="therapy_idea:record-test",
        source_type="therapy_idea",
        source_id=str(uuid4()),
        title="Decision record",
        outcome="promote_broader_program",
        rationale="Persist finite decisions for audit.",
        recommended_downstream_action="Create or update the research program.",
        decisive_questions=["Which evidence changes confidence?"],
        evidence_tasks=["Acquire one decisive evidence packet."],
    )
    record = ValidationDecisionRecord.from_decision(decision)
    memory_repo = InMemoryResearchRepository()
    sqlite_repo = SQLiteResearchRepository(tmp_path / "validation-decision-record.sqlite3", seed=False)

    for repo in (memory_repo, sqlite_repo):
        saved = repo.upsert_validation_decision(record)
        assert saved.decision_id == decision.decision_id
        assert repo.get_validation_decision(decision.decision_id).decision.packet_id == decision.packet_id
        assert repo.list_validation_decisions(outcome="promote_broader_program", limit=10)[0].decision_id == (
            decision.decision_id
        )


def test_public_candidate_records_round_trip_in_memory_and_sqlite(tmp_path):
    candidate = PublicCandidateRecord(
        candidate_id="twog-candidate-roundtrip",
        display_id="TWOG-RT",
        title="Round-trip candidate",
        public_status="investigating",
        visibility="private",
    )
    snapshot = PublicCandidateSnapshot(
        candidate_id=candidate.candidate_id,
        snapshot_version=1,
        content_hash="roundtriphash",
        title=candidate.title,
        payload={"identity": {"candidate_id": candidate.candidate_id}},
    )
    event = PublicCandidateDecisionEvent(
        candidate_id=candidate.candidate_id,
        action="snapshot_generated",
        related_snapshot_id=snapshot.snapshot_id,
    )
    memory_repo = InMemoryResearchRepository()
    sqlite_repo = SQLiteResearchRepository(tmp_path / "public-candidates.sqlite3", seed=False)

    for repo in (memory_repo, sqlite_repo):
        repo.upsert_public_candidate(candidate)
        repo.upsert_public_candidate_snapshot(snapshot)
        repo.append_public_candidate_decision_event(event)

        assert repo.get_public_candidate(candidate.candidate_id).display_id == "TWOG-RT"
        assert repo.list_public_candidates(PublicCandidateLibraryRequest(query="round-trip", limit=10))[0].candidate_id == (
            candidate.candidate_id
        )
        assert repo.list_public_candidate_snapshots(candidate_id=candidate.candidate_id)[0].content_hash == (
            snapshot.content_hash
        )
        assert repo.list_public_candidate_decision_events(candidate_id=candidate.candidate_id)[0].action == (
            "snapshot_generated"
        )


def test_research_program_contract_and_repository_round_trip(tmp_path):
    program = _research_program_fixture()
    payload = program.model_dump(mode="json")
    payload["gate_decision"] = "bad"
    with pytest.raises(ValueError):
        ResearchProgramRecord.model_validate(payload)
    payload = program.model_dump(mode="json")
    payload["evidence_loop_count"] = 3
    payload["max_evidence_loops"] = 2
    with pytest.raises(ValueError):
        ResearchProgramRecord.model_validate(payload)

    for repo in (
        InMemoryResearchRepository(),
        SQLiteResearchRepository(tmp_path / "research-programs.sqlite3", seed=False),
    ):
        stored = repo.upsert_research_program(program)
        assert repo.get_research_program(stored.program_id).title == "Vascular ecology program"
        assert repo.list_research_programs(thesis_query="coagulation", limit=10)
        assert repo.list_research_programs(gate_decision="needs_one_more_pass", limit=10)
        assert repo.list_research_programs(status="active", limit=10) == []


def test_research_workspace_contract_and_repository_round_trip(tmp_path):
    workspace = ResearchWorkspaceRecord(
        work_packet_id="wp-citation-1",
        candidate_id="twog-candidate-447eb8089965",
        candidate_snapshot_hash="sha256:snapshot",
        evidence_bundle_hash="sha256:evidence",
        checkout_manifest_hash="sha256:manifest",
        checkout_manifest={"candidate_id": "twog-candidate-447eb8089965", "packet": "wp-citation-1"},
        provider="manual",
        git_repo="https://github.com/chasepenelli/twog",
        git_ref="main",
        neon_branch_name="twog-wp-citation-1",
        skill_profile="literature_and_citation",
        installed_skill_refs=[
            "K-Dense-AI/scientific-agent-skills:literature-review",
            "K-Dense-AI/scientific-agent-skills:citation-management",
            "K-Dense-AI/scientific-agent-skills:database-lookup",
            "K-Dense-AI/scientific-agent-skills:database-lookup",
        ],
        recommended_source_refs=["pubmed", "crossref", "europe_pmc"],
        status="requested",
    )

    assert workspace.installed_skill_refs == [
        "K-Dense-AI/scientific-agent-skills:literature-review",
        "K-Dense-AI/scientific-agent-skills:citation-management",
        "K-Dense-AI/scientific-agent-skills:database-lookup",
    ]

    payload = workspace.model_dump(mode="json")
    payload["provider"] = "bad"
    with pytest.raises(ValidationError):
        ResearchWorkspaceRecord.model_validate(payload)

    payload = workspace.model_dump(mode="json")
    payload["provider"] = "e2b"
    payload["status"] = "ready"
    payload["provider_workspace_id"] = None
    with pytest.raises(ValueError):
        ResearchWorkspaceRecord.model_validate(payload)

    payload = workspace.model_dump(mode="json")
    payload["provider"] = "e2b"
    payload["status"] = "ready"
    payload["provider_workspace_id"] = "sandbox-1"
    payload["neon_branch_id"] = "br-test"
    payload["database_secret_ref"] = None
    with pytest.raises(ValueError):
        ResearchWorkspaceRecord.model_validate(payload)

    for repo in (
        InMemoryResearchRepository(),
        SQLiteResearchRepository(tmp_path / "research-workspaces.sqlite3", seed=False),
    ):
        stored = repo.upsert_research_workspace(workspace)
        assert repo.get_research_workspace(stored.workspace_id).candidate_id == workspace.candidate_id
        assert repo.list_research_workspaces(candidate_id=workspace.candidate_id, limit=10)
        assert repo.list_research_workspaces(work_packet_id="wp-citation-1", limit=10)
        assert repo.list_research_workspaces(skill_profile="literature_and_citation", limit=10)
        assert repo.list_research_workspaces(status="active", limit=10) == []


def test_proof_capsule_cli_and_sqlite_round_trip(monkeypatch, capsys, tmp_path):
    db_path = tmp_path / "proof-capsule-cli.sqlite3"
    repo = SQLiteResearchRepository(db_path, seed=False)
    workspace = repo.upsert_research_workspace(
        ResearchWorkspaceRecord(
            candidate_id="twog-candidate-447eb8089965",
            work_packet_id="wp-proof-cli",
            provider="neon",
            neon_branch_id="br-proof-cli",
            neon_branch_name="twog-proof-cli",
            database_secret_ref="neon://project/br-proof-cli/neondb/neondb_owner",
            status="requested",
        )
    )
    manifest = build_research_workspace_checkout_manifest(
        repo,
        ResearchWorkspaceCheckoutManifestRequest(workspace_id=workspace.workspace_id),
    ).manifest
    capsule_path = tmp_path / "proof-capsule.json"
    capsule_payload = ProofCapsuleSubmitRequest(
        workspace_id=workspace.workspace_id,
        checkout_manifest_hash=manifest.content_hash,
        candidate_id=workspace.candidate_id,
        work_packet_id="wp-proof-cli",
        packet_type="evidence_addition",
        requested_action="evidence_review",
        target=ProofCapsuleTarget(section="Evidence table", claim_id="claim-1"),
        summary=ProofCapsuleSummary(
            title="Add companion evidence note",
            finding="A new source supports the evidence table wording.",
            why_it_matters="The proof record can become more complete after review.",
            limitations=["The source has not been promoted by an operator."],
        ),
        payload={"method_notes": "External workspace review."},
        source_refs=[ProofCapsuleSourceRef(title="External evidence note", url="https://example.org/note")],
    ).model_dump(mode="json")
    capsule_path.write_text(json.dumps(capsule_payload, sort_keys=True))

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "hsa-ingestion",
            "--db",
            str(db_path),
            "proof-capsule-submit",
            "--file",
            str(capsule_path),
        ],
    )
    cli_module.main()
    submit_output = json.loads(capsys.readouterr().out)

    assert submit_output["accepted"] is True
    assert submit_output["persisted"] is True
    assert submit_output["capsule"]["packet_type"] == "evidence_addition"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "hsa-ingestion",
            "--db",
            str(db_path),
            "proof-capsules",
            "--candidate-id",
            "twog-candidate-447eb8089965",
        ],
    )
    cli_module.main()
    list_output = json.loads(capsys.readouterr().out)

    assert list_output["capsule_count"] == 1
    assert list_output["capsules"][0]["workspace_id"] == str(workspace.workspace_id)


def test_reward_event_repository_roundtrip_sqlite_and_memory(tmp_path):
    sqlite_repo = SQLiteResearchRepository(tmp_path / "reward-events.sqlite3", seed=False)
    memory_repo = InMemoryResearchRepository()

    for repo in (sqlite_repo, memory_repo):
        run = repo.create_agent_run(
            AgentRunRecord(
                agent_name="research_brief_agent",
                model_profile="openrouter_required",
                source_key="pubmed",
            )
        )
        event = repo.create_reward_event(
            RewardEventRecord(
                event_source="operator_review",
                score=1.0,
                dimension_scores={"overall": 1.0, "operator_usefulness": 1.0},
                agent_run_id=run.agent_run_id,
                agent_name=run.agent_name,
                model_profile=run.model_profile,
                source_key=run.source_key,
                verdict="useful",
            )
        )

        assert repo.get_reward_event(event.reward_event_id).score == 1.0
        assert repo.list_reward_events(agent_run_id=run.agent_run_id)[0].reward_event_id == event.reward_event_id
        assert repo.list_reward_events(agent_name="research_brief_agent")
        assert repo.list_reward_events(source_key="pubmed")
        assert repo.list_reward_events(event_source="operator_review")
        assert repo.list_reward_events(agent_name="full_text_ops_agent") == []


def test_research_lead_repository_roundtrip_sqlite_and_memory(tmp_path):
    sqlite_repo = SQLiteResearchRepository(tmp_path / "research-leads.sqlite3", seed=False)
    memory_repo = InMemoryResearchRepository()

    for repo in (sqlite_repo, memory_repo):
        lead = repo.upsert_research_lead(
            ResearchLeadRecord(
                title="Institutional HSA lead",
                url="https://example.edu/research/hsa",
                lead_type="institutional_article",
                source_key="x_linked_article",
                reason="Parser found a credible non-durable item.",
                topic_tags=["hemangiosarcoma"],
            )
        )
        duplicate = repo.upsert_research_lead(
            ResearchLeadRecord(
                title="Institutional HSA lead duplicate",
                url="https://example.edu/research/hsa",
                lead_type="institutional_article",
                source_key="x_linked_article",
                reason="Same URL.",
            )
        )
        updated = repo.update_research_lead(lead.lead_id, status="watching", metadata={"reviewed": True})

        assert duplicate.lead_id == lead.lead_id
        assert updated is not None
        assert updated.status == "watching"
        assert updated.metadata["reviewed"] is True
        assert repo.get_research_lead(lead.lead_id).identity_key == lead.identity_key
        assert repo.list_research_leads(status="watching", source_key="x_linked_article")
        assert repo.list_research_leads(lead_type="institutional_article", limit=1)[0].lead_id == lead.lead_id


def test_dagster_structured_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    def fail_direct_factory():
        raise AssertionError("dagster assets must use the injected repository resource")

    def fake_pipeline(repository, **kwargs):
        assert repository is sentinel_repository
        return {"repository": "injected", "source_keys": kwargs["source_keys"]}

    monkeypatch.setattr(storage, "build_sql_repository", fail_direct_factory)
    monkeypatch.setattr(structured_orchestration, "run_structured_sources_pipeline", fake_pipeline)

    result = dagster_asset_module.structured_source_smoke_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == ["build_repository"]
    assert result == {"repository": "injected", "source_keys": dagster_asset_module.STRUCTURED_SOURCE_SMOKE_KEYS}


def test_dagster_full_text_ops_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def run_full_text_ops(self, request):
            assert isinstance(request, FullTextOpsRequest)
            return FullTextOpsResult(
                agent_run_id=uuid4(),
                actions=[
                    FullTextOpsAction(
                        source_key="europe_pmc",
                        action="run_source_date_partition",
                        severity="watch",
                        reason="Partition evidence is missing.",
                    )
                ],
                schedule_readiness="needs_partition_validation",
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.full_text_ops_agent_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == ["build_repository"]
    assert result.value["agent_name"] == "full_text_ops_agent"
    assert result.metadata["action_count"] == 1


def test_dagster_research_hunt_queue_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    lead_id = uuid4()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def build_research_hunt_queue_report(self, request):
            assert isinstance(request, ResearchHuntQueueReportRequest)
            assert request.lead_ids == [lead_id]
            assert request.stale_after_hours == 24
            return ResearchHuntQueueReportResult(
                scanned_lead_count=1,
                lead_count=1,
                executable_task_count=1,
                hunting_count=1,
                status_counts={"open": 1},
                task_class_counts={"concrete": 1},
                control_status_counts={"hunting": 1},
                leads=[
                    ResearchHuntLeadQueueRow(
                        lead_id=lead_id,
                        title="Hunt queue",
                        status="watching",
                        priority=5,
                        control_status="hunting",
                        open_task_count=1,
                        open_concrete_count=1,
                        recommended_action="run_concrete_hunt_tasks",
                    )
                ],
                tasks=[
                    ResearchHuntTaskQueueRow(
                        lead_id=lead_id,
                        task_id=str(uuid4()),
                        task_type="claim_extract",
                        task_class="concrete",
                        status="open",
                        priority=20,
                        action="Run claim extraction.",
                        runnable_by_default=True,
                        recommended_action="run_research_hunt_tasks",
                    )
                ],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.research_hunt_queue_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "lead_ids": [str(lead_id)],
                "lead_statuses": ["watching"],
                "source_keys": [],
                "limit": 10,
                "task_limit": 10,
                "stale_after_hours": 24,
                "include_tasks": True,
                "include_suppressed": True,
            }
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert result.value["executable_task_count"] == 1
    assert result.metadata["executable_task_count"].value == 1


def test_dagster_research_hunt_queue_maintenance_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    lead_id = uuid4()
    task_id = uuid4()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def maintain_research_hunt_queue(self, request):
            assert isinstance(request, ResearchHuntQueueMaintenanceRequest)
            assert request.lead_ids == [lead_id]
            assert request.dagster_run_id == "dagster-hunt-maintenance-test"
            assert request.dry_run is False
            return ResearchHuntQueueMaintenanceResult(
                dry_run=False,
                candidate_count=1,
                suppressed_count=1,
                updated_lead_count=1,
                items=[
                    ResearchHuntQueueMaintenanceItem(
                        lead_id=lead_id,
                        task_id=str(task_id),
                        task_type="research_followup",
                        task_class="passive",
                        action="Monitor future publications.",
                        previous_status="open",
                        suppression_reason="passive_monitoring_note",
                        dry_run=False,
                    )
                ],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.research_hunt_queue_maintenance_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "lead_ids": [str(lead_id)],
                "lead_statuses": ["watching"],
                "source_keys": [],
                "reasons": ["passive_monitoring_note"],
                "stale_after_hours": 72,
                "limit": 10,
                "dry_run": False,
            },
            run_id="dagster-hunt-maintenance-test",
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert result.value["suppressed_count"] == 1
    assert result.metadata["suppressed_count"].value == 1


def test_dagster_research_hunt_synthesis_queue_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    lead_id = uuid4()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def queue_ready_research_hunt_synthesis(self, request):
            assert isinstance(request, ResearchHuntSynthesisQueueRequest)
            assert request.lead_ids == [lead_id]
            assert request.dry_run is False
            assert request.dagster_run_id == "dagster-hunt-synthesis-test"
            assert request.review_models == ["anthropic/claude-sonnet-4.6"]
            return ResearchHuntSynthesisQueueResult(
                dry_run=False,
                candidate_count=1,
                queued_count=1,
                updated_lead_count=1,
                queue_items=[
                    ResearchBriefQueueItem(
                        topic="Review research lead: Supported lead",
                        source_key="pubmed",
                        priority=20,
                        review_models=["anthropic/claude-sonnet-4.6"],
                    )
                ],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.research_hunt_synthesis_queue_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "lead_ids": [str(lead_id)],
                "lead_statuses": ["watching"],
                "source_keys": ["pubmed"],
                "limit": 10,
                "disease_scope": "canine hemangiosarcoma and human angiosarcoma",
                "priority": 40,
                "max_chunks_per_perspective": 10,
                "max_claims": 20,
                "max_chunk_chars": 2200,
                "brief_style": "technical",
                "model_profile": "research_brief",
                "review_mode": "openrouter_required",
                "review_models": ["anthropic/claude-sonnet-4.6"],
                "dry_run": False,
                "transition_leads": True,
            },
            run_id="dagster-hunt-synthesis-test",
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert result.value["queued_count"] == 1
    assert result.metadata["queued_count"].value == 1


def test_dagster_research_brief_evaluation_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    brief_id = uuid4()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def evaluate_research_brief(self, request):
            assert isinstance(request, ResearchBriefEvaluationRequest)
            assert request.brief_id == brief_id
            assert request.review_mode == "openrouter_required"
            return ResearchBriefEvaluationResult(
                brief_id=brief_id,
                agent_run_id=uuid4(),
                topic="VEGF therapy",
                source_key="pubmed",
                overall_score=0.82,
                citation_coverage_score=0.8,
                perspective_balance_score=0.8,
                contradiction_handling_score=0.8,
                novelty_score=0.8,
                actionability_score=0.8,
                weakness_transparency_score=0.8,
                passes_quality_bar=True,
                readiness="ready_for_hypothesis_review",
                recommendations=["Promote this brief."],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.research_brief_evaluation_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "brief_id": str(brief_id),
                "limit": 1,
                "minimum_overall_score": 0.7,
            },
            run_id="dagster-test-run",
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert result.value["brief_id"] == str(brief_id)
    assert result.metadata["readiness"] == "ready_for_hypothesis_review"


def test_dagster_validation_packet_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    therapy_idea_id = uuid4()
    candidate = HypothesisPromotionCandidate(
        candidate_id=f"therapy_idea:{therapy_idea_id}",
        source_type="therapy_idea",
        source_id=str(therapy_idea_id),
        therapy_idea_id=therapy_idea_id,
        title="Pazopanib KDR packet",
        hypothesis="Review pazopanib for KDR-altered canine HSA.",
        promotion_state="ready_for_validation_plan",
        score=0.78,
        candidate_therapies=["pazopanib"],
        targets=["KDR"],
        evidence_refs=["C1", "C2"],
    )

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def build_validation_packets(self, request):
            assert isinstance(request, ValidationPacketRequest)
            assert request.therapy_idea_id == therapy_idea_id
            assert request.dagster_run_id == "dagster-validation-packet-test"
            return ValidationPacketResult(
                packet_count=1,
                ready_count=1,
                packets=[
                    ValidationPacket(
                        packet_id="validation_packet:test",
                        candidate_id=candidate.candidate_id,
                        source_type="therapy_idea",
                        source_id=str(therapy_idea_id),
                        therapy_idea_id=therapy_idea_id,
                        promotion_candidate=candidate,
                        title=candidate.title,
                        hypothesis=candidate.hypothesis,
                        candidate_therapies=["pazopanib"],
                        targets=["KDR"],
                        evidence_refs=["C1", "C2"],
                        status="ready_for_review",
                        readiness="ready_for_validation_plan",
                        score=0.78,
                    )
                ],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.validation_packet_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "candidate_id": None,
                "therapy_idea_id": str(therapy_idea_id),
                "plan_id": None,
                "queue_item_id": None,
                "brief_id": None,
                "evaluation_id": None,
                "topic_query": None,
                "source_key": None,
                "include_queue_items": True,
                "queue_if_ready": False,
                "dry_run": True,
                "max_tasks": 8,
                "priority": 40,
                "limit": 10,
                "model_profile": "validation_packet_builder",
            },
            run_id="dagster-validation-packet-test",
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert result.value["packet_count"] == 1
    assert result.metadata["packet_count"].value == 1
    assert result.metadata["ready_count"].value == 1


def test_dagster_validation_decision_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    therapy_idea_id = uuid4()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def build_validation_decision_report(self, request):
            assert isinstance(request, ValidationDecisionReportRequest)
            assert request.therapy_idea_id == therapy_idea_id
            assert request.metadata["dagster_decision_run_id"] == "dagster-validation-decision-test"
            return ValidationDecisionReportResult(
                decision_count=1,
                packet_count=1,
                outcome_counts={"promote_broader_program": 1},
                decisions=[
                    ValidationDecisionPacket(
                        decision_id="validation_decision:test",
                        packet_id="validation_packet:test",
                        candidate_id=f"therapy_idea:{therapy_idea_id}",
                        source_type="therapy_idea",
                        source_id=str(therapy_idea_id),
                        therapy_idea_id=therapy_idea_id,
                        title="Sorafenib decision",
                        outcome="promote_broader_program",
                        confidence=0.72,
                        validation_ready=False,
                        specific_claim_viability="uncertain",
                        broader_program_signal="strong",
                        rationale="Promote the broader program.",
                        recommended_downstream_action="Create a Research Program Board item.",
                        recommended_program_thesis="Biomarker-stratified vascular/TKI program.",
                        decisive_questions=["Which subgroup benefits?"],
                        evidence_tasks=["Trace direct canine evidence."],
                    )
                ],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.validation_decision_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "candidate_id": None,
                "therapy_idea_id": str(therapy_idea_id),
                "plan_id": None,
                "queue_item_id": None,
                "brief_id": None,
                "evaluation_id": None,
                "topic_query": None,
                "source_key": None,
                "include_queue_items": True,
                "include_evidence_addendum": True,
                "include_source_packets": False,
                "addendum_limit": 25,
                "limit": 10,
            },
            run_id="dagster-validation-decision-test",
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert result.value["decision_count"] == 1
    assert result.metadata["decision_count"].value == 1
    assert result.metadata["outcome_counts"].data == {"promote_broader_program": 1}


def test_dagster_research_program_assets_use_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    program = _research_program_fixture()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def run_research_program_board(self, request):
            assert isinstance(request, ResearchProgramReviewRequest)
            assert request.dagster_run_id == "dagster-research-program-test"
            assert request.review_models == ["test/opus"]
            return ResearchProgramReviewResult(
                program_count=1,
                persisted_count=1,
                packet_count=0,
                evidence_chunk_count=0,
                programs=[program],
            )

        def list_research_programs(self, request):
            assert isinstance(request, ResearchProgramBoardRequest)
            assert request.thesis_query == "vascular"
            return ResearchProgramBoardResult(program_count=1, programs=[program])

        def run_research_program_evidence_loop(self, request):
            assert isinstance(request, ResearchProgramEvidenceLoopRequest)
            assert request.program_id == program.program_id
            assert request.dagster_run_id == "dagster-research-program-loop-test"
            return ResearchProgramEvidenceLoopResult(
                program_id=program.program_id,
                program_title=program.title,
                loop_count_before=0,
                loop_count_after=1,
                max_evidence_loops=2,
                task_count=1,
                selected_task_count=1,
                research_lead_count=1,
                source_query_count=2,
                brief_queue_count=1,
                program=program,
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    board_result = dagster_asset_module.research_program_board_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "thesis_topic": "vascular program",
                "disease_scope": "canine hemangiosarcoma and human angiosarcoma",
                "topic_query": None,
                "source_key": None,
                "max_packets": 5,
                "max_chunks": 20,
                "max_programs": 1,
                "max_evidence_loops": 2,
                "review_mode": "openrouter_required",
                "review_models": ["test/opus"],
                "model_profile": "research_program_board",
                "persist": True,
            },
            run_id="dagster-research-program-test",
        ),
        FakeRepositoryResource(),
    )
    library_result = dagster_asset_module.research_program_library_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "status": None,
                "gate_decision": None,
                "thesis_query": "vascular",
                "limit": 50,
            },
            run_id="dagster-research-program-library-test",
        ),
        FakeRepositoryResource(),
    )
    loop_result = dagster_asset_module.research_program_evidence_loop_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "program_id": str(program.program_id),
                "thesis_query": None,
                "source_keys": [],
                "max_tasks": 5,
                "max_source_queries": 20,
                "max_sources_per_task": 4,
                "queue_briefs": True,
                "create_research_leads": True,
                "create_source_queries": True,
                "priority": 40,
                "max_chunks_per_perspective": 10,
                "max_claims": 20,
                "max_chunk_chars": 2200,
                "brief_style": "technical",
                "model_profile": "research_brief",
                "review_mode": "openrouter_required",
                "review_models": [],
                "dry_run": False,
            },
            run_id="dagster-research-program-loop-test",
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository", "build_repository", "build_repository"]
    assert board_result.value["program_count"] == 1
    assert board_result.metadata["program_count"].value == 1
    assert board_result.metadata["persisted_count"].value == 1
    assert library_result.value["program_count"] == 1
    assert library_result.metadata["program_count"].value == 1
    assert loop_result.value["selected_task_count"] == 1
    assert loop_result.metadata["loop_count_after"].value == 1


def test_dagster_research_workspace_assets_use_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    workspace = ResearchWorkspaceRecord(
        candidate_id="twog-candidate-447eb8089965",
        work_packet_id="wp-dagster-1",
        provider="neon",
        neon_branch_name="twog-dagster",
        skill_profile="database_lookup",
        status="requested",
    )

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def provision_neon_research_workspace(self, request):
            assert isinstance(request, NeonBranchWorkspaceRequest)
            assert request.candidate_id == "twog-candidate-447eb8089965"
            assert request.dry_run is True
            assert request.metadata["dagster_run_id"] == "dagster-workspace-neon-test"
            return NeonBranchWorkspaceResult(
                workspace=workspace,
                dry_run=True,
                branch_name="twog-dagster",
            )

        def list_research_workspaces(self, request):
            assert isinstance(request, ResearchWorkspaceLibraryRequest)
            assert request.candidate_id == "twog-candidate-447eb8089965"
            assert request.provider == "neon"
            return ResearchWorkspaceLibraryResult(
                workspace_count=1,
                status_counts={"requested": 1},
                provider_counts={"neon": 1},
                workspaces=[workspace],
            )

        def cleanup_research_workspaces(self, request):
            assert isinstance(request, ResearchWorkspaceCleanupRequest)
            assert request.candidate_id == "twog-candidate-447eb8089965"
            assert request.dry_run is True
            assert request.metadata["dagster_run_id"] == "dagster-workspace-cleanup-test"
            return ResearchWorkspaceCleanupResult(
                dry_run=True,
                workspace_count=1,
                candidate_count=1,
                candidates=[
                    {
                        "workspace": workspace.model_dump(mode="json"),
                        "eligible": True,
                        "action": "dry_run",
                        "reason": "eligible_for_neon_branch_cleanup",
                    }
                ],
            )

        def build_research_workspace_checkout_manifest(self, request):
            assert isinstance(request, ResearchWorkspaceCheckoutManifestRequest)
            assert request.candidate_id == "twog-candidate-447eb8089965"
            assert request.work_packet_id == "wp-dagster-1"
            assert request.metadata["dagster_run_id"] == "dagster-workspace-manifest-test"
            result = build_research_workspace_checkout_manifest(
                InMemoryResearchRepository(),
                ResearchWorkspaceCheckoutManifestRequest(
                    candidate_id=request.candidate_id,
                    work_packet_id=request.work_packet_id,
                    method_refs=request.method_refs,
                    open_questions=request.open_questions,
                    persist_to_workspace=False,
                ),
            )
            return result.model_copy(update={"workspace": workspace, "persisted": False})

        def submit_proof_capsule(self, request):
            assert isinstance(request, ProofCapsuleSubmitRequest)
            assert request.candidate_id == "twog-candidate-447eb8089965"
            assert request.metadata["dagster_run_id"] == "dagster-proof-capsule-submit-test"
            capsule = ProofCapsuleRecord(
                workspace_id=request.workspace_id,
                checkout_manifest_hash=request.checkout_manifest_hash,
                candidate_id=request.candidate_id,
                work_packet_id=request.work_packet_id,
                packet_type=request.packet_type,
                requested_action=request.requested_action,
                target=request.target,
                summary=request.summary,
                payload=request.payload,
                content_hash="sha256:proof-capsule-test",
            )
            return ProofCapsuleSubmitResult(
                capsule=capsule,
                workspace=workspace,
                accepted=True,
                persisted=True,
            )

        def list_proof_capsules(self, request):
            assert isinstance(request, ProofCapsuleLibraryRequest)
            assert request.candidate_id == "twog-candidate-447eb8089965"
            capsule = ProofCapsuleRecord(
                workspace_id=workspace.workspace_id,
                checkout_manifest_hash="sha256:manifest",
                candidate_id=workspace.candidate_id,
                work_packet_id=workspace.work_packet_id,
                packet_type="citation_repair",
                requested_action="citation_repair",
                target=ProofCapsuleTarget(section="Literature"),
                summary=ProofCapsuleSummary(
                    title="Repair citation",
                    finding="Citation needs review.",
                    why_it_matters="It affects public proof quality.",
                    limitations=["Operator review is still required."],
                ),
                content_hash="sha256:proof-capsule-test",
            )
            return ProofCapsuleLibraryResult(
                capsule_count=1,
                status_counts={"submitted": 1},
                packet_type_counts={"citation_repair": 1},
                requested_action_counts={"citation_repair": 1},
                capsules=[capsule],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    neon_result = dagster_asset_module.research_workspace_neon_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "candidate_id": "twog-candidate-447eb8089965",
                "work_packet_id": "wp-dagster-1",
                "project_id": "project-test",
                "parent_branch_id": None,
                "parent_branch_name": None,
                "branch_name": "twog-dagster",
                "branch_name_prefix": "twog-workspace",
                "database_name": "neondb",
                "role_name": "neondb_owner",
                "ttl_hours": 24,
                "suspend_timeout_seconds": 300,
                "candidate_snapshot_hash": None,
                "evidence_bundle_hash": None,
                "checkout_manifest_hash": None,
                "git_repo": None,
                "git_ref": None,
                "git_branch": None,
                "artifact_root": None,
                "skill_profile": "database_lookup",
                "installed_skill_refs": [],
                "recommended_source_refs": [],
                "dry_run": True,
                "persist": True,
            },
            run_id="dagster-workspace-neon-test",
        ),
        FakeRepositoryResource(),
    )
    library_result = dagster_asset_module.research_workspace_library_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "workspace_id": None,
                "candidate_id": "twog-candidate-447eb8089965",
                "work_packet_id": None,
                "provider": "neon",
                "status": None,
                "statuses": [],
                "skill_profile": None,
                "include_expired": True,
                "limit": 50,
            },
            run_id="dagster-workspace-library-test",
        ),
        FakeRepositoryResource(),
    )
    cleanup_result = dagster_asset_module.research_workspace_cleanup_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "workspace_id": None,
                "candidate_id": "twog-candidate-447eb8089965",
                "work_packet_id": None,
                "provider": "neon",
                "expired_before": None,
                "limit": 50,
                "reason": "dagster_cleanup_test",
                "dry_run": True,
            },
            run_id="dagster-workspace-cleanup-test",
        ),
        FakeRepositoryResource(),
    )
    manifest_result = dagster_asset_module.research_workspace_checkout_manifest_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "workspace_id": None,
                "candidate_id": "twog-candidate-447eb8089965",
                "work_packet_id": "wp-dagster-1",
                "candidate_snapshot_hash": None,
                "evidence_bundle_hash": None,
                "method_refs": ["candidate-record-v1"],
                "open_questions": ["What should the reviewer check?"],
                "allowed_task_types": [],
                "expected_outputs": [],
                "artifact_refs": [],
                "git_repo": None,
                "git_ref": None,
                "git_branch": None,
                "database_secret_ref": None,
                "skill_profile": "core",
                "installed_skill_refs": [],
                "recommended_source_refs": [],
                "persist_to_workspace": False,
            },
            run_id="dagster-workspace-manifest-test",
        ),
        FakeRepositoryResource(),
    )
    proof_capsule_result = dagster_asset_module.proof_capsule_submit_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "capsule_json": json.dumps(
                    {
                        "workspace_id": str(workspace.workspace_id),
                        "checkout_manifest_hash": "sha256:manifest",
                        "candidate_id": "twog-candidate-447eb8089965",
                        "work_packet_id": "wp-dagster-1",
                        "packet_type": "citation_repair",
                        "requested_action": "citation_repair",
                        "target": {"section": "Literature"},
                        "summary": {
                            "title": "Repair citation",
                            "finding": "Citation needs review.",
                            "why_it_matters": "It affects public proof quality.",
                            "limitations": ["Operator review is still required."],
                        },
                    }
                ),
                "persist": True,
            },
            run_id="dagster-proof-capsule-submit-test",
        ),
        FakeRepositoryResource(),
    )
    proof_capsule_library_result = dagster_asset_module.proof_capsule_library_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "capsule_id": None,
                "workspace_id": None,
                "checkout_manifest_hash": None,
                "candidate_id": "twog-candidate-447eb8089965",
                "work_packet_id": None,
                "packet_type": None,
                "requested_action": None,
                "status": None,
                "statuses": [],
                "limit": 50,
            },
            run_id="dagster-proof-capsule-library-test",
        ),
        FakeRepositoryResource(),
    )

    assert calls == [
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
    ]
    assert neon_result.value["dry_run"] is True
    assert neon_result.metadata["branch_name"].value == "twog-dagster"
    assert library_result.value["workspace_count"] == 1
    assert library_result.metadata["provider_counts"].data == {"neon": 1}
    assert cleanup_result.value["candidate_count"] == 1
    assert cleanup_result.metadata["candidate_count"].value == 1
    assert manifest_result.value["manifest"]["content_hash"].startswith("sha256:")
    assert manifest_result.metadata["checkout_manifest_hash"].value.startswith("sha256:")
    assert proof_capsule_result.value["accepted"] is True
    assert proof_capsule_result.metadata["capsule_id"].value
    assert proof_capsule_library_result.value["capsule_count"] == 1
    assert proof_capsule_library_result.metadata["packet_type_counts"].data == {"citation_repair": 1}


def test_dagster_validation_plan_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    brief_id = uuid4()
    evaluation_id = uuid4()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def plan_validation(self, request):
            assert isinstance(request, ValidationPlanRequest)
            assert request.evaluation_id == evaluation_id
            return ValidationPlanResult(
                brief_id=brief_id,
                evaluation_id=evaluation_id,
                agent_run_id=uuid4(),
                topic="VEGF therapy",
                source_key="pubmed",
                status="ready_for_review",
                readiness="ready_for_expert_review",
                tasks=[
                    ValidationPlanTask(
                        task_type="expert_review",
                        title="Expert review",
                        objective="Assess the validation path.",
                        rationale="Ready brief.",
                        evidence_refs=["C1"],
                    )
                ],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.validation_plan_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "evaluation_id": str(evaluation_id),
                "require_ready_evaluation": True,
                "max_tasks": 8,
            },
            run_id="dagster-test-run",
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert result.value["brief_id"] == str(brief_id)
    assert result.metadata["readiness"] == "ready_for_expert_review"
    assert result.metadata["task_count"].value == 1


def test_dagster_research_brief_followup_queue_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    lead_id = uuid4()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def queue_research_brief_followups(self, request):
            assert isinstance(request, ResearchBriefFollowupQueueRequest)
            assert request.limit == 10
            assert request.dry_run is True
            return ResearchBriefFollowupQueueResult(
                candidate_brief_count=1,
                limitation_count=1,
                queued_count=0,
                dry_run=True,
                followup_leads=[
                    ResearchLeadRecord(
                        lead_id=lead_id,
                        identity_key=f"research_lead:brief_followup:{lead_id}",
                        title="Follow up evidence limitation",
                        status="followup",
                        evidence_refs=["research_brief:test"],
                    )
                ],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.research_brief_followup_queue_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "brief_ids": [],
                "evaluation_ids": [],
                "limit": 10,
                "include_evaluations": True,
                "max_limitations_per_brief": 20,
                "dry_run": True,
            }
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert result.value["candidate_brief_count"] == 1
    assert result.metadata["limitation_count"].value == 1


def test_dagster_research_followup_resolver_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []
    lead_id = uuid4()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def resolve_research_followups(self, request):
            assert isinstance(request, ResearchFollowupResolverRequest)
            assert request.lead_ids == [lead_id]
            assert request.dagster_run_id == "dagster-test-run"
            return ResearchFollowupResolverResult(
                agent_run_id=uuid4(),
                leads_seen=1,
                promoted_leads=1,
                lead_results=[
                    ResearchFollowupLeadResult(
                        lead_id=lead_id,
                        status_before="followup",
                        status_after="watching",
                        actions=["promoted_to_watching"],
                        evidence_refs=["chunk:1"],
                        durable_source_keys=["pubmed"],
                        promoted=True,
                    )
                ],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.research_followup_resolver_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "lead_ids": [str(lead_id)],
                "statuses": ["followup"],
                "source_keys": [],
                "search_source_keys": ["pubmed"],
                "limit": 25,
                "ingest_source_followups": True,
                "search_missing_identifiers": True,
                "promote_ready_leads": True,
                "run_claim_extraction": True,
                "dry_run": False,
                "min_evidence_chunks": 1,
                "search_limit_per_source": 2,
                "max_search_terms": 12,
            },
            run_id="dagster-test-run",
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert result.value["leads_seen"] == 1
    assert result.metadata["promoted_leads"].value == 1
    assert result.metadata["lead_results"].records[0].data["durable_source_keys"] == '["pubmed"]'


def test_dagster_full_text_source_specific_assets_use_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    def fake_pipeline(repository, **kwargs):
        assert repository is sentinel_repository
        return {
            **kwargs,
            "sources": [],
            "totals": {},
            "errors": [],
        }

    monkeypatch.setattr(structured_orchestration, "run_structured_sources_pipeline", fake_pipeline)
    monkeypatch.setattr(structured_orchestration, "run_structured_sources_ingestion_pipeline", fake_pipeline)

    europe_pmc_report = dagster_asset_module.europe_pmc_full_text_refresh_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    pmc_oa_report = dagster_asset_module.pmc_oa_full_text_refresh_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    ingest_smoke_report = dagster_asset_module.literature_full_text_ingest_smoke_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    europe_pmc_ingest_report = dagster_asset_module.europe_pmc_full_text_ingest_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    pmc_oa_ingest_report = dagster_asset_module.pmc_oa_full_text_ingest_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    smoke_report = dagster_asset_module.literature_full_text_smoke_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    partition_context = SimpleNamespace(
        multi_partition_key=dagster_asset_module.dg.MultiPartitionKey(
            {
                "source": "europe_pmc",
                "date": "2026-04-27",
            }
        )
    )
    partition_result = (
        dagster_asset_module.literature_full_text_source_date_report.node_def.compute_fn.decorated_fn(
            partition_context,
            FakeRepositoryResource(),
        )
    )

    assert calls == [
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
    ]
    assert europe_pmc_report["source_keys"] == ("europe_pmc",)
    assert europe_pmc_report["source_limits"] == {"europe_pmc": 10}
    assert europe_pmc_report["mode"] == "refresh"
    assert pmc_oa_report["source_keys"] == ("pmc_oa",)
    assert pmc_oa_report["source_limits"] == {"pmc_oa": 3}
    assert pmc_oa_report["mode"] == "refresh"
    assert ingest_smoke_report["source_keys"] == dagster_asset_module.LITERATURE_FULL_TEXT_SOURCE_KEYS
    assert ingest_smoke_report["source_limits"] == {"europe_pmc": 1, "pmc_oa": 1}
    assert ingest_smoke_report["mode"] == "ingestion_only"
    assert europe_pmc_ingest_report["source_keys"] == ("europe_pmc",)
    assert europe_pmc_ingest_report["source_limits"] == {"europe_pmc": 10}
    assert europe_pmc_ingest_report["mode"] == "ingestion_only"
    assert pmc_oa_ingest_report["source_keys"] == ("pmc_oa",)
    assert pmc_oa_ingest_report["source_limits"] == {"pmc_oa": 3}
    assert pmc_oa_ingest_report["mode"] == "ingestion_only"
    assert smoke_report["source_keys"] == dagster_asset_module.LITERATURE_FULL_TEXT_SOURCE_KEYS
    assert smoke_report["source_limits"] == {"europe_pmc": 1, "pmc_oa": 1}
    assert smoke_report["mode"] == "refresh"
    assert partition_result.value["source_keys"] == ("europe_pmc",)
    assert partition_result.value["source_limits"] == {"europe_pmc": 10}
    assert partition_result.value["partition_date"] == "2026-04-27"
    assert partition_result.value["mode"] == "source_date_partition"


def test_dagster_embedding_index_asset_uses_injected_repository(monkeypatch):
    calls = []

    class FakeRepository:
        def embedding_coverage(self, *, embedding_model=None, **kwargs):
            calls.append(("embedding_coverage", embedding_model, kwargs))
            return EmbeddingCoverageSummary(
                embedding_model=embedding_model,
                total_chunks=1,
                embedded_chunks=1,
                missing_chunks=0,
                coverage_ratio=1.0,
                embedding_models={embedding_model: 1},
            )

        def coverage_summary(self):
            calls.append(("coverage_summary",))
            return {"document_chunks": 1, "text_embeddings": 1}

    sentinel_repository = FakeRepository()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append(("build_repository",))
            return sentinel_repository

    def fake_index_embeddings_for_repository(repository, **kwargs):
        assert repository is sentinel_repository
        assert kwargs == {"embedding_model": "local-hash-v1"}
        calls.append(("index_embeddings_for_repository",))
        return EmbeddingIndexResult(
            embedding_model="local-hash-v1",
            chunks_seen=1,
            embeddings_created=1,
        )

    monkeypatch.setattr(
        "hsa_research.ingestion_bridge.embeddings.index_embeddings_for_repository",
        fake_index_embeddings_for_repository,
    )

    result = dagster_asset_module.embedding_index_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == [
        ("build_repository",),
        ("index_embeddings_for_repository",),
        ("embedding_coverage", "local-hash-v1", {}),
        ("coverage_summary",),
    ]
    assert isinstance(result, dagster_asset_module.dg.MaterializeResult)
    assert result.value["embedding_model"] == "local-hash-v1"
    assert result.value["totals"]["chunks_seen"] == 1
    assert result.value["totals"]["embeddings_created"] == 1
    assert result.value["embedding_coverage"]["embedded_chunks"] == 1
    assert result.value["passes_minimum_bar"] is True
    assert result.metadata["embedded_chunks"] == 1
    assert result.metadata["passes_minimum_bar"] is True


def test_dagster_embedding_maintenance_asset_uses_injected_repository(monkeypatch):
    calls = []

    class FakeRepository:
        pass

    sentinel_repository = FakeRepository()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append(("build_repository",))
            return sentinel_repository

    def fake_maintain_embedding_index(repository, **kwargs):
        assert repository is sentinel_repository
        assert kwargs == {"embedding_model": "local-hash-v1"}
        calls.append(("maintain_embedding_index",))
        return EmbeddingMaintenanceResult(
            embedding_model="local-hash-v1",
            prune_embedding_model=None,
            source_key=None,
            object_type=None,
            orphan_embeddings_seen=2,
            orphan_embeddings_deleted=2,
            prune_enabled=True,
            embedding_coverage=EmbeddingCoverageSummary(
                embedding_model="local-hash-v1",
                total_chunks=3,
                embedded_chunks=3,
                missing_chunks=0,
                coverage_ratio=1.0,
                embedding_models={"local-hash-v1": 3},
            ),
            coverage={"document_chunks": 3, "text_embeddings": 3},
        )

    monkeypatch.setattr(
        "hsa_research.ingestion_bridge.embeddings.maintain_embedding_index",
        fake_maintain_embedding_index,
    )

    result = dagster_asset_module.embedding_maintenance_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == [("build_repository",), ("maintain_embedding_index",)]
    assert isinstance(result, dagster_asset_module.dg.MaterializeResult)
    assert result.value["orphan_embeddings"]["seen"] == 2
    assert result.value["orphan_embeddings"]["deleted"] == 2
    assert result.value["embedding_coverage"]["missing_chunks"] == 0
    assert result.value["passes_minimum_bar"] is True
    assert result.metadata["orphan_embeddings_deleted"] == 2
    assert result.metadata["embedded_chunks"] == 3
    assert result.metadata["passes_minimum_bar"] is True


def test_research_brief_repository_roundtrip_sqlite_and_memory(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "research-brief-ledger.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        record = ResearchBriefRecord(
            agent_run_id=uuid4(),
            topic="VEGF therapy in canine hemangiosarcoma",
            disease_scope="canine hemangiosarcoma and human angiosarcoma",
            source_key="pubmed",
            brief_style="technical",
            model_profile="research_brief",
            review_mode="deterministic_only",
            final_brief="Stored synthesis [C1].",
            summary={"finding_count": 1},
            result_payload={"final_brief": "Stored synthesis [C1]."},
            citation_count=1,
            finding_count=1,
            research_lead_count=2,
            hard_error_count=1,
            evidence_limitation_count=3,
            error_count=1,
        )

        saved = repo.upsert_research_brief(record)
        fetched = repo.get_research_brief(saved.brief_id)
        listed = repo.list_research_briefs(source_key="pubmed", topic_query="vegf")

        assert fetched is not None
        assert fetched.brief_id == saved.brief_id
        assert fetched.result_payload["final_brief"] == "Stored synthesis [C1]."
        assert fetched.hard_error_count == 1
        assert fetched.evidence_limitation_count == 3
        assert fetched.error_count == 1
        assert listed[0].brief_id == saved.brief_id
        assert repo.list_research_briefs(status="archived") == []


def test_research_brief_evaluation_repository_roundtrip_sqlite_and_memory(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "research-brief-evaluations.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        brief_id = uuid4()
        evaluation = ResearchBriefEvaluationRecord(
            brief_id=brief_id,
            agent_run_id=uuid4(),
            topic="VEGF therapy in canine hemangiosarcoma",
            source_key="pubmed",
            overall_score=0.82,
            passes_quality_bar=True,
            readiness="ready_for_hypothesis_review",
            summary={"overall_score": 0.82},
            result_payload={"overall_score": 0.82, "recommendations": ["Promote."]},
        )

        saved = repo.upsert_research_brief_evaluation(evaluation)
        fetched = repo.get_research_brief_evaluation(saved.evaluation_id)
        listed = repo.list_research_brief_evaluations(
            brief_id=brief_id,
            readiness="ready_for_hypothesis_review",
            passes_quality_bar=True,
        )

        assert fetched is not None
        assert fetched.evaluation_id == saved.evaluation_id
        assert fetched.result_payload["overall_score"] == 0.82
        assert listed[0].evaluation_id == saved.evaluation_id
        assert repo.list_research_brief_evaluations(passes_quality_bar=False) == []


def test_validation_plan_repository_roundtrip_sqlite_and_memory(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "validation-plans.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        brief_id = uuid4()
        evaluation_id = uuid4()
        record = ValidationPlanRecord(
            brief_id=brief_id,
            evaluation_id=evaluation_id,
            agent_run_id=uuid4(),
            topic="VEGF validation path",
            source_key="pubmed",
            status="ready_for_review",
            readiness="ready_for_expert_review",
            task_count=2,
            hypothesis_count=1,
            result_payload={"plan_id": "payload"},
            summary={"task_count": 2},
        )

        saved = repo.upsert_validation_plan(record)
        fetched = repo.get_validation_plan(saved.plan_id)
        listed = repo.list_validation_plans(
            brief_id=brief_id,
            evaluation_id=evaluation_id,
            status="ready_for_review",
            readiness="ready_for_expert_review",
            limit=1,
        )

        assert fetched is not None
        assert fetched.plan_id == saved.plan_id
        assert listed[0].plan_id == saved.plan_id
        assert repo.list_validation_plans(status="blocked") == []


def test_validation_request_queue_repository_roundtrip_sqlite_and_memory(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "validation-request-queue.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        plan_id = uuid4()
        task_id = uuid4()
        item = repo.upsert_validation_request_queue_item(
            ValidationRequestQueueItem(
                plan_id=plan_id,
                task_id=task_id,
                brief_id=uuid4(),
                source_key="pubmed",
                topic="VEGF validation path",
                task_type="expert_review",
                title="Review target validation",
                objective="Review whether this target is ready for validation.",
                rationale="The plan is source-traceable.",
                priority=25,
                validation_request=ValidationRequest(
                    validation_type="expert_review",
                    target_name="VEGFA",
                    objective="Review whether this target is ready for validation.",
                ),
            )
        )
        duplicate = repo.upsert_validation_request_queue_item(
            item.model_copy(update={"priority": 50})
        )
        updated = repo.update_validation_request_queue_item(
            item.queue_item_id,
            status="approved",
            approved_by="unit-test",
            approval_note="Looks actionable.",
            quality_gates=["approval_required"],
            dispatch_blockers=["assay_context_required"],
        )
        listed = repo.list_validation_request_queue_items(
            plan_id=plan_id,
            status="approved",
            source_key="pubmed",
            task_type="expert_review",
            topic_query="VEGF",
            limit=1,
        )

        assert duplicate.queue_item_id == item.queue_item_id
        assert updated is not None
        assert updated.status == "approved"
        assert updated.approved_by == "unit-test"
        assert updated.approval_note == "Looks actionable."
        assert updated.quality_gates == ["approval_required"]
        assert updated.dispatch_blockers == ["assay_context_required"]
        assert listed[0].queue_item_id == item.queue_item_id
        assert repo.get_validation_request_queue_item(item.queue_item_id).queue_item_id == item.queue_item_id


def test_research_brief_queue_contract_and_repository_roundtrip(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "research-brief-queue.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        item = repo.upsert_research_brief_queue_item(
            ResearchBriefQueueItem(
                topic=" VEGF therapy in canine hemangiosarcoma ",
                disease_scope="canine hemangiosarcoma",
                source_key="pubmed",
                priority=10,
                review_mode="deterministic_only",
                review_models=["model-a", "model-a"],
            )
        )
        duplicate = repo.upsert_research_brief_queue_item(
            ResearchBriefQueueItem(
                topic="VEGF therapy in canine hemangiosarcoma",
                disease_scope="canine hemangiosarcoma",
                source_key="pubmed",
                priority=20,
                review_mode="deterministic_only",
            )
        )
        updated = repo.update_research_brief_queue_item(
            item.queue_item_id,
            status="running",
            priority=5,
            attempts=1,
            metadata={"runner": "test"},
        )

        assert item.identity_key is not None
        assert item.topic == "VEGF therapy in canine hemangiosarcoma"
        assert item.review_models == ["model-a"]
        assert duplicate.queue_item_id == item.queue_item_id
        assert updated is not None
        assert updated.status == "running"
        assert updated.priority == 5
        assert updated.attempts == 1
        assert updated.metadata["runner"] == "test"
        assert repo.get_research_brief_queue_item(item.queue_item_id).queue_item_id == item.queue_item_id
        assert repo.list_research_brief_queue_items(status="running", source_key="pubmed", topic_query="vegf")[0].queue_item_id == item.queue_item_id


def test_research_followup_resolver_uses_stored_durable_chunks_before_promotion(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-chunks.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_minimal_source_claim(repo, "pubmed")
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="PubMed source record canine HSA",
            lead_type="linked_article",
            status="followup",
            priority=20,
            source_key="x_topic",
            origin_source_key="x_topic",
            reason="Needs durable stored evidence.",
            topic_tags=["canine", "hemangiosarcoma"],
        )
    )

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_source_keys=["pubmed"],
            ingest_source_followups=False,
            search_missing_identifiers=False,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.promoted_leads == 1
    assert result.lead_results[0].durable_source_keys == ["pubmed"]
    assert any(ref.startswith("chunk:") for ref in result.lead_results[0].evidence_refs)
    inspection = result.lead_results[0].metadata["evidence_inspection"]
    assert inspection["inspected_count"] >= 1
    assert inspection["records"][0]["source_key"] == "pubmed"
    assert "canine hemangiosarcoma" in inspection["records"][0]["text_preview"]
    assert updated is not None
    assert updated.status == "watching"
    assert updated.source_key == "pubmed"
    assert updated.suggested_sources == ["pubmed"]


def test_compute_jobs_round_trip_and_filter_in_sqlite(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "compute-jobs.sqlite3", seed=False)
    queue_item_id = uuid4()
    record = repo.upsert_compute_job(
        ComputeJobRecord(
            queue_item_id=queue_item_id,
            status="approved",
            runner_kind="modal",
            compute_profile="gpu_l4",
            validation_type="docking",
            title="Dock candidate A against KDR",
            objective="Create a durable compute job without live submission.",
        )
    )

    fetched = repo.get_compute_job(record.compute_job_id)
    filtered = repo.list_compute_jobs(status="approved", runner_kind="modal", queue_item_id=queue_item_id)
    updated = repo.update_compute_job(
        record.compute_job_id,
        status="submitted",
        external_run_id=f"dry-run:{record.compute_job_id}",
        metadata={"submission_mode": "dry_run"},
    )

    assert fetched == record
    assert filtered == [record]
    assert updated is not None
    assert updated.status == "submitted"
    assert updated.submitted_at is not None
    assert updated.metadata["submission_mode"] == "dry_run"


def test_europe_pmc_v2_normalizer_can_store_licensed_full_text():
    record = EuropePMCHarvesterV2().normalize(
        {
            "id": "PMC123",
            "pmcid": "PMC123",
            "title": "Endothelial biology review",
            "abstractText": "Sparse abstract.",
            "isOpenAccess": "Y",
        },
        full_text_xml="""
        <article xmlns="http://jats.nlm.nih.gov">
          <front>
            <article-meta>
              <article-id pub-id-type="pmc">PMC123</article-id>
            </article-meta>
          </front>
          <body>
            <sec>
              <title>Results</title>
              <p>Human angiosarcoma full text mentions VEGF and propranolol.</p>
            </sec>
          </body>
        </article>
        """,
    )
    harvester = EuropePMCHarvesterV2()

    assert record.raw_record.raw_payload["full_text"] == "Results Human angiosarcoma full text mentions VEGF and propranolol."
    assert record.raw_record.raw_payload["full_text_sections"] == [
        {
            "section_label": "full_text:results",
            "title": "Results",
            "text": "Results Human angiosarcoma full text mentions VEGF and propranolol.",
        }
    ]
    assert record.research_object.metadata["full_text_available"] is True
    assert record.research_object.metadata["body_only_match"] is True
    assert record.research_object.metadata["body_ingestion_policy"]["matched_concepts"] == ["human_angiosarcoma"]
    assert harvester.chunk_section_label(record) == "full_text"
    assert "full text mentions VEGF" in harvester.text_for_chunking(record)
    sections = harvester.chunk_text_sections(record)
    assert [section_label for section_label, _text in sections] == ["title_abstract", "full_text:results"]
    assert "Sparse abstract" in sections[0][1]
    assert "Sparse abstract" not in sections[1][1]
    assert "full text mentions VEGF" in sections[1][1]


def test_x_topic_monitor_requires_review_before_storage_contracts():
    payload = {
        "id": "123",
        "author_id": "456",
        "author": {"username": "vetonc"},
        "created_at": "2026-04-28T10:00:00Z",
        "lang": "en",
        "text": "Canine hemangiosarcoma signal.",
    }
    candidate = x_topic_monitor.normalize_post_payload(payload, query_name="x_disease_monitoring")

    with pytest.raises(ValueError):
        x_topic_monitor.to_research_record(candidate, payload, accepted_by="operator")

    accepted = candidate.model_copy(update={"review_status": x_topic_monitor.XReviewStatus.ACCEPTED_SIGNAL})
    record = x_topic_monitor.to_research_record(accepted, payload, accepted_by="operator")

    assert record.raw_record.source_key == "x_topic_monitor"
    assert record.research_object.object_type == ResearchObjectType.KNOWLEDGE_ENTRY
    assert record.research_object.dedupe_key == "x_topic_monitor:post:123"
    assert record.document_chunk.section_label == "x_topic_signal"
    assert "text retention mode=store_metadata_only" in record.document_chunk.text_content


def test_source_versions_and_primitive_call_events_round_trip_in_sqlite(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    source_version = repo.upsert_source_version(
        SourceVersionRecord(source_key="hgnc", source_version="2026.05", source_url="https://www.genenames.org/")
    )
    event = repo.create_primitive_call_event(
        PrimitiveCallEvent(
            primitive_name="entity_lookup",
            request_hash="request-hash",
            result_hash="result-hash",
            source_versions={"hgnc": "2026.05"},
            input_payload={"query": "VEGFR2"},
            output_payload={"canonical_id": "entrezgene:3791"},
        )
    )

    assert repo.list_source_versions(source_key="hgnc")[0].source_version == source_version.source_version
    assert repo.list_primitive_call_events(primitive_name="entity_lookup")[0].event_id == event.event_id


def test_local_store_persists_raw_and_research_object(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    record = OpenAlexHarvesterV2().normalize(
        {
            "id": "https://openalex.org/W456",
            "doi": "https://doi.org/10.1234/local",
            "title": "Local persistence example",
            "publication_year": 2026,
            "abstract_inverted_index": {"Local": [0], "object": [1]},
            "primary_location": {"landing_page_url": "https://doi.org/10.1234/local"},
        }
    )

    fetch_run_id = repo.create_fetch_run("openalex", "unit_test")
    raw_id = repo.upsert_raw_record(record.raw_record, fetch_run_id)
    object_id = repo.upsert_research_object(record.research_object, raw_id)
    saved = repo.get_research_object(object_id)

    assert saved is not None
    assert saved.title == "Local persistence example"
    assert repo.coverage_summary()["research_objects"] == 1


def test_sqlite_text_embeddings_persist_and_report_coverage(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Embedding persistence example",
            source_key="pubmed",
            dedupe_key="pubmed:embedding-persistence",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGF signaling is discussed in canine hemangiosarcoma.",
            content_hash="embedding-chunk-1",
        )
    )

    saved = repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunk.id,
            research_object_id=object_id,
            chunk_index=chunk.chunk_index,
            source_key="pubmed",
            object_type="publication",
            content_hash=chunk.content_hash,
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[1.0, 0.0, 0.0],
            text_preview=chunk.text_content,
        )
    )

    fetched = repo.get_text_embedding(saved.embedding_id)
    listed = repo.list_text_embeddings(source_key="pubmed", embedding_model="unit-embedding-v1")
    coverage = repo.embedding_coverage(source_key="pubmed", embedding_model="unit-embedding-v1")

    assert fetched == saved
    assert listed == [saved]
    assert repo.coverage_summary()["text_embeddings"] == 1
    assert coverage.total_chunks == 1
    assert coverage.embedded_chunks == 1
    assert coverage.missing_chunks == 0
    assert coverage.coverage_ratio == 1.0
    assert coverage.embedding_models == {"unit-embedding-v1": 1}


def test_index_embeddings_for_repository_is_idempotent_and_includes_entities(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Embedding index example",
            source_key="pubmed",
            dedupe_key="pubmed:embedding-index",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGF receptor 2 appears in canine hemangiosarcoma literature.",
            content_hash="embedding-index-chunk-1",
        )
    )
    repo.upsert_entity_mention(
        EntityMention(
            research_object_id=object_id,
            chunk_id=chunk.id,
            chunk_index=chunk.chunk_index,
            section_label=chunk.section_label,
            source_key="pubmed",
            entity_type="target",
            canonical_name="KDR",
            normalized_key="target:kdr",
            matched_text="VEGF receptor 2",
            matched_alias="VEGF receptor 2",
            chunk_char_start=0,
            chunk_char_end=15,
            resolver_name="unit",
            resolver_version="1",
            match_rule="unit",
        )
    )

    first_result = index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    first_embedding = repo.list_text_embeddings(source_key="pubmed", embedding_model="local-hash-test")[0]
    second_result = index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    second_embedding = repo.list_text_embeddings(source_key="pubmed", embedding_model="local-hash-test")[0]

    assert first_result.errors == ()
    assert first_result.chunks_seen == 1
    assert first_result.embeddings_created == 1
    assert second_result.embeddings_skipped == 1
    assert second_result.embeddings_created == 0
    assert second_embedding.embedding_id == first_embedding.embedding_id
    assert second_embedding.embedding == first_embedding.embedding
    assert len(repo.list_text_embeddings(embedding_model="local-hash-test")) == 1
    assert "canonical_entities: target: KDR [target:kdr]" in second_embedding.text_preview
    assert second_embedding.metadata["chunk_content_hash"] == chunk.content_hash
    assert second_embedding.metadata["canonical_entity_count"] == 1


def test_index_embeddings_for_repository_uses_configured_provider(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="OpenRouter embedding index example",
            source_key="pubmed",
            dedupe_key="pubmed:openrouter-embedding-index",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Sorafenib safety in dogs.",
            content_hash="openrouter-embedding-index-chunk",
        )
    )

    class FakeProvider:
        embedding_model = "openrouter:unit-embedding"
        provider_name = "openrouter"
        provider_model = "unit-embedding"

        @property
        def embedding_dimensions(self):
            return 3

        def embed_text(self, text):
            return [1.0, 0.0, 0.0]

        def embed_texts(self, texts):
            return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr(
        "hsa_research.ingestion_bridge.embeddings.build_embedding_provider",
        lambda embedding_model, dimensions=None: FakeProvider(),
    )

    result = index_embeddings_for_repository(
        repo,
        source_key="pubmed",
        embedding_model="openrouter:unit-embedding",
        batch_size=8,
    )
    embedding = repo.list_text_embeddings(embedding_model="openrouter:unit-embedding")[0]

    assert result.errors == ()
    assert result.embeddings_created == 1
    assert embedding.embedding == [1.0, 0.0, 0.0]
    assert embedding.metadata["provider"] == "openrouter"
    assert embedding.metadata["provider_model"] == "unit-embedding"


def test_sqlite_text_embedding_search_uses_json_vectors_and_upserts_by_chunk_model(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Embedding search example",
            source_key="pubmed",
            dedupe_key="pubmed:embedding-search",
        )
    )
    chunks = [
        repo.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=index,
                section_label="abstract",
                text_content=text,
                content_hash=f"embedding-search-{index}",
            )
        )
        for index, text in enumerate(
            [
                "VEGF signaling and angiogenesis in hemangiosarcoma.",
                "Chemotherapy toxicity monitoring in dogs.",
            ]
        )
    ]
    first = repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunks[0].id,
            research_object_id=object_id,
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash=chunks[0].content_hash,
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[1.0, 0.0, 0.0],
        )
    )
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunks[1].id,
            research_object_id=object_id,
            chunk_index=1,
            source_key="pubmed",
            object_type="publication",
            content_hash=chunks[1].content_hash,
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[0.0, 1.0, 0.0],
        )
    )

    results = repo.search_text_embeddings(
        TextEmbeddingSearchRequest(
            query_embedding=[0.95, 0.05, 0.0],
            embedding_model="unit-embedding-v1",
            source_key="pubmed",
            limit=2,
        )
    )

    updated = repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunks[0].id,
            research_object_id=object_id,
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash="embedding-search-0-updated",
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[0.0, 0.0, 1.0],
        )
    )
    updated_results = repo.search_text_embeddings(
        TextEmbeddingSearchRequest(query_embedding=[0.0, 0.0, 1.0], embedding_model="unit-embedding-v1")
    )

    assert [result.embedding.chunk_id for result in results] == [chunks[0].id, chunks[1].id]
    assert results[0].score > results[1].score
    assert updated.embedding_id == first.embedding_id
    assert updated.content_hash == "embedding-search-0-updated"
    assert len(repo.list_text_embeddings(embedding_model="unit-embedding-v1")) == 2
    assert updated_results[0].embedding.embedding_id == first.embedding_id


def test_scrape_bridge_stores_snapshot_and_parses_generic_html(tmp_path, monkeypatch):
    html_path = tmp_path / "trial.html"
    html_path.write_text(
        """
        <html>
          <head><title>Canine Hemangiosarcoma Trial</title></head>
          <body><a href="/trial/1">Trial detail</a></body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="generic_html",
        storage_policy="metadata_only",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    fetch = bridge.fetch(ScrapeFetchRequest(source_key="test_scraper", urls=[html_path.as_uri()]))
    artifact = repo.get_artifact(fetch.artifact_ids[0])
    parse = bridge.parse("test_scraper")

    assert fetch.fetched_pages == 1
    assert artifact is not None
    assert artifact.artifact_type == "scrape_snapshot"
    assert artifact.metadata["source_key"] == "test_scraper"
    assert artifact.metadata["requires_review"] is True
    assert parse.artifacts_seen == 1
    assert parse.parsed_records == 1
    assert len(parse.review_ids) == 1
    assert parse.records[0].title == "Canine Hemangiosarcoma Trial"
    assert parse.records[0].record_type == "veterinary_trial"
    assert parse.records[0].review_status == "needs_review"
    reviews = repo.list_scrape_reviews(source_key="test_scraper", review_status="needs_review")
    assert len(reviews) == 1
    assert reviews[0].title == "Canine Hemangiosarcoma Trial"


def test_source_followup_queue_roundtrips_sqlite_and_memory(tmp_path):
    sqlite_repo = SQLiteResearchRepository(tmp_path / "followups.sqlite3", seed=False)
    memory_repo = InMemoryResearchRepository()

    for repo in (sqlite_repo, memory_repo):
        review = repo.upsert_scrape_review(
            ScrapeReviewRecord(
                source_key="x_linked_article",
                artifact_id=uuid4(),
                source_record_id="article-1",
                title="Angiosarcoma article",
                canonical_url="https://example.edu/article",
                parser_confidence=0.7,
                fields={
                    "primary_source_links": [
                        {
                            "url": "https://doi.org/10.1234/test",
                            "recommended_source_key": "crossref",
                            "identifier_type": "doi",
                            "identifier": "10.1234/Test",
                            "should_ingest": True,
                            "reason": "DOI found.",
                        }
                    ]
                },
            )
        )

        queued = HSAResearchService(repo).queue_source_followups(
            SourceFollowupQueueRequest(review_ids=[review.review_id])
        )
        queued_again = HSAResearchService(repo).queue_source_followups(
            SourceFollowupQueueRequest(review_ids=[review.review_id])
        )
        queued_existing = HSAResearchService(repo).queue_source_followups(
            SourceFollowupQueueRequest(review_ids=[review.review_id], include_existing=True)
        )
        rows = HSAResearchService(repo).list_source_followups(source_key="crossref")

        assert queued.queued == 1
        assert queued_again.skipped_existing == 1
        assert queued_existing.queued == 0
        assert len(queued_existing.items) == 1
        assert len(rows) == 1
        assert rows[0].identifier == "10.1234/test"
        assert rows[0].status == "queued"


def test_scrape_manifest_discovers_avma_candidate_urls_from_stored_seed_page(tmp_path, monkeypatch):
    seed_path = tmp_path / "seed.html"
    detail_dir = tmp_path / "s"
    detail_dir.mkdir()
    detail_path = detail_dir / "combination-therapy-hsa-123456.html"
    detail_path.write_text(
        "<html><head><title>Combination therapy in canine hemangiosarcoma</title></head><body></body></html>",
        encoding="utf-8",
    )
    seed_path.write_text(
        f"""
        <html>
          <body>
            <a href="{detail_path.as_uri()}">Hemangiosarcoma clinical trial</a>
            <a href="{(tmp_path / "about.html").as_uri()}">About</a>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="avma_vctr_test",
        display_name="AVMA Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="avma_vctr",
        storage_policy="link_and_registry_metadata",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")
    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[seed_path.as_uri()]))

    manifest = bridge.build_manifest(ScrapeManifestRequest(source_key="avma_vctr_test"))
    manifest_artifact = repo.get_artifact(manifest.manifest_artifact_id)

    assert manifest.seed_artifacts_seen == 1
    assert len(manifest.candidate_urls) == 1
    assert manifest.candidate_urls[0].url == detail_path.as_uri()
    assert manifest.candidate_urls[0].confidence == 0.8
    assert manifest_artifact.artifact_type == "scrape_manifest"
    assert manifest_artifact.metadata["candidate_count"] == 1
