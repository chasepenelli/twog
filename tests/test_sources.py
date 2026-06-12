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

def test_public_candidate_integrity_report_flags_missing_source_and_manifest():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    candidate_id = "twog-candidate-integrity"
    therapy_idea_id = uuid4()
    snapshot_id = uuid4()
    trace_id = uuid4()
    manifest_id = uuid4()

    candidate = PublicCandidateRecord(
        candidate_id=candidate_id,
        trace_id=trace_id,
        title="Integrity candidate",
        visibility="draft_public",
        therapy_idea_id=therapy_idea_id,
        latest_snapshot_id=snapshot_id,
    )
    snapshot = PublicCandidateSnapshot(
        snapshot_id=snapshot_id,
        trace_id=trace_id,
        candidate_id=candidate_id,
        snapshot_version=1,
        content_hash="hash-integrity",
        title="Integrity candidate",
        metadata={"run_manifest_id": str(manifest_id), "trace_id": str(trace_id)},
        payload={"reproducibility": {"run_manifest_id": str(manifest_id), "trace_id": str(trace_id)}},
    )
    repo.upsert_public_candidate(candidate)
    repo.upsert_public_candidate_snapshot(snapshot)

    missing = service.build_public_candidate_integrity_report(
        PublicCandidateIntegrityReportRequest(
            candidate_ids=[candidate_id],
            expected_candidate_therapy_ids={candidate_id: therapy_idea_id},
        )
    )
    assert missing.strict_export_ready is False
    assert missing.checks[0].candidate_found is True
    assert missing.checks[0].latest_snapshot_found is True
    assert missing.checks[0].run_manifest_found is False
    assert missing.checks[0].therapy_idea_found is False
    assert f"therapy_idea_missing:{therapy_idea_id}" in missing.checks[0].problems
    assert f"run_manifest_missing:{manifest_id}" in missing.checks[0].problems

    idea = TherapyIdea(
        title="Integrity therapy idea",
        hypothesis="A public candidate needs source therapy provenance.",
        rationale="Used to test public candidate export readiness.",
        candidate_therapies=["test therapy"],
        targets=["KDR"],
        evidence_refs=["C1"],
        priority_score=0.9,
    ).model_copy(update={"idea_id": therapy_idea_id})
    repo.upsert_therapy_idea(TherapyIdeaRecord(idea=idea, status="ready_for_promotion", score=0.9))
    repo.upsert_run_manifest(
        RunManifestRecord(
            manifest_id=manifest_id,
            trace_id=trace_id,
            manifest_type="public_candidate_snapshot",
            status="completed",
            candidate_ids=[candidate_id],
            therapy_idea_ids=[therapy_idea_id],
        )
    )

    ready = service.build_public_candidate_integrity_report(
        PublicCandidateIntegrityReportRequest(
            candidate_ids=[candidate_id],
            expected_candidate_therapy_ids={candidate_id: therapy_idea_id},
        )
    )
    assert ready.strict_export_ready is True
    assert ready.checks[0].strict_export_ready is True
    assert ready.candidates_ready_for_strict_export == [candidate_id]


def test_evidence_ref_repair_report_resolves_source_qualified_text_refs(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "evidence-ref-repair-qualified.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief, _evaluation = _seed_evaluated_brief(repo, topic="Qualified citation packet", duplicate_count=0)
    explicit_ref = f"research_brief:{brief.brief_id}#C2"
    repo.upsert_research_brief(
        brief.model_copy(
            update={
                "final_brief": f"Use the source-qualified comparator ref {explicit_ref}.",
                "updated_at": datetime.now(UTC),
            }
        )
    )

    report = service.build_evidence_ref_repair_report(
        EvidenceRefRepairRequest(brief_id=brief.brief_id, include_validation_packet=False)
    )

    assert any(
        item.normalized_ref == explicit_ref
        and item.status == "resolved"
        and item.matched_citation_id == "C2"
        for item in report.items
    )
    assert not any(item.ref == "C2" and item.evidence_snippet == explicit_ref for item in report.items)


def test_research_lead_contracts_validate_and_normalize_identity():
    lead = ResearchLeadRecord(
        title="  AACR HSA Abstract  ",
        url="https://example.edu/news/hsa#section",
        lead_type="institutional_article",
        status="new",
        topic_tags=["HSA", "hsa", "Angiosarcoma"],
        suggested_sources=["PubMed", "pubmed"],
    )

    assert lead.title == "AACR HSA Abstract"
    assert lead.url == "https://example.edu/news/hsa"
    assert lead.identity_key == "research_lead:url:https://example.edu/news/hsa"
    assert lead.topic_tags == ["hsa", "angiosarcoma"]
    assert lead.suggested_sources == ["pubmed"]
    assert ResearchLeadCollectRequest().agent_names == ["x_linked_article_review_agent", "x_topic_review_agent"]
    with pytest.raises(ValueError):
        ResearchLeadRecord(title="x", lead_type="bad")
    with pytest.raises(ValueError):
        ResearchLeadRecord(title="x", status="bad")


def test_full_text_ingestion_pipeline_skips_downstream_work(monkeypatch):
    calls = []

    class FakeIngestionResult:
        def model_dump(self, mode):
            assert mode == "json"
            return {
                "source_key": "europe_pmc",
                "query_name": "licensed_full_text_hsa",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 2,
                "full_text_research_objects": 1,
                "section_chunk_counts": {"title_abstract": 1, "full_text": 1},
                "status": "completed",
                "errors": [],
            }

    class FakePipeline:
        def __init__(self, repository):
            calls.append(("init", repository))

        def initialize(self):
            calls.append(("initialize",))

        def ingest_source(self, source_key, limit):
            calls.append(("ingest_source", source_key, limit))
            return [FakeIngestionResult()]

    class FakeRepository:
        def source_runtime_summary(self, source_key, sample_limit=5):
            assert source_key == "europe_pmc"
            return {
                "source_key": source_key,
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 2,
                "entity_mentions": 0,
                "claims": 0,
            }

        def list_research_objects(self, source_key=None):
            assert source_key == "europe_pmc"
            return [SimpleNamespace(metadata={"full_text_available": True})]

        def list_document_chunks(self, source_key=None):
            assert source_key == "europe_pmc"
            return [
                SimpleNamespace(section_label="title_abstract", text_content="title"),
                SimpleNamespace(section_label="full_text", text_content="body"),
            ]

        def coverage_summary(self):
            return {"document_chunks": 2}

    def fail_downstream(*args, **kwargs):
        raise AssertionError("ingestion-only pipeline must not run downstream claim work")

    monkeypatch.setattr(structured_orchestration, "LocalIngestionPipeline", FakePipeline)
    monkeypatch.setattr(structured_orchestration, "resolve_entities_for_repository", fail_downstream)
    monkeypatch.setattr(structured_orchestration, "extract_claims_for_repository", fail_downstream)
    monkeypatch.setattr(structured_orchestration, "curate_claims_for_repository", fail_downstream)

    report = structured_orchestration.run_structured_sources_ingestion_pipeline(
        FakeRepository(),
        source_keys=("europe_pmc",),
        source_limits={"europe_pmc": 1},
    )

    assert calls[-1] == ("ingest_source", "europe_pmc", 1)
    assert report["mode"] == "ingestion_only"
    assert report["totals"]["document_chunks"] == 2
    source_report = report["sources"][0]
    assert source_report["entity_resolution"]["status"] == "skipped"
    assert source_report["extraction"]["status"] == "skipped"
    assert source_report["curation"]["status"] == "skipped"
    assert source_report["full_text_qa"]["passes_full_text_bar"] is True
    assert source_report["full_text_qa"]["triage"]["action"] == "no_action"
    assert source_report["full_text_triage_action"] == "no_action"


def test_full_text_partition_allows_empty_current_day(monkeypatch):
    calls = []

    class EmptyIngestionResult:
        def model_dump(self, mode):
            assert mode == "json"
            return {
                "source_key": "europe_pmc",
                "query_name": "comparative_hsa_open_access:partition_2026-04-27",
                "raw_records": 0,
                "research_objects": 0,
                "document_chunks": 0,
                "full_text_research_objects": 0,
                "section_chunk_counts": {},
                "status": "completed",
                "errors": [],
            }

    class FakePipeline:
        def __init__(self, repository):
            calls.append(("init", repository))

        def initialize(self):
            calls.append(("initialize",))

        def ingest_source(self, source_key, limit=25, **kwargs):
            calls.append(("ingest_source", source_key, limit, kwargs))
            return [EmptyIngestionResult()]

    class FakeRepository:
        def source_runtime_summary(self, source_key, sample_limit=5):
            return {
                "source_key": source_key,
                "raw_records": 0,
                "research_objects": 0,
                "document_chunks": 0,
                "entity_mentions": 0,
                "claims": 0,
            }

        def list_research_objects(self, source_key=None):
            return []

        def list_document_chunks(self, source_key=None):
            return []

        def coverage_summary(self):
            return {}

    monkeypatch.setattr(structured_orchestration, "LocalIngestionPipeline", FakePipeline)

    report = structured_orchestration.run_structured_sources_ingestion_pipeline(
        FakeRepository(),
        source_keys=("europe_pmc",),
        source_limits={"europe_pmc": 1},
        partition_date="2026-04-27",
    )

    source_report = report["sources"][0]
    assert calls[-1] == (
        "ingest_source",
        "europe_pmc",
        1,
        {
            "query_param_overrides": {
                "published_after": "2026-04-27",
                "published_before": "2026-04-27",
            },
            "query_name_suffix": "partition_2026-04-27",
        },
    )
    assert report["partition_date"] == "2026-04-27"
    assert source_report["full_text_qa"]["current_empty_passes"] is True
    assert source_report["full_text_qa"]["passes_full_text_bar"] is True
    assert source_report["full_text_qa"]["triage"]["action"] == "no_action"


def test_full_text_ops_ready_when_health_and_partition_are_clean(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-ready.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    _seed_full_text_source_claim(repo, "pmc_oa")
    partition_report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "sources": [
            {"source_key": "europe_pmc", "full_text_qa": {"passes_full_text_bar": True}},
            {"source_key": "pmc_oa", "full_text_qa": {"current_empty_passes": True}},
        ],
        "errors": [],
    }

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            partition_date="2026-04-27",
            full_text_report=partition_report,
            review_mode="deterministic_only",
        )
    )

    assert result.schedule_readiness == "ready_to_enable"
    assert result.should_block_schedule is False
    assert any(action.action == "ready_to_enable_schedule" for action in result.actions)
    assert {action.action for action in result.actions if action.source_key != "all"} == {"mark_clean"}


def test_full_text_ops_external_review_packet_includes_deterministic_guardrail(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-external-review.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    partition_report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "sources": [
            {"source_key": "europe_pmc", "full_text_qa": {"passes_full_text_bar": True}},
        ],
        "errors": [],
    }

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            full_text_report=partition_report,
            review_mode="external_required",
        )
    )

    assert result.schedule_readiness == "keep_stopped"
    assert result.should_block_schedule is True
    assert any(action.action == "needs_human_review" for action in result.actions)
    assert result.evidence["external_reviewer"]["provider"] == "openai_chatgpt_pro"
    assert result.evidence["review_packet"]["deterministic_guardrail_result"]["schedule_readiness"] == "ready_to_enable"


def test_full_text_ops_openrouter_compare_records_each_model(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-openrouter.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    repo.create_agent_run(
        AgentRunRecord(
            agent_name="full_text_ops_agent",
            status=RunStatus.COMPLETED,
            source_key="europe_pmc",
            partition_date="2026-04-26",
            output_payload={
                "schedule_readiness": "keep_stopped",
                "should_block_schedule": True,
                "actions": [{"source_key": "all", "action": "needs_human_review", "severity": "watch"}],
                "evidence": {
                    "review_packet": {"large_nested_payload": "x" * 10000},
                    "model_reviews": [{"model_name": "old", "status": "failed", "error": "old failure"}],
                },
            },
            summary={"actions": 1},
            errors=["old failure"],
        )
    )
    partition_report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "sources": [
            {"source_key": "europe_pmc", "full_text_qa": {"passes_full_text_bar": True}},
        ],
        "errors": [],
    }

    def fake_review_model(model_name, review_payload):
        assert review_payload["deterministic_guardrail_result"]["schedule_readiness"] == "ready_to_enable"
        assert not _contains_key(review_payload, "output_payload")
        assert not _contains_key(review_payload, "review_packet")
        assert review_payload["recent_agent_runs"][0]["output"]["model_review_statuses"][0]["model_name"] == "old"
        return {
            "text": json.dumps(
                {
                    "agent_name": "full_text_ops_agent",
                    "model_profile": "reviewer",
                    "schedule_readiness": "ready_to_enable",
                    "should_block_schedule": False,
                    "actions": [
                        {
                            "source_key": "all",
                            "action": "ready_to_enable_schedule",
                            "severity": "info",
                            "reason": f"{model_name} agrees the evidence is clean.",
                            "evidence_refs": ["deterministic_guardrail_result"],
                        }
                    ],
                    "evidence": {"model_name": model_name},
                    "errors": [],
                }
            ),
            "metadata": {"provider": "openrouter", "model_name": model_name},
        }

    monkeypatch.setattr(full_text_ops, "_openrouter_review_model", fake_review_model)

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            full_text_report=partition_report,
            review_mode="openrouter_compare",
            review_models=["openai/gpt-5.1", "anthropic/claude-sonnet-4.6"],
        )
    )

    assert result.schedule_readiness == "ready_to_enable"
    assert result.evidence["selected_model"] == "openai/gpt-5.1"
    assert [review["model_name"] for review in result.evidence["model_reviews"]] == [
        "openai/gpt-5.1",
        "anthropic/claude-sonnet-4.6",
    ]
    assert all(review["status"] == "completed" for review in result.evidence["model_reviews"])


def test_full_text_ops_openrouter_defaults_to_sonnet_latest(monkeypatch):
    monkeypatch.delenv("HSA_FULL_TEXT_OPS_MODEL", raising=False)
    monkeypatch.delenv("HSA_FULL_TEXT_OPS_REVIEW_MODELS", raising=False)

    assert full_text_ops._review_models(FullTextOpsRequest(review_mode="openrouter_required")) == [
        "~anthropic/claude-sonnet-latest"
    ]
    assert full_text_ops._review_models(FullTextOpsRequest(review_mode="openrouter_compare")) == [
        "~anthropic/claude-sonnet-latest"
    ]


def test_full_text_ops_openrouter_compare_persists_model_failures(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-openrouter-failures.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    partition_report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "sources": [
            {"source_key": "europe_pmc", "full_text_qa": {"passes_full_text_bar": True}},
        ],
        "errors": [],
    }

    def failing_review_model(model_name, review_payload):
        assert review_payload["deterministic_guardrail_result"]["schedule_readiness"] == "ready_to_enable"
        raise RuntimeError(f"{model_name} unavailable")

    monkeypatch.setattr(full_text_ops, "_openrouter_review_model", failing_review_model)

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            full_text_report=partition_report,
            review_mode="openrouter_compare",
            review_models=["openai/gpt-5.1", "anthropic/claude-sonnet-4.6"],
        )
    )

    assert result.schedule_readiness == "keep_stopped"
    assert result.should_block_schedule is True
    assert result.evidence["openrouter_all_models_failed"] is True
    assert result.evidence["selected_model"] is None
    assert [review["status"] for review in result.evidence["model_reviews"]] == ["failed", "failed"]
    assert any("openai/gpt-5.1 unavailable" in error for error in result.errors)
    assert any(action.action == "needs_human_review" for action in result.actions)


def test_full_text_ops_maps_triage_actions_to_recommendations(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-triage.sqlite3", seed=False)
    report = {
        "source_keys": ["europe_pmc", "pmc_oa"],
        "sources": [
            {
                "source_key": "europe_pmc",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 1,
                "full_text_qa": {
                    "passes_full_text_bar": False,
                    "triage": {"action": "needs_parser_fix", "severity": "blocking"},
                },
            },
            {
                "source_key": "pmc_oa",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 1,
                "full_text_qa": {
                    "passes_full_text_bar": False,
                    "triage": {"action": "needs_license_review", "severity": "blocking"},
                },
            },
        ],
    }

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc", "pmc_oa"],
            source_health_report=report,
            review_mode="deterministic_only",
        )
    )

    assert result.schedule_readiness == "blocked"
    assert result.should_block_schedule is True
    assert {action.action for action in result.actions} >= {"inspect_parser", "inspect_license"}


def test_full_text_ops_service_is_recommend_only(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-recommend-only.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    claims_before = len(repo.list_claims(source_key="europe_pmc", include_seed_claims=True))
    source_queries_before = repo.list_source_queries(source_key="europe_pmc")

    result = HSAResearchService(repo).run_full_text_ops(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            review_mode="deterministic_only",
        )
    )

    assert result.agent_run_id is not None
    assert len(repo.list_claims(source_key="europe_pmc", include_seed_claims=True)) == claims_before
    assert repo.list_source_queries(source_key="europe_pmc") == source_queries_before
    assert repo.list_agent_runs(agent_name="full_text_ops_agent", status="completed")


def test_local_pipeline_initializes_sources_and_queries(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)

    output = pipeline.initialize()
    coverage = pipeline.coverage()

    assert output["sources"] >= 4
    assert coverage["sources"] >= 4
    assert coverage["source_queries"] >= 4
    assert any(query.query_name == "licensed_full_text_hsa" for query in repo.list_source_queries("pmc_oa"))
    unpaywall_queries = repo.list_source_queries("unpaywall", active_only=False)
    assert any(query.query_name == "oa_discovery_hsa_titles" and query.active for query in unpaywall_queries)
    assert any(
        query.query_name == "human_vascular_sarcoma_trials"
        for query in repo.list_source_queries("clinicaltrials_gov")
    )
    assert any(query.query_name == "canine_hsa_trials" for query in repo.list_source_queries("avma_vctr"))
    assert any(query.query_name == "canine_hsa_cases" for query in repo.list_source_queries("icdc"))
    assert any(query.query_name == "canine_hsa_expression" for query in repo.list_source_queries("geo"))
    assert any(query.query_name == "canine_hsa_sequence_runs" for query in repo.list_source_queries("sra"))
    assert any(query.query_name == "priority_compounds" for query in repo.list_source_queries("pubchem"))
    assert any(query.query_name == "priority_compound_bioactivities" for query in repo.list_source_queries("chembl"))
    chembl_query = next(query for query in repo.list_source_queries("chembl") if query.query_name == "priority_compound_bioactivities")
    assert "CHEMBL279" in chembl_query.query_params["target_chembl_ids"]
    assert chembl_query.query_params["target_organisms"] == ["Homo sapiens", "Canis lupus familiaris"]
    assert chembl_query.query_params["include_cell_line_assays"] is True
    assert "sarcoma" in chembl_query.query_params["cell_line_terms"]
    pubchem_query = next(query for query in repo.list_source_queries("pubchem") if query.query_name == "priority_compounds")
    assert pubchem_query.query_params["require_exact_match"] is True
    assert any(query.query_name == "canine_human_priority_targets" for query in repo.list_source_queries("uniprot"))
    assert any(query.query_name == "priority_target_structures" for query in repo.list_source_queries("rcsb_pdb"))
    assert any(query.query_name == "priority_drug_safety" for query in repo.list_source_queries("openfda_animal_events"))


def test_structured_source_qa_reports_source_scoped_counts(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    raw_record = RawSourceRecord(
        source_key="pubchem",
        source_record_id="CID:4946",
        content_hash="pubchem-4946",
        source_url="https://pubchem.ncbi.nlm.nih.gov/compound/4946",
        raw_payload={"cid": 4946},
    )
    raw_record_id = repo.upsert_raw_record(raw_record)
    research_object = ResearchObject(
        object_type="compound_record",
        title="Propranolol",
        canonical_url="https://pubchem.ncbi.nlm.nih.gov/compound/4946",
        source_key="pubchem",
        raw_record_id=raw_record_id,
        dedupe_key="pubchem:4946",
        identifiers={"cid": "4946"},
    )
    object_id = repo.upsert_research_object(research_object, raw_record_id)
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="pubchem_identity",
            text_content="Propranolol has PubChem CID 4946.",
            content_hash="chunk-pubchem-4946",
        )
    )
    repo.upsert_claim(
        ClaimSearchResult(
            claim_id=uuid4(),
            statement="Propranolol has PubChem identity CID 4946.",
            claim_type=ClaimType.OTHER,
            direction=ClaimDirection.NEUTRAL,
            confidence=0.82,
            evidence_level=EvidenceLevel.IN_SILICO,
            source_object_id=object_id,
            source_title="Propranolol",
            source_url="https://pubchem.ncbi.nlm.nih.gov/compound/4946",
            support_count=1,
            metadata={"curation_status": "promote"},
        )
    )

    qa = structured_source_qa(repo, "pubchem")

    assert qa["raw_records"] == 1
    assert qa["research_objects"] == 1
    assert qa["document_chunks"] == 1
    assert qa["claims"] == 1
    assert qa["claim_status"] == {"promote": 1}
    assert qa["claim_types"] == {"other": 1}
    assert qa["passes_minimum_bar"] is True
    assert qa["sample_claims"][0]["curation_status"] == "promote"

    report = build_structured_source_count_report(repo, source_keys=["pubchem", "chembl"], sample_limit=1)

    assert report["source_keys"] == ["pubchem", "chembl"]
    assert report["totals"] == {
        "raw_records": 1,
        "research_objects": 1,
        "document_chunks": 1,
        "entity_mentions": 0,
        "claims": 1,
    }
    assert report["failed_sources"] == ["chembl"]
    assert report["passes_minimum_bar"] is False
    assert report["minimum_bar"] == {"require_claims": True}
    assert report["sources"][0]["sample_claims"][0]["statement"] == "Propranolol has PubChem identity CID 4946."

    source_health_report = build_structured_source_count_report(
        repo,
        source_keys=["pubchem"],
        sample_limit=1,
        require_claims=False,
    )

    assert source_health_report["failed_sources"] == []
    assert source_health_report["minimum_bar"] == {"require_claims": False}


def test_source_health_report_separates_failed_and_watch_sources(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_minimal_source_claim(
        repo,
        "pubchem",
        curation_status="needs_review",
        extraction_status="source_context",
    )

    report = build_source_health_report(repo, source_keys=["pubchem", "chembl"], sample_limit=1)
    pubchem = next(source for source in report["sources"] if source["source_key"] == "pubchem")
    chembl = next(source for source in report["sources"] if source["source_key"] == "chembl")

    assert report["source_keys"] == ["pubchem", "chembl"]
    assert report["summary"] == {
        "sources": 2,
        "healthy": 0,
        "triage": 0,
        "watch": 1,
        "failing": 1,
        "embedding_missing": 1,
        "source_followup_failed": 0,
        "source_followup_pending": 0,
        "sources_without_active_queries": 2,
    }
    assert report["failed_sources"] == ["chembl"]
    assert report["watch_sources"] == ["pubchem"]
    assert report["triage_sources"] == []
    assert pubchem["health_status"] == "watch"
    assert pubchem["source_role"] == "evidence"
    assert pubchem["health_score"] >= report["minimum_bar"]["min_health_score"]
    assert pubchem["passes_minimum_bar"] is True
    assert pubchem["claim_metadata"]["extraction_status"] == {"source_context": 1}
    assert pubchem["embedding_health"]["missing_chunks"] == 1
    assert pubchem["source_followup_health"]["failed"] == 0
    assert pubchem["source_query_health"]["active_source_queries"] == 0
    assert any("source-context" in risk for risk in pubchem["risks"])
    assert chembl["health_status"] == "failing"
    assert chembl["passes_minimum_bar"] is False


def test_source_health_report_includes_operational_readiness(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_minimal_source_claim(repo, "pubmed")
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="pubmed_hsa_active",
            query_text="canine hemangiosarcoma",
            active=True,
        )
    )
    repo.upsert_source_followup(
        SourceFollowupQueueItem(
            source_key="pubmed",
            identifier_type="doi",
            identifier="10.1000/hsa.1",
            status="failed",
            attempts=1,
            last_error="HTTP 429",
        )
    )

    report = build_source_health_report(repo, source_keys=["pubmed"], sample_limit=1)
    pubmed = report["sources"][0]

    assert report["embedding_missing_sources"] == ["pubmed"]
    assert report["source_followup_failed_sources"] == ["pubmed"]
    assert report["source_followup_pending_sources"] == []
    assert report["sources_without_active_queries"] == []
    assert pubmed["health_status"] == "watch"
    assert pubmed["embedding_health"]["available"] is True
    assert pubmed["embedding_health"]["total_chunks"] == 1
    assert pubmed["embedding_health"]["missing_chunks"] == 1
    assert pubmed["missing_embeddings"] == 1
    assert pubmed["source_followup_health"]["failed"] == 1
    assert pubmed["source_followup_failed"] == 1
    assert pubmed["source_followup_health"]["recent_failed"][0]["last_error"] == "HTTP 429"
    assert pubmed["source_query_health"]["active_source_queries"] == 1
    assert pubmed["active_source_queries"] == 1
    assert any("embedding_index_job" in action for action in pubmed["recommended_actions"])
    assert any("pubmed_source_followup_ingest_job" in action for action in pubmed["recommended_actions"])


def test_source_health_report_marks_complete_embeddings(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_minimal_source_claim(repo, "pubmed")
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="pubmed_hsa_active",
            query_text="canine hemangiosarcoma",
            active=True,
        )
    )
    chunk = repo.list_document_chunks(source_key="pubmed")[0]
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunk.id,
            research_object_id=chunk.research_object_id,
            chunk_index=chunk.chunk_index,
            source_key="pubmed",
            object_type="publication",
            content_hash=chunk.content_hash,
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[1.0, 0.0, 0.0],
        )
    )

    report = build_source_health_report(repo, source_keys=["pubmed"], sample_limit=1)
    pubmed = report["sources"][0]

    assert report["embedding_missing_sources"] == []
    assert pubmed["health_status"] == "healthy"
    assert pubmed["embedding_health"]["coverage_ratio"] == 1.0
    assert pubmed["missing_embeddings"] == 0
    assert "embedding_coverage_complete" in pubmed["signals"]
    assert "active_source_queries_present" in pubmed["signals"]


def test_source_health_report_marks_expected_triage_sources(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_minimal_source_claim(
        repo,
        "sra",
        curation_status="needs_review",
        extraction_status="source_context",
    )

    report = build_source_health_report(repo, source_keys=["sra"], sample_limit=1)
    sra = report["sources"][0]

    assert report["summary"] == {
        "sources": 1,
        "healthy": 0,
        "triage": 1,
        "watch": 0,
        "failing": 0,
        "embedding_missing": 1,
        "source_followup_failed": 0,
        "source_followup_pending": 0,
        "sources_without_active_queries": 1,
    }
    assert report["failed_sources"] == []
    assert report["triage_sources"] == ["sra"]
    assert report["watch_sources"] == []
    assert sra["source_role"] == "triage"
    assert sra["health_status"] == "triage"
    assert sra["passes_minimum_bar"] is True
    assert "triage_only_source" in sra["signals"]
    assert any("specialized triage agent" in action for action in sra["recommended_actions"])


def test_full_text_source_health_requires_body_chunks(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_minimal_source_claim(repo, "europe_pmc")

    report = build_source_health_report(repo, source_keys=["europe_pmc"], sample_limit=1)
    europe_pmc = report["sources"][0]

    assert report["failed_sources"] == ["europe_pmc"]
    assert report["passes_minimum_bar"] is False
    assert europe_pmc["full_text_qa"]["passes_full_text_bar"] is False
    assert europe_pmc["full_text_qa"]["triage"]["action"] == "needs_parser_fix"
    assert europe_pmc["full_text_triage_action"] == "needs_parser_fix"
    assert europe_pmc["minimum_bar"]["full_text_required_passes"] is False
    assert report["full_text_blocking_sources"] == ["europe_pmc"]
    assert any("Full-text source lacks" in risk for risk in europe_pmc["risks"])
    assert any("Full-text triage action: needs_parser_fix" in risk for risk in europe_pmc["risks"])


def test_full_text_source_count_report_passes_with_body_chunks(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")

    report = build_structured_source_count_report(repo, source_keys=["europe_pmc"], sample_limit=1)
    qa = full_text_source_qa(repo, "europe_pmc")

    assert report["failed_sources"] == []
    assert report["passes_minimum_bar"] is True
    assert report["sources"][0]["full_text_qa"]["passes_full_text_bar"] is True
    assert report["sources"][0]["full_text_triage_action"] == "no_action"
    assert qa["full_text_research_objects"] == 1
    assert qa["full_text_document_chunks"] == 1
    assert qa["triage"]["action"] == "no_action"
    current_run_qa = full_text_source_qa(repo, "europe_pmc", ingestion_results=[])
    assert current_run_qa["passes_full_text_bar"] is False
    assert current_run_qa["triage"]["action"] == "retry_later"


def test_full_text_qa_uses_body_chunks_as_persisted_gate(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    raw_record = RawSourceRecord(
        source_key="pmc_oa",
        source_record_id="pmc_oa:metadata-gap",
        content_hash="pmc-oa-metadata-gap-raw",
        source_url="https://example.org/pmc_oa/metadata-gap",
        raw_payload={"source_key": "pmc_oa", "full_text": "Full text body is persisted."},
    )
    raw_record_id = repo.upsert_raw_record(raw_record)
    research_object = ResearchObject(
        object_type="publication",
        title="PMC OA full text with missing object flag",
        canonical_url="https://example.org/pmc_oa/metadata-gap",
        source_key="pmc_oa",
        raw_record_id=raw_record_id,
        dedupe_key="pmc_oa:metadata-gap",
        metadata={},
    )
    object_id = repo.upsert_research_object(research_object, raw_record_id)
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="full_text:results",
            text_content="Full text body mentions canine hemangiosarcoma.",
            content_hash="pmc-oa-metadata-gap-chunk",
        )
    )

    qa = full_text_source_qa(repo, "pmc_oa")
    current_qa = full_text_source_qa(
        repo,
        "pmc_oa",
        ingestion_results=[
            {
                "source_key": "pmc_oa",
                "query_name": "metadata_gap_current_run",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 1,
                "full_text_research_objects": 0,
                "section_chunk_counts": {"full_text:results": 1},
                "status": "completed",
                "errors": [],
            }
        ],
    )

    assert qa["full_text_research_objects"] == 0
    assert qa["full_text_document_chunks"] == 1
    assert qa["passes_persisted_full_text_bar"] is True
    assert qa["passes_full_text_bar"] is True
    assert current_qa["passes_current_full_text_bar"] is True
    assert current_qa["triage"]["action"] == "no_action"


def test_service_triages_full_text_issue(tmp_path):
    service = make_service(tmp_path)

    result = service.triage_full_text_issue(
        FullTextTriageRequest(
            source_key="pmc_oa",
            stage="fetch",
            error_message="429 too many requests",
            http_status=429,
        )
    )

    assert result.action == "retry_later"
    assert result.should_retry is True
    assert result.should_block_schedule is False


def test_entity_resolution_persists_entities_aliases_and_mentions_idempotently(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:1",
            content_hash="entity-resolution-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/1/",
            raw_payload={"pmid": "1"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Entity resolution example",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            dedupe_key="pmid:1",
            identifiers={"pmid": "1"},
        ),
        raw_record_id,
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Adriamycin is discussed with VEGF receptor 2 and KIT, but not kitchen, in human angiosarcoma.",
            content_hash="entity-resolution-chunk",
        )
    )

    result = resolve_entities_for_repository(repo, source_key="pubmed")
    second_result = resolve_entities_for_repository(repo, source_key="pubmed")
    entities = repo.list_entities()
    mentions = repo.list_entity_mentions(source_key="pubmed")

    assert result.errors == []
    assert result.chunks_seen == 1
    assert result.mentions_upserted >= 4
    assert second_result.errors == []
    assert {entity.canonical_name for entity in entities} >= {"doxorubicin", "KDR", "KIT"}
    assert len(mentions) == len({mention.mention_id for mention in mentions})
    assert len(mentions) == len(repo.list_entity_mentions(chunk_id=chunk.id))
    assert sum(1 for mention in mentions if mention.canonical_name == "KIT") == 1
    assert normalize_entity_key("compound", "propranolol", {"pubchem_cid": "4946"}) == "pubchem_cid:4946"
    coverage = repo.coverage_summary()
    assert coverage["entity_aliases"] >= 1
    assert coverage["entity_mentions"] == len(mentions)
    assert repo.source_runtime_summary("pubmed")["entity_mentions"] == len(mentions)


def test_research_hunt_broad_task_passes_action_query_and_source_override(monkeypatch):
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    task_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Find toceranib/VEGFR inhibitor monotherapy outcomes in canine splenic HSA",
            status="watching",
            suggested_sources=["pubmed", "europe_pmc", "openalex", "clinicaltrials_gov", "crossref"],
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "hunting",
                    "open_task_count": 1,
                    "best_signal": {"score": 100, "evidence_refs": [], "evidence_fit": {"fit": "strong"}},
                    "tasks": [
                        {
                            "task_id": str(task_id),
                            "identity_key": "broaden_query:clinicaltrials",
                            "status": "open",
                            "task_type": "broaden_query",
                            "action": (
                                "run_a_targeted_clinicaltrials.gov_search_for_"
                                "'toceranib_hemangiosarcoma'_and_'angiosarcoma_vegfr_inhibitor'"
                            ),
                            "source_keys": ["pubmed", "europe_pmc", "openalex", "clinicaltrials_gov", "crossref"],
                            "priority": 35,
                        }
                    ],
                }
            },
        )
    )
    captured: dict[str, ResearchFollowupLoopRequest] = {}

    def fake_loop(self, request):
        captured["request"] = request
        return ResearchFollowupLoopResult(lead_id=request.lead_id, dry_run=False)

    monkeypatch.setattr(HSAResearchService, "run_research_followup_loop", fake_loop)

    result = service.run_research_hunt_tasks(
        ResearchHuntTaskRunRequest(lead_ids=[lead.lead_id], task_ids=[task_id], dry_run=False, evaluate=False)
    )

    assert result.completed_count == 1
    assert captured["request"].source_keys == ["clinicaltrials_gov"]
    assert captured["request"].search_query_text is not None
    assert "toceranib" in captured["request"].search_query_text
    assert "hemangiosarcoma" in captured["request"].search_query_text
    assert "vegfr" in captured["request"].search_query_text
    assert "inhibitor" in captured["request"].search_query_text


def test_unpaywall_v2_normalizer_preserves_oa_location_metadata():
    record = UnpaywallHarvesterV2().normalize(
        {
            "score": 0.42,
            "snippet": "<b>Angiosarcoma</b> open access title match",
            "response": {
                "doi": "https://doi.org/10.1234/HSA.OA",
                "doi_url": "https://doi.org/10.1234/HSA.OA",
                "title": "Human angiosarcoma open access review",
                "year": 2026,
                "published_date": "2026-04-01",
                "publisher": "Example Publisher",
                "journal_name": "Example Journal",
                "genre": "journal-article",
                "is_oa": True,
                "oa_status": "gold",
                "journal_is_in_doaj": True,
                "best_oa_location": {
                    "url_for_landing_page": "https://example.org/article",
                    "url_for_pdf": "https://example.org/article.pdf",
                    "license": "cc-by",
                    "host_type": "publisher",
                    "version": "publishedVersion",
                },
                "oa_locations": [
                    {
                        "url_for_landing_page": "https://example.org/article",
                        "license": "cc-by",
                        "host_type": "publisher",
                    }
                ],
                "z_authors": [{"given": "Ada", "family": "Lovelace"}],
            },
        }
    )

    assert record.raw_record.source_key == "unpaywall"
    assert record.research_object.identifiers["doi"] == "10.1234/hsa.oa"
    assert record.research_object.canonical_url == "https://example.org/article"
    assert record.research_object.metadata["best_oa_location"]["url_for_pdf"] == "https://example.org/article.pdf"
    assert record.research_object.metadata["authors"] == ["Ada Lovelace"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["human_angiosarcoma"]
    assert UnpaywallHarvesterV2().chunk_section_label(record) == "oa_discovery_metadata"


def test_unpaywall_v2_fetch_uses_title_search_endpoint(monkeypatch):
    calls = []

    def fake_get_json(url, params, **kwargs):
        calls.append((url, params, kwargs))
        return {
            "results": [
                {
                    "score": 1.0,
                    "response": {
                        "doi": "10.1234/example",
                        "title": "Canine hemangiosarcoma open access paper",
                        "is_oa": True,
                        "best_oa_location": {"url_for_landing_page": "https://example.org/paper"},
                    },
                }
            ]
        }

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    records = UnpaywallHarvesterV2().fetch(
        '"canine hemangiosarcoma"',
        limit=1,
        email="contact@example.org",
        is_oa=True,
    )

    assert len(records) == 1
    assert calls == [
        (
            "https://api.unpaywall.org/v2/search/",
            {"query": '"canine hemangiosarcoma"', "is_oa": "true", "email": "contact@example.org"},
            {"timeout_seconds": harvesters_v2.DEFAULT_REQUEST_TIMEOUT_SECONDS, "attempts": harvesters_v2.DEFAULT_REQUEST_ATTEMPTS},
        )
    ]


def test_unpaywall_v2_fetch_uses_doi_endpoint_for_doi_queries(monkeypatch):
    calls = []

    def fake_get_json(url, params, **kwargs):
        calls.append((url, params, kwargs))
        return {
            "doi": "10.1234/example",
            "title": "Human angiosarcoma open access DOI record",
            "is_oa": True,
            "best_oa_location": {"url_for_landing_page": "https://example.org/doi-record"},
        }

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    records = UnpaywallHarvesterV2().fetch(
        "https://doi.org/10.1234/example",
        limit=1,
        email="contact@example.org",
    )

    assert len(records) == 1
    assert records[0].research_object.identifiers["doi"] == "10.1234/example"
    assert calls == [
        (
            "https://api.unpaywall.org/v2/10.1234%2Fexample",
            {"email": "contact@example.org"},
            {"timeout_seconds": harvesters_v2.DEFAULT_REQUEST_TIMEOUT_SECONDS, "attempts": harvesters_v2.DEFAULT_REQUEST_ATTEMPTS},
        )
    ]


def test_pmc_oa_v2_normalizer_extracts_license_and_full_text():
    xml = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <GetRecord>
        <record>
          <metadata>
            <article xmlns="http://jats.nlm.nih.gov" xmlns:xlink="http://www.w3.org/1999/xlink" article-type="research-article">
              <front>
                <journal-meta>
                  <journal-title-group><journal-title>Example Journal</journal-title></journal-title-group>
                </journal-meta>
                <article-meta>
                  <article-id pub-id-type="pmid">12345</article-id>
                  <article-id pub-id-type="pmc">PMC999999</article-id>
                  <article-id pub-id-type="doi">10.1234/PMC.TEST</article-id>
                  <title-group>
                    <article-title>Canine <italic>hemangiosarcoma</italic> and human angiosarcoma</article-title>
                  </title-group>
                  <pub-date pub-type="epub"><year>2026</year><month>04</month><day>01</day></pub-date>
                  <permissions>
                    <license license-type="open-access" xlink:href="https://creativecommons.org/licenses/by/4.0/">
                      <license-p>Creative Commons Attribution License</license-p>
                    </license>
                  </permissions>
                  <abstract><p>Human angiosarcoma analog evidence.</p></abstract>
                </article-meta>
              </front>
              <body>
                <sec>
                  <title>Results</title>
                  <p>Canine hemangiosarcoma full text mentions VEGF and propranolol.</p>
                </sec>
              </body>
            </article>
          </metadata>
        </record>
      </GetRecord>
    </OAI-PMH>
    """

    record = PMCOAHarvesterV2().normalize(
        xml,
        oa_metadata={"oa_license": "CC BY", "links": [{"format": "tgz", "href": "ftp://example.test/a.tgz"}]},
        source_query="hemangiosarcoma",
    )

    assert record.raw_record.source_key == "pmc_oa"
    assert record.raw_record.raw_payload["full_text"] == "Results Canine hemangiosarcoma full text mentions VEGF and propranolol."
    assert record.raw_record.raw_payload["full_text_sections"] == [
        {
            "section_label": "full_text:results",
            "title": "Results",
            "text": "Results Canine hemangiosarcoma full text mentions VEGF and propranolol.",
        }
    ]
    assert record.research_object.identifiers["pmcid"] == "PMC999999"
    assert record.research_object.identifiers["doi"] == "10.1234/pmc.test"
    assert record.research_object.metadata["journal"] == "Example Journal"
    assert record.research_object.metadata["license"]["oa_license"] == "CC BY"
    assert record.research_object.metadata["license"]["jats_license_url"] == "https://creativecommons.org/licenses/by/4.0/"
    assert record.research_object.metadata["full_text_available"] is True
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == [
        "canine_hsa",
        "human_angiosarcoma",
    ]


def test_pmc_oa_v2_chunks_full_text_not_just_abstract():
    record = PMCOAHarvesterV2().normalize(
        """
        <article xmlns="http://jats.nlm.nih.gov">
          <front>
            <article-meta>
              <article-id pub-id-type="pmc">PMC1</article-id>
              <title-group><article-title>Canine hemangiosarcoma</article-title></title-group>
              <abstract><p>Short abstract.</p></abstract>
            </article-meta>
          </front>
          <body><p>Full text body with human angiosarcoma comparative evidence.</p></body>
        </article>
        """,
        oa_metadata={"oa_license": "CC BY"},
    )
    harvester = PMCOAHarvesterV2()

    assert harvester.chunk_section_label(record) == "full_text"
    assert "Full text body" in harvester.text_for_chunking(record)
    sections = harvester.chunk_text_sections(record)
    assert [section_label for section_label, _text in sections] == ["title_abstract", "full_text"]
    assert "Short abstract" in sections[0][1]
    assert "Short abstract" not in sections[1][1]
    assert sections[1][1] == "Full text body with human angiosarcoma comparative evidence."


def test_pmc_oa_v2_does_not_label_abstract_only_records_as_full_text():
    record = PMCOAHarvesterV2().normalize(
        """
        <article xmlns="http://jats.nlm.nih.gov">
          <front>
            <article-meta>
              <article-id pub-id-type="pmc">PMC2</article-id>
              <title-group><article-title>Canine hemangiosarcoma</article-title></title-group>
              <abstract><p>Short abstract.</p></abstract>
            </article-meta>
          </front>
        </article>
        """,
        oa_metadata={"oa_license": "CC BY"},
    )
    harvester = PMCOAHarvesterV2()

    assert record.research_object.metadata["full_text_available"] is False
    assert harvester.chunk_section_label(record) == "title_abstract"
    assert harvester.chunk_text_sections(record) == [("title_abstract", "Canine hemangiosarcoma\n\nShort abstract.")]


def test_local_ingestion_preserves_full_text_chunks_when_duplicate_metadata_arrives(tmp_path, monkeypatch):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    pipeline = LocalIngestionPipeline(repo)
    query = SourceQuery(
        source_key="europe_pmc",
        query_name="licensed_full_text_test",
        query_text="hemangiosarcoma",
    )
    full_text_record = EuropePMCHarvesterV2().normalize(
        {
            "id": "PMC123",
            "pmcid": "PMC123",
            "title": "Endothelial biology review",
            "abstractText": "Sparse abstract.",
            "isOpenAccess": "Y",
        },
        full_text_xml="""
        <article xmlns="http://jats.nlm.nih.gov">
          <body><p>Canine hemangiosarcoma full text body mentions VEGF and propranolol.</p></body>
        </article>
        """,
    )
    abstract_only_record = EuropePMCHarvesterV2().normalize(
        {
            "id": "PMC123",
            "pmcid": "PMC123",
            "title": "Endothelial biology review",
            "abstractText": "Sparse abstract.",
            "isOpenAccess": "Y",
        }
    )

    class FakeEuropePMCHarvester(EuropePMCHarvesterV2):
        records = [full_text_record]

        def fetch(self, query_text, limit=25, **params):
            return self.records

    monkeypatch.setitem(harvesters_v2.HARVESTERS_V2, "europe_pmc", FakeEuropePMCHarvester)

    result = pipeline.ingest_query(query, limit=1)
    chunks = repo.list_document_chunks(source_key="europe_pmc")

    assert result.document_chunks == 2
    assert result.full_text_research_objects == 1
    assert result.section_chunk_counts == {"title_abstract": 1, "full_text": 1}
    assert [chunk.section_label for chunk in chunks] == ["title_abstract", "full_text"]
    assert "Sparse abstract" not in chunks[1].text_content

    FakeEuropePMCHarvester.records = [abstract_only_record]
    refreshed = pipeline.ingest_query(query, limit=1)
    refreshed_chunks = repo.list_document_chunks(source_key="europe_pmc")
    refreshed_object = repo.get_research_object(full_text_record.research_object.id)

    assert refreshed.document_chunks == 0
    assert refreshed.full_text_research_objects == 0
    assert refreshed.section_chunk_counts == {}
    assert refreshed_object is not None
    assert refreshed_object.metadata["full_text_available"] is True
    assert [chunk.section_label for chunk in refreshed_chunks] == ["title_abstract", "full_text"]


def test_pmc_oa_v2_is_registered_harvester():
    assert HARVESTERS_V2["pmc_oa"] is PMCOAHarvesterV2


def test_unpaywall_is_registered_for_manual_oa_discovery():
    assert HARVESTERS_V2["unpaywall"] is UnpaywallHarvesterV2
    assert "unpaywall" not in HOSTED_API_REPORT_KEYS


def test_literature_corpus_harvest_targets_hundreds_of_papers():
    assert LITERATURE_CORPUS_SOURCE_KEYS == (
        "openalex",
        "pubmed",
        "crossref",
    )
    assert sum(LITERATURE_CORPUS_SOURCE_LIMITS.values()) >= 300
    assert set(LITERATURE_CORPUS_SOURCE_KEYS).isdisjoint(LITERATURE_FULL_TEXT_SOURCE_KEYS)


def test_full_text_refresh_keeps_heavy_sources_bounded():
    assert LITERATURE_FULL_TEXT_SOURCE_KEYS == ("europe_pmc", "pmc_oa")
    assert LITERATURE_FULL_TEXT_SOURCE_LIMITS["europe_pmc"] <= 10
    assert LITERATURE_FULL_TEXT_SOURCE_LIMITS["pmc_oa"] <= 3


def test_all_api_smoke_covers_every_hosted_report_source():
    assert ALL_API_SMOKE_KEYS == HOSTED_API_REPORT_KEYS
    assert set(ALL_API_SMOKE_KEYS) == {
        "pubchem",
        "chembl",
        "uniprot",
        "rcsb_pdb",
        "openfda_animal_events",
        "icdc",
        "geo",
        "sra",
        "openalex",
        "pubmed",
        "europe_pmc",
        "crossref",
        "pmc_oa",
        "clinicaltrials_gov",
    }


def test_x_topic_monitor_builds_official_api_request_only():
    request = x_topic_monitor.build_recent_search_request(
        x_topic_monitor.XTopicRequest(
            query='"canine hemangiosarcoma"',
            query_name="x_disease_monitoring",
            max_results=10,
        )
    )

    assert request.method == "GET"
    assert request.url.startswith("https://api.x.com/2/tweets/search/recent?")
    assert request.params["query"] == '"canine hemangiosarcoma" lang:en -is:retweet'
    assert request.headers["Authorization"] == "Bearer <X_BEARER_TOKEN>"
    assert request.billable is True
    assert any("Official X API" in note for note in request.notes)


def test_x_topic_monitor_builds_twitterapi_io_request():
    request = x_topic_monitor.build_twitterapi_io_search_request(
        x_topic_monitor.XTopicRequest(
            query='"canine hemangiosarcoma"',
            query_name="x_disease_monitoring",
            max_results=10,
        )
    )

    assert request.method == "GET"
    assert request.url.startswith("https://api.twitterapi.io/twitter/tweet/advanced_search?")
    assert request.params["query"] == '"canine hemangiosarcoma" lang:en -filter:retweets'
    assert request.params["queryType"] == "Latest"
    assert request.headers["x-api-key"] == "<TWITTERAPI_IO_KEY>"
    assert request.billable is True


def test_x_topic_monitor_normalizes_post_for_manual_review():
    candidate = x_topic_monitor.normalize_post_payload(
        {
            "id": "123",
            "author_id": "456",
            "author": {"username": "vetonc"},
            "conversation_id": "789",
            "created_at": "2026-04-28T10:00:00Z",
            "lang": "en",
            "text": "New canine hemangiosarcoma trial links to PubMed.",
            "entities": {
                "urls": [
                    {"expanded_url": "https://pubmed.ncbi.nlm.nih.gov/123456/"},
                ]
            },
        },
        query_name="x_trial_monitoring",
    )

    assert candidate.source_record_id == "123"
    assert candidate.canonical_url == "https://x.com/vetonc/status/123"
    assert candidate.review_status == x_topic_monitor.XReviewStatus.NEEDS_REVIEW
    assert "canine hemangiosarcoma" in candidate.matched_terms
    assert candidate.durable_links == ["https://pubmed.ncbi.nlm.nih.gov/123456/"]
    assert candidate.quality_score > 0.5


def test_x_topic_monitor_extracts_durable_links_from_text_fallbacks():
    candidate = x_topic_monitor.normalize_post_payload(
        {
            "id": "123",
            "author": {"username": "vetonc"},
            "lang": "en",
            "text": (
                "Canine hemangiosarcoma update with DOI 10.1158/0008-5472.CAN-26-0002, "
                "PMID: 87654321, PMCID: PMC6686562, NCT12345678, and "
                "https://www.nature.com/articles/s41586-026-00001."
            ),
        },
        query_name="x_disease_monitoring",
    )

    assert candidate.durable_links == [
        "https://clinicaltrials.gov/study/NCT12345678",
        "https://doi.org/10.1158/0008-5472.CAN-26-0002",
        "https://pmc.ncbi.nlm.nih.gov/articles/PMC6686562/",
        "https://pubmed.ncbi.nlm.nih.gov/87654321/",
        "https://www.nature.com/articles/s41586-026-00001",
    ]
    assert "contains durable source link" in candidate.review_reasons


def test_x_topic_review_contracts_validate():
    linked_source = XTopicLinkedSource(
        url="https://pubmed.ncbi.nlm.nih.gov/123456/",
        recommended_source_key="pubmed",
        identifier_type="pmid",
        identifier="123456",
        should_ingest=True,
        reason="PubMed record.",
    )
    action = XTopicReviewAction(
        source_record_id="123",
        query_name="x_trial_monitoring",
        action="flag_for_ingestion",
        severity="watch",
        reason="Linked PubMed record should be harvested.",
        ingestible_links=[linked_source],
    )
    result = XTopicReviewResult(actions=[action])

    assert XTopicReviewRequest().review_mode == "openrouter_required"
    assert result.actions[0].ingestible_links[0].identifier_type == "pmid"
    with pytest.raises(ValueError):
        XTopicReviewAction(source_record_id="123", action="bad", severity="watch", reason="bad")
    with pytest.raises(ValueError):
        XTopicLinkedSource(url="https://example.com", identifier_type="bad", reason="bad")


def test_x_topic_review_resolves_short_links_before_classification(monkeypatch):
    monkeypatch.setattr(
        x_topic_review,
        "_follow_redirects",
        lambda url: "https://pubmed.ncbi.nlm.nih.gov/123456/",
    )

    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "quality_score": 0.7,
                    "durable_links": ["https://go.ufl.edu/r2uqpua"],
                }
            ],
        )
    )

    link = result.actions[0].ingestible_links[0]
    assert result.ingestion_candidate_count == 1
    assert result.actions[0].action == "flag_for_ingestion"
    assert link.url == "https://pubmed.ncbi.nlm.nih.gov/123456/"
    assert link.recommended_source_key == "pubmed"
    assert link.identifier == "123456"
    assert link.metadata["original_url"] == "https://go.ufl.edu/r2uqpua"
    assert link.metadata["resolved"] is True


def test_x_topic_review_strips_tracking_params_from_publisher_dois():
    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "quality_score": 0.7,
                    "durable_links": [
                        "https://www.frontiersin.org/journals/veterinary-science/articles/10.3389/fvets.2026.1778366/full?utm_source=twitter#metrics"
                    ],
                }
            ],
        )
    )

    link = result.actions[0].ingestible_links[0]
    assert link.recommended_source_key == "crossref"
    assert link.identifier == "10.3389/fvets.2026.1778366"


def test_x_topic_review_sends_high_volume_link_lists_to_human_review():
    links = [f"https://pubmed.ncbi.nlm.nih.gov/{100000 + index}/" for index in range(21)]

    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "quality_score": 0.9,
                    "durable_links": links,
                }
            ],
        )
    )

    action = result.actions[0]
    assert action.action == "needs_human_review"
    assert action.ingestible_links == []
    assert action.metadata["actionable_link_count"] == 21
    assert result.ingestion_candidate_count == 0
    assert result.needs_human_review_count == 1


def test_x_topic_review_openrouter_preserves_deterministic_ingestion_guardrail(monkeypatch):
    def fake_review_model(model_name, review_payload):
        assert model_name == "anthropic/claude-sonnet-test"
        assert review_payload["candidates"][0]["durable_links"] == [
            "https://doi.org/10.7717/peerj.4375"
        ]
        return {
            "text": json.dumps(
                {
                    "actions": [
                        {
                            "source_record_id": "123",
                            "query_name": "x_disease_monitoring",
                            "username": "vetonc",
                            "action": "needs_human_review",
                            "severity": "critical",
                            "reason": "Model wants a human pass.",
                            "links": ["https://doi.org/10.7717/peerj.4375"],
                            "evidence_refs": ["candidate:123"],
                            "metadata": {"reviewer": "model"},
                            "extra_field": "ignored",
                        }
                    ],
                    "evidence": {"review_summary": "reviewed"},
                    "errors": [],
                }
            ),
            "metadata": {"model_name": "anthropic/claude-sonnet-test", "usage": {"cost": 0.01}},
        }

    monkeypatch.setattr(x_topic_review, "_openrouter_review_model", fake_review_model)

    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="openrouter_required",
            review_models=["anthropic/claude-sonnet-test"],
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "username": "vetonc",
                    "quality_score": 0.7,
                    "durable_links": ["https://doi.org/10.7717/peerj.4375"],
                }
            ],
        )
    )

    assert result.ingestion_candidate_count == 1
    assert [action.action for action in result.actions] == ["needs_human_review", "flag_for_ingestion"]
    assert result.actions[0].severity == "watch"
    assert result.actions[1].ingestible_links[0].recommended_source_key == "crossref"
    assert result.evidence["model_reviews"][0]["status"] == "completed"


def test_clinicaltrials_gov_v2_is_registered_harvester():
    assert HARVESTERS_V2["clinicaltrials_gov"] is ClinicalTrialsGovHarvesterV2


def test_avma_vctr_v2_is_registered_harvester():
    assert HARVESTERS_V2["avma_vctr"] is AVMAVCTRHarvesterV2


def test_icdc_v2_is_registered_harvester():
    assert HARVESTERS_V2["icdc"] is ICDCHarvesterV2


def test_geo_and_sra_v2_are_registered_harvesters():
    assert HARVESTERS_V2["geo"] is GEOHarvesterV2
    assert HARVESTERS_V2["sra"] is SRAHarvesterV2


def test_research_primitive_sources_are_registered_harvesters():
    assert HARVESTERS_V2["hgnc"] is HGNCHarvesterV2
    assert HARVESTERS_V2["vgnc"] is VGNCHarvesterV2
    assert HARVESTERS_V2["ncbi_gene"] is NCBIGeneHarvesterV2
    assert HARVESTERS_V2["ensembl_xrefs"] is EnsemblXrefsHarvesterV2
    assert HARVESTERS_V2["ensembl_compara"] is EnsemblComparaHarvesterV2
    assert HARVESTERS_V2["unichem"] is UniChemHarvesterV2
    assert HARVESTERS_V2["oma"] is OMAHarvesterV2
    assert HARVESTERS_V2["mondo"] is MONDOHarvesterV2
    assert HARVESTERS_V2["doid"] is DOIDHarvesterV2
    assert HARVESTERS_V2["reactome"] is ReactomeHarvesterV2
    assert HARVESTERS_V2["wikipathways"] is WikiPathwaysHarvesterV2


def test_entity_lookup_primitive_resolves_alias_and_records_event(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    entity = repo.upsert_entity(
        ResolvedEntity(
            entity_type="target",
            canonical_name="KDR",
            normalized_key="kdr",
            external_ids={"entrezgene": "3791", "uniprot": "P35968"},
            resolver_name="hgnc-synonyms",
            resolver_version="2026.05",
            metadata={"organism_taxid": "9606"},
        )
    )
    repo.upsert_entity_alias(
        EntityAlias(
            entity_id=entity.entity_id,
            entity_type="target",
            alias="VEGFR2",
            alias_normalized="vegfr2",
            canonical_name="KDR",
            normalized_key="kdr",
            resolver_name="hgnc-synonyms",
            resolver_version="2026.05",
            metadata={"organism_taxid": "9606", "hgnc": "HGNC:6307"},
        )
    )

    response = service_module.HSAResearchService(repo).resolve_entity_lookup(
        EntityLookupRequest(query="VEGFR2", category="target", organism="9606")
    )

    assert response.result is not None
    assert response.result.canonical_id == "entrezgene:3791"
    assert response.result.source_version == "2026.05"
    assert response.event_id is not None
    assert repo.list_primitive_call_events(primitive_name="entity_lookup")[0].event_id == response.event_id


def test_entity_lookup_index_materializes_hgnc_aliases_from_source_records(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    record = HGNCHarvesterV2().normalize(
        {
            "hgnc_id": "HGNC:6307",
            "symbol": "KDR",
            "name": "kinase insert domain receptor",
            "alias_symbol": ["VEGFR2", "FLK1"],
            "entrez_id": "3791",
            "ensembl_gene_id": "ENSG00000128052",
            "uniprot_ids": ["P35968"],
            "taxonomy_id": "9606",
            "source_version": "2026.05",
        }
    )
    raw_id = repo.upsert_raw_record(record.raw_record)
    repo.upsert_research_object(record.research_object, raw_id)

    result = HSAResearchService(repo).materialize_entity_lookup_index(
        EntityLookupIndexRequest(source_keys=["hgnc"], limit_per_source=10)
    )
    response = HSAResearchService(repo).resolve_entity_lookup(
        EntityLookupRequest(query="VEGFR2", category="target", organism="9606")
    )

    assert result.records_seen == 1
    assert result.entities_upserted == 1
    assert result.aliases_upserted >= 3
    assert result.source_summaries[0].source_version == "2026.05"
    assert repo.list_source_versions(source_key="hgnc")[0].source_version == "2026.05"
    assert response.result is not None
    assert response.result.canonical_id == "entrezgene:3791"
    assert response.result.source_table == "hgnc"
    assert response.result.source_version == "2026.05"


def test_entity_lookup_index_keeps_human_and_canine_symbols_organism_scoped(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    human = HGNCHarvesterV2().normalize(
        {
            "hgnc_id": "HGNC:6307",
            "symbol": "KDR",
            "name": "kinase insert domain receptor",
            "alias_symbol": ["VEGFR2"],
            "entrez_id": "3791",
            "taxonomy_id": "9606",
            "source_version": "2026.05",
        }
    )
    canine = VGNCHarvesterV2().normalize(
        {
            "vgnc_id": "VGNC:53234",
            "symbol": "KDR",
            "name": "kinase insert domain receptor",
            "alias_symbol": ["VEGFR2"],
            "taxonomy_id": "9615",
            "source_version": "2026.05",
        }
    )
    for record in (human, canine):
        raw_id = repo.upsert_raw_record(record.raw_record)
        repo.upsert_research_object(record.research_object, raw_id)

    HSAResearchService(repo).materialize_entity_lookup_index(
        EntityLookupIndexRequest(source_keys=["hgnc", "vgnc"], limit_per_source=10)
    )
    human_response = HSAResearchService(repo).resolve_entity_lookup(
        EntityLookupRequest(query="VEGFR2", category="target", organism="9606")
    )
    canine_response = HSAResearchService(repo).resolve_entity_lookup(
        EntityLookupRequest(query="VEGFR2", category="target", organism="9615")
    )

    assert human_response.result is not None
    assert canine_response.result is not None
    assert human_response.result.organism == "9606"
    assert canine_response.result.organism == "9615"
    assert human_response.result.canonical_id == "entrezgene:3791"
    assert canine_response.result.canonical_id == "VGNC:53234"


def test_entity_lookup_prefers_source_backed_alias_over_local_fallback(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    fallback = repo.upsert_entity(
        ResolvedEntity(
            entity_type="target",
            canonical_name="KDR",
            normalized_key="target:uniprot_accession:p35968",
            external_ids={"uniprot_accession": "P35968"},
            resolver_name="local_deterministic_entity_resolver",
            resolver_version="0.1",
            metadata={"organism_taxid": "9606"},
        )
    )
    repo.upsert_entity_alias(
        EntityAlias(
            entity_id=fallback.entity_id,
            entity_type="target",
            alias="VEGFR2",
            alias_normalized="vegfr2",
            canonical_name="KDR",
            normalized_key=fallback.normalized_key,
            resolver_name="local_deterministic_entity_resolver",
            resolver_version="0.1",
            metadata={"organism_taxid": "9606"},
        )
    )
    record = HGNCHarvesterV2().normalize(
        {
            "hgnc_id": "HGNC:6307",
            "symbol": "KDR",
            "name": "kinase insert domain receptor",
            "alias_symbol": ["VEGFR2"],
            "entrez_id": "3791",
            "taxonomy_id": "9606",
            "source_version": "2026.05",
        }
    )
    raw_id = repo.upsert_raw_record(record.raw_record)
    repo.upsert_research_object(record.research_object, raw_id)
    HSAResearchService(repo).materialize_entity_lookup_index(
        EntityLookupIndexRequest(source_keys=["hgnc"], limit_per_source=10)
    )

    response = HSAResearchService(repo).resolve_entity_lookup(
        EntityLookupRequest(query="VEGFR2", category="target", organism="9606")
    )

    assert response.result is not None
    assert response.failure is None
    assert response.result.canonical_id == "entrezgene:3791"
    assert response.result.source_table == "hgnc"


def test_entity_lookup_index_does_not_create_targets_from_pubchem_compounds(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    record = PubChemHarvesterV2().normalize(
        {
            "query_term": "propranolol",
            "properties": {
                "CID": 4946,
                "Title": "Propranolol",
                "CanonicalSMILES": "CC(C)NCC(COC1=CC=CC2=CC=CC=C21)O",
                "InChIKey": "AQHHHDLHHXJYJD-UHFFFAOYSA-N",
            },
            "synonyms": ["Propranolol", "Inderal"],
        }
    )
    raw_id = repo.upsert_raw_record(record.raw_record)
    repo.upsert_research_object(record.research_object, raw_id)

    result = HSAResearchService(repo).materialize_entity_lookup_index(
        EntityLookupIndexRequest(source_keys=["pubchem"], limit_per_source=10)
    )
    response = HSAResearchService(repo).resolve_entity_lookup(EntityLookupRequest(query="Inderal", category="compound"))

    assert result.entities_upserted == 1
    assert not repo.list_entities(entity_type="target")
    assert response.result is not None
    assert response.result.canonical_id == "pubchem:4946"


def test_phase_three_api_harvesters_are_registered():
    assert HARVESTERS_V2["pubchem"] is PubChemHarvesterV2
    assert HARVESTERS_V2["chembl"] is ChEMBLHarvesterV2
    assert HARVESTERS_V2["uniprot"] is UniProtHarvesterV2
    assert HARVESTERS_V2["rcsb_pdb"] is RCSBPDBHarvesterV2
    assert HARVESTERS_V2["openfda_animal_events"] is OpenFDAAnimalEventsHarvesterV2


def test_build_chunk_embedding_text_includes_canonical_entity_context():
    object_id = uuid4()
    chunk_id = uuid4()
    research_object = ResearchObject(
        id=object_id,
        object_type="publication",
        title="Canonical entity embedding example",
        abstract="Canine HSA angiogenesis context.",
        source_key="pubmed",
        identifiers={"pmid": "123"},
    )
    chunk = DocumentChunk(
        id=chunk_id,
        research_object_id=object_id,
        chunk_index=0,
        section_label="abstract",
        text_content="KDR is also called VEGF receptor 2 in canine hemangiosarcoma.",
        content_hash="embedding-text-context",
    )
    mentions = [
        EntityMention(
            research_object_id=object_id,
            chunk_id=chunk_id,
            chunk_index=0,
            section_label="abstract",
            source_key="pubmed",
            entity_type="target",
            canonical_name="KDR",
            normalized_key="target:kdr",
            matched_text="VEGF receptor 2",
            matched_alias="VEGF receptor 2",
            chunk_char_start=19,
            chunk_char_end=34,
            external_ids={"chembl_id": "CHEMBL279"},
            resolver_name="unit",
            resolver_version="1",
            match_rule="unit",
        ),
        EntityMention(
            research_object_id=object_id,
            chunk_id=chunk_id,
            chunk_index=0,
            section_label="abstract",
            source_key="pubmed",
            entity_type="disease",
            canonical_name="canine hemangiosarcoma",
            normalized_key="disease:canine_hsa",
            matched_text="canine hemangiosarcoma",
            matched_alias="canine hemangiosarcoma",
            chunk_char_start=38,
            chunk_char_end=61,
            resolver_name="unit",
            resolver_version="1",
            match_rule="unit",
        ),
    ]

    text = build_chunk_embedding_text(chunk, research_object, mentions)

    assert "title: Canonical entity embedding example" in text
    assert "chunk_text: KDR is also called VEGF receptor 2" in text
    assert "canonical_entities:" in text
    assert "target: KDR [target:kdr] (chembl_id=CHEMBL279)" in text
    assert "disease: canine hemangiosarcoma [disease:canine_hsa]" in text


def test_service_search_research_chunks_uses_embeddings_without_returning_vectors(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGFA angiogenesis in canine hemangiosarcoma",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-embedding",
        )
    )
    vegf_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis VEGFA angiogenesis in canine hemangiosarcoma.",
            content_hash="retrieval-embedding-vegf",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=1,
            section_label="methods",
            text_content="Doxorubicin cardiotoxicity monitoring in dogs.",
            content_hash="retrieval-embedding-dox",
        )
    )
    index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")

    results = service.search_research_chunks(
        ResearchChunkSearchRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            embedding_model="local-hash-test",
            limit=2,
        )
    )
    payload = results.model_dump(mode="json")

    assert results.search_mode == "embedding"
    assert results.results[0].match_type == "embedding"
    assert results.results[0].chunk.id == vegf_chunk.id
    assert not _contains_key(payload, "embedding")


def test_service_search_research_chunks_hybrid_reranks_keyword_specific_hits(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    provider = LocalDeterministicEmbeddingProvider(embedding_model="local-hash-test")
    query = "sorafenib canine hemangiosarcoma safety dose limiting toxicity"
    generic_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="dataset",
            title="canine hemangiosarcoma cell lines",
            source_key="geo",
            dedupe_key="geo:retrieval-hybrid-generic",
        )
    )
    generic_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=generic_object_id,
            chunk_index=0,
            section_label="dataset",
            text_content="canine hemangiosarcoma cell lines and tissues",
            content_hash="retrieval-hybrid-generic",
        )
    )
    specific_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Sorafenib safety and dose limiting toxicity in canine hemangiosarcoma",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-hybrid-specific",
        )
    )
    specific_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=specific_object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Sorafenib canine hemangiosarcoma safety dose limiting toxicity evidence.",
            content_hash="retrieval-hybrid-specific",
        )
    )
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=generic_chunk.id,
            research_object_id=generic_object_id,
            chunk_index=0,
            source_key="geo",
            object_type="dataset",
            content_hash="retrieval-hybrid-generic-embedding",
            embedding_model="local-hash-test",
            embedding_dimensions=provider.embedding_dimensions,
            embedding=provider.embed_text(query),
        )
    )
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=specific_chunk.id,
            research_object_id=specific_object_id,
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash="retrieval-hybrid-specific-embedding",
            embedding_model="local-hash-test",
            embedding_dimensions=provider.embedding_dimensions,
            embedding=provider.embed_text("unrelated assay protocol"),
        )
    )

    results = service.search_research_chunks(
        ResearchChunkSearchRequest(
            query=query,
            embedding_model="local-hash-test",
            limit=2,
        )
    )

    assert results.search_mode == "embedding"
    assert results.results[0].chunk.id == specific_chunk.id
    assert results.results[1].chunk.id == generic_chunk.id


def test_service_search_research_chunks_overfetches_stale_embedding_hits(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGFA angiogenesis overfetch example",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-overfetch",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis in canine hemangiosarcoma.",
            content_hash="retrieval-overfetch-valid",
        )
    )
    provider = LocalDeterministicEmbeddingProvider(embedding_model="local-hash-test")
    stale_vector = provider.embed_text("VEGFA angiogenesis")
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=uuid4(),
            research_object_id=object_id,
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash="retrieval-overfetch-stale",
            embedding_model="local-hash-test",
            embedding_dimensions=provider.embedding_dimensions,
            embedding=stale_vector,
        )
    )
    index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")

    results = service.search_research_chunks(
        ResearchChunkSearchRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            embedding_model="local-hash-test",
            limit=1,
        )
    )

    assert results.search_mode == "embedding"
    assert results.results[0].chunk.id == chunk.id


def test_service_search_research_chunks_falls_back_to_keyword_and_bounds_text(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Keyword fallback retrieval example",
            abstract="VEGFA angiogenesis context for canine HSA.",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-keyword",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis " + ("canine hemangiosarcoma " * 20),
            content_hash="retrieval-keyword-vegf",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=1,
            section_label="methods",
            text_content="Chemotherapy dosing schedule.",
            content_hash="retrieval-keyword-other",
        )
    )

    results = service.search_research_chunks(
        ResearchChunkSearchRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            limit=5,
            max_chunk_chars=220,
        )
    )

    assert results.search_mode == "keyword"
    assert results.results[0].match_type == "keyword"
    assert results.results[0].chunk.id == chunk.id
    assert len(results.results[0].chunk.text_content) == 220
    assert results.results[0].text_truncated is True


def test_service_get_chunk_context_and_research_object_are_bounded(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Chunk context example",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-context",
        )
    )
    chunks = [
        repo.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=index,
                section_label=f"section-{index}",
                text_content=text,
                content_hash=f"retrieval-context-{index}",
            )
        )
        for index, text in enumerate(
            [
                "Background chunk about canine hemangiosarcoma.",
                "Middle chunk mentions VEGFA receptor signaling in canine hemangiosarcoma.",
                "Follow-up chunk about translational validation.",
            ]
        )
    ]
    repo.upsert_entity_mention(
        EntityMention(
            research_object_id=object_id,
            chunk_id=chunks[1].id,
            chunk_index=1,
            section_label="section-1",
            source_key="pubmed",
            entity_type="target",
            canonical_name="VEGFA",
            normalized_key="target:vegfa",
            matched_text="VEGFA",
            matched_alias="VEGFA",
            chunk_char_start=22,
            chunk_char_end=27,
            resolver_name="unit",
            resolver_version="1",
            match_rule="unit",
        )
    )

    context = service.get_chunk_context(
        ChunkContextRequest(chunk_id=chunks[1].id, window=1, max_chunk_chars=220)
    )
    object_result = service.get_research_object(
        ResearchObjectReadRequest(research_object_id=object_id, max_chunks=2, max_chunk_chars=220)
    )

    assert context is not None
    assert context.chunk.id == chunks[1].id
    assert [chunk.id for chunk in context.before_chunks] == [chunks[0].id]
    assert [chunk.id for chunk in context.after_chunks] == [chunks[2].id]
    assert context.entity_mentions[0].canonical_name == "VEGFA"
    assert object_result is not None
    assert object_result.research_object.id == object_id
    assert [chunk.id for chunk in object_result.chunks] == [chunks[0].id, chunks[1].id]


def test_document_chunk_upsert_preserves_readable_stable_chunk_id(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Stable chunk id retrieval example",
            source_key="pubmed",
            dedupe_key="pubmed:stable-chunk-id",
        )
    )
    original_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Original text.",
            content_hash="stable-chunk-original",
        )
    )
    replacement_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis stable chunk context.",
            content_hash="stable-chunk-replacement",
        )
    )

    search = service.search_research_chunks(
        ResearchChunkSearchRequest(query="VEGFA angiogenesis", source_key="pubmed", limit=1)
    )
    index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    smoke = service.run_retrieval_smoke(
        RetrievalSmokeRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            embedding_model="local-hash-test",
            require_embedding=True,
        )
    )

    assert replacement_chunk.id == original_chunk.id
    assert repo.get_document_chunk(replacement_chunk.id) is not None
    assert search.results[0].chunk.id == original_chunk.id
    assert smoke.passed is True
    assert smoke.selected_chunk_id == original_chunk.id
    assert smoke.chunk_context is not None
    assert smoke.chunk_context.chunk.content_hash == "stable-chunk-replacement"


def test_mcp_full_text_triage_helper_dumps_action(monkeypatch, tmp_path):
    service = make_service(tmp_path)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)

    payload = mcp_server.triage_full_text_issue_tool(
        source_key="europe_pmc",
        stage="dagster_run",
        error_message="Timed out while running hosted refresh",
        runtime_seconds=2700,
        timeout_seconds=2700,
        raw_records=10,
        research_objects=10,
    )

    assert payload["triage_name"] == "full_text_triage_agent"
    assert payload["action"] == "reduce_batch_size"
    assert payload["should_retry"] is True
    assert payload["should_block_schedule"] is True


def test_backfill_papers_json_creates_object_and_chunk(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    papers_path = tmp_path / "papers.json"
    papers_path.write_text(
        """
        [
          {
            "pmid": "123",
            "doi": "10.1234/hsa",
            "title": "Canine hemangiosarcoma backfill",
            "abstract": "This abstract mentions canine hemangiosarcoma.",
            "journal": "Example Journal",
            "year": "2026",
            "source": "pubmed",
            "url": "https://pubmed.ncbi.nlm.nih.gov/123/"
          }
        ]
        """
    )

    result = backfill_papers_json(repo, papers_path)
    coverage = repo.coverage_summary()

    assert result.raw_records == 1
    assert result.research_objects == 1
    assert result.document_chunks == 1
    assert coverage["document_chunks"] == 1


def test_backfill_deep_dives_creates_knowledge_entry_chunks(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    deep_dives = tmp_path / "deep_dives"
    deep_dives.mkdir()
    (deep_dives / "treatment_example.md").write_text(
        "# Treatment Example\n\n## TL;DR\n\nThis is a local knowledge entry.\n\n## Detail\n\nMore text."
    )

    result = backfill_deep_dives(repo, deep_dives)
    objects = repo.list_research_objects(object_type="knowledge_entry")

    assert result.raw_records == 1
    assert result.research_objects == 1
    assert result.document_chunks == 1
    assert objects[0].metadata["track"] == "treatment"


def test_local_claim_extractor_attaches_persisted_entity_mentions(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    title = "Propranolol and VEGF in canine hemangiosarcoma"
    obj_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title=title,
            abstract="Canine hemangiosarcoma studies discuss propranolol with VEGF and angiogenesis.",
            source_key="pubmed",
        )
    )
    for chunk in chunk_text(obj_id, title, section_label="title_abstract"):
        repo.upsert_document_chunk(chunk)
    resolved = resolve_entities_for_repository(repo, source_key="pubmed")
    mentions = repo.list_entity_mentions(source_key="pubmed")

    result = extract_claims_for_repository(repo, source_key="pubmed")
    claims = repo.search_claims(
        ClaimSearchRequest(query="propranolol", species="canine", min_confidence=0.1, include_drafts=True)
    )

    assert resolved.mentions_upserted >= 2
    assert result.claims_written >= 1
    assert claims
    assert set(claims[0].metadata["source_entity_mention_ids"]) == {str(mention.mention_id) for mention in mentions}
    assert set(claims[0].metadata["source_entity_canonical_names"]) >= {"propranolol", "VEGFA"}
    assert set(claims[0].metadata["source_entity_types"]) >= {"compound", "target"}


def test_unpaywall_claim_extractor_creates_oa_discovery_context_claim(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Human angiosarcoma open access review",
            source_key="unpaywall",
            metadata={
                "license_policy": "metadata_and_open_access_location_links",
                "best_oa_location": {
                    "url_for_landing_page": "https://example.org/article",
                    "url_for_pdf": "https://example.org/article.pdf",
                    "license": "cc-by",
                },
            },
        )
    )
    for chunk in chunk_text(
        object_id,
        "Human angiosarcoma open access review. OA status: gold. License: cc-by.",
        section_label="oa_discovery_metadata",
    ):
        repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, source_key="unpaywall", limit=10)
    claims = repo.list_claims(source_key="unpaywall", include_seed_claims=True, limit=10)

    assert result.claims_written == 1
    assert claims[0].statement.startswith("Unpaywall record provides human angiosarcoma")
    assert claims[0].metadata["rule_key"] == "source-context:human_angiosarcoma_analog"


def test_local_claim_extractor_creates_dataset_source_context_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    geo_id = repo.upsert_research_object(
        ResearchObject(
            object_type="dataset",
            title="Canine hemangiosarcoma expression dataset",
            source_key="geo",
        )
    )
    sra_id = repo.upsert_research_object(
        ResearchObject(
            object_type="dataset",
            title="Dog hemangiosarcoma sequence runs",
            source_key="sra",
        )
    )
    icdc_id = repo.upsert_research_object(
        ResearchObject(
            object_type="dataset",
            title="ICDC canine case CASE-1: Hemangiosarcoma",
            source_key="icdc",
        )
    )
    for object_id, text in (
        (geo_id, "Canine hemangiosarcoma expression dataset."),
        (sra_id, "Dog hemangiosarcoma sequence runs."),
        (icdc_id, "Diagnosis: Hemangiosarcoma. Species: canine."),
    ):
        for chunk in chunk_text(object_id, text, section_label="dataset_metadata"):
            repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, limit=10)
    claims = repo.search_claims(ClaimSearchRequest(query="source context", min_confidence=0.1, include_drafts=True, limit=10))
    statements = [claim.statement for claim in claims]

    assert result.claims_written == 3
    assert any("GEO record provides canine HSA source context" in statement for statement in statements)
    assert any("SRA record provides canine HSA source context" in statement for statement in statements)
    assert any("ICDC record provides canine HSA source context" in statement for statement in statements)


def test_local_claim_extractor_creates_structured_source_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    pubchem_id = repo.upsert_research_object(
        ResearchObject(
            object_type="compound_record",
            title="Propranolol",
            source_key="pubchem",
            identifiers={"pubchem_cid": "4946", "inchikey": "AQHHHDLHHXJYJD-UHFFFAOYSA-N"},
            metadata={"identity_match": {"identity_verified": True}},
        )
    )
    uniprot_id = repo.upsert_research_object(
        ResearchObject(
            object_type="structure",
            title="KDR Vascular endothelial growth factor receptor 2",
            source_key="uniprot",
            identifiers={"uniprot_accession": "P35968"},
            metadata={
                "target_gene": "KDR",
                "organism": "Homo sapiens",
                "species_scope": "human",
                "reviewed": True,
                "alphafold_ids": ["AF-P35968-F1"],
            },
        )
    )
    rcsb_id = repo.upsert_research_object(
        ResearchObject(
            object_type="structure",
            title="Human VEGFR2 kinase domain",
            source_key="rcsb_pdb",
            identifiers={"pdb_id": "3VHE"},
            metadata={"target_gene": "KDR", "experimental_methods": ["X-RAY DIFFRACTION"]},
        )
    )
    openfda_id = repo.upsert_research_object(
        ResearchObject(
            object_type="safety_report",
            title="openFDA animal adverse event for Doxorubicin in Dog",
            source_key="openfda_animal_events",
            identifiers={"openfda_report_id": "US-FDA-CVM-2026-0001"},
            metadata={
                "matched_drug_name": "Doxorubicin",
                "species": "Dog",
                "reaction_terms": ["Vomiting", "Neutropenia"],
                "serious_ae": "true",
            },
        )
    )
    for object_id, label in (
        (pubchem_id, "compound_metadata"),
        (uniprot_id, "protein_target_metadata"),
        (rcsb_id, "structure_metadata"),
        (openfda_id, "safety_report_metadata"),
    ):
        for chunk in chunk_text(object_id, "structured source text", section_label=label):
            repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, limit=10)
    claims = repo.search_claims(ClaimSearchRequest(min_confidence=0.1, include_drafts=True, limit=20))
    statements = [claim.statement for claim in claims]

    assert result.claims_written == 4
    assert any("PubChem compound identity CID 4946" in statement for statement in statements)
    assert any("UniProtKB target metadata for Homo sapiens" in statement for statement in statements)
    assert any("RCSB PDB contains experimental structure 3VHE" in statement for statement in statements)
    assert any("openFDA animal adverse event signal reports in Dog" in statement for statement in statements)


def test_claim_curator_keeps_pmc_oa_source_context_review_only(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Licensed full text source context",
            source_key="pmc_oa",
        )
    )
    text = "Human angiosarcoma source context. " + ("Licensed full text background. " * 12)
    for index in range(6):
        repo.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=index,
                section_label="full_text",
                text_content=text,
                content_hash=f"pmc-oa-source-context-{index}",
            )
        )

    extract_claims_for_repository(repo, source_key="pmc_oa")
    result = ClaimCuratorAgent(repo).curate(ClaimCurationRequest(source_key="pmc_oa", limit=20, promote_threshold=0.5))
    review_decisions = [item for item in result.decisions if item.decision == "needs_review"]

    assert result.promoted == 0
    assert result.needs_review == 1
    assert result.merged_duplicates == 5
    assert review_decisions
    assert "source-context triage claim is review-only" in review_decisions[0].reasons
    assert "licensed full-text chunk has substantive snippet" in review_decisions[0].reasons


def test_claim_curator_downgrades_stale_source_context_promotions(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Stale source context",
            source_key="crossref",
        )
    )
    claim_id = uuid4()
    repo.upsert_claim(
        ClaimSearchResult(
            claim_id=claim_id,
            statement="Crossref record provides canine-human comparative angiosarcoma/HSA source context relevant to HSA evidence triage.",
            claim_type=ClaimType.OTHER,
            direction=ClaimDirection.NEUTRAL,
            confidence=0.7,
            evidence_level=EvidenceLevel.UNKNOWN,
            source_object_id=object_id,
            support_count=1,
            metadata={
                "curation_status": "promote",
                "curation_score": 0.7,
                "extraction_status": "curated",
                "rule_key": "source-context:canine_human_comparative",
                "context_key": "canine_human_comparative",
                "source_chunk_id": str(uuid4()),
            },
        )
    )

    result = ClaimCuratorAgent(repo).curate(ClaimCurationRequest(source_key="crossref", limit=20))
    updated = repo.get_claim(claim_id)

    assert result.needs_review == 1
    assert updated is not None
    assert updated.metadata["curation_status"] == "needs_review"
    assert updated.metadata["extraction_status"] == "draft"
    assert updated.confidence == 0.49


def test_source_scout_prioritizes_zero_coverage_bridges(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()

    result = SourceScoutAgent(repo).scout(SourceScoutRequest(max_recommendations=5))
    keys = [recommendation.source_key for recommendation in result.recommendations]

    assert "pubmed" in keys
    assert "europe_pmc" in keys
    assert result.recommendations[0].status == "coverage_gap"
    assert result.next_actions


def test_scrape_profiles_keep_avma_approval_gated():
    profiles = {profile.source_key: profile for profile in list_scrape_profiles()}

    assert "avma_vctr" in profiles
    assert profiles["avma_vctr"].approval_required is True
    assert profiles["avma_vctr"].enabled is False
    assert profiles["avma_vctr"].robots_policy == "unknown"
    assert profiles["avma_vctr"].parser == "avma_vctr"
    assert profiles["x_linked_article"].approval_required is True
    assert profiles["x_linked_article"].enabled is False
    assert profiles["x_linked_article"].parser == "generic_html"


def test_scrape_bridge_refuses_approval_gated_fetch_without_approval(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    result = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts").fetch(
        ScrapeFetchRequest(
            source_key="avma_vctr",
            urls=["https://veterinaryclinicaltrials.org/"],
        )
    )

    assert result.fetched_pages == 0
    assert result.artifact_ids == []
    assert "requires explicit approval" in result.errors[0]


def test_disabled_scrape_source_requires_profile_review_before_approved_fetch(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    result = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts").fetch(
        ScrapeFetchRequest(
            source_key="avma_vctr",
            urls=["https://veterinaryclinicaltrials.org/"],
            approved_by="unit-test",
        )
    )

    assert result.fetched_pages == 0
    assert "requires source profile review" in result.errors[0]


def test_scrape_profile_review_is_persisted(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    review = bridge.review_profile(
        ScrapeProfileReviewRequest(
            source_key="avma_vctr",
            robots_policy="reviewed",
            approved_for_fetch=True,
            reviewed_by="unit-test",
            review_note="robots and storage policy reviewed",
        )
    )

    assert review.approved_for_fetch is True
    assert review.robots_policy == "reviewed"
    assert repo.get_scrape_profile_review("avma_vctr").reviewed_by == "unit-test"


def test_x_linked_article_parser_extracts_primary_source_links(tmp_path, monkeypatch):
    html_path = tmp_path / "article.html"
    html_path.write_text(
        """
        <html>
          <head><title>Angiosarcoma genomic landscape</title></head>
          <body>
            <a href="https://pubmed.ncbi.nlm.nih.gov/12345678/">PubMed</a>
            <a href="https://doi.org/10.1158/0008-5472.CAN-26-0001">Cancer Research DOI</a>
            Clinical trial NCT12345678 is also mentioned.
            A trailing delimiter should normalize: 10.1186/s40425-017-0263-0&
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="x_linked_article",
        display_name="X Linked Article Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="generic_html",
        storage_policy="metadata_and_link_review",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="x_linked_article", urls=[html_path.as_uri()]))
    record = bridge.parse("x_linked_article").records[0]
    source_links = record.fields["primary_source_links"]

    assert record.record_type == "publication"
    assert record.parser_confidence >= 0.55
    assert {
        (link["recommended_source_key"], link["identifier_type"], link["identifier"])
        for link in source_links
    } >= {
        ("pubmed", "pmid", "12345678"),
        ("crossref", "doi", "10.1158/0008-5472.CAN-26-0001"),
        ("crossref", "doi", "10.1186/s40425-017-0263-0"),
        ("clinicaltrials_gov", "nct", "NCT12345678"),
    }
    assert all(not link["identifier"].endswith("&") for link in source_links)


def test_x_linked_article_parser_extracts_metadata_identifiers(tmp_path, monkeypatch):
    html_path = tmp_path / "article-metadata.html"
    html_path.write_text(
        """
        <html>
          <head>
            <meta name="citation_title" content="Angiosarcoma translational trial">
            <meta name="citation_doi" content="10.1186/s40425-019-0689-7">
            <meta name="citation_pmid" content="31395100">
            <script type="application/ld+json">
            {
              "@type": "ScholarlyArticle",
              "headline": "JSON-LD title",
              "identifier": ["PMC6686562"],
              "datePublished": "2019-08-01"
            }
            </script>
          </head>
          <body><p>No visible primary links.</p></body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="x_linked_article",
        display_name="X Linked Article Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="generic_html",
        storage_policy="metadata_and_link_review",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="x_linked_article", urls=[html_path.as_uri()]))
    record = bridge.parse("x_linked_article").records[0]

    assert record.title == "Angiosarcoma translational trial"
    assert record.fields["article_metadata"]["doi"] == "10.1186/s40425-019-0689-7"
    assert record.fields["article_metadata"]["pmid"] == "31395100"
    assert record.fields["article_metadata"]["pmcid"] == "PMC6686562"
    assert {
        (link["recommended_source_key"], link["identifier_type"], link["identifier"])
        for link in record.fields["primary_source_links"]
    } >= {
        ("crossref", "doi", "10.1186/s40425-019-0689-7"),
        ("pubmed", "pmid", "31395100"),
        ("pmc_oa", "pmcid", "PMC6686562"),
    }


def test_x_linked_article_parser_keeps_context_separate_when_no_primary_identifier(tmp_path, monkeypatch):
    html_path = tmp_path / "article-context.html"
    html_path.write_text(
        """
        <html>
          <head><title>Angiosarcoma frontier</title></head>
          <body>
            <p>Now, for the first time, the team has performed comprehensive genomic
            profiling of angiosarcoma cells, analyzing hundreds of genes in specific
            cell types and studying how they interact with the environment.</p>
            <p>The work was <a href="https://www.abstractsonline.com/pp8/#!/21436/presentation/7856">
            presented</a> at the American Association for Cancer Research Annual
            Meeting 2026 and showed RAS plays a role in survival and spread.</p>
            <p>Research in Kim's lab focuses on angiosarcoma's counterpart in dogs,
            hemangiosarcoma, which is common in dogs.</p>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="x_linked_article",
        display_name="X Linked Article Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="generic_html",
        storage_policy="metadata_and_link_review",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="x_linked_article", urls=[html_path.as_uri()]))
    record = bridge.parse("x_linked_article").records[0]

    assert record.fields["primary_source_links"] == []
    assert record.parser_confidence >= 0.45
    assert "hemangiosarcoma" in record.fields["article_text_preview"]
    assert any("comprehensive genomic profiling" in span["text"] for span in record.fields["evidence_spans"])
    assert record.fields["context_links"] == [
        {
            "href": "https://www.abstractsonline.com/pp8/#!/21436/presentation/7856",
            "text": "presented",
            "host": "www.abstractsonline.com",
            "reason": "conference_abstract_link",
        }
    ]


def test_x_linked_article_review_openrouter_preserves_queue_guardrail(tmp_path, monkeypatch):
    from hsa_research.ingestion_bridge import x_linked_article_review

    repo = SQLiteResearchRepository(tmp_path / "linked-review-openrouter.sqlite3", seed=False)
    review = repo.upsert_scrape_review(
        ScrapeReviewRecord(
            source_key="x_linked_article",
            artifact_id=uuid4(),
            source_record_id="article-1",
            title="Angiosarcoma article",
            parser_confidence=0.7,
            fields={
                "primary_source_links": [
                    {
                        "url": "https://doi.org/10.1234/test",
                        "recommended_source_key": "crossref",
                        "identifier_type": "doi",
                        "identifier": "10.1234/test",
                        "should_ingest": True,
                        "reason": "DOI found.",
                    }
                ]
            },
        )
    )

    def fake_review_model(model_name, review_payload):
        assert model_name == "anthropic/claude-sonnet-test"
        assert review_payload["articles"][0]["review_id"] == str(review.review_id)
        return {
            "text": json.dumps(
                {
                    "actions": [
                        {
                            "review_id": str(review.review_id),
                            "source_record_id": "article-1",
                            "action": "needs_human_review",
                            "severity": "watch",
                            "reason": "Model wants a human pass.",
                        }
                    ],
                    "evidence": {"review_summary": "reviewed"},
                    "errors": [],
                }
            ),
            "metadata": {"model_name": "anthropic/claude-sonnet-test", "usage": {"cost": 0.01}},
        }

    monkeypatch.setattr(x_linked_article_review, "_openrouter_review_model", fake_review_model)

    result = HSAResearchService(repo).run_x_linked_article_review(
        XLinkedArticleReviewRequest(
            review_ids=[review.review_id],
            review_mode="openrouter_required",
            review_models=["anthropic/claude-sonnet-test"],
        )
    )

    assert result.queue_candidate_count == 1
    assert [action.action for action in result.actions] == [
        "needs_human_review",
        "queue_primary_source_followup",
    ]
    assert result.evidence["model_reviews"][0]["status"] == "completed"


def test_x_linked_article_review_openrouter_rejects_unvalidated_context_links(tmp_path, monkeypatch):
    from hsa_research.ingestion_bridge import x_linked_article_review

    repo = SQLiteResearchRepository(tmp_path / "linked-review-invalid-model.sqlite3", seed=False)
    review = repo.upsert_scrape_review(
        ScrapeReviewRecord(
            source_key="x_linked_article",
            artifact_id=uuid4(),
            source_record_id="article-context",
            title="Angiosarcoma conference article",
            parser_confidence=0.45,
            fields={
                "primary_source_links": [],
                "context_links": [
                    {
                        "href": "https://www.abstractsonline.com/pp8/#!/21436/presentation/7856",
                        "text": "presented",
                        "host": "www.abstractsonline.com",
                        "reason": "conference_abstract_link",
                    }
                ],
                "evidence_spans": [
                    {
                        "text": "The work was presented at AACR.",
                        "matched_terms": ["presented"],
                        "reason": "article_body_source_context",
                    }
                ],
            },
        )
    )

    def fake_review_model(model_name, review_payload):
        assert review_payload["articles"][0]["context_links"][0]["host"] == "www.abstractsonline.com"
        assert review_payload["articles"][0]["evidence_spans"]
        return {
            "text": json.dumps(
                {
                    "actions": [
                        {
                            "review_id": str(review.review_id),
                            "source_record_id": "article-context",
                            "action": "queue_primary_source_followup",
                            "severity": "watch",
                            "reason": "Model tried to queue a conference page.",
                            "followup_links": [
                                {
                                    "url": "https://www.abstractsonline.com/pp8/#!/21436/presentation/7856",
                                    "recommended_source_key": "x_linked_article",
                                    "identifier_type": "unknown",
                                    "identifier": None,
                                    "should_ingest": True,
                                    "reason": "Conference context.",
                                }
                            ],
                        }
                    ],
                    "evidence": {"review_summary": "reviewed"},
                    "errors": [],
                }
            ),
            "metadata": {"model_name": "anthropic/claude-sonnet-test", "usage": {"cost": 0.01}},
        }

    monkeypatch.setattr(x_linked_article_review, "_openrouter_review_model", fake_review_model)

    result = HSAResearchService(repo).run_x_linked_article_review(
        XLinkedArticleReviewRequest(
            review_ids=[review.review_id],
            review_mode="openrouter_required",
            review_models=["anthropic/claude-sonnet-test"],
        )
    )

    assert result.actions[0].action == "needs_human_review"
    assert result.actions[0].followup_links == []
    assert result.queue_candidate_count == 0
    assert result.needs_human_review_count >= 1


def test_scrape_bridge_ingest_requires_approval(tmp_path, monkeypatch):
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    result = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts").ingest(
        ScrapeIngestRequest(source_key="test_scraper")
    )

    assert result.promoted_records == 0
    assert "requires explicit approval" in result.errors[0]


def test_scrape_bridge_promotes_snapshot_after_review_approval(tmp_path, monkeypatch):
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
    parse = bridge.parse("test_scraper")
    review = bridge.review(
        ScrapeReviewRequest(
            source_key="test_scraper",
            review_ids=parse.review_ids,
            decision="accepted",
            reviewed_by="unit-test",
            review_note="fields look valid",
        )
    )

    ingest = bridge.ingest(
        ScrapeIngestRequest(
            source_key="test_scraper",
            review_ids=[record.review_id for record in review.records],
            approved_by="unit-test",
            approval_note="reviewed parsed fields",
        )
    )
    objects = repo.list_research_objects(source_key="test_scraper")

    assert fetch.fetched_pages == 1
    assert review.reviewed_records == 1
    assert ingest.promoted_records == 1
    assert ingest.review_records_seen == 1
    assert ingest.raw_records == 1
    assert ingest.research_objects == 1
    assert ingest.document_chunks == 1
    assert objects[0].title == "Canine Hemangiosarcoma Trial"
    assert objects[0].object_type == "veterinary_trial"
    assert objects[0].metadata["review_status"] == "accepted"
    assert objects[0].metadata["approved_by"] == "unit-test"
    assert objects[0].metadata["review_id"] == str(review.records[0].review_id)


def test_scrape_review_queue_preserves_review_decision_on_reparse(tmp_path, monkeypatch):
    html_path = tmp_path / "trial.html"
    html_path.write_text("<html><head><title>Reviewed Trial</title></head><body></body></html>", encoding="utf-8")
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        parser="generic_html",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")
    bridge.fetch(ScrapeFetchRequest(source_key="test_scraper", urls=[html_path.as_uri()]))
    first_parse = bridge.parse("test_scraper")
    bridge.review(
        ScrapeReviewRequest(
            source_key="test_scraper",
            review_ids=first_parse.review_ids,
            decision="rejected",
            reviewed_by="unit-test",
            review_note="not a target source",
        )
    )

    second_parse = bridge.parse("test_scraper")
    reviews = repo.list_scrape_reviews(source_key="test_scraper")

    assert second_parse.review_ids == first_parse.review_ids
    assert second_parse.records[0].review_status == "rejected"
    assert len(reviews) == 1
    assert reviews[0].review_status == "rejected"
    assert reviews[0].reviewer == "unit-test"


def test_avma_vctr_parser_extracts_trial_fields(tmp_path, monkeypatch):
    html_path = tmp_path / "avma.html"
    html_path.write_text(
        """
        <html>
          <head>
            <meta property="og:title" content="Evaluation of a combination of three drugs in dogs with hemangiosarcoma">
            <meta name="description" content="Combination therapy for dogs with Hemangiosarcoma.">
          </head>
          <body>
            <h1>Evaluation of a combination of three drugs in dogs with hemangiosarcoma</h1>
            <p>The objective of this study is to investigate doxorubicin or carboplatin and temozolomide with propranolol in dogs with hemangiosarcoma.</p>
            <dl>
              <dt>Condition</dt><dd>Hemangiosarcoma</dd>
              <dt>Species</dt><dd>Canine</dd>
              <dt>Study Type</dt><dd>Drug</dd>
              <dt>Funding</dt><dd>Unfunded</dd>
              <dt>Status</dt><dd>Recruiting</dd>
              <dt>Investigator</dt><dd>Claire Lemons, DVM</dd>
            </dl>
            <a href="/s/combination-therapy-hsa-123456/">Learn More</a>
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

    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[html_path.as_uri()]))
    parse = bridge.parse("avma_vctr_test")
    record = parse.records[0]

    assert parse.parsed_records == 1
    assert record.title == "Evaluation of a combination of three drugs in dogs with hemangiosarcoma"
    assert record.source_record_id.endswith("/avma.html")
    assert record.record_type == "veterinary_trial"
    assert record.fields["condition"] == "Hemangiosarcoma"
    assert record.fields["species"] == "Canine"
    assert record.fields["study_type"] == "Drug"
    assert record.fields["funding"] == "Unfunded"
    assert record.fields["status"] == "Recruiting"
    assert record.fields["investigator"] == "Claire Lemons, DVM"
    assert record.parser_confidence >= 0.3


def test_avma_vctr_parser_extracts_embedded_study_json(tmp_path, monkeypatch):
    html_path = tmp_path / "embedded.html"
    html_path.write_text(
        """
        <html>
          <head><meta property="og:title" content="Antibody therapy for dogs with splenic hemangiosarcoma"></head>
          <body>
            <script id="d_study_keywords" type="application/json">["hemangiosarcoma", "VEGF"]</script>
            <script id="d_avma_study_data" type="application/json">
              {"vct_code": "VCT16000189", "patients_randomly_assigned": true}
            </script>
            <script id="d_avma_studycontent_data" type="application/json">
              {
                "diagnosis": "Hemangiosarcoma",
                "inclusion_criteria": "<p>Splenic hemangiosarcoma after splenectomy.</p>",
                "exclusion_criteria": "Metastatic disease at screening.",
                "intervention_name": "Anti-VEGF antibody",
                "potential_benefits": "Increased time to progression",
                "potential_risks": "Elevated blood pressure",
                "pri_outcome_name": "Safety",
                "pri_outcome_measure": "Blood pressure measurement",
                "pri_outcome_endpoint": "Safety",
                "sec_outcome1_name": "Overall survival",
                "sec_outcome1_measure": "Survival tracking",
                "sec_outcome1_endpoint": "Death or euthanasia",
                "funding_source_institution": true
              }
            </script>
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
        parser="avma_vctr",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[html_path.as_uri()]))
    record = bridge.parse("avma_vctr_test").records[0]

    assert record.fields["vct_code"] == "VCT16000189"
    assert record.fields["condition"] == "Hemangiosarcoma"
    assert record.fields["keywords"] == ["hemangiosarcoma", "VEGF"]
    assert record.fields["intervention"] == "Anti-VEGF antibody"
    assert record.fields["eligibility"] == "Splenic hemangiosarcoma after splenectomy."
    assert record.fields["primary_outcome"]["measure"] == "Blood pressure measurement"
    assert record.fields["secondary_outcomes"][0]["name"] == "Overall survival"
    assert record.fields["funding_sources"] == ["institution"]
    assert record.parser_confidence >= 0.65


def test_avma_vctr_parser_keeps_sparse_pages_low_confidence(tmp_path, monkeypatch):
    html_path = tmp_path / "sparse.html"
    html_path.write_text("<html><head><title>Unknown Veterinary Page</title></head><body></body></html>", encoding="utf-8")
    profile = ScrapeSourceProfile(
        source_key="avma_vctr_test",
        display_name="AVMA Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        parser="avma_vctr",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[html_path.as_uri()]))
    parse = bridge.parse("avma_vctr_test")
    record = parse.records[0]

    assert record.title == "Unknown Veterinary Page"
    assert "condition" not in record.fields
    assert "species" not in record.fields
    assert record.parser_confidence < 0.3


def test_fetch_scrape_manifest_fetches_manifest_candidate_pages(tmp_path, monkeypatch):
    seed_path = tmp_path / "seed.html"
    detail_dir = tmp_path / "s"
    detail_dir.mkdir()
    detail_path = detail_dir / "solid-tumor-study.html"
    detail_path.write_text(
        "<html><head><title>Solid tumor study</title></head><body>Canine solid tumor trial.</body></html>",
        encoding="utf-8",
    )
    seed_path.write_text(f'<html><body><a href="{detail_path.as_uri()}">Solid tumor study</a></body></html>', encoding="utf-8")
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

    fetch = bridge.fetch_manifest(
        ScrapeManifestFetchRequest(
            source_key="avma_vctr_test",
            manifest_artifact_id=manifest.manifest_artifact_id,
            max_pages=1,
        )
    )

    assert fetch.fetched_pages == 1
    assert len(fetch.artifact_ids) == 1
    assert repo.get_artifact(fetch.artifact_ids[0]).metadata["source_url"] == detail_path.as_uri()


def test_scrape_bridge_skips_urls_outside_profile_allowlist(tmp_path, monkeypatch):
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url="file:///allowed",
        allowed_url_patterns=["file:///allowed/*"],
        robots_policy="reviewed",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    result = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts").fetch(
        ScrapeFetchRequest(source_key="test_scraper", urls=[(tmp_path / "outside.html").as_uri()])
    )

    assert result.fetched_pages == 0
    assert result.skipped_pages == 1
    assert "outside allowed patterns" in result.errors[0]
