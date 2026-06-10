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
    _md_runpod_input,
    _ready_for_therapy_ideas_program,
    _research_program_fixture,
    _seed_evaluated_brief,
    _seed_full_text_source_claim,
    _seed_minimal_source_claim,
    _seed_program_committee_corpus,
    _write_minimal_xlsx,
    _xlsx_column_name,
)

def test_public_candidate_contracts_validate_and_reject_bad_status():
    candidate = PublicCandidateRecord(
        candidate_id="twog-candidate-test",
        display_id="TWOG-TEST",
        candidate_kind="peptide",
        title="Vimentin peptide strategy",
        public_status="proposed",
        visibility="draft_public",
        targets=["VIM", "VIM"],
        candidate_therapies=["vimentin-targeting peptide"],
        evidence_refs=["C1", "C1"],
    )
    assert candidate.targets == ["VIM"]
    assert candidate.evidence_refs == ["C1"]

    snapshot = PublicCandidateSnapshot(
        candidate_id=candidate.candidate_id,
        content_hash="abc123456789",
        title=candidate.title,
        candidate_kind=candidate.candidate_kind,
        public_status=candidate.public_status,
    )
    assert snapshot.snapshot_version == 1

    event = PublicCandidateDecisionEvent(
        candidate_id=candidate.candidate_id,
        action="proposed",
        new_status="proposed",
    )
    assert event.actor == "twog_system"

    payload = candidate.model_dump(mode="json")
    payload["public_status"] = "maybe_ready"
    with pytest.raises(ValidationError):
        PublicCandidateRecord.model_validate(payload)


def test_public_candidate_snapshot_generation_repairs_existing_snapshot_manifest(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "public-candidate-manifest-repair.sqlite3", seed=False)
    service = HSAResearchService(repo)
    missing_therapy_idea_id = uuid4()
    snapshot_id = uuid4()
    candidate = PublicCandidateRecord(
        candidate_id="twog-candidate-existing",
        display_id="TWOG-EXISTING",
        title="Existing public candidate",
        summary="Existing static candidate generated before run manifest receipts.",
        public_status="investigating",
        visibility="draft_public",
        therapy_idea_id=missing_therapy_idea_id,
        latest_snapshot_id=snapshot_id,
        content_hash="hash-existing",
    )
    snapshot = PublicCandidateSnapshot(
        snapshot_id=snapshot_id,
        candidate_id=candidate.candidate_id,
        snapshot_version=1,
        content_hash="hash-existing",
        title=candidate.title,
        public_status="investigating",
        payload={"identity": {"candidate_id": candidate.candidate_id}, "reproducibility": {}},
    )
    repo.upsert_public_candidate(candidate)
    repo.upsert_public_candidate_snapshot(snapshot)

    result = service.generate_public_candidate_snapshot(
        PublicCandidateGenerateRequest(
            candidate_id=candidate.candidate_id,
            therapy_idea_id=missing_therapy_idea_id,
            visibility="draft_public",
            pipeline_version="manifest-repair-test",
            commit_sha="repair123",
            metadata={"dagster_run_id": "dagster-run-repair"},
        )
    )

    assert result.errors == []
    assert result.candidate is not None
    assert result.snapshot is not None
    assert result.snapshot.snapshot_id == snapshot_id
    assert result.snapshot.metadata["manifest_repair"] is True
    assert result.snapshot.metadata["run_manifest_id"]
    assert result.snapshot.metadata["trace_id"]
    assert result.snapshot.payload["reproducibility"]["run_manifest_id"] == result.snapshot.metadata["run_manifest_id"]
    assert result.snapshot.payload["reproducibility"]["dagster_run_id"] == "dagster-run-repair"
    manifest = repo.get_run_manifest(UUID(result.snapshot.metadata["run_manifest_id"]))
    assert manifest is not None
    assert manifest.manifest_type == "public_candidate_snapshot"
    assert manifest.candidate_ids == [candidate.candidate_id]
    assert manifest.therapy_idea_ids == [missing_therapy_idea_id]
    assert result.decision_events[0].action == "annotated"


def test_public_candidate_generation_blocks_low_grade_incremental_ideas(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "public-candidate-moonshot-gate.sqlite3", seed=False)
    service = HSAResearchService(repo)
    idea = TherapyIdea(
        title="Pazopanib dose monitoring follow-up",
        hypothesis="Dose monitoring might make pazopanib safer in canine HSA.",
        rationale="This is a useful operational note, but it is not a big public candidate thesis.",
        candidate_therapies=["pazopanib"],
        targets=["KDR"],
        evidence_refs=["C1"],
        evidence_strength="low",
        next_experiments=[],
        risks=[],
        priority_score=0.61,
    )
    repo.upsert_therapy_idea(
        TherapyIdeaRecord(
            idea=idea,
            topic="incremental pazopanib follow-up",
            status="proposed",
            score=0.61,
        )
    )

    blocked = service.generate_public_candidate_snapshot(
        PublicCandidateGenerateRequest(therapy_idea_id=idea.idea_id, visibility="draft_public")
    )

    assert blocked.candidate is None
    assert blocked.snapshot is None
    assert "public_candidate_requires_moonshot_grade" in blocked.errors
    assert blocked.moonshot_gate["passed"] is False
    assert "frontier_weighted_score_below_0.80" in blocked.moonshot_gate["blockers"]
    assert "missing_frontier_modality" in blocked.moonshot_gate["blockers"]

    preview = service.generate_public_candidate_snapshot(
        PublicCandidateGenerateRequest(
            therapy_idea_id=idea.idea_id,
            visibility="draft_public",
            require_moonshot_grade=False,
            persist=False,
        )
    )

    assert preview.errors == []
    assert preview.candidate is not None
    assert preview.moonshot_gate["passed"] is False


def test_public_candidate_generation_weights_frontier_modalities_over_conventional_score(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "public-candidate-frontier-moonshot.sqlite3", seed=False)
    service = HSAResearchService(repo)
    idea = TherapyIdea(
        title="Personalized mRNA neoantigen vaccine for endothelial HSA antigens",
        hypothesis=(
            "A personalized mRNA neoantigen vaccine could train immune recognition around "
            "endothelial tumor antigens in canine HSA."
        ),
        rationale=(
            "This is a frontier modality thesis: use tumor sequencing and antigen selection "
            "to create a testable personalized vaccine strategy, not a conventional monotherapy tweak."
        ),
        candidate_therapies=["personalized mRNA neoantigen vaccine"],
        targets=["VIM", "KDR"],
        biomarkers=["neoantigen load", "endothelial antigen expression"],
        mechanism="mRNA vaccine expression of selected tumor antigens to prime anti-tumor immunity.",
        evidence_refs=["C1", "C2"],
        evidence_strength="medium",
        next_experiments=["Define antigen-selection metrics and cross-species comparator data."],
        risks=["Neoantigen prediction may not translate across canine HSA tumors."],
        priority_score=0.45,
    )
    repo.upsert_therapy_idea(
        TherapyIdeaRecord(
            idea=idea,
            topic="frontier personalized vaccine program",
            status="ready_for_promotion",
            score=0.45,
            source_program_id=uuid4(),
        )
    )

    result = service.generate_public_candidate_snapshot(
        PublicCandidateGenerateRequest(therapy_idea_id=idea.idea_id, visibility="draft_public", persist=False)
    )

    assert result.errors == []
    assert result.candidate is not None
    assert result.moonshot_gate["passed"] is True
    frontier = result.moonshot_gate["frontier_policy"]
    assert frontier["frontier_modality_weight"] == 0.9
    assert "mrna_personalized_vaccine" in frontier["matched_modalities"]
    assert result.moonshot_gate["weighted_score"] >= 0.8


def test_candidate_contribution_database_url_prefers_neon():
    assert (
        candidate_contribution_intake.candidate_contribution_database_url(
            {
                "NEON_DATABASE_URL": "postgresql://neon",
                "HSA_DATABASE_URL": "postgresql://hsa",
            }
        )
        == "postgresql://neon"
    )
    assert (
        candidate_contribution_intake.candidate_contribution_database_url(
            {
                "HSA_DATABASE_URL": "postgresql://hsa",
            }
        )
        == "postgresql://hsa"
    )


def test_candidate_contribution_intake_report_summarizes_rows():
    report = candidate_contribution_intake.build_candidate_contribution_intake_report_from_rows(
        [
            {
                "contribution_id": "11111111-1111-1111-1111-111111111111",
                "candidate_id": "twog-candidate-447eb8089965",
                "display_id": "TWOG-15F50D",
                "snapshot_content_hash": "abc123",
                "source_payload_url": "/api/public-candidates/twog-candidate-447eb8089965",
                "status": "queued_for_intake",
                "contribution_type": "evidence",
                "relation_to_current_record": "extends",
                "requested_system_action": "evidence_review",
                "contributor": {"contact": "reviewer@example.com"},
                "evidence": [{"url": "https://example.com/paper"}],
                "artifacts": [],
                "packet": {
                    "workspace_id": "025250d3-5982-4e77-84e7-b7e7d75157b2",
                    "checkout_manifest_hash": "sha256:manifest",
                },
                "created_at": datetime(2026, 5, 19, tzinfo=UTC),
            },
            {
                "contribution_id": "22222222-2222-2222-2222-222222222222",
                "candidate_id": "twog-candidate-447eb8089965",
                "display_id": "TWOG-15F50D",
                "status": "queued_for_intake",
                "contribution_type": "critique",
                "relation_to_current_record": "extends",
                "requested_system_action": "no_action",
                "contributor": {"contact": "smoke@example.com"},
                "evidence": [],
                "artifacts": [],
            },
        ],
        statuses=["queued_for_intake"],
        limit=25,
    )

    assert report["summary"]["row_count"] == 2
    assert report["summary"]["queued_for_intake"] == 2
    assert report["summary"]["actionable_count"] == 1
    assert report["requested_action_counts"] == {"evidence_review": 1, "no_action": 1}
    assert report["recommended_route_counts"]["accepted_for_evidence_review"] == 1
    assert report["rows"][0]["created_at"] == "2026-05-19T00:00:00+00:00"
    assert report["rows"][0]["workspace_id"] == "025250d3-5982-4e77-84e7-b7e7d75157b2"
    assert report["rows"][0]["checkout_manifest_hash"] == "sha256:manifest"


def test_candidate_contribution_triage_plan_routes_accepted_rows():
    contribution_id = "11111111-1111-1111-1111-111111111111"
    report = candidate_contribution_intake.build_candidate_contribution_triage_plan_from_rows(
        [
            {
                "contribution_id": contribution_id,
                "candidate_id": "twog-candidate-447eb8089965",
                "display_id": "TWOG-15F50D",
                "status": "queued_for_intake",
                "packet": {
                    "workspace_id": "025250d3-5982-4e77-84e7-b7e7d75157b2",
                    "checkout_manifest_hash": "sha256:manifest",
                },
                "review_notes": None,
                "promoted_queue_id": None,
            }
        ],
        contribution_ids=[contribution_id],
        action="accept_for_evidence_review",
        operator="operator@example.com",
        review_notes="Good citation repair lead.",
        dry_run=True,
        timestamp=datetime(2026, 5, 19, 12, 0, tzinfo=UTC),
    )

    assert report["dry_run"] is True
    assert report["target_status"] == "accepted_for_evidence_review"
    assert report["summary"] == {
        "requested_count": 1,
        "selected_count": 1,
        "missing_count": 0,
        "updated_count": 0,
    }
    assert report["rows"][0]["old_status"] == "queued_for_intake"
    assert report["rows"][0]["new_status"] == "accepted_for_evidence_review"
    assert report["rows"][0]["workspace_id"] == "025250d3-5982-4e77-84e7-b7e7d75157b2"
    assert report["rows"][0]["checkout_manifest_hash"] == "sha256:manifest"
    assert report["rows"][0]["promoted_queue_id"] == (
        f"candidate_contribution:{contribution_id}:evidence_review"
    )
    assert "Good citation repair lead." in report["rows"][0]["review_notes"]


def test_candidate_contribution_triage_plan_flags_missing_ids():
    report = candidate_contribution_intake.build_candidate_contribution_triage_plan_from_rows(
        [],
        contribution_ids=["missing-id"],
        action="reject",
        operator="operator",
        review_notes="Not enough evidence.",
        dry_run=False,
    )

    assert report["summary"]["requested_count"] == 1
    assert report["summary"]["selected_count"] == 0
    assert report["summary"]["missing_count"] == 1
    assert report["summary"]["updated_count"] == 0
    assert report["missing_contribution_ids"] == ["missing-id"]


def test_candidate_contribution_triage_rejects_invalid_action():
    with pytest.raises(ValueError, match="action must be one of"):
        candidate_contribution_intake.build_candidate_contribution_triage_plan_from_rows(
            [],
            contribution_ids=["11111111-1111-1111-1111-111111111111"],
            action="auto_approve_everything",
            operator="operator",
        )

    report = candidate_contribution_intake.triage_candidate_contributions(
        database_url=None,
        contribution_ids=["11111111-1111-1111-1111-111111111111"],
        action="auto_approve_everything",
        operator="operator",
    )
    assert report["errors"]


def test_command_center_web_candidate_contribution_payloads(monkeypatch):
    captured_report = {}

    def fake_report(**kwargs):
        captured_report.update(kwargs)
        return {"summary": {"row_count": 1}, "rows": [{"contribution_id": "abc"}], "errors": []}

    monkeypatch.setattr(candidate_contribution_intake, "build_candidate_contribution_intake_report", fake_report)

    report = command_center_web.list_candidate_contributions_payload(
        object(),
        {
            "status": ["queued_for_intake,triage_in_progress"],
            "candidate_id": ["twog-candidate-1"],
            "include_packet": ["true"],
            "limit": ["7"],
        },
    )

    assert report["summary"]["row_count"] == 1
    assert captured_report == {
        "statuses": ["queued_for_intake", "triage_in_progress"],
        "candidate_ids": ["twog-candidate-1"],
        "limit": 7,
        "include_packet": True,
    }

    captured_triage = {}

    def fake_triage(**kwargs):
        captured_triage.update(kwargs)
        return {"dry_run": kwargs["dry_run"], "action": kwargs["action"], "summary": {"updated_count": 1}}

    monkeypatch.setattr(candidate_contribution_intake, "triage_candidate_contributions", fake_triage)

    triage = command_center_web.triage_candidate_contribution_payload(
        object(),
        "abc",
        {
            "action": "accept_for_evidence_review",
            "operator": "reviewer",
            "review_notes": "Looks useful.",
            "dry_run": "false",
        },
    )

    assert triage["dry_run"] is False
    assert captured_triage == {
        "contribution_ids": ["abc"],
        "action": "accept_for_evidence_review",
        "operator": "reviewer",
        "review_notes": "Looks useful.",
        "dry_run": False,
    }

    with pytest.raises(ValueError, match="action is required"):
        command_center_web.triage_candidate_contribution_payload(object(), "abc", {})


def test_pmc_oa_v2_fetch_caps_candidate_metadata_scans(monkeypatch):
    metadata_calls = []

    def fake_get_json(url, params):
        assert url.endswith("/esearch.fcgi")
        assert params["retmax"] == 4
        return {"esearchresult": {"idlist": [str(index) for index in range(1, 20)]}}

    def fake_metadata(pmcid, **kwargs):
        metadata_calls.append((pmcid, kwargs))
        return None

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)
    monkeypatch.setattr(harvesters_v2, "_pmc_oa_metadata", fake_metadata)

    records = PMCOAHarvesterV2().fetch(
        "hemangiosarcoma",
        limit=3,
        max_candidate_records=4,
    )

    assert records == []
    assert [pmcid for pmcid, _kwargs in metadata_calls] == ["PMC1", "PMC2", "PMC3", "PMC4"]
    assert all(
        kwargs["timeout_seconds"] == harvesters_v2.FULL_TEXT_REQUEST_TIMEOUT_SECONDS
        and kwargs["attempts"] == harvesters_v2.FULL_TEXT_REQUEST_ATTEMPTS
        for _pmcid, kwargs in metadata_calls
    )


def test_twitterapi_io_provider_searches_and_normalizes_candidates():
    calls = []

    def fake_transport(url, params, headers, timeout_seconds):
        calls.append((url, params, headers, timeout_seconds))
        return {
            "tweets": [
                {
                    "id": "123",
                    "url": "https://x.com/vetonc/status/123",
                    "text": "New canine hemangiosarcoma trial links to PubMed.",
                    "createdAt": "2026-04-28T10:00:00Z",
                    "lang": "en",
                    "conversationId": "789",
                    "retweetCount": 1,
                    "replyCount": 2,
                    "likeCount": 3,
                    "quoteCount": 4,
                    "viewCount": 500,
                    "author": {"id": "456", "userName": "vetonc", "name": "Vet Onc"},
                    "entities": {
                        "urls": [
                            {"expanded_url": "https://pubmed.ncbi.nlm.nih.gov/123456/"},
                        ]
                    },
                }
            ],
            "has_next_page": False,
            "next_cursor": "",
        }

    result = x_topic_monitor.TwitterApiIoProvider(
        api_key="test-key",
        transport=fake_transport,
        timeout_seconds=12.0,
    ).search(
        x_topic_monitor.XTopicRequest(
            query='"canine hemangiosarcoma"',
            query_name="x_trial_monitoring",
            max_results=10,
        )
    )

    assert calls == [
        (
            "https://api.twitterapi.io/twitter/tweet/advanced_search",
            {"query": '"canine hemangiosarcoma" lang:en -filter:retweets', "queryType": "Latest", "cursor": ""},
            {"x-api-key": "test-key"},
            12.0,
        )
    ]
    assert result.provider == "twitterapi_io"
    assert result.raw_tweet_count == 1
    assert len(result.candidates) == 1
    assert result.candidates[0].canonical_url == "https://x.com/vetonc/status/123"
    assert result.candidates[0].username == "vetonc"
    assert result.candidates[0].durable_links == ["https://pubmed.ncbi.nlm.nih.gov/123456/"]
    assert result.candidates[0].metadata["provider_payload"]["provider"] == "twitterapi_io"


def test_avma_vctr_claim_extractor_requires_primary_hsa_scope():
    extractor = LocalRuleClaimExtractor()
    melanoma = ResearchObject(
        object_type="veterinary_trial",
        title="The effect of a novel mushroom formula on canine oral malignant melanoma",
        abstract=(
            "This canine melanoma study evaluates immune activity and angiogenesis. "
            "A cited hemangiosarcoma paper informed dose selection."
        ),
        source_key="avma_vctr",
        metadata={"conditions": ["Melanoma"]},
    )
    melanoma_chunk = DocumentChunk(
        research_object_id=melanoma.id,
        chunk_index=0,
        section_label="veterinary_trial_record",
        text_content="Dogs receive an immune supplement. Angiogenesis and macrophage activity are monitored.",
        content_hash="melanoma",
    )
    hsa = ResearchObject(
        object_type="veterinary_trial",
        title="Combination therapy for dogs with hemangiosarcoma",
        abstract="Dogs with splenic hemangiosarcoma receive doxorubicin.",
        source_key="avma_vctr",
        metadata={"conditions": ["Hemangiosarcoma"]},
    )
    hsa_chunk = DocumentChunk(
        research_object_id=hsa.id,
        chunk_index=0,
        section_label="veterinary_trial_record",
        text_content="Dogs with hemangiosarcoma receive doxorubicin chemotherapy in this trial.",
        content_hash="hsa",
    )

    assert extractor.extract_chunk(melanoma_chunk, melanoma) == []
    claims = extractor.extract_chunk(hsa_chunk, hsa)
    assert any(claim.statement.startswith("doxorubicin is discussed") for claim in claims)


def test_local_claim_extractor_creates_draft_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    papers_path = tmp_path / "papers.json"
    papers_path.write_text(
        """
        [
          {
            "pmid": "123",
            "title": "Propranolol and VEGF in canine hemangiosarcoma",
            "abstract": "Canine hemangiosarcoma studies discuss propranolol with VEGF and angiogenesis.",
            "journal": "Example Journal",
            "year": "2026",
            "source": "pubmed"
          }
        ]
        """
    )
    backfill_papers_json(repo, papers_path)

    result = extract_claims_for_repository(repo, source_key="current_papers")
    claims = repo.search_claims(
        ClaimSearchRequest(query="propranolol", species="canine", min_confidence=0.1, include_drafts=True)
    )

    assert result.chunks_seen == 1
    assert result.claims_written >= 1
    assert any(claim.metadata.get("extraction_status") == "draft" for claim in claims)


def test_local_claim_extractor_handles_human_angiosarcoma_analogs(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    title = "Paclitaxel targets VEGF signaling in human angiosarcoma"
    obj_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title=title,
            abstract="Human angiosarcoma studies discuss paclitaxel with VEGF and angiogenesis.",
            source_key="pubmed",
        )
    )
    for chunk in chunk_text(obj_id, title, section_label="title_abstract"):
        repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, source_key="pubmed")
    claims = repo.search_claims(
        ClaimSearchRequest(query="paclitaxel", species="human", min_confidence=0.1, include_drafts=True)
    )

    assert result.claims_written >= 1
    assert any(claim.metadata.get("context_key") == "human_angiosarcoma_analog" for claim in claims)


def test_local_claim_extractor_creates_sparse_scholarly_context_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    europe_pmc_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Primary vaginal angiosarcoma case report",
            source_key="europe_pmc",
        )
    )
    crossref_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Vascular sarcoma clinical series",
            source_key="crossref",
        )
    )
    for object_id, text in (
        (europe_pmc_id, "Primary vaginal angiosarcoma case report."),
        (crossref_id, "Vascular sarcoma clinical series."),
    ):
        for chunk in chunk_text(object_id, text, section_label="title_abstract"):
            repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, limit=10)
    claims = repo.search_claims(ClaimSearchRequest(query="source context", min_confidence=0.1, include_drafts=True, limit=10))
    statements = [claim.statement for claim in claims]

    assert result.claims_written == 2
    assert any("Europe PMC record provides human angiosarcoma" in statement for statement in statements)
    assert any("Crossref record provides human angiosarcoma" in statement for statement in statements)


def test_local_claim_extractor_creates_structured_chembl_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    title = "TOCERANIB IC50 against Vascular endothelial growth factor receptor 2"
    obj_id = repo.upsert_research_object(
        ResearchObject(
            object_type="bioactivity_assay",
            title=title,
            abstract="Inhibition of Vascular endothelial growth factor receptor 2.",
            source_key="chembl",
            identifiers={"chembl_activity_id": "726668", "chembl_target_id": "CHEMBL279"},
            metadata={
                "query_term": "toceranib",
                "molecule_pref_name": "TOCERANIB",
                "target_pref_name": "Vascular endothelial growth factor receptor 2",
                "target_gene": "KDR",
                "target_category": "vegf_angiogenesis",
                "target_organism": "Homo sapiens",
                "assay_type": "B",
                "standard_type": "IC50",
                "standard_relation": "=",
                "standard_value": "60.0",
                "standard_units": "nM",
                "pchembl_value": "7.22",
                "pchembl_numeric": 7.22,
            },
        )
    )
    for chunk in chunk_text(obj_id, title, section_label="bioactivity_assay"):
        repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, source_key="chembl")
    claims = repo.search_claims(
        ClaimSearchRequest(query="toceranib", species="human", min_confidence=0.1, include_drafts=True)
    )

    assert result.chunks_seen == 1
    assert result.claims_written == 1
    assert claims[0].claim_type == "compound_modulates_target"
    assert claims[0].evidence_level == "in_vitro"
    assert claims[0].metadata["context_key"] == "chembl_target_bioactivity"
    assert "pChEMBL 7.22" in claims[0].statement


# --- Phase 1: validation-ready candidate gate + stable snapshot hash ---

def _seed_validation_ready_candidate(repo, candidate_id="vr-candidate", *, ready=True):
    """Seed a candidate + snapshot + (optionally validation_ready) decision."""
    snapshot = PublicCandidateSnapshot(
        candidate_id=candidate_id,
        content_hash="a" * 40,
        title="Vimentin peptide strategy",
        public_status="evidence_supported",
        snapshot_version=1,
    )
    repo.upsert_public_candidate_snapshot(snapshot)
    repo.upsert_public_candidate(
        PublicCandidateRecord(
            candidate_id=candidate_id,
            title="Vimentin peptide strategy",
            public_status="evidence_supported" if ready else "proposed",
            evidence_refs=["PMID:123"] if ready else [],
            content_hash="a" * 40,
            latest_snapshot_id=snapshot.snapshot_id,
        )
    )
    decision = ValidationDecisionPacket(
        decision_id=f"validation_decision:{candidate_id}",
        packet_id=f"validation_packet:{candidate_id}",
        candidate_id=candidate_id,
        source_type="therapy_idea",
        source_id=candidate_id,
        title="Public candidate decision",
        outcome="promote_broader_program",
        confidence=0.72,
        validation_ready=ready,
        specific_claim_viability="uncertain",
        broader_program_signal="strong",
        rationale="The broader VIM peptide program has enough signal for external validation.",
        recommended_downstream_action="Create an inspectable candidate record; validation recommend-only.",
        decisive_questions=["Does VIM expression enrich in canine HSA cohorts?"],
        evidence_tasks=["Attach processed omics readout and assay strategy."],
        evidence_summary={"evidence_refs": ["PMID:123"]},
    )
    repo.upsert_validation_decision(ValidationDecisionRecord.from_decision(decision))
    return candidate_id


def test_validation_ready_gate_passes_and_persists(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "vr-ready.sqlite3", seed=False)
    service = HSAResearchService(repo)
    candidate_id = _seed_validation_ready_candidate(repo, ready=True)

    readiness = service.assess_candidate_validation_readiness(candidate_id)
    assert readiness is not None
    assert readiness.ready is True
    assert readiness.blockers == []
    assert readiness.snapshot_hash == "a" * 40
    # open questions derived from the decision's decisive questions + evidence tasks
    assert "Does VIM expression enrich in canine HSA cohorts?" in readiness.open_questions
    assert "Attach processed omics readout and assay strategy." in readiness.open_questions

    # persisted onto the candidate
    persisted = repo.get_public_candidate(candidate_id)
    assert persisted.validation_ready is True
    assert persisted.validation_ready_at is not None
    assert persisted.validation_ready_snapshot_hash == "a" * 40
    assert persisted.validation_ready_open_questions == readiness.open_questions

    # audit event appended
    events = repo.list_public_candidate_decision_events(candidate_id=candidate_id)
    assert any(event.action == "validation_ready_assessed" for event in events)


def test_validation_ready_gate_blocks_unready_candidate(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "vr-blocked.sqlite3", seed=False)
    service = HSAResearchService(repo)
    candidate_id = _seed_validation_ready_candidate(repo, ready=False)

    readiness = service.assess_candidate_validation_readiness(candidate_id)
    assert readiness is not None
    assert readiness.ready is False
    assert "no_validation_ready_decision" in readiness.blockers
    assert "missing_evidence_bundle" in readiness.blockers

    persisted = repo.get_public_candidate(candidate_id)
    assert persisted.validation_ready is False
    assert persisted.validation_ready_at is None
    assert persisted.validation_ready_blockers


def test_validation_ready_gate_returns_none_for_missing_candidate(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "vr-missing.sqlite3", seed=False)
    service = HSAResearchService(repo)
    assert service.assess_candidate_validation_readiness("does-not-exist") is None


def test_checkout_manifest_requires_validation_ready(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "vr-checkout.sqlite3", seed=False)
    service = HSAResearchService(repo)
    candidate_id = _seed_validation_ready_candidate(repo, ready=True)

    # unready: gate not yet assessed -> checkout blocked
    blocked = service.build_research_workspace_checkout_manifest(
        ResearchWorkspaceCheckoutManifestRequest(
            candidate_id=candidate_id,
            require_validation_ready=True,
            persist_to_workspace=False,
        )
    )
    assert any("candidate_not_validation_ready" in error for error in blocked.errors)

    # assess -> ready, then checkout succeeds and auto-fills snapshot hash + open questions
    service.assess_candidate_validation_readiness(candidate_id)
    allowed = service.build_research_workspace_checkout_manifest(
        ResearchWorkspaceCheckoutManifestRequest(
            candidate_id=candidate_id,
            require_validation_ready=True,
            persist_to_workspace=False,
        )
    )
    assert not any("candidate_not_validation_ready" in error for error in allowed.errors)
    assert allowed.manifest is not None
    assert allowed.manifest.candidate_snapshot_hash == "a" * 40
    assert "Does VIM expression enrich in canine HSA cohorts?" in allowed.manifest.open_questions


def test_public_candidate_snapshot_content_hash_is_stable_across_regeneration(tmp_path):
    """The snapshot content_hash must be reproducible for identical inputs — the proof-capsule
    chain (workspace.candidate_snapshot_hash) depends on it not drifting."""
    repo = SQLiteResearchRepository(tmp_path / "vr-hash-stable.sqlite3", seed=False)
    service = HSAResearchService(repo)
    idea = TherapyIdea(
        title="Vimentin peptide blockade strategy",
        hypothesis="A vimentin-directed peptide may disrupt vascular HSA invasion programs.",
        rationale="VIM sits at a plausible tumor ecology interface and needs inspectable validation.",
        candidate_therapies=["vimentin-targeting peptide"],
        targets=["VIM"],
        biomarkers=["VIM expression"],
        evidence_refs=["PMID:123", "PMID:456"],
        evidence_strength="medium",
        risks=["direct canine peptide evidence remains sparse"],
        next_experiments=["Run processed omics VIM expression readout."],
        priority_score=0.82,
    )
    repo.upsert_therapy_idea(
        TherapyIdeaRecord(
            idea=idea,
            source_brief_id=uuid4(),
            topic="VIM peptide program",
            status="ready_for_promotion",
            score=0.82,
        )
    )
    request = PublicCandidateGenerateRequest(
        therapy_idea_id=idea.idea_id,
        require_moonshot_grade=False,
        pipeline_version="test-v1",
        commit_sha="abc123",
    )
    first = service.generate_public_candidate_snapshot(request)
    second = service.generate_public_candidate_snapshot(request)
    assert first.snapshot is not None and second.snapshot is not None
    # version increments, but the content hash is identical for identical inputs
    assert first.snapshot.content_hash == second.snapshot.content_hash
    assert len(first.snapshot.content_hash) == 64  # sha256 hex
