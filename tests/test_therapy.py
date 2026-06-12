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

def test_therapy_ideas_round_trip_and_committee_from_brief(tmp_path):
    for repo in [
        InMemoryResearchRepository(),
        SQLiteResearchRepository(tmp_path / "therapy-ideas.sqlite3", seed=False),
    ]:
        service = HSAResearchService(repo)
        brief, evaluation = _seed_evaluated_brief(repo, duplicate_count=0)
        result = service.run_therapy_committee(
            TherapyCommitteeRequest(
                brief_id=brief.brief_id,
                evaluation_id=evaluation.evaluation_id,
                review_mode="deterministic_only",
                max_claims=0,
            )
        )
        ideas = service.list_therapy_ideas(
            TherapyIdeaLibraryRequest(source_brief_id=brief.brief_id, limit=20)
        )

        assert result.source_brief_id == brief.brief_id
        assert result.source_evaluation_id == evaluation.evaluation_id
        assert result.ranked_ideas
        assert ideas.idea_count == len(result.ranked_ideas)
        assert ideas.ideas[0].source_brief_id == brief.brief_id
        assert repo.get_therapy_idea(ideas.ideas[0].therapy_idea_id) is not None


def test_therapy_committee_from_research_program_persists_three_linked_ideas(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "program-therapy-ideas.sqlite3", seed=False)
    _seed_program_committee_corpus(repo)
    program = repo.upsert_research_program(_ready_for_therapy_ideas_program())
    result = HSAResearchService(repo).run_therapy_committee(
        TherapyCommitteeRequest(
            program_id=program.program_id,
            review_mode="deterministic_only",
            max_claims=0,
            max_ideas_per_perspective=3,
        )
    )
    ideas = HSAResearchService(repo).list_therapy_ideas(
        TherapyIdeaLibraryRequest(source_program_id=program.program_id, limit=20)
    )

    assert result.source_program_id == program.program_id
    assert result.evidence["research_program"]["program_id"] == str(program.program_id)
    assert len(result.ranked_ideas) == 3
    assert ideas.idea_count == 3
    assert {idea.source_program_id for idea in ideas.ideas} == {program.program_id}
    assert all(idea.source_brief_id is None for idea in ideas.ideas)


def test_therapy_committee_blocks_missing_or_unready_research_program(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "blocked-program-therapy-ideas.sqlite3", seed=False)
    missing_result = HSAResearchService(repo).run_therapy_committee(
        TherapyCommitteeRequest(program_id=uuid4(), review_mode="deterministic_only")
    )
    unready = repo.upsert_research_program(_research_program_fixture())
    unready_result = HSAResearchService(repo).run_therapy_committee(
        TherapyCommitteeRequest(program_id=unready.program_id, review_mode="deterministic_only")
    )

    assert not missing_result.ranked_ideas
    assert any("Research program not found" in error for error in missing_result.errors)
    assert not unready_result.ranked_ideas
    assert any("not ready for therapy ideas" in error for error in unready_result.errors)
    assert HSAResearchService(repo).list_therapy_ideas(
        TherapyIdeaLibraryRequest(source_program_id=unready.program_id)
    ).idea_count == 0


def test_evidence_fit_treats_multi_therapy_terms_as_alternatives(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "evidence-fit-therapy-alternatives.sqlite3", seed=False)
    fetch_run_id = repo.create_fetch_run("europe_pmc", "pazopanib-canine-hsa")
    raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="europe_pmc",
            source_record_id="pazopanib-canine-hsa",
            content_hash="pazopanib-canine-hsa",
            raw_payload={"source_id": "pazopanib-canine-hsa"},
        ),
        fetch_run_id=fetch_run_id,
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type=ResearchObjectType.PUBLICATION,
            title="Pazopanib safety evidence in canine hemangiosarcoma and human angiosarcoma",
            abstract=(
                "Pazopanib was discussed as a VEGFR inhibitor with safety and tolerability "
                "context for canine hemangiosarcoma and human angiosarcoma."
            ),
            source_key="europe_pmc",
            dedupe_key="europe_pmc:pazopanib-canine-hsa",
            identifiers={"source_id": "pazopanib-canine-hsa"},
            raw_record_id=raw_id,
        ),
        raw_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Pazopanib safety and tolerability were discussed for canine hemangiosarcoma "
                "and human angiosarcoma comparative oncology evidence."
            ),
            content_hash="pazopanib-canine-hsa-chunk",
        )
    )
    lead = ResearchLeadRecord(
        title="Repair duplicate citations for pazopanib, sorafenib, and propranolol in canine HSA",
        status="followup",
        topic_tags=["pazopanib", "sorafenib", "propranolol", "canine", "hemangiosarcoma", "safety"],
    )
    query = SourceQuery(
        source_key="europe_pmc",
        query_name="pazopanib-canine-hsa",
        query_text="pazopanib canine safety hemangiosarcoma angiosarcoma",
        query_params={
            "required_terms": [
                "pazopanib",
                "sorafenib",
                "propranolol",
                "canine",
                "safety",
                "hemangiosarcoma",
                "angiosarcoma",
            ]
        },
        track="validation_gap",
    )
    ingest_result = ValidationGapSourceIngestResult(
        dry_run=False,
        source_keys=["europe_pmc"],
        query_count=1,
        attempted_query_count=1,
        completed_query_count=1,
        raw_records=1,
        research_objects=1,
        document_chunks=1,
        source_queries=[query],
        results=[
            IngestionResult(
                source_key="europe_pmc",
                query_name="pazopanib-canine-hsa",
                query_text=query.query_text,
                fetch_run_id=fetch_run_id,
                raw_records=1,
                research_objects=1,
                document_chunks=1,
                status=RunStatus.COMPLETED,
            )
        ],
    )

    assessment = evidence_fit.assess_research_followup_ingest_evidence_fit(repo, lead, ingest_result)

    assert assessment.fit == "strong"
    assert "pazopanib" in assessment.matched_terms
    assert "sorafenib" in assessment.missing_terms
    assert "propranolol" in assessment.missing_terms


def test_research_followup_refinement_splits_multi_therapy_repair_queries(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-refinement-multi-therapy.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Repair duplicate citations: pazopanib sorafenib propranolol canine HSA angiosarcoma",
            status="followup",
            priority=20,
            source_key="pubmed",
            origin_source_key="research_brief_quality",
            reason=(
                "Retrieve primary human angiosarcoma and canine hemangiosarcoma data for "
                "pazopanib, sorafenib, and propranolol without bundling therapies together."
            ),
            suggested_sources=["pubmed", "europe_pmc"],
            topic_tags=["citation_dedupe_repair", "canine_hemangiosarcoma"],
            metadata={
                "research_followup_queue": {
                    "followup_kind": "citation_dedupe_repair",
                    "requires_manual_research": False,
                    "topic": (
                        "Pazopanib, sorafenib, and propranolol evidence in canine "
                        "hemangiosarcoma and human angiosarcoma"
                    ),
                }
            },
        )
    )

    result = service.refine_research_followups(
        ResearchFollowupRefinementRequest(
            lead_ids=[lead.lead_id],
            source_keys=["pubmed", "europe_pmc"],
            max_queries_per_review=10,
            operator="operator",
        )
    )
    queries = repo.list_source_queries(active_only=True)

    assert result.scanned_count == 1
    assert result.source_queries_created == 6
    assert result.query_count == 6
    for therapy in ("pazopanib", "sorafenib", "propranolol"):
        matching = [query for query in queries if therapy in query.query_text.lower()]
        assert len(matching) == 2
        assert {query.source_key for query in matching} == {"pubmed", "europe_pmc"}
        assert all(therapy in query.query_params["required_terms"] for query in matching)
        other_therapies = {"pazopanib", "sorafenib", "propranolol"} - {therapy}
        assert all(
            not any(other in query.query_text.lower() for other in other_therapies)
            for query in matching
        )
        assert all(
            not any(other in query.query_params["required_terms"] for other in other_therapies)
            for query in matching
        )


def test_model_review_summary_includes_therapy_committee_ideas_and_model_reviews():
    run = {
        "agent_run_id": "run-therapy",
        "agent_name": "therapy_committee_chair_agent",
        "status": "completed",
        "source_key": None,
        "partition_date": None,
        "completed_at": "2026-05-04T01:19:36Z",
        "summary": {"idea_count": 1, "top_idea": "PD-1 plus VEGFR2"},
        "output_payload": {
            "committee_run_id": "committee-1",
            "decision_summary": "Top recommend-only idea: PD-1 plus VEGFR2.",
            "errors": [],
            "ranked_ideas": [
                {
                    "idea_id": "idea-1",
                    "title": "PD-1 plus VEGFR2",
                    "priority_score": 0.82,
                    "evidence_strength": "low",
                }
            ],
            "reports": [
                {
                    "perspective": "target_biology",
                    "evidence": {
                        "model_review": {
                            "requested_model": "anthropic/claude-sonnet-4.6",
                            "model_name": "anthropic/claude-sonnet-4.6",
                            "json_repair_attempted": True,
                            "usage": {"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": 30},
                            "original_review": {"model_name": "anthropic/claude-sonnet-4.6"},
                        }
                    },
                }
            ],
        },
    }

    summary = cli_module._model_review_summary(run)

    assert summary["committee_run_id"] == "committee-1"
    assert summary["idea_count"] == 1
    assert summary["top_ideas"][0]["title"] == "PD-1 plus VEGFR2"
    assert summary["model_reviews"][0]["perspective"] == "target_biology"
    assert summary["model_reviews"][0]["json_repair_attempted"] is True
    assert summary["model_reviews"][0]["usage"]["total_tokens"] == 30


def test_research_followup_resolver_preserves_hyphenated_therapy_query_terms():
    lead = ResearchLeadRecord(
        title="Safety signal: Anti-PD-1 monotherapy efficacy and safety data in canine HSA",
        summary="Need canine CA-4F12-E6 anti-PD-1 safety and tolerability evidence.",
        lead_type="unknown",
        status="watching",
        source_key="pubmed",
        topic_tags=["canine", "hemangiosarcoma"],
    )

    query = research_followup_resolver._lead_search_query(lead, max_terms=12)

    assert "ca-4f12-e6" in query
    assert "anti-pd-1" in query
    assert "pd-1" in query
    assert "canine" in query
    assert "hemangiosarcoma" in query
    assert " anti " not in f" {query} "


def test_therapy_committee_contract_rejects_invalid_priority_score():
    with pytest.raises(ValidationError):
        TherapyIdea(
            title="Invalid idea",
            hypothesis="Priority score is outside the allowed range.",
            rationale="The contract should reject malformed committee output.",
            evidence_refs=["C1"],
            priority_score=1.5,
        )


def test_therapy_committee_load_json_object_repairs_model_formatting():
    payload = therapy_committee._load_json_object(
        """
        Model output:
        ```json
        {
          "summary": "Parsed after local cleanup"
          "ideas": [],
          "evidence_limitations": [],
          "errors": [],
        }
        ```
        """
    )

    assert payload["summary"] == "Parsed after local cleanup"
    assert payload["ideas"] == []


def test_therapy_committee_openrouter_perspective_repairs_invalid_model_json(monkeypatch):
    review_calls = []
    repair_calls = []

    def fake_review_model(model_name, review_payload):
        review_calls.append((model_name, review_payload))
        return {
            "text": '{"summary": "truncated", "ideas": [',
            "metadata": {"model_name": model_name, "request_id": "review-1"},
        }

    def fake_repair_model(model_name, malformed_text, *, parse_error, original_metadata=None):
        repair_calls.append((model_name, malformed_text, parse_error, original_metadata))
        return {
            "text": json.dumps(
                {
                    "summary": "Repaired committee JSON",
                    "ideas": [
                        {
                            "title": "KDR-gated kinase validation",
                            "hypothesis": "KDR-positive HSA warrants a cited kinase validation pass.",
                            "rationale": "The cited evidence links vascular signaling with the disease context.",
                            "candidate_therapies": ["toceranib"],
                            "targets": ["KDR"],
                            "biomarkers": ["KDR"],
                            "mechanism": "VEGFR signaling blockade may reduce vascular tumor signaling.",
                            "evidence_refs": ["C1"],
                            "evidence_strength": "low",
                            "translational_path": "Start with ex vivo or cell-model validation.",
                            "risks": ["Evidence remains indirect."],
                            "next_experiments": ["Run a KDR/phospho-VEGFR readout."],
                            "priority_score": 0.71,
                        }
                    ],
                    "evidence_limitations": ["Repair preserved model content after syntax failure."],
                    "errors": [],
                }
            ),
            "metadata": {
                "model_name": model_name,
                "request_id": "repair-1",
                "json_repair_attempted": True,
            },
        }

    monkeypatch.setattr(therapy_committee, "_openrouter_review_model", fake_review_model)
    monkeypatch.setattr(therapy_committee, "_openrouter_repair_json_model", fake_repair_model)

    citation = ResearchBriefCitation(
        citation_id="C1",
        chunk_id=uuid4(),
        research_object_id=uuid4(),
        source_key="pubmed",
        title="KDR therapy signal",
        quote="Canine hemangiosarcoma evidence discusses KDR and vascular tumor signaling.",
    )
    report = therapy_committee._run_openrouter_perspective(
        TherapyCommitteeRequest(
            topic="KDR therapy ideas for canine hemangiosarcoma",
            review_mode="openrouter_required",
            review_models=["test/model"],
            max_ideas_per_perspective=1,
        ),
        "target_biology",
        {"citations": [citation], "claims": [], "research_leads": [], "search_queries": {}, "errors": []},
    )

    assert len(review_calls) == 1
    assert len(repair_calls) == 1
    assert repair_calls[0][3]["request_id"] == "review-1"
    assert report.summary == "Repaired committee JSON"
    assert report.ideas[0].title == "KDR-gated kinase validation"
    assert report.evidence["model_review"]["json_repair_attempted"] is True


def test_therapy_committee_ranking_dedupes_same_therapy_family():
    ideas = [
        TherapyIdea(
            title="Biomarker-gated sorafenib for VEGFR-2-high HSA",
            hypothesis="Sorafenib may help VEGFR-2-high canine HSA.",
            rationale="The idea depends on VEGFR-2 expression.",
            candidate_therapies=["sorafenib"],
            targets=["VEGFR-2", "PDGFR-beta"],
            evidence_refs=["C1"],
            priority_score=0.74,
        ),
        TherapyIdea(
            title="Sorafenib kinase-selectivity de-risking before translation",
            hypothesis="Sorafenib should be tested against VEGFR-2 and PDGFR-beta before translation.",
            rationale="This is the same VEGFR TKI family with a stronger de-risking plan.",
            candidate_therapies=["sorafenib", "toceranib"],
            targets=["KDR", "PDGFR-\u03b2"],
            evidence_refs=["C1"],
            priority_score=0.79,
        ),
        TherapyIdea(
            title="PIK3CA-mutant HSA pathway inhibition",
            hypothesis="PIK3CA-mutant HSA may expose a PI3K-AKT-mTOR vulnerability.",
            rationale="This is a distinct pathway family.",
            candidate_therapies=["alpelisib"],
            targets=["PIK3CA", "PTEN", "mTOR"],
            evidence_refs=["C2"],
            priority_score=0.52,
        ),
    ]

    ranked = therapy_committee._rank_ideas(ideas)

    assert [idea.title for idea in ranked] == [
        "Sorafenib kinase-selectivity de-risking before translation",
        "PIK3CA-mutant HSA pathway inhibition",
    ]


def test_therapy_committee_runs_cited_idea_layer(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "therapy-committee.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:therapy",
            content_hash="therapy-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/therapy/",
            raw_payload={"pmid": "therapy"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="KDR VEGFA and mTOR therapy in canine hemangiosarcoma",
            abstract=(
                "Canine hemangiosarcoma and human angiosarcoma share vascular biology. "
                "KDR, VEGFA, MTOR, CD31, propranolol, sirolimus, and paclitaxel are discussed."
            ),
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/therapy/",
            dedupe_key="pmid:therapy",
            identifiers={"pmid": "therapy"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma translational therapy evidence discusses KDR, VEGFA, "
                "MTOR, CD31, propranolol, sirolimus, paclitaxel, toxicity, target selection, "
                "biomarker readouts, and human angiosarcoma analog evidence."
            ),
            content_hash="therapy-chunk",
        )
    )

    result = HSAResearchService(repo).run_therapy_committee(
        TherapyCommitteeRequest(
            topic="KDR VEGFA mTOR therapy ideas for canine hemangiosarcoma",
            max_chunks_per_perspective=3,
            max_claims=0,
            review_mode="deterministic_only",
        )
    )

    assert isinstance(result, TherapyCommitteeResult)
    assert len(result.reports) == 5
    assert {report.perspective for report in result.reports} == {
        "target_biology",
        "drug_repurposing",
        "translational_clinical",
        "peptide_specialist",
        "skeptic_risk",
    }
    assert result.ranked_ideas
    assert result.ranked_ideas[0].evidence_refs
    assert result.evidence["citation_count"] >= 1
    agent_runs = repo.list_agent_runs(agent_name="therapy_committee_chair_agent", limit=10)
    assert agent_runs
    assert agent_runs[0].status == RunStatus.COMPLETED


def test_research_brief_synthesis_remaps_duplicate_citations_and_ranks_therapy_hypotheses():
    first_object_id = uuid4()
    second_object_id = uuid4()
    citation = ResearchBriefCitation(
        citation_id="C1",
        chunk_id=uuid4(),
        research_object_id=first_object_id,
        title="Shared vascular biology in hemangiosarcoma",
        quote="Canine and human vascular sarcoma biology has overlapping pathway signals.",
        relevance="translational_hypothesis:cross_species:biology",
        metadata={"identifiers": {"doi": "10.1234/hsa.therapy"}},
    )
    duplicate = ResearchBriefCitation(
        citation_id="C2",
        chunk_id=uuid4(),
        research_object_id=second_object_id,
        title="Toceranib monotherapy outcomes in canine hemangiosarcoma",
        quote="Toceranib monotherapy therapy evidence discusses dose, response, survival, and toxicity.",
        relevance="translational_hypothesis:comparative_model:therapy",
        metadata={"identifiers": {"doi": "https://doi.org/10.1234/HSA.THERAPY"}},
    )
    broad_finding = ResearchBriefFinding(
        claim="Shared vascular biology can motivate comparative pathway work.",
        stance="opportunity",
        citations=["C1"],
        evidence_strength="low",
        reasoning="The citation describes overlapping pathway signals.",
    )
    therapy_finding = ResearchBriefFinding(
        claim="Toceranib monotherapy should be prioritized as a therapy-specific validation hypothesis.",
        stance="opportunity",
        citations=["C2"],
        evidence_strength="medium",
        reasoning="The duplicate citation discusses therapy dose, response, survival, and toxicity.",
    )
    report = ResearchBriefPerspectiveReport(
        perspective="translational_hypothesis",
        agent_name="translational_hypothesis_agent",
        summary="Translational hypotheses were found.",
        findings=[broad_finding, therapy_finding],
        citations=[citation, duplicate],
    )
    evidence = research_brief_agent.ResearchBriefEvidenceBundle(
        citations=[citation, duplicate],
        claims=[],
        research_leads=[],
        search_queries={},
        errors=[],
    )

    result = research_brief_agent.ResearchBriefAgent(InMemoryResearchRepository()).synthesize(
        ResearchBriefRequest(
            topic="Toceranib monotherapy in canine hemangiosarcoma",
            review_mode="deterministic_only",
        ),
        [report],
        evidence=evidence,
    )

    assert len(result.citations) == 1
    assert result.citations[0].metadata["dedupe"]["duplicate_citation_ids"] == ["C2"]
    assert result.ranked_hypotheses[0].claim.startswith("Toceranib monotherapy")
    assert result.ranked_hypotheses[0].citations == ["C1"]
    assert result.ranked_hypotheses[0].metadata["citation_aliases"] == {"C2": "C1"}
    assert result.ranked_hypotheses[0].metadata["therapy_relevance_score"] > result.ranked_hypotheses[1].metadata[
        "therapy_relevance_score"
    ]
    assert "[C2]" not in result.final_brief


def test_research_brief_therapy_relevance_terms_filter_prompt_noise():
    citation = ResearchBriefCitation(
        citation_id="C1",
        chunk_id=uuid4(),
        research_object_id=uuid4(),
        title="VEGFR inhibitor response in vascular sarcoma",
        quote="Sorafenib VEGFR inhibitor evidence discusses response and survival.",
        relevance="therapy validation",
    )
    finding = ResearchBriefFinding(
        claim="Sorafenib and VEGFR response should be reviewed before validation.",
        stance="opportunity",
        citations=["C1"],
        evidence_strength="medium",
        reasoning="The signal is therapy-specific, not a generic citation traceability issue.",
    )

    _score, hits = research_brief_agent._therapy_relevance_score(
        finding,
        {"C1": citation},
        ResearchBriefRequest(
            topic=(
                "C21 content and traceability: C21 is cited in the negative evidence needs "
                "and is not included in the core evidence refs packet."
            ),
            disease_scope="canine hemangiosarcoma and human angiosarcoma",
        ),
    )

    assert {"sorafenib", "vegfr", "response"}.issubset(set(hits))
    assert not {
        "and",
        "but",
        "c21",
        "cited",
        "content",
        "core",
        "correlation",
        "findings",
        "for",
        "formally",
        "inconsistency",
        "resolve",
    } & set(hits)
