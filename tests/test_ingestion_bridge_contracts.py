import xml.etree.ElementTree as ET
from uuid import uuid4

from hsa_research.ingestion_bridge.contracts import (
    BoltzRunRequest,
    CandidateDossierRequest,
    ClaimDirection,
    ClaimSearchRequest,
    ClaimSearchResult,
    ClaimType,
    CommitHypothesisRequest,
    DocumentChunk,
    EvidenceLevel,
    HypothesisProposalRequest,
    RawSourceRecord,
    ResearchObject,
    ScrapeFetchRequest,
    ScrapeIngestRequest,
    ScrapeManifestFetchRequest,
    ScrapeManifestRequest,
    ScrapeProfileReviewRequest,
    ScrapeReviewRequest,
    ScrapeSourceProfile,
    ValidationRequest,
    ClaimCurationRequest,
    SourceScoutRequest,
)
from hsa_research.ingestion_bridge.backfill import backfill_deep_dives, backfill_papers_json
from hsa_research.ingestion_bridge.claim_curator import ClaimCuratorAgent
from hsa_research.ingestion_bridge.claim_extractor import LocalRuleClaimExtractor, extract_claims_for_repository
from hsa_research.ingestion_bridge.chunker import chunk_text
from hsa_research.ingestion_bridge.dagster_assets import (
    ALL_API_SMOKE_KEYS,
    HOSTED_API_REPORT_KEYS,
    LITERATURE_CLINICAL_SMOKE_KEYS,
)
from hsa_research.ingestion_bridge import harvesters_v2
from hsa_research.ingestion_bridge.harvesters_v2 import (
    AVMAVCTRHarvesterV2,
    ChEMBLHarvesterV2,
    ClinicalTrialsGovHarvesterV2,
    EuropePMCHarvesterV2,
    GEOHarvesterV2,
    HARVESTERS_V2,
    ICDCHarvesterV2,
    OpenAlexHarvesterV2,
    OpenFDAAnimalEventsHarvesterV2,
    PMCOAHarvesterV2,
    PubChemHarvesterV2,
    PubMedHarvesterV2,
    RCSBPDBHarvesterV2,
    SRAHarvesterV2,
    UniProtHarvesterV2,
)
from hsa_research.ingestion_bridge.local_ingest import LocalIngestionPipeline
from hsa_research.ingestion_bridge.local_store import SQLiteResearchRepository
from hsa_research.ingestion_bridge.query_policy import build_scholarly_source_queries, infer_comparative_scope
from hsa_research.ingestion_bridge import scraper_bridge
from hsa_research.ingestion_bridge.scraper_bridge import ScrapeBridge, list_scrape_profiles
from hsa_research.ingestion_bridge.service import HSAResearchService
from hsa_research.ingestion_bridge.source_scout import SourceScoutAgent
from hsa_research.ingestion_bridge.source_health import build_source_health_report
from hsa_research.ingestion_bridge.structured_orchestration import (
    build_structured_source_count_report,
    run_structured_sources_pipeline,
    structured_source_qa,
)


def make_service(tmp_path):
    return HSAResearchService(SQLiteResearchRepository(tmp_path / "hsa.sqlite3"))


def _seed_minimal_source_claim(
    repo: SQLiteResearchRepository,
    source_key: str,
    *,
    curation_status: str = "promote",
    extraction_status: str = "typed",
) -> None:
    raw_record = RawSourceRecord(
        source_key=source_key,
        source_record_id=f"{source_key}:1",
        content_hash=f"{source_key}-raw",
        source_url=f"https://example.org/{source_key}/1",
        raw_payload={"source_key": source_key},
    )
    raw_record_id = repo.upsert_raw_record(raw_record)
    research_object = ResearchObject(
        object_type="publication",
        title=f"{source_key} source record",
        canonical_url=f"https://example.org/{source_key}/1",
        source_key=source_key,
        raw_record_id=raw_record_id,
        dedupe_key=f"{source_key}:1",
    )
    object_id = repo.upsert_research_object(research_object, raw_record_id)
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=f"{source_key} mentions canine hemangiosarcoma and human angiosarcoma context.",
            content_hash=f"{source_key}-chunk",
        )
    )
    repo.upsert_claim(
        ClaimSearchResult(
            claim_id=uuid4(),
            statement=f"{source_key} provides source context for canine HSA research.",
            claim_type=ClaimType.OTHER,
            direction=ClaimDirection.NEUTRAL,
            confidence=0.7,
            evidence_level=EvidenceLevel.REVIEW,
            source_object_id=object_id,
            source_title=f"{source_key} source record",
            source_url=f"https://example.org/{source_key}/1",
            support_count=1,
            metadata={"curation_status": curation_status, "extraction_status": extraction_status},
        )
    )


def test_search_claims_uses_typed_contracts(tmp_path):
    service = make_service(tmp_path)

    results = service.search_claims(
        ClaimSearchRequest(query="propranolol", species="canine", min_confidence=0.1)
    )

    assert results.total == 1
    assert "Propranolol" in results.results[0].statement


def test_propose_hypothesis_defaults_to_draft(tmp_path):
    service = make_service(tmp_path)

    draft = service.propose_hypothesis(
        HypothesisProposalRequest(objective="propranolol in canine HSA", candidate_name="propranolol")
    )

    assert draft.status == "draft"
    assert draft.hypothesis_id is None
    assert draft.supporting_claim_ids


def test_commit_hypothesis_requires_explicit_call(tmp_path):
    service = make_service(tmp_path)
    draft = service.propose_hypothesis(
        HypothesisProposalRequest(objective="angiogenesis in canine HSA", target_name="VEGFA")
    )

    committed = service.commit_hypothesis(
        CommitHypothesisRequest(draft=draft, approved_by="test", approval_note="unit test")
    )

    assert committed.status == "approved"
    assert committed.hypothesis_id is not None
    assert committed.metadata["approved_by"] == "test"


def test_run_boltz_returns_approval_gated_handle(tmp_path):
    service = make_service(tmp_path)

    handle = service.run_boltz(
        BoltzRunRequest(target_name="cKDR", ligand_name="test ligand", ligand_smiles="CCO")
    )

    assert handle.status == "needs_approval"
    assert service.get_run_status(handle.run_id) == handle


def test_request_validation_can_queue_without_approval(tmp_path):
    service = make_service(tmp_path)

    handle = service.request_validation(
        ValidationRequest(
            validation_type="admet",
            candidate_name="propranolol",
            objective="Screen canine safety risk",
            require_approval=False,
        )
    )

    assert handle.status == "queued"
    assert service.get_candidate(CandidateDossierRequest(candidate_name="propranolol")) is not None


def test_local_pipeline_initializes_sources_and_queries(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)

    output = pipeline.initialize()
    coverage = pipeline.coverage()

    assert output["sources"] >= 4
    assert coverage["sources"] >= 4
    assert coverage["source_queries"] >= 4
    assert any(query.query_name == "licensed_full_text_hsa" for query in repo.list_source_queries("pmc_oa"))
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
    assert report["totals"] == {"raw_records": 1, "research_objects": 1, "document_chunks": 1, "claims": 1}
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
    assert report["summary"] == {"sources": 2, "healthy": 0, "triage": 0, "watch": 1, "failing": 1}
    assert report["failed_sources"] == ["chembl"]
    assert report["watch_sources"] == ["pubchem"]
    assert report["triage_sources"] == []
    assert pubchem["health_status"] == "watch"
    assert pubchem["source_role"] == "evidence"
    assert pubchem["health_score"] >= report["minimum_bar"]["min_health_score"]
    assert pubchem["passes_minimum_bar"] is True
    assert pubchem["claim_metadata"]["extraction_status"] == {"source_context": 1}
    assert any("source-context" in risk for risk in pubchem["risks"])
    assert chembl["health_status"] == "failing"
    assert chembl["passes_minimum_bar"] is False


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

    assert report["summary"] == {"sources": 1, "healthy": 0, "triage": 1, "watch": 0, "failing": 0}
    assert report["failed_sources"] == []
    assert report["triage_sources"] == ["sra"]
    assert report["watch_sources"] == []
    assert sra["source_role"] == "triage"
    assert sra["health_status"] == "triage"
    assert sra["passes_minimum_bar"] is True
    assert "triage_only_source" in sra["signals"]
    assert any("specialized triage agent" in action for action in sra["recommended_actions"])


def test_structured_pipeline_can_report_empty_selection(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)

    report = run_structured_sources_pipeline(repo, source_keys=[], initialize=False)

    assert report["source_keys"] == []
    assert report["sources"] == []
    assert report["totals"] == {"raw_records": 0, "research_objects": 0, "document_chunks": 0, "claims": 0}
    assert report["errors"] == []


def test_openalex_v2_normalizer_produces_raw_and_research_object():
    harvester = OpenAlexHarvesterV2()

    record = harvester.normalize(
        {
            "id": "https://openalex.org/W123",
            "doi": "https://doi.org/10.1234/example",
            "title": "Canine hemangiosarcoma example",
            "publication_year": 2026,
            "publication_date": "2026-01-02",
            "abstract_inverted_index": {"Canine": [0], "HSA": [1]},
            "ids": {"pmid": "https://pubmed.ncbi.nlm.nih.gov/123"},
            "primary_location": {
                "landing_page_url": "https://doi.org/10.1234/example",
                "source": {"display_name": "Example Journal"},
            },
        }
    )

    assert record.raw_record.source_key == "openalex"
    assert record.research_object.title == "Canine hemangiosarcoma example"
    assert record.research_object.identifiers["doi"] == "10.1234/example"
    assert record.research_object.abstract == "Canine HSA"
    assert record.research_object.metadata["harvester"] == "v2"


def test_scholarly_query_policy_always_includes_human_angiosarcoma():
    queries = build_scholarly_source_queries()
    pubmed_query = next(query for query in queries if query.source_key == "pubmed" and query.query_name == "comparative_hsa_required")
    pmc_query = next(query for query in queries if query.source_key == "pmc_oa")

    assert queries
    assert all("angiosarcoma" in query.query_text.lower() for query in queries)
    assert all("hemangiosarcoma" in query.query_text.lower() for query in queries)
    assert "angiosarcoma[tiab]" in pubmed_query.query_text
    assert "[tiab]" in pmc_query.query_text
    assert "comparative oncology" not in pmc_query.query_text.lower()


def test_comparative_scope_does_not_match_angiosarcoma_inside_hemangiosarcoma():
    policy = infer_comparative_scope(
        "Canine hemangiosarcoma angiogenesis",
        "Canine hemangiosarcoma studies discuss VEGF.",
    )

    assert policy["matched_concepts"] == ["canine_hsa"]


def test_pubmed_v2_normalizer_handles_nested_xml_text():
    article = ET.fromstring(
        """
        <PubmedArticle>
          <MedlineCitation>
            <PMID>123</PMID>
            <Article>
              <ArticleTitle>Canine <i>hemangiosarcoma</i> and human angiosarcoma</ArticleTitle>
              <Abstract>
                <AbstractText>Human <b>angiosarcoma</b> analog evidence.</AbstractText>
              </Abstract>
              <Journal>
                <Title>Example Journal</Title>
                <JournalIssue><PubDate><Year>2026</Year></PubDate></JournalIssue>
              </Journal>
            </Article>
          </MedlineCitation>
        </PubmedArticle>
        """
    )

    record = PubMedHarvesterV2().normalize(article)

    assert record.research_object.title == "Canine hemangiosarcoma and human angiosarcoma"
    assert record.research_object.abstract == "Human angiosarcoma analog evidence."
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == [
        "canine_hsa",
        "human_angiosarcoma",
    ]


def test_europe_pmc_v2_normalizer_cleans_escaped_title_markup():
    record = EuropePMCHarvesterV2().normalize(
        {
            "id": "x1",
            "title": "Primary &lt;i&gt;Vaginal&lt;/i&gt; Angiosarcoma",
            "abstractText": "Human angiosarcoma case report.",
            "pubYear": "2026",
        }
    )

    assert record.research_object.title == "Primary Vaginal Angiosarcoma"
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["human_angiosarcoma"]


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
    assert record.research_object.metadata["full_text_available"] is True
    assert record.research_object.metadata["body_only_match"] is True
    assert record.research_object.metadata["body_ingestion_policy"]["matched_concepts"] == ["human_angiosarcoma"]
    assert harvester.chunk_section_label(record) == "full_text"
    assert "full text mentions VEGF" in harvester.text_for_chunking(record)


def test_europe_pmc_v2_fetch_keeps_body_only_policy_match(monkeypatch):
    def fake_get_json(url, params):
        assert url.endswith("/search")
        assert params["resultType"] == "core"
        return {
            "resultList": {
                "result": [
                    {
                        "id": "PMC123",
                        "pmcid": "PMC123",
                        "title": "Endothelial biology review",
                        "abstractText": "Sparse abstract.",
                        "isOpenAccess": "Y",
                    }
                ]
            }
        }

    def fake_get_text(url, params):
        assert url == "https://www.ebi.ac.uk/europepmc/webservices/rest/PMC123/fullTextXML"
        assert params == {}
        return """
        <article xmlns="http://jats.nlm.nih.gov">
          <front><article-meta><article-id pub-id-type="pmc">PMC123</article-id></article-meta></front>
          <body><p>Canine hemangiosarcoma full text mentions VEGF and propranolol.</p></body>
        </article>
        """

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)
    monkeypatch.setattr(harvesters_v2, "_get_text", fake_get_text)

    records = EuropePMCHarvesterV2().fetch("hemangiosarcoma", limit=1, open_access=True, require_policy_match=True)

    assert len(records) == 1
    assert records[0].research_object.metadata["body_only_match"] is True
    assert records[0].research_object.metadata["body_ingestion_policy"]["matched_concepts"] == ["canine_hsa"]


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


def test_pmc_oa_v2_fetch_keeps_body_only_policy_match(monkeypatch):
    xml = """
    <article xmlns="http://jats.nlm.nih.gov">
      <front>
        <article-meta>
          <article-id pub-id-type="pmc">PMC123456</article-id>
          <title-group><article-title>Open access endothelial biology review</article-title></title-group>
          <permissions>
            <license license-type="open-access">
              <license-p>Creative Commons Attribution License</license-p>
            </license>
          </permissions>
        </article-meta>
      </front>
      <body><p>Human angiosarcoma full text mentions VEGF and propranolol.</p></body>
    </article>
    """

    def fake_get_json(url, params):
        assert url.endswith("/esearch.fcgi")
        assert params["db"] == "pmc"
        return {"esearchresult": {"idlist": ["123456"]}}

    def fake_get_text(url, params):
        assert url == "https://pmc.ncbi.nlm.nih.gov/api/oai/v1/mh/"
        assert params["identifier"] == "oai:pubmedcentral.nih.gov:123456"
        return xml

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)
    monkeypatch.setattr(harvesters_v2, "_get_text", fake_get_text)
    monkeypatch.setattr(
        harvesters_v2,
        "_pmc_oa_metadata",
        lambda pmcid: {"oa_license": "CC BY", "retracted": "no"},
    )
    monkeypatch.setattr(harvesters_v2.time, "sleep", lambda _seconds: None)

    records = PMCOAHarvesterV2().fetch("hemangiosarcoma", limit=1, require_policy_match=True)

    assert len(records) == 1
    assert records[0].research_object.metadata["body_only_match"] is True
    assert records[0].research_object.metadata["body_ingestion_policy"]["matched_concepts"] == [
        "human_angiosarcoma"
    ]


def test_pmc_oa_v2_is_registered_harvester():
    assert HARVESTERS_V2["pmc_oa"] is PMCOAHarvesterV2


def test_hosted_literature_smoke_includes_pmc_oa():
    assert "pmc_oa" in LITERATURE_CLINICAL_SMOKE_KEYS
    assert "pmc_oa" in HOSTED_API_REPORT_KEYS


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


def test_clinicaltrials_gov_v2_normalizer_extracts_trial_fields():
    study = {
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT00000001",
                "orgStudyIdInfo": {"id": "ORG-1"},
                "briefTitle": "Pazopanib in Angiosarcoma",
                "officialTitle": "Pazopanib for Patients With Advanced Human Angiosarcoma",
            },
            "statusModule": {
                "overallStatus": "RECRUITING",
                "startDateStruct": {"date": "2026-01-01", "type": "ACTUAL"},
                "studyFirstPostDateStruct": {"date": "2026-02-01", "type": "ACTUAL"},
                "completionDateStruct": {"date": "2028-01", "type": "ESTIMATED"},
            },
            "descriptionModule": {
                "briefSummary": "This study tests pazopanib in human angiosarcoma.",
                "detailedDescription": "Participants receive pazopanib and undergo response assessment.",
            },
            "conditionsModule": {"conditions": ["Angiosarcoma", "Vascular Sarcoma"]},
            "designModule": {
                "studyType": "INTERVENTIONAL",
                "phases": ["PHASE2"],
                "enrollmentInfo": {"count": 42, "type": "ESTIMATED"},
            },
            "armsInterventionsModule": {
                "interventions": [
                    {"type": "DRUG", "name": "Pazopanib"},
                    {"type": "DRUG", "name": "Paclitaxel"},
                ]
            },
            "outcomesModule": {
                "primaryOutcomes": [{"measure": "Objective response rate"}],
                "secondaryOutcomes": [{"measure": "Progression-free survival"}],
            },
            "eligibilityModule": {
                "eligibilityCriteria": "Inclusion: measurable angiosarcoma.",
                "minimumAge": "18 Years",
                "sex": "ALL",
                "stdAges": ["ADULT", "OLDER_ADULT"],
            },
            "sponsorCollaboratorsModule": {
                "leadSponsor": {"name": "Example Cancer Center"},
                "collaborators": [{"name": "National Cancer Institute"}],
            },
            "contactsLocationsModule": {
                "locations": [
                    {
                        "facility": "Example Hospital",
                        "city": "Denver",
                        "state": "Colorado",
                        "country": "United States",
                        "status": "RECRUITING",
                    }
                ]
            },
        }
    }

    harvester = ClinicalTrialsGovHarvesterV2()
    record = harvester.normalize(study)

    assert record.raw_record.source_key == "clinicaltrials_gov"
    assert record.research_object.object_type == "clinical_trial"
    assert record.research_object.identifiers["nct_id"] == "NCT00000001"
    assert record.research_object.canonical_url == "https://clinicaltrials.gov/study/NCT00000001"
    assert record.research_object.metadata["overall_status"] == "RECRUITING"
    assert record.research_object.metadata["interventions"] == ["Pazopanib", "Paclitaxel"]
    assert record.research_object.metadata["primary_outcomes"] == ["Objective response rate"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == [
        "human_angiosarcoma",
        "vascular_sarcoma_analog",
    ]
    assert "Inclusion: measurable angiosarcoma." in harvester.text_for_chunking(record)


def test_clinicaltrials_gov_v2_is_registered_harvester():
    assert HARVESTERS_V2["clinicaltrials_gov"] is ClinicalTrialsGovHarvesterV2


def _avma_vctr_study_card():
    return {
        "id": 12345,
        "is_similar": False,
        "description": "<p>Dogs with splenic hemangiosarcoma receive antibody therapy after splenectomy.</p>",
        "absolute_url": "https://veterinaryclinicaltrials.org/s/antibody-therapy-hsa/",
        "tagline": "Dogs with splenic hemangiosarcoma after splenectomy",
        "name": "Safety and efficacy of antibody therapy for dogs with splenic hemangiosarcoma",
        "visible_sc_items": [
            {"id": "species-dog", "tag_path": "/species/dogs", "label": "Dogs", "parent_label": "Species"},
            {
                "id": "oncology-hsa",
                "tag_path": "/primary-field/oncology/hemangiosarcoma",
                "label": "Hemangiosarcoma",
                "parent_label": "Oncology",
            },
            {
                "id": "intervention-biologic",
                "tag_path": "/intervention-type/biologic",
                "label": "Biologic",
                "parent_label": "Intervention type",
            },
            {
                "id": "financial-covered",
                "tag_path": "/financial-incentive/study-costs-covered",
                "label": "Study costs covered",
                "parent_label": "Financial incentive",
            },
        ],
        "thumbnail": "https://veterinaryclinicaltrials.org/media/cache/thumb.jpg",
        "study_type": "Interventional",
        "target_gender": "All",
        "age_range": "1 year and older",
        "top_investigator": "Claire Lemons, DVM",
        "status": "Recruiting",
        "status_color": "green",
        "distance_to_location": None,
        "vct_code": "VCT16000189",
    }


def test_avma_vctr_v2_normalizer_extracts_study_card_metadata():
    harvester = AVMAVCTRHarvesterV2()
    record = harvester.normalize(_avma_vctr_study_card(), source_query="hemangiosarcoma", source_total=27)

    assert record.raw_record.source_key == "avma_vctr"
    assert record.raw_record.source_record_id == "VCT16000189"
    assert record.research_object.object_type == "veterinary_trial"
    assert record.research_object.identifiers["vct_code"] == "VCT16000189"
    assert record.research_object.identifiers["avma_study_id"] == "12345"
    assert record.research_object.dedupe_key == "vct_code:vct16000189"
    assert record.research_object.canonical_url == "https://veterinaryclinicaltrials.org/s/antibody-therapy-hsa/"
    assert record.research_object.title == "Safety and efficacy of antibody therapy for dogs with splenic hemangiosarcoma"
    assert record.research_object.abstract == (
        "Dogs with splenic hemangiosarcoma receive antibody therapy after splenectomy."
    )
    assert record.research_object.metadata["status"] == "Recruiting"
    assert record.research_object.metadata["species"] == ["Dogs"]
    assert record.research_object.metadata["conditions"] == ["Hemangiosarcoma"]
    assert record.research_object.metadata["intervention_types"] == ["Biologic"]
    assert record.research_object.metadata["financial_incentives"] == ["Study costs covered"]
    assert record.research_object.metadata["visible_search_categories"]["Species"] == ["Dogs"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["canine_hsa"]
    assert record.research_object.metadata["source_total"] == 27
    assert "VCT16000189" in harvester.text_for_chunking(record)
    assert "Biologic" in harvester.text_for_chunking(record)


def test_avma_vctr_v2_normalizer_does_not_treat_late_reference_as_primary_hsa():
    study = _avma_vctr_study_card() | {
        "name": "The effect of a novel mushroom formula on canine oral malignant melanoma",
        "description": (
            "Oral malignant melanoma is an aggressive cancer in dogs. This study evaluates a mushroom "
            "supplement for melanoma. " + ("General oncology background. " * 40)
            + "A cited hemangiosarcoma study informed the dose."
        ),
        "tagline": "Evaluation of Medicinal Mushroom Supplementation in Canine Oral Malignant Melanoma",
        "visible_sc_items": [
            {"id": "species-dog", "tag_path": "/species/dogs", "label": "Canine", "parent_label": "Species"},
            {
                "id": "oncology-melanoma",
                "tag_path": "/primary-field/oncology/melanoma",
                "label": "Melanoma",
                "parent_label": "Oncology",
            },
        ],
        "vct_code": "VCT-MELANOMA",
    }

    record = AVMAVCTRHarvesterV2().normalize(study, source_query="hemangiosarcoma")

    assert record.research_object.metadata["conditions"] == ["Melanoma"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == []


def test_avma_vctr_v2_fetch_uses_public_search_endpoint(monkeypatch):
    captured = {}

    def fake_get_json(url, params):
        captured["url"] = url
        captured["params"] = params
        return {"total": 1, "studies": [_avma_vctr_study_card()]}

    monkeypatch.setattr("hsa_research.ingestion_bridge.harvesters_v2._get_json", fake_get_json)

    records = AVMAVCTRHarvesterV2().fetch("hemangiosarcoma", limit=5)

    assert len(records) == 1
    assert captured["url"] == "https://veterinaryclinicaltrials.org/avma/studies/search/json/"
    assert captured["params"]["search"] == "hemangiosarcoma"
    assert captured["params"]["skip"] == 0
    assert captured["params"]["take"] == 5
    assert captured["params"]["sort_by"] == "score"
    assert captured["params"]["skip_similar_studies"] == "true"
    assert captured["params"]["extra_aggregations"] == "[]"


def test_avma_vctr_v2_is_registered_harvester():
    assert HARVESTERS_V2["avma_vctr"] is AVMAVCTRHarvesterV2


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


def test_icdc_v2_normalizer_extracts_canine_case_metadata():
    case = {
        "case_id": "TCL01-DEN-HSA",
        "study_code": "TCL01",
        "study_type": "Genomics",
        "cohort": "Cell line",
        "breed": "Golden Retriever",
        "diagnosis": "Hemangiosarcoma",
        "disease_site": "Kidney",
        "primary_disease_site": "Kidney",
        "stage_of_disease": "Unknown",
        "age": 11.0,
        "sex": "Male",
        "response_to_treatment": "Not Applicable",
        "files": ["file-1", "file-2"],
        "treatment_data": "Yes",
        "follow_up_data": "No",
        "pathology_report": "No",
    }
    study = {
        "clinical_study_designation": "TCL01",
        "clinical_study_name": "Whole exome sequencing analysis of canine cancer cell lines",
        "clinical_study_description": "This study analyzes canine cancer cell lines including hemangiosarcoma.",
        "clinical_study_type": "Genomics",
        "accession_id": "000008",
        "dates_of_conduct": "2017-2019",
        "study_disposition": "Unrestricted",
    }

    harvester = ICDCHarvesterV2()
    record = harvester.normalize(case, study)

    assert record.raw_record.source_key == "icdc"
    assert record.research_object.object_type == "dataset"
    assert record.research_object.identifiers["icdc_case_id"] == "TCL01-DEN-HSA"
    assert record.research_object.identifiers["study_code"] == "TCL01"
    assert record.research_object.metadata["breed"] == "Golden Retriever"
    assert record.research_object.metadata["file_count"] == 2
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["canine_hsa"]
    assert "Diagnosis: Hemangiosarcoma" in harvester.text_for_chunking(record)


def test_icdc_v2_is_registered_harvester():
    assert HARVESTERS_V2["icdc"] is ICDCHarvesterV2


def test_geo_v2_normalizer_extracts_dataset_metadata():
    item = {
        "uid": "200310480",
        "accession": "GSE310480",
        "title": "MicroRNA biomarkers for canine visceral hemangiosarcoma",
        "summary": "Canine visceral hemangiosarcoma samples identify miRNA biomarkers.",
        "gse": "310480",
        "taxon": "Canis lupus familiaris",
        "entrytype": "GSE",
        "gdstype": "Non-coding RNA profiling by high throughput sequencing",
        "pdat": "2026/04/08",
        "suppfile": "TXT, XLSX",
        "samples": [{"accession": "GSM1", "title": "Cancer spleen 1"}],
        "n_samples": 36,
        "pubmedids": ["41924723"],
        "ftplink": "ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE310nnn/GSE310480/",
        "bioproject": "PRJNA1366394",
    }

    harvester = GEOHarvesterV2()
    record = harvester.normalize(item)

    assert record.raw_record.source_key == "geo"
    assert record.research_object.object_type == "dataset"
    assert record.research_object.identifiers["geo_accession"] == "GSE310480"
    assert record.research_object.identifiers["bioproject"] == "PRJNA1366394"
    assert record.research_object.metadata["sample_accessions"] == ["GSM1"]
    assert record.research_object.metadata["supplementary_file_types"] == ["TXT", "XLSX"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["canine_hsa"]
    assert "Cancer spleen 1" in harvester.text_for_chunking(record)


def test_sra_v2_normalizer_extracts_run_metadata():
    item = {
        "uid": "42394755",
        "expxml": """
          <Summary>
            <Title>NM-BS23039 Canine hemangiosarcoma primary cell</Title>
            <Platform instrument_model="Illumina NovaSeq 6000">ILLUMINA</Platform>
            <Statistics total_runs="1" total_spots="26421116" total_bases="7926334800" total_size="2407693008"/>
          </Summary>
          <Submitter acc="SRA2311501" center_name="Tokyo University of Agriculture and Technology"/>
          <Experiment acc="SRX31723477" name="NM-BS23039 Canine hemangiosarcoma primary cell"/>
          <Study acc="SRP660537" name="Canine hemangiosarcoma primary cell RNA sequencing"/>
          <Organism taxid="9615" ScientificName="Canis lupus familiaris"/>
          <Sample acc="SRS27692090"/>
          <Library_descriptor>
            <LIBRARY_NAME>NM-BS23039_L1_1</LIBRARY_NAME>
            <LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY>
            <LIBRARY_SOURCE>TRANSCRIPTOMIC</LIBRARY_SOURCE>
            <LIBRARY_SELECTION>RANDOM</LIBRARY_SELECTION>
            <LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT>
          </Library_descriptor>
          <Bioproject>PRJNA1399620</Bioproject>
          <Biosample>SAMN54501165</Biosample>
        """,
        "runs": '<Run acc="SRR36719144" total_spots="26421116" total_bases="7926334800" is_public="true"/>',
        "createdate": "2026/03/31",
        "updatedate": "2026/01/07",
    }

    harvester = SRAHarvesterV2()
    record = harvester.normalize(item)

    assert record.raw_record.source_key == "sra"
    assert record.research_object.object_type == "dataset"
    assert record.research_object.identifiers["sra_experiment"] == "SRX31723477"
    assert record.research_object.identifiers["sra_run"] == "SRR36719144"
    assert record.research_object.identifiers["bioproject"] == "PRJNA1399620"
    assert record.research_object.metadata["library_strategy"] == "RNA-Seq"
    assert record.research_object.metadata["library_layout"] == "PAIRED"
    assert record.research_object.metadata["statistics"]["total_spots"] == "26421116"
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["canine_hsa"]
    assert "SRR36719144" in harvester.text_for_chunking(record)


def test_geo_and_sra_v2_are_registered_harvesters():
    assert HARVESTERS_V2["geo"] is GEOHarvesterV2
    assert HARVESTERS_V2["sra"] is SRAHarvesterV2


def test_pubchem_v2_normalizer_extracts_compound_metadata():
    payload = {
        "query_term": "propranolol",
        "properties": {
            "CID": 4946,
            "Title": "Propranolol",
            "MolecularFormula": "C16H21NO2",
            "MolecularWeight": 259.34,
            "CanonicalSMILES": "CC(C)NCC(COC1=CC=CC2=CC=CC=C21)O",
            "InChIKey": "AQHHHDLHHXJYJD-UHFFFAOYSA-N",
            "IUPACName": "1-naphthalen-1-yloxy-3-(propan-2-ylamino)propan-2-ol",
            "XLogP": 3.0,
            "TPSA": 41.5,
        },
        "synonyms": ["Propranolol", "Inderal"],
    }

    harvester = PubChemHarvesterV2()
    record = harvester.normalize(payload)

    assert record.raw_record.source_key == "pubchem"
    assert record.research_object.object_type == "compound_record"
    assert record.research_object.identifiers["pubchem_cid"] == "4946"
    assert record.research_object.identifiers["inchikey"] == "AQHHHDLHHXJYJD-UHFFFAOYSA-N"
    assert record.research_object.dedupe_key == "pubchem_cid:4946"
    assert record.research_object.canonical_url == "https://pubchem.ncbi.nlm.nih.gov/compound/4946"
    assert record.research_object.metadata["canonical_smiles"].startswith("CC(C)")
    assert "Inderal" in harvester.text_for_chunking(record)


def test_chembl_v2_normalizer_extracts_bioactivity_metadata():
    payload = {
        "query_term": "toceranib",
        "molecule": {
            "molecule_chembl_id": "CHEMBL13608",
            "pref_name": "TOCERANIB",
            "max_phase": 4,
            "molecule_type": "Small molecule",
        },
        "activity": {
            "activity_id": 123,
            "molecule_chembl_id": "CHEMBL13608",
            "target_chembl_id": "CHEMBL279",
            "target_pref_name": "Vascular endothelial growth factor receptor 2",
            "target_organism": "Homo sapiens",
            "assay_chembl_id": "CHEMBL-A",
            "document_chembl_id": "CHEMBL-D",
            "standard_type": "IC50",
            "standard_relation": "=",
            "standard_value": "5.0",
            "standard_units": "nM",
            "pchembl_value": "8.3",
            "assay_description": "Inhibition of VEGFR2 kinase activity.",
        },
    }

    harvester = ChEMBLHarvesterV2()
    record = harvester.normalize(payload)

    assert record.raw_record.source_key == "chembl"
    assert record.research_object.object_type == "bioactivity_assay"
    assert record.research_object.identifiers["chembl_activity_id"] == "123"
    assert record.research_object.identifiers["chembl_molecule_id"] == "CHEMBL13608"
    assert record.research_object.dedupe_key == "chembl_activity_id:123"
    assert record.research_object.metadata["standard_type"] == "IC50"
    assert record.research_object.metadata["target_pref_name"] == "Vascular endothelial growth factor receptor 2"
    assert record.research_object.metadata["target_gene"] == "KDR"
    assert record.research_object.metadata["target_category"] == "vegf_angiogenesis"
    assert record.research_object.metadata["pchembl_numeric"] == 8.3
    assert "Target gate: KDR (vegf_angiogenesis)" in harvester.text_for_chunking(record)
    assert "pChEMBL: 8.3" in harvester.text_for_chunking(record)


def test_chembl_v2_fetches_only_target_gated_relevant_bioactivities(monkeypatch):
    def fake_get_json(url, params):
        if url.endswith("/molecule.json"):
            if params.get("pref_name__iexact") == "toceranib":
                return {
                    "molecules": [
                        {
                            "molecule_chembl_id": "CHEMBL13608",
                            "pref_name": "TOCERANIB",
                            "max_phase": 2,
                            "molecule_type": "Small molecule",
                        }
                    ]
                }
            return {"molecules": []}
        if url.endswith("/activity.json"):
            assert params["target_chembl_id__in"] == "CHEMBL279"
            assert params["standard_type__in"] == "IC50"
            assert params["assay_type__in"] == "B"
            assert params["order_by"] == "-pchembl_value"
            return {
                "activities": [
                    {
                        "activity_id": 1,
                        "molecule_chembl_id": "CHEMBL13608",
                        "target_chembl_id": "CHEMBL279",
                        "target_pref_name": "Vascular endothelial growth factor receptor 2",
                        "target_organism": "Homo sapiens",
                        "assay_type": "B",
                        "standard_type": "IC50",
                        "standard_relation": "=",
                        "standard_value": "60.0",
                        "standard_units": "nM",
                        "pchembl_value": "7.22",
                        "assay_description": "Inhibition of VEGFR2.",
                    },
                    {
                        "activity_id": 2,
                        "target_chembl_id": "CHEMBL999",
                        "target_organism": "Homo sapiens",
                        "assay_type": "B",
                        "standard_type": "IC50",
                        "pchembl_value": "9.0",
                    },
                    {
                        "activity_id": 3,
                        "target_chembl_id": "CHEMBL279",
                        "target_organism": "Homo sapiens",
                        "assay_type": "B",
                        "standard_type": "IC50",
                        "pchembl_value": "3.5",
                    },
                ]
            }
        raise AssertionError(f"Unexpected ChEMBL URL: {url}")

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    harvester = ChEMBLHarvesterV2()
    records = harvester.fetch(
        "toceranib",
        limit=3,
        target_chembl_ids=["CHEMBL279"],
        target_organisms=["Homo sapiens"],
        standard_types=["IC50"],
        assay_types=["B"],
        min_pchembl=6.0,
        activities_per_molecule=3,
        include_cell_line_assays=False,
    )

    assert len(records) == 1
    assert records[0].research_object.identifiers["chembl_activity_id"] == "1"
    assert records[0].research_object.metadata["target_gene"] == "KDR"
    assert records[0].research_object.metadata["target_category"] == "vegf_angiogenesis"


def test_chembl_v2_cell_line_lane_requires_real_disease_term(monkeypatch):
    def fake_get_json(url, params):
        if url.endswith("/molecule.json"):
            if params.get("pref_name__iexact") == "paclitaxel":
                return {
                    "molecules": [
                        {
                            "molecule_chembl_id": "CHEMBL428647",
                            "pref_name": "PACLITAXEL",
                            "max_phase": 4,
                            "molecule_type": "Small molecule",
                        }
                    ]
                }
            return {"molecules": []}
        if url.endswith("/activity.json") and params.get("target_type") == "CELL-LINE":
            return {
                "activities": [
                    {
                        "activity_id": 10,
                        "target_chembl_id": "CHEMBL210",
                        "target_pref_name": "Beta-2 adrenergic receptor",
                        "target_organism": "Homo sapiens",
                        "assay_type": "F",
                        "standard_type": "IC50",
                        "pchembl_value": "9.2",
                        "assay_description": "Activity in endogenously expressing cells.",
                    },
                    {
                        "activity_id": 11,
                        "target_chembl_id": "CHEMBL613827",
                        "target_pref_name": "MES-SA/Dx5",
                        "target_organism": "Homo sapiens",
                        "assay_type": "F",
                        "standard_type": "IC50",
                        "pchembl_value": "10.4",
                        "assay_description": "Cytotoxic activity against uterine sarcoma cells.",
                    },
                ]
            }
        if url.endswith("/activity.json"):
            return {"activities": []}
        raise AssertionError(f"Unexpected ChEMBL URL: {url}")

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    harvester = ChEMBLHarvesterV2()
    records = harvester.fetch(
        "paclitaxel",
        limit=3,
        target_chembl_ids=["CHEMBL210"],
        target_organisms=["Homo sapiens"],
        include_cell_line_assays=True,
        cell_line_terms=["sarcoma", "dog"],
        cell_line_records_per_molecule=2,
    )

    assert len(records) == 1
    assert records[0].research_object.identifiers["chembl_activity_id"] == "11"
    assert records[0].research_object.metadata["target_category"] == "cell_cytotoxicity"
    assert records[0].research_object.metadata["matched_cell_line_term"] == "sarcoma"


def test_uniprot_v2_normalizer_extracts_target_metadata():
    entry = {
        "primaryAccession": "P35968",
        "uniProtKBId": "VGFR2_HUMAN",
        "entryType": "UniProtKB reviewed (Swiss-Prot)",
        "proteinDescription": {
            "recommendedName": {
                "fullName": {"value": "Vascular endothelial growth factor receptor 2"}
            }
        },
        "genes": [{"geneName": {"value": "KDR"}, "synonyms": [{"value": "VEGFR2"}]}],
        "organism": {"scientificName": "Homo sapiens", "taxonId": 9606},
        "sequence": {"length": 1356, "molWeight": 151527},
        "comments": [
            {
                "commentType": "FUNCTION",
                "texts": [{"value": "Tyrosine-protein kinase receptor for VEGFA."}],
            }
        ],
        "keywords": [{"name": "Angiogenesis"}],
        "uniProtKBCrossReferences": [{"database": "AlphaFoldDB", "id": "AF-P35968-F1"}],
    }

    harvester = UniProtHarvesterV2()
    record = harvester.normalize(entry, source_query="KDR")

    assert record.raw_record.source_key == "uniprot"
    assert record.research_object.object_type == "structure"
    assert record.research_object.identifiers["uniprot_accession"] == "P35968"
    assert record.research_object.identifiers["gene_symbol"] == "KDR"
    assert record.research_object.dedupe_key == "uniprot_accession:p35968"
    assert record.research_object.metadata["reviewed"] is True
    assert record.research_object.metadata["target_gene"] == "KDR"
    assert record.research_object.metadata["target_category"] == "vegf_angiogenesis"
    assert record.research_object.metadata["species_scope"] == "human"
    assert record.research_object.metadata["gene_match_verified"] is True
    assert record.research_object.metadata["alphafold_ids"] == ["AF-P35968-F1"]
    assert "AlphaFold IDs: AF-P35968-F1" in harvester.text_for_chunking(record)


def test_rcsb_pdb_v2_normalizer_extracts_structure_metadata():
    payload = {
        "query_term": "KDR",
        "search_hit": {"identifier": "3VHE", "score": 42.0},
        "entry": {
            "rcsb_id": "3VHE",
            "struct": {"title": "Crystal structure of VEGFR2 kinase domain"},
            "exptl": [{"method": "X-RAY DIFFRACTION"}],
            "rcsb_accession_info": {
                "deposit_date": "2011-01-01",
                "initial_release_date": "2012-02-01",
                "revision_date": "2020-01-01",
                "has_released_experimental_data": True,
            },
            "citation": [{"pdbx_database_id_PubMed": 22212345}],
            "rcsb_entry_info": {"polymer_entity_count_protein": 1},
        },
    }

    harvester = RCSBPDBHarvesterV2()
    record = harvester.normalize(payload)

    assert record.raw_record.source_key == "rcsb_pdb"
    assert record.research_object.object_type == "structure"
    assert record.research_object.identifiers["pdb_id"] == "3VHE"
    assert record.research_object.identifiers["pmid"] == "22212345"
    assert record.research_object.dedupe_key == "pdb_id:3vhe"
    assert record.research_object.publication_year == 2012
    assert record.research_object.metadata["target_gene"] == "KDR"
    assert record.research_object.metadata["target_category"] == "vegf_angiogenesis"
    assert record.research_object.metadata["experimental_methods"] == ["X-RAY DIFFRACTION"]
    assert record.research_object.metadata["protein_entity_count"] == 1
    assert "PDB ID: 3VHE" in harvester.text_for_chunking(record)


def test_openfda_animal_events_v2_normalizer_extracts_safety_metadata():
    event = {
        "unique_aer_id_number": "US-FDA-CVM-2026-0001",
        "original_receive_date": "20260401",
        "animal": {"species": "Dog", "breed": "Golden Retriever", "gender": "Female", "age": {"unit": "Year", "value": "9"}},
        "drug": [
            {
                "brand_name": "Example Doxorubicin",
                "active_ingredients": [{"name": "doxorubicin"}],
            }
        ],
        "reaction": [{"veddra_term_name": "Vomiting", "veddra_term_code": "334"}, {"veddra_term_name": "Neutropenia"}],
        "outcome": "Recovered",
        "serious_ae": "true",
        "primary_reporter": "Veterinarian",
    }

    harvester = OpenFDAAnimalEventsHarvesterV2()
    record = harvester.normalize(event, source_query="doxorubicin", source_search='animal.species:"Dog"')

    assert record.raw_record.source_key == "openfda_animal_events"
    assert record.research_object.object_type == "safety_report"
    assert record.research_object.identifiers["openfda_report_id"] == "US-FDA-CVM-2026-0001"
    assert record.research_object.dedupe_key == "openfda_report_id:us-fda-cvm-2026-0001"
    assert record.research_object.publication_year == 2026
    assert record.research_object.metadata["species"] == "Dog"
    assert record.research_object.metadata["drug_names"] == ["Example Doxorubicin", "doxorubicin"]
    assert record.research_object.metadata["reaction_terms"] == ["Vomiting", "Neutropenia"]
    assert record.research_object.metadata["reaction_codes"] == ["334"]
    assert "Responsible use: signal_generation_only_not_clinical_decision_support" in harvester.text_for_chunking(record)


def test_phase_three_api_harvesters_are_registered():
    assert HARVESTERS_V2["pubchem"] is PubChemHarvesterV2
    assert HARVESTERS_V2["chembl"] is ChEMBLHarvesterV2
    assert HARVESTERS_V2["uniprot"] is UniProtHarvesterV2
    assert HARVESTERS_V2["rcsb_pdb"] is RCSBPDBHarvesterV2
    assert HARVESTERS_V2["openfda_animal_events"] is OpenFDAAnimalEventsHarvesterV2


def test_openalex_v2_filters_unmatched_records_by_default():
    harvester = OpenAlexHarvesterV2()
    matched = harvester.normalize(
        {
            "id": "https://openalex.org/W1",
            "title": "Human angiosarcoma therapy",
            "publication_year": 2026,
            "abstract_inverted_index": {"Human": [0], "angiosarcoma": [1]},
            "primary_location": {"landing_page_url": "https://example.test/matched"},
        }
    )
    unmatched = harvester.normalize(
        {
            "id": "https://openalex.org/W2",
            "title": "Unrelated oncology therapy",
            "publication_year": 2026,
            "abstract_inverted_index": {"Unrelated": [0], "oncology": [1]},
            "primary_location": {"landing_page_url": "https://example.test/unmatched"},
        }
    )

    assert harvester.filter_relevant([matched, unmatched], {}) == [matched]


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


def test_claim_curator_agent_promotes_supported_draft_claim(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    papers_path = tmp_path / "papers.json"
    papers_path.write_text(
        """
        [
          {
            "pmid": "123",
            "title": "VEGF in canine hemangiosarcoma",
            "abstract": "Canine hemangiosarcoma studies discuss VEGF and angiogenesis.",
            "journal": "Example Journal",
            "year": "2026",
            "source": "pubmed"
          },
          {
            "pmid": "124",
            "title": "Canine hemangiosarcoma angiogenesis",
            "abstract": "Canine hemangiosarcoma work again discusses VEGF and angiogenesis.",
            "journal": "Example Journal",
            "year": "2026",
            "source": "pubmed"
          }
        ]
        """
    )
    backfill_papers_json(repo, papers_path)
    extract_claims_for_repository(repo, source_key="current_papers")

    result = ClaimCuratorAgent(repo).curate(ClaimCurationRequest(limit=20, promote_threshold=0.5))
    visible_claims = repo.search_claims(ClaimSearchRequest(query="VEGFA", species="canine", min_confidence=0.1))

    assert result.claims_seen >= 2
    assert result.promoted >= 1
    assert result.merged_duplicates >= 1
    assert any(claim.metadata["curation_status"] == "promote" for claim in visible_claims)


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
