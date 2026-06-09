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

def test_omics_accession_hunt_ingests_and_reports_accessions(tmp_path, monkeypatch):
    repo = SQLiteResearchRepository(tmp_path / "omics-hunt.sqlite3", seed=False)

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit=5, persist_query=True):
            if persist_query:
                self.repository.upsert_source_query(query)
            if query.source_key == "geo":
                raw_id = self.repository.upsert_raw_record(
                    RawSourceRecord(
                        source_key="geo",
                        source_record_id="GSE310480",
                        content_hash="geo-hunt",
                        raw_payload={"accession": "GSE310480"},
                    )
                )
                self.repository.upsert_research_object(
                    ResearchObject(
                        object_type=ResearchObjectType.DATASET,
                        title="MicroRNA biomarkers for canine visceral hemangiosarcoma",
                        abstract="Canine visceral hemangiosarcoma RNA-seq expression data.",
                        source_key="geo",
                        dedupe_key="geo_accession:gse310480",
                        identifiers={"geo_accession": "GSE310480", "bioproject": "PRJNA1366394"},
                        metadata={"taxon": "Canis lupus familiaris", "sample_count": 36},
                    ),
                    raw_id,
                )
                return IngestionResult(
                    source_key=query.source_key,
                    query_name=query.query_name,
                    query_text=query.query_text,
                    fetch_run_id=uuid4(),
                    raw_records=1,
                    research_objects=1,
                    document_chunks=1,
                )
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                raw_records=0,
                research_objects=0,
                document_chunks=0,
            )

    monkeypatch.setattr(omics_accession_hunt, "LocalIngestionPipeline", FakePipeline)

    result = HSAResearchService(repo).run_omics_accession_hunt(
        OmicsAccessionHuntRequest(
            source_keys=["geo", "sra"],
            query_texts=["canine hemangiosarcoma RNA-seq"],
            limit_per_query=2,
            max_queries=2,
        )
    )

    assert isinstance(result, OmicsAccessionHuntResult)
    assert result.query_count == 2
    assert result.raw_records == 1
    assert result.accession_hit_count == 1
    assert result.accession_hits[0].accession == "GSE310480"
    assert result.accession_hits[0].bioproject == "PRJNA1366394"
    assert result.negative_queries[0]["source_key"] == "sra"


def test_omics_accession_hunt_interleaves_sources_under_query_cap():
    repo = InMemoryResearchRepository()
    result = HSAResearchService(repo).run_omics_accession_hunt(
        OmicsAccessionHuntRequest(
            source_keys=["geo", "sra"],
            query_texts=[
                "canine hemangiosarcoma RNA-seq",
                "human angiosarcoma RNA-seq",
                "vimentin angiosarcoma",
            ],
            max_queries=4,
            dry_run=True,
        )
    )

    assert [query.source_key for query in result.source_queries] == ["geo", "sra", "geo", "sra"]
    assert [query.query_text for query in result.source_queries] == [
        "canine hemangiosarcoma RNA-seq",
        "canine hemangiosarcoma RNA-seq",
        "human angiosarcoma RNA-seq",
        "human angiosarcoma RNA-seq",
    ]


def test_omics_evidence_packets_package_direct_and_analog_accessions(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "omics-packets.sqlite3", seed=False)
    geo_raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="geo",
            source_record_id="GSE150705",
            content_hash="geo-packet",
            raw_payload={"accession": "GSE150705"},
        )
    )
    geo_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type=ResearchObjectType.DATASET,
            title="Canine hemangiosarcoma ChRO-seq and VIM expression dataset",
            abstract=(
                "Canine hemangiosarcoma transcriptome evidence for VIM/vimentin, angiogenesis, "
                "coagulation, and vascular injury programs."
            ),
            source_key="geo",
            raw_record_id=geo_raw_id,
            dedupe_key="geo_accession:gse150705",
            identifiers={
                "geo_accession": "GSE150705",
                "bioproject": "PRJNA633277",
                "pmid": "34023294",
            },
            metadata={
                "organism": "Canis lupus familiaris",
                "sample_count": 21,
                "library_strategy": "ChRO-seq",
                "sample_accessions": ["GSM4550001", "GSM4550002"],
                "supplementary_file_types": ["TXT"],
            },
        ),
        geo_raw_id,
    )
    sra_raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="sra",
            source_record_id="SRX31723468",
            content_hash="sra-packet",
            raw_payload={"accession": "SRX31723468"},
        )
    )
    repo.upsert_research_object(
        ResearchObject(
            object_type=ResearchObjectType.DATASET,
            title="Primary canine hemangiosarcoma cells RNA-Seq VIM expression",
            abstract="RNA-seq run from canine hemangiosarcoma primary cells with vimentin biology context.",
            source_key="sra",
            raw_record_id=sra_raw_id,
            dedupe_key="sra_experiment:srx31723468",
            identifiers={
                "sra_experiment": "SRX31723468",
                "bioproject": "PRJNA1399620",
            },
            metadata={
                "organism": "Canis lupus familiaris",
                "sample_count": 1,
                "library_strategy": "RNA-Seq",
                "run_accessions": ["SRR36719153"],
                "sample_accessions": ["SRS25058134"],
            },
        ),
        sra_raw_id,
    )
    human_raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="geo",
            source_record_id="GSE203215",
            content_hash="geo-human-packet",
            raw_payload={"accession": "GSE203215"},
        )
    )
    repo.upsert_research_object(
        ResearchObject(
            object_type=ResearchObjectType.DATASET,
            title="Human angiosarcoma RNA-seq VIM expression cohort",
            abstract="Human angiosarcoma transcriptome dataset with VIM/vimentin and angiogenesis signals.",
            source_key="geo",
            raw_record_id=human_raw_id,
            dedupe_key="geo_accession:gse203215",
            identifiers={"geo_accession": "GSE203215"},
            metadata={
                "organism": "Homo sapiens",
                "sample_count": 12,
                "library_strategy": "RNA-seq",
                "sample_accessions": ["GSM6170001"],
                "supplementary_file_types": ["TSV"],
            },
        ),
        human_raw_id,
    )
    off_topic_raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="geo",
            source_record_id="GSE30723",
            content_hash="geo-off-topic-packet",
            raw_payload={"accession": "GSE30723"},
        )
    )
    repo.upsert_research_object(
        ResearchObject(
            object_type=ResearchObjectType.DATASET,
            title="Human primary alveolar cells after influenza infection",
            abstract="Background text mentions canine hemangiosarcoma, but this dataset is not an angiosarcoma cohort.",
            source_key="geo",
            raw_record_id=off_topic_raw_id,
            dedupe_key="geo_accession:gse30723",
            identifiers={"geo_accession": "GSE30723"},
            metadata={
                "organism": "Homo sapiens",
                "sample_count": 24,
                "library_strategy": "Expression profiling by array",
                "sample_accessions": ["GSM762702"],
                "supplementary_file_types": ["CEL"],
            },
        ),
        off_topic_raw_id,
    )

    result = HSAResearchService(repo).build_omics_evidence_packets(
        OmicsEvidencePacketRequest(
            source_keys=["geo", "sra"],
            gene_symbols=["VIM", "vimentin"],
            min_datasets_per_packet=1,
        )
    )

    assert isinstance(result, OmicsEvidencePacketResult)
    assert result.scanned_dataset_count == 4
    assert result.selected_dataset_count == 4
    assert result.direct_dataset_count == 2
    assert result.analog_dataset_count == 1
    packets = {packet.packet_key: packet for packet in result.packets}
    assert {"canine_hsa", "human_angiosarcoma"} <= set(packets)
    canine_packet = packets["canine_hsa"]
    assert canine_packet.readiness == "ready_for_omics_review"
    assert canine_packet.direct_dataset_count == 2
    assert geo_object_id in {dataset.research_object_id for dataset in canine_packet.datasets}
    assert "GSE150705" in canine_packet.accessions
    assert "SRX31723468" in canine_packet.accessions
    assert "GSE30723" not in canine_packet.accessions
    assert "expression_matrix_or_raw_counts_required" in canine_packet.dispatch_blockers
    assert "VIM_or_vimentin_readout_computed_with_direction_and_effect_size" in canine_packet.quality_gates
    assert packets["human_angiosarcoma"].analog_dataset_count == 1


def test_omics_readout_contracts_reject_invalid_gene_set_key():
    with pytest.raises(ValidationError):
        OmicsGeneSetScore(gene_set_key="not_a_gene_set")  # type: ignore[arg-type]


def test_omics_readouts_map_platform_probe_ids_before_scoring(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "omics-probe-map.sqlite3", seed=False)
    matrix_text = "\n".join(
        [
            "ID_REF\tcontrol_1\thsa_tumor_1",
            "probe_vim\t4.0\t8.0",
            "probe_fn1\t4.0\t7.0",
            "probe_kdr\t3.0\t6.0",
            "probe_f3\t2.0\t5.0",
        ]
    )
    raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="geo",
            source_record_id="GSEPROBEMAP",
            content_hash="omics-probe-map-raw",
            raw_payload={"accession": "GSEPROBEMAP"},
        )
    )
    repo.upsert_research_object(
        ResearchObject(
            object_type=ResearchObjectType.DATASET,
            title="Canine hemangiosarcoma probe matrix with VIM expression",
            abstract="Canine hemangiosarcoma transcriptome evidence for VIM/vimentin expression.",
            source_key="geo",
            raw_record_id=raw_id,
            dedupe_key="geo_accession:gseprobemap",
            identifiers={"geo_accession": "GSEPROBEMAP"},
            metadata={
                "organism": "Canis lupus familiaris",
                "sample_count": 2,
                "sample_accessions": ["control_1", "hsa_tumor_1"],
                "supplementary_file_types": ["TSV"],
                "matrix_text": matrix_text,
                "platform_probe_map": {
                    "probe_vim": "VIM",
                    "probe_fn1": "FN1",
                    "probe_kdr": "KDR",
                    "probe_f3": "F3",
                },
            },
        ),
        raw_id,
    )

    result = HSAResearchService(repo).build_omics_readouts(
        OmicsReadoutRequest(packet_key="canine_hsa", accessions=["GSEPROBEMAP"], max_datasets=1)
    )

    dataset_result = result.datasets[0]
    assert dataset_result.target_expression is not None
    assert dataset_result.target_expression.detected is True
    assert dataset_result.target_expression.detected_gene_symbols == ["VIM"]
    gene_sets = {score.gene_set_key: score for score in dataset_result.gene_set_scores}
    assert gene_sets["mesenchymal_ecm"].detected_gene_symbols == ["VIM", "FN1"]
    assert gene_sets["angiogenesis_endothelial"].detected_gene_symbols == ["KDR"]
    assert gene_sets["coagulation_vascular_injury"].detected_gene_symbols == ["F3"]
    assert dataset_result.metadata["platform_probe_mapping_count"] == 4


def test_omics_readouts_map_explicit_gene_identifiers_before_scoring(tmp_path):
    repo = InMemoryResearchRepository()
    matrix_text = "\n".join(
        [
            "\tHSA_25mMGlc_1.genes.results\tHSA_0mMGlc_1.genes.results",
            "ENSCAFG00000004529\t7.5\t8.2",
            "ENSCAFG00000099999\t4.0\t5.0",
        ]
    )
    obj = ResearchObject(
        object_type=ResearchObjectType.DATASET,
        title="Canine HSA RNA-seq matrix with legacy Ensembl IDs",
        abstract="Canine hemangiosarcoma RNA-seq expression matrix.",
        source_key="geo",
        dedupe_key="geo_accession:gselegacyid",
        identifiers={"geo_accession": "GSELEGACYID"},
        metadata={
            "organism": "Canis lupus familiaris",
            "sample_count": 2,
            "sample_accessions": ["GSM1", "GSM2"],
            "supplementary_file_types": ["TSV"],
            "matrix_text": matrix_text,
            "gene_symbol_map": {"ENSCAFG00000004529": "VIM"},
        },
    )
    repo.research_objects[obj.id] = obj

    result = HSAResearchService(repo).build_omics_readouts(
        OmicsReadoutRequest(packet_key="canine_hsa", accessions=["GSELEGACYID"], max_datasets=1)
    )

    dataset_result = result.datasets[0]
    assert dataset_result.target_expression is not None
    assert dataset_result.target_expression.detected is True
    assert dataset_result.target_expression.detected_gene_symbols == ["VIM"]
    assert dataset_result.metadata["sample_roles"]["HSA_0mMGlc_1.genes.results"] == (
        "hsa_glucose_deprivation_cell_line"
    )


def test_omics_readouts_exclude_numeric_gene_metadata_columns(tmp_path):
    repo = InMemoryResearchRepository()
    matrix_text = "\n".join(
        [
            "Gene\tTarget\ttumor_1\ttumor_2\tENTREZ_GENE_ID\tNCBI_NAME",
            "VIM\tVIM_1\t8.0\t8.2\t7431\tvimentin",
            "FN1\tFN1_1\t7.0\t7.2\t2335\tfibronectin",
        ]
    )
    obj = ResearchObject(
        object_type=ResearchObjectType.DATASET,
        title="Human angiosarcoma targeted RNA-seq expression matrix",
        abstract="Human angiosarcoma targeted RNA-seq matrix with gene metadata columns.",
        source_key="geo",
        dedupe_key="geo_accession:gsetargetedmatrix",
        identifiers={"geo_accession": "GSETARGETEDMATRIX"},
        metadata={
            "organism": "Homo sapiens",
            "sample_count": 2,
            "sample_accessions": ["GSM1", "GSM2"],
            "supplementary_file_types": ["TXT"],
            "matrix_text": matrix_text,
        },
    )
    repo.research_objects[obj.id] = obj

    result = HSAResearchService(repo).build_omics_readouts(
        OmicsReadoutRequest(packet_key="human_angiosarcoma", accessions=["GSETARGETEDMATRIX"], max_datasets=1)
    )

    dataset_result = result.datasets[0]
    assert dataset_result.status == "computed"
    assert dataset_result.sample_count == 2
    assert set(dataset_result.sample_groups) == {"tumor_1", "tumor_2"}
    assert dataset_result.target_expression is not None
    assert dataset_result.target_expression.detected is True


def test_omics_readouts_parse_xlsx_processed_matrix_uri(tmp_path):
    repo = InMemoryResearchRepository()
    xlsx_path = tmp_path / "processed_matrix.xlsx"
    _write_minimal_xlsx(
        xlsx_path,
        [
            ["gene", "control_1", "control_2", "hsa_tumor_1", "hsa_tumor_2"],
            ["VIM", "4.0", "4.1", "8.0", "8.2"],
            ["FN1", "4.0", "4.2", "7.0", "7.2"],
            ["VEGFA", "3.0", "3.1", "6.0", "6.2"],
            ["F3", "2.0", "2.2", "5.0", "5.1"],
        ],
    )
    obj = ResearchObject(
        object_type=ResearchObjectType.DATASET,
        title="Canine hemangiosarcoma supplementary XLSX VIM expression",
        abstract="Canine hemangiosarcoma transcriptome evidence for VIM/vimentin expression.",
        source_key="geo",
        dedupe_key="geo_accession:gsexlsxmatrix",
        identifiers={"geo_accession": "GSEXLSXMATRIX"},
        metadata={
            "organism": "Canis lupus familiaris",
            "sample_count": 4,
            "sample_accessions": ["control_1", "control_2", "hsa_tumor_1", "hsa_tumor_2"],
            "supplementary_file_types": ["XLSX"],
        },
    )
    repo.research_objects[obj.id] = obj

    result = HSAResearchService(repo).build_omics_readouts(
        OmicsReadoutRequest(
            packet_key="canine_hsa",
            accessions=["GSEXLSXMATRIX"],
            matrix_uri_by_accession={"GSEXLSXMATRIX": str(xlsx_path)},
            max_datasets=1,
        )
    )

    assert result.computed_count == 1
    dataset_result = result.datasets[0]
    assert dataset_result.target_expression is not None
    assert dataset_result.target_expression.detected is True
    assert dataset_result.target_expression.support_level == "differential_support"


def test_omics_readouts_low_comparator_count_does_not_make_differential_claim(tmp_path):
    repo = InMemoryResearchRepository()
    xlsx_path = tmp_path / "low_comparator_matrix.xlsx"
    _write_minimal_xlsx(
        xlsx_path,
        [
            ["gene", "normal_endothelial_1", "hsa_tumor_1", "hsa_tumor_2"],
            ["VIM", "4.0", "8.0", "7.8"],
            ["FN1", "4.0", "7.0", "7.2"],
            ["VEGFA", "3.0", "6.0", "6.1"],
            ["F3", "2.0", "5.0", "5.2"],
        ],
    )
    obj = ResearchObject(
        object_type=ResearchObjectType.DATASET,
        title="Canine hemangiosarcoma tissue processed XLSX VIM expression",
        abstract="Canine hemangiosarcoma tumor tissue compared to normal endothelial context.",
        source_key="geo",
        dedupe_key="geo_accession:gselowcomparator",
        identifiers={"geo_accession": "GSELOWCOMPARATOR"},
        metadata={
            "organism": "Canis lupus familiaris",
            "sample_count": 3,
            "sample_accessions": ["normal_endothelial_1", "hsa_tumor_1", "hsa_tumor_2"],
            "supplementary_file_types": ["XLSX"],
        },
    )
    repo.research_objects[obj.id] = obj

    result = HSAResearchService(repo).build_omics_readouts(
        OmicsReadoutRequest(
            packet_key="canine_hsa",
            accessions=["GSELOWCOMPARATOR"],
            matrix_uri_by_accession={"GSELOWCOMPARATOR": str(xlsx_path)},
            max_datasets=1,
        )
    )

    dataset_result = result.datasets[0]
    assert dataset_result.target_expression is not None
    assert dataset_result.target_expression.support_level == "insufficient_labels"
    assert dataset_result.target_expression.effect_size is None
    assert "control_sample_count_low" in dataset_result.limitations
    assert "cell_line_expression_context_not_primary_tissue" not in dataset_result.limitations
    roles = dataset_result.metadata["sample_roles"]
    assert roles["hsa_tumor_1"] == "hsa_tumor_context"


def test_omics_readouts_repair_disease_comparator_sample_roles(tmp_path):
    repo = InMemoryResearchRepository()
    matrix_text = "\n".join(
        [
            "gene\tGSM_HSA_1\tGSM_HSA_2\tGSM_OS_1\tGSM_HEM_1",
            "VIM\t8.0\t8.2\t4.0\t4.1",
            "FN1\t7.0\t7.2\t4.0\t4.2",
        ]
    )
    obj = ResearchObject(
        object_type=ResearchObjectType.DATASET,
        title="Canine hemangiosarcoma and disease-comparator expression profiling",
        abstract="Canine hemangiosarcoma expression profiling with osteosarcoma and splenic hematoma comparators.",
        source_key="geo",
        dedupe_key="geo_accession:gsecomparator",
        identifiers={"geo_accession": "GSECOMPARATOR"},
        metadata={
            "organism": "Canis lupus familiaris",
            "sample_count": 4,
            "sample_accessions": ["GSM_HSA_1", "GSM_HSA_2", "GSM_OS_1", "GSM_HEM_1"],
            "sample_titles": {
                "GSM_HSA_1": "Golden Retriever hemangiosarcoma",
                "GSM_HSA_2": "Dalmatian hemangiosarcoma",
                "GSM_OS_1": "Golden Retriever osteosarcoma",
                "GSM_HEM_1": "Keeshond splenic hematoma",
            },
            "supplementary_file_types": ["TSV"],
            "matrix_text": matrix_text,
        },
    )
    repo.research_objects[obj.id] = obj

    result = HSAResearchService(repo).build_omics_readouts(
        OmicsReadoutRequest(packet_key="canine_hsa", accessions=["GSECOMPARATOR"], max_datasets=1)
    )

    dataset_result = result.datasets[0]
    assert dataset_result.sample_groups["GSM_HSA_1"] == "tumor"
    assert dataset_result.sample_groups["GSM_OS_1"] == "control"
    assert dataset_result.metadata["sample_roles"]["GSM_OS_1"] == "disease_comparator_control"
    assert dataset_result.metadata["comparison_design"] == "hsa_vs_other_disease_comparator"
    assert "disease_comparator_not_normal_control" in dataset_result.limitations


def test_omics_readouts_sample_title_zip_does_not_override_parsed_sample_names(tmp_path):
    repo = InMemoryResearchRepository()
    xlsx_path = tmp_path / "knockdown_matrix.xlsx"
    _write_minimal_xlsx(
        xlsx_path,
        [
            ["gene", "shA_1.genes.results", "scr1_1.genes.results"],
            ["VIM", "8.0", "4.0"],
        ],
    )
    obj = ResearchObject(
        object_type=ResearchObjectType.DATASET,
        title="Canine hemangiosarcoma supplementary XLSX VIM expression",
        abstract="Canine hemangiosarcoma transcriptome evidence for VIM/vimentin expression.",
        source_key="geo",
        dedupe_key="geo_accession:gseknockdownmatrix",
        identifiers={"geo_accession": "GSEKNOCKDOWNMATRIX"},
        metadata={
            "organism": "Canis lupus familiaris",
            "sample_count": 2,
            "sample_accessions": ["GSM1", "GSM2"],
            "sample_titles": ["scr1_1", "shA_1"],
            "supplementary_file_types": ["XLSX"],
        },
    )
    repo.research_objects[obj.id] = obj

    result = HSAResearchService(repo).build_omics_readouts(
        OmicsReadoutRequest(
            packet_key="canine_hsa",
            accessions=["GSEKNOCKDOWNMATRIX"],
            matrix_uri_by_accession={"GSEKNOCKDOWNMATRIX": str(xlsx_path)},
            max_datasets=1,
        )
    )

    groups = result.datasets[0].sample_groups
    assert groups["shA_1.genes.results"] == "tumor"
    assert groups["scr1_1.genes.results"] == "tumor"
    roles = result.datasets[0].metadata["sample_roles"]
    assert roles["shA_1.genes.results"] == "hsa_knockdown_cell_line"
    assert roles["scr1_1.genes.results"] == "hsa_scramble_control_cell_line"
    assert "perturbation_context_present_not_primary_tumor_normal" in result.datasets[0].limitations


def test_omics_readouts_skip_chro_seq_bigwig_for_locus_extractor():
    repo = InMemoryResearchRepository()
    obj = ResearchObject(
        object_type=ResearchObjectType.DATASET,
        title="Canine hemangiosarcoma ChRO-seq bigWig signal dataset",
        abstract="ChRO-seq signal files for canine hemangiosarcoma tumor tissue and normal tissue.",
        source_key="geo",
        dedupe_key="geo_accession:gsechrobw",
        identifiers={"geo_accession": "GSECHROBW"},
        metadata={
            "organism": "Canis lupus familiaris",
            "sample_count": 6,
            "library_strategy": "ChRO-seq",
            "sample_accessions": ["GSM1", "GSM2"],
            "supplementary_file_types": ["BW"],
        },
    )
    repo.research_objects[obj.id] = obj

    result = HSAResearchService(repo).build_omics_readouts(
        OmicsReadoutRequest(packet_key="canine_hsa", accessions=["GSECHROBW"], max_datasets=1)
    )

    assert result.computed_count == 0
    assert result.skipped_count == 1
    assert result.datasets[0].skipped_reason == "chro_seq_bigwig_locus_extraction_required"
    assert result.datasets[0].metadata["recommended_next_path"] == "bigwig_locus_signal_extractor"
    assert result.datasets[0].sample_count == 2
    assert result.datasets[0].metadata["locus_signal_metadata"]["runner_status"] == "recommend_only"


def test_omics_locus_signals_report_missing_pybigwig(monkeypatch):
    from hsa_research.ingestion_bridge import omics_locus_signals

    monkeypatch.setattr(omics_locus_signals, "_load_pybigwig", lambda: None)
    repo = InMemoryResearchRepository()
    obj = ResearchObject(
        object_type=ResearchObjectType.DATASET,
        title="Canine hemangiosarcoma ChRO-seq bigWig VIM signal dataset",
        abstract="Canine hemangiosarcoma ChRO-seq signal tracks for tumor and normal tissue.",
        source_key="geo",
        dedupe_key="geo_accession:gsemissingpybigwig",
        identifiers={"geo_accession": "GSEMISSINGPYBIGWIG"},
        metadata={
            "organism": "Canis lupus familiaris",
            "sample_count": 2,
            "library_strategy": "ChRO-seq",
            "sample_accessions": ["tumor_1", "normal_1"],
            "sample_titles": {
                "tumor_1": "canine HSA tumor",
                "normal_1": "canine normal tissue",
            },
            "supplementary_file_types": ["BW"],
        },
    )
    repo.research_objects[obj.id] = obj

    result = HSAResearchService(repo).build_omics_locus_signals(
        OmicsLocusSignalRequest(
            accessions=["GSEMISSINGPYBIGWIG"],
            bigwig_uri_by_sample={
                "tumor_1": {"minus": "file:///tmp/tumor_1_minus.bw"},
                "normal_1": {"minus": "file:///tmp/normal_1_minus.bw"},
            },
        )
    )

    assert result.computed_count == 0
    assert result.skipped_count == 1
    assert result.datasets[0].skipped_reason == "pybigwig_missing"
    assert "pybigwig_dependency_required" in result.datasets[0].limitations


def test_omics_followup_contracts_reject_invalid_task_type():
    task = OmicsFollowupTask(
        task_type="steady_state_expression",
        title="Find RNA expression",
        objective="Find steady-state RNA expression evidence.",
        rationale="ChRO-seq signal does not prove steady-state RNA abundance.",
        query_text="canine hemangiosarcoma VIM RNA-seq expression",
        source_keys=["GEO", "PubMed", "GEO"],
        target_genes=["vim", "VIM"],
    )

    assert task.source_keys == ["geo", "pubmed"]
    assert task.target_genes == ["VIM"]
    assert task.identity_key is not None

    with pytest.raises(ValidationError):
        OmicsFollowupTask(
            task_type="bad",
            title="Bad",
            objective="Bad objective",
            rationale="Bad rationale",
            query_text="bad query",
        )


def test_omics_followup_generator_creates_bounded_leads_and_queries():
    repo = InMemoryResearchRepository()
    locus_report = {
        "datasets": [
            {
                "dataset": {
                    "accession": "GSE150705",
                    "source_key": "geo",
                    "title": "Canine HSA ChRO-seq",
                },
                "sample_count": 8,
                "computed_sample_count": 8,
                "tumor_sample_count": 4,
                "control_sample_count": 4,
                "support_level": "differential_null",
                "tumor_control_delta": 0.0435,
                "effect_size": 0.2191,
                "comparison_p_value": 0.7566,
                "normalization_status": "not_verified",
                "limitations": [
                    "bigwig_locus_signal_extractor_first_pass",
                    "bigwig_normalization_not_verified",
                    "chro_seq_signal_not_steady_state_mrna",
                ],
                "metadata": {
                    "manifest_sample_count": 21,
                    "comparison_design": "primary_tumor_vs_normal_tissue",
                    "sample_groups": {
                        "GSM4556970": "tumor",
                        "GSM4556971": "control",
                    },
                },
            }
        ],
        "validation_agent_result": {
            "decision": "hold",
            "missing_evidence": [
                "Verified cross-sample bigWig normalization.",
                "Steady-state mRNA or protein-level VIM evidence.",
                "Human angiosarcoma cross-species comparator.",
            ],
        },
    }

    preview = HSAResearchService(repo).build_omics_followups(
        OmicsFollowupRequest(
            accessions=["GSE150705"],
            gene_symbols=["VIM"],
            omics_locus_signal_report=locus_report,
            max_tasks=6,
            dry_run=True,
        )
    )

    assert isinstance(preview, OmicsFollowupResult)
    assert preview.generated_task_count == 6
    assert preview.persisted_research_lead_count == 0
    task_types = {task.task_type for task in preview.tasks}
    assert "steady_state_expression" in task_types
    assert "protein_expression" in task_types
    assert "normalization_review" in task_types
    assert "sample_metadata_review" in task_types
    assert "cross_species_comparator" in task_types
    assert all("VIM" in task.target_genes for task in preview.tasks)

    applied = HSAResearchService(repo).build_omics_followups(
        OmicsFollowupRequest(
            accessions=["GSE150705"],
            gene_symbols=["VIM"],
            omics_locus_signal_report=locus_report,
            max_tasks=3,
            dry_run=False,
        )
    )

    assert applied.generated_task_count == 3
    assert applied.persisted_research_lead_count == 3
    assert applied.persisted_source_query_count >= 3
    assert all("omics_followup" in lead.topic_tags for lead in applied.research_leads)
    stored_leads = repo.list_research_leads(status="followup")
    assert len(stored_leads) == 3
    stored_queries = repo.list_source_queries(active_only=True)
    assert all(query.track == "omics_followup" for query in stored_queries)


def test_omics_readouts_skip_raw_sra_without_processed_matrix():
    repo = InMemoryResearchRepository()
    obj = ResearchObject(
        object_type=ResearchObjectType.DATASET,
        title="Primary canine hemangiosarcoma cells RNA-Seq VIM expression",
        abstract="RNA-seq run from canine hemangiosarcoma primary cells with vimentin context.",
        source_key="sra",
        dedupe_key="sra_experiment:srxrawonly",
        identifiers={"sra_experiment": "SRXRAWONLY"},
        metadata={
            "organism": "Canis lupus familiaris",
            "sample_count": 1,
            "library_strategy": "RNA-seq",
            "run_accessions": ["SRRRAWONLY"],
            "sample_accessions": ["SRSRAWONLY"],
        },
    )
    repo.research_objects[obj.id] = obj

    result = HSAResearchService(repo).build_omics_readouts(
        OmicsReadoutRequest(packet_key="canine_hsa", accessions=["SRXRAWONLY"])
    )

    assert result.computed_count == 0
    assert result.skipped_count == 1
    assert result.datasets[0].skipped_reason == "raw_sra_reprocessing_required"
    assert "raw_sra_reprocessing_required" in result.datasets[0].limitations
