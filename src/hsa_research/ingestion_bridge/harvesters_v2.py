"""Fresh harvester v2 layer with required comparative oncology coverage.

These harvesters are the new ingestion bridge surface. Scholarly sources always
expand searches to include canine HSA plus human angiosarcoma and close vascular
sarcoma analogs unless a caller explicitly disables the comparative policy.
"""

from __future__ import annotations

import json
import os
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from html import unescape
from typing import Any

from .contracts import HarvestedRecord, RawSourceRecord, ResearchObject, ResearchObjectType
from .local_store import stable_json_hash
from .query_policy import (
    ComparativeQueryStyle,
    expand_with_comparative_policy,
    infer_comparative_scope,
)

USER_AGENT = "hsa-autoresearch-ingestion-bridge-v2/0.2 (mailto:poppa@bradyandgraffiti.com)"

CHEMBL_PRIORITY_TARGETS: dict[str, dict[str, str]] = {
    "CHEMBL213": {"gene": "ADRB1", "category": "beta_adrenergic"},
    "CHEMBL210": {"gene": "ADRB2", "category": "beta_adrenergic"},
    "CHEMBL2289": {"gene": "ADRB2", "category": "beta_adrenergic", "species": "canine"},
    "CHEMBL279": {"gene": "KDR", "category": "vegf_angiogenesis"},
    "CHEMBL1955": {"gene": "FLT4", "category": "vegf_angiogenesis"},
    "CHEMBL2095227": {"gene": "VEGFR", "category": "vegf_angiogenesis"},
    "CHEMBL1936": {"gene": "KIT", "category": "rtk"},
    "CHEMBL5303563": {"gene": "KIT", "category": "rtk", "species": "canine"},
    "CHEMBL2007": {"gene": "PDGFRA", "category": "rtk"},
    "CHEMBL1913": {"gene": "PDGFRB", "category": "rtk"},
    "CHEMBL5303562": {"gene": "PDGFRB", "category": "rtk", "species": "canine"},
    "CHEMBL1974": {"gene": "FLT3", "category": "rtk"},
    "CHEMBL1844": {"gene": "CSF1R", "category": "rtk"},
    "CHEMBL2842": {"gene": "MTOR", "category": "pi3k_mtor"},
    "CHEMBL325": {"gene": "HDAC1", "category": "epigenetic"},
    "CHEMBL1937": {"gene": "HDAC2", "category": "epigenetic"},
    "CHEMBL1829": {"gene": "HDAC3", "category": "epigenetic"},
    "CHEMBL1865": {"gene": "HDAC6", "category": "epigenetic"},
    "CHEMBL3192": {"gene": "HDAC8", "category": "epigenetic"},
    "CHEMBL1806": {"gene": "TOP2A", "category": "cytotoxic"},
    "CHEMBL3396": {"gene": "TOP2B", "category": "cytotoxic"},
    "CHEMBL2094255": {"gene": "TOP2", "category": "cytotoxic"},
    "CHEMBL3832941": {"gene": "TUBA", "category": "microtubule"},
    "CHEMBL3832942": {"gene": "TUBB", "category": "microtubule"},
}
CHEMBL_PRIORITY_TARGET_IDS = tuple(CHEMBL_PRIORITY_TARGETS)
CHEMBL_PRIORITY_TARGET_ORGANISMS = ("Homo sapiens", "Canis lupus familiaris")
CHEMBL_PRIORITY_STANDARD_TYPES = ("IC50", "Ki", "Kd", "EC50")
CHEMBL_PRIORITY_ASSAY_TYPES = ("B", "F")
CHEMBL_PRIORITY_CELL_LINE_TERMS = (
    "angiosarcoma",
    "hemangiosarcoma",
    "haemangiosarcoma",
    "sarcoma",
    "endothelial",
    "vascular",
    "canine",
    "dog",
)
CHEMBL_PRIORITY_CELL_LINE_STANDARD_TYPES = ("IC50", "EC50")

STRUCTURED_TARGET_GATES: dict[str, dict[str, object]] = {
    "VEGFA": {
        "category": "vegf_angiogenesis",
        "aliases": ("vegfa", "vegf-a", "vascular endothelial growth factor a"),
    },
    "KDR": {
        "category": "vegf_angiogenesis",
        "aliases": (
            "kdr",
            "vegfr2",
            "vegfr-2",
            "vascular endothelial growth factor receptor 2",
            "vascular endothelial growth factor receptor-2",
        ),
    },
    "FLT4": {
        "category": "vegf_angiogenesis",
        "aliases": (
            "flt4",
            "vegfr3",
            "vegfr-3",
            "vascular endothelial growth factor receptor 3",
            "vascular endothelial growth factor receptor-3",
        ),
    },
    "KIT": {
        "category": "rtk",
        "aliases": ("kit", "c-kit", "mast/stem cell growth factor receptor kit", "stem cell growth factor receptor kit"),
    },
    "MTOR": {"category": "pi3k_mtor", "aliases": ("mtor", "mechanistic target of rapamycin", "mammalian target of rapamycin")},
    "CD47": {"category": "immune_checkpoint", "aliases": ("cd47", "leukocyte surface antigen cd47")},
    "SIRPA": {"category": "immune_checkpoint", "aliases": ("sirpa", "sirp-alpha", "sirp alpha", "signal regulatory protein alpha")},
    "TP53": {"category": "tumor_suppressor", "aliases": ("tp53", "p53", "cellular tumor antigen p53")},
}


class HarvesterV2(ABC):
    """Source-specific v2 harvester contract."""

    source_key: str

    @abstractmethod
    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        """Fetch and normalize source records."""

    def text_for_chunking(self, record: HarvestedRecord) -> str:
        """Return the legally storable text for document chunking."""

        return "\n\n".join(
            part for part in (record.research_object.title, record.research_object.abstract) if part
        )

    def chunk_section_label(self, record: HarvestedRecord) -> str:
        """Return the source-specific section label for produced chunks."""

        _ = record
        return "title_abstract"


class ScholarlyHarvesterV2(HarvesterV2):
    """Base class for scholarly metadata harvesters with comparative expansion."""

    query_style: ComparativeQueryStyle = "generic"

    def prepare_query(self, query_text: str, params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        params = dict(params)
        policy = params.pop("comparative_policy", "required")
        if policy == "disabled":
            return query_text, params
        return expand_with_comparative_policy(query_text, self.query_style), params

    def filter_relevant(
        self,
        records: list[HarvestedRecord],
        params: dict[str, Any],
    ) -> list[HarvestedRecord]:
        if not params.pop("require_policy_match", True):
            return records
        return [
            record
            for record in records
            if record.research_object.metadata.get("ingestion_policy", {}).get("matched_concepts")
        ]


class OpenAlexHarvesterV2(ScholarlyHarvesterV2):
    source_key = "openalex"
    query_style = "openalex"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        query_text, params = self.prepare_query(query_text, params)
        data = _get_json(
            "https://api.openalex.org/works",
            {
                "search": query_text,
                "per-page": min(limit, 200),
                **params,
            },
        )
        return self.filter_relevant([self.normalize(item) for item in data.get("results", [])], params)

    def normalize(self, item: dict[str, Any]) -> HarvestedRecord:
        source_id = item.get("id")
        doi = _normalize_doi(item.get("doi"))
        ids = item.get("ids") or {}
        identifiers = {
            "openalex_id": source_id,
            "doi": doi,
            "pmid": _strip_url_id(ids.get("pmid")),
            "pmcid": _strip_url_id(ids.get("pmcid")),
        }
        identifiers = {key: value for key, value in identifiers.items() if value}
        primary_location = item.get("primary_location") or {}
        landing_page = primary_location.get("landing_page_url") or item.get("doi") or source_id
        abstract = _openalex_abstract(item.get("abstract_inverted_index"))
        raw = _raw_record(self.source_key, source_id, landing_page, item)
        obj = ResearchObject(
            object_type=ResearchObjectType.PUBLICATION,
            title=item.get("title"),
            abstract=abstract,
            canonical_url=landing_page,
            publication_year=item.get("publication_year"),
            published_at=item.get("publication_date"),
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, source_id),
            identifiers=identifiers,
            metadata={
                "source_display_name": (primary_location.get("source") or {}).get("display_name"),
                "cited_by_count": item.get("cited_by_count"),
                "type": item.get("type"),
                "open_access": item.get("open_access"),
                "ingestion_policy": infer_comparative_scope(item.get("title"), abstract),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)


class CrossrefHarvesterV2(ScholarlyHarvesterV2):
    source_key = "crossref"
    query_style = "crossref"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        query_text, params = self.prepare_query(query_text, params)
        data = _get_json(
            "https://api.crossref.org/works",
            {
                "query": query_text,
                "rows": min(limit, 100),
                **params,
            },
        )
        items = (data.get("message") or {}).get("items", [])
        return self.filter_relevant([self.normalize(item) for item in items], params)

    def normalize(self, item: dict[str, Any]) -> HarvestedRecord:
        doi = _normalize_doi(item.get("DOI"))
        title = _clean_markup(_first(item.get("title")))
        abstract = _clean_markup(item.get("abstract"))
        published = _crossref_date(item)
        identifiers = {"doi": doi, "source_id": doi}
        identifiers = {key: value for key, value in identifiers.items() if value}
        source_url = item.get("URL") or (f"https://doi.org/{doi}" if doi else None)
        raw = _raw_record(self.source_key, doi, source_url, item)
        obj = ResearchObject(
            object_type=ResearchObjectType.PUBLICATION,
            title=title,
            abstract=abstract,
            canonical_url=source_url,
            publication_year=_year_from_date(published),
            published_at=published,
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, doi or title),
            identifiers=identifiers,
            metadata={
                "container_title": _first(item.get("container-title")),
                "publisher": item.get("publisher"),
                "type": item.get("type"),
                "license": item.get("license"),
                "is_referenced_by_count": item.get("is-referenced-by-count"),
                "ingestion_policy": infer_comparative_scope(title, abstract),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)


class EuropePMCHarvesterV2(ScholarlyHarvesterV2):
    source_key = "europe_pmc"
    query_style = "europe_pmc"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        query_text, params = self.prepare_query(query_text, params)
        open_access = params.pop("open_access", False)
        fetch_full_text = params.pop("fetch_full_text", open_access)
        if open_access and "OPEN_ACCESS:" not in query_text.upper():
            query_text = f"({query_text}) AND OPEN_ACCESS:Y"
        data = _get_json(
            "https://www.ebi.ac.uk/europepmc/webservices/rest/search",
            {
                "query": query_text,
                "format": params.pop("format", "json"),
                "resultType": params.pop("resultType", "core"),
                "pageSize": min(limit, 1000),
                **params,
            },
        )
        items = ((data.get("resultList") or {}).get("result")) or []
        records = [
            self.normalize(
                item,
                full_text_xml=_europe_pmc_full_text_xml(_europe_pmc_full_text_id(item)) if fetch_full_text else None,
            )
            for item in items
        ]
        return self.filter_relevant(records, params)

    def filter_relevant(
        self,
        records: list[HarvestedRecord],
        params: dict[str, Any],
    ) -> list[HarvestedRecord]:
        if not params.pop("require_policy_match", True):
            return records
        return [record for record in records if _record_matches_policy(record)]

    def normalize(self, item: dict[str, Any], *, full_text_xml: str | None = None) -> HarvestedRecord:
        pmid = item.get("pmid")
        pmcid = item.get("pmcid")
        doi = _normalize_doi(item.get("doi"))
        source_id = item.get("id") or pmid or pmcid or doi
        identifiers = {
            "pmid": pmid,
            "pmcid": pmcid,
            "doi": doi,
            "source_id": source_id,
        }
        identifiers = {key: value for key, value in identifiers.items() if value}
        source_url = _europe_pmc_url(item)
        title = _clean_markup(item.get("title"))
        abstract = _clean_markup(item.get("abstractText"))
        full_text = _jats_full_text(full_text_xml) if full_text_xml else None
        title_abstract_policy = infer_comparative_scope(title, abstract)
        body_policy = infer_comparative_scope(None, full_text[:8000] if full_text else None)
        payload = item | {
            "full_text_xml": full_text_xml,
            "full_text": full_text,
        }
        raw = _raw_record(self.source_key, source_id, source_url, payload)
        obj = ResearchObject(
            object_type=ResearchObjectType.PREPRINT if item.get("source") == "PPR" else ResearchObjectType.PUBLICATION,
            title=title,
            abstract=abstract,
            canonical_url=source_url,
            publication_year=_safe_int(item.get("pubYear")),
            published_at=item.get("firstPublicationDate") or item.get("journalInfo", {}).get("dateOfPublication"),
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, source_id),
            identifiers=identifiers,
            metadata={
                "journal": item.get("journalTitle"),
                "author_string": item.get("authorString"),
                "is_open_access": item.get("isOpenAccess"),
                "cited_by_count": item.get("citedByCount"),
                "source": item.get("source"),
                "license": item.get("license"),
                "license_policy": "metadata_plus_open_access_when_licensed",
                "full_text_available": bool(full_text),
                "ingestion_policy": title_abstract_policy,
                "body_ingestion_policy": body_policy,
                "body_only_match": bool(body_policy["matched_concepts"])
                and not bool(title_abstract_policy["matched_concepts"]),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)

    def text_for_chunking(self, record: HarvestedRecord) -> str:
        return "\n\n".join(
            part
            for part in (
                record.research_object.title,
                record.research_object.abstract,
                record.raw_record.raw_payload.get("full_text"),
            )
            if part
        )

    def chunk_section_label(self, record: HarvestedRecord) -> str:
        return "full_text" if record.raw_record.raw_payload.get("full_text") else "title_abstract"


class PubMedHarvesterV2(ScholarlyHarvesterV2):
    source_key = "pubmed"
    query_style = "pubmed"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        query_text, params = self.prepare_query(query_text, params)
        search = _get_json(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            {
                "db": "pubmed",
                "term": query_text,
                "retmode": "json",
                "retmax": min(limit, 200),
                **params,
            },
        )
        ids = (search.get("esearchresult") or {}).get("idlist", [])
        if not ids:
            return []
        xml = _get_text(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
            {"db": "pubmed", "id": ",".join(ids), "retmode": "xml"},
        )
        records = [self.normalize(article) for article in ET.fromstring(xml).findall(".//PubmedArticle")]
        return self.filter_relevant(records, params)

    def normalize(self, article: ET.Element) -> HarvestedRecord:
        pmid = _xml_text(article, ".//MedlineCitation/PMID")
        title = _xml_join_text(article.find(".//ArticleTitle"))
        abstract = " ".join(
            text
            for node in article.findall(".//Abstract/AbstractText")
            if (text := _xml_join_text(node))
        )
        doi = None
        pmcid = None
        for article_id in article.findall(".//ArticleIdList/ArticleId"):
            id_type = article_id.attrib.get("IdType")
            if id_type == "doi":
                doi = _normalize_doi(article_id.text)
            elif id_type == "pmc":
                pmcid = article_id.text

        identifiers = {"pmid": pmid, "doi": doi, "pmcid": pmcid, "source_id": pmid}
        identifiers = {key: value for key, value in identifiers.items() if value}
        pub_date = _pubmed_date(article)
        source_url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else None
        payload = _xml_to_dictish(article)
        raw = _raw_record(self.source_key, pmid, source_url, payload)
        obj = ResearchObject(
            object_type=ResearchObjectType.PUBLICATION,
            title=title,
            abstract=abstract or None,
            canonical_url=source_url,
            publication_year=_year_from_date(pub_date),
            published_at=pub_date,
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, pmid),
            identifiers=identifiers,
            metadata={
                "journal": _xml_text(article, ".//Journal/Title"),
                "publication_types": [
                    node.text for node in article.findall(".//PublicationTypeList/PublicationType") if node.text
                ],
                "mesh_terms": [
                    node.text for node in article.findall(".//MeshHeading/DescriptorName") if node.text
                ],
                "ingestion_policy": infer_comparative_scope(title, abstract),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)


class PMCOAHarvesterV2(ScholarlyHarvesterV2):
    """PMC Open Access full-text harvester using license-aware OAI-PMH records."""

    source_key = "pmc_oa"
    query_style = "pubmed"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        query_text, params = self.prepare_query(query_text, params)
        license_required = params.pop("license_required", True)
        skip_retracted = params.pop("skip_retracted", True)
        if license_required and "open access" not in query_text.lower():
            query_text = f"({query_text}) AND \"open access\"[filter]"
        require_policy_match = params.get("require_policy_match", True)
        candidate_limit = min(max(limit * 50, 100), 200)
        search = _get_json(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            {
                "db": "pmc",
                "term": query_text,
                "retmode": "json",
                "retmax": candidate_limit,
                **params,
            },
        )
        ids = (search.get("esearchresult") or {}).get("idlist", [])
        records: list[HarvestedRecord] = []
        for pmc_numeric_id in ids:
            if len(records) >= limit:
                break
            pmcid = _normalize_pmcid(pmc_numeric_id)
            try:
                oa_metadata = _pmc_oa_metadata(pmcid)
            except (ET.ParseError, RuntimeError, ValueError):
                continue
            if not oa_metadata:
                continue
            if license_required and not oa_metadata.get("oa_license"):
                continue
            if skip_retracted and oa_metadata.get("retracted") == "yes":
                continue
            try:
                xml = _get_text(
                    "https://pmc.ncbi.nlm.nih.gov/api/oai/v1/mh/",
                    {
                        "verb": "GetRecord",
                        "identifier": f"oai:pubmedcentral.nih.gov:{_pmcid_numeric(pmcid)}",
                        "metadataPrefix": "pmc",
                    },
                )
                record = self.normalize(xml, pmcid=pmcid, oa_metadata=oa_metadata, source_query=query_text)
            except (ET.ParseError, RuntimeError, ValueError):
                continue
            if require_policy_match and not _pmc_record_matches_policy(record):
                continue
            records.append(record)
            time.sleep(0.1)
        return self.filter_relevant(records, params)

    def filter_relevant(
        self,
        records: list[HarvestedRecord],
        params: dict[str, Any],
    ) -> list[HarvestedRecord]:
        if not params.pop("require_policy_match", True):
            return records
        return [record for record in records if _pmc_record_matches_policy(record)]

    def normalize(
        self,
        xml_text: str,
        *,
        pmcid: str | None = None,
        oa_metadata: dict[str, Any] | None = None,
        source_query: str | None = None,
    ) -> HarvestedRecord:
        root = ET.fromstring(xml_text)
        error = _find_first(root, ".//{*}error")
        if error is not None:
            raise ValueError(_xml_join_text(error) or "PMC OAI-PMH returned an error")

        article = _find_first(root, ".//{*}metadata/{*}article")
        if article is None and _local_name(root.tag) == "article":
            article = root
        if article is None:
            raise ValueError("PMC OAI-PMH response did not include a JATS article")

        identifiers = _jats_article_ids(article)
        if pmcid:
            identifiers["pmcid"] = pmcid
        elif identifiers.get("pmc"):
            identifiers["pmcid"] = _normalize_pmcid(identifiers.pop("pmc"))
        elif identifiers.get("pmcid"):
            identifiers["pmcid"] = _normalize_pmcid(identifiers["pmcid"])
        pmcid = identifiers.get("pmcid")

        title = _clean_markup(_xml_join_text(_find_first(article, ".//{*}article-title")))
        abstract = _clean_multiline_text(
            "\n\n".join(
                text
                for node in article.findall(".//{*}abstract")
                if (text := _xml_join_text(node))
            )
        )
        full_text = _clean_multiline_text(_xml_join_text(_find_first(article, ".//{*}body")))
        pub_date = _jats_pub_date(article)
        source_url = f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/" if pmcid else None
        license_info = {
            **(oa_metadata or {}),
            **_jats_license(article),
        }
        payload = {
            "pmcid": pmcid,
            "source_query": source_query,
            "license": license_info,
            "article_xml": xml_text,
            "full_text": full_text,
            "abstract": abstract,
        }
        raw = _raw_record(self.source_key, pmcid, source_url, payload)
        title_abstract_policy = infer_comparative_scope(title, abstract)
        body_policy = infer_comparative_scope(None, full_text[:8000] if full_text else None)
        obj = ResearchObject(
            object_type=ResearchObjectType.PUBLICATION,
            title=title,
            abstract=abstract,
            canonical_url=source_url,
            publication_year=_year_from_date(pub_date),
            published_at=pub_date,
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, pmcid or title),
            identifiers={key: value for key, value in identifiers.items() if value},
            metadata={
                "journal": _clean_markup(_xml_join_text(_find_first(article, ".//{*}journal-title"))),
                "article_type": article.attrib.get("article-type"),
                "license": license_info,
                "license_policy": "store_only_when_license_allows",
                "full_text_available": bool(full_text),
                "ingestion_policy": title_abstract_policy,
                "body_ingestion_policy": body_policy,
                "body_only_match": bool(body_policy["matched_concepts"])
                and not bool(title_abstract_policy["matched_concepts"]),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)

    def text_for_chunking(self, record: HarvestedRecord) -> str:
        return "\n\n".join(
            part
            for part in (
                record.research_object.title,
                record.research_object.abstract,
                record.raw_record.raw_payload.get("full_text"),
            )
            if part
        )

    def chunk_section_label(self, record: HarvestedRecord) -> str:
        _ = record
        return "full_text"


class ClinicalTrialsGovHarvesterV2(HarvesterV2):
    """ClinicalTrials.gov API v2 harvester for human vascular sarcoma analog trials."""

    source_key = "clinicaltrials_gov"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        params = dict(params)
        search_area = params.pop("search_area", "term")
        query_param = "query.cond" if search_area == "condition" else "query.term"
        data = _get_json(
            "https://clinicaltrials.gov/api/v2/studies",
            {
                query_param: query_text,
                "pageSize": min(limit, 1000),
                "format": "json",
                **params,
            },
        )
        records = [self.normalize(study) for study in data.get("studies", [])]
        return [
            record
            for record in records
            if record.research_object.metadata.get("ingestion_policy", {}).get("matched_concepts")
        ]

    def normalize(self, study: dict[str, Any]) -> HarvestedRecord:
        protocol = study.get("protocolSection") or {}
        identification = protocol.get("identificationModule") or {}
        status = protocol.get("statusModule") or {}
        description = protocol.get("descriptionModule") or {}
        conditions_module = protocol.get("conditionsModule") or {}
        design = protocol.get("designModule") or {}
        sponsors = protocol.get("sponsorCollaboratorsModule") or {}
        eligibility = protocol.get("eligibilityModule") or {}
        contacts_locations = protocol.get("contactsLocationsModule") or {}
        arms_interventions = protocol.get("armsInterventionsModule") or {}
        outcomes = protocol.get("outcomesModule") or {}

        nct_id = identification.get("nctId")
        title = identification.get("briefTitle") or identification.get("officialTitle")
        brief_summary = _clean_markup(description.get("briefSummary"))
        detailed_description = _clean_markup(description.get("detailedDescription"))
        conditions = _string_list(conditions_module.get("conditions"))
        interventions = _clinical_trial_interventions(arms_interventions)
        primary_outcomes = _clinical_trial_outcomes(outcomes.get("primaryOutcomes"))
        secondary_outcomes = _clinical_trial_outcomes(outcomes.get("secondaryOutcomes"))
        lead_sponsor = (sponsors.get("leadSponsor") or {}).get("name")
        start_date = _date_struct_value(status.get("startDateStruct"))
        first_posted = _date_struct_value(status.get("studyFirstPostDateStruct"))
        completion_date = _date_struct_value(status.get("completionDateStruct"))
        enrollment = design.get("enrollmentInfo") or {}
        identifiers = {
            "nct_id": nct_id,
            "source_id": nct_id,
            "org_study_id": ((identification.get("orgStudyIdInfo") or {}).get("id")),
        }
        identifiers = {key: value for key, value in identifiers.items() if value}
        source_url = f"https://clinicaltrials.gov/study/{nct_id}" if nct_id else None
        policy_text = " ".join(
            part
            for part in (
                title,
                brief_summary,
                detailed_description,
                " ".join(conditions),
            )
            if part
        )

        raw = _raw_record(self.source_key, nct_id, source_url, study)
        obj = ResearchObject(
            object_type=ResearchObjectType.CLINICAL_TRIAL,
            title=title,
            abstract=brief_summary,
            canonical_url=source_url,
            publication_year=_year_from_date(first_posted or start_date),
            published_at=first_posted or start_date,
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, nct_id or title),
            identifiers=identifiers,
            metadata={
                "official_title": identification.get("officialTitle"),
                "overall_status": status.get("overallStatus"),
                "has_results": bool(status.get("resultsFirstPostDateStruct")),
                "start_date": start_date,
                "completion_date": completion_date,
                "study_type": design.get("studyType"),
                "phases": _string_list(design.get("phases")),
                "enrollment": enrollment.get("count"),
                "enrollment_type": enrollment.get("type"),
                "conditions": conditions,
                "interventions": interventions,
                "primary_outcomes": primary_outcomes,
                "secondary_outcomes": secondary_outcomes,
                "lead_sponsor": lead_sponsor,
                "collaborators": [
                    collaborator.get("name")
                    for collaborator in sponsors.get("collaborators", [])
                    if collaborator.get("name")
                ],
                "minimum_age": eligibility.get("minimumAge"),
                "maximum_age": eligibility.get("maximumAge"),
                "sex": eligibility.get("sex"),
                "standard_ages": _string_list(eligibility.get("stdAges")),
                "locations": _clinical_trial_locations(contacts_locations),
                "ingestion_policy": infer_comparative_scope(title, policy_text),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)

    def text_for_chunking(self, record: HarvestedRecord) -> str:
        protocol = record.raw_record.raw_payload.get("protocolSection") or {}
        description = protocol.get("descriptionModule") or {}
        conditions = record.research_object.metadata.get("conditions", [])
        interventions = record.research_object.metadata.get("interventions", [])
        primary_outcomes = record.research_object.metadata.get("primary_outcomes", [])
        eligibility = protocol.get("eligibilityModule") or {}
        return "\n\n".join(
            part
            for part in (
                record.research_object.title,
                record.research_object.abstract,
                _clean_markup(description.get("detailedDescription")),
                f"Conditions: {'; '.join(conditions)}" if conditions else None,
                f"Interventions: {'; '.join(interventions)}" if interventions else None,
                f"Primary outcomes: {'; '.join(primary_outcomes)}" if primary_outcomes else None,
                _clean_markup(eligibility.get("eligibilityCriteria")),
            )
            if part
        )

    def chunk_section_label(self, record: HarvestedRecord) -> str:
        _ = record
        return "clinical_trial_record"


class AVMAVCTRHarvesterV2(HarvesterV2):
    """AVMA Veterinary Clinical Trials Registry public JSON harvester."""

    source_key = "avma_vctr"
    base_url = "https://veterinaryclinicaltrials.org/"
    api_url = "https://veterinaryclinicaltrials.org/avma/studies/search/json/"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        params = dict(params)
        require_policy_match = params.pop("require_policy_match", True)
        take = min(_safe_int(params.pop("take", limit)) or limit, limit, 100)
        skip_similar = params.pop("skip_similar_studies", True)
        if isinstance(skip_similar, bool):
            skip_similar = str(skip_similar).lower()
        extra_aggregations = params.pop("extra_aggregations", "[]")
        if isinstance(extra_aggregations, (dict, list, tuple)):
            extra_aggregations = json.dumps(extra_aggregations)

        data = _get_json(
            self.api_url,
            {
                "search": query_text,
                "skip": params.pop("skip", 0),
                "take": take,
                "sort_by": params.pop("sort_by", "score"),
                "skip_similar_studies": skip_similar,
                "extra_aggregations": extra_aggregations,
                **params,
            },
        )
        studies = data.get("studies") or data.get("results") or []
        records = [
            self.normalize(study, source_query=query_text, source_total=_safe_int(data.get("total")))
            for study in studies
            if isinstance(study, dict)
        ]
        if not require_policy_match:
            return records
        return [
            record
            for record in records
            if record.research_object.metadata.get("ingestion_policy", {}).get("matched_concepts")
        ]

    def normalize(
        self,
        study: dict[str, Any],
        *,
        source_query: str | None = None,
        source_total: int | None = None,
    ) -> HarvestedRecord:
        avma_id = str(study.get("id")) if study.get("id") is not None else None
        vct_code = _clean_markup(study.get("vct_code"))
        source_record_id = vct_code or avma_id
        source_url = self._absolute_url(study.get("absolute_url"))
        title = _clean_markup(study.get("name")) or _clean_markup(study.get("title"))
        abstract = _clean_markup(study.get("description")) or _clean_markup(study.get("tagline"))
        visible_categories = self._visible_search_categories(study.get("visible_sc_items"))
        species = self._category_values(visible_categories, ("Species",))
        conditions = self._category_values(
            visible_categories,
            ("Condition", "Conditions", "Diagnosis", "Oncology"),
        )
        intervention_types = self._category_values(
            visible_categories,
            ("Intervention type", "Intervention Type", "Intervention"),
        )
        financial_incentives = self._category_values(visible_categories, ("Financial incentive",))
        policy_text = " ".join(
            part
            for part in (
                title,
                _clean_markup(study.get("tagline")),
                " ".join(species),
                " ".join(conditions),
                " ".join(intervention_types),
                abstract[:600] if abstract else None,
            )
            if part
        )
        identifiers = {
            "vct_code": vct_code,
            "avma_study_id": avma_id,
            "source_id": source_record_id,
        }
        identifiers = {key: value for key, value in identifiers.items() if value}

        raw = _raw_record(self.source_key, source_record_id, source_url, study)
        obj = ResearchObject(
            object_type=ResearchObjectType.VETERINARY_TRIAL,
            title=title,
            abstract=abstract,
            canonical_url=source_url,
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, source_record_id or title),
            identifiers=identifiers,
            metadata={
                "vct_code": vct_code,
                "avma_study_id": avma_id,
                "tagline": _clean_markup(study.get("tagline")),
                "status": study.get("status"),
                "status_color": study.get("status_color"),
                "study_type": study.get("study_type"),
                "target_gender": study.get("target_gender"),
                "age_range": study.get("age_range"),
                "top_investigator": study.get("top_investigator"),
                "thumbnail": study.get("thumbnail"),
                "distance_to_location": study.get("distance_to_location"),
                "is_similar": study.get("is_similar"),
                "species": species,
                "conditions": conditions,
                "intervention_types": intervention_types,
                "financial_incentives": financial_incentives,
                "visible_search_categories": visible_categories,
                "source_query": source_query,
                "source_total": source_total,
                "source_endpoint": self.api_url,
                "license_policy": "link_and_registry_metadata",
                "ingestion_policy": infer_comparative_scope(title, policy_text),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)

    def text_for_chunking(self, record: HarvestedRecord) -> str:
        metadata = record.research_object.metadata
        categories = metadata.get("visible_search_categories") or {}
        category_lines = [
            f"{category}: {'; '.join(values)}"
            for category, values in categories.items()
            if isinstance(values, list) and values
        ]
        return "\n\n".join(
            part
            for part in (
                record.research_object.title,
                record.research_object.abstract,
                f"VCT code: {metadata.get('vct_code')}" if metadata.get("vct_code") else None,
                f"Status: {metadata.get('status')}" if metadata.get("status") else None,
                f"Study type: {metadata.get('study_type')}" if metadata.get("study_type") else None,
                f"Top investigator: {metadata.get('top_investigator')}"
                if metadata.get("top_investigator")
                else None,
                "\n".join(category_lines) if category_lines else None,
            )
            if part
        )

    def chunk_section_label(self, record: HarvestedRecord) -> str:
        _ = record
        return "veterinary_trial_record"

    @classmethod
    def _absolute_url(cls, value: str | None) -> str | None:
        if not value:
            return None
        return urllib.parse.urljoin(cls.base_url, str(value))

    @staticmethod
    def _visible_search_categories(items: Any) -> dict[str, list[str]]:
        if not isinstance(items, list):
            return {}
        categories: dict[str, list[str]] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            parent = _clean_markup(item.get("parent_label") or item.get("parent") or item.get("category"))
            label = _clean_markup(item.get("label") or item.get("name"))
            if not parent or not label:
                continue
            categories.setdefault(parent, [])
            if label not in categories[parent]:
                categories[parent].append(label)
        return categories

    @staticmethod
    def _category_values(categories: dict[str, list[str]], names: tuple[str, ...]) -> list[str]:
        wanted = {name.lower() for name in names}
        values: list[str] = []
        for category, labels in categories.items():
            if category.lower() not in wanted:
                continue
            for label in labels:
                if label not in values:
                    values.append(label)
        return values


class ICDCHarvesterV2(HarvesterV2):
    """Integrated Canine Data Commons GraphQL harvester for canine HSA case metadata."""

    source_key = "icdc"
    graphql_url = "https://caninecommons.cancer.gov/v1/graphql/"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        diagnoses = params.pop("diagnosis", None) or [query_text]
        data = _post_json(
            self.graphql_url,
            {
                "query": """
                query CaseOverview($diagnosis: [String], $first: Int, $offset: Int) {
                  caseOverview(diagnosis: $diagnosis, first: $first, offset: $offset, order_by: "case_id", sort_direction: "asc") {
                    case_id
                    study_code
                    study_type
                    cohort
                    breed
                    diagnosis
                    stage_of_disease
                    age
                    sex
                    neutered_status
                    weight
                    response_to_treatment
                    disease_site
                    primary_disease_site
                    date_of_diagnosis
                    histology_cytopathology
                    histological_grade
                    pathology_report
                    treatment_data
                    follow_up_data
                    concurrent_disease
                    concurrent_disease_type
                    arm
                    files
                    individual_id
                  }
                }
                """,
                "variables": {"diagnosis": diagnoses, "first": min(limit, 500), "offset": 0},
            },
        )
        cases = (data.get("data") or {}).get("caseOverview") or []
        study_codes = sorted({case.get("study_code") for case in cases if case.get("study_code")})
        studies = self._study_metadata(study_codes)
        return [self.normalize(case, studies.get(case.get("study_code"))) for case in cases]

    def normalize(self, case: dict[str, Any], study: dict[str, Any] | None = None) -> HarvestedRecord:
        case_id = case.get("case_id")
        study_code = case.get("study_code")
        title = f"ICDC canine case {case_id}: {case.get('diagnosis')}" if case_id else "ICDC canine case"
        study_description = (study or {}).get("clinical_study_description")
        identifiers = {
            "icdc_case_id": case_id,
            "source_id": case_id,
            "study_code": study_code,
            "accession_id": (study or {}).get("accession_id"),
        }
        identifiers = {key: value for key, value in identifiers.items() if value}
        source_url = f"https://caninecommons.cancer.gov/case/{urllib.parse.quote(case_id)}" if case_id else None
        policy_text = " ".join(
            part
            for part in (
                case.get("diagnosis"),
                case.get("disease_site"),
                case.get("primary_disease_site"),
                case.get("histology_cytopathology"),
                case.get("treatment_data"),
                study_description,
            )
            if part
        )
        payload = {"case": case, "study": study or {}}
        raw = _raw_record(self.source_key, case_id, source_url, payload)
        obj = ResearchObject(
            object_type=ResearchObjectType.DATASET,
            title=title,
            abstract=study_description,
            canonical_url=source_url,
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, case_id),
            identifiers=identifiers,
            metadata={
                "icdc_record_type": "case",
                "study_code": study_code,
                "study_name": (study or {}).get("clinical_study_name"),
                "study_type": case.get("study_type") or (study or {}).get("clinical_study_type"),
                "study_disposition": (study or {}).get("study_disposition"),
                "dates_of_conduct": (study or {}).get("dates_of_conduct"),
                "cohort": case.get("cohort"),
                "breed": case.get("breed"),
                "diagnosis": case.get("diagnosis"),
                "disease_site": case.get("disease_site"),
                "primary_disease_site": case.get("primary_disease_site"),
                "stage_of_disease": case.get("stage_of_disease"),
                "age": case.get("age"),
                "sex": case.get("sex"),
                "neutered_status": case.get("neutered_status"),
                "weight": case.get("weight"),
                "response_to_treatment": case.get("response_to_treatment"),
                "histology_cytopathology": case.get("histology_cytopathology"),
                "histological_grade": case.get("histological_grade"),
                "pathology_report": case.get("pathology_report"),
                "treatment_data": case.get("treatment_data"),
                "follow_up_data": case.get("follow_up_data"),
                "file_count": len(case.get("files") or []),
                "files": case.get("files") or [],
                "license_policy": "open_access_metadata",
                "ingestion_policy": infer_comparative_scope(title, policy_text),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)

    def text_for_chunking(self, record: HarvestedRecord) -> str:
        metadata = record.research_object.metadata
        return "\n\n".join(
            part
            for part in (
                record.research_object.title,
                record.research_object.abstract,
                f"Diagnosis: {metadata.get('diagnosis')}" if metadata.get("diagnosis") else None,
                f"Breed: {metadata.get('breed')}" if metadata.get("breed") else None,
                f"Disease site: {metadata.get('disease_site')}" if metadata.get("disease_site") else None,
                f"Primary disease site: {metadata.get('primary_disease_site')}"
                if metadata.get("primary_disease_site")
                else None,
                f"Stage of disease: {metadata.get('stage_of_disease')}" if metadata.get("stage_of_disease") else None,
                f"Response to treatment: {metadata.get('response_to_treatment')}"
                if metadata.get("response_to_treatment")
                else None,
                f"Study: {metadata.get('study_code')} {metadata.get('study_name')}"
                if metadata.get("study_code") or metadata.get("study_name")
                else None,
            )
            if part
        )

    def chunk_section_label(self, record: HarvestedRecord) -> str:
        _ = record
        return "icdc_case_metadata"

    def _study_metadata(self, study_codes: list[str]) -> dict[str, dict[str, Any]]:
        studies = {}
        for study_code in study_codes:
            data = _post_json(
                self.graphql_url,
                {
                    "query": """
                    query Study($studyCode: String) {
                      study(clinical_study_designation: $studyCode, first: 1) {
                        clinical_study_id
                        clinical_study_designation
                        clinical_study_name
                        clinical_study_description
                        clinical_study_type
                        accession_id
                        dates_of_conduct
                        study_disposition
                      }
                    }
                    """,
                    "variables": {"studyCode": study_code},
                },
            )
            rows = (data.get("data") or {}).get("study") or []
            if rows:
                studies[study_code] = rows[0]
        return studies


class GEOHarvesterV2(HarvesterV2):
    """NCBI GEO DataSets harvester via E-utilities metadata summaries."""

    source_key = "geo"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        params = dict(params)
        db = params.pop("db", "gds")
        search = _get_json(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            {"db": db, "term": query_text, "retmode": "json", "retmax": min(limit, 200), **params},
        )
        ids = (search.get("esearchresult") or {}).get("idlist", [])
        if not ids:
            return []
        summary = _get_json(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
            {"db": db, "id": ",".join(ids), "retmode": "json"},
        )
        result = summary.get("result") or {}
        records = [self.normalize(result[uid]) for uid in result.get("uids", []) if uid in result]
        return [
            record
            for record in records
            if record.research_object.metadata.get("ingestion_policy", {}).get("matched_concepts")
        ]

    def normalize(self, item: dict[str, Any]) -> HarvestedRecord:
        accession = item.get("accession")
        title = _clean_markup(item.get("title"))
        summary = _clean_markup(item.get("summary"))
        pubmedids = [str(pmid) for pmid in item.get("pubmedids", []) if pmid]
        samples = item.get("samples") if isinstance(item.get("samples"), list) else []
        sample_accessions = [sample.get("accession") for sample in samples if sample.get("accession")]
        sample_titles = [sample.get("title") for sample in samples if sample.get("title")]
        identifiers = {
            "geo_uid": str(item.get("uid")) if item.get("uid") else None,
            "geo_accession": accession,
            "gse": f"GSE{item.get('gse')}" if item.get("gse") and not str(item.get("gse")).startswith("GSE") else item.get("gse"),
            "gds": f"GDS{item.get('gds')}" if item.get("gds") and not str(item.get("gds")).startswith("GDS") else item.get("gds"),
            "bioproject": item.get("bioproject"),
            "pmid": pubmedids[0] if pubmedids else None,
        }
        identifiers = {key: value for key, value in identifiers.items() if value}
        source_url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={accession}" if accession else None
        raw = _raw_record(self.source_key, str(item.get("uid")) if item.get("uid") else accession, source_url, item)
        obj = ResearchObject(
            object_type=ResearchObjectType.DATASET,
            title=title,
            abstract=summary,
            canonical_url=source_url,
            publication_year=_year_from_slash_date(item.get("pdat")),
            published_at=item.get("pdat"),
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, accession or str(item.get("uid"))),
            identifiers=identifiers,
            metadata={
                "entry_type": item.get("entrytype"),
                "dataset_type": item.get("gdstype"),
                "taxon": item.get("taxon"),
                "platform_accessions": _split_semicolon(item.get("gpl")),
                "sample_count": _safe_int(item.get("n_samples")),
                "sample_accessions": sample_accessions,
                "sample_titles": sample_titles[:50],
                "supplementary_file_types": _split_comma(item.get("suppfile")),
                "ftp_link": item.get("ftplink"),
                "geo2r": item.get("geo2r"),
                "pubmedids": pubmedids,
                "relations": item.get("relations") or [],
                "projects": item.get("projects") or [],
                "license_policy": "metadata_and_dataset_links",
                "ingestion_policy": infer_comparative_scope(title, summary),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)

    def text_for_chunking(self, record: HarvestedRecord) -> str:
        metadata = record.research_object.metadata
        sample_titles = metadata.get("sample_titles", [])
        return "\n\n".join(
            part
            for part in (
                record.research_object.title,
                record.research_object.abstract,
                f"Taxon: {metadata.get('taxon')}" if metadata.get("taxon") else None,
                f"Dataset type: {metadata.get('dataset_type')}" if metadata.get("dataset_type") else None,
                f"Samples: {'; '.join(sample_titles[:20])}" if sample_titles else None,
                f"BioProject: {record.research_object.identifiers.get('bioproject')}"
                if record.research_object.identifiers.get("bioproject")
                else None,
            )
            if part
        )

    def chunk_section_label(self, record: HarvestedRecord) -> str:
        _ = record
        return "geo_dataset_metadata"


class SRAHarvesterV2(HarvesterV2):
    """NCBI SRA harvester via E-utilities experiment/run summaries."""

    source_key = "sra"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        params = dict(params)
        db = params.pop("db", "sra")
        search = _get_json(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            {"db": db, "term": query_text, "retmode": "json", "retmax": min(limit, 200), **params},
        )
        ids = (search.get("esearchresult") or {}).get("idlist", [])
        if not ids:
            return []
        summary = _get_json(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
            {"db": db, "id": ",".join(ids), "retmode": "json"},
        )
        result = summary.get("result") or {}
        records = [self.normalize(result[uid]) for uid in result.get("uids", []) if uid in result]
        return [
            record
            for record in records
            if record.research_object.metadata.get("ingestion_policy", {}).get("matched_concepts")
        ]

    def normalize(self, item: dict[str, Any]) -> HarvestedRecord:
        expxml = _parse_xml_fragment(item.get("expxml"))
        runs_xml = _parse_xml_fragment(item.get("runs"))
        summary_node = expxml.find("Summary")
        experiment_node = expxml.find("Experiment")
        study_node = expxml.find("Study")
        organism_node = expxml.find("Organism")
        sample_node = expxml.find("Sample")
        submitter_node = expxml.find("Submitter")
        platform_node = expxml.find("Summary/Platform")
        statistics_node = expxml.find("Summary/Statistics")
        library_node = expxml.find("Library_descriptor")
        first_run = runs_xml.find("Run")
        run_accessions = [run.attrib.get("acc") for run in runs_xml.findall("Run") if run.attrib.get("acc")]

        title = _xml_text(expxml, "Summary/Title") or (experiment_node.attrib.get("name") if experiment_node is not None else None)
        study_name = study_node.attrib.get("name") if study_node is not None else None
        experiment_acc = experiment_node.attrib.get("acc") if experiment_node is not None else None
        study_acc = study_node.attrib.get("acc") if study_node is not None else None
        sample_acc = sample_node.attrib.get("acc") if sample_node is not None else None
        bioproject = _xml_text(expxml, "Bioproject")
        biosample = _xml_text(expxml, "Biosample")
        identifiers = {
            "sra_uid": str(item.get("uid")) if item.get("uid") else None,
            "sra_experiment": experiment_acc,
            "sra_study": study_acc,
            "sra_sample": sample_acc,
            "sra_run": run_accessions[0] if run_accessions else None,
            "bioproject": bioproject,
            "biosample": biosample,
            "source_id": experiment_acc or str(item.get("uid")),
        }
        identifiers = {key: value for key, value in identifiers.items() if value}
        source_url = f"https://www.ncbi.nlm.nih.gov/sra/{experiment_acc}" if experiment_acc else None
        abstract = _clean_markup(study_name)
        policy_text = " ".join(part for part in (title, study_name, _xml_text(expxml, "Organism")) if part)
        raw = _raw_record(self.source_key, str(item.get("uid")) if item.get("uid") else experiment_acc, source_url, item)
        obj = ResearchObject(
            object_type=ResearchObjectType.DATASET,
            title=_clean_markup(title),
            abstract=abstract,
            canonical_url=source_url,
            publication_year=_year_from_slash_date(item.get("createdate")),
            published_at=item.get("createdate"),
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, experiment_acc or str(item.get("uid"))),
            identifiers=identifiers,
            metadata={
                "study_name": study_name,
                "organism": organism_node.attrib.get("ScientificName") if organism_node is not None else None,
                "taxid": organism_node.attrib.get("taxid") if organism_node is not None else None,
                "platform": _xml_join_text(platform_node),
                "instrument_model": platform_node.attrib.get("instrument_model") if platform_node is not None else None,
                "library_name": _xml_text(library_node, "LIBRARY_NAME") if library_node is not None else None,
                "library_strategy": _xml_text(library_node, "LIBRARY_STRATEGY") if library_node is not None else None,
                "library_source": _xml_text(library_node, "LIBRARY_SOURCE") if library_node is not None else None,
                "library_selection": _xml_text(library_node, "LIBRARY_SELECTION") if library_node is not None else None,
                "library_layout": _sra_library_layout(library_node),
                "submitter": dict(submitter_node.attrib) if submitter_node is not None else {},
                "statistics": dict(statistics_node.attrib) if statistics_node is not None else {},
                "run_accessions": run_accessions,
                "first_run": dict(first_run.attrib) if first_run is not None else {},
                "created_at": item.get("createdate"),
                "updated_at": item.get("updatedate"),
                "license_policy": "metadata_and_dataset_links",
                "ingestion_policy": infer_comparative_scope(title, policy_text),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)

    def text_for_chunking(self, record: HarvestedRecord) -> str:
        metadata = record.research_object.metadata
        return "\n\n".join(
            part
            for part in (
                record.research_object.title,
                record.research_object.abstract,
                f"Organism: {metadata.get('organism')}" if metadata.get("organism") else None,
                f"Library strategy: {metadata.get('library_strategy')}" if metadata.get("library_strategy") else None,
                f"Library source: {metadata.get('library_source')}" if metadata.get("library_source") else None,
                f"Runs: {'; '.join(metadata.get('run_accessions', [])[:10])}"
                if metadata.get("run_accessions")
                else None,
                f"BioProject: {record.research_object.identifiers.get('bioproject')}"
                if record.research_object.identifiers.get("bioproject")
                else None,
            )
            if part
        )

    def chunk_section_label(self, record: HarvestedRecord) -> str:
        _ = record
        return "sra_run_metadata"


class PubChemHarvesterV2(HarvesterV2):
    """PubChem PUG REST harvester for priority compound metadata."""

    source_key = "pubchem"
    property_fields = (
        "MolecularFormula",
        "MolecularWeight",
        "CanonicalSMILES",
        "IsomericSMILES",
        "InChIKey",
        "IUPACName",
        "XLogP",
        "TPSA",
        "ExactMass",
    )

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        terms = _query_terms(query_text)
        records_per_term = _safe_int(params.pop("records_per_term", 1)) or 1
        require_exact_match = _safe_bool(params.pop("require_exact_match", True))
        records: list[HarvestedRecord] = []
        for term in terms:
            if len(records) >= limit:
                break
            encoded = urllib.parse.quote(term, safe="")
            try:
                properties = _get_json(
                    f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{encoded}/property/{','.join(self.property_fields)}/JSON",
                    {},
                )
                synonyms = _get_json(
                    f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{encoded}/synonyms/JSON",
                    {},
                )
            except RuntimeError:
                continue
            rows = ((properties.get("PropertyTable") or {}).get("Properties")) or []
            synonym_rows = ((synonyms.get("InformationList") or {}).get("Information")) or []
            synonym_by_cid = {str(row.get("CID")): row.get("Synonym") or [] for row in synonym_rows}
            term_records: list[tuple[int, HarvestedRecord]] = []
            for row in rows:
                cid = row.get("CID")
                identity_match = _pubchem_identity_match(term, row, synonym_by_cid.get(str(cid), []))
                if require_exact_match and not identity_match["identity_verified"]:
                    continue
                payload = {
                    "query_term": term,
                    "properties": row,
                    "synonyms": synonym_by_cid.get(str(cid), []),
                    "identity_match": identity_match,
                    "source_query": query_text,
                }
                term_records.append((int(identity_match["rank"]), self.normalize(payload)))
            for _, record in sorted(term_records, key=lambda item: item[0])[:records_per_term]:
                if len(records) >= limit:
                    break
                records.append(record)
        return records

    def normalize(self, payload: dict[str, Any]) -> HarvestedRecord:
        properties = payload.get("properties") or {}
        synonyms = _string_list(payload.get("synonyms"))[:100]
        cid = str(properties.get("CID")) if properties.get("CID") is not None else None
        title = _clean_markup(properties.get("Title")) or _clean_markup(payload.get("query_term")) or f"PubChem CID {cid}"
        source_url = f"https://pubchem.ncbi.nlm.nih.gov/compound/{cid}" if cid else None
        identifiers = {
            "pubchem_cid": cid,
            "inchikey": properties.get("InChIKey"),
            "source_id": cid,
        }
        identifiers = {key: str(value) for key, value in identifiers.items() if value}
        abstract = f"PubChem compound metadata for {title}." if title else None
        raw = _raw_record(self.source_key, cid, source_url, payload)
        obj = ResearchObject(
            object_type=ResearchObjectType.COMPOUND_RECORD,
            title=title,
            abstract=abstract,
            canonical_url=source_url,
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, cid or title),
            identifiers=identifiers,
            metadata={
                "query_term": payload.get("query_term"),
                "molecular_formula": properties.get("MolecularFormula"),
                "molecular_weight": properties.get("MolecularWeight"),
                "canonical_smiles": properties.get("CanonicalSMILES"),
                "isomeric_smiles": properties.get("IsomericSMILES"),
                "inchikey": properties.get("InChIKey"),
                "iupac_name": properties.get("IUPACName"),
                "xlogp": properties.get("XLogP"),
                "tpsa": properties.get("TPSA"),
                "exact_mass": properties.get("ExactMass"),
                "synonyms": synonyms,
                "identity_match": payload.get("identity_match") or {},
                "license_policy": "metadata",
                "ingestion_policy": infer_comparative_scope(title, " ".join(synonyms[:20])),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)

    def text_for_chunking(self, record: HarvestedRecord) -> str:
        metadata = record.research_object.metadata
        return "\n\n".join(
            part
            for part in (
                record.research_object.title,
                record.research_object.abstract,
                f"PubChem CID: {record.research_object.identifiers.get('pubchem_cid')}",
                f"IUPAC name: {metadata.get('iupac_name')}" if metadata.get("iupac_name") else None,
                f"Molecular formula: {metadata.get('molecular_formula')}" if metadata.get("molecular_formula") else None,
                f"Canonical SMILES: {metadata.get('canonical_smiles')}" if metadata.get("canonical_smiles") else None,
                f"InChIKey: {metadata.get('inchikey')}" if metadata.get("inchikey") else None,
                f"Synonyms: {'; '.join(metadata.get('synonyms', [])[:25])}" if metadata.get("synonyms") else None,
            )
            if part
        )

    def chunk_section_label(self, record: HarvestedRecord) -> str:
        _ = record
        return "compound_metadata"


class ChEMBLHarvesterV2(HarvesterV2):
    """ChEMBL web services harvester for priority compound bioactivity rows."""

    source_key = "chembl"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        terms = _query_terms(query_text)
        molecules_per_term = _safe_int(params.pop("molecules_per_term", 1)) or 1
        default_activity_limit = max(1, (limit + max(1, len(terms)) - 1) // max(1, len(terms)))
        activities_per_molecule = _safe_int(params.pop("activities_per_molecule", None)) or default_activity_limit
        target_ids = _chembl_param_list(params.pop("target_chembl_ids", None), CHEMBL_PRIORITY_TARGET_IDS)
        target_organisms = _chembl_param_list(
            params.pop("target_organisms", None),
            CHEMBL_PRIORITY_TARGET_ORGANISMS,
        )
        standard_types = _chembl_param_list(params.pop("standard_types", None), CHEMBL_PRIORITY_STANDARD_TYPES)
        assay_types = _chembl_param_list(params.pop("assay_types", None), CHEMBL_PRIORITY_ASSAY_TYPES)
        min_pchembl = _safe_float(params.pop("min_pchembl", 4.0))
        include_cell_line_assays = _safe_bool(params.pop("include_cell_line_assays", True))
        cell_line_terms = _chembl_param_list(params.pop("cell_line_terms", None), CHEMBL_PRIORITY_CELL_LINE_TERMS)
        cell_line_standard_types = _chembl_param_list(
            params.pop("cell_line_standard_types", None),
            CHEMBL_PRIORITY_CELL_LINE_STANDARD_TYPES,
        )
        cell_line_scan_limit = _safe_int(params.pop("cell_line_scan_limit", 50)) or 50
        cell_line_records_per_molecule = _safe_int(params.pop("cell_line_records_per_molecule", 2)) or 2
        target_id_set = set(target_ids)
        target_organism_set = set(target_organisms)
        standard_type_set = set(standard_types)
        assay_type_set = set(assay_types)
        cell_line_standard_type_set = set(cell_line_standard_types)
        records: list[HarvestedRecord] = []
        seen_activity_ids: set[str] = set()
        for term in terms:
            if len(records) >= limit:
                break
            molecules = _chembl_molecule_candidates(term, molecules_per_term)
            for molecule in molecules:
                if len(records) >= limit:
                    break
                molecule_id = molecule.get("molecule_chembl_id")
                if not molecule_id:
                    continue
                activity_params = {
                    "molecule_chembl_id": molecule_id,
                    "limit": min(activities_per_molecule, max(1, limit - len(records))),
                    "standard_type__isnull": "false",
                    "target_chembl_id__isnull": "false",
                    "pchembl_value__isnull": "false",
                    "order_by": "-pchembl_value",
                }
                if target_ids:
                    activity_params["target_chembl_id__in"] = ",".join(target_ids)
                if standard_types:
                    activity_params["standard_type__in"] = ",".join(standard_types)
                if assay_types:
                    activity_params["assay_type__in"] = ",".join(assay_types)
                try:
                    activity_data = _get_json(
                        "https://www.ebi.ac.uk/chembl/api/data/activity.json",
                        activity_params,
                    )
                except RuntimeError:
                    continue
                for activity in activity_data.get("activities") or []:
                    if len(records) >= limit:
                        break
                    activity_id = str(activity.get("activity_id")) if activity.get("activity_id") is not None else None
                    if activity_id and activity_id in seen_activity_ids:
                        continue
                    if not _chembl_activity_is_relevant(
                        activity,
                        target_ids=target_id_set,
                        target_organisms=target_organism_set,
                        standard_types=standard_type_set,
                        assay_types=assay_type_set,
                        min_pchembl=min_pchembl,
                    ):
                        continue
                    if activity_id:
                        seen_activity_ids.add(activity_id)
                    records.append(
                        self.normalize(
                            {
                                "query_term": term,
                                "molecule": molecule,
                                "activity": activity,
                                "target_gate": CHEMBL_PRIORITY_TARGETS.get(str(activity.get("target_chembl_id"))),
                            }
                        )
                    )
                if not include_cell_line_assays or len(records) >= limit:
                    continue
                try:
                    cell_line_data = _get_json(
                        "https://www.ebi.ac.uk/chembl/api/data/activity.json",
                        {
                            "molecule_chembl_id": molecule_id,
                            "limit": max(1, min(cell_line_scan_limit, 1000)),
                            "target_type": "CELL-LINE",
                            "standard_type__in": ",".join(cell_line_standard_types),
                            "assay_type__in": "F",
                            "pchembl_value__isnull": "false",
                            "order_by": "-pchembl_value",
                        },
                    )
                except RuntimeError:
                    continue
                cell_line_records = 0
                for activity in cell_line_data.get("activities") or []:
                    if len(records) >= limit or cell_line_records >= cell_line_records_per_molecule:
                        break
                    activity_id = str(activity.get("activity_id")) if activity.get("activity_id") is not None else None
                    if activity_id and activity_id in seen_activity_ids:
                        continue
                    matched_cell_line_term = _chembl_cell_line_match(
                        activity,
                        cell_line_terms=cell_line_terms,
                        target_organisms=target_organism_set,
                        standard_types=cell_line_standard_type_set,
                        min_pchembl=min_pchembl,
                    )
                    if not matched_cell_line_term:
                        continue
                    if activity_id:
                        seen_activity_ids.add(activity_id)
                    cell_line_records += 1
                    records.append(
                        self.normalize(
                            {
                                "query_term": term,
                                "molecule": molecule,
                                "activity": activity,
                                "target_gate": {
                                    "category": "cell_cytotoxicity",
                                    "matched_term": matched_cell_line_term,
                                },
                            }
                        )
                    )
        return records

    def normalize(self, payload: dict[str, Any]) -> HarvestedRecord:
        molecule = payload.get("molecule") or {}
        activity = payload.get("activity") or {}
        molecule_id = molecule.get("molecule_chembl_id") or activity.get("molecule_chembl_id")
        activity_id = str(activity.get("activity_id")) if activity.get("activity_id") is not None else None
        target_id = activity.get("target_chembl_id")
        target_name = activity.get("target_pref_name")
        target_gate = payload.get("target_gate") or CHEMBL_PRIORITY_TARGETS.get(str(target_id))
        compound_name = molecule.get("pref_name") or activity.get("molecule_pref_name") or payload.get("query_term") or molecule_id
        standard_type = activity.get("standard_type")
        title = " ".join(
            part
            for part in (
                str(compound_name) if compound_name else None,
                str(standard_type) if standard_type else "bioactivity",
                f"against {target_name}" if target_name else None,
            )
            if part
        )
        source_record_id = activity_id or ":".join(
            str(part)
            for part in (molecule_id, target_id, activity.get("assay_chembl_id"), standard_type)
            if part
        )
        source_url = f"https://www.ebi.ac.uk/chembl/compound_report_card/{molecule_id}/" if molecule_id else None
        identifiers = {
            "chembl_activity_id": activity_id,
            "chembl_molecule_id": molecule_id,
            "chembl_target_id": target_id,
            "chembl_assay_id": activity.get("assay_chembl_id"),
            "chembl_document_id": activity.get("document_chembl_id"),
            "source_id": source_record_id,
        }
        identifiers = {key: str(value) for key, value in identifiers.items() if value}
        abstract = _clean_markup(activity.get("assay_description"))
        raw = _raw_record(self.source_key, source_record_id, source_url, payload)
        obj = ResearchObject(
            object_type=ResearchObjectType.BIOACTIVITY_ASSAY,
            title=_clean_markup(title),
            abstract=abstract,
            canonical_url=source_url,
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, source_record_id or title),
            identifiers=identifiers,
            metadata={
                "query_term": payload.get("query_term"),
                "molecule_pref_name": compound_name,
                "max_phase": molecule.get("max_phase"),
                "molecule_type": molecule.get("molecule_type"),
                "target_pref_name": target_name,
                "target_gene": target_gate.get("gene") if isinstance(target_gate, dict) else None,
                "target_category": target_gate.get("category") if isinstance(target_gate, dict) else None,
                "matched_cell_line_term": target_gate.get("matched_term") if isinstance(target_gate, dict) else None,
                "target_organism": activity.get("target_organism"),
                "assay_type": activity.get("assay_type"),
                "assay_confidence_score": activity.get("confidence_score"),
                "assay_description": abstract,
                "standard_type": standard_type,
                "standard_relation": activity.get("standard_relation"),
                "standard_value": activity.get("standard_value"),
                "standard_units": activity.get("standard_units"),
                "pchembl_value": activity.get("pchembl_value"),
                "pchembl_numeric": _safe_float(activity.get("pchembl_value")),
                "activity_comment": activity.get("activity_comment"),
                "data_validity_comment": activity.get("data_validity_comment"),
                "document_year": activity.get("document_year"),
                "license_policy": "cc_by_sa",
                "ingestion_policy": infer_comparative_scope(title, abstract),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)

    def text_for_chunking(self, record: HarvestedRecord) -> str:
        metadata = record.research_object.metadata
        activity_line = " ".join(
            str(part)
            for part in (
                metadata.get("standard_type"),
                metadata.get("standard_relation"),
                metadata.get("standard_value"),
                metadata.get("standard_units"),
            )
            if part
        )
        return "\n\n".join(
            part
            for part in (
                record.research_object.title,
                record.research_object.abstract,
                f"Target gate: {metadata.get('target_gene')} ({metadata.get('target_category')})"
                if metadata.get("target_gene")
                else f"Target gate: {metadata.get('target_category')}"
                if metadata.get("target_category")
                else None,
                f"Matched cell-line term: {metadata.get('matched_cell_line_term')}"
                if metadata.get("matched_cell_line_term")
                else None,
                f"Target: {metadata.get('target_pref_name')}" if metadata.get("target_pref_name") else None,
                f"Target organism: {metadata.get('target_organism')}" if metadata.get("target_organism") else None,
                f"Assay type: {metadata.get('assay_type')}" if metadata.get("assay_type") else None,
                f"Activity: {activity_line}" if activity_line else None,
                f"pChEMBL: {metadata.get('pchembl_value')}" if metadata.get("pchembl_value") else None,
            )
            if part
        )

    def chunk_section_label(self, record: HarvestedRecord) -> str:
        _ = record
        return "bioactivity_assay"


class UniProtHarvesterV2(HarvesterV2):
    """UniProtKB REST harvester for priority target protein records."""

    source_key = "uniprot"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        terms = _query_terms(query_text)
        organism_ids = _string_list(params.pop("organism_ids", ["9615", "9606"]))
        reviewed = params.pop("reviewed", None)
        require_gene_match = _safe_bool(params.pop("require_gene_match", True))
        dedupe_gene_organism = _safe_bool(params.pop("dedupe_gene_organism", True))
        size_per_term = _safe_int(params.pop("size_per_term", None)) or max(1, (limit + max(1, len(terms)) - 1) // max(1, len(terms)))
        records: list[HarvestedRecord] = []
        seen_gene_organism: set[tuple[str, str]] = set()
        for term in terms:
            if len(records) >= limit:
                break
            organism_query = " OR ".join(f"organism_id:{organism_id}" for organism_id in organism_ids)
            query_parts = [f"(gene:{term})", f"({organism_query})"]
            if reviewed is not None:
                query_parts.append(f"reviewed:{str(bool(reviewed)).lower()}")
            data = _get_json(
                "https://rest.uniprot.org/uniprotkb/search",
                {
                    "query": " AND ".join(query_parts),
                    "format": "json",
                    "size": min(size_per_term, max(1, limit - len(records)), 500),
                },
            )
            for entry in data.get("results") or []:
                if len(records) >= limit:
                    break
                if require_gene_match and not _uniprot_matches_source_query(entry, term):
                    continue
                gene_names = _uniprot_gene_names(entry)
                organism = entry.get("organism") or {}
                gene_key = (gene_names[0] if gene_names else term).upper()
                organism_key = str(organism.get("taxonId") or "")
                if dedupe_gene_organism and (gene_key, organism_key) in seen_gene_organism:
                    continue
                seen_gene_organism.add((gene_key, organism_key))
                records.append(self.normalize(entry, source_query=term))
        return records

    def normalize(self, entry: dict[str, Any], *, source_query: str | None = None) -> HarvestedRecord:
        accession = entry.get("primaryAccession")
        gene_names = _uniprot_gene_names(entry)
        protein_name = _uniprot_protein_name(entry)
        organism = entry.get("organism") or {}
        sequence = entry.get("sequence") or {}
        function_text = _uniprot_comment_text(entry, "FUNCTION")
        source_url = f"https://www.uniprot.org/uniprotkb/{accession}/entry" if accession else None
        identifiers = {
            "uniprot_accession": accession,
            "gene_symbol": gene_names[0] if gene_names else source_query,
            "organism_taxid": organism.get("taxonId"),
            "source_id": accession,
        }
        identifiers = {key: str(value) for key, value in identifiers.items() if value}
        title = " ".join(part for part in (gene_names[0] if gene_names else None, protein_name) if part) or accession
        abstract = function_text or protein_name
        cross_refs = _uniprot_cross_refs(entry)
        target_gate = _target_gate(source_query or (gene_names[0] if gene_names else ""))
        raw = _raw_record(self.source_key, accession, source_url, entry | {"source_query": source_query})
        obj = ResearchObject(
            object_type=ResearchObjectType.STRUCTURE,
            title=_clean_markup(title),
            abstract=_clean_markup(abstract),
            canonical_url=source_url,
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, accession or title),
            identifiers=identifiers,
            metadata={
                "source_query": source_query,
                "uniprotkb_id": entry.get("uniProtkbId"),
                "entry_type": entry.get("entryType"),
                "reviewed": "reviewed" in str(entry.get("entryType", "")).lower()
                and "unreviewed" not in str(entry.get("entryType", "")).lower(),
                "protein_name": protein_name,
                "gene_names": gene_names,
                "target_gene": source_query or (gene_names[0] if gene_names else None),
                "target_category": target_gate.get("category") if target_gate else None,
                "gene_match_verified": _uniprot_matches_source_query(entry, source_query) if source_query else None,
                "organism": organism.get("scientificName"),
                "organism_taxid": organism.get("taxonId"),
                "species_scope": _species_scope(organism.get("scientificName")),
                "sequence_length": sequence.get("length"),
                "molecular_weight": sequence.get("molWeight"),
                "keywords": [keyword.get("name") for keyword in entry.get("keywords", []) if keyword.get("name")],
                "cross_references": cross_refs,
                "alphafold_ids": [xref["id"] for xref in cross_refs if xref.get("database") == "AlphaFoldDB"],
                "alphafold_available": any(xref.get("database") == "AlphaFoldDB" for xref in cross_refs),
                "license_policy": "metadata_and_sequence_links",
                "ingestion_policy": infer_comparative_scope(title, abstract),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)

    def text_for_chunking(self, record: HarvestedRecord) -> str:
        metadata = record.research_object.metadata
        return "\n\n".join(
            part
            for part in (
                record.research_object.title,
                record.research_object.abstract,
                f"Target gate: {metadata.get('target_gene')} ({metadata.get('target_category')})"
                if metadata.get("target_gene")
                else None,
                f"Genes: {'; '.join(metadata.get('gene_names', []))}" if metadata.get("gene_names") else None,
                f"Organism: {metadata.get('organism')}" if metadata.get("organism") else None,
                f"UniProt accession: {record.research_object.identifiers.get('uniprot_accession')}",
                f"AlphaFold IDs: {'; '.join(metadata.get('alphafold_ids', []))}" if metadata.get("alphafold_ids") else None,
            )
            if part
        )

    def chunk_section_label(self, record: HarvestedRecord) -> str:
        _ = record
        return "protein_target_metadata"


class RCSBPDBHarvesterV2(HarvesterV2):
    """RCSB PDB search/data API harvester for priority target structures."""

    source_key = "rcsb_pdb"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        terms = _query_terms(query_text)
        rows_per_term = max(1, _safe_int(params.pop("rows_per_term", None)) or min(limit, 5))
        require_target_match = _safe_bool(params.pop("require_target_match", True))
        require_protein_entity = _safe_bool(params.pop("require_protein_entity", True))
        seen: set[str] = set()
        records: list[HarvestedRecord] = []
        for term in terms:
            if len(records) >= limit:
                break
            search = _post_json(
                "https://search.rcsb.org/rcsbsearch/v2/query",
                {
                    "query": {
                        "type": "terminal",
                        "service": "full_text",
                        "parameters": {"value": term},
                    },
                    "return_type": "entry",
                    "request_options": {
                        "paginate": {"start": 0, "rows": min(rows_per_term, max(1, limit - len(records)))},
                        "results_content_type": ["experimental"],
                    },
                },
            )
            for hit in search.get("result_set") or []:
                pdb_id = hit.get("identifier")
                if not pdb_id or pdb_id in seen or len(records) >= limit:
                    continue
                seen.add(pdb_id)
                try:
                    entry = _get_json(f"https://data.rcsb.org/rest/v1/core/entry/{pdb_id}", {})
                except RuntimeError:
                    entry = {"rcsb_id": pdb_id}
                target_match = _rcsb_target_match(term, entry)
                if require_target_match and not target_match:
                    continue
                if require_protein_entity and not _rcsb_has_protein_entity(entry):
                    continue
                records.append(self.normalize({"query_term": term, "search_hit": hit, "entry": entry, "target_match": target_match}))
        return records

    def normalize(self, payload: dict[str, Any]) -> HarvestedRecord:
        entry = payload.get("entry") or {}
        search_hit = payload.get("search_hit") or {}
        pdb_id = entry.get("rcsb_id") or search_hit.get("identifier")
        struct = entry.get("struct") or {}
        title = _clean_markup(struct.get("title")) or f"RCSB PDB structure {pdb_id}"
        accession = entry.get("rcsb_accession_info") or {}
        methods = [
            item.get("method")
            for item in entry.get("exptl", [])
            if isinstance(item, dict) and item.get("method")
        ]
        citations = entry.get("citation") if isinstance(entry.get("citation"), list) else []
        pmids = [str(citation.get("pdbx_database_id_PubMed")) for citation in citations if citation.get("pdbx_database_id_PubMed")]
        target_match = payload.get("target_match") or _rcsb_target_match(payload.get("query_term"), entry)
        source_url = f"https://www.rcsb.org/structure/{pdb_id}" if pdb_id else None
        identifiers = {
            "pdb_id": pdb_id,
            "pmid": pmids[0] if pmids else None,
            "source_id": pdb_id,
        }
        identifiers = {key: str(value) for key, value in identifiers.items() if value}
        abstract = f"Experimental structure metadata from RCSB PDB. Methods: {'; '.join(methods)}." if methods else None
        raw = _raw_record(self.source_key, pdb_id, source_url, payload)
        obj = ResearchObject(
            object_type=ResearchObjectType.STRUCTURE,
            title=title,
            abstract=abstract,
            canonical_url=source_url,
            publication_year=_year_from_date(accession.get("initial_release_date")),
            published_at=accession.get("initial_release_date"),
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, pdb_id or title),
            identifiers=identifiers,
            metadata={
                "query_term": payload.get("query_term"),
                "target_gene": target_match.get("target_gene") if target_match else None,
                "target_category": target_match.get("target_category") if target_match else None,
                "target_alias_matched": target_match.get("matched_alias") if target_match else None,
                "experimental_methods": methods,
                "deposition_date": accession.get("deposit_date"),
                "initial_release_date": accession.get("initial_release_date"),
                "revision_date": accession.get("revision_date"),
                "pdb_format_compatible": accession.get("has_released_experimental_data"),
                "search_score": search_hit.get("score"),
                "pmids": pmids,
                "entry_info": entry.get("rcsb_entry_info") or {},
                "protein_entity_count": (entry.get("rcsb_entry_info") or {}).get("polymer_entity_count_protein"),
                "license_policy": "metadata_and_structure_links",
                "ingestion_policy": infer_comparative_scope(title, abstract),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)

    def text_for_chunking(self, record: HarvestedRecord) -> str:
        metadata = record.research_object.metadata
        return "\n\n".join(
            part
            for part in (
                record.research_object.title,
                record.research_object.abstract,
                f"Target gate: {metadata.get('target_gene')} ({metadata.get('target_category')})"
                if metadata.get("target_gene")
                else None,
                f"Matched target alias: {metadata.get('target_alias_matched')}"
                if metadata.get("target_alias_matched")
                else None,
                f"PDB ID: {record.research_object.identifiers.get('pdb_id')}",
                f"Methods: {'; '.join(metadata.get('experimental_methods', []))}"
                if metadata.get("experimental_methods")
                else None,
                f"PubMed IDs: {'; '.join(metadata.get('pmids', []))}" if metadata.get("pmids") else None,
            )
            if part
        )

    def chunk_section_label(self, record: HarvestedRecord) -> str:
        _ = record
        return "structure_metadata"


class OpenFDAAnimalEventsHarvesterV2(HarvesterV2):
    """openFDA Animal and Veterinary adverse event harvester."""

    source_key = "openfda_animal_events"
    api_url = "https://api.fda.gov/animalandveterinary/event.json"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        terms = _query_terms(query_text)
        species = params.pop("species", "Dog")
        search_override = params.pop("search", None)
        if search_override:
            terms = [query_text]
        records: list[HarvestedRecord] = []
        default_per_term = max(1, (limit + max(1, len(terms)) - 1) // max(1, len(terms)))
        per_term = max(1, min(1000, _safe_int(params.pop("per_term", None)) or default_per_term))
        for term in terms:
            if len(records) >= limit:
                break
            search = search_override or (
                f'animal.species:"{species}" AND '
                f'(drug.active_ingredients.name:"{term}" OR drug.brand_name:"{term}")'
            )
            try:
                data = _get_json(self.api_url, {"search": search, "limit": min(per_term, limit - len(records))})
            except RuntimeError:
                continue
            for event in data.get("results") or []:
                if len(records) >= limit:
                    break
                records.append(self.normalize(event, source_query=term, source_search=search))
        return records

    def normalize(
        self,
        event: dict[str, Any],
        *,
        source_query: str | None = None,
        source_search: str | None = None,
    ) -> HarvestedRecord:
        report_id = (
            event.get("unique_aer_id_number")
            or event.get("original_receive_date")
            or stable_json_hash(event)[:16]
        )
        animal = event.get("animal") if isinstance(event.get("animal"), dict) else {}
        drugs = event.get("drug") if isinstance(event.get("drug"), list) else []
        reactions = event.get("reaction") if isinstance(event.get("reaction"), list) else []
        drug_names = _openfda_drug_names(drugs)
        matched_drug_name = _openfda_matched_drug_name(drug_names, source_query)
        reaction_terms = _openfda_reaction_terms(reactions)
        reaction_codes = _openfda_reaction_codes(reactions)
        species = animal.get("species")
        title = " ".join(
            part
            for part in (
                "openFDA animal adverse event",
                str(report_id),
                f"for {matched_drug_name}" if matched_drug_name else None,
                f"in {species}" if species else None,
            )
            if part
        )
        source_url = f"{self.api_url}?search=unique_aer_id_number:{urllib.parse.quote(str(report_id), safe='')}"
        identifiers = {
            "openfda_report_id": str(report_id),
            "source_id": str(report_id),
        }
        abstract = " ".join(
            part
            for part in (
                f"Species: {species}." if species else None,
                f"Drug products: {'; '.join(drug_names[:8])}." if drug_names else None,
                f"Reported reactions: {'; '.join(reaction_terms[:12])}." if reaction_terms else None,
            )
            if part
        )
        raw = _raw_record(self.source_key, str(report_id), source_url, event | {"source_query": source_query})
        obj = ResearchObject(
            object_type=ResearchObjectType.SAFETY_REPORT,
            title=_clean_markup(title),
            abstract=_clean_markup(abstract),
            canonical_url=source_url,
            published_at=event.get("original_receive_date") or event.get("latest_fda_received_date"),
            publication_year=_year_from_compact_date(event.get("original_receive_date") or event.get("latest_fda_received_date")),
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, str(report_id)),
            identifiers=identifiers,
            metadata={
                "source_query": source_query,
                "source_search": source_search,
                "species": species,
                "breed": animal.get("breed"),
                "gender": animal.get("gender"),
                "age": animal.get("age"),
                "weight": animal.get("weight"),
                "drug_names": drug_names,
                "matched_drug_name": matched_drug_name,
                "reaction_terms": reaction_terms,
                "reaction_codes": reaction_codes,
                "outcome": event.get("outcome"),
                "serious_ae": event.get("serious_ae"),
                "type_of_information": event.get("type_of_information"),
                "primary_reporter": event.get("primary_reporter"),
                "receiver": event.get("receiver"),
                "license_policy": "public_safety_reports_with_limitations",
                "responsible_use": "signal_generation_only_not_clinical_decision_support",
                "ingestion_policy": infer_comparative_scope(title, abstract),
                "harvester": "v2",
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)

    def text_for_chunking(self, record: HarvestedRecord) -> str:
        metadata = record.research_object.metadata
        return "\n\n".join(
            part
            for part in (
                record.research_object.title,
                record.research_object.abstract,
                f"Breed: {metadata.get('breed')}" if metadata.get("breed") else None,
                f"Matched drug: {metadata.get('matched_drug_name')}" if metadata.get("matched_drug_name") else None,
                f"Reaction terms: {'; '.join(metadata.get('reaction_terms', [])[:12])}"
                if metadata.get("reaction_terms")
                else None,
                f"Outcome: {metadata.get('outcome')}" if metadata.get("outcome") else None,
                f"Serious adverse event: {metadata.get('serious_ae')}" if metadata.get("serious_ae") else None,
                f"Responsible use: {metadata.get('responsible_use')}",
            )
            if part
        )

    def chunk_section_label(self, record: HarvestedRecord) -> str:
        _ = record
        return "safety_report_metadata"


HARVESTERS_V2: dict[str, type[HarvesterV2]] = {
    "avma_vctr": AVMAVCTRHarvesterV2,
    "chembl": ChEMBLHarvesterV2,
    "clinicaltrials_gov": ClinicalTrialsGovHarvesterV2,
    "geo": GEOHarvesterV2,
    "icdc": ICDCHarvesterV2,
    "openalex": OpenAlexHarvesterV2,
    "crossref": CrossrefHarvesterV2,
    "europe_pmc": EuropePMCHarvesterV2,
    "openfda_animal_events": OpenFDAAnimalEventsHarvesterV2,
    "pmc_oa": PMCOAHarvesterV2,
    "pubchem": PubChemHarvesterV2,
    "pubmed": PubMedHarvesterV2,
    "rcsb_pdb": RCSBPDBHarvesterV2,
    "sra": SRAHarvesterV2,
    "uniprot": UniProtHarvesterV2,
}

HARVESTERS = HARVESTERS_V2


def get_harvester(source_key: str) -> HarvesterV2:
    try:
        return HARVESTERS_V2[source_key]()
    except KeyError as exc:
        raise ValueError(f"No v2 harvester registered for source: {source_key}") from exc


def _get_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    return json.loads(_get_text(url, params))


def _post_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                parsed = json.loads(response.read().decode("utf-8"))
                if parsed.get("errors"):
                    raise RuntimeError(f"GraphQL errors from {url}: {parsed['errors']}")
                return parsed
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in {429, 500, 502, 503, 504} or attempt == 2:
                detail = exc.read().decode("utf-8", errors="replace")[:500]
                raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc
            time.sleep(1.5 * (attempt + 1))
        except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
            last_error = exc
            if attempt == 2:
                break
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"Request failed for {url}: {last_error}") from last_error


def _get_text(url: str, params: dict[str, Any]) -> str:
    params = dict(params)
    if "eutils.ncbi.nlm.nih.gov" in url and "api_key" not in params and os.getenv("NCBI_API_KEY"):
        params["api_key"] = os.environ["NCBI_API_KEY"]
    query = urllib.parse.urlencode({key: value for key, value in params.items() if value is not None})
    request = urllib.request.Request(f"{url}?{query}", headers={"User-Agent": USER_AGENT})
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                return response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in {429, 500, 502, 503, 504} or attempt == 2:
                detail = exc.read().decode("utf-8", errors="replace")[:500]
                raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc
            time.sleep(1.5 * (attempt + 1))
        except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
            last_error = exc
            if attempt == 2:
                break
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"Request failed for {url}: {last_error}") from last_error


def _raw_record(source_key: str, source_record_id: str | None, source_url: str | None, payload: dict[str, Any]) -> RawSourceRecord:
    return RawSourceRecord(
        source_key=source_key,
        source_record_id=source_record_id,
        source_url=source_url,
        content_hash=stable_json_hash(payload),
        raw_payload=payload,
    )


def _dedupe_key(identifiers: dict[str, str], source_key: str, fallback: str | None) -> str:
    for identifier_type in (
        "doi",
        "pdb_id",
        "pmid",
        "pmcid",
        "nct_id",
        "vct_code",
        "icdc_case_id",
        "geo_accession",
        "sra_experiment",
        "sra_study",
        "bioproject",
        "pubchem_cid",
        "chembl_activity_id",
        "chembl_molecule_id",
        "uniprot_accession",
        "openfda_report_id",
        "openalex_id",
        "source_id",
    ):
        value = identifiers.get(identifier_type)
        if value:
            return f"{identifier_type}:{value.lower()}"
    return f"{source_key}:{(fallback or 'unknown').lower()}"


def _normalize_doi(value: str | None) -> str | None:
    if not value:
        return None
    doi = value.strip()
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi, flags=re.I)
    return doi.lower() or None


def _strip_url_id(value: str | None) -> str | None:
    if not value:
        return None
    return value.rstrip("/").split("/")[-1]


def _openalex_abstract(index: dict[str, list[int]] | None) -> str | None:
    if not index:
        return None
    words: dict[int, str] = {}
    for word, positions in index.items():
        for position in positions:
            words[position] = word
    return " ".join(words[position] for position in sorted(words))


def _first(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _clean_markup(value: str | None) -> str | None:
    if not value:
        return None
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", unescape(value))).strip() or None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item]


def _query_terms(query_text: str) -> list[str]:
    terms = []
    for part in re.split(r"\s+OR\s+|[,;]", query_text, flags=re.I):
        cleaned = part.strip().strip("()").strip().strip('"').strip("'")
        if cleaned and cleaned not in terms:
            terms.append(cleaned)
    return terms or [query_text]


def _normalized_token_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _pubchem_identity_match(term: str, properties: dict[str, Any], synonyms: list[Any]) -> dict[str, Any]:
    normalized_term = _normalized_token_text(term)
    title = properties.get("Title")
    if normalized_term and _normalized_token_text(title) == normalized_term:
        return {"identity_verified": True, "match_type": "exact_title", "matched_value": title, "rank": 0}
    for synonym in synonyms:
        if normalized_term and _normalized_token_text(synonym) == normalized_term:
            return {"identity_verified": True, "match_type": "exact_synonym", "matched_value": synonym, "rank": 1}
    return {"identity_verified": False, "match_type": "name_search_fallback", "matched_value": title, "rank": 9}


def _target_gate(term: str | None) -> dict[str, object] | None:
    if not term:
        return None
    normalized = _normalized_token_text(term)
    for gene, gate in STRUCTURED_TARGET_GATES.items():
        aliases = tuple(gate.get("aliases", ()))
        if normalized == _normalized_token_text(gene) or any(normalized == _normalized_token_text(alias) for alias in aliases):
            return {"target_gene": gene, "category": gate.get("category"), "aliases": aliases}
    return None


def _target_alias_match(term: str | None, text: str | None) -> dict[str, str] | None:
    haystack = f" {_normalized_token_text(text)} "
    if not haystack.strip():
        return None
    gate = _target_gate(term)
    if not gate:
        return None
    for alias in (str(gate["target_gene"]), *(str(item) for item in gate.get("aliases", ()))):
        normalized_alias = _normalized_token_text(alias)
        if normalized_alias and f" {normalized_alias} " in haystack:
            return {
                "target_gene": str(gate["target_gene"]),
                "target_category": str(gate.get("category") or ""),
                "matched_alias": alias,
            }
    return None


def _species_scope(organism: Any) -> str | None:
    normalized = str(organism or "").lower()
    if normalized == "homo sapiens":
        return "human"
    if normalized == "canis lupus familiaris":
        return "canine"
    return None


def _uniprot_matches_source_query(entry: dict[str, Any], source_query: str | None) -> bool:
    if not source_query:
        return True
    normalized_query = _normalized_token_text(source_query)
    gene_names = _uniprot_gene_names(entry)
    return any(_normalized_token_text(name) == normalized_query for name in gene_names)


def _rcsb_target_match(term: str | None, entry: dict[str, Any]) -> dict[str, str] | None:
    struct = entry.get("struct") if isinstance(entry, dict) else {}
    title = struct.get("title") if isinstance(struct, dict) else None
    return _target_alias_match(term, title)


def _rcsb_has_protein_entity(entry: dict[str, Any]) -> bool:
    entry_info = entry.get("rcsb_entry_info") if isinstance(entry, dict) else {}
    value = (entry_info or {}).get("polymer_entity_count_protein") if isinstance(entry_info, dict) else None
    parsed = _safe_int(value)
    return parsed is not None and parsed > 0


def _chembl_param_list(value: Any, default: tuple[str, ...]) -> list[str]:
    if value is None:
        return list(default)
    if isinstance(value, str):
        return [part.strip() for part in re.split(r"[,;]", value) if part.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return list(default)


def _chembl_molecule_candidates(term: str, limit: int) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    exact_queries = (
        {"pref_name__iexact": term, "limit": limit},
        {"molecule_synonyms__molecule_synonym__iexact": term, "limit": limit},
        {"molecule_synonyms__synonyms__iexact": term, "limit": limit},
    )
    for query in exact_queries:
        try:
            data = _get_json("https://www.ebi.ac.uk/chembl/api/data/molecule.json", query)
        except RuntimeError:
            continue
        _chembl_add_unique_molecules(candidates, data.get("molecules") or [], seen, limit)
        if len(candidates) >= limit:
            return candidates[:limit]
    if candidates:
        return candidates[:limit]
    try:
        data = _get_json("https://www.ebi.ac.uk/chembl/api/data/molecule/search.json", {"q": term, "limit": limit})
    except RuntimeError:
        return candidates
    _chembl_add_unique_molecules(candidates, data.get("molecules") or [], seen, limit)
    return candidates[:limit]


def _chembl_add_unique_molecules(
    candidates: list[dict[str, Any]],
    molecules: list[Any],
    seen: set[str],
    limit: int,
) -> None:
    for molecule in molecules:
        if not isinstance(molecule, dict):
            continue
        molecule_id = molecule.get("molecule_chembl_id")
        if not molecule_id or molecule_id in seen:
            continue
        seen.add(str(molecule_id))
        candidates.append(molecule)
        if len(candidates) >= limit:
            return


def _chembl_activity_is_relevant(
    activity: dict[str, Any],
    *,
    target_ids: set[str],
    target_organisms: set[str],
    standard_types: set[str],
    assay_types: set[str],
    min_pchembl: float | None,
) -> bool:
    if target_ids and str(activity.get("target_chembl_id")) not in target_ids:
        return False
    if target_organisms and str(activity.get("target_organism")) not in target_organisms:
        return False
    if standard_types and str(activity.get("standard_type")) not in standard_types:
        return False
    if assay_types and str(activity.get("assay_type")) not in assay_types:
        return False
    pchembl = _safe_float(activity.get("pchembl_value"))
    if min_pchembl is not None and (pchembl is None or pchembl < min_pchembl):
        return False
    return True


def _chembl_cell_line_match(
    activity: dict[str, Any],
    *,
    cell_line_terms: list[str],
    target_organisms: set[str],
    standard_types: set[str],
    min_pchembl: float | None,
) -> str | None:
    if str(activity.get("target_chembl_id")) in CHEMBL_PRIORITY_TARGETS:
        return None
    target_organism = activity.get("target_organism")
    if target_organism and target_organisms and str(target_organism) not in target_organisms:
        return None
    if standard_types and str(activity.get("standard_type")) not in standard_types:
        return None
    if str(activity.get("assay_type")) != "F":
        return None
    pchembl = _safe_float(activity.get("pchembl_value"))
    if min_pchembl is not None and (pchembl is None or pchembl < min_pchembl):
        return None
    haystack = " ".join(
        str(part)
        for part in (
            activity.get("target_pref_name"),
            activity.get("target_organism"),
            activity.get("assay_description"),
        )
        if part
    ).lower()
    for term in cell_line_terms:
        pattern = re.compile(rf"(?<![a-z0-9]){re.escape(term.lower())}(?![a-z0-9])")
        if pattern.search(haystack):
            return term
    return None


def _date_struct_value(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    date = value.get("date")
    return str(date) if date else None


def _clinical_trial_interventions(module: dict[str, Any]) -> list[str]:
    interventions = []
    for intervention in module.get("interventions", []) or []:
        name = intervention.get("name")
        if name:
            interventions.append(name)
    return interventions


def _clinical_trial_outcomes(outcomes: Any) -> list[str]:
    if not isinstance(outcomes, list):
        return []
    return [outcome.get("measure") for outcome in outcomes if outcome.get("measure")]


def _clinical_trial_locations(module: dict[str, Any]) -> list[dict[str, str]]:
    locations = []
    for location in module.get("locations", []) or []:
        locations.append(
            {
                key: value
                for key, value in {
                    "facility": (location.get("facility") or "").strip() or None,
                    "city": location.get("city"),
                    "state": location.get("state"),
                    "country": location.get("country"),
                    "status": location.get("status"),
                }.items()
                if value
            }
        )
    return locations


def _crossref_date(item: dict[str, Any]) -> str | None:
    for key in ("published-print", "published-online", "issued", "created"):
        date_parts = ((item.get(key) or {}).get("date-parts") or [])
        if date_parts and date_parts[0]:
            parts = [str(part) for part in date_parts[0]]
            return "-".join(parts)
    return None


def _year_from_slash_date(value: str | None) -> int | None:
    if not value:
        return None
    return _safe_int(str(value).split("/")[0])


def _year_from_date(value: str | None) -> int | None:
    if not value:
        return None
    return _safe_int(value.split("-")[0])


def _year_from_compact_date(value: str | None) -> int | None:
    if not value:
        return None
    text = str(value)
    if len(text) >= 4 and text[:4].isdigit():
        return _safe_int(text[:4])
    return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off"}
    return bool(value)


def _split_semicolon(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in str(value).split(";") if part.strip()]


def _split_comma(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]


def _europe_pmc_url(item: dict[str, Any]) -> str | None:
    if item.get("pmid"):
        return f"https://europepmc.org/article/MED/{item['pmid']}"
    if item.get("pmcid"):
        return f"https://europepmc.org/article/PMC/{item['pmcid']}"
    if item.get("doi"):
        return f"https://doi.org/{item['doi']}"
    return None


def _find_first(node: ET.Element, path: str) -> ET.Element | None:
    return node.find(path)


def _find_first_of(node: ET.Element, paths: tuple[str, ...]) -> ET.Element | None:
    for path in paths:
        found = _find_first(node, path)
        if found is not None:
            return found
    return None


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _xml_text(node: ET.Element, path: str) -> str | None:
    found = node.find(path)
    if found is None or found.text is None:
        return None
    return found.text.strip() or None


def _parse_xml_fragment(value: str | None) -> ET.Element:
    return ET.fromstring(f"<root>{value or ''}</root>")


def _sra_library_layout(node: ET.Element | None) -> str | None:
    if node is None:
        return None
    layout = node.find("LIBRARY_LAYOUT")
    if layout is None or not list(layout):
        return None
    return _local_name(list(layout)[0].tag)


def _xml_join_text(node: ET.Element | None) -> str | None:
    if node is None:
        return None
    text = " ".join(part.strip() for part in node.itertext() if part and part.strip())
    return text or None


def _clean_multiline_text(value: str | None) -> str | None:
    if not value:
        return None
    return re.sub(r"\s+", " ", value).strip() or None


def _pubmed_date(article: ET.Element) -> str | None:
    pub_date = article.find(".//JournalIssue/PubDate")
    if pub_date is None:
        return None
    year = _xml_text(pub_date, "Year")
    month = _xml_text(pub_date, "Month")
    day = _xml_text(pub_date, "Day")
    return "-".join(part for part in (year, month, day) if part)


def _xml_to_dictish(node: ET.Element) -> dict[str, Any]:
    return {
        "tag": node.tag,
        "attributes": dict(node.attrib),
        "text": (node.text or "").strip(),
        "children": [_xml_to_dictish(child) for child in list(node)],
    }


def _normalize_pmcid(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip()
    if cleaned.upper().startswith("PMC"):
        return f"PMC{cleaned[3:]}"
    return f"PMC{cleaned}"


def _pmcid_numeric(pmcid: str | None) -> str | None:
    if not pmcid:
        return None
    return pmcid.upper().removeprefix("PMC")


def _pmc_oa_metadata(pmcid: str | None) -> dict[str, Any] | None:
    if not pmcid:
        return None
    xml = _get_text("https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi", {"id": pmcid})
    root = ET.fromstring(xml)
    error = root.find(".//error")
    if error is not None:
        return None
    record = root.find(".//record")
    if record is None:
        return None
    links = [
        {
            "format": link.attrib.get("format"),
            "href": link.attrib.get("href"),
            "updated": link.attrib.get("updated"),
        }
        for link in record.findall("link")
    ]
    return {
        "oa_record_id": record.attrib.get("id"),
        "citation": record.attrib.get("citation"),
        "oa_license": record.attrib.get("license"),
        "retracted": record.attrib.get("retracted"),
        "links": links,
    }


def _pmc_record_matches_policy(record: HarvestedRecord) -> bool:
    return _record_matches_policy(record)


def _record_matches_policy(record: HarvestedRecord) -> bool:
    metadata = record.research_object.metadata
    title_abstract_policy = metadata.get("ingestion_policy")
    body_policy = metadata.get("body_ingestion_policy")
    return bool(
        (
            isinstance(title_abstract_policy, dict)
            and title_abstract_policy.get("matched_concepts")
        )
        or (
            isinstance(body_policy, dict)
            and body_policy.get("matched_concepts")
        )
    )


def _europe_pmc_full_text_id(item: dict[str, Any]) -> str | None:
    return item.get("pmcid") or item.get("id")


def _europe_pmc_full_text_xml(article_id: str | None) -> str | None:
    if not article_id:
        return None
    try:
        return _get_text(
            f"https://www.ebi.ac.uk/europepmc/webservices/rest/{urllib.parse.quote(str(article_id))}/fullTextXML",
            {},
        )
    except (ET.ParseError, RuntimeError, ValueError):
        return None


def _jats_full_text(xml_text: str | None) -> str | None:
    if not xml_text:
        return None
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None
    article = _find_first(root, ".//{*}article")
    if article is None and _local_name(root.tag) == "article":
        article = root
    body = _find_first(article, ".//{*}body") if article is not None else _find_first(root, ".//{*}body")
    return _clean_multiline_text(_xml_join_text(body)) if body is not None else None


def _jats_article_ids(article: ET.Element) -> dict[str, str]:
    identifiers: dict[str, str] = {}
    for node in article.findall(".//{*}article-id"):
        id_type = node.attrib.get("pub-id-type")
        text = _xml_join_text(node)
        if not id_type or not text:
            continue
        normalized_type = "pmcid" if id_type in {"pmc", "pmcid"} else id_type
        identifiers[normalized_type] = _normalize_doi(text) if normalized_type == "doi" else text.strip()
    if identifiers.get("pmcid"):
        identifiers["pmcid"] = _normalize_pmcid(identifiers["pmcid"]) or identifiers["pmcid"]
    return identifiers


def _jats_pub_date(article: ET.Element) -> str | None:
    pub_date = _find_first_of(
        article,
        (
            ".//{*}article-meta/{*}pub-date[@date-type='pub']",
            ".//{*}article-meta/{*}pub-date[@pub-type='epub']",
            ".//{*}article-meta/{*}pub-date[@pub-type='ppub']",
            ".//{*}article-meta/{*}pub-date",
        ),
    )
    if pub_date is None:
        return None
    year = _xml_text_ns(pub_date, "year")
    month = _xml_text_ns(pub_date, "month")
    day = _xml_text_ns(pub_date, "day")
    return "-".join(part for part in (year, month, day) if part)


def _xml_text_ns(node: ET.Element, local_name: str) -> str | None:
    return _xml_join_text(_find_first(node, f"{{*}}{local_name}"))


def _jats_license(article: ET.Element) -> dict[str, Any]:
    license_node = _find_first_of(article, (".//{*}permissions/{*}license", ".//{*}license"))
    if license_node is None:
        return {}
    href = (
        license_node.attrib.get("{http://www.w3.org/1999/xlink}href")
        or license_node.attrib.get("href")
        or license_node.attrib.get("xlink:href")
    )
    return {
        "jats_license_type": license_node.attrib.get("license-type"),
        "jats_license_url": href,
        "jats_license_text": _xml_join_text(license_node),
    }


def _uniprot_gene_names(entry: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for gene in entry.get("genes") or []:
        for key in ("geneName", "orderedLocusNames", "orfNames"):
            value = gene.get(key)
            values = value if isinstance(value, list) else [value]
            for item in values:
                if isinstance(item, dict) and item.get("value") and item["value"] not in names:
                    names.append(item["value"])
        for synonym in gene.get("synonyms") or []:
            if isinstance(synonym, dict) and synonym.get("value") and synonym["value"] not in names:
                names.append(synonym["value"])
    return names


def _uniprot_protein_name(entry: dict[str, Any]) -> str | None:
    description = entry.get("proteinDescription") or {}
    recommended = description.get("recommendedName") or {}
    full_name = recommended.get("fullName") or {}
    if full_name.get("value"):
        return full_name["value"]
    for alternative in description.get("alternativeNames") or []:
        alt_name = (alternative.get("fullName") or {}).get("value")
        if alt_name:
            return alt_name
    submission_names = description.get("submissionNames") or []
    if submission_names:
        return (submission_names[0].get("fullName") or {}).get("value")
    return None


def _uniprot_comment_text(entry: dict[str, Any], comment_type: str) -> str | None:
    texts = []
    for comment in entry.get("comments") or []:
        if comment.get("commentType") != comment_type:
            continue
        for text in comment.get("texts") or []:
            value = text.get("value")
            if value:
                texts.append(value)
    return _clean_markup(" ".join(texts)) if texts else None


def _uniprot_cross_refs(entry: dict[str, Any]) -> list[dict[str, Any]]:
    refs = []
    for ref in entry.get("uniProtKBCrossReferences") or []:
        if not isinstance(ref, dict) or not ref.get("database"):
            continue
        refs.append(
            {
                "database": ref.get("database"),
                "id": ref.get("id"),
                "properties": ref.get("properties") or [],
            }
        )
    return refs


def _openfda_drug_names(drugs: list[Any]) -> list[str]:
    names: list[str] = []
    for drug in drugs:
        if not isinstance(drug, dict):
            continue
        for key in ("brand_name", "generic_name", "manufacturer_name"):
            value = drug.get(key)
            if value and str(value) not in names:
                names.append(str(value))
        active_ingredients = drug.get("active_ingredients")
        if isinstance(active_ingredients, list):
            for ingredient in active_ingredients:
                if isinstance(ingredient, dict):
                    value = ingredient.get("name")
                else:
                    value = ingredient
                if value and str(value) not in names:
                    names.append(str(value))
    return names


def _openfda_matched_drug_name(drug_names: list[str], source_query: str | None) -> str | None:
    if not drug_names:
        return None
    if not source_query:
        return drug_names[0]
    query = source_query.strip().lower()
    if not query:
        return drug_names[0]
    pattern = re.compile(rf"(?<![a-z0-9]){re.escape(query)}(?![a-z0-9])", re.I)
    for name in drug_names:
        if pattern.search(name):
            return name
    for name in drug_names:
        if query in name.lower():
            return name
    return drug_names[0]


def _openfda_reaction_terms(reactions: list[Any]) -> list[str]:
    terms: list[str] = []
    for reaction in reactions:
        if not isinstance(reaction, dict):
            continue
        for key in ("veddra_term_name", "reaction_term", "clinical_sign"):
            value = reaction.get(key)
            if value and str(value) not in terms:
                terms.append(str(value))
    return terms


def _openfda_reaction_codes(reactions: list[Any]) -> list[str]:
    codes: list[str] = []
    for reaction in reactions:
        if not isinstance(reaction, dict):
            continue
        value = reaction.get("veddra_term_code")
        if value and str(value) not in codes:
            codes.append(str(value))
    return codes
