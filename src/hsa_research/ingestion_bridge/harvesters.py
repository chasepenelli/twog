"""Legacy deterministic source harvesters for scholarly metadata.

New work should use ``harvesters_v2.py``. This file is kept only for reference
while the ingestion bridge is being rebuilt around the comparative oncology
query policy.
"""

from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from html import unescape
from typing import Any

from .contracts import HarvestedRecord, RawSourceRecord, ResearchObject, ResearchObjectType
from .local_store import stable_json_hash

USER_AGENT = "hsa-autoresearch-ingestion-bridge/0.1 (mailto:poppa@bradyandgraffiti.com)"


class SourceHarvester(ABC):
    """Base class for source-specific harvesters."""

    source_key: str

    @abstractmethod
    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        """Fetch and normalize source records."""


class OpenAlexHarvester(SourceHarvester):
    source_key = "openalex"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        data = _get_json(
            "https://api.openalex.org/works",
            {
                "search": query_text,
                "per-page": min(limit, 200),
                **params,
            },
        )
        return [self.normalize(item) for item in data.get("results", [])]

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
        year = item.get("publication_year")
        primary_location = item.get("primary_location") or {}
        landing_page = primary_location.get("landing_page_url") or item.get("doi") or source_id
        raw = _raw_record(self.source_key, source_id, landing_page, item)
        obj = ResearchObject(
            object_type=ResearchObjectType.PUBLICATION,
            title=item.get("title"),
            abstract=_openalex_abstract(item.get("abstract_inverted_index")),
            canonical_url=landing_page,
            publication_year=year,
            published_at=item.get("publication_date"),
            source_key=self.source_key,
            dedupe_key=_dedupe_key(identifiers, self.source_key, source_id),
            identifiers=identifiers,
            metadata={
                "source_display_name": (primary_location.get("source") or {}).get("display_name"),
                "cited_by_count": item.get("cited_by_count"),
                "type": item.get("type"),
                "open_access": item.get("open_access"),
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)


class CrossrefHarvester(SourceHarvester):
    source_key = "crossref"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        data = _get_json(
            "https://api.crossref.org/works",
            {
                "query": query_text,
                "rows": min(limit, 100),
                **params,
            },
        )
        items = (data.get("message") or {}).get("items", [])
        return [self.normalize(item) for item in items]

    def normalize(self, item: dict[str, Any]) -> HarvestedRecord:
        doi = _normalize_doi(item.get("DOI"))
        title = _first(item.get("title"))
        published = _crossref_date(item)
        identifiers = {"doi": doi, "source_id": doi}
        identifiers = {key: value for key, value in identifiers.items() if value}
        source_url = item.get("URL") or (f"https://doi.org/{doi}" if doi else None)
        raw = _raw_record(self.source_key, doi, source_url, item)
        obj = ResearchObject(
            object_type=ResearchObjectType.PUBLICATION,
            title=title,
            abstract=_clean_markup(item.get("abstract")),
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
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)


class EuropePMCHarvester(SourceHarvester):
    source_key = "europe_pmc"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
        data = _get_json(
            "https://www.ebi.ac.uk/europepmc/webservices/rest/search",
            {
                "query": query_text,
                "format": "json",
                "pageSize": min(limit, 1000),
                **params,
            },
        )
        items = ((data.get("resultList") or {}).get("result")) or []
        return [self.normalize(item) for item in items]

    def normalize(self, item: dict[str, Any]) -> HarvestedRecord:
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
        raw = _raw_record(self.source_key, source_id, source_url, item)
        obj = ResearchObject(
            object_type=ResearchObjectType.PREPRINT if item.get("source") == "PPR" else ResearchObjectType.PUBLICATION,
            title=item.get("title"),
            abstract=_clean_markup(item.get("abstractText")),
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
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)


class PubMedHarvester(SourceHarvester):
    source_key = "pubmed"

    def fetch(self, query_text: str, limit: int = 25, **params: Any) -> list[HarvestedRecord]:
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
        return [self.normalize(article) for article in ET.fromstring(xml).findall(".//PubmedArticle")]

    def normalize(self, article: ET.Element) -> HarvestedRecord:
        pmid = _xml_text(article, ".//MedlineCitation/PMID")
        title = _xml_text(article, ".//ArticleTitle")
        abstract = " ".join(
            text.strip()
            for text in article.findall(".//Abstract/AbstractText")
            if text.text and text.text.strip()
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
            },
        )
        return HarvestedRecord(raw_record=raw, research_object=obj)


HARVESTERS: dict[str, type[SourceHarvester]] = {
    "openalex": OpenAlexHarvester,
    "crossref": CrossrefHarvester,
    "europe_pmc": EuropePMCHarvester,
    "pubmed": PubMedHarvester,
}


def get_harvester(source_key: str) -> SourceHarvester:
    try:
        return HARVESTERS[source_key]()
    except KeyError as exc:
        raise ValueError(f"No harvester registered for source: {source_key}") from exc


def _get_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    return json.loads(_get_text(url, params))


def _get_text(url: str, params: dict[str, Any]) -> str:
    query = urllib.parse.urlencode({key: value for key, value in params.items() if value is not None})
    request = urllib.request.Request(f"{url}?{query}", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=45) as response:
        return response.read().decode("utf-8")


def _raw_record(source_key: str, source_record_id: str | None, source_url: str | None, payload: dict[str, Any]) -> RawSourceRecord:
    return RawSourceRecord(
        source_key=source_key,
        source_record_id=source_record_id,
        source_url=source_url,
        content_hash=stable_json_hash(payload),
        raw_payload=payload,
    )


def _dedupe_key(identifiers: dict[str, str], source_key: str, fallback: str | None) -> str:
    for identifier_type in ("doi", "pmid", "pmcid", "openalex_id", "source_id"):
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
    return unescape(re.sub(r"<[^>]+>", " ", value)).strip() or None


def _crossref_date(item: dict[str, Any]) -> str | None:
    for key in ("published-print", "published-online", "issued", "created"):
        date_parts = ((item.get(key) or {}).get("date-parts") or [])
        if date_parts and date_parts[0]:
            parts = [str(part) for part in date_parts[0]]
            return "-".join(parts)
    return None


def _year_from_date(value: str | None) -> int | None:
    if not value:
        return None
    return _safe_int(value.split("-")[0])


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _europe_pmc_url(item: dict[str, Any]) -> str | None:
    if item.get("pmid"):
        return f"https://europepmc.org/article/MED/{item['pmid']}"
    if item.get("pmcid"):
        return f"https://europepmc.org/article/PMC/{item['pmcid']}"
    if item.get("doi"):
        return f"https://doi.org/{item['doi']}"
    return None


def _xml_text(node: ET.Element, path: str) -> str | None:
    found = node.find(path)
    if found is None or found.text is None:
        return None
    return found.text.strip() or None


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
