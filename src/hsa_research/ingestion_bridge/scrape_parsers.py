"""Deterministic parsers for controlled scraper artifacts."""

from __future__ import annotations

import json
import re
import urllib.parse
from html import unescape
from typing import Any, Callable
from uuid import UUID

from .contracts import ArtifactHandle, ResearchObjectType, ScrapeManifestItem, ScrapeParsedRecord, ScrapeSourceProfile

ParserFunc = Callable[[ScrapeSourceProfile, ArtifactHandle, str], ScrapeParsedRecord | None]
_DOI_RE = re.compile(r"10\.\d{4,9}/[^\s\"'<>]+", re.IGNORECASE)
_DOI_TRAILING_CHARS = ").,;:&"


def parse_scrape_html(
    profile: ScrapeSourceProfile,
    artifact: ArtifactHandle,
    html: str,
) -> ScrapeParsedRecord | None:
    """Dispatch HTML to a configured parser."""

    parser = PARSER_REGISTRY.get(profile.parser)
    if parser is None:
        raise ValueError(f"Unsupported parser: {profile.parser}")
    return parser(profile, artifact, html)


def discover_manifest_candidates(
    profile: ScrapeSourceProfile,
    artifact: ArtifactHandle,
    html: str,
) -> list[ScrapeManifestItem]:
    """Extract likely detail-page URLs from a stored source page."""

    if profile.parser == "avma_vctr":
        return _discover_avma_vctr_candidates(profile, artifact, html)
    return _discover_generic_candidates(profile, artifact, html)


def parse_generic_html(
    profile: ScrapeSourceProfile,
    artifact: ArtifactHandle,
    html: str,
) -> ScrapeParsedRecord | None:
    """Extract conservative title/link metadata from arbitrary HTML."""

    title = _best_title(html)
    source_url = artifact.metadata.get("source_url")
    links = _html_links(html, source_url)
    primary_source_links = _primary_source_links(html, links)
    record_type = (
        ResearchObjectType.PUBLICATION
        if profile.source_key == "x_linked_article"
        else ResearchObjectType.VETERINARY_TRIAL
    )
    confidence = 0.35 if title else 0.15
    if primary_source_links:
        confidence = max(confidence, 0.55)
    return ScrapeParsedRecord(
        source_key=profile.source_key,
        source_record_id=_stable_source_record_id(source_url, title, artifact.artifact_id),
        title=title,
        canonical_url=source_url,
        record_type=record_type,
        fields={
            "links": links[:100],
            "primary_source_links": primary_source_links[:50],
            "parser": profile.parser,
            "storage_policy": profile.storage_policy,
        },
        parser_confidence=confidence,
        review_status="needs_review",
        artifact_id=artifact.artifact_id,
    )


def parse_avma_vctr_html(
    profile: ScrapeSourceProfile,
    artifact: ArtifactHandle,
    html: str,
) -> ScrapeParsedRecord | None:
    """Parse AVMA Veterinary Clinical Trials Registry study/profile pages."""

    source_url = artifact.metadata.get("source_url")
    text = _visible_text(html)
    label_values = _extract_label_values(html)
    jsonld_values = _extract_jsonld_values(html)
    avma_study_data = _script_json_by_id(html, "d_avma_study_data")
    avma_content_data = _script_json_by_id(html, "d_avma_studycontent_data")
    keywords = _script_json_by_id(html, "d_study_keywords")
    fields: dict[str, Any] = {
        "parser": profile.parser,
        "storage_policy": profile.storage_policy,
        "links": _html_links(html, source_url)[:100],
    }
    if isinstance(keywords, list) and keywords:
        fields["keywords"] = [_clean_text(str(keyword)) for keyword in keywords if str(keyword).strip()]
    if isinstance(avma_study_data, dict):
        _copy_text_fields(
            avma_study_data,
            fields,
            {
                "vct_code": "vct_code",
                "selection_species": "species",
                "selection_study_type": "study_type",
                "selection_intervention_type": "intervention_type",
                "selection_financial_incentive": "funding",
                "selection_primary_field": "primary_field",
            },
        )
        for flag in ("patients_randomly_assigned", "investigator_aware", "owners_aware"):
            if flag in avma_study_data and avma_study_data[flag] is not None:
                fields[flag] = avma_study_data[flag]
    if isinstance(avma_content_data, dict):
        _copy_text_fields(
            avma_content_data,
            fields,
            {
                "diagnosis": "condition",
                "inclusion_criteria": "eligibility",
                "exclusion_criteria": "exclusion_criteria",
                "intervention_name": "intervention",
                "potential_benefits": "potential_benefits",
                "potential_risks": "potential_risks",
                "study_results": "study_results",
                "funding_source_names": "funding_source_names",
            },
        )
        primary_outcome = _outcome_from_content_data(avma_content_data, "pri_outcome")
        if primary_outcome:
            fields["primary_outcome"] = primary_outcome
        secondary_outcomes = [
            outcome
            for prefix in ("sec_outcome1", "sec_outcome2")
            if (outcome := _outcome_from_content_data(avma_content_data, prefix))
        ]
        if secondary_outcomes:
            fields["secondary_outcomes"] = secondary_outcomes
        funding_sources = _funding_sources_from_content_data(avma_content_data)
        if funding_sources:
            fields["funding_sources"] = funding_sources

    title = _first_present(
        jsonld_values.get("name"),
        _meta_content(html, "og:title"),
        _heading(html, 1),
        _best_title(html),
    )
    summary = _first_present(
        jsonld_values.get("description"),
        _meta_content(html, "description"),
        _first_long_paragraph(html),
    )
    if summary:
        fields["summary"] = summary

    label_map = {
        "condition": ("condition", "disease", "diagnosis", "cancer type"),
        "species": ("species",),
        "study_type": ("study type", "trial type", "category"),
        "intervention": ("intervention", "treatment", "drug", "device", "procedure"),
        "funding": ("funding", "financial support", "cost"),
        "status": ("status", "recruitment status"),
        "location": ("location", "locations", "site", "sites"),
        "institution": ("institution", "organization", "hospital", "university"),
        "investigator": ("investigator", "principal investigator", "study contact"),
        "contact": ("contact", "email", "phone"),
        "eligibility": ("eligibility", "inclusion criteria", "criteria"),
    }
    for field_name, aliases in label_map.items():
        value = _lookup_label_value(label_values, aliases)
        if value:
            fields[field_name] = value

    inferred = _infer_avma_fields(text)
    for field_name, value in inferred.items():
        fields.setdefault(field_name, value)

    confidence = _avma_confidence(title, fields, summary)
    return ScrapeParsedRecord(
        source_key=profile.source_key,
        source_record_id=_stable_source_record_id(source_url, title, artifact.artifact_id),
        title=title,
        canonical_url=source_url,
        record_type=ResearchObjectType.VETERINARY_TRIAL,
        fields=fields,
        parser_confidence=confidence,
        review_status="needs_review",
        artifact_id=artifact.artifact_id,
    )


PARSER_REGISTRY: dict[str, ParserFunc] = {
    "generic_html": parse_generic_html,
    "avma_vctr": parse_avma_vctr_html,
}


def _discover_avma_vctr_candidates(
    profile: ScrapeSourceProfile,
    artifact: ArtifactHandle,
    html: str,
) -> list[ScrapeManifestItem]:
    source_url = artifact.metadata.get("source_url")
    candidates: list[ScrapeManifestItem] = []
    for link in _html_links(html, source_url):
        href = str(link.get("href") or "")
        text = str(link.get("text") or "")
        if not _allowed_by_profile(profile, href):
            continue
        parsed = urllib.parse.urlparse(href)
        path = parsed.path.lower()
        haystack = f"{path} {text}".lower()
        if "/s/" not in path:
            continue
        score = 0.45
        reason = "avma_study_path"
        if any(term in haystack for term in ("trial", "study", "hemangiosarcoma", "angiosarcoma", "sarcoma", "tumor")):
            score = 0.8
            reason = "avma_study_path_with_trial_terms"
        candidates.append(
            ScrapeManifestItem(
                url=href,
                link_text=text or None,
                discovered_from=source_url,
                reason=reason,
                confidence=score,
            )
        )
    return _dedupe_manifest_items(candidates)


def _discover_generic_candidates(
    profile: ScrapeSourceProfile,
    artifact: ArtifactHandle,
    html: str,
) -> list[ScrapeManifestItem]:
    source_url = artifact.metadata.get("source_url")
    candidates: list[ScrapeManifestItem] = []
    for link in _html_links(html, source_url):
        href = str(link.get("href") or "")
        text = str(link.get("text") or "")
        if not _allowed_by_profile(profile, href):
            continue
        haystack = f"{href} {text}".lower()
        if not any(term in haystack for term in ("trial", "study", "clinical", "hemangiosarcoma", "angiosarcoma")):
            continue
        candidates.append(
            ScrapeManifestItem(
                url=href,
                link_text=text or None,
                discovered_from=source_url,
                reason="generic_trial_terms",
                confidence=0.55,
            )
        )
    return _dedupe_manifest_items(candidates)


def _best_title(html: str) -> str | None:
    return _first_present(_heading(html, 1), _meta_content(html, "og:title"), _title_tag(html))


def _title_tag(html: str) -> str | None:
    match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.I | re.S)
    if not match:
        return None
    return _clean_html(match.group(1))


def _heading(html: str, level: int) -> str | None:
    match = re.search(rf"<h{level}\b[^>]*>(.*?)</h{level}>", html, flags=re.I | re.S)
    if not match:
        return None
    return _clean_html(match.group(1))


def _meta_content(html: str, name_or_property: str) -> str | None:
    attr_pattern = re.escape(name_or_property)
    patterns = [
        rf"<meta\b[^>]*(?:name|property)=[\"']{attr_pattern}[\"'][^>]*content=[\"']([^\"']+)[\"'][^>]*>",
        rf"<meta\b[^>]*content=[\"']([^\"']+)[\"'][^>]*(?:name|property)=[\"']{attr_pattern}[\"'][^>]*>",
    ]
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.I | re.S)
        if match:
            return _clean_text(match.group(1))
    return None


def _html_links(html: str, base_url: str | None) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = []
    for match in re.finditer(r"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", html, flags=re.I | re.S):
        href = unescape(match.group(1)).strip()
        text = _clean_html(match.group(2))
        links.append(
            {
                "href": urllib.parse.urljoin(base_url or "", href),
                "text": text,
            }
        )
    return links


def _primary_source_links(html: str, links: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for link in links:
        href = str(link.get("href") or "")
        text = str(link.get("text") or "")
        classified = _classify_primary_source_link(href, text)
        if classified:
            candidates.append(classified)

    text = _visible_text(html)
    for doi in _extract_dois(text):
        candidates.append(
            {
                "url": f"https://doi.org/{doi}",
                "recommended_source_key": "crossref",
                "identifier_type": "doi",
                "identifier": doi,
                "should_ingest": True,
                "reason": "DOI found in linked article text.",
            }
        )
    for pmid in sorted(set(re.findall(r"\bPMID[:\s]*(\d{5,})\b", text, flags=re.I))):
        candidates.append(
            {
                "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                "recommended_source_key": "pubmed",
                "identifier_type": "pmid",
                "identifier": pmid,
                "should_ingest": True,
                "reason": "PMID found in linked article text.",
            }
        )
    for nct in sorted(set(re.findall(r"\b(NCT\d{8})\b", text, flags=re.I))):
        nct = nct.upper()
        candidates.append(
            {
                "url": f"https://clinicaltrials.gov/study/{nct}",
                "recommended_source_key": "clinicaltrials_gov",
                "identifier_type": "nct",
                "identifier": nct,
                "should_ingest": True,
                "reason": "ClinicalTrials.gov identifier found in linked article text.",
            }
        )
    return _dedupe_primary_source_links(candidates)


def _classify_primary_source_link(url: str, text: str = "") -> dict[str, Any] | None:
    parsed = urllib.parse.urlparse(url)
    host_path = f"{parsed.netloc}{parsed.path}".lower()
    doi = _extract_doi(url) or next(iter(_extract_dois(text)), None)
    if doi:
        return {
            "url": f"https://doi.org/{doi}",
            "recommended_source_key": "crossref",
            "identifier_type": "doi",
            "identifier": doi,
            "should_ingest": True,
            "reason": "DOI link found in linked article.",
        }
    if "pubmed.ncbi.nlm.nih.gov" in host_path:
        match = re.search(r"/(\d{5,})/?(?:$|[?#])", url)
        if match:
            return {
                "url": url,
                "recommended_source_key": "pubmed",
                "identifier_type": "pmid",
                "identifier": match.group(1),
                "should_ingest": True,
                "reason": "PubMed link found in linked article.",
            }
    if "pmc.ncbi.nlm.nih.gov" in host_path:
        match = re.search(r"/articles/(PMC\d+)/?", url, flags=re.I)
        if match:
            return {
                "url": url,
                "recommended_source_key": "pmc_oa",
                "identifier_type": "pmcid",
                "identifier": match.group(1).upper(),
                "should_ingest": True,
                "reason": "PMC link found in linked article.",
            }
    if "clinicaltrials.gov" in host_path:
        match = re.search(r"(NCT\d{8})", url, flags=re.I)
        if match:
            nct = match.group(1).upper()
            return {
                "url": url,
                "recommended_source_key": "clinicaltrials_gov",
                "identifier_type": "nct",
                "identifier": nct,
                "should_ingest": True,
                "reason": "ClinicalTrials.gov link found in linked article.",
            }
    return None


def _extract_doi(value: str) -> str | None:
    parsed = urllib.parse.urlparse(value)
    if "doi.org" in parsed.netloc.lower():
        doi = urllib.parse.unquote(parsed.path.lstrip("/"))
        return _clean_doi(doi)
    return next(iter(_extract_dois(value)), None)


def _extract_dois(value: str) -> list[str]:
    dois = [doi for match in _DOI_RE.finditer(value) if (doi := _clean_doi(match.group(0)))]
    return sorted(set(dois))


def _clean_doi(value: str) -> str | None:
    doi = value.strip().rstrip(_DOI_TRAILING_CHARS)
    return doi or None


def _dedupe_primary_source_links(links: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for link in links:
        key = (
            str(link.get("identifier_type") or "unknown"),
            str(link.get("identifier") or link.get("url") or ""),
        )
        deduped.setdefault(key, link)
    return list(deduped.values())


def _extract_label_values(html: str) -> dict[str, str]:
    values: dict[str, str] = {}
    patterns = [
        r"<dt\b[^>]*>(.*?)</dt>\s*<dd\b[^>]*>(.*?)</dd>",
        r"<th\b[^>]*>(.*?)</th>\s*<td\b[^>]*>(.*?)</td>",
        r"<(?:strong|b)\b[^>]*>\s*([^:<]{2,80}):?\s*</(?:strong|b)>\s*([^<]{1,500})",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, html, flags=re.I | re.S):
            label = _normalize_label(_clean_html(match.group(1)))
            value = _clean_html(match.group(2))
            if label and value:
                values[label] = value
    return values


def _lookup_label_value(label_values: dict[str, str], aliases: tuple[str, ...]) -> str | None:
    normalized_aliases = {_normalize_label(alias) for alias in aliases}
    for label, value in label_values.items():
        if label in normalized_aliases:
            return value
    for label, value in label_values.items():
        if any(alias in label for alias in normalized_aliases):
            return value
    return None


def _extract_jsonld_values(html: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for match in re.finditer(
        r"<script\b[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        html,
        flags=re.I | re.S,
    ):
        try:
            payload = json.loads(unescape(match.group(1)).strip())
        except json.JSONDecodeError:
            continue
        candidates = payload if isinstance(payload, list) else [payload]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            for key in ("name", "description"):
                value = candidate.get(key)
                if isinstance(value, str) and value.strip():
                    values.setdefault(key, _clean_text(value))
    return values


def _script_json_by_id(html: str, element_id: str) -> Any:
    pattern = rf"<script\b[^>]*id=[\"']{re.escape(element_id)}[\"'][^>]*type=[\"']application/json[\"'][^>]*>(.*?)</script>"
    match = re.search(pattern, html, flags=re.I | re.S)
    if not match:
        return None
    value = unescape(match.group(1)).strip()
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _copy_text_fields(source: dict[str, Any], target: dict[str, Any], field_map: dict[str, str]) -> None:
    for source_key, target_key in field_map.items():
        value = source.get(source_key)
        if isinstance(value, str) and value.strip():
            target[target_key] = _clean_html(value)


def _outcome_from_content_data(source: dict[str, Any], prefix: str) -> dict[str, str] | None:
    outcome: dict[str, str] = {}
    for suffix, field_name in (("name", "name"), ("measure", "measure"), ("endpoint", "endpoint")):
        value = source.get(f"{prefix}_{suffix}")
        if isinstance(value, str) and value.strip():
            outcome[field_name] = _clean_html(value)
    return outcome or None


def _funding_sources_from_content_data(source: dict[str, Any]) -> list[str]:
    funding_sources: list[str] = []
    for key, label in (
        ("funding_source_government", "government"),
        ("funding_source_foundation", "foundation"),
        ("funding_source_company", "company"),
        ("funding_source_institution", "institution"),
        ("funding_source_none", "none"),
    ):
        if source.get(key) is True:
            funding_sources.append(label)
    return funding_sources


def _first_long_paragraph(html: str) -> str | None:
    for match in re.finditer(r"<p\b[^>]*>(.*?)</p>", html, flags=re.I | re.S):
        paragraph = _clean_html(match.group(1))
        if len(paragraph) >= 80:
            return paragraph
    return None


def _infer_avma_fields(text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    lowered = text.lower()
    diseases = [
        "hemangiosarcoma",
        "angiosarcoma",
        "histiocytic sarcoma",
        "osteosarcoma",
        "soft tissue sarcoma",
        "mast cell tumor",
        "solid tumor",
    ]
    for disease in diseases:
        if disease in lowered:
            fields["condition"] = disease.title()
            break
    if re.search(r"\b(canine|dog|dogs)\b", lowered):
        fields["species"] = "Canine"
    elif re.search(r"\b(feline|cat|cats)\b", lowered):
        fields["species"] = "Feline"
    for intervention in ("Drug", "Device", "Surgery/Other procedure", "Radiation", "Immunotherapy"):
        if intervention.lower() in lowered:
            fields["study_type"] = intervention
            break
    funding_match = re.search(
        r"\b((?:fully|partially|un)funded(?: [a-z0-9$><=\- ]{0,80})?)",
        text,
        flags=re.I,
    )
    if funding_match:
        fields["funding"] = _clean_text(funding_match.group(1))
    status_match = re.search(r"\b(recruiting|enrolling|active|closed|completed)\b", text, flags=re.I)
    if status_match:
        fields["status"] = status_match.group(1).title()
    return fields


def _avma_confidence(title: str | None, fields: dict[str, Any], summary: str | None) -> float:
    score = 0.0
    if title:
        score += 0.25
    if fields.get("condition"):
        score += 0.2
    if fields.get("species"):
        score += 0.15
    if fields.get("vct_code"):
        score += 0.1
    if fields.get("intervention"):
        score += 0.1
    if summary:
        score += 0.1
    for optional in ("status", "funding", "study_type", "investigator", "institution", "location"):
        if fields.get(optional):
            score += 0.05
    return min(score, 0.9)


def _stable_source_record_id(source_url: str | None, title: str | None, artifact_id: UUID | None) -> str:
    if source_url:
        parsed = urllib.parse.urlparse(source_url)
        path = parsed.path.strip("/")
        if path:
            return f"{parsed.netloc}/{path}"
        return parsed.netloc or source_url
    if title:
        return "title:" + re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    return f"artifact:{artifact_id}"


def _allowed_by_profile(profile: ScrapeSourceProfile, url: str) -> bool:
    return any(_fnmatch_url(url, pattern) for pattern in profile.allowed_url_patterns)


def _fnmatch_url(url: str, pattern: str) -> bool:
    import fnmatch

    return fnmatch.fnmatch(url, pattern)


def _dedupe_manifest_items(items: list[ScrapeManifestItem]) -> list[ScrapeManifestItem]:
    deduped: dict[str, ScrapeManifestItem] = {}
    for item in items:
        existing = deduped.get(item.url)
        if existing is None or item.confidence > existing.confidence:
            deduped[item.url] = item
    return sorted(deduped.values(), key=lambda item: (-item.confidence, item.url))


def _visible_text(html: str) -> str:
    cleaned = re.sub(r"<(script|style)\b[^>]*>.*?</\1>", " ", html, flags=re.I | re.S)
    return _clean_html(cleaned)


def _clean_html(value: str) -> str:
    return _clean_text(re.sub(r"<[^>]+>", " ", value))


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(value)).strip()


def _normalize_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _first_present(*values: str | None) -> str | None:
    for value in values:
        if value and value.strip():
            return value.strip()
    return None
