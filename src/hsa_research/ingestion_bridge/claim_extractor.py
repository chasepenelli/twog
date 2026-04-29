"""Conservative local claim extraction over document chunks.

This extractor is intentionally deterministic and low-confidence. Its job is to
populate the claim graph with provenance-backed drafts so we can test the v2
storage/search loop before using frontier models for deeper extraction.
"""

from __future__ import annotations

import re
from uuid import NAMESPACE_URL, uuid5

from .contracts import (
    ClaimDirection,
    ClaimExtractionResult,
    ClaimSearchResult,
    ClaimType,
    DocumentChunk,
    EntityMention,
    EntityRef,
    EvidenceLevel,
    ResearchObject,
)
from .local_store import SQLiteResearchRepository

EXTRACTOR_NAME = "local_rule_claim_extractor"
EXTRACTOR_VERSION = "0.1"

DISEASE_TERMS = (
    "hemangiosarcoma",
    "haemangiosarcoma",
    " hsa ",
    "splenic mass",
    "spontaneous hemoperitoneum",
)

CANINE_TERMS = ("canine", "dog", "dogs", "veterinary")

HUMAN_ANALOG_TERMS = (
    "angiosarcoma",
    "cutaneous angiosarcoma",
    "cardiac angiosarcoma",
    "hepatic angiosarcoma",
    "radiation-associated angiosarcoma",
    "hemangioendothelioma",
    "haemangioendothelioma",
    "vascular sarcoma",
    "endothelial sarcoma",
)

COMPOUNDS = {
    "propranolol": ("propranolol",),
    "doxorubicin": ("doxorubicin", "adriamycin"),
    "toceranib": ("toceranib", "palladia"),
    "sirolimus": ("sirolimus", "rapamycin"),
    "curcumin": ("curcumin",),
    "honokiol": ("honokiol",),
    "fucoidan": ("fucoidan",),
    "metformin": ("metformin",),
    "losartan": ("losartan",),
    "valproic acid": ("valproic acid", "valproate"),
    "vorinostat": ("vorinostat", "saha"),
    "panobinostat": ("panobinostat",),
    "tocopherol": ("tocopherol", "vitamin e"),
    "cyclophosphamide": ("cyclophosphamide",),
    "paclitaxel": ("paclitaxel",),
    "sorafenib": ("sorafenib",),
    "sunitinib": ("sunitinib",),
    "masitinib": ("masitinib",),
    "imatinib": ("imatinib",),
}

TARGETS = {
    "VEGFA": ("vegfa", "vegf", "vascular endothelial growth factor"),
    "KDR": ("kdr", "vegfr2", "vegfr-2", "vegf receptor 2"),
    "FLT4": ("flt4", "vegfr3", "vegfr-3"),
    "PDGFRB": ("pdgfrb", "pdgfr-beta", "pdgfr beta"),
    "KIT": ("kit", "c-kit", "cd117"),
    "PIK3CA": ("pik3ca", "pi3k"),
    "PIK3R6": ("pik3r6",),
    "AKT": ("akt", "protein kinase b"),
    "MTOR": ("mtor", "mammalian target of rapamycin"),
    "CD47": ("cd47",),
    "SIRPA": ("sirpa", "sirp-alpha", "signal regulatory protein alpha"),
    "TP53": ("tp53", "p53"),
    "POT1": ("pot1",),
    "ANGPT2": ("angpt2", "angiopoietin-2", "angiopoietin 2"),
    "MET": ("met", "c-met"),
    "EGFR": ("egfr",),
    "HDAC": ("hdac", "histone deacetylase"),
    "PTGS2": ("ptgs2", "cox-2", "cox2"),
    "MMP9": ("mmp9", "mmp-9"),
    "PLAUR": ("plaur", "upar"),
}

BIOMARKERS = {
    "CD31": ("cd31", "pecam1"),
    "CD34": ("cd34",),
    "CD44": ("cd44",),
    "CD105": ("cd105", "endoglin"),
    "CD117": ("cd117", "c-kit"),
    "CD133": ("cd133",),
    "CD146": ("cd146",),
    "CD204": ("cd204",),
    "CD206": ("cd206",),
    "ctDNA": ("ctdna", "circulating tumor dna"),
    "cfDNA": ("cfdna", "cell-free dna", "cell free dna"),
    "nucleosome": ("nucleosome", "nucleosomes"),
    "miRNA": ("mirna", "microrna", "micro-rna"),
}

PATHWAY_TERMS = {
    "angiogenesis": ("angiogenesis", "angiogenic"),
    "PI3K/AKT/mTOR": ("pi3k", "akt", "mtor"),
    "immune checkpoint": ("cd47", "sirpa", "checkpoint", "macrophage"),
    "epigenetic regulation": ("hdac", "histone deacetylase", "epigenetic"),
}

SAFETY_TERMS = (
    "toxicity",
    "adverse event",
    "adverse events",
    "contraindication",
    "dose-limiting",
    "dose limiting",
    "cardiotoxicity",
    "myelosuppression",
    "hepatotoxicity",
)

TRANSLATION_TERMS = (
    "ortholog",
    "orthologue",
    "homology",
    "sequence identity",
    "human",
    "comparative oncology",
    "translational",
)

SCHOLARLY_SOURCE_KEYS = {
    "current_papers",
    "pubmed",
    "europe_pmc",
    "openalex",
    "crossref",
    "pmc_oa",
    "unpaywall",
}
TRIAGE_CONTEXT_SOURCE_KEYS = SCHOLARLY_SOURCE_KEYS | {"icdc", "geo", "sra"}


class LocalRuleClaimExtractor:
    """Draft claim extractor with explicit source-chunk provenance."""

    def extract_chunk(self, chunk: DocumentChunk, obj: ResearchObject | None = None) -> list[ClaimSearchResult]:
        if obj and obj.source_key == "chembl":
            return _dedupe_claims(self._extract_chembl_chunk(chunk, obj))
        if obj and obj.source_key == "pubchem":
            return _dedupe_claims(self._extract_pubchem_chunk(chunk, obj))
        if obj and obj.source_key == "uniprot":
            return _dedupe_claims(self._extract_uniprot_chunk(chunk, obj))
        if obj and obj.source_key == "rcsb_pdb":
            return _dedupe_claims(self._extract_rcsb_chunk(chunk, obj))
        if obj and obj.source_key == "openfda_animal_events":
            return _dedupe_claims(self._extract_openfda_chunk(chunk, obj))

        text = f"{obj.title if obj and obj.title else ''}\n{chunk.text_content}"
        if obj and obj.source_key == "avma_vctr" and not _avma_trial_primary_scope_matches(obj):
            return []
        normalized = _normalize_context(text)
        context = _context_scope(normalized)
        if context is None:
            return []

        compounds = _find_entities(normalized, COMPOUNDS)
        targets = _find_entities(normalized, TARGETS)
        biomarkers = _find_entities(normalized, BIOMARKERS)
        pathways = _find_entities(normalized, PATHWAY_TERMS)
        disease_entity = EntityRef(entity_type="disease", canonical_name=context["disease_name"], role="disease")
        claims: list[ClaimSearchResult] = []

        for compound in compounds[:4]:
            claims.append(
                self._claim(
                    chunk,
                    obj,
                    context=context,
                    rule_key=f"compound:{compound}",
                    statement=f"{compound} is discussed in {context['evidence_label']} evidence as a treatment or intervention candidate.",
                    claim_type=ClaimType.COMPOUND_AFFECTS_OUTCOME,
                    direction=ClaimDirection.UNKNOWN,
                    confidence=0.38,
                    entities=[
                        EntityRef(entity_type="compound", canonical_name=compound, role="compound"),
                        disease_entity,
                    ],
                )
            )

        for compound in compounds[:3]:
            for target in targets[:3]:
                claims.append(
                    self._claim(
                        chunk,
                        obj,
                        context=context,
                        rule_key=f"compound-target:{compound}:{target}",
                        statement=f"{compound} is mentioned with {target} in {context['evidence_label']}-relevant evidence.",
                        claim_type=ClaimType.COMPOUND_MODULATES_TARGET,
                        direction=ClaimDirection.UNKNOWN,
                        confidence=0.34,
                        entities=[
                            EntityRef(entity_type="compound", canonical_name=compound, role="compound"),
                            EntityRef(entity_type="target", canonical_name=target, role="target"),
                            disease_entity,
                        ],
                    )
                )

        for target in targets[:5]:
            claims.append(
                self._claim(
                    chunk,
                    obj,
                    context=context,
                    rule_key=f"target:{target}",
                    statement=f"{target} is associated with {context['evidence_label']}-relevant biology in this source context.",
                    claim_type=ClaimType.TARGET_ASSOCIATED_WITH_DISEASE,
                    direction=ClaimDirection.POSITIVE,
                    confidence=0.42,
                    entities=[
                        EntityRef(entity_type="target", canonical_name=target, role="target"),
                        disease_entity,
                    ],
                )
            )

        for biomarker in biomarkers[:4]:
            claims.append(
                self._claim(
                    chunk,
                    obj,
                    context=context,
                    rule_key=f"biomarker:{biomarker}",
                    statement=f"{biomarker} is discussed as a biomarker, phenotype marker, or detection feature in {context['evidence_label']} context.",
                    claim_type=ClaimType.BIOMARKER_PREDICTS_STATE,
                    direction=ClaimDirection.UNKNOWN,
                    confidence=0.4,
                    entities=[
                        EntityRef(entity_type="target", canonical_name=biomarker, role="biomarker"),
                        disease_entity,
                    ],
                )
            )

        for pathway in pathways[:3]:
            claims.append(
                self._claim(
                    chunk,
                    obj,
                    context=context,
                    rule_key=f"pathway:{pathway}",
                    statement=f"{pathway} is discussed as active or therapeutically relevant in {context['evidence_label']} context.",
                    claim_type=ClaimType.PATHWAY_ACTIVE_IN_DISEASE,
                    direction=ClaimDirection.POSITIVE,
                    confidence=0.4,
                    entities=[
                        EntityRef(entity_type="pathway", canonical_name=pathway, role="pathway"),
                        disease_entity,
                    ],
                )
            )

        if compounds and any(term in normalized for term in SAFETY_TERMS):
            for compound in compounds[:3]:
                claims.append(
                    self._claim(
                        chunk,
                        obj,
                        context=context,
                        rule_key=f"safety:{compound}",
                        statement=f"{compound} is mentioned with a safety, toxicity, or tolerability signal.",
                        claim_type=ClaimType.SAFETY_SIGNAL,
                        direction=ClaimDirection.UNKNOWN,
                        confidence=0.36,
                        entities=[
                            EntityRef(entity_type="compound", canonical_name=compound, role="compound"),
                            EntityRef(entity_type="outcome", canonical_name="safety signal", role="outcome"),
                            disease_entity,
                        ],
                    )
                )

        if targets and _has_species_translation_context(normalized):
            for target in targets[:3]:
                claims.append(
                    self._claim(
                        chunk,
                        obj,
                        context=context,
                        rule_key=f"translation:{target}",
                        statement=f"{target} has canine-human translational context in this source.",
                        claim_type=ClaimType.SPECIES_TRANSLATION,
                        direction=ClaimDirection.UNKNOWN,
                        confidence=0.35,
                        entities=[
                            EntityRef(entity_type="target", canonical_name=target, role="target"),
                            EntityRef(entity_type="species", canonical_name="canine", role="species"),
                            EntityRef(entity_type="species", canonical_name="human", role="comparator_species"),
                        ],
                    )
                )

        if not claims and obj and obj.source_key in TRIAGE_CONTEXT_SOURCE_KEYS:
            claims.append(
                self._claim(
                    chunk,
                    obj,
                    context=context,
                    rule_key=f"source-context:{context['context_key']}",
                    statement=(
                        f"{_source_label(obj.source_key)} record provides {context['evidence_label']} "
                        "source context relevant to HSA evidence triage."
                    ),
                    claim_type=ClaimType.OTHER,
                    direction=ClaimDirection.NEUTRAL,
                    confidence=0.3,
                    entities=[disease_entity],
                )
            )

        return _dedupe_claims(claims)

    def _extract_pubchem_chunk(self, chunk: DocumentChunk, obj: ResearchObject) -> list[ClaimSearchResult]:
        metadata = obj.metadata or {}
        compound = _clean_entity_name(obj.title or metadata.get("query_term"))
        cid = obj.identifiers.get("pubchem_cid")
        inchikey = obj.identifiers.get("inchikey") or metadata.get("inchikey")
        if not compound or not cid:
            return []
        identity_match = metadata.get("identity_match") if isinstance(metadata.get("identity_match"), dict) else {}
        if identity_match and identity_match.get("identity_verified") is False:
            return []
        statement = f"{compound} has PubChem compound identity CID {cid}"
        if inchikey:
            statement += f" and InChIKey {inchikey}"
        statement += "."
        return [
            self._claim(
                chunk,
                obj,
                context={
                    "context_key": "pubchem_compound_identity",
                    "evidence_label": "PubChem compound identity",
                    "species": None,
                },
                rule_key=f"pubchem-identity:{cid}",
                statement=statement,
                claim_type=ClaimType.OTHER,
                direction=ClaimDirection.NEUTRAL,
                confidence=0.5,
                entities=[
                    EntityRef(entity_type="compound", canonical_name=compound, role="compound", external_ids={"pubchem_cid": cid}),
                    EntityRef(entity_type="identifier", canonical_name=f"PubChem CID {cid}", role="compound_identifier"),
                ],
            )
        ]

    def _extract_uniprot_chunk(self, chunk: DocumentChunk, obj: ResearchObject) -> list[ClaimSearchResult]:
        metadata = obj.metadata or {}
        target = _clean_entity_name(metadata.get("target_gene") or (metadata.get("gene_names") or [None])[0])
        accession = obj.identifiers.get("uniprot_accession")
        organism = _clean_entity_name(metadata.get("organism"))
        if not target or not accession or not organism:
            return []
        alpha_ids = metadata.get("alphafold_ids") if isinstance(metadata.get("alphafold_ids"), list) else []
        statement = f"{target} has UniProtKB target metadata for {organism} with accession {accession}"
        if alpha_ids:
            statement += f" and AlphaFold model {alpha_ids[0]}"
        statement += "."
        return [
            self._claim(
                chunk,
                obj,
                context={
                    "context_key": "uniprot_target_identity",
                    "evidence_label": "UniProt target identity",
                    "species": metadata.get("species_scope"),
                },
                rule_key=f"uniprot-target:{target}:{accession}",
                statement=statement,
                claim_type=ClaimType.OTHER,
                direction=ClaimDirection.NEUTRAL,
                confidence=0.54 if metadata.get("reviewed") else 0.48,
                entities=[
                    EntityRef(entity_type="target", canonical_name=target, role="target", external_ids={"uniprot_accession": accession}),
                    EntityRef(entity_type="species", canonical_name=organism, role="species"),
                ],
            )
        ]

    def _extract_rcsb_chunk(self, chunk: DocumentChunk, obj: ResearchObject) -> list[ClaimSearchResult]:
        metadata = obj.metadata or {}
        pdb_id = obj.identifiers.get("pdb_id")
        target = _clean_entity_name(metadata.get("target_gene") or metadata.get("query_term"))
        methods = metadata.get("experimental_methods") if isinstance(metadata.get("experimental_methods"), list) else []
        if not pdb_id or not target:
            return []
        method_text = "; ".join(str(method) for method in methods[:2]) if methods else "experimental structure metadata"
        statement = f"RCSB PDB contains experimental structure {pdb_id} supporting {target} structure context using {method_text}."
        return [
            self._claim(
                chunk,
                obj,
                context={
                    "context_key": "rcsb_structure_support",
                    "evidence_label": "RCSB experimental structure support",
                    "species": None,
                },
                rule_key=f"rcsb-structure:{target}:{pdb_id}",
                statement=statement,
                claim_type=ClaimType.OTHER,
                direction=ClaimDirection.NEUTRAL,
                confidence=0.52,
                entities=[
                    EntityRef(entity_type="target", canonical_name=target, role="target"),
                    EntityRef(entity_type="structure", canonical_name=f"PDB {pdb_id}", role="structure", external_ids={"pdb_id": pdb_id}),
                ],
            )
        ]

    def _extract_openfda_chunk(self, chunk: DocumentChunk, obj: ResearchObject) -> list[ClaimSearchResult]:
        metadata = obj.metadata or {}
        drug = _clean_entity_name(metadata.get("matched_drug_name") or metadata.get("source_query"))
        species = _clean_entity_name(metadata.get("species"))
        reactions = metadata.get("reaction_terms") if isinstance(metadata.get("reaction_terms"), list) else []
        if not drug or not species or not reactions:
            return []
        reaction_text = "; ".join(str(reaction) for reaction in reactions[:6])
        statement = (
            f"{drug} has openFDA animal adverse event signal reports in {species} with reported reactions: "
            f"{reaction_text}. This is signal-generation evidence only, not incidence or causality."
        )
        return [
            self._claim(
                chunk,
                obj,
                context={
                    "context_key": "openfda_veterinary_safety_signal",
                    "evidence_label": "openFDA veterinary safety signal",
                    "species": _openfda_species_scope(species),
                },
                rule_key=f"openfda-safety:{drug}:{obj.identifiers.get('openfda_report_id')}",
                statement=statement,
                claim_type=ClaimType.SAFETY_SIGNAL,
                direction=ClaimDirection.UNKNOWN,
                confidence=0.46 if _truthy(metadata.get("serious_ae")) else 0.42,
                entities=[
                    EntityRef(entity_type="compound", canonical_name=drug, role="compound"),
                    EntityRef(entity_type="outcome", canonical_name=reaction_text, role="safety_signal"),
                    EntityRef(entity_type="species", canonical_name=species, role="species"),
                ],
            )
        ]

    def _extract_chembl_chunk(self, chunk: DocumentChunk, obj: ResearchObject) -> list[ClaimSearchResult]:
        metadata = obj.metadata or {}
        compound = _clean_entity_name(metadata.get("molecule_pref_name") or metadata.get("query_term") or obj.title)
        target_name = _clean_entity_name(metadata.get("target_pref_name"))
        target_gene = _clean_entity_name(metadata.get("target_gene"))
        target_category = _clean_entity_name(metadata.get("target_category"))
        matched_cell_line_term = _clean_entity_name(metadata.get("matched_cell_line_term"))
        pchembl = metadata.get("pchembl_value")
        standard_type = metadata.get("standard_type")
        standard_relation = metadata.get("standard_relation")
        standard_value = metadata.get("standard_value")
        standard_units = metadata.get("standard_units")
        species = _chembl_species(metadata.get("target_organism"))
        measurement = _chembl_measurement_phrase(
            standard_type=standard_type,
            standard_relation=standard_relation,
            standard_value=standard_value,
            standard_units=standard_units,
            pchembl=pchembl,
        )
        confidence = _chembl_claim_confidence(metadata)

        if not compound or not target_name:
            return []

        if target_category == "cell_cytotoxicity":
            statement = f"{compound} shows ChEMBL functional activity in {target_name} cell-line assay{measurement}."
            return [
                self._claim(
                    chunk,
                    obj,
                    context={
                        "context_key": "chembl_cell_line_bioactivity",
                        "evidence_label": "ChEMBL cell-line bioactivity",
                        "species": species or "human",
                    },
                    rule_key=f"chembl-cell-line:{compound}:{target_name}:{pchembl}",
                    statement=statement,
                    claim_type=ClaimType.COMPOUND_AFFECTS_OUTCOME,
                    direction=ClaimDirection.UNKNOWN,
                    confidence=max(0.42, confidence - 0.04),
                    entities=[
                        EntityRef(entity_type="compound", canonical_name=compound, role="compound"),
                        EntityRef(entity_type="cell_line", canonical_name=target_name, role="model"),
                        EntityRef(
                            entity_type="outcome",
                            canonical_name=f"{matched_cell_line_term or 'cell-line'} functional activity",
                            role="outcome",
                        ),
                    ],
                )
            ]

        target_label = f"{target_name} ({target_gene})" if target_gene else target_name
        statement = f"{compound} shows ChEMBL bioactivity against {target_label}{measurement}."
        entities = [
            EntityRef(entity_type="compound", canonical_name=compound, role="compound"),
            EntityRef(
                entity_type="target",
                canonical_name=target_gene or target_name,
                role="target",
                external_ids={"chembl_target_id": obj.identifiers.get("chembl_target_id", "")},
            ),
        ]
        if target_category:
            entities.append(EntityRef(entity_type="pathway", canonical_name=target_category, role="mechanism_class"))
        return [
            self._claim(
                chunk,
                obj,
                context={
                    "context_key": "chembl_target_bioactivity",
                    "evidence_label": "ChEMBL target bioactivity",
                    "species": species or "unknown",
                },
                rule_key=f"chembl-target:{compound}:{target_gene or target_name}:{pchembl}",
                statement=statement,
                claim_type=ClaimType.COMPOUND_MODULATES_TARGET,
                direction=ClaimDirection.UNKNOWN,
                confidence=confidence,
                entities=entities,
            )
        ]

    def _claim(
        self,
        chunk: DocumentChunk,
        obj: ResearchObject | None,
        *,
        context: dict[str, str],
        rule_key: str,
        statement: str,
        claim_type: ClaimType,
        direction: ClaimDirection,
        confidence: float,
        entities: list[EntityRef],
    ) -> ClaimSearchResult:
        claim_id = uuid5(NAMESPACE_URL, f"{EXTRACTOR_NAME}:{EXTRACTOR_VERSION}:{chunk.id}:{rule_key}")
        source_url = obj.canonical_url if obj else None
        return ClaimSearchResult(
            claim_id=claim_id,
            statement=statement,
            claim_type=claim_type,
            direction=direction,
            confidence=confidence,
            evidence_level=_infer_evidence_level(obj),
            species=context["species"],
            entities=entities,
            source_object_id=chunk.research_object_id,
            source_title=obj.title if obj else None,
            source_url=source_url,
            support_count=1,
            metadata={
                "extraction_status": "draft",
                "extractor_name": EXTRACTOR_NAME,
                "extractor_version": EXTRACTOR_VERSION,
                "source_chunk_id": str(chunk.id),
                "source_chunk_index": chunk.chunk_index,
                "section_label": chunk.section_label,
                "rule_key": rule_key,
                "context_key": context["context_key"],
                "context_label": context["evidence_label"],
                "evidence_snippet": _snippet(chunk.text_content),
            },
        )


def extract_claims_for_repository(
    repository: SQLiteResearchRepository,
    *,
    source_key: str | None = None,
    object_type: str | None = None,
    limit: int | None = None,
) -> ClaimExtractionResult:
    """Extract and persist draft claims for local chunks."""

    extractor = LocalRuleClaimExtractor()
    result = ClaimExtractionResult(extractor_name=f"{EXTRACTOR_NAME}:{EXTRACTOR_VERSION}")
    chunks = repository.list_document_chunks(source_key=source_key, object_type=object_type, limit=limit)

    for chunk in chunks:
        result.chunks_seen += 1
        try:
            obj = repository.get_research_object(chunk.research_object_id)
            claims = extractor.extract_chunk(chunk, obj)
            entity_mentions = repository.list_entity_mentions(chunk_id=chunk.id)
            if entity_mentions:
                claims = [_with_source_entity_mentions(claim, entity_mentions) for claim in claims]
            if claims:
                result.chunks_with_claims += 1
            result.claims_extracted += len(claims)
            for claim in claims:
                repository.upsert_claim(claim)
                result.claims_written += 1
        except Exception as exc:
            result.errors.append(f"{chunk.id}: {exc}")

    return result


def _with_source_entity_mentions(
    claim: ClaimSearchResult,
    entity_mentions: list[EntityMention],
) -> ClaimSearchResult:
    metadata = dict(claim.metadata)
    metadata.update(
        {
            "source_entity_mention_ids": [str(mention.mention_id) for mention in entity_mentions],
            "source_entity_canonical_names": [mention.canonical_name for mention in entity_mentions],
            "source_entity_types": [mention.entity_type for mention in entity_mentions],
        }
    )
    return claim.model_copy(update={"metadata": metadata})


def _clean_entity_name(value: object) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned or None


def _chembl_species(target_organism: object) -> str | None:
    organism = str(target_organism or "").lower()
    if organism == "homo sapiens":
        return "human"
    if organism == "canis lupus familiaris":
        return "canine"
    return None


def _chembl_measurement_phrase(
    *,
    standard_type: object,
    standard_relation: object,
    standard_value: object,
    standard_units: object,
    pchembl: object,
) -> str:
    pieces = []
    if standard_type and standard_value:
        relation = str(standard_relation or "").strip()
        value = " ".join(str(part) for part in (standard_value, standard_units) if part)
        pieces.append(f"{standard_type} {relation} {value}".replace("  ", " ").strip())
    if pchembl:
        pieces.append(f"pChEMBL {pchembl}")
    return f" ({'; '.join(pieces)})" if pieces else ""


def _chembl_claim_confidence(metadata: dict[str, object]) -> float:
    pchembl = _safe_float(metadata.get("pchembl_numeric") or metadata.get("pchembl_value"))
    if pchembl is None:
        return 0.46
    if pchembl >= 9:
        return 0.66
    if pchembl >= 8:
        return 0.62
    if pchembl >= 6:
        return 0.56
    return 0.48


def _safe_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _openfda_species_scope(species: str | None) -> str | None:
    if str(species or "").lower() == "dog":
        return "canine"
    return str(species).lower() if species else None


def _source_label(source_key: str) -> str:
    return {
        "current_papers": "Literature",
        "pubmed": "PubMed",
        "europe_pmc": "Europe PMC",
        "openalex": "OpenAlex",
        "crossref": "Crossref",
        "pmc_oa": "PMC OA",
        "unpaywall": "Unpaywall",
        "icdc": "ICDC",
        "geo": "GEO",
        "sra": "SRA",
    }.get(source_key, source_key)


def _normalize_context(text: str) -> str:
    normalized_text = re.sub(r"\s+", " ", text).lower()
    return f" {normalized_text} "


def _context_scope(text: str) -> dict[str, str] | None:
    canine_hsa = any(term in text for term in DISEASE_TERMS) and any(term in text for term in CANINE_TERMS)
    human_analog = any(_contains_term(text, term) for term in HUMAN_ANALOG_TERMS)
    comparative = "comparative oncology" in text or "translational oncology" in text or (canine_hsa and human_analog)

    if comparative and canine_hsa and human_analog:
        return {
            "context_key": "canine_human_comparative",
            "evidence_label": "canine-human comparative angiosarcoma/HSA",
            "disease_name": "canine hemangiosarcoma / human angiosarcoma",
            "species": "comparative",
        }
    if canine_hsa:
        return {
            "context_key": "canine_hsa",
            "evidence_label": "canine HSA",
            "disease_name": "canine hemangiosarcoma",
            "species": "canine",
        }
    if human_analog:
        return {
            "context_key": "human_angiosarcoma_analog",
            "evidence_label": "human angiosarcoma or vascular sarcoma analog",
            "disease_name": "human angiosarcoma / vascular sarcoma analog",
            "species": "human",
        }
    if comparative:
        return {
            "context_key": "comparative_oncology",
            "evidence_label": "comparative oncology",
            "disease_name": "comparative oncology vascular sarcoma context",
            "species": "comparative",
        }
    return None


def _has_species_translation_context(text: str) -> bool:
    return "canine" in text and any(term in text for term in TRANSLATION_TERMS)


def _avma_trial_primary_scope_matches(obj: ResearchObject) -> bool:
    metadata = obj.metadata or {}
    conditions = metadata.get("conditions") if isinstance(metadata.get("conditions"), list) else []
    primary_text = _normalize_context(
        " ".join(
            part
            for part in (
                obj.title,
                metadata.get("tagline") if isinstance(metadata.get("tagline"), str) else None,
                " ".join(str(condition) for condition in conditions),
                (obj.abstract or "")[:600],
            )
            if part
        )
    )
    disease_terms = (
        "hemangiosarcoma",
        "haemangiosarcoma",
        "hsa",
        "angiosarcoma",
        "hemangioendothelioma",
        "haemangioendothelioma",
        "vascular sarcoma",
        "endothelial sarcoma",
    )
    return any(_contains_term(primary_text, term) for term in disease_terms)


def _find_entities(text: str, dictionary: dict[str, tuple[str, ...]]) -> list[str]:
    found: list[str] = []
    for canonical, aliases in dictionary.items():
        if any(_contains_term(text, alias) for alias in aliases):
            found.append(canonical)
    return found


def _contains_term(text: str, term: str) -> bool:
    if re.search(r"[a-z0-9]", term):
        return re.search(rf"(?<![a-z0-9]){re.escape(term.lower())}(?![a-z0-9])", text) is not None
    return term.lower() in text


def _infer_evidence_level(obj: ResearchObject | None) -> EvidenceLevel:
    if obj is None:
        return EvidenceLevel.UNKNOWN
    if obj.object_type == "knowledge_entry":
        return EvidenceLevel.REVIEW
    if obj.object_type == "bioactivity_assay":
        return EvidenceLevel.IN_VITRO
    if obj.source_key in {"pubchem", "uniprot", "rcsb_pdb"}:
        return EvidenceLevel.IN_SILICO
    if obj.source_key == "openfda_animal_events":
        return EvidenceLevel.CANINE_CLINICAL
    text = f"{obj.title or ''} {obj.abstract or ''}".lower()
    if "clinical" in text or "retrospective" in text or "trial" in text:
        return EvidenceLevel.CANINE_CLINICAL
    if "cell line" in text or "in vitro" in text:
        return EvidenceLevel.IN_VITRO
    if "mouse" in text or "murine" in text or "xenograft" in text:
        return EvidenceLevel.ANIMAL_MODEL
    return EvidenceLevel.UNKNOWN


def _snippet(text: str, limit: int = 480) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned[:limit].rstrip() + ("..." if len(cleaned) > limit else "")


def _dedupe_claims(claims: list[ClaimSearchResult]) -> list[ClaimSearchResult]:
    seen: set[str] = set()
    deduped: list[ClaimSearchResult] = []
    for claim in claims:
        key = f"{claim.claim_type}:{claim.statement}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(claim)
    return deduped
