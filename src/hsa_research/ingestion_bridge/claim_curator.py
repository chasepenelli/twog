"""Claim curation agent for local-first claim review.

The first implementation is deterministic on purpose: it gives us an auditable
agent contract and durable decisions before wiring Claude/OpenAI into the same
interface. Future model-backed curators should preserve these input/output
contracts and write the same curation metadata.
"""

from __future__ import annotations

import re
from datetime import datetime
from uuid import UUID

from .contracts import (
    ClaimCurationDecision,
    ClaimCurationItem,
    ClaimCurationRequest,
    ClaimCurationResult,
    ClaimSearchResult,
)
from .local_store import SQLiteResearchRepository

CURATOR_NAME = "local_claim_curator_agent"
CURATOR_VERSION = "0.1"

REJECTED_STATUSES = {
    ClaimCurationDecision.REJECT.value,
    ClaimCurationDecision.MERGE_DUPLICATE.value,
}

EVIDENCE_BONUS = {
    "canine_clinical": 0.12,
    "human_clinical": 0.08,
    "in_vitro": 0.07,
    "animal_model": 0.07,
    "ex_vivo": 0.06,
    "review": 0.05,
    "in_silico": 0.03,
    "unknown": 0.0,
}


class ClaimCuratorAgent:
    """Review, dedupe, and promote draft claims into a cleaner claim layer."""

    def __init__(
        self,
        repository: SQLiteResearchRepository,
        *,
        curator_name: str = CURATOR_NAME,
        curator_version: str = CURATOR_VERSION,
    ) -> None:
        self.repository = repository
        self.curator_name = curator_name
        self.curator_version = curator_version

    def curate(self, request: ClaimCurationRequest) -> ClaimCurationResult:
        result = ClaimCurationResult(
            curator_name=f"{self.curator_name}:{self.curator_version}",
            model_profile=request.model_profile,
            dry_run=request.dry_run,
        )
        claims = self._load_claims(request)
        result.claims_seen = len(claims)

        for group in _group_claims(claims).values():
            try:
                canonical = max(group, key=lambda claim: (_score_claim(claim, len(group)), claim.confidence))
                canonical_score, canonical_reasons = _score_with_reasons(canonical, len(group))
                canonical_decision = _decision_for_claim(canonical, canonical_score, request.promote_threshold)
                canonical_item = self._item(
                    canonical,
                    decision=canonical_decision,
                    curation_score=canonical_score,
                    curated_confidence=_curated_confidence(canonical, canonical_score),
                    canonical_claim_id=canonical.claim_id,
                    reasons=canonical_reasons,
                )
                self._record_decision(canonical_item, result)
                if not request.dry_run:
                    self._persist_decision(canonical, canonical_item, group, request.model_profile)

                for claim in group:
                    if claim.claim_id == canonical.claim_id:
                        continue
                    duplicate_score, duplicate_reasons = _score_with_reasons(claim, len(group))
                    duplicate_reasons.append(f"merged with canonical claim {canonical.claim_id}")
                    duplicate_item = self._item(
                        claim,
                        decision=ClaimCurationDecision.MERGE_DUPLICATE,
                        curation_score=duplicate_score,
                        curated_confidence=claim.confidence,
                        canonical_claim_id=canonical.claim_id,
                        reasons=duplicate_reasons,
                    )
                    self._record_decision(duplicate_item, result)
                    if not request.dry_run:
                        self._persist_decision(claim, duplicate_item, group, request.model_profile)
            except Exception as exc:
                result.errors.append(f"{group[0].claim_id}: {exc}")

        return result

    def _load_claims(self, request: ClaimCurationRequest) -> list[ClaimSearchResult]:
        claims = self.repository.list_claims(
            source_key=request.source_key,
            query=request.query,
            min_confidence=request.min_confidence,
            extraction_status="draft",
            include_seed_claims=request.include_seed_claims,
        )
        eligible = [
            claim
            for claim in claims
            if claim.metadata.get("curation_status") not in REJECTED_STATUSES | {ClaimCurationDecision.PROMOTE.value}
        ]
        return eligible[: request.limit]

    def _item(
        self,
        claim: ClaimSearchResult,
        *,
        decision: ClaimCurationDecision,
        curation_score: float,
        curated_confidence: float,
        canonical_claim_id: UUID | None,
        reasons: list[str],
    ) -> ClaimCurationItem:
        return ClaimCurationItem(
            claim_id=claim.claim_id,
            statement=claim.statement,
            decision=decision,
            curation_score=round(curation_score, 4),
            original_confidence=claim.confidence,
            curated_confidence=round(curated_confidence, 4),
            canonical_claim_id=canonical_claim_id,
            reasons=reasons,
        )

    def _record_decision(self, item: ClaimCurationItem, result: ClaimCurationResult) -> None:
        result.decisions.append(item)
        decision = _enum_value(item.decision)
        if decision == ClaimCurationDecision.PROMOTE.value:
            result.promoted += 1
        elif decision == ClaimCurationDecision.MERGE_DUPLICATE.value:
            result.merged_duplicates += 1
        elif decision == ClaimCurationDecision.NEEDS_REVIEW.value:
            result.needs_review += 1
        elif decision == ClaimCurationDecision.REJECT.value:
            result.rejected += 1

    def _persist_decision(
        self,
        claim: ClaimSearchResult,
        item: ClaimCurationItem,
        group: list[ClaimSearchResult],
        model_profile: str,
    ) -> None:
        decision = _enum_value(item.decision)
        metadata = claim.metadata | {
            "curation_status": decision,
            "curation_score": item.curation_score,
            "curation_reasons": item.reasons,
            "curator_name": self.curator_name,
            "curator_version": self.curator_version,
            "curated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "model_profile": model_profile,
        }
        if item.canonical_claim_id:
            metadata["canonical_claim_id"] = str(item.canonical_claim_id)
        if decision == ClaimCurationDecision.PROMOTE.value:
            metadata["extraction_status"] = "curated"
            metadata["merged_claim_ids"] = [str(other.claim_id) for other in group if other.claim_id != claim.claim_id]

        updated = claim.model_copy(
            update={
                "confidence": item.curated_confidence,
                "support_count": max(claim.support_count, len(group)),
                "metadata": metadata,
            }
        )
        self.repository.upsert_claim(updated)


def curate_claims_for_repository(
    repository: SQLiteResearchRepository,
    request: ClaimCurationRequest | None = None,
) -> ClaimCurationResult:
    """Run the local claim curator against a SQLite repository."""

    return ClaimCuratorAgent(repository).curate(request or ClaimCurationRequest())


def _group_claims(claims: list[ClaimSearchResult]) -> dict[str, list[ClaimSearchResult]]:
    grouped: dict[str, list[ClaimSearchResult]] = {}
    for claim in claims:
        grouped.setdefault(_claim_fingerprint(claim), []).append(claim)
    return grouped


def _claim_fingerprint(claim: ClaimSearchResult) -> str:
    entities = sorted(
        f"{entity.entity_type}:{entity.canonical_name.lower()}:{entity.role or ''}"
        for entity in claim.entities
        if entity.entity_type != "disease"
    )
    return "|".join([_enum_value(claim.claim_type), _normalize_statement(claim.statement), ",".join(entities)])


def _normalize_statement(statement: str) -> str:
    return re.sub(r"\s+", " ", statement.lower()).strip()


def _score_claim(claim: ClaimSearchResult, support_count: int) -> float:
    score, _ = _score_with_reasons(claim, support_count)
    return score


def _score_with_reasons(claim: ClaimSearchResult, support_count: int) -> tuple[float, list[str]]:
    reasons: list[str] = []
    score = claim.confidence

    evidence_bonus = EVIDENCE_BONUS.get(_enum_value(claim.evidence_level), 0.0)
    if evidence_bonus:
        score += evidence_bonus
        reasons.append(f"evidence level adds {evidence_bonus:.2f}")
    else:
        reasons.append("evidence level is unknown")

    if claim.source_object_id and claim.metadata.get("source_chunk_id"):
        score += 0.06
        reasons.append("has source object and chunk provenance")
    else:
        score -= 0.15
        reasons.append("missing source object or chunk provenance")

    if support_count > 1:
        support_bonus = min(0.18, 0.04 * (support_count - 1))
        score += support_bonus
        reasons.append(f"{support_count} matching draft claims add support")

    non_disease_entities = [entity for entity in claim.entities if entity.entity_type != "disease"]
    if len(non_disease_entities) >= 2:
        score += 0.04
        reasons.append("claim links multiple non-disease entities")

    if len(claim.statement) < 35:
        score -= 0.1
        reasons.append("statement is too short")

    return max(0.0, min(0.95, score)), reasons


def _decision_for_claim(
    claim: ClaimSearchResult,
    curation_score: float,
    promote_threshold: float,
) -> ClaimCurationDecision:
    if not claim.source_object_id or not claim.metadata.get("source_chunk_id"):
        return ClaimCurationDecision.NEEDS_REVIEW
    if claim.confidence < 0.2 or len(claim.statement) < 35:
        return ClaimCurationDecision.REJECT
    if curation_score >= promote_threshold:
        return ClaimCurationDecision.PROMOTE
    return ClaimCurationDecision.NEEDS_REVIEW


def _curated_confidence(claim: ClaimSearchResult, curation_score: float) -> float:
    return max(claim.confidence, min(0.9, curation_score))


def _enum_value(value: object) -> str:
    return value.value if hasattr(value, "value") else str(value)
