"""LLM-as-judge pre-grader for proof capsules.

Scans proof_capsules without any non-llm_evaluator review and asks an LLM
to score them across the 7 rubric dimensions. Stores the result as a
proof_capsule_reviews row with reviewer_type="llm_evaluator" — these
rows are advisory recommendations only. They do NOT award proof points;
the operator's manual review remains the source of truth for reward
emission (sync_reward_events_from_proof_capsule_reviews skips them).

Why this exists: when auto-orchestrated dives (Slice 3) start landing
8 capsules per new candidate, operators need a pre-grade to prioritize
their queue. The LLM's scores become the default that the operator can
adopt or override.

Cost control:
- Default model is claude-haiku-4-5-20251001 via OpenRouter (~$0.001/grade).
- Cache by capsule content_hash: a re-graded capsule with the same
  content_hash returns the cached scores.
- Per-pass cap on number of capsules graded.
- HSA_LLM_CAPSULE_GRADER_MODEL env var overrides for Sonnet/Opus.
"""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from .candidate_contribution_intake import candidate_contribution_database_url


DEFAULT_OPENROUTER_MODEL = "anthropic/claude-haiku-4-5-20251001"
DEFAULT_MAX_TOKENS = 400
DEFAULT_TEMPERATURE = 0.1
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_PASS_LIMIT = 20


_RUBRIC_DEFINITION = """\
You are grading a proof capsule submitted to the TWOG Proof Network.
Score the seven rubric dimensions, each in [0.0, 1.0], where 1.0 is
"this is the best a capsule of this type could be" and 0.0 is "this is
not useful at all". Be strict: most capsules should land between 0.4
and 0.85. Only load-bearing, verifiable, primary-source-backed findings
deserve 0.9+ on a dimension.

Dimensions:

- scientific_usefulness: Does this change what someone would do with
  this candidate? Could a reviewer demote, accept, or reroute based on
  this finding?
- provenance_strength: Can a reader verify this without re-doing the
  work? Are DOIs / PMIDs / artifact hashes / specific quotes present?
- actionability: Does this lead to a concrete next step? "Add this
  citation", "demote this claim", "rerun this assay" — or just
  hand-wavy?
- reproducibility: Could a competent contributor reproduce the result
  from what's in the capsule? notebook_ref, method_refs, artifact_manifest
  detail.
- novelty: Does this surface something the candidate record didn't
  already say? Or is it a restatement?
- clarity: Could a reviewer who hasn't read the candidate understand
  this in one pass?
- downstream_impact: Does this make later work cheaper, faster, or
  safer? Or is it isolated?

Also provide:
- verdict: one of "accepted" | "routed_to_validation" |
  "routed_to_compute_review" | "needs_changes" | "rejected".
- rationale: ONE SENTENCE, max 200 chars, explaining the score.

Negative findings (demotion, replication failure, citation
misattribution) are first-class: a well-argued demotion earns full
reward. Reward truth, not direction.

Respond with ONLY a JSON object matching this exact schema, no markdown:

{
  "scientific_usefulness": <float 0-1>,
  "provenance_strength": <float 0-1>,
  "actionability": <float 0-1>,
  "reproducibility": <float 0-1>,
  "novelty": <float 0-1>,
  "clarity": <float 0-1>,
  "downstream_impact": <float 0-1>,
  "verdict": <string>,
  "rationale": <string max 200 chars>
}
"""


@dataclass
class LLMGradeResult:
    """A single LLM grading pass over one capsule."""

    proof_capsule_id: str
    scientific_usefulness: float
    provenance_strength: float
    actionability: float
    reproducibility: float
    novelty: float
    clarity: float
    downstream_impact: float
    verdict: str
    rationale: str
    model_name: str
    content_hash: str  # the capsule content_hash this grade is for
    cached: bool = False  # True if we found a prior grade for this content_hash


def llm_grader_model() -> str:
    return os.getenv("HSA_LLM_CAPSULE_GRADER_MODEL", DEFAULT_OPENROUTER_MODEL)


def grade_capsule(
    *,
    proof_capsule_id: str,
    capsule_body: Mapping[str, Any],
    candidate_excerpt: Mapping[str, Any] | None = None,
    model_name: str | None = None,
) -> LLMGradeResult:
    """Call the LLM to grade one capsule. Pure-ish: no DB writes.

    The caller is responsible for storing the result as a
    proof_capsule_reviews row via record_proof_capsule_review.
    """

    model = model_name or llm_grader_model()
    user_payload = {
        "capsule": {
            "title": capsule_body.get("title"),
            "capsule_type": capsule_body.get("capsule_type"),
            "analysis_summary": capsule_body.get("analysis_summary"),
            "findings": capsule_body.get("findings"),
            "limitations": capsule_body.get("limitations"),
            "method_refs": capsule_body.get("method_refs"),
            "artifact_manifest": capsule_body.get("artifact_manifest"),
            "candidate_snapshot_hash": capsule_body.get("candidate_snapshot_hash"),
            "evidence_bundle_hash": capsule_body.get("evidence_bundle_hash"),
        },
        "candidate_context": candidate_excerpt or {},
    }
    response = _openrouter_completion(model, user_payload)
    parsed = _parse_grade_response(response["text"])
    return LLMGradeResult(
        proof_capsule_id=proof_capsule_id,
        scientific_usefulness=parsed["scientific_usefulness"],
        provenance_strength=parsed["provenance_strength"],
        actionability=parsed["actionability"],
        reproducibility=parsed["reproducibility"],
        novelty=parsed["novelty"],
        clarity=parsed["clarity"],
        downstream_impact=parsed["downstream_impact"],
        verdict=parsed["verdict"],
        rationale=parsed["rationale"],
        model_name=response["metadata"]["model_name"],
        content_hash=str(capsule_body.get("content_hash") or ""),
    )


def grade_pending_capsules(
    *,
    database_url: str | None = None,
    limit: int = DEFAULT_PASS_LIMIT,
    model_name: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Find capsules with no non-llm_evaluator review and grade them.

    Returns a summary dict. Writes proof_capsule_reviews rows with
    reviewer_type="llm_evaluator" unless dry_run is true.

    Cache: if a capsule's content_hash already has an llm_evaluator
    review, the result is re-used (no LLM call) so re-running this
    pass is cheap.
    """

    connection_string = database_url or candidate_contribution_database_url()
    if not connection_string:
        return {"status": "no_database", "graded": 0, "skipped": 0, "errors": []}

    bounded_limit = max(1, min(int(limit), 200))
    pending = _list_pending_capsules(connection_string, bounded_limit)
    cache = _load_grade_cache_by_content_hash(connection_string)

    graded: list[dict[str, Any]] = []
    cached_hits: list[dict[str, Any]] = []
    errors: list[str] = []

    for row in pending:
        content_hash = str(row.get("content_hash") or "")
        cached = cache.get(content_hash)
        if cached:
            cached_hits.append({"proof_capsule_id": row["proof_capsule_id"], "content_hash": content_hash})
            continue
        try:
            result = grade_capsule(
                proof_capsule_id=row["proof_capsule_id"],
                capsule_body=row,
                candidate_excerpt={"candidate_id": row.get("candidate_id")},
                model_name=model_name,
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"capsule {row['proof_capsule_id']}: {exc}")
            continue
        if not dry_run:
            try:
                _persist_llm_grade(connection_string, result)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"persist {row['proof_capsule_id']}: {exc}")
                continue
        graded.append(
            {
                "proof_capsule_id": result.proof_capsule_id,
                "model_name": result.model_name,
                "verdict": result.verdict,
                "weighted_score_preview": _weighted_score(result),
            }
        )
    return {
        "status": "ok",
        "scanned": len(pending),
        "graded": len(graded),
        "cached_hits": len(cached_hits),
        "errors": errors,
        "dry_run": dry_run,
        "results": graded,
    }


def _list_pending_capsules(connection_string: str, limit: int) -> list[dict[str, Any]]:
    """Find capsules with no non-llm_evaluator review. Newest first.

    We intentionally include capsules that already have an llm_evaluator
    review — the cache layer dedupes by content_hash so we don't
    re-grade identical content, but we DO re-grade when the capsule has
    been edited (different content_hash → new grade).
    """

    with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    c.proof_capsule_id::text as proof_capsule_id,
                    c.candidate_id,
                    c.capsule_type,
                    c.title,
                    c.analysis_summary,
                    c.findings,
                    c.limitations,
                    c.method_refs,
                    c.artifact_manifest,
                    c.candidate_snapshot_hash,
                    c.evidence_bundle_hash,
                    c.content_hash
                from proof_capsules c
                where not exists (
                    select 1 from proof_capsule_reviews r
                    where r.proof_capsule_id = c.proof_capsule_id
                      and r.reviewer_type != 'llm_evaluator'
                )
                and not exists (
                    select 1 from proof_capsule_reviews r2
                    where r2.proof_capsule_id = c.proof_capsule_id
                      and r2.reviewer_type = 'llm_evaluator'
                )
                order by c.submitted_at desc
                limit %s
                """,
                [limit],
            )
            return [dict(row) for row in cur.fetchall()]


def _load_grade_cache_by_content_hash(connection_string: str) -> dict[str, dict[str, Any]]:
    """Pre-existing llm_evaluator grades, keyed by the content_hash of
    the capsule at grade time. Cheap to load: bounded by capsule count.
    """

    with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    c.content_hash,
                    r.proof_capsule_id::text as proof_capsule_id,
                    r.scientific_usefulness,
                    r.provenance_strength,
                    r.reproducibility,
                    r.actionability,
                    r.novelty,
                    r.clarity,
                    r.downstream_impact,
                    r.verdict,
                    r.rationale
                from proof_capsule_reviews r
                join proof_capsules c on c.proof_capsule_id = r.proof_capsule_id
                where r.reviewer_type = 'llm_evaluator'
                """
            )
            return {row["content_hash"]: dict(row) for row in cur.fetchall() if row["content_hash"]}


def _persist_llm_grade(connection_string: str, result: LLMGradeResult) -> None:
    """Write the LLM grade as a proof_capsule_reviews row.

    Using raw psycopg2 (not the typed record_proof_capsule_review) because
    we want to write a non-operator reviewer_type and keep the public
    boundary clean. The row exists for operator-console pre-fill; it never
    awards proof points (sync_reward_events filters reviewer_type=
    llm_evaluator).
    """

    with psycopg2.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into proof_capsule_reviews (
                    proof_capsule_id,
                    reviewer_type,
                    reviewer_id,
                    verdict,
                    confidence,
                    scientific_usefulness,
                    provenance_strength,
                    reproducibility,
                    actionability,
                    novelty,
                    clarity,
                    downstream_impact,
                    rationale,
                    required_changes,
                    payload
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                [
                    result.proof_capsule_id,
                    "llm_evaluator",
                    f"llm:{result.model_name}",
                    result.verdict,
                    None,
                    result.scientific_usefulness,
                    result.provenance_strength,
                    result.reproducibility,
                    result.actionability,
                    result.novelty,
                    result.clarity,
                    result.downstream_impact,
                    result.rationale,
                    None,
                    json.dumps(
                        {
                            "model_name": result.model_name,
                            "graded_content_hash": result.content_hash,
                            "is_recommendation": True,
                        }
                    ),
                ],
            )
        conn.commit()


def _weighted_score(result: LLMGradeResult) -> float:
    """Quick preview of the weighted-rubric score using the same weights
    proof_capsules.compute_capsule_reward_from_rubric uses. Used only
    for the summary log; the authoritative computation happens in the
    Python module."""
    weights = {
        "scientific_usefulness": 0.25,
        "provenance_strength": 0.20,
        "actionability": 0.20,
        "reproducibility": 0.10,
        "novelty": 0.10,
        "downstream_impact": 0.10,
        "clarity": 0.05,
    }
    total = 0.0
    for dim, weight in weights.items():
        total += weight * float(getattr(result, dim))
    return round(total, 4)


# ---------- OpenRouter call (mirrors agent_performance._openrouter_review_model) ----------


def _openrouter_completion(model_name: str, user_payload: Mapping[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for LLM capsule grading.")
    payload = {
        "model": model_name,
        "temperature": float(os.getenv("HSA_LLM_CAPSULE_GRADER_TEMPERATURE", str(DEFAULT_TEMPERATURE))),
        "max_tokens": int(os.getenv("HSA_LLM_CAPSULE_GRADER_MAX_TOKENS", str(DEFAULT_MAX_TOKENS))),
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _RUBRIC_DEFINITION},
            {"role": "user", "content": json.dumps(user_payload, sort_keys=True, default=str)},
        ],
    }
    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "HTTP-Referer": os.getenv("OPENROUTER_HTTP_REFERER", "https://github.com/chasepenelli/hsa-dagster"),
            "X-Title": os.getenv("OPENROUTER_APP_TITLE", "twog-llm-capsule-grader"),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(
            req,
            timeout=float(os.getenv("HSA_LLM_CAPSULE_GRADER_TIMEOUT_SECONDS", str(DEFAULT_TIMEOUT_SECONDS))),
        ) as resp:
            response_payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenRouter HTTP {e.code}: {body[:300]}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"OpenRouter request failed: {e}") from e
    choices = response_payload.get("choices") or []
    if not choices:
        raise RuntimeError(f"OpenRouter response had no choices: {response_payload}")
    text = (choices[0].get("message") or {}).get("content") or ""
    if not text:
        raise RuntimeError(f"OpenRouter response had no content: {response_payload}")
    return {
        "text": text,
        "metadata": {
            "provider": "openrouter",
            "model_name": response_payload.get("model", model_name),
            "requested_model": model_name,
            "usage": response_payload.get("usage", {}),
        },
    }


def _parse_grade_response(text: str) -> dict[str, Any]:
    """Strip optional code fences and parse the JSON object. Validates
    presence of all 7 rubric dims, clamps to [0, 1], and defaults the
    rationale/verdict to safe values if missing."""
    stripped = text.strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", stripped, flags=re.DOTALL)
    if fenced:
        stripped = fenced.group(1).strip()
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end <= start:
            raise
        parsed = json.loads(stripped[start : end + 1])
    if not isinstance(parsed, dict):
        raise RuntimeError(f"LLM grade response was not a JSON object: {stripped[:200]}")
    out: dict[str, Any] = {}
    for dim in (
        "scientific_usefulness", "provenance_strength", "actionability",
        "reproducibility", "novelty", "clarity", "downstream_impact",
    ):
        value = parsed.get(dim)
        if not isinstance(value, (int, float)):
            raise RuntimeError(f"LLM grade response missing or non-numeric '{dim}': {parsed}")
        out[dim] = max(0.0, min(1.0, float(value)))
    verdict = str(parsed.get("verdict") or "").strip()
    if verdict not in (
        "accepted", "routed_to_validation", "routed_to_compute_review",
        "needs_changes", "rejected",
    ):
        # Default to needs_changes (which awards 0 pp under the new
        # scoring) if the LLM emits something unexpected. Safer than
        # silently misclassifying as accepted.
        verdict = "needs_changes"
    out["verdict"] = verdict
    rationale = str(parsed.get("rationale") or "").strip()
    out["rationale"] = rationale[:500] if rationale else "LLM grade (no rationale provided)"
    return out
