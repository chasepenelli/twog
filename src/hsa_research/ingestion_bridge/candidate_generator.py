"""Candidate generation — turn curated/grounded idea sources into NEW dockable candidate rows.

Pure + offline helpers (no DB, no network). The service (``generate_candidate_ideas``) consumes these,
gates each row on real input-resolvability, dedups against existing candidates, and seeds the survivors
validation-ready so the falsification loop docks them. Generation itself spends no GPU money.

Two sources behind one shape:
  - curated_seed: ``data/candidate_generation_seed.json`` — real compounds vs the verified targets.
  - claims:       derive (target, therapy) pairs from the claims corpus, kept only when the target is
                  a verified-library key (the spend gate decides the rest downstream).
"""

from __future__ import annotations

import json
import pathlib
import re
from typing import Any

_DEFAULT_SEED = pathlib.Path(__file__).resolve().parents[3] / "data" / "candidate_generation_seed.json"

# canonical short suffixes so generated ids match the existing roster convention (alpelisib-pi3ka, …)
_TARGET_SHORT: dict[str, str] = {"PIK3CA": "pik3ca", "KDR": "vegfr2", "MTOR": "mtor"}


def _slug(text: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", str(text).strip().lower())).strip("-")


def candidate_id_for(compound: str, target: str) -> str:
    """Deterministic, content-addressable id for a (compound, target) pair. Stable across runs so the
    same idea always dedups to the same candidate (e.g. copanlisib + PIK3CA -> 'copanlisib-pik3ca')."""
    short = _TARGET_SHORT.get(str(target).upper(), _slug(target))
    return f"{_slug(compound)}-{short}"


def load_candidate_generation_seed(path: str | pathlib.Path | None = None) -> list[dict[str, Any]]:
    """Load the curated seed rows. Returns [] if the file is absent."""
    p = pathlib.Path(path) if path is not None else _DEFAULT_SEED
    if not p.exists():
        return []
    return json.loads(p.read_text()).get("candidates", [])


def pairs_from_claims(claims: list[Any], verified_targets: set[str]) -> list[dict[str, Any]]:
    """Best-effort (target, therapy) extraction from claim records, kept ONLY when the claim's target
    normalizes to a verified-library key. Free-text claims rarely map cleanly today, so this returns
    few/none until the corpus + a normalization layer grow — by design it never invents a pairing."""
    rows: list[dict[str, Any]] = []
    upper_verified = {t.upper() for t in verified_targets}
    for claim in claims:
        targets = [t for t in (getattr(claim, "targets", None) or []) if str(t).upper() in upper_verified]
        compounds = list(getattr(claim, "compounds", None) or [])
        if not targets or not compounds:
            continue
        rows.append({
            "compound": compounds[0],
            "target": str(targets[0]).upper(),
            "biomarkers": [],
            "evidence_refs": [f"claim:{getattr(claim, 'claim_id', '')}"],
            "rationale": getattr(claim, "statement", "") or "Derived from a supporting claim.",
        })
    return rows


def title_for(compound: str, target: str) -> str:
    """Human-readable falsification title for a generated candidate."""
    return f"Falsify: does {compound} engage {target} in canine HSA × human angiosarcoma?"


def compose_candidate_reasoning(
    *, compound: str, target: str, rationale: str = "", biomarkers: list[str] | None = None
) -> tuple[str, str]:
    """Deterministic, entity-grounded reasoning for the no-committee campaign path — so a generated
    candidate carries a real (if modest) mechanistic premise + translational payoff the MoonshotRubric
    spine can surface, instead of reading 'premise_unstated'. Pure string ops over what the row already
    owns (NEVER invents a target/SMILES/finding). The mechanism states a HYPOTHESIS, not a result, and
    the translational path is explicitly CONDITIONAL ('If … holds and the axis replicates …') so it
    cannot overclaim. Clearly less rich than committee-authored idea.mechanism — by design."""
    compound = (compound or "the compound").strip()
    target = (target or "the target").strip()
    bms = [str(b).strip() for b in (biomarkers or []) if str(b).strip()]
    mechanism = f"{compound} is hypothesized to engage {target}"
    if rationale and rationale.strip():
        mechanism = f"{mechanism}; {rationale.strip()}"
    subset = f" in the {', '.join(bms)}-defined subset" if bms else ""
    # The CONSEQUENT only (the payoff we'd be entitled to expect). The conditionality + ceiling are
    # supplied by the rubric's expected_payoff wrapper ("If all lanes survive …") + the fixed caveat, so
    # writing the path as a bare consequent avoids a double-"If" and still cannot overclaim.
    translational_path = (
        f"the {compound}-{target} pairing becomes a mutation-selective treatment candidate to advance{subset}"
    )
    return mechanism[:1500], translational_path[:2000]
