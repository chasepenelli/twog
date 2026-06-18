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
