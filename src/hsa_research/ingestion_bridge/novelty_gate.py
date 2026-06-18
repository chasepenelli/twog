"""Novelty / prior-art gate — quantify how explored a (compound, target, disease) hypothesis is.

The INPUT ANALOG of the docking spend-gate: before twog spends compute attacking a hypothesis, check
whether it is already well-studied in the literature, so the engine stops re-deriving known biochemistry
and only pays to attack genuinely NOVEL ideas. The prior-art signal is a literature COUNT — Europe PMC's
`hitCount` for a boolean query (free, no API key, covers PubMed + full text + preprints). Pure scoring is
separated from network I/O so it is unit-testable offline (inject ``count_fn``).

Comparative-oncology twist: the highest-value quadrant is "studied in HUMAN angiosarcoma but NOT in
canine hemangiosarcoma" (or vice-versa) — the cross-species translation is itself the novelty. See
``assess_translational``.
"""

from __future__ import annotations

import json
import math
import urllib.parse
import urllib.request
from typing import Any, Callable

EUROPEPMC_SEARCH = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
DEFAULT_THRESHOLD = 0.5  # novelty_score >= this AND no exact-triple prior art => novel
_UA = "twog-novelty-gate/1.0 (comparative-oncology research; contact chasepenelli@gmail.com)"


def _norm(s: Any) -> str:
    return " ".join(str(s or "").lower().split())


def triple_key(compound: str, target: str, disease: str) -> str:
    """Deterministic cache/ledger key for a hypothesis triple (used as AgentRun.source_key)."""
    return f"novelty:{_norm(compound)}|{_norm(target)}|{_norm(disease)}"


def _and(*terms: str) -> str:
    return " AND ".join(f'"{t}"' for t in terms if t)


def europepmc_count(query: str, *, timeout: float = 15.0) -> int:
    """Total literature matches for a boolean query — Europe PMC `hitCount`. Free, no key. Returns 0
    on any error (treated as 'no prior art found', but callers should note source failures)."""
    params = urllib.parse.urlencode({"query": query, "format": "json", "resultType": "idlist", "pageSize": 1})
    req = urllib.request.Request(f"{EUROPEPMC_SEARCH}?{params}", headers={"User-Agent": _UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - fixed trusted host
        data = json.loads(resp.read().decode("utf-8"))
    return int(data.get("hitCount", 0) or 0)


def score_from_counts(n_ctd: int, n_ct: int, n_cd: int, n_td: int, *, w_ctd: float = 3.0, w_pair: float = 1.0) -> float:
    """Map prior-art counts to a novelty score in (0, 1]. High = under-explored. The exact-triple count
    is weighted heaviest; pairwise counts are adjacent prior art. Log-compressed so a few papers don't
    crater the score but a large literature does."""
    prior = w_ctd * n_ctd + w_pair * (n_ct + n_cd + n_td)
    return round(1.0 / (1.0 + math.log1p(max(0.0, prior))), 4)


def assess(compound: str, target: str, disease: str, *, count_fn: Callable[[str], int],
           threshold: float = DEFAULT_THRESHOLD) -> dict[str, Any]:
    """Assess one (compound, target, disease) triple. Returns a JSON-serializable verdict carrying the
    score, the raw counts, and the EXACT queries used (so the judgment is itself re-checkable)."""
    q_ctd, q_ct, q_cd, q_td = _and(compound, target, disease), _and(compound, target), _and(compound, disease), _and(target, disease)
    n_ctd, n_ct, n_cd, n_td = count_fn(q_ctd), count_fn(q_ct), count_fn(q_cd), count_fn(q_td)
    score = score_from_counts(n_ctd, n_ct, n_cd, n_td)
    # The GATE decision is the honest "has anyone published this exact hypothesis": no exact-triple
    # prior art => novel. (Pairwise counts only reflect how famous the target/disease are in general —
    # a well-studied target untested against THIS compound in THIS disease is still a novel hypothesis.)
    # `novelty_score` is the graded ranking signal (crowded neighborhood => lower), not the gate.
    is_novel = n_ctd == 0
    tier = "known" if n_ctd else ("incremental" if (n_ct or n_cd) else "frontier")
    return {
        "compound": compound, "target": target, "disease": disease,
        "novelty_score": score,
        "is_novel": is_novel,
        "novelty_tier": tier,
        "prior_art": {"n_ctd": n_ctd, "n_ct": n_ct, "n_cd": n_cd, "n_td": n_td},
        "queries": {"ctd": q_ctd, "ct": q_ct, "cd": q_cd, "td": q_td},
        "source": "europepmc",
        "threshold": threshold,
        "rationale": (
            (f"{n_ctd} paper(s) study the exact compound×target×disease — already explored"
             if n_ctd else "no literature on the exact compound×target×disease triple — novel hypothesis")
            + f"; adjacent prior art: compound×target={n_ct}, compound×disease={n_cd}, target×disease={n_td}."
        ),
    }


def assess_translational(compound: str, target: str, *, count_fn: Callable[[str], int],
                         canine: str = "canine hemangiosarcoma", human: str = "angiosarcoma",
                         threshold: float = DEFAULT_THRESHOLD) -> dict[str, Any]:
    """The comparative-oncology entry point: assess the pair against BOTH the canine (HSA) and human
    (AS) context. Novelty is judged on the canine side (twog's frontier); a `translational_gap` flag
    marks the sweet spot — established in human angiosarcoma but NOT yet tested in canine HSA."""
    c = assess(compound, target, canine, count_fn=count_fn, threshold=threshold)
    h = assess(compound, target, human, count_fn=count_fn, threshold=threshold)
    gap = h["prior_art"]["n_ctd"] > 0 and c["prior_art"]["n_ctd"] == 0
    return {
        "compound": compound, "target": target,
        "canine": c, "human": h,
        "is_novel": c["is_novel"],            # the canine-HSA frontier is where twog adds value
        "novelty_score": c["novelty_score"],
        "translational_gap": gap,
        "rationale": (
            f"canine HSA: {c['rationale']} | human AS: {h['rationale']}"
            + (" | TRANSLATIONAL GAP: known in human angiosarcoma, untested in canine HSA." if gap else "")
        ),
    }
