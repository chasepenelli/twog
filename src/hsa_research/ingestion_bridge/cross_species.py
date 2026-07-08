"""Cross-species white-space discovery — the novel-hypothesis frontier.

twog's edge is the dog↔human bridge. A target that is associated with HUMAN angiosarcoma, has a
conserved CANINE ortholog, but is NOT yet studied in canine hemangiosarcoma is *white space*: the
cross-species translation is itself the novelty (vs. re-docking textbook drugs against known targets).

Recipe (all free, read-only APIs):
  Open Targets (human angiosarcoma → associated targets)
    → Ensembl Compara (human target → canine ortholog + % identity, the join key)
      → novelty gate (target × "canine hemangiosarcoma" literature count)
        → white space = associated in human AS, conserved ortholog, ~no canine-HSA literature.

I/O is injected (``ot_fetch`` / ``ens_fetch`` / ``canine_lit_count``) so the logic is unit-testable
offline. Targets not yet in the verified target library become a verification queue (what to add so
drugs can be docked against them — the growth loop).
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable

OPEN_TARGETS_GQL = "https://api.platform.opentargets.org/api/v4/graphql"
ENSEMBL_REST = "https://rest.ensembl.org"
ANGIOSARCOMA_EFO = "EFO_0003968"
_UA = "twog-cross-species/1.0 (comparative-oncology research; contact chasepenelli@gmail.com)"

_ASSOC_QUERY = (
    "query($efo:String!,$size:Int!){ disease(efoId:$efo){ name "
    "associatedTargets(page:{index:0,size:$size}){ rows{ score target{ approvedSymbol id } } } } }"
)


def _ot_fetch(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    """POST a GraphQL query to Open Targets. Raises on transport error (caller decides)."""
    req = urllib.request.Request(
        OPEN_TARGETS_GQL, data=json.dumps({"query": query, "variables": variables}).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": _UA},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - fixed trusted host
        return json.loads(resp.read().decode("utf-8"))


def _ens_fetch(symbol: str) -> list[dict[str, Any]]:
    """Return Ensembl Compara human→dog orthology homologies for a gene symbol. Resilient: any network
    error (timeout/HTTP/DNS) degrades to [] (treated as 'no ortholog found' → target skipped), with one
    retry — a single flaky API call must not crash autonomous discovery."""
    url = (f"{ENSEMBL_REST}/homology/symbol/human/{urllib.parse.quote(symbol)}"
           "?target_species=canis_lupus_familiaris;type=orthologues;content-type=application/json")
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:  # noqa: S310
                data = json.loads(resp.read().decode("utf-8"))
            return ((data.get("data") or [{}])[0] or {}).get("homologies", []) or []
        except (urllib.error.URLError, TimeoutError, OSError, ValueError):
            if attempt == 0:
                continue
            return []
    return []


def opentargets_associated_targets(efo_id: str, *, size: int = 25,
                                   fetch: Callable[[str, dict], dict] = _ot_fetch) -> list[dict[str, Any]]:
    """Targets associated with a disease, ranked by Open Targets association score."""
    data = fetch(_ASSOC_QUERY, {"efo": efo_id, "size": size})
    rows = (((data.get("data") or {}).get("disease") or {}).get("associatedTargets") or {}).get("rows", [])
    out = []
    for r in rows:
        t = r.get("target") or {}
        if t.get("approvedSymbol"):
            out.append({"symbol": t["approvedSymbol"], "ensembl_id": t.get("id"), "ot_score": round(r.get("score", 0.0), 4)})
    return out


def canine_ortholog(symbol: str, *, fetch: Callable[[str], list] = _ens_fetch) -> dict[str, Any] | None:
    """Best canine ortholog (highest % identity) for a human gene symbol, or None if none exists."""
    homs = fetch(symbol)
    if not homs:
        return None
    best = max(homs, key=lambda h: (h.get("target") or {}).get("perc_id") or 0.0)
    t = best.get("target") or {}
    return {"canine_gene_id": t.get("id"), "perc_id": t.get("perc_id")}


def white_space_targets(
    efo_id: str = ANGIOSARCOMA_EFO,
    *,
    canine_lit_count: Callable[[str], int],
    verified_targets: set[str] | None = None,
    ot_fetch: Callable[[str, dict], dict] = _ot_fetch,
    ens_fetch: Callable[[str], list] = _ens_fetch,
    size: int = 25,
    max_targets: int = 12,
) -> list[dict[str, Any]]:
    """Rank human-disease targets by cross-species white-space: associated in human (AS), conserved
    canine ortholog present, and ~no canine-HSA literature. Skips targets with no canine ortholog."""
    verified = {t.upper() for t in (verified_targets or set())}
    targets = opentargets_associated_targets(efo_id, size=size, fetch=ot_fetch)
    rows: list[dict[str, Any]] = []
    for t in targets:
        if len(rows) >= max_targets:
            break
        orth = canine_ortholog(t["symbol"], fetch=ens_fetch)
        if not orth or not orth.get("canine_gene_id"):
            continue  # no conserved ortholog → not a comparative-oncology opportunity
        canine_hits = canine_lit_count(f'"{t["symbol"]}" AND "canine hemangiosarcoma"')
        rows.append({
            **t,
            "canine_gene_id": orth["canine_gene_id"],
            "perc_id": orth.get("perc_id"),
            "canine_hsa_hits": canine_hits,
            "is_white_space": canine_hits == 0,  # studied in human AS, conserved, untested in canine HSA
            "dockable": t["symbol"].upper() in verified,
        })
    return rows
