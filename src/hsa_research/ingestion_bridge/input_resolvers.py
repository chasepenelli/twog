"""Network input resolvers (v0.2) — auto-fetch real lane inputs so a candidate that merely NAMES a
target + therapy can run real compute, no hand-curation. PubChem -> ligand SMILES; RCSB -> a target
structure (apo receptor + a docking box centered on the crystal ligand pocket).

Honest by construction: any failure returns None (never fabricated). Results are cached in-memory so a
multi-round loop doesn't hammer the APIs. Network access lives ONLY here, behind the lane_inputs seam —
the rest of twog stays offline and deterministic, so CI mocks this and never touches the network.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any

# HETATM residue names that are NOT the drug-like ligand (solvent / ions / cryo agents).
_NON_LIGAND_HET = {
    "HOH", "SO4", "PO4", "GOL", "EDO", "CL", "NA", "MG", "ZN", "CA", "K", "MN", "ACT", "DMS",
    "PEG", "PG4", "BME", "TRS", "FMT", "IOD", "BR", "NO3", "CO3", "ACE", "EPE", "MES", "IMD",
}
_PUBCHEM_SMILES = (
    "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{name}/property/CanonicalSMILES/TXT"
)
_RCSB_SEARCH = "https://search.rcsb.org/rcsbsearch/v2/query"
_RCSB_DOWNLOAD = "https://files.rcsb.org/download/{pdb_id}.pdb"
_UNIPROT_SEARCH = (
    "https://rest.uniprot.org/uniprotkb/search?query=gene:{gene}+AND+reviewed:true"
    "&format=json&size=1&fields=sequence"
)


def _http_get(url: str, timeout: int = 60) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as fh:
        return fh.read().decode("utf-8")


def _http_post_json(url: str, payload: dict, timeout: int = 60) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as fh:
        return json.loads(fh.read().decode("utf-8"))


def receptor_and_box(pdb_text: str) -> tuple[str, dict[str, Any]]:
    """Strip HETATM -> apo receptor; box center = centroid of the largest drug-like ligand (or {} ->
    the gnina lane falls back to a whole-receptor autobox)."""
    receptor_lines: list[str] = []
    het_by_res: dict[tuple, list[tuple[float, float, float]]] = {}
    for line in pdb_text.splitlines():
        rec = line[:6].strip()
        if rec == "ATOM":
            receptor_lines.append(line)
        elif rec == "HETATM":
            resn = line[17:20].strip()
            if resn in _NON_LIGAND_HET:
                continue
            try:
                xyz = (float(line[30:38]), float(line[38:46]), float(line[46:54]))
            except ValueError:
                continue
            het_by_res.setdefault((resn, line[21], line[22:26]), []).append(xyz)
    receptor_lines.append("END")
    receptor = "\n".join(receptor_lines) + "\n"
    if not het_by_res:
        return receptor, {}
    biggest = max(het_by_res.values(), key=len)
    n = len(biggest)
    cx, cy, cz = (round(sum(c[i] for c in biggest) / n, 2) for i in range(3))
    return receptor, {"center_x": cx, "center_y": cy, "center_z": cz, "size_x": 22, "size_y": 22, "size_z": 22}


class NetworkInputResolvers:
    """Live network resolvers (PubChem + RCSB). Inject into HSAResearchService.input_resolvers to make
    the autonomous loop self-supply real inputs from a candidate's named target + therapy."""

    def __init__(self) -> None:
        self._smiles_cache: dict[str, str | None] = {}
        self._structure_cache: dict[str, dict[str, Any] | None] = {}
        self._sequence_cache: dict[str, str | None] = {}

    def compound_smiles(self, name: str) -> str | None:
        if name in self._smiles_cache:
            return self._smiles_cache[name]
        smiles: str | None = None
        try:
            raw = _http_get(_PUBCHEM_SMILES.format(name=urllib.parse.quote(name)))
            first = raw.strip().splitlines()[0].strip() if raw.strip() else ""
            smiles = first or None
        except Exception:  # noqa: BLE001 — honest None on any network/parse failure
            smiles = None
        self._smiles_cache[name] = smiles
        return smiles

    def target_structure(self, target: str) -> dict[str, Any] | None:
        if target in self._structure_cache:
            return self._structure_cache[target]
        result: dict[str, Any] | None = None
        try:
            pdb_id = self._top_pdb_id(target)
            if pdb_id:
                receptor, box = receptor_and_box(_http_get(_RCSB_DOWNLOAD.format(pdb_id=pdb_id)))
                if receptor.strip():
                    result = {"receptor_pdb": receptor, "pdb_id": pdb_id, **box}
        except Exception:  # noqa: BLE001
            result = None
        self._structure_cache[target] = result
        return result

    def protein_sequence(self, target: str) -> str | None:
        """Resolve a target gene name to its reviewed (Swiss-Prot) canonical sequence — for co-folding."""
        if target in self._sequence_cache:
            return self._sequence_cache[target]
        seq: str | None = None
        try:
            raw = _http_get(_UNIPROT_SEARCH.format(gene=urllib.parse.quote(target)))
            results = json.loads(raw).get("results") or []
            if results:
                value = (results[0].get("sequence") or {}).get("value")
                seq = value or None
        except Exception:  # noqa: BLE001
            seq = None
        self._sequence_cache[target] = seq
        return seq

    def _top_pdb_id(self, target: str) -> str | None:
        query = {
            "query": {"type": "terminal", "service": "full_text", "parameters": {"value": target}},
            "return_type": "entry",
            "request_options": {
                "paginate": {"start": 0, "rows": 1},
                "results_content_type": ["experimental"],
            },
        }
        data = _http_post_json(_RCSB_SEARCH, query)
        result_set = data.get("result_set") or []
        return result_set[0]["identifier"] if result_set else None
