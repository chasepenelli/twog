"""Durable resolved-input catalog — resolve once, reuse for any future test.

Companion to the verified-target catalog (``data/target_library.json``, which caches receptors/boxes):
this stores the OTHER live-fetched inputs — compound SMILES (PubChem) and protein sequences (UniProt) —
keyed by entity, in a version-controlled JSON file. A ``CatalogResolvers`` wrapper reads THROUGH it:
check the catalog → fall back to the wrapped resolver (network) → persist the result. So the second time
anything needs sorafenib's SMILES or KDR's sequence, it comes from the catalog with no network call.

Honest by construction: only REAL resolved values are stored (the wrapped resolver never fabricates);
a miss is recorded as nothing (re-tried next time), never as a fake value. Pure JSON, no DB — it grows
as a durable, inspectable asset (graduate to a Neon table when the deploy needs a shared write store)."""

from __future__ import annotations

import json
import pathlib
from typing import Any

_DEFAULT_PATH = pathlib.Path(__file__).resolve().parents[3] / "data" / "input_catalog.json"


def _key(s: str) -> str:
    return (s or "").strip().lower()


class InputCatalog:
    """A persistent {compounds: {name: smiles}, sequences: {gene: seq}} store."""

    def __init__(self, path: str | pathlib.Path | None = None) -> None:
        self.path = pathlib.Path(path) if path is not None else _DEFAULT_PATH
        self._data: dict[str, dict[str, str]] = {"compounds": {}, "sequences": {}}
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text())
                self._data["compounds"] = dict(loaded.get("compounds") or {})
                self._data["sequences"] = dict(loaded.get("sequences") or {})
            except Exception:
                pass  # a corrupt catalog degrades to empty, never crashes a run

    # --- reads ---
    def get_smiles(self, name: str) -> str | None:
        return self._data["compounds"].get(_key(name))

    def get_sequence(self, gene: str) -> str | None:
        return self._data["sequences"].get(_key(gene))

    @property
    def counts(self) -> dict[str, int]:
        return {"compounds": len(self._data["compounds"]), "sequences": len(self._data["sequences"])}

    # --- writes (persist immediately so a crash mid-run keeps what we learned) ---
    def put_smiles(self, name: str, smiles: str) -> None:
        if name and smiles:
            self._data["compounds"][_key(name)] = smiles
            self.save()

    def put_sequence(self, gene: str, seq: str) -> None:
        if gene and seq:
            self._data["sequences"][_key(gene)] = seq
            self.save()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": "input-catalog-v1",
            "compounds": dict(sorted(self._data["compounds"].items())),
            "sequences": dict(sorted(self._data["sequences"].items())),
        }
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


class CatalogResolvers:
    """Wrap a base resolver (e.g. NetworkInputResolvers) with read-through catalog caching. Structures
    are left to the verified-target library, so only SMILES + sequences are cached here. Drop-in: exposes
    the same compound_smiles / target_structure / protein_sequence interface."""

    def __init__(self, base: Any, catalog: InputCatalog | None = None) -> None:
        self._base = base
        self.catalog = catalog or InputCatalog()

    def compound_smiles(self, name: str) -> str | None:
        hit = self.catalog.get_smiles(name)
        if hit is not None:
            return hit
        val = self._base.compound_smiles(name)
        if val:
            self.catalog.put_smiles(name, val)
        return val

    def protein_sequence(self, target: str) -> str | None:
        hit = self.catalog.get_sequence(target)
        if hit is not None:
            return hit
        val = self._base.protein_sequence(target)
        if val:
            self.catalog.put_sequence(target, val)
        return val

    def target_structure(self, target: str) -> dict[str, Any] | None:
        # structures stay in the verified-target library; pass through to the base resolver
        return self._base.target_structure(target)
