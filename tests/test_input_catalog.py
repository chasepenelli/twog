"""Durable resolved-input catalog — resolve-once, reuse-anywhere (read-through, never fabricates)."""

from __future__ import annotations

from hsa_research.ingestion_bridge.input_catalog import CatalogResolvers, InputCatalog


class _CountingResolver:
    """A base resolver that counts network hits and can be set to 'offline' (raise on any call)."""

    def __init__(self, smiles=None, seq=None, offline=False):
        self._smiles, self._seq, self.offline = smiles, seq, offline
        self.calls = 0

    def compound_smiles(self, name):
        if self.offline:
            raise RuntimeError("network called")
        self.calls += 1
        return self._smiles

    def protein_sequence(self, target):
        if self.offline:
            raise RuntimeError("network called")
        self.calls += 1
        return self._seq

    def target_structure(self, target):
        return None


def test_catalog_read_through_fetches_once_then_reuses(tmp_path):
    cat = InputCatalog(tmp_path / "cat.json")
    base = _CountingResolver(smiles="CCO")
    r = CatalogResolvers(base, cat)
    assert r.compound_smiles("ethanol") == "CCO" and base.calls == 1  # first: network
    assert r.compound_smiles("ethanol") == "CCO" and base.calls == 1  # second: catalog, NO network
    # a fresh catalog from the same file + an OFFLINE base still answers (durable across processes)
    offline = CatalogResolvers(_CountingResolver(offline=True), InputCatalog(tmp_path / "cat.json"))
    assert offline.compound_smiles("ethanol") == "CCO"  # served from disk, no network


def test_catalog_sequences_persist(tmp_path):
    cat = InputCatalog(tmp_path / "c.json")
    CatalogResolvers(_CountingResolver(seq="MAAA"), cat).protein_sequence("KDR")
    assert InputCatalog(tmp_path / "c.json").get_sequence("kdr") == "MAAA"  # case-insensitive key, persisted


def test_catalog_never_stores_a_miss(tmp_path):
    cat = InputCatalog(tmp_path / "c.json")
    base = _CountingResolver(smiles=None)  # PubChem miss
    r = CatalogResolvers(base, cat)
    assert r.compound_smiles("nonesuch") is None
    assert cat.get_smiles("nonesuch") is None  # a miss is NOT cached as a value
    # a later successful resolve still works (the miss didn't poison the catalog)
    base._smiles = "C1=CC=CC=C1"
    assert r.compound_smiles("nonesuch") == "C1=CC=CC=C1"


def test_catalog_counts(tmp_path):
    cat = InputCatalog(tmp_path / "c.json")
    cat.put_smiles("Aspirin", "CC(=O)OC1=CC=CC=C1C(=O)O")
    cat.put_sequence("PIK3CA", "MPPR")
    assert cat.counts == {"compounds": 1, "sequences": 1}
    assert cat.get_smiles("aspirin")  # case-insensitive
