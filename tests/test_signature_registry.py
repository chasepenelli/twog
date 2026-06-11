"""Guard the shipped canine TME signature registry (structure + dual-assembly coverage)."""

from __future__ import annotations

import json
import pathlib

REGISTRY = (
    pathlib.Path(__file__).resolve().parents[1]
    / "datasets" / "canine_hsa_comparative" / "canine_tme_signature_registry.json"
)


def _load():
    return json.loads(REGISTRY.read_text())


def test_registry_structure_and_assemblies():
    reg = _load()
    assert reg["version"] == "canine-tme-signatures-v1"
    assert set(reg["assemblies"]) == {"canfam3_1", "ros_cfam_1_0"}
    assert len(reg["panels"]) >= 16
    # CD8A is a core marker — must resolve to both assemblies in the expected namespaces
    cd8a = reg["gene_index"]["CD8A"]
    assert cd8a["canfam3_1"].startswith("ENSCAFG00000")
    assert cd8a["ros_cfam_1_0"].startswith("ENSCAFG00845")


def test_registry_coverage_is_high_and_honest():
    reg = _load()
    n = reg["coverage"]["genes"]
    # near-complete on both assemblies; coverage is recorded explicitly (not hidden)
    assert reg["coverage"]["canfam3_1_resolved"] >= n - 2
    assert reg["coverage"]["ros_cfam_1_0_resolved"] >= n - 2
    # every panel gene appears in the flat index
    for panel in reg["panels"].values():
        for gene in panel:
            assert gene["symbol"] in reg["gene_index"]
