"""Build a versioned canine immune/TME signature registry (the dog-specific MSigDB-style resource).

Resolves each human marker symbol to BOTH canine assemblies — CanFam3.1 (ENSCAFG00000…, archived
Ensembl r104; joins GSE95183) and ROS_Cfam_1.0 (ENSCAFG00845…, current Ensembl; joins new datasets)
— so a downstream analysis can use whichever assembly its matrix is on. Unresolved genes are kept
as null (canine annotation is sparser than human) so coverage is explicit, not hidden.

    python datasets/canine_hsa_comparative/build_signature_registry.py
"""
from __future__ import annotations

import json
import os
import time
import urllib.request

REGISTRY_VERSION = "canine-tme-signatures-v1"
SP = "canis_lupus_familiaris"
HOST_CANFAM31 = "https://may2021.rest.ensembl.org"  # release 104 = CanFam3.1
HOST_ROS = "https://rest.ensembl.org"               # current = ROS_Cfam_1.0

PANELS: dict[str, list[str]] = {
    "T_cell_general": ["CD3D", "CD3E", "CD2", "CD5"],
    "CD8_cytotoxic": ["CD8A", "CD8B", "GZMB", "GZMA", "GZMK", "PRF1", "NKG7"],
    "CD4_helper": ["CD4", "IL7R"],
    "Treg": ["FOXP3", "CTLA4", "IL2RA", "IKZF2"],
    "NK": ["KLRK1", "NCR1", "KLRB1"],
    "B_cell": ["CD19", "MS4A1", "CD79A", "CD79B"],
    "M1_macrophage": ["CD68", "NOS2", "CD86", "IL1B"],
    "M2_TAM": ["CD163", "MRC1", "MSR1", "CSF1R"],
    "dendritic": ["ITGAX", "BATF3", "FLT3"],
    "IFNg_response": ["IFNG", "STAT1", "IRF1", "GBP1", "CXCL9", "CXCL10", "CXCL11", "IDO1", "TAP1", "PSMB9", "B2M", "JAK2"],
    "checkpoint_exhaustion": ["PDCD1", "LAG3", "HAVCR2", "CTLA4", "CD274", "TIGIT"],
    "immunosuppressive_cytokines": ["IL10", "TGFB1", "TGFB2", "CCL2", "IL6"],
    "fibroblast_stroma": ["COL1A1", "COL3A1", "PDGFRB", "ACTA2", "DCN", "LUM", "FAP", "THY1"],
    "endothelial": ["PECAM1", "VWF", "CDH5", "KDR", "TEK", "FLT1"],
    "angiogenesis": ["VEGFA", "ANGPT2", "DLL4", "ESM1", "CD34", "NOTCH4"],
    "proliferation": ["MKI67", "PCNA", "TOP2A", "CCNB1", "CCNA2", "BUB1", "FOXM1", "CDK1"],
}


def resolve(host: str, symbol: str, prefix: str) -> str | None:
    url = f"{host}/xrefs/symbol/{SP}/{symbol}?content-type=application/json"
    try:
        with urllib.request.urlopen(url, timeout=25) as r:
            for d in json.load(r):
                if str(d.get("id", "")).startswith(prefix):
                    return d["id"]
    except Exception:
        return None
    return None


def build() -> None:
    genes = sorted({g for v in PANELS.values() for g in v})
    index: dict[str, dict] = {}
    for g in genes:
        index[g] = {
            "symbol": g,
            "canfam3_1": resolve(HOST_CANFAM31, g, "ENSCAFG00000"),
            "ros_cfam_1_0": resolve(HOST_ROS, g, "ENSCAFG00845"),
        }
        time.sleep(0.15)
    resolved31 = sum(1 for v in index.values() if v["canfam3_1"])
    resolved_ros = sum(1 for v in index.values() if v["ros_cfam_1_0"])
    registry = {
        "version": REGISTRY_VERSION,
        "species": SP,
        "assemblies": {"canfam3_1": "Ensembl r104", "ros_cfam_1_0": "Ensembl current"},
        "coverage": {"genes": len(genes), "canfam3_1_resolved": resolved31, "ros_cfam_1_0_resolved": resolved_ros},
        "panels": {name: [index[g] for g in panel] for name, panel in PANELS.items()},
        "gene_index": index,
    }
    out = os.path.join(os.path.dirname(__file__), "canine_tme_signature_registry.json")
    json.dump(registry, open(out, "w"), indent=2)
    print(f"{len(PANELS)} panels, {len(genes)} genes | CanFam3.1 {resolved31}/{len(genes)} | "
          f"ROS_Cfam_1.0 {resolved_ros}/{len(genes)} -> {out}")


if __name__ == "__main__":
    build()
