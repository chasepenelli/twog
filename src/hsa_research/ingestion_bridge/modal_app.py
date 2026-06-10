"""Modal app for twog compute lanes — first lane: the omics-crux CPU analysis.

Runs the omics-review engine on Modal cloud CPU (fits easily in the $30/mo free tier). If no
inline expression is provided, the heavy SRA/GEO pull (load_omics_dataset) runs *remotely* on
Modal where there's network + disk — that's the natural home for it.

Auth: `python -m modal setup` (done; token in ~/.modal.toml, profile chasepenelli).

Run a smoke (first real cloud execution) from the repo root with the package importable:
    PYTHONPATH=src python -m modal run src/hsa_research/ingestion_bridge/modal_app.py

Deploy it so the ModalComputeRunner adapter can call it by name:
    PYTHONPATH=src python -m modal deploy src/hsa_research/ingestion_bridge/modal_app.py

Note: the local-source inclusion (add_local_python_source) is verified on the first `modal run`;
if the remote import of hsa_research fails, run with PYTHONPATH=src as above (the package must be
importable locally for Modal to ship it).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    import modal
except ImportError:  # modal is an optional dep — the rest of twog never imports this module eagerly
    modal = None  # type: ignore[assignment]


if modal is not None:
    # Ship ONLY the self-contained omics_review module (numpy-only) as a flat top-level module —
    # importing it through the hsa_research package would drag in contracts/pydantic/etc., which the
    # slim image doesn't have. The engine has zero twog dependencies, so this is clean and minimal.
    _OMICS_MODULE = Path(__file__).resolve().parent / "omics_review.py"
    image = (
        modal.Image.debian_slim()
        .pip_install("numpy>=1.26,<3")
        .add_local_file(str(_OMICS_MODULE), "/root/omics_review.py")
    )
    app = modal.App("twog-compute")

    @app.function(image=image, cpu=1.0, timeout=900)
    def run_omics_review_remote(config: dict[str, Any]) -> dict[str, Any]:
        """Remote omics-review: runs the real analysis engine on Modal CPU."""
        import sys

        sys.path.insert(0, "/root")
        from omics_review import load_omics_dataset, run_omics_review

        expression = config.get("expression")
        strata = config.get("strata")
        if not expression or not strata:
            expression, strata = load_omics_dataset(config.get("datasets") or [])
        return run_omics_review(
            expression=expression,
            strata=strata,
            signatures=config.get("signatures"),
            direction_hypothesis=config.get("direction_hypothesis", "immunosuppression_higher_in_mutant"),
            min_n_per_stratum=int(config.get("min_n_per_stratum", 5)),
            source_refs=config.get("source_refs"),
        )

    @app.local_entrypoint()
    def main() -> None:
        """Tiny smoke: a fixture where the PIK3CA-mutant stratum IS immunosuppressed -> 'supports'."""
        genes = [
            "FOXP3", "CTLA4", "IL2RA", "IKZF2", "CD163", "MRC1", "MSR1", "CSF1R",
            "IL10", "TGFB1", "CCL2", "IL6", "CXCL8",
        ]
        mut = [f"m{i}" for i in range(5)]
        wt = [f"w{i}" for i in range(5)]
        expression = {s: {g: 3.0 for g in genes} for s in mut}
        expression.update({s: {g: 1.0 for g in genes} for s in wt})
        strata = {**{s: "mutant" for s in mut}, **{s: "wt" for s in wt}}
        result = run_omics_review_remote.remote(
            {"expression": expression, "strata": strata, "source_refs": ["PRJNA562916", "GSE225599"]}
        )
        print(f"Modal omics result -> signal={result['signal']} confidence={result['confidence']}")
        print(f"  n_mutant={result['metrics']['n_mutant']} n_wt={result['metrics']['n_wt']}")
