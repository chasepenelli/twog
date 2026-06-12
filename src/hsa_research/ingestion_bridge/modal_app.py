"""Backward-compat shim. The Modal app moved UP one level to ``hsa_research.modal_app`` so the
adapter->Modal path loads it WITHOUT triggering ``ingestion_bridge/__init__`` (which imports pydantic,
absent from the slim science images). Importing the functions here keeps their ``__module__`` =
``hsa_research.modal_app`` (light), which is what Modal ships + imports on the container.

Smoke / deploy now target the new path:
    PYTHONPATH=src python -m modal run    src/hsa_research/modal_app.py
    PYTHONPATH=src python -m modal deploy src/hsa_research/modal_app.py
"""

from __future__ import annotations

from hsa_research.modal_app import *  # noqa: F401,F403
from hsa_research.modal_app import (  # noqa: F401  (explicit re-exports the adapter imports)
    app,
    run_boltz_remote,
    run_gnina_remote,
    run_md_checkpoint_remote,
    run_omics_review_remote,
)
