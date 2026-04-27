"""Dagster+ entrypoint for the HSA AutoResearch v2 assets."""

from dagster import definitions

from hsa_research.ingestion_bridge.dagster_assets import defs as ingestion_bridge_defs


@definitions
def defs():
    return ingestion_bridge_defs
