"""MCP server for HSA AutoResearch.

Run locally:

    python -m hsa_research.ingestion_bridge.mcp_server

Then connect Claude Code or an MCP inspector to the streamable HTTP server.
"""

from __future__ import annotations

from uuid import UUID

from .contracts import (
    BoltzRunRequest,
    CandidateDossierRequest,
    ClaimCurationRequest,
    ClaimSearchRequest,
    CommitHypothesisRequest,
    HypothesisDraft,
    HypothesisProposalRequest,
    ModelProfile,
    SourceScoutRequest,
    ValidationRequest,
)
from .service import get_service

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:  # pragma: no cover - exercised only when optional dependency is missing
    FastMCP = None  # type: ignore[assignment]


if FastMCP is None:  # pragma: no cover
    mcp = None
else:
    mcp = FastMCP(
        "HSA AutoResearch",
        stateless_http=True,
        json_response=True,
        instructions=(
            "Use these tools to inspect HSA claims, candidates, hypotheses, "
            "and async validation runs. Expensive tools return run handles."
        ),
    )


if mcp is not None:

    @mcp.tool()
    def search_claims(
        query: str | None = None,
        species: str | None = None,
        targets: list[str] | None = None,
        compounds: list[str] | None = None,
        min_confidence: float = 0.0,
        include_drafts: bool = False,
        limit: int = 20,
    ) -> dict:
        """Search provenance-backed scientific claims."""

        request = ClaimSearchRequest(
            query=query,
            species=species,
            targets=targets or [],
            compounds=compounds or [],
            min_confidence=min_confidence,
            include_drafts=include_drafts,
            limit=limit,
        )
        return get_service().search_claims(request).model_dump(mode="json")

    @mcp.tool()
    def curate_claims(
        source_key: str | None = None,
        query: str | None = None,
        limit: int = 100,
        min_confidence: float = 0.0,
        promote_threshold: float = 0.5,
        include_seed_claims: bool = False,
        dry_run: bool = False,
        model_profile: str = "reviewer",
    ) -> dict:
        """Run the claim curator agent to dedupe, review, and promote draft claims."""

        request = ClaimCurationRequest(
            source_key=source_key,
            query=query,
            limit=limit,
            min_confidence=min_confidence,
            promote_threshold=promote_threshold,
            include_seed_claims=include_seed_claims,
            dry_run=dry_run,
            model_profile=model_profile,
        )
        return get_service().curate_claims(request).model_dump(mode="json")

    @mcp.tool()
    def scout_sources(
        focus: str = "all",
        max_phase: int = 3,
        max_recommendations: int = 12,
        include_registered_sources: bool = True,
        include_expansion_sources: bool = True,
        model_profile: str = "long_context_reviewer",
    ) -> dict:
        """Prioritize ingestion source gaps and starter queries."""

        request = SourceScoutRequest(
            focus=focus,  # type: ignore[arg-type]
            max_phase=max_phase,
            max_recommendations=max_recommendations,
            include_registered_sources=include_registered_sources,
            include_expansion_sources=include_expansion_sources,
            model_profile=model_profile,
        )
        return get_service().scout_sources(request).model_dump(mode="json")

    @mcp.tool()
    def get_candidate(
        candidate_id: str | None = None,
        candidate_name: str | None = None,
        include_claims: bool = True,
        include_artifacts: bool = True,
        include_validation: bool = True,
    ) -> dict:
        """Return a candidate dossier with evidence, artifacts, validation state, and risk flags."""

        request = CandidateDossierRequest(
            candidate_id=UUID(candidate_id) if candidate_id else None,
            candidate_name=candidate_name,
            include_claims=include_claims,
            include_artifacts=include_artifacts,
            include_validation=include_validation,
        )
        dossier = get_service().get_candidate(request)
        return {} if dossier is None else dossier.model_dump(mode="json")

    @mcp.tool()
    def propose_hypothesis(
        objective: str,
        claim_ids: list[str] | None = None,
        candidate_name: str | None = None,
        target_name: str | None = None,
        species: str = "canine",
        commit: bool = False,
    ) -> dict:
        """Draft a hypothesis from claims and gaps. By default this does not commit durable state."""

        request = HypothesisProposalRequest(
            objective=objective,
            claim_ids=[UUID(claim_id) for claim_id in (claim_ids or [])],
            candidate_name=candidate_name,
            target_name=target_name,
            species=species,
            commit=commit,
        )
        return get_service().propose_hypothesis(request).model_dump(mode="json")

    @mcp.tool()
    def commit_hypothesis(draft: dict, approved_by: str, approval_note: str | None = None) -> dict:
        """Persist a human-approved hypothesis draft."""

        request = CommitHypothesisRequest(
            draft=HypothesisDraft.model_validate(draft),
            approved_by=approved_by,
            approval_note=approval_note,
        )
        return get_service().commit_hypothesis(request).model_dump(mode="json")

    @mcp.tool()
    def run_boltz(
        target_name: str,
        ligand_smiles: str | None = None,
        ligand_name: str | None = None,
        protein_sequence: str | None = None,
        candidate_id: str | None = None,
        priority: int = 100,
        require_approval: bool = True,
    ) -> dict:
        """Queue a Boltz prediction request and return an async run handle."""

        request = BoltzRunRequest(
            target_name=target_name,
            ligand_smiles=ligand_smiles,
            ligand_name=ligand_name,
            protein_sequence=protein_sequence,
            candidate_id=UUID(candidate_id) if candidate_id else None,
            priority=priority,
            require_approval=require_approval,
        )
        return get_service().run_boltz(request).model_dump(mode="json")

    @mcp.tool()
    def request_validation(
        validation_type: str,
        objective: str,
        candidate_id: str | None = None,
        candidate_name: str | None = None,
        target_name: str | None = None,
        priority: int = 100,
        require_approval: bool = True,
        metadata: dict | None = None,
    ) -> dict:
        """Queue validation such as docking, MD, ADMET, homology, safety, or expert review."""

        request = ValidationRequest(
            validation_type=validation_type,  # type: ignore[arg-type]
            objective=objective,
            candidate_id=UUID(candidate_id) if candidate_id else None,
            candidate_name=candidate_name,
            target_name=target_name,
            priority=priority,
            require_approval=require_approval,
            metadata=metadata or {},
        )
        return get_service().request_validation(request).model_dump(mode="json")

    @mcp.tool()
    def get_run_status(run_id: str) -> dict:
        """Return status for a Dagster, RunPod, MCP, local, or external async run."""

        handle = get_service().get_run_status(UUID(run_id))
        return {} if handle is None else handle.model_dump(mode="json")

    @mcp.tool()
    def get_artifact(artifact_id: str) -> dict:
        """Return artifact metadata and links."""

        artifact = get_service().get_artifact(UUID(artifact_id))
        return {} if artifact is None else artifact.model_dump(mode="json")

    @mcp.tool()
    def list_model_profiles() -> dict:
        """List simple logical model profiles used by the service layer."""

        profiles = get_service().list_model_profiles()
        return {"profiles": [profile.model_dump(mode="json") for profile in profiles]}

    @mcp.tool()
    def set_model_profile(
        profile_key: str,
        provider: str,
        purpose: str,
        model_name: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> dict:
        """Update a simple model profile without adding a model gateway."""

        profile = ModelProfile(
            profile_key=profile_key,
            provider=provider,  # type: ignore[arg-type]
            model_name=model_name,
            purpose=purpose,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return get_service().set_model_profile(profile).model_dump(mode="json")

    @mcp.resource("claim://{claim_id}")
    def claim_resource(claim_id: str) -> dict:
        """Fetch a claim as an MCP resource."""

        claim = get_service().get_claim(UUID(claim_id))
        return {} if claim is None else claim.model_dump(mode="json")

    @mcp.resource("run://{run_id}")
    def run_resource(run_id: str) -> dict:
        """Fetch an async run as an MCP resource."""

        handle = get_service().get_run_status(UUID(run_id))
        return {} if handle is None else handle.model_dump(mode="json")

    @mcp.resource("artifact://{artifact_id}")
    def artifact_resource(artifact_id: str) -> dict:
        """Fetch artifact metadata as an MCP resource."""

        artifact = get_service().get_artifact(UUID(artifact_id))
        return {} if artifact is None else artifact.model_dump(mode="json")


def main() -> None:
    """Run the MCP server using streamable HTTP."""

    if mcp is None:
        raise RuntimeError('Install the MCP SDK first: pip install "mcp[cli]"')
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
