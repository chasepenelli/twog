# MD Compute Expert Gate SOP

## Purpose

TWOG must not submit live MD jobs to RunPod until the exact input packet has been reviewed by a qualified human expert. This keeps the GPU lane approval-first while the worker contract is still being hardened.

## Scope

This SOP covers the first MD lane only:

- Endpoint: `hsa-md-validation`
- Endpoint ID: `cbf4ffekmo36t9`
- Runner: RunPod serverless
- Job type: one smoke-scale MD worker contract test

DiffDock, Boltz, docking, raw omics, and larger MD workflows are out of scope until this gate proves reliable.

## Required MD Input Packet

Every live MD job must include:

- `protein_pdb`: PDB text with at least one `ATOM` or `HETATM` record and `TER` or `END`.
- `compound_smiles`: single ligand SMILES string without whitespace.
- `target_name`: target or protein name.
- `compound_name`: ligand/candidate name.
- `simulation_steps`: first smoke runs must be small, currently capped at 1000.
- `temperature`: simulation temperature.
- `protein_source`: provenance for the protein structure.
- `ligand_source`: provenance for the ligand identity.
- `preparation_method`: how the protein/ligand inputs were prepared.

Optional fields include `ph`, `box_padding`, `force_field`, and `solvent_model`.

## Workflow

1. Create or select an approved MD compute job from the validation queue.
2. Generate an MD expert review packet with `md_expert_review_packet_job` in Dagster, or through the service method `create_md_expert_review_packet`.
3. Review the generated `review_document` with either:
   - a human MD, docking, or structure-computation expert, or
   - the dedicated `md_expert_review_agent`.
4. The expert must return one of:
   - `approved`
   - `needs_changes`
   - `rejected`
5. Persist that decision with `record_md_expert_approval`.
   - Human approvals are stored with `reviewer_type=human_expert`.
   - Agent approvals are stored with `reviewer_type=md_expert_agent` and linked to the `AgentRunRecord`.
6. Live submit is allowed only when:
   - the compute job is approved,
   - `dry_run=false`,
   - `RUNPOD_API_KEY` and `HSA_RUNPOD_ENDPOINT_ID` are configured,
   - the exact packet hash exists,
   - the exact packet hash has an `approved` expert approval from either a human expert or the MD expert agent,
   - safety and cost bounds are present.

## Current Worker History

The current worker contract discovered from live tests:

- Missing `protein_pdb` fails with `protein_pdb is required`.
- Missing `compound_smiles` fails with `compound_smiles is required`.
- Minimal ligand smoke reached worker execution and failed during ligand preparation.

That means the next live test should be a scientifically valid prepared packet, not another minimal placeholder.

## Expert Checklist

The expert should answer:

- Are these protein and ligand inputs valid for a smoke test?
- Is the worker contract complete?
- Are ligand/protein preparation assumptions acceptable?
- What fields or artifacts are missing?
- Is this endpoint appropriate, or should the worker image be rebuilt?
- Are cost and capacity bounds acceptable for exactly one small run?

## MD Expert Agent

The MD expert agent is a recommend-and-approve gate for smoke-scale compute readiness. It does not claim therapeutic efficacy and does not replace later scientific validation. It reviews:

- exact packet hash,
- protein and ligand provenance,
- preparation assumptions,
- worker input schema,
- expected outputs and failure modes,
- safety/cost bounds,
- prior worker errors.

It can approve only the exact packet it reviewed. Changing the PDB, SMILES, endpoint, model settings, or packet version changes the hash and requires another review.

## Gate Behavior

If a live MD submit is attempted without the required packet and approval, TWOG blocks the job locally and records one of:

- `md_input_packet_invalid`
- `md_expert_review_packet_required`
- `md_safety_cost_bounds_required`
- `md_expert_approval_required`

The gate intentionally blocks before sending anything to RunPod.
