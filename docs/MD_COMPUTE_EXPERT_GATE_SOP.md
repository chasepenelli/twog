# MD Compute Expert Gate SOP

## Purpose

TWOG must not submit live MD jobs to RunPod until the exact input packet has been reviewed and approved. The approval can come from a qualified human expert or the dedicated MD expert agent, but it must be tied to the exact packet hash being submitted. This keeps the GPU lane approval-first while the worker contract is still being hardened.

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

For the first smoke route, TWOG should also provide the formerly optional preparation settings explicitly:

- `ph`: currently `7.4` for the pazopanib/KDR smoke.
- `box_padding`: currently `10.0`.
- `force_field`: currently `protein=amber14; ligand=worker_default_openff_or_gaff`.
- `solvent_model`: currently `tip3p`.

The current contract is that TWOG supplies a protein-only PDB and canonical SMILES. The RunPod worker is responsible for SMILES-to-3D conversion, protonation, ligand parameterization, ligand placement, and either a successful smoke result or a structured preparation failure.

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
- The first approved packet for pazopanib/KDR used a stripped protein-only PDB, explicit preparation settings, and packet hash `7601216eb7080b76f83e86262094f727a466d966ed8af0ee437d864faca75ed9`.
- The first live RunPod submission created a RunPod job handle and persisted it in the compute ledger.
- The first terminal RunPod result failed inside worker ligand preparation:
  - status: `FAILED`
  - location: `/app/handler.py`, `dock_ligand_vina`
  - failing command: `mk_prepare_ligand.py -i ligand.pdb -o ligand.pdbqt`
  - interpretation: TWOG reached the RunPod worker successfully; the remaining defect is the worker's ligand preparation path.

That means the next live test should be a scientifically valid prepared packet, not another minimal placeholder.

## Live Runbook

Use this route for a single approved smoke test:

1. Create or reuse the approved validation queue item.
2. Create or reuse the compute job with `compute_job_job`.
3. Generate the expert packet with `md_expert_review_packet_job`.
4. Run the MD expert review agent with OpenRouter and persist the approval.
5. Confirm the exact packet hash in the approval matches the packet hash in the compute job.
6. Submit once with `compute_job_job` using `dry_run=false`, `submit=true`, and `poll=true`.
7. For follow-up polling, use `submit=false` and `poll=true`.

Do not submit again just to poll. If a compute job has already received a RunPod handle, follow-up runs must reuse that handle.

## Polling And Recovery Rules

The durable compute ledger is the source of truth for RunPod handles:

- `runpod_job_id` and `external_run_id` must be preserved whenever an existing compute job is rebuilt from a queue item.
- Poll-only jobs must never erase `runpod_job_id`.
- Poll-only jobs should target an existing `compute_job_id` when possible.
- If an earlier poll-only bug cleared the RunPod handle, use the `recover_runpod_job_id` field once, then poll with `submit=false`.

Recovery exists only to reconnect TWOG to a job that was already submitted. It is not an alternate approval path and must not be used to invent or bypass RunPod job IDs.

## Post-Smoke Decision Tree

After the first live smoke reaches a terminal state:

- `completed`: capture output artifacts, worker status payload, and any metrics. The next planning step is whether the worker output is scientifically interpretable or only operationally successful.
- `failed` with structured ligand/protein prep error: keep the endpoint but create a worker-contract issue with exact missing dependency, preparation step, or input assumption.
- `failed` with unstructured error: harden the worker image/logging before any larger run.
- `running` or `submitted`: keep polling with `submit=false`.
- `blocked`: inspect `last_error`; fix the local gate or ledger path before touching RunPod again.

No larger MD, docking, Boltz, raw omics, or repeated GPU tests should run until this single smoke is understood.

## Worker Fix Plan

The current endpoint appears to prepare ligands through an intermediate `ligand.pdb` file and then convert that file to PDBQT with `mk_prepare_ligand.py`. That is fragile for small molecules because PDB does not reliably preserve ligand bond order, stereochemistry, or charge information.

The preferred worker fix is:

1. Generate the ligand from `compound_smiles` with RDKit.
2. Add hydrogens and embed a 3D conformer.
3. Optimize the conformer with an appropriate force field such as MMFF94 or UFF.
4. Write ligand SDF/MOL with bond orders preserved.
5. Run ligand preparation from SDF/MOL/MOL2 into PDBQT, not from ligand PDB.
6. Capture stdout/stderr from every subprocess and return it in the RunPod `output` payload.
7. Treat ligand-prep failure as a structured result with:
   - `stage`
   - `command`
   - `return_code`
   - `stdout_tail`
   - `stderr_tail`
   - `input_artifact_summary`

The next endpoint should be tested with three tiers before any larger MD claim:

1. Simple positive-control ligand that should pass ligand prep.
2. Pazopanib/KDR smoke with 10 simulation steps.
3. A slightly longer still-non-scientific smoke only after tier 2 completes.

If the current endpoint image cannot be patched, create a new TWOG-owned RunPod worker image with a minimal `handler.py`, explicit requirements, local handler test input, and a container build workflow. RunPod's documented worker layout is `Dockerfile`, `src/handler.py`, and `requirements.txt`; the handler processes `job["input"]` and starts with `runpod.serverless.start`.

## TWOG-Owned MD Worker

The first repo-owned worker lives at `runpod_workers/md_smoke/`.

It preserves ligand chemistry by using this ligand route:

1. parse `compound_smiles` with RDKit,
2. add hydrogens,
3. embed a 3D conformer,
4. optimize with MMFF94 when available, otherwise UFF,
5. write SDF/MOL intermediates with bond orders preserved,
6. prepare PDBQT from SDF with `mk_prepare_ligand.py`.

The worker must not prepare PDBQT from a ligand PDB intermediate. The earlier opaque endpoint failed on `mk_prepare_ligand.py -i ligand.pdb -o ligand.pdbqt`; the owned worker uses `ligand.sdf` as the PDBQT input instead.

The GitHub Actions workflow `.github/workflows/build-md-worker.yml` builds and tests the container for `linux/amd64`, then publishes the private repo image:

- `ghcr.io/chasepenelli/twog-md-worker:<commit-sha>`
- `ghcr.io/chasepenelli/twog-md-worker:smoke-v1`

RunPod uses the credential-free public image published from the worker-only public mirror:

- public repo: `chasepenelli/twog-md-worker-public`
- public image: `ghcr.io/chasepenelli/twog-md-worker-public:smoke-v1`

The new RunPod endpoint should be created from `ghcr.io/chasepenelli/twog-md-worker-public:smoke-v1` with:

- endpoint name: `twog-md-smoke-v1`
- `workersMin=0`
- `workersMax=2` during smoke validation
- raise to `workersMax=5` only after positive-control and pazopanib/KDR smoke tiers pass

After endpoint creation, update `HSA_RUNPOD_ENDPOINT_ID` in GitHub Actions and Dagster+ to the new endpoint ID. Do not reuse the old opaque endpoint for this lane.

Current hosted state:

- GHCR image build and publish workflow: passing on `main`.
- RunPod template: `3qszkm4q1c` (`twog-md-smoke-v1-template`).
- RunPod endpoint: `bpjbi4te75eoul` (`twog-md-smoke-v1`).
- RunPod template image: `ghcr.io/chasepenelli/twog-md-worker-public:smoke-v1`.
- Old opaque MD endpoint: `cbf4ffekmo36t9` (`hsa-md-validation`) reduced from `workersMax=5` to `workersMax=3` to free quota for the owned worker.
- GitHub Actions secret `HSA_RUNPOD_ENDPOINT_ID` points to `bpjbi4te75eoul`; the Dagster+ env sync workflow was dispatched after the update.

Hosted smoke validation:

- Positive-control job `f51a77ba-187e-4eeb-9a15-15d37b83ee1f-u2`: completed.
- Pazopanib/KDR ligand-prep smoke job `63956262-f684-43bc-8e47-97d2a3e57d52-u1`: completed.
- Both jobs completed `input_validation`, `protein_prep`, `ligand_3d`, and `ligand_pdbqt`.
- Both jobs prepared PDBQT from `ligand.sdf`, not from ligand PDB.
- Docking and OpenMM MD remain intentionally skipped in `smoke-v1`.

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
