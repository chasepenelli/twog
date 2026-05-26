# TWOG Research Workspaces

Research workspaces are the bounded execution layer between public WorkPackets and reviewed ProofCapsules.

The point is not to let public contributors mutate TWOG. The point is to let a person or agent check out a specific research task into an isolated environment, work against a frozen candidate snapshot and evidence bundle, then check the result back in for operator review.

## Flow

```text
WorkPacket
  -> ResearchWorkspaceRecord
      -> sandbox provider record
      -> optional Neon branch record
      -> checkout manifest hash
      -> skill profile
  -> contributor/agent work
  -> ProofCapsule check-in
  -> operator review
  -> candidate update, validation routing, archive, or rejection
```

## What The Record Stores

`ResearchWorkspaceRecord` is intentionally provider-neutral. It can describe a manual workspace today and an E2B, Daytona, Coder, DevPod, or GitHub Codespaces workspace later without changing the public contract.

It stores:

- candidate and WorkPacket identifiers;
- candidate snapshot and evidence bundle hashes;
- checkout manifest hash and payload;
- sandbox provider and provider workspace ID;
- Git repo/ref/branch used in the workspace;
- Neon branch ID/name/parent branch;
- a secret reference for the branch database URL, never the raw DSN;
- artifact root;
- status, expiration, errors, and metadata;
- skill profile and installed skill references.

## Skill Profiles

K-Dense Scientific Agent Skills are useful here as workspace presets, not as core TWOG dependencies.

TWOG should install or expose only the skills needed for a specific WorkPacket:

| Skill profile | Typical WorkPacket | Useful skill refs |
| --- | --- | --- |
| `literature_and_citation` | citation repair, claim critique | `literature-review`, `citation-management`, `paper-lookup`, `parallel-web` |
| `database_lookup` | target/source lookup | `database-lookup`, `bioservices`, `gget`, `biopython` |
| `chemistry` | compound critique, analog search | `rdkit`, `datamol`, `medchem`, `molfeat`, `deepchem` |
| `omics` | expression or dataset note | `scanpy`, `pydeseq2`, `anndata`, `cellxgene-census`, `gget` |
| `md_review` | docking/MD reproducibility review | `molecular-dynamics`, `rdkit`, `diffdock`, `mdanalysis`-style workflows |
| `validation_planning` | validation proposal | `scientific-critical-thinking`, `peer-review`, `hypothesis-generation` |

Do not install the full K-Dense catalog by default. A broad skill install expands the attack surface and makes runs harder to reproduce.

## Boundaries

- Workspace users do not receive production database credentials.
- Neon branches are isolated and should have TTLs.
- Public submissions do not mutate candidate records.
- GPU compute is not triggered from public workspace check-in.
- Operator review remains the write gate.

## Neon Branch Allocator

The first allocator is `provision_neon_branch_workspace(...)` in `research_workspaces.py`.

It accepts a `NeonBranchWorkspaceRequest`, creates or reuses a named Neon branch, asks Neon for a connection URI only to verify the branch is connectable, and persists a `ResearchWorkspaceRecord`.

Important behavior:

- `dry_run=true` records the planned workspace without touching Neon.
- `dry_run=false` requires an injected Neon client.
- Branches are idempotent by branch name.
- The allocator stores `database_secret_ref`, not the raw database URL.
- TTL defaults to 24 hours.
- A read-write endpoint is requested for live branches.
- K-Dense-style skill presets are attached from `skill_profile`.
- Live service execution reads `NEON_API_KEY`, `NEON_PROJECT_ID`, and optional `NEON_PARENT_BRANCH_ID`.
- Missing live secrets return a persisted `failed` workspace result instead of silently falling back to dry-run behavior.

The direct Neon API endpoints behind the live client are:

- `POST /projects/{project_id}/branches`
- `GET /projects/{project_id}/branches`
- `GET /projects/{project_id}/connection_uri`

This allocator does not yet inject the database URL into E2B, Daytona, Coder, or DevPod. That belongs to the sandbox-provider slice.

## Operator Surfaces

CLI:

```bash
hsa-ingestion research-workspace-neon \
  --candidate-id twog-candidate-447eb8089965 \
  --work-packet-id wp-citation-1 \
  --skill-profile literature_and_citation
```

The command above is a dry run. Live Neon creation requires `--execute` and the Neon env vars above.

```bash
hsa-ingestion research-workspaces --candidate-id twog-candidate-447eb8089965
```

Dagster:

- `research_workspace_neon_report` / `research_workspace_neon_job`
- `research_workspace_library_report` / `research_workspace_library_job`

Dagster defaults to `dry_run=true`; live creation requires `dry_run=false` in run config. The GitHub env sync workflow publishes Neon secrets to Dagster+ only when they exist.

## Daytona Stub

`DaytonaWorkspaceRequest` and `plan_daytona_workspace(...)` are intentionally non-live in this slice. The stub records the intended sandbox shape and fails closed for live execution. The future Daytona provider should receive:

- Neon branch `database_secret_ref`, not a raw DSN;
- checkout manifest hash and payload;
- candidate snapshot and evidence bundle hashes;
- repo/ref/branch checkout information;
- selected K-Dense skill refs from `skill_profile`.

Contributor workspaces must not receive production database credentials, OpenRouter keys, RunPod keys, or GitHub write tokens.

## Next Slices

1. Daytona sandbox allocator.
2. Checkout manifest builder.
3. ProofCapsule link-back to `workspace_id`.
4. Workspace cleanup and expiration job.
5. Operator cleanup/revoke surface for workspace reports.
