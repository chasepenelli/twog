create table if not exists research_workspaces (
  workspace_id text primary key,
  work_packet_id text,
  candidate_id text not null,
  provider text not null,
  status text not null,
  skill_profile text not null,
  provider_workspace_id text,
  neon_branch_id text,
  neon_branch_name text,
  checkout_manifest_hash text,
  expires_at timestamptz,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists research_workspaces_candidate_status_idx
  on research_workspaces(candidate_id, status, updated_at desc);

create index if not exists research_workspaces_packet_idx
  on research_workspaces(work_packet_id, updated_at desc);

create index if not exists research_workspaces_provider_status_idx
  on research_workspaces(provider, status, updated_at desc);

create index if not exists research_workspaces_skill_idx
  on research_workspaces(skill_profile, updated_at desc);
