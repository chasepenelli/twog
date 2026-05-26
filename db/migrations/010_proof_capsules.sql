create table if not exists proof_capsules (
  capsule_id text primary key,
  workspace_id text not null,
  checkout_manifest_hash text not null,
  candidate_id text not null,
  work_packet_id text,
  packet_type text not null,
  requested_action text not null,
  status text not null,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists proof_capsules_candidate_status_idx
  on proof_capsules(candidate_id, status, updated_at desc);

create index if not exists proof_capsules_workspace_idx
  on proof_capsules(workspace_id, updated_at desc);

create index if not exists proof_capsules_manifest_idx
  on proof_capsules(checkout_manifest_hash, updated_at desc);

create index if not exists proof_capsules_packet_idx
  on proof_capsules(packet_type, status, updated_at desc);
