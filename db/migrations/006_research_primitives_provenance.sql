-- TWOG research primitive provenance tables.

create table if not exists source_versions (
  source_key text primary key,
  source_version text not null,
  materialized_at timestamptz not null,
  source_url text,
  artifact_id text,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists source_versions_materialized_idx
  on source_versions(materialized_at desc);

create table if not exists primitive_call_events (
  event_id text primary key,
  primitive_name text not null,
  status text not null,
  request_hash text not null,
  result_hash text,
  agent_run_id text,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

create index if not exists primitive_call_events_name_idx
  on primitive_call_events(primitive_name, created_at desc);

create index if not exists primitive_call_events_agent_idx
  on primitive_call_events(agent_run_id, created_at desc);
