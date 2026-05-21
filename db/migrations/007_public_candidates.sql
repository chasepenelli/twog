create table if not exists public_candidates (
  candidate_id text primary key,
  display_id text,
  candidate_kind text not null,
  public_status text not null,
  visibility text not null,
  therapy_idea_id text,
  latest_snapshot_id text,
  content_hash text,
  priority_score double precision not null default 0.5,
  title text not null,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists public_candidates_status_idx
  on public_candidates(public_status, priority_score desc, updated_at desc);
create index if not exists public_candidates_visibility_idx
  on public_candidates(visibility, updated_at desc);
create index if not exists public_candidates_kind_idx
  on public_candidates(candidate_kind, updated_at desc);
create index if not exists public_candidates_therapy_idx
  on public_candidates(therapy_idea_id, updated_at desc);
create index if not exists public_candidates_display_idx
  on public_candidates(display_id);

create table if not exists public_candidate_snapshots (
  snapshot_id text primary key,
  candidate_id text not null,
  snapshot_version integer not null,
  content_hash text not null,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  unique(candidate_id, snapshot_version)
);

create index if not exists public_candidate_snapshots_candidate_idx
  on public_candidate_snapshots(candidate_id, snapshot_version desc);
create index if not exists public_candidate_snapshots_hash_idx
  on public_candidate_snapshots(content_hash);

create table if not exists public_candidate_decision_events (
  event_id text primary key,
  candidate_id text not null,
  occurred_at timestamptz not null,
  action text not null,
  actor text not null,
  new_status text,
  related_snapshot_id text,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

create index if not exists public_candidate_decision_events_candidate_idx
  on public_candidate_decision_events(candidate_id, occurred_at desc);
create index if not exists public_candidate_decision_events_action_idx
  on public_candidate_decision_events(action, occurred_at desc);
