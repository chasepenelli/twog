create table if not exists candidate_contribution_intake (
  contribution_id uuid primary key,
  candidate_id text not null,
  display_id text,
  snapshot_content_hash text,
  source_payload_url text not null,
  status text not null default 'queued_for_intake',
  contribution_type text not null,
  relation_to_current_record text not null,
  requested_system_action text not null,
  contributor jsonb not null default '{}'::jsonb,
  evidence jsonb not null default '[]'::jsonb,
  artifacts jsonb not null default '[]'::jsonb,
  packet jsonb not null,
  review_notes text,
  promoted_queue_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  reviewed_at timestamptz,
  constraint candidate_contribution_intake_status_check
    check (status in (
      'queued_for_intake',
      'triage_in_progress',
      'needs_more_information',
      'rejected',
      'accepted_for_evidence_review',
      'accepted_for_validation_queue',
      'accepted_for_compute_review',
      'archived'
    )),
  constraint candidate_contribution_intake_type_check
    check (contribution_type in (
      'evidence',
      'critique',
      'replication',
      'artifact',
      'validation_proposal',
      'compute_result'
    ))
);

create index if not exists candidate_contribution_intake_candidate_created_idx
  on candidate_contribution_intake (candidate_id, created_at desc);

create index if not exists candidate_contribution_intake_status_created_idx
  on candidate_contribution_intake (status, created_at desc);

create index if not exists candidate_contribution_intake_action_created_idx
  on candidate_contribution_intake (requested_system_action, created_at desc);
