-- Proof Network work packets.
--
-- A work_packet is a bounded, checkout-able task that TWOG publishes against
-- a public candidate. Humans and agents pick a packet up, perform the work,
-- and submit a ProofCapsule (migration 009). Public reads of work packets
-- are intake-only and never mutate candidate state.

create table if not exists work_packets (
  work_packet_id uuid primary key,
  candidate_id text not null,
  packet_type text not null,
  title text not null,
  question text not null,
  why_it_matters text not null default '',
  target_claim_ids jsonb not null default '[]'::jsonb,
  target_section text,
  required_inputs jsonb not null default '[]'::jsonb,
  suggested_methods jsonb not null default '[]'::jsonb,
  expected_outputs jsonb not null default '[]'::jsonb,
  acceptance_criteria jsonb not null default '[]'::jsonb,
  reward_hint text not null default '',
  difficulty text not null default 'moderate',
  status text not null default 'open',
  notebook_recommended boolean not null default false,
  created_by text not null default 'twog_system',
  retired_reason text,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint work_packets_packet_type_check
    check (packet_type in (
      'citation_repair',
      'claim_critique',
      'evidence_addition',
      'omics_note',
      'docking_replication',
      'md_review',
      'validation_proposal',
      'demotion_case',
      'methods_review'
    )),
  constraint work_packets_difficulty_check
    check (difficulty in ('light', 'moderate', 'heavy')),
  constraint work_packets_status_check
    check (status in ('open', 'in_progress', 'completed', 'retired'))
);

create index if not exists work_packets_candidate_status_idx
  on work_packets(candidate_id, status, updated_at desc);
create index if not exists work_packets_status_created_idx
  on work_packets(status, created_at desc);
create index if not exists work_packets_type_status_idx
  on work_packets(packet_type, status, updated_at desc);
