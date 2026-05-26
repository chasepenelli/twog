-- Proof Network proof capsules + reviews.
--
-- A proof_capsule is the serious work-object that a contributor submits
-- against a work_packet (or as a freeform contribution to a candidate). It
-- carries candidate/evidence snapshot hashes, methods, an analysis summary,
-- an artifact manifest, and a content hash. Public submissions never mutate
-- the candidate record; capsules enter a review queue, and only accepted
-- capsules can be attributed in the candidate decision history.

create table if not exists proof_capsules (
  proof_capsule_id uuid primary key,
  work_packet_id uuid,
  candidate_id text not null,
  capsule_type text not null,
  title text not null,
  contributor jsonb not null default '{}'::jsonb,
  task_manifest jsonb not null default '{}'::jsonb,
  candidate_snapshot_hash text,
  evidence_bundle_hash text,
  method_refs jsonb not null default '[]'::jsonb,
  notebook_ref text,
  analysis_summary text not null,
  findings text not null default '',
  output_refs jsonb not null default '[]'::jsonb,
  artifact_manifest jsonb not null default '[]'::jsonb,
  limitations text not null default '',
  conflicts_or_disclosures text not null default '',
  requested_review_route text,
  content_hash text not null,
  signature text,
  status text not null default 'submitted',
  payload jsonb not null,
  submitted_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  reviewed_at timestamptz,
  constraint proof_capsules_capsule_type_check
    check (capsule_type in (
      'citation_repair',
      'claim_critique',
      'evidence_addition',
      'omics_note',
      'docking_replication',
      'md_review',
      'validation_proposal',
      'demotion_case',
      'methods_review',
      'freeform'
    )),
  constraint proof_capsules_status_check
    check (status in (
      'submitted',
      'in_review',
      'needs_changes',
      'accepted',
      'rejected',
      'archived',
      'routed_to_validation',
      'routed_to_compute_review'
    ))
);

create index if not exists proof_capsules_candidate_status_idx
  on proof_capsules(candidate_id, status, submitted_at desc);
create index if not exists proof_capsules_status_submitted_idx
  on proof_capsules(status, submitted_at desc);
create index if not exists proof_capsules_work_packet_idx
  on proof_capsules(work_packet_id, submitted_at desc);
create index if not exists proof_capsules_content_hash_idx
  on proof_capsules(content_hash);

create table if not exists proof_capsule_reviews (
  review_id uuid primary key,
  proof_capsule_id uuid not null,
  reviewer_type text not null,
  reviewer_id text not null,
  verdict text not null,
  confidence double precision not null default 0.5,
  scientific_usefulness double precision,
  provenance_strength double precision,
  reproducibility double precision,
  actionability double precision,
  novelty double precision,
  clarity double precision,
  downstream_impact double precision,
  rationale text not null,
  required_changes text not null default '',
  linked_agent_run_id uuid,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  constraint proof_capsule_reviews_reviewer_type_check
    check (reviewer_type in ('operator', 'llm_evaluator', 'system', 'external_expert')),
  constraint proof_capsule_reviews_verdict_check
    check (verdict in (
      'accepted',
      'needs_changes',
      'rejected',
      'archived',
      'routed_to_validation',
      'routed_to_compute_review'
    ))
);

create index if not exists proof_capsule_reviews_capsule_idx
  on proof_capsule_reviews(proof_capsule_id, created_at desc);
create index if not exists proof_capsule_reviews_verdict_idx
  on proof_capsule_reviews(verdict, created_at desc);
