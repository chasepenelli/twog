alter table candidate_contribution_intake
  add column if not exists contribution_content_hash text;

update candidate_contribution_intake
  set contribution_content_hash = concat('legacy-md5:', md5(packet::text))
  where contribution_content_hash is null;

alter table candidate_contribution_intake
  drop constraint if exists candidate_contribution_intake_type_check;

alter table candidate_contribution_intake
  add constraint candidate_contribution_intake_type_check
  check (contribution_type in (
    'evidence_addition',
    'citation_repair',
    'claim_critique',
    'replication_result',
    'compute_artifact',
    'omics_note',
    'validation_proposal',
    'safety_or_translation_note',
    'candidate_demotion_case',
    'evidence',
    'critique',
    'replication',
    'artifact',
    'compute_result'
  ));

create index if not exists candidate_contribution_intake_hash_idx
  on candidate_contribution_intake (contribution_content_hash);
