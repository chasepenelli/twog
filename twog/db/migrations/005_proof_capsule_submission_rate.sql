-- Per-handle Proof Capsule submission rate tracking (public-site mirror).
--
-- See db/migrations/010_proof_capsule_submission_rate.sql for the
-- semantics. The submit path on Next.js reads/writes this table.

create table if not exists proof_capsule_submission_rate (
  handle text not null,
  window_start timestamptz not null,
  submission_count integer not null default 0,
  primary key (handle, window_start),
  constraint proof_capsule_submission_rate_count_check check (submission_count >= 0)
);

create index if not exists proof_capsule_submission_rate_handle_window_idx
  on proof_capsule_submission_rate (handle, window_start desc);
create index if not exists proof_capsule_submission_rate_window_idx
  on proof_capsule_submission_rate (window_start);
