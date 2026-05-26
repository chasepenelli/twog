-- Per-handle Proof Capsule submission rate tracking.
--
-- Hour-bucketed sliding window. Each row records the count of capsule
-- submissions a handle made within the corresponding hour. The submit
-- path sums counts across the trailing window and rejects 429 if the
-- total exceeds the configured per-hour limit. Idempotent reruns are
-- safe (ON CONFLICT DO ... UPDATE), and old rows can be GC'd by an
-- operator job once they fall outside the window.

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
