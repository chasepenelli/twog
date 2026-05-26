-- Workspace checkout/check-in link fields.
--
-- The public contribution intake table is owned by the website path in some
-- environments. Keep this migration additive and tolerant so research bridge
-- deploys do not fail before that table exists.

alter table if exists candidate_contribution_intake
    add column if not exists workspace_id text;

alter table if exists candidate_contribution_intake
    add column if not exists checkout_manifest_hash text;
