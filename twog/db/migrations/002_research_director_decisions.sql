-- Research Director decisions: grounded, re-derivable choices about what the engine should test next.
-- Each row is one decision from a director reasoning pass over the REAL proof ledger. Every decision
-- must cite real capsule_ids (cited_capsule_ids) — the grounding is verifiable, not asserted. The engine
-- never acts on these automatically; they are a prioritized, transparent recommendation surface.

create table if not exists research_director_decisions (
  decision_id uuid primary key default gen_random_uuid(),
  run_tag text not null,                         -- groups one director pass
  candidate_id text,                             -- the idea this decision is about
  title text,                                    -- human-readable candidate/idea title
  target text,                                   -- e.g. KDR / PIK3CA
  action_kind text not null,                     -- propose_frontier | deprioritize | next_test | escalate | needs_input
  modality text,                                 -- frontier modality for propose_frontier (stapled_peptide, peptide_protac, degrader, molecular_glue, base_edit, prime_edit, car_t, mrna_vaccine, adc, covalent, allele_specific, synthetic_lethality)
  testable_now boolean,                          -- true ONLY for a genuine small-molecule pocket binder (real tool compound) vs a redock-verified target; degraders/peptides/glues/biologics/gene-edits are NOT dockable by gnina and are proposals until designed
  decision text not null,                        -- the recommended action, in plain language
  rationale text not null,                       -- grounded reasoning (cites the evidence)
  confidence double precision,                   -- 0..1 (never certainty)
  priority integer,                              -- rank within the pass (1 = highest)
  current_verdict text,                          -- still-standing | ruled-out | needs-more (of the grounding target/candidate)
  next_lane text,                                -- suggested next lane (docking/cofolding/md/omics) when testable now
  est_cost_usd double precision,                 -- estimated cost of the recommended next test
  cited_capsule_ids jsonb not null default '[]'::jsonb,  -- real capsule_ids grounding this decision
  model_profile text,                            -- which model reasoned
  created_at timestamptz not null default now(),
  constraint research_director_decisions_action_kind_check
    check (action_kind in ('propose_frontier', 'deprioritize', 'next_test', 'escalate', 'needs_input'))
);

create index if not exists research_director_decisions_run_priority_idx
  on research_director_decisions (run_tag, priority asc);
create index if not exists research_director_decisions_candidate_idx
  on research_director_decisions (candidate_id, created_at desc);
create index if not exists research_director_decisions_created_idx
  on research_director_decisions (created_at desc);
