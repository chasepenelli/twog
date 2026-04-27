-- 005_ingestion_bridge_v2.sql
-- Foundation schema for research-object ingestion, provenance-backed claims,
-- MCP tool calls, lightweight observability, and async validation requests.

create extension if not exists "uuid-ossp";
create extension if not exists pgcrypto;

-- ==================== Source Registry ====================

create table if not exists ingestion_sources (
  id uuid primary key default gen_random_uuid(),
  source_key text not null unique,
  display_name text not null,
  source_kind text not null check (
    source_kind in (
      'scholarly_metadata',
      'open_access_full_text',
      'veterinary_trial',
      'clinical_trial',
      'canine_oncology',
      'omics',
      'chemistry',
      'target_structure',
      'safety',
      'internal'
    )
  ),
  base_url text,
  documentation_url text,
  license_policy text not null default 'metadata_only',
  requires_api_key boolean not null default false,
  enabled boolean not null default true,
  priority integer not null default 100,
  phase integer not null default 1,
  rate_limit_per_minute integer,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists source_capabilities (
  id uuid primary key default gen_random_uuid(),
  source_id uuid not null references ingestion_sources(id) on delete cascade,
  capability text not null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (source_id, capability)
);

create table if not exists source_queries (
  id uuid primary key default gen_random_uuid(),
  source_id uuid not null references ingestion_sources(id) on delete cascade,
  query_name text not null,
  query_text text not null,
  query_params jsonb not null default '{}'::jsonb,
  track text check (track in ('treatment','early_detection','supplements','breed_screening','cross_cutting')),
  object_type text,
  active boolean not null default true,
  created_by text not null default 'system',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (source_id, query_name)
);

create table if not exists source_fetch_runs (
  id uuid primary key default gen_random_uuid(),
  source_id uuid references ingestion_sources(id) on delete set null,
  source_query_id uuid references source_queries(id) on delete set null,
  dagster_run_id text,
  status text not null default 'queued' check (status in ('queued','running','completed','failed','cancelled')),
  started_at timestamptz,
  completed_at timestamptz,
  records_found integer not null default 0,
  records_inserted integer not null default 0,
  records_updated integer not null default 0,
  error_message text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

-- ==================== Raw and Canonical Research Objects ====================

create table if not exists raw_source_records (
  id uuid primary key default gen_random_uuid(),
  source_id uuid not null references ingestion_sources(id) on delete cascade,
  fetch_run_id uuid references source_fetch_runs(id) on delete set null,
  source_record_id text,
  source_url text,
  content_hash text not null,
  raw_payload jsonb not null,
  retrieved_at timestamptz not null default now(),
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  unique (source_id, content_hash)
);

create table if not exists research_objects (
  id uuid primary key default gen_random_uuid(),
  object_type text not null check (
    object_type in (
      'publication',
      'preprint',
      'clinical_trial',
      'veterinary_trial',
      'dataset',
      'compound_record',
      'bioactivity_assay',
      'safety_report',
      'drug_label',
      'structure',
      'validation_run',
      'knowledge_entry'
    )
  ),
  title text,
  abstract text,
  canonical_url text,
  publication_year integer,
  published_at date,
  language text default 'en',
  source_id uuid references ingestion_sources(id) on delete set null,
  raw_record_id uuid references raw_source_records(id) on delete set null,
  dedupe_key text,
  provenance jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists research_objects_dedupe_key_idx
  on research_objects (dedupe_key)
  where dedupe_key is not null;

create table if not exists identifier_links (
  id uuid primary key default gen_random_uuid(),
  research_object_id uuid not null references research_objects(id) on delete cascade,
  identifier_type text not null,
  identifier_value text not null,
  identifier_url text,
  source_id uuid references ingestion_sources(id) on delete set null,
  confidence numeric(5,4) not null default 1.0,
  created_at timestamptz not null default now(),
  unique (identifier_type, identifier_value, research_object_id)
);

create table if not exists object_versions (
  id uuid primary key default gen_random_uuid(),
  research_object_id uuid not null references research_objects(id) on delete cascade,
  raw_record_id uuid references raw_source_records(id) on delete set null,
  version_hash text not null,
  version_payload jsonb not null default '{}'::jsonb,
  observed_at timestamptz not null default now(),
  unique (research_object_id, version_hash)
);

-- ==================== Artifacts and Chunks ====================

create table if not exists artifacts (
  id uuid primary key default gen_random_uuid(),
  research_object_id uuid references research_objects(id) on delete cascade,
  artifact_type text not null check (
    artifact_type in ('metadata','abstract','full_text_xml','full_text_html','pdf','supplement','structure','molecule','report','run_output','scrape_snapshot','scrape_manifest')
  ),
  uri text not null,
  storage_backend text not null default 'external',
  content_hash text,
  mime_type text,
  license text,
  legal_status text not null default 'unknown' check (
    legal_status in ('metadata_only','open_access_store_allowed','link_only','link_and_registry_metadata','restricted','unknown')
  ),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists scrape_review_records (
  id uuid primary key default gen_random_uuid(),
  artifact_id uuid references artifacts(id) on delete cascade,
  source_id uuid references ingestion_sources(id) on delete cascade,
  source_record_id text not null,
  title text,
  canonical_url text,
  record_type text,
  fields jsonb not null default '{}'::jsonb,
  parser_confidence numeric(5,4) not null default 0,
  review_status text not null default 'needs_review' check (
    review_status in ('needs_review','accepted','rejected')
  ),
  reviewer text,
  review_note text,
  parsed_at timestamptz not null default now(),
  reviewed_at timestamptz,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (artifact_id, source_record_id)
);

create index if not exists scrape_review_records_status_idx
  on scrape_review_records (source_id, review_status, updated_at desc);

create table if not exists scrape_source_profile_reviews (
  id uuid primary key default gen_random_uuid(),
  source_id uuid references ingestion_sources(id) on delete cascade,
  source_key text not null unique,
  robots_policy text not null check (
    robots_policy in ('unknown','reviewed','disallow','manual_only')
  ),
  approved_for_fetch boolean not null default false,
  reviewed_by text not null,
  review_note text,
  allowed_url_patterns jsonb not null default '[]'::jsonb,
  storage_policy text,
  reviewed_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists document_chunks (
  id uuid primary key default gen_random_uuid(),
  research_object_id uuid not null references research_objects(id) on delete cascade,
  artifact_id uuid references artifacts(id) on delete set null,
  chunk_index integer not null,
  section_label text,
  text_content text not null,
  content_hash text not null,
  token_count integer,
  char_start integer,
  char_end integer,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (research_object_id, chunk_index)
);

-- ==================== Entity Layer ====================

create table if not exists entities (
  id uuid primary key default gen_random_uuid(),
  entity_type text not null check (
    entity_type in (
      'compound',
      'target',
      'gene',
      'protein',
      'pathway',
      'disease',
      'species',
      'breed',
      'assay',
      'cell_line',
      'drug_class',
      'supplement',
      'mechanism',
      'outcome',
      'structure',
      'dataset'
    )
  ),
  canonical_name text not null,
  normalized_key text not null,
  external_ids jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (entity_type, normalized_key)
);

create table if not exists entity_aliases (
  id uuid primary key default gen_random_uuid(),
  entity_id uuid not null references entities(id) on delete cascade,
  alias text not null,
  source text not null default 'system',
  confidence numeric(5,4) not null default 1.0,
  created_at timestamptz not null default now(),
  unique (entity_id, lower(alias))
);

create table if not exists entity_mentions (
  id uuid primary key default gen_random_uuid(),
  chunk_id uuid references document_chunks(id) on delete cascade,
  research_object_id uuid not null references research_objects(id) on delete cascade,
  entity_id uuid references entities(id) on delete set null,
  mention_text text not null,
  entity_type text,
  char_start integer,
  char_end integer,
  normalized_candidate text,
  extractor_name text not null default 'system',
  extractor_version text,
  confidence numeric(5,4),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

-- ==================== Claim and Tag Layer ====================

create table if not exists claims (
  id uuid primary key default gen_random_uuid(),
  claim_type text not null check (
    claim_type in (
      'target_associated_with_disease',
      'compound_modulates_target',
      'compound_affects_outcome',
      'pathway_active_in_disease',
      'biomarker_predicts_state',
      'safety_signal',
      'species_translation',
      'validation_result',
      'other'
    )
  ),
  statement text not null,
  direction text not null default 'unknown' check (
    direction in ('positive','negative','neutral','mixed','unknown')
  ),
  species text,
  evidence_level text check (
    evidence_level in ('in_silico','in_vitro','ex_vivo','animal_model','canine_clinical','human_clinical','review','unknown')
  ),
  confidence numeric(5,4) not null default 0.0,
  extraction_status text not null default 'draft' check (
    extraction_status in ('draft','needs_review','accepted','rejected','superseded')
  ),
  extractor_name text not null default 'system',
  extractor_version text,
  source_research_object_id uuid references research_objects(id) on delete set null,
  source_chunk_id uuid references document_chunks(id) on delete set null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists claim_evidence (
  id uuid primary key default gen_random_uuid(),
  claim_id uuid not null references claims(id) on delete cascade,
  research_object_id uuid references research_objects(id) on delete set null,
  chunk_id uuid references document_chunks(id) on delete set null,
  evidence_quote text,
  evidence_summary text,
  support_direction text not null default 'supports' check (
    support_direction in ('supports','contradicts','context','unclear')
  ),
  confidence numeric(5,4) not null default 0.0,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists claim_entities (
  claim_id uuid not null references claims(id) on delete cascade,
  entity_id uuid not null references entities(id) on delete cascade,
  role text not null,
  created_at timestamptz not null default now(),
  primary key (claim_id, entity_id, role)
);

create table if not exists claim_conflicts (
  id uuid primary key default gen_random_uuid(),
  claim_id uuid not null references claims(id) on delete cascade,
  conflicting_claim_id uuid not null references claims(id) on delete cascade,
  conflict_type text not null default 'direction',
  notes text,
  created_at timestamptz not null default now(),
  unique (claim_id, conflicting_claim_id)
);

create table if not exists tag_sets (
  id uuid primary key default gen_random_uuid(),
  tag_set_key text not null unique,
  display_name text not null,
  description text,
  created_at timestamptz not null default now()
);

create table if not exists tags (
  id uuid primary key default gen_random_uuid(),
  tag_set_id uuid references tag_sets(id) on delete cascade,
  tag_key text not null,
  display_name text not null,
  description text,
  created_at timestamptz not null default now(),
  unique (tag_set_id, tag_key)
);

create table if not exists tag_assignments (
  id uuid primary key default gen_random_uuid(),
  tag_id uuid not null references tags(id) on delete cascade,
  object_type text not null check (
    object_type in ('research_object','chunk','entity','claim','candidate','hypothesis','artifact','run')
  ),
  object_id uuid not null,
  assigned_by text not null,
  assignment_method text not null default 'system',
  confidence numeric(5,4),
  evidence_claim_id uuid references claims(id) on delete set null,
  source_chunk_id uuid references document_chunks(id) on delete set null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

-- ==================== MCP and Agent Observability ====================

create table if not exists mcp_tool_calls (
  id uuid primary key default gen_random_uuid(),
  tool_name text not null,
  mode text not null default 'read' check (mode in ('read','draft','write','async_compute')),
  input_hash text,
  sanitized_input jsonb not null default '{}'::jsonb,
  output_summary text,
  structured_output jsonb not null default '{}'::jsonb,
  status text not null default 'queued' check (status in ('queued','running','completed','failed','cancelled')),
  latency_ms integer,
  error_message text,
  created_by text,
  created_at timestamptz not null default now(),
  completed_at timestamptz
);

create table if not exists model_calls (
  id uuid primary key default gen_random_uuid(),
  mcp_tool_call_id uuid references mcp_tool_calls(id) on delete set null,
  agent_run_id uuid,
  model_profile text,
  model_name text,
  prompt_version text,
  input_hash text,
  output_hash text,
  token_input integer,
  token_output integer,
  latency_ms integer,
  status text not null default 'completed' check (status in ('completed','failed','cancelled')),
  error_message text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists agent_runs (
  id uuid primary key default gen_random_uuid(),
  agent_name text not null,
  agent_version text,
  objective text,
  status text not null default 'queued' check (status in ('queued','running','completed','failed','cancelled','needs_approval')),
  dagster_run_id text,
  mcp_tool_call_id uuid references mcp_tool_calls(id) on delete set null,
  input_hash text,
  output_summary text,
  metadata jsonb not null default '{}'::jsonb,
  created_by text,
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz
);

create table if not exists approval_events (
  id uuid primary key default gen_random_uuid(),
  object_type text not null,
  object_id uuid not null,
  requested_by text,
  reviewed_by text,
  status text not null default 'pending' check (status in ('pending','approved','rejected','expired','cancelled')),
  reason text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  reviewed_at timestamptz
);

create table if not exists async_runs (
  id uuid primary key default gen_random_uuid(),
  run_kind text not null check (
    run_kind in ('dagster','runpod','mcp','local','external')
  ),
  run_name text not null,
  external_run_id text,
  status text not null default 'queued' check (status in ('queued','running','completed','failed','cancelled','needs_approval')),
  requested_by text,
  mcp_tool_call_id uuid references mcp_tool_calls(id) on delete set null,
  dagster_run_id text,
  runpod_job_id text,
  cost_estimate_usd numeric(10,4),
  input_payload jsonb not null default '{}'::jsonb,
  output_payload jsonb not null default '{}'::jsonb,
  artifact_ids uuid[] not null default '{}',
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz
);

-- ==================== Hypotheses and Candidate Links ====================

create table if not exists hypotheses (
  id uuid primary key default gen_random_uuid(),
  title text not null,
  hypothesis text not null,
  rationale text,
  status text not null default 'draft' check (
    status in ('draft','proposed','approved','rejected','testing','validated','invalidated','archived')
  ),
  proposed_by text not null default 'system',
  confidence numeric(5,4),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists hypothesis_claims (
  hypothesis_id uuid not null references hypotheses(id) on delete cascade,
  claim_id uuid not null references claims(id) on delete cascade,
  role text not null default 'supporting',
  created_at timestamptz not null default now(),
  primary key (hypothesis_id, claim_id, role)
);

-- ==================== Indexes and Views ====================

create index if not exists ingestion_sources_kind_idx on ingestion_sources (source_kind);
create index if not exists ingestion_sources_enabled_idx on ingestion_sources (enabled, priority);
create index if not exists source_fetch_runs_source_idx on source_fetch_runs (source_id, created_at desc);
create index if not exists raw_source_records_source_idx on raw_source_records (source_id, retrieved_at desc);
create index if not exists raw_source_records_source_record_idx on raw_source_records (source_id, source_record_id);
create index if not exists research_objects_type_idx on research_objects (object_type, updated_at desc);
create index if not exists research_objects_source_idx on research_objects (source_id);
create index if not exists research_objects_title_idx on research_objects using gin (to_tsvector('english', coalesce(title,'') || ' ' || coalesce(abstract,'')));
create index if not exists identifier_links_value_idx on identifier_links (identifier_type, identifier_value);
create index if not exists artifacts_object_idx on artifacts (research_object_id, artifact_type);
create index if not exists document_chunks_object_idx on document_chunks (research_object_id, chunk_index);
create index if not exists document_chunks_hash_idx on document_chunks (content_hash);
create index if not exists entities_type_key_idx on entities (entity_type, normalized_key);
create index if not exists entity_mentions_object_idx on entity_mentions (research_object_id);
create index if not exists entity_mentions_entity_idx on entity_mentions (entity_id);
create index if not exists claims_type_idx on claims (claim_type);
create index if not exists claims_confidence_idx on claims (confidence desc);
create index if not exists claims_status_idx on claims (extraction_status);
create index if not exists claims_species_idx on claims (species);
create index if not exists claims_statement_idx on claims using gin (to_tsvector('english', statement));
create index if not exists claim_evidence_claim_idx on claim_evidence (claim_id);
create index if not exists tag_assignments_object_idx on tag_assignments (object_type, object_id);
create index if not exists mcp_tool_calls_tool_idx on mcp_tool_calls (tool_name, created_at desc);
create index if not exists agent_runs_name_idx on agent_runs (agent_name, created_at desc);
create index if not exists async_runs_status_idx on async_runs (status, created_at desc);
create index if not exists hypotheses_status_idx on hypotheses (status, created_at desc);

create or replace view view_claim_search as
select
  c.id,
  c.claim_type,
  c.statement,
  c.direction,
  c.species,
  c.evidence_level,
  c.confidence,
  c.extraction_status,
  ro.title as source_title,
  ro.canonical_url as source_url,
  c.created_at,
  c.updated_at
from claims c
left join research_objects ro on ro.id = c.source_research_object_id
where c.extraction_status in ('accepted','needs_review','draft');

create or replace view view_ingestion_coverage as
select
  s.source_key,
  s.display_name,
  s.source_kind,
  count(distinct r.id) as raw_record_count,
  count(distinct ro.id) as research_object_count,
  max(r.retrieved_at) as last_retrieved_at
from ingestion_sources s
left join raw_source_records r on r.source_id = s.id
left join research_objects ro on ro.source_id = s.id
group by s.source_key, s.display_name, s.source_kind;

comment on table ingestion_sources is 'Registry of data sources and their licensing/API properties.';
comment on table raw_source_records is 'Immutable-ish source payloads with stable hashes for provenance and replay.';
comment on table research_objects is 'Canonical objects such as publications, trials, datasets, compounds, safety reports, structures, and validation runs.';
comment on table claims is 'Structured scientific assertions extracted from research objects with provenance.';
comment on table tag_assignments is 'Provenance-backed tags over claims, entities, objects, candidates, hypotheses, artifacts, and runs.';
comment on table mcp_tool_calls is 'Lightweight MCP observability log for tool calls and structured outputs.';
comment on table async_runs is 'Async handles for Dagster, RunPod, MCP, local, and external compute jobs.';
