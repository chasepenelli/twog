-- 006_command_center_board.sql
-- Durable Command Center board placement and activity stream records.

create table if not exists command_center_board_stages (
  entity_type text not null,
  entity_id text not null,
  board_stage text not null,
  actor text not null,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (entity_type, entity_id)
);

create index if not exists command_center_board_stages_stage_idx
  on command_center_board_stages(board_stage, updated_at desc);

create table if not exists command_center_activity_events (
  event_id text primary key,
  occurred_at timestamptz not null,
  actor text not null,
  source text not null,
  event_type text not null,
  entity_type text not null,
  entity_id text not null,
  severity text not null,
  correlation_id text,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

create index if not exists command_center_activity_events_time_idx
  on command_center_activity_events(occurred_at desc);
create index if not exists command_center_activity_events_entity_idx
  on command_center_activity_events(entity_type, entity_id, occurred_at desc);
create index if not exists command_center_activity_events_source_idx
  on command_center_activity_events(source, occurred_at desc);
