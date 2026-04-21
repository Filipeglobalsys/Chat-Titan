-- Power BI Copilot - Schema inicial

create extension if not exists "uuid-ossp";

-- Workspaces (grupos do Power BI)
create table if not exists workspaces (
  id          text primary key,
  name        text not null,
  type        text,
  is_read_only boolean default false,
  synced_at   timestamptz default now()
);

-- Datasets
create table if not exists datasets (
  id              text primary key,
  workspace_id    text references workspaces(id) on delete cascade,
  name            text not null,
  configured_by   text,
  is_refreshable  boolean default false,
  created_date    timestamptz,
  synced_at       timestamptz default now()
);

create index if not exists datasets_workspace_idx on datasets(workspace_id);

-- Tables (tabelas dentro do dataset)
create table if not exists tables (
  id          uuid primary key default uuid_generate_v4(),
  dataset_id  text references datasets(id) on delete cascade,
  name        text not null,
  synced_at   timestamptz default now(),
  unique(dataset_id, name)
);

create index if not exists tables_dataset_idx on tables(dataset_id);

-- Columns
create table if not exists columns (
  id          uuid primary key default uuid_generate_v4(),
  table_id    uuid references tables(id) on delete cascade,
  name        text not null,
  data_type   text,
  column_type text,
  synced_at   timestamptz default now(),
  unique(table_id, name)
);

create index if not exists columns_table_idx on columns(table_id);

-- Measures
create table if not exists measures (
  id          uuid primary key default uuid_generate_v4(),
  table_id    uuid references tables(id) on delete cascade,
  name        text not null,
  expression  text,
  description text,
  synced_at   timestamptz default now(),
  unique(table_id, name)
);

create index if not exists measures_table_idx on measures(table_id);

-- Row Level Security (RLS) - opcional, ativar conforme necessário
-- alter table workspaces enable row level security;
-- alter table datasets enable row level security;
-- alter table tables enable row level security;
-- alter table columns enable row level security;
-- alter table measures enable row level security;
