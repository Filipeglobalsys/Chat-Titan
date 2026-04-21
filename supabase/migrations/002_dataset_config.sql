-- Add sync_status and persistent dataset config (RLS credentials, gateway settings)
alter table datasets add column if not exists sync_status text;
alter table datasets add column if not exists dataset_config jsonb;
