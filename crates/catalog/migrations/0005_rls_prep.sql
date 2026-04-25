-- 0005_rls_prep.sql — denormalize tenant_id onto every tenant-scoped table.
-- RLS policies cannot reach through FKs, so each table needs the column.

ALTER TABLE cdc_slots
  ADD COLUMN IF NOT EXISTS tenant_id UUID;

UPDATE cdc_slots cs
SET tenant_id = p.tenant_id
FROM pipelines p
WHERE p.pipeline_id = cs.pipeline_id
  AND cs.tenant_id IS NULL;

ALTER TABLE cdc_slots
  ALTER COLUMN tenant_id SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'cdc_slots_tenant_fk'
  ) THEN
    ALTER TABLE cdc_slots
      ADD CONSTRAINT cdc_slots_tenant_fk
        FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE;
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS cdc_slots_tenant_id_idx ON cdc_slots(tenant_id);

ALTER TABLE stream_state
  ADD COLUMN IF NOT EXISTS tenant_id UUID;

UPDATE stream_state ss
SET tenant_id = p.tenant_id
FROM pipelines p
WHERE p.pipeline_id = ss.pipeline_id
  AND ss.tenant_id IS NULL;

ALTER TABLE stream_state
  ALTER COLUMN tenant_id SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'stream_state_tenant_fk'
  ) THEN
    ALTER TABLE stream_state
      ADD CONSTRAINT stream_state_tenant_fk
        FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE;
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS stream_state_tenant_id_idx ON stream_state(tenant_id);
