-- 0008_tenants_status.sql — proper suspension via status column.
-- Replaces the II.1.c "suspended:<name>" name-prefix hack.

ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'suspended'));

UPDATE tenants
SET name = substring(name FROM length('suspended:') + 1),
    status = 'suspended'
WHERE name LIKE 'suspended:%';
