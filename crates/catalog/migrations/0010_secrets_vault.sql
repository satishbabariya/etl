-- 0010_secrets_vault.sql — add 'vault' to allowed backends.
ALTER TABLE secrets DROP CONSTRAINT IF EXISTS secrets_backend_check;
ALTER TABLE secrets ADD CONSTRAINT secrets_backend_check
    CHECK (backend IN ('env', 'file', 'vault'));
