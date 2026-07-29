-- Account capabilities are an explicit control-plane choice. The shared
-- provisioning transaction keeps this table aligned with every enabled profile.
CREATE TABLE provider_account_operations (
    provider_account_id UUID NOT NULL,
    provider_id TEXT NOT NULL,
    operation_id TEXT NOT NULL CHECK (
        operation_id IN ('images.generations', 'videos.generations')
    ),
    state TEXT NOT NULL CHECK (state IN ('enabled', 'disabled')),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (provider_account_id, operation_id),
    FOREIGN KEY (provider_account_id, provider_id)
        REFERENCES provider_accounts(provider_account_id, provider_id)
        ON DELETE RESTRICT
);

CREATE INDEX provider_account_operations_provider_lookup_idx
    ON provider_account_operations(provider_id, operation_id, provider_account_id)
    WHERE state = 'enabled';

-- Preserve the effective capabilities of accounts provisioned before this
-- migration. New accounts write the requested capability set transactionally.
INSERT INTO provider_account_operations
  (provider_account_id, provider_id, operation_id, state, created_at_ms, updated_at_ms)
SELECT profile.provider_account_id, profile.provider_id, profile.operation_id,
       'enabled', MIN(profile.created_at_ms), MAX(profile.updated_at_ms)
FROM provider_execution_profiles profile
WHERE profile.state = 'enabled'
  AND profile.operation_id IN ('images.generations', 'videos.generations')
GROUP BY profile.provider_account_id, profile.provider_id, profile.operation_id;

-- Keep rolling upgrades safe when an older process creates a profile after this
-- migration but before every writer has adopted the shared provisioning kernel.
CREATE FUNCTION sync_provider_account_operation_from_profile()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.state = 'enabled'
       AND NEW.operation_id IN ('images.generations', 'videos.generations') THEN
        INSERT INTO provider_account_operations
          (provider_account_id, provider_id, operation_id, state, created_at_ms, updated_at_ms)
        VALUES
          (NEW.provider_account_id, NEW.provider_id, NEW.operation_id,
           'enabled', NEW.created_at_ms, NEW.updated_at_ms)
        ON CONFLICT (provider_account_id, operation_id) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER provider_execution_profiles_account_operation_sync
AFTER INSERT OR UPDATE OF state, operation_id ON provider_execution_profiles
FOR EACH ROW
EXECUTE FUNCTION sync_provider_account_operation_from_profile();
