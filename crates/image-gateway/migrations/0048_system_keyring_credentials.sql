-- Credential resolution is material-type aware. Dreamina stores OAuth material
-- in its isolated per-account OS keyring, so new rows must describe that
-- storage truth instead of reusing the auth-file label.
CREATE OR REPLACE FUNCTION initialize_provider_account_credential_head() RETURNS TRIGGER AS $$
DECLARE
    strategy_value TEXT;
    material_kind_value TEXT;
BEGIN
    strategy_value := CASE
        WHEN NEW.provider_id IN ('openai-codex', 'grok-cli') THEN 'broker_managed'
        WHEN NEW.provider_id = 'dreamina-cli' THEN 'cli_managed'
        ELSE 'reauth_only'
    END;
    material_kind_value := CASE
        WHEN NEW.provider_id = 'dreamina-cli' THEN 'system_keyring'
        ELSE 'auth_file'
    END;
    INSERT INTO provider_account_credential_revisions (
        provider_account_id, revision, material_kind, material_fingerprint_sha256,
        access_expires_at_ms, created_at_ms
    ) VALUES (
        NEW.provider_account_id, 1, material_kind_value,
        NEW.credential_auth_sha256, NULL, NEW.created_at_ms
    );
    INSERT INTO provider_account_credential_heads (
        provider_account_id, active_revision, lifecycle_state, refresh_strategy,
        refresh_after_ms, next_refresh_at_ms, last_attempt_at_ms, last_success_at_ms,
        consecutive_failures, last_error_code, lease_owner, lease_epoch,
        lease_expires_at_ms, control_version, created_at_ms, updated_at_ms
    ) VALUES (
        NEW.provider_account_id, 1, 'active', strategy_value,
        NULL, NEW.created_at_ms, NULL, NULL, 0, NULL, NULL, 0, NULL, 1,
        NEW.created_at_ms, NEW.updated_at_ms
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
