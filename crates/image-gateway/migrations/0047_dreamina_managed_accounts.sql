-- Dreamina OAuth material is stored by the official CLI through the OS keyring.
-- New managed accounts are provisioned only after the control plane creates a
-- private per-account keyring namespace, so their durable credential identity
-- can use the same auth-file revision contract as the other managed CLIs.
CREATE OR REPLACE FUNCTION initialize_provider_account_credential_head() RETURNS TRIGGER AS $$
DECLARE
    strategy_value TEXT;
BEGIN
    strategy_value := CASE
        WHEN NEW.provider_id IN ('openai-codex', 'grok-cli') THEN 'broker_managed'
        WHEN NEW.provider_id = 'dreamina-cli' THEN 'cli_managed'
        ELSE 'reauth_only'
    END;
    INSERT INTO provider_account_credential_revisions (
        provider_account_id, revision, material_kind, material_fingerprint_sha256,
        access_expires_at_ms, created_at_ms
    ) VALUES (
        NEW.provider_account_id, 1, 'auth_file',
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

-- Accounts created before isolated keyrings existed must be explicitly
-- reauthorized; their append-only system_keyring revision is not promoted.
UPDATE provider_account_credential_heads head
SET lifecycle_state = 'reauth_required', refresh_strategy = 'cli_managed',
    next_refresh_at_ms = NULL,
    updated_at_ms = GREATEST(head.updated_at_ms, head.created_at_ms),
    control_version = control_version + 1
FROM provider_accounts account
WHERE account.provider_account_id = head.provider_account_id
  AND account.provider_id = 'dreamina-cli'
  AND head.lifecycle_state = 'unsupported';
