CREATE TABLE provider_account_credential_revisions (
    provider_account_id UUID NOT NULL REFERENCES provider_accounts(provider_account_id)
        ON DELETE RESTRICT,
    revision BIGINT NOT NULL CHECK (revision > 0),
    material_kind TEXT NOT NULL CHECK (
        material_kind IN ('auth_file', 'system_keyring')
    ),
    material_fingerprint_sha256 TEXT NOT NULL CHECK (
        material_fingerprint_sha256 ~ '^[0-9a-f]{64}$'
    ),
    access_expires_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (provider_account_id, revision)
);

CREATE TABLE provider_account_credential_heads (
    provider_account_id UUID PRIMARY KEY REFERENCES provider_accounts(provider_account_id)
        ON DELETE RESTRICT,
    active_revision BIGINT NOT NULL CHECK (active_revision > 0),
    lifecycle_state TEXT NOT NULL CHECK (
        lifecycle_state IN (
            'active', 'refresh_due', 'refreshing', 'reauth_required', 'unsupported'
        )
    ),
    refresh_strategy TEXT NOT NULL CHECK (
        refresh_strategy IN ('broker_managed', 'cli_managed', 'reauth_only')
    ),
    refresh_after_ms BIGINT,
    next_refresh_at_ms BIGINT,
    last_attempt_at_ms BIGINT,
    last_success_at_ms BIGINT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    last_error_code TEXT CHECK (
        last_error_code IS NULL OR (
            char_length(last_error_code) BETWEEN 1 AND 128
            AND last_error_code ~ '^[A-Za-z0-9_.-]+$'
        )
    ),
    lease_owner TEXT CHECK (
        lease_owner IS NULL OR (
            char_length(lease_owner) BETWEEN 1 AND 128
            AND lease_owner !~ '[[:cntrl:]]'
        )
    ),
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at_ms BIGINT,
    control_version BIGINT NOT NULL DEFAULT 1 CHECK (control_version > 0),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (provider_account_id, active_revision)
        REFERENCES provider_account_credential_revisions(provider_account_id, revision)
        ON DELETE RESTRICT,
    CHECK (
        (lease_owner IS NULL AND lease_expires_at_ms IS NULL)
        OR
        (lease_owner IS NOT NULL AND lease_epoch > 0 AND lease_expires_at_ms IS NOT NULL)
    ),
    CHECK (refresh_after_ms IS NULL OR refresh_after_ms >= 0),
    CHECK (next_refresh_at_ms IS NULL OR next_refresh_at_ms >= 0),
    CHECK (last_attempt_at_ms IS NULL OR last_attempt_at_ms >= 0),
    CHECK (last_success_at_ms IS NULL OR last_success_at_ms >= 0)
);

CREATE INDEX provider_account_credential_heads_due_idx
    ON provider_account_credential_heads (next_refresh_at_ms, provider_account_id)
    WHERE refresh_strategy = 'broker_managed'
      AND lifecycle_state IN ('active', 'refresh_due');

CREATE TABLE provider_account_credential_events (
    credential_event_id UUID PRIMARY KEY,
    provider_account_id UUID NOT NULL REFERENCES provider_accounts(provider_account_id)
        ON DELETE RESTRICT,
    event_type TEXT NOT NULL CHECK (
        event_type IN (
            'refresh_claimed', 'refresh_succeeded', 'refresh_failed',
            'reauth_required', 'credential_resolved'
        )
    ),
    from_revision BIGINT CHECK (from_revision IS NULL OR from_revision > 0),
    to_revision BIGINT CHECK (to_revision IS NULL OR to_revision > 0),
    lease_epoch BIGINT CHECK (lease_epoch IS NULL OR lease_epoch > 0),
    executor_execution_id UUID REFERENCES executor_executions(executor_execution_id)
        ON DELETE RESTRICT,
    error_code TEXT CHECK (
        error_code IS NULL OR (
            char_length(error_code) BETWEEN 1 AND 128
            AND error_code ~ '^[A-Za-z0-9_.-]+$'
        )
    ),
    created_at_ms BIGINT NOT NULL
);

CREATE INDEX provider_account_credential_events_account_idx
    ON provider_account_credential_events (provider_account_id, created_at_ms DESC);

CREATE FUNCTION reject_provider_credential_ledger_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'provider credential ledger rows are append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_account_credential_revisions_append_only
    BEFORE UPDATE OR DELETE ON provider_account_credential_revisions
    FOR EACH ROW EXECUTE FUNCTION reject_provider_credential_ledger_mutation();

CREATE TRIGGER provider_account_credential_events_append_only
    BEFORE UPDATE OR DELETE ON provider_account_credential_events
    FOR EACH ROW EXECUTE FUNCTION reject_provider_credential_ledger_mutation();

CREATE FUNCTION initialize_provider_account_credential_head() RETURNS TRIGGER AS $$
DECLARE
    material_kind_value TEXT;
    lifecycle_value TEXT;
    strategy_value TEXT;
BEGIN
    material_kind_value := CASE
        WHEN NEW.provider_id = 'dreamina-cli' THEN 'system_keyring'
        ELSE 'auth_file'
    END;
    lifecycle_value := CASE
        WHEN NEW.provider_id = 'dreamina-cli' THEN 'unsupported'
        ELSE 'active'
    END;
    strategy_value := CASE
        WHEN NEW.provider_id IN ('openai-codex', 'grok-cli') THEN 'broker_managed'
        WHEN NEW.provider_id = 'dreamina-cli' THEN 'cli_managed'
        ELSE 'reauth_only'
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
        NEW.provider_account_id, 1, lifecycle_value, strategy_value,
        NULL, NEW.created_at_ms, NULL, NULL, 0, NULL, NULL, 0, NULL, 1,
        NEW.created_at_ms, NEW.updated_at_ms
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_accounts_initialize_credential_head
    AFTER INSERT ON provider_accounts
    FOR EACH ROW EXECUTE FUNCTION initialize_provider_account_credential_head();

INSERT INTO provider_account_credential_revisions (
    provider_account_id, revision, material_kind, material_fingerprint_sha256,
    access_expires_at_ms, created_at_ms
)
SELECT provider_account_id, 1,
       CASE WHEN provider_id = 'dreamina-cli' THEN 'system_keyring' ELSE 'auth_file' END,
       credential_auth_sha256, NULL, created_at_ms
FROM provider_accounts;

INSERT INTO provider_account_credential_heads (
    provider_account_id, active_revision, lifecycle_state, refresh_strategy,
    refresh_after_ms, next_refresh_at_ms, last_attempt_at_ms, last_success_at_ms,
    consecutive_failures, last_error_code, lease_owner, lease_epoch,
    lease_expires_at_ms, control_version, created_at_ms, updated_at_ms
)
SELECT provider_account_id, 1,
       CASE WHEN provider_id = 'dreamina-cli' THEN 'unsupported' ELSE 'active' END,
       CASE
           WHEN provider_id IN ('openai-codex', 'grok-cli') THEN 'broker_managed'
           WHEN provider_id = 'dreamina-cli' THEN 'cli_managed'
           ELSE 'reauth_only'
       END,
       NULL, created_at_ms, NULL, NULL, 0, NULL, NULL, 0, NULL, 1,
       created_at_ms, updated_at_ms
FROM provider_accounts;
