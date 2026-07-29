CREATE TABLE identity_users (
    user_id UUID PRIMARY KEY,
    normalized_email TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    roles TEXT[] NOT NULL,
    scopes TEXT[] NOT NULL,
    authz_version BIGINT NOT NULL DEFAULT 1,
    disabled_at_ms BIGINT,
    failed_login_count INTEGER NOT NULL DEFAULT 0,
    locked_until_ms BIGINT,
    last_login_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    CONSTRAINT identity_users_email_check CHECK (
        normalized_email = lower(normalized_email)
        AND char_length(normalized_email) BETWEEN 3 AND 254
    ),
    CONSTRAINT identity_users_display_name_check CHECK (
        char_length(display_name) BETWEEN 1 AND 128
    ),
    CONSTRAINT identity_users_roles_check CHECK (cardinality(roles) > 0),
    CONSTRAINT identity_users_scopes_check CHECK (cardinality(scopes) > 0),
    CONSTRAINT identity_users_authz_version_check CHECK (authz_version > 0),
    CONSTRAINT identity_users_failed_login_count_check CHECK (failed_login_count >= 0),
    CONSTRAINT identity_users_time_order_check CHECK (updated_at_ms >= created_at_ms)
);

CREATE TABLE identity_password_credentials (
    user_id UUID PRIMARY KEY REFERENCES identity_users(user_id) ON DELETE CASCADE,
    password_hash TEXT NOT NULL,
    password_version INTEGER NOT NULL DEFAULT 1,
    changed_at_ms BIGINT NOT NULL,
    CONSTRAINT identity_password_hash_check CHECK (
        password_hash LIKE '$argon2id$%'
        AND char_length(password_hash) BETWEEN 64 AND 1024
    ),
    CONSTRAINT identity_password_version_check CHECK (password_version > 0)
);

CREATE TABLE identity_session_families (
    session_id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES identity_users(user_id) ON DELETE CASCADE,
    client_id TEXT NOT NULL,
    authz_version_at_login BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    last_seen_at_ms BIGINT NOT NULL,
    idle_expires_at_ms BIGINT NOT NULL,
    absolute_expires_at_ms BIGINT NOT NULL,
    revoked_at_ms BIGINT,
    revoke_reason TEXT,
    CONSTRAINT identity_session_client_check CHECK (char_length(client_id) BETWEEN 1 AND 128),
    CONSTRAINT identity_session_authz_version_check CHECK (authz_version_at_login > 0),
    CONSTRAINT identity_session_time_order_check CHECK (
        last_seen_at_ms >= created_at_ms
        AND idle_expires_at_ms > created_at_ms
        AND absolute_expires_at_ms >= idle_expires_at_ms
    ),
    CONSTRAINT identity_session_revoke_check CHECK (
        (revoked_at_ms IS NULL AND revoke_reason IS NULL)
        OR (revoked_at_ms IS NOT NULL AND revoke_reason IS NOT NULL)
    )
);

CREATE INDEX identity_session_families_user_active_idx
    ON identity_session_families (user_id, absolute_expires_at_ms)
    WHERE revoked_at_ms IS NULL;

CREATE TABLE identity_refresh_tokens (
    token_id UUID PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES identity_session_families(session_id) ON DELETE CASCADE,
    parent_token_id UUID REFERENCES identity_refresh_tokens(token_id),
    secret_hash BYTEA NOT NULL UNIQUE,
    pepper_version INTEGER NOT NULL,
    issued_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    consumed_at_ms BIGINT,
    revoked_at_ms BIGINT,
    replaced_by_token_id UUID REFERENCES identity_refresh_tokens(token_id),
    CONSTRAINT identity_refresh_hash_check CHECK (octet_length(secret_hash) = 32),
    CONSTRAINT identity_refresh_pepper_check CHECK (pepper_version BETWEEN 1 AND 65535),
    CONSTRAINT identity_refresh_time_check CHECK (expires_at_ms > issued_at_ms),
    CONSTRAINT identity_refresh_replacement_check CHECK (
        (consumed_at_ms IS NULL AND replaced_by_token_id IS NULL)
        OR (consumed_at_ms IS NOT NULL AND replaced_by_token_id IS NOT NULL)
    ),
    CONSTRAINT identity_refresh_not_self_parent_check CHECK (parent_token_id IS DISTINCT FROM token_id),
    CONSTRAINT identity_refresh_not_self_replacement_check CHECK (replaced_by_token_id IS DISTINCT FROM token_id)
);

CREATE INDEX identity_refresh_tokens_session_idx
    ON identity_refresh_tokens (session_id, issued_at_ms DESC);

CREATE TABLE identity_login_throttles (
    throttle_key BYTEA PRIMARY KEY,
    dimension TEXT NOT NULL,
    window_started_at_ms BIGINT NOT NULL,
    failure_count INTEGER NOT NULL,
    blocked_until_ms BIGINT,
    updated_at_ms BIGINT NOT NULL,
    CONSTRAINT identity_login_throttle_key_check CHECK (octet_length(throttle_key) = 32),
    CONSTRAINT identity_login_throttle_dimension_check CHECK (
        dimension IN ('account', 'network', 'global')
    ),
    CONSTRAINT identity_login_throttle_failure_check CHECK (failure_count >= 0),
    CONSTRAINT identity_login_throttle_time_check CHECK (updated_at_ms >= window_started_at_ms)
);

CREATE INDEX identity_login_throttles_blocked_idx
    ON identity_login_throttles (blocked_until_ms)
    WHERE blocked_until_ms IS NOT NULL;

CREATE TABLE identity_audit_events (
    event_id UUID PRIMARY KEY,
    actor_user_id UUID,
    session_id UUID,
    request_id TEXT,
    action TEXT NOT NULL,
    resource_type TEXT,
    resource_id TEXT,
    outcome TEXT NOT NULL,
    reason_code TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at_ms BIGINT NOT NULL,
    CONSTRAINT identity_audit_action_check CHECK (char_length(action) BETWEEN 1 AND 128),
    CONSTRAINT identity_audit_outcome_check CHECK (outcome IN ('success', 'denied', 'failure')),
    CONSTRAINT identity_audit_metadata_check CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX identity_audit_events_actor_created_idx
    ON identity_audit_events (actor_user_id, created_at_ms DESC);

CREATE INDEX identity_audit_events_session_created_idx
    ON identity_audit_events (session_id, created_at_ms DESC);
