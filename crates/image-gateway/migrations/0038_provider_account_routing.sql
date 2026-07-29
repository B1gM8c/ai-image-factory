ALTER TABLE provider_accounts
    ADD CONSTRAINT provider_accounts_environment_identity_unique
    UNIQUE (provider_account_id, provider_id);

-- Managed CLI accounts are isolated by an opaque environment reference. The
-- reference is resolved only by the control plane and is never returned by the
-- public API.
CREATE TABLE provider_account_environments (
    provider_account_id UUID PRIMARY KEY
        REFERENCES provider_accounts(provider_account_id) ON DELETE RESTRICT,
    provider_id TEXT NOT NULL,
    environment_kind TEXT NOT NULL CHECK (
        environment_kind IN ('codex_home_v1', 'grok_home_v1', 'dreamina_home_v1')
    ),
    environment_ref TEXT NOT NULL UNIQUE CHECK (
        char_length(environment_ref) BETWEEN 1 AND 2048
        AND environment_ref !~ '[[:cntrl:]]'
    ),
    upstream_identity_sha256 TEXT NOT NULL CHECK (
        upstream_identity_sha256 ~ '^[0-9a-f]{64}$'
    ),
    display_name TEXT NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 128),
    account_email TEXT CHECK (account_email IS NULL OR char_length(account_email) <= 320),
    state TEXT NOT NULL CHECK (state IN ('active', 'disabled', 'invalid')),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (provider_id, upstream_identity_sha256),
    UNIQUE (provider_account_id, provider_id),
    FOREIGN KEY (provider_account_id, provider_id)
        REFERENCES provider_accounts(provider_account_id, provider_id)
        ON DELETE RESTRICT
);

CREATE TABLE provider_account_login_sessions (
    login_session_id UUID PRIMARY KEY,
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    account_key TEXT NOT NULL CHECK (
        char_length(account_key) BETWEEN 1 AND 128
        AND account_key ~ '^[A-Za-z0-9_.-]+$'
    ),
    display_name TEXT NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 128),
    environment_ref TEXT NOT NULL UNIQUE CHECK (
        char_length(environment_ref) BETWEEN 1 AND 2048
        AND environment_ref !~ '[[:cntrl:]]'
    ),
    provider_login_id TEXT,
    verification_url TEXT,
    user_code TEXT,
    status TEXT NOT NULL CHECK (
        status IN ('starting', 'waiting_for_user', 'validating', 'succeeded', 'failed', 'expired')
    ),
    max_concurrency INTEGER NOT NULL CHECK (max_concurrency BETWEEN 1 AND 1024),
    provider_account_id UUID
        REFERENCES provider_accounts(provider_account_id) ON DELETE RESTRICT,
    error_code TEXT CHECK (error_code IS NULL OR char_length(error_code) <= 128),
    expires_at_ms BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    completed_at_ms BIGINT,
    CHECK (
        (status = 'succeeded' AND provider_account_id IS NOT NULL AND completed_at_ms IS NOT NULL)
        OR status <> 'succeeded'
    )
);

CREATE INDEX provider_account_login_sessions_status_expiry_idx
    ON provider_account_login_sessions (status, expires_at_ms)
    WHERE status IN ('starting', 'waiting_for_user', 'validating');

CREATE TABLE provider_account_quota_snapshots (
    provider_account_id UUID PRIMARY KEY
        REFERENCES provider_accounts(provider_account_id) ON DELETE RESTRICT,
    provider_id TEXT NOT NULL,
    plan_type TEXT,
    credits_balance TEXT,
    credits_unlimited BOOLEAN,
    status TEXT NOT NULL CHECK (status IN ('observed', 'stale', 'unavailable')),
    observed_at_ms BIGINT NOT NULL,
    last_error_code TEXT,
    FOREIGN KEY (provider_account_id, provider_id)
        REFERENCES provider_account_environments(provider_account_id, provider_id)
        ON DELETE RESTRICT
);

CREATE TABLE provider_account_quota_windows (
    provider_account_id UUID NOT NULL,
    provider_id TEXT NOT NULL,
    limit_id TEXT NOT NULL CHECK (char_length(limit_id) BETWEEN 1 AND 128),
    limit_name TEXT,
    window_role TEXT NOT NULL CHECK (window_role IN ('primary', 'secondary')),
    window_duration_mins BIGINT CHECK (window_duration_mins IS NULL OR window_duration_mins > 0),
    used_percent INTEGER NOT NULL CHECK (used_percent BETWEEN 0 AND 100),
    resets_at_ms BIGINT,
    observed_at_ms BIGINT NOT NULL,
    PRIMARY KEY (provider_account_id, limit_id, window_role),
    FOREIGN KEY (provider_account_id, provider_id)
        REFERENCES provider_account_environments(provider_account_id, provider_id)
        ON DELETE RESTRICT
);

-- A route is an immutable, versioned selection policy. A single-account route
-- and an account group have the same runtime shape, so API keys and schedulers
-- do not need polymorphic branches.
ALTER TABLE provider_execution_profiles
    ADD CONSTRAINT provider_execution_profiles_route_identity_unique
    UNIQUE (execution_profile_id, provider_account_id, provider_id, operation_id, command_schema);

CREATE TABLE provider_routes (
    route_id UUID NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    route_key TEXT NOT NULL CHECK (
        char_length(route_key) BETWEEN 1 AND 128
        AND route_key ~ '^[A-Za-z0-9_.-]+$'
    ),
    display_name TEXT NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 128),
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    operation_id TEXT NOT NULL CHECK (char_length(operation_id) BETWEEN 1 AND 128),
    command_schema TEXT NOT NULL CHECK (char_length(command_schema) BETWEEN 1 AND 128),
    route_kind TEXT NOT NULL CHECK (route_kind IN ('account', 'group')),
    selection_strategy TEXT NOT NULL CHECK (
        selection_strategy IN ('quota_aware_least_loaded', 'priority_weighted')
    ),
    state TEXT NOT NULL CHECK (state IN ('enabled', 'disabled')),
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (route_id, revision),
    UNIQUE (route_key, revision),
    UNIQUE (route_id, revision, provider_id, operation_id, command_schema)
);

CREATE UNIQUE INDEX provider_routes_enabled_key_uidx
    ON provider_routes(route_key) WHERE state = 'enabled';

CREATE TABLE provider_route_members (
    route_id UUID NOT NULL,
    route_revision BIGINT NOT NULL,
    provider_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    command_schema TEXT NOT NULL,
    provider_account_id UUID NOT NULL,
    execution_profile_id UUID NOT NULL,
    priority SMALLINT NOT NULL DEFAULT 0 CHECK (priority BETWEEN -1000 AND 1000),
    weight INTEGER NOT NULL DEFAULT 100 CHECK (weight BETWEEN 1 AND 1000000),
    state TEXT NOT NULL DEFAULT 'enabled' CHECK (state IN ('enabled', 'disabled')),
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (route_id, route_revision, execution_profile_id),
    FOREIGN KEY (route_id, route_revision, provider_id, operation_id, command_schema)
        REFERENCES provider_routes(route_id, revision, provider_id, operation_id, command_schema)
        ON DELETE RESTRICT,
    FOREIGN KEY (
        execution_profile_id, provider_account_id, provider_id, operation_id, command_schema
    ) REFERENCES provider_execution_profiles(
        execution_profile_id, provider_account_id, provider_id, operation_id, command_schema
    ) ON DELETE RESTRICT
);

CREATE INDEX provider_route_members_profile_lookup_idx
    ON provider_route_members(execution_profile_id, route_id, route_revision)
    WHERE state = 'enabled';

CREATE TABLE gateway_api_key_provider_routes (
    api_key_id TEXT NOT NULL,
    service_account_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    command_schema TEXT NOT NULL,
    route_id UUID NOT NULL,
    route_revision BIGINT NOT NULL,
    bound_at_ms BIGINT NOT NULL,
    PRIMARY KEY (api_key_id, provider_id, operation_id),
    FOREIGN KEY (api_key_id, service_account_id, project_id, tenant_id)
        REFERENCES gateway_api_keys(id, service_account_id, project_id, tenant_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (route_id, route_revision, provider_id, operation_id, command_schema)
        REFERENCES provider_routes(route_id, revision, provider_id, operation_id, command_schema)
        ON DELETE RESTRICT
);

CREATE TABLE job_provider_route_attributions (
    job_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    api_key_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    command_schema TEXT NOT NULL,
    route_id UUID NOT NULL,
    route_revision BIGINT NOT NULL,
    attributed_at_ms BIGINT NOT NULL,
    FOREIGN KEY (job_id, tenant_id)
        REFERENCES jobs(job_id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (route_id, route_revision, provider_id, operation_id, command_schema)
        REFERENCES provider_routes(route_id, revision, provider_id, operation_id, command_schema)
        ON DELETE RESTRICT
);

CREATE FUNCTION reject_job_provider_route_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'job provider route attribution is immutable'
        USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_provider_route_attributions_immutable
BEFORE UPDATE OR DELETE ON job_provider_route_attributions
FOR EACH ROW EXECUTE FUNCTION reject_job_provider_route_mutation();
