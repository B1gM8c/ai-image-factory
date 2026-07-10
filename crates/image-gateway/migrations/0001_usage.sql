CREATE TABLE IF NOT EXISTS usage_events (
    event_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'tenant_default',
    request_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    units INTEGER NOT NULL CHECK (units > 0),
    outcome TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS usage_events_tenant_created_at_ms_idx
    ON usage_events (tenant_id, created_at_ms);

CREATE TABLE IF NOT EXISTS gateway_service_accounts (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    deleted_at BIGINT
);

CREATE TABLE IF NOT EXISTS gateway_api_keys (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    service_account_id TEXT NOT NULL REFERENCES gateway_service_accounts(id),
    name TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    redacted_value TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    last_used_at BIGINT,
    deleted_at BIGINT
);

CREATE INDEX IF NOT EXISTS gateway_api_keys_project_id_idx
    ON gateway_api_keys (project_id, created_at);

CREATE TABLE IF NOT EXISTS quota_reservations (
    reservation_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'tenant_default',
    request_id TEXT NOT NULL,
    job_id UUID,
    requested_units INTEGER NOT NULL CHECK (requested_units > 0),
    committed_units INTEGER NOT NULL DEFAULT 0,
    started_units INTEGER NOT NULL DEFAULT 0,
    released_units INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS quota_reservations_active_tenant_idx
    ON quota_reservations (tenant_id, state, expires_at_ms);

CREATE TABLE IF NOT EXISTS jobs (
    job_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'tenant_default',
    request_id TEXT NOT NULL,
    operation TEXT NOT NULL DEFAULT 'generation',
    provider_id TEXT NOT NULL DEFAULT 'openai-codex',
    model TEXT NOT NULL DEFAULT 'gpt-image-2',
    state TEXT NOT NULL,
    requested_units INTEGER NOT NULL,
    charged_units INTEGER NOT NULL DEFAULT 0,
    reservation_id UUID,
    queue_entered_at_ms BIGINT,
    started_at_ms BIGINT,
    finished_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL DEFAULT 0,
    updated_at_ms BIGINT NOT NULL DEFAULT 0,
    last_error_code TEXT,
    last_error_message TEXT
);

CREATE INDEX IF NOT EXISTS jobs_tenant_state_created_idx
    ON jobs (tenant_id, state, created_at_ms);

CREATE TABLE IF NOT EXISTS metering_events (
    event_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    job_id UUID,
    reservation_id UUID,
    request_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    event_type TEXT NOT NULL,
    units INTEGER NOT NULL DEFAULT 0,
    outcome TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS metering_events_tenant_created_idx
    ON metering_events (tenant_id, created_at_ms);
