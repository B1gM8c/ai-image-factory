CREATE TABLE admission_sessions (
    session_id UUID PRIMARY KEY,
    owner_token UUID NOT NULL UNIQUE,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    api_profile TEXT NOT NULL,
    operation TEXT NOT NULL,
    request_id TEXT NOT NULL,
    idempotency_key_digest TEXT,
    request_hash TEXT NOT NULL CHECK (char_length(request_hash) = 64),
    state TEXT NOT NULL CHECK (state IN ('receiving', 'attached', 'aborted')),
    job_id UUID REFERENCES jobs(job_id) ON DELETE RESTRICT,
    deadline_at_ms BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    CHECK (idempotency_key_digest IS NULL OR char_length(idempotency_key_digest) = 64),
    CHECK ((state = 'attached') = (job_id IS NOT NULL))
);

CREATE INDEX admission_sessions_state_deadline_idx
    ON admission_sessions (state, deadline_at_ms);

CREATE TABLE idempotency_requests (
    project_id TEXT NOT NULL,
    api_profile TEXT NOT NULL,
    operation TEXT NOT NULL,
    key_digest TEXT NOT NULL CHECK (char_length(key_digest) = 64),
    tenant_id TEXT NOT NULL,
    request_hash TEXT NOT NULL CHECK (char_length(request_hash) = 64),
    session_id UUID NOT NULL UNIQUE REFERENCES admission_sessions(session_id) ON DELETE RESTRICT,
    job_id UUID REFERENCES jobs(job_id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK (state IN ('receiving', 'accepted', 'succeeded', 'failed', 'uncertain', 'aborted')),
    terminal_outcome TEXT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (project_id, api_profile, operation, key_digest),
    CHECK ((state IN ('accepted', 'succeeded', 'failed', 'uncertain')) = (job_id IS NOT NULL)),
    CHECK ((state IN ('succeeded', 'failed', 'uncertain')) = (terminal_outcome IS NOT NULL))
);

CREATE TABLE job_payloads (
    job_id UUID PRIMARY KEY REFERENCES jobs(job_id) ON DELETE RESTRICT,
    admission_session_id UUID NOT NULL UNIQUE REFERENCES admission_sessions(session_id) ON DELETE RESTRICT,
    command_schema TEXT NOT NULL,
    command_json JSONB NOT NULL CHECK (jsonb_typeof(command_json) = 'object'),
    request_hash TEXT NOT NULL CHECK (char_length(request_hash) = 64),
    created_at_ms BIGINT NOT NULL
);

CREATE TABLE work_items (
    work_item_id UUID PRIMARY KEY,
    job_id UUID NOT NULL UNIQUE REFERENCES jobs(job_id) ON DELETE RESTRICT,
    kind TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ready', 'leased', 'running', 'succeeded', 'failed', 'uncertain')),
    available_at_ms BIGINT NOT NULL,
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_owner TEXT,
    lease_expires_at_ms BIGINT,
    execution_id UUID UNIQUE,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    CHECK (
        (state IN ('leased', 'running') AND lease_owner IS NOT NULL AND lease_expires_at_ms IS NOT NULL AND execution_id IS NOT NULL)
        OR
        (state NOT IN ('leased', 'running') AND lease_owner IS NULL AND lease_expires_at_ms IS NULL)
    )
);

CREATE INDEX work_items_ready_idx
    ON work_items (available_at_ms, created_at_ms)
    WHERE state = 'ready';

CREATE INDEX work_items_lease_expiry_idx
    ON work_items (lease_expires_at_ms)
    WHERE state IN ('leased', 'running');

CREATE TABLE job_attempts (
    attempt_id UUID PRIMARY KEY,
    execution_id UUID NOT NULL UNIQUE,
    work_item_id UUID NOT NULL REFERENCES work_items(work_item_id) ON DELETE RESTRICT,
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    worker_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('claimed', 'running', 'succeeded', 'failed', 'uncertain')),
    started_at_ms BIGINT,
    finished_at_ms BIGINT,
    error_code TEXT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (work_item_id, lease_epoch)
);

CREATE TABLE job_events (
    event_id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE RESTRICT,
    event_type TEXT NOT NULL,
    semantic_key TEXT NOT NULL,
    payload_json JSONB NOT NULL CHECK (jsonb_typeof(payload_json) = 'object'),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (job_id, semantic_key)
);

CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE RESTRICT,
    event_type TEXT NOT NULL,
    semantic_key TEXT NOT NULL,
    payload_json JSONB NOT NULL CHECK (jsonb_typeof(payload_json) = 'object'),
    created_at_ms BIGINT NOT NULL,
    published_at_ms BIGINT,
    UNIQUE (job_id, semantic_key)
);

CREATE INDEX outbox_events_pending_idx
    ON outbox_events (created_at_ms)
    WHERE published_at_ms IS NULL;
