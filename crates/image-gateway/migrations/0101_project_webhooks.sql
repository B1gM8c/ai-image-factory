CREATE TABLE project_webhook_endpoints (
    endpoint_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    name TEXT,
    url TEXT NOT NULL,
    event_types TEXT[] NOT NULL,
    state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('active', 'disabled', 'deleted')),
    signing_key_version INTEGER NOT NULL CHECK (signing_key_version > 0),
    secret_revision BIGINT NOT NULL DEFAULT 1 CHECK (secret_revision > 0),
    created_by_user_id UUID NOT NULL REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    disabled_at_ms BIGINT,
    deleted_at_ms BIGINT,
    control_version BIGINT NOT NULL DEFAULT 1 CHECK (control_version > 0),
    UNIQUE (endpoint_id, project_id, organization_id),
    CHECK (name IS NULL OR char_length(name) BETWEEN 1 AND 128),
    CHECK (char_length(url) BETWEEN 1 AND 2048),
    CHECK (cardinality(event_types) BETWEEN 1 AND 64),
    CHECK (array_position(event_types, NULL) IS NULL),
    CHECK (updated_at_ms >= created_at_ms),
    CHECK (
        (state = 'active' AND disabled_at_ms IS NULL AND deleted_at_ms IS NULL)
        OR
        (state = 'disabled' AND disabled_at_ms IS NOT NULL AND deleted_at_ms IS NULL)
        OR
        (state = 'deleted' AND deleted_at_ms IS NOT NULL)
    ),
    FOREIGN KEY (project_id, organization_id)
        REFERENCES gateway_projects(id, tenant_id) ON DELETE RESTRICT
);

CREATE INDEX project_webhook_endpoints_project_created_idx
    ON project_webhook_endpoints(project_id, created_at_ms DESC, endpoint_id DESC);

CREATE INDEX project_webhook_endpoints_active_events_idx
    ON project_webhook_endpoints USING GIN(event_types)
    WHERE state = 'active';

CREATE TABLE project_webhook_endpoint_runtime (
    endpoint_id TEXT PRIMARY KEY
        REFERENCES project_webhook_endpoints(endpoint_id) ON DELETE RESTRICT,
    paused_until_ms BIGINT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0
        CHECK (consecutive_failures >= 0),
    updated_at_ms BIGINT NOT NULL
);

CREATE TABLE project_webhook_events (
    event_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('outbox', 'test')),
    outbox_event_id UUID UNIQUE REFERENCES outbox_events(event_id) ON DELETE RESTRICT,
    event_type TEXT NOT NULL,
    payload_json JSONB NOT NULL CHECK (jsonb_typeof(payload_json) = 'object'),
    payload_body BYTEA NOT NULL,
    created_at_ms BIGINT NOT NULL,
    UNIQUE (event_id, project_id, organization_id),
    CHECK (
        (source_kind = 'outbox' AND outbox_event_id IS NOT NULL)
        OR
        (source_kind = 'test' AND outbox_event_id IS NULL)
    ),
    FOREIGN KEY (project_id, organization_id)
        REFERENCES gateway_projects(id, tenant_id) ON DELETE RESTRICT
);

CREATE INDEX project_webhook_events_project_created_idx
    ON project_webhook_events(project_id, created_at_ms DESC, event_id DESC);

CREATE TABLE project_webhook_deliveries (
    delivery_id UUID PRIMARY KEY,
    event_id TEXT NOT NULL,
    endpoint_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN (
            'pending',
            'leased',
            'retry_wait',
            'succeeded',
            'dead_lettered',
            'canceled'
        )),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at_ms BIGINT NOT NULL,
    retry_deadline_at_ms BIGINT NOT NULL,
    lease_owner TEXT,
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at_ms BIGINT,
    last_http_status INTEGER,
    last_error_code TEXT,
    last_attempt_at_ms BIGINT,
    delivered_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (event_id, endpoint_id),
    CHECK (retry_deadline_at_ms > created_at_ms),
    CHECK (updated_at_ms >= created_at_ms),
    CHECK (last_http_status IS NULL OR last_http_status BETWEEN 100 AND 599),
    CHECK (
        (state = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at_ms IS NOT NULL)
        OR
        (state <> 'leased' AND lease_owner IS NULL AND lease_expires_at_ms IS NULL)
    ),
    CHECK (
        (state = 'succeeded' AND delivered_at_ms IS NOT NULL)
        OR
        (state <> 'succeeded' AND delivered_at_ms IS NULL)
    ),
    FOREIGN KEY (project_id, organization_id)
        REFERENCES gateway_projects(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (event_id, project_id, organization_id)
        REFERENCES project_webhook_events(event_id, project_id, organization_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (endpoint_id, project_id, organization_id)
        REFERENCES project_webhook_endpoints(endpoint_id, project_id, organization_id)
        ON DELETE RESTRICT
);

CREATE INDEX project_webhook_deliveries_ready_idx
    ON project_webhook_deliveries(next_attempt_at_ms, created_at_ms)
    WHERE state IN ('pending', 'retry_wait');

CREATE INDEX project_webhook_deliveries_lease_expiry_idx
    ON project_webhook_deliveries(lease_expires_at_ms)
    WHERE state = 'leased';

CREATE INDEX project_webhook_deliveries_endpoint_created_idx
    ON project_webhook_deliveries(endpoint_id, created_at_ms DESC, delivery_id DESC);

CREATE TABLE project_webhook_attempts (
    attempt_id UUID PRIMARY KEY,
    delivery_id UUID NOT NULL
        REFERENCES project_webhook_deliveries(delivery_id) ON DELETE RESTRICT,
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'retry', 'dead_lettered')),
    webhook_timestamp BIGINT NOT NULL,
    http_status INTEGER,
    error_code TEXT,
    duration_ms BIGINT NOT NULL CHECK (duration_ms >= 0),
    next_attempt_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    UNIQUE (delivery_id, attempt_number),
    CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    CHECK (
        (outcome = 'retry' AND next_attempt_at_ms IS NOT NULL)
        OR
        (outcome <> 'retry' AND next_attempt_at_ms IS NULL)
    )
);

CREATE INDEX project_webhook_attempts_delivery_created_idx
    ON project_webhook_attempts(delivery_id, attempt_number DESC);

COMMENT ON TABLE project_webhook_endpoints IS
    'Project-scoped Standard Webhooks endpoints. Plaintext signing secrets are never persisted.';
COMMENT ON TABLE project_webhook_events IS
    'Canonical immutable webhook event envelopes materialized from the durable job outbox or explicit tests.';
COMMENT ON TABLE project_webhook_deliveries IS
    'At-least-once endpoint delivery state with fenced leases and a 72-hour retry deadline.';
COMMENT ON TABLE project_webhook_attempts IS
    'Append-only webhook delivery attempt audit without response bodies or secret material.';
