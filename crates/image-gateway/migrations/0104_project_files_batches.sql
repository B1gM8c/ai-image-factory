CREATE TABLE project_files (
    file_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    purpose TEXT NOT NULL CHECK (
        purpose IN (
            'assistants',
            'batch',
            'batch_output',
            'fine-tune',
            'vision',
            'user_data',
            'evals'
        )
    ),
    filename TEXT NOT NULL,
    storage_backend TEXT NOT NULL,
    object_key TEXT NOT NULL,
    sha256_hex TEXT NOT NULL,
    byte_size BIGINT NOT NULL CHECK (
        byte_size > 0
        AND byte_size <= 536870912
        AND (purpose <> 'batch' OR byte_size <= 209715200)
    ),
    state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('active', 'deleted')),
    expires_at_ms BIGINT,
    deleted_at_ms BIGINT,
    cleanup_lease_owner TEXT,
    cleanup_lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (cleanup_lease_epoch >= 0),
    cleanup_lease_expires_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (file_id, project_id, tenant_id),
    UNIQUE (object_key),
    FOREIGN KEY (project_id, tenant_id)
        REFERENCES gateway_projects(id, tenant_id) ON DELETE RESTRICT,
    CHECK (char_length(file_id) BETWEEN 1 AND 96),
    CHECK (char_length(filename) BETWEEN 1 AND 512),
    CHECK (storage_backend = 'filesystem-v1'),
    CHECK (object_key ~ '^batch-files/[0-9a-f]{2}/[0-9a-f]{32}$'),
    CHECK (sha256_hex ~ '^[0-9a-f]{64}$'),
    CHECK (expires_at_ms IS NULL OR expires_at_ms > created_at_ms),
    CHECK (updated_at_ms >= created_at_ms),
    CHECK (
        (state = 'active' AND deleted_at_ms IS NULL)
        OR
        (state = 'deleted' AND deleted_at_ms IS NOT NULL)
    ),
    CHECK (
        (cleanup_lease_owner IS NULL AND cleanup_lease_expires_at_ms IS NULL)
        OR
        (cleanup_lease_owner IS NOT NULL AND cleanup_lease_expires_at_ms IS NOT NULL)
    )
);

CREATE INDEX project_files_project_created_idx
    ON project_files(tenant_id, project_id, created_at_ms DESC, file_id DESC)
    WHERE state = 'active';

CREATE INDEX project_files_expiry_cleanup_idx
    ON project_files(
        expires_at_ms,
        cleanup_lease_expires_at_ms,
        created_at_ms,
        file_id
    )
    WHERE state = 'active' AND expires_at_ms IS NOT NULL;

CREATE INDEX project_files_deleted_cleanup_idx
    ON project_files(
        COALESCE(cleanup_lease_expires_at_ms, deleted_at_ms),
        deleted_at_ms,
        file_id
    )
    WHERE state = 'deleted';

CREATE TABLE project_batches (
    batch_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    input_file_id TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    model TEXT NOT NULL,
    completion_window TEXT NOT NULL DEFAULT '24h'
        CHECK (completion_window = '24h'),
    status TEXT NOT NULL DEFAULT 'validating' CHECK (
        status IN (
            'validating',
            'failed',
            'in_progress',
            'finalizing',
            'completed',
            'expired',
            'cancelling',
            'cancelled'
        )
    ),
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB
        CHECK (jsonb_typeof(metadata) = 'object'),
    auth_snapshot JSONB NOT NULL
        CHECK (jsonb_typeof(auth_snapshot) = 'object'),
    route_snapshot JSONB NOT NULL
        CHECK (jsonb_typeof(route_snapshot) = 'object'),
    request_count_total INTEGER NOT NULL CHECK (
        request_count_total > 0 AND request_count_total <= 50000
    ),
    request_count_completed INTEGER NOT NULL DEFAULT 0
        CHECK (request_count_completed >= 0),
    request_count_failed INTEGER NOT NULL DEFAULT 0
        CHECK (request_count_failed >= 0),
    request_count_cancelled INTEGER NOT NULL DEFAULT 0
        CHECK (request_count_cancelled >= 0),
    output_retention_seconds INTEGER NOT NULL DEFAULT 2592000 CHECK (
        output_retention_seconds BETWEEN 3600 AND 2592000
    ),
    errors JSONB,
    control_version BIGINT NOT NULL DEFAULT 1 CHECK (control_version > 0),
    lease_owner TEXT,
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    validated_at_ms BIGINT,
    in_progress_at_ms BIGINT,
    finalizing_at_ms BIGINT,
    completed_at_ms BIGINT,
    failed_at_ms BIGINT,
    expires_at_ms BIGINT NOT NULL,
    cancel_requested_at_ms BIGINT,
    cancelled_at_ms BIGINT,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (batch_id, project_id, tenant_id),
    FOREIGN KEY (project_id, tenant_id)
        REFERENCES gateway_projects(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (input_file_id, project_id, tenant_id)
        REFERENCES project_files(file_id, project_id, tenant_id)
        ON DELETE RESTRICT,
    CHECK (char_length(batch_id) BETWEEN 1 AND 96),
    CHECK (char_length(endpoint) BETWEEN 1 AND 256),
    CHECK (char_length(model) BETWEEN 1 AND 256),
    CHECK (
        request_count_completed
        + request_count_failed
        + request_count_cancelled
        <= request_count_total
    ),
    CHECK (errors IS NULL OR jsonb_typeof(errors) IN ('object', 'array')),
    CHECK (expires_at_ms > created_at_ms),
    CHECK (updated_at_ms >= created_at_ms),
    CHECK (
        (lease_owner IS NULL AND lease_expires_at_ms IS NULL)
        OR
        (lease_owner IS NOT NULL AND lease_expires_at_ms IS NOT NULL)
    ),
    CHECK (
        (status = 'in_progress'
            AND validated_at_ms IS NOT NULL
            AND in_progress_at_ms IS NOT NULL)
        OR status <> 'in_progress'
    ),
    CHECK (
        (status = 'finalizing' AND finalizing_at_ms IS NOT NULL)
        OR status <> 'finalizing'
    ),
    CHECK (
        (status = 'completed' AND completed_at_ms IS NOT NULL)
        OR status <> 'completed'
    ),
    CHECK (
        (status = 'failed' AND failed_at_ms IS NOT NULL)
        OR status <> 'failed'
    ),
    CHECK (
        (status IN ('cancelling', 'cancelled')
            AND cancel_requested_at_ms IS NOT NULL)
        OR status NOT IN ('cancelling', 'cancelled')
    ),
    CHECK (
        (status = 'cancelled' AND cancelled_at_ms IS NOT NULL)
        OR status <> 'cancelled'
    )
);

CREATE INDEX project_batches_project_created_idx
    ON project_batches(tenant_id, project_id, created_at_ms DESC, batch_id DESC);

CREATE INDEX project_batches_recovery_idx
    ON project_batches(updated_at_ms, batch_id)
    WHERE status IN ('validating', 'in_progress', 'finalizing', 'cancelling');

CREATE INDEX project_batches_expiry_idx
    ON project_batches(expires_at_ms, created_at_ms, batch_id)
    WHERE status IN ('validating', 'in_progress', 'finalizing', 'cancelling');

CREATE TABLE project_batch_requests (
    request_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0 AND ordinal < 50000),
    custom_id TEXT NOT NULL,
    method TEXT NOT NULL CHECK (method = 'POST'),
    request_url TEXT NOT NULL,
    model TEXT NOT NULL,
    request_body JSONB NOT NULL CHECK (jsonb_typeof(request_body) = 'object'),
    request_hash TEXT NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'leased', 'completed', 'failed', 'cancelled')),
    available_at_ms BIGINT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error JSONB,
    response_status_code SMALLINT,
    response_request_id TEXT,
    response_body JSONB,
    error JSONB,
    lease_owner TEXT,
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    started_at_ms BIGINT,
    completed_at_ms BIGINT,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (request_id, batch_id, project_id, tenant_id),
    UNIQUE (batch_id, ordinal),
    UNIQUE (batch_id, custom_id),
    FOREIGN KEY (batch_id, project_id, tenant_id)
        REFERENCES project_batches(batch_id, project_id, tenant_id)
        ON DELETE RESTRICT,
    CHECK (char_length(custom_id) BETWEEN 1 AND 256),
    CHECK (char_length(request_url) BETWEEN 1 AND 256),
    CHECK (char_length(model) BETWEEN 1 AND 256),
    CHECK (available_at_ms >= created_at_ms),
    CHECK (last_error IS NULL OR jsonb_typeof(last_error) = 'object'),
    CHECK (
        response_status_code IS NULL
        OR response_status_code BETWEEN 100 AND 599
    ),
    CHECK (updated_at_ms >= created_at_ms),
    CHECK (
        (state = 'leased'
            AND lease_owner IS NOT NULL
            AND lease_expires_at_ms IS NOT NULL
            AND started_at_ms IS NOT NULL)
        OR
        (state <> 'leased'
            AND lease_owner IS NULL
            AND lease_expires_at_ms IS NULL)
    ),
    CHECK (
        (state = 'completed'
            AND response_status_code IS NOT NULL
            AND response_body IS NOT NULL
            AND error IS NULL
            AND completed_at_ms IS NOT NULL)
        OR state <> 'completed'
    ),
    CHECK (
        (state = 'failed'
            AND response_body IS NULL
            AND error IS NOT NULL
            AND completed_at_ms IS NOT NULL)
        OR state <> 'failed'
    ),
    CHECK (
        (state = 'cancelled' AND completed_at_ms IS NOT NULL)
        OR state <> 'cancelled'
    )
);

CREATE INDEX project_batch_requests_claim_idx
    ON project_batch_requests(
        tenant_id,
        project_id,
        batch_id,
        (
            CASE WHEN state = 'pending' THEN 0 ELSE 1 END
        ),
        available_at_ms,
        COALESCE(lease_expires_at_ms, available_at_ms),
        ordinal
    )
    WHERE state IN ('pending', 'leased');

CREATE INDEX project_batch_requests_lease_expiry_idx
    ON project_batch_requests(lease_expires_at_ms, batch_id, ordinal)
    WHERE state = 'leased';

CREATE TABLE project_batch_output_files (
    batch_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('output', 'error')),
    file_id TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (batch_id, role),
    UNIQUE (file_id),
    FOREIGN KEY (batch_id, project_id, tenant_id)
        REFERENCES project_batches(batch_id, project_id, tenant_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (file_id, project_id, tenant_id)
        REFERENCES project_files(file_id, project_id, tenant_id)
        ON DELETE RESTRICT
);

ALTER TABLE gateway_request_observations
    DROP CONSTRAINT IF EXISTS gateway_request_observations_source_check;

ALTER TABLE gateway_request_observations
    ADD CONSTRAINT gateway_request_observations_source_check
    CHECK (source IN ('models', 'images', 'videos', 'files', 'batches'))
    NOT VALID;

ALTER TABLE gateway_request_observations
    VALIDATE CONSTRAINT gateway_request_observations_source_check;

COMMENT ON TABLE project_files IS
    'Project-scoped OpenAI-compatible file metadata; bytes live in a private blob store.';
COMMENT ON TABLE project_batches IS
    'Project-scoped durable Batch API containers with fenced finalization leases.';
COMMENT ON TABLE project_batch_requests IS
    'Validated Batch JSONL rows with recoverable fenced execution leases.';
COMMENT ON TABLE project_batch_output_files IS
    'At most one output and one error file for each Batch container.';
