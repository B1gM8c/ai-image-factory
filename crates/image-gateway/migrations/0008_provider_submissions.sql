CREATE TABLE job_outputs (
    output_id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE RESTRICT,
    output_index INTEGER NOT NULL CHECK (output_index >= 0),
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'running', 'succeeded', 'failed', 'uncertain')
    ),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    started_at_ms BIGINT,
    finished_at_ms BIGINT,
    error_code TEXT CHECK (
        error_code IS NULL OR error_code ~ '^[A-Za-z0-9_.-]{1,128}$'
    ),
    UNIQUE (job_id, output_index),
    UNIQUE (output_id, job_id),
    CHECK (
        (state = 'pending' AND started_at_ms IS NULL AND finished_at_ms IS NULL)
        OR
        (state = 'running' AND started_at_ms IS NOT NULL AND finished_at_ms IS NULL)
        OR
        (state IN ('succeeded', 'failed', 'uncertain')
            AND started_at_ms IS NOT NULL AND finished_at_ms IS NOT NULL)
    ),
    CHECK (state <> 'succeeded' OR error_code IS NULL)
);

ALTER TABLE work_items
    ADD CONSTRAINT work_items_identity_job_unique
    UNIQUE (work_item_id, job_id);

ALTER TABLE jobs
    ADD CONSTRAINT jobs_provider_identity_unique
    UNIQUE (job_id, tenant_id, provider_id, model);

ALTER TABLE job_attempts
    ADD CONSTRAINT job_attempts_execution_work_epoch_unique
    UNIQUE (execution_id, work_item_id, lease_epoch);

CREATE TABLE provider_submissions (
    submission_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    output_id UUID NOT NULL UNIQUE,
    job_id UUID NOT NULL,
    tenant_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    model TEXT NOT NULL,
    work_item_id UUID NOT NULL,
    created_by_execution_id UUID NOT NULL,
    created_by_lease_epoch BIGINT NOT NULL CHECK (created_by_lease_epoch > 0),
    command_schema TEXT NOT NULL CHECK (command_schema <> ''),
    command_hash TEXT NOT NULL CHECK (command_hash ~ '^[0-9a-f]{64}$'),
    state TEXT NOT NULL CHECK (
        state IN ('prepared', 'running', 'succeeded', 'failed', 'uncertain', 'canceled')
    ),
    result_manifest_id UUID,
    prepared_at_ms BIGINT NOT NULL,
    started_at_ms BIGINT,
    finished_at_ms BIGINT,
    updated_at_ms BIGINT NOT NULL,
    error_code TEXT CHECK (
        error_code IS NULL OR error_code ~ '^[A-Za-z0-9_.-]{1,128}$'
    ),
    FOREIGN KEY (output_id, job_id)
        REFERENCES job_outputs(output_id, job_id) ON DELETE RESTRICT,
    FOREIGN KEY (job_id, tenant_id, provider_id, model)
        REFERENCES jobs(job_id, tenant_id, provider_id, model) ON DELETE RESTRICT,
    FOREIGN KEY (work_item_id, job_id)
        REFERENCES work_items(work_item_id, job_id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by_execution_id, work_item_id, created_by_lease_epoch)
        REFERENCES job_attempts(execution_id, work_item_id, lease_epoch) ON DELETE RESTRICT,
    UNIQUE (executor_execution_id, submission_id),
    UNIQUE (submission_id, work_item_id, job_id),
    CHECK (executor_execution_id <> created_by_execution_id),
    CHECK (
        (state = 'prepared' AND started_at_ms IS NULL AND finished_at_ms IS NULL
            AND result_manifest_id IS NULL AND error_code IS NULL)
        OR
        (state = 'running' AND started_at_ms IS NOT NULL AND finished_at_ms IS NULL
            AND result_manifest_id IS NULL AND error_code IS NULL)
        OR
        (state = 'succeeded' AND started_at_ms IS NOT NULL AND finished_at_ms IS NOT NULL
            AND result_manifest_id IS NOT NULL AND error_code IS NULL)
        OR
        (state IN ('failed', 'uncertain')
            AND started_at_ms IS NOT NULL AND finished_at_ms IS NOT NULL
            AND result_manifest_id IS NULL AND error_code IS NOT NULL)
        OR
        (state = 'canceled' AND started_at_ms IS NULL AND finished_at_ms IS NOT NULL
            AND result_manifest_id IS NULL AND error_code IS NOT NULL)
    )
);

CREATE TABLE provider_submission_attachments (
    submission_id UUID NOT NULL REFERENCES provider_submissions(submission_id) ON DELETE RESTRICT,
    job_id UUID NOT NULL,
    attempt_execution_id UUID NOT NULL,
    work_item_id UUID NOT NULL,
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    attached_at_ms BIGINT NOT NULL,
    PRIMARY KEY (submission_id, attempt_execution_id),
    FOREIGN KEY (submission_id, work_item_id, job_id)
        REFERENCES provider_submissions(submission_id, work_item_id, job_id) ON DELETE RESTRICT,
    FOREIGN KEY (attempt_execution_id, work_item_id, lease_epoch)
        REFERENCES job_attempts(execution_id, work_item_id, lease_epoch) ON DELETE RESTRICT
);

CREATE TABLE executor_executions (
    executor_execution_id UUID PRIMARY KEY,
    submission_id UUID NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (
        state IN ('prepared', 'leased', 'running', 'succeeded', 'failed', 'uncertain', 'canceled')
    ),
    executor_owner TEXT,
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    leased_at_ms BIGINT,
    started_at_ms BIGINT,
    finished_at_ms BIGINT,
    updated_at_ms BIGINT NOT NULL,
    error_code TEXT CHECK (
        error_code IS NULL OR error_code ~ '^[A-Za-z0-9_.-]{1,128}$'
    ),
    UNIQUE (executor_execution_id, submission_id),
    FOREIGN KEY (executor_execution_id, submission_id)
        REFERENCES provider_submissions(executor_execution_id, submission_id)
        ON DELETE RESTRICT,
    CHECK (
        (state = 'prepared'
            AND executor_owner IS NULL AND lease_epoch = 0
            AND lease_expires_at_ms IS NULL AND leased_at_ms IS NULL
            AND started_at_ms IS NULL AND finished_at_ms IS NULL AND error_code IS NULL)
        OR
        (state = 'leased'
            AND executor_owner IS NOT NULL AND executor_owner <> '' AND lease_epoch > 0
            AND lease_expires_at_ms IS NOT NULL AND leased_at_ms IS NOT NULL
            AND started_at_ms IS NULL AND finished_at_ms IS NULL AND error_code IS NULL)
        OR
        (state = 'running'
            AND executor_owner IS NOT NULL AND executor_owner <> '' AND lease_epoch > 0
            AND lease_expires_at_ms IS NOT NULL AND leased_at_ms IS NOT NULL
            AND started_at_ms IS NOT NULL AND finished_at_ms IS NULL AND error_code IS NULL)
        OR
        (state = 'succeeded'
            AND executor_owner IS NULL AND lease_epoch > 0
            AND lease_expires_at_ms IS NULL AND leased_at_ms IS NOT NULL
            AND started_at_ms IS NOT NULL AND finished_at_ms IS NOT NULL AND error_code IS NULL)
        OR
        (state IN ('failed', 'uncertain')
            AND executor_owner IS NULL AND lease_epoch > 0
            AND lease_expires_at_ms IS NULL AND leased_at_ms IS NOT NULL
            AND started_at_ms IS NOT NULL AND finished_at_ms IS NOT NULL
            AND error_code IS NOT NULL)
        OR
        (state = 'canceled'
            AND executor_owner IS NULL AND lease_epoch > 0
            AND lease_expires_at_ms IS NULL AND leased_at_ms IS NOT NULL
            AND started_at_ms IS NULL AND finished_at_ms IS NOT NULL
            AND error_code IS NOT NULL)
    )
);

CREATE INDEX executor_executions_claim_idx
    ON executor_executions (created_at_ms, executor_execution_id)
    WHERE state IN ('prepared', 'leased');

CREATE INDEX executor_executions_running_expiry_idx
    ON executor_executions (lease_expires_at_ms, executor_execution_id)
    WHERE state = 'running';

CREATE TABLE executor_result_manifests (
    manifest_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    submission_id UUID NOT NULL UNIQUE,
    storage_backend TEXT NOT NULL CHECK (storage_backend <> ''),
    object_key TEXT NOT NULL CHECK (object_key <> ''),
    sha256_hex TEXT NOT NULL CHECK (sha256_hex ~ '^[0-9a-f]{64}$'),
    byte_size BIGINT NOT NULL CHECK (byte_size > 0),
    media_type TEXT NOT NULL CHECK (media_type LIKE 'image/%'),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (storage_backend, object_key),
    UNIQUE (manifest_id, executor_execution_id, submission_id),
    FOREIGN KEY (executor_execution_id, submission_id)
        REFERENCES executor_executions(executor_execution_id, submission_id)
        ON DELETE RESTRICT
);

ALTER TABLE provider_submissions
    ADD CONSTRAINT provider_submissions_result_manifest_fk
    FOREIGN KEY (result_manifest_id, executor_execution_id, submission_id)
    REFERENCES executor_result_manifests(manifest_id, executor_execution_id, submission_id)
    ON DELETE RESTRICT;
