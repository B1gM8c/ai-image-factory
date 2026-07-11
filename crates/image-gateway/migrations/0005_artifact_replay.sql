CREATE TABLE job_response_projections (
    job_id UUID PRIMARY KEY REFERENCES jobs(job_id) ON DELETE RESTRICT,
    api_profile TEXT NOT NULL,
    response_schema TEXT NOT NULL,
    created_at_seconds BIGINT NOT NULL CHECK (created_at_seconds > 0),
    output_format TEXT NOT NULL,
    quality TEXT NOT NULL,
    size TEXT NOT NULL,
    background TEXT NOT NULL,
    stream BOOLEAN NOT NULL,
    limit_5h INTEGER NOT NULL CHECK (limit_5h >= 0),
    remaining_5h INTEGER NOT NULL CHECK (remaining_5h BETWEEN 0 AND limit_5h),
    limit_7d INTEGER NOT NULL CHECK (limit_7d >= 0),
    remaining_7d INTEGER NOT NULL CHECK (remaining_7d BETWEEN 0 AND limit_7d),
    artifact_count INTEGER NOT NULL CHECK (artifact_count > 0),
    created_at_ms BIGINT NOT NULL
);

CREATE TABLE artifacts (
    artifact_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE RESTRICT,
    work_item_id UUID NOT NULL REFERENCES work_items(work_item_id) ON DELETE RESTRICT,
    execution_id UUID NOT NULL REFERENCES job_attempts(execution_id) ON DELETE RESTRICT,
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    output_index INTEGER NOT NULL CHECK (output_index >= 0),
    state TEXT NOT NULL CHECK (state = 'ready'),
    storage_backend TEXT NOT NULL,
    object_key TEXT NOT NULL CHECK (object_key <> ''),
    sha256_hex TEXT NOT NULL CHECK (char_length(sha256_hex) = 64),
    byte_size BIGINT NOT NULL CHECK (byte_size > 0),
    media_type TEXT NOT NULL CHECK (media_type LIKE 'image/%'),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (storage_backend, object_key)
);

CREATE UNIQUE INDEX artifacts_job_output_uidx
    ON artifacts (job_id, output_index);

CREATE UNIQUE INDEX artifacts_execution_output_uidx
    ON artifacts (execution_id, output_index);
