CREATE TABLE gateway_request_observations (
    request_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    method TEXT NOT NULL,
    route_pattern TEXT NOT NULL,
    request_path TEXT NOT NULL,
    status_code SMALLINT NOT NULL,
    duration_ms BIGINT NOT NULL,
    error_code TEXT,
    idempotency_key_digest TEXT,
    tenant_id TEXT,
    project_id TEXT,
    service_account_id TEXT,
    api_key_id TEXT,
    actor_user_id UUID,
    auth_kind TEXT,
    job_id UUID,
    content_captured BOOLEAN NOT NULL DEFAULT FALSE,
    content_expires_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    completed_at_ms BIGINT NOT NULL,
    CONSTRAINT gateway_request_observations_source_check
        CHECK (source IN ('models', 'images', 'videos', 'files')),
    CONSTRAINT gateway_request_observations_method_check
        CHECK (method IN ('GET', 'POST', 'PUT', 'PATCH', 'DELETE')),
    CONSTRAINT gateway_request_observations_status_check
        CHECK (status_code BETWEEN 100 AND 599),
    CONSTRAINT gateway_request_observations_duration_check
        CHECK (duration_ms >= 0),
    CONSTRAINT gateway_request_observations_idempotency_digest_check
        CHECK (
            idempotency_key_digest IS NULL
            OR char_length(idempotency_key_digest) = 64
        ),
    CONSTRAINT gateway_request_observations_content_shape_check
        CHECK (
            (NOT content_captured AND content_expires_at_ms IS NULL)
            OR (content_captured AND content_expires_at_ms IS NOT NULL)
        ),
    CONSTRAINT gateway_request_observations_time_check
        CHECK (completed_at_ms >= created_at_ms)
);

CREATE INDEX gateway_request_observations_created_idx
    ON gateway_request_observations (created_at_ms DESC, request_id DESC);

CREATE INDEX gateway_request_observations_project_created_idx
    ON gateway_request_observations (project_id, created_at_ms DESC, request_id DESC)
    WHERE project_id IS NOT NULL;

CREATE INDEX gateway_request_observations_project_source_created_idx
    ON gateway_request_observations (
        project_id, source, created_at_ms DESC, request_id DESC
    )
    WHERE project_id IS NOT NULL;

CREATE INDEX gateway_request_observations_actor_created_idx
    ON gateway_request_observations (actor_user_id, created_at_ms DESC, request_id DESC)
    WHERE actor_user_id IS NOT NULL;

CREATE INDEX gateway_request_observations_api_key_created_idx
    ON gateway_request_observations (api_key_id, created_at_ms DESC, request_id DESC)
    WHERE api_key_id IS NOT NULL;

CREATE INDEX gateway_request_observations_project_status_created_idx
    ON gateway_request_observations (
        project_id, status_code, created_at_ms DESC, request_id DESC
    )
    WHERE project_id IS NOT NULL;

INSERT INTO gateway_request_observations (
    request_id,
    source,
    method,
    route_pattern,
    request_path,
    status_code,
    duration_ms,
    error_code,
    tenant_id,
    project_id,
    service_account_id,
    api_key_id,
    actor_user_id,
    auth_kind,
    job_id,
    created_at_ms,
    completed_at_ms
)
SELECT
    job.request_id,
    CASE
        WHEN job.operation = 'video_generation' THEN 'videos'
        ELSE 'images'
    END,
    'POST',
    CASE job.operation
        WHEN 'edit' THEN '/v1/images/edits'
        WHEN 'video_generation' THEN '/v1/videos/generations'
        ELSE '/v1/images/generations'
    END,
    CASE job.operation
        WHEN 'edit' THEN '/v1/images/edits'
        WHEN 'video_generation' THEN '/v1/videos/generations'
        ELSE '/v1/images/generations'
    END,
    CASE job.state
        WHEN 'succeeded' THEN 200
        WHEN 'queued' THEN 202
        WHEN 'running' THEN 202
        WHEN 'failed' THEN 500
        WHEN 'uncertain' THEN 503
        ELSE 500
    END,
    GREATEST(
        COALESCE(job.finished_at_ms, job.updated_at_ms) - job.created_at_ms,
        0
    ),
    job.last_error_code,
    job.tenant_id,
    attribution.project_id,
    attribution.service_account_id,
    attribution.api_key_id,
    attribution.actor_user_id,
    attribution.auth_kind,
    job.job_id,
    job.created_at_ms,
    GREATEST(
        COALESCE(job.finished_at_ms, job.updated_at_ms),
        job.created_at_ms
    )
FROM jobs job
LEFT JOIN job_auth_attributions attribution
  ON attribution.job_id = job.job_id
ON CONFLICT (request_id) DO NOTHING;
