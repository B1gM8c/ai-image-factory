-- Bbox analysis is an optional sidecar. Keep its storage and queue independent
-- from image generation jobs so sidecar failure cannot affect image delivery.
CREATE TABLE media_segment_assets (
    asset_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL CHECK (tenant_id <> ''),
    project_id TEXT NOT NULL CHECK (project_id <> ''),
    owner_id TEXT NOT NULL,
    digest TEXT NOT NULL CHECK (digest ~ '^[0-9a-f]{64}$'),
    metadata JSONB NOT NULL CHECK (jsonb_typeof(metadata) = 'object'),
    byte_size BIGINT NOT NULL CHECK (byte_size > 0 AND byte_size <= 20971520),
    expires_at_ms BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    CONSTRAINT media_segment_assets_expiry_check
        CHECK (expires_at_ms > created_at_ms),
    CONSTRAINT media_segment_assets_scope_digest_key
        UNIQUE (tenant_id, project_id, owner_id, digest),
    CONSTRAINT media_segment_assets_scope_identity_key
        UNIQUE (asset_id, tenant_id, project_id, owner_id)
);

CREATE INDEX media_segment_assets_project_quota_idx
    ON media_segment_assets (tenant_id, project_id, expires_at_ms)
    INCLUDE (byte_size);

CREATE INDEX media_segment_assets_expiry_idx
    ON media_segment_assets (expires_at_ms, asset_id);

CREATE TABLE media_segment_results (
    result_id UUID PRIMARY KEY,
    asset_id UUID NOT NULL,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    cache_key TEXT NOT NULL CHECK (cache_key ~ '^[0-9a-f]{64}$'),
    analyzer_key TEXT NOT NULL CHECK (analyzer_key ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL CHECK (status IN ('queued', 'processing', 'completed', 'failed')),
    lease_token UUID,
    lease_deadline_at_ms BIGINT,
    queue_deadline_at_ms BIGINT NOT NULL,
    result_json JSONB NOT NULL CHECK (jsonb_typeof(result_json) = 'object'),
    timings_json JSONB,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    CONSTRAINT media_segment_results_asset_scope_fk
        FOREIGN KEY (asset_id, tenant_id, project_id, owner_id)
        REFERENCES media_segment_assets (asset_id, tenant_id, project_id, owner_id)
        ON DELETE CASCADE,
    CONSTRAINT media_segment_results_scope_cache_key
        UNIQUE (tenant_id, project_id, owner_id, cache_key),
    CONSTRAINT media_segment_results_time_check
        CHECK (queue_deadline_at_ms > created_at_ms AND updated_at_ms >= created_at_ms),
    CONSTRAINT media_segment_results_lease_shape_check CHECK (
        (status = 'processing'
            AND lease_token IS NOT NULL
            AND lease_deadline_at_ms IS NOT NULL
            AND lease_deadline_at_ms > updated_at_ms)
        OR
        (status <> 'processing'
            AND lease_token IS NULL
            AND lease_deadline_at_ms IS NULL)
    ),
    CONSTRAINT media_segment_results_timings_shape_check CHECK (
        timings_json IS NULL OR jsonb_typeof(timings_json) = 'object'
    )
);

CREATE INDEX media_segment_results_claim_idx
    ON media_segment_results (analyzer_key, created_at_ms, result_id)
    WHERE status = 'queued';

CREATE INDEX media_segment_results_active_project_idx
    ON media_segment_results (tenant_id, project_id, status)
    WHERE status IN ('queued', 'processing');

CREATE INDEX media_segment_results_lease_expiry_idx
    ON media_segment_results (lease_deadline_at_ms, result_id)
    WHERE status = 'processing';

CREATE INDEX media_segment_results_queue_expiry_idx
    ON media_segment_results (queue_deadline_at_ms, result_id)
    WHERE status = 'queued';
