-- Input ownership is independent of result retention. Existing blobs remain owned
-- until the worker confirms deletion; expiry alone never frees storage quota.
ALTER TABLE media_segment_assets
    ADD COLUMN source_state TEXT NOT NULL DEFAULT 'retained'
        CHECK (source_state IN ('retained', 'releasing', 'released')),
    ADD COLUMN source_release_attempt_at_ms BIGINT NOT NULL DEFAULT 0,
    -- Restored pixels must survive the registration -> new enqueue interval;
    -- old terminal results must not immediately release that new source again.
    ADD COLUMN source_waiting_for_result BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX media_segment_assets_owned_source_idx
    ON media_segment_assets (tenant_id, project_id)
    INCLUDE (byte_size)
    WHERE source_state <> 'released';

CREATE INDEX media_segment_assets_source_cleanup_idx
    ON media_segment_assets (source_release_attempt_at_ms, expires_at_ms, asset_id)
    WHERE source_state <> 'released';

CREATE INDEX media_segment_results_asset_status_idx
    ON media_segment_results (asset_id, status);

CREATE INDEX media_segment_results_asset_analyzer_idx
    ON media_segment_results (asset_id, analyzer_key);

CREATE TABLE media_segment_worker_heartbeats (
    analyzer_key TEXT NOT NULL CHECK (analyzer_key ~ '^[0-9a-f]{64}$'),
    observed_at_ms BIGINT NOT NULL,
    release_terminal_sources BOOLEAN NOT NULL,
    -- Keep both rollout modes visible until their heartbeats expire; a worker
    -- in the old mode must not be hidden by the last writer during rollback.
    PRIMARY KEY (analyzer_key, release_terminal_sources)
);

CREATE INDEX media_segment_worker_heartbeats_fresh_idx
    ON media_segment_worker_heartbeats (observed_at_ms)
    INCLUDE (release_terminal_sources);
