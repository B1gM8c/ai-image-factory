ALTER TABLE gateway_projects
    ADD COLUMN file_storage_limit_bytes BIGINT NOT NULL DEFAULT 2147483648
        CHECK (file_storage_limit_bytes > 0),
    ADD COLUMN file_storage_limit_count INTEGER NOT NULL DEFAULT 1000
        CHECK (file_storage_limit_count > 0);

ALTER TABLE project_files
    ADD COLUMN cleanup_completed_at_ms BIGINT;

ALTER TABLE project_files
    ADD CONSTRAINT project_files_cleanup_completion_check CHECK (
        cleanup_completed_at_ms IS NULL
        OR (
            state = 'deleted'
            AND deleted_at_ms IS NOT NULL
            AND cleanup_completed_at_ms >= deleted_at_ms
            AND cleanup_lease_owner IS NULL
            AND cleanup_lease_expires_at_ms IS NULL
        )
    );

CREATE INDEX project_files_project_storage_pending_idx
    ON project_files(tenant_id, project_id)
    INCLUDE (byte_size)
    WHERE cleanup_completed_at_ms IS NULL;

CREATE INDEX project_files_cleanup_recovery_idx
    ON project_files(
        COALESCE(cleanup_lease_expires_at_ms, 0),
        COALESCE(deleted_at_ms, expires_at_ms),
        file_id
    )
    WHERE cleanup_completed_at_ms IS NULL
      AND (state = 'deleted' OR expires_at_ms IS NOT NULL);
