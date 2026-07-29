ALTER TABLE project_batches
    ADD COLUMN result_bytes BIGINT NOT NULL DEFAULT 0
        CHECK (result_bytes >= 0);
