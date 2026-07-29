CREATE INDEX jobs_admin_operation_created_idx
    ON jobs (operation, created_at_ms DESC, job_id DESC);

CREATE INDEX jobs_admin_model_created_idx
    ON jobs (model, created_at_ms DESC, job_id DESC);
