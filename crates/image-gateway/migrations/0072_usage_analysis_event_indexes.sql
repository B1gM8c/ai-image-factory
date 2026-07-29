CREATE INDEX customer_rated_usage_lines_created_job_idx
    ON customer_rated_usage_lines (created_at_ms, job_id);

CREATE INDEX rated_usage_job_created_idx
    ON rated_usage (job_id, created_at_ms);
