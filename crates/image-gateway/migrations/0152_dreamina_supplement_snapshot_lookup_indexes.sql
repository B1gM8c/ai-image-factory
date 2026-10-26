-- The frozen evidence snapshot resolves UUIDs supplied by JSON as text. Keep
-- those lookups indexed without changing its serialized evidence or hash.
CREATE INDEX IF NOT EXISTS provider_submissions_submission_id_text_lookup
    ON provider_submissions ((submission_id::text)) INCLUDE (job_id);

CREATE INDEX IF NOT EXISTS work_items_work_item_id_text_lookup
    ON work_items ((work_item_id::text)) INCLUDE (job_id);
