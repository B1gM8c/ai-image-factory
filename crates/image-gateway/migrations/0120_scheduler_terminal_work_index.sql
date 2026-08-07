CREATE INDEX provider_submissions_admin_work_terminal_idx
    ON provider_submissions (work_item_id, submission_id, executor_execution_id);
