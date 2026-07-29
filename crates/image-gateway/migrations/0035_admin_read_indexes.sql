CREATE INDEX jobs_admin_global_created_idx
    ON jobs (created_at_ms DESC, job_id DESC);

CREATE INDEX jobs_admin_provider_created_idx
    ON jobs (provider_id, created_at_ms DESC, job_id DESC);

CREATE INDEX jobs_admin_state_created_idx
    ON jobs (state, created_at_ms DESC, job_id DESC);

CREATE INDEX jobs_admin_request_id_idx
    ON jobs (request_id);

CREATE INDEX jobs_admin_uncertain_updated_idx
    ON jobs (updated_at_ms, job_id)
    WHERE state = 'uncertain';

CREATE INDEX usage_events_admin_created_idx
    ON usage_events (created_at_ms, billing_metric, billing_unit, outcome);

CREATE INDEX rated_usage_admin_created_idx
    ON rated_usage (created_at_ms, job_id);

CREATE INDEX provider_receipts_admin_created_idx
    ON provider_receipts (created_at_ms, job_id);

CREATE INDEX ledger_transaction_seals_admin_sealed_idx
    ON ledger_transaction_seals (sealed_at_ms, transaction_id);

CREATE INDEX work_items_admin_awaiting_executor_idx
    ON work_items (updated_at_ms, work_item_id)
    WHERE state = 'awaiting_executor';

CREATE INDEX work_items_admin_uncertain_updated_idx
    ON work_items (updated_at_ms, work_item_id)
    WHERE state = 'uncertain';

CREATE INDEX provider_remote_tasks_admin_uncertain_terminal_idx
    ON provider_remote_tasks (terminal_at_ms, submission_id)
    WHERE state = 'uncertain';
