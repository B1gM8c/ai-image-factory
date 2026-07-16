SET LOCAL lock_timeout = '5s';

LOCK TABLE provider_remote_submit_intents IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE provider_remote_submit_intents
    ADD COLUMN IF NOT EXISTS output_index INTEGER,
    ADD COLUMN IF NOT EXISTS output_total INTEGER;

ALTER TABLE provider_remote_submit_intents
    DISABLE TRIGGER provider_submit_intent_update_guard;

UPDATE provider_remote_submit_intents intent
SET output_index = job_output.output_index,
    output_total = job.requested_units
FROM provider_submissions submission
JOIN job_outputs job_output
  ON job_output.output_id = submission.output_id
 AND job_output.job_id = submission.job_id
JOIN jobs job ON job.job_id = submission.job_id
WHERE submission.submission_id = intent.submission_id
  AND submission.executor_execution_id = intent.executor_execution_id
  AND (intent.output_index IS NULL OR intent.output_total IS NULL);

SET CONSTRAINTS ALL IMMEDIATE;

ALTER TABLE provider_remote_submit_intents
    ENABLE TRIGGER provider_submit_intent_update_guard;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_remote_submit_intents
        WHERE output_index IS NULL OR output_total IS NULL
    ) THEN
        RAISE EXCEPTION
            'atomic submit migration could not bind existing output projections';
    END IF;
END;
$$;

ALTER TABLE provider_remote_submit_intents
    ALTER COLUMN output_index SET NOT NULL,
    ALTER COLUMN output_total SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'provider_remote_submit_intents'::regclass
          AND conname = 'provider_remote_submit_intents_output_projection_check'
    ) THEN
        ALTER TABLE provider_remote_submit_intents
            ADD CONSTRAINT provider_remote_submit_intents_output_projection_check
            CHECK (output_total > 0 AND output_index >= 0 AND output_index < output_total);
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS provider_submit_quarantined_receipts (
    event_identity TEXT PRIMARY KEY CHECK (
        char_length(event_identity) BETWEEN 1 AND 255
        AND event_identity ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
        AND event_identity NOT LIKE '%://%'
    ),
    submission_id UUID NOT NULL REFERENCES
        provider_remote_submit_intents(submission_id) ON DELETE RESTRICT,
    executor_execution_id UUID NOT NULL,
    execution_binding_sha256 CHAR(64) NOT NULL CHECK (
        execution_binding_sha256 ~ '^[0-9a-f]{64}$'
    ),
    expected_provider_id TEXT NOT NULL CHECK (
        expected_provider_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
        AND expected_provider_id NOT LIKE '%://%'
    ),
    observed_provider_id TEXT NOT NULL CHECK (
        observed_provider_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
        AND observed_provider_id NOT LIKE '%://%'
    ),
    observed_submission_id TEXT NOT NULL CHECK (
        observed_submission_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
        AND observed_submission_id NOT LIKE '%://%'
    ),
    remote_operation_id TEXT NOT NULL CHECK (
        remote_operation_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
        AND remote_operation_id NOT LIKE '%://%'
    ),
    provider_request_id TEXT CHECK (
        provider_request_id IS NULL
        OR (
            provider_request_id ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,254}$'
            AND provider_request_id NOT LIKE '%://%'
        )
    ),
    reason TEXT NOT NULL CHECK (reason ~ '^[A-Za-z0-9_.-]{1,128}$'),
    recorded_at_ms BIGINT NOT NULL CHECK (recorded_at_ms > 0),
    UNIQUE (submission_id, event_identity)
);

CREATE OR REPLACE FUNCTION validate_provider_submit_quarantined_receipt_insert()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM provider_remote_submit_intents intent
        WHERE intent.submission_id = NEW.submission_id
          AND intent.executor_execution_id = NEW.executor_execution_id
          AND intent.execution_binding_sha256 = NEW.execution_binding_sha256
          AND intent.provider_id = NEW.expected_provider_id
          AND intent.state IN ('sending', 'outcome_unknown')
    ) THEN
        RAISE EXCEPTION
            'quarantined receipt requires its exact active submit binding';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION reject_provider_submit_quarantined_receipt_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'provider submit quarantined receipts are append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS provider_submit_quarantined_receipt_insert_guard
    ON provider_submit_quarantined_receipts;
CREATE TRIGGER provider_submit_quarantined_receipt_insert_guard
    BEFORE INSERT ON provider_submit_quarantined_receipts
    FOR EACH ROW EXECUTE FUNCTION validate_provider_submit_quarantined_receipt_insert();

DROP TRIGGER IF EXISTS provider_submit_quarantined_receipts_reject_update
    ON provider_submit_quarantined_receipts;
CREATE TRIGGER provider_submit_quarantined_receipts_reject_update
    BEFORE UPDATE OR DELETE ON provider_submit_quarantined_receipts
    FOR EACH ROW EXECUTE FUNCTION reject_provider_submit_quarantined_receipt_mutation();

DROP TRIGGER IF EXISTS provider_submit_quarantined_receipts_reject_truncate
    ON provider_submit_quarantined_receipts;
CREATE TRIGGER provider_submit_quarantined_receipts_reject_truncate
    BEFORE TRUNCATE ON provider_submit_quarantined_receipts
    FOR EACH STATEMENT EXECUTE FUNCTION reject_provider_submit_quarantined_receipt_mutation();

CREATE OR REPLACE FUNCTION enforce_provider_submit_output_projection_identity()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.output_index IS DISTINCT FROM OLD.output_index
       OR NEW.output_total IS DISTINCT FROM OLD.output_total THEN
        RAISE EXCEPTION 'provider submit output projection is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS provider_submit_output_projection_identity
    ON provider_remote_submit_intents;
CREATE TRIGGER provider_submit_output_projection_identity
    BEFORE UPDATE ON provider_remote_submit_intents
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_submit_output_projection_identity();

CREATE OR REPLACE FUNCTION validate_provider_submit_intent_insert() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF NEW.state NOT IN ('reserved', 'sending')
       OR NEW.remote_operation_id IS NOT NULL
       OR NEW.provider_request_id IS NOT NULL
       OR NEW.receipt_event_identity IS NOT NULL
       OR NEW.failure_event_identity IS NOT NULL
       OR NEW.failure_error_code IS NOT NULL
       OR NEW.output_total <= 0
       OR NEW.output_index < 0
       OR NEW.output_index >= NEW.output_total
       OR (NEW.state = 'reserved' AND NEW.send_started_at_ms IS NOT NULL)
       OR (NEW.state = 'sending'
           AND (NEW.send_started_at_ms IS DISTINCT FROM NEW.created_at_ms
                OR NEW.updated_at_ms IS DISTINCT FROM NEW.created_at_ms))
       OR NOT EXISTS (
            SELECT 1
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            JOIN job_outputs job_output
              ON job_output.output_id = submission.output_id
             AND job_output.job_id = submission.job_id
            JOIN jobs job ON job.job_id = submission.job_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = execution.executor_execution_id
             AND allocation.submission_id = execution.submission_id
            WHERE execution.executor_execution_id = NEW.executor_execution_id
              AND execution.submission_id = NEW.submission_id
              AND execution.state = 'running'
              AND submission.state = 'running'
              AND execution.executor_owner = NEW.submit_owner
              AND execution.lease_epoch = NEW.submit_lease_epoch
              AND execution.launch_owner = NEW.submit_owner
              AND execution.launch_lease_epoch = NEW.submit_lease_epoch
              AND execution.lease_expires_at_ms > now_ms
              AND submission.provider_id = NEW.provider_id
              AND submission.provider_account_id = NEW.provider_account_id
              AND submission.operation_binding_version = 2
              AND submission.completion_mode = 'remote_task'
              AND submission.idempotency_mode = 'submission_bound'
              AND job_output.output_index = NEW.output_index
              AND job.requested_units = NEW.output_total
              AND allocation.state = 'held'
       ) THEN
        RAISE EXCEPTION
            'provider submit acquisition requires an exact remote operation binding';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER provider_submit_intent_recovery_projection_check
    ON provider_remote_submit_intents;

CREATE CONSTRAINT TRIGGER provider_submit_intent_recovery_projection_check
    AFTER INSERT OR UPDATE ON provider_remote_submit_intents
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_submit_recovery_projection();
