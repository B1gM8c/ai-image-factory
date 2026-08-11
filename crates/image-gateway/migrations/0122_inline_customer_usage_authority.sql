DROP TRIGGER provider_usage_facts_reject_mutation
    ON provider_usage_facts;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT con.conname
        FROM pg_constraint con
        WHERE con.conrelid = 'provider_usage_facts'::regclass
          AND con.contype = 'f'
          AND con.confrelid IN (
              'provider_receipts'::regclass,
              'provider_submissions'::regclass
          )
    LOOP
        EXECUTE format(
            'ALTER TABLE provider_usage_facts DROP CONSTRAINT %I',
            constraint_name
        );
    END LOOP;
END;
$$;

ALTER TABLE provider_usage_facts
    ALTER COLUMN submission_id DROP NOT NULL,
    ALTER COLUMN receipt_id DROP NOT NULL,
    ADD COLUMN attempt_execution_id UUID,
    ADD CONSTRAINT provider_usage_facts_authority_check CHECK (
        (
            submission_id IS NOT NULL
            AND receipt_id IS NOT NULL
            AND attempt_execution_id IS NULL
        )
        OR (
            submission_id IS NULL
            AND receipt_id IS NULL
            AND attempt_execution_id IS NOT NULL
        )
    ),
    ADD CONSTRAINT provider_usage_facts_output_job_fk
        FOREIGN KEY (output_id, job_id)
        REFERENCES job_outputs(output_id, job_id) ON DELETE RESTRICT,
    ADD CONSTRAINT provider_usage_facts_durable_receipt_fk
        FOREIGN KEY (receipt_id, submission_id, output_id, job_id)
        REFERENCES provider_receipts(
            receipt_id, submission_id, output_id, job_id
        ) ON DELETE RESTRICT,
    ADD CONSTRAINT provider_usage_facts_durable_submission_fk
        FOREIGN KEY (submission_id, output_id, job_id, provider_id)
        REFERENCES provider_submissions(
            submission_id, output_id, job_id, provider_id
        ) ON DELETE RESTRICT,
    ADD CONSTRAINT provider_usage_facts_inline_attempt_fk
        FOREIGN KEY (attempt_execution_id)
        REFERENCES job_attempts(execution_id) ON DELETE RESTRICT;

CREATE INDEX provider_usage_facts_inline_attempt_idx
    ON provider_usage_facts(attempt_execution_id)
    WHERE attempt_execution_id IS NOT NULL;

CREATE TRIGGER provider_usage_facts_reject_mutation
BEFORE UPDATE OR DELETE ON provider_usage_facts
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE OR REPLACE FUNCTION validate_v4_provider_usage_fact_insert() RETURNS TRIGGER AS $$
DECLARE
    contract_version SMALLINT;
    quote_tenant_id TEXT;
    quote_currency TEXT;
    canonical_outcome TEXT;
    canonical_account_id UUID;
    canonical_provider_id TEXT;
    canonical_job_state TEXT;
    canonical_quota_state TEXT;
    canonical_requested_units INTEGER;
    canonical_charged_units INTEGER;
    canonical_committed_units INTEGER;
    canonical_released_units INTEGER;
BEGIN
    SELECT job.economics_contract_version
      INTO STRICT contract_version
    FROM jobs job
    WHERE job.job_id = NEW.job_id;

    IF NEW.attempt_execution_id IS NULL THEN
        SELECT receipt.outcome, submission.provider_account_id,
               submission.provider_id
          INTO STRICT canonical_outcome, canonical_account_id,
                      canonical_provider_id
        FROM provider_receipts receipt
        JOIN provider_submissions submission
          ON submission.submission_id = receipt.submission_id
         AND submission.output_id = receipt.output_id
         AND submission.job_id = receipt.job_id
         AND submission.provider_id = receipt.provider_id
        WHERE receipt.receipt_id = NEW.receipt_id
          AND receipt.submission_id = NEW.submission_id
          AND receipt.output_id = NEW.output_id
          AND receipt.job_id = NEW.job_id
          AND receipt.provider_id = NEW.provider_id;
    ELSE
        IF contract_version <> 4 OR NEW.fact_domain <> 'customer_billable' THEN
            RAISE EXCEPTION 'inline usage authority is customer billing only'
                USING ERRCODE = '23514';
        END IF;
        SELECT
            CASE attempt.state
                WHEN 'succeeded' THEN 'succeeded'
                WHEN 'failed' THEN 'failed'
                ELSE NULL
            END,
            profile.provider_account_id,
            profile.provider_id,
            job.state,
            quota.state,
            job.requested_units,
            job.charged_units,
            quota.committed_units,
            quota.released_units
          INTO STRICT canonical_outcome, canonical_account_id,
                      canonical_provider_id, canonical_job_state,
                      canonical_quota_state, canonical_requested_units,
                      canonical_charged_units, canonical_committed_units,
                      canonical_released_units
        FROM job_attempts attempt
        JOIN work_items work
          ON work.work_item_id = attempt.work_item_id
         AND work.job_id = NEW.job_id
         AND work.execution_id = attempt.execution_id
        JOIN provider_execution_profiles profile
          ON profile.execution_profile_id = work.execution_profile_id
        JOIN jobs job
          ON job.job_id = work.job_id
        JOIN quota_reservations quota
          ON quota.reservation_id = job.reservation_id
         AND quota.job_id = job.job_id
         AND quota.tenant_id = job.tenant_id
        WHERE attempt.execution_id = NEW.attempt_execution_id
          AND attempt.state IN ('succeeded', 'failed');

        IF canonical_requested_units <= 0
           OR (
               canonical_outcome = 'succeeded'
               AND (
                   canonical_job_state <> 'succeeded'
                   OR canonical_quota_state <> 'committed'
                   OR canonical_charged_units <> canonical_requested_units
                   OR canonical_committed_units <> canonical_requested_units
                   OR canonical_released_units <> 0
               )
           )
           OR (
               canonical_outcome = 'failed'
               AND (
                   canonical_job_state <> 'failed'
                   OR canonical_quota_state <> 'released'
                   OR canonical_charged_units <> 0
                   OR canonical_committed_units <> 0
                   OR canonical_released_units <> canonical_requested_units
               )
           ) THEN
            RAISE EXCEPTION 'inline usage authority conflicts with terminal economics'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    IF NEW.provider_account_id IS DISTINCT FROM canonical_account_id
       OR NEW.provider_id IS DISTINCT FROM canonical_provider_id
       OR (
           NEW.metric <> 'provider_reported_cost'
           AND NEW.terminal_outcome IS DISTINCT FROM canonical_outcome
       ) THEN
        RAISE EXCEPTION 'provider usage fact conflicts with canonical authority'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.metric = 'provider_reported_cost'
       AND NEW.fact_domain <> 'provider_actual' THEN
        RAISE EXCEPTION 'provider cost fact has an invalid domain'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.quantity_source = 'media_inspected'
       AND (
           NEW.fact_domain <> 'provider_benchmark'
           OR NEW.metric <> 'video_output_second'
           OR NEW.unit <> 'second'
           OR NEW.confidence <> 'exact'
           OR NEW.quantity <= 0
           OR NEW.terminal_outcome <> 'succeeded'
       ) THEN
        RAISE EXCEPTION 'media inspected fact has an invalid domain'
            USING ERRCODE = '23514';
    END IF;

    IF contract_version = 4
       AND NEW.fact_domain = 'customer_billable' THEN
        SELECT tenant_id, currency
          INTO STRICT quote_tenant_id, quote_currency
        FROM customer_price_quotes
        WHERE job_id = NEW.job_id;
        PERFORM pg_advisory_xact_lock(
            hashtextextended('budget:' || quote_tenant_id || ':' || quote_currency, 0)
        );
        PERFORM 1 FROM jobs WHERE job_id = NEW.job_id FOR UPDATE;
        IF EXISTS (
            SELECT 1 FROM customer_rated_usage WHERE job_id = NEW.job_id
        ) THEN
            RAISE EXCEPTION 'settled customer usage facts are immutable'
                USING ERRCODE = '55000';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
