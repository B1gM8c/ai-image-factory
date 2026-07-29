-- Extend the durable executor pipeline to the independently rated v4 contract.
-- The previous functions are intentionally patched from their migration-defined
-- bodies so v2/v3 behavior remains byte-for-byte equivalent apart from the
-- accepted contract set.
DO $$
DECLARE
    target REGPROCEDURE;
    definition TEXT;
    patched TEXT;
BEGIN
    FOREACH target IN ARRAY ARRAY[
        'validate_executor_handoff()'::REGPROCEDURE,
        'enqueue_executor_terminal_reduction()'::REGPROCEDURE,
        'validate_executor_terminal_reduction_projection()'::REGPROCEDURE,
        'assert_terminal_quota_accounting(uuid)'::REGPROCEDURE,
        'assert_terminal_parent_completion(uuid)'::REGPROCEDURE,
        'validate_terminal_quota_update()'::REGPROCEDURE,
        'assert_v2_parent_if_present(uuid)'::REGPROCEDURE,
        'protect_v2_idempotency_binding()'::REGPROCEDURE
    ]
    LOOP
        definition := pg_get_functiondef(target);
        patched := replace(definition, '(2, 3)', '(2, 3, 4)');
        IF patched = definition THEN
            RAISE EXCEPTION 'expected v2/v3 contract guard in %', target;
        END IF;
        EXECUTE patched;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION assert_v2_parent_if_present(target_job_id UUID)
RETURNS VOID AS $$
BEGIN
    IF target_job_id IS NOT NULL AND (
        EXISTS (
            SELECT 1
            FROM provider_submissions submission
            JOIN jobs job ON job.job_id = submission.job_id
            WHERE submission.job_id = target_job_id
              AND job.economics_contract_version IN (2, 3)
        )
        OR EXISTS (
            SELECT 1
            FROM executor_terminal_reductions reduction
            JOIN provider_submissions submission
              ON submission.submission_id = reduction.submission_id
            JOIN jobs job ON job.job_id = submission.job_id
            WHERE submission.job_id = target_job_id
              AND job.economics_contract_version = 4
        )
    ) THEN
        PERFORM assert_terminal_parent_completion(target_job_id);
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_executor_terminal_reduction_completion()
RETURNS TRIGGER AS $$
DECLARE
    receipt_row provider_receipts%ROWTYPE;
    artifact_row artifacts%ROWTYPE;
    submission_row provider_submissions%ROWTYPE;
    contract_version SMALLINT;
    decision_error_code TEXT;
    output_index_value INTEGER;
    authority_id_value UUID;
    authority_backend TEXT;
    authority_sha256 TEXT;
    authority_byte_size BIGINT;
    authority_media_type TEXT;
    expected_receipt_outcome TEXT;
    expected_object_key TEXT;
    reservation_id_value UUID;
    economic_meter_count BIGINT;
    rating_count BIGINT;
    hold_state_value TEXT;
    usage_fact_count BIGINT;
    invalid_usage_fact_count BIGINT;
BEGIN
    IF NEW.state <> 'completed' THEN
        RETURN NULL;
    END IF;

    SELECT submission.* INTO STRICT submission_row
    FROM provider_submissions submission
    WHERE submission.submission_id = NEW.submission_id
      AND submission.executor_execution_id = NEW.executor_execution_id
      AND submission.resolution_decision_id = NEW.resolution_decision_id
      AND submission.state = NEW.resolved_state;

    SELECT decision.error_code INTO decision_error_code
    FROM executor_resolution_decisions decision
    WHERE decision.decision_id = NEW.resolution_decision_id
      AND decision.executor_execution_id = NEW.executor_execution_id
      AND decision.submission_id = NEW.submission_id
      AND decision.resolved_state = NEW.resolved_state;

    expected_receipt_outcome := CASE
        WHEN NEW.resolved_state = 'succeeded' THEN 'succeeded'
        WHEN NEW.resolved_state = 'uncertain' THEN 'uncertain'
        WHEN NEW.resolved_state = 'canceled' THEN 'no_effect'
        WHEN decision_error_code = 'provider_no_effect' THEN 'no_effect'
        ELSE 'failed'
    END;

    SELECT receipt.* INTO STRICT receipt_row
    FROM provider_receipts receipt
    WHERE receipt.receipt_id = NEW.provider_receipt_id;
    IF receipt_row.submission_id IS DISTINCT FROM NEW.submission_id
       OR receipt_row.output_id IS DISTINCT FROM submission_row.output_id
       OR receipt_row.job_id IS DISTINCT FROM submission_row.job_id
       OR receipt_row.provider_id IS DISTINCT FROM submission_row.provider_id
       OR receipt_row.outcome IS DISTINCT FROM expected_receipt_outcome
       OR receipt_row.receipt_schema IS DISTINCT FROM 'executor.resolution.v1'
       OR receipt_row.evidence ->> 'submission_id'
            IS DISTINCT FROM NEW.submission_id::TEXT
       OR receipt_row.evidence ->> 'executor_execution_id'
            IS DISTINCT FROM NEW.executor_execution_id::TEXT
       OR receipt_row.evidence ->> 'resolution_decision_id'
            IS DISTINCT FROM NEW.resolution_decision_id::TEXT
       OR receipt_row.evidence ->> 'resolved_state'
            IS DISTINCT FROM NEW.resolved_state THEN
        RAISE EXCEPTION 'completed terminal reduction receipt is not canonical';
    END IF;

    SELECT job.reservation_id, job.economics_contract_version
      INTO STRICT reservation_id_value, contract_version
    FROM jobs job
    WHERE job.job_id = submission_row.job_id
      AND job.tenant_id = submission_row.tenant_id
      AND job.economics_contract_version IN (2, 3, 4);
    IF NEW.quota_reservation_id IS DISTINCT FROM reservation_id_value THEN
        RAISE EXCEPTION 'completed terminal reduction quota reservation is not canonical';
    END IF;

    IF contract_version IN (2, 3) THEN
        SELECT COUNT(*),
               COUNT(rating.rated_usage_id),
               MIN(hold.state)
          INTO economic_meter_count, rating_count, hold_state_value
        FROM economic_metering_events meter
        LEFT JOIN rated_usage rating
          ON rating.meter_event_id = meter.meter_event_id
         AND rating.output_id = meter.output_id
         AND rating.job_id = meter.job_id
        JOIN output_holds hold
          ON hold.output_id = meter.output_id AND hold.job_id = meter.job_id
        WHERE meter.receipt_id = NEW.provider_receipt_id
          AND meter.submission_id = NEW.submission_id
          AND meter.output_id = submission_row.output_id
          AND meter.job_id = submission_row.job_id
          AND meter.outcome = expected_receipt_outcome;
        IF economic_meter_count <> 1
           OR (expected_receipt_outcome = 'uncertain'
               AND (rating_count <> 0 OR hold_state_value <> 'held'))
           OR (expected_receipt_outcome <> 'uncertain'
               AND (rating_count <> 1 OR hold_state_value <> 'settled')) THEN
            RAISE EXCEPTION 'completed terminal reduction economic facts are incomplete';
        END IF;
    ELSE
        SELECT COUNT(*) FILTER (WHERE metric <> 'provider_reported_cost'),
               COUNT(*) FILTER (
                   WHERE metric <> 'provider_reported_cost'
                     AND terminal_outcome IS DISTINCT FROM expected_receipt_outcome
               )
          INTO usage_fact_count, invalid_usage_fact_count
        FROM provider_usage_facts
        WHERE receipt_id = NEW.provider_receipt_id
          AND submission_id = NEW.submission_id
          AND output_id = submission_row.output_id
          AND job_id = submission_row.job_id;
        IF (expected_receipt_outcome = 'uncertain' AND usage_fact_count <> 0)
           OR (
               expected_receipt_outcome <> 'uncertain'
               AND (usage_fact_count = 0 OR invalid_usage_fact_count <> 0)
           ) THEN
            RAISE EXCEPTION 'v4 terminal reduction usage facts are incomplete';
        END IF;
    END IF;

    IF NEW.resolved_state = 'succeeded' THEN
        SELECT output.output_index, authority.authority_id,
               authority.storage_backend, authority.sha256_hex,
               authority.byte_size, authority.media_type
          INTO STRICT output_index_value, authority_id_value, authority_backend,
               authority_sha256, authority_byte_size, authority_media_type
        FROM job_outputs output
        JOIN executor_result_manifests manifest
          ON manifest.manifest_id = submission_row.result_manifest_id
         AND manifest.executor_execution_id = NEW.executor_execution_id
         AND manifest.submission_id = NEW.submission_id
        JOIN executor_artifact_authorities authority
          ON authority.authority_id = manifest.artifact_authority_id
         AND authority.executor_execution_id = NEW.executor_execution_id
         AND authority.submission_id = NEW.submission_id
        WHERE output.output_id = submission_row.output_id
          AND output.job_id = submission_row.job_id;

        SELECT artifact.* INTO STRICT artifact_row
        FROM artifacts artifact
        WHERE artifact.artifact_id = NEW.customer_artifact_id;
        expected_object_key := 'objects/'
            || substring(replace(submission_row.output_id::TEXT, '-', '') FROM 1 FOR 2)
            || '/' || replace(submission_row.output_id::TEXT, '-', '');
        IF artifact_row.artifact_id IS DISTINCT FROM submission_row.output_id
           OR artifact_row.tenant_id IS DISTINCT FROM submission_row.tenant_id
           OR artifact_row.job_id IS DISTINCT FROM submission_row.job_id
           OR artifact_row.work_item_id IS DISTINCT FROM submission_row.work_item_id
           OR artifact_row.execution_id
                IS DISTINCT FROM submission_row.created_by_execution_id
           OR artifact_row.lease_epoch
                IS DISTINCT FROM submission_row.created_by_lease_epoch
           OR artifact_row.output_index IS DISTINCT FROM output_index_value
           OR artifact_row.state IS DISTINCT FROM 'ready'
           OR artifact_row.storage_backend IS DISTINCT FROM authority_backend
           OR artifact_row.object_key IS DISTINCT FROM expected_object_key
           OR artifact_row.sha256_hex IS DISTINCT FROM authority_sha256
           OR artifact_row.byte_size IS DISTINCT FROM authority_byte_size
           OR artifact_row.media_type IS DISTINCT FROM authority_media_type
           OR receipt_row.evidence -> 'artifact' ->> 'authority_id'
                IS DISTINCT FROM authority_id_value::TEXT
           OR receipt_row.evidence -> 'artifact' ->> 'sha256_hex'
                IS DISTINCT FROM authority_sha256
           OR (receipt_row.evidence -> 'artifact' ->> 'byte_size')::BIGINT
                IS DISTINCT FROM authority_byte_size
           OR receipt_row.evidence -> 'artifact' ->> 'media_type'
                IS DISTINCT FROM authority_media_type THEN
            RAISE EXCEPTION 'completed terminal reduction artifact is not canonical';
        END IF;
    ELSIF NEW.customer_artifact_id IS NOT NULL
          OR receipt_row.evidence ->> 'error_code' IS DISTINCT FROM decision_error_code THEN
        RAISE EXCEPTION 'non-success terminal reduction cannot publish an artifact';
    END IF;

    PERFORM assert_terminal_quota_accounting(submission_row.job_id);
    PERFORM assert_terminal_parent_completion(submission_row.job_id);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_v4_provider_usage_fact_insert() RETURNS TRIGGER AS $$
DECLARE
    contract_version SMALLINT;
    quote_tenant_id TEXT;
    quote_currency TEXT;
    receipt_outcome TEXT;
    submission_account_id UUID;
BEGIN
    SELECT job.economics_contract_version
      INTO STRICT contract_version
    FROM jobs job
    WHERE job.job_id = NEW.job_id;

    SELECT receipt.outcome, submission.provider_account_id
      INTO STRICT receipt_outcome, submission_account_id
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

    IF NEW.provider_account_id IS DISTINCT FROM submission_account_id
       OR (
           NEW.metric <> 'provider_reported_cost'
           AND NEW.terminal_outcome IS DISTINCT FROM receipt_outcome
       ) THEN
        RAISE EXCEPTION 'provider usage fact conflicts with canonical receipt'
            USING ERRCODE = '23514';
    END IF;

    IF contract_version = 4 AND NEW.metric <> 'provider_reported_cost' THEN
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

CREATE TRIGGER provider_usage_facts_validate_v4_terminal
BEFORE INSERT ON provider_usage_facts
FOR EACH ROW EXECUTE FUNCTION validate_v4_provider_usage_fact_insert();

CREATE FUNCTION validate_ledger_economics_contract() RETURNS TRIGGER AS $$
DECLARE
    contract_version SMALLINT;
BEGIN
    IF NEW.source_job_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT economics_contract_version INTO STRICT contract_version
    FROM jobs WHERE job_id = NEW.source_job_id;
    IF (NEW.transaction_type = 'customer_charge' AND contract_version NOT IN (2, 3))
       OR (NEW.transaction_type = 'customer_job_charge' AND contract_version <> 4) THEN
        RAISE EXCEPTION 'ledger customer charge conflicts with economics contract'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_transactions_validate_economics_contract
BEFORE INSERT ON ledger_transactions
FOR EACH ROW EXECUTE FUNCTION validate_ledger_economics_contract();

CREATE UNIQUE INDEX ledger_transactions_provider_receipt_uidx
    ON ledger_transactions(source_receipt_id)
    WHERE transaction_type = 'provider_cost';

CREATE FUNCTION validate_v4_terminal_customer_rating() RETURNS TRIGGER AS $$
DECLARE
    target_job_id UUID;
    expected_outputs INTEGER;
    completed_outputs BIGINT;
    uncertain_outputs BIGINT;
    hold_state_value TEXT;
    rating_count BIGINT;
    rating_total BIGINT;
    customer_ledger_count BIGINT;
BEGIN
    IF NEW.state <> 'completed' THEN
        RETURN NULL;
    END IF;
    SELECT submission.job_id, job.output_count
      INTO target_job_id, expected_outputs
    FROM provider_submissions submission
    JOIN jobs job ON job.job_id = submission.job_id
    WHERE submission.submission_id = NEW.submission_id
      AND job.economics_contract_version = 4;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    SELECT COUNT(*) FILTER (WHERE reduction.state = 'completed'),
           COUNT(*) FILTER (
               WHERE reduction.state = 'completed'
                 AND reduction.resolved_state = 'uncertain'
           )
      INTO completed_outputs, uncertain_outputs
    FROM executor_terminal_reductions reduction
    JOIN provider_submissions submission
      ON submission.submission_id = reduction.submission_id
    WHERE submission.job_id = target_job_id;

    SELECT state INTO STRICT hold_state_value
    FROM customer_billing_holds
    WHERE job_id = target_job_id;
    SELECT COUNT(*), COALESCE(MAX(total_amount_micros), 0)
      INTO rating_count, rating_total
    FROM customer_rated_usage
    WHERE job_id = target_job_id;
    SELECT COUNT(*) INTO customer_ledger_count
    FROM ledger_transactions
    WHERE source_job_id = target_job_id
      AND transaction_type = 'customer_job_charge';

    IF completed_outputs < expected_outputs THEN
        IF hold_state_value <> 'held' OR rating_count <> 0
           OR customer_ledger_count <> 0 THEN
            RAISE EXCEPTION 'incomplete v4 job cannot be customer-rated';
        END IF;
    ELSIF completed_outputs <> expected_outputs THEN
        RAISE EXCEPTION 'v4 terminal reduction count exceeds output count';
    ELSIF uncertain_outputs > 0 THEN
        IF hold_state_value <> 'held' OR rating_count <> 0
           OR customer_ledger_count <> 0 THEN
            RAISE EXCEPTION 'uncertain v4 job must retain its customer hold';
        END IF;
    ELSIF hold_state_value <> 'settled' OR rating_count <> 1
          OR (
              rating_total > 0
              AND customer_ledger_count <> 1
          )
          OR (
              rating_total = 0
              AND customer_ledger_count <> 0
          ) THEN
        RAISE EXCEPTION 'definite v4 job requires one canonical customer rating';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER executor_terminal_reductions_validate_v4_rating
AFTER INSERT OR UPDATE ON executor_terminal_reductions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_v4_terminal_customer_rating();
