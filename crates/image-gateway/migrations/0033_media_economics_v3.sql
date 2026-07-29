LOCK TABLE jobs, job_outputs, quota_reservations, usage_events, metering_events,
    price_versions, price_quotes, job_response_projections IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE jobs
    ADD COLUMN output_count INTEGER,
    ADD COLUMN billable_units INTEGER,
    ADD COLUMN billing_metric TEXT NOT NULL DEFAULT 'output',
    ADD COLUMN billing_unit TEXT NOT NULL DEFAULT 'output';

UPDATE jobs
SET output_count = requested_units,
    billable_units = requested_units;

-- Existing schemas contain deferred invariant triggers on jobs. Flush their
-- events before the next ALTER TABLE so this migration works with live data.
SET CONSTRAINTS ALL IMMEDIATE;

ALTER TABLE jobs
    ALTER COLUMN output_count SET NOT NULL,
    ALTER COLUMN billable_units SET NOT NULL,
    DROP CONSTRAINT jobs_economics_contract_version_check,
    ADD CONSTRAINT jobs_economics_contract_version_check CHECK (
        economics_contract_version IN (1, 2, 3)
    ),
    ADD CONSTRAINT jobs_media_economics_dimensions_check CHECK (
        output_count > 0
        AND billable_units > 0
        AND requested_units = billable_units
        AND billing_metric IN ('output', 'request', 'video_second')
        AND billing_unit IN ('output', 'request', 'second')
        AND (
            (billing_metric = 'output' AND billing_unit = 'output'
                AND output_count = billable_units)
            OR (billing_metric = 'request' AND billing_unit = 'request'
                AND output_count = 1 AND billable_units = 1)
            OR (billing_metric = 'video_second' AND billing_unit = 'second'
                AND output_count = 1)
        )
        AND (
            economics_contract_version <> 3
            OR (billing_metric = 'video_second' AND billing_unit = 'second'
                AND output_count = 1)
        )
    );

ALTER TABLE job_outputs
    ADD COLUMN billable_units INTEGER NOT NULL DEFAULT 1
        CHECK (billable_units > 0);

ALTER TABLE quota_reservations
    ADD COLUMN billing_metric TEXT NOT NULL DEFAULT 'output'
        CHECK (billing_metric IN ('output', 'request', 'video_second')),
    ADD COLUMN billing_unit TEXT NOT NULL DEFAULT 'output'
        CHECK (billing_unit IN ('output', 'request', 'second')),
    ADD CONSTRAINT quota_reservations_billing_dimension_check CHECK (
        (billing_metric = 'output' AND billing_unit = 'output')
        OR (billing_metric = 'request' AND billing_unit = 'request')
        OR (billing_metric = 'video_second' AND billing_unit = 'second')
    );

ALTER TABLE usage_events
    ADD COLUMN billing_metric TEXT NOT NULL DEFAULT 'output'
        CHECK (billing_metric IN ('output', 'request', 'video_second')),
    ADD COLUMN billing_unit TEXT NOT NULL DEFAULT 'output'
        CHECK (billing_unit IN ('output', 'request', 'second'));

ALTER TABLE metering_events
    ADD COLUMN billing_metric TEXT NOT NULL DEFAULT 'output'
        CHECK (billing_metric IN ('output', 'request', 'video_second')),
    ADD COLUMN billing_unit TEXT NOT NULL DEFAULT 'output'
        CHECK (billing_unit IN ('output', 'request', 'second'));

CREATE INDEX usage_events_tenant_metric_created_at_ms_idx
    ON usage_events (tenant_id, billing_metric, billing_unit, created_at_ms);

CREATE INDEX quota_reservations_active_tenant_metric_idx
    ON quota_reservations
      (tenant_id, billing_metric, billing_unit, state, expires_at_ms);

ALTER TABLE price_versions
    ADD COLUMN billing_metric TEXT NOT NULL DEFAULT 'output'
        CHECK (billing_metric IN ('output', 'request', 'video_second')),
    ADD COLUMN billing_unit TEXT NOT NULL DEFAULT 'output'
        CHECK (billing_unit IN ('output', 'request', 'second')),
    ADD CONSTRAINT price_versions_billing_dimension_check CHECK (
        (billing_metric = 'output' AND billing_unit = 'output')
        OR (billing_metric = 'request' AND billing_unit = 'request')
        OR (billing_metric = 'video_second' AND billing_unit = 'second')
    ),
    ADD CONSTRAINT price_versions_active_video_price_check CHECK (
        billing_metric <> 'video_second'
        OR state <> 'active'
        OR success_micros > 0
    );

DROP INDEX price_versions_active_route_uidx;
CREATE UNIQUE INDEX price_versions_active_route_metric_uidx
    ON price_versions
      (api_profile, operation, provider_id, model, billing_metric, billing_unit)
    WHERE state = 'active';

CREATE OR REPLACE FUNCTION preserve_published_price_version() RETURNS TRIGGER AS $$
BEGIN
    IF ROW(
        OLD.price_key, OLD.version, OLD.api_profile, OLD.operation, OLD.provider_id, OLD.model,
        OLD.billing_metric, OLD.billing_unit, OLD.currency,
        OLD.success_micros, OLD.failed_micros, OLD.no_effect_micros, OLD.created_at_ms
    ) IS DISTINCT FROM ROW(
        NEW.price_key, NEW.version, NEW.api_profile, NEW.operation, NEW.provider_id, NEW.model,
        NEW.billing_metric, NEW.billing_unit, NEW.currency,
        NEW.success_micros, NEW.failed_micros, NEW.no_effect_micros, NEW.created_at_ms
    ) THEN
        RAISE EXCEPTION 'published price fields are immutable';
    END IF;
    IF (OLD.state, NEW.state) NOT IN (('draft', 'active'), ('active', 'retired'))
       AND OLD.state <> NEW.state THEN
        RAISE EXCEPTION 'invalid price version transition';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER price_quotes_reject_mutation ON price_quotes;

ALTER TABLE price_quotes
    ADD COLUMN billing_metric TEXT NOT NULL DEFAULT 'output'
        CHECK (billing_metric IN ('output', 'request', 'video_second')),
    ADD COLUMN billing_unit TEXT NOT NULL DEFAULT 'output'
        CHECK (billing_unit IN ('output', 'request', 'second')),
    ADD COLUMN billable_units BIGINT;

UPDATE price_quotes SET billable_units = output_count;

SET CONSTRAINTS ALL IMMEDIATE;

ALTER TABLE price_quotes
    ALTER COLUMN billable_units SET NOT NULL,
    DROP CONSTRAINT price_quotes_check,
    ADD CONSTRAINT price_quotes_max_total_check CHECK (
        max_total_micros::NUMERIC =
        billable_units::NUMERIC
            * GREATEST(success_micros, failed_micros, no_effect_micros)::NUMERIC
    ),
    ADD CONSTRAINT price_quotes_media_dimensions_check CHECK (
        output_count > 0
        AND billable_units > 0
        AND (
            (billing_metric = 'output' AND billing_unit = 'output'
                AND output_count::BIGINT = billable_units)
            OR (billing_metric = 'request' AND billing_unit = 'request'
                AND output_count = 1 AND billable_units = 1)
            OR (billing_metric = 'video_second' AND billing_unit = 'second'
                AND output_count = 1)
        )
    );

CREATE TRIGGER price_quotes_reject_mutation
    BEFORE UPDATE OR DELETE ON price_quotes
    FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

ALTER TABLE job_response_projections
    DROP CONSTRAINT job_response_projections_operation_check,
    ADD CONSTRAINT job_response_projections_operation_check CHECK (
        operation IN ('generation', 'edit', 'video_generation')
    );

-- V2 image jobs and V3 media jobs share the durable executor pipeline. Output
-- cardinality is independent from the number of units represented by each output.
CREATE OR REPLACE FUNCTION validate_executor_handoff() RETURNS TRIGGER AS $$
DECLARE
    contract_version SMALLINT;
    expected_outputs INTEGER;
    attempt_state TEXT;
    attempt_handed_off_at BIGINT;
    actual_output_count BIGINT;
    invalid_output_count BIGINT;
    submission_count BIGINT;
    execution_count BIGINT;
    attachment_count BIGINT;
BEGIN
    IF NEW.state <> 'awaiting_executor' THEN
        RETURN NULL;
    END IF;

    SELECT job.economics_contract_version, job.output_count,
           attempt.state, attempt.handed_off_at_ms
      INTO contract_version, expected_outputs, attempt_state, attempt_handed_off_at
    FROM jobs job
    JOIN job_attempts attempt
      ON attempt.work_item_id = NEW.work_item_id
     AND attempt.execution_id = NEW.execution_id
     AND attempt.lease_epoch = NEW.lease_epoch
    WHERE job.job_id = NEW.job_id;

    SELECT COUNT(*),
           COUNT(*) FILTER (
               WHERE output.output_index < 0
                  OR output.output_index >= expected_outputs
           )
      INTO actual_output_count, invalid_output_count
    FROM job_outputs output
    WHERE output.job_id = NEW.job_id;

    SELECT COUNT(*)
      INTO submission_count
    FROM provider_submissions submission
    JOIN job_outputs output
      ON output.output_id = submission.output_id
     AND output.job_id = submission.job_id
    WHERE submission.job_id = NEW.job_id
      AND submission.work_item_id = NEW.work_item_id
      AND submission.created_by_execution_id = NEW.execution_id
      AND submission.created_by_lease_epoch = NEW.lease_epoch
      AND submission.execution_profile_id = NEW.execution_profile_id
      AND submission.state = 'prepared';

    SELECT COUNT(*)
      INTO execution_count
    FROM executor_executions execution
    JOIN provider_submissions submission
      ON submission.executor_execution_id = execution.executor_execution_id
     AND submission.submission_id = execution.submission_id
    WHERE submission.job_id = NEW.job_id
      AND submission.work_item_id = NEW.work_item_id
      AND execution.state = 'prepared';

    SELECT COUNT(*)
      INTO attachment_count
    FROM provider_submission_attachments attachment
    JOIN provider_submissions submission
      ON submission.submission_id = attachment.submission_id
     AND submission.job_id = attachment.job_id
     AND submission.work_item_id = attachment.work_item_id
    WHERE attachment.job_id = NEW.job_id
      AND attachment.work_item_id = NEW.work_item_id
      AND attachment.attempt_execution_id = NEW.execution_id
      AND attachment.lease_epoch = NEW.lease_epoch;

    IF contract_version IS NULL OR contract_version NOT IN (2, 3)
       OR expected_outputs IS NULL OR expected_outputs <= 0
       OR attempt_state IS DISTINCT FROM 'handed_off'
       OR attempt_handed_off_at IS DISTINCT FROM NEW.handed_off_at_ms
       OR actual_output_count <> expected_outputs
       OR invalid_output_count <> 0
       OR submission_count <> expected_outputs
       OR execution_count <> expected_outputs
       OR attachment_count <> expected_outputs THEN
        RAISE EXCEPTION 'executor handoff is incomplete or inconsistent';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enqueue_executor_terminal_reduction() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.state IN ('succeeded', 'failed', 'uncertain', 'canceled')
       AND OLD.state NOT IN ('succeeded', 'failed', 'uncertain', 'canceled')
       AND EXISTS (
           SELECT 1 FROM jobs job
           WHERE job.job_id = NEW.job_id
             AND job.economics_contract_version IN (2, 3)
       ) THEN
        INSERT INTO executor_terminal_reductions (
            submission_id, executor_execution_id, resolution_decision_id,
            resolved_state, state, created_at_ms, updated_at_ms
        ) VALUES (
            NEW.submission_id, NEW.executor_execution_id, NEW.resolution_decision_id,
            NEW.state, 'ready', NEW.finished_at_ms, NEW.finished_at_ms
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_executor_terminal_reduction_projection()
RETURNS TRIGGER AS $$
DECLARE
    reduction_count BIGINT;
BEGIN
    IF NEW.state NOT IN ('succeeded', 'failed', 'uncertain', 'canceled')
       OR NOT EXISTS (
           SELECT 1 FROM jobs job
           WHERE job.job_id = NEW.job_id
             AND job.economics_contract_version IN (2, 3)
       ) THEN
        RETURN NULL;
    END IF;

    SELECT COUNT(*) INTO reduction_count
    FROM executor_terminal_reductions reduction
    WHERE reduction.submission_id = NEW.submission_id
      AND reduction.executor_execution_id = NEW.executor_execution_id
      AND reduction.resolution_decision_id = NEW.resolution_decision_id
      AND reduction.resolved_state = NEW.state;
    IF reduction_count <> 1 THEN
        RAISE EXCEPTION
            'V2/V3 terminal provider submission requires one canonical reduction';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assert_terminal_quota_accounting(target_job_id UUID)
RETURNS VOID AS $$
DECLARE
    expected_output_count INTEGER;
    expected_billable_units INTEGER;
    reservation_requested_units INTEGER;
    committed_units INTEGER;
    released_units INTEGER;
    reservation_state TEXT;
    actual_output_count BIGINT;
    actual_billable_units BIGINT;
    succeeded_count BIGINT;
    failed_count BIGINT;
    uncertain_count BIGINT;
    completed_count BIGINT;
    succeeded_units BIGINT;
    failed_units BIGINT;
    submission_count BIGINT;
BEGIN
    SELECT job.output_count, job.billable_units, quota.requested_units,
           quota.committed_units, quota.released_units, quota.state
      INTO STRICT expected_output_count, expected_billable_units,
           reservation_requested_units, committed_units, released_units,
           reservation_state
    FROM quota_reservations quota
    JOIN jobs job
      ON job.reservation_id = quota.reservation_id
     AND job.job_id = quota.job_id
     AND job.tenant_id = quota.tenant_id
    WHERE job.job_id = target_job_id
      AND job.economics_contract_version IN (2, 3);

    SELECT COUNT(*), COALESCE(SUM(output.billable_units), 0)
      INTO actual_output_count, actual_billable_units
    FROM job_outputs output
    WHERE output.job_id = target_job_id;

    SELECT COUNT(*) FILTER (
               WHERE reduction.state = 'completed'
                 AND reduction.resolved_state = 'succeeded'
           ),
           COUNT(*) FILTER (
               WHERE reduction.state = 'completed'
                 AND reduction.resolved_state IN ('failed', 'canceled')
           ),
           COUNT(*) FILTER (
               WHERE reduction.state = 'completed'
                 AND reduction.resolved_state = 'uncertain'
           ),
           COUNT(*) FILTER (WHERE reduction.state = 'completed'),
           COALESCE(SUM(output.billable_units) FILTER (
               WHERE reduction.state = 'completed'
                 AND reduction.resolved_state = 'succeeded'
           ), 0),
           COALESCE(SUM(output.billable_units) FILTER (
               WHERE reduction.state = 'completed'
                 AND reduction.resolved_state IN ('failed', 'canceled')
           ), 0)
      INTO succeeded_count, failed_count, uncertain_count, completed_count,
           succeeded_units, failed_units
    FROM executor_terminal_reductions reduction
    JOIN provider_submissions submission
      ON submission.submission_id = reduction.submission_id
     AND submission.executor_execution_id = reduction.executor_execution_id
    JOIN job_outputs output
      ON output.output_id = submission.output_id
     AND output.job_id = submission.job_id
    WHERE submission.job_id = target_job_id;

    SELECT COUNT(*) INTO submission_count
    FROM provider_submissions submission
    WHERE submission.job_id = target_job_id;

    IF reservation_requested_units <> expected_billable_units
       OR actual_output_count <> expected_output_count
       OR actual_billable_units <> expected_billable_units
       OR submission_count <> expected_output_count
       OR committed_units <> succeeded_units
       OR released_units <> failed_units
       OR (completed_count < expected_output_count AND reservation_state <> 'reserved')
       OR (completed_count = expected_output_count AND uncertain_count > 0
           AND reservation_state <> 'reserved')
       OR (completed_count = expected_output_count AND uncertain_count = 0
           AND succeeded_count > 0 AND reservation_state <> 'committed')
       OR (completed_count = expected_output_count AND uncertain_count = 0
           AND succeeded_count = 0 AND reservation_state <> 'released') THEN
        RAISE EXCEPTION 'terminal reduction quota accounting is inconsistent';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assert_terminal_parent_completion(target_job_id UUID)
RETURNS VOID AS $$
DECLARE
    expected_output_count INTEGER;
    expected_billable_units INTEGER;
    charged_units INTEGER;
    job_state_value TEXT;
    work_item_id_value UUID;
    work_state_value TEXT;
    attempt_state_value TEXT;
    actual_output_count BIGINT;
    actual_billable_units BIGINT;
    succeeded_count BIGINT;
    failed_count BIGINT;
    uncertain_count BIGINT;
    completed_count BIGINT;
    succeeded_units BIGINT;
    artifact_count BIGINT;
    projection_count BIGINT;
    invalid_idempotency_count BIGINT;
    terminal_event_count BIGINT;
    terminal_outbox_count BIGINT;
    expected_parent_state TEXT;
BEGIN
    SELECT job.output_count, job.billable_units, job.charged_units, job.state,
           work.work_item_id, work.state, attempt.state
      INTO STRICT expected_output_count, expected_billable_units, charged_units,
           job_state_value, work_item_id_value, work_state_value, attempt_state_value
    FROM jobs job
    JOIN work_items work ON work.job_id = job.job_id
    JOIN job_attempts attempt
      ON attempt.work_item_id = work.work_item_id
     AND attempt.execution_id = work.execution_id
     AND attempt.lease_epoch = work.lease_epoch
    WHERE job.job_id = target_job_id
      AND job.economics_contract_version IN (2, 3);
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'V2/V3 provider submissions require one canonical parent attempt projection';
    END IF;

    SELECT COUNT(*), COALESCE(SUM(output.billable_units), 0)
      INTO actual_output_count, actual_billable_units
    FROM job_outputs output
    WHERE output.job_id = target_job_id;

    SELECT COUNT(*) FILTER (
               WHERE reduction.state = 'completed'
                 AND reduction.resolved_state = 'succeeded'
           ),
           COUNT(*) FILTER (
               WHERE reduction.state = 'completed'
                 AND reduction.resolved_state IN ('failed', 'canceled')
           ),
           COUNT(*) FILTER (
               WHERE reduction.state = 'completed'
                 AND reduction.resolved_state = 'uncertain'
           ),
           COUNT(*) FILTER (WHERE reduction.state = 'completed'),
           COALESCE(SUM(output.billable_units) FILTER (
               WHERE reduction.state = 'completed'
                 AND reduction.resolved_state = 'succeeded'
           ), 0)
      INTO succeeded_count, failed_count, uncertain_count, completed_count,
           succeeded_units
    FROM executor_terminal_reductions reduction
    JOIN provider_submissions submission
      ON submission.submission_id = reduction.submission_id
     AND submission.executor_execution_id = reduction.executor_execution_id
    JOIN job_outputs output
      ON output.output_id = submission.output_id
     AND output.job_id = submission.job_id
    WHERE submission.job_id = target_job_id;

    SELECT COUNT(*) INTO artifact_count FROM artifacts WHERE job_id = target_job_id;
    SELECT COUNT(*) INTO projection_count
    FROM job_response_projections WHERE job_id = target_job_id;
    SELECT COUNT(*) INTO terminal_event_count
    FROM job_events
    WHERE job_id = target_job_id
      AND semantic_key = 'work.' || work_item_id_value::TEXT || '.executor-terminal';
    SELECT COUNT(*) INTO terminal_outbox_count
    FROM outbox_events
    WHERE job_id = target_job_id
      AND semantic_key = 'work.' || work_item_id_value::TEXT || '.executor-terminal';

    IF actual_output_count <> expected_output_count
       OR actual_billable_units <> expected_billable_units THEN
        RAISE EXCEPTION 'job output economics are inconsistent';
    END IF;

    IF completed_count < expected_output_count THEN
        IF work_state_value <> 'awaiting_executor'
           OR attempt_state_value <> 'handed_off'
           OR job_state_value NOT IN ('reserved', 'queued', 'running', 'artifact_ready')
           OR artifact_count <> succeeded_count
           OR projection_count <> 0
           OR terminal_event_count <> 0
           OR terminal_outbox_count <> 0 THEN
            RAISE EXCEPTION 'incomplete terminal reductions changed the parent projection';
        END IF;
        RETURN;
    ELSIF completed_count <> expected_output_count THEN
        RAISE EXCEPTION 'terminal reduction count exceeds the requested output count';
    END IF;

    expected_parent_state := CASE
        WHEN uncertain_count > 0 THEN 'uncertain'
        WHEN failed_count > 0 THEN 'failed'
        ELSE 'succeeded'
    END;
    SELECT COUNT(*) INTO invalid_idempotency_count
    FROM idempotency_requests request
    WHERE request.job_id = target_job_id
      AND (request.state <> expected_parent_state
           OR request.terminal_outcome <> expected_parent_state);
    IF work_state_value <> expected_parent_state
       OR attempt_state_value <> expected_parent_state
       OR job_state_value <> expected_parent_state
       OR charged_units <> succeeded_units
       OR artifact_count <> succeeded_count
       OR invalid_idempotency_count <> 0
       OR terminal_event_count <> 1
       OR terminal_outbox_count <> 1
       OR (expected_parent_state = 'succeeded' AND projection_count <> 1)
       OR (expected_parent_state <> 'succeeded' AND projection_count <> 0) THEN
        RAISE EXCEPTION 'completed terminal reductions require one canonical parent projection';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_executor_terminal_reduction_completion()
RETURNS TRIGGER AS $$
DECLARE
    receipt_row provider_receipts%ROWTYPE;
    artifact_row artifacts%ROWTYPE;
    submission_row provider_submissions%ROWTYPE;
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

    SELECT job.reservation_id INTO STRICT reservation_id_value
    FROM jobs job
    WHERE job.job_id = submission_row.job_id
      AND job.tenant_id = submission_row.tenant_id
      AND job.economics_contract_version IN (2, 3);
    IF NEW.quota_reservation_id IS DISTINCT FROM reservation_id_value THEN
        RAISE EXCEPTION 'completed terminal reduction quota reservation is not canonical';
    END IF;

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

CREATE OR REPLACE FUNCTION validate_v2_provider_receipt_reduction()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM jobs job
        WHERE job.job_id = NEW.job_id
          AND job.economics_contract_version IN (2, 3)
    ) AND NOT EXISTS (
        SELECT 1
        FROM executor_terminal_reductions reduction
        WHERE reduction.submission_id = NEW.submission_id
          AND reduction.provider_receipt_id = NEW.receipt_id
          AND reduction.state = 'completed'
    ) THEN
        RAISE EXCEPTION
            'V2/V3 provider receipt requires canonical terminal reduction completion';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_terminal_quota_update() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM jobs job
        JOIN provider_submissions submission ON submission.job_id = job.job_id
        JOIN executor_terminal_reductions reduction
          ON reduction.submission_id = submission.submission_id
        WHERE job.job_id = NEW.job_id
          AND job.economics_contract_version IN (2, 3)
    ) THEN
        PERFORM assert_terminal_quota_accounting(NEW.job_id);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assert_v2_parent_if_present(target_job_id UUID)
RETURNS VOID AS $$
BEGIN
    IF target_job_id IS NOT NULL AND EXISTS (
        SELECT 1
        FROM provider_submissions submission
        JOIN jobs job ON job.job_id = submission.job_id
        WHERE submission.job_id = target_job_id
          AND job.economics_contract_version IN (2, 3)
    ) THEN
        PERFORM assert_terminal_parent_completion(target_job_id);
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION protect_v2_idempotency_binding() RETURNS TRIGGER AS $$
BEGIN
    IF OLD.job_id IS NOT NULL AND EXISTS (
        SELECT 1
        FROM provider_submissions submission
        JOIN jobs job ON job.job_id = submission.job_id
        WHERE submission.job_id = OLD.job_id
          AND job.economics_contract_version IN (2, 3)
    ) AND (TG_OP = 'DELETE' OR NEW.job_id IS DISTINCT FROM OLD.job_id) THEN
        RAISE EXCEPTION
            'V2/V3 idempotency binding is immutable after provider handoff';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

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
              AND job.output_count = NEW.output_total
              AND allocation.state = 'held'
       ) THEN
        RAISE EXCEPTION
            'provider submit acquisition requires an exact remote operation binding';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
