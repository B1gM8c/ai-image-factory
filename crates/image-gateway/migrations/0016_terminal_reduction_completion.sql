LOCK TABLE executor_terminal_reductions, provider_receipts, artifacts,
    quota_reservations IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM executor_terminal_reductions WHERE state = 'completed') THEN
        RAISE EXCEPTION
            'terminal reduction completion migration requires completed reductions to be repaired first';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jobs job
        JOIN provider_submissions submission ON submission.job_id = job.job_id
        JOIN work_items work ON work.job_id = job.job_id
        JOIN job_attempts attempt
          ON attempt.work_item_id = work.work_item_id
         AND attempt.execution_id = work.execution_id
         AND attempt.lease_epoch = work.lease_epoch
        WHERE job.economics_contract_version = 2
          AND (job.state NOT IN ('reserved', 'queued', 'running', 'artifact_ready')
               OR work.state <> 'awaiting_executor'
               OR attempt.state <> 'handed_off')
    ) THEN
        RAISE EXCEPTION
            'terminal reduction completion migration requires early V2 parent terminals to be repaired first';
    END IF;
END;
$$;

ALTER TABLE executor_terminal_reductions
    ADD COLUMN completion_owner TEXT CHECK (
        completion_owner IS NULL
        OR (char_length(completion_owner) BETWEEN 1 AND 255
            AND completion_owner !~ '[[:cntrl:]]')
    ),
    ADD COLUMN provider_receipt_id UUID
        REFERENCES provider_receipts(receipt_id) ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    ADD COLUMN customer_artifact_id UUID
        REFERENCES artifacts(artifact_id) ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    ADD COLUMN quota_reservation_id UUID
        REFERENCES quota_reservations(reservation_id) ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT executor_terminal_reduction_completion_shape CHECK (
        (state IN ('ready', 'leased')
            AND completion_owner IS NULL
            AND provider_receipt_id IS NULL AND customer_artifact_id IS NULL
            AND quota_reservation_id IS NULL)
        OR
        (state = 'completed'
            AND completion_owner IS NOT NULL
            AND provider_receipt_id IS NOT NULL
            AND quota_reservation_id IS NOT NULL
            AND ((resolved_state = 'succeeded') = (customer_artifact_id IS NOT NULL)))
    ),
    ADD CONSTRAINT executor_terminal_reduction_receipt_unique UNIQUE (provider_receipt_id),
    ADD CONSTRAINT executor_terminal_reduction_artifact_unique UNIQUE (customer_artifact_id);

ALTER TABLE quota_reservations
    ADD CONSTRAINT quota_reservation_output_slices CHECK (
        committed_units >= 0 AND started_units >= 0 AND released_units >= 0
        AND committed_units::NUMERIC + released_units::NUMERIC <= requested_units::NUMERIC
        AND (
            state IN ('reserved', 'expired')
            OR (state = 'committed' AND committed_units > 0
                AND committed_units::NUMERIC + released_units::NUMERIC = requested_units::NUMERIC)
            OR (state = 'released' AND committed_units = 0
                AND released_units = requested_units)
        )
    );

CREATE FUNCTION assert_terminal_quota_accounting(target_job_id UUID) RETURNS VOID AS $$
DECLARE
    requested_count INTEGER;
    committed_count INTEGER;
    released_count INTEGER;
    reservation_state TEXT;
    succeeded_count BIGINT;
    failed_count BIGINT;
    uncertain_count BIGINT;
    completed_count BIGINT;
    submission_count BIGINT;
BEGIN
    SELECT quota.requested_units, quota.committed_units, quota.released_units, quota.state
      INTO STRICT requested_count, committed_count, released_count, reservation_state
    FROM quota_reservations quota
    JOIN jobs job
      ON job.reservation_id = quota.reservation_id
     AND job.job_id = quota.job_id
     AND job.tenant_id = quota.tenant_id
    WHERE job.job_id = target_job_id AND job.economics_contract_version = 2;

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
           COUNT(*) FILTER (WHERE reduction.state = 'completed')
      INTO succeeded_count, failed_count, uncertain_count, completed_count
    FROM executor_terminal_reductions reduction
    JOIN provider_submissions submission
      ON submission.submission_id = reduction.submission_id
     AND submission.executor_execution_id = reduction.executor_execution_id
    WHERE submission.job_id = target_job_id;

    SELECT COUNT(*) INTO submission_count
    FROM provider_submissions submission
    WHERE submission.job_id = target_job_id;

    IF submission_count <> requested_count
       OR committed_count <> succeeded_count
       OR released_count <> failed_count
       OR (completed_count < requested_count AND reservation_state <> 'reserved')
       OR (completed_count = requested_count AND uncertain_count > 0
           AND reservation_state <> 'reserved')
       OR (completed_count = requested_count AND uncertain_count = 0
           AND succeeded_count > 0 AND reservation_state <> 'committed')
       OR (completed_count = requested_count AND uncertain_count = 0
           AND succeeded_count = 0 AND reservation_state <> 'released') THEN
        RAISE EXCEPTION 'terminal reduction quota accounting is inconsistent';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION assert_terminal_parent_completion(target_job_id UUID) RETURNS VOID AS $$
DECLARE
    requested_count INTEGER;
    charged_count INTEGER;
    job_state_value TEXT;
    work_item_id_value UUID;
    work_state_value TEXT;
    attempt_state_value TEXT;
    succeeded_count BIGINT;
    failed_count BIGINT;
    uncertain_count BIGINT;
    completed_count BIGINT;
    artifact_count BIGINT;
    projection_count BIGINT;
    invalid_idempotency_count BIGINT;
    terminal_event_count BIGINT;
    terminal_outbox_count BIGINT;
    expected_parent_state TEXT;
BEGIN
    SELECT job.requested_units, job.charged_units, job.state,
           work.work_item_id, work.state, attempt.state
      INTO STRICT requested_count, charged_count, job_state_value,
           work_item_id_value, work_state_value, attempt_state_value
    FROM jobs job
    JOIN work_items work ON work.job_id = job.job_id
    JOIN job_attempts attempt
      ON attempt.work_item_id = work.work_item_id
     AND attempt.execution_id = work.execution_id
     AND attempt.lease_epoch = work.lease_epoch
    WHERE job.job_id = target_job_id AND job.economics_contract_version = 2;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'V2 provider submissions require one canonical parent attempt projection';
    END IF;

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
           COUNT(*) FILTER (WHERE reduction.state = 'completed')
      INTO succeeded_count, failed_count, uncertain_count, completed_count
    FROM executor_terminal_reductions reduction
    JOIN provider_submissions submission
      ON submission.submission_id = reduction.submission_id
     AND submission.executor_execution_id = reduction.executor_execution_id
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

    IF completed_count < requested_count THEN
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
    ELSIF completed_count <> requested_count THEN
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
       OR charged_count <> succeeded_count
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

CREATE FUNCTION validate_executor_terminal_reduction_completion() RETURNS TRIGGER AS $$
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
      AND job.economics_contract_version = 2;
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

CREATE CONSTRAINT TRIGGER executor_terminal_reduction_completion_check
    AFTER INSERT OR UPDATE ON executor_terminal_reductions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_executor_terminal_reduction_completion();

CREATE FUNCTION validate_v2_provider_receipt_reduction() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM jobs job
        WHERE job.job_id = NEW.job_id AND job.economics_contract_version = 2
    ) AND NOT EXISTS (
        SELECT 1
        FROM executor_terminal_reductions reduction
        WHERE reduction.submission_id = NEW.submission_id
          AND reduction.provider_receipt_id = NEW.receipt_id
          AND reduction.state = 'completed'
    ) THEN
        RAISE EXCEPTION 'V2 provider receipt requires canonical terminal reduction completion';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_receipt_terminal_reduction_check
    AFTER INSERT OR UPDATE ON provider_receipts
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_v2_provider_receipt_reduction();

CREATE FUNCTION validate_terminal_quota_update() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM jobs job
        JOIN provider_submissions submission ON submission.job_id = job.job_id
        JOIN executor_terminal_reductions reduction
          ON reduction.submission_id = submission.submission_id
        WHERE job.job_id = NEW.job_id AND job.economics_contract_version = 2
    ) THEN
        PERFORM assert_terminal_quota_accounting(NEW.job_id);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER quota_terminal_reduction_accounting_check
    AFTER UPDATE ON quota_reservations
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_terminal_quota_update();

CREATE FUNCTION assert_v2_parent_if_present(target_job_id UUID) RETURNS VOID AS $$
BEGIN
    IF target_job_id IS NOT NULL AND EXISTS (
        SELECT 1
        FROM provider_submissions submission
        JOIN jobs job ON job.job_id = submission.job_id
        WHERE submission.job_id = target_job_id AND job.economics_contract_version = 2
    ) THEN
        PERFORM assert_terminal_parent_completion(target_job_id);
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_terminal_parent_update() RETURNS TRIGGER AS $$
DECLARE
    target_job_id UUID;
    previous_job_id UUID;
BEGIN
    IF TG_TABLE_NAME = 'job_attempts' THEN
        IF TG_OP <> 'DELETE' THEN
            SELECT work.job_id INTO target_job_id
            FROM work_items work WHERE work.work_item_id = NEW.work_item_id;
        END IF;
        IF TG_OP <> 'INSERT' THEN
            SELECT work.job_id INTO previous_job_id
            FROM work_items work WHERE work.work_item_id = OLD.work_item_id;
        END IF;
    ELSE
        IF TG_OP <> 'DELETE' THEN
            target_job_id := NEW.job_id;
        END IF;
        IF TG_OP <> 'INSERT' THEN
            previous_job_id := OLD.job_id;
        END IF;
    END IF;
    PERFORM assert_v2_parent_if_present(previous_job_id);
    IF target_job_id IS DISTINCT FROM previous_job_id THEN
        PERFORM assert_v2_parent_if_present(target_job_id);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION protect_v2_idempotency_binding() RETURNS TRIGGER AS $$
BEGIN
    IF OLD.job_id IS NOT NULL AND EXISTS (
        SELECT 1
        FROM provider_submissions submission
        JOIN jobs job ON job.job_id = submission.job_id
        WHERE submission.job_id = OLD.job_id AND job.economics_contract_version = 2
    ) AND (TG_OP = 'DELETE' OR NEW.job_id IS DISTINCT FROM OLD.job_id) THEN
        RAISE EXCEPTION 'V2 idempotency binding is immutable after provider handoff';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER idempotency_requests_protect_v2_binding
    BEFORE UPDATE OR DELETE ON idempotency_requests
    FOR EACH ROW EXECUTE FUNCTION protect_v2_idempotency_binding();

CREATE CONSTRAINT TRIGGER jobs_terminal_parent_check
    AFTER UPDATE ON jobs DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_terminal_parent_update();
CREATE CONSTRAINT TRIGGER work_items_terminal_parent_check
    AFTER UPDATE ON work_items DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_terminal_parent_update();
CREATE CONSTRAINT TRIGGER job_attempts_terminal_parent_check
    AFTER UPDATE ON job_attempts DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_terminal_parent_update();
CREATE CONSTRAINT TRIGGER provider_submissions_terminal_parent_check
    AFTER INSERT OR UPDATE ON provider_submissions DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_terminal_parent_update();
CREATE CONSTRAINT TRIGGER idempotency_terminal_parent_check
    AFTER INSERT OR UPDATE OR DELETE ON idempotency_requests
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_terminal_parent_update();
CREATE CONSTRAINT TRIGGER response_projection_terminal_parent_check
    AFTER INSERT OR UPDATE OR DELETE ON job_response_projections
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_terminal_parent_update();
CREATE CONSTRAINT TRIGGER artifacts_terminal_parent_check
    AFTER INSERT OR UPDATE OR DELETE ON artifacts DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_terminal_parent_update();
CREATE CONSTRAINT TRIGGER job_events_terminal_parent_check
    AFTER INSERT OR UPDATE OR DELETE ON job_events DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_terminal_parent_update();
CREATE CONSTRAINT TRIGGER outbox_terminal_parent_check
    AFTER INSERT OR UPDATE OR DELETE ON outbox_events DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION validate_terminal_parent_update();

CREATE FUNCTION protect_completed_terminal_artifact() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM executor_terminal_reductions reduction
        WHERE reduction.customer_artifact_id = OLD.artifact_id
          AND reduction.state = 'completed'
    ) THEN
        RAISE EXCEPTION 'completed terminal reduction artifact is immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER artifacts_protect_completed_terminal
    BEFORE UPDATE OR DELETE ON artifacts
    FOR EACH ROW EXECUTE FUNCTION protect_completed_terminal_artifact();

CREATE FUNCTION reject_artifact_truncate() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'customer artifacts are durable and cannot be truncated';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER artifacts_reject_truncate
    BEFORE TRUNCATE ON artifacts
    FOR EACH STATEMENT EXECUTE FUNCTION reject_artifact_truncate();
