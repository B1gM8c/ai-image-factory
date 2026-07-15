DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_submissions s
        LEFT JOIN executor_executions e
          ON e.executor_execution_id = s.executor_execution_id
         AND e.submission_id = s.submission_id
        WHERE e.executor_execution_id IS NULL
           OR e.state NOT IN ('prepared', 'leased')
           OR s.state <> 'prepared'
    ) THEN
        RAISE EXCEPTION
            'executor observation migration requires aligned prepared or leased executions';
    END IF;
END;
$$;

ALTER TABLE provider_submissions
    ADD CONSTRAINT provider_submission_executor_identity_check CHECK (
        submission_id <> '00000000-0000-0000-0000-000000000000'::UUID
        AND executor_execution_id <> '00000000-0000-0000-0000-000000000000'::UUID
        AND submission_id <> executor_execution_id
    );

ALTER TABLE executor_executions
    ADD COLUMN launch_owner TEXT,
    ADD COLUMN launch_lease_epoch BIGINT,
    ADD CONSTRAINT executor_execution_launch_fence_check CHECK (
        (state IN ('prepared', 'leased', 'canceled')
            AND launch_owner IS NULL AND launch_lease_epoch IS NULL)
        OR
        (state IN ('running', 'succeeded', 'failed', 'uncertain')
            AND launch_owner IS NOT NULL AND launch_owner <> ''
            AND launch_lease_epoch IS NOT NULL AND launch_lease_epoch > 0)
    ),
    ADD CONSTRAINT executor_execution_launch_fence_unique UNIQUE (
        executor_execution_id, submission_id, launch_owner, launch_lease_epoch
    ),
    ADD CONSTRAINT executor_execution_identity_check CHECK (
        executor_execution_id <> '00000000-0000-0000-0000-000000000000'::UUID
        AND submission_id <> '00000000-0000-0000-0000-000000000000'::UUID
        AND executor_execution_id <> submission_id
    );

CREATE FUNCTION enforce_executor_launch_fence_write_once() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'prepared'
           OR NEW.launch_owner IS NOT NULL
           OR NEW.launch_lease_epoch IS NOT NULL THEN
            RAISE EXCEPTION 'executor execution must be inserted prepared without a launch fence';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.launch_owner IS NOT NULL OR OLD.launch_lease_epoch IS NOT NULL THEN
        IF NEW.launch_owner IS DISTINCT FROM OLD.launch_owner
           OR NEW.launch_lease_epoch IS DISTINCT FROM OLD.launch_lease_epoch THEN
            RAISE EXCEPTION 'executor launch fence is immutable';
        END IF;
    ELSIF NEW.launch_owner IS NOT NULL OR NEW.launch_lease_epoch IS NOT NULL THEN
        IF OLD.state <> 'leased' OR NEW.state <> 'running'
           OR NEW.launch_owner IS DISTINCT FROM OLD.executor_owner
           OR NEW.launch_lease_epoch IS DISTINCT FROM OLD.lease_epoch THEN
            RAISE EXCEPTION 'executor launch fence may only be set by the leased-to-running transition';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_execution_launch_fence_write_once
    BEFORE INSERT OR UPDATE ON executor_executions
    FOR EACH ROW EXECUTE FUNCTION enforce_executor_launch_fence_write_once();

CREATE FUNCTION enforce_executor_lease_updates() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms THEN
        RAISE EXCEPTION 'executor execution identity is immutable';
    END IF;
    IF OLD.state IN ('succeeded', 'failed', 'uncertain', 'canceled') THEN
        RAISE EXCEPTION 'terminal executor execution is immutable';
    END IF;
    IF NOT (
        (OLD.state = 'prepared' AND NEW.state = 'leased')
        OR (OLD.state = 'leased' AND NEW.state IN ('leased', 'running', 'canceled'))
        OR (OLD.state = 'running'
            AND NEW.state IN ('running', 'succeeded', 'failed', 'uncertain'))
    ) THEN
        RAISE EXCEPTION 'invalid executor execution state transition';
    END IF;
    IF OLD.state = 'prepared' AND NEW.state = 'leased' THEN
        IF NEW.lease_epoch <> OLD.lease_epoch + 1
           OR NEW.lease_expires_at_ms IS NULL
           OR NEW.lease_expires_at_ms <= now_ms THEN
            RAISE EXCEPTION 'executor claim must create a future monotonic lease fence';
        END IF;
    ELSIF OLD.state = 'leased' AND NEW.state = 'leased' THEN
        IF NEW.executor_owner IS DISTINCT FROM OLD.executor_owner
           OR NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch THEN
            IF OLD.lease_expires_at_ms > now_ms
               OR NEW.lease_epoch <> OLD.lease_epoch + 1
               OR NEW.lease_expires_at_ms <= now_ms THEN
                RAISE EXCEPTION 'executor reclaim requires an expired lease and the next epoch';
            END IF;
        ELSIF NEW.lease_expires_at_ms IS DISTINCT FROM OLD.lease_expires_at_ms THEN
            RAISE EXCEPTION 'leased executor cannot renew the same lease fence';
        END IF;
    ELSIF OLD.state = 'leased' AND NEW.state = 'running' THEN
        IF NEW.executor_owner IS DISTINCT FROM OLD.executor_owner
           OR NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR NEW.lease_expires_at_ms IS DISTINCT FROM OLD.lease_expires_at_ms
           OR NEW.lease_expires_at_ms <= now_ms THEN
            RAISE EXCEPTION 'executor start must preserve a live lease fence';
        END IF;
    ELSIF OLD.state = 'running' AND NEW.state = 'running' THEN
        IF NEW.executor_owner IS DISTINCT FROM OLD.executor_owner
           OR NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR OLD.lease_expires_at_ms <= now_ms
           OR NEW.lease_expires_at_ms <= now_ms
           OR NEW.lease_expires_at_ms < OLD.lease_expires_at_ms THEN
            RAISE EXCEPTION 'running executor lease fence is immutable and monotonic';
        END IF;
    END IF;
    IF OLD.state = 'running'
       AND (NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
            OR NEW.leased_at_ms IS DISTINCT FROM OLD.leased_at_ms
            OR NEW.started_at_ms IS DISTINCT FROM OLD.started_at_ms) THEN
        RAISE EXCEPTION 'running executor history is immutable';
    END IF;
    IF OLD.state = 'leased' AND NEW.state IN ('running', 'canceled')
       AND (NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
            OR NEW.leased_at_ms IS DISTINCT FROM OLD.leased_at_ms) THEN
        RAISE EXCEPTION 'leased executor history is immutable across resolution';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_execution_lease_updates
    BEFORE UPDATE ON executor_executions
    FOR EACH ROW EXECUTE FUNCTION enforce_executor_lease_updates();

CREATE FUNCTION enforce_provider_submission_state_transition() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.output_id IS DISTINCT FROM OLD.output_id
       OR NEW.job_id IS DISTINCT FROM OLD.job_id
       OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.model IS DISTINCT FROM OLD.model
       OR NEW.work_item_id IS DISTINCT FROM OLD.work_item_id
       OR NEW.created_by_execution_id IS DISTINCT FROM OLD.created_by_execution_id
       OR NEW.created_by_lease_epoch IS DISTINCT FROM OLD.created_by_lease_epoch
       OR NEW.command_schema IS DISTINCT FROM OLD.command_schema
       OR NEW.command_hash IS DISTINCT FROM OLD.command_hash
       OR NEW.prepared_at_ms IS DISTINCT FROM OLD.prepared_at_ms THEN
        RAISE EXCEPTION 'provider submission identity and command are immutable';
    END IF;
    IF OLD.state IN ('succeeded', 'failed', 'uncertain', 'canceled') THEN
        RAISE EXCEPTION 'terminal provider submission is immutable';
    END IF;
    IF NOT (
        (OLD.state = 'prepared' AND NEW.state IN ('running', 'canceled'))
        OR (OLD.state = 'running'
            AND NEW.state IN ('succeeded', 'failed', 'uncertain'))
    ) THEN
        RAISE EXCEPTION 'invalid provider submission state transition';
    END IF;
    IF OLD.state = 'running'
       AND NEW.started_at_ms IS DISTINCT FROM OLD.started_at_ms THEN
        RAISE EXCEPTION 'provider submission start history is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_submission_state_transition
    BEFORE UPDATE ON provider_submissions
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_submission_state_transition();

CREATE FUNCTION reject_executor_execution_deletion() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'executor executions are durable and cannot be deleted';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_executions_reject_delete
    BEFORE DELETE ON executor_executions
    FOR EACH ROW EXECUTE FUNCTION reject_executor_execution_deletion();

CREATE TRIGGER executor_executions_reject_truncate
    BEFORE TRUNCATE ON executor_executions
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_execution_deletion();

CREATE TABLE executor_runner_observations (
    observation_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    submission_id UUID NOT NULL UNIQUE,
    launch_owner TEXT NOT NULL CHECK (launch_owner <> ''),
    launch_lease_epoch BIGINT NOT NULL CHECK (launch_lease_epoch > 0),
    observed_state TEXT NOT NULL CHECK (
        observed_state IN ('succeeded', 'failed', 'uncertain')
    ),
    result_manifest_id UUID,
    error_code TEXT CHECK (
        error_code IS NULL OR error_code ~ '^[A-Za-z0-9_.-]{1,128}$'
    ),
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    resolution_fingerprint TEXT GENERATED ALWAYS AS (
        CASE
            WHEN observed_state = 'succeeded'
                THEN 'manifest:' || result_manifest_id::TEXT
            ELSE 'error:' || error_code
        END
    ) STORED,
    observed_at_ms BIGINT NOT NULL,
    CHECK (observation_id = executor_execution_id),
    CHECK (
        (observed_state = 'succeeded'
            AND result_manifest_id IS NOT NULL AND error_code IS NULL)
        OR
        (observed_state IN ('failed', 'uncertain')
            AND result_manifest_id IS NULL AND error_code IS NOT NULL)
    ),
    UNIQUE (observation_id, executor_execution_id, submission_id),
    UNIQUE (
        observation_id, executor_execution_id, submission_id,
        observed_state, resolution_fingerprint
    ),
    FOREIGN KEY (
        executor_execution_id, submission_id, launch_owner, launch_lease_epoch
    ) REFERENCES executor_executions (
        executor_execution_id, submission_id, launch_owner, launch_lease_epoch
    ) ON DELETE RESTRICT,
    FOREIGN KEY (result_manifest_id, executor_execution_id, submission_id)
        REFERENCES executor_result_manifests (
            manifest_id, executor_execution_id, submission_id
        ) ON DELETE RESTRICT
);

CREATE TABLE executor_resolution_decisions (
    decision_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    submission_id UUID NOT NULL UNIQUE,
    source TEXT NOT NULL CHECK (
        source IN (
            'active_runner_observation',
            'executor_lease_expired',
            'executor_start_abandoned'
        )
    ),
    observation_id UUID,
    resolved_state TEXT NOT NULL CHECK (
        resolved_state IN ('succeeded', 'failed', 'uncertain', 'canceled')
    ),
    result_manifest_id UUID,
    error_code TEXT CHECK (
        error_code IS NULL OR error_code ~ '^[A-Za-z0-9_.-]{1,128}$'
    ),
    resolution_fingerprint TEXT GENERATED ALWAYS AS (
        CASE
            WHEN resolved_state = 'succeeded'
                THEN 'manifest:' || result_manifest_id::TEXT
            ELSE 'error:' || error_code
        END
    ) STORED,
    decided_at_ms BIGINT NOT NULL,
    CHECK (decision_id = executor_execution_id),
    CHECK (
        (resolved_state = 'succeeded'
            AND result_manifest_id IS NOT NULL AND error_code IS NULL)
        OR
        (resolved_state IN ('failed', 'uncertain', 'canceled')
            AND result_manifest_id IS NULL AND error_code IS NOT NULL)
    ),
    CHECK (
        (source = 'active_runner_observation'
            AND observation_id IS NOT NULL
            AND resolved_state IN ('succeeded', 'failed', 'uncertain'))
        OR
        (source = 'executor_lease_expired'
            AND observation_id IS NULL AND resolved_state = 'uncertain'
            AND error_code = 'executor_lease_expired')
        OR
        (source = 'executor_start_abandoned'
            AND observation_id IS NULL AND resolved_state = 'canceled'
            AND error_code = 'executor_start_abandoned')
    ),
    UNIQUE (decision_id, executor_execution_id, submission_id, resolved_state),
    FOREIGN KEY (executor_execution_id, submission_id)
        REFERENCES executor_executions(executor_execution_id, submission_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (
        observation_id, executor_execution_id, submission_id,
        resolved_state, resolution_fingerprint
    ) REFERENCES executor_runner_observations (
        observation_id, executor_execution_id, submission_id,
        observed_state, resolution_fingerprint
    ) ON DELETE RESTRICT
);

CREATE FUNCTION validate_executor_resolution_decision() RETURNS TRIGGER AS $$
DECLARE
    executor_state TEXT;
    executor_owner_value TEXT;
    executor_epoch BIGINT;
    executor_expiry BIGINT;
    launch_owner_value TEXT;
    launch_epoch BIGINT;
    submission_state TEXT;
    work_state TEXT;
    now_ms BIGINT;
BEGIN
    SELECT e.state, e.executor_owner, e.lease_epoch, e.lease_expires_at_ms,
           e.launch_owner, e.launch_lease_epoch, s.state, w.state,
           floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
    INTO executor_state, executor_owner_value, executor_epoch, executor_expiry,
         launch_owner_value, launch_epoch, submission_state, work_state, now_ms
    FROM executor_executions e
    JOIN provider_submissions s
      ON s.executor_execution_id = e.executor_execution_id
     AND s.submission_id = e.submission_id
    JOIN work_items w ON w.work_item_id = s.work_item_id AND w.job_id = s.job_id
    WHERE e.executor_execution_id = NEW.executor_execution_id
      AND e.submission_id = NEW.submission_id
    FOR UPDATE OF e, s;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'resolution decision does not name a durable execution';
    END IF;
    IF NEW.source = 'active_runner_observation' THEN
        IF executor_state <> 'running' OR submission_state <> 'running'
           OR executor_owner_value IS DISTINCT FROM launch_owner_value
           OR executor_epoch IS DISTINCT FROM launch_epoch
           OR executor_expiry IS NULL OR executor_expiry <= now_ms THEN
            RAISE EXCEPTION 'active resolution requires the live launch fence';
        END IF;
    ELSIF NEW.source = 'executor_lease_expired' THEN
        IF executor_state <> 'running' OR submission_state <> 'running'
           OR executor_expiry IS NULL OR executor_expiry > now_ms THEN
            RAISE EXCEPTION 'expiry resolution requires an expired running execution';
        END IF;
    ELSIF NEW.source = 'executor_start_abandoned' THEN
        IF executor_state <> 'leased' OR submission_state <> 'prepared'
           OR launch_owner_value IS NOT NULL OR launch_epoch IS NOT NULL
           OR executor_expiry IS NULL OR executor_expiry > now_ms
           OR work_state NOT IN ('succeeded', 'failed', 'uncertain') THEN
            RAISE EXCEPTION 'abandoned resolution requires expired unstarted terminal work';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_resolution_decision_validate
    BEFORE INSERT ON executor_resolution_decisions
    FOR EACH ROW EXECUTE FUNCTION validate_executor_resolution_decision();

ALTER TABLE executor_executions
    ADD COLUMN resolution_decision_id UUID,
    ADD CONSTRAINT executor_execution_resolution_presence_check CHECK (
        (state IN ('prepared', 'leased', 'running') AND resolution_decision_id IS NULL)
        OR
        (state IN ('succeeded', 'failed', 'uncertain', 'canceled')
            AND resolution_decision_id IS NOT NULL)
    ),
    ADD CONSTRAINT executor_execution_resolution_fk FOREIGN KEY (
        resolution_decision_id, executor_execution_id, submission_id, state
    ) REFERENCES executor_resolution_decisions (
        decision_id, executor_execution_id, submission_id, resolved_state
    ) ON DELETE RESTRICT;

ALTER TABLE provider_submissions
    ADD COLUMN resolution_decision_id UUID,
    ADD CONSTRAINT provider_submission_resolution_presence_check CHECK (
        (state IN ('prepared', 'running') AND resolution_decision_id IS NULL)
        OR
        (state IN ('succeeded', 'failed', 'uncertain', 'canceled')
            AND resolution_decision_id IS NOT NULL)
    ),
    ADD CONSTRAINT provider_submission_resolution_fk FOREIGN KEY (
        resolution_decision_id, executor_execution_id, submission_id, state
    ) REFERENCES executor_resolution_decisions (
        decision_id, executor_execution_id, submission_id, resolved_state
    ) ON DELETE RESTRICT;

CREATE FUNCTION enforce_executor_resolution_projection() RETURNS TRIGGER AS $$
DECLARE
    executor_state TEXT;
    executor_decision UUID;
    submission_state TEXT;
    submission_decision UUID;
    submission_manifest UUID;
    submission_error TEXT;
    executor_error TEXT;
    decision_manifest UUID;
    decision_error TEXT;
BEGIN
    SELECT e.state, e.resolution_decision_id, e.error_code,
           s.state, s.resolution_decision_id, s.result_manifest_id, s.error_code
    INTO executor_state, executor_decision, executor_error,
         submission_state, submission_decision, submission_manifest, submission_error
    FROM executor_executions e
    JOIN provider_submissions s
      ON s.executor_execution_id = e.executor_execution_id
     AND s.submission_id = e.submission_id
    WHERE e.executor_execution_id = NEW.executor_execution_id
      AND e.submission_id = NEW.submission_id;

    IF executor_state IN ('prepared', 'leased') THEN
        IF submission_state <> 'prepared' THEN
            RAISE EXCEPTION 'executor and submission nonterminal projections are inconsistent';
        END IF;
    ELSIF executor_state = 'running' THEN
        IF submission_state <> 'running' THEN
            RAISE EXCEPTION 'executor and submission nonterminal projections are inconsistent';
        END IF;
    ELSE
        IF executor_state IS DISTINCT FROM submission_state
           OR executor_decision IS DISTINCT FROM submission_decision
           OR executor_decision IS NULL THEN
            RAISE EXCEPTION
                'executor and submission terminal resolution projections must match';
        END IF;
        SELECT result_manifest_id, error_code
        INTO decision_manifest, decision_error
        FROM executor_resolution_decisions
        WHERE decision_id = executor_decision
          AND executor_execution_id = NEW.executor_execution_id
          AND submission_id = NEW.submission_id;
        IF submission_manifest IS DISTINCT FROM decision_manifest
           OR submission_error IS DISTINCT FROM decision_error
           OR executor_error IS DISTINCT FROM decision_error THEN
            RAISE EXCEPTION
                'canonical terminal payload must match the resolution decision';
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER executor_execution_resolution_projection_check
    AFTER INSERT OR UPDATE ON executor_executions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_executor_resolution_projection();

CREATE CONSTRAINT TRIGGER provider_submission_resolution_projection_check
    AFTER INSERT OR UPDATE ON provider_submissions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_executor_resolution_projection();

CREATE FUNCTION enforce_resolution_decision_projection() RETURNS TRIGGER AS $$
DECLARE
    executor_state TEXT;
    executor_decision UUID;
    submission_state TEXT;
    submission_decision UUID;
BEGIN
    SELECT e.state, e.resolution_decision_id,
           s.state, s.resolution_decision_id
    INTO executor_state, executor_decision,
         submission_state, submission_decision
    FROM executor_executions e
    JOIN provider_submissions s
      ON s.executor_execution_id = e.executor_execution_id
     AND s.submission_id = e.submission_id
    WHERE e.executor_execution_id = NEW.executor_execution_id
      AND e.submission_id = NEW.submission_id;

    IF NOT FOUND
       OR executor_state IS DISTINCT FROM NEW.resolved_state
       OR submission_state IS DISTINCT FROM NEW.resolved_state
       OR executor_decision IS DISTINCT FROM NEW.decision_id
       OR submission_decision IS DISTINCT FROM NEW.decision_id THEN
        RAISE EXCEPTION 'resolution decision must be projected atomically';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER executor_resolution_decision_projection_check
    AFTER INSERT ON executor_resolution_decisions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_resolution_decision_projection();

CREATE FUNCTION reject_executor_evidence_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'executor observations and decisions are append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_runner_observations_reject_mutation
    BEFORE UPDATE OR DELETE ON executor_runner_observations
    FOR EACH ROW EXECUTE FUNCTION reject_executor_evidence_mutation();

CREATE TRIGGER executor_runner_observations_reject_truncate
    BEFORE TRUNCATE ON executor_runner_observations
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_evidence_mutation();

CREATE TRIGGER executor_resolution_decisions_reject_mutation
    BEFORE UPDATE OR DELETE ON executor_resolution_decisions
    FOR EACH ROW EXECUTE FUNCTION reject_executor_evidence_mutation();

CREATE TRIGGER executor_resolution_decisions_reject_truncate
    BEFORE TRUNCATE ON executor_resolution_decisions
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_evidence_mutation();
