-- Explicit, append-only operator risk acceptance. This is not terminal evidence,
-- a provider cancellation, a financial waiver, or permission to replay a job.
CREATE TABLE executor_capacity_risk_waivers (
    allocation_id UUID PRIMARY KEY REFERENCES executor_capacity_allocations(allocation_id),
    submission_id UUID NOT NULL UNIQUE REFERENCES provider_submissions(submission_id),
    executor_execution_id UUID NOT NULL UNIQUE REFERENCES executor_executions(executor_execution_id),
    provider_account_id UUID NOT NULL REFERENCES provider_accounts(provider_account_id),
    resource_policy_id UUID NOT NULL,
    resource_policy_revision BIGINT NOT NULL,
    original_record_sha256 TEXT NOT NULL CHECK (original_record_sha256 ~ '^[a-f0-9]{64}$'),
    authorization_id UUID NOT NULL,
    authorization_manifest_sha256 TEXT NOT NULL CHECK (authorization_manifest_sha256 ~ '^[a-f0-9]{64}$'),
    operator_reference TEXT NOT NULL CHECK (length(operator_reference) BETWEEN 1 AND 256 AND operator_reference !~ '[[:cntrl:]]'),
    actor TEXT NOT NULL CHECK (length(actor) BETWEEN 1 AND 128 AND actor !~ '[[:cntrl:]]'),
    risk_category TEXT NOT NULL CHECK (risk_category = 'unknown_original_may_still_occupy_upstream_capacity'),
    authorized_before_ms BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    FOREIGN KEY (resource_policy_id, resource_policy_revision)
        REFERENCES executor_resource_policies(resource_policy_id, revision),
    CHECK (allocation_id = executor_execution_id),
    CHECK (created_at_ms >= authorized_before_ms)
);
CREATE INDEX executor_capacity_risk_waivers_policy_idx
    ON executor_capacity_risk_waivers(resource_policy_id, resource_policy_revision);

CREATE TRIGGER executor_capacity_risk_waivers_immutable BEFORE UPDATE OR DELETE
    ON executor_capacity_risk_waivers FOR EACH ROW EXECUTE FUNCTION reject_executor_evidence_mutation();
CREATE TRIGGER executor_capacity_risk_waivers_no_truncate BEFORE TRUNCATE
    ON executor_capacity_risk_waivers FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_evidence_mutation();

-- Only safe metadata is hashed. No prompt, material URL, credential, or raw CLI
-- payload is copied into the operator manifest or its receipt.
CREATE FUNCTION executor_capacity_original_record_sha256(target UUID) RETURNS TEXT
LANGUAGE SQL STABLE AS $$
    SELECT encode(sha256(convert_to(jsonb_build_object(
        'allocation', to_jsonb(a),
        'submission', jsonb_build_object('id', s.submission_id, 'execution', s.executor_execution_id,
            'job', s.job_id, 'account', s.provider_account_id, 'provider', s.provider_id,
            'command_hash', s.command_hash, 'state', s.state, 'updated_at_ms', s.updated_at_ms),
        'execution', jsonb_build_object('id', e.executor_execution_id, 'state', e.state,
            'lease_epoch', e.lease_epoch, 'updated_at_ms', e.updated_at_ms,
            'resolution_decision_id', e.resolution_decision_id),
        'intent', jsonb_build_object('state', i.state, 'remote_operation_id', i.remote_operation_id,
            'updated_at_ms', i.updated_at_ms),
        'reconciliation', jsonb_build_object('state', r.state, 'evidence_revision', r.evidence_revision,
            'payload_hash', r.payload_hash)
    )::TEXT, 'UTF8')), 'hex')
    FROM executor_capacity_allocations a
    JOIN provider_submissions s USING (submission_id, executor_execution_id)
    JOIN executor_executions e USING (submission_id, executor_execution_id)
    JOIN provider_remote_submit_intents i USING (submission_id, executor_execution_id)
    JOIN provider_capacity_reconciliations r USING (submission_id, executor_execution_id)
    WHERE a.allocation_id = target
$$;

CREATE FUNCTION executor_waived_held_count(policy_id UUID, policy_revision BIGINT) RETURNS INTEGER
LANGUAGE SQL STABLE AS $$
    SELECT COUNT(*)::INTEGER FROM executor_capacity_risk_waivers w
    JOIN executor_capacity_allocations a USING (allocation_id)
    WHERE w.resource_policy_id = policy_id AND w.resource_policy_revision = policy_revision
      AND a.state = 'held'
$$;

CREATE FUNCTION executor_effective_allocated_count(policy_id UUID, policy_revision BIGINT) RETURNS INTEGER
LANGUAGE SQL STABLE AS $$
    SELECT allocated_count - executor_waived_held_count(policy_id, policy_revision)
    FROM executor_resource_policies WHERE resource_policy_id = policy_id AND revision = policy_revision
$$;

CREATE FUNCTION validate_executor_capacity_risk_waiver() RETURNS TRIGGER AS $$
BEGIN
    -- Match the normal reconciliation lock order; never update the frozen rows.
    PERFORM 1 FROM executor_executions e
    JOIN provider_submissions s USING (executor_execution_id, submission_id)
    JOIN executor_capacity_allocations a USING (executor_execution_id, submission_id)
    WHERE a.allocation_id = NEW.allocation_id
      AND a.submission_id = NEW.submission_id AND a.executor_execution_id = NEW.executor_execution_id
      AND a.resource_policy_id = NEW.resource_policy_id AND a.resource_policy_revision = NEW.resource_policy_revision
      AND s.provider_account_id = NEW.provider_account_id AND s.provider_id = 'dreamina-cli'
      AND a.state = 'held' AND a.acquired_at_ms <= NEW.authorized_before_ms
      AND e.state = 'uncertain' AND s.state = 'uncertain'
    FOR UPDATE OF e, s, a;
    IF NOT FOUND THEN RAISE EXCEPTION 'capacity risk waiver scope is not an eligible original'; END IF;
    PERFORM 1 FROM provider_remote_submit_intents
    WHERE submission_id = NEW.submission_id AND state = 'deadline_quarantined'
      AND remote_operation_id IS NULL FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'capacity risk waiver requires unknown original intent'; END IF;
    PERFORM 1 FROM provider_capacity_reconciliations
    WHERE submission_id = NEW.submission_id AND state = 'active' AND evidence_revision = 0
    FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'capacity risk waiver conflicts with capacity evidence'; END IF;
    PERFORM 1 FROM executor_resource_policies
    WHERE resource_policy_id = NEW.resource_policy_id AND revision = NEW.resource_policy_revision
      AND provider_account_id = NEW.provider_account_id FOR UPDATE;
    IF NOT FOUND OR executor_capacity_original_record_sha256(NEW.allocation_id)
        IS DISTINCT FROM NEW.original_record_sha256 THEN
        RAISE EXCEPTION 'capacity risk waiver reviewed original changed';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(NEW.authorization_id::TEXT,0));
    IF EXISTS (SELECT 1 FROM executor_capacity_risk_waivers
        WHERE authorization_id = NEW.authorization_id
          AND authorization_manifest_sha256 <> NEW.authorization_manifest_sha256) THEN
        RAISE EXCEPTION 'capacity risk waiver authorization cannot be reused';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER executor_capacity_risk_waivers_insert BEFORE INSERT ON executor_capacity_risk_waivers
    FOR EACH ROW EXECUTE FUNCTION validate_executor_capacity_risk_waiver();

-- Preserve the raw counter == held invariant and revision-switch guard. Only
-- the upper bound changes: waived originals can remain permanently frozen.
ALTER TABLE executor_resource_policies DROP CONSTRAINT executor_resource_policies_check;
ALTER TABLE executor_resource_policies ADD CONSTRAINT executor_resource_policy_raw_nonnegative
    CHECK (allocated_count >= 0);

CREATE FUNCTION enforce_executor_effective_capacity_limit() RETURNS TRIGGER AS $$
DECLARE policy_id UUID; policy_revision BIGINT; effective_count INTEGER; hard_limit INTEGER;
BEGIN
    -- Heartbeats do not change membership. Preserve the existing heartbeat
    -- contract: no policy lock/scan when the immutable identity and state stay.
    IF TG_TABLE_NAME = 'executor_capacity_allocations' AND TG_OP = 'UPDATE' THEN
        IF NEW.state = OLD.state THEN RETURN NULL; END IF;
    END IF;
    policy_id := NEW.resource_policy_id;
    IF TG_TABLE_NAME = 'executor_resource_policies' THEN policy_revision := NEW.revision;
    ELSE policy_revision := NEW.resource_policy_revision; END IF;
    SELECT max_concurrency INTO hard_limit FROM executor_resource_policies
    WHERE resource_policy_id = policy_id AND revision = policy_revision FOR UPDATE;
    effective_count := executor_effective_allocated_count(policy_id, policy_revision);
    IF effective_count IS NULL OR effective_count < 0 OR effective_count > hard_limit THEN
        RAISE EXCEPTION 'effective executor capacity exceeds its policy limit';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER executor_resource_policy_effective_limit
    AFTER INSERT OR UPDATE ON executor_resource_policies DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_executor_effective_capacity_limit();
CREATE CONSTRAINT TRIGGER executor_allocation_effective_limit
    AFTER INSERT OR UPDATE ON executor_capacity_allocations DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_executor_effective_capacity_limit();
CREATE CONSTRAINT TRIGGER executor_waiver_effective_limit
    AFTER INSERT ON executor_capacity_risk_waivers DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_executor_effective_capacity_limit();
