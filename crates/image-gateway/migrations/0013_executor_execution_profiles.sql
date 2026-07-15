CREATE TABLE provider_credential_pools (
    credential_pool_id UUID PRIMARY KEY,
    pool_key TEXT NOT NULL UNIQUE CHECK (
        char_length(pool_key) BETWEEN 1 AND 128
        AND pool_key ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    state TEXT NOT NULL CHECK (state IN ('enabled', 'disabled')),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (credential_pool_id, provider_id)
);

CREATE TABLE provider_accounts (
    provider_account_id UUID PRIMARY KEY,
    credential_pool_id UUID NOT NULL,
    provider_id TEXT NOT NULL,
    account_key TEXT NOT NULL CHECK (
        char_length(account_key) BETWEEN 1 AND 128
        AND account_key ~ '^[A-Za-z0-9_.-]+$'
    ),
    credential_ref TEXT NOT NULL CHECK (
        char_length(credential_ref) BETWEEN 1 AND 1024
        AND credential_ref !~ '[[:cntrl:]]'
    ),
    credential_revision BIGINT NOT NULL CHECK (credential_revision > 0),
    credential_auth_sha256 TEXT NOT NULL CHECK (
        credential_auth_sha256 ~ '^[0-9a-f]{64}$'
    ),
    state TEXT NOT NULL CHECK (state IN ('enabled', 'disabled')),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (credential_pool_id, provider_id)
        REFERENCES provider_credential_pools(credential_pool_id, provider_id)
        ON DELETE RESTRICT,
    UNIQUE (credential_pool_id, account_key),
    UNIQUE (provider_account_id, credential_pool_id, provider_id),
    UNIQUE (
        provider_account_id, credential_pool_id, provider_id,
        credential_ref, credential_revision
    )
);

CREATE TABLE executor_resource_policies (
    resource_policy_id UUID NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    credential_pool_id UUID NOT NULL,
    provider_account_id UUID NOT NULL,
    provider_id TEXT NOT NULL,
    execution_class TEXT NOT NULL CHECK (
        char_length(execution_class) BETWEEN 1 AND 128
        AND execution_class ~ '^[A-Za-z0-9_.-]+$'
    ),
    max_concurrency INTEGER NOT NULL CHECK (max_concurrency BETWEEN 1 AND 1000000),
    allocated_count INTEGER NOT NULL DEFAULT 0 CHECK (
        allocated_count >= 0 AND allocated_count <= max_concurrency
    ),
    state TEXT NOT NULL CHECK (state IN ('enabled', 'disabled')),
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (resource_policy_id, revision),
    FOREIGN KEY (
        provider_account_id, credential_pool_id, provider_id
    ) REFERENCES provider_accounts (
        provider_account_id, credential_pool_id, provider_id
    ) ON DELETE RESTRICT,
    UNIQUE (
        resource_policy_id, revision, credential_pool_id,
        provider_account_id, provider_id
    )
);

CREATE UNIQUE INDEX executor_resource_policies_enabled_account_uidx
    ON executor_resource_policies (provider_account_id)
    WHERE state = 'enabled';

CREATE TABLE provider_execution_profiles (
    execution_profile_id UUID PRIMARY KEY,
    profile_key TEXT NOT NULL UNIQUE CHECK (
        char_length(profile_key) BETWEEN 1 AND 128
        AND profile_key ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    command_schema TEXT NOT NULL CHECK (
        char_length(command_schema) BETWEEN 1 AND 128
        AND command_schema ~ '^[A-Za-z0-9_.-]+$'
    ),
    adapter_revision TEXT NOT NULL CHECK (
        char_length(adapter_revision) BETWEEN 1 AND 128
        AND adapter_revision ~ '^[A-Za-z0-9_.-]+$'
    ),
    credential_pool_id UUID NOT NULL,
    provider_account_id UUID NOT NULL,
    credential_ref TEXT NOT NULL,
    credential_revision BIGINT NOT NULL,
    resource_policy_id UUID NOT NULL,
    resource_policy_revision BIGINT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('enabled', 'disabled')),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (
        provider_account_id, credential_pool_id, provider_id,
        credential_ref, credential_revision
    ) REFERENCES provider_accounts (
        provider_account_id, credential_pool_id, provider_id,
        credential_ref, credential_revision
    ) ON DELETE RESTRICT,
    FOREIGN KEY (
        resource_policy_id, resource_policy_revision, credential_pool_id,
        provider_account_id, provider_id
    ) REFERENCES executor_resource_policies (
        resource_policy_id, revision, credential_pool_id,
        provider_account_id, provider_id
    )
        ON DELETE RESTRICT,
    UNIQUE (
        provider_id, command_schema, adapter_revision, credential_pool_id,
        provider_account_id, credential_ref, credential_revision,
        resource_policy_id, resource_policy_revision
    ),
    UNIQUE (
        execution_profile_id, credential_pool_id, provider_account_id,
        credential_ref, credential_revision, resource_policy_id,
        resource_policy_revision, provider_id, command_schema, adapter_revision
    )
);

CREATE FUNCTION enforce_execution_binding_identity() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'execution binding identities cannot be deleted';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF to_jsonb(NEW) - 'state' - 'updated_at_ms'
           IS DISTINCT FROM to_jsonb(OLD) - 'state' - 'updated_at_ms' THEN
            RAISE EXCEPTION 'execution binding identity is immutable';
        END IF;
        IF NEW.updated_at_ms < OLD.updated_at_ms THEN
            RAISE EXCEPTION 'execution binding timestamp cannot move backwards';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_credential_pools_identity
    BEFORE UPDATE OR DELETE ON provider_credential_pools
    FOR EACH ROW EXECUTE FUNCTION enforce_execution_binding_identity();
CREATE TRIGGER provider_accounts_identity
    BEFORE UPDATE OR DELETE ON provider_accounts
    FOR EACH ROW EXECUTE FUNCTION enforce_execution_binding_identity();
CREATE TRIGGER provider_execution_profiles_identity
    BEFORE UPDATE OR DELETE ON provider_execution_profiles
    FOR EACH ROW EXECUTE FUNCTION enforce_execution_binding_identity();

CREATE FUNCTION enforce_resource_policy_allocation_counter() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'executor resource policy revisions are immutable';
    END IF;
    IF TG_OP = 'INSERT' AND NEW.allocated_count <> 0 THEN
        RAISE EXCEPTION 'new executor resource policy must start with zero allocations';
    END IF;
    IF TG_OP = 'INSERT' AND NEW.state = 'enabled' AND EXISTS (
        SELECT 1 FROM executor_resource_policies policy
        WHERE policy.provider_account_id = NEW.provider_account_id
          AND policy.allocated_count > 0
    ) THEN
        RAISE EXCEPTION 'provider account still has held capacity on another policy revision';
    END IF;
    IF TG_OP = 'INSERT' THEN
        RETURN NEW;
    END IF;
    IF to_jsonb(NEW) - 'allocated_count' - 'state'
       IS DISTINCT FROM to_jsonb(OLD) - 'allocated_count' - 'state' THEN
        RAISE EXCEPTION 'executor resource policy configuration is immutable';
    END IF;
    IF OLD.state = 'disabled' AND NEW.state = 'enabled' AND EXISTS (
        SELECT 1 FROM executor_resource_policies policy
        WHERE policy.provider_account_id = NEW.provider_account_id
          AND (policy.resource_policy_id, policy.revision)
              <> (NEW.resource_policy_id, NEW.revision)
          AND policy.allocated_count > 0
    ) THEN
        RAISE EXCEPTION 'provider account still has held capacity on another policy revision';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_resource_policies_allocation_counter
    BEFORE INSERT OR UPDATE OR DELETE ON executor_resource_policies
    FOR EACH ROW EXECUTE FUNCTION enforce_resource_policy_allocation_counter();

LOCK TABLE provider_submissions, executor_executions IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_submissions submission
        JOIN executor_executions execution
          ON execution.executor_execution_id = submission.executor_execution_id
         AND execution.submission_id = submission.submission_id
        WHERE submission.state IN ('prepared', 'running')
           OR execution.state IN ('prepared', 'leased', 'running')
    ) THEN
        RAISE EXCEPTION
            'execution profile migration requires active executor submissions to be drained';
    END IF;
END;
$$;

ALTER TABLE provider_submissions
    ADD COLUMN execution_profile_id UUID,
    ADD COLUMN credential_pool_id UUID,
    ADD COLUMN provider_account_id UUID,
    ADD COLUMN credential_ref TEXT,
    ADD COLUMN credential_revision BIGINT,
    ADD COLUMN adapter_revision TEXT,
    ADD COLUMN resource_policy_id UUID,
    ADD COLUMN resource_policy_revision BIGINT,
    ADD CONSTRAINT provider_submission_binding_all_or_none CHECK (
        (execution_profile_id IS NULL
            AND credential_pool_id IS NULL
            AND provider_account_id IS NULL
            AND credential_ref IS NULL
            AND credential_revision IS NULL
            AND adapter_revision IS NULL
            AND resource_policy_id IS NULL
            AND resource_policy_revision IS NULL)
        OR
        (execution_profile_id IS NOT NULL
            AND credential_pool_id IS NOT NULL
            AND provider_account_id IS NOT NULL
            AND credential_ref IS NOT NULL
            AND credential_revision IS NOT NULL
            AND adapter_revision IS NOT NULL
            AND resource_policy_id IS NOT NULL
            AND resource_policy_revision IS NOT NULL)
    ),
    ADD CONSTRAINT provider_submission_execution_profile_fk FOREIGN KEY (
        execution_profile_id, credential_pool_id, provider_account_id,
        credential_ref, credential_revision, resource_policy_id,
        resource_policy_revision, provider_id, command_schema, adapter_revision
    ) REFERENCES provider_execution_profiles (
        execution_profile_id, credential_pool_id, provider_account_id,
        credential_ref, credential_revision, resource_policy_id,
        resource_policy_revision, provider_id, command_schema, adapter_revision
    ) ON DELETE RESTRICT,
    ADD CONSTRAINT provider_submission_bound_identity_unique UNIQUE (
        executor_execution_id, submission_id, execution_profile_id,
        resource_policy_id, resource_policy_revision
    );

ALTER TABLE work_items
    ADD COLUMN execution_profile_id UUID REFERENCES provider_execution_profiles(execution_profile_id)
        ON DELETE RESTRICT;

CREATE FUNCTION enforce_work_execution_profile_fence() RETURNS TRIGGER AS $$
BEGIN
    IF OLD.execution_profile_id IS NOT NULL
       AND NEW.execution_profile_id IS DISTINCT FROM OLD.execution_profile_id THEN
        RAISE EXCEPTION 'work execution profile is immutable once selected';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER work_items_execution_profile_fence
    BEFORE UPDATE ON work_items
    FOR EACH ROW EXECUTE FUNCTION enforce_work_execution_profile_fence();

CREATE FUNCTION validate_bound_provider_submission() RETURNS TRIGGER AS $$
DECLARE
    profile_enabled BOOLEAN;
BEGIN
    IF NEW.execution_profile_id IS NULL THEN
        RAISE EXCEPTION 'new provider submissions require an execution profile';
    END IF;
    SELECT p.state = 'enabled'
           AND pool.state = 'enabled'
           AND account.state = 'enabled'
           AND policy.state = 'enabled'
    INTO profile_enabled
    FROM provider_execution_profiles p
    JOIN provider_credential_pools pool
      ON pool.credential_pool_id = p.credential_pool_id
     AND pool.provider_id = p.provider_id
    JOIN provider_accounts account
      ON account.provider_account_id = p.provider_account_id
     AND account.credential_pool_id = p.credential_pool_id
     AND account.provider_id = p.provider_id
     AND account.credential_ref = p.credential_ref
     AND account.credential_revision = p.credential_revision
    JOIN executor_resource_policies policy
      ON policy.resource_policy_id = p.resource_policy_id
     AND policy.revision = p.resource_policy_revision
    WHERE p.execution_profile_id = NEW.execution_profile_id
      AND p.provider_id = NEW.provider_id
      AND p.command_schema = NEW.command_schema
      AND p.adapter_revision = NEW.adapter_revision
      AND p.credential_pool_id = NEW.credential_pool_id
      AND p.provider_account_id = NEW.provider_account_id
      AND p.credential_ref = NEW.credential_ref
      AND p.credential_revision = NEW.credential_revision
      AND p.resource_policy_id = NEW.resource_policy_id
      AND p.resource_policy_revision = NEW.resource_policy_revision;
    IF profile_enabled IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION 'provider submission execution profile is unavailable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_submissions_require_active_binding
    BEFORE INSERT ON provider_submissions
    FOR EACH ROW EXECUTE FUNCTION validate_bound_provider_submission();

CREATE OR REPLACE FUNCTION enforce_provider_submission_state_transition() RETURNS TRIGGER AS $$
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
       OR NEW.prepared_at_ms IS DISTINCT FROM OLD.prepared_at_ms
       OR NEW.execution_profile_id IS DISTINCT FROM OLD.execution_profile_id
       OR NEW.credential_pool_id IS DISTINCT FROM OLD.credential_pool_id
       OR NEW.provider_account_id IS DISTINCT FROM OLD.provider_account_id
       OR NEW.credential_ref IS DISTINCT FROM OLD.credential_ref
       OR NEW.credential_revision IS DISTINCT FROM OLD.credential_revision
       OR NEW.adapter_revision IS DISTINCT FROM OLD.adapter_revision
       OR NEW.resource_policy_id IS DISTINCT FROM OLD.resource_policy_id
       OR NEW.resource_policy_revision IS DISTINCT FROM OLD.resource_policy_revision THEN
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

CREATE TABLE executor_capacity_allocations (
    allocation_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    submission_id UUID NOT NULL UNIQUE,
    execution_profile_id UUID NOT NULL,
    resource_policy_id UUID NOT NULL,
    resource_policy_revision BIGINT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('held', 'released')),
    acquired_at_ms BIGINT NOT NULL,
    last_heartbeat_at_ms BIGINT NOT NULL,
    released_at_ms BIGINT,
    release_decision_id UUID,
    released_state TEXT CHECK (
        released_state IS NULL
        OR released_state IN ('succeeded', 'failed', 'uncertain', 'canceled')
    ),
    release_reason TEXT CHECK (
        release_reason IS NULL
        OR release_reason IN ('terminal_evidence', 'executor_start_abandoned')
    ),
    CHECK (allocation_id = executor_execution_id),
    CHECK (
        (state = 'held' AND released_at_ms IS NULL AND release_reason IS NULL
            AND release_decision_id IS NULL AND released_state IS NULL)
        OR
        (state = 'released' AND released_at_ms IS NOT NULL AND release_reason IS NOT NULL
            AND release_decision_id IS NOT NULL AND released_state IS NOT NULL)
    ),
    FOREIGN KEY (
        executor_execution_id, submission_id, execution_profile_id,
        resource_policy_id, resource_policy_revision
    ) REFERENCES provider_submissions (
        executor_execution_id, submission_id, execution_profile_id,
        resource_policy_id, resource_policy_revision
    ) ON DELETE RESTRICT,
    FOREIGN KEY (
        release_decision_id, executor_execution_id, submission_id, released_state
    ) REFERENCES executor_resolution_decisions (
        decision_id, executor_execution_id, submission_id, resolved_state
    ) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX executor_capacity_allocations_held_execution_idx
    ON executor_capacity_allocations (execution_profile_id, executor_execution_id)
    WHERE state = 'held';

CREATE INDEX executor_capacity_allocations_orphan_idx
    ON executor_capacity_allocations (last_heartbeat_at_ms, executor_execution_id)
    WHERE state = 'held';

CREATE FUNCTION enforce_executor_capacity_allocation_transition() RETURNS TRIGGER AS $$
DECLARE
    decision_source TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'executor capacity allocations are durable';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'held' OR NEW.released_at_ms IS NOT NULL
           OR NEW.release_reason IS NOT NULL OR NEW.release_decision_id IS NOT NULL
           OR NEW.released_state IS NOT NULL THEN
            RAISE EXCEPTION 'executor capacity allocation must be inserted held';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.allocation_id IS DISTINCT FROM OLD.allocation_id
       OR NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.execution_profile_id IS DISTINCT FROM OLD.execution_profile_id
       OR NEW.resource_policy_id IS DISTINCT FROM OLD.resource_policy_id
       OR NEW.resource_policy_revision IS DISTINCT FROM OLD.resource_policy_revision
       OR NEW.acquired_at_ms IS DISTINCT FROM OLD.acquired_at_ms THEN
        RAISE EXCEPTION 'executor capacity allocation identity is immutable';
    END IF;
    IF OLD.state = 'released' THEN
        RAISE EXCEPTION 'released executor capacity allocation is immutable';
    END IF;
    IF NEW.last_heartbeat_at_ms < OLD.last_heartbeat_at_ms THEN
        RAISE EXCEPTION 'executor capacity heartbeat cannot move backwards';
    END IF;
    IF NOT (
        (OLD.state = 'held' AND NEW.state = 'held'
            AND NEW.released_at_ms IS NULL AND NEW.release_reason IS NULL
            AND NEW.release_decision_id IS NULL AND NEW.released_state IS NULL)
        OR
        (OLD.state = 'held' AND NEW.state = 'released'
            AND NEW.released_at_ms IS NOT NULL AND NEW.release_reason IS NOT NULL
            AND NEW.release_decision_id IS NOT NULL AND NEW.released_state IS NOT NULL)
    ) THEN
        RAISE EXCEPTION 'invalid executor capacity allocation transition';
    END IF;
    IF NEW.state = 'released' THEN
        SELECT source INTO decision_source
        FROM executor_resolution_decisions
        WHERE decision_id = NEW.release_decision_id
          AND executor_execution_id = NEW.executor_execution_id
          AND submission_id = NEW.submission_id
          AND resolved_state = NEW.released_state;
        IF NEW.release_reason = 'terminal_evidence' THEN
            IF decision_source IS NULL OR NOT EXISTS (
                SELECT 1 FROM executor_runner_observations observation
                WHERE observation.executor_execution_id = NEW.executor_execution_id
                  AND observation.submission_id = NEW.submission_id
            ) THEN
                RAISE EXCEPTION 'terminal capacity release requires durable runner evidence';
            END IF;
        ELSIF NEW.release_reason = 'executor_start_abandoned' THEN
            IF decision_source IS DISTINCT FROM 'executor_start_abandoned'
               OR NEW.released_state IS DISTINCT FROM 'canceled' THEN
                RAISE EXCEPTION 'abandoned capacity release requires its fenced decision';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_capacity_allocations_transition
    BEFORE INSERT OR UPDATE OR DELETE ON executor_capacity_allocations
    FOR EACH ROW EXECUTE FUNCTION enforce_executor_capacity_allocation_transition();

CREATE FUNCTION enforce_executor_capacity_counter_balance() RETURNS TRIGGER AS $$
DECLARE
    policy_id UUID;
    policy_revision BIGINT;
    stored_count INTEGER;
    held_count BIGINT;
BEGIN
    IF TG_TABLE_NAME = 'executor_resource_policies' THEN
        policy_id := NEW.resource_policy_id;
        policy_revision := NEW.revision;
    ELSE
        policy_id := NEW.resource_policy_id;
        policy_revision := NEW.resource_policy_revision;
    END IF;
    SELECT allocated_count INTO stored_count
    FROM executor_resource_policies
    WHERE resource_policy_id = policy_id AND revision = policy_revision;
    SELECT COUNT(*) INTO held_count
    FROM executor_capacity_allocations
    WHERE resource_policy_id = policy_id
      AND resource_policy_revision = policy_revision
      AND state = 'held';
    IF stored_count IS NULL OR stored_count::BIGINT <> held_count THEN
        RAISE EXCEPTION 'executor capacity allocation counter is unbalanced';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER executor_resource_policy_counter_balance
    AFTER INSERT OR UPDATE ON executor_resource_policies
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_executor_capacity_counter_balance();

CREATE CONSTRAINT TRIGGER executor_capacity_allocation_counter_balance
    AFTER INSERT OR UPDATE ON executor_capacity_allocations
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_executor_capacity_counter_balance();
