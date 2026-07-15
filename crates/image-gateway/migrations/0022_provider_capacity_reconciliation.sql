LOCK TABLE executor_executions, provider_submissions,
    executor_capacity_allocations, provider_remote_submit_intents,
    provider_submit_recoveries, executor_resolution_decisions,
    executor_resource_policies
    IN ACCESS EXCLUSIVE MODE;

ALTER TABLE executor_capacity_allocations
    ADD COLUMN release_reconciliation_id UUID;

CREATE TABLE provider_capacity_reconciliations (
    reconciliation_id UUID PRIMARY KEY,
    submission_id UUID NOT NULL UNIQUE,
    executor_execution_id UUID NOT NULL UNIQUE,
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_account_id UUID NOT NULL,
    provider_deadline_at_ms BIGINT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'released')),
    available_at_ms BIGINT NOT NULL,
    reconciliation_owner TEXT CHECK (
        reconciliation_owner IS NULL
        OR (char_length(reconciliation_owner) BETWEEN 1 AND 255
            AND reconciliation_owner !~ '[[:cntrl:]]')
    ),
    reconciliation_lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (
        reconciliation_lease_epoch >= 0
    ),
    evidence_revision BIGINT NOT NULL DEFAULT 0 CHECK (
        evidence_revision IN (0, 1)
    ),
    claimed_evidence_revision BIGINT CHECK (
        claimed_evidence_revision IN (0, 1)
    ),
    last_command_kind TEXT CHECK (
        last_command_kind IS NULL OR last_command_kind IN ('claim', 'defer')
    ),
    last_command_id TEXT CHECK (
        last_command_id IS NULL
        OR (char_length(last_command_id) BETWEEN 1 AND 255
            AND last_command_id !~ '[[:cntrl:]]')
    ),
    last_command_owner TEXT CHECK (
        last_command_owner IS NULL
        OR (char_length(last_command_owner) BETWEEN 1 AND 255
            AND last_command_owner !~ '[[:cntrl:]]')
    ),
    last_command_lease_epoch BIGINT CHECK (
        last_command_lease_epoch IS NULL OR last_command_lease_epoch > 0
    ),
    claim_command_claimed_at_ms BIGINT,
    claim_command_lease_expires_at_ms BIGINT,
    evidence_kind TEXT CHECK (
        evidence_kind IS NULL
        OR evidence_kind IN ('confirmed_no_effect', 'remote_terminal')
    ),
    remote_operation_id TEXT CHECK (
        remote_operation_id IS NULL
        OR (char_length(remote_operation_id) BETWEEN 1 AND 255
            AND remote_operation_id !~ '[[:cntrl:]]')
    ),
    remote_terminal_state TEXT CHECK (
        remote_terminal_state IS NULL
        OR remote_terminal_state IN ('succeeded', 'failed', 'canceled')
    ),
    event_identity TEXT CHECK (
        event_identity IS NULL
        OR (char_length(event_identity) BETWEEN 1 AND 255
            AND event_identity !~ '[[:cntrl:]]')
    ),
    payload_hash TEXT CHECK (
        payload_hash IS NULL OR payload_hash ~ '^[0-9a-f]{64}$'
    ),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    released_at_ms BIGINT,
    CHECK (reconciliation_id = executor_execution_id),
    CHECK (updated_at_ms >= created_at_ms),
    CHECK (
        (last_command_kind IS NULL AND last_command_id IS NULL
            AND last_command_owner IS NULL AND last_command_lease_epoch IS NULL
            AND claim_command_claimed_at_ms IS NULL
            AND claim_command_lease_expires_at_ms IS NULL)
        OR
        (last_command_kind = 'claim' AND last_command_id IS NOT NULL
            AND last_command_owner IS NOT NULL AND last_command_lease_epoch IS NOT NULL
            AND claim_command_claimed_at_ms IS NOT NULL
            AND claim_command_lease_expires_at_ms > claim_command_claimed_at_ms)
        OR
        (last_command_kind = 'defer' AND last_command_id IS NOT NULL
            AND last_command_owner IS NOT NULL AND last_command_lease_epoch IS NOT NULL
            AND claim_command_claimed_at_ms IS NULL
            AND claim_command_lease_expires_at_ms IS NULL)
    ),
    CHECK (
        (state = 'active' AND released_at_ms IS NULL
            AND evidence_kind IS NULL AND remote_operation_id IS NULL
            AND remote_terminal_state IS NULL AND event_identity IS NULL
            AND payload_hash IS NULL)
        OR
        (state = 'released' AND released_at_ms = updated_at_ms
            AND evidence_kind IS NOT NULL AND event_identity IS NOT NULL
            AND payload_hash IS NOT NULL)
    ),
    CHECK (
        (evidence_kind IS NULL
            AND remote_operation_id IS NULL AND remote_terminal_state IS NULL)
        OR
        (evidence_kind = 'confirmed_no_effect'
            AND remote_operation_id IS NULL AND remote_terminal_state IS NULL)
        OR
        (evidence_kind = 'remote_terminal'
            AND remote_operation_id IS NOT NULL AND remote_terminal_state IS NOT NULL)
    ),
    CHECK (
        (reconciliation_owner IS NULL AND claimed_evidence_revision IS NULL)
        OR
        (reconciliation_owner IS NOT NULL
            AND reconciliation_lease_epoch > 0
            AND claimed_evidence_revision IS NOT NULL)
    ),
    FOREIGN KEY (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) REFERENCES provider_remote_submit_intents (
        submission_id, executor_execution_id, provider_id, provider_account_id
    ) ON DELETE RESTRICT,
    UNIQUE (reconciliation_id, executor_execution_id, submission_id)
);

CREATE INDEX provider_capacity_reconciliations_claim_idx
    ON provider_capacity_reconciliations (
        provider_account_id, available_at_ms,
        provider_deadline_at_ms, submission_id
    )
    WHERE state = 'active';

CREATE UNIQUE INDEX provider_capacity_reconciliations_remote_operation_idx
    ON provider_capacity_reconciliations (
        provider_account_id, remote_operation_id
    )
    WHERE remote_operation_id IS NOT NULL;

CREATE UNIQUE INDEX provider_capacity_reconciliations_claim_command_idx
    ON provider_capacity_reconciliations (
        provider_id, provider_account_id, last_command_owner, last_command_id
    )
    WHERE state = 'active' AND last_command_kind = 'claim';

INSERT INTO provider_capacity_reconciliations (
    reconciliation_id, submission_id, executor_execution_id,
    provider_id, provider_account_id, provider_deadline_at_ms,
    state, available_at_ms, reconciliation_owner,
    reconciliation_lease_epoch, evidence_revision,
    created_at_ms, updated_at_ms
)
SELECT intent.executor_execution_id, intent.submission_id,
       intent.executor_execution_id, intent.provider_id,
       intent.provider_account_id, recovery.provider_deadline_at_ms,
       'active', decision.decided_at_ms, NULL, 0,
       CASE WHEN intent.receipt_event_identity IS NULL THEN 0 ELSE 1 END,
       decision.decided_at_ms, decision.decided_at_ms
FROM provider_remote_submit_intents intent
JOIN provider_submit_recoveries recovery
  ON recovery.submission_id = intent.submission_id
 AND recovery.executor_execution_id = intent.executor_execution_id
JOIN executor_resolution_decisions decision
  ON decision.provider_submit_intent_id = intent.submission_id
 AND decision.executor_execution_id = intent.executor_execution_id
 AND decision.submission_id = intent.submission_id
JOIN executor_capacity_allocations allocation
  ON allocation.executor_execution_id = intent.executor_execution_id
 AND allocation.submission_id = intent.submission_id
WHERE intent.state = 'deadline_quarantined'
  AND recovery.state = 'closed'
  AND decision.source = 'remote_submit_deadline'
  AND decision.resolved_state = 'uncertain'
  AND allocation.state = 'held';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_remote_submit_intents intent
        LEFT JOIN provider_capacity_reconciliations reconciliation
          ON reconciliation.submission_id = intent.submission_id
         AND reconciliation.executor_execution_id = intent.executor_execution_id
        WHERE intent.state = 'deadline_quarantined'
          AND reconciliation.reconciliation_id IS NULL
    ) THEN
        RAISE EXCEPTION
            'capacity reconciliation migration found an incomplete deadline quarantine';
    END IF;
END;
$$;

ALTER TABLE executor_capacity_allocations
    DROP CONSTRAINT executor_capacity_allocations_release_reason_check,
    ADD CONSTRAINT executor_capacity_allocations_release_reason_check CHECK (
        release_reason IS NULL
        OR release_reason IN (
            'terminal_evidence', 'executor_start_abandoned',
            'remote_provider_observation', 'remote_submit_outcome',
            'provider_capacity_reconciliation'
        )
    ),
    ADD CONSTRAINT executor_capacity_allocations_reconciliation_check CHECK (
        (release_reason = 'provider_capacity_reconciliation')
        = (release_reconciliation_id IS NOT NULL)
    ),
    ADD CONSTRAINT executor_capacity_allocations_reconciliation_fk
        FOREIGN KEY (
            release_reconciliation_id, executor_execution_id, submission_id
        ) REFERENCES provider_capacity_reconciliations (
            reconciliation_id, executor_execution_id, submission_id
        ) ON DELETE RESTRICT;

CREATE OR REPLACE FUNCTION enforce_executor_capacity_allocation_transition()
RETURNS TRIGGER AS $$
DECLARE
    decision_source TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'executor capacity allocations are durable';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'held' OR NEW.released_at_ms IS NOT NULL
           OR NEW.release_reason IS NOT NULL OR NEW.release_decision_id IS NOT NULL
           OR NEW.release_reconciliation_id IS NOT NULL
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
            AND NEW.release_decision_id IS NULL
            AND NEW.release_reconciliation_id IS NULL
            AND NEW.released_state IS NULL)
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
        ELSIF NEW.release_reason = 'remote_provider_observation' THEN
            IF decision_source IS DISTINCT FROM 'remote_provider_observation'
               OR NOT EXISTS (
                    SELECT 1
                    FROM executor_resolution_decisions decision
                    JOIN provider_task_observations observation
                      ON observation.observation_id = decision.provider_task_observation_id
                     AND observation.executor_execution_id = decision.executor_execution_id
                     AND observation.submission_id = decision.submission_id
                    WHERE decision.decision_id = NEW.release_decision_id
               ) THEN
                RAISE EXCEPTION
                    'remote capacity release requires durable provider evidence';
            END IF;
        ELSIF NEW.release_reason = 'remote_submit_outcome' THEN
            IF decision_source IS DISTINCT FROM 'remote_submit_outcome'
               OR NOT EXISTS (
                    SELECT 1
                    FROM executor_resolution_decisions decision
                    JOIN provider_remote_submit_intents intent
                      ON intent.submission_id = decision.provider_submit_intent_id
                     AND intent.executor_execution_id = decision.executor_execution_id
                    WHERE decision.decision_id = NEW.release_decision_id
                      AND intent.state = 'rejected'
                      AND decision.resolved_state = 'failed'
               ) THEN
                RAISE EXCEPTION
                    'submit capacity release requires durable provider outcome';
            END IF;
        ELSIF NEW.release_reason = 'provider_capacity_reconciliation' THEN
            IF decision_source IS DISTINCT FROM 'remote_submit_deadline'
               OR NEW.released_state IS DISTINCT FROM 'uncertain'
               OR NOT EXISTS (
                    SELECT 1
                    FROM provider_capacity_reconciliations reconciliation
                    WHERE reconciliation.reconciliation_id =
                          NEW.release_reconciliation_id
                      AND reconciliation.executor_execution_id =
                          NEW.executor_execution_id
                      AND reconciliation.submission_id = NEW.submission_id
                      AND reconciliation.state = 'released'
                      AND reconciliation.evidence_kind IN (
                            'confirmed_no_effect', 'remote_terminal'
                          )
               ) THEN
                RAISE EXCEPTION
                    'quarantined capacity release requires strong reconciliation evidence';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_provider_submit_deadline_capacity_hold()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.state = 'held' AND NEW.state = 'released'
       AND EXISTS (
            SELECT 1
            FROM executor_executions execution
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = execution.resolution_decision_id
             AND decision.executor_execution_id = execution.executor_execution_id
             AND decision.submission_id = execution.submission_id
            WHERE execution.executor_execution_id = NEW.executor_execution_id
              AND execution.submission_id = NEW.submission_id
              AND decision.source = 'remote_submit_deadline'
       )
       AND NOT (
            NEW.release_reason = 'provider_capacity_reconciliation'
            AND NEW.release_reconciliation_id IS NOT NULL
       ) THEN
        RAISE EXCEPTION
            'deadline-quarantined provider capacity requires reconciliation evidence';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION enforce_provider_capacity_reconciliation_insert()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.state <> 'active'
       OR NEW.reconciliation_owner IS NOT NULL
       OR NEW.reconciliation_lease_epoch <> 0
       OR NEW.claimed_evidence_revision IS NOT NULL
           OR NEW.last_command_kind IS NOT NULL
       OR NEW.claim_command_claimed_at_ms IS NOT NULL
       OR NEW.claim_command_lease_expires_at_ms IS NOT NULL
       OR NEW.evidence_kind IS NOT NULL
       OR NEW.released_at_ms IS NOT NULL
       OR NEW.created_at_ms IS DISTINCT FROM NEW.updated_at_ms
       OR NEW.available_at_ms IS DISTINCT FROM NEW.created_at_ms
       OR NOT EXISTS (
            SELECT 1
            FROM provider_remote_submit_intents intent
            JOIN provider_submit_recoveries recovery
              ON recovery.submission_id = intent.submission_id
             AND recovery.executor_execution_id = intent.executor_execution_id
            JOIN executor_resolution_decisions decision
              ON decision.provider_submit_intent_id = intent.submission_id
             AND decision.executor_execution_id = intent.executor_execution_id
             AND decision.submission_id = intent.submission_id
            JOIN executor_executions execution
              ON execution.executor_execution_id = intent.executor_execution_id
             AND execution.submission_id = intent.submission_id
            JOIN provider_submissions submission
              ON submission.executor_execution_id = intent.executor_execution_id
             AND submission.submission_id = intent.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = intent.executor_execution_id
             AND allocation.submission_id = intent.submission_id
            WHERE intent.submission_id = NEW.submission_id
              AND intent.executor_execution_id = NEW.executor_execution_id
              AND intent.provider_id = NEW.provider_id
              AND intent.provider_account_id = NEW.provider_account_id
              AND intent.state = 'deadline_quarantined'
              AND recovery.state = 'closed'
              AND recovery.provider_deadline_at_ms = NEW.provider_deadline_at_ms
              AND decision.source = 'remote_submit_deadline'
              AND decision.resolved_state = 'uncertain'
              AND execution.state = 'uncertain'
              AND submission.state = 'uncertain'
              AND allocation.state = 'held'
              AND NEW.evidence_revision = CASE
                    WHEN intent.receipt_event_identity IS NULL THEN 0 ELSE 1 END
       ) THEN
        RAISE EXCEPTION
            'capacity reconciliation requires canonical deadline quarantine';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_capacity_reconciliation_insert_guard
    BEFORE INSERT ON provider_capacity_reconciliations
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_capacity_reconciliation_insert();

CREATE FUNCTION enforce_provider_capacity_reconciliation_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.reconciliation_id IS DISTINCT FROM OLD.reconciliation_id
       OR NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.provider_account_id IS DISTINCT FROM OLD.provider_account_id
       OR NEW.provider_deadline_at_ms IS DISTINCT FROM OLD.provider_deadline_at_ms
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms
       OR NEW.updated_at_ms < OLD.updated_at_ms THEN
        RAISE EXCEPTION 'capacity reconciliation identity is immutable';
    END IF;
    IF OLD.state = 'released' THEN
        RAISE EXCEPTION 'released capacity reconciliation is immutable';
    END IF;

    IF NEW.state = 'released' THEN
        IF OLD.reconciliation_owner IS NULL
           OR NEW.reconciliation_owner IS DISTINCT FROM OLD.reconciliation_owner
           OR NEW.reconciliation_lease_epoch IS DISTINCT FROM
              OLD.reconciliation_lease_epoch
           OR NEW.available_at_ms IS DISTINCT FROM OLD.available_at_ms
           OR OLD.available_at_ms <= NEW.updated_at_ms
           OR NEW.evidence_revision IS DISTINCT FROM OLD.evidence_revision
           OR OLD.claimed_evidence_revision IS DISTINCT FROM OLD.evidence_revision
           OR NEW.claimed_evidence_revision IS DISTINCT FROM
              OLD.claimed_evidence_revision
           OR NEW.last_command_kind IS DISTINCT FROM OLD.last_command_kind
           OR NEW.last_command_id IS DISTINCT FROM OLD.last_command_id
           OR NEW.last_command_owner IS DISTINCT FROM OLD.last_command_owner
           OR NEW.last_command_lease_epoch IS DISTINCT FROM
              OLD.last_command_lease_epoch
           OR NEW.claim_command_claimed_at_ms IS DISTINCT FROM
              OLD.claim_command_claimed_at_ms
           OR NEW.claim_command_lease_expires_at_ms IS DISTINCT FROM
              OLD.claim_command_lease_expires_at_ms THEN
            RAISE EXCEPTION 'capacity evidence requires its live fenced lease';
        END IF;
        IF NEW.evidence_kind = 'remote_terminal'
           AND NOT EXISTS (
                SELECT 1
                FROM provider_remote_submit_intents intent
                WHERE intent.submission_id = NEW.submission_id
                  AND intent.executor_execution_id = NEW.executor_execution_id
                  AND intent.provider_id = NEW.provider_id
                  AND intent.provider_account_id = NEW.provider_account_id
                  AND intent.state = 'deadline_quarantined'
                  AND intent.remote_operation_id = NEW.remote_operation_id
                  AND intent.receipt_event_identity IS NOT NULL
           ) THEN
            RAISE EXCEPTION
                'remote terminal evidence requires its durable submit receipt';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.evidence_revision = OLD.evidence_revision + 1 THEN
        IF NEW.reconciliation_owner IS DISTINCT FROM OLD.reconciliation_owner
           OR NEW.reconciliation_lease_epoch IS DISTINCT FROM
              OLD.reconciliation_lease_epoch
           OR NEW.claimed_evidence_revision IS DISTINCT FROM
              OLD.claimed_evidence_revision
           OR NEW.last_command_kind IS DISTINCT FROM OLD.last_command_kind
           OR NEW.last_command_id IS DISTINCT FROM OLD.last_command_id
           OR NEW.last_command_owner IS DISTINCT FROM OLD.last_command_owner
           OR NEW.last_command_lease_epoch IS DISTINCT FROM
              OLD.last_command_lease_epoch
           OR NEW.claim_command_claimed_at_ms IS DISTINCT FROM
              OLD.claim_command_claimed_at_ms
           OR NEW.claim_command_lease_expires_at_ms IS DISTINCT FROM
              OLD.claim_command_lease_expires_at_ms
           OR NEW.evidence_kind IS NOT NULL
           OR (OLD.reconciliation_owner IS NOT NULL
               AND NEW.available_at_ms IS DISTINCT FROM OLD.available_at_ms)
           OR (OLD.reconciliation_owner IS NULL
               AND NEW.available_at_ms > OLD.available_at_ms) THEN
            RAISE EXCEPTION 'invalid capacity reconciliation receipt wake';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.evidence_revision IS DISTINCT FROM OLD.evidence_revision THEN
        RAISE EXCEPTION 'capacity evidence revision must be monotonic';
    END IF;

    IF OLD.reconciliation_owner IS NULL AND NEW.reconciliation_owner IS NOT NULL THEN
        IF NEW.reconciliation_lease_epoch <> OLD.reconciliation_lease_epoch + 1
           OR NEW.claimed_evidence_revision IS DISTINCT FROM OLD.evidence_revision
           OR NEW.available_at_ms <= NEW.updated_at_ms
           OR NEW.last_command_kind <> 'claim'
           OR NEW.last_command_owner IS DISTINCT FROM NEW.reconciliation_owner
           OR NEW.last_command_lease_epoch IS DISTINCT FROM
              NEW.reconciliation_lease_epoch
           OR NEW.claim_command_claimed_at_ms IS DISTINCT FROM NEW.updated_at_ms
           OR NEW.claim_command_lease_expires_at_ms IS DISTINCT FROM
              NEW.available_at_ms THEN
            RAISE EXCEPTION 'invalid capacity reconciliation claim';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.reconciliation_owner IS NOT NULL
       AND NEW.reconciliation_owner IS NOT NULL THEN
        IF NEW.reconciliation_lease_epoch = OLD.reconciliation_lease_epoch + 1 THEN
            IF OLD.available_at_ms > NEW.updated_at_ms
               OR NEW.claimed_evidence_revision IS DISTINCT FROM
                  NEW.evidence_revision
               OR NEW.available_at_ms <= NEW.updated_at_ms
               OR NEW.last_command_kind <> 'claim'
               OR NEW.last_command_owner IS DISTINCT FROM
                  NEW.reconciliation_owner
               OR NEW.last_command_lease_epoch IS DISTINCT FROM
                  NEW.reconciliation_lease_epoch
               OR NEW.claim_command_claimed_at_ms IS DISTINCT FROM NEW.updated_at_ms
               OR NEW.claim_command_lease_expires_at_ms IS DISTINCT FROM
                  NEW.available_at_ms THEN
                RAISE EXCEPTION
                    'capacity reconciliation reclaim requires expiry';
            END IF;
            RETURN NEW;
        END IF;
        IF NEW.reconciliation_owner IS DISTINCT FROM OLD.reconciliation_owner
           OR NEW.reconciliation_lease_epoch IS DISTINCT FROM
              OLD.reconciliation_lease_epoch
           OR NEW.claimed_evidence_revision IS DISTINCT FROM
              OLD.claimed_evidence_revision
           OR NEW.claimed_evidence_revision IS DISTINCT FROM NEW.evidence_revision
           OR NEW.available_at_ms < OLD.available_at_ms
           OR NEW.available_at_ms <= NEW.updated_at_ms
           OR NEW.last_command_kind IS DISTINCT FROM OLD.last_command_kind
           OR NEW.last_command_id IS DISTINCT FROM OLD.last_command_id
           OR NEW.last_command_owner IS DISTINCT FROM OLD.last_command_owner
           OR NEW.last_command_lease_epoch IS DISTINCT FROM
              OLD.last_command_lease_epoch
           OR NEW.claim_command_claimed_at_ms IS DISTINCT FROM
              OLD.claim_command_claimed_at_ms
           OR NEW.claim_command_lease_expires_at_ms IS DISTINCT FROM
              OLD.claim_command_lease_expires_at_ms THEN
            RAISE EXCEPTION 'invalid capacity reconciliation heartbeat';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.reconciliation_owner IS NOT NULL AND NEW.reconciliation_owner IS NULL THEN
        IF NEW.reconciliation_lease_epoch IS DISTINCT FROM
              OLD.reconciliation_lease_epoch
           OR NEW.claimed_evidence_revision IS NOT NULL
           OR NEW.available_at_ms < NEW.updated_at_ms
           OR NEW.last_command_kind <> 'defer'
           OR NEW.last_command_owner IS DISTINCT FROM OLD.reconciliation_owner
           OR NEW.last_command_lease_epoch IS DISTINCT FROM
              OLD.reconciliation_lease_epoch
           OR NEW.claim_command_claimed_at_ms IS NOT NULL
           OR NEW.claim_command_lease_expires_at_ms IS NOT NULL THEN
            RAISE EXCEPTION 'invalid capacity reconciliation defer';
        END IF;
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'unsupported capacity reconciliation mutation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_capacity_reconciliation_update_guard
    BEFORE UPDATE ON provider_capacity_reconciliations
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_capacity_reconciliation_update();

CREATE FUNCTION reject_provider_capacity_reconciliation_delete()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'provider capacity reconciliations are durable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_capacity_reconciliations_reject_delete
    BEFORE DELETE ON provider_capacity_reconciliations
    FOR EACH ROW EXECUTE FUNCTION reject_provider_capacity_reconciliation_delete();

CREATE TRIGGER provider_capacity_reconciliations_reject_truncate
    BEFORE TRUNCATE ON provider_capacity_reconciliations
    FOR EACH STATEMENT EXECUTE FUNCTION reject_provider_capacity_reconciliation_delete();

CREATE FUNCTION enforce_provider_capacity_reconciliation_projection()
RETURNS TRIGGER AS $$
DECLARE
    target_submission UUID := COALESCE(NEW.submission_id, OLD.submission_id);
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_capacity_reconciliations reconciliation
        JOIN provider_remote_submit_intents intent
          ON intent.submission_id = reconciliation.submission_id
         AND intent.executor_execution_id = reconciliation.executor_execution_id
        JOIN provider_submit_recoveries recovery
          ON recovery.submission_id = reconciliation.submission_id
         AND recovery.executor_execution_id = reconciliation.executor_execution_id
        JOIN executor_resolution_decisions decision
          ON decision.provider_submit_intent_id = reconciliation.submission_id
         AND decision.executor_execution_id = reconciliation.executor_execution_id
         AND decision.submission_id = reconciliation.submission_id
        JOIN executor_executions execution
          ON execution.executor_execution_id = reconciliation.executor_execution_id
         AND execution.submission_id = reconciliation.submission_id
        JOIN provider_submissions submission
          ON submission.executor_execution_id = reconciliation.executor_execution_id
         AND submission.submission_id = reconciliation.submission_id
        JOIN executor_capacity_allocations allocation
          ON allocation.executor_execution_id = reconciliation.executor_execution_id
         AND allocation.submission_id = reconciliation.submission_id
        WHERE reconciliation.submission_id = target_submission
          AND NOT (
            intent.state = 'deadline_quarantined'
            AND recovery.state = 'closed'
            AND decision.source = 'remote_submit_deadline'
            AND decision.resolved_state = 'uncertain'
            AND execution.state = 'uncertain'
            AND submission.state = 'uncertain'
            AND (
              (reconciliation.state = 'active'
                AND reconciliation.evidence_revision = CASE
                    WHEN intent.receipt_event_identity IS NULL THEN 0 ELSE 1 END
                AND allocation.state = 'held')
              OR
              (reconciliation.state = 'released'
                AND allocation.state = 'released'
                AND allocation.release_reason =
                    'provider_capacity_reconciliation'
                AND allocation.release_reconciliation_id =
                    reconciliation.reconciliation_id
                AND allocation.release_decision_id = decision.decision_id
                AND allocation.released_state = 'uncertain')
            )
          )
    ) THEN
        RAISE EXCEPTION 'capacity reconciliation projection is inconsistent';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_capacity_reconciliation_projection_check
    AFTER INSERT OR UPDATE ON provider_capacity_reconciliations
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_capacity_reconciliation_projection();

CREATE CONSTRAINT TRIGGER executor_capacity_reconciliation_projection_check
    AFTER UPDATE ON executor_capacity_allocations
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_capacity_reconciliation_projection();

CREATE CONSTRAINT TRIGGER provider_submit_intent_capacity_projection_check
    AFTER UPDATE ON provider_remote_submit_intents
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_capacity_reconciliation_projection();

CREATE FUNCTION enforce_late_receipt_capacity_evidence()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.state = 'deadline_quarantined'
       AND NEW.state = 'deadline_quarantined'
       AND OLD.remote_operation_id IS NULL
       AND NEW.remote_operation_id IS NOT NULL
       AND EXISTS (
            SELECT 1
            FROM provider_capacity_reconciliations reconciliation
            WHERE reconciliation.submission_id = NEW.submission_id
              AND reconciliation.state = 'released'
              AND reconciliation.evidence_kind = 'remote_terminal'
              AND reconciliation.remote_operation_id IS DISTINCT FROM
                  NEW.remote_operation_id
       ) THEN
        RAISE EXCEPTION 'late receipt conflicts with terminal capacity evidence';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_submit_intent_capacity_evidence_guard
    BEFORE UPDATE ON provider_remote_submit_intents
    FOR EACH ROW EXECUTE FUNCTION enforce_late_receipt_capacity_evidence();

CREATE OR REPLACE FUNCTION enforce_provider_submit_intent_projection()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.state = 'attached' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM provider_remote_tasks task
            JOIN provider_task_observations observation
              ON observation.observation_id = task.state_observation_id
             AND observation.executor_execution_id = task.executor_execution_id
             AND observation.submission_id = task.submission_id
            JOIN executor_executions execution
              ON execution.executor_execution_id = task.executor_execution_id
             AND execution.submission_id = task.submission_id
            JOIN provider_submissions submission
              ON submission.executor_execution_id = task.executor_execution_id
             AND submission.submission_id = task.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = task.executor_execution_id
             AND allocation.submission_id = task.submission_id
            WHERE task.executor_execution_id = NEW.executor_execution_id
              AND task.submission_id = NEW.submission_id
              AND task.provider_id = NEW.provider_id
              AND task.provider_account_id = NEW.provider_account_id
              AND task.submit_owner = NEW.submit_owner
              AND task.submit_lease_epoch = NEW.submit_lease_epoch
              AND task.remote_operation_id = NEW.remote_operation_id
              AND task.provider_request_id IS NOT DISTINCT FROM NEW.provider_request_id
              AND task.state = 'provider_waiting'
              AND observation.source = 'submit_attach'
              AND observation.observed_state = 'provider_waiting'
              AND execution.state = 'provider_waiting'
              AND execution.executor_owner IS NULL
              AND execution.lease_epoch = NEW.submit_lease_epoch
              AND execution.lease_expires_at_ms IS NULL
              AND execution.launch_owner = NEW.submit_owner
              AND execution.launch_lease_epoch = NEW.submit_lease_epoch
              AND submission.state = 'provider_waiting'
              AND allocation.state = 'held'
        ) THEN
            RAISE EXCEPTION
                'attached provider submit intent requires its complete remote handoff';
        END IF;
    ELSIF NEW.state = 'rejected' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM executor_resolution_decisions decision
            JOIN executor_executions execution
              ON execution.executor_execution_id = decision.executor_execution_id
             AND execution.submission_id = decision.submission_id
            JOIN provider_submissions submission
              ON submission.executor_execution_id = decision.executor_execution_id
             AND submission.submission_id = decision.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = decision.executor_execution_id
             AND allocation.submission_id = decision.submission_id
            WHERE decision.executor_execution_id = NEW.executor_execution_id
              AND decision.submission_id = NEW.submission_id
              AND decision.provider_submit_intent_id = NEW.submission_id
              AND decision.source = 'remote_submit_outcome'
              AND decision.resolved_state = 'failed'
              AND decision.error_code = NEW.failure_error_code
              AND execution.state = 'failed'
              AND execution.executor_owner IS NULL
              AND execution.lease_expires_at_ms IS NULL
              AND execution.resolution_decision_id = decision.decision_id
              AND submission.state = 'failed'
              AND submission.resolution_decision_id = decision.decision_id
              AND allocation.state = 'released'
              AND allocation.release_reason = 'remote_submit_outcome'
              AND allocation.release_decision_id = decision.decision_id
              AND allocation.released_state = 'failed'
        ) THEN
            RAISE EXCEPTION
                'rejected provider submit intent requires its complete terminal projection';
        END IF;
    ELSIF NEW.state = 'deadline_quarantined' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM provider_submit_recoveries recovery
            JOIN executor_resolution_decisions decision
              ON decision.provider_submit_intent_id = recovery.submission_id
             AND decision.executor_execution_id = recovery.executor_execution_id
             AND decision.submission_id = recovery.submission_id
            JOIN executor_executions execution
              ON execution.executor_execution_id = decision.executor_execution_id
             AND execution.submission_id = decision.submission_id
            JOIN provider_submissions submission
              ON submission.executor_execution_id = decision.executor_execution_id
             AND submission.submission_id = decision.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = decision.executor_execution_id
             AND allocation.submission_id = decision.submission_id
            JOIN provider_capacity_reconciliations reconciliation
              ON reconciliation.executor_execution_id = decision.executor_execution_id
             AND reconciliation.submission_id = decision.submission_id
            WHERE recovery.submission_id = NEW.submission_id
              AND recovery.state = 'closed'
              AND decision.source = 'remote_submit_deadline'
              AND decision.resolved_state = 'uncertain'
              AND decision.error_code = 'provider_submit_deadline'
              AND decision.decided_at_ms >= recovery.provider_deadline_at_ms
              AND execution.state = 'uncertain'
              AND execution.executor_owner IS NULL
              AND execution.lease_expires_at_ms IS NULL
              AND execution.resolution_decision_id = decision.decision_id
              AND submission.state = 'uncertain'
              AND submission.resolution_decision_id = decision.decision_id
              AND (
                (allocation.state = 'held'
                  AND reconciliation.state = 'active')
                OR
                (allocation.state = 'released'
                  AND allocation.release_reason =
                      'provider_capacity_reconciliation'
                  AND allocation.release_reconciliation_id =
                      reconciliation.reconciliation_id
                  AND reconciliation.state = 'released')
              )
        ) THEN
            RAISE EXCEPTION
                'deadline-quarantined submit requires its capacity reconciliation';
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
