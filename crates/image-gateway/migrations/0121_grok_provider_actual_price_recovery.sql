-- Migration 0119 first published Grok's provider-reported actual-cost contract.
-- Executions completed before that publication could carry valid immutable cost
-- evidence but fail terminal reduction because no version covered its timestamp.

CREATE TABLE provider_actual_price_backfills (
    media_kind TEXT PRIMARY KEY CHECK (media_kind IN ('image', 'video')),
    price_book_id UUID NOT NULL REFERENCES price_books(price_book_id) ON DELETE RESTRICT,
    source_price_book_version_id UUID NOT NULL UNIQUE
        REFERENCES price_book_versions(price_book_version_id) ON DELETE RESTRICT,
    historical_price_book_version_id UUID NOT NULL UNIQUE
        REFERENCES price_book_versions(price_book_version_id) ON DELETE RESTRICT,
    source_effective_from_ms BIGINT NOT NULL,
    historical_effective_from_ms BIGINT NOT NULL,
    historical_effective_until_ms BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    CHECK (historical_effective_from_ms < historical_effective_until_ms),
    CHECK (historical_effective_until_ms = source_effective_from_ms)
);

CREATE TABLE executor_terminal_reduction_recoveries (
    submission_id UUID PRIMARY KEY
        REFERENCES executor_terminal_reductions(submission_id) ON DELETE RESTRICT,
    executor_execution_id UUID NOT NULL UNIQUE,
    prior_lease_epoch BIGINT NOT NULL CHECK (prior_lease_epoch > 0),
    prior_claimed_at_ms BIGINT NOT NULL,
    prior_blocked_error_code TEXT NOT NULL CHECK (
        prior_blocked_error_code = 'canonical_conflict'
    ),
    prior_blocked_by TEXT NOT NULL,
    prior_blocked_at_ms BIGINT NOT NULL,
    recovery_reason TEXT NOT NULL CHECK (
        recovery_reason = 'provider_actual_price_backfill'
    ),
    recovered_at_ms BIGINT NOT NULL,
    UNIQUE (submission_id, executor_execution_id)
);

CREATE FUNCTION reject_terminal_reduction_recovery_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'terminal reduction recovery evidence is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_actual_price_backfills_reject_mutation
    BEFORE UPDATE OR DELETE ON provider_actual_price_backfills
    FOR EACH ROW EXECUTE FUNCTION reject_terminal_reduction_recovery_mutation();

CREATE TRIGGER provider_actual_price_backfills_reject_truncate
    BEFORE TRUNCATE ON provider_actual_price_backfills
    FOR EACH STATEMENT EXECUTE FUNCTION reject_terminal_reduction_recovery_mutation();

CREATE TRIGGER executor_terminal_reduction_recoveries_reject_mutation
    BEFORE UPDATE OR DELETE ON executor_terminal_reduction_recoveries
    FOR EACH ROW EXECUTE FUNCTION reject_terminal_reduction_recovery_mutation();

CREATE TRIGGER executor_terminal_reduction_recoveries_reject_truncate
    BEFORE TRUNCATE ON executor_terminal_reduction_recoveries
    FOR EACH STATEMENT EXECUTE FUNCTION reject_terminal_reduction_recovery_mutation();

DO $$
DECLARE
    target_book_id UUID;
    current_version_id UUID;
    replacement_version_id UUID;
    current_effective_from_ms BIGINT;
    oldest_evidence_at_ms BIGINT;
    next_version INTEGER;
    now_ms BIGINT := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
    media_kind_value TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('grok-provider-actual-history-v1', 0)
    );

    SELECT price_book_id INTO target_book_id
    FROM price_books
    WHERE price_book_key = 'provider_actual.grok-cli.reported'
      AND purpose = 'provider_actual'
      AND scope_type = 'platform'
      AND provider_id = 'grok-cli'
      AND currency = 'USD'
      AND state = 'active'
    FOR UPDATE;

    IF target_book_id IS NULL THEN
        RETURN;
    END IF;

    FOREACH media_kind_value IN ARRAY ARRAY['image', 'video']
    LOOP
        SELECT MIN(evidence.created_at_ms) INTO oldest_evidence_at_ms
        FROM executor_provider_cost_evidence evidence
        JOIN provider_submissions submission
          ON submission.submission_id = evidence.submission_id
         AND submission.executor_execution_id = evidence.executor_execution_id
        JOIN customer_price_quotes quote
          ON quote.job_id = submission.job_id
         AND quote.tenant_id = submission.tenant_id
        WHERE evidence.provider_id = 'grok-cli'
          AND evidence.execution_surface = 'provider_cli'
          AND quote.provider_id = 'grok-cli'
          AND quote.media_kind = media_kind_value;

        IF oldest_evidence_at_ms IS NULL THEN
            CONTINUE;
        END IF;

        SELECT price_book_version_id, effective_from_ms
        INTO current_version_id, current_effective_from_ms
        FROM price_book_versions
        WHERE price_book_id = target_book_id
          AND state = 'active'
          AND api_profile = '*'
          AND operation = '*'
          AND provider_id = 'grok-cli'
          AND provider_model_id IS NULL
          AND public_model_id = '*'
          AND media_kind = media_kind_value
          AND service_tier = 'standard'
          AND execution_surface = 'provider_cli'
          AND billing_mode = 'provider_reported'
          AND effective_from_ms <= now_ms
          AND (effective_until_ms IS NULL OR effective_until_ms > now_ms)
        FOR UPDATE;

        IF current_version_id IS NULL
           OR oldest_evidence_at_ms >= current_effective_from_ms THEN
            CONTINUE;
        END IF;

        SELECT COALESCE(MAX(version), 0) + 1 INTO next_version
        FROM price_book_versions
        WHERE price_book_id = target_book_id;

        replacement_version_id := CASE media_kind_value
            WHEN 'image' THEN '3b19fa97-4835-4a2f-9558-0121a1f00001'::UUID
            ELSE '3b19fa97-4835-4a2f-9558-0121a1f00002'::UUID
        END;

        INSERT INTO price_book_versions (
            price_book_version_id, price_book_id, version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier,
            execution_surface, billing_mode, is_free, state,
            effective_from_ms, effective_until_ms,
            source_kind, source_url, source_checked_at_ms, notes,
            control_version, created_at_ms, updated_at_ms
        )
        VALUES (
            replacement_version_id, target_book_id, next_version,
            '*', '*', 'grok-cli', NULL, '*', media_kind_value, 'standard',
            'provider_cli', 'provider_reported', FALSE, 'retired',
            oldest_evidence_at_ms, current_effective_from_ms,
            'imported', NULL, NULL,
            'Covers Grok provider-reported cost evidence that predates the default price',
            1, now_ms, now_ms
        );

        INSERT INTO provider_actual_price_backfills (
            media_kind, price_book_id, source_price_book_version_id,
            historical_price_book_version_id, source_effective_from_ms,
            historical_effective_from_ms, historical_effective_until_ms,
            created_at_ms
        ) VALUES (
            media_kind_value, target_book_id, current_version_id,
            replacement_version_id, current_effective_from_ms,
            oldest_evidence_at_ms, current_effective_from_ms, now_ms
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_executor_terminal_reduction_lease() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.executor_execution_id IS DISTINCT FROM OLD.executor_execution_id
       OR NEW.resolution_decision_id IS DISTINCT FROM OLD.resolution_decision_id
       OR NEW.resolved_state IS DISTINCT FROM OLD.resolved_state
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms THEN
        RAISE EXCEPTION 'terminal reduction identity is immutable';
    END IF;
    IF OLD.state = 'completed' THEN
        RAISE EXCEPTION 'terminal reduction terminal state is immutable';
    END IF;
    IF OLD.state = 'blocked' AND NEW.state = 'ready' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM executor_terminal_reduction_recoveries recovery
            WHERE recovery.submission_id = OLD.submission_id
              AND recovery.executor_execution_id = OLD.executor_execution_id
              AND recovery.prior_lease_epoch = OLD.lease_epoch
              AND recovery.prior_claimed_at_ms = OLD.claimed_at_ms
              AND recovery.prior_blocked_error_code = OLD.blocked_error_code
              AND recovery.prior_blocked_by = OLD.blocked_by
              AND recovery.prior_blocked_at_ms = OLD.blocked_at_ms
              AND recovery.recovered_at_ms = NEW.updated_at_ms
        ) THEN
            RAISE EXCEPTION 'blocked terminal reduction recovery requires immutable evidence';
        END IF;
    ELSIF OLD.state = 'blocked' THEN
        RAISE EXCEPTION 'terminal reduction terminal state is immutable';
    ELSIF OLD.state = 'ready' AND NEW.state = 'leased' THEN
        IF NEW.lease_epoch <> 1 OR NEW.lease_expires_at_ms <= now_ms THEN
            RAISE EXCEPTION 'terminal reduction claim requires its first future lease';
        END IF;
    ELSIF OLD.state = 'leased' AND NEW.state = 'leased' THEN
        IF NEW.lease_owner IS NOT DISTINCT FROM OLD.lease_owner
           AND NEW.lease_epoch = OLD.lease_epoch THEN
            IF OLD.lease_expires_at_ms <= now_ms
               OR NEW.lease_expires_at_ms < OLD.lease_expires_at_ms
               OR NEW.claimed_at_ms IS DISTINCT FROM OLD.claimed_at_ms THEN
                RAISE EXCEPTION 'terminal reduction heartbeat requires the live lease';
            END IF;
        ELSIF OLD.lease_expires_at_ms > now_ms
              OR NEW.lease_epoch <> OLD.lease_epoch + 1
              OR NEW.lease_expires_at_ms <= now_ms THEN
            RAISE EXCEPTION 'terminal reduction reclaim requires an expired lease';
        END IF;
    ELSIF OLD.state = 'leased' AND NEW.state IN ('completed', 'blocked') THEN
        IF OLD.lease_expires_at_ms <= now_ms
           OR NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR NEW.claimed_at_ms IS DISTINCT FROM OLD.claimed_at_ms THEN
            RAISE EXCEPTION 'terminal reduction finalization requires the live lease';
        END IF;
    ELSE
        RAISE EXCEPTION 'invalid terminal reduction state transition';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

INSERT INTO executor_terminal_reduction_recoveries (
    submission_id, executor_execution_id, prior_lease_epoch,
    prior_claimed_at_ms, prior_blocked_error_code, prior_blocked_by,
    prior_blocked_at_ms, recovery_reason, recovered_at_ms
)
SELECT reduction.submission_id, reduction.executor_execution_id,
       reduction.lease_epoch, reduction.claimed_at_ms,
       reduction.blocked_error_code, reduction.blocked_by,
       reduction.blocked_at_ms, 'provider_actual_price_backfill',
       (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
FROM executor_terminal_reductions reduction
JOIN provider_submissions submission
  ON submission.submission_id = reduction.submission_id
 AND submission.executor_execution_id = reduction.executor_execution_id
JOIN job_outputs output
  ON output.output_id = submission.output_id
 AND output.job_id = submission.job_id
JOIN work_items work
  ON work.work_item_id = submission.work_item_id
 AND work.job_id = submission.job_id
JOIN job_attempts attempt
  ON attempt.execution_id = submission.created_by_execution_id
 AND attempt.work_item_id = submission.work_item_id
 AND attempt.lease_epoch = submission.created_by_lease_epoch
JOIN executor_executions execution
  ON execution.executor_execution_id = submission.executor_execution_id
 AND execution.submission_id = submission.submission_id
JOIN executor_result_manifests manifest
  ON manifest.manifest_id = submission.result_manifest_id
 AND manifest.executor_execution_id = submission.executor_execution_id
 AND manifest.submission_id = submission.submission_id
JOIN executor_artifact_authorities authority
  ON authority.authority_id = manifest.artifact_authority_id
 AND authority.executor_execution_id = manifest.executor_execution_id
 AND authority.submission_id = manifest.submission_id
JOIN executor_provider_cost_evidence evidence
  ON evidence.manifest_id = manifest.manifest_id
 AND evidence.executor_execution_id = manifest.executor_execution_id
 AND evidence.submission_id = manifest.submission_id
JOIN customer_price_quotes quote
  ON quote.job_id = submission.job_id
 AND quote.tenant_id = submission.tenant_id
JOIN provider_actual_price_backfills backfill
  ON backfill.media_kind = quote.media_kind
JOIN price_book_versions version
  ON version.price_book_version_id = backfill.historical_price_book_version_id
 AND version.price_book_id = backfill.price_book_id
 AND version.state = 'retired'
WHERE reduction.state = 'blocked'
  AND reduction.resolved_state = 'succeeded'
  AND reduction.blocked_error_code = 'canonical_conflict'
  AND submission.provider_id = 'grok-cli'
  AND submission.state = 'succeeded'
  AND execution.state = 'succeeded'
  AND output.state = 'pending'
  AND work.state = 'awaiting_executor'
  AND attempt.state = 'handed_off'
  AND evidence.provider_id = 'grok-cli'
  AND evidence.execution_surface = 'provider_cli'
  AND evidence.created_at_ms < backfill.source_effective_from_ms
  AND version.effective_from_ms <= evidence.created_at_ms
  AND (version.effective_until_ms IS NULL
       OR version.effective_until_ms > evidence.created_at_ms)
  AND NOT EXISTS (
      SELECT 1 FROM provider_receipts receipt
      WHERE receipt.submission_id = submission.submission_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM artifacts artifact
      WHERE artifact.job_id = submission.job_id
        AND artifact.output_index = output.output_index
  )
ON CONFLICT (submission_id) DO NOTHING;

UPDATE executor_terminal_reductions reduction
SET state = 'ready',
    lease_owner = NULL,
    lease_epoch = 0,
    lease_expires_at_ms = NULL,
    claimed_at_ms = NULL,
    blocked_error_code = NULL,
    blocked_by = NULL,
    blocked_at_ms = NULL,
    updated_at_ms = recovery.recovered_at_ms
FROM executor_terminal_reduction_recoveries recovery
WHERE reduction.submission_id = recovery.submission_id
  AND reduction.executor_execution_id = recovery.executor_execution_id
  AND reduction.state = 'blocked'
  AND reduction.lease_epoch = recovery.prior_lease_epoch
  AND reduction.claimed_at_ms = recovery.prior_claimed_at_ms
  AND reduction.blocked_error_code = recovery.prior_blocked_error_code
  AND reduction.blocked_by = recovery.prior_blocked_by
  AND reduction.blocked_at_ms = recovery.prior_blocked_at_ms;
