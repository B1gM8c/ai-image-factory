-- Submission authority keeps its original frozen deadline. Only an accepted
-- single-video operation receives a separate, immutable completion budget.
CREATE TABLE accepted_completion_budgets (
    submission_id UUID PRIMARY KEY REFERENCES provider_remote_tasks(submission_id) ON DELETE RESTRICT,
    executor_execution_id UUID NOT NULL UNIQUE,
    remote_operation_id TEXT NOT NULL,
    original_submit_deadline_at_ms BIGINT NOT NULL,
    accepted_at_ms BIGINT NOT NULL,
    completion_deadline_at_ms BIGINT NOT NULL,
    policy_version INTEGER NOT NULL CHECK (policy_version = 1),
    grant_source TEXT NOT NULL CHECK (grant_source IN ('accepted_attach', 'migration_0143')),
    granted_at_ms BIGINT NOT NULL,
    CHECK (accepted_at_ms > 0 AND accepted_at_ms <= 9223372036768375807),
    CHECK (completion_deadline_at_ms = accepted_at_ms + 86400000),
    CHECK (original_submit_deadline_at_ms > accepted_at_ms
           AND completion_deadline_at_ms > original_submit_deadline_at_ms),
    CHECK (granted_at_ms >= accepted_at_ms AND granted_at_ms < completion_deadline_at_ms),
    FOREIGN KEY (executor_execution_id, submission_id)
        REFERENCES provider_submissions(executor_execution_id, submission_id) ON DELETE RESTRICT
);

CREATE FUNCTION validate_accepted_completion_budget() RETURNS TRIGGER AS $$
DECLARE
    task provider_remote_tasks%ROWTYPE;
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    SELECT * INTO STRICT task FROM provider_remote_tasks
    WHERE submission_id = NEW.submission_id FOR UPDATE;
    IF task.provider_id <> 'dreamina-cli' OR task.state <> 'provider_waiting'
       OR task.executor_execution_id <> NEW.executor_execution_id
       OR task.remote_operation_id <> NEW.remote_operation_id
       OR task.provider_deadline_at_ms <> NEW.original_submit_deadline_at_ms
       OR task.created_at_ms <> NEW.accepted_at_ms
       OR task.poll_owner IS NOT NULL OR task.poll_lease_expires_at_ms IS NOT NULL
       OR task.deadline_quarantine_id IS NOT NULL
       OR NEW.granted_at_ms NOT BETWEEN now_ms - 1000 AND now_ms
       OR NEW.completion_deadline_at_ms <= now_ms
       OR NOT EXISTS (
            SELECT 1 FROM provider_submissions submission
            JOIN jobs job USING (job_id)
            JOIN executor_executions execution
              ON execution.executor_execution_id = submission.executor_execution_id
             AND execution.submission_id = submission.submission_id
            JOIN provider_remote_submit_intents intent
              ON intent.submission_id = submission.submission_id
             AND intent.executor_execution_id = submission.executor_execution_id
            JOIN provider_submit_recoveries recovery
              ON recovery.submission_id = submission.submission_id
             AND recovery.executor_execution_id = submission.executor_execution_id
            JOIN executor_capacity_allocations allocation
              ON allocation.submission_id = submission.submission_id
             AND allocation.executor_execution_id = submission.executor_execution_id
            JOIN customer_billing_holds hold ON hold.job_id = job.job_id
            WHERE submission.submission_id = NEW.submission_id
              AND submission.provider_id = 'dreamina-cli'
              AND submission.operation_id = 'videos.generations'
              AND job.operation = 'video_generation' AND job.output_count = 1
              AND job.economics_contract_version = 4 AND job.charged_units = 0
              AND submission.state = 'provider_waiting'
              AND execution.state = 'provider_waiting'
              AND submission.resolution_decision_id IS NULL
              AND execution.resolution_decision_id IS NULL
              AND intent.state = 'attached' AND intent.remote_operation_id = NEW.remote_operation_id
              AND intent.receipt_event_identity IS NOT NULL
              AND recovery.state = 'closed' AND recovery.invocation_attempt = 1
              AND recovery.provider_deadline_at_ms = NEW.original_submit_deadline_at_ms
              AND allocation.state = 'held'
              AND hold.state = 'held' AND hold.captured_micros = 0 AND hold.released_micros = 0
              AND NOT EXISTS (SELECT 1 FROM provider_receipts WHERE job_id = job.job_id)
              AND NOT EXISTS (SELECT 1 FROM usage_events WHERE job_id = job.job_id)
              AND NOT EXISTS (SELECT 1 FROM provider_usage_facts WHERE job_id = job.job_id)
              AND NOT EXISTS (SELECT 1 FROM customer_rated_usage WHERE job_id = job.job_id)
              AND NOT EXISTS (SELECT 1 FROM ledger_transactions WHERE source_job_id = job.job_id)
              AND NOT EXISTS (SELECT 1 FROM artifacts WHERE job_id = job.job_id)
              AND NOT EXISTS (SELECT 1 FROM executor_result_manifests WHERE submission_id = NEW.submission_id)
              AND NOT EXISTS (SELECT 1 FROM provider_remote_video_results WHERE submission_id = NEW.submission_id)
              AND NOT EXISTS (SELECT 1 FROM provider_submit_quarantined_receipts WHERE submission_id = NEW.submission_id)
       ) THEN
        RAISE EXCEPTION 'completion budget requires one untouched accepted Dreamina video';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER accepted_completion_budget_insert_guard BEFORE INSERT ON accepted_completion_budgets
    FOR EACH ROW EXECUTE FUNCTION validate_accepted_completion_budget();
CREATE TRIGGER accepted_completion_budget_immutable BEFORE UPDATE OR DELETE ON accepted_completion_budgets
    FOR EACH ROW EXECUTE FUNCTION reject_executor_evidence_mutation();
CREATE TRIGGER accepted_completion_budget_no_truncate BEFORE TRUNCATE ON accepted_completion_budgets
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_evidence_mutation();

CREATE FUNCTION provider_task_completion_deadline(target_submission UUID, original_deadline BIGINT)
RETURNS BIGINT LANGUAGE SQL STABLE AS $$
    SELECT COALESCE((SELECT completion_deadline_at_ms FROM accepted_completion_budgets
                    WHERE submission_id = target_submission
                      AND original_submit_deadline_at_ms = original_deadline), original_deadline);
$$;

CREATE FUNCTION grant_dreamina_accepted_completion_budget() RETURNS TRIGGER AS $$
DECLARE task provider_remote_tasks%ROWTYPE;
BEGIN
    IF NEW.state <> 'provider_waiting' OR OLD.state = 'provider_waiting'
       OR NEW.provider_id <> 'dreamina-cli' OR NEW.operation_id <> 'videos.generations' THEN
        RETURN NULL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM jobs WHERE job_id = NEW.job_id
                   AND operation = 'video_generation' AND output_count = 1
                   AND economics_contract_version = 4 AND charged_units = 0) THEN
        RETURN NULL;
    END IF;
    SELECT * INTO task FROM provider_remote_tasks WHERE submission_id = NEW.submission_id;
    IF FOUND AND task.created_at_ms <= 9223372036768375807
       AND task.created_at_ms + 86400000 > task.provider_deadline_at_ms THEN
        INSERT INTO accepted_completion_budgets
          (submission_id, executor_execution_id, remote_operation_id,
           original_submit_deadline_at_ms, accepted_at_ms, completion_deadline_at_ms,
           policy_version, grant_source, granted_at_ms)
        VALUES (task.submission_id, task.executor_execution_id, task.remote_operation_id,
                task.provider_deadline_at_ms, task.created_at_ms, task.created_at_ms + 86400000,
                1, 'accepted_attach', floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_accepted_completion_budget AFTER UPDATE OF state ON provider_submissions
    FOR EACH ROW EXECUTE FUNCTION grant_dreamina_accepted_completion_budget();

-- The migration may rescue only an accepted, unresolved task still within its
-- original acceptance-based 24h window. It never reopens terminal evidence.
INSERT INTO accepted_completion_budgets
  (submission_id, executor_execution_id, remote_operation_id,
   original_submit_deadline_at_ms, accepted_at_ms, completion_deadline_at_ms,
   policy_version, grant_source, granted_at_ms)
SELECT task.submission_id, task.executor_execution_id, task.remote_operation_id,
       task.provider_deadline_at_ms, task.created_at_ms, task.created_at_ms + 86400000,
       1, 'migration_0143', floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
FROM provider_remote_tasks task
JOIN provider_submissions submission USING (submission_id, executor_execution_id)
JOIN jobs job ON job.job_id = submission.job_id
JOIN executor_executions execution
  ON execution.executor_execution_id = task.executor_execution_id
 AND execution.submission_id = task.submission_id
JOIN provider_remote_submit_intents intent
  ON intent.submission_id = task.submission_id
 AND intent.executor_execution_id = task.executor_execution_id
JOIN provider_submit_recoveries recovery
  ON recovery.submission_id = task.submission_id
 AND recovery.executor_execution_id = task.executor_execution_id
JOIN executor_capacity_allocations allocation
  ON allocation.submission_id = task.submission_id
 AND allocation.executor_execution_id = task.executor_execution_id
JOIN customer_billing_holds hold ON hold.job_id = job.job_id
WHERE task.provider_id = 'dreamina-cli' AND submission.operation_id = 'videos.generations'
  AND task.state = 'provider_waiting' AND submission.state = 'provider_waiting'
  AND job.operation = 'video_generation' AND job.output_count = 1
  AND job.economics_contract_version = 4 AND job.charged_units = 0
  AND submission.provider_id = 'dreamina-cli'
  AND execution.state = 'provider_waiting'
  AND submission.resolution_decision_id IS NULL AND execution.resolution_decision_id IS NULL
  AND intent.state = 'attached' AND intent.remote_operation_id = task.remote_operation_id
  AND intent.receipt_event_identity IS NOT NULL
  AND recovery.state = 'closed' AND recovery.invocation_attempt = 1
  AND recovery.provider_deadline_at_ms = task.provider_deadline_at_ms
  AND allocation.state = 'held'
  AND hold.state = 'held' AND hold.captured_micros = 0 AND hold.released_micros = 0
  AND NOT EXISTS (SELECT 1 FROM provider_receipts WHERE job_id = job.job_id)
  AND NOT EXISTS (SELECT 1 FROM usage_events WHERE job_id = job.job_id)
  AND NOT EXISTS (SELECT 1 FROM provider_usage_facts WHERE job_id = job.job_id)
  AND NOT EXISTS (SELECT 1 FROM customer_rated_usage WHERE job_id = job.job_id)
  AND NOT EXISTS (SELECT 1 FROM ledger_transactions WHERE source_job_id = job.job_id)
  AND NOT EXISTS (SELECT 1 FROM artifacts WHERE job_id = job.job_id)
  AND NOT EXISTS (SELECT 1 FROM executor_result_manifests WHERE submission_id = task.submission_id)
  AND NOT EXISTS (SELECT 1 FROM provider_remote_video_results WHERE submission_id = task.submission_id)
  AND NOT EXISTS (SELECT 1 FROM provider_submit_quarantined_receipts WHERE submission_id = task.submission_id)
  AND task.poll_owner IS NULL AND task.poll_lease_expires_at_ms IS NULL
  AND task.deadline_quarantine_id IS NULL
  AND task.created_at_ms <= 9223372036768375807
  AND task.created_at_ms + 86400000 > task.provider_deadline_at_ms
  AND task.created_at_ms + 86400000 > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;

ALTER TABLE provider_remote_tasks
    DROP CONSTRAINT provider_remote_tasks_poll_fence_check,
    ADD CONSTRAINT provider_remote_tasks_poll_fence_check CHECK (
        (poll_owner IS NULL AND poll_lease_expires_at_ms IS NULL AND poll_claimed_at_ms IS NULL)
        OR (poll_owner IS NOT NULL AND poll_lease_epoch > 0
            AND poll_lease_expires_at_ms IS NOT NULL AND poll_claimed_at_ms IS NOT NULL
            AND poll_claimed_at_ms < poll_lease_expires_at_ms
            AND poll_lease_expires_at_ms <= provider_task_completion_deadline(submission_id, provider_deadline_at_ms)
            AND state = 'provider_waiting')),
    DROP CONSTRAINT provider_remote_tasks_deadline_shape_check,
    ADD CONSTRAINT provider_remote_tasks_deadline_shape_check CHECK (
        provider_deadline_at_ms > created_at_ms
        AND (state <> 'provider_waiting' OR next_poll_at_ms <= provider_task_completion_deadline(submission_id, provider_deadline_at_ms))
        AND (deadline_quarantine_id IS NULL OR
            (state = 'uncertain' AND artifact_ref IS NULL AND error_code = 'provider_remote_task_deadline'
             AND effect_certainty = 'unknown_remote_effect' AND next_poll_at_ms IS NULL
             AND poll_owner IS NULL AND poll_lease_expires_at_ms IS NULL AND poll_claimed_at_ms IS NULL
             AND terminal_at_ms >= provider_task_completion_deadline(submission_id, provider_deadline_at_ms))));

-- Patch the current definitions, retaining later remote-video/other guards.
DO $migration$
DECLARE definition TEXT; patched TEXT; prefix TEXT; marker TEXT;
BEGIN
    definition := pg_get_functiondef('validate_provider_task_observation()'::REGPROCEDURE);
    marker := 'ELSIF NEW.source IN (''poll'', ''cancel'') THEN';
    IF strpos(definition, marker) = 0 THEN RAISE EXCEPTION 'observation guard drift'; END IF;
    prefix := split_part(definition, marker, 1);
    patched := substr(definition, length(prefix) + 1);
    FOREACH marker IN ARRAY ARRAY[
        'task.provider_deadline_at_ms <= now_ms',
        'NEW.next_poll_at_ms > task.provider_deadline_at_ms',
        'task.provider_deadline_at_ms > NEW.observed_at_ms'
    ] LOOP
        IF strpos(patched, marker) = 0 THEN
            RAISE EXCEPTION 'observation budget guard drift: %', marker;
        END IF;
    END LOOP;
    patched := prefix || replace(patched, 'task.provider_deadline_at_ms',
        'provider_task_completion_deadline(task.submission_id, task.provider_deadline_at_ms)');
    EXECUTE patched;

    definition := pg_get_functiondef('enforce_provider_remote_task_update()'::REGPROCEDURE);
    patched := replace(definition, 'OLD.provider_deadline_at_ms <= now_ms',
        'provider_task_completion_deadline(OLD.submission_id, OLD.provider_deadline_at_ms) <= now_ms');
    IF patched = definition THEN RAISE EXCEPTION 'task update live budget guard drift'; END IF;
    definition := patched;
    patched := replace(definition, 'OLD.provider_deadline_at_ms > now_ms',
        'provider_task_completion_deadline(OLD.submission_id, OLD.provider_deadline_at_ms) > now_ms');
    IF patched = definition THEN RAISE EXCEPTION 'task update expired budget guard drift'; END IF;
    definition := patched;
    patched := replace(definition, 'NEW.poll_lease_expires_at_ms > NEW.provider_deadline_at_ms',
        'NEW.poll_lease_expires_at_ms > provider_task_completion_deadline(NEW.submission_id, NEW.provider_deadline_at_ms)');
    IF patched = definition THEN RAISE EXCEPTION 'task update poll fence budget guard drift'; END IF;
    definition := patched;
    patched := replace(definition, 'OLD.provider_deadline_at_ms > observation.observed_at_ms',
        'provider_task_completion_deadline(OLD.submission_id, OLD.provider_deadline_at_ms) > observation.observed_at_ms');
    IF patched = definition THEN RAISE EXCEPTION 'task update artifact recovery budget guard drift'; END IF;
    EXECUTE patched;

    definition := pg_get_functiondef('validate_provider_remote_task_quarantine_insert()'::REGPROCEDURE);
    patched := replace(definition, 'OR NEW.quarantined_at_ms > now_ms',
        'OR provider_task_completion_deadline(task.submission_id, task.provider_deadline_at_ms) > NEW.quarantined_at_ms OR NEW.quarantined_at_ms > now_ms');
    IF patched = definition THEN RAISE EXCEPTION 'quarantine budget guard drift'; END IF;
    EXECUTE patched;

    definition := pg_get_functiondef('validate_provider_remote_video_result()'::REGPROCEDURE);
    patched := replace(definition, 'task.provider_deadline_at_ms <= NEW.observed_at_ms',
        'provider_task_completion_deadline(task.submission_id, task.provider_deadline_at_ms) <= NEW.observed_at_ms');
    IF patched = definition THEN RAISE EXCEPTION 'video result budget guard drift'; END IF;
    EXECUTE patched;
END;
$migration$;
