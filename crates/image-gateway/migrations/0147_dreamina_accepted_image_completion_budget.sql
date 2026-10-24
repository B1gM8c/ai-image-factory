-- Images retain their frozen submit authority and one real remote operation.
-- Policy 1 adds only the original-acceptance-based 24h completion window.
ALTER TABLE accepted_completion_budgets
    DROP CONSTRAINT accepted_completion_budgets_grant_source_check,
    ADD CONSTRAINT accepted_completion_budgets_grant_source_check
        CHECK (grant_source IN ('accepted_attach', 'migration_0143', 'migration_0147'));

-- Share one eligibility check between attach, the insert fence, and backfill.
-- VOLATILE sees the submission state that the attach statement just updated.
CREATE FUNCTION dreamina_accepted_completion_budget_eligible(target_submission UUID)
RETURNS BOOLEAN LANGUAGE SQL VOLATILE AS $$
    SELECT EXISTS (
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
            JOIN provider_remote_tasks task
              ON task.submission_id = submission.submission_id
             AND task.executor_execution_id = submission.executor_execution_id
            WHERE submission.submission_id = target_submission
              AND task.provider_id = 'dreamina-cli' AND task.state = 'provider_waiting'
              AND task.poll_owner IS NULL AND task.poll_lease_expires_at_ms IS NULL
              AND task.poll_claimed_at_ms IS NULL AND task.deadline_quarantine_id IS NULL
              AND submission.provider_id = 'dreamina-cli'
              AND (
                  (submission.operation_id = 'videos.generations'
                   AND job.operation = 'video_generation' AND job.output_count = 1)
                  OR (
                      submission.operation_id = 'images.generations'
                      AND job.operation IN ('generation', 'edit')
                      AND job.provider_id = task.provider_id
                      AND submission.provider_account_id = task.provider_account_id
                      AND intent.provider_id = submission.provider_id
                      AND intent.provider_account_id = submission.provider_account_id
                      AND recovery.provider_id = submission.provider_id
                      AND recovery.provider_account_id = submission.provider_account_id
                      AND submission.operation_binding_version = 2
                      AND submission.completion_mode = 'remote_task'
                      AND submission.command_schema = 'dreamina-cli.submit.v1'
                      AND NOT EXISTS (
                          SELECT 1 FROM executor_artifact_authorities authority
                          JOIN provider_submissions member USING (submission_id, executor_execution_id)
                          WHERE member.job_id = job.job_id)
                      AND NOT EXISTS (
                          SELECT 1 FROM executor_result_manifests manifest
                          JOIN provider_submissions member USING (submission_id, executor_execution_id)
                          WHERE member.job_id = job.job_id)
                      AND EXISTS (
                          SELECT 1 FROM job_payloads payload
                          WHERE payload.job_id = job.job_id AND payload.command_schema = submission.command_schema
                            AND payload.command_json -> 'poll' = '0'::JSONB
                            AND (
                                (job.output_count = 1
                                 AND payload.command_json -> 'schema_version' = '1'::JSONB
                                 AND (
                                     (payload.command_json ->> 'operation' IN ('text2image', 'image2image')
                                      AND payload.command_json -> 'generate_num' = '1'::JSONB)
                                     OR (payload.command_json ->> 'operation' = 'image_upscale'
                                         AND NOT payload.command_json ? 'generate_num'))
                                 AND (SELECT count(*) FROM provider_submissions WHERE job_id = job.job_id) = 1
                                 AND NOT EXISTS (SELECT 1 FROM dreamina_native_batch_members
                                                 WHERE submission_id = task.submission_id))
                                OR EXISTS (
                                    SELECT 1 FROM dreamina_native_batches batch
                                    WHERE batch.leader_submission_id = task.submission_id
                                      AND batch.job_id = job.job_id AND batch.output_count = job.output_count
                                      AND batch.command_hash = submission.command_hash
                                      AND payload.command_json -> 'schema_version' = '2'::JSONB
                                      AND payload.command_json ->> 'operation' IN ('text2image', 'image2image')
                                      AND payload.command_json -> 'generate_num' = to_jsonb(batch.output_count)
                                      AND (SELECT count(*) FROM dreamina_native_batch_members
                                           WHERE leader_submission_id = batch.leader_submission_id) = batch.output_count
                                      AND NOT EXISTS (
                                          SELECT 1 FROM dreamina_native_batch_members member
                                          JOIN provider_submissions member_submission USING (submission_id, executor_execution_id)
                                          JOIN executor_executions member_execution USING (submission_id, executor_execution_id)
                                          JOIN job_outputs output ON output.output_id = member_submission.output_id
                                          WHERE member.leader_submission_id = batch.leader_submission_id
                                            AND (member.output_index >= batch.output_count
                                                 OR member_submission.job_id <> batch.job_id OR output.job_id <> batch.job_id
                                                 OR output.output_index <> member.output_index
                                                 OR member_submission.provider_id <> submission.provider_id
                                                 OR member_submission.provider_account_id <> submission.provider_account_id
                                                 OR member_submission.execution_profile_id <> submission.execution_profile_id
                                                 OR member_submission.work_item_id <> submission.work_item_id
                                                 OR member_submission.command_hash <> batch.command_hash
                                                 OR member_submission.resolution_decision_id IS NOT NULL
                                                 OR member_execution.resolution_decision_id IS NOT NULL
                                                 OR (member.output_index > 0 AND
                                                     (member_submission.state <> 'prepared' OR member_execution.state <> 'prepared'))))
                                      AND NOT EXISTS (
                                          SELECT 1 FROM dreamina_native_batch_members member
                                          JOIN dreamina_native_batch_results result USING (submission_id, executor_execution_id)
                                          WHERE member.leader_submission_id = batch.leader_submission_id)
                                      AND NOT EXISTS (
                                          SELECT 1 FROM dreamina_native_batch_members member
                                          JOIN provider_remote_submit_intents member_intent USING (submission_id, executor_execution_id)
                                          WHERE member.leader_submission_id = batch.leader_submission_id AND member.output_index > 0)
                                )
                            )
                      )
                  )
              )
              AND job.economics_contract_version = 4 AND job.charged_units = 0
              AND submission.state = 'provider_waiting'
              AND execution.state = 'provider_waiting'
              AND submission.resolution_decision_id IS NULL
              AND execution.resolution_decision_id IS NULL
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
    );
$$;

CREATE OR REPLACE FUNCTION validate_accepted_completion_budget() RETURNS TRIGGER AS $$
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
       OR NOT dreamina_accepted_completion_budget_eligible(NEW.submission_id)
       OR (NEW.grant_source = 'migration_0147' AND NOT EXISTS (
           SELECT 1 FROM provider_submissions WHERE submission_id = NEW.submission_id
             AND operation_id = 'images.generations')) THEN
        RAISE EXCEPTION 'completion budget requires one untouched accepted Dreamina operation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION grant_dreamina_accepted_completion_budget() RETURNS TRIGGER AS $$
DECLARE task provider_remote_tasks%ROWTYPE;
BEGIN
    IF NEW.state <> 'provider_waiting' OR OLD.state = 'provider_waiting'
       OR NEW.provider_id <> 'dreamina-cli' OR NEW.operation_id NOT IN ('videos.generations', 'images.generations') THEN
        RETURN NULL;
    END IF;
    SELECT * INTO task FROM provider_remote_tasks WHERE submission_id = NEW.submission_id FOR UPDATE;
    IF FOUND AND task.created_at_ms <= 9223372036768375807
       AND task.created_at_ms + 86400000 > task.provider_deadline_at_ms
       AND dreamina_accepted_completion_budget_eligible(task.submission_id) THEN
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

-- Lock before rechecking the original task. A concurrent poll is skipped;
-- no lease, next-poll time, submit context, or acceptance timestamp is reset.
DO $migration$
DECLARE candidate provider_remote_tasks%ROWTYPE;
BEGIN
    FOR candidate IN
        SELECT task.* FROM provider_remote_tasks task
        JOIN provider_submissions submission USING (submission_id, executor_execution_id)
        WHERE submission.operation_id = 'images.generations'
          AND NOT EXISTS (SELECT 1 FROM accepted_completion_budgets WHERE submission_id = task.submission_id)
          AND task.created_at_ms <= 9223372036768375807
          AND task.created_at_ms + 86400000 > task.provider_deadline_at_ms
          AND task.created_at_ms + 86400000 > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
          AND dreamina_accepted_completion_budget_eligible(task.submission_id)
        FOR UPDATE OF task SKIP LOCKED
    LOOP
        IF dreamina_accepted_completion_budget_eligible(candidate.submission_id) THEN
            INSERT INTO accepted_completion_budgets
              (submission_id, executor_execution_id, remote_operation_id,
               original_submit_deadline_at_ms, accepted_at_ms, completion_deadline_at_ms,
               policy_version, grant_source, granted_at_ms)
            VALUES (candidate.submission_id, candidate.executor_execution_id, candidate.remote_operation_id,
                    candidate.provider_deadline_at_ms, candidate.created_at_ms, candidate.created_at_ms + 86400000,
                    1, 'migration_0147', floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT);
        END IF;
    END LOOP;
END;
$migration$;

-- Followers use the effective deadline of their one real, immutable leader.
-- Assert each original deadline guard separately before patching it.
DO $migration$
DECLARE definition TEXT; patched TEXT; marker TEXT;
BEGIN
    definition := pg_get_functiondef('validate_dreamina_native_batch_result()'::REGPROCEDURE);
    FOREACH marker IN ARRAY ARRAY[
        'task.provider_deadline_at_ms > NEW.observed_at_ms',
        'task.provider_deadline_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT'
    ] LOOP
        patched := replace(definition, marker, replace(marker, 'task.provider_deadline_at_ms',
            'provider_task_completion_deadline(task.submission_id, task.provider_deadline_at_ms)'));
        IF patched = definition THEN RAISE EXCEPTION 'native batch completion guard drift: %', marker; END IF;
        definition := patched;
    END LOOP;
    EXECUTE definition;
    definition := pg_get_functiondef('validate_dreamina_native_member_artifact()'::REGPROCEDURE);
    marker := 'task.provider_deadline_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT';
    patched := replace(definition, marker, replace(marker, 'task.provider_deadline_at_ms',
        'provider_task_completion_deadline(task.submission_id, task.provider_deadline_at_ms)'));
    IF patched = definition THEN RAISE EXCEPTION 'native artifact completion guard drift'; END IF;
    EXECUTE patched;
END;
$migration$;
