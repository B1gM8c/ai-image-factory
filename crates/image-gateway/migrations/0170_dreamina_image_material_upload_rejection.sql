-- Reuse the append-only rejection audit for an original single-image edit.
-- The executor's uncertain decision and its provider receipt remain unchanged.
CREATE FUNCTION is_operator_dreamina_image_upload_rejection(
    target_job_id UUID, target_submission_id UUID
) RETURNS BOOLEAN LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1
        FROM jobs job
        JOIN job_payloads payload USING (job_id)
        JOIN work_items work USING (job_id)
        JOIN admission_sessions admission
          ON admission.session_id = payload.admission_session_id AND admission.job_id = job.job_id
         AND admission.tenant_id = job.tenant_id AND admission.operation = job.operation
        JOIN provider_submissions submission
          ON submission.job_id = job.job_id AND submission.work_item_id = work.work_item_id
         AND submission.tenant_id = job.tenant_id AND submission.provider_id = job.provider_id
        JOIN provider_remote_submit_intents intent USING (submission_id, executor_execution_id)
        WHERE job.job_id = target_job_id AND submission.submission_id = target_submission_id
          AND job.provider_id = 'dreamina-cli' AND job.operation = 'generation'
          AND job.output_count = 1 AND job.billable_units = 1 AND work.kind = 'image_batch'
          AND admission.api_profile = 'dreamina-cli-images-v1'
          AND submission.operation_id = 'images.generations'
          AND submission.operation_binding_version = 2 AND submission.completion_mode = 'remote_task'
          AND submission.command_schema = payload.command_schema
          AND payload.command_schema = 'dreamina-cli.submit.v1'
          AND payload.command_json @> '{"schema_version":1,"operation":"image2image","generate_num":1,"poll":0}'
          AND intent.provider_id = submission.provider_id
          AND intent.provider_account_id = submission.provider_account_id
          AND intent.output_index = 0 AND intent.output_total = 1
          AND intent.failure_error_code = 'dreamina_submit_rejected'
          AND NOT EXISTS (SELECT 1 FROM dreamina_native_batches batch WHERE batch.job_id = job.job_id)
          AND NOT EXISTS (SELECT 1 FROM dreamina_image_result_sets result WHERE result.submission_id = submission.submission_id)
          AND NOT EXISTS (SELECT 1 FROM executor_capacity_risk_waivers waiver WHERE waiver.submission_id = submission.submission_id)
          AND NOT EXISTS (SELECT 1 FROM dreamina_operator_image_success_resolutions success WHERE success.job_id = job.job_id)
          AND NOT EXISTS (SELECT 1 FROM dreamina_operator_image_supplement_authorizations supplement WHERE supplement.job_id = job.job_id)
    );
$$;

DO $migration$
DECLARE definition TEXT; patched TEXT;
BEGIN
    definition := pg_get_functiondef('assert_operator_dreamina_rejection(uuid)'::REGPROCEDURE);
    patched := replace(definition,
        'AND job.operation = ''video_generation'' AND job.output_count = 1',
        $condition$AND job.output_count = 1 AND (
              (job.operation = 'video_generation' AND work.kind = 'video_single')
              OR (resolution.reason = 'dreamina_material_upload_failed' AND quota.started_units = 0
                  AND is_operator_dreamina_image_upload_rejection(job.job_id, submission.submission_id))
          )$condition$);
    IF patched = definition THEN
        RAISE EXCEPTION 'expected existing job kind in operator rejection guard';
    END IF;
    definition := patched;
    patched := replace(definition, 'AND work.kind = ''video_single'' AND work.state = ''failed''',
        'AND work.state = ''failed''');
    IF patched = definition THEN
        RAISE EXCEPTION 'expected existing work kind in operator rejection guard';
    END IF;
    EXECUTE patched;
END;
$migration$;
