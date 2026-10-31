-- Extend only the reviewed original-archive authority to the original 15-second
-- contract. Legacy poll recovery remains 10 seconds; no historical row is edited.
DO $migration$
DECLARE definition TEXT; patched TEXT; old TEXT; replacement TEXT;
BEGIN
    definition := pg_get_functiondef('assert_operator_dreamina_normal_video_archive_evidence(dreamina_operator_video_success_resolutions)'::REGPROCEDURE);
    patched := definition;
    FOR old, replacement IN VALUES
        ('job.requested_units = 10 AND job.billable_units = 10',
         'job.requested_units IN (10,15) AND job.billable_units = job.requested_units'),
        ('payload.command_json -> ''duration'' = ''10''::JSONB',
         'payload.command_json -> ''duration'' = to_jsonb(job.requested_units)'),
        ('output.billable_units = 10', 'output.billable_units = job.requested_units'),
        ('quota.requested_units = 10', 'quota.requested_units = job.requested_units'),
        ('''duration'', ''10'', ''resolution''', '''duration'', job.requested_units::TEXT, ''resolution'''),
        ('intent.failure_error_code = ''dreamina_submit_receipt_invalid''',
         'intent.failure_error_code IN (''dreamina_submit_receipt_invalid'', ''dreamina_submit_recovery_evidence_invalid'')'),
        ('max_quantity = 10 AND quantity_source',
         'max_quantity = (SELECT requested_units FROM jobs WHERE job_id = resolution.job_id) AND quantity_source')
    LOOP
        IF (length(patched) - length(replace(patched, old, ''))) / length(old) <> 1 THEN
            RAISE EXCEPTION 'missing unique original archive duration guard: %', old;
        END IF;
        patched := replace(patched, old, replacement);
    END LOOP;
    EXECUTE patched;

    definition := pg_get_functiondef('assert_operator_dreamina_video_success(uuid)'::REGPROCEDURE);
    old := 'charge_id UUID;';
    IF strpos(definition, old) = 0 THEN RAISE EXCEPTION 'missing video success declaration'; END IF;
    patched := replace(definition, old, 'charge_id UUID; original_duration INTEGER;');
    old := 'PERFORM assert_operator_dreamina_video_success_evidence(resolution);';
    IF strpos(patched, old) = 0 THEN RAISE EXCEPTION 'missing original evidence validation'; END IF;
    patched := replace(patched, old, $body$PERFORM assert_operator_dreamina_video_success_evidence(resolution);
    SELECT CASE WHEN resolution.evidence_mode = 'normal_submit_archive_v1'
        THEN job.requested_units ELSE 10 END INTO STRICT original_duration
    FROM jobs job WHERE job.job_id = target_job_id;
    IF original_duration NOT IN (10,15) THEN
        RAISE EXCEPTION 'unsupported original archive duration';
    END IF;$body$);
    FOREACH old IN ARRAY ARRAY[
        'job.charged_units = 10', 'quota.committed_units = 10',
        'fact.quantity = 10', 'usage.units = 10', 'line.actual_quantity = 10',
        'outcome = ''succeeded'' AND units = 10'
    ] LOOP
        IF (length(patched) - length(replace(patched, old, ''))) / length(old) <> 1 THEN
            RAISE EXCEPTION 'missing unique original settlement duration guard: %', old;
        END IF;
        patched := replace(patched, old, left(old, length(old) - 2) || 'original_duration');
    END LOOP;
    EXECUTE patched;

    definition := pg_get_functiondef('validate_v4_provider_usage_fact_insert()'::REGPROCEDURE);
    old := 'AND NEW.quantity = 10 AND NEW.quantity_source = ''request_derived''';
    IF (length(definition) - length(replace(definition, old, ''))) / length(old) <> 1 THEN
        RAISE EXCEPTION 'missing unique reviewed video usage quantity guard';
    END IF;
    patched := replace(definition, old, $body$AND NEW.quantity = (
                     SELECT CASE WHEN resolution.evidence_mode = 'normal_submit_archive_v1'
                         THEN job.requested_units ELSE 10 END
                     FROM dreamina_operator_video_success_resolutions resolution
                     JOIN jobs job ON job.job_id = resolution.job_id
                     WHERE resolution.resolution_id::TEXT = NEW.metadata_json ->> 'operator_resolution_id'
                       AND resolution.job_id = NEW.job_id
                       AND (resolution.evidence_mode = 'legacy_poll_v1'
                            OR (resolution.evidence_mode = 'normal_submit_archive_v1'
                                AND job.requested_units IN (10,15))))
                 AND NEW.quantity_source = 'request_derived'$body$);
    EXECUTE patched;
END;
$migration$;
