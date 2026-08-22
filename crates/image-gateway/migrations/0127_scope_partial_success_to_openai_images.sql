-- Migration 126 introduced partial-output success for the shared media parent
-- invariant. Keep that migration immutable and narrow the policy to the
-- OpenAI-compatible image commands that expose n > 1 at this API boundary.
DO $migration$
DECLARE
    target REGPROCEDURE := 'assert_terminal_parent_completion(uuid)'::REGPROCEDURE;
    definition TEXT;
    patched TEXT;
BEGIN
    definition := pg_get_functiondef(target);
    patched := replace(
        definition,
        $old$expected_parent_state := CASE
        WHEN uncertain_count > 0 THEN 'uncertain'
        WHEN succeeded_count > 0 THEN 'succeeded'
        ELSE 'failed'
    END;$old$,
        $new$expected_parent_state := CASE
        WHEN uncertain_count > 0 THEN 'uncertain'
        WHEN failed_count > 0 AND NOT EXISTS (
            SELECT 1
            FROM job_payloads payload
            WHERE payload.job_id = target_job_id
              AND payload.command_schema IN (
                  'openai.images.generation.v1', 'openai.images.edit.v1'
              )
        ) THEN 'failed'
        WHEN succeeded_count > 0 THEN 'succeeded'
        ELSE 'failed'
    END;$new$
    );
    IF patched = definition THEN
        RAISE EXCEPTION 'expected partial terminal parent policy was not found';
    END IF;
    EXECUTE patched;
END;
$migration$;
