-- Extend the existing operator refusal audit only for an exact, bound 1310
-- pre-acceptance refusal. Original executor uncertainty remains immutable.
ALTER TABLE dreamina_operator_rejection_resolutions
    DROP CONSTRAINT dreamina_operator_rejection_resolutions_reason_check,
    ADD CONSTRAINT dreamina_operator_rejection_resolutions_reason_check CHECK (
        reason IN ('dreamina_compliance_confirmation_required', 'dreamina_concurrency_limit_exceeded')
    );

DO $migration$
DECLARE definition TEXT; patched TEXT;
BEGIN
    definition := pg_get_functiondef('assert_operator_dreamina_rejection(uuid)'::REGPROCEDURE);
    patched := replace(definition,
        'AND intent.failure_error_code = ''dreamina_submit_rejected''',
        'AND ((resolution.reason = ''dreamina_compliance_confirmation_required''
                 AND intent.failure_error_code = ''dreamina_submit_rejected'')
              OR (resolution.reason = ''dreamina_concurrency_limit_exceeded''
                 AND intent.failure_error_code = ''dreamina_submit_recovery_evidence_invalid''))');
    IF patched = definition THEN
        RAISE EXCEPTION 'expected immutable rejection audit guard';
    END IF;
    EXECUTE patched;
END;
$migration$;
