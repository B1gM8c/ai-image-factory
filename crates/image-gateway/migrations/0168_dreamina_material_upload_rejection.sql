-- A sealed material-upload failure may resolve the original customer hold
-- after review, while leaving the original executor decision and receipt intact.
ALTER TABLE dreamina_operator_rejection_resolutions
    DROP CONSTRAINT dreamina_operator_rejection_resolutions_reason_check,
    ADD CONSTRAINT dreamina_operator_rejection_resolutions_reason_check CHECK (
        reason IN ('dreamina_compliance_confirmation_required', 'dreamina_concurrency_limit_exceeded',
                   'dreamina_material_upload_failed')
    );

DO $migration$
DECLARE definition TEXT; patched TEXT;
BEGIN
    definition := pg_get_functiondef('assert_operator_dreamina_rejection(uuid)'::REGPROCEDURE);
    patched := replace(definition,
        'AND intent.failure_error_code = ''dreamina_submit_recovery_evidence_invalid''))',
        'AND intent.failure_error_code = ''dreamina_submit_recovery_evidence_invalid'')
              OR (resolution.reason = ''dreamina_material_upload_failed''
                 AND intent.failure_error_code IN (''dreamina_submit_rejected'', ''dreamina_submit_recovery_evidence_invalid'')
                 AND intent.failure_event_identity IS NOT NULL AND intent.send_started_at_ms IS NOT NULL))');
    IF patched = definition THEN
        RAISE EXCEPTION 'expected existing rejection audit guard';
    END IF;
    EXECUTE patched;
END;
$migration$;
