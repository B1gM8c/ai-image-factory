-- Separate the original four-image/download authority from this exact historical
-- one-image/query-only + HTTP-retrieval authority. Never relax the v1 predicate.
DO $migration$
DECLARE constraint_name TEXT;
BEGIN
    SELECT conname INTO STRICT constraint_name FROM pg_constraint
    WHERE conrelid = 'dreamina_operator_image_success_resolutions'::REGCLASS
        AND contype = 'c'
        AND pg_get_constraintdef(oid) = 'CHECK ((upstream_result_count = 4))';
    EXECUTE format('ALTER TABLE dreamina_operator_image_success_resolutions DROP CONSTRAINT %I', constraint_name);
END;
$migration$;

ALTER TABLE dreamina_operator_image_success_resolutions
    ADD COLUMN evidence_mode TEXT NOT NULL DEFAULT 'download_four_v1',
    ADD COLUMN retrieval_audit_sha256 TEXT,
    ADD COLUMN image_url_sha256 TEXT,
    ADD COLUMN tenant_id TEXT,
    ADD COLUMN auth_attribution_json JSONB,
    ADD CONSTRAINT dreamina_operator_image_success_evidence_mode_check CHECK (
        (evidence_mode = 'download_four_v1' AND upstream_result_count = 4
            AND retrieval_audit_sha256 IS NULL AND image_url_sha256 IS NULL
            AND tenant_id IS NULL AND auth_attribution_json IS NULL)
        OR (evidence_mode = 'query_only_http_retrieval_v2' AND upstream_result_count = 1
            AND retrieval_audit_sha256 IS NOT NULL AND retrieval_audit_sha256 ~ '^[a-f0-9]{64}$'
            AND image_url_sha256 IS NOT NULL AND image_url_sha256 ~ '^[a-f0-9]{64}$'
            AND tenant_id IS NOT NULL AND length(tenant_id) > 0
            AND auth_attribution_json IS NOT NULL AND jsonb_typeof(auth_attribution_json) = 'object')
    );

-- Snapshot the deployed validator bodies. V1 remains byte-for-byte equivalent;
-- V2 copies its frozen execution/quote/lease fences with only the known error
-- code substitution, then adds the narrower model, tenant and owner requirements.
DO $migration$
DECLARE definition TEXT; copied TEXT; target TEXT;
BEGIN
    FOREACH target IN ARRAY ARRAY[
        'assert_operator_dreamina_image_success_evidence',
        'assert_operator_dreamina_image_success'
    ] LOOP
        definition := pg_get_functiondef(to_regprocedure(CASE WHEN target LIKE '%_evidence'
            THEN target || '(dreamina_operator_image_success_resolutions)' ELSE target || '(uuid)' END));
        IF definition IS NULL OR strpos(definition, 'dreamina_poll_artifact_invalid') = 0 THEN
            RAISE EXCEPTION 'expected strict historical four-image validator %', target;
        END IF;
        copied := replace(definition, 'FUNCTION ' || quote_ident(current_schema()) || '.' || target || '(',
            'FUNCTION ' || quote_ident(current_schema()) || '.' || target || '_v1(');
        IF copied = definition THEN RAISE EXCEPTION 'expected schema-qualified validator %', target; END IF;
        EXECUTE copied;
        copied := replace(copied, target || '_v1(', target || '_query_http_v2(');
        copied := replace(copied, 'dreamina_poll_artifact_invalid', 'dreamina_poll_protocol_invalid');
        IF target LIKE '%_evidence' THEN
            copied := replace(copied, E'BEGIN\n', $guard$BEGIN
    IF resolution.evidence_mode IS DISTINCT FROM 'query_only_http_retrieval_v2'
      OR resolution.upstream_result_count IS DISTINCT FROM 1
      OR NOT EXISTS (
          SELECT 1 FROM jobs job
          JOIN job_auth_attributions attribution USING (job_id, tenant_id)
          JOIN job_payloads payload USING (job_id)
          JOIN provider_submissions submission ON submission.submission_id = resolution.submission_id
              AND submission.job_id = job.job_id
          JOIN customer_price_quotes quote ON quote.job_id = job.job_id
          WHERE job.job_id = resolution.job_id AND job.tenant_id = resolution.tenant_id
              AND to_jsonb(attribution) = resolution.auth_attribution_json
              AND payload.command_json ->> 'model_version' = '5.0Pro'
              AND quote.provider_id = 'dreamina-cli'
              AND quote.provider_model_id = '5.0Pro'
              AND submission.remote_result_id IS NULL
      ) OR (SELECT count(*) FROM work_items WHERE job_id = resolution.job_id) <> 1
    THEN RAISE EXCEPTION 'single-image protocol recovery requires original tenant, owner, and 5.0Pro quote'; END IF;
$guard$);
        END IF;
        EXECUTE copied;
    END LOOP;

    -- The insert fence retains every untouched-funds predicate for both modes.
    definition := pg_get_functiondef('validate_operator_dreamina_image_success_insert()'::REGPROCEDURE);
    copied := replace(definition, '''dreamina_poll_artifact_invalid''',
        '(CASE WHEN NEW.evidence_mode = ''query_only_http_retrieval_v2'' THEN ''dreamina_poll_protocol_invalid'' ELSE ''dreamina_poll_artifact_invalid'' END)');
    IF copied = definition THEN RAISE EXCEPTION 'expected historical insert-fence error code'; END IF;
    EXECUTE copied;
END;
$migration$;

CREATE OR REPLACE FUNCTION assert_operator_dreamina_image_success_evidence(
    resolution dreamina_operator_image_success_resolutions
) RETURNS VOID AS $$
BEGIN
    -- NULL preserves the existing evidence-only helper's partial-record contract.
    -- Persisted resolutions always have a non-NULL mode and the paired count check.
    IF resolution.evidence_mode IS NULL OR resolution.evidence_mode = 'download_four_v1' THEN
        PERFORM assert_operator_dreamina_image_success_evidence_v1(resolution);
    ELSIF resolution.evidence_mode = 'query_only_http_retrieval_v2' THEN
        PERFORM assert_operator_dreamina_image_success_evidence_query_http_v2(resolution);
    ELSE RAISE EXCEPTION 'unsupported Dreamina image recovery evidence mode'; END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assert_operator_dreamina_image_success(target_job_id UUID) RETURNS VOID AS $$
DECLARE mode TEXT;
BEGIN
    SELECT evidence_mode INTO STRICT mode FROM dreamina_operator_image_success_resolutions WHERE job_id = target_job_id;
    IF mode = 'download_four_v1' THEN PERFORM assert_operator_dreamina_image_success_v1(target_job_id);
    ELSIF mode = 'query_only_http_retrieval_v2' THEN PERFORM assert_operator_dreamina_image_success_query_http_v2(target_job_id);
    ELSE RAISE EXCEPTION 'unsupported Dreamina image recovery evidence mode'; END IF;
END;
$$ LANGUAGE plpgsql;
