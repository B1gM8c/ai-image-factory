CREATE TABLE executor_provider_cost_evidence (
    manifest_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    submission_id UUID NOT NULL UNIQUE,
    scope TEXT NOT NULL CHECK (
        scope IN ('api_response', 'cli_invocation')
    ),
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    execution_surface TEXT NOT NULL CHECK (
        execution_surface IN ('provider_api', 'provider_cli')
    ),
    provider_operation_id TEXT NOT NULL CHECK (
        char_length(provider_operation_id) BETWEEN 1 AND 512
        AND provider_operation_id !~ '[[:cntrl:]]'
    ),
    currency TEXT NOT NULL CHECK (currency = 'USD'),
    native_unit TEXT NOT NULL CHECK (native_unit = 'usd_tick'),
    native_quantity NUMERIC(39, 0) NOT NULL CHECK (
        native_quantity >= 0
    ),
    authority TEXT NOT NULL CHECK (authority = 'provider_reported'),
    confidence TEXT NOT NULL CHECK (confidence = 'exact'),
    evidence_hash TEXT NOT NULL CHECK (
        evidence_hash ~ '^[0-9a-f]{64}$'
    ),
    evidence_path TEXT NOT NULL CHECK (
        char_length(evidence_path) BETWEEN 1 AND 512
        AND evidence_path !~ '[[:cntrl:]]'
    ),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (manifest_id, executor_execution_id, submission_id),
    FOREIGN KEY (manifest_id, executor_execution_id, submission_id)
        REFERENCES executor_result_manifests(
            manifest_id, executor_execution_id, submission_id
        )
        ON DELETE RESTRICT,
    CHECK (
        (scope = 'api_response' AND execution_surface = 'provider_api')
        OR
        (scope = 'cli_invocation' AND execution_surface = 'provider_cli')
    )
);

CREATE INDEX executor_provider_cost_evidence_operation_idx
    ON executor_provider_cost_evidence(
        provider_id, execution_surface, provider_operation_id
    );

CREATE FUNCTION validate_executor_provider_cost_evidence()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM executor_result_manifests manifest
        JOIN provider_submissions submission
          ON submission.submission_id = manifest.submission_id
         AND submission.executor_execution_id =
             manifest.executor_execution_id
        WHERE manifest.manifest_id = NEW.manifest_id
          AND manifest.executor_execution_id =
              NEW.executor_execution_id
          AND manifest.submission_id = NEW.submission_id
          AND submission.provider_id = NEW.provider_id
    ) THEN
        RAISE EXCEPTION
            'executor provider cost evidence is outside its execution authority'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_provider_cost_evidence_validate_contract
BEFORE INSERT ON executor_provider_cost_evidence
FOR EACH ROW EXECUTE FUNCTION validate_executor_provider_cost_evidence();

CREATE TRIGGER executor_provider_cost_evidence_reject_mutation
BEFORE UPDATE OR DELETE ON executor_provider_cost_evidence
FOR EACH ROW EXECUTE FUNCTION reject_executor_artifact_authority_mutation();

CREATE TRIGGER executor_provider_cost_evidence_reject_truncate
BEFORE TRUNCATE ON executor_provider_cost_evidence
FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_artifact_authority_mutation();
