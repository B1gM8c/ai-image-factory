CREATE TABLE executor_artifact_authorities (
    authority_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL UNIQUE,
    submission_id UUID NOT NULL UNIQUE,
    output_id UUID NOT NULL UNIQUE,
    job_id UUID NOT NULL,
    storage_backend TEXT NOT NULL CHECK (storage_backend = 'filesystem-v1'),
    storage_namespace TEXT NOT NULL CHECK (
        char_length(storage_namespace) BETWEEN 1 AND 1024
        AND storage_namespace !~ '[[:cntrl:]]'
        AND storage_namespace LIKE 'filesystem-v1:%'
    ),
    object_key TEXT NOT NULL CHECK (
        char_length(object_key) BETWEEN 1 AND 1024
        AND object_key !~ '[[:cntrl:]]'
    ),
    sha256_hex TEXT NOT NULL CHECK (sha256_hex ~ '^[0-9a-f]{64}$'),
    byte_size BIGINT NOT NULL CHECK (byte_size BETWEEN 1 AND 268435456),
    media_type TEXT NOT NULL CHECK (
        media_type IN ('image/png', 'image/jpeg', 'image/webp')
    ),
    created_at_ms BIGINT NOT NULL,
    CHECK (authority_id = executor_execution_id),
    CHECK (submission_id <> executor_execution_id),
    UNIQUE (storage_namespace, object_key),
    UNIQUE (authority_id, executor_execution_id, submission_id),
    UNIQUE (
        authority_id, executor_execution_id, submission_id,
        storage_backend, storage_namespace, object_key, sha256_hex, byte_size, media_type
    ),
    FOREIGN KEY (executor_execution_id, submission_id)
        REFERENCES executor_executions(executor_execution_id, submission_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (submission_id, output_id, job_id)
        REFERENCES provider_submissions(submission_id, output_id, job_id)
        ON DELETE RESTRICT
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM executor_result_manifests) THEN
        RAISE EXCEPTION
            'executor artifact authority migration requires executor_result_manifests to be empty';
    END IF;
END;
$$;

ALTER TABLE executor_result_manifests
    ADD COLUMN artifact_authority_id UUID NOT NULL;

ALTER TABLE executor_result_manifests
    DROP COLUMN storage_backend,
    DROP COLUMN object_key,
    DROP COLUMN sha256_hex,
    DROP COLUMN byte_size,
    DROP COLUMN media_type;

ALTER TABLE executor_result_manifests
    ADD CONSTRAINT executor_result_manifest_authority_fk
    FOREIGN KEY (
        artifact_authority_id, executor_execution_id, submission_id
    )
    REFERENCES executor_artifact_authorities (
        authority_id, executor_execution_id, submission_id
    )
    ON DELETE RESTRICT;

ALTER TABLE executor_result_manifests
    ADD CONSTRAINT executor_result_manifest_deterministic_ids_check
    CHECK (
        manifest_id = submission_id
        AND artifact_authority_id = executor_execution_id
    );

CREATE FUNCTION reject_executor_artifact_authority_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'executor artifact authorities are append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_artifact_authorities_reject_mutation
    BEFORE UPDATE OR DELETE ON executor_artifact_authorities
    FOR EACH ROW EXECUTE FUNCTION reject_executor_artifact_authority_mutation();

CREATE TRIGGER executor_artifact_authorities_reject_truncate
    BEFORE TRUNCATE ON executor_artifact_authorities
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_artifact_authority_mutation();

CREATE TRIGGER executor_result_manifests_reject_mutation
    BEFORE UPDATE OR DELETE ON executor_result_manifests
    FOR EACH ROW EXECUTE FUNCTION reject_executor_artifact_authority_mutation();

CREATE TRIGGER executor_result_manifests_reject_truncate
    BEFORE TRUNCATE ON executor_result_manifests
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_artifact_authority_mutation();
