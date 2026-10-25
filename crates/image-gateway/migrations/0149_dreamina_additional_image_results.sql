-- Physical CLI candidates are not billable job outputs or executor result manifests.
CREATE TABLE dreamina_image_result_sets (
    submission_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL,
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE RESTRICT,
    returned_count INTEGER NOT NULL CHECK (returned_count BETWEEN 1 AND 10),
    created_at_ms BIGINT NOT NULL,
    cleanup_after_ms BIGINT NOT NULL CHECK (cleanup_after_ms > created_at_ms),
    cleanup_state TEXT NOT NULL DEFAULT 'available' CHECK (cleanup_state IN ('available', 'deleting', 'deleted')),
    cleanup_owner TEXT,
    cleanup_epoch BIGINT NOT NULL DEFAULT 0 CHECK (cleanup_epoch >= 0),
    cleanup_lease_expires_at_ms BIGINT,
    retry_after_ms BIGINT NOT NULL DEFAULT 0,
    UNIQUE (submission_id, executor_execution_id, job_id),
    FOREIGN KEY (executor_execution_id, submission_id)
      REFERENCES executor_executions(executor_execution_id, submission_id) ON DELETE RESTRICT,
    CHECK ((cleanup_state = 'deleting' AND cleanup_owner IS NOT NULL
              AND cleanup_epoch > 0 AND cleanup_lease_expires_at_ms IS NOT NULL)
        OR (cleanup_state <> 'deleting' AND cleanup_owner IS NULL
              AND cleanup_lease_expires_at_ms IS NULL))
);

CREATE TABLE dreamina_image_additional_artifacts (
    submission_id UUID NOT NULL,
    executor_execution_id UUID NOT NULL,
    job_id UUID NOT NULL,
    source_index INTEGER NOT NULL CHECK (source_index BETWEEN 1 AND 9),
    artifact_id UUID NOT NULL UNIQUE,
    storage_backend TEXT NOT NULL CHECK (storage_backend = 'filesystem-v1'),
    storage_namespace TEXT NOT NULL CHECK (storage_namespace LIKE 'filesystem-v1:%' AND char_length(storage_namespace) <= 1024 AND storage_namespace !~ '[[:cntrl:]]'),
    object_key TEXT NOT NULL CHECK (char_length(object_key) BETWEEN 1 AND 1024 AND object_key !~ '[[:cntrl:]]'),
    sha256_hex TEXT NOT NULL CHECK (sha256_hex ~ '^[0-9a-f]{64}$'),
    byte_size BIGINT NOT NULL CHECK (byte_size BETWEEN 1 AND 268435456),
    media_type TEXT NOT NULL CHECK (media_type IN ('image/png', 'image/jpeg', 'image/webp')),
    PRIMARY KEY (submission_id, source_index),
    UNIQUE (storage_namespace, object_key),
    FOREIGN KEY (submission_id, executor_execution_id, job_id)
      REFERENCES dreamina_image_result_sets(submission_id, executor_execution_id, job_id) ON DELETE RESTRICT
);

CREATE FUNCTION protect_dreamina_image_result_set() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP <> 'UPDATE' THEN
        RAISE EXCEPTION 'Dreamina physical result sets cannot be removed';
    END IF;
    IF (NEW.submission_id, NEW.executor_execution_id, NEW.job_id, NEW.returned_count,
        NEW.created_at_ms, NEW.cleanup_after_ms)
       IS DISTINCT FROM
       (OLD.submission_id, OLD.executor_execution_id, OLD.job_id, OLD.returned_count,
        OLD.created_at_ms, OLD.cleanup_after_ms) THEN
        RAISE EXCEPTION 'Dreamina physical result metadata is immutable';
    END IF;
    IF OLD.cleanup_state = 'deleted' OR NEW.cleanup_epoch < OLD.cleanup_epoch THEN
        RAISE EXCEPTION 'Dreamina physical result cleanup cannot be reversed';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_image_result_sets_protect BEFORE UPDATE OR DELETE ON dreamina_image_result_sets
    FOR EACH ROW EXECUTE FUNCTION protect_dreamina_image_result_set();
CREATE TRIGGER dreamina_image_result_sets_reject_truncate BEFORE TRUNCATE ON dreamina_image_result_sets
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_artifact_authority_mutation();
CREATE TRIGGER dreamina_image_additional_artifacts_immutable BEFORE UPDATE OR DELETE ON dreamina_image_additional_artifacts
    FOR EACH ROW EXECUTE FUNCTION reject_executor_artifact_authority_mutation();
CREATE TRIGGER dreamina_image_additional_artifacts_reject_truncate BEFORE TRUNCATE ON dreamina_image_additional_artifacts
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_artifact_authority_mutation();

CREATE FUNCTION validate_dreamina_image_result_set() RETURNS TRIGGER AS $$
DECLARE
    source_submission UUID;
    expected_count INTEGER;
    actual_count BIGINT;
    last_index INTEGER;
BEGIN
    source_submission := NEW.submission_id;
    SELECT returned_count - 1 INTO STRICT expected_count
      FROM dreamina_image_result_sets WHERE submission_id = source_submission;
    SELECT count(*), COALESCE(max(source_index), 0) INTO actual_count, last_index
      FROM dreamina_image_additional_artifacts WHERE submission_id = source_submission;
    IF actual_count <> expected_count OR last_index <> expected_count THEN
        RAISE EXCEPTION 'Dreamina physical result set is incomplete';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER dreamina_image_result_set_complete AFTER INSERT ON dreamina_image_result_sets
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_dreamina_image_result_set();
CREATE CONSTRAINT TRIGGER dreamina_image_artifacts_complete AFTER INSERT ON dreamina_image_additional_artifacts
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_dreamina_image_result_set();
CREATE INDEX dreamina_image_result_sets_cleanup_idx
    ON dreamina_image_result_sets(cleanup_state, cleanup_after_ms, submission_id)
    WHERE cleanup_state <> 'deleted';
CREATE INDEX dreamina_image_result_sets_job_idx ON dreamina_image_result_sets(job_id);

-- Register the bounded deterministic staging namespace before CLI polling writes bytes.
-- A crash before sealing the result set therefore still leaves a reclaimable intent.
CREATE TABLE dreamina_image_stage_intents (
    submission_id UUID PRIMARY KEY,
    executor_execution_id UUID NOT NULL,
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE RESTRICT,
    cleanup_after_ms BIGINT NOT NULL,
    cleanup_state TEXT NOT NULL DEFAULT 'available' CHECK (cleanup_state IN ('available','deleting','deleted')),
    cleanup_owner TEXT,
    cleanup_epoch BIGINT NOT NULL DEFAULT 0 CHECK (cleanup_epoch >= 0),
    cleanup_lease_expires_at_ms BIGINT,
    retry_after_ms BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (executor_execution_id, submission_id)
      REFERENCES executor_executions(executor_execution_id, submission_id) ON DELETE RESTRICT,
    CHECK ((cleanup_state = 'deleting' AND cleanup_owner IS NOT NULL
              AND cleanup_epoch > 0 AND cleanup_lease_expires_at_ms IS NOT NULL)
        OR (cleanup_state <> 'deleting' AND cleanup_owner IS NULL
              AND cleanup_lease_expires_at_ms IS NULL))
);
CREATE INDEX dreamina_image_stage_intents_cleanup_idx
    ON dreamina_image_stage_intents(cleanup_state,cleanup_after_ms,submission_id)
    WHERE cleanup_state <> 'deleted';
CREATE FUNCTION protect_dreamina_image_stage_intent() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP <> 'UPDATE' THEN RAISE EXCEPTION 'Dreamina staging intents cannot be removed'; END IF;
    IF (NEW.submission_id,NEW.executor_execution_id,NEW.job_id,NEW.cleanup_after_ms)
        IS DISTINCT FROM (OLD.submission_id,OLD.executor_execution_id,OLD.job_id,OLD.cleanup_after_ms)
       OR OLD.cleanup_state = 'deleted' OR NEW.cleanup_epoch < OLD.cleanup_epoch THEN
        RAISE EXCEPTION 'Dreamina staging intent metadata is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_image_stage_intents_protect BEFORE UPDATE OR DELETE ON dreamina_image_stage_intents
    FOR EACH ROW EXECUTE FUNCTION protect_dreamina_image_stage_intent();
CREATE TRIGGER dreamina_image_stage_intents_reject_truncate BEFORE TRUNCATE ON dreamina_image_stage_intents
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_artifact_authority_mutation();

CREATE FUNCTION bind_dreamina_image_result_lineage() RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM provider_submissions submission
                   WHERE submission.submission_id = NEW.submission_id
                     AND submission.executor_execution_id = NEW.executor_execution_id
                     AND submission.job_id = NEW.job_id
                     AND submission.provider_id = 'dreamina-cli') THEN
        RAISE EXCEPTION 'Dreamina physical results do not match their original submission';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_image_result_sets_lineage BEFORE INSERT ON dreamina_image_result_sets
    FOR EACH ROW EXECUTE FUNCTION bind_dreamina_image_result_lineage();
CREATE TRIGGER dreamina_image_stage_intents_lineage BEFORE INSERT ON dreamina_image_stage_intents
    FOR EACH ROW EXECUTE FUNCTION bind_dreamina_image_result_lineage();
