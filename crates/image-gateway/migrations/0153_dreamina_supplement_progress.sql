-- Supplement observations never rewrite the unknown original execution.
CREATE TABLE dreamina_operator_video_supplement_observations (
    observation_id UUID PRIMARY KEY,
    authorization_id UUID NOT NULL REFERENCES dreamina_operator_video_supplement_authorizations(authorization_id),
    dispatch_nonce UUID NOT NULL,
    remote_operation_id TEXT NOT NULL,
    stdout_sha256 TEXT NOT NULL CHECK (stdout_sha256 ~ '^[a-f0-9]{64}$'),
    observed_at_ms BIGINT NOT NULL CHECK (observed_at_ms > 0),
    query_status TEXT NOT NULL CHECK (query_status IN ('querying', 'failed', 'success')),
    progress_json JSONB CHECK (progress_json IS NULL OR
        (query_status = 'querying' AND jsonb_typeof(progress_json) = 'object'
         AND octet_length(progress_json::TEXT) <= 1024)),
    UNIQUE (authorization_id, observed_at_ms, stdout_sha256)
);
CREATE INDEX dreamina_supplement_observations_latest
    ON dreamina_operator_video_supplement_observations
        (authorization_id, observed_at_ms DESC, observation_id DESC);

CREATE FUNCTION assert_dreamina_supplement_observation() RETURNS TRIGGER AS $$
BEGIN
    PERFORM 1 FROM dreamina_operator_video_supplement_authorizations supplement
    WHERE supplement.authorization_id = NEW.authorization_id
      AND supplement.dispatch_nonce = NEW.dispatch_nonce
      AND supplement.remote_operation_id = NEW.remote_operation_id
      AND supplement.submit_stdout_sha256 IS NOT NULL
      AND supplement.state IN ('accepted', 'uncertain')
      AND NEW.observed_at_ms >= supplement.dispatched_at_ms
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'supplement observation does not match accepted dispatch';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dreamina_supplement_observation_bound
    BEFORE INSERT ON dreamina_operator_video_supplement_observations
    FOR EACH ROW EXECUTE FUNCTION assert_dreamina_supplement_observation();
CREATE TRIGGER dreamina_supplement_observation_immutable
    BEFORE UPDATE OR DELETE ON dreamina_operator_video_supplement_observations
    FOR EACH ROW EXECUTE FUNCTION reject_executor_evidence_mutation();
CREATE TRIGGER dreamina_supplement_observation_no_truncate
    BEFORE TRUNCATE ON dreamina_operator_video_supplement_observations
    FOR EACH STATEMENT EXECUTE FUNCTION reject_executor_evidence_mutation();
