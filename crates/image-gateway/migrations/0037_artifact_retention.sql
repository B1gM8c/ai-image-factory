LOCK TABLE job_response_projections IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE artifact_retention_policies (
    policy_key TEXT PRIMARY KEY CHECK (policy_key = 'default'),
    policy_version BIGINT NOT NULL CHECK (policy_version > 0),
    retain_for_ms BIGINT NOT NULL CHECK (retain_for_ms BETWEEN 1000 AND 31536000000),
    read_drain_ms BIGINT NOT NULL CHECK (read_drain_ms BETWEEN 1000 AND 86400000),
    retry_delay_ms BIGINT NOT NULL CHECK (retry_delay_ms BETWEEN 1000 AND 86400000),
    updated_at_ms BIGINT NOT NULL CHECK (updated_at_ms > 0)
);

INSERT INTO artifact_retention_policies
  (policy_key, policy_version, retain_for_ms, read_drain_ms, retry_delay_ms, updated_at_ms)
VALUES ('default', 1, 86400000, 900000, 60000,
        (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT);

CREATE TABLE job_artifact_retention (
    job_id UUID PRIMARY KEY REFERENCES job_response_projections(job_id) ON DELETE RESTRICT,
    policy_key TEXT NOT NULL REFERENCES artifact_retention_policies(policy_key) ON DELETE RESTRICT,
    policy_version BIGINT NOT NULL CHECK (policy_version > 0),
    retain_for_ms BIGINT NOT NULL CHECK (retain_for_ms BETWEEN 1000 AND 31536000000),
    read_drain_ms BIGINT NOT NULL CHECK (read_drain_ms BETWEEN 1000 AND 86400000),
    retry_delay_ms BIGINT NOT NULL CHECK (retry_delay_ms BETWEEN 1000 AND 86400000),
    state TEXT NOT NULL CHECK (state IN ('available', 'expired', 'deleting', 'deleted')),
    expires_at_ms BIGINT NOT NULL,
    expired_at_ms BIGINT,
    purge_after_ms BIGINT,
    lease_owner TEXT CHECK (
        lease_owner IS NULL OR (
            char_length(lease_owner) BETWEEN 1 AND 255
            AND lease_owner !~ '[[:cntrl:]]'
        )
    ),
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at_ms BIGINT,
    delete_attempts INTEGER NOT NULL DEFAULT 0 CHECK (delete_attempts >= 0),
    last_error_code TEXT CHECK (
        last_error_code IS NULL OR last_error_code ~ '^[A-Za-z0-9_.-]{1,128}$'
    ),
    deleted_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    CHECK (expires_at_ms >= created_at_ms),
    CHECK (
        (state = 'available'
            AND expired_at_ms IS NULL AND purge_after_ms IS NULL
            AND lease_owner IS NULL AND lease_expires_at_ms IS NULL
            AND deleted_at_ms IS NULL)
        OR
        (state = 'expired'
            AND expired_at_ms IS NOT NULL AND purge_after_ms IS NOT NULL
            AND lease_owner IS NULL AND lease_expires_at_ms IS NULL
            AND deleted_at_ms IS NULL)
        OR
        (state = 'deleting'
            AND expired_at_ms IS NOT NULL AND purge_after_ms IS NOT NULL
            AND lease_owner IS NOT NULL AND lease_epoch > 0
            AND lease_expires_at_ms IS NOT NULL AND deleted_at_ms IS NULL)
        OR
        (state = 'deleted'
            AND expired_at_ms IS NOT NULL AND purge_after_ms IS NOT NULL
            AND lease_owner IS NULL AND lease_expires_at_ms IS NULL
            AND deleted_at_ms IS NOT NULL)
    )
);

INSERT INTO job_artifact_retention
  (job_id, policy_key, policy_version, retain_for_ms, read_drain_ms, retry_delay_ms,
   state, expires_at_ms, created_at_ms, updated_at_ms)
SELECT projection.job_id, policy.policy_key, policy.policy_version,
       policy.retain_for_ms, policy.read_drain_ms, policy.retry_delay_ms, 'available',
       GREATEST(
           projection.created_at_ms + policy.retain_for_ms,
           (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT + policy.retain_for_ms
       ),
       projection.created_at_ms, projection.created_at_ms
FROM job_response_projections projection
CROSS JOIN artifact_retention_policies policy
WHERE policy.policy_key = 'default';

CREATE FUNCTION create_job_artifact_retention() RETURNS TRIGGER AS $$
DECLARE
    policy artifact_retention_policies%ROWTYPE;
BEGIN
    SELECT * INTO STRICT policy
    FROM artifact_retention_policies
    WHERE policy_key = 'default';
    INSERT INTO job_artifact_retention
      (job_id, policy_key, policy_version, retain_for_ms, read_drain_ms, retry_delay_ms,
       state, expires_at_ms, created_at_ms, updated_at_ms)
    VALUES (NEW.job_id, policy.policy_key, policy.policy_version,
            policy.retain_for_ms, policy.read_drain_ms, policy.retry_delay_ms, 'available',
            NEW.created_at_ms + policy.retain_for_ms,
            NEW.created_at_ms, NEW.created_at_ms);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_response_projections_create_retention
    AFTER INSERT ON job_response_projections
    FOR EACH ROW EXECUTE FUNCTION create_job_artifact_retention();

CREATE FUNCTION protect_artifact_retention_policy() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'artifact retention policies are durable';
    END IF;
    IF NEW.policy_key IS DISTINCT FROM OLD.policy_key
       OR NEW.policy_version <= OLD.policy_version
       OR NEW.updated_at_ms <= OLD.updated_at_ms THEN
        RAISE EXCEPTION 'artifact retention policy updates require a new version';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER artifact_retention_policies_protect
    BEFORE UPDATE OR DELETE ON artifact_retention_policies
    FOR EACH ROW EXECUTE FUNCTION protect_artifact_retention_policy();

CREATE FUNCTION reject_artifact_retention_policy_truncate() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'artifact retention policies are durable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER artifact_retention_policies_reject_truncate
    BEFORE TRUNCATE ON artifact_retention_policies
    FOR EACH STATEMENT EXECUTE FUNCTION reject_artifact_retention_policy_truncate();

CREATE FUNCTION protect_job_artifact_retention() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'artifact retention records are durable';
    END IF;
    IF NEW.job_id IS DISTINCT FROM OLD.job_id
       OR NEW.policy_key IS DISTINCT FROM OLD.policy_key
       OR NEW.policy_version IS DISTINCT FROM OLD.policy_version
       OR NEW.retain_for_ms IS DISTINCT FROM OLD.retain_for_ms
       OR NEW.read_drain_ms IS DISTINCT FROM OLD.read_drain_ms
       OR NEW.retry_delay_ms IS DISTINCT FROM OLD.retry_delay_ms
       OR NEW.expires_at_ms IS DISTINCT FROM OLD.expires_at_ms
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms
       OR NEW.updated_at_ms < OLD.updated_at_ms THEN
        RAISE EXCEPTION 'invalid artifact retention state transition';
    END IF;

    IF NEW IS NOT DISTINCT FROM OLD THEN
        RETURN NEW;
    END IF;

    IF OLD.state = 'available' AND NEW.state = 'expired' THEN
        IF NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR NEW.delete_attempts IS DISTINCT FROM OLD.delete_attempts
           OR NEW.lease_owner IS DISTINCT FROM OLD.lease_owner
           OR NEW.lease_expires_at_ms IS DISTINCT FROM OLD.lease_expires_at_ms
           OR NEW.deleted_at_ms IS DISTINCT FROM OLD.deleted_at_ms
           OR NEW.last_error_code IS NOT NULL THEN
            RAISE EXCEPTION 'invalid artifact retention expiry transition';
        END IF;
    ELSIF OLD.state = 'expired' AND NEW.state = 'expired' THEN
        IF NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR NEW.delete_attempts IS DISTINCT FROM OLD.delete_attempts
           OR NEW.expired_at_ms IS DISTINCT FROM OLD.expired_at_ms
           OR NEW.lease_owner IS DISTINCT FROM OLD.lease_owner
           OR NEW.lease_expires_at_ms IS DISTINCT FROM OLD.lease_expires_at_ms
           OR NEW.deleted_at_ms IS DISTINCT FROM OLD.deleted_at_ms
           OR NEW.last_error_code IS NULL THEN
            RAISE EXCEPTION 'invalid artifact retention deferral transition';
        END IF;
    ELSIF OLD.state = 'expired' AND NEW.state = 'deleting' THEN
        IF NEW.lease_epoch <> OLD.lease_epoch + 1
           OR NEW.delete_attempts <> OLD.delete_attempts + 1
           OR NEW.expired_at_ms IS DISTINCT FROM OLD.expired_at_ms
           OR NEW.purge_after_ms IS DISTINCT FROM OLD.purge_after_ms
           OR NEW.last_error_code IS DISTINCT FROM OLD.last_error_code
           OR NEW.deleted_at_ms IS DISTINCT FROM OLD.deleted_at_ms THEN
            RAISE EXCEPTION 'invalid artifact retention claim transition';
        END IF;
    ELSIF OLD.state = 'deleting' AND NEW.state = 'deleting' THEN
        IF NEW.lease_epoch <> OLD.lease_epoch + 1
           OR NEW.delete_attempts <> OLD.delete_attempts + 1
           OR NEW.expired_at_ms IS DISTINCT FROM OLD.expired_at_ms
           OR NEW.purge_after_ms IS DISTINCT FROM OLD.purge_after_ms
           OR NEW.last_error_code IS DISTINCT FROM OLD.last_error_code
           OR NEW.deleted_at_ms IS DISTINCT FROM OLD.deleted_at_ms THEN
            RAISE EXCEPTION 'invalid artifact retention reclaim transition';
        END IF;
    ELSIF OLD.state = 'deleting' AND NEW.state = 'expired' THEN
        IF NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR NEW.delete_attempts IS DISTINCT FROM OLD.delete_attempts
           OR NEW.expired_at_ms IS DISTINCT FROM OLD.expired_at_ms
           OR NEW.deleted_at_ms IS DISTINCT FROM OLD.deleted_at_ms
           OR NEW.last_error_code IS NULL THEN
            RAISE EXCEPTION 'invalid artifact retention retry transition';
        END IF;
    ELSIF OLD.state = 'deleting' AND NEW.state = 'deleted' THEN
        IF NEW.lease_epoch IS DISTINCT FROM OLD.lease_epoch
           OR NEW.delete_attempts IS DISTINCT FROM OLD.delete_attempts
           OR NEW.expired_at_ms IS DISTINCT FROM OLD.expired_at_ms
           OR NEW.purge_after_ms IS DISTINCT FROM OLD.purge_after_ms
           OR NEW.last_error_code IS NOT NULL THEN
            RAISE EXCEPTION 'invalid artifact retention completion transition';
        END IF;
    ELSE
        RAISE EXCEPTION 'invalid artifact retention state transition';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_artifact_retention_protect
    BEFORE UPDATE OR DELETE ON job_artifact_retention
    FOR EACH ROW EXECUTE FUNCTION protect_job_artifact_retention();

CREATE FUNCTION reject_job_artifact_retention_truncate() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'artifact retention records are durable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_artifact_retention_reject_truncate
    BEFORE TRUNCATE ON job_artifact_retention
    FOR EACH STATEMENT EXECUTE FUNCTION reject_job_artifact_retention_truncate();

CREATE INDEX job_artifact_retention_expire_idx
    ON job_artifact_retention (expires_at_ms, job_id)
    WHERE state = 'available';

CREATE INDEX job_artifact_retention_purge_idx
    ON job_artifact_retention (purge_after_ms, job_id)
    WHERE state = 'expired';

CREATE INDEX job_artifact_retention_reclaim_idx
    ON job_artifact_retention (lease_expires_at_ms, job_id)
    WHERE state = 'deleting';

CREATE INDEX job_artifact_retention_failure_idx
    ON job_artifact_retention (job_id)
    WHERE last_error_code IS NOT NULL;
