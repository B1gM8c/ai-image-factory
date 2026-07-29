ALTER TABLE gateway_projects
    ADD COLUMN service_tier TEXT NOT NULL DEFAULT 'default';

ALTER TABLE gateway_projects
    ADD CONSTRAINT gateway_projects_service_tier_check
    CHECK (service_tier IN ('default', 'priority'));

COMMENT ON COLUMN gateway_projects.service_tier IS
    'Default processing tier for requests that omit service_tier or use auto.';

CREATE TABLE job_service_tier_decisions (
    job_id UUID PRIMARY KEY REFERENCES jobs(job_id) ON DELETE RESTRICT,
    requested_service_tier TEXT NOT NULL,
    project_service_tier TEXT NOT NULL,
    effective_service_tier TEXT NOT NULL,
    fallback_reason TEXT,
    created_at_ms BIGINT NOT NULL,
    CONSTRAINT job_service_tier_requested_check
        CHECK (requested_service_tier IN ('auto', 'default', 'flex', 'priority')),
    CONSTRAINT job_service_tier_project_check
        CHECK (project_service_tier IN ('default', 'priority')),
    CONSTRAINT job_service_tier_effective_check
        CHECK (effective_service_tier IN ('default', 'flex', 'priority')),
    CONSTRAINT job_service_tier_fallback_reason_check
        CHECK (
            fallback_reason IS NULL
            OR char_length(fallback_reason) BETWEEN 1 AND 128
        )
);

CREATE INDEX job_service_tier_decisions_effective_created_idx
    ON job_service_tier_decisions (effective_service_tier, created_at_ms DESC, job_id);

CREATE INDEX job_service_tier_decisions_project_created_idx
    ON job_service_tier_decisions (project_service_tier, created_at_ms DESC, job_id);

COMMENT ON TABLE job_service_tier_decisions IS
    'Immutable requested, project-default, and actually served processing tier captured at admission.';

CREATE FUNCTION reject_job_service_tier_decision_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'job service tier decisions are immutable';
END;
$$;

CREATE TRIGGER job_service_tier_decisions_immutable
BEFORE UPDATE OR DELETE ON job_service_tier_decisions
FOR EACH ROW
EXECUTE FUNCTION reject_job_service_tier_decision_mutation();
