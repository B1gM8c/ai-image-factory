CREATE TABLE billing_integrity_runs (
    run_id UUID PRIMARY KEY,
    check_version SMALLINT NOT NULL CHECK (check_version > 0),
    scanner_version TEXT NOT NULL CHECK (
        char_length(scanner_version) BETWEEN 1 AND 64
    ),
    check_set TEXT[] NOT NULL CHECK (cardinality(check_set) > 0),
    scope_type TEXT NOT NULL CHECK (scope_type IN ('platform', 'organization')),
    scope_id TEXT,
    state TEXT NOT NULL CHECK (state = 'completed'),
    actor_kind TEXT NOT NULL CHECK (actor_kind IN ('manual', 'scheduled')),
    initiated_by_user_id UUID
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    session_id UUID,
    as_of_ms BIGINT NOT NULL,
    started_at_ms BIGINT NOT NULL,
    completed_at_ms BIGINT NOT NULL,
    critical_count INTEGER NOT NULL CHECK (critical_count >= 0),
    warning_count INTEGER NOT NULL CHECK (warning_count >= 0),
    finding_count INTEGER NOT NULL CHECK (
        finding_count = critical_count + warning_count
    ),
    summary JSONB NOT NULL CHECK (jsonb_typeof(summary) = 'object'),
    CHECK (completed_at_ms >= started_at_ms),
    CHECK (
        (scope_type = 'platform' AND scope_id IS NULL)
        OR (scope_type = 'organization' AND scope_id IS NOT NULL)
    ),
    CHECK (
        (actor_kind = 'manual'
            AND initiated_by_user_id IS NOT NULL
            AND session_id IS NOT NULL)
        OR (actor_kind = 'scheduled'
            AND initiated_by_user_id IS NULL
            AND session_id IS NULL)
    )
);

CREATE INDEX billing_integrity_runs_completed_idx
    ON billing_integrity_runs(completed_at_ms DESC, run_id DESC);

CREATE TABLE billing_integrity_findings (
    finding_id UUID PRIMARY KEY,
    run_id UUID NOT NULL
        REFERENCES billing_integrity_runs(run_id) ON DELETE RESTRICT,
    finding_key TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('critical', 'warning')),
    category TEXT NOT NULL CHECK (
        category IN (
            'account_balance', 'hold_lifecycle', 'customer_charge',
            'attribution', 'provider_cost', 'allocation'
        )
    ),
    finding_code TEXT NOT NULL,
    tenant_id TEXT,
    currency TEXT CHECK (
        currency IS NULL OR currency ~ '^[A-Z]{3}$'
    ),
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    expected JSONB NOT NULL CHECK (jsonb_typeof(expected) = 'object'),
    actual JSONB NOT NULL CHECK (jsonb_typeof(actual) = 'object'),
    details JSONB NOT NULL CHECK (jsonb_typeof(details) = 'object'),
    detected_at_ms BIGINT NOT NULL,
    UNIQUE (run_id, finding_key)
);

CREATE INDEX billing_integrity_findings_run_severity_idx
    ON billing_integrity_findings(
        run_id, severity, category, finding_code, finding_key
    );

CREATE FUNCTION preserve_billing_integrity_evidence()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'billing integrity evidence is immutable'
        USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER preserve_billing_integrity_run
BEFORE UPDATE OR DELETE ON billing_integrity_runs
FOR EACH ROW EXECUTE FUNCTION preserve_billing_integrity_evidence();

CREATE TRIGGER preserve_billing_integrity_finding
BEFORE UPDATE OR DELETE ON billing_integrity_findings
FOR EACH ROW EXECUTE FUNCTION preserve_billing_integrity_evidence();

COMMENT ON TABLE billing_integrity_runs IS
    'Immutable point-in-time billing integrity scan summaries.';

COMMENT ON TABLE billing_integrity_findings IS
    'Immutable evidence-only findings; scanners never repair financial facts.';
