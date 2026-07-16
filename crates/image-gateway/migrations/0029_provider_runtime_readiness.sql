CREATE TABLE provider_runtime_leases (
    runtime_id UUID PRIMARY KEY,
    execution_profile_id UUID NOT NULL,
    runtime_role TEXT NOT NULL CHECK (runtime_role IN ('submit', 'poll')),
    runtime_owner TEXT NOT NULL CHECK (
        char_length(runtime_owner) BETWEEN 1 AND 255
        AND runtime_owner !~ '[^!-~]'
    ),
    state TEXT NOT NULL CHECK (state IN ('active', 'draining')),
    heartbeat_at_ms BIGINT NOT NULL,
    lease_expires_at_ms BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    CHECK (
        heartbeat_at_ms >= created_at_ms
        AND updated_at_ms >= heartbeat_at_ms
        AND lease_expires_at_ms > heartbeat_at_ms
    ),
    FOREIGN KEY (execution_profile_id)
        REFERENCES provider_execution_profiles(execution_profile_id)
        ON DELETE RESTRICT,
    UNIQUE (execution_profile_id, runtime_role, runtime_owner)
);

-- Heartbeats update only heap columns. The lookup index remains unchanged on
-- every renewal and cardinality is bounded by live or not-yet-reaped runtimes.
CREATE INDEX provider_runtime_leases_profile_role_idx
    ON provider_runtime_leases (execution_profile_id, runtime_role);

CREATE FUNCTION enforce_provider_runtime_lease_transition() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'active'
           OR NEW.heartbeat_at_ms <> NEW.created_at_ms
           OR NEW.updated_at_ms <> NEW.created_at_ms
           OR NEW.heartbeat_at_ms > now_ms THEN
            RAISE EXCEPTION 'provider runtime lease must be inserted active';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        IF OLD.state <> 'draining'
           AND OLD.lease_expires_at_ms >
               floor(extract(epoch FROM statement_timestamp()) * 1000)::BIGINT THEN
            RAISE EXCEPTION 'live provider runtime lease must drain before deletion';
        END IF;
        RETURN OLD;
    END IF;
    IF NEW.runtime_id IS DISTINCT FROM OLD.runtime_id
       OR NEW.execution_profile_id IS DISTINCT FROM OLD.execution_profile_id
       OR NEW.runtime_role IS DISTINCT FROM OLD.runtime_role
       OR NEW.runtime_owner IS DISTINCT FROM OLD.runtime_owner
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms THEN
        RAISE EXCEPTION 'provider runtime lease identity is immutable';
    END IF;
    IF OLD.state = 'draining' AND NEW.state <> 'draining' THEN
        RAISE EXCEPTION 'draining provider runtime lease cannot reactivate';
    END IF;
    IF NEW.heartbeat_at_ms < OLD.heartbeat_at_ms
       OR NEW.updated_at_ms < OLD.updated_at_ms
       OR NEW.lease_expires_at_ms <= NEW.heartbeat_at_ms
       OR NEW.heartbeat_at_ms > now_ms THEN
        RAISE EXCEPTION 'provider runtime lease time is invalid';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_runtime_leases_transition
    BEFORE INSERT OR UPDATE OR DELETE ON provider_runtime_leases
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_runtime_lease_transition();
