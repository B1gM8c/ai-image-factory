ALTER TABLE provider_submit_recoveries
    ADD CONSTRAINT provider_submit_recoveries_lease_deadline_check CHECK (
        recovery_lease_expires_at_ms IS NULL
        OR recovery_lease_expires_at_ms <= provider_deadline_at_ms
    );

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM executor_capacity_allocations
        WHERE last_heartbeat_at_ms >
              floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
    ) THEN
        RAISE EXCEPTION
            'provider capacity heartbeat migration requires non-future heartbeats';
    END IF;
END;
$$;

CREATE FUNCTION enforce_executor_capacity_heartbeat_time() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF TG_OP = 'INSERT' AND NEW.last_heartbeat_at_ms > now_ms THEN
        RAISE EXCEPTION 'executor capacity heartbeat cannot be in the future';
    ELSIF TG_OP = 'UPDATE'
          AND NEW.last_heartbeat_at_ms IS DISTINCT FROM OLD.last_heartbeat_at_ms
          AND NEW.last_heartbeat_at_ms > now_ms THEN
        RAISE EXCEPTION 'executor capacity heartbeat cannot be in the future';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_capacity_allocations_heartbeat_time_guard
    BEFORE INSERT OR UPDATE ON executor_capacity_allocations
    FOR EACH ROW EXECUTE FUNCTION enforce_executor_capacity_heartbeat_time();

CREATE FUNCTION enforce_provider_recovery_attach_deadline() RETURNS TRIGGER AS $$
DECLARE
    now_ms BIGINT := floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
    IF NEW.attach_recovery_owner IS NOT NULL
       AND NOT EXISTS (
            SELECT 1
            FROM provider_submit_recoveries recovery
            WHERE recovery.submission_id = NEW.submission_id
              AND recovery.executor_execution_id = NEW.executor_execution_id
              AND recovery.provider_deadline_at_ms > now_ms
       ) THEN
        RAISE EXCEPTION 'recovered attach requires a future provider deadline';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_remote_task_recovery_deadline_guard
    BEFORE INSERT ON provider_remote_tasks
    FOR EACH ROW EXECUTE FUNCTION enforce_provider_recovery_attach_deadline();
