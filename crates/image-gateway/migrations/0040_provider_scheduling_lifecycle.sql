-- Mutable account-level scheduling controls stay outside immutable execution
-- policy revisions. The policy remains a hard safety ceiling.
CREATE TABLE provider_account_execution_controls (
    provider_account_id UUID PRIMARY KEY
        REFERENCES provider_accounts(provider_account_id) ON DELETE RESTRICT,
    desired_max_concurrency INTEGER NOT NULL CHECK (
        desired_max_concurrency BETWEEN 1 AND 1000000
    ),
    lifecycle_state TEXT NOT NULL CHECK (
        lifecycle_state IN ('active', 'draining', 'disabled')
    ),
    control_version BIGINT NOT NULL CHECK (control_version > 0),
    drain_started_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);

CREATE TABLE provider_account_execution_control_events (
    event_id UUID PRIMARY KEY,
    provider_account_id UUID NOT NULL
        REFERENCES provider_account_execution_controls(provider_account_id)
        ON DELETE RESTRICT,
    previous_control_version BIGINT NOT NULL,
    control_version BIGINT NOT NULL,
    previous_max_concurrency INTEGER NOT NULL,
    max_concurrency INTEGER NOT NULL,
    previous_lifecycle_state TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    UNIQUE (provider_account_id, control_version)
);

CREATE FUNCTION reject_provider_account_control_event_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'provider account control events are immutable'
        USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_account_execution_control_events_immutable
BEFORE UPDATE OR DELETE ON provider_account_execution_control_events
FOR EACH ROW EXECUTE FUNCTION reject_provider_account_control_event_mutation();

INSERT INTO provider_account_execution_controls
  (provider_account_id, desired_max_concurrency, lifecycle_state,
   control_version, created_at_ms, updated_at_ms)
SELECT DISTINCT ON (policy.provider_account_id)
       policy.provider_account_id, policy.max_concurrency,
       CASE WHEN policy.state = 'enabled' THEN 'active' ELSE 'disabled' END,
       1, policy.created_at_ms, policy.created_at_ms
FROM executor_resource_policies policy
ORDER BY policy.provider_account_id, (policy.state = 'enabled') DESC,
         policy.revision DESC;

CREATE FUNCTION initialize_provider_account_execution_control() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO provider_account_execution_controls
      (provider_account_id, desired_max_concurrency, lifecycle_state,
       control_version, created_at_ms, updated_at_ms)
    VALUES (
      NEW.provider_account_id, NEW.max_concurrency,
      CASE WHEN NEW.state = 'enabled' THEN 'active' ELSE 'disabled' END,
      1, NEW.created_at_ms, NEW.created_at_ms
    )
    ON CONFLICT (provider_account_id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER executor_resource_policy_initialize_account_control
AFTER INSERT ON executor_resource_policies
FOR EACH ROW EXECUTE FUNCTION initialize_provider_account_execution_control();

-- Managed Codex accounts reserve a stable hard ceiling while retaining their
-- existing configured value in desired_max_concurrency.
ALTER TABLE executor_resource_policies
    DISABLE TRIGGER executor_resource_policies_allocation_counter;
UPDATE executor_resource_policies policy
SET max_concurrency = 64
FROM provider_account_environments environment
WHERE environment.provider_account_id = policy.provider_account_id
  AND environment.provider_id = 'openai-codex'
  AND policy.max_concurrency < 64;
ALTER TABLE executor_resource_policies
    ENABLE TRIGGER executor_resource_policies_allocation_counter;

-- A mutable head points to the current immutable route revision. New jobs read
-- the head once; queued jobs retain the exact revision they already attributed.
DO $$
BEGIN
    IF EXISTS (
        SELECT route_id
        FROM provider_routes
        GROUP BY route_id
        HAVING COUNT(DISTINCT ROW(
            route_key, provider_id, operation_id, command_schema, route_kind
        )) > 1
    ) THEN
        RAISE EXCEPTION 'provider route identity drift prevents head migration';
    END IF;
    IF EXISTS (
        SELECT route_key
        FROM provider_routes
        GROUP BY route_key
        HAVING COUNT(DISTINCT route_id) > 1
    ) THEN
        RAISE EXCEPTION 'duplicate provider route keys prevent head migration';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE provider_route_heads (
    route_id UUID PRIMARY KEY,
    route_key TEXT NOT NULL UNIQUE CHECK (
        char_length(route_key) BETWEEN 1 AND 128
        AND route_key ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    command_schema TEXT NOT NULL,
    route_kind TEXT NOT NULL CHECK (route_kind IN ('account', 'group')),
    current_revision BIGINT NOT NULL CHECK (current_revision > 0),
    state TEXT NOT NULL CHECK (state IN ('enabled', 'disabled')),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (
        route_id, current_revision, provider_id, operation_id, command_schema
    ) REFERENCES provider_routes (
        route_id, revision, provider_id, operation_id, command_schema
    ) ON DELETE RESTRICT
);

INSERT INTO provider_route_heads
  (route_id, route_key, provider_id, operation_id, command_schema, route_kind,
   current_revision, state, created_at_ms, updated_at_ms)
SELECT DISTINCT ON (route_id)
       route_id, route_key, provider_id, operation_id, command_schema, route_kind,
       revision, state, created_at_ms, created_at_ms
FROM provider_routes
ORDER BY route_id, revision DESC;

DROP INDEX provider_routes_enabled_key_uidx;

ALTER TABLE gateway_api_key_provider_routes
    ADD CONSTRAINT gateway_api_key_provider_route_head_fk
    FOREIGN KEY (route_id) REFERENCES provider_route_heads(route_id) ON DELETE RESTRICT;

CREATE FUNCTION enforce_provider_route_head_identity() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'provider route heads cannot be deleted'
            USING ERRCODE = '55000';
    END IF;
    IF to_jsonb(NEW) - 'current_revision' - 'state' - 'updated_at_ms'
       IS DISTINCT FROM to_jsonb(OLD) - 'current_revision' - 'state' - 'updated_at_ms'
       OR NEW.current_revision < OLD.current_revision
       OR NEW.current_revision > OLD.current_revision + 1
       OR NEW.updated_at_ms < OLD.updated_at_ms THEN
        RAISE EXCEPTION 'provider route head identity is immutable'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_route_heads_identity
BEFORE UPDATE OR DELETE ON provider_route_heads
FOR EACH ROW EXECUTE FUNCTION enforce_provider_route_head_identity();

CREATE FUNCTION enforce_provider_route_revision_identity() RETURNS TRIGGER AS $$
DECLARE
    head provider_route_heads%ROWTYPE;
BEGIN
    SELECT * INTO head FROM provider_route_heads WHERE route_id = NEW.route_id;
    IF FOUND AND (
        NEW.route_key <> head.route_key
        OR NEW.provider_id <> head.provider_id
        OR NEW.operation_id <> head.operation_id
        OR NEW.command_schema <> head.command_schema
        OR NEW.route_kind <> head.route_kind
    ) THEN
        RAISE EXCEPTION 'provider route revision identity does not match its head'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_route_revision_identity
BEFORE INSERT ON provider_routes
FOR EACH ROW EXECUTE FUNCTION enforce_provider_route_revision_identity();

CREATE FUNCTION reject_provider_route_revision_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'provider route revisions are immutable'
        USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_routes_revision_immutable
BEFORE UPDATE OR DELETE ON provider_routes
FOR EACH ROW EXECUTE FUNCTION reject_provider_route_revision_mutation();

CREATE FUNCTION reject_provider_route_member_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'provider route revision members are immutable'
        USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_route_members_immutable
BEFORE UPDATE OR DELETE ON provider_route_members
FOR EACH ROW EXECUTE FUNCTION reject_provider_route_member_mutation();

COMMENT ON TABLE provider_account_execution_controls IS
    'Versioned live capacity and drain controls shared by every profile of an account.';
