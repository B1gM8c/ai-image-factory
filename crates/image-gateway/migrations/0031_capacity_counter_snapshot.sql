CREATE OR REPLACE FUNCTION enforce_executor_capacity_counter_balance()
RETURNS TRIGGER AS $$
DECLARE
    policy_id UUID;
    policy_revision BIGINT;
    stored_count INTEGER;
    held_count BIGINT;
BEGIN
    IF TG_TABLE_NAME = 'executor_capacity_allocations'
       AND TG_OP = 'UPDATE'
       AND OLD.state = 'held'
       AND NEW.state = 'held' THEN
        RETURN NULL;
    END IF;

    IF TG_TABLE_NAME = 'executor_resource_policies' THEN
        policy_id := NEW.resource_policy_id;
        policy_revision := NEW.revision;
    ELSE
        policy_id := NEW.resource_policy_id;
        policy_revision := NEW.resource_policy_revision;
    END IF;

    SELECT policy.allocated_count,
           COUNT(allocation.allocation_id) FILTER (
               WHERE allocation.state = 'held'
           )
    INTO stored_count, held_count
    FROM executor_resource_policies policy
    LEFT JOIN executor_capacity_allocations allocation
      ON allocation.resource_policy_id = policy.resource_policy_id
     AND allocation.resource_policy_revision = policy.revision
    WHERE policy.resource_policy_id = policy_id
      AND policy.revision = policy_revision
    GROUP BY policy.allocated_count;

    IF stored_count IS NULL OR stored_count::BIGINT <> held_count THEN
        RAISE EXCEPTION 'executor capacity allocation counter is unbalanced';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
