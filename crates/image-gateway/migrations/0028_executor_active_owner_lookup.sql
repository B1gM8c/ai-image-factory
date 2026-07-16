SET LOCAL lock_timeout = '5s';

CREATE INDEX executor_executions_active_owner_idx
    ON executor_executions (executor_owner)
    WHERE executor_owner IS NOT NULL
      AND state IN ('leased', 'running');
