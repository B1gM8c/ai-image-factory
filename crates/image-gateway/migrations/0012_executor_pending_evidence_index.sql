CREATE INDEX executor_executions_pending_evidence_idx
    ON executor_executions (
        launch_owner,
        state,
        lease_expires_at_ms,
        updated_at_ms,
        executor_execution_id
    )
    WHERE launch_owner IS NOT NULL
      AND state IN ('running', 'succeeded', 'failed', 'uncertain');
