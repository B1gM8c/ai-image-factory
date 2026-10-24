-- A failed query can record waiting without progress. Read the last successful
-- progress report without changing its immutable observation time or task head.
CREATE INDEX provider_task_observations_progress_projection_idx
    ON provider_task_observations (
        submission_id, executor_execution_id, observed_at_ms DESC,
        poll_lease_epoch DESC, observation_id DESC
    )
    WHERE progress_json IS NOT NULL
      AND source = 'poll'
      AND observed_state = 'provider_waiting';
