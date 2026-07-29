UPDATE artifact_retention_policies
SET policy_version = policy_version + 1,
    retain_for_ms = 1800000,
    read_drain_ms = 60000,
    retry_delay_ms = 60000,
    updated_at_ms = GREATEST(
        updated_at_ms + 1,
        (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
    )
WHERE policy_key = 'default'
  AND (
      retain_for_ms IS DISTINCT FROM 1800000
      OR read_drain_ms IS DISTINCT FROM 60000
      OR retry_delay_ms IS DISTINCT FROM 60000
  );
