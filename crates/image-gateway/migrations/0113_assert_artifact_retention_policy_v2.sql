DO $$
BEGIN
    PERFORM 1
    FROM artifact_retention_policies
    WHERE policy_key = 'default'
      AND policy_version = 2
      AND retain_for_ms = 1800000
      AND read_drain_ms = 60000
      AND retry_delay_ms = 60000;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'artifact retention policy must be v2 with 30 minute retention and 1 minute drain/retry';
    END IF;
END
$$;
