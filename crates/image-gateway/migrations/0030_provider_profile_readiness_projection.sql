CREATE VIEW provider_profile_readiness AS
WITH db_clock AS MATERIALIZED (
    SELECT floor(extract(epoch FROM statement_timestamp()) * 1000)::BIGINT AS now_ms
),
profile_lanes AS (
    SELECT profile.execution_profile_id,
           profile.profile_key,
           profile.provider_id,
           (
               profile.state = 'enabled'
               AND pool.state = 'enabled'
               AND account.state = 'enabled'
               AND policy.state = 'enabled'
               AND account.credential_ref = profile.credential_ref
               AND account.credential_revision = profile.credential_revision
               AND policy.credential_pool_id = profile.credential_pool_id
               AND policy.provider_account_id = profile.provider_account_id
               AND policy.provider_id = profile.provider_id
               AND policy.max_concurrency BETWEEN 1 AND 1024
           ) AS runnable,
           COUNT(*) FILTER (
               WHERE runtime.runtime_role = 'submit'
                 AND runtime.state = 'active'
                 AND runtime.lease_expires_at_ms > db_clock.now_ms
           )::BIGINT AS active_submitters,
           COUNT(*) FILTER (
               WHERE runtime.runtime_role = 'poll'
                 AND runtime.state = 'active'
                 AND runtime.lease_expires_at_ms > db_clock.now_ms
           )::BIGINT AS active_pollers,
           COUNT(*) FILTER (
               WHERE runtime.runtime_role = 'submit'
                 AND runtime.state = 'draining'
                 AND runtime.lease_expires_at_ms > db_clock.now_ms
           )::BIGINT AS draining_submitters,
           COUNT(*) FILTER (
               WHERE runtime.runtime_role = 'poll'
                 AND runtime.state = 'draining'
                 AND runtime.lease_expires_at_ms > db_clock.now_ms
           )::BIGINT AS draining_pollers
    FROM provider_execution_profiles profile
    JOIN provider_credential_pools pool
      ON pool.credential_pool_id = profile.credential_pool_id
     AND pool.provider_id = profile.provider_id
    JOIN provider_accounts account
      ON account.provider_account_id = profile.provider_account_id
     AND account.credential_pool_id = profile.credential_pool_id
     AND account.provider_id = profile.provider_id
    JOIN executor_resource_policies policy
      ON policy.resource_policy_id = profile.resource_policy_id
     AND policy.revision = profile.resource_policy_revision
    CROSS JOIN db_clock
    LEFT JOIN provider_runtime_leases runtime
      ON runtime.execution_profile_id = profile.execution_profile_id
    WHERE profile.completion_mode = 'remote_task'
    GROUP BY profile.execution_profile_id, profile.profile_key,
             profile.provider_id, profile.state, profile.credential_ref,
             profile.credential_revision, profile.credential_pool_id,
             profile.provider_account_id, pool.state, account.state,
             account.credential_ref, account.credential_revision,
             policy.state, policy.credential_pool_id,
             policy.provider_account_id, policy.provider_id,
             policy.max_concurrency
)
SELECT execution_profile_id,
       profile_key,
       provider_id,
       CASE
           WHEN NOT runnable THEN 'blocked'
           WHEN active_submitters > 0 AND active_pollers > 0 THEN 'active'
           WHEN draining_submitters > 0 OR draining_pollers > 0 THEN 'draining'
           ELSE 'configured'
       END AS status,
       active_submitters,
       active_pollers,
       draining_submitters,
       draining_pollers
FROM profile_lanes;
