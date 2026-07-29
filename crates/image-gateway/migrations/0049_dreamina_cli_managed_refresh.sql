DROP INDEX provider_account_credential_heads_due_idx;

CREATE INDEX provider_account_credential_heads_due_idx
    ON provider_account_credential_heads (next_refresh_at_ms, provider_account_id)
    WHERE refresh_strategy IN ('broker_managed', 'cli_managed')
      AND lifecycle_state IN ('active', 'refresh_due');

UPDATE provider_account_credential_heads head
SET next_refresh_at_ms = LEAST(
        COALESCE(head.next_refresh_at_ms,
                 floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT),
        floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
    ),
    updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT,
    control_version = control_version + 1
FROM provider_accounts account
WHERE account.provider_account_id = head.provider_account_id
  AND account.provider_id = 'dreamina-cli'
  AND head.refresh_strategy = 'cli_managed'
  AND head.lifecycle_state IN ('active', 'refresh_due');
