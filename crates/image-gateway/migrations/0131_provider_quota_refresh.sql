-- Quota observation leases are independent of credential refresh leases. A
-- monotonically increasing epoch fences snapshot publication after cancellation.
CREATE TABLE provider_account_quota_refreshes (
    provider_account_id UUID PRIMARY KEY REFERENCES provider_accounts(provider_account_id),
    next_attempt_at_ms BIGINT NOT NULL DEFAULT 0,
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    lease_token UUID,
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at_ms BIGINT,
    last_attempt_at_ms BIGINT,
    last_completed_at_ms BIGINT,
    last_error_code TEXT,
    CHECK ((lease_token IS NULL AND lease_expires_at_ms IS NULL)
        OR (lease_token IS NOT NULL AND lease_expires_at_ms IS NOT NULL AND lease_epoch > 0))
);
CREATE INDEX provider_account_quota_refreshes_due_idx
    ON provider_account_quota_refreshes(next_attempt_at_ms, provider_account_id);
