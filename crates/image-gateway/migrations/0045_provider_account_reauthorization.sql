ALTER TABLE provider_account_credential_events
    DROP CONSTRAINT provider_account_credential_events_event_type_check;

ALTER TABLE provider_account_credential_events
    ADD CONSTRAINT provider_account_credential_events_event_type_check CHECK (
        event_type IN (
            'refresh_claimed', 'refresh_succeeded', 'refresh_failed',
            'reauth_required', 'reauth_succeeded', 'credential_resolved'
        )
    );

CREATE UNIQUE INDEX provider_account_login_sessions_active_reauth_idx
    ON provider_account_login_sessions (provider_account_id)
    WHERE provider_account_id IS NOT NULL
      AND status IN ('starting', 'waiting_for_user', 'validating');
