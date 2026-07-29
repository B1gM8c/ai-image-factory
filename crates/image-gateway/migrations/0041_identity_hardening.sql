ALTER TABLE identity_refresh_tokens
    ADD CONSTRAINT identity_refresh_tokens_session_token_unique
    UNIQUE (session_id, token_id);

ALTER TABLE identity_refresh_tokens
    DROP CONSTRAINT identity_refresh_tokens_parent_token_id_fkey,
    DROP CONSTRAINT identity_refresh_tokens_replaced_by_token_id_fkey;

ALTER TABLE identity_refresh_tokens
    ADD CONSTRAINT identity_refresh_tokens_parent_same_session_fkey
        FOREIGN KEY (session_id, parent_token_id)
        REFERENCES identity_refresh_tokens (session_id, token_id),
    ADD CONSTRAINT identity_refresh_tokens_replacement_same_session_fkey
        FOREIGN KEY (session_id, replaced_by_token_id)
        REFERENCES identity_refresh_tokens (session_id, token_id);

CREATE UNIQUE INDEX identity_refresh_tokens_parent_unique_idx
    ON identity_refresh_tokens (parent_token_id)
    WHERE parent_token_id IS NOT NULL;

CREATE UNIQUE INDEX identity_refresh_tokens_replacement_unique_idx
    ON identity_refresh_tokens (replaced_by_token_id)
    WHERE replaced_by_token_id IS NOT NULL;

CREATE INDEX identity_session_families_absolute_expiry_idx
    ON identity_session_families (absolute_expires_at_ms, session_id);

CREATE INDEX identity_session_families_revoked_idx
    ON identity_session_families (revoked_at_ms, session_id)
    WHERE revoked_at_ms IS NOT NULL;

CREATE INDEX identity_login_throttles_gc_idx
    ON identity_login_throttles (updated_at_ms, throttle_key);

CREATE INDEX identity_audit_events_created_idx
    ON identity_audit_events (created_at_ms, event_id);
