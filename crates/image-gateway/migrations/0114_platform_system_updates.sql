CREATE TABLE platform_release_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
    repository TEXT,
    target_triple TEXT NOT NULL,
    current_version TEXT NOT NULL,
    current_commit_sha TEXT,
    previous_version TEXT,
    previous_commit_sha TEXT,
    latest_version TEXT,
    latest_commit_sha TEXT,
    latest_verified BOOLEAN NOT NULL DEFAULT FALSE,
    last_checked_at_ms BIGINT,
    last_applied_at_ms BIGINT,
    last_error_code TEXT,
    last_error_message TEXT,
    updated_at_ms BIGINT NOT NULL,
    CONSTRAINT platform_release_state_singleton_check CHECK (singleton),
    CONSTRAINT platform_release_state_repository_check CHECK (
        repository IS NULL OR char_length(repository) BETWEEN 3 AND 200
    ),
    CONSTRAINT platform_release_state_target_check CHECK (
        char_length(target_triple) BETWEEN 3 AND 100
    ),
    CONSTRAINT platform_release_state_current_version_check CHECK (
        char_length(current_version) BETWEEN 1 AND 100
    ),
    CONSTRAINT platform_release_state_latest_version_check CHECK (
        latest_version IS NULL OR char_length(latest_version) BETWEEN 1 AND 100
    )
);

CREATE TABLE platform_update_commands (
    command_id UUID PRIMARY KEY,
    action TEXT NOT NULL,
    target_version TEXT,
    status TEXT NOT NULL,
    phase TEXT NOT NULL DEFAULT 'queued',
    idempotency_key_digest TEXT NOT NULL UNIQUE,
    request_digest TEXT NOT NULL,
    requested_by_user_id UUID NOT NULL,
    requested_by_session_id UUID NOT NULL,
    lease_owner TEXT,
    lease_epoch BIGINT NOT NULL DEFAULT 0,
    lease_expires_at_ms BIGINT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    progress JSONB NOT NULL DEFAULT '{}'::jsonb,
    failure_code TEXT,
    failure_message TEXT,
    requested_at_ms BIGINT NOT NULL,
    started_at_ms BIGINT,
    completed_at_ms BIGINT,
    updated_at_ms BIGINT NOT NULL,
    CONSTRAINT platform_update_commands_action_check CHECK (
        action IN ('check', 'apply')
    ),
    CONSTRAINT platform_update_commands_target_check CHECK (
        (action = 'check' AND target_version IS NULL)
        OR
        (action = 'apply' AND target_version IS NOT NULL
            AND char_length(target_version) BETWEEN 1 AND 100)
    ),
    CONSTRAINT platform_update_commands_status_check CHECK (
        status IN (
            'queued', 'running', 'succeeded', 'failed',
            'restoring', 'restored', 'restore_required'
        )
    ),
    CONSTRAINT platform_update_commands_phase_check CHECK (
        phase IN (
            'queued', 'preflight', 'staged', 'quiescing', 'quiesced',
            'recovery_ready', 'migrated', 'switched',
            'verified', 'restoring', 'restored', 'failed'
        )
    ),
    CONSTRAINT platform_update_commands_idempotency_digest_check CHECK (
        char_length(idempotency_key_digest) = 64
    ),
    CONSTRAINT platform_update_commands_request_digest_check CHECK (
        char_length(request_digest) = 64
    ),
    CONSTRAINT platform_update_commands_progress_check CHECK (
        jsonb_typeof(progress) = 'object'
    ),
    CONSTRAINT platform_update_commands_lease_check CHECK (
        (lease_owner IS NULL AND lease_expires_at_ms IS NULL)
        OR
        (lease_owner IS NOT NULL AND lease_expires_at_ms IS NOT NULL
            AND char_length(lease_owner) BETWEEN 1 AND 200)
    )
);

CREATE UNIQUE INDEX platform_update_commands_active_uidx
    ON platform_update_commands ((TRUE))
    WHERE status IN ('queued', 'running', 'restoring', 'restore_required');

CREATE INDEX platform_update_commands_requested_idx
    ON platform_update_commands (requested_at_ms DESC, command_id DESC);

CREATE INDEX platform_update_commands_claim_idx
    ON platform_update_commands (status, lease_expires_at_ms, requested_at_ms)
    WHERE status IN ('queued', 'running');

CREATE TABLE platform_update_events (
    event_id UUID PRIMARY KEY,
    command_id UUID NOT NULL
        REFERENCES platform_update_commands(command_id) ON DELETE CASCADE,
    phase TEXT NOT NULL,
    outcome TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at_ms BIGINT NOT NULL,
    CONSTRAINT platform_update_events_phase_check CHECK (
        char_length(phase) BETWEEN 1 AND 64
    ),
    CONSTRAINT platform_update_events_outcome_check CHECK (
        outcome IN ('started', 'succeeded', 'failed', 'info')
    ),
    CONSTRAINT platform_update_events_details_check CHECK (
        jsonb_typeof(details) = 'object'
    )
);

CREATE INDEX platform_update_events_command_created_idx
    ON platform_update_events (command_id, created_at_ms, event_id);
