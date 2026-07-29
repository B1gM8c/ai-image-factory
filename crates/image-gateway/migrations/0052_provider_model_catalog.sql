CREATE TABLE provider_models (
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    model_id TEXT NOT NULL CHECK (
        char_length(model_id) BETWEEN 1 AND 255
        AND model_id !~ '[[:cntrl:]]'
    ),
    media_kind TEXT NOT NULL CHECK (media_kind IN ('image', 'video')),
    display_name TEXT NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 255),
    adapter_state TEXT NOT NULL CHECK (adapter_state IN ('supported', 'discovered')),
    lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('enabled', 'disabled')),
    operation_ids TEXT[] NOT NULL DEFAULT '{}',
    source_kind TEXT NOT NULL CHECK (source_kind IN ('adapter_contract', 'cli_help', 'cli_models')),
    first_seen_at_ms BIGINT NOT NULL,
    last_seen_at_ms BIGINT NOT NULL,
    last_successful_refresh_at_ms BIGINT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::JSONB CHECK (jsonb_typeof(metadata_json) = 'object'),
    PRIMARY KEY (provider_id, model_id, media_kind),
    CHECK (adapter_state = 'supported' OR lifecycle_state = 'disabled')
);

CREATE INDEX provider_models_provider_state_idx
    ON provider_models (provider_id, lifecycle_state, adapter_state, media_kind, model_id);

CREATE TABLE provider_model_refreshes (
    refresh_id UUID PRIMARY KEY,
    provider_account_id UUID NOT NULL,
    provider_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    discovered_count INTEGER NOT NULL DEFAULT 0 CHECK (discovered_count >= 0),
    error_code TEXT CHECK (error_code IS NULL OR char_length(error_code) <= 128),
    started_at_ms BIGINT,
    completed_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (provider_account_id, provider_id)
        REFERENCES provider_account_environments(provider_account_id, provider_id)
        ON DELETE RESTRICT
);

CREATE INDEX provider_model_refreshes_status_idx
    ON provider_model_refreshes (status, created_at_ms, refresh_id);

CREATE UNIQUE INDEX provider_model_refreshes_active_account_idx
    ON provider_model_refreshes (provider_account_id)
    WHERE status IN ('queued', 'running');

CREATE TABLE provider_account_model_observations (
    provider_account_id UUID NOT NULL,
    provider_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    media_kind TEXT NOT NULL,
    available BOOLEAN NOT NULL,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('adapter_contract', 'cli_help', 'cli_models')),
    cli_version TEXT CHECK (cli_version IS NULL OR char_length(cli_version) <= 255),
    observed_at_ms BIGINT NOT NULL,
    refresh_id UUID NOT NULL REFERENCES provider_model_refreshes(refresh_id) ON DELETE RESTRICT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::JSONB CHECK (jsonb_typeof(metadata_json) = 'object'),
    PRIMARY KEY (provider_account_id, provider_id, model_id, media_kind),
    FOREIGN KEY (provider_account_id, provider_id)
        REFERENCES provider_account_environments(provider_account_id, provider_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (provider_id, model_id, media_kind)
        REFERENCES provider_models(provider_id, model_id, media_kind)
        ON DELETE RESTRICT
);

CREATE INDEX provider_account_model_observations_model_idx
    ON provider_account_model_observations
       (provider_id, model_id, media_kind, available, observed_at_ms DESC);
