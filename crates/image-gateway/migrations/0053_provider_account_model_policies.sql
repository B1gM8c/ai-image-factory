ALTER TABLE provider_models
    ADD COLUMN execution_model_id TEXT;

UPDATE provider_models
SET execution_model_id = CASE
    WHEN provider_id = 'dreamina-cli' AND media_kind = 'image'
        THEN 'dreamina-image-' || model_id
    ELSE model_id
END;

ALTER TABLE provider_models
    ALTER COLUMN execution_model_id SET NOT NULL,
    ADD CONSTRAINT provider_models_execution_model_id_check CHECK (
        char_length(execution_model_id) BETWEEN 1 AND 255
        AND execution_model_id !~ '[[:cntrl:]]'
    );

CREATE INDEX provider_models_execution_lookup_idx
    ON provider_models (provider_id, execution_model_id, lifecycle_state, adapter_state);

CREATE TABLE provider_account_model_configurations (
    provider_account_id UUID PRIMARY KEY,
    provider_id TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('automatic', 'allowlist')),
    version BIGINT NOT NULL CHECK (version > 0),
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (provider_account_id, provider_id),
    FOREIGN KEY (provider_account_id, provider_id)
        REFERENCES provider_account_environments(provider_account_id, provider_id)
        ON DELETE RESTRICT
);

CREATE TABLE provider_account_model_bindings (
    provider_account_id UUID NOT NULL,
    provider_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    media_kind TEXT NOT NULL,
    configured_at_ms BIGINT NOT NULL,
    PRIMARY KEY (provider_account_id, model_id, media_kind),
    FOREIGN KEY (provider_account_id, provider_id)
        REFERENCES provider_account_model_configurations(provider_account_id, provider_id)
        ON DELETE CASCADE,
    FOREIGN KEY (provider_id, model_id, media_kind)
        REFERENCES provider_models(provider_id, model_id, media_kind)
        ON DELETE RESTRICT
);

CREATE INDEX provider_account_model_bindings_scheduler_idx
    ON provider_account_model_bindings
       (provider_account_id, provider_id, model_id, media_kind);
