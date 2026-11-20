ALTER TABLE provider_account_environments
    DROP CONSTRAINT provider_account_environments_environment_kind_check,
    ADD CONSTRAINT provider_account_environments_environment_kind_check CHECK (
        (provider_id = 'dreamina-canvas' AND environment_kind = 'dreamina_canvas_home_v1')
        OR (provider_id <> 'dreamina-canvas'
            AND environment_kind IN ('codex_home_v1', 'grok_home_v1', 'dreamina_home_v1'))
    );
