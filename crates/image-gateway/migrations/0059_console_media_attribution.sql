ALTER TABLE job_auth_attributions
    ADD COLUMN actor_user_id UUID,
    ADD COLUMN actor_session_id UUID,
    ADD COLUMN actor_authz_version BIGINT,
    ADD COLUMN route_provider_id TEXT,
    ADD COLUMN route_operation_id TEXT,
    ADD COLUMN route_command_schema TEXT,
    ADD COLUMN route_id UUID,
    ADD COLUMN route_revision BIGINT;

ALTER TABLE job_auth_attributions
    DROP CONSTRAINT job_auth_attributions_auth_kind_check,
    DROP CONSTRAINT job_auth_attributions_check,
    ADD CONSTRAINT job_auth_attributions_auth_kind_check
        CHECK (auth_kind IN ('api_key', 'legacy', 'user_session')),
    ADD CONSTRAINT job_auth_attributions_actor_user_fk
        FOREIGN KEY (actor_user_id)
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    ADD CONSTRAINT job_auth_attributions_route_fk
        FOREIGN KEY (
            route_id,
            route_revision,
            route_provider_id,
            route_operation_id,
            route_command_schema
        )
        REFERENCES provider_routes(
            route_id,
            revision,
            provider_id,
            operation_id,
            command_schema
        )
        ON DELETE RESTRICT,
    ADD CONSTRAINT job_auth_attributions_shape_check CHECK (
        (
            auth_kind = 'api_key'
            AND project_id IS NOT NULL
            AND service_account_id IS NOT NULL
            AND api_key_id IS NOT NULL
            AND credential_authz_version IS NOT NULL
            AND credential_authz_version > 0
            AND actor_user_id IS NULL
            AND actor_session_id IS NULL
            AND actor_authz_version IS NULL
            AND route_id IS NULL
            AND route_revision IS NULL
            AND route_provider_id IS NULL
            AND route_operation_id IS NULL
            AND route_command_schema IS NULL
        )
        OR
        (
            auth_kind = 'user_session'
            AND project_id IS NOT NULL
            AND service_account_id IS NULL
            AND api_key_id IS NULL
            AND credential_authz_version IS NULL
            AND actor_user_id IS NOT NULL
            AND actor_session_id IS NOT NULL
            AND actor_authz_version IS NOT NULL
            AND actor_authz_version > 0
            AND route_id IS NOT NULL
            AND route_revision IS NOT NULL
            AND route_revision > 0
            AND route_provider_id IS NOT NULL
            AND route_operation_id IS NOT NULL
            AND route_command_schema IS NOT NULL
        )
        OR
        (
            auth_kind = 'legacy'
            AND service_account_id IS NULL
            AND api_key_id IS NULL
            AND credential_authz_version IS NULL
            AND actor_user_id IS NULL
            AND actor_session_id IS NULL
            AND actor_authz_version IS NULL
            AND route_id IS NULL
            AND route_revision IS NULL
            AND route_provider_id IS NULL
            AND route_operation_id IS NULL
            AND route_command_schema IS NULL
        )
    );

CREATE INDEX job_auth_attributions_actor_admitted_idx
    ON job_auth_attributions (actor_user_id, admitted_at_ms DESC, job_id DESC)
    WHERE actor_user_id IS NOT NULL;

ALTER TABLE job_provider_route_attributions
    ALTER COLUMN api_key_id DROP NOT NULL;
