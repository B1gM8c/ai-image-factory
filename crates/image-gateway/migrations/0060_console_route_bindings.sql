CREATE TABLE gateway_platform_provider_routes (
    provider_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    command_schema TEXT NOT NULL,
    route_id UUID NOT NULL,
    route_revision BIGINT NOT NULL CHECK (route_revision > 0),
    state TEXT NOT NULL CHECK (state IN ('enabled', 'disabled')),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (provider_id, operation_id),
    FOREIGN KEY (
        route_id,
        route_revision,
        provider_id,
        operation_id,
        command_schema
    )
        REFERENCES provider_routes(
            route_id,
            revision,
            provider_id,
            operation_id,
            command_schema
        )
        ON DELETE RESTRICT
);

CREATE TABLE gateway_project_provider_routes (
    project_id TEXT NOT NULL REFERENCES gateway_projects(id) ON DELETE RESTRICT,
    provider_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    command_schema TEXT NOT NULL,
    route_id UUID NOT NULL,
    route_revision BIGINT NOT NULL CHECK (route_revision > 0),
    state TEXT NOT NULL CHECK (state IN ('enabled', 'disabled')),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (project_id, provider_id, operation_id),
    FOREIGN KEY (
        route_id,
        route_revision,
        provider_id,
        operation_id,
        command_schema
    )
        REFERENCES provider_routes(
            route_id,
            revision,
            provider_id,
            operation_id,
            command_schema
        )
        ON DELETE RESTRICT
);

-- Existing installations get a platform default only when the enabled route is
-- unambiguous. Future route changes must update this binding explicitly.
INSERT INTO gateway_platform_provider_routes (
    provider_id,
    operation_id,
    command_schema,
    route_id,
    route_revision,
    state,
    created_at_ms,
    updated_at_ms
)
SELECT
    head.provider_id,
    head.operation_id,
    head.command_schema,
    head.route_id,
    head.current_revision,
    'enabled',
    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT,
    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
FROM provider_route_heads head
WHERE head.state = 'enabled'
  AND NOT EXISTS (
      SELECT 1
      FROM provider_route_heads competing
      WHERE competing.state = 'enabled'
        AND competing.provider_id = head.provider_id
        AND competing.operation_id = head.operation_id
        AND competing.route_id <> head.route_id
  );

ALTER TABLE job_auth_attributions
    DROP CONSTRAINT job_auth_attributions_shape_check,
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
            AND (
                (
                    route_id IS NULL
                    AND route_revision IS NULL
                    AND route_provider_id IS NULL
                    AND route_operation_id IS NULL
                    AND route_command_schema IS NULL
                )
                OR
                (
                    route_id IS NOT NULL
                    AND route_revision IS NOT NULL
                    AND route_revision > 0
                    AND route_provider_id IS NOT NULL
                    AND route_operation_id IS NOT NULL
                    AND route_command_schema IS NOT NULL
                )
            )
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
