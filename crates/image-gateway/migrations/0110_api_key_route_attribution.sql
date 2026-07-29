-- Personal API key attribution added credential ownership after routed API key
-- attribution was introduced. Preserve both dimensions in the immutable job
-- snapshot: API keys may be unrouted or carry one complete route identity.
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
            AND credential_owner_user_id IS NULL
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
            AND credential_owner_user_id IS NULL
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
