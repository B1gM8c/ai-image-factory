ALTER TABLE job_auth_attributions
    ADD COLUMN credential_owner_user_id UUID;

ALTER TABLE job_auth_attributions
    DROP CONSTRAINT job_auth_attributions_shape_check,
    ADD CONSTRAINT job_auth_attributions_credential_owner_user_fk
        FOREIGN KEY (credential_owner_user_id)
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
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
            AND credential_owner_user_id IS NULL
            AND actor_user_id IS NOT NULL
            AND actor_session_id IS NOT NULL
            AND actor_authz_version IS NOT NULL
            AND actor_authz_version > 0
            AND route_id IS NOT NULL
            AND route_revision IS NOT NULL
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

CREATE INDEX job_auth_attributions_credential_owner_admitted_idx
    ON job_auth_attributions (
        credential_owner_user_id,
        admitted_at_ms DESC,
        job_id DESC
    )
    WHERE credential_owner_user_id IS NOT NULL;

ALTER TABLE gateway_request_observations
    ADD COLUMN credential_owner_user_id UUID,
    ADD CONSTRAINT gateway_request_observations_credential_owner_user_fk
        FOREIGN KEY (credential_owner_user_id)
        REFERENCES identity_users(user_id) ON DELETE RESTRICT;

CREATE INDEX gateway_request_observations_credential_owner_created_idx
    ON gateway_request_observations (
        credential_owner_user_id,
        created_at_ms DESC,
        request_id DESC
    )
    WHERE credential_owner_user_id IS NOT NULL;
