ALTER TABLE gateway_service_accounts
    ADD COLUMN owner_type TEXT NOT NULL DEFAULT 'service_account',
    ADD COLUMN owner_user_id UUID;

ALTER TABLE gateway_service_accounts
    ADD CONSTRAINT gateway_service_accounts_owner_type_check
        CHECK (owner_type IN ('service_account', 'user')),
    ADD CONSTRAINT gateway_service_accounts_owner_shape_check
        CHECK (
            (owner_type = 'service_account' AND owner_user_id IS NULL)
            OR (owner_type = 'user' AND owner_user_id IS NOT NULL)
        ),
    ADD CONSTRAINT gateway_service_accounts_owner_user_fk
        FOREIGN KEY (owner_user_id)
        REFERENCES identity_users(user_id)
        ON DELETE RESTRICT
        NOT VALID;

ALTER TABLE gateway_service_accounts
    VALIDATE CONSTRAINT gateway_service_accounts_owner_user_fk;

CREATE UNIQUE INDEX gateway_service_accounts_active_user_owner_uidx
    ON gateway_service_accounts (project_id, owner_user_id)
    WHERE owner_type = 'user' AND deleted_at IS NULL;

ALTER TABLE gateway_api_keys
    ADD COLUMN permission_mode TEXT NOT NULL DEFAULT 'all',
    ADD COLUMN permissions JSONB NOT NULL DEFAULT '{}'::JSONB,
    ADD COLUMN created_by_user_id UUID,
    ADD COLUMN revoked_by_user_id UUID,
    ADD COLUMN revocation_reason TEXT;

ALTER TABLE gateway_api_keys
    ADD CONSTRAINT gateway_api_keys_permission_mode_check
        CHECK (permission_mode IN ('all', 'restricted', 'read_only')),
    ADD CONSTRAINT gateway_api_keys_permissions_object_check
        CHECK (jsonb_typeof(permissions) = 'object'),
    ADD CONSTRAINT gateway_api_keys_created_by_user_fk
        FOREIGN KEY (created_by_user_id)
        REFERENCES identity_users(user_id)
        ON DELETE RESTRICT
        NOT VALID,
    ADD CONSTRAINT gateway_api_keys_revoked_by_user_fk
        FOREIGN KEY (revoked_by_user_id)
        REFERENCES identity_users(user_id)
        ON DELETE RESTRICT
        NOT VALID,
    ADD CONSTRAINT gateway_api_keys_revocation_shape_check
        CHECK (
            (deleted_at IS NULL AND revoked_by_user_id IS NULL AND revocation_reason IS NULL)
            OR (deleted_at IS NOT NULL)
        );

ALTER TABLE gateway_api_keys
    VALIDATE CONSTRAINT gateway_api_keys_created_by_user_fk;

ALTER TABLE gateway_api_keys
    VALIDATE CONSTRAINT gateway_api_keys_revoked_by_user_fk;

CREATE INDEX gateway_api_keys_created_by_active_idx
    ON gateway_api_keys (project_id, created_by_user_id, created_at, id)
    WHERE deleted_at IS NULL AND created_by_user_id IS NOT NULL;

CREATE INDEX identity_audit_events_resource_created_idx
    ON identity_audit_events (resource_type, resource_id, created_at_ms DESC)
    WHERE resource_type IS NOT NULL AND resource_id IS NOT NULL;
