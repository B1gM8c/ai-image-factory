ALTER TABLE identity_organizations
    DROP CONSTRAINT identity_organizations_check,
    ADD CONSTRAINT identity_organizations_owner_check CHECK (
        (
            organization_kind IN ('personal', 'team')
            AND owner_user_id IS NOT NULL
        )
        OR (
            organization_kind = 'system'
            AND owner_user_id IS NULL
        )
    ) NOT VALID;

ALTER TABLE identity_organizations
    VALIDATE CONSTRAINT identity_organizations_owner_check;
