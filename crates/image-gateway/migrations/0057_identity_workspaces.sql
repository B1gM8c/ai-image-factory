-- tenant_id is the organization/workspace security boundary. Existing
-- tenant values remain valid organization identifiers so this migration does
-- not rewrite jobs, usage, billing, or project credentials.
CREATE TABLE identity_organizations (
    organization_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    organization_kind TEXT NOT NULL CHECK (
        organization_kind IN ('personal', 'team', 'system')
    ),
    owner_user_id UUID REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    CHECK (
        char_length(organization_id) BETWEEN 1 AND 256
        AND organization_id !~ '[[:cntrl:]]'
    ),
    CHECK (char_length(display_name) BETWEEN 1 AND 128),
    CHECK (
        (organization_kind = 'personal' AND owner_user_id IS NOT NULL)
        OR organization_kind = 'system'
    ),
    CHECK (updated_at_ms >= created_at_ms)
);

CREATE UNIQUE INDEX identity_organizations_personal_owner_uidx
    ON identity_organizations(owner_user_id)
    WHERE organization_kind = 'personal';

-- Preserve every tenant already represented by a project, including imported
-- historical tenants that cannot be attributed to an identity user.
INSERT INTO identity_organizations (
    organization_id,
    display_name,
    organization_kind,
    owner_user_id,
    created_at_ms,
    updated_at_ms
)
SELECT
    project.tenant_id,
    LEFT(
        CASE
            WHEN project.tenant_id = 'tenant_default' THEN 'Legacy workspace'
            ELSE 'Imported workspace ' || project.tenant_id
        END,
        128
    ),
    'system',
    NULL,
    MIN(project.created_at) * 1000,
    MIN(project.created_at) * 1000
FROM gateway_projects AS project
GROUP BY project.tenant_id
ON CONFLICT (organization_id) DO NOTHING;

INSERT INTO identity_organizations (
    organization_id,
    display_name,
    organization_kind,
    owner_user_id,
    created_at_ms,
    updated_at_ms
)
VALUES (
    'tenant_default',
    'Legacy workspace',
    'system',
    NULL,
    (EXTRACT(EPOCH FROM transaction_timestamp()) * 1000)::BIGINT,
    (EXTRACT(EPOCH FROM transaction_timestamp()) * 1000)::BIGINT
)
ON CONFLICT (organization_id) DO NOTHING;

CREATE TABLE identity_organization_memberships (
    organization_id TEXT NOT NULL
        REFERENCES identity_organizations(organization_id) ON DELETE RESTRICT,
    user_id UUID NOT NULL REFERENCES identity_users(user_id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('owner', 'member')),
    state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'disabled')),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (organization_id, user_id),
    CHECK (updated_at_ms >= created_at_ms)
);

CREATE INDEX identity_organization_memberships_user_active_idx
    ON identity_organization_memberships(user_id, organization_id)
    WHERE state = 'active';

CREATE TABLE identity_project_memberships (
    organization_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    user_id UUID NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('owner', 'member')),
    state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'disabled')),
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (organization_id, project_id, user_id),
    FOREIGN KEY (project_id, organization_id)
        REFERENCES gateway_projects(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (organization_id, user_id)
        REFERENCES identity_organization_memberships(organization_id, user_id)
        ON DELETE CASCADE,
    CHECK (updated_at_ms >= created_at_ms)
);

CREATE INDEX identity_project_memberships_user_active_idx
    ON identity_project_memberships(user_id, organization_id, project_id)
    WHERE state = 'active';

CREATE UNIQUE INDEX identity_project_memberships_user_default_uidx
    ON identity_project_memberships(user_id)
    WHERE state = 'active' AND is_default;

-- Deterministic identifiers make retry and backfill behavior idempotent while
-- keeping identity creation independent from application-side coordination.
INSERT INTO identity_organizations (
    organization_id,
    display_name,
    organization_kind,
    owner_user_id,
    created_at_ms,
    updated_at_ms
)
SELECT
    'org_' || REPLACE(user_record.user_id::TEXT, '-', ''),
    LEFT(user_record.display_name || ' workspace', 128),
    'personal',
    user_record.user_id,
    user_record.created_at_ms,
    user_record.created_at_ms
FROM identity_users AS user_record;

INSERT INTO gateway_projects (id, tenant_id, name, created_at, archived_at)
SELECT
    'proj_' || REPLACE(user_record.user_id::TEXT, '-', ''),
    'org_' || REPLACE(user_record.user_id::TEXT, '-', ''),
    'Default project',
    user_record.created_at_ms / 1000,
    NULL
FROM identity_users AS user_record;

INSERT INTO identity_organization_memberships (
    organization_id,
    user_id,
    role,
    state,
    created_at_ms,
    updated_at_ms
)
SELECT
    'org_' || REPLACE(user_record.user_id::TEXT, '-', ''),
    user_record.user_id,
    'owner',
    'active',
    user_record.created_at_ms,
    user_record.created_at_ms
FROM identity_users AS user_record;

INSERT INTO identity_project_memberships (
    organization_id,
    project_id,
    user_id,
    role,
    state,
    is_default,
    created_at_ms,
    updated_at_ms
)
SELECT
    'org_' || REPLACE(user_record.user_id::TEXT, '-', ''),
    'proj_' || REPLACE(user_record.user_id::TEXT, '-', ''),
    user_record.user_id,
    'owner',
    'active',
    TRUE,
    user_record.created_at_ms,
    user_record.created_at_ms
FROM identity_users AS user_record;

CREATE FUNCTION identity_provision_personal_workspace() RETURNS TRIGGER AS $$
DECLARE
    personal_organization_id TEXT :=
        'org_' || REPLACE(NEW.user_id::TEXT, '-', '');
    default_project_id TEXT :=
        'proj_' || REPLACE(NEW.user_id::TEXT, '-', '');
BEGIN
    INSERT INTO identity_organizations (
        organization_id,
        display_name,
        organization_kind,
        owner_user_id,
        created_at_ms,
        updated_at_ms
    )
    VALUES (
        personal_organization_id,
        LEFT(NEW.display_name || ' workspace', 128),
        'personal',
        NEW.user_id,
        NEW.created_at_ms,
        NEW.created_at_ms
    );

    INSERT INTO gateway_projects (id, tenant_id, name, created_at, archived_at)
    VALUES (
        default_project_id,
        personal_organization_id,
        'Default project',
        NEW.created_at_ms / 1000,
        NULL
    );

    INSERT INTO identity_organization_memberships (
        organization_id,
        user_id,
        role,
        state,
        created_at_ms,
        updated_at_ms
    )
    VALUES (
        personal_organization_id,
        NEW.user_id,
        'owner',
        'active',
        NEW.created_at_ms,
        NEW.created_at_ms
    );

    INSERT INTO identity_project_memberships (
        organization_id,
        project_id,
        user_id,
        role,
        state,
        is_default,
        created_at_ms,
        updated_at_ms
    )
    VALUES (
        personal_organization_id,
        default_project_id,
        NEW.user_id,
        'owner',
        'active',
        TRUE,
        NEW.created_at_ms,
        NEW.created_at_ms
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER identity_users_provision_personal_workspace
AFTER INSERT ON identity_users
FOR EACH ROW EXECUTE FUNCTION identity_provision_personal_workspace();

ALTER TABLE provider_accounts
    ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'tenant_default',
    ADD COLUMN owner_user_id UUID;

ALTER TABLE provider_accounts
    ADD CONSTRAINT provider_accounts_tenant_fk
        FOREIGN KEY (tenant_id)
        REFERENCES identity_organizations(organization_id)
        ON DELETE RESTRICT
        NOT VALID,
    ADD CONSTRAINT provider_accounts_owner_user_fk
        FOREIGN KEY (owner_user_id)
        REFERENCES identity_users(user_id)
        ON DELETE RESTRICT
        NOT VALID,
    ADD CONSTRAINT provider_accounts_tenant_identity_unique
        UNIQUE (provider_account_id, tenant_id);

ALTER TABLE provider_accounts
    VALIDATE CONSTRAINT provider_accounts_tenant_fk;

ALTER TABLE provider_accounts
    VALIDATE CONSTRAINT provider_accounts_owner_user_fk;

CREATE INDEX provider_accounts_tenant_owner_updated_idx
    ON provider_accounts(
        tenant_id,
        owner_user_id,
        updated_at_ms DESC,
        provider_account_id
    );

-- RLS is intentionally deferred. The current application role owns the tables
-- and can bypass row policies; explicit scoped repositories remain the primary
-- boundary until migration and runtime database roles are separated.
