CREATE TABLE gateway_projects (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    archived_at BIGINT,
    UNIQUE (id, tenant_id)
);

-- Historical schemas used project_id as the quota tenant, except for the
-- built-in project whose established tenant is tenant_default. Preserve that
-- behavior without inventing ownership data that did not previously exist.
WITH existing_projects AS (
    SELECT project_id, MIN(created_at) AS created_at
    FROM (
        SELECT project_id, created_at FROM gateway_service_accounts
        UNION ALL
        SELECT project_id, created_at FROM gateway_api_keys
    ) AS credentials
    GROUP BY project_id
)
INSERT INTO gateway_projects (id, tenant_id, name, created_at, archived_at)
SELECT
    project_id,
    CASE
        WHEN project_id = 'proj_default' THEN 'tenant_default'
        ELSE project_id
    END,
    CASE
        WHEN project_id = 'proj_default' THEN 'Default project'
        ELSE 'Imported project ' || project_id
    END,
    created_at,
    NULL
FROM existing_projects;

INSERT INTO gateway_projects (id, tenant_id, name, created_at, archived_at)
VALUES (
    'proj_default',
    'tenant_default',
    'Default project',
    EXTRACT(EPOCH FROM transaction_timestamp())::BIGINT,
    NULL
)
ON CONFLICT (id) DO NOTHING;

ALTER TABLE gateway_service_accounts
    ADD COLUMN tenant_id TEXT;

UPDATE gateway_service_accounts AS service_account
SET tenant_id = project.tenant_id
FROM gateway_projects AS project
WHERE project.id = service_account.project_id;

-- A validated check lets PostgreSQL establish NOT NULL without a second
-- blocking heap scan on supported versions.
ALTER TABLE gateway_service_accounts
    ADD CONSTRAINT gateway_service_accounts_tenant_required_check
        CHECK (tenant_id IS NOT NULL) NOT VALID;

ALTER TABLE gateway_service_accounts
    VALIDATE CONSTRAINT gateway_service_accounts_tenant_required_check;

ALTER TABLE gateway_service_accounts
    ALTER COLUMN tenant_id SET NOT NULL;

ALTER TABLE gateway_service_accounts
    DROP CONSTRAINT gateway_service_accounts_tenant_required_check,
    ADD CONSTRAINT gateway_service_accounts_project_tenant_unique
        UNIQUE (id, project_id, tenant_id),
    ADD CONSTRAINT gateway_service_accounts_project_tenant_fk
        FOREIGN KEY (project_id, tenant_id)
        REFERENCES gateway_projects (id, tenant_id)
        ON DELETE RESTRICT
        NOT VALID;

ALTER TABLE gateway_service_accounts
    VALIDATE CONSTRAINT gateway_service_accounts_project_tenant_fk;

ALTER TABLE gateway_api_keys
    ADD COLUMN tenant_id TEXT,
    ADD COLUMN expires_at BIGINT,
    ADD COLUMN authz_version BIGINT NOT NULL DEFAULT 1
        CHECK (authz_version > 0);

UPDATE gateway_api_keys AS api_key
SET tenant_id = project.tenant_id
FROM gateway_projects AS project
WHERE project.id = api_key.project_id;

ALTER TABLE gateway_api_keys
    ADD CONSTRAINT gateway_api_keys_tenant_required_check
        CHECK (tenant_id IS NOT NULL) NOT VALID;

ALTER TABLE gateway_api_keys
    VALIDATE CONSTRAINT gateway_api_keys_tenant_required_check;

ALTER TABLE gateway_api_keys
    ALTER COLUMN tenant_id SET NOT NULL;

ALTER TABLE gateway_api_keys
    DROP CONSTRAINT gateway_api_keys_tenant_required_check,
    ADD CONSTRAINT gateway_api_keys_ownership_unique
        UNIQUE (id, service_account_id, project_id, tenant_id),
    ADD CONSTRAINT gateway_api_keys_service_account_ownership_fk
        FOREIGN KEY (service_account_id, project_id, tenant_id)
        REFERENCES gateway_service_accounts (id, project_id, tenant_id)
        ON DELETE RESTRICT
        NOT VALID;

ALTER TABLE gateway_api_keys
    VALIDATE CONSTRAINT gateway_api_keys_service_account_ownership_fk;

-- Migration 0009 already creates this identity on normal upgrade and fresh
-- paths. Keep 0036 independently safe for a compatible historical schema that
-- lacks it, without creating a duplicate unique index.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS constraint_record
        WHERE constraint_record.conrelid = 'jobs'::REGCLASS
          AND constraint_record.contype IN ('p', 'u')
          AND constraint_record.conkey = ARRAY[
              (
                  SELECT attnum
                  FROM pg_attribute
                  WHERE attrelid = 'jobs'::REGCLASS
                    AND attname = 'job_id'
                    AND NOT attisdropped
              ),
              (
                  SELECT attnum
                  FROM pg_attribute
                  WHERE attrelid = 'jobs'::REGCLASS
                    AND attname = 'tenant_id'
                    AND NOT attisdropped
              )
          ]::SMALLINT[]
    ) THEN
        ALTER TABLE jobs
            ADD CONSTRAINT jobs_auth_attribution_tenant_identity_unique
            UNIQUE (job_id, tenant_id);
    END IF;
END
$$;

CREATE TABLE job_auth_attributions (
    job_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    project_id TEXT,
    service_account_id TEXT,
    api_key_id TEXT,
    credential_authz_version BIGINT,
    auth_kind TEXT NOT NULL CHECK (auth_kind IN ('api_key', 'legacy')),
    admitted_at_ms BIGINT NOT NULL,
    FOREIGN KEY (job_id, tenant_id)
        REFERENCES jobs (job_id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (project_id, tenant_id)
        REFERENCES gateway_projects (id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (service_account_id, project_id, tenant_id)
        REFERENCES gateway_service_accounts (id, project_id, tenant_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (api_key_id, service_account_id, project_id, tenant_id)
        REFERENCES gateway_api_keys (id, service_account_id, project_id, tenant_id)
        ON DELETE RESTRICT,
    CHECK (
        (
            auth_kind = 'api_key'
            AND project_id IS NOT NULL
            AND service_account_id IS NOT NULL
            AND api_key_id IS NOT NULL
            AND credential_authz_version IS NOT NULL
            AND credential_authz_version > 0
        )
        OR
        (
            auth_kind = 'legacy'
            AND service_account_id IS NULL
            AND api_key_id IS NULL
            AND credential_authz_version IS NULL
        )
    )
);

ALTER TABLE usage_events
    ADD COLUMN job_id UUID;

ALTER TABLE usage_events
    ADD CONSTRAINT usage_events_job_fk
        FOREIGN KEY (job_id) REFERENCES jobs (job_id)
        ON DELETE RESTRICT
        NOT VALID;

ALTER TABLE usage_events
    VALIDATE CONSTRAINT usage_events_job_fk;

CREATE INDEX gateway_projects_active_created_idx
    ON gateway_projects (created_at, id)
    WHERE archived_at IS NULL;

CREATE INDEX gateway_api_keys_active_project_created_idx
    ON gateway_api_keys (project_id, created_at, id)
    WHERE deleted_at IS NULL;

CREATE INDEX job_auth_attributions_api_key_admitted_idx
    ON job_auth_attributions (api_key_id, admitted_at_ms DESC, job_id DESC)
    WHERE api_key_id IS NOT NULL;

CREATE INDEX job_auth_attributions_project_admitted_idx
    ON job_auth_attributions (project_id, admitted_at_ms DESC, job_id DESC)
    WHERE project_id IS NOT NULL;

CREATE INDEX usage_events_job_created_idx
    ON usage_events (job_id, created_at_ms)
    WHERE job_id IS NOT NULL;

CREATE FUNCTION reject_job_auth_attribution_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'job authentication attribution is immutable'
        USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_auth_attributions_immutable
BEFORE UPDATE OR DELETE ON job_auth_attributions
FOR EACH ROW EXECUTE FUNCTION reject_job_auth_attribution_mutation();
