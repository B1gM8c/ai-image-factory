-- Repair projects created through the historical platform-owner path, which
-- could write a gateway project without an organization or owner membership.
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
    LEFT('Recovered workspace ' || project.tenant_id, 128),
    'system',
    NULL,
    MIN(project.created_at) * 1000,
    MIN(project.created_at) * 1000
FROM gateway_projects project
LEFT JOIN identity_organizations organization
  ON organization.organization_id = project.tenant_id
WHERE organization.organization_id IS NULL
GROUP BY project.tenant_id;

WITH platform_owner AS (
    SELECT user_id
    FROM identity_users
    WHERE disabled_at_ms IS NULL
      AND roles @> ARRAY['platform_owner']::TEXT[]
    ORDER BY created_at_ms, user_id
    LIMIT 1
),
unowned_projects AS (
    SELECT project.id AS project_id,
           project.tenant_id AS organization_id,
           project.created_at * 1000 AS created_at_ms
    FROM gateway_projects project
    WHERE NOT EXISTS (
        SELECT 1
        FROM identity_project_memberships membership
        WHERE membership.organization_id = project.tenant_id
          AND membership.project_id = project.id
          AND membership.state = 'active'
    )
),
candidate_owners AS (
    SELECT unowned.organization_id,
           unowned.project_id,
           unowned.created_at_ms,
           COALESCE(
               (
                   SELECT membership.user_id
                   FROM identity_organization_memberships membership
                   WHERE membership.organization_id = unowned.organization_id
                     AND membership.state = 'active'
                     AND membership.role = 'owner'
                   ORDER BY membership.created_at_ms, membership.user_id
                   LIMIT 1
               ),
               (SELECT user_id FROM platform_owner)
           ) AS user_id
    FROM unowned_projects unowned
)
INSERT INTO identity_organization_memberships (
    organization_id,
    user_id,
    role,
    state,
    created_at_ms,
    updated_at_ms
)
SELECT DISTINCT
    candidate.organization_id,
    candidate.user_id,
    'owner',
    'active',
    candidate.created_at_ms,
    candidate.created_at_ms
FROM candidate_owners candidate
WHERE candidate.user_id IS NOT NULL
ON CONFLICT (organization_id, user_id) DO UPDATE
SET state = 'active',
    updated_at_ms = GREATEST(
        identity_organization_memberships.updated_at_ms,
        EXCLUDED.updated_at_ms
    );

WITH platform_owner AS (
    SELECT user_id
    FROM identity_users
    WHERE disabled_at_ms IS NULL
      AND roles @> ARRAY['platform_owner']::TEXT[]
    ORDER BY created_at_ms, user_id
    LIMIT 1
),
unowned_projects AS (
    SELECT project.id AS project_id,
           project.tenant_id AS organization_id,
           project.created_at * 1000 AS created_at_ms
    FROM gateway_projects project
    WHERE NOT EXISTS (
        SELECT 1
        FROM identity_project_memberships membership
        WHERE membership.organization_id = project.tenant_id
          AND membership.project_id = project.id
          AND membership.state = 'active'
    )
),
candidate_owners AS (
    SELECT unowned.organization_id,
           unowned.project_id,
           unowned.created_at_ms,
           COALESCE(
               (
                   SELECT membership.user_id
                   FROM identity_organization_memberships membership
                   WHERE membership.organization_id = unowned.organization_id
                     AND membership.state = 'active'
                     AND membership.role = 'owner'
                   ORDER BY membership.created_at_ms, membership.user_id
                   LIMIT 1
               ),
               (SELECT user_id FROM platform_owner)
           ) AS user_id
    FROM unowned_projects unowned
)
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
    candidate.organization_id,
    candidate.project_id,
    candidate.user_id,
    'owner',
    'active',
    FALSE,
    candidate.created_at_ms,
    candidate.created_at_ms
FROM candidate_owners candidate
WHERE candidate.user_id IS NOT NULL
ON CONFLICT (organization_id, project_id, user_id) DO UPDATE
SET role = 'owner',
    state = 'active',
    updated_at_ms = GREATEST(
        identity_project_memberships.updated_at_ms,
        EXCLUDED.updated_at_ms
    );
