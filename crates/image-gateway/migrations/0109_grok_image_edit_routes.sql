-- Grok image edits share the managed account and quota pool with image
-- generation while retaining a distinct immutable execution profile.
ALTER TABLE provider_account_operations
    DROP CONSTRAINT provider_account_operations_operation_id_check;

ALTER TABLE provider_account_operations
    ADD CONSTRAINT provider_account_operations_operation_id_check
    CHECK (
        operation_id IN ('images.generations', 'images.edits', 'videos.generations')
    );

CREATE OR REPLACE FUNCTION sync_provider_account_operation_from_profile()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.state = 'enabled'
       AND NEW.operation_id IN ('images.generations', 'images.edits', 'videos.generations') THEN
        INSERT INTO provider_account_operations
          (provider_account_id, provider_id, operation_id, state, created_at_ms, updated_at_ms)
        VALUES
          (NEW.provider_account_id, NEW.provider_id, NEW.operation_id,
           'enabled', NEW.created_at_ms, NEW.updated_at_ms)
        ON CONFLICT (provider_account_id, operation_id) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$;

INSERT INTO provider_execution_profiles (
    execution_profile_id,
    profile_key,
    provider_id,
    command_schema,
    adapter_revision,
    credential_pool_id,
    provider_account_id,
    credential_ref,
    credential_revision,
    resource_policy_id,
    resource_policy_revision,
    state,
    created_at_ms,
    updated_at_ms,
    operation_id,
    operation_descriptor_revision,
    operation_descriptor_sha256_v1,
    completion_mode,
    idempotency_mode
)
SELECT
    md5(profile.provider_account_id::text || ':profile:grok:images.edits')::UUID,
    'managed.grok.edits.' || replace(profile.provider_account_id::text, '-', ''),
    profile.provider_id,
    'grok-cli.images.edit.v1',
    'grok-cli-0.2.102.agentic-media.v1',
    profile.credential_pool_id,
    profile.provider_account_id,
    profile.credential_ref,
    profile.credential_revision,
    profile.resource_policy_id,
    profile.resource_policy_revision,
    profile.state,
    profile.created_at_ms,
    profile.updated_at_ms,
    'images.edits',
    'grok-cli/images.edits/v1',
    '8b4e7bf805b5cc45bdd2316c72b4915f43e885fc908131a7bc8d920302c18db7',
    'inline',
    'submission_bound'
FROM provider_execution_profiles profile
WHERE profile.provider_id = 'grok-cli'
  AND profile.operation_id = 'images.generations'
  AND profile.state = 'enabled'
  AND NOT EXISTS (
      SELECT 1
      FROM provider_execution_profiles existing
      WHERE existing.provider_account_id = profile.provider_account_id
        AND existing.operation_id = 'images.edits'
        AND existing.state = 'enabled'
  )
ON CONFLICT DO NOTHING;

INSERT INTO provider_account_operations (
    provider_account_id,
    provider_id,
    operation_id,
    state,
    created_at_ms,
    updated_at_ms
)
SELECT
    profile.provider_account_id,
    profile.provider_id,
    profile.operation_id,
    'enabled',
    profile.created_at_ms,
    profile.updated_at_ms
FROM provider_execution_profiles profile
WHERE profile.provider_id = 'grok-cli'
  AND profile.operation_id = 'images.edits'
  AND profile.state = 'enabled'
ON CONFLICT (provider_account_id, operation_id) DO UPDATE
SET state = 'enabled',
    updated_at_ms = EXCLUDED.updated_at_ms;

INSERT INTO provider_routes (
    route_id,
    revision,
    route_key,
    display_name,
    provider_id,
    operation_id,
    command_schema,
    route_kind,
    selection_strategy,
    state,
    created_at_ms,
    quota_freshness_ms,
    unknown_quota_policy
)
SELECT
    md5(edit_profile.provider_account_id::text || ':route:grok:images.edits')::UUID,
    1,
    'account.' || replace(edit_profile.provider_account_id::text, '-', '') || '.grok-edits',
    left(source_route.display_name || ' edits', 128),
    edit_profile.provider_id,
    edit_profile.operation_id,
    edit_profile.command_schema,
    'account',
    source_route.selection_strategy,
    'enabled',
    source_route.created_at_ms,
    source_route.quota_freshness_ms,
    source_route.unknown_quota_policy
FROM provider_execution_profiles edit_profile
JOIN LATERAL (
    SELECT route.*
    FROM provider_route_members member
    JOIN provider_routes route
      ON route.route_id = member.route_id
     AND route.revision = member.route_revision
    WHERE member.provider_account_id = edit_profile.provider_account_id
      AND member.operation_id = 'images.generations'
      AND member.state = 'enabled'
      AND route.route_kind = 'account'
      AND route.state = 'enabled'
    ORDER BY route.created_at_ms, route.route_id
    LIMIT 1
) source_route ON TRUE
WHERE edit_profile.provider_id = 'grok-cli'
  AND edit_profile.operation_id = 'images.edits'
  AND edit_profile.state = 'enabled'
  AND NOT EXISTS (
      SELECT 1
      FROM provider_route_members existing
      WHERE existing.provider_account_id = edit_profile.provider_account_id
        AND existing.operation_id = 'images.edits'
        AND existing.state = 'enabled'
  )
ON CONFLICT DO NOTHING;

INSERT INTO provider_route_heads (
    route_id,
    route_key,
    provider_id,
    operation_id,
    command_schema,
    route_kind,
    current_revision,
    state,
    created_at_ms,
    updated_at_ms
)
SELECT
    route.route_id,
    route.route_key,
    route.provider_id,
    route.operation_id,
    route.command_schema,
    route.route_kind,
    route.revision,
    'enabled',
    route.created_at_ms,
    route.created_at_ms
FROM provider_routes route
WHERE route.provider_id = 'grok-cli'
  AND route.operation_id = 'images.edits'
  AND route.state = 'enabled'
ON CONFLICT DO NOTHING;

INSERT INTO provider_route_members (
    route_id,
    route_revision,
    provider_id,
    operation_id,
    command_schema,
    provider_account_id,
    execution_profile_id,
    priority,
    weight,
    state,
    created_at_ms,
    minimum_remaining_percent
)
SELECT
    route.route_id,
    route.revision,
    route.provider_id,
    route.operation_id,
    route.command_schema,
    profile.provider_account_id,
    profile.execution_profile_id,
    0,
    100,
    'enabled',
    route.created_at_ms,
    0
FROM provider_routes route
JOIN provider_execution_profiles profile
  ON profile.provider_account_id =
     (substring(route.route_key from '^account\.([0-9a-f]{32})\.grok-edits$'))::UUID
 AND profile.provider_id = route.provider_id
 AND profile.operation_id = route.operation_id
 AND profile.command_schema = route.command_schema
 AND profile.state = 'enabled'
WHERE route.provider_id = 'grok-cli'
  AND route.operation_id = 'images.edits'
  AND route.state = 'enabled'
ON CONFLICT DO NOTHING;

INSERT INTO provider_route_model_mappings (
    route_id,
    route_revision,
    provider_id,
    operation_id,
    command_schema,
    api_profile,
    public_model_id,
    provider_model_id,
    execution_model_id,
    media_kind,
    created_at_ms
)
SELECT
    route.route_id,
    route.revision,
    route.provider_id,
    route.operation_id,
    route.command_schema,
    'xai-images-v1',
    model.model_id,
    model.model_id,
    model.execution_model_id,
    model.media_kind,
    route.created_at_ms
FROM provider_routes route
JOIN provider_models model
  ON model.provider_id = route.provider_id
 AND route.operation_id = ANY(model.operation_ids)
 AND model.model_id = 'grok-imagine-image-quality'
 AND model.media_kind = 'image'
 AND model.adapter_state = 'supported'
 AND model.lifecycle_state = 'enabled'
WHERE route.provider_id = 'grok-cli'
  AND route.operation_id = 'images.edits'
  AND route.state = 'enabled'
ON CONFLICT DO NOTHING;

WITH candidates AS (
    SELECT route.*
    FROM provider_routes route
    JOIN provider_route_heads head
      ON head.route_id = route.route_id
     AND head.current_revision = route.revision
     AND head.state = 'enabled'
    WHERE route.provider_id = 'grok-cli'
      AND route.operation_id = 'images.edits'
      AND route.state = 'enabled'
),
unambiguous AS (
    SELECT min(route_id::text)::UUID AS route_id
    FROM candidates
    HAVING count(*) = 1
)
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
    route.provider_id,
    route.operation_id,
    route.command_schema,
    route.route_id,
    route.revision,
    'enabled',
    route.created_at_ms,
    route.created_at_ms
FROM unambiguous
JOIN candidates route ON route.route_id = unambiguous.route_id
ON CONFLICT (provider_id, operation_id) DO NOTHING;
