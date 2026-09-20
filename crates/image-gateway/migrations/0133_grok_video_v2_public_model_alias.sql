-- Keep the xAI-compatible public preview model name while preserving the
-- signed V2 provider, execution, and pricing identity. Route revisions and
-- their members/model mappings are immutable, so repair by advancing the
-- mutable route head and every binding that referenced the prior revision.
DO $$
DECLARE
    candidate RECORD;
    next_revision BIGINT;
    now_ms BIGINT;
BEGIN
    FOR candidate IN
        SELECT head.route_id, head.current_revision
        FROM provider_route_heads head
        JOIN provider_route_model_mappings mapping
          ON mapping.route_id = head.route_id
         AND mapping.route_revision = head.current_revision
         AND mapping.provider_id = head.provider_id
         AND mapping.operation_id = head.operation_id
         AND mapping.command_schema = head.command_schema
        WHERE head.provider_id = 'grok-cli'
          AND head.operation_id = 'videos.generations'
          AND head.command_schema = 'grok-cli.videos.generate.v2'
          AND mapping.api_profile = 'xai-videos-v1'
          AND mapping.public_model_id = 'grok-imagine-video-1.5'
          AND mapping.provider_model_id = 'grok-imagine-video-1.5'
          AND mapping.execution_model_id = 'grok-imagine-video-1.5'
          AND NOT EXISTS (
              SELECT 1
              FROM provider_route_model_mappings existing
              WHERE existing.route_id = head.route_id
                AND existing.route_revision = head.current_revision
                AND existing.api_profile = 'xai-videos-v1'
                AND existing.public_model_id = 'grok-imagine-video-1.5-preview'
                AND existing.provider_model_id = 'grok-imagine-video-1.5'
                AND existing.execution_model_id = 'grok-imagine-video-1.5'
          )
        ORDER BY head.route_id
    LOOP
        next_revision := candidate.current_revision + 1;
        now_ms := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;

        IF EXISTS (
            SELECT 1
            FROM provider_routes
            WHERE route_id = candidate.route_id
              AND revision = next_revision
        ) THEN
            RAISE EXCEPTION 'next Grok V2 route revision already exists for %', candidate.route_id;
        END IF;

        INSERT INTO provider_routes (
            route_id, revision, route_key, display_name, provider_id,
            operation_id, command_schema, route_kind, selection_strategy,
            state, created_at_ms, quota_freshness_ms, unknown_quota_policy
        )
        SELECT route_id, next_revision, route_key, display_name, provider_id,
               operation_id, command_schema, route_kind, selection_strategy,
               state, now_ms, quota_freshness_ms, unknown_quota_policy
        FROM provider_routes
        WHERE route_id = candidate.route_id
          AND revision = candidate.current_revision;

        INSERT INTO provider_route_members (
            route_id, route_revision, provider_id, operation_id, command_schema,
            provider_account_id, execution_profile_id, priority, weight, state,
            created_at_ms, minimum_remaining_percent
        )
        SELECT route_id, next_revision, provider_id, operation_id, command_schema,
               provider_account_id, execution_profile_id, priority, weight, state,
               now_ms, minimum_remaining_percent
        FROM provider_route_members
        WHERE route_id = candidate.route_id
          AND route_revision = candidate.current_revision;

        INSERT INTO provider_route_model_mappings (
            route_id, route_revision, provider_id, operation_id, command_schema,
            api_profile, public_model_id, provider_model_id, execution_model_id,
            media_kind, created_at_ms
        )
        SELECT route_id, next_revision, provider_id, operation_id, command_schema,
               api_profile,
               CASE
                   WHEN public_model_id = 'grok-imagine-video-1.5'
                    AND provider_model_id = 'grok-imagine-video-1.5'
                    AND execution_model_id = 'grok-imagine-video-1.5'
                   THEN 'grok-imagine-video-1.5-preview'
                   ELSE public_model_id
               END,
               provider_model_id, execution_model_id, media_kind, now_ms
        FROM provider_route_model_mappings
        WHERE route_id = candidate.route_id
          AND route_revision = candidate.current_revision
          AND public_model_id <> 'grok-imagine-video-1.5-preview';

        UPDATE gateway_api_key_provider_routes
        SET route_revision = next_revision, bound_at_ms = now_ms
        WHERE route_id = candidate.route_id
          AND route_revision = candidate.current_revision;

        UPDATE gateway_project_provider_routes
        SET route_revision = next_revision, updated_at_ms = now_ms
        WHERE route_id = candidate.route_id
          AND route_revision = candidate.current_revision;

        UPDATE gateway_platform_provider_routes
        SET route_revision = next_revision, updated_at_ms = now_ms
        WHERE route_id = candidate.route_id
          AND route_revision = candidate.current_revision;

        UPDATE provider_route_heads
        SET current_revision = next_revision, updated_at_ms = now_ms
        WHERE route_id = candidate.route_id
          AND current_revision = candidate.current_revision;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'Grok V2 route head changed while applying public model alias';
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
