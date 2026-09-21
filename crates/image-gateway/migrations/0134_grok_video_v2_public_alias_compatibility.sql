-- Both published public names resolve to the signed canonical V2 model.
-- Preserve immutable route history and all mappings, advancing only bindings
-- to the repaired head. No credentials, prices, or queued jobs are changed.
-- Public names remain unique (the primary key); multiple names may share one
-- execution target. Resolution and pricing still bind the exact public name.
ALTER TABLE provider_route_model_mappings
    DROP CONSTRAINT IF EXISTS provider_route_model_mappings_route_id_route_revision_api_p_key;
-- Keep the execution lookup index used by admission and attribution checks.
CREATE INDEX IF NOT EXISTS provider_route_model_mappings_execution_idx
    ON provider_route_model_mappings (route_id, route_revision, api_profile, execution_model_id);

DO $$
DECLARE
    candidate RECORD;
    next_revision BIGINT;
    now_ms BIGINT;
BEGIN
    FOR candidate IN
        SELECT head.route_id, head.current_revision
        FROM provider_route_heads head
        WHERE head.provider_id = 'grok-cli'
          AND head.operation_id = 'videos.generations'
          AND head.command_schema = 'grok-cli.videos.generate.v2'
          AND EXISTS (
              SELECT 1 FROM provider_route_model_mappings mapping
              WHERE mapping.route_id = head.route_id
                AND mapping.route_revision = head.current_revision
                AND mapping.api_profile = 'xai-videos-v1'
                AND mapping.public_model_id IN ('grok-imagine-video-1.5', 'grok-imagine-video-1.5-preview')
                AND mapping.provider_model_id = 'grok-imagine-video-1.5'
                AND mapping.execution_model_id = 'grok-imagine-video-1.5'
          )
          AND (
              SELECT COUNT(*) FROM provider_route_model_mappings mapping
              WHERE mapping.route_id = head.route_id
                AND mapping.route_revision = head.current_revision
                AND mapping.api_profile = 'xai-videos-v1'
                AND mapping.public_model_id IN ('grok-imagine-video-1.5', 'grok-imagine-video-1.5-preview')
          ) = 1
        ORDER BY head.route_id
    LOOP
        next_revision := candidate.current_revision + 1;
        now_ms := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;

        INSERT INTO provider_routes (
            route_id, revision, route_key, display_name, provider_id,
            operation_id, command_schema, route_kind, selection_strategy,
            state, created_at_ms, quota_freshness_ms, unknown_quota_policy
        )
        SELECT route_id, next_revision, route_key, display_name, provider_id,
               operation_id, command_schema, route_kind, selection_strategy,
               state, now_ms, quota_freshness_ms, unknown_quota_policy
        FROM provider_routes
        WHERE route_id = candidate.route_id AND revision = candidate.current_revision;

        INSERT INTO provider_route_members (
            route_id, route_revision, provider_id, operation_id, command_schema,
            provider_account_id, execution_profile_id, priority, weight, state,
            created_at_ms, minimum_remaining_percent
        )
        SELECT route_id, next_revision, provider_id, operation_id, command_schema,
               provider_account_id, execution_profile_id, priority, weight, state,
               now_ms, minimum_remaining_percent
        FROM provider_route_members
        WHERE route_id = candidate.route_id AND route_revision = candidate.current_revision;

        INSERT INTO provider_route_model_mappings (
            route_id, route_revision, provider_id, operation_id, command_schema,
            api_profile, public_model_id, provider_model_id, execution_model_id,
            media_kind, created_at_ms
        )
        SELECT route_id, next_revision, provider_id, operation_id, command_schema,
               api_profile, public_model_id, provider_model_id, execution_model_id,
               media_kind, now_ms
        FROM provider_route_model_mappings
        WHERE route_id = candidate.route_id AND route_revision = candidate.current_revision;

        INSERT INTO provider_route_model_mappings (
            route_id, route_revision, provider_id, operation_id, command_schema,
            api_profile, public_model_id, provider_model_id, execution_model_id,
            media_kind, created_at_ms
        )
        SELECT route_id, next_revision, provider_id, operation_id, command_schema,
               api_profile,
               CASE public_model_id
                   WHEN 'grok-imagine-video-1.5' THEN 'grok-imagine-video-1.5-preview'
                   ELSE 'grok-imagine-video-1.5'
               END,
               provider_model_id, execution_model_id, media_kind, now_ms
        FROM provider_route_model_mappings
        WHERE route_id = candidate.route_id AND route_revision = candidate.current_revision
          AND api_profile = 'xai-videos-v1'
          AND public_model_id IN ('grok-imagine-video-1.5', 'grok-imagine-video-1.5-preview')
          AND provider_model_id = 'grok-imagine-video-1.5'
          AND execution_model_id = 'grok-imagine-video-1.5';

        UPDATE gateway_api_key_provider_routes
        SET route_revision = next_revision, bound_at_ms = now_ms
        WHERE route_id = candidate.route_id AND route_revision = candidate.current_revision;
        UPDATE gateway_project_provider_routes
        SET route_revision = next_revision, updated_at_ms = now_ms
        WHERE route_id = candidate.route_id AND route_revision = candidate.current_revision;
        UPDATE gateway_platform_provider_routes
        SET route_revision = next_revision, updated_at_ms = now_ms
        WHERE route_id = candidate.route_id AND route_revision = candidate.current_revision;
        UPDATE provider_route_heads
        SET current_revision = next_revision, updated_at_ms = now_ms
        WHERE route_id = candidate.route_id AND current_revision = candidate.current_revision;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Grok V2 route head changed while restoring public aliases';
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
