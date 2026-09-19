-- Add the V2 model and immutable pricing identity without changing V1 rows.
INSERT INTO provider_models (
    provider_id, model_id, execution_model_id, media_kind, display_name, adapter_state,
    lifecycle_state, operation_ids, source_kind, first_seen_at_ms,
    last_seen_at_ms, last_successful_refresh_at_ms, metadata_json
)
SELECT provider_id,
       'grok-imagine-video-1.5',
       'grok-imagine-video-1.5',
       media_kind,
       'Grok Imagine Video 1.5',
       adapter_state,
       lifecycle_state,
       operation_ids,
       source_kind,
       first_seen_at_ms,
       last_seen_at_ms,
       last_successful_refresh_at_ms,
       metadata_json
FROM provider_models
WHERE provider_id = 'grok-cli'
  AND model_id = 'grok-imagine-video-1.5-preview'
  AND media_kind = 'video'
  AND NOT EXISTS (
      SELECT 1
      FROM provider_models existing
      WHERE existing.provider_id = 'grok-cli'
        AND existing.model_id = 'grok-imagine-video-1.5'
        AND existing.media_kind = 'video'
  )
LIMIT 1
ON CONFLICT DO NOTHING;

INSERT INTO pricing_surface_contract_revisions (
    contract_key, revision, contract_hash, contract_schema_version,
    api_profile, operation, provider_id, provider_model_id,
    public_model_id, media_kind, service_tier, execution_surface,
    normalizer_key, normalizer_revision, contract_json, created_at_ms
)
VALUES (
    'grok-cli.videos.generations.v2.pricing-surface:95737ab45d7c904a',
    1,
    '7d1f94c4cc807d9b82d4c199bd05181867b53c6268ae89b1665a1e7c2f42aa87',
    2,
    'xai-videos-v1', 'video_generation', 'grok-cli',
    'grok-imagine-video-1.5', 'grok-imagine-video-1.5',
    'video', 'standard', 'provider_cli',
    'grok-cli.videos.generate.v2', 1,
    $contract$
    {"contract":{"api_profiles":["xai-videos-v1"],"command_schema":"grok-cli.videos.generate.v2","constraints":[{"ConditionalPresence":{"cases":[{"forbidden":[],"required":["aspect_ratio"],"selector_values":["0"]},{"forbidden":[],"required":[],"selector_values":["1"]},{"forbidden":[],"required":["aspect_ratio"],"selector_values":["2","3","4","5","6","7","8","9"]}],"selector":{"Dimension":"input_image_count"}}}],"contract_id":"grok-cli.videos.generations.v2.pricing-surface","contract_version":1,"dimensions":[{"domain":{"IntegerClosed":{"max":15,"min":1}},"key":"duration","required":true},{"domain":{"Enum":["480p","720p"]},"key":"resolution","required":true},{"domain":{"IntegerClosed":{"max":9,"min":0}},"key":"input_image_count","required":true},{"domain":{"Enum":["1:1","16:9","9:16","4:3","3:4","3:2","2:3"]},"key":"aspect_ratio","required":false}],"media_kind":"video","metering_bases":[{"confidence":"exact","customer_sale_required":true,"metric":"image_input","quantity_source":"request_derived","unit":"image"},{"confidence":"exact","customer_sale_required":true,"metric":"video_requested_second","quantity_source":"request_derived","unit":"second"},{"confidence":"exact","customer_sale_required":false,"metric":"video_output_second","quantity_source":"request_derived","unit":"second"}],"normalizer_key":"grok-cli.videos.generate.v2","normalizer_revision":1,"output_cardinality":{"Fixed":1},"pricing_operation":"video_generation","provider_id":"grok-cli","provider_models":["grok-imagine-video-1.5"],"route_operation":"videos.generations","support":"Supported"},"exact_surface":{"api_profile":"xai-videos-v1","execution_surface":"provider_cli","provider_model_id":"grok-imagine-video-1.5","public_model_id":"grok-imagine-video-1.5","service_tier":"standard"},"schema_version":2}
    $contract$::JSONB,
    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
)
ON CONFLICT (contract_key, revision) DO NOTHING;

WITH target_book AS (
    SELECT price_book_id
    FROM price_books
    WHERE purpose = 'customer_sale'
      AND scope_type = 'platform'
      AND currency = 'USD'
      AND state = 'active'
    ORDER BY created_at_ms, price_book_id
    LIMIT 1
), next_version AS (
    SELECT target_book.price_book_id,
           COALESCE(MAX(version.version), 0) + 1 AS version
    FROM target_book
    LEFT JOIN price_book_versions version
      ON version.price_book_id = target_book.price_book_id
    GROUP BY target_book.price_book_id
)
INSERT INTO price_book_versions (
    price_book_version_id, price_book_id, version, api_profile,
    operation, provider_id, provider_model_id, public_model_id,
    media_kind, service_tier, execution_surface, billing_mode,
    is_free, state, effective_from_ms, effective_until_ms,
    source_kind, source_url, source_checked_at_ms, notes,
    control_version, created_at_ms, updated_at_ms
)
SELECT 'f2c8d05a-a6d3-4b7f-8b0a-2be4bd8ed132'::UUID,
       next_version.price_book_id,
       next_version.version,
       'xai-videos-v1', 'video_generation', 'grok-cli',
       'grok-imagine-video-1.5', 'grok-imagine-video-1.5',
       'video', 'standard', 'provider_cli', 'customer_rate',
       FALSE, 'draft', now_ms, NULL,
       'official_document', 'https://docs.x.ai/developers/models',
       1783555200000, 'Draft Grok Imagine Video 1.5 V2 customer price',
       1, now_ms, now_ms
FROM next_version
CROSS JOIN LATERAL (
    SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
) clock
WHERE NOT EXISTS (
    SELECT 1
    FROM price_book_versions version
    WHERE version.price_book_version_id = 'f2c8d05a-a6d3-4b7f-8b0a-2be4bd8ed132'::UUID
)
ON CONFLICT DO NOTHING;

INSERT INTO price_components (
    price_component_id, price_book_version_id, component_key,
    metric, unit, unit_size, unit_price_micros, outcome,
    quantity_source, required_confidence, rounding_mode,
    dimensions_json, created_at_ms
)
SELECT md5('f2c8d05a-a6d3-4b7f-8b0a-2be4bd8ed132:' || component_key)::UUID,
       'f2c8d05a-a6d3-4b7f-8b0a-2be4bd8ed132'::UUID,
       component_key, metric, unit, unit_size, unit_price_micros,
       outcome, quantity_source, required_confidence, rounding_mode,
       dimensions_json, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
FROM (VALUES
    ('image-input-succeeded', 'image_input', 'image', 1::BIGINT, 10000::BIGINT, 'succeeded', 'request_derived', 'exact', 'exact', '{}'::JSONB),
    ('image-input-failed', 'image_input', 'image', 1::BIGINT, 0::BIGINT, 'failed', 'request_derived', 'exact', 'exact', '{}'::JSONB),
    ('image-input-no-effect', 'image_input', 'image', 1::BIGINT, 0::BIGINT, 'no_effect', 'request_derived', 'exact', 'exact', '{}'::JSONB),
    ('video-second-succeeded-480p', 'video_requested_second', 'second', 1::BIGINT, 80000::BIGINT, 'succeeded', 'request_derived', 'exact', 'exact', '{}'::JSONB),
    ('video-second-succeeded-720p', 'video_requested_second', 'second', 1::BIGINT, 140000::BIGINT, 'succeeded', 'request_derived', 'exact', 'exact', '{"resolution":"720p"}'::JSONB),
    ('video-second-failed', 'video_requested_second', 'second', 1::BIGINT, 0::BIGINT, 'failed', 'request_derived', 'exact', 'exact', '{}'::JSONB),
    ('video-second-no-effect', 'video_requested_second', 'second', 1::BIGINT, 0::BIGINT, 'no_effect', 'request_derived', 'exact', 'exact', '{}'::JSONB)
) AS components(component_key, metric, unit, unit_size, unit_price_micros, outcome, quantity_source, required_confidence, rounding_mode, dimensions_json)
WHERE EXISTS (
    SELECT 1 FROM price_book_versions version
    WHERE version.price_book_version_id = 'f2c8d05a-a6d3-4b7f-8b0a-2be4bd8ed132'::UUID
)
ON CONFLICT DO NOTHING;

INSERT INTO price_book_version_surface_contract_bindings (
    price_book_version_id, contract_key, contract_revision,
    contract_hash, bound_at_ms
)
SELECT 'f2c8d05a-a6d3-4b7f-8b0a-2be4bd8ed132'::UUID,
       'grok-cli.videos.generations.v2.pricing-surface:95737ab45d7c904a',
       1,
       '7d1f94c4cc807d9b82d4c199bd05181867b53c6268ae89b1665a1e7c2f42aa87',
       (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
WHERE EXISTS (
    SELECT 1 FROM price_book_versions version
    WHERE version.price_book_version_id = 'f2c8d05a-a6d3-4b7f-8b0a-2be4bd8ed132'::UUID
)
  AND NOT EXISTS (
      SELECT 1
      FROM price_book_version_surface_contract_bindings binding
      WHERE binding.price_book_version_id = 'f2c8d05a-a6d3-4b7f-8b0a-2be4bd8ed132'::UUID
        AND binding.contract_key = 'grok-cli.videos.generations.v2.pricing-surface:95737ab45d7c904a'
  )
ON CONFLICT DO NOTHING;

INSERT INTO provider_execution_profiles (
    execution_profile_id, profile_key, provider_id, command_schema,
    adapter_revision, credential_pool_id, provider_account_id,
    credential_ref, credential_revision, resource_policy_id,
    resource_policy_revision, state, created_at_ms, updated_at_ms,
    operation_id, operation_descriptor_revision,
    operation_descriptor_sha256_v1, completion_mode, idempotency_mode
)
SELECT md5(profile.provider_account_id::TEXT || ':profile:grok:videos.v2')::UUID,
       'managed.grok.videos.v2.' || replace(profile.provider_account_id::TEXT, '-', ''),
       profile.provider_id,
       'grok-cli.videos.generate.v2',
       'grok-cli-1.0.34.agentic-video.v1',
       profile.credential_pool_id, profile.provider_account_id,
       profile.credential_ref, profile.credential_revision,
       profile.resource_policy_id, profile.resource_policy_revision,
       'disabled', profile.created_at_ms, profile.updated_at_ms,
       'videos.generations', 'grok-cli/videos.generations/v2',
       '7d9fa78e4528e9cf833abb97261c9f4a7a5c369d74b70d1462a892c4c747aa7b',
       'inline', 'submission_bound'
FROM (
    SELECT profile.*,
           ROW_NUMBER() OVER (
               PARTITION BY profile.provider_account_id
               ORDER BY profile.updated_at_ms DESC, profile.created_at_ms DESC,
                        profile.execution_profile_id
           ) AS account_rank
    FROM provider_execution_profiles profile
    WHERE profile.provider_id = 'grok-cli'
      AND profile.operation_id = 'videos.generations'
      AND profile.command_schema = 'grok-cli.videos.generate.v1'
      AND profile.state = 'enabled'
) profile
WHERE profile.account_rank = 1
ON CONFLICT DO NOTHING;

INSERT INTO provider_routes (
    route_id, revision, route_key, display_name, provider_id,
    operation_id, command_schema, route_kind, selection_strategy,
    state, created_at_ms, quota_freshness_ms, unknown_quota_policy
)
SELECT md5(profile.provider_account_id::TEXT || ':route:grok:videos.v2')::UUID,
       1,
       'account.' || replace(profile.provider_account_id::TEXT, '-', '') || '.grok-video-v2',
       left('Grok Video V2 ' || profile.profile_key, 128),
       profile.provider_id, 'videos.generations', 'grok-cli.videos.generate.v2',
       'account', source_route.selection_strategy, 'disabled',
       profile.created_at_ms, source_route.quota_freshness_ms,
       source_route.unknown_quota_policy
FROM provider_execution_profiles profile
JOIN LATERAL (
    SELECT route.selection_strategy, route.quota_freshness_ms,
           route.unknown_quota_policy
    FROM provider_route_members member
    JOIN provider_routes route
      ON route.route_id = member.route_id
     AND route.revision = member.route_revision
    WHERE member.provider_account_id = profile.provider_account_id
      AND member.operation_id = 'videos.generations'
      AND member.command_schema = 'grok-cli.videos.generate.v1'
    ORDER BY route.created_at_ms, route.route_id
    LIMIT 1
) source_route ON TRUE
WHERE profile.provider_id = 'grok-cli'
  AND profile.operation_id = 'videos.generations'
  AND profile.command_schema = 'grok-cli.videos.generate.v2'
ON CONFLICT DO NOTHING;

INSERT INTO provider_route_heads (
    route_id, route_key, provider_id, operation_id, command_schema,
    route_kind, current_revision, state, created_at_ms, updated_at_ms
)
SELECT route.route_id, route.route_key, route.provider_id,
       route.operation_id, route.command_schema, route.route_kind,
       route.revision, 'disabled', route.created_at_ms, route.created_at_ms
FROM provider_routes route
WHERE route.provider_id = 'grok-cli'
  AND route.operation_id = 'videos.generations'
  AND route.command_schema = 'grok-cli.videos.generate.v2'
ON CONFLICT DO NOTHING;

INSERT INTO provider_route_members (
    route_id, route_revision, provider_id, operation_id, command_schema,
    provider_account_id, execution_profile_id, priority, weight, state,
    created_at_ms, minimum_remaining_percent
)
SELECT route.route_id, route.revision, route.provider_id,
       route.operation_id, route.command_schema, profile.provider_account_id,
       profile.execution_profile_id, 0, 100, 'disabled', route.created_at_ms, 0
FROM provider_routes route
JOIN provider_execution_profiles profile
  ON profile.provider_id = route.provider_id
 AND profile.operation_id = route.operation_id
 AND profile.command_schema = route.command_schema
 AND route.route_key = 'account.' || replace(profile.provider_account_id::TEXT, '-', '') || '.grok-video-v2'
WHERE route.provider_id = 'grok-cli'
  AND route.operation_id = 'videos.generations'
  AND route.command_schema = 'grok-cli.videos.generate.v2'
ON CONFLICT DO NOTHING;

INSERT INTO provider_route_model_mappings (
    route_id, route_revision, provider_id, operation_id, command_schema,
    api_profile, public_model_id, provider_model_id, execution_model_id,
    media_kind, created_at_ms
)
SELECT route.route_id, route.revision, route.provider_id,
       route.operation_id, route.command_schema, 'xai-videos-v1',
       'grok-imagine-video-1.5', 'grok-imagine-video-1.5',
       'grok-imagine-video-1.5', 'video', route.created_at_ms
FROM provider_routes route
JOIN provider_models model
  ON model.provider_id = 'grok-cli'
 AND model.model_id = 'grok-imagine-video-1.5'
 AND model.media_kind = 'video'
WHERE route.provider_id = 'grok-cli'
  AND route.operation_id = 'videos.generations'
  AND route.command_schema = 'grok-cli.videos.generate.v2'
ON CONFLICT DO NOTHING;
