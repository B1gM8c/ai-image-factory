-- Publish the built-in Grok Video 1.5 customer price for both the canonical
-- public model and the CLI preview alias. Existing active platform prices
-- always take precedence.

INSERT INTO pricing_surface_contract_revisions (
    contract_key, revision, contract_hash, contract_schema_version,
    api_profile, operation, provider_id, provider_model_id,
    public_model_id, media_kind, service_tier, execution_surface,
    normalizer_key, normalizer_revision, contract_json, created_at_ms
)
VALUES
(
    'grok-cli.videos.generations.pricing-surface:49388adc2d36987c',
    3,
    '691501d655945c9195e88931e040cdd92a773f13d4e3dc19449401b9e0d4c83f',
    2,
    'xai-videos-v1', 'video_generation', 'grok-cli',
    'grok-imagine-video-1.5-preview', 'grok-imagine-video-1.5',
    'video', 'standard', 'provider_cli',
    'grok-cli.videos.generate.v1', 1,
    $contract$
    {"contract":{"api_profiles":["xai-videos-v1"],"command_schema":"grok-cli.videos.generate.v1","constraints":[{"ConditionalPresence":{"cases":[{"forbidden":[],"required":["aspect_ratio"],"selector_values":["grok-imagine-video"]},{"forbidden":[],"required":[],"selector_values":["grok-imagine-video-1.5","grok-imagine-video-1.5-preview","grok-imagine-video-1.5-2026-05-30"]}],"selector":"ProviderModel"}},{"ConditionalEnum":{"cases":[{"allowed_values":["2","3","4","5","6","7"],"selector_values":["grok-imagine-video"]},{"allowed_values":["0","1"],"selector_values":["grok-imagine-video-1.5","grok-imagine-video-1.5-preview","grok-imagine-video-1.5-2026-05-30"]}],"field":"input_image_count","selector":"ProviderModel"}},{"ConditionalPresence":{"cases":[{"forbidden":[],"required":["aspect_ratio"],"selector_values":["0"]},{"forbidden":["aspect_ratio"],"required":[],"selector_values":["1"]},{"forbidden":[],"required":["aspect_ratio"],"selector_values":["2","3","4","5","6","7"]}],"selector":{"Dimension":"input_image_count"}}}],"contract_id":"grok-cli.videos.generations.pricing-surface","contract_version":3,"dimensions":[{"domain":{"Enum":["6","10"]},"key":"duration","required":true},{"domain":{"Enum":["480p","720p"]},"key":"resolution","required":true},{"domain":{"IntegerClosed":{"max":7,"min":0}},"key":"input_image_count","required":true},{"domain":{"Enum":["1:1","16:9","9:16","3:2","2:3"]},"key":"aspect_ratio","required":false}],"media_kind":"video","metering_bases":[{"confidence":"exact","customer_sale_required":true,"metric":"image_input","quantity_source":"request_derived","unit":"image"},{"confidence":"exact","customer_sale_required":true,"metric":"video_requested_second","quantity_source":"request_derived","unit":"second"},{"confidence":"exact","customer_sale_required":false,"metric":"video_output_second","quantity_source":"request_derived","unit":"second"}],"normalizer_key":"grok-cli.videos.generate.v1","normalizer_revision":1,"output_cardinality":{"Fixed":1},"pricing_operation":"video_generation","provider_id":"grok-cli","provider_models":["grok-imagine-video","grok-imagine-video-1.5","grok-imagine-video-1.5-preview","grok-imagine-video-1.5-2026-05-30"],"route_operation":"videos.generations","support":"Supported"},"exact_surface":{"api_profile":"xai-videos-v1","execution_surface":"provider_cli","provider_model_id":"grok-imagine-video-1.5-preview","public_model_id":"grok-imagine-video-1.5","service_tier":"standard"},"schema_version":2}
    $contract$::JSONB,
    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
),
(
    'grok-cli.videos.generations.pricing-surface:7bd88c8c1783c05c',
    3,
    '3cb4343abbd125bf363705376fc9a18e21c7155507106f83ec1a2afe8fd026ce',
    2,
    'xai-videos-v1', 'video_generation', 'grok-cli',
    'grok-imagine-video-1.5-preview', 'grok-imagine-video-1.5-preview',
    'video', 'standard', 'provider_cli',
    'grok-cli.videos.generate.v1', 1,
    $contract$
    {"contract":{"api_profiles":["xai-videos-v1"],"command_schema":"grok-cli.videos.generate.v1","constraints":[{"ConditionalPresence":{"cases":[{"forbidden":[],"required":["aspect_ratio"],"selector_values":["grok-imagine-video"]},{"forbidden":[],"required":[],"selector_values":["grok-imagine-video-1.5","grok-imagine-video-1.5-preview","grok-imagine-video-1.5-2026-05-30"]}],"selector":"ProviderModel"}},{"ConditionalEnum":{"cases":[{"allowed_values":["2","3","4","5","6","7"],"selector_values":["grok-imagine-video"]},{"allowed_values":["0","1"],"selector_values":["grok-imagine-video-1.5","grok-imagine-video-1.5-preview","grok-imagine-video-1.5-2026-05-30"]}],"field":"input_image_count","selector":"ProviderModel"}},{"ConditionalPresence":{"cases":[{"forbidden":[],"required":["aspect_ratio"],"selector_values":["0"]},{"forbidden":["aspect_ratio"],"required":[],"selector_values":["1"]},{"forbidden":[],"required":["aspect_ratio"],"selector_values":["2","3","4","5","6","7"]}],"selector":{"Dimension":"input_image_count"}}}],"contract_id":"grok-cli.videos.generations.pricing-surface","contract_version":3,"dimensions":[{"domain":{"Enum":["6","10"]},"key":"duration","required":true},{"domain":{"Enum":["480p","720p"]},"key":"resolution","required":true},{"domain":{"IntegerClosed":{"max":7,"min":0}},"key":"input_image_count","required":true},{"domain":{"Enum":["1:1","16:9","9:16","3:2","2:3"]},"key":"aspect_ratio","required":false}],"media_kind":"video","metering_bases":[{"confidence":"exact","customer_sale_required":true,"metric":"image_input","quantity_source":"request_derived","unit":"image"},{"confidence":"exact","customer_sale_required":true,"metric":"video_requested_second","quantity_source":"request_derived","unit":"second"},{"confidence":"exact","customer_sale_required":false,"metric":"video_output_second","quantity_source":"request_derived","unit":"second"}],"normalizer_key":"grok-cli.videos.generate.v1","normalizer_revision":1,"output_cardinality":{"Fixed":1},"pricing_operation":"video_generation","provider_id":"grok-cli","provider_models":["grok-imagine-video","grok-imagine-video-1.5","grok-imagine-video-1.5-preview","grok-imagine-video-1.5-2026-05-30"],"route_operation":"videos.generations","support":"Supported"},"exact_surface":{"api_profile":"xai-videos-v1","execution_surface":"provider_cli","provider_model_id":"grok-imagine-video-1.5-preview","public_model_id":"grok-imagine-video-1.5-preview","service_tier":"standard"},"schema_version":2}
    $contract$::JSONB,
    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
)
ON CONFLICT (contract_key, revision) DO NOTHING;

DO $$
DECLARE
    target_book_id UUID;
    target_version_id UUID;
    next_version INTEGER;
    now_ms BIGINT := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
    public_model_name TEXT;
    contract_key_value TEXT;
    contract_hash_value TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('default-grok-video-customer-pricing-v1', 0)
    );

    SELECT price_book_id INTO target_book_id
    FROM price_books
    WHERE purpose = 'customer_sale'
      AND scope_type = 'platform'
      AND currency = 'USD'
      AND state = 'active'
    ORDER BY created_at_ms, price_book_id
    LIMIT 1
    FOR UPDATE;

    IF target_book_id IS NULL THEN
        target_book_id := '790eb133-3f0d-4ce5-b28f-6dab2a520ec5';
        INSERT INTO price_books (
            price_book_id, price_book_key, display_name, purpose,
            scope_type, organization_id, project_id, provider_id,
            currency, state, control_version, created_at_ms, updated_at_ms
        )
        VALUES (
            target_book_id, 'customer_sale.platform.default',
            'Default customer pricing', 'customer_sale', 'platform',
            NULL, NULL, NULL, 'USD', 'active', 1, now_ms, now_ms
        );
    END IF;

    FOR public_model_name, contract_key_value, contract_hash_value,
        target_version_id
    IN VALUES
      (
        'grok-imagine-video-1.5',
        'grok-cli.videos.generations.pricing-surface:49388adc2d36987c',
        '691501d655945c9195e88931e040cdd92a773f13d4e3dc19449401b9e0d4c83f',
        '97861e28-1793-4ee0-9c97-4e181910f1ce'::UUID
      ),
      (
        'grok-imagine-video-1.5-preview',
        'grok-cli.videos.generations.pricing-surface:7bd88c8c1783c05c',
        '3cb4343abbd125bf363705376fc9a18e21c7155507106f83ec1a2afe8fd026ce',
        '30401d9d-416b-4b3a-aca3-8856a26164a3'::UUID
      )
    LOOP
        IF EXISTS (
            SELECT 1
            FROM price_books book
            JOIN price_book_versions version
              ON version.price_book_id = book.price_book_id
            WHERE book.purpose = 'customer_sale'
              AND book.scope_type = 'platform'
              AND book.currency = 'USD'
              AND book.state = 'active'
              AND version.state = 'active'
              AND version.billing_mode = 'customer_rate'
              AND version.api_profile IN ('*', 'xai-videos-v1')
              AND version.operation IN ('*', 'video_generation')
              AND (version.provider_id IS NULL OR version.provider_id = 'grok-cli')
              AND (
                  version.provider_model_id IS NULL
                  OR version.provider_model_id = 'grok-imagine-video-1.5-preview'
              )
              AND version.public_model_id IN ('*', public_model_name)
              AND version.media_kind = 'video'
              AND version.service_tier IN ('*', 'standard')
              AND version.execution_surface = 'provider_cli'
              AND version.effective_from_ms <= now_ms
              AND (
                  version.effective_until_ms IS NULL
                  OR version.effective_until_ms > now_ms
              )
        ) THEN
            CONTINUE;
        END IF;

        SELECT COALESCE(MAX(version), 0) + 1 INTO next_version
        FROM price_book_versions
        WHERE price_book_id = target_book_id;

        INSERT INTO price_book_versions (
            price_book_version_id, price_book_id, version, api_profile,
            operation, provider_id, provider_model_id, public_model_id,
            media_kind, service_tier, execution_surface, billing_mode,
            is_free, state, effective_from_ms, effective_until_ms,
            source_kind, source_url, source_checked_at_ms, notes,
            control_version, created_at_ms, updated_at_ms
        )
        VALUES (
            target_version_id, target_book_id, next_version,
            'xai-videos-v1', 'video_generation', 'grok-cli',
            'grok-imagine-video-1.5-preview', public_model_name,
            'video', 'standard', 'provider_cli', 'customer_rate',
            FALSE, 'draft', now_ms, NULL, 'official_document',
            'https://docs.x.ai/developers/models', 1783555200000,
            'Built-in Grok Imagine Video 1.5 customer price',
            1, now_ms, now_ms
        );

        INSERT INTO price_components (
            price_component_id, price_book_version_id, component_key,
            metric, unit, unit_size, unit_price_micros, outcome,
            quantity_source, required_confidence, rounding_mode,
            dimensions_json, created_at_ms
        )
        VALUES
          (md5(target_version_id::TEXT || ':image-input-succeeded')::UUID,
           target_version_id, 'image-input-succeeded',
           'image_input', 'image', 1, 10000, 'succeeded',
           'request_derived', 'exact', 'exact', '{}'::JSONB, now_ms),
          (md5(target_version_id::TEXT || ':image-input-failed')::UUID,
           target_version_id, 'image-input-failed',
           'image_input', 'image', 1, 0, 'failed',
           'request_derived', 'exact', 'exact', '{}'::JSONB, now_ms),
          (md5(target_version_id::TEXT || ':image-input-no-effect')::UUID,
           target_version_id, 'image-input-no-effect',
           'image_input', 'image', 1, 0, 'no_effect',
           'request_derived', 'exact', 'exact', '{}'::JSONB, now_ms),
          (md5(target_version_id::TEXT || ':video-second-succeeded-480p')::UUID,
           target_version_id, 'video-second-succeeded-480p',
           'video_requested_second', 'second', 1, 80000, 'succeeded',
           'request_derived', 'exact', 'exact', '{}'::JSONB, now_ms),
          (md5(target_version_id::TEXT || ':video-second-succeeded-720p')::UUID,
           target_version_id, 'video-second-succeeded-720p',
           'video_requested_second', 'second', 1, 140000, 'succeeded',
           'request_derived', 'exact', 'exact',
           '{"resolution":"720p"}'::JSONB, now_ms),
          (md5(target_version_id::TEXT || ':video-second-failed')::UUID,
           target_version_id, 'video-second-failed',
           'video_requested_second', 'second', 1, 0, 'failed',
           'request_derived', 'exact', 'exact', '{}'::JSONB, now_ms),
          (md5(target_version_id::TEXT || ':video-second-no-effect')::UUID,
           target_version_id, 'video-second-no-effect',
           'video_requested_second', 'second', 1, 0, 'no_effect',
           'request_derived', 'exact', 'exact', '{}'::JSONB, now_ms);

        INSERT INTO price_book_version_surface_contract_bindings (
            price_book_version_id, contract_key, contract_revision,
            contract_hash, bound_at_ms
        )
        VALUES (
            target_version_id, contract_key_value, 3,
            contract_hash_value, now_ms
        );

        UPDATE price_book_versions
        SET state = 'active', control_version = control_version + 1,
            updated_at_ms = now_ms
        WHERE price_book_version_id = target_version_id;
    END LOOP;
END;
$$;
