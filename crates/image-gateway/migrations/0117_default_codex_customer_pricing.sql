-- Keep the built-in Codex image surfaces usable without a separate pricing
-- setup step. Existing active platform prices always take precedence.

INSERT INTO pricing_surface_contract_revisions (
    contract_key, revision, contract_hash, contract_schema_version,
    api_profile, operation, provider_id, provider_model_id,
    public_model_id, media_kind, service_tier, execution_surface,
    normalizer_key, normalizer_revision, contract_json, created_at_ms
)
VALUES
(
    'openai-codex.images.generations.pricing-surface:564f65862fd2c138',
    2,
    '9a965e38530f45ac0f56adaebff2c14c05fc8e5163014c626de16bf5695d18d0',
    2,
    'openai-images-v1', 'generation', 'openai-codex', 'gpt-image-2',
    'gpt-image-2', 'image', 'standard', 'provider_cli',
    'openai.images.generation.v1', 1,
    $contract$
    {
      "contract": {
        "support": "Supported",
        "dimensions": [
          {"key": "quality", "domain": {"Enum": ["auto", "low", "medium", "high"]}, "required": true},
          {"key": "size", "domain": {"StringPredicate": "GptImage2SizeV1"}, "required": true}
        ],
        "media_kind": "image",
        "constraints": [],
        "contract_id": "openai-codex.images.generations.pricing-surface",
        "provider_id": "openai-codex",
        "api_profiles": ["openai-images-v1"],
        "command_schema": "openai.images.generation.v1",
        "metering_bases": [
          {"unit": "image", "metric": "image_output", "confidence": "exact", "quantity_source": "request_derived", "customer_sale_required": true},
          {"unit": "token", "metric": "image_output_token", "confidence": "estimated", "quantity_source": "official_lookup", "customer_sale_required": false}
        ],
        "normalizer_key": "openai.images.generation.v1",
        "provider_models": ["gpt-image-2", "gpt-image-2-2026-04-21"],
        "route_operation": "images.generations",
        "contract_version": 2,
        "pricing_operation": "generation",
        "output_cardinality": {"ClosedRange": {"max": 10, "min": 1}},
        "normalizer_revision": 1
      },
      "exact_surface": {
        "api_profile": "openai-images-v1",
        "service_tier": "standard",
        "public_model_id": "gpt-image-2",
        "execution_surface": "provider_cli",
        "provider_model_id": "gpt-image-2"
      },
      "schema_version": 2
    }
    $contract$::JSONB,
    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
),
(
    'openai-codex.images.edits.pricing-surface:564f65862fd2c138',
    2,
    'd6c7704813517a5f17e2994f46827a40a66c3cda7299b410a09d8ab69c0cc169',
    2,
    'openai-images-v1', 'edit', 'openai-codex', 'gpt-image-2',
    'gpt-image-2', 'image', 'standard', 'provider_cli',
    'openai.images.edit.v1', 1,
    $contract$
    {
      "contract": {
        "support": "Supported",
        "dimensions": [
          {"key": "quality", "domain": {"Enum": ["auto", "low", "medium", "high"]}, "required": true},
          {"key": "size", "domain": {"StringPredicate": "GptImage2SizeV1"}, "required": true}
        ],
        "media_kind": "image",
        "constraints": [],
        "contract_id": "openai-codex.images.edits.pricing-surface",
        "provider_id": "openai-codex",
        "api_profiles": ["openai-images-v1"],
        "command_schema": "openai.images.edit.v1",
        "metering_bases": [
          {"unit": "image", "metric": "image_output", "confidence": "exact", "quantity_source": "request_derived", "customer_sale_required": true},
          {"unit": "token", "metric": "image_output_token", "confidence": "estimated", "quantity_source": "official_lookup", "customer_sale_required": false}
        ],
        "normalizer_key": "openai.images.edit.v1",
        "provider_models": ["gpt-image-2", "gpt-image-2-2026-04-21"],
        "route_operation": "images.edits",
        "contract_version": 2,
        "pricing_operation": "edit",
        "output_cardinality": {"ClosedRange": {"max": 10, "min": 1}},
        "normalizer_revision": 1
      },
      "exact_surface": {
        "api_profile": "openai-images-v1",
        "service_tier": "standard",
        "public_model_id": "gpt-image-2",
        "execution_surface": "provider_cli",
        "provider_model_id": "gpt-image-2"
      },
      "schema_version": 2
    }
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
    operation_name TEXT;
    contract_key_value TEXT;
    contract_hash_value TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('default-codex-customer-pricing-v1', 0)
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
        target_book_id := '53fba93d-61f8-4507-bb9c-9365fa8d1117';
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

    FOR operation_name, contract_key_value, contract_hash_value IN
        VALUES
          (
            'generation',
            'openai-codex.images.generations.pricing-surface:564f65862fd2c138',
            '9a965e38530f45ac0f56adaebff2c14c05fc8e5163014c626de16bf5695d18d0'
          ),
          (
            'edit',
            'openai-codex.images.edits.pricing-surface:564f65862fd2c138',
            'd6c7704813517a5f17e2994f46827a40a66c3cda7299b410a09d8ab69c0cc169'
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
              AND version.api_profile IN ('*', 'openai-images-v1')
              AND version.operation IN ('*', operation_name)
              AND (
                  version.provider_id IS NULL
                  OR version.provider_id = 'openai-codex'
              )
              AND (
                  version.provider_model_id IS NULL
                  OR version.provider_model_id = 'gpt-image-2'
              )
              AND version.public_model_id IN ('*', 'gpt-image-2')
              AND version.media_kind = 'image'
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

        target_version_id := CASE operation_name
            WHEN 'generation' THEN 'd7734203-0a26-4cc0-8e50-3cc7b7d55117'::UUID
            ELSE '48e26e62-8680-443e-bce3-13077ba25117'::UUID
        END;

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
            'openai-images-v1', operation_name, 'openai-codex',
            'gpt-image-2', 'gpt-image-2', 'image', 'standard',
            'provider_cli', 'customer_rate', FALSE, 'draft', now_ms,
            NULL, 'imported', NULL, NULL,
            'Built-in default customer price for Codex image output',
            1, now_ms, now_ms
        );

        INSERT INTO price_components (
            price_component_id, price_book_version_id, component_key,
            metric, unit, unit_size, unit_price_micros, outcome,
            quantity_source, required_confidence, rounding_mode,
            dimensions_json, created_at_ms
        )
        VALUES
          (
            CASE operation_name
              WHEN 'generation' THEN '830a57f9-e68c-4bbb-8880-14b0aab05117'::UUID
              ELSE '11e6959f-5736-4651-ad86-07c80f4f5117'::UUID
            END,
            target_version_id, 'image-output-succeeded', 'image_output',
            'image', 1, 40000, 'succeeded', 'request_derived', 'exact',
            'exact', '{}'::JSONB, now_ms
          ),
          (
            CASE operation_name
              WHEN 'generation' THEN 'b3c6ea2a-6e32-4a16-8bac-624ddc7c5117'::UUID
              ELSE '599099e2-e872-4578-b338-24bc69925117'::UUID
            END,
            target_version_id, 'image-output-failed', 'image_output',
            'image', 1, 0, 'failed', 'request_derived', 'exact',
            'exact', '{}'::JSONB, now_ms
          ),
          (
            CASE operation_name
              WHEN 'generation' THEN 'e6970470-8971-4227-996c-7347c22d5117'::UUID
              ELSE '4a6ec576-cf34-454d-b634-3d791a1f5117'::UUID
            END,
            target_version_id, 'image-output-no-effect', 'image_output',
            'image', 1, 0, 'no_effect', 'request_derived', 'exact',
            'exact', '{}'::JSONB, now_ms
          );

        INSERT INTO price_book_version_surface_contract_bindings (
            price_book_version_id, contract_key, contract_revision,
            contract_hash, bound_at_ms
        )
        VALUES (
            target_version_id, contract_key_value, 2,
            contract_hash_value, now_ms
        );

        UPDATE price_book_versions
        SET state = 'active', control_version = control_version + 1,
            updated_at_ms = now_ms
        WHERE price_book_version_id = target_version_id;
    END LOOP;
END;
$$;
