-- Keep the built-in Grok image surfaces usable without a separate pricing
-- setup step. Existing active platform prices always take precedence.

INSERT INTO pricing_surface_contract_revisions (
    contract_key, revision, contract_hash, contract_schema_version,
    api_profile, operation, provider_id, provider_model_id,
    public_model_id, media_kind, service_tier, execution_surface,
    normalizer_key, normalizer_revision, contract_json, created_at_ms
)
VALUES
(
    'grok-cli.images.generations.pricing-surface:7fcd536e6620b752',
    2,
    '18b99fb87e73e939708986ac7720abd157854aff3167e9c1db8d58f4069822cb',
    2,
    'xai-images-v1', 'generation', 'grok-cli',
    'grok-imagine-image-quality', 'grok-imagine-image-quality',
    'image', 'standard', 'provider_cli',
    'grok-cli.images.generate.v1', 1,
    $contract$
    {
      "contract": {
        "api_profiles": ["xai-images-v1"],
        "command_schema": "grok-cli.images.generate.v1",
        "constraints": [],
        "contract_id": "grok-cli.images.generations.pricing-surface",
        "contract_version": 2,
        "dimensions": [
          {
            "domain": {"Enum": ["auto", "1:1", "3:4", "4:3", "9:16", "16:9", "2:3", "3:2", "9:19.5", "19.5:9", "9:20", "20:9", "1:2", "2:1"]},
            "key": "aspect_ratio",
            "required": true
          },
          {
            "domain": {"Enum": ["1k"]},
            "key": "resolution",
            "required": true
          }
        ],
        "media_kind": "image",
        "metering_bases": [
          {"confidence": "exact", "customer_sale_required": true, "metric": "image_output", "quantity_source": "request_derived", "unit": "image"}
        ],
        "normalizer_key": "grok-cli.images.generate.v1",
        "normalizer_revision": 1,
        "output_cardinality": {"Fixed": 1},
        "pricing_operation": "generation",
        "provider_id": "grok-cli",
        "provider_models": ["grok-imagine-image", "grok-imagine-image-quality"],
        "route_operation": "images.generations",
        "support": "Supported"
      },
      "exact_surface": {
        "api_profile": "xai-images-v1",
        "execution_surface": "provider_cli",
        "provider_model_id": "grok-imagine-image-quality",
        "public_model_id": "grok-imagine-image-quality",
        "service_tier": "standard"
      },
      "schema_version": 2
    }
    $contract$::JSONB,
    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
),
(
    'grok-cli.images.generations.pricing-surface:1862e9bd258bae42',
    2,
    'faa7e03bf5f74edf5094d2657c005a77186c4a72b80a3260d5570dd52681e8f8',
    2,
    'xai-images-v1', 'generation', 'grok-cli',
    'grok-imagine-image', 'grok-imagine-image',
    'image', 'standard', 'provider_cli',
    'grok-cli.images.generate.v1', 1,
    $contract$
    {
      "contract": {
        "api_profiles": ["xai-images-v1"],
        "command_schema": "grok-cli.images.generate.v1",
        "constraints": [],
        "contract_id": "grok-cli.images.generations.pricing-surface",
        "contract_version": 2,
        "dimensions": [
          {
            "domain": {"Enum": ["auto", "1:1", "3:4", "4:3", "9:16", "16:9", "2:3", "3:2", "9:19.5", "19.5:9", "9:20", "20:9", "1:2", "2:1"]},
            "key": "aspect_ratio",
            "required": true
          },
          {
            "domain": {"Enum": ["1k"]},
            "key": "resolution",
            "required": true
          }
        ],
        "media_kind": "image",
        "metering_bases": [
          {"confidence": "exact", "customer_sale_required": true, "metric": "image_output", "quantity_source": "request_derived", "unit": "image"}
        ],
        "normalizer_key": "grok-cli.images.generate.v1",
        "normalizer_revision": 1,
        "output_cardinality": {"Fixed": 1},
        "pricing_operation": "generation",
        "provider_id": "grok-cli",
        "provider_models": ["grok-imagine-image", "grok-imagine-image-quality"],
        "route_operation": "images.generations",
        "support": "Supported"
      },
      "exact_surface": {
        "api_profile": "xai-images-v1",
        "execution_surface": "provider_cli",
        "provider_model_id": "grok-imagine-image",
        "public_model_id": "grok-imagine-image",
        "service_tier": "standard"
      },
      "schema_version": 2
    }
    $contract$::JSONB,
    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
),
(
    'grok-cli.images.edits.pricing-surface:7fcd536e6620b752',
    1,
    'fd5f199d36a8a9e5373eed26c301520b491b949ad099004024918cb99b3a2a0d',
    2,
    'xai-images-v1', 'edit', 'grok-cli',
    'grok-imagine-image-quality', 'grok-imagine-image-quality',
    'image', 'standard', 'provider_cli',
    'grok-cli.images.edit.v1', 1,
    $contract$
    {
      "contract": {
        "api_profiles": ["xai-images-v1"],
        "command_schema": "grok-cli.images.edit.v1",
        "constraints": [],
        "contract_id": "grok-cli.images.edits.pricing-surface",
        "contract_version": 1,
        "dimensions": [
          {
            "domain": {"Enum": ["auto", "1:1", "3:4", "4:3", "9:16", "16:9", "2:3", "3:2", "9:19.5", "19.5:9", "9:20", "20:9", "1:2", "2:1"]},
            "key": "aspect_ratio",
            "required": true
          },
          {
            "domain": {"Enum": ["1k"]},
            "key": "resolution",
            "required": true
          }
        ],
        "media_kind": "image",
        "metering_bases": [
          {"confidence": "exact", "customer_sale_required": true, "metric": "image_output", "quantity_source": "request_derived", "unit": "image"}
        ],
        "normalizer_key": "grok-cli.images.edit.v1",
        "normalizer_revision": 1,
        "output_cardinality": {"Fixed": 1},
        "pricing_operation": "edit",
        "provider_id": "grok-cli",
        "provider_models": ["grok-imagine-image-quality"],
        "route_operation": "images.edits",
        "support": "Supported"
      },
      "exact_surface": {
        "api_profile": "xai-images-v1",
        "execution_surface": "provider_cli",
        "provider_model_id": "grok-imagine-image-quality",
        "public_model_id": "grok-imagine-image-quality",
        "service_tier": "standard"
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
    model_name TEXT;
    unit_price BIGINT;
    contract_key_value TEXT;
    contract_revision_value BIGINT;
    contract_hash_value TEXT;
    version_id_value UUID;
    succeeded_component_id UUID;
    failed_component_id UUID;
    no_effect_component_id UUID;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('default-grok-customer-pricing-v1', 0)
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
        target_book_id := '0c2f124b-a37e-48f4-a890-2c64c5d90118';
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

    FOR operation_name, model_name, unit_price,
        contract_key_value, contract_revision_value, contract_hash_value,
        version_id_value, succeeded_component_id, failed_component_id,
        no_effect_component_id
    IN VALUES
      (
        'generation', 'grok-imagine-image-quality', 50000::BIGINT,
        'grok-cli.images.generations.pricing-surface:7fcd536e6620b752',
        2::BIGINT,
        '18b99fb87e73e939708986ac7720abd157854aff3167e9c1db8d58f4069822cb',
        '78934743-1e57-4878-a47f-e03c98a80118'::UUID,
        'e668e42f-e264-4e27-84e8-62b00d7c0118'::UUID,
        '79bd0a67-96ae-4559-98b3-d6c8652b0118'::UUID,
        'de4a066f-8104-4de0-b4e2-f4781f430118'::UUID
      ),
      (
        'generation', 'grok-imagine-image', 20000::BIGINT,
        'grok-cli.images.generations.pricing-surface:1862e9bd258bae42',
        2::BIGINT,
        'faa7e03bf5f74edf5094d2657c005a77186c4a72b80a3260d5570dd52681e8f8',
        '51ef86e8-a837-4f76-92eb-1db343fb0118'::UUID,
        'b660f69d-80f0-4f04-89aa-d1591b9b0118'::UUID,
        '90864a63-deaa-463a-ae40-4fa3e9890118'::UUID,
        '603e84af-08ac-4463-bf08-c9e8bfca0118'::UUID
      ),
      (
        'edit', 'grok-imagine-image-quality', 50000::BIGINT,
        'grok-cli.images.edits.pricing-surface:7fcd536e6620b752',
        1::BIGINT,
        'fd5f199d36a8a9e5373eed26c301520b491b949ad099004024918cb99b3a2a0d',
        'a32baf31-ad34-4a27-b9db-300bb8e80118'::UUID,
        '6d2073ef-7b84-48d8-8de4-e569d3ec0118'::UUID,
        '4268fe91-59fd-475e-9a11-4acde7470118'::UUID,
        'b650d205-1d79-429d-8a21-838bc9dc0118'::UUID
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
              AND version.api_profile IN ('*', 'xai-images-v1')
              AND version.operation IN ('*', operation_name)
              AND (version.provider_id IS NULL OR version.provider_id = 'grok-cli')
              AND (version.provider_model_id IS NULL OR version.provider_model_id = model_name)
              AND version.public_model_id IN ('*', model_name)
              AND version.media_kind = 'image'
              AND version.service_tier IN ('*', 'standard')
              AND version.execution_surface = 'provider_cli'
              AND version.effective_from_ms <= now_ms
              AND (version.effective_until_ms IS NULL OR version.effective_until_ms > now_ms)
        ) THEN
            CONTINUE;
        END IF;

        SELECT COALESCE(MAX(version), 0) + 1 INTO next_version
        FROM price_book_versions
        WHERE price_book_id = target_book_id;

        target_version_id := version_id_value;
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
            'xai-images-v1', operation_name, 'grok-cli', model_name,
            model_name, 'image', 'standard', 'provider_cli',
            'customer_rate', FALSE, 'draft', now_ms, NULL,
            'official_document', 'https://docs.x.ai/developers/pricing',
            1784822400000,
            'Built-in default customer price for Grok image output',
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
            succeeded_component_id, target_version_id,
            'image-output-succeeded', 'image_output', 'image', 1,
            unit_price, 'succeeded', 'request_derived', 'exact',
            'exact', '{}'::JSONB, now_ms
          ),
          (
            failed_component_id, target_version_id,
            'image-output-failed', 'image_output', 'image', 1,
            0, 'failed', 'request_derived', 'exact',
            'exact', '{}'::JSONB, now_ms
          ),
          (
            no_effect_component_id, target_version_id,
            'image-output-no-effect', 'image_output', 'image', 1,
            0, 'no_effect', 'request_derived', 'exact',
            'exact', '{}'::JSONB, now_ms
          );

        INSERT INTO price_book_version_surface_contract_bindings (
            price_book_version_id, contract_key, contract_revision,
            contract_hash, bound_at_ms
        )
        VALUES (
            target_version_id, contract_key_value,
            contract_revision_value, contract_hash_value, now_ms
        );

        UPDATE price_book_versions
        SET state = 'active', control_version = control_version + 1,
            updated_at_ms = now_ms
        WHERE price_book_version_id = target_version_id;
    END LOOP;
END;
$$;
