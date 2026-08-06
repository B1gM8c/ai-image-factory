-- Grok CLI reports the authoritative invocation cost in terminal receipts.
-- These component-free versions bind that native evidence to the provider
-- actual-cost ledger without deriving or overriding the reported amount.

INSERT INTO price_books (
    price_book_id, price_book_key, display_name, purpose,
    scope_type, organization_id, project_id, provider_id,
    currency, state, control_version, created_at_ms, updated_at_ms
)
VALUES (
    '3b19fa97-4835-4a2f-9558-0119a1f00001',
    'provider_actual.grok-cli.reported',
    'Grok CLI provider-reported actual cost',
    'provider_actual', 'platform', NULL, NULL, 'grok-cli',
    'USD', 'active', 1,
    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT,
    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
)
ON CONFLICT (price_book_key) DO NOTHING;

DO $$
DECLARE
    target_book_id UUID;
    target_version_id UUID;
    next_version INTEGER;
    now_ms BIGINT := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
    media_kind_value TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('default-grok-provider-actual-v1', 0)
    );

    SELECT price_book_id INTO target_book_id
    FROM price_books
    WHERE price_book_key = 'provider_actual.grok-cli.reported'
      AND purpose = 'provider_actual'
      AND scope_type = 'platform'
      AND provider_id = 'grok-cli'
      AND currency = 'USD'
      AND state = 'active'
    FOR UPDATE;

    IF target_book_id IS NULL THEN
        RETURN;
    END IF;

    FOREACH media_kind_value IN ARRAY ARRAY['image', 'video']
    LOOP
        IF EXISTS (
            SELECT 1
            FROM price_books book
            JOIN price_book_versions version USING (price_book_id)
            WHERE book.purpose = 'provider_actual'
              AND book.scope_type = 'platform'
              AND book.provider_id = 'grok-cli'
              AND book.currency = 'USD'
              AND book.state = 'active'
              AND version.state = 'active'
              AND version.api_profile = '*'
              AND version.operation = '*'
              AND version.provider_id = 'grok-cli'
              AND version.provider_model_id IS NULL
              AND version.public_model_id = '*'
              AND version.media_kind = media_kind_value
              AND version.service_tier = 'standard'
              AND version.execution_surface = 'provider_cli'
              AND version.billing_mode = 'provider_reported'
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

        target_version_id := CASE media_kind_value
            WHEN 'image' THEN '3b19fa97-4835-4a2f-9558-0119a1f00002'::UUID
            ELSE '3b19fa97-4835-4a2f-9558-0119a1f00003'::UUID
        END;

        INSERT INTO price_book_versions (
            price_book_version_id, price_book_id, version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier,
            execution_surface, billing_mode, is_free, state,
            effective_from_ms, effective_until_ms,
            source_kind, source_url, source_checked_at_ms, notes,
            control_version, created_at_ms, updated_at_ms
        )
        VALUES (
            target_version_id, target_book_id, next_version,
            '*', '*', 'grok-cli', NULL, '*', media_kind_value, 'standard',
            'provider_cli', 'provider_reported', FALSE, 'draft',
            now_ms, NULL, 'imported', NULL, NULL,
            'Uses the authoritative cost reported by the Grok CLI terminal receipt',
            1, now_ms, now_ms
        );

        UPDATE price_book_versions
        SET state = 'active', control_version = control_version + 1,
            updated_at_ms = now_ms
        WHERE price_book_version_id = target_version_id
          AND state = 'draft';
    END LOOP;
END;
$$;
