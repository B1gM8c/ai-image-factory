CREATE EXTENSION IF NOT EXISTS btree_gist;

DROP INDEX IF EXISTS price_book_versions_active_match_uidx;

ALTER TABLE price_book_versions
ADD CONSTRAINT price_book_versions_active_period_excl
EXCLUDE USING gist (
    price_book_id WITH =,
    api_profile WITH =,
    operation WITH =,
    (COALESCE(provider_id, '')) WITH =,
    (COALESCE(provider_model_id, '')) WITH =,
    public_model_id WITH =,
    media_kind WITH =,
    service_tier WITH =,
    execution_surface WITH =,
    billing_mode WITH =,
    int8range(effective_from_ms, effective_until_ms, '[)') WITH &&
)
WHERE (state = 'active');

CREATE INDEX price_book_versions_active_match_idx
    ON price_book_versions (
        price_book_id, api_profile, operation,
        COALESCE(provider_id, ''), COALESCE(provider_model_id, ''),
        public_model_id, media_kind, service_tier,
        execution_surface, billing_mode,
        effective_from_ms, effective_until_ms
    )
    WHERE state = 'active';

CREATE OR REPLACE FUNCTION preserve_price_book_version() RETURNS TRIGGER AS $$
DECLARE
    book_purpose TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.state <> 'draft' THEN
            RAISE EXCEPTION 'published price book version is immutable'
                USING ERRCODE = '55000';
        END IF;
        RETURN OLD;
    END IF;

    IF OLD.state = 'draft' THEN
        IF NEW.state NOT IN ('draft', 'active') THEN
            RAISE EXCEPTION 'invalid price book version transition'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.state = 'active' THEN
            SELECT purpose INTO STRICT book_purpose
            FROM price_books
            WHERE price_book_id = NEW.price_book_id;

            IF NOT (
                (book_purpose = 'customer_sale'
                    AND NEW.billing_mode = 'customer_rate')
                OR (book_purpose = 'provider_actual'
                    AND NEW.billing_mode IN ('provider_reported', 'contract_rate'))
                OR (book_purpose = 'provider_estimated'
                    AND NEW.billing_mode IN ('published_rate', 'contract_rate'))
                OR (book_purpose = 'provider_allocated'
                    AND NEW.billing_mode IN (
                        'subscription_allocation', 'membership_points'
                    ))
                OR (book_purpose = 'provider_benchmark'
                    AND NEW.billing_mode = 'published_rate')
            ) THEN
                RAISE EXCEPTION 'price book purpose and billing mode are incompatible'
                    USING ERRCODE = '23514';
            END IF;

            IF NEW.billing_mode = 'provider_reported' THEN
                IF NEW.is_free OR EXISTS (
                    SELECT 1 FROM price_components
                    WHERE price_book_version_id = NEW.price_book_version_id
                ) THEN
                    RAISE EXCEPTION 'provider-reported cost versions cannot contain rates'
                        USING ERRCODE = '23514';
                END IF;
            ELSE
                IF NOT EXISTS (
                    SELECT 1 FROM price_components
                    WHERE price_book_version_id = NEW.price_book_version_id
                ) THEN
                    RAISE EXCEPTION 'priced versions require components'
                        USING ERRCODE = '23514';
                END IF;
                IF NEW.is_free AND EXISTS (
                    SELECT 1 FROM price_components
                    WHERE price_book_version_id = NEW.price_book_version_id
                      AND unit_price_micros <> 0
                ) THEN
                    RAISE EXCEPTION 'free versions require zero-valued components'
                        USING ERRCODE = '23514';
                END IF;
                IF NOT NEW.is_free AND NOT EXISTS (
                    SELECT 1 FROM price_components
                    WHERE price_book_version_id = NEW.price_book_version_id
                      AND unit_price_micros > 0
                ) THEN
                    RAISE EXCEPTION 'paid versions require a positive component'
                        USING ERRCODE = '23514';
                END IF;
                IF book_purpose = 'provider_actual'
                   AND EXISTS (
                       SELECT 1 FROM price_components
                       WHERE price_book_version_id = NEW.price_book_version_id
                         AND (
                             quantity_source <> 'provider_reported'
                             OR required_confidence <> 'exact'
                         )
                   ) THEN
                    RAISE EXCEPTION 'provider actual rates require exact provider usage'
                        USING ERRCODE = '23514';
                END IF;
            END IF;
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.state = 'active'
       AND NEW.state IN ('active', 'retired')
       AND ROW(
           OLD.price_book_id, OLD.version, OLD.api_profile, OLD.operation,
           OLD.provider_id, OLD.provider_model_id, OLD.public_model_id,
           OLD.media_kind, OLD.service_tier, OLD.execution_surface,
           OLD.billing_mode, OLD.is_free, OLD.effective_from_ms,
           OLD.source_kind, OLD.source_url, OLD.source_checked_at_ms,
           OLD.notes, OLD.created_at_ms
       ) IS NOT DISTINCT FROM ROW(
           NEW.price_book_id, NEW.version, NEW.api_profile, NEW.operation,
           NEW.provider_id, NEW.provider_model_id, NEW.public_model_id,
           NEW.media_kind, NEW.service_tier, NEW.execution_surface,
           NEW.billing_mode, NEW.is_free, NEW.effective_from_ms,
           NEW.source_kind, NEW.source_url, NEW.source_checked_at_ms,
           NEW.notes, NEW.created_at_ms
       )
       AND (
           (
               NEW.state = 'active'
               AND (
                   NEW.effective_until_ms IS NULL
                   OR NEW.effective_until_ms > NEW.effective_from_ms
               )
           )
           OR (
               NEW.state = 'retired'
               AND NEW.effective_until_ms IS NOT NULL
               AND NEW.effective_until_ms > NEW.effective_from_ms
           )
       ) THEN
        RETURN NEW;
    END IF;

    IF ROW(OLD.*) IS DISTINCT FROM ROW(NEW.*) THEN
        RAISE EXCEPTION 'published price book version is immutable'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
