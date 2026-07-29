CREATE OR REPLACE FUNCTION validate_customer_quote_source() RETURNS TRIGGER AS $$
DECLARE
    source_book price_books%ROWTYPE;
    source_version price_book_versions%ROWTYPE;
BEGIN
    SELECT * INTO STRICT source_book
    FROM price_books
    WHERE price_book_id = NEW.price_book_id;

    SELECT * INTO STRICT source_version
    FROM price_book_versions
    WHERE price_book_version_id = NEW.price_book_version_id
      AND price_book_id = NEW.price_book_id;

    IF source_book.state <> 'active'
       OR source_book.purpose <> 'customer_sale'
       OR source_book.currency <> NEW.currency
       OR (
           source_book.provider_id IS NOT NULL
           AND source_book.provider_id IS DISTINCT FROM NEW.provider_id
       )
       OR source_version.state <> 'active'
       OR source_version.billing_mode <> 'customer_rate'
       OR source_version.billing_mode <> NEW.billing_mode
       OR source_version.is_free <> NEW.is_free
       OR source_version.effective_from_ms > NEW.created_at_ms
       OR (
           source_version.effective_until_ms IS NOT NULL
           AND NEW.created_at_ms >= source_version.effective_until_ms
       )
       OR NOT (
           source_version.api_profile IN ('*', NEW.api_profile)
           OR EXISTS (
               SELECT 1
               FROM api_profile_pricing_aliases alias
               WHERE alias.api_profile = NEW.api_profile
                 AND alias.pricing_api_profile = source_version.api_profile
           )
       )
       OR source_version.operation NOT IN ('*', NEW.operation)
       OR (
           source_version.provider_id IS NOT NULL
           AND source_version.provider_id IS DISTINCT FROM NEW.provider_id
       )
       OR (
           source_version.provider_model_id IS NOT NULL
           AND source_version.provider_model_id
               IS DISTINCT FROM NEW.provider_model_id
       )
       OR source_version.public_model_id NOT IN ('*', NEW.public_model_id)
       OR source_version.media_kind <> NEW.media_kind
       OR source_version.service_tier NOT IN ('*', NEW.service_tier)
       OR source_version.execution_surface <> NEW.execution_surface THEN
        RAISE EXCEPTION 'customer quote does not match its published source'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
