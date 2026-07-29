DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM customer_price_quotes) THEN
        RAISE EXCEPTION
            '0065 requires an empty customer_price_quotes table or an explicit request-dimension backfill';
    END IF;
END
$$;

ALTER TABLE customer_price_quotes
    ADD COLUMN request_dimensions_json JSONB NOT NULL
    CHECK (jsonb_typeof(request_dimensions_json) = 'object');
