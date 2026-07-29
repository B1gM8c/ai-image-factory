ALTER TABLE customer_price_quote_lines
    ADD COLUMN rate_adjustment_numerator BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN rate_adjustment_denominator BIGINT NOT NULL DEFAULT 1,
    ADD CONSTRAINT customer_price_quote_lines_rate_adjustment_check CHECK (
        (rate_adjustment_numerator, rate_adjustment_denominator)
        IN ((1, 1), (1, 2))
    );

ALTER TABLE customer_price_quote_lines
    DROP CONSTRAINT IF EXISTS customer_price_quote_lines_check,
    DROP CONSTRAINT IF EXISTS customer_price_quote_lines_check1;

CREATE OR REPLACE FUNCTION validate_customer_quote_line_source() RETURNS TRIGGER AS $$
DECLARE
    source_version_id UUID;
    quote_version_id UUID;
    processing_mode TEXT;
    source_component price_components%ROWTYPE;
    adjusted_numerator NUMERIC;
    adjusted_denominator NUMERIC;
    expected_amount NUMERIC;
BEGIN
    SELECT * INTO STRICT source_component
    FROM price_components
    WHERE price_component_id = NEW.price_component_id;

    SELECT price_book_version_id,
           COALESCE(request_dimensions_json ->> 'processing_mode', 'synchronous')
      INTO STRICT quote_version_id, processing_mode
    FROM customer_price_quotes
    WHERE quote_id = NEW.quote_id AND job_id = NEW.job_id;

    source_version_id := source_component.price_book_version_id;
    IF source_version_id <> quote_version_id
       OR ROW(
           NEW.component_key, NEW.metric, NEW.unit, NEW.unit_size,
           NEW.unit_price_micros, NEW.quantity_source,
           NEW.required_confidence, NEW.rounding_mode, NEW.dimensions_json
       ) IS DISTINCT FROM ROW(
           source_component.component_key, source_component.metric,
           source_component.unit, source_component.unit_size,
           source_component.unit_price_micros,
           source_component.quantity_source,
           source_component.required_confidence,
           source_component.rounding_mode,
           source_component.dimensions_json
       )
       OR source_component.outcome NOT IN ('any', NEW.terminal_outcome) THEN
        RAISE EXCEPTION 'customer quote line does not match its published source'
            USING ERRCODE = '23514';
    END IF;

    IF (
        processing_mode = 'batch'
        AND ROW(NEW.rate_adjustment_numerator, NEW.rate_adjustment_denominator)
            IS DISTINCT FROM ROW(1::BIGINT, 2::BIGINT)
    ) OR (
        processing_mode <> 'batch'
        AND ROW(NEW.rate_adjustment_numerator, NEW.rate_adjustment_denominator)
            IS DISTINCT FROM ROW(1::BIGINT, 1::BIGINT)
    ) THEN
        RAISE EXCEPTION 'customer quote rate adjustment does not match processing mode'
            USING ERRCODE = '23514';
    END IF;

    adjusted_numerator :=
        NEW.max_quantity::NUMERIC
        * NEW.unit_price_micros::NUMERIC
        * NEW.rate_adjustment_numerator::NUMERIC;
    adjusted_denominator :=
        NEW.unit_size::NUMERIC
        * NEW.rate_adjustment_denominator::NUMERIC;
    expected_amount := CASE NEW.rounding_mode
        WHEN 'floor' THEN FLOOR(adjusted_numerator / adjusted_denominator)
        WHEN 'ceil' THEN CEIL(adjusted_numerator / adjusted_denominator)
        WHEN 'half_up' THEN
            FLOOR(
                (adjusted_numerator + adjusted_denominator / 2)
                / adjusted_denominator
            )
        WHEN 'exact' THEN adjusted_numerator / adjusted_denominator
    END;

    IF NEW.max_amount_micros::NUMERIC <> expected_amount
       OR (
           NEW.rounding_mode = 'exact'
           AND MOD(adjusted_numerator, adjusted_denominator) <> 0
       ) THEN
        RAISE EXCEPTION 'customer quote line amount does not match its rate adjustment'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_customer_rating_line_source() RETURNS TRIGGER AS $$
DECLARE
    quote_line customer_price_quote_lines%ROWTYPE;
    adjusted_numerator NUMERIC;
    adjusted_denominator NUMERIC;
    expected_amount NUMERIC;
BEGIN
    SELECT * INTO STRICT quote_line
    FROM customer_price_quote_lines
    WHERE quote_line_id = NEW.quote_line_id
      AND quote_id = NEW.quote_id
      AND job_id = NEW.job_id;

    IF NEW.actual_quantity > quote_line.max_quantity THEN
        RAISE EXCEPTION 'customer rating quantity exceeds its quote'
            USING ERRCODE = '23514';
    END IF;

    adjusted_numerator :=
        NEW.actual_quantity::NUMERIC
        * quote_line.unit_price_micros::NUMERIC
        * quote_line.rate_adjustment_numerator::NUMERIC;
    adjusted_denominator :=
        quote_line.unit_size::NUMERIC
        * quote_line.rate_adjustment_denominator::NUMERIC;
    expected_amount := CASE quote_line.rounding_mode
        WHEN 'floor' THEN FLOOR(adjusted_numerator / adjusted_denominator)
        WHEN 'ceil' THEN CEIL(adjusted_numerator / adjusted_denominator)
        WHEN 'half_up' THEN
            FLOOR(
                (adjusted_numerator + adjusted_denominator / 2)
                / adjusted_denominator
            )
        WHEN 'exact' THEN adjusted_numerator / adjusted_denominator
    END;

    IF NEW.amount_micros::NUMERIC <> expected_amount
       OR (
           quote_line.rounding_mode = 'exact'
           AND MOD(adjusted_numerator, adjusted_denominator) <> 0
       ) THEN
        RAISE EXCEPTION 'customer rating amount does not match its quote'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
