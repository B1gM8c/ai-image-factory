-- Fractional Dreamina multiframe durations retain their exact requested
-- milliseconds in V4 customer pricing. Existing second-based quota and facts
-- remain unchanged; no prices or historical economic rows are rewritten.
DO $migration$
DECLARE
    table_name TEXT;
    check_name TEXT;
    old_expression TEXT;
    dimension TEXT;
    new_value TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'price_components', 'provider_usage_facts', 'customer_price_quote_lines'
    ] LOOP
        FOREACH dimension IN ARRAY ARRAY['metric', 'unit'] LOOP
            check_name := table_name || '_' || dimension || '_check';
            SELECT pg_get_expr(constraint_row.conbin, constraint_row.conrelid)
            INTO STRICT old_expression
            FROM pg_constraint constraint_row
            WHERE constraint_row.conrelid = table_name::REGCLASS
              AND constraint_row.conname = check_name
              AND constraint_row.contype = 'c';
            new_value := CASE dimension
                WHEN 'metric' THEN 'video_requested_millisecond'
                ELSE 'millisecond'
            END;
            EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', table_name, check_name);
            EXECUTE format(
                'ALTER TABLE %I ADD CONSTRAINT %I CHECK ((%s) OR %I = %L)',
                table_name, check_name, old_expression, dimension, new_value
            );
        END LOOP;

        IF table_name <> 'customer_price_quote_lines' THEN
            check_name := table_name || '_check';
            SELECT pg_get_expr(constraint_row.conbin, constraint_row.conrelid)
            INTO STRICT old_expression
            FROM pg_constraint constraint_row
            WHERE constraint_row.conrelid = table_name::REGCLASS
              AND constraint_row.conname = check_name
              AND constraint_row.contype = 'c';
            EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', table_name, check_name);
            EXECUTE format(
                'ALTER TABLE %I ADD CONSTRAINT %I CHECK ((%s) OR (metric = %L AND unit = %L))',
                table_name, check_name, old_expression,
                'video_requested_millisecond', 'millisecond'
            );
        END IF;

        -- The additional unit must not be paired with any existing metric, nor
        -- may the new metric accidentally retain a second-based quantity.
        EXECUTE format(
            'ALTER TABLE %I ADD CONSTRAINT %I CHECK ((metric = %L) = (unit = %L))',
            table_name, table_name || '_millisecond_unit_check',
            'video_requested_millisecond', 'millisecond'
        );
    END LOOP;
END;
$migration$;
