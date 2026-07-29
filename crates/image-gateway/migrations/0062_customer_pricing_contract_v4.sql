CREATE TABLE customer_price_quotes (
    quote_id UUID PRIMARY KEY,
    job_id UUID NOT NULL UNIQUE,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    price_book_id UUID NOT NULL,
    price_book_version_id UUID NOT NULL,
    api_profile TEXT NOT NULL CHECK (
        char_length(api_profile) BETWEEN 1 AND 128
        AND api_profile !~ '[[:cntrl:]]'
    ),
    operation TEXT NOT NULL CHECK (
        char_length(operation) BETWEEN 1 AND 128
        AND operation !~ '[[:cntrl:]]'
    ),
    provider_id TEXT CHECK (
        provider_id IS NULL
        OR (
            char_length(provider_id) BETWEEN 1 AND 128
            AND provider_id ~ '^[A-Za-z0-9_.-]+$'
        )
    ),
    provider_model_id TEXT CHECK (
        provider_model_id IS NULL
        OR (
            char_length(provider_model_id) BETWEEN 1 AND 255
            AND provider_model_id !~ '[[:cntrl:]]'
        )
    ),
    public_model_id TEXT NOT NULL CHECK (
        char_length(public_model_id) BETWEEN 1 AND 255
        AND public_model_id !~ '[[:cntrl:]]'
    ),
    media_kind TEXT NOT NULL CHECK (media_kind IN ('image', 'video')),
    service_tier TEXT NOT NULL CHECK (
        char_length(service_tier) BETWEEN 1 AND 64
        AND service_tier ~ '^[A-Za-z0-9_.-]+$'
    ),
    execution_surface TEXT NOT NULL CHECK (
        execution_surface IN ('provider_api', 'provider_cli', 'manual_import')
    ),
    billing_mode TEXT NOT NULL CHECK (billing_mode = 'customer_rate'),
    is_free BOOLEAN NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    max_total_micros BIGINT NOT NULL CHECK (max_total_micros >= 0),
    quote_hash TEXT NOT NULL CHECK (quote_hash ~ '^[a-f0-9]{64}$'),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (quote_id, job_id),
    UNIQUE (quote_id, job_id, currency),
    UNIQUE (quote_id, job_id, tenant_id, currency),
    FOREIGN KEY (job_id, tenant_id)
        REFERENCES jobs(job_id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (project_id, tenant_id)
        REFERENCES gateway_projects(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (price_book_version_id, price_book_id)
        REFERENCES price_book_versions(price_book_version_id, price_book_id)
        ON DELETE RESTRICT,
    CHECK (is_free OR max_total_micros > 0),
    CHECK (NOT is_free OR max_total_micros = 0)
);

CREATE INDEX customer_price_quotes_project_created_idx
    ON customer_price_quotes(project_id, created_at_ms DESC, quote_id DESC);

CREATE TABLE customer_price_quote_lines (
    quote_line_id UUID PRIMARY KEY,
    quote_id UUID NOT NULL,
    job_id UUID NOT NULL,
    price_component_id UUID NOT NULL
        REFERENCES price_components(price_component_id) ON DELETE RESTRICT,
    component_key TEXT NOT NULL CHECK (
        char_length(component_key) BETWEEN 1 AND 128
        AND component_key ~ '^[A-Za-z0-9_.:-]+$'
    ),
    partition_key TEXT NOT NULL CHECK (
        char_length(partition_key) BETWEEN 1 AND 128
        AND partition_key !~ '[[:cntrl:]]'
    ),
    terminal_outcome TEXT NOT NULL CHECK (
        terminal_outcome IN ('succeeded', 'failed', 'no_effect')
    ),
    metric TEXT NOT NULL CHECK (
        metric IN (
            'request',
            'image_input',
            'image_output',
            'text_input_token',
            'cached_text_input_token',
            'image_input_token',
            'cached_image_input_token',
            'image_output_token',
            'video_input_token',
            'video_output_token',
            'video_input_second',
            'video_output_second',
            'membership_point'
        )
    ),
    unit TEXT NOT NULL CHECK (
        unit IN ('request', 'image', 'token', 'second', 'point')
    ),
    unit_size BIGINT NOT NULL CHECK (unit_size > 0),
    unit_price_micros BIGINT NOT NULL CHECK (unit_price_micros >= 0),
    quantity_source TEXT NOT NULL CHECK (
        quantity_source IN (
            'provider_reported',
            'request_derived',
            'official_lookup',
            'operator_adjustment'
        )
    ),
    required_confidence TEXT NOT NULL CHECK (
        required_confidence IN ('exact', 'bounded', 'estimated', 'any')
    ),
    rounding_mode TEXT NOT NULL CHECK (
        rounding_mode IN ('ceil', 'floor', 'half_up', 'exact')
    ),
    dimensions_json JSONB NOT NULL CHECK (
        jsonb_typeof(dimensions_json) = 'object'
    ),
    max_quantity BIGINT NOT NULL CHECK (max_quantity > 0),
    max_amount_micros BIGINT NOT NULL CHECK (max_amount_micros >= 0),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (
        quote_id, partition_key, terminal_outcome, price_component_id
    ),
    UNIQUE (quote_line_id, quote_id, job_id),
    FOREIGN KEY (quote_id, job_id)
        REFERENCES customer_price_quotes(quote_id, job_id) ON DELETE RESTRICT,
    CHECK (
        max_amount_micros::NUMERIC =
        CASE rounding_mode
            WHEN 'floor' THEN
                FLOOR(
                    max_quantity::NUMERIC * unit_price_micros::NUMERIC
                    / unit_size::NUMERIC
                )
            WHEN 'ceil' THEN
                CEIL(
                    max_quantity::NUMERIC * unit_price_micros::NUMERIC
                    / unit_size::NUMERIC
                )
            WHEN 'half_up' THEN
                FLOOR(
                    (
                        max_quantity::NUMERIC * unit_price_micros::NUMERIC
                        + unit_size::NUMERIC / 2
                    ) / unit_size::NUMERIC
                )
            WHEN 'exact' THEN
                max_quantity::NUMERIC * unit_price_micros::NUMERIC
                    / unit_size::NUMERIC
        END
    ),
    CHECK (
        rounding_mode <> 'exact'
        OR MOD(
            max_quantity::NUMERIC * unit_price_micros::NUMERIC,
            unit_size::NUMERIC
        ) = 0
    )
);

CREATE INDEX customer_price_quote_lines_quote_idx
    ON customer_price_quote_lines(
        quote_id, partition_key, terminal_outcome, component_key
    );

CREATE TABLE customer_billing_holds (
    hold_id UUID PRIMARY KEY,
    quote_id UUID NOT NULL UNIQUE,
    job_id UUID NOT NULL UNIQUE,
    tenant_id TEXT NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    held_micros BIGINT NOT NULL CHECK (held_micros >= 0),
    captured_micros BIGINT NOT NULL DEFAULT 0 CHECK (captured_micros >= 0),
    released_micros BIGINT NOT NULL DEFAULT 0 CHECK (released_micros >= 0),
    state TEXT NOT NULL CHECK (state IN ('held', 'settled', 'released')),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (quote_id, job_id, tenant_id, currency)
        REFERENCES customer_price_quotes(quote_id, job_id, tenant_id, currency)
        ON DELETE RESTRICT,
    CHECK (
        captured_micros::NUMERIC + released_micros::NUMERIC
            <= held_micros::NUMERIC
    ),
    CHECK (
        (state = 'held')
        OR (
            state = 'settled'
            AND captured_micros::NUMERIC + released_micros::NUMERIC
                = held_micros::NUMERIC
        )
        OR (
            state = 'released'
            AND captured_micros = 0
            AND released_micros = held_micros
        )
    ),
    CHECK (updated_at_ms >= created_at_ms)
);

CREATE TABLE customer_rated_usage (
    rated_usage_id UUID PRIMARY KEY,
    semantic_key TEXT NOT NULL UNIQUE CHECK (
        char_length(semantic_key) BETWEEN 1 AND 512
        AND semantic_key !~ '[[:cntrl:]]'
    ),
    quote_id UUID NOT NULL UNIQUE,
    job_id UUID NOT NULL UNIQUE,
    fact_set_hash TEXT NOT NULL CHECK (fact_set_hash ~ '^[a-f0-9]{64}$'),
    total_amount_micros BIGINT NOT NULL CHECK (total_amount_micros >= 0),
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    rating_hash TEXT NOT NULL CHECK (rating_hash ~ '^[a-f0-9]{64}$'),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (rated_usage_id, quote_id, job_id),
    FOREIGN KEY (quote_id, job_id, currency)
        REFERENCES customer_price_quotes(quote_id, job_id, currency)
        ON DELETE RESTRICT
);

CREATE TABLE customer_rated_usage_lines (
    rated_usage_line_id UUID PRIMARY KEY,
    rated_usage_id UUID NOT NULL,
    quote_id UUID NOT NULL,
    job_id UUID NOT NULL,
    quote_line_id UUID NOT NULL,
    actual_quantity BIGINT NOT NULL CHECK (actual_quantity >= 0),
    amount_micros BIGINT NOT NULL CHECK (amount_micros >= 0),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (rated_usage_id, quote_line_id),
    UNIQUE (rated_usage_line_id, rated_usage_id),
    FOREIGN KEY (rated_usage_id, quote_id, job_id)
        REFERENCES customer_rated_usage(rated_usage_id, quote_id, job_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (quote_line_id, quote_id, job_id)
        REFERENCES customer_price_quote_lines(quote_line_id, quote_id, job_id)
        ON DELETE RESTRICT
);

CREATE TABLE customer_rated_usage_fact_links (
    rated_usage_line_id UUID NOT NULL,
    usage_fact_id UUID NOT NULL UNIQUE
        REFERENCES provider_usage_facts(usage_fact_id) ON DELETE RESTRICT,
    linked_at_ms BIGINT NOT NULL,
    PRIMARY KEY (rated_usage_line_id, usage_fact_id),
    FOREIGN KEY (rated_usage_line_id)
        REFERENCES customer_rated_usage_lines(rated_usage_line_id)
        ON DELETE RESTRICT
);

CREATE FUNCTION validate_customer_quote_source() RETURNS TRIGGER AS $$
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
           source_book.scope_type = 'organization'
           AND source_book.organization_id <> NEW.tenant_id
       )
       OR (
           source_book.scope_type = 'project'
           AND (
               source_book.organization_id <> NEW.tenant_id
               OR source_book.project_id <> NEW.project_id
           )
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
       OR source_version.api_profile NOT IN ('*', NEW.api_profile)
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

CREATE TRIGGER customer_price_quotes_validate_source
BEFORE INSERT ON customer_price_quotes
FOR EACH ROW EXECUTE FUNCTION validate_customer_quote_source();

CREATE FUNCTION validate_customer_quote_line_source() RETURNS TRIGGER AS $$
DECLARE
    source_version_id UUID;
    quote_version_id UUID;
    source_component price_components%ROWTYPE;
BEGIN
    SELECT * INTO STRICT source_component
    FROM price_components
    WHERE price_component_id = NEW.price_component_id;

    SELECT price_book_version_id INTO STRICT quote_version_id
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
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customer_price_quote_lines_validate_source
BEFORE INSERT ON customer_price_quote_lines
FOR EACH ROW EXECUTE FUNCTION validate_customer_quote_line_source();

CREATE FUNCTION validate_customer_billing_hold_source() RETURNS TRIGGER AS $$
DECLARE
    quote_max BIGINT;
BEGIN
    SELECT max_total_micros INTO STRICT quote_max
    FROM customer_price_quotes
    WHERE quote_id = NEW.quote_id
      AND job_id = NEW.job_id
      AND tenant_id = NEW.tenant_id
      AND currency = NEW.currency;

    IF NEW.held_micros <> quote_max
       OR NEW.state <> 'held'
       OR NEW.captured_micros <> 0
       OR NEW.released_micros <> 0
       OR NEW.updated_at_ms <> NEW.created_at_ms THEN
        RAISE EXCEPTION 'customer billing hold does not match its quote'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customer_billing_holds_validate_source
BEFORE INSERT ON customer_billing_holds
FOR EACH ROW EXECUTE FUNCTION validate_customer_billing_hold_source();

CREATE FUNCTION validate_customer_rating_line_source() RETURNS TRIGGER AS $$
DECLARE
    quote_line customer_price_quote_lines%ROWTYPE;
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

    expected_amount := CASE quote_line.rounding_mode
        WHEN 'floor' THEN
            FLOOR(
                NEW.actual_quantity::NUMERIC
                * quote_line.unit_price_micros::NUMERIC
                / quote_line.unit_size::NUMERIC
            )
        WHEN 'ceil' THEN
            CEIL(
                NEW.actual_quantity::NUMERIC
                * quote_line.unit_price_micros::NUMERIC
                / quote_line.unit_size::NUMERIC
            )
        WHEN 'half_up' THEN
            FLOOR(
                (
                    NEW.actual_quantity::NUMERIC
                    * quote_line.unit_price_micros::NUMERIC
                    + quote_line.unit_size::NUMERIC / 2
                ) / quote_line.unit_size::NUMERIC
            )
        WHEN 'exact' THEN
            NEW.actual_quantity::NUMERIC
            * quote_line.unit_price_micros::NUMERIC
            / quote_line.unit_size::NUMERIC
    END;

    IF NEW.amount_micros::NUMERIC <> expected_amount
       OR (
           quote_line.rounding_mode = 'exact'
           AND MOD(
               NEW.actual_quantity::NUMERIC
               * quote_line.unit_price_micros::NUMERIC,
               quote_line.unit_size::NUMERIC
           ) <> 0
       ) THEN
        RAISE EXCEPTION 'customer rating amount does not match its quote'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customer_rated_usage_lines_validate_source
BEFORE INSERT ON customer_rated_usage_lines
FOR EACH ROW EXECUTE FUNCTION validate_customer_rating_line_source();

CREATE FUNCTION validate_customer_rating_fact_link() RETURNS TRIGGER AS $$
DECLARE
    line_job_id UUID;
    fact_job_id UUID;
BEGIN
    SELECT rating_line.job_id INTO STRICT line_job_id
    FROM customer_rated_usage_lines rating_line
    WHERE rating_line.rated_usage_line_id = NEW.rated_usage_line_id;

    SELECT usage.job_id INTO STRICT fact_job_id
    FROM provider_usage_facts usage
    WHERE usage.usage_fact_id = NEW.usage_fact_id;

    IF line_job_id <> fact_job_id THEN
        RAISE EXCEPTION 'customer rating fact belongs to another job'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customer_rated_usage_fact_links_validate_source
BEFORE INSERT ON customer_rated_usage_fact_links
FOR EACH ROW EXECUTE FUNCTION validate_customer_rating_fact_link();

CREATE FUNCTION validate_customer_quote_total() RETURNS TRIGGER AS $$
DECLARE
    target_quote_id UUID := COALESCE(NEW.quote_id, OLD.quote_id);
    expected_total NUMERIC;
    stored_total BIGINT;
    line_count BIGINT;
BEGIN
    SELECT quote.max_total_micros, COUNT(line.quote_line_id)
      INTO STRICT stored_total, line_count
    FROM customer_price_quotes quote
    LEFT JOIN customer_price_quote_lines line
      ON line.quote_id = quote.quote_id
    WHERE quote.quote_id = target_quote_id
    GROUP BY quote.max_total_micros;

    SELECT COALESCE(SUM(partition_max), 0)
      INTO expected_total
    FROM (
        SELECT MAX(outcome_total) AS partition_max
        FROM (
            SELECT partition_key, terminal_outcome,
                   SUM(max_amount_micros::NUMERIC) AS outcome_total
            FROM customer_price_quote_lines
            WHERE quote_id = target_quote_id
            GROUP BY partition_key, terminal_outcome
        ) outcomes
        GROUP BY partition_key
    ) partitions;

    IF line_count = 0 OR expected_total <> stored_total::NUMERIC THEN
        RAISE EXCEPTION 'customer quote total does not match frozen lines'
            USING ERRCODE = '23514';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER customer_price_quotes_validate_total
AFTER INSERT ON customer_price_quotes
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_customer_quote_total();

CREATE CONSTRAINT TRIGGER customer_price_quote_lines_validate_total
AFTER INSERT OR UPDATE OR DELETE ON customer_price_quote_lines
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_customer_quote_total();

CREATE FUNCTION validate_customer_rating_total() RETURNS TRIGGER AS $$
DECLARE
    target_rating_id UUID := COALESCE(NEW.rated_usage_id, OLD.rated_usage_id);
    expected_total NUMERIC;
    stored_total BIGINT;
    quote_max BIGINT;
BEGIN
    SELECT rating.total_amount_micros, quote.max_total_micros
      INTO STRICT stored_total, quote_max
    FROM customer_rated_usage rating
    JOIN customer_price_quotes quote ON quote.quote_id = rating.quote_id
    WHERE rating.rated_usage_id = target_rating_id;

    SELECT COALESCE(SUM(amount_micros::NUMERIC), 0)
      INTO expected_total
    FROM customer_rated_usage_lines
    WHERE rated_usage_id = target_rating_id;

    IF expected_total <> stored_total::NUMERIC OR stored_total > quote_max THEN
        RAISE EXCEPTION 'customer rating total is invalid'
            USING ERRCODE = '23514';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER customer_rated_usage_validate_total
AFTER INSERT ON customer_rated_usage
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_customer_rating_total();

CREATE CONSTRAINT TRIGGER customer_rated_usage_lines_validate_total
AFTER INSERT OR UPDATE OR DELETE ON customer_rated_usage_lines
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_customer_rating_total();

CREATE FUNCTION preserve_customer_billing_hold() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'customer billing holds cannot be deleted'
            USING ERRCODE = '55000';
    END IF;
    IF ROW(
        OLD.hold_id, OLD.quote_id, OLD.job_id, OLD.tenant_id,
        OLD.currency, OLD.held_micros, OLD.created_at_ms
    ) IS DISTINCT FROM ROW(
        NEW.hold_id, NEW.quote_id, NEW.job_id, NEW.tenant_id,
        NEW.currency, NEW.held_micros, NEW.created_at_ms
    )
       OR NEW.captured_micros < OLD.captured_micros
       OR NEW.released_micros < OLD.released_micros
       OR NEW.updated_at_ms < OLD.updated_at_ms
       OR (OLD.state = 'held' AND NEW.state NOT IN ('held', 'settled', 'released'))
       OR (OLD.state <> 'held' AND NEW.state <> OLD.state) THEN
        RAISE EXCEPTION 'invalid customer billing hold transition'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customer_billing_holds_preserve
BEFORE UPDATE OR DELETE ON customer_billing_holds
FOR EACH ROW EXECUTE FUNCTION preserve_customer_billing_hold();

CREATE TRIGGER customer_price_quotes_reject_mutation
BEFORE UPDATE OR DELETE ON customer_price_quotes
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER customer_price_quote_lines_reject_mutation
BEFORE UPDATE OR DELETE ON customer_price_quote_lines
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER customer_rated_usage_reject_mutation
BEFORE UPDATE OR DELETE ON customer_rated_usage
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER customer_rated_usage_lines_reject_mutation
BEFORE UPDATE OR DELETE ON customer_rated_usage_lines
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER customer_rated_usage_fact_links_reject_mutation
BEFORE UPDATE OR DELETE ON customer_rated_usage_fact_links
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER customer_price_quotes_reject_truncate
BEFORE TRUNCATE ON customer_price_quotes
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

CREATE TRIGGER customer_price_quote_lines_reject_truncate
BEFORE TRUNCATE ON customer_price_quote_lines
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

CREATE TRIGGER customer_billing_holds_reject_truncate
BEFORE TRUNCATE ON customer_billing_holds
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

CREATE TRIGGER customer_rated_usage_reject_truncate
BEFORE TRUNCATE ON customer_rated_usage
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

CREATE TRIGGER customer_rated_usage_lines_reject_truncate
BEFORE TRUNCATE ON customer_rated_usage_lines
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

CREATE TRIGGER customer_rated_usage_fact_links_reject_truncate
BEFORE TRUNCATE ON customer_rated_usage_fact_links
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();
