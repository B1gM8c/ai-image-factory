ALTER TABLE jobs
    DROP CONSTRAINT jobs_economics_contract_version_check,
    ADD CONSTRAINT jobs_economics_contract_version_check CHECK (
        economics_contract_version IN (1, 2, 3, 4)
    );

ALTER TABLE job_auth_attributions
    ADD CONSTRAINT job_auth_attributions_quote_identity_unique
    UNIQUE (job_id, tenant_id, project_id, admitted_at_ms);

ALTER TABLE customer_price_quotes
    ADD CONSTRAINT customer_price_quotes_job_project_admission_fk
    FOREIGN KEY (job_id, tenant_id, project_id, created_at_ms)
    REFERENCES job_auth_attributions(
        job_id, tenant_id, project_id, admitted_at_ms
    )
    ON DELETE RESTRICT;

ALTER TABLE customer_price_quote_lines
    ADD COLUMN reservation_quantity_source TEXT NOT NULL
        DEFAULT 'request_derived' CHECK (
            reservation_quantity_source IN (
                'provider_reported',
                'request_derived',
                'official_lookup',
                'operator_adjustment'
            )
        ),
    ADD COLUMN reservation_confidence TEXT NOT NULL
        DEFAULT 'bounded' CHECK (
            reservation_confidence IN ('exact', 'bounded', 'estimated')
        );

ALTER TABLE customer_price_quote_lines
    ALTER COLUMN reservation_quantity_source DROP DEFAULT,
    ALTER COLUMN reservation_confidence DROP DEFAULT;

DROP TRIGGER provider_usage_facts_reject_mutation
    ON provider_usage_facts;

ALTER TABLE provider_usage_facts
    ADD COLUMN billing_partition_key TEXT NOT NULL DEFAULT 'legacy' CHECK (
        char_length(billing_partition_key) BETWEEN 1 AND 128
        AND billing_partition_key !~ '[[:cntrl:]]'
    ),
    ADD COLUMN terminal_outcome TEXT CHECK (
        terminal_outcome IN (
            'succeeded', 'failed', 'no_effect', 'uncertain'
        )
    );

UPDATE provider_usage_facts AS fact
SET terminal_outcome = receipt.outcome
FROM provider_receipts AS receipt
WHERE receipt.receipt_id = fact.receipt_id;

ALTER TABLE provider_usage_facts
    ALTER COLUMN terminal_outcome SET NOT NULL;

ALTER TABLE provider_usage_facts
    ALTER COLUMN billing_partition_key DROP DEFAULT,
    ALTER COLUMN terminal_outcome DROP DEFAULT;

CREATE TRIGGER provider_usage_facts_reject_mutation
BEFORE UPDATE OR DELETE ON provider_usage_facts
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE INDEX provider_usage_facts_customer_rating_idx
    ON provider_usage_facts(
        job_id, billing_partition_key, terminal_outcome,
        metric, unit, quantity_source, confidence, usage_fact_id
    )
    WHERE metric <> 'provider_reported_cost';

ALTER TABLE customer_billing_holds
    ADD CONSTRAINT customer_billing_holds_account_fk
    FOREIGN KEY (tenant_id, currency)
    REFERENCES billing_accounts(tenant_id, currency)
    ON DELETE RESTRICT;

ALTER TABLE ledger_transactions
    DROP CONSTRAINT ledger_transactions_transaction_type_check,
    DROP CONSTRAINT ledger_transactions_check,
    ADD CONSTRAINT ledger_transactions_transaction_type_check CHECK (
        transaction_type IN (
            'customer_charge',
            'customer_job_charge',
            'provider_cost',
            'adjustment'
        )
    ),
    ADD CONSTRAINT ledger_transactions_check CHECK (
        (
            transaction_type IN ('customer_charge', 'provider_cost')
            AND source_output_id IS NOT NULL
            AND source_job_id IS NOT NULL
            AND source_submission_id IS NOT NULL
            AND source_receipt_id IS NOT NULL
        )
        OR
        (
            transaction_type = 'customer_job_charge'
            AND source_output_id IS NULL
            AND source_job_id IS NOT NULL
            AND source_submission_id IS NULL
            AND source_receipt_id IS NULL
        )
        OR
        (
            transaction_type = 'adjustment'
            AND source_output_id IS NULL
            AND source_job_id IS NULL
            AND source_submission_id IS NULL
            AND source_receipt_id IS NULL
        )
    ),
    ADD CONSTRAINT ledger_transactions_source_job_fk
        FOREIGN KEY (source_job_id) REFERENCES jobs(job_id) ON DELETE RESTRICT;

CREATE UNIQUE INDEX ledger_transactions_customer_job_uidx
    ON ledger_transactions(source_job_id)
    WHERE transaction_type = 'customer_job_charge';

CREATE OR REPLACE FUNCTION preserve_price_component() RETURNS TRIGGER AS $$
DECLARE
    old_parent_state TEXT;
    new_parent_state TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM 1
        FROM price_book_versions
        WHERE price_book_version_id = NEW.price_book_version_id
        FOR UPDATE;
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM 1
        FROM price_book_versions
        WHERE price_book_version_id = OLD.price_book_version_id
        FOR UPDATE;
    ELSE
        PERFORM 1
        FROM price_book_versions
        WHERE price_book_version_id IN (
            OLD.price_book_version_id, NEW.price_book_version_id
        )
        ORDER BY price_book_version_id
        FOR UPDATE;
    END IF;

    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        SELECT state INTO STRICT old_parent_state
        FROM price_book_versions
        WHERE price_book_version_id = OLD.price_book_version_id;

        IF old_parent_state <> 'draft' THEN
            RAISE EXCEPTION 'published price component is immutable'
                USING ERRCODE = '55000';
        END IF;
    END IF;
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        SELECT state INTO STRICT new_parent_state
        FROM price_book_versions
        WHERE price_book_version_id = NEW.price_book_version_id;

        IF new_parent_state <> 'draft' THEN
            RAISE EXCEPTION 'published price component is immutable'
                USING ERRCODE = '55000';
        END IF;
    END IF;
    IF TG_OP = 'UPDATE'
       AND NEW.price_book_version_id <> OLD.price_book_version_id THEN
        RAISE EXCEPTION 'price component parent is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

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

    IF NOT EXISTS (
           SELECT 1
           FROM jobs
           WHERE job_id = NEW.job_id
             AND tenant_id = NEW.tenant_id
             AND economics_contract_version = 4
       )
       OR source_book.state <> 'active'
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

CREATE OR REPLACE FUNCTION validate_customer_rating_fact_link()
RETURNS TRIGGER AS $$
DECLARE
    rating_line customer_rated_usage_lines%ROWTYPE;
    quote_line customer_price_quote_lines%ROWTYPE;
    quote customer_price_quotes%ROWTYPE;
    fact provider_usage_facts%ROWTYPE;
    actual_confidence_rank INTEGER;
    required_confidence_rank INTEGER;
BEGIN
    SELECT * INTO STRICT rating_line
    FROM customer_rated_usage_lines
    WHERE rated_usage_line_id = NEW.rated_usage_line_id;

    SELECT * INTO STRICT quote_line
    FROM customer_price_quote_lines
    WHERE quote_line_id = rating_line.quote_line_id
      AND quote_id = rating_line.quote_id
      AND job_id = rating_line.job_id;

    SELECT * INTO STRICT quote
    FROM customer_price_quotes
    WHERE quote_id = rating_line.quote_id
      AND job_id = rating_line.job_id;

    SELECT * INTO STRICT fact
    FROM provider_usage_facts
    WHERE usage_fact_id = NEW.usage_fact_id;

    actual_confidence_rank := CASE fact.confidence
        WHEN 'exact' THEN 3
        WHEN 'bounded' THEN 2
        WHEN 'estimated' THEN 1
    END;
    required_confidence_rank := CASE quote_line.required_confidence
        WHEN 'exact' THEN 3
        WHEN 'bounded' THEN 2
        WHEN 'estimated' THEN 1
        WHEN 'any' THEN 1
    END;

    IF fact.job_id <> rating_line.job_id
       OR (
           quote.provider_id IS NOT NULL
           AND fact.provider_id <> quote.provider_id
       )
       OR fact.execution_surface <> quote.execution_surface
       OR fact.metric <> quote_line.metric
       OR fact.unit <> quote_line.unit
       OR fact.quantity_source <> quote_line.quantity_source
       OR actual_confidence_rank < required_confidence_rank
       OR fact.billing_partition_key <> quote_line.partition_key
       OR fact.terminal_outcome <> quote_line.terminal_outcome
       OR NOT (fact.metadata_json @> quote_line.dimensions_json) THEN
        RAISE EXCEPTION 'customer rating fact does not match its frozen quote line'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_customer_rating_total() RETURNS TRIGGER AS $$
DECLARE
    target_rating_id UUID := COALESCE(NEW.rated_usage_id, OLD.rated_usage_id);
    expected_total NUMERIC;
    stored_total BIGINT;
    quote_max BIGINT;
    line_count BIGINT;
BEGIN
    SELECT rating.total_amount_micros, quote.max_total_micros
      INTO STRICT stored_total, quote_max
    FROM customer_rated_usage rating
    JOIN customer_price_quotes quote ON quote.quote_id = rating.quote_id
    WHERE rating.rated_usage_id = target_rating_id;

    SELECT COALESCE(SUM(amount_micros::NUMERIC), 0), COUNT(*)
      INTO expected_total, line_count
    FROM customer_rated_usage_lines
    WHERE rated_usage_id = target_rating_id;

    IF line_count = 0
       OR expected_total <> stored_total::NUMERIC
       OR stored_total > quote_max THEN
        RAISE EXCEPTION 'customer rating total is invalid'
            USING ERRCODE = '23514';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_customer_rating_fact_set() RETURNS TRIGGER AS $$
DECLARE
    target_rating_id UUID;
    target_job_id UUID;
BEGIN
    IF TG_TABLE_NAME = 'customer_rated_usage' THEN
        target_rating_id := COALESCE(NEW.rated_usage_id, OLD.rated_usage_id);
    ELSIF TG_TABLE_NAME = 'customer_rated_usage_lines' THEN
        target_rating_id := COALESCE(NEW.rated_usage_id, OLD.rated_usage_id);
    ELSE
        SELECT line.rated_usage_id INTO STRICT target_rating_id
        FROM customer_rated_usage_lines line
        WHERE line.rated_usage_line_id =
            COALESCE(NEW.rated_usage_line_id, OLD.rated_usage_line_id);
    END IF;

    SELECT job_id INTO STRICT target_job_id
    FROM customer_rated_usage
    WHERE rated_usage_id = target_rating_id;

    IF EXISTS (
        SELECT 1
        FROM customer_rated_usage_lines line
        LEFT JOIN customer_rated_usage_fact_links link
          ON link.rated_usage_line_id = line.rated_usage_line_id
        LEFT JOIN provider_usage_facts fact
          ON fact.usage_fact_id = link.usage_fact_id
        WHERE line.rated_usage_id = target_rating_id
        GROUP BY line.rated_usage_line_id, line.actual_quantity
        HAVING COUNT(fact.usage_fact_id) = 0
            OR COALESCE(SUM(fact.quantity::NUMERIC), 0)
                <> line.actual_quantity::NUMERIC
    ) OR EXISTS (
        SELECT 1
        FROM provider_usage_facts fact
        WHERE fact.job_id = target_job_id
          AND fact.metric <> 'provider_reported_cost'
          AND NOT EXISTS (
              SELECT 1
              FROM customer_rated_usage_fact_links link
              JOIN customer_rated_usage_lines line
                ON line.rated_usage_line_id = link.rated_usage_line_id
              WHERE line.rated_usage_id = target_rating_id
                AND link.usage_fact_id = fact.usage_fact_id
          )
    ) THEN
        RAISE EXCEPTION 'customer rating does not exactly cover immutable usage facts'
            USING ERRCODE = '23514';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER customer_rated_usage_validate_fact_set
AFTER INSERT ON customer_rated_usage
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_customer_rating_fact_set();

CREATE CONSTRAINT TRIGGER customer_rated_usage_lines_validate_fact_set
AFTER INSERT ON customer_rated_usage_lines
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_customer_rating_fact_set();

CREATE CONSTRAINT TRIGGER customer_rated_usage_fact_links_validate_fact_set
AFTER INSERT ON customer_rated_usage_fact_links
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_customer_rating_fact_set();

CREATE FUNCTION validate_customer_hold_rating() RETURNS TRIGGER AS $$
DECLARE
    target_job_id UUID;
    hold customer_billing_holds%ROWTYPE;
    rating_total BIGINT;
    rating_count BIGINT;
BEGIN
    IF TG_TABLE_NAME = 'customer_billing_holds' THEN
        target_job_id := COALESCE(NEW.job_id, OLD.job_id);
    ELSE
        target_job_id := COALESCE(NEW.job_id, OLD.job_id);
    END IF;

    SELECT * INTO STRICT hold
    FROM customer_billing_holds
    WHERE job_id = target_job_id;

    SELECT COUNT(*), COALESCE(MAX(total_amount_micros), 0)
      INTO rating_count, rating_total
    FROM customer_rated_usage
    WHERE job_id = target_job_id;

    IF (
        hold.state = 'held'
        AND rating_count <> 0
    ) OR (
        hold.state = 'settled'
        AND (
            rating_count <> 1
            OR hold.captured_micros <> rating_total
            OR hold.released_micros <> hold.held_micros - rating_total
        )
    ) OR (
        hold.state = 'released'
        AND rating_count <> 0
    ) THEN
        RAISE EXCEPTION 'customer billing hold does not match terminal rating'
            USING ERRCODE = '23514';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER customer_billing_holds_validate_rating
AFTER INSERT OR UPDATE ON customer_billing_holds
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_customer_hold_rating();

CREATE CONSTRAINT TRIGGER customer_rated_usage_validate_hold
AFTER INSERT ON customer_rated_usage
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_customer_hold_rating();
