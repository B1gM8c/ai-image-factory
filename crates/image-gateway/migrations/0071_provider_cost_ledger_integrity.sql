CREATE FUNCTION validate_provider_cost_ledger_amount()
RETURNS TRIGGER AS $$
DECLARE
    target_transaction_id UUID;
    transaction_row ledger_transactions%ROWTYPE;
    expected_currency TEXT;
    expected_amount_micros BIGINT;
    posting_count BIGINT;
    posting_sum NUMERIC;
    positive_amount NUMERIC;
    negative_amount NUMERIC;
    seal_count BIGINT;
BEGIN
    target_transaction_id := NEW.transaction_id;

    SELECT * INTO STRICT transaction_row
    FROM ledger_transactions
    WHERE transaction_id = target_transaction_id;

    IF transaction_row.transaction_type <> 'provider_cost'
       OR (
           transaction_row.source_provider_cost_observation_id IS NULL
           AND transaction_row.source_provider_cost_allocation_line_id IS NULL
       ) THEN
        RETURN NULL;
    END IF;

    IF transaction_row.source_provider_cost_observation_id IS NOT NULL THEN
        SELECT currency, amount_micros
          INTO STRICT expected_currency, expected_amount_micros
        FROM provider_cost_observations
        WHERE provider_cost_observation_id =
              transaction_row.source_provider_cost_observation_id;
    ELSE
        SELECT pool.currency, line.amount_micros
          INTO STRICT expected_currency, expected_amount_micros
        FROM provider_cost_allocation_lines line
        JOIN provider_cost_allocation_pools pool
          ON pool.provider_cost_allocation_pool_id =
             line.provider_cost_allocation_pool_id
        WHERE line.provider_cost_allocation_line_id =
              transaction_row.source_provider_cost_allocation_line_id
          AND line.provider_cost_allocation_pool_id =
              transaction_row.source_provider_cost_allocation_pool_id;
    END IF;

    SELECT COUNT(*),
           COALESCE(SUM(amount_micros::NUMERIC), 0),
           MAX(amount_micros::NUMERIC) FILTER (WHERE amount_micros > 0),
           MIN(amount_micros::NUMERIC) FILTER (WHERE amount_micros < 0)
      INTO posting_count, posting_sum, positive_amount, negative_amount
    FROM ledger_postings
    WHERE transaction_id = target_transaction_id;

    SELECT COUNT(*) INTO seal_count
    FROM ledger_transaction_seals
    WHERE transaction_id = target_transaction_id;

    IF expected_amount_micros <= 0
       OR transaction_row.currency <> expected_currency
       OR posting_count <> 2
       OR posting_sum <> 0
       OR positive_amount <> expected_amount_micros::NUMERIC
       OR negative_amount <> -expected_amount_micros::NUMERIC
       OR seal_count <> 1 THEN
        RAISE EXCEPTION 'provider cost ledger amount does not match its authority'
            USING ERRCODE = '23514';
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER ledger_transactions_provider_cost_amount_guard
AFTER INSERT ON ledger_transactions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_ledger_amount();

CREATE CONSTRAINT TRIGGER ledger_postings_provider_cost_amount_guard
AFTER INSERT ON ledger_postings
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_ledger_amount();

CREATE CONSTRAINT TRIGGER ledger_transaction_seals_provider_cost_amount_guard
AFTER INSERT ON ledger_transaction_seals
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_ledger_amount();

CREATE FUNCTION validate_provider_cost_observation_receipt_link()
RETURNS TRIGGER AS $$
DECLARE
    observation provider_cost_observations%ROWTYPE;
BEGIN
    SELECT * INTO STRICT observation
    FROM provider_cost_observations
    WHERE provider_cost_observation_id = NEW.provider_cost_observation_id;

    IF NOT EXISTS (
        SELECT 1
        FROM provider_cost_observation_fact_links observation_fact
        JOIN provider_usage_facts fact
          ON fact.usage_fact_id = observation_fact.usage_fact_id
        WHERE observation_fact.provider_cost_observation_id =
              NEW.provider_cost_observation_id
          AND fact.receipt_id = NEW.receipt_id
          AND fact.provider_id = observation.provider_id
          AND fact.provider_account_id = observation.provider_account_id
          AND fact.execution_surface = observation.execution_surface
    ) THEN
        RAISE EXCEPTION 'provider cost observation receipt is outside its fact set'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_observation_receipts_validate_fact_set
BEFORE INSERT ON provider_cost_observation_receipts
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_observation_receipt_link();

CREATE FUNCTION validate_provider_cost_allocation_line_period()
RETURNS TRIGGER AS $$
DECLARE
    pool provider_cost_allocation_pools%ROWTYPE;
BEGIN
    SELECT * INTO STRICT pool
    FROM provider_cost_allocation_pools
    WHERE provider_cost_allocation_pool_id =
          NEW.provider_cost_allocation_pool_id;

    IF pool.allocation_basis IN ('successful_job', 'successful_output') THEN
        IF NOT EXISTS (
            SELECT 1
            FROM provider_submissions submission
            JOIN provider_receipts receipt
              ON receipt.submission_id = submission.submission_id
             AND receipt.output_id = submission.output_id
             AND receipt.job_id = submission.job_id
             AND receipt.provider_id = submission.provider_id
            WHERE submission.job_id = NEW.job_id
              AND (
                  NEW.output_id IS NULL
                  OR submission.output_id = NEW.output_id
              )
              AND submission.provider_id = NEW.provider_id
              AND submission.provider_account_id =
                  NEW.provider_account_id
              AND receipt.outcome = 'succeeded'
              AND receipt.created_at_ms >= pool.period_start_ms
              AND receipt.created_at_ms < pool.period_end_ms
        ) THEN
            RAISE EXCEPTION 'provider allocation evidence is outside its period'
                USING ERRCODE = '23514';
        END IF;
    ELSIF NOT EXISTS (
        SELECT 1
        FROM provider_usage_facts fact
        WHERE fact.usage_fact_id = NEW.basis_usage_fact_id
          AND fact.created_at_ms >= pool.period_start_ms
          AND fact.created_at_ms < pool.period_end_ms
    ) THEN
        RAISE EXCEPTION 'provider allocation usage fact is outside its period'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_allocation_lines_validate_period
BEFORE INSERT OR UPDATE ON provider_cost_allocation_lines
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_allocation_line_period();

CREATE OR REPLACE FUNCTION validate_customer_quote_source()
RETURNS TRIGGER AS $$
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

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_cost_observation_receipts receipt_link
        JOIN provider_cost_observations observation
          ON observation.provider_cost_observation_id =
             receipt_link.provider_cost_observation_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM provider_cost_observation_fact_links fact_link
            JOIN provider_usage_facts fact
              ON fact.usage_fact_id = fact_link.usage_fact_id
            WHERE fact_link.provider_cost_observation_id =
                  receipt_link.provider_cost_observation_id
              AND fact.receipt_id = receipt_link.receipt_id
              AND fact.provider_id = observation.provider_id
              AND fact.provider_account_id =
                  observation.provider_account_id
              AND fact.execution_surface =
                  observation.execution_surface
        )
    ) THEN
        RAISE EXCEPTION 'existing provider cost receipt links violate fact-set attribution'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM ledger_transactions transaction
        JOIN LATERAL (
            SELECT COUNT(*) AS posting_count,
                   COALESCE(SUM(amount_micros::NUMERIC), 0) AS posting_sum,
                   MAX(amount_micros::NUMERIC)
                       FILTER (WHERE amount_micros > 0) AS positive_amount,
                   MIN(amount_micros::NUMERIC)
                       FILTER (WHERE amount_micros < 0) AS negative_amount
            FROM ledger_postings posting
            WHERE posting.transaction_id = transaction.transaction_id
        ) postings ON TRUE
        LEFT JOIN provider_cost_observations observation
          ON observation.provider_cost_observation_id =
             transaction.source_provider_cost_observation_id
        LEFT JOIN provider_cost_allocation_lines allocation_line
          ON allocation_line.provider_cost_allocation_line_id =
             transaction.source_provider_cost_allocation_line_id
         AND allocation_line.provider_cost_allocation_pool_id =
             transaction.source_provider_cost_allocation_pool_id
        LEFT JOIN provider_cost_allocation_pools allocation_pool
          ON allocation_pool.provider_cost_allocation_pool_id =
             allocation_line.provider_cost_allocation_pool_id
        WHERE transaction.transaction_type = 'provider_cost'
          AND (
              transaction.source_provider_cost_observation_id IS NOT NULL
              OR transaction.source_provider_cost_allocation_line_id IS NOT NULL
          )
          AND (
              transaction.currency IS DISTINCT FROM
                  COALESCE(observation.currency, allocation_pool.currency)
              OR postings.posting_count <> 2
              OR postings.posting_sum <> 0
              OR postings.positive_amount IS DISTINCT FROM
                  COALESCE(
                      observation.amount_micros,
                      allocation_line.amount_micros
                  )::NUMERIC
              OR postings.negative_amount IS DISTINCT FROM
                  -COALESCE(
                      observation.amount_micros,
                      allocation_line.amount_micros
                  )::NUMERIC
              OR NOT EXISTS (
                  SELECT 1
                  FROM ledger_transaction_seals seal
                  WHERE seal.transaction_id = transaction.transaction_id
              )
          )
    ) THEN
        RAISE EXCEPTION 'existing provider cost ledgers violate source amounts'
            USING ERRCODE = '23514';
    END IF;
END;
$$;
