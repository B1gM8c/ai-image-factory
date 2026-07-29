CREATE TABLE provider_cost_allocation_pools (
    provider_cost_allocation_pool_id UUID PRIMARY KEY,
    semantic_key TEXT NOT NULL UNIQUE CHECK (
        char_length(semantic_key) BETWEEN 1 AND 512
        AND semantic_key !~ '[[:cntrl:]]'
    ),
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_account_id UUID NOT NULL,
    price_book_version_id UUID NOT NULL
        REFERENCES price_book_versions(price_book_version_id)
        ON DELETE RESTRICT,
    period_start_ms BIGINT NOT NULL,
    period_end_ms BIGINT NOT NULL CHECK (period_end_ms > period_start_ms),
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    total_amount_micros BIGINT NOT NULL CHECK (total_amount_micros >= 0),
    residual_amount_micros BIGINT NOT NULL DEFAULT 0 CHECK (
        residual_amount_micros >= 0
        AND residual_amount_micros <= total_amount_micros
    ),
    allocation_basis TEXT NOT NULL CHECK (
        allocation_basis IN (
            'successful_job',
            'successful_output',
            'provider_usage',
            'membership_point'
        )
    ),
    state TEXT NOT NULL CHECK (state IN ('draft', 'closed')),
    control_version BIGINT NOT NULL DEFAULT 1 CHECK (control_version > 0),
    created_at_ms BIGINT NOT NULL,
    closed_at_ms BIGINT,
    FOREIGN KEY (provider_account_id, provider_id)
        REFERENCES provider_accounts(provider_account_id, provider_id)
        ON DELETE RESTRICT,
    CHECK (
        (state = 'draft' AND closed_at_ms IS NULL)
        OR (state = 'closed' AND closed_at_ms IS NOT NULL)
    ),
    UNIQUE (
        provider_account_id, period_start_ms, period_end_ms,
        currency, price_book_version_id
    ),
    UNIQUE (provider_cost_allocation_pool_id, provider_id, provider_account_id)
);

CREATE TABLE provider_cost_allocation_lines (
    provider_cost_allocation_line_id UUID PRIMARY KEY,
    provider_cost_allocation_pool_id UUID NOT NULL,
    provider_id TEXT NOT NULL,
    provider_account_id UUID NOT NULL,
    job_id UUID NOT NULL,
    output_id UUID,
    basis_usage_fact_id UUID
        REFERENCES provider_usage_facts(usage_fact_id) ON DELETE RESTRICT,
    basis_quantity NUMERIC(38, 0) NOT NULL CHECK (basis_quantity > 0),
    basis_unit TEXT NOT NULL CHECK (
        basis_unit IN (
            'job', 'output', 'request', 'image', 'token', 'second', 'point'
        )
    ),
    amount_micros BIGINT NOT NULL CHECK (amount_micros >= 0),
    created_at_ms BIGINT NOT NULL,
    FOREIGN KEY (
        provider_cost_allocation_pool_id, provider_id, provider_account_id
    ) REFERENCES provider_cost_allocation_pools(
        provider_cost_allocation_pool_id, provider_id, provider_account_id
    ) ON DELETE RESTRICT,
    FOREIGN KEY (output_id, job_id)
        REFERENCES job_outputs(output_id, job_id) ON DELETE RESTRICT,
    CHECK (output_id IS NOT NULL OR basis_unit = 'job'),
    UNIQUE NULLS NOT DISTINCT (
        provider_cost_allocation_pool_id, job_id, output_id
    ),
    UNIQUE (provider_cost_allocation_line_id, provider_cost_allocation_pool_id)
);

CREATE INDEX provider_cost_allocation_pools_period_idx
    ON provider_cost_allocation_pools(
        provider_id, provider_account_id, period_start_ms, period_end_ms
    );

CREATE INDEX provider_cost_allocation_lines_job_idx
    ON provider_cost_allocation_lines(job_id, output_id);

CREATE FUNCTION validate_provider_cost_allocation_pool()
RETURNS TRIGGER AS $$
DECLARE
    target_pool_id UUID;
    pool provider_cost_allocation_pools%ROWTYPE;
    book_purpose TEXT;
    book_provider_id TEXT;
    book_currency TEXT;
    version_provider_id TEXT;
    version_billing_mode TEXT;
    allocated_total NUMERIC;
    invalid_line_count BIGINT;
BEGIN
    target_pool_id := COALESCE(
        NEW.provider_cost_allocation_pool_id,
        OLD.provider_cost_allocation_pool_id
    );

    SELECT * INTO STRICT pool
    FROM provider_cost_allocation_pools
    WHERE provider_cost_allocation_pool_id = target_pool_id;

    SELECT book.purpose, book.provider_id, book.currency,
           version.provider_id, version.billing_mode
      INTO STRICT book_purpose, book_provider_id, book_currency,
                  version_provider_id, version_billing_mode
    FROM price_book_versions version
    JOIN price_books book ON book.price_book_id = version.price_book_id
    WHERE version.price_book_version_id = pool.price_book_version_id
      AND version.state IN ('active', 'retired');

    IF book_purpose <> 'provider_allocated'
       OR COALESCE(version_provider_id, book_provider_id)
          IS DISTINCT FROM pool.provider_id
       OR book_currency <> pool.currency
       OR version_billing_mode NOT IN (
           'subscription_allocation', 'membership_points'
       ) THEN
        RAISE EXCEPTION 'provider allocation pool price version is invalid'
            USING ERRCODE = '23514';
    END IF;

    SELECT COUNT(*) INTO invalid_line_count
    FROM provider_cost_allocation_lines line
    WHERE line.provider_cost_allocation_pool_id = target_pool_id
      AND NOT (
        (
            pool.allocation_basis = 'successful_job'
            AND line.basis_unit = 'job'
            AND line.output_id IS NULL
            AND line.basis_usage_fact_id IS NULL
            AND EXISTS (
                SELECT 1
                FROM provider_submissions submission
                JOIN provider_receipts receipt
                  ON receipt.submission_id = submission.submission_id
                 AND receipt.output_id = submission.output_id
                 AND receipt.job_id = submission.job_id
                 AND receipt.provider_id = submission.provider_id
                WHERE submission.job_id = line.job_id
                  AND submission.provider_id = line.provider_id
                  AND submission.provider_account_id =
                      line.provider_account_id
                  AND receipt.outcome = 'succeeded'
            )
        )
        OR
        (
            pool.allocation_basis = 'successful_output'
            AND line.basis_unit = 'output'
            AND line.output_id IS NOT NULL
            AND line.basis_usage_fact_id IS NULL
            AND EXISTS (
                SELECT 1
                FROM provider_submissions submission
                JOIN provider_receipts receipt
                  ON receipt.submission_id = submission.submission_id
                 AND receipt.output_id = submission.output_id
                 AND receipt.job_id = submission.job_id
                 AND receipt.provider_id = submission.provider_id
                WHERE submission.job_id = line.job_id
                  AND submission.output_id = line.output_id
                  AND submission.provider_id = line.provider_id
                  AND submission.provider_account_id =
                      line.provider_account_id
                  AND receipt.outcome = 'succeeded'
            )
        )
        OR
        (
            pool.allocation_basis IN ('provider_usage', 'membership_point')
            AND line.output_id IS NOT NULL
            AND line.basis_usage_fact_id IS NOT NULL
            AND EXISTS (
                SELECT 1
                FROM provider_usage_facts fact
                WHERE fact.usage_fact_id = line.basis_usage_fact_id
                  AND fact.job_id = line.job_id
                  AND fact.output_id = line.output_id
                  AND fact.provider_id = line.provider_id
                  AND fact.provider_account_id = line.provider_account_id
                  AND fact.quantity::NUMERIC = line.basis_quantity
                  AND fact.unit = line.basis_unit
                  AND (
                      pool.allocation_basis = 'provider_usage'
                      OR (
                          fact.metric = 'membership_point'
                          AND fact.unit = 'point'
                      )
                  )
            )
        )
      );

    IF invalid_line_count <> 0 THEN
        RAISE EXCEPTION 'provider allocation line evidence is invalid'
            USING ERRCODE = '23514';
    END IF;

    IF pool.state = 'closed' THEN
        SELECT COALESCE(SUM(line.amount_micros::NUMERIC), 0)
          INTO allocated_total
        FROM provider_cost_allocation_lines line
        WHERE line.provider_cost_allocation_pool_id = target_pool_id;

        IF allocated_total + pool.residual_amount_micros::NUMERIC
           <> pool.total_amount_micros::NUMERIC THEN
            RAISE EXCEPTION 'provider allocation pool is not conserved'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION preserve_provider_cost_allocation_pool()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'provider allocation pool deletion is forbidden'
            USING ERRCODE = '55000';
    END IF;

    IF OLD.state = 'closed' THEN
        RAISE EXCEPTION 'closed provider allocation pool is immutable'
            USING ERRCODE = '55000';
    END IF;

    IF NEW.provider_cost_allocation_pool_id <> OLD.provider_cost_allocation_pool_id
       OR NEW.semantic_key <> OLD.semantic_key
       OR NEW.provider_id <> OLD.provider_id
       OR NEW.provider_account_id <> OLD.provider_account_id
       OR NEW.price_book_version_id <> OLD.price_book_version_id
       OR NEW.period_start_ms <> OLD.period_start_ms
       OR NEW.period_end_ms <> OLD.period_end_ms
       OR NEW.currency <> OLD.currency
       OR NEW.allocation_basis <> OLD.allocation_basis
       OR NEW.created_at_ms <> OLD.created_at_ms THEN
        RAISE EXCEPTION 'provider allocation pool identity is immutable'
            USING ERRCODE = '55000';
    END IF;

    IF NEW.control_version <> OLD.control_version + 1 THEN
        RAISE EXCEPTION 'provider allocation pool control version must advance'
            USING ERRCODE = '40001';
    END IF;

    IF NEW.state = 'draft' AND NEW.closed_at_ms IS NOT NULL THEN
        RAISE EXCEPTION 'draft provider allocation pool cannot be closed'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION preserve_provider_cost_allocation_line()
RETURNS TRIGGER AS $$
DECLARE
    pool_state TEXT;
    target_pool_id UUID;
BEGIN
    IF TG_OP = 'DELETE' THEN
        target_pool_id := OLD.provider_cost_allocation_pool_id;
    ELSE
        target_pool_id := NEW.provider_cost_allocation_pool_id;
    END IF;

    SELECT state INTO STRICT pool_state
    FROM provider_cost_allocation_pools
    WHERE provider_cost_allocation_pool_id = target_pool_id
    FOR UPDATE;
    IF pool_state <> 'draft' THEN
        RAISE EXCEPTION 'closed provider allocation lines are immutable'
            USING ERRCODE = '55000';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_allocation_pools_preserve_closed
BEFORE UPDATE OR DELETE ON provider_cost_allocation_pools
FOR EACH ROW EXECUTE FUNCTION preserve_provider_cost_allocation_pool();

CREATE TRIGGER provider_cost_allocation_lines_preserve_closed
BEFORE INSERT OR UPDATE OR DELETE ON provider_cost_allocation_lines
FOR EACH ROW EXECUTE FUNCTION preserve_provider_cost_allocation_line();

CREATE CONSTRAINT TRIGGER provider_cost_allocation_pools_validate
AFTER INSERT OR UPDATE ON provider_cost_allocation_pools
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_allocation_pool();

CREATE CONSTRAINT TRIGGER provider_cost_allocation_lines_validate
AFTER INSERT OR UPDATE OR DELETE ON provider_cost_allocation_lines
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_allocation_pool();

CREATE TRIGGER provider_cost_allocation_pools_reject_truncate
BEFORE TRUNCATE ON provider_cost_allocation_pools
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

CREATE TRIGGER provider_cost_allocation_lines_reject_truncate
BEFORE TRUNCATE ON provider_cost_allocation_lines
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

ALTER TABLE ledger_transactions
    ADD COLUMN source_provider_cost_allocation_pool_id UUID,
    ADD COLUMN source_provider_cost_allocation_line_id UUID;

ALTER TABLE ledger_transactions
    DROP CONSTRAINT ledger_transactions_check,
    ADD CONSTRAINT ledger_transactions_provider_cost_allocation_source_pair
        CHECK (
            (
                source_provider_cost_allocation_pool_id IS NULL
                AND source_provider_cost_allocation_line_id IS NULL
            )
            OR (
                source_provider_cost_allocation_pool_id IS NOT NULL
                AND source_provider_cost_allocation_line_id IS NOT NULL
            )
        );

ALTER TABLE ledger_transactions
    ADD CONSTRAINT ledger_transactions_provider_cost_allocation_line_fk
        FOREIGN KEY (
            source_provider_cost_allocation_line_id,
            source_provider_cost_allocation_pool_id
        )
        REFERENCES provider_cost_allocation_lines(
            provider_cost_allocation_line_id,
            provider_cost_allocation_pool_id
        )
        ON DELETE RESTRICT;

ALTER TABLE ledger_transactions
    ADD CONSTRAINT ledger_transactions_check CHECK (
        (
            transaction_type = 'customer_charge'
            AND source_output_id IS NOT NULL
            AND source_job_id IS NOT NULL
            AND source_submission_id IS NOT NULL
            AND source_receipt_id IS NOT NULL
            AND source_provider_cost_observation_id IS NULL
            AND source_provider_cost_allocation_pool_id IS NULL
        )
        OR
        (
            transaction_type = 'customer_job_charge'
            AND source_output_id IS NULL
            AND source_job_id IS NOT NULL
            AND source_submission_id IS NULL
            AND source_receipt_id IS NULL
            AND source_provider_cost_observation_id IS NULL
            AND source_provider_cost_allocation_pool_id IS NULL
        )
        OR
        (
            transaction_type = 'provider_cost'
            AND (
                (
                    source_output_id IS NOT NULL
                    AND source_job_id IS NOT NULL
                    AND source_submission_id IS NOT NULL
                    AND source_receipt_id IS NOT NULL
                    AND source_provider_cost_observation_id IS NULL
                    AND source_provider_cost_allocation_pool_id IS NULL
                )
                OR
                (
                    source_output_id IS NULL
                    AND source_job_id IS NULL
                    AND source_submission_id IS NULL
                    AND source_receipt_id IS NULL
                    AND source_provider_cost_observation_id IS NOT NULL
                    AND source_provider_cost_allocation_pool_id IS NULL
                )
                OR
                (
                    source_job_id IS NOT NULL
                    AND source_submission_id IS NULL
                    AND source_receipt_id IS NULL
                    AND source_provider_cost_observation_id IS NULL
                    AND source_provider_cost_allocation_pool_id IS NOT NULL
                )
            )
        )
        OR
        (
            transaction_type = 'adjustment'
            AND source_output_id IS NULL
            AND source_job_id IS NULL
            AND source_submission_id IS NULL
            AND source_receipt_id IS NULL
            AND source_provider_cost_observation_id IS NULL
            AND source_provider_cost_allocation_pool_id IS NULL
        )
    );

CREATE UNIQUE INDEX ledger_transactions_provider_cost_allocation_line_uidx
    ON ledger_transactions(source_provider_cost_allocation_line_id)
    WHERE transaction_type = 'provider_cost'
      AND source_provider_cost_allocation_line_id IS NOT NULL;

CREATE FUNCTION validate_provider_cost_allocation_transaction()
RETURNS TRIGGER AS $$
DECLARE
    allocation_line provider_cost_allocation_lines%ROWTYPE;
    pool_state TEXT;
BEGIN
    IF NEW.transaction_type <> 'provider_cost'
       OR NEW.source_provider_cost_allocation_line_id IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT line.*
      INTO STRICT allocation_line
    FROM provider_cost_allocation_lines line
    WHERE line.provider_cost_allocation_line_id =
          NEW.source_provider_cost_allocation_line_id
      AND line.provider_cost_allocation_pool_id =
          NEW.source_provider_cost_allocation_pool_id;

    SELECT state INTO STRICT pool_state
    FROM provider_cost_allocation_pools
    WHERE provider_cost_allocation_pool_id =
          NEW.source_provider_cost_allocation_pool_id;

    IF pool_state <> 'closed'
       OR NEW.source_job_id <> allocation_line.job_id
       OR NEW.source_output_id IS DISTINCT FROM allocation_line.output_id THEN
        RAISE EXCEPTION 'provider allocation ledger source is invalid'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_transactions_validate_provider_allocation
BEFORE INSERT ON ledger_transactions
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_allocation_transaction();
