ALTER TABLE billing_integrity_findings
    DROP CONSTRAINT billing_integrity_findings_category_check,
    ADD CONSTRAINT billing_integrity_findings_category_check CHECK (
        category IN (
            'account_balance', 'hold_lifecycle', 'customer_charge',
            'customer_refund', 'attribution', 'provider_cost', 'allocation'
        )
    );

ALTER TABLE billing_accounts
    ADD COLUMN refunded_micros BIGINT NOT NULL DEFAULT 0
        CHECK (refunded_micros >= 0);

ALTER TABLE billing_accounts
    DROP CONSTRAINT billing_accounts_check,
    ADD CONSTRAINT billing_accounts_refunded_not_above_captured CHECK (
        refunded_micros <= captured_micros
    ),
    ADD CONSTRAINT billing_accounts_check CHECK (
        (
            held_micros::NUMERIC
            + captured_micros::NUMERIC
            - refunded_micros::NUMERIC
        ) <= credit_limit_micros::NUMERIC
    );

ALTER TABLE ledger_transactions
    ADD COLUMN reverses_transaction_id UUID
        REFERENCES ledger_transactions(transaction_id) ON DELETE RESTRICT;

ALTER TABLE ledger_transactions
    DROP CONSTRAINT ledger_transactions_transaction_type_check,
    DROP CONSTRAINT ledger_transactions_check,
    ADD CONSTRAINT ledger_transactions_transaction_type_check CHECK (
        transaction_type IN (
            'customer_charge',
            'customer_job_charge',
            'customer_refund',
            'provider_cost',
            'adjustment'
        )
    ),
    ADD CONSTRAINT ledger_transactions_check CHECK (
        (
            transaction_type = 'customer_charge'
            AND source_output_id IS NOT NULL
            AND source_job_id IS NOT NULL
            AND source_submission_id IS NOT NULL
            AND source_receipt_id IS NOT NULL
            AND source_provider_cost_observation_id IS NULL
            AND source_provider_cost_allocation_pool_id IS NULL
            AND reverses_transaction_id IS NULL
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
            AND reverses_transaction_id IS NULL
        )
        OR
        (
            transaction_type = 'customer_refund'
            AND source_output_id IS NULL
            AND source_job_id IS NULL
            AND source_submission_id IS NULL
            AND source_receipt_id IS NULL
            AND source_provider_cost_observation_id IS NULL
            AND source_provider_cost_allocation_pool_id IS NULL
            AND reverses_transaction_id IS NOT NULL
        )
        OR
        (
            transaction_type = 'provider_cost'
            AND reverses_transaction_id IS NULL
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
            AND reverses_transaction_id IS NULL
        )
    );

CREATE INDEX ledger_transactions_reversal_source_idx
    ON ledger_transactions(reverses_transaction_id)
    WHERE reverses_transaction_id IS NOT NULL;

CREATE TABLE customer_refunds (
    refund_id UUID PRIMARY KEY,
    original_transaction_id UUID NOT NULL
        REFERENCES ledger_transactions(transaction_id) ON DELETE RESTRICT,
    refund_transaction_id UUID NOT NULL UNIQUE
        REFERENCES ledger_transactions(transaction_id) ON DELETE RESTRICT,
    tenant_id TEXT NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    amount_micros BIGINT NOT NULL CHECK (amount_micros > 0),
    reason_code TEXT NOT NULL CHECK (
        reason_code IN (
            'customer_request',
            'service_failure',
            'billing_correction',
            'fraud_dispute',
            'provider_refund_pass_through',
            'other'
        )
    ),
    reason TEXT NOT NULL CHECK (char_length(reason) BETWEEN 3 AND 500),
    idempotency_key_digest TEXT NOT NULL CHECK (
        idempotency_key_digest ~ '^[0-9a-f]{64}$'
    ),
    request_hash TEXT NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    actor_user_id UUID NOT NULL,
    session_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL,
    FOREIGN KEY (tenant_id, currency)
        REFERENCES billing_accounts(tenant_id, currency) ON DELETE RESTRICT,
    CONSTRAINT customer_refunds_original_idempotency_key
        UNIQUE (original_transaction_id, idempotency_key_digest)
);

CREATE INDEX customer_refunds_original_created_idx
    ON customer_refunds(
        original_transaction_id,
        created_at_ms DESC,
        refund_id DESC
    );

CREATE INDEX customer_refunds_account_created_idx
    ON customer_refunds(
        tenant_id,
        currency,
        created_at_ms DESC,
        refund_id DESC
    );

CREATE FUNCTION validate_customer_refund_transaction_source()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    source_transaction ledger_transactions%ROWTYPE;
BEGIN
    IF NEW.transaction_type <> 'customer_refund' THEN
        RETURN NEW;
    END IF;

    IF NEW.reverses_transaction_id IS NULL
       OR NEW.reverses_transaction_id = NEW.transaction_id THEN
        RAISE EXCEPTION 'customer refund must reverse a distinct customer charge'
            USING ERRCODE = '23514';
    END IF;

    SELECT *
      INTO STRICT source_transaction
    FROM ledger_transactions
    WHERE transaction_id = NEW.reverses_transaction_id
    FOR UPDATE;

    IF source_transaction.transaction_type NOT IN (
           'customer_charge', 'customer_job_charge'
       )
       OR source_transaction.currency <> NEW.currency
       OR NOT EXISTS (
            SELECT 1
            FROM ledger_transaction_seals
            WHERE transaction_id = NEW.reverses_transaction_id
       ) THEN
        RAISE EXCEPTION 'customer refund source must be a sealed customer charge'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER ledger_transactions_validate_customer_refund_source
BEFORE INSERT ON ledger_transactions
FOR EACH ROW EXECUTE FUNCTION validate_customer_refund_transaction_source();

CREATE FUNCTION require_customer_refund_evidence()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.transaction_type = 'customer_refund'
       AND (
            SELECT COUNT(*)
            FROM customer_refunds
            WHERE refund_transaction_id = NEW.transaction_id
       ) <> 1 THEN
        RAISE EXCEPTION 'customer refund ledger transaction requires one refund record'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER ledger_transactions_require_customer_refund_evidence
AFTER INSERT ON ledger_transactions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION require_customer_refund_evidence();

CREATE FUNCTION validate_customer_refund()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    original_transaction ledger_transactions%ROWTYPE;
    refund_transaction ledger_transactions%ROWTYPE;
    original_posting_count BIGINT;
    original_posting_sum NUMERIC;
    original_receivable NUMERIC;
    original_revenue NUMERIC;
    refund_posting_count BIGINT;
    refund_posting_sum NUMERIC;
    refund_receivable NUMERIC;
    refund_revenue NUMERIC;
    cumulative_refunds NUMERIC;
BEGIN
    SELECT *
      INTO STRICT original_transaction
    FROM ledger_transactions
    WHERE transaction_id = NEW.original_transaction_id
    FOR UPDATE;

    IF original_transaction.transaction_type NOT IN (
        'customer_charge', 'customer_job_charge'
    )
       OR original_transaction.currency <> NEW.currency
       OR NOT EXISTS (
            SELECT 1
            FROM ledger_transaction_seals
            WHERE transaction_id = NEW.original_transaction_id
       ) THEN
        RAISE EXCEPTION 'refund source must be a sealed customer charge'
            USING ERRCODE = '23514';
    END IF;

    SELECT *
      INTO STRICT refund_transaction
    FROM ledger_transactions
    WHERE transaction_id = NEW.refund_transaction_id;

    IF refund_transaction.transaction_type <> 'customer_refund'
       OR refund_transaction.currency <> NEW.currency
       OR refund_transaction.reverses_transaction_id <>
          NEW.original_transaction_id
       OR refund_transaction.payload_hash <> NEW.request_hash
       OR NOT EXISTS (
            SELECT 1
            FROM ledger_transaction_seals
            WHERE transaction_id = NEW.refund_transaction_id
       ) THEN
        RAISE EXCEPTION 'refund ledger transaction is not a sealed reversal'
            USING ERRCODE = '23514';
    END IF;

    SELECT COUNT(*)::BIGINT,
           COALESCE(SUM(posting.amount_micros::NUMERIC), 0),
           COALESCE(SUM(posting.amount_micros::NUMERIC) FILTER (
               WHERE account.owner_type = 'tenant'
                 AND account.owner_id = NEW.tenant_id
                 AND account.account_type = 'receivable'
           ), 0),
           COALESCE(SUM(posting.amount_micros::NUMERIC) FILTER (
               WHERE account.owner_type = 'platform'
                 AND account.owner_id = 'platform'
                 AND account.account_type = 'revenue'
           ), 0)
      INTO original_posting_count,
           original_posting_sum,
           original_receivable,
           original_revenue
    FROM ledger_postings posting
    JOIN ledger_accounts account
      ON account.account_id = posting.account_id
     AND account.currency = posting.currency
    WHERE posting.transaction_id = NEW.original_transaction_id;

    IF original_posting_count <> 2
       OR original_posting_sum <> 0
       OR original_receivable <= 0
       OR original_revenue <> -original_receivable THEN
        RAISE EXCEPTION 'refund source has invalid customer charge postings'
            USING ERRCODE = '23514';
    END IF;

    SELECT COUNT(*)::BIGINT,
           COALESCE(SUM(posting.amount_micros::NUMERIC), 0),
           COALESCE(SUM(posting.amount_micros::NUMERIC) FILTER (
               WHERE account.owner_type = 'tenant'
                 AND account.owner_id = NEW.tenant_id
                 AND account.account_type = 'receivable'
           ), 0),
           COALESCE(SUM(posting.amount_micros::NUMERIC) FILTER (
               WHERE account.owner_type = 'platform'
                 AND account.owner_id = 'platform'
                 AND account.account_type = 'revenue'
           ), 0)
      INTO refund_posting_count,
           refund_posting_sum,
           refund_receivable,
           refund_revenue
    FROM ledger_postings posting
    JOIN ledger_accounts account
      ON account.account_id = posting.account_id
     AND account.currency = posting.currency
    WHERE posting.transaction_id = NEW.refund_transaction_id;

    IF refund_posting_count <> 2
       OR refund_posting_sum <> 0
       OR refund_receivable <> -NEW.amount_micros::NUMERIC
       OR refund_revenue <> NEW.amount_micros::NUMERIC THEN
        RAISE EXCEPTION 'refund postings do not exactly reverse customer revenue'
            USING ERRCODE = '23514';
    END IF;

    SELECT COALESCE(SUM(amount_micros::NUMERIC), 0)
      INTO cumulative_refunds
    FROM customer_refunds
    WHERE original_transaction_id = NEW.original_transaction_id;

    IF cumulative_refunds > original_receivable THEN
        RAISE EXCEPTION 'cumulative refunds exceed the original customer charge'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER customer_refunds_validate
AFTER INSERT ON customer_refunds
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_customer_refund();

CREATE FUNCTION validate_billing_account_refund_total()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    expected_refunded NUMERIC;
    actual_refunded BIGINT;
    target_tenant_id TEXT;
    target_currency TEXT;
BEGIN
    target_tenant_id := NEW.tenant_id;
    target_currency := NEW.currency;

    SELECT COALESCE(SUM(amount_micros::NUMERIC), 0)
      INTO expected_refunded
    FROM customer_refunds
    WHERE tenant_id = target_tenant_id
      AND currency = target_currency;

    SELECT refunded_micros
      INTO STRICT actual_refunded
    FROM billing_accounts
    WHERE tenant_id = target_tenant_id
      AND currency = target_currency;

    IF actual_refunded::NUMERIC <> expected_refunded THEN
        RAISE EXCEPTION 'billing account refund counter does not match sealed refunds'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER customer_refunds_validate_account_total
AFTER INSERT ON customer_refunds
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_billing_account_refund_total();

CREATE CONSTRAINT TRIGGER billing_accounts_validate_refund_total
AFTER UPDATE OF refunded_micros ON billing_accounts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_billing_account_refund_total();

CREATE TRIGGER customer_refunds_reject_mutation
BEFORE UPDATE OR DELETE ON customer_refunds
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER customer_refunds_reject_truncate
BEFORE TRUNCATE ON customer_refunds
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

COMMENT ON TABLE customer_refunds IS
    'Immutable customer refund evidence bound to one sealed customer charge and one sealed reversal transaction.';
