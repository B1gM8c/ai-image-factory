ALTER TABLE jobs
    ADD COLUMN economics_contract_version SMALLINT NOT NULL DEFAULT 1
        CHECK (economics_contract_version IN (1, 2));

ALTER TABLE jobs
    ADD CONSTRAINT jobs_economic_tenant_identity_unique UNIQUE (job_id, tenant_id);

CREATE TABLE price_versions (
    price_version_id UUID PRIMARY KEY,
    price_key TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    api_profile TEXT NOT NULL,
    operation TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    model TEXT NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    success_micros BIGINT NOT NULL CHECK (success_micros >= 0),
    failed_micros BIGINT NOT NULL CHECK (failed_micros >= 0),
    no_effect_micros BIGINT NOT NULL CHECK (no_effect_micros >= 0),
    state TEXT NOT NULL CHECK (state IN ('draft', 'active', 'retired')),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE (price_key, version),
    UNIQUE (price_version_id, currency, success_micros, failed_micros, no_effect_micros)
);

CREATE UNIQUE INDEX price_versions_active_route_uidx
    ON price_versions (api_profile, operation, provider_id, model)
    WHERE state = 'active';

INSERT INTO price_versions
  (price_version_id, price_key, version, api_profile, operation, provider_id, model,
   currency, success_micros, failed_micros, no_effect_micros, state,
   created_at_ms, updated_at_ms)
VALUES
  ('00000000-0000-4000-8000-000000000009', 'platform-beta-default', 1,
   '*', '*', '*', '*', 'USD', 0, 0, 0, 'active', 0, 0);

CREATE TABLE billing_accounts (
    tenant_id TEXT NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    credit_limit_micros BIGINT NOT NULL DEFAULT 0 CHECK (credit_limit_micros >= 0),
    held_micros BIGINT NOT NULL DEFAULT 0 CHECK (held_micros >= 0),
    captured_micros BIGINT NOT NULL DEFAULT 0 CHECK (captured_micros >= 0),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, currency),
    CHECK ((held_micros::NUMERIC + captured_micros::NUMERIC) <= credit_limit_micros::NUMERIC)
);

CREATE TABLE price_quotes (
    quote_id UUID PRIMARY KEY,
    job_id UUID NOT NULL UNIQUE REFERENCES jobs(job_id) ON DELETE RESTRICT,
    price_version_id UUID NOT NULL REFERENCES price_versions(price_version_id) ON DELETE RESTRICT,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    output_count INTEGER NOT NULL CHECK (output_count BETWEEN 1 AND 10),
    success_micros BIGINT NOT NULL CHECK (success_micros >= 0),
    failed_micros BIGINT NOT NULL CHECK (failed_micros >= 0),
    no_effect_micros BIGINT NOT NULL CHECK (no_effect_micros >= 0),
    max_total_micros BIGINT NOT NULL CHECK (max_total_micros >= 0),
    quote_hash TEXT NOT NULL UNIQUE CHECK (quote_hash ~ '^[0-9a-f]{64}$'),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (quote_id, job_id),
    UNIQUE (quote_id, job_id, currency),
    FOREIGN KEY (price_version_id, currency, success_micros, failed_micros, no_effect_micros)
        REFERENCES price_versions
          (price_version_id, currency, success_micros, failed_micros, no_effect_micros)
        ON DELETE RESTRICT,
    CHECK (
        max_total_micros::NUMERIC =
        output_count::NUMERIC * GREATEST(success_micros, failed_micros, no_effect_micros)::NUMERIC
    )
);

CREATE TABLE output_holds (
    output_id UUID PRIMARY KEY,
    job_id UUID NOT NULL,
    quote_id UUID NOT NULL,
    tenant_id TEXT NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    held_micros BIGINT NOT NULL CHECK (held_micros >= 0),
    captured_micros BIGINT NOT NULL DEFAULT 0 CHECK (captured_micros >= 0),
    released_micros BIGINT NOT NULL DEFAULT 0 CHECK (released_micros >= 0),
    state TEXT NOT NULL CHECK (state IN ('held', 'settled')),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (output_id, job_id)
        REFERENCES job_outputs(output_id, job_id) ON DELETE RESTRICT,
    FOREIGN KEY (quote_id, job_id, currency)
        REFERENCES price_quotes(quote_id, job_id, currency) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, currency)
        REFERENCES billing_accounts(tenant_id, currency) ON DELETE RESTRICT,
    FOREIGN KEY (job_id, tenant_id)
        REFERENCES jobs(job_id, tenant_id) ON DELETE RESTRICT,
    CHECK (
        (state = 'held' AND captured_micros = 0 AND released_micros = 0)
        OR
        (state = 'settled'
         AND captured_micros::NUMERIC + released_micros::NUMERIC = held_micros::NUMERIC)
    )
);

CREATE INDEX output_holds_open_tenant_idx
    ON output_holds (tenant_id, currency, output_id)
    WHERE state = 'held';

ALTER TABLE provider_submissions
    ADD CONSTRAINT provider_submissions_receipt_identity_unique
    UNIQUE (submission_id, output_id, job_id);

ALTER TABLE provider_submissions
    ADD CONSTRAINT provider_submissions_receipt_provider_identity_unique
    UNIQUE (submission_id, output_id, job_id, provider_id);

CREATE TABLE provider_receipts (
    receipt_id UUID PRIMARY KEY,
    semantic_key TEXT NOT NULL UNIQUE,
    submission_id UUID NOT NULL UNIQUE,
    output_id UUID NOT NULL,
    job_id UUID NOT NULL,
    provider_id TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'no_effect', 'uncertain')),
    receipt_schema TEXT NOT NULL CHECK (receipt_schema <> ''),
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    evidence JSONB NOT NULL CHECK (jsonb_typeof(evidence) = 'object'),
    provider_cost_micros BIGINT CHECK (provider_cost_micros >= 0),
    provider_cost_currency TEXT CHECK (provider_cost_currency ~ '^[A-Z]{3}$'),
    created_at_ms BIGINT NOT NULL,
    FOREIGN KEY (submission_id, output_id, job_id, provider_id)
        REFERENCES provider_submissions(submission_id, output_id, job_id, provider_id)
        ON DELETE RESTRICT,
    UNIQUE (receipt_id, submission_id, output_id, job_id),
    CHECK ((provider_cost_micros IS NULL) = (provider_cost_currency IS NULL))
);

CREATE TABLE economic_metering_events (
    meter_event_id UUID PRIMARY KEY,
    semantic_key TEXT NOT NULL UNIQUE,
    output_id UUID NOT NULL,
    job_id UUID NOT NULL,
    submission_id UUID NOT NULL,
    receipt_id UUID NOT NULL,
    fact_kind TEXT NOT NULL CHECK (fact_kind IN ('output_terminal', 'uncertain_observation')),
    metric TEXT NOT NULL,
    quantity BIGINT NOT NULL CHECK (quantity >= 0),
    unit TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'no_effect', 'uncertain')),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (meter_event_id, output_id, job_id),
    FOREIGN KEY (output_id, job_id)
        REFERENCES job_outputs(output_id, job_id) ON DELETE RESTRICT,
    FOREIGN KEY (submission_id, output_id, job_id)
        REFERENCES provider_submissions(submission_id, output_id, job_id) ON DELETE RESTRICT,
    FOREIGN KEY (receipt_id, submission_id, output_id, job_id)
        REFERENCES provider_receipts(receipt_id, submission_id, output_id, job_id)
        ON DELETE RESTRICT
);

CREATE UNIQUE INDEX economic_metering_output_terminal_uidx
    ON economic_metering_events (output_id)
    WHERE fact_kind = 'output_terminal';

CREATE TABLE rated_usage (
    rated_usage_id UUID PRIMARY KEY,
    semantic_key TEXT NOT NULL UNIQUE,
    meter_event_id UUID NOT NULL UNIQUE,
    output_id UUID NOT NULL UNIQUE,
    job_id UUID NOT NULL,
    quote_id UUID NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'no_effect')),
    quantity BIGINT NOT NULL CHECK (quantity > 0),
    unit_price_micros BIGINT NOT NULL CHECK (unit_price_micros >= 0),
    amount_micros BIGINT NOT NULL CHECK (amount_micros >= 0),
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_at_ms BIGINT NOT NULL,
    FOREIGN KEY (meter_event_id, output_id, job_id)
        REFERENCES economic_metering_events(meter_event_id, output_id, job_id) ON DELETE RESTRICT,
    FOREIGN KEY (output_id, job_id)
        REFERENCES job_outputs(output_id, job_id) ON DELETE RESTRICT,
    FOREIGN KEY (quote_id, job_id, currency)
        REFERENCES price_quotes(quote_id, job_id, currency) ON DELETE RESTRICT,
    CHECK (amount_micros::NUMERIC = quantity::NUMERIC * unit_price_micros::NUMERIC)
);

CREATE TABLE ledger_accounts (
    account_id UUID PRIMARY KEY,
    account_key TEXT NOT NULL UNIQUE,
    owner_type TEXT NOT NULL CHECK (owner_type IN ('tenant', 'platform', 'provider')),
    owner_id TEXT NOT NULL,
    account_type TEXT NOT NULL CHECK (
        account_type IN ('receivable', 'revenue', 'expense', 'payable')
    ),
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (account_id, currency)
);

CREATE TABLE ledger_transactions (
    transaction_id UUID PRIMARY KEY,
    semantic_key TEXT NOT NULL UNIQUE,
    source_output_id UUID REFERENCES job_outputs(output_id) ON DELETE RESTRICT,
    source_job_id UUID,
    source_submission_id UUID,
    source_receipt_id UUID,
    transaction_type TEXT NOT NULL CHECK (
        transaction_type IN ('customer_charge', 'provider_cost', 'adjustment')
    ),
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (transaction_id, currency),
    FOREIGN KEY (source_output_id, source_job_id)
        REFERENCES job_outputs(output_id, job_id) ON DELETE RESTRICT,
    FOREIGN KEY (source_receipt_id, source_submission_id, source_output_id, source_job_id)
        REFERENCES provider_receipts(receipt_id, submission_id, output_id, job_id)
        ON DELETE RESTRICT,
    CHECK (
        (transaction_type IN ('customer_charge', 'provider_cost')
         AND source_output_id IS NOT NULL AND source_job_id IS NOT NULL
         AND source_submission_id IS NOT NULL AND source_receipt_id IS NOT NULL)
        OR
        (transaction_type = 'adjustment'
         AND source_output_id IS NULL AND source_job_id IS NULL
         AND source_submission_id IS NULL AND source_receipt_id IS NULL)
    )
);

CREATE UNIQUE INDEX ledger_transactions_customer_output_uidx
    ON ledger_transactions (source_output_id)
    WHERE transaction_type = 'customer_charge';

CREATE TABLE ledger_postings (
    transaction_id UUID NOT NULL,
    posting_no SMALLINT NOT NULL CHECK (posting_no > 0),
    account_id UUID NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    amount_micros BIGINT NOT NULL CHECK (amount_micros <> 0),
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (transaction_id, posting_no),
    FOREIGN KEY (transaction_id, currency)
        REFERENCES ledger_transactions(transaction_id, currency) ON DELETE RESTRICT,
    FOREIGN KEY (account_id, currency)
        REFERENCES ledger_accounts(account_id, currency) ON DELETE RESTRICT
);

CREATE TABLE ledger_transaction_seals (
    transaction_id UUID PRIMARY KEY REFERENCES ledger_transactions(transaction_id) ON DELETE RESTRICT,
    sealed_at_ms BIGINT NOT NULL
);

CREATE FUNCTION reject_economic_fact_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'economic facts are append-only';
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION reject_economic_fact_truncate() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'economic facts cannot be truncated';
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'price_quotes', 'provider_receipts', 'economic_metering_events',
        'rated_usage', 'ledger_accounts', 'ledger_transactions', 'ledger_postings',
        'ledger_transaction_seals'
    ]
    LOOP
        EXECUTE format(
            'CREATE TRIGGER %I_reject_mutation BEFORE UPDATE OR DELETE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation()',
            table_name, table_name
        );
        EXECUTE format(
            'CREATE TRIGGER %I_reject_truncate BEFORE TRUNCATE ON %I '
            'FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate()',
            table_name, table_name
        );
    END LOOP;
END;
$$;

CREATE FUNCTION preserve_published_price_version() RETURNS TRIGGER AS $$
BEGIN
    IF ROW(
        OLD.price_key, OLD.version, OLD.api_profile, OLD.operation, OLD.provider_id, OLD.model,
        OLD.currency, OLD.success_micros, OLD.failed_micros, OLD.no_effect_micros, OLD.created_at_ms
    ) IS DISTINCT FROM ROW(
        NEW.price_key, NEW.version, NEW.api_profile, NEW.operation, NEW.provider_id, NEW.model,
        NEW.currency, NEW.success_micros, NEW.failed_micros, NEW.no_effect_micros, NEW.created_at_ms
    ) THEN
        RAISE EXCEPTION 'published price fields are immutable';
    END IF;
    IF (OLD.state, NEW.state) NOT IN (('draft', 'active'), ('active', 'retired'))
       AND OLD.state <> NEW.state THEN
        RAISE EXCEPTION 'invalid price version transition';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER price_versions_preserve_published
    BEFORE UPDATE ON price_versions
    FOR EACH ROW EXECUTE FUNCTION preserve_published_price_version();

CREATE TRIGGER price_versions_reject_delete
    BEFORE DELETE ON price_versions
    FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER price_versions_reject_truncate
    BEFORE TRUNCATE ON price_versions
    FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

CREATE FUNCTION assert_ledger_transaction_balanced() RETURNS TRIGGER AS $$
DECLARE
    target_id UUID;
    posting_count BIGINT;
    posting_sum NUMERIC;
    seal_count BIGINT;
BEGIN
    target_id := NEW.transaction_id;
    SELECT COUNT(*), COALESCE(SUM(amount_micros::NUMERIC), 0)
      INTO posting_count, posting_sum
      FROM ledger_postings
     WHERE transaction_id = target_id;
    SELECT COUNT(*) INTO seal_count
      FROM ledger_transaction_seals
     WHERE transaction_id = target_id;
    IF posting_count < 2 OR posting_sum <> 0 OR seal_count <> 1 THEN
        RAISE EXCEPTION 'ledger transaction % is not balanced', target_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION reject_posting_to_sealed_transaction() RETURNS TRIGGER AS $$
BEGIN
    PERFORM 1 FROM ledger_transactions
     WHERE transaction_id = NEW.transaction_id
     FOR UPDATE;
    IF EXISTS (
        SELECT 1 FROM ledger_transaction_seals WHERE transaction_id = NEW.transaction_id
    ) THEN
        RAISE EXCEPTION 'ledger transaction % is sealed', NEW.transaction_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION lock_ledger_transaction_before_seal() RETURNS TRIGGER AS $$
BEGIN
    PERFORM 1 FROM ledger_transactions
     WHERE transaction_id = NEW.transaction_id
     FOR UPDATE;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_postings_reject_sealed_insert
    BEFORE INSERT ON ledger_postings
    FOR EACH ROW EXECUTE FUNCTION reject_posting_to_sealed_transaction();

CREATE TRIGGER ledger_transaction_seals_lock_parent
    BEFORE INSERT ON ledger_transaction_seals
    FOR EACH ROW EXECUTE FUNCTION lock_ledger_transaction_before_seal();

CREATE CONSTRAINT TRIGGER ledger_transactions_balance_guard
    AFTER INSERT ON ledger_transactions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_ledger_transaction_balanced();

CREATE CONSTRAINT TRIGGER ledger_postings_balance_guard
    AFTER INSERT ON ledger_postings
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_ledger_transaction_balanced();

CREATE CONSTRAINT TRIGGER ledger_transaction_seals_balance_guard
    AFTER INSERT ON ledger_transaction_seals
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_ledger_transaction_balanced();
