CREATE TABLE provider_cost_observations (
    provider_cost_observation_id UUID PRIMARY KEY,
    observation_key TEXT NOT NULL UNIQUE CHECK (
        observation_key ~ '^[0-9a-f]{64}$'
    ),
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_account_id UUID NOT NULL,
    execution_surface TEXT NOT NULL CHECK (
        execution_surface IN ('provider_api', 'provider_cli', 'manual_import')
    ),
    provider_operation_id TEXT NOT NULL CHECK (
        char_length(provider_operation_id) BETWEEN 1 AND 512
        AND provider_operation_id !~ '[[:cntrl:]]'
    ),
    purpose TEXT NOT NULL CHECK (purpose = 'provider_actual'),
    price_book_version_id UUID NOT NULL
        REFERENCES price_book_versions(price_book_version_id)
        ON DELETE RESTRICT,
    fact_set_hash TEXT NOT NULL CHECK (fact_set_hash ~ '^[0-9a-f]{64}$'),
    currency TEXT NOT NULL CHECK (currency = 'USD'),
    native_unit TEXT NOT NULL CHECK (native_unit = 'usd_tick'),
    native_quantity NUMERIC(38, 0) NOT NULL CHECK (native_quantity >= 0),
    authority TEXT NOT NULL CHECK (authority = 'provider_reported'),
    confidence TEXT NOT NULL CHECK (confidence = 'exact'),
    evidence_hash TEXT NOT NULL CHECK (evidence_hash ~ '^[0-9a-f]{64}$'),
    evidence_path TEXT NOT NULL CHECK (
        char_length(evidence_path) BETWEEN 1 AND 512
        AND evidence_path !~ '[[:cntrl:]]'
    ),
    amount_micros BIGINT NOT NULL CHECK (amount_micros >= 0),
    rounding_mode TEXT NOT NULL CHECK (
        rounding_mode = 'half_up_after_aggregate'
    ),
    rounding_delta_native_atoms BIGINT NOT NULL CHECK (
        rounding_delta_native_atoms BETWEEN -4999 AND 5000
    ),
    created_at_ms BIGINT NOT NULL,
    FOREIGN KEY (provider_account_id, provider_id)
        REFERENCES provider_accounts(provider_account_id, provider_id)
        ON DELETE RESTRICT,
    UNIQUE (
        provider_id, execution_surface, provider_operation_id
    ),
    UNIQUE (provider_cost_observation_id, provider_id),
    CHECK (
        amount_micros::NUMERIC * 10000 - native_quantity
        = rounding_delta_native_atoms::NUMERIC
    )
);

ALTER TABLE provider_usage_facts
    ADD CONSTRAINT provider_usage_facts_cost_link_identity_unique
    UNIQUE (
        usage_fact_id, provider_id, provider_account_id, execution_surface
    );

CREATE TABLE provider_cost_observation_fact_links (
    provider_cost_observation_id UUID NOT NULL,
    usage_fact_id UUID NOT NULL,
    provider_id TEXT NOT NULL,
    provider_account_id UUID NOT NULL,
    execution_surface TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (provider_cost_observation_id, usage_fact_id),
    FOREIGN KEY (provider_cost_observation_id, provider_id)
        REFERENCES provider_cost_observations(
            provider_cost_observation_id, provider_id
        ) ON DELETE RESTRICT,
    FOREIGN KEY (
        usage_fact_id, provider_id, provider_account_id, execution_surface
    ) REFERENCES provider_usage_facts(
        usage_fact_id, provider_id, provider_account_id, execution_surface
    ) ON DELETE RESTRICT
);

ALTER TABLE provider_receipts
    ADD CONSTRAINT provider_receipts_id_provider_unique
    UNIQUE (receipt_id, provider_id);

CREATE TABLE provider_cost_observation_receipts (
    provider_cost_observation_id UUID NOT NULL,
    receipt_id UUID NOT NULL,
    provider_id TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (provider_cost_observation_id, receipt_id),
    FOREIGN KEY (provider_cost_observation_id, provider_id)
        REFERENCES provider_cost_observations(
            provider_cost_observation_id, provider_id
        ) ON DELETE RESTRICT,
    FOREIGN KEY (receipt_id, provider_id)
        REFERENCES provider_receipts(receipt_id, provider_id)
        ON DELETE RESTRICT
);

CREATE INDEX provider_cost_observation_receipts_receipt_idx
    ON provider_cost_observation_receipts(receipt_id);

CREATE FUNCTION validate_provider_cost_observation_fact_set()
RETURNS TRIGGER AS $$
DECLARE
    target_observation_id UUID;
    observation provider_cost_observations%ROWTYPE;
    book_purpose TEXT;
    book_provider_id TEXT;
    version_provider_id TEXT;
    version_billing_mode TEXT;
    linked_count BIGINT;
    linked_quantity NUMERIC(38, 0);
BEGIN
    target_observation_id :=
        COALESCE(NEW.provider_cost_observation_id, OLD.provider_cost_observation_id);

    SELECT * INTO STRICT observation
    FROM provider_cost_observations
    WHERE provider_cost_observation_id = target_observation_id;

    SELECT book.purpose, book.provider_id,
           version.provider_id, version.billing_mode
      INTO STRICT book_purpose, book_provider_id,
                  version_provider_id, version_billing_mode
    FROM price_book_versions version
    JOIN price_books book ON book.price_book_id = version.price_book_id
    WHERE version.price_book_version_id = observation.price_book_version_id
      AND version.state IN ('active', 'retired');

    IF book_purpose <> 'provider_actual'
       OR COALESCE(version_provider_id, book_provider_id)
          IS DISTINCT FROM observation.provider_id
       OR version_billing_mode <> 'provider_reported' THEN
        RAISE EXCEPTION 'provider cost observation price version is invalid'
            USING ERRCODE = '23514';
    END IF;

    SELECT COUNT(*), COALESCE(SUM(fact.quantity::NUMERIC), 0)
      INTO linked_count, linked_quantity
    FROM provider_cost_observation_fact_links link
    JOIN provider_usage_facts fact
      ON fact.usage_fact_id = link.usage_fact_id
    WHERE link.provider_cost_observation_id = target_observation_id
      AND fact.provider_id = observation.provider_id
      AND fact.provider_account_id = observation.provider_account_id
      AND fact.execution_surface = observation.execution_surface
      AND fact.fact_domain = 'provider_actual'
      AND fact.metric = 'provider_reported_cost'
      AND fact.unit = observation.native_unit
      AND fact.quantity_source = observation.authority
      AND fact.confidence = observation.confidence;

    IF linked_count = 0 OR linked_quantity <> observation.native_quantity THEN
        RAISE EXCEPTION 'provider cost observation does not exactly cover cost facts'
            USING ERRCODE = '23514';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_cost_observations_validate_fact_set
AFTER INSERT ON provider_cost_observations
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_observation_fact_set();

CREATE CONSTRAINT TRIGGER provider_cost_observation_fact_links_validate_fact_set
AFTER INSERT ON provider_cost_observation_fact_links
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_observation_fact_set();

CREATE TRIGGER provider_cost_observations_reject_mutation
BEFORE UPDATE OR DELETE ON provider_cost_observations
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER provider_cost_observation_fact_links_reject_mutation
BEFORE UPDATE OR DELETE ON provider_cost_observation_fact_links
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER provider_cost_observation_receipts_reject_mutation
BEFORE UPDATE OR DELETE ON provider_cost_observation_receipts
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER provider_cost_observations_reject_truncate
BEFORE TRUNCATE ON provider_cost_observations
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

CREATE TRIGGER provider_cost_observation_fact_links_reject_truncate
BEFORE TRUNCATE ON provider_cost_observation_fact_links
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

CREATE TRIGGER provider_cost_observation_receipts_reject_truncate
BEFORE TRUNCATE ON provider_cost_observation_receipts
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

ALTER TABLE ledger_transactions
    ADD COLUMN source_provider_cost_observation_id UUID
        REFERENCES provider_cost_observations(provider_cost_observation_id)
        ON DELETE RESTRICT;

CREATE UNIQUE INDEX ledger_transactions_provider_cost_observation_uidx
    ON ledger_transactions(source_provider_cost_observation_id)
    WHERE transaction_type = 'provider_cost'
      AND source_provider_cost_observation_id IS NOT NULL;
