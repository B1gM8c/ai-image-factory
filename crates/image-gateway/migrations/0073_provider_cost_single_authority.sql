CREATE FUNCTION provider_cost_ledger_payload_hash(
    source_semantic_key TEXT,
    source_currency TEXT,
    source_amount_micros BIGINT,
    source_provider_id TEXT
)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
STRICT
AS $$
    SELECT encode(
        sha256(
            convert_to('provider-cost-ledger-v1', 'UTF8')
            || decode('00', 'hex')
            || int8send(
                octet_length(convert_to(source_semantic_key, 'UTF8'))::BIGINT
            )
            || convert_to(source_semantic_key, 'UTF8')
            || int8send(
                octet_length(convert_to(source_currency, 'UTF8'))::BIGINT
            )
            || convert_to(source_currency, 'UTF8')
            || int8send(
                octet_length(
                    convert_to(source_amount_micros::TEXT, 'UTF8')
                )::BIGINT
            )
            || convert_to(source_amount_micros::TEXT, 'UTF8')
            || int8send(
                octet_length(convert_to(source_provider_id, 'UTF8'))::BIGINT
            )
            || convert_to(source_provider_id, 'UTF8')
        ),
        'hex'
    )
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_cost_observation_fact_links
        GROUP BY usage_fact_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'a provider cost fact is claimed by multiple observations'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM provider_cost_observation_receipts
        GROUP BY receipt_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'a provider receipt is claimed by multiple cost observations'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM provider_cost_observation_fact_links link
        JOIN provider_cost_observations observation
          ON observation.provider_cost_observation_id =
             link.provider_cost_observation_id
        JOIN provider_usage_facts fact
          ON fact.usage_fact_id = link.usage_fact_id
        WHERE link.provider_id <> observation.provider_id
           OR link.provider_account_id <> observation.provider_account_id
           OR link.execution_surface <> observation.execution_surface
           OR fact.provider_id <> observation.provider_id
           OR fact.provider_account_id <> observation.provider_account_id
           OR fact.execution_surface <> observation.execution_surface
           OR fact.fact_domain <> 'provider_actual'
           OR fact.metric <> 'provider_reported_cost'
           OR fact.unit <> observation.native_unit
           OR fact.quantity_source <> observation.authority
           OR fact.confidence <> observation.confidence
    ) THEN
        RAISE EXCEPTION 'existing provider cost fact links violate their authority contract'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM provider_cost_observations observation
        JOIN LATERAL (
            SELECT
                COUNT(*) AS linked_count,
                COUNT(*) FILTER (
                    WHERE fact.provider_id = observation.provider_id
                      AND fact.provider_account_id =
                          observation.provider_account_id
                      AND fact.execution_surface =
                          observation.execution_surface
                      AND fact.fact_domain = 'provider_actual'
                      AND fact.metric = 'provider_reported_cost'
                      AND fact.unit = observation.native_unit
                      AND fact.quantity_source = observation.authority
                      AND fact.confidence = observation.confidence
                ) AS valid_count,
                COALESCE(SUM(fact.quantity::NUMERIC), 0) AS linked_quantity,
                encode(
                    sha256(
                        string_agg(
                            uuid_send(fact.usage_fact_id),
                            ''::BYTEA
                            ORDER BY fact.usage_fact_id
                        )
                    ),
                    'hex'
                ) AS linked_fact_set_hash
            FROM provider_cost_observation_fact_links link
            JOIN provider_usage_facts fact
              ON fact.usage_fact_id = link.usage_fact_id
            WHERE link.provider_cost_observation_id =
                  observation.provider_cost_observation_id
        ) fact_set ON TRUE
        WHERE fact_set.linked_count = 0
           OR fact_set.valid_count <> fact_set.linked_count
           OR fact_set.linked_quantity <> observation.native_quantity
           OR fact_set.linked_fact_set_hash
              IS DISTINCT FROM observation.fact_set_hash
           OR (
               observation.amount_micros > 0
               AND (
                   SELECT COUNT(*)
                   FROM ledger_transactions ledger_tx
                   WHERE ledger_tx.transaction_type = 'provider_cost'
                     AND ledger_tx.source_provider_cost_observation_id =
                         observation.provider_cost_observation_id
               ) <> 1
           )
           OR (
               observation.amount_micros = 0
               AND EXISTS (
                   SELECT 1
                   FROM ledger_transactions ledger_tx
                   WHERE ledger_tx.transaction_type = 'provider_cost'
                     AND ledger_tx.source_provider_cost_observation_id =
                         observation.provider_cost_observation_id
               )
           )
           OR EXISTS (
               SELECT fact.receipt_id
               FROM provider_cost_observation_fact_links fact_link
               JOIN provider_usage_facts fact
                 ON fact.usage_fact_id = fact_link.usage_fact_id
               WHERE fact_link.provider_cost_observation_id =
                     observation.provider_cost_observation_id
               EXCEPT
               SELECT receipt_link.receipt_id
               FROM provider_cost_observation_receipts receipt_link
               WHERE receipt_link.provider_cost_observation_id =
                     observation.provider_cost_observation_id
           )
           OR EXISTS (
               SELECT receipt_link.receipt_id
               FROM provider_cost_observation_receipts receipt_link
               WHERE receipt_link.provider_cost_observation_id =
                     observation.provider_cost_observation_id
               EXCEPT
               SELECT fact.receipt_id
               FROM provider_cost_observation_fact_links fact_link
               JOIN provider_usage_facts fact
                 ON fact.usage_fact_id = fact_link.usage_fact_id
               WHERE fact_link.provider_cost_observation_id =
                     observation.provider_cost_observation_id
           )
    ) THEN
        RAISE EXCEPTION 'existing provider cost observations do not exactly cover their facts and receipts'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM ledger_transactions ledger_tx
        LEFT JOIN provider_cost_observations observation
          ON observation.provider_cost_observation_id =
             ledger_tx.source_provider_cost_observation_id
        LEFT JOIN provider_cost_allocation_lines allocation_line
          ON allocation_line.provider_cost_allocation_line_id =
             ledger_tx.source_provider_cost_allocation_line_id
         AND allocation_line.provider_cost_allocation_pool_id =
             ledger_tx.source_provider_cost_allocation_pool_id
        LEFT JOIN provider_cost_allocation_pools allocation_pool
          ON allocation_pool.provider_cost_allocation_pool_id =
             allocation_line.provider_cost_allocation_pool_id
        JOIN LATERAL (
            SELECT
                COUNT(*) AS posting_count,
                COALESCE(SUM(posting.amount_micros::NUMERIC), 0)
                    AS posting_sum,
                MAX(posting.amount_micros::NUMERIC)
                    FILTER (WHERE posting.amount_micros > 0)
                    AS positive_amount,
                MIN(posting.amount_micros::NUMERIC)
                    FILTER (WHERE posting.amount_micros < 0)
                    AS negative_amount,
                MAX(account.account_key)
                    FILTER (WHERE posting.amount_micros > 0)
                    AS positive_account_key,
                MAX(account.owner_type)
                    FILTER (WHERE posting.amount_micros > 0)
                    AS positive_owner_type,
                MAX(account.owner_id)
                    FILTER (WHERE posting.amount_micros > 0)
                    AS positive_owner_id,
                MAX(account.account_type)
                    FILTER (WHERE posting.amount_micros > 0)
                    AS positive_account_type,
                MAX(account.account_key)
                    FILTER (WHERE posting.amount_micros < 0)
                    AS negative_account_key,
                MAX(account.owner_type)
                    FILTER (WHERE posting.amount_micros < 0)
                    AS negative_owner_type,
                MAX(account.owner_id)
                    FILTER (WHERE posting.amount_micros < 0)
                    AS negative_owner_id,
                MAX(account.account_type)
                    FILTER (WHERE posting.amount_micros < 0)
                    AS negative_account_type
            FROM ledger_postings posting
            JOIN ledger_accounts account
              ON account.account_id = posting.account_id
             AND account.currency = posting.currency
            WHERE posting.transaction_id = ledger_tx.transaction_id
        ) postings ON TRUE
        JOIN LATERAL (
            SELECT COUNT(*) AS seal_count
            FROM ledger_transaction_seals seal
            WHERE seal.transaction_id = ledger_tx.transaction_id
        ) seals ON TRUE
        WHERE ledger_tx.transaction_type = 'provider_cost'
          AND (
              ledger_tx.source_provider_cost_observation_id IS NOT NULL
              OR ledger_tx.source_provider_cost_allocation_line_id IS NOT NULL
          )
          AND (
              COALESCE(
                  observation.amount_micros,
                  allocation_line.amount_micros
              ) <= 0
              OR ledger_tx.currency IS DISTINCT FROM
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
              OR postings.positive_account_key IS DISTINCT FROM
                 'platform:' ||
                 COALESCE(observation.currency, allocation_pool.currency) ||
                 ':provider-expense'
              OR postings.positive_owner_type IS DISTINCT FROM 'platform'
              OR postings.positive_owner_id IS DISTINCT FROM 'platform'
              OR postings.positive_account_type IS DISTINCT FROM 'expense'
              OR postings.negative_account_key IS DISTINCT FROM
                 'provider:' ||
                 COALESCE(observation.provider_id, allocation_line.provider_id) ||
                 ':' ||
                 COALESCE(observation.currency, allocation_pool.currency) ||
                 ':payable'
              OR postings.negative_owner_type IS DISTINCT FROM 'provider'
              OR postings.negative_owner_id IS DISTINCT FROM
                 COALESCE(observation.provider_id, allocation_line.provider_id)
              OR postings.negative_account_type IS DISTINCT FROM 'payable'
              OR seals.seal_count <> 1
              OR (
                  observation.provider_cost_observation_id IS NOT NULL
                  AND (
                      ledger_tx.semantic_key IS DISTINCT FROM
                          'provider-cost-observation:v1:' ||
                          observation.observation_key
                      OR ledger_tx.payload_hash IS DISTINCT FROM
                          provider_cost_ledger_payload_hash(
                              ledger_tx.semantic_key,
                              ledger_tx.currency,
                              observation.amount_micros,
                              observation.provider_id
                          )
                  )
              )
          )
    ) THEN
        RAISE EXCEPTION 'existing provider cost ledgers violate their accounts or immutable identity'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM ledger_transactions ledger_tx
        LEFT JOIN provider_submissions submission
          ON submission.submission_id = ledger_tx.source_submission_id
         AND submission.output_id = ledger_tx.source_output_id
         AND submission.job_id = ledger_tx.source_job_id
        WHERE ledger_tx.transaction_type = 'provider_cost'
          AND ledger_tx.source_provider_cost_observation_id IS NULL
          AND ledger_tx.source_provider_cost_allocation_line_id IS NULL
          AND (
              ledger_tx.source_receipt_id IS NULL
              OR submission.provider_account_id IS NULL
          )
    ) THEN
        RAISE EXCEPTION 'a legacy provider cost lacks an attributable provider account'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE UNIQUE INDEX provider_cost_observation_fact_links_fact_uidx
    ON provider_cost_observation_fact_links(usage_fact_id);

CREATE UNIQUE INDEX provider_cost_observation_receipts_receipt_uidx
    ON provider_cost_observation_receipts(receipt_id);

ALTER TABLE provider_cost_observations
    DROP CONSTRAINT
        provider_cost_observations_provider_id_execution_surface_pr_key,
    ADD CONSTRAINT provider_cost_observations_operation_account_unique
        UNIQUE (
            provider_id, provider_account_id,
            execution_surface, provider_operation_id
        );

CREATE FUNCTION validate_provider_cost_fact_link()
RETURNS TRIGGER AS $$
DECLARE
    observation provider_cost_observations%ROWTYPE;
    fact provider_usage_facts%ROWTYPE;
BEGIN
    SELECT * INTO STRICT observation
    FROM provider_cost_observations
    WHERE provider_cost_observation_id =
          NEW.provider_cost_observation_id;

    SELECT * INTO STRICT fact
    FROM provider_usage_facts
    WHERE usage_fact_id = NEW.usage_fact_id;

    IF NEW.provider_id <> observation.provider_id
       OR NEW.provider_account_id <> observation.provider_account_id
       OR NEW.execution_surface <> observation.execution_surface
       OR fact.provider_id <> observation.provider_id
       OR fact.provider_account_id <> observation.provider_account_id
       OR fact.execution_surface <> observation.execution_surface
       OR fact.fact_domain <> 'provider_actual'
       OR fact.metric <> 'provider_reported_cost'
       OR fact.unit <> observation.native_unit
       OR fact.quantity_source <> observation.authority
       OR fact.confidence <> observation.confidence THEN
        RAISE EXCEPTION 'provider cost fact link is outside its authority contract'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_observation_fact_links_validate_contract
BEFORE INSERT ON provider_cost_observation_fact_links
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_fact_link();

CREATE OR REPLACE FUNCTION validate_provider_cost_observation_fact_set()
RETURNS TRIGGER AS $$
DECLARE
    target_observation_id UUID;
    observation provider_cost_observations%ROWTYPE;
    book_purpose TEXT;
    book_provider_id TEXT;
    version_provider_id TEXT;
    version_billing_mode TEXT;
    linked_count BIGINT;
    valid_count BIGINT;
    linked_quantity NUMERIC(38, 0);
    linked_fact_set_hash TEXT;
    ledger_count BIGINT;
BEGIN
    target_observation_id :=
        COALESCE(
            NEW.provider_cost_observation_id,
            OLD.provider_cost_observation_id
        );

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

    SELECT
        COUNT(*),
        COUNT(*) FILTER (
            WHERE fact.provider_id = observation.provider_id
              AND fact.provider_account_id =
                  observation.provider_account_id
              AND fact.execution_surface =
                  observation.execution_surface
              AND fact.fact_domain = 'provider_actual'
              AND fact.metric = 'provider_reported_cost'
              AND fact.unit = observation.native_unit
              AND fact.quantity_source = observation.authority
              AND fact.confidence = observation.confidence
        ),
        COALESCE(SUM(fact.quantity::NUMERIC), 0),
        encode(
            sha256(
                string_agg(
                    uuid_send(fact.usage_fact_id),
                    ''::BYTEA
                    ORDER BY fact.usage_fact_id
                )
            ),
            'hex'
        )
      INTO linked_count, valid_count, linked_quantity,
           linked_fact_set_hash
    FROM provider_cost_observation_fact_links link
    JOIN provider_usage_facts fact
      ON fact.usage_fact_id = link.usage_fact_id
    WHERE link.provider_cost_observation_id = target_observation_id;

    IF linked_count = 0
       OR valid_count <> linked_count
       OR linked_quantity <> observation.native_quantity
       OR linked_fact_set_hash <> observation.fact_set_hash THEN
        RAISE EXCEPTION 'provider cost observation does not exactly cover cost facts'
            USING ERRCODE = '23514';
    END IF;

    SELECT COUNT(*) INTO ledger_count
    FROM ledger_transactions ledger_tx
    WHERE ledger_tx.transaction_type = 'provider_cost'
      AND ledger_tx.source_provider_cost_observation_id =
          target_observation_id;

    IF (observation.amount_micros > 0 AND ledger_count <> 1)
       OR (observation.amount_micros = 0 AND ledger_count <> 0) THEN
        RAISE EXCEPTION 'provider cost observation ledger coverage is incomplete'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT fact.receipt_id
        FROM provider_cost_observation_fact_links fact_link
        JOIN provider_usage_facts fact
          ON fact.usage_fact_id = fact_link.usage_fact_id
        WHERE fact_link.provider_cost_observation_id =
              target_observation_id
        EXCEPT
        SELECT receipt_link.receipt_id
        FROM provider_cost_observation_receipts receipt_link
        WHERE receipt_link.provider_cost_observation_id =
              target_observation_id
    )
    OR EXISTS (
        SELECT receipt_link.receipt_id
        FROM provider_cost_observation_receipts receipt_link
        WHERE receipt_link.provider_cost_observation_id =
              target_observation_id
        EXCEPT
        SELECT fact.receipt_id
        FROM provider_cost_observation_fact_links fact_link
        JOIN provider_usage_facts fact
          ON fact.usage_fact_id = fact_link.usage_fact_id
        WHERE fact_link.provider_cost_observation_id =
              target_observation_id
    ) THEN
        RAISE EXCEPTION 'provider cost receipt links do not equal the fact set'
            USING ERRCODE = '23514';
    END IF;

    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE EXTENSION IF NOT EXISTS btree_gist;

ALTER TABLE provider_cost_allocation_pools
    ADD CONSTRAINT provider_cost_allocation_pools_closed_period_excl
    EXCLUDE USING gist (
        provider_account_id WITH =,
        currency WITH =,
        int8range(period_start_ms, period_end_ms, '[)') WITH &&
    )
    WHERE (state = 'closed')
    DEFERRABLE INITIALLY IMMEDIATE;

CREATE TABLE provider_cost_authority_claims (
    claim_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_account_id UUID NOT NULL,
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE RESTRICT,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    authority_kind TEXT NOT NULL CHECK (
        authority_kind IN (
            'provider_actual',
            'provider_allocated',
            'provider_legacy'
        )
    ),
    authority_period INT8RANGE NOT NULL CHECK (
        NOT isempty(authority_period)
        AND lower_inc(authority_period)
        AND NOT upper_inc(authority_period)
    ),
    source_provider_cost_observation_id UUID
        REFERENCES provider_cost_observations(provider_cost_observation_id)
        ON DELETE RESTRICT,
    source_usage_fact_id UUID
        REFERENCES provider_usage_facts(usage_fact_id)
        ON DELETE RESTRICT,
    source_provider_cost_allocation_pool_id UUID,
    source_provider_cost_allocation_line_id UUID,
    source_legacy_transaction_id UUID
        REFERENCES ledger_transactions(transaction_id)
        ON DELETE RESTRICT,
    created_at_ms BIGINT NOT NULL,
    FOREIGN KEY (provider_account_id, provider_id)
        REFERENCES provider_accounts(provider_account_id, provider_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (
        source_provider_cost_allocation_line_id,
        source_provider_cost_allocation_pool_id
    ) REFERENCES provider_cost_allocation_lines(
        provider_cost_allocation_line_id,
        provider_cost_allocation_pool_id
    ) ON DELETE RESTRICT,
    CHECK (
        (
            authority_kind = 'provider_actual'
            AND source_provider_cost_observation_id IS NOT NULL
            AND source_usage_fact_id IS NOT NULL
            AND source_provider_cost_allocation_pool_id IS NULL
            AND source_provider_cost_allocation_line_id IS NULL
            AND source_legacy_transaction_id IS NULL
        )
        OR
        (
            authority_kind = 'provider_allocated'
            AND source_provider_cost_observation_id IS NULL
            AND source_usage_fact_id IS NULL
            AND source_provider_cost_allocation_pool_id IS NOT NULL
            AND source_provider_cost_allocation_line_id IS NOT NULL
            AND source_legacy_transaction_id IS NULL
        )
        OR
        (
            authority_kind = 'provider_legacy'
            AND source_provider_cost_observation_id IS NULL
            AND source_usage_fact_id IS NULL
            AND source_provider_cost_allocation_pool_id IS NULL
            AND source_provider_cost_allocation_line_id IS NULL
            AND source_legacy_transaction_id IS NOT NULL
        )
    )
);

CREATE UNIQUE INDEX provider_cost_authority_actual_fact_uidx
    ON provider_cost_authority_claims(source_usage_fact_id)
    WHERE authority_kind = 'provider_actual';

CREATE UNIQUE INDEX provider_cost_authority_allocation_line_uidx
    ON provider_cost_authority_claims(
        source_provider_cost_allocation_line_id
    )
    WHERE authority_kind = 'provider_allocated';

CREATE UNIQUE INDEX provider_cost_authority_legacy_transaction_uidx
    ON provider_cost_authority_claims(source_legacy_transaction_id)
    WHERE authority_kind = 'provider_legacy';

ALTER TABLE provider_cost_authority_claims
    ADD CONSTRAINT provider_cost_authority_period_excl
    EXCLUDE USING gist (
        provider_id WITH =,
        provider_account_id WITH =,
        job_id WITH =,
        currency WITH =,
        authority_period WITH &&,
        authority_kind WITH <>
    )
    DEFERRABLE INITIALLY IMMEDIATE;

CREATE OR REPLACE FUNCTION validate_provider_cost_ledger_amount()
RETURNS TRIGGER AS $$
DECLARE
    target_transaction_id UUID;
    transaction_row ledger_transactions%ROWTYPE;
    expected_currency TEXT;
    expected_amount_micros BIGINT;
    expected_provider_id TEXT;
    expected_observation_key TEXT;
    posting_count BIGINT;
    posting_sum NUMERIC;
    positive_amount NUMERIC;
    negative_amount NUMERIC;
    positive_account_key TEXT;
    positive_owner_type TEXT;
    positive_owner_id TEXT;
    positive_account_type TEXT;
    negative_account_key TEXT;
    negative_owner_type TEXT;
    negative_owner_id TEXT;
    negative_account_type TEXT;
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
        SELECT currency, amount_micros, provider_id, observation_key
          INTO STRICT expected_currency, expected_amount_micros,
                      expected_provider_id, expected_observation_key
        FROM provider_cost_observations
        WHERE provider_cost_observation_id =
              transaction_row.source_provider_cost_observation_id;
    ELSE
        SELECT pool.currency, line.amount_micros, line.provider_id, NULL
          INTO STRICT expected_currency, expected_amount_micros,
                      expected_provider_id, expected_observation_key
        FROM provider_cost_allocation_lines line
        JOIN provider_cost_allocation_pools pool
          ON pool.provider_cost_allocation_pool_id =
             line.provider_cost_allocation_pool_id
        WHERE line.provider_cost_allocation_line_id =
              transaction_row.source_provider_cost_allocation_line_id
          AND line.provider_cost_allocation_pool_id =
              transaction_row.source_provider_cost_allocation_pool_id;
    END IF;

    SELECT
        COUNT(*),
        COALESCE(SUM(posting.amount_micros::NUMERIC), 0),
        MAX(posting.amount_micros::NUMERIC)
            FILTER (WHERE posting.amount_micros > 0),
        MIN(posting.amount_micros::NUMERIC)
            FILTER (WHERE posting.amount_micros < 0),
        MAX(account.account_key)
            FILTER (WHERE posting.amount_micros > 0),
        MAX(account.owner_type)
            FILTER (WHERE posting.amount_micros > 0),
        MAX(account.owner_id)
            FILTER (WHERE posting.amount_micros > 0),
        MAX(account.account_type)
            FILTER (WHERE posting.amount_micros > 0),
        MAX(account.account_key)
            FILTER (WHERE posting.amount_micros < 0),
        MAX(account.owner_type)
            FILTER (WHERE posting.amount_micros < 0),
        MAX(account.owner_id)
            FILTER (WHERE posting.amount_micros < 0),
        MAX(account.account_type)
            FILTER (WHERE posting.amount_micros < 0)
      INTO posting_count, posting_sum, positive_amount, negative_amount,
           positive_account_key, positive_owner_type, positive_owner_id,
           positive_account_type, negative_account_key,
           negative_owner_type, negative_owner_id, negative_account_type
    FROM ledger_postings posting
    JOIN ledger_accounts account
      ON account.account_id = posting.account_id
     AND account.currency = posting.currency
    WHERE posting.transaction_id = target_transaction_id;

    SELECT COUNT(*) INTO seal_count
    FROM ledger_transaction_seals
    WHERE transaction_id = target_transaction_id;

    IF expected_amount_micros <= 0
       OR transaction_row.currency <> expected_currency
       OR posting_count <> 2
       OR posting_sum <> 0
       OR positive_amount <> expected_amount_micros::NUMERIC
       OR negative_amount <> -expected_amount_micros::NUMERIC
       OR positive_account_key <>
          'platform:' || expected_currency || ':provider-expense'
       OR positive_owner_type <> 'platform'
       OR positive_owner_id <> 'platform'
       OR positive_account_type <> 'expense'
       OR negative_account_key <>
          'provider:' || expected_provider_id || ':' ||
          expected_currency || ':payable'
       OR negative_owner_type <> 'provider'
       OR negative_owner_id <> expected_provider_id
       OR negative_account_type <> 'payable'
       OR seal_count <> 1 THEN
        RAISE EXCEPTION 'provider cost ledger does not match its authority'
            USING ERRCODE = '23514';
    END IF;

    IF expected_observation_key IS NOT NULL
       AND (
           transaction_row.semantic_key <>
               'provider-cost-observation:v1:' ||
               expected_observation_key
           OR transaction_row.payload_hash <>
               provider_cost_ledger_payload_hash(
                   transaction_row.semantic_key,
                   transaction_row.currency,
                   expected_amount_micros,
                   expected_provider_id
               )
       ) THEN
        RAISE EXCEPTION 'provider actual cost ledger identity is invalid'
            USING ERRCODE = '23514';
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

INSERT INTO provider_cost_authority_claims (
    provider_id, provider_account_id, job_id, currency,
    authority_kind, authority_period,
    source_provider_cost_observation_id, source_usage_fact_id,
    created_at_ms
)
SELECT
    fact.provider_id,
    fact.provider_account_id,
    fact.job_id,
    observation.currency,
    'provider_actual',
    int8range(receipt.created_at_ms, receipt.created_at_ms + 1, '[)'),
    link.provider_cost_observation_id,
    link.usage_fact_id,
    link.created_at_ms
FROM provider_cost_observation_fact_links link
JOIN provider_cost_observations observation
  ON observation.provider_cost_observation_id =
     link.provider_cost_observation_id
JOIN provider_usage_facts fact
  ON fact.usage_fact_id = link.usage_fact_id
JOIN provider_receipts receipt
  ON receipt.receipt_id = fact.receipt_id;

INSERT INTO provider_cost_authority_claims (
    provider_id, provider_account_id, job_id, currency,
    authority_kind, authority_period,
    source_provider_cost_allocation_pool_id,
    source_provider_cost_allocation_line_id,
    created_at_ms
)
SELECT
    line.provider_id,
    line.provider_account_id,
    line.job_id,
    pool.currency,
    'provider_allocated',
    int8range(pool.period_start_ms, pool.period_end_ms, '[)'),
    line.provider_cost_allocation_pool_id,
    line.provider_cost_allocation_line_id,
    pool.closed_at_ms
FROM provider_cost_allocation_lines line
JOIN provider_cost_allocation_pools pool
  ON pool.provider_cost_allocation_pool_id =
     line.provider_cost_allocation_pool_id
WHERE pool.state = 'closed';

INSERT INTO provider_cost_authority_claims (
    provider_id, provider_account_id, job_id, currency,
    authority_kind, authority_period,
    source_legacy_transaction_id, created_at_ms
)
SELECT
    submission.provider_id,
    submission.provider_account_id,
    ledger_tx.source_job_id,
    ledger_tx.currency,
    'provider_legacy',
    int8range(receipt.created_at_ms, receipt.created_at_ms + 1, '[)'),
    ledger_tx.transaction_id,
    ledger_tx.created_at_ms
FROM ledger_transactions ledger_tx
JOIN provider_submissions submission
  ON submission.submission_id = ledger_tx.source_submission_id
 AND submission.output_id = ledger_tx.source_output_id
 AND submission.job_id = ledger_tx.source_job_id
JOIN provider_receipts receipt
  ON receipt.receipt_id = ledger_tx.source_receipt_id
WHERE ledger_tx.transaction_type = 'provider_cost'
  AND ledger_tx.source_provider_cost_observation_id IS NULL
  AND ledger_tx.source_provider_cost_allocation_line_id IS NULL;

CREATE FUNCTION claim_provider_actual_cost_authority()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO provider_cost_authority_claims (
        provider_id, provider_account_id, job_id, currency,
        authority_kind, authority_period,
        source_provider_cost_observation_id, source_usage_fact_id,
        created_at_ms
    )
    SELECT
        fact.provider_id,
        fact.provider_account_id,
        fact.job_id,
        observation.currency,
        'provider_actual',
        int8range(receipt.created_at_ms, receipt.created_at_ms + 1, '[)'),
        NEW.provider_cost_observation_id,
        NEW.usage_fact_id,
        NEW.created_at_ms
    FROM provider_usage_facts fact
    JOIN provider_receipts receipt
      ON receipt.receipt_id = fact.receipt_id
    JOIN provider_cost_observations observation
      ON observation.provider_cost_observation_id =
         NEW.provider_cost_observation_id
    WHERE fact.usage_fact_id = NEW.usage_fact_id
      AND fact.provider_id = NEW.provider_id
      AND fact.provider_account_id = NEW.provider_account_id
      AND fact.execution_surface = NEW.execution_surface;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'provider actual cost authority is not attributable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION claim_closed_provider_allocation_authority()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.state <> 'closed'
       OR (TG_OP = 'UPDATE' AND OLD.state = 'closed') THEN
        RETURN NEW;
    END IF;

    INSERT INTO provider_cost_authority_claims (
        provider_id, provider_account_id, job_id, currency,
        authority_kind, authority_period,
        source_provider_cost_allocation_pool_id,
        source_provider_cost_allocation_line_id,
        created_at_ms
    )
    SELECT
        line.provider_id,
        line.provider_account_id,
        line.job_id,
        NEW.currency,
        'provider_allocated',
        int8range(NEW.period_start_ms, NEW.period_end_ms, '[)'),
        line.provider_cost_allocation_pool_id,
        line.provider_cost_allocation_line_id,
        NEW.closed_at_ms
    FROM provider_cost_allocation_lines line
    WHERE line.provider_cost_allocation_pool_id =
          NEW.provider_cost_allocation_pool_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION claim_legacy_provider_cost_authority()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.transaction_type <> 'provider_cost'
       OR NEW.source_provider_cost_observation_id IS NOT NULL
       OR NEW.source_provider_cost_allocation_line_id IS NOT NULL THEN
        RETURN NEW;
    END IF;

    INSERT INTO provider_cost_authority_claims (
        provider_id, provider_account_id, job_id, currency,
        authority_kind, authority_period,
        source_legacy_transaction_id, created_at_ms
    )
    SELECT
        submission.provider_id,
        submission.provider_account_id,
        NEW.source_job_id,
        NEW.currency,
        'provider_legacy',
        int8range(receipt.created_at_ms, receipt.created_at_ms + 1, '[)'),
        NEW.transaction_id,
        NEW.created_at_ms
    FROM provider_submissions submission
    JOIN provider_receipts receipt
      ON receipt.receipt_id = NEW.source_receipt_id
    WHERE submission.submission_id = NEW.source_submission_id
      AND submission.output_id = NEW.source_output_id
      AND submission.job_id = NEW.source_job_id
      AND submission.provider_account_id IS NOT NULL;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'legacy provider cost authority is not attributable'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_observation_fact_links_claim_authority
AFTER INSERT ON provider_cost_observation_fact_links
FOR EACH ROW EXECUTE FUNCTION claim_provider_actual_cost_authority();

CREATE TRIGGER provider_cost_allocation_pools_claim_authority
AFTER INSERT OR UPDATE ON provider_cost_allocation_pools
FOR EACH ROW EXECUTE FUNCTION claim_closed_provider_allocation_authority();

CREATE TRIGGER ledger_transactions_claim_legacy_provider_cost_authority
AFTER INSERT ON ledger_transactions
FOR EACH ROW EXECUTE FUNCTION claim_legacy_provider_cost_authority();

CREATE FUNCTION preserve_provider_cost_authority_claim()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'provider cost authority claims are immutable'
        USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_authority_claims_reject_mutation
BEFORE UPDATE OR DELETE ON provider_cost_authority_claims
FOR EACH ROW EXECUTE FUNCTION preserve_provider_cost_authority_claim();

CREATE TRIGGER provider_cost_authority_claims_reject_truncate
BEFORE TRUNCATE ON provider_cost_authority_claims
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();
