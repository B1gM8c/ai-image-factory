DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_cost_allocation_pools
        WHERE state = 'closed'
    ) THEN
        RAISE EXCEPTION
            'closed provider allocations must be migrated with their original source evidence'
            USING ERRCODE = '55000';
    END IF;
END;
$$;

ALTER TABLE provider_cost_allocation_pools
    ADD COLUMN candidate_snapshot_hash TEXT;

ALTER TABLE provider_cost_allocation_lines
    ADD COLUMN basis_receipt_id UUID,
    ADD COLUMN basis_receipt_payload_hash TEXT,
    ADD COLUMN basis_quote_id UUID,
    ADD COLUMN basis_quote_hash TEXT;

WITH chosen_basis AS (
    SELECT DISTINCT ON (line.provider_cost_allocation_line_id)
        line.provider_cost_allocation_line_id,
        receipt.receipt_id,
        receipt.payload_hash,
        quote.quote_id,
        quote.quote_hash
    FROM provider_cost_allocation_lines line
    JOIN provider_cost_allocation_pools pool
      ON pool.provider_cost_allocation_pool_id =
         line.provider_cost_allocation_pool_id
    JOIN provider_submissions submission
      ON submission.job_id = line.job_id
     AND submission.provider_id = line.provider_id
     AND submission.provider_account_id = line.provider_account_id
     AND (
         line.output_id IS NULL
         OR submission.output_id = line.output_id
     )
    JOIN provider_receipts receipt
      ON receipt.submission_id = submission.submission_id
     AND receipt.output_id = submission.output_id
     AND receipt.job_id = submission.job_id
     AND receipt.provider_id = submission.provider_id
     AND receipt.outcome = 'succeeded'
     AND receipt.created_at_ms >= pool.period_start_ms
     AND receipt.created_at_ms < pool.period_end_ms
    JOIN customer_price_quotes quote
      ON quote.job_id = receipt.job_id
    ORDER BY
        line.provider_cost_allocation_line_id,
        receipt.created_at_ms,
        receipt.receipt_id
)
UPDATE provider_cost_allocation_lines line
SET basis_receipt_id = basis.receipt_id,
    basis_receipt_payload_hash = basis.payload_hash,
    basis_quote_id = basis.quote_id,
    basis_quote_hash = basis.quote_hash
FROM chosen_basis basis
WHERE basis.provider_cost_allocation_line_id =
      line.provider_cost_allocation_line_id;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_cost_allocation_lines
        WHERE basis_receipt_id IS NULL
           OR basis_receipt_payload_hash IS NULL
           OR basis_quote_id IS NULL
           OR basis_quote_hash IS NULL
    ) THEN
        RAISE EXCEPTION
            'provider allocation lines cannot be bound to immutable receipt snapshots'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

UPDATE provider_cost_allocation_pools
SET candidate_snapshot_hash = encode(
    sha256(
        convert_to(
            'provider-cost-allocation-legacy-snapshot:v1:' ||
            provider_cost_allocation_pool_id::TEXT,
            'UTF8'
        )
    ),
    'hex'
),
    control_version = control_version + 1
WHERE candidate_snapshot_hash IS NULL;

-- The legacy close guards are deferred constraint triggers. Drain their
-- backfill events before altering either table, then restore deferred mode for
-- the remainder of the migration transaction.
SET CONSTRAINTS ALL IMMEDIATE;
SET CONSTRAINTS ALL DEFERRED;

ALTER TABLE provider_cost_allocation_pools
    ALTER COLUMN candidate_snapshot_hash SET NOT NULL,
    ADD CONSTRAINT provider_cost_allocation_snapshot_hash_format
        CHECK (candidate_snapshot_hash ~ '^[0-9a-f]{64}$');

ALTER TABLE provider_cost_allocation_lines
    ALTER COLUMN basis_receipt_id SET NOT NULL,
    ALTER COLUMN basis_receipt_payload_hash SET NOT NULL,
    ALTER COLUMN basis_quote_id SET NOT NULL,
    ALTER COLUMN basis_quote_hash SET NOT NULL,
    ADD CONSTRAINT provider_cost_allocation_line_receipt_fk
        FOREIGN KEY (basis_receipt_id)
        REFERENCES provider_receipts(receipt_id)
        ON DELETE RESTRICT,
    ADD CONSTRAINT provider_cost_allocation_line_quote_fk
        FOREIGN KEY (basis_quote_id)
        REFERENCES customer_price_quotes(quote_id)
        ON DELETE RESTRICT,
    ADD CONSTRAINT provider_cost_allocation_line_receipt_hash_format
        CHECK (basis_receipt_payload_hash ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT provider_cost_allocation_line_quote_hash_format
        CHECK (basis_quote_hash ~ '^[0-9a-f]{64}$');

CREATE INDEX provider_cost_allocation_lines_receipt_idx
    ON provider_cost_allocation_lines(basis_receipt_id);

CREATE INDEX provider_cost_allocation_lines_quote_idx
    ON provider_cost_allocation_lines(basis_quote_id);

ALTER TABLE provider_cost_authority_claims
    ADD COLUMN source_receipt_id UUID;

UPDATE provider_cost_authority_claims claim
SET source_receipt_id = fact.receipt_id
FROM provider_usage_facts fact
WHERE claim.authority_kind = 'provider_actual'
  AND fact.usage_fact_id = claim.source_usage_fact_id;

UPDATE provider_cost_authority_claims claim
SET source_receipt_id = line.basis_receipt_id
FROM provider_cost_allocation_lines line
WHERE claim.authority_kind = 'provider_allocated'
  AND line.provider_cost_allocation_line_id =
      claim.source_provider_cost_allocation_line_id
  AND line.provider_cost_allocation_pool_id =
      claim.source_provider_cost_allocation_pool_id;

UPDATE provider_cost_authority_claims claim
SET source_receipt_id = ledger_tx.source_receipt_id
FROM ledger_transactions ledger_tx
WHERE claim.authority_kind = 'provider_legacy'
  AND ledger_tx.transaction_id = claim.source_legacy_transaction_id;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_cost_authority_claims
        WHERE source_receipt_id IS NULL
    ) THEN
        RAISE EXCEPTION
            'provider cost authority cannot be bound to one immutable receipt'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT source_receipt_id
        FROM provider_cost_authority_claims
        GROUP BY source_receipt_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'one provider receipt has multiple cost authorities'
            USING ERRCODE = '23P01';
    END IF;
END;
$$;

ALTER TABLE provider_cost_authority_claims
    ALTER COLUMN source_receipt_id SET NOT NULL,
    ADD CONSTRAINT provider_cost_authority_receipt_fk
        FOREIGN KEY (source_receipt_id)
        REFERENCES provider_receipts(receipt_id)
        ON DELETE RESTRICT;

CREATE UNIQUE INDEX provider_cost_authority_receipt_uidx
    ON provider_cost_authority_claims(source_receipt_id);

DROP INDEX provider_cost_obligations_actual_claim_uidx;

CREATE UNIQUE INDEX provider_cost_obligations_settlement_claim_uidx
    ON provider_cost_obligations(settlement_claim_id)
    WHERE settlement_claim_id IS NOT NULL;

CREATE TABLE provider_cost_allocation_closures (
    provider_cost_allocation_pool_id UUID PRIMARY KEY
        REFERENCES provider_cost_allocation_pools(
            provider_cost_allocation_pool_id
        )
        ON DELETE RESTRICT,
    idempotency_key_digest TEXT NOT NULL UNIQUE CHECK (
        idempotency_key_digest ~ '^[0-9a-f]{64}$'
    ),
    request_hash TEXT NOT NULL CHECK (
        request_hash ~ '^[0-9a-f]{64}$'
    ),
    candidate_snapshot_hash TEXT NOT NULL CHECK (
        candidate_snapshot_hash ~ '^[0-9a-f]{64}$'
    ),
    source_kind TEXT NOT NULL CHECK (
        source_kind IN (
            'provider_invoice',
            'provider_contract',
            'provider_subscription',
            'provider_statement'
        )
    ),
    source_reference TEXT NOT NULL CHECK (
        char_length(source_reference) BETWEEN 1 AND 512
        AND source_reference !~ '[[:cntrl:]]'
    ),
    source_evidence_hash TEXT NOT NULL CHECK (
        source_evidence_hash ~ '^[0-9a-f]{64}$'
    ),
    source_period_start_ms BIGINT NOT NULL,
    source_period_end_ms BIGINT NOT NULL CHECK (
        source_period_end_ms > source_period_start_ms
    ),
    source_currency TEXT NOT NULL CHECK (
        source_currency ~ '^[A-Z]{3}$'
    ),
    source_amount_micros BIGINT NOT NULL CHECK (
        source_amount_micros >= 0
    ),
    closed_by_user_id UUID NOT NULL
        REFERENCES identity_users(user_id)
        ON DELETE RESTRICT,
    closed_by_session_id UUID NOT NULL,
    created_at_ms BIGINT NOT NULL
);

CREATE FUNCTION validate_provider_cost_authority_receipt()
RETURNS TRIGGER AS $$
DECLARE
    receipt provider_receipts%ROWTYPE;
    submission provider_submissions%ROWTYPE;
    fact provider_usage_facts%ROWTYPE;
    observation provider_cost_observations%ROWTYPE;
    line provider_cost_allocation_lines%ROWTYPE;
    pool provider_cost_allocation_pools%ROWTYPE;
    ledger_tx ledger_transactions%ROWTYPE;
BEGIN
    SELECT * INTO STRICT receipt
    FROM provider_receipts
    WHERE receipt_id = NEW.source_receipt_id;

    SELECT * INTO STRICT submission
    FROM provider_submissions
    WHERE submission_id = receipt.submission_id;

    IF NEW.authority_kind = 'provider_actual' THEN
        SELECT * INTO STRICT fact
        FROM provider_usage_facts
        WHERE usage_fact_id = NEW.source_usage_fact_id;

        SELECT * INTO STRICT observation
        FROM provider_cost_observations
        WHERE provider_cost_observation_id =
              NEW.source_provider_cost_observation_id;

        IF NOT EXISTS (
            SELECT 1
            FROM provider_cost_observation_fact_links link
            WHERE link.provider_cost_observation_id =
                  NEW.source_provider_cost_observation_id
              AND link.usage_fact_id = NEW.source_usage_fact_id
        )
           OR fact.receipt_id <> NEW.source_receipt_id
           OR fact.provider_id <> NEW.provider_id
           OR fact.provider_account_id IS DISTINCT FROM
              NEW.provider_account_id
           OR fact.job_id <> NEW.job_id
           OR observation.provider_id <> NEW.provider_id
           OR observation.provider_account_id <>
              NEW.provider_account_id
           OR observation.currency <> NEW.currency
           OR NEW.authority_period <>
              int8range(
                  receipt.created_at_ms,
                  receipt.created_at_ms + 1,
                  '[)'
              ) THEN
            RAISE EXCEPTION
                'provider actual authority does not match its receipt'
                USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.authority_kind = 'provider_allocated' THEN
        SELECT * INTO STRICT line
        FROM provider_cost_allocation_lines
        WHERE provider_cost_allocation_line_id =
              NEW.source_provider_cost_allocation_line_id
          AND provider_cost_allocation_pool_id =
              NEW.source_provider_cost_allocation_pool_id;

        SELECT * INTO STRICT pool
        FROM provider_cost_allocation_pools
        WHERE provider_cost_allocation_pool_id =
              NEW.source_provider_cost_allocation_pool_id;

        IF pool.state <> 'closed'
           OR pool.allocation_basis <> 'successful_output'
           OR NOT EXISTS (
               SELECT 1
               FROM provider_cost_allocation_closures closure
               WHERE closure.provider_cost_allocation_pool_id =
                     pool.provider_cost_allocation_pool_id
           )
           OR line.basis_receipt_id <> NEW.source_receipt_id
           OR line.provider_id <> NEW.provider_id
           OR line.provider_account_id <> NEW.provider_account_id
           OR line.job_id <> NEW.job_id
           OR line.output_id IS NULL
           OR line.output_id <> receipt.output_id
           OR pool.currency <> NEW.currency
           OR NEW.authority_period <>
              int8range(pool.period_start_ms, pool.period_end_ms, '[)') THEN
            RAISE EXCEPTION
                'provider allocated authority does not match its closed receipt snapshot'
                USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.authority_kind = 'provider_legacy' THEN
        SELECT * INTO STRICT ledger_tx
        FROM ledger_transactions
        WHERE transaction_id = NEW.source_legacy_transaction_id;

        IF ledger_tx.source_receipt_id <> NEW.source_receipt_id
           OR ledger_tx.source_job_id <> NEW.job_id
           OR ledger_tx.currency <> NEW.currency
           OR receipt.provider_id <> NEW.provider_id
           OR submission.provider_account_id IS DISTINCT FROM
              NEW.provider_account_id
           OR NEW.authority_period <>
              int8range(
                  receipt.created_at_ms,
                  receipt.created_at_ms + 1,
                  '[)'
              ) THEN
            RAISE EXCEPTION
                'legacy provider authority does not match its receipt'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_authority_claims_validate_receipt
BEFORE INSERT ON provider_cost_authority_claims
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_authority_receipt();

CREATE OR REPLACE FUNCTION claim_provider_actual_cost_authority()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO provider_cost_authority_claims (
        provider_id, provider_account_id, job_id, currency,
        authority_kind, authority_period, source_receipt_id,
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
        receipt.receipt_id,
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

CREATE OR REPLACE FUNCTION claim_closed_provider_allocation_authority()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.state <> 'closed'
       OR (TG_OP = 'UPDATE' AND OLD.state = 'closed') THEN
        RETURN NEW;
    END IF;

    INSERT INTO provider_cost_authority_claims (
        provider_id, provider_account_id, job_id, currency,
        authority_kind, authority_period, source_receipt_id,
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
        line.basis_receipt_id,
        line.provider_cost_allocation_pool_id,
        line.provider_cost_allocation_line_id,
        NEW.closed_at_ms
    FROM provider_cost_allocation_lines line
    WHERE line.provider_cost_allocation_pool_id =
          NEW.provider_cost_allocation_pool_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION claim_legacy_provider_cost_authority()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.transaction_type <> 'provider_cost'
       OR NEW.source_provider_cost_observation_id IS NOT NULL
       OR NEW.source_provider_cost_allocation_line_id IS NOT NULL THEN
        RETURN NEW;
    END IF;

    INSERT INTO provider_cost_authority_claims (
        provider_id, provider_account_id, job_id, currency,
        authority_kind, authority_period, source_receipt_id,
        source_legacy_transaction_id, created_at_ms
    )
    SELECT
        submission.provider_id,
        submission.provider_account_id,
        NEW.source_job_id,
        NEW.currency,
        'provider_legacy',
        int8range(receipt.created_at_ms, receipt.created_at_ms + 1, '[)'),
        receipt.receipt_id,
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

CREATE OR REPLACE FUNCTION settle_provider_cost_obligations_from_claim()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.authority_kind IN ('provider_actual', 'provider_allocated') THEN
        UPDATE provider_cost_obligations obligation
        SET state = 'settled',
            expected_authority_kind = NEW.authority_kind,
            currency = NEW.currency,
            settlement_claim_id = NEW.claim_id,
            pending_reason_code = NULL,
            settled_at_ms = NEW.created_at_ms,
            updated_at_ms = NEW.created_at_ms,
            control_version = obligation.control_version + 1
        WHERE obligation.receipt_id = NEW.source_receipt_id
          AND obligation.provider_id = NEW.provider_id
          AND obligation.provider_account_id =
              NEW.provider_account_id
          AND obligation.job_id = NEW.job_id
          AND obligation.state IN ('expected', 'pending');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_provider_cost_allocation_receipt_snapshot()
RETURNS TRIGGER AS $$
DECLARE
    pool provider_cost_allocation_pools%ROWTYPE;
    receipt provider_receipts%ROWTYPE;
    submission provider_submissions%ROWTYPE;
    quote customer_price_quotes%ROWTYPE;
    book price_books%ROWTYPE;
    version price_book_versions%ROWTYPE;
BEGIN
    SELECT * INTO STRICT pool
    FROM provider_cost_allocation_pools
    WHERE provider_cost_allocation_pool_id =
          NEW.provider_cost_allocation_pool_id;

    SELECT * INTO STRICT receipt
    FROM provider_receipts
    WHERE receipt_id = NEW.basis_receipt_id;

    SELECT * INTO STRICT submission
    FROM provider_submissions
    WHERE submission_id = receipt.submission_id;

    SELECT * INTO STRICT quote
    FROM customer_price_quotes
    WHERE quote_id = NEW.basis_quote_id;

    SELECT * INTO STRICT version
    FROM price_book_versions
    WHERE price_book_version_id = pool.price_book_version_id;

    SELECT * INTO STRICT book
    FROM price_books
    WHERE price_book_id = version.price_book_id;

    IF receipt.payload_hash <> NEW.basis_receipt_payload_hash
       OR quote.quote_hash <> NEW.basis_quote_hash
       OR receipt.job_id <> NEW.job_id
       OR receipt.provider_id <> NEW.provider_id
       OR receipt.outcome <> 'succeeded'
       OR receipt.created_at_ms < pool.period_start_ms
       OR receipt.created_at_ms >= pool.period_end_ms
       OR submission.provider_account_id IS DISTINCT FROM
          NEW.provider_account_id
       OR submission.job_id <> NEW.job_id
       OR submission.provider_id <> NEW.provider_id
       OR (
           NEW.output_id IS NOT NULL
           AND receipt.output_id <> NEW.output_id
       )
       OR quote.job_id <> NEW.job_id
       OR quote.provider_id IS DISTINCT FROM NEW.provider_id
       OR (
           version.api_profile <> '*'
           AND quote.api_profile <> version.api_profile
           AND NOT EXISTS (
               SELECT 1
               FROM api_profile_pricing_aliases alias
               WHERE alias.api_profile = quote.api_profile
                 AND alias.pricing_api_profile =
                     version.api_profile
           )
       )
       OR version.operation NOT IN ('*', quote.operation)
       OR (
           version.provider_model_id IS NOT NULL
           AND version.provider_model_id IS DISTINCT FROM
               quote.provider_model_id
       )
       OR version.public_model_id NOT IN ('*', quote.public_model_id)
       OR version.media_kind <> quote.media_kind
       OR version.service_tier NOT IN ('*', quote.service_tier)
       OR version.execution_surface <> quote.execution_surface
       OR (
           book.scope_type = 'organization'
           AND quote.tenant_id <> book.organization_id
       )
       OR (
           book.scope_type = 'project'
           AND (
               quote.tenant_id <> book.organization_id
               OR quote.project_id <> book.project_id
           )
       ) THEN
        RAISE EXCEPTION
            'provider allocation receipt snapshot is outside its price surface'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_allocation_lines_validate_receipt_snapshot
BEFORE INSERT OR UPDATE ON provider_cost_allocation_lines
FOR EACH ROW
EXECUTE FUNCTION validate_provider_cost_allocation_receipt_snapshot();

CREATE FUNCTION validate_provider_cost_allocation_closure()
RETURNS TRIGGER AS $$
DECLARE
    pool provider_cost_allocation_pools%ROWTYPE;
BEGIN
    SELECT * INTO STRICT pool
    FROM provider_cost_allocation_pools
    WHERE provider_cost_allocation_pool_id =
          NEW.provider_cost_allocation_pool_id
    FOR UPDATE;

    IF pool.state <> 'draft'
       OR pool.allocation_basis <> 'successful_output'
       OR pool.residual_amount_micros <> 0
       OR NEW.candidate_snapshot_hash <>
          pool.candidate_snapshot_hash
       OR NEW.source_period_start_ms <> pool.period_start_ms
       OR NEW.source_period_end_ms <> pool.period_end_ms
       OR NEW.source_currency <> pool.currency
       OR NEW.source_amount_micros <> pool.total_amount_micros THEN
        RAISE EXCEPTION
            'provider allocation closure does not match its immutable draft'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_allocation_closures_validate
BEFORE INSERT ON provider_cost_allocation_closures
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_allocation_closure();

CREATE FUNCTION preserve_provider_cost_allocation_closure()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION
        'provider allocation closures are immutable'
        USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_allocation_closures_reject_mutation
BEFORE UPDATE OR DELETE ON provider_cost_allocation_closures
FOR EACH ROW EXECUTE FUNCTION preserve_provider_cost_allocation_closure();

CREATE TRIGGER provider_cost_allocation_closures_reject_truncate
BEFORE TRUNCATE ON provider_cost_allocation_closures
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

CREATE OR REPLACE FUNCTION preserve_provider_cost_allocation_pool()
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

    IF NEW.provider_cost_allocation_pool_id <>
       OLD.provider_cost_allocation_pool_id
       OR NEW.semantic_key <> OLD.semantic_key
       OR NEW.provider_id <> OLD.provider_id
       OR NEW.provider_account_id <> OLD.provider_account_id
       OR NEW.price_book_version_id <> OLD.price_book_version_id
       OR NEW.period_start_ms <> OLD.period_start_ms
       OR NEW.period_end_ms <> OLD.period_end_ms
       OR NEW.currency <> OLD.currency
       OR NEW.total_amount_micros <> OLD.total_amount_micros
       OR NEW.residual_amount_micros <> OLD.residual_amount_micros
       OR NEW.allocation_basis <> OLD.allocation_basis
       OR NEW.candidate_snapshot_hash <>
          OLD.candidate_snapshot_hash
       OR NEW.created_at_ms <> OLD.created_at_ms THEN
        RAISE EXCEPTION 'provider allocation pool identity is immutable'
            USING ERRCODE = '55000';
    END IF;

    IF NEW.control_version <> OLD.control_version + 1 THEN
        RAISE EXCEPTION
            'provider allocation pool control version must advance'
            USING ERRCODE = '40001';
    END IF;

    IF NEW.state = 'draft' AND NEW.closed_at_ms IS NOT NULL THEN
        RAISE EXCEPTION 'draft provider allocation pool cannot be closed'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION preserve_provider_cost_allocation_line()
RETURNS TRIGGER AS $$
DECLARE
    pool_state TEXT;
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        RAISE EXCEPTION 'provider allocation lines are immutable'
            USING ERRCODE = '55000';
    END IF;

    SELECT state INTO STRICT pool_state
    FROM provider_cost_allocation_pools
    WHERE provider_cost_allocation_pool_id =
          NEW.provider_cost_allocation_pool_id
    FOR UPDATE;

    IF pool_state <> 'draft' THEN
        RAISE EXCEPTION 'provider allocation lines require a draft pool'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_closed_provider_cost_allocation_evidence(
    target_pool_id UUID
)
RETURNS VOID AS $$
DECLARE
    pool_state TEXT;
    closure_count BIGINT;
    line_count BIGINT;
    claim_count BIGINT;
    invalid_claim_count BIGINT;
    invalid_snapshot_count BIGINT;
BEGIN
    IF target_pool_id IS NULL THEN
        RETURN;
    END IF;

    SELECT state INTO STRICT pool_state
    FROM provider_cost_allocation_pools
    WHERE provider_cost_allocation_pool_id = target_pool_id;

    IF pool_state <> 'closed' THEN
        RETURN;
    END IF;

    SELECT COUNT(*) INTO closure_count
    FROM provider_cost_allocation_closures closure
    JOIN provider_cost_allocation_pools pool
      ON pool.provider_cost_allocation_pool_id =
         closure.provider_cost_allocation_pool_id
    WHERE closure.provider_cost_allocation_pool_id = target_pool_id
      AND closure.candidate_snapshot_hash =
          pool.candidate_snapshot_hash
      AND closure.source_period_start_ms = pool.period_start_ms
      AND closure.source_period_end_ms = pool.period_end_ms
      AND closure.source_currency = pool.currency
      AND closure.source_amount_micros =
          pool.total_amount_micros;

    SELECT COUNT(*) INTO line_count
    FROM provider_cost_allocation_lines
    WHERE provider_cost_allocation_pool_id = target_pool_id;

    SELECT
        COUNT(*) FILTER (WHERE claim.claim_id IS NULL),
        COUNT(DISTINCT claim.claim_id)
      INTO invalid_claim_count, claim_count
    FROM provider_cost_allocation_lines line
    LEFT JOIN provider_cost_authority_claims claim
      ON claim.source_provider_cost_allocation_pool_id =
         line.provider_cost_allocation_pool_id
     AND claim.source_provider_cost_allocation_line_id =
         line.provider_cost_allocation_line_id
     AND claim.source_receipt_id = line.basis_receipt_id
     AND claim.provider_id = line.provider_id
     AND claim.provider_account_id = line.provider_account_id
     AND claim.job_id = line.job_id
     AND claim.authority_kind = 'provider_allocated'
    WHERE line.provider_cost_allocation_pool_id = target_pool_id;

    SELECT COUNT(*) INTO invalid_snapshot_count
    FROM provider_cost_allocation_lines line
    JOIN provider_receipts receipt
      ON receipt.receipt_id = line.basis_receipt_id
    JOIN customer_price_quotes quote
      ON quote.quote_id = line.basis_quote_id
    WHERE line.provider_cost_allocation_pool_id = target_pool_id
      AND (
          receipt.payload_hash <>
              line.basis_receipt_payload_hash
          OR quote.quote_hash <> line.basis_quote_hash
      );

    IF closure_count <> 1
       OR line_count = 0
       OR invalid_claim_count <> 0
       OR claim_count <> line_count
       OR invalid_snapshot_count <> 0 THEN
        RAISE EXCEPTION
            'closed provider allocation lacks immutable evidence and authority coverage'
            USING ERRCODE = '23514';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_provider_cost_allocation_close_evidence_guard()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM validate_closed_provider_cost_allocation_evidence(
        COALESCE(
            NEW.provider_cost_allocation_pool_id,
            OLD.provider_cost_allocation_pool_id
        )
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_provider_cost_allocation_claim_evidence_guard()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM validate_closed_provider_cost_allocation_evidence(
        NEW.source_provider_cost_allocation_pool_id
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_cost_allocation_pools_evidence_guard
AFTER INSERT OR UPDATE ON provider_cost_allocation_pools
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION validate_provider_cost_allocation_close_evidence_guard();

CREATE CONSTRAINT TRIGGER provider_cost_allocation_closures_evidence_guard
AFTER INSERT ON provider_cost_allocation_closures
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION validate_provider_cost_allocation_close_evidence_guard();

CREATE CONSTRAINT TRIGGER provider_cost_allocation_claims_evidence_guard
AFTER INSERT ON provider_cost_authority_claims
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
WHEN (NEW.authority_kind = 'provider_allocated')
EXECUTE FUNCTION validate_provider_cost_allocation_claim_evidence_guard();

CREATE FUNCTION validate_provider_cost_allocation_ledger_identity()
RETURNS TRIGGER AS $$
DECLARE
    line provider_cost_allocation_lines%ROWTYPE;
    pool provider_cost_allocation_pools%ROWTYPE;
    expected_semantic_key TEXT;
BEGIN
    IF NEW.transaction_type <> 'provider_cost'
       OR NEW.source_provider_cost_allocation_line_id IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT * INTO STRICT line
    FROM provider_cost_allocation_lines
    WHERE provider_cost_allocation_line_id =
          NEW.source_provider_cost_allocation_line_id
      AND provider_cost_allocation_pool_id =
          NEW.source_provider_cost_allocation_pool_id;

    SELECT * INTO STRICT pool
    FROM provider_cost_allocation_pools
    WHERE provider_cost_allocation_pool_id =
          NEW.source_provider_cost_allocation_pool_id;

    expected_semantic_key :=
        'provider-cost-allocation-line:v1:' ||
        line.provider_cost_allocation_line_id::TEXT;

    IF NEW.semantic_key <> expected_semantic_key
       OR NEW.payload_hash <>
          provider_cost_ledger_payload_hash(
              expected_semantic_key,
              pool.currency,
              line.amount_micros,
              line.provider_id
          ) THEN
        RAISE EXCEPTION
            'provider allocation ledger identity is invalid'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_transactions_validate_provider_allocation_identity
BEFORE INSERT ON ledger_transactions
FOR EACH ROW
EXECUTE FUNCTION validate_provider_cost_allocation_ledger_identity();

COMMENT ON TABLE provider_cost_allocation_closures IS
    'Immutable close command and provider invoice, contract, subscription, or statement evidence for one allocation pool.';

COMMENT ON COLUMN provider_cost_allocation_lines.basis_receipt_id IS
    'Stable provider receipt selected when the allocation candidate snapshot was created.';

COMMENT ON COLUMN provider_cost_allocation_lines.basis_quote_id IS
    'Immutable customer quote that preserves the route, model, tier, and execution surface used for eligibility.';
