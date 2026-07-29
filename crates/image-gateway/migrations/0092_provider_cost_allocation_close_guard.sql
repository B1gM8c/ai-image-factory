DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM provider_cost_allocation_pools pool
        WHERE pool.state = 'closed'
          AND (
              pool.residual_amount_micros <> 0
              OR NOT EXISTS (
                  SELECT 1
                  FROM provider_cost_allocation_lines line
                  WHERE line.provider_cost_allocation_pool_id =
                        pool.provider_cost_allocation_pool_id
              )
              OR EXISTS (
                  SELECT 1
                  FROM provider_cost_allocation_lines line
                  LEFT JOIN ledger_transactions ledger_tx
                    ON ledger_tx.source_provider_cost_allocation_pool_id =
                       line.provider_cost_allocation_pool_id
                   AND ledger_tx.source_provider_cost_allocation_line_id =
                       line.provider_cost_allocation_line_id
                   AND ledger_tx.transaction_type = 'provider_cost'
                  LEFT JOIN ledger_transaction_seals seal
                    ON seal.transaction_id = ledger_tx.transaction_id
                  WHERE line.provider_cost_allocation_pool_id =
                        pool.provider_cost_allocation_pool_id
                  GROUP BY line.provider_cost_allocation_line_id,
                           line.amount_micros
                  HAVING (
                      line.amount_micros > 0
                      AND (
                          COUNT(ledger_tx.transaction_id) <> 1
                          OR COUNT(seal.transaction_id) <> 1
                      )
                  )
                  OR (
                      line.amount_micros = 0
                      AND COUNT(ledger_tx.transaction_id) <> 0
                  )
              )
          )
    ) THEN
        RAISE EXCEPTION
            'closed provider allocation pools require complete sealed ledger coverage'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE FUNCTION validate_closed_provider_cost_allocation_ledger(
    target_pool_id UUID
)
RETURNS VOID AS $$
DECLARE
    pool_state TEXT;
    residual BIGINT;
    line_count BIGINT;
    invalid_line_count BIGINT;
BEGIN
    IF target_pool_id IS NULL THEN
        RETURN;
    END IF;

    SELECT state, residual_amount_micros
      INTO STRICT pool_state, residual
    FROM provider_cost_allocation_pools
    WHERE provider_cost_allocation_pool_id = target_pool_id;

    IF pool_state <> 'closed' THEN
        RETURN;
    END IF;

    SELECT COUNT(*)
      INTO line_count
    FROM provider_cost_allocation_lines line
    WHERE line.provider_cost_allocation_pool_id = target_pool_id;

    SELECT COUNT(*)
      INTO invalid_line_count
    FROM (
        SELECT line.provider_cost_allocation_line_id,
               line.amount_micros,
               COUNT(ledger_tx.transaction_id) AS transaction_count,
               COUNT(seal.transaction_id) AS seal_count
        FROM provider_cost_allocation_lines line
        LEFT JOIN ledger_transactions ledger_tx
          ON ledger_tx.source_provider_cost_allocation_pool_id =
             line.provider_cost_allocation_pool_id
         AND ledger_tx.source_provider_cost_allocation_line_id =
             line.provider_cost_allocation_line_id
         AND ledger_tx.transaction_type = 'provider_cost'
        LEFT JOIN ledger_transaction_seals seal
          ON seal.transaction_id = ledger_tx.transaction_id
        WHERE line.provider_cost_allocation_pool_id = target_pool_id
        GROUP BY line.provider_cost_allocation_line_id,
                 line.amount_micros
    ) coverage
    WHERE (
        coverage.amount_micros > 0
        AND (
            coverage.transaction_count <> 1
            OR coverage.seal_count <> 1
        )
    )
    OR (
        coverage.amount_micros = 0
        AND coverage.transaction_count <> 0
    );

    IF residual <> 0 OR line_count = 0 OR invalid_line_count <> 0 THEN
        RAISE EXCEPTION
            'closed provider allocation pool lacks complete sealed ledger coverage'
            USING ERRCODE = '23514';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_provider_cost_allocation_pool_close_guard()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM validate_closed_provider_cost_allocation_ledger(
        COALESCE(
            NEW.provider_cost_allocation_pool_id,
            OLD.provider_cost_allocation_pool_id
        )
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_provider_cost_allocation_line_close_guard()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM validate_closed_provider_cost_allocation_ledger(
        COALESCE(
            NEW.provider_cost_allocation_pool_id,
            OLD.provider_cost_allocation_pool_id
        )
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_provider_cost_allocation_transaction_close_guard()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM validate_closed_provider_cost_allocation_ledger(
        NEW.source_provider_cost_allocation_pool_id
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION validate_provider_cost_allocation_posting_close_guard()
RETURNS TRIGGER AS $$
DECLARE
    target_pool_id UUID;
BEGIN
    SELECT source_provider_cost_allocation_pool_id
      INTO target_pool_id
    FROM ledger_transactions
    WHERE transaction_id = NEW.transaction_id;
    PERFORM validate_closed_provider_cost_allocation_ledger(target_pool_id);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_cost_allocation_pools_close_guard
AFTER INSERT OR UPDATE ON provider_cost_allocation_pools
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION validate_provider_cost_allocation_pool_close_guard();

CREATE CONSTRAINT TRIGGER provider_cost_allocation_lines_close_guard
AFTER INSERT OR UPDATE OR DELETE ON provider_cost_allocation_lines
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION validate_provider_cost_allocation_line_close_guard();

CREATE CONSTRAINT TRIGGER provider_cost_allocation_transactions_close_guard
AFTER INSERT ON ledger_transactions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION validate_provider_cost_allocation_transaction_close_guard();

CREATE CONSTRAINT TRIGGER provider_cost_allocation_postings_close_guard
AFTER INSERT ON ledger_postings
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION validate_provider_cost_allocation_posting_close_guard();

CREATE CONSTRAINT TRIGGER provider_cost_allocation_seals_close_guard
AFTER INSERT ON ledger_transaction_seals
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION validate_provider_cost_allocation_posting_close_guard();

CREATE FUNCTION reject_ungoverned_ledger_adjustment()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.transaction_type = 'adjustment' THEN
        RAISE EXCEPTION
            'generic ledger adjustments are disabled; use an evidenced business operation'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_transactions_reject_ungoverned_adjustment
BEFORE INSERT ON ledger_transactions
FOR EACH ROW EXECUTE FUNCTION reject_ungoverned_ledger_adjustment();

COMMENT ON FUNCTION validate_closed_provider_cost_allocation_ledger(UUID) IS
    'A closed allocation pool must have zero residual, at least one line, and one sealed provider-cost transaction for every positive line.';

COMMENT ON FUNCTION reject_ungoverned_ledger_adjustment() IS
    'Generic adjustments are not a product API. Credits, refunds, reversals, and corrections require dedicated immutable evidence.';
