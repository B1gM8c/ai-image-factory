ALTER TABLE customer_billing_holds
    ADD COLUMN grant_held_micros BIGINT NOT NULL DEFAULT 0
        CHECK (grant_held_micros >= 0),
    ADD COLUMN account_held_micros BIGINT NOT NULL DEFAULT 0
        CHECK (account_held_micros >= 0),
    ADD COLUMN grant_captured_micros BIGINT NOT NULL DEFAULT 0
        CHECK (grant_captured_micros >= 0),
    ADD COLUMN account_captured_micros BIGINT NOT NULL DEFAULT 0
        CHECK (account_captured_micros >= 0),
    ADD COLUMN grant_released_micros BIGINT NOT NULL DEFAULT 0
        CHECK (grant_released_micros >= 0),
    ADD COLUMN account_released_micros BIGINT NOT NULL DEFAULT 0
        CHECK (account_released_micros >= 0);

UPDATE customer_billing_holds
SET account_held_micros = held_micros,
    account_captured_micros = captured_micros,
    account_released_micros = released_micros;

ALTER TABLE customer_billing_holds
    ADD CONSTRAINT customer_billing_holds_funding_split_check CHECK (
        grant_held_micros::NUMERIC + account_held_micros::NUMERIC
            = held_micros::NUMERIC
        AND grant_captured_micros::NUMERIC
            + account_captured_micros::NUMERIC
            = captured_micros::NUMERIC
        AND grant_released_micros::NUMERIC
            + account_released_micros::NUMERIC
            = released_micros::NUMERIC
        AND grant_captured_micros::NUMERIC
            + grant_released_micros::NUMERIC
            <= grant_held_micros::NUMERIC
        AND account_captured_micros::NUMERIC
            + account_released_micros::NUMERIC
            <= account_held_micros::NUMERIC
    ),
    ADD CONSTRAINT customer_billing_holds_grant_identity_unique
        UNIQUE (hold_id, tenant_id, currency);

CREATE OR REPLACE FUNCTION preserve_customer_billing_hold()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'customer billing holds cannot be deleted'
            USING ERRCODE = '55000';
    END IF;

    IF ROW(
        OLD.hold_id, OLD.quote_id, OLD.job_id, OLD.tenant_id,
        OLD.currency, OLD.held_micros,
        OLD.grant_held_micros, OLD.account_held_micros,
        OLD.created_at_ms
    ) IS DISTINCT FROM ROW(
        NEW.hold_id, NEW.quote_id, NEW.job_id, NEW.tenant_id,
        NEW.currency, NEW.held_micros,
        NEW.grant_held_micros, NEW.account_held_micros,
        NEW.created_at_ms
    )
       OR NEW.captured_micros < OLD.captured_micros
       OR NEW.released_micros < OLD.released_micros
       OR NEW.grant_captured_micros < OLD.grant_captured_micros
       OR NEW.account_captured_micros < OLD.account_captured_micros
       OR NEW.grant_released_micros < OLD.grant_released_micros
       OR NEW.account_released_micros < OLD.account_released_micros
       OR NEW.updated_at_ms < OLD.updated_at_ms
       OR (OLD.state = 'held' AND NEW.state NOT IN ('held', 'settled', 'released'))
       OR (OLD.state <> 'held' AND NEW.state <> OLD.state) THEN
        RAISE EXCEPTION 'invalid customer billing hold transition'
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END;
$$;

ALTER TABLE customer_refunds
    ADD COLUMN grant_restored_micros BIGINT NOT NULL DEFAULT 0
        CHECK (grant_restored_micros >= 0),
    ADD COLUMN account_refunded_micros BIGINT NOT NULL DEFAULT 0
        CHECK (account_refunded_micros >= 0);

ALTER TABLE customer_refunds
    DISABLE TRIGGER customer_refunds_reject_mutation;

UPDATE customer_refunds
SET account_refunded_micros = amount_micros;

ALTER TABLE customer_refunds
    ENABLE TRIGGER customer_refunds_reject_mutation;

ALTER TABLE customer_refunds
    ADD CONSTRAINT customer_refunds_funding_split_check CHECK (
        grant_restored_micros::NUMERIC
            + account_refunded_micros::NUMERIC
            = amount_micros::NUMERIC
    ),
    ADD CONSTRAINT customer_refunds_grant_identity_unique
        UNIQUE (refund_id, tenant_id, currency);

CREATE OR REPLACE FUNCTION validate_billing_account_refund_total()
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

    SELECT COALESCE(SUM(account_refunded_micros::NUMERIC), 0)
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
        RAISE EXCEPTION
            'billing account refund counter does not match account-funded refunds'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

ALTER TABLE ledger_accounts
    DROP CONSTRAINT ledger_accounts_account_type_check,
    ADD CONSTRAINT ledger_accounts_account_type_check CHECK (
        account_type IN (
            'receivable', 'revenue', 'expense', 'payable',
            'credit_liability'
        )
    );

CREATE TABLE credit_grants (
    grant_id UUID PRIMARY KEY,
    semantic_key TEXT NOT NULL UNIQUE CHECK (
        char_length(semantic_key) BETWEEN 1 AND 512
        AND semantic_key !~ '[[:cntrl:]]'
    ),
    tenant_id TEXT NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    source_kind TEXT NOT NULL CHECK (source_kind = 'promotional'),
    source_reference TEXT NOT NULL CHECK (
        char_length(source_reference) BETWEEN 1 AND 512
        AND source_reference !~ '[[:cntrl:]]'
    ),
    received_at_ms BIGINT NOT NULL CHECK (received_at_ms >= 0),
    expires_at_ms BIGINT NOT NULL,
    original_amount_micros BIGINT NOT NULL
        CHECK (original_amount_micros > 0),
    reserved_micros BIGINT NOT NULL DEFAULT 0 CHECK (reserved_micros >= 0),
    consumed_micros BIGINT NOT NULL DEFAULT 0 CHECK (consumed_micros >= 0),
    restored_micros BIGINT NOT NULL DEFAULT 0 CHECK (restored_micros >= 0),
    expired_micros BIGINT NOT NULL DEFAULT 0 CHECK (expired_micros >= 0),
    revoked_micros BIGINT NOT NULL DEFAULT 0 CHECK (revoked_micros >= 0),
    available_micros BIGINT GENERATED ALWAYS AS (
        (
            original_amount_micros::NUMERIC
            - reserved_micros::NUMERIC
            - consumed_micros::NUMERIC
            + restored_micros::NUMERIC
            - expired_micros::NUMERIC
            - revoked_micros::NUMERIC
        )::BIGINT
    ) STORED,
    state TEXT NOT NULL CHECK (state IN ('active', 'expired', 'revoked')),
    control_version BIGINT NOT NULL CHECK (control_version > 0),
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms BIGINT NOT NULL CHECK (updated_at_ms >= created_at_ms),
    FOREIGN KEY (tenant_id, currency)
        REFERENCES billing_accounts(tenant_id, currency) ON DELETE RESTRICT,
    UNIQUE (grant_id, tenant_id, currency),
    UNIQUE (tenant_id, currency, source_kind, source_reference),
    CONSTRAINT credit_grants_window_check CHECK (
        expires_at_ms > received_at_ms
        AND received_at_ms <= created_at_ms
    ),
    CONSTRAINT credit_grants_restoration_check CHECK (
        restored_micros <= consumed_micros
    ),
    CONSTRAINT credit_grants_balance_check CHECK (
        original_amount_micros::NUMERIC
        - reserved_micros::NUMERIC
        - consumed_micros::NUMERIC
        + restored_micros::NUMERIC
        - expired_micros::NUMERIC
        - revoked_micros::NUMERIC
        >= 0
    ),
    CONSTRAINT credit_grants_terminal_balance_check CHECK (
        state = 'active'
        OR (
            original_amount_micros::NUMERIC
            - reserved_micros::NUMERIC
            - consumed_micros::NUMERIC
            + restored_micros::NUMERIC
            - expired_micros::NUMERIC
            - revoked_micros::NUMERIC
            = 0
        )
    )
);

CREATE INDEX credit_grants_fefo_idx
    ON credit_grants(
        tenant_id, currency, expires_at_ms, grant_id
    )
    WHERE state = 'active' AND available_micros > 0;

CREATE INDEX credit_grants_expiry_idx
    ON credit_grants(expires_at_ms, grant_id)
    WHERE state = 'active'
      AND available_micros > 0
      AND reserved_micros = 0;

CREATE TABLE customer_billing_hold_grant_reservations (
    grant_reservation_id UUID PRIMARY KEY,
    hold_id UUID NOT NULL,
    grant_id UUID NOT NULL,
    tenant_id TEXT NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    reserved_micros BIGINT NOT NULL CHECK (reserved_micros > 0),
    consumed_micros BIGINT NOT NULL DEFAULT 0 CHECK (consumed_micros >= 0),
    released_micros BIGINT NOT NULL DEFAULT 0 CHECK (released_micros >= 0),
    state TEXT NOT NULL CHECK (state IN ('reserved', 'consumed', 'released')),
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms BIGINT NOT NULL CHECK (updated_at_ms >= created_at_ms),
    FOREIGN KEY (hold_id, tenant_id, currency)
        REFERENCES customer_billing_holds(hold_id, tenant_id, currency)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (grant_id, tenant_id, currency)
        REFERENCES credit_grants(grant_id, tenant_id, currency)
        ON DELETE RESTRICT,
    UNIQUE (hold_id, grant_id),
    UNIQUE (
        grant_reservation_id, hold_id, grant_id, tenant_id, currency
    ),
    CONSTRAINT credit_grant_reservation_balance_check CHECK (
        consumed_micros::NUMERIC + released_micros::NUMERIC
            <= reserved_micros::NUMERIC
    ),
    CONSTRAINT credit_grant_reservation_state_check CHECK (
        (
            state = 'reserved'
            AND consumed_micros::NUMERIC + released_micros::NUMERIC
                < reserved_micros::NUMERIC
        )
        OR (
            state = 'consumed'
            AND consumed_micros > 0
            AND consumed_micros::NUMERIC + released_micros::NUMERIC
                = reserved_micros::NUMERIC
        )
        OR (
            state = 'released'
            AND consumed_micros = 0
            AND released_micros = reserved_micros
        )
    )
);

CREATE INDEX credit_grant_reservations_hold_idx
    ON customer_billing_hold_grant_reservations(
        hold_id, grant_reservation_id
    );

CREATE INDEX credit_grant_reservations_grant_idx
    ON customer_billing_hold_grant_reservations(
        grant_id, state, grant_reservation_id
    );

CREATE TABLE credit_grant_events (
    grant_event_id UUID PRIMARY KEY,
    grant_id UUID NOT NULL,
    tenant_id TEXT NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    event_sequence BIGINT NOT NULL CHECK (event_sequence > 0),
    event_type TEXT NOT NULL CHECK (
        event_type IN (
            'issued', 'reserved', 'consumed', 'released',
            'restored_available', 'restored_expired',
            'expired', 'revoked'
        )
    ),
    amount_micros BIGINT NOT NULL CHECK (amount_micros > 0),
    grant_reservation_id UUID,
    hold_id UUID,
    refund_id UUID,
    related_grant_event_id UUID,
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    occurred_at_ms BIGINT NOT NULL CHECK (occurred_at_ms >= 0),
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms >= occurred_at_ms),
    FOREIGN KEY (grant_id, tenant_id, currency)
        REFERENCES credit_grants(grant_id, tenant_id, currency)
        ON DELETE RESTRICT,
    FOREIGN KEY (
        grant_reservation_id, hold_id, grant_id, tenant_id, currency
    )
        REFERENCES customer_billing_hold_grant_reservations(
            grant_reservation_id, hold_id, grant_id, tenant_id, currency
        )
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (refund_id, tenant_id, currency)
        REFERENCES customer_refunds(refund_id, tenant_id, currency)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (related_grant_event_id)
        REFERENCES credit_grant_events(grant_event_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (grant_id, event_sequence),
    UNIQUE (grant_event_id, grant_id, tenant_id, currency),
    CONSTRAINT credit_grant_events_shape_check CHECK (
        (
            event_type IN ('issued', 'expired', 'revoked')
            AND grant_reservation_id IS NULL
            AND hold_id IS NULL
            AND refund_id IS NULL
            AND related_grant_event_id IS NULL
        )
        OR (
            event_type IN ('reserved', 'consumed', 'released')
            AND grant_reservation_id IS NOT NULL
            AND hold_id IS NOT NULL
            AND refund_id IS NULL
            AND related_grant_event_id IS NULL
        )
        OR (
            event_type IN ('restored_available', 'restored_expired')
            AND grant_reservation_id IS NULL
            AND hold_id IS NULL
            AND refund_id IS NOT NULL
            AND related_grant_event_id IS NOT NULL
        )
    )
);

CREATE INDEX credit_grant_events_grant_idx
    ON credit_grant_events(grant_id, event_sequence);

CREATE INDEX credit_grant_events_hold_idx
    ON credit_grant_events(hold_id, grant_event_id)
    WHERE hold_id IS NOT NULL;

CREATE INDEX credit_grant_events_refund_idx
    ON credit_grant_events(refund_id, grant_event_id)
    WHERE refund_id IS NOT NULL;

ALTER TABLE ledger_transactions
    ADD COLUMN source_credit_grant_event_id UUID,
    ADD CONSTRAINT ledger_transactions_credit_grant_event_fk
        FOREIGN KEY (source_credit_grant_event_id)
        REFERENCES credit_grant_events(grant_event_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX ledger_transactions_credit_grant_event_uidx
    ON ledger_transactions(source_credit_grant_event_id)
    WHERE source_credit_grant_event_id IS NOT NULL;

CREATE TABLE credit_grant_operations (
    operation_id UUID PRIMARY KEY,
    grant_id UUID NOT NULL,
    grant_event_id UUID NOT NULL,
    tenant_id TEXT NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    operation TEXT NOT NULL CHECK (operation IN ('issue', 'revoke')),
    idempotency_key_digest TEXT NOT NULL CHECK (
        idempotency_key_digest ~ '^[0-9a-f]{64}$'
    ),
    request_hash TEXT NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    actor_user_id UUID NOT NULL,
    actor_session_id UUID NOT NULL,
    reason TEXT NOT NULL CHECK (
        char_length(reason) BETWEEN 1 AND 500
        AND reason !~ '[[:cntrl:]]'
    ),
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms >= 0),
    FOREIGN KEY (grant_id, tenant_id, currency)
        REFERENCES credit_grants(grant_id, tenant_id, currency)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (grant_event_id, grant_id, tenant_id, currency)
        REFERENCES credit_grant_events(
            grant_event_id, grant_id, tenant_id, currency
        )
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (tenant_id, operation, idempotency_key_digest),
    UNIQUE (grant_event_id)
);

CREATE INDEX credit_grant_operations_grant_idx
    ON credit_grant_operations(grant_id, created_at_ms, operation_id);

ALTER TABLE ledger_transactions
    DROP CONSTRAINT ledger_transactions_transaction_type_check,
    DROP CONSTRAINT ledger_transactions_check,
    ADD CONSTRAINT ledger_transactions_transaction_type_check CHECK (
        transaction_type IN (
            'customer_charge',
            'customer_job_charge',
            'customer_refund',
            'provider_cost',
            'credit_grant_issued',
            'credit_grant_consumed',
            'credit_grant_restored',
            'credit_grant_expired',
            'credit_grant_revoked',
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
            AND source_credit_grant_event_id IS NULL
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
            AND source_credit_grant_event_id IS NULL
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
            AND source_credit_grant_event_id IS NULL
        )
        OR
        (
            transaction_type = 'provider_cost'
            AND reverses_transaction_id IS NULL
            AND source_credit_grant_event_id IS NULL
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
            transaction_type IN (
                'credit_grant_issued',
                'credit_grant_consumed',
                'credit_grant_expired',
                'credit_grant_revoked'
            )
            AND source_output_id IS NULL
            AND source_job_id IS NULL
            AND source_submission_id IS NULL
            AND source_receipt_id IS NULL
            AND source_provider_cost_observation_id IS NULL
            AND source_provider_cost_allocation_pool_id IS NULL
            AND reverses_transaction_id IS NULL
            AND source_credit_grant_event_id IS NOT NULL
        )
        OR
        (
            transaction_type = 'credit_grant_restored'
            AND source_output_id IS NULL
            AND source_job_id IS NULL
            AND source_submission_id IS NULL
            AND source_receipt_id IS NULL
            AND source_provider_cost_observation_id IS NULL
            AND source_provider_cost_allocation_pool_id IS NULL
            AND reverses_transaction_id IS NOT NULL
            AND source_credit_grant_event_id IS NOT NULL
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
            AND source_credit_grant_event_id IS NULL
        )
    );

CREATE FUNCTION preserve_credit_grant()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'credit grants cannot be deleted'
            USING ERRCODE = '55000';
    END IF;

    IF ROW(
        OLD.grant_id, OLD.semantic_key, OLD.tenant_id, OLD.currency,
        OLD.source_kind, OLD.source_reference, OLD.received_at_ms,
        OLD.expires_at_ms, OLD.original_amount_micros, OLD.created_at_ms
    ) IS DISTINCT FROM ROW(
        NEW.grant_id, NEW.semantic_key, NEW.tenant_id, NEW.currency,
        NEW.source_kind, NEW.source_reference, NEW.received_at_ms,
        NEW.expires_at_ms, NEW.original_amount_micros, NEW.created_at_ms
    )
       OR NEW.consumed_micros < OLD.consumed_micros
       OR NEW.restored_micros < OLD.restored_micros
       OR NEW.expired_micros < OLD.expired_micros
       OR NEW.revoked_micros < OLD.revoked_micros
       OR NEW.control_version <> OLD.control_version + 1
       OR NEW.updated_at_ms < OLD.updated_at_ms
       OR (
            OLD.state = 'active'
            AND NEW.state NOT IN ('active', 'expired', 'revoked')
       )
       OR (
            OLD.state <> 'active'
            AND NEW.state <> OLD.state
       ) THEN
        RAISE EXCEPTION 'invalid credit grant transition'
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER credit_grants_preserve
BEFORE UPDATE OR DELETE ON credit_grants
FOR EACH ROW EXECUTE FUNCTION preserve_credit_grant();

CREATE FUNCTION preserve_credit_grant_reservation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'credit grant reservations cannot be deleted'
            USING ERRCODE = '55000';
    END IF;

    IF ROW(
        OLD.grant_reservation_id, OLD.hold_id, OLD.grant_id,
        OLD.tenant_id, OLD.currency, OLD.reserved_micros,
        OLD.created_at_ms
    ) IS DISTINCT FROM ROW(
        NEW.grant_reservation_id, NEW.hold_id, NEW.grant_id,
        NEW.tenant_id, NEW.currency, NEW.reserved_micros,
        NEW.created_at_ms
    )
       OR NEW.consumed_micros < OLD.consumed_micros
       OR NEW.released_micros < OLD.released_micros
       OR NEW.updated_at_ms < OLD.updated_at_ms
       OR (
            OLD.state = 'reserved'
            AND NEW.state NOT IN ('reserved', 'consumed', 'released')
       )
       OR (
            OLD.state <> 'reserved'
            AND NEW.state <> OLD.state
       ) THEN
        RAISE EXCEPTION 'invalid credit grant reservation transition'
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER credit_grant_reservations_preserve
BEFORE UPDATE OR DELETE ON customer_billing_hold_grant_reservations
FOR EACH ROW EXECUTE FUNCTION preserve_credit_grant_reservation();

CREATE FUNCTION validate_credit_grant_state(target_grant_id UUID)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    grant_row credit_grants%ROWTYPE;
    event_count BIGINT;
    max_sequence BIGINT;
    issued_count BIGINT;
    issued_amount NUMERIC;
    expected_reserved NUMERIC;
    expected_consumed NUMERIC;
    expected_restored NUMERIC;
    expected_expired NUMERIC;
    expected_revoked NUMERIC;
    expired_count BIGINT;
    revoked_count BIGINT;
    invalid_order_count BIGINT;
BEGIN
    SELECT *
      INTO STRICT grant_row
    FROM credit_grants
    WHERE grant_id = target_grant_id;

    SELECT COUNT(*),
           COALESCE(MAX(event_sequence), 0),
           COUNT(*) FILTER (WHERE event_type = 'issued'),
           COALESCE(SUM(amount_micros::NUMERIC)
               FILTER (WHERE event_type = 'issued'), 0),
           COALESCE(SUM(amount_micros::NUMERIC)
               FILTER (WHERE event_type = 'reserved'), 0)
             - COALESCE(SUM(amount_micros::NUMERIC)
               FILTER (WHERE event_type IN ('consumed', 'released')), 0),
           COALESCE(SUM(amount_micros::NUMERIC)
               FILTER (WHERE event_type = 'consumed'), 0),
           COALESCE(SUM(amount_micros::NUMERIC)
               FILTER (WHERE event_type IN (
                   'restored_available', 'restored_expired'
               )), 0),
           COALESCE(SUM(amount_micros::NUMERIC)
               FILTER (WHERE event_type IN (
                   'expired', 'restored_expired'
               )), 0),
           COALESCE(SUM(amount_micros::NUMERIC)
               FILTER (WHERE event_type = 'revoked'), 0),
           COUNT(*) FILTER (WHERE event_type = 'expired'),
           COUNT(*) FILTER (WHERE event_type = 'revoked')
      INTO event_count, max_sequence, issued_count, issued_amount,
           expected_reserved, expected_consumed, expected_restored,
           expected_expired, expected_revoked, expired_count, revoked_count
    FROM credit_grant_events
    WHERE grant_id = target_grant_id;

    SELECT COUNT(*)
      INTO invalid_order_count
    FROM credit_grant_events event
    WHERE event.grant_id = target_grant_id
      AND event.event_type <> 'restored_expired'
      AND EXISTS (
          SELECT 1
          FROM credit_grant_events terminal
          WHERE terminal.grant_id = event.grant_id
            AND terminal.event_type IN ('expired', 'revoked')
            AND terminal.event_sequence < event.event_sequence
      );

    IF issued_count <> 1
       OR issued_amount <> grant_row.original_amount_micros::NUMERIC
       OR event_count <> max_sequence
       OR max_sequence <> grant_row.control_version
       OR expected_reserved <> grant_row.reserved_micros::NUMERIC
       OR expected_consumed <> grant_row.consumed_micros::NUMERIC
       OR expected_restored <> grant_row.restored_micros::NUMERIC
       OR expected_expired <> grant_row.expired_micros::NUMERIC
       OR expected_revoked <> grant_row.revoked_micros::NUMERIC
       OR expired_count > 1
       OR revoked_count > 1
       OR expired_count + revoked_count > 1
       OR invalid_order_count <> 0
       OR (
            grant_row.state = 'active'
            AND expired_count + revoked_count <> 0
       )
       OR (
            grant_row.state = 'expired'
            AND (expired_count <> 1 OR revoked_count <> 0)
       )
       OR (
            grant_row.state = 'revoked'
            AND (revoked_count <> 1 OR expired_count <> 0)
       ) THEN
        RAISE EXCEPTION
            'credit grant counters do not match immutable events'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE FUNCTION validate_credit_grant_reservation_state(
    target_reservation_id UUID
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    reservation_row customer_billing_hold_grant_reservations%ROWTYPE;
    reserved_count BIGINT;
    reserved_amount NUMERIC;
    consumed_amount NUMERIC;
    released_amount NUMERIC;
BEGIN
    SELECT *
      INTO STRICT reservation_row
    FROM customer_billing_hold_grant_reservations
    WHERE grant_reservation_id = target_reservation_id;

    SELECT COUNT(*) FILTER (WHERE event_type = 'reserved'),
           COALESCE(SUM(amount_micros::NUMERIC)
               FILTER (WHERE event_type = 'reserved'), 0),
           COALESCE(SUM(amount_micros::NUMERIC)
               FILTER (WHERE event_type = 'consumed'), 0),
           COALESCE(SUM(amount_micros::NUMERIC)
               FILTER (WHERE event_type = 'released'), 0)
      INTO reserved_count, reserved_amount, consumed_amount, released_amount
    FROM credit_grant_events
    WHERE grant_reservation_id = target_reservation_id;

    IF reserved_count <> 1
       OR reserved_amount <> reservation_row.reserved_micros::NUMERIC
       OR consumed_amount <> reservation_row.consumed_micros::NUMERIC
       OR released_amount <> reservation_row.released_micros::NUMERIC THEN
        RAISE EXCEPTION
            'credit grant reservation counters do not match immutable events'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE FUNCTION validate_customer_billing_hold_grant_split(
    target_hold_id UUID
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    hold_row customer_billing_holds%ROWTYPE;
    reserved_amount NUMERIC;
    consumed_amount NUMERIC;
    released_amount NUMERIC;
BEGIN
    SELECT *
      INTO STRICT hold_row
    FROM customer_billing_holds
    WHERE hold_id = target_hold_id;

    SELECT COALESCE(SUM(reserved_micros::NUMERIC), 0),
           COALESCE(SUM(consumed_micros::NUMERIC), 0),
           COALESCE(SUM(released_micros::NUMERIC), 0)
      INTO reserved_amount, consumed_amount, released_amount
    FROM customer_billing_hold_grant_reservations
    WHERE hold_id = target_hold_id;

    IF reserved_amount <> hold_row.grant_held_micros::NUMERIC
       OR consumed_amount <> hold_row.grant_captured_micros::NUMERIC
       OR released_amount <> hold_row.grant_released_micros::NUMERIC THEN
        RAISE EXCEPTION
            'customer billing hold grant split does not match reservations'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE FUNCTION validate_credit_grant_refund_split(target_refund_id UUID)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    refund_row customer_refunds%ROWTYPE;
    restored_amount NUMERIC;
BEGIN
    SELECT *
      INTO STRICT refund_row
    FROM customer_refunds
    WHERE refund_id = target_refund_id;

    SELECT COALESCE(SUM(amount_micros::NUMERIC), 0)
      INTO restored_amount
    FROM credit_grant_events
    WHERE refund_id = target_refund_id
      AND event_type IN ('restored_available', 'restored_expired');

    IF restored_amount <> refund_row.grant_restored_micros::NUMERIC THEN
        RAISE EXCEPTION
            'customer refund grant split does not match restoration events'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE FUNCTION validate_credit_grant_event_contract(target_event_id UUID)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    event_row credit_grant_events%ROWTYPE;
    grant_row credit_grants%ROWTYPE;
    related_event credit_grant_events%ROWTYPE;
    refund_row customer_refunds%ROWTYPE;
    related_restored NUMERIC;
    source_job_id UUID;
    related_job_id UUID;
BEGIN
    SELECT *
      INTO STRICT event_row
    FROM credit_grant_events
    WHERE grant_event_id = target_event_id;

    SELECT *
      INTO STRICT grant_row
    FROM credit_grants
    WHERE grant_id = event_row.grant_id;

    IF event_row.event_type = 'issued'
       AND (
            event_row.event_sequence <> 1
            OR event_row.amount_micros <> grant_row.original_amount_micros
            OR event_row.occurred_at_ms <> grant_row.received_at_ms
       ) THEN
        RAISE EXCEPTION 'credit grant issuance event is invalid'
            USING ERRCODE = '23514';
    END IF;

    IF event_row.event_type = 'expired'
       AND event_row.occurred_at_ms < grant_row.expires_at_ms THEN
        RAISE EXCEPTION 'credit grant cannot expire before its fixed expiry'
            USING ERRCODE = '23514';
    END IF;

    IF event_row.event_type = 'restored_available'
       AND (
            event_row.occurred_at_ms >= grant_row.expires_at_ms
            OR grant_row.state <> 'active'
       ) THEN
        RAISE EXCEPTION 'credit grant restoration is no longer available'
            USING ERRCODE = '23514';
    END IF;

    IF event_row.event_type IN ('restored_available', 'restored_expired') THEN
        SELECT *
          INTO STRICT related_event
        FROM credit_grant_events
        WHERE grant_event_id = event_row.related_grant_event_id;

        SELECT *
          INTO STRICT refund_row
        FROM customer_refunds
        WHERE refund_id = event_row.refund_id;

        IF related_event.grant_id <> event_row.grant_id
           OR related_event.event_type <> 'consumed' THEN
            RAISE EXCEPTION
                'credit grant restoration must reference one consumption event'
                USING ERRCODE = '23514';
        END IF;

        SELECT COALESCE(SUM(amount_micros::NUMERIC), 0)
          INTO related_restored
        FROM credit_grant_events
        WHERE related_grant_event_id = related_event.grant_event_id
          AND event_type IN ('restored_available', 'restored_expired');

        IF related_restored > related_event.amount_micros::NUMERIC THEN
            RAISE EXCEPTION
                'credit grant restorations exceed the original consumption'
                USING ERRCODE = '23514';
        END IF;

        SELECT transaction.source_job_id
          INTO STRICT source_job_id
        FROM ledger_transactions transaction
        WHERE transaction.transaction_id =
            refund_row.original_transaction_id;

        SELECT hold.job_id
          INTO STRICT related_job_id
        FROM customer_billing_hold_grant_reservations reservation
        JOIN customer_billing_holds hold
          ON hold.hold_id = reservation.hold_id
        WHERE reservation.grant_reservation_id =
            related_event.grant_reservation_id;

        IF source_job_id <> related_job_id THEN
            RAISE EXCEPTION
                'credit grant restoration does not belong to the refunded charge'
                USING ERRCODE = '23514';
        END IF;
    END IF;
END;
$$;

CREATE FUNCTION validate_credit_grant_ledger_evidence(target_event_id UUID)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    event_row credit_grant_events%ROWTYPE;
    transaction_row ledger_transactions%ROWTYPE;
    expected_type TEXT;
    transaction_count BIGINT;
    seal_count BIGINT;
    posting_count BIGINT;
    posting_sum NUMERIC;
    liability_amount NUMERIC;
    receivable_amount NUMERIC;
    expense_amount NUMERIC;
    related_transaction_id UUID;
BEGIN
    SELECT *
      INTO STRICT event_row
    FROM credit_grant_events
    WHERE grant_event_id = target_event_id;

    IF event_row.event_type IN ('reserved', 'released') THEN
        IF EXISTS (
            SELECT 1
            FROM ledger_transactions
            WHERE source_credit_grant_event_id = target_event_id
        ) THEN
            RAISE EXCEPTION
                'credit grant reservation events cannot create ledger entries'
                USING ERRCODE = '23514';
        END IF;
        RETURN;
    END IF;

    expected_type := CASE event_row.event_type
        WHEN 'issued' THEN 'credit_grant_issued'
        WHEN 'consumed' THEN 'credit_grant_consumed'
        WHEN 'restored_available' THEN 'credit_grant_restored'
        WHEN 'restored_expired' THEN 'credit_grant_restored'
        WHEN 'expired' THEN 'credit_grant_expired'
        WHEN 'revoked' THEN 'credit_grant_revoked'
        ELSE NULL
    END;

    SELECT COUNT(*)
      INTO transaction_count
    FROM ledger_transactions
    WHERE source_credit_grant_event_id = target_event_id;

    IF transaction_count <> 1 THEN
        RAISE EXCEPTION
            'credit grant event requires one ledger transaction'
            USING ERRCODE = '23514';
    END IF;

    SELECT *
      INTO STRICT transaction_row
    FROM ledger_transactions
    WHERE source_credit_grant_event_id = target_event_id;

    SELECT COUNT(*)
      INTO seal_count
    FROM ledger_transaction_seals
    WHERE transaction_id = transaction_row.transaction_id;

    SELECT COUNT(*)::BIGINT,
           COALESCE(SUM(posting.amount_micros::NUMERIC), 0),
           COALESCE(SUM(posting.amount_micros::NUMERIC) FILTER (
               WHERE account.owner_type = 'tenant'
                 AND account.owner_id = event_row.tenant_id
                 AND account.account_type = 'credit_liability'
           ), 0),
           COALESCE(SUM(posting.amount_micros::NUMERIC) FILTER (
               WHERE account.owner_type = 'tenant'
                 AND account.owner_id = event_row.tenant_id
                 AND account.account_type = 'receivable'
           ), 0),
           COALESCE(SUM(posting.amount_micros::NUMERIC) FILTER (
               WHERE account.owner_type = 'platform'
                 AND account.account_type = 'expense'
           ), 0)
      INTO posting_count, posting_sum, liability_amount,
           receivable_amount, expense_amount
    FROM ledger_postings posting
    JOIN ledger_accounts account
      ON account.account_id = posting.account_id
     AND account.currency = posting.currency
    WHERE posting.transaction_id = transaction_row.transaction_id;

    IF transaction_row.transaction_type <> expected_type
       OR transaction_row.currency <> event_row.currency
       OR transaction_row.payload_hash <> event_row.payload_hash
       OR seal_count <> 1
       OR posting_count <> 2
       OR posting_sum <> 0 THEN
        RAISE EXCEPTION 'credit grant ledger evidence is invalid'
            USING ERRCODE = '23514';
    END IF;

    IF event_row.event_type = 'issued'
       AND (
            liability_amount <> -event_row.amount_micros::NUMERIC
            OR expense_amount <> event_row.amount_micros::NUMERIC
            OR receivable_amount <> 0
       ) THEN
        RAISE EXCEPTION 'credit grant issuance postings are invalid'
            USING ERRCODE = '23514';
    ELSIF event_row.event_type = 'consumed'
       AND (
            liability_amount <> event_row.amount_micros::NUMERIC
            OR receivable_amount <> -event_row.amount_micros::NUMERIC
            OR expense_amount <> 0
       ) THEN
        RAISE EXCEPTION 'credit grant consumption postings are invalid'
            USING ERRCODE = '23514';
    ELSIF event_row.event_type IN (
        'restored_available', 'restored_expired'
    ) THEN
        SELECT transaction_id
          INTO STRICT related_transaction_id
        FROM ledger_transactions
        WHERE source_credit_grant_event_id =
            event_row.related_grant_event_id;

        IF transaction_row.reverses_transaction_id <> related_transaction_id
           OR liability_amount <> -event_row.amount_micros::NUMERIC
           OR receivable_amount <> event_row.amount_micros::NUMERIC
           OR expense_amount <> 0 THEN
            RAISE EXCEPTION 'credit grant restoration postings are invalid'
                USING ERRCODE = '23514';
        END IF;
    ELSIF event_row.event_type IN ('expired', 'revoked')
       AND (
            liability_amount <> event_row.amount_micros::NUMERIC
            OR expense_amount <> -event_row.amount_micros::NUMERIC
            OR receivable_amount <> 0
       ) THEN
        RAISE EXCEPTION 'credit grant terminal postings are invalid'
            USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE FUNCTION credit_grant_row_validate_events()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM validate_credit_grant_state(NEW.grant_id);
    RETURN NEW;
END;
$$;

CREATE FUNCTION credit_grant_event_validate_all()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM validate_credit_grant_event_contract(NEW.grant_event_id);
    PERFORM validate_credit_grant_state(NEW.grant_id);
    PERFORM validate_credit_grant_ledger_evidence(NEW.grant_event_id);

    IF NEW.grant_reservation_id IS NOT NULL THEN
        PERFORM validate_credit_grant_reservation_state(
            NEW.grant_reservation_id
        );
        PERFORM validate_customer_billing_hold_grant_split(NEW.hold_id);
    END IF;

    IF NEW.refund_id IS NOT NULL THEN
        PERFORM validate_credit_grant_refund_split(NEW.refund_id);
    END IF;

    RETURN NEW;
END;
$$;

CREATE FUNCTION credit_grant_reservation_validate_events()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM validate_credit_grant_reservation_state(
        NEW.grant_reservation_id
    );
    PERFORM validate_customer_billing_hold_grant_split(NEW.hold_id);
    RETURN NEW;
END;
$$;

CREATE FUNCTION customer_billing_hold_validate_grant_split()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM validate_customer_billing_hold_grant_split(NEW.hold_id);
    RETURN NEW;
END;
$$;

CREATE FUNCTION customer_refund_validate_grant_split()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM validate_credit_grant_refund_split(NEW.refund_id);
    RETURN NEW;
END;
$$;

CREATE FUNCTION ledger_transaction_validate_credit_grant()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.source_credit_grant_event_id IS NOT NULL THEN
        PERFORM validate_credit_grant_ledger_evidence(
            NEW.source_credit_grant_event_id
        );
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER credit_grants_validate_events
AFTER INSERT OR UPDATE ON credit_grants
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION credit_grant_row_validate_events();

CREATE CONSTRAINT TRIGGER credit_grant_events_validate_all
AFTER INSERT ON credit_grant_events
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION credit_grant_event_validate_all();

CREATE CONSTRAINT TRIGGER credit_grant_reservations_validate_events
AFTER INSERT OR UPDATE ON customer_billing_hold_grant_reservations
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION credit_grant_reservation_validate_events();

CREATE CONSTRAINT TRIGGER customer_billing_holds_validate_grant_split
AFTER INSERT OR UPDATE ON customer_billing_holds
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION customer_billing_hold_validate_grant_split();

CREATE CONSTRAINT TRIGGER customer_refunds_validate_grant_split
AFTER INSERT ON customer_refunds
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION customer_refund_validate_grant_split();

CREATE CONSTRAINT TRIGGER ledger_transactions_validate_credit_grant
AFTER INSERT ON ledger_transactions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION ledger_transaction_validate_credit_grant();

CREATE TRIGGER credit_grant_events_reject_mutation
BEFORE UPDATE OR DELETE ON credit_grant_events
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER credit_grant_operations_reject_mutation
BEFORE UPDATE OR DELETE ON credit_grant_operations
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER credit_grants_reject_truncate
BEFORE TRUNCATE ON credit_grants
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

CREATE TRIGGER credit_grant_reservations_reject_truncate
BEFORE TRUNCATE ON customer_billing_hold_grant_reservations
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

CREATE TRIGGER credit_grant_events_reject_truncate
BEFORE TRUNCATE ON credit_grant_events
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

CREATE TRIGGER credit_grant_operations_reject_truncate
BEFORE TRUNCATE ON credit_grant_operations
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();

COMMENT ON TABLE credit_grants IS
    'Organization and currency scoped promotional credit batches with fixed expiry and event-derived balances.';

COMMENT ON TABLE customer_billing_hold_grant_reservations IS
    'FEFO grant reservations that fund one customer billing hold across one or more immutable grant batches.';

COMMENT ON TABLE credit_grant_events IS
    'Append-only issuance, reservation, consumption, restoration, expiry, and revocation evidence for credit grants.';

COMMENT ON TABLE credit_grant_operations IS
    'Idempotent append-only evidence for administrator-issued and revoked credit grants.';
