ALTER TABLE billing_accounts
    ADD COLUMN control_version BIGINT NOT NULL DEFAULT 1
        CHECK (control_version > 0);

CREATE TABLE billing_account_limit_changes (
    change_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    previous_credit_limit_micros BIGINT NOT NULL
        CHECK (previous_credit_limit_micros >= 0),
    new_credit_limit_micros BIGINT NOT NULL
        CHECK (new_credit_limit_micros >= 0),
    control_version BIGINT NOT NULL CHECK (control_version > 0),
    actor_user_id UUID NOT NULL,
    session_id UUID NOT NULL,
    reason TEXT NOT NULL CHECK (char_length(reason) BETWEEN 3 AND 500),
    created_at_ms BIGINT NOT NULL,
    FOREIGN KEY (tenant_id, currency)
        REFERENCES billing_accounts(tenant_id, currency) ON DELETE RESTRICT,
    UNIQUE (tenant_id, currency, control_version)
);

CREATE INDEX billing_account_limit_changes_account_created_idx
    ON billing_account_limit_changes(
        tenant_id,
        currency,
        created_at_ms DESC,
        change_id DESC
    );

CREATE FUNCTION validate_billing_account_limit_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.credit_limit_micros = OLD.credit_limit_micros THEN
        IF NEW.control_version <> OLD.control_version THEN
            RAISE EXCEPTION
                'billing account control version changed without a credit limit change'
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.control_version <> OLD.control_version + 1 THEN
        RAISE EXCEPTION
            'billing account credit limit changes must increment control_version once'
            USING ERRCODE = '55000';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM billing_account_limit_changes change
        WHERE change.tenant_id = NEW.tenant_id
          AND change.currency = NEW.currency
          AND change.control_version = NEW.control_version
          AND change.previous_credit_limit_micros = OLD.credit_limit_micros
          AND change.new_credit_limit_micros = NEW.credit_limit_micros
    ) THEN
        RAISE EXCEPTION
            'billing account credit limit change is missing immutable control evidence'
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER validate_billing_account_limit_update
BEFORE UPDATE ON billing_accounts
FOR EACH ROW EXECUTE FUNCTION validate_billing_account_limit_change();

CREATE FUNCTION preserve_billing_account_limit_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION
        'billing account limit change history is immutable'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER preserve_billing_account_limit_change
BEFORE UPDATE OR DELETE ON billing_account_limit_changes
FOR EACH ROW EXECUTE FUNCTION preserve_billing_account_limit_change();

COMMENT ON TABLE billing_account_limit_changes IS
    'Immutable platform-owner evidence for organization billing credit-limit changes.';
