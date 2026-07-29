CREATE FUNCTION reject_new_legacy_provider_receipt_cost()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.provider_cost_micros IS NOT NULL
       OR NEW.provider_cost_currency IS NOT NULL THEN
        RAISE EXCEPTION 'legacy provider receipt cost writes are disabled'
            USING ERRCODE = '0A000';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_receipts_reject_legacy_cost
BEFORE INSERT ON provider_receipts
FOR EACH ROW EXECUTE FUNCTION reject_new_legacy_provider_receipt_cost();

CREATE FUNCTION reject_new_legacy_provider_cost_ledger()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.transaction_type = 'provider_cost'
       AND NEW.source_provider_cost_observation_id IS NULL
       AND NEW.source_provider_cost_allocation_line_id IS NULL THEN
        RAISE EXCEPTION 'legacy provider cost ledger writes are disabled'
            USING ERRCODE = '0A000';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_transactions_reject_legacy_provider_cost
BEFORE INSERT ON ledger_transactions
FOR EACH ROW EXECUTE FUNCTION reject_new_legacy_provider_cost_ledger();
