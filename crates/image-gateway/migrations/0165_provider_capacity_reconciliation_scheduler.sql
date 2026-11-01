-- Scheduling disposition is separate from the immutable original execution and
-- from evidence of remote completion. Manual review never releases capacity.
ALTER TABLE provider_capacity_reconciliations
    ADD COLUMN review_status TEXT NOT NULL DEFAULT 'auto',
    ADD COLUMN manual_reason TEXT,
    ADD COLUMN query_failures INTEGER NOT NULL DEFAULT 0,
    ADD CONSTRAINT provider_capacity_review_disposition CHECK (
      query_failures BETWEEN 0 AND 3 AND (
        (review_status = 'auto' AND manual_reason IS NULL)
        OR (review_status = 'manual_required'
            AND manual_reason IN ('missing_original_id', 'original_evidence_frozen', 'query_unavailable'))
      )
    );

-- Permit a scheduling-only change, retaining every pre-existing identity,
-- receipt, lease and projection invariant. Do not amend supplement fences.
DO $migration$
DECLARE definition TEXT; marker TEXT := '    IF NEW.state = ''released'' THEN';
BEGIN
    definition := pg_get_functiondef('enforce_provider_capacity_reconciliation_update()'::regprocedure);
    IF position(marker IN definition) = 0 THEN
        RAISE EXCEPTION 'capacity reconciliation update guard changed';
    END IF;
    definition := replace(definition, marker, $patch$
    IF NEW.state = 'active' AND
       (to_jsonb(NEW) - ARRAY['review_status','manual_reason','query_failures']) =
       (to_jsonb(OLD) - ARRAY['review_status','manual_reason','query_failures']) THEN
        RETURN NEW;
    END IF;
    IF NEW.state = 'released' THEN$patch$);
    EXECUTE definition;
END;
$migration$;

CREATE FUNCTION enforce_capacity_review_disposition() RETURNS TRIGGER AS $$
DECLARE original_id TEXT; frozen BOOLEAN;
BEGIN
    SELECT intent.remote_operation_id,
           EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations auth
                  WHERE auth.submission_id = NEW.submission_id)
           OR EXISTS(SELECT 1 FROM dreamina_operator_image_supplement_authorizations auth
                     WHERE auth.submission_id = NEW.submission_id)
    INTO original_id, frozen
    FROM provider_remote_submit_intents intent WHERE intent.submission_id = NEW.submission_id;

    -- Existing reviewed operator commands retain their normal fenced lease.
    -- Their terminal transition is still validated by the full evidence guard.
    IF NEW.state = 'released' THEN
        NEW.review_status := 'auto';
        NEW.manual_reason := NULL;
        NEW.query_failures := 0;
    END IF;

    -- A real late receipt can re-arm a missing-ID record; a clock/TTL cannot.
    IF NEW.evidence_revision > OLD.evidence_revision
       AND OLD.manual_reason = 'missing_original_id'
       AND original_id IS NOT NULL AND NOT frozen THEN
        NEW.review_status := 'auto';
        NEW.manual_reason := NULL;
        NEW.query_failures := 0;
    END IF;
    IF OLD.review_status = 'manual_required' AND NEW.review_status = 'auto'
       AND NEW.state <> 'released'
       AND NOT (NEW.evidence_revision > OLD.evidence_revision
                AND OLD.manual_reason = 'missing_original_id'
                AND original_id IS NOT NULL AND NOT frozen) THEN
        RAISE EXCEPTION 'capacity manual review requires new original receipt evidence';
    END IF;
    IF NEW.review_status = 'manual_required' THEN
        IF NEW.state <> 'active'
           OR (NEW.manual_reason = 'missing_original_id' AND original_id IS NOT NULL)
           OR (NEW.manual_reason = 'original_evidence_frozen' AND NOT frozen)
           OR (NEW.manual_reason = 'query_unavailable' AND NEW.query_failures <> 3) THEN
            RAISE EXCEPTION 'capacity manual review disposition lacks its cause';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Runs before the existing lifecycle guard, including when a receipt wakes it.
CREATE TRIGGER a_capacity_review_disposition
    BEFORE UPDATE ON provider_capacity_reconciliations
    FOR EACH ROW EXECUTE FUNCTION enforce_capacity_review_disposition();

CREATE INDEX provider_capacity_auto_review_idx
    ON provider_capacity_reconciliations(provider_id, provider_account_id, available_at_ms, submission_id)
    WHERE state = 'active' AND review_status = 'auto';

-- Existing unclaimed unknown originals must not stay in an always-due queue.
-- No execution/allocation/financial record or its original snapshot is touched.
UPDATE provider_capacity_reconciliations reconciliation
SET review_status = 'manual_required',
    manual_reason = CASE WHEN
      EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations auth WHERE auth.submission_id = reconciliation.submission_id)
      OR EXISTS(SELECT 1 FROM dreamina_operator_image_supplement_authorizations auth WHERE auth.submission_id = reconciliation.submission_id)
      THEN 'original_evidence_frozen' ELSE 'missing_original_id' END
FROM provider_remote_submit_intents intent
WHERE intent.submission_id = reconciliation.submission_id
  AND reconciliation.state = 'active' AND reconciliation.reconciliation_owner IS NULL
  AND (intent.remote_operation_id IS NULL
       OR EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations auth WHERE auth.submission_id = reconciliation.submission_id)
       OR EXISTS(SELECT 1 FROM dreamina_operator_image_supplement_authorizations auth WHERE auth.submission_id = reconciliation.submission_id));
