CREATE TABLE provider_cost_obligations (
    receipt_id UUID PRIMARY KEY,
    submission_id UUID NOT NULL UNIQUE,
    output_id UUID NOT NULL,
    job_id UUID NOT NULL,
    provider_id TEXT NOT NULL CHECK (
        char_length(provider_id) BETWEEN 1 AND 128
        AND provider_id ~ '^[A-Za-z0-9_.-]+$'
    ),
    provider_account_id UUID,
    currency TEXT CHECK (
        currency IS NULL OR currency ~ '^[A-Z]{3}$'
    ),
    state TEXT NOT NULL CHECK (
        state IN ('expected', 'pending', 'settled', 'waived')
    ),
    expected_authority_kind TEXT CHECK (
        expected_authority_kind IS NULL
        OR expected_authority_kind IN (
            'provider_actual', 'provider_allocated'
        )
    ),
    settlement_claim_id BIGINT
        REFERENCES provider_cost_authority_claims(claim_id)
        ON DELETE RESTRICT,
    pending_reason_code TEXT CHECK (
        pending_reason_code IS NULL
        OR pending_reason_code IN (
            'policy_unresolved',
            'provider_outcome_uncertain',
            'legacy_unbound_account',
            'authority_pending'
        )
    ),
    waiver_reason_code TEXT CHECK (
        waiver_reason_code IS NULL
        OR waiver_reason_code IN (
            'confirmed_no_effect',
            'contractual_no_direct_cost',
            'provider_invoice_no_charge',
            'legal_adjustment'
        )
    ),
    waiver_source_kind TEXT CHECK (
        waiver_source_kind IS NULL
        OR waiver_source_kind IN (
            'provider_receipt',
            'provider_contract',
            'provider_invoice',
            'legal_adjustment'
        )
    ),
    waiver_source_id TEXT CHECK (
        waiver_source_id IS NULL
        OR (
            char_length(waiver_source_id) BETWEEN 1 AND 512
            AND waiver_source_id !~ '[[:cntrl:]]'
        )
    ),
    waiver_evidence_hash TEXT CHECK (
        waiver_evidence_hash IS NULL
        OR waiver_evidence_hash ~ '^[0-9a-f]{64}$'
    ),
    waived_by_user_id UUID
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    waived_by_session_id UUID,
    due_at_ms BIGINT NOT NULL,
    escalate_at_ms BIGINT NOT NULL CHECK (
        escalate_at_ms > due_at_ms
    ),
    pending_since_ms BIGINT,
    last_reviewed_at_ms BIGINT,
    next_review_at_ms BIGINT,
    review_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (
        review_attempt_count >= 0
    ),
    control_version BIGINT NOT NULL DEFAULT 1 CHECK (
        control_version > 0
    ),
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    settled_at_ms BIGINT,
    waived_at_ms BIGINT,
    FOREIGN KEY (receipt_id, submission_id, output_id, job_id)
        REFERENCES provider_receipts(
            receipt_id, submission_id, output_id, job_id
        ) ON DELETE RESTRICT,
    FOREIGN KEY (receipt_id, provider_id)
        REFERENCES provider_receipts(receipt_id, provider_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (provider_account_id, provider_id)
        REFERENCES provider_accounts(provider_account_id, provider_id)
        ON DELETE RESTRICT,
    CHECK (updated_at_ms >= created_at_ms),
    CHECK (
        next_review_at_ms IS NULL
        OR last_reviewed_at_ms IS NOT NULL
    ),
    CHECK (
        last_reviewed_at_ms IS NULL
        OR last_reviewed_at_ms >= created_at_ms
    ),
    CHECK (
        next_review_at_ms IS NULL
        OR next_review_at_ms >= last_reviewed_at_ms
    ),
    CHECK (
        (
            state = 'expected'
            AND expected_authority_kind IS NOT NULL
            AND currency IS NOT NULL
            AND settlement_claim_id IS NULL
            AND pending_reason_code IS NULL
            AND pending_since_ms IS NULL
            AND waiver_reason_code IS NULL
            AND waiver_source_kind IS NULL
            AND waiver_source_id IS NULL
            AND waiver_evidence_hash IS NULL
            AND waived_by_user_id IS NULL
            AND waived_by_session_id IS NULL
            AND settled_at_ms IS NULL
            AND waived_at_ms IS NULL
        )
        OR
        (
            state = 'pending'
            AND settlement_claim_id IS NULL
            AND pending_reason_code IS NOT NULL
            AND pending_since_ms IS NOT NULL
            AND (
                (expected_authority_kind IS NULL AND currency IS NULL)
                OR
                (expected_authority_kind IS NOT NULL AND currency IS NOT NULL)
            )
            AND waiver_reason_code IS NULL
            AND waiver_source_kind IS NULL
            AND waiver_source_id IS NULL
            AND waiver_evidence_hash IS NULL
            AND waived_by_user_id IS NULL
            AND waived_by_session_id IS NULL
            AND settled_at_ms IS NULL
            AND waived_at_ms IS NULL
        )
        OR
        (
            state = 'settled'
            AND expected_authority_kind IS NOT NULL
            AND currency IS NOT NULL
            AND settlement_claim_id IS NOT NULL
            AND pending_reason_code IS NULL
            AND waiver_reason_code IS NULL
            AND waiver_source_kind IS NULL
            AND waiver_source_id IS NULL
            AND waiver_evidence_hash IS NULL
            AND waived_by_user_id IS NULL
            AND waived_by_session_id IS NULL
            AND settled_at_ms IS NOT NULL
            AND waived_at_ms IS NULL
        )
        OR
        (
            state = 'waived'
            AND settlement_claim_id IS NULL
            AND pending_reason_code IS NULL
            AND waiver_reason_code IS NOT NULL
            AND waiver_source_kind IS NOT NULL
            AND waiver_source_id IS NOT NULL
            AND waiver_evidence_hash IS NOT NULL
            AND waived_by_user_id IS NOT NULL
            AND waived_by_session_id IS NOT NULL
            AND settled_at_ms IS NULL
            AND waived_at_ms IS NOT NULL
        )
    )
);

CREATE UNIQUE INDEX provider_cost_obligations_actual_claim_uidx
    ON provider_cost_obligations(settlement_claim_id)
    WHERE expected_authority_kind = 'provider_actual'
      AND settlement_claim_id IS NOT NULL;

CREATE INDEX provider_cost_obligations_queue_idx
    ON provider_cost_obligations(
        state, due_at_ms, escalate_at_ms, receipt_id
    )
    WHERE state IN ('expected', 'pending');

CREATE INDEX provider_cost_obligations_account_idx
    ON provider_cost_obligations(
        provider_id, provider_account_id, state, created_at_ms DESC
    );

CREATE TABLE provider_cost_obligation_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    receipt_id UUID NOT NULL
        REFERENCES provider_cost_obligations(receipt_id)
        ON DELETE RESTRICT,
    control_version BIGINT NOT NULL CHECK (control_version > 0),
    previous_state TEXT CHECK (
        previous_state IS NULL
        OR previous_state IN (
            'expected', 'pending', 'settled', 'waived'
        )
    ),
    state TEXT NOT NULL CHECK (
        state IN ('expected', 'pending', 'settled', 'waived')
    ),
    event_kind TEXT NOT NULL CHECK (
        event_kind IN (
            'created', 'classified', 'reviewed',
            'settled', 'waived'
        )
    ),
    details JSONB NOT NULL CHECK (jsonb_typeof(details) = 'object'),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (receipt_id, control_version)
);

CREATE INDEX provider_cost_obligation_events_receipt_idx
    ON provider_cost_obligation_events(receipt_id, event_id);

CREATE FUNCTION validate_provider_cost_obligation()
RETURNS TRIGGER AS $$
DECLARE
    claim provider_cost_authority_claims%ROWTYPE;
    receipt provider_receipts%ROWTYPE;
    valid_settlement BOOLEAN;
BEGIN
    SELECT * INTO STRICT receipt
    FROM provider_receipts
    WHERE receipt_id = NEW.receipt_id;

    IF NEW.state = 'settled' THEN
        SELECT * INTO STRICT claim
        FROM provider_cost_authority_claims
        WHERE claim_id = NEW.settlement_claim_id;

        IF claim.authority_kind <> NEW.expected_authority_kind
           OR claim.provider_id <> NEW.provider_id
           OR claim.provider_account_id IS DISTINCT FROM
              NEW.provider_account_id
           OR claim.job_id <> NEW.job_id
           OR claim.currency <> NEW.currency
           OR NOT (claim.authority_period @> receipt.created_at_ms) THEN
            RAISE EXCEPTION
                'provider cost obligation settlement identity is invalid'
                USING ERRCODE = '23514';
        END IF;

        IF claim.authority_kind = 'provider_actual' THEN
            SELECT EXISTS (
                SELECT 1
                FROM provider_usage_facts fact
                JOIN provider_cost_observation_receipts link
                  ON link.provider_cost_observation_id =
                     claim.source_provider_cost_observation_id
                 AND link.receipt_id = fact.receipt_id
                WHERE fact.usage_fact_id = claim.source_usage_fact_id
                  AND fact.receipt_id = NEW.receipt_id
                  AND fact.submission_id = NEW.submission_id
                  AND fact.output_id = NEW.output_id
                  AND fact.job_id = NEW.job_id
                  AND fact.provider_id = NEW.provider_id
                  AND fact.provider_account_id IS NOT DISTINCT FROM
                      NEW.provider_account_id
            ) INTO valid_settlement;
        ELSE
            SELECT EXISTS (
                SELECT 1
                FROM provider_cost_allocation_lines line
                WHERE line.provider_cost_allocation_line_id =
                      claim.source_provider_cost_allocation_line_id
                  AND line.provider_cost_allocation_pool_id =
                      claim.source_provider_cost_allocation_pool_id
                  AND line.provider_id = NEW.provider_id
                  AND line.provider_account_id =
                      NEW.provider_account_id
                  AND line.job_id = NEW.job_id
                  AND (
                      line.output_id IS NULL
                      OR line.output_id = NEW.output_id
                  )
            ) INTO valid_settlement;
        END IF;

        IF valid_settlement IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION
                'provider cost claim does not cover its obligation'
                USING ERRCODE = '23514';
        END IF;
    END IF;

    IF NEW.state = 'waived'
       AND NEW.waiver_reason_code = 'confirmed_no_effect'
       AND (
           receipt.outcome <> 'no_effect'
           OR NEW.waiver_source_kind <> 'provider_receipt'
           OR NEW.waiver_source_id <> NEW.receipt_id::TEXT
       ) THEN
        RAISE EXCEPTION
            'confirmed no-effect waiver lacks matching provider evidence'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_obligations_validate
BEFORE INSERT OR UPDATE ON provider_cost_obligations
FOR EACH ROW EXECUTE FUNCTION validate_provider_cost_obligation();

CREATE FUNCTION enforce_provider_cost_obligation_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'provider cost obligations cannot be deleted'
            USING ERRCODE = '55000';
    END IF;
    IF NEW.receipt_id IS DISTINCT FROM OLD.receipt_id
       OR NEW.submission_id IS DISTINCT FROM OLD.submission_id
       OR NEW.output_id IS DISTINCT FROM OLD.output_id
       OR NEW.job_id IS DISTINCT FROM OLD.job_id
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.provider_account_id IS DISTINCT FROM OLD.provider_account_id
       OR NEW.due_at_ms IS DISTINCT FROM OLD.due_at_ms
       OR NEW.escalate_at_ms IS DISTINCT FROM OLD.escalate_at_ms
       OR NEW.created_at_ms IS DISTINCT FROM OLD.created_at_ms THEN
        RAISE EXCEPTION 'provider cost obligation identity is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.state IN ('settled', 'waived') THEN
        RAISE EXCEPTION 'provider cost obligation is already terminal'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.state = 'expected'
       AND NEW.state NOT IN ('expected', 'pending', 'settled', 'waived') THEN
        RAISE EXCEPTION 'invalid provider cost obligation transition'
            USING ERRCODE = '23514';
    END IF;
    IF OLD.state = 'pending'
       AND NEW.state NOT IN ('pending', 'settled', 'waived') THEN
        RAISE EXCEPTION 'invalid provider cost obligation transition'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.control_version <> OLD.control_version + 1
       OR NEW.updated_at_ms < OLD.updated_at_ms THEN
        RAISE EXCEPTION
            'provider cost obligation control version is stale'
            USING ERRCODE = '40001';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_obligations_preserve
BEFORE UPDATE OR DELETE ON provider_cost_obligations
FOR EACH ROW EXECUTE FUNCTION enforce_provider_cost_obligation_transition();

CREATE FUNCTION record_provider_cost_obligation_event()
RETURNS TRIGGER AS $$
DECLARE
    event_kind_value TEXT;
BEGIN
    event_kind_value := CASE
        WHEN TG_OP = 'INSERT' THEN 'created'
        WHEN NEW.state = 'settled' THEN 'settled'
        WHEN NEW.state = 'waived' THEN 'waived'
        WHEN NEW.expected_authority_kind IS DISTINCT FROM
             OLD.expected_authority_kind THEN 'classified'
        ELSE 'reviewed'
    END;
    INSERT INTO provider_cost_obligation_events (
        receipt_id, control_version, previous_state, state,
        event_kind, details, created_at_ms
    )
    VALUES (
        NEW.receipt_id,
        NEW.control_version,
        CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE OLD.state END,
        NEW.state,
        event_kind_value,
        jsonb_build_object(
            'expected_authority_kind', NEW.expected_authority_kind,
            'settlement_claim_id', NEW.settlement_claim_id,
            'pending_reason_code', NEW.pending_reason_code,
            'review_attempt_count', NEW.review_attempt_count
        ),
        NEW.updated_at_ms
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_obligations_record_event
AFTER INSERT OR UPDATE ON provider_cost_obligations
FOR EACH ROW EXECUTE FUNCTION record_provider_cost_obligation_event();

CREATE FUNCTION preserve_provider_cost_obligation_event()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'provider cost obligation events are immutable'
        USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_obligation_events_immutable
BEFORE UPDATE OR DELETE ON provider_cost_obligation_events
FOR EACH ROW EXECUTE FUNCTION preserve_provider_cost_obligation_event();

CREATE TRIGGER provider_cost_obligation_events_reject_truncate
BEFORE TRUNCATE ON provider_cost_obligation_events
FOR EACH STATEMENT EXECUTE FUNCTION preserve_provider_cost_obligation_event();

CREATE FUNCTION create_provider_cost_obligation()
RETURNS TRIGGER AS $$
DECLARE
    account_id UUID;
    pending_reason TEXT;
BEGIN
    SELECT provider_account_id INTO account_id
    FROM provider_submissions
    WHERE submission_id = NEW.submission_id;

    pending_reason := CASE
        WHEN account_id IS NULL THEN 'legacy_unbound_account'
        WHEN NEW.outcome = 'uncertain' THEN 'provider_outcome_uncertain'
        ELSE 'policy_unresolved'
    END;

    INSERT INTO provider_cost_obligations (
        receipt_id, submission_id, output_id, job_id,
        provider_id, provider_account_id, currency, state,
        expected_authority_kind, settlement_claim_id,
        pending_reason_code, due_at_ms, escalate_at_ms,
        pending_since_ms, control_version, created_at_ms, updated_at_ms
    )
    VALUES (
        NEW.receipt_id, NEW.submission_id, NEW.output_id, NEW.job_id,
        NEW.provider_id, account_id, NULL, 'pending',
        NULL, NULL, pending_reason,
        NEW.created_at_ms + 86400000,
        NEW.created_at_ms + 172800000,
        NEW.created_at_ms, 1, NEW.created_at_ms, NEW.created_at_ms
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_receipts_create_cost_obligation
AFTER INSERT ON provider_receipts
FOR EACH ROW EXECUTE FUNCTION create_provider_cost_obligation();

CREATE FUNCTION settle_provider_cost_obligations_from_claim()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.authority_kind = 'provider_actual' THEN
        UPDATE provider_cost_obligations obligation
        SET state = 'settled',
            expected_authority_kind = NEW.authority_kind,
            currency = NEW.currency,
            settlement_claim_id = NEW.claim_id,
            pending_reason_code = NULL,
            settled_at_ms = NEW.created_at_ms,
            updated_at_ms = NEW.created_at_ms,
            control_version = obligation.control_version + 1
        FROM provider_usage_facts fact
        WHERE fact.usage_fact_id = NEW.source_usage_fact_id
          AND obligation.receipt_id = fact.receipt_id
          AND obligation.state IN ('expected', 'pending');
    ELSIF NEW.authority_kind = 'provider_allocated' THEN
        UPDATE provider_cost_obligations obligation
        SET state = 'settled',
            expected_authority_kind = NEW.authority_kind,
            currency = NEW.currency,
            settlement_claim_id = NEW.claim_id,
            pending_reason_code = NULL,
            settled_at_ms = NEW.created_at_ms,
            updated_at_ms = NEW.created_at_ms,
            control_version = obligation.control_version + 1
        FROM provider_receipts receipt,
             provider_cost_allocation_lines line
        WHERE line.provider_cost_allocation_line_id =
              NEW.source_provider_cost_allocation_line_id
          AND line.provider_cost_allocation_pool_id =
              NEW.source_provider_cost_allocation_pool_id
          AND obligation.receipt_id = receipt.receipt_id
          AND obligation.provider_id = NEW.provider_id
          AND obligation.provider_account_id =
              NEW.provider_account_id
          AND obligation.job_id = NEW.job_id
          AND (
              line.output_id IS NULL
              OR obligation.output_id = line.output_id
          )
          AND NEW.authority_period @> receipt.created_at_ms
          AND obligation.state IN ('expected', 'pending');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_cost_authority_claims_settle_obligations
AFTER INSERT ON provider_cost_authority_claims
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION settle_provider_cost_obligations_from_claim();

INSERT INTO provider_cost_obligations (
    receipt_id, submission_id, output_id, job_id,
    provider_id, provider_account_id, currency, state,
    expected_authority_kind, settlement_claim_id,
    pending_reason_code, due_at_ms, escalate_at_ms,
    pending_since_ms, control_version, created_at_ms, updated_at_ms,
    settled_at_ms
)
SELECT
    receipt.receipt_id,
    receipt.submission_id,
    receipt.output_id,
    receipt.job_id,
    receipt.provider_id,
    submission.provider_account_id,
    COALESCE(actual_claim.currency, allocated_claim.currency),
    CASE
        WHEN COALESCE(actual_claim.claim_id, allocated_claim.claim_id)
             IS NULL THEN 'pending'
        ELSE 'settled'
    END,
    COALESCE(
        actual_claim.authority_kind,
        allocated_claim.authority_kind
    ),
    COALESCE(actual_claim.claim_id, allocated_claim.claim_id),
    CASE
        WHEN COALESCE(actual_claim.claim_id, allocated_claim.claim_id)
             IS NOT NULL THEN NULL
        WHEN submission.provider_account_id IS NULL
             THEN 'legacy_unbound_account'
        WHEN receipt.outcome = 'uncertain'
             THEN 'provider_outcome_uncertain'
        ELSE 'policy_unresolved'
    END,
    receipt.created_at_ms + 86400000,
    receipt.created_at_ms + 172800000,
    CASE
        WHEN COALESCE(actual_claim.claim_id, allocated_claim.claim_id)
             IS NULL THEN receipt.created_at_ms
        ELSE NULL
    END,
    1,
    receipt.created_at_ms,
    COALESCE(
        actual_claim.created_at_ms,
        allocated_claim.created_at_ms,
        receipt.created_at_ms
    ),
    COALESCE(actual_claim.created_at_ms, allocated_claim.created_at_ms)
FROM provider_receipts receipt
JOIN provider_submissions submission
  ON submission.submission_id = receipt.submission_id
 AND submission.output_id = receipt.output_id
 AND submission.job_id = receipt.job_id
 AND submission.provider_id = receipt.provider_id
LEFT JOIN LATERAL (
    SELECT claim.claim_id, claim.authority_kind,
           claim.currency, claim.created_at_ms
    FROM provider_cost_authority_claims claim
    JOIN provider_usage_facts fact
      ON fact.usage_fact_id = claim.source_usage_fact_id
    WHERE claim.authority_kind = 'provider_actual'
      AND fact.receipt_id = receipt.receipt_id
    ORDER BY claim.claim_id
    LIMIT 1
) actual_claim ON TRUE
LEFT JOIN LATERAL (
    SELECT claim.claim_id, claim.authority_kind,
           claim.currency, claim.created_at_ms
    FROM provider_cost_authority_claims claim
    JOIN provider_cost_allocation_lines line
      ON line.provider_cost_allocation_line_id =
         claim.source_provider_cost_allocation_line_id
     AND line.provider_cost_allocation_pool_id =
         claim.source_provider_cost_allocation_pool_id
    WHERE claim.authority_kind = 'provider_allocated'
      AND claim.provider_id = receipt.provider_id
      AND claim.provider_account_id =
          submission.provider_account_id
      AND claim.job_id = receipt.job_id
      AND (
          line.output_id IS NULL
          OR line.output_id = receipt.output_id
      )
      AND claim.authority_period @> receipt.created_at_ms
    ORDER BY claim.claim_id
    LIMIT 1
) allocated_claim ON TRUE;

CREATE FUNCTION verify_provider_receipt_cost_obligation()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM provider_cost_obligations obligation
        WHERE obligation.receipt_id = NEW.receipt_id
          AND obligation.submission_id = NEW.submission_id
          AND obligation.output_id = NEW.output_id
          AND obligation.job_id = NEW.job_id
          AND obligation.provider_id = NEW.provider_id
    ) THEN
        RAISE EXCEPTION 'provider receipt lacks a cost obligation'
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER provider_receipts_require_cost_obligation
AFTER INSERT ON provider_receipts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION verify_provider_receipt_cost_obligation();

COMMENT ON TABLE provider_cost_obligations IS
    'Receipt-scoped provider cost lifecycle; amounts remain authoritative only in actual or allocation facts.';

COMMENT ON TABLE provider_cost_obligation_events IS
    'Immutable lifecycle history for provider cost obligations.';
