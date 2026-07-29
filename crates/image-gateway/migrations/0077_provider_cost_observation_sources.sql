CREATE TABLE provider_cost_observation_sources (
    provider_cost_observation_id UUID PRIMARY KEY
        REFERENCES provider_cost_observations(provider_cost_observation_id)
        ON DELETE RESTRICT,
    source_kind TEXT NOT NULL CHECK (
        source_kind IN ('executor_verified', 'legacy_unverified')
    ),
    executor_provider_cost_evidence_manifest_id UUID UNIQUE
        REFERENCES executor_provider_cost_evidence(manifest_id)
        ON DELETE RESTRICT,
    legacy_reason TEXT CHECK (
        legacy_reason IS NULL
        OR legacy_reason IN (
            'manual_import',
            'incomplete_linkage'
        )
    ),
    created_at_ms BIGINT NOT NULL,
    CHECK (
        (
            source_kind = 'executor_verified'
            AND executor_provider_cost_evidence_manifest_id IS NOT NULL
            AND legacy_reason IS NULL
        )
        OR
        (
            source_kind = 'legacy_unverified'
            AND executor_provider_cost_evidence_manifest_id IS NULL
            AND legacy_reason IS NOT NULL
        )
    )
);

CREATE INDEX provider_cost_observation_sources_kind_idx
    ON provider_cost_observation_sources(source_kind, created_at_ms);

WITH receipt_shape AS (
    SELECT provider_cost_observation_id,
           COUNT(*) AS receipt_count,
           (array_agg(receipt_id ORDER BY receipt_id))[1] AS receipt_id
    FROM provider_cost_observation_receipts
    GROUP BY provider_cost_observation_id
),
fact_shape AS (
    SELECT provider_cost_observation_id,
           COUNT(*) AS fact_count,
           (array_agg(usage_fact_id ORDER BY usage_fact_id))[1]
               AS usage_fact_id
    FROM provider_cost_observation_fact_links
    GROUP BY provider_cost_observation_id
),
candidates AS (
    SELECT observation.provider_cost_observation_id,
           evidence.manifest_id,
           observation.created_at_ms,
           COUNT(*) OVER (
               PARTITION BY observation.provider_cost_observation_id
           ) AS observation_matches,
           COUNT(*) OVER (
               PARTITION BY evidence.manifest_id
           ) AS evidence_matches
    FROM provider_cost_observations observation
    JOIN receipt_shape receipt_link
      ON receipt_link.provider_cost_observation_id =
         observation.provider_cost_observation_id
     AND receipt_link.receipt_count = 1
    JOIN provider_receipts receipt
      ON receipt.receipt_id = receipt_link.receipt_id
    JOIN fact_shape fact_link
      ON fact_link.provider_cost_observation_id =
         observation.provider_cost_observation_id
     AND fact_link.fact_count = 1
    JOIN provider_usage_facts fact
      ON fact.usage_fact_id = fact_link.usage_fact_id
     AND fact.receipt_id = receipt.receipt_id
    JOIN provider_submissions submission
      ON submission.submission_id = receipt.submission_id
     AND submission.output_id = receipt.output_id
     AND submission.job_id = receipt.job_id
     AND submission.provider_id = receipt.provider_id
    JOIN executor_provider_cost_evidence evidence
      ON evidence.submission_id = submission.submission_id
     AND evidence.executor_execution_id =
         submission.executor_execution_id
    WHERE observation.execution_surface <> 'manual_import'
      AND observation.provider_account_id =
          submission.provider_account_id
      AND fact.submission_id = submission.submission_id
      AND fact.job_id = submission.job_id
      AND fact.output_id = submission.output_id
      AND fact.provider_account_id =
          submission.provider_account_id
      AND evidence.provider_id = observation.provider_id
      AND evidence.execution_surface =
          observation.execution_surface
      AND evidence.provider_operation_id =
          observation.provider_operation_id
      AND evidence.currency = observation.currency
      AND evidence.native_unit = observation.native_unit
      AND evidence.native_quantity = observation.native_quantity
      AND evidence.authority = observation.authority
      AND evidence.confidence = observation.confidence
      AND evidence.evidence_hash = observation.evidence_hash
      AND evidence.evidence_path = observation.evidence_path
      AND observation.created_at_ms >= evidence.created_at_ms
)
INSERT INTO provider_cost_observation_sources (
    provider_cost_observation_id, source_kind,
    executor_provider_cost_evidence_manifest_id,
    legacy_reason, created_at_ms
)
SELECT provider_cost_observation_id, 'executor_verified',
       manifest_id, NULL, created_at_ms
FROM candidates
WHERE observation_matches = 1
  AND evidence_matches = 1;

INSERT INTO provider_cost_observation_sources (
    provider_cost_observation_id, source_kind,
    executor_provider_cost_evidence_manifest_id,
    legacy_reason, created_at_ms
)
SELECT observation.provider_cost_observation_id,
       'legacy_unverified',
       NULL,
       CASE
           WHEN observation.execution_surface = 'manual_import'
               THEN 'manual_import'
           ELSE 'incomplete_linkage'
       END,
       observation.created_at_ms
FROM provider_cost_observations observation
LEFT JOIN provider_cost_observation_sources source
  ON source.provider_cost_observation_id =
     observation.provider_cost_observation_id
WHERE source.provider_cost_observation_id IS NULL;

CREATE FUNCTION validate_provider_cost_observation_source()
RETURNS TRIGGER AS $$
DECLARE
    target_observation_id UUID;
    source provider_cost_observation_sources%ROWTYPE;
    valid_link_count BIGINT;
BEGIN
    target_observation_id :=
        COALESCE(NEW.provider_cost_observation_id,
                 OLD.provider_cost_observation_id);

    SELECT * INTO STRICT source
    FROM provider_cost_observation_sources
    WHERE provider_cost_observation_id =
          target_observation_id;

    IF source.source_kind = 'legacy_unverified' THEN
        RETURN COALESCE(NEW, OLD);
    END IF;

    SELECT COUNT(*) INTO valid_link_count
    FROM provider_cost_observations observation
    JOIN executor_provider_cost_evidence evidence
      ON evidence.manifest_id =
         source.executor_provider_cost_evidence_manifest_id
    JOIN provider_cost_observation_receipts receipt_link
      ON receipt_link.provider_cost_observation_id =
         observation.provider_cost_observation_id
    JOIN provider_receipts receipt
      ON receipt.receipt_id = receipt_link.receipt_id
     AND receipt.provider_id = receipt_link.provider_id
    JOIN provider_cost_observation_fact_links fact_link
      ON fact_link.provider_cost_observation_id =
         observation.provider_cost_observation_id
    JOIN provider_usage_facts fact
      ON fact.usage_fact_id = fact_link.usage_fact_id
    JOIN provider_submissions submission
      ON submission.submission_id = receipt.submission_id
     AND submission.executor_execution_id =
         evidence.executor_execution_id
     AND submission.output_id = receipt.output_id
     AND submission.job_id = receipt.job_id
     AND submission.provider_id = receipt.provider_id
    WHERE observation.provider_cost_observation_id =
          target_observation_id
      AND (
          SELECT COUNT(*)
          FROM provider_cost_observation_receipts exact_receipt
          WHERE exact_receipt.provider_cost_observation_id =
                target_observation_id
      ) = 1
      AND (
          SELECT COUNT(*)
          FROM provider_cost_observation_fact_links exact_fact
          WHERE exact_fact.provider_cost_observation_id =
                target_observation_id
      ) = 1
      AND evidence.submission_id = submission.submission_id
      AND observation.provider_id = evidence.provider_id
      AND observation.provider_account_id =
          submission.provider_account_id
      AND observation.execution_surface =
          evidence.execution_surface
      AND observation.provider_operation_id =
          evidence.provider_operation_id
      AND observation.currency = evidence.currency
      AND observation.native_unit = evidence.native_unit
      AND observation.native_quantity =
          evidence.native_quantity
      AND observation.authority = evidence.authority
      AND observation.confidence = evidence.confidence
      AND observation.evidence_hash = evidence.evidence_hash
      AND observation.evidence_path = evidence.evidence_path
      AND observation.created_at_ms >= evidence.created_at_ms
      AND fact.receipt_id = receipt.receipt_id
      AND fact.submission_id = submission.submission_id
      AND fact.job_id = submission.job_id
      AND fact.output_id = submission.output_id
      AND fact.provider_id = observation.provider_id
      AND fact.provider_account_id =
          observation.provider_account_id
      AND fact.execution_surface =
          observation.execution_surface
      AND fact.fact_domain = 'provider_actual'
      AND fact.metric = 'provider_reported_cost'
      AND fact.unit = observation.native_unit
      AND fact.quantity::NUMERIC =
          observation.native_quantity
      AND fact.quantity_source = observation.authority
      AND fact.confidence = observation.confidence;

    IF valid_link_count <> 1 THEN
        RAISE EXCEPTION
            'provider cost observation source is not executor verified'
            USING ERRCODE = '23514';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION require_provider_cost_observation_source()
RETURNS TRIGGER AS $$
BEGIN
    IF (
        SELECT COUNT(*)
        FROM provider_cost_observation_sources source
        WHERE source.provider_cost_observation_id =
              NEW.provider_cost_observation_id
    ) <> 1 THEN
        RAISE EXCEPTION
            'provider cost observation requires one source'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION reject_new_unverified_provider_cost_source()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.source_kind <> 'executor_verified' THEN
        RAISE EXCEPTION
            'new provider cost observations require executor evidence'
            USING ERRCODE = '0A000';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER provider_cost_observation_sources_require_verified
BEFORE INSERT ON provider_cost_observation_sources
FOR EACH ROW EXECUTE FUNCTION
    reject_new_unverified_provider_cost_source();

CREATE CONSTRAINT TRIGGER provider_cost_observations_require_source
AFTER INSERT ON provider_cost_observations
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION
    require_provider_cost_observation_source();

CREATE CONSTRAINT TRIGGER provider_cost_observation_sources_validate
AFTER INSERT ON provider_cost_observation_sources
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION
    validate_provider_cost_observation_source();

CREATE CONSTRAINT TRIGGER provider_cost_observation_fact_links_validate_source
AFTER INSERT ON provider_cost_observation_fact_links
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION
    validate_provider_cost_observation_source();

CREATE CONSTRAINT TRIGGER provider_cost_observation_receipts_validate_source
AFTER INSERT ON provider_cost_observation_receipts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION
    validate_provider_cost_observation_source();

CREATE TRIGGER provider_cost_observation_sources_reject_mutation
BEFORE UPDATE OR DELETE ON provider_cost_observation_sources
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

CREATE TRIGGER provider_cost_observation_sources_reject_truncate
BEFORE TRUNCATE ON provider_cost_observation_sources
FOR EACH STATEMENT EXECUTE FUNCTION reject_economic_fact_truncate();
