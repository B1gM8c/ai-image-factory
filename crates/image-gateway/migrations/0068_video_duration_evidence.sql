ALTER TABLE executor_artifact_authorities
    ADD COLUMN media_duration_ms BIGINT CHECK (
        media_duration_ms IS NULL
        OR (
            media_type = 'video/mp4'
            AND media_duration_ms BETWEEN 1 AND 86400000
        )
    );

DROP TRIGGER provider_usage_facts_reject_mutation
    ON provider_usage_facts;

ALTER TABLE provider_usage_facts
    ADD COLUMN fact_domain TEXT CHECK (
        fact_domain IN (
            'customer_billable',
            'provider_actual',
            'provider_estimated',
            'provider_allocated',
            'provider_benchmark'
        )
    );

UPDATE provider_usage_facts
SET fact_domain = CASE
    WHEN metric = 'provider_reported_cost' THEN 'provider_actual'
    ELSE 'customer_billable'
END;

ALTER TABLE provider_usage_facts
    ALTER COLUMN fact_domain SET NOT NULL;

CREATE TRIGGER provider_usage_facts_reject_mutation
BEFORE UPDATE OR DELETE ON provider_usage_facts
FOR EACH ROW EXECUTE FUNCTION reject_economic_fact_mutation();

ALTER TABLE price_components
    DROP CONSTRAINT price_components_metric_check,
    DROP CONSTRAINT price_components_quantity_source_check,
    DROP CONSTRAINT price_components_check,
    ADD CONSTRAINT price_components_metric_check CHECK (
        metric IN (
            'request',
            'image_input',
            'image_output',
            'text_input_token',
            'cached_text_input_token',
            'image_input_token',
            'cached_image_input_token',
            'image_output_token',
            'video_input_token',
            'video_output_token',
            'video_input_second',
            'video_requested_second',
            'video_output_second',
            'membership_point'
        )
    ),
    ADD CONSTRAINT price_components_quantity_source_check CHECK (
        quantity_source IN (
            'provider_reported',
            'request_derived',
            'media_inspected',
            'official_lookup',
            'operator_adjustment'
        )
    ),
    ADD CONSTRAINT price_components_check CHECK (
        (metric = 'request' AND unit = 'request')
        OR (metric IN ('image_input', 'image_output') AND unit = 'image')
        OR (
            metric IN (
                'text_input_token',
                'cached_text_input_token',
                'image_input_token',
                'cached_image_input_token',
                'image_output_token',
                'video_input_token',
                'video_output_token'
            )
            AND unit = 'token'
        )
        OR (
            metric IN (
                'video_input_second',
                'video_requested_second',
                'video_output_second'
            )
            AND unit = 'second'
        )
        OR (metric = 'membership_point' AND unit = 'point')
    );

ALTER TABLE provider_usage_facts
    DROP CONSTRAINT provider_usage_facts_metric_check,
    DROP CONSTRAINT provider_usage_facts_quantity_source_check,
    DROP CONSTRAINT provider_usage_facts_check,
    ADD CONSTRAINT provider_usage_facts_metric_check CHECK (
        metric IN (
            'request',
            'image_input',
            'image_output',
            'text_input_token',
            'cached_text_input_token',
            'image_input_token',
            'cached_image_input_token',
            'image_output_token',
            'video_input_token',
            'video_output_token',
            'video_input_second',
            'video_requested_second',
            'video_output_second',
            'membership_point',
            'provider_reported_cost'
        )
    ),
    ADD CONSTRAINT provider_usage_facts_quantity_source_check CHECK (
        quantity_source IN (
            'provider_reported',
            'request_derived',
            'media_inspected',
            'official_lookup',
            'operator_adjustment'
        )
    ),
    ADD CONSTRAINT provider_usage_facts_check CHECK (
        (metric = 'request' AND unit = 'request')
        OR (metric IN ('image_input', 'image_output') AND unit = 'image')
        OR (
            metric IN (
                'text_input_token',
                'cached_text_input_token',
                'image_input_token',
                'cached_image_input_token',
                'image_output_token',
                'video_input_token',
                'video_output_token'
            )
            AND unit = 'token'
        )
        OR (
            metric IN (
                'video_input_second',
                'video_requested_second',
                'video_output_second'
            )
            AND unit = 'second'
        )
        OR (metric = 'membership_point' AND unit = 'point')
        OR (metric = 'provider_reported_cost' AND unit = 'usd_tick')
    );

ALTER TABLE customer_price_quote_lines
    DROP CONSTRAINT customer_price_quote_lines_metric_check,
    ADD CONSTRAINT customer_price_quote_lines_metric_check CHECK (
        metric IN (
            'request',
            'image_input',
            'image_output',
            'text_input_token',
            'cached_text_input_token',
            'image_input_token',
            'cached_image_input_token',
            'image_output_token',
            'video_input_token',
            'video_output_token',
            'video_input_second',
            'video_requested_second',
            'video_output_second',
            'membership_point'
        )
    );

CREATE OR REPLACE FUNCTION validate_customer_rating_fact_set() RETURNS TRIGGER AS $$
DECLARE
    target_rating_id UUID;
    target_job_id UUID;
BEGIN
    IF TG_TABLE_NAME = 'customer_rated_usage' THEN
        target_rating_id := COALESCE(NEW.rated_usage_id, OLD.rated_usage_id);
    ELSIF TG_TABLE_NAME = 'customer_rated_usage_lines' THEN
        target_rating_id := COALESCE(NEW.rated_usage_id, OLD.rated_usage_id);
    ELSE
        SELECT line.rated_usage_id INTO STRICT target_rating_id
        FROM customer_rated_usage_lines line
        WHERE line.rated_usage_line_id =
            COALESCE(NEW.rated_usage_line_id, OLD.rated_usage_line_id);
    END IF;

    SELECT job_id INTO STRICT target_job_id
    FROM customer_rated_usage
    WHERE rated_usage_id = target_rating_id;

    IF EXISTS (
        SELECT 1
        FROM customer_rated_usage_lines line
        LEFT JOIN customer_rated_usage_fact_links link
          ON link.rated_usage_line_id = line.rated_usage_line_id
        LEFT JOIN provider_usage_facts fact
          ON fact.usage_fact_id = link.usage_fact_id
         AND fact.fact_domain = 'customer_billable'
        WHERE line.rated_usage_id = target_rating_id
        GROUP BY line.rated_usage_line_id, line.actual_quantity
        HAVING COUNT(fact.usage_fact_id) = 0
            OR COALESCE(SUM(fact.quantity::NUMERIC), 0)
                <> line.actual_quantity::NUMERIC
    ) OR EXISTS (
        SELECT 1
        FROM customer_rated_usage_fact_links link
        JOIN customer_rated_usage_lines line
          ON line.rated_usage_line_id = link.rated_usage_line_id
        JOIN provider_usage_facts fact
          ON fact.usage_fact_id = link.usage_fact_id
        WHERE line.rated_usage_id = target_rating_id
          AND fact.fact_domain <> 'customer_billable'
    ) OR EXISTS (
        SELECT 1
        FROM provider_usage_facts fact
        WHERE fact.job_id = target_job_id
          AND fact.fact_domain = 'customer_billable'
          AND NOT EXISTS (
              SELECT 1
              FROM customer_rated_usage_fact_links link
              JOIN customer_rated_usage_lines line
                ON line.rated_usage_line_id = link.rated_usage_line_id
              WHERE line.rated_usage_id = target_rating_id
                AND link.usage_fact_id = fact.usage_fact_id
          )
    ) THEN
        RAISE EXCEPTION 'customer rating does not exactly cover immutable usage facts'
            USING ERRCODE = '23514';
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_v4_provider_usage_fact_insert() RETURNS TRIGGER AS $$
DECLARE
    contract_version SMALLINT;
    quote_tenant_id TEXT;
    quote_currency TEXT;
    receipt_outcome TEXT;
    submission_account_id UUID;
BEGIN
    SELECT job.economics_contract_version
      INTO STRICT contract_version
    FROM jobs job
    WHERE job.job_id = NEW.job_id;

    SELECT receipt.outcome, submission.provider_account_id
      INTO STRICT receipt_outcome, submission_account_id
    FROM provider_receipts receipt
    JOIN provider_submissions submission
      ON submission.submission_id = receipt.submission_id
     AND submission.output_id = receipt.output_id
     AND submission.job_id = receipt.job_id
     AND submission.provider_id = receipt.provider_id
    WHERE receipt.receipt_id = NEW.receipt_id
      AND receipt.submission_id = NEW.submission_id
      AND receipt.output_id = NEW.output_id
      AND receipt.job_id = NEW.job_id
      AND receipt.provider_id = NEW.provider_id;

    IF NEW.provider_account_id IS DISTINCT FROM submission_account_id
       OR (
           NEW.metric <> 'provider_reported_cost'
           AND NEW.terminal_outcome IS DISTINCT FROM receipt_outcome
       ) THEN
        RAISE EXCEPTION 'provider usage fact conflicts with canonical receipt'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.metric = 'provider_reported_cost'
       AND NEW.fact_domain <> 'provider_actual' THEN
        RAISE EXCEPTION 'provider cost fact has an invalid domain'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.quantity_source = 'media_inspected'
       AND (
           NEW.fact_domain <> 'provider_benchmark'
           OR NEW.metric <> 'video_output_second'
           OR NEW.unit <> 'second'
           OR NEW.confidence <> 'exact'
           OR NEW.quantity <= 0
           OR NEW.terminal_outcome <> 'succeeded'
       ) THEN
        RAISE EXCEPTION 'media inspected fact has an invalid domain'
            USING ERRCODE = '23514';
    END IF;

    IF contract_version = 4
       AND NEW.fact_domain = 'customer_billable' THEN
        SELECT tenant_id, currency
          INTO STRICT quote_tenant_id, quote_currency
        FROM customer_price_quotes
        WHERE job_id = NEW.job_id;
        PERFORM pg_advisory_xact_lock(
            hashtextextended('budget:' || quote_tenant_id || ':' || quote_currency, 0)
        );
        PERFORM 1 FROM jobs WHERE job_id = NEW.job_id FOR UPDATE;
        IF EXISTS (
            SELECT 1 FROM customer_rated_usage WHERE job_id = NEW.job_id
        ) THEN
            RAISE EXCEPTION 'settled customer usage facts are immutable'
                USING ERRCODE = '55000';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
