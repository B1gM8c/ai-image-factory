CREATE TABLE project_spend_budgets (
    project_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    monthly_budget_micros BIGINT NOT NULL CHECK (monthly_budget_micros > 0),
    limit_type TEXT NOT NULL DEFAULT 'soft' CHECK (limit_type = 'soft'),
    period_kind TEXT NOT NULL DEFAULT 'calendar_month_utc'
        CHECK (period_kind = 'calendar_month_utc'),
    control_version BIGINT NOT NULL DEFAULT 1 CHECK (control_version > 0),
    created_by_user_id UUID NOT NULL
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    updated_by_user_id UUID NOT NULL
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    FOREIGN KEY (project_id, organization_id)
        REFERENCES gateway_projects(id, tenant_id) ON DELETE RESTRICT,
    CHECK (updated_at_ms >= created_at_ms)
);

CREATE TABLE project_spend_alert_thresholds (
    project_id TEXT NOT NULL
        REFERENCES project_spend_budgets(project_id) ON DELETE CASCADE,
    threshold_percent SMALLINT NOT NULL
        CHECK (threshold_percent BETWEEN 1 AND 100),
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (project_id, threshold_percent)
);

CREATE TABLE project_spend_evaluation_queue (
    project_id TEXT PRIMARY KEY
        REFERENCES project_spend_budgets(project_id) ON DELETE CASCADE,
    requested_at_ms BIGINT NOT NULL
);

CREATE TABLE project_spend_alert_events (
    event_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    period_start_ms BIGINT NOT NULL,
    period_end_ms BIGINT NOT NULL,
    threshold_percent SMALLINT NOT NULL
        CHECK (threshold_percent BETWEEN 1 AND 100),
    budget_control_version BIGINT NOT NULL CHECK (budget_control_version > 0),
    monthly_budget_micros BIGINT NOT NULL CHECK (monthly_budget_micros > 0),
    spend_micros BIGINT NOT NULL CHECK (spend_micros >= 0),
    notification_state TEXT NOT NULL DEFAULT 'pending'
        CHECK (notification_state IN ('pending', 'acknowledged')),
    created_at_ms BIGINT NOT NULL,
    acknowledged_at_ms BIGINT,
    acknowledged_by_user_id UUID
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    FOREIGN KEY (project_id, organization_id)
        REFERENCES gateway_projects(id, tenant_id) ON DELETE RESTRICT,
    UNIQUE (
        project_id,
        currency,
        period_start_ms,
        threshold_percent,
        budget_control_version
    ),
    CHECK (period_end_ms > period_start_ms),
    CHECK (
        (notification_state = 'pending'
         AND acknowledged_at_ms IS NULL
         AND acknowledged_by_user_id IS NULL)
        OR
        (notification_state = 'acknowledged'
         AND acknowledged_at_ms IS NOT NULL
         AND acknowledged_by_user_id IS NOT NULL)
    )
);

CREATE INDEX project_spend_alert_events_project_period_idx
    ON project_spend_alert_events(
        project_id,
        period_start_ms DESC,
        threshold_percent
    );

CREATE TABLE project_spend_notification_deliveries (
    delivery_id UUID PRIMARY KEY,
    event_id UUID NOT NULL
        REFERENCES project_spend_alert_events(event_id) ON DELETE RESTRICT,
    recipient_user_id UUID NOT NULL
        REFERENCES identity_users(user_id) ON DELETE RESTRICT,
    channel TEXT NOT NULL DEFAULT 'in_app' CHECK (channel = 'in_app'),
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'delivered', 'dead_letter')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at_ms BIGINT NOT NULL,
    lease_owner TEXT,
    lease_expires_at_ms BIGINT,
    last_error_code TEXT,
    created_at_ms BIGINT NOT NULL,
    delivered_at_ms BIGINT,
    read_at_ms BIGINT,
    UNIQUE (event_id, recipient_user_id, channel),
    CHECK (
        (lease_owner IS NULL AND lease_expires_at_ms IS NULL)
        OR (lease_owner IS NOT NULL AND lease_expires_at_ms IS NOT NULL)
    ),
    CHECK (
        (state = 'delivered' AND delivered_at_ms IS NOT NULL)
        OR (state <> 'delivered' AND delivered_at_ms IS NULL)
    ),
    CHECK (read_at_ms IS NULL OR delivered_at_ms IS NOT NULL),
    CHECK (read_at_ms IS NULL OR read_at_ms >= delivered_at_ms)
);

CREATE INDEX project_spend_notification_deliveries_pending_idx
    ON project_spend_notification_deliveries(next_attempt_at_ms, delivery_id)
    WHERE state = 'pending';

CREATE INDEX project_spend_notification_deliveries_inbox_idx
    ON project_spend_notification_deliveries(
        recipient_user_id,
        read_at_ms NULLS FIRST,
        created_at_ms DESC,
        delivery_id DESC
    )
    WHERE state = 'delivered';

CREATE INDEX job_auth_attributions_project_job_idx
    ON job_auth_attributions(project_id, job_id)
    WHERE project_id IS NOT NULL;

CREATE INDEX customer_rated_usage_created_quote_idx
    ON customer_rated_usage(created_at_ms, quote_id);

CREATE FUNCTION enqueue_project_spend_evaluation_from_legacy_rating()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO project_spend_evaluation_queue(project_id, requested_at_ms)
    SELECT attribution.project_id, NEW.created_at_ms
    FROM job_auth_attributions attribution
    JOIN project_spend_budgets budget
      ON budget.project_id = attribution.project_id
    WHERE attribution.job_id = NEW.job_id
      AND attribution.project_id IS NOT NULL
    ON CONFLICT (project_id) DO UPDATE
    SET requested_at_ms = GREATEST(
        project_spend_evaluation_queue.requested_at_ms,
        EXCLUDED.requested_at_ms
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER rated_usage_enqueue_project_spend_evaluation
AFTER INSERT ON rated_usage
FOR EACH ROW EXECUTE FUNCTION enqueue_project_spend_evaluation_from_legacy_rating();

CREATE FUNCTION enqueue_project_spend_evaluation_from_customer_rating()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO project_spend_evaluation_queue(project_id, requested_at_ms)
    SELECT quote.project_id, NEW.created_at_ms
    FROM customer_price_quotes quote
    JOIN project_spend_budgets budget
      ON budget.project_id = quote.project_id
    WHERE quote.quote_id = NEW.quote_id
      AND quote.job_id = NEW.job_id
    ON CONFLICT (project_id) DO UPDATE
    SET requested_at_ms = GREATEST(
        project_spend_evaluation_queue.requested_at_ms,
        EXCLUDED.requested_at_ms
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER customer_rated_usage_enqueue_project_spend_evaluation
AFTER INSERT ON customer_rated_usage
FOR EACH ROW EXECUTE FUNCTION enqueue_project_spend_evaluation_from_customer_rating();

COMMENT ON TABLE project_spend_budgets IS
    'OpenAI-style project monitoring budgets: UTC calendar month soft thresholds only.';

COMMENT ON TABLE project_spend_evaluation_queue IS
    'Commit-coupled hints for asynchronous alert evaluation; rated usage remains authoritative.';
