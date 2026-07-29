ALTER TABLE project_spend_budgets
    DROP CONSTRAINT project_spend_budgets_limit_type_check;

ALTER TABLE project_spend_budgets
    ADD CONSTRAINT project_spend_budgets_limit_type_check
    CHECK (limit_type IN ('soft', 'hard'));

COMMENT ON TABLE project_spend_budgets IS
    'UTC calendar-month project spend controls with soft alerts and optional admission-time hard limits.';
