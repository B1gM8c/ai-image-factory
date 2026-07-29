use sqlx::{AssertSqlSafe, FromRow};

use super::{
    AdminReadError, AdminReadScope, MAX_BILLING_WINDOW_MS, MAX_USAGE_SERIES_ROWS,
    UsageActivityPoint, UsageAnalysisQuery, UsageAnalysisSnapshot, UsageFilterOption,
    UsageFilterOptions, UsageInterval, UsageSpendPoint, postgres::PostgresAdminReadStore,
};

impl PostgresAdminReadStore {
    pub(super) async fn usage_analysis_scoped_impl(
        &self,
        scope: &AdminReadScope,
        query: UsageAnalysisQuery,
    ) -> Result<UsageAnalysisSnapshot, AdminReadError> {
        validate_query(&query)?;
        let (mut tx, window) = self
            .begin_snapshot(query.window_ms, MAX_BILLING_WINDOW_MS)
            .await?;
        let tenant_ids = scope.tenant_ids().map(|ids| ids.to_vec());
        let effective_actor_user_id =
            scope.actor_user_id_for_project(query.project_id.as_deref())?;

        let activity_sql = activity_sql();
        let activity = sqlx::query_as::<_, ActivityPointRow>(AssertSqlSafe(activity_sql))
            .bind(window.from_ms)
            .bind(window.to_ms)
            .bind(&tenant_ids)
            .bind(effective_actor_user_id)
            .bind(&query.project_id)
            .bind(&query.api_key_id)
            .bind(query.filter_user_id)
            .bind(&query.provider_id)
            .bind(&query.model)
            .bind(&query.operation)
            .bind(&query.service_tier)
            .bind(query.interval.as_millis())
            .bind(query.group_by.as_str())
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?;
        enforce_row_limit(activity.len())?;

        let spend_sql = spend_sql();
        let spend = sqlx::query_as::<_, SpendPointRow>(AssertSqlSafe(spend_sql))
            .bind(window.from_ms)
            .bind(window.to_ms)
            .bind(&tenant_ids)
            .bind(effective_actor_user_id)
            .bind(&query.project_id)
            .bind(&query.api_key_id)
            .bind(query.filter_user_id)
            .bind(&query.provider_id)
            .bind(&query.model)
            .bind(&query.operation)
            .bind(&query.service_tier)
            .bind(query.interval.as_millis())
            .bind(query.group_by.as_str())
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?;
        enforce_row_limit(spend.len())?;

        let include_user_options =
            matches!(scope, AdminReadScope::Platform) || query.project_id.is_some();
        let option_rows = sqlx::query_as::<_, FilterOptionRow>(FILTER_OPTIONS_SQL)
            .bind(window.from_ms)
            .bind(window.to_ms)
            .bind(&tenant_ids)
            .bind(effective_actor_user_id)
            .bind(&query.project_id)
            .bind(include_user_options)
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?;

        tx.commit().await.map_err(unavailable)?;
        Ok(UsageAnalysisSnapshot {
            as_of_ms: window.as_of_ms,
            from_ms: window.from_ms,
            to_ms: window.to_ms,
            interval: query.interval.as_str().to_string(),
            interval_ms: query.interval.as_millis(),
            group_by: query.group_by.as_str().to_string(),
            activity: activity.into_iter().map(Into::into).collect(),
            spend: spend.into_iter().map(Into::into).collect(),
            filter_options: collect_filter_options(option_rows),
        })
    }
}

fn validate_query(query: &UsageAnalysisQuery) -> Result<(), AdminReadError> {
    if query.interval.as_millis() > query.window_ms {
        return Err(invalid("interval cannot exceed the usage window"));
    }
    if query.interval == UsageInterval::Minute && query.window_ms > 24 * 60 * 60 * 1_000 {
        return Err(invalid("1m interval is limited to a 24h usage window"));
    }
    Ok(())
}

fn enforce_row_limit(row_count: usize) -> Result<(), AdminReadError> {
    if row_count > MAX_USAGE_SERIES_ROWS {
        return Err(invalid(
            "usage query produced too many grouped rows; narrow the window or filters",
        ));
    }
    Ok(())
}

fn collect_filter_options(rows: Vec<FilterOptionRow>) -> UsageFilterOptions {
    let mut options = UsageFilterOptions::default();
    for row in rows {
        let option = UsageFilterOption {
            value: row.value,
            label: row.label,
        };
        match row.kind.as_str() {
            "project" => options.projects.push(option),
            "api_key" => options.api_keys.push(option),
            "user" => options.users.push(option),
            "provider" => options.providers.push(option),
            "model" => options.models.push(option),
            "operation" => options.operations.push(option),
            "service_tier" => options.service_tiers.push(option),
            _ => {}
        }
    }
    options
}

fn invalid(message: impl Into<String>) -> AdminReadError {
    AdminReadError::InvalidQuery(message.into())
}

fn unavailable(error: sqlx::Error) -> AdminReadError {
    tracing::warn!(error = %error, "usage analysis query failed");
    AdminReadError::Unavailable
}

#[derive(FromRow)]
struct ActivityPointRow {
    bucket_start_ms: i64,
    group_kind: String,
    group_value: String,
    group_label: String,
    billing_metric: String,
    billing_unit: String,
    outcome: String,
    quantity: String,
}

impl From<ActivityPointRow> for UsageActivityPoint {
    fn from(row: ActivityPointRow) -> Self {
        Self {
            bucket_start_ms: row.bucket_start_ms,
            group_kind: row.group_kind,
            group_value: row.group_value,
            group_label: row.group_label,
            billing_metric: row.billing_metric,
            billing_unit: row.billing_unit,
            outcome: row.outcome,
            quantity: row.quantity,
        }
    }
}

#[derive(FromRow)]
struct SpendPointRow {
    bucket_start_ms: i64,
    group_kind: String,
    group_value: String,
    group_label: String,
    billing_metric: String,
    billing_unit: String,
    outcome: String,
    currency: String,
    quantity: String,
    amount_micros: String,
}

impl From<SpendPointRow> for UsageSpendPoint {
    fn from(row: SpendPointRow) -> Self {
        Self {
            bucket_start_ms: row.bucket_start_ms,
            group_kind: row.group_kind,
            group_value: row.group_value,
            group_label: row.group_label,
            billing_metric: row.billing_metric,
            billing_unit: row.billing_unit,
            outcome: row.outcome,
            currency: row.currency,
            quantity: row.quantity,
            amount_micros: row.amount_micros,
        }
    }
}

#[derive(FromRow)]
struct FilterOptionRow {
    kind: String,
    value: String,
    label: String,
}

const SELECTED_JOBS_SQL: &str = r#"
    SELECT job.job_id, job.tenant_id, job.provider_id, job.model,
           job.operation, job.state, job.economics_contract_version,
           job.created_at_ms, attribution.project_id,
           attribution.api_key_id,
           COALESCE(
               attribution.actor_user_id,
               attribution.credential_owner_user_id
           ) AS actor_user_id,
           COALESCE(project.name, attribution.project_id, '未归属项目')
               AS project_label,
           COALESCE(api_key.name, attribution.api_key_id, '直接登录')
               AS api_key_label,
           COALESCE(tier.effective_service_tier, 'default')
               AS effective_service_tier,
           CASE COALESCE(tier.effective_service_tier, 'default')
             WHEN 'default' THEN 'Default'
             WHEN 'flex' THEN 'Flex'
             WHEN 'priority' THEN 'Priority'
           END AS service_tier_label,
           COALESCE(identity_user.display_name,
                    attribution.actor_user_id::TEXT,
                    attribution.credential_owner_user_id::TEXT,
                    '服务账号/未归属')
               AS user_label
    FROM jobs job
    LEFT JOIN job_auth_attributions attribution
      ON attribution.job_id = job.job_id
    LEFT JOIN gateway_projects project
      ON project.id = attribution.project_id
     AND project.tenant_id = job.tenant_id
    LEFT JOIN gateway_api_keys api_key
      ON api_key.id = attribution.api_key_id
    LEFT JOIN identity_users identity_user
      ON identity_user.user_id = COALESCE(
          attribution.actor_user_id,
          attribution.credential_owner_user_id
      )
    LEFT JOIN job_service_tier_decisions tier
      ON tier.job_id = job.job_id
    WHERE ($3::TEXT[] IS NULL OR job.tenant_id = ANY($3))
      AND (
        $4::UUID IS NULL
        OR attribution.actor_user_id = $4
        OR attribution.credential_owner_user_id = $4
      )
      AND ($5::TEXT IS NULL OR attribution.project_id = $5)
      AND ($6::TEXT IS NULL OR attribution.api_key_id = $6)
      AND (
        $7::UUID IS NULL
        OR attribution.actor_user_id = $7
        OR attribution.credential_owner_user_id = $7
      )
      AND ($8::TEXT IS NULL OR job.provider_id = $8)
      AND ($9::TEXT IS NULL OR job.model = $9)
      AND ($10::TEXT IS NULL OR job.operation = $10)
      AND (
        $11::TEXT IS NULL
        OR COALESCE(tier.effective_service_tier, 'default') = $11
      )
"#;

fn activity_sql() -> String {
    format!(
        r#"
    WITH selected_jobs AS NOT MATERIALIZED (
    {SELECTED_JOBS_SQL}
    ),
    activity_events AS (
        SELECT job.*, job.created_at_ms AS event_at_ms,
               'request'::TEXT AS billing_metric,
               'request'::TEXT AS billing_unit,
               job.state AS outcome,
               1::NUMERIC AS quantity
        FROM selected_jobs job
        WHERE job.created_at_ms >= $1
          AND job.created_at_ms < $2

        UNION ALL

        SELECT job.*, usage.created_at_ms AS event_at_ms,
               usage.billing_metric, usage.billing_unit, usage.outcome,
               usage.units::NUMERIC AS quantity
        FROM usage_events usage
        JOIN selected_jobs job ON job.job_id = usage.job_id
        WHERE usage.created_at_ms >= $1
          AND usage.created_at_ms < $2
          AND job.economics_contract_version IN (1, 2, 3)
          AND usage.billing_metric <> 'request'

        UNION ALL

        SELECT job.*, rated_line.created_at_ms AS event_at_ms,
               quote_line.metric AS billing_metric,
               quote_line.unit AS billing_unit,
               quote_line.terminal_outcome AS outcome,
               rated_line.actual_quantity::NUMERIC AS quantity
        FROM customer_rated_usage_lines rated_line
        JOIN customer_rated_usage rated
          ON rated.rated_usage_id = rated_line.rated_usage_id
        JOIN customer_price_quote_lines quote_line
          ON quote_line.quote_line_id = rated_line.quote_line_id
         AND quote_line.quote_id = rated.quote_id
         AND quote_line.job_id = rated.job_id
        JOIN selected_jobs job ON job.job_id = rated.job_id
        WHERE rated_line.created_at_ms >= $1
          AND rated_line.created_at_ms < $2
          AND job.economics_contract_version = 4
          AND quote_line.metric <> 'request'
    )
    SELECT (event_at_ms / $12) * $12 AS bucket_start_ms,
           $13::TEXT AS group_kind,
           CASE $13
             WHEN 'none' THEN 'all'
             WHEN 'line_item' THEN billing_metric
             WHEN 'project' THEN COALESCE(project_id, 'unattributed')
             WHEN 'api_key' THEN COALESCE(api_key_id, 'session')
             WHEN 'user' THEN COALESCE(actor_user_id::TEXT, 'service_account')
             WHEN 'provider' THEN provider_id
             WHEN 'model' THEN model
             WHEN 'operation' THEN operation
             WHEN 'service_tier' THEN effective_service_tier
           END AS group_value,
           CASE $13
             WHEN 'none' THEN '全部'
             WHEN 'line_item' THEN billing_metric
             WHEN 'project' THEN project_label
             WHEN 'api_key' THEN api_key_label
             WHEN 'user' THEN user_label
             WHEN 'provider' THEN provider_id
             WHEN 'model' THEN model
             WHEN 'operation' THEN operation
             WHEN 'service_tier' THEN service_tier_label
           END AS group_label,
           billing_metric, billing_unit, outcome,
           SUM(quantity)::TEXT AS quantity
    FROM activity_events
    GROUP BY bucket_start_ms, group_value, group_label,
             billing_metric, billing_unit, outcome
    ORDER BY bucket_start_ms, group_label, billing_metric,
             billing_unit, outcome
    LIMIT 5001
    "#
    )
}

fn spend_sql() -> String {
    format!(
        r#"
    WITH selected_jobs AS NOT MATERIALIZED (
    {SELECTED_JOBS_SQL}
    ),
    rating_lines AS (
        SELECT job.*, rated.created_at_ms AS event_at_ms,
               meter.metric AS billing_metric,
               meter.unit AS billing_unit,
               rated.outcome, rated.currency,
               rated.quantity::NUMERIC AS quantity,
               rated.amount_micros::NUMERIC AS amount_micros
        FROM rated_usage rated
        JOIN economic_metering_events meter
          ON meter.meter_event_id = rated.meter_event_id
        JOIN selected_jobs job ON job.job_id = rated.job_id
        WHERE rated.created_at_ms >= $1
          AND rated.created_at_ms < $2
          AND job.economics_contract_version IN (1, 2, 3)

        UNION ALL

        SELECT job.*, rated_line.created_at_ms AS event_at_ms,
               quote_line.metric AS billing_metric,
               quote_line.unit AS billing_unit,
               quote_line.terminal_outcome AS outcome,
               rated.currency,
               rated_line.actual_quantity::NUMERIC AS quantity,
               rated_line.amount_micros::NUMERIC AS amount_micros
        FROM customer_rated_usage_lines rated_line
        JOIN customer_rated_usage rated
          ON rated.rated_usage_id = rated_line.rated_usage_id
        JOIN customer_price_quote_lines quote_line
          ON quote_line.quote_line_id = rated_line.quote_line_id
         AND quote_line.quote_id = rated.quote_id
         AND quote_line.job_id = rated.job_id
        JOIN selected_jobs job ON job.job_id = rated.job_id
        WHERE rated_line.created_at_ms >= $1
          AND rated_line.created_at_ms < $2
          AND job.economics_contract_version = 4
    )
    SELECT (event_at_ms / $12) * $12 AS bucket_start_ms,
           $13::TEXT AS group_kind,
           CASE $13
             WHEN 'none' THEN 'all'
             WHEN 'line_item' THEN billing_metric
             WHEN 'project' THEN COALESCE(project_id, 'unattributed')
             WHEN 'api_key' THEN COALESCE(api_key_id, 'session')
             WHEN 'user' THEN COALESCE(actor_user_id::TEXT, 'service_account')
             WHEN 'provider' THEN provider_id
             WHEN 'model' THEN model
             WHEN 'operation' THEN operation
             WHEN 'service_tier' THEN effective_service_tier
           END AS group_value,
           CASE $13
             WHEN 'none' THEN '全部'
             WHEN 'line_item' THEN billing_metric
             WHEN 'project' THEN project_label
             WHEN 'api_key' THEN api_key_label
             WHEN 'user' THEN user_label
             WHEN 'provider' THEN provider_id
             WHEN 'model' THEN model
             WHEN 'operation' THEN operation
             WHEN 'service_tier' THEN service_tier_label
           END AS group_label,
           billing_metric, billing_unit, outcome, currency,
           SUM(quantity)::TEXT AS quantity,
           SUM(amount_micros)::TEXT AS amount_micros
    FROM rating_lines
    GROUP BY bucket_start_ms, group_value, group_label,
             billing_metric, billing_unit, outcome, currency
    ORDER BY bucket_start_ms, group_label, billing_metric,
             billing_unit, outcome, currency
    LIMIT 5001
    "#
    )
}

const FILTER_OPTIONS_SQL: &str = r#"
    WITH candidate_job_ids AS (
        SELECT job_id
        FROM jobs
        WHERE created_at_ms >= $1 AND created_at_ms < $2

        UNION

        SELECT job_id
        FROM usage_events
        WHERE job_id IS NOT NULL
          AND created_at_ms >= $1 AND created_at_ms < $2

        UNION

        SELECT job_id
        FROM rated_usage
        WHERE created_at_ms >= $1 AND created_at_ms < $2

        UNION

        SELECT job_id
        FROM customer_rated_usage_lines
        WHERE created_at_ms >= $1 AND created_at_ms < $2
    ),
    visible_jobs AS (
        SELECT job.job_id, job.provider_id, job.model, job.operation,
               COALESCE(tier.effective_service_tier, 'default')
                   AS effective_service_tier,
               CASE COALESCE(tier.effective_service_tier, 'default')
                 WHEN 'default' THEN 'Default'
                 WHEN 'flex' THEN 'Flex'
                 WHEN 'priority' THEN 'Priority'
               END AS service_tier_label,
               attribution.project_id, attribution.api_key_id,
               COALESCE(
                   attribution.actor_user_id,
                   attribution.credential_owner_user_id
               ) AS actor_user_id,
               COALESCE(project.name, attribution.project_id) AS project_label,
               COALESCE(api_key.name, attribution.api_key_id) AS api_key_label,
               COALESCE(identity_user.display_name,
                        attribution.actor_user_id::TEXT,
                        attribution.credential_owner_user_id::TEXT) AS user_label
        FROM jobs job
        LEFT JOIN job_auth_attributions attribution
          ON attribution.job_id = job.job_id
        LEFT JOIN gateway_projects project
          ON project.id = attribution.project_id
         AND project.tenant_id = job.tenant_id
        LEFT JOIN gateway_api_keys api_key
          ON api_key.id = attribution.api_key_id
        LEFT JOIN identity_users identity_user
          ON identity_user.user_id = COALESCE(
              attribution.actor_user_id,
              attribution.credential_owner_user_id
          )
        LEFT JOIN job_service_tier_decisions tier
          ON tier.job_id = job.job_id
        JOIN candidate_job_ids candidate ON candidate.job_id = job.job_id
        WHERE ($3::TEXT[] IS NULL OR job.tenant_id = ANY($3))
          AND (
            $4::UUID IS NULL
            OR attribution.actor_user_id = $4
            OR attribution.credential_owner_user_id = $4
          )
          AND ($5::TEXT IS NULL OR attribution.project_id = $5)
    ),
    options AS (
        SELECT 'project'::TEXT AS kind, project_id AS value,
               project_label AS label
        FROM visible_jobs
        WHERE project_id IS NOT NULL

        UNION

        SELECT 'api_key', api_key_id, api_key_label
        FROM visible_jobs
        WHERE api_key_id IS NOT NULL

        UNION

        SELECT 'user', actor_user_id::TEXT, user_label
        FROM visible_jobs
        WHERE $6::BOOLEAN
          AND actor_user_id IS NOT NULL

        UNION

        SELECT 'provider', provider_id, provider_id
        FROM visible_jobs

        UNION

        SELECT 'model', model, model
        FROM visible_jobs

        UNION

        SELECT 'operation', operation, operation
        FROM visible_jobs

        UNION

        SELECT 'service_tier', effective_service_tier, service_tier_label
        FROM visible_jobs
    )
    , ranked_options AS (
        SELECT kind, value, label,
               ROW_NUMBER() OVER (
                   PARTITION BY kind ORDER BY label, value
               ) AS option_rank
        FROM options
        WHERE value IS NOT NULL AND label IS NOT NULL
    )
    SELECT kind, value, label
    FROM ranked_options
    WHERE option_rank <= 200
    ORDER BY kind, label, value
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_read::UsageGroupBy;

    fn query(window_ms: i64, interval: UsageInterval) -> UsageAnalysisQuery {
        UsageAnalysisQuery {
            window_ms,
            interval,
            group_by: UsageGroupBy::LineItem,
            project_id: None,
            api_key_id: None,
            filter_user_id: None,
            provider_id: None,
            model: None,
            operation: None,
            service_tier: None,
        }
    }

    #[test]
    fn minute_interval_requires_a_short_window() {
        assert!(validate_query(&query(24 * 60 * 60 * 1_000, UsageInterval::Minute)).is_ok());
        assert!(validate_query(&query(7 * 24 * 60 * 60 * 1_000, UsageInterval::Minute)).is_err());
    }

    #[test]
    fn interval_cannot_exceed_window() {
        assert!(validate_query(&query(60 * 60 * 1_000, UsageInterval::Hour)).is_ok());
        assert!(validate_query(&query(60 * 1_000, UsageInterval::Hour)).is_err());
    }
}
