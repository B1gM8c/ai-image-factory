use std::{env, time::Duration};

use gpt_image_2_gateway::database::{connect_test_pool_with_search_path, run_migrations};
use sqlx::{AssertSqlSafe, PgPool};
use tokio::time::timeout;
use uuid::Uuid;

use super::{TestResult, require};

const DATABASE_ENV: &str = "TEST_DATABASE_URL";

pub(crate) struct TestDatabase {
    database_url: String,
    schema: String,
    pool: PgPool,
}

impl TestDatabase {
    pub(crate) async fn new() -> TestResult<Option<Self>> {
        let Some(database_url) = env::var(DATABASE_ENV)
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err(format!("{DATABASE_ENV} must be set when CI is present"));
            }
            eprintln!("skipping process smoke test: {DATABASE_ENV} is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_process_smoke_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, 4, &schema)
            .await
            .map_err(|error| format!("test database should be reachable: {error:?}"))?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(|error| format!("failed to identify test database: {error}"))?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!(
                "refusing schema DDL because current_database() is {database_name:?}, which does not contain 'test'"
            ));
        }

        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(|error| format!("failed to create isolated schema {schema}: {error}"))?;
        let setup = async {
            let current_schema: String = sqlx::query_scalar("SELECT current_schema()")
                .fetch_one(&pool)
                .await
                .map_err(|error| format!("failed to inspect current schema: {error}"))?;
            require(
                current_schema == schema,
                format!(
                    "database helper resolved search_path to {current_schema:?}, expected {schema:?}"
                ),
            )?;
            run_migrations(&pool)
                .await
                .map_err(|error| format!("failed to migrate isolated schema: {error:?}"))
        }
        .await;
        if let Err(error) = setup {
            let _ = drop_schema(&pool, &schema).await;
            pool.close().await;
            return Err(error);
        }

        Ok(Some(Self {
            database_url,
            schema,
            pool,
        }))
    }

    pub(crate) fn database_url(&self) -> &str {
        &self.database_url
    }

    pub(crate) fn schema(&self) -> &str {
        &self.schema
    }

    pub(crate) async fn wait_for_generation_work_count(&self, expected: i64) -> TestResult {
        timeout(Duration::from_secs(5), async {
            loop {
                let count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM work_items WHERE state IN ('ready', 'leased', 'running')",
                )
                .fetch_one(&self.pool)
                .await
                .map_err(|error| format!("failed to count generation work: {error}"))?;
                if count >= expected {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| format!("durable queue did not reach {expected} active work items"))?
    }

    pub(crate) async fn assert_transitions(&self, request_id: &str) -> TestResult {
        let transition: TransitionRow = sqlx::query_as(
            r#"
            SELECT j.job_id, qr.reservation_id,
                   j.state AS job_state,
                   j.requested_units AS job_requested_units,
                   j.charged_units,
                   j.finished_at_ms IS NOT NULL AS job_finished,
                   qr.state AS reservation_state,
                   qr.requested_units AS reservation_requested_units,
                   qr.committed_units,
                   qr.released_units
            FROM jobs j
            JOIN quota_reservations qr
              ON qr.reservation_id = j.reservation_id
             AND qr.job_id = j.job_id
             AND qr.tenant_id = j.tenant_id
            WHERE j.request_id = $1 AND j.tenant_id = 'tenant_default'
            "#,
        )
        .bind(request_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| format!("failed to read job and reservation transition: {error}"))?;
        require(
            transition.job_state == "succeeded"
                && transition.job_requested_units == 1
                && transition.charged_units == 1
                && transition.job_finished
                && transition.reservation_state == "committed"
                && transition.reservation_requested_units == 1
                && transition.committed_units == 1
                && transition.released_units == 0,
            format!("unexpected succeeded/committed transition: {transition:?}"),
        )?;

        let charged: (i64, i64, Option<String>, Option<String>) = sqlx::query_as(
            r#"
            SELECT COUNT(*), COALESCE(SUM(units), 0)::BIGINT, MIN(outcome), MIN(operation)
            FROM usage_events
            WHERE request_id = $1 AND tenant_id = 'tenant_default'
            "#,
        )
        .bind(request_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| format!("failed to read charged usage transition: {error}"))?;
        require(
            charged
                == (
                    1,
                    1,
                    Some("charged".to_string()),
                    Some("generation".to_string()),
                ),
            format!("unexpected charged usage transition: {charged:?}"),
        )?;

        let metering: Vec<MeteringRow> = sqlx::query_as(
            r#"
            SELECT me.event_type, me.units, me.outcome, me.job_id, me.reservation_id
            FROM metering_events me
            JOIN jobs j
              ON j.job_id = me.job_id
             AND j.tenant_id = me.tenant_id
             AND j.request_id = me.request_id
            JOIN quota_reservations qr
              ON qr.reservation_id = me.reservation_id
             AND qr.job_id = j.job_id
             AND qr.tenant_id = me.tenant_id
             AND qr.request_id = me.request_id
            WHERE me.request_id = $1
              AND me.tenant_id = 'tenant_default'
              AND j.job_id = $2
              AND qr.reservation_id = $3
            ORDER BY me.event_type
            "#,
        )
        .bind(request_id)
        .bind(transition.job_id)
        .bind(transition.reservation_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| format!("failed to read metering transitions: {error}"))?;
        let expected = vec![
            MeteringRow::expected(
                "job_succeeded",
                "succeeded",
                transition.job_id,
                transition.reservation_id,
            ),
            MeteringRow::expected(
                "quota_committed",
                "succeeded",
                transition.job_id,
                transition.reservation_id,
            ),
            MeteringRow::expected(
                "quota_reserved",
                "reserved",
                transition.job_id,
                transition.reservation_id,
            ),
        ];
        require(
            metering == expected,
            format!("unexpected authoritative metering transitions: {metering:?}"),
        )?;

        let durable: DurableTransitionRow = sqlx::query_as(
            r#"
            SELECT a.state AS admission_state,
                   p.command_schema,
                   p.command_json ->> 'model' AS command_model,
                   p.command_json ->> 'source_api_profile' AS source_api_profile,
                   w.state AS work_state,
                   w.lease_epoch,
                   w.execution_id IS NOT NULL AS has_execution_id,
                   ja.state AS attempt_state,
                   ja.worker_id,
                   i.state AS idempotency_state,
                   (SELECT COUNT(*) FROM job_response_projections rp
                    WHERE rp.job_id = a.job_id) AS projection_count,
                   (SELECT COUNT(*) FROM artifacts ar
                    WHERE ar.job_id = a.job_id AND ar.state = 'ready') AS artifact_count
            FROM admission_sessions a
            JOIN idempotency_requests i ON i.session_id = a.session_id
            JOIN job_payloads p ON p.admission_session_id = a.session_id
            JOIN work_items w ON w.job_id = p.job_id
            JOIN job_attempts ja
              ON ja.work_item_id = w.work_item_id
             AND ja.execution_id = w.execution_id
             AND ja.lease_epoch = w.lease_epoch
            WHERE a.job_id = $1
            "#,
        )
        .bind(transition.job_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| format!("failed to read durable execution transition: {error}"))?;
        require(
            durable.admission_state == "attached"
                && durable.command_schema == "openai.images.generation.v1"
                && durable.command_model.as_deref() == Some("gpt-image-2")
                && durable.source_api_profile.as_deref() == Some("openai-images-v1")
                && durable.work_state == "succeeded"
                && durable.lease_epoch == 1
                && durable.has_execution_id
                && durable.attempt_state == "succeeded"
                && durable.worker_id == "process-smoke-workerd"
                && durable.idempotency_state == "succeeded"
                && durable.projection_count == 1
                && durable.artifact_count == 1,
            format!("unexpected durable execution transition: {durable:?}"),
        )?;

        let event_types: Vec<String> = sqlx::query_scalar(
            "SELECT event_type FROM job_events WHERE job_id = $1 ORDER BY event_type",
        )
        .bind(transition.job_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| format!("failed to read durable job events: {error}"))?;
        let outbox_types: Vec<String> = sqlx::query_scalar(
            "SELECT event_type FROM outbox_events WHERE job_id = $1 ORDER BY event_type",
        )
        .bind(transition.job_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| format!("failed to read durable outbox events: {error}"))?;
        let expected_events = vec!["job.accepted".to_string(), "job.succeeded".to_string()];
        require(
            event_types == expected_events && outbox_types == expected_events,
            format!("unexpected durable events: job={event_types:?}, outbox={outbox_types:?}"),
        )
    }

    pub(crate) async fn assert_edit_transitions(&self, request_id: &str) -> TestResult {
        let row: EditTransitionRow = sqlx::query_as(
            r#"
            SELECT j.state AS job_state, j.operation, qr.state AS quota_state,
                   a.state AS admission_state, p.command_schema,
                   w.state AS work_state, ja.state AS attempt_state,
                   COALESCE(i.state, '') AS idempotency_state,
                   rp.operation AS projection_operation,
                   (SELECT COUNT(*) FROM job_input_manifests m WHERE m.job_id = j.job_id) AS manifest_count,
                   (SELECT COUNT(*) FROM job_input_objects o WHERE o.job_id = j.job_id) AS input_count,
                   (SELECT COUNT(*) FROM artifacts ar WHERE ar.job_id = j.job_id AND ar.state = 'ready') AS artifact_count,
                   (SELECT COUNT(*) FROM usage_events ue
                    WHERE ue.request_id = j.request_id AND ue.operation = 'edit' AND ue.outcome = 'charged') AS charge_count
            FROM jobs j
            JOIN quota_reservations qr ON qr.reservation_id = j.reservation_id AND qr.job_id = j.job_id
            JOIN admission_sessions a ON a.job_id = j.job_id
            JOIN job_payloads p ON p.job_id = j.job_id
            JOIN work_items w ON w.job_id = j.job_id
            JOIN job_attempts ja ON ja.work_item_id = w.work_item_id AND ja.execution_id = w.execution_id
            LEFT JOIN idempotency_requests i ON i.job_id = j.job_id
            JOIN job_response_projections rp ON rp.job_id = j.job_id
            WHERE j.request_id = $1
            "#,
        )
        .bind(request_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| format!("failed to read edit durable transition: {error}"))?;
        require(
            row.job_state == "succeeded"
                && row.operation == "edit"
                && row.quota_state == "committed"
                && row.admission_state == "attached"
                && row.command_schema == "openai.images.edit.v1"
                && row.work_state == "succeeded"
                && row.attempt_state == "succeeded"
                && row.idempotency_state == "succeeded"
                && row.projection_operation == "edit"
                && row.manifest_count == 1
                && row.input_count == 1
                && row.artifact_count == 1
                && row.charge_count == 1,
            format!("unexpected durable edit transition: {row:?}"),
        )
    }

    pub(crate) async fn cleanup(self) -> TestResult {
        let result = timeout(
            Duration::from_secs(5),
            drop_schema(&self.pool, &self.schema),
        )
        .await
        .map_err(|_| format!("timed out cleaning isolated schema {}", self.schema))?;
        self.pool.close().await;
        result
    }
}

async fn drop_schema(pool: &PgPool, schema: &str) -> TestResult {
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|error| format!("failed to clean isolated schema {schema}: {error}"))
}

#[derive(Debug, sqlx::FromRow)]
struct TransitionRow {
    job_id: Uuid,
    reservation_id: Uuid,
    job_state: String,
    job_requested_units: i32,
    charged_units: i32,
    job_finished: bool,
    reservation_state: String,
    reservation_requested_units: i32,
    committed_units: i32,
    released_units: i32,
}

#[derive(Debug, sqlx::FromRow)]
struct EditTransitionRow {
    job_state: String,
    operation: String,
    quota_state: String,
    admission_state: String,
    command_schema: String,
    work_state: String,
    attempt_state: String,
    idempotency_state: String,
    projection_operation: String,
    manifest_count: i64,
    input_count: i64,
    artifact_count: i64,
    charge_count: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct DurableTransitionRow {
    admission_state: String,
    command_schema: String,
    command_model: Option<String>,
    source_api_profile: Option<String>,
    work_state: String,
    lease_epoch: i64,
    has_execution_id: bool,
    attempt_state: String,
    worker_id: String,
    idempotency_state: String,
    projection_count: i64,
    artifact_count: i64,
}

#[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
struct MeteringRow {
    event_type: String,
    units: i32,
    outcome: String,
    job_id: Option<Uuid>,
    reservation_id: Option<Uuid>,
}

impl MeteringRow {
    fn expected(event_type: &str, outcome: &str, job_id: Uuid, reservation_id: Uuid) -> Self {
        Self {
            event_type: event_type.to_string(),
            units: 1,
            outcome: outcome.to_string(),
            job_id: Some(job_id),
            reservation_id: Some(reservation_id),
        }
    }
}
