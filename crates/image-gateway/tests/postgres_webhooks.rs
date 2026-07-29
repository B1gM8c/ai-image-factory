use std::{env, time::Duration};

use gpt_image_2_gateway::{
    database::{connect_test_pool_with_search_path, run_migrations},
    webhooks::{
        CreateProjectWebhookRequest, PostgresProjectWebhookService, PostgresWebhookRelay,
        ProjectWebhookService, UpdateProjectWebhookRequest, WebhookAttemptResult,
        WebhookDeliveryState, WebhookDestinationPolicy, WebhookEndpointState,
        WebhookSigningKeyring,
    },
};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn project_webhooks_preserve_tenant_secret_and_delivery_invariants() -> TestResult {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let Some(schema) = TestSchema::new(8).await? else {
        return Ok(());
    };
    let result = webhook_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

async fn webhook_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("webhook migrations failed: {error:?}"))?;
    let first = seed_project(pool, "first").await?;
    let second = seed_project(pool, "second").await?;
    let keyring = WebhookSigningKeyring::new(1, [(1, vec![0x42; 32])]).map_err(debug_error)?;
    let service = PostgresProjectWebhookService::new(
        pool.clone(),
        keyring,
        WebhookDestinationPolicy::permissive_for_tests(),
    );
    let relay = PostgresWebhookRelay::new(pool.clone());

    let created = service
        .create_endpoint(
            &first.project_id,
            first.user_id,
            create_request("http://127.0.0.1:19001/webhook"),
        )
        .await
        .map_err(debug_error)?;
    require(
        created.signing_secret.starts_with("whsec_"),
        "created endpoint did not return its one-time signing secret",
    )?;
    let forbidden_secret_columns: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'project_webhook_endpoints'
          AND column_name IN ('signing_secret', 'secret', 'secret_hash', 'secret_ciphertext')
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        forbidden_secret_columns == 0,
        "webhook endpoint schema persisted secret material",
    )?;

    let other = service
        .create_endpoint(
            &second.project_id,
            second.user_id,
            create_request("http://127.0.0.1:19002/webhook"),
        )
        .await
        .map_err(debug_error)?;
    let first_list = service
        .list_endpoints(&first.project_id, None, 100)
        .await
        .map_err(debug_error)?;
    require(
        first_list.data.len() == 1
            && first_list.data[0].id == created.endpoint.id
            && first_list.data[0].project_id == first.project_id,
        "project endpoint listing crossed the project boundary",
    )?;
    let cross_project_deliveries = service
        .list_deliveries(&first.project_id, &other.endpoint.id, None, 100)
        .await
        .map_err(debug_error)?;
    require(
        cross_project_deliveries.data.is_empty(),
        "delivery listing crossed the project boundary",
    )?;

    let updated = service
        .update_endpoint(
            &first.project_id,
            &created.endpoint.id,
            first.user_id,
            UpdateProjectWebhookRequest {
                name: Some("Primary delivery endpoint".to_string()),
                url: created.endpoint.url.clone(),
                event_types: created.endpoint.event_types.clone(),
                state: WebhookEndpointState::Active,
                expected_control_version: created.endpoint.control_version,
            },
        )
        .await
        .map_err(debug_error)?;
    let stale_update = service
        .update_endpoint(
            &first.project_id,
            &created.endpoint.id,
            first.user_id,
            UpdateProjectWebhookRequest {
                name: Some("Stale writer".to_string()),
                url: updated.url.clone(),
                event_types: updated.event_types.clone(),
                state: WebhookEndpointState::Active,
                expected_control_version: created.endpoint.control_version,
            },
        )
        .await
        .expect_err("stale webhook update must fail");
    require(
        stale_update.status_code().as_u16() == 409,
        "stale webhook update did not use conflict semantics",
    )?;
    let rotated = service
        .rotate_secret(&first.project_id, &created.endpoint.id, first.user_id)
        .await
        .map_err(debug_error)?;
    require(
        rotated.signing_secret != created.signing_secret
            && rotated.secret_revision == created.endpoint.secret_revision + 1,
        "webhook secret rotation did not advance the derived secret revision",
    )?;

    let first_test = service
        .enqueue_test(&first.project_id, &created.endpoint.id, first.user_id)
        .await
        .map_err(debug_error)?;
    let first_lease = relay
        .claim_deliveries("worker-a", 1, Duration::from_secs(60))
        .await
        .map_err(|error| format!("first delivery claim failed: {error:?}"))?
        .pop()
        .ok_or_else(|| "test delivery was not claimable".to_string())?;
    require(
        first_lease.delivery_id == first_test.delivery_id,
        "claimed the wrong test delivery",
    )?;
    sqlx::query(
        "UPDATE project_webhook_deliveries SET lease_expires_at_ms = 0 WHERE delivery_id = $1",
    )
    .bind(first_lease.delivery_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let replacement_lease = relay
        .claim_deliveries("worker-b", 1, Duration::from_secs(60))
        .await
        .map_err(|error| format!("replacement delivery claim failed: {error:?}"))?
        .pop()
        .ok_or_else(|| "expired lease was not reclaimable".to_string())?;
    require(
        replacement_lease.lease_epoch == first_lease.lease_epoch + 1,
        "lease reclamation did not advance the fencing epoch",
    )?;
    let stale_finish = relay
        .finish_attempt(&first_lease, attempt(Some(200), None, None))
        .await
        .expect_err("superseded lease must not finalize");
    require(
        stale_finish.status_code().as_u16() == 409,
        "superseded webhook lease did not use conflict semantics",
    )?;
    relay
        .finish_attempt(
            &replacement_lease,
            attempt(Some(429), Some("http_429"), Some(2_000)),
        )
        .await
        .map_err(|error| format!("429 attempt finalization failed: {error:?}"))?;
    let retry_state: (String, i64, Option<i64>) = sqlx::query_as(
        r#"
        SELECT delivery.state, delivery.next_attempt_at_ms, runtime.paused_until_ms
        FROM project_webhook_deliveries delivery
        JOIN project_webhook_endpoint_runtime runtime
          ON runtime.endpoint_id = delivery.endpoint_id
        WHERE delivery.delivery_id = $1
        "#,
    )
    .bind(replacement_lease.delivery_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        retry_state.0 == "retry_wait"
            && retry_state.2.is_some_and(|paused| paused == retry_state.1),
        "HTTP 429 did not pause the endpoint until the delivery retry",
    )?;

    sqlx::query(
        r#"
        UPDATE project_webhook_endpoint_runtime
        SET paused_until_ms = NULL
        WHERE endpoint_id = $1
        "#,
    )
    .bind(&created.endpoint.id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE project_webhook_deliveries
        SET next_attempt_at_ms = 0
        WHERE delivery_id = $1
        "#,
    )
    .bind(replacement_lease.delivery_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let pending_test = service
        .enqueue_test(&first.project_id, &created.endpoint.id, first.user_id)
        .await
        .map_err(debug_error)?;
    let gone_lease = relay
        .claim_deliveries("worker-c", 1, Duration::from_secs(60))
        .await
        .map_err(|error| format!("410 delivery claim failed: {error:?}"))?
        .pop()
        .ok_or_else(|| "retry delivery was not claimable".to_string())?;
    require(
        gone_lease.delivery_id == replacement_lease.delivery_id,
        "retry ordering did not preserve the due delivery",
    )?;
    relay
        .finish_attempt(&gone_lease, attempt(Some(410), Some("http_410"), None))
        .await
        .map_err(|error| format!("410 attempt finalization failed: {error:?}"))?;
    let endpoint_state: String =
        sqlx::query_scalar("SELECT state FROM project_webhook_endpoints WHERE endpoint_id = $1")
            .bind(&created.endpoint.id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let pending_state: String =
        sqlx::query_scalar("SELECT state FROM project_webhook_deliveries WHERE delivery_id = $1")
            .bind(pending_test.delivery_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    require(
        endpoint_state == "disabled" && pending_state == "canceled",
        "HTTP 410 did not disable the endpoint and cancel queued deliveries",
    )?;

    let fanout_endpoint = service
        .create_endpoint(
            &first.project_id,
            first.user_id,
            create_request("http://127.0.0.1:19003/webhook"),
        )
        .await
        .map_err(debug_error)?;
    let outbox_event_id = seed_terminal_outbox(
        pool,
        &first.organization_id,
        &first.project_id,
        first.user_id,
        "generation",
        "job.succeeded",
    )
    .await?;
    require(
        relay
            .fan_out_once(100)
            .await
            .map_err(|error| format!("first outbox fanout failed: {error:?}"))?
            == 1
            && relay
                .fan_out_once(100)
                .await
                .map_err(|error| format!("repeat outbox fanout failed: {error:?}"))?
                == 0,
        "outbox fanout did not publish exactly once",
    )?;
    let materialized: (i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT COUNT(*) FROM project_webhook_events WHERE outbox_event_id = $1),
          (SELECT COUNT(*) FROM project_webhook_deliveries
           WHERE endpoint_id = $2 AND event_id = 'evt_' || replace($1::TEXT, '-', '')),
          (SELECT COUNT(*) FROM project_webhook_outbox_receipts
           WHERE outbox_event_id = $1),
          (SELECT COUNT(*) FROM outbox_events
           WHERE event_id = $1 AND published_at_ms IS NULL)
        "#,
    )
    .bind(outbox_event_id)
    .bind(&fanout_endpoint.endpoint.id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        materialized == (1, 1, 1, 1),
        "webhook fanout was not exactly-once or mutated the shared outbox marker",
    )?;
    let audit_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM identity_audit_events
        WHERE actor_user_id = $1
          AND action LIKE 'webhook.%'
        "#,
    )
    .bind(first.user_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        audit_count >= 6,
        "webhook control-plane mutations were not audit logged",
    )?;
    let delivery_list = service
        .list_deliveries(&first.project_id, &created.endpoint.id, None, 100)
        .await
        .map_err(debug_error)?;
    require(
        delivery_list
            .data
            .iter()
            .any(|delivery| delivery.state == WebhookDeliveryState::DeadLettered)
            && delivery_list
                .data
                .iter()
                .any(|delivery| delivery.state == WebhookDeliveryState::Canceled),
        "delivery read model did not expose terminal 410 outcomes",
    )
}

fn create_request(url: &str) -> CreateProjectWebhookRequest {
    CreateProjectWebhookRequest {
        name: None,
        url: url.to_string(),
        event_types: vec![
            "image.generation.completed".to_string(),
            "image.generation.failed".to_string(),
        ],
    }
}

fn attempt(
    http_status: Option<u16>,
    error_code: Option<&str>,
    retry_after_ms: Option<i64>,
) -> WebhookAttemptResult {
    WebhookAttemptResult {
        http_status,
        error_code: error_code.map(str::to_string),
        retry_after_ms,
        duration_ms: 5,
        webhook_timestamp: 1_700_000_000,
    }
}

struct SeededProject {
    user_id: Uuid,
    organization_id: String,
    project_id: String,
}

async fn seed_project(pool: &PgPool, suffix: &str) -> TestResult<SeededProject> {
    let user_id = Uuid::new_v4();
    let organization_id = format!("org_{}", user_id.simple());
    let project_id = format!("proj_{}", user_id.simple());
    let now = database_now(pool).await?;
    sqlx::query(
        r#"
        INSERT INTO identity_users
          (user_id, normalized_email, display_name, roles, scopes,
           authz_version, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, ARRAY['member'], ARRAY['console:access'], 1, $4, $4)
        "#,
    )
    .bind(user_id)
    .bind(format!("{suffix}-{}@webhook.test", user_id.simple()))
    .bind(format!("{suffix} webhook user"))
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(SeededProject {
        user_id,
        organization_id,
        project_id,
    })
}

async fn seed_terminal_outbox(
    pool: &PgPool,
    organization_id: &str,
    project_id: &str,
    actor_user_id: Uuid,
    operation: &str,
    event_type: &str,
) -> TestResult<Uuid> {
    let job_id = Uuid::new_v4();
    let event_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let route_id = Uuid::new_v4();
    let now = database_now(pool).await?;
    sqlx::query(
        r#"
        INSERT INTO jobs
          (job_id, tenant_id, request_id, operation, provider_id, model,
           state, requested_units, output_count, billable_units,
           billing_metric, billing_unit, economics_contract_version,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, 'provider-test', 'model-test',
                'succeeded', 1, 1, 1, 'output', 'output', 2, $5, $5)
        "#,
    )
    .bind(job_id)
    .bind(organization_id)
    .bind(format!("request-{}", job_id.simple()))
    .bind(operation)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO identity_session_families
          (session_id, user_id, client_id, authz_version_at_login,
           created_at_ms, last_seen_at_ms, idle_expires_at_ms,
           absolute_expires_at_ms)
        VALUES ($1, $2, 'webhook-test', 1, $3, $3, $4, $4)
        "#,
    )
    .bind(session_id)
    .bind(actor_user_id)
    .bind(now)
    .bind(now + 3_600_000)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_routes
          (route_id, revision, route_key, display_name, provider_id,
           operation_id, command_schema, route_kind, selection_strategy,
           state, created_at_ms)
        VALUES ($1, 1, $2, 'Webhook test route', 'provider-test',
                'images.generations', 'provider.command.v1', 'account',
                'quota_aware_least_loaded', 'enabled', $3)
        "#,
    )
    .bind(route_id)
    .bind(format!("webhook-test-{route_id}"))
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_auth_attributions
          (job_id, tenant_id, project_id, actor_user_id, actor_session_id,
           actor_authz_version, route_provider_id, route_operation_id,
           route_command_schema, route_id, route_revision, auth_kind,
           admitted_at_ms)
        VALUES ($1, $2, $3, $4, $5, 1, 'provider-test',
                'images.generations', 'provider.command.v1', $6, 1,
                'user_session', $7)
        "#,
    )
    .bind(job_id)
    .bind(organization_id)
    .bind(project_id)
    .bind(actor_user_id)
    .bind(session_id)
    .bind(route_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO outbox_events
          (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms)
        VALUES ($1, $2, $3, $4, '{}'::JSONB, $5)
        "#,
    )
    .bind(event_id)
    .bind(job_id)
    .bind(event_type)
    .bind(format!("terminal:{event_type}"))
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(event_id)
}

async fn database_now(pool: &PgPool) -> TestResult<i64> {
    sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(pool)
        .await
        .map_err(debug_error)
}

fn require(condition: bool, message: &str) -> TestResult {
    condition.then_some(()).ok_or_else(|| message.to_string())
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

struct TestSchema {
    name: String,
    pool: PgPool,
}

impl TestSchema {
    async fn new(max_connections: u32) -> TestResult<Option<Self>> {
        let Some(database_url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set in CI".to_string());
            }
            eprintln!("skipping PostgreSQL webhook test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("project_webhooks_test_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, max_connections, &name)
            .await
            .map_err(debug_error)?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(debug_error)?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!(
                "refusing schema DDL because database {database_name:?} is not a test database"
            ));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        Ok(Some(Self { name, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.name
        )))
        .execute(&self.pool)
        .await
        .map_err(debug_error);
        self.pool.close().await;
        result.map(|_| ())
    }
}
