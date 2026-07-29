use std::{env, sync::Arc};

use factory_identity::{AccessTokenCodec, AuthPolicy, IdentityService, RefreshTokenKeyring};
use gpt_image_2_gateway::{
    database::{connect_test_pool_with_search_path, run_migrations},
    identity::PostgresIdentityRepository,
    project_limits::{
        PostgresProjectSpendBudgetService, ProjectSpendBudgetService,
        UpdateProjectSpendBudgetRequest,
    },
};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const PRIVATE_KEY: &[u8] = br#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgJ6r5c63M0tPZV05C
Y0U72GBHm9iqV7QaUgFxk/9dBn+hRANCAAT5ufmoZxTrAkeOwJFSjVcbQ1Pvl2sw
892/nV1rvRJwDokKy+s00P46StleDgXLe9hOly8yM81frZfcMeI1krz+
-----END PRIVATE KEY-----
"#;
const PUBLIC_KEY: &[u8] = br#"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE+bn5qGcU6wJHjsCRUo1XG0NT75dr
MPPdv51da70ScA6JCsvrNND+OkrZXg4Fy3vYTpcvMjPNX62X3DHiNZK8/g==
-----END PUBLIC KEY-----
"#;
const PASSWORD: &str = "correct horse battery staple";

#[tokio::test]
async fn project_spend_budgets_are_versioned_and_queue_evaluation_is_single_claim() -> TestResult {
    let Some(schema) = TestSchema::new(8).await? else {
        return Ok(());
    };
    let result = project_budget_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

async fn project_budget_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("project limit migrations failed: {error:?}"))?;
    let identity = identity_service(pool)?;
    identity
        .bootstrap_admin(
            "project-owner@limits.test".to_string(),
            "Project Limits Owner".to_string(),
            PASSWORD.to_string(),
        )
        .await
        .map_err(debug_error)?;
    let owner = identity
        .list_users(None, 10)
        .await
        .map_err(debug_error)?
        .into_iter()
        .find(|user| user.email == "project-owner@limits.test")
        .ok_or_else(|| "bootstrapped project owner is missing".to_string())?;
    let project = owner
        .projects
        .first()
        .ok_or_else(|| "bootstrapped owner has no default project".to_string())?;
    let service = PostgresProjectSpendBudgetService::new(pool.clone());

    let empty = service
        .get_budget(&project.project_id)
        .await
        .map_err(debug_error)?;
    require(
        !empty.configured
            && empty.control_version == "0"
            && empty.spend_micros == "0"
            && empty.limit_type == gpt_image_2_gateway::project_limits::ProjectSpendLimitType::Soft
            && empty.period_kind == "calendar_month_utc",
        "fresh project invented a spend budget",
    )?;

    let created = service
        .update_budget(
            &project.project_id,
            owner.user_id,
            budget_request("USD", "1000000", vec![90, 50, 90], "0"),
        )
        .await
        .map_err(debug_error)?;
    require(
        created.configured
            && created.control_version == "1"
            && created.alert_thresholds == [50, 90, 100]
            && created.spend_micros == "0"
            && created.reserved_micros == "0"
            && created.alert_events.is_empty(),
        "created budget did not preserve normalized soft-limit controls",
    )?;

    let stale = service
        .update_budget(
            &project.project_id,
            owner.user_id,
            budget_request("USD", "2000000", vec![75], "0"),
        )
        .await
        .expect_err("stale control version must conflict");
    require(
        stale.status_code().as_u16() == 409,
        "stale budget update did not fail with conflict",
    )?;

    let updated = service
        .update_budget(
            &project.project_id,
            owner.user_id,
            budget_request("usd", "2000000", vec![75], "1"),
        )
        .await
        .map_err(debug_error)?;
    require(
        updated.control_version == "2"
            && updated.currency.as_deref() == Some("USD")
            && updated.alert_thresholds == [75, 100],
        "budget update did not advance its immutable control version",
    )?;

    let first_service = service.clone();
    let second_service = service.clone();
    let first_project = project.project_id.clone();
    let second_project = project.project_id.clone();
    let owner_user_id = owner.user_id;
    let first = tokio::spawn(async move {
        first_service
            .update_budget(
                &first_project,
                owner_user_id,
                budget_request("USD", "3000000", vec![60], "2"),
            )
            .await
    });
    let second = tokio::spawn(async move {
        second_service
            .update_budget(
                &second_project,
                owner_user_id,
                budget_request("USD", "4000000", vec![80], "2"),
            )
            .await
    });
    let first = first.await.map_err(debug_error)?;
    let second = second.await.map_err(debug_error)?;
    require(
        matches!(
            (&first, &second),
            (Ok(_), Err(error)) | (Err(error), Ok(_))
                if error.status_code().as_u16() == 409
        ),
        "two concurrent updates with one control version both committed",
    )?;
    let current_after_concurrency = service
        .get_budget(&project.project_id)
        .await
        .map_err(debug_error)?;
    let hard = service
        .update_budget(
            &project.project_id,
            owner.user_id,
            hard_budget_request(
                "USD",
                "5000000",
                vec![80],
                &current_after_concurrency.control_version,
            ),
        )
        .await
        .map_err(debug_error)?;
    require(
        hard.limit_type == gpt_image_2_gateway::project_limits::ProjectSpendLimitType::Hard
            && hard.reserved_micros == "0",
        "project hard limit was not persisted or projected",
    )?;

    sqlx::query(
        "INSERT INTO project_spend_evaluation_queue(project_id, requested_at_ms) VALUES ($1, 1)",
    )
    .bind(&project.project_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let first_evaluator = service.clone();
    let second_evaluator = service.clone();
    let (first_claim, second_claim) = tokio::join!(
        first_evaluator.evaluate_pending(100),
        second_evaluator.evaluate_pending(100)
    );
    let claimed = first_claim.map_err(debug_error)? + second_claim.map_err(debug_error)?;
    require(
        claimed == 1,
        "one queued project was evaluated more than once",
    )?;
    let queue_size: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_spend_evaluation_queue")
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_spend_alert_events")
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
    require(
        queue_size == 0 && event_count == 0,
        "zero spend either left the queue stuck or emitted a false alert",
    )?;

    let organization_owner = identity
        .create_member_user(
            "organization-owner@limits.test".to_string(),
            "Project Limits Organization Owner".to_string(),
            PASSWORD.to_string(),
        )
        .await
        .map_err(debug_error)?;
    let outsider = identity
        .create_member_user(
            "outsider@limits.test".to_string(),
            "Project Limits Outsider".to_string(),
            PASSWORD.to_string(),
        )
        .await
        .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO identity_organization_memberships(
            organization_id, user_id, role, state, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 'owner', 'active', 10, 10)
        "#,
    )
    .bind(&project.organization_id)
    .bind(organization_owner.user_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let current = service
        .get_budget(&project.project_id)
        .await
        .map_err(debug_error)?;
    let event_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO project_spend_alert_events(
            event_id, project_id, organization_id, currency,
            period_start_ms, period_end_ms, threshold_percent,
            budget_control_version, monthly_budget_micros, spend_micros,
            created_at_ms
        )
        VALUES ($1, $2, $3, $4, $5, $6, 100, $7, $8, $8, 11)
        "#,
    )
    .bind(event_id)
    .bind(&project.project_id)
    .bind(&project.organization_id)
    .bind(
        current
            .currency
            .as_deref()
            .ok_or_else(|| "configured budget is missing currency".to_string())?,
    )
    .bind(current.period_start_ms)
    .bind(current.period_end_ms)
    .bind(
        current
            .control_version
            .parse::<i64>()
            .map_err(debug_error)?,
    )
    .bind(
        current
            .monthly_budget_micros
            .as_deref()
            .ok_or_else(|| "configured budget is missing amount".to_string())?
            .parse::<i64>()
            .map_err(debug_error)?,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    service
        .get_budget(&project.project_id)
        .await
        .map_err(debug_error)?;

    let owner_notifications = service
        .list_notifications(owner.user_id, 20)
        .await
        .map_err(debug_error)?;
    let organization_owner_notifications = service
        .list_notifications(organization_owner.user_id, 20)
        .await
        .map_err(debug_error)?;
    let outsider_notifications = service
        .list_notifications(outsider.user_id, 20)
        .await
        .map_err(debug_error)?;
    require(
        owner_notifications.unread_count == 1
            && owner_notifications.data.len() == 1
            && organization_owner_notifications.unread_count == 1
            && organization_owner_notifications.data.len() == 1
            && outsider_notifications.unread_count == 0
            && outsider_notifications.data.is_empty(),
        "notification recipients were duplicated or leaked across user boundaries",
    )?;
    let owner_delivery = owner_notifications.data[0].delivery_id;
    let organization_owner_delivery = organization_owner_notifications.data[0].delivery_id;
    let cross_user = service
        .mark_notification_read(outsider.user_id, owner_delivery)
        .await
        .expect_err("another user must not mark a notification read");
    require(
        cross_user.status_code().as_u16() == 404,
        "recipient isolation did not hide another user's notification",
    )?;

    let owner_read = service
        .mark_notification_read(owner.user_id, owner_delivery)
        .await
        .map_err(debug_error)?;
    let pending_state: String = sqlx::query_scalar(
        "SELECT notification_state FROM project_spend_alert_events WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        owner_read.read_at_ms.is_some() && pending_state == "pending",
        "one recipient acknowledgement prematurely acknowledged the shared event",
    )?;
    service
        .mark_notification_read(organization_owner.user_id, organization_owner_delivery)
        .await
        .map_err(debug_error)?;
    let acknowledged_state: String = sqlx::query_scalar(
        "SELECT notification_state FROM project_spend_alert_events WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let pending_delivery_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_spend_notification_deliveries WHERE state = 'pending'",
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        acknowledged_state == "acknowledged" && pending_delivery_count == 0,
        "completed in-app deliveries stayed pending or left the event unacknowledged",
    )?;

    let missing = service
        .get_budget("project-not-visible")
        .await
        .expect_err("unknown project must not expose budget state");
    require(
        missing.status_code().as_u16() == 404,
        "unknown project did not use not-found isolation",
    )
}

fn identity_service(pool: &PgPool) -> TestResult<Arc<IdentityService>> {
    let policy = AuthPolicy::default();
    let access_tokens = AccessTokenCodec::new(
        "project-limits-test",
        PRIVATE_KEY,
        [("project-limits-test".to_string(), PUBLIC_KEY.to_vec())],
        "https://identity.project-limits.test",
        "urn:aif:admin",
        &policy,
    )
    .map_err(debug_error)?;
    let refresh_tokens = RefreshTokenKeyring::new(1, [(1, vec![0x5a; 32])]).map_err(debug_error)?;
    IdentityService::new(
        Arc::new(PostgresIdentityRepository::new(pool.clone())),
        access_tokens,
        refresh_tokens,
        policy,
    )
    .map(Arc::new)
    .map_err(debug_error)
}

fn budget_request(
    currency: &str,
    monthly_budget_micros: &str,
    alert_thresholds: Vec<i16>,
    expected_control_version: &str,
) -> UpdateProjectSpendBudgetRequest {
    UpdateProjectSpendBudgetRequest {
        currency: currency.to_string(),
        monthly_budget_micros: monthly_budget_micros.to_string(),
        limit_type: gpt_image_2_gateway::project_limits::ProjectSpendLimitType::Soft,
        alert_thresholds,
        expected_control_version: expected_control_version.to_string(),
    }
}

fn hard_budget_request(
    currency: &str,
    monthly_budget_micros: &str,
    alert_thresholds: Vec<i16>,
    expected_control_version: &str,
) -> UpdateProjectSpendBudgetRequest {
    UpdateProjectSpendBudgetRequest {
        currency: currency.to_string(),
        monthly_budget_micros: monthly_budget_micros.to_string(),
        limit_type: gpt_image_2_gateway::project_limits::ProjectSpendLimitType::Hard,
        alert_thresholds,
        expected_control_version: expected_control_version.to_string(),
    }
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
            eprintln!("skipping PostgreSQL project limits test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("project_limits_test_{}", Uuid::new_v4().simple());
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
