use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use gpt_image_2_gateway::{
    EditJob, ExecutionContextStore, ExecutionSettlementStore, GeneratedImage, GenerationJob,
    ImageGatewayError, ImageGenerator, PostgresExecutionContextStore,
    PostgresExecutionSettlementStore, PostgresUsageStore, UsageCharge, UsageLimits, UsageStore,
    Workerd,
    admission::{
        AdmissionClaim, AdmissionStore, AttachJob, ClaimAdmission, GenerationCommandV1,
        PostgresAdmissionStore,
    },
    artifacts::InMemoryArtifactBlobStore,
    database::{connect_test_pool_with_search_path, run_migrations},
};
use image::{ImageBuffer, ImageFormat, Rgb};
use serde_json::to_value;
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn leased_work_reconstructs_generation_and_quota_context() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let request_id = format!("req_{}", Uuid::new_v4().simple());
        let tenant_id = format!("tenant_{}", Uuid::new_v4().simple());
        let job = generation_job(&request_id);
        let command =
            GenerationCommandV1::from_generation_job(&job, "openai-images-v1", "openai-codex");
        let request_hash = command.request_hash_hex();
        let usage = PostgresUsageStore::new(database.pool.clone());
        let reservation = usage
            .reserve(UsageCharge {
                tenant_id: tenant_id.clone(),
                request_id: request_id.clone(),
                operation: "generation",
                provider_id: "openai-codex".to_string(),
                model: "gpt-image-2".to_string(),
                units: 2,
                limits: UsageLimits {
                    five_hour_image_limit: 17,
                    seven_day_image_limit: 53,
                },
            })
            .await
            .map_err(|error| format!("reserve failed: {error:?}"))?;
        let admission = PostgresAdmissionStore::new(database.pool.clone());
        let claim = admission
            .claim(ClaimAdmission {
                tenant_id: tenant_id.clone(),
                project_id: "project-worker".to_string(),
                api_profile: "openai-images-v1".to_string(),
                operation: "generation".to_string(),
                request_id: request_id.clone(),
                idempotency_key_digest: Some("c".repeat(64)),
                request_hash: request_hash.clone(),
                deadline_at_ms: i64::MAX,
            })
            .await
            .map_err(|error| format!("claim failed: {error}"))?;
        let AdmissionClaim::Owner(ticket) = claim else {
            return Err(format!("unexpected admission claim: {claim:?}"));
        };
        admission
            .attach(AttachJob {
                ticket,
                job_id: reservation.job_id,
                command_schema: "openai.images.generation.v1".to_string(),
                command_json: to_value(&command).map_err(|error| error.to_string())?,
                work_kind: "image_batch".to_string(),
                schedule_scope: format!("tenant:{tenant_id}"),
                schedule_weight: 1,
                schedule_priority: 1,
                schedule_cost: 2,
            })
            .await
            .map_err(|error| format!("attach failed: {error}"))?;
        let lease = admission
            .claim_ready("workerd-test", 60_000)
            .await
            .map_err(|error| format!("work claim failed: {error}"))?
            .ok_or_else(|| "attached work was not claimable".to_string())?;
        let store = PostgresExecutionContextStore::new(database.pool.clone());
        let context = store
            .load_generation(&lease)
            .await
            .map_err(|error| format!("leased execution context load failed: {error:?}"))?;
        admission
            .start(&lease)
            .await
            .map_err(|error| format!("work start failed: {error}"))?;

        require(context.job.request_id == request_id, "request id changed")?;
        require(context.job.prompt == job.prompt, "prompt changed")?;
        require(context.job.n == 2, "output count changed")?;
        require(
            context.reservation.reservation_id == reservation.reservation_id
                && context.reservation.job_id == reservation.job_id,
            "reservation identity changed",
        )?;
        require(
            context.reservation.snapshot == reservation.snapshot,
            "quota snapshot changed",
        )?;
        require(
            context.reservation.charge.provider_id == "openai-codex"
                && context.reservation.charge.model == "gpt-image-2",
            "provider binding changed",
        )?;

        let mut forged = lease.clone();
        forged.execution_id = Uuid::new_v4();
        require(
            store.load_generation(&forged).await.is_err(),
            "forged execution identity loaded context",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn workerd_executes_ready_generation_without_gateway_memory() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let job = generation_job(&format!("req_{}", Uuid::new_v4().simple()));
        let admission = Arc::new(PostgresAdmissionStore::new(database.pool.clone()));
        let usage = Arc::new(PostgresUsageStore::new(database.pool.clone()));
        let reservation = prepare_ready_work(admission.as_ref(), usage.as_ref(), &job).await?;
        let artifacts = Arc::new(InMemoryArtifactBlobStore::default());
        let settlement = Arc::new(PostgresExecutionSettlementStore::new(
            database.pool.clone(),
            artifacts.clone(),
        ));
        let generator = CountingGenerator::new();
        let workerd = Workerd::new(
            "workerd-integration".to_string(),
            Arc::new(generator.clone()),
            admission,
            Arc::new(PostgresExecutionContextStore::new(database.pool.clone())),
            settlement.clone(),
            artifacts,
            Duration::from_secs(5),
        );

        let executed = workerd
            .run_once()
            .await
            .map_err(|error| format!("workerd execution failed: {error:?}"))?;
        require(
            executed == Some(reservation.job_id),
            "workerd executed the wrong job",
        )?;
        require(
            generator.calls.load(Ordering::SeqCst) == 1,
            "workerd invoked the provider more than once",
        )?;
        let result = settlement
            .load_generation_result(reservation.job_id)
            .await
            .map_err(|error| format!("result load failed: {error:?}"))?
            .ok_or_else(|| "workerd did not persist a result".to_string())?;
        require(
            result.images.len() == 2,
            "workerd persisted wrong output count",
        )?;
        require(
            workerd
                .run_once()
                .await
                .map_err(|error| format!("idle workerd failed: {error:?}"))?
                .is_none(),
            "completed work was claimed again",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn invalid_durable_context_is_failed_once_without_provider_execution() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let job = generation_job(&format!("req_{}", Uuid::new_v4().simple()));
        let admission = Arc::new(PostgresAdmissionStore::new(database.pool.clone()));
        let usage = Arc::new(PostgresUsageStore::new(database.pool.clone()));
        let reservation = prepare_ready_work(admission.as_ref(), usage.as_ref(), &job).await?;
        sqlx::query("UPDATE job_payloads SET request_hash = $2 WHERE job_id = $1")
            .bind(reservation.job_id)
            .bind("0".repeat(64))
            .execute(&database.pool)
            .await
            .map_err(|error| format!("failed to corrupt durable context: {error}"))?;
        let expired_job = generation_job(&format!("req_{}", Uuid::new_v4().simple()));
        let expired_reservation =
            prepare_ready_work(admission.as_ref(), usage.as_ref(), &expired_job).await?;
        sqlx::query(
            "UPDATE quota_reservations SET state = 'expired', expires_at_ms = 0 WHERE reservation_id = $1",
        )
            .bind(expired_reservation.reservation_id)
            .execute(&database.pool)
            .await
            .map_err(|error| format!("failed to expire durable context: {error}"))?;
        let artifacts = Arc::new(InMemoryArtifactBlobStore::default());
        let settlement = Arc::new(PostgresExecutionSettlementStore::new(
            database.pool.clone(),
            artifacts.clone(),
        ));
        let generator = CountingGenerator::new();
        let workerd = Workerd::new(
            "workerd-invalid-context".to_string(),
            Arc::new(generator.clone()),
            admission,
            Arc::new(PostgresExecutionContextStore::new(database.pool.clone())),
            settlement,
            artifacts,
            Duration::from_secs(5),
        );

        require(
            workerd.run_once().await.is_err(),
            "invalid context unexpectedly completed",
        )?;
        require(
            workerd.run_once().await.is_err(),
            "expired context unexpectedly completed",
        )?;
        require(
            generator.calls.load(Ordering::SeqCst) == 0,
            "provider ran for an invalid durable context",
        )?;
        let states: (String, String, String, i32, String) = sqlx::query_as(
            r#"
            SELECT w.state, a.state, qr.state, qr.released_units, j.state
            FROM work_items w
            JOIN job_attempts a ON a.work_item_id = w.work_item_id
            JOIN jobs j ON j.job_id = w.job_id
            JOIN quota_reservations qr ON qr.reservation_id = j.reservation_id
            WHERE w.job_id = $1
            "#,
        )
        .bind(reservation.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect invalid context state: {error}"))?;
        require(
            states
                == (
                    "failed".into(),
                    "failed".into(),
                    "released".into(),
                    2,
                    "failed".into(),
                ),
            format!("invalid context was not atomically terminalized: {states:?}"),
        )?;
        let expired_states: (String, String, String, i32, String) = sqlx::query_as(
            r#"
            SELECT w.state, a.state, qr.state, qr.released_units, j.state
            FROM work_items w
            JOIN job_attempts a ON a.work_item_id = w.work_item_id
            JOIN jobs j ON j.job_id = w.job_id
            JOIN quota_reservations qr ON qr.reservation_id = j.reservation_id
            WHERE w.job_id = $1
            "#,
        )
        .bind(expired_reservation.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect expired context state: {error}"))?;
        require(
            expired_states
                == (
                    "failed".into(),
                    "failed".into(),
                    "released".into(),
                    2,
                    "failed".into(),
                ),
            format!("expired context was not atomically terminalized: {expired_states:?}"),
        )?;
        require(
            workerd
                .run_once()
                .await
                .map_err(|error| format!("idle workerd failed: {error:?}"))?
                .is_none(),
            "invalid context was claimed again",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

async fn prepare_ready_work(
    admission: &PostgresAdmissionStore,
    usage: &PostgresUsageStore,
    job: &GenerationJob,
) -> TestResult<gpt_image_2_gateway::UsageReservation> {
    let tenant_id = format!("tenant_{}", Uuid::new_v4().simple());
    let command = GenerationCommandV1::from_generation_job(job, "openai-images-v1", "openai-codex");
    let request_hash = command.request_hash_hex();
    let reservation = usage
        .reserve(UsageCharge {
            tenant_id: tenant_id.clone(),
            request_id: job.request_id.clone(),
            operation: "generation",
            provider_id: "openai-codex".to_string(),
            model: "gpt-image-2".to_string(),
            units: job.n,
            limits: UsageLimits {
                five_hour_image_limit: 17,
                seven_day_image_limit: 53,
            },
        })
        .await
        .map_err(|error| format!("reserve failed: {error:?}"))?;
    let claim = admission
        .claim(ClaimAdmission {
            tenant_id: tenant_id.clone(),
            project_id: "project-workerd".to_string(),
            api_profile: "openai-images-v1".to_string(),
            operation: "generation".to_string(),
            request_id: job.request_id.clone(),
            idempotency_key_digest: Some(format!("{:064x}", Uuid::new_v4().as_u128())),
            request_hash,
            deadline_at_ms: i64::MAX,
        })
        .await
        .map_err(|error| format!("claim failed: {error}"))?;
    let AdmissionClaim::Owner(ticket) = claim else {
        return Err(format!("unexpected workerd claim: {claim:?}"));
    };
    admission
        .attach(AttachJob {
            ticket,
            job_id: reservation.job_id,
            command_schema: "openai.images.generation.v1".to_string(),
            command_json: to_value(command).map_err(|error| error.to_string())?,
            work_kind: "image_batch".to_string(),
            schedule_scope: format!("tenant:{tenant_id}"),
            schedule_weight: 1,
            schedule_priority: 1,
            schedule_cost: u64::from(job.n),
        })
        .await
        .map_err(|error| format!("attach failed: {error}"))?;
    Ok(reservation)
}

#[derive(Clone)]
struct CountingGenerator {
    calls: Arc<AtomicUsize>,
    image: Vec<u8>,
}

impl CountingGenerator {
    fn new() -> Self {
        let image = ImageBuffer::from_pixel(2, 1, Rgb([20_u8, 40, 60]));
        let mut cursor = std::io::Cursor::new(Vec::new());
        image
            .write_to(&mut cursor, ImageFormat::Png)
            .expect("encode worker fixture");
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            image: cursor.into_inner(),
        }
    }
}

#[async_trait]
impl ImageGenerator for CountingGenerator {
    async fn generate(&self, job: GenerationJob) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok((0..job.n)
            .map(|_| GeneratedImage {
                bytes: self.image.clone(),
            })
            .collect())
    }

    async fn edit(&self, _: EditJob) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
        unreachable!("workerd test does not execute edits")
    }
}

fn generation_job(request_id: &str) -> GenerationJob {
    GenerationJob {
        request_id: request_id.to_string(),
        model: "gpt-image-2".to_string(),
        prompt: "worker context reconstruction".to_string(),
        n: 2,
        size: "auto".to_string(),
        quality: "high".to_string(),
        output_format: "png".to_string(),
        output_compression: None,
        background: "opaque".to_string(),
        stream: false,
        partial_images: 0,
    }
}

struct TestDatabase {
    schema: String,
    pool: PgPool,
}

impl TestDatabase {
    async fn new() -> TestResult<Option<Self>> {
        let Some(url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set in CI".to_string());
            }
            eprintln!("skipping execution context test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_execution_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 4, &schema)
            .await
            .map_err(|error| format!("test database connection failed: {error:?}"))?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(|error| format!("database name query failed: {error}"))?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!("refusing DDL in non-test database {database_name}"));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(|error| format!("schema creation failed: {error}"))?;
        run_migrations(&pool)
            .await
            .map_err(|error| format!("migration failed: {error:?}"))?;
        Ok(Some(Self { schema, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .map_err(|error| format!("schema cleanup failed: {error}"));
        self.pool.close().await;
        result.map(|_| ())
    }
}

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn combine(primary: TestResult, cleanup: TestResult) -> TestResult {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(format!("{primary}; cleanup also failed: {cleanup}")),
    }
}
