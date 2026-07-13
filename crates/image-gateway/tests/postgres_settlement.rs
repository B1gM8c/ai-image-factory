use std::{env, sync::Arc};

use gpt_image_2_gateway::{
    ExecutionSettlementStore, PostgresExecutionSettlementStore, PostgresUsageStore, UsageCharge,
    UsageLimits, UsageReservation, UsageStore,
    admission::{
        AdmissionClaim, AdmissionContract, AdmissionStore, AdmissionTicket, AttachJob,
        ClaimAdmission, WorkLease,
    },
    artifacts::{
        ArtifactBlobStore, ArtifactIdentity, GENERATION_RESPONSE_SCHEMA,
        GenerationResponseProjection, GenerationResultManifest, InMemoryArtifactBlobStore,
    },
    database::{connect_test_pool_with_search_path, run_migrations},
};
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn forged_reservation_rolls_back_every_success_transition() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = RunningFixture::new(&database.pool).await?;
        let artifacts = Arc::new(InMemoryArtifactBlobStore::default());
        let result_manifest = fixture.result_manifest(artifacts.as_ref()).await?;
        let store = PostgresExecutionSettlementStore::new(database.pool.clone(), artifacts);

        for forged in forged_reservations(&fixture.reservation) {
            require(
                store
                    .succeed(&fixture.lease, &forged, &result_manifest)
                    .await
                    .is_err(),
                "forged reservation was accepted",
            )?;
        }

        let state: RollbackState = sqlx::query_as(
            r#"
            SELECT
              w.state AS work_state,
              qr.state AS quota_state,
              i.state AS idempotency_state,
              (SELECT COUNT(*) FROM usage_events ue
               WHERE ue.tenant_id = qr.tenant_id
                 AND ue.request_id = qr.request_id
                 AND ue.outcome = 'charged') AS charged_usage_events,
              (SELECT COUNT(*) FROM metering_events me
               WHERE me.job_id = w.job_id
                 AND me.event_type IN ('quota_committed', 'job_succeeded')) AS success_metering_events,
              (SELECT COUNT(*) FROM job_events je
               WHERE je.job_id = w.job_id AND je.event_type = 'job.succeeded') AS succeeded_job_events,
              (SELECT COUNT(*) FROM outbox_events oe
               WHERE oe.job_id = w.job_id AND oe.event_type = 'job.succeeded') AS succeeded_outbox_events
            FROM work_items w
            JOIN quota_reservations qr ON qr.job_id = w.job_id
            JOIN idempotency_requests i ON i.job_id = w.job_id
            WHERE w.work_item_id = $1
            "#,
        )
        .bind(fixture.lease.work_item_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to read rollback state: {error}"))?;

        require(state.work_state == "running", "work did not remain running")?;
        require(state.quota_state == "reserved", "quota did not remain reserved")?;
        require(
            state.idempotency_state == "accepted",
            "idempotency did not remain accepted",
        )?;
        require(
            state.charged_usage_events == 0
                && state.success_metering_events == 0
                && state.succeeded_job_events == 0
                && state.succeeded_outbox_events == 0,
            format!("forged settlement leaked success effects: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn valid_settlement_is_atomic_and_duplicate_calls_are_side_effect_free() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = RunningFixture::new(&database.pool).await?;
        let artifacts = Arc::new(InMemoryArtifactBlobStore::default());
        let result_manifest = fixture.result_manifest(artifacts.as_ref()).await?;
        let store = PostgresExecutionSettlementStore::new(database.pool.clone(), artifacts);

        let first = store
            .succeed(&fixture.lease, &fixture.reservation, &result_manifest)
            .await
            .map_err(|error| format!("valid settlement failed: {error:?}"))?;
        assert_snapshot_matches(&first, &fixture.reservation)?;
        assert_success_state(&database.pool, &fixture).await?;
        let counts_after_first = success_effect_counts(&database.pool, &fixture).await?;

        let repeated = store
            .succeed(&fixture.lease, &fixture.reservation, &result_manifest)
            .await
            .map_err(|error| format!("duplicate settlement failed: {error:?}"))?;
        assert_snapshot_matches(&repeated, &fixture.reservation)?;
        let counts_after_repeat = success_effect_counts(&database.pool, &fixture).await?;

        require(
            counts_after_repeat == counts_after_first,
            format!(
                "duplicate settlement wrote success effects: first={counts_after_first:?}, repeated={counts_after_repeat:?}"
            ),
        )?;
        require(
            counts_after_repeat == SuccessEffectCounts::expected(),
            format!("unexpected success effect counts: {counts_after_repeat:?}"),
        )?;

        let mut conflicting_manifest = result_manifest.clone();
        conflicting_manifest.projection.quality = "low".to_string();
        require(
            store
                .succeed(
                    &fixture.lease,
                    &fixture.reservation,
                    &conflicting_manifest,
                )
                .await
                .is_err(),
            "completed settlement accepted a different response projection",
        )?;
        let counts_after_conflict = success_effect_counts(&database.pool, &fixture).await?;
        require(
            counts_after_conflict == counts_after_repeat,
            "conflicting duplicate settlement changed durable success effects",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn valid_handles_from_different_jobs_cannot_be_cross_settled() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let left = RunningFixture::new(&database.pool).await?;
        let right = RunningFixture::new(&database.pool).await?;
        let artifacts = Arc::new(InMemoryArtifactBlobStore::default());
        let result_manifest = left.result_manifest(artifacts.as_ref()).await?;
        let store = PostgresExecutionSettlementStore::new(database.pool.clone(), artifacts);

        require(
            store
                .succeed(&left.lease, &right.reservation, &result_manifest)
                .await
                .is_err(),
            "valid handles from different jobs were cross-settled",
        )?;
        let states: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM work_items
               WHERE job_id IN ($1, $2) AND state = 'running'),
              (SELECT COUNT(*) FROM quota_reservations
               WHERE job_id IN ($1, $2) AND state = 'reserved'),
              (SELECT COUNT(*) FROM usage_events
               WHERE request_id IN ($3, $4) AND outcome = 'charged')
            "#,
        )
        .bind(left.reservation.job_id)
        .bind(right.reservation.job_id)
        .bind(&left.reservation.charge.request_id)
        .bind(&right.reservation.charge.request_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect cross-settlement rollback: {error}"))?;
        require(
            states == (2, 2, 0),
            format!("cross-settlement mutated durable state: {states:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn failure_settlement_releases_quota_and_transitions_every_state_atomically() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = RunningFixture::new(&database.pool).await?;
        let artifacts = Arc::new(InMemoryArtifactBlobStore::default());
        let store = PostgresExecutionSettlementStore::new(database.pool.clone(), artifacts);

        store
            .fail(&fixture.lease, &fixture.reservation, "provider_rejected")
            .await
            .map_err(|error| format!("failure settlement failed: {error:?}"))?;
        let first = failure_state(&database.pool, &fixture).await?;
        require(
            first == FailureState::expected(),
            format!("unexpected failure settlement state: {first:?}"),
        )?;
        match store
            .generation_status(fixture.lease.job_id)
            .await
            .map_err(|error| format!("failure status load failed: {error:?}"))?
        {
            gpt_image_2_gateway::GenerationResultStatus::Failed { error_code } => require(
                error_code.as_deref() == Some("provider_rejected"),
                format!("failure status lost its error code: {error_code:?}"),
            )?,
            status => return Err(format!("unexpected failure status: {status:?}")),
        }

        store
            .fail(&fixture.lease, &fixture.reservation, "provider_rejected")
            .await
            .map_err(|error| format!("duplicate failure settlement failed: {error:?}"))?;
        let repeated = failure_state(&database.pool, &fixture).await?;
        require(
            repeated == first,
            format!("duplicate failure settlement changed state: {repeated:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
struct FailureState {
    work_failed: bool,
    attempt_failed: bool,
    idempotency_failed: bool,
    job_failed: bool,
    quota_released: bool,
    charged_usage: i64,
    release_metering: i64,
    failed_metering: i64,
    job_events: i64,
    outbox: i64,
}

impl FailureState {
    fn expected() -> Self {
        Self {
            work_failed: true,
            attempt_failed: true,
            idempotency_failed: true,
            job_failed: true,
            quota_released: true,
            charged_usage: 0,
            release_metering: 1,
            failed_metering: 1,
            job_events: 1,
            outbox: 1,
        }
    }
}

async fn failure_state(pool: &PgPool, fixture: &RunningFixture) -> TestResult<FailureState> {
    sqlx::query_as(
        r#"
        SELECT
          (SELECT state = 'failed' FROM work_items WHERE work_item_id = $1) AS work_failed,
          (SELECT state = 'failed' FROM job_attempts WHERE execution_id = $2) AS attempt_failed,
          (SELECT state = 'failed' FROM idempotency_requests WHERE job_id = $3) AS idempotency_failed,
          (SELECT state = 'failed' FROM jobs WHERE job_id = $3) AS job_failed,
          (SELECT state = 'released' AND released_units = requested_units
           FROM quota_reservations WHERE reservation_id = $4) AS quota_released,
          (SELECT COUNT(*) FROM usage_events
           WHERE request_id = $5 AND outcome = 'charged') AS charged_usage,
          (SELECT COUNT(*) FROM metering_events
           WHERE job_id = $3 AND event_type = 'quota_released') AS release_metering,
          (SELECT COUNT(*) FROM metering_events
           WHERE job_id = $3 AND event_type = 'job_failed') AS failed_metering,
          (SELECT COUNT(*) FROM job_events
           WHERE job_id = $3 AND event_type = 'job.failed') AS job_events,
          (SELECT COUNT(*) FROM outbox_events
           WHERE job_id = $3 AND event_type = 'job.failed') AS outbox
        "#,
    )
    .bind(fixture.lease.work_item_id)
    .bind(fixture.lease.execution_id)
    .bind(fixture.lease.job_id)
    .bind(fixture.reservation.reservation_id)
    .bind(&fixture.reservation.charge.request_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failure state query failed: {error}"))
}

#[derive(Debug, sqlx::FromRow)]
struct RollbackState {
    work_state: String,
    quota_state: String,
    idempotency_state: String,
    charged_usage_events: i64,
    success_metering_events: i64,
    succeeded_job_events: i64,
    succeeded_outbox_events: i64,
}

#[derive(Debug)]
struct RunningFixture {
    lease: WorkLease,
    reservation: UsageReservation,
}

impl RunningFixture {
    async fn new(pool: &PgPool) -> TestResult<Self> {
        let tenant_id = format!("tenant-{}", Uuid::new_v4().simple());
        let request_id = format!("req_{}", Uuid::new_v4().simple());
        let key_digest = Uuid::new_v4().simple().to_string().repeat(2);
        let usage = PostgresUsageStore::new(pool.clone());
        let reservation = usage
            .reserve(UsageCharge {
                tenant_id: tenant_id.clone(),
                request_id: request_id.clone(),
                admission_session_id: None,
                operation: "generation",
                provider_id: "openai-codex".to_string(),
                model: "gpt-image-2".to_string(),
                units: 2,
                limits: UsageLimits {
                    five_hour_image_limit: 20,
                    seven_day_image_limit: 40,
                },
            })
            .await
            .map_err(|error| format!("failed to reserve quota: {error:?}"))?;

        let admission = gpt_image_2_gateway::admission::PostgresAdmissionStore::new(pool.clone());
        let claim = admission
            .claim(ClaimAdmission {
                owner_token: Uuid::new_v4(),
                tenant_id,
                project_id: "project-settlement".to_string(),
                api_profile: "openai-images-v1".to_string(),
                operation: "generation".to_string(),
                request_id,
                idempotency_key_digest: Some(key_digest),
                request_hash: "b".repeat(64),
                deadline_at_ms: i64::MAX,
            })
            .await
            .map_err(|error| format!("failed to claim admission: {error}"))?;
        let AdmissionClaim::Owner(ticket) = claim else {
            return Err(format!("unexpected admission claim: {claim:?}"));
        };
        let lease = admission
            .attach_and_start(
                attach_request(ticket, reservation.job_id),
                "worker-settlement",
                60_000,
            )
            .await
            .map_err(|error| format!("failed to attach and start work: {error}"))?;

        Ok(Self { lease, reservation })
    }

    async fn result_manifest(
        &self,
        artifacts: &dyn ArtifactBlobStore,
    ) -> TestResult<GenerationResultManifest> {
        let mut stored = Vec::new();
        for output_index in 0..self.reservation.charge.units {
            stored.push(
                artifacts
                    .put(
                        ArtifactIdentity {
                            artifact_id: Uuid::new_v4(),
                            tenant_id: self.reservation.charge.tenant_id.clone(),
                            job_id: self.lease.job_id,
                            work_item_id: self.lease.work_item_id,
                            execution_id: self.lease.execution_id,
                            lease_epoch: self.lease.lease_epoch,
                            output_index,
                            media_type: "image/png".to_string(),
                        },
                        format!("artifact-{output_index}").as_bytes(),
                    )
                    .await
                    .map_err(|error| format!("failed to stage test artifact: {error:?}"))?,
            );
        }
        Ok(GenerationResultManifest {
            job_id: self.lease.job_id,
            tenant_id: self.reservation.charge.tenant_id.clone(),
            projection: GenerationResponseProjection {
                api_profile: "openai-images-v1".to_string(),
                operation: "generation".to_string(),
                response_schema: GENERATION_RESPONSE_SCHEMA.to_string(),
                created_at_seconds: 1_800_000_000,
                output_format: "png".to_string(),
                quality: "high".to_string(),
                size: "1024x1024".to_string(),
                background: "opaque".to_string(),
                stream: false,
                usage: self.reservation.snapshot.clone(),
            },
            artifacts: stored,
        })
    }
}

fn attach_request(ticket: AdmissionTicket, job_id: Uuid) -> AttachJob {
    AttachJob {
        ticket,
        job_id,
        command_schema: "openai.images.generation.v1".to_string(),
        command_json: json!({"prompt": "atomic settlement"}),
        input_manifest: None,
        work_kind: "image_batch".to_string(),
        schedule_scope: "tenant-settlement".to_string(),
        schedule_weight: 1,
        schedule_priority: 1,
        schedule_cost: 1,
        contract: AdmissionContract::LegacyV1,
    }
}

fn forged_reservations(reservation: &UsageReservation) -> Vec<UsageReservation> {
    let mut forged = Vec::new();

    let mut value = reservation.clone();
    value.reservation_id = Uuid::new_v4();
    forged.push(value);

    let mut value = reservation.clone();
    value.job_id = Uuid::new_v4();
    forged.push(value);

    let mut value = reservation.clone();
    value.charge.tenant_id = "forged-tenant".to_string();
    forged.push(value);

    let mut value = reservation.clone();
    value.charge.request_id = "forged-request".to_string();
    forged.push(value);

    let mut value = reservation.clone();
    value.charge.operation = "edit";
    forged.push(value);

    let mut value = reservation.clone();
    value.charge.units += 1;
    forged.push(value);

    let mut value = reservation.clone();
    value.charge.provider_id = "forged-provider".to_string();
    forged.push(value);

    let mut value = reservation.clone();
    value.charge.model = "forged-model".to_string();
    forged.push(value);

    forged
}

async fn assert_success_state(pool: &PgPool, fixture: &RunningFixture) -> TestResult {
    let work: (String, Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT state, lease_owner, lease_expires_at_ms FROM work_items WHERE work_item_id = $1",
    )
    .bind(fixture.lease.work_item_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to read work state: {error}"))?;
    require(
        work == ("succeeded".to_string(), None, None),
        format!("unexpected settled work state: {work:?}"),
    )?;

    let attempt: (String, Option<i64>) =
        sqlx::query_as("SELECT state, finished_at_ms FROM job_attempts WHERE execution_id = $1")
            .bind(fixture.lease.execution_id)
            .fetch_one(pool)
            .await
            .map_err(|error| format!("failed to read attempt state: {error}"))?;
    require(
        attempt.0 == "succeeded" && attempt.1.is_some(),
        format!("unexpected attempt state: {attempt:?}"),
    )?;

    let idempotency: (String, Option<String>) = sqlx::query_as(
        "SELECT state, terminal_outcome FROM idempotency_requests WHERE job_id = $1",
    )
    .bind(fixture.lease.job_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to read idempotency state: {error}"))?;
    require(
        idempotency == ("succeeded".to_string(), Some("succeeded".to_string())),
        format!("unexpected idempotency state: {idempotency:?}"),
    )?;

    let quota_and_job: (String, i32, String, i32, Option<i64>) = sqlx::query_as(
        r#"
        SELECT qr.state, qr.committed_units, j.state, j.charged_units, j.finished_at_ms
        FROM quota_reservations qr
        JOIN jobs j ON j.job_id = qr.job_id AND j.reservation_id = qr.reservation_id
        WHERE qr.reservation_id = $1
        "#,
    )
    .bind(fixture.reservation.reservation_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to read quota and job state: {error}"))?;
    require(
        quota_and_job.0 == "committed"
            && quota_and_job.1 == fixture.reservation.charge.units as i32
            && quota_and_job.2 == "succeeded"
            && quota_and_job.3 == fixture.reservation.charge.units as i32
            && quota_and_job.4.is_some(),
        format!("unexpected quota/job state: {quota_and_job:?}"),
    )
}

fn assert_snapshot_matches(
    actual: &gpt_image_2_gateway::UsageSnapshot,
    reservation: &UsageReservation,
) -> TestResult {
    require(
        actual.limit_5h == reservation.snapshot.limit_5h
            && actual.remaining_5h == reservation.snapshot.remaining_5h
            && actual.limit_7d == reservation.snapshot.limit_7d
            && actual.remaining_7d == reservation.snapshot.remaining_7d,
        format!(
            "settlement returned the wrong snapshot: actual={actual:?}, expected={:?}",
            reservation.snapshot
        ),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, sqlx::FromRow)]
struct SuccessEffectCounts {
    usage: i64,
    quota_metering: i64,
    job_metering: i64,
    job_events: i64,
    outbox: i64,
    projections: i64,
    artifacts: i64,
}

impl SuccessEffectCounts {
    fn expected() -> Self {
        Self {
            usage: 1,
            quota_metering: 1,
            job_metering: 1,
            job_events: 1,
            outbox: 1,
            projections: 1,
            artifacts: 2,
        }
    }
}

async fn success_effect_counts(
    pool: &PgPool,
    fixture: &RunningFixture,
) -> TestResult<SuccessEffectCounts> {
    sqlx::query_as(
        r#"
        SELECT
          (SELECT COUNT(*) FROM usage_events
           WHERE tenant_id = $1 AND request_id = $2 AND outcome = 'charged') AS usage,
          (SELECT COUNT(*) FROM metering_events
           WHERE job_id = $3 AND event_type = 'quota_committed') AS quota_metering,
          (SELECT COUNT(*) FROM metering_events
           WHERE job_id = $3 AND event_type = 'job_succeeded') AS job_metering,
          (SELECT COUNT(*) FROM job_events
           WHERE job_id = $3 AND event_type = 'job.succeeded') AS job_events,
          (SELECT COUNT(*) FROM outbox_events
           WHERE job_id = $3 AND event_type = 'job.succeeded') AS outbox,
          (SELECT COUNT(*) FROM job_response_projections
           WHERE job_id = $3) AS projections,
          (SELECT COUNT(*) FROM artifacts
           WHERE job_id = $3) AS artifacts
        "#,
    )
    .bind(&fixture.reservation.charge.tenant_id)
    .bind(&fixture.reservation.charge.request_id)
    .bind(fixture.reservation.job_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to count success effects: {error}"))
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
            eprintln!("skipping PostgreSQL settlement test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_settlement_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 8, &schema)
            .await
            .map_err(|error| format!("failed to connect to test database: {error:?}"))?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(|error| format!("failed to identify database: {error}"))?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!("refusing DDL in non-test database {database_name}"));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(|error| format!("failed to create test schema: {error}"))?;
        if let Err(error) = run_migrations(&pool).await {
            let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                .execute(&pool)
                .await;
            pool.close().await;
            return Err(format!("failed to migrate test schema: {error:?}"));
        }
        Ok(Some(Self { schema, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .map_err(|error| format!("failed to drop test schema: {error}"));
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
