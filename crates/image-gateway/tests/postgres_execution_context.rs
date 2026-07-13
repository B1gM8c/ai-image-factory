use std::{
    env,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use gpt_image_2_gateway::{
    EditJob, ExecutionContextStore, ExecutionSettlementStore, GeneratedImage, GenerationJob,
    ImageGatewayError, ImageGenerator, PostgresExecutionContextStore,
    PostgresExecutionSettlementStore, PostgresUsageStore, UsageCharge, UsageLimits,
    UsageReservation, UsageStore, Workerd,
    admission::{
        AdmissionClaim, AdmissionContract, AdmissionStore, AttachInputManifest, AttachInputObject,
        AttachJob, ClaimAdmission, EDIT_COMMAND_SCHEMA, EDIT_INPUT_MANIFEST_SCHEMA, EditCommandV1,
        EditInputDescriptorV1, EditInputRoleV1, GenerationCommandV1, PostgresAdmissionStore,
        WorkLease,
    },
    artifacts::InMemoryArtifactBlobStore,
    database::{connect_test_pool_with_search_path, run_migrations},
    input_blobs::{
        InputBlobDeleteError, InputBlobKey, InputBlobReadError, InputBlobRef, InputBlobStore,
        InputBlobWriteError,
    },
};
use image::{ImageBuffer, ImageFormat, Rgb, Rgba};
use serde_json::to_value;
use sha2::{Digest, Sha256};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn workerd_rejects_mismatched_storage_instances() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let artifacts = Arc::new(InMemoryArtifactBlobStore::default());
    let other_inputs = Arc::new(InMemoryArtifactBlobStore::default());
    let settlement = Arc::new(PostgresExecutionSettlementStore::new(
        database.pool.clone(),
        artifacts.clone(),
    ));
    let result = Workerd::new(
        "mismatched-workerd".to_string(),
        Arc::new(CountingGenerator::new()),
        Arc::new(PostgresAdmissionStore::new(database.pool.clone())),
        Arc::new(PostgresExecutionContextStore::new(database.pool.clone())),
        settlement,
        artifacts,
        other_inputs,
        Duration::from_secs(5),
    );
    let assertion = require(result.is_err(), "mismatched workerd storage was accepted");
    combine(assertion, database.cleanup().await)
}

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
                admission_session_id: None,
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
                owner_token: Uuid::new_v4(),
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
                input_manifest: None,
                work_kind: "image_batch".to_string(),
                schedule_scope: format!("tenant:{tenant_id}"),
                schedule_weight: 1,
                schedule_priority: 1,
                schedule_cost: 2,
                contract: AdmissionContract::LegacyV1,
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
async fn leased_edit_reconstructs_command_quota_and_ordered_input_metadata() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let prepared = prepare_ready_edit(&database.pool, "workerd-edit-success").await?;
        let store = PostgresExecutionContextStore::new(database.pool.clone());

        let context = store
            .load_edit(&prepared.lease)
            .await
            .map_err(|error| format!("leased edit context load failed: {error:?}"))?;

        require(context.command == prepared.command, "edit command changed")?;
        require(
            context.reservation.reservation_id == prepared.reservation.reservation_id
                && context.reservation.job_id == prepared.reservation.job_id
                && context.reservation.snapshot == prepared.reservation.snapshot,
            "edit quota reservation changed",
        )?;
        require(
            context.inputs.len() == prepared.inputs.len(),
            "edit input count changed",
        )?;
        for (actual, expected) in context.inputs.iter().zip(&prepared.inputs) {
            require(actual.blob == expected.blob, "edit blob reference changed")?;
            require(
                actual.role == expected.role
                    && actual.index == expected.index
                    && actual.media_type == expected.media_type,
                "edit input order or metadata changed",
            )?;
        }
        require(
            context.response_schema == "openai.images.response.v1",
            "edit response schema changed",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn edit_context_rejects_manifest_and_input_metadata_tampering() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresExecutionContextStore::new(database.pool.clone());

        let manifest = prepare_ready_edit(&database.pool, "workerd-edit-manifest").await?;
        sqlx::query("UPDATE job_input_manifests SET manifest_hash = $2 WHERE job_id = $1")
            .bind(manifest.reservation.job_id)
            .bind("f".repeat(64))
            .execute(&database.pool)
            .await
            .map_err(|error| format!("failed to tamper edit manifest: {error}"))?;
        require(
            store.load_edit(&manifest.lease).await.is_err(),
            "tampered edit manifest loaded context",
        )?;

        let object = prepare_ready_edit(&database.pool, "workerd-edit-object").await?;
        sqlx::query(
            "UPDATE job_input_objects SET sha256_hex = $2 WHERE job_id = $1 AND role = 'image' AND input_index = 0",
        )
        .bind(object.reservation.job_id)
        .bind("e".repeat(64))
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to tamper edit input hash: {error}"))?;
        require(
            store.load_edit(&object.lease).await.is_err(),
            "tampered edit input hash loaded context",
        )?;

        let backend = prepare_ready_edit(&database.pool, "workerd-edit-backend").await?;
        sqlx::query(
            "UPDATE job_input_objects SET storage_backend = '' WHERE job_id = $1 AND role = 'mask'",
        )
        .bind(backend.reservation.job_id)
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to tamper edit input backend: {error}"))?;
        require(
            store.load_edit(&backend.lease).await.is_err(),
            "invalid edit input backend loaded context",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn edit_context_rejects_cross_session_lease_and_inactive_job_state() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let first = prepare_ready_edit(&database.pool, "workerd-edit-session-a").await?;
        let second = prepare_ready_edit(&database.pool, "workerd-edit-session-b").await?;
        let store = PostgresExecutionContextStore::new(database.pool.clone());

        let forged = WorkLease {
            job_id: second.lease.job_id,
            command_schema: second.lease.command_schema.clone(),
            command_json: second.lease.command_json.clone(),
            ..first.lease.clone()
        };
        require(
            store.load_edit(&forged).await.is_err(),
            "cross-session edit lease loaded context",
        )?;

        sqlx::query("UPDATE jobs SET state = 'succeeded' WHERE job_id = $1")
            .bind(first.reservation.job_id)
            .execute(&database.pool)
            .await
            .map_err(|error| format!("failed to terminalize edit job: {error}"))?;
        require(
            store.load_edit(&first.lease).await.is_err(),
            "inactive edit job loaded context",
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
            artifacts.clone(),
            artifacts,
            Duration::from_secs(5),
        )
        .expect("workerd stores share one backend");

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
async fn workerd_hydrates_and_executes_durable_edit_inputs() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let admission = Arc::new(PostgresAdmissionStore::new(database.pool.clone()));
        let usage = PostgresUsageStore::new(database.pool.clone());
        let blobs = Arc::new(InMemoryArtifactBlobStore::default());
        let (reservation, image_bytes, mask_bytes) =
            prepare_attached_edit_with_blobs(admission.as_ref(), &usage, blobs.as_ref()).await?;
        let settlement = Arc::new(PostgresExecutionSettlementStore::new(
            database.pool.clone(),
            blobs.clone(),
        ));
        let generator = CountingGenerator::new();
        let workerd = Workerd::new(
            "workerd-edit-integration".to_string(),
            Arc::new(generator.clone()),
            admission,
            Arc::new(PostgresExecutionContextStore::new(database.pool.clone())),
            settlement.clone(),
            blobs.clone(),
            blobs,
            Duration::from_secs(5),
        )
        .expect("workerd stores share one backend");

        let executed = workerd
            .run_once()
            .await
            .map_err(|error| format!("edit workerd execution failed: {error:?}"))?;
        require(
            executed == Some(reservation.job_id),
            "workerd executed the wrong edit job",
        )?;
        {
            let edits = generator.edits.lock().map_err(|_| "edit lock poisoned")?;
            require(
                edits.len() == 1,
                "edit provider was not invoked exactly once",
            )?;
            require(
                edits[0].images.len() == 1
                    && edits[0].images[0].bytes == image_bytes
                    && edits[0]
                        .mask
                        .as_ref()
                        .is_some_and(|mask| mask.bytes == mask_bytes),
                "workerd did not hydrate the persisted edit bytes",
            )?;
        }
        let stored = settlement
            .load_generation_result(reservation.job_id)
            .await
            .map_err(|error| format!("edit result load failed: {error:?}"))?
            .ok_or_else(|| "workerd did not persist the edit result".to_string())?;
        require(
            stored.projection.operation == "edit" && stored.images.len() == 1,
            "workerd persisted an invalid edit result projection",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn corrupt_edit_input_fails_once_without_provider_execution() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let admission = Arc::new(PostgresAdmissionStore::new(database.pool.clone()));
        let usage = PostgresUsageStore::new(database.pool.clone());
        let blobs = Arc::new(InMemoryArtifactBlobStore::default());
        let (reservation, _, _) =
            prepare_attached_edit_with_blobs(admission.as_ref(), &usage, blobs.as_ref()).await?;
        let settlement = Arc::new(PostgresExecutionSettlementStore::new(
            database.pool.clone(),
            blobs.clone(),
        ));
        let storage_identity = InputBlobStore::storage_identity(blobs.as_ref());
        let generator = CountingGenerator::new();
        let workerd = Workerd::new(
            "workerd-corrupt-edit".to_string(),
            Arc::new(generator.clone()),
            admission,
            Arc::new(PostgresExecutionContextStore::new(database.pool.clone())),
            settlement,
            blobs,
            Arc::new(IntegrityInputStore { storage_identity }),
            Duration::from_secs(5),
        )
        .expect("fault-injection store preserves backend identity");

        require(
            workerd.run_once().await.is_err(),
            "corrupt edit input unexpectedly completed",
        )?;
        require(
            generator.calls.load(Ordering::SeqCst) == 0,
            "provider ran for corrupt edit input",
        )?;
        let states: (String, String, String, i32, Option<String>) = sqlx::query_as(
            r#"
            SELECT w.state, ja.state, qr.state, qr.released_units, j.last_error_code
            FROM jobs j
            JOIN quota_reservations qr ON qr.reservation_id = j.reservation_id
            JOIN work_items w ON w.job_id = j.job_id
            JOIN job_attempts ja ON ja.work_item_id = w.work_item_id
            WHERE j.job_id = $1
            "#,
        )
        .bind(reservation.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect corrupt edit settlement: {error}"))?;
        require(
            states
                == (
                    "failed".to_string(),
                    "failed".to_string(),
                    "released".to_string(),
                    1,
                    Some("input_artifact_integrity".to_string()),
                ),
            format!("corrupt edit input was not terminalized atomically: {states:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn unavailable_edit_input_keeps_lease_and_quota_recoverable() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let admission = Arc::new(PostgresAdmissionStore::new(database.pool.clone()));
        let usage = PostgresUsageStore::new(database.pool.clone());
        let blobs = Arc::new(InMemoryArtifactBlobStore::default());
        let (reservation, _, _) =
            prepare_attached_edit_with_blobs(admission.as_ref(), &usage, blobs.as_ref()).await?;
        let settlement = Arc::new(PostgresExecutionSettlementStore::new(
            database.pool.clone(),
            blobs.clone(),
        ));
        let storage_identity = InputBlobStore::storage_identity(blobs.as_ref());
        let generator = CountingGenerator::new();
        let workerd = Workerd::new(
            "workerd-unavailable-edit".to_string(),
            Arc::new(generator.clone()),
            admission,
            Arc::new(PostgresExecutionContextStore::new(database.pool.clone())),
            settlement,
            blobs,
            Arc::new(UnavailableInputStore { storage_identity }),
            Duration::from_secs(5),
        )
        .expect("fault-injection store preserves backend identity");

        require(
            workerd.run_once().await.is_err(),
            "unavailable edit input unexpectedly completed",
        )?;
        require(
            generator.calls.load(Ordering::SeqCst) == 0,
            "provider ran while edit input storage was unavailable",
        )?;
        let states: (String, String, String, i32, Option<String>) = sqlx::query_as(
            r#"
            SELECT w.state, ja.state, qr.state, qr.released_units, j.last_error_code
            FROM jobs j
            JOIN quota_reservations qr ON qr.reservation_id = j.reservation_id
            JOIN work_items w ON w.job_id = j.job_id
            JOIN job_attempts ja ON ja.work_item_id = w.work_item_id
            WHERE j.job_id = $1
            "#,
        )
        .bind(reservation.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect unavailable edit state: {error}"))?;
        require(
            states
                == (
                    "leased".to_string(),
                    "claimed".to_string(),
                    "reserved".to_string(),
                    0,
                    None,
                ),
            format!("transient input failure became terminal: {states:?}"),
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
            artifacts.clone(),
            artifacts,
            Duration::from_secs(5),
        )
        .expect("workerd stores share one backend");

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
            admission_session_id: None,
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
            owner_token: Uuid::new_v4(),
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
            input_manifest: None,
            work_kind: "image_batch".to_string(),
            schedule_scope: format!("tenant:{tenant_id}"),
            schedule_weight: 1,
            schedule_priority: 1,
            schedule_cost: u64::from(job.n),
            contract: AdmissionContract::LegacyV1,
        })
        .await
        .map_err(|error| format!("attach failed: {error}"))?;
    Ok(reservation)
}

async fn prepare_attached_edit_with_blobs(
    admission: &PostgresAdmissionStore,
    usage: &PostgresUsageStore,
    blobs: &dyn InputBlobStore,
) -> TestResult<(UsageReservation, Vec<u8>, Vec<u8>)> {
    let request_id = format!("req_{}", Uuid::new_v4().simple());
    let tenant_id = format!("tenant_{}", Uuid::new_v4().simple());
    let image_bytes = rgba_png([10, 20, 30, 255]);
    let mask_bytes = rgba_png([0, 0, 0, 0]);
    let descriptors = vec![
        EditInputDescriptorV1 {
            byte_size: image_bytes.len() as u64,
            index: 0,
            media_type: "image/png".to_string(),
            role: EditInputRoleV1::Image,
            sha256_hex: hex::encode(Sha256::digest(&image_bytes)),
        },
        EditInputDescriptorV1 {
            byte_size: mask_bytes.len() as u64,
            index: 0,
            media_type: "image/png".to_string(),
            role: EditInputRoleV1::Mask,
            sha256_hex: hex::encode(Sha256::digest(&mask_bytes)),
        },
    ];
    let command = EditCommandV1::from_edit_job(
        &edit_job(&request_id),
        descriptors,
        "openai-images-v1",
        "openai-codex",
    );
    let reservation = usage
        .reserve(UsageCharge {
            tenant_id: tenant_id.clone(),
            request_id: request_id.clone(),
            admission_session_id: None,
            operation: "edit",
            provider_id: "openai-codex".to_string(),
            model: "gpt-image-2".to_string(),
            units: 1,
            limits: UsageLimits {
                five_hour_image_limit: 17,
                seven_day_image_limit: 53,
            },
        })
        .await
        .map_err(|error| format!("edit reserve failed: {error:?}"))?;
    let claim = admission
        .claim(ClaimAdmission {
            owner_token: Uuid::new_v4(),
            tenant_id: tenant_id.clone(),
            project_id: "project-edit-worker".to_string(),
            api_profile: "openai-images-v1".to_string(),
            operation: "edit".to_string(),
            request_id,
            idempotency_key_digest: None,
            request_hash: command.request_hash_hex(),
            deadline_at_ms: i64::MAX,
        })
        .await
        .map_err(|error| format!("edit claim failed: {error}"))?;
    let AdmissionClaim::Owner(ticket) = claim else {
        return Err(format!("unexpected edit admission claim: {claim:?}"));
    };
    let mut inputs = Vec::new();
    for (role, index, bytes) in [
        (EditInputRoleV1::Image, 0_u16, image_bytes.as_slice()),
        (EditInputRoleV1::Mask, 0_u16, mask_bytes.as_slice()),
    ] {
        let blob = blobs
            .put(
                InputBlobKey {
                    admission_session_id: ticket.session_id,
                    input_id: Uuid::new_v4(),
                },
                bytes,
            )
            .await
            .map_err(|error| format!("edit blob put failed: {error:?}"))?;
        inputs.push(AttachInputObject {
            blob,
            role,
            index,
            media_type: "image/png".to_string(),
        });
    }
    admission
        .attach(AttachJob {
            ticket,
            job_id: reservation.job_id,
            command_schema: EDIT_COMMAND_SCHEMA.to_string(),
            command_json: to_value(&command).map_err(|error| error.to_string())?,
            input_manifest: Some(AttachInputManifest {
                manifest_schema: EDIT_INPUT_MANIFEST_SCHEMA.to_string(),
                manifest_hash: command.input_manifest_hash_hex(),
                inputs,
            }),
            work_kind: "image_batch".to_string(),
            schedule_scope: format!("tenant:{tenant_id}"),
            schedule_weight: 1,
            schedule_priority: 1,
            schedule_cost: 1,
            contract: AdmissionContract::LegacyV1,
        })
        .await
        .map_err(|error| format!("edit attach failed: {error}"))?;
    Ok((reservation, image_bytes, mask_bytes))
}

struct PreparedEdit {
    lease: WorkLease,
    reservation: UsageReservation,
    command: EditCommandV1,
    inputs: Vec<AttachInputObject>,
}

async fn prepare_ready_edit(pool: &PgPool, worker_id: &str) -> TestResult<PreparedEdit> {
    let request_id = format!("req_{}", Uuid::new_v4().simple());
    let tenant_id = format!("tenant_{}", Uuid::new_v4().simple());
    let placeholder_inputs = edit_inputs(Uuid::new_v4());
    let placeholder_command = edit_command(&request_id, &placeholder_inputs);
    let usage = PostgresUsageStore::new(pool.clone());
    let reservation = usage
        .reserve(UsageCharge {
            tenant_id: tenant_id.clone(),
            request_id: request_id.clone(),
            admission_session_id: None,
            operation: "edit",
            provider_id: "openai-codex".to_string(),
            model: "gpt-image-2".to_string(),
            units: 1,
            limits: UsageLimits {
                five_hour_image_limit: 17,
                seven_day_image_limit: 53,
            },
        })
        .await
        .map_err(|error| format!("edit reserve failed: {error:?}"))?;
    let admission = PostgresAdmissionStore::new(pool.clone());
    let claim = admission
        .claim(ClaimAdmission {
            owner_token: Uuid::new_v4(),
            tenant_id: tenant_id.clone(),
            project_id: "project-edit-worker".to_string(),
            api_profile: "openai-images-v1".to_string(),
            operation: "edit".to_string(),
            request_id: request_id.clone(),
            idempotency_key_digest: None,
            request_hash: placeholder_command.request_hash_hex(),
            deadline_at_ms: i64::MAX,
        })
        .await
        .map_err(|error| format!("edit claim failed: {error}"))?;
    let AdmissionClaim::Owner(ticket) = claim else {
        return Err(format!("unexpected edit admission claim: {claim:?}"));
    };
    let inputs = edit_inputs(ticket.session_id);
    let command = edit_command(&request_id, &inputs);
    admission
        .attach(AttachJob {
            ticket,
            job_id: reservation.job_id,
            command_schema: EDIT_COMMAND_SCHEMA.to_string(),
            command_json: to_value(&command).map_err(|error| error.to_string())?,
            input_manifest: Some(AttachInputManifest {
                manifest_schema: EDIT_INPUT_MANIFEST_SCHEMA.to_string(),
                manifest_hash: command.input_manifest_hash_hex(),
                inputs: inputs.clone(),
            }),
            work_kind: "image_batch".to_string(),
            schedule_scope: format!("tenant:{tenant_id}"),
            schedule_weight: 1,
            schedule_priority: 1,
            schedule_cost: 1,
            contract: AdmissionContract::LegacyV1,
        })
        .await
        .map_err(|error| format!("edit attach failed: {error}"))?;
    let lease = admission
        .claim_ready(worker_id, 60_000)
        .await
        .map_err(|error| format!("edit work claim failed: {error}"))?
        .ok_or_else(|| "attached edit work was not claimable".to_string())?;
    Ok(PreparedEdit {
        lease,
        reservation,
        command,
        inputs,
    })
}

fn edit_inputs(session_id: Uuid) -> Vec<AttachInputObject> {
    [
        (EditInputRoleV1::Image, 1, "2".repeat(64), 234_u64),
        (EditInputRoleV1::Image, 0, "1".repeat(64), 123_u64),
        (EditInputRoleV1::Mask, 0, "3".repeat(64), 45_u64),
    ]
    .into_iter()
    .map(|(role, index, sha256_hex, byte_size)| AttachInputObject {
        blob: InputBlobRef {
            key: InputBlobKey {
                admission_session_id: session_id,
                input_id: Uuid::new_v4(),
            },
            storage_backend: "filesystem".to_string(),
            object_key: format!(
                "inputs/{}/{role}-{index}",
                session_id.simple(),
                role = role.as_str()
            ),
            sha256_hex,
            byte_size,
        },
        role,
        index,
        media_type: "image/png".to_string(),
    })
    .collect()
}

fn edit_command(request_id: &str, inputs: &[AttachInputObject]) -> EditCommandV1 {
    EditCommandV1::from_edit_job(
        &EditJob {
            request_id: request_id.to_string(),
            model: "gpt-image-2".to_string(),
            prompt: "replace the sky".to_string(),
            moderation: "auto".to_string(),
            images: Vec::new(),
            mask: None,
            n: 1,
            size: "1024x1024".to_string(),
            quality: "high".to_string(),
            output_format: "png".to_string(),
            output_compression: None,
            background: "auto".to_string(),
            stream: false,
            partial_images: 0,
        },
        inputs
            .iter()
            .map(|input| EditInputDescriptorV1 {
                byte_size: input.blob.byte_size,
                index: input.index,
                media_type: input.media_type.clone(),
                role: input.role,
                sha256_hex: input.blob.sha256_hex.clone(),
            })
            .collect(),
        "openai-images-v1",
        "openai-codex",
    )
}

#[derive(Clone)]
struct CountingGenerator {
    calls: Arc<AtomicUsize>,
    edits: Arc<Mutex<Vec<EditJob>>>,
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
            edits: Arc::new(Mutex::new(Vec::new())),
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

    async fn edit(&self, job: EditJob) -> Result<Vec<GeneratedImage>, ImageGatewayError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.edits.lock().expect("edit lock").push(job.clone());
        Ok((0..job.n)
            .map(|_| GeneratedImage {
                bytes: self.image.clone(),
            })
            .collect())
    }
}

fn generation_job(request_id: &str) -> GenerationJob {
    GenerationJob {
        request_id: request_id.to_string(),
        model: "gpt-image-2".to_string(),
        prompt: "worker context reconstruction".to_string(),
        moderation: "auto".to_string(),
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

fn edit_job(request_id: &str) -> EditJob {
    EditJob {
        request_id: request_id.to_string(),
        model: "gpt-image-2".to_string(),
        prompt: "replace the sky".to_string(),
        moderation: "auto".to_string(),
        images: Vec::new(),
        mask: None,
        n: 1,
        size: "auto".to_string(),
        quality: "high".to_string(),
        output_format: "png".to_string(),
        output_compression: None,
        background: "auto".to_string(),
        stream: false,
        partial_images: 0,
    }
}

fn rgba_png(pixel: [u8; 4]) -> Vec<u8> {
    let image = ImageBuffer::from_pixel(1, 1, Rgba(pixel));
    let mut cursor = std::io::Cursor::new(Vec::new());
    image
        .write_to(&mut cursor, ImageFormat::Png)
        .expect("encode edit input fixture");
    cursor.into_inner()
}

struct IntegrityInputStore {
    storage_identity: String,
}

#[async_trait]
impl InputBlobStore for IntegrityInputStore {
    fn storage_identity(&self) -> String {
        self.storage_identity.clone()
    }

    async fn put(&self, _: InputBlobKey, _: &[u8]) -> Result<InputBlobRef, InputBlobWriteError> {
        Err(InputBlobWriteError::Unavailable)
    }

    async fn get(&self, _: &InputBlobRef) -> Result<Vec<u8>, InputBlobReadError> {
        Err(InputBlobReadError::Integrity)
    }

    async fn delete(&self, _: &InputBlobRef) -> Result<(), InputBlobDeleteError> {
        Ok(())
    }

    async fn delete_session(&self, _: Uuid) -> Result<(), InputBlobDeleteError> {
        Ok(())
    }
}

struct UnavailableInputStore {
    storage_identity: String,
}

#[async_trait]
impl InputBlobStore for UnavailableInputStore {
    fn storage_identity(&self) -> String {
        self.storage_identity.clone()
    }

    async fn put(&self, _: InputBlobKey, _: &[u8]) -> Result<InputBlobRef, InputBlobWriteError> {
        Err(InputBlobWriteError::Unavailable)
    }

    async fn get(&self, _: &InputBlobRef) -> Result<Vec<u8>, InputBlobReadError> {
        Err(InputBlobReadError::Unavailable)
    }

    async fn delete(&self, _: &InputBlobRef) -> Result<(), InputBlobDeleteError> {
        Ok(())
    }

    async fn delete_session(&self, _: Uuid) -> Result<(), InputBlobDeleteError> {
        Ok(())
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
