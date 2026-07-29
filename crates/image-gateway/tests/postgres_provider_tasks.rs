use std::{
    env,
    fs::{self, File},
    os::unix::fs::PermissionsExt,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use gpt_image_2_gateway::{
    ExecutorClaimScope, ExecutorHandoffStore, ExecutorLaunchContext, ExecutorSubmissionLease,
    ExecutorSubmissionOutcome, ExecutorSubmissionStore, FilesystemArtifactBlobStore,
    FilesystemProviderArtifactStagerFactory, PostgresExecutorSubmissionStore,
    PostgresProviderTaskStore, ProviderArtifactAuthority, ProviderArtifactPublication,
    ProviderArtifactStageContext, ProviderArtifactStager, ProviderArtifactStagerFactory,
    ProviderCapacityEvidence, ProviderCapacityEvidenceOutcome, ProviderCapacityReconciliationState,
    ProviderCapacityReconciliationStore, ProviderCapacityTerminalState, ProviderExecutionContext,
    ProviderPollDaemon, ProviderPollDaemonConfig, ProviderPollDriver, ProviderPollDriverCall,
    ProviderPollOrchestrator, ProviderPollOrchestratorConfig, ProviderPollRun, ProviderPollStore,
    ProviderProfileReadinessStatus, ProviderProfileReadinessStore, ProviderProfileReadinessSummary,
    ProviderRemoteTask, ProviderRuntimeProfileStore, ProviderRuntimeReadinessStore,
    ProviderRuntimeRegistration, ProviderRuntimeRole, ProviderRuntimeSupervisor,
    ProviderRuntimeSupervisorConfig, ProviderRuntimeSupervisorError, ProviderSubmitAcquire,
    ProviderSubmitFailureKind, ProviderSubmitIntent, ProviderSubmitIntentState,
    ProviderSubmitIterationCommand, ProviderSubmitOrchestrator, ProviderSubmitOrchestratorError,
    ProviderSubmitOutcome, ProviderSubmitProjectionError, ProviderSubmitProjector,
    ProviderSubmitRecoveryFence, ProviderSubmitRecoveryLease, ProviderSubmitRecoveryWork,
    ProviderSubmitRun, ProviderSubmitService, ProviderSubmitServiceConfig, ProviderSubmitStart,
    ProviderSubmitWork, ProviderTaskClaimScope, ProviderTaskDeadlineStore, ProviderTaskLease,
    ProviderTaskObservation, ProviderTaskObservationOutcome, ProviderTaskObservationSource,
    ProviderTaskState, ProviderTaskStore, ProviderTaskStoreError, RemoteTaskAttach,
    RemoteTaskSubmitFailure, RemoteTaskSubmitReceipt, RemoteTaskSubmitReservation,
    StagedProviderArtifact, VerifiedCallbackWakeup,
    admission::WorkLease,
    database::{connect_test_pool_with_search_path, run_migrations},
};
use image::{DynamicImage, ImageFormat, Rgba, RgbaImage};
use image_cli_runtime::{RecoverableAttemptWorkspace, WorkingDirectory};
use image_provider_dreamina_cli::{
    ADAPTER_REVISION as DREAMINA_ADAPTER_REVISION, DREAMINA_IMAGE_GENERATION_OPERATION_V1,
    DREAMINA_SUBMIT_COMMAND_SCHEMA, PROVIDER_ID as DREAMINA_PROVIDER_ID,
};
use image_provider_sdk::{
    ArtifactMetadata, ArtifactSink, ArtifactSinkError, ArtifactSinkErrorKind,
    CanonicalCommandPayload, DurableArtifactManifest, DurableArtifactRef, EffectCertainty,
    OutputSlot, PendingOperation, PollObservation, ProviderCommandIdentity, ProviderFailure,
    ProviderFailureClass, ProviderRequestId, RemoteOperationRef, RetryDirective,
    SingleOutputCommand,
};
use image_provider_test_support::{
    OutputPlan, PollStep, ScriptedFakeProvider, SubmitStep, TestPayload,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const PROFILE_ID: Uuid = Uuid::from_u128(0x1710);
const POOL_ID: Uuid = Uuid::from_u128(0x1720);
const ACCOUNT_ID: Uuid = Uuid::from_u128(0x1730);
const POLICY_ID: Uuid = Uuid::from_u128(0x1740);
const DREAMINA_PROFILE_ID: Uuid = Uuid::from_u128(0x2710);
const DREAMINA_POOL_ID: Uuid = Uuid::from_u128(0x2720);
const DREAMINA_ACCOUNT_ID: Uuid = Uuid::from_u128(0x2730);
const DREAMINA_POLICY_ID: Uuid = Uuid::from_u128(0x2740);
const DREAMINA_PROFILE_KEY: &str = "dreamina-image-runtime-test";
const DREAMINA_CREDENTIAL_REF: &str = "test-vault.dreamina.1";

#[derive(Clone, Copy, Default)]
struct TestSubmitProjector {
    reject_fresh: bool,
}

impl ProviderSubmitProjector<ScriptedFakeProvider> for TestSubmitProjector {
    fn project_fresh(
        &self,
        lease: &ExecutorSubmissionLease,
        context: &ExecutorLaunchContext,
    ) -> Result<SingleOutputCommand<TestPayload>, ProviderSubmitProjectionError> {
        if self.reject_fresh {
            return Err(ProviderSubmitProjectionError::InvalidSourceCommand);
        }
        if lease.output_index != context.output_index()
            || lease.command_schema != context.command_schema()
            || lease.command_hash != context.command_hash()
        {
            return Err(ProviderSubmitProjectionError::ContractMismatch);
        }
        test_submit_command(
            u32::try_from(lease.output_index)
                .map_err(|_| ProviderSubmitProjectionError::OutputOutOfRange)?,
            1,
            lease.command_hash.clone(),
        )
    }

    fn project_recovery(
        &self,
        lease: &ProviderSubmitRecoveryLease,
    ) -> Result<SingleOutputCommand<TestPayload>, ProviderSubmitProjectionError> {
        test_submit_command(
            lease.intent.output_index,
            lease.intent.output_total,
            lease.context().command_hash().to_owned(),
        )
    }
}

fn test_submit_command(
    output_index: u32,
    output_total: u32,
    source_command_sha256: String,
) -> Result<SingleOutputCommand<TestPayload>, ProviderSubmitProjectionError> {
    let output = OutputSlot::new(output_index, output_total)
        .map_err(|_| ProviderSubmitProjectionError::OutputOutOfRange)?;
    SingleOutputCommand::new(
        output,
        TestPayload::bound_to(b"provider-test-payload".to_vec(), source_command_sha256),
    )
    .map_err(|_| ProviderSubmitProjectionError::InvalidSourceCommand)
}

#[derive(Clone)]
struct BlockingPendingPollDriver {
    started: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
    calls: Arc<AtomicUsize>,
}

impl BlockingPendingPollDriver {
    fn new() -> Self {
        Self {
            started: Arc::new(tokio::sync::Semaphore::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl ProviderPollDriver for BlockingPendingPollDriver {
    fn provider_id(&self) -> &'static str {
        "provider-test"
    }

    async fn poll<S: ArtifactSink>(
        &self,
        _call: &ProviderPollDriverCall,
        _sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.add_permits(1);
        self.release
            .acquire()
            .await
            .expect("blocking provider release")
            .forget();
        Ok(PollObservation::Pending {
            next_poll_after_ms: Some(10_000),
        })
    }
}

macro_rules! submit_failure {
    ($store:expr, $lease:expr, $kind:expr, $event:expr, $error:expr $(,)?) => {
        submit_failure_request($store, $lease, $kind, $event, $error).await?
    };
}

macro_rules! submit_receipt {
    ($store:expr, $lease:expr, $operation:expr, $event:expr $(,)?) => {
        submit_receipt_request($store, $lease, $operation, $event).await?
    };
}

macro_rules! attach_request {
    ($store:expr, $lease:expr, $operation:expr, $event:expr $(,)?) => {
        bound_attach_request($store, $lease, $operation, $event).await?
    };
}

#[cfg(unix)]
#[path = "postgres_provider_tasks/gated_submit.rs"]
mod gated_submit;

#[tokio::test]
async fn active_runtime_profile_freezes_scope_and_capacity() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let profile = store
            .load_active_runtime_profile("provider-task-profile")
            .await
            .map_err(debug_error)?;

        require(
            profile.execution_profile_id() == PROFILE_ID
                && profile.profile_key() == "provider-task-profile"
                && profile.provider_id() == "provider-test"
                && profile.command_schema() == "provider-command-v1"
                && profile.operation_id() == "images.generations"
                && profile.operation_descriptor_revision() == "provider-test/images.generations/v1"
                && profile.operation_descriptor_sha256_v1() == "2".repeat(64)
                && profile.idempotency_mode() == "submission_bound"
                && profile.adapter_revision() == "provider-test-adapter-v1"
                && profile.credential_pool_id() == POOL_ID
                && profile.provider_account_id() == ACCOUNT_ID
                && profile.credential_ref() == "test-vault.provider-task.1"
                && profile.credential_revision() == 1
                && profile.credential_auth_sha256() == "1".repeat(64)
                && profile.resource_policy_id() == POLICY_ID
                && profile.resource_policy_revision() == 1
                && profile.max_in_flight() == 100
                && profile.claim_scope() == claim_scope(),
            format!("unexpected active runtime profile: {profile:?}"),
        )?;
        let debug = format!("{profile:?}");
        require(
            !debug.contains("test-vault.provider-task.1") && !debug.contains(&"1".repeat(64)),
            "runtime profile Debug leaked credential identity",
        )
    }
    .await;
    let cleanup = database.cleanup().await;
    combine(result, cleanup)
}

#[tokio::test]
async fn runtime_profile_rejects_each_disabled_dependency() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let cases = [
            (
                "UPDATE provider_execution_profiles SET state = 'disabled' WHERE execution_profile_id = $1",
                "UPDATE provider_execution_profiles SET state = 'enabled' WHERE execution_profile_id = $1",
                PROFILE_ID,
            ),
            (
                "UPDATE provider_credential_pools SET state = 'disabled' WHERE credential_pool_id = $1",
                "UPDATE provider_credential_pools SET state = 'enabled' WHERE credential_pool_id = $1",
                POOL_ID,
            ),
            (
                "UPDATE provider_accounts SET state = 'disabled' WHERE provider_account_id = $1",
                "UPDATE provider_accounts SET state = 'enabled' WHERE provider_account_id = $1",
                ACCOUNT_ID,
            ),
            (
                "UPDATE executor_resource_policies SET state = 'disabled' WHERE resource_policy_id = $1",
                "UPDATE executor_resource_policies SET state = 'enabled' WHERE resource_policy_id = $1",
                POLICY_ID,
            ),
        ];

        for (disable, enable, id) in cases {
            sqlx::query(disable)
                .bind(id)
                .execute(&database.pool)
                .await
                .map_err(debug_error)?;
            require(
                matches!(
                    store
                        .load_active_runtime_profile("provider-task-profile")
                        .await,
                    Err(ProviderTaskStoreError::NotFound)
                ),
                format!("disabled profile dependency {id} remained runnable"),
            )?;
            sqlx::query(enable)
                .bind(id)
                .execute(&database.pool)
                .await
                .map_err(debug_error)?;
        }

        store
            .load_active_runtime_profile("provider-task-profile")
            .await
            .map(|_| ())
            .map_err(debug_error)
    }
    .await;
    let cleanup = database.cleanup().await;
    combine(result, cleanup)
}

#[tokio::test]
async fn runtime_readiness_projects_configured_active_draining_and_withdrawn_states() -> TestResult
{
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        require_profile_status(
            &store,
            ProviderProfileReadinessStatus::Configured,
            (0, 0, 0, 0),
        )
        .await?;

        let submit = store
            .register_runtime(
                &runtime_registration(ProviderRuntimeRole::Submit, "submit-a"),
                30_000,
            )
            .await
            .map_err(debug_error)?;
        let future = database_now(&database.pool).await? + 60_000;
        require(
            sqlx::query(
                r#"
                UPDATE provider_runtime_leases
                SET heartbeat_at_ms = $2, lease_expires_at_ms = $2 + 30_000,
                    updated_at_ms = $2
                WHERE runtime_id = $1
                "#,
            )
            .bind(submit.runtime_id)
            .bind(future)
            .execute(&database.pool)
            .await
            .is_err(),
            "runtime lease accepted a future heartbeat outside the readiness store",
        )?;
        require_profile_status(
            &store,
            ProviderProfileReadinessStatus::Configured,
            (1, 0, 0, 0),
        )
        .await?;

        let poll = store
            .register_runtime(
                &runtime_registration(ProviderRuntimeRole::Poll, "poll-a"),
                30_000,
            )
            .await
            .map_err(debug_error)?;
        require_profile_status(&store, ProviderProfileReadinessStatus::Active, (1, 1, 0, 0))
            .await?;

        let draining = store
            .begin_runtime_drain(&poll, 30_000)
            .await
            .map_err(debug_error)?;
        require(
            draining.state == gpt_image_2_gateway::ProviderRuntimeLeaseState::Draining,
            "poll runtime did not enter draining state",
        )?;
        let draining = store
            .heartbeat_runtime(&poll, 30_000)
            .await
            .map_err(debug_error)?;
        require(
            draining.state == gpt_image_2_gateway::ProviderRuntimeLeaseState::Draining,
            "draining heartbeat reactivated the runtime",
        )?;
        require_profile_status(
            &store,
            ProviderProfileReadinessStatus::Draining,
            (1, 0, 0, 1),
        )
        .await?;

        store
            .withdraw_runtime(&draining)
            .await
            .map_err(debug_error)?;
        store.withdraw_runtime(&submit).await.map_err(debug_error)?;
        require_profile_status(
            &store,
            ProviderProfileReadinessStatus::Configured,
            (0, 0, 0, 0),
        )
        .await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn runtime_readiness_summary_is_bounded_and_preserves_status_precedence() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        require_profile_summary(
            &store,
            ProviderProfileReadinessSummary {
                configured: 1,
                active: 0,
                draining: 0,
                blocked: 0,
            },
        )
        .await?;

        let submit = store
            .register_runtime(
                &runtime_registration(ProviderRuntimeRole::Submit, "summary-submit"),
                30_000,
            )
            .await
            .map_err(debug_error)?;
        let poll = store
            .register_runtime(
                &runtime_registration(ProviderRuntimeRole::Poll, "summary-poll"),
                30_000,
            )
            .await
            .map_err(debug_error)?;
        require_profile_summary(
            &store,
            ProviderProfileReadinessSummary {
                configured: 0,
                active: 1,
                draining: 0,
                blocked: 0,
            },
        )
        .await?;

        let poll = store
            .begin_runtime_drain(&poll, 30_000)
            .await
            .map_err(debug_error)?;
        require_profile_summary(
            &store,
            ProviderProfileReadinessSummary {
                configured: 0,
                active: 0,
                draining: 1,
                blocked: 0,
            },
        )
        .await?;

        sqlx::query(
            "UPDATE provider_execution_profiles SET state = 'disabled' WHERE execution_profile_id = $1",
        )
        .bind(PROFILE_ID)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        require_profile_summary(
            &store,
            ProviderProfileReadinessSummary {
                configured: 0,
                active: 0,
                draining: 0,
                blocked: 1,
            },
        )
        .await?;

        store.withdraw_runtime(&poll).await.map_err(debug_error)?;
        store.withdraw_runtime(&submit).await.map_err(debug_error)
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn runtime_readiness_blocks_disabled_profile_dependencies() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        sqlx::query(
            "UPDATE provider_accounts SET state = 'disabled' WHERE provider_account_id = $1",
        )
        .bind(ACCOUNT_ID)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        require_profile_status(
            &store,
            ProviderProfileReadinessStatus::Blocked,
            (0, 0, 0, 0),
        )
        .await?;
        require(
            store
                .register_runtime(
                    &runtime_registration(ProviderRuntimeRole::Submit, "blocked-submit"),
                    30_000,
                )
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "disabled account admitted a runtime lease",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn expired_runtime_is_not_live_and_exact_identity_can_register_again() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let registration = runtime_registration(ProviderRuntimeRole::Submit, "expiring-submit");
        let expired = store
            .register_runtime(&registration, 5)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        require(
            store.heartbeat_runtime(&expired, 30_000).await
                == Err(ProviderTaskStoreError::StaleLease),
            "expired runtime renewed its lease",
        )?;
        require_profile_status(
            &store,
            ProviderProfileReadinessStatus::Configured,
            (0, 0, 0, 0),
        )
        .await?;
        let replacement = store
            .register_runtime(&registration, 30_000)
            .await
            .map_err(debug_error)?;
        require(
            replacement.runtime_id == registration.runtime_id
                && replacement.heartbeat_at_ms > expired.heartbeat_at_ms,
            "expired exact runtime identity did not register as a new lease",
        )?;
        store
            .withdraw_runtime(&replacement)
            .await
            .map_err(debug_error)
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_runtime_registration_fences_duplicate_owner() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let first = runtime_registration(ProviderRuntimeRole::Poll, "one-owner");
        let second = ProviderRuntimeRegistration {
            runtime_id: Uuid::new_v4(),
            ..first.clone()
        };
        let (left, right) = tokio::join!(
            store.register_runtime(&first, 30_000),
            store.register_runtime(&second, 30_000),
        );
        let results = [left, right];
        require(
            results.iter().filter(|result| result.is_ok()).count() == 1
                && results
                    .iter()
                    .filter(|result| **result == Err(ProviderTaskStoreError::Conflict))
                    .count()
                    == 1,
            format!("duplicate owner registration was not exactly fenced: {results:?}"),
        )?;
        let winner = results
            .into_iter()
            .find_map(Result::ok)
            .ok_or_else(|| "runtime owner had no winner".to_string())?;
        store.withdraw_runtime(&winner).await.map_err(debug_error)
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn runtime_supervisor_stops_when_postgres_lease_authority_is_lost() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let registration =
            runtime_registration(ProviderRuntimeRole::Submit, "postgres-supervisor-loss");
        let supervisor = ProviderRuntimeSupervisor::new(
            store.clone(),
            registration.clone(),
            ProviderRuntimeSupervisorConfig {
                lease_ms: 5_000,
                heartbeat_interval: Duration::from_millis(20),
            },
        );
        let started = Arc::new(tokio::sync::Semaphore::new(0));
        let stopped = Arc::new(AtomicBool::new(false));
        let run = tokio::spawn({
            let started = Arc::clone(&started);
            let stopped = Arc::clone(&stopped);
            async move {
                supervisor
                    .run_until_shutdown(std::future::pending(), |shutdown| async move {
                        started.add_permits(1);
                        shutdown.wait().await;
                        stopped.store(true, Ordering::SeqCst);
                        Ok::<(), std::convert::Infallible>(())
                    })
                    .await
            }
        });
        started.acquire().await.map_err(debug_error)?.forget();
        sqlx::query(
            r#"
            UPDATE provider_runtime_leases
            SET lease_expires_at_ms = heartbeat_at_ms + 1
            WHERE runtime_id = $1
            "#,
        )
        .bind(registration.runtime_id)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(5)).await;

        let outcome = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .map_err(|_| "runtime supervisor did not stop after lease loss".to_owned())?
            .map_err(debug_error)?;
        require(
            outcome
                == Err(ProviderRuntimeSupervisorError::Heartbeat(
                    ProviderTaskStoreError::StaleLease,
                )),
            format!("runtime supervisor did not report lease loss: {outcome:?}"),
        )?;
        require(
            stopped.load(Ordering::SeqCst),
            "runtime did not receive shutdown after lease loss",
        )?;

        let replacement = store
            .register_runtime(
                &ProviderRuntimeRegistration {
                    runtime_id: Uuid::new_v4(),
                    ..registration
                },
                5_000,
            )
            .await
            .map_err(debug_error)?;
        store
            .withdraw_runtime(&replacement)
            .await
            .map_err(debug_error)
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn provider_pollerd_runs_fake_dreamina_cli_against_real_postgres_and_drains() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_dreamina_execution_profile(&database.pool).await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let executor = seed_attached_remote_task_for_runtime_profile(
            &database.pool,
            &store,
            "provider-pollerd-worker",
            "provider-pollerd",
            30_000,
            0,
            DREAMINA_PROFILE_ID,
            DREAMINA_PROVIDER_ID,
            "dreamina-image-5.0",
            DREAMINA_SUBMIT_COMMAND_SCHEMA,
            DREAMINA_ADAPTER_REVISION,
        )
        .await?;

        let root = tempfile::tempdir().map_err(debug_error)?;
        let account_home = root.path().join("account-home");
        let workspace = root.path().join("workspace");
        let artifact_root = root.path().join("artifacts");
        for path in [&account_home, &workspace, &artifact_root] {
            fs::create_dir(path).map_err(debug_error)?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
        }
        let bytes = png_bytes([31, 41, 59, 255]);
        fs::write(account_home.join("source.png"), &bytes).map_err(debug_error)?;
        let executable = root.path().join("dreamina");
        let script = br#"#!/bin/sh
download=
submit=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --submit_id) shift; submit=$1 ;;
    --download_dir) shift; download=$1 ;;
  esac
  shift
done
printf called > "$HOME/query-called"
/bin/cp "$HOME/source.png" "$download/result.png"
printf '{"submit_id":"%s","gen_status":"success"}' "$submit"
"#;
        fs::write(&executable, script).map_err(debug_error)?;
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).map_err(debug_error)?;
        let executable_sha256 = hex::encode(Sha256::digest(script));
        let log_path = root.path().join("provider-pollerd.log");
        let log = File::create(&log_path).map_err(debug_error)?;
        let stderr = log.try_clone().map_err(debug_error)?;
        let database_url = env::var("TEST_DATABASE_URL").map_err(debug_error)?;
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_provider-pollerd"));
        command
            .env_clear()
            .env("DATABASE_URL", database_url)
            .env("GATEWAY_DATABASE_SCHEMA", &database.schema)
            .env("GATEWAY_ARTIFACT_ROOT", &artifact_root)
            .env("PROVIDER_POLLER_ACTIVATION", "dreamina-image-v1")
            .env("PROVIDER_POLLER_PROFILE_KEY", DREAMINA_PROFILE_KEY)
            .env("PROVIDER_POLLER_OWNER", "provider-pollerd-process-test")
            .env(
                "PROVIDER_POLLER_CREDENTIAL_POOL_ID",
                DREAMINA_POOL_ID.to_string(),
            )
            .env(
                "PROVIDER_POLLER_ACCOUNT_ID",
                DREAMINA_ACCOUNT_ID.to_string(),
            )
            .env("PROVIDER_POLLER_CREDENTIAL_REF", DREAMINA_CREDENTIAL_REF)
            .env("PROVIDER_POLLER_CREDENTIAL_REVISION", "1")
            .env("PROVIDER_POLLER_CREDENTIAL_AUTH_SHA256", "c".repeat(64))
            .env("PROVIDER_POLLER_ACCOUNT_HOME", &account_home)
            .env("PROVIDER_POLLER_WORKSPACE_ROOT", &workspace)
            .env("PROVIDER_POLLER_EXECUTABLE", &executable)
            .env("PROVIDER_POLLER_EXECUTABLE_SHA256", &executable_sha256)
            .env("PROVIDER_POLLER_MAX_ARTIFACT_BYTES", "1048576")
            .env("PROVIDER_POLLER_MAX_MATERIALIZATIONS", "1")
            .env("PROVIDER_POLLER_LEASE_MS", "5000")
            .env("PROVIDER_POLLER_HEARTBEAT_INTERVAL_MS", "1000")
            .env("PROVIDER_POLLER_IDLE_DELAY_MS", "10")
            .env("PROVIDER_POLLER_ERROR_BASE_DELAY_MS", "10")
            .env("PROVIDER_POLLER_ERROR_MAX_DELAY_MS", "100")
            .env("PROVIDER_POLLER_SHUTDOWN_DRAIN_MS", "5000")
            .env("PROVIDER_POLLER_CLI_WALL_TIMEOUT_MS", "5000")
            .env("PROVIDER_POLLER_CLI_TERMINATION_GRACE_MS", "50")
            .env("PROVIDER_POLLER_RUNTIME_LEASE_MS", "5000")
            .env("PROVIDER_POLLER_RUNTIME_HEARTBEAT_INTERVAL_MS", "250")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true);
        command.process_group(0);
        let mut child = command.spawn().map_err(debug_error)?;
        let pid = child
            .id()
            .ok_or_else(|| "provider-pollerd PID unavailable".to_string())?;

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = child.try_wait().map_err(debug_error)? {
                    return Err(format!(
                        "provider-pollerd exited before resolving work with {status}: {}",
                        read_test_log(&log_path)
                    ));
                }
                let state: Option<String> = sqlx::query_scalar(
                    "SELECT state FROM provider_remote_tasks WHERE submission_id = $1",
                )
                .bind(executor.submission_id)
                .fetch_optional(&database.pool)
                .await
                .map_err(debug_error)?;
                if state.as_deref() == Some("artifact_ready") {
                    return Ok::<(), String>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            format!(
                "provider-pollerd did not resolve fake Dreamina work: {}",
                read_test_log(&log_path)
            )
        })??;
        tokio::time::sleep(Duration::from_millis(600)).await;
        let runtime: (String, bool) = sqlx::query_as(
            r#"
            SELECT state, heartbeat_at_ms > created_at_ms
            FROM provider_runtime_leases
            WHERE execution_profile_id = $1 AND runtime_role = 'poll'
              AND runtime_owner = 'provider-pollerd-process-test'
            "#,
        )
        .bind(DREAMINA_PROFILE_ID)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            runtime == ("active".to_owned(), true),
            format!("provider-pollerd runtime lease was not active and heartbeating: {runtime:?}"),
        )?;

        signal_process_group(pid, libc::SIGTERM)?;
        let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .map_err(|_| {
                format!(
                    "provider-pollerd did not drain after SIGTERM: {}",
                    read_test_log(&log_path)
                )
            })?
            .map_err(debug_error)?;
        require(
            status.success(),
            format!(
                "provider-pollerd SIGTERM exit was {status}: {}",
                read_test_log(&log_path)
            ),
        )?;
        require(
            runtime_lease_count(
                &database.pool,
                DREAMINA_PROFILE_ID,
                "poll",
                "provider-pollerd-process-test",
            )
            .await?
                == 0,
            "provider-pollerd did not withdraw its runtime lease",
        )?;

        let authority = executor.executor_execution_id.simple().to_string();
        let object = artifact_root
            .join("executor-objects")
            .join(&authority[..2])
            .join(&authority);
        require(
            fs::read(object).map_err(debug_error)? == bytes,
            "provider-pollerd did not preserve exact Dreamina artifact bytes",
        )?;
        require(
            account_home.join("query-called").is_file(),
            "provider-pollerd did not invoke the fake Dreamina CLI",
        )?;
        require(
            fs::read_dir(&workspace).map_err(debug_error)?.all(|entry| {
                !entry
                    .map(|entry| {
                        entry
                            .file_name()
                            .as_encoded_bytes()
                            .starts_with(b".dreamina-poll-")
                    })
                    .unwrap_or(false)
            }),
            "provider-pollerd left an attempt directory after success",
        )?;
        let logs = read_test_log(&log_path);
        require(
            logs.contains("provider-pollerd started")
                && logs.contains("provider-pollerd stopped")
                && !logs.contains(DREAMINA_CREDENTIAL_REF)
                && !logs.contains(&"c".repeat(64))
                && !logs.contains(account_home.to_str().unwrap_or_default()),
            format!("provider-pollerd diagnostics were incomplete or leaked identity: {logs}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn provider_submitd_drains_fake_dreamina_submit_and_restarts_without_resubmit() -> TestResult
{
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_dreamina_execution_profile(&database.pool).await?;
        let work = seed_work_lease_for_runtime_profile_with_command(
            &database.pool,
            "provider-submitd-worker",
            DREAMINA_PROVIDER_ID,
            "dreamina-image-5.0",
            DREAMINA_SUBMIT_COMMAND_SCHEMA,
            json!({
                "operation": "text2image",
                "schema_version": 1,
                "prompt": "provider submit daemon process test",
                "model_version": "5.0",
                "ratio": "1:1",
                "resolution_type": "2k",
                "generate_num": 1,
                "poll": 0
            }),
        )
        .await?;
        let executor_store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = executor_store
            .prepare_and_handoff(&work, DREAMINA_PROFILE_ID)
            .await
            .map_err(debug_error)?;
        let submission_id = prepared
            .first()
            .ok_or_else(|| "provider-submitd fixture was not prepared".to_owned())?
            .submission_id;

        let root = tempfile::tempdir().map_err(debug_error)?;
        let account_home = root.path().join("account-home");
        let workspace = root.path().join("workspace");
        let journal = root.path().join("journal");
        for path in [&account_home, &workspace, &journal] {
            fs::create_dir(path).map_err(debug_error)?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
        }
        let executable = root.path().join("dreamina");
        let script = br#"#!/bin/sh
printf 'invoked\n' >> "$HOME/submit-invocations"
printf started > "$HOME/submit-started"
/bin/sleep 1
printf '{"submit_id":"dreamina-submitd-operation-1","gen_status":"querying"}'
"#;
        fs::write(&executable, script).map_err(debug_error)?;
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).map_err(debug_error)?;
        let executable_sha256 = hex::encode(Sha256::digest(script));
        let runner = std::path::Path::new(env!("CARGO_BIN_EXE_remote-submit-runner"));
        let runner_sha256 = hex::encode(Sha256::digest(fs::read(runner).map_err(debug_error)?));
        let database_url = env::var("TEST_DATABASE_URL").map_err(debug_error)?;
        let build_command = |log_path: &std::path::Path| -> TestResult<tokio::process::Command> {
            let log = File::create(log_path).map_err(debug_error)?;
            let stderr = log.try_clone().map_err(debug_error)?;
            let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_provider-submitd"));
            command
                .env_clear()
                .env("DATABASE_URL", &database_url)
                .env("GATEWAY_DATABASE_SCHEMA", &database.schema)
                .env("PROVIDER_SUBMITTER_ACTIVATION", "dreamina-image-submit-v1")
                .env("PROVIDER_SUBMITTER_PROFILE_KEY", DREAMINA_PROFILE_KEY)
                .env(
                    "PROVIDER_SUBMITTER_OWNER_PREFIX",
                    "provider-submitd-process-test",
                )
                .env(
                    "PROVIDER_SUBMITTER_CREDENTIAL_POOL_ID",
                    DREAMINA_POOL_ID.to_string(),
                )
                .env(
                    "PROVIDER_SUBMITTER_ACCOUNT_ID",
                    DREAMINA_ACCOUNT_ID.to_string(),
                )
                .env("PROVIDER_SUBMITTER_CREDENTIAL_REF", DREAMINA_CREDENTIAL_REF)
                .env("PROVIDER_SUBMITTER_CREDENTIAL_REVISION", "1")
                .env("PROVIDER_SUBMITTER_CREDENTIAL_AUTH_SHA256", "c".repeat(64))
                .env("PROVIDER_SUBMITTER_ACCOUNT_HOME", &account_home)
                .env("PROVIDER_SUBMITTER_WORKSPACE_ROOT", &workspace)
                .env("PROVIDER_SUBMITTER_JOURNAL_ROOT", &journal)
                .env("PROVIDER_SUBMITTER_EXECUTABLE", &executable)
                .env("PROVIDER_SUBMITTER_EXECUTABLE_SHA256", &executable_sha256)
                .env("PROVIDER_SUBMITTER_RUNNER", runner)
                .env("PROVIDER_SUBMITTER_RUNNER_SHA256", &runner_sha256)
                .env("PROVIDER_SUBMITTER_PROVIDER_TIMEOUT_MS", "10000")
                .env("PROVIDER_SUBMITTER_EXECUTOR_LEASE_MS", "10000")
                .env("PROVIDER_SUBMITTER_RECOVERY_LEASE_MS", "10000")
                .env("PROVIDER_SUBMITTER_HEARTBEAT_INTERVAL_MS", "1000")
                .env("PROVIDER_SUBMITTER_RECOVERY_RETRY_MS", "10")
                .env("PROVIDER_SUBMITTER_IDLE_DELAY_MS", "10")
                .env("PROVIDER_SUBMITTER_ERROR_BASE_DELAY_MS", "10")
                .env("PROVIDER_SUBMITTER_ERROR_MAX_DELAY_MS", "100")
                .env("PROVIDER_SUBMITTER_SHUTDOWN_DRAIN_MS", "5000")
                .env("PROVIDER_SUBMITTER_CLI_WALL_TIMEOUT_MS", "5000")
                .env("PROVIDER_SUBMITTER_CLI_TERMINATION_GRACE_MS", "50")
                .env("PROVIDER_SUBMITTER_RUNTIME_LEASE_MS", "5000")
                .env("PROVIDER_SUBMITTER_RUNTIME_HEARTBEAT_INTERVAL_MS", "250")
                .env("RUST_LOG", "info")
                .stdin(Stdio::null())
                .stdout(Stdio::from(log))
                .stderr(Stdio::from(stderr))
                .kill_on_drop(true);
            command.process_group(0);
            Ok(command)
        };

        let first_log = root.path().join("provider-submitd-first.log");
        let mut first = build_command(&first_log)?.spawn().map_err(debug_error)?;
        let first_pid = first
            .id()
            .ok_or_else(|| "provider-submitd PID unavailable".to_owned())?;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = first.try_wait().map_err(debug_error)? {
                    return Err(format!(
                        "provider-submitd exited before fake submit started with {status}: {}",
                        read_test_log(&first_log)
                    ));
                }
                if account_home.join("submit-started").is_file() {
                    return Ok::<(), String>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            format!(
                "provider-submitd did not invoke fake Dreamina submit: {}",
                read_test_log(&first_log)
            )
        })??;
        tokio::time::sleep(Duration::from_millis(600)).await;
        let runtime: (String, bool) = sqlx::query_as(
            r#"
            SELECT state, heartbeat_at_ms > created_at_ms
            FROM provider_runtime_leases
            WHERE execution_profile_id = $1 AND runtime_role = 'submit'
              AND runtime_owner = 'provider-submitd-process-test'
            "#,
        )
        .bind(DREAMINA_PROFILE_ID)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            runtime == ("active".to_owned(), true),
            format!("provider-submitd runtime lease was not active and heartbeating: {runtime:?}"),
        )?;

        signal_process(first_pid, libc::SIGTERM)?;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(status) = first.try_wait().map_err(debug_error)? {
                    return Err(format!(
                        "provider-submitd exited before publishing drain state with {status}: {}",
                        read_test_log(&first_log)
                    ));
                }
                let state: Option<String> = sqlx::query_scalar(
                    r#"
                    SELECT state
                    FROM provider_runtime_leases
                    WHERE execution_profile_id = $1 AND runtime_role = 'submit'
                      AND runtime_owner = 'provider-submitd-process-test'
                    "#,
                )
                .bind(DREAMINA_PROFILE_ID)
                .fetch_optional(&database.pool)
                .await
                .map_err(debug_error)?;
                if state.as_deref() == Some("draining") {
                    return Ok::<(), String>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            format!(
                "provider-submitd did not publish draining before lane shutdown: {}",
                read_test_log(&first_log)
            )
        })??;
        let first_status = tokio::time::timeout(Duration::from_secs(5), first.wait())
            .await
            .map_err(|_| {
                format!(
                    "provider-submitd did not drain in-flight submit: {}",
                    read_test_log(&first_log)
                )
            })?
            .map_err(debug_error)?;
        require(
            first_status.success(),
            format!(
                "provider-submitd in-flight drain exited with {first_status}: {}",
                read_test_log(&first_log)
            ),
        )?;
        require(
            runtime_lease_count(
                &database.pool,
                DREAMINA_PROFILE_ID,
                "submit",
                "provider-submitd-process-test",
            )
            .await?
                == 0,
            "provider-submitd did not withdraw its first runtime lease",
        )?;
        let attached: (String, String) = sqlx::query_as(
            "SELECT state, remote_operation_id FROM provider_remote_tasks WHERE submission_id = $1",
        )
        .bind(submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            attached
                == (
                    "provider_waiting".to_owned(),
                    "dreamina-submitd-operation-1".to_owned(),
                ),
            format!("provider-submitd did not durably attach fake submit: {attached:?}"),
        )?;

        let second_log = root.path().join("provider-submitd-second.log");
        let mut second = build_command(&second_log)?.spawn().map_err(debug_error)?;
        let second_pid = second
            .id()
            .ok_or_else(|| "restarted provider-submitd PID unavailable".to_owned())?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = second.try_wait().map_err(debug_error)? {
                    return Err(format!(
                        "restarted provider-submitd exited before shutdown with {status}: {}",
                        read_test_log(&second_log)
                    ));
                }
                if read_test_log(&second_log).contains("provider-submitd started") {
                    return Ok::<(), String>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "restarted provider-submitd did not become ready".to_owned())??;
        tokio::time::sleep(Duration::from_millis(100)).await;
        signal_process(second_pid, libc::SIGTERM)?;
        let second_status = tokio::time::timeout(Duration::from_secs(5), second.wait())
            .await
            .map_err(|_| {
                format!(
                    "restarted provider-submitd did not drain: {}",
                    read_test_log(&second_log)
                )
            })?
            .map_err(debug_error)?;
        require(
            second_status.success(),
            format!(
                "restarted provider-submitd exited with {second_status}: {}",
                read_test_log(&second_log)
            ),
        )?;
        require(
            runtime_lease_count(
                &database.pool,
                DREAMINA_PROFILE_ID,
                "submit",
                "provider-submitd-process-test",
            )
            .await?
                == 0,
            "restarted provider-submitd did not withdraw its runtime lease",
        )?;

        let invocations =
            fs::read_to_string(account_home.join("submit-invocations")).map_err(debug_error)?;
        require(
            invocations.lines().count() == 1,
            format!("provider-submitd relaunched attached work: {invocations:?}"),
        )?;
        require(
            fs::read_dir(&workspace).map_err(debug_error)?.all(|entry| {
                !entry
                    .map(|entry| {
                        entry
                            .file_name()
                            .as_encoded_bytes()
                            .starts_with(b".provider-submit-")
                    })
                    .unwrap_or(false)
            }),
            "provider-submitd left an attempt workspace after durable attach",
        )?;
        let logs = format!(
            "{}\n{}",
            read_test_log(&first_log),
            read_test_log(&second_log)
        );
        require(
            logs.contains("provider-submitd started")
                && logs.contains("provider-submitd stopped")
                && !logs.contains(DREAMINA_CREDENTIAL_REF)
                && !logs.contains(&"c".repeat(64))
                && !logs.contains(account_home.to_str().unwrap_or_default())
                && !logs.contains(workspace.to_str().unwrap_or_default())
                && !logs.contains(journal.to_str().unwrap_or_default())
                && !logs.contains(executable.to_str().unwrap_or_default()),
            format!("provider-submitd diagnostics were incomplete or leaked identity: {logs}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn provider_poll_orchestrator_materializes_and_atomically_resolves() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let executor = seed_attached_remote_task(
            &database.pool,
            &store,
            "poll-orchestrator-worker",
            "poll-orchestrator",
            30_000,
            0,
        )
        .await?;
        let artifact_root = tempfile::tempdir().map_err(debug_error)?;
        let artifacts =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let bytes = png_bytes([10, 20, 30, 255]);
        let provider = ScriptedFakeProvider::default();
        provider.push_poll(PollStep::Complete(OutputPlan {
            chunks: bytes.chunks(7).map(<[u8]>::to_vec).collect(),
            media_type: "image/png".to_owned(),
            provider_request_id: Some(
                ProviderRequestId::new("request-poll-orchestrator-operation")
                    .map_err(debug_error)?,
            ),
        }));
        let stagers =
            FilesystemProviderArtifactStagerFactory::new(Arc::clone(&artifacts), 1024 * 1024)
                .map_err(debug_error)?;
        let orchestrator = ProviderPollOrchestrator::new(
            store,
            provider.clone(),
            stagers,
            ProviderPollOrchestratorConfig {
                scope: claim_scope(),
                owner: "poll-orchestrator-owner".to_owned(),
                lease_ms: 5_000,
                heartbeat_interval: Duration::from_secs(1),
                max_materializations: 2,
            },
        )
        .map_err(debug_error)?;

        let run = orchestrator.run_once().await.map_err(debug_error)?;
        require(
            matches!(
                run,
                ProviderPollRun::Observed(ref task)
                    if task.state == ProviderTaskState::ArtifactReady
            ),
            format!("poll orchestrator did not resolve artifact_ready: {run:?}"),
        )?;
        require(
            provider.calls().poll == 1,
            "completed poll did not invoke exactly one provider poll",
        )?;
        let authority_name = executor.executor_execution_id.simple().to_string();
        let object_path = artifact_root
            .path()
            .join("executor-objects")
            .join(&authority_name[..2])
            .join(&authority_name);
        require(
            std::fs::read(&object_path).map_err(debug_error)? == bytes,
            "real poll stager did not publish the exact provider bytes",
        )?;
        let staging_path = artifact_root
            .path()
            .join("executor-staging")
            .join(&authority_name[..2])
            .join(&authority_name);
        require(
            std::fs::read_dir(staging_path)
                .map_err(debug_error)?
                .next()
                .is_none(),
            "real poll stager left a provisional object after success",
        )?;

        let projection: (String, String, String, String, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
                SELECT task.state, execution.state, submission.state, allocation.state,
                  (SELECT COUNT(*) FROM provider_task_observations observation
                   WHERE observation.submission_id = $1
                     AND observation.source = 'poll'
                     AND observation.observed_state = 'artifact_ready'),
                  (SELECT COUNT(*) FROM executor_artifact_authorities authority
                   WHERE authority.executor_execution_id = $2),
                  (SELECT COUNT(*) FROM executor_result_manifests manifest
                   WHERE manifest.executor_execution_id = $2),
                  (SELECT COUNT(*) FROM executor_resolution_decisions decision
                   WHERE decision.submission_id = $1
                     AND decision.source = 'remote_provider_observation'),
                  (SELECT COUNT(*) FROM executor_terminal_reductions reduction
                   WHERE reduction.submission_id = $1 AND reduction.state = 'ready')
                FROM provider_remote_tasks task
                JOIN executor_executions execution
                  ON execution.executor_execution_id = task.executor_execution_id
                 AND execution.submission_id = task.submission_id
                JOIN provider_submissions submission
                  ON submission.executor_execution_id = task.executor_execution_id
                 AND submission.submission_id = task.submission_id
                JOIN executor_capacity_allocations allocation
                  ON allocation.executor_execution_id = task.executor_execution_id
                WHERE task.submission_id = $1
                "#,
        )
        .bind(executor.submission_id)
        .bind(executor.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            projection
                == (
                    "artifact_ready".to_owned(),
                    "succeeded".to_owned(),
                    "succeeded".to_owned(),
                    "released".to_owned(),
                    1,
                    1,
                    1,
                    1,
                    1,
                ),
            format!("poll orchestration did not commit one canonical success: {projection:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn provider_poll_daemon_claims_due_tasks_once_across_concurrent_lanes() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        const TASKS: usize = 6;

        let store = PostgresProviderTaskStore::new(database.pool.clone());
        for index in 0..TASKS {
            seed_attached_remote_task(
                &database.pool,
                &store,
                &format!("poll-daemon-worker-{index}"),
                &format!("poll-daemon-{index}"),
                30_000,
                0,
            )
            .await?;
        }

        let provider = ScriptedFakeProvider::default();
        for _ in 0..TASKS {
            provider.push_poll(PollStep::Pending {
                next_poll_after_ms: Some(10_000),
            });
        }
        let orchestrator = Arc::new(
            ProviderPollOrchestrator::new(
                store,
                provider.clone(),
                ManifestOnlyPollStagerFactory::default(),
                ProviderPollOrchestratorConfig {
                    scope: claim_scope(),
                    owner: "poll-daemon-owner".to_owned(),
                    lease_ms: 5_000,
                    heartbeat_interval: Duration::from_secs(1),
                    max_materializations: 2,
                },
            )
            .map_err(debug_error)?,
        );
        let daemon =
            ProviderPollDaemon::new(orchestrator, poll_daemon_config(3)).map_err(debug_error)?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let run = tokio::spawn(async move {
            daemon
                .run_until_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        wait_for_poll_observations(&database.pool, TASKS as i64).await?;
        shutdown_tx.send(()).map_err(debug_error)?;
        let report = run.await.map_err(debug_error)?.map_err(debug_error)?;

        require(
            report.observed == TASKS as u64 && report.errors == 0,
            format!("poll daemon report was not exact: {report:?}"),
        )?;
        require(
            provider.calls().poll == TASKS,
            "concurrent poll lanes invoked the provider more than once per due task",
        )?;
        let projection: (i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM provider_remote_tasks
               WHERE state = 'provider_waiting'),
              (SELECT COUNT(*) FROM provider_remote_tasks
               WHERE poll_owner IS NOT NULL OR poll_lease_expires_at_ms IS NOT NULL),
              (SELECT COUNT(*) FROM provider_task_observations
               WHERE source = 'poll' AND observed_state = 'provider_waiting'),
              (SELECT COUNT(*) FROM (
                 SELECT submission_id
                 FROM provider_task_observations
                 WHERE source = 'poll'
                 GROUP BY submission_id
                 HAVING COUNT(*) <> 1
               ) duplicates)
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            projection == (TASKS as i64, 0, TASKS as i64, 0),
            format!("concurrent poll claims did not resolve exactly once: {projection:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn two_provider_poll_daemons_hold_distinct_leases_and_resolve_once() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        for index in 0..2 {
            seed_attached_remote_task(
                &database.pool,
                &store,
                &format!("dual-poll-daemon-worker-{index}"),
                &format!("dual-poll-daemon-{index}"),
                30_000,
                0,
            )
            .await?;
        }

        let left_driver = BlockingPendingPollDriver::new();
        let right_driver = BlockingPendingPollDriver::new();
        let left = ProviderPollDaemon::new(
            Arc::new(
                ProviderPollOrchestrator::new(
                    store.clone(),
                    left_driver.clone(),
                    ManifestOnlyPollStagerFactory::default(),
                    ProviderPollOrchestratorConfig {
                        scope: claim_scope(),
                        owner: "left-poll-daemon".to_owned(),
                        lease_ms: 5_000,
                        heartbeat_interval: Duration::from_secs(1),
                        max_materializations: 1,
                    },
                )
                .map_err(debug_error)?,
            ),
            poll_daemon_config(1),
        )
        .map_err(debug_error)?;
        let right = ProviderPollDaemon::new(
            Arc::new(
                ProviderPollOrchestrator::new(
                    store,
                    right_driver.clone(),
                    ManifestOnlyPollStagerFactory::default(),
                    ProviderPollOrchestratorConfig {
                        scope: claim_scope(),
                        owner: "right-poll-daemon".to_owned(),
                        lease_ms: 5_000,
                        heartbeat_interval: Duration::from_secs(1),
                        max_materializations: 1,
                    },
                )
                .map_err(debug_error)?,
            ),
            poll_daemon_config(1),
        )
        .map_err(debug_error)?;

        let (left_shutdown_tx, left_shutdown_rx) = tokio::sync::oneshot::channel();
        let left_run = tokio::spawn(async move {
            left.run_until_shutdown(async {
                let _ = left_shutdown_rx.await;
            })
            .await
        });
        left_driver
            .started
            .acquire()
            .await
            .expect("left provider started")
            .forget();

        let (right_shutdown_tx, right_shutdown_rx) = tokio::sync::oneshot::channel();
        let right_run = tokio::spawn(async move {
            right
                .run_until_shutdown(async {
                    let _ = right_shutdown_rx.await;
                })
                .await
        });
        right_driver
            .started
            .acquire()
            .await
            .expect("right provider started")
            .forget();

        let active_leases: (i64, i64) = sqlx::query_as(
            r#"
            SELECT COUNT(*), COUNT(DISTINCT poll_owner)
            FROM provider_remote_tasks
            WHERE poll_owner IS NOT NULL
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            active_leases == (2, 2),
            format!("independent daemons did not hold distinct leases: {active_leases:?}"),
        )?;

        left_driver.release.add_permits(1);
        right_driver.release.add_permits(1);
        wait_for_poll_observations(&database.pool, 2).await?;
        left_shutdown_tx.send(()).map_err(debug_error)?;
        right_shutdown_tx.send(()).map_err(debug_error)?;
        let left_report = left_run.await.map_err(debug_error)?.map_err(debug_error)?;
        let right_report = right_run.await.map_err(debug_error)?.map_err(debug_error)?;

        require(
            left_report.observed == 1
                && right_report.observed == 1
                && left_report.errors == 0
                && right_report.errors == 0,
            format!(
                "independent daemon reports were not exact: left={left_report:?}, right={right_report:?}"
            ),
        )?;
        require(
            left_driver.calls.load(Ordering::SeqCst) == 1
                && right_driver.calls.load(Ordering::SeqCst) == 1,
            "independent daemons duplicated a provider poll",
        )?;
        let projection: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM provider_task_observations
               WHERE source = 'poll' AND observed_state = 'provider_waiting'),
              (SELECT COUNT(*) FROM provider_remote_tasks
               WHERE poll_owner IS NOT NULL OR poll_lease_expires_at_ms IS NOT NULL),
              (SELECT COUNT(*) FROM (
                 SELECT submission_id
                 FROM provider_task_observations
                 WHERE source = 'poll'
                 GROUP BY submission_id
                 HAVING COUNT(*) <> 1
               ) duplicates)
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            projection == (2, 0, 0),
            format!("independent daemon resolution was not exactly once: {projection:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn provider_poll_orchestrator_reuses_object_after_pre_authority_crash() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let postgres = PostgresProviderTaskStore::new(database.pool.clone());
        let executor = seed_attached_remote_task(
            &database.pool,
            &postgres,
            "poll-object-replay-worker",
            "poll-object-replay",
            30_000,
            0,
        )
        .await?;
        let artifact_root = tempfile::tempdir().map_err(debug_error)?;
        let artifacts =
            Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
        let bytes = png_bytes([40, 50, 60, 255]);
        let store = FailFirstArtifactPublishStore::new(postgres);

        let first_provider =
            completed_poll_provider(&bytes, "request-poll-object-replay-operation")?;
        let first = ProviderPollOrchestrator::new(
            store.clone(),
            first_provider.clone(),
            FilesystemProviderArtifactStagerFactory::new(Arc::clone(&artifacts), 1024 * 1024)
                .map_err(debug_error)?,
            ProviderPollOrchestratorConfig {
                scope: claim_scope(),
                owner: "pre-authority-crash".to_owned(),
                lease_ms: 100,
                heartbeat_interval: Duration::from_millis(20),
                max_materializations: 1,
            },
        )
        .map_err(debug_error)?;
        require(
            matches!(
                first.run_once().await,
                Err(gpt_image_2_gateway::ProviderPollOrchestratorError::Store(
                    ProviderTaskStoreError::Unavailable
                ))
            ),
            "pre-authority crash fixture did not fail after immutable object publication",
        )?;
        let authority_name = executor.executor_execution_id.simple().to_string();
        let object_path = artifact_root
            .path()
            .join("executor-objects")
            .join(&authority_name[..2])
            .join(&authority_name);
        require(
            std::fs::read(&object_path).map_err(debug_error)? == bytes,
            "pre-authority crash did not leave the exact immutable object",
        )?;
        let before_recovery: (i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM executor_artifact_authorities
               WHERE executor_execution_id = $1),
              poll_lease_expires_at_ms
            FROM provider_remote_tasks
            WHERE executor_execution_id = $1
            "#,
        )
        .bind(executor.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            before_recovery.0 == 0,
            "failed authority publication unexpectedly committed database authority",
        )?;
        sleep_until_database_time(&database.pool, before_recovery.1 + 20).await?;

        let recovery_provider =
            completed_poll_provider(&bytes, "request-poll-object-replay-operation")?;
        let recovery = ProviderPollOrchestrator::new(
            store,
            recovery_provider.clone(),
            FilesystemProviderArtifactStagerFactory::new(Arc::clone(&artifacts), 1024 * 1024)
                .map_err(debug_error)?,
            ProviderPollOrchestratorConfig {
                scope: claim_scope(),
                owner: "post-authority-recovery".to_owned(),
                lease_ms: 5_000,
                heartbeat_interval: Duration::from_secs(1),
                max_materializations: 1,
            },
        )
        .map_err(debug_error)?;
        let run = recovery.run_once().await.map_err(debug_error)?;
        require(
            matches!(
                run,
                ProviderPollRun::Observed(ref task)
                    if task.state == ProviderTaskState::ArtifactReady
            ),
            format!("immutable object replay did not converge to artifact_ready: {run:?}"),
        )?;
        require(
            first_provider.calls().poll == 1 && recovery_provider.calls().poll == 1,
            "crash replay did not perform exactly one poll per lease epoch",
        )?;
        let after_recovery: (String, i64, i64) = sqlx::query_as(
            r#"
            SELECT task.state,
              (SELECT COUNT(*) FROM executor_artifact_authorities
               WHERE executor_execution_id = $1),
              (SELECT COUNT(*) FROM executor_result_manifests
               WHERE executor_execution_id = $1)
            FROM provider_remote_tasks task
            WHERE task.executor_execution_id = $1
            "#,
        )
        .bind(executor.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            after_recovery == ("artifact_ready".to_owned(), 1, 1),
            format!("immutable object replay did not commit exactly once: {after_recovery:?}"),
        )?;
        require(
            std::fs::read(object_path).map_err(debug_error)? == bytes,
            "byte-stable recovery changed the immutable object",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn provider_poll_orchestrator_recovers_committed_authority_without_repoll() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let executor = seed_attached_remote_task(
            &database.pool,
            &store,
            "poll-recovery-worker",
            "poll-recovery",
            30_000,
            0,
        )
        .await?;
        let crashed_lease = store
            .claim_due(&claim_scope(), "crashed-poller", 100)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "crash recovery task was not claimable".to_owned())?;
        let bytes = b"committed-before-crash";
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let authority = poll_artifact_authority(
            executor.executor_execution_id,
            digest,
            bytes.len() as u64,
            "image/png",
        )?;
        store
            .publish_artifact_authority(&crashed_lease, &authority)
            .await
            .map_err(debug_error)?;
        sleep_until_database_time(&database.pool, crashed_lease.poll_lease_expires_at_ms + 20)
            .await?;

        let provider = ScriptedFakeProvider::default();
        let stagers = ManifestOnlyPollStagerFactory::default();
        let orchestrator = ProviderPollOrchestrator::new(
            store,
            provider.clone(),
            stagers.clone(),
            ProviderPollOrchestratorConfig {
                scope: claim_scope(),
                owner: "recovery-poller".to_owned(),
                lease_ms: 5_000,
                heartbeat_interval: Duration::from_secs(1),
                max_materializations: 2,
            },
        )
        .map_err(debug_error)?;

        let run = orchestrator.run_once().await.map_err(debug_error)?;
        require(
            matches!(
                run,
                ProviderPollRun::Observed(ref task)
                    if task.state == ProviderTaskState::ArtifactReady
            ),
            format!("committed authority did not recover to artifact_ready: {run:?}"),
        )?;
        require(
            provider.calls().poll == 0 && stagers.begins.load(Ordering::SeqCst) == 0,
            "authority recovery re-polled the provider or re-materialized the artifact",
        )?;

        let projection: (String, String, String, String, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT task.state, execution.state, submission.state, allocation.state,
              (SELECT COUNT(*) FROM provider_task_observations observation
               WHERE observation.submission_id = $1
                 AND observation.source = 'poll'
                 AND observation.observed_state = 'artifact_ready'),
              (SELECT COUNT(*) FROM executor_artifact_authorities authority
               WHERE authority.executor_execution_id = $2),
              (SELECT COUNT(*) FROM executor_resolution_decisions decision
               WHERE decision.submission_id = $1
                 AND decision.source = 'remote_provider_observation')
            FROM provider_remote_tasks task
            JOIN executor_executions execution
              ON execution.executor_execution_id = task.executor_execution_id
             AND execution.submission_id = task.submission_id
            JOIN provider_submissions submission
              ON submission.executor_execution_id = task.executor_execution_id
             AND submission.submission_id = task.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = task.executor_execution_id
            WHERE task.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .bind(executor.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            projection
                == (
                    "artifact_ready".to_owned(),
                    "succeeded".to_owned(),
                    "succeeded".to_owned(),
                    "released".to_owned(),
                    1,
                    1,
                    1,
                ),
            format!("authority recovery did not converge exactly once: {projection:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn atomic_submit_acquire_elects_one_dispatch_without_reserved_gap() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "atomic-submit-worker").await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let reservation = reservation_request(&lease);
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let store = store.clone();
            let reservation = reservation.clone();
            tasks.push(tokio::spawn(async move {
                store.acquire_submit(&reservation).await
            }));
        }

        let mut dispatches = 0;
        let mut non_dispatches = 0;
        for task in tasks {
            match task.await.map_err(debug_error)?.map_err(debug_error)? {
                ProviderSubmitAcquire::Dispatch(_) => dispatches += 1,
                ProviderSubmitAcquire::Busy(_) => non_dispatches += 1,
                other => {
                    return Err(format!("unexpected concurrent acquire outcome: {other:?}"));
                }
            }
        }
        require(
            dispatches == 1 && non_dispatches == 31,
            format!("dispatch election was not unique: {dispatches}/{non_dispatches}"),
        )?;

        let projection: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              COUNT(*) FILTER (WHERE intent.state = 'sending'),
              COUNT(*) FILTER (WHERE intent.state = 'reserved'),
              COUNT(recovery.submission_id)
            FROM provider_remote_submit_intents intent
            LEFT JOIN provider_submit_recoveries recovery
              ON recovery.submission_id = intent.submission_id
            WHERE intent.submission_id = $1
            "#,
        )
        .bind(lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            projection == (1, 0, 1),
            format!("atomic acquire left an invalid projection: {projection:?}"),
        )?;
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn submit_work_rejects_unjournalable_commands_before_database_acquire() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "command-boundary-worker").await?;
        for payload in [Vec::new(), vec![b'x'; 1024 * 1024 + 1]] {
            let command = SingleOutputCommand::new(
                OutputSlot::new(0, 1).map_err(debug_error)?,
                TestPayload::bound_to(payload, lease.command_hash.clone()),
            )
            .map_err(debug_error)?;
            require(
                matches!(
                    ProviderSubmitWork::<ScriptedFakeProvider>::new(&lease, command),
                    Err(ProviderSubmitOrchestratorError::InvalidWork)
                ),
                "unjournalable canonical command crossed the work boundary",
            )?;
        }
        let intent_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_remote_submit_intents WHERE submission_id = $1",
        )
        .bind(lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            intent_count == 0,
            "command validation mutated submit state before acquire",
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn submit_service_claims_projects_and_attaches_fresh_work() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_work_lease(&database.pool, "submit-service-fresh").await?;
        let executor_store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = executor_store
            .prepare_and_handoff(&work, PROFILE_ID)
            .await
            .map_err(debug_error)?;
        let submission = prepared
            .first()
            .ok_or_else(|| "fresh submit service fixture was not prepared".to_owned())?;
        let provider = ScriptedFakeProvider::default();
        provider.push_submit(SubmitStep::Pending(PendingOperation::new(
            RemoteOperationRef::new(
                "provider-test",
                submission.submission_id.to_string(),
                "submit-service-fresh-operation",
            )
            .map_err(debug_error)?,
            None,
            Some(25),
        )));
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let service = ProviderSubmitService::new(
            executor_store,
            PostgresProviderTaskStore::new(database.pool.clone()),
            provider.clone(),
            TestSubmitProjector::default(),
            submit_service_config(200),
            journal.path().join("remote-submit"),
        )
        .map_err(debug_error)?;
        let command = submit_iteration_command("submit-service-fresh");

        let run = service.run_once(&command).await.map_err(debug_error)?;
        let task_state: String =
            sqlx::query_scalar("SELECT state FROM provider_remote_tasks WHERE submission_id = $1")
                .bind(submission.submission_id)
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
        let owner: String = sqlx::query_scalar(
            "SELECT submit_owner FROM provider_remote_submit_intents WHERE submission_id = $1",
        )
        .bind(submission.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            run == ProviderSubmitRun::FreshSubmitted
                && provider.calls().submit == 1
                && task_state == "provider_waiting"
                && owner == command.owner(),
            format!("fresh submit service did not converge: {run:?}/{task_state}/{owner}"),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn submit_service_resumes_unacknowledged_fresh_claim_without_reclaiming() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_work_lease(&database.pool, "submit-service-lost-claim").await?;
        let executor_store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = executor_store
            .prepare_and_handoff(&work, PROFILE_ID)
            .await
            .map_err(debug_error)?;
        let submission = prepared
            .first()
            .ok_or_else(|| "lost claim fixture was not prepared".to_owned())?;
        let command = submit_iteration_command("submit-service-lost-claim");
        let config = submit_service_config(200);
        let claimed = executor_store
            .claim_prepared(
                &config.executor_scope,
                command.owner(),
                config.executor_lease_ms,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "lost claim fixture was not claimable".to_owned())?;

        let provider = ScriptedFakeProvider::default();
        provider.push_submit(SubmitStep::Pending(PendingOperation::new(
            RemoteOperationRef::new(
                "provider-test",
                submission.submission_id.to_string(),
                "submit-service-lost-claim-operation",
            )
            .map_err(debug_error)?,
            None,
            Some(25),
        )));
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let service = ProviderSubmitService::new(
            executor_store,
            PostgresProviderTaskStore::new(database.pool.clone()),
            provider.clone(),
            TestSubmitProjector::default(),
            config,
            journal.path().join("remote-submit"),
        )
        .map_err(debug_error)?;

        let run = service.run_once(&command).await.map_err(debug_error)?;
        let durable: (String, i64, String) = sqlx::query_as(
            r#"
            SELECT execution.state, execution.lease_epoch, intent.state
            FROM executor_executions execution
            JOIN provider_remote_submit_intents intent
              ON intent.executor_execution_id = execution.executor_execution_id
             AND intent.submission_id = execution.submission_id
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(submission.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            run == ProviderSubmitRun::FreshSubmitted
                && provider.calls().submit == 1
                && durable
                    == (
                        "provider_waiting".to_owned(),
                        claimed.executor_lease_epoch,
                        "attached".to_owned(),
                    ),
            format!("lost fresh claim was not resumed exactly once: {run:?}/{durable:?}"),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn submit_service_prioritizes_expired_recovery_before_fresh_work() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let recovery_lease =
            seed_running_submission_with_lease(&database.pool, "submit-service-recovery", 100)
                .await?;
        let provider_store = PostgresProviderTaskStore::new(database.pool.clone());
        let recovery_command = orchestrator_command(&recovery_lease);
        let acquired = provider_store
            .acquire_submit(&RemoteTaskSubmitReservation::new(
                &recovery_lease,
                format!("provider-submit-{}", recovery_lease.submission_id.simple()),
                recovery_command.output(),
                recovery_command.identity(),
                2_000,
            ))
            .await
            .map_err(debug_error)?;
        require(
            matches!(acquired, ProviderSubmitAcquire::Dispatch(_)),
            format!("recovery fixture did not elect sending: {acquired:?}"),
        )?;

        let fresh_work = seed_work_lease(&database.pool, "submit-service-waiting-fresh").await?;
        let executor_store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let fresh = executor_store
            .prepare_and_handoff(&fresh_work, PROFILE_ID)
            .await
            .map_err(debug_error)?;
        let fresh = fresh
            .first()
            .ok_or_else(|| "fresh priority fixture was not prepared".to_owned())?;
        tokio::time::sleep(Duration::from_millis(130)).await;

        let provider = ScriptedFakeProvider::default();
        provider.push_submit(SubmitStep::Pending(PendingOperation::new(
            RemoteOperationRef::new(
                "provider-test",
                recovery_lease.submission_id.to_string(),
                "submit-service-recovered-operation",
            )
            .map_err(debug_error)?,
            None,
            None,
        )));
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let service = ProviderSubmitService::new(
            executor_store,
            provider_store,
            provider.clone(),
            TestSubmitProjector::default(),
            submit_service_config(2_000),
            journal.path().join("remote-submit"),
        )
        .map_err(debug_error)?;

        let run = service
            .run_once(&submit_iteration_command("submit-service-priority"))
            .await
            .map_err(debug_error)?;
        let fresh_state: String = sqlx::query_scalar(
            "SELECT state FROM executor_executions WHERE executor_execution_id = $1",
        )
        .bind(fresh.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        let recovered_operation: String = sqlx::query_scalar(
            "SELECT remote_operation_id FROM provider_remote_tasks WHERE submission_id = $1",
        )
        .bind(recovery_lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            run == ProviderSubmitRun::RecoveryCompleted
                && provider.calls().submit == 1
                && fresh_state == "prepared"
                && recovered_operation == "submit-service-recovered-operation",
            format!(
                "submit recovery was not prioritized: {run:?}/{fresh_state}/{recovered_operation}"
            ),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn submit_service_projection_failure_terminates_before_provider_effect() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_work_lease(&database.pool, "submit-service-invalid-projection").await?;
        let executor_store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = executor_store
            .prepare_and_handoff(&work, PROFILE_ID)
            .await
            .map_err(debug_error)?;
        let submission = prepared
            .first()
            .ok_or_else(|| "projection failure fixture was not prepared".to_owned())?;
        let provider = ScriptedFakeProvider::default();
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let service = ProviderSubmitService::new(
            executor_store,
            PostgresProviderTaskStore::new(database.pool.clone()),
            provider.clone(),
            TestSubmitProjector { reject_fresh: true },
            submit_service_config(200),
            journal.path().join("remote-submit"),
        )
        .map_err(debug_error)?;

        let run = service
            .run_once(&submit_iteration_command("submit-service-invalid"))
            .await
            .map_err(debug_error)?;
        let states: (String, String, i64) = sqlx::query_as(
            r#"
            SELECT execution.state, submission.state,
                   COUNT(intent.submission_id)
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            LEFT JOIN provider_remote_submit_intents intent
              ON intent.submission_id = submission.submission_id
            WHERE execution.executor_execution_id = $1
            GROUP BY execution.state, submission.state
            "#,
        )
        .bind(submission.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            run == ProviderSubmitRun::FreshProjectionRejected
                && provider.calls().submit == 0
                && states == ("failed".to_owned(), "failed".to_owned(), 0),
            format!("projection failure crossed the provider boundary: {run:?}/{states:?}"),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn submit_service_heartbeats_fresh_authority_during_provider_timeout() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let work = seed_work_lease(&database.pool, "submit-service-heartbeat").await?;
        let executor_store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        let prepared = executor_store
            .prepare_and_handoff(&work, PROFILE_ID)
            .await
            .map_err(debug_error)?;
        let submission = prepared
            .first()
            .ok_or_else(|| "heartbeat fixture was not prepared".to_owned())?;
        let provider = ScriptedFakeProvider::default();
        provider.push_submit(SubmitStep::Never);
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let mut config = submit_service_config(150);
        config.executor_lease_ms = 80;
        config.recovery_lease_ms = 80;
        config.heartbeat_interval = Duration::from_millis(20);
        let service = ProviderSubmitService::new(
            executor_store,
            PostgresProviderTaskStore::new(database.pool.clone()),
            provider.clone(),
            TestSubmitProjector::default(),
            config,
            journal.path().join("remote-submit"),
        )
        .map_err(debug_error)?;

        let started = Instant::now();
        let run = service
            .run_once(&submit_iteration_command("submit-service-heartbeat"))
            .await
            .map_err(debug_error)?;
        let projection: (String, i64, i64) = sqlx::query_as(
            r#"
            SELECT intent.state, execution.started_at_ms,
                   execution.lease_expires_at_ms
            FROM provider_remote_submit_intents intent
            JOIN executor_executions execution
              ON execution.executor_execution_id = intent.executor_execution_id
             AND execution.submission_id = intent.submission_id
            WHERE intent.submission_id = $1
            "#,
        )
        .bind(submission.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            run == ProviderSubmitRun::FreshSubmitted
                && provider.calls().submit == 1
                && projection.0 == "outcome_unknown"
                && projection.2.saturating_sub(projection.1) >= 120
                && started.elapsed() >= Duration::from_millis(100),
            format!("fresh submit authority was not heartbeated: {run:?}/{projection:?}"),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn submit_orchestrator_dispatches_once_and_replays_without_resubmit() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "orchestrator-worker").await?;
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let journal_root = journal.path().join("remote-submit");
        let provider = ScriptedFakeProvider::default();
        provider.push_submit(SubmitStep::Pending(PendingOperation::new(
            RemoteOperationRef::new(
                "provider-test",
                lease.submission_id.to_string(),
                "remote-operation-1",
            )
            .map_err(debug_error)?,
            None,
            Some(250),
        )));
        let orchestrator = Arc::new(
            ProviderSubmitOrchestrator::new(
                PostgresProviderTaskStore::new(database.pool.clone()),
                provider.clone(),
                60_000,
                &journal_root,
            )
            .map_err(debug_error)?,
        );
        let wrong_total = ProviderSubmitWork::<ScriptedFakeProvider>::new(
            &lease,
            SingleOutputCommand::new(
                OutputSlot::new(0, 2).map_err(debug_error)?,
                TestPayload::bound_to(
                    b"provider-test-payload".to_vec(),
                    lease.command_hash.clone(),
                ),
            )
            .map_err(debug_error)?,
        )
        .map_err(debug_error)?;
        let wrong_total = orchestrator.submit(wrong_total).await;
        require(
            matches!(
                wrong_total,
                Err(ProviderSubmitOrchestratorError::Store(
                    ProviderTaskStoreError::Conflict
                ))
            ) && provider.calls().submit == 0,
            "submit work accepted an output total that disagreed with durable job identity",
        )?;

        let mut tasks = Vec::new();
        for _ in 0..32 {
            let orchestrator = Arc::clone(&orchestrator);
            let work = orchestrator_work(&lease)?;
            tasks.push(tokio::spawn(async move { orchestrator.submit(work).await }));
        }
        let mut attached = 0;
        for task in tasks {
            let observed = task.await.map_err(debug_error)?.map_err(debug_error)?;
            if matches!(observed, ProviderSubmitOutcome::Attached(_)) {
                attached += 1;
            }
        }
        require(
            attached >= 1,
            "concurrent submit did not produce an attached task",
        )?;
        let restarted = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            provider.clone(),
            120_000,
            &journal_root,
        )
        .map_err(debug_error)?;
        let replay = restarted
            .submit(orchestrator_work(&lease)?)
            .await
            .map_err(debug_error)?;
        require(
            matches!(replay, ProviderSubmitOutcome::Attached(_)) && provider.calls().submit == 1,
            format!("replay resubmitted or lost task: {replay:?}"),
        )?;
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[cfg(unix)]
#[tokio::test]
async fn submit_orchestrator_recovers_launch_prefix_before_remote_effect() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "launch-prefix-worker").await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let command = orchestrator_command(&lease);
        let reservation = RemoteTaskSubmitReservation::new(
            &lease,
            format!("provider-submit-{}", lease.submission_id.simple()),
            command.output(),
            command.identity(),
            60_000,
        );
        let acquired = store
            .acquire_submit(&reservation)
            .await
            .map_err(debug_error)?;
        let ProviderSubmitAcquire::Dispatch(authority) = acquired else {
            return Err(format!("initial acquire did not dispatch: {acquired:?}"));
        };
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let journal_root = journal.path().join("remote-submit");
        seed_remote_submit_launch_prefix(
            &journal_root,
            authority.intent(),
            authority.context(),
            &command,
        )?;
        drop(authority);

        let provider = ScriptedFakeProvider::default();
        provider.push_submit(SubmitStep::Pending(PendingOperation::new(
            RemoteOperationRef::new(
                "provider-test",
                lease.submission_id.to_string(),
                "launch-prefix-operation",
            )
            .map_err(debug_error)?,
            None,
            None,
        )));
        let recovered =
            ProviderSubmitOrchestrator::new(store, provider.clone(), 60_000, &journal_root)
                .map_err(debug_error)?
                .submit(
                    ProviderSubmitWork::<ScriptedFakeProvider>::new(&lease, command)
                        .map_err(debug_error)?,
                )
                .await
                .map_err(debug_error)?;
        require(
            matches!(recovered, ProviderSubmitOutcome::Attached(ref task)
                if task.remote_operation_id == "launch-prefix-operation")
                && provider.calls().submit == 1,
            format!("launch prefix did not recover exactly once: {recovered:?}"),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn submit_orchestrator_recovers_durable_receipt_after_database_failure() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "receipt-recovery-worker").await?;
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let journal_root = journal.path().join("remote-submit");
        let provider = ScriptedFakeProvider::default();
        provider.push_submit(SubmitStep::Pending(PendingOperation::new(
            RemoteOperationRef::new(
                "provider-test",
                lease.submission_id.to_string(),
                "durable-operation-1",
            )
            .map_err(debug_error)?,
            Some(ProviderRequestId::new("durable-request-1").map_err(debug_error)?),
            Some(125),
        )));
        sqlx::query(
            r#"
            CREATE FUNCTION reject_provider_submit_receipt()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              IF NEW.state = 'operation_known' THEN
                RAISE EXCEPTION 'injected receipt transaction failure';
              END IF;
              RETURN NEW;
            END
            $$
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            CREATE TRIGGER reject_provider_submit_receipt
            BEFORE UPDATE ON provider_remote_submit_intents
            FOR EACH ROW EXECUTE FUNCTION reject_provider_submit_receipt()
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        let first = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            provider.clone(),
            60_000,
            &journal_root,
        )
        .map_err(debug_error)?
        .submit(orchestrator_work(&lease)?)
        .await;
        require(
            matches!(first, Err(ProviderSubmitOrchestratorError::Store(_)))
                && provider.calls().submit == 1,
            format!("receipt fault was not injected after one provider call: {first:?}"),
        )?;
        let state_after_failure: String = sqlx::query_scalar(
            "SELECT state FROM provider_remote_submit_intents WHERE submission_id = $1",
        )
        .bind(lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state_after_failure == "sending",
            format!("failed receipt transaction partially committed: {state_after_failure}"),
        )?;

        sqlx::query(
            "DROP TRIGGER reject_provider_submit_receipt ON provider_remote_submit_intents",
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query("DROP FUNCTION reject_provider_submit_receipt()")
            .execute(&database.pool)
            .await
            .map_err(debug_error)?;
        let recovered = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            provider.clone(),
            60_000,
            &journal_root,
        )
        .map_err(debug_error)?
        .submit(orchestrator_work(&lease)?)
        .await
        .map_err(debug_error)?;
        require(
            matches!(recovered, ProviderSubmitOutcome::Attached(ref task)
                if task.remote_operation_id == "durable-operation-1"
                    && task.provider_request_id.as_deref() == Some("durable-request-1"))
                && provider.calls().submit == 1,
            format!("durable receipt was not imported without resubmit: {recovered:?}"),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn submit_orchestrator_does_not_resubmit_after_released_future_is_aborted() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "released-abort-worker").await?;
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let journal_root = journal.path().join("remote-submit");
        let provider = ScriptedFakeProvider::default();
        provider.push_submit(SubmitStep::Never);
        let orchestrator = Arc::new(
            ProviderSubmitOrchestrator::new(
                PostgresProviderTaskStore::new(database.pool.clone()),
                provider.clone(),
                60_000,
                &journal_root,
            )
            .map_err(debug_error)?,
        );
        let running = {
            let orchestrator = Arc::clone(&orchestrator);
            let work = orchestrator_work(&lease)?;
            tokio::spawn(async move { orchestrator.submit(work).await })
        };
        for _ in 0..100 {
            if provider.calls().submit == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        require(
            provider.calls().submit == 1,
            "provider future was not reached after durable dispatch release",
        )?;
        running.abort();
        require(
            running.await.is_err_and(|error| error.is_cancelled()),
            "in-flight submit task was not aborted at the intended crash window",
        )?;

        let recovered = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            provider.clone(),
            60_000,
            &journal_root,
        )
        .map_err(debug_error)?
        .submit(orchestrator_work(&lease)?)
        .await
        .map_err(debug_error)?;
        require(
            matches!(recovered, ProviderSubmitOutcome::AwaitingEvidence(ref intent)
                if intent.state == ProviderSubmitIntentState::Sending)
                && provider.calls().submit == 1,
            format!("released submission was retried after abort: {recovered:?}"),
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn submit_projection_rejects_sending_without_recovery() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "projection-worker").await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        store
            .reserve_submit(&reservation_request(&lease))
            .await
            .map_err(debug_error)?;
        let mut tx = database.pool.begin().await.map_err(debug_error)?;
        let now: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE provider_remote_submit_intents
            SET state = 'sending', send_started_at_ms = $2, updated_at_ms = $2
            WHERE submission_id = $1
            "#,
        )
        .bind(lease.submission_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
        require(
            tx.commit().await.is_err(),
            "sending intent committed without an active recovery row",
        )?;
        let intent = store
            .load_submit_intent(lease.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "reserved intent disappeared after rollback".to_string())?;
        require(
            intent.state == ProviderSubmitIntentState::Reserved,
            "failed projection did not roll the intent back to reserved",
        )?;

        let direct = seed_running_submission(&database.pool, "direct-projection-worker").await?;
        let mut tx = database.pool.begin().await.map_err(debug_error)?;
        let now: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO provider_remote_submit_intents
              (submission_id, executor_execution_id, provider_id, provider_account_id,
               submit_owner, submit_lease_epoch, idempotency_key, state,
               output_index, output_total,
               provider_command_sha256, execution_binding_sha256,
               provider_timeout_ms, send_started_at_ms, created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'provider-test', $3, $4, $5, $6, 'sending', 0, 1,
                    repeat('a', 64), repeat('b', 64), 60000, $7, $7, $7)
            "#,
        )
        .bind(direct.submission_id)
        .bind(direct.executor_execution_id)
        .bind(ACCOUNT_ID)
        .bind(&direct.executor_owner)
        .bind(direct.executor_lease_epoch)
        .bind(format!("provider-submit-{}", direct.submission_id.simple()))
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
        require(
            tx.commit().await.is_err(),
            "direct sending insert committed without an active recovery row",
        )?;
        let direct_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_remote_submit_intents WHERE submission_id = $1",
        )
        .bind(direct.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            direct_count == 0,
            "failed direct sending insert was not fully rolled back",
        )?;
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn submit_orchestrator_bounds_a_stuck_provider_future() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "submit-timeout-worker").await?;
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let provider = ScriptedFakeProvider::default();
        provider.push_submit(SubmitStep::Never);
        let orchestrator = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            provider.clone(),
            200,
            journal.path().join("remote-submit"),
        )
        .map_err(debug_error)?;

        let started = Instant::now();
        let observed = orchestrator
            .submit(orchestrator_work(&lease)?)
            .await
            .map_err(debug_error)?;
        require(
            started.elapsed() < Duration::from_secs(2)
                && matches!(observed, ProviderSubmitOutcome::AwaitingEvidence(ref intent)
                    if intent.state == ProviderSubmitIntentState::OutcomeUnknown
                        && intent.failure_error_code.as_deref() == Some("provider_submit_timeout"))
                && provider.calls().submit == 1,
            format!("stuck submit was not bounded as uncertain: {observed:?}"),
        )?;
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn atomic_submit_migration_bounds_lock_wait_and_retries_cleanly() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let mut holder = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("LOCK TABLE provider_remote_submit_intents IN ROW EXCLUSIVE MODE")
            .execute(&mut *holder)
            .await
            .map_err(debug_error)?;

        let mut blocked = database.pool.begin().await.map_err(debug_error)?;
        let attempted = tokio::time::timeout(
            Duration::from_secs(7),
            sqlx::raw_sql(include_str!(
                "../migrations/0027_atomic_provider_submit_acquisition.sql"
            ))
            .execute(&mut *blocked),
        )
        .await
        .map_err(|_| "atomic submit migration exceeded its lock timeout".to_string())?;
        require(
            attempted.is_err(),
            "atomic submit migration ignored a conflicting writer lock",
        )?;
        blocked.rollback().await.map_err(debug_error)?;
        holder.rollback().await.map_err(debug_error)?;

        let mut retry = database.pool.begin().await.map_err(debug_error)?;
        sqlx::raw_sql(include_str!(
            "../migrations/0027_atomic_provider_submit_acquisition.sql"
        ))
        .execute(&mut *retry)
        .await
        .map_err(debug_error)?;
        retry.commit().await.map_err(debug_error)
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn atomic_submit_migration_preserves_and_recovers_schema_26_submit_states() -> TestResult {
    let Some(database) = TestDatabase::new_before_atomic_submit_acquisition().await? else {
        return Ok(());
    };
    let result = async {
        let reserved_lease =
            seed_running_submission(&database.pool, "schema-26-reserved-submit").await?;
        let sending_lease =
            seed_running_submission(&database.pool, "schema-26-sending-submit").await?;
        let reserved = reservation_request(&reserved_lease);
        let sending = reservation_request(&sending_lease);
        seed_schema_26_submit(&database.pool, &reserved_lease, &reserved, false).await?;
        seed_schema_26_submit(&database.pool, &sending_lease, &sending, true).await?;

        let before: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT submission_id, state FROM provider_remote_submit_intents ORDER BY submission_id",
        )
        .fetch_all(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            before.iter().any(|row| row.1 == "reserved")
                && before.iter().any(|row| row.1 == "sending"),
            format!("schema 26 fixtures did not cover both active states: {before:?}"),
        )?;

        sqlx::raw_sql(include_str!(
            "../migrations/0027_atomic_provider_submit_acquisition.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        let after: Vec<(Uuid, String, i32, i32)> = sqlx::query_as(
            "SELECT submission_id, state, output_index, output_total FROM provider_remote_submit_intents ORDER BY submission_id",
        )
        .fetch_all(&database.pool)
        .await
        .map_err(debug_error)?;
        let after_identity: Vec<(Uuid, String)> = after
            .iter()
            .map(|row| (row.0, row.1.clone()))
            .collect();
        require(
            after_identity == before && after.iter().all(|row| row.2 == 0 && row.3 == 1),
            format!("0027 rewrote schema 26 submit history: {before:?} -> {after:?}"),
        )?;

        for migration in [
            include_str!("../migrations/0028_executor_active_owner_lookup.sql"),
            include_str!("../migrations/0029_provider_runtime_readiness.sql"),
            include_str!("../migrations/0030_provider_profile_readiness_projection.sql"),
            include_str!("../migrations/0031_capacity_counter_snapshot.sql"),
            include_str!("../migrations/0032_video_artifact_media.sql"),
            include_str!("../migrations/0033_media_economics_v3.sql"),
        ] {
            sqlx::raw_sql(migration)
                .execute(&database.pool)
                .await
                .map_err(debug_error)?;
        }

        let store = PostgresProviderTaskStore::new(database.pool.clone());
        require(
            matches!(
                store.acquire_submit(&reserved).await.map_err(debug_error)?,
                ProviderSubmitAcquire::Dispatch(_)
            ),
            "0027 did not let atomic acquire adopt a schema 26 reserved intent",
        )?;
        require(
            matches!(
                store.acquire_submit(&sending).await.map_err(debug_error)?,
                ProviderSubmitAcquire::Busy(_)
            ),
            "0027 treated a schema 26 sending intent as new dispatch authority",
        )
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn submit_orchestrator_never_retries_unknown_remote_effect() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "uncertain-submit-worker").await?;
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let provider = ScriptedFakeProvider::default();
        provider.push_submit(SubmitStep::Fail(
            ProviderFailure::new(
                ProviderFailureClass::Ambiguous,
                "submit_effect_unknown",
                EffectCertainty::UnknownRemoteEffect,
                RetryDirective::Never,
            )
            .map_err(debug_error)?,
        ));
        let orchestrator = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            provider.clone(),
            60_000,
            journal.path().join("remote-submit"),
        )
        .map_err(debug_error)?;

        let first = orchestrator
            .submit(orchestrator_work(&lease)?)
            .await
            .map_err(debug_error)?;
        let replay = orchestrator
            .submit(orchestrator_work(&lease)?)
            .await
            .map_err(debug_error)?;
        require(
            matches!(first, ProviderSubmitOutcome::AwaitingEvidence(ref intent)
                if intent.state == ProviderSubmitIntentState::OutcomeUnknown)
                && matches!(replay, ProviderSubmitOutcome::AwaitingEvidence(ref intent)
                    if intent.state == ProviderSubmitIntentState::OutcomeUnknown)
                && provider.calls().submit == 1,
            "unknown remote effect was retried or projected incorrectly",
        )?;
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn submit_orchestrator_quarantines_misattributed_receipt() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "receipt-fence-worker").await?;
        let journal = tempfile::tempdir().map_err(debug_error)?;
        let provider = ScriptedFakeProvider::default();
        provider.push_submit(SubmitStep::Pending(PendingOperation::new(
            RemoteOperationRef::new(
                "another-provider",
                lease.submission_id.to_string(),
                "misattributed-operation",
            )
            .map_err(debug_error)?,
            Some(ProviderRequestId::new("misattributed-request").map_err(debug_error)?),
            None,
        )));
        let orchestrator = ProviderSubmitOrchestrator::new(
            PostgresProviderTaskStore::new(database.pool.clone()),
            provider.clone(),
            60_000,
            journal.path().join("remote-submit"),
        )
        .map_err(debug_error)?;

        let observed = orchestrator
            .submit(orchestrator_work(&lease)?)
            .await
            .map_err(debug_error)?;
        let task_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_remote_tasks WHERE submission_id = $1",
        )
        .bind(lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        let evidence: (String, String, String, Option<String>) = sqlx::query_as(
            r#"
            SELECT observed_provider_id, observed_submission_id,
                   remote_operation_id, provider_request_id
            FROM provider_submit_quarantined_receipts
            WHERE submission_id = $1
            "#,
        )
        .bind(lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            matches!(observed, ProviderSubmitOutcome::AwaitingEvidence(ref intent)
                if intent.state == ProviderSubmitIntentState::OutcomeUnknown)
                && provider.calls().submit == 1
                && task_count == 0
                && evidence
                    == (
                        "another-provider".to_owned(),
                        lease.submission_id.to_string(),
                        "misattributed-operation".to_owned(),
                        Some("misattributed-request".to_owned()),
                    )
                && sqlx::query(
                    "UPDATE provider_submit_quarantined_receipts SET reason = 'tampered' WHERE submission_id = $1",
                )
                .bind(lease.submission_id)
                .execute(&database.pool)
                .await
                .is_err(),
            "misattributed receipt was attached or lost its uncertainty",
        )?;
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn remote_task_store_closes_attach_poll_callback_and_cancel_invariants() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let first = seed_running_submission(&database.pool, "remote-worker-a").await?;
        let second = seed_running_submission(&database.pool, "remote-worker-b").await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());

        let reservation = reservation_request(&first);
        let (reserved_left, reserved_right) = tokio::join!(
            store.reserve_submit(&reservation),
            store.reserve_submit(&reservation)
        );
        let reserved_left = reserved_left.map_err(debug_error)?;
        let reserved_right = reserved_right.map_err(debug_error)?;
        require(
            reserved_left == reserved_right
                && reserved_left.state == ProviderSubmitIntentState::Reserved,
            "concurrent submit reservation did not converge",
        )?;
        let (started_left, started_right) = tokio::join!(
            store.start_submit(&reservation),
            store.start_submit(&reservation)
        );
        let starts = [started_left.map_err(debug_error)?, started_right.map_err(debug_error)?];
        require(
            starts
                .iter()
                .filter(|start| matches!(start, ProviderSubmitStart::Acquired(_)))
                .count()
                == 1
                && starts.iter().all(|start| match start {
                    ProviderSubmitStart::Acquired(intent)
                    | ProviderSubmitStart::Existing(intent) =>
                        intent.intent.state == ProviderSubmitIntentState::Sending,
                }),
            "concurrent submit start did not elect exactly one sender",
        )?;
        let first_context = match &starts[0] {
            ProviderSubmitStart::Acquired(invocation)
            | ProviderSubmitStart::Existing(invocation) => invocation.context().clone(),
        };
        require(
            first_context.command_hash() == first.command_hash
                && first_context.operation_id() == "images.generations"
                && first_context.operation_descriptor_revision()
                    == "provider-test/images.generations/v1"
                && first_context.operation_descriptor_sha256_v1() == "2".repeat(64)
                && first_context.completion_mode() == "remote_task"
                && first_context.idempotency_mode() == "submission_bound"
                && first_context.operation_binding_version() == 2
                && first_context.provider_command_sha256()
                    == hex::encode(reservation.provider_command().canonical_sha256())
                && first_context.execution_binding_sha256()
                    == reserved_left.execution_binding_sha256,
            "submit start omitted or changed its exact operation binding",
        )?;
        let receipt = submit_receipt!(&store, &first, "operation-a", "submit-receipt-a");
        let (receipt_left, receipt_right) = tokio::join!(
            store.record_submit_receipt(&receipt),
            store.record_submit_receipt(&receipt)
        );
        let receipt_left = receipt_left.map_err(debug_error)?;
        let receipt_right = receipt_right.map_err(debug_error)?;
        require(
            receipt_left == receipt_right
                && receipt_left.state == ProviderSubmitIntentState::OperationKnown,
            "concurrent submit receipt did not converge",
        )?;
        let attach = attach_request!(&store, &first, "operation-a", "submit-event-a");
        let (left, right) = tokio::join!(store.attach(&attach), store.attach(&attach));
        let left = left.map_err(debug_error)?;
        let right = right.map_err(debug_error)?;
        require(left == right, "concurrent attach did not converge on one task")?;
        require(
            left.state == ProviderTaskState::ProviderWaiting,
            "attached task was not waiting",
        )?;
        let task_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_remote_tasks WHERE submission_id = $1",
        )
        .bind(first.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(task_count == 1, "attach created more than one remote task")?;
        let executor_projection: (String, Option<String>, Option<i64>, String) = sqlx::query_as(
            r#"
            SELECT execution.state, execution.executor_owner,
                   execution.lease_expires_at_ms, submission.state
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(first.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            executor_projection
                == ("provider_waiting".to_string(), None, None, "provider_waiting".to_string()),
            format!("executor lease was retained while waiting: {executor_projection:?}"),
        )?;

        let conflicting = RemoteTaskAttach {
            remote_operation_id: "operation-conflict".to_string(),
            ..attach.clone()
        };
        require(
            store.attach(&conflicting).await == Err(ProviderTaskStoreError::Conflict),
            "same submission accepted a conflicting remote operation",
        )?;
        store
            .reserve_submit(&reservation_request(&second))
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation_request(&second))
            .await
            .map_err(debug_error)?;
        let second_binding = binding_sha256(&store, &second).await?;
        let mut cross_bound_receipt = receipt.clone();
        cross_bound_receipt.execution_binding_sha256 = second_binding.clone();
        require(
            store.record_submit_receipt(&cross_bound_receipt).await
                == Err(ProviderTaskStoreError::Conflict),
            "submit receipt accepted another execution binding",
        )?;
        let mut cross_bound_failure = submit_failure_request(
            &store,
            &second,
            ProviderSubmitFailureKind::OutcomeUnknown,
            "cross-bound-failure",
            "submit_effect_unknown",
        )
        .await?;
        cross_bound_failure.execution_binding_sha256 =
            first_context.execution_binding_sha256().to_string();
        require(
            store.record_submit_failure(&cross_bound_failure).await
                == Err(ProviderTaskStoreError::Conflict),
            "submit failure accepted another execution binding",
        )?;
        require(
            store
                .record_submit_receipt(&submit_receipt!(&store,
                    &second,
                    "operation-a",
                    "conflicting-receipt-b",
                ))
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "same account remote operation was accepted across submissions",
        )?;
        store
            .record_submit_receipt(&submit_receipt!(&store,
                &second,
                "operation-b",
                "submit-receipt-b",
            ))
            .await
            .map_err(debug_error)?;
        let mut cross_bound_attach =
            attach_request!(&store, &second, "operation-b", "cross-bound-attach");
        cross_bound_attach.execution_binding_sha256 =
            first_context.execution_binding_sha256().to_string();
        require(
            store.attach(&cross_bound_attach).await == Err(ProviderTaskStoreError::Conflict),
            "remote task attach accepted another execution binding",
        )?;
        let cross_submission = attach_request!(&store, &second, "operation-a", "submit-event-b");
        require(
            store.attach(&cross_submission).await == Err(ProviderTaskStoreError::Conflict),
            "same account remote operation was attached across submissions",
        )?;
        let second_attach = attach_request!(&store, &second, "operation-b", "submit-event-b");

        let capacity_before_callback =
            capacity_heartbeat(&database.pool, first.executor_execution_id).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let callback = VerifiedCallbackWakeup {
            submission_id: first.submission_id,
            event_identity: "callback-event-a".to_string(),
        };
        store
            .record_verified_callback(&callback)
            .await
            .map_err(debug_error)?;
        store
            .record_verified_callback(&callback)
            .await
            .map_err(debug_error)?;
        let callback_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM provider_task_observations
            WHERE submission_id = $1 AND source = 'verified_callback'
            "#,
        )
        .bind(first.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(callback_count == 1, "duplicate callback was not deduplicated")?;
        require(
            capacity_heartbeat(&database.pool, first.executor_execution_id).await?
                == capacity_before_callback,
            "callback wakeup impersonated provider worker liveness",
        )?;
        let callback_task = store
            .load(first.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "callback task disappeared".to_string())?;
        require(
            callback_task.state == ProviderTaskState::ProviderWaiting
                && callback_task.artifact_ref.is_none(),
            "callback granted terminal or artifact authority",
        )?;

        let scope = claim_scope();
        let capacity_before_poll_claim =
            capacity_heartbeat(&database.pool, first.executor_execution_id).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let first_lease = store
            .claim_due(&scope, "poller-a", 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "first task was not pollable".to_string())?;
        require(
            first_lease.context() == &first_context,
            "poll claim re-resolved the frozen provider context",
        )?;
        let capacity_after_poll_claim =
            capacity_heartbeat(&database.pool, first.executor_execution_id).await?;
        require(
            capacity_after_poll_claim > capacity_before_poll_claim,
            "poll claim did not heartbeat held provider capacity",
        )?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let first_lease = store
            .heartbeat(&first_lease, 5_000)
            .await
            .map_err(debug_error)?;
        require(
            capacity_heartbeat(&database.pool, first.executor_execution_id).await?
                > capacity_after_poll_claim,
            "poll lease renewal did not heartbeat held provider capacity",
        )?;
        store
            .request_cancel(first.submission_id)
            .await
            .map_err(debug_error)?;
        let uncertain = ProviderTaskObservation {
            event_identity: "cancel-unknown-a".to_string(),
            source: ProviderTaskObservationSource::Cancel,
            outcome: ProviderTaskObservationOutcome::Uncertain {
                error_code: "cancel_effect_unknown".to_string(),
            },
        };
        let (concurrent_heartbeat, terminal) = tokio::time::timeout(
            Duration::from_secs(2),
            async {
                tokio::join!(
                    store.heartbeat(&first_lease, 5_000),
                    store.record_observation(&first_lease, &uncertain),
                )
            },
        )
        .await
        .map_err(|_| "poll heartbeat and terminal release deadlocked".to_string())?;
        require(
            matches!(
                concurrent_heartbeat,
                Ok(_) | Err(ProviderTaskStoreError::StaleLease)
            ),
            "poll heartbeat and terminal release produced an invalid race result",
        )?;
        let terminal = terminal.map_err(debug_error)?;
        require(
            terminal.state == ProviderTaskState::Uncertain,
            "unknown cancellation was projected as canceled",
        )?;
        let canonical: (String, String, String, String, i32) = sqlx::query_as(
            r#"
            SELECT execution.state, submission.state, decision.source,
                   allocation.state, policy.allocated_count
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = execution.resolution_decision_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = execution.executor_execution_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(first.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            canonical
                == (
                    "uncertain".to_string(),
                    "uncertain".to_string(),
                    "remote_provider_observation".to_string(),
                    "released".to_string(),
                    1,
                ),
            format!("remote terminal evidence did not close canonical state: {canonical:?}"),
        )?;
        let reductions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM executor_terminal_reductions WHERE submission_id = $1 AND state = 'ready'",
        )
        .bind(first.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(reductions == 1, "remote terminal reduction was not enqueued")?;
        let replay = store
            .record_observation(&first_lease, &uncertain)
            .await
            .map_err(debug_error)?;
        require(replay == terminal, "duplicate observation was not idempotent")?;
        let observation_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_task_observations WHERE submission_id = $1 AND event_identity = $2",
        )
        .bind(first.submission_id)
        .bind(&uncertain.event_identity)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(observation_count == 1, "duplicate observation was appended twice")?;
        store
            .record_verified_callback(&VerifiedCallbackWakeup {
                submission_id: first.submission_id,
                event_identity: "terminal-callback-a".to_string(),
            })
            .await
            .map_err(debug_error)?;
        let terminal_callback_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_task_observations WHERE submission_id = $1 AND event_identity = 'terminal-callback-a'",
        )
        .bind(first.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            terminal_callback_count == 0,
            "terminal callback replay appended or heartbeated durable state",
        )?;

        store.attach(&second_attach).await.map_err(debug_error)?;
        let (attach_replay, second_claim) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                store.attach(&second_attach),
                store.claim_due(&scope, "poller-old", 200)
            )
        })
        .await
        .map_err(|_| "attach replay and poll claim deadlocked".to_string())?;
        attach_replay.map_err(debug_error)?;
        let second_lease = second_claim
            .map_err(debug_error)?
            .ok_or_else(|| "second task was not pollable".to_string())?;
        let authority_id = second.executor_execution_id.simple().to_string();
        let authority = ProviderArtifactAuthority::new(
            "filesystem-v1".to_string(),
            "filesystem-v1:provider-task-test".to_string(),
            format!("executor-objects/{}/{}", &authority_id[..2], authority_id),
            "a".repeat(64),
            128,
            "image/png".to_string(),
        )
        .ok_or_else(|| "valid provider artifact authority was rejected".to_string())?;
        let mut task_locker = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SELECT 1 FROM provider_remote_tasks WHERE submission_id = $1 FOR UPDATE")
            .bind(second.submission_id)
            .execute(&mut *task_locker)
            .await
            .map_err(debug_error)?;
        let stale_store = store.clone();
        let stale_lease = second_lease.clone();
        let stale_authority = authority.clone();
        let mut blocked_publication = tokio::spawn(async move {
            stale_store
                .publish_artifact_authority(&stale_lease, &stale_authority)
                .await
        });
        let stale_store = store.clone();
        let stale_lease = second_lease.clone();
        let mut blocked_observation = tokio::spawn(async move {
            stale_store
                .record_observation(
                    &stale_lease,
                    &ProviderTaskObservation {
                        event_identity: "expired-while-locked-b".to_string(),
                        source: ProviderTaskObservationSource::Poll,
                        outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                    },
                )
                .await
        });
        let stale_store = store.clone();
        let stale_lease = second_lease.clone();
        let mut blocked_heartbeat =
            tokio::spawn(async move { stale_store.heartbeat(&stale_lease, 5_000).await });
        tokio::time::sleep(Duration::from_millis(300)).await;
        require(
            !blocked_publication.is_finished()
                && !blocked_observation.is_finished()
                && !blocked_heartbeat.is_finished(),
            "provider write did not wait for the task fence lock",
        )?;
        task_locker.commit().await.map_err(debug_error)?;
        let stale_publication =
            tokio::time::timeout(Duration::from_secs(2), &mut blocked_publication)
                .await
                .map_err(|_| "stale authority publication remained blocked".to_string())?
                .map_err(debug_error)?;
        require(
            stale_publication == Err(ProviderTaskStoreError::StaleLease),
            "authority publication used a database timestamp captured before its task lock",
        )?;
        let stale_observation =
            tokio::time::timeout(Duration::from_secs(2), &mut blocked_observation)
                .await
                .map_err(|_| "stale provider observation remained blocked".to_string())?
                .map_err(debug_error)?;
        require(
            stale_observation.is_err(),
            "provider observation used a database timestamp captured before its task lock",
        )?;
        let stale_heartbeat = tokio::time::timeout(Duration::from_secs(2), &mut blocked_heartbeat)
            .await
            .map_err(|_| "stale provider heartbeat remained blocked".to_string())?
            .map_err(debug_error)?;
        require(
            stale_heartbeat == Err(ProviderTaskStoreError::StaleLease),
            "provider heartbeat used a database timestamp captured before its task lock",
        )?;
        let stale_observation_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_task_observations WHERE submission_id = $1 AND event_identity = 'expired-while-locked-b'",
        )
        .bind(second.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            stale_observation_count == 0,
            "expired poll owner left append-only observation evidence",
        )?;
        let authority_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM executor_artifact_authorities WHERE authority_id = $1",
        )
        .bind(second.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            authority_count == 0,
            "expired poll owner published immutable artifact authority",
        )?;
        let reclaimed = store
            .claim_due(&scope, "poller-new", 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "expired task was not reclaimed".to_string())?;
        require(
            reclaimed.poll_lease_epoch == second_lease.poll_lease_epoch + 1,
            "poll lease epoch did not advance on reclaim",
        )?;
        let capacity_after_reclaim =
            capacity_heartbeat(&database.pool, second.executor_execution_id).await?;
        require(
            store.heartbeat(&second_lease, 5_000).await
                == Err(ProviderTaskStoreError::StaleLease),
            "expired poll fence renewed provider capacity",
        )?;
        require(
            capacity_heartbeat(&database.pool, second.executor_execution_id).await?
                == capacity_after_reclaim,
            "stale poll heartbeat changed held provider capacity",
        )?;
        let stale_result = store
            .record_observation(
                &second_lease,
                &ProviderTaskObservation {
                    event_identity: "stale-poll-b".to_string(),
                    source: ProviderTaskObservationSource::Poll,
                    outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                },
            )
            .await;
        require(stale_result.is_err(), "stale poll fence wrote an observation")?;
        let waiting = ProviderTaskObservation {
            event_identity: "waiting-replay-b".to_string(),
            source: ProviderTaskObservationSource::Poll,
            outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
        };
        let first_waiting = store
            .record_observation(&reclaimed, &waiting)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let replayed_waiting = store
            .record_observation(&reclaimed, &waiting)
            .await
            .map_err(debug_error)?;
        require(
            first_waiting.next_poll_at_ms == replayed_waiting.next_poll_at_ms,
            "waiting observation replay changed its absolute poll time",
        )?;
        let conflicting_waiting = ProviderTaskObservation {
            outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 1 },
            ..waiting.clone()
        };
        require(
            store
                .record_observation(&reclaimed, &conflicting_waiting)
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "waiting observation replay accepted a different relative delay",
        )?;
        let reclaimed = store
            .claim_due(&scope, "poller-after-replay", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "replayed waiting task was not claimable".to_string())?;
        let cancel_without_request = store
            .record_observation(
                &reclaimed,
                &ProviderTaskObservation {
                    event_identity: "forged-cancel-b".to_string(),
                    source: ProviderTaskObservationSource::Cancel,
                    outcome: ProviderTaskObservationOutcome::Canceled {
                        error_code: "provider_canceled".to_string(),
                    },
                },
            )
            .await;
        require(
            cancel_without_request.is_err(),
            "task became canceled without a durable cancel request",
        )?;
        require(
            store
                .load(second.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|task| task.state == ProviderTaskState::ProviderWaiting),
            "failed fenced writes changed another submission",
        )?;
        require(
            sqlx::query(
                r#"
                INSERT INTO provider_task_observations
                  (observation_id, submission_id, executor_execution_id,
                   event_identity, source, observed_state, artifact_ref,
                   result_manifest_id, artifact_sha256_hex, artifact_byte_size,
                   artifact_media_type, error_code, effect_certainty,
                   next_poll_at_ms, poll_owner, poll_lease_epoch, payload_hash,
                   observed_at_ms)
                VALUES ($1, $2, $3, 'artifact-ready-b', 'poll', 'artifact_ready',
                        'durable-object-b', $2, $4, 128, 'image/png', NULL,
                        'not_applicable', NULL, $5, $6, $7,
                        floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(second.submission_id)
            .bind(second.executor_execution_id)
            .bind("a".repeat(64))
            .bind(&reclaimed.poll_owner)
            .bind(reclaimed.poll_lease_epoch)
            .bind("b".repeat(64))
            .execute(&database.pool)
            .await
            .is_err(),
            "artifact_ready committed before its immutable authority and manifest",
        )?;
        require(
            store
                .load(second.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|task| task.state == ProviderTaskState::ProviderWaiting),
            "rejected artifact_ready changed the durable task",
        )?;
        let premature_observations: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_task_observations WHERE submission_id = $1 AND event_identity = 'artifact-ready-b'",
        )
        .bind(second.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            premature_observations == 0,
            "rejected artifact_ready left append-only evidence behind",
        )?;
        let publication = store
            .publish_artifact_authority(&reclaimed, &authority)
            .await
            .map_err(|error| format!("publish remote artifact authority: {error:?}"))?;
        let mut stale_publication_replay = reclaimed.clone();
        stale_publication_replay.poll_lease_expires_at_ms = 0;
        require(
            store
                .publish_artifact_authority(&stale_publication_replay, &authority)
                .await
                .map_err(debug_error)?
                == publication,
            "exact authority commit-ack replay required a live poll lease",
        )?;
        require(
            store
                .record_observation(
                    &reclaimed,
                    &ProviderTaskObservation {
                        event_identity: "failure-after-authority-b".to_string(),
                        source: ProviderTaskObservationSource::Poll,
                        outcome: ProviderTaskObservationOutcome::Failed {
                            error_code: "contradictory_failure".to_string(),
                        },
                    },
                )
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "contradictory failure won after immutable artifact publication",
        )?;
        let artifact_ready = ProviderTaskObservation {
            event_identity: "artifact-ready-b".to_string(),
            source: ProviderTaskObservationSource::Poll,
            outcome: ProviderTaskObservationOutcome::ArtifactReady {
                artifact_ref: "durable-object-b".to_string(),
                publication: publication.clone(),
            },
        };
        let mut split_observation = database.pool.begin().await.map_err(debug_error)?;
        let split_observed_at = database_now(&database.pool).await?;
        sqlx::query(
            r#"
            INSERT INTO provider_task_observations
              (observation_id, submission_id, executor_execution_id,
               event_identity, source, observed_state, artifact_ref,
               result_manifest_id, artifact_sha256_hex, artifact_byte_size,
               artifact_media_type, error_code, effect_certainty,
               next_poll_at_ms, poll_owner, poll_lease_epoch, payload_hash,
               observed_at_ms)
            VALUES ($1, $2, $3, 'artifact-ready-b', 'poll', 'artifact_ready',
                    'durable-object-b', $2, $4, 128, 'image/png', NULL,
                    'not_applicable', NULL, $5, $6, $7, $8)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(second.submission_id)
        .bind(second.executor_execution_id)
        .bind("a".repeat(64))
        .bind(&reclaimed.poll_owner)
        .bind(reclaimed.poll_lease_epoch)
        .bind("e".repeat(64))
        .bind(split_observed_at)
        .execute(&mut *split_observation)
        .await
        .map_err(debug_error)?;
        require(
            split_observation.commit().await.is_err(),
            "raw artifact_ready observation committed without canonical resolution",
        )?;
        let ready = store
            .record_observation(&reclaimed, &artifact_ready)
            .await
            .map_err(|error| format!("record remote artifact ready: {error:?}"))?;
        require(
            ready.state == ProviderTaskState::ArtifactReady,
            "verified remote artifact did not become ready",
        )?;
        let replayed_ready = store
            .record_observation(&reclaimed, &artifact_ready)
            .await
            .map_err(|error| format!("replay remote artifact ready: {error:?}"))?;
        require(
            replayed_ready == ready,
            "artifact_ready commit-ack replay changed the durable task",
        )?;
        require(
            publication.manifest().manifest_id() == second.submission_id,
            "remote artifact manifest identity drifted",
        )?;
        let success_projection: (String, String, String, String) = sqlx::query_as(
            r#"
            SELECT execution.state, submission.state, decision.source, allocation.state
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = execution.resolution_decision_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = execution.executor_execution_id
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(second.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            success_projection
                == (
                    "succeeded".to_string(),
                    "succeeded".to_string(),
                    "remote_provider_observation".to_string(),
                    "released".to_string(),
            ),
            format!("remote artifact did not close canonical success: {success_projection:?}"),
        )?;
        let exact_counts: (i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM provider_task_observations
               WHERE submission_id = $1 AND event_identity = 'artifact-ready-b'),
              (SELECT COUNT(*) FROM executor_resolution_decisions
               WHERE submission_id = $1 AND source = 'remote_provider_observation')
            "#,
        )
        .bind(second.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            exact_counts == (1, 1),
            format!("artifact_ready replay duplicated evidence or resolution: {exact_counts:?}"),
        )?;

        let third = seed_running_submission(&database.pool, "remote-worker-c").await?;
        let mut invalid_projection = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = 'provider_waiting', executor_owner = NULL,
                lease_expires_at_ms = NULL, updated_at_ms = updated_at_ms + 1
            WHERE executor_execution_id = $1
            "#,
        )
        .bind(third.executor_execution_id)
        .execute(&mut *invalid_projection)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "UPDATE provider_submissions SET state = 'provider_waiting', updated_at_ms = updated_at_ms + 1 WHERE submission_id = $1",
        )
        .bind(third.submission_id)
        .execute(&mut *invalid_projection)
        .await
        .map_err(debug_error)?;
        require(
            invalid_projection.commit().await.is_err(),
            "provider_waiting committed without a durable remote task",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn provider_task_leases_reject_cross_submission_splicing() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        seed_attached_remote_task(&database.pool, &store, "splice-a", "splice-a", 30_000, 0)
            .await?;
        seed_attached_remote_task(&database.pool, &store, "splice-b", "splice-b", 30_000, 0)
            .await?;
        let scope = claim_scope();
        let first = store
            .claim_due(&scope, "splice-poller", 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "first splice fixture was not claimable".to_string())?;
        let second = store
            .claim_due(&scope, "splice-poller", 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "second splice fixture was not claimable".to_string())?;
        require(
            first.task.submission_id != second.task.submission_id,
            "splice fixtures resolved to the same submission",
        )?;

        let mut forged = first.clone();
        forged.task = second.task.clone();
        forged.poll_owner = second.poll_owner.clone();
        forged.poll_lease_epoch = second.poll_lease_epoch;
        let observation = ProviderTaskObservation {
            event_identity: "forged-cross-submission-poll".to_string(),
            source: ProviderTaskObservationSource::Poll,
            outcome: ProviderTaskObservationOutcome::Waiting {
                poll_after_ms: 1_000,
            },
        };
        require(
            store.heartbeat(&forged, 5_000).await == Err(ProviderTaskStoreError::InvalidInput)
                && store.record_observation(&forged, &observation).await
                    == Err(ProviderTaskStoreError::InvalidInput),
            "poll lease authority was spliceable across submissions",
        )?;
        let mut forged_request = first.clone();
        forged_request.task.provider_request_id = Some("forged-provider-request".to_owned());
        let mut forged_cancel = first.clone();
        forged_cancel.task.cancel_requested = !forged_cancel.task.cancel_requested;
        let mut forged_state = first.clone();
        forged_state.task.state = ProviderTaskState::ArtifactReady;
        let mut forged_epoch = first.clone();
        forged_epoch.task.poll_lease_epoch += 1;
        require(
            store.heartbeat(&forged_request, 5_000).await
                == Err(ProviderTaskStoreError::InvalidInput)
                && store.heartbeat(&forged_cancel, 5_000).await
                    == Err(ProviderTaskStoreError::InvalidInput)
                && store.heartbeat(&forged_state, 5_000).await
                    == Err(ProviderTaskStoreError::InvalidInput)
                && store.heartbeat(&forged_epoch, 5_000).await
                    == Err(ProviderTaskStoreError::InvalidInput),
            "poll lease decision fields were mutable outside the database authority",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_recovery_leases_reject_cross_submission_splicing() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let first =
            seed_running_submission_with_lease(&database.pool, "splice-recovery-a", 250).await?;
        let second =
            seed_running_submission_with_lease(&database.pool, "splice-recovery-b", 250).await?;
        for executor in [&first, &second] {
            let reservation = reservation_request(executor);
            store
                .reserve_submit(&reservation)
                .await
                .map_err(debug_error)?;
            store
                .start_submit(&reservation)
                .await
                .map_err(debug_error)?;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let scope = claim_scope();
        let first = store
            .claim_submit_recovery(&scope, "splice-recovery", "splice-command-a", 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "first recovery splice fixture was not claimable".to_string())?;
        let second = store
            .claim_submit_recovery(&scope, "splice-recovery", "splice-command-b", 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "second recovery splice fixture was not claimable".to_string())?;
        require(
            first.intent.submission_id != second.intent.submission_id,
            "recovery splice fixtures resolved to the same submission",
        )?;

        let mut forged = first.clone();
        forged.intent = second.intent.clone();
        forged.recovery_owner = second.recovery_owner.clone();
        forged.recovery_lease_epoch = second.recovery_lease_epoch;
        require(
            store.heartbeat_submit_recovery(&forged, 5_000).await
                == Err(ProviderTaskStoreError::InvalidInput)
                && store
                    .defer_submit_recovery(&forged, "forged-defer", 1_000)
                    .await
                    == Err(ProviderTaskStoreError::InvalidInput),
            "submit recovery authority was spliceable across submissions",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_submit_requires_exact_remote_operation_binding() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let inline_profile_id = Uuid::new_v4();
        let inline_adapter = "provider-test-inline-v1";
        let now = database_now(&database.pool).await?;
        sqlx::query(
            r#"
            INSERT INTO provider_execution_profiles
              (execution_profile_id, profile_key, provider_id, command_schema,
               operation_id, operation_descriptor_revision,
               operation_descriptor_sha256_v1, completion_mode, idempotency_mode,
               adapter_revision, credential_pool_id, provider_account_id,
               credential_ref, credential_revision, resource_policy_id,
               resource_policy_revision, state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'provider-test', 'provider-command-v1',
                    'images.generations', 'provider-test/images.generations/v1',
                    $3, 'inline', 'submission_bound', $4, $5, $6,
                    'test-vault.provider-task.1', 1, $7, 1, 'enabled', $8, $8)
            "#,
        )
        .bind(inline_profile_id)
        .bind(format!("provider-inline-{}", inline_profile_id.simple()))
        .bind("2".repeat(64))
        .bind(inline_adapter)
        .bind(POOL_ID)
        .bind(ACCOUNT_ID)
        .bind(POLICY_ID)
        .bind(now)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        let inline = seed_running_submission_for_profile(
            &database.pool,
            "inline-binding",
            5_000,
            inline_profile_id,
            inline_adapter,
        )
        .await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        require(
            store.reserve_submit(&reservation_request(&inline)).await
                == Err(ProviderTaskStoreError::Conflict),
            "inline operation profile entered the remote submit lifecycle",
        )?;
        let intent_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_remote_submit_intents WHERE submission_id = $1",
        )
        .bind(inline.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(intent_count == 0, "rejected inline submit left durable intent evidence")?;
        require(
            sqlx::query(
                "UPDATE provider_execution_profiles SET completion_mode = 'remote_task' WHERE execution_profile_id = $1",
            )
            .bind(inline_profile_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "execution profile operation semantics were mutable",
        )?;
        require(
            sqlx::query(
                "UPDATE provider_submissions SET operation_descriptor_sha256_v1 = $2 WHERE submission_id = $1",
            )
            .bind(inline.submission_id)
            .bind("3".repeat(64))
            .execute(&database.pool)
            .await
            .is_err(),
            "provider submission descriptor snapshot was mutable",
        )?;

        let remote = seed_running_submission(&database.pool, "remote-binding").await?;
        let reservation = reservation_request(&remote);
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        let changed_command = RemoteTaskSubmitReservation::new(
            &remote,
            reservation.idempotency_key.clone(),
            OutputSlot::new(reservation.output_index(), reservation.output_total())
                .map_err(debug_error)?,
            provider_command_identity([4; 32]),
            reservation.provider_timeout_ms,
        );
        require(
            store.reserve_submit(&changed_command).await == Err(ProviderTaskStoreError::Conflict),
            "reservation replay accepted a different provider command digest",
        )?;
        let mut changed_timeout = reservation.clone();
        changed_timeout.provider_timeout_ms += 1;
        require(
            store.reserve_submit(&changed_timeout).await == Err(ProviderTaskStoreError::Conflict),
            "reservation replay accepted a different provider timeout",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_fences_late_poll_and_quarantines_capacity() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let executor = seed_attached_remote_task(
            &database.pool,
            &store,
            "remote-deadline-worker",
            "remote-deadline",
            900,
            60_000,
        )
        .await?;
        let (task_deadline, recovery_deadline, next_poll): (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT task.provider_deadline_at_ms, recovery.provider_deadline_at_ms,
                   task.next_poll_at_ms
            FROM provider_remote_tasks task
            JOIN provider_submit_recoveries recovery
              ON recovery.submission_id = task.submission_id
             AND recovery.executor_execution_id = task.executor_execution_id
            WHERE task.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            task_deadline == recovery_deadline && next_poll == task_deadline,
            "attached task did not retain its exact bounded recovery deadline",
        )?;
        require(
            store
                .resolve_due_remote_task_deadline(&ProviderTaskClaimScope {
                    provider_id: "provider-test".to_string(),
                    provider_account_id: Uuid::new_v4(),
                })
                .await
                .map_err(debug_error)?
                .is_none(),
            "deadline resolver escaped its provider/account scope",
        )?;
        store
            .request_cancel(executor.submission_id)
            .await
            .map_err(debug_error)?;

        let lease = store
            .claim_due(&claim_scope(), "remote-deadline-poller", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "bounded remote task was not claimable".to_string())?;
        require(
            lease.context().provider_deadline_at_ms() == task_deadline
                && lease.poll_lease_expires_at_ms <= task_deadline,
            "poll claim extended beyond the frozen provider deadline",
        )?;
        let lease = store
            .heartbeat(&lease, 60_000)
            .await
            .map_err(debug_error)?;
        require(
            lease.poll_lease_expires_at_ms == task_deadline,
            "poll heartbeat was not capped at the frozen provider deadline",
        )?;
        let heartbeat_before_deadline =
            capacity_heartbeat(&database.pool, executor.executor_execution_id).await?;

        let authority = artifact_authority(&executor, "remote-deadline")?;
        let mut task_locker = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SELECT 1 FROM provider_remote_tasks WHERE submission_id = $1 FOR UPDATE")
            .bind(executor.submission_id)
            .execute(&mut *task_locker)
            .await
            .map_err(debug_error)?;
        let heartbeat_store = store.clone();
        let heartbeat_lease = lease.clone();
        let mut blocked_heartbeat = tokio::spawn(async move {
            heartbeat_store.heartbeat(&heartbeat_lease, 60_000).await
        });
        let observation_store = store.clone();
        let observation_lease = lease.clone();
        let mut blocked_observation = tokio::spawn(async move {
            observation_store
                .record_observation(
                    &observation_lease,
                    &ProviderTaskObservation {
                        event_identity: "late-deadline-poll".to_string(),
                        source: ProviderTaskObservationSource::Poll,
                        outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                    },
                )
                .await
        });
        let publication_store = store.clone();
        let publication_lease = lease.clone();
        let publication_authority = authority.clone();
        let mut blocked_publication = tokio::spawn(async move {
            publication_store
                .publish_artifact_authority(&publication_lease, &publication_authority)
                .await
        });
        sleep_until_database_time(&database.pool, task_deadline + 20).await?;
        require(
            !blocked_heartbeat.is_finished()
                && !blocked_observation.is_finished()
                && !blocked_publication.is_finished(),
            "late provider write did not wait for the task authority lock",
        )?;
        task_locker.commit().await.map_err(debug_error)?;
        let late_heartbeat = tokio::time::timeout(Duration::from_secs(2), &mut blocked_heartbeat)
            .await
            .map_err(|_| "late heartbeat remained blocked".to_string())?
            .map_err(debug_error)?;
        let late_observation =
            tokio::time::timeout(Duration::from_secs(2), &mut blocked_observation)
                .await
                .map_err(|_| "late observation remained blocked".to_string())?
                .map_err(debug_error)?;
        let late_publication =
            tokio::time::timeout(Duration::from_secs(2), &mut blocked_publication)
                .await
                .map_err(|_| "late artifact publication remained blocked".to_string())?
                .map_err(debug_error)?;
        require(
            late_heartbeat == Err(ProviderTaskStoreError::StaleLease)
                && late_observation == Err(ProviderTaskStoreError::StaleLease)
                && late_publication == Err(ProviderTaskStoreError::StaleLease),
            "a provider write crossed the database absolute deadline",
        )?;
        let late_evidence: (i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM provider_task_observations
               WHERE submission_id = $1 AND event_identity = 'late-deadline-poll'),
              (SELECT COUNT(*) FROM executor_artifact_authorities
               WHERE executor_execution_id = $2)
            "#,
        )
        .bind(executor.submission_id)
        .bind(executor.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            late_evidence == (0, 0),
            format!("late provider write left durable evidence: {late_evidence:?}"),
        )?;

        let left_scope = claim_scope();
        let right_scope = claim_scope();
        let (left, right) = tokio::join!(
            store.resolve_due_remote_task_deadline(&left_scope),
            store.resolve_due_remote_task_deadline(&right_scope),
        );
        let results = [left.map_err(debug_error)?, right.map_err(debug_error)?];
        require(
            results.iter().filter(|result| result.is_some()).count() == 1,
            "concurrent deadline resolvers did not elect exactly one transition",
        )?;
        let resolved = results
            .into_iter()
            .flatten()
            .next()
            .ok_or_else(|| "deadline resolver returned no task".to_string())?;
        require(
            resolved.submission_id == executor.submission_id
                && resolved.state == ProviderTaskState::Uncertain,
            "deadline quarantine changed the public compatibility projection",
        )?;
        let state_projection: (
            String,
            Option<String>,
            Option<Uuid>,
            String,
            String,
            String,
            String,
        ) = sqlx::query_as(
            r#"
            SELECT task.state, task.error_code, task.deadline_quarantine_id,
                   execution.state, submission.state, decision.source,
                   allocation.state
            FROM provider_remote_tasks task
            JOIN executor_executions execution
              ON execution.executor_execution_id = task.executor_execution_id
             AND execution.submission_id = task.submission_id
            JOIN provider_submissions submission
              ON submission.executor_execution_id = task.executor_execution_id
             AND submission.submission_id = task.submission_id
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = execution.resolution_decision_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = task.executor_execution_id
             AND allocation.submission_id = task.submission_id
            WHERE task.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            state_projection
                == (
                    "uncertain".to_string(),
                    Some("provider_remote_task_deadline".to_string()),
                    Some(executor.executor_execution_id),
                    "uncertain".to_string(),
                    "uncertain".to_string(),
                    "remote_task_deadline".to_string(),
                    "held".to_string(),
                ),
            format!("deadline quarantine state diverged: {state_projection:?}"),
        )?;
        let authority_projection: (i32, i64, i64) = sqlx::query_as(
            r#"
            SELECT policy.allocated_count,
                   (SELECT COUNT(*) FROM provider_remote_task_quarantines quarantine
                    WHERE quarantine.submission_id = task.submission_id),
                   (SELECT COUNT(*) FROM executor_terminal_reductions reduction
                    WHERE reduction.submission_id = task.submission_id
                      AND reduction.state = 'ready')
            FROM provider_remote_tasks task
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = task.executor_execution_id
             AND allocation.submission_id = task.submission_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE task.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            authority_projection == (1, 1, 1),
            format!("deadline quarantine authority diverged: {authority_projection:?}"),
        )?;
        require(
            capacity_heartbeat(&database.pool, executor.executor_execution_id).await?
                == heartbeat_before_deadline,
            "deadline quarantine impersonated provider liveness",
        )?;
        require(
            sqlx::query(
                "UPDATE provider_remote_task_quarantines SET error_code = error_code WHERE submission_id = $1",
            )
            .bind(executor.submission_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "deadline quarantine authority was mutable",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_recovers_committed_artifact_authority() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let executor = seed_attached_remote_task(
            &database.pool,
            &store,
            "artifact-deadline-worker",
            "artifact-deadline",
            700,
            0,
        )
        .await?;
        let lease = store
            .claim_due(&claim_scope(), "artifact-deadline-poller", 60_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "artifact deadline task was not claimable".to_string())?;
        let deadline = lease.context().provider_deadline_at_ms();
        let reserved_event = "internal:artifact-authority-recovery-v1";
        require(
            store
                .record_observation(
                    &lease,
                    &ProviderTaskObservation {
                        event_identity: reserved_event.to_string(),
                        source: ProviderTaskObservationSource::Poll,
                        outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                    },
                )
                .await
                == Err(ProviderTaskStoreError::InvalidInput)
                && store
                    .record_verified_callback(&VerifiedCallbackWakeup {
                        submission_id: executor.submission_id,
                        event_identity: reserved_event.to_string(),
                    })
                    .await
                    == Err(ProviderTaskStoreError::InvalidInput),
            "public provider evidence entered the internal event namespace",
        )?;
        let now = database_now(&database.pool).await?;
        require(
            sqlx::query(
                r#"
                INSERT INTO provider_task_observations
                  (observation_id, submission_id, executor_execution_id,
                   event_identity, source, observed_state, effect_certainty,
                   next_poll_at_ms, poll_owner, poll_lease_epoch,
                   payload_hash, observed_at_ms)
                VALUES ($1, $2, $3, $4, 'poll', 'provider_waiting',
                        'not_applicable', $5, $6, $7, $8, $5)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(executor.submission_id)
            .bind(executor.executor_execution_id)
            .bind(reserved_event)
            .bind(now)
            .bind(&lease.poll_owner)
            .bind(lease.poll_lease_epoch)
            .bind("f".repeat(64))
            .execute(&database.pool)
            .await
            .is_err(),
            "database accepted public evidence with the internal event identity",
        )?;
        let publication = store
            .publish_artifact_authority(
                &lease,
                &artifact_authority(&executor, "artifact-deadline")?,
            )
            .await
            .map_err(debug_error)?;
        sleep_until_database_time(&database.pool, deadline + 20).await?;

        let recovered = store
            .resolve_due_remote_task_deadline(&claim_scope())
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "committed artifact authority was not recoverable".to_string())?;
        require(
            recovered.state == ProviderTaskState::ArtifactReady
                && recovered.artifact_ref.as_deref()
                    == Some(
                        format!("manifest:{}", publication.manifest().manifest_id().simple())
                            .as_str(),
                    ),
            "deadline resolver did not materialize the committed artifact authority",
        )?;
        let projection: (String, String, String, String, i64, i64) = sqlx::query_as(
            r#"
            SELECT execution.state, submission.state, decision.source,
                   allocation.state,
                   (SELECT COUNT(*) FROM provider_task_observations observation
                    WHERE observation.submission_id = submission.submission_id
                      AND observation.source = 'artifact_recovery'),
                   (SELECT COUNT(*) FROM provider_remote_task_quarantines quarantine
                    WHERE quarantine.submission_id = submission.submission_id)
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = execution.resolution_decision_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = execution.executor_execution_id
             AND allocation.submission_id = execution.submission_id
            WHERE submission.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            projection
                == (
                    "succeeded".to_string(),
                    "succeeded".to_string(),
                    "remote_provider_observation".to_string(),
                    "released".to_string(),
                    1,
                    0,
                ),
            format!("artifact authority recovery projection diverged: {projection:?}"),
        )?;
        require(
            store
                .resolve_due_remote_task_deadline(&claim_scope())
                .await
                .map_err(debug_error)?
                .is_none(),
            "artifact authority recovery replayed a terminal deadline",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_error_code_remains_public_provider_evidence() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let executor = seed_attached_remote_task(
            &database.pool,
            &store,
            "deadline-error-worker",
            "deadline-error",
            60_000,
            0,
        )
        .await?;
        let lease = store
            .claim_due(&claim_scope(), "deadline-error-poller", 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "deadline error task was not claimable".to_string())?;
        let resolved = store
            .record_observation(
                &lease,
                &ProviderTaskObservation {
                    event_identity: "deadline-error-observation".to_string(),
                    source: ProviderTaskObservationSource::Poll,
                    outcome: ProviderTaskObservationOutcome::Uncertain {
                        error_code: "provider_remote_task_deadline".to_string(),
                    },
                },
            )
            .await
            .map_err(debug_error)?;
        require(
            resolved.state == ProviderTaskState::Uncertain
                && resolved.error_code.as_deref() == Some("provider_remote_task_deadline"),
            "provider uncertainty error code changed its public projection",
        )?;
        let projection: (Option<Uuid>, String, String, i64) = sqlx::query_as(
            r#"
            SELECT task.deadline_quarantine_id, decision.source, allocation.state,
                   (SELECT COUNT(*) FROM provider_remote_task_quarantines quarantine
                    WHERE quarantine.submission_id = task.submission_id)
            FROM provider_remote_tasks task
            JOIN executor_executions execution
              ON execution.executor_execution_id = task.executor_execution_id
             AND execution.submission_id = task.submission_id
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = execution.resolution_decision_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = task.executor_execution_id
             AND allocation.submission_id = task.submission_id
            WHERE task.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            projection
                == (
                    None,
                    "remote_provider_observation".to_string(),
                    "released".to_string(),
                    0,
                ),
            format!("provider uncertainty was confused with quarantine: {projection:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn poll_claim_stops_after_one_locked_candidate_window() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        for index in 0..65 {
            let lease =
                seed_running_submission(&database.pool, &format!("bounded-poll-worker-{index}"))
                    .await?;
            let reservation = reservation_request(&lease);
            store
                .reserve_submit(&reservation)
                .await
                .map_err(debug_error)?;
            store
                .start_submit(&reservation)
                .await
                .map_err(debug_error)?;
            let operation_id = format!("bounded-poll-operation-{index}");
            store
                .record_submit_receipt(&submit_receipt!(
                    &store,
                    &lease,
                    &operation_id,
                    &format!("bounded-poll-receipt-{index}"),
                ))
                .await
                .map_err(debug_error)?;
            store
                .attach(&attach_request!(
                    &store,
                    &lease,
                    &operation_id,
                    &format!("bounded-poll-attach-{index}"),
                ))
                .await
                .map_err(debug_error)?;
        }

        let first_window: Vec<Uuid> = sqlx::query_scalar(
            r#"
            SELECT submission_id
            FROM provider_remote_tasks
            WHERE provider_id = 'provider-test'
              AND provider_account_id = $1
              AND state = 'provider_waiting'
            ORDER BY GREATEST(
                       next_poll_at_ms,
                       COALESCE(poll_lease_expires_at_ms, next_poll_at_ms)
                     ),
                     submission_id
            LIMIT 64
            "#,
        )
        .bind(ACCOUNT_ID)
        .fetch_all(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            first_window.len() == 64,
            "poll claim fixture did not fill its window",
        )?;
        let mut locker = database.pool.begin().await.map_err(debug_error)?;
        let locked: i64 = sqlx::query_scalar(
            r#"
            WITH locked AS (
              SELECT submission_id
              FROM provider_remote_tasks
              WHERE submission_id = ANY($1)
              FOR UPDATE
            )
            SELECT COUNT(*) FROM locked
            "#,
        )
        .bind(&first_window)
        .fetch_one(&mut *locker)
        .await
        .map_err(debug_error)?;
        require(
            locked == 64,
            "poll claim fixture did not lock its first window",
        )?;

        require(
            store
                .claim_due(&claim_scope(), "bounded-poll-claimant", 5_000)
                .await
                .map_err(debug_error)?
                .is_none(),
            "poll claim scanned beyond its locked 64-row candidate window",
        )?;
        locker.commit().await.map_err(debug_error)?;
        require(
            store
                .claim_due(&claim_scope(), "bounded-poll-after-unlock", 5_000)
                .await
                .map_err(debug_error)?
                .is_some(),
            "poll claim remained empty after its candidate window unlocked",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_resolver_stops_after_one_locked_candidate_window() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        for index in 0..65 {
            seed_attached_remote_task(
                &database.pool,
                &store,
                &format!("bounded-deadline-worker-{index}"),
                &format!("bounded-deadline-{index}"),
                5_000,
                0,
            )
            .await?;
        }
        let latest_deadline: i64 =
            sqlx::query_scalar("SELECT MAX(provider_deadline_at_ms) FROM provider_remote_tasks")
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
        sleep_until_database_time(&database.pool, latest_deadline + 20).await?;

        let mut locked_window = database.pool.begin().await.map_err(debug_error)?;
        let locked: Vec<Uuid> = sqlx::query_scalar(
            r#"
            SELECT submission_id
            FROM provider_remote_tasks
            WHERE provider_id = 'provider-test'
              AND provider_account_id = $1
              AND state = 'provider_waiting'
            ORDER BY provider_deadline_at_ms, submission_id
            LIMIT 64
            FOR UPDATE
            "#,
        )
        .bind(ACCOUNT_ID)
        .fetch_all(&mut *locked_window)
        .await
        .map_err(debug_error)?;
        require(
            locked.len() == 64,
            "failed to lock the first deadline candidate window",
        )?;
        let scope = claim_scope();
        let bounded = tokio::time::timeout(
            Duration::from_secs(2),
            store.resolve_due_remote_task_deadline(&scope),
        )
        .await
        .map_err(|_| "deadline resolver blocked behind its candidate window".to_string())?
        .map_err(debug_error)?;
        require(
            bounded.is_none(),
            "deadline resolver scanned past its fixed locked candidate window",
        )?;
        locked_window.commit().await.map_err(debug_error)?;
        require(
            store
                .resolve_due_remote_task_deadline(&claim_scope())
                .await
                .map_err(debug_error)?
                .is_some(),
            "deadline resolver did not resume after the candidate window unlocked",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_submit_recovery_command_retries_share_one_result() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "same-recovery-command", 200)
                .await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let mut reservation = reservation_request(&executor);
        reservation.provider_timeout_ms = 60_000;
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(250)).await;

        let scope = claim_scope();
        let (left, right) = tokio::join!(
            store.claim_submit_recovery(&scope, "same-command-owner", "claim/retry@1", 5_000,),
            store.claim_submit_recovery(&scope, "same-command-owner", "claim/retry@1", 5_000,),
        );
        let left = left
            .map_err(debug_error)?
            .ok_or_else(|| "first concurrent command retry returned no lease".to_string())?;
        let right = right
            .map_err(debug_error)?
            .ok_or_else(|| "second concurrent command retry returned no lease".to_string())?;
        require(
            left == right,
            "concurrent retries of one command returned different authority",
        )?;
        let command_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM provider_submit_recovery_commands
            WHERE provider_id = 'provider-test' AND provider_account_id = $1
              AND command_owner = 'same-command-owner'
              AND command_id = 'claim/retry@1'
            "#,
        )
        .bind(ACCOUNT_ID)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            command_count == 1,
            "concurrent command retries wrote more than one receipt",
        )?;
        require(
            sqlx::query(
                r#"
                INSERT INTO provider_submit_recovery_commands
                SELECT provider_id, provider_account_id, command_owner,
                       'claim/retry-alias', command_kind, request_duration_ms,
                       submission_id, executor_execution_id, recovery_lease_epoch,
                       claim_claimed_at_ms, claim_lease_expires_at_ms, intent_state,
                       intent_remote_operation_id, intent_provider_request_id,
                       intent_send_started_at_ms, intent_receipt_event_identity,
                       intent_failure_event_identity, intent_failure_error_code,
                       intent_updated_at_ms, created_at_ms
                FROM provider_submit_recovery_commands
                WHERE command_id = 'claim/retry@1'
                "#,
            )
            .execute(&database.pool)
            .await
            .is_err(),
            "database accepted a second command identity for one recovery transition",
        )?;
        require(
            store
                .claim_submit_recovery(&scope, "empty-owner", "empty-claim", 5_000)
                .await
                .map_err(debug_error)?
                .is_none(),
            "live recovery unexpectedly produced a second claim",
        )?;
        let empty_command_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_submit_recovery_commands WHERE command_id = 'empty-claim'",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            empty_command_count == 0,
            "empty recovery polling wrote an unbounded command receipt",
        )?;
        require(
            sqlx::query(
                "UPDATE provider_submit_recovery_commands SET request_duration_ms = request_duration_ms + 1 WHERE command_id = 'claim/retry@1'",
            )
            .execute(&database.pool)
            .await
            .is_err(),
            "database allowed recovery command receipt mutation",
        )?;
        require(
            sqlx::query(
                "DELETE FROM provider_submit_recovery_commands WHERE command_id = 'claim/retry@1'",
            )
            .execute(&database.pool)
            .await
            .is_err(),
            "database allowed recovery command receipt deletion",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_recovery_claim_stops_after_one_locked_candidate_window() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        for index in 0..65 {
            let lease = seed_running_submission_with_lease(
                &database.pool,
                &format!("bounded-recovery-worker-{index}"),
                2_000,
            )
            .await?;
            let mut reservation = reservation_request(&lease);
            reservation.provider_timeout_ms = 60_000;
            store
                .reserve_submit(&reservation)
                .await
                .map_err(debug_error)?;
            store
                .start_submit(&reservation)
                .await
                .map_err(debug_error)?;
        }
        tokio::time::sleep(Duration::from_millis(2_100)).await;

        let first_window: Vec<Uuid> = sqlx::query_scalar(
            r#"
            SELECT submission_id
            FROM provider_submit_recoveries
            WHERE provider_id = 'provider-test'
              AND provider_account_id = $1
              AND state = 'active'
              AND GREATEST(
                    next_recovery_at_ms,
                    COALESCE(recovery_lease_expires_at_ms, next_recovery_at_ms)
                  ) <= floor(
                    extract(epoch FROM statement_timestamp()) * 1000
                  )::BIGINT
            ORDER BY GREATEST(
                       next_recovery_at_ms,
                       COALESCE(recovery_lease_expires_at_ms, next_recovery_at_ms)
                     ),
                     provider_deadline_at_ms,
                     submission_id
            LIMIT 64
            "#,
        )
        .bind(ACCOUNT_ID)
        .fetch_all(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            first_window.len() == 64,
            "submit recovery fixture did not fill its candidate window",
        )?;
        let mut locker = database.pool.begin().await.map_err(debug_error)?;
        let locked: i64 = sqlx::query_scalar(
            r#"
            WITH locked AS (
              SELECT submission_id
              FROM executor_capacity_allocations
              WHERE submission_id = ANY($1)
              FOR UPDATE
            )
            SELECT COUNT(*) FROM locked
            "#,
        )
        .bind(&first_window)
        .fetch_one(&mut *locker)
        .await
        .map_err(debug_error)?;
        require(
            locked == 64,
            "submit recovery fixture did not lock its first candidate window",
        )?;

        require(
            store
                .claim_submit_recovery(
                    &claim_scope(),
                    "bounded-recovery-claimant",
                    "bounded-recovery-claim",
                    5_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "submit recovery claim scanned beyond its locked 64-row candidate window",
        )?;
        locker.commit().await.map_err(debug_error)?;
        require(
            store
                .claim_submit_recovery(
                    &claim_scope(),
                    "bounded-recovery-after-unlock",
                    "bounded-recovery-after-unlock-claim",
                    5_000,
                )
                .await
                .map_err(debug_error)?
                .is_some(),
            "submit recovery claim remained empty after its candidate window unlocked",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_recovery_claim_stops_after_one_expired_candidate_window() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        for index in 0..65 {
            let lease = seed_running_submission_with_lease(
                &database.pool,
                &format!("expired-window-recovery-{index}"),
                2_000,
            )
            .await?;
            let mut reservation = reservation_request(&lease);
            reservation.provider_timeout_ms = if index < 64 { 500 } else { 60_000 };
            store
                .reserve_submit(&reservation)
                .await
                .map_err(debug_error)?;
            store
                .start_submit(&reservation)
                .await
                .map_err(debug_error)?;
        }
        tokio::time::sleep(Duration::from_millis(2_100)).await;

        require(
            store
                .claim_submit_recovery(
                    &claim_scope(),
                    "expired-window-owner",
                    "expired-window-claim",
                    5_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "submit recovery claim scanned past its first 64 expired candidates",
        )?;
        let command_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_submit_recovery_commands WHERE command_id = 'expired-window-claim'",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            command_count == 0,
            "expired recovery window persisted a no-effect command",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_intent_lifecycle_fences_ambiguous_replay_and_late_evidence() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let late_lease =
            seed_running_submission_with_lease(&database.pool, "submit-late-receipt", 1_000)
                .await?;
        let late_reservation = reservation_request(&late_lease);
        store
            .reserve_submit(&late_reservation)
            .await
            .map_err(debug_error)?;
        require(
            matches!(
                store
                    .start_submit(&late_reservation)
                    .await
                    .map_err(debug_error)?,
                ProviderSubmitStart::Acquired(ref intent)
                    if intent.intent.state == ProviderSubmitIntentState::Sending
            ),
            "first submit start did not acquire send authority",
        )?;
        require(
            matches!(
                store
                    .start_submit(&late_reservation)
                    .await
                    .map_err(debug_error)?,
                ProviderSubmitStart::Existing(ref intent)
                    if intent.intent.state == ProviderSubmitIntentState::Sending
            ),
            "submit start replay acquired a second sender",
        )?;
        let executor_store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        require(
            executor_store
                .record_outcome(
                    &late_lease,
                    &ExecutorSubmissionOutcome::Failed {
                        error_code: "wrong_terminal_path".to_string(),
                    },
                )
                .await
                .is_err(),
            "generic runner terminalized an active remote submit protocol",
        )?;
        let recoverable = store
            .load_submit_intent(late_lease.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "sending intent was not durable".to_string())?;
        require(
            recoverable.state == ProviderSubmitIntentState::Sending
                && recoverable.submit_owner == late_lease.executor_owner
                && recoverable.submit_lease_epoch == late_lease.executor_lease_epoch,
            "sending intent lost its frozen recovery identity",
        )?;

        let unknown = submit_failure!(&store,
            &late_lease,
            ProviderSubmitFailureKind::OutcomeUnknown,
            "submit-receipt-lost",
            "submit_effect_unknown",
        );
        let (concurrent_start, outcome_unknown) = tokio::join!(
            store.start_submit(&late_reservation),
            store.record_submit_failure(&unknown)
        );
        let concurrent_start = concurrent_start.map_err(debug_error)?;
        let outcome_unknown = outcome_unknown.map_err(debug_error)?;
        require(
            matches!(
                concurrent_start,
                ProviderSubmitStart::Existing(ref intent)
                    if matches!(
                        intent.intent.state,
                        ProviderSubmitIntentState::Sending
                            | ProviderSubmitIntentState::OutcomeUnknown
                    )
            ),
            "submit replay did not serialize with ambiguous failure recording",
        )?;
        let replay = store
            .record_submit_failure(&unknown)
            .await
            .map_err(debug_error)?;
        require(
            outcome_unknown == replay
                && outcome_unknown.state == ProviderSubmitIntentState::OutcomeUnknown
                && outcome_unknown.remote_operation_id.is_none()
                && outcome_unknown.failure_error_code.as_deref()
                    == Some("submit_effect_unknown"),
            "unknown submit outcome was not durable and idempotent",
        )?;
        let mismatched_failure = RemoteTaskSubmitFailure {
            kind: ProviderSubmitFailureKind::Rejected,
            ..unknown.clone()
        };
        require(
            store.record_submit_failure(&mismatched_failure).await
                == Err(ProviderTaskStoreError::Conflict),
            "failure replay accepted the same evidence under a different kind",
        )?;
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        require(
            executor_store
                .reconcile_expired(100)
                .await
                .map_err(debug_error)?
                == 0,
            "generic lease reconciliation stole submit recovery ownership",
        )?;
        let known = store
            .record_submit_receipt(&submit_receipt!(&store,
                &late_lease,
                "operation-late",
                "late-receipt",
            ))
            .await
            .map_err(debug_error)?;
        require(
            known.state == ProviderSubmitIntentState::OperationKnown
                && known.remote_operation_id.as_deref() == Some("operation-late"),
            "late receipt did not replace the unknown outcome with stable identity",
        )?;
        let failure_replay_after_receipt = store
            .record_submit_failure(&unknown)
            .await
            .map_err(debug_error)?;
        require(
            failure_replay_after_receipt == known
                && known.failure_event_identity.as_deref() == Some("submit-receipt-lost")
                && known.failure_error_code.as_deref() == Some("submit_effect_unknown"),
            "late receipt overwrote or invalidated prior ambiguity evidence",
        )?;
        let recovery = store
            .claim_submit_recovery(
                &claim_scope(),
                "submit-recovery-a",
                "claim-late-receipt",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "expired submit was not recoverable".to_string())?;
        require(
            recovery.intent.state == ProviderSubmitIntentState::OperationKnown
                && recovery.submission_idempotency_key() == late_reservation.idempotency_key
                && recovery.context().invocation_attempt() == 1,
            "recovery claim did not return the frozen invocation context",
        )?;
        let mut recovered_attach =
            attach_request!(&store, &late_lease, "operation-late", "late-attach");
        recovered_attach.recovery_fence = Some(ProviderSubmitRecoveryFence {
            recovery_owner: recovery.recovery_owner.clone(),
            recovery_lease_epoch: recovery.recovery_lease_epoch,
        });
        let attached = store
            .attach(&recovered_attach)
            .await
            .map_err(debug_error)?;
        require(
            attached.state == ProviderTaskState::ProviderWaiting,
            "expired executor lease prevented durable receipt handoff",
        )?;

        let rejected_lease = seed_running_submission(&database.pool, "submit-rejected").await?;
        let rejected_reservation = reservation_request(&rejected_lease);
        store
            .reserve_submit(&rejected_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&rejected_reservation)
            .await
            .map_err(debug_error)?;
        let rejected = submit_failure!(&store,
            &rejected_lease,
            ProviderSubmitFailureKind::Rejected,
            "provider-rejected-event",
            "provider_rejected",
        );
        let rejected_intent = store
            .record_submit_failure(&rejected)
            .await
            .map_err(debug_error)?;
        require(
            rejected_intent.state == ProviderSubmitIntentState::Rejected
                && rejected_intent.remote_operation_id.is_none(),
            "confirmed rejection retained an ambiguous remote operation",
        )?;
        let rejected_projection: (String, String, String, String) = sqlx::query_as(
            r#"
            SELECT execution.state, submission.state, allocation.state,
                   allocation.release_reason
            FROM executor_executions execution
            JOIN provider_submissions submission
              ON submission.executor_execution_id = execution.executor_execution_id
             AND submission.submission_id = execution.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = execution.executor_execution_id
             AND allocation.submission_id = execution.submission_id
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(rejected_lease.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            rejected_projection
                == (
                    "failed".to_string(),
                    "failed".to_string(),
                    "released".to_string(),
                    "remote_submit_outcome".to_string(),
                ),
            "confirmed rejection did not atomically close canonical capacity",
        )?;
        let rejected_reductions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM executor_terminal_reductions WHERE submission_id = $1 AND state = 'ready'",
        )
        .bind(rejected_lease.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            rejected_reductions == 1,
            "confirmed rejection did not enqueue one terminal reduction",
        )?;

        let remote_task_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_remote_tasks")
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
        require(
            remote_task_count == 1,
            "submit recovery created an unexpected number of remote tasks",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_intent_terminal_projections_are_deferred_and_atomic() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());

        let rejected_lease = seed_running_submission(&database.pool, "bare-rejected").await?;
        let rejected_reservation = reservation_request(&rejected_lease);
        store
            .reserve_submit(&rejected_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&rejected_reservation)
            .await
            .map_err(debug_error)?;
        let mut rejected_tx = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE provider_remote_submit_intents
            SET state = 'rejected', failure_event_identity = 'bare-rejected-event',
                failure_error_code = 'provider_rejected', updated_at_ms = updated_at_ms + 1
            WHERE submission_id = $1
            "#,
        )
        .bind(rejected_lease.submission_id)
        .execute(&mut *rejected_tx)
        .await
        .map_err(debug_error)?;
        require(
            rejected_tx.commit().await.is_err(),
            "rejected intent committed without terminal parent and capacity projection",
        )?;
        require(
            store
                .load_submit_intent(rejected_lease.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| intent.state == ProviderSubmitIntentState::Sending),
            "failed rejected projection did not roll back the intent",
        )?;

        let attached_lease = seed_running_submission(&database.pool, "bare-attached").await?;
        let attached_reservation = reservation_request(&attached_lease);
        store
            .reserve_submit(&attached_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&attached_reservation)
            .await
            .map_err(debug_error)?;
        store
            .record_submit_receipt(&submit_receipt!(
                &store,
                &attached_lease,
                "bare-operation",
                "bare-receipt",
            ))
            .await
            .map_err(debug_error)?;
        let mut attached_tx = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE provider_remote_submit_intents
            SET state = 'attached', updated_at_ms = updated_at_ms + 1
            WHERE submission_id = $1
            "#,
        )
        .bind(attached_lease.submission_id)
        .execute(&mut *attached_tx)
        .await
        .map_err(debug_error)?;
        require(
            attached_tx.commit().await.is_err(),
            "attached intent committed without a remote task handoff",
        )?;
        require(
            store
                .load_submit_intent(attached_lease.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| intent.state == ProviderSubmitIntentState::OperationKnown),
            "failed attached projection did not roll back the intent",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_recovery_claim_is_scoped_fenced_and_reclaimable() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "recovery-claim", 250).await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let mut reservation = reservation_request(&executor);
        reservation.provider_timeout_ms = 30_000;
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        let invocation = match store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?
        {
            ProviderSubmitStart::Acquired(invocation) => invocation,
            ProviderSubmitStart::Existing(_) => {
                return Err("first submit start did not acquire authority".to_string());
            }
        };
        require(
            invocation.context().model() == "model-test"
                && invocation.context().command_schema() == "provider-command-v1"
                && invocation.context().command_hash() == executor.command_hash
                && invocation.context().operation_id() == "images.generations"
                && invocation.context().operation_descriptor_revision()
                    == "provider-test/images.generations/v1"
                && invocation.context().operation_descriptor_sha256_v1() == "2".repeat(64)
                && invocation.context().completion_mode() == "remote_task"
                && invocation.context().idempotency_mode() == "submission_bound"
                && invocation.context().operation_binding_version() == 2
                && invocation.context().execution_profile_id() == PROFILE_ID
                && invocation.context().adapter_revision() == "provider-test-adapter-v1"
                && invocation.context().credential_pool_id() == POOL_ID
                && invocation.context().credential_ref() == "test-vault.provider-task.1"
                && invocation.context().credential_revision() == 1
                && invocation.context().credential_auth_sha256() == "1".repeat(64)
                && invocation.context().resource_policy_id() == POLICY_ID
                && invocation.context().resource_policy_revision() == 1
                && invocation.submission_idempotency_key() == reservation.idempotency_key
                && invocation.context().provider_command_sha256()
                    == hex::encode(reservation.provider_command().canonical_sha256())
                && invocation.context().execution_binding_sha256()
                    == invocation.intent.execution_binding_sha256
                && invocation.context().invocation_attempt() == 1
                && invocation.context().provider_timeout_ms() == 30_000
                && invocation.context().provider_deadline_at_ms()
                    - invocation.intent.send_started_at_ms.unwrap_or_default()
                    == 30_000,
            "submit start did not return its exact frozen invocation context",
        )?;
        let context_debug = format!("{:?}", invocation.context());
        require(
            !context_debug.contains("test-vault.provider-task.1")
                && !context_debug.contains(&"1".repeat(64)),
            "provider context Debug output exposed credential identity",
        )?;
        let mut conflicting_timeout = reservation.clone();
        conflicting_timeout.provider_timeout_ms += 1;
        require(
            store.start_submit(&conflicting_timeout).await
                == Err(ProviderTaskStoreError::Conflict),
            "submit replay rewrote its frozen provider timeout",
        )?;
        require(
            sqlx::query(
                "UPDATE provider_submit_recoveries SET provider_deadline_at_ms = provider_deadline_at_ms + 1 WHERE submission_id = $1",
            )
            .bind(executor.submission_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "raw SQL rewrote the absolute provider deadline",
        )?;

        tokio::time::sleep(Duration::from_millis(300)).await;
        let wrong_scope = ProviderTaskClaimScope {
            provider_id: "provider-test".to_string(),
            provider_account_id: Uuid::new_v4(),
        };
        require(
            store
                .claim_submit_recovery(
                    &wrong_scope,
                    "wrong-account",
                    "claim-wrong-account",
                    2_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "recovery claim crossed the provider account boundary",
        )?;

        let scope = claim_scope();
        require(
            sqlx::query(
                r#"
                UPDATE executor_capacity_allocations
                SET last_heartbeat_at_ms =
                    floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + 60_000
                WHERE executor_execution_id = $1
                "#,
            )
            .bind(executor.executor_execution_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "database accepted a future provider capacity heartbeat",
        )?;
        let capacity_before_recovery_claim =
            capacity_heartbeat(&database.pool, executor.executor_execution_id).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let (left, right) = tokio::join!(
            store.claim_submit_recovery(&scope, "recovery-a", "claim-recovery-a", 200),
            store.claim_submit_recovery(&scope, "recovery-b", "claim-recovery-b", 200),
        );
        let mut winners = [left.map_err(debug_error)?, right.map_err(debug_error)?]
            .into_iter()
            .flatten();
        let first = winners
            .next()
            .ok_or_else(|| "due recovery had no claimant".to_string())?;
        require(
            winners.next().is_none()
                && first.intent.submission_id == executor.submission_id
                && first.context() == invocation.context(),
            "concurrent recovery claim did not elect exactly one frozen context",
        )?;
        let first_claim_command = match first.recovery_owner.as_str() {
            "recovery-a" => "claim-recovery-a",
            "recovery-b" => "claim-recovery-b",
            _ => return Err("unexpected recovery claim owner".to_string()),
        };
        let claim_replay = store
            .claim_submit_recovery(
                &scope,
                &first.recovery_owner,
                first_claim_command,
                200,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "exact recovery claim replay disappeared".to_string())?;
        require(
            claim_replay == first,
            "exact recovery claim replay minted different authority",
        )?;
        require(
            store
                .claim_submit_recovery(
                    &scope,
                    &first.recovery_owner,
                    first_claim_command,
                    201,
                )
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "claim command identity accepted different lease parameters",
        )?;
        require(
            store
                .defer_submit_recovery(&first, first_claim_command, 100)
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "claim command identity was reused for a defer command",
        )?;
        store
            .record_submit_receipt(&submit_receipt!(&store,
                &executor,
                "operation-recovered",
                "receipt-recovered",
            ))
            .await
            .map_err(debug_error)?;
        let replay_after_receipt = store
            .claim_submit_recovery(
                &scope,
                &first.recovery_owner,
                first_claim_command,
                200,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "claim replay disappeared after late receipt".to_string())?;
        require(
            replay_after_receipt == first,
            "late receipt rewrote the original recovery claim response",
        )?;
        let capacity_after_recovery_claim =
            capacity_heartbeat(&database.pool, executor.executor_execution_id).await?;
        require(
            capacity_after_recovery_claim > capacity_before_recovery_claim,
            "submit recovery claim did not heartbeat held provider capacity",
        )?;
        require(
            first.recovery_lease_expires_at_ms <= first.context().provider_deadline_at_ms(),
            "submit recovery claim crossed the absolute provider deadline",
        )?;
        let expired_epoch = first.recovery_lease_epoch;
        let mut recovery_locker = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            "SELECT 1 FROM provider_submit_recoveries WHERE submission_id = $1 FOR UPDATE",
        )
        .bind(executor.submission_id)
        .execute(&mut *recovery_locker)
        .await
        .map_err(debug_error)?;
        let stale_store = store.clone();
        let stale_recovery = first.clone();
        let mut blocked_heartbeat = tokio::spawn(async move {
            stale_store
                .heartbeat_submit_recovery(&stale_recovery, 2_000)
                .await
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        require(
            !blocked_heartbeat.is_finished(),
            "submit recovery heartbeat did not wait for its fence lock",
        )?;
        recovery_locker.commit().await.map_err(debug_error)?;
        let stale_heartbeat = tokio::time::timeout(Duration::from_secs(2), &mut blocked_heartbeat)
            .await
            .map_err(|_| "stale submit recovery heartbeat remained blocked".to_string())?
            .map_err(debug_error)?;
        require(
            stale_heartbeat == Err(ProviderTaskStoreError::StaleLease),
            "submit recovery heartbeat revived an expired epoch after a lock wait",
        )?;
        let expired_claim_replay = store
            .claim_submit_recovery(
                &scope,
                &first.recovery_owner,
                first_claim_command,
                200,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "expired claim acknowledgement replay disappeared".to_string())?;
        require(
            expired_claim_replay == first,
            "expired claim acknowledgement replay minted new authority",
        )?;
        let first = store
            .claim_submit_recovery(
                &scope,
                "recovery-after-expired-heartbeat",
                "claim-after-expired-heartbeat",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "expired heartbeat recovery was not reclaimable".to_string())?;
        require(
            first.recovery_lease_epoch == expired_epoch + 1,
            "expired heartbeat reclaim did not advance the recovery epoch",
        )?;
        let historical_claim_replay = store
            .claim_submit_recovery(
                &scope,
                &expired_claim_replay.recovery_owner,
                first_claim_command,
                200,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "superseded claim acknowledgement disappeared".to_string())?;
        require(
            historical_claim_replay == expired_claim_replay,
            "superseded claim command minted or returned different authority",
        )?;
        require(
            sqlx::query(
                r#"
                UPDATE provider_submit_recoveries
                SET recovery_lease_expires_at_ms = provider_deadline_at_ms + 1,
                    updated_at_ms = updated_at_ms + 1
                WHERE submission_id = $1
                "#,
            )
            .bind(executor.submission_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "database accepted a recovery lease beyond the provider deadline",
        )?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let renewed = store
            .heartbeat_submit_recovery(&first, 3_000)
            .await
            .map_err(debug_error)?;
        require(
            renewed.recovery_lease_expires_at_ms > first.recovery_lease_expires_at_ms,
            "recovery heartbeat did not advance monotonically",
        )?;
        require(
            renewed.recovery_lease_expires_at_ms <= renewed.context().provider_deadline_at_ms(),
            "submit recovery heartbeat crossed the absolute provider deadline",
        )?;
        let replay_after_heartbeat = store
            .claim_submit_recovery(
                &scope,
                &first.recovery_owner,
                "claim-after-expired-heartbeat",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "claim replay disappeared after heartbeat".to_string())?;
        require(
            replay_after_heartbeat == first,
            "heartbeat rewrote the original claim command result",
        )?;
        require(
            capacity_heartbeat(&database.pool, executor.executor_execution_id).await?
                > capacity_after_recovery_claim,
            "submit recovery renewal did not heartbeat held provider capacity",
        )?;
        require(
            store
                .claim_submit_recovery(
                    &claim_scope(),
                    "recovery-c",
                    "claim-live-recovery-check",
                    2_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "live recovery lease was stolen",
        )?;

        let forged_defer_at = database_now(&database.pool).await?;
        let mut forged_defer = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO provider_submit_recovery_commands (
                provider_id, provider_account_id, command_owner, command_id,
                command_kind, request_duration_ms, submission_id,
                executor_execution_id, recovery_lease_epoch, created_at_ms
            ) VALUES ($1, $2, $3, 'forged-defer', 'defer', 100, $4, $5, $6, $7)
            "#,
        )
        .bind(&renewed.intent.provider_id)
        .bind(renewed.intent.provider_account_id)
        .bind(&renewed.recovery_owner)
        .bind(renewed.intent.submission_id)
        .bind(renewed.intent.executor_execution_id)
        .bind(renewed.recovery_lease_epoch)
        .bind(forged_defer_at)
        .execute(&mut *forged_defer)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE provider_submit_recoveries
            SET recovery_owner = NULL, recovery_lease_expires_at_ms = NULL,
                recovery_claimed_at_ms = NULL,
                next_recovery_at_ms = LEAST(provider_deadline_at_ms, $3 + 101),
                updated_at_ms = $3
            WHERE submission_id = $1 AND executor_execution_id = $2
            "#,
        )
        .bind(renewed.intent.submission_id)
        .bind(renewed.intent.executor_execution_id)
        .bind(forged_defer_at)
        .execute(&mut *forged_defer)
        .await
        .map_err(debug_error)?;
        require(
            forged_defer.commit().await.is_err(),
            "database committed a defer receipt with a different retry result",
        )?;
        store
            .defer_submit_recovery(&renewed, "defer-recovery-c", 100)
            .await
            .map_err(debug_error)?;
        require(
            store
                .defer_submit_recovery(&renewed, "defer-recovery-c", 101)
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "defer command identity accepted different retry parameters",
        )?;
        store
            .defer_submit_recovery(&renewed, "defer-recovery-c", 100)
            .await
            .map_err(debug_error)?;
        require(
            store
                .claim_submit_recovery(
                    &claim_scope(),
                    "recovery-c",
                    "claim-before-defer-due",
                    2_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "deferred recovery became immediately claimable",
        )?;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let reclaimed = store
            .claim_submit_recovery(
                &claim_scope(),
                "recovery-c",
                "claim-after-defer/due",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "deferred recovery was not reclaimed".to_string())?;
        require(
            reclaimed.recovery_lease_epoch == first.recovery_lease_epoch + 1,
            "recovery reclaim did not advance the fence epoch",
        )?;
        store
            .defer_submit_recovery(&renewed, "defer-recovery-c", 100)
            .await
            .map_err(debug_error)?;

        store
            .record_submit_receipt(&submit_receipt!(&store,
                &executor,
                "operation-recovered",
                "receipt-recovered",
            ))
            .await
            .map_err(debug_error)?;
        let direct = attach_request!(&store, &executor, "operation-recovered", "direct-attach");
        require(
            store.attach(&direct).await == Err(ProviderTaskStoreError::StaleLease),
            "expired executor attached without the recovery fence",
        )?;
        let mut stale = direct.clone();
        stale.recovery_fence = Some(ProviderSubmitRecoveryFence {
            recovery_owner: first.recovery_owner,
            recovery_lease_epoch: first.recovery_lease_epoch,
        });
        require(
            store.attach(&stale).await == Err(ProviderTaskStoreError::StaleLease),
            "stale recovery epoch attached the remote operation",
        )?;
        let mut recovered = direct.clone();
        recovered.recovery_fence = Some(ProviderSubmitRecoveryFence {
            recovery_owner: reclaimed.recovery_owner.clone(),
            recovery_lease_epoch: reclaimed.recovery_lease_epoch,
        });
        let task = store.attach(&recovered).await.map_err(debug_error)?;
        require(
            task.state == ProviderTaskState::ProviderWaiting,
            "live recovery fence did not attach the known operation",
        )?;
        require(
            store.attach(&stale).await == Err(ProviderTaskStoreError::StaleLease)
                && store.attach(&direct).await == Err(ProviderTaskStoreError::StaleLease),
            "completed recovered attach acknowledged stale authority",
        )?;
        require(
            store.attach(&recovered).await.map_err(debug_error)? == task,
            "current recovery fence could not replay its completed attach",
        )?;
        require(
            store
                .heartbeat_submit_recovery(&reclaimed, 2_000)
                .await
                == Err(ProviderTaskStoreError::StaleLease),
            "closed recovery lease remained writable after attach",
        )?;
        let replay_after_close = store
            .claim_submit_recovery(
                &scope,
                &reclaimed.recovery_owner,
                "claim-after-defer/due",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "claim acknowledgement disappeared after close".to_string())?;
        require(
            replay_after_close == reclaimed,
            "closed recovery changed its historical claim acknowledgement",
        )?;
        store
            .defer_submit_recovery(&renewed, "defer-recovery-c", 100)
            .await
            .map_err(debug_error)?;

        let deadline_executor =
            seed_running_submission_with_lease(&database.pool, "recovery-deadline", 20).await?;
        let mut deadline_reservation = reservation_request(&deadline_executor);
        deadline_reservation.provider_timeout_ms = 120;
        store
            .reserve_submit(&deadline_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&deadline_reservation)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(30)).await;
        let deadline_lease = store
            .claim_submit_recovery(
                &claim_scope(),
                "deadline-recovery",
                "claim-deadline-recovery",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "pre-deadline recovery was not claimable".to_string())?;
        store
            .record_submit_receipt(&submit_receipt!(&store,
                &deadline_executor,
                "operation-after-deadline",
                "receipt-before-deadline",
            ))
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let mut deadline_attach = attach_request!(&store,
            &deadline_executor,
            "operation-after-deadline",
            "attach-after-deadline",
        );
        deadline_attach.recovery_fence = Some(ProviderSubmitRecoveryFence {
            recovery_owner: deadline_lease.recovery_owner,
            recovery_lease_epoch: deadline_lease.recovery_lease_epoch,
        });
        require(
            store.attach(&deadline_attach).await == Err(ProviderTaskStoreError::StaleLease),
            "recovery fence attached a remote operation after the provider deadline",
        )?;

        let rejected_executor =
            seed_running_submission_with_lease(&database.pool, "recovery-reject", 200).await?;
        let rejected_reservation = reservation_request(&rejected_executor);
        store
            .reserve_submit(&rejected_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&rejected_reservation)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let rejection_lease = store
            .claim_submit_recovery(
                &claim_scope(),
                "recovery-rejector",
                "claim-recovery-rejector",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "confirmed rejection recovery was not claimable".to_string())?;
        let mut rejection = submit_failure!(&store,
            &rejected_executor,
            ProviderSubmitFailureKind::Rejected,
            "recovered-rejection",
            "provider_rejected",
        );
        rejection.recovery_fence = Some(ProviderSubmitRecoveryFence {
            recovery_owner: rejection_lease.recovery_owner.clone(),
            recovery_lease_epoch: rejection_lease.recovery_lease_epoch,
        });
        let (concurrent_heartbeat, rejected) = tokio::time::timeout(
            Duration::from_secs(2),
            async {
                tokio::join!(
                    store.heartbeat_submit_recovery(&rejection_lease, 2_000),
                    store.record_submit_failure(&rejection),
                )
            },
        )
        .await
        .map_err(|_| "recovery heartbeat and terminal release deadlocked".to_string())?;
        require(
            rejected.map_err(debug_error)?.state == ProviderSubmitIntentState::Rejected,
            "live recovery owner could not atomically commit confirmed rejection",
        )?;
        require(
            matches!(
                concurrent_heartbeat,
                Ok(_) | Err(ProviderTaskStoreError::StaleLease)
            ),
            "heartbeat and terminal release produced an invalid race result",
        )?;
        require(
            store
                .heartbeat_submit_recovery(&rejection_lease, 2_000)
                .await
                == Err(ProviderTaskStoreError::StaleLease),
            "confirmed recovered rejection did not close its recovery lease",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn capacity_heartbeat_migration_fails_closed_on_future_legacy_state() -> TestResult {
    let Some(database) = TestDatabase::new_before_capacity_heartbeats().await? else {
        return Ok(());
    };
    let result = async {
        let executor = seed_running_submission(&database.pool, "capacity-upgrade").await?;
        let mut invalid = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE executor_capacity_allocations
            SET last_heartbeat_at_ms =
                floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + 60_000
            WHERE executor_execution_id = $1
            "#,
        )
        .bind(executor.executor_execution_id)
        .execute(&mut *invalid)
        .await
        .map_err(debug_error)?;
        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0020_provider_capacity_heartbeats.sql"
            ))
            .execute(&mut *invalid)
            .await
            .is_err(),
            "0020 accepted a future legacy capacity heartbeat",
        )?;
        invalid.rollback().await.map_err(debug_error)?;

        let mut valid = database.pool.begin().await.map_err(debug_error)?;
        sqlx::raw_sql(include_str!(
            "../migrations/0020_provider_capacity_heartbeats.sql"
        ))
        .execute(&mut *valid)
        .await
        .map_err(|error| format!("0020 failed after legacy rollback: {error}"))?;
        valid.commit().await.map_err(debug_error)?;
        let constraint_exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
              SELECT 1 FROM pg_constraint
              WHERE conrelid = 'provider_submit_recoveries'::regclass
                AND conname = 'provider_submit_recoveries_lease_deadline_check'
            )
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            constraint_exists,
            "successful 19 -> 20 upgrade omitted its deadline constraint",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn deadline_quarantine_migration_accepts_due_active_recovery() -> TestResult {
    let Some(database) = TestDatabase::new_before_deadline_quarantine().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "deadline-upgrade", 5_000).await?;
        seed_legacy_sending_submit(&database.pool, &executor, 60).await?;
        tokio::time::sleep(Duration::from_millis(90)).await;

        let mut business = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            SELECT submission_id
            FROM provider_remote_submit_intents
            WHERE submission_id = $1
            FOR UPDATE
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&mut *business)
        .await
        .map_err(debug_error)?;
        let mut migration_connection = database.pool.acquire().await.map_err(debug_error)?;
        let migration_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *migration_connection)
            .await
            .map_err(debug_error)?;
        let intent_relation_oid: i64 =
            sqlx::query_scalar("SELECT 'provider_remote_submit_intents'::regclass::oid::bigint")
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
        let mut migration = tokio::spawn(async move {
            sqlx::raw_sql(include_str!(
                "../migrations/0021_provider_submit_deadline_quarantine.sql"
            ))
            .execute(&mut *migration_connection)
            .await
        });
        let lock_observation = match tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let locks: Vec<(String, bool)> = sqlx::query_as(
                    r#"
                    SELECT mode, granted
                    FROM pg_locks
                    WHERE pid = $1 AND relation::bigint = $2
                    ORDER BY mode, granted
                    "#,
                )
                .bind(migration_pid)
                .bind(intent_relation_oid)
                .fetch_all(&database.pool)
                .await
                .map_err(debug_error)?;
                if locks
                    .iter()
                    .any(|(mode, granted)| mode == "AccessExclusiveLock" && !granted)
                {
                    return Ok(locks);
                }
                if migration.is_finished() {
                    return Err("0021 completed before its blocked lock was observable".to_string());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        {
            Ok(observation) => observation,
            Err(_) => Err("0021 did not request its blocking table lock in time".to_string()),
        };
        business.commit().await.map_err(debug_error)?;
        let migration_result =
            match tokio::time::timeout(Duration::from_secs(5), &mut migration).await {
                Ok(Ok(Ok(_))) => Ok(()),
                Ok(Ok(Err(error))) => Err(format!(
                    "20 -> 21 migration rejected due active recovery: {error}"
                )),
                Ok(Err(error)) => Err(format!("20 -> 21 migration task failed: {error}")),
                Err(_) => {
                    migration.abort();
                    let _ = migration.await;
                    Err("20 -> 21 migration remained blocked after business commit".to_string())
                }
            };
        migration_result?;
        sqlx::raw_sql(include_str!(
            "../migrations/0022_provider_capacity_reconciliation.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(|error| format!("21 -> 22 migration failed after lock test: {error}"))?;
        let locks = lock_observation?;
        require(
            !locks
                .iter()
                .any(|(mode, granted)| mode == "ShareRowExclusiveLock" && *granted),
            format!("0021 held a weaker lock before ACCESS EXCLUSIVE: {locks:?}"),
        )?;
        let resolved = resolve_legacy_due_submit_deadline(&database.pool, &executor).await?;
        require(
            resolved == "deadline_quarantined",
            "20 -> 21 migration changed deadline resolver semantics",
        )?;
        let index_definition: String = sqlx::query_scalar(
            r#"
            SELECT lower(indexdef) FROM pg_indexes
            WHERE schemaname = current_schema()
              AND indexname = 'provider_submit_recoveries_deadline_idx'
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            index_definition
                .contains("provider_account_id, provider_deadline_at_ms, submission_id")
                && !index_definition.contains("provider_id, provider_account_id")
                && index_definition.contains("where (state = 'active'::text)"),
            format!("deadline migration created the wrong queue index: {index_definition}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn capacity_heartbeat_skips_policy_scan_without_weakening_counter_guard() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission(&database.pool, "heartbeat-counter-snapshot").await?;
        let mut policy_lock = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("LOCK TABLE executor_resource_policies IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *policy_lock)
            .await
            .map_err(debug_error)?;

        let heartbeat_pool = database.pool.clone();
        let executor_execution_id = executor.executor_execution_id;
        let submission_id = executor.submission_id;
        let mut heartbeat = tokio::spawn(async move {
            sqlx::query(
                r#"
                UPDATE executor_capacity_allocations
                SET last_heartbeat_at_ms = GREATEST(
                    last_heartbeat_at_ms,
                    floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
                )
                WHERE executor_execution_id = $1 AND submission_id = $2
                  AND state = 'held'
                "#,
            )
            .bind(executor_execution_id)
            .bind(submission_id)
            .execute(&heartbeat_pool)
            .await
        });
        let heartbeat_result =
            tokio::time::timeout(Duration::from_millis(500), &mut heartbeat).await;
        policy_lock.rollback().await.map_err(debug_error)?;
        let heartbeat_result = match heartbeat_result {
            Ok(result) => result.map_err(debug_error)?.map_err(debug_error)?,
            Err(_) => {
                heartbeat.abort();
                let _ = heartbeat.await;
                return Err("capacity heartbeat waited on the policy table".to_string());
            }
        };
        require(
            heartbeat_result.rows_affected() == 1,
            "capacity heartbeat did not update its held allocation",
        )?;

        require(
            sqlx::query(
                r#"
                UPDATE executor_resource_policies
                SET allocated_count = allocated_count + 1
                WHERE resource_policy_id = $1 AND revision = 1
                "#,
            )
            .bind(POLICY_ID)
            .execute(&database.pool)
            .await
            .is_err(),
            "capacity guard accepted an unbalanced policy counter",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn capacity_reconciliation_migration_backfills_deadline_quarantine() -> TestResult {
    let Some(database) = TestDatabase::new_before_capacity_reconciliation().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "capacity-upgrade", 5_000).await?;
        seed_legacy_sending_submit(&database.pool, &executor, 40).await?;
        tokio::time::sleep(Duration::from_millis(60)).await;
        force_deadline_quarantine_v21(&database.pool, &executor).await?;

        sqlx::raw_sql(include_str!(
            "../migrations/0022_provider_capacity_reconciliation.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(|error| format!("21 -> 22 capacity migration failed: {error}"))?;
        let backfill: (String, i64, String, i32) = sqlx::query_as(
            r#"
            SELECT reconciliation.state, reconciliation.evidence_revision,
                   allocation.state, policy.allocated_count
            FROM provider_capacity_reconciliations reconciliation
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = reconciliation.executor_execution_id
             AND allocation.submission_id = reconciliation.submission_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE reconciliation.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            backfill == ("active".to_string(), 0, "held".to_string(), 1),
            format!("21 -> 22 backfill diverged: {backfill:?}"),
        )?;
        release_legacy_capacity_no_effect(&database.pool, &executor).await?;
        require(
            sqlx::query_scalar::<_, i32>(
                "SELECT allocated_count FROM executor_resource_policies WHERE resource_policy_id = $1 AND revision = 1",
            )
            .bind(POLICY_ID)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?
                == 0,
            "backfilled reconciliation did not release capacity exactly once",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn capacity_reconciliation_migration_rejects_incomplete_quarantine() -> TestResult {
    let Some(database) = TestDatabase::new_before_capacity_reconciliation().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "capacity-drift", 5_000).await?;
        seed_legacy_sending_submit(&database.pool, &executor, 40).await?;
        tokio::time::sleep(Duration::from_millis(60)).await;
        force_deadline_quarantine_v21(&database.pool, &executor).await?;

        sqlx::query(
            "ALTER TABLE provider_submit_recoveries DISABLE TRIGGER provider_submit_recoveries_reject_delete",
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::query("DELETE FROM provider_submit_recoveries WHERE submission_id = $1")
            .bind(executor.submission_id)
            .execute(&database.pool)
            .await
            .map_err(debug_error)?;
        sqlx::query(
            "ALTER TABLE provider_submit_recoveries ENABLE TRIGGER provider_submit_recoveries_reject_delete",
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0022_provider_capacity_reconciliation.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0022 silently skipped an incomplete deadline quarantine",
        )?;
        require(
            !sqlx::query_scalar::<_, bool>(
                "SELECT to_regclass('provider_capacity_reconciliations') IS NOT NULL",
            )
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?,
            "failed 0022 migration did not roll back atomically",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn atomic_artifact_migration_rejects_unresolved_ready_task() -> TestResult {
    let Some(database) = TestDatabase::new_before_atomic_artifact_resolution().await? else {
        return Ok(());
    };
    let result = async {
        let legacy =
            seed_v22_artifact_ready(&database.pool, "artifact-upgrade", "artifact-upgrade").await?;

        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0023_atomic_provider_artifact_resolution.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0023 accepted an unresolved artifact_ready task",
        )?;
        let migration_rolled_back: bool = sqlx::query_scalar(
            "SELECT to_regprocedure('enforce_provider_terminal_observation_projection()') IS NULL",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            migration_rolled_back,
            "failed 0023 migration did not roll back atomically",
        )?;
        let unresolved_projection: (String, String) = sqlx::query_as(
            r#"
            SELECT task.state, execution.state
            FROM provider_remote_tasks task
            JOIN executor_executions execution
              ON execution.executor_execution_id = task.executor_execution_id
             AND execution.submission_id = task.submission_id
            WHERE task.submission_id = $1
            "#,
        )
        .bind(legacy.executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            unresolved_projection == ("artifact_ready".to_string(), "provider_waiting".to_string()),
            format!("failed 0023 changed legacy evidence: {unresolved_projection:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn atomic_artifact_migration_backfills_canonical_ready_task() -> TestResult {
    let Some(database) = TestDatabase::new_before_atomic_artifact_resolution().await? else {
        return Ok(());
    };
    let result = async {
        let legacy = seed_v22_artifact_ready(
            &database.pool,
            "artifact-upgrade-canonical",
            "artifact-upgrade-canonical",
        )
        .await?;
        let mut canonical = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO executor_resolution_decisions
              (decision_id, executor_execution_id, submission_id, source,
               observation_id, provider_task_observation_id, resolved_state,
               result_manifest_id, error_code, decided_at_ms)
            VALUES ($1, $1, $2, 'remote_provider_observation',
                    NULL, $3, 'succeeded', $2, NULL, $4)
            "#,
        )
        .bind(legacy.executor.executor_execution_id)
        .bind(legacy.executor.submission_id)
        .bind(legacy.observation_id)
        .bind(legacy.observed_at_ms)
        .execute(&mut *canonical)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = 'succeeded', resolution_decision_id = $1,
                finished_at_ms = $3, updated_at_ms = $3, error_code = NULL
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'provider_waiting'
            "#,
        )
        .bind(legacy.executor.executor_execution_id)
        .bind(legacy.executor.submission_id)
        .bind(legacy.observed_at_ms)
        .execute(&mut *canonical)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = 'succeeded', result_manifest_id = $1,
                resolution_decision_id = $2, finished_at_ms = $3,
                updated_at_ms = $3, error_code = NULL
            WHERE executor_execution_id = $2 AND submission_id = $1
              AND state = 'provider_waiting'
            "#,
        )
        .bind(legacy.executor.submission_id)
        .bind(legacy.executor.executor_execution_id)
        .bind(legacy.observed_at_ms)
        .execute(&mut *canonical)
        .await
        .map_err(debug_error)?;
        let policy: (Uuid, i64) = sqlx::query_as(
            r#"
            SELECT resource_policy_id, resource_policy_revision
            FROM executor_capacity_allocations
            WHERE executor_execution_id = $1 AND submission_id = $2
            FOR UPDATE
            "#,
        )
        .bind(legacy.executor.executor_execution_id)
        .bind(legacy.executor.submission_id)
        .fetch_one(&mut *canonical)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE executor_capacity_allocations
            SET state = 'released', released_at_ms = $3,
                release_decision_id = $1, released_state = 'succeeded',
                release_reason = 'remote_provider_observation',
                last_heartbeat_at_ms = GREATEST(last_heartbeat_at_ms, $3)
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'held'
            "#,
        )
        .bind(legacy.executor.executor_execution_id)
        .bind(legacy.executor.submission_id)
        .bind(legacy.observed_at_ms)
        .execute(&mut *canonical)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            UPDATE executor_resource_policies
            SET allocated_count = allocated_count - 1
            WHERE resource_policy_id = $1 AND revision = $2
              AND allocated_count > 0
            "#,
        )
        .bind(policy.0)
        .bind(policy.1)
        .execute(&mut *canonical)
        .await
        .map_err(debug_error)?;
        canonical.commit().await.map_err(debug_error)?;

        sqlx::raw_sql(include_str!(
            "../migrations/0023_atomic_provider_artifact_resolution.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        let fingerprint: (Option<Uuid>, Option<String>, Option<i64>, Option<String>) =
            sqlx::query_as(
                r#"
                SELECT result_manifest_id, artifact_sha256_hex,
                       artifact_byte_size, artifact_media_type
                FROM provider_task_observations
                WHERE observation_id = $1
                "#,
            )
            .bind(legacy.observation_id)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?;
        require(
            fingerprint
                == (
                    Some(legacy.executor.submission_id),
                    Some("c".repeat(64)),
                    Some(128),
                    Some("image/png".to_string()),
                ),
            format!("0023 did not backfill exact artifact evidence: {fingerprint:?}"),
        )?;
        require(
            sqlx::query(
                "UPDATE provider_task_observations SET payload_hash = payload_hash WHERE observation_id = $1",
            )
            .bind(legacy.observation_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "0023 did not restore append-only observation protection",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn atomic_artifact_migration_rolls_back_after_late_failure() -> TestResult {
    let Some(database) = TestDatabase::new_before_atomic_artifact_resolution().await? else {
        return Ok(());
    };
    let result = async {
        let script = format!(
            "{}\nDO $$ BEGIN RAISE EXCEPTION 'forced late migration failure'; END $$;",
            include_str!("../migrations/0023_atomic_provider_artifact_resolution.sql")
        );
        require(
            sqlx::raw_sql(AssertSqlSafe(script))
                .execute(&database.pool)
                .await
                .is_err(),
            "forced late 0023 failure unexpectedly committed",
        )?;
        let residue: (i64, bool, bool, bool) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*)
               FROM information_schema.columns
               WHERE table_schema = current_schema()
                 AND table_name = 'provider_task_observations'
                 AND column_name = 'result_manifest_id'),
              to_regclass('provider_task_observations_manifest_uidx') IS NOT NULL,
              to_regprocedure('enforce_provider_terminal_observation_projection()') IS NOT NULL,
              EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgrelid = 'provider_task_observations'::regclass
                  AND tgname = 'provider_task_observations_reject_mutation'
                  AND NOT tgisinternal
              )
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            residue == (0, false, false, true),
            format!("late 0023 failure left schema residue: {residue:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn recovery_command_migration_requires_drained_claimants() -> TestResult {
    let Some(database) = TestDatabase::new_before_replayable_recovery_commands().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "recovery-command-upgrade", 5_000)
                .await?;
        seed_legacy_sending_submit(&database.pool, &executor, 60_000).await?;
        let now = database_now(&database.pool).await?;
        sqlx::query(
            r#"
            UPDATE provider_submit_recoveries
            SET recovery_owner = 'legacy-recovery-worker',
                recovery_lease_epoch = recovery_lease_epoch + 1,
                recovery_lease_expires_at_ms = $2 + 5_000,
                recovery_claimed_at_ms = $2, updated_at_ms = $2
            WHERE submission_id = $1 AND state = 'active'
            "#,
        )
        .bind(executor.submission_id)
        .bind(now)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;

        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0024_replayable_provider_submit_recovery_commands.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0024 accepted a live legacy recovery claimant",
        )?;
        let command_table_exists: bool = sqlx::query_scalar(
            "SELECT to_regclass('provider_submit_recovery_commands') IS NOT NULL",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            !command_table_exists,
            "failed 0024 migration left command schema residue",
        )?;

        let now = database_now(&database.pool).await?;
        sqlx::query(
            r#"
            UPDATE provider_submit_recoveries
            SET recovery_owner = NULL,
                recovery_lease_expires_at_ms = NULL,
                recovery_claimed_at_ms = NULL,
                next_recovery_at_ms = GREATEST(next_recovery_at_ms, $2 + 100),
                updated_at_ms = $2
            WHERE submission_id = $1 AND state = 'active'
            "#,
        )
        .bind(executor.submission_id)
        .bind(now)
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        sqlx::raw_sql(include_str!(
            "../migrations/0024_replayable_provider_submit_recovery_commands.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(|error| format!("0024 rejected a drained recovery queue: {error}"))?;
        let migrated: (bool, bool) = sqlx::query_as(
            r#"
            SELECT to_regclass('provider_submit_recovery_commands') IS NOT NULL,
                   to_regclass('provider_submit_recovery_commands_pkey') IS NOT NULL
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(migrated == (true, true), "0024 schema is incomplete")?;
        let now = database_now(&database.pool).await?;
        let mut malformed_claim = database.pool.begin().await.map_err(debug_error)?;
        let malformed_epoch: i64 = sqlx::query_scalar(
            r#"
            UPDATE provider_submit_recoveries
            SET recovery_owner = 'malformed-claim-writer',
                recovery_lease_epoch = recovery_lease_epoch + 1,
                recovery_lease_expires_at_ms = $2 + 1,
                recovery_claimed_at_ms = $2, updated_at_ms = $2
            WHERE submission_id = $1 AND state = 'active'
            RETURNING recovery_lease_epoch
            "#,
        )
        .bind(executor.submission_id)
        .bind(now)
        .fetch_one(&mut *malformed_claim)
        .await
        .map_err(debug_error)?;
        let malformed_rejected = sqlx::query(
            r#"
            INSERT INTO provider_submit_recovery_commands (
                provider_id, provider_account_id, command_owner, command_id,
                command_kind, request_duration_ms, submission_id,
                executor_execution_id, recovery_lease_epoch,
                claim_claimed_at_ms, claim_lease_expires_at_ms,
                intent_state, intent_remote_operation_id,
                intent_provider_request_id, intent_send_started_at_ms,
                intent_receipt_event_identity, intent_failure_event_identity,
                intent_failure_error_code, intent_updated_at_ms, created_at_ms
            )
            SELECT recovery.provider_id, recovery.provider_account_id,
                   'malformed-claim-writer', 'malformed-claim', 'claim', 5000,
                   recovery.submission_id, recovery.executor_execution_id,
                   $2, $3, $3 + 1, intent.state, intent.remote_operation_id,
                   intent.provider_request_id, intent.send_started_at_ms,
                   intent.receipt_event_identity, intent.failure_event_identity,
                   intent.failure_error_code, intent.updated_at_ms, $3
            FROM provider_submit_recoveries recovery
            JOIN provider_remote_submit_intents intent
              ON intent.submission_id = recovery.submission_id
             AND intent.executor_execution_id = recovery.executor_execution_id
            WHERE recovery.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .bind(malformed_epoch)
        .bind(now)
        .execute(&mut *malformed_claim)
        .await
        .is_err();
        malformed_claim.rollback().await.map_err(debug_error)?;
        require(
            malformed_rejected,
            "0024 accepted a claim receipt whose duration did not match its lease",
        )?;
        let now = database_now(&database.pool).await?;
        require(
            sqlx::query(
                r#"
                UPDATE provider_submit_recoveries
                SET recovery_owner = 'legacy-after-migration',
                    recovery_lease_epoch = recovery_lease_epoch + 1,
                    recovery_lease_expires_at_ms = $2 + 5_000,
                    recovery_claimed_at_ms = $2, updated_at_ms = $2
                WHERE submission_id = $1 AND state = 'active'
                "#,
            )
            .bind(executor.submission_id)
            .bind(now)
            .execute(&database.pool)
            .await
            .is_err(),
            "0024 allowed an old writer to claim without command evidence",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn recovery_command_migration_rolls_back_after_late_failure() -> TestResult {
    let Some(database) = TestDatabase::new_before_replayable_recovery_commands().await? else {
        return Ok(());
    };
    let result = async {
        let script = format!(
            "{}\nDO $$ BEGIN RAISE EXCEPTION 'forced late migration failure'; END $$;",
            include_str!("../migrations/0024_replayable_provider_submit_recovery_commands.sql")
        );
        require(
            sqlx::raw_sql(AssertSqlSafe(script))
                .execute(&database.pool)
                .await
                .is_err(),
            "forced late 0024 failure unexpectedly committed",
        )?;
        let residue: (bool, bool) = sqlx::query_as(
            r#"
            SELECT to_regclass('provider_submit_recovery_commands') IS NOT NULL,
                   EXISTS (
                     SELECT 1 FROM pg_trigger
                     WHERE tgrelid = 'provider_submit_recoveries'::regclass
                       AND tgname =
                           'provider_submit_recovery_command_projection_check'
                   )
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            residue == (false, false),
            format!("late 0024 failure left schema residue: {residue:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn operation_binding_migration_rejects_unknown_profiles_atomically() -> TestResult {
    let Some(database) = TestDatabase::new_before_operation_binding().await? else {
        return Ok(());
    };
    let result = async {
        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0026_immutable_provider_operation_binding.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0026 inferred an unknown provider operation descriptor",
        )?;
        assert_operation_binding_migration_rolled_back(&database.pool).await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn operation_binding_migration_requires_pre_activation_remote_history() -> TestResult {
    let Some(database) = TestDatabase::new_before_operation_binding().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "operation-binding-evidence").await?;
        seed_legacy_sending_submit(&database.pool, &lease, 60_000).await?;
        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0026_immutable_provider_operation_binding.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0026 accepted legacy remote submit evidence",
        )?;
        assert_operation_binding_migration_rolled_back(&database.pool).await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn operation_binding_migration_requires_active_executions_to_be_drained() -> TestResult {
    let Some(database) = TestDatabase::new_before_operation_binding().await? else {
        return Ok(());
    };
    let result = async {
        seed_running_submission(&database.pool, "operation-binding-active").await?;
        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0026_immutable_provider_operation_binding.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0026 accepted an active executor submission",
        )?;
        assert_operation_binding_migration_rolled_back(&database.pool).await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn operation_binding_migration_backfills_known_codex_profile() -> TestResult {
    let Some(database) = TestDatabase::new_before_operation_binding().await? else {
        return Ok(());
    };
    let result = async {
        sqlx::raw_sql(
            r#"
            ALTER TABLE provider_execution_profiles
              DISABLE TRIGGER provider_execution_profiles_identity;
            DELETE FROM provider_execution_profiles;
            ALTER TABLE provider_execution_profiles
              ENABLE TRIGGER provider_execution_profiles_identity;
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        let pool_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        let policy_id = Uuid::new_v4();
        let profile_id = Uuid::new_v4();
        let now = database_now(&database.pool).await?;
        let mut seed = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            "INSERT INTO provider_credential_pools (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms) VALUES ($1, $2, 'openai-codex', 'enabled', $3, $3)",
        )
        .bind(pool_id)
        .bind(format!("codex-pool-{}", pool_id.simple()))
        .bind(now)
        .execute(&mut *seed)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "INSERT INTO provider_accounts (provider_account_id, credential_pool_id, provider_id, account_key, credential_ref, credential_revision, credential_auth_sha256, state, created_at_ms, updated_at_ms) VALUES ($1, $2, 'openai-codex', $3, $4, 1, $5, 'enabled', $6, $6)",
        )
        .bind(account_id)
        .bind(pool_id)
        .bind(format!("codex-account-{}", account_id.simple()))
        .bind(format!("test-vault.codex.{}", account_id.simple()))
        .bind("a".repeat(64))
        .bind(now)
        .execute(&mut *seed)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "INSERT INTO executor_resource_policies (resource_policy_id, revision, credential_pool_id, provider_account_id, provider_id, execution_class, max_concurrency, state, created_at_ms) VALUES ($1, 1, $2, $3, 'openai-codex', 'inline', 1, 'enabled', $4)",
        )
        .bind(policy_id)
        .bind(pool_id)
        .bind(account_id)
        .bind(now)
        .execute(&mut *seed)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "INSERT INTO provider_execution_profiles (execution_profile_id, profile_key, provider_id, command_schema, adapter_revision, credential_pool_id, provider_account_id, credential_ref, credential_revision, resource_policy_id, resource_policy_revision, state, created_at_ms, updated_at_ms) VALUES ($1, $2, 'openai-codex', 'openai.images.generation.v1', 'codex-cli-v1', $3, $4, $5, 1, $6, 1, 'enabled', $7, $7)",
        )
        .bind(profile_id)
        .bind(format!("codex-profile-{}", profile_id.simple()))
        .bind(pool_id)
        .bind(account_id)
        .bind(format!("test-vault.codex.{}", account_id.simple()))
        .bind(policy_id)
        .bind(now)
        .execute(&mut *seed)
        .await
        .map_err(debug_error)?;
        seed.commit().await.map_err(debug_error)?;

        let forced = format!(
            "{}\nDO $$ BEGIN RAISE EXCEPTION 'forced late 0026 failure'; END $$;",
            include_str!("../migrations/0026_immutable_provider_operation_binding.sql")
        );
        require(
            sqlx::raw_sql(AssertSqlSafe(forced))
                .execute(&database.pool)
                .await
                .is_err(),
            "forced late 0026 failure unexpectedly committed",
        )?;
        assert_operation_binding_migration_rolled_back(&database.pool).await?;

        sqlx::raw_sql(include_str!(
            "../migrations/0026_immutable_provider_operation_binding.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(|error| format!("0026 rejected a known Codex profile: {error}"))?;
        let profile: (String, String, String, String, String) = sqlx::query_as(
            r#"
            SELECT operation_id, operation_descriptor_revision,
                   operation_descriptor_sha256_v1, completion_mode, idempotency_mode
            FROM provider_execution_profiles
            WHERE execution_profile_id = $1
            "#,
        )
        .bind(profile_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            profile
                == (
                    "images.generations".to_string(),
                    "openai-codex/images.generations/v1".to_string(),
                    "f7f3e84594bfda2312d9420aa22108e76b10b3b22c52535ccf768f944d9b7aaa"
                        .to_string(),
                    "inline".to_string(),
                    "submission_bound".to_string(),
                ),
            "0026 backfilled the wrong Codex operation descriptor",
        )?;
        require(
            sqlx::query(
                "UPDATE provider_execution_profiles SET operation_id = 'images.edits' WHERE execution_profile_id = $1",
            )
            .bind(profile_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "0026 did not restore profile immutability",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn operation_binding_migration_preserves_terminal_v1_handoff_replay() -> TestResult {
    let Some(database) = TestDatabase::new_before_operation_binding().await? else {
        return Ok(());
    };
    let result = async {
        let (mut work, executor) =
            seed_legacy_running_submission_and_work(&database.pool, "terminal-v1", 5_000)
                .await?;
        let executor_store = PostgresExecutorSubmissionStore::new(database.pool.clone());
        executor_store
            .record_outcome(
                &executor,
                &ExecutorSubmissionOutcome::Failed {
                    error_code: "terminal_v1_fixture".to_string(),
                },
            )
            .await
            .map_err(debug_error)?;

        // The shared legacy fixture provisions a synthetic provider. Rewrite
        // only its immutable test identity so it represents a historical Codex
        // terminal row before applying 0026.
        let mut rewrite = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SET LOCAL session_replication_role = replica")
            .execute(&mut *rewrite)
            .await
            .map_err(debug_error)?;
        for (statement, identity) in [
            (
                "UPDATE provider_credential_pools SET provider_id = 'openai-codex' WHERE credential_pool_id = $1",
                POOL_ID,
            ),
            (
                "UPDATE provider_accounts SET provider_id = 'openai-codex' WHERE provider_account_id = $1",
                ACCOUNT_ID,
            ),
            (
                "UPDATE executor_resource_policies SET provider_id = 'openai-codex' WHERE resource_policy_id = $1",
                POLICY_ID,
            ),
        ] {
            sqlx::query(statement)
                .bind(identity)
                .execute(&mut *rewrite)
                .await
                .map_err(debug_error)?;
        }
        sqlx::query(
            "UPDATE provider_execution_profiles SET provider_id = 'openai-codex', command_schema = 'openai.images.generation.v1' WHERE execution_profile_id = $1",
        )
        .bind(PROFILE_ID)
        .execute(&mut *rewrite)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "UPDATE jobs SET provider_id = 'openai-codex', model = 'gpt-image-2' WHERE job_id = $1",
        )
        .bind(work.job_id)
        .execute(&mut *rewrite)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "UPDATE job_payloads SET command_schema = 'openai.images.generation.v1' WHERE job_id = $1",
        )
        .bind(work.job_id)
        .execute(&mut *rewrite)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            "UPDATE provider_submissions SET provider_id = 'openai-codex', model = 'gpt-image-2', command_schema = 'openai.images.generation.v1' WHERE submission_id = $1",
        )
        .bind(executor.submission_id)
        .execute(&mut *rewrite)
        .await
        .map_err(debug_error)?;
        rewrite.commit().await.map_err(debug_error)?;
        work.command_schema = "openai.images.generation.v1".to_string();

        sqlx::raw_sql(include_str!(
            "../migrations/0026_immutable_provider_operation_binding.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(|error| format!("0026 rejected a terminal v1 submission: {error}"))?;
        let legacy_binding: (i16, Option<String>, Option<String>, Option<String>) =
            sqlx::query_as(
                r#"
                SELECT operation_binding_version, operation_id,
                       operation_descriptor_sha256_v1, completion_mode
                FROM provider_submissions
                WHERE submission_id = $1
                "#,
            )
            .bind(executor.submission_id)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?;
        require(
            legacy_binding == (1, None, None, None),
            format!("0026 rewrote terminal history instead of retaining v1: {legacy_binding:?}"),
        )?;

        apply_migration_range(&database.pool, 27, i64::MAX).await?;

        let replay = executor_store
            .prepare_and_handoff(&work, PROFILE_ID)
            .await
            .map_err(debug_error)?;
        require(
            replay.len() == 1
                && replay[0].submission_id == executor.submission_id
                && replay[0].executor_execution_id == executor.executor_execution_id,
            "terminal v1 handoff replay minted different executor identities",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_migration_backfills_under_first_lock_and_rolls_back() -> TestResult {
    let Some(database) = TestDatabase::new_before_remote_task_deadline().await? else {
        return Ok(());
    };
    let result = async {
        let (executor, expected_deadline) = seed_v24_remote_task(
            &database.pool,
            "remote-deadline-upgrade",
            "remote-deadline-upgrade",
            60_000,
            false,
        )
        .await?;
        let forced = format!(
            "{}\nDO $$ BEGIN RAISE EXCEPTION 'forced late migration failure'; END $$;",
            include_str!("../migrations/0025_provider_remote_task_deadline_quarantine.sql")
        );
        require(
            sqlx::raw_sql(AssertSqlSafe(forced))
                .execute(&database.pool)
                .await
                .is_err(),
            "forced late 0025 failure unexpectedly committed",
        )?;
        let residue: (i64, bool, bool) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = current_schema()
                 AND table_name = 'provider_remote_tasks'
                 AND column_name IN (
                    'provider_deadline_at_ms', 'deadline_quarantine_id'
                 )),
              to_regclass('provider_remote_task_quarantines') IS NOT NULL,
              EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgrelid = 'provider_remote_tasks'::regclass
                  AND tgname = 'provider_remote_task_update_guard'
                  AND NOT tgisinternal
              )
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            residue == (0, false, true),
            format!("late 0025 failure left schema residue: {residue:?}"),
        )?;

        let mut business = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SELECT 1 FROM provider_remote_tasks WHERE submission_id = $1 FOR UPDATE")
            .bind(executor.submission_id)
            .execute(&mut *business)
            .await
            .map_err(debug_error)?;
        let mut migration_connection = database.pool.acquire().await.map_err(debug_error)?;
        let migration_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *migration_connection)
            .await
            .map_err(debug_error)?;
        let task_relation_oid: i64 =
            sqlx::query_scalar("SELECT 'provider_remote_tasks'::regclass::oid::bigint")
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
        let mut migration = tokio::spawn(async move {
            sqlx::raw_sql(include_str!(
                "../migrations/0025_provider_remote_task_deadline_quarantine.sql"
            ))
            .execute(&mut *migration_connection)
            .await
        });
        let lock_observation = match tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let locks: Vec<(String, bool)> = sqlx::query_as(
                    r#"
                    SELECT mode, granted
                    FROM pg_locks
                    WHERE pid = $1 AND relation::bigint = $2
                    ORDER BY mode, granted
                    "#,
                )
                .bind(migration_pid)
                .bind(task_relation_oid)
                .fetch_all(&database.pool)
                .await
                .map_err(debug_error)?;
                if locks
                    .iter()
                    .any(|(mode, granted)| mode == "AccessExclusiveLock" && !granted)
                {
                    return Ok(locks);
                }
                if migration.is_finished() {
                    return Err("0025 completed before its first lock was observable".to_string());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        {
            Ok(observation) => observation,
            Err(_) => Err("0025 did not request its first table lock in time".to_string()),
        };
        business.commit().await.map_err(debug_error)?;
        match tokio::time::timeout(Duration::from_secs(5), &mut migration).await {
            Ok(Ok(Ok(_))) => {}
            Ok(Ok(Err(error))) => return Err(format!("24 -> 25 migration failed: {error}")),
            Ok(Err(error)) => return Err(format!("24 -> 25 migration task failed: {error}")),
            Err(_) => {
                migration.abort();
                let _ = migration.await;
                return Err("24 -> 25 migration remained blocked after business commit".to_string());
            }
        }
        let locks = lock_observation?;
        require(
            !locks
                .iter()
                .any(|(mode, granted)| mode == "ShareRowExclusiveLock" && *granted),
            format!("0025 acquired a weaker task lock before ACCESS EXCLUSIVE: {locks:?}"),
        )?;
        let migrated: (i64, i64, bool, String) = sqlx::query_as(
            r#"
            SELECT task.provider_deadline_at_ms, recovery.provider_deadline_at_ms,
                   task.deadline_quarantine_id IS NULL,
                   task.state
            FROM provider_remote_tasks task
            JOIN provider_submit_recoveries recovery
              ON recovery.submission_id = task.submission_id
             AND recovery.executor_execution_id = task.executor_execution_id
            WHERE task.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            migrated
                == (
                    expected_deadline,
                    expected_deadline,
                    true,
                    "provider_waiting".to_string(),
                ),
            format!("24 -> 25 deadline backfill diverged: {migrated:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_migration_rejects_active_poll_owner() -> TestResult {
    let Some(database) = TestDatabase::new_before_remote_task_deadline().await? else {
        return Ok(());
    };
    let result = async {
        seed_v24_remote_task(
            &database.pool,
            "remote-deadline-active-upgrade",
            "remote-deadline-active-upgrade",
            60_000,
            true,
        )
        .await?;
        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0025_provider_remote_task_deadline_quarantine.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0025 accepted an active legacy poll owner",
        )?;
        assert_remote_task_deadline_migration_rolled_back(&database.pool).await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_migration_rejects_due_waiting_task() -> TestResult {
    let Some(database) = TestDatabase::new_before_remote_task_deadline().await? else {
        return Ok(());
    };
    let result = async {
        let (_, deadline) = seed_v24_remote_task(
            &database.pool,
            "remote-deadline-due-upgrade",
            "remote-deadline-due-upgrade",
            150,
            false,
        )
        .await?;
        sleep_until_database_time(&database.pool, deadline + 20).await?;
        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0025_provider_remote_task_deadline_quarantine.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0025 silently migrated a waiting task already past its deadline",
        )?;
        assert_remote_task_deadline_migration_rolled_back(&database.pool).await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn remote_task_deadline_migration_rejects_reserved_legacy_event() -> TestResult {
    let Some(database) = TestDatabase::new_before_remote_task_deadline().await? else {
        return Ok(());
    };
    let result = async {
        let (executor, _) = seed_v24_remote_task(
            &database.pool,
            "remote-deadline-event-upgrade",
            "remote-deadline-event-upgrade",
            60_000,
            false,
        )
        .await?;
        let now = database_now(&database.pool).await?;
        sqlx::query(
            r#"
            INSERT INTO provider_task_observations
              (observation_id, submission_id, executor_execution_id,
               event_identity, source, observed_state, effect_certainty,
               next_poll_at_ms, payload_hash, observed_at_ms)
            VALUES ($1, $2, $3, 'internal:artifact-authority-recovery-v1',
                    'verified_callback', 'provider_waiting', 'not_applicable',
                    $4, $5, $4)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(executor.submission_id)
        .bind(executor.executor_execution_id)
        .bind(now)
        .bind("a".repeat(64))
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0025_provider_remote_task_deadline_quarantine.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0025 accepted a legacy event occupying the internal recovery identity",
        )?;
        assert_remote_task_deadline_migration_rolled_back(&database.pool).await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn provider_submit_commit_classifies_deferred_projection_conflicts() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let receipt_executor =
            seed_running_submission_with_lease(&database.pool, "deferred-receipt", 5_000).await?;
        let mut receipt_reservation = reservation_request(&receipt_executor);
        receipt_reservation.provider_timeout_ms = 60_000;
        store
            .reserve_submit(&receipt_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&receipt_reservation)
            .await
            .map_err(debug_error)?;
        sqlx::raw_sql(
            r#"
            CREATE FUNCTION test_deferred_submit_conflict() RETURNS TRIGGER AS $$
            BEGIN
                RAISE EXCEPTION USING
                    ERRCODE = 'P0001',
                    MESSAGE = 'injected deferred provider submit conflict';
            END;
            $$ LANGUAGE plpgsql;

            CREATE CONSTRAINT TRIGGER test_deferred_submit_receipt_conflict
            AFTER UPDATE ON provider_remote_submit_intents
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW
            WHEN (NEW.receipt_event_identity = 'receipt-deferred-conflict')
            EXECUTE FUNCTION test_deferred_submit_conflict();
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            store
                .record_submit_receipt(&submit_receipt!(
                    &store,
                    &receipt_executor,
                    "operation-deferred-conflict",
                    "receipt-deferred-conflict",
                ))
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "deferred receipt projection failure was not classified as conflict",
        )?;
        require(
            store
                .load_submit_intent(receipt_executor.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| {
                    intent.state == ProviderSubmitIntentState::Sending
                        && intent.remote_operation_id.is_none()
                }),
            "failed receipt commit did not roll back its intent update",
        )?;

        let deadline_executor =
            seed_running_submission_with_lease(&database.pool, "deferred-deadline", 5_000).await?;
        let mut deadline_reservation = reservation_request(&deadline_executor);
        deadline_reservation.provider_timeout_ms = 40;
        store
            .reserve_submit(&deadline_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&deadline_reservation)
            .await
            .map_err(debug_error)?;
        sqlx::raw_sql(
            r#"
            CREATE CONSTRAINT TRIGGER test_deferred_submit_deadline_conflict
            AFTER UPDATE ON provider_remote_submit_intents
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW
            WHEN (NEW.state = 'deadline_quarantined')
            EXECUTE FUNCTION test_deferred_submit_conflict();
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(70)).await;
        require(
            store.resolve_due_submit_deadline(&claim_scope()).await
                == Err(ProviderTaskStoreError::Conflict),
            "deferred deadline projection failure was not classified as conflict",
        )?;
        require(
            store
                .load_submit_intent(deadline_executor.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| intent.state == ProviderSubmitIntentState::Sending),
            "failed deadline commit did not roll back quarantine",
        )?;
        sqlx::raw_sql(
            r#"
            DROP TRIGGER test_deferred_submit_deadline_conflict
                ON provider_remote_submit_intents;
            DROP TRIGGER test_deferred_submit_receipt_conflict
                ON provider_remote_submit_intents;
            DROP FUNCTION test_deferred_submit_conflict();
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            store
                .resolve_due_submit_deadline(&claim_scope())
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| {
                    intent.state == ProviderSubmitIntentState::DeadlineQuarantined
                }),
            "deadline did not resolve after removing the injected conflict",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_deadline_quarantines_capacity_and_preserves_late_receipts() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "deadline-quarantine", 5_000)
                .await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let mut reservation = reservation_request(&executor);
        reservation.provider_timeout_ms = 200;
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        let ambiguity = submit_failure!(
            &store,
            &executor,
            ProviderSubmitFailureKind::OutcomeUnknown,
            "deadline-ambiguity",
            "provider_submit_ambiguous",
        );
        store
            .record_submit_failure(&ambiguity)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut plan_tx = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *plan_tx)
            .await
            .map_err(debug_error)?;
        let plan: Vec<String> = sqlx::query_scalar(
            r#"
            EXPLAIN (COSTS OFF)
            SELECT recovery.submission_id, recovery.executor_execution_id
            FROM provider_submit_recoveries recovery
            JOIN provider_remote_submit_intents intent
              ON intent.submission_id = recovery.submission_id
             AND intent.executor_execution_id = recovery.executor_execution_id
            JOIN executor_executions execution
              ON execution.executor_execution_id = recovery.executor_execution_id
             AND execution.submission_id = recovery.submission_id
            JOIN provider_submissions submission
              ON submission.executor_execution_id = recovery.executor_execution_id
             AND submission.submission_id = recovery.submission_id
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = recovery.executor_execution_id
             AND allocation.submission_id = recovery.submission_id
            WHERE recovery.provider_id = $1
              AND recovery.provider_account_id = $2
              AND recovery.state = 'active'
              AND recovery.provider_deadline_at_ms <= floor(
                    extract(epoch FROM statement_timestamp()) * 1000
                  )::BIGINT
              AND intent.state IN ('sending', 'outcome_unknown', 'operation_known')
              AND execution.state = 'running'
              AND submission.state = 'running'
              AND allocation.state = 'held'
            ORDER BY recovery.provider_deadline_at_ms, recovery.submission_id
            FOR UPDATE OF execution, submission, allocation SKIP LOCKED
            LIMIT 1
            "#,
        )
        .bind("provider-test")
        .bind(ACCOUNT_ID)
        .fetch_all(&mut *plan_tx)
        .await
        .map_err(debug_error)?;
        plan_tx.rollback().await.map_err(debug_error)?;
        let plan = plan.join("\n");
        require(
            plan.contains("provider_submit_recoveries_deadline_idx")
                && !plan
                    .lines()
                    .any(|line| line.trim_start().starts_with("Sort")),
            format!("deadline resolver lost its bounded queue plan:\n{plan}"),
        )?;

        let mut resolvers = tokio::task::JoinSet::new();
        for _ in 0..64 {
            let store = store.clone();
            resolvers.spawn(async move { store.resolve_due_submit_deadline(&claim_scope()).await });
        }
        let mut winners = 0;
        while let Some(result) = resolvers.join_next().await {
            if result.map_err(debug_error)?.map_err(debug_error)?.is_some() {
                winners += 1;
            }
        }
        require(
            winners == 1,
            "concurrent deadline resolvers did not elect one winner",
        )?;

        let intent = store
            .load_submit_intent(executor.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "deadline resolver lost its submit intent".to_string())?;
        require(
            intent.state == ProviderSubmitIntentState::DeadlineQuarantined
                && intent.remote_operation_id.is_none(),
            "deadline resolver did not preserve unknown remote effect",
        )?;
        let projection: (String, String, String, String, String, String, i32, String) =
            sqlx::query_as(
                r#"
                SELECT execution.state, submission.state, decision.source,
                       decision.resolved_state, decision.error_code,
                       allocation.state, policy.allocated_count, recovery.state
                FROM executor_executions execution
                JOIN provider_submissions submission
                  ON submission.executor_execution_id = execution.executor_execution_id
                 AND submission.submission_id = execution.submission_id
                JOIN executor_resolution_decisions decision
                  ON decision.decision_id = execution.resolution_decision_id
                JOIN executor_capacity_allocations allocation
                  ON allocation.executor_execution_id = execution.executor_execution_id
                 AND allocation.submission_id = execution.submission_id
                JOIN executor_resource_policies policy
                  ON policy.resource_policy_id = allocation.resource_policy_id
                 AND policy.revision = allocation.resource_policy_revision
                JOIN provider_submit_recoveries recovery
                  ON recovery.executor_execution_id = execution.executor_execution_id
                 AND recovery.submission_id = execution.submission_id
                WHERE execution.executor_execution_id = $1
                "#,
            )
            .bind(executor.executor_execution_id)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?;
        require(
            projection
                == (
                    "uncertain".to_string(),
                    "uncertain".to_string(),
                    "remote_submit_deadline".to_string(),
                    "uncertain".to_string(),
                    "provider_submit_deadline".to_string(),
                    "held".to_string(),
                    1,
                    "closed".to_string(),
                ),
            format!("deadline quarantine projection diverged: {projection:?}"),
        )?;
        let durable_counts: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM executor_resolution_decisions
               WHERE executor_execution_id = $1),
              (SELECT COUNT(*) FROM executor_terminal_reductions
               WHERE executor_execution_id = $1),
              (SELECT COUNT(*) FROM provider_remote_tasks
               WHERE executor_execution_id = $1)
            "#,
        )
        .bind(executor.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            durable_counts == (1, 1, 0),
            format!("deadline resolver duplicated durable evidence: {durable_counts:?}"),
        )?;
        require(
            store
                .resolve_due_submit_deadline(&claim_scope())
                .await
                .map_err(debug_error)?
                .is_none(),
            "resolved deadline remained claimable",
        )?;
        require(
            store
                .record_submit_failure(&ambiguity)
                .await
                .map_err(debug_error)?
                .state
                == ProviderSubmitIntentState::DeadlineQuarantined,
            "exact ambiguity replay lost its terminal evidence",
        )?;
        require(
            sqlx::query(
                r#"
                UPDATE executor_capacity_allocations
                SET state = 'released', released_at_ms = last_heartbeat_at_ms,
                    release_decision_id = $1, released_state = 'uncertain',
                    release_reason = 'terminal_evidence'
                WHERE executor_execution_id = $1
                "#,
            )
            .bind(executor.executor_execution_id)
            .execute(&database.pool)
            .await
            .is_err(),
            "raw SQL released deadline-quarantined provider capacity",
        )?;

        let receipt = submit_receipt!(
            &store,
            &executor,
            "operation-late-after-deadline",
            "receipt-late-after-deadline",
        );
        let late = store
            .record_submit_receipt(&receipt)
            .await
            .map_err(debug_error)?;
        require(
            late.state == ProviderSubmitIntentState::DeadlineQuarantined
                && late.remote_operation_id.as_deref() == Some("operation-late-after-deadline"),
            "late receipt changed the customer terminal result or lost provider identity",
        )?;
        require(
            store
                .record_submit_receipt(&receipt)
                .await
                .map_err(debug_error)?
                == late,
            "exact late receipt replay did not converge",
        )?;
        let conflicting = submit_receipt!(
            &store,
            &executor,
            "operation-conflicting-after-deadline",
            "receipt-conflicting-after-deadline",
        );
        require(
            store.record_submit_receipt(&conflicting).await
                == Err(ProviderTaskStoreError::Conflict),
            "conflicting late receipt rewrote provider identity",
        )?;
        require(
            store
                .attach(&attach_request!(
                    &store,
                    &executor,
                    "operation-late-after-deadline",
                    "attach-late-after-deadline",
                ))
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "deadline-quarantined receipt reopened provider attachment",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_deadline_races_converge_without_deadlock() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let executor =
            seed_running_submission_with_lease(&database.pool, "deadline-receipt-race", 5_000)
                .await?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let mut reservation = reservation_request(&executor);
        reservation.provider_timeout_ms = 80;
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let receipt = submit_receipt!(
            &store,
            &executor,
            "operation-deadline-race",
            "receipt-deadline-race",
        );
        let scope = claim_scope();
        let (receipt_result, deadline_result) =
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(
                    store.record_submit_receipt(&receipt),
                    store.resolve_due_submit_deadline(&scope)
                )
            })
            .await
            .map_err(|_| "deadline and late receipt deadlocked".to_string())?;
        receipt_result.map_err(|error| format!("deadline race receipt: {error:?}"))?;
        if deadline_result
            .map_err(|error| format!("deadline race resolver: {error:?}"))?
            .is_none()
        {
            store
                .resolve_due_submit_deadline(&claim_scope())
                .await
                .map_err(|error| format!("deadline race retry: {error:?}"))?
                .ok_or_else(|| "skipped deadline did not become claimable".to_string())?;
        }
        let intent = store
            .load_submit_intent(executor.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "deadline race lost its intent".to_string())?;
        require(
            intent.state == ProviderSubmitIntentState::DeadlineQuarantined
                && intent.remote_operation_id.as_deref() == Some("operation-deadline-race")
                && intent.receipt_event_identity.as_deref() == Some("receipt-deadline-race"),
            "deadline and receipt race did not preserve both terminal and provider evidence",
        )?;
        let evidence: (i64, i64, String) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM executor_resolution_decisions
               WHERE executor_execution_id = $1),
              (SELECT COUNT(*) FROM provider_remote_tasks
               WHERE executor_execution_id = $1),
              (SELECT state FROM executor_capacity_allocations
               WHERE executor_execution_id = $1)
            "#,
        )
        .bind(executor.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            evidence == (1, 0, "held".to_string()),
            format!("deadline race produced conflicting evidence: {evidence:?}"),
        )?;

        let attach_executor =
            seed_running_submission_with_lease(&database.pool, "deadline-attach-race", 5_000)
                .await?;
        let mut attach_reservation = reservation_request(&attach_executor);
        attach_reservation.provider_timeout_ms = 200;
        store
            .reserve_submit(&attach_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&attach_reservation)
            .await
            .map_err(debug_error)?;
        store
            .record_submit_receipt(&submit_receipt!(
                &store,
                &attach_executor,
                "operation-deadline-attach-race",
                "receipt-deadline-attach-race",
            ))
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let attach = attach_request!(
            &store,
            &attach_executor,
            "operation-deadline-attach-race",
            "attach-deadline-race",
        );
        let attach_scope = claim_scope();
        let (attach_result, deadline_result) =
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(
                    store.attach(&attach),
                    store.resolve_due_submit_deadline(&attach_scope)
                )
            })
            .await
            .map_err(|_| "deadline and attach deadlocked".to_string())?;
        require(
            matches!(
                attach_result,
                Err(ProviderTaskStoreError::Conflict | ProviderTaskStoreError::StaleLease)
            ),
            "deadline-due attach retained authority",
        )?;
        if deadline_result.map_err(debug_error)?.is_none() {
            store
                .resolve_due_submit_deadline(&claim_scope())
                .await
                .map_err(debug_error)?
                .ok_or_else(|| "attach-skipped deadline did not become claimable".to_string())?;
        }
        let attach_projection: (String, i64) = sqlx::query_as(
            r#"
            SELECT intent.state,
                   (SELECT COUNT(*) FROM provider_remote_tasks task
                    WHERE task.submission_id = intent.submission_id)
            FROM provider_remote_submit_intents intent
            WHERE intent.submission_id = $1
            "#,
        )
        .bind(attach_executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            attach_projection == ("deadline_quarantined".to_string(), 0),
            format!("attach race escaped quarantine: {attach_projection:?}"),
        )?;

        let heartbeat_executor =
            seed_running_submission_with_lease(&database.pool, "deadline-heartbeat-race", 100)
                .await?;
        let mut heartbeat_reservation = reservation_request(&heartbeat_executor);
        heartbeat_reservation.provider_timeout_ms = 1_000;
        store
            .reserve_submit(&heartbeat_reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&heartbeat_reservation)
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let recovery = store
            .claim_submit_recovery(
                &claim_scope(),
                "deadline-heartbeat-owner",
                "claim-deadline-heartbeat",
                2_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "pre-deadline recovery was not claimable".to_string())?;
        tokio::time::sleep(Duration::from_millis(900)).await;
        let heartbeat_scope = claim_scope();
        let (heartbeat_result, deadline_result) =
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(
                    store.heartbeat_submit_recovery(&recovery, 2_000),
                    store.resolve_due_submit_deadline(&heartbeat_scope)
                )
            })
            .await
            .map_err(|_| "deadline and recovery heartbeat deadlocked".to_string())?;
        require(
            heartbeat_result == Err(ProviderTaskStoreError::StaleLease),
            "post-deadline recovery heartbeat retained authority",
        )?;
        if deadline_result.map_err(debug_error)?.is_none() {
            store
                .resolve_due_submit_deadline(&claim_scope())
                .await
                .map_err(debug_error)?
                .ok_or_else(|| "heartbeat-skipped deadline did not become claimable".to_string())?;
        }
        require(
            store
                .load_submit_intent(heartbeat_executor.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| {
                    intent.state == ProviderSubmitIntentState::DeadlineQuarantined
                }),
            "heartbeat race did not converge to deadline quarantine",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn capacity_reconciliation_is_scoped_fenced_and_exactly_replayable() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let first = seed_deadline_quarantine(&database.pool, &store, "capacity-release").await?;
        let raw_release_at = database_now(&database.pool).await?;
        require(
            sqlx::query(
                r#"
                UPDATE executor_capacity_allocations
                SET state = 'released', released_at_ms = $2,
                    release_decision_id = $1, released_state = 'uncertain',
                    release_reason = 'provider_capacity_reconciliation',
                    release_reconciliation_id = $1,
                    last_heartbeat_at_ms = GREATEST(last_heartbeat_at_ms, $2)
                WHERE executor_execution_id = $1
                "#,
            )
            .bind(first.executor_execution_id)
            .bind(raw_release_at)
            .execute(&database.pool)
            .await
            .is_err(),
            "raw SQL released quarantined capacity without strong evidence",
        )?;

        let mut plan_tx = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *plan_tx)
            .await
            .map_err(debug_error)?;
        let plan: Vec<String> = sqlx::query_scalar(
            r#"
            EXPLAIN (COSTS OFF)
            WITH queue_candidates AS MATERIALIZED (
              SELECT reconciliation.submission_id,
                     reconciliation.executor_execution_id,
                     reconciliation.available_at_ms,
                     reconciliation.provider_deadline_at_ms
              FROM provider_capacity_reconciliations reconciliation
              WHERE reconciliation.provider_id = $1
                AND reconciliation.provider_account_id = $2
                AND reconciliation.state = 'active'
                AND reconciliation.available_at_ms <= floor(
                      extract(epoch FROM statement_timestamp()) * 1000
                    )::BIGINT
              ORDER BY reconciliation.available_at_ms,
                       reconciliation.provider_deadline_at_ms,
                       reconciliation.submission_id
              LIMIT 64
            )
            SELECT candidate.submission_id
            FROM queue_candidates candidate
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = candidate.executor_execution_id
             AND allocation.submission_id = candidate.submission_id
             AND allocation.state = 'held'
            ORDER BY candidate.available_at_ms,
                     candidate.provider_deadline_at_ms,
                     candidate.submission_id
            FOR UPDATE OF allocation SKIP LOCKED
            LIMIT 1
            "#,
        )
        .bind("provider-test")
        .bind(ACCOUNT_ID)
        .fetch_all(&mut *plan_tx)
        .await
        .map_err(debug_error)?;
        plan_tx.rollback().await.map_err(debug_error)?;
        let plan = plan.join("\n");
        require(
            plan.contains("provider_capacity_reconciliations_claim_idx")
                && plan.matches("Limit").count() >= 2,
            format!("capacity reconciliation lost its bounded queue plan:\n{plan}"),
        )?;

        let wrong_scope = ProviderTaskClaimScope {
            provider_id: "provider-test".to_string(),
            provider_account_id: Uuid::new_v4(),
        };
        require(
            store
                .claim_due_capacity_reconciliation(
                    &wrong_scope,
                    "wrong-capacity-account",
                    "wrong-capacity-claim",
                    5_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "capacity reconciliation crossed its account scope",
        )?;

        let mut claims = tokio::task::JoinSet::new();
        for index in 0..64 {
            let store = store.clone();
            claims.spawn(async move {
                let command = format!("capacity-claim-{index}");
                let result = store
                    .claim_due_capacity_reconciliation(
                        &claim_scope(),
                        "capacity-reconciler",
                        &command,
                        5_000,
                    )
                    .await;
                (command, result)
            });
        }
        let mut winner = None;
        while let Some(result) = claims.join_next().await {
            let (command, claimed) = result.map_err(debug_error)?;
            if let Some(lease) = claimed.map_err(debug_error)? {
                require(winner.is_none(), "more than one capacity claimant won")?;
                winner = Some((command, lease));
            }
        }
        let (claim_command, lease) = winner.ok_or_else(|| "no capacity claimant won".to_string())?;
        let replay_index: String = sqlx::query_scalar(
            r#"
            SELECT lower(indexdef)
            FROM pg_indexes
            WHERE schemaname = current_schema()
              AND indexname =
                  'provider_capacity_reconciliations_claim_command_idx'
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            replay_index.contains("unique index")
                && replay_index.contains(
                    "(provider_id, provider_account_id, last_command_owner, last_command_id)"
                )
                && replay_index.contains("where")
                && replay_index.contains("last_command_kind = 'claim'"),
            format!("claim acknowledgement replay index diverged: {replay_index}"),
        )?;
        let replay = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "capacity-reconciler",
                &claim_command,
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "exact claim replay lost its lease".to_string())?;
        require(replay == lease, "exact claim replay changed the lease epoch")?;
        let mut replay_blocker = database.pool.begin().await.map_err(debug_error)?;
        sqlx::query(
            "SELECT TRUE FROM executor_capacity_allocations WHERE executor_execution_id = $1 FOR UPDATE",
        )
        .bind(first.executor_execution_id)
        .fetch_one(&mut *replay_blocker)
        .await
        .map_err(debug_error)?;
        let replay_store = store.clone();
        let blocked_command = claim_command.clone();
        let mut blocked_replay = tokio::spawn(async move {
            replay_store
                .claim_due_capacity_reconciliation(
                    &claim_scope(),
                    "capacity-reconciler",
                    &blocked_command,
                    5_000,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        require(
            !blocked_replay.is_finished(),
            "exact claim replay skipped a temporarily locked allocation",
        )?;
        replay_blocker.commit().await.map_err(debug_error)?;
        let blocked_result = tokio::time::timeout(Duration::from_secs(3), &mut blocked_replay)
            .await
            .map_err(|_| "exact claim replay remained blocked".to_string())?
            .map_err(debug_error)?
            .map_err(debug_error)?;
        require(
            blocked_result == Some(lease.clone()),
            "lock-delayed claim replay created different authority",
        )?;
        require(
            lease.context().provider_deadline_at_ms()
                == lease.reconciliation.provider_deadline_at_ms,
            "capacity claim re-resolved its frozen provider context",
        )?;

        let evidence = ProviderCapacityEvidence {
            event_identity: "confirmed-no-effect-1".to_string(),
            outcome: ProviderCapacityEvidenceOutcome::ConfirmedNoEffect,
        };
        let released = store
            .record_capacity_evidence(&lease, &evidence)
            .await
            .map_err(debug_error)?;
        require(
            released.state == ProviderCapacityReconciliationState::Released
                && released.evidence.as_ref() == Some(&evidence),
            "strong no-effect evidence was not frozen",
        )?;
        require(
            store
                .record_capacity_evidence(&lease, &evidence)
                .await
                .map_err(debug_error)?
                == released,
            "release acknowledgement loss was not exactly replayable",
        )?;
        require(
            store
                .record_capacity_evidence(
                    &lease,
                    &ProviderCapacityEvidence {
                        event_identity: "conflicting-no-effect".to_string(),
                        outcome: ProviderCapacityEvidenceOutcome::ConfirmedNoEffect,
                    },
                )
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "conflicting capacity evidence rewrote a release",
        )?;
        let projection: (String, String, String, i32) = sqlx::query_as(
            r#"
            SELECT execution.state, allocation.state, allocation.release_reason,
                   policy.allocated_count
            FROM executor_executions execution
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = execution.executor_execution_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE execution.executor_execution_id = $1
            "#,
        )
        .bind(first.executor_execution_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            projection
                == (
                    "uncertain".to_string(),
                    "released".to_string(),
                    "provider_capacity_reconciliation".to_string(),
                    0,
                ),
            format!("capacity evidence release diverged: {projection:?}"),
        )?;
        let late_after_release = store
            .record_submit_receipt(&submit_receipt!(&store,
                &first,
                "operation-after-no-effect",
                "receipt-after-no-effect",
            ))
            .await
            .map_err(debug_error)?;
        require(
            late_after_release.state == ProviderSubmitIntentState::DeadlineQuarantined,
            "late receipt reopened the customer result after release",
        )?;

        let second = seed_deadline_quarantine(&database.pool, &store, "capacity-revision").await?;
        let stale = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "revision-owner-a",
                "revision-claim-a",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "revision test was not claimable".to_string())?;
        let raw_receipt_at = database_now(&database.pool).await?;
        require(
            sqlx::query(
                r#"
                UPDATE provider_remote_submit_intents
                SET remote_operation_id = 'raw-receipt-operation',
                    provider_request_id = 'raw-receipt-request',
                    receipt_event_identity = 'raw-receipt-event',
                    updated_at_ms = $2
                WHERE submission_id = $1
                  AND state = 'deadline_quarantined'
                  AND remote_operation_id IS NULL
                "#,
            )
            .bind(second.submission_id)
            .bind(raw_receipt_at)
            .execute(&database.pool)
            .await
            .is_err(),
            "raw receipt bypassed the reconciliation evidence revision",
        )?;
        store
            .record_submit_receipt(&submit_receipt!(&store,
                &second,
                "operation-before-release",
                "receipt-before-release",
            ))
            .await
            .map_err(debug_error)?;
        require(
            store
                .claim_due_capacity_reconciliation(
                    &claim_scope(),
                    "revision-owner-a",
                    "revision-claim-a",
                    5_000,
                )
                .await
                .map_err(debug_error)?
                == Some(stale.clone()),
            "receipt wake changed the exact claim response snapshot",
        )?;
        require(
            store
                .heartbeat_capacity_reconciliation(&stale, 5_000)
                .await
                == Err(ProviderTaskStoreError::StaleLease),
            "receipt evidence did not fence a stale heartbeat",
        )?;
        store
            .defer_capacity_reconciliation(&stale, "revision-defer-a", 60_000)
            .await
            .map_err(debug_error)?;
        store
            .defer_capacity_reconciliation(&stale, "revision-defer-a", 60_000)
            .await
            .map_err(debug_error)?;
        let fresh = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "revision-owner-b",
                "revision-claim-b",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "fresh receipt evidence was deferred away".to_string())?;
        require(
            fresh.reconciliation_lease_epoch > stale.reconciliation_lease_epoch
                && fresh.claimed_evidence_revision == 1,
            "receipt wake did not advance both lease and evidence fences",
        )?;
        require(
            store
                .record_capacity_evidence(
                    &fresh,
                    &ProviderCapacityEvidence {
                        event_identity: "wrong-terminal-operation".to_string(),
                        outcome: ProviderCapacityEvidenceOutcome::RemoteTerminal {
                            remote_operation_id: "operation-conflict".to_string(),
                            terminal_state: ProviderCapacityTerminalState::Failed,
                        },
                    },
                )
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "terminal evidence changed the durable remote operation",
        )?;
        let terminal = ProviderCapacityEvidence {
            event_identity: "terminal-operation-before-release".to_string(),
            outcome: ProviderCapacityEvidenceOutcome::RemoteTerminal {
                remote_operation_id: "operation-before-release".to_string(),
                terminal_state: ProviderCapacityTerminalState::Succeeded,
            },
        };
        store
            .record_capacity_evidence(&fresh, &terminal)
            .await
            .map_err(debug_error)?;

        let third = seed_deadline_quarantine(&database.pool, &store, "capacity-terminal").await?;
        let third_lease = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "terminal-owner",
                "terminal-claim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "terminal evidence test was not claimable".to_string())?;
        let third_evidence = ProviderCapacityEvidence {
            event_identity: "terminal-with-receipt".to_string(),
            outcome: ProviderCapacityEvidenceOutcome::RemoteTerminal {
                remote_operation_id: "operation-terminal-authority".to_string(),
                terminal_state: ProviderCapacityTerminalState::Canceled,
            },
        };
        require(
            store
                .record_capacity_evidence(&third_lease, &third_evidence)
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "remote terminal evidence established an unowned operation identity",
        )?;
        store
            .record_submit_receipt(&submit_receipt!(&store,
                &third,
                "operation-terminal-authority",
                "receipt-terminal-authority",
            ))
            .await
            .map_err(debug_error)?;
        store
            .defer_capacity_reconciliation(&third_lease, "terminal-defer", 60_000)
            .await
            .map_err(debug_error)?;
        let third_fresh = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "terminal-finisher",
                "terminal-reclaim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "terminal receipt wake was not claimable".to_string())?;
        store
            .record_capacity_evidence(&third_fresh, &third_evidence)
            .await
            .map_err(debug_error)?;
        require(
            store
                .record_submit_receipt(&submit_receipt!(&store,
                    &third,
                    "operation-terminal-conflict",
                    "receipt-terminal-conflict",
                ))
                .await
                == Err(ProviderTaskStoreError::Conflict),
            "late receipt contradicted remote terminal evidence",
        )?;
        store
            .record_submit_receipt(&submit_receipt!(&store,
                &third,
                "operation-terminal-authority",
                "receipt-terminal-authority",
            ))
            .await
            .map_err(debug_error)?;

        let fourth = seed_deadline_quarantine(&database.pool, &store, "capacity-stale-epoch").await?;
        let expired = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "expired-owner",
                "expired-claim",
                40,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "stale epoch test was not claimable".to_string())?;
        tokio::time::sleep(Duration::from_millis(60)).await;
        require(
            store
                .claim_due_capacity_reconciliation(
                    &claim_scope(),
                    "expired-owner",
                    "expired-claim",
                    40,
                )
                .await
                .map_err(debug_error)?
                == Some(expired.clone()),
            "expired claim acknowledgement replay created a new epoch",
        )?;
        let reclaimed = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "reclaimed-owner",
                "reclaimed-claim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "expired capacity lease was not reclaimable".to_string())?;
        let stale_evidence = ProviderCapacityEvidence {
            event_identity: "expired-owner-no-effect".to_string(),
            outcome: ProviderCapacityEvidenceOutcome::ConfirmedNoEffect,
        };
        require(
            store
                .record_capacity_evidence(&expired, &stale_evidence)
                .await
                == Err(ProviderTaskStoreError::StaleLease),
            "expired reconciliation epoch released capacity",
        )?;
        store
            .record_capacity_evidence(
                &reclaimed,
                &ProviderCapacityEvidence {
                    event_identity: "reclaimed-owner-no-effect".to_string(),
                    outcome: ProviderCapacityEvidenceOutcome::ConfirmedNoEffect,
                },
            )
            .await
            .map_err(debug_error)?;
        require(
            store
                .load_submit_intent(fourth.submission_id)
                .await
                .map_err(debug_error)?
                .is_some_and(|intent| {
                    intent.state == ProviderSubmitIntentState::DeadlineQuarantined
                }),
            "capacity reconciliation changed the customer deadline decision",
        )?;

        require(
            sqlx::query_scalar::<_, i32>(
                "SELECT allocated_count FROM executor_resource_policies WHERE resource_policy_id = $1 AND revision = 1",
            )
            .bind(POLICY_ID)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?
                == 0,
            "capacity releases did not balance the shared policy counter",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn capacity_evidence_and_late_receipt_race_converges_without_deadlock() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let executor =
            seed_deadline_quarantine(&database.pool, &store, "capacity-receipt-race").await?;
        let lease = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "capacity-race-owner",
                "capacity-race-claim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "capacity race was not claimable".to_string())?;
        let receipt = submit_receipt!(
            &store,
            &executor,
            "operation-capacity-race",
            "receipt-capacity-race",
        );
        let evidence = ProviderCapacityEvidence {
            event_identity: "terminal-capacity-race".to_string(),
            outcome: ProviderCapacityEvidenceOutcome::RemoteTerminal {
                remote_operation_id: "operation-capacity-race".to_string(),
                terminal_state: ProviderCapacityTerminalState::Succeeded,
            },
        };
        let (receipt_result, evidence_result) =
            tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(
                    store.record_submit_receipt(&receipt),
                    store.record_capacity_evidence(&lease, &evidence)
                )
            })
            .await
            .map_err(|_| "capacity evidence and late receipt deadlocked".to_string())?;
        receipt_result.map_err(debug_error)?;
        match evidence_result {
            Ok(_) => {}
            Err(ProviderTaskStoreError::StaleLease) => {
                store
                    .defer_capacity_reconciliation(&lease, "capacity-race-defer", 60_000)
                    .await
                    .map_err(debug_error)?;
                let fresh = store
                    .claim_due_capacity_reconciliation(
                        &claim_scope(),
                        "capacity-race-finisher",
                        "capacity-race-reclaim",
                        5_000,
                    )
                    .await
                    .map_err(debug_error)?
                    .ok_or_else(|| "receipt wake was lost during race recovery".to_string())?;
                store
                    .record_capacity_evidence(&fresh, &evidence)
                    .await
                    .map_err(debug_error)?;
            }
            Err(error) => return Err(format!("capacity evidence race failed: {error:?}")),
        }
        let projection: (String, String, String, i32) = sqlx::query_as(
            r#"
            SELECT intent.state, allocation.state, reconciliation.state,
                   policy.allocated_count
            FROM provider_remote_submit_intents intent
            JOIN executor_capacity_allocations allocation
              ON allocation.executor_execution_id = intent.executor_execution_id
             AND allocation.submission_id = intent.submission_id
            JOIN provider_capacity_reconciliations reconciliation
              ON reconciliation.executor_execution_id = intent.executor_execution_id
             AND reconciliation.submission_id = intent.submission_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = allocation.resource_policy_id
             AND policy.revision = allocation.resource_policy_revision
            WHERE intent.submission_id = $1
            "#,
        )
        .bind(executor.submission_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            projection
                == (
                    "deadline_quarantined".to_string(),
                    "released".to_string(),
                    "released".to_string(),
                    0,
                ),
            format!("receipt/evidence race did not converge: {projection:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_lifecycle_migration_backfills_attached_receipts() -> TestResult {
    let Some(database) = TestDatabase::new_before_submit_lifecycle().await? else {
        return Ok(());
    };
    let result = async {
        let lease = seed_running_submission(&database.pool, "submit-upgrade").await?;
        seed_legacy_attached_task(&database.pool, &lease).await?;
        sqlx::raw_sql(include_str!(
            "../migrations/0018_provider_submit_lifecycle.sql"
        ))
        .execute(&database.pool)
        .await
        .map_err(|error| format!("0018 attached receipt migration failed: {error}"))?;
        let migrated: (String, String, Option<String>, Option<i64>, Option<String>) =
            sqlx::query_as(
                r#"
                SELECT state, remote_operation_id, provider_request_id,
                       send_started_at_ms, receipt_event_identity
                FROM provider_remote_submit_intents
                WHERE submission_id = $1
                "#,
            )
            .bind(lease.submission_id)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?;
        require(
            migrated.0 == "attached"
                && migrated.1 == "legacy-operation"
                && migrated.2.as_deref() == Some("legacy-request")
                && migrated.3.is_some()
                && migrated.4.as_deref() == Some("legacy-attach-event"),
            format!("0018 did not preserve the attached receipt: {migrated:?}"),
        )?;
        require(
            sqlx::raw_sql(include_str!(
                "../migrations/0019_provider_submit_recovery_leases.sql"
            ))
            .execute(&database.pool)
            .await
            .is_err(),
            "0019 fabricated a provider deadline for legacy remote activity",
        )?;
        let recovery_table_exists: bool = sqlx::query_scalar(
            "SELECT to_regclass(current_schema() || '.provider_submit_recoveries') IS NOT NULL",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            !recovery_table_exists,
            "failed 0019 recovery migration did not roll back atomically",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

async fn seed_legacy_attached_task(pool: &PgPool, lease: &ExecutorSubmissionLease) -> TestResult {
    let now = database_now(pool).await?;
    let observation_id = Uuid::new_v4();
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_remote_submit_intents
          (submission_id, executor_execution_id, provider_id, provider_account_id,
           submit_owner, submit_lease_epoch, idempotency_key, state,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'provider-test', $3, $4, $5, $6, 'reserved', $7, $7)
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(ACCOUNT_ID)
    .bind(&lease.executor_owner)
    .bind(lease.executor_lease_epoch)
    .bind(format!("legacy-submit-{}", lease.submission_id.simple()))
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "UPDATE provider_remote_submit_intents SET state = 'attached', remote_operation_id = 'legacy-operation', updated_at_ms = $2 WHERE submission_id = $1",
    )
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_remote_tasks
          (submission_id, executor_execution_id, provider_id, provider_account_id,
           remote_operation_id, provider_request_id, submit_owner, submit_lease_epoch,
           state, effect_certainty, next_poll_at_ms, state_observation_id,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'provider-test', $3, 'legacy-operation', 'legacy-request',
                $4, $5, 'provider_waiting', 'not_applicable', $6, $7, $6, $6)
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(ACCOUNT_ID)
    .bind(&lease.executor_owner)
    .bind(lease.executor_lease_epoch)
    .bind(now)
    .bind(observation_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_task_observations
          (observation_id, submission_id, executor_execution_id, event_identity,
           source, observed_state, effect_certainty, next_poll_at_ms,
           payload_hash, observed_at_ms)
        VALUES ($1, $2, $3, 'legacy-attach-event', 'submit_attach',
                'provider_waiting', 'not_applicable', $4, $5, $4)
        "#,
    )
    .bind(observation_id)
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(now)
    .bind("a".repeat(64))
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE executor_executions
        SET state = 'provider_waiting', executor_owner = NULL,
            lease_expires_at_ms = NULL, updated_at_ms = $3
        WHERE executor_execution_id = $1 AND submission_id = $2
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "UPDATE provider_submissions SET state = 'provider_waiting', updated_at_ms = $2 WHERE submission_id = $1",
    )
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)
}

struct LegacyArtifactReady {
    executor: ExecutorSubmissionLease,
    observation_id: Uuid,
    observed_at_ms: i64,
}

async fn seed_v22_artifact_ready(
    pool: &PgPool,
    worker: &str,
    identity: &str,
) -> TestResult<LegacyArtifactReady> {
    // Migration 0023 is tested against the exact v22 schema. The temporary task
    // columns model the later deadline fields only long enough to reuse the legacy
    // attach fixture; no current production store is run against this schema.
    sqlx::raw_sql(
        r#"
        ALTER TABLE provider_remote_tasks
          ADD COLUMN IF NOT EXISTS provider_deadline_at_ms BIGINT,
          ADD COLUMN IF NOT EXISTS deadline_quarantine_id UUID
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let executor = seed_running_submission(pool, worker).await?;
    seed_legacy_attached_remote_task(pool, &executor, identity, 60_000, 0).await?;
    let poll_owner = format!("{identity}-poller");
    let claim_at_ms = database_now(pool).await?;
    let authority_id = executor.executor_execution_id.simple().to_string();
    let storage_namespace = format!("filesystem-v1:{identity}");
    let object_key = format!("executor-objects/{}/{}", &authority_id[..2], authority_id);
    let mut publication = pool.begin().await.map_err(debug_error)?;
    let claimed = sqlx::query(
        r#"
        UPDATE provider_remote_tasks
        SET poll_owner = $2, poll_lease_epoch = poll_lease_epoch + 1,
            poll_lease_expires_at_ms = $3 + 60_000,
            poll_claimed_at_ms = $3, updated_at_ms = $3
        WHERE submission_id = $1 AND state = 'provider_waiting'
          AND poll_owner IS NULL AND next_poll_at_ms <= $3
        "#,
    )
    .bind(executor.submission_id)
    .bind(&poll_owner)
    .bind(claim_at_ms)
    .execute(&mut *publication)
    .await
    .map_err(debug_error)?
    .rows_affected();
    require(claimed == 1, "v22 artifact task was not claimable")?;
    sqlx::query(
        r#"
        INSERT INTO executor_artifact_authorities
          (authority_id, executor_execution_id, submission_id, output_id, job_id,
           storage_backend, storage_namespace, object_key, sha256_hex, byte_size,
           media_type, created_at_ms)
        VALUES ($1, $1, $2, $3, $4, 'filesystem-v1', $5, $6, $7, 128,
                'image/png', $8)
        "#,
    )
    .bind(executor.executor_execution_id)
    .bind(executor.submission_id)
    .bind(executor.output_id)
    .bind(executor.job_id)
    .bind(storage_namespace)
    .bind(object_key)
    .bind("c".repeat(64))
    .bind(claim_at_ms)
    .execute(&mut *publication)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO executor_result_manifests
          (manifest_id, artifact_authority_id, executor_execution_id,
           submission_id, created_at_ms)
        VALUES ($1, $2, $2, $1, $3)
        "#,
    )
    .bind(executor.submission_id)
    .bind(executor.executor_execution_id)
    .bind(claim_at_ms)
    .execute(&mut *publication)
    .await
    .map_err(debug_error)?;
    publication.commit().await.map_err(debug_error)?;

    let observation_id = Uuid::new_v4();
    let observed_at_ms = database_now(pool).await?;
    let event_identity = format!("{identity}-ready");
    let artifact_ref = format!("{identity}-object");
    let mut legacy = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_task_observations
          (observation_id, submission_id, executor_execution_id,
           event_identity, source, observed_state, artifact_ref,
           error_code, effect_certainty, next_poll_at_ms, poll_owner,
           poll_lease_epoch, payload_hash, observed_at_ms)
        VALUES ($1, $2, $3, $4, 'poll', 'artifact_ready', $5, NULL,
                'not_applicable', NULL, $6, $7, $8, $9)
        "#,
    )
    .bind(observation_id)
    .bind(executor.submission_id)
    .bind(executor.executor_execution_id)
    .bind(event_identity)
    .bind(&artifact_ref)
    .bind(&poll_owner)
    .bind(1_i64)
    .bind("d".repeat(64))
    .bind(observed_at_ms)
    .execute(&mut *legacy)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_remote_tasks
        SET state = 'artifact_ready', artifact_ref = $2,
            next_poll_at_ms = NULL, poll_owner = NULL,
            poll_lease_expires_at_ms = NULL, poll_claimed_at_ms = NULL,
            state_observation_id = $3, updated_at_ms = $4, terminal_at_ms = $4
        WHERE submission_id = $1
        "#,
    )
    .bind(executor.submission_id)
    .bind(artifact_ref)
    .bind(observation_id)
    .bind(observed_at_ms)
    .execute(&mut *legacy)
    .await
    .map_err(debug_error)?;
    legacy.commit().await.map_err(debug_error)?;
    Ok(LegacyArtifactReady {
        executor,
        observation_id,
        observed_at_ms,
    })
}

fn reservation_request(lease: &ExecutorSubmissionLease) -> RemoteTaskSubmitReservation {
    let mut provider_command = Sha256::new();
    provider_command.update(b"ai-image-factory/provider-test-command/v1\0");
    provider_command.update(lease.submission_id.as_bytes());
    provider_command.update(lease.output_id.as_bytes());
    provider_command.update(lease.output_index.to_be_bytes());
    provider_command.update(lease.command_hash.as_bytes());
    RemoteTaskSubmitReservation::new(
        lease,
        format!("provider-submit-{}", lease.submission_id.simple()),
        OutputSlot::new(
            u32::try_from(lease.output_index).expect("test output index is nonnegative"),
            1,
        )
        .expect("test output projection is valid"),
        provider_command_identity(provider_command.finalize().into()),
        60_000,
    )
}

async fn seed_schema_26_submit(
    pool: &PgPool,
    lease: &ExecutorSubmissionLease,
    reservation: &RemoteTaskSubmitReservation,
    sending: bool,
) -> TestResult {
    let now = database_now(pool).await?;
    let provider_command_sha256 = hex::encode(reservation.provider_command().canonical_sha256());
    let execution_binding_sha256 =
        schema_26_execution_binding_sha256(lease, reservation, &provider_command_sha256);
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_remote_submit_intents
          (submission_id, executor_execution_id, provider_id, provider_account_id,
           submit_owner, submit_lease_epoch, idempotency_key, state,
           provider_command_sha256, execution_binding_sha256,
           provider_timeout_ms, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'provider-test', $3, $4, $5, $6, 'reserved',
                $7, $8, $9, $10, $10)
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(ACCOUNT_ID)
    .bind(&lease.executor_owner)
    .bind(lease.executor_lease_epoch)
    .bind(&reservation.idempotency_key)
    .bind(provider_command_sha256)
    .bind(execution_binding_sha256)
    .bind(reservation.provider_timeout_ms)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    if sending {
        sqlx::query(
            r#"
            UPDATE provider_remote_submit_intents
            SET state = 'sending', send_started_at_ms = $3, updated_at_ms = $3
            WHERE submission_id = $1 AND executor_execution_id = $2
              AND state = 'reserved'
            "#,
        )
        .bind(lease.submission_id)
        .bind(lease.executor_execution_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
        let provider_deadline_at_ms = now + reservation.provider_timeout_ms;
        sqlx::query(
            r#"
            INSERT INTO provider_submit_recoveries
              (submission_id, executor_execution_id, provider_id, provider_account_id,
               invocation_attempt, provider_timeout_ms, provider_deadline_at_ms,
               next_recovery_at_ms, state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'provider-test', $3, 1, $4, $5, $6,
                    'active', $7, $7)
            "#,
        )
        .bind(lease.submission_id)
        .bind(lease.executor_execution_id)
        .bind(ACCOUNT_ID)
        .bind(reservation.provider_timeout_ms)
        .bind(provider_deadline_at_ms)
        .bind(
            lease
                .executor_lease_expires_at_ms
                .min(provider_deadline_at_ms),
        )
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
    }
    tx.commit().await.map_err(debug_error)
}

fn schema_26_execution_binding_sha256(
    lease: &ExecutorSubmissionLease,
    reservation: &RemoteTaskSubmitReservation,
    provider_command_sha256: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"ai-image-factory/provider-execution-binding/v1\0");
    for (name, value) in [
        (
            "submission_id",
            reservation.submission_id.as_bytes().as_slice(),
        ),
        (
            "executor_execution_id",
            reservation.executor_execution_id.as_bytes().as_slice(),
        ),
        ("output_id", lease.output_id.as_bytes().as_slice()),
        ("provider_id", lease.provider_id.as_bytes()),
        ("provider_account_id", ACCOUNT_ID.as_bytes().as_slice()),
        ("model", lease.model.as_bytes()),
        ("command_schema", lease.command_schema.as_bytes()),
        ("command_hash", lease.command_hash.as_bytes()),
        ("operation_id", b"images.generations".as_slice()),
        (
            "operation_descriptor_revision",
            b"provider-test/images.generations/v1".as_slice(),
        ),
        ("operation_descriptor_sha256_v1", "2".repeat(64).as_bytes()),
        ("completion_mode", b"remote_task".as_slice()),
        ("idempotency_mode", b"submission_bound".as_slice()),
        ("operation_binding_version", 2_i16.to_be_bytes().as_slice()),
        ("execution_profile_id", PROFILE_ID.as_bytes().as_slice()),
        ("adapter_revision", lease.adapter_revision.as_bytes()),
        ("credential_pool_id", POOL_ID.as_bytes().as_slice()),
        ("credential_ref", b"test-vault.provider-task.1".as_slice()),
        ("credential_revision", 1_i64.to_be_bytes().as_slice()),
        ("credential_auth_sha256", "1".repeat(64).as_bytes()),
        ("resource_policy_id", POLICY_ID.as_bytes().as_slice()),
        ("resource_policy_revision", 1_i64.to_be_bytes().as_slice()),
        ("executor_owner", reservation.executor_owner.as_bytes()),
        (
            "executor_lease_epoch",
            reservation.executor_lease_epoch.to_be_bytes().as_slice(),
        ),
        (
            "submission_idempotency_key",
            reservation.idempotency_key.as_bytes(),
        ),
        (
            "provider_command_sha256",
            provider_command_sha256.as_bytes(),
        ),
        (
            "provider_timeout_ms",
            reservation.provider_timeout_ms.to_be_bytes().as_slice(),
        ),
    ] {
        digest.update((name.len() as u64).to_be_bytes());
        digest.update(name.as_bytes());
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    hex::encode(digest.finalize())
}

fn orchestrator_command(lease: &ExecutorSubmissionLease) -> SingleOutputCommand<TestPayload> {
    SingleOutputCommand::new(
        OutputSlot::new(0, 1).expect("one test output is valid"),
        TestPayload::bound_to(
            b"provider-test-payload".to_vec(),
            lease.command_hash.clone(),
        ),
    )
    .expect("provider test command is canonical")
}

#[cfg(unix)]
fn seed_remote_submit_launch_prefix(
    root: &std::path::Path,
    intent: &ProviderSubmitIntent,
    context: &ProviderExecutionContext,
    command: &SingleOutputCommand<TestPayload>,
) -> TestResult<Uuid> {
    use std::{
        fs::{self, File, OpenOptions},
        io::Write,
        os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    };

    let entry = root.join(intent.submission_id.simple().to_string());
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root)
        .map_err(debug_error)?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&entry)
        .map_err(debug_error)?;
    let command_bytes = command.canonical_payload();
    let spec = json!({
        "schema_version": 1,
        "submission_id": intent.submission_id,
        "executor_execution_id": intent.executor_execution_id,
        "provider_id": intent.provider_id,
        "provider_account_id": intent.provider_account_id,
        "submit_owner": intent.submit_owner,
        "submit_lease_epoch": intent.submit_lease_epoch,
        "output_index": intent.output_index,
        "output_total": intent.output_total,
        "command_schema": context.command_schema(),
        "adapter_revision": context.adapter_revision(),
        "provider_command_sha256": context.provider_command_sha256(),
        "execution_binding_sha256": context.execution_binding_sha256(),
        "execution_profile_id": context.execution_profile_id(),
        "credential_pool_id": context.credential_pool_id(),
        "credential_revision": context.credential_revision(),
        "credential_auth_sha256": context.credential_auth_sha256(),
        "resource_policy_id": context.resource_policy_id(),
        "resource_policy_revision": context.resource_policy_revision(),
        "provider_deadline_at_ms": context.provider_deadline_at_ms(),
        "command_bytes_sha256": hex::encode(Sha256::digest(command_bytes)),
        "command_byte_size": command_bytes.len(),
    });
    let launch_nonce = Uuid::new_v4();
    let launch = json!({
        "execution_binding_sha256": context.execution_binding_sha256(),
        "launch_nonce": launch_nonce,
    });
    for (name, bytes) in [
        ("command.bin", command_bytes.to_vec()),
        ("spec.json", serde_json::to_vec(&spec).map_err(debug_error)?),
        (
            "launch.json",
            serde_json::to_vec(&launch).map_err(debug_error)?,
        ),
    ] {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(entry.join(name))
            .map_err(debug_error)?;
        file.write_all(&bytes).map_err(debug_error)?;
        file.sync_all().map_err(debug_error)?;
    }
    File::open(&entry)
        .and_then(|directory| directory.sync_all())
        .map_err(debug_error)?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(debug_error)?;
    Ok(launch_nonce)
}

fn orchestrator_work(
    lease: &ExecutorSubmissionLease,
) -> TestResult<ProviderSubmitWork<ScriptedFakeProvider>> {
    ProviderSubmitWork::new(lease, orchestrator_command(lease)).map_err(debug_error)
}

fn provider_command_identity(canonical_sha256: [u8; 32]) -> ProviderCommandIdentity {
    struct TestCommandPayload([u8; 32]);

    impl CanonicalCommandPayload for TestCommandPayload {
        const SCHEMA_ID: &'static str = "provider-test.command.v1";
        const ADAPTER_REVISION: &'static str = "provider-test-adapter-v1";

        fn source_command_sha256(&self) -> &str {
            "1111111111111111111111111111111111111111111111111111111111111111"
        }

        fn into_canonical_bytes(self, _output: OutputSlot) -> Vec<u8> {
            self.0.to_vec()
        }
    }

    SingleOutputCommand::new(
        OutputSlot::new(0, 1).expect("one test output is valid"),
        TestCommandPayload(canonical_sha256),
    )
    .expect("provider test command identity is valid")
    .identity()
}

async fn submit_failure_request(
    store: &PostgresProviderTaskStore,
    lease: &ExecutorSubmissionLease,
    kind: ProviderSubmitFailureKind,
    event_identity: &str,
    error_code: &str,
) -> TestResult<RemoteTaskSubmitFailure> {
    let execution_binding_sha256 = binding_sha256(store, lease).await?;
    Ok(RemoteTaskSubmitFailure {
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        executor_owner: lease.executor_owner.clone(),
        executor_lease_epoch: lease.executor_lease_epoch,
        kind,
        event_identity: event_identity.to_string(),
        error_code: error_code.to_string(),
        execution_binding_sha256,
        recovery_fence: None,
    })
}

async fn submit_receipt_request(
    store: &PostgresProviderTaskStore,
    lease: &ExecutorSubmissionLease,
    operation: &str,
    event: &str,
) -> TestResult<RemoteTaskSubmitReceipt> {
    let execution_binding_sha256 = binding_sha256(store, lease).await?;
    Ok(RemoteTaskSubmitReceipt {
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        executor_owner: lease.executor_owner.clone(),
        executor_lease_epoch: lease.executor_lease_epoch,
        remote_operation_id: operation.to_string(),
        provider_request_id: Some(format!("request-{operation}")),
        event_identity: event.to_string(),
        execution_binding_sha256,
    })
}

async fn bound_attach_request(
    store: &PostgresProviderTaskStore,
    lease: &ExecutorSubmissionLease,
    operation: &str,
    event: &str,
) -> TestResult<RemoteTaskAttach> {
    let execution_binding_sha256 = binding_sha256(store, lease).await?;
    Ok(RemoteTaskAttach {
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        executor_owner: lease.executor_owner.clone(),
        executor_lease_epoch: lease.executor_lease_epoch,
        remote_operation_id: operation.to_string(),
        provider_request_id: Some(format!("request-{operation}")),
        event_identity: event.to_string(),
        execution_binding_sha256,
        poll_after_ms: 0,
        recovery_fence: None,
    })
}

async fn binding_sha256(
    store: &PostgresProviderTaskStore,
    lease: &ExecutorSubmissionLease,
) -> TestResult<String> {
    store
        .load_submit_intent(lease.submission_id)
        .await
        .map_err(debug_error)?
        .map(|intent| intent.execution_binding_sha256)
        .ok_or_else(|| "provider submit intent is unavailable".to_string())
}

fn claim_scope() -> ProviderTaskClaimScope {
    ProviderTaskClaimScope {
        provider_id: "provider-test".to_string(),
        provider_account_id: ACCOUNT_ID,
    }
}

async fn seed_legacy_sending_submit(
    pool: &PgPool,
    lease: &ExecutorSubmissionLease,
    provider_timeout_ms: i64,
) -> TestResult<i64> {
    let reservation = reservation_request(lease);
    let now = database_now(pool).await?;
    let provider_deadline_at_ms = now + provider_timeout_ms;
    let next_recovery_at_ms = lease
        .executor_lease_expires_at_ms
        .min(provider_deadline_at_ms);
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_remote_submit_intents
          (submission_id, executor_execution_id, provider_id, provider_account_id,
           submit_owner, submit_lease_epoch, idempotency_key, state,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'provider-test', $3, $4, $5, $6, 'reserved', $7, $7)
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(ACCOUNT_ID)
    .bind(&lease.executor_owner)
    .bind(lease.executor_lease_epoch)
    .bind(&reservation.idempotency_key)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_remote_submit_intents
        SET state = 'sending', send_started_at_ms = $3, updated_at_ms = $3
        WHERE submission_id = $1 AND executor_execution_id = $2 AND state = 'reserved'
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_submit_recoveries
          (submission_id, executor_execution_id, provider_id, provider_account_id,
           invocation_attempt, provider_timeout_ms, provider_deadline_at_ms,
           next_recovery_at_ms, state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'provider-test', $3, 1, $4, $5, $6, 'active', $7, $7)
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(ACCOUNT_ID)
    .bind(provider_timeout_ms)
    .bind(provider_deadline_at_ms)
    .bind(next_recovery_at_ms)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)?;
    Ok(provider_deadline_at_ms)
}

async fn seed_attached_remote_task(
    pool: &PgPool,
    store: &PostgresProviderTaskStore,
    worker: &str,
    identity: &str,
    provider_timeout_ms: i64,
    poll_after_ms: i64,
) -> TestResult<ExecutorSubmissionLease> {
    let lease = seed_running_submission_with_lease(pool, worker, 5_000).await?;
    if !operation_binding_exists(pool).await? {
        seed_legacy_attached_remote_task(
            pool,
            &lease,
            identity,
            provider_timeout_ms,
            poll_after_ms,
        )
        .await?;
        return Ok(lease);
    }
    let mut reservation = reservation_request(&lease);
    reservation.provider_timeout_ms = provider_timeout_ms;
    store
        .reserve_submit(&reservation)
        .await
        .map_err(debug_error)?;
    store
        .start_submit(&reservation)
        .await
        .map_err(debug_error)?;
    let operation = format!("{identity}-operation");
    store
        .record_submit_receipt(&submit_receipt!(
            &store,
            &lease,
            &operation,
            &format!("{identity}-receipt"),
        ))
        .await
        .map_err(debug_error)?;
    let mut attach = attach_request!(&store, &lease, &operation, &format!("{identity}-attach"));
    attach.poll_after_ms = poll_after_ms;
    store.attach(&attach).await.map_err(debug_error)?;
    Ok(lease)
}

#[allow(clippy::too_many_arguments)]
async fn seed_attached_remote_task_for_runtime_profile(
    pool: &PgPool,
    store: &PostgresProviderTaskStore,
    worker: &str,
    identity: &str,
    provider_timeout_ms: i64,
    poll_after_ms: i64,
    execution_profile_id: Uuid,
    provider_id: &str,
    model: &str,
    command_schema: &str,
    adapter_revision: &str,
) -> TestResult<ExecutorSubmissionLease> {
    let lease = seed_running_submission_for_runtime_profile(
        pool,
        worker,
        5_000,
        execution_profile_id,
        provider_id,
        model,
        command_schema,
        adapter_revision,
    )
    .await?;
    let mut reservation = reservation_request(&lease);
    reservation.provider_timeout_ms = provider_timeout_ms;
    store
        .reserve_submit(&reservation)
        .await
        .map_err(debug_error)?;
    store
        .start_submit(&reservation)
        .await
        .map_err(debug_error)?;
    let operation = format!("{identity}-operation");
    store
        .record_submit_receipt(&submit_receipt!(
            store,
            &lease,
            &operation,
            &format!("{identity}-receipt"),
        ))
        .await
        .map_err(debug_error)?;
    let mut attach = attach_request!(store, &lease, &operation, &format!("{identity}-attach"));
    attach.poll_after_ms = poll_after_ms;
    store.attach(&attach).await.map_err(debug_error)?;
    Ok(lease)
}

async fn seed_legacy_attached_remote_task(
    pool: &PgPool,
    lease: &ExecutorSubmissionLease,
    identity: &str,
    provider_timeout_ms: i64,
    poll_after_ms: i64,
) -> TestResult {
    let provider_deadline_at_ms =
        seed_legacy_sending_submit(pool, lease, provider_timeout_ms).await?;
    let operation = format!("{identity}-operation");
    let request_id = format!("request-{operation}");
    let receipt_event = format!("{identity}-receipt");
    let attach_event = format!("{identity}-attach");
    let observation_id = Uuid::new_v4();
    let now = database_now(pool).await?;
    let next_poll_at_ms = (now + poll_after_ms).min(provider_deadline_at_ms);
    let payload_hash = legacy_observation_hash(
        "submit_attach",
        "provider_waiting",
        "not_applicable",
        Some(next_poll_at_ms),
        None,
        None,
    );
    let has_task_deadline: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema = current_schema()
            AND table_name = 'provider_remote_tasks'
            AND column_name = 'provider_deadline_at_ms'
        )
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_remote_submit_intents
        SET state = 'operation_known', remote_operation_id = $3,
            provider_request_id = $4, receipt_event_identity = $5,
            updated_at_ms = $6
        WHERE submission_id = $1 AND executor_execution_id = $2 AND state = 'sending'
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(&operation)
    .bind(&request_id)
    .bind(&receipt_event)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_remote_submit_intents
        SET state = 'attached', updated_at_ms = $3
        WHERE submission_id = $1 AND executor_execution_id = $2
          AND state = 'operation_known'
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    if has_task_deadline {
        sqlx::query(
            r#"
            INSERT INTO provider_remote_tasks
              (submission_id, executor_execution_id, provider_id, provider_account_id,
               remote_operation_id, provider_request_id, submit_owner, submit_lease_epoch,
               attach_recovery_owner, attach_recovery_lease_epoch,
               provider_deadline_at_ms, state, effect_certainty, next_poll_at_ms,
               state_observation_id, created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'provider-test', $3, $4, $5, $6, $7, NULL, NULL,
                    $8, 'provider_waiting', 'not_applicable', $9, $10, $11, $11)
            "#,
        )
        .bind(lease.submission_id)
        .bind(lease.executor_execution_id)
        .bind(ACCOUNT_ID)
        .bind(&operation)
        .bind(&request_id)
        .bind(&lease.executor_owner)
        .bind(lease.executor_lease_epoch)
        .bind(provider_deadline_at_ms)
        .bind(next_poll_at_ms)
        .bind(observation_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
    } else {
        sqlx::query(
            r#"
            INSERT INTO provider_remote_tasks
              (submission_id, executor_execution_id, provider_id, provider_account_id,
               remote_operation_id, provider_request_id, submit_owner, submit_lease_epoch,
               attach_recovery_owner, attach_recovery_lease_epoch,
               state, effect_certainty, next_poll_at_ms,
               state_observation_id, created_at_ms, updated_at_ms)
            VALUES ($1, $2, 'provider-test', $3, $4, $5, $6, $7, NULL, NULL,
                    'provider_waiting', 'not_applicable', $8, $9, $10, $10)
            "#,
        )
        .bind(lease.submission_id)
        .bind(lease.executor_execution_id)
        .bind(ACCOUNT_ID)
        .bind(&operation)
        .bind(&request_id)
        .bind(&lease.executor_owner)
        .bind(lease.executor_lease_epoch)
        .bind(next_poll_at_ms)
        .bind(observation_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
    }
    sqlx::query(
        r#"
        INSERT INTO provider_task_observations
          (observation_id, submission_id, executor_execution_id, event_identity,
           source, observed_state, effect_certainty, next_poll_at_ms,
           payload_hash, observed_at_ms)
        VALUES ($1, $2, $3, $4, 'submit_attach', 'provider_waiting',
                'not_applicable', $5, $6, $7)
        "#,
    )
    .bind(observation_id)
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(attach_event)
    .bind(next_poll_at_ms)
    .bind(payload_hash)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_submit_recoveries
        SET state = 'closed', next_recovery_at_ms = NULL,
            recovery_owner = NULL, recovery_lease_expires_at_ms = NULL,
            recovery_claimed_at_ms = NULL, updated_at_ms = $3, closed_at_ms = $3
        WHERE submission_id = $1 AND executor_execution_id = $2 AND state = 'active'
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE executor_executions
        SET state = 'provider_waiting', executor_owner = NULL,
            lease_expires_at_ms = NULL, updated_at_ms = $3
        WHERE executor_execution_id = $1 AND submission_id = $2 AND state = 'running'
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_submissions
        SET state = 'provider_waiting', updated_at_ms = $3
        WHERE submission_id = $1 AND executor_execution_id = $2 AND state = 'running'
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)
}

fn legacy_observation_hash(
    source: &str,
    state: &str,
    effect_certainty: &str,
    next_poll_at_ms: Option<i64>,
    poll_owner: Option<&str>,
    poll_lease_epoch: Option<i64>,
) -> String {
    let mut hash = Sha256::new();
    for value in [
        source,
        state,
        "",
        "",
        "",
        "",
        "",
        effect_certainty,
        poll_owner.unwrap_or(""),
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.update(0_u64.to_be_bytes());
    hash.update(next_poll_at_ms.unwrap_or(-1).to_be_bytes());
    hash.update(poll_lease_epoch.unwrap_or(-1).to_be_bytes());
    hex::encode(hash.finalize())
}

async fn seed_v24_remote_task(
    pool: &PgPool,
    worker: &str,
    identity: &str,
    provider_timeout_ms: i64,
    leave_claimed: bool,
) -> TestResult<(ExecutorSubmissionLease, i64)> {
    sqlx::raw_sql(
        r#"
        ALTER TABLE provider_remote_tasks
          ADD COLUMN provider_deadline_at_ms BIGINT,
          ADD COLUMN deadline_quarantine_id UUID
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let store = PostgresProviderTaskStore::new(pool.clone());
    let executor =
        seed_attached_remote_task(pool, &store, worker, identity, provider_timeout_ms, 0).await?;
    let deadline: i64 = sqlx::query_scalar(
        "SELECT provider_deadline_at_ms FROM provider_submit_recoveries WHERE submission_id = $1",
    )
    .bind(executor.submission_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    if leave_claimed {
        let now = database_now(pool).await?;
        let changed = sqlx::query(
            r#"
            UPDATE provider_remote_tasks
            SET poll_owner = $2, poll_lease_epoch = poll_lease_epoch + 1,
                poll_lease_expires_at_ms = $3 + 5_000,
                poll_claimed_at_ms = $3, updated_at_ms = $3
            WHERE submission_id = $1 AND state = 'provider_waiting'
              AND poll_owner IS NULL AND next_poll_at_ms <= $3
            "#,
        )
        .bind(executor.submission_id)
        .bind(format!("{identity}-poller"))
        .bind(now)
        .execute(pool)
        .await
        .map_err(debug_error)?
        .rows_affected();
        require(changed == 1, "v24 remote task was not claimable")?;
    }
    sqlx::raw_sql(
        r#"
        ALTER TABLE provider_remote_tasks
          DROP COLUMN deadline_quarantine_id,
          DROP COLUMN provider_deadline_at_ms
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok((executor, deadline))
}

async fn assert_remote_task_deadline_migration_rolled_back(pool: &PgPool) -> TestResult {
    let residue: (i64, bool, bool) = sqlx::query_as(
        r#"
        SELECT
          (SELECT COUNT(*) FROM information_schema.columns
           WHERE table_schema = current_schema()
             AND table_name = 'provider_remote_tasks'
             AND column_name IN (
                'provider_deadline_at_ms', 'deadline_quarantine_id'
             )),
          to_regclass('provider_remote_task_quarantines') IS NOT NULL,
          EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgrelid = 'provider_remote_tasks'::regclass
              AND tgname = 'provider_remote_task_update_guard'
              AND NOT tgisinternal
          )
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        residue == (0, false, true),
        format!("failed 0025 migration left schema residue: {residue:?}"),
    )
}

async fn assert_operation_binding_migration_rolled_back(pool: &PgPool) -> TestResult {
    let residue: (i64, bool, bool) = sqlx::query_as(
        r#"
        SELECT
          (SELECT COUNT(*) FROM information_schema.columns
           WHERE table_schema = current_schema()
             AND (
               (table_name = 'provider_execution_profiles'
                AND column_name = 'operation_id')
               OR (table_name = 'provider_submissions'
                   AND column_name = 'operation_binding_version')
               OR (table_name = 'provider_remote_submit_intents'
                   AND column_name = 'execution_binding_sha256')
             )),
          EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgrelid = 'provider_execution_profiles'::regclass
              AND tgname = 'provider_execution_profiles_identity'
              AND NOT tgisinternal
          ),
          EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgrelid = 'provider_submissions'::regclass
              AND tgname = 'provider_submission_state_transition'
              AND NOT tgisinternal
          )
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        residue == (0, true, true),
        format!("failed 0026 migration left schema residue: {residue:?}"),
    )
}

fn artifact_authority(
    lease: &ExecutorSubmissionLease,
    identity: &str,
) -> TestResult<ProviderArtifactAuthority> {
    let authority_id = lease.executor_execution_id.simple().to_string();
    ProviderArtifactAuthority::new(
        "filesystem-v1".to_string(),
        format!("filesystem-v1:{identity}"),
        format!("executor-objects/{}/{}", &authority_id[..2], authority_id),
        "a".repeat(64),
        128,
        "image/png".to_string(),
    )
    .ok_or_else(|| "valid provider artifact authority was rejected".to_string())
}

#[derive(Clone)]
struct FailFirstArtifactPublishStore {
    inner: PostgresProviderTaskStore,
    fail_publish: Arc<AtomicBool>,
}

impl FailFirstArtifactPublishStore {
    fn new(inner: PostgresProviderTaskStore) -> Self {
        Self {
            inner,
            fail_publish: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl ProviderPollStore for FailFirstArtifactPublishStore {
    async fn claim_poll(
        &self,
        scope: &ProviderTaskClaimScope,
        owner: &str,
        lease_ms: i64,
    ) -> Result<Option<ProviderTaskLease>, ProviderTaskStoreError> {
        self.inner.claim_due(scope, owner, lease_ms).await
    }

    async fn heartbeat_poll(
        &self,
        lease: &ProviderTaskLease,
        lease_ms: i64,
    ) -> Result<ProviderTaskLease, ProviderTaskStoreError> {
        self.inner.heartbeat(lease, lease_ms).await
    }

    async fn publish_poll_artifact(
        &self,
        lease: &ProviderTaskLease,
        authority: &ProviderArtifactAuthority,
    ) -> Result<ProviderArtifactPublication, ProviderTaskStoreError> {
        if self.fail_publish.swap(false, Ordering::SeqCst) {
            return Err(ProviderTaskStoreError::Unavailable);
        }
        self.inner
            .publish_artifact_authority(lease, authority)
            .await
    }

    async fn record_poll_observation(
        &self,
        lease: &ProviderTaskLease,
        observation: &ProviderTaskObservation,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
        self.inner.record_observation(lease, observation).await
    }
}

#[derive(Clone, Default)]
struct ManifestOnlyPollStagerFactory {
    begins: Arc<AtomicUsize>,
}

impl ProviderArtifactStagerFactory for ManifestOnlyPollStagerFactory {
    type Stager = ManifestOnlyPollStager;

    async fn begin(
        &self,
        context: &ProviderArtifactStageContext,
    ) -> Result<Self::Stager, ArtifactSinkError> {
        self.begins.fetch_add(1, Ordering::SeqCst);
        Ok(ManifestOnlyPollStager {
            executor_execution_id: context.executor_execution_id(),
            bytes: Vec::new(),
        })
    }
}

struct ManifestOnlyPollStager {
    executor_execution_id: Uuid,
    bytes: Vec<u8>,
}

impl ProviderArtifactStager for ManifestOnlyPollStager {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), ArtifactSinkError> {
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }

    async fn finalize(
        &mut self,
        metadata: ArtifactMetadata<'_>,
    ) -> Result<StagedProviderArtifact, ArtifactSinkError> {
        if self.bytes.is_empty() {
            return Err(poll_sink_error("provider_poll_test_artifact_empty"));
        }
        let digest: [u8; 32] = Sha256::digest(&self.bytes).into();
        let manifest = DurableArtifactManifest::new(
            DurableArtifactRef::new(
                "provider-test",
                self.executor_execution_id.simple().to_string(),
            )
            .map_err(|_| poll_sink_error("provider_poll_test_ref_invalid"))?,
            metadata.media_type,
            self.bytes.len() as u64,
            digest,
        )
        .map_err(|_| poll_sink_error("provider_poll_test_manifest_invalid"))?;
        let authority = poll_artifact_authority(
            self.executor_execution_id,
            digest,
            self.bytes.len() as u64,
            metadata.media_type,
        )
        .map_err(|_| poll_sink_error("provider_poll_test_authority_invalid"))?;
        StagedProviderArtifact::new(manifest, authority)
            .map_err(|_| poll_sink_error("provider_poll_test_contract_invalid"))
    }
}

fn poll_artifact_authority(
    executor_execution_id: Uuid,
    digest: [u8; 32],
    byte_size: u64,
    media_type: &str,
) -> TestResult<ProviderArtifactAuthority> {
    let authority_id = executor_execution_id.simple().to_string();
    ProviderArtifactAuthority::new(
        "filesystem-v1".to_owned(),
        "filesystem-v1:provider-poll-integration".to_owned(),
        format!("executor-objects/{}/{}", &authority_id[..2], authority_id),
        hex::encode(digest),
        byte_size,
        media_type.to_owned(),
    )
    .ok_or_else(|| "valid poll artifact authority was rejected".to_owned())
}

fn poll_sink_error(code: &'static str) -> ArtifactSinkError {
    ArtifactSinkError::new(ArtifactSinkErrorKind::InvalidArtifact, code)
}

fn completed_poll_provider(
    bytes: &[u8],
    provider_request_id: &str,
) -> TestResult<ScriptedFakeProvider> {
    let provider = ScriptedFakeProvider::default();
    provider.push_poll(PollStep::Complete(OutputPlan {
        chunks: bytes.chunks(7).map(<[u8]>::to_vec).collect(),
        media_type: "image/png".to_owned(),
        provider_request_id: Some(
            ProviderRequestId::new(provider_request_id).map_err(debug_error)?,
        ),
    }));
    Ok(provider)
}

fn poll_daemon_config(max_in_flight: usize) -> ProviderPollDaemonConfig {
    ProviderPollDaemonConfig {
        max_in_flight,
        idle_delay: Duration::from_millis(10),
        error_base_delay: Duration::from_millis(10),
        error_max_delay: Duration::from_millis(100),
        shutdown_drain_timeout: Duration::from_secs(1),
    }
}

fn submit_service_config(provider_timeout_ms: i64) -> ProviderSubmitServiceConfig {
    ProviderSubmitServiceConfig {
        executor_scope: ExecutorClaimScope {
            execution_profile_id: PROFILE_ID,
            provider_id: "provider-test".to_owned(),
            command_schema: "provider-command-v1".to_owned(),
            adapter_revision: "provider-test-adapter-v1".to_owned(),
        },
        provider_scope: claim_scope(),
        provider_timeout_ms,
        executor_lease_ms: 200,
        recovery_lease_ms: 200,
        heartbeat_interval: Duration::from_millis(25),
        recovery_retry_after_ms: 50,
    }
}

fn submit_iteration_command(identity: &str) -> ProviderSubmitIterationCommand {
    ProviderSubmitIterationCommand::new(
        format!("{identity}-owner"),
        format!("{identity}-claim"),
        format!("{identity}-defer"),
    )
    .expect("submit iteration command is valid")
}

async fn wait_for_poll_observations(pool: &PgPool, expected: i64) -> TestResult {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let observations: i64 = sqlx::query_scalar(
                r#"
                SELECT COUNT(*)
                FROM provider_task_observations
                WHERE source = 'poll' AND observed_state = 'provider_waiting'
                "#,
            )
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
            if observations == expected {
                return Ok::<(), String>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| "poll daemon did not resolve all due tasks in time".to_owned())?
}

fn png_bytes(pixel: [u8; 4]) -> Vec<u8> {
    let image = RgbaImage::from_pixel(1, 1, Rgba(pixel));
    let mut cursor = std::io::Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(image)
        .write_to(&mut cursor, ImageFormat::Png)
        .unwrap();
    cursor.into_inner()
}

async fn seed_deadline_quarantine(
    pool: &PgPool,
    store: &PostgresProviderTaskStore,
    worker: &str,
) -> TestResult<ExecutorSubmissionLease> {
    let lease = seed_running_submission_with_lease(pool, worker, 5_000).await?;
    let mut reservation = reservation_request(&lease);
    reservation.provider_timeout_ms = 40;
    store
        .reserve_submit(&reservation)
        .await
        .map_err(debug_error)?;
    store
        .start_submit(&reservation)
        .await
        .map_err(debug_error)?;
    store
        .record_submit_failure(&submit_failure!(
            &store,
            &lease,
            ProviderSubmitFailureKind::OutcomeUnknown,
            &format!("{worker}-ambiguous"),
            "provider_submit_ambiguous",
        ))
        .await
        .map_err(debug_error)?;
    tokio::time::sleep(Duration::from_millis(60)).await;
    let resolved = store
        .resolve_due_submit_deadline(&claim_scope())
        .await
        .map_err(debug_error)?
        .ok_or_else(|| format!("{worker} deadline was not resolvable"))?;
    require(
        resolved.submission_id == lease.submission_id
            && resolved.state == ProviderSubmitIntentState::DeadlineQuarantined,
        format!("{worker} resolved the wrong deadline"),
    )?;
    Ok(lease)
}

async fn force_deadline_quarantine_v21(
    pool: &PgPool,
    lease: &ExecutorSubmissionLease,
) -> TestResult {
    let now = database_now(pool).await?;
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_remote_submit_intents
        SET state = 'deadline_quarantined', updated_at_ms = $3
        WHERE submission_id = $1 AND executor_execution_id = $2
          AND state IN ('sending', 'outcome_unknown', 'operation_known')
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_submit_recoveries
        SET state = 'closed', next_recovery_at_ms = NULL,
            recovery_owner = NULL, recovery_lease_expires_at_ms = NULL,
            recovery_claimed_at_ms = NULL,
            updated_at_ms = $3, closed_at_ms = $3
        WHERE submission_id = $1 AND executor_execution_id = $2
          AND state = 'active' AND provider_deadline_at_ms <= $3
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO executor_resolution_decisions
          (decision_id, executor_execution_id, submission_id, source,
           observation_id, provider_task_observation_id, provider_submit_intent_id,
           resolved_state, result_manifest_id, error_code, decided_at_ms)
        VALUES ($1, $1, $2, 'remote_submit_deadline', NULL, NULL, $2,
                'uncertain', NULL, 'provider_submit_deadline', $3)
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE executor_executions
        SET state = 'uncertain', executor_owner = NULL,
            lease_expires_at_ms = NULL, resolution_decision_id = $1,
            finished_at_ms = $3, updated_at_ms = $3,
            error_code = 'provider_submit_deadline'
        WHERE executor_execution_id = $1 AND submission_id = $2
          AND state = 'running'
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_submissions
        SET state = 'uncertain', resolution_decision_id = $1,
            finished_at_ms = $3, updated_at_ms = $3,
            error_code = 'provider_submit_deadline'
        WHERE executor_execution_id = $1 AND submission_id = $2
          AND state = 'running'
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)
}

async fn resolve_legacy_due_submit_deadline(
    pool: &PgPool,
    lease: &ExecutorSubmissionLease,
) -> TestResult<String> {
    let now = database_now(pool).await?;
    let mut tx = pool.begin().await.map_err(debug_error)?;
    let changed = sqlx::query(
        r#"
        UPDATE provider_remote_submit_intents
        SET state = 'deadline_quarantined', updated_at_ms = $3
        WHERE submission_id = $1 AND executor_execution_id = $2
          AND state IN ('sending', 'outcome_unknown', 'operation_known')
          AND EXISTS (
            SELECT 1 FROM provider_submit_recoveries recovery
            WHERE recovery.submission_id = $1
              AND recovery.executor_execution_id = $2
              AND recovery.state = 'active'
              AND recovery.provider_deadline_at_ms <= $3
          )
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?
    .rows_affected();
    require(changed == 1, "migrated due recovery was not resolvable")?;
    sqlx::query(
        r#"
        UPDATE provider_submit_recoveries
        SET state = 'closed', next_recovery_at_ms = NULL,
            recovery_owner = NULL, recovery_lease_expires_at_ms = NULL,
            recovery_claimed_at_ms = NULL, updated_at_ms = $3, closed_at_ms = $3
        WHERE submission_id = $1 AND executor_execution_id = $2 AND state = 'active'
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.executor_execution_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO executor_resolution_decisions
          (decision_id, executor_execution_id, submission_id, source,
           observation_id, provider_task_observation_id, provider_submit_intent_id,
           resolved_state, result_manifest_id, error_code, decided_at_ms)
        VALUES ($1, $1, $2, 'remote_submit_deadline', NULL, NULL, $2,
                'uncertain', NULL, 'provider_submit_deadline', $3)
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE executor_executions
        SET state = 'uncertain', executor_owner = NULL,
            lease_expires_at_ms = NULL, resolution_decision_id = $1,
            finished_at_ms = $3, updated_at_ms = $3,
            error_code = 'provider_submit_deadline'
        WHERE executor_execution_id = $1 AND submission_id = $2 AND state = 'running'
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_submissions
        SET state = 'uncertain', resolution_decision_id = $1,
            finished_at_ms = $3, updated_at_ms = $3,
            error_code = 'provider_submit_deadline'
        WHERE executor_execution_id = $1 AND submission_id = $2 AND state = 'running'
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_capacity_reconciliations
          (reconciliation_id, submission_id, executor_execution_id,
           provider_id, provider_account_id, provider_deadline_at_ms,
           state, available_at_ms, reconciliation_owner,
           reconciliation_lease_epoch, evidence_revision,
           created_at_ms, updated_at_ms)
        SELECT $1, intent.submission_id, intent.executor_execution_id,
               intent.provider_id, intent.provider_account_id,
               recovery.provider_deadline_at_ms, 'active', $3, NULL, 0,
               CASE WHEN intent.receipt_event_identity IS NULL THEN 0 ELSE 1 END,
               $3, $3
        FROM provider_remote_submit_intents intent
        JOIN provider_submit_recoveries recovery
          ON recovery.submission_id = intent.submission_id
         AND recovery.executor_execution_id = intent.executor_execution_id
        WHERE intent.executor_execution_id = $1 AND intent.submission_id = $2
          AND intent.state = 'deadline_quarantined' AND recovery.state = 'closed'
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)?;
    sqlx::query_scalar("SELECT state FROM provider_remote_submit_intents WHERE submission_id = $1")
        .bind(lease.submission_id)
        .fetch_one(pool)
        .await
        .map_err(debug_error)
}

async fn seed_running_submission(
    pool: &PgPool,
    worker: &str,
) -> TestResult<ExecutorSubmissionLease> {
    seed_running_submission_with_lease(pool, worker, 60_000).await
}

async fn seed_running_submission_with_lease(
    pool: &PgPool,
    worker: &str,
    lease_ms: i64,
) -> TestResult<ExecutorSubmissionLease> {
    seed_running_submission_for_profile(
        pool,
        worker,
        lease_ms,
        PROFILE_ID,
        "provider-test-adapter-v1",
    )
    .await
}

async fn seed_running_submission_for_profile(
    pool: &PgPool,
    worker: &str,
    lease_ms: i64,
    execution_profile_id: Uuid,
    adapter_revision: &str,
) -> TestResult<ExecutorSubmissionLease> {
    seed_running_submission_for_runtime_profile(
        pool,
        worker,
        lease_ms,
        execution_profile_id,
        "provider-test",
        "model-test",
        "provider-command-v1",
        adapter_revision,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn seed_running_submission_for_runtime_profile(
    pool: &PgPool,
    worker: &str,
    lease_ms: i64,
    execution_profile_id: Uuid,
    provider_id: &str,
    model: &str,
    command_schema: &str,
    adapter_revision: &str,
) -> TestResult<ExecutorSubmissionLease> {
    if !operation_binding_exists(pool).await? || !media_economics_exists(pool).await? {
        require(
            execution_profile_id == PROFILE_ID
                && provider_id == "provider-test"
                && model == "model-test"
                && command_schema == "provider-command-v1"
                && adapter_revision == "provider-test-adapter-v1",
            "legacy fixture only supports its frozen execution profile",
        )?;
        return seed_legacy_running_submission_with_lease(pool, worker, lease_ms).await;
    }
    let work =
        seed_work_lease_for_runtime_profile(pool, worker, provider_id, model, command_schema)
            .await?;
    let store = PostgresExecutorSubmissionStore::new(pool.clone());
    let prepared = store
        .prepare_and_handoff(&work, execution_profile_id)
        .await
        .map_err(debug_error)?;
    require(prepared.len() == 1, "expected one provider submission")?;
    let lease = store
        .claim_prepared(
            &ExecutorClaimScope {
                execution_profile_id,
                provider_id: provider_id.to_string(),
                command_schema: command_schema.to_string(),
                adapter_revision: adapter_revision.to_string(),
            },
            &format!("executor-{worker}"),
            lease_ms,
        )
        .await
        .map_err(debug_error)?
        .ok_or_else(|| "prepared submission was not claimable".to_string())?;
    store.start(&lease).await.map_err(debug_error)?;
    Ok(lease)
}

async fn operation_binding_exists(pool: &PgPool) -> TestResult<bool> {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = current_schema()
            AND table_name = 'provider_submissions'
            AND column_name = 'operation_binding_version'
        )
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)
}

async fn media_economics_exists(pool: &PgPool) -> TestResult<bool> {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = current_schema()
            AND table_name = 'jobs'
            AND column_name = 'output_count'
        )
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)
}

async fn release_legacy_capacity_no_effect(
    pool: &PgPool,
    lease: &ExecutorSubmissionLease,
) -> TestResult {
    let now = database_now(pool).await?;
    let owner = "capacity-upgrade-owner";
    let command_id = "capacity-upgrade-claim";
    let event_identity = "capacity-upgrade-no-effect";
    let mut tx = pool.begin().await.map_err(debug_error)?;
    let claimed = sqlx::query(
        r#"
        UPDATE provider_capacity_reconciliations
        SET reconciliation_owner = $2,
            reconciliation_lease_epoch = reconciliation_lease_epoch + 1,
            claimed_evidence_revision = evidence_revision,
            available_at_ms = $3 + 5_000,
            last_command_kind = 'claim', last_command_id = $4,
            last_command_owner = $2,
            last_command_lease_epoch = reconciliation_lease_epoch + 1,
            claim_command_claimed_at_ms = $3,
            claim_command_lease_expires_at_ms = $3 + 5_000,
            updated_at_ms = $3
        WHERE submission_id = $1 AND state = 'active'
          AND reconciliation_owner IS NULL AND available_at_ms <= $3
        "#,
    )
    .bind(lease.submission_id)
    .bind(owner)
    .bind(now)
    .bind(command_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?
    .rows_affected();
    require(claimed == 1, "backfilled reconciliation was not claimable")?;
    sqlx::query(
        r#"
        UPDATE provider_capacity_reconciliations
        SET state = 'released', evidence_kind = 'confirmed_no_effect',
            event_identity = $2, payload_hash = $3,
            updated_at_ms = $4, released_at_ms = $4
        WHERE submission_id = $1 AND state = 'active'
          AND reconciliation_owner = $5 AND reconciliation_lease_epoch = 1
          AND claimed_evidence_revision = evidence_revision
          AND available_at_ms > $4
        "#,
    )
    .bind(lease.submission_id)
    .bind(event_identity)
    .bind("e".repeat(64))
    .bind(now)
    .bind(owner)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE executor_capacity_allocations
        SET state = 'released', released_at_ms = $3,
            release_decision_id = $1, released_state = 'uncertain',
            release_reason = 'provider_capacity_reconciliation',
            release_reconciliation_id = $1
        WHERE executor_execution_id = $1 AND submission_id = $2 AND state = 'held'
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE executor_resource_policies
        SET allocated_count = allocated_count - 1
        WHERE resource_policy_id = $1 AND revision = 1 AND allocated_count > 0
        "#,
    )
    .bind(POLICY_ID)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)
}

async fn seed_legacy_running_submission_with_lease(
    pool: &PgPool,
    worker: &str,
    lease_ms: i64,
) -> TestResult<ExecutorSubmissionLease> {
    seed_legacy_running_submission_and_work(pool, worker, lease_ms)
        .await
        .map(|(_, lease)| lease)
}

async fn seed_legacy_running_submission_and_work(
    pool: &PgPool,
    worker: &str,
    lease_ms: i64,
) -> TestResult<(WorkLease, ExecutorSubmissionLease)> {
    let work = seed_work_lease(pool, worker).await?;
    let command_hash = hex::encode(Sha256::digest(
        serde_json::to_vec(&work.command_json).map_err(debug_error)?,
    ));
    let (output_id, output_index): (Uuid, i32) =
        sqlx::query_as("SELECT output_id, output_index FROM job_outputs WHERE job_id = $1")
            .bind(work.job_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let submission_id = Uuid::new_v4();
    let executor_execution_id = Uuid::new_v4();
    let executor_owner = format!("executor-{worker}");
    let now = database_now(pool).await?;
    let executor_lease_expires_at_ms = now + lease_ms;
    let mut tx = pool.begin().await.map_err(debug_error)?;

    sqlx::query(
        "UPDATE work_items SET execution_profile_id = $2, updated_at_ms = $3 WHERE work_item_id = $1",
    )
    .bind(work.work_item_id)
    .bind(PROFILE_ID)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    if operation_binding_exists(pool).await? {
        sqlx::query(
            r#"
            INSERT INTO provider_submissions
              (submission_id, executor_execution_id, output_id, job_id,
               tenant_id, provider_id, model, work_item_id,
               created_by_execution_id, created_by_lease_epoch, command_schema, command_hash,
               execution_profile_id, credential_pool_id, provider_account_id,
               credential_ref, credential_revision, adapter_revision,
               resource_policy_id, resource_policy_revision,
               operation_id, operation_descriptor_revision,
               operation_descriptor_sha256_v1, completion_mode, idempotency_mode,
               operation_binding_version, state, prepared_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, 'provider-task-test', 'provider-test', 'model-test', $5,
                    $6, $7, 'provider-command-v1', $8, $9, $10, $11,
                    'test-vault.provider-task.1', 1, 'provider-test-adapter-v1',
                    $12, 1, 'images.generations', 'provider-test/images.generations/v1',
                    $13, 'remote_task', 'submission_bound', 2,
                    'prepared', $14, $14)
            "#,
        )
        .bind(submission_id)
        .bind(executor_execution_id)
        .bind(output_id)
        .bind(work.job_id)
        .bind(work.work_item_id)
        .bind(work.execution_id)
        .bind(work.lease_epoch)
        .bind(&command_hash)
        .bind(PROFILE_ID)
        .bind(POOL_ID)
        .bind(ACCOUNT_ID)
        .bind(POLICY_ID)
        .bind("2".repeat(64))
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
    } else {
        sqlx::query(
            r#"
            INSERT INTO provider_submissions
              (submission_id, executor_execution_id, output_id, job_id,
               tenant_id, provider_id, model, work_item_id,
               created_by_execution_id, created_by_lease_epoch, command_schema, command_hash,
               execution_profile_id, credential_pool_id, provider_account_id,
               credential_ref, credential_revision, adapter_revision,
               resource_policy_id, resource_policy_revision,
               state, prepared_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, 'provider-task-test', 'provider-test', 'model-test', $5,
                    $6, $7, 'provider-command-v1', $8, $9, $10, $11,
                    'test-vault.provider-task.1', 1, 'provider-test-adapter-v1',
                    $12, 1, 'prepared', $13, $13)
            "#,
        )
        .bind(submission_id)
        .bind(executor_execution_id)
        .bind(output_id)
        .bind(work.job_id)
        .bind(work.work_item_id)
        .bind(work.execution_id)
        .bind(work.lease_epoch)
        .bind(&command_hash)
        .bind(PROFILE_ID)
        .bind(POOL_ID)
        .bind(ACCOUNT_ID)
        .bind(POLICY_ID)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
    }
    sqlx::query(
        r#"
        INSERT INTO executor_executions
          (executor_execution_id, submission_id, state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'prepared', $3, $3)
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_submission_attachments
          (submission_id, job_id, attempt_execution_id, work_item_id, lease_epoch, attached_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(submission_id)
    .bind(work.job_id)
    .bind(work.execution_id)
    .bind(work.work_item_id)
    .bind(work.lease_epoch)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE job_attempts
        SET state = 'handed_off', handed_off_at_ms = $4, updated_at_ms = $4
        WHERE work_item_id = $1 AND execution_id = $2 AND lease_epoch = $3
          AND state = 'claimed'
        "#,
    )
    .bind(work.work_item_id)
    .bind(work.execution_id)
    .bind(work.lease_epoch)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE work_items
        SET state = 'awaiting_executor', lease_owner = NULL,
            lease_expires_at_ms = NULL, handed_off_at_ms = $2, updated_at_ms = $2
        WHERE work_item_id = $1 AND state = 'leased'
        "#,
    )
    .bind(work.work_item_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)?;

    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE executor_resource_policies
        SET allocated_count = allocated_count + 1
        WHERE resource_policy_id = $1 AND revision = 1
          AND state = 'enabled' AND allocated_count < max_concurrency
        "#,
    )
    .bind(POLICY_ID)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO executor_capacity_allocations
          (allocation_id, executor_execution_id, submission_id, execution_profile_id,
           resource_policy_id, resource_policy_revision, state,
           acquired_at_ms, last_heartbeat_at_ms)
        VALUES ($1, $1, $2, $3, $4, 1, 'held', $5, $5)
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .bind(PROFILE_ID)
    .bind(POLICY_ID)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE executor_executions
        SET state = 'leased', executor_owner = $3, lease_epoch = 1,
            lease_expires_at_ms = $4, leased_at_ms = $5, updated_at_ms = $5
        WHERE executor_execution_id = $1 AND submission_id = $2 AND state = 'prepared'
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .bind(&executor_owner)
    .bind(executor_lease_expires_at_ms)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE executor_executions
        SET state = 'running', launch_owner = $3, launch_lease_epoch = 1,
            started_at_ms = $4, updated_at_ms = $4
        WHERE executor_execution_id = $1 AND submission_id = $2
          AND executor_owner = $3 AND lease_epoch = 1 AND state = 'leased'
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .bind(&executor_owner)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE provider_submissions
        SET state = 'running', started_at_ms = $3, updated_at_ms = $3
        WHERE submission_id = $1 AND executor_execution_id = $2 AND state = 'prepared'
        "#,
    )
    .bind(submission_id)
    .bind(executor_execution_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)?;

    let lease = ExecutorSubmissionLease {
        submission_id,
        executor_execution_id,
        output_id,
        job_id: work.job_id,
        tenant_id: "provider-task-test".to_string(),
        provider_id: "provider-test".to_string(),
        model: "model-test".to_string(),
        work_item_id: work.work_item_id,
        output_index,
        command_schema: "provider-command-v1".to_string(),
        command_hash,
        execution_profile_id: PROFILE_ID,
        adapter_revision: "provider-test-adapter-v1".to_string(),
        executor_owner,
        executor_lease_epoch: 1,
        executor_lease_expires_at_ms,
    };
    Ok((work, lease))
}

async fn seed_work_lease(pool: &PgPool, worker: &str) -> TestResult<WorkLease> {
    seed_work_lease_for_runtime_profile(
        pool,
        worker,
        "provider-test",
        "model-test",
        "provider-command-v1",
    )
    .await
}

async fn seed_work_lease_for_runtime_profile(
    pool: &PgPool,
    worker: &str,
    provider_id: &str,
    model: &str,
    command_schema: &str,
) -> TestResult<WorkLease> {
    seed_work_lease_for_runtime_profile_with_command(
        pool,
        worker,
        provider_id,
        model,
        command_schema,
        json!({"schema_version": 1, "operation": "generation", "n": 1, "prompt": "remote task"}),
    )
    .await
}

async fn seed_work_lease_for_runtime_profile_with_command(
    pool: &PgPool,
    worker: &str,
    provider_id: &str,
    model: &str,
    command_schema: &str,
    command: serde_json::Value,
) -> TestResult<WorkLease> {
    let job_id = Uuid::new_v4();
    let work_item_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let now = database_now(pool).await?;
    let request_id = format!("request-{}", Uuid::new_v4().simple());
    let media_economics_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'jobs' AND column_name = 'output_count')",
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    if media_economics_exists {
        sqlx::query(
            r#"
            INSERT INTO jobs
              (job_id, tenant_id, request_id, operation, provider_id, model, state,
               requested_units, output_count, billable_units, billing_metric, billing_unit,
               economics_contract_version, created_at_ms, updated_at_ms)
            VALUES ($1, 'provider-task-test', $2, 'generation', $3,
                    $4, 'reserved', 1, 1, 1, 'output', 'output', 2, $5, $5)
            "#,
        )
        .bind(job_id)
        .bind(&request_id)
        .bind(provider_id)
        .bind(model)
        .bind(now)
        .execute(pool)
        .await
        .map_err(debug_error)?;
    } else {
        sqlx::query(
            r#"
            INSERT INTO jobs
              (job_id, tenant_id, request_id, operation, provider_id, model, state,
               requested_units, economics_contract_version, created_at_ms, updated_at_ms)
            VALUES ($1, 'provider-task-test', $2, 'generation', $3,
                    $4, 'reserved', 1, 2, $5, $5)
            "#,
        )
        .bind(job_id)
        .bind(&request_id)
        .bind(provider_id)
        .bind(model)
        .bind(now)
        .execute(pool)
        .await
        .map_err(debug_error)?;
    }
    sqlx::query(
        "INSERT INTO job_outputs (output_id, job_id, output_index, state, created_at_ms, updated_at_ms) VALUES ($1, $2, 0, 'pending', $3, $3)",
    )
    .bind(Uuid::new_v4())
    .bind(job_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO admission_sessions
          (session_id, owner_token, tenant_id, project_id, api_profile, operation,
           request_id, request_hash, state, job_id, deadline_at_ms, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'provider-task-test', 'project-test', 'openai-images-v1',
                'generation', $3, $4, 'attached', $5, $6, $7, $7)
        "#,
    )
    .bind(session_id)
    .bind(Uuid::new_v4())
    .bind(&request_id)
    .bind("d".repeat(64))
    .bind(job_id)
    .bind(now + 300_000)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO job_payloads (job_id, admission_session_id, command_schema, command_json, request_hash, created_at_ms) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(job_id)
    .bind(session_id)
    .bind(command_schema)
    .bind(&command)
    .bind("d".repeat(64))
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO work_items
          (work_item_id, job_id, kind, state, available_at_ms, lease_epoch,
           lease_owner, lease_expires_at_ms, execution_id, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'generation', 'leased', $4, 1, $3, $5, $6, $4, $4)
        "#,
    )
    .bind(work_item_id)
    .bind(job_id)
    .bind(worker)
    .bind(now)
    .bind(now + 300_000)
    .bind(execution_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_attempts
          (attempt_id, execution_id, work_item_id, lease_epoch, worker_id,
           state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, 1, $4, 'claimed', $5, $5)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(execution_id)
    .bind(work_item_id)
    .bind(worker)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(WorkLease {
        work_item_id,
        job_id,
        execution_id,
        lease_epoch: 1,
        worker_id: worker.to_string(),
        command_schema: command_schema.to_string(),
        command_json: command,
    })
}

async fn seed_execution_profile(pool: &PgPool) -> TestResult {
    let now = database_now(pool).await?;
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query("INSERT INTO provider_credential_pools (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms) VALUES ($1, 'provider-task-pool', 'provider-test', 'enabled', $2, $2)")
        .bind(POOL_ID).bind(now).execute(&mut *tx).await.map_err(debug_error)?;
    sqlx::query("INSERT INTO provider_accounts (provider_account_id, credential_pool_id, provider_id, account_key, credential_ref, credential_revision, credential_auth_sha256, state, created_at_ms, updated_at_ms) VALUES ($1, $2, 'provider-test', 'provider-task-account', 'test-vault.provider-task.1', 1, $3, 'enabled', $4, $4)")
        .bind(ACCOUNT_ID).bind(POOL_ID).bind("1".repeat(64)).bind(now)
        .execute(&mut *tx).await.map_err(debug_error)?;
    sqlx::query("INSERT INTO executor_resource_policies (resource_policy_id, revision, credential_pool_id, provider_account_id, provider_id, execution_class, max_concurrency, state, created_at_ms) VALUES ($1, 1, $2, $3, 'provider-test', 'remote-task', 100, 'enabled', $4)")
        .bind(POLICY_ID).bind(POOL_ID).bind(ACCOUNT_ID).bind(now)
        .execute(&mut *tx).await.map_err(debug_error)?;
    let operation_binding_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'provider_execution_profiles' AND column_name = 'operation_id')",
    )
    .fetch_one(&mut *tx)
    .await
    .map_err(debug_error)?;
    if operation_binding_exists {
        sqlx::query("INSERT INTO provider_execution_profiles (execution_profile_id, profile_key, provider_id, command_schema, operation_id, operation_descriptor_revision, operation_descriptor_sha256_v1, completion_mode, idempotency_mode, adapter_revision, credential_pool_id, provider_account_id, credential_ref, credential_revision, resource_policy_id, resource_policy_revision, state, created_at_ms, updated_at_ms) VALUES ($1, 'provider-task-profile', 'provider-test', 'provider-command-v1', 'images.generations', 'provider-test/images.generations/v1', $2, 'remote_task', 'submission_bound', 'provider-test-adapter-v1', $3, $4, 'test-vault.provider-task.1', 1, $5, 1, 'enabled', $6, $6)")
            .bind(PROFILE_ID).bind("2".repeat(64)).bind(POOL_ID).bind(ACCOUNT_ID).bind(POLICY_ID).bind(now)
            .execute(&mut *tx).await.map_err(debug_error)?;
    } else {
        sqlx::query("INSERT INTO provider_execution_profiles (execution_profile_id, profile_key, provider_id, command_schema, adapter_revision, credential_pool_id, provider_account_id, credential_ref, credential_revision, resource_policy_id, resource_policy_revision, state, created_at_ms, updated_at_ms) VALUES ($1, 'provider-task-profile', 'provider-test', 'provider-command-v1', 'provider-test-adapter-v1', $2, $3, 'test-vault.provider-task.1', 1, $4, 1, 'enabled', $5, $5)")
            .bind(PROFILE_ID).bind(POOL_ID).bind(ACCOUNT_ID).bind(POLICY_ID).bind(now)
            .execute(&mut *tx).await.map_err(debug_error)?;
    }
    tx.commit().await.map_err(debug_error)
}

async fn seed_dreamina_execution_profile(pool: &PgPool) -> TestResult {
    let now = database_now(pool).await?;
    let operation = DREAMINA_IMAGE_GENERATION_OPERATION_V1;
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_credential_pools
          (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms)
        VALUES ($1, 'dreamina-image-runtime-test', $2, 'enabled', $3, $3)
        "#,
    )
    .bind(DREAMINA_POOL_ID)
    .bind(DREAMINA_PROVIDER_ID)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_accounts
          (provider_account_id, credential_pool_id, provider_id, account_key,
           credential_ref, credential_revision, credential_auth_sha256,
           state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, 'dreamina-image-runtime-test', $4, 1, $5,
                'enabled', $6, $6)
        "#,
    )
    .bind(DREAMINA_ACCOUNT_ID)
    .bind(DREAMINA_POOL_ID)
    .bind(DREAMINA_PROVIDER_ID)
    .bind(DREAMINA_CREDENTIAL_REF)
    .bind("c".repeat(64))
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO executor_resource_policies
          (resource_policy_id, revision, credential_pool_id, provider_account_id,
           provider_id, execution_class, max_concurrency, state, created_at_ms)
        VALUES ($1, 1, $2, $3, $4, 'remote-task', 2, 'enabled', $5)
        "#,
    )
    .bind(DREAMINA_POLICY_ID)
    .bind(DREAMINA_POOL_ID)
    .bind(DREAMINA_ACCOUNT_ID)
    .bind(DREAMINA_PROVIDER_ID)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_execution_profiles
          (execution_profile_id, profile_key, provider_id, command_schema,
           operation_id, operation_descriptor_revision,
           operation_descriptor_sha256_v1, completion_mode, idempotency_mode,
           adapter_revision, credential_pool_id, provider_account_id,
           credential_ref, credential_revision, resource_policy_id,
           resource_policy_revision, state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, 1, $14, 1, 'enabled', $15, $15)
        "#,
    )
    .bind(DREAMINA_PROFILE_ID)
    .bind(DREAMINA_PROFILE_KEY)
    .bind(DREAMINA_PROVIDER_ID)
    .bind(operation.command_schema)
    .bind(operation.id)
    .bind(operation.descriptor_revision)
    .bind(operation.canonical_sha256_v1_hex())
    .bind(operation.completion.as_str())
    .bind(operation.idempotency.as_str())
    .bind(DREAMINA_ADAPTER_REVISION)
    .bind(DREAMINA_POOL_ID)
    .bind(DREAMINA_ACCOUNT_ID)
    .bind(DREAMINA_CREDENTIAL_REF)
    .bind(DREAMINA_POLICY_ID)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)
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
            eprintln!("skipping PostgreSQL provider task test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_provider_tasks_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 8, &schema)
            .await
            .map_err(debug_error)?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(debug_error)?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!("refusing DDL in non-test database {database_name}"));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        if let Err(error) = run_migrations(&pool).await {
            let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                .execute(&pool)
                .await;
            pool.close().await;
            return Err(format!("migration failed: {error:?}"));
        }
        seed_execution_profile(&pool).await?;
        Ok(Some(Self { schema, pool }))
    }

    async fn new_before_submit_lifecycle() -> TestResult<Option<Self>> {
        let Some(url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set in CI".to_string());
            }
            eprintln!("skipping PostgreSQL migration test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_provider_upgrade_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 8, &schema)
            .await
            .map_err(debug_error)?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(debug_error)?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!("refusing DDL in non-test database {database_name}"));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        for migration in [
            include_str!("../migrations/0000_legacy_reconciliation.sql"),
            include_str!("../migrations/0001_usage.sql"),
            include_str!("../migrations/0002_durable_admission.sql"),
            include_str!("../migrations/0003_durable_scheduling.sql"),
            include_str!("../migrations/0004_api_key_hmac.sql"),
            include_str!("../migrations/0005_artifact_replay.sql"),
            include_str!("../migrations/0006_execution_context.sql"),
            include_str!("../migrations/0007_edit_inputs.sql"),
            include_str!("../migrations/0008_provider_submissions.sql"),
            include_str!("../migrations/0009_economic_kernel.sql"),
            include_str!("../migrations/0010_executor_artifact_authority.sql"),
            include_str!("../migrations/0011_executor_observation_resolution.sql"),
            include_str!("../migrations/0012_executor_pending_evidence_index.sql"),
            include_str!("../migrations/0013_executor_execution_profiles.sql"),
            include_str!("../migrations/0014_executor_handoff.sql"),
            include_str!("../migrations/0015_executor_terminal_reductions.sql"),
            include_str!("../migrations/0016_terminal_reduction_completion.sql"),
            include_str!("../migrations/0017_provider_remote_tasks.sql"),
        ] {
            if let Err(error) = sqlx::raw_sql(migration).execute(&pool).await {
                let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                    .execute(&pool)
                    .await;
                pool.close().await;
                return Err(format!("pre-0018 migration failed: {error}"));
            }
        }
        seed_execution_profile(&pool).await?;
        Ok(Some(Self { schema, pool }))
    }

    async fn new_before_capacity_heartbeats() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_submit_lifecycle().await? else {
            return Ok(None);
        };
        for migration in [
            include_str!("../migrations/0018_provider_submit_lifecycle.sql"),
            include_str!("../migrations/0019_provider_submit_recovery_leases.sql"),
        ] {
            if let Err(error) = sqlx::raw_sql(migration).execute(&database.pool).await {
                let cleanup = database.cleanup().await;
                return match cleanup {
                    Ok(()) => Err(format!("pre-0020 migration failed: {error}")),
                    Err(cleanup) => Err(format!(
                        "pre-0020 migration failed: {error}; cleanup failed: {cleanup}"
                    )),
                };
            }
        }
        Ok(Some(database))
    }

    async fn new_before_deadline_quarantine() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_capacity_heartbeats().await? else {
            return Ok(None);
        };
        if let Err(error) = sqlx::raw_sql(include_str!(
            "../migrations/0020_provider_capacity_heartbeats.sql"
        ))
        .execute(&database.pool)
        .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("pre-0021 migration failed: {error}")),
                Err(cleanup) => Err(format!(
                    "pre-0021 migration failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        Ok(Some(database))
    }

    async fn new_before_capacity_reconciliation() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_deadline_quarantine().await? else {
            return Ok(None);
        };
        if let Err(error) = sqlx::raw_sql(include_str!(
            "../migrations/0021_provider_submit_deadline_quarantine.sql"
        ))
        .execute(&database.pool)
        .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("pre-0022 migration failed: {error}")),
                Err(cleanup) => Err(format!(
                    "pre-0022 migration failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        Ok(Some(database))
    }

    async fn new_before_atomic_artifact_resolution() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_capacity_reconciliation().await? else {
            return Ok(None);
        };
        if let Err(error) = sqlx::raw_sql(include_str!(
            "../migrations/0022_provider_capacity_reconciliation.sql"
        ))
        .execute(&database.pool)
        .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("pre-0023 migration failed: {error}")),
                Err(cleanup) => Err(format!(
                    "pre-0023 migration failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        Ok(Some(database))
    }

    async fn new_before_replayable_recovery_commands() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_atomic_artifact_resolution().await? else {
            return Ok(None);
        };
        if let Err(error) = sqlx::raw_sql(include_str!(
            "../migrations/0023_atomic_provider_artifact_resolution.sql"
        ))
        .execute(&database.pool)
        .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("pre-0024 migration failed: {error}")),
                Err(cleanup) => Err(format!(
                    "pre-0024 migration failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        Ok(Some(database))
    }

    async fn new_before_remote_task_deadline() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_replayable_recovery_commands().await? else {
            return Ok(None);
        };
        if let Err(error) = sqlx::raw_sql(include_str!(
            "../migrations/0024_replayable_provider_submit_recovery_commands.sql"
        ))
        .execute(&database.pool)
        .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("pre-0025 migration failed: {error}")),
                Err(cleanup) => Err(format!(
                    "pre-0025 migration failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        Ok(Some(database))
    }

    async fn new_before_operation_binding() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_remote_task_deadline().await? else {
            return Ok(None);
        };
        if let Err(error) = sqlx::raw_sql(include_str!(
            "../migrations/0025_provider_remote_task_deadline_quarantine.sql"
        ))
        .execute(&database.pool)
        .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("pre-0026 migration failed: {error}")),
                Err(cleanup) => Err(format!(
                    "pre-0026 migration failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        Ok(Some(database))
    }

    async fn new_before_atomic_submit_acquisition() -> TestResult<Option<Self>> {
        let Some(database) = Self::new_before_operation_binding().await? else {
            return Ok(None);
        };
        if let Err(error) = sqlx::raw_sql(
            r#"
            ALTER TABLE provider_execution_profiles
              DISABLE TRIGGER provider_execution_profiles_identity;
            DELETE FROM provider_execution_profiles;
            ALTER TABLE provider_execution_profiles
              ENABLE TRIGGER provider_execution_profiles_identity;
            "#,
        )
        .execute(&database.pool)
        .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("pre-0026 profile drain failed: {error}")),
                Err(cleanup) => Err(format!(
                    "pre-0026 profile drain failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        if let Err(error) = sqlx::raw_sql(include_str!(
            "../migrations/0026_immutable_provider_operation_binding.sql"
        ))
        .execute(&database.pool)
        .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("pre-0027 migration failed: {error}")),
                Err(cleanup) => Err(format!(
                    "pre-0027 migration failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        let now = database_now(&database.pool).await?;
        if let Err(error) = sqlx::query("INSERT INTO provider_execution_profiles (execution_profile_id, profile_key, provider_id, command_schema, operation_id, operation_descriptor_revision, operation_descriptor_sha256_v1, completion_mode, idempotency_mode, adapter_revision, credential_pool_id, provider_account_id, credential_ref, credential_revision, resource_policy_id, resource_policy_revision, state, created_at_ms, updated_at_ms) VALUES ($1, 'provider-task-profile', 'provider-test', 'provider-command-v1', 'images.generations', 'provider-test/images.generations/v1', $2, 'remote_task', 'submission_bound', 'provider-test-adapter-v1', $3, $4, 'test-vault.provider-task.1', 1, $5, 1, 'enabled', $6, $6)")
            .bind(PROFILE_ID)
            .bind("2".repeat(64))
            .bind(POOL_ID)
            .bind(ACCOUNT_ID)
            .bind(POLICY_ID)
            .bind(now)
            .execute(&database.pool)
            .await
        {
            let cleanup = database.cleanup().await;
            return match cleanup {
                Ok(()) => Err(format!("schema 26 profile provisioning failed: {error}")),
                Err(cleanup) => Err(format!(
                    "schema 26 profile provisioning failed: {error}; cleanup failed: {cleanup}"
                )),
            };
        }
        Ok(Some(database))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .map_err(debug_error);
        self.pool.close().await;
        result.map(|_| ())
    }
}

async fn apply_migration_range(pool: &PgPool, first_version: i64, last_version: i64) -> TestResult {
    let migration_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let mut migrations = fs::read_dir(&migration_dir)
        .map_err(|error| format!("failed to read {}: {error}", migration_dir.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to enumerate migrations: {error}"))?;
    migrations.sort();

    for path in migrations {
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(version) = file_name
            .split_once('_')
            .and_then(|(version, _)| version.parse::<i64>().ok())
        else {
            continue;
        };
        if !(first_version..=last_version).contains(&version) {
            continue;
        }
        let sql = fs::read_to_string(&path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        let mut transaction = pool
            .begin()
            .await
            .map_err(|error| format!("failed to begin migration {version}: {error}"))?;
        sqlx::raw_sql(AssertSqlSafe(sql))
            .execute(&mut *transaction)
            .await
            .map_err(|error| format!("migration {version} failed: {error}"))?;
        transaction
            .commit()
            .await
            .map_err(|error| format!("failed to commit migration {version}: {error}"))?;
    }
    Ok(())
}

fn runtime_registration(role: ProviderRuntimeRole, owner: &str) -> ProviderRuntimeRegistration {
    ProviderRuntimeRegistration {
        runtime_id: Uuid::new_v4(),
        execution_profile_id: PROFILE_ID,
        role,
        runtime_owner: owner.to_string(),
    }
}

async fn require_profile_status(
    store: &PostgresProviderTaskStore,
    status: ProviderProfileReadinessStatus,
    counts: (i64, i64, i64, i64),
) -> TestResult {
    let profiles = store.list_profile_readiness().await.map_err(debug_error)?;
    let profile = profiles
        .iter()
        .find(|profile| profile.execution_profile_id == PROFILE_ID)
        .ok_or_else(|| "provider runtime profile readiness was missing".to_string())?;
    require(
        profile.status == status
            && (
                profile.active_submitters,
                profile.active_pollers,
                profile.draining_submitters,
                profile.draining_pollers,
            ) == counts,
        format!("unexpected provider profile readiness: {profile:?}"),
    )
}

async fn require_profile_summary(
    store: &PostgresProviderTaskStore,
    expected: ProviderProfileReadinessSummary,
) -> TestResult {
    let actual = store
        .summarize_profile_readiness()
        .await
        .map_err(debug_error)?;
    require(
        actual == expected,
        format!("unexpected provider profile readiness summary: {actual:?}"),
    )
}

async fn runtime_lease_count(
    pool: &PgPool,
    execution_profile_id: Uuid,
    role: &str,
    owner: &str,
) -> TestResult<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM provider_runtime_leases
        WHERE execution_profile_id = $1 AND runtime_role = $2
          AND runtime_owner = $3
        "#,
    )
    .bind(execution_profile_id)
    .bind(role)
    .bind(owner)
    .fetch_one(pool)
    .await
    .map_err(debug_error)
}

async fn database_now(pool: &PgPool) -> TestResult<i64> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(pool)
        .await
        .map_err(debug_error)
}

async fn sleep_until_database_time(pool: &PgPool, target_ms: i64) -> TestResult {
    let now = database_now(pool).await?;
    if target_ms > now {
        tokio::time::sleep(Duration::from_millis(
            u64::try_from(target_ms - now).map_err(debug_error)?,
        ))
        .await;
    }
    Ok(())
}

async fn capacity_heartbeat(pool: &PgPool, executor_execution_id: Uuid) -> TestResult<i64> {
    sqlx::query_scalar(
        r#"
        SELECT last_heartbeat_at_ms
        FROM executor_capacity_allocations
        WHERE executor_execution_id = $1 AND state = 'held'
        "#,
    )
    .bind(executor_execution_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)
}

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

fn read_test_log(path: &std::path::Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|_| "<provider daemon log unavailable>".to_owned())
}

fn signal_process_group(pid: u32, signal: libc::c_int) -> TestResult {
    // SAFETY: the test supplies a positive child PID and a valid Unix signal.
    let result = unsafe { libc::kill(-(pid as libc::pid_t), signal) };
    if result == 0 {
        Ok(())
    } else {
        Err(format!(
            "failed to signal provider daemon process group: {}",
            std::io::Error::last_os_error()
        ))
    }
}

fn signal_process(pid: u32, signal: libc::c_int) -> TestResult {
    // SAFETY: the test supplies a positive child PID and a valid Unix signal.
    let result = unsafe { libc::kill(pid as libc::pid_t, signal) };
    if result == 0 {
        Ok(())
    } else {
        Err(format!(
            "failed to signal provider daemon process: {}",
            std::io::Error::last_os_error()
        ))
    }
}

fn combine(result: TestResult, cleanup: TestResult) -> TestResult {
    match (result, cleanup) {
        (Err(error), Err(cleanup)) => Err(format!("{error}; cleanup failed: {cleanup}")),
        (Err(error), _) => Err(error),
        (_, Err(error)) => Err(error),
        _ => Ok(()),
    }
}
