use std::{
    fs,
    os::unix::fs::PermissionsExt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use image_cli_runtime::WorkingDirectory;
use image_provider_dreamina_cli::{
    ADAPTER_REVISION as DREAMINA_ADAPTER_REVISION, DREAMINA_SUBMIT_COMMAND_SCHEMA,
    DreaminaCliQueryPolicyV1, PROVIDER_ID as DREAMINA_PROVIDER_ID,
};
use image_provider_sdk::{
    ArtifactMetadata, ArtifactSink, ArtifactSinkError, ArtifactSinkErrorKind, Completed,
    DurableArtifactManifest, DurableArtifactRef, PollObservation,
};
use image_provider_test_support::{OutputPlan, PollStep, ScriptedFakeProvider};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::sync::Semaphore;
use uuid::Uuid;

use super::*;
use crate::{
    artifacts::executor_object_key,
    executor::ExecutorResultManifest,
    provider_tasks::{
        ProviderArtifactAuthority, ProviderArtifactPublication, ProviderExecutionContext,
        ProviderRemoteTask, ProviderTaskClaimScope, ProviderTaskLease, ProviderTaskObservation,
        ProviderTaskObservationOutcome, ProviderTaskState, ProviderTaskStoreError,
    },
    providers::dreamina_cli::{DreaminaCliPollDriverV1, DreaminaCliRuntimeBindingV1},
};

#[derive(Clone, Default)]
struct TestStagerFactory {
    begins: Arc<AtomicUsize>,
}

impl ProviderArtifactStagerFactory for TestStagerFactory {
    type Stager = TestStager;

    async fn begin(
        &self,
        context: &ProviderArtifactStageContext,
    ) -> Result<Self::Stager, ArtifactSinkError> {
        self.begins.fetch_add(1, Ordering::SeqCst);
        Ok(TestStager {
            executor_execution_id: context.executor_execution_id(),
            bytes: Vec::new(),
        })
    }
}

struct TestStager {
    executor_execution_id: Uuid,
    bytes: Vec<u8>,
}

impl ProviderArtifactStager for TestStager {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), ArtifactSinkError> {
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }

    async fn finalize(
        &mut self,
        metadata: ArtifactMetadata<'_>,
    ) -> Result<StagedProviderArtifact, ArtifactSinkError> {
        if self.bytes.is_empty() {
            return Err(sink_error("empty_test_artifact"));
        }
        let digest: [u8; 32] = Sha256::digest(&self.bytes).into();
        let manifest = DurableArtifactManifest::new(
            DurableArtifactRef::new(
                "provider-test",
                self.executor_execution_id.simple().to_string(),
            )
            .map_err(|_| sink_error("test_artifact_ref_invalid"))?,
            metadata.media_type,
            self.bytes.len() as u64,
            digest,
        )
        .map_err(|_| sink_error("test_manifest_invalid"))?;
        let authority = ProviderArtifactAuthority::new(
            "filesystem-v1".to_owned(),
            "filesystem-v1:provider-poll-test".to_owned(),
            executor_object_key(self.executor_execution_id),
            hex::encode(digest),
            self.bytes.len() as u64,
            metadata.media_type.to_owned(),
        )
        .ok_or_else(|| sink_error("test_authority_invalid"))?;
        StagedProviderArtifact::new(manifest, authority)
            .map_err(|_| sink_error("test_staged_artifact_invalid"))
    }
}

#[derive(Default)]
struct FakeStoreState {
    lease: Option<ProviderTaskLease>,
    heartbeat_count: usize,
    fail_heartbeat: bool,
    actions: Vec<&'static str>,
    observations: Vec<ProviderTaskObservation>,
}

#[derive(Clone, Default)]
struct FakeStore {
    state: Arc<Mutex<FakeStoreState>>,
}

impl FakeStore {
    fn with_lease(lease: ProviderTaskLease) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeStoreState {
                lease: Some(lease),
                ..FakeStoreState::default()
            })),
        }
    }
}

impl ProviderPollStore for FakeStore {
    async fn claim_poll(
        &self,
        _scope: &ProviderTaskClaimScope,
        _owner: &str,
        _lease_ms: i64,
    ) -> Result<Option<ProviderTaskLease>, ProviderTaskStoreError> {
        Ok(self.state.lock().unwrap().lease.take())
    }

    async fn heartbeat_poll(
        &self,
        lease: &ProviderTaskLease,
        _lease_ms: i64,
    ) -> Result<ProviderTaskLease, ProviderTaskStoreError> {
        let mut state = self.state.lock().unwrap();
        state.heartbeat_count += 1;
        if state.fail_heartbeat {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        let mut renewed = lease.clone();
        renewed.poll_lease_expires_at_ms += 100;
        Ok(renewed)
    }

    async fn publish_poll_artifact(
        &self,
        lease: &ProviderTaskLease,
        authority: &ProviderArtifactAuthority,
    ) -> Result<ProviderArtifactPublication, ProviderTaskStoreError> {
        self.state.lock().unwrap().actions.push("publish");
        Ok(publication(lease, authority))
    }

    async fn record_poll_observation(
        &self,
        lease: &ProviderTaskLease,
        observation: &ProviderTaskObservation,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
        let mut state = self.state.lock().unwrap();
        state.actions.push("record");
        state.observations.push(observation.clone());
        let mut task = lease.task.clone();
        match &observation.outcome {
            ProviderTaskObservationOutcome::Waiting { .. } => {}
            ProviderTaskObservationOutcome::ArtifactReady { artifact_ref, .. } => {
                task.state = ProviderTaskState::ArtifactReady;
                task.artifact_ref = Some(artifact_ref.clone());
                task.next_poll_at_ms = None;
            }
            ProviderTaskObservationOutcome::Failed { error_code } => {
                task.state = ProviderTaskState::Failed;
                task.error_code = Some(error_code.clone());
                task.next_poll_at_ms = None;
            }
            ProviderTaskObservationOutcome::Canceled { error_code } => {
                task.state = ProviderTaskState::Canceled;
                task.error_code = Some(error_code.clone());
                task.next_poll_at_ms = None;
            }
            ProviderTaskObservationOutcome::Uncertain { error_code } => {
                task.state = ProviderTaskState::Uncertain;
                task.error_code = Some(error_code.clone());
                task.next_poll_at_ms = None;
            }
        }
        Ok(task)
    }
}

struct SlowPendingDriver;

impl ProviderPollDriver for SlowPendingDriver {
    fn provider_id(&self) -> &'static str {
        "provider-test"
    }

    async fn poll<S: ArtifactSink>(
        &self,
        _call: &ProviderPollDriverCall,
        _sink: &mut S,
    ) -> Result<PollObservation, image_provider_sdk::ProviderFailure> {
        tokio::time::sleep(Duration::from_millis(35)).await;
        Ok(PollObservation::Pending {
            next_poll_after_ms: Some(100),
        })
    }
}

struct NeverDriver {
    dropped: Arc<AtomicBool>,
}

impl ProviderPollDriver for NeverDriver {
    fn provider_id(&self) -> &'static str {
        "provider-test"
    }

    async fn poll<S: ArtifactSink>(
        &self,
        _call: &ProviderPollDriverCall,
        _sink: &mut S,
    ) -> Result<PollObservation, image_provider_sdk::ProviderFailure> {
        let _guard = DropFlag(Arc::clone(&self.dropped));
        std::future::pending().await
    }
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct ForgingDriver;

impl ProviderPollDriver for ForgingDriver {
    fn provider_id(&self) -> &'static str {
        "provider-test"
    }

    async fn poll<S: ArtifactSink>(
        &self,
        _call: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> Result<PollObservation, image_provider_sdk::ProviderFailure> {
        sink.write_chunk(b"real-bytes")
            .await
            .map_err(sink_failure)?;
        sink.finalize(ArtifactMetadata {
            media_type: "image/png",
        })
        .await
        .map_err(sink_failure)?;
        let forged = DurableArtifactManifest::new(
            DurableArtifactRef::new("provider-test", "forged").unwrap(),
            "image/png",
            5,
            [9_u8; 32],
        )
        .unwrap();
        Ok(PollObservation::Completed(Completed::new(forged, None)))
    }
}

#[tokio::test]
async fn controlled_sink_acquires_materialization_capacity_only_on_first_byte() {
    let lease = lease(None);
    let factory = TestStagerFactory::default();
    let limiter = Arc::new(Semaphore::new(1));
    let context = ProviderArtifactStageContext::from_lease(&lease);
    let mut sink = ControlledProviderArtifactSink::new(&factory, context, Arc::clone(&limiter));

    assert_eq!(limiter.available_permits(), 1);
    assert_eq!(factory.begins.load(Ordering::SeqCst), 0);
    sink.write_chunk(b"artifact").await.unwrap();
    assert_eq!(limiter.available_permits(), 0);
    assert_eq!(factory.begins.load(Ordering::SeqCst), 1);
    let manifest = sink
        .finalize(ArtifactMetadata {
            media_type: "image/png",
        })
        .await
        .unwrap();
    assert!(
        sink.finalize(ArtifactMetadata {
            media_type: "image/png"
        })
        .await
        .is_err()
    );
    let staged = sink.into_finalized(&manifest).unwrap();

    assert_eq!(staged.manifest(), &manifest);
    assert_eq!(limiter.available_permits(), 1);
}

#[tokio::test]
async fn pending_poll_records_once_without_consuming_materialization_capacity() {
    let lease = lease(None);
    let store = FakeStore::with_lease(lease.clone());
    let provider = ScriptedFakeProvider::default();
    provider.push_poll(PollStep::Pending {
        next_poll_after_ms: Some(250),
    });
    let factory = TestStagerFactory::default();
    let orchestrator = orchestrator(store.clone(), provider.clone(), factory.clone());

    let result = orchestrator.run_once().await.unwrap();

    assert!(matches!(
        result,
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::ProviderWaiting,
            ..
        })
    ));
    let state = store.state.lock().unwrap();
    assert_eq!(provider.calls().poll, 1);
    assert_eq!(factory.begins.load(Ordering::SeqCst), 0);
    assert_eq!(state.actions, ["record"]);
    assert!(matches!(
        state.observations[0].outcome,
        ProviderTaskObservationOutcome::Waiting { poll_after_ms: 250 }
    ));
}

#[tokio::test]
async fn completed_poll_publishes_before_recording_terminal_evidence() {
    let lease = lease(None);
    let store = FakeStore::with_lease(lease);
    let provider = ScriptedFakeProvider::default();
    provider.push_poll(PollStep::Complete(OutputPlan {
        chunks: vec![b"one".to_vec(), b"two".to_vec()],
        media_type: "image/png".to_owned(),
        provider_request_id: None,
    }));
    let orchestrator = orchestrator(store.clone(), provider, TestStagerFactory::default());

    let result = orchestrator.run_once().await.unwrap();

    assert!(matches!(
        result,
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::ArtifactReady,
            ..
        })
    ));
    let state = store.state.lock().unwrap();
    assert_eq!(state.actions, ["publish", "record"]);
    assert!(matches!(
        state.observations[0].outcome,
        ProviderTaskObservationOutcome::ArtifactReady { .. }
    ));
}

#[tokio::test]
async fn committed_authority_recovery_skips_provider_and_stager() {
    let base = lease(None);
    let authority = authority(&base, b"committed");
    let committed = publication(&base, &authority);
    let lease = lease(Some(committed));
    let store = FakeStore::with_lease(lease);
    let provider = ScriptedFakeProvider::default();
    let factory = TestStagerFactory::default();
    let orchestrator = orchestrator(store.clone(), provider.clone(), factory.clone());

    let result = orchestrator.run_once().await.unwrap();

    assert!(matches!(
        result,
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::ArtifactReady,
            ..
        })
    ));
    assert_eq!(provider.calls().poll, 0);
    assert_eq!(factory.begins.load(Ordering::SeqCst), 0);
    assert_eq!(store.state.lock().unwrap().actions, ["record"]);
}

#[tokio::test]
async fn long_poll_renews_lease_while_lightweight_pending_uses_no_sink_bytes() {
    let store = FakeStore::with_lease(lease(None));
    let orchestrator = ProviderPollOrchestrator::new(
        store.clone(),
        SlowPendingDriver,
        TestStagerFactory::default(),
        config(Duration::from_millis(5)),
    )
    .unwrap();

    orchestrator.run_once().await.unwrap();

    assert!(store.state.lock().unwrap().heartbeat_count >= 2);
}

#[tokio::test]
async fn heartbeat_loss_cancels_in_flight_provider_future_without_observation() {
    let store = FakeStore::with_lease(lease(None));
    store.state.lock().unwrap().fail_heartbeat = true;
    let dropped = Arc::new(AtomicBool::new(false));
    let orchestrator = ProviderPollOrchestrator::new(
        store.clone(),
        NeverDriver {
            dropped: Arc::clone(&dropped),
        },
        TestStagerFactory::default(),
        config(Duration::from_millis(5)),
    )
    .unwrap();

    let error = tokio::time::timeout(Duration::from_secs(1), orchestrator.run_once())
        .await
        .unwrap()
        .unwrap_err();

    assert!(matches!(
        error,
        ProviderPollOrchestratorError::Store(ProviderTaskStoreError::StaleLease)
    ));
    assert!(dropped.load(Ordering::SeqCst));
    assert!(store.state.lock().unwrap().observations.is_empty());
}

#[tokio::test]
async fn database_budget_timeout_cancels_in_flight_provider_future_without_observation() {
    let mut lease = lease(None);
    lease.remaining_budget_ms = 10;
    let store = FakeStore::with_lease(lease);
    let dropped = Arc::new(AtomicBool::new(false));
    let orchestrator = ProviderPollOrchestrator::new(
        store.clone(),
        NeverDriver {
            dropped: Arc::clone(&dropped),
        },
        TestStagerFactory::default(),
        config(Duration::from_millis(20)),
    )
    .unwrap();

    let error = tokio::time::timeout(Duration::from_secs(1), orchestrator.run_once())
        .await
        .unwrap()
        .unwrap_err();

    assert!(matches!(
        error,
        ProviderPollOrchestratorError::ProviderDeadlineElapsed
    ));
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(store.state.lock().unwrap().heartbeat_count, 0);
    assert!(store.state.lock().unwrap().observations.is_empty());
}

#[tokio::test]
async fn forged_completed_manifest_becomes_uncertain_without_authority_publication() {
    let store = FakeStore::with_lease(lease(None));
    let orchestrator = ProviderPollOrchestrator::new(
        store.clone(),
        ForgingDriver,
        TestStagerFactory::default(),
        config(Duration::from_millis(5)),
    )
    .unwrap();

    let result = orchestrator.run_once().await.unwrap();

    assert!(matches!(
        result,
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::Uncertain,
            ..
        })
    ));
    let state = store.state.lock().unwrap();
    assert_eq!(state.actions, ["record"]);
    assert!(matches!(
        &state.observations[0].outcome,
        ProviderTaskObservationOutcome::Uncertain { error_code }
            if error_code == "provider_poll_artifact_contract"
    ));
}

#[tokio::test]
async fn unverified_terminal_failures_become_uncertain() {
    let failures = [
        image_provider_sdk::ProviderFailure::new(
            image_provider_sdk::ProviderFailureClass::Throttled,
            "retryable_terminal_failure",
            image_provider_sdk::EffectCertainty::NoRemoteEffect,
            image_provider_sdk::RetryDirective::Backoff,
        )
        .unwrap(),
        image_provider_sdk::ProviderFailure::new(
            image_provider_sdk::ProviderFailureClass::Ambiguous,
            "ambiguous_terminal_failure",
            image_provider_sdk::EffectCertainty::UnknownRemoteEffect,
            image_provider_sdk::RetryDirective::Never,
        )
        .unwrap(),
    ];

    for failure in failures {
        let store = FakeStore::with_lease(lease(None));
        let provider = ScriptedFakeProvider::default();
        provider.push_poll(PollStep::Failed(failure));
        let orchestrator = orchestrator(store.clone(), provider, TestStagerFactory::default());

        let result = orchestrator.run_once().await.unwrap();

        assert!(matches!(
            result,
            ProviderPollRun::Observed(ProviderRemoteTask {
                state: ProviderTaskState::Uncertain,
                ..
            })
        ));
        assert!(matches!(
            &store.state.lock().unwrap().observations[0].outcome,
            ProviderTaskObservationOutcome::Uncertain { error_code }
                if error_code == "provider_poll_failure_contract"
        ));
    }
}

#[tokio::test]
async fn dreamina_driver_composes_through_account_fenced_poll_call() {
    let root = TempDir::new().unwrap();
    let workspace = root.path().join("workspace");
    let account_home = root.path().join("account-home");
    fs::create_dir(&workspace).unwrap();
    fs::create_dir(&account_home).unwrap();
    fs::set_permissions(&workspace, fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(&account_home, fs::Permissions::from_mode(0o700)).unwrap();
    let executable = root.path().join("dreamina");
    let script = br#"#!/bin/sh
submit=
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--submit_id" ]; then
    shift
    submit=$1
  fi
  shift
done
printf query > "$HOME/query-called"
printf '{"submit_id":"%s","gen_status":"querying"}' "$submit"
"#;
    fs::write(&executable, script).unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).unwrap();

    let provider_account_id = Uuid::from_u128(10);
    let execution_profile_id = Uuid::from_u128(3);
    let credential_auth_sha256 = "c".repeat(64);
    let binding = DreaminaCliRuntimeBindingV1::new(
        execution_profile_id,
        provider_account_id,
        credential_auth_sha256.clone(),
    )
    .unwrap();
    let policy = || {
        DreaminaCliQueryPolicyV1::new(
            &executable,
            Sha256::digest(script).into(),
            WorkingDirectory::new(&workspace).unwrap(),
            WorkingDirectory::new(&account_home).unwrap(),
            Duration::from_secs(5),
            Duration::from_millis(50),
        )
        .unwrap()
    };
    let driver = || DreaminaCliPollDriverV1::new(policy(), binding.clone(), 1024 * 1024).unwrap();
    let config = |provider_account_id| ProviderPollOrchestratorConfig {
        scope: ProviderTaskClaimScope {
            provider_id: DREAMINA_PROVIDER_ID.to_owned(),
            provider_account_id,
        },
        owner: "poll-owner".to_owned(),
        lease_ms: 100,
        heartbeat_interval: Duration::from_millis(20),
        max_materializations: 1,
    };

    let valid_lease = dreamina_lease(provider_account_id);
    let valid_store = FakeStore::with_lease(valid_lease);
    let orchestrator = ProviderPollOrchestrator::new(
        valid_store.clone(),
        driver(),
        TestStagerFactory::default(),
        config(provider_account_id),
    )
    .unwrap();

    let result = orchestrator.run_once().await.unwrap();

    assert!(matches!(
        result,
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::ProviderWaiting,
            ..
        })
    ));
    assert!(account_home.join("query-called").is_file());
    assert!(dreamina_workspace_has_no_attempts(&workspace));
    fs::remove_file(account_home.join("query-called")).unwrap();
    drop(orchestrator);

    let wrong_account_id = Uuid::from_u128(11);
    let wrong_store = FakeStore::with_lease(dreamina_lease(wrong_account_id));
    let orchestrator = ProviderPollOrchestrator::new(
        wrong_store.clone(),
        driver(),
        TestStagerFactory::default(),
        config(wrong_account_id),
    )
    .unwrap();

    let result = orchestrator.run_once().await.unwrap();

    assert!(matches!(
        result,
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::Uncertain,
            ..
        })
    ));
    assert!(matches!(
        &wrong_store.state.lock().unwrap().observations[0].outcome,
        ProviderTaskObservationOutcome::Uncertain { error_code }
            if error_code == "dreamina_poll_binding_mismatch"
    ));
    assert!(!account_home.join("query-called").exists());
    assert!(dreamina_workspace_has_no_attempts(&workspace));
}

fn orchestrator<D: ProviderPollDriver>(
    store: FakeStore,
    driver: D,
    factory: TestStagerFactory,
) -> ProviderPollOrchestrator<FakeStore, D, TestStagerFactory> {
    ProviderPollOrchestrator::new(store, driver, factory, config(Duration::from_millis(20)))
        .unwrap()
}

#[test]
fn invalid_materialization_limits_are_rejected_without_panicking() {
    for max_materializations in [0, Semaphore::MAX_PERMITS + 1] {
        let result = ProviderPollOrchestrator::new(
            FakeStore::default(),
            ScriptedFakeProvider::default(),
            TestStagerFactory::default(),
            ProviderPollOrchestratorConfig {
                max_materializations,
                ..config(Duration::from_millis(20))
            },
        );
        assert!(matches!(
            result,
            Err(ProviderPollOrchestratorError::InvalidConfiguration)
        ));
    }
}

fn config(heartbeat_interval: Duration) -> ProviderPollOrchestratorConfig {
    ProviderPollOrchestratorConfig {
        scope: scope(),
        owner: "poll-owner".to_owned(),
        lease_ms: 100,
        heartbeat_interval,
        max_materializations: 1,
    }
}

fn scope() -> ProviderTaskClaimScope {
    ProviderTaskClaimScope {
        provider_id: "provider-test".to_owned(),
        provider_account_id: Uuid::from_u128(10),
    }
}

fn lease(committed_artifact: Option<ProviderArtifactPublication>) -> ProviderTaskLease {
    ProviderTaskLease {
        task: ProviderRemoteTask {
            submission_id: Uuid::from_u128(1),
            executor_execution_id: Uuid::from_u128(2),
            provider_id: "provider-test".to_owned(),
            provider_account_id: Uuid::from_u128(10),
            remote_operation_id: "remote-operation-1".to_owned(),
            provider_request_id: None,
            state: ProviderTaskState::ProviderWaiting,
            artifact_ref: None,
            error_code: None,
            next_poll_at_ms: Some(1),
            cancel_requested: false,
            poll_lease_epoch: 1,
        },
        context: ProviderExecutionContext {
            model: "provider-test-model".to_owned(),
            command_schema: "provider-command-v1".to_owned(),
            command_hash: "a".repeat(64),
            operation_id: "images.generations".to_owned(),
            operation_descriptor_revision: "provider-test/images.generations/v1".to_owned(),
            operation_descriptor_sha256_v1: "b".repeat(64),
            completion_mode: "remote_task".to_owned(),
            idempotency_mode: "submission_bound".to_owned(),
            operation_binding_version: 2,
            execution_profile_id: Uuid::from_u128(3),
            adapter_revision: "provider-test-adapter-v1".to_owned(),
            credential_pool_id: Uuid::from_u128(4),
            credential_ref: "test-vault.provider.1".to_owned(),
            credential_revision: 1,
            credential_auth_sha256: "c".repeat(64),
            resource_policy_id: Uuid::from_u128(5),
            resource_policy_revision: 1,
            submission_idempotency_key: "provider-submit-1".to_owned(),
            provider_command_sha256: "d".repeat(64),
            execution_binding_sha256: "e".repeat(64),
            invocation_attempt: 1,
            provider_timeout_ms: 60_000,
            provider_deadline_at_ms: 9_999_999_999_999,
        },
        committed_artifact,
        remaining_budget_ms: 60_000,
        poll_owner: "poll-owner".to_owned(),
        poll_lease_epoch: 1,
        poll_lease_expires_at_ms: 9_999_999_999_999,
        authority_seal: [0_u8; 32],
    }
}

fn dreamina_lease(provider_account_id: Uuid) -> ProviderTaskLease {
    let mut lease = lease(None);
    lease.task.provider_id = DREAMINA_PROVIDER_ID.to_owned();
    lease.task.provider_account_id = provider_account_id;
    lease.task.remote_operation_id = "dreamina-task-1".to_owned();
    lease.context.model = "dreamina-image-3.0".to_owned();
    lease.context.command_schema = DREAMINA_SUBMIT_COMMAND_SCHEMA.to_owned();
    lease.context.operation_id = "images.generations".to_owned();
    lease.context.adapter_revision = DREAMINA_ADAPTER_REVISION.to_owned();
    lease
}

fn dreamina_workspace_has_no_attempts(path: &std::path::Path) -> bool {
    fs::read_dir(path).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .as_encoded_bytes()
            .starts_with(b".dreamina-poll-")
    })
}

fn authority(lease: &ProviderTaskLease, bytes: &[u8]) -> ProviderArtifactAuthority {
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    ProviderArtifactAuthority::new(
        "filesystem-v1".to_owned(),
        "filesystem-v1:provider-poll-test".to_owned(),
        executor_object_key(lease.task.executor_execution_id),
        hex::encode(digest),
        bytes.len() as u64,
        "image/png".to_owned(),
    )
    .unwrap()
}

fn publication(
    lease: &ProviderTaskLease,
    authority: &ProviderArtifactAuthority,
) -> ProviderArtifactPublication {
    ProviderArtifactPublication {
        manifest: ExecutorResultManifest::new(
            lease.task.submission_id,
            lease.task.executor_execution_id,
        )
        .unwrap(),
        sha256_hex: authority.sha256_hex.clone(),
        byte_size: authority.byte_size,
        media_type: authority.media_type.clone(),
    }
}

fn sink_error(code: &'static str) -> ArtifactSinkError {
    ArtifactSinkError::new(ArtifactSinkErrorKind::InvalidArtifact, code)
}

fn sink_failure(error: ArtifactSinkError) -> image_provider_sdk::ProviderFailure {
    image_provider_sdk::ProviderFailure::new(
        image_provider_sdk::ProviderFailureClass::ArtifactInvalid,
        error.code(),
        image_provider_sdk::EffectCertainty::UnknownRemoteEffect,
        image_provider_sdk::RetryDirective::Never,
    )
    .unwrap()
}
