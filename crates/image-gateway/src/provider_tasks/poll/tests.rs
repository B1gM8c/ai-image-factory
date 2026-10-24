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
        NativeBatchMember, ProviderArtifactAuthority, ProviderArtifactPublication,
        ProviderExecutionContext, ProviderRemoteTask, ProviderTaskClaimScope, ProviderTaskLease,
        ProviderTaskObservation, ProviderTaskObservationOutcome, ProviderTaskState,
        ProviderTaskStoreError,
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
    due_deadline: Option<ProviderRemoteTask>,
    heartbeat_count: usize,
    fail_heartbeat: bool,
    actions: Vec<&'static str>,
    observations: Vec<ProviderTaskObservation>,
    native_members: Option<Vec<NativeBatchMember>>,
    fail_native_publish_once: Option<i32>,
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
    async fn resolve_due_poll_deadline(
        &self,
        scope: &ProviderTaskClaimScope,
    ) -> Result<Option<ProviderRemoteTask>, ProviderTaskStoreError> {
        let mut state = self.state.lock().unwrap();
        if let Some(task) = state.due_deadline.take() {
            assert_eq!(task.provider_id, scope.provider_id);
            assert_eq!(task.provider_account_id, scope.provider_account_id);
            state.actions.push("deadline");
            return Ok(Some(task));
        }
        Ok(None)
    }

    async fn native_batch_members(
        &self,
        _lease: &ProviderTaskLease,
    ) -> Result<Option<Vec<NativeBatchMember>>, ProviderTaskStoreError> {
        Ok(self.state.lock().unwrap().native_members.clone())
    }

    async fn publish_native_batch_artifact(
        &self,
        lease: &ProviderTaskLease,
        output_index: i32,
        authority: &ProviderArtifactAuthority,
    ) -> Result<ProviderArtifactPublication, ProviderTaskStoreError> {
        let mut state = self.state.lock().unwrap();
        if state.fail_native_publish_once == Some(output_index) {
            state.fail_native_publish_once = None;
            return Err(ProviderTaskStoreError::Unavailable);
        }
        let member = &mut state.native_members.as_mut().unwrap()[output_index as usize];
        let mut member_lease = lease.clone();
        member_lease.task.submission_id = member.submission_id;
        member_lease.task.executor_execution_id = member.executor_execution_id;
        let publication = publication(&member_lease, authority);
        member.committed_artifact = Some(publication.clone());
        state.actions.push("publish-member");
        Ok(publication)
    }

    async fn record_native_batch_failure(
        &self,
        _lease: &ProviderTaskLease,
        output_index: i32,
        error_code: &str,
    ) -> Result<(), ProviderTaskStoreError> {
        let mut state = self.state.lock().unwrap();
        state.native_members.as_mut().unwrap()[output_index as usize].committed_failure_code =
            Some(error_code.to_owned());
        state.actions.push("missing-member");
        Ok(())
    }

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
        let mut state = self.state.lock().unwrap();
        if state.native_members.as_ref().is_some_and(|members| {
            members[1..].iter().any(|member| {
                member.committed_artifact.is_none() && member.committed_failure_code.is_none()
            })
        }) {
            return Err(ProviderTaskStoreError::Conflict);
        }
        state.actions.push("publish");
        let publication = publication(lease, authority);
        if let Some(members) = state.native_members.as_mut() {
            members[0].committed_artifact = Some(publication.clone());
        }
        Ok(publication)
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
            ProviderTaskObservationOutcome::RemoteReady { .. } => {
                task.state = ProviderTaskState::RemoteReady;
                task.next_poll_at_ms = None;
            }
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

struct ProgressDriver(image_provider_sdk::ProviderTaskProgress);

impl ProviderPollDriver for ProgressDriver {
    fn provider_id(&self) -> &'static str {
        "provider-test"
    }

    async fn poll<S: ArtifactSink>(
        &self,
        _call: &ProviderPollDriverCall,
        _sink: &mut S,
    ) -> Result<PollObservation, image_provider_sdk::ProviderFailure> {
        Ok(PollObservation::PendingWithProgress {
            next_poll_after_ms: Some(250),
            progress: self.0.clone(),
        })
    }
}

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

#[derive(Clone)]
struct NativeBatchDriver {
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl ProviderPollDriver for NativeBatchDriver {
    fn provider_id(&self) -> &'static str {
        DREAMINA_PROVIDER_ID
    }

    async fn poll<S: ArtifactSink>(
        &self,
        _call: &ProviderPollDriverCall,
        _sink: &mut S,
    ) -> Result<PollObservation, image_provider_sdk::ProviderFailure> {
        panic!("native batch must not use the single-output poll path")
    }

    async fn poll_native_batch<S: ProviderNativeBatchArtifactSink>(
        &self,
        _call: &ProviderPollDriverCall,
        _output_count: usize,
        sink: &mut S,
    ) -> Result<ProviderNativeBatchPollObservation, image_provider_sdk::ProviderFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let outputs = self.outputs.lock().unwrap().clone();
        for (index, bytes) in outputs.iter().enumerate() {
            sink.select_output(index).map_err(sink_failure)?;
            sink.write_chunk(bytes).await.map_err(sink_failure)?;
            sink.finalize(ArtifactMetadata {
                media_type: "image/png",
            })
            .await
            .map_err(sink_failure)?;
        }
        Ok(ProviderNativeBatchPollObservation::Completed {
            output_count: outputs.len(),
        })
    }
}

fn native_batch_fixture(
    count: usize,
    actual: usize,
) -> (
    FakeStore,
    NativeBatchDriver,
    TestStagerFactory,
    ProviderTaskLease,
) {
    let lease = dreamina_lease(Uuid::from_u128(10));
    let store = FakeStore::with_lease(lease.clone());
    store.state.lock().unwrap().native_members = Some(
        (0..count)
            .map(|index| NativeBatchMember {
                output_index: index as i32,
                submission_id: if index == 0 {
                    lease.task.submission_id
                } else {
                    Uuid::from_u128(100 + index as u128)
                },
                executor_execution_id: if index == 0 {
                    lease.task.executor_execution_id
                } else {
                    Uuid::from_u128(200 + index as u128)
                },
                output_id: Uuid::from_u128(300 + index as u128),
                job_id: Uuid::from_u128(400),
                committed_artifact: None,
                committed_failure_code: None,
            })
            .collect(),
    );
    let driver = NativeBatchDriver {
        calls: Arc::new(AtomicUsize::new(0)),
        outputs: Arc::new(Mutex::new(
            (0..actual)
                .map(|index| format!("image-{index}").into_bytes())
                .collect(),
        )),
    };
    (store, driver, TestStagerFactory::default(), lease)
}

fn native_batch_orchestrator(
    store: FakeStore,
    driver: NativeBatchDriver,
    factory: TestStagerFactory,
) -> ProviderPollOrchestrator<FakeStore, NativeBatchDriver, TestStagerFactory> {
    let mut config = config(Duration::from_millis(20));
    config.scope.provider_id = DREAMINA_PROVIDER_ID.to_owned();
    ProviderPollOrchestrator::new(store, driver, factory, config).unwrap()
}

#[tokio::test]
async fn native_batch_releases_single_staging_permit_between_all_ten_outputs() {
    let (store, driver, factory, _) = native_batch_fixture(10, 10);
    let orchestrator = native_batch_orchestrator(store.clone(), driver.clone(), factory.clone());
    let run = tokio::time::timeout(Duration::from_secs(1), orchestrator.run_once())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        run,
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::ArtifactReady,
            ..
        })
    ));
    assert_eq!(driver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(factory.begins.load(Ordering::SeqCst), 10);
    let state = store.state.lock().unwrap();
    assert!(
        state
            .native_members
            .as_ref()
            .unwrap()
            .iter()
            .all(|member| member.committed_artifact.is_some())
    );
    assert_eq!(state.actions.last(), Some(&"record"));
    assert_eq!(state.actions[9..], ["publish", "record"]);
    assert!(
        state.actions[..9]
            .iter()
            .all(|action| *action == "publish-member")
    );
}

#[tokio::test]
async fn native_batch_records_missing_outputs_before_leader_completion() {
    let (store, driver, factory, _) = native_batch_fixture(3, 1);
    let run = native_batch_orchestrator(store.clone(), driver, factory)
        .run_once()
        .await
        .unwrap();
    assert!(matches!(
        run,
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::ArtifactReady,
            ..
        })
    ));
    let state = store.state.lock().unwrap();
    assert_eq!(
        state.actions,
        ["missing-member", "missing-member", "publish", "record"]
    );
    let members = state.native_members.as_ref().unwrap();
    assert!(members[0].committed_artifact.is_some());
    assert!(
        members[1..]
            .iter()
            .all(|member| member.committed_failure_code.as_deref()
                == Some("dreamina_native_batch_result_missing"))
    );
}

#[tokio::test]
async fn native_batch_storage_failure_and_restart_reuse_committed_results_without_regeneration() {
    let (store, driver, factory, lease) = native_batch_fixture(3, 3);
    store.state.lock().unwrap().fail_native_publish_once = Some(2);
    let orchestrator = native_batch_orchestrator(store.clone(), driver.clone(), factory.clone());
    assert!(matches!(
        orchestrator.run_once().await,
        Err(ProviderPollOrchestratorError::Store(
            ProviderTaskStoreError::Unavailable
        ))
    ));
    assert_eq!(factory.begins.load(Ordering::SeqCst), 3);
    {
        let state = store.state.lock().unwrap();
        assert!(state.observations.is_empty());
        assert_eq!(state.actions, ["publish-member"]);
        let members = state.native_members.as_ref().unwrap();
        assert!(members[0].committed_artifact.is_none());
        assert!(members[1].committed_artifact.is_some());
        assert!(members[2].committed_artifact.is_none());
    }
    store.state.lock().unwrap().lease = Some(lease.clone());
    orchestrator.run_once().await.unwrap();
    assert_eq!(driver.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        factory.begins.load(Ordering::SeqCst),
        5,
        "committed follower is verified rather than staged again"
    );
    store.state.lock().unwrap().lease = Some(lease);
    orchestrator.run_once().await.unwrap();
    assert_eq!(
        driver.calls.load(Ordering::SeqCst),
        2,
        "all committed members recover without another query"
    );
    assert_eq!(factory.begins.load(Ordering::SeqCst), 5);
    assert_eq!(
        store.state.lock().unwrap().actions,
        [
            "publish-member",
            "publish-member",
            "publish",
            "record",
            "record"
        ]
    );
}

#[tokio::test]
async fn native_batch_restart_rejects_changed_ordinal_mapping() {
    let (store, driver, factory, lease) = native_batch_fixture(3, 3);
    store.state.lock().unwrap().fail_native_publish_once = Some(2);
    let orchestrator = native_batch_orchestrator(store.clone(), driver.clone(), factory.clone());
    assert!(orchestrator.run_once().await.is_err());
    driver.outputs.lock().unwrap().swap(1, 2);
    store.state.lock().unwrap().lease = Some(lease);
    let run = orchestrator.run_once().await.unwrap();
    assert!(matches!(
        run,
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::Uncertain,
            ..
        })
    ));
    assert_eq!(factory.begins.load(Ordering::SeqCst), 4);
    assert_eq!(driver.calls.load(Ordering::SeqCst), 2);
    let state = store.state.lock().unwrap();
    assert!(
        state.native_members.as_ref().unwrap()[0]
            .committed_artifact
            .is_none()
    );
    assert!(
        state.native_members.as_ref().unwrap()[1]
            .committed_artifact
            .is_some()
    );
    assert!(
        state.native_members.as_ref().unwrap()[2]
            .committed_artifact
            .is_none()
    );
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
async fn pending_progress_is_observation_metadata_without_materialization() {
    let progress = image_provider_sdk::ProviderTaskProgress {
        provider_status: "Queueing".to_owned(),
        queue_position: Some(3341),
        queue_total: Some(476_961),
        estimated_remaining_seconds: Some(27_131),
    };
    for valid in [true, false] {
        let store = FakeStore::with_lease(lease(None));
        let factory = TestStagerFactory::default();
        let mut reported = progress.clone();
        if !valid {
            reported.queue_total = Some(1);
        }
        let orchestrator = orchestrator(store.clone(), ProgressDriver(reported), factory.clone());
        assert!(matches!(
            orchestrator.run_once().await.unwrap(),
            ProviderPollRun::Observed(ProviderRemoteTask {
                state: ProviderTaskState::ProviderWaiting,
                ..
            })
        ));
        let state = store.state.lock().unwrap();
        assert_eq!(factory.begins.load(Ordering::SeqCst), 0);
        assert_eq!(state.actions, ["record"]);
        assert_eq!(
            state.observations[0].progress,
            valid.then(|| progress.clone())
        );
        assert_eq!(
            state.observations[0].outcome,
            ProviderTaskObservationOutcome::Waiting { poll_after_ms: 250 }
        );
    }
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
async fn native_batch_dreamina_driver_composes_through_account_fenced_poll_call() {
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
download=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --submit_id) shift; submit=$1 ;;
    --download_dir) shift; download=$1 ;;
  esac
  shift
done
printf x >> "$HOME/query-count"
/bin/cp "$HOME/source.png" "$download/00.png"
/bin/cp "$HOME/source.png" "$download/01.png"
printf '{"submit_id":"%s","gen_status":"success"}' "$submit"
"#;
    fs::write(&executable, script).unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).unwrap();
    fs::write(
        account_home.join("source.png"),
        b"\x89PNG\r\n\x1a\nnative-batch",
    )
    .unwrap();
    let (store, _, factory, lease) = native_batch_fixture(2, 2);
    let binding = DreaminaCliRuntimeBindingV1::new(
        lease.context.execution_profile_id,
        lease.task.provider_account_id,
        lease.context.credential_auth_sha256.clone(),
    )
    .unwrap();
    let policy = DreaminaCliQueryPolicyV1::new(
        &executable,
        Sha256::digest(script).into(),
        WorkingDirectory::new(&workspace).unwrap(),
        WorkingDirectory::new(&account_home).unwrap(),
        Duration::from_secs(5),
        Duration::from_millis(50),
    )
    .unwrap();
    let driver = DreaminaCliPollDriverV1::new(policy, binding, 1024 * 1024).unwrap();
    let mut config = config(Duration::from_millis(20));
    config.scope.provider_id = DREAMINA_PROVIDER_ID.to_owned();
    let orchestrator =
        ProviderPollOrchestrator::new(store.clone(), driver, factory.clone(), config).unwrap();
    let result = orchestrator.run_once().await.unwrap();
    assert!(matches!(
        result,
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::ArtifactReady,
            ..
        })
    ));
    assert_eq!(factory.begins.load(Ordering::SeqCst), 2);
    assert_eq!(
        store.state.lock().unwrap().actions,
        ["publish-member", "publish", "record"]
    );
    assert_eq!(fs::read(account_home.join("query-count")).unwrap(), b"x");
    assert!(dreamina_workspace_has_no_attempts(&workspace));
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
            execution_profile_id,
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

#[tokio::test]
async fn each_poll_iteration_resolves_a_due_deadline_before_launching_another_query() {
    let current = lease(None);
    let mut expired = current.task.clone();
    expired.state = ProviderTaskState::Uncertain;
    expired.error_code = Some("provider_remote_task_deadline".to_owned());
    expired.next_poll_at_ms = None;
    let store = FakeStore::with_lease(current);
    store.state.lock().unwrap().due_deadline = Some(expired.clone());
    let factory = TestStagerFactory::default();
    let run = orchestrator(
        store.clone(),
        ScriptedFakeProvider::default(),
        factory.clone(),
    )
    .run_once()
    .await
    .unwrap();
    assert_eq!(run, ProviderPollRun::Observed(expired));
    let state = store.state.lock().unwrap();
    assert!(state.lease.is_some());
    assert!(state.observations.is_empty());
    assert_eq!(state.actions, ["deadline"]);
    assert_eq!(factory.begins.load(Ordering::SeqCst), 0);
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
        execution_profile_id: Uuid::from_u128(3),
    }
}

pub(super) fn lease(committed_artifact: Option<ProviderArtifactPublication>) -> ProviderTaskLease {
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
        completion_deadline_at_ms: 9_999_999_999_999,
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
