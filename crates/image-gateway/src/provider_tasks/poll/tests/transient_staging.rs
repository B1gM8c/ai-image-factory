use super::*;

#[derive(Clone, Copy)]
enum Fault {
    Begin,
    AdditionalBegin,
    Integrity,
    FirstChunk,
    SecondChunk,
    Finalize,
}

#[derive(Clone)]
struct FaultingFactory {
    inner: TestStagerFactory,
    fault: Fault,
    armed: Arc<AtomicBool>,
    drops: Arc<AtomicUsize>,
}

struct FaultingStager {
    inner: TestStager,
    factory: FaultingFactory,
    chunks: usize,
}

impl Drop for FaultingStager {
    fn drop(&mut self) {
        self.factory.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl ProviderArtifactStagerFactory for FaultingFactory {
    type Stager = FaultingStager;

    async fn begin(
        &self,
        context: &ProviderArtifactStageContext,
    ) -> Result<Self::Stager, ArtifactSinkError> {
        if matches!(self.fault, Fault::Integrity) {
            return Err(ArtifactSinkError::new(
                ArtifactSinkErrorKind::InvalidArtifact,
                "injected_conflicting_object",
            ));
        }
        if (matches!(self.fault, Fault::Begin)
            || matches!(self.fault, Fault::AdditionalBegin)
                && context.artifact_id() != context.executor_execution_id())
            && self.armed.swap(false, Ordering::SeqCst)
        {
            return Err(storage_failure());
        }
        Ok(FaultingStager {
            inner: self.inner.begin(context).await?,
            factory: self.clone(),
            chunks: 0,
        })
    }
}

impl ProviderArtifactStager for FaultingStager {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), ArtifactSinkError> {
        self.chunks += 1;
        if (matches!(self.factory.fault, Fault::FirstChunk) && self.chunks == 1
            || matches!(self.factory.fault, Fault::SecondChunk) && self.chunks == 2)
            && self.factory.armed.swap(false, Ordering::SeqCst)
        {
            return Err(storage_failure());
        }
        self.inner.write_chunk(chunk).await
    }

    async fn finalize(
        &mut self,
        metadata: ArtifactMetadata<'_>,
    ) -> Result<StagedProviderArtifact, ArtifactSinkError> {
        if matches!(self.factory.fault, Fault::Finalize)
            && self.factory.armed.swap(false, Ordering::SeqCst)
        {
            return Err(storage_failure());
        }
        self.inner.finalize(metadata).await
    }
}

#[derive(Clone)]
struct StreamingDriver {
    operations: Arc<Mutex<Vec<String>>>,
    fail_after_partial_download: Arc<AtomicBool>,
}

impl ProviderPollDriver for StreamingDriver {
    fn provider_id(&self) -> &'static str {
        "provider-test"
    }

    async fn poll<S: ArtifactSink>(
        &self,
        call: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> Result<PollObservation, image_provider_sdk::ProviderFailure> {
        self.operations
            .lock()
            .unwrap()
            .push(call.operation().operation_id().to_owned());
        sink.write_chunk(b"first")
            .await
            .map_err(transient_storage)?;
        if self
            .fail_after_partial_download
            .swap(false, Ordering::SeqCst)
        {
            return Err(transient_storage(storage_failure()));
        }
        sink.write_chunk(b"second")
            .await
            .map_err(transient_storage)?;
        let manifest = sink
            .finalize(ArtifactMetadata {
                media_type: "image/png",
            })
            .await
            .map_err(transient_storage)?;
        Ok(PollObservation::Completed(Completed::new(manifest, None)))
    }
}

fn storage_failure() -> ArtifactSinkError {
    ArtifactSinkError::new(
        ArtifactSinkErrorKind::Storage,
        "injected_storage_unavailable",
    )
}

fn transient_storage(error: ArtifactSinkError) -> image_provider_sdk::ProviderFailure {
    image_provider_sdk::ProviderFailure::new(
        image_provider_sdk::ProviderFailureClass::Transient,
        error.code(),
        image_provider_sdk::EffectCertainty::NoRemoteEffect,
        image_provider_sdk::RetryDirective::Backoff,
    )
    .unwrap()
}

#[tokio::test]
async fn transient_begin_chunk_and_finalize_failures_retry_the_same_accepted_operation() {
    for fault in [
        Fault::Begin,
        Fault::FirstChunk,
        Fault::SecondChunk,
        Fault::Finalize,
    ] {
        let original = lease(None);
        let store = FakeStore::with_lease(original.clone());
        let factory = FaultingFactory {
            inner: TestStagerFactory::default(),
            fault,
            armed: Arc::new(AtomicBool::new(true)),
            drops: Arc::new(AtomicUsize::new(0)),
        };
        let driver = StreamingDriver {
            operations: Arc::new(Mutex::new(Vec::new())),
            fail_after_partial_download: Arc::new(AtomicBool::new(false)),
        };
        let orchestrator = ProviderPollOrchestrator::new(
            store.clone(),
            driver.clone(),
            factory.clone(),
            config(Duration::from_millis(20)),
        )
        .unwrap();
        assert!(matches!(
            orchestrator.run_once().await.unwrap(),
            ProviderPollRun::Observed(ProviderRemoteTask {
                state: ProviderTaskState::ProviderWaiting,
                ..
            })
        ));
        {
            let mut state = store.state.lock().unwrap();
            assert!(state.saved_primary.is_none());
            assert_eq!(state.actions, ["record"]);
            state.lease = Some(original.clone());
        }
        assert!(matches!(
            orchestrator.run_once().await.unwrap(),
            ProviderPollRun::Observed(ProviderRemoteTask {
                state: ProviderTaskState::ArtifactReady,
                ..
            })
        ));
        assert_eq!(
            *driver.operations.lock().unwrap(),
            [
                original.task.remote_operation_id.clone(),
                original.task.remote_operation_id.clone()
            ]
        );
        let state = store.state.lock().unwrap();
        assert_eq!(state.actions, ["record", "publish", "record"]);
        assert!(state.saved_primary.is_some());
        assert!(factory.drops.load(Ordering::SeqCst) > 0);
    }
}

#[tokio::test]
async fn incomplete_transport_stream_is_discarded_before_querying_the_same_id_again() {
    let original = lease(None);
    let store = FakeStore::with_lease(original.clone());
    let driver = StreamingDriver {
        operations: Arc::new(Mutex::new(Vec::new())),
        fail_after_partial_download: Arc::new(AtomicBool::new(true)),
    };
    let factory = FaultingFactory {
        inner: TestStagerFactory::default(),
        fault: Fault::Begin,
        armed: Arc::new(AtomicBool::new(false)),
        drops: Arc::new(AtomicUsize::new(0)),
    };
    let orchestrator = ProviderPollOrchestrator::new(
        store.clone(),
        driver.clone(),
        factory.clone(),
        config(Duration::from_millis(20)),
    )
    .unwrap();
    orchestrator.run_once().await.unwrap();
    assert_eq!(factory.drops.load(Ordering::SeqCst), 1);
    store.state.lock().unwrap().lease = Some(original.clone());
    orchestrator.run_once().await.unwrap();
    assert_eq!(
        *driver.operations.lock().unwrap(),
        [
            original.task.remote_operation_id.clone(),
            original.task.remote_operation_id
        ]
    );
    assert_eq!(
        store.state.lock().unwrap().actions,
        ["record", "publish", "record"]
    );
}

#[derive(Clone)]
struct AdditionalDriver {
    operations: Arc<Mutex<Vec<String>>>,
}

impl ProviderPollDriver for AdditionalDriver {
    fn provider_id(&self) -> &'static str {
        DREAMINA_PROVIDER_ID
    }
    fn supports_additional_images(&self) -> bool {
        true
    }
    async fn poll<S: ArtifactSink>(
        &self,
        _: &ProviderPollDriverCall,
        _: &mut S,
    ) -> Result<PollObservation, image_provider_sdk::ProviderFailure> {
        panic!("use additional sink")
    }
    async fn poll_with_additional_images<S: ProviderAdditionalImageArtifactSink>(
        &self,
        call: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> Result<PollObservation, image_provider_sdk::ProviderFailure> {
        self.operations
            .lock()
            .unwrap()
            .push(call.operation().operation_id().to_owned());
        let mut primary = None;
        for index in 0..2 {
            sink.select_image(index).map_err(sink_failure)?;
            sink.write_chunk(&[index as u8 + 1])
                .await
                .map_err(transient_storage)?;
            let manifest = sink
                .finalize(ArtifactMetadata {
                    media_type: "image/png",
                })
                .await
                .map_err(transient_storage)?;
            if index == 0 {
                primary = Some(manifest);
            }
        }
        Ok(PollObservation::Completed(Completed::new(
            primary.unwrap(),
            None,
        )))
    }
}

#[tokio::test]
async fn transient_sibling_storage_failure_keeps_all_results_unpublished_then_requeries_same_id() {
    let original = dreamina_lease(Uuid::from_u128(10));
    let store = FakeStore::with_lease(original.clone());
    let factory = FaultingFactory {
        inner: TestStagerFactory::default(),
        fault: Fault::AdditionalBegin,
        armed: Arc::new(AtomicBool::new(true)),
        drops: Arc::new(AtomicUsize::new(0)),
    };
    let driver = AdditionalDriver {
        operations: Arc::new(Mutex::new(Vec::new())),
    };
    let mut cfg = config(Duration::from_millis(20));
    cfg.scope.provider_id = DREAMINA_PROVIDER_ID.to_owned();
    let orchestrator =
        ProviderPollOrchestrator::new(store.clone(), driver.clone(), factory, cfg).unwrap();
    assert!(matches!(
        orchestrator.run_once().await.unwrap(),
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::ProviderWaiting,
            ..
        })
    ));
    {
        let state = store.state.lock().unwrap();
        assert_eq!(state.actions, ["prepare-extras", "record"]);
        assert!(state.saved_primary.is_none());
        assert!(state.additional_images.is_none());
    }
    store.state.lock().unwrap().lease = Some(original.clone());
    assert!(matches!(
        orchestrator.run_once().await.unwrap(),
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::ArtifactReady,
            ..
        })
    ));
    let state = store.state.lock().unwrap();
    assert_eq!(
        state.actions,
        [
            "prepare-extras",
            "record",
            "prepare-extras",
            "publish-extras",
            "publish",
            "record"
        ]
    );
    assert_eq!(state.additional_images.as_ref().unwrap().len(), 1);
    assert_eq!(
        *driver.operations.lock().unwrap(),
        [
            original.task.remote_operation_id.clone(),
            original.task.remote_operation_id
        ]
    );
}

#[tokio::test]
async fn integrity_failure_cannot_be_relabelled_as_retryable_storage_by_a_driver() {
    let store = FakeStore::with_lease(lease(None));
    let factory = FaultingFactory {
        inner: TestStagerFactory::default(),
        fault: Fault::Integrity,
        armed: Arc::new(AtomicBool::new(true)),
        drops: Arc::new(AtomicUsize::new(0)),
    };
    let driver = StreamingDriver {
        operations: Arc::new(Mutex::new(Vec::new())),
        fail_after_partial_download: Arc::new(AtomicBool::new(false)),
    };
    let orchestrator = ProviderPollOrchestrator::new(
        store.clone(),
        driver,
        factory,
        config(Duration::from_millis(20)),
    )
    .unwrap();
    assert!(matches!(
        orchestrator.run_once().await.unwrap(),
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::Uncertain,
            ..
        })
    ));
    assert!(store.state.lock().unwrap().saved_primary.is_none());
}

#[derive(Clone)]
struct VideoDeliveryDriver {
    operations: Arc<Mutex<Vec<String>>>,
    fail_first: Arc<AtomicBool>,
}

impl ProviderPollDriver for VideoDeliveryDriver {
    fn provider_id(&self) -> &'static str {
        "provider-test"
    }
    async fn poll<S: ArtifactSink>(
        &self,
        call: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> Result<PollObservation, image_provider_sdk::ProviderFailure> {
        self.operations
            .lock()
            .unwrap()
            .push(call.operation().operation_id().to_owned());
        let bytes = include_bytes!("../../../media_inputs/fixtures/blue.mp4");
        for chunk in bytes.chunks(37) {
            sink.write_chunk(chunk).await.map_err(transient_storage)?;
            if self.fail_first.swap(false, Ordering::SeqCst) {
                return Err(transient_storage(storage_failure()));
            }
        }
        let manifest = sink
            .finalize(ArtifactMetadata {
                media_type: "video/mp4",
            })
            .await
            .map_err(sink_failure)?;
        Ok(PollObservation::Completed(Completed::new(manifest, None)))
    }
}

#[tokio::test]
async fn interrupted_video_materialization_recovers_same_id_to_one_canonical_mp4_publication() {
    use crate::artifacts::{
        FilesystemArtifactBlobStore, FilesystemProviderArtifactStagerFactory, MAX_ARTIFACT_BYTES,
    };
    let root = tempfile::tempdir().unwrap();
    let blobs = Arc::new(FilesystemArtifactBlobStore::new(root.path()).unwrap());
    let factory = FilesystemProviderArtifactStagerFactory::new(blobs, MAX_ARTIFACT_BYTES).unwrap();
    let mut original = lease(None);
    original.context.operation_id = "videos.generations".to_owned();
    let store = FakeStore::with_lease(original.clone());
    let driver = VideoDeliveryDriver {
        operations: Arc::new(Mutex::new(Vec::new())),
        fail_first: Arc::new(AtomicBool::new(true)),
    };
    let orchestrator = ProviderPollOrchestrator::new(
        store.clone(),
        driver.clone(),
        factory,
        config(Duration::from_millis(20)),
    )
    .unwrap();
    assert!(matches!(
        orchestrator.run_once().await.unwrap(),
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::ProviderWaiting,
            ..
        })
    ));
    assert!(store.state.lock().unwrap().saved_primary.is_none());
    store.state.lock().unwrap().lease = Some(original.clone());
    assert!(matches!(
        orchestrator.run_once().await.unwrap(),
        ProviderPollRun::Observed(ProviderRemoteTask {
            state: ProviderTaskState::ArtifactReady,
            ..
        })
    ));
    let publication = store.state.lock().unwrap().saved_primary.clone().unwrap();
    assert_eq!(publication.media_type, "video/mp4");
    let object = root
        .path()
        .join(executor_object_key(original.task.executor_execution_id));
    assert_eq!(
        fs::read(object).unwrap(),
        include_bytes!("../../../media_inputs/fixtures/blue.mp4")
    );
    let mut replay = original.clone();
    replay.committed_artifact = Some(publication);
    store.state.lock().unwrap().lease = Some(replay);
    orchestrator.run_once().await.unwrap();
    assert_eq!(
        *driver.operations.lock().unwrap(),
        [
            original.task.remote_operation_id.clone(),
            original.task.remote_operation_id
        ]
    );
    assert_eq!(
        store
            .state
            .lock()
            .unwrap()
            .actions
            .iter()
            .filter(|action| **action == "publish")
            .count(),
        1
    );
}
