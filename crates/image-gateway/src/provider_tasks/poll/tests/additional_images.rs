use super::*;
use crate::provider_tasks::ProviderAdditionalImageArtifactSink;

#[derive(Clone)]
struct PhysicalImagesDriver {
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl ProviderPollDriver for PhysicalImagesDriver {
    fn supports_additional_images(&self) -> bool {
        true
    }

    fn provider_id(&self) -> &'static str {
        DREAMINA_PROVIDER_ID
    }

    async fn poll<S: ArtifactSink>(
        &self,
        _: &ProviderPollDriverCall,
        _: &mut S,
    ) -> Result<PollObservation, image_provider_sdk::ProviderFailure> {
        panic!("physical image branch must use the explicit sink")
    }

    async fn poll_with_additional_images<S: ProviderAdditionalImageArtifactSink>(
        &self,
        _: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> Result<PollObservation, image_provider_sdk::ProviderFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let outputs = self.outputs.lock().unwrap().clone();
        let mut primary = None;
        for (index, bytes) in outputs.iter().enumerate() {
            sink.select_image(index).map_err(sink_failure)?;
            sink.write_chunk(bytes).await.map_err(sink_failure)?;
            let manifest = sink
                .finalize(ArtifactMetadata {
                    media_type: "image/png",
                })
                .await
                .map_err(sink_failure)?;
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

fn fixture(
    count: usize,
) -> (
    FakeStore,
    PhysicalImagesDriver,
    TestStagerFactory,
    ProviderTaskLease,
) {
    let lease = dreamina_lease(Uuid::from_u128(10));
    let store = FakeStore::with_lease(lease.clone());
    let driver = PhysicalImagesDriver {
        calls: Arc::new(AtomicUsize::new(0)),
        outputs: Arc::new(Mutex::new(
            (0..count)
                .map(|index| format!("physical-{index}").into_bytes())
                .collect(),
        )),
    };
    (store, driver, TestStagerFactory::default(), lease)
}

fn image_orchestrator(
    store: FakeStore,
    driver: PhysicalImagesDriver,
    factory: TestStagerFactory,
) -> ProviderPollOrchestrator<FakeStore, PhysicalImagesDriver, TestStagerFactory> {
    let mut config = config(Duration::from_millis(20));
    config.scope.provider_id = DREAMINA_PROVIDER_ID.to_owned();
    ProviderPollOrchestrator::new(store, driver, factory, config).unwrap()
}

#[tokio::test]
async fn one_paid_slot_publishes_all_physical_images_before_the_leader_with_one_permit() {
    for count in [1, 4, 10] {
        let (store, driver, factory, lease) = fixture(count);
        let run = tokio::time::timeout(
            Duration::from_secs(1),
            image_orchestrator(store.clone(), driver.clone(), factory.clone()).run_once(),
        )
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
        let state = store.state.lock().unwrap();
        assert_eq!(
            state.actions,
            ["prepare-extras", "publish-extras", "publish", "record"]
        );
        let extra = state.additional_images.as_ref().unwrap();
        assert_eq!(extra.len(), count - 1);
        for (index, authority) in extra {
            assert_eq!(
                authority.object_key,
                executor_object_key(
                    crate::provider_tasks::additional_image_artifact_id(
                        lease.task.executor_execution_id,
                        *index
                    )
                    .unwrap()
                )
            );
        }
        assert_eq!(factory.begins.load(Ordering::SeqCst), count);
        assert_eq!(driver.calls.load(Ordering::SeqCst), 1);
        assert!(state.native_members.is_none());
        assert_eq!(state.observations.len(), 1);
    }
}

#[tokio::test]
async fn extra_storage_failure_never_publishes_or_settles_the_paid_leader() {
    let (store, driver, factory, lease) = fixture(4);
    store.state.lock().unwrap().fail_additional_publish_once = true;
    let orchestrator = image_orchestrator(store.clone(), driver.clone(), factory);
    assert!(orchestrator.run_once().await.is_err());
    {
        let mut state = store.state.lock().unwrap();
        assert_eq!(state.actions, ["prepare-extras"]);
        assert!(state.saved_primary.is_none());
        assert!(state.observations.is_empty());
        state.lease = Some(lease);
    }
    orchestrator.run_once().await.unwrap();
    assert_eq!(driver.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        store.state.lock().unwrap().actions,
        [
            "prepare-extras",
            "prepare-extras",
            "publish-extras",
            "publish",
            "record"
        ]
    );
}

#[tokio::test]
async fn restart_reuses_immutable_extras_and_rejects_a_changed_or_shrunk_result_set() {
    for changed in [false, true] {
        let (store, driver, factory, lease) = fixture(4);
        store.state.lock().unwrap().fail_primary_publish_once = true;
        let orchestrator = image_orchestrator(store.clone(), driver.clone(), factory);
        assert!(orchestrator.run_once().await.is_err());
        assert_eq!(
            store
                .state
                .lock()
                .unwrap()
                .additional_images
                .as_ref()
                .unwrap()
                .len(),
            3
        );
        if changed {
            driver.outputs.lock().unwrap().pop();
        }
        store.state.lock().unwrap().lease = Some(lease.clone());
        if changed {
            assert!(orchestrator.run_once().await.is_err());
            assert!(store.state.lock().unwrap().saved_primary.is_none());
            assert!(store.state.lock().unwrap().observations.is_empty());
        } else {
            orchestrator.run_once().await.unwrap();
            let primary = store.state.lock().unwrap().saved_primary.clone().unwrap();
            let mut recovered = lease;
            recovered.committed_artifact = Some(primary);
            store.state.lock().unwrap().lease = Some(recovered);
            orchestrator.run_once().await.unwrap();
            assert_eq!(
                driver.calls.load(Ordering::SeqCst),
                2,
                "a committed leader recovers without another provider query"
            );
            assert_eq!(
                store
                    .state
                    .lock()
                    .unwrap()
                    .additional_images
                    .as_ref()
                    .unwrap()
                    .len(),
                3
            );
        }
    }
}

#[tokio::test]
async fn physical_sink_rejects_non_image_or_non_contiguous_results() {
    let (_, _, factory, lease) = fixture(1);
    let limit = Arc::new(Semaphore::new(1));
    let mut sink = super::super::additional_images::ControlledAdditionalImageSink::new(
        &factory, &lease, limit,
    );
    assert!(sink.select_image(1).is_err());
    sink.select_image(0).unwrap();
    sink.write_chunk(b"first").await.unwrap();
    assert!(
        sink.finalize(ArtifactMetadata {
            media_type: "video/mp4"
        })
        .await
        .is_err()
    );
}
