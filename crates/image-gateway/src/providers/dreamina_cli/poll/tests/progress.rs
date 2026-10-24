use super::*;

#[tokio::test]
async fn single_batch_and_video_keep_valid_query_progress_and_bounded_cadence() {
    let cases = [
        (
            r#", "queue_info":{"queue_idx":3,"queue_length":100,"queue_status":"Queueing"},"estimated_remaining_seconds":27131"#,
            "Queueing",
            Some(3),
            Some(100),
            Some(27131),
            QUEUED_POLL_AFTER_MS,
        ),
        (
            r#", "queue_info":{"queue_idx":3,"queue_length":100,"queue_status":"Queueing"},"estimated_remaining_seconds":61"#,
            "Queueing",
            Some(3),
            Some(100),
            Some(61),
            QUEUED_POLL_AFTER_MS,
        ),
        (
            r#", "queue_info":{"queue_idx":3,"queue_length":100,"queue_status":"Queueing"},"estimated_remaining_seconds":60"#,
            "Queueing",
            Some(3),
            Some(100),
            Some(60),
            DEFAULT_POLL_AFTER_MS,
        ),
        (
            r#", "queue_info":{"queue_idx":0,"queue_length":100,"queue_status":"Queueing"},"estimated_remaining_seconds":0"#,
            "Queueing",
            Some(0),
            Some(100),
            Some(0),
            DEFAULT_POLL_AFTER_MS,
        ),
        (
            r#", "queue_info":{"queue_idx":3,"queue_length":100,"queue_status":"Queueing"}"#,
            "Queueing",
            Some(3),
            Some(100),
            None,
            QUEUED_POLL_AFTER_MS,
        ),
        (
            r#", "queue_info":{"queue_idx":0,"queue_length":100,"queue_status":"Generating"},"estimated_remaining_seconds":120"#,
            "Generating",
            Some(0),
            Some(100),
            Some(120),
            DEFAULT_POLL_AFTER_MS,
        ),
        (
            r#", "queue_info":{"queue_idx":0,"queue_length":100,"queue_status":"FutureState"},"estimated_remaining_seconds":-1"#,
            "FutureState",
            Some(0),
            Some(100),
            None,
            DEFAULT_POLL_AFTER_MS,
        ),
        ("", "querying", None, None, None, DEFAULT_POLL_AFTER_MS),
        (
            r#", "estimated_remaining_seconds":45"#,
            "querying",
            None,
            None,
            Some(45),
            DEFAULT_POLL_AFTER_MS,
        ),
        (
            r#", "queue_info":{"queue_idx":200,"queue_length":100,"queue_status":"Queueing"},"estimated_remaining_seconds":2592001"#,
            "querying",
            None,
            None,
            None,
            DEFAULT_POLL_AFTER_MS,
        ),
    ];
    for (fields, status, queue_position, queue_total, estimate, delay) in cases {
        for mode in ["single", "native_batch", "video"] {
            let fixture = Fixture::new(&format!(
                "printf '%s' '{{\"submit_id\":\"task-1\",\"gen_status\":\"querying\"{fields}}}'"
            ));
            let driver = fixture.driver(1024);
            let (actual_delay, progress) = poll_pending(&fixture, &driver, mode).await.unwrap();
            assert_eq!(actual_delay, Some(delay), "{mode}: {fields}");
            assert_eq!(
                progress,
                ProviderTaskProgress {
                    provider_status: status.to_owned(),
                    queue_position,
                    queue_total,
                    estimated_remaining_seconds: estimate,
                },
                "{mode}: {fields}"
            );
            assert!(progress.validate().is_ok());
            fixture.assert_queries_used_original_submit_id(1);
            fixture.assert_workspace_has_no_attempts();
        }
    }
}

#[tokio::test]
async fn transient_query_failure_retries_the_original_submit_id_after_five_seconds() {
    for mode in ["single", "native_batch", "video"] {
        let fixture = Fixture::new(
            r#"if [ ! -f "$HOME/failed-once" ]; then
  : > "$HOME/failed-once"
  exit 1
fi
printf '{"submit_id":"%s","gen_status":"querying"}' "$submit""#,
        );
        let driver = fixture.driver(1024);
        let failure = poll_pending(&fixture, &driver, mode).await.unwrap_err();
        assert_eq!(failure.code(), "dreamina_poll_process_unavailable");
        assert_query_retry(&failure);
        let (delay, progress) = poll_pending(&fixture, &driver, mode).await.unwrap();
        assert_eq!(delay, Some(DEFAULT_POLL_AFTER_MS));
        assert_eq!(progress.provider_status, "querying");
        assert!(progress.validate().is_ok());
        fixture.assert_queries_used_original_submit_id(2);
        fixture.assert_workspace_has_no_attempts();
    }
}

#[tokio::test]
async fn artifact_storage_failures_keep_the_same_query_and_explicit_retry_interval() {
    for native_batch in [false, true] {
        for fail_on_finalize in [false, true] {
            let fixture = Fixture::new(if native_batch {
                r#"/bin/cp "$HOME/source.png" "$download/00.png"
/bin/cp "$HOME/source.png" "$download/01.png"
printf '{"submit_id":"%s","gen_status":"success"}' "$submit""#
            } else {
                r#"/bin/cp "$HOME/source.png" "$download/result.png"
printf '{"submit_id":"%s","gen_status":"success"}' "$submit""#
            });
            fs::write(
                fixture.account_home.join("source.png"),
                png_bytes([1, 2, 3, 255]),
            )
            .unwrap();
            let driver = fixture.driver(1024);
            let operation = fixture.operation();
            for _ in 0..2 {
                let mut sink = StorageFailingSink { fail_on_finalize };
                let failure = if native_batch {
                    driver
                        .poll_native_batch_operation(&operation, 2, &mut sink)
                        .await
                        .unwrap_err()
                } else {
                    driver
                        .poll_operation(&operation, false, &mut sink)
                        .await
                        .unwrap_err()
                };
                assert_eq!(failure.code(), "dreamina_poll_artifact_storage_unavailable");
                assert_query_retry(&failure);
            }
            fixture.assert_queries_used_original_submit_id(2);
            fixture.assert_workspace_has_no_attempts();
        }
    }
}

fn assert_query_retry(failure: &ProviderFailure) {
    assert_eq!(failure.class(), ProviderFailureClass::Transient);
    assert_eq!(failure.effect(), EffectCertainty::NoRemoteEffect);
    assert_eq!(failure.retry(), RetryDirective::Backoff);
    assert_eq!(failure.retry_after_ms(), Some(5_000));
}

pub(super) async fn poll_pending(
    fixture: &Fixture,
    driver: &DreaminaCliPollDriverV1,
    mode: &str,
) -> Result<(Option<u64>, ProviderTaskProgress), ProviderFailure> {
    let operation = fixture.operation();
    if mode == "native_batch" {
        let mut sink = BatchTestSink::default();
        let observation = driver
            .poll_native_batch_operation(&operation, 2, &mut sink)
            .await?;
        assert!(sink.outputs.is_empty());
        assert_eq!(sink.finalized, 0);
        let ProviderNativeBatchPollObservation::PendingWithProgress {
            next_poll_after_ms,
            progress,
        } = observation
        else {
            panic!("expected native batch pending progress");
        };
        Ok((next_poll_after_ms, progress))
    } else {
        let mut sink = fixture.sink([0; 32]);
        let observation = driver
            .poll_operation(&operation, mode == "video", &mut sink)
            .await?;
        assert!(sink.bytes().is_empty());
        assert_eq!(sink.finalize_count(), 0);
        let PollObservation::PendingWithProgress {
            next_poll_after_ms,
            progress,
        } = observation
        else {
            panic!("expected pending progress");
        };
        Ok((next_poll_after_ms, progress))
    }
}

struct StorageFailingSink {
    fail_on_finalize: bool,
}

impl ArtifactSink for StorageFailingSink {
    async fn write_chunk(&mut self, _: &[u8]) -> Result<(), ArtifactSinkError> {
        if self.fail_on_finalize {
            Ok(())
        } else {
            Err(ArtifactSinkError::new(
                ArtifactSinkErrorKind::Storage,
                "unavailable",
            ))
        }
    }

    async fn finalize(
        &mut self,
        _: ArtifactMetadata<'_>,
    ) -> Result<image_provider_sdk::DurableArtifactManifest, ArtifactSinkError> {
        Err(ArtifactSinkError::new(
            ArtifactSinkErrorKind::Storage,
            "unavailable",
        ))
    }
}

impl ProviderNativeBatchArtifactSink for StorageFailingSink {
    fn select_output(&mut self, _: usize) -> Result<(), ArtifactSinkError> {
        Ok(())
    }
}
