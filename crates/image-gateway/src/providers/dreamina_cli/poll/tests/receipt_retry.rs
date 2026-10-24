use super::*;

#[tokio::test]
async fn malformed_and_unknown_query_receipts_back_off_without_reopening_the_task() {
    for mode in ["single", "native_batch", "video"] {
        let fixture = Fixture::new(
            r#"stage=0
if [ -f "$HOME/stage" ]; then stage=$(/bin/cat "$HOME/stage"); fi
printf '%s' "$((stage + 1))" > "$HOME/stage"
case "$stage" in
  0) printf '{"submit_id":"%s","gen_status":"querying","queue_info":{"queue_idx":3,"queue_length":100,"queue_status":"Queueing"}}' "$submit" ;;
  1) printf '{"gen_status":' ;;
  2) printf '{"submit_id":"%s","gen_status":"future-status"}' "$submit" ;;
  *) printf '{"submit_id":"%s","gen_status":"querying"}' "$submit" ;;
esac"#,
        );
        let driver = fixture.driver(1024);
        let (_, first) = progress::poll_pending(&fixture, &driver, mode)
            .await
            .unwrap();
        assert_eq!(first.provider_status, "Queueing");
        for _ in 0..2 {
            let failure = progress::poll_pending(&fixture, &driver, mode)
                .await
                .unwrap_err();
            assert_receipt_retry(&failure);
        }
        let (_, final_progress) = progress::poll_pending(&fixture, &driver, mode)
            .await
            .unwrap();
        assert_eq!(final_progress.provider_status, "querying");
        fixture.assert_queries_used_original_submit_id(4);
        fixture.assert_workspace_has_no_attempts();
    }
}

#[tokio::test]
async fn truncated_query_capture_retries_the_original_task_with_a_bounded_delay() {
    for mode in ["single", "native_batch", "video"] {
        for redirect in ["", ">&2"] {
            let fixture = Fixture::new(&format!(
                r#"if [ ! -f "$HOME/truncated-once" ]; then
  : > "$HOME/truncated-once"
  /usr/bin/head -c 65537 /dev/zero {redirect}
fi
printf '{{"submit_id":"%s","gen_status":"querying"}}' "$submit""#
            ));
            let driver = fixture.driver(1024);
            let failure = progress::poll_pending(&fixture, &driver, mode)
                .await
                .unwrap_err();
            assert_receipt_retry(&failure);
            let (_, observed) = progress::poll_pending(&fixture, &driver, mode)
                .await
                .unwrap();
            assert_eq!(observed.provider_status, "querying");
            fixture.assert_queries_used_original_submit_id(2);
            fixture.assert_workspace_has_no_attempts();
        }
    }
}

fn assert_receipt_retry(failure: &ProviderFailure) {
    assert_eq!(failure.code(), "dreamina_poll_receipt_unavailable");
    assert_eq!(failure.class(), ProviderFailureClass::Transient);
    assert_eq!(failure.effect(), EffectCertainty::NoRemoteEffect);
    assert_eq!(failure.retry(), RetryDirective::Backoff);
    assert_eq!(failure.retry_after_ms(), Some(30_000));
}

#[test]
fn safety_contract_runtime_failures_remain_permanent() {
    for error in [
        RuntimeError::Policy("invalid binding".to_owned()),
        RuntimeError::MissingOutputContract,
        RuntimeError::UnexpectedOutputContract,
        RuntimeError::Output(OutputError::ChangedDuringRead),
        RuntimeError::OutputTask("join failure".to_owned()),
    ] {
        let failure = map_runtime_error(error);
        assert_eq!(failure.code(), "dreamina_poll_protocol_invalid");
        assert_eq!(failure.class(), ProviderFailureClass::Permanent);
        assert_eq!(failure.retry(), RetryDirective::Never);
        assert_eq!(failure.retry_after_ms(), None);
    }
}
