use super::*;
use gpt_image_2_gateway::billing_control::{
    BillingAccountControlService, BillingControlActor, PostgresBillingAccountControlService,
    UpdateBillingAccountLimitRequest,
};

async fn claimed(pool: &PgPool, fixture: &Fixture) -> TestResult<DreaminaVideoSupplementPlan> {
    let service = store(pool);
    let plan = service
        .authorize_dreamina_video_supplement(&fixture.request)
        .await
        .map_err(debug_error)?;
    service
        .claim_dreamina_video_supplement_dispatch(
            plan.authorization_id,
            plan.dispatch_nonce,
            &plan.runtime_binding_json,
        )
        .await
        .map_err(debug_error)?
        .ok_or_else(|| "missing single dispatch permit".into())
}

fn accepted(plan: &DreaminaVideoSupplementPlan, remote: &str) -> Accepted {
    Accepted {
        authorization_id: plan.authorization_id,
        dispatch_nonce: plan.dispatch_nonce,
        remote_operation_id: remote.into(),
        submit_stdout_sha256: sha256(remote.as_bytes()),
    }
}

#[tokio::test]
async fn recovered_acceptance_is_idempotent_and_retains_original_hold() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&db.pool, "seedance2.0fast_vip").await?;
        let service = store(&db.pool);
        let plan = claimed(&db.pool, &fixture).await?;
        let receipt = accepted(&plan, "offline-recovered-accepted");
        service
            .record_dreamina_video_supplement_recovered_acceptance(&receipt)
            .await
            .map_err(debug_error)?;
        // Both entry points preserve the exact same durable accepted identity.
        service
            .record_dreamina_video_supplement_accepted(&receipt)
            .await
            .map_err(debug_error)?;
        service
            .record_dreamina_video_supplement_recovered_acceptance(&receipt)
            .await
            .map_err(debug_error)?;
        let bound = service
            .load_dreamina_video_supplement(plan.authorization_id)
            .await
            .map_err(debug_error)?;
        require(
            bound.state == "accepted"
                && bound.remote_operation_id.as_deref()
                    == Some(receipt.remote_operation_id.as_str())
                && bound.submit_stdout_sha256.as_deref()
                    == Some(receipt.submit_stdout_sha256.as_str()),
            "recovered acceptance changed its identity",
        )?;
        let mut changed = receipt.clone();
        changed.dispatch_nonce = Uuid::new_v4();
        require(
            service
                .record_dreamina_video_supplement_recovered_acceptance(&changed)
                .await
                .is_err(),
            "wrong nonce recovered an acceptance",
        )?;
        changed = receipt.clone();
        changed.remote_operation_id = "offline-different-operation".into();
        require(
            service
                .record_dreamina_video_supplement_recovered_acceptance(&changed)
                .await
                .is_err(),
            "recovery replaced an accepted remote",
        )?;
        changed = receipt;
        changed.submit_stdout_sha256 = "b".repeat(64);
        require(
            service
                .record_dreamina_video_supplement_recovered_acceptance(&changed)
                .await
                .is_err(),
            "recovery replaced an accepted receipt digest",
        )?;
        assert_pending(&db.pool, plan.job_id).await
    }
    .await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn recovered_and_ordinary_acceptance_cannot_bind_one_remote_to_two_originals() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let first = fixture(&db.pool, "seedance2.0fast_vip").await?;
        // Admit the second fictional original through the normal audited credit
        // limit control, rather than bypassing its immutable evidence guard.
        PostgresBillingAccountControlService::new(db.pool.clone())
            .update_limit(
                "executord-process-smoke",
                "USD",
                BillingControlActor {
                    user_id: Uuid::new_v4(),
                    session_id: Uuid::new_v4(),
                },
                UpdateBillingAccountLimitRequest {
                    credit_limit_micros: "2000000".into(),
                    expected_control_version: "1".into(),
                    reason: "Offline two-original acceptance race fixture".into(),
                },
            )
            .await
            .map_err(debug_error)?;
        let second = fixture(&db.pool, "seedance2.0mini").await?;
        let first_plan = claimed(&db.pool, &first).await?;
        let second_plan = claimed(&db.pool, &second).await?;
        let first_receipt = accepted(&first_plan, "offline-shared-remote");
        let second_receipt = accepted(&second_plan, "offline-shared-remote");
        let service = store(&db.pool);
        let (recovered, ordinary) = tokio::join!(
            service.record_dreamina_video_supplement_recovered_acceptance(&first_receipt),
            service.record_dreamina_video_supplement_accepted(&second_receipt),
        );
        require(
            usize::from(recovered.is_ok()) + usize::from(ordinary.is_ok()) == 1,
            "two originals acquired the same recovered remote operation",
        )?;
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM dreamina_operator_video_supplement_authorizations WHERE provider_account_id=$1 AND remote_operation_id=$2",
        )
        .bind(first_plan.provider_account_id)
        .bind("offline-shared-remote")
        .fetch_one(&db.pool)
        .await
        .map_err(debug_error)?;
        require(count == 1, "duplicate accepted remote leaked")?;
        assert_pending(&db.pool, first_plan.job_id).await?;
        assert_pending(&db.pool, second_plan.job_id).await
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn recovered_acceptance_rejects_authorized_and_superseded_attempts() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&db.pool, "seedance2.0fast_vip").await?;
        let service = store(&db.pool);
        let parent = service
            .authorize_dreamina_video_supplement(&fixture.request)
            .await
            .map_err(debug_error)?;
        let receipt = accepted(&parent, "offline-old-attempt-remote");
        require(
            service
                .record_dreamina_video_supplement_recovered_acceptance(&receipt)
                .await
                .is_err(),
            "recovery accepted a never-dispatched authority",
        )?;
        service
            .claim_dreamina_video_supplement_dispatch(
                parent.authorization_id,
                parent.dispatch_nonce,
                &parent.runtime_binding_json,
            )
            .await
            .map_err(debug_error)?
            .ok_or("missing parent permit")?;
        service
            .record_dreamina_video_supplement_uncertain(&Uncertain {
                authorization_id: parent.authorization_id,
                dispatch_nonce: parent.dispatch_nonce,
                error_code: "offline_empty_stdout".into(),
            })
            .await
            .map_err(debug_error)?;
        let mut successor = fixture.request.clone();
        successor.operator_reference = "offline-new-separate-successor-authorization".into();
        let child = service
            .authorize_dreamina_video_supplement_successor(parent.authorization_id, &successor)
            .await
            .map_err(debug_error)?;
        require(
            service
                .record_dreamina_video_supplement_recovered_acceptance(&receipt)
                .await
                .is_err(),
            "recovery revived a superseded parent",
        )?;
        let child = service
            .load_dreamina_video_supplement(child.authorization_id)
            .await
            .map_err(debug_error)?;
        require(
            child.state == "authorized" && child.remote_operation_id.is_none(),
            "old acceptance changed successor",
        )?;
        assert_pending(&db.pool, parent.job_id).await
    }
    .await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn recovered_query_receipt_publishes_original_video_and_charge_once() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&db.pool, "seedance2.0fast_vip").await?;
        let service = store(&db.pool);
        let plan = claimed(&db.pool, &fixture).await?;
        let (mut accepted, publish) = evidence_with_video(&fixture, &plan, video_bytes())?;
        let query = fs::read(fixture.root.join("reviewed-query.json")).map_err(debug_error)?;
        fs::remove_file(fixture.root.join("submit-stdout.json")).map_err(debug_error)?;
        accepted.submit_stdout_sha256 = sha256(&query);
        let marker = serde_json::to_vec(&json!({
            "schema_version": 1,
            "evidence_mode": "native_log_acceptance_query_v1",
            "authorization_id": plan.authorization_id,
            "dispatch_nonce": plan.dispatch_nonce,
            "command_hash": plan.command_hash,
            "quote_hash": plan.quote_hash,
            "remote_operation_id": accepted.remote_operation_id,
            "query_stdout_sha256": sha256(&query),
            "reviewed_recovery_sha256": sha256(b"offline-reviewed-native-log-and-query"),
        })).map_err(debug_error)?;
        write(&fixture.root, "acceptance-query.json", &query)?;
        write(&fixture.root, "acceptance-recovery.json", &marker)?;
        let mut review: serde_json::Value = serde_json::from_slice(
            &fs::read(fixture.root.join("review.json")).map_err(debug_error)?,
        ).map_err(debug_error)?;
        review["submit_stdout_sha256"] = json!(sha256(&query));
        review["acceptance_recovery_sha256"] = json!("a".repeat(64));
        write(&fixture.root, "review.json", &serde_json::to_vec(&review).map_err(debug_error)?)?;
        service.record_dreamina_video_supplement_recovered_acceptance(&accepted).await.map_err(debug_error)?;
        require(
            service.publish_dreamina_video_supplement(&fixture.root, &publish).await.is_err(),
            "recovery published with a replaced acceptance marker",
        )?;
        assert_pending(&db.pool, plan.job_id).await?;
        review["acceptance_recovery_sha256"] = json!(sha256(&marker));
        write(&fixture.root, "review.json", &serde_json::to_vec(&review).map_err(debug_error)?)?;
        let first = service.publish_dreamina_video_supplement(&fixture.root, &publish).await.map_err(debug_error)?;
        let repeated = service.publish_dreamina_video_supplement(&fixture.root, &publish).await.map_err(debug_error)?;
        require(
            first == repeated && first.captured_micros == 1_200_000
                && first.released_micros == 0 && first.committed_units == 10
                && first.released_units == 0,
            "recovered publication changed the original hold or settled twice",
        )?;
        require(!fixture.root.join("submit-stdout.json").exists(), "recovery manufactured a submit stdout")?;
        let counts: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM dreamina_operator_video_supplement_resolutions WHERE job_id=$1), (SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1), (SELECT count(*) FROM customer_rated_usage WHERE job_id=$1), (SELECT count(*) FROM provider_submissions WHERE job_id=$1)",
        ).bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(counts == (1, 1, 1, 1), "recovered result duplicated a resolution, charge or submission")?;
        let original: (String, String) = sqlx::query_as(
            "SELECT submission.state, receipt.outcome FROM provider_submissions submission JOIN provider_receipts receipt USING(submission_id) WHERE submission.job_id=$1",
        ).bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(original == ("uncertain".into(), "uncertain".into()), "recovery rewrote original uncertain evidence")?;
        let artifact = service.load_video_artifact("executord-process-smoke", first.artifact_id).await.map_err(debug_error)?.ok_or("missing recovered video bytes")?;
        require(artifact.bytes == video_bytes(), "recovered publication changed video bytes")
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}
