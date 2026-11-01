use super::*;
use gpt_image_2_gateway::capacity_risk_waivers::{
    CapacityRiskWaiverManifest, CapacityRiskWaiverOriginal, review_or_apply_capacity_risk_waiver,
};
use gpt_image_2_gateway::{
    ProviderCapacityEvidence, ProviderCapacityEvidenceOutcome, ProviderCapacityReconciliationStore,
};

async fn manifest(pool: &PgPool, fixtures: &[Fixture]) -> TestResult<(Vec<u8>, String)> {
    let now = database_now(pool).await?;
    let mut originals = Vec::new();
    for fixture in fixtures {
        let id = fixture.request.executor_execution_id;
        let digest = sqlx::query_scalar("SELECT executor_capacity_original_record_sha256($1)")
            .bind(id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
        originals.push(CapacityRiskWaiverOriginal {
            allocation_id: id,
            executor_execution_id: id,
            submission_id: fixture.request.submission_id,
            original_record_sha256: digest,
        });
    }
    let value = CapacityRiskWaiverManifest {
        schema_version: 1,
        authorization_id: Uuid::new_v4(),
        provider_account_id: DREAMINA_ACCOUNT_ID,
        resource_policy_id: DREAMINA_POLICY_ID,
        resource_policy_revision: 1,
        operator_reference: "offline:explicit-risk-acceptance".into(),
        actor: "offline-root-operator".into(),
        risk_acknowledgement: "unknown_original_may_still_occupy_upstream_capacity".into(),
        observed_before_ms: now,
        expires_at_ms: now + 60_000,
        originals,
    };
    let bytes = serde_json::to_vec(&value).map_err(debug_error)?;
    let hash = sha256(&bytes);
    Ok((bytes, hash))
}

async fn prepare_new(pool: &PgPool) -> TestResult {
    let work = seed_supplement_video(pool, "seedance2.0fast_vip", 0).await?;
    seed_v4_customer_quote_with_funding(pool,&work,V4CustomerQuoteIdentity{provider_model_id:"seedance2.0fast_vip",public_model_id:"seedance2.0fast_vip",dimensions:json!({"duration":"10","ratio":"16:9","resolution":"720p","processing_mode":"synchronous","workflow":"multimodal2video","input_image_count":"1","input_video_count":"0","input_audio_count":"0"}),..V4CustomerQuoteIdentity::dreamina_video()},V4CustomerQuoteBasis{metric:"video_requested_second",unit:"second",unit_size:1,unit_price_micros:120_000,quantity_source:"request_derived",confidence:"exact",max_quantity:10,max_amount_micros:1_200_000},0).await?;
    PostgresExecutorSubmissionStore::new(pool.clone())
        .prepare_and_handoff(&work, DREAMINA_VIDEO_PROFILE_ID)
        .await
        .map_err(debug_error)?;
    seed_terminal_quota(pool, &work).await
}

fn scope() -> ExecutorClaimScope {
    ExecutorClaimScope {
        execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
        provider_id: DREAMINA_PROVIDER_ID.into(),
        command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
        adapter_revision: DREAMINA_ADAPTER_REVISION.into(),
    }
}

#[tokio::test]
async fn capacity_risk_waiver_preserves_frozen_15_and_admits_only_40_effective() -> TestResult {
    let Some(db) = TestDatabase::new_with_capacity(40).await? else {
        return Ok(());
    };
    let result = async {
        // Seed sufficient fictional balance before any hold; do not alter
        // account limits or financial guards during the capacity exercise.
        let now = database_now(&db.pool).await?;
        sqlx::query("INSERT INTO billing_accounts (tenant_id,currency,credit_limit_micros,held_micros,captured_micros,created_at_ms,updated_at_ms) VALUES ('executord-process-smoke','USD',100000000,0,0,$1,$1)")
            .bind(now).execute(&db.pool).await.map_err(debug_error)?;
        let mut fixtures = Vec::new();
        for _ in 0..15 {
            let item = fixture(&db.pool, "seedance2.0fast_vip").await?;
            store(&db.pool).authorize_dreamina_video_supplement(&item.request).await.map_err(debug_error)?;
            fixtures.push(item);
        }
        let (bytes, hash) = manifest(&db.pool, &fixtures).await?;
        let dry = review_or_apply_capacity_risk_waiver(&db.pool, &bytes, &hash, false).await.map_err(debug_error)?;
        require(!dry.applied && dry.raw_allocated_count==15 && dry.effective_allocated_count==0, "incorrect prospective dry run")?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM executor_capacity_risk_waivers").fetch_one(&db.pool).await.map_err(debug_error)?;
        require(count==0, "dry run persisted waivers")?;
        let (a,b) = tokio::join!(review_or_apply_capacity_risk_waiver(&db.pool,&bytes,&hash,true),review_or_apply_capacity_risk_waiver(&db.pool,&bytes,&hash,true));
        let a=a.map_err(debug_error)?;let b=b.map_err(debug_error)?;
        require(a.already_applied+b.already_applied==15,"concurrent apply duplicated or omitted waivers")?;
        for item in &fixtures {
            let unchanged: bool = sqlx::query_scalar("SELECT original_evidence_json = dreamina_video_supplement_original_snapshot(job_id) FROM dreamina_operator_video_supplement_authorizations WHERE job_id=$1")
                .bind(item.request.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
            require(unchanged,"waiver changed frozen original evidence")?;
            assert_pending(&db.pool,item.request.job_id).await?;
        }
        for _ in 0..41 { prepare_new(&db.pool).await?; }
        let mut handles = Vec::new();
        for n in 0..41 {
            let pool = db.pool.clone();
            handles.push(tokio::spawn(async move {
                PostgresExecutorSubmissionStore::new(pool).claim_prepared(&scope(),&format!("waiver-claim-{n}"),60_000).await
            }));
        }
        let mut acquired = HashSet::new();
        for handle in handles {
            if let Some(lease)=handle.await.map_err(debug_error)?.map_err(debug_error)? {
                require(acquired.insert(lease.executor_execution_id),"duplicate executor claim")?;
            }
        }
        require(acquired.len()==40,format!("admitted {} instead of exactly 40 effective slots",acquired.len()))?;
        let counters:(i32,i32,i32,i64)=sqlx::query_as("SELECT allocated_count, executor_waived_held_count(resource_policy_id,revision),executor_effective_allocated_count(resource_policy_id,revision),(SELECT count(*) FROM executor_capacity_allocations WHERE state='held' AND resource_policy_id=$1) FROM executor_resource_policies WHERE resource_policy_id=$1 AND revision=1")
            .bind(DREAMINA_POLICY_ID).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(counters==(55,15,40,55),format!("incorrect raw/effective counters: {counters:?}"))?;
        require(PostgresExecutorSubmissionStore::new(db.pool.clone()).claim_prepared(&scope(),"must-not-overbook",60_000).await.map_err(debug_error)?.is_none(),"41st effective claim admitted")?;
        Ok(())
    }.await;
    combine(result, db.cleanup().await)
}

#[tokio::test]
async fn capacity_risk_waiver_rejects_scope_and_digest_changes_and_is_append_only() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixtures = vec![fixture(&db.pool, "seedance2.0fast_vip").await?];
        let (bytes, hash) = manifest(&db.pool, &fixtures).await?;
        let baseline: CapacityRiskWaiverManifest =
            serde_json::from_slice(&bytes).map_err(debug_error)?;
        for mutation in 0..7 {
            let mut bad = baseline.clone();
            match mutation {
                0 => bad.provider_account_id = Uuid::new_v4(),
                1 => bad.resource_policy_revision += 1,
                2 => bad.originals[0].original_record_sha256 = "0".repeat(64),
                3 => bad.originals[0].submission_id = Uuid::new_v4(),
                4 => bad.expires_at_ms = 1,
                5 => bad.risk_acknowledgement = "confirmed_no_effect".into(),
                _ => bad.originals.push(bad.originals[0].clone()),
            }
            let altered = serde_json::to_vec(&bad).map_err(debug_error)?;
            require(
                review_or_apply_capacity_risk_waiver(&db.pool, &altered, &sha256(&altered), true)
                    .await
                    .is_err(),
                format!("scope mutation {mutation} accepted"),
            )?;
        }
        require(
            review_or_apply_capacity_risk_waiver(&db.pool, &bytes, &"0".repeat(64), true)
                .await
                .is_err(),
            "unreviewed manifest accepted",
        )?;
        review_or_apply_capacity_risk_waiver(&db.pool, &bytes, &hash, true)
            .await
            .map_err(debug_error)?;
        for statement in [
            "UPDATE executor_capacity_risk_waivers SET actor='changed'",
            "DELETE FROM executor_capacity_risk_waivers",
            "TRUNCATE executor_capacity_risk_waivers",
        ] {
            require(
                sqlx::query(statement).execute(&db.pool).await.is_err(),
                "waiver evidence mutation accepted",
            )?;
        }
        let mut bad = baseline;
        bad.authorization_id = Uuid::new_v4();
        let altered = serde_json::to_vec(&bad).map_err(debug_error)?;
        require(
            review_or_apply_capacity_risk_waiver(&db.pool, &altered, &sha256(&altered), true)
                .await
                .is_err(),
            "allocation waived under a second authorization",
        )?;
        assert_pending(&db.pool, fixtures[0].request.job_id).await?;
        Ok(())
    }
    .await;
    combine(result, db.cleanup().await)
}

#[tokio::test]
async fn capacity_risk_waiver_and_proven_release_never_double_free() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        let fixtures=vec![fixture(&db.pool,"seedance2.0fast_vip").await?];
        let tasks=PostgresProviderTaskStore::new(db.pool.clone());
        let lease=tasks.claim_due_capacity_reconciliation(&ProviderTaskClaimScope {
            execution_profile_id:DREAMINA_VIDEO_PROFILE_ID,provider_id:DREAMINA_PROVIDER_ID.into(),provider_account_id:DREAMINA_ACCOUNT_ID,
        },"offline-capacity-proof","offline-proof-command",60_000).await.map_err(debug_error)?.ok_or("missing capacity lease")?;
        let (bytes,hash)=manifest(&db.pool,&fixtures).await?;
        // This fixture explicitly supplies a trusted synthetic proof. No age,
        // empty output, or waiver itself is converted into no-effect evidence.
        let proof=ProviderCapacityEvidence { event_identity:"offline-confirmed-no-effect".into(),outcome:ProviderCapacityEvidenceOutcome::ConfirmedNoEffect };
        let (waiver,released)=tokio::join!(
            review_or_apply_capacity_risk_waiver(&db.pool,&bytes,&hash,true),
            tasks.record_capacity_evidence(&lease,&proof)
        );
        released.map_err(debug_error)?;
        // If release won, the now-released allocation is correctly ineligible.
        if let Err(error)=waiver {
            require(error.status_code()==axum::http::StatusCode::CONFLICT,
                format!("unexpected waiver/release race failure: {error:?}"))?;
        }
        let counters:(i32,i32,i32)=sqlx::query_as("SELECT allocated_count,executor_waived_held_count(resource_policy_id,revision),executor_effective_allocated_count(resource_policy_id,revision) FROM executor_resource_policies WHERE resource_policy_id=$1 AND revision=1")
            .bind(DREAMINA_POLICY_ID).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(counters==(0,0,0),format!("release plus waiver double-freed capacity: {counters:?}"))?;
        assert_pending(&db.pool,fixtures[0].request.job_id).await?;
        Ok(())
    }.await;
    combine(result, db.cleanup().await)
}
