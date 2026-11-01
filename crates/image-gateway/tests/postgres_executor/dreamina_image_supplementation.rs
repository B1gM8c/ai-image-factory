use super::*;
use gpt_image_2_gateway::retention::{
    ArtifactRetentionStore, PostgresArtifactRetentionStore, reconcile_artifact_retention,
};
use gpt_image_2_gateway::{
    DreaminaImageSupplementAcceptedRequest as Accepted,
    DreaminaImageSupplementAuthorizationRequest as Authorize, DreaminaImageSupplementPlan,
    DreaminaImageSupplementPublishRequest as Publish,
    DreaminaImageSupplementUncertainRequest as Uncertain, settlement::ImageResultStatus,
};
use image_provider_dreamina_cli::parse_submit_command;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::path::{Path, PathBuf};
use std::str::FromStr;

const PRICE: i64 = 30_000;
struct Fixture {
    request: Authorize,
    provider_command_sha256: String,
    root: PathBuf,
    _directory: tempfile::TempDir,
}
fn store(pool: &PgPool) -> PostgresExecutionSettlementStore {
    PostgresExecutionSettlementStore::new(
        pool.clone(),
        Arc::new(InMemoryArtifactBlobStore::default()),
    )
}
async fn fixture(pool: &PgPool) -> TestResult<Fixture> {
    let work = seed_dreamina_generation_lease_with_model(
        pool,
        "image-supplement-original",
        DREAMINA_IMAGES_API_PROFILE,
        1,
        "4.5",
    )
    .await?;
    seed_v4_customer_quote_with_funding(pool, &work,
        V4CustomerQuoteIdentity {
            dimensions: json!({"resolution_type":"2k","ratio":"1:1","processing_mode":"synchronous"}),
            provider_model_id: "4.5", ..V4CustomerQuoteIdentity::dreamina()
        },
        V4CustomerQuoteBasis { metric:"image_output",unit:"image",unit_size:1,unit_price_micros:PRICE,quantity_source:"request_derived",confidence:"exact",max_quantity:1,max_amount_micros:PRICE },0).await?;
    let executor = PostgresExecutorSubmissionStore::new(pool.clone());
    executor
        .prepare_and_handoff(&work, DREAMINA_PROFILE_ID)
        .await
        .map_err(debug_error)?;
    seed_terminal_quota(pool, &work).await?;
    let scope = ExecutorClaimScope {
        execution_profile_id: DREAMINA_PROFILE_ID,
        provider_id: DREAMINA_PROVIDER_ID.into(),
        command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
        adapter_revision: DREAMINA_ADAPTER_REVISION.into(),
    };
    let lease = executor
        .claim_prepared(&scope, "supplement-original", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or("missing original lease")?;
    executor.start(&lease).await.map_err(debug_error)?;
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, 1).map_err(debug_error)?,
        DreaminaSubmitPayloadV1::new(
            &lease.command_hash,
            parse_submit_command(&serde_json::to_vec(&work.command_json).map_err(debug_error)?)
                .map_err(debug_error)?,
        )
        .map_err(debug_error)?,
    )
    .map_err(debug_error)?;
    let reservation = RemoteTaskSubmitReservation::new(
        &lease,
        format!("original-{}", lease.submission_id.simple()),
        command.output(),
        command.identity(),
        500,
    );
    let tasks = PostgresProviderTaskStore::new(pool.clone());
    tasks
        .reserve_submit(&reservation)
        .await
        .map_err(debug_error)?;
    let provider_command_sha256: String = sqlx::query_scalar(
        "SELECT provider_command_sha256 FROM provider_remote_submit_intents WHERE submission_id=$1",
    )
    .bind(lease.submission_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        provider_command_sha256 == hex::encode(command.canonical_sha256())
            && provider_command_sha256 != lease.command_hash,
        "fixture must retain distinct source and provider command identities",
    )?;
    tasks
        .start_submit(&reservation)
        .await
        .map_err(debug_error)?;
    tokio::time::sleep(Duration::from_millis(550)).await;
    tasks
        .resolve_due_submit_deadline(&ProviderTaskClaimScope {
            provider_id: DREAMINA_PROVIDER_ID.into(),
            provider_account_id: DREAMINA_ACCOUNT_ID,
            execution_profile_id: DREAMINA_PROFILE_ID,
        })
        .await
        .map_err(debug_error)?
        .ok_or("original deadline not due")?;
    let terminal = PostgresExecutorTerminalStore::new(pool.clone());
    let terminal_lease = terminal
        .claim_terminal("supplement-reducer", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or("missing terminal reduction")?;
    terminal
        .complete_terminal(&terminal_lease, None)
        .await
        .map_err(debug_error)?;
    let quote: String =
        sqlx::query_scalar("SELECT quote_hash FROM customer_price_quotes WHERE job_id=$1")
            .bind(work.job_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let now = database_now(pool).await?;
    sqlx::query("INSERT INTO provider_account_environments (provider_account_id,provider_id,environment_kind,environment_ref,upstream_identity_sha256,display_name,state,created_at_ms,updated_at_ms) VALUES ($1,'dreamina-cli','dreamina_home_v1','/offline/supplement-home',$2,'Supplement fixture','active',$3,$3) ON CONFLICT (provider_account_id) DO NOTHING").bind(DREAMINA_ACCOUNT_ID).bind("d".repeat(64)).bind(now).execute(pool).await.map_err(debug_error)?;
    let credentials:(String,i64,String)=sqlx::query_as("SELECT credential_ref,credential_revision,credential_auth_sha256 FROM provider_accounts WHERE provider_account_id=$1").bind(DREAMINA_ACCOUNT_ID).fetch_one(pool).await.map_err(debug_error)?;
    let runtime = json!({"execution_profile_id":DREAMINA_PROFILE_ID,"provider_account_id":DREAMINA_ACCOUNT_ID,"upstream_identity_sha256":"d".repeat(64),"credential_revision":credentials.1,"credential_ref_sha256":sha256(credentials.0.as_bytes()),"credential_auth_sha256":credentials.2,"account_home_sha256":sha256(b"/offline/supplement-home"),"cli_path":"/offline/dreamina","cli_sha256":"e".repeat(64),"runner_path":"/offline/runner","runner_sha256":"f".repeat(64)});
    let directory = tempfile::tempdir().map_err(debug_error)?;
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
        .map_err(debug_error)?;
    let root = fs::canonicalize(directory.path()).map_err(debug_error)?;
    Ok(Fixture {
        request: Authorize {
            authorization_id: Uuid::new_v4(),
            acknowledged_unknown_original_effect: true,
            runtime_binding_json: runtime,
            job_id: work.job_id,
            submission_id: lease.submission_id,
            executor_execution_id: lease.executor_execution_id,
            expected_command_sha256: lease.command_hash,
            expected_quote_hash: quote,
            operator_reference: "offline-regression:one-manual-supplement".into(),
        },
        provider_command_sha256,
        root,
        _directory: directory,
    })
}
fn write(root: &Path, name: &str, bytes: &[u8]) -> TestResult {
    fs::write(root.join(name), bytes).map_err(debug_error)?;
    fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o600)).map_err(debug_error)
}
fn png(index: usize) -> Vec<u8> {
    let mut bytes = std::io::Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(RgbaImage::from_pixel(
        2,
        2,
        Rgba([index as u8 + 10, 20, 30, 255]),
    ))
    .write_to(&mut bytes, ImageFormat::Png)
    .expect("fixture PNG");
    bytes.into_inner()
}
fn evidence(
    fixture: &Fixture,
    plan: &DreaminaImageSupplementPlan,
    count: usize,
) -> TestResult<(Accepted, Publish)> {
    let remote = format!("new-image-{}", plan.authorization_id.simple());
    let dir = fixture.root.join("fresh-cli-download");
    let submit = serde_json::to_vec(&json!({"submit_id":remote,"gen_status":"querying"}))
        .map_err(debug_error)?;
    let mut images = Vec::new();
    let mut downloaded = Vec::new();
    for index in 0..count {
        let filename = format!("{remote}_image_{}.png", index + 1);
        let evidence_filename = if index == 0 {
            "selected.png".into()
        } else {
            format!("image-{index}.png")
        };
        let bytes = png(index);
        write(&fixture.root, &evidence_filename, &bytes)?;
        images.push(json!({"item_id":format!("item-{index}"),"width":2,"height":2,"path":dir.join(&filename)}));
        downloaded.push(json!({"source_index":index,"filename":filename,"evidence_filename":evidence_filename,"sha256":sha256(&bytes),"byte_size":bytes.len()}));
    }
    let query=serde_json::to_vec(&json!({"submit_id":remote,"gen_status":"success","result_json":{"images":images,"videos":[]}})).map_err(debug_error)?;
    let audit=serde_json::to_vec(&json!({"schema_version":1,"authorization_id":plan.authorization_id,"dispatch_nonce":plan.dispatch_nonce,"job_id":plan.job_id,"submission_id":plan.submission_id,"executor_execution_id":plan.executor_execution_id,"remote_operation_id":remote,"argv":["query_result","--submit_id",remote,"--download_dir",dir],"stdout_sha256":sha256(&query),"exit_code":0,"executed_at_ms":1,"downloaded_images":downloaded})).map_err(debug_error)?;
    let review=serde_json::to_vec(&json!({"schema_version":1,"authority":"dreamina-image-manual-supplement/v1","stdout_sha256":sha256(&query),"execution_audit_sha256":sha256(&audit),"quote_hash":plan.quote_hash,"provider_command_sha256":plan.provider_command_sha256,"submit_stdout_sha256":sha256(&submit),"authorization_id":plan.authorization_id,"dispatch_nonce":plan.dispatch_nonce})).map_err(debug_error)?;
    for (name, bytes) in [
        ("submit-stdout.json", submit.clone()),
        ("reviewed-query.json", query.clone()),
        ("execution-audit.json", audit),
        ("review.json", review),
    ] {
        write(&fixture.root, name, &bytes)?;
    }
    Ok((
        Accepted {
            authorization_id: plan.authorization_id,
            dispatch_nonce: plan.dispatch_nonce,
            remote_operation_id: remote,
            submit_stdout_sha256: sha256(&submit),
        },
        Publish {
            authorization_id: plan.authorization_id,
            dispatch_nonce: plan.dispatch_nonce,
            reviewed_stdout_sha256: sha256(&query),
        },
    ))
}
async fn assert_pending(pool: &PgPool, job: Uuid) -> TestResult {
    let status:(String,String,i64,i64,i32,i32)=sqlx::query_as("SELECT job.state,hold.state,hold.captured_micros,hold.released_micros,quota.committed_units,quota.released_units FROM jobs job JOIN customer_billing_holds hold USING(job_id) JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id WHERE job.job_id=$1").bind(job).fetch_one(pool).await.map_err(debug_error)?;
    require(
        status == ("uncertain".into(), "held".into(), 0, 0, 0, 0),
        format!("original funds/state changed: {status:?}"),
    )
}
async fn original_snapshot(pool: &PgPool, job: Uuid) -> TestResult<serde_json::Value> {
    sqlx::query_scalar("SELECT dreamina_image_supplement_original_snapshot($1)")
        .bind(job)
        .fetch_one(pool)
        .await
        .map_err(debug_error)
}
async fn claim(
    store: &PostgresExecutionSettlementStore,
    plan: &DreaminaImageSupplementPlan,
) -> TestResult {
    store
        .claim_dreamina_image_supplement_dispatch(
            plan.authorization_id,
            plan.dispatch_nonce,
            &plan.runtime_binding_json,
        )
        .await
        .map_err(debug_error)?
        .ok_or("missing one-shot permit")?;
    Ok(())
}

#[tokio::test]
async fn image_supplement_explicit_authorization_and_concurrent_claim_are_once_only() -> TestResult
{
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&db.pool).await?;
        let store = store(&db.pool);
        let before = original_snapshot(&db.pool, fixture.request.job_id).await?;
        for changed in 0..5 {
            let mut invalid = fixture.request.clone();
            match changed {
                0 => invalid.acknowledged_unknown_original_effect = false,
                1 => invalid.authorization_id = Uuid::nil(),
                2 => invalid.expected_quote_hash = "f".repeat(64),
                3 => invalid.expected_command_sha256 = "e".repeat(64),
                _ => invalid.expected_command_sha256 = fixture.provider_command_sha256.clone(),
            }
            require(
                store
                    .authorize_dreamina_image_supplement(&invalid)
                    .await
                    .is_err(),
                "invalid explicit authorization accepted",
            )?;
        }
        let plan = store
            .authorize_dreamina_image_supplement(&fixture.request)
            .await
            .map_err(debug_error)?;
        let replay = store
            .authorize_dreamina_image_supplement(&fixture.request)
            .await
            .map_err(debug_error)?;
        require(
            replay.authorization_id == plan.authorization_id
                && replay.dispatch_nonce == plan.dispatch_nonce,
            "authorization replay changed permit",
        )?;
        let mut second = fixture.request.clone();
        second.authorization_id = Uuid::new_v4();
        require(
            store
                .authorize_dreamina_image_supplement(&second)
                .await
                .is_err(),
            "second authorization for same unknown original accepted",
        )?;
        let (a, b) = tokio::join!(
            store.claim_dreamina_image_supplement_dispatch(
                plan.authorization_id,
                plan.dispatch_nonce,
                &plan.runtime_binding_json
            ),
            store.claim_dreamina_image_supplement_dispatch(
                plan.authorization_id,
                plan.dispatch_nonce,
                &plan.runtime_binding_json
            )
        );
        require(
            usize::from(a.map_err(debug_error)?.is_some())
                + usize::from(b.map_err(debug_error)?.is_some())
                == 1,
            "concurrent dispatch yielded duplicate permits",
        )?;
        // A zero-byte submit receipt is Unknown, never permission for another POST.
        store
            .record_dreamina_image_supplement_uncertain(&Uncertain {
                authorization_id: plan.authorization_id,
                dispatch_nonce: plan.dispatch_nonce,
                error_code: "dreamina_submit_receipt_invalid".into(),
            })
            .await
            .map_err(debug_error)?;
        require(
            store
                .claim_dreamina_image_supplement_dispatch(
                    plan.authorization_id,
                    plan.dispatch_nonce,
                    &plan.runtime_binding_json,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "empty receipt reopened dispatch",
        )?;
        require(
            store
                .load_dreamina_image_supplement(plan.authorization_id)
                .await
                .map_err(debug_error)?
                .state
                == "uncertain",
            "missing uncertain supplement state",
        )?;
        require(
            original_snapshot(&db.pool, plan.job_id).await? == before,
            "authorization/empty receipt rewrote original evidence",
        )?;
        assert_pending(&db.pool, plan.job_id).await
    }
    .await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn image_supplement_preserves_all_one_to_four_images_and_settles_original_once() -> TestResult
{
    for count in 1..=4 {
        let Some(db) = TestDatabase::new().await? else {
            return Ok(());
        };
        let result=async {
            let fixture=fixture(&db.pool).await?;
            let blobs=Arc::new(InMemoryArtifactBlobStore::default());
            let store=PostgresExecutionSettlementStore::new(db.pool.clone(),blobs.clone());
            let before=original_snapshot(&db.pool,fixture.request.job_id).await?;
            let plan=store.authorize_dreamina_image_supplement(&fixture.request).await.map_err(debug_error)?;
            require(plan.command_hash==fixture.request.expected_command_sha256 && plan.provider_command_sha256==fixture.provider_command_sha256 && plan.command_hash!=plan.provider_command_sha256,"authorization conflated source and provider command identities")?;
            require(plan.billable_units==1 && plan.command_json["generate_num"]==1 && plan.command_json["model_version"]=="4.5","supplement changed original requested quantity/model")?;
            claim(&store,&plan).await?;
            let(accepted,publish)=evidence(&fixture,&plan,count)?;
            store.record_dreamina_image_supplement_accepted(&accepted).await.map_err(debug_error)?;
            let(a,b)=tokio::join!(store.publish_dreamina_image_supplement(&fixture.root,&publish),store.publish_dreamina_image_supplement(&fixture.root,&publish));
            let first=a.map_err(debug_error)?;
            require(first==b.map_err(debug_error)? && first.returned_count==count as i32 && first.captured_micros==PRICE && first.released_micros==0 && first.committed_units==1 && first.released_units==0,"concurrent publish changed original single charge")?;
            require(original_snapshot(&db.pool,plan.job_id).await?==before,"publish rewrote old Unknown receipt or request/quote identity")?;
            let saved_hashes:(String,String)=sqlx::query_as("SELECT command_hash,provider_command_sha256 FROM dreamina_operator_image_supplement_resolutions WHERE authorization_id=$1").bind(plan.authorization_id).fetch_one(&db.pool).await.map_err(debug_error)?;
            require(saved_hashes==(fixture.request.expected_command_sha256.clone(),fixture.provider_command_sha256.clone()) && first.provider_command_sha256==fixture.provider_command_sha256,"publication rewrote or conflated command identities")?;
            let counts:(i64,i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM provider_submissions WHERE job_id=$1),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1),(SELECT count(*) FROM provider_usage_facts WHERE job_id=$1),(SELECT count(*) FROM artifacts WHERE job_id=$1),(SELECT count(*) FROM dreamina_operator_image_supplement_artifacts WHERE job_id=$1)").bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
            require(counts==(1,1,1,1,count as i64),format!("unexpected duplicate charge/artifacts: {counts:?}"))?;
            // A fresh reader uses only durable canonical/public projection state.
            let reads=PostgresExecutionSettlementStore::new(db.pool.clone(),blobs);
            reads.validate_complete_image_result_budget("executord-process-smoke","project-test",None,plan.job_id).await.map_err(debug_error)?;
            let result=reads.project_image_status("executord-process-smoke","project-test",None,plan.job_id).await.map_err(debug_error)?.ok_or("missing canonical result")?;
            require(matches!(result.status,ImageResultStatus::Succeeded{ref result,ref output_counts} if result.images.len()==1 && output_counts.requested==1 && output_counts.delivered==1),"canonical image result absent or billed candidate count")?;
            let extras=reads.project_additional_image_results("executord-process-smoke","project-test",None,plan.job_id).await.map_err(debug_error)?;
            require(extras.len()==count-1 && extras.iter().enumerate().all(|(i,item)|item.source_index==i+1 && item.bytes==png(i+1)),"additional images lost or reordered")?;
            for (tenant,project,actor) in [("wrong-tenant","project-test",None),("executord-process-smoke","wrong-project",None),("executord-process-smoke","project-test",Some(Uuid::new_v4()))] {
                require(reads.project_additional_image_results(tenant,project,actor,plan.job_id).await.map_err(debug_error)?.is_empty(),"supplement leaked additional image scope")?;
            }
            require(store.claim_dreamina_image_supplement_dispatch(plan.authorization_id,plan.dispatch_nonce,&plan.runtime_binding_json).await.map_err(debug_error)?.is_none(),"successful publication reopened dispatch")?;
            Ok(())
        }.await;
        let cleanup = db.cleanup().await;
        result.and(cleanup)?;
    }
    Ok(())
}

#[tokio::test]
async fn image_supplement_wrong_native_receipt_or_changed_bytes_cannot_publish() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&db.pool).await?;
        let store = store(&db.pool);
        let plan = store
            .authorize_dreamina_image_supplement(&fixture.request)
            .await
            .map_err(debug_error)?;
        claim(&store, &plan).await?;
        let (accepted, publish) = evidence(&fixture, &plan, 2)?;
        store
            .record_dreamina_image_supplement_accepted(&accepted)
            .await
            .map_err(debug_error)?;
        let mut review: serde_json::Value = serde_json::from_slice(
            &fs::read(fixture.root.join("review.json")).map_err(debug_error)?,
        )
        .map_err(debug_error)?;
        review["provider_command_sha256"] = json!(plan.command_hash);
        write(
            &fixture.root,
            "review.json",
            &serde_json::to_vec(&review).map_err(debug_error)?,
        )?;
        require(
            store
                .publish_dreamina_image_supplement(&fixture.root, &publish)
                .await
                .is_err(),
            "source command digest was accepted as provider command identity",
        )?;
        assert_pending(&db.pool, plan.job_id).await?;
        evidence(&fixture, &plan, 2)?;
        let mut wrong = accepted.clone();
        wrong.remote_operation_id = "different-native".into();
        require(
            store
                .record_dreamina_image_supplement_accepted(&wrong)
                .await
                .is_err(),
            "accepted native receipt was replaced",
        )?;
        let mut query: serde_json::Value = serde_json::from_slice(
            &fs::read(fixture.root.join("reviewed-query.json")).map_err(debug_error)?,
        )
        .map_err(debug_error)?;
        query["submit_id"] = json!("different-native");
        let bytes = serde_json::to_vec(&query).map_err(debug_error)?;
        // Rebind every review hash: native identity itself must still reject.
        let mut audit: serde_json::Value = serde_json::from_slice(
            &fs::read(fixture.root.join("execution-audit.json")).map_err(debug_error)?,
        )
        .map_err(debug_error)?;
        audit["stdout_sha256"] = json!(sha256(&bytes));
        let auditbytes = serde_json::to_vec(&audit).map_err(debug_error)?;
        let mut review: serde_json::Value = serde_json::from_slice(
            &fs::read(fixture.root.join("review.json")).map_err(debug_error)?,
        )
        .map_err(debug_error)?;
        review["stdout_sha256"] = json!(sha256(&bytes));
        review["execution_audit_sha256"] = json!(sha256(&auditbytes));
        write(&fixture.root, "reviewed-query.json", &bytes)?;
        write(&fixture.root, "execution-audit.json", &auditbytes)?;
        write(
            &fixture.root,
            "review.json",
            &serde_json::to_vec(&review).map_err(debug_error)?,
        )?;
        let altered = Publish {
            reviewed_stdout_sha256: sha256(&bytes),
            ..publish
        };
        require(
            store
                .publish_dreamina_image_supplement(&fixture.root, &altered)
                .await
                .is_err(),
            "query from different native ID delivered",
        )?;
        let (_, correct) = evidence(&fixture, &plan, 2)?;
        write(&fixture.root, "image-1.png", &png(9))?;
        require(
            store
                .publish_dreamina_image_supplement(&fixture.root, &correct)
                .await
                .is_err(),
            "changed additional image bytes delivered",
        )?;
        assert_pending(&db.pool, plan.job_id).await
    }
    .await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn image_supplement_freezes_old_receipt_result_failure_and_original_identity() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        let fixture=fixture(&db.pool).await?;let store=store(&db.pool);
        let plan=store.authorize_dreamina_image_supplement(&fixture.request).await.map_err(debug_error)?;
        for query in [
            "UPDATE provider_submissions SET updated_at_ms=updated_at_ms+1 WHERE job_id=$1",
            "UPDATE executor_executions SET updated_at_ms=updated_at_ms+1 WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)",
            "UPDATE provider_remote_submit_intents SET updated_at_ms=updated_at_ms+1 WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)",
            "UPDATE provider_receipts SET evidence=evidence || '{\"late_result\":true}'::jsonb WHERE job_id=$1",
            "UPDATE job_payloads SET command_json=command_json || '{\"prompt\":\"changed\"}'::jsonb WHERE job_id=$1",
            "UPDATE customer_billing_holds SET state='released',released_micros=held_micros,account_released_micros=account_held_micros WHERE job_id=$1",
            "UPDATE jobs SET state='failed' WHERE job_id=$1",
            "UPDATE jobs SET request_id='another-original' WHERE job_id=$1",
            "UPDATE customer_price_quotes SET request_dimensions_json='{}'::jsonb WHERE job_id=$1",
        ] {
            let mut tx=db.pool.begin().await.map_err(debug_error)?;
            let attempt=sqlx::query(query).bind(plan.job_id).execute(&mut *tx).await;
            let rejection=match attempt {
                Err(error)=>{tx.rollback().await.map_err(debug_error)?;error},
                Ok(changed)=>{
                    require(changed.rows_affected()==1,"mutation fixture matched no original row")?;
                    tx.commit().await.err().ok_or("old mutation escaped fence")?
                }
            };
            require(matches!(&rejection,sqlx::Error::Database(error) if error.code().as_deref()==Some("P0001")),format!("mutation was not rejected by a safety fence: {rejection:?}"))?;
        }
        let mut changed=plan.runtime_binding_json.clone(); changed["cli_sha256"]=json!("b".repeat(64));
        require(store.claim_dreamina_image_supplement_dispatch(plan.authorization_id,plan.dispatch_nonce,&changed).await.is_err(),"runtime pin mismatch dispatched")?;
        require(store.load_dreamina_image_supplement(plan.authorization_id).await.map_err(debug_error)?.state=="authorized","failed claim consumed authorization")?;
        assert_pending(&db.pool,plan.job_id).await
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn image_supplement_audit_failure_rolls_back_canonical_result_and_single_charge() -> TestResult
{
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        let fixture=fixture(&db.pool).await?;let store=store(&db.pool);
        let plan=store.authorize_dreamina_image_supplement(&fixture.request).await.map_err(debug_error)?;
        claim(&store,&plan).await?;let(accepted,publish)=evidence(&fixture,&plan,4)?;
        store.record_dreamina_image_supplement_accepted(&accepted).await.map_err(debug_error)?;
        sqlx::raw_sql("CREATE FUNCTION reject_image_supplement_audit() RETURNS TRIGGER AS $$ BEGIN IF NEW.semantic_key='job.operator_dreamina_image_supplement_success' THEN RAISE EXCEPTION 'offline injected audit failure'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql; CREATE TRIGGER reject_image_supplement_audit BEFORE INSERT ON outbox_events FOR EACH ROW EXECUTE FUNCTION reject_image_supplement_audit();").execute(&db.pool).await.map_err(debug_error)?;
        require(store.publish_dreamina_image_supplement(&fixture.root,&publish).await.is_err(),"audit failure published/charged")?;
        assert_pending(&db.pool,plan.job_id).await?;
        let counts:(i64,i64,i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM dreamina_operator_image_supplement_resolutions WHERE job_id=$1),(SELECT count(*) FROM dreamina_operator_image_supplement_artifacts WHERE job_id=$1),(SELECT count(*) FROM artifacts WHERE job_id=$1),(SELECT count(*) FROM job_response_projections WHERE job_id=$1),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1),(SELECT count(*) FROM customer_rated_usage WHERE job_id=$1)").bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(counts==(0,0,0,0,0,0),"audit failure left partial result/charge")?;
        sqlx::query("DROP TRIGGER reject_image_supplement_audit ON outbox_events").execute(&db.pool).await.map_err(debug_error)?;
        let published=store.publish_dreamina_image_supplement(&fixture.root,&publish).await.map_err(debug_error)?;
        require(published.captured_micros==PRICE && published.returned_count==4,"safe publication retry changed original bill or image set")
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

async fn clock(database: &mut TestDatabase) -> TestResult {
    sqlx::query("CREATE TABLE supplement_image_test_clock(offset_ms BIGINT NOT NULL)")
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
    sqlx::query("INSERT INTO supplement_image_test_clock VALUES(0)")
        .execute(&database.pool)
        .await
        .map_err(debug_error)?;
    for name in [
        "clock_timestamp",
        "statement_timestamp",
        "transaction_timestamp",
    ] {
        sqlx::query(AssertSqlSafe(format!("CREATE FUNCTION {name}() RETURNS TIMESTAMPTZ LANGUAGE SQL VOLATILE AS 'SELECT pg_catalog.{name}() + offset_ms * interval ''1 millisecond'' FROM supplement_image_test_clock'")))
            .execute(&database.pool).await.map_err(debug_error)?;
    }
    let options = PgConnectOptions::from_str(&env::var("TEST_DATABASE_URL").map_err(debug_error)?)
        .map_err(debug_error)?
        .options([("search_path", format!("{},pg_catalog", database.schema))]);
    let pool = PgPoolOptions::new()
        .max_connections(12)
        .connect_with(options)
        .await
        .map_err(debug_error)?;
    std::mem::replace(&mut database.pool, pool).close().await;
    Ok(())
}

async fn advance(pool: &PgPool, offset: i64) -> TestResult {
    sqlx::query("UPDATE supplement_image_test_clock SET offset_ms=$1")
        .bind(offset)
        .execute(pool)
        .await
        .map_err(debug_error)?;
    Ok(())
}

#[tokio::test]
async fn image_supplement_all_physical_images_follow_original_retention_without_rebilling()
-> TestResult {
    let Some(mut db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        clock(&mut db).await?;
        let fixture=fixture(&db.pool).await?;
        let files=tempfile::tempdir().map_err(debug_error)?;
        let blobs=Arc::new(FilesystemArtifactBlobStore::new(files.path()).map_err(debug_error)?);
        let store=PostgresExecutionSettlementStore::new(db.pool.clone(),blobs.clone());
        let plan=store.authorize_dreamina_image_supplement(&fixture.request).await.map_err(debug_error)?;
        claim(&store,&plan).await?;let(accepted,publish)=evidence(&fixture,&plan,4)?;
        store.record_dreamina_image_supplement_accepted(&accepted).await.map_err(debug_error)?;
        store.publish_dreamina_image_supplement(&fixture.root,&publish).await.map_err(debug_error)?;
        let objects:Vec<String>=sqlx::query_scalar("SELECT object_key FROM dreamina_operator_image_supplement_artifacts WHERE authorization_id=$1 ORDER BY source_index").bind(plan.authorization_id).fetch_all(&db.pool).await.map_err(debug_error)?;
        require(objects.len()==4 && objects.iter().all(|key|files.path().join(key).is_file()),"physical images not all stored")?;
        let gc=PostgresArtifactRetentionStore::new(db.pool.clone());
        require(gc.expire_due(5).await.map_err(debug_error)?==0 && gc.claim_due("before-ttl",1000).await.map_err(debug_error)?.is_none(),"original TTL shortened by supplement")?;
        advance(&db.pool,1_800_001).await?;
        require(store.project_additional_image_results("executord-process-smoke","project-test",None,plan.job_id).await.is_err(),"expired images remained readable")?;
        advance(&db.pool,1_900_000).await?;
        let cleaned=reconcile_artifact_retention(&gc,&blobs,"supplement-retention",1000,5).await.map_err(debug_error)?;
        require(cleaned.deleted==1 && cleaned.failed==0 && objects.iter().all(|key|!files.path().join(key).exists()),format!("retention did not remove primary plus all extras: {cleaned:?}"))?;
        let finances:(i64,i64,i64,i32)=sqlx::query_as("SELECT (SELECT count(*) FROM ledger_transactions WHERE source_job_id=hold.job_id),hold.captured_micros,hold.released_micros,quota.committed_units FROM customer_billing_holds hold JOIN jobs job USING(job_id) JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id WHERE hold.job_id=$1").bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(finances==(1,PRICE,0,1),"retention mutated original single settlement")
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn image_supplement_native_identity_cannot_attach_to_two_originals() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        // Seed room for two fictitious holds before either original is admitted.
        let now=database_now(&db.pool).await?;
        sqlx::query("INSERT INTO billing_accounts(tenant_id,currency,credit_limit_micros,held_micros,captured_micros,created_at_ms,updated_at_ms) VALUES ('executord-process-smoke','USD',$1,0,0,$2,$2)").bind(PRICE*2).bind(now).execute(&db.pool).await.map_err(debug_error)?;
        let original_a=fixture(&db.pool).await?;
        let original_b=fixture(&db.pool).await?;
        let store=store(&db.pool);
        let a=store.authorize_dreamina_image_supplement(&original_a.request).await.map_err(debug_error)?;
        let b=store.authorize_dreamina_image_supplement(&original_b.request).await.map_err(debug_error)?;
        claim(&store,&a).await?;claim(&store,&b).await?;
        let(accepted_a,_)=evidence(&original_a,&a,1)?;
        let accepted_b=Accepted{authorization_id:b.authorization_id,dispatch_nonce:b.dispatch_nonce,remote_operation_id:accepted_a.remote_operation_id.clone(),submit_stdout_sha256:accepted_a.submit_stdout_sha256.clone()};
        let(left,right)=tokio::join!(store.record_dreamina_image_supplement_accepted(&accepted_a),store.record_dreamina_image_supplement_accepted(&accepted_b));
        require(usize::from(left.is_ok())+usize::from(right.is_ok())==1,"same native ID attached to two original jobs")?;
        let bound:i64=sqlx::query_scalar("SELECT count(*) FROM dreamina_operator_image_supplement_authorizations WHERE remote_operation_id=$1").bind(&accepted_a.remote_operation_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(bound==1,"duplicate native authority persisted")?;
        assert_pending(&db.pool,a.job_id).await?;assert_pending(&db.pool,b.job_id).await
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn capacity_scheduler_image_frozen_original_is_classified_without_mutation() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&db.pool).await?;
        let plan = store(&db.pool).authorize_dreamina_image_supplement(&fixture.request).await.map_err(debug_error)?;
        let before: serde_json::Value = sqlx::query_scalar("SELECT dreamina_image_supplement_original_snapshot($1)").bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        let allocation_before:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(a) FROM executor_capacity_allocations a WHERE submission_id=$1").bind(fixture.request.submission_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        let tasks=PostgresProviderTaskStore::new(db.pool.clone());
        let scope=ProviderTaskClaimScope { provider_id:DREAMINA_PROVIDER_ID.into(), provider_account_id:DREAMINA_ACCOUNT_ID, execution_profile_id:DREAMINA_PROFILE_ID };
        require(tasks.classify_capacity_reconciliation(&scope).await.map_err(debug_error)?==1,"frozen original was not classified before claim")?;
        use gpt_image_2_gateway::ProviderCapacityReconciliationStore;
        require(tasks.claim_due_capacity_reconciliation(&scope,"must-not-claim-frozen","frozen-test",5000).await.map_err(debug_error)?.is_none(),"frozen original reached heartbeat/CLI lane")?;
        let disposition:(String,Option<String>)=sqlx::query_as("SELECT review_status,manual_reason FROM provider_capacity_reconciliations WHERE submission_id=$1").bind(fixture.request.submission_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(disposition==("manual_required".into(),Some("original_evidence_frozen".into())),"frozen original lost its manual reason")?;
        let after:serde_json::Value=sqlx::query_scalar("SELECT dreamina_image_supplement_original_snapshot($1)").bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        let allocation_after:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(a) FROM executor_capacity_allocations a WHERE submission_id=$1").bind(fixture.request.submission_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(before==after && allocation_before==allocation_after,"classification altered frozen original or allocation heartbeat")
    }.await;
    result.and(db.cleanup().await)
}
