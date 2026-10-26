use super::*;
use serde_json::Value;

async fn unknown_parent(
    pool: &PgPool,
    fixture: &Fixture,
) -> TestResult<DreaminaVideoSupplementPlan> {
    let service = store(pool);
    let parent = service
        .authorize_dreamina_video_supplement(&fixture.request)
        .await
        .map_err(debug_error)?;
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
            error_code: "offline_unknown_effect".into(),
        })
        .await
        .map_err(debug_error)?;
    Ok(parent)
}

async fn row(pool: &PgPool, id: Uuid) -> TestResult<Value> {
    sqlx::query_scalar(
        "SELECT to_jsonb(authority) FROM dreamina_operator_video_supplement_authorizations authority WHERE authorization_id=$1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)
}

#[tokio::test]
async fn supplement_successor_has_one_new_permit_and_one_original_charge() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&db.pool, "seedance2.0fast_vip").await?;
        let service = store(&db.pool);
        let parent = unknown_parent(&db.pool, &fixture).await?;
        let original_parent = row(&db.pool, parent.authorization_id).await?;
        require(service.authorize_dreamina_video_supplement_successor(parent.authorization_id, &fixture.request).await.is_err(), "same human reference reopened an attempt")?;
        let mut request = fixture.request.clone();
        request.operator_reference = "offline-new-human-instruction:successor".into();
        let (first, second) = tokio::join!(
            service.authorize_dreamina_video_supplement_successor(parent.authorization_id, &request),
            service.authorize_dreamina_video_supplement_successor(parent.authorization_id, &request),
        );
        let child = first.map_err(debug_error)?;
        require(child.authorization_id==second.map_err(debug_error)?.authorization_id
            && child.authorization_id!=parent.authorization_id && child.dispatch_nonce!=parent.dispatch_nonce,
            "successor concurrency changed identity or reused nonce")?;
        require(row(&db.pool,parent.authorization_id).await?==original_parent,"creating successor rewrote old authority")?;
        require(service.load_dreamina_video_supplement(parent.authorization_id).await.is_err(),"superseded parent remained active")?;
        require(service.claim_dreamina_video_supplement_dispatch(parent.authorization_id,parent.dispatch_nonce,&parent.runtime_binding_json).await.is_err(),"superseded parent acquired permit")?;
        let (old_accept, old_publish) = evidence(&fixture,&parent)?;
        require(service.record_dreamina_video_supplement_accepted(&old_accept).await.is_err(),"late parent acceptance succeeded")?;
        require(service.publish_dreamina_video_supplement(&fixture.root,&old_publish).await.is_err(),"late parent published")?;
        require(service.record_dreamina_video_supplement_uncertain(&Uncertain{authorization_id:parent.authorization_id,dispatch_nonce:parent.dispatch_nonce,error_code:"late".into()}).await.is_err(),"late parent changed uncertainty")?;
        require(sqlx::query("UPDATE dreamina_operator_video_supplement_authorizations SET error_code='late',updated_at_ms=updated_at_ms+1 WHERE authorization_id=$1").bind(parent.authorization_id).execute(&db.pool).await.is_err(),"direct parent mutation was allowed")?;
        assert_pending(&db.pool,child.job_id).await?;
        let (a,b) = tokio::join!(
            service.claim_dreamina_video_supplement_dispatch(child.authorization_id,child.dispatch_nonce,&child.runtime_binding_json),
            service.claim_dreamina_video_supplement_dispatch(child.authorization_id,child.dispatch_nonce,&child.runtime_binding_json),
        );
        require(usize::from(a.map_err(debug_error)?.is_some())+usize::from(b.map_err(debug_error)?.is_some())==1,"duplicate successor dispatch")?;
        // A newly constructed service represents a restarted operator process.
        require(store(&db.pool).claim_dreamina_video_supplement_dispatch(child.authorization_id,child.dispatch_nonce,&child.runtime_binding_json).await.map_err(debug_error)?.is_none(),"restart reopened successor permit")?;
        let (accepted,publish)=evidence(&fixture,&child)?;
        service.record_dreamina_video_supplement_accepted(&accepted).await.map_err(debug_error)?;
        require(matches!(service.video_status("executord-process-smoke",child.job_id).await.map_err(debug_error)?, Some(gpt_image_2_gateway::settlement::VideoResultStatus::Pending { .. })), "unchanged gateway query did not find accepted successor")?;
        let mut another=request.clone();another.operator_reference="offline-should-not-replace-accepted".into();
        require(service.authorize_dreamina_video_supplement_successor(child.authorization_id,&another).await.is_err(),"accepted child could be replaced")?;
        let (a,b)=tokio::join!(
            service.publish_dreamina_video_supplement(&fixture.root,&publish),
            service.publish_dreamina_video_supplement(&fixture.root,&publish),
        );
        let resolution=a.map_err(debug_error)?;
        require(resolution==b.map_err(debug_error)? && resolution.captured_micros==1_200_000 && resolution.released_micros==0,"successor did not settle original price once")?;
        let counts:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM dreamina_operator_video_supplement_authorizations WHERE job_id=$1),(SELECT count(*) FROM dreamina_operator_video_supplement_resolutions WHERE job_id=$1),(SELECT count(*) FROM customer_rated_usage WHERE job_id=$1),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1)").bind(child.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(counts==(2,1,1,1),"duplicate successor settlement or missing immutable history")?;
        require(row(&db.pool,parent.authorization_id).await?==original_parent,"publication rewrote parent evidence")?;
        require(service.authorize_dreamina_video_supplement_successor(child.authorization_id,&another).await.is_err(),"success child could be replaced")?;
        Ok(())
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn supplement_successor_rolls_back_and_fences_stale_repeatable_read() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        let fixture=fixture(&db.pool,"seedance2.0mini").await?;
        let service=store(&db.pool);
        let parent=unknown_parent(&db.pool,&fixture).await?;
        let before=row(&db.pool,parent.authorization_id).await?;
        let mut request=fixture.request.clone();request.operator_reference="offline-new-human-instruction:rollback".into();
        sqlx::query("CREATE FUNCTION successor_test_abort() RETURNS TRIGGER AS $$ BEGIN IF NEW.previous_authorization_id IS NOT NULL THEN RAISE EXCEPTION 'offline rollback'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql").execute(&db.pool).await.map_err(debug_error)?;
        sqlx::query("CREATE CONSTRAINT TRIGGER successor_test_abort AFTER INSERT ON dreamina_operator_video_supplement_authorizations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION successor_test_abort()").execute(&db.pool).await.map_err(debug_error)?;
        require(service.authorize_dreamina_video_supplement_successor(parent.authorization_id,&request).await.is_err(),"forced successor rollback was ignored")?;
        sqlx::query("DROP TRIGGER successor_test_abort ON dreamina_operator_video_supplement_authorizations").execute(&db.pool).await.map_err(debug_error)?;
        require(row(&db.pool,parent.authorization_id).await?==before,"rollback rewrote parent")?;
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM dreamina_operator_video_supplement_authorizations WHERE job_id=$1").bind(parent.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(count==1,"rolled back child leaked")?;
        assert_pending(&db.pool,parent.job_id).await?;
        let mut stale=db.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").execute(&mut *stale).await.map_err(debug_error)?;
        sqlx::query("SELECT state FROM dreamina_operator_video_supplement_authorizations WHERE authorization_id=$1").bind(parent.authorization_id).fetch_one(&mut *stale).await.map_err(debug_error)?;
        let child=service.authorize_dreamina_video_supplement_successor(parent.authorization_id,&request).await.map_err(debug_error)?;
        let late=sqlx::query("UPDATE dreamina_operator_video_supplement_authorizations SET error_code='late',updated_at_ms=updated_at_ms+1 WHERE authorization_id=$1").bind(parent.authorization_id).execute(&mut *stale).await;
        require(late.is_err(),"stale snapshot changed a superseded authority")?;
        stale.rollback().await.map_err(debug_error)?;
        require(row(&db.pool,parent.authorization_id).await?==before,"stale writer rewrote parent")?;
        let mut different=request.clone();different.operator_reference="offline-third-reference-for-same-parent".into();
        require(service.authorize_dreamina_video_supplement_successor(parent.authorization_id,&different).await.is_err(),"one parent accepted another successor/reference")?;
        let successor=service.authorize_dreamina_video_supplement_successor(parent.authorization_id,&request).await.map_err(debug_error)?;
        require(successor.authorization_id==child.authorization_id,"new human instruction replay was not idempotent")?;
        require(service.project_video_status("wrong-tenant","project-test",None,child.job_id).await.map_err(debug_error)?.is_none(),"successor escaped original scope")?;
        assert_pending(&db.pool,parent.job_id).await
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn supplement_successor_upgrade_preserves_schema153_authority_and_hold() -> TestResult {
    let Ok(url) = env::var("TEST_DATABASE_URL") else {
        return Ok(());
    };
    let schema = format!(
        "image_gateway_successor_upgrade_{}",
        Uuid::new_v4().simple()
    );
    let pool = connect_test_pool_with_search_path(&url, 24, &schema)
        .await
        .map_err(debug_error)?;
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .map_err(debug_error)?;
    require(
        database_name.contains("test"),
        "refusing upgrade test in non-test database",
    )?;
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
        .execute(&pool)
        .await
        .map_err(debug_error)?;
    let db = TestDatabase { schema, pool };
    let result=async {
        let all=sqlx::migrate!("./migrations");
        let old=sqlx::migrate::Migrator::with_migrations(all.iter().filter(|m|m.version<=153).cloned().collect());
        old.run(&db.pool).await.map_err(debug_error)?;
        seed_execution_profiles(&db.pool).await?;
        let fixture=fixture(&db.pool,"seedance2.0fast_vip").await?;
        let parent=Uuid::new_v4();let nonce=Uuid::new_v4();let now=database_now(&db.pool).await?;
        sqlx::query("INSERT INTO dreamina_operator_video_supplement_authorizations (authorization_id,job_id,submission_id,executor_execution_id,output_id,original_receipt_id,hold_id,provider_account_id,execution_profile_id,dispatch_nonce,command_hash,quote_hash,execution_binding_sha256,command_json,api_profile,billable_units,operator_reference,original_evidence_json,state,created_at_ms,updated_at_ms,runtime_binding_json) SELECT $1,j.job_id,s.submission_id,s.executor_execution_id,s.output_id,r.provider_receipt_id,h.hold_id,s.provider_account_id,s.execution_profile_id,$3,s.command_hash,q.quote_hash,i.execution_binding_sha256,p.command_json,a.api_profile,j.billable_units,$4,dreamina_video_supplement_original_snapshot(j.job_id),'authorized',$6,$6,$5 FROM jobs j JOIN provider_submissions s USING(job_id) JOIN executor_terminal_reductions r USING(submission_id) JOIN customer_billing_holds h USING(job_id) JOIN customer_price_quotes q ON q.quote_id=h.quote_id JOIN provider_remote_submit_intents i USING(submission_id) JOIN job_payloads p ON p.job_id=j.job_id JOIN admission_sessions a ON a.session_id=p.admission_session_id WHERE j.job_id=$2")
            .bind(parent).bind(fixture.request.job_id).bind(nonce).bind(&fixture.request.operator_reference).bind(&fixture.request.runtime_binding_json).bind(now).execute(&db.pool).await.map_err(debug_error)?;
        sqlx::query("UPDATE dreamina_operator_video_supplement_authorizations SET state='dispatching',dispatched_at_ms=$2,updated_at_ms=$2 WHERE authorization_id=$1").bind(parent).bind(now).execute(&db.pool).await.map_err(debug_error)?;
        sqlx::query("UPDATE dreamina_operator_video_supplement_authorizations SET state='uncertain',error_code='old_unknown' WHERE authorization_id=$1").bind(parent).execute(&db.pool).await.map_err(debug_error)?;
        let before=row(&db.pool,parent).await?;
        let hold_before:Value=sqlx::query_scalar("SELECT to_jsonb(hold) FROM customer_billing_holds hold WHERE job_id=$1").bind(fixture.request.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        let upgraded=sqlx::migrate::Migrator::with_migrations(all.iter().filter(|m|m.version<=154).cloned().collect());
        upgraded.run(&db.pool).await.map_err(debug_error)?;
        let mut after=row(&db.pool,parent).await?;
        require(after.as_object_mut().ok_or("invalid authority")?.remove("previous_authorization_id")==Some(Value::Null),"legacy root did not receive only nullable predecessor column")?;
        require(after==before,"schema154 rewrote old authority")?;
        let mut request=fixture.request.clone();request.operator_reference="offline-after-real-schema153-upgrade".into();
        let child=store(&db.pool).authorize_dreamina_video_supplement_successor(parent,&request).await.map_err(debug_error)?;
        require(child.authorization_id!=parent && child.dispatch_nonce!=nonce,"upgrade reused old authority")?;
        let hold_after:Value=sqlx::query_scalar("SELECT to_jsonb(hold) FROM customer_billing_holds hold WHERE job_id=$1").bind(fixture.request.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(hold_after==hold_before,"upgrade/successor altered original financial hold")?;
        assert_pending(&db.pool,fixture.request.job_id).await
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}
