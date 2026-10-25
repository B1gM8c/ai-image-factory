//! Synthetic offline fixtures: no CLI invocation, HTTP fetch, or customer data.
use super::dreamina_image_success_reconciliation::{
    Fixture, fixture_with_mode, immutable_snapshot, rollback_snapshot, write_private,
};
use super::*;
use gpt_image_2_gateway::resolve_dreamina_image_success;

async fn fixture(pool: &PgPool) -> TestResult<Fixture> {
    fixture_with_mode(
        pool,
        0,
        json!({"resolution_type":"2k","ratio":"1:1","processing_mode":"synchronous"}),
        true,
    )
    .await
}
fn read_json(fixture: &Fixture, name: &str) -> TestResult<serde_json::Value> {
    serde_json::from_slice(&fs::read(fixture.evidence_root.join(name)).map_err(debug_error)?)
        .map_err(debug_error)
}
fn save_json(fixture: &Fixture, name: &str, value: &serde_json::Value) -> TestResult {
    write_private(
        &fixture.evidence_root,
        name,
        &serde_json::to_vec(value).map_err(debug_error)?,
    )
}
// Rehash deliberately to exercise semantic fences beyond stale file hashes.
fn rehash_review(fixture: &mut Fixture) -> TestResult {
    let mut review = read_json(fixture, "review.json")?;
    for (file, field) in [
        ("reviewed-query.json", "stdout_sha256"),
        ("execution-audit.json", "execution_audit_sha256"),
        ("retrieval-audit.json", "retrieval_audit_sha256"),
        ("selected.png", "selected_sha256"),
    ] {
        review[field] = json!(sha256(
            &fs::read(fixture.evidence_root.join(file)).map_err(debug_error)?
        ));
    }
    fixture.request.reviewed_stdout_sha256 = review["stdout_sha256"]
        .as_str()
        .ok_or("missing stdout hash")?
        .into();
    let mut audit = read_json(fixture, "execution-audit.json")?;
    audit["stdout_sha256"] = review["stdout_sha256"].clone();
    save_json(fixture, "execution-audit.json", &audit)?;
    review["execution_audit_sha256"] = json!(sha256(
        &fs::read(fixture.evidence_root.join("execution-audit.json")).map_err(debug_error)?
    ));
    save_json(fixture, "review.json", &review)
}

#[tokio::test]
async fn original_single_image_protocol_success_is_atomic_concurrent_and_charged_once() -> TestResult
{
    let Some(database) =
        super::accepted_image_completion_budget::database_before_image_completion_budget().await?
    else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&database.pool).await?;
        let before = immutable_snapshot(&database.pool,fixture.request.job_id).await?;
        let (first, second) = tokio::join!(
            resolve_dreamina_image_success(&database.pool,fixture.blobs.as_ref(),&fixture.evidence_root,&fixture.request),
            resolve_dreamina_image_success(&database.pool,fixture.blobs.as_ref(),&fixture.evidence_root,&fixture.request));
        let first=first.map_err(debug_error)?;
        require(first==second.map_err(debug_error)? && first.captured_micros==30_000 && first.released_micros==0 && first.committed_units==1 && first.released_units==0,"single-image concurrent frozen-price settlement diverged")?;
        require(first==resolve_dreamina_image_success(&database.pool,fixture.blobs.as_ref(),&fixture.evidence_root,&fixture.request).await.map_err(debug_error)?,"exact replay changed resolution")?;
        require(before==immutable_snapshot(&database.pool,fixture.request.job_id).await?,"recovery changed original uncertain evidence")?;
        let counts:(i64,i64,i64,i64,i32,String)=sqlx::query_as("SELECT (SELECT count(*) FROM artifacts WHERE job_id=$1),(SELECT count(*) FROM customer_rated_usage WHERE job_id=$1),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1),(SELECT count(*) FROM usage_events WHERE job_id=$1),upstream_result_count,evidence_mode FROM dreamina_operator_image_success_resolutions WHERE job_id=$1")
            .bind(fixture.request.job_id).fetch_one(&database.pool).await.map_err(debug_error)?;
        require(counts==(1,1,1,1,1,"query_only_http_retrieval_v2".into()),"single-image recovery duplicated settlement or delivery")?;
        let mut different=fixture.request.clone();different.operator_reference.push_str(":changed");
        require(resolve_dreamina_image_success(&database.pool,fixture.blobs.as_ref(),&fixture.evidence_root,&different).await.is_err(),"changed replay accepted")?;
        Ok(())
    }.await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn single_image_protocol_recovery_rolls_back_deferred_failure_then_settles_once() -> TestResult
{
    let Some(database) =
        super::accepted_image_completion_budget::database_before_image_completion_budget().await?
    else {
        return Ok(());
    };
    let result=async {
        let fixture=fixture(&database.pool).await?;
        let before=rollback_snapshot(&database.pool,fixture.request.job_id).await?;
        sqlx::query("CREATE FUNCTION reject_single_image_recovery_commit() RETURNS TRIGGER LANGUAGE plpgsql AS 'BEGIN RAISE EXCEPTION ''injected deferred recovery failure''; END'").execute(&database.pool).await.map_err(debug_error)?;
        sqlx::query("CREATE CONSTRAINT TRIGGER reject_single_image_recovery_commit AFTER INSERT ON dreamina_operator_image_success_resolutions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_single_image_recovery_commit()")
            .execute(&database.pool).await.map_err(debug_error)?;
        require(resolve_dreamina_image_success(&database.pool,fixture.blobs.as_ref(),&fixture.evidence_root,&fixture.request).await.is_err(),"deferred failure committed")?;
        require(before==rollback_snapshot(&database.pool,fixture.request.job_id).await?,"deferred failure left partial charge or success")?;
        sqlx::query("DROP TRIGGER reject_single_image_recovery_commit ON dreamina_operator_image_success_resolutions").execute(&database.pool).await.map_err(debug_error)?;
        let result=resolve_dreamina_image_success(&database.pool,fixture.blobs.as_ref(),&fixture.evidence_root,&fixture.request).await.map_err(debug_error)?;
        require(result.captured_micros==30_000 && result.committed_units==1,"retry after rollback changed original price")
    }.await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn single_image_protocol_recovery_rejects_rehashed_identity_url_quote_and_byte_mismatch()
-> TestResult {
    for scenario in [
        "query_id",
        "audit_id",
        "retrieval_id",
        "url",
        "quote",
        "owner",
        "tenant",
        "image_bytes",
        "image_size",
        "image_dimensions",
        "count_four",
        "query_download",
        "unknown_mode",
        "unknown_field",
        "truncated_png",
        "receipt_path",
        "redirect",
    ] {
        let Some(database) =
            super::accepted_image_completion_budget::database_before_image_completion_budget()
                .await?
        else {
            return Ok(());
        };
        let result = async {
            let mut fixture = fixture(&database.pool).await?;
            let before = rollback_snapshot(&database.pool, fixture.request.job_id).await?;
            match scenario {
                "query_id" => {
                    let mut v = read_json(&fixture, "reviewed-query.json")?;
                    v["submit_id"] = json!("other-task");
                    save_json(&fixture, "reviewed-query.json", &v)?;
                }
                "audit_id" => {
                    let mut v = read_json(&fixture, "execution-audit.json")?;
                    v["submission_id"] = json!(Uuid::new_v4());
                    save_json(&fixture, "execution-audit.json", &v)?;
                }
                "retrieval_id" => {
                    let mut v = read_json(&fixture, "retrieval-audit.json")?;
                    v["executor_execution_id"] = json!(Uuid::new_v4());
                    save_json(&fixture, "retrieval-audit.json", &v)?;
                }
                "url" => {
                    let mut v = read_json(&fixture, "reviewed-query.json")?;
                    v["result_json"]["images"][0]["image_url"] =
                        json!("https://media.example.test/other.png");
                    save_json(&fixture, "reviewed-query.json", &v)?;
                }
                "quote" | "owner" | "tenant" => {
                    let mut v = read_json(&fixture, "review.json")?;
                    match scenario {
                        "quote" => v["quote_hash"] = json!("a".repeat(64)),
                        "owner" => {
                            v["auth_attribution"]["project_id"] = json!("another-owner-project")
                        }
                        _ => v["tenant_id"] = json!("another-tenant"),
                    };
                    save_json(&fixture, "review.json", &v)?;
                }
                "image_bytes" => write_private(
                    &fixture.evidence_root,
                    "selected.png",
                    &png_bytes([99, 88, 77, 255]),
                )?,
                "truncated_png" => {
                    let bytes = fs::read(fixture.evidence_root.join("selected.png"))
                        .map_err(debug_error)?;
                    write_private(&fixture.evidence_root, "selected.png", &bytes[..33])?;
                    let mut v = read_json(&fixture, "retrieval-audit.json")?;
                    v["sha256"] = json!(sha256(&bytes[..33]));
                    v["byte_size"] = json!(33);
                    save_json(&fixture, "retrieval-audit.json", &v)?;
                }
                "image_size" | "image_dimensions" | "redirect" => {
                    let mut v = read_json(&fixture, "retrieval-audit.json")?;
                    let field = match scenario {
                        "image_size" => "byte_size",
                        "image_dimensions" => "width",
                        _ => "redirect_count",
                    };
                    v[field] = json!(2);
                    save_json(&fixture, "retrieval-audit.json", &v)?;
                }
                "count_four" => {
                    let mut v = read_json(&fixture, "reviewed-query.json")?;
                    let item = v["result_json"]["images"][0].clone();
                    v["result_json"]["images"] =
                        json!([item.clone(), item.clone(), item.clone(), item]);
                    save_json(&fixture, "reviewed-query.json", &v)?;
                }
                "query_download" => {
                    let mut v = read_json(&fixture, "execution-audit.json")?;
                    v["argv"]
                        .as_array_mut()
                        .ok_or("argv missing")?
                        .extend([json!("--download_dir"), json!("/a/download")]);
                    save_json(&fixture, "execution-audit.json", &v)?;
                }
                "unknown_mode" | "unknown_field" => {
                    let mut v = read_json(&fixture, "review.json")?;
                    v[if scenario == "unknown_mode" {
                        "evidence_mode"
                    } else {
                        "override"
                    }] = json!("unsupported");
                    save_json(&fixture, "review.json", &v)?;
                }
                "receipt_path" => {
                    let mut v = read_json(&fixture, "reviewed-query.json")?;
                    v["result_json"]["images"][0]["path"] = json!("/any/path.png");
                    save_json(&fixture, "reviewed-query.json", &v)?;
                }
                _ => return Err("unknown fixture scenario".into()),
            }
            rehash_review(&mut fixture)?;
            require(
                resolve_dreamina_image_success(
                    &database.pool,
                    fixture.blobs.as_ref(),
                    &fixture.evidence_root,
                    &fixture.request,
                )
                .await
                .is_err(),
                &format!("accepted invalid {scenario}"),
            )?;
            require(
                before == rollback_snapshot(&database.pool, fixture.request.job_id).await?,
                &format!("invalid {scenario} changed funds or projections"),
            )
        }
        .await;
        database.cleanup().await?;
        result?;
    }
    Ok(())
}
