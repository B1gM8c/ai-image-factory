use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use gpt_image_2_gateway::{
    FilesystemArtifactBlobStore,
    batches::{
        BatchFileBlob, BatchFileBlobError, BatchFileBlobStore, BatchRequestSuccess,
        BatchResultRole, BatchService, BatchStatus, CreateProjectBatch, CreateProjectFile,
        PostgresBatchService, ProjectFilePurpose, ProjectScope, ValidatedBatchLine,
    },
    database::{connect_test_pool_with_search_path, run_migrations},
};
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

struct FlakyDeleteBlobStore {
    inner: Arc<FilesystemArtifactBlobStore>,
    delete_failures_remaining: AtomicUsize,
}

impl FlakyDeleteBlobStore {
    fn new(inner: Arc<FilesystemArtifactBlobStore>, delete_failures: usize) -> Self {
        Self {
            inner,
            delete_failures_remaining: AtomicUsize::new(delete_failures),
        }
    }

    fn consume_delete_failure(&self) -> bool {
        self.delete_failures_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
    }

    fn fail_next_delete(&self) {
        self.delete_failures_remaining.store(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl BatchFileBlobStore for FlakyDeleteBlobStore {
    async fn put(
        &self,
        file_uuid: Uuid,
        bytes: &[u8],
    ) -> Result<BatchFileBlob, BatchFileBlobError> {
        self.inner.put(file_uuid, bytes).await
    }

    async fn get(
        &self,
        file_uuid: Uuid,
        blob: &BatchFileBlob,
    ) -> Result<Vec<u8>, BatchFileBlobError> {
        self.inner.get(file_uuid, blob).await
    }

    async fn delete(
        &self,
        file_uuid: Uuid,
        blob: &BatchFileBlob,
    ) -> Result<(), BatchFileBlobError> {
        if self.consume_delete_failure() {
            return Err(BatchFileBlobError::Unavailable);
        }
        self.inner.delete(file_uuid, blob).await
    }
}

#[tokio::test]
async fn project_files_and_batches_preserve_ownership_recovery_and_fencing() -> TestResult {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let Some(schema) = TestSchema::new(12).await? else {
        return Ok(());
    };
    let result = batch_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn project_file_capacity_and_cleanup_are_recoverable_and_fenced() -> TestResult {
    let Some(schema) = TestSchema::new(16).await? else {
        return Ok(());
    };
    let result = project_file_storage_hardening_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

async fn batch_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("batch migrations failed: {error:?}"))?;
    let first = seed_project(pool, "first").await?;
    let second = seed_project(pool, "second").await?;
    let artifact_root = tempfile::tempdir().map_err(debug_error)?;
    let blobs =
        Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
    let idempotent_uuid = Uuid::new_v4();
    let first_blob = blobs
        .put(idempotent_uuid, b"idempotent batch blob")
        .await
        .map_err(debug_error)?;
    let repeated_blob = blobs
        .put(idempotent_uuid, b"idempotent batch blob")
        .await
        .map_err(debug_error)?;
    require(
        first_blob == repeated_blob,
        "idempotent batch blob write changed stored integrity facts",
    )?;
    let conflicting_blob = blobs
        .put(idempotent_uuid, b"different batch blob")
        .await
        .expect_err("same object key must reject different bytes");
    require(
        conflicting_blob == BatchFileBlobError::Unavailable,
        "conflicting idempotent write did not fail closed",
    )?;
    let mut forged_blob = first_blob.clone();
    forged_blob.object_key = "batch-files/../../escape".to_string();
    let forged_read = blobs
        .get(idempotent_uuid, &forged_blob)
        .await
        .expect_err("arbitrary batch blob path must be rejected");
    require(
        forged_read == BatchFileBlobError::Integrity,
        "arbitrary batch blob path was not rejected as an integrity error",
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let object_path = artifact_root.path().join(&first_blob.object_key);
        let shard_path = object_path
            .parent()
            .ok_or_else(|| "batch object key has no shard".to_string())?;
        let object_mode = std::fs::metadata(&object_path)
            .map_err(debug_error)?
            .permissions()
            .mode()
            & 0o777;
        let shard_mode = std::fs::metadata(shard_path)
            .map_err(debug_error)?
            .permissions()
            .mode()
            & 0o777;
        require(
            object_mode == 0o600 && shard_mode == 0o700,
            "batch blob and shard permissions were not private",
        )?;
    }
    blobs
        .delete(idempotent_uuid, &first_blob)
        .await
        .map_err(debug_error)?;
    let service = PostgresBatchService::new(pool.clone(), blobs);

    let input = service
        .create_file(
            &first,
            CreateProjectFile {
                filename: "requests.jsonl",
                purpose: ProjectFilePurpose::Batch,
                bytes: b"{\"custom_id\":\"request-0\"}\n",
                expires_after: None,
            },
        )
        .await
        .map_err(debug_error)?;
    require(
        input.expires_at_ms.is_some() && input.bytes > 0 && input.sha256_hex.len() == 64,
        "batch input file did not persist its default retention and integrity facts",
    )?;
    let object_key: String =
        sqlx::query_scalar("SELECT object_key FROM project_files WHERE file_id = $1")
            .bind(&input.id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    require(
        object_key.starts_with("batch-files/")
            && object_key.split('/').count() == 3
            && !object_key.contains(".."),
        "batch file object key was not service-controlled",
    )?;

    let cross_file = service
        .get_file(&second, &input.id)
        .await
        .expect_err("another project must not retrieve this file");
    require(
        cross_file.status_code().as_u16() == 404,
        "cross-project file lookup did not use non-disclosing 404 semantics",
    )?;
    let cross_content = service
        .read_file(&second, &input.id)
        .await
        .expect_err("another project must not read this file");
    require(
        cross_content.status_code().as_u16() == 404,
        "cross-project file content lookup leaked resource existence",
    )?;

    let duplicate = service
        .create_batch(
            &first,
            create_batch(
                &input.id,
                json!({"api_key_id": "key-safe", "authz_version": 7}),
                vec![
                    line(0, "duplicate", "gpt-image-2"),
                    line(1, "duplicate", "gpt-image-2"),
                ],
            ),
        )
        .await
        .expect_err("duplicate custom_id must fail");
    require(
        duplicate.status_code().as_u16() == 400,
        "duplicate custom_id did not use invalid request semantics",
    )?;

    let unsafe_snapshot = service
        .create_batch(
            &first,
            create_batch(
                &input.id,
                json!({"authorization": "Bearer must-not-persist"}),
                vec![line(0, "unsafe", "gpt-image-2")],
            ),
        )
        .await
        .expect_err("raw authorization material must not persist");
    require(
        unsafe_snapshot.status_code().as_u16() == 400,
        "unsafe auth snapshot was not rejected",
    )?;
    let mut unsafe_route_request = create_batch(
        &input.id,
        json!({"api_key_id": "key-safe"}),
        vec![line(0, "unsafe-route", "gpt-image-2")],
    );
    unsafe_route_request.route_snapshot = json!({
        "headers": {"authorization": "Bearer must-not-persist"}
    });
    let unsafe_route = service
        .create_batch(&first, unsafe_route_request)
        .await
        .expect_err("raw route headers must not persist");
    require(
        unsafe_route.status_code().as_u16() == 400,
        "unsafe route snapshot was not rejected",
    )?;

    let batch = service
        .create_batch(
            &first,
            create_batch(
                &input.id,
                json!({
                    "api_key_id": "key-safe",
                    "credential_authz_version": 7,
                    "actor_user_id": "user-safe"
                }),
                vec![
                    line(0, "success", "gpt-image-2"),
                    line(1, "failure", "gpt-image-2"),
                ],
            ),
        )
        .await
        .map_err(debug_error)?;
    require(
        batch.status == BatchStatus::Validating,
        "new batch did not begin in validating",
    )?;
    let public_batch = serde_json::to_value(&batch).map_err(debug_error)?;
    require(
        public_batch.get("safe_auth_snapshot").is_none()
            && public_batch.get("auth_snapshot").is_none()
            && public_batch.get("route_snapshot").is_none(),
        "public ProjectBatch DTO exposed an internal execution snapshot",
    )?;
    let snapshot = service
        .load_execution_snapshot(&first, &batch.id)
        .await
        .map_err(debug_error)?;
    require(
        snapshot.safe_auth_snapshot["api_key_id"] == "key-safe"
            && snapshot.route_snapshot["route_id"] == "route-safe",
        "worker execution snapshot did not preserve safe auth and route facts",
    )?;
    let cross_snapshot = service
        .load_execution_snapshot(&second, &batch.id)
        .await
        .expect_err("another project must not load execution snapshots");
    require(
        cross_snapshot.status_code().as_u16() == 404,
        "cross-project execution snapshot lookup leaked batch existence",
    )?;
    let cross_batch = service
        .get_batch(&second, &batch.id)
        .await
        .expect_err("another project must not retrieve this batch");
    require(
        cross_batch.status_code().as_u16() == 404,
        "cross-project batch lookup did not use non-disclosing 404 semantics",
    )?;

    let runnable_before_validation = service
        .list_runnable_batches(100)
        .await
        .map_err(debug_error)?;
    require(
        runnable_before_validation.iter().any(|target| {
            target.scope == first
                && target.batch_id == batch.id
                && target.status == BatchStatus::Validating
        }),
        "cross-project recovery scan omitted a validating batch",
    )?;
    service
        .mark_batch_validated(&first, &batch.id)
        .await
        .map_err(debug_error)?;

    let in_use = service
        .delete_file(&first, &input.id)
        .await
        .expect_err("non-terminal batch input must not be deleted");
    require(
        in_use.status_code().as_u16() == 409,
        "in-use batch input did not return conflict",
    )?;

    let stale = service
        .claim_requests(
            &first,
            &batch.id,
            "worker-stale",
            1,
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?
        .pop()
        .ok_or_else(|| "first batch request was not claimed".to_string())?;
    sqlx::query(
        r#"
        UPDATE project_batch_requests
        SET lease_expires_at_ms = 0
        WHERE tenant_id = $1 AND project_id = $2
          AND batch_id = $3 AND request_id = $4
        "#,
    )
    .bind(&first.tenant_id)
    .bind(&first.project_id)
    .bind(&batch.id)
    .bind(stale.request_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE project_batch_requests
        SET available_at_ms = 9000000000000
        WHERE tenant_id = $1 AND project_id = $2
          AND batch_id = $3 AND request_id <> $4
          AND state = 'pending'
        "#,
    )
    .bind(&first.tenant_id)
    .bind(&first.project_id)
    .bind(&batch.id)
    .bind(stale.request_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let replacement = service
        .claim_requests(
            &first,
            &batch.id,
            "worker-recovery",
            1,
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?
        .pop()
        .ok_or_else(|| "expired request lease was not recoverable".to_string())?;
    require(
        replacement.request_id == stale.request_id
            && replacement.lease_epoch == stale.lease_epoch + 1
            && stale.attempt_count == 1
            && replacement.attempt_count == 2,
        "request lease recovery did not advance its fencing epoch and attempt count",
    )?;
    let stale_finish = service
        .complete_request(
            &stale,
            BatchRequestSuccess {
                status_code: 200,
                request_id: Some("request-stale".to_string()),
                body: json!({"data": "must-not-commit"}),
            },
        )
        .await
        .expect_err("superseded request lease must not commit");
    require(
        stale_finish.status_code().as_u16() == 409,
        "stale request lease did not fail with conflict",
    )?;
    service
        .complete_request(
            &replacement,
            BatchRequestSuccess {
                status_code: 200,
                request_id: Some("request-success".to_string()),
                body: json!({"data": [{"b64_json": "safe"}]}),
            },
        )
        .await
        .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE project_batch_requests
        SET available_at_ms = created_at_ms
        WHERE tenant_id = $1 AND project_id = $2
          AND batch_id = $3 AND state = 'pending'
        "#,
    )
    .bind(&first.tenant_id)
    .bind(&first.project_id)
    .bind(&batch.id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let failure = service
        .claim_requests(
            &first,
            &batch.id,
            "worker-failure",
            1,
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?
        .pop()
        .ok_or_else(|| "second batch request was not claimed".to_string())?;
    service
        .retry_request(
            &failure,
            json!({"code": "rate_limited", "message": "retry later"}),
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?;
    let delayed_claim = service
        .claim_requests(
            &first,
            &batch.id,
            "worker-too-early",
            1,
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?;
    require(
        delayed_claim.is_empty(),
        "request retry ignored its available_at backoff",
    )?;
    let premature_finalization = service
        .claim_finalization(
            &first,
            &batch.id,
            "finalizer-too-early",
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?;
    require(
        premature_finalization.is_none(),
        "delayed pending request did not block batch finalization",
    )?;
    let retry_observation: (i64, i32, Value) = sqlx::query_as(
        r#"
        SELECT available_at_ms, attempt_count, last_error
        FROM project_batch_requests
        WHERE tenant_id = $1 AND project_id = $2
          AND batch_id = $3 AND request_id = $4
        "#,
    )
    .bind(&first.tenant_id)
    .bind(&first.project_id)
    .bind(&batch.id)
    .bind(failure.request_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        retry_observation.0 > failure.lease_expires_at_ms - 60_000
            && retry_observation.1 == 1
            && retry_observation.2["code"] == "rate_limited",
        "request retry did not persist its delay, attempt count, and last error",
    )?;
    sqlx::query(
        r#"
        UPDATE project_batch_requests
        SET available_at_ms = created_at_ms
        WHERE tenant_id = $1 AND project_id = $2
          AND batch_id = $3 AND request_id = $4
        "#,
    )
    .bind(&first.tenant_id)
    .bind(&first.project_id)
    .bind(&batch.id)
    .bind(failure.request_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let retried_failure = service
        .claim_requests(
            &first,
            &batch.id,
            "worker-after-backoff",
            1,
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?
        .pop()
        .ok_or_else(|| "request was not claimable after its retry delay".to_string())?;
    require(
        retried_failure.request_id == failure.request_id
            && retried_failure.lease_epoch == failure.lease_epoch + 1
            && retried_failure.attempt_count == 2,
        "retry claim did not preserve request identity and advance fencing",
    )?;
    let stale_retry = service
        .retry_request(
            &failure,
            json!({"code": "stale_worker", "message": "must not commit"}),
            Duration::from_secs(1),
        )
        .await
        .expect_err("superseded retry lease must not change the request");
    require(
        stale_retry.status_code().as_u16() == 409,
        "stale retry lease did not fail with conflict",
    )?;
    service
        .fail_request(
            &retried_failure,
            json!({"code": "provider_failed", "message": "stable failure"}),
        )
        .await
        .map_err(debug_error)?;

    let finalization = service
        .claim_finalization(&first, &batch.id, "finalizer-a", Duration::from_secs(60))
        .await
        .map_err(debug_error)?
        .ok_or_else(|| "terminal request set was not finalizable".to_string())?;
    let (output, error) = service
        .materialize_result_files(&finalization)
        .await
        .map_err(debug_error)?;
    let output = output.ok_or_else(|| "output JSONL file was not generated".to_string())?;
    let error = error.ok_or_else(|| "error JSONL file was not generated".to_string())?;
    let repeated = service
        .materialize_result_files(&finalization)
        .await
        .map_err(debug_error)?;
    require(
        repeated.0.as_ref().map(|file| &file.id) == Some(&output.id)
            && repeated.1.as_ref().map(|file| &file.id) == Some(&error.id),
        "result materialization was not idempotent by output role",
    )?;
    let output_role_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM project_batch_output_files WHERE batch_id = $1")
            .bind(&batch.id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    require(
        output_role_count == 2,
        "batch persisted more than one file for an output role",
    )?;
    let output_jsonl = service
        .read_file(&first, &output.id)
        .await
        .map_err(debug_error)?;
    let error_jsonl = service
        .generate_result_jsonl(&first, &batch.id, BatchResultRole::Error)
        .await
        .map_err(debug_error)?;
    require(
        String::from_utf8_lossy(&output_jsonl).contains("\"custom_id\":\"success\"")
            && String::from_utf8_lossy(&error_jsonl).contains("\"custom_id\":\"failure\""),
        "result JSONL did not preserve custom_id correlation",
    )?;
    let completed = service
        .finalize_batch(&finalization)
        .await
        .map_err(debug_error)?;
    require(
        completed.status == BatchStatus::Completed
            && completed.output_file_id.as_deref() == Some(output.id.as_str())
            && completed.error_file_id.as_deref() == Some(error.id.as_str()),
        "completed batch did not expose its unique output and error files",
    )?;

    let cancel_batch = service
        .create_batch(
            &first,
            create_batch(
                &input.id,
                json!({"api_key_id": "key-cancel", "credential_authz_version": 3}),
                vec![
                    line(0, "running-on-cancel", "gpt-image-2"),
                    line(1, "delayed-on-cancel", "gpt-image-2"),
                    line(2, "pending-on-cancel", "gpt-image-2"),
                ],
            ),
        )
        .await
        .map_err(debug_error)?;
    service
        .mark_batch_validated(&first, &cancel_batch.id)
        .await
        .map_err(debug_error)?;
    let mut cancel_claims = service
        .claim_requests(
            &first,
            &cancel_batch.id,
            "worker-running",
            2,
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?;
    cancel_claims.sort_by_key(|lease| lease.ordinal);
    require(
        cancel_claims.len() == 2,
        "cancellation test requests were not claimed",
    )?;
    let running = cancel_claims.remove(0);
    let delayed_on_cancel = cancel_claims.remove(0);
    service
        .retry_request(
            &delayed_on_cancel,
            json!({"code": "rate_limited", "message": "cancel during backoff"}),
            Duration::from_secs(60 * 60),
        )
        .await
        .map_err(debug_error)?;
    let first_cancel = service.clone();
    let second_cancel = service.clone();
    let first_scope = first.clone();
    let second_scope = first.clone();
    let first_batch_id = cancel_batch.id.clone();
    let second_batch_id = cancel_batch.id.clone();
    let (first_result, second_result) = tokio::join!(
        first_cancel.cancel_batch(&first_scope, &first_batch_id),
        second_cancel.cancel_batch(&second_scope, &second_batch_id),
    );
    require(
        first_result.map_err(debug_error)?.status == BatchStatus::Cancelling
            && second_result.map_err(debug_error)?.status == BatchStatus::Cancelling,
        "concurrent cancellation did not converge idempotently",
    )?;
    let after_cancel = service
        .claim_requests(
            &first,
            &cancel_batch.id,
            "worker-must-not-claim",
            10,
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?;
    require(
        after_cancel.is_empty(),
        "cancelling batch allowed a new request claim",
    )?;
    service
        .complete_request(
            &running,
            BatchRequestSuccess {
                status_code: 200,
                request_id: Some("request-finished-during-cancel".to_string()),
                body: json!({"data": "partial"}),
            },
        )
        .await
        .map_err(debug_error)?;
    let cancel_finalization = service
        .claim_finalization(
            &first,
            &cancel_batch.id,
            "finalizer-cancel",
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?
        .ok_or_else(|| "cancelled request set was not finalizable".to_string())?;
    service
        .materialize_result_files(&cancel_finalization)
        .await
        .map_err(debug_error)?;
    let cancelled = service
        .finalize_batch(&cancel_finalization)
        .await
        .map_err(debug_error)?;
    require(
        cancelled.status == BatchStatus::Cancelled
            && cancelled.request_counts.completed == 1
            && cancelled.request_counts.cancelled == 2,
        "cancellation did not preserve running work and cancel immediate and delayed pending work",
    )?;

    let expiring_batch = service
        .create_batch(
            &first,
            create_batch(
                &input.id,
                json!({
                    "api_key_id": "key-expiring",
                    "credential_authz_version": 4
                }),
                vec![
                    line(0, "leased-at-expiry", "gpt-image-2"),
                    line(1, "pending-at-expiry", "gpt-image-2"),
                ],
            ),
        )
        .await
        .map_err(debug_error)?;
    service
        .mark_batch_validated(&first, &expiring_batch.id)
        .await
        .map_err(debug_error)?;
    let expiring_lease = service
        .claim_requests(
            &first,
            &expiring_batch.id,
            "worker-crossing-deadline",
            1,
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?
        .pop()
        .ok_or_else(|| "expiring batch request was not leased".to_string())?;
    sqlx::query(
        r#"
        UPDATE project_batches
        SET expires_at_ms = created_at_ms + 1
        WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
        "#,
    )
    .bind(&first.tenant_id)
    .bind(&first.project_id)
    .bind(&expiring_batch.id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let expiring = service
        .expire_batch(&first, &expiring_batch.id)
        .await
        .map_err(debug_error)?;
    require(
        expiring.status == BatchStatus::InProgress
            && expiring.request_counts.cancelled == 1
            && expiring.cancel_requested_at_ms.is_none(),
        "natural expiry did not remain non-terminal or distinguish itself from user cancellation",
    )?;
    let leased_state: (String, Option<String>) = sqlx::query_as(
        r#"
        SELECT state, lease_owner
        FROM project_batch_requests
        WHERE tenant_id = $1 AND project_id = $2
          AND batch_id = $3 AND request_id = $4
        "#,
    )
    .bind(&first.tenant_id)
    .bind(&first.project_id)
    .bind(&expiring_batch.id)
    .bind(expiring_lease.request_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        leased_state.0 == "leased" && leased_state.1.as_deref() == Some("worker-crossing-deadline"),
        "natural expiry cancelled or unfenced an active request lease",
    )?;
    let after_expiry = service
        .claim_requests(
            &first,
            &expiring_batch.id,
            "worker-after-deadline",
            10,
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?;
    require(
        after_expiry.is_empty(),
        "natural expiry allowed a new request claim",
    )?;
    let before_expired_lease_finishes = service
        .claim_finalization(
            &first,
            &expiring_batch.id,
            "finalizer-before-leased-result",
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?;
    require(
        before_expired_lease_finishes.is_none(),
        "natural expiry finalized while an active request lease was still running",
    )?;
    service
        .complete_request(
            &expiring_lease,
            BatchRequestSuccess {
                status_code: 200,
                request_id: Some("request-completed-after-expiry".to_string()),
                body: json!({"data": [{"b64_json": "completed-after-expiry"}]}),
            },
        )
        .await
        .map_err(debug_error)?;
    let expiry_finalization = service
        .claim_finalization(
            &first,
            &expiring_batch.id,
            "finalizer-natural-expiry",
            Duration::from_secs(60),
        )
        .await
        .map_err(debug_error)?
        .ok_or_else(|| "naturally expired batch was not finalizable".to_string())?;
    let (expired_output, expired_error) = service
        .materialize_result_files(&expiry_finalization)
        .await
        .map_err(debug_error)?;
    let expired_output =
        expired_output.ok_or_else(|| "expired batch output was not generated".to_string())?;
    require(
        expired_error.is_none(),
        "successful expired batch unexpectedly generated an error file",
    )?;
    let expired = service
        .finalize_batch(&expiry_finalization)
        .await
        .map_err(debug_error)?;
    require(
        expired.status == BatchStatus::Expired
            && expired.request_counts.completed == 1
            && expired.request_counts.cancelled == 1
            && expired.output_file_id.as_deref() == Some(expired_output.id.as_str())
            && expired.completed_at_ms.is_none()
            && expired.cancel_requested_at_ms.is_none()
            && expired.cancelled_at_ms.is_none(),
        "natural expiry did not preserve completed work and converge to expired",
    )?;
    let expired_output_jsonl = service
        .read_file(&first, &expired_output.id)
        .await
        .map_err(debug_error)?;
    require(
        String::from_utf8_lossy(&expired_output_jsonl)
            .contains("\"custom_id\":\"leased-at-expiry\"")
            && String::from_utf8_lossy(&expired_output_jsonl).contains("completed-after-expiry"),
        "naturally expired batch output was not readable after finalization",
    )?;

    let second_input = service
        .create_file(
            &second,
            CreateProjectFile {
                filename: "second.jsonl",
                purpose: ProjectFilePurpose::Batch,
                bytes: b"{\"custom_id\":\"second\"}\n",
                expires_after: None,
            },
        )
        .await
        .map_err(debug_error)?;
    let second_batch = service
        .create_batch(
            &second,
            create_batch(
                &second_input.id,
                json!({"api_key_id": "key-second", "credential_authz_version": 1}),
                vec![line(0, "second-project", "gpt-image-2")],
            ),
        )
        .await
        .map_err(debug_error)?;
    service
        .mark_batch_validated(&second, &second_batch.id)
        .await
        .map_err(debug_error)?;
    let runnable = service
        .list_runnable_batches(100)
        .await
        .map_err(debug_error)?;
    require(
        runnable.iter().any(|target| {
            target.scope == second
                && target.batch_id == second_batch.id
                && target.status == BatchStatus::InProgress
        }) && runnable.iter().all(|target| {
            target.batch_id != completed.id
                && target.batch_id != cancelled.id
                && target.batch_id != expired.id
        }),
        "recovery scan did not cross project scopes safely or included terminal work",
    )?;

    service
        .delete_file(&first, &input.id)
        .await
        .map_err(debug_error)?;
    let deleted = service
        .get_file(&first, &input.id)
        .await
        .expect_err("soft-deleted file must not remain readable");
    require(
        deleted.status_code().as_u16() == 404,
        "soft-deleted file did not become non-disclosing",
    )
}

async fn project_file_storage_hardening_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("storage hardening migrations failed: {error:?}"))?;
    let capacity_scope = seed_project(pool, "capacity").await?;
    sqlx::query(
        r#"
        UPDATE gateway_projects
        SET file_storage_limit_bytes = 32,
            file_storage_limit_count = 1
        WHERE id = $1 AND tenant_id = $2
        "#,
    )
    .bind(&capacity_scope.project_id)
    .bind(&capacity_scope.tenant_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let artifact_root = tempfile::tempdir().map_err(debug_error)?;
    let filesystem =
        Arc::new(FilesystemArtifactBlobStore::new(artifact_root.path()).map_err(debug_error)?);
    let blobs = Arc::new(FlakyDeleteBlobStore::new(filesystem, 0));
    let service = Arc::new(PostgresBatchService::new(pool.clone(), blobs.clone()));
    let barrier = Arc::new(tokio::sync::Barrier::new(3));

    let first_upload = {
        let service = Arc::clone(&service);
        let scope = capacity_scope.clone();
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            service
                .create_file(
                    &scope,
                    CreateProjectFile {
                        filename: "capacity-a.jsonl",
                        purpose: ProjectFilePurpose::Batch,
                        bytes: b"1234567890abcdef",
                        expires_after: None,
                    },
                )
                .await
        })
    };
    let second_upload = {
        let service = Arc::clone(&service);
        let scope = capacity_scope.clone();
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            service
                .create_file(
                    &scope,
                    CreateProjectFile {
                        filename: "capacity-b.jsonl",
                        purpose: ProjectFilePurpose::Batch,
                        bytes: b"fedcba0987654321",
                        expires_after: None,
                    },
                )
                .await
        })
    };
    barrier.wait().await;
    let first_upload = first_upload.await.map_err(debug_error)?;
    let second_upload = second_upload.await.map_err(debug_error)?;
    let (created, rejected) = match (first_upload, second_upload) {
        (Ok(created), Err(rejected)) | (Err(rejected), Ok(created)) => (created, rejected),
        (Ok(_), Ok(_)) => return Err("concurrent uploads bypassed the project file count".into()),
        (Err(first), Err(second)) => {
            return Err(format!(
                "both concurrent uploads failed unexpectedly: {first:?}; {second:?}"
            ));
        }
    };
    require(
        rejected.status_code().as_u16() == 409,
        "project file capacity rejection was not an explicit 4xx conflict",
    )?;
    let persisted_usage: (i64, i64) = sqlx::query_as(
        r#"
        SELECT COALESCE(SUM(byte_size), 0)::BIGINT, COUNT(*)::BIGINT
        FROM project_files
        WHERE tenant_id = $1
          AND project_id = $2
          AND cleanup_completed_at_ms IS NULL
        "#,
    )
    .bind(&capacity_scope.tenant_id)
    .bind(&capacity_scope.project_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        persisted_usage == (16, 1),
        "serialized project capacity did not preserve exactly one upload",
    )?;

    blobs.fail_next_delete();
    service
        .delete_file(&capacity_scope, &created.id)
        .await
        .map_err(debug_error)?;
    let pending_cleanup: (String, Option<i64>, Option<String>) = sqlx::query_as(
        r#"
        SELECT state, cleanup_completed_at_ms, cleanup_lease_owner
        FROM project_files
        WHERE tenant_id = $1 AND project_id = $2 AND file_id = $3
        "#,
    )
    .bind(&capacity_scope.tenant_id)
    .bind(&capacity_scope.project_id)
    .bind(&created.id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        pending_cleanup.0 == "deleted"
            && pending_cleanup.1.is_none()
            && pending_cleanup.2.is_none(),
        "failed physical deletion did not remain as retryable logical deletion",
    )?;
    let replacement_while_pending = service
        .create_file(
            &capacity_scope,
            CreateProjectFile {
                filename: "still-counted.jsonl",
                purpose: ProjectFilePurpose::Batch,
                bytes: b"1234567890abcdef",
                expires_after: None,
            },
        )
        .await
        .expect_err("logically deleted but uncleaned blob must still consume capacity");
    require(
        replacement_while_pending.status_code().as_u16() == 409,
        "pending physical cleanup stopped consuming project capacity",
    )?;

    let stale_lease = service
        .claim_file_cleanup("cleanup-stale", 10, Duration::from_secs(60))
        .await
        .map_err(debug_error)?
        .into_iter()
        .find(|lease| lease.file_id == created.id)
        .ok_or_else(|| "retryable logical deletion was not claimed".to_string())?;
    sqlx::query(
        r#"
        UPDATE project_files
        SET cleanup_lease_expires_at_ms = 0
        WHERE tenant_id = $1 AND project_id = $2 AND file_id = $3
        "#,
    )
    .bind(&capacity_scope.tenant_id)
    .bind(&capacity_scope.project_id)
    .bind(&created.id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let current_lease = service
        .claim_file_cleanup("cleanup-current", 10, Duration::from_secs(60))
        .await
        .map_err(debug_error)?
        .into_iter()
        .find(|lease| lease.file_id == created.id)
        .ok_or_else(|| "expired cleanup lease was not recoverable".to_string())?;
    let stale_completion = service
        .complete_file_cleanup(&stale_lease)
        .await
        .expect_err("stale cleanup owner must not complete a newer lease");
    require(
        stale_completion.status_code().as_u16() == 409,
        "stale cleanup completion was not fenced",
    )?;
    service
        .delete_file_blob(&current_lease)
        .await
        .map_err(debug_error)?;
    service
        .complete_file_cleanup(&current_lease)
        .await
        .map_err(debug_error)?;
    let cleanup_completed: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT cleanup_completed_at_ms
        FROM project_files
        WHERE tenant_id = $1 AND project_id = $2 AND file_id = $3
        "#,
    )
    .bind(&capacity_scope.tenant_id)
    .bind(&capacity_scope.project_id)
    .bind(&created.id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        cleanup_completed.is_some(),
        "successful physical cleanup did not persist completion",
    )?;

    sqlx::query(
        r#"
        UPDATE gateway_projects
        SET file_storage_limit_bytes = 8,
            file_storage_limit_count = 10
        WHERE id = $1 AND tenant_id = $2
        "#,
    )
    .bind(&capacity_scope.project_id)
    .bind(&capacity_scope.tenant_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    blobs.fail_next_delete();
    let byte_limit = service
        .create_file(
            &capacity_scope,
            CreateProjectFile {
                filename: "too-large-for-project.jsonl",
                purpose: ProjectFilePurpose::Batch,
                bytes: b"123456789",
                expires_after: None,
            },
        )
        .await
        .expect_err("project byte capacity must reject an individually valid file");
    require(
        byte_limit.status_code().as_u16() == 409,
        "project byte capacity rejection was not an explicit 4xx conflict",
    )?;
    let failed_upload_id: String = sqlx::query_scalar(
        r#"
        SELECT file_id
        FROM project_files
        WHERE tenant_id = $1
          AND project_id = $2
          AND filename = 'too-large-for-project.jsonl'
          AND state = 'deleted'
          AND cleanup_completed_at_ms IS NULL
        "#,
    )
    .bind(&capacity_scope.tenant_id)
    .bind(&capacity_scope.project_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let failed_upload_cleanup = service
        .claim_file_cleanup("cleanup-failed-upload", 10, Duration::from_secs(60))
        .await
        .map_err(debug_error)?
        .into_iter()
        .find(|lease| lease.file_id == failed_upload_id)
        .ok_or_else(|| "failed upload rollback was not recoverable".to_string())?;
    service
        .delete_file_blob(&failed_upload_cleanup)
        .await
        .map_err(debug_error)?;
    service
        .complete_file_cleanup(&failed_upload_cleanup)
        .await
        .map_err(debug_error)?;

    let expiry_scope = seed_project(pool, "expiry-cleanup").await?;
    let expired_file = service
        .create_file(
            &expiry_scope,
            CreateProjectFile {
                filename: "expired.jsonl",
                purpose: ProjectFilePurpose::Batch,
                bytes: b"{\"expired\":true}\n",
                expires_after: None,
            },
        )
        .await
        .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE project_files
        SET created_at_ms = 1,
            expires_at_ms = 2
        WHERE tenant_id = $1 AND project_id = $2 AND file_id = $3
        "#,
    )
    .bind(&expiry_scope.tenant_id)
    .bind(&expiry_scope.project_id)
    .bind(&expired_file.id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let expired_lease = service
        .claim_file_cleanup("cleanup-expired", 10, Duration::from_secs(60))
        .await
        .map_err(debug_error)?
        .into_iter()
        .find(|lease| lease.file_id == expired_file.id)
        .ok_or_else(|| "expired active file was not claimed".to_string())?;
    let expired_state: (String, Option<i64>) =
        sqlx::query_as("SELECT state, deleted_at_ms FROM project_files WHERE file_id = $1")
            .bind(&expired_file.id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    require(
        expired_state.0 == "deleted" && expired_state.1.is_some(),
        "expired active file was not atomically moved to logical deletion",
    )?;
    service
        .delete_file_blob(&expired_lease)
        .await
        .map_err(debug_error)?;
    service
        .complete_file_cleanup(&expired_lease)
        .await
        .map_err(debug_error)?;

    let protected_input = service
        .create_file(
            &expiry_scope,
            CreateProjectFile {
                filename: "protected.jsonl",
                purpose: ProjectFilePurpose::Batch,
                bytes: b"{\"protected\":true}\n",
                expires_after: None,
            },
        )
        .await
        .map_err(debug_error)?;
    let protected_batch = service
        .create_batch(
            &expiry_scope,
            create_batch(
                &protected_input.id,
                json!({"api_key_id": "cleanup-protection"}),
                vec![line(0, "protected", "gpt-image-2")],
            ),
        )
        .await
        .map_err(debug_error)?;
    sqlx::query(
        r#"
        UPDATE project_files
        SET created_at_ms = 1,
            expires_at_ms = 2
        WHERE tenant_id = $1 AND project_id = $2 AND file_id = $3
        "#,
    )
    .bind(&expiry_scope.tenant_id)
    .bind(&expiry_scope.project_id)
    .bind(&protected_input.id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let protected_claims = service
        .claim_file_cleanup("cleanup-protected", 100, Duration::from_secs(60))
        .await
        .map_err(debug_error)?;
    require(
        protected_claims
            .iter()
            .all(|lease| lease.file_id != protected_input.id),
        "active batch input was reclaimed after file expiry",
    )?;
    service
        .fail_batch_validation(
            &expiry_scope,
            &protected_batch.id,
            json!([{"code": "test_terminal"}]),
        )
        .await
        .map_err(debug_error)?;
    let released_input = service
        .claim_file_cleanup("cleanup-terminal", 100, Duration::from_secs(60))
        .await
        .map_err(debug_error)?
        .into_iter()
        .find(|lease| lease.file_id == protected_input.id)
        .ok_or_else(|| "terminal batch did not release its expired input".to_string())?;
    service
        .delete_file_blob(&released_input)
        .await
        .map_err(debug_error)?;
    service
        .complete_file_cleanup(&released_input)
        .await
        .map_err(debug_error)
}

fn create_batch(
    input_file_id: &str,
    safe_auth_snapshot: Value,
    lines: Vec<ValidatedBatchLine>,
) -> CreateProjectBatch {
    CreateProjectBatch {
        input_file_id: input_file_id.to_string(),
        endpoint: "/v1/images/generations".to_string(),
        completion_window: "24h".to_string(),
        metadata: json!({"suite": "postgres_batches"}),
        safe_auth_snapshot,
        route_snapshot: json!({
            "route_id": "route-safe",
            "route_revision": 1,
            "provider_id": "openai-codex"
        }),
        output_retention: Duration::from_secs(24 * 60 * 60),
        lines,
    }
}

fn line(ordinal: u32, custom_id: &str, model: &str) -> ValidatedBatchLine {
    ValidatedBatchLine {
        ordinal,
        custom_id: custom_id.to_string(),
        method: "POST".to_string(),
        url: "/v1/images/generations".to_string(),
        model: model.to_string(),
        body: json!({
            "model": model,
            "prompt": format!("batch request {custom_id}")
        }),
    }
}

async fn seed_project(pool: &PgPool, suffix: &str) -> TestResult<ProjectScope> {
    let user_id = Uuid::new_v4();
    let now = database_now(pool).await?;
    sqlx::query(
        r#"
        INSERT INTO identity_users
          (user_id, normalized_email, display_name, roles, scopes,
           authz_version, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, ARRAY['member'], ARRAY['console:access'], 1, $4, $4)
        "#,
    )
    .bind(user_id)
    .bind(format!("{suffix}-{}@batch.test", user_id.simple()))
    .bind(format!("{suffix} batch user"))
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(ProjectScope::new(
        format!("org_{}", user_id.simple()),
        format!("proj_{}", user_id.simple()),
    ))
}

async fn database_now(pool: &PgPool) -> TestResult<i64> {
    sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(pool)
        .await
        .map_err(debug_error)
}

fn require(condition: bool, message: &str) -> TestResult {
    condition.then_some(()).ok_or_else(|| message.to_string())
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

struct TestSchema {
    name: String,
    pool: PgPool,
}

impl TestSchema {
    async fn new(max_connections: u32) -> TestResult<Option<Self>> {
        let Some(database_url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set in CI".to_string());
            }
            eprintln!("skipping PostgreSQL batch test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("image_gateway_batch_test_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, max_connections, &name)
            .await
            .map_err(|error| format!("test database should be reachable: {error:?}"))?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(debug_error)?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!(
                "refusing schema DDL because current_database() is {database_name:?}"
            ));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        Ok(Some(Self { name, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.name
        )))
        .execute(&self.pool)
        .await
        .map_err(debug_error);
        self.pool.close().await;
        result.map(|_| ())
    }
}
