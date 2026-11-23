use gpt_image_2_gateway::{
    admission::{AttachInputObject, DreaminaStagedInputs, DreaminaWorkflowIntent},
    settlement::ImageResultStatus,
};
use image_api_contracts::dreamina::DreaminaReleasedSettlement;
use image_provider_dreamina_cli::StagedImageV1;

use super::*;

const PROMPT: &str = "offline historic image edit";
const RECEIPT: &[u8] = br#"{"submit_id":"40d80c02-66de-416b-b638-7609c25ede38","gen_status":"fail","prompt":"offline historic image edit","fail_reason":"upload resource \"third.png\": upload image: upload phase, no file upload, please check log for more details"}"#;
const TENANT: &str = "executord-process-smoke";
const PROJECT: &str = "project-test";

async fn seed_three_reference_edit(pool: &PgPool) -> TestResult<WorkLease> {
    let job_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let owner_token = Uuid::new_v4();
    let request = serde_json::from_value(json!({
        "workflow": "image2image",
        "images": [
            "https://example.com/first.png",
            "https://example.com/second.png",
            "https://example.com/third.png",
        ],
        "prompt": PROMPT,
        "model_version": "5.0",
        "resolution_type": "2k",
        "ratio": "1:1",
        "generate_num": 1,
    }))
    .map_err(debug_error)?;
    let mut staged = DreaminaStagedInputs::default();
    for (index, name) in ["first.png", "second.png", "third.png"]
        .into_iter()
        .enumerate()
    {
        let input_id = Uuid::new_v4();
        let digest = sha256(name.as_bytes());
        staged
            .images
            .push(StagedImageV1::new(name, &digest, 1024, 768).map_err(debug_error)?);
        staged.objects.push(AttachInputObject {
            role: EditInputRoleV1::Image,
            index: index as u16,
            media_type: "image/png".into(),
            blob: InputBlobRef {
                key: InputBlobKey {
                    admission_session_id: session_id,
                    input_id,
                },
                storage_backend: "test".into(),
                object_key: format!("{session_id}/{input_id}"),
                sha256_hex: digest,
                byte_size: 100,
            },
        });
    }
    let plan = DreaminaWorkflowIntent::image(request)
        .map_err(debug_error)?
        .bind(staged)
        .map_err(debug_error)?;
    let attach = plan.attach(
        AdmissionTicket {
            session_id,
            owner_token,
            request_hash: plan.source_request_hash().into(),
        },
        job_id,
        "offline-historical-image",
    );
    seed_generation_lease(
        pool,
        "compliance-resolution-worker",
        GenerationLeaseSeed {
            job_id,
            admission_session_id: session_id,
            owner_token,
            api_profile: DREAMINA_IMAGES_API_PROFILE.into(),
            provider_id: plan.provider_id().into(),
            model: format!("dreamina-image-{}", plan.provider_model_id()),
            request_id: format!("historic-image-{}", job_id.simple()),
            request_hash: attach.ticket.request_hash,
            command_schema: attach.command_schema,
            command_json: attach.command_json,
            operation: "generation".into(),
            work_kind: attach.work_kind,
            output_count: 1,
            billable_units: 1,
            output_billable_units: 1,
            billing_metric: "output".into(),
            billing_unit: "output".into(),
        },
    )
    .await
}

#[tokio::test]
async fn historical_image_upload_rejection_releases_the_original_hold_once() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let pool = &database.pool;
    let result = async {
        let work = seed_three_reference_edit(pool).await?;
        let fixture = refusal_fixture_for_work(
            pool,
            work,
            V4CustomerQuoteIdentity::dreamina(),
            true,
            RECEIPT,
            "dreamina_submit_rejected",
        )
        .await?;
        let source = serde_json::to_vec(&fixture.work.command_json).map_err(debug_error)?;
        let DreaminaSubmitRequestV1::ImageToImage(edit) =
            parse_submit_command(&source).map_err(debug_error)?
        else {
            return Err("offline fixture requires an image2image command".into());
        };
        let tasks = PostgresProviderTaskStore::new(pool.clone());
        require(
            tasks
                .classify_capacity_reconciliation(&ProviderTaskClaimScope {
                    execution_profile_id: DREAMINA_PROFILE_ID,
                    ..video_poll_scope()
                })
                .await
                .map_err(debug_error)?
                == 1,
            "historical image capacity was not classified for reconciliation",
        )?;
        let job = fixture.work.job_id;
        let read = PostgresExecutionSettlementStore::new(
            pool.clone(),
            Arc::new(InMemoryArtifactBlobStore::default()),
        );
        let public_receipt = || read.project_image_released_settlement(TENANT, PROJECT, None, job);
        let original_status = read
            .project_image_status(TENANT, PROJECT, None, job)
            .await
            .map_err(debug_error)?
            .ok_or("missing historical image job")?;
        let digest = sha256(original_status.request_id.as_bytes());
        let original_key = || {
            read.project_image_job_by_idempotency_key(TENANT, PROJECT, None, "generation", &digest)
        };
        require(
            matches!(original_status.status, ImageResultStatus::Uncertain)
                && original_key().await.map_err(debug_error)? == Some(job)
                && public_receipt().await.map_err(debug_error)?.is_none(),
            "the original held image must be uncertain and have no release receipt",
        )?;
        let before = snapshot(pool, &fixture).await?;
        let canonical = canonical_evidence(pool, &fixture).await?;
        let resolve = async |request: &DreaminaComplianceRejectionResolutionRequest| {
            resolve_dreamina_compliance_rejection(pool, &fixture.journal_root, request).await
        };
        let request = fixture.request();
        require(
            resolve(&request).await.is_err(),
            "an unpinned shell receipt released the hold",
        )?;
        bind_reviewed_fixture_request(&fixture, edit.to_argv())?;
        let wrong_review = DreaminaComplianceRejectionResolutionRequest {
            reviewed_stdout_sha256: "a".repeat(64),
            ..request.clone()
        };
        require(
            resolve(&wrong_review).await.is_err(),
            "wrong stdout digest released the hold",
        )?;
        require(
            snapshot(pool, &fixture).await? == before
                && canonical_evidence(pool, &fixture).await? == canonical,
            "rejected evidence changed the original state or hold",
        )?;

        let (first, competing) = tokio::join!(resolve(&request), resolve(&request));
        let audit = match (first, competing) {
            (Ok(first), Ok(second)) => {
                require(first == second, "concurrent historical audits differed")?;
                first
            }
            (Ok(audit), Err(_)) | (Err(_), Ok(audit)) => audit,
            (Err(first), Err(second)) => {
                return Err(format!(
                    "both historical audits failed: {first:?}/{second:?}"
                ));
            }
        };
        require(
            resolve(&request).await.map_err(debug_error)? == audit
                && audit.job_id == job
                && audit.request_id == original_status.request_id
                && audit.submission_id == request.submission_id
                && audit.executor_execution_id == request.executor_execution_id
                && audit.released_units == 1
                && audit.released_micros == HELD_MICROS
                && audit.captured_micros == 0,
            format!("historical image release lost its original identity or accounting: {audit:?}"),
        )?;
        let status = read
            .project_image_status(TENANT, PROJECT, None, job)
            .await
            .map_err(debug_error)?
            .ok_or("missing resolved historical image job")?;
        require(
            matches!(status.status, ImageResultStatus::Failed { error_code: Some(ref code) }
            if code == "dreamina_material_upload_failed")
                && original_key().await.map_err(debug_error)? == Some(job),
            format!("original ID and idempotency key lost their failed job: {status:?}"),
        )?;
        let receipt = DreaminaReleasedSettlement {
            basis: Some("dreamina_image_material_upload_rejection_v1".into()),
            state: "released".into(),
            resolution_audit_id: audit.resolution_id.to_string(),
            job_id: job.to_string(),
            request_id: audit.request_id.clone(),
            hold_id: audit.hold_id.to_string(),
            currency: "USD".into(),
            held_micros: HELD_MICROS.to_string(),
            released_micros: HELD_MICROS.to_string(),
            captured_micros: "0".into(),
        };
        require(
            public_receipt().await.map_err(debug_error)? == Some(receipt.clone()),
            "authorized image lookup did not expose the exact historical release receipt",
        )?;
        for (tenant, project, actor) in [
            ("other-tenant", PROJECT, None),
            (TENANT, "other-project", None),
            (TENANT, PROJECT, Some(Uuid::new_v4())),
        ] {
            require(
                read.project_image_released_settlement(tenant, project, actor, job)
                    .await
                    .map_err(debug_error)?
                    .is_none(),
                "historical receipt leaked outside its scope",
            )?;
        }
        let after = snapshot(pool, &fixture).await?;
        require(
            after.hold_state == "released"
                && after.released_micros == HELD_MICROS
                && after.captured_micros == 0
                && after.quota_released == 1
                && after.quota_committed == 0
                && after.ledger_count == 0
                && after.usage_count == 0
                && after.remote_count == 0
                && after.submission_count == 1
                && after.resolution_count == 1
                && after.resolution_event_count == 1
                && after.resolution_outbox_count == 1
                && canonical_evidence(pool, &fixture).await? == canonical,
            format!("historical resolution was not append-only and exactly once: {after:?}"),
        )?;
        let intent = tasks
            .load_submit_intent(request.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or("resolved image intent missing")?;
        require(
            tasks
                .record_submit_receipt(&RemoteTaskSubmitReceipt {
                    submission_id: request.submission_id,
                    executor_execution_id: request.executor_execution_id,
                    executor_owner: fixture.lease.executor_owner.clone(),
                    executor_lease_epoch: fixture.lease.executor_lease_epoch,
                    remote_operation_id: "offline-late-acceptance".into(),
                    provider_request_id: None,
                    event_identity: "offline-late-acceptance".into(),
                    execution_binding_sha256: intent.execution_binding_sha256,
                })
                .await
                .is_err()
                && snapshot(pool, &fixture).await? == after
                && public_receipt().await.map_err(debug_error)? == Some(receipt),
            "late acceptance changed the historical image release",
        )?;
        fixture.assert_one_fixture_invocation()
    }
    .await;
    combine(result, database.cleanup().await)
}
