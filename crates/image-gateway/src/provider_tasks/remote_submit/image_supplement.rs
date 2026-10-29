//! One explicitly authorized image supplement, with its own receipt and once claim.
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use image_cli_runtime::{
    AsyncOutputSink, FreshOutputDirectory, NoopSpawnObserver, TokioProcessBackend,
    VerifiedExecutable, WorkingDirectory,
};
use image_provider_dreamina_cli::{
    DreaminaCliPolicyV1, DreaminaCliQueryPolicyV1, DreaminaQueryStatusV1, DreaminaSubmitPayloadV1,
    DreaminaSubmitRequestV1, QueryResultRequestV1, parse_query_receipt, parse_receipt,
    parse_submit_command,
};
use image_provider_sdk::{OutputSlot, SingleOutputCommand};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use super::video_supplement::{
    LaunchEvidence, Runtime, SubmitBudget, evidence_directory, now_ms, private_read,
    private_read_optional, private_subdirectory, private_write_once, stores, terminal_stdout,
    validate_poll_binding, validate_prepared_budget,
};
use super::{GatedCliObservation, GatedCliSubmission};
use crate::{
    DreaminaImageSupplementAcceptedRequest, DreaminaImageSupplementAuthorizationRequest,
    DreaminaImageSupplementPlan, DreaminaImageSupplementPublishRequest,
    DreaminaImageSupplementUncertainRequest, ImageGatewayError, PostgresExecutionSettlementStore,
    artifacts::sha256_hex,
    providers::dreamina_cli::{command_to_gated, ordered_image_outputs},
};

const MAX_IMAGE_BYTES: u64 = 32 * 1024 * 1024;
const MAX_JSON_BYTES: u64 = 1024 * 1024;
const QUERY_WALL_LIMIT: Duration = Duration::from_secs(120);
const TERMINATION_GRACE: Duration = Duration::from_secs(2);
const AUTHORITY: &str = "dreamina-image-manual-supplement/v1";

#[derive(Serialize)]
pub struct DreaminaImageSupplementOperatorReceipt {
    pub authorization_id: Uuid,
    pub state: String,
    pub remote_operation_id: Option<String>,
    pub stdout_sha256: Option<String>,
    pub evidence_directory: PathBuf,
}

/// Validates and freezes the exact original command. This never invokes the CLI.
pub async fn prepare_dreamina_image_supplement_operator(
    pool: &PgPool,
    evidence_root: &Path,
    request: &DreaminaImageSupplementAuthorizationRequest,
) -> Result<DreaminaImageSupplementOperatorReceipt, ImageGatewayError> {
    let budget = SubmitBudget::from_env()?;
    let (store, _) = stores(pool)?;
    let (profile, account): (Uuid, Uuid) = sqlx::query_as("SELECT execution_profile_id,provider_account_id FROM provider_submissions WHERE job_id=$1 AND submission_id=$2 AND executor_execution_id=$3")
        .bind(request.job_id).bind(request.submission_id).bind(request.executor_execution_id).fetch_optional(pool).await.map_err(|_| failure())?.ok_or_else(failure)?;
    let runtime = Runtime::load_for_operation(pool, profile, account, "images.generations").await?;
    let mut request = request.clone();
    request.runtime_binding_json = runtime.binding()?;
    let plan = store.authorize_dreamina_image_supplement(&request).await?;
    let directory = evidence_directory(evidence_root, plan.authorization_id)?;
    if plan.state != "authorized" {
        return Ok(receipt(&plan, &directory, &plan.state, None));
    }
    let command = prepare_command(&plan, &runtime, budget)?;
    private_write_once(&directory, "plan.json", &serde_json::to_vec_pretty(&json!({
        "schema_version":2,"authority":AUTHORITY,"submit_budget":budget,"authorization":plan,
        "cli_executable_sha256":hex::encode(runtime.executable_sha256),"runner_sha256":hex::encode(runtime.runner_sha256),
        "argv":command.arguments().iter().map(|arg| arg.to_string_lossy().into_owned()).collect::<Vec<_>>(),
        "additional_customer_charge":false,
    })).map_err(|_| failure())?)?;
    Ok(receipt(&plan, &directory, "authorized", None))
}

/// Only a committed authorized -> dispatching claim may release generation.
/// Every failure after that claim consumes this authority, including empty stdout.
pub async fn dispatch_dreamina_image_supplement_operator(
    pool: &PgPool,
    evidence_root: &Path,
    authorization_id: Uuid,
) -> Result<DreaminaImageSupplementOperatorReceipt, ImageGatewayError> {
    let (store, _) = stores(pool)?;
    let plan = store
        .load_dreamina_image_supplement(authorization_id)
        .await?;
    let directory = evidence_directory(evidence_root, authorization_id)?;
    if plan.state != "authorized" {
        return Err(failure());
    }
    let prepared: serde_json::Value =
        serde_json::from_slice(&private_read(&directory, "plan.json", MAX_JSON_BYTES)?)
            .map_err(|_| failure())?;
    if prepared["authority"] != json!(AUTHORITY)
        || prepared["authorization"]["authorization_id"] != json!(authorization_id)
        || prepared["authorization"]["dispatch_nonce"] != json!(plan.dispatch_nonce)
        || prepared["authorization"]["command_hash"] != json!(plan.command_hash)
        || prepared["authorization"]["provider_command_sha256"]
            != json!(plan.provider_command_sha256)
        || prepared["authorization"]["quote_hash"] != json!(plan.quote_hash)
        || prepared["authorization"]["runtime_binding_json"] != plan.runtime_binding_json
    {
        return Err(failure());
    }
    let budget = validate_prepared_budget(&prepared, SubmitBudget::from_env()?)?;
    let runtime = runtime_for_plan(pool, &plan).await?;
    if runtime.binding()? != plan.runtime_binding_json
        || prepared["cli_executable_sha256"] != json!(hex::encode(runtime.executable_sha256))
        || prepared["runner_sha256"] != json!(hex::encode(runtime.runner_sha256))
    {
        return Err(failure());
    }
    let command = prepare_command(&plan, &runtime, budget)?;
    let gated = command_to_gated(&command, runtime.executable_sha256).map_err(|_| failure())?;
    let gate_root = private_subdirectory(&directory, "gate")?;
    let mut credential_guard = pool.begin().await.map_err(|_| failure())?;
    crate::provider_tasks::lock_dreamina_submit_credentials(
        &mut credential_guard,
        runtime.profile.provider_account_id(),
        runtime.profile.credential_revision(),
        runtime.profile.credential_ref(),
        runtime.profile.credential_auth_sha256(),
    )
    .await
    .map_err(|_| failure())?;
    runtime.verify_identity()?;
    let launch = LaunchEvidence {
        authorization_id,
        dispatch_nonce: plan.dispatch_nonce,
        execution_binding_sha256: supplement_binding(&plan)?,
        absolute_deadline_unix_ms: budget.launch_deadline(now_ms()?)?,
    };
    if store
        .claim_dreamina_image_supplement_dispatch(
            authorization_id,
            plan.dispatch_nonce,
            &runtime.binding()?,
        )
        .await?
        .is_none()
    {
        return Err(failure());
    }
    let result: Result<Vec<u8>, ImageGatewayError> = async {
        private_write_once(
            &directory,
            "launch.json",
            &serde_json::to_vec(&launch).map_err(|_| failure())?,
        )?;
        let binding = launch.binding()?;
        let gate = GatedCliSubmission::new(&gate_root, authorization_id).map_err(|_| failure())?;
        gate.prepare(&binding, &gated).map_err(|_| failure())?;
        let runner = VerifiedExecutable::new_with_sha256(&runtime.runner, runtime.runner_sha256)
            .map_err(|_| failure())?;
        let mut child = tokio::process::Command::new(runner.path())
            .arg(&gate_root)
            .arg(authorization_id.to_string())
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| failure())?;
        let deadline = tokio::time::Instant::now() + budget.wall_limit() + Duration::from_secs(4);
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(failure());
            }
            match gate.observe(&binding).map_err(|_| failure())? {
                GatedCliObservation::Ready(ready) => {
                    runtime.verify_identity()?;
                    gate.release(&binding, &ready).map_err(|_| failure())?;
                    credential_guard.commit().await.map_err(|_| failure())?;
                    break;
                }
                GatedCliObservation::Terminal(terminal) => return terminal_stdout(&terminal),
                GatedCliObservation::Lost { .. } => return Err(failure()),
                _ => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        loop {
            match gate.observe(&binding).map_err(|_| failure())? {
                GatedCliObservation::Terminal(terminal) => {
                    let _ = child.wait().await;
                    return terminal_stdout(&terminal);
                }
                GatedCliObservation::Lost { .. } => return Err(failure()),
                _ if tokio::time::Instant::now() >= deadline => {
                    let _ = gate.terminate_orphan(&binding);
                    return Err(failure());
                }
                _ => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    }
    .await;
    match result {
        Ok(stdout) => accept_stdout(&store, &plan, &directory, &stdout).await,
        Err(_) => {
            mark_uncertain(
                &store,
                &plan,
                "dreamina_image_supplement_dispatch_uncertain",
            )
            .await?;
            Ok(receipt(&plan, &directory, "uncertain", None))
        }
    }
}

/// Queries only this supplement's genuine durable native ID; never starts generation.
pub async fn poll_dreamina_image_supplement_operator(
    pool: &PgPool,
    evidence_root: &Path,
    authorization_id: Uuid,
) -> Result<DreaminaImageSupplementOperatorReceipt, ImageGatewayError> {
    let (store, _) = stores(pool)?;
    let mut plan = store
        .load_dreamina_image_supplement(authorization_id)
        .await?;
    let directory = evidence_directory(evidence_root, authorization_id)?;
    if plan.state == "succeeded" {
        return Ok(receipt(&plan, &directory, "published", None));
    }
    if plan.remote_operation_id.is_none() {
        if !matches!(plan.state.as_str(), "dispatching" | "uncertain") {
            return Err(failure());
        }
        let stdout = bound_terminal_stdout(&plan, &directory)?;
        accept_stdout(&store, &plan, &directory, &stdout).await?;
        plan = store
            .load_dreamina_image_supplement(authorization_id)
            .await?;
    }
    if let Some(done) = publish_saved(&store, &plan, &directory).await? {
        return Ok(done);
    }
    let remote = plan.remote_operation_id.as_deref().ok_or_else(failure)?;
    if !matches!(plan.state.as_str(), "accepted" | "uncertain") {
        return Err(failure());
    }
    let runtime = runtime_for_plan(pool, &plan).await?;
    let attempt = private_subdirectory(
        runtime.workspace_root.path(),
        &format!(".image-supplement-query-{}", Uuid::new_v4().simple()),
    )?;
    let output = FreshOutputDirectory::new(
        &WorkingDirectory::new_private(&attempt).map_err(|_| failure())?,
        MAX_IMAGE_BYTES,
    )
    .map_err(|_| failure())?;
    let query = QueryResultRequestV1::new(remote, &attempt).map_err(|_| failure())?;
    let policy = DreaminaCliQueryPolicyV1::new(
        &runtime.executable,
        runtime.executable_sha256,
        runtime.workspace_root.clone(),
        runtime.home.clone(),
        QUERY_WALL_LIMIT,
        TERMINATION_GRACE,
    )
    .map_err(|_| failure())?;
    let command = policy.command_spec(&query).map_err(|_| failure())?;
    let mut guard = pool.begin().await.map_err(|_| failure())?;
    crate::provider_tasks::lock_dreamina_submit_credentials(
        &mut guard,
        runtime.profile.provider_account_id(),
        runtime.profile.credential_revision(),
        runtime.profile.credential_ref(),
        runtime.profile.credential_auth_sha256(),
    )
    .await
    .map_err(|_| failure())?;
    runtime.verify_identity()?;
    let completion = TokioProcessBackend
        .execute_process(&command, &mut NoopSpawnObserver)
        .await
        .map_err(|_| failure())?;
    if !completion.status.success()
        || completion.stdout.is_truncated()
        || completion.stderr.is_truncated()
    {
        return Err(failure());
    }
    let stdout = completion.stdout.bytes();
    let parsed = parse_query_receipt(stdout).map_err(|_| failure())?;
    if parsed.submit_id() != remote {
        return Err(failure());
    }
    let observation = private_subdirectory(
        &directory,
        &format!("observation-{}", Uuid::new_v4().simple()),
    )?;
    private_write_once(&observation, "query-stdout.json", stdout)?;
    guard.commit().await.map_err(|_| failure())?;
    if parsed.status() != DreaminaQueryStatusV1::Success {
        output.ensure_empty().map_err(|_| failure())?;
        return Ok(receipt(
            &plan,
            &observation,
            if parsed.status() == DreaminaQueryStatusV1::Failed {
                "supplement_failed_original_held"
            } else {
                "querying"
            },
            Some(sha256_hex(stdout)),
        ));
    }
    let count = parsed
        .image_result_count()
        .filter(|n| (1..=4).contains(n))
        .ok_or_else(failure)?;
    let files = ordered_image_outputs(output.verify_files(4).map_err(|_| failure())?, &parsed)
        .map_err(|_| failure())?;
    if files.len() != count {
        return Err(failure());
    }
    let evidence_name = format!("success-{}", Uuid::new_v4().simple());
    let evidence = private_subdirectory(&directory, &evidence_name)?;
    let mut images = Vec::new();
    for (index, file) in files.into_iter().enumerate() {
        let filename = file
            .relative_filename()
            .to_str()
            .ok_or_else(failure)?
            .to_owned();
        let (sealed, bytes) = file
            .seal_to_async_sink(ImageBytes(Vec::new()))
            .await
            .map_err(|_| failure())?;
        if !bytes.0.starts_with(b"\x89PNG\r\n\x1a\n") || sha256_hex(&bytes.0) != sealed.sha256_hex {
            return Err(failure());
        }
        let name = if index == 0 {
            "selected.png".to_owned()
        } else {
            format!("image-{index}.png")
        };
        private_write_once(&evidence, &name, &bytes.0)?;
        images.push(json!({"source_index":index,"filename":filename,"evidence_filename":name,"sha256":sealed.sha256_hex,"byte_size":sealed.byte_size}));
    }
    let audit=serde_json::to_vec(&json!({"schema_version":1,"authorization_id":authorization_id,"dispatch_nonce":plan.dispatch_nonce,"job_id":plan.job_id,"submission_id":plan.submission_id,"executor_execution_id":plan.executor_execution_id,"remote_operation_id":remote,"argv":["query_result","--submit_id",remote,"--download_dir",attempt],"stdout_sha256":sha256_hex(stdout),"exit_code":0,"executed_at_ms":now_ms()?,"downloaded_images":images})).map_err(|_| failure())?;
    let submit = bound_terminal_stdout(&plan, &directory)?;
    if plan.submit_stdout_sha256.as_deref() != Some(sha256_hex(&submit).as_str()) {
        return Err(failure());
    }
    private_write_once(&evidence, "submit-stdout.json", &submit)?;
    private_write_once(&evidence, "reviewed-query.json", stdout)?;
    private_write_once(&evidence, "execution-audit.json", &audit)?;
    private_write_once(&evidence,"review.json",&serde_json::to_vec(&json!({"schema_version":1,"authority":AUTHORITY,"stdout_sha256":sha256_hex(stdout),"execution_audit_sha256":sha256_hex(&audit),"quote_hash":plan.quote_hash,"provider_command_sha256":plan.provider_command_sha256,"submit_stdout_sha256":sha256_hex(&submit),"authorization_id":authorization_id,"dispatch_nonce":plan.dispatch_nonce})).map_err(|_| failure())?)?;
    private_write_once(
        &directory,
        "publication.json",
        &serde_json::to_vec(&Publication {
            evidence_name,
            reviewed_stdout_sha256: sha256_hex(stdout),
        })
        .map_err(|_| failure())?,
    )?;
    publish_saved(&store, &plan, &directory)
        .await?
        .ok_or_else(failure)
}

/// Replays only the saved evidence transaction, without any upstream call.
pub async fn publish_dreamina_image_supplement_operator(
    pool: &PgPool,
    evidence_root: &Path,
    authorization_id: Uuid,
) -> Result<DreaminaImageSupplementOperatorReceipt, ImageGatewayError> {
    let (store, _) = stores(pool)?;
    let plan = store
        .load_dreamina_image_supplement(authorization_id)
        .await?;
    let directory = evidence_directory(evidence_root, authorization_id)?;
    if plan.state == "succeeded" {
        return Ok(receipt(&plan, &directory, "published", None));
    }
    publish_saved(&store, &plan, &directory)
        .await?
        .ok_or_else(failure)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Publication {
    evidence_name: String,
    reviewed_stdout_sha256: String,
}

async fn publish_saved(
    store: &PostgresExecutionSettlementStore,
    plan: &DreaminaImageSupplementPlan,
    directory: &Path,
) -> Result<Option<DreaminaImageSupplementOperatorReceipt>, ImageGatewayError> {
    let Some(raw) = private_read_optional(directory, "publication.json", MAX_JSON_BYTES)? else {
        return Ok(None);
    };
    let pointer: Publication = serde_json::from_slice(&raw).map_err(|_| failure())?;
    if !pointer.evidence_name.starts_with("success-")
        || pointer.evidence_name.len() != 40
        || !pointer.evidence_name[8..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit())
    {
        return Err(failure());
    }
    let evidence = directory.join(pointer.evidence_name);
    store
        .publish_dreamina_image_supplement(
            &evidence,
            &DreaminaImageSupplementPublishRequest {
                authorization_id: plan.authorization_id,
                dispatch_nonce: plan.dispatch_nonce,
                reviewed_stdout_sha256: pointer.reviewed_stdout_sha256.clone(),
            },
        )
        .await?;
    Ok(Some(receipt(
        plan,
        &evidence,
        "published",
        Some(pointer.reviewed_stdout_sha256),
    )))
}

fn prepare_command(
    plan: &DreaminaImageSupplementPlan,
    runtime: &Runtime,
    budget: SubmitBudget,
) -> Result<image_cli_runtime::CommandSpec, ImageGatewayError> {
    let bytes = serde_json::to_vec(&plan.command_json).map_err(|_| failure())?;
    if sha256_hex(&bytes) != plan.command_hash {
        return Err(failure());
    }
    let request = parse_submit_command(&bytes).map_err(|_| failure())?;
    // The original admission hash and provider canonical command digest are
    // separate identities. Rebuild the same typed command as normal submission.
    let payload =
        DreaminaSubmitPayloadV1::new(&plan.command_hash, request.clone()).map_err(|_| failure())?;
    let canonical =
        SingleOutputCommand::new(OutputSlot::new(0, 1).map_err(|_| failure())?, payload)
            .map_err(|_| failure())?;
    if hex::encode(canonical.canonical_sha256()) != plan.provider_command_sha256 {
        return Err(failure());
    }
    if !matches!(&request,DreaminaSubmitRequestV1::TextToImage(image) if image.model().as_str()=="4.5" && image.generate_num()==1)
    {
        return Err(failure());
    }
    let workspace = WorkingDirectory::new_private(private_subdirectory(
        runtime.workspace_root.path(),
        &format!(".image-supplement-{}", plan.authorization_id.simple()),
    )?)
    .map_err(|_| failure())?;
    DreaminaCliPolicyV1::new(
        &runtime.executable,
        runtime.executable_sha256,
        runtime.workspace_root.clone(),
        runtime.home.clone(),
        budget.wall_limit(),
        TERMINATION_GRACE,
    )
    .map_err(|_| failure())?
    .command_spec_in(&request, workspace)
    .map_err(|_| failure())
}

async fn runtime_for_plan(
    pool: &PgPool,
    plan: &DreaminaImageSupplementPlan,
) -> Result<Runtime, ImageGatewayError> {
    let runtime = Runtime::load_for_operation(
        pool,
        plan.execution_profile_id,
        plan.provider_account_id,
        "images.generations",
    )
    .await?;
    validate_poll_binding(&plan.runtime_binding_json, &runtime.binding()?)?;
    Ok(runtime)
}
fn supplement_binding(plan: &DreaminaImageSupplementPlan) -> Result<String, ImageGatewayError> {
    Ok(sha256_hex(&serde_json::to_vec(&json!({"authority":AUTHORITY,"authorization_id":plan.authorization_id,"dispatch_nonce":plan.dispatch_nonce,"job_id":plan.job_id,"submission_id":plan.submission_id,"executor_execution_id":plan.executor_execution_id,"original_execution_binding_sha256":plan.execution_binding_sha256,"command_hash":plan.command_hash,"provider_command_sha256":plan.provider_command_sha256,"quote_hash":plan.quote_hash,"runtime_binding":plan.runtime_binding_json})).map_err(|_|failure())?))
}
fn bound_terminal_stdout(
    plan: &DreaminaImageSupplementPlan,
    directory: &Path,
) -> Result<Vec<u8>, ImageGatewayError> {
    let launch: LaunchEvidence =
        serde_json::from_slice(&private_read(directory, "launch.json", MAX_JSON_BYTES)?)
            .map_err(|_| failure())?;
    if launch.authorization_id != plan.authorization_id
        || launch.dispatch_nonce != plan.dispatch_nonce
        || launch.execution_binding_sha256 != supplement_binding(plan)?
    {
        return Err(failure());
    }
    let gate = GatedCliSubmission::new(directory.join("gate"), plan.authorization_id)
        .map_err(|_| failure())?;
    match gate.observe(&launch.binding()?).map_err(|_| failure())? {
        GatedCliObservation::Terminal(terminal) => terminal_stdout(&terminal),
        _ => Err(failure()),
    }
}
async fn accept_stdout(
    store: &PostgresExecutionSettlementStore,
    plan: &DreaminaImageSupplementPlan,
    directory: &Path,
    stdout: &[u8],
) -> Result<DreaminaImageSupplementOperatorReceipt, ImageGatewayError> {
    let parsed = match parse_receipt(stdout) {
        Ok(receipt) => receipt,
        Err(_) => {
            mark_uncertain(store, plan, "dreamina_image_supplement_receipt_invalid").await?;
            if stdout.is_empty() {
                private_write_once(directory,"missing-submit-receipt.json",&serde_json::to_vec(&json!({"authorization_id":plan.authorization_id,"dispatch_nonce":plan.dispatch_nonce,"stdout_byte_size":0,"stdout_sha256":sha256_hex(stdout)})).map_err(|_|failure())?)?;
            } else {
                private_write_once(directory, "submit-stdout.json", stdout)?;
            }
            return Ok(receipt(
                plan,
                directory,
                "uncertain",
                Some(sha256_hex(stdout)),
            ));
        }
    };
    private_write_once(directory, "submit-stdout.json", stdout)?;
    store
        .record_dreamina_image_supplement_accepted(&DreaminaImageSupplementAcceptedRequest {
            authorization_id: plan.authorization_id,
            dispatch_nonce: plan.dispatch_nonce,
            remote_operation_id: parsed.submit_id().into(),
            submit_stdout_sha256: sha256_hex(stdout),
        })
        .await?;
    let mut result = receipt(plan, directory, "accepted", Some(sha256_hex(stdout)));
    result.remote_operation_id = Some(parsed.submit_id().into());
    Ok(result)
}
async fn mark_uncertain(
    store: &PostgresExecutionSettlementStore,
    plan: &DreaminaImageSupplementPlan,
    code: &str,
) -> Result<(), ImageGatewayError> {
    store
        .record_dreamina_image_supplement_uncertain(&DreaminaImageSupplementUncertainRequest {
            authorization_id: plan.authorization_id,
            dispatch_nonce: plan.dispatch_nonce,
            error_code: code.into(),
        })
        .await
}
fn receipt(
    plan: &DreaminaImageSupplementPlan,
    directory: &Path,
    state: &str,
    stdout_sha256: Option<String>,
) -> DreaminaImageSupplementOperatorReceipt {
    DreaminaImageSupplementOperatorReceipt {
        authorization_id: plan.authorization_id,
        state: state.into(),
        remote_operation_id: plan.remote_operation_id.clone(),
        stdout_sha256,
        evidence_directory: directory.into(),
    }
}
fn failure() -> ImageGatewayError {
    ImageGatewayError::config(
        "image supplement authority or evidence validation failed; generation must not be repeated",
    )
}
struct ImageBytes(Vec<u8>);
impl AsyncOutputSink for ImageBytes {
    type Error = std::io::Error;
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), Self::Error> {
        if self.0.len().saturating_add(chunk.len()) > MAX_IMAGE_BYTES as usize {
            return Err(std::io::Error::other("image too large"));
        }
        self.0.extend_from_slice(chunk);
        Ok(())
    }
}
