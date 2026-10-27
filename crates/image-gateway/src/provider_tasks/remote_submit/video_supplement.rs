//! Explicit operator authority for one replacement video. This never retries a generation.
use std::{
    env, fs,
    io::Read,
    os::fd::{AsRawFd, OwnedFd},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use image_cli_runtime::{
    NoopSpawnObserver, TokioProcessBackend, VerifiedExecutable, WorkingDirectory,
};
use image_provider_dreamina_cli::{
    DreaminaCliPolicyV1, DreaminaCliQueryPolicyV1, DreaminaQueryStatusV1, DreaminaSubmitRequestV1,
    QueryResultRequestV1, parse_query_receipt, parse_receipt, parse_submit_command,
};
use rustix::fs::{self as rfs, Mode, OFlags, RenameFlags};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use super::{GatedCliBinding, GatedCliObservation, GatedCliProcessOutcome, GatedCliSubmission};
use crate::{
    DreaminaVideoSupplementAcceptedRequest, DreaminaVideoSupplementAuthorizationRequest,
    DreaminaVideoSupplementPlan, DreaminaVideoSupplementPublishRequest,
    DreaminaVideoSupplementUncertainRequest, ImageGatewayError, PostgresExecutionSettlementStore,
    artifacts::{FilesystemArtifactBlobStore, artifact_root_from_env, sha256_hex},
    executor::ExecutorInputObject,
    input_blobs::{InputBlobKey, InputBlobRef, InputBlobStore},
    media_inputs::{MediaInputKind, probe_input},
    provider_tasks::{
        PostgresProviderTaskStore, ProviderRuntimeProfile, ProviderRuntimeProfileStore,
    },
    providers::dreamina_cli::{
        command_to_gated, dreamina_credential_fingerprint, dreamina_input_files,
    },
};

const QUERY_WALL_LIMIT: Duration = Duration::from_secs(60);
const MAX_SUBMIT_DURATION_MS: u64 = 24 * 60 * 60 * 1_000;
const SUBMIT_RECEIPT_MARGIN_MS: u64 = 60_000;
const TERMINATION_GRACE: Duration = Duration::from_secs(2);
const MAX_VIDEO_BYTES: usize = 128 * 1024 * 1024;
const MAX_PRIVATE_JSON_BYTES: u64 = 1024 * 1024;

mod acceptance_recovery;
pub use acceptance_recovery::recover_dreamina_video_supplement_acceptance_operator;

/// This reviewed budget belongs to the new operator launch, not the expired
/// original execution. It is intentionally outside the 11-key DB identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SubmitBudget {
    cli_wall_timeout_ms: u64,
    provider_timeout_ms: u64,
}

impl SubmitBudget {
    fn from_env() -> Result<Self, ImageGatewayError> {
        Self::parse(
            &required_env("PROVIDER_SUBMITTER_CLI_WALL_TIMEOUT_MS")?,
            &required_env("PROVIDER_SUBMITTER_PROVIDER_TIMEOUT_MS")?,
        )
    }

    fn parse(wall: &str, provider: &str) -> Result<Self, ImageGatewayError> {
        let budget = Self {
            cli_wall_timeout_ms: wall.parse().map_err(|_| failure())?,
            provider_timeout_ms: provider.parse().map_err(|_| failure())?,
        };
        budget.validate()?;
        Ok(budget)
    }

    fn validate(self) -> Result<(), ImageGatewayError> {
        let maximum_wall = self
            .provider_timeout_ms
            .checked_sub(SUBMIT_RECEIPT_MARGIN_MS)
            .ok_or_else(failure)?;
        if self.cli_wall_timeout_ms == 0
            || self.provider_timeout_ms > MAX_SUBMIT_DURATION_MS
            || self.cli_wall_timeout_ms > maximum_wall
        {
            return Err(ImageGatewayError::config(
                "video supplement submit budget must leave at least 60000ms for its receipt",
            ));
        }
        Ok(())
    }

    fn wall_limit(self) -> Duration {
        Duration::from_millis(self.cli_wall_timeout_ms)
    }

    fn launch_deadline(self, now: u64) -> Result<u64, ImageGatewayError> {
        now.checked_add(self.cli_wall_timeout_ms)
            .ok_or_else(failure)
    }
}

fn validate_prepared_budget(
    prepared: &serde_json::Value,
    current: SubmitBudget,
) -> Result<SubmitBudget, ImageGatewayError> {
    let frozen: SubmitBudget =
        serde_json::from_value(prepared.get("submit_budget").cloned().ok_or_else(|| {
            ImageGatewayError::config(
                "legacy supplement plan has no reviewed submit budget; dispatch refused",
            )
        })?)
        .map_err(|_| failure())?;
    frozen.validate()?;
    if prepared["schema_version"] != json!(2) || frozen != current {
        return Err(ImageGatewayError::config(
            "video supplement submit budget changed after prepare",
        ));
    }
    Ok(frozen)
}

#[derive(Serialize)]
pub struct DreaminaVideoSupplementOperatorReceipt {
    pub authorization_id: Uuid,
    pub state: String,
    pub remote_operation_id: Option<String>,
    pub stdout_sha256: Option<String>,
    pub evidence_directory: PathBuf,
}

struct Runtime {
    profile: ProviderRuntimeProfile,
    upstream_identity_sha256: String,
    executable: PathBuf,
    executable_sha256: [u8; 32],
    runner: PathBuf,
    runner_sha256: [u8; 32],
    home: WorkingDirectory,
    workspace_root: WorkingDirectory,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LaunchEvidence {
    authorization_id: Uuid,
    dispatch_nonce: Uuid,
    execution_binding_sha256: String,
    absolute_deadline_unix_ms: u64,
}

#[derive(FromRow)]
struct InputRow {
    admission_session_id: Uuid,
    input_id: Uuid,
    role: String,
    input_index: i16,
    media_type: String,
    storage_backend: String,
    object_key: String,
    sha256_hex: String,
    byte_size: i64,
}

/// Creates a private, reviewable plan and verifies the original frozen inputs for free.
/// It neither starts the helper nor releases an upstream command.
pub async fn prepare_dreamina_video_supplement_operator(
    pool: &PgPool,
    evidence_root: &Path,
    request: &DreaminaVideoSupplementAuthorizationRequest,
) -> Result<DreaminaVideoSupplementOperatorReceipt, ImageGatewayError> {
    prepare_operator(pool, evidence_root, request, None).await
}

/// Prepare only a newly authorized successor. Dispatch remains a separate
/// one-shot command with a new authority ID and protected evidence directory.
pub async fn prepare_dreamina_video_supplement_successor_operator(
    pool: &PgPool,
    evidence_root: &Path,
    previous_authorization_id: Uuid,
    request: &DreaminaVideoSupplementAuthorizationRequest,
) -> Result<DreaminaVideoSupplementOperatorReceipt, ImageGatewayError> {
    prepare_operator(
        pool,
        evidence_root,
        request,
        Some(previous_authorization_id),
    )
    .await
}

async fn prepare_operator(
    pool: &PgPool,
    evidence_root: &Path,
    request: &DreaminaVideoSupplementAuthorizationRequest,
    previous_authorization_id: Option<Uuid>,
) -> Result<DreaminaVideoSupplementOperatorReceipt, ImageGatewayError> {
    let submit_budget = SubmitBudget::from_env()?;
    let (store, blobs) = stores(pool)?;
    let (profile_id, account_id): (Uuid, Uuid) = sqlx::query_as(
        "SELECT execution_profile_id,provider_account_id FROM provider_submissions WHERE job_id=$1 AND submission_id=$2 AND executor_execution_id=$3",
    )
    .bind(request.job_id)
    .bind(request.submission_id)
    .bind(request.executor_execution_id)
    .fetch_optional(pool)
    .await
    .map_err(|_| failure())?
    .ok_or_else(failure)?;
    let runtime = Runtime::load_for_identity(pool, profile_id, account_id).await?;
    let mut request = request.clone();
    request.runtime_binding_json = runtime.binding()?;
    let plan = match previous_authorization_id {
        Some(previous) => {
            store
                .authorize_dreamina_video_supplement_successor(previous, &request)
                .await?
        }
        None => store.authorize_dreamina_video_supplement(&request).await?,
    };
    let directory = evidence_directory(evidence_root, plan.authorization_id)?;
    if plan.state != "authorized" {
        return Ok(receipt(&plan, &directory, &plan.state, None));
    }
    let (_, command) = prepare_command(pool, &plan, &runtime, blobs, submit_budget).await?;
    private_write_once(&directory, "plan.json", &serde_json::to_vec_pretty(&json!({
        "schema_version": 2,
        "authority": "dreamina-video-manual-supplement",
        "submit_budget": submit_budget,
        "previous_authorization_id": previous_authorization_id,
        "authorization": plan,
        "cli_executable_sha256": hex::encode(runtime.executable_sha256),
        "runner_sha256": hex::encode(runtime.runner_sha256),
        "argv": command.arguments().iter().map(|arg| arg.to_string_lossy().into_owned()).collect::<Vec<_>>(),
        "additional_customer_charge": false,
    })).map_err(|_| failure())?)?;
    Ok(receipt(&plan, &directory, "authorized", None))
}

/// Claim is durable before any helper can release generation. Every later invocation is
/// refused, including after a timeout, unknown result, process restart, or failed launch.
pub async fn dispatch_dreamina_video_supplement_operator(
    pool: &PgPool,
    evidence_root: &Path,
    authorization_id: Uuid,
) -> Result<DreaminaVideoSupplementOperatorReceipt, ImageGatewayError> {
    let (store, blobs) = stores(pool)?;
    let plan = store
        .load_dreamina_video_supplement(authorization_id)
        .await?;
    let directory = evidence_directory(evidence_root, authorization_id)?;
    if plan.state != "authorized" {
        return Err(ImageGatewayError::config(
            "video supplement dispatch was already claimed; use poll only",
        ));
    }
    // An authorized plan must already have been written and reviewed through prepare.
    let prepared_plan = private_read(&directory, "plan.json", MAX_PRIVATE_JSON_BYTES)?;
    let prepared_plan: serde_json::Value =
        serde_json::from_slice(&prepared_plan).map_err(|_| failure())?;
    if prepared_plan["authorization"]["authorization_id"] != json!(authorization_id)
        || prepared_plan["authorization"]["dispatch_nonce"] != json!(plan.dispatch_nonce)
        || prepared_plan["authorization"]["command_hash"] != json!(plan.command_hash)
        || prepared_plan["authorization"]["quote_hash"] != json!(plan.quote_hash)
        || prepared_plan["authorization"]["runtime_binding_json"] != plan.runtime_binding_json
    {
        return Err(failure());
    }
    let submit_budget = validate_prepared_budget(&prepared_plan, SubmitBudget::from_env()?)?;
    let runtime = Runtime::load(pool, &plan).await?;
    let runtime_binding = runtime.binding()?;
    if runtime_binding != plan.runtime_binding_json
        || prepared_plan["cli_executable_sha256"] != json!(hex::encode(runtime.executable_sha256))
        || prepared_plan["runner_sha256"] != json!(hex::encode(runtime.runner_sha256))
    {
        return Err(failure());
    }
    let (_, command) = prepare_command(pool, &plan, &runtime, blobs, submit_budget).await?;
    let gate_command =
        command_to_gated(&command, runtime.executable_sha256).map_err(|_| failure())?;
    let gate_root = private_subdirectory(&directory, "gate")?;
    // Lock the same live credential head used by the normal submit runtime; no
    // credential refresh or keychain replacement can cross the release boundary.
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
        absolute_deadline_unix_ms: submit_budget.launch_deadline(now_ms()?)?,
    };
    // From here every failure remains uncertain and the one-shot claim is never reset.
    let result = one_shot_dispatch(
        &store,
        authorization_id,
        plan.dispatch_nonce,
        &runtime_binding,
        || async {
            private_write_once(
                &directory,
                "launch.json",
                &serde_json::to_vec(&launch).map_err(|_| failure())?,
            )?;
            let binding = launch.binding()?;
            let gate =
                GatedCliSubmission::new(&gate_root, authorization_id).map_err(|_| failure())?;
            gate.prepare(&binding, &gate_command)
                .map_err(|_| failure())?;
            let runner =
                VerifiedExecutable::new_with_sha256(&runtime.runner, runtime.runner_sha256)
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
            let deadline =
                tokio::time::Instant::now() + submit_budget.wall_limit() + Duration::from_secs(4);
            loop {
                if tokio::time::Instant::now() >= deadline {
                    return Err(failure());
                }
                match gate.observe(&binding).map_err(|_| failure())? {
                    GatedCliObservation::Ready(ready) => {
                        runtime.verify_identity()?;
                        gate.release(&binding, &ready).map_err(|_| failure())?;
                        credential_guard.commit().await.map_err(|_| failure())?;
                        // The guard cannot be consumed twice; continue using a separate loop.
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
        },
    )
    .await?
    .ok_or_else(|| {
        ImageGatewayError::config("video supplement dispatch was already claimed; use poll only")
    })?;
    match result {
        Ok(stdout) => accept_stdout(&store, &plan, &directory, &stdout).await,
        Err(_) => {
            store
                .record_dreamina_video_supplement_uncertain(
                    &DreaminaVideoSupplementUncertainRequest {
                        authorization_id,
                        dispatch_nonce: plan.dispatch_nonce,
                        error_code: "dreamina_video_supplement_dispatch_uncertain".into(),
                    },
                )
                .await?;
            Ok(receipt(&plan, &directory, "uncertain", None))
        }
    }
}

/// Queries only a genuine accepted replacement ID. If dispatch died before recording
/// acceptance, durable terminal stdout may recover it; this path never starts generation.
pub async fn poll_dreamina_video_supplement_operator(
    pool: &PgPool,
    evidence_root: &Path,
    authorization_id: Uuid,
) -> Result<DreaminaVideoSupplementOperatorReceipt, ImageGatewayError> {
    let (store, _) = stores(pool)?;
    let mut plan = store
        .load_dreamina_video_supplement(authorization_id)
        .await?;
    let directory = evidence_directory(evidence_root, authorization_id)?;
    if plan.state == "succeeded" {
        return Ok(receipt(&plan, &directory, "published", None));
    }
    if store
        .is_dreamina_video_supplement_cancelled(authorization_id, plan.dispatch_nonce)
        .await?
    {
        return Ok(receipt(&plan, &directory, "cancelled", None));
    }
    if plan.remote_operation_id.is_none() {
        recover_acceptance(&store, &plan, &directory).await?;
        plan = store
            .load_dreamina_video_supplement(authorization_id)
            .await?;
    }
    let remote = plan.remote_operation_id.as_deref().ok_or_else(failure)?;
    if !matches!(plan.state.as_str(), "accepted" | "uncertain") {
        return Err(failure());
    }
    if let Some(receipt) = recover_publication(&store, &plan, &directory).await? {
        return Ok(receipt);
    }
    let runtime = Runtime::load(pool, &plan).await?;
    let attempt = private_subdirectory(
        runtime.workspace_root.path(),
        &format!(".supplement-query-{}", Uuid::new_v4().simple()),
    )?;
    let policy = DreaminaCliQueryPolicyV1::new(
        &runtime.executable,
        runtime.executable_sha256,
        runtime.workspace_root.clone(),
        runtime.home.clone(),
        QUERY_WALL_LIMIT,
        TERMINATION_GRACE,
    )
    .map_err(|_| failure())?;
    let query = QueryResultRequestV1::query_only(remote, &attempt).map_err(|_| failure())?;
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
    let parsed = validate_query(stdout, remote)?;
    let stdout_sha256 = sha256_hex(stdout);
    let observation = private_subdirectory(
        &directory,
        &format!("observation-{}", Uuid::new_v4().simple()),
    )?;
    private_write_once(&observation, "query-stdout.json", stdout)?;
    let observed_at_ms = i64::try_from(now_ms()?).map_err(|_| failure())?;
    private_write_once(
        &observation,
        "observation.json",
        &serde_json::to_vec(&json!({
            "authorization_id": authorization_id, "dispatch_nonce": plan.dispatch_nonce,
            "remote_operation_id": remote, "stdout_sha256": stdout_sha256,
            "observed_at_ms": observed_at_ms, "status": format!("{:?}", parsed.status()),
        }))
        .map_err(|_| failure())?,
    )?;
    store
        .record_dreamina_video_supplement_observation(
            authorization_id,
            plan.dispatch_nonce,
            stdout,
            observed_at_ms,
        )
        .await?;
    guard.commit().await.map_err(|_| failure())?;
    if parsed.status() != DreaminaQueryStatusV1::Success {
        // Failure never releases/refunds the original held usage.
        return Ok(receipt(
            &plan,
            &directory,
            if parsed.status() == DreaminaQueryStatusV1::Failed {
                "supplement_failed_original_held"
            } else {
                "querying"
            },
            Some(stdout_sha256),
        ));
    }
    let video = parsed.remote_video_result().ok_or_else(failure)?;
    let mut total = 0;
    let downloaded = crate::api::video_inputs::decode_media_source(
        "video",
        video.video_url(),
        MAX_VIDEO_BYTES,
        MAX_VIDEO_BYTES,
        &["video/mp4", "application/octet-stream"],
        &mut total,
    )
    .await
    .map_err(|_| failure())?;
    publish_video_evidence(
        &store,
        &plan,
        &directory,
        stdout,
        &downloaded.bytes,
        i64::try_from(now_ms()?).map_err(|_| failure())?,
    )
    .await
}

async fn publish_video_evidence(
    store: &PostgresExecutionSettlementStore,
    plan: &DreaminaVideoSupplementPlan,
    directory: &Path,
    stdout: &[u8],
    video_bytes: &[u8],
    executed_at_ms: i64,
) -> Result<DreaminaVideoSupplementOperatorReceipt, ImageGatewayError> {
    let remote = plan.remote_operation_id.as_deref().ok_or_else(failure)?;
    let parsed = validate_query(stdout, remote)?;
    let video = parsed.remote_video_result().ok_or_else(failure)?;
    let stdout_sha256 = sha256_hex(stdout);
    let metadata = probe_input(video_bytes, MediaInputKind::Video).map_err(|_| failure())?;
    let audit = serde_json::to_vec(&json!({
        "schema_version": 1, "authorization_id": plan.authorization_id, "dispatch_nonce": plan.dispatch_nonce,
        "job_id": plan.job_id, "submission_id": plan.submission_id, "executor_execution_id": plan.executor_execution_id,
        "remote_operation_id": remote, "argv": ["query_result", "--submit_id", remote],
        "stdout_sha256": stdout_sha256, "exit_code": 0, "executed_at_ms": executed_at_ms,
    })).map_err(|_| failure())?;
    let source = acceptance_recovery::recovered_receipt(plan, directory)?;
    let recovery_digest = source.as_ref().map(|(_, marker)| sha256_hex(marker));
    let review = serde_json::to_vec(&json!({
        "schema_version": 1, "stdout_sha256": stdout_sha256, "execution_audit_sha256": sha256_hex(&audit),
        "video_url_sha256": sha256_hex(video.video_url().as_bytes()), "quote_hash": plan.quote_hash,
        "provider_command_sha256": plan.command_hash, "submit_stdout_sha256": plan.submit_stdout_sha256,
        "acceptance_recovery_sha256": recovery_digest, "retrieved_video_sha256": sha256_hex(video_bytes), "retrieved_video_byte_size": video_bytes.len(),
    })).map_err(|_| failure())?;
    let evidence_name = format!("success-{}", Uuid::new_v4().simple());
    let evidence = private_subdirectory(&directory, &evidence_name)?;
    match acceptance_recovery::recovered_receipt(plan, directory)? {
        Some((accepted, marker)) => {
            private_write_once(&evidence, "acceptance-query.json", &accepted)?;
            private_write_once(&evidence, "acceptance-recovery.json", &marker)?;
        }
        None => private_write_once(
            &evidence,
            "submit-stdout.json",
            &private_read(directory, "submit-stdout.json", 64 * 1024)?,
        )?,
    }
    private_write_once(&evidence, "reviewed-query.json", stdout)?;
    private_write_once(&evidence, "execution-audit.json", &audit)?;
    private_write_once(&evidence, "retrieved-video.mp4", video_bytes)?;
    private_write_once(&evidence, "review.json", &review)?;
    private_write_once(&evidence, "video-metadata.json", &serde_json::to_vec(&json!({
        "expires_at_ms": video.expires_at_ms(), "dimensions": metadata.dimensions(), "duration_ms": metadata.duration_ms(),
        "sha256": sha256_hex(video_bytes), "byte_size": video_bytes.len(),
    })).map_err(|_| failure())?)?;
    private_write_once(
        &directory,
        "publication.json",
        &serde_json::to_vec(&PublicationEvidence {
            evidence_name,
            reviewed_stdout_sha256: stdout_sha256.clone(),
        })
        .map_err(|_| failure())?,
    )?;
    recover_publication(store, plan, directory)
        .await?
        .ok_or_else(failure)
}

fn validate_query(
    stdout: &[u8],
    expected_remote: &str,
) -> Result<image_provider_dreamina_cli::DreaminaQueryReceiptV1, ImageGatewayError> {
    let receipt = parse_query_receipt(stdout).map_err(|_| failure())?;
    if receipt.submit_id() != expected_remote
        || (receipt.status() == DreaminaQueryStatusV1::Success
            && receipt.remote_video_result().is_none())
    {
        return Err(failure());
    }
    Ok(receipt)
}

#[async_trait::async_trait]
trait DispatchClaimStore: Sync {
    async fn claim(
        &self,
        authorization_id: Uuid,
        dispatch_nonce: Uuid,
        runtime_binding: &serde_json::Value,
    ) -> Result<bool, ImageGatewayError>;
}
#[async_trait::async_trait]
impl DispatchClaimStore for PostgresExecutionSettlementStore {
    async fn claim(
        &self,
        authorization_id: Uuid,
        dispatch_nonce: Uuid,
        runtime_binding: &serde_json::Value,
    ) -> Result<bool, ImageGatewayError> {
        Ok(self
            .claim_dreamina_video_supplement_dispatch(
                authorization_id,
                dispatch_nonce,
                runtime_binding,
            )
            .await?
            .is_some())
    }
}
/// The launch closure is consumed only by the successful durable claimant. This has no
/// retry loop and never resets a claim when a transport times out or returns unknown.
async fn one_shot_dispatch<S, F, Fut>(
    store: &S,
    authorization_id: Uuid,
    dispatch_nonce: Uuid,
    runtime_binding: &serde_json::Value,
    launch: F,
) -> Result<Option<Result<Vec<u8>, ImageGatewayError>>, ImageGatewayError>
where
    S: DispatchClaimStore,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Vec<u8>, ImageGatewayError>>,
{
    if !store
        .claim(authorization_id, dispatch_nonce, runtime_binding)
        .await?
    {
        return Ok(None);
    }
    Ok(Some(launch().await))
}

async fn recover_acceptance(
    store: &PostgresExecutionSettlementStore,
    plan: &DreaminaVideoSupplementPlan,
    directory: &Path,
) -> Result<(), ImageGatewayError> {
    if !matches!(plan.state.as_str(), "dispatching" | "uncertain") {
        return Err(failure());
    }
    let stdout = recover_submit_stdout(plan, directory)?;
    accept_stdout(store, plan, directory, &stdout).await?;
    Ok(())
}

fn recover_submit_stdout(
    plan: &DreaminaVideoSupplementPlan,
    directory: &Path,
) -> Result<Vec<u8>, ImageGatewayError> {
    let _lock = recovery_lock(directory)?;
    let existing = private_read_optional(directory, "submit-stdout.json", 64 * 1024)?;
    if let Some(stdout) = &existing {
        if parse_receipt(stdout).is_ok() {
            return if bound_terminal_stdout(plan, directory)? == *stdout {
                Ok(stdout.clone())
            } else {
                Err(failure())
            };
        }
        // A complete JSON document with a different meaning is conflicting evidence.
        if serde_json::from_slice::<serde_json::Value>(stdout).is_ok() {
            return Err(failure());
        }
    }
    let stdout = bound_terminal_stdout(plan, directory)?;
    // Only the complete terminal of this exact authorization/binding can
    // supersede an old crash-torn JSON file. Keep the original bytes private.
    parse_receipt(&stdout).map_err(|_| failure())?;
    if let Some(existing) = existing {
        quarantine_broken_json(directory, "submit-stdout.json", &existing)?;
    }
    private_write_once(directory, "submit-stdout.json", &stdout)?;
    Ok(stdout)
}

fn bound_terminal_stdout(
    plan: &DreaminaVideoSupplementPlan,
    directory: &Path,
) -> Result<Vec<u8>, ImageGatewayError> {
    let launch: LaunchEvidence = serde_json::from_slice(&private_read(
        directory,
        "launch.json",
        MAX_PRIVATE_JSON_BYTES,
    )?)
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
        _ => Err(ImageGatewayError::config(
            "video supplement has no accepted receipt; dispatch remains claimed and cannot be repeated",
        )),
    }
}

#[async_trait::async_trait]
trait ReceiptStore: Sync {
    async fn record_accepted(
        &self,
        request: &DreaminaVideoSupplementAcceptedRequest,
    ) -> Result<(), ImageGatewayError>;
    async fn record_uncertain(
        &self,
        request: &DreaminaVideoSupplementUncertainRequest,
    ) -> Result<(), ImageGatewayError>;
}

#[async_trait::async_trait]
impl ReceiptStore for PostgresExecutionSettlementStore {
    async fn record_accepted(
        &self,
        request: &DreaminaVideoSupplementAcceptedRequest,
    ) -> Result<(), ImageGatewayError> {
        self.record_dreamina_video_supplement_accepted(request)
            .await
    }
    async fn record_uncertain(
        &self,
        request: &DreaminaVideoSupplementUncertainRequest,
    ) -> Result<(), ImageGatewayError> {
        self.record_dreamina_video_supplement_uncertain(request)
            .await
    }
}

async fn accept_stdout<S: ReceiptStore>(
    store: &S,
    plan: &DreaminaVideoSupplementPlan,
    directory: &Path,
    stdout: &[u8],
) -> Result<DreaminaVideoSupplementOperatorReceipt, ImageGatewayError> {
    let accepted = match parse_receipt(stdout) {
        Ok(receipt) => receipt,
        Err(_) => {
            store
                .record_uncertain(&DreaminaVideoSupplementUncertainRequest {
                    authorization_id: plan.authorization_id,
                    dispatch_nonce: plan.dispatch_nonce,
                    error_code: "dreamina_video_supplement_receipt_invalid".into(),
                })
                .await?;
            // Empty output is possible after a real upstream acceptance. Record
            // uncertainty before writing evidence: the journal deliberately
            // refuses an empty file and must not leave this claim dispatching.
            if stdout.is_empty() {
                private_write_once(
                    directory,
                    "missing-submit-receipt.json",
                    &serde_json::to_vec(&json!({
                        "schema_version": 1,
                        "authorization_id": plan.authorization_id,
                        "dispatch_nonce": plan.dispatch_nonce,
                        "stdout_byte_size": 0,
                        "stdout_sha256": sha256_hex(stdout),
                        "error_code": "dreamina_video_supplement_receipt_invalid",
                    }))
                    .map_err(|_| failure())?,
                )?;
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
    let digest = sha256_hex(stdout);
    store
        .record_accepted(&DreaminaVideoSupplementAcceptedRequest {
            authorization_id: plan.authorization_id,
            dispatch_nonce: plan.dispatch_nonce,
            remote_operation_id: accepted.submit_id().into(),
            submit_stdout_sha256: digest.clone(),
        })
        .await?;
    let mut receipt = receipt(plan, directory, "accepted", Some(digest));
    receipt.remote_operation_id = Some(accepted.submit_id().into());
    Ok(receipt)
}

fn terminal_stdout(
    terminal: &super::GatedCliProcessTerminal,
) -> Result<Vec<u8>, ImageGatewayError> {
    if !terminal.released()
        || !terminal.exec_started()
        || terminal.stdout_truncated()
        || terminal.stderr_truncated()
        || !matches!(
            terminal.outcome(),
            GatedCliProcessOutcome::Exited {
                exit_code: Some(0),
                signal: None
            }
        )
    {
        return Err(failure());
    }
    Ok(terminal.stdout().to_vec())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicationEvidence {
    evidence_name: String,
    reviewed_stdout_sha256: String,
}

async fn recover_publication(
    store: &PostgresExecutionSettlementStore,
    plan: &DreaminaVideoSupplementPlan,
    directory: &Path,
) -> Result<Option<DreaminaVideoSupplementOperatorReceipt>, ImageGatewayError> {
    let Some(publication) = recover_publication_pointer(plan, directory)? else {
        return Ok(None);
    };
    if !valid_success_name(&publication.evidence_name) {
        return Err(failure());
    }
    let evidence = directory.join(&publication.evidence_name);
    validate_private_directory(&evidence)?;
    store
        .publish_dreamina_video_supplement(
            &evidence,
            &DreaminaVideoSupplementPublishRequest {
                authorization_id: plan.authorization_id,
                dispatch_nonce: plan.dispatch_nonce,
                reviewed_stdout_sha256: publication.reviewed_stdout_sha256.clone(),
            },
        )
        .await?;
    Ok(Some(receipt(
        plan,
        &evidence,
        "published",
        Some(publication.reviewed_stdout_sha256),
    )))
}

fn valid_success_name(name: &str) -> bool {
    name.starts_with("success-")
        && name.len() == 40
        && name[8..].bytes().all(|b| b.is_ascii_hexdigit())
}

fn recover_publication_pointer(
    plan: &DreaminaVideoSupplementPlan,
    directory: &Path,
) -> Result<Option<PublicationEvidence>, ImageGatewayError> {
    let _lock = recovery_lock(directory)?;
    let Some(broken) =
        private_read_optional(directory, "publication.json", MAX_PRIVATE_JSON_BYTES)?
    else {
        return Ok(None);
    };
    if let Ok(pointer) = serde_json::from_slice::<PublicationEvidence>(&broken) {
        return Ok(Some(pointer));
    }
    if serde_json::from_slice::<serde_json::Value>(&broken).is_ok() {
        return Err(failure());
    }
    let submit = match acceptance_recovery::recovered_receipt(plan, directory)? {
        Some((query, _)) => query,
        None => bound_terminal_stdout(plan, directory)?,
    };
    if plan.submit_stdout_sha256.as_deref() != Some(sha256_hex(&submit).as_str())
        || Some(parse_receipt(&submit).map_err(|_| failure())?.submit_id())
            != plan.remote_operation_id.as_deref()
    {
        return Err(failure());
    }
    let mut candidate = None;
    for entry in fs::read_dir(directory).map_err(|_| failure())? {
        let entry = entry.map_err(|_| failure())?;
        let Some(name) = entry
            .file_name()
            .to_str()
            .map(str::to_owned)
            .filter(|name| valid_success_name(name))
        else {
            continue;
        };
        if let Ok(pointer) = complete_publication_evidence(plan, directory, &name, &submit) {
            if candidate.replace(pointer).is_some() {
                return Err(failure());
            }
        }
    }
    let pointer = candidate.ok_or_else(failure)?;
    quarantine_broken_json(directory, "publication.json", &broken)?;
    private_write_once(
        directory,
        "publication.json",
        &serde_json::to_vec(&pointer).map_err(|_| failure())?,
    )?;
    Ok(Some(pointer))
}

fn complete_publication_evidence(
    plan: &DreaminaVideoSupplementPlan,
    directory: &Path,
    name: &str,
    submit: &[u8],
) -> Result<PublicationEvidence, ImageGatewayError> {
    let evidence = directory.join(name);
    let raw = private_read(&evidence, "reviewed-query.json", MAX_PRIVATE_JSON_BYTES)?;
    let remote = plan.remote_operation_id.as_deref().ok_or_else(failure)?;
    let query = validate_query(&raw, remote)?;
    let accepted = match acceptance_recovery::recovered_receipt(plan, directory)? {
        Some((_, marker)) => {
            if private_read(
                &evidence,
                "acceptance-recovery.json",
                MAX_PRIVATE_JSON_BYTES,
            )? != marker
            {
                return Err(failure());
            }
            private_read(&evidence, "acceptance-query.json", 64 * 1024)?
        }
        None => private_read(&evidence, "submit-stdout.json", 64 * 1024)?,
    };
    if query.status() != DreaminaQueryStatusV1::Success || accepted != submit {
        return Err(failure());
    }
    let audit = private_read(&evidence, "execution-audit.json", MAX_PRIVATE_JSON_BYTES)?;
    let audit_json: serde_json::Value = serde_json::from_slice(&audit).map_err(|_| failure())?;
    let review: serde_json::Value = serde_json::from_slice(&private_read(
        &evidence,
        "review.json",
        MAX_PRIVATE_JSON_BYTES,
    )?)
    .map_err(|_| failure())?;
    let video = private_read(&evidence, "retrieved-video.mp4", MAX_VIDEO_BYTES as u64)?;
    probe_input(&video, MediaInputKind::Video).map_err(|_| failure())?;
    let stdout_hash = sha256_hex(&raw);
    if audit_json["schema_version"] != json!(1)
        || review["schema_version"] != json!(1)
        || audit_json["executed_at_ms"]
            .as_i64()
            .is_none_or(|time| time <= 0)
        || audit_json["authorization_id"] != json!(plan.authorization_id)
        || audit_json["dispatch_nonce"] != json!(plan.dispatch_nonce)
        || audit_json["job_id"] != json!(plan.job_id)
        || audit_json["submission_id"] != json!(plan.submission_id)
        || audit_json["executor_execution_id"] != json!(plan.executor_execution_id)
        || audit_json["remote_operation_id"] != json!(remote)
        || audit_json["argv"] != json!(["query_result", "--submit_id", remote])
        || audit_json["stdout_sha256"] != json!(stdout_hash)
        || audit_json["exit_code"] != json!(0)
        || review["stdout_sha256"] != json!(stdout_hash)
        || review["execution_audit_sha256"] != json!(sha256_hex(&audit))
        || review["quote_hash"] != json!(plan.quote_hash)
        || review["provider_command_sha256"] != json!(plan.command_hash)
        || review["submit_stdout_sha256"] != json!(sha256_hex(submit))
        || review["retrieved_video_sha256"] != json!(sha256_hex(&video))
        || review["retrieved_video_byte_size"] != json!(video.len())
        || review["video_url_sha256"]
            != json!(sha256_hex(
                query
                    .remote_video_result()
                    .ok_or_else(failure)?
                    .video_url()
                    .as_bytes()
            ))
    {
        return Err(failure());
    }
    Ok(PublicationEvidence {
        evidence_name: name.to_owned(),
        reviewed_stdout_sha256: stdout_hash,
    })
}

impl LaunchEvidence {
    fn binding(&self) -> Result<GatedCliBinding, ImageGatewayError> {
        GatedCliBinding::new(
            &self.execution_binding_sha256,
            self.dispatch_nonce,
            self.absolute_deadline_unix_ms,
        )
        .map_err(|_| failure())
    }
}

impl Runtime {
    async fn load(
        pool: &PgPool,
        plan: &DreaminaVideoSupplementPlan,
    ) -> Result<Self, ImageGatewayError> {
        let runtime =
            Self::load_for_identity(pool, plan.execution_profile_id, plan.provider_account_id)
                .await?;
        validate_poll_binding(&plan.runtime_binding_json, &runtime.binding()?)?;
        Ok(runtime)
    }

    async fn load_for_identity(
        pool: &PgPool,
        profile_id: Uuid,
        account_id: Uuid,
    ) -> Result<Self, ImageGatewayError> {
        let profile = PostgresProviderTaskStore::new(pool.clone())
            .load_active_runtime_profile(&required_env("PROVIDER_SUBMITTER_PROFILE_KEY")?)
            .await
            .map_err(|_| failure())?;
        if profile.provider_id() != "dreamina-cli"
            || profile.operation_id() != "videos.generations"
            || profile.provider_account_id() != account_id
            || profile.execution_profile_id() != profile_id
            || profile.credential_pool_id().to_string()
                != required_env("PROVIDER_SUBMITTER_CREDENTIAL_POOL_ID")?
            || profile.provider_account_id().to_string()
                != required_env("PROVIDER_SUBMITTER_ACCOUNT_ID")?
            || profile.credential_ref() != required_env("PROVIDER_SUBMITTER_CREDENTIAL_REF")?
            || profile.credential_revision().to_string()
                != required_env("PROVIDER_SUBMITTER_CREDENTIAL_REVISION")?
            || profile.credential_auth_sha256()
                != required_env("PROVIDER_SUBMITTER_CREDENTIAL_AUTH_SHA256")?
        {
            return Err(failure());
        }
        let home = WorkingDirectory::new_private(required_env("PROVIDER_SUBMITTER_ACCOUNT_HOME")?)
            .map_err(|_| failure())?;
        let workspace_root =
            WorkingDirectory::new_private(required_env("PROVIDER_SUBMITTER_WORKSPACE_ROOT")?)
                .map_err(|_| failure())?;
        let executable_sha256 = digest_env("PROVIDER_SUBMITTER_EXECUTABLE_SHA256")?;
        let runner_sha256 = digest_env("PROVIDER_SUBMITTER_RUNNER_SHA256")?;
        let executable = VerifiedExecutable::new_with_sha256(
            required_env("PROVIDER_SUBMITTER_EXECUTABLE")?,
            executable_sha256,
        )
        .map_err(|_| failure())?
        .path()
        .to_owned();
        let runner = VerifiedExecutable::new_with_sha256(
            required_env("PROVIDER_SUBMITTER_RUNNER")?,
            runner_sha256,
        )
        .map_err(|_| failure())?
        .path()
        .to_owned();
        let upstream_identity_sha256: String = sqlx::query_scalar(
            "SELECT upstream_identity_sha256 FROM provider_account_environments WHERE provider_account_id=$1 AND provider_id='dreamina-cli' AND environment_kind='dreamina_home_v1' AND state='active' AND environment_ref=$2",
        ).bind(account_id).bind(home.path().to_str().ok_or_else(failure)?)
            .fetch_optional(pool).await.map_err(|_| failure())?.ok_or_else(failure)?;
        let runtime = Self {
            profile,
            upstream_identity_sha256,
            executable,
            executable_sha256,
            runner,
            runner_sha256,
            home,
            workspace_root,
        };
        runtime.verify_identity()?;
        Ok(runtime)
    }
    fn verify_identity(&self) -> Result<(), ImageGatewayError> {
        // Private WorkingDirectory validates owner == the service UID at every command.
        validate_private_directory(self.home.path())?;
        validate_private_directory(self.workspace_root.path())?;
        VerifiedExecutable::new_with_sha256(&self.executable, self.executable_sha256)
            .map_err(|_| failure())?;
        VerifiedExecutable::new_with_sha256(&self.runner, self.runner_sha256)
            .map_err(|_| failure())?;
        if dreamina_credential_fingerprint(self.home.path()).map_err(|_| failure())?
            != self.profile.credential_auth_sha256()
        {
            return Err(failure());
        }
        Ok(())
    }

    fn binding(&self) -> Result<serde_json::Value, ImageGatewayError> {
        Ok(json!({
            "execution_profile_id": self.profile.execution_profile_id(),
            "provider_account_id": self.profile.provider_account_id(),
            "upstream_identity_sha256": self.upstream_identity_sha256,
            "credential_revision": self.profile.credential_revision(),
            "credential_ref_sha256": sha256_hex(self.profile.credential_ref().as_bytes()),
            "credential_auth_sha256": self.profile.credential_auth_sha256(),
            "account_home_sha256": sha256_hex(self.home.path().to_str().ok_or_else(failure)?.as_bytes()),
            "cli_path": self.executable.to_str().ok_or_else(failure)?,
            "cli_sha256": hex::encode(self.executable_sha256),
            "runner_path": self.runner.to_str().ok_or_else(failure)?,
            "runner_sha256": hex::encode(self.runner_sha256),
        }))
    }
}

fn validate_poll_binding(
    frozen: &serde_json::Value,
    current: &serde_json::Value,
) -> Result<(), ImageGatewayError> {
    // Poll may use a normally renewed credential for the same frozen upstream identity.
    // Dispatch additionally requires full equality, including the credential revision.
    for name in [
        "execution_profile_id",
        "provider_account_id",
        "upstream_identity_sha256",
        "account_home_sha256",
        "cli_path",
        "cli_sha256",
        "runner_path",
        "runner_sha256",
    ] {
        if frozen.get(name).is_none() || frozen.get(name) != current.get(name) {
            return Err(failure());
        }
    }
    Ok(())
}

async fn prepare_command(
    pool: &PgPool,
    plan: &DreaminaVideoSupplementPlan,
    runtime: &Runtime,
    blobs: Arc<FilesystemArtifactBlobStore>,
    submit_budget: SubmitBudget,
) -> Result<(DreaminaSubmitRequestV1, image_cli_runtime::CommandSpec), ImageGatewayError> {
    let bytes = serde_json::to_vec(&plan.command_json).map_err(|_| failure())?;
    if sha256_hex(&bytes) != plan.command_hash {
        return Err(failure());
    }
    let request = parse_submit_command(&bytes).map_err(|_| failure())?;
    if !matches!(
        request,
        DreaminaSubmitRequestV1::TextToVideo(_)
            | DreaminaSubmitRequestV1::ImageToVideo(_)
            | DreaminaSubmitRequestV1::FramesToVideo(_)
            | DreaminaSubmitRequestV1::MultiframeToVideo(_)
            | DreaminaSubmitRequestV1::MultimodalToVideo(_)
    ) || request.output_count() != 1
    {
        return Err(failure());
    }
    let workspace = private_subdirectory(
        runtime.workspace_root.path(),
        &format!(".supplement-{}", plan.authorization_id.simple()),
    )?;
    let workspace = WorkingDirectory::new_private(workspace).map_err(|_| failure())?;
    let rows: Vec<InputRow> = sqlx::query_as(r#"
        SELECT i.admission_session_id, i.input_id, i.role, i.input_index, i.media_type,
               i.storage_backend, i.object_key, i.sha256_hex, i.byte_size
        FROM job_input_objects i JOIN job_payloads p ON p.job_id=i.job_id AND p.admission_session_id=i.admission_session_id
        WHERE i.job_id=$1
        ORDER BY CASE i.role WHEN 'image' THEN 0 WHEN 'mask' THEN 1 WHEN 'video' THEN 2 WHEN 'audio' THEN 3 ELSE 4 END, i.input_index
    "#).bind(plan.job_id).fetch_all(pool).await.map_err(|_| failure())?;
    let inputs = rows
        .into_iter()
        .map(|row| {
            ExecutorInputObject::new(
                InputBlobRef {
                    key: InputBlobKey {
                        admission_session_id: row.admission_session_id,
                        input_id: row.input_id,
                    },
                    storage_backend: row.storage_backend,
                    object_key: row.object_key,
                    sha256_hex: row.sha256_hex,
                    byte_size: u64::try_from(row.byte_size).map_err(|_| failure())?,
                },
                row.role,
                u16::try_from(row.input_index).map_err(|_| failure())?,
                row.media_type,
            )
            .ok_or_else(failure)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let files = dreamina_input_files(&request, &inputs).map_err(|_| failure())?;
    let blobs: Arc<dyn InputBlobStore> = blobs;
    super::inputs::stage_inputs(
        Some(&blobs),
        &inputs,
        files,
        &workspace,
        Instant::now(),
        submit_budget.wall_limit(),
    )
    .await
    .map_err(|_| failure())?;
    let policy = DreaminaCliPolicyV1::new(
        &runtime.executable,
        runtime.executable_sha256,
        runtime.workspace_root.clone(),
        runtime.home.clone(),
        submit_budget.wall_limit(),
        TERMINATION_GRACE,
    )
    .map_err(|_| failure())?;
    let command = policy
        .command_spec_in(&request, workspace)
        .map_err(|_| failure())?;
    Ok((request, command))
}

fn stores(
    pool: &PgPool,
) -> Result<
    (
        PostgresExecutionSettlementStore,
        Arc<FilesystemArtifactBlobStore>,
    ),
    ImageGatewayError,
> {
    let blobs = Arc::new(FilesystemArtifactBlobStore::new(artifact_root_from_env()?)?);
    Ok((
        PostgresExecutionSettlementStore::new(pool.clone(), blobs.clone()),
        blobs,
    ))
}
fn supplement_binding(plan: &DreaminaVideoSupplementPlan) -> Result<String, ImageGatewayError> {
    Ok(sha256_hex(&serde_json::to_vec(&json!({"authority":"dreamina-video-manual-supplement/v1", "authorization_id":plan.authorization_id,
        "dispatch_nonce":plan.dispatch_nonce,"job_id":plan.job_id,"submission_id":plan.submission_id,"executor_execution_id":plan.executor_execution_id,
        "original_execution_binding_sha256":plan.execution_binding_sha256,"command_hash":plan.command_hash,"quote_hash":plan.quote_hash,
        "runtime_binding":plan.runtime_binding_json})).map_err(|_| failure())?))
}
fn receipt(
    plan: &DreaminaVideoSupplementPlan,
    directory: &Path,
    state: &str,
    stdout_sha256: Option<String>,
) -> DreaminaVideoSupplementOperatorReceipt {
    DreaminaVideoSupplementOperatorReceipt {
        authorization_id: plan.authorization_id,
        state: state.into(),
        remote_operation_id: plan.remote_operation_id.clone(),
        stdout_sha256,
        evidence_directory: directory.into(),
    }
}
fn required_env(name: &str) -> Result<String, ImageGatewayError> {
    env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(failure)
}
fn digest_env(name: &str) -> Result<[u8; 32], ImageGatewayError> {
    hex::decode(required_env(name)?)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(failure)
}
fn now_ms() -> Result<u64, ImageGatewayError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| failure())?
        .as_millis() as u64)
}
fn failure() -> ImageGatewayError {
    ImageGatewayError::config("video supplement operator preflight or evidence validation failed")
}

fn evidence_directory(root: &Path, id: Uuid) -> Result<PathBuf, ImageGatewayError> {
    validate_private_directory(root)?;
    private_subdirectory(root, &id.simple().to_string())
}
fn private_subdirectory(parent: &Path, name: &str) -> Result<PathBuf, ImageGatewayError> {
    validate_private_directory(parent)?;
    let path = parent.join(name);
    match fs::DirBuilder::new().mode(0o700).create(&path) {
        Ok(()) => fs::File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(|_| failure())?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(failure()),
    }
    validate_private_directory(&path)?;
    Ok(path)
}
fn validate_private_directory(path: &Path) -> Result<(), ImageGatewayError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| failure())?;
    if !path.is_absolute()
        || fs::canonicalize(path).ok().as_deref() != Some(path)
        || !metadata.is_dir()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(failure());
    }
    Ok(())
}
fn private_write_once(directory: &Path, name: &str, bytes: &[u8]) -> Result<(), ImageGatewayError> {
    let fd = private_directory_fd(directory)?;
    super::journal::publish_or_compare(&fd, name, bytes, bytes.len() as u64)
        .map_err(|_| failure())?;
    super::journal::validate_bound_root(
        directory,
        &fd,
        super::journal::RemoteSubmitJournalError::Integrity,
    )
    .map_err(|_| failure())
}
fn private_read(directory: &Path, name: &str, maximum: u64) -> Result<Vec<u8>, ImageGatewayError> {
    private_read_optional(directory, name, maximum)?
        .filter(|bytes| !bytes.is_empty())
        .ok_or_else(failure)
}

fn private_directory_fd(directory: &Path) -> Result<OwnedFd, ImageGatewayError> {
    validate_private_directory(directory)?;
    let fd = rfs::open(
        directory,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| failure())?;
    super::journal::validate_bound_root(
        directory,
        &fd,
        super::journal::RemoteSubmitJournalError::Integrity,
    )
    .map_err(|_| failure())?;
    Ok(fd)
}

fn recovery_lock(directory: &Path) -> Result<fs::File, ImageGatewayError> {
    let fd = private_directory_fd(directory)?;
    let lock: fs::File = rfs::openat(
        &fd,
        ".operator-recovery.lock",
        OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|_| failure())?
    .into();
    let meta = lock.metadata().map_err(|_| failure())?;
    if !meta.is_file()
        || meta.nlink() != 1
        || meta.mode() & 0o777 != 0o600
        || meta.uid() != unsafe { libc::geteuid() }
        || unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0
    {
        return Err(failure());
    }
    Ok(lock)
}

fn quarantine_broken_json(
    directory: &Path,
    name: &str,
    broken: &[u8],
) -> Result<(), ImageGatewayError> {
    if serde_json::from_slice::<serde_json::Value>(broken).is_ok()
        || private_read_optional(directory, name, MAX_PRIVATE_JSON_BYTES)?.as_deref()
            != Some(broken)
    {
        return Err(failure());
    }
    let fd = private_directory_fd(directory)?;
    let quarantine = format!(".quarantined-{name}-{}", Uuid::new_v4().simple());
    rfs::renameat_with(&fd, name, &fd, &quarantine, RenameFlags::NOREPLACE)
        .map_err(|_| failure())?;
    rfs::fsync(&fd).map_err(|_| failure())
}

fn private_read_optional(
    directory: &Path,
    name: &str,
    maximum: u64,
) -> Result<Option<Vec<u8>>, ImageGatewayError> {
    validate_private_directory(directory)?;
    let mut file = match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(directory.join(name))
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(failure()),
    };
    let metadata = file.metadata().map_err(|_| failure())?;
    if !metadata.is_file()
        || metadata.mode() & 0o077 != 0
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.len() > maximum
    {
        return Err(failure());
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| failure())?;
    if bytes.len() as u64 != metadata.len() {
        return Err(failure());
    }
    Ok(Some(bytes))
}

#[cfg(test)]
mod tests;
