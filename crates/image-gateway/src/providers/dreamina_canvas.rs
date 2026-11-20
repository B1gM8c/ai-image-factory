//! Pinned official Canvas CLI transport for a single Seedream 5.5 text image.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use image_cli_runtime::{
    CommandSpec, ExclusiveAttemptWorkspace, FreshOutputDirectory, NoopSpawnObserver,
    ProcessCompletion, TokioProcessBackend, VerifiedExecutable, WorkingDirectory,
};
use image_provider_contracts::{OfficialParamsContract, OfficialParamsKind, OperationDescriptor};
use image_provider_dreamina_cli::DREAMINA_IMAGE_GENERATION_OPERATION_V1;
use image_provider_sdk::{
    ArtifactMetadata, ArtifactSink, CanonicalCommandPayload, Completed, EffectCertainty,
    OutputSlot, PendingOperation, PollObservation, ProviderFailure, ProviderFailureClass,
    RemoteOperationRef, RetryDirective, SingleOutputCommand,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::provider_tasks::{
    ProviderCapacityReconciliationLease, ProviderCapacityReconciliationState,
    ProviderCapacityTerminalState,
};
use crate::providers::dreamina_cli::{
    ProviderCapacityQueryDriver, ProviderCapacityQueryError, ProviderCapacityQueryObservation,
};
use crate::{
    executor::{ExecutorLaunchContext, ExecutorSubmissionLease},
    media_inputs::{MediaInputKind, probe_input},
    provider_tasks::{
        GatedCliCommand, GatedCliReceiptEvidence, GatedCliSubmitCodec,
        ProviderAccountHomeCapability, ProviderExecutionContext, ProviderPollDriver,
        ProviderPollDriverCall, ProviderRuntimeProfile, ProviderSubmitDriver,
        ProviderSubmitDriverCall, ProviderSubmitIntent, ProviderSubmitProjectionError,
        ProviderSubmitProjector, ProviderSubmitRecoveryLease, SUBMIT_RECEIPT_RECOVERY_PENDING,
    },
};

pub const CANVAS_PROVIDER_ID: &str = "dreamina-canvas";
pub const CANVAS_IMAGE_MODEL: &str = "dreamina-canvas-image-5.5";
pub const CANVAS_SUBMIT_COMMAND_SCHEMA: &str = "dreamina-canvas.submit.v1";
pub const CANVAS_ADAPTER_REVISION: &str = "dreamina-canvas.remote-task.v1";
pub const CANVAS_EXECUTABLE_SHA256: &str =
    "0641c18766d57df52d14c8af93ed9fb156e4e5b78ca66272ae3a6b600f8223a7";
pub const CANVAS_IMAGE_GENERATION_OPERATION: OperationDescriptor = OperationDescriptor {
    command_schema: CANVAS_SUBMIT_COMMAND_SCHEMA,
    descriptor_revision: "dreamina-canvas/images.generations/v1",
    official_params: OfficialParamsContract {
        kind: OfficialParamsKind::DreaminaCli,
        schema_id: "dreamina-canvas/seedream-5.5-text2image-v1",
        passthrough_allowed: false,
    },
    ..DREAMINA_IMAGE_GENERATION_OPERATION_V1
};

const VENDOR_MODEL: &str = "seedream_5.5";
const VENDOR_RESOLUTION: &str = "1.5K";
const CREDIT_CEILING: &str = "4";
const IMAGE_POLL_MS: u64 = 5_000;
const MAX_IMAGE_BYTES: u64 = 32 * 1024 * 1024;
const RATIOS: &[&str] = &["1:1", "3:4", "16:9", "4:3", "9:16", "2:3", "3:2", "21:9"];

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanvasImagePlan {
    schema_version: u8,
    operation: String,
    model_version: String,
    resolution_type: String,
    generate_num: u8,
    ratio: String,
    prompt: String,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "Canvas requires a single Seedream 5.5 text image at 1.5K, a supported ratio, and plain text"
)]
pub struct CanvasImagePlanError;

impl CanvasImagePlan {
    pub fn new(prompt: String, ratio: &str) -> Result<Self, CanvasImagePlanError> {
        let plan = Self {
            schema_version: 1,
            operation: "text2image".to_owned(),
            model_version: "5.5".to_owned(),
            resolution_type: "1.5k".to_owned(),
            generate_num: 1,
            ratio: ratio.to_owned(),
            prompt,
        };
        plan.validate()?;
        Ok(plan)
    }

    pub fn ratio(&self) -> &str {
        &self.ratio
    }

    fn validate(&self) -> Result<(), CanvasImagePlanError> {
        // Canvas interprets {{...}} as project/resource references. This route is
        // deliberately text-only and owns no access to customer Canvas projects.
        if self.schema_version != 1
            || self.operation != "text2image"
            || self.model_version != "5.5"
            || self.resolution_type != "1.5k"
            || self.generate_num != 1
            || !RATIOS.contains(&self.ratio.as_str())
            || self.prompt.trim().is_empty()
            || self.prompt.encode_utf16().count() > 5000
            || self.prompt.contains('\0')
            || self.prompt.contains("{{")
        {
            return Err(CanvasImagePlanError);
        }
        Ok(())
    }

    pub(crate) fn from_command(bytes: &[u8]) -> Result<Self, CanvasImagePlanError> {
        let plan: Self = serde_json::from_slice(bytes).map_err(|_| CanvasImagePlanError)?;
        plan.validate()?;
        Ok(plan)
    }
}

pub struct CanvasImagePayload {
    source_sha256: String,
    plan: CanvasImagePlan,
}

impl CanonicalCommandPayload for CanvasImagePayload {
    const SCHEMA_ID: &'static str = CANVAS_SUBMIT_COMMAND_SCHEMA;
    const ADAPTER_REVISION: &'static str = CANVAS_ADAPTER_REVISION;

    fn source_command_sha256(&self) -> &str {
        &self.source_sha256
    }

    fn into_canonical_bytes(self, _output: OutputSlot) -> Vec<u8> {
        serde_json::to_vec(&self.plan).expect("Canvas Image plan is serializable")
    }
}

pub struct CanvasCliProcessConfig {
    pub executable: PathBuf,
    pub executable_sha256: [u8; 32],
    pub workspace: WorkingDirectory,
    pub state_dir: PathBuf,
    pub project_id: Uuid,
    pub oauth_profile: String,
    pub wall_timeout: Duration,
    pub termination_grace: Duration,
}

#[derive(Debug, thiserror::Error)]
#[error("Canvas executable, private directories, runtime profile, or project binding is invalid")]
pub struct CanvasCliConfigError;

#[derive(Clone)]
pub struct CanvasCliConfig {
    executable: VerifiedExecutable,
    workspace: WorkingDirectory,
    state_dir: WorkingDirectory,
    home: WorkingDirectory,
    project_id: Uuid,
    oauth_profile: String,
    wall_timeout: Duration,
    termination_grace: Duration,
    execution_profile_id: Uuid,
    provider_account_id: Uuid,
    credential_sha256: String,
}

impl CanvasCliConfig {
    pub fn from_runtime_profile(
        profile: &ProviderRuntimeProfile,
        account_home: &ProviderAccountHomeCapability,
        process: CanvasCliProcessConfig,
    ) -> Result<Self, CanvasCliConfigError> {
        let descriptor = CANVAS_IMAGE_GENERATION_OPERATION;
        if profile.provider_id() != CANVAS_PROVIDER_ID
            || profile.command_schema() != descriptor.command_schema
            || profile.operation_id() != descriptor.id
            || profile.operation_descriptor_revision() != descriptor.descriptor_revision
            || profile.operation_descriptor_sha256_v1() != descriptor.canonical_sha256_v1_hex()
            || profile.idempotency_mode() != descriptor.idempotency.as_str()
            || profile.adapter_revision() != CANVAS_ADAPTER_REVISION
            || profile.max_in_flight() != 1
            || hex::encode(process.executable_sha256) != CANVAS_EXECUTABLE_SHA256
            || process.project_id.is_nil()
            || process.oauth_profile.is_empty()
            || process.oauth_profile.len() > 128
            || !process
                .oauth_profile
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
            || process.wall_timeout.is_zero()
            || process.termination_grace.is_zero()
        {
            return Err(CanvasCliConfigError);
        }
        let executable =
            VerifiedExecutable::new_with_sha256(process.executable, process.executable_sha256)
                .map_err(|_| CanvasCliConfigError)?;
        let home = account_home
            .bind(profile)
            .map_err(|_| CanvasCliConfigError)?;
        let state_dir =
            WorkingDirectory::new_private(process.state_dir).map_err(|_| CanvasCliConfigError)?;
        let roots = [home.path(), process.workspace.path(), state_dir.path()];
        for (i, left) in roots.iter().enumerate() {
            if left.to_str().is_none()
                || roots
                    .iter()
                    .skip(i + 1)
                    .any(|right| left.starts_with(right) || right.starts_with(left))
            {
                return Err(CanvasCliConfigError);
            }
        }
        Ok(Self {
            executable,
            workspace: process.workspace,
            state_dir,
            home,
            project_id: process.project_id,
            oauth_profile: process.oauth_profile,
            wall_timeout: process.wall_timeout,
            termination_grace: process.termination_grace,
            execution_profile_id: profile.execution_profile_id(),
            provider_account_id: profile.provider_account_id(),
            credential_sha256: profile.credential_auth_sha256().to_owned(),
        })
    }

    fn matches(&self, account_id: Uuid, context: &ProviderExecutionContext) -> bool {
        account_id == self.provider_account_id
            && context.execution_profile_id() == self.execution_profile_id
            && context.credential_auth_sha256() == self.credential_sha256
            && context.command_schema() == CANVAS_SUBMIT_COMMAND_SCHEMA
            && context.adapter_revision() == CANVAS_ADAPTER_REVISION
            && context.operation_id() == CANVAS_IMAGE_GENERATION_OPERATION.id
            && context.operation_descriptor_sha256_v1()
                == CANVAS_IMAGE_GENERATION_OPERATION.canonical_sha256_v1_hex()
            && context.model() == CANVAS_IMAGE_MODEL
    }

    fn environment(&self) -> BTreeMap<String, String> {
        let environment = BTreeMap::from([
            (
                "HOME".to_owned(),
                self.home
                    .path()
                    .to_str()
                    .expect("validated HOME")
                    .to_owned(),
            ),
            (
                "DREAMINA_CANVAS_STATE_DIR".to_owned(),
                self.state_dir
                    .path()
                    .to_str()
                    .expect("validated state directory")
                    .to_owned(),
            ),
        ]);
        #[cfg(target_os = "linux")]
        let environment = {
            let mut environment = environment;
            environment.insert(
                "DBUS_SESSION_BUS_ADDRESS".to_owned(),
                image_provider_dreamina_cli::dreamina_secret_service_bus_address(self.home.path()),
            );
            environment
        };
        environment
    }

    pub async fn verify_authenticated(&self) -> Result<(), CanvasCliConfigError> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Account {
            user_id: u64,
        }

        let mut arguments = self.arguments();
        arguments.extend(["auth".into(), "account".into()]);
        let result = self
            .observe(self.workspace.clone(), arguments)
            .await
            .map_err(|_| CanvasCliConfigError)?;
        let account: Account = response(result.stdout.bytes()).ok_or(CanvasCliConfigError)?;
        let fingerprint = hex::encode(Sha256::digest(format!(
            "ai-image-factory/dreamina-canvas/oauth-user/v1:{}",
            account.user_id
        )));
        if account.user_id != 0 && fingerprint == self.credential_sha256 {
            Ok(())
        } else {
            Err(CanvasCliConfigError)
        }
    }

    fn arguments(&self) -> Vec<String> {
        vec![
            "--format=json".into(),
            "--non-interactive".into(),
            format!("--profile={}", self.oauth_profile),
        ]
    }

    fn pending(&self, submission: Uuid) -> Result<PendingOperation, ProviderFailure> {
        let operation = RemoteOperationRef::new(
            CANVAS_PROVIDER_ID,
            submission.to_string(),
            format!("{}:{submission}", self.project_id),
        )
        .map_err(|_| unknown("canvas_submit_receipt_invalid"))?;
        Ok(PendingOperation::new(operation, None, Some(IMAGE_POLL_MS)))
    }

    fn status_arguments(&self, submission: Uuid) -> Vec<String> {
        let mut arguments = self.arguments();
        arguments.extend([
            "operation".into(),
            "status".into(),
            submission.to_string(),
            format!("--project-id={}", self.project_id),
        ]);
        arguments
    }

    async fn observe(
        &self,
        workspace: WorkingDirectory,
        arguments: Vec<String>,
    ) -> Result<ProcessCompletion, ProviderFailure> {
        let mut command = CommandSpec::new_receipt(
            self.executable.clone(),
            workspace,
            self.wall_timeout,
            self.termination_grace,
        )
        .map_err(|_| read_failure("canvas_observation_config_invalid"))?
        .require_directory(self.home.clone())
        .require_directory(self.state_dir.clone());
        for (key, value) in self.environment() {
            command = command
                .env(key, value)
                .map_err(|_| read_failure("canvas_observation_config_invalid"))?;
        }
        for argument in arguments {
            command = command
                .arg(argument)
                .map_err(|_| read_failure("canvas_observation_config_invalid"))?;
        }
        let completion = TokioProcessBackend
            .execute_process(&command, &mut NoopSpawnObserver)
            .await
            .map_err(|_| read_failure("canvas_observation_process_failed"))?;
        if !completion.status.success()
            || completion.stdout.is_truncated()
            || completion.stderr.is_truncated()
        {
            return Err(read_failure("canvas_observation_receipt_invalid"));
        }
        Ok(completion)
    }
}

#[derive(Clone)]
pub struct CanvasCliSubmitCodec {
    config: CanvasCliConfig,
}

impl CanvasCliSubmitCodec {
    pub fn new(config: CanvasCliConfig) -> Self {
        Self { config }
    }
}

impl GatedCliSubmitCodec for CanvasCliSubmitCodec {
    type Payload = CanvasImagePayload;

    fn provider_id(&self) -> &'static str {
        CANVAS_PROVIDER_ID
    }

    fn command(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
        command: &SingleOutputCommand<Self::Payload>,
        workspace: &WorkingDirectory,
    ) -> Result<GatedCliCommand, ProviderFailure> {
        if intent.provider_id != CANVAS_PROVIDER_ID
            || !self.config.matches(intent.provider_account_id, context)
            || intent.submission_id.is_nil()
            || intent.output_index != 0
            || intent.output_total != 1
            || command.output().index() != 0
            || command.output().total() != 1
            || workspace.path().parent() != Some(self.config.workspace.path())
        {
            return Err(rejected("canvas_submit_binding_mismatch"));
        }
        let plan = CanvasImagePlan::from_command(command.canonical_payload())
            .map_err(|_| rejected("canvas_submit_command_invalid"))?;
        let arguments = submit_arguments(
            self.config.arguments(),
            &plan,
            self.config.project_id,
            intent.submission_id,
        );
        GatedCliCommand::new(
            self.config.executable.path(),
            CANVAS_EXECUTABLE_SHA256,
            workspace.path(),
            arguments,
            self.config.environment(),
            Vec::new(),
            self.config.wall_timeout,
            self.config.termination_grace,
        )
        .map_err(|_| rejected("canvas_submit_command_invalid"))
    }

    fn decode_receipt(
        &self,
        intent: &ProviderSubmitIntent,
        _command: &SingleOutputCommand<Self::Payload>,
        stdout: &[u8],
    ) -> Result<PendingOperation, ProviderFailure> {
        validate_submit(stdout, self.config.project_id, intent.submission_id)
            .ok_or_else(|| unknown("canvas_submit_receipt_invalid"))?;
        self.config.pending(intent.submission_id)
    }

    async fn recover_receipt(
        &self,
        call: &ProviderSubmitDriverCall<Self::Payload>,
        evidence: Arc<GatedCliReceiptEvidence>,
    ) -> Result<Option<PendingOperation>, ProviderFailure> {
        let awaiting = || unknown(SUBMIT_RECEIPT_RECOVERY_PENDING);
        let workspace = WorkingDirectory::new_private(evidence.command().working_directory())
            .map_err(|_| awaiting())?;
        let projected = self
            .command(
                call.intent(),
                call.execution_context(),
                call.command(),
                &workspace,
            )
            .map_err(|_| awaiting())?;
        if evidence.execution_binding_sha256()
            != call.execution_context().execution_binding_sha256()
            || evidence.command() != &projected
        {
            return Err(awaiting());
        }
        let submission = call.intent().submission_id;
        let guard = call.guard_credentials().await.map_err(|_| awaiting())?;
        let completion = self
            .config
            .observe(workspace, self.config.status_arguments(submission))
            .await;
        drop(guard);
        let completion = completion.map_err(|_| awaiting())?;
        let bytes = completion.stdout.bytes();
        let status =
            parse_status(bytes, self.config.project_id, submission).ok_or_else(awaiting)?;
        if !status.accepted() {
            return Err(awaiting());
        }
        let observation = serde_json::to_vec(&serde_json::json!({
            "gate_request_sha256": evidence.request_sha256(),
            "query_stdout_sha256": hex::encode(Sha256::digest(bytes)),
            "project_id": self.config.project_id,
            "submit_id": submission,
            "state": status.state,
        }))
        .map_err(|_| awaiting())?;
        evidence
            .publish(
                &format!(
                    "process-receipt-recovery-canvas-{}.json",
                    Uuid::new_v4().simple()
                ),
                &observation,
            )
            .map_err(|_| awaiting())?;
        self.config.pending(submission).map(Some)
    }
}

impl<D: ProviderSubmitDriver<Payload = CanvasImagePayload>> ProviderSubmitProjector<D>
    for CanvasCliSubmitCodec
{
    fn project_fresh(
        &self,
        lease: &ExecutorSubmissionLease,
        context: &ExecutorLaunchContext,
    ) -> Result<SingleOutputCommand<CanvasImagePayload>, ProviderSubmitProjectionError> {
        if lease.provider_id != CANVAS_PROVIDER_ID
            || lease.output_index != context.output_index()
            || lease.command_hash != context.command_hash()
            || lease.command_schema != CANVAS_SUBMIT_COMMAND_SCHEMA
            || context.command_schema() != CANVAS_SUBMIT_COMMAND_SCHEMA
            || lease.adapter_revision != CANVAS_ADAPTER_REVISION
            || !context.inputs().is_empty()
        {
            return Err(ProviderSubmitProjectionError::ContractMismatch);
        }
        project(
            context.command_json(),
            context.command_hash(),
            lease.output_index == 0,
        )
    }

    fn project_recovery(
        &self,
        lease: &ProviderSubmitRecoveryLease,
    ) -> Result<SingleOutputCommand<CanvasImagePayload>, ProviderSubmitProjectionError> {
        let context = lease.context();
        if lease.intent.provider_id != CANVAS_PROVIDER_ID
            || !self
                .config
                .matches(lease.intent.provider_account_id, context)
        {
            return Err(ProviderSubmitProjectionError::ContractMismatch);
        }
        project(
            lease.command_json(),
            context.command_hash(),
            lease.intent.output_index == 0 && lease.intent.output_total == 1,
        )
    }
}

fn project(
    value: &serde_json::Value,
    source_sha256: &str,
    single_output: bool,
) -> Result<SingleOutputCommand<CanvasImagePayload>, ProviderSubmitProjectionError> {
    if !single_output {
        return Err(ProviderSubmitProjectionError::OutputOutOfRange);
    }
    let source = serde_json::to_vec(value)
        .map_err(|_| ProviderSubmitProjectionError::InvalidSourceCommand)?;
    let plan = CanvasImagePlan::from_command(&source)
        .map_err(|_| ProviderSubmitProjectionError::InvalidSourceCommand)?;
    let output =
        OutputSlot::new(0, 1).map_err(|_| ProviderSubmitProjectionError::OutputOutOfRange)?;
    SingleOutputCommand::new(
        output,
        CanvasImagePayload {
            source_sha256: source_sha256.to_owned(),
            plan,
        },
    )
    .map_err(|_| ProviderSubmitProjectionError::InvalidSourceCommand)
}

fn derived_uuid(submission: Uuid, domain: &[u8]) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(b"factory/dreamina-canvas/v1\0");
    digest.update(domain);
    digest.update(submission.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn node_id(submission: Uuid) -> String {
    const CROCKFORD_BASE32: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";
    let digest = Sha256::digest(
        [
            b"factory/dreamina-canvas/node/v1\0".as_slice(),
            submission.as_bytes(),
        ]
        .concat(),
    );
    let hash = u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 prefix is eight octets"),
    );
    let mut id = String::from("node_");
    for shift in (0..10).rev() {
        id.push(char::from(
            CROCKFORD_BASE32[((hash >> (shift * 5)) & 31) as usize],
        ));
    }
    id
}

fn submit_arguments(
    mut arguments: Vec<String>,
    plan: &CanvasImagePlan,
    project: Uuid,
    submit: Uuid,
) -> Vec<String> {
    arguments.extend([
        "node".into(),
        "create".into(),
        "image".into(),
        format!("--project-id={project}"),
        format!("--node-id={}", node_id(submit)),
        format!("--update-id={}", derived_uuid(submit, b"update")),
        "--mode=t2i".into(),
        format!("--model={VENDOR_MODEL}"),
        format!("--resolution={VENDOR_RESOLUTION}"),
        format!("--ratio={}", plan.ratio()),
        "--count=1".into(),
        format!("--prompt={}", plan.prompt),
        "--title=Factory Seedream 5.5".into(),
        "--run".into(),
        format!("--submit-id={submit}"),
        format!("--credit-ceiling={CREDIT_CEILING}"),
    ]);
    arguments
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Response<T> {
    schema_version: String,
    ok: bool,
    data: T,
}

fn response<T: DeserializeOwned>(bytes: &[u8]) -> Option<T> {
    let response: Response<T> = serde_json::from_slice(bytes).ok()?;
    (response.ok && response.schema_version == "2").then_some(response.data)
}

#[derive(Deserialize)]
struct Submitted {
    submission: Submission,
    node: SubmittedNode,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubmittedNode {
    node_id: String,
    project_id: Option<Uuid>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Submission {
    submit_id: Uuid,
    state: String,
}

fn validate_submit(bytes: &[u8], project: Uuid, submit: Uuid) -> Option<()> {
    let created: Submitted = response(bytes)?;
    (created.submission.submit_id == submit
        && created.submission.state.eq_ignore_ascii_case("accepted")
        && created.node.node_id == node_id(submit)
        && created.node.project_id == Some(project))
    .then_some(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OperationStatus {
    operation_ref: Uuid,
    state: String,
    resources: Vec<OperationResource>,
    submission_outcome: Option<String>,
    submission: Option<OperationSubmission>,
}

#[derive(Deserialize)]
struct OperationSubmission {
    state: String,
    resubmittable: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OperationResource {
    resource_id: Uuid,
    source_project_id: Option<Uuid>,
    state: String,
}

fn parse_status(bytes: &[u8], project: Uuid, submission: Uuid) -> Option<OperationStatus> {
    let status: OperationStatus = response(bytes)?;
    (status.operation_ref == submission
        && status.resources.len() <= 1
        && status
            .resources
            .iter()
            .all(|r| !r.resource_id.is_nil() && r.source_project_id == Some(project)))
    .then_some(status)
}

impl OperationStatus {
    fn accepted(&self) -> bool {
        // Only positive server evidence can recover dispatch. Unknown or not found
        // never means the operation was unaccepted or that it can be repeated.
        if self
            .submission_outcome
            .as_deref()
            .is_some_and(|outcome| outcome != "accepted")
            || self
                .submission
                .as_ref()
                .is_some_and(|submission| submission.resubmittable == Some(true))
        {
            return false;
        }
        self.has_matching_result("succeeded")
            || self.confirmed_terminal() == Some(ProviderCapacityTerminalState::Failed)
            || self.in_progress() && !self.resources.is_empty()
    }

    fn in_progress(&self) -> bool {
        ["accepted", "queued", "running", "pending"]
            .iter()
            .any(|state| self.state.eq_ignore_ascii_case(state))
    }

    fn has_matching_result(&self, state: &str) -> bool {
        self.state.eq_ignore_ascii_case(state)
            && self.resources.len() == 1
            && self.resources[0].state.eq_ignore_ascii_case(state)
    }

    fn confirmed_terminal(&self) -> Option<ProviderCapacityTerminalState> {
        if self.submission_outcome.as_deref() != Some("accepted")
            || !self.submission.as_ref().is_some_and(|submission| {
                submission.state == "completed" && submission.resubmittable == Some(false)
            })
        {
            return None;
        }
        if self.has_matching_result("succeeded") {
            Some(ProviderCapacityTerminalState::Succeeded)
        } else if self.has_matching_result("failed") {
            Some(ProviderCapacityTerminalState::Failed)
        } else {
            None
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DownloadReceipt {
    resource_id: Uuid,
    path: PathBuf,
    size: u64,
    sha256: String,
    content_type: String,
}

fn verify_download(
    output: FreshOutputDirectory,
    expected_path: &Path,
    resource: Uuid,
    receipt: DownloadReceipt,
) -> Result<(Vec<u8>, &'static str), ProviderFailure> {
    let transient = || read_failure("canvas_artifact_unavailable");
    if receipt.resource_id != resource
        || receipt.path != expected_path
        || !(1..=MAX_IMAGE_BYTES).contains(&receipt.size)
    {
        return Err(transient());
    }
    let (sealed, bytes) = output
        .seal_single_file_to_sink(Vec::new())
        .map_err(|_| transient())?;
    if sealed.relative_filename != Path::new("result")
        || sealed.byte_size != receipt.size
        || sealed.sha256_hex != receipt.sha256
    {
        return Err(transient());
    }
    let probe = probe_input(&bytes, MediaInputKind::Image).map_err(|_| transient())?;
    if probe.media_type() != receipt.content_type {
        return Err(transient());
    }
    Ok((bytes, probe.media_type()))
}

pub struct CanvasCliPollDriver {
    config: CanvasCliConfig,
    attempts: ExclusiveAttemptWorkspace,
}

pub struct CanvasCliCapacityQueryDriver {
    config: CanvasCliConfig,
    attempts: ExclusiveAttemptWorkspace,
}

impl CanvasCliCapacityQueryDriver {
    pub fn new(
        mut config: CanvasCliConfig,
        workspace: WorkingDirectory,
    ) -> Result<Self, CanvasCliConfigError> {
        let attempts = ExclusiveAttemptWorkspace::acquire(&workspace, ".canvas-capacity-")
            .map_err(|_| CanvasCliConfigError)?;
        config.wall_timeout = config.wall_timeout.min(Duration::from_secs(20));
        Ok(Self { config, attempts })
    }
}

impl ProviderCapacityQueryDriver for CanvasCliCapacityQueryDriver {
    async fn query(
        &self,
        lease: &ProviderCapacityReconciliationLease,
    ) -> Result<ProviderCapacityQueryObservation, ProviderCapacityQueryError> {
        let reconciliation = &lease.reconciliation;
        let remote_id = lease
            .remote_operation_id()
            .ok_or(ProviderCapacityQueryError::MissingRemoteId)?;
        let submission = reconciliation.submission_id;
        if reconciliation.provider_id != CANVAS_PROVIDER_ID
            || reconciliation.state != ProviderCapacityReconciliationState::Active
            || reconciliation.reconciliation_owner.as_deref()
                != Some(lease.reconciliation_owner.as_str())
            || reconciliation.reconciliation_lease_epoch != lease.reconciliation_lease_epoch
            || reconciliation.evidence_revision != lease.claimed_evidence_revision
            || !self
                .config
                .matches(reconciliation.provider_account_id, lease.context())
            || remote_id != format!("{}:{submission}", self.config.project_id)
        {
            return Err(ProviderCapacityQueryError::BindingMismatch);
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
            .ok_or(ProviderCapacityQueryError::LeaseExpired)?;
        if lease.reconciliation_lease_expires_at_ms <= now {
            return Err(ProviderCapacityQueryError::LeaseExpired);
        }
        let unavailable = |_| ProviderCapacityQueryError::QueryUnavailable;
        let attempt = self.attempts.create_attempt().map_err(unavailable)?;
        let workspace = attempt.working_directory().map_err(unavailable)?;
        let completion = self
            .config
            .observe(workspace, self.config.status_arguments(submission))
            .await
            .map_err(|_| ProviderCapacityQueryError::QueryUnavailable)?;
        let stdout = completion.stdout.bytes();
        let status = parse_status(stdout, self.config.project_id, submission)
            .ok_or(ProviderCapacityQueryError::ReceiptMismatch)?;
        capacity_observation(status, stdout)
    }
}

fn capacity_observation(
    status: OperationStatus,
    stdout: &[u8],
) -> Result<ProviderCapacityQueryObservation, ProviderCapacityQueryError> {
    if let Some(terminal_state) = status.confirmed_terminal() {
        // Capacity alone is resolved. The upstream operation receipt has no
        // billing evidence or validated Factory artifact.
        Ok(ProviderCapacityQueryObservation::Terminal {
            terminal_state,
            event_identity: format!(
                "canvas-capacity-query:{}",
                hex::encode(Sha256::digest(stdout))
            ),
        })
    } else if status.in_progress() {
        Ok(ProviderCapacityQueryObservation::Pending {
            next_poll_after_ms: IMAGE_POLL_MS,
            progress: image_provider_sdk::ProviderTaskProgress {
                provider_status: status.state.to_ascii_lowercase(),
                queue_position: None,
                queue_total: None,
                estimated_remaining_seconds: None,
            },
        })
    } else {
        Err(ProviderCapacityQueryError::TerminalUnclassified)
    }
}

impl CanvasCliPollDriver {
    pub fn new(config: CanvasCliConfig) -> Result<Self, CanvasCliConfigError> {
        let attempts = ExclusiveAttemptWorkspace::acquire(&config.workspace, ".canvas-poll-")
            .map_err(|_| CanvasCliConfigError)?;
        Ok(Self { config, attempts })
    }

    async fn fetch_output<S: ArtifactSink>(
        &self,
        resource: Uuid,
        sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        let transient = || read_failure("canvas_artifact_unavailable");
        let attempt = self.attempts.create_attempt().map_err(|_| transient())?;
        let workspace = attempt.working_directory().map_err(|_| transient())?;
        let output =
            FreshOutputDirectory::new(&workspace, MAX_IMAGE_BYTES).map_err(|_| transient())?;
        let file = workspace.path().join("result");
        let path = file.to_str().ok_or_else(transient)?;
        let mut arguments = self.config.arguments();
        arguments.extend([
            "resource".into(),
            "download".into(),
            resource.to_string(),
            format!("--project-id={}", self.config.project_id),
            format!("--output={path}"),
        ]);
        let completion = self.config.observe(workspace, arguments).await?;
        let receipt: DownloadReceipt = response(completion.stdout.bytes()).ok_or_else(transient)?;
        let bytes =
            tokio::task::spawn_blocking(move || verify_download(output, &file, resource, receipt))
                .await
                .map_err(|_| transient())??;
        for chunk in bytes.0.chunks(64 * 1024) {
            sink.write_chunk(chunk).await.map_err(|_| transient())?;
        }
        let manifest = sink
            .finalize(ArtifactMetadata {
                media_type: bytes.1,
            })
            .await
            .map_err(|_| transient())?;
        Ok(PollObservation::Completed(Completed::new(manifest, None)))
    }
}

impl ProviderPollDriver for CanvasCliPollDriver {
    fn provider_id(&self) -> &'static str {
        CANVAS_PROVIDER_ID
    }

    async fn poll<S: ArtifactSink>(
        &self,
        call: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        let invalid = || unknown("canvas_poll_binding_mismatch");
        let operation = call.operation();
        let (project, submit) = operation
            .operation_id()
            .split_once(':')
            .ok_or_else(invalid)?;
        let submit = Uuid::parse_str(submit).map_err(|_| invalid())?;
        if operation.provider_id() != CANVAS_PROVIDER_ID
            || project != self.config.project_id.to_string()
            || operation.submission_id() != submit.to_string()
            || !self
                .config
                .matches(call.provider_account_id(), call.execution_context())
        {
            return Err(invalid());
        }
        let attempt = self
            .attempts
            .create_attempt()
            .map_err(|_| read_failure("canvas_poll_workspace_unavailable"))?;
        let workspace = attempt
            .working_directory()
            .map_err(|_| read_failure("canvas_poll_workspace_unavailable"))?;
        let observation = self
            .config
            .observe(workspace, self.config.status_arguments(submit))
            .await?;
        let status = parse_status(observation.stdout.bytes(), self.config.project_id, submit)
            .ok_or_else(|| read_failure("canvas_poll_receipt_invalid"))?;
        if status.has_matching_result("succeeded") && status.accepted() {
            self.fetch_output(status.resources[0].resource_id, sink)
                .await
        } else if status.confirmed_terminal() == Some(ProviderCapacityTerminalState::Failed) {
            // An accepted job may have been charged. Do not turn such a result
            // into a pre-submit refusal or a new generation.
            Err(unknown("canvas_accepted_generation_failed"))
        } else {
            Ok(PollObservation::Pending {
                next_poll_after_ms: Some(IMAGE_POLL_MS),
            })
        }
    }
}

fn rejected(code: &'static str) -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Permanent,
        code,
        EffectCertainty::NoRemoteEffect,
        RetryDirective::Never,
    )
    .expect("valid fixed Canvas error")
}

fn unknown(code: &'static str) -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Ambiguous,
        code,
        EffectCertainty::UnknownRemoteEffect,
        RetryDirective::Never,
    )
    .expect("valid fixed Canvas error")
}

fn read_failure(code: &'static str) -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Transient,
        code,
        EffectCertainty::NoRemoteEffect,
        RetryDirective::Backoff,
    )
    .expect("valid fixed Canvas error")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn ids() -> (Uuid, Uuid, Uuid) {
        (
            Uuid::from_u128(10),
            Uuid::from_u128(20),
            Uuid::from_u128(30),
        )
    }

    fn envelope(data: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({"schemaVersion":"2","ok":true,"data":data})).unwrap()
    }

    #[test]
    fn route_requires_exact_model_workflow_and_no_reference_syntax() {
        let plan = CanvasImagePlan::new("单张蓝色剪纸".to_owned(), "9:16").unwrap();
        let value = serde_json::to_value(plan).unwrap();
        assert_eq!(value["model_version"], "5.5");
        assert_eq!(value["resolution_type"], "1.5k");
        assert_eq!(value["operation"], "text2image");
        assert_eq!(value["generate_num"], 1);
        for (field, invalid) in [
            ("model_version", json!("5.0")),
            ("operation", json!("image2image")),
            ("generate_num", json!(2)),
            ("resolution_type", json!("2k")),
            ("ratio", json!("17:9")),
            ("prompt", json!("{{res:abc}}")),
            ("prompt", json!("\0")),
        ] {
            let mut value = value.clone();
            value[field] = invalid;
            assert!(CanvasImagePlan::from_command(&serde_json::to_vec(&value).unwrap()).is_err());
        }
        assert!(CanvasImagePlan::new(" ".repeat(5_001), "1:1").is_err());
        assert!(CanvasImagePlan::new("🟦".repeat(2_501), "1:1").is_err());
        let mut value = value;
        value["reference_images"] = json!(["fixture"]);
        assert!(CanvasImagePlan::from_command(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn paid_argv_uses_stable_distinct_bound_ids_and_a_fixed_credit_ceiling() {
        let (project, submit, another) = ids();
        let prompt = "--another-flag\n中文".to_owned();
        let plan = CanvasImagePlan::new(prompt.clone(), "9:16").unwrap();
        let actual = |submit| submit_arguments(Vec::new(), &plan, project, submit);
        let argv = actual(submit);
        assert_eq!(argv, actual(submit));
        assert_ne!(argv, actual(another));
        assert_eq!(&argv[..3], ["node", "create", "image"]);
        assert!(argv.contains(&format!("--prompt={prompt}")));
        assert!(argv.contains(&format!("--project-id={project}")));
        assert!(argv.contains(&format!("--submit-id={submit}")));
        assert!(argv.contains(&"--credit-ceiling=4".to_owned()));
        assert!(
            !argv
                .iter()
                .any(|arg| matches!(arg.as_str(), "--wait" | "--yes"))
        );
        assert_ne!(node_id(submit), node_id(another));
        assert_eq!(node_id(submit).len(), 15);
        assert!(argv.contains(&format!("--node-id={}", node_id(submit))));
        assert_ne!(derived_uuid(submit, b"update"), submit);
    }

    #[test]
    fn submit_requires_exact_bootstrapped_task_and_node() {
        let (project, submit, other) = ids();
        let payload = json!({
            "node":{"nodeId": node_id(submit),"projectId": project},
            "submission":{"submitId":submit,"state":"accepted"}
        });
        assert!(validate_submit(&envelope(payload.clone()), project, submit).is_some());
        assert!(validate_submit(&envelope(payload.clone()), project, other).is_none());
        assert!(validate_submit(&envelope(payload.clone()), other, submit).is_none());
        let mut altered = payload.clone();
        altered["submission"]["state"] = json!("awaiting_confirmation");
        assert!(validate_submit(&envelope(altered), project, submit).is_none());
        let mut altered = payload;
        altered["node"]["nodeId"] = json!("node_unknown");
        assert!(validate_submit(&envelope(altered), project, submit).is_none());
    }

    #[test]
    fn status_requires_same_operation_and_positive_per_resource_evidence() {
        let (project, submit, resource) = ids();
        let payload = json!({
            "operationRef":submit,"state":"succeeded",
            "resources":[{"resourceId":resource,"sourceProjectId":project,"state":"succeeded"}]
        });
        assert!(
            parse_status(&envelope(payload.clone()), project, submit)
                .unwrap()
                .accepted()
        );
        assert!(parse_status(&envelope(payload.clone()), project, resource).is_none());
        assert!(parse_status(&envelope(payload.clone()), submit, submit).is_none());
        for state in ["not_found", "awaiting_confirmation", "failed", "unknown"] {
            let mut payload = payload.clone();
            payload["state"] = json!(state);
            assert!(
                !parse_status(&envelope(payload), project, submit)
                    .unwrap()
                    .accepted()
            );
        }
        let mut incomplete = payload.clone();
        incomplete["resources"] = json!([]);
        assert!(
            !parse_status(&envelope(incomplete), project, submit)
                .unwrap()
                .accepted()
        );
        let mut multiple = payload;
        multiple["resources"].as_array_mut().unwrap().push(json!({
            "resourceId":project,"sourceProjectId":project,"state":"succeeded"
        }));
        assert!(parse_status(&envelope(multiple), project, submit).is_none());
        assert!(
            response::<Value>(br#"{"ok":true,"ok":false,"schemaVersion":"2","data":{}}"#).is_none()
        );
    }

    #[test]
    fn accepted_terminal_receipts_reattach_and_release_only_capacity() {
        let (project, submit, resource) = ids();
        for (state, expected) in [
            ("failed", ProviderCapacityTerminalState::Failed),
            ("succeeded", ProviderCapacityTerminalState::Succeeded),
        ] {
            let payload = json!({
                "operationRef": submit,
                "state": state,
                "submissionOutcome": "accepted",
                "submission": {"state": "completed", "resubmittable": false},
                "resources": [{
                    "resourceId": resource, "sourceProjectId": project, "state": state
                }]
            });
            let stdout = envelope(payload.clone());
            let status = parse_status(&stdout, project, submit).unwrap();
            assert!(status.accepted());
            assert_eq!(
                capacity_observation(status, &stdout).unwrap(),
                ProviderCapacityQueryObservation::Terminal {
                    terminal_state: expected,
                    event_identity: format!(
                        "canvas-capacity-query:{}",
                        hex::encode(Sha256::digest(&stdout))
                    ),
                }
            );

            for (field, value) in [
                ("submissionOutcome", json!("rejected")),
                (
                    "submission",
                    json!({"state":"completed", "resubmittable":true}),
                ),
                ("state", json!("awaiting_confirmation")),
                ("resources", json!([])),
                (
                    "resources",
                    json!([{
                        "resourceId": resource, "sourceProjectId": project, "state": "running"
                    }]),
                ),
            ] {
                let mut changed = payload.clone();
                changed[field] = value;
                let stdout = envelope(changed);
                let status = parse_status(&stdout, project, submit).unwrap();
                assert!(!status.accepted());
                assert_eq!(
                    capacity_observation(status, &stdout),
                    Err(ProviderCapacityQueryError::TerminalUnclassified)
                );
            }
        }
    }
    #[test]
    fn artifact_matches_original_resource_path_size_sha_and_decoded_content_type() {
        let mut buffer = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(3, 2)
            .write_to(&mut buffer, image::ImageFormat::Png)
            .unwrap();
        let png = buffer.into_inner();
        let (_, resource, another) = ids();
        for (field, replacement) in [
            ("", Value::Null),
            ("resourceId", json!(another)),
            ("path", json!("other")),
            ("size", json!(png.len() + 1)),
            ("sha256", json!("0".repeat(64))),
            ("contentType", json!("image/jpeg")),
        ] {
            let directory = tempfile::tempdir().unwrap();
            use std::os::unix::fs::PermissionsExt;
            let private_path = directory.path().canonicalize().unwrap();
            std::fs::set_permissions(&private_path, std::fs::Permissions::from_mode(0o700))
                .unwrap();
            let workspace = WorkingDirectory::new_private(&private_path).unwrap();
            let path = workspace.path().join("result");
            let output = FreshOutputDirectory::new(&workspace, MAX_IMAGE_BYTES).unwrap();
            std::fs::write(&path, &png).unwrap();
            let mut wire = json!({
                "resourceId": resource,
                "path": path,
                "size": png.len(),
                "sha256": hex::encode(Sha256::digest(&png)),
                "contentType": "image/png",
            });
            if !field.is_empty() {
                wire[field] = replacement;
            }
            let receipt: DownloadReceipt = response(&envelope(wire)).unwrap();
            let verified = verify_download(output, &path, resource, receipt);
            if field.is_empty() {
                assert_eq!(verified.unwrap(), (png.clone(), "image/png"));
            } else {
                assert!(verified.is_err(), "misbound {field} was accepted");
            }
        }
    }
}
