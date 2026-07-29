use async_trait::async_trait;
use image_provider_contracts::ProviderReportedCostEvidenceV1;
use serde_json::Value;
use uuid::Uuid;

use crate::admission::WorkLease;

mod codex_request;
mod codex_supervisor;
mod daemon;
mod grok_request;
mod grok_supervisor;
mod owner_guard;
mod postgres;
mod private_auth;
mod process_supervisor;
mod profile_binding;
mod provisioning;
mod runner;

pub(crate) use private_auth::read_verified_auth;

pub use codex_request::{
    CodexOutputRequest, CodexRequestProjectionError, project_codex_output_request,
};
pub use codex_supervisor::{
    CODEX_GENERATION_ADAPTER_REVISION, CodexProcessSupervisor, codex_auth_file_sha256,
    prepare_codex_auth_copy, run_codex_runner_child,
};
pub use daemon::{ExecutorDaemon, ExecutorDaemonError, ExecutorDaemonRun};
pub use grok_request::{
    GrokExecutionRequest, GrokRequestProjectionError, XAI_IMAGES_API_PROFILE,
    XAI_VIDEOS_API_PROFILE, project_grok_execution_request,
};
pub use grok_supervisor::{GrokProcessSupervisor, grok_auth_file_sha256, run_grok_runner_child};
pub use owner_guard::{ExecutorOwnerGuardError, PostgresExecutorOwnerGuard};
pub use postgres::PostgresExecutorSubmissionStore;
pub(crate) use postgres::release_capacity_allocation;
pub use process_supervisor::ExecutorProcessSupervisor;
pub use profile_binding::{
    ExecutorProfileBinding, ExecutorProfileBindingError, identify_executor_profile_binding,
};
pub use provisioning::{
    CODEX_EDIT_INLINE_ADAPTER_REVISION, CodexExecutionProfileProvisioning,
    CodexProfileProvisioningError, DreaminaExecutionProfileProvisioning,
    DreaminaProfileProvisioningError, ExecutionProfileProvisioning,
    ExecutionProfileProvisioningError, GrokExecutionProfileProvisioning,
    GrokProfileProvisioningError, ProvisionedCodexExecutionProfile,
    ProvisionedDreaminaExecutionProfile, ProvisionedExecutionProfile,
    ProvisionedGrokExecutionProfile, provision_codex_edit_execution_profile_in_transaction,
    provision_codex_execution_profile, provision_codex_execution_profile_in_transaction,
    provision_dreamina_execution_profile, provision_dreamina_execution_profile_in_transaction,
    provision_dreamina_video_execution_profile,
    provision_dreamina_video_execution_profile_in_transaction,
    provision_grok_edit_execution_profile, provision_grok_edit_execution_profile_in_transaction,
    provision_grok_execution_profile, provision_grok_execution_profile_in_transaction,
    provision_grok_video_execution_profile, provision_grok_video_execution_profile_in_transaction,
};
pub use runner::{
    DurableEvidenceRecovery, DurableRunner, DurableRunnerResult, ExecutorArtifactSink,
    JournaledDurableRunner, RunnerError, RunnerLaunchAuthority, RunnerOutcome,
    SingleOutputSupervisor, SupervisedOutput,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedExecutorSubmission {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub output_id: Uuid,
    pub job_id: Uuid,
    pub tenant_id: String,
    pub provider_id: String,
    pub model: String,
    pub work_item_id: Uuid,
    pub output_index: i32,
    pub command_schema: String,
    pub command_hash: String,
    pub execution_profile_id: Uuid,
    pub adapter_revision: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorSubmissionLease {
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub output_id: Uuid,
    pub job_id: Uuid,
    pub tenant_id: String,
    pub provider_id: String,
    pub model: String,
    pub work_item_id: Uuid,
    pub output_index: i32,
    pub command_schema: String,
    pub command_hash: String,
    pub execution_profile_id: Uuid,
    pub adapter_revision: String,
    pub executor_owner: String,
    pub executor_lease_epoch: i64,
    pub executor_lease_expires_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutorSubmissionResume {
    Leased(ExecutorSubmissionLease),
    Running(ExecutorSubmissionLease),
}

impl ExecutorSubmissionResume {
    pub fn needs_start(&self) -> bool {
        matches!(self, Self::Leased(_))
    }

    pub fn into_lease(self) -> ExecutorSubmissionLease {
        match self {
            Self::Leased(lease) | Self::Running(lease) => lease,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorClaimScope {
    pub execution_profile_id: Uuid,
    pub provider_id: String,
    pub command_schema: String,
    pub adapter_revision: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorExecutionProfile {
    pub execution_profile_id: Uuid,
    pub profile_key: String,
    pub provider_id: String,
    pub command_schema: String,
    pub operation_id: String,
    pub operation_descriptor_revision: String,
    pub operation_descriptor_sha256_v1: String,
    pub completion_mode: String,
    pub idempotency_mode: String,
    pub adapter_revision: String,
    pub credential_pool_id: Uuid,
    pub provider_account_id: Uuid,
    pub credential_ref: String,
    pub credential_revision: i64,
    pub credential_auth_sha256: String,
    pub resource_policy_id: Uuid,
    pub resource_policy_revision: i64,
    pub max_concurrency: i32,
}

impl ExecutorExecutionProfile {
    pub fn claim_scope(&self) -> ExecutorClaimScope {
        ExecutorClaimScope {
            execution_profile_id: self.execution_profile_id,
            provider_id: self.provider_id.clone(),
            command_schema: self.command_schema.clone(),
            adapter_revision: self.adapter_revision.clone(),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct ExecutorLaunchContext {
    request_id: String,
    api_profile: String,
    output_index: i32,
    command_schema: String,
    command_hash: String,
    command_json: Value,
    inputs: Vec<ExecutorInputObject>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorInputObject {
    pub(crate) blob: crate::input_blobs::InputBlobRef,
    pub(crate) role: String,
    pub(crate) index: u16,
    pub(crate) media_type: String,
}

impl ExecutorInputObject {
    pub fn new(
        blob: crate::input_blobs::InputBlobRef,
        role: impl Into<String>,
        index: u16,
        media_type: impl Into<String>,
    ) -> Option<Self> {
        let role = role.into();
        let media_type = media_type.into();
        let valid_descriptor = match role.as_str() {
            "image" => {
                index <= 15
                    && matches!(
                        media_type.as_str(),
                        "image/png" | "image/jpeg" | "image/webp"
                    )
            }
            "mask" => index == 0 && media_type == "image/png",
            _ => false,
        };
        if !valid_descriptor
            || blob.byte_size == 0
            || blob.sha256_hex.len() != 64
            || !blob
                .sha256_hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return None;
        }
        Some(Self {
            blob,
            role,
            index,
            media_type,
        })
    }

    pub fn blob(&self) -> &crate::input_blobs::InputBlobRef {
        &self.blob
    }

    pub fn role(&self) -> &str {
        &self.role
    }

    pub fn index(&self) -> u16 {
        self.index
    }

    pub fn media_type(&self) -> &str {
        &self.media_type
    }
}

impl ExecutorLaunchContext {
    pub fn new(
        request_id: impl Into<String>,
        api_profile: impl Into<String>,
        output_index: i32,
        command_schema: impl Into<String>,
        command_hash: impl Into<String>,
        command_json: Value,
    ) -> Option<Self> {
        let request_id = request_id.into();
        let api_profile = api_profile.into();
        let command_schema = command_schema.into();
        let command_hash = command_hash.into();
        if output_index < 0
            || ![&request_id, &api_profile, &command_schema]
                .into_iter()
                .all(|value| {
                    !value.is_empty()
                        && value.len() <= 1_024
                        && !value.bytes().any(|byte| byte.is_ascii_control())
                })
            || !is_sha256(&command_hash)
        {
            return None;
        }
        Some(Self {
            request_id,
            api_profile,
            output_index,
            command_schema,
            command_hash,
            command_json,
            inputs: Vec::new(),
        })
    }

    pub fn with_inputs(mut self, inputs: Vec<ExecutorInputObject>) -> Option<Self> {
        if inputs
            .iter()
            .enumerate()
            .any(|(index, input)| usize::from(input.index) != index)
        {
            return None;
        }
        self.inputs = inputs;
        Some(self)
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn api_profile(&self) -> &str {
        &self.api_profile
    }

    pub fn output_index(&self) -> i32 {
        self.output_index
    }

    pub fn command_schema(&self) -> &str {
        &self.command_schema
    }

    pub fn command_hash(&self) -> &str {
        &self.command_hash
    }

    pub fn command_json(&self) -> &Value {
        &self.command_json
    }

    pub fn inputs(&self) -> &[ExecutorInputObject] {
        &self.inputs
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExecutorArtifactAuthority {
    pub(crate) authority_id: Uuid,
    pub(crate) storage_backend: String,
    pub(crate) storage_namespace: String,
    pub(crate) object_key: String,
    pub(crate) sha256_hex: String,
    pub(crate) byte_size: u64,
    pub(crate) media_type: String,
    pub(crate) media_duration_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorResultManifest {
    pub(crate) manifest_id: Uuid,
    pub(crate) artifact_authority_id: Uuid,
    pub(crate) provider_reported_cost: Option<ProviderReportedCostEvidenceV1>,
}

impl ExecutorResultManifest {
    pub fn new(manifest_id: Uuid, artifact_authority_id: Uuid) -> Option<Self> {
        if manifest_id.is_nil()
            || artifact_authority_id.is_nil()
            || manifest_id == artifact_authority_id
        {
            return None;
        }
        Some(Self {
            manifest_id,
            artifact_authority_id,
            provider_reported_cost: None,
        })
    }

    pub fn manifest_id(&self) -> Uuid {
        self.manifest_id
    }

    pub fn artifact_authority_id(&self) -> Uuid {
        self.artifact_authority_id
    }

    pub fn provider_reported_cost(&self) -> Option<&ProviderReportedCostEvidenceV1> {
        self.provider_reported_cost.as_ref()
    }

    pub fn with_provider_reported_cost(
        mut self,
        evidence: Option<ProviderReportedCostEvidenceV1>,
    ) -> Option<Self> {
        if evidence
            .as_ref()
            .is_some_and(|evidence| evidence.validate().is_err())
        {
            return None;
        }
        self.provider_reported_cost = evidence;
        Some(self)
    }
}

const MAX_RESULT_BYTES: u64 = 256 * 1024 * 1024;

pub(crate) fn artifact_authority_is_valid(authority: &ExecutorArtifactAuthority) -> bool {
    let duration_is_valid = match authority.media_type.as_str() {
        "video/mp4" => authority
            .media_duration_ms
            .is_some_and(|duration| (1..=86_400_000).contains(&duration)),
        _ => authority.media_duration_ms.is_none(),
    };
    duration_is_valid
        && artifact_descriptor_is_valid(
            &authority.storage_backend,
            &authority.storage_namespace,
            &authority.object_key,
            &authority.sha256_hex,
            authority.byte_size,
            &authority.media_type,
        )
}

pub(crate) fn result_manifest_is_valid(manifest: &ExecutorResultManifest) -> bool {
    !manifest.manifest_id.is_nil()
        && !manifest.artifact_authority_id.is_nil()
        && manifest.manifest_id != manifest.artifact_authority_id
        && manifest
            .provider_reported_cost
            .as_ref()
            .is_none_or(|evidence| evidence.validate().is_ok())
}

pub(crate) fn artifact_descriptor_is_valid(
    storage_backend: &str,
    storage_namespace: &str,
    object_key: &str,
    sha256_hex: &str,
    byte_size: u64,
    media_type: &str,
) -> bool {
    storage_backend == "filesystem-v1"
        && !storage_namespace.is_empty()
        && storage_namespace.len() <= 1_024
        && !storage_namespace
            .bytes()
            .any(|byte| byte.is_ascii_control())
        && storage_namespace.starts_with("filesystem-v1:")
        && !object_key.is_empty()
        && object_key.len() <= 1_024
        && !object_key.bytes().any(|byte| byte.is_ascii_control())
        && is_sha256(sha256_hex)
        && (1..=MAX_RESULT_BYTES).contains(&byte_size)
        && matches!(
            media_type,
            "image/png" | "image/jpeg" | "image/webp" | "video/mp4"
        )
}

pub(crate) fn error_code_is_valid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutorSubmissionOutcome {
    Succeeded(ExecutorResultManifest),
    Failed { error_code: String },
    Uncertain { error_code: String },
}

impl ExecutorSubmissionOutcome {
    fn state(&self) -> &'static str {
        match self {
            Self::Succeeded(_) => "succeeded",
            Self::Failed { .. } => "failed",
            Self::Uncertain { .. } => "uncertain",
        }
    }

    fn error_code(&self) -> Option<&str> {
        match self {
            Self::Succeeded(_) => None,
            Self::Failed { error_code } | Self::Uncertain { error_code } => Some(error_code),
        }
    }

    fn manifest(&self) -> Option<&ExecutorResultManifest> {
        match self {
            Self::Succeeded(manifest) => Some(manifest),
            Self::Failed { .. } | Self::Uncertain { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorRunnerObservation {
    pub(crate) observation_id: Uuid,
    pub(crate) executor_execution_id: Uuid,
    pub(crate) submission_id: Uuid,
    pub(crate) outcome: ExecutorSubmissionOutcome,
}

impl ExecutorRunnerObservation {
    pub fn new(
        executor_execution_id: Uuid,
        submission_id: Uuid,
        outcome: ExecutorSubmissionOutcome,
    ) -> Option<Self> {
        if executor_execution_id.is_nil()
            || submission_id.is_nil()
            || executor_execution_id == submission_id
        {
            return None;
        }
        Some(Self {
            observation_id: executor_execution_id,
            executor_execution_id,
            submission_id,
            outcome,
        })
    }

    pub fn observation_id(&self) -> Uuid {
        self.observation_id
    }

    pub fn outcome(&self) -> &ExecutorSubmissionOutcome {
        &self.outcome
    }
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExecutorSubmissionError {
    #[error("executor submission storage is unavailable")]
    Unavailable,
    #[error("executor submission parameters conflict with durable identity")]
    Conflict,
    #[error("executor submission input is invalid")]
    InvalidInput,
    #[error("executor submission lease is stale or invalid")]
    StaleLease,
}

#[async_trait]
pub trait ExecutorHandoffStore: Send + Sync + 'static {
    async fn prepare_and_handoff(
        &self,
        lease: &WorkLease,
        execution_profile_id: Uuid,
    ) -> Result<Vec<PreparedExecutorSubmission>, ExecutorSubmissionError>;
}

#[async_trait]
pub trait ExecutorSubmissionStore: Send + Sync + 'static {
    async fn resume_owned(
        &self,
        scope: &ExecutorClaimScope,
        owner: &str,
    ) -> Result<Option<ExecutorSubmissionResume>, ExecutorSubmissionError>;

    async fn claim_prepared(
        &self,
        scope: &ExecutorClaimScope,
        owner: &str,
        lease_ms: i64,
    ) -> Result<Option<ExecutorSubmissionLease>, ExecutorSubmissionError>;

    async fn start(&self, lease: &ExecutorSubmissionLease) -> Result<(), ExecutorSubmissionError>;

    async fn heartbeat(
        &self,
        lease: &ExecutorSubmissionLease,
        lease_ms: i64,
    ) -> Result<ExecutorSubmissionLease, ExecutorSubmissionError>;

    async fn record_outcome(
        &self,
        lease: &ExecutorSubmissionLease,
        outcome: &ExecutorSubmissionOutcome,
    ) -> Result<(), ExecutorSubmissionError>;

    async fn reconcile_expired(&self, limit: u32) -> Result<u64, ExecutorSubmissionError>;
}

#[async_trait]
pub trait ExecutorExecutionProfileStore: Send + Sync + 'static {
    async fn load_execution_profile(
        &self,
        profile_key: &str,
    ) -> Result<ExecutorExecutionProfile, ExecutorSubmissionError>;
}

#[async_trait]
pub trait ExecutorEvidenceStore: Send + Sync + 'static {
    async fn load_pending_evidence(
        &self,
        scope: &ExecutorClaimScope,
        owner: &str,
    ) -> Result<Option<ExecutorSubmissionLease>, ExecutorSubmissionError>;
}

#[async_trait]
pub trait ExecutorLaunchContextStore: Send + Sync + 'static {
    async fn load_launch_context(
        &self,
        lease: &ExecutorSubmissionLease,
    ) -> Result<ExecutorLaunchContext, ExecutorSubmissionError>;
}

#[async_trait]
pub(crate) trait ExecutorArtifactAuthorityStore: Send + Sync + 'static {
    async fn publish_artifact_authority(
        &self,
        lease: &ExecutorSubmissionLease,
        authority: &ExecutorArtifactAuthority,
    ) -> Result<(), ExecutorSubmissionError>;
}
