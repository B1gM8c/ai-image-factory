use std::{
    fs,
    io::{self, Read, Write},
    os::fd::{AsRawFd, OwnedFd},
    path::{Path, PathBuf},
};

use image_provider_sdk::{
    OpaqueProviderId, PendingOperation, ProviderRequestId, RemoteOperationRef,
};
use rustix::{
    fs::{self as rfs, AtFlags, FileType, Mode, OFlags, RenameFlags},
    io::Errno,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::super::{ProviderExecutionContext, ProviderSubmitIntent};

const SPEC_FILE: &str = "spec.json";
const COMMAND_FILE: &str = "command.bin";
const LAUNCH_FILE: &str = "launch.json";
const RELEASE_FILE: &str = "dispatch-released.json";
const RECEIPT_EVIDENCE_FILE: &str = "receipt.evidence";
const TERMINAL_FILE: &str = "terminal.json";
const RECEIPT_SCHEMA: &str = "ai-image-factory/provider-submit-receipt/v1";
const SPEC_VERSION: u16 = 1;
const MAX_MARKER_BYTES: u64 = 64 * 1024;
const MAX_COMMAND_BYTES: u64 = 1024 * 1024;
const MAX_RECEIPT_BYTES: u64 = 64 * 1024;
const MAX_TEXT_BYTES: usize = 255;
const MAX_ERROR_CODE_BYTES: usize = 128;

pub(crate) struct RemoteSubmitJournal {
    root: OwnedFd,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteSubmitJournalSpec {
    schema_version: u16,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    provider_id: String,
    provider_account_id: Uuid,
    submit_owner: String,
    submit_lease_epoch: i64,
    output_index: u32,
    output_total: u32,
    command_schema: String,
    adapter_revision: String,
    provider_command_sha256: String,
    execution_binding_sha256: String,
    execution_profile_id: Uuid,
    credential_pool_id: Uuid,
    credential_revision: i64,
    credential_auth_sha256: String,
    resource_policy_id: Uuid,
    resource_policy_revision: i64,
    provider_deadline_at_ms: i64,
    command_bytes_sha256: String,
    command_byte_size: u64,
}

pub(crate) enum RemoteSubmitLaunch {
    Launch(RemoteSubmitLaunchAuthority),
    Attach(RemoteSubmitJournalObservation),
}

pub(crate) enum RemoteSubmitRelease {
    Dispatch(RemoteSubmitReleasedAuthority),
    Attach(RemoteSubmitJournalObservation),
}

pub(crate) struct RemoteSubmitLaunchAuthority {
    launch_nonce: Uuid,
}

#[derive(Clone, Copy)]
pub(crate) struct RemoteSubmitReleasedAuthority {
    launch_nonce: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RemoteSubmitJournalObservation {
    Prepared,
    LaunchCommitted,
    DispatchReleased,
    Terminal(RemoteSubmitJournalTerminal),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RemoteSubmitJournalTerminal {
    Accepted(PendingOperation),
    Rejected { error_code: String },
    Unknown { error_code: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum RemoteSubmitJournalError {
    #[error("remote submit journal input is invalid")]
    InvalidInput,
    #[error("remote submit journal entry was not found")]
    NotFound,
    #[error("remote submit journal identity conflicts with durable state")]
    Conflict,
    #[error("remote submit journal integrity validation failed")]
    Integrity,
    #[error("remote submit journal storage is unavailable")]
    Unavailable,
}

struct Entry {
    directory: OwnedFd,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskLaunch {
    execution_binding_sha256: String,
    launch_nonce: Uuid,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskRelease {
    execution_binding_sha256: String,
    launch_nonce: Uuid,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskReceiptEvidence {
    schema: String,
    execution_binding_sha256: String,
    launch_nonce: Uuid,
    observed_provider_id: String,
    observed_submission_id: String,
    remote_operation_id: String,
    provider_request_id: Option<String>,
    next_poll_after_ms: Option<u64>,
    payload_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum DiskTerminal {
    Accepted {
        execution_binding_sha256: String,
        launch_nonce: Uuid,
        receipt_sha256: String,
        receipt_byte_size: u64,
    },
    Rejected {
        execution_binding_sha256: String,
        launch_nonce: Uuid,
        error_code: String,
    },
    Unknown {
        execution_binding_sha256: String,
        launch_nonce: Uuid,
        error_code: String,
    },
}

impl RemoteSubmitJournalSpec {
    pub(crate) fn new(
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
        canonical_command: &[u8],
    ) -> Result<Self, RemoteSubmitJournalError> {
        let command_byte_size = u64::try_from(canonical_command.len())
            .map_err(|_| RemoteSubmitJournalError::InvalidInput)?;
        let spec = Self {
            schema_version: SPEC_VERSION,
            submission_id: intent.submission_id,
            executor_execution_id: intent.executor_execution_id,
            provider_id: intent.provider_id.clone(),
            provider_account_id: intent.provider_account_id,
            submit_owner: intent.submit_owner.clone(),
            submit_lease_epoch: intent.submit_lease_epoch,
            output_index: intent.output_index,
            output_total: intent.output_total,
            command_schema: context.command_schema().to_owned(),
            adapter_revision: context.adapter_revision().to_owned(),
            provider_command_sha256: context.provider_command_sha256().to_owned(),
            execution_binding_sha256: context.execution_binding_sha256().to_owned(),
            execution_profile_id: context.execution_profile_id(),
            credential_pool_id: context.credential_pool_id(),
            credential_revision: context.credential_revision(),
            credential_auth_sha256: context.credential_auth_sha256().to_owned(),
            resource_policy_id: context.resource_policy_id(),
            resource_policy_revision: context.resource_policy_revision(),
            provider_deadline_at_ms: context.provider_deadline_at_ms(),
            command_bytes_sha256: sha256(canonical_command),
            command_byte_size,
        };
        spec.validate(RemoteSubmitJournalError::InvalidInput)?;
        if spec.provider_command_sha256 != intent.provider_command_sha256 {
            return Err(RemoteSubmitJournalError::InvalidInput);
        }
        Ok(spec)
    }

    pub(crate) fn submission_id(&self) -> Uuid {
        self.submission_id
    }

    fn validate(&self, error: RemoteSubmitJournalError) -> Result<(), RemoteSubmitJournalError> {
        if self.schema_version != SPEC_VERSION
            || self.submission_id.is_nil()
            || self.executor_execution_id.is_nil()
            || self.submission_id == self.executor_execution_id
            || self.provider_account_id.is_nil()
            || self.execution_profile_id.is_nil()
            || self.credential_pool_id.is_nil()
            || self.resource_policy_id.is_nil()
            || self.submit_lease_epoch <= 0
            || self.output_total == 0
            || self.output_index >= self.output_total
            || self.credential_revision <= 0
            || self.resource_policy_revision <= 0
            || self.provider_deadline_at_ms <= 0
            || !(1..=MAX_COMMAND_BYTES).contains(&self.command_byte_size)
            || !valid_opaque_provider_id(&self.provider_id)
            || !valid_owner(&self.submit_owner)
            || !valid_text(&self.command_schema)
            || !valid_text(&self.adapter_revision)
            || !valid_sha256(&self.provider_command_sha256)
            || !valid_sha256(&self.execution_binding_sha256)
            || !valid_sha256(&self.credential_auth_sha256)
            || !valid_sha256(&self.command_bytes_sha256)
        {
            return Err(error);
        }
        Ok(())
    }
}

impl RemoteSubmitJournal {
    pub(crate) fn validate_canonical_command(
        canonical_command: &[u8],
    ) -> Result<(), RemoteSubmitJournalError> {
        if canonical_command.is_empty() || canonical_command.len() as u64 > MAX_COMMAND_BYTES {
            return Err(RemoteSubmitJournalError::InvalidInput);
        }
        Ok(())
    }

    pub(crate) fn new(root: impl AsRef<Path>) -> Result<Self, RemoteSubmitJournalError> {
        let root_path = prepare_root(root.as_ref())?;
        let root = rfs::open(
            &root_path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| RemoteSubmitJournalError::InvalidInput)?;
        validate_directory(&root, RemoteSubmitJournalError::InvalidInput)?;
        Ok(Self { root })
    }

    pub(crate) fn prepare(
        &self,
        spec: &RemoteSubmitJournalSpec,
        canonical_command: &[u8],
    ) -> Result<(), RemoteSubmitJournalError> {
        spec.validate(RemoteSubmitJournalError::InvalidInput)?;
        validate_command(
            spec,
            canonical_command,
            RemoteSubmitJournalError::InvalidInput,
        )?;
        match self.open_prepared(spec) {
            Ok(_) => return Ok(()),
            Err(RemoteSubmitJournalError::NotFound | RemoteSubmitJournalError::Integrity) => {}
            Err(error) => return Err(error),
        }
        let entry = self.ensure_entry(spec.submission_id)?;
        publish_or_compare(
            &entry.directory,
            COMMAND_FILE,
            canonical_command,
            MAX_COMMAND_BYTES,
        )?;
        publish_json_or_compare(&entry.directory, SPEC_FILE, spec, MAX_MARKER_BYTES)
    }

    pub(crate) fn commit_launch(
        &self,
        spec: &RemoteSubmitJournalSpec,
    ) -> Result<RemoteSubmitLaunch, RemoteSubmitJournalError> {
        let entry = self.open_prepared(spec)?;
        let observation = self.observe_entry(&entry, spec)?;
        match observation {
            RemoteSubmitJournalObservation::Prepared => {}
            RemoteSubmitJournalObservation::LaunchCommitted => {
                return Ok(RemoteSubmitLaunch::Launch(launch_authority(
                    &entry.directory,
                    spec,
                )?));
            }
            RemoteSubmitJournalObservation::DispatchReleased
            | RemoteSubmitJournalObservation::Terminal(_) => {
                return Ok(RemoteSubmitLaunch::Attach(observation));
            }
        }
        let launch = DiskLaunch {
            execution_binding_sha256: spec.execution_binding_sha256.clone(),
            launch_nonce: Uuid::new_v4(),
        };
        match publish_bytes(
            &entry.directory,
            LAUNCH_FILE,
            &serialize(&launch)?,
            MAX_MARKER_BYTES,
        )? {
            true => Ok(RemoteSubmitLaunch::Launch(RemoteSubmitLaunchAuthority {
                launch_nonce: launch.launch_nonce,
            })),
            false => Ok(RemoteSubmitLaunch::Launch(launch_authority(
                &entry.directory,
                spec,
            )?)),
        }
    }

    pub(crate) fn release_dispatch(
        &self,
        spec: &RemoteSubmitJournalSpec,
        authority: RemoteSubmitLaunchAuthority,
    ) -> Result<RemoteSubmitRelease, RemoteSubmitJournalError> {
        let entry = self.open_prepared(spec)?;
        let launch =
            read_required_json::<DiskLaunch>(&entry.directory, LAUNCH_FILE, MAX_MARKER_BYTES)?;
        validate_launch(&launch, spec)?;
        if launch.launch_nonce != authority.launch_nonce {
            return Err(RemoteSubmitJournalError::Conflict);
        }
        let release = DiskRelease {
            execution_binding_sha256: spec.execution_binding_sha256.clone(),
            launch_nonce: authority.launch_nonce,
        };
        match publish_json_create_or_compare(
            &entry.directory,
            RELEASE_FILE,
            &release,
            MAX_MARKER_BYTES,
        )? {
            true => Ok(RemoteSubmitRelease::Dispatch(
                RemoteSubmitReleasedAuthority {
                    launch_nonce: authority.launch_nonce,
                },
            )),
            false => Ok(RemoteSubmitRelease::Attach(
                self.observe_entry(&entry, spec)?,
            )),
        }
    }

    pub(crate) fn publish_accepted(
        &self,
        spec: &RemoteSubmitJournalSpec,
        authority: &RemoteSubmitReleasedAuthority,
        pending: &PendingOperation,
    ) -> Result<(), RemoteSubmitJournalError> {
        let entry = self.open_released(spec, authority)?;
        let receipt = DiskReceiptEvidence::new(spec, authority, pending)?;
        let receipt_evidence = serialize(&receipt)?;
        let terminal = DiskTerminal::Accepted {
            execution_binding_sha256: spec.execution_binding_sha256.clone(),
            launch_nonce: authority.launch_nonce,
            receipt_sha256: sha256(&receipt_evidence),
            receipt_byte_size: receipt_evidence.len() as u64,
        };
        terminal.validate(spec, RemoteSubmitJournalError::InvalidInput)?;
        publish_or_compare(
            &entry.directory,
            RECEIPT_EVIDENCE_FILE,
            &receipt_evidence,
            MAX_RECEIPT_BYTES,
        )?;
        publish_json_or_compare(&entry.directory, TERMINAL_FILE, &terminal, MAX_MARKER_BYTES)
    }

    pub(crate) fn publish_failure(
        &self,
        spec: &RemoteSubmitJournalSpec,
        authority: &RemoteSubmitReleasedAuthority,
        terminal: &RemoteSubmitJournalTerminal,
    ) -> Result<(), RemoteSubmitJournalError> {
        let disk = match terminal {
            RemoteSubmitJournalTerminal::Accepted(_) => {
                return Err(RemoteSubmitJournalError::InvalidInput);
            }
            RemoteSubmitJournalTerminal::Rejected { error_code } => DiskTerminal::Rejected {
                execution_binding_sha256: spec.execution_binding_sha256.clone(),
                launch_nonce: authority.launch_nonce,
                error_code: error_code.clone(),
            },
            RemoteSubmitJournalTerminal::Unknown { error_code } => DiskTerminal::Unknown {
                execution_binding_sha256: spec.execution_binding_sha256.clone(),
                launch_nonce: authority.launch_nonce,
                error_code: error_code.clone(),
            },
        };
        disk.validate(spec, RemoteSubmitJournalError::InvalidInput)?;
        let entry = self.open_released(spec, authority)?;
        publish_json_or_compare(&entry.directory, TERMINAL_FILE, &disk, MAX_MARKER_BYTES)
    }

    pub(crate) fn observe(
        &self,
        spec: &RemoteSubmitJournalSpec,
    ) -> Result<RemoteSubmitJournalObservation, RemoteSubmitJournalError> {
        let entry = self.open_prepared(spec)?;
        self.observe_entry(&entry, spec)
    }

    fn observe_entry(
        &self,
        entry: &Entry,
        spec: &RemoteSubmitJournalSpec,
    ) -> Result<RemoteSubmitJournalObservation, RemoteSubmitJournalError> {
        let terminal =
            read_optional_json::<DiskTerminal>(&entry.directory, TERMINAL_FILE, MAX_MARKER_BYTES)?;
        let receipt_evidence =
            read_optional_bytes(&entry.directory, RECEIPT_EVIDENCE_FILE, MAX_RECEIPT_BYTES)?;
        let release =
            read_optional_json::<DiskRelease>(&entry.directory, RELEASE_FILE, MAX_MARKER_BYTES)?;
        let launch =
            read_optional_json::<DiskLaunch>(&entry.directory, LAUNCH_FILE, MAX_MARKER_BYTES)?;
        if (terminal.is_some() || receipt_evidence.is_some()) && release.is_none()
            || release.is_some() && launch.is_none()
        {
            return Err(RemoteSubmitJournalError::Integrity);
        }
        let Some(launch) = launch else {
            return Ok(RemoteSubmitJournalObservation::Prepared);
        };
        validate_launch(&launch, spec)?;
        let Some(release) = release else {
            return Ok(RemoteSubmitJournalObservation::LaunchCommitted);
        };
        validate_release(&release, spec, launch.launch_nonce)?;
        let Some(terminal) = terminal else {
            let Some(receipt_evidence) = receipt_evidence else {
                return Ok(RemoteSubmitJournalObservation::DispatchReleased);
            };
            let receipt = parse_receipt_evidence(&receipt_evidence)?;
            receipt.validate(
                spec,
                launch.launch_nonce,
                RemoteSubmitJournalError::Integrity,
            )?;
            return Ok(RemoteSubmitJournalObservation::Terminal(
                RemoteSubmitJournalTerminal::Accepted(receipt.into_pending()?),
            ));
        };
        terminal.validate(spec, RemoteSubmitJournalError::Integrity)?;
        if terminal.launch_nonce() != launch.launch_nonce {
            return Err(RemoteSubmitJournalError::Integrity);
        }
        Ok(RemoteSubmitJournalObservation::Terminal(
            terminal.into_terminal(receipt_evidence, spec, launch.launch_nonce)?,
        ))
    }

    fn ensure_entry(&self, submission_id: Uuid) -> Result<Entry, RemoteSubmitJournalError> {
        validate_directory(&self.root, RemoteSubmitJournalError::Integrity)?;
        let name = submission_id.simple().to_string();
        match rfs::mkdirat(&self.root, &name, Mode::RWXU) {
            Ok(()) => rfs::fsync(&self.root).map_err(|_| RemoteSubmitJournalError::Unavailable)?,
            Err(Errno::EXIST) => {}
            Err(_) => return Err(RemoteSubmitJournalError::Unavailable),
        }
        self.open_entry(submission_id)
    }

    fn open_entry(&self, submission_id: Uuid) -> Result<Entry, RemoteSubmitJournalError> {
        validate_directory(&self.root, RemoteSubmitJournalError::Integrity)?;
        let fd = rfs::openat(
            &self.root,
            submission_id.simple().to_string(),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| {
            if error == Errno::NOENT {
                RemoteSubmitJournalError::NotFound
            } else {
                RemoteSubmitJournalError::Integrity
            }
        })?;
        validate_directory(&fd, RemoteSubmitJournalError::Integrity)?;
        Ok(Entry { directory: fd })
    }

    fn open_prepared(
        &self,
        expected: &RemoteSubmitJournalSpec,
    ) -> Result<Entry, RemoteSubmitJournalError> {
        expected.validate(RemoteSubmitJournalError::InvalidInput)?;
        let entry = self.open_entry(expected.submission_id)?;
        let actual = read_required_json::<RemoteSubmitJournalSpec>(
            &entry.directory,
            SPEC_FILE,
            MAX_MARKER_BYTES,
        )?;
        actual.validate(RemoteSubmitJournalError::Integrity)?;
        if actual != *expected {
            return Err(RemoteSubmitJournalError::Conflict);
        }
        let command = read_required_bytes(&entry.directory, COMMAND_FILE, MAX_COMMAND_BYTES)?;
        validate_command(expected, &command, RemoteSubmitJournalError::Integrity)?;
        Ok(entry)
    }

    fn open_released(
        &self,
        spec: &RemoteSubmitJournalSpec,
        authority: &RemoteSubmitReleasedAuthority,
    ) -> Result<Entry, RemoteSubmitJournalError> {
        let entry = self.open_prepared(spec)?;
        let release =
            read_required_json::<DiskRelease>(&entry.directory, RELEASE_FILE, MAX_MARKER_BYTES)?;
        validate_release(&release, spec, authority.launch_nonce)?;
        Ok(entry)
    }
}

impl DiskReceiptEvidence {
    fn new(
        spec: &RemoteSubmitJournalSpec,
        authority: &RemoteSubmitReleasedAuthority,
        pending: &PendingOperation,
    ) -> Result<Self, RemoteSubmitJournalError> {
        let operation = pending.operation();
        let mut receipt = Self {
            schema: RECEIPT_SCHEMA.to_owned(),
            execution_binding_sha256: spec.execution_binding_sha256.clone(),
            launch_nonce: authority.launch_nonce,
            observed_provider_id: operation.provider_id().to_owned(),
            observed_submission_id: operation.submission_id().to_owned(),
            remote_operation_id: operation.operation_id().to_owned(),
            provider_request_id: pending
                .provider_request_id()
                .map(|value| value.as_str().to_owned()),
            next_poll_after_ms: pending.next_poll_after_ms(),
            payload_sha256: String::new(),
        };
        receipt.payload_sha256 = receipt.canonical_payload_sha256();
        receipt.validate(
            spec,
            authority.launch_nonce,
            RemoteSubmitJournalError::InvalidInput,
        )?;
        Ok(receipt)
    }

    fn validate(
        &self,
        spec: &RemoteSubmitJournalSpec,
        launch_nonce: Uuid,
        error: RemoteSubmitJournalError,
    ) -> Result<(), RemoteSubmitJournalError> {
        if self.schema != RECEIPT_SCHEMA
            || self.execution_binding_sha256 != spec.execution_binding_sha256
            || !valid_sha256(&self.execution_binding_sha256)
            || self.launch_nonce != launch_nonce
            || launch_nonce.is_nil()
            || !valid_sha256(&self.payload_sha256)
            || self.payload_sha256 != self.canonical_payload_sha256()
            || !valid_opaque_provider_id(&self.observed_provider_id)
            || !valid_opaque_provider_id(&self.observed_submission_id)
            || !valid_opaque_provider_id(&self.remote_operation_id)
            || self
                .provider_request_id
                .as_deref()
                .is_some_and(|value| !valid_opaque_provider_id(value))
        {
            return Err(error);
        }
        Ok(())
    }

    fn canonical_payload_sha256(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"ai-image-factory/provider-submit-receipt-evidence/v1\0");
        hash_field(&mut digest, self.schema.as_bytes());
        hash_field(&mut digest, self.execution_binding_sha256.as_bytes());
        hash_field(&mut digest, self.launch_nonce.as_bytes());
        hash_field(&mut digest, self.observed_provider_id.as_bytes());
        hash_field(&mut digest, self.observed_submission_id.as_bytes());
        hash_field(&mut digest, self.remote_operation_id.as_bytes());
        match &self.provider_request_id {
            Some(value) => {
                digest.update([1]);
                hash_field(&mut digest, value.as_bytes());
            }
            None => digest.update([0]),
        }
        match self.next_poll_after_ms {
            Some(value) => {
                digest.update([1]);
                digest.update(value.to_be_bytes());
            }
            None => digest.update([0]),
        }
        hex::encode(digest.finalize())
    }

    fn into_pending(self) -> Result<PendingOperation, RemoteSubmitJournalError> {
        let operation = RemoteOperationRef::new(
            self.observed_provider_id,
            self.observed_submission_id,
            self.remote_operation_id,
        )
        .map_err(|_| RemoteSubmitJournalError::Integrity)?;
        let provider_request_id = self
            .provider_request_id
            .map(ProviderRequestId::new)
            .transpose()
            .map_err(|_| RemoteSubmitJournalError::Integrity)?;
        Ok(PendingOperation::new(
            operation,
            provider_request_id,
            self.next_poll_after_ms,
        ))
    }
}

impl DiskTerminal {
    fn validate(
        &self,
        spec: &RemoteSubmitJournalSpec,
        error: RemoteSubmitJournalError,
    ) -> Result<(), RemoteSubmitJournalError> {
        if self.execution_binding_sha256() != spec.execution_binding_sha256
            || !valid_sha256(self.execution_binding_sha256())
            || self.launch_nonce().is_nil()
        {
            return Err(error);
        }
        match self {
            Self::Accepted {
                receipt_sha256,
                receipt_byte_size,
                ..
            } => {
                if !valid_sha256(receipt_sha256)
                    || !(1..=MAX_RECEIPT_BYTES).contains(receipt_byte_size)
                {
                    return Err(error);
                }
            }
            Self::Rejected { error_code, .. } | Self::Unknown { error_code, .. } => {
                if !valid_error_code(error_code) {
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    fn into_terminal(
        self,
        receipt_evidence: Option<Vec<u8>>,
        spec: &RemoteSubmitJournalSpec,
        launch_nonce: Uuid,
    ) -> Result<RemoteSubmitJournalTerminal, RemoteSubmitJournalError> {
        match self {
            Self::Accepted {
                receipt_sha256,
                receipt_byte_size,
                ..
            } => {
                let evidence = receipt_evidence.ok_or(RemoteSubmitJournalError::Integrity)?;
                if evidence.len() as u64 != receipt_byte_size || sha256(&evidence) != receipt_sha256
                {
                    return Err(RemoteSubmitJournalError::Integrity);
                }
                let receipt = parse_receipt_evidence(&evidence)?;
                receipt.validate(spec, launch_nonce, RemoteSubmitJournalError::Integrity)?;
                Ok(RemoteSubmitJournalTerminal::Accepted(
                    receipt.into_pending()?,
                ))
            }
            Self::Rejected { error_code, .. } if receipt_evidence.is_none() => {
                Ok(RemoteSubmitJournalTerminal::Rejected { error_code })
            }
            Self::Unknown { error_code, .. } if receipt_evidence.is_none() => {
                Ok(RemoteSubmitJournalTerminal::Unknown { error_code })
            }
            Self::Rejected { .. } | Self::Unknown { .. } => {
                Err(RemoteSubmitJournalError::Integrity)
            }
        }
    }

    fn execution_binding_sha256(&self) -> &str {
        match self {
            Self::Accepted {
                execution_binding_sha256,
                ..
            }
            | Self::Rejected {
                execution_binding_sha256,
                ..
            }
            | Self::Unknown {
                execution_binding_sha256,
                ..
            } => execution_binding_sha256,
        }
    }

    fn launch_nonce(&self) -> Uuid {
        match self {
            Self::Accepted { launch_nonce, .. }
            | Self::Rejected { launch_nonce, .. }
            | Self::Unknown { launch_nonce, .. } => *launch_nonce,
        }
    }
}

fn parse_receipt_evidence(bytes: &[u8]) -> Result<DiskReceiptEvidence, RemoteSubmitJournalError> {
    serde_json::from_slice(bytes).map_err(|_| RemoteSubmitJournalError::Integrity)
}

fn validate_command(
    spec: &RemoteSubmitJournalSpec,
    command: &[u8],
    error: RemoteSubmitJournalError,
) -> Result<(), RemoteSubmitJournalError> {
    if command.is_empty()
        || command.len() as u64 != spec.command_byte_size
        || sha256(command) != spec.command_bytes_sha256
    {
        return Err(error);
    }
    Ok(())
}

fn validate_launch(
    launch: &DiskLaunch,
    spec: &RemoteSubmitJournalSpec,
) -> Result<(), RemoteSubmitJournalError> {
    if launch.launch_nonce.is_nil()
        || launch.execution_binding_sha256 != spec.execution_binding_sha256
    {
        return Err(RemoteSubmitJournalError::Integrity);
    }
    Ok(())
}

fn launch_authority(
    directory: &OwnedFd,
    spec: &RemoteSubmitJournalSpec,
) -> Result<RemoteSubmitLaunchAuthority, RemoteSubmitJournalError> {
    let launch = read_required_json::<DiskLaunch>(directory, LAUNCH_FILE, MAX_MARKER_BYTES)?;
    validate_launch(&launch, spec)?;
    Ok(RemoteSubmitLaunchAuthority {
        launch_nonce: launch.launch_nonce,
    })
}

fn validate_release(
    release: &DiskRelease,
    spec: &RemoteSubmitJournalSpec,
    launch_nonce: Uuid,
) -> Result<(), RemoteSubmitJournalError> {
    if release.launch_nonce != launch_nonce
        || release.execution_binding_sha256 != spec.execution_binding_sha256
    {
        return Err(RemoteSubmitJournalError::Integrity);
    }
    Ok(())
}

fn prepare_root(path: &Path) -> Result<PathBuf, RemoteSubmitJournalError> {
    if !path.is_absolute() {
        return Err(RemoteSubmitJournalError::InvalidInput);
    }
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            create_private_directory(path).map_err(|_| RemoteSubmitJournalError::Unavailable)?;
            sync_parent(path)?;
        }
        Err(_) => return Err(RemoteSubmitJournalError::Unavailable),
    }
    validate_private_directory_path(path, RemoteSubmitJournalError::InvalidInput)?;
    sync_directory(path)?;
    fs::canonicalize(path).map_err(|_| RemoteSubmitJournalError::Unavailable)
}

fn validate_private_directory_path(
    path: &Path,
    error: RemoteSubmitJournalError,
) -> Result<(), RemoteSubmitJournalError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| RemoteSubmitJournalError::Unavailable)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(error);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o777 != 0o700
        {
            return Err(error);
        }
    }
    Ok(())
}

fn validate_directory(
    directory: &OwnedFd,
    error: RemoteSubmitJournalError,
) -> Result<(), RemoteSubmitJournalError> {
    let stat = rfs::fstat(directory).map_err(|_| RemoteSubmitJournalError::Unavailable)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory
        || Mode::from_raw_mode(stat.st_mode) != Mode::RWXU
        || stat.st_uid != unsafe { libc::geteuid() }
    {
        return Err(error);
    }
    Ok(())
}

fn publish_json_or_compare<T>(
    directory: &OwnedFd,
    name: &str,
    value: &T,
    max_bytes: u64,
) -> Result<(), RemoteSubmitJournalError>
where
    T: Serialize + DeserializeOwned + Eq,
{
    publish_json_create_or_compare(directory, name, value, max_bytes).map(|_| ())
}

fn publish_json_create_or_compare<T>(
    directory: &OwnedFd,
    name: &str,
    value: &T,
    max_bytes: u64,
) -> Result<bool, RemoteSubmitJournalError>
where
    T: Serialize + DeserializeOwned + Eq,
{
    let bytes = serialize(value)?;
    match publish_bytes(directory, name, &bytes, max_bytes)? {
        true => Ok(true),
        false => {
            let existing = read_required_json::<T>(directory, name, max_bytes)?;
            if existing == *value {
                Ok(false)
            } else {
                Err(RemoteSubmitJournalError::Conflict)
            }
        }
    }
}

fn publish_or_compare(
    directory: &OwnedFd,
    name: &str,
    bytes: &[u8],
    max_bytes: u64,
) -> Result<(), RemoteSubmitJournalError> {
    match publish_bytes(directory, name, bytes, max_bytes)? {
        true => Ok(()),
        false => {
            let existing = read_required_bytes(directory, name, max_bytes)?;
            if existing == bytes {
                Ok(())
            } else {
                Err(RemoteSubmitJournalError::Conflict)
            }
        }
    }
}

fn publish_bytes(
    directory: &OwnedFd,
    name: &str,
    bytes: &[u8],
    max_bytes: u64,
) -> Result<bool, RemoteSubmitJournalError> {
    if bytes.is_empty() || bytes.len() as u64 > max_bytes {
        return Err(RemoteSubmitJournalError::InvalidInput);
    }
    let temporary = format!(".tmp-{}", Uuid::new_v4().simple());
    let fd = rfs::openat(
        directory,
        &temporary,
        OFlags::WRONLY
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::NOFOLLOW
            | OFlags::CLOEXEC
            | OFlags::NONBLOCK,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|_| RemoteSubmitJournalError::Unavailable)?;
    rfs::fchmod(&fd, Mode::RUSR | Mode::WUSR).map_err(|_| RemoteSubmitJournalError::Unavailable)?;
    let mut file = fs::File::from(fd);
    if file.write_all(bytes).is_err() || sync_file(&file).is_err() {
        let _ = rfs::unlinkat(directory, &temporary, AtFlags::empty());
        return Err(RemoteSubmitJournalError::Unavailable);
    }
    match rfs::renameat_with(
        directory,
        &temporary,
        directory,
        name,
        RenameFlags::NOREPLACE,
    ) {
        Ok(()) => {
            rfs::fsync(directory).map_err(|_| RemoteSubmitJournalError::Unavailable)?;
            Ok(true)
        }
        Err(Errno::EXIST) => {
            rfs::unlinkat(directory, &temporary, AtFlags::empty())
                .map_err(|_| RemoteSubmitJournalError::Unavailable)?;
            rfs::fsync(directory).map_err(|_| RemoteSubmitJournalError::Unavailable)?;
            Ok(false)
        }
        Err(_) => {
            let _ = rfs::unlinkat(directory, &temporary, AtFlags::empty());
            Err(RemoteSubmitJournalError::Unavailable)
        }
    }
}

fn read_required_json<T: DeserializeOwned>(
    directory: &OwnedFd,
    name: &str,
    max_bytes: u64,
) -> Result<T, RemoteSubmitJournalError> {
    let bytes = read_required_bytes(directory, name, max_bytes)?;
    serde_json::from_slice(&bytes).map_err(|_| RemoteSubmitJournalError::Integrity)
}

fn read_optional_json<T: DeserializeOwned>(
    directory: &OwnedFd,
    name: &str,
    max_bytes: u64,
) -> Result<Option<T>, RemoteSubmitJournalError> {
    let Some(bytes) = read_optional_bytes(directory, name, max_bytes)? else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| RemoteSubmitJournalError::Integrity)
}

fn read_required_bytes(
    directory: &OwnedFd,
    name: &str,
    max_bytes: u64,
) -> Result<Vec<u8>, RemoteSubmitJournalError> {
    read_optional_bytes(directory, name, max_bytes)?.ok_or(RemoteSubmitJournalError::Integrity)
}

fn read_optional_bytes(
    directory: &OwnedFd,
    name: &str,
    max_bytes: u64,
) -> Result<Option<Vec<u8>>, RemoteSubmitJournalError> {
    let fd = match rfs::openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(Errno::NOENT) => return Ok(None),
        Err(_) => return Err(RemoteSubmitJournalError::Integrity),
    };
    let mut file = fs::File::from(fd);
    let stat = rfs::fstat(&file).map_err(|_| RemoteSubmitJournalError::Unavailable)?;
    let size = validate_file(&stat, max_bytes)?;
    let mut bytes = Vec::with_capacity(size);
    Read::by_ref(&mut file)
        .take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| RemoteSubmitJournalError::Unavailable)?;
    let final_stat = rfs::fstat(&file).map_err(|_| RemoteSubmitJournalError::Unavailable)?;
    if validate_file(&final_stat, max_bytes)? != size || bytes.len() != size {
        return Err(RemoteSubmitJournalError::Integrity);
    }
    Ok(Some(bytes))
}

fn validate_file(stat: &rfs::Stat, max_bytes: u64) -> Result<usize, RemoteSubmitJournalError> {
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
        || Mode::from_raw_mode(stat.st_mode) != Mode::RUSR | Mode::WUSR
        || stat.st_uid != unsafe { libc::geteuid() }
        || stat.st_nlink != 1
        || stat.st_size <= 0
        || stat.st_size as u64 > max_bytes
    {
        return Err(RemoteSubmitJournalError::Integrity);
    }
    usize::try_from(stat.st_size).map_err(|_| RemoteSubmitJournalError::Integrity)
}

fn serialize(value: &impl Serialize) -> Result<Vec<u8>, RemoteSubmitJournalError> {
    let bytes = serde_json::to_vec(value).map_err(|_| RemoteSubmitJournalError::InvalidInput)?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_MARKER_BYTES {
        return Err(RemoteSubmitJournalError::InvalidInput);
    }
    Ok(bytes)
}

fn sync_file(file: &fs::File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } == 0 {
            return Ok(());
        }
        Err(io::Error::last_os_error())
    }
    #[cfg(not(target_os = "macos"))]
    {
        file.sync_all()
    }
}

fn sync_parent(path: &Path) -> Result<(), RemoteSubmitJournalError> {
    let parent = path
        .parent()
        .ok_or(RemoteSubmitJournalError::InvalidInput)?;
    sync_directory(parent)
}

fn sync_directory(path: &Path) -> Result<(), RemoteSubmitJournalError> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| RemoteSubmitJournalError::Unavailable)
}

#[cfg(unix)]
fn create_private_directory(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    fs::DirBuilder::new().mode(0o700).create(path)
}

fn valid_owner(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_TEXT_BYTES
        && !value.contains("://")
        && value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_alphanumeric()
                || (index > 0 && matches!(byte, b'.' | b'_' | b':' | b'@' | b'/' | b'-'))
        })
}

fn valid_opaque_provider_id(value: &str) -> bool {
    OpaqueProviderId::new(value.to_owned()).is_ok()
}

fn valid_text(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_TEXT_BYTES
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn valid_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ERROR_CODE_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn hash_field(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc, thread};

    use image_provider_sdk::{PendingOperation, ProviderRequestId, RemoteOperationRef};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn prepare_is_exactly_replayable_and_rejects_conflicting_identity() {
        let fixture = Fixture::new();
        fixture
            .journal
            .prepare(&fixture.spec, &fixture.command)
            .unwrap();
        fixture
            .journal
            .prepare(&fixture.spec, &fixture.command)
            .unwrap();

        let mut conflict = fixture.spec.clone();
        conflict.submit_owner = "another-owner".to_owned();
        assert_eq!(
            fixture.journal.prepare(&conflict, &fixture.command),
            Err(RemoteSubmitJournalError::Conflict)
        );
        assert_eq!(
            fixture.journal.prepare(&fixture.spec, b"different-command"),
            Err(RemoteSubmitJournalError::InvalidInput)
        );
    }

    #[test]
    fn prepare_repairs_a_durable_command_only_prefix() {
        let fixture = Fixture::new();
        let entry = fixture
            .journal
            .ensure_entry(fixture.spec.submission_id)
            .unwrap();
        publish_or_compare(
            &entry.directory,
            COMMAND_FILE,
            &fixture.command,
            MAX_COMMAND_BYTES,
        )
        .unwrap();

        fixture
            .journal
            .prepare(&fixture.spec, &fixture.command)
            .unwrap();
        assert_eq!(
            fixture.journal.observe(&fixture.spec),
            Ok(RemoteSubmitJournalObservation::Prepared)
        );
    }

    #[test]
    fn concurrent_release_elects_exactly_one_dispatch_authority() {
        let fixture = Fixture::new();
        fixture
            .journal
            .prepare(&fixture.spec, &fixture.command)
            .unwrap();
        let journal = Arc::new(fixture.journal);
        let mut workers = Vec::new();
        for _ in 0..32 {
            let journal = Arc::clone(&journal);
            let spec = fixture.spec.clone();
            workers.push(thread::spawn(move || {
                let RemoteSubmitLaunch::Launch(launch) = journal.commit_launch(&spec).unwrap()
                else {
                    return false;
                };
                matches!(
                    journal.release_dispatch(&spec, launch).unwrap(),
                    RemoteSubmitRelease::Dispatch(_)
                )
            }));
        }

        let dispatches = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|launched| *launched)
            .count();
        assert_eq!(dispatches, 1);
        assert_eq!(
            journal.observe(&fixture.spec),
            Ok(RemoteSubmitJournalObservation::DispatchReleased)
        );
    }

    #[test]
    fn observation_rejects_terminal_without_release_predecessor() {
        let fixture = Fixture::new();
        fixture
            .journal
            .prepare(&fixture.spec, &fixture.command)
            .unwrap();
        let entry = fixture.journal.open_prepared(&fixture.spec).unwrap();
        publish_json_or_compare(
            &entry.directory,
            TERMINAL_FILE,
            &DiskTerminal::Rejected {
                execution_binding_sha256: fixture.spec.execution_binding_sha256.clone(),
                launch_nonce: Uuid::new_v4(),
                error_code: "provider_rejected".to_owned(),
            },
            MAX_MARKER_BYTES,
        )
        .unwrap();

        assert_eq!(
            fixture.journal.observe(&fixture.spec),
            Err(RemoteSubmitJournalError::Integrity)
        );
    }

    #[test]
    fn accepted_receipt_evidence_recovers_before_terminal_and_reopens_exactly() {
        let fixture = Fixture::new();
        let released = fixture.release();
        let pending = fixture.pending();
        let evidence =
            serialize(&DiskReceiptEvidence::new(&fixture.spec, &released, &pending).unwrap())
                .unwrap();
        let entry = fixture
            .journal
            .open_released(&fixture.spec, &released)
            .unwrap();
        publish_or_compare(
            &entry.directory,
            RECEIPT_EVIDENCE_FILE,
            &evidence,
            MAX_RECEIPT_BYTES,
        )
        .unwrap();
        assert_eq!(
            fixture.journal.observe(&fixture.spec),
            Ok(RemoteSubmitJournalObservation::Terminal(
                RemoteSubmitJournalTerminal::Accepted(pending.clone())
            ))
        );

        fixture
            .journal
            .publish_accepted(&fixture.spec, &released, &pending)
            .unwrap();
        assert_eq!(
            fixture.journal.observe(&fixture.spec),
            Ok(RemoteSubmitJournalObservation::Terminal(
                RemoteSubmitJournalTerminal::Accepted(pending)
            ))
        );
    }

    #[test]
    fn receipt_tampering_and_hardlinked_markers_fail_closed() {
        let fixture = Fixture::new();
        let released = fixture.release();
        fixture
            .journal
            .publish_accepted(&fixture.spec, &released, &fixture.pending())
            .unwrap();
        let entry = fixture.entry_path();
        let receipt_path = entry.join(RECEIPT_EVIDENCE_FILE);
        let mut receipt: serde_json::Value =
            serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
        receipt["remote_operation_id"] = serde_json::Value::String("valid-but-wrong".to_owned());
        fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        assert_eq!(
            fixture.journal.observe(&fixture.spec),
            Err(RemoteSubmitJournalError::Integrity)
        );

        let fixture = Fixture::new();
        let released = fixture.release();
        fixture
            .journal
            .publish_failure(
                &fixture.spec,
                &released,
                &RemoteSubmitJournalTerminal::Rejected {
                    error_code: "provider_rejected".to_owned(),
                },
            )
            .unwrap();
        fs::hard_link(
            fixture.entry_path().join(TERMINAL_FILE),
            fixture.temp.path().join("linked-terminal"),
        )
        .unwrap();
        assert_eq!(
            fixture.journal.observe(&fixture.spec),
            Err(RemoteSubmitJournalError::Integrity)
        );
    }

    #[cfg(unix)]
    #[test]
    fn root_rejects_symlinks_and_non_private_permissions() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let parent = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let link = parent.path().join("journal-link");
        symlink(target.path(), &link).unwrap();
        assert!(matches!(
            RemoteSubmitJournal::new(&link),
            Err(RemoteSubmitJournalError::InvalidInput)
        ));

        let public = parent.path().join("public-journal");
        fs::create_dir(&public).unwrap();
        fs::set_permissions(&public, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(
            RemoteSubmitJournal::new(&public),
            Err(RemoteSubmitJournalError::InvalidInput)
        ));
    }

    struct Fixture {
        temp: TempDir,
        root: PathBuf,
        journal: RemoteSubmitJournal,
        spec: RemoteSubmitJournalSpec,
        command: Vec<u8>,
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("remote-submit");
            let journal = RemoteSubmitJournal::new(&root).unwrap();
            let command = br#"{"prompt":"durable submit"}"#.to_vec();
            let spec = RemoteSubmitJournalSpec {
                schema_version: SPEC_VERSION,
                submission_id: Uuid::from_u128(0x101),
                executor_execution_id: Uuid::from_u128(0x102),
                provider_id: "provider-test".to_owned(),
                provider_account_id: Uuid::from_u128(0x103),
                submit_owner: "submit-worker".to_owned(),
                submit_lease_epoch: 1,
                output_index: 0,
                output_total: 1,
                command_schema: "provider.test.v1".to_owned(),
                adapter_revision: "adapter-v1".to_owned(),
                provider_command_sha256: sha256(b"provider-command"),
                execution_binding_sha256: sha256(b"execution-binding"),
                execution_profile_id: Uuid::from_u128(0x104),
                credential_pool_id: Uuid::from_u128(0x105),
                credential_revision: 1,
                credential_auth_sha256: sha256(b"credential-auth"),
                resource_policy_id: Uuid::from_u128(0x106),
                resource_policy_revision: 1,
                provider_deadline_at_ms: 1_900_000_000_000,
                command_bytes_sha256: sha256(&command),
                command_byte_size: command.len() as u64,
            };
            Self {
                temp,
                root,
                journal,
                spec,
                command,
            }
        }

        fn release(&self) -> RemoteSubmitReleasedAuthority {
            self.journal.prepare(&self.spec, &self.command).unwrap();
            let RemoteSubmitLaunch::Launch(launch) =
                self.journal.commit_launch(&self.spec).unwrap()
            else {
                panic!("fresh journal did not grant launch authority");
            };
            let RemoteSubmitRelease::Dispatch(released) =
                self.journal.release_dispatch(&self.spec, launch).unwrap()
            else {
                panic!("fresh journal did not grant dispatch authority");
            };
            released
        }

        fn pending(&self) -> PendingOperation {
            PendingOperation::new(
                RemoteOperationRef::new(
                    &self.spec.provider_id,
                    self.spec.submission_id.to_string(),
                    "_remote.operation:1",
                )
                .unwrap(),
                Some(ProviderRequestId::new(".provider-request").unwrap()),
                Some(250),
            )
        }

        fn entry_path(&self) -> PathBuf {
            self.root.join(self.spec.submission_id.simple().to_string())
        }
    }
}
