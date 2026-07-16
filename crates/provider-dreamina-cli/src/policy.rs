use std::{ffi::OsString, path::Path, time::Duration};

use image_cli_runtime::{CommandSpec, CommandSpecError, VerifiedExecutable, WorkingDirectory};
use thiserror::Error;

use crate::{TextToImageRequestV1, TextToVideoRequestV1};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DreaminaSubmitRequestV1 {
    TextToImage(TextToImageRequestV1),
    TextToVideo(TextToVideoRequestV1),
}

impl DreaminaSubmitRequestV1 {
    pub(crate) fn argv(&self) -> Result<Vec<OsString>, DreaminaCliPolicyError> {
        match self {
            Self::TextToImage(request) if request.generate_num() != 1 => {
                Err(DreaminaCliPolicyError::BatchSubmissionUnsupported)
            }
            Self::TextToImage(request) => Ok(request.to_argv()),
            Self::TextToVideo(request) => Ok(request.to_argv()),
        }
    }
}

impl From<TextToImageRequestV1> for DreaminaSubmitRequestV1 {
    fn from(request: TextToImageRequestV1) -> Self {
        Self::TextToImage(request)
    }
}

impl From<TextToVideoRequestV1> for DreaminaSubmitRequestV1 {
    fn from(request: TextToVideoRequestV1) -> Self {
        Self::TextToVideo(request)
    }
}

#[derive(Clone, Debug)]
pub struct DreaminaCliPolicyV1 {
    executable: VerifiedExecutable,
    executable_sha256: [u8; 32],
    working_directory: WorkingDirectory,
    account_home: WorkingDirectory,
    wall_timeout: Duration,
    termination_grace: Duration,
}

impl DreaminaCliPolicyV1 {
    pub fn new(
        executable_path: impl AsRef<Path>,
        executable_sha256: [u8; 32],
        working_directory: WorkingDirectory,
        account_home: WorkingDirectory,
        wall_timeout: Duration,
        termination_grace: Duration,
    ) -> Result<Self, DreaminaCliPolicyError> {
        if wall_timeout.is_zero() || termination_grace.is_zero() {
            return Err(CommandSpecError::InvalidTimeout.into());
        }
        if working_directory.path().starts_with(account_home.path())
            || account_home.path().starts_with(working_directory.path())
        {
            return Err(DreaminaCliPolicyError::OverlappingDirectories);
        }
        let executable = VerifiedExecutable::new_with_sha256(executable_path, executable_sha256)?;
        Ok(Self {
            executable,
            executable_sha256,
            working_directory,
            account_home,
            wall_timeout,
            termination_grace,
        })
    }

    pub fn executable_sha256(&self) -> [u8; 32] {
        self.executable_sha256
    }

    pub fn command_spec(
        &self,
        request: &DreaminaSubmitRequestV1,
    ) -> Result<CommandSpec, DreaminaCliPolicyError> {
        let mut command = CommandSpec::new_receipt(
            self.executable.clone(),
            self.working_directory.clone(),
            self.wall_timeout,
            self.termination_grace,
        )?
        .env("HOME", self.account_home.path().as_os_str())?
        .env("TMPDIR", self.working_directory.path().as_os_str())?;
        for argument in request.argv()? {
            command = command.arg(argument)?;
        }
        Ok(command)
    }
}

#[derive(Debug, Error)]
pub enum DreaminaCliPolicyError {
    #[error(transparent)]
    Command(#[from] CommandSpecError),
    #[error("Dreamina account home and execution workspace must not overlap")]
    OverlappingDirectories,
    #[error("Dreamina execution supports exactly one output per submission")]
    BatchSubmissionUnsupported,
}
