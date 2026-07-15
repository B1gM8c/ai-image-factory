use std::{ffi::OsString, time::Duration};

use image_cli_runtime::{
    CommandSpec, CommandSpecError, ExitClassification, ReceiptCliPolicy, VerifiedExecutable,
    WorkingDirectory, default_exit_classification,
};
use thiserror::Error;

use crate::{
    AcceptedReceipt, ReceiptError, TextToImageRequestV1, TextToVideoRequestV1, parse_receipt,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DreaminaSubmitRequestV1 {
    TextToImage(TextToImageRequestV1),
    TextToVideo(TextToVideoRequestV1),
}

impl DreaminaSubmitRequestV1 {
    fn argv(&self) -> Result<Vec<OsString>, DreaminaCliPolicyError> {
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
    working_directory: WorkingDirectory,
    account_home: WorkingDirectory,
    wall_timeout: Duration,
    termination_grace: Duration,
}

impl DreaminaCliPolicyV1 {
    pub fn new(
        executable: VerifiedExecutable,
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
        Ok(Self {
            executable,
            working_directory,
            account_home,
            wall_timeout,
            termination_grace,
        })
    }
}

impl ReceiptCliPolicy for DreaminaCliPolicyV1 {
    type Request = DreaminaSubmitRequestV1;
    type Receipt = AcceptedReceipt;
    type Error = DreaminaCliPolicyError;

    fn command(&self, request: &Self::Request) -> Result<CommandSpec, Self::Error> {
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

    fn classify_exit(&self, status: &std::process::ExitStatus) -> ExitClassification {
        default_exit_classification(status)
    }

    fn parse_receipt(&self, stdout: &[u8]) -> Result<Self::Receipt, Self::Error> {
        parse_receipt(stdout).map_err(Into::into)
    }
}

#[derive(Debug, Error)]
pub enum DreaminaCliPolicyError {
    #[error(transparent)]
    Command(#[from] CommandSpecError),
    #[error(transparent)]
    Receipt(#[from] ReceiptError),
    #[error("Dreamina account home and execution workspace must not overlap")]
    OverlappingDirectories,
    #[error("Dreamina execution supports exactly one output per submission")]
    BatchSubmissionUnsupported,
}
