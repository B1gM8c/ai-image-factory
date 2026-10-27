use std::{ffi::OsString, io::Read, path::Path, time::Duration};

use image_cli_runtime::{
    CommandSpec, CommandSpecError, ExitClassification, ReceiptCliPolicy, VerifiedExecutable,
    WorkingDirectory, default_exit_classification,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    DreaminaNativeImageBatchV2, DreaminaQueryReceiptV1, FramesToVideoRequestV1,
    ImageToImageRequestV1, ImageToVideoRequestV1, ImageUpscaleRequestV1,
    MultiframeToVideoRequestV1, MultimodalToVideoRequestV1, QueryResultRequestV1, ReceiptError,
    StagedInputKindV1, StagedInputRefV1, TextToImageRequestV1, TextToVideoRequestV1,
    parse_query_receipt,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DreaminaSubmitRequestV1 {
    TextToImage(TextToImageRequestV1),
    TextToVideo(TextToVideoRequestV1),
    ImageToImage(ImageToImageRequestV1),
    ImageUpscale(ImageUpscaleRequestV1),
    ImageToVideo(ImageToVideoRequestV1),
    FramesToVideo(FramesToVideoRequestV1),
    MultiframeToVideo(MultiframeToVideoRequestV1),
    MultimodalToVideo(MultimodalToVideoRequestV1),
}

impl DreaminaSubmitRequestV1 {
    pub fn output_count(&self) -> u8 {
        match self {
            Self::TextToImage(request) => request.generate_num(),
            Self::ImageToImage(request) => request.generate_num(),
            _ => 1,
        }
    }

    pub fn into_single_output(self) -> Self {
        match self {
            Self::TextToImage(request) => Self::TextToImage(request.into_single_output()),
            Self::ImageToImage(request) => Self::ImageToImage(request.into_single_output()),
            request => request,
        }
    }

    pub(crate) fn argv(&self) -> Result<Vec<OsString>, DreaminaCliPolicyError> {
        if self.output_count() != 1 {
            return Err(DreaminaCliPolicyError::BatchSubmissionUnsupported);
        }
        match self {
            Self::TextToImage(request) => Ok(request.to_argv()),
            Self::TextToVideo(request) => Ok(request.to_argv()),
            Self::ImageToImage(request) => Ok(request.to_argv()),
            Self::ImageUpscale(request) => Ok(request.to_argv()),
            Self::ImageToVideo(request) => Ok(request.to_argv()),
            Self::FramesToVideo(request) => Ok(request.to_argv()),
            Self::MultiframeToVideo(request) => Ok(request.to_argv()),
            Self::MultimodalToVideo(request) => Ok(request.to_argv()),
        }
    }

    /// Stable media-category order: images, videos, audio; frame order is preserved.
    pub fn staged_inputs(&self) -> Vec<StagedInputRefV1<'_>> {
        let images: Vec<_> = match self {
            Self::ImageToImage(r) => r.images().iter().collect(),
            Self::ImageUpscale(r) => vec![r.image()],
            Self::ImageToVideo(r) => vec![r.image()],
            Self::FramesToVideo(r) => vec![r.first(), r.last()],
            Self::MultiframeToVideo(r) => r.images().iter().collect(),
            Self::MultimodalToVideo(r) => r.images().iter().collect(),
            _ => Vec::new(),
        };
        let mut inputs: Vec<_> = images
            .into_iter()
            .enumerate()
            .map(|(index, image)| StagedInputRefV1 {
                kind: StagedInputKindV1::Image,
                index,
                filename: image.filename(),
                sha256: image.sha256(),
                dimensions: Some((image.width(), image.height())),
                duration_ms: None,
            })
            .collect();
        if let Self::MultimodalToVideo(r) = self {
            for (kind, media) in [
                (StagedInputKindV1::Video, r.videos()),
                (StagedInputKindV1::Audio, r.audios()),
            ] {
                inputs.extend(
                    media
                        .iter()
                        .enumerate()
                        .map(|(index, input)| StagedInputRefV1 {
                            kind,
                            index,
                            filename: input.filename(),
                            sha256: input.sha256(),
                            dimensions: None,
                            duration_ms: Some(input.duration_ms()),
                        }),
                );
            }
        }
        inputs
    }

    pub fn input_names(&self) -> Vec<&str> {
        self.staged_inputs()
            .into_iter()
            .map(|input| input.filename)
            .collect()
    }
}

impl From<ImageToImageRequestV1> for DreaminaSubmitRequestV1 {
    fn from(request: ImageToImageRequestV1) -> Self {
        Self::ImageToImage(request)
    }
}
impl From<ImageUpscaleRequestV1> for DreaminaSubmitRequestV1 {
    fn from(request: ImageUpscaleRequestV1) -> Self {
        Self::ImageUpscale(request)
    }
}
impl From<ImageToVideoRequestV1> for DreaminaSubmitRequestV1 {
    fn from(request: ImageToVideoRequestV1) -> Self {
        Self::ImageToVideo(request)
    }
}
impl From<FramesToVideoRequestV1> for DreaminaSubmitRequestV1 {
    fn from(request: FramesToVideoRequestV1) -> Self {
        Self::FramesToVideo(request)
    }
}
impl From<MultiframeToVideoRequestV1> for DreaminaSubmitRequestV1 {
    fn from(request: MultiframeToVideoRequestV1) -> Self {
        Self::MultiframeToVideo(request)
    }
}
impl From<MultimodalToVideoRequestV1> for DreaminaSubmitRequestV1 {
    fn from(request: MultimodalToVideoRequestV1) -> Self {
        Self::MultimodalToVideo(request)
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
    workspace_root: WorkingDirectory,
    account_home: WorkingDirectory,
    wall_timeout: Duration,
    termination_grace: Duration,
}

impl DreaminaCliPolicyV1 {
    pub fn new(
        executable_path: impl AsRef<Path>,
        executable_sha256: [u8; 32],
        workspace_root: WorkingDirectory,
        account_home: WorkingDirectory,
        wall_timeout: Duration,
        termination_grace: Duration,
    ) -> Result<Self, DreaminaCliPolicyError> {
        if wall_timeout.is_zero() || termination_grace.is_zero() {
            return Err(CommandSpecError::InvalidTimeout.into());
        }
        if workspace_root.path().starts_with(account_home.path())
            || account_home.path().starts_with(workspace_root.path())
        {
            return Err(DreaminaCliPolicyError::OverlappingDirectories);
        }
        let executable = VerifiedExecutable::new_with_sha256(executable_path, executable_sha256)?;
        Ok(Self {
            executable,
            executable_sha256,
            workspace_root,
            account_home,
            wall_timeout,
            termination_grace,
        })
    }

    pub fn executable_sha256(&self) -> [u8; 32] {
        self.executable_sha256
    }

    pub fn account_home_path(&self) -> &Path {
        self.account_home.path()
    }

    pub fn workspace_root_path(&self) -> &Path {
        self.workspace_root.path()
    }

    /// Query the same account without widening the submit policy's credential
    /// capability or reusing its expired generation deadline.
    pub fn query_policy(
        &self,
        wall_timeout: Duration,
        termination_grace: Duration,
    ) -> Result<DreaminaCliQueryPolicyV1, DreaminaCliQueryPolicyError> {
        DreaminaCliQueryPolicyV1::new(
            self.executable.path(),
            self.executable_sha256,
            self.workspace_root.clone(),
            self.account_home.clone(),
            wall_timeout,
            termination_grace,
        )
    }

    pub fn command_spec(
        &self,
        request: &DreaminaSubmitRequestV1,
    ) -> Result<CommandSpec, DreaminaCliPolicyError> {
        self.build_command(request, self.workspace_root.clone(), request.argv()?)
    }

    pub fn command_spec_in(
        &self,
        request: &DreaminaSubmitRequestV1,
        workspace: WorkingDirectory,
    ) -> Result<CommandSpec, DreaminaCliPolicyError> {
        if workspace.path().parent() != Some(self.workspace_root.path()) {
            return Err(DreaminaCliPolicyError::ExecutionWorkspaceOutsideRoot);
        }
        self.build_command(request, workspace, request.argv()?)
    }

    pub fn command_spec_native_image_batch_in(
        &self,
        batch: &DreaminaNativeImageBatchV2,
        workspace: WorkingDirectory,
    ) -> Result<CommandSpec, DreaminaCliPolicyError> {
        if workspace.path().parent() != Some(self.workspace_root.path()) {
            return Err(DreaminaCliPolicyError::ExecutionWorkspaceOutsideRoot);
        }
        self.build_command(batch.request(), workspace, batch.argv())
    }

    fn build_command(
        &self,
        request: &DreaminaSubmitRequestV1,
        workspace: WorkingDirectory,
        arguments: Vec<OsString>,
    ) -> Result<CommandSpec, DreaminaCliPolicyError> {
        for input in request.staged_inputs() {
            verify_input(workspace.path(), input)?;
        }
        let mut command = CommandSpec::new_receipt(
            self.executable.clone(),
            workspace.clone(),
            self.wall_timeout,
            self.termination_grace,
        )?
        .env("HOME", self.account_home.path().as_os_str())?
        .env("TMPDIR", workspace.path().as_os_str())?;
        #[cfg(target_os = "linux")]
        {
            command = command.env(
                "DBUS_SESSION_BUS_ADDRESS",
                crate::dreamina_secret_service_bus_address(self.account_home.path()),
            )?;
        }
        for argument in arguments {
            command = command.arg(argument)?;
        }
        Ok(command)
    }
}

#[derive(Clone, Debug)]
pub struct DreaminaCliQueryPolicyV1 {
    executable: VerifiedExecutable,
    workspace_root: WorkingDirectory,
    account_home: WorkingDirectory,
    wall_timeout: Duration,
    termination_grace: Duration,
}

impl DreaminaCliQueryPolicyV1 {
    pub fn new(
        executable_path: impl AsRef<Path>,
        executable_sha256: [u8; 32],
        workspace_root: WorkingDirectory,
        account_home: WorkingDirectory,
        wall_timeout: Duration,
        termination_grace: Duration,
    ) -> Result<Self, DreaminaCliQueryPolicyError> {
        if wall_timeout.is_zero() || termination_grace.is_zero() {
            return Err(CommandSpecError::InvalidTimeout.into());
        }
        if workspace_root.path().starts_with(account_home.path())
            || account_home.path().starts_with(workspace_root.path())
        {
            return Err(DreaminaCliQueryPolicyError::OverlappingDirectories);
        }
        let executable = VerifiedExecutable::new_with_sha256(executable_path, executable_sha256)?;
        Ok(Self {
            executable,
            workspace_root,
            account_home,
            wall_timeout,
            termination_grace,
        })
    }

    pub fn workspace_root(&self) -> &Path {
        self.workspace_root.path()
    }

    pub fn workspace_root_directory(&self) -> &WorkingDirectory {
        &self.workspace_root
    }

    pub fn command_spec(
        &self,
        request: &QueryResultRequestV1,
    ) -> Result<CommandSpec, DreaminaCliQueryPolicyError> {
        if request.download_dir().parent() != Some(self.workspace_root.path()) {
            return Err(DreaminaCliQueryPolicyError::DownloadDirectoryOutsideWorkspace);
        }
        let working_directory = if request.downloads_media() {
            self.workspace_root.clone()
        } else {
            let directory = WorkingDirectory::new_private(request.download_dir())?;
            if directory.path().parent() != Some(self.workspace_root.path()) {
                return Err(DreaminaCliQueryPolicyError::DownloadDirectoryOutsideWorkspace);
            }
            directory
        };
        let mut command = CommandSpec::new_receipt(
            self.executable.clone(),
            working_directory,
            self.wall_timeout,
            self.termination_grace,
        )?
        .require_directory(self.account_home.clone())
        .env("HOME", self.account_home.path().as_os_str())?
        .env("TMPDIR", request.download_dir().as_os_str())?;
        #[cfg(target_os = "linux")]
        {
            command = command.env(
                "DBUS_SESSION_BUS_ADDRESS",
                crate::dreamina_secret_service_bus_address(self.account_home.path()),
            )?;
        }
        for argument in request.to_argv() {
            command = command.arg(argument)?;
        }
        Ok(command)
    }
}

impl ReceiptCliPolicy for DreaminaCliQueryPolicyV1 {
    type Request = QueryResultRequestV1;
    type Receipt = DreaminaQueryReceiptV1;
    type Error = DreaminaCliQueryPolicyError;

    fn command(&self, request: &Self::Request) -> Result<CommandSpec, Self::Error> {
        self.command_spec(request)
    }

    fn classify_exit(&self, status: &std::process::ExitStatus) -> ExitClassification {
        default_exit_classification(status)
    }

    fn parse_receipt(&self, stdout: &[u8]) -> Result<Self::Receipt, Self::Error> {
        parse_query_receipt(stdout).map_err(Into::into)
    }
}

#[derive(Debug, Error)]
pub enum DreaminaCliPolicyError {
    #[error(transparent)]
    Command(#[from] CommandSpecError),
    #[error("Dreamina account home and execution workspace must not overlap")]
    OverlappingDirectories,
    #[error("Dreamina execution workspace must be one direct child of the configured root")]
    ExecutionWorkspaceOutsideRoot,
    #[error("Dreamina execution supports exactly one output per submission")]
    BatchSubmissionUnsupported,
    #[error(
        "Dreamina staged input is missing, not a regular file, or differs from its frozen digest"
    )]
    InvalidStagedInput,
}

fn verify_input(
    workspace: &Path,
    input: StagedInputRefV1<'_>,
) -> Result<(), DreaminaCliPolicyError> {
    let path = workspace.join(input.filename);
    let metadata =
        std::fs::symlink_metadata(&path).map_err(|_| DreaminaCliPolicyError::InvalidStagedInput)?;
    if !metadata.is_file() {
        return Err(DreaminaCliPolicyError::InvalidStagedInput);
    }
    let mut file =
        std::fs::File::open(&path).map_err(|_| DreaminaCliPolicyError::InvalidStagedInput)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| DreaminaCliPolicyError::InvalidStagedInput)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let digest = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if digest != input.sha256 {
        return Err(DreaminaCliPolicyError::InvalidStagedInput);
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum DreaminaCliQueryPolicyError {
    #[error(transparent)]
    Command(#[from] CommandSpecError),
    #[error("Dreamina account home and query workspace must not overlap")]
    OverlappingDirectories,
    #[error("Dreamina query download directory must be one direct workspace child")]
    DownloadDirectoryOutsideWorkspace,
    #[error(transparent)]
    Receipt(#[from] ReceiptError),
}
