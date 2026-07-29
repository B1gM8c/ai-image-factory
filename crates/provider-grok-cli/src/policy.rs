use std::{path::Path, time::Duration};

use image_cli_runtime::{CommandSpec, CommandSpecError, VerifiedExecutable, WorkingDirectory};
use serde_json::{Value, json};
use thiserror::Error;

use crate::{GrokImageEditRequestV1, GrokImageGenerationRequestV1, GrokVideoGenerationRequestV1};

const MAX_SESSION_CWD_COMPONENT_BYTES: usize = 255;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GrokCliRequestV1 {
    ImageGeneration(GrokImageGenerationRequestV1),
    ImageEdit(GrokImageEditRequestV1),
    VideoGeneration(GrokVideoGenerationRequestV1),
}

impl From<GrokImageGenerationRequestV1> for GrokCliRequestV1 {
    fn from(request: GrokImageGenerationRequestV1) -> Self {
        Self::ImageGeneration(request)
    }
}

impl From<GrokImageEditRequestV1> for GrokCliRequestV1 {
    fn from(request: GrokImageEditRequestV1) -> Self {
        Self::ImageEdit(request)
    }
}

impl From<GrokVideoGenerationRequestV1> for GrokCliRequestV1 {
    fn from(request: GrokVideoGenerationRequestV1) -> Self {
        Self::VideoGeneration(request)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GrokTool {
    ImageGeneration,
    ImageEdit,
    ImageToVideo,
    ReferenceToVideo,
}

impl GrokTool {
    pub const fn name(self) -> &'static str {
        match self {
            Self::ImageGeneration => "image_gen",
            Self::ImageEdit => "image_edit",
            Self::ImageToVideo => "image_to_video",
            Self::ReferenceToVideo => "reference_to_video",
        }
    }

    const fn artifact_folder(self) -> &'static str {
        match self {
            Self::ImageGeneration | Self::ImageEdit => "images",
            Self::ImageToVideo | Self::ReferenceToVideo => "videos",
        }
    }

    const fn artifact_filename(self) -> &'static str {
        match self {
            Self::ImageGeneration | Self::ImageEdit => "1.jpg",
            Self::ImageToVideo | Self::ReferenceToVideo => "1.mp4",
        }
    }
}

#[derive(Clone, Debug)]
pub struct GrokInvocationV1 {
    session_id: String,
    tool: GrokTool,
    expected_arguments: Value,
    session_directory: std::path::PathBuf,
    history_path: std::path::PathBuf,
    artifact_path: std::path::PathBuf,
}

impl GrokInvocationV1 {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn tool(&self) -> GrokTool {
        self.tool
    }

    pub fn expected_arguments(&self) -> &Value {
        &self.expected_arguments
    }

    pub fn session_directory(&self) -> &Path {
        &self.session_directory
    }

    pub fn history_path(&self) -> &Path {
        &self.history_path
    }

    pub fn artifact_path(&self) -> &Path {
        &self.artifact_path
    }
}

#[derive(Clone, Debug)]
pub struct GrokCliPolicyV1 {
    executable: VerifiedExecutable,
    executable_sha256: [u8; 32],
    workspace_root: WorkingDirectory,
    runtime_home: WorkingDirectory,
    grok_home: WorkingDirectory,
    wall_timeout: Duration,
    termination_grace: Duration,
}

impl GrokCliPolicyV1 {
    pub fn new(
        executable_path: impl AsRef<Path>,
        executable_sha256: [u8; 32],
        workspace_root: WorkingDirectory,
        runtime_home: WorkingDirectory,
        grok_home: WorkingDirectory,
        wall_timeout: Duration,
        termination_grace: Duration,
    ) -> Result<Self, GrokCliPolicyError> {
        if wall_timeout.is_zero() || termination_grace.is_zero() {
            return Err(CommandSpecError::InvalidTimeout.into());
        }
        ensure_disjoint_directories(&workspace_root, &runtime_home, &grok_home)?;
        let executable = VerifiedExecutable::new_with_sha256(executable_path, executable_sha256)?;
        Ok(Self {
            executable,
            executable_sha256,
            workspace_root,
            runtime_home,
            grok_home,
            wall_timeout,
            termination_grace,
        })
    }

    pub fn executable_sha256(&self) -> [u8; 32] {
        self.executable_sha256
    }

    pub fn command_spec_in(
        &self,
        request: &GrokCliRequestV1,
        session_id: &str,
        workspace: WorkingDirectory,
    ) -> Result<(CommandSpec, GrokInvocationV1), GrokCliPolicyError> {
        if workspace.path().parent() != Some(self.workspace_root.path()) {
            return Err(GrokCliPolicyError::ExecutionWorkspaceOutsideRoot);
        }
        validate_session_id(session_id)?;

        let invocation =
            build_invocation(request, session_id, workspace.path(), self.grok_home.path())?;
        if invocation.session_directory.exists() {
            return Err(GrokCliPolicyError::SessionAlreadyExists);
        }

        let prompt = dispatch_prompt(invocation.tool, &invocation.expected_arguments)?;
        let mut command = CommandSpec::new_receipt(
            self.executable.clone(),
            workspace.clone(),
            self.wall_timeout,
            self.termination_grace,
        )?
        .require_directory(self.runtime_home.clone())
        .require_directory(self.grok_home.clone())
        .env("HOME", self.runtime_home.path().as_os_str())?
        .env("GROK_HOME", self.grok_home.path().as_os_str())?
        .env("TMPDIR", workspace.path().as_os_str())?
        .env("NO_COLOR", "1")?
        .env("TERM", "dumb")?
        .arg("--cwd")?
        .arg(workspace.path().as_os_str())?
        .arg("--no-memory")?
        .arg("--no-plan")?
        .arg("--no-subagents")?
        .arg("--disable-web-search")?
        .arg("--always-approve")?
        .arg("--tools")?
        .arg(invocation.tool.name())?
        .arg("--max-turns")?
        .arg("3")?
        .arg("--no-wait-for-background")?
        .arg("--session-id")?
        .arg(session_id)?
        .arg("--output-format")?
        .arg("streaming-json")?
        .arg("--prompt-file")?
        .arg("/dev/stdin")?
        .stdin(prompt.into_bytes())?;

        if let GrokCliRequestV1::ImageGeneration(request) = request {
            command = command.env("GROK_IMAGE_GEN_MODEL_OVERRIDE", request.model().as_str())?;
        } else if matches!(request, GrokCliRequestV1::VideoGeneration(_)) {
            command = command.env("GROK_DISABLE_ZDR_INCOMPATIBLE_TOOLS", "true")?;
        }
        Ok((command, invocation))
    }
}

#[derive(Debug, Error)]
pub enum GrokCliPolicyError {
    #[error(transparent)]
    Command(#[from] CommandSpecError),
    #[error("Grok runtime home, credential home, and workspace root must not overlap")]
    OverlappingDirectories,
    #[error("Grok execution workspace must be one direct child of the configured root")]
    ExecutionWorkspaceOutsideRoot,
    #[error("Grok session ID must be a canonical lowercase UUID")]
    InvalidSessionId,
    #[error("Grok execution workspace is not valid UTF-8")]
    NonUtf8Workspace,
    #[error("Grok CLI long-workspace session encoding is intentionally unsupported")]
    LongWorkspacePathUnsupported,
    #[error("Grok session directory already exists")]
    SessionAlreadyExists,
    #[error("Grok dispatch prompt serialization failed")]
    PromptSerialization,
}

fn ensure_disjoint_directories(
    workspace_root: &WorkingDirectory,
    runtime_home: &WorkingDirectory,
    grok_home: &WorkingDirectory,
) -> Result<(), GrokCliPolicyError> {
    let directories = [workspace_root.path(), runtime_home.path(), grok_home.path()];
    for (index, left) in directories.iter().enumerate() {
        for right in &directories[index + 1..] {
            if left.starts_with(right) || right.starts_with(left) {
                return Err(GrokCliPolicyError::OverlappingDirectories);
            }
        }
    }
    Ok(())
}

fn validate_session_id(session_id: &str) -> Result<(), GrokCliPolicyError> {
    let bytes = session_id.as_bytes();
    let valid = bytes.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes[index] == b'-')
        && bytes.iter().enumerate().all(|(index, byte)| {
            [8, 13, 18, 23].contains(&index) || byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
        });
    if !valid {
        return Err(GrokCliPolicyError::InvalidSessionId);
    }
    Ok(())
}

fn build_invocation(
    request: &GrokCliRequestV1,
    session_id: &str,
    workspace: &Path,
    grok_home: &Path,
) -> Result<GrokInvocationV1, GrokCliPolicyError> {
    let workspace_text = workspace
        .to_str()
        .ok_or(GrokCliPolicyError::NonUtf8Workspace)?;
    let encoded_workspace = urlencoding::encode(workspace_text);
    if encoded_workspace.len() > MAX_SESSION_CWD_COMPONENT_BYTES {
        return Err(GrokCliPolicyError::LongWorkspacePathUnsupported);
    }

    let (tool, expected_arguments) = expected_tool_call(request, workspace);
    let session_directory = grok_home
        .join("sessions")
        .join(encoded_workspace.as_ref())
        .join(session_id);
    let artifact_path = session_directory
        .join(tool.artifact_folder())
        .join(tool.artifact_filename());
    let history_path = session_directory.join("chat_history.jsonl");
    Ok(GrokInvocationV1 {
        session_id: session_id.to_owned(),
        tool,
        expected_arguments,
        session_directory,
        history_path,
        artifact_path,
    })
}

fn expected_tool_call(request: &GrokCliRequestV1, workspace: &Path) -> (GrokTool, Value) {
    match request {
        GrokCliRequestV1::ImageGeneration(request) => (
            GrokTool::ImageGeneration,
            json!({
                "prompt": request.prompt(),
                "aspect_ratio": request.aspect_ratio().as_str(),
            }),
        ),
        GrokCliRequestV1::ImageEdit(request) => (
            GrokTool::ImageEdit,
            json!({
                "prompt": request.prompt(),
                "image": absolute_image_paths(workspace, request.images()),
                "aspect_ratio": request.aspect_ratio().as_str(),
            }),
        ),
        GrokCliRequestV1::VideoGeneration(GrokVideoGenerationRequestV1::ImageToVideo(request)) => (
            GrokTool::ImageToVideo,
            json!({
                "prompt": request.prompt(),
                "image": workspace.join(request.image().filename()),
                "duration": request.duration().seconds(),
                "resolution_name": request.resolution().as_str(),
            }),
        ),
        GrokCliRequestV1::VideoGeneration(GrokVideoGenerationRequestV1::ReferenceToVideo(
            request,
        )) => (
            GrokTool::ReferenceToVideo,
            json!({
                "prompt": request.prompt(),
                "images": absolute_image_paths(workspace, request.images()),
                "aspect_ratio": request.aspect_ratio().as_str(),
                "duration": request.duration().seconds(),
                "resolution_name": request.resolution().as_str(),
            }),
        ),
    }
}

fn absolute_image_paths(
    workspace: &Path,
    images: &[crate::StagedImageV1],
) -> Vec<std::path::PathBuf> {
    images
        .iter()
        .map(|image| workspace.join(image.filename()))
        .collect()
}

fn dispatch_prompt(tool: GrokTool, arguments: &Value) -> Result<String, GrokCliPolicyError> {
    let arguments =
        serde_json::to_string(arguments).map_err(|_| GrokCliPolicyError::PromptSerialization)?;
    Ok(format!(
        "Call the enabled `{}` tool exactly once with exactly this JSON object as its arguments:\n{}\nDo not call any other tool. After the tool result, end immediately.",
        tool.name(),
        arguments
    ))
}
