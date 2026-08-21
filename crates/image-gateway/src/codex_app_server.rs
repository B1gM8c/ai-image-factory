use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
};
use uuid::Uuid;

use crate::runner::process::{CodexExtensionOutputRoot, ProcessSpoolError};

const MAX_CODEX_OUTPUT_BYTES: u64 = 32 * 1024 * 1024;
const MAX_PROTOCOL_LINE_BYTES: usize = 48 * 1024 * 1024;
const MAX_PROTOCOL_CAPTURE_BYTES: usize = 64 * 1024 * 1024;
const MAX_PROTOCOL_REQUEST_BYTES: usize = 1024 * 1024;
const REAP_TIMEOUT: Duration = Duration::from_secs(5);

const IMAGE_GENERATION_DEVELOPER_INSTRUCTIONS: &str = "For this thread, image requests MUST invoke the enabled namespaced tool image_gen.imagegen (wire name image_gen__imagegen) exactly once. Never answer an image request with text only. Do not use shell or local programs to create, copy, move, rename, edit, or delete the generated artifact. After the image tool completes, stop.";

pub(crate) struct CodexAppServerRequest<'a> {
    pub(crate) executable: &'a Path,
    pub(crate) workspace: &'a Path,
    pub(crate) codex_home: &'a Path,
    pub(crate) prompt: &'a str,
    pub(crate) input_paths: &'a [PathBuf],
    pub(crate) timeout: Duration,
    pub(crate) environment: &'a [(String, String)],
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum CodexAppServerError {
    Unavailable,
    SpawnIdentity,
    Stdin,
    Timeout,
    Protocol,
    ProcessExited,
    RequestRejected,
    TurnFailed,
    NoImage,
    ImageIncomplete,
    MultipleImages,
    OutputMissing,
    OutputInvalid,
    OutputUnavailable,
}

impl CodexAppServerError {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::Unavailable => "service_unavailable",
            Self::SpawnIdentity => "codex_process_identity_unavailable",
            Self::Stdin => "codex_stdin_failed",
            Self::Timeout => "codex_timeout",
            Self::Protocol => "codex_event_capture_invalid",
            Self::ProcessExited => "codex_process_exited_without_terminal",
            Self::RequestRejected | Self::TurnFailed => "codex_cli_failed",
            Self::NoImage => "codex_no_image_output",
            Self::ImageIncomplete | Self::OutputMissing | Self::OutputInvalid => {
                "codex_image_output_disappeared"
            }
            Self::MultipleImages => "codex_multiple_image_outputs",
            Self::OutputUnavailable => "service_unavailable",
        }
    }
}

#[derive(Default)]
struct ProtocolState {
    thread_id: Option<Uuid>,
    announced_thread_id: Option<Uuid>,
    turn_id: Option<String>,
    announced_turn_id: Option<String>,
    saw_image_generation: bool,
    started_image_call_id: Option<String>,
    started_image_count: usize,
    completed_image_call_id: Option<String>,
    completed_image_count: usize,
    image_failed: bool,
    image_incomplete: bool,
}

impl ProtocolState {
    fn bind_thread(&mut self, thread_id: Uuid) -> Result<(), CodexAppServerError> {
        if self
            .thread_id
            .replace(thread_id)
            .is_some_and(|value| value != thread_id)
            || self
                .announced_thread_id
                .is_some_and(|value| value != thread_id)
        {
            return Err(CodexAppServerError::Protocol);
        }
        Ok(())
    }

    fn bind_turn(&mut self, turn_id: String) -> Result<(), CodexAppServerError> {
        if !valid_turn_id(&turn_id)
            || self
                .turn_id
                .as_deref()
                .is_some_and(|value| value != turn_id)
            || self
                .announced_turn_id
                .as_deref()
                .is_some_and(|value| value != turn_id)
        {
            return Err(CodexAppServerError::Protocol);
        }
        self.turn_id = Some(turn_id);
        Ok(())
    }

    fn observe_notification(
        &mut self,
        message: &Value,
        codex_home: &Path,
    ) -> Result<bool, CodexAppServerError> {
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .ok_or(CodexAppServerError::Protocol)?;
        let params = message.get("params").unwrap_or(&Value::Null);
        match method {
            "thread/started" => {
                let candidate = params
                    .pointer("/thread/id")
                    .and_then(Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok())
                    .ok_or(CodexAppServerError::Protocol)?;
                if self
                    .announced_thread_id
                    .replace(candidate)
                    .is_some_and(|value| value != candidate)
                    || self.thread_id.is_some_and(|value| value != candidate)
                {
                    return Err(CodexAppServerError::Protocol);
                }
            }
            "turn/started" => {
                let candidate = params
                    .pointer("/turn/id")
                    .and_then(Value::as_str)
                    .ok_or(CodexAppServerError::Protocol)?;
                self.observe_turn_identity(params, candidate)?;
                if self
                    .announced_turn_id
                    .as_deref()
                    .is_some_and(|value| value != candidate)
                {
                    return Err(CodexAppServerError::Protocol);
                }
                self.announced_turn_id = Some(candidate.to_string());
            }
            "item/started" | "item/completed" => {
                let item_type = params
                    .pointer("/item/type")
                    .and_then(Value::as_str)
                    .ok_or(CodexAppServerError::Protocol)?;
                self.observe_bound_identity(params)?;
                if item_type != "imageGeneration" {
                    return if matches!(item_type, "userMessage" | "reasoning" | "agentMessage") {
                        Ok(false)
                    } else {
                        Err(CodexAppServerError::Protocol)
                    };
                }
                self.saw_image_generation = true;
                let call_id = params
                    .pointer("/item/id")
                    .and_then(Value::as_str)
                    .filter(|value| valid_call_id(value))
                    .ok_or(CodexAppServerError::Protocol)?;
                if method == "item/started" {
                    self.started_image_count = self.started_image_count.saturating_add(1);
                    if self.started_image_count > 1 {
                        return Err(CodexAppServerError::MultipleImages);
                    }
                    if self
                        .started_image_call_id
                        .as_deref()
                        .is_some_and(|value| value != call_id)
                    {
                        return Err(CodexAppServerError::MultipleImages);
                    }
                    self.started_image_call_id = Some(call_id.to_string());
                    return Ok(false);
                }

                self.completed_image_count = self.completed_image_count.saturating_add(1);
                if self.completed_image_count > 1
                    || self
                        .started_image_call_id
                        .as_deref()
                        .is_some_and(|value| value != call_id)
                    || self
                        .completed_image_call_id
                        .as_deref()
                        .is_some_and(|value| value != call_id)
                {
                    return Err(CodexAppServerError::MultipleImages);
                }
                self.completed_image_call_id = Some(call_id.to_string());
                match params.pointer("/item/status").and_then(Value::as_str) {
                    Some("completed") => {
                        if params
                            .pointer("/item/result")
                            .and_then(Value::as_str)
                            .is_none_or(str::is_empty)
                        {
                            return Err(CodexAppServerError::Protocol);
                        }
                        let saved_path = params
                            .pointer("/item/savedPath")
                            .and_then(Value::as_str)
                            .ok_or(CodexAppServerError::ImageIncomplete)?;
                        let thread_id = self.thread_id.ok_or(CodexAppServerError::Protocol)?;
                        let expected = codex_home
                            .join("generated_images")
                            .join(thread_id.to_string())
                            .join(format!("{call_id}.png"));
                        if Path::new(saved_path) != expected {
                            return Err(CodexAppServerError::Protocol);
                        }
                    }
                    Some("failed") => self.image_failed = true,
                    _ => self.image_incomplete = true,
                }
            }
            "turn/completed" => {
                let candidate = params
                    .pointer("/turn/id")
                    .and_then(Value::as_str)
                    .ok_or(CodexAppServerError::Protocol)?;
                self.observe_turn_identity(params, candidate)?;
                return match params.pointer("/turn/status").and_then(Value::as_str) {
                    Some("completed") => Ok(true),
                    Some("failed" | "interrupted") => Err(CodexAppServerError::TurnFailed),
                    _ => Err(CodexAppServerError::Protocol),
                };
            }
            "error" => return Err(CodexAppServerError::RequestRejected),
            _ => {}
        }
        Ok(false)
    }

    fn observe_bound_identity(&self, params: &Value) -> Result<(), CodexAppServerError> {
        let expected_thread = self.thread_id.ok_or(CodexAppServerError::Protocol)?;
        let expected_turn = self
            .turn_id
            .as_deref()
            .or(self.announced_turn_id.as_deref())
            .ok_or(CodexAppServerError::Protocol)?;
        let actual_thread = params
            .get("threadId")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or(CodexAppServerError::Protocol)?;
        let actual_turn = params
            .get("turnId")
            .and_then(Value::as_str)
            .ok_or(CodexAppServerError::Protocol)?;
        if actual_thread != expected_thread || actual_turn != expected_turn {
            return Err(CodexAppServerError::Protocol);
        }
        Ok(())
    }

    fn observe_turn_identity(
        &self,
        params: &Value,
        candidate_turn: &str,
    ) -> Result<(), CodexAppServerError> {
        let actual_thread = params
            .get("threadId")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or(CodexAppServerError::Protocol)?;
        if self.thread_id.is_some_and(|value| value != actual_thread)
            || self
                .turn_id
                .as_deref()
                .is_some_and(|value| value != candidate_turn)
            || !valid_turn_id(candidate_turn)
        {
            return Err(CodexAppServerError::Protocol);
        }
        Ok(())
    }

    fn authority(&self) -> Result<(String, String), CodexAppServerError> {
        if self.thread_id != self.announced_thread_id || self.turn_id != self.announced_turn_id {
            return Err(CodexAppServerError::Protocol);
        }
        if self.image_failed {
            return Err(CodexAppServerError::TurnFailed);
        }
        if self.image_incomplete
            || (self.saw_image_generation
                && (self.started_image_count != 1 || self.completed_image_count != 1))
        {
            return Err(CodexAppServerError::ImageIncomplete);
        }
        if self.completed_image_count == 0 {
            return Err(CodexAppServerError::NoImage);
        }
        if self.started_image_count != 1 || self.completed_image_count != 1 {
            return Err(CodexAppServerError::MultipleImages);
        }
        Ok((
            self.thread_id
                .ok_or(CodexAppServerError::Protocol)?
                .to_string(),
            self.completed_image_call_id
                .clone()
                .ok_or(CodexAppServerError::Protocol)?,
        ))
    }
}

pub(crate) async fn run_codex_app_server<F>(
    request: CodexAppServerRequest<'_>,
    on_spawn: F,
) -> Result<Vec<u8>, CodexAppServerError>
where
    F: FnOnce(u32) -> Result<(), ()>,
{
    if request.timeout.is_zero()
        || !request.workspace.is_absolute()
        || !request.codex_home.is_absolute()
    {
        return Err(CodexAppServerError::Protocol);
    }
    let workspace =
        std::fs::canonicalize(request.workspace).map_err(|_| CodexAppServerError::Protocol)?;
    let codex_home =
        std::fs::canonicalize(request.codex_home).map_err(|_| CodexAppServerError::Protocol)?;
    let native_output_root =
        CodexExtensionOutputRoot::open(&codex_home).map_err(map_output_root_error)?;
    let mut command = Command::new(request.executable);
    command
        .arg("app-server")
        .arg("--listen")
        .arg("stdio://")
        .arg("--enable")
        .arg("image_generation")
        .arg("--disable")
        .arg("plugins")
        .arg("--disable")
        .arg("apps")
        .arg("--disable")
        .arg("shell_tool")
        .arg("--disable")
        .arg("unified_exec")
        .env_clear()
        .current_dir(&workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    configure_process_group(&mut command);
    for (name, value) in request.environment {
        command.env(name, value);
    }
    command
        .env("CODEX_HOME", &codex_home)
        .env("HOME", &codex_home)
        .env("TMPDIR", &workspace);

    let mut child = command
        .spawn()
        .map_err(|_| CodexAppServerError::Unavailable)?;
    let pid = child.id().ok_or(CodexAppServerError::SpawnIdentity)?;
    if on_spawn(pid).is_err() {
        terminate_child(&mut child).await;
        return Err(CodexAppServerError::SpawnIdentity);
    }
    let mut stdin = child.stdin.take().ok_or(CodexAppServerError::Stdin)?;
    let stdout = child.stdout.take().ok_or(CodexAppServerError::Protocol)?;
    let stderr = child.stderr.take().ok_or(CodexAppServerError::Protocol)?;
    let stderr_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr);
        let mut buffer = [0_u8; 8192];
        loop {
            match tokio::io::AsyncReadExt::read(&mut reader, &mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    });
    let mut stdout = BufReader::new(stdout);
    let mut state = ProtocolState::default();
    let mut capture_bytes = 0_usize;

    let protocol_result = tokio::time::timeout(request.timeout, async {
        send_message(
            &mut stdin,
            &json!({
                "id": 1,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": "ai-image-factory",
                        "title": "AI Image Factory",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {
                        "experimentalApi": false,
                        "requestAttestation": false,
                        "mcpServerOpenaiFormElicitation": false
                    }
                }
            }),
        )
        .await?;
        let initialize =
            wait_for_response(&mut stdout, &mut state, &codex_home, &mut capture_bytes, 1).await?;
        if initialize
            .get("codexHome")
            .and_then(Value::as_str)
            .is_none_or(|value| Path::new(value) != codex_home)
        {
            return Err(CodexAppServerError::Protocol);
        }
        send_message(&mut stdin, &json!({"method": "initialized"})).await?;

        send_message(
            &mut stdin,
            &json!({
                "id": 2,
                "method": "thread/start",
                "params": {
                    "cwd": workspace,
                    "approvalPolicy": "never",
                    "sandbox": "workspace-write",
                    "ephemeral": true,
                    "developerInstructions": IMAGE_GENERATION_DEVELOPER_INSTRUCTIONS
                }
            }),
        )
        .await?;
        let thread =
            wait_for_response(&mut stdout, &mut state, &codex_home, &mut capture_bytes, 2).await?;
        let thread_id = thread
            .pointer("/thread/id")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or(CodexAppServerError::Protocol)?;
        state.bind_thread(thread_id)?;

        let mut input = vec![json!({
            "type": "text",
            "text": request.prompt,
            "textElements": []
        })];
        input.extend(request.input_paths.iter().map(|path| {
            json!({
                "type": "localImage",
                "path": path,
                "detail": "original"
            })
        }));
        send_message(
            &mut stdin,
            &json!({
                "id": 3,
                "method": "turn/start",
                "params": {
                    "threadId": thread_id,
                    "clientUserMessageId": null,
                    "input": input,
                    "cwd": workspace,
                    "approvalPolicy": "never"
                }
            }),
        )
        .await?;
        let turn =
            wait_for_response(&mut stdout, &mut state, &codex_home, &mut capture_bytes, 3).await?;
        let turn_id = turn
            .pointer("/turn/id")
            .and_then(Value::as_str)
            .ok_or(CodexAppServerError::Protocol)?
            .to_string();
        state.bind_turn(turn_id)?;

        loop {
            let message = read_message(&mut stdout, &mut capture_bytes).await?;
            if message.get("id").is_some() {
                return Err(CodexAppServerError::Protocol);
            }
            if state.observe_notification(&message, &codex_home)? {
                break;
            }
        }
        drop(stdin);
        while let Some(message) = read_optional_message(&mut stdout, &mut capture_bytes).await? {
            if message.get("id").is_some() || message.get("method").is_none() {
                return Err(CodexAppServerError::Protocol);
            }
            if matches!(
                message.get("method").and_then(Value::as_str),
                Some(
                    "thread/started"
                        | "turn/started"
                        | "item/started"
                        | "item/completed"
                        | "turn/completed"
                        | "error"
                )
            ) {
                return Err(CodexAppServerError::Protocol);
            }
            if state.observe_notification(&message, &codex_home)? {
                return Err(CodexAppServerError::Protocol);
            }
        }
        state.authority()
    })
    .await;

    let authority = match protocol_result {
        Ok(Ok(authority)) => authority,
        Ok(Err(error)) => {
            terminate_child(&mut child).await;
            let _ = tokio::time::timeout(REAP_TIMEOUT, stderr_task).await;
            return Err(error);
        }
        Err(_) => {
            terminate_child(&mut child).await;
            let _ = tokio::time::timeout(REAP_TIMEOUT, stderr_task).await;
            return Err(CodexAppServerError::Timeout);
        }
    };

    let (thread_id, call_id) = authority;
    terminate_child(&mut child).await;
    let _ = tokio::time::timeout(REAP_TIMEOUT, stderr_task).await;
    let output = tokio::task::spawn_blocking(move || {
        native_output_root.read(&thread_id, &call_id, MAX_CODEX_OUTPUT_BYTES)
    })
    .await
    .map_err(|_| CodexAppServerError::OutputUnavailable)?
    .map_err(map_output_read_error)?
    .ok_or(CodexAppServerError::OutputMissing);
    output
}

async fn wait_for_response<R: AsyncBufRead + Unpin>(
    stdout: &mut R,
    state: &mut ProtocolState,
    codex_home: &Path,
    capture_bytes: &mut usize,
    expected_id: i64,
) -> Result<Value, CodexAppServerError> {
    loop {
        let message = read_message(stdout, capture_bytes).await?;
        if message.get("method").is_some() && message.get("id").is_none() {
            if state.observe_notification(&message, codex_home)? {
                return Err(CodexAppServerError::Protocol);
            }
            continue;
        }
        if message.get("id").and_then(Value::as_i64) != Some(expected_id) {
            return Err(CodexAppServerError::Protocol);
        }
        match (message.get("result"), message.get("error")) {
            (Some(result), None) => return Ok(result.clone()),
            (None, Some(_)) => return Err(CodexAppServerError::RequestRejected),
            _ => return Err(CodexAppServerError::Protocol),
        }
    }
}

async fn send_message(stdin: &mut ChildStdin, message: &Value) -> Result<(), CodexAppServerError> {
    let bytes = serde_json::to_vec(message).map_err(|_| CodexAppServerError::Protocol)?;
    if bytes.len() > MAX_PROTOCOL_REQUEST_BYTES {
        return Err(CodexAppServerError::Protocol);
    }
    stdin
        .write_all(&bytes)
        .await
        .map_err(|_| CodexAppServerError::Stdin)?;
    stdin
        .write_all(b"\n")
        .await
        .map_err(|_| CodexAppServerError::Stdin)?;
    stdin.flush().await.map_err(|_| CodexAppServerError::Stdin)
}

async fn read_message<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    capture_bytes: &mut usize,
) -> Result<Value, CodexAppServerError> {
    let line = read_bounded_line(reader)
        .await?
        .ok_or(CodexAppServerError::ProcessExited)?;
    record_capture_bytes(capture_bytes, line.len())?;
    serde_json::from_slice(&line).map_err(|_| CodexAppServerError::Protocol)
}

async fn read_optional_message<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    capture_bytes: &mut usize,
) -> Result<Option<Value>, CodexAppServerError> {
    let Some(line) = read_bounded_line(reader).await? else {
        return Ok(None);
    };
    record_capture_bytes(capture_bytes, line.len())?;
    serde_json::from_slice(&line)
        .map(Some)
        .map_err(|_| CodexAppServerError::Protocol)
}

fn record_capture_bytes(total: &mut usize, bytes: usize) -> Result<(), CodexAppServerError> {
    *total = total
        .checked_add(bytes)
        .ok_or(CodexAppServerError::Protocol)?;
    if *total > MAX_PROTOCOL_CAPTURE_BYTES {
        return Err(CodexAppServerError::Protocol);
    }
    Ok(())
}

async fn read_bounded_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Vec<u8>>, CodexAppServerError> {
    let mut line = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|_| CodexAppServerError::Protocol)?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(CodexAppServerError::ProcessExited)
            };
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if line.len().saturating_add(take) > MAX_PROTOCOL_LINE_BYTES {
            return Err(CodexAppServerError::Protocol);
        }
        line.extend_from_slice(&available[..take]);
        let complete = available.get(take.saturating_sub(1)) == Some(&b'\n');
        reader.consume(take);
        if complete {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.is_empty() {
                return Err(CodexAppServerError::Protocol);
            }
            return Ok(Some(line));
        }
    }
}

fn valid_call_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255 - ".png".len()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_turn_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn map_output_root_error(error: ProcessSpoolError) -> CodexAppServerError {
    match error {
        ProcessSpoolError::Unavailable => CodexAppServerError::OutputUnavailable,
        ProcessSpoolError::InvalidInput
        | ProcessSpoolError::Conflict
        | ProcessSpoolError::Integrity => CodexAppServerError::OutputInvalid,
    }
}

fn map_output_read_error(error: ProcessSpoolError) -> CodexAppServerError {
    map_output_root_error(error)
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    command.process_group(0);
    unsafe {
        command.pre_exec(|| {
            libc::umask(0o077);
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

async fn terminate_child(child: &mut Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
    }
    let _ = child.start_kill();
    let _ = tokio::time::timeout(REAP_TIMEOUT, child.wait()).await;
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use tempfile::TempDir;

    use super::*;

    const THREAD_ID: &str = "019fd666-0416-7da2-bcc3-7f2f51efd3c8";
    const TURN_ID: &str = "019fd666-0416-7da2-bcc3-7f2f51efd3c9";
    const CALL_ID: &str = "call_exact_image";

    #[derive(Clone, Copy)]
    enum FakeMode {
        Normal,
        NoImage,
        MultipleImages,
        TransientOutput,
        ReplacedOutput,
        MalformedSuffix,
        JoinTimeout,
        LateImage,
    }

    struct FakeAppServer {
        _root: TempDir,
        executable: PathBuf,
        workspace: PathBuf,
        codex_home: PathBuf,
        expected: Vec<u8>,
    }

    impl FakeAppServer {
        fn new(mode: FakeMode) -> Self {
            Self::new_with_payload(mode, b"first-native-image")
        }

        fn new_with_payload(mode: FakeMode, payload: &[u8]) -> Self {
            let root = TempDir::new().unwrap();
            let executable = root.path().join("fake-codex");
            let workspace = root.path().join("workspace");
            let codex_home = root.path().join("codex-home");
            let source = root.path().join("source.png");
            let replacement = root.path().join("replacement.png");
            std::fs::create_dir(&workspace).unwrap();
            std::fs::create_dir(&codex_home).unwrap();
            std::fs::set_permissions(&workspace, std::fs::Permissions::from_mode(0o700)).unwrap();
            std::fs::set_permissions(&codex_home, std::fs::Permissions::from_mode(0o700)).unwrap();
            std::fs::write(&source, payload).unwrap();
            std::fs::write(&replacement, b"replacement-native-image").unwrap();

            let output_action = match mode {
                FakeMode::NoImage | FakeMode::LateImage => String::new(),
                FakeMode::TransientOutput => format!(
                    "/bin/cp '{}' \"$output_path\"\n/bin/rm \"$output_path\"\n",
                    source.display()
                ),
                FakeMode::ReplacedOutput => format!(
                    "/bin/cp '{}' \"$output_path\"\n/bin/cp '{}' \"$output_path.next\"\n/bin/chmod 600 \"$output_path.next\"\n/bin/mv \"$output_path.next\" \"$output_path\"\n",
                    source.display(),
                    replacement.display()
                ),
                _ => format!("/bin/cp '{}' \"$output_path\"\n", source.display()),
            };
            let image_events = match mode {
                FakeMode::NoImage => String::new(),
                FakeMode::MultipleImages => format!(
                    "printf '{{\"method\":\"item/started\",\"params\":{{\"threadId\":\"{THREAD_ID}\",\"turnId\":\"{TURN_ID}\",\"item\":{{\"type\":\"imageGeneration\",\"id\":\"{CALL_ID}\",\"status\":\"inProgress\"}}}}}}\\n'\nprintf '{{\"method\":\"item/started\",\"params\":{{\"threadId\":\"{THREAD_ID}\",\"turnId\":\"{TURN_ID}\",\"item\":{{\"type\":\"imageGeneration\",\"id\":\"call_other_image\",\"status\":\"inProgress\"}}}}}}\\n'\n"
                ),
                _ => format!(
                    "printf '{{\"method\":\"item/started\",\"params\":{{\"threadId\":\"{THREAD_ID}\",\"turnId\":\"{TURN_ID}\",\"item\":{{\"type\":\"imageGeneration\",\"id\":\"{CALL_ID}\",\"status\":\"inProgress\"}}}}}}\\n'\nprintf '{{\"method\":\"item/completed\",\"params\":{{\"threadId\":\"{THREAD_ID}\",\"turnId\":\"{TURN_ID}\",\"item\":{{\"type\":\"imageGeneration\",\"id\":\"{CALL_ID}\",\"status\":\"completed\",\"result\":\"cG5n\",\"savedPath\":\"%s\"}}}}}}\\n' \"$output_path\"\n"
                ),
            };
            let after_terminal = match mode {
                FakeMode::MalformedSuffix => "printf 'not-json\\n'\n".to_string(),
                FakeMode::JoinTimeout => "/bin/sleep 30\n".to_string(),
                FakeMode::LateImage => format!(
                    "printf '{{\"method\":\"item/started\",\"params\":{{\"threadId\":\"{THREAD_ID}\",\"turnId\":\"{TURN_ID}\",\"item\":{{\"type\":\"imageGeneration\",\"id\":\"{CALL_ID}\",\"status\":\"inProgress\"}}}}}}\\n'\nprintf '{{\"method\":\"item/completed\",\"params\":{{\"threadId\":\"{THREAD_ID}\",\"turnId\":\"{TURN_ID}\",\"item\":{{\"type\":\"imageGeneration\",\"id\":\"{CALL_ID}\",\"status\":\"completed\",\"result\":\"cG5n\",\"savedPath\":\"%s\"}}}}}}\\n' \"$output_path\"\n"
                ),
                _ => String::new(),
            };
            let script = format!(
                "#!/bin/sh\nset -eu\nIFS= read -r initialize\nprintf '{{\"id\":1,\"result\":{{\"codexHome\":\"%s\"}}}}\\n' \"$CODEX_HOME\"\nIFS= read -r initialized\nIFS= read -r thread_start\nprintf '{{\"method\":\"thread/started\",\"params\":{{\"thread\":{{\"id\":\"{THREAD_ID}\"}}}}}}\\n'\nprintf '{{\"id\":2,\"result\":{{\"thread\":{{\"id\":\"{THREAD_ID}\"}}}}}}\\n'\nIFS= read -r turn_start\nprintf '{{\"method\":\"turn/started\",\"params\":{{\"threadId\":\"{THREAD_ID}\",\"turn\":{{\"id\":\"{TURN_ID}\"}}}}}}\\n'\nprintf '{{\"id\":3,\"result\":{{\"turn\":{{\"id\":\"{TURN_ID}\"}}}}}}\\n'\noutput_dir=\"$CODEX_HOME/generated_images/{THREAD_ID}\"\noutput_path=\"$output_dir/{CALL_ID}.png\"\n/bin/mkdir -p \"$output_dir\"\n/bin/chmod 700 \"$CODEX_HOME/generated_images\" \"$output_dir\"\n{output_action}/bin/chmod 600 \"$output_path\" 2>/dev/null || true\n{image_events}printf '{{\"method\":\"turn/completed\",\"params\":{{\"threadId\":\"{THREAD_ID}\",\"turn\":{{\"id\":\"{TURN_ID}\",\"status\":\"completed\"}}}}}}\\n'\n{after_terminal}while IFS= read -r ignored; do :; done\n"
            );
            std::fs::write(&executable, script).unwrap();
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
            let expected = if matches!(mode, FakeMode::ReplacedOutput) {
                b"replacement-native-image".to_vec()
            } else {
                payload.to_vec()
            };
            Self {
                _root: root,
                executable,
                workspace,
                codex_home,
                expected,
            }
        }

        async fn run(&self, timeout: Duration) -> Result<Vec<u8>, CodexAppServerError> {
            run_codex_app_server(
                CodexAppServerRequest {
                    executable: &self.executable,
                    workspace: &self.workspace,
                    codex_home: &self.codex_home,
                    prompt: "invoke image_gen.imagegen exactly once",
                    input_paths: &[],
                    timeout,
                    environment: &[("PATH".to_string(), "/usr/bin:/bin".to_string())],
                },
                |_| Ok(()),
            )
            .await
        }
    }

    #[tokio::test]
    async fn bounded_reader_rejects_unterminated_and_oversized_lines() {
        let mut unterminated = BufReader::new(&b"{}"[..]);
        assert_eq!(
            read_bounded_line(&mut unterminated).await,
            Err(CodexAppServerError::ProcessExited)
        );

        let payload = vec![b'x'; MAX_PROTOCOL_LINE_BYTES + 1];
        let mut oversized = BufReader::new(payload.as_slice());
        assert_eq!(
            read_bounded_line(&mut oversized).await,
            Err(CodexAppServerError::Protocol)
        );
    }

    #[tokio::test]
    async fn exact_app_server_handoff_reads_only_the_authorized_native_output() {
        let fixture = FakeAppServer::new(FakeMode::Normal);
        assert_eq!(
            fixture.run(Duration::from_secs(30)).await.unwrap(),
            fixture.expected
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "61-process stress gate; run explicitly to avoid starving unrelated process tests"]
    async fn request_private_handoffs_do_not_cross_at_1_20_40() {
        for concurrency in [1_usize, 20, 40] {
            let started = tokio::time::Instant::now();
            let mut tasks = tokio::task::JoinSet::new();
            for index in 0..concurrency {
                tasks.spawn(async move {
                    let expected = format!("native-{concurrency}-{index}").into_bytes();
                    let fixture = FakeAppServer::new_with_payload(FakeMode::Normal, &expected);
                    let actual =
                        fixture
                            .run(Duration::from_secs(60))
                            .await
                            .unwrap_or_else(|error| {
                                panic!("concurrency={concurrency} index={index} failed: {error:?}")
                            });
                    (expected, actual)
                });
            }
            while let Some(result) = tasks.join_next().await {
                let (expected, actual) = result.unwrap();
                assert_eq!(actual, expected);
            }
            let elapsed = started.elapsed();
            eprintln!("app-server concurrency={concurrency} elapsed={elapsed:?}");
            assert!(elapsed < Duration::from_secs(60));
        }
    }

    #[tokio::test]
    async fn no_image_multiple_ids_and_transient_output_fail_closed() {
        for (mode, expected) in [
            (FakeMode::NoImage, CodexAppServerError::NoImage),
            (
                FakeMode::MultipleImages,
                CodexAppServerError::MultipleImages,
            ),
            (
                FakeMode::TransientOutput,
                CodexAppServerError::OutputMissing,
            ),
        ] {
            let fixture = FakeAppServer::new(mode);
            assert_eq!(fixture.run(Duration::from_secs(30)).await, Err(expected));
        }
    }

    #[tokio::test]
    async fn same_name_replacement_before_terminal_reads_the_final_inode() {
        let fixture = FakeAppServer::new(FakeMode::ReplacedOutput);
        assert_eq!(
            fixture.run(Duration::from_secs(30)).await.unwrap(),
            fixture.expected
        );
    }

    #[tokio::test]
    async fn malformed_suffix_and_capture_join_timeout_fail_closed() {
        let malformed = FakeAppServer::new(FakeMode::MalformedSuffix);
        assert_eq!(
            malformed.run(Duration::from_secs(30)).await,
            Err(CodexAppServerError::Protocol)
        );

        let timeout = FakeAppServer::new(FakeMode::JoinTimeout);
        assert_eq!(
            timeout.run(Duration::from_millis(200)).await,
            Err(CodexAppServerError::Timeout)
        );

        let late = FakeAppServer::new(FakeMode::LateImage);
        assert_eq!(
            late.run(Duration::from_secs(30)).await,
            Err(CodexAppServerError::Protocol)
        );
    }

    #[test]
    fn completed_image_requires_exact_bound_authority() {
        let home = Path::new("/private/codex-home");
        let thread_id = Uuid::parse_str("019fd666-0416-7da2-bcc3-7f2f51efd3c8").unwrap();
        let turn_id = "019fd666-0416-7da2-bcc3-7f2f51efd3c9".to_string();
        let call_id = "call_exact_image";
        let mut state = ProtocolState::default();
        state.bind_thread(thread_id).unwrap();
        state.bind_turn(turn_id.clone()).unwrap();
        state
            .observe_notification(
                &json!({
                    "method": "thread/started",
                    "params": { "thread": { "id": thread_id } }
                }),
                home,
            )
            .unwrap();
        state
            .observe_notification(
                &json!({
                    "method": "turn/started",
                    "params": { "threadId": thread_id, "turn": { "id": turn_id.clone() } }
                }),
                home,
            )
            .unwrap();
        let started = json!({
            "method": "item/started",
            "params": {
                "threadId": thread_id,
                "turnId": turn_id,
                "item": {
                    "type": "imageGeneration",
                    "id": call_id,
                    "status": "inProgress"
                }
            }
        });
        let event = json!({
            "method": "item/completed",
            "params": {
                "threadId": thread_id,
                "turnId": turn_id,
                "item": {
                    "type": "imageGeneration",
                    "id": call_id,
                    "status": "completed",
                    "result": "cG5n",
                    "savedPath": format!("{}/generated_images/{thread_id}/{call_id}.png", home.display())
                }
            }
        });

        assert!(!state.observe_notification(&started, home).unwrap());
        assert!(!state.observe_notification(&event, home).unwrap());
        assert_eq!(
            state.authority().unwrap(),
            (thread_id.to_string(), call_id.to_string())
        );
    }

    #[test]
    fn multiple_image_ids_and_mismatched_path_fail_closed() {
        let home = Path::new("/private/codex-home");
        let thread_id = Uuid::parse_str("019fd666-0416-7da2-bcc3-7f2f51efd3c8").unwrap();
        let turn_id = "019fd666-0416-7da2-bcc3-7f2f51efd3c9".to_string();
        let mut state = ProtocolState::default();
        state.bind_thread(thread_id).unwrap();
        state.bind_turn(turn_id.clone()).unwrap();
        let event = |call_id: &str, saved_path: &str| {
            json!({
                "method": "item/completed",
                "params": {
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "item": {
                        "type": "imageGeneration",
                        "id": call_id,
                        "status": "completed",
                        "result": "cG5n",
                        "savedPath": saved_path
                    }
                }
            })
        };
        assert_eq!(
            state.observe_notification(&event("call_a", "/other/output.png"), home),
            Err(CodexAppServerError::Protocol)
        );

        let mut state = ProtocolState::default();
        state.bind_thread(thread_id).unwrap();
        state.bind_turn(turn_id.clone()).unwrap();
        let first = format!("{}/generated_images/{thread_id}/call_a.png", home.display());
        let second = format!("{}/generated_images/{thread_id}/call_b.png", home.display());
        state
            .observe_notification(&event("call_a", &first), home)
            .unwrap();
        assert_eq!(
            state.observe_notification(&event("call_b", &second), home),
            Err(CodexAppServerError::MultipleImages)
        );
    }

    #[test]
    fn non_image_tool_items_fail_closed() {
        let home = Path::new("/private/codex-home");
        let thread_id = Uuid::parse_str("019fd666-0416-7da2-bcc3-7f2f51efd3c8").unwrap();
        let turn_id = "019fd666-0416-7da2-bcc3-7f2f51efd3c9".to_string();
        let mut state = ProtocolState::default();
        state.bind_thread(thread_id).unwrap();
        state.bind_turn(turn_id.clone()).unwrap();
        let item = |item_type: &str| {
            json!({
                "method": "item/started",
                "params": {
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "item": { "type": item_type, "id": "item_one" }
                }
            })
        };

        assert!(
            !state
                .observe_notification(&item("reasoning"), home)
                .unwrap()
        );
        assert_eq!(
            state.observe_notification(&item("commandExecution"), home),
            Err(CodexAppServerError::Protocol)
        );
    }
}
