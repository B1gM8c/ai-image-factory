use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
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
const MAX_DIAGNOSTIC_FIELD_BYTES: usize = 4 * 1024;
const MAX_STDERR_DIGEST_BYTES: usize = 64 * 1024;
const REAP_TIMEOUT: Duration = Duration::from_secs(5);

const IMAGE_GENERATION_DEVELOPER_INSTRUCTIONS: &str = "For this thread, image requests MUST invoke the enabled namespaced tool image_gen.imagegen (wire name image_gen__imagegen) exactly once. Never answer an image request with text only. Do not use shell or local programs to create, copy, move, rename, edit, or delete the generated artifact. After the image tool completes, stop.";
const IMAGE_GENERATION_ORCHESTRATOR_MODEL: &str = "gpt-5.4";
const IMAGE_GENERATION_DIRECT_TOOL_CONFIG: &str =
    "features.code_mode.direct_only_tool_namespaces=[\"image_gen\"]";

type FailureDiagnosticSink<'a> =
    &'a (dyn Fn(&CodexAppServerFailureDiagnosticV1) -> Result<(), ()> + Sync);

pub(crate) struct CodexAppServerRequest<'a> {
    pub(crate) request_id: &'a str,
    pub(crate) image_index: u32,
    pub(crate) attempt: u8,
    pub(crate) executable: &'a Path,
    pub(crate) workspace: &'a Path,
    pub(crate) codex_home: &'a Path,
    pub(crate) prompt: &'a str,
    pub(crate) input_paths: &'a [PathBuf],
    pub(crate) timeout: Duration,
    pub(crate) environment: &'a [(String, String)],
    pub(crate) failure_diagnostic_sink: Option<FailureDiagnosticSink<'a>>,
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
    ImageToolFailed,
    ContentPolicyRejected,
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
            Self::RequestRejected => "codex_app_server_request_rejected",
            Self::TurnFailed => "codex_turn_failed",
            Self::ImageToolFailed => "codex_image_tool_failed",
            Self::ContentPolicyRejected => "content_policy_rejected",
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
    failure_diagnostic: Option<FailureDiagnostic>,
}

#[derive(Debug)]
struct FailureDiagnostic {
    source: &'static str,
    class: &'static str,
    numeric_code: Option<i64>,
    code: FieldDiagnostic,
    message: FieldDiagnostic,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodexAppServerFailureDiagnosticV1 {
    schema_version: u16,
    failure_category: String,
    source: String,
    class: String,
    numeric_code: Option<i64>,
    code: PersistedFieldDiagnostic,
    message: PersistedFieldDiagnostic,
    stderr: Option<PersistedStreamDiagnostic>,
    exit: PersistedExitDiagnostic,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedFieldDiagnostic {
    sha256: Option<String>,
    bytes: usize,
    truncated: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedStreamDiagnostic {
    sha256: String,
    bytes: usize,
    truncated: bool,
    class: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedExitDiagnostic {
    observed: bool,
    code: Option<i32>,
    signal: Option<i32>,
}

#[derive(Debug, Default)]
struct FieldDiagnostic {
    sha256: Option<String>,
    bytes: usize,
    truncated: bool,
}

#[derive(Debug, Default)]
struct StreamDiagnostic {
    sha256: String,
    bytes: usize,
    truncated: bool,
    class: String,
}

#[derive(Debug, Default)]
struct ExitDiagnostic {
    observed: bool,
    code: Option<i32>,
    signal: Option<i32>,
}

impl ProtocolState {
    fn record_failure(&mut self, source: &'static str, value: &Value) {
        if self.failure_diagnostic.is_some() {
            return;
        }
        let code = failure_string(
            value,
            &[
                "/code",
                "/error/code",
                "/result/code",
                "/result/error/code",
                "/type",
                "/error/type",
                "/result/type",
                "/result/error/type",
            ],
        );
        let message = failure_string(
            value,
            &[
                "/message",
                "/error",
                "/error/message",
                "/result",
                "/result/message",
                "/result/error",
                "/result/error/message",
            ],
        );
        self.failure_diagnostic = Some(FailureDiagnostic {
            source,
            class: classify_failure(code, message),
            numeric_code: failure_numeric_code(value),
            code: summarize_field(code),
            message: summarize_field(message),
        });
    }

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
                    return if matches!(
                        item_type,
                        "userMessage" | "reasoning" | "agentMessage" | "plan"
                    ) {
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
                    Some("failed") => {
                        self.image_failed = true;
                        self.record_failure(
                            "image_generation_item",
                            params.pointer("/item").unwrap_or(&Value::Null),
                        );
                    }
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
                    Some("failed" | "interrupted") => {
                        self.record_failure(
                            "turn_terminal",
                            params.pointer("/turn/error").unwrap_or(&Value::Null),
                        );
                        Err(CodexAppServerError::TurnFailed)
                    }
                    _ => Err(CodexAppServerError::Protocol),
                };
            }
            "error" => {
                self.record_failure("server_error_notification", params);
                return Err(CodexAppServerError::RequestRejected);
            }
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
            return Err(CodexAppServerError::ImageToolFailed);
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
        .arg("--strict-config")
        .arg("-c")
        .arg(IMAGE_GENERATION_DIRECT_TOOL_CONFIG)
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
        let mut digest = Sha256::new();
        let mut sample = Vec::with_capacity(MAX_STDERR_DIGEST_BYTES);
        let mut captured = 0_usize;
        let mut total = 0_usize;
        loop {
            match tokio::io::AsyncReadExt::read(&mut reader, &mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    total = total.saturating_add(read);
                    let remaining = MAX_STDERR_DIGEST_BYTES.saturating_sub(captured);
                    let take = remaining.min(read);
                    digest.update(&buffer[..take]);
                    sample.extend_from_slice(&buffer[..take]);
                    captured = captured.saturating_add(take);
                }
            }
        }
        StreamDiagnostic {
            sha256: hex::encode(digest.finalize()),
            bytes: total,
            truncated: total > MAX_STDERR_DIGEST_BYTES,
            class: classify_stream_bytes(&sample),
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
                    "model": IMAGE_GENERATION_ORCHESTRATOR_MODEL,
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
            let exit = observe_child_exit(&mut child);
            terminate_child(&mut child).await;
            let stderr = await_stderr_diagnostic(stderr_task).await;
            let error = refine_image_tool_error(error, stderr.as_ref());
            report_failure(&request, &state, error, stderr.as_ref(), &exit);
            return Err(error);
        }
        Err(_) => {
            let exit = observe_child_exit(&mut child);
            terminate_child(&mut child).await;
            let stderr = await_stderr_diagnostic(stderr_task).await;
            report_failure(
                &request,
                &state,
                CodexAppServerError::Timeout,
                stderr.as_ref(),
                &exit,
            );
            return Err(CodexAppServerError::Timeout);
        }
    };

    let (thread_id, call_id) = authority;
    terminate_child(&mut child).await;
    let _ = await_stderr_diagnostic(stderr_task).await;
    let output = tokio::task::spawn_blocking(move || {
        native_output_root.read(&thread_id, &call_id, MAX_CODEX_OUTPUT_BYTES)
    })
    .await
    .map_err(|_| CodexAppServerError::OutputUnavailable)?
    .map_err(map_output_read_error)?
    .ok_or(CodexAppServerError::OutputMissing);
    output
}

fn refine_image_tool_error(
    error: CodexAppServerError,
    stderr: Option<&StreamDiagnostic>,
) -> CodexAppServerError {
    if error == CodexAppServerError::ImageToolFailed
        && stderr.is_some_and(|value| {
            value.class.split([':', '+']).any(|signal| {
                matches!(
                    signal,
                    "content_policy" | "cyber_policy" | "safety" | "moderation"
                )
            })
        })
    {
        CodexAppServerError::ContentPolicyRejected
    } else {
        error
    }
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
            (None, Some(error)) => {
                state.record_failure("rpc_rejection", error);
                return Err(CodexAppServerError::RequestRejected);
            }
            _ => return Err(CodexAppServerError::Protocol),
        }
    }
}

fn failure_string<'a>(value: &'a Value, pointers: &[&str]) -> Option<&'a str> {
    value.as_str().or_else(|| {
        pointers
            .iter()
            .find_map(|pointer| value.pointer(pointer)?.as_str())
    })
}

fn failure_numeric_code(value: &Value) -> Option<i64> {
    ["/code", "/error/code", "/result/code", "/result/error/code"]
        .iter()
        .find_map(|pointer| value.pointer(pointer)?.as_i64())
}

fn summarize_field(value: Option<&str>) -> FieldDiagnostic {
    let Some(value) = value else {
        return FieldDiagnostic::default();
    };
    let bytes = value.as_bytes();
    let captured = &bytes[..bytes.len().min(MAX_DIAGNOSTIC_FIELD_BYTES)];
    FieldDiagnostic {
        sha256: Some(hex::encode(Sha256::digest(captured))),
        bytes: bytes.len(),
        truncated: bytes.len() > MAX_DIAGNOSTIC_FIELD_BYTES,
    }
}

fn classify_failure(code: Option<&str>, message: Option<&str>) -> &'static str {
    let mut sample = Vec::with_capacity(MAX_DIAGNOSTIC_FIELD_BYTES * 2);
    for value in [code, message].into_iter().flatten() {
        let bytes = value.as_bytes();
        sample.extend_from_slice(&bytes[..bytes.len().min(MAX_DIAGNOSTIC_FIELD_BYTES)]);
        sample.push(b' ');
    }
    classify_bytes(&sample)
}

fn classify_bytes(value: &[u8]) -> &'static str {
    let normalized = String::from_utf8_lossy(value).to_ascii_lowercase();
    if normalized.contains("originator") {
        "originator_policy"
    } else if normalized.contains("entitlement") || normalized.contains("not entitled") {
        "entitlement"
    } else if normalized.contains("content_policy") || normalized.contains("cyber_policy") {
        "content_policy"
    } else if normalized.contains("status 403")
        || normalized.contains("status: 403")
        || normalized.contains("\"status\":403")
        || normalized.contains("forbidden")
    {
        "forbidden"
    } else if normalized.contains("rate_limit")
        || normalized.contains("rate limit")
        || normalized.contains("quota")
        || normalized.contains("resource_exhausted")
    {
        "rate_limit"
    } else if normalized.contains("unauthorized")
        || normalized.contains("authentication")
        || normalized.contains("credential")
    {
        "authentication"
    } else if normalized.contains("invalid_argument")
        || normalized.contains("invalid argument")
        || normalized.contains("unsupported")
    {
        "invalid_request"
    } else if normalized.contains("safety")
        || normalized.contains("policy")
        || normalized.contains("rejected")
    {
        "policy"
    } else if normalized.contains("timeout")
        || normalized.contains("unavailable")
        || normalized.contains("overloaded")
        || normalized.contains("network")
    {
        "availability"
    } else if normalized.contains("tool") || normalized.contains("image_generation") {
        "tool_failure"
    } else {
        "unknown"
    }
}

fn classify_stream_bytes(value: &[u8]) -> String {
    let normalized = String::from_utf8_lossy(value);
    if let Some(code) = extract_stable_api_error_code(&normalized) {
        return format!("api_code:{code}");
    }
    let lowercase = normalized.to_ascii_lowercase();
    let signals = stream_policy_signals(&lowercase);
    for status in [400_u16, 401, 403, 404, 409, 422, 429, 500, 502, 503, 504] {
        if lowercase.contains(&format!("http {status}")) {
            return if signals.is_empty() {
                format!("http_status:{status}")
            } else {
                format!("http_status:{status}:{}", signals.join("+"))
            };
        }
    }
    if !signals.is_empty() {
        return signals.join("+");
    }
    classify_bytes(value).to_string()
}

fn stream_policy_signals(value: &str) -> Vec<&'static str> {
    let mut signals = Vec::new();
    for (signal, needles) in [
        ("originator", &["originator"][..]),
        ("entitlement", &["entitlement", "not entitled"]),
        ("content_policy", &["content_policy", "content policy"]),
        ("cyber_policy", &["cyber_policy", "cyber policy"]),
        ("safety", &["safety"]),
        ("moderation", &["moderation"]),
        ("retention", &["retention", "zero data", "zdr"]),
        ("organization", &["organization", "organisation"]),
        ("account", &["account"]),
        ("prompt", &["prompt"]),
        ("rejected", &["rejected"]),
        ("unsupported", &["unsupported"]),
        ("blocked", &["blocked"]),
        ("policy", &["policy"]),
    ] {
        if needles.iter().any(|needle| value.contains(needle)) {
            signals.push(signal);
        }
    }
    signals
}

fn extract_stable_api_error_code(value: &str) -> Option<String> {
    for marker in [r#""code":""#, r#"\"code\":\""#] {
        let Some(start) = value.find(marker).map(|index| index + marker.len()) else {
            continue;
        };
        let code: String = value[start..]
            .chars()
            .take(64)
            .take_while(|value| value.is_ascii_alphanumeric() || matches!(value, '_' | '-' | '.'))
            .collect();
        if !code.is_empty() {
            return Some(code.to_ascii_lowercase());
        }
    }
    None
}

async fn await_stderr_diagnostic(
    task: tokio::task::JoinHandle<StreamDiagnostic>,
) -> Option<StreamDiagnostic> {
    tokio::time::timeout(REAP_TIMEOUT, task)
        .await
        .ok()
        .and_then(Result::ok)
}

fn report_failure(
    request: &CodexAppServerRequest<'_>,
    state: &ProtocolState,
    error: CodexAppServerError,
    stderr: Option<&StreamDiagnostic>,
    exit: &ExitDiagnostic,
) {
    let diagnostic = build_failure_diagnostic(state, error, stderr, exit);
    trace_failure(request, &diagnostic);
    if request
        .failure_diagnostic_sink
        .is_some_and(|sink| sink(&diagnostic).is_err())
    {
        tracing::warn!(
            request.id = request.request_id,
            image.index = request.image_index,
            codex.attempt = request.attempt,
            codex.failure.category = error.code(),
            "Codex app-server failure diagnostic could not be persisted"
        );
    }
}

fn build_failure_diagnostic(
    state: &ProtocolState,
    error: CodexAppServerError,
    stderr: Option<&StreamDiagnostic>,
    exit: &ExitDiagnostic,
) -> CodexAppServerFailureDiagnosticV1 {
    let failure = state.failure_diagnostic.as_ref();
    CodexAppServerFailureDiagnosticV1 {
        schema_version: 1,
        failure_category: error.code().to_string(),
        source: failure.map_or("none", |value| value.source).to_string(),
        class: failure.map_or("unknown", |value| value.class).to_string(),
        numeric_code: failure.and_then(|value| value.numeric_code),
        code: failure
            .map(|value| PersistedFieldDiagnostic {
                sha256: value.code.sha256.clone(),
                bytes: value.code.bytes,
                truncated: value.code.truncated,
            })
            .unwrap_or_default(),
        message: failure
            .map(|value| PersistedFieldDiagnostic {
                sha256: value.message.sha256.clone(),
                bytes: value.message.bytes,
                truncated: value.message.truncated,
            })
            .unwrap_or_default(),
        stderr: stderr.map(|value| PersistedStreamDiagnostic {
            sha256: value.sha256.clone(),
            bytes: value.bytes,
            truncated: value.truncated,
            class: value.class.to_string(),
        }),
        exit: PersistedExitDiagnostic {
            observed: exit.observed,
            code: exit.code,
            signal: exit.signal,
        },
    }
}

fn trace_failure(
    request: &CodexAppServerRequest<'_>,
    diagnostic: &CodexAppServerFailureDiagnosticV1,
) {
    tracing::warn!(
        request.id = request.request_id,
        image.index = request.image_index,
        codex.attempt = request.attempt,
        codex.failure.category = diagnostic.failure_category,
        codex.failure.source = diagnostic.source,
        codex.failure.class = diagnostic.class,
        codex.failure.numeric_code = diagnostic.numeric_code,
        codex.failure.code_sha256 = diagnostic.code.sha256.as_deref().unwrap_or("none"),
        codex.failure.code_bytes = diagnostic.code.bytes,
        codex.failure.code_truncated = diagnostic.code.truncated,
        codex.failure.message_sha256 = diagnostic.message.sha256.as_deref().unwrap_or("none"),
        codex.failure.message_bytes = diagnostic.message.bytes,
        codex.failure.message_truncated = diagnostic.message.truncated,
        codex.stderr.class = diagnostic
            .stderr
            .as_ref()
            .map_or("unknown", |value| &value.class),
        codex.stderr.sha256 = diagnostic
            .stderr
            .as_ref()
            .map_or("unavailable", |value| value.sha256.as_str()),
        codex.stderr.bytes = diagnostic.stderr.as_ref().map_or(0, |value| value.bytes),
        codex.stderr.truncated = diagnostic
            .stderr
            .as_ref()
            .is_some_and(|value| value.truncated),
        codex.exit.observed = diagnostic.exit.observed,
        codex.exit.code = diagnostic.exit.code,
        codex.exit.signal = diagnostic.exit.signal,
        "Codex app-server failed with bounded redacted diagnostics"
    );
}

fn observe_child_exit(child: &mut Child) -> ExitDiagnostic {
    let Ok(Some(status)) = child.try_wait() else {
        return ExitDiagnostic::default();
    };
    ExitDiagnostic {
        observed: true,
        code: status.code(),
        #[cfg(unix)]
        signal: status.signal(),
        #[cfg(not(unix))]
        signal: None,
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
                    request_id: "req_test",
                    image_index: 1,
                    attempt: 1,
                    executable: &self.executable,
                    workspace: &self.workspace,
                    codex_home: &self.codex_home,
                    prompt: "invoke image_gen.imagegen exactly once",
                    input_paths: &[],
                    timeout,
                    environment: &[("PATH".to_string(), "/usr/bin:/bin".to_string())],
                    failure_diagnostic_sink: None,
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
    fn image_tool_failure_keeps_only_bounded_redacted_diagnostics() {
        let home = Path::new("/private/codex-home");
        let thread_id = Uuid::parse_str(THREAD_ID).unwrap();
        let mut state = announced_state(home, thread_id);
        state
            .observe_notification(
                &json!({
                    "method": "item/started",
                    "params": {
                        "threadId": THREAD_ID,
                        "turnId": TURN_ID,
                        "item": {
                            "type": "imageGeneration",
                            "id": CALL_ID,
                            "status": "inProgress"
                        }
                    }
                }),
                home,
            )
            .unwrap();
        let sensitive_sample = "hidden-user-material";
        state
            .observe_notification(
                &json!({
                    "method": "item/completed",
                    "params": {
                        "threadId": THREAD_ID,
                        "turnId": TURN_ID,
                        "item": {
                            "type": "imageGeneration",
                            "id": CALL_ID,
                            "status": "failed",
                            "result": {
                                "code": "rate_limit_exceeded",
                                "message": sensitive_sample
                            }
                        }
                    }
                }),
                home,
            )
            .unwrap();

        assert_eq!(state.authority(), Err(CodexAppServerError::ImageToolFailed));
        let diagnostic = state.failure_diagnostic.as_ref().unwrap();
        assert_eq!(diagnostic.source, "image_generation_item");
        assert_eq!(diagnostic.class, "rate_limit");
        assert_eq!(diagnostic.message.bytes, sensitive_sample.len());
        assert!(!diagnostic.message.truncated);
        let digest = diagnostic.message.sha256.as_ref().unwrap();
        assert_eq!(digest.len(), 64);
        assert!(!digest.contains("hidden-user-material"));

        let persisted = build_failure_diagnostic(
            &state,
            CodexAppServerError::ImageToolFailed,
            Some(&StreamDiagnostic {
                sha256: hex::encode(Sha256::digest(b"stderr-sensitive-material")),
                bytes: 25,
                truncated: false,
                class: "authentication".to_string(),
            }),
            &ExitDiagnostic {
                observed: true,
                code: Some(1),
                signal: None,
            },
        );
        let encoded = serde_json::to_vec(&persisted).unwrap();
        assert!(encoded.len() < 64 * 1024);
        assert!(
            !encoded
                .windows(sensitive_sample.len())
                .any(|value| { value == sensitive_sample.as_bytes() })
        );
        assert!(
            !encoded
                .windows(b"stderr-sensitive-material".len())
                .any(|value| { value == b"stderr-sensitive-material" })
        );
        assert_eq!(persisted.schema_version, 1);
        assert_eq!(persisted.failure_category, "codex_image_tool_failed");
        assert_eq!(persisted.class, "rate_limit");
    }

    #[test]
    fn turn_failure_classifies_without_retaining_upstream_text() {
        let home = Path::new("/private/codex-home");
        let thread_id = Uuid::parse_str(THREAD_ID).unwrap();
        let mut state = announced_state(home, thread_id);
        let error = state.observe_notification(
            &json!({
                "method": "turn/completed",
                "params": {
                    "threadId": THREAD_ID,
                    "turn": {
                        "id": TURN_ID,
                        "status": "failed",
                        "error": {
                            "code": "invalid_argument",
                            "message": "unsupported hidden-user-material"
                        }
                    }
                }
            }),
            home,
        );

        assert_eq!(error, Err(CodexAppServerError::TurnFailed));
        let diagnostic = state.failure_diagnostic.unwrap();
        assert_eq!(diagnostic.source, "turn_terminal");
        assert_eq!(diagnostic.class, "invalid_request");
        assert_eq!(diagnostic.code.sha256.unwrap().len(), 64);
        assert_eq!(diagnostic.message.sha256.unwrap().len(), 64);
    }

    #[test]
    fn diagnostic_fields_are_strictly_bounded() {
        let value = "x".repeat(MAX_DIAGNOSTIC_FIELD_BYTES + 1);
        let summary = summarize_field(Some(&value));
        assert_eq!(summary.bytes, MAX_DIAGNOSTIC_FIELD_BYTES + 1);
        assert!(summary.truncated);
        assert_eq!(summary.sha256.unwrap().len(), 64);

        assert_eq!(
            failure_numeric_code(&json!({ "code": -32001, "message": value })),
            Some(-32001)
        );
    }

    #[test]
    fn stderr_classification_preserves_actionable_policy_boundaries() {
        assert_eq!(
            classify_bytes(b"originator is not allowed"),
            "originator_policy"
        );
        assert_eq!(classify_bytes(b"account is not entitled"), "entitlement");
        assert_eq!(classify_bytes(b"code=content_policy"), "content_policy");
        assert_eq!(
            classify_bytes(b"request failed with status 403"),
            "forbidden"
        );
        assert_eq!(classify_bytes(b"policy rejected"), "policy");
        assert_eq!(
            classify_stream_bytes(
                br#"image generation failed: http 400 Bad Request: Some("{\"error\":{\"code\":\"content_policy\"}}")"#,
            ),
            "api_code:content_policy"
        );
        assert_eq!(
            classify_stream_bytes(b"image generation failed: http 422 Unprocessable Entity"),
            "http_status:422"
        );
        assert_eq!(
            classify_stream_bytes(
                b"image generation failed: http 400 Bad Request: organization zero data retention policy rejected",
            ),
            "http_status:400:retention+organization+rejected+policy"
        );
    }

    #[test]
    fn only_explicit_content_safety_signals_refine_image_tool_failures() {
        let diagnostic = |class: &str| StreamDiagnostic {
            class: class.to_string(),
            ..StreamDiagnostic::default()
        };
        for class in [
            "api_code:content_policy",
            "api_code:cyber_policy",
            "http_status:400:safety+moderation+rejected+blocked",
        ] {
            assert_eq!(
                refine_image_tool_error(
                    CodexAppServerError::ImageToolFailed,
                    Some(&diagnostic(class)),
                ),
                CodexAppServerError::ContentPolicyRejected,
            );
        }
        for class in [
            "http_status:400:retention+organization+rejected+policy",
            "http_status:400:prompt+unsupported",
            "http_status:429",
        ] {
            assert_eq!(
                refine_image_tool_error(
                    CodexAppServerError::ImageToolFailed,
                    Some(&diagnostic(class)),
                ),
                CodexAppServerError::ImageToolFailed,
            );
        }
        assert_eq!(
            refine_image_tool_error(CodexAppServerError::TurnFailed, Some(&diagnostic("safety"))),
            CodexAppServerError::TurnFailed,
        );
    }

    fn announced_state(home: &Path, thread_id: Uuid) -> ProtocolState {
        let mut state = ProtocolState::default();
        state.bind_thread(thread_id).unwrap();
        state.bind_turn(TURN_ID.to_string()).unwrap();
        state
            .observe_notification(
                &json!({
                    "method": "thread/started",
                    "params": { "thread": { "id": THREAD_ID } }
                }),
                home,
            )
            .unwrap();
        state
            .observe_notification(
                &json!({
                    "method": "turn/started",
                    "params": { "threadId": THREAD_ID, "turn": { "id": TURN_ID } }
                }),
                home,
            )
            .unwrap();
        state
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
    fn informational_items_are_allowed_but_non_image_tools_fail_closed() {
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
        assert!(!state.observe_notification(&item("plan"), home).unwrap());
        assert_eq!(
            state.observe_notification(&item("commandExecution"), home),
            Err(CodexAppServerError::Protocol)
        );
    }
}
