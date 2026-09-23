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
const IMAGE_GENERATION_DIRECT_TOOL_CONFIG: &str =
    "features.code_mode.direct_only_tool_namespaces=[\"image_gen\"]";
const WEB_SEARCH_DISABLED_CONFIG: &str = "web_search=\"disabled\"";

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
    capture_diagnostic: ProtocolCaptureDiagnostic,
}

#[derive(Default)]
struct ProtocolCaptureDiagnostic {
    phase: &'static str,
    reason: &'static str,
    last_message_class: &'static str,
    message_count: usize,
    notification_count: usize,
    captured_bytes: usize,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    protocol: Option<PersistedProtocolDiagnostic>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedProtocolDiagnostic {
    phase: String,
    reason: String,
    last_message_class: String,
    message_count: usize,
    notification_count: usize,
    captured_bytes: usize,
}

impl CodexAppServerFailureDiagnosticV1 {
    pub(crate) fn is_retryable_authentication_rejection(&self) -> bool {
        if self.failure_category != CodexAppServerError::ImageToolFailed.code()
            || !matches!(
                self.class.as_str(),
                "unknown" | "tool_failure" | "rejected" | "authentication"
            )
            || self.numeric_code.is_some_and(|code| code != 401)
        {
            return false;
        }
        let stderr_class = self
            .stderr
            .as_ref()
            .map_or("unknown", |value| value.class.as_str());
        if stderr_class == "http_status:401" {
            return true;
        }
        if let Some(signals) = stderr_class.strip_prefix("http_status:401:") {
            return signals
                .split('+')
                .all(|signal| matches!(signal, "rejected" | "authentication"));
        }
        self.numeric_code == Some(401)
            && stderr_class
                .split('+')
                .all(|signal| matches!(signal, "unknown" | "rejected" | "authentication"))
    }
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
    fn protocol_phase(&mut self, phase: &'static str) {
        self.capture_diagnostic.phase = phase;
        self.capture_diagnostic.reason = "validation_failed";
    }

    fn protocol_error(&mut self, reason: &'static str) -> CodexAppServerError {
        self.capture_diagnostic.reason = reason;
        CodexAppServerError::Protocol
    }

    fn observe_message_class(&mut self, message: &Value, captured_bytes: usize) {
        self.capture_diagnostic.message_count =
            self.capture_diagnostic.message_count.saturating_add(1);
        self.capture_diagnostic.captured_bytes = captured_bytes;
        self.capture_diagnostic.last_message_class =
            match message.get("method").and_then(Value::as_str) {
                Some("thread/started") => "thread_started",
                Some("turn/started") => "turn_started",
                Some("item/started") => "item_started",
                Some("item/completed") => "item_completed",
                Some("turn/completed") => "turn_completed",
                Some("error") => "error_notification",
                Some(_) => "other_notification",
                None if message.get("id").is_some() => "rpc_response",
                None => "unclassified",
            };
        if message.get("method").is_some() {
            self.capture_diagnostic.notification_count =
                self.capture_diagnostic.notification_count.saturating_add(1);
        }
    }

    fn record_failure(&mut self, source: &'static str, value: &Value) {
        if self.failure_diagnostic.is_some() {
            return;
        }
        let explicit_codes = failure_strings(
            value,
            &["/code", "/error/code", "/result/code", "/result/error/code"],
        );
        let nested_types = failure_strings(
            value,
            &["/error/type", "/result/type", "/result/error/type"],
        );
        let envelope_type = value.pointer("/type").and_then(Value::as_str);
        let mut explicit_types = nested_types;
        if let Some(envelope_type) = envelope_type
            .filter(|value| source != "image_generation_item" || *value != "imageGeneration")
        {
            explicit_types.push(envelope_type);
        }
        let code = explicit_codes
            .first()
            .copied()
            .or_else(|| explicit_types.first().copied())
            .or(envelope_type);
        let messages = failure_strings(
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
        let message = messages.first().copied();
        let (numeric_codes, invalid_numeric_code) = failure_numeric_codes(value);
        let mut class = preferred_failure_class(&explicit_codes, &explicit_types, &messages);
        let explicit_non_authentication = explicit_codes
            .iter()
            .chain(explicit_types.iter())
            .any(|value| !explicit_authentication_rejection(value));
        let message_non_authentication = messages
            .iter()
            .any(|value| !retryable_authentication_message(value));
        let numeric_non_authentication =
            invalid_numeric_code || numeric_codes.iter().any(|code| *code != 401);
        if (explicit_non_authentication || message_non_authentication || numeric_non_authentication)
            && matches!(
                class,
                "unknown" | "tool_failure" | "authentication" | "rejected"
            )
        {
            class = "explicit_failure";
        }
        self.failure_diagnostic = Some(FailureDiagnostic {
            source,
            class,
            numeric_code: numeric_codes.first().copied(),
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
            return Err(self.protocol_error("thread_identity_mismatch"));
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
            return Err(self.protocol_error("turn_identity_mismatch"));
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
            .ok_or_else(|| self.protocol_error("method_missing"))?;
        self.capture_diagnostic.reason = match method {
            "thread/started" => "thread_notification_invalid",
            "turn/started" => "turn_notification_invalid",
            "item/started" | "item/completed" => "item_notification_invalid",
            "turn/completed" => "turn_terminal_invalid",
            _ => "notification_invalid",
        };
        let params = message.get("params").unwrap_or(&Value::Null);
        match method {
            "thread/started" => {
                let candidate = params
                    .pointer("/thread/id")
                    .and_then(Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok())
                    .ok_or_else(|| self.protocol_error("thread_notification_id_invalid"))?;
                if self
                    .announced_thread_id
                    .replace(candidate)
                    .is_some_and(|value| value != candidate)
                    || self.thread_id.is_some_and(|value| value != candidate)
                {
                    return Err(self.protocol_error("thread_identity_mismatch"));
                }
            }
            "turn/started" => {
                let candidate = params
                    .pointer("/turn/id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| self.protocol_error("turn_notification_id_missing"))?;
                self.observe_turn_identity(params, candidate)?;
                if self
                    .announced_turn_id
                    .as_deref()
                    .is_some_and(|value| value != candidate)
                {
                    return Err(self.protocol_error("turn_identity_mismatch"));
                }
                self.announced_turn_id = Some(candidate.to_string());
            }
            "item/started" | "item/completed" => {
                let item_type = params
                    .pointer("/item/type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| self.protocol_error("item_type_missing"))?;
                self.observe_bound_identity(params)?;
                if item_type != "imageGeneration" {
                    return if matches!(
                        item_type,
                        "userMessage" | "reasoning" | "agentMessage" | "plan"
                    ) {
                        Ok(false)
                    } else {
                        Err(self.protocol_error("unexpected_item_type"))
                    };
                }
                self.saw_image_generation = true;
                let call_id = params
                    .pointer("/item/id")
                    .and_then(Value::as_str)
                    .filter(|value| valid_call_id(value))
                    .ok_or_else(|| self.protocol_error("image_call_id_invalid"))?;
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
                            return Err(self.protocol_error("image_result_missing"));
                        }
                        let saved_path = params
                            .pointer("/item/savedPath")
                            .and_then(Value::as_str)
                            .ok_or(CodexAppServerError::ImageIncomplete)?;
                        let thread_id = self
                            .thread_id
                            .ok_or_else(|| self.protocol_error("thread_identity_missing"))?;
                        let expected = codex_home
                            .join("generated_images")
                            .join(thread_id.to_string())
                            .join(format!("{call_id}.png"));
                        if Path::new(saved_path) != expected {
                            return Err(self.protocol_error("image_saved_path_mismatch"));
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
                    .ok_or_else(|| self.protocol_error("turn_terminal_id_missing"))?;
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
                    _ => Err(self.protocol_error("turn_terminal_status_invalid")),
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

    fn observe_bound_identity(&mut self, params: &Value) -> Result<(), CodexAppServerError> {
        let expected_thread = self
            .thread_id
            .ok_or_else(|| self.protocol_error("thread_identity_missing"))?;
        let actual_thread = params
            .get("threadId")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or_else(|| self.protocol_error("item_thread_id_invalid"))?;
        let actual_turn = params
            .get("turnId")
            .and_then(Value::as_str)
            .ok_or_else(|| self.protocol_error("item_turn_id_missing"))?;
        let Some(expected_turn) = self
            .turn_id
            .as_deref()
            .or(self.announced_turn_id.as_deref())
        else {
            return Err(self.protocol_error("turn_identity_missing"));
        };
        if actual_thread != expected_thread || actual_turn != expected_turn {
            return Err(self.protocol_error("item_identity_mismatch"));
        }
        Ok(())
    }

    fn observe_turn_identity(
        &mut self,
        params: &Value,
        candidate_turn: &str,
    ) -> Result<(), CodexAppServerError> {
        let actual_thread = params
            .get("threadId")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or_else(|| self.protocol_error("turn_thread_id_invalid"))?;
        if self.thread_id.is_some_and(|value| value != actual_thread)
            || self
                .turn_id
                .as_deref()
                .is_some_and(|value| value != candidate_turn)
            || !valid_turn_id(candidate_turn)
        {
            return Err(self.protocol_error("turn_identity_mismatch"));
        }
        Ok(())
    }

    fn authority(&mut self) -> Result<(String, String), CodexAppServerError> {
        if self.thread_id != self.announced_thread_id || self.turn_id != self.announced_turn_id {
            return Err(self.protocol_error("terminal_identity_mismatch"));
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
                .ok_or_else(|| self.protocol_error("thread_identity_missing"))?
                .to_string(),
            self.completed_image_call_id
                .clone()
                .ok_or_else(|| self.protocol_error("image_call_id_missing"))?,
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
        .arg("-c")
        .arg(WEB_SEARCH_DISABLED_CONFIG)
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
        .arg("--disable")
        .arg("standalone_web_search")
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
        state.protocol_phase("initialize");
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
            return Err(state.protocol_error("codex_home_mismatch"));
        }
        send_message(&mut stdin, &json!({"method": "initialized"})).await?;

        state.protocol_phase("thread_start");
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
            .ok_or_else(|| state.protocol_error("thread_id_invalid"))?;
        state.bind_thread(thread_id)?;

        state.protocol_phase("turn_start");
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
            .ok_or_else(|| state.protocol_error("turn_id_missing"))?
            .to_string();
        state.bind_turn(turn_id)?;

        state.protocol_phase("event_stream");
        loop {
            let message = read_message(&mut stdout, &mut state, &mut capture_bytes).await?;
            if message.get("id").is_some() {
                return Err(state.protocol_error("unexpected_response"));
            }
            if state.observe_notification(&message, &codex_home)? {
                break;
            }
        }
        drop(stdin);
        state.protocol_phase("post_terminal");
        while let Some(message) =
            read_optional_message(&mut stdout, &mut state, &mut capture_bytes).await?
        {
            if message.get("id").is_some() || message.get("method").is_none() {
                return Err(state.protocol_error("post_terminal_message_invalid"));
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
                return Err(state.protocol_error("post_terminal_event"));
            }
            if state.observe_notification(&message, &codex_home)? {
                return Err(state.protocol_error("post_terminal_event"));
            }
        }
        state.protocol_phase("authority");
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
        let message = read_message(stdout, state, capture_bytes).await?;
        if message.get("method").is_some() && message.get("id").is_none() {
            if state.observe_notification(&message, codex_home)? {
                return Err(state.protocol_error("terminal_before_response"));
            }
            continue;
        }
        if message.get("id").and_then(Value::as_i64) != Some(expected_id) {
            return Err(state.protocol_error("unexpected_response_id"));
        }
        match (message.get("result"), message.get("error")) {
            (Some(result), None) => return Ok(result.clone()),
            (None, Some(error)) => {
                state.record_failure("rpc_rejection", error);
                return Err(CodexAppServerError::RequestRejected);
            }
            _ => return Err(state.protocol_error("response_envelope_invalid")),
        }
    }
}

fn failure_strings<'a>(value: &'a Value, pointers: &[&str]) -> Vec<&'a str> {
    value
        .as_str()
        .into_iter()
        .chain(
            pointers
                .iter()
                .filter_map(|pointer| value.pointer(pointer)?.as_str()),
        )
        .collect()
}

#[cfg(test)]
fn failure_numeric_code(value: &Value) -> Option<i64> {
    failure_numeric_codes(value).0.into_iter().next()
}

fn failure_numeric_codes(value: &Value) -> (Vec<i64>, bool) {
    let mut codes = Vec::new();
    let mut invalid_numeric_code = false;
    for pointer in [
        "/code",
        "/status",
        "/error/code",
        "/error/status",
        "/result/code",
        "/result/status",
        "/result/error/code",
        "/result/error/status",
    ] {
        let Some(value) = value.pointer(pointer) else {
            continue;
        };
        if let Some(code) = value.as_i64() {
            codes.push(code);
        } else if value.is_number() {
            invalid_numeric_code = true;
        }
    }
    (codes, invalid_numeric_code)
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

fn preferred_failure_class(
    explicit_codes: &[&str],
    explicit_types: &[&str],
    messages: &[&str],
) -> &'static str {
    let explicit_values = || explicit_codes.iter().chain(explicit_types.iter()).copied();
    if let Some(class) = explicit_values()
        .map(|value| classify_bytes(value.as_bytes()))
        .find(|class| {
            !matches!(
                *class,
                "unknown" | "tool_failure" | "authentication" | "rejected"
            )
        })
    {
        return class;
    }
    if let Some(class) = messages
        .iter()
        .map(|value| classify_bytes(value.as_bytes()))
        .find(|class| {
            !matches!(
                *class,
                "unknown" | "tool_failure" | "authentication" | "rejected"
            )
        })
    {
        return class;
    }
    explicit_values()
        .chain(messages.iter().copied())
        .next()
        .map_or("unknown", |value| classify_bytes(value.as_bytes()))
}

fn explicit_authentication_rejection(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "authentication"
            | "authentication_error"
            | "unauthorized"
            | "credential_rejected"
            | "rejected"
    )
}

fn retryable_authentication_message(value: &str) -> bool {
    explicit_authentication_rejection(value)
}

fn classify_bytes(value: &[u8]) -> &'static str {
    let normalized = String::from_utf8_lossy(value).to_ascii_lowercase();
    if normalized.contains("originator") {
        "originator_policy"
    } else if normalized.contains("entitlement") || normalized.contains("not entitled") {
        "entitlement"
    } else if normalized.contains("content_policy") || normalized.contains("cyber_policy") {
        "content_policy"
    } else if normalized.contains("retention")
        || normalized.contains("zero data")
        || normalized.contains("zdr")
    {
        "retention"
    } else if normalized.contains("organization") || normalized.contains("organisation") {
        "organization"
    } else if normalized.contains("account") {
        "account"
    } else if normalized.contains("prompt") {
        "prompt"
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
    } else if normalized.contains("invalid_argument")
        || normalized.contains("invalid argument")
        || normalized.contains("invalid_request")
        || normalized.contains("unsupported")
    {
        "invalid_request"
    } else if normalized.contains("safety")
        || normalized.contains("moderation")
        || normalized.contains("policy")
        || normalized.contains("blocked")
    {
        "policy"
    } else if normalized.contains("timeout")
        || normalized.contains("unavailable")
        || normalized.contains("availability")
        || normalized.contains("overloaded")
        || normalized.contains("network")
    {
        "availability"
    } else if normalized.contains("unauthorized")
        || normalized.contains("authentication")
        || normalized.contains("credential")
    {
        "authentication"
    } else if normalized.contains("rejected") {
        "rejected"
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
    let statuses = observed_http_statuses(&lowercase);
    if statuses.len() > 1 {
        return format!(
            "http_status_conflict:{}",
            statuses
                .iter()
                .map(u16::to_string)
                .collect::<Vec<_>>()
                .join("+")
        );
    }
    if let Some(status) = statuses.first() {
        return if signals.is_empty() {
            format!("http_status:{status}")
        } else {
            format!("http_status:{status}:{}", signals.join("+"))
        };
    }
    if !signals.is_empty() {
        return signals.join("+");
    }
    classify_bytes(value).to_string()
}

fn observed_http_statuses(value: &str) -> Vec<u16> {
    let bytes = value.as_bytes();
    let mut statuses = Vec::new();
    for index in 0..bytes.len().saturating_sub(3) {
        if &bytes[index..index + 4] != b"http"
            || index.checked_sub(1).is_some_and(|previous| {
                bytes[previous].is_ascii_alphanumeric() || bytes[previous] == b'_'
            })
        {
            continue;
        }
        let mut cursor = index + 4;
        if cursor >= bytes.len() || !bytes[cursor].is_ascii_whitespace() {
            continue;
        }
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        let start = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            cursor += 1;
        }
        if cursor - start != 3
            || cursor < bytes.len()
                && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
        {
            continue;
        }
        let Ok(status) = value[start..cursor].parse::<u16>() else {
            continue;
        };
        if (100..=599).contains(&status) && !statuses.contains(&status) {
            statuses.push(status);
        }
    }
    statuses.sort_unstable();
    statuses
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
        (
            "forbidden",
            &["forbidden", "status 403", "status: 403", "\"status\":403"],
        ),
        (
            "rate_limit",
            &["rate_limit", "rate limit", "quota", "resource_exhausted"],
        ),
        (
            "availability",
            &[
                "timeout",
                "unavailable",
                "availability",
                "overloaded",
                "network",
            ],
        ),
        (
            "invalid_request",
            &["invalid_argument", "invalid argument", "invalid_request"],
        ),
        (
            "authentication",
            &["unauthorized", "authentication", "credential"],
        ),
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
        protocol: (error == CodexAppServerError::Protocol).then(|| PersistedProtocolDiagnostic {
            phase: state.capture_diagnostic.phase.to_string(),
            reason: state.capture_diagnostic.reason.to_string(),
            last_message_class: state.capture_diagnostic.last_message_class.to_string(),
            message_count: state.capture_diagnostic.message_count,
            notification_count: state.capture_diagnostic.notification_count,
            captured_bytes: state.capture_diagnostic.captured_bytes,
        }),
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
        codex.protocol.phase = diagnostic
            .protocol
            .as_ref()
            .map_or("none", |value| value.phase.as_str()),
        codex.protocol.reason = diagnostic
            .protocol
            .as_ref()
            .map_or("none", |value| value.reason.as_str()),
        codex.protocol.last_message_class = diagnostic
            .protocol
            .as_ref()
            .map_or("none", |value| value.last_message_class.as_str()),
        codex.protocol.message_count = diagnostic
            .protocol
            .as_ref()
            .map_or(0, |value| value.message_count),
        codex.protocol.notification_count = diagnostic
            .protocol
            .as_ref()
            .map_or(0, |value| value.notification_count),
        codex.protocol.captured_bytes = diagnostic
            .protocol
            .as_ref()
            .map_or(0, |value| value.captured_bytes),
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
    state: &mut ProtocolState,
    capture_bytes: &mut usize,
) -> Result<Value, CodexAppServerError> {
    let line = read_bounded_line(reader)
        .await
        .map_err(|error| {
            if error == CodexAppServerError::Protocol {
                state.protocol_error("frame_invalid")
            } else {
                error
            }
        })?
        .ok_or(CodexAppServerError::ProcessExited)?;
    record_capture_bytes(capture_bytes, line.len())
        .map_err(|_| state.protocol_error("capture_limit_exceeded"))?;
    state.capture_diagnostic.captured_bytes = *capture_bytes;
    let message =
        serde_json::from_slice(&line).map_err(|_| state.protocol_error("json_invalid"))?;
    state.observe_message_class(&message, *capture_bytes);
    Ok(message)
}

async fn read_optional_message<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    state: &mut ProtocolState,
    capture_bytes: &mut usize,
) -> Result<Option<Value>, CodexAppServerError> {
    let Some(line) = read_bounded_line(reader).await.map_err(|error| {
        if error == CodexAppServerError::Protocol {
            state.protocol_error("frame_invalid")
        } else {
            error
        }
    })?
    else {
        return Ok(None);
    };
    record_capture_bytes(capture_bytes, line.len())
        .map_err(|_| state.protocol_error("capture_limit_exceeded"))?;
    state.capture_diagnostic.captured_bytes = *capture_bytes;
    let message =
        serde_json::from_slice(&line).map_err(|_| state.protocol_error("json_invalid"))?;
    state.observe_message_class(&message, *capture_bytes);
    Ok(Some(message))
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
        MalformedEvent,
        UnexpectedResponse,
        MismatchedSavedPath,
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
                FakeMode::MalformedEvent => "printf 'not-json\\n'\n".to_string(),
                FakeMode::UnexpectedResponse => {
                    "printf '{\"id\":99,\"result\":{}}\\n'\n".to_string()
                }
                FakeMode::MismatchedSavedPath => format!(
                    "printf '{{\"method\":\"item/started\",\"params\":{{\"threadId\":\"{THREAD_ID}\",\"turnId\":\"{TURN_ID}\",\"item\":{{\"type\":\"imageGeneration\",\"id\":\"{CALL_ID}\",\"status\":\"inProgress\"}}}}}}\\n'\nprintf '{{\"method\":\"item/completed\",\"params\":{{\"threadId\":\"{THREAD_ID}\",\"turnId\":\"{TURN_ID}\",\"item\":{{\"type\":\"imageGeneration\",\"id\":\"{CALL_ID}\",\"status\":\"completed\",\"result\":\"cG5n\",\"savedPath\":\"/private/secret-token.png\"}}}}}}\\n'\n"
                ),
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
            self.run_capturing(timeout).await.0
        }

        async fn run_capturing(
            &self,
            timeout: Duration,
        ) -> (
            Result<Vec<u8>, CodexAppServerError>,
            Option<CodexAppServerFailureDiagnosticV1>,
        ) {
            let diagnostic = std::sync::Mutex::new(None);
            let sink = |value: &CodexAppServerFailureDiagnosticV1| {
                *diagnostic.lock().map_err(|_| ())? = Some(value.clone());
                Ok(())
            };
            let result = run_codex_app_server(
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
                    failure_diagnostic_sink: Some(&sink),
                },
                |_| Ok(()),
            )
            .await;
            (result, diagnostic.into_inner().unwrap())
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

    #[tokio::test]
    async fn protocol_failures_persist_only_bounded_content_free_phase_and_reason() {
        for (mode, phase, reason, last_message_class) in [
            (
                FakeMode::MalformedEvent,
                "event_stream",
                "json_invalid",
                "rpc_response",
            ),
            (
                FakeMode::UnexpectedResponse,
                "event_stream",
                "unexpected_response",
                "rpc_response",
            ),
            (
                FakeMode::MismatchedSavedPath,
                "event_stream",
                "image_saved_path_mismatch",
                "item_completed",
            ),
            (
                FakeMode::MalformedSuffix,
                "post_terminal",
                "json_invalid",
                "turn_completed",
            ),
        ] {
            let fixture = FakeAppServer::new(mode);
            let (result, diagnostic) = fixture.run_capturing(Duration::from_secs(30)).await;
            assert_eq!(result, Err(CodexAppServerError::Protocol));
            let diagnostic = diagnostic.expect("failure diagnostic must be emitted");
            let protocol = diagnostic.protocol.as_ref().unwrap();
            assert_eq!(protocol.phase, phase);
            assert_eq!(protocol.reason, reason);
            assert_eq!(protocol.last_message_class, last_message_class);
            assert!(protocol.message_count >= 5);
            assert!(protocol.notification_count >= 2);
            assert!(protocol.captured_bytes > 0);
            let serialized = serde_json::to_string(&diagnostic).unwrap();
            assert!(!serialized.contains("secret-token"));
            assert!(!serialized.contains("invoke image_gen"));
            assert!(!serialized.contains("generated_images"));
            assert!(serialized.len() < 4096);

            // Additive V1 fields do not prevent reading diagnostics from earlier releases.
            let mut old = serde_json::to_value(&diagnostic).unwrap();
            old.as_object_mut().unwrap().remove("protocol");
            assert!(serde_json::from_value::<CodexAppServerFailureDiagnosticV1>(old).is_ok());
        }
    }

    #[test]
    fn identity_and_authority_protocol_errors_replace_stale_reasons() {
        let thread_id = Uuid::parse_str(THREAD_ID).unwrap();
        let reason = |state: &ProtocolState| {
            build_failure_diagnostic(
                state,
                CodexAppServerError::Protocol,
                None,
                &ExitDiagnostic::default(),
            )
            .protocol
            .unwrap()
            .reason
        };

        let mut state = ProtocolState::default();
        state.protocol_phase("event_stream");
        state.thread_id = Some(thread_id);
        state.capture_diagnostic.reason = "stale_reason";
        assert_eq!(
            state.observe_bound_identity(&json!({ "threadId": THREAD_ID, "turnId": TURN_ID })),
            Err(CodexAppServerError::Protocol)
        );
        assert_eq!(reason(&state), "turn_identity_missing");

        state.turn_id = Some(TURN_ID.to_string());
        assert_eq!(
            state.observe_bound_identity(&json!({ "threadId": "invalid", "turnId": TURN_ID })),
            Err(CodexAppServerError::Protocol)
        );
        assert_eq!(reason(&state), "item_thread_id_invalid");
        assert_eq!(
            state.observe_bound_identity(&json!({ "threadId": THREAD_ID })),
            Err(CodexAppServerError::Protocol)
        );
        assert_eq!(reason(&state), "item_turn_id_missing");
        assert_eq!(
            state.observe_turn_identity(&json!({ "threadId": "invalid" }), TURN_ID),
            Err(CodexAppServerError::Protocol)
        );
        assert_eq!(reason(&state), "turn_thread_id_invalid");

        let mut state = ProtocolState::default();
        state.protocol_phase("authority");
        state.turn_id = Some(TURN_ID.to_string());
        state.announced_turn_id = state.turn_id.clone();
        state.started_image_count = 1;
        state.completed_image_count = 1;
        state.completed_image_call_id = Some(CALL_ID.to_string());
        state.capture_diagnostic.reason = "stale_reason";
        assert_eq!(state.authority(), Err(CodexAppServerError::Protocol));
        assert_eq!(reason(&state), "thread_identity_missing");

        state.thread_id = Some(thread_id);
        state.announced_thread_id = Some(thread_id);
        state.completed_image_call_id = None;
        assert_eq!(state.authority(), Err(CodexAppServerError::Protocol));
        assert_eq!(reason(&state), "image_call_id_missing");
    }

    #[tokio::test]
    async fn concurrent_outputs_keep_failure_capture_private_to_its_child() {
        let good = FakeAppServer::new_with_payload(FakeMode::Normal, b"good-private-image");
        let bad = FakeAppServer::new(FakeMode::MismatchedSavedPath);
        let (good_run, bad_run) = tokio::join!(
            good.run_capturing(Duration::from_secs(30)),
            bad.run_capturing(Duration::from_secs(30))
        );
        assert_eq!(good_run.0, Ok(good.expected));
        assert!(good_run.1.is_none());
        assert_eq!(bad_run.0, Err(CodexAppServerError::Protocol));
        assert_eq!(
            bad_run.1.unwrap().protocol.unwrap().reason,
            "image_saved_path_mismatch"
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
        assert_eq!(classify_bytes(b"moderation rejected"), "policy");
        assert_eq!(classify_bytes(b"request blocked"), "policy");
        assert_eq!(
            classify_bytes(b"invalid_request rejected"),
            "invalid_request"
        );
        assert_eq!(classify_bytes(b"provider rejected"), "rejected");
        assert_eq!(classify_bytes(b"retention rejected"), "retention");
        assert_eq!(classify_bytes(b"organization rejected"), "organization");
        assert_eq!(classify_bytes(b"account suspended rejected"), "account");
        assert_eq!(classify_bytes(b"prompt rejected"), "prompt");
        assert_eq!(
            classify_stream_bytes(b"HTTP 401 rejected"),
            "http_status:401:rejected"
        );
        assert_eq!(classify_stream_bytes(b"HTTP 4010 rejected"), "rejected");
        assert_eq!(classify_stream_bytes(b"HTTP 401abc rejected"), "rejected");
        assert_eq!(
            classify_stream_bytes(b"HTTP 401 rejected\nHTTP 503 server error"),
            "http_status_conflict:401+503"
        );
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
    fn only_definitive_authentication_rejections_are_retryable() {
        let persisted = |failure_category: &str, numeric_code: Option<i64>, stderr_class: &str| {
            CodexAppServerFailureDiagnosticV1 {
                schema_version: 1,
                failure_category: failure_category.to_string(),
                source: "image_generation_item".to_string(),
                class: "tool_failure".to_string(),
                numeric_code,
                code: PersistedFieldDiagnostic::default(),
                message: PersistedFieldDiagnostic::default(),
                stderr: Some(PersistedStreamDiagnostic {
                    sha256: "a".repeat(64),
                    bytes: 32,
                    truncated: false,
                    class: stderr_class.to_string(),
                }),
                exit: PersistedExitDiagnostic {
                    observed: true,
                    code: Some(0),
                    signal: None,
                },
                protocol: None,
            }
        };

        for diagnostic in [
            persisted("codex_image_tool_failed", Some(401), "unknown"),
            persisted("codex_image_tool_failed", None, "http_status:401"),
            persisted("codex_image_tool_failed", None, "http_status:401:rejected"),
            persisted(
                "codex_image_tool_failed",
                None,
                "http_status:401:authentication+rejected",
            ),
            persisted("codex_image_tool_failed", Some(401), "rejected"),
        ] {
            assert!(diagnostic.is_retryable_authentication_rejection());
        }
        let mut policy_with_numeric_status =
            persisted("codex_image_tool_failed", Some(401), "unknown");
        policy_with_numeric_status.class = "policy".to_string();
        for diagnostic in [
            policy_with_numeric_status,
            persisted(
                "codex_image_tool_failed",
                Some(401),
                "http_status:401:policy+rejected",
            ),
            persisted("codex_image_tool_failed", None, "rejected"),
            persisted("codex_image_tool_failed", None, "http_status:429"),
            persisted("codex_image_tool_failed", None, "http_status:503"),
            persisted("codex_image_tool_failed", Some(401), "http_status:503"),
            persisted("codex_image_tool_failed", Some(403), "http_status:401"),
            persisted(
                "codex_image_tool_failed",
                Some(401),
                "api_code:authentication",
            ),
            persisted(
                "codex_image_tool_failed",
                Some(401),
                "api_code:rate_limit_exceeded",
            ),
            persisted("codex_image_tool_failed", None, "http_status:4010"),
            persisted("codex_image_tool_failed", None, "http_status:401:"),
            persisted("codex_turn_failed", Some(401), "http_status:401"),
        ] {
            assert!(!diagnostic.is_retryable_authentication_rejection());
        }
        for explicit_nonretryable_signal in [
            "content_policy",
            "cyber_policy",
            "safety",
            "moderation",
            "policy",
            "blocked",
            "invalid_request",
            "unsupported",
            "rate_limit",
            "originator",
            "entitlement",
            "forbidden",
            "availability",
            "retention",
            "organization",
            "account",
            "prompt",
            "future_explicit_failure",
        ] {
            let diagnostic = persisted(
                "codex_image_tool_failed",
                None,
                &format!("http_status:401:{explicit_nonretryable_signal}"),
            );
            assert!(!diagnostic.is_retryable_authentication_rejection());
            let mut diagnostic = persisted(
                "codex_image_tool_failed",
                Some(401),
                "http_status:401:rejected",
            );
            diagnostic.class = explicit_nonretryable_signal.to_string();
            assert!(!diagnostic.is_retryable_authentication_rejection());
        }

        let persisted_from_failure = |message: &str| {
            let mut state = ProtocolState::default();
            state.record_failure(
                "image_generation_item",
                &json!({ "code": 401, "message": message }),
            );
            build_failure_diagnostic(
                &state,
                CodexAppServerError::ImageToolFailed,
                Some(&StreamDiagnostic {
                    sha256: "a".repeat(64),
                    bytes: 32,
                    truncated: false,
                    class: "http_status:401:rejected".to_string(),
                }),
                &ExitDiagnostic {
                    observed: true,
                    code: Some(0),
                    signal: None,
                },
            )
        };

        let persisted = persisted_from_failure("rejected");
        assert_eq!(persisted.class, "rejected");
        assert_eq!(persisted.numeric_code, Some(401));
        assert!(persisted.is_retryable_authentication_rejection());

        for code in [
            "authentication",
            "authentication_error",
            "unauthorized",
            "credential_rejected",
            "rejected",
        ] {
            let mut state = ProtocolState::default();
            state.record_failure(
                "image_generation_item",
                &json!({
                    "type": "imageGeneration",
                    "status": "failed",
                    "result": { "code": code }
                }),
            );
            let persisted = build_failure_diagnostic(
                &state,
                CodexAppServerError::ImageToolFailed,
                Some(&StreamDiagnostic {
                    class: classify_stream_bytes(b"HTTP 401 rejected"),
                    ..StreamDiagnostic::default()
                }),
                &ExitDiagnostic {
                    observed: true,
                    code: Some(0),
                    signal: None,
                },
            );
            assert!(persisted.is_retryable_authentication_rejection(), "{code}");
        }

        for (message, expected_class) in [
            ("content_policy rejected", "content_policy"),
            ("cyber_policy rejected", "content_policy"),
            ("safety rejected", "policy"),
            ("moderation rejected", "policy"),
            ("policy rejected", "policy"),
            ("blocked rejected", "policy"),
            ("invalid_request rejected", "invalid_request"),
            ("unsupported rejected", "invalid_request"),
            ("authentication blocked rejected", "policy"),
            ("unauthorized moderation rejected", "policy"),
            ("credential invalid_request rejected", "invalid_request"),
            ("rate_limit rejected", "rate_limit"),
            ("originator rejected", "originator_policy"),
            ("entitlement rejected", "entitlement"),
            ("forbidden rejected", "forbidden"),
            ("unavailable authentication rejected", "availability"),
            ("retention rejected", "retention"),
            ("organization rejected", "organization"),
            ("account suspended rejected", "account"),
            ("prompt rejected", "prompt"),
        ] {
            let persisted = persisted_from_failure(message);
            assert_eq!(persisted.class, expected_class);
            assert!(!persisted.is_retryable_authentication_rejection());
        }

        for (code, expected_class) in [
            ("account_suspended", "account"),
            ("account_suspended_rejected", "account"),
            ("retention", "retention"),
            ("future_explicit_failure", "explicit_failure"),
            ("future_explicit_failure_rejected", "explicit_failure"),
        ] {
            let mut state = ProtocolState::default();
            state.record_failure(
                "image_generation_item",
                &json!({
                    "type": "imageGeneration",
                    "status": "failed",
                    "result": { "code": code }
                }),
            );
            let persisted = build_failure_diagnostic(
                &state,
                CodexAppServerError::ImageToolFailed,
                Some(&StreamDiagnostic {
                    class: classify_stream_bytes(b"HTTP 401 rejected"),
                    ..StreamDiagnostic::default()
                }),
                &ExitDiagnostic {
                    observed: true,
                    code: Some(0),
                    signal: None,
                },
            );
            assert_eq!(persisted.class, expected_class, "{code}");
            assert!(!persisted.is_retryable_authentication_rejection(), "{code}");
        }

        let mut state = ProtocolState::default();
        state.record_failure(
            "image_generation_item",
            &json!({
                "type": "imageGeneration",
                "status": "failed",
                "result": { "message": "future provider failure" }
            }),
        );
        assert_eq!(state.failure_diagnostic.unwrap().class, "explicit_failure");
    }

    #[test]
    fn conflicting_structured_failure_signals_never_retry_authentication() {
        for (value, expected_class, expected_numeric_code) in [
            (
                json!({
                    "type": "imageGeneration",
                    "status": "failed",
                    "result": {
                        "code": "rejected",
                        "error": { "code": "content_policy" }
                    }
                }),
                "content_policy",
                None,
            ),
            (
                json!({
                    "type": "imageGeneration",
                    "status": "failed",
                    "result": {
                        "type": "authentication_error",
                        "error": { "type": "server_error" }
                    }
                }),
                "explicit_failure",
                None,
            ),
            (
                json!({
                    "type": "imageGeneration",
                    "status": "failed",
                    "result": {
                        "message": "rejected",
                        "error": { "message": "content_policy rejected" }
                    }
                }),
                "content_policy",
                None,
            ),
            (
                json!({
                    "type": "imageGeneration",
                    "status": "failed",
                    "code": 401,
                    "result": { "code": 403 }
                }),
                "explicit_failure",
                Some(401),
            ),
            (
                json!({
                    "type": "imageGeneration",
                    "status": "failed",
                    "result": { "code": "rejected", "status": 403 }
                }),
                "explicit_failure",
                Some(403),
            ),
            (
                json!({
                    "type": "imageGeneration",
                    "status": "failed",
                    "result": { "code": 403.0 }
                }),
                "explicit_failure",
                None,
            ),
            (
                json!({
                    "type": "imageGeneration",
                    "status": "failed",
                    "result": { "code": 1e100 }
                }),
                "explicit_failure",
                None,
            ),
            (
                json!({
                    "type": "imageGeneration",
                    "status": "failed",
                    "result": {
                        "message": "rejected",
                        "error": { "message": "server_error" }
                    }
                }),
                "explicit_failure",
                None,
            ),
            (
                json!({
                    "type": "imageGeneration",
                    "status": "failed",
                    "result": { "message": "HTTP 503 rejected" }
                }),
                "explicit_failure",
                None,
            ),
            (
                json!({
                    "type": "imageGeneration",
                    "status": "failed",
                    "result": {
                        "message": "HTTP 401 rejected",
                        "error": { "message": "HTTP 503" }
                    }
                }),
                "explicit_failure",
                None,
            ),
            (
                json!({
                    "type": "imageGeneration",
                    "status": "failed",
                    "result": {
                        "code": "authentication_error",
                        "message": "server_error rejected"
                    }
                }),
                "explicit_failure",
                None,
            ),
        ] {
            let mut state = ProtocolState::default();
            state.record_failure("image_generation_item", &value);
            let persisted = build_failure_diagnostic(
                &state,
                CodexAppServerError::ImageToolFailed,
                Some(&StreamDiagnostic {
                    class: classify_stream_bytes(b"HTTP 401 rejected"),
                    ..StreamDiagnostic::default()
                }),
                &ExitDiagnostic {
                    observed: true,
                    code: Some(0),
                    signal: None,
                },
            );

            assert_eq!(persisted.class, expected_class, "{value}");
            assert_eq!(persisted.numeric_code, expected_numeric_code, "{value}");
            assert!(
                !persisted.is_retryable_authentication_rejection(),
                "{value}"
            );
        }
    }

    #[test]
    fn production_http_401_rejection_preserves_unknown_item_shape() {
        let mut state = ProtocolState::default();
        state.record_failure(
            "image_generation_item",
            &json!({
                "type": "imageGeneration", "status": "failed", "result": null
            }),
        );
        let persisted = build_failure_diagnostic(
            &state,
            CodexAppServerError::ImageToolFailed,
            Some(&StreamDiagnostic {
                class: classify_stream_bytes(b"HTTP 401 rejected"),
                ..StreamDiagnostic::default()
            }),
            &ExitDiagnostic {
                observed: true,
                code: Some(0),
                signal: None,
            },
        );
        assert_eq!(persisted.class, "unknown");
        assert_eq!(persisted.numeric_code, None);
        assert_eq!(persisted.code.bytes, "imageGeneration".len());
        assert_eq!(persisted.message.bytes, 0);
        assert_eq!(
            persisted.stderr.as_ref().unwrap().class,
            "http_status:401:rejected"
        );
        assert!(persisted.is_retryable_authentication_rejection());

        for (signal, expected_class) in [
            ("forbidden", "forbidden"),
            ("rate_limit", "rate_limit"),
            ("unavailable", "availability"),
            ("invalid_request", "invalid_request"),
            ("retention", "retention"),
            ("organization", "organization"),
            ("account", "account"),
            ("prompt", "prompt"),
        ] {
            let mut diagnostic = persisted.clone();
            let stderr_class =
                classify_stream_bytes(format!("HTTP 401 {signal} rejected").as_bytes());
            assert!(
                stderr_class
                    .split([':', '+'])
                    .any(|value| value == expected_class)
            );
            diagnostic.stderr.as_mut().unwrap().class = stderr_class;
            assert!(
                !diagnostic.is_retryable_authentication_rejection(),
                "{signal}"
            );
        }
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
        assert_eq!(
            state.observe_notification(&item("webSearch"), home),
            Err(CodexAppServerError::Protocol)
        );
    }
}
