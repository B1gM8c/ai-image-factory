use std::path::PathBuf;

use image_provider_contracts::{ProviderCostEvidenceScope, ProviderReportedCostEvidenceV1};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::{GrokInvocationV1, PROVIDER_ID};

pub const MAX_STDOUT_BYTES: usize = 64 * 1024;
pub const MAX_HISTORY_BYTES: usize = 1024 * 1024;
const MAX_JSONL_RECORDS: usize = 2_048;

#[derive(Clone, Debug, PartialEq)]
pub struct GrokCliReceiptV1 {
    session_id: String,
    headless_request_id: String,
    stop_reason: String,
    artifact_path: PathBuf,
    effective_tool_prompt: Option<String>,
    headless_usage: Option<Value>,
    provider_reported_cost: Option<ProviderReportedCostEvidenceV1>,
}

impl GrokCliReceiptV1 {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn headless_request_id(&self) -> &str {
        &self.headless_request_id
    }

    pub fn stop_reason(&self) -> &str {
        &self.stop_reason
    }

    pub fn artifact_path(&self) -> &std::path::Path {
        &self.artifact_path
    }

    pub fn effective_tool_prompt(&self) -> Option<&str> {
        self.effective_tool_prompt.as_deref()
    }

    /// Agent-model usage is observability data, not authoritative media billing.
    pub fn headless_usage(&self) -> Option<&Value> {
        self.headless_usage.as_ref()
    }

    /// Exact provider-reported cost for the entire CLI invocation, when emitted.
    pub fn provider_reported_cost(&self) -> Option<&ProviderReportedCostEvidenceV1> {
        self.provider_reported_cost.as_ref()
    }
}

pub fn parse_invocation_receipt(
    stdout: &[u8],
    history: &[u8],
    invocation: &GrokInvocationV1,
) -> Result<GrokCliReceiptV1, GrokReceiptError> {
    let end = parse_end_event(stdout, invocation.session_id())?;
    let tool_result = parse_history(history, invocation)?;
    Ok(GrokCliReceiptV1 {
        session_id: end.session_id,
        headless_request_id: end.request_id,
        stop_reason: end.stop_reason,
        artifact_path: invocation.artifact_path().to_path_buf(),
        effective_tool_prompt: tool_result.effective_tool_prompt,
        headless_usage: end.usage,
        provider_reported_cost: end.provider_reported_cost,
    })
}

#[derive(Debug, Eq, Error, PartialEq)]
pub enum GrokReceiptError {
    #[error("Grok stdout is empty or exceeds the bounded receipt limit")]
    InvalidStdoutSize,
    #[error("Grok history is empty or exceeds the bounded history limit")]
    InvalidHistorySize,
    #[error("Grok streaming output contains invalid JSON")]
    InvalidStreamingJson,
    #[error("Grok streaming output reported an error: {0}")]
    CliError(String),
    #[error("Grok streaming output must end with exactly one end event")]
    MissingTerminalEvent,
    #[error("Grok terminal event does not match the expected session")]
    SessionMismatch,
    #[error("Grok terminal event contains invalid identifiers")]
    InvalidTerminalEvent,
    #[error("Grok history contains invalid JSON")]
    InvalidHistoryJson,
    #[error("Grok history contains an unexpected tool call")]
    UnexpectedToolCall,
    #[error("Grok history must contain exactly one expected tool call and result")]
    MissingToolResult,
    #[error("Grok tool arguments differ from the admitted request")]
    ToolArgumentsMismatch,
    #[error("Grok video generation requires output.upload_url for a Zero Data Retention team")]
    VideoOutputUploadUrlRequired,
    #[error("Grok tool execution failed")]
    ToolExecutionFailed,
    #[error("Grok tool result is invalid")]
    InvalidToolResult,
    #[error("Grok tool result points outside the expected session artifact path")]
    ArtifactPathMismatch,
}

struct EndEvent {
    session_id: String,
    request_id: String,
    stop_reason: String,
    usage: Option<Value>,
    provider_reported_cost: Option<ProviderReportedCostEvidenceV1>,
}

struct ParsedToolResult {
    effective_tool_prompt: Option<String>,
}

fn parse_end_event(stdout: &[u8], expected_session: &str) -> Result<EndEvent, GrokReceiptError> {
    if stdout.is_empty() || stdout.len() > MAX_STDOUT_BYTES {
        return Err(GrokReceiptError::InvalidStdoutSize);
    }
    let text = std::str::from_utf8(stdout).map_err(|_| GrokReceiptError::InvalidStreamingJson)?;
    let mut records = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        if records.len() == MAX_JSONL_RECORDS {
            return Err(GrokReceiptError::InvalidStreamingJson);
        }
        let value: Value =
            serde_json::from_str(line).map_err(|_| GrokReceiptError::InvalidStreamingJson)?;
        if value.get("type").and_then(Value::as_str) == Some("error") {
            let message = value
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown Grok CLI error");
            return Err(GrokReceiptError::CliError(message.to_owned()));
        }
        records.push((value, line.as_bytes()));
    }
    let Some((last, raw_last)) = records.last() else {
        return Err(GrokReceiptError::MissingTerminalEvent);
    };
    let end_count = records
        .iter()
        .filter(|(value, _)| value.get("type").and_then(Value::as_str) == Some("end"))
        .count();
    if end_count != 1 || last.get("type").and_then(Value::as_str) != Some("end") {
        return Err(GrokReceiptError::MissingTerminalEvent);
    }

    let session_id = required_text(last, "sessionId")?;
    if session_id != expected_session {
        return Err(GrokReceiptError::SessionMismatch);
    }
    let request_id = required_text(last, "requestId")?;
    let stop_reason = required_text(last, "stopReason")?;
    let provider_reported_cost = provider_reported_cost(last, raw_last, &request_id)?;
    Ok(EndEvent {
        session_id,
        request_id,
        stop_reason,
        usage: projected_usage(last),
        provider_reported_cost,
    })
}

fn parse_history(
    history: &[u8],
    invocation: &GrokInvocationV1,
) -> Result<ParsedToolResult, GrokReceiptError> {
    if history.is_empty() || history.len() > MAX_HISTORY_BYTES {
        return Err(GrokReceiptError::InvalidHistorySize);
    }
    let text = std::str::from_utf8(history).map_err(|_| GrokReceiptError::InvalidHistoryJson)?;
    let mut expected_call: Option<(String, Value)> = None;
    let mut matching_result: Option<Value> = None;
    let mut record_count = 0_usize;

    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        record_count += 1;
        if record_count > MAX_JSONL_RECORDS {
            return Err(GrokReceiptError::InvalidHistorySize);
        }
        let value: Value =
            serde_json::from_str(line).map_err(|_| GrokReceiptError::InvalidHistoryJson)?;
        match value.get("type").and_then(Value::as_str) {
            Some("assistant") => {
                let Some(calls) = value.get("tool_calls") else {
                    continue;
                };
                let calls = calls
                    .as_array()
                    .ok_or(GrokReceiptError::InvalidHistoryJson)?;
                for call in calls {
                    if expected_call.is_some() {
                        return Err(GrokReceiptError::UnexpectedToolCall);
                    }
                    let name = call
                        .get("name")
                        .and_then(Value::as_str)
                        .ok_or(GrokReceiptError::InvalidHistoryJson)?;
                    if name != invocation.tool().name() {
                        return Err(GrokReceiptError::UnexpectedToolCall);
                    }
                    let id = call
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|value| valid_text(value))
                        .ok_or(GrokReceiptError::InvalidHistoryJson)?;
                    let arguments = call
                        .get("arguments")
                        .and_then(Value::as_str)
                        .ok_or(GrokReceiptError::InvalidHistoryJson)?;
                    let arguments: Value = serde_json::from_str(arguments)
                        .map_err(|_| GrokReceiptError::InvalidHistoryJson)?;
                    expected_call = Some((id.to_owned(), arguments));
                }
            }
            Some("tool_result") => {
                let Some((call_id, _)) = expected_call.as_ref() else {
                    return Err(GrokReceiptError::UnexpectedToolCall);
                };
                if value.get("tool_call_id").and_then(Value::as_str) != Some(call_id) {
                    return Err(GrokReceiptError::UnexpectedToolCall);
                }
                if matching_result.is_some() {
                    return Err(GrokReceiptError::MissingToolResult);
                }
                let content = value
                    .get("content")
                    .and_then(Value::as_str)
                    .ok_or(GrokReceiptError::InvalidToolResult)?;
                let content = content.trim();
                if content.contains(
                    "Zero Data Retention teams must provide output.upload_url for video generation",
                ) {
                    return Err(GrokReceiptError::VideoOutputUploadUrlRequired);
                }
                if content.starts_with("Tool `") && content.contains("` failed:") {
                    return Err(GrokReceiptError::ToolExecutionFailed);
                }
                matching_result = Some(
                    serde_json::from_str(content)
                        .map_err(|_| GrokReceiptError::InvalidToolResult)?,
                );
            }
            _ => {}
        }
    }

    let Some((_, actual_arguments)) = expected_call else {
        return Err(GrokReceiptError::MissingToolResult);
    };
    let Some(tool_result) = matching_result else {
        return Err(GrokReceiptError::MissingToolResult);
    };
    if actual_arguments != *invocation.expected_arguments() {
        return Err(GrokReceiptError::ToolArgumentsMismatch);
    }
    validate_tool_result(&tool_result, invocation)?;
    Ok(ParsedToolResult {
        effective_tool_prompt: actual_arguments
            .get("prompt")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

fn validate_tool_result(
    tool_result: &Value,
    invocation: &GrokInvocationV1,
) -> Result<(), GrokReceiptError> {
    let path = tool_result
        .get("path")
        .and_then(Value::as_str)
        .ok_or(GrokReceiptError::InvalidToolResult)?;
    let filename = tool_result
        .get("filename")
        .and_then(Value::as_str)
        .ok_or(GrokReceiptError::InvalidToolResult)?;
    let session_folder = tool_result
        .get("session_folder")
        .and_then(Value::as_str)
        .ok_or(GrokReceiptError::InvalidToolResult)?;
    if std::path::Path::new(path) != invocation.artifact_path()
        || invocation
            .artifact_path()
            .file_name()
            .and_then(|name| name.to_str())
            != Some(filename)
        || invocation
            .artifact_path()
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            != Some(session_folder)
    {
        return Err(GrokReceiptError::ArtifactPathMismatch);
    }
    Ok(())
}

fn required_text(value: &Value, field: &str) -> Result<String, GrokReceiptError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| valid_text(value))
        .map(str::to_owned)
        .ok_or(GrokReceiptError::InvalidTerminalEvent)
}

fn valid_text(value: &str) -> bool {
    !value.trim().is_empty() && !value.chars().any(char::is_control)
}

fn projected_usage(end: &Value) -> Option<Value> {
    const FIELDS: [&str; 7] = [
        "inputTokens",
        "outputTokens",
        "cacheReadInputTokens",
        "modelCalls",
        "costUSD",
        "modelUsage",
        "usage_is_incomplete",
    ];
    let mut usage = Map::new();
    for field in FIELDS {
        if let Some(value) = end.get(field) {
            usage.insert(field.to_owned(), value.clone());
        }
    }
    (!usage.is_empty()).then_some(Value::Object(usage))
}

fn provider_reported_cost(
    end: &Value,
    raw_end: &[u8],
    request_id: &str,
) -> Result<Option<ProviderReportedCostEvidenceV1>, GrokReceiptError> {
    let Some(quantity) = end.get("total_cost_usd_ticks") else {
        return Ok(None);
    };
    if end
        .get("usage_is_incomplete")
        .is_some_and(|value| value.as_bool() != Some(false))
    {
        return Err(GrokReceiptError::InvalidTerminalEvent);
    }
    let quantity = quantity
        .as_u64()
        .map(u128::from)
        .ok_or(GrokReceiptError::InvalidTerminalEvent)?;
    ProviderReportedCostEvidenceV1::usd_ticks(
        ProviderCostEvidenceScope::CliInvocation,
        PROVIDER_ID,
        "provider_cli",
        request_id,
        quantity,
        raw_end,
        "end.total_cost_usd_ticks",
    )
    .map(Some)
    .map_err(|_| GrokReceiptError::InvalidTerminalEvent)
}
