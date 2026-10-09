//! Content-free observations, never evidence that a failed invocation is safe to replay.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GrokCliErrorClass {
    Authentication,
    RateLimit,
    InvalidRequest,
    Unknown,
}

/// Only fixed categories escape this parser. Arbitrary CLI error text, tool arguments,
/// prompts, URLs, provider IDs and credentials must never become diagnostic fields.
pub fn grok_cli_error_class(stdout: &[u8]) -> GrokCliErrorClass {
    let mut observed = None;
    for record in jsonl(stdout, crate::MAX_STDOUT_BYTES).into_iter().flatten() {
        if record.get("type").and_then(Value::as_str) != Some("error") {
            continue;
        }
        let code = record.get("code").or_else(|| record.pointer("/error/code"));
        let class = match code.and_then(Value::as_str) {
            Some("unauthorized" | "authentication_error" | "invalid_api_key" | "token_expired") => {
                GrokCliErrorClass::Authentication
            }
            Some("rate_limit_exceeded" | "rate_limit_error") => GrokCliErrorClass::RateLimit,
            Some("invalid_request_error" | "invalid_argument" | "unsupported_parameter") => {
                GrokCliErrorClass::InvalidRequest
            }
            _ => GrokCliErrorClass::Unknown,
        };
        if observed.is_some_and(|previous| previous != class) {
            return GrokCliErrorClass::Unknown;
        }
        observed = Some(class);
    }
    observed.unwrap_or(GrokCliErrorClass::Unknown)
}

/// `Some(false)` means no media tool call was observed in a valid bounded history,
/// NOT that the provider performed no work. Missing/invalid history is unknown.
pub fn grok_media_tool_call_observed(history: Option<&[u8]>) -> Option<bool> {
    let records = jsonl(history?, crate::MAX_HISTORY_BYTES)?;
    Some(records.iter().any(|record| {
        record.get("type").and_then(Value::as_str) == Some("assistant")
            && record
                .get("tool_calls")
                .and_then(Value::as_array)
                .is_some_and(|calls| {
                    calls.iter().any(|call| {
                        matches!(
                            call.get("name").and_then(Value::as_str),
                            Some(
                                "image_gen"
                                    | "image_edit"
                                    | "video_gen"
                                    | "image_to_video"
                                    | "reference_to_video"
                            )
                        )
                    })
                })
    }))
}

fn jsonl(bytes: &[u8], max_bytes: usize) -> Option<Vec<Value>> {
    if bytes.is_empty() || bytes.len() > max_bytes {
        return None;
    }
    let text = std::str::from_utf8(bytes).ok()?;
    let mut records = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        if records.len() >= 2_048 {
            return None;
        }
        let record: Value = serde_json::from_str(line).ok()?;
        if !record.is_object() {
            return None;
        }
        records.push(record);
    }
    (!records.is_empty()).then_some(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_only_classify_structured_allowlisted_error_codes() {
        for (code, class) in [
            ("token_expired", GrokCliErrorClass::Authentication),
            ("rate_limit_exceeded", GrokCliErrorClass::RateLimit),
            ("unsupported_parameter", GrokCliErrorClass::InvalidRequest),
            ("secret-token", GrokCliErrorClass::Unknown),
        ] {
            let event = serde_json::json!({"type":"error", "code":code,
                "message":"secret prompt https://user:password@host/credential"});
            let classified = grok_cli_error_class(&serde_json::to_vec(&event).unwrap());
            assert_eq!(classified, class);
            let saved = serde_json::to_string(&classified).unwrap();
            assert!(!saved.contains("secret"));
            assert!(!saved.contains("password"));
        }
        assert_eq!(
            grok_cli_error_class(br#"{"type":"user","code":"token_expired"}"#),
            GrokCliErrorClass::Unknown
        );
        assert_eq!(
            grok_cli_error_class(b"error: token_expired"),
            GrokCliErrorClass::Unknown
        );
        assert_eq!(
            grok_cli_error_class(b"{\"type\":\"error\",\"code\":\"token_expired\"}\n{"),
            GrokCliErrorClass::Unknown
        );
    }

    #[test]
    fn diagnostics_distinguish_missing_history_from_observed_media_call() {
        assert_eq!(grok_media_tool_call_observed(None), None);
        assert_eq!(grok_media_tool_call_observed(Some(b"{")), None);
        assert_eq!(grok_media_tool_call_observed(Some(b"[]")), None);
        assert_eq!(
            grok_media_tool_call_observed(Some(br#"{"type":"user","text":"image_edit"}"#)),
            Some(false)
        );
        assert_eq!(grok_media_tool_call_observed(Some(br#"{"type":"assistant","tool_calls":[{"name":"image_edit","arguments":"secret"}]}"#)), Some(true));
        assert_eq!(
            grok_media_tool_call_observed(Some(&vec![b' '; crate::MAX_HISTORY_BYTES + 1])),
            None
        );
    }
}
