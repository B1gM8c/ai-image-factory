use std::{
    path::{Path, PathBuf},
    process::Stdio,
};

use axum::http::Uri;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

use super::CodexLoginMethod;

const INITIALIZE_ID: i64 = 1;
const LOGIN_ID: i64 = 2;
const ACCOUNT_ID: i64 = 3;
const RATE_LIMITS_ID: i64 = 4;
const MAX_PROTOCOL_LINE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum CodexAppServerError {
    #[error("codex app-server process is unavailable")]
    Process,
    #[error("codex app-server protocol is invalid")]
    Protocol,
    #[error("codex app-server request failed (code {code:?})")]
    Request { code: Option<i64> },
    #[error("codex account login failed")]
    Login,
}

impl CodexAppServerError {
    pub const fn reauthorization_required(&self) -> bool {
        matches!(self, Self::Login)
    }
}

#[derive(Clone, Debug)]
pub struct CodexLoginChallenge {
    pub provider_login_id: String,
    pub authorization_url: String,
    pub user_code: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CodexAccountSnapshot {
    pub email: Option<String>,
    pub plan_type: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CodexQuotaSnapshot {
    pub plan_type: Option<String>,
    pub credits_balance: Option<String>,
    pub credits_unlimited: Option<bool>,
    pub windows: Vec<CodexQuotaWindow>,
}

#[derive(Clone, Debug)]
pub struct CodexQuotaWindow {
    pub limit_id: String,
    pub limit_name: Option<String>,
    pub window_role: &'static str,
    pub window_duration_mins: Option<i64>,
    pub used_percent: i32,
    pub resets_at_ms: Option<i64>,
}

pub struct CodexAppServer {
    child: Child,
    stdin: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
}

impl CodexAppServer {
    pub async fn spawn(executable: &Path, codex_home: &Path) -> Result<Self, CodexAppServerError> {
        if !executable.is_absolute() || !codex_home.is_absolute() {
            return Err(CodexAppServerError::Process);
        }
        let mut child = Command::new(executable)
            .arg("app-server")
            .arg("--stdio")
            .env("CODEX_HOME", codex_home)
            .env_remove("CODEX_API_KEY")
            .env_remove("OPENAI_API_KEY")
            .env_remove("XAI_API_KEY")
            .current_dir(codex_home)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| CodexAppServerError::Process)?;
        let stdin = child.stdin.take().ok_or(CodexAppServerError::Process)?;
        let stdout = child.stdout.take().ok_or(CodexAppServerError::Process)?;
        let mut server = Self {
            child,
            stdin,
            lines: BufReader::new(stdout).lines(),
        };
        let initialized = server
            .request(
                INITIALIZE_ID,
                "initialize",
                Some(json!({
                    "clientInfo": {
                        "name": "ai-image-factory",
                        "title": "AI Image Factory",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": { "experimentalApi": false }
                })),
            )
            .await?;
        if !initialized.is_object() {
            return Err(CodexAppServerError::Protocol);
        }
        server.notify("initialized", None).await?;
        Ok(server)
    }

    pub async fn start_login(
        &mut self,
        method: CodexLoginMethod,
    ) -> Result<CodexLoginChallenge, CodexAppServerError> {
        let login_type = match method {
            CodexLoginMethod::BrowserOauth => "chatgpt",
            CodexLoginMethod::DeviceCode => "chatgptDeviceCode",
        };
        let result = self
            .request(
                LOGIN_ID,
                "account/login/start",
                Some(json!({ "type": login_type })),
            )
            .await?;
        parse_login_challenge(method, &result)
    }

    pub async fn wait_for_login(
        &mut self,
        expected_login_id: &str,
    ) -> Result<(), CodexAppServerError> {
        loop {
            let message = self.read_message().await?;
            if message.get("method").and_then(Value::as_str) != Some("account/login/completed") {
                continue;
            }
            let params = message
                .get("params")
                .and_then(Value::as_object)
                .ok_or(CodexAppServerError::Protocol)?;
            if params.get("loginId").and_then(Value::as_str) != Some(expected_login_id) {
                continue;
            }
            return match params.get("success").and_then(Value::as_bool) {
                Some(true) => Ok(()),
                Some(false) => Err(CodexAppServerError::Login),
                None => Err(CodexAppServerError::Protocol),
            };
        }
    }

    pub async fn account(&mut self) -> Result<CodexAccountSnapshot, CodexAppServerError> {
        self.read_account(false).await
    }

    pub async fn refresh_account(&mut self) -> Result<CodexAccountSnapshot, CodexAppServerError> {
        self.read_account(true).await
    }

    async fn read_account(
        &mut self,
        refresh_token: bool,
    ) -> Result<CodexAccountSnapshot, CodexAppServerError> {
        let result = self
            .request(
                ACCOUNT_ID,
                "account/read",
                Some(json!({ "refreshToken": refresh_token })),
            )
            .await?;
        let account = result
            .get("account")
            .and_then(Value::as_object)
            .ok_or(CodexAppServerError::Login)?;
        Ok(CodexAccountSnapshot {
            email: account
                .get("email")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            plan_type: account
                .get("planType")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        })
    }

    pub async fn quota(&mut self) -> Result<CodexQuotaSnapshot, CodexAppServerError> {
        let result = self
            .request(RATE_LIMITS_ID, "account/rateLimits/read", None)
            .await?;
        parse_quota(&result)
    }

    pub async fn shutdown(mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }

    async fn request(
        &mut self,
        id: i64,
        method: &str,
        params: Option<Value>,
    ) -> Result<Value, CodexAppServerError> {
        let mut message = json!({ "id": id, "method": method });
        if let Some(params) = params {
            message["params"] = params;
        }
        self.write_message(&message).await?;
        loop {
            let response = self.read_message().await?;
            if response.get("id").and_then(Value::as_i64) != Some(id) {
                continue;
            }
            if let Some(error) = response_error(&response) {
                return Err(error);
            }
            return response
                .get("result")
                .cloned()
                .ok_or(CodexAppServerError::Protocol);
        }
    }

    async fn notify(
        &mut self,
        method: &str,
        params: Option<Value>,
    ) -> Result<(), CodexAppServerError> {
        let mut message = json!({ "method": method });
        if let Some(params) = params {
            message["params"] = params;
        }
        self.write_message(&message).await
    }

    async fn write_message(&mut self, message: &Value) -> Result<(), CodexAppServerError> {
        let mut encoded = serde_json::to_vec(message).map_err(|_| CodexAppServerError::Protocol)?;
        if encoded.len() > MAX_PROTOCOL_LINE_BYTES {
            return Err(CodexAppServerError::Protocol);
        }
        encoded.push(b'\n');
        self.stdin
            .write_all(&encoded)
            .await
            .map_err(|_| CodexAppServerError::Process)?;
        self.stdin
            .flush()
            .await
            .map_err(|_| CodexAppServerError::Process)
    }

    async fn read_message(&mut self) -> Result<Value, CodexAppServerError> {
        let line = self
            .lines
            .next_line()
            .await
            .map_err(|_| CodexAppServerError::Process)?
            .ok_or(CodexAppServerError::Process)?;
        if line.len() > MAX_PROTOCOL_LINE_BYTES {
            return Err(CodexAppServerError::Protocol);
        }
        serde_json::from_str(&line).map_err(|_| CodexAppServerError::Protocol)
    }
}

fn response_error(response: &Value) -> Option<CodexAppServerError> {
    response
        .get("error")
        .map(|error| CodexAppServerError::Request {
            code: error.get("code").and_then(Value::as_i64),
        })
}

fn parse_login_challenge(
    method: CodexLoginMethod,
    result: &Value,
) -> Result<CodexLoginChallenge, CodexAppServerError> {
    let (expected_type, url_field) = match method {
        CodexLoginMethod::BrowserOauth => ("chatgpt", "authUrl"),
        CodexLoginMethod::DeviceCode => ("chatgptDeviceCode", "verificationUrl"),
    };
    if result.get("type").and_then(Value::as_str) != Some(expected_type) {
        return Err(CodexAppServerError::Protocol);
    }
    Ok(CodexLoginChallenge {
        provider_login_id: required_string(result, "loginId")?,
        authorization_url: required_https_url(result, url_field)?,
        user_code: match method {
            CodexLoginMethod::BrowserOauth => None,
            CodexLoginMethod::DeviceCode => Some(required_string(result, "userCode")?),
        },
    })
}

fn parse_quota(value: &Value) -> Result<CodexQuotaSnapshot, CodexAppServerError> {
    let rate_limits = value
        .get("rateLimitsByLimitId")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_else(|| {
            let mut fallback = serde_json::Map::new();
            if let Some(snapshot) = value.get("rateLimits") {
                let key = snapshot
                    .get("limitId")
                    .and_then(Value::as_str)
                    .unwrap_or("codex")
                    .to_owned();
                fallback.insert(key, snapshot.clone());
            }
            fallback
        });
    if rate_limits.is_empty() {
        return Err(CodexAppServerError::Protocol);
    }
    let mut windows = Vec::new();
    let mut plan_type = None;
    let mut credits_balance = None;
    let mut credits_unlimited = None;
    for (fallback_limit_id, snapshot) in rate_limits {
        plan_type = plan_type.or_else(|| {
            snapshot
                .get("planType")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        });
        if let Some(credits) = snapshot.get("credits") {
            credits_balance = credits
                .get("balance")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or(credits_balance);
            credits_unlimited = credits
                .get("unlimited")
                .and_then(Value::as_bool)
                .or(credits_unlimited);
        }
        let limit_id = snapshot
            .get("limitId")
            .and_then(Value::as_str)
            .unwrap_or(&fallback_limit_id)
            .to_owned();
        let limit_name = snapshot
            .get("limitName")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        for (field, role) in [("primary", "primary"), ("secondary", "secondary")] {
            let Some(window) = snapshot.get(field).and_then(Value::as_object) else {
                continue;
            };
            let used_percent = window
                .get("usedPercent")
                .and_then(Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())
                .filter(|value| (0..=100).contains(value))
                .ok_or(CodexAppServerError::Protocol)?;
            windows.push(CodexQuotaWindow {
                limit_id: limit_id.clone(),
                limit_name: limit_name.clone(),
                window_role: role,
                window_duration_mins: window.get("windowDurationMins").and_then(Value::as_i64),
                used_percent,
                resets_at_ms: window
                    .get("resetsAt")
                    .and_then(Value::as_i64)
                    .and_then(|seconds| seconds.checked_mul(1_000)),
            });
        }
    }
    Ok(CodexQuotaSnapshot {
        plan_type,
        credits_balance,
        credits_unlimited,
        windows,
    })
}

fn required_string(value: &Value, key: &str) -> Result<String, CodexAppServerError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or(CodexAppServerError::Protocol)
}

fn required_https_url(value: &Value, key: &str) -> Result<String, CodexAppServerError> {
    let raw = required_string(value, key)?;
    let uri = raw
        .parse::<Uri>()
        .map_err(|_| CodexAppServerError::Protocol)?;
    if uri.scheme_str() != Some("https") || uri.authority().is_none() {
        return Err(CodexAppServerError::Protocol);
    }
    Ok(raw)
}

pub fn resolve_executable(executable: PathBuf) -> Result<PathBuf, CodexAppServerError> {
    if executable.is_absolute() {
        return executable
            .is_file()
            .then_some(executable)
            .ok_or(CodexAppServerError::Process);
    }
    let path = std::env::var_os("PATH").ok_or(CodexAppServerError::Process)?;
    std::env::split_paths(&path)
        .map(|directory| directory.join(&executable))
        .find(|candidate| candidate.is_file())
        .ok_or(CodexAppServerError::Process)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_primary_and_secondary_rate_limit_windows() {
        let parsed = parse_quota(&json!({
            "rateLimits": {},
            "rateLimitsByLimitId": {
                "codex": {
                    "limitId": "codex",
                    "limitName": "Codex",
                    "planType": "pro",
                    "credits": { "balance": "12.5", "hasCredits": true, "unlimited": false },
                    "primary": { "usedPercent": 20, "windowDurationMins": 300, "resetsAt": 123 },
                    "secondary": { "usedPercent": 35, "windowDurationMins": 10080, "resetsAt": 456 }
                }
            }
        }))
        .expect("valid snapshot");
        assert_eq!(parsed.plan_type.as_deref(), Some("pro"));
        assert_eq!(parsed.windows.len(), 2);
        assert_eq!(parsed.windows[0].resets_at_ms, Some(123_000));
        assert_eq!(parsed.windows[1].window_duration_mins, Some(10_080));
    }

    #[test]
    fn preserves_json_rpc_error_code_without_message_content() {
        let error = response_error(&json!({
            "id": 4,
            "error": { "code": -32001, "message": "upstream details" }
        }))
        .expect("request error");
        assert!(matches!(
            error,
            CodexAppServerError::Request { code: Some(-32001) }
        ));
    }

    #[test]
    fn only_an_explicit_missing_account_requires_reauthorization() {
        assert!(CodexAppServerError::Login.reauthorization_required());
        assert!(!CodexAppServerError::Request { code: Some(-32603) }.reauthorization_required());
        assert!(!CodexAppServerError::Process.reauthorization_required());
        assert!(!CodexAppServerError::Protocol.reauthorization_required());
    }

    #[test]
    fn parses_browser_oauth_login_challenge() {
        let parsed = parse_login_challenge(
            CodexLoginMethod::BrowserOauth,
            &json!({
                "type": "chatgpt",
                "loginId": "oauth-login",
                "authUrl": "https://chatgpt.com/auth?state=opaque"
            }),
        )
        .expect("valid browser OAuth challenge");
        assert_eq!(parsed.provider_login_id, "oauth-login");
        assert_eq!(
            parsed.authorization_url,
            "https://chatgpt.com/auth?state=opaque"
        );
        assert_eq!(parsed.user_code, None);
    }

    #[test]
    fn parses_device_code_login_challenge() {
        let parsed = parse_login_challenge(
            CodexLoginMethod::DeviceCode,
            &json!({
                "type": "chatgptDeviceCode",
                "loginId": "device-login",
                "verificationUrl": "https://auth.openai.com/codex/device",
                "userCode": "ABCD-1234"
            }),
        )
        .expect("valid device-code challenge");
        assert_eq!(parsed.provider_login_id, "device-login");
        assert_eq!(
            parsed.authorization_url,
            "https://auth.openai.com/codex/device"
        );
        assert_eq!(parsed.user_code.as_deref(), Some("ABCD-1234"));
    }

    #[test]
    fn rejects_non_https_login_url() {
        let result = parse_login_challenge(
            CodexLoginMethod::BrowserOauth,
            &json!({
                "type": "chatgpt",
                "loginId": "oauth-login",
                "authUrl": "javascript:alert(1)"
            }),
        );
        assert!(matches!(result, Err(CodexAppServerError::Protocol)));
    }
}
