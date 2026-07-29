use std::{
    io,
    net::IpAddr,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    time::{Instant, sleep, timeout},
};

use crate::providers::dreamina_cli::prepare_dreamina_account_home;

use super::{CodexLoginMethod, grok_login::copy_proxy_environment};

const MAX_COMMAND_OUTPUT_BYTES: usize = 64 * 1024;
const LOGIN_START_TIMEOUT: Duration = Duration::from_secs(45);
const LOGIN_CHECK_TIMEOUT: Duration = Duration::from_secs(45);
const CREDIT_TIMEOUT: Duration = Duration::from_secs(30);
const LOGIN_RETRY_DELAY: Duration = Duration::from_secs(1);
const LOGIN_TTL: Duration = Duration::from_secs(15 * 60);

pub(super) struct DreaminaLoginChallenge {
    pub authorization_url: String,
    pub user_code: String,
}

pub(super) struct DreaminaLoginProcess {
    executable: PathBuf,
    home: PathBuf,
    device_code: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DreaminaAccountSnapshot {
    pub user_id: String,
    pub vip_level: Option<String>,
    pub total_credit: i64,
    pub cli_permission: DreaminaCliPermission,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DreaminaCliPermission {
    Granted,
    Required,
    Unknown,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum DreaminaCreditObservationError {
    #[error("Dreamina account authorization is no longer valid")]
    ReauthorizationRequired,
    #[error("Dreamina credit command is unavailable")]
    Unavailable(#[source] io::Error),
    #[error("Dreamina credit response is invalid")]
    InvalidResponse,
}

impl DreaminaCreditObservationError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ReauthorizationRequired => "dreamina_reauthorization_required",
            Self::Unavailable(_) => "dreamina_credit_observer_failed",
            Self::InvalidResponse => "dreamina_credit_response_invalid",
        }
    }

    pub fn reauthorization_required(&self) -> bool {
        matches!(self, Self::ReauthorizationRequired)
    }
}

impl DreaminaLoginProcess {
    pub async fn start(
        executable: &Path,
        home: &Path,
        method: CodexLoginMethod,
    ) -> io::Result<(Self, DreaminaLoginChallenge)> {
        if method != CodexLoginMethod::DeviceCode
            || !executable.is_absolute()
            || !home.is_absolute()
        {
            return Err(io::Error::other("Dreamina login configuration is invalid"));
        }
        prepare_dreamina_account_home(home)
            .await
            .map_err(io::Error::other)?;
        let output = run_cli(
            executable,
            home,
            &["login", "--headless"],
            LOGIN_START_TIMEOUT,
        )
        .await?;
        if !output.status.success() {
            return Err(io::Error::other("Dreamina login challenge failed"));
        }
        let text = combined_text(&output);
        let authorization_url = field_value(&text, "verification_uri")
            .filter(|value| valid_authorization_url(value))
            .ok_or_else(|| io::Error::other("Dreamina login URL is invalid"))?;
        let user_code = field_value(&text, "user_code")
            .filter(|value| valid_short_code(value))
            .ok_or_else(|| io::Error::other("Dreamina login user code is invalid"))?;
        let device_code = field_value(&text, "device_code")
            .filter(|value| valid_device_code(value))
            .ok_or_else(|| io::Error::other("Dreamina login device code is invalid"))?;
        Ok((
            Self {
                executable: executable.to_path_buf(),
                home: home.to_path_buf(),
                device_code,
            },
            DreaminaLoginChallenge {
                authorization_url,
                user_code,
            },
        ))
    }

    pub async fn wait(self) -> io::Result<Option<DreaminaAccountSnapshot>> {
        let deadline = Instant::now() + LOGIN_TTL;
        while Instant::now() < deadline {
            let device_argument = format!("--device_code={}", self.device_code);
            let output = run_cli(
                &self.executable,
                &self.home,
                &["login", "checklogin", &device_argument, "--poll=30"],
                LOGIN_CHECK_TIMEOUT,
            )
            .await?;
            let text = combined_text(&output);
            if let Ok(mut account) = observe_dreamina_credit(&self.executable, &self.home).await {
                if account.cli_permission == DreaminaCliPermission::Unknown {
                    account.cli_permission =
                        login_completion_permission(output.status.success(), &text);
                }
                return Ok(Some(account));
            }
            let text = text.to_ascii_lowercase();
            if text.contains("expired")
                || text.contains("denied")
                || text.contains("invalid device")
                || text.contains("已过期")
                || text.contains("已拒绝")
            {
                return Ok(None);
            }
            sleep(LOGIN_RETRY_DELAY).await;
        }
        Ok(None)
    }
}

pub(super) async fn observe_dreamina_account(
    executable: &Path,
    home: &Path,
) -> Result<DreaminaAccountSnapshot, DreaminaCreditObservationError> {
    let mut account = observe_dreamina_credit(executable, home).await?;
    if account.cli_permission == DreaminaCliPermission::Unknown {
        account.cli_permission = observe_dreamina_cli_permission(executable, home)
            .await
            .unwrap_or(DreaminaCliPermission::Unknown);
    }
    Ok(account)
}

pub(super) async fn observe_dreamina_credit(
    executable: &Path,
    home: &Path,
) -> Result<DreaminaAccountSnapshot, DreaminaCreditObservationError> {
    if !executable.is_absolute() || !home.is_absolute() {
        return Err(DreaminaCreditObservationError::Unavailable(
            io::Error::other("Dreamina credit observer configuration is invalid"),
        ));
    }
    prepare_dreamina_account_home(home)
        .await
        .map_err(io::Error::other)
        .map_err(DreaminaCreditObservationError::Unavailable)?;
    let output = run_cli(executable, home, &["user_credit"], CREDIT_TIMEOUT)
        .await
        .map_err(DreaminaCreditObservationError::Unavailable)?;
    let text = combined_text(&output);
    if !output.status.success() {
        return Err(if authorization_is_invalid(&text) {
            DreaminaCreditObservationError::ReauthorizationRequired
        } else {
            DreaminaCreditObservationError::Unavailable(io::Error::other(
                "Dreamina credit observation failed",
            ))
        });
    }
    parse_credit(&text).map_err(|_| DreaminaCreditObservationError::InvalidResponse)
}

struct CommandOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

async fn run_cli(
    executable: &Path,
    home: &Path,
    arguments: &[&str],
    wall_timeout: Duration,
) -> io::Result<CommandOutput> {
    let temporary = home.join(".tmp");
    std::fs::create_dir_all(&temporary)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o700))?;
    }
    let mut command = Command::new(executable);
    command
        .args(arguments)
        .env_clear()
        .env("HOME", home)
        .env("TMPDIR", &temporary)
        .env("NO_COLOR", "1")
        .env("TERM", "dumb")
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .current_dir(home)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    copy_proxy_environment(&mut command);
    let mut child = command.spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("Dreamina stdout is unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("Dreamina stderr is unavailable"))?;
    timeout(wall_timeout, async move {
        let (stdout, stderr, status) =
            tokio::try_join!(read_bounded(stdout), read_bounded(stderr), child.wait(),)?;
        Ok(CommandOutput {
            status,
            stdout,
            stderr,
        })
    })
    .await
    .map_err(|_| io::Error::other("Dreamina command timed out"))?
}

async fn read_bounded<R>(reader: R) -> io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    reader
        .take((MAX_COMMAND_OUTPUT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > MAX_COMMAND_OUTPUT_BYTES {
        return Err(io::Error::other("Dreamina command output exceeded limit"));
    }
    Ok(bytes)
}

fn combined_text(output: &CommandOutput) -> String {
    let mut bytes = Vec::with_capacity(output.stdout.len() + output.stderr.len() + 1);
    bytes.extend_from_slice(&output.stdout);
    bytes.push(b'\n');
    bytes.extend_from_slice(&output.stderr);
    strip_ansi(&String::from_utf8_lossy(&bytes))
}

fn parse_credit(output: &str) -> io::Result<DreaminaAccountSnapshot> {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(output.trim()) {
        let user_id = value
            .get("user_id")
            .and_then(json_identifier)
            .filter(|value| !value.is_empty() && value.len() <= 128)
            .ok_or_else(|| io::Error::other("Dreamina user identity is invalid"))?;
        let total_credit = value
            .get("total_credit")
            .and_then(json_nonnegative_i64)
            .ok_or_else(|| io::Error::other("Dreamina credit balance is invalid"))?;
        return Ok(DreaminaAccountSnapshot {
            user_id,
            vip_level: value
                .get("vip_level")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty() && value.chars().count() <= 128)
                .map(str::to_owned),
            total_credit,
            cli_permission: value
                .get("has_cli_permission")
                .and_then(serde_json::Value::as_bool)
                .map_or(DreaminaCliPermission::Unknown, |granted| {
                    if granted {
                        DreaminaCliPermission::Granted
                    } else {
                        DreaminaCliPermission::Required
                    }
                }),
        });
    }
    let user_id = field_value(output, "user_id")
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .ok_or_else(|| io::Error::other("Dreamina user identity is invalid"))?;
    let total_credit = field_value(output, "total_credit")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .ok_or_else(|| io::Error::other("Dreamina credit balance is invalid"))?;
    Ok(DreaminaAccountSnapshot {
        user_id,
        vip_level: optional_field(output, "vip_level", 128),
        total_credit,
        cli_permission: field_value(output, "has_cli_permission")
            .and_then(|value| value.parse::<bool>().ok())
            .map_or(DreaminaCliPermission::Unknown, |granted| {
                if granted {
                    DreaminaCliPermission::Granted
                } else {
                    DreaminaCliPermission::Required
                }
            }),
    })
}

async fn observe_dreamina_cli_permission(
    executable: &Path,
    home: &Path,
) -> io::Result<DreaminaCliPermission> {
    let output = run_cli(
        executable,
        home,
        &["login", "--headless"],
        LOGIN_START_TIMEOUT,
    )
    .await?;
    Ok(explicit_cli_permission(&combined_text(&output)))
}

fn login_completion_permission(command_succeeded: bool, output: &str) -> DreaminaCliPermission {
    match explicit_cli_permission(output) {
        DreaminaCliPermission::Required => DreaminaCliPermission::Required,
        DreaminaCliPermission::Unknown if command_succeeded => DreaminaCliPermission::Granted,
        permission => permission,
    }
}

fn explicit_cli_permission(output: &str) -> DreaminaCliPermission {
    let normalized = output.to_ascii_lowercase();
    if normalized.contains("没有 dreamina_cli 使用权限")
        || normalized.contains("does not have dreamina_cli permission")
        || normalized.contains("dreamina_cli permission is required")
    {
        return DreaminaCliPermission::Required;
    }
    DreaminaCliPermission::Unknown
}

fn json_identifier(value: &serde_json::Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|value| value.to_string()))
        .or_else(|| {
            value
                .as_i64()
                .filter(|value| *value >= 0)
                .map(|value| value.to_string())
        })
}

fn json_nonnegative_i64(value: &serde_json::Value) -> Option<i64> {
    value.as_i64().filter(|value| *value >= 0).or_else(|| {
        value
            .as_str()?
            .parse::<i64>()
            .ok()
            .filter(|value| *value >= 0)
    })
}

fn authorization_is_invalid(output: &str) -> bool {
    let output = output.to_ascii_lowercase();
    [
        "未检测到有效登录态",
        "请先执行 dreamina login",
        "not logged in",
        "login required",
        "please run dreamina login",
        "invalid_grant",
        "refresh token expired",
    ]
    .iter()
    .any(|marker| output.contains(marker))
}

fn optional_field(output: &str, name: &str, maximum: usize) -> Option<String> {
    field_value(output, name).filter(|value| !value.is_empty() && value.chars().count() <= maximum)
}

fn field_value(output: &str, name: &str) -> Option<String> {
    output.lines().find_map(|line| {
        let line = line.trim();
        let (field, value) = line.split_once(':')?;
        (field.trim() == name)
            .then(|| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    })
}

fn valid_authorization_url(value: &str) -> bool {
    if value.len() > 2_048 || value.bytes().any(|byte| byte.is_ascii_control()) {
        return false;
    }
    let Ok(url) = reqwest::Url::parse(value) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && host != "localhost"
        && !host.ends_with(".local")
        && host.parse::<IpAddr>().is_err()
}

fn valid_short_code(value: &str) -> bool {
    (4..=32).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_device_code(value: &str) -> bool {
    (16..=4_096).contains(&value.len()) && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn strip_ansi(value: &str) -> String {
    let mut clean = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character != '\u{1b}' {
            clean.push(character);
            continue;
        }
        if chars.next() != Some('[') {
            continue;
        }
        for sequence in chars.by_ref() {
            if sequence.is_ascii_alphabetic() {
                break;
            }
        }
    }
    clean
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_official_plain_text_login_challenge() {
        let output = "verification_uri: https://example.com/device\nuser_code: ABCD-1234\ndevice_code: 0123456789abcdef0123456789abcdef\n";
        assert_eq!(
            field_value(output, "verification_uri").as_deref(),
            Some("https://example.com/device")
        );
        assert!(valid_short_code(&field_value(output, "user_code").unwrap()));
        assert!(valid_device_code(
            &field_value(output, "device_code").unwrap()
        ));
    }

    #[test]
    fn parses_credit_snapshot_without_inventing_rate_limit_windows() {
        let account = parse_credit(
            "user_id: 123456\nuser_name: operator\nvip_level: advanced\ntotal_credit: 9876\n",
        )
        .unwrap();
        assert_eq!(account.user_id, "123456");
        assert_eq!(account.vip_level.as_deref(), Some("advanced"));
        assert_eq!(account.total_credit, 9876);
        assert_eq!(account.cli_permission, DreaminaCliPermission::Unknown);
    }

    #[test]
    fn parses_current_json_credit_response() {
        let account = parse_credit(
            r#"{"total_credit":66,"user_id":2744000166772206,"user_name":"","vip_level":""}"#,
        )
        .unwrap();
        assert_eq!(account.user_id, "2744000166772206");
        assert_eq!(account.vip_level, None);
        assert_eq!(account.total_credit, 66);
        assert_eq!(account.cli_permission, DreaminaCliPermission::Unknown);
    }

    #[test]
    fn separates_oauth_success_from_cli_generation_permission() {
        let output = "OAuth 登录成功。\nuser_id: 123\ntotal_credit: 66\n登录成功，但当前账号没有 dreamina_cli 使用权限: 仅限高级或高级以上的会员等级";
        assert_eq!(
            login_completion_permission(true, output),
            DreaminaCliPermission::Required
        );
        assert_eq!(
            login_completion_permission(
                false,
                "已复用当前本地 OAuth 登录态。\nuser_id: 123\ntotal_credit: 66"
            ),
            DreaminaCliPermission::Unknown
        );
        assert_eq!(
            login_completion_permission(
                true,
                "已复用当前本地 OAuth 登录态。\nuser_id: 123\ntotal_credit: 66"
            ),
            DreaminaCliPermission::Granted
        );
        assert_eq!(
            explicit_cli_permission("已复用当前本地 OAuth 登录态。"),
            DreaminaCliPermission::Unknown
        );
    }

    #[test]
    fn classifies_expired_login_without_treating_transient_errors_as_reauthorization() {
        assert!(authorization_is_invalid(
            "未检测到有效登录态，请先执行 dreamina login"
        ));
        assert!(authorization_is_invalid("oauth failed: invalid_grant"));
        assert!(!authorization_is_invalid(
            "request failed: upstream temporarily unavailable"
        ));
    }

    #[test]
    fn rejects_local_or_credential_bearing_authorization_urls() {
        assert!(!valid_authorization_url("http://example.com/device"));
        assert!(!valid_authorization_url("https://localhost/device"));
        assert!(!valid_authorization_url(
            "https://user:secret@example.com/device"
        ));
    }
}
