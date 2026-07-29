use std::{path::Path, process::Stdio, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, ChildStdout, Command},
    time::timeout,
};

use super::CodexLoginMethod;

const MAX_LOGIN_OUTPUT_BYTES: usize = 64 * 1024;
const CHALLENGE_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct GrokLoginChallenge {
    pub authorization_url: String,
    pub user_code: Option<String>,
}

pub(super) struct GrokLoginProcess {
    child: Child,
    output: BufReader<ChildStdout>,
}

impl GrokLoginProcess {
    pub async fn start(
        executable: &Path,
        home: &Path,
        method: CodexLoginMethod,
    ) -> std::io::Result<(Self, GrokLoginChallenge)> {
        let mut command = platform_command(executable, method)?;
        command
            .env_clear()
            .env("HOME", home)
            .env("GROK_HOME", home)
            .env("GROK_DISABLE_AUTOUPDATER", "1")
            .env("BROWSER", "/usr/bin/false")
            .env("TERM", "xterm-256color")
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        copy_proxy_environment(&mut command);
        let mut child = command.spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| std::io::Error::other("Grok login output is unavailable"))?;
        let mut process = Self {
            child,
            output: BufReader::new(stdout),
        };
        let challenge = timeout(CHALLENGE_TIMEOUT, process.read_challenge(method))
            .await
            .map_err(|_| std::io::Error::other("Grok login challenge timed out"))??;
        Ok((process, challenge))
    }

    pub async fn wait(mut self) -> std::io::Result<bool> {
        let mut total = 0_usize;
        let mut line = String::new();
        loop {
            line.clear();
            let read = self.output.read_line(&mut line).await?;
            if read == 0 {
                break;
            }
            total = total.saturating_add(read);
            if total > MAX_LOGIN_OUTPUT_BYTES {
                let _ = self.child.kill().await;
                return Err(std::io::Error::other("Grok login output exceeded limit"));
            }
        }
        Ok(self.child.wait().await?.success())
    }

    async fn read_challenge(
        &mut self,
        method: CodexLoginMethod,
    ) -> std::io::Result<GrokLoginChallenge> {
        let mut output = String::new();
        loop {
            let mut line = String::new();
            let read = self.output.read_line(&mut line).await?;
            if read == 0 {
                return Err(std::io::Error::other(
                    "Grok login exited before returning a challenge",
                ));
            }
            output.push_str(&strip_ansi(&line));
            if output.len() > MAX_LOGIN_OUTPUT_BYTES {
                return Err(std::io::Error::other("Grok login output exceeded limit"));
            }
            let Some(url) = first_https_url(&output) else {
                continue;
            };
            if !allowed_login_url(&url, method) {
                return Err(std::io::Error::other("Grok login returned an invalid URL"));
            }
            let user_code = if method == CodexLoginMethod::DeviceCode {
                query_value(&url, "user_code")
            } else {
                None
            };
            if method == CodexLoginMethod::DeviceCode && user_code.is_none() {
                continue;
            }
            return Ok(GrokLoginChallenge {
                authorization_url: url,
                user_code,
            });
        }
    }
}

#[cfg(target_os = "macos")]
fn platform_command(executable: &Path, method: CodexLoginMethod) -> std::io::Result<Command> {
    let mut command = Command::new("/usr/bin/script");
    command
        .arg("-q")
        .arg("/dev/null")
        .arg(executable)
        .arg("login")
        .arg(method_flag(method));
    Ok(command)
}

#[cfg(not(target_os = "macos"))]
fn platform_command(executable: &Path, method: CodexLoginMethod) -> std::io::Result<Command> {
    let mut command = Command::new(executable);
    command.arg("login").arg(method_flag(method));
    Ok(command)
}

fn method_flag(method: CodexLoginMethod) -> &'static str {
    match method {
        CodexLoginMethod::BrowserOauth => "--oauth",
        CodexLoginMethod::DeviceCode => "--device-auth",
    }
}

pub(super) fn copy_proxy_environment(command: &mut Command) {
    for name in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "no_proxy",
    ] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
}

pub(super) async fn refresh_grok_auth(executable: &Path, home: &Path) -> std::io::Result<()> {
    if !executable.is_absolute() || !home.is_absolute() {
        return Err(std::io::Error::other(
            "Grok refresh configuration is invalid",
        ));
    }
    let mut command = Command::new(executable);
    command
        .arg("models")
        .env_clear()
        .env("HOME", home)
        .env("GROK_HOME", home)
        .env("GROK_DISABLE_AUTOUPDATER", "1")
        .env("TERM", "dumb")
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .current_dir(home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    copy_proxy_environment(&mut command);
    let status = timeout(Duration::from_secs(45), command.status())
        .await
        .map_err(|_| std::io::Error::other("Grok credential refresh timed out"))??;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other("Grok credential refresh failed"))
    }
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

fn first_https_url(value: &str) -> Option<String> {
    let start = value.find("https://")?;
    let tail = &value[start..];
    let end = tail
        .find(|character: char| character.is_whitespace())
        .unwrap_or(tail.len());
    Some(tail[..end].trim_end_matches(['\r', '\n']).to_owned())
}

fn allowed_login_url(url: &str, method: CodexLoginMethod) -> bool {
    match method {
        CodexLoginMethod::BrowserOauth => url.starts_with("https://auth.x.ai/oauth2/authorize?"),
        CodexLoginMethod::DeviceCode => url.starts_with("https://accounts.x.ai/oauth2/device?"),
    }
}

fn query_value(url: &str, key: &str) -> Option<String> {
    let query = url.split_once('?')?.1;
    query.split('&').find_map(|part| {
        let (name, value) = part.split_once('=')?;
        (name == key && !value.is_empty()).then(|| value.to_owned())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bounded_device_challenge() {
        let output =
            "\u{1b}[90mnotice\u{1b}[0m\nhttps://accounts.x.ai/oauth2/device?user_code=ABCD-1234\n";
        let clean = strip_ansi(output);
        let url = first_https_url(&clean).unwrap();
        assert!(allowed_login_url(&url, CodexLoginMethod::DeviceCode));
        assert_eq!(query_value(&url, "user_code").as_deref(), Some("ABCD-1234"));
    }

    #[test]
    fn rejects_untrusted_oauth_host() {
        assert!(!allowed_login_url(
            "https://example.test/oauth2/authorize?x=1",
            CodexLoginMethod::BrowserOauth,
        ));
    }
}
