#![cfg(unix)]

use std::process::Command;

#[test]
fn provider_submitd_is_disabled_without_an_explicit_activation_token() {
    let output = Command::new(env!("CARGO_BIN_EXE_provider-submitd"))
        .env_clear()
        .output()
        .expect("start provider-submitd");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("PROVIDER_SUBMITTER_ACTIVATION is required"));
    assert!(!stderr.contains("DATABASE_URL"));
}

#[test]
fn submit_budget_mismatch_is_rejected_before_preparing_a_home_or_opening_a_database() {
    let root = tempfile::tempdir().unwrap();
    let account = root.path().join("account");
    let output = Command::new(env!("CARGO_BIN_EXE_provider-submitd"))
        .env_clear()
        .envs([
            ("PROVIDER_SUBMITTER_ACTIVATION", "dreamina-remote-submit-v1"),
            ("PROVIDER_SUBMITTER_PROFILE_KEY", "synthetic-video-profile"),
            (
                "PROVIDER_SUBMITTER_CREDENTIAL_POOL_ID",
                "00000000-0000-0000-0000-000000000001",
            ),
            (
                "PROVIDER_SUBMITTER_ACCOUNT_ID",
                "00000000-0000-0000-0000-000000000002",
            ),
            ("PROVIDER_SUBMITTER_CREDENTIAL_REF", "synthetic-credential"),
            ("PROVIDER_SUBMITTER_CREDENTIAL_REVISION", "1"),
            (
                "PROVIDER_SUBMITTER_EXECUTABLE",
                "/nonexistent/synthetic-cli",
            ),
            ("PROVIDER_SUBMITTER_RUNNER", "/nonexistent/synthetic-runner"),
            ("PROVIDER_SUBMITTER_PROVIDER_TIMEOUT_MS", "600000"),
            ("PROVIDER_SUBMITTER_CLI_WALL_TIMEOUT_MS", "600001"),
        ])
        .env("PROVIDER_SUBMITTER_CREDENTIAL_AUTH_SHA256", "a".repeat(64))
        .env("PROVIDER_SUBMITTER_EXECUTABLE_SHA256", "b".repeat(64))
        .env("PROVIDER_SUBMITTER_RUNNER_SHA256", "c".repeat(64))
        .env("PROVIDER_SUBMITTER_ACCOUNT_HOME", &account)
        .env(
            "PROVIDER_SUBMITTER_WORKSPACE_ROOT",
            root.path().join("workspace"),
        )
        .env(
            "PROVIDER_SUBMITTER_JOURNAL_ROOT",
            root.path().join("journal"),
        )
        .output()
        .expect("start provider-submitd with synthetic configuration");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains(
        "PROVIDER_SUBMITTER_CLI_WALL_TIMEOUT_MS must not exceed PROVIDER_SUBMITTER_PROVIDER_TIMEOUT_MS"
    ));
    assert!(!stderr.contains("DATABASE_URL"));
    assert!(!account.exists());
}
