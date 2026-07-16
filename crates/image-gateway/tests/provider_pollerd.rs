#![cfg(unix)]

use std::process::Command;

#[test]
fn provider_pollerd_is_disabled_without_an_explicit_activation_token() {
    let output = Command::new(env!("CARGO_BIN_EXE_provider-pollerd"))
        .env_clear()
        .output()
        .expect("start provider-pollerd");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("PROVIDER_POLLER_ACTIVATION is required"));
    assert!(!stderr.contains("DATABASE_URL"));
}
