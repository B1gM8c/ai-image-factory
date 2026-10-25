use std::{fs, os::unix::fs::PermissionsExt};

use image_provider_dreamina_cli::DreaminaCliPolicyV1;
use image_provider_test_support::TestPayload;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use super::*;
use crate::providers::dreamina_cli::{DreaminaCliRuntimeBindingV1, DreaminaCliSubmitCodecV1};

const PERMISSION_DENIED: &[u8] =
    "当前账号没有 dreamina_cli 使用权限: current account is not allowed to use dreamina_cli\n"
        .as_bytes();

struct Fixture {
    _root: TempDir,
    codec: DreaminaCliSubmitCodecV1,
}

impl Fixture {
    fn new() -> Self {
        let root = TempDir::new().unwrap();
        let executable = root.path().join("dreamina");
        let bytes = b"#!/bin/sh\nprintf '{}'\n";
        fs::write(&executable, bytes).unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).unwrap();
        let workspace = root.path().join("workspace");
        let home = root.path().join("home");
        for path in [&workspace, &home] {
            fs::create_dir(path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let policy = DreaminaCliPolicyV1::new(
            &executable,
            Sha256::digest(bytes).into(),
            WorkingDirectory::new_private(&workspace).unwrap(),
            WorkingDirectory::new_private(&home).unwrap(),
            Duration::from_secs(30),
            Duration::from_millis(100),
        )
        .unwrap();
        let binding =
            DreaminaCliRuntimeBindingV1::new(Uuid::new_v4(), Uuid::new_v4(), "a".repeat(64))
                .unwrap();
        Self {
            _root: root,
            codec: DreaminaCliSubmitCodecV1::new(policy, binding),
        }
    }
}

fn exited(code: i32) -> GatedCliProcessOutcome {
    GatedCliProcessOutcome::Exited {
        exit_code: Some(code),
        signal: None,
    }
}

#[test]
fn exact_dreamina_permission_diagnostic_keeps_unknown_effect_and_never_retries() {
    let fixture = Fixture::new();
    let code = nonzero_exit_diagnostic(
        &fixture.codec,
        &exited(1),
        &[],
        false,
        PERMISSION_DENIED,
        false,
    )
    .unwrap();
    let failure = unknown_process_failure(code);

    assert_eq!(code, "dreamina_cli_permission_denied");
    assert_eq!(failure.class(), ProviderFailureClass::Ambiguous);
    assert_eq!(failure.effect(), EffectCertainty::UnknownRemoteEffect);
    assert_eq!(failure.retry(), RetryDirective::Never);
    assert!(!failure.to_string().contains("current account"));
}

#[test]
fn permission_diagnostic_requires_exact_complete_evidence() {
    let fixture = Fixture::new();
    for (stdout, stdout_truncated, stderr, stderr_truncated) in [
        (b"accepted".as_slice(), false, PERMISSION_DENIED, false),
        (b"".as_slice(), true, PERMISSION_DENIED, false),
        (b"".as_slice(), false, PERMISSION_DENIED, true),
        (
            b"".as_slice(),
            false,
            b"current account is not allowed to use dreamina_cli\n".as_slice(),
            false,
        ),
        (
            b"".as_slice(),
            false,
            b"permission denied: https://example.test/private?token=fixture".as_slice(),
            false,
        ),
        (
            b"".as_slice(),
            false,
            b"prompt: current account is not allowed to use dreamina_cli".as_slice(),
            false,
        ),
    ] {
        assert_eq!(
            nonzero_exit_diagnostic(
                &fixture.codec,
                &exited(1),
                stdout,
                stdout_truncated,
                stderr,
                stderr_truncated,
            ),
            None
        );
    }
    for stderr in [
        [PERMISSION_DENIED, b"extra output"].concat(),
        PERMISSION_DENIED[..PERMISSION_DENIED.len() - 1].to_vec(),
        [b" ", PERMISSION_DENIED].concat(),
    ] {
        assert_eq!(fixture.codec.nonzero_exit_diagnostic(1, &stderr), None);
    }
}

#[test]
fn timeout_signal_and_other_exit_outcomes_never_use_permission_diagnostic() {
    let fixture = Fixture::new();
    for outcome in [
        exited(0),
        exited(2),
        GatedCliProcessOutcome::Exited {
            exit_code: None,
            signal: None,
        },
        GatedCliProcessOutcome::Exited {
            exit_code: Some(1),
            signal: Some(9),
        },
        GatedCliProcessOutcome::TimedOut,
        GatedCliProcessOutcome::AbsoluteDeadlineElapsed,
        GatedCliProcessOutcome::GateFailed {
            error_code: "gate-failed".to_owned(),
        },
        GatedCliProcessOutcome::ResidualProcessGroup,
    ] {
        assert_eq!(
            nonzero_exit_diagnostic(
                &fixture.codec,
                &outcome,
                &[],
                false,
                PERMISSION_DENIED,
                false
            ),
            None
        );
    }
}

struct DefaultCodec;

impl GatedCliSubmitCodec for DefaultCodec {
    type Payload = TestPayload;

    fn provider_id(&self) -> &'static str {
        "provider-test"
    }

    fn command(
        &self,
        _intent: &ProviderSubmitIntent,
        _context: &ProviderExecutionContext,
        _command: &image_provider_sdk::SingleOutputCommand<Self::Payload>,
        _workspace: &WorkingDirectory,
    ) -> Result<GatedCliCommand, ProviderFailure> {
        unreachable!("diagnostic tests do not create CLI commands")
    }

    fn decode_receipt(
        &self,
        _intent: &ProviderSubmitIntent,
        _command: &image_provider_sdk::SingleOutputCommand<Self::Payload>,
        _stdout: &[u8],
    ) -> Result<PendingOperation, ProviderFailure> {
        unreachable!("nonzero exits do not decode success receipts")
    }
}

#[test]
fn another_provider_keeps_the_default_diagnostic_behavior() {
    assert_eq!(
        nonzero_exit_diagnostic(
            &DefaultCodec,
            &exited(1),
            &[],
            false,
            PERMISSION_DENIED,
            false
        ),
        None
    );
}
