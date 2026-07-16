use uuid::Uuid;

use super::*;

#[test]
fn remote_task_profile_freezes_scope_and_capacity_without_debugging_credentials() {
    let profile = ProviderRuntimeProfile::new(profile()).unwrap();

    assert_eq!(
        profile.claim_scope(),
        ProviderTaskClaimScope {
            provider_id: "provider-test".to_owned(),
            provider_account_id: Uuid::from_u128(3),
        }
    );
    assert_eq!(profile.max_in_flight(), 4);
    assert_eq!(profile.credential_ref(), "vault.provider-test.1");
    assert_eq!(profile.credential_auth_sha256(), "c".repeat(64));
    let debug = format!("{profile:?}");
    assert!(!debug.contains("vault.provider-test.1"));
    assert!(!debug.contains(&"c".repeat(64)));
}

#[test]
fn inline_and_oversized_profiles_are_rejected_before_daemon_construction() {
    let mut inline = profile();
    inline.completion_mode = "inline".to_owned();
    assert!(ProviderRuntimeProfile::new(inline).is_err());

    let mut oversized = profile();
    oversized.max_concurrency =
        i32::try_from(MAX_PROVIDER_RUNTIME_LANES + 1).expect("test lane count fits i32");
    assert!(ProviderRuntimeProfile::new(oversized).is_err());
}

#[test]
fn malformed_frozen_identity_is_rejected() {
    let mut malformed = profile();
    malformed.credential_auth_sha256 = "not-a-digest".to_owned();
    assert!(ProviderRuntimeProfile::new(malformed).is_err());
}

fn profile() -> ExecutorExecutionProfile {
    ExecutorExecutionProfile {
        execution_profile_id: Uuid::from_u128(1),
        profile_key: "provider-test-profile".to_owned(),
        provider_id: "provider-test".to_owned(),
        command_schema: "provider-test.command.v1".to_owned(),
        operation_id: "images.generations".to_owned(),
        operation_descriptor_revision: "provider-test/images.generations/v1".to_owned(),
        operation_descriptor_sha256_v1: "b".repeat(64),
        completion_mode: "remote_task".to_owned(),
        idempotency_mode: "submission_bound".to_owned(),
        adapter_revision: "provider-test-adapter-v1".to_owned(),
        credential_pool_id: Uuid::from_u128(2),
        provider_account_id: Uuid::from_u128(3),
        credential_ref: "vault.provider-test.1".to_owned(),
        credential_revision: 1,
        credential_auth_sha256: "c".repeat(64),
        resource_policy_id: Uuid::from_u128(4),
        resource_policy_revision: 1,
        max_concurrency: 4,
    }
}
