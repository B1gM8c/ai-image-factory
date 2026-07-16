use std::{fs, os::unix::fs::PermissionsExt};

use crate::executor::ExecutorExecutionProfile;

use super::*;

#[test]
fn binds_only_the_exact_profile_identity_and_redacts_sensitive_fields() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("account-home");
    fs::create_dir(&home).unwrap();
    fs::set_permissions(&home, fs::Permissions::from_mode(0o700)).unwrap();
    let profile = ProviderRuntimeProfile::new(profile_fixture()).unwrap();
    let capability = ProviderAccountHomeCapability::new(
        profile.provider_id(),
        profile.credential_pool_id(),
        profile.provider_account_id(),
        profile.credential_ref(),
        profile.credential_revision(),
        profile.credential_auth_sha256(),
        &home,
    )
    .unwrap();

    let canonical_home = fs::canonicalize(&home).unwrap();
    assert_eq!(capability.bind(&profile).unwrap().path(), canonical_home);
    let debug = format!("{capability:?}");
    assert!(debug.contains("[redacted]"));
    assert!(!debug.contains(profile.credential_ref()));
    assert!(!debug.contains(profile.credential_auth_sha256()));
    assert!(!debug.contains(canonical_home.to_str().unwrap()));

    let mut changed = profile_fixture();
    changed.credential_revision += 1;
    let changed = ProviderRuntimeProfile::new(changed).unwrap();
    assert!(matches!(
        capability.bind(&changed),
        Err(ProviderAccountHomeCapabilityError::ProfileMismatch)
    ));
}

#[test]
fn rejects_non_private_account_home_before_binding() {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o755)).unwrap();
    let profile = ProviderRuntimeProfile::new(profile_fixture()).unwrap();

    assert!(matches!(
        ProviderAccountHomeCapability::new(
            profile.provider_id(),
            profile.credential_pool_id(),
            profile.provider_account_id(),
            profile.credential_ref(),
            profile.credential_revision(),
            profile.credential_auth_sha256(),
            root.path(),
        ),
        Err(ProviderAccountHomeCapabilityError::Directory(
            CommandSpecError::InvalidPrivateWorkingDirectory
        ))
    ));
}

fn profile_fixture() -> ExecutorExecutionProfile {
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
