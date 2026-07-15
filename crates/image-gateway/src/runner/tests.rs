use std::{
    fs,
    path::Path,
    sync::{Arc, Barrier, mpsc},
    thread,
    time::Duration,
};

use tempfile::TempDir;
use uuid::Uuid;

use super::{
    FilesystemRunnerJournal, LaunchDecision, RunnerJournalError, RunnerJournalObservation,
};
use crate::executor::{ExecutorResultManifest, ExecutorSubmissionLease, RunnerOutcome};

const SECRET: &str = "sk-example-do-not-persist";

#[test]
fn concurrent_journal_create_prepares_one_identity() {
    let parent = TempDir::new().unwrap();
    let root = parent.path().join("journal");
    let lease = Arc::new(lease());
    let barrier = Arc::new(Barrier::new(8));
    let handles = (0..8)
        .map(|_| {
            let root = root.clone();
            let lease = Arc::clone(&lease);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                FilesystemRunnerJournal::new(root)
                    .unwrap()
                    .start_or_attach(&lease)
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        assert_eq!(
            handle.join().unwrap().unwrap(),
            RunnerJournalObservation::Prepared
        );
    }
}

#[test]
fn concurrent_commit_launch_has_exactly_one_winner() {
    let (_temp, journal) = journal();
    let lease = Arc::new(lease());
    journal.start_or_attach(&lease).unwrap();
    let journal = Arc::new(journal);
    let barrier = Arc::new(Barrier::new(12));
    let handles = (0..12)
        .map(|_| {
            let journal = Arc::clone(&journal);
            let lease = Arc::clone(&lease);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                journal.commit_launch(&lease).unwrap()
            })
        })
        .collect::<Vec<_>>();
    let decisions = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        decisions
            .iter()
            .filter(|value| **value == LaunchDecision::LaunchOnce)
            .count(),
        1
    );
    assert_eq!(
        decisions
            .iter()
            .filter(|value| **value == LaunchDecision::Attach)
            .count(),
        11
    );
}

#[test]
fn reopening_attaches_and_never_launches_again() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("journal");
    let lease = lease();
    let first = FilesystemRunnerJournal::new(&root).unwrap();
    first.start_or_attach(&lease).unwrap();
    assert_eq!(
        first.commit_launch(&lease).unwrap(),
        LaunchDecision::LaunchOnce
    );
    drop(first);

    let reopened = FilesystemRunnerJournal::new(&root).unwrap();
    assert_eq!(
        reopened.start_or_attach(&lease).unwrap(),
        RunnerJournalObservation::LaunchCommitted
    );
    assert_eq!(
        reopened.commit_launch(&lease).unwrap(),
        LaunchDecision::Attach
    );
}

#[test]
fn same_execution_with_different_command_hash_conflicts() {
    let (_temp, journal) = journal();
    let lease = lease();
    journal.start_or_attach(&lease).unwrap();
    let changed = ExecutorSubmissionLease {
        command_hash: "b".repeat(64),
        ..lease
    };

    assert_eq!(
        journal.start_or_attach(&changed),
        Err(RunnerJournalError::Conflict)
    );
    assert_eq!(
        journal.commit_launch(&changed),
        Err(RunnerJournalError::Conflict)
    );
}

#[test]
fn same_execution_with_different_profile_or_adapter_conflicts() {
    let (_temp, journal) = journal();
    let lease = lease();
    journal.start_or_attach(&lease).unwrap();
    let changed_profile = ExecutorSubmissionLease {
        execution_profile_id: Uuid::new_v4(),
        ..lease.clone()
    };
    let changed_adapter = ExecutorSubmissionLease {
        adapter_revision: "adapter-v2".to_string(),
        ..lease
    };

    assert_eq!(
        journal.start_or_attach(&changed_profile),
        Err(RunnerJournalError::Conflict)
    );
    assert_eq!(
        journal.start_or_attach(&changed_adapter),
        Err(RunnerJournalError::Conflict)
    );
}

#[test]
fn terminal_replay_is_idempotent_but_different_value_conflicts() {
    let (_temp, journal) = journal();
    let lease = lease();
    journal.start_or_attach(&lease).unwrap();
    journal.commit_launch(&lease).unwrap();
    let outcome = success();

    journal.publish_terminal(&lease, &outcome).unwrap();
    journal.publish_terminal(&lease, &outcome).unwrap();
    assert_eq!(
        journal.start_or_attach(&lease).unwrap(),
        RunnerJournalObservation::Terminal(outcome)
    );
    assert_eq!(
        journal.publish_terminal(
            &lease,
            &RunnerOutcome::Failed {
                error_code: "provider_failed".to_string(),
            },
        ),
        Err(RunnerJournalError::Conflict)
    );
}

#[test]
fn corrupt_or_unknown_marker_fails_closed() {
    let (temp, journal) = journal();
    let lease = lease();
    journal.start_or_attach(&lease).unwrap();
    fs::write(
        execution_dir(temp.path(), &lease).join("launch.json"),
        br#"{"owner":"owner","epoch":7,"unexpected":true}"#,
    )
    .unwrap();

    assert_eq!(
        journal.start_or_attach(&lease),
        Err(RunnerJournalError::Integrity)
    );
    assert_eq!(
        journal.commit_launch(&lease),
        Err(RunnerJournalError::Integrity)
    );
}

#[cfg(unix)]
#[test]
fn symlink_root_and_execution_entry_are_rejected() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let actual = temp.path().join("actual");
    fs::create_dir(&actual).unwrap();
    let linked_root = temp.path().join("linked");
    symlink(&actual, &linked_root).unwrap();
    assert!(matches!(
        FilesystemRunnerJournal::new(&linked_root),
        Err(RunnerJournalError::InvalidInput | RunnerJournalError::Integrity)
    ));

    let root = temp.path().join("journal");
    let journal = FilesystemRunnerJournal::new(&root).unwrap();
    let lease = lease();
    symlink(&actual, execution_dir(temp.path(), &lease)).unwrap();
    assert_eq!(
        journal.start_or_attach(&lease),
        Err(RunnerJournalError::Integrity)
    );
}

#[test]
fn journal_never_persists_tenant_or_example_secret() {
    let (temp, journal) = journal();
    let lease = lease();
    journal.start_or_attach(&lease).unwrap();
    journal.commit_launch(&lease).unwrap();
    journal
        .publish_terminal(
            &lease,
            &RunnerOutcome::Uncertain {
                error_code: "runner_lost".to_string(),
            },
        )
        .unwrap();

    let text = read_tree(temp.path());
    assert!(!text.contains("tenant-super-secret"));
    assert!(!text.contains(SECRET));
    assert!(!text.contains("executor_lease_expires_at_ms"));
}

#[test]
fn terminal_requires_a_launch_marker() {
    let (_temp, journal) = journal();
    let lease = lease();
    journal.start_or_attach(&lease).unwrap();

    assert_eq!(
        journal.publish_terminal(&lease, &success()),
        Err(RunnerJournalError::Integrity)
    );
}

#[cfg(unix)]
#[test]
fn terminal_marker_without_launch_is_integrity() {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};

    let (temp, journal) = journal();
    let lease = lease();
    journal.start_or_attach(&lease).unwrap();
    let path = execution_dir(temp.path(), &lease).join("terminal.json");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    file.write_all(br#"{"state":"failed","error_code":"provider_failed"}"#)
        .unwrap();
    file.sync_all().unwrap();

    assert_eq!(
        journal.start_or_attach(&lease),
        Err(RunnerJournalError::Integrity)
    );
}

#[cfg(unix)]
#[test]
fn marker_permissions_and_hardlinks_fail_integrity_validation() {
    use std::os::unix::fs::PermissionsExt;

    let (permission_temp, permission_journal) = journal();
    let permission_lease = lease();
    permission_journal
        .start_or_attach(&permission_lease)
        .unwrap();
    let permission_spec =
        execution_dir(permission_temp.path(), &permission_lease).join("spec.json");
    fs::set_permissions(&permission_spec, fs::Permissions::from_mode(0o640)).unwrap();
    assert_eq!(
        permission_journal.start_or_attach(&permission_lease),
        Err(RunnerJournalError::Integrity)
    );

    let (link_temp, link_journal) = journal();
    let link_lease = lease();
    link_journal.start_or_attach(&link_lease).unwrap();
    let link_spec = execution_dir(link_temp.path(), &link_lease).join("spec.json");
    fs::hard_link(&link_spec, link_temp.path().join("extra-link")).unwrap();
    assert_eq!(
        link_journal.start_or_attach(&link_lease),
        Err(RunnerJournalError::Integrity)
    );
}

#[cfg(unix)]
#[test]
fn preexisting_wide_directories_are_rejected_without_permission_repair() {
    use std::os::unix::fs::PermissionsExt;

    let root_temp = TempDir::new().unwrap();
    let root = root_temp.path().join("journal");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o750)).unwrap();
    assert!(matches!(
        FilesystemRunnerJournal::new(&root),
        Err(RunnerJournalError::InvalidInput)
    ));
    assert_eq!(
        fs::metadata(&root).unwrap().permissions().mode() & 0o777,
        0o750
    );

    let execution_temp = TempDir::new().unwrap();
    let execution_root = execution_temp.path().join("journal");
    let journal = FilesystemRunnerJournal::new(&execution_root).unwrap();
    let lease = lease();
    let execution = execution_dir(execution_temp.path(), &lease);
    fs::create_dir(&execution).unwrap();
    fs::set_permissions(&execution, fs::Permissions::from_mode(0o750)).unwrap();
    assert_eq!(
        journal.start_or_attach(&lease),
        Err(RunnerJournalError::Integrity)
    );
    assert_eq!(
        fs::metadata(&execution).unwrap().permissions().mode() & 0o777,
        0o750
    );
}

#[test]
fn terminal_manifest_persists_only_valid_authority_references_and_error_codes() {
    let (_temp, journal) = journal();
    let lease = lease();
    journal.start_or_attach(&lease).unwrap();
    journal.commit_launch(&lease).unwrap();

    let RunnerOutcome::Succeeded(base) = success() else {
        unreachable!();
    };
    let nil_manifest = RunnerOutcome::Succeeded(ExecutorResultManifest {
        manifest_id: Uuid::nil(),
        ..base.clone()
    });
    assert_eq!(
        journal.publish_terminal(&lease, &nil_manifest),
        Err(RunnerJournalError::InvalidInput)
    );
    let aliased_identity = RunnerOutcome::Succeeded(ExecutorResultManifest {
        manifest_id: base.artifact_authority_id,
        artifact_authority_id: base.artifact_authority_id,
    });
    assert_eq!(
        journal.publish_terminal(&lease, &aliased_identity),
        Err(RunnerJournalError::InvalidInput)
    );
    assert_eq!(
        journal.publish_terminal(
            &lease,
            &RunnerOutcome::Failed {
                error_code: "not allowed".to_string(),
            }
        ),
        Err(RunnerJournalError::InvalidInput)
    );
}

#[cfg(unix)]
#[test]
fn fifo_marker_is_rejected_without_blocking() {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    let (temp, journal) = journal();
    let lease = lease();
    journal.start_or_attach(&lease).unwrap();
    let fifo = execution_dir(temp.path(), &lease).join("launch.json");
    let fifo = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    // SAFETY: fifo is a valid NUL-terminated path and mkfifo does not retain it.
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || sender.send(journal.start_or_attach(&lease)).unwrap());

    assert_eq!(
        receiver.recv_timeout(Duration::from_millis(500)).unwrap(),
        Err(RunnerJournalError::Integrity)
    );
}

#[test]
fn launch_marker_fences_owner_and_epoch() {
    let (_temp, journal) = journal();
    let lease = lease();
    journal.start_or_attach(&lease).unwrap();
    journal.commit_launch(&lease).unwrap();
    let owner_changed = ExecutorSubmissionLease {
        executor_owner: "replacement-owner".to_string(),
        ..lease.clone()
    };
    let epoch_changed = ExecutorSubmissionLease {
        executor_lease_epoch: lease.executor_lease_epoch + 1,
        ..lease
    };

    assert_eq!(
        journal.start_or_attach(&owner_changed),
        Err(RunnerJournalError::Conflict)
    );
    assert_eq!(
        journal.commit_launch(&owner_changed),
        Err(RunnerJournalError::Conflict)
    );
    assert_eq!(
        journal.start_or_attach(&epoch_changed),
        Err(RunnerJournalError::Conflict)
    );
    assert_eq!(
        journal.commit_launch(&epoch_changed),
        Err(RunnerJournalError::Conflict)
    );
}

#[test]
fn journal_stays_bound_to_open_root_after_path_replacement() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("journal");
    let journal = FilesystemRunnerJournal::new(&root).unwrap();
    let original = temp.path().join("original-root");
    fs::rename(&root, &original).unwrap();
    fs::create_dir(&root).unwrap();
    let lease = lease();

    assert_eq!(
        journal.start_or_attach(&lease).unwrap(),
        RunnerJournalObservation::Prepared
    );
    assert!(fs::read_dir(&root).unwrap().next().is_none());
    assert!(
        original
            .join(lease.executor_execution_id.simple().to_string())
            .join("spec.json")
            .is_file()
    );
}

fn journal() -> (TempDir, FilesystemRunnerJournal) {
    let temp = TempDir::new().unwrap();
    let journal = FilesystemRunnerJournal::new(temp.path().join("journal")).unwrap();
    (temp, journal)
}

fn execution_dir(parent: &Path, lease: &ExecutorSubmissionLease) -> std::path::PathBuf {
    parent
        .join("journal")
        .join(lease.executor_execution_id.simple().to_string())
}

fn lease() -> ExecutorSubmissionLease {
    ExecutorSubmissionLease {
        submission_id: Uuid::new_v4(),
        executor_execution_id: Uuid::new_v4(),
        output_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        tenant_id: format!("tenant-super-secret-{SECRET}"),
        provider_id: "provider-test".to_string(),
        model: "model-test".to_string(),
        work_item_id: Uuid::new_v4(),
        output_index: 0,
        command_schema: "provider-command-v1".to_string(),
        command_hash: "a".repeat(64),
        execution_profile_id: Uuid::new_v4(),
        adapter_revision: "adapter-v1".to_string(),
        executor_owner: "owner-7".to_string(),
        executor_lease_epoch: 7,
        executor_lease_expires_at_ms: i64::MAX,
    }
}

fn success() -> RunnerOutcome {
    RunnerOutcome::Succeeded(ExecutorResultManifest::new(Uuid::new_v4(), Uuid::new_v4()).unwrap())
}

fn read_tree(path: &Path) -> String {
    let mut output = String::new();
    for entry in fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            output.push_str(&read_tree(&path));
        } else {
            output.push_str(&String::from_utf8_lossy(&fs::read(path).unwrap()));
        }
    }
    output
}
