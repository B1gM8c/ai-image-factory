use std::{cell::RefCell, os::unix::fs::PermissionsExt, rc::Rc};

use super::*;

type ReadHook = Box<dyn FnMut(&'static str)>;
thread_local! {
    static AFTER_READ: RefCell<Option<ReadHook>> = RefCell::new(None);
}

pub(super) fn after_read(name: &'static str) {
    AFTER_READ.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(name);
        }
    });
}

fn with_after_read<T>(hook: impl FnMut(&'static str) + 'static, run: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            AFTER_READ.with(|hook| *hook.borrow_mut() = None);
        }
    }
    AFTER_READ.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
    let _reset = Reset;
    run()
}

struct Fixture {
    _temp: tempfile::TempDir,
    submission: GatedCliSubmission,
    binding: GatedCliBinding,
    helper: DiskHelperIdentity,
    child: DiskChildIdentity,
    release: DiskRelease,
    exec_start: DiskExecStart,
    terminal: DiskTerminal,
    lock: RefCell<Option<HelperLock>>,
}

impl Fixture {
    fn new(helper_alive: bool) -> Rc<Self> {
        let temp = tempfile::tempdir().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let submission =
            GatedCliSubmission::new(temp.path().join("journal"), Uuid::new_v4()).unwrap();
        let binding = GatedCliBinding::new(
            sha256(b"observation-binding"),
            Uuid::new_v4(),
            unix_time_ms().unwrap() + 60_000,
        )
        .unwrap();
        let command = GatedCliCommand::new(
            "/bin/sh",
            sha256(&fs::read("/bin/sh").unwrap()),
            temp.path(),
            vec!["-c".to_owned(), "printf receipt".to_owned()],
            BTreeMap::new(),
            Vec::new(),
            Duration::from_secs(1),
            Duration::from_millis(100),
        )
        .unwrap();
        submission.prepare(&binding, &command).unwrap();
        let request = submission.read_request(&binding).unwrap();
        let lock = submission.acquire_helper_lock().unwrap();
        let mut helper = DiskHelperIdentity::capture(&lock).unwrap();
        if !helper_alive {
            helper.start_token = "already-exited-helper".to_owned();
        }
        let child = DiskChildIdentity {
            pid: helper.pid + 1,
            start_token: "already-exited-child".to_owned(),
            boot_token: helper.boot_token.clone(),
            process_group_id: helper.pid + 1,
            helper_nonce: helper.helper_nonce,
            child_nonce: Uuid::new_v4(),
        };
        let release = DiskRelease::new(&request, &helper, &child);
        let exec_start = DiskExecStart::new(&request, &helper, &child);
        let terminal = DiskTerminal::new(
            &request,
            &helper,
            &child,
            true,
            true,
            DiskOutcome::Exited {
                exit_code: Some(0),
                signal: None,
            },
            CapturedStream {
                bytes: b"receipt".to_vec(),
                truncated: false,
            },
            CapturedStream {
                bytes: Vec::new(),
                truncated: false,
            },
        );
        Rc::new(Self {
            _temp: temp,
            submission,
            binding,
            helper,
            child,
            release,
            exec_start,
            terminal,
            lock: RefCell::new(Some(lock)),
        })
    }

    fn publish_through(&self, last: &'static str) {
        for name in [
            HELPER_FILE,
            CHILD_FILE,
            RELEASE_FILE,
            EXEC_STARTED_FILE,
            TERMINAL_FILE,
        ] {
            match name {
                HELPER_FILE => self.submission.publish_helper(&self.helper).unwrap(),
                CHILD_FILE => self.submission.publish_child(&self.child).unwrap(),
                RELEASE_FILE => publish_or_compare(
                    &self.submission.directory,
                    RELEASE_FILE,
                    &serialize_bounded(&self.release, MAX_IDENTITY_BYTES).unwrap(),
                    MAX_IDENTITY_BYTES,
                )
                .unwrap(),
                EXEC_STARTED_FILE => self
                    .submission
                    .publish_exec_start(&self.exec_start)
                    .unwrap(),
                TERMINAL_FILE => self.submission.publish_terminal(&self.terminal).unwrap(),
                _ => unreachable!(),
            }
            if name == last {
                return;
            }
        }
        panic!("unknown marker");
    }

    fn observe(&self) -> Result<GatedCliObservation, GatedCliProcessError> {
        self.submission.observe(&self.binding)
    }
}

fn publication_after_read_is_observable(
    previous: Option<&'static str>,
    boundary: &'static str,
    next: &'static str,
) {
    let fixture = Fixture::new(true);
    if let Some(previous) = previous {
        fixture.publish_through(previous);
    }
    let writer = Rc::clone(&fixture);
    let mut published = false;
    let result = with_after_read(
        move |name| {
            if name == boundary && !published {
                published = true;
                writer.publish_through(next);
            }
        },
        || fixture.observe(),
    );
    assert!(
        result.is_ok(),
        "legal publication after {boundary}: {result:?}"
    );
    fixture.publish_through(TERMINAL_FILE);
    assert!(matches!(
        fixture.observe(),
        Ok(GatedCliObservation::Terminal(_))
    ));
}

#[test]
fn child_published_after_missing_helper_read_is_not_corruption() {
    publication_after_read_is_observable(None, HELPER_FILE, CHILD_FILE);
}

#[test]
fn release_published_after_missing_child_read_is_not_corruption() {
    publication_after_read_is_observable(Some(HELPER_FILE), CHILD_FILE, RELEASE_FILE);
}

#[test]
fn exec_published_after_missing_release_read_is_not_corruption() {
    publication_after_read_is_observable(Some(CHILD_FILE), RELEASE_FILE, EXEC_STARTED_FILE);
}

#[test]
fn terminal_published_after_missing_exec_read_is_not_corruption() {
    publication_after_read_is_observable(Some(RELEASE_FILE), EXEC_STARTED_FILE, TERMINAL_FILE);
}

fn terminal_published_before_liveness_check(helper_alive: bool) {
    let fixture = Fixture::new(helper_alive);
    fixture.publish_through(RELEASE_FILE);
    let writer = Rc::clone(&fixture);
    let mut published = false;
    let result = with_after_read(
        move |name| {
            if name == TERMINAL_FILE && !published {
                published = true;
                writer.publish_through(TERMINAL_FILE);
                writer.lock.borrow_mut().take();
            }
        },
        || fixture.observe(),
    );
    assert!(
        matches!(result, Ok(GatedCliObservation::Terminal(_))),
        "{result:?}"
    );
}

#[test]
fn terminal_refresh_after_unlock_uses_fresh_exec_evidence() {
    terminal_published_before_liveness_check(true);
}

#[test]
fn terminal_published_before_helper_exit_is_not_lost() {
    terminal_published_before_liveness_check(false);
}

#[test]
fn genuine_missing_exec_evidence_and_terminal_tampering_are_rejected() {
    let fixture = Fixture::new(false);
    fixture.publish_through(TERMINAL_FILE);
    fixture.lock.borrow_mut().take();
    let exec_path = fixture.submission.entry_path.join(EXEC_STARTED_FILE);
    fs::remove_file(&exec_path).unwrap();
    assert_eq!(fixture.observe(), Err(GatedCliProcessError::Integrity));
    fixture
        .submission
        .publish_exec_start(&fixture.exec_start)
        .unwrap();
    assert!(matches!(
        fixture.observe(),
        Ok(GatedCliObservation::Terminal(_))
    ));
    let mut terminal = fixture.terminal.clone();
    terminal.launch_nonce = Uuid::new_v4();
    fs::write(
        fixture.submission.entry_path.join(TERMINAL_FILE),
        serde_json::to_vec(&terminal).unwrap(),
    )
    .unwrap();
    assert_eq!(fixture.observe(), Err(GatedCliProcessError::Integrity));
}

#[test]
fn genuinely_exited_helper_without_terminal_remains_lost() {
    let fixture = Fixture::new(false);
    fixture.publish_through(RELEASE_FILE);
    fixture.lock.borrow_mut().take();
    assert_eq!(
        fixture.observe(),
        Ok(GatedCliObservation::Lost {
            released: true,
            child_alive: false
        })
    );
}
