use std::{
    fs::{self, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::{
        fd::{AsRawFd, OwnedFd, RawFd},
        unix::{
            fs::{MetadataExt, OpenOptionsExt},
            process::{CommandExt, ExitStatusExt},
        },
    },
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use uuid::Uuid;

use super::{
    CHILD_FILE, CapturedStream, DiskChildIdentity, DiskExecStart, DiskHelperIdentity, DiskOutcome,
    DiskRelease, DiskRequest, DiskTerminal, GatedCliProcessError, GatedCliSubmission, HELPER_FILE,
    MAX_CAPTURED_STREAM_BYTES, MAX_EXEC_STATUS_BYTES, MAX_IDENTITY_BYTES, MAX_TERMINAL_BYTES,
    PROCESS_POLL_INTERVAL, RELEASE_FILE, STDIN_FILE, TERMINAL_FILE, map_journal_error,
    unix::{create_pipe, process_group_exists, set_cloexec, signal_process_group, unix_time_ms},
};
use crate::provider_tasks::remote_submit::journal::{
    read_optional_bytes, read_optional_json, read_required_json, valid_error_code,
};

enum ReleaseWait {
    Released(DiskRelease),
    ChildExited,
    AbsoluteDeadlineElapsed,
}

#[doc(hidden)]
pub fn run_remote_submit_runner(
    root: impl AsRef<Path>,
    submission_id: Uuid,
) -> Result<(), GatedCliProcessError> {
    let submission = GatedCliSubmission::new(root, submission_id)?;
    let request = submission.read_unbound_request()?;
    if read_optional_json::<DiskTerminal>(&submission.directory, TERMINAL_FILE, MAX_TERMINAL_BYTES)
        .map_err(map_journal_error)?
        .is_some()
    {
        return Ok(());
    }
    if read_optional_bytes(&submission.directory, CHILD_FILE, MAX_IDENTITY_BYTES)
        .map_err(map_journal_error)?
        .is_some()
    {
        return Err(GatedCliProcessError::Conflict);
    }
    let helper_lock = submission.acquire_helper_lock()?;
    let helper = DiskHelperIdentity::capture(&helper_lock)?;
    submission.publish_helper(&helper)?;

    let (release_read, release_write) = create_pipe()?;
    let (exec_status_read, exec_status_write) = create_pipe()?;
    set_cloexec(release_read.as_raw_fd(), false)?;
    set_cloexec(exec_status_write.as_raw_fd(), false)?;
    let mut child = spawn_gate_child(
        &submission,
        release_read.as_raw_fd(),
        exec_status_write.as_raw_fd(),
        helper.helper_nonce,
    )?;
    drop(release_read);
    drop(exec_status_write);
    let mut release_write = fs::File::from(release_write);
    let mut exec_status_read = fs::File::from(exec_status_read);
    let pid = child.id();
    let child_identity = DiskChildIdentity::capture(pid, &helper)?;
    submission.publish_child(&child_identity)?;

    let stdout = child
        .stdout
        .take()
        .ok_or(GatedCliProcessError::Unavailable)?;
    let stderr = child
        .stderr
        .take()
        .ok_or(GatedCliProcessError::Unavailable)?;
    let stdout_thread = thread::spawn(move || capture_stream(stdout));
    let stderr_thread = thread::spawn(move || capture_stream(stderr));
    let exec_status_thread = thread::spawn(move || read_exec_status(&mut exec_status_read));

    let release_wait =
        wait_for_release(&submission, &request, &helper, &child_identity, &mut child)?;
    let (mut release, gate_authorized, outcome) = match release_wait {
        ReleaseWait::Released(release) => {
            let gate_authorized = unix_time_ms()? < request.absolute_deadline_unix_ms;
            if gate_authorized {
                release_write
                    .write_all(&[1])
                    .map_err(|_| GatedCliProcessError::Unavailable)?;
                drop(release_write);
                let outcome = wait_for_command(&request, &child_identity, &mut child)?;
                (Some(release), true, outcome)
            } else {
                drop(release_write);
                kill_owned_child(&child_identity, &mut child)?;
                (Some(release), false, DiskOutcome::AbsoluteDeadlineElapsed)
            }
        }
        ReleaseWait::AbsoluteDeadlineElapsed => {
            drop(release_write);
            kill_owned_child(&child_identity, &mut child)?;
            (None, false, DiskOutcome::AbsoluteDeadlineElapsed)
        }
        ReleaseWait::ChildExited => {
            drop(release_write);
            (
                None,
                false,
                DiskOutcome::GateFailed {
                    error_code: "gate_lost_before_release".to_owned(),
                },
            )
        }
    };
    let status = child
        .try_wait()
        .map_err(|_| GatedCliProcessError::Unavailable)?
        .ok_or(GatedCliProcessError::Unavailable)?;
    let stdout = join_capture(stdout_thread)?;
    let stderr = join_capture(stderr_thread)?;
    let exec_status = exec_status_thread
        .join()
        .map_err(|_| GatedCliProcessError::Unavailable)??;
    if release.is_none() {
        release = submission.read_release(&request, &helper, &child_identity)?;
    }
    let released = release.is_some();
    let exec_start =
        submission.read_exec_start(&request, &helper, &child_identity, release.as_ref())?;
    let exec_started = exec_start.is_some();
    let outcome = if matches!(outcome, DiskOutcome::AbsoluteDeadlineElapsed) {
        outcome
    } else if !exec_status.is_empty() {
        let error_code = parse_gate_error(&exec_status);
        if error_code == "gate_not_ready" && unix_time_ms()? >= request.absolute_deadline_unix_ms {
            DiskOutcome::AbsoluteDeadlineElapsed
        } else {
            DiskOutcome::GateFailed { error_code }
        }
    } else if gate_authorized && !exec_started {
        match outcome {
            DiskOutcome::AbsoluteDeadlineElapsed => outcome,
            _ => DiskOutcome::GateFailed {
                error_code: "gate_lost_before_exec".to_owned(),
            },
        }
    } else {
        outcome
    };
    let outcome = normalize_exit_outcome(outcome, status, exec_started);
    let terminal = DiskTerminal::new(
        &request,
        &helper,
        &child_identity,
        released,
        exec_started,
        outcome,
        stdout,
        stderr,
    );
    terminal.validate(
        &request,
        &helper,
        &child_identity,
        release.as_ref(),
        exec_start.as_ref(),
    )?;
    submission.publish_terminal(&terminal)?;
    drop(helper_lock);
    Ok(())
}

#[doc(hidden)]
pub fn run_remote_submit_gate(
    root: impl AsRef<Path>,
    submission_id: Uuid,
    release_fd: OwnedFd,
    exec_status_fd: OwnedFd,
    helper_nonce: Uuid,
) -> Result<(), GatedCliProcessError> {
    if release_fd.as_raw_fd() <= libc::STDERR_FILENO
        || exec_status_fd.as_raw_fd() <= libc::STDERR_FILENO
        || release_fd.as_raw_fd() == exec_status_fd.as_raw_fd()
        || helper_nonce.is_nil()
    {
        return Err(GatedCliProcessError::InvalidInput);
    }
    let mut release = fs::File::from(release_fd);
    let mut exec_status = fs::File::from(exec_status_fd);
    let result = run_gate_inner(
        root.as_ref(),
        submission_id,
        helper_nonce,
        &mut release,
        &mut exec_status,
    );
    if let Err(error) = result {
        let _ = exec_status.write_all(gate_error_code(error).as_bytes());
        let _ = exec_status.flush();
        return Err(error);
    }
    Ok(())
}

fn run_gate_inner(
    root: &Path,
    submission_id: Uuid,
    helper_nonce: Uuid,
    release_pipe: &mut fs::File,
    exec_status: &mut fs::File,
) -> Result<(), GatedCliProcessError> {
    let mut byte = [0_u8; 1];
    release_pipe
        .read_exact(&mut byte)
        .map_err(|_| GatedCliProcessError::Unavailable)?;
    if byte != [1] {
        return Err(GatedCliProcessError::Integrity);
    }
    let submission = GatedCliSubmission::new(root, submission_id)?;
    let request = submission.read_unbound_request()?;
    let helper = read_required_json::<DiskHelperIdentity>(
        &submission.directory,
        HELPER_FILE,
        MAX_IDENTITY_BYTES,
    )
    .map_err(map_journal_error)?;
    let child = read_required_json::<DiskChildIdentity>(
        &submission.directory,
        CHILD_FILE,
        MAX_IDENTITY_BYTES,
    )
    .map_err(map_journal_error)?;
    helper.validate()?;
    child.validate(&helper)?;
    if helper.helper_nonce != helper_nonce || child.pid != std::process::id() {
        return Err(GatedCliProcessError::Integrity);
    }
    let release =
        read_required_json::<DiskRelease>(&submission.directory, RELEASE_FILE, MAX_IDENTITY_BYTES)
            .map_err(map_journal_error)?;
    release.validate(&request, &helper, &child)?;
    if unix_time_ms()? >= request.absolute_deadline_unix_ms {
        return Err(GatedCliProcessError::NotReady);
    }
    let command = request.rebuild_command()?;
    let stdin = open_stdin_file(&submission, &request)?;
    let mut process = Command::new(command.executable().path());
    process
        .args(command.arguments())
        .current_dir(command.working_directory().path())
        .env_clear()
        .stdin(stdin)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    for (name, value) in command.environment() {
        process.env(name, value);
    }
    set_cloexec(exec_status.as_raw_fd(), true)?;
    if unix_time_ms()? >= request.absolute_deadline_unix_ms {
        return Err(GatedCliProcessError::NotReady);
    }
    let exec_start = DiskExecStart::new(&request, &helper, &child);
    submission.publish_exec_start(&exec_start)?;
    if unix_time_ms()? >= request.absolute_deadline_unix_ms {
        return Err(GatedCliProcessError::NotReady);
    }
    let error = process.exec();
    let _ = set_cloexec(exec_status.as_raw_fd(), false);
    let _ = exec_status.write_all(format!("exec_failed:{}", error.kind() as u8).as_bytes());
    let _ = exec_status.flush();
    Err(GatedCliProcessError::Unavailable)
}

fn spawn_gate_child(
    submission: &GatedCliSubmission,
    release_fd: RawFd,
    exec_status_fd: RawFd,
    helper_nonce: Uuid,
) -> Result<Child, GatedCliProcessError> {
    let executable = std::env::current_exe().map_err(|_| GatedCliProcessError::Unavailable)?;
    let mut command = Command::new(executable);
    command
        .arg("--gate")
        .arg(&submission.root_path)
        .arg(submission.submission_id.to_string())
        .arg(release_fd.to_string())
        .arg(exec_status_fd.to_string())
        .arg(helper_nonce.to_string())
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    command
        .spawn()
        .map_err(|_| GatedCliProcessError::Unavailable)
}

fn wait_for_release(
    submission: &GatedCliSubmission,
    request: &DiskRequest,
    helper: &DiskHelperIdentity,
    child_identity: &DiskChildIdentity,
    child: &mut Child,
) -> Result<ReleaseWait, GatedCliProcessError> {
    let absolute_deadline = monotonic_absolute_deadline(request.absolute_deadline_unix_ms)?;
    loop {
        if let Some(release) = submission.read_release(request, helper, child_identity)? {
            return Ok(ReleaseWait::Released(release));
        }
        if child
            .try_wait()
            .map_err(|_| GatedCliProcessError::Unavailable)?
            .is_some()
        {
            return Ok(ReleaseWait::ChildExited);
        }
        if unix_time_ms()? >= request.absolute_deadline_unix_ms
            || Instant::now() >= absolute_deadline
        {
            return Ok(ReleaseWait::AbsoluteDeadlineElapsed);
        }
        thread::sleep(PROCESS_POLL_INTERVAL);
    }
}

fn wait_for_command(
    request: &DiskRequest,
    child_identity: &DiskChildIdentity,
    child: &mut Child,
) -> Result<DiskOutcome, GatedCliProcessError> {
    let absolute_deadline = monotonic_absolute_deadline(request.absolute_deadline_unix_ms)?;
    let wall_deadline = deadline_after(Duration::from_millis(request.wall_timeout_ms))?;
    let termination_grace = Duration::from_millis(request.termination_grace_ms);
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|_| GatedCliProcessError::Unavailable)?
        {
            let outcome = exit_outcome(status);
            if terminate_residual_process_group(child_identity, termination_grace)? {
                return Ok(DiskOutcome::ResidualProcessGroup);
            }
            return Ok(outcome);
        }
        if unix_time_ms()? >= request.absolute_deadline_unix_ms
            || Instant::now() >= absolute_deadline
        {
            kill_owned_child(child_identity, child)?;
            return Ok(DiskOutcome::AbsoluteDeadlineElapsed);
        }
        if Instant::now() >= wall_deadline {
            terminate_owned_child(child_identity, child, termination_grace)?;
            return Ok(DiskOutcome::TimedOut);
        }
        thread::sleep(PROCESS_POLL_INTERVAL);
    }
}

fn terminate_owned_child(
    identity: &DiskChildIdentity,
    child: &mut Child,
    grace: Duration,
) -> Result<(), GatedCliProcessError> {
    signal_process_group(identity.process_group_id, libc::SIGTERM)?;
    let deadline = deadline_after(grace)?;
    while Instant::now() < deadline {
        if child
            .try_wait()
            .map_err(|_| GatedCliProcessError::Unavailable)?
            .is_some()
        {
            return ensure_group_gone(identity);
        }
        thread::sleep(PROCESS_POLL_INTERVAL);
    }
    kill_owned_child(identity, child)
}

fn kill_owned_child(
    identity: &DiskChildIdentity,
    child: &mut Child,
) -> Result<(), GatedCliProcessError> {
    signal_process_group(identity.process_group_id, libc::SIGKILL)?;
    let reap_deadline = deadline_after(Duration::from_secs(1))?;
    while Instant::now() < reap_deadline {
        if child
            .try_wait()
            .map_err(|_| GatedCliProcessError::Unavailable)?
            .is_some()
        {
            return ensure_group_gone(identity);
        }
        thread::sleep(PROCESS_POLL_INTERVAL);
    }
    Err(GatedCliProcessError::Unavailable)
}

fn terminate_residual_process_group(
    identity: &DiskChildIdentity,
    grace: Duration,
) -> Result<bool, GatedCliProcessError> {
    if !process_group_exists(identity.process_group_id)? {
        return Ok(false);
    }
    signal_process_group(identity.process_group_id, libc::SIGTERM)?;
    let deadline = deadline_after(grace)?;
    while Instant::now() < deadline {
        if !process_group_exists(identity.process_group_id)? {
            return Ok(true);
        }
        thread::sleep(PROCESS_POLL_INTERVAL);
    }
    signal_process_group(identity.process_group_id, libc::SIGKILL)?;
    ensure_group_gone(identity)?;
    Ok(true)
}

fn ensure_group_gone(identity: &DiskChildIdentity) -> Result<(), GatedCliProcessError> {
    if !process_group_exists(identity.process_group_id)? {
        return Ok(());
    }
    signal_process_group(identity.process_group_id, libc::SIGKILL)?;
    let deadline = deadline_after(Duration::from_secs(1))?;
    while Instant::now() < deadline {
        if !process_group_exists(identity.process_group_id)? {
            return Ok(());
        }
        thread::sleep(PROCESS_POLL_INTERVAL);
    }
    Err(GatedCliProcessError::Unavailable)
}

fn monotonic_absolute_deadline(
    absolute_deadline_unix_ms: u64,
) -> Result<Instant, GatedCliProcessError> {
    deadline_after(Duration::from_millis(
        absolute_deadline_unix_ms.saturating_sub(unix_time_ms()?),
    ))
}

fn deadline_after(duration: Duration) -> Result<Instant, GatedCliProcessError> {
    Instant::now()
        .checked_add(duration)
        .ok_or(GatedCliProcessError::InvalidInput)
}

fn normalize_exit_outcome(
    outcome: DiskOutcome,
    status: ExitStatus,
    exec_started: bool,
) -> DiskOutcome {
    if !exec_started {
        return outcome;
    }
    match outcome {
        DiskOutcome::Exited { .. } => exit_outcome(status),
        other => other,
    }
}

fn exit_outcome(status: ExitStatus) -> DiskOutcome {
    DiskOutcome::Exited {
        exit_code: status.code(),
        signal: status.signal(),
    }
}

fn open_stdin_file(
    submission: &GatedCliSubmission,
    request: &DiskRequest,
) -> Result<Stdio, GatedCliProcessError> {
    if request.stdin_byte_size == 0 {
        return OpenOptions::new()
            .read(true)
            .open("/dev/null")
            .map(Stdio::from)
            .map_err(|_| GatedCliProcessError::Unavailable);
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(submission.entry_path.join(STDIN_FILE))
        .map_err(|_| GatedCliProcessError::Integrity)?;
    let metadata = file
        .metadata()
        .map_err(|_| GatedCliProcessError::Unavailable)?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.nlink() != 1
        || metadata.len() != request.stdin_byte_size
    {
        return Err(GatedCliProcessError::Integrity);
    }
    let mut bytes = Vec::with_capacity(request.stdin_byte_size as usize);
    file.read_to_end(&mut bytes)
        .map_err(|_| GatedCliProcessError::Unavailable)?;
    request.validate_stdin(Some(&bytes))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|_| GatedCliProcessError::Unavailable)?;
    Ok(Stdio::from(file))
}

fn capture_stream(mut stream: impl Read) -> io::Result<CapturedStream> {
    let mut bytes = Vec::new();
    let mut truncated = false;
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let retained = read.min(MAX_CAPTURED_STREAM_BYTES.saturating_sub(bytes.len()));
        bytes.extend_from_slice(&buffer[..retained]);
        truncated |= retained < read;
    }
    Ok(CapturedStream { bytes, truncated })
}

fn read_exec_status(stream: &mut fs::File) -> Result<Vec<u8>, GatedCliProcessError> {
    let mut bytes = Vec::new();
    Read::by_ref(stream)
        .take(MAX_EXEC_STATUS_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GatedCliProcessError::Unavailable)?;
    if bytes.len() > MAX_EXEC_STATUS_BYTES {
        return Err(GatedCliProcessError::Integrity);
    }
    Ok(bytes)
}

fn join_capture(
    handle: thread::JoinHandle<io::Result<CapturedStream>>,
) -> Result<CapturedStream, GatedCliProcessError> {
    handle
        .join()
        .map_err(|_| GatedCliProcessError::Unavailable)?
        .map_err(|_| GatedCliProcessError::Unavailable)
}

fn parse_gate_error(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let code = text
        .split_once(':')
        .map(|(prefix, _)| prefix)
        .unwrap_or(text.as_ref());
    if valid_error_code(code) {
        code.to_owned()
    } else {
        "gate_failed".to_owned()
    }
}

fn gate_error_code(error: GatedCliProcessError) -> &'static str {
    match error {
        GatedCliProcessError::InvalidInput => "gate_invalid_input",
        GatedCliProcessError::Conflict => "gate_conflict",
        GatedCliProcessError::Integrity => "gate_integrity",
        GatedCliProcessError::Unavailable => "gate_unavailable",
        GatedCliProcessError::Busy => "gate_busy",
        GatedCliProcessError::NotReady => "gate_not_ready",
    }
}
