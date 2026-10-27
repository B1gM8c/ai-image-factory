//! Append-only receipt recovery evidence, separate from the original CLI streams.

use super::*;
use std::io::Read;

const EVIDENCE_PREFIX: &str = "process-receipt-recovery-";
const MAX_EVIDENCE_BYTES: u64 = 4 * 1024 * 1024;

/// Constructed only from a validated, released terminal gate. Codecs cannot use
/// this capability to rewrite the gate, its original stdout, or its nonce.
pub struct GatedCliReceiptEvidence {
    submission: GatedCliSubmission,
    binding: GatedCliBinding,
    command: GatedCliCommand,
    request_sha256: String,
    terminal_sha256: String,
    started_at_ms: i64,
    finished_at_ms: i64,
}

impl GatedCliSubmission {
    pub(crate) fn receipt_evidence(
        self,
        binding: &GatedCliBinding,
        expected: &GatedCliProcessTerminal,
    ) -> Result<GatedCliReceiptEvidence, GatedCliProcessError> {
        let request = self.read_request(binding)?;
        let chain = self.read_process_evidence(&request)?;
        let (started, started_at_ms, _): (DiskExecStart, _, _) =
            marker_millis(&self, EXEC_STARTED_FILE)?;
        let (terminal, finished_at_ms, terminal_sha256): (DiskTerminal, _, _) =
            marker_millis(&self, TERMINAL_FILE)?;
        let helper = chain
            .helper
            .as_ref()
            .ok_or(GatedCliProcessError::Integrity)?;
        let child = chain
            .child
            .as_ref()
            .ok_or(GatedCliProcessError::Integrity)?;
        started.validate(&request, helper, child, chain.release.as_ref())?;
        terminal.validate(
            &request,
            helper,
            child,
            chain.release.as_ref(),
            Some(&started),
        )?;
        let terminal = terminal.into_public()?;
        if &terminal != expected || !terminal.released() || !terminal.exec_started() {
            return Err(GatedCliProcessError::Integrity);
        }
        if started_at_ms > finished_at_ms {
            return Err(GatedCliProcessError::Integrity);
        }
        let stdin = read_optional_bytes(&self.directory, STDIN_FILE, MAX_STDIN_BYTES as u64)
            .map_err(map_journal_error)?
            .unwrap_or_default();
        let request_bytes = read_optional_bytes(&self.directory, REQUEST_FILE, MAX_REQUEST_BYTES)
            .map_err(map_journal_error)?
            .ok_or(GatedCliProcessError::Integrity)?;
        let reread: DiskRequest =
            serde_json::from_slice(&request_bytes).map_err(|_| GatedCliProcessError::Integrity)?;
        if reread != request {
            return Err(GatedCliProcessError::Integrity);
        }
        let command = GatedCliCommand {
            executable: request.executable,
            executable_sha256: request.executable_sha256,
            working_directory: request.working_directory,
            arguments: request.arguments,
            environment: request.environment,
            stdin,
            wall_timeout_ms: request.wall_timeout_ms,
            termination_grace_ms: request.termination_grace_ms,
        };
        Ok(GatedCliReceiptEvidence {
            submission: self,
            binding: binding.clone(),
            command,
            request_sha256: hex::encode(Sha256::digest(&request_bytes)),
            terminal_sha256,
            started_at_ms,
            finished_at_ms,
        })
    }
}

impl GatedCliReceiptEvidence {
    pub fn command(&self) -> &GatedCliCommand {
        &self.command
    }

    pub fn request_sha256(&self) -> &str {
        &self.request_sha256
    }

    pub fn terminal_sha256(&self) -> &str {
        &self.terminal_sha256
    }

    pub fn started_at_ms(&self) -> i64 {
        self.started_at_ms
    }

    pub fn finished_at_ms(&self) -> i64 {
        self.finished_at_ms
    }

    pub fn launch_nonce(&self) -> Uuid {
        self.binding.launch_nonce
    }

    pub fn execution_binding_sha256(&self) -> &str {
        &self.binding.execution_binding_sha256
    }

    pub fn read(&self, name: &str, limit: u64) -> Result<Option<Vec<u8>>, GatedCliProcessError> {
        validate_name(name, limit)?;
        self.submission.read_request(&self.binding)?;
        read_optional_bytes(&self.submission.directory, name, limit).map_err(map_journal_error)
    }

    pub fn publish(&self, name: &str, bytes: &[u8]) -> Result<(), GatedCliProcessError> {
        validate_name(name, bytes.len() as u64)?;
        self.submission.read_request(&self.binding)?;
        publish_or_compare(&self.submission.directory, name, bytes, MAX_EVIDENCE_BYTES)
            .map_err(map_journal_error)
    }
}

fn validate_name(name: &str, limit: u64) -> Result<(), GatedCliProcessError> {
    if !name.starts_with(EVIDENCE_PREFIX)
        || name.len() > 160
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
        || limit == 0
        || limit > MAX_EVIDENCE_BYTES
    {
        return Err(GatedCliProcessError::InvalidInput);
    }
    Ok(())
}

fn marker_millis<T: DeserializeOwned>(
    submission: &GatedCliSubmission,
    name: &str,
) -> Result<(T, i64, String), GatedCliProcessError> {
    // Timestamp and identity come from the same no-follow descriptor, never a
    // second path lookup after validating a different file.
    let fd = rfs::openat(
        &submission.directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(|_| GatedCliProcessError::Integrity)?;
    let mut file = fs::File::from(fd);
    let stat = rfs::fstat(&file).map_err(|_| GatedCliProcessError::Unavailable)?;
    let size = super::super::journal::validate_file(&stat, MAX_TERMINAL_BYTES)
        .map_err(map_journal_error)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_TERMINAL_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GatedCliProcessError::Unavailable)?;
    let final_stat = rfs::fstat(&file).map_err(|_| GatedCliProcessError::Unavailable)?;
    if bytes.len() != size
        || super::super::journal::validate_file(&final_stat, MAX_TERMINAL_BYTES)
            .map_err(map_journal_error)?
            != size
        || final_stat.st_mtime != stat.st_mtime
        || final_stat.st_mtime_nsec != stat.st_mtime_nsec
    {
        return Err(GatedCliProcessError::Integrity);
    }
    let value = serde_json::from_slice(&bytes).map_err(|_| GatedCliProcessError::Integrity)?;
    let millis = stat
        .st_mtime
        .checked_mul(1000)
        // Darwin exposes signed nanoseconds, Linux unsigned. Dividing first
        // bounds either representation to an i64 before the checked addition.
        .and_then(|millis| millis.checked_add((stat.st_mtime_nsec / 1_000_000) as i64))
        .ok_or(GatedCliProcessError::Integrity)?;
    Ok((value, millis, hex::encode(Sha256::digest(&bytes))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_capability_never_names_an_original_marker_or_path() {
        for name in [
            "process-terminal.json",
            "terminal.json",
            "../process-receipt-recovery-a",
            "process-receipt-recovery-x/y",
        ] {
            assert!(validate_name(name, 1).is_err());
        }
        assert!(validate_name("process-receipt-recovery-source.json", 1024).is_ok());
    }
}
