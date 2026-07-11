mod assertions;
mod database;
mod process;

pub(crate) use assertions::{
    assert_artifact_bytes, assert_codex_outputs, assert_prompt_semantics, assert_response, header,
    opaque_png, tamper_artifact,
};
pub(crate) use database::TestDatabase;
pub(crate) use process::{
    GatewayProcess, SmokeFiles, poll_health, read_pid, start_gateway_with_retry,
    startup_failed_from_address_in_use,
};

pub(crate) type TestResult<T = ()> = Result<T, String>;

pub(crate) const API_TOKEN: &str = "process-smoke-api-secret";
pub(crate) const ADMIN_TOKEN: &str = "process-smoke-admin-secret";

pub(crate) fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

pub(crate) fn combine_results(
    primary: TestResult,
    cleanup: TestResult,
    cleanup_name: &str,
) -> TestResult {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(cleanup)) => Err(format!("{cleanup_name} failed: {cleanup}")),
        (Err(error), Err(cleanup)) => {
            Err(format!("{error}\n{cleanup_name} also failed: {cleanup}"))
        }
    }
}
