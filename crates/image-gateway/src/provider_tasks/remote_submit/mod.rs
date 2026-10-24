pub(crate) mod compliance_evidence;
mod compliance_reconciliation;
mod driver;
mod inputs;
mod journal;
mod process;

pub use driver::{GatedCliPreparedSubmission, GatedCliSubmitCodec, GatedCliSubmitDriver};
pub use inputs::GatedCliInputFile;
pub(super) use journal::{
    RemoteSubmitJournal, RemoteSubmitJournalError, RemoteSubmitJournalObservation,
    RemoteSubmitJournalSpec, RemoteSubmitJournalTerminal, RemoteSubmitLaunch,
    RemoteSubmitLaunchAuthority, RemoteSubmitRelease, RemoteSubmitReleasedAuthority,
};
pub use process::{
    GatedCliBinding, GatedCliCommand, GatedCliObservation, GatedCliProcessError,
    GatedCliProcessOutcome, GatedCliProcessTerminal, GatedCliReady, GatedCliSubmission,
    run_remote_submit_gate, run_remote_submit_runner,
};

pub use compliance_reconciliation::{
    DreaminaComplianceRejectionResolution, DreaminaComplianceRejectionResolutionRequest,
    resolve_dreamina_compliance_rejection,
};
