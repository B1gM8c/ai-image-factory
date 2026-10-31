mod accepted_terminal_evidence;
mod accepted_terminal_recovery;
pub(crate) mod compliance_evidence;
mod compliance_reconciliation;
mod driver;
mod image_supplement;
mod inputs;
mod journal;
mod operator_failure_release;
mod process;
mod video_supplement;

pub use image_supplement::{
    DreaminaImageSupplementOperatorReceipt, dispatch_dreamina_image_supplement_operator,
    poll_dreamina_image_supplement_operator, prepare_dreamina_image_supplement_operator,
    publish_dreamina_image_supplement_operator,
};

pub use video_supplement::{
    DreaminaVideoSupplementOperatorReceipt, dispatch_dreamina_video_supplement_operator,
    poll_dreamina_video_supplement_operator, prepare_dreamina_video_supplement_operator,
    prepare_dreamina_video_supplement_successor_operator,
    recover_dreamina_video_supplement_acceptance_operator,
};

pub use driver::{GatedCliPreparedSubmission, GatedCliSubmitCodec, GatedCliSubmitDriver};
pub use inputs::GatedCliInputFile;
pub(super) use journal::{
    RemoteSubmitJournal, RemoteSubmitJournalError, RemoteSubmitJournalObservation,
    RemoteSubmitJournalSpec, RemoteSubmitJournalTerminal, RemoteSubmitLaunch,
    RemoteSubmitLaunchAuthority, RemoteSubmitRelease, RemoteSubmitReleasedAuthority,
};
pub use process::{
    GatedCliBinding, GatedCliCommand, GatedCliObservation, GatedCliProcessError,
    GatedCliProcessOutcome, GatedCliProcessTerminal, GatedCliReady, GatedCliReceiptEvidence,
    GatedCliSubmission, run_remote_submit_gate, run_remote_submit_runner,
};

pub use compliance_reconciliation::{
    DreaminaComplianceRejectionResolution, DreaminaComplianceRejectionResolutionRequest,
    resolve_dreamina_compliance_rejection,
};

pub use accepted_terminal_recovery::{
    DreaminaAcceptedTerminalResolution, DreaminaAcceptedTerminalResolutionRequest,
    resolve_dreamina_accepted_terminal,
};
