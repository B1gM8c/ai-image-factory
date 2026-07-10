#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub enum JobState {
    Accepted,
    Reserved,
    Queued,
    Leased,
    Running,
    ProviderWaiting,
    ArtifactReady,
    Succeeded,
    Failed,
    Canceled,
    TimedOut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub enum ReservationState {
    Reserved,
    Committed,
    Released,
    Expired,
}

impl JobState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Reserved => "reserved",
            Self::Queued => "queued",
            Self::Leased => "leased",
            Self::Running => "running",
            Self::ProviderWaiting => "provider_waiting",
            Self::ArtifactReady => "artifact_ready",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::TimedOut => "timed_out",
        }
    }

    #[allow(dead_code)]
    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Accepted, Self::Reserved)
                | (Self::Reserved, Self::Queued)
                | (Self::Reserved, Self::Running)
                | (Self::Queued, Self::Leased)
                | (Self::Leased, Self::Running)
                | (Self::Running, Self::ProviderWaiting)
                | (Self::Running, Self::ArtifactReady)
                | (Self::Running, Self::Succeeded)
                | (Self::Running, Self::Failed)
                | (Self::Running, Self::Canceled)
                | (Self::Running, Self::TimedOut)
                | (Self::ProviderWaiting, Self::ArtifactReady)
                | (Self::ProviderWaiting, Self::Failed)
                | (Self::ProviderWaiting, Self::Canceled)
                | (Self::ProviderWaiting, Self::TimedOut)
                | (Self::ArtifactReady, Self::Succeeded)
        )
    }
}

impl ReservationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Committed => "committed",
            Self::Released => "released",
            Self::Expired => "expired",
        }
    }

    #[allow(dead_code)]
    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Reserved, Self::Committed)
                | (Self::Reserved, Self::Released)
                | (Self::Reserved, Self::Expired)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synchronous_job_can_succeed_or_fail_after_running() {
        assert!(JobState::Accepted.can_transition_to(JobState::Reserved));
        assert!(JobState::Reserved.can_transition_to(JobState::Running));
        assert!(JobState::Running.can_transition_to(JobState::Succeeded));
        assert!(JobState::Running.can_transition_to(JobState::Failed));
    }

    #[test]
    fn terminal_job_states_do_not_transition() {
        assert!(!JobState::Succeeded.can_transition_to(JobState::Failed));
        assert!(!JobState::Failed.can_transition_to(JobState::Running));
        assert!(!JobState::Canceled.can_transition_to(JobState::Queued));
        assert!(!JobState::TimedOut.can_transition_to(JobState::Queued));
    }

    #[test]
    fn reservation_terminal_states_are_one_way() {
        assert!(ReservationState::Reserved.can_transition_to(ReservationState::Committed));
        assert!(ReservationState::Reserved.can_transition_to(ReservationState::Released));
        assert!(ReservationState::Reserved.can_transition_to(ReservationState::Expired));
        assert!(!ReservationState::Committed.can_transition_to(ReservationState::Released));
        assert!(!ReservationState::Released.can_transition_to(ReservationState::Committed));
    }
}
