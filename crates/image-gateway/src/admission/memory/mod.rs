use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use image_scheduler_policy::{SchedulerConfig, ScopeWeight, effective_finish_tag, next_finish_tag};
use serde_json::Value;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::{
    AdmissionClaim, AdmissionError, AdmissionStore, AdmissionTicket, AttachInputManifest,
    AttachJob, AttachedWork, ClaimAdmission, WorkLease, WorkOutcome, validate_attach_request,
};

#[derive(Default)]
pub struct InMemoryAdmissionStore {
    state: Mutex<MemoryState>,
}

#[derive(Default)]
struct MemoryState {
    sessions: HashMap<Uuid, Session>,
    idempotency: HashMap<IdempotencyScope, IdempotencyRecord>,
    work_items: HashMap<Uuid, WorkItem>,
    work_by_job: HashMap<Uuid, Uuid>,
    work_order: Vec<Uuid>,
    scope_next_finish: HashMap<String, u64>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct IdempotencyScope {
    project_id: String,
    api_profile: String,
    operation: String,
    key_digest: String,
}

#[derive(Clone)]
struct Session {
    owner_token: Uuid,
    request_hash: String,
    state: SessionState,
    deadline_at_ms: i64,
    idempotency_scope: Option<IdempotencyScope>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SessionState {
    Receiving,
    Attached,
    Aborted,
}

#[derive(Clone)]
struct IdempotencyRecord {
    session_id: Uuid,
    request_hash: String,
    state: String,
    job_id: Option<Uuid>,
}

struct WorkItem {
    session_id: Uuid,
    job_id: Uuid,
    state: WorkState,
    lease_epoch: i64,
    lease_owner: Option<String>,
    lease_expires_at_ms: Option<i64>,
    execution_id: Option<Uuid>,
    command_schema: String,
    command_json: Value,
    input_manifest: Option<AttachInputManifest>,
    contract: super::AdmissionContract,
    schedule_priority: u8,
    schedule_finish_tag: u64,
    enqueued_at_ms: u64,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum WorkState {
    Ready,
    Leased,
    Running,
    Succeeded,
    Failed,
    Uncertain,
}

impl WorkState {
    fn from_outcome(outcome: WorkOutcome) -> Self {
        match outcome {
            WorkOutcome::Succeeded => Self::Succeeded,
            WorkOutcome::Failed => Self::Failed,
            WorkOutcome::Uncertain => Self::Uncertain,
        }
    }
}

#[async_trait]
impl AdmissionStore for InMemoryAdmissionStore {
    async fn claim(&self, request: ClaimAdmission) -> Result<AdmissionClaim, AdmissionError> {
        let now = now_ms();
        let mut state = self.state.lock().await;
        if let Some((session_id, session)) = state
            .sessions
            .iter()
            .find(|(_, session)| session.owner_token == request.owner_token)
            .map(|(session_id, session)| (*session_id, session.clone()))
        {
            if session.request_hash != request.request_hash {
                return Err(AdmissionError::InvalidOwner);
            }
            if session.state == SessionState::Receiving {
                if session.deadline_at_ms <= now {
                    abort_session(&mut state, session_id);
                    return Err(AdmissionError::Expired);
                }
                return Ok(AdmissionClaim::Owner(AdmissionTicket {
                    session_id,
                    owner_token: request.owner_token,
                    request_hash: request.request_hash,
                }));
            }
        }
        let scope = request
            .idempotency_key_digest
            .as_ref()
            .map(|key_digest| IdempotencyScope {
                project_id: request.project_id.clone(),
                api_profile: request.api_profile.clone(),
                operation: request.operation.clone(),
                key_digest: key_digest.clone(),
            });

        if let Some(scope) = scope.as_ref()
            && let Some(existing) = state.idempotency.get(scope).cloned()
        {
            if existing.request_hash != request.request_hash {
                return Ok(AdmissionClaim::Conflict {
                    job_id: existing.job_id,
                });
            }
            if existing.state == "receiving" {
                let session = state
                    .sessions
                    .get(&existing.session_id)
                    .ok_or(AdmissionError::Unavailable)?;
                if session.deadline_at_ms <= now {
                    abort_session(&mut state, existing.session_id);
                    return Err(AdmissionError::Expired);
                }
                if session.owner_token == request.owner_token {
                    return Ok(AdmissionClaim::Owner(AdmissionTicket {
                        session_id: existing.session_id,
                        owner_token: request.owner_token,
                        request_hash: request.request_hash,
                    }));
                }
                return Ok(AdmissionClaim::InProgress {
                    session_id: existing.session_id,
                });
            }
            if existing.state != "aborted" || existing.job_id.is_some() {
                return match existing.job_id {
                    Some(job_id) => Ok(AdmissionClaim::Existing {
                        job_id,
                        state: existing.state,
                    }),
                    None => Ok(AdmissionClaim::Conflict { job_id: None }),
                };
            }
        }

        if request.deadline_at_ms <= now {
            return Err(AdmissionError::Expired);
        }

        let ticket = AdmissionTicket {
            session_id: Uuid::new_v4(),
            owner_token: request.owner_token,
            request_hash: request.request_hash.clone(),
        };
        state.sessions.insert(
            ticket.session_id,
            Session {
                owner_token: ticket.owner_token,
                request_hash: request.request_hash.clone(),
                state: SessionState::Receiving,
                deadline_at_ms: request.deadline_at_ms,
                idempotency_scope: scope.clone(),
            },
        );
        if let Some(scope) = scope {
            state.idempotency.insert(
                scope,
                IdempotencyRecord {
                    session_id: ticket.session_id,
                    request_hash: request.request_hash,
                    state: "receiving".to_string(),
                    job_id: None,
                },
            );
        }
        Ok(AdmissionClaim::Owner(ticket))
    }

    async fn attach(&self, request: AttachJob) -> Result<AttachedWork, AdmissionError> {
        validate_attach_request(&request)?;
        if request.contract != super::AdmissionContract::LegacyV1 {
            return Err(AdmissionError::InvalidCommand);
        }
        let now = now_ms();
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .get(&request.ticket.session_id)
            .cloned()
            .ok_or(AdmissionError::InvalidOwner)?;
        if session.owner_token != request.ticket.owner_token
            || session.request_hash != request.ticket.request_hash
        {
            return Err(AdmissionError::InvalidOwner);
        }
        if session.state == SessionState::Attached
            && let Some(work_item_id) = state.work_by_job.get(&request.job_id)
            && let Some(work) = state.work_items.get(work_item_id)
            && work.session_id == request.ticket.session_id
            && work.command_schema == request.command_schema
            && work.command_json == request.command_json
            && work.input_manifest == request.input_manifest
        {
            return Ok(AttachedWork {
                work_item_id: *work_item_id,
                job_id: request.job_id,
            });
        }
        if session.state != SessionState::Receiving {
            return Err(AdmissionError::InvalidOwner);
        }
        if session.deadline_at_ms <= now {
            abort_session(&mut state, request.ticket.session_id);
            return Err(AdmissionError::Expired);
        }
        if state.work_by_job.contains_key(&request.job_id) {
            return Err(AdmissionError::Unavailable);
        }
        let (schedule_finish_tag, _) = schedule_slot(&mut state, &request)?;

        let work_item_id = Uuid::new_v4();
        state.work_items.insert(
            work_item_id,
            WorkItem {
                session_id: request.ticket.session_id,
                job_id: request.job_id,
                state: WorkState::Ready,
                lease_epoch: 0,
                lease_owner: None,
                lease_expires_at_ms: None,
                execution_id: None,
                command_schema: request.command_schema,
                command_json: request.command_json,
                input_manifest: request.input_manifest,
                contract: request.contract,
                schedule_priority: request.schedule_priority,
                schedule_finish_tag,
                enqueued_at_ms: now as u64,
            },
        );
        state.work_by_job.insert(request.job_id, work_item_id);
        state.work_order.push(work_item_id);
        let session = state
            .sessions
            .get_mut(&request.ticket.session_id)
            .ok_or(AdmissionError::Unavailable)?;
        session.state = SessionState::Attached;
        if let Some(scope) = session.idempotency_scope.clone() {
            let record = state
                .idempotency
                .get_mut(&scope)
                .ok_or(AdmissionError::Unavailable)?;
            record.state = "accepted".to_string();
            record.job_id = Some(request.job_id);
        }
        Ok(AttachedWork {
            work_item_id,
            job_id: request.job_id,
        })
    }

    async fn abort(&self, ticket: &AdmissionTicket) -> Result<(), AdmissionError> {
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .get(&ticket.session_id)
            .ok_or(AdmissionError::InvalidOwner)?;
        if session.owner_token != ticket.owner_token
            || session.request_hash != ticket.request_hash
            || session.state != SessionState::Receiving
        {
            return Err(AdmissionError::InvalidOwner);
        }
        abort_session(&mut state, ticket.session_id);
        Ok(())
    }

    async fn attach_and_start(
        &self,
        request: AttachJob,
        worker_id: &str,
        lease_duration_ms: i64,
    ) -> Result<WorkLease, AdmissionError> {
        let now = now_ms();
        {
            let state = self.state.lock().await;
            if let Some(session) = state.sessions.get(&request.ticket.session_id)
                && session.owner_token == request.ticket.owner_token
                && session.request_hash == request.ticket.request_hash
                && session.state == SessionState::Attached
                && let Some(work_item_id) = state.work_by_job.get(&request.job_id)
                && let Some(work) = state.work_items.get(work_item_id)
                && work.session_id == request.ticket.session_id
                && work.state == WorkState::Running
                && work.lease_owner.as_deref() == Some(worker_id)
                && work
                    .lease_expires_at_ms
                    .is_some_and(|deadline| deadline > now)
                && work.command_schema == request.command_schema
                && work.command_json == request.command_json
                && work.input_manifest == request.input_manifest
            {
                return Ok(WorkLease {
                    work_item_id: *work_item_id,
                    job_id: work.job_id,
                    execution_id: work.execution_id.ok_or(AdmissionError::InvalidOwner)?,
                    lease_epoch: work.lease_epoch,
                    worker_id: worker_id.to_string(),
                    command_schema: work.command_schema.clone(),
                    command_json: work.command_json.clone(),
                });
            }
        }

        let attached = self.attach(request).await?;
        let lease = self
            .claim_job(attached.job_id, worker_id, lease_duration_ms)
            .await?
            .ok_or(AdmissionError::Unavailable)?;
        self.start(&lease).await?;
        Ok(lease)
    }

    async fn claim_ready(
        &self,
        worker_id: &str,
        lease_duration_ms: i64,
        contract: super::AdmissionContract,
    ) -> Result<Option<WorkLease>, AdmissionError> {
        self.claim_matching(None, worker_id, lease_duration_ms, Some(contract))
            .await
    }

    async fn claim_job(
        &self,
        job_id: Uuid,
        worker_id: &str,
        lease_duration_ms: i64,
    ) -> Result<Option<WorkLease>, AdmissionError> {
        self.claim_matching(Some(job_id), worker_id, lease_duration_ms, None)
            .await
    }

    async fn start(&self, lease: &WorkLease) -> Result<(), AdmissionError> {
        let now = now_ms();
        let mut state = self.state.lock().await;
        let work = state
            .work_items
            .get_mut(&lease.work_item_id)
            .ok_or(AdmissionError::StaleLease)?;
        validate_lease(work, lease, now, &[WorkState::Leased])?;
        work.state = WorkState::Running;
        Ok(())
    }

    async fn heartbeat(
        &self,
        lease: &WorkLease,
        lease_duration_ms: i64,
    ) -> Result<(), AdmissionError> {
        let now = now_ms();
        let mut state = self.state.lock().await;
        let work = state
            .work_items
            .get_mut(&lease.work_item_id)
            .ok_or(AdmissionError::StaleLease)?;
        validate_lease(work, lease, now, &[WorkState::Leased, WorkState::Running])?;
        work.lease_expires_at_ms = Some(lease_deadline(now, lease_duration_ms));
        Ok(())
    }

    async fn settle(
        &self,
        lease: &WorkLease,
        outcome: WorkOutcome,
        _error_code: Option<&str>,
    ) -> Result<(), AdmissionError> {
        let now = now_ms();
        let mut state = self.state.lock().await;
        let session_id = {
            let work = state
                .work_items
                .get_mut(&lease.work_item_id)
                .ok_or(AdmissionError::StaleLease)?;
            validate_lease(work, lease, now, &[WorkState::Running])?;
            work.state = WorkState::from_outcome(outcome);
            work.lease_owner = None;
            work.lease_expires_at_ms = None;
            work.session_id
        };
        if let Some(scope) = state
            .sessions
            .get(&session_id)
            .and_then(|session| session.idempotency_scope.as_ref())
            .cloned()
        {
            let record = state
                .idempotency
                .get_mut(&scope)
                .ok_or(AdmissionError::Unavailable)?;
            record.state = outcome.as_str().to_string();
        }
        Ok(())
    }
}

impl InMemoryAdmissionStore {
    async fn claim_matching(
        &self,
        job_id: Option<Uuid>,
        worker_id: &str,
        lease_duration_ms: i64,
        contract: Option<super::AdmissionContract>,
    ) -> Result<Option<WorkLease>, AdmissionError> {
        let now = now_ms();
        let mut state = self.state.lock().await;
        let work_item_id = state
            .work_order
            .iter()
            .copied()
            .filter(|work_item_id| {
                state.work_items.get(work_item_id).is_some_and(|work| {
                    work.state == WorkState::Ready
                        && job_id.is_none_or(|job_id| work.job_id == job_id)
                        && contract.is_none_or(|contract| work.contract == contract)
                })
            })
            .min_by_key(|work_item_id| {
                let work = state
                    .work_items
                    .get(work_item_id)
                    .expect("filtered work item must exist");
                (
                    effective_finish_tag(
                        work.schedule_finish_tag,
                        work.enqueued_at_ms,
                        image_scheduler_policy::Priority::new(work.schedule_priority)
                            .expect("validated schedule priority"),
                        now as u64,
                        SchedulerConfig::default(),
                    ),
                    u8::MAX - work.schedule_priority,
                    work.enqueued_at_ms,
                    work_item_id.as_u128(),
                )
            });
        let Some(work_item_id) = work_item_id else {
            return Ok(None);
        };
        let work = state
            .work_items
            .get_mut(&work_item_id)
            .ok_or(AdmissionError::Unavailable)?;
        work.state = WorkState::Leased;
        work.lease_epoch += 1;
        work.lease_owner = Some(worker_id.to_string());
        work.lease_expires_at_ms = Some(lease_deadline(now, lease_duration_ms));
        let execution_id = Uuid::new_v4();
        work.execution_id = Some(execution_id);
        Ok(Some(WorkLease {
            work_item_id,
            job_id: work.job_id,
            execution_id,
            lease_epoch: work.lease_epoch,
            worker_id: worker_id.to_string(),
            command_schema: work.command_schema.clone(),
            command_json: work.command_json.clone(),
        }))
    }
}

fn validate_lease(
    work: &WorkItem,
    lease: &WorkLease,
    now: i64,
    allowed_states: &[WorkState],
) -> Result<(), AdmissionError> {
    let matches = work.job_id == lease.job_id
        && work.lease_epoch == lease.lease_epoch
        && work.lease_owner.as_deref() == Some(lease.worker_id.as_str())
        && work.execution_id == Some(lease.execution_id)
        && work
            .lease_expires_at_ms
            .is_some_and(|expires| expires > now)
        && allowed_states.contains(&work.state);
    if matches {
        Ok(())
    } else {
        Err(AdmissionError::StaleLease)
    }
}

fn schedule_slot(
    state: &mut MemoryState,
    request: &AttachJob,
) -> Result<(u64, u32), AdmissionError> {
    let weight = ScopeWeight::new(request.schedule_weight)
        .ok_or(AdmissionError::InvalidCommand)?
        .value();
    if request.schedule_scope.is_empty()
        || request.schedule_priority > 3
        || request.schedule_cost == 0
    {
        return Err(AdmissionError::InvalidCommand);
    }
    let previous = state
        .scope_next_finish
        .get(&request.schedule_scope)
        .copied()
        .unwrap_or_default();
    let finish = next_finish_tag(
        previous,
        request.schedule_cost,
        ScopeWeight::new(weight).expect("validated schedule weight"),
    );
    state
        .scope_next_finish
        .insert(request.schedule_scope.clone(), finish);
    Ok((finish, weight))
}

fn abort_session(state: &mut MemoryState, session_id: Uuid) {
    let Some(session) = state.sessions.get_mut(&session_id) else {
        return;
    };
    session.state = SessionState::Aborted;
    if let Some(scope) = session.idempotency_scope.clone()
        && let Some(record) = state.idempotency.get_mut(&scope)
        && record.state == "receiving"
    {
        record.state = "aborted".to_string();
    }
}

fn lease_deadline(now: i64, lease_duration_ms: i64) -> i64 {
    now.saturating_add(lease_duration_ms.max(1))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
