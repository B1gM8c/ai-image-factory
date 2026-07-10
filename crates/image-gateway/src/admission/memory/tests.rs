use std::{sync::Arc, time::Duration};

use serde_json::json;
use tokio::sync::Barrier;
use uuid::Uuid;

use super::super::{
    AdmissionClaim, AdmissionError, AdmissionStore, AdmissionTicket, AttachJob, ClaimAdmission,
    WorkOutcome,
};
use super::InMemoryAdmissionStore;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_millis() as i64
}

fn claim_request(key_digest: Option<&str>, request_hash: &str) -> ClaimAdmission {
    ClaimAdmission {
        tenant_id: "tenant-a".to_string(),
        project_id: "project-a".to_string(),
        api_profile: "openai-images-v1".to_string(),
        operation: "images.generate".to_string(),
        request_id: Uuid::new_v4().to_string(),
        idempotency_key_digest: key_digest.map(str::to_string),
        request_hash: request_hash.to_string(),
        deadline_at_ms: now_ms() + 60_000,
    }
}

async fn owner(
    store: &InMemoryAdmissionStore,
    key_digest: Option<&str>,
    request_hash: &str,
) -> AdmissionTicket {
    match store
        .claim(claim_request(key_digest, request_hash))
        .await
        .expect("claim must succeed")
    {
        AdmissionClaim::Owner(ticket) => ticket,
        other => panic!("expected owner, got {other:?}"),
    }
}

async fn attach(store: &InMemoryAdmissionStore, ticket: AdmissionTicket, job_id: Uuid) {
    store
        .attach(AttachJob {
            ticket,
            job_id,
            command_schema: "image.generate.v1".to_string(),
            command_json: json!({"prompt": "draw a lighthouse"}),
            work_kind: "image.generate".to_string(),
        })
        .await
        .expect("attach must succeed");
}

#[tokio::test]
async fn keyed_claim_has_one_owner_and_enforces_hash_conflicts() {
    let store = Arc::new(InMemoryAdmissionStore::default());
    let barrier = Arc::new(Barrier::new(16));
    let mut tasks = Vec::new();

    for _ in 0..16 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .claim(claim_request(Some("digest-a"), "hash-a"))
                .await
                .expect("claim must succeed")
        }));
    }

    let mut owners = 0;
    let mut in_progress = 0;
    for task in tasks {
        match task.await.expect("task must complete") {
            AdmissionClaim::Owner(_) => owners += 1,
            AdmissionClaim::InProgress { .. } => in_progress += 1,
            other => panic!("unexpected claim: {other:?}"),
        }
    }
    assert_eq!(owners, 1);
    assert_eq!(in_progress, 15);

    assert!(matches!(
        store
            .claim(claim_request(Some("digest-a"), "different-hash"))
            .await
            .expect("conflict is a claim result"),
        AdmissionClaim::Conflict { job_id: None }
    ));

    let mut other_scope = claim_request(Some("digest-a"), "hash-a");
    other_scope.project_id = "project-b".to_string();
    assert!(matches!(
        store.claim(other_scope).await.expect("claim must succeed"),
        AdmissionClaim::Owner(_)
    ));
}

#[tokio::test]
async fn claims_without_an_idempotency_key_are_independent() {
    let store = InMemoryAdmissionStore::default();

    let first = store
        .claim(claim_request(None, "same-hash"))
        .await
        .expect("first claim must succeed");
    let second = store
        .claim(claim_request(None, "same-hash"))
        .await
        .expect("second claim must succeed");

    let (AdmissionClaim::Owner(first), AdmissionClaim::Owner(second)) = (first, second) else {
        panic!("unkeyed claims must both own independent sessions");
    };
    assert_ne!(first.session_id, second.session_id);
}

#[tokio::test]
async fn attach_requires_the_owner_and_an_object_command() {
    let store = InMemoryAdmissionStore::default();
    let ticket = owner(&store, Some("attach-key"), "attach-hash").await;
    let job_id = Uuid::new_v4();
    let forged = AdmissionTicket {
        owner_token: Uuid::new_v4(),
        ..ticket.clone()
    };

    assert!(matches!(
        store
            .attach(AttachJob {
                ticket: forged,
                job_id,
                command_schema: "image.generate.v1".to_string(),
                command_json: json!({"prompt": "forged"}),
                work_kind: "image.generate".to_string(),
            })
            .await,
        Err(AdmissionError::InvalidOwner)
    ));
    assert!(matches!(
        store
            .attach(AttachJob {
                ticket: ticket.clone(),
                job_id,
                command_schema: "image.generate.v1".to_string(),
                command_json: json!(["not", "an", "object"]),
                work_kind: "image.generate".to_string(),
            })
            .await,
        Err(AdmissionError::InvalidCommand)
    ));

    attach(&store, ticket, job_id).await;
}

#[tokio::test]
async fn claim_job_only_takes_the_requested_ready_work() {
    let store = Arc::new(InMemoryAdmissionStore::default());
    let first_job = Uuid::new_v4();
    let second_job = Uuid::new_v4();
    attach(&store, owner(&store, None, "first").await, first_job).await;
    attach(&store, owner(&store, None, "second").await, second_job).await;

    let barrier = Arc::new(Barrier::new(2));
    let mut tasks = Vec::new();
    for worker in ["worker-a", "worker-b"] {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .claim_job(first_job, worker, 30_000)
                .await
                .expect("claim_job must succeed")
        }));
    }

    let mut claimed = tasks.into_iter();
    let first = claimed
        .next()
        .expect("first task must exist")
        .await
        .expect("first task must complete");
    let second = claimed
        .next()
        .expect("second task must exist")
        .await
        .expect("second task must complete");
    assert_eq!(
        usize::from(first.is_some()) + usize::from(second.is_some()),
        1
    );
    assert_eq!(
        first.as_ref().or(second.as_ref()).unwrap().job_id,
        first_job
    );

    let remaining = store
        .claim_ready("worker-c", 30_000)
        .await
        .expect("claim_ready must succeed")
        .expect("second job must remain ready");
    assert_eq!(remaining.job_id, second_job);
}

#[tokio::test]
async fn lease_transitions_fence_owner_execution_epoch_and_expiry() {
    let store = InMemoryAdmissionStore::default();
    let job_id = Uuid::new_v4();
    attach(&store, owner(&store, None, "lease").await, job_id).await;
    let lease = store
        .claim_job(job_id, "worker-a", 30_000)
        .await
        .expect("claim must succeed")
        .expect("work must be ready");

    let mut stale = lease.clone();
    stale.lease_epoch += 1;
    assert!(matches!(
        store.start(&stale).await,
        Err(AdmissionError::StaleLease)
    ));
    stale = lease.clone();
    stale.execution_id = Uuid::new_v4();
    assert!(matches!(
        store.start(&stale).await,
        Err(AdmissionError::StaleLease)
    ));
    stale = lease.clone();
    stale.worker_id = "worker-b".to_string();
    assert!(matches!(
        store.start(&stale).await,
        Err(AdmissionError::StaleLease)
    ));

    store.start(&lease).await.expect("valid lease must start");
    store
        .heartbeat(&lease, 30_000)
        .await
        .expect("valid lease must heartbeat");
    store
        .settle(&lease, WorkOutcome::Succeeded, None)
        .await
        .expect("valid lease must settle");
    assert!(matches!(
        store.settle(&lease, WorkOutcome::Succeeded, None).await,
        Err(AdmissionError::StaleLease)
    ));

    let expiring_job = Uuid::new_v4();
    attach(
        &store,
        owner(&store, None, "expiring-lease").await,
        expiring_job,
    )
    .await;
    let expiring = store
        .claim_job(expiring_job, "worker-a", 1)
        .await
        .expect("claim must succeed")
        .expect("work must be ready");
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(matches!(
        store.start(&expiring).await,
        Err(AdmissionError::StaleLease)
    ));
}

#[tokio::test]
async fn expired_deadlines_are_rejected_and_receiving_sessions_are_aborted() {
    let store = InMemoryAdmissionStore::default();
    let mut expired = claim_request(Some("already-expired"), "hash");
    expired.deadline_at_ms = now_ms() - 1;
    assert!(matches!(
        store.claim(expired).await,
        Err(AdmissionError::Expired)
    ));

    let mut short = claim_request(Some("expires-before-attach"), "hash");
    short.deadline_at_ms = now_ms() + 5;
    let AdmissionClaim::Owner(ticket) = store.claim(short.clone()).await.unwrap() else {
        panic!("first claim must own the session");
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(matches!(
        store
            .attach(AttachJob {
                ticket,
                job_id: Uuid::new_v4(),
                command_schema: "image.generate.v1".to_string(),
                command_json: json!({"prompt": "too late"}),
                work_kind: "image.generate".to_string(),
            })
            .await,
        Err(AdmissionError::Expired)
    ));
    short.deadline_at_ms = now_ms() + 60_000;
    assert!(matches!(
        store
            .claim(short)
            .await
            .expect("aborted replay is a result"),
        AdmissionClaim::Owner(_)
    ));
}

#[tokio::test]
async fn terminal_key_replays_the_existing_job_and_outcome() {
    let store = InMemoryAdmissionStore::default();
    let request = claim_request(Some("terminal-key"), "terminal-hash");
    let AdmissionClaim::Owner(ticket) = store.claim(request.clone()).await.unwrap() else {
        panic!("first claim must own the session");
    };
    let job_id = Uuid::new_v4();
    attach(&store, ticket, job_id).await;
    let lease = store
        .claim_job(job_id, "worker-a", 30_000)
        .await
        .unwrap()
        .expect("work must be ready");
    store.start(&lease).await.unwrap();
    store
        .settle(&lease, WorkOutcome::Failed, Some("provider_error"))
        .await
        .unwrap();

    assert_eq!(
        store.claim(request).await.unwrap(),
        AdmissionClaim::Existing {
            job_id,
            state: "failed".to_string(),
        }
    );
}
