use std::{future::Future, time::Duration};

use crate::providers::dreamina_cli::{
    ProviderCapacityQueryDriver, ProviderCapacityQueryObservation,
};

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderCapacityReconciliationRun {
    Idle,
    Classified(u64),
    Pending,
    Released,
    Deferred,
}

/// A single query-only lane. It cannot submit, replay, cancel or settle a job.
pub struct ProviderCapacityReconciliationWorker<D> {
    store: PostgresProviderTaskStore,
    driver: D,
    scope: ProviderTaskClaimScope,
    owner: String,
    lease_ms: i64,
    heartbeat_interval: Duration,
}

impl<D: ProviderCapacityQueryDriver> ProviderCapacityReconciliationWorker<D> {
    pub fn new(
        store: PostgresProviderTaskStore,
        driver: D,
        scope: ProviderTaskClaimScope,
        owner: String,
        lease_ms: i64,
        heartbeat_interval: Duration,
    ) -> Result<Self, ProviderTaskStoreError> {
        validate_scope(&scope)?;
        if owner.trim().is_empty()
            || !(1..=MAX_LEASE_MS).contains(&lease_ms)
            || heartbeat_interval.is_zero()
            || heartbeat_interval.as_millis() >= lease_ms as u128 / 2
        {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        Ok(Self {
            store,
            driver,
            scope,
            owner,
            lease_ms,
            heartbeat_interval,
        })
    }

    pub async fn run_once(
        &self,
    ) -> Result<ProviderCapacityReconciliationRun, ProviderTaskStoreError> {
        let classified = self
            .store
            .classify_capacity_reconciliation(&self.scope)
            .await?;
        if classified > 0 {
            return Ok(ProviderCapacityReconciliationRun::Classified(classified));
        }
        let Some(mut lease) = self
            .store
            .claim_due_capacity_reconciliation(
                &self.scope,
                &self.owner,
                &Uuid::new_v4().to_string(),
                self.lease_ms,
            )
            .await?
        else {
            return Ok(ProviderCapacityReconciliationRun::Idle);
        };
        // A new unknown row can arrive between classification and claim. Never
        // pass it to the CLI, nor infer NoEffect from the missing receipt.
        if lease.remote_operation_id().is_none() {
            self.store.defer_capacity_query(&lease, 1, 0).await?;
            let classified = self
                .store
                .classify_capacity_reconciliation(&self.scope)
                .await?;
            return Ok(ProviderCapacityReconciliationRun::Classified(classified));
        }
        let query_lease = lease.clone();
        let query = self.driver.query(&query_lease);
        tokio::pin!(query);
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + self.heartbeat_interval,
            self.heartbeat_interval,
        );
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let observation = loop {
            tokio::select! {
                result = &mut query => break result,
                _ = heartbeat.tick() => {
                    lease = self.store.heartbeat_capacity_reconciliation(&lease, self.lease_ms).await?;
                }
            }
        };
        match observation {
            Ok(ProviderCapacityQueryObservation::Pending {
                next_poll_after_ms, ..
            }) => {
                let delay = next_poll_after_ms.clamp(1_000, MAX_RETRY_AFTER_MS as u64) as i64;
                self.store.defer_capacity_query(&lease, delay, 0).await?;
                Ok(ProviderCapacityReconciliationRun::Pending)
            }
            Ok(ProviderCapacityQueryObservation::Terminal {
                terminal_state,
                event_identity,
            }) => {
                self.store
                    .record_capacity_evidence(
                        &lease,
                        &ProviderCapacityEvidence {
                            event_identity,
                            outcome: ProviderCapacityEvidenceOutcome::RemoteTerminal {
                                remote_operation_id: lease
                                    .remote_operation_id()
                                    .ok_or(ProviderTaskStoreError::InvalidInput)?
                                    .to_owned(),
                                terminal_state,
                            },
                        },
                    )
                    .await?;
                Ok(ProviderCapacityReconciliationRun::Released)
            }
            Err(error) => {
                tracing::warn!(code = error.code(), "capacity original query deferred");
                self.store
                    .defer_capacity_query(&lease, 30_000, if error.is_retryable() { 1 } else { 3 })
                    .await?;
                Ok(ProviderCapacityReconciliationRun::Deferred)
            }
        }
    }

    /// Stop taking new work, then bound the active query/DB transaction drain.
    /// Dropping a timed-out future rolls back its transaction and terminates only
    /// its query process; it cannot assert terminal or no-effect evidence.
    pub async fn run_until_shutdown<S: Future<Output = ()>>(
        &self,
        shutdown: S,
        drain_timeout: Duration,
    ) {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
            }
            let iteration = tokio::time::timeout(
                Duration::from_millis(self.lease_ms as u64).saturating_mul(2),
                self.run_once(),
            );
            tokio::pin!(iteration);
            let result = tokio::select! {
                biased;
                _ = &mut shutdown => {
                    let _ = tokio::time::timeout(drain_timeout, &mut iteration).await;
                    break;
                },
                result = &mut iteration => result,
            };
            if !matches!(result, Ok(Ok(_))) {
                tracing::warn!("capacity reconciliation iteration deferred or timed out");
                tokio::select! {
                    _ = &mut shutdown => break,
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                }
            }
        }
    }
}
