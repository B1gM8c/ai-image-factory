use std::{future::Future, time::Duration};

use tokio::{
    sync::watch,
    time::{Instant, MissedTickBehavior},
};

use super::{
    MAX_RUNTIME_LEASE_MS, ProviderRuntimeLease, ProviderRuntimeReadinessStore,
    ProviderRuntimeRegistration, ProviderTaskStoreError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderRuntimeSupervisorConfig {
    pub lease_ms: i64,
    pub heartbeat_interval: Duration,
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderRuntimeSupervisorError<E> {
    #[error("provider runtime supervisor configuration is invalid")]
    InvalidConfiguration,
    #[error("provider runtime registration failed: {0}")]
    Registration(ProviderTaskStoreError),
    #[error("provider runtime heartbeat failed: {0}")]
    Heartbeat(ProviderTaskStoreError),
    #[error("provider runtime drain transition failed: {0}")]
    Drain(ProviderTaskStoreError),
    #[error("provider runtime withdrawal failed: {0}")]
    Withdraw(ProviderTaskStoreError),
    #[error("provider runtime failed: {0}")]
    Runtime(E),
}

pub struct ProviderRuntimeShutdown {
    receiver: watch::Receiver<bool>,
}

impl ProviderRuntimeShutdown {
    pub async fn wait(mut self) {
        if *self.receiver.borrow() {
            return;
        }
        while self.receiver.changed().await.is_ok() {
            if *self.receiver.borrow_and_update() {
                return;
            }
        }
    }
}

pub struct ProviderRuntimeSupervisor<S> {
    store: S,
    registration: ProviderRuntimeRegistration,
    config: ProviderRuntimeSupervisorConfig,
}

impl<S> ProviderRuntimeSupervisor<S>
where
    S: ProviderRuntimeReadinessStore,
{
    pub fn new(
        store: S,
        registration: ProviderRuntimeRegistration,
        config: ProviderRuntimeSupervisorConfig,
    ) -> Self {
        Self {
            store,
            registration,
            config,
        }
    }

    pub async fn run_until_shutdown<Shutdown, Run, Runtime, Output, RuntimeError>(
        &self,
        shutdown: Shutdown,
        run: Run,
    ) -> Result<Output, ProviderRuntimeSupervisorError<RuntimeError>>
    where
        Shutdown: Future<Output = ()>,
        Run: FnOnce(ProviderRuntimeShutdown) -> Runtime,
        Runtime: Future<Output = Result<Output, RuntimeError>>,
    {
        validate_config(self.config)
            .map_err(|()| ProviderRuntimeSupervisorError::InvalidConfiguration)?;
        let mut lease = self
            .bounded_store_call(
                self.store
                    .register_runtime(&self.registration, self.config.lease_ms),
            )
            .await
            .map_err(ProviderRuntimeSupervisorError::Registration)?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runtime = run(ProviderRuntimeShutdown {
            receiver: shutdown_rx,
        });
        tokio::pin!(runtime);
        tokio::pin!(shutdown);
        let first_tick = Instant::now() + self.config.heartbeat_interval;
        let mut heartbeat = tokio::time::interval_at(first_tick, self.config.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        enum Trigger<T, E> {
            Runtime(Result<T, E>),
            Shutdown,
            Heartbeat(ProviderTaskStoreError),
        }

        let trigger = loop {
            tokio::select! {
                biased;
                result = &mut runtime => break Trigger::Runtime(result),
                _ = &mut shutdown => break Trigger::Shutdown,
                _ = heartbeat.tick() => {
                    let renewal_result = {
                        let renewal = self.bounded_store_call(
                            self.store.heartbeat_runtime(&lease, self.config.lease_ms),
                        );
                        tokio::pin!(renewal);
                        tokio::select! {
                            biased;
                            result = &mut runtime => break Trigger::Runtime(result),
                            _ = &mut shutdown => break Trigger::Shutdown,
                            result = &mut renewal => result,
                        }
                    };
                    match renewal_result {
                        Ok(next) => lease = next,
                        Err(error) => break Trigger::Heartbeat(error),
                    }
                }
            }
        };

        match trigger {
            Trigger::Runtime(result) => self.finish_completed_runtime(lease, result).await,
            Trigger::Shutdown => {
                let drain = self.bounded_store_call(
                    self.store.begin_runtime_drain(&lease, self.config.lease_ms),
                );
                let (next, failure) = match drain.await {
                    Ok(next) => (Some(next), None),
                    Err(error) => (None, Some(LeaseFailure::Drain(error))),
                };
                if let Some(next) = next {
                    lease = next;
                }
                let can_heartbeat = failure.is_none();
                let _ = shutdown_tx.send(true);
                self.finish_stopping_runtime(
                    lease,
                    &mut runtime,
                    &mut heartbeat,
                    failure,
                    can_heartbeat,
                )
                .await
            }
            Trigger::Heartbeat(error) => {
                let mut can_heartbeat = false;
                let drain = self.bounded_store_call(
                    self.store.begin_runtime_drain(&lease, self.config.lease_ms),
                );
                if let Ok(next) = drain.await {
                    lease = next;
                    can_heartbeat = true;
                }
                let _ = shutdown_tx.send(true);
                self.finish_stopping_runtime(
                    lease,
                    &mut runtime,
                    &mut heartbeat,
                    Some(LeaseFailure::Heartbeat(error)),
                    can_heartbeat,
                )
                .await
            }
        }
    }

    async fn finish_completed_runtime<Output, RuntimeError>(
        &self,
        mut lease: ProviderRuntimeLease,
        result: Result<Output, RuntimeError>,
    ) -> Result<Output, ProviderRuntimeSupervisorError<RuntimeError>> {
        let drain = self
            .bounded_store_call(self.store.begin_runtime_drain(&lease, self.config.lease_ms))
            .await;
        if let Ok(next) = drain.as_ref() {
            lease = next.clone();
        }
        let withdraw = self
            .bounded_store_call(self.store.withdraw_runtime(&lease))
            .await;
        match result {
            Err(error) => Err(ProviderRuntimeSupervisorError::Runtime(error)),
            Ok(output) => match drain {
                Err(error) => Err(ProviderRuntimeSupervisorError::Drain(error)),
                Ok(_) => match withdraw {
                    Err(error) => Err(ProviderRuntimeSupervisorError::Withdraw(error)),
                    Ok(()) => Ok(output),
                },
            },
        }
    }

    async fn finish_stopping_runtime<Runtime, Output, RuntimeError>(
        &self,
        mut lease: ProviderRuntimeLease,
        runtime: &mut std::pin::Pin<&mut Runtime>,
        heartbeat: &mut tokio::time::Interval,
        mut failure: Option<LeaseFailure>,
        mut can_heartbeat: bool,
    ) -> Result<Output, ProviderRuntimeSupervisorError<RuntimeError>>
    where
        Runtime: Future<Output = Result<Output, RuntimeError>>,
    {
        let result = loop {
            tokio::select! {
                biased;
                result = &mut *runtime => break result,
                _ = heartbeat.tick(), if can_heartbeat => {
                    let renewal_result = {
                        let renewal = self.bounded_store_call(
                            self.store.heartbeat_runtime(&lease, self.config.lease_ms),
                        );
                        tokio::pin!(renewal);
                        tokio::select! {
                            biased;
                            result = &mut *runtime => break result,
                            result = &mut renewal => result,
                        }
                    };
                    match renewal_result {
                        Ok(next) => lease = next,
                        Err(error) => {
                            can_heartbeat = false;
                            if failure.is_none() {
                                failure = Some(LeaseFailure::Heartbeat(error));
                            }
                        }
                    }
                }
            }
        };
        let withdraw = self
            .bounded_store_call(self.store.withdraw_runtime(&lease))
            .await;
        match result {
            Err(error) => Err(ProviderRuntimeSupervisorError::Runtime(error)),
            Ok(output) => match failure {
                Some(LeaseFailure::Heartbeat(error)) => {
                    Err(ProviderRuntimeSupervisorError::Heartbeat(error))
                }
                Some(LeaseFailure::Drain(error)) => {
                    Err(ProviderRuntimeSupervisorError::Drain(error))
                }
                None => match withdraw {
                    Err(error) => Err(ProviderRuntimeSupervisorError::Withdraw(error)),
                    Ok(()) => Ok(output),
                },
            },
        }
    }

    async fn bounded_store_call<T>(
        &self,
        call: impl Future<Output = Result<T, ProviderTaskStoreError>>,
    ) -> Result<T, ProviderTaskStoreError> {
        tokio::time::timeout(self.config.heartbeat_interval, call)
            .await
            .unwrap_or(Err(ProviderTaskStoreError::Unavailable))
    }
}

enum LeaseFailure {
    Heartbeat(ProviderTaskStoreError),
    Drain(ProviderTaskStoreError),
}

fn validate_config(config: ProviderRuntimeSupervisorConfig) -> Result<(), ()> {
    let heartbeat_ms = i64::try_from(config.heartbeat_interval.as_millis()).map_err(|_| ())?;
    if !(1..=MAX_RUNTIME_LEASE_MS).contains(&config.lease_ms)
        || heartbeat_ms <= 0
        || heartbeat_ms > MAX_RUNTIME_LEASE_MS
        || heartbeat_ms.saturating_mul(3) > config.lease_ms
    {
        return Err(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use tokio::sync::{Semaphore, oneshot};
    use uuid::Uuid;

    use super::*;
    use crate::provider_tasks::{
        ProviderProfileReadiness, ProviderRuntimeLeaseState, ProviderRuntimeRole,
    };

    #[derive(Clone, Default)]
    struct FakeStore {
        events: Arc<Mutex<Vec<&'static str>>>,
        fail_heartbeat: Arc<AtomicBool>,
        hang_heartbeat: Arc<AtomicBool>,
    }

    impl FakeStore {
        fn events(&self) -> Vec<&'static str> {
            self.events.lock().unwrap().clone()
        }

        fn record(&self, event: &'static str) {
            self.events.lock().unwrap().push(event);
        }
    }

    #[async_trait]
    impl ProviderRuntimeReadinessStore for FakeStore {
        async fn register_runtime(
            &self,
            registration: &ProviderRuntimeRegistration,
            lease_ms: i64,
        ) -> Result<ProviderRuntimeLease, ProviderTaskStoreError> {
            self.record("register");
            Ok(ProviderRuntimeLease {
                runtime_id: registration.runtime_id,
                execution_profile_id: registration.execution_profile_id,
                role: registration.role,
                runtime_owner: registration.runtime_owner.clone(),
                state: ProviderRuntimeLeaseState::Active,
                heartbeat_at_ms: 1,
                lease_expires_at_ms: 1 + lease_ms,
            })
        }

        async fn heartbeat_runtime(
            &self,
            lease: &ProviderRuntimeLease,
            lease_ms: i64,
        ) -> Result<ProviderRuntimeLease, ProviderTaskStoreError> {
            self.record("heartbeat");
            if self.hang_heartbeat.load(Ordering::SeqCst) {
                return std::future::pending().await;
            }
            if self.fail_heartbeat.load(Ordering::SeqCst) {
                return Err(ProviderTaskStoreError::Unavailable);
            }
            let mut next = lease.clone();
            next.heartbeat_at_ms += 1;
            next.lease_expires_at_ms = next.heartbeat_at_ms + lease_ms;
            Ok(next)
        }

        async fn begin_runtime_drain(
            &self,
            lease: &ProviderRuntimeLease,
            lease_ms: i64,
        ) -> Result<ProviderRuntimeLease, ProviderTaskStoreError> {
            self.record("drain");
            let mut next = lease.clone();
            next.state = ProviderRuntimeLeaseState::Draining;
            next.heartbeat_at_ms += 1;
            next.lease_expires_at_ms = next.heartbeat_at_ms + lease_ms;
            Ok(next)
        }

        async fn withdraw_runtime(
            &self,
            _lease: &ProviderRuntimeLease,
        ) -> Result<(), ProviderTaskStoreError> {
            self.record("withdraw");
            Ok(())
        }

        async fn list_profile_readiness(
            &self,
        ) -> Result<Vec<ProviderProfileReadiness>, ProviderTaskStoreError> {
            unreachable!("supervisor does not read aggregate readiness")
        }
    }

    #[derive(Debug, Eq, PartialEq, thiserror::Error)]
    #[error("test runtime failed")]
    struct TestRuntimeError;

    fn registration() -> ProviderRuntimeRegistration {
        ProviderRuntimeRegistration {
            runtime_id: Uuid::new_v4(),
            execution_profile_id: Uuid::new_v4(),
            role: ProviderRuntimeRole::Submit,
            runtime_owner: "provider-runtime-supervisor-test".to_owned(),
        }
    }

    fn config() -> ProviderRuntimeSupervisorConfig {
        ProviderRuntimeSupervisorConfig {
            lease_ms: 100,
            heartbeat_interval: Duration::from_millis(10),
        }
    }

    #[tokio::test]
    async fn external_shutdown_drains_before_signalling_and_heartbeats_until_withdrawal() {
        let store = FakeStore::default();
        let supervisor = ProviderRuntimeSupervisor::new(store.clone(), registration(), config());
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let run = tokio::spawn({
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                supervisor
                    .run_until_shutdown(
                        async {
                            let _ = shutdown_rx.await;
                        },
                        |shutdown| async move {
                            started.add_permits(1);
                            shutdown.wait().await;
                            release
                                .acquire()
                                .await
                                .expect("release supervised runtime")
                                .forget();
                            Ok::<(), TestRuntimeError>(())
                        },
                    )
                    .await
            }
        });

        started.acquire().await.expect("runtime started").forget();
        shutdown_tx.send(()).expect("signal supervisor");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let events = store.events();
                let drain = events.iter().position(|event| *event == "drain");
                let heartbeat = events.iter().rposition(|event| *event == "heartbeat");
                if drain
                    .zip(heartbeat)
                    .is_some_and(|(drain, heartbeat)| drain < heartbeat)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("draining runtime heartbeat");
        release.add_permits(1);

        assert_eq!(run.await.expect("supervisor task"), Ok(()));
        let events = store.events();
        assert_eq!(events.first(), Some(&"register"));
        assert_eq!(events.last(), Some(&"withdraw"));
        let drain = events.iter().position(|event| *event == "drain").unwrap();
        let heartbeat = events
            .iter()
            .rposition(|event| *event == "heartbeat")
            .unwrap();
        assert!(drain < heartbeat);
    }

    #[tokio::test]
    async fn heartbeat_loss_stops_the_runtime_and_is_reported() {
        let store = FakeStore::default();
        store.fail_heartbeat.store(true, Ordering::SeqCst);
        let supervisor = ProviderRuntimeSupervisor::new(store.clone(), registration(), config());
        let stopped = Arc::new(AtomicBool::new(false));

        let result = supervisor
            .run_until_shutdown(std::future::pending(), {
                let stopped = Arc::clone(&stopped);
                |shutdown| async move {
                    shutdown.wait().await;
                    stopped.store(true, Ordering::SeqCst);
                    Ok::<(), TestRuntimeError>(())
                }
            })
            .await;

        assert_eq!(
            result,
            Err(ProviderRuntimeSupervisorError::Heartbeat(
                ProviderTaskStoreError::Unavailable
            ))
        );
        assert!(stopped.load(Ordering::SeqCst));
        assert_eq!(
            store.events(),
            vec!["register", "heartbeat", "drain", "withdraw"]
        );
    }

    #[tokio::test]
    async fn heartbeat_timeout_stops_the_runtime_without_blocking_forever() {
        let store = FakeStore::default();
        store.hang_heartbeat.store(true, Ordering::SeqCst);
        let supervisor = ProviderRuntimeSupervisor::new(
            store.clone(),
            registration(),
            ProviderRuntimeSupervisorConfig {
                lease_ms: 30,
                heartbeat_interval: Duration::from_millis(5),
            },
        );
        let stopped = Arc::new(AtomicBool::new(false));

        let result = tokio::time::timeout(
            Duration::from_millis(100),
            supervisor.run_until_shutdown(std::future::pending(), {
                let stopped = Arc::clone(&stopped);
                |shutdown| async move {
                    shutdown.wait().await;
                    stopped.store(true, Ordering::SeqCst);
                    Ok::<(), TestRuntimeError>(())
                }
            }),
        )
        .await
        .expect("bounded supervisor heartbeat");

        assert_eq!(
            result,
            Err(ProviderRuntimeSupervisorError::Heartbeat(
                ProviderTaskStoreError::Unavailable
            ))
        );
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn invalid_config_fails_before_registration() {
        let store = FakeStore::default();
        let supervisor = ProviderRuntimeSupervisor::new(
            store.clone(),
            registration(),
            ProviderRuntimeSupervisorConfig {
                lease_ms: 20,
                heartbeat_interval: Duration::from_millis(10),
            },
        );

        let result = supervisor
            .run_until_shutdown(std::future::pending(), |_| async {
                Ok::<(), TestRuntimeError>(())
            })
            .await;

        assert_eq!(
            result,
            Err(ProviderRuntimeSupervisorError::InvalidConfiguration)
        );
        assert!(store.events().is_empty());
    }
}
