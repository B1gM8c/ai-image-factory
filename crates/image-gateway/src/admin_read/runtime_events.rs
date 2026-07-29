use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use sqlx::postgres::{PgListener, PgNotification};
use tokio::{sync::broadcast, time::MissedTickBehavior};
use uuid::Uuid;

use super::{
    AdminReadStore, ProviderAccountConcurrencySnapshot, ProviderAccountRuntimeEvent,
    ProviderQueuePressure,
};

pub(crate) const PROVIDER_ACCOUNT_RUNTIME_CHANNEL: &str =
    "ai_image_factory_provider_account_runtime";
const EVENT_CHANNEL_CAPACITY: usize = 256;
const COALESCE_WINDOW: Duration = Duration::from_millis(50);
const PERIODIC_RESYNC: Duration = Duration::from_secs(30);

pub struct ProviderAccountRuntimeEventHub {
    sender: broadcast::Sender<ProviderAccountRuntimeEvent>,
    sequence: AtomicU64,
}

impl ProviderAccountRuntimeEventHub {
    pub async fn connect(
        database_url: &str,
        store: Arc<dyn AdminReadStore>,
    ) -> Result<Arc<Self>, sqlx::Error> {
        let mut listener = PgListener::connect(database_url).await?;
        listener.listen(PROVIDER_ACCOUNT_RUNTIME_CHANNEL).await?;

        let (sender, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let hub = Arc::new(Self {
            sender,
            sequence: AtomicU64::new(0),
        });
        tokio::spawn(run_listener(listener, Arc::clone(&hub), store));
        Ok(hub)
    }

    pub fn subscribe(&self) -> (broadcast::Receiver<ProviderAccountRuntimeEvent>, u64) {
        let receiver = self.sender.subscribe();
        let sequence = self.sequence.load(Ordering::Acquire);
        (receiver, sequence)
    }

    pub fn snapshot_event(
        &self,
        sequence: u64,
        snapshot: ProviderAccountConcurrencySnapshot,
    ) -> ProviderAccountRuntimeEvent {
        ProviderAccountRuntimeEvent {
            kind: "snapshot".to_string(),
            sequence,
            as_of_ms: snapshot.as_of_ms,
            accounts: snapshot.accounts,
            queue: snapshot.queue,
        }
    }

    pub fn resync_required_event(&self) -> ProviderAccountRuntimeEvent {
        ProviderAccountRuntimeEvent {
            kind: "resync_required".to_string(),
            sequence: self.sequence.load(Ordering::Acquire),
            as_of_ms: 0,
            accounts: Vec::new(),
            queue: ProviderQueuePressure {
                queued_work_items: "0".to_string(),
                pending_batch_requests: "0".to_string(),
            },
        }
    }

    async fn publish(
        &self,
        store: &dyn AdminReadStore,
        provider_account_ids: Option<&[Uuid]>,
        kind: &'static str,
    ) {
        if self.sender.receiver_count() == 0 {
            return;
        }
        let snapshot = match store
            .provider_account_concurrency(provider_account_ids)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(?error, "provider account runtime event read failed");
                return;
            }
        };
        let sequence = self.sequence.fetch_add(1, Ordering::AcqRel) + 1;
        let _ = self.sender.send(ProviderAccountRuntimeEvent {
            kind: kind.to_string(),
            sequence,
            as_of_ms: snapshot.as_of_ms,
            accounts: snapshot.accounts,
            queue: snapshot.queue,
        });
    }
}

async fn run_listener(
    mut listener: PgListener,
    hub: Arc<ProviderAccountRuntimeEventHub>,
    store: Arc<dyn AdminReadStore>,
) {
    let mut periodic_resync = tokio::time::interval(PERIODIC_RESYNC);
    periodic_resync.set_missed_tick_behavior(MissedTickBehavior::Skip);
    periodic_resync.tick().await;

    loop {
        tokio::select! {
            notification = listener.recv() => {
                match notification {
                    Ok(first) => {
                        let notification_batch =
                            collect_notification_batch(&mut listener, first).await;
                        if notification_batch.full_snapshot {
                            hub.publish(store.as_ref(), None, "snapshot").await;
                        } else {
                            let mut account_ids =
                                notification_batch.account_ids.into_iter().collect::<Vec<_>>();
                            account_ids.sort_unstable();
                            hub.publish(store.as_ref(), Some(&account_ids), "delta").await;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(?error, "provider account runtime listener disconnected");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
            _ = periodic_resync.tick() => {
                hub.publish(store.as_ref(), None, "snapshot").await;
            }
        }
    }
}

async fn collect_notification_batch(
    listener: &mut PgListener,
    first: PgNotification,
) -> NotificationBatch {
    let mut batch = NotificationBatch::default();
    insert_notification(&mut batch, &first);
    let deadline = tokio::time::sleep(COALESCE_WINDOW);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            notification = listener.recv() => {
                match notification {
                    Ok(notification) => insert_notification(&mut batch, &notification),
                    Err(error) => {
                        tracing::warn!(?error, "provider account runtime batch interrupted");
                        break;
                    }
                }
            }
            _ = &mut deadline => break,
        }
    }
    batch
}

#[derive(Default)]
struct NotificationBatch {
    account_ids: HashSet<Uuid>,
    full_snapshot: bool,
}

fn insert_notification(batch: &mut NotificationBatch, notification: &PgNotification) {
    if notification.payload() == "*" {
        batch.full_snapshot = true;
        return;
    }
    match Uuid::parse_str(notification.payload()) {
        Ok(provider_account_id) => {
            batch.account_ids.insert(provider_account_id);
        }
        Err(error) => tracing::warn!(
            payload = notification.payload(),
            ?error,
            "ignored invalid provider account runtime notification"
        ),
    }
}
