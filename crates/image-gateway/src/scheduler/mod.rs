use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::ImageGatewayError;

#[derive(Debug)]
pub struct JobScheduler {
    semaphore: Arc<Semaphore>,
    queued: AtomicUsize,
    max_queue_size: usize,
    queue_timeout: Duration,
}

#[derive(Debug)]
pub struct TenantJobScheduler {
    global: Arc<JobScheduler>,
    tenants: Mutex<HashMap<String, Arc<JobScheduler>>>,
    max_concurrent_jobs_per_tenant: usize,
    max_queue_size_per_tenant: usize,
    queue_timeout: Duration,
}

pub struct TenantJobPermit {
    _tenant: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}

struct QueueGuard<'a> {
    queued: &'a AtomicUsize,
    active: bool,
}

impl<'a> QueueGuard<'a> {
    fn try_new(queued: &'a AtomicUsize, max_queue_size: usize) -> Option<Self> {
        loop {
            let current = queued.load(Ordering::SeqCst);
            if current >= max_queue_size {
                return None;
            }
            if queued
                .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Some(Self {
                    queued,
                    active: true,
                });
            }
        }
    }

    fn release(mut self) {
        self.active = false;
        self.queued.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Drop for QueueGuard<'_> {
    fn drop(&mut self) {
        if self.active {
            self.queued.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl JobScheduler {
    pub fn new(max_concurrent_jobs: usize, max_queue_size: usize, queue_timeout: Duration) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent_jobs.max(1))),
            queued: AtomicUsize::new(0),
            max_queue_size,
            queue_timeout,
        }
    }

    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, ImageGatewayError> {
        if let Ok(permit) = self.semaphore.clone().try_acquire_owned() {
            return Ok(permit);
        }

        let guard = QueueGuard::try_new(&self.queued, self.max_queue_size)
            .ok_or_else(ImageGatewayError::queue_overloaded)?;

        let permit =
            tokio::time::timeout(self.queue_timeout, self.semaphore.clone().acquire_owned())
                .await
                .map_err(|_| ImageGatewayError::queue_timeout())?
                .map_err(|_| ImageGatewayError::queue_overloaded());

        guard.release();
        permit
    }
}

impl TenantJobScheduler {
    pub fn new(
        max_concurrent_jobs: usize,
        max_queue_size: usize,
        max_concurrent_jobs_per_tenant: usize,
        max_queue_size_per_tenant: usize,
        queue_timeout: Duration,
    ) -> Self {
        Self {
            global: Arc::new(JobScheduler::new(
                max_concurrent_jobs,
                max_queue_size,
                queue_timeout,
            )),
            tenants: Mutex::new(HashMap::new()),
            max_concurrent_jobs_per_tenant,
            max_queue_size_per_tenant,
            queue_timeout,
        }
    }

    pub async fn acquire(&self, tenant_id: &str) -> Result<TenantJobPermit, ImageGatewayError> {
        let tenant = {
            let mut tenants = self
                .tenants
                .lock()
                .map_err(|_| ImageGatewayError::internal("tenant scheduler lock poisoned"))?;
            tenants
                .entry(tenant_id.to_string())
                .or_insert_with(|| {
                    Arc::new(JobScheduler::new(
                        self.max_concurrent_jobs_per_tenant,
                        self.max_queue_size_per_tenant,
                        self.queue_timeout,
                    ))
                })
                .clone()
        };

        let tenant_permit = tenant.acquire().await?;
        let global_permit = self.global.acquire().await?;
        Ok(TenantJobPermit {
            _tenant: tenant_permit,
            _global: global_permit,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn canceled_waiter_releases_queue_slot() {
        let scheduler = Arc::new(JobScheduler::new(1, 1, Duration::from_secs(10)));
        let _held = scheduler.acquire().await.unwrap();

        let waiting_scheduler = scheduler.clone();
        let waiter = tokio::spawn(async move { waiting_scheduler.acquire().await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        waiter.abort();
        let _ = waiter.await;

        assert_eq!(scheduler.queued.load(Ordering::SeqCst), 0);
    }
}
