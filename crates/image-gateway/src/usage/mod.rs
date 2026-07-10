use std::{
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    jobs::{JobState, ReservationState},
};

const FIVE_HOURS_MS: i64 = 5 * 60 * 60 * 1000;
const SEVEN_DAYS_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const RESERVATION_TTL_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Clone, Debug)]
pub struct UsageLimits {
    pub five_hour_image_limit: u32,
    pub seven_day_image_limit: u32,
}

#[derive(Clone, Debug)]
pub struct UsageCharge {
    pub tenant_id: String,
    pub request_id: String,
    pub operation: &'static str,
    pub units: u32,
    pub limits: UsageLimits,
}

#[derive(Clone, Debug)]
pub struct UsageReservation {
    pub reservation_id: Uuid,
    pub job_id: Uuid,
    pub charge: UsageCharge,
    pub snapshot: UsageSnapshot,
}

#[derive(Clone, Debug)]
pub struct UsageSnapshot {
    pub limit_5h: u32,
    pub remaining_5h: u32,
    pub limit_7d: u32,
    pub remaining_7d: u32,
}

#[async_trait]
pub trait UsageStore: Send + Sync + 'static {
    async fn reserve(&self, charge: UsageCharge) -> Result<UsageReservation, ImageGatewayError>;

    async fn commit(
        &self,
        reservation: &UsageReservation,
    ) -> Result<UsageSnapshot, ImageGatewayError>;

    async fn release(
        &self,
        reservation: &UsageReservation,
        reason: &'static str,
    ) -> Result<(), ImageGatewayError>;

    async fn charge(&self, charge: UsageCharge) -> Result<UsageSnapshot, ImageGatewayError> {
        let reservation = self.reserve(charge).await?;
        self.commit(&reservation).await
    }
}

#[derive(Default)]
pub struct InMemoryUsageStore {
    state: Mutex<InMemoryUsageState>,
}

#[derive(Default)]
struct InMemoryUsageState {
    events: Vec<UsageEvent>,
    reservations: Vec<UsageReservationRecord>,
    jobs: Vec<JobRecord>,
    metering_events: Vec<MeteringEvent>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct UsageEvent {
    tenant_id: String,
    created_at_ms: i64,
    units: u32,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct UsageReservationRecord {
    reservation_id: Uuid,
    job_id: Uuid,
    tenant_id: String,
    request_id: String,
    operation: &'static str,
    requested_units: u32,
    committed_units: u32,
    released_units: u32,
    state: ReservationState,
    created_at_ms: i64,
    updated_at_ms: i64,
    expires_at_ms: i64,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct JobRecord {
    job_id: Uuid,
    tenant_id: String,
    request_id: String,
    operation: &'static str,
    state: JobState,
    requested_units: u32,
    charged_units: u32,
    reservation_id: Uuid,
    created_at_ms: i64,
    updated_at_ms: i64,
    finished_at_ms: Option<i64>,
    last_error_code: Option<&'static str>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct MeteringEvent {
    event_id: Uuid,
    tenant_id: String,
    job_id: Uuid,
    reservation_id: Uuid,
    request_id: String,
    operation: &'static str,
    event_type: &'static str,
    units: u32,
    outcome: &'static str,
    created_at_ms: i64,
}

#[async_trait]
impl UsageStore for InMemoryUsageStore {
    async fn reserve(&self, charge: UsageCharge) -> Result<UsageReservation, ImageGatewayError> {
        let now = now_ms();
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("usage store lock poisoned"))?;
        prune_usage_state(&mut state, now);

        let (five_used, seven_used) = used_units_for_tenant(&state, &charge.tenant_id, now);
        let snapshot = ensure_quota(&charge, five_used, seven_used)?;
        let reservation_id = Uuid::new_v4();
        let job_id = Uuid::new_v4();

        state.jobs.push(JobRecord {
            job_id,
            tenant_id: charge.tenant_id.clone(),
            request_id: charge.request_id.clone(),
            operation: charge.operation,
            state: JobState::Reserved,
            requested_units: charge.units,
            charged_units: 0,
            reservation_id,
            created_at_ms: now,
            updated_at_ms: now,
            finished_at_ms: None,
            last_error_code: None,
        });
        state.reservations.push(UsageReservationRecord {
            reservation_id,
            job_id,
            tenant_id: charge.tenant_id.clone(),
            request_id: charge.request_id.clone(),
            operation: charge.operation,
            requested_units: charge.units,
            committed_units: 0,
            released_units: 0,
            state: ReservationState::Reserved,
            created_at_ms: now,
            updated_at_ms: now,
            expires_at_ms: now + RESERVATION_TTL_MS,
        });
        state.metering_events.push(MeteringEvent {
            event_id: Uuid::new_v4(),
            tenant_id: charge.tenant_id.clone(),
            job_id,
            reservation_id,
            request_id: charge.request_id.clone(),
            operation: charge.operation,
            event_type: "quota_reserved",
            units: charge.units,
            outcome: "reserved",
            created_at_ms: now,
        });

        Ok(UsageReservation {
            reservation_id,
            job_id,
            charge,
            snapshot,
        })
    }

    async fn commit(
        &self,
        reservation: &UsageReservation,
    ) -> Result<UsageSnapshot, ImageGatewayError> {
        let now = now_ms();
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("usage store lock poisoned"))?;

        let Some(record) = state
            .reservations
            .iter_mut()
            .find(|record| record.reservation_id == reservation.reservation_id)
        else {
            return Err(ImageGatewayError::internal("reservation not found"));
        };

        match record.state {
            ReservationState::Committed => return Ok(reservation.snapshot.clone()),
            ReservationState::Reserved => {}
            ReservationState::Released | ReservationState::Expired => {
                return Err(ImageGatewayError::internal("reservation is not active"));
            }
        }

        record.state = ReservationState::Committed;
        record.committed_units = reservation.charge.units;
        record.updated_at_ms = now;
        state.events.push(UsageEvent {
            tenant_id: reservation.charge.tenant_id.clone(),
            created_at_ms: now,
            units: reservation.charge.units,
        });
        if let Some(job) = state
            .jobs
            .iter_mut()
            .find(|job| job.job_id == reservation.job_id)
        {
            job.state = JobState::Succeeded;
            job.charged_units = reservation.charge.units;
            job.updated_at_ms = now;
            job.finished_at_ms = Some(now);
        }
        state.metering_events.push(MeteringEvent {
            event_id: Uuid::new_v4(),
            tenant_id: reservation.charge.tenant_id.clone(),
            job_id: reservation.job_id,
            reservation_id: reservation.reservation_id,
            request_id: reservation.charge.request_id.clone(),
            operation: reservation.charge.operation,
            event_type: "quota_committed",
            units: reservation.charge.units,
            outcome: "succeeded",
            created_at_ms: now,
        });
        state.metering_events.push(MeteringEvent {
            event_id: Uuid::new_v4(),
            tenant_id: reservation.charge.tenant_id.clone(),
            job_id: reservation.job_id,
            reservation_id: reservation.reservation_id,
            request_id: reservation.charge.request_id.clone(),
            operation: reservation.charge.operation,
            event_type: "job_succeeded",
            units: reservation.charge.units,
            outcome: "succeeded",
            created_at_ms: now,
        });

        Ok(reservation.snapshot.clone())
    }

    async fn release(
        &self,
        reservation: &UsageReservation,
        reason: &'static str,
    ) -> Result<(), ImageGatewayError> {
        let now = now_ms();
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("usage store lock poisoned"))?;

        let Some(record) = state
            .reservations
            .iter_mut()
            .find(|record| record.reservation_id == reservation.reservation_id)
        else {
            return Err(ImageGatewayError::internal("reservation not found"));
        };

        match record.state {
            ReservationState::Released | ReservationState::Expired => return Ok(()),
            ReservationState::Committed => return Ok(()),
            ReservationState::Reserved => {}
        }

        record.state = ReservationState::Released;
        record.released_units = reservation.charge.units;
        record.updated_at_ms = now;
        if let Some(job) = state
            .jobs
            .iter_mut()
            .find(|job| job.job_id == reservation.job_id)
        {
            job.state = JobState::Failed;
            job.updated_at_ms = now;
            job.finished_at_ms = Some(now);
            job.last_error_code = Some(reason);
        }
        state.metering_events.push(MeteringEvent {
            event_id: Uuid::new_v4(),
            tenant_id: reservation.charge.tenant_id.clone(),
            job_id: reservation.job_id,
            reservation_id: reservation.reservation_id,
            request_id: reservation.charge.request_id.clone(),
            operation: reservation.charge.operation,
            event_type: "quota_released",
            units: reservation.charge.units,
            outcome: reason,
            created_at_ms: now,
        });
        state.metering_events.push(MeteringEvent {
            event_id: Uuid::new_v4(),
            tenant_id: reservation.charge.tenant_id.clone(),
            job_id: reservation.job_id,
            reservation_id: reservation.reservation_id,
            request_id: reservation.charge.request_id.clone(),
            operation: reservation.charge.operation,
            event_type: "job_failed",
            units: reservation.charge.units,
            outcome: reason,
            created_at_ms: now,
        });

        Ok(())
    }
}

#[derive(Clone)]
pub struct PostgresUsageStore {
    pool: Arc<PgPool>,
}

impl PostgresUsageStore {
    pub async fn connect(database_url: &str) -> Result<Self, ImageGatewayError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("PostgreSQL is unavailable"))?;
        let store = Self {
            pool: Arc::new(pool),
        };
        store.migrate().await?;
        Ok(store)
    }

    pub async fn migrate(&self) -> Result<(), ImageGatewayError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS usage_events (
                event_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL DEFAULT 'tenant_default',
                request_id TEXT NOT NULL,
                operation TEXT NOT NULL,
                units INTEGER NOT NULL CHECK (units > 0),
                outcome TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL
            )
            "#,
        )
        .execute(&*self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("failed to migrate usage_events"))?;

        sqlx::query(
            "ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'tenant_default'",
        )
        .execute(&*self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("failed to migrate usage tenant"))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS usage_events_tenant_created_at_ms_idx
            ON usage_events (tenant_id, created_at_ms)
            "#,
        )
        .execute(&*self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("failed to migrate usage index"))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS quota_reservations (
                reservation_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL DEFAULT 'tenant_default',
                request_id TEXT NOT NULL,
                job_id UUID,
                requested_units INTEGER NOT NULL CHECK (requested_units > 0),
                committed_units INTEGER NOT NULL DEFAULT 0,
                started_units INTEGER NOT NULL DEFAULT 0,
                released_units INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                expires_at_ms BIGINT NOT NULL
            )
            "#,
        )
        .execute(&*self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("failed to migrate reservations"))?;

        for statement in [
            "ALTER TABLE quota_reservations ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'tenant_default'",
            "ALTER TABLE quota_reservations ADD COLUMN IF NOT EXISTS job_id UUID",
            "ALTER TABLE quota_reservations ADD COLUMN IF NOT EXISTS committed_units INTEGER NOT NULL DEFAULT 0",
        ] {
            sqlx::query(statement)
                .execute(&*self.pool)
                .await
                .map_err(|_| {
                    ImageGatewayError::service_unavailable("failed to migrate reservation columns")
                })?;
        }

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS quota_reservations_active_tenant_idx
            ON quota_reservations (tenant_id, state, expires_at_ms)
            "#,
        )
        .execute(&*self.pool)
        .await
        .map_err(|_| {
            ImageGatewayError::service_unavailable("failed to migrate reservation index")
        })?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
                job_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL DEFAULT 'tenant_default',
                request_id TEXT NOT NULL,
                operation TEXT NOT NULL DEFAULT 'generation',
                provider_id TEXT NOT NULL DEFAULT 'openai-codex',
                model TEXT NOT NULL DEFAULT 'gpt-image-2',
                state TEXT NOT NULL,
                requested_units INTEGER NOT NULL,
                charged_units INTEGER NOT NULL DEFAULT 0,
                reservation_id UUID,
                queue_entered_at_ms BIGINT,
                started_at_ms BIGINT,
                finished_at_ms BIGINT,
                created_at_ms BIGINT NOT NULL DEFAULT 0,
                updated_at_ms BIGINT NOT NULL DEFAULT 0,
                last_error_code TEXT,
                last_error_message TEXT
            )
            "#,
        )
        .execute(&*self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("failed to migrate jobs"))?;

        for statement in [
            "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'tenant_default'",
            "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS operation TEXT NOT NULL DEFAULT 'generation'",
            "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS provider_id TEXT NOT NULL DEFAULT 'openai-codex'",
            "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS model TEXT NOT NULL DEFAULT 'gpt-image-2'",
            "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS reservation_id UUID",
            "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS created_at_ms BIGINT NOT NULL DEFAULT 0",
            "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS updated_at_ms BIGINT NOT NULL DEFAULT 0",
            "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_error_code TEXT",
            "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_error_message TEXT",
        ] {
            sqlx::query(statement)
                .execute(&*self.pool)
                .await
                .map_err(|_| {
                    ImageGatewayError::service_unavailable("failed to migrate job columns")
                })?;
        }

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS jobs_tenant_state_created_idx
            ON jobs (tenant_id, state, created_at_ms)
            "#,
        )
        .execute(&*self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("failed to migrate job index"))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS metering_events (
                event_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                job_id UUID,
                reservation_id UUID,
                request_id TEXT NOT NULL,
                operation TEXT NOT NULL,
                event_type TEXT NOT NULL,
                units INTEGER NOT NULL DEFAULT 0,
                outcome TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL
            )
            "#,
        )
        .execute(&*self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("failed to migrate metering"))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS metering_events_tenant_created_idx
            ON metering_events (tenant_id, created_at_ms)
            "#,
        )
        .execute(&*self.pool)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("failed to migrate metering index"))?;

        Ok(())
    }
}

#[async_trait]
impl UsageStore for PostgresUsageStore {
    async fn reserve(&self, charge: UsageCharge) -> Result<UsageReservation, ImageGatewayError> {
        let now = now_ms();
        let reservation_id = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(quota_lock_id(&charge.tenant_id))
            .execute(&mut *tx)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("quota lock unavailable"))?;

        sqlx::query(
            r#"
            UPDATE quota_reservations
            SET state = 'expired', updated_at_ms = $1
            WHERE tenant_id = $2 AND state = 'reserved' AND expires_at_ms <= $1
            "#,
        )
        .bind(now)
        .bind(&charge.tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        let (five_events, seven_events): (i64, i64) = sqlx::query_as(
            r#"
            SELECT
              COALESCE(SUM(CASE WHEN created_at_ms >= $1 THEN units ELSE 0 END), 0)::BIGINT AS five_used,
              COALESCE(SUM(units), 0)::BIGINT AS seven_used
            FROM usage_events
            WHERE tenant_id = $3 AND created_at_ms >= $2
            "#,
        )
        .bind(now - FIVE_HOURS_MS)
        .bind(now - SEVEN_DAYS_MS)
        .bind(&charge.tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        let (five_reserved, seven_reserved): (i64, i64) = sqlx::query_as(
            r#"
            SELECT
              COALESCE(SUM(CASE WHEN created_at_ms >= $1 THEN requested_units - committed_units - released_units ELSE 0 END), 0)::BIGINT AS five_reserved,
              COALESCE(SUM(requested_units - committed_units - released_units), 0)::BIGINT AS seven_reserved
            FROM quota_reservations
            WHERE tenant_id = $3
              AND state = 'reserved'
              AND expires_at_ms > $4
              AND created_at_ms >= $2
            "#,
        )
        .bind(now - FIVE_HOURS_MS)
        .bind(now - SEVEN_DAYS_MS)
        .bind(&charge.tenant_id)
        .bind(now)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        let five_used = to_u32_saturated(five_events + five_reserved);
        let seven_used = to_u32_saturated(seven_events + seven_reserved);
        let snapshot = ensure_quota(&charge, five_used, seven_used)?;

        sqlx::query(
            r#"
            INSERT INTO jobs
              (job_id, tenant_id, request_id, operation, provider_id, model, state,
               requested_units, charged_units, reservation_id, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, 'openai-codex', 'gpt-image-2', $5,
                    $6, 0, $7, $8, $8)
            "#,
        )
        .bind(job_id)
        .bind(&charge.tenant_id)
        .bind(&charge.request_id)
        .bind(charge.operation)
        .bind(JobState::Reserved.as_str())
        .bind(charge.units as i32)
        .bind(reservation_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("job state unavailable"))?;

        sqlx::query(
            r#"
            INSERT INTO quota_reservations
              (reservation_id, tenant_id, request_id, job_id, requested_units,
               committed_units, started_units, released_units, state,
               created_at_ms, updated_at_ms, expires_at_ms)
            VALUES ($1, $2, $3, $4, $5, 0, 0, 0, $6, $7, $7, $8)
            "#,
        )
        .bind(reservation_id)
        .bind(&charge.tenant_id)
        .bind(&charge.request_id)
        .bind(job_id)
        .bind(charge.units as i32)
        .bind(ReservationState::Reserved.as_str())
        .bind(now)
        .bind(now + RESERVATION_TTL_MS)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        insert_metering_event(
            &mut tx,
            &charge.tenant_id,
            job_id,
            reservation_id,
            &charge.request_id,
            charge.operation,
            "quota_reserved",
            charge.units,
            "reserved",
            now,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        Ok(UsageReservation {
            reservation_id,
            job_id,
            charge,
            snapshot,
        })
    }

    async fn commit(
        &self,
        reservation: &UsageReservation,
    ) -> Result<UsageSnapshot, ImageGatewayError> {
        let now = now_ms();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        let row: Option<(String, i32)> = sqlx::query_as(
            r#"
            SELECT state, requested_units
            FROM quota_reservations
            WHERE reservation_id = $1
            FOR UPDATE
            "#,
        )
        .bind(reservation.reservation_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        let Some((state, requested_units)) = row else {
            return Err(ImageGatewayError::internal("reservation not found"));
        };
        if state == ReservationState::Committed.as_str() {
            return Ok(reservation.snapshot.clone());
        }
        if state != ReservationState::Reserved.as_str() {
            return Err(ImageGatewayError::internal("reservation is not active"));
        }

        sqlx::query(
            r#"
            INSERT INTO usage_events
              (event_id, tenant_id, request_id, operation, units, outcome, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, 'charged', $6)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&reservation.charge.tenant_id)
        .bind(&reservation.charge.request_id)
        .bind(reservation.charge.operation)
        .bind(requested_units)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        sqlx::query(
            r#"
            UPDATE quota_reservations
            SET committed_units = requested_units,
                state = $2,
                updated_at_ms = $3
            WHERE reservation_id = $1
            "#,
        )
        .bind(reservation.reservation_id)
        .bind(ReservationState::Committed.as_str())
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        sqlx::query(
            r#"
            UPDATE jobs
            SET state = $2,
                charged_units = requested_units,
                finished_at_ms = $3,
                updated_at_ms = $3
            WHERE job_id = $1
            "#,
        )
        .bind(reservation.job_id)
        .bind(JobState::Succeeded.as_str())
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("job state unavailable"))?;

        insert_metering_event(
            &mut tx,
            &reservation.charge.tenant_id,
            reservation.job_id,
            reservation.reservation_id,
            &reservation.charge.request_id,
            reservation.charge.operation,
            "quota_committed",
            reservation.charge.units,
            "succeeded",
            now,
        )
        .await?;
        insert_metering_event(
            &mut tx,
            &reservation.charge.tenant_id,
            reservation.job_id,
            reservation.reservation_id,
            &reservation.charge.request_id,
            reservation.charge.operation,
            "job_succeeded",
            reservation.charge.units,
            "succeeded",
            now,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;
        Ok(reservation.snapshot.clone())
    }

    async fn release(
        &self,
        reservation: &UsageReservation,
        reason: &'static str,
    ) -> Result<(), ImageGatewayError> {
        let now = now_ms();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        let row: Option<(String, i32)> = sqlx::query_as(
            r#"
            SELECT state, requested_units
            FROM quota_reservations
            WHERE reservation_id = $1
            FOR UPDATE
            "#,
        )
        .bind(reservation.reservation_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        let Some((state, requested_units)) = row else {
            return Err(ImageGatewayError::internal("reservation not found"));
        };
        if state != ReservationState::Reserved.as_str() {
            return Ok(());
        }

        sqlx::query(
            r#"
            UPDATE quota_reservations
            SET released_units = $2,
                state = $3,
                updated_at_ms = $4
            WHERE reservation_id = $1
            "#,
        )
        .bind(reservation.reservation_id)
        .bind(requested_units)
        .bind(ReservationState::Released.as_str())
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        sqlx::query(
            r#"
            UPDATE jobs
            SET state = $2,
                finished_at_ms = $3,
                updated_at_ms = $3,
                last_error_code = $4
            WHERE job_id = $1
            "#,
        )
        .bind(reservation.job_id)
        .bind(JobState::Failed.as_str())
        .bind(now)
        .bind(reason)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("job state unavailable"))?;

        insert_metering_event(
            &mut tx,
            &reservation.charge.tenant_id,
            reservation.job_id,
            reservation.reservation_id,
            &reservation.charge.request_id,
            reservation.charge.operation,
            "quota_released",
            reservation.charge.units,
            reason,
            now,
        )
        .await?;
        insert_metering_event(
            &mut tx,
            &reservation.charge.tenant_id,
            reservation.job_id,
            reservation.reservation_id,
            &reservation.charge.request_id,
            reservation.charge.operation,
            "job_failed",
            reservation.charge.units,
            reason,
            now,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_metering_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: &str,
    job_id: Uuid,
    reservation_id: Uuid,
    request_id: &str,
    operation: &str,
    event_type: &str,
    units: u32,
    outcome: &str,
    now: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO metering_events
          (event_id, tenant_id, job_id, reservation_id, request_id, operation,
           event_type, units, outcome, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(tenant_id)
    .bind(job_id)
    .bind(reservation_id)
    .bind(request_id)
    .bind(operation)
    .bind(event_type)
    .bind(units as i32)
    .bind(outcome)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(|_| ImageGatewayError::service_unavailable("metering state unavailable"))?;
    Ok(())
}

fn prune_usage_state(state: &mut InMemoryUsageState, now: i64) {
    state
        .events
        .retain(|event| event.created_at_ms >= now - SEVEN_DAYS_MS);
    for reservation in &mut state.reservations {
        if reservation.state == ReservationState::Reserved && reservation.expires_at_ms <= now {
            reservation.state = ReservationState::Expired;
            reservation.updated_at_ms = now;
        }
    }
}

fn used_units_for_tenant(state: &InMemoryUsageState, tenant_id: &str, now: i64) -> (u32, u32) {
    let five_events = state
        .events
        .iter()
        .filter(|event| event.tenant_id == tenant_id)
        .filter(|event| event.created_at_ms >= now - FIVE_HOURS_MS)
        .map(|event| event.units)
        .sum::<u32>();
    let seven_events = state
        .events
        .iter()
        .filter(|event| event.tenant_id == tenant_id)
        .map(|event| event.units)
        .sum::<u32>();
    let five_reserved = active_reserved_units(state, tenant_id, now, now - FIVE_HOURS_MS);
    let seven_reserved = active_reserved_units(state, tenant_id, now, now - SEVEN_DAYS_MS);
    (
        five_events.saturating_add(five_reserved),
        seven_events.saturating_add(seven_reserved),
    )
}

fn active_reserved_units(
    state: &InMemoryUsageState,
    tenant_id: &str,
    now: i64,
    window_start: i64,
) -> u32 {
    state
        .reservations
        .iter()
        .filter(|reservation| reservation.tenant_id == tenant_id)
        .filter(|reservation| reservation.state == ReservationState::Reserved)
        .filter(|reservation| reservation.expires_at_ms > now)
        .filter(|reservation| reservation.created_at_ms >= window_start)
        .map(|reservation| {
            reservation
                .requested_units
                .saturating_sub(reservation.committed_units)
                .saturating_sub(reservation.released_units)
        })
        .sum()
}

fn ensure_quota(
    charge: &UsageCharge,
    five_used: u32,
    seven_used: u32,
) -> Result<UsageSnapshot, ImageGatewayError> {
    if five_used.saturating_add(charge.units) > charge.limits.five_hour_image_limit {
        let remaining_5h = charge
            .limits
            .five_hour_image_limit
            .saturating_sub(five_used);
        let remaining_7d = charge
            .limits
            .seven_day_image_limit
            .saturating_sub(seven_used);
        return Err(ImageGatewayError::quota_exceeded(
            "5-hour image quota exceeded",
            charge.limits.five_hour_image_limit,
            charge.limits.seven_day_image_limit,
            remaining_5h,
            remaining_7d,
            "5h",
        ));
    }

    if seven_used.saturating_add(charge.units) > charge.limits.seven_day_image_limit {
        let remaining_5h = charge
            .limits
            .five_hour_image_limit
            .saturating_sub(five_used);
        let remaining_7d = charge
            .limits
            .seven_day_image_limit
            .saturating_sub(seven_used);
        return Err(ImageGatewayError::quota_exceeded(
            "7-day image quota exceeded",
            charge.limits.five_hour_image_limit,
            charge.limits.seven_day_image_limit,
            remaining_5h,
            remaining_7d,
            "7d",
        ));
    }

    Ok(UsageSnapshot {
        limit_5h: charge.limits.five_hour_image_limit,
        remaining_5h: charge
            .limits
            .five_hour_image_limit
            .saturating_sub(five_used.saturating_add(charge.units)),
        limit_7d: charge.limits.seven_day_image_limit,
        remaining_7d: charge
            .limits
            .seven_day_image_limit
            .saturating_sub(seven_used.saturating_add(charge.units)),
    })
}

fn quota_lock_id(tenant_id: &str) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(b"quota:");
    hasher.update(tenant_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(bytes)
}

fn to_u32_saturated(value: i64) -> u32 {
    if value <= 0 {
        0
    } else {
        u32::try_from(value).unwrap_or(u32::MAX)
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn charge(units: u32) -> UsageCharge {
        UsageCharge {
            tenant_id: "tenant_a".to_string(),
            request_id: Uuid::new_v4().to_string(),
            operation: "generation",
            units,
            limits: UsageLimits {
                five_hour_image_limit: 2,
                seven_day_image_limit: 2,
            },
        }
    }

    #[tokio::test]
    async fn active_reservation_counts_against_quota_until_released() {
        let store = InMemoryUsageStore::default();
        let reservation = store.reserve(charge(2)).await.unwrap();

        let denied = store.reserve(charge(1)).await.unwrap_err();
        assert_eq!(
            denied.status_code(),
            axum::http::StatusCode::TOO_MANY_REQUESTS
        );

        store
            .release(&reservation, "provider_failed")
            .await
            .unwrap();
        let retry = store.reserve(charge(1)).await.unwrap();
        assert_eq!(retry.snapshot.remaining_5h, 1);
    }

    #[tokio::test]
    async fn commit_is_idempotent_and_charges_once() {
        let store = InMemoryUsageStore::default();
        let reservation = store.reserve(charge(1)).await.unwrap();

        store.commit(&reservation).await.unwrap();
        store.commit(&reservation).await.unwrap();

        let next = store.reserve(charge(1)).await.unwrap();
        assert_eq!(next.snapshot.remaining_5h, 0);
        let denied = store.reserve(charge(1)).await.unwrap_err();
        assert_eq!(
            denied.status_code(),
            axum::http::StatusCode::TOO_MANY_REQUESTS
        );
    }
}
