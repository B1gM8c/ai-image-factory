use std::{
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use image_provider_contracts::BillingMetric;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    auth::RequestAttribution,
    error::QuotaExceededContext,
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
    pub attribution: Option<RequestAttribution>,
    pub request_id: String,
    pub admission_session_id: Option<Uuid>,
    pub operation: &'static str,
    pub provider_id: String,
    pub model: String,
    pub output_count: u32,
    pub billable_units: u32,
    pub billing_metric: BillingMetric,
    pub limits: UsageLimits,
}

impl UsageCharge {
    pub fn billing_unit(&self) -> &'static str {
        match self.billing_metric {
            BillingMetric::Output => "output",
            BillingMetric::Request => "request",
            BillingMetric::VideoSecond => "second",
        }
    }

    fn dimensions_are_valid(&self) -> bool {
        self.output_count > 0
            && self.billable_units > 0
            && match self.billing_metric {
                BillingMetric::Output => self.output_count == self.billable_units,
                BillingMetric::Request => self.output_count == 1 && self.billable_units == 1,
                BillingMetric::VideoSecond => self.output_count == 1,
            }
    }
}

#[derive(Clone, Debug)]
pub struct UsageReservation {
    pub reservation_id: Uuid,
    pub job_id: Uuid,
    pub charge: UsageCharge,
    pub snapshot: UsageSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Debug)]
struct UsageEvent {
    tenant_id: String,
    created_at_ms: i64,
    units: u32,
    billing_metric: BillingMetric,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct UsageReservationRecord {
    reservation_id: Uuid,
    job_id: Uuid,
    tenant_id: String,
    request_id: String,
    admission_session_id: Option<Uuid>,
    operation: &'static str,
    requested_units: u32,
    billing_metric: BillingMetric,
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
    provider_id: String,
    model: String,
    state: JobState,
    requested_units: u32,
    output_count: u32,
    billing_metric: BillingMetric,
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
    billing_metric: BillingMetric,
    outcome: &'static str,
    created_at_ms: i64,
}

#[async_trait]
impl UsageStore for InMemoryUsageStore {
    async fn reserve(&self, charge: UsageCharge) -> Result<UsageReservation, ImageGatewayError> {
        if !charge.dimensions_are_valid() {
            return Err(ImageGatewayError::internal("usage dimensions are invalid"));
        }
        let now = now_ms();
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageGatewayError::internal("usage store lock poisoned"))?;
        prune_usage_state(&mut state, now);

        let (five_used, seven_used) =
            used_units_for_tenant(&state, &charge.tenant_id, charge.billing_metric, now);
        let snapshot = ensure_quota(&charge, five_used, seven_used)?;
        let reservation_id = Uuid::new_v4();
        let job_id = Uuid::new_v4();

        state.jobs.push(JobRecord {
            job_id,
            tenant_id: charge.tenant_id.clone(),
            request_id: charge.request_id.clone(),
            operation: charge.operation,
            provider_id: charge.provider_id.clone(),
            model: charge.model.clone(),
            state: JobState::Reserved,
            requested_units: charge.billable_units,
            output_count: charge.output_count,
            billing_metric: charge.billing_metric,
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
            admission_session_id: charge.admission_session_id,
            operation: charge.operation,
            requested_units: charge.billable_units,
            billing_metric: charge.billing_metric,
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
            units: charge.billable_units,
            billing_metric: charge.billing_metric,
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
        record.committed_units = reservation.charge.billable_units;
        record.updated_at_ms = now;
        state.events.push(UsageEvent {
            tenant_id: reservation.charge.tenant_id.clone(),
            created_at_ms: now,
            units: reservation.charge.billable_units,
            billing_metric: reservation.charge.billing_metric,
        });
        if let Some(job) = state
            .jobs
            .iter_mut()
            .find(|job| job.job_id == reservation.job_id)
        {
            job.state = JobState::Succeeded;
            job.charged_units = reservation.charge.billable_units;
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
            units: reservation.charge.billable_units,
            billing_metric: reservation.charge.billing_metric,
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
            units: reservation.charge.billable_units,
            billing_metric: reservation.charge.billing_metric,
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
        record.released_units = reservation.charge.billable_units;
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
            units: reservation.charge.billable_units,
            billing_metric: reservation.charge.billing_metric,
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
            units: reservation.charge.billable_units,
            billing_metric: reservation.charge.billing_metric,
            outcome: reason,
            created_at_ms: now,
        });

        Ok(())
    }
}

#[derive(Clone)]
pub struct PostgresUsageStore {
    pool: PgPool,
}

impl PostgresUsageStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct ExistingReservationRow {
    reservation_id: Uuid,
    job_id: Uuid,
    tenant_id: String,
    request_id: String,
    operation: String,
    provider_id: String,
    model: String,
    requested_units: i32,
    output_count: i32,
    billing_metric: String,
    billing_unit: String,
    reservation_state: String,
    job_state: String,
    limit_5h: Option<i32>,
    remaining_5h: Option<i32>,
    limit_7d: Option<i32>,
    remaining_7d: Option<i32>,
}

#[async_trait]
impl UsageStore for PostgresUsageStore {
    async fn reserve(&self, charge: UsageCharge) -> Result<UsageReservation, ImageGatewayError> {
        if !charge.dimensions_are_valid() {
            return Err(ImageGatewayError::internal("usage dimensions are invalid"));
        }
        let reservation_id = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        let (mut tx, now) = begin_quota_transition(&self.pool, &charge.tenant_id).await?;

        if let Some(existing) = existing_session_reservation(&mut tx, &charge).await? {
            tx.commit()
                .await
                .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;
            return Ok(existing);
        }

        crate::project_model_policy::enforce_project_model_controls(&mut tx, &charge, now).await?;
        lock_active_attribution(&mut tx, &charge).await?;

        sqlx::query(
            r#"
            UPDATE quota_reservations
            SET state = 'expired', updated_at_ms = $1
            WHERE tenant_id = $2 AND state = 'reserved' AND expires_at_ms <= $1
              AND NOT EXISTS (
                SELECT 1 FROM work_items w WHERE w.job_id = quota_reservations.job_id
              )
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
              AND billing_metric = $4 AND billing_unit = $5
            "#,
        )
        .bind(now - FIVE_HOURS_MS)
        .bind(now - SEVEN_DAYS_MS)
        .bind(&charge.tenant_id)
        .bind(charge.billing_metric.as_str())
        .bind(charge.billing_unit())
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
              AND billing_metric = $5 AND billing_unit = $6
              AND state = 'reserved'
              AND (
                expires_at_ms > $4
                OR EXISTS (
                  SELECT 1 FROM work_items w
                  WHERE w.job_id = quota_reservations.job_id
                    AND w.state IN ('ready', 'leased', 'running', 'awaiting_executor', 'uncertain')
                )
              )
              AND created_at_ms >= $2
            "#,
        )
        .bind(now - FIVE_HOURS_MS)
        .bind(now - SEVEN_DAYS_MS)
        .bind(&charge.tenant_id)
        .bind(now)
        .bind(charge.billing_metric.as_str())
        .bind(charge.billing_unit())
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        let five_used = to_u32_saturated(five_events + five_reserved);
        let seven_used = to_u32_saturated(seven_events + seven_reserved);
        let snapshot = ensure_quota(&charge, five_used, seven_used)?;
        let limit_5h = quota_i32(snapshot.limit_5h)?;
        let remaining_5h = quota_i32(snapshot.remaining_5h)?;
        let limit_7d = quota_i32(snapshot.limit_7d)?;
        let remaining_7d = quota_i32(snapshot.remaining_7d)?;

        sqlx::query(postgres_job_insert_sql())
            .bind(job_id)
            .bind(&charge.tenant_id)
            .bind(&charge.request_id)
            .bind(charge.operation)
            .bind(&charge.provider_id)
            .bind(&charge.model)
            .bind(JobState::Reserved.as_str())
            .bind(i32::try_from(charge.output_count).map_err(|_| {
                ImageGatewayError::internal("usage output count exceeds PostgreSQL range")
            })?)
            .bind(i32::try_from(charge.billable_units).map_err(|_| {
                ImageGatewayError::internal("usage billable units exceed PostgreSQL range")
            })?)
            .bind(charge.billing_metric.as_str())
            .bind(charge.billing_unit())
            .bind(reservation_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("job state unavailable"))?;

        insert_job_attribution(&mut tx, job_id, &charge, now).await?;

        sqlx::query(
            r#"
            INSERT INTO quota_reservations
              (reservation_id, tenant_id, request_id, job_id, requested_units,
               committed_units, started_units, released_units, state,
               created_at_ms, updated_at_ms, expires_at_ms,
               limit_5h, remaining_5h, limit_7d, remaining_7d,
               admission_session_id, billing_metric, billing_unit)
            VALUES ($1, $2, $3, $4, $5, 0, 0, 0, $6, $7, $7, $8,
                    $9, $10, $11, $12, $13, $14, $15)
            "#,
        )
        .bind(reservation_id)
        .bind(&charge.tenant_id)
        .bind(&charge.request_id)
        .bind(job_id)
        .bind(charge.billable_units as i32)
        .bind(ReservationState::Reserved.as_str())
        .bind(now)
        .bind(now + RESERVATION_TTL_MS)
        .bind(limit_5h)
        .bind(remaining_5h)
        .bind(limit_7d)
        .bind(remaining_7d)
        .bind(charge.admission_session_id)
        .bind(charge.billing_metric.as_str())
        .bind(charge.billing_unit())
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
            charge.billing_metric.as_str(),
            charge.billing_unit(),
            "quota_reserved",
            charge.billable_units,
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
        let (mut tx, now) =
            begin_quota_transition(&self.pool, &reservation.charge.tenant_id).await?;

        let locked = lock_quota_reservation(&mut tx, reservation).await?;
        if locked.state == ReservationState::Committed.as_str() {
            tx.commit()
                .await
                .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;
            return Ok(reservation.snapshot.clone());
        }
        if locked.state != ReservationState::Reserved.as_str() {
            return Err(ImageGatewayError::internal("reservation is not active"));
        }

        sqlx::query(
            r#"
            INSERT INTO usage_events
              (event_id, tenant_id, job_id, request_id, operation, billing_metric,
               billing_unit, units, outcome, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'charged', $9)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&locked.tenant_id)
        .bind(locked.job_id)
        .bind(&locked.request_id)
        .bind(&locked.operation)
        .bind(&locked.billing_metric)
        .bind(&locked.billing_unit)
        .bind(locked.requested_units)
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
            WHERE reservation_id = $1 AND tenant_id = $4
            "#,
        )
        .bind(locked.reservation_id)
        .bind(ReservationState::Committed.as_str())
        .bind(now)
        .bind(&locked.tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        let job_update = sqlx::query(
            r#"
            UPDATE jobs
            SET state = $2,
                charged_units = $6,
                finished_at_ms = $3,
                updated_at_ms = $3
            WHERE job_id = $1 AND tenant_id = $4 AND reservation_id = $5
            "#,
        )
        .bind(locked.job_id)
        .bind(JobState::Succeeded.as_str())
        .bind(now)
        .bind(&locked.tenant_id)
        .bind(locked.reservation_id)
        .bind(locked.requested_units)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("job state unavailable"))?;

        require_one_job_updated(job_update)?;

        insert_metering_event(
            &mut tx,
            &locked.tenant_id,
            locked.job_id,
            locked.reservation_id,
            &locked.request_id,
            &locked.operation,
            &locked.billing_metric,
            &locked.billing_unit,
            "quota_committed",
            locked.requested_units as u32,
            "succeeded",
            now,
        )
        .await?;
        insert_metering_event(
            &mut tx,
            &locked.tenant_id,
            locked.job_id,
            locked.reservation_id,
            &locked.request_id,
            &locked.operation,
            &locked.billing_metric,
            &locked.billing_unit,
            "job_succeeded",
            locked.requested_units as u32,
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
        let (mut tx, now) =
            begin_quota_transition(&self.pool, &reservation.charge.tenant_id).await?;

        let locked = lock_quota_reservation(&mut tx, reservation).await?;
        if locked.state != ReservationState::Reserved.as_str() {
            tx.commit()
                .await
                .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;
            return Ok(());
        }

        sqlx::query(
            r#"
            UPDATE quota_reservations
            SET released_units = $2,
                state = $3,
                updated_at_ms = $4
            WHERE reservation_id = $1 AND tenant_id = $5
            "#,
        )
        .bind(locked.reservation_id)
        .bind(locked.requested_units)
        .bind(ReservationState::Released.as_str())
        .bind(now)
        .bind(&locked.tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

        let job_update = sqlx::query(
            r#"
            UPDATE jobs
            SET state = $2,
                finished_at_ms = $3,
                updated_at_ms = $3,
                last_error_code = $4
            WHERE job_id = $1 AND tenant_id = $5 AND reservation_id = $6
            "#,
        )
        .bind(locked.job_id)
        .bind(JobState::Failed.as_str())
        .bind(now)
        .bind(reason)
        .bind(&locked.tenant_id)
        .bind(locked.reservation_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("job state unavailable"))?;

        require_one_job_updated(job_update)?;

        insert_metering_event(
            &mut tx,
            &locked.tenant_id,
            locked.job_id,
            locked.reservation_id,
            &locked.request_id,
            &locked.operation,
            &locked.billing_metric,
            &locked.billing_unit,
            "quota_released",
            locked.requested_units as u32,
            reason,
            now,
        )
        .await?;
        insert_metering_event(
            &mut tx,
            &locked.tenant_id,
            locked.job_id,
            locked.reservation_id,
            &locked.request_id,
            &locked.operation,
            &locked.billing_metric,
            &locked.billing_unit,
            "job_failed",
            locked.requested_units as u32,
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

async fn lock_active_attribution(
    tx: &mut Transaction<'_, Postgres>,
    charge: &UsageCharge,
) -> Result<(), ImageGatewayError> {
    let Some(attribution) = &charge.attribution else {
        return Ok(());
    };
    let active = match (
        attribution.service_account_id.as_deref(),
        attribution.api_key_id.as_deref(),
        attribution.actor_user_id,
    ) {
        (Some(service_account_id), Some(api_key_id), None) => {
            let Some(authz_version) = attribution.credential_authz_version else {
                return Err(ImageGatewayError::internal(
                    "request attribution is missing credential authorization version",
                ));
            };
            if let Some(route) = attribution.route.as_ref() {
                sqlx::query_scalar::<_, i32>(
                    r#"
                    SELECT 1
                    FROM gateway_api_keys credential
                    JOIN gateway_service_accounts account
                      ON account.id = credential.service_account_id
                     AND account.project_id = credential.project_id
                     AND account.tenant_id = credential.tenant_id
                    JOIN gateway_projects project
                      ON project.id = credential.project_id
                     AND project.tenant_id = credential.tenant_id
                    JOIN gateway_api_key_provider_routes binding
                      ON binding.api_key_id = credential.id
                     AND binding.project_id = credential.project_id
                     AND binding.tenant_id = credential.tenant_id
                     AND binding.service_account_id = credential.service_account_id
                     AND binding.route_id = $8
                     AND binding.route_revision = $9
                     AND binding.provider_id = $10
                     AND binding.operation_id = $11
                     AND binding.command_schema = $12
                    JOIN provider_route_heads route
                      ON route.route_id = binding.route_id
                     AND route.current_revision = binding.route_revision
                     AND route.provider_id = binding.provider_id
                     AND route.operation_id = binding.operation_id
                     AND route.command_schema = binding.command_schema
                     AND route.state = 'enabled'
                    WHERE credential.id = $1
                      AND credential.project_id = $2
                      AND credential.tenant_id = $3
                      AND credential.service_account_id = $4
                      AND credential.deleted_at IS NULL
                      AND account.deleted_at IS NULL
                      AND project.archived_at IS NULL
                      AND (credential.expires_at IS NULL OR credential.expires_at > $5)
                      AND credential.authz_version = $6
                      AND account.owner_user_id IS NOT DISTINCT FROM $7
                      AND (
                        (
                          account.owner_type = 'service_account'
                          AND $7::UUID IS NULL
                        )
                        OR
                        (
                          account.owner_type = 'user'
                          AND $7::UUID IS NOT NULL
                          AND EXISTS (
                            SELECT 1
                            FROM identity_project_memberships membership
                            JOIN identity_users identity
                              ON identity.user_id = membership.user_id
                             AND identity.disabled_at_ms IS NULL
                            WHERE membership.organization_id = project.tenant_id
                              AND membership.project_id = project.id
                              AND membership.user_id = $7
                              AND membership.state = 'active'
                          )
                        )
                      )
                    FOR SHARE OF credential, account, project, binding, route
                    "#,
                )
                .bind(api_key_id)
                .bind(&attribution.project_id)
                .bind(&charge.tenant_id)
                .bind(service_account_id)
                .bind(now_seconds())
                .bind(authz_version)
                .bind(attribution.credential_owner_user_id)
                .bind(route.route_id)
                .bind(route.route_revision)
                .bind(&route.provider_id)
                .bind(&route.operation_id)
                .bind(&route.command_schema)
                .fetch_optional(&mut **tx)
                .await
            } else {
                sqlx::query_scalar::<_, i32>(
                    r#"
                    SELECT 1
                    FROM gateway_api_keys credential
                    JOIN gateway_service_accounts account
                      ON account.id = credential.service_account_id
                     AND account.project_id = credential.project_id
                     AND account.tenant_id = credential.tenant_id
                    JOIN gateway_projects project
                      ON project.id = credential.project_id
                     AND project.tenant_id = credential.tenant_id
                    WHERE credential.id = $1
                      AND credential.project_id = $2
                      AND credential.tenant_id = $3
                      AND credential.service_account_id = $4
                      AND credential.deleted_at IS NULL
                      AND account.deleted_at IS NULL
                      AND project.archived_at IS NULL
                      AND (credential.expires_at IS NULL OR credential.expires_at > $5)
                      AND credential.authz_version = $6
                      AND account.owner_user_id IS NOT DISTINCT FROM $7
                      AND (
                        (
                          account.owner_type = 'service_account'
                          AND $7::UUID IS NULL
                        )
                        OR
                        (
                          account.owner_type = 'user'
                          AND $7::UUID IS NOT NULL
                          AND EXISTS (
                            SELECT 1
                            FROM identity_project_memberships membership
                            JOIN identity_users identity
                              ON identity.user_id = membership.user_id
                             AND identity.disabled_at_ms IS NULL
                            WHERE membership.organization_id = project.tenant_id
                              AND membership.project_id = project.id
                              AND membership.user_id = $7
                              AND membership.state = 'active'
                          )
                        )
                      )
                    FOR SHARE OF credential, account, project
                    "#,
                )
                .bind(api_key_id)
                .bind(&attribution.project_id)
                .bind(&charge.tenant_id)
                .bind(service_account_id)
                .bind(now_seconds())
                .bind(authz_version)
                .bind(attribution.credential_owner_user_id)
                .fetch_optional(&mut **tx)
                .await
            }
        }
        (None, None, Some(actor_user_id)) => {
            if attribution.credential_owner_user_id.is_some() {
                return Err(ImageGatewayError::internal(
                    "user session attribution has a credential owner",
                ));
            }
            let (Some(actor_session_id), Some(actor_authz_version), Some(route)) = (
                attribution.actor_session_id,
                attribution.actor_authz_version,
                attribution.route.as_ref(),
            ) else {
                return Err(ImageGatewayError::internal(
                    "user session attribution is incomplete",
                ));
            };
            sqlx::query_scalar::<_, i32>(
                r#"
                SELECT 1
                FROM identity_users identity
                JOIN identity_session_families session
                  ON session.user_id = identity.user_id
                 AND session.session_id = $2
                 AND session.authz_version_at_login = $3
                 AND session.revoked_at_ms IS NULL
                 AND session.idle_expires_at_ms >
                     (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
                 AND session.absolute_expires_at_ms >
                     (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
                JOIN gateway_projects project
                  ON project.id = $4 AND project.tenant_id = $5
                 AND project.archived_at IS NULL
                JOIN provider_route_heads route
                  ON route.route_id = $6
                 AND route.current_revision = $7
                 AND route.provider_id = $8
                 AND route.operation_id = $9
                 AND route.command_schema = $10
                 AND route.state = 'enabled'
                WHERE identity.user_id = $1
                  AND identity.authz_version = $3
                  AND identity.disabled_at_ms IS NULL
                  AND (
                    ('platform_owner' = ANY(identity.roles) AND 'admin:*' = ANY(identity.scopes))
                    OR EXISTS (
                      SELECT 1
                      FROM identity_project_memberships membership
                      WHERE membership.user_id = identity.user_id
                        AND membership.organization_id = project.tenant_id
                        AND membership.project_id = project.id
                        AND membership.state = 'active'
                    )
                  )
                FOR SHARE OF identity, session, project, route
                "#,
            )
            .bind(actor_user_id)
            .bind(actor_session_id)
            .bind(actor_authz_version)
            .bind(&attribution.project_id)
            .bind(&charge.tenant_id)
            .bind(route.route_id)
            .bind(route.route_revision)
            .bind(&route.provider_id)
            .bind(&route.operation_id)
            .bind(&route.command_schema)
            .fetch_optional(&mut **tx)
            .await
        }
        (None, None, None) => {
            if attribution.credential_authz_version.is_some()
                || attribution.credential_owner_user_id.is_some()
            {
                return Err(ImageGatewayError::internal(
                    "legacy attribution has credential metadata",
                ));
            }
            sqlx::query_scalar::<_, i32>(
                r#"
                SELECT 1
                FROM gateway_projects
                WHERE id = $1 AND tenant_id = $2 AND archived_at IS NULL
                FOR SHARE
                "#,
            )
            .bind(&attribution.project_id)
            .bind(&charge.tenant_id)
            .fetch_optional(&mut **tx)
            .await
        }
        _ => {
            return Err(ImageGatewayError::internal(
                "request attribution is incomplete",
            ));
        }
    }
    .map_err(|_| ImageGatewayError::service_unavailable("credential state unavailable"))?;
    if active.is_some() {
        Ok(())
    } else {
        Err(ImageGatewayError::authentication())
    }
}

async fn insert_job_attribution(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    charge: &UsageCharge,
    admitted_at_ms: i64,
) -> Result<(), ImageGatewayError> {
    let Some(attribution) = &charge.attribution else {
        return Ok(());
    };
    let auth_kind = match (
        attribution.api_key_id.is_some(),
        attribution.actor_user_id.is_some(),
    ) {
        (true, false) => "api_key",
        (false, true) => "user_session",
        (false, false) => "legacy",
        (true, true) => {
            return Err(ImageGatewayError::internal(
                "request attribution has conflicting principals",
            ));
        }
    };
    let route = attribution.route.as_ref();
    sqlx::query(
        r#"
        INSERT INTO job_auth_attributions
          (job_id, tenant_id, project_id, service_account_id, api_key_id,
           credential_authz_version, credential_owner_user_id,
           actor_user_id, actor_session_id,
           actor_authz_version, route_provider_id, route_operation_id,
           route_command_schema, route_id, route_revision, auth_kind, admitted_at_ms)
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8,
          $9, $10, $11, $12, $13, $14, $15, $16, $17
        )
        "#,
    )
    .bind(job_id)
    .bind(&charge.tenant_id)
    .bind(&attribution.project_id)
    .bind(&attribution.service_account_id)
    .bind(&attribution.api_key_id)
    .bind(attribution.credential_authz_version)
    .bind(attribution.credential_owner_user_id)
    .bind(attribution.actor_user_id)
    .bind(attribution.actor_session_id)
    .bind(attribution.actor_authz_version)
    .bind(route.map(|route| route.provider_id.as_str()))
    .bind(route.map(|route| route.operation_id.as_str()))
    .bind(route.map(|route| route.command_schema.as_str()))
    .bind(route.map(|route| route.route_id))
    .bind(route.map(|route| route.route_revision))
    .bind(auth_kind)
    .bind(admitted_at_ms)
    .execute(&mut **tx)
    .await
    .map_err(|_| ImageGatewayError::service_unavailable("job attribution unavailable"))?;
    Ok(())
}

async fn existing_session_reservation(
    tx: &mut Transaction<'_, Postgres>,
    charge: &UsageCharge,
) -> Result<Option<UsageReservation>, ImageGatewayError> {
    let Some(session_id) = charge.admission_session_id else {
        return Ok(None);
    };
    let row: Option<ExistingReservationRow> = sqlx::query_as(
        r#"
        SELECT qr.reservation_id, qr.job_id, qr.tenant_id, qr.request_id,
               j.operation, j.provider_id, j.model, qr.requested_units,
               j.output_count, qr.billing_metric, qr.billing_unit,
               qr.state AS reservation_state, j.state AS job_state,
               qr.limit_5h, qr.remaining_5h, qr.limit_7d, qr.remaining_7d
        FROM quota_reservations qr
        JOIN jobs j
          ON j.job_id = qr.job_id
         AND j.tenant_id = qr.tenant_id
         AND j.reservation_id = qr.reservation_id
        WHERE qr.admission_session_id = $1
        FOR UPDATE OF qr, j
        "#,
    )
    .bind(session_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let units = u32::try_from(row.requested_units)
        .map_err(|_| ImageGatewayError::internal("stored reservation units are invalid"))?;
    let snapshot = UsageSnapshot {
        limit_5h: row
            .limit_5h
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| ImageGatewayError::internal("stored quota snapshot is invalid"))?,
        remaining_5h: row
            .remaining_5h
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| ImageGatewayError::internal("stored quota snapshot is invalid"))?,
        limit_7d: row
            .limit_7d
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| ImageGatewayError::internal("stored quota snapshot is invalid"))?,
        remaining_7d: row
            .remaining_7d
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| ImageGatewayError::internal("stored quota snapshot is invalid"))?,
    };
    if row.tenant_id != charge.tenant_id
        || row.request_id != charge.request_id
        || row.operation != charge.operation
        || row.provider_id != charge.provider_id
        || row.model != charge.model
        || units != charge.billable_units
        || u32::try_from(row.output_count).ok() != Some(charge.output_count)
        || row.billing_metric != charge.billing_metric.as_str()
        || row.billing_unit != charge.billing_unit()
        || row.reservation_state != ReservationState::Reserved.as_str()
        || row.job_state != JobState::Reserved.as_str()
        || snapshot.limit_5h != charge.limits.five_hour_image_limit
        || snapshot.limit_7d != charge.limits.seven_day_image_limit
    {
        return Err(ImageGatewayError::internal(
            "admission session reservation does not match the request",
        ));
    }
    Ok(Some(UsageReservation {
        reservation_id: row.reservation_id,
        job_id: row.job_id,
        charge: charge.clone(),
        snapshot,
    }))
}

fn quota_i32(value: u32) -> Result<i32, ImageGatewayError> {
    i32::try_from(value)
        .map_err(|_| ImageGatewayError::config("quota limit exceeds PostgreSQL range"))
}

async fn begin_quota_transition<'a>(
    pool: &'a PgPool,
    tenant_id: &str,
) -> Result<(Transaction<'a, Postgres>, i64), ImageGatewayError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(quota_lock_id(tenant_id))
        .execute(&mut *tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota lock unavailable"))?;
    let now =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;
    Ok((tx, now))
}

#[derive(sqlx::FromRow)]
struct LockedQuotaReservation {
    reservation_id: Uuid,
    tenant_id: String,
    state: String,
    requested_units: i32,
    job_id: Uuid,
    request_id: String,
    operation: String,
    billing_metric: String,
    billing_unit: String,
    admission_session_id: Option<Uuid>,
}

async fn lock_quota_reservation(
    tx: &mut Transaction<'_, Postgres>,
    reservation: &UsageReservation,
) -> Result<LockedQuotaReservation, ImageGatewayError> {
    let locked: Option<LockedQuotaReservation> = sqlx::query_as(
        r#"
        SELECT
          qr.reservation_id,
          qr.tenant_id,
          qr.state,
          qr.requested_units,
          qr.job_id,
          qr.request_id,
          j.operation,
          qr.billing_metric,
          qr.billing_unit,
          qr.admission_session_id
        FROM quota_reservations qr
        JOIN jobs j
          ON j.job_id = qr.job_id
         AND j.tenant_id = qr.tenant_id
         AND j.reservation_id = qr.reservation_id
        WHERE qr.reservation_id = $1 AND qr.tenant_id = $2
        FOR UPDATE OF qr, j
        "#,
    )
    .bind(reservation.reservation_id)
    .bind(&reservation.charge.tenant_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|_| ImageGatewayError::service_unavailable("quota state unavailable"))?;

    let Some(locked) = locked else {
        return Err(ImageGatewayError::internal("reservation not found"));
    };
    let units_match = u32::try_from(locked.requested_units)
        .is_ok_and(|units| units == reservation.charge.billable_units);
    if locked.job_id != reservation.job_id
        || locked.request_id != reservation.charge.request_id
        || locked.operation != reservation.charge.operation
        || locked.billing_metric != reservation.charge.billing_metric.as_str()
        || locked.billing_unit != reservation.charge.billing_unit()
        || locked.admission_session_id != reservation.charge.admission_session_id
        || !units_match
    {
        return Err(ImageGatewayError::internal(
            "reservation handle does not match stored quota state",
        ));
    }
    Ok(locked)
}

fn require_one_job_updated(result: sqlx::postgres::PgQueryResult) -> Result<(), ImageGatewayError> {
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(ImageGatewayError::internal(
            "reservation job transition did not update exactly one row",
        ))
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
    billing_metric: &str,
    billing_unit: &str,
    event_type: &str,
    units: u32,
    outcome: &str,
    now: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO metering_events
          (event_id, tenant_id, job_id, reservation_id, request_id, operation,
           billing_metric, billing_unit, event_type, units, outcome, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(tenant_id)
    .bind(job_id)
    .bind(reservation_id)
    .bind(request_id)
    .bind(operation)
    .bind(billing_metric)
    .bind(billing_unit)
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

fn used_units_for_tenant(
    state: &InMemoryUsageState,
    tenant_id: &str,
    billing_metric: BillingMetric,
    now: i64,
) -> (u32, u32) {
    let five_events = state
        .events
        .iter()
        .filter(|event| event.tenant_id == tenant_id)
        .filter(|event| event.billing_metric == billing_metric)
        .filter(|event| event.created_at_ms >= now - FIVE_HOURS_MS)
        .map(|event| event.units)
        .sum::<u32>();
    let seven_events = state
        .events
        .iter()
        .filter(|event| event.tenant_id == tenant_id)
        .filter(|event| event.billing_metric == billing_metric)
        .map(|event| event.units)
        .sum::<u32>();
    let five_reserved =
        active_reserved_units(state, tenant_id, billing_metric, now, now - FIVE_HOURS_MS);
    let seven_reserved =
        active_reserved_units(state, tenant_id, billing_metric, now, now - SEVEN_DAYS_MS);
    (
        five_events.saturating_add(five_reserved),
        seven_events.saturating_add(seven_reserved),
    )
}

fn active_reserved_units(
    state: &InMemoryUsageState,
    tenant_id: &str,
    billing_metric: BillingMetric,
    now: i64,
    window_start: i64,
) -> u32 {
    state
        .reservations
        .iter()
        .filter(|reservation| reservation.tenant_id == tenant_id)
        .filter(|reservation| reservation.billing_metric == billing_metric)
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
    if five_used.saturating_add(charge.billable_units) > charge.limits.five_hour_image_limit {
        let remaining_5h = charge
            .limits
            .five_hour_image_limit
            .saturating_sub(five_used);
        let remaining_7d = charge
            .limits
            .seven_day_image_limit
            .saturating_sub(seven_used);
        return Err(ImageGatewayError::quota_exceeded(
            format!("5-hour {} quota exceeded", quota_subject(charge)),
            QuotaExceededContext {
                billing_metric: charge.billing_metric.as_str(),
                billing_unit: charge.billing_unit(),
                limit_5h: charge.limits.five_hour_image_limit,
                limit_7d: charge.limits.seven_day_image_limit,
                remaining_5h,
                remaining_7d,
                window: "5h",
            },
        ));
    }

    if seven_used.saturating_add(charge.billable_units) > charge.limits.seven_day_image_limit {
        let remaining_5h = charge
            .limits
            .five_hour_image_limit
            .saturating_sub(five_used);
        let remaining_7d = charge
            .limits
            .seven_day_image_limit
            .saturating_sub(seven_used);
        return Err(ImageGatewayError::quota_exceeded(
            format!("7-day {} quota exceeded", quota_subject(charge)),
            QuotaExceededContext {
                billing_metric: charge.billing_metric.as_str(),
                billing_unit: charge.billing_unit(),
                limit_5h: charge.limits.five_hour_image_limit,
                limit_7d: charge.limits.seven_day_image_limit,
                remaining_5h,
                remaining_7d,
                window: "7d",
            },
        ));
    }

    Ok(UsageSnapshot {
        limit_5h: charge.limits.five_hour_image_limit,
        remaining_5h: charge
            .limits
            .five_hour_image_limit
            .saturating_sub(five_used.saturating_add(charge.billable_units)),
        limit_7d: charge.limits.seven_day_image_limit,
        remaining_7d: charge
            .limits
            .seven_day_image_limit
            .saturating_sub(seven_used.saturating_add(charge.billable_units)),
    })
}

fn quota_subject(charge: &UsageCharge) -> &'static str {
    match charge.billing_metric {
        BillingMetric::Output => "output",
        BillingMetric::Request => "request",
        BillingMetric::VideoSecond => "video-second",
    }
}

pub(crate) fn quota_lock_id(tenant_id: &str) -> i64 {
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

fn now_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn postgres_job_insert_sql() -> &'static str {
    r#"
    INSERT INTO jobs
      (job_id, tenant_id, request_id, operation, provider_id, model, state,
       output_count, requested_units, billable_units, billing_metric, billing_unit,
       charged_units, reservation_id, created_at_ms, updated_at_ms)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10, $11,
            0, $12, $13, $13)
    "#
}

#[cfg(test)]
mod tests {
    use super::*;

    fn charge(units: u32) -> UsageCharge {
        UsageCharge {
            tenant_id: "tenant_a".to_string(),
            attribution: None,
            request_id: Uuid::new_v4().to_string(),
            admission_session_id: None,
            operation: "generation",
            provider_id: image_provider_contracts::openai_codex::PROVIDER_ID.to_string(),
            model: image_provider_contracts::openai_codex::MODEL_GPT_IMAGE_2.to_string(),
            output_count: units,
            billable_units: units,
            billing_metric: BillingMetric::Output,
            limits: UsageLimits {
                five_hour_image_limit: 2,
                seven_day_image_limit: 2,
            },
        }
    }

    #[test]
    fn snapshot_model_identity_is_preserved_for_jobs_and_usage_persistence() {
        use image_provider_contracts::openai_codex;
        use serde_json::json;

        let job = crate::models::parse_generation(
            json!({
                "model": openai_codex::MODEL_GPT_IMAGE_2_SNAPSHOT,
                "prompt": "a snapshot identity test"
            }),
            "req-snapshot".to_string(),
        )
        .unwrap();
        assert_eq!(job.model, openai_codex::MODEL_GPT_IMAGE_2_SNAPSHOT);

        let charge = UsageCharge {
            tenant_id: "tenant_a".to_string(),
            attribution: None,
            request_id: job.request_id,
            admission_session_id: None,
            operation: "generation",
            provider_id: openai_codex::PROVIDER_ID.to_string(),
            model: job.model,
            output_count: job.n,
            billable_units: job.n,
            billing_metric: BillingMetric::Output,
            limits: UsageLimits {
                five_hour_image_limit: 2,
                seven_day_image_limit: 2,
            },
        };
        assert_eq!(charge.provider_id, openai_codex::PROVIDER_ID);
        assert_eq!(charge.model, openai_codex::MODEL_GPT_IMAGE_2_SNAPSHOT);

        let insert_sql = postgres_job_insert_sql();
        assert!(insert_sql.contains("VALUES ($1, $2, $3, $4, $5, $6, $7,"));
        assert!(!insert_sql.contains("'openai-codex'"));
        assert!(!insert_sql.contains("'gpt-image-2'"));
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
