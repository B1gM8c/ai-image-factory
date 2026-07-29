use std::collections::HashMap;

use async_trait::async_trait;
use factory_identity::{
    AuthenticatedPrincipal, BootstrapUser, CredentialUser, IdentityError, IdentityRepository,
    IdentityUserAccess, LoginAttemptReservation, NewSession, OrganizationMembership,
    ProjectMembership, RefreshRevocation, RefreshRotation, RefreshRotationOutcome, SessionSubject,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use super::map_repository_error;

#[derive(Clone)]
pub struct PostgresIdentityRepository {
    pool: PgPool,
}

impl PostgresIdentityRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl IdentityRepository for PostgresIdentityRepository {
    async fn reserve_login_attempt(
        &self,
        reservation: LoginAttemptReservation,
    ) -> Result<bool, IdentityError> {
        let window_ms = seconds_to_ms(reservation.window_seconds);
        let block_until_ms = reservation
            .now_ms
            .saturating_add(seconds_to_ms(reservation.block_seconds));
        let global_limit =
            i32::try_from(reservation.global_limit).map_err(|_| IdentityError::Unavailable)?;
        let account_limit =
            i32::try_from(reservation.account_limit).map_err(|_| IdentityError::Unavailable)?;
        let mut tx = self.pool.begin().await.map_err(map_repository_error)?;
        let global_allowed = reserve_throttle_dimension(
            &mut tx,
            &reservation.global_key,
            "global",
            reservation.now_ms,
            window_ms,
            block_until_ms,
            global_limit,
        )
        .await?;
        if !global_allowed {
            tx.commit().await.map_err(map_repository_error)?;
            return Ok(false);
        }
        let account_allowed = reserve_throttle_dimension(
            &mut tx,
            &reservation.account_key,
            "account",
            reservation.now_ms,
            window_ms,
            block_until_ms,
            account_limit,
        )
        .await?;
        tx.commit().await.map_err(map_repository_error)?;
        Ok(account_allowed)
    }

    async fn credential_user_by_email(
        &self,
        normalized_email: &str,
    ) -> Result<Option<CredentialUser>, IdentityError> {
        sqlx::query(
            r#"
            SELECT u.user_id, u.normalized_email, u.display_name, u.roles, u.scopes,
                   u.authz_version, u.disabled_at_ms, u.failed_login_count, u.locked_until_ms,
                   c.password_hash, c.password_version
            FROM identity_users u
            JOIN identity_password_credentials c ON c.user_id = u.user_id
            WHERE u.normalized_email = $1
            "#,
        )
        .bind(normalized_email)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_repository_error)?
        .map(credential_user_from_row)
        .transpose()
    }

    async fn record_login_failure(
        &self,
        user_id: Option<Uuid>,
        now_ms: i64,
        max_failed_logins: u32,
        lockout_seconds: u64,
    ) -> Result<(), IdentityError> {
        let max_failed_logins =
            i32::try_from(max_failed_logins).map_err(|_| IdentityError::Unavailable)?;
        let lockout_ms = i64::try_from(lockout_seconds.saturating_mul(1000)).unwrap_or(i64::MAX);
        let lock_until_ms = now_ms.saturating_add(lockout_ms);
        let mut tx = self.pool.begin().await.map_err(map_repository_error)?;
        sqlx::query(
            r#"
            UPDATE identity_users
            SET failed_login_count = failed_login_count + 1,
                locked_until_ms = CASE
                    WHEN failed_login_count + 1 >= $3 THEN $4
                    ELSE locked_until_ms
                END,
                updated_at_ms = GREATEST(updated_at_ms, $2)
            WHERE user_id = $1
              AND (locked_until_ms IS NULL OR locked_until_ms <= $2)
            "#,
        )
        .bind(user_id)
        .bind(now_ms)
        .bind(max_failed_logins)
        .bind(lock_until_ms)
        .execute(&mut *tx)
        .await
        .map_err(map_repository_error)?;
        insert_audit(
            &mut tx,
            user_id,
            None,
            "identity.login",
            "denied",
            Some("invalid_credentials"),
            now_ms,
        )
        .await?;
        tx.commit().await.map_err(map_repository_error)
    }

    async fn create_session(&self, session: NewSession) -> Result<bool, IdentityError> {
        let mut tx = self.pool.begin().await.map_err(map_repository_error)?;
        let still_eligible = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT TRUE
            FROM identity_users u
            JOIN identity_password_credentials c ON c.user_id = u.user_id
            WHERE u.user_id = $1
              AND u.disabled_at_ms IS NULL
              AND (u.locked_until_ms IS NULL OR u.locked_until_ms <= $2)
              AND c.password_version = $3
            FOR UPDATE OF u, c
            "#,
        )
        .bind(session.user_id)
        .bind(session.created_at_ms)
        .bind(session.password_version)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_repository_error)?
        .unwrap_or(false);
        if !still_eligible {
            tx.rollback().await.map_err(map_repository_error)?;
            return Ok(false);
        }
        sqlx::query(
            r#"
            INSERT INTO identity_session_families
              (session_id, user_id, client_id, authz_version_at_login, created_at_ms,
               last_seen_at_ms, idle_expires_at_ms, absolute_expires_at_ms)
            VALUES ($1, $2, $3, $4, $5, $5, $6, $7)
            "#,
        )
        .bind(session.session_id)
        .bind(session.user_id)
        .bind(&session.client_id)
        .bind(session.authz_version_at_login)
        .bind(session.created_at_ms)
        .bind(session.idle_expires_at_ms)
        .bind(session.absolute_expires_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(map_repository_error)?;
        sqlx::query(
            r#"
            INSERT INTO identity_refresh_tokens
              (token_id, session_id, secret_hash, pepper_version, issued_at_ms, expires_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(session.refresh_token_id)
        .bind(session.session_id)
        .bind(session.refresh_secret_hash.as_slice())
        .bind(i32::from(session.refresh_pepper_version))
        .bind(session.created_at_ms)
        .bind(session.idle_expires_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(map_repository_error)?;
        sqlx::query(
            r#"
            UPDATE identity_users
            SET failed_login_count = 0, locked_until_ms = NULL, last_login_at_ms = $2,
                updated_at_ms = GREATEST(updated_at_ms, $2)
            WHERE user_id = $1
            "#,
        )
        .bind(session.user_id)
        .bind(session.created_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(map_repository_error)?;
        insert_audit(
            &mut tx,
            Some(session.user_id),
            Some(session.session_id),
            "identity.login",
            "success",
            None,
            session.created_at_ms,
        )
        .await?;
        sqlx::query("DELETE FROM identity_login_throttles WHERE throttle_key = $1")
            .bind(session.login_account_key.as_slice())
            .execute(&mut *tx)
            .await
            .map_err(map_repository_error)?;
        tx.commit().await.map_err(map_repository_error)?;
        Ok(true)
    }

    async fn rotate_refresh(
        &self,
        rotation: RefreshRotation,
    ) -> Result<RefreshRotationOutcome, IdentityError> {
        let mut tx = self.pool.begin().await.map_err(map_repository_error)?;
        let row = sqlx::query(
            r#"
            SELECT r.secret_hash, r.pepper_version, r.expires_at_ms, r.consumed_at_ms,
                   r.revoked_at_ms AS token_revoked_at_ms,
                   s.session_id, s.client_id, s.authz_version_at_login,
                   s.idle_expires_at_ms, s.absolute_expires_at_ms,
                   s.revoked_at_ms AS session_revoked_at_ms,
                   u.user_id, u.normalized_email, u.display_name, u.roles, u.scopes,
                   u.authz_version, u.disabled_at_ms
            FROM identity_refresh_tokens r
            JOIN identity_session_families s ON s.session_id = r.session_id
            JOIN identity_users u ON u.user_id = s.user_id
            WHERE r.token_id = $1
            FOR UPDATE OF r, s
            "#,
        )
        .bind(rotation.presented_token_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_repository_error)?;
        let Some(row) = row else {
            tx.rollback().await.map_err(map_repository_error)?;
            return Ok(RefreshRotationOutcome::Invalid);
        };
        let stored_hash: Vec<u8> = row.try_get("secret_hash").map_err(map_repository_error)?;
        let stored_version: i32 = row
            .try_get("pepper_version")
            .map_err(map_repository_error)?;
        if stored_version != i32::from(rotation.presented_pepper_version)
            || !constant_time_eq(&stored_hash, &rotation.presented_secret_hash)
        {
            tx.rollback().await.map_err(map_repository_error)?;
            return Ok(RefreshRotationOutcome::Invalid);
        }

        let session_id: Uuid = row.try_get("session_id").map_err(map_repository_error)?;
        let user_id: Uuid = row.try_get("user_id").map_err(map_repository_error)?;
        let consumed: Option<i64> = row
            .try_get("consumed_at_ms")
            .map_err(map_repository_error)?;
        let token_revoked: Option<i64> = row
            .try_get("token_revoked_at_ms")
            .map_err(map_repository_error)?;
        if consumed.is_some() || token_revoked.is_some() {
            revoke_family(&mut tx, session_id, rotation.now_ms, "refresh_reuse").await?;
            insert_audit(
                &mut tx,
                Some(user_id),
                Some(session_id),
                "identity.refresh_reuse",
                "denied",
                Some("refresh_reuse_detected"),
                rotation.now_ms,
            )
            .await?;
            tx.commit().await.map_err(map_repository_error)?;
            return Ok(RefreshRotationOutcome::Reused);
        }

        let expires_at: i64 = row.try_get("expires_at_ms").map_err(map_repository_error)?;
        let idle_expires_at: i64 = row
            .try_get("idle_expires_at_ms")
            .map_err(map_repository_error)?;
        let absolute_expires_at: i64 = row
            .try_get("absolute_expires_at_ms")
            .map_err(map_repository_error)?;
        let session_revoked: Option<i64> = row
            .try_get("session_revoked_at_ms")
            .map_err(map_repository_error)?;
        let disabled: Option<i64> = row
            .try_get("disabled_at_ms")
            .map_err(map_repository_error)?;
        let client_id: String = row.try_get("client_id").map_err(map_repository_error)?;
        let authz_version_at_login: i64 = row
            .try_get("authz_version_at_login")
            .map_err(map_repository_error)?;
        let current_authz_version: i64 =
            row.try_get("authz_version").map_err(map_repository_error)?;
        if expires_at <= rotation.now_ms
            || idle_expires_at <= rotation.now_ms
            || absolute_expires_at <= rotation.now_ms
            || session_revoked.is_some()
            || disabled.is_some()
            || client_id != rotation.client_id
            || authz_version_at_login != current_authz_version
        {
            revoke_family(&mut tx, session_id, rotation.now_ms, "refresh_invalid").await?;
            tx.commit().await.map_err(map_repository_error)?;
            return Ok(RefreshRotationOutcome::Invalid);
        }
        let next_idle = rotation.idle_expires_at_ms.min(absolute_expires_at);
        sqlx::query(
            r#"
            INSERT INTO identity_refresh_tokens
              (token_id, session_id, parent_token_id, secret_hash, pepper_version,
               issued_at_ms, expires_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(rotation.replacement_token_id)
        .bind(session_id)
        .bind(rotation.presented_token_id)
        .bind(rotation.replacement_secret_hash.as_slice())
        .bind(i32::from(rotation.replacement_pepper_version))
        .bind(rotation.now_ms)
        .bind(next_idle)
        .execute(&mut *tx)
        .await
        .map_err(map_repository_error)?;
        sqlx::query(
            r#"
            UPDATE identity_refresh_tokens
            SET consumed_at_ms = $2, replaced_by_token_id = $3
            WHERE token_id = $1
            "#,
        )
        .bind(rotation.presented_token_id)
        .bind(rotation.now_ms)
        .bind(rotation.replacement_token_id)
        .execute(&mut *tx)
        .await
        .map_err(map_repository_error)?;
        sqlx::query(
            r#"
            UPDATE identity_session_families
            SET last_seen_at_ms = $2, idle_expires_at_ms = $3
            WHERE session_id = $1
            "#,
        )
        .bind(session_id)
        .bind(rotation.now_ms)
        .bind(next_idle)
        .execute(&mut *tx)
        .await
        .map_err(map_repository_error)?;
        insert_audit(
            &mut tx,
            Some(user_id),
            Some(session_id),
            "identity.refresh",
            "success",
            None,
            rotation.now_ms,
        )
        .await?;
        let subject = SessionSubject {
            session_id,
            user_id,
            normalized_email: row
                .try_get("normalized_email")
                .map_err(map_repository_error)?,
            display_name: row.try_get("display_name").map_err(map_repository_error)?,
            roles: row.try_get("roles").map_err(map_repository_error)?,
            scopes: row.try_get("scopes").map_err(map_repository_error)?,
            authz_version: row.try_get("authz_version").map_err(map_repository_error)?,
            refresh_expires_at_ms: next_idle,
            absolute_expires_at_ms: absolute_expires_at,
        };
        tx.commit().await.map_err(map_repository_error)?;
        Ok(RefreshRotationOutcome::Rotated(subject))
    }

    async fn revoke_session(
        &self,
        session_id: Uuid,
        now_ms: i64,
        reason: &str,
    ) -> Result<(), IdentityError> {
        let mut tx = self.pool.begin().await.map_err(map_repository_error)?;
        let user_id = sqlx::query_scalar::<_, Uuid>(
            "SELECT user_id FROM identity_session_families WHERE session_id = $1 FOR UPDATE",
        )
        .bind(session_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_repository_error)?;
        revoke_family(&mut tx, session_id, now_ms, reason).await?;
        insert_audit(
            &mut tx,
            user_id,
            Some(session_id),
            "identity.logout",
            "success",
            None,
            now_ms,
        )
        .await?;
        tx.commit().await.map_err(map_repository_error)
    }

    async fn revoke_session_by_refresh(
        &self,
        revocation: RefreshRevocation,
    ) -> Result<bool, IdentityError> {
        let mut tx = self.pool.begin().await.map_err(map_repository_error)?;
        let row = sqlx::query(
            r#"
            SELECT r.secret_hash, r.pepper_version, s.session_id, s.user_id
            FROM identity_refresh_tokens r
            JOIN identity_session_families s ON s.session_id = r.session_id
            WHERE r.token_id = $1
            FOR UPDATE OF r, s
            "#,
        )
        .bind(revocation.presented_token_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_repository_error)?;
        let Some(row) = row else {
            tx.rollback().await.map_err(map_repository_error)?;
            return Ok(false);
        };
        let stored_hash: Vec<u8> = row.try_get("secret_hash").map_err(map_repository_error)?;
        let stored_version: i32 = row
            .try_get("pepper_version")
            .map_err(map_repository_error)?;
        if stored_version != i32::from(revocation.presented_pepper_version)
            || !constant_time_eq(&stored_hash, &revocation.presented_secret_hash)
        {
            tx.rollback().await.map_err(map_repository_error)?;
            return Ok(false);
        }
        let session_id: Uuid = row.try_get("session_id").map_err(map_repository_error)?;
        let user_id: Uuid = row.try_get("user_id").map_err(map_repository_error)?;
        revoke_family(&mut tx, session_id, revocation.now_ms, &revocation.reason).await?;
        insert_audit(
            &mut tx,
            Some(user_id),
            Some(session_id),
            "identity.logout",
            "success",
            None,
            revocation.now_ms,
        )
        .await?;
        tx.commit().await.map_err(map_repository_error)?;
        Ok(true)
    }

    async fn active_session_principal(
        &self,
        session_id: Uuid,
        user_id: Uuid,
        authz_version: i64,
        now_ms: i64,
    ) -> Result<Option<AuthenticatedPrincipal>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT u.user_id, u.normalized_email, u.display_name, u.roles, u.scopes,
                   u.authz_version, u.disabled_at_ms, u.created_at_ms
            FROM identity_session_families s
            JOIN identity_users u ON u.user_id = s.user_id
            WHERE s.session_id = $1 AND u.user_id = $2 AND u.authz_version = $3
              AND s.revoked_at_ms IS NULL AND u.disabled_at_ms IS NULL
              AND s.authz_version_at_login = $3
              AND s.idle_expires_at_ms > $4 AND s.absolute_expires_at_ms > $4
            "#,
        )
        .bind(session_id)
        .bind(user_id)
        .bind(authz_version)
        .bind(now_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_repository_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut users = vec![user_access_from_row(row)?];
        hydrate_memberships(&self.pool, &mut users).await?;
        let user = users.pop().ok_or(IdentityError::Unavailable)?;
        Ok(Some(AuthenticatedPrincipal {
            user_id,
            session_id,
            email: user.email,
            display_name: user.display_name,
            roles: user.roles,
            scopes: user.scopes,
            authz_version,
            organizations: user.organizations,
            projects: user.projects,
        }))
    }

    async fn bootstrap_user(&self, user: BootstrapUser) -> Result<bool, IdentityError> {
        let mut tx = self.pool.begin().await.map_err(map_repository_error)?;
        let inserted = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO identity_users
              (user_id, normalized_email, display_name, roles, scopes, authz_version,
               created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, 1, $6, $6)
            ON CONFLICT (normalized_email) DO NOTHING
            RETURNING user_id
            "#,
        )
        .bind(user.user_id)
        .bind(&user.normalized_email)
        .bind(&user.display_name)
        .bind(&user.roles)
        .bind(&user.scopes)
        .bind(user.created_at_ms)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_repository_error)?;
        if inserted.is_none() {
            tx.rollback().await.map_err(map_repository_error)?;
            return Ok(false);
        }
        sqlx::query(
            r#"
            INSERT INTO identity_password_credentials
              (user_id, password_hash, password_version, changed_at_ms)
            VALUES ($1, $2, 1, $3)
            "#,
        )
        .bind(user.user_id)
        .bind(&user.password_hash)
        .bind(user.created_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(map_repository_error)?;
        insert_audit(
            &mut tx,
            Some(user.user_id),
            None,
            "identity.bootstrap",
            "success",
            None,
            user.created_at_ms,
        )
        .await?;
        tx.commit().await.map_err(map_repository_error)?;
        Ok(true)
    }

    async fn get_user_access(
        &self,
        user_id: Uuid,
    ) -> Result<Option<IdentityUserAccess>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT user_id, normalized_email, display_name, roles, scopes,
                   authz_version, disabled_at_ms, created_at_ms
            FROM identity_users
            WHERE user_id = $1
            "#,
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_repository_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut users = vec![user_access_from_row(row)?];
        hydrate_memberships(&self.pool, &mut users).await?;
        Ok(users.pop())
    }

    async fn list_users(
        &self,
        after_email: Option<&str>,
        limit: usize,
    ) -> Result<Vec<IdentityUserAccess>, IdentityError> {
        let limit = i64::try_from(limit).map_err(|_| IdentityError::InvalidInput)?;
        let rows = sqlx::query(
            r#"
            SELECT user_id, normalized_email, display_name, roles, scopes,
                   authz_version, disabled_at_ms, created_at_ms
            FROM identity_users
            WHERE $1::TEXT IS NULL OR normalized_email > $1
            ORDER BY normalized_email
            LIMIT $2
            "#,
        )
        .bind(after_email)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(map_repository_error)?;
        let mut users = rows
            .into_iter()
            .map(user_access_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        hydrate_memberships(&self.pool, &mut users).await?;
        Ok(users)
    }
}

fn credential_user_from_row(row: sqlx::postgres::PgRow) -> Result<CredentialUser, IdentityError> {
    Ok(CredentialUser {
        user_id: row.try_get("user_id").map_err(map_repository_error)?,
        normalized_email: row
            .try_get("normalized_email")
            .map_err(map_repository_error)?,
        display_name: row.try_get("display_name").map_err(map_repository_error)?,
        password_hash: row.try_get("password_hash").map_err(map_repository_error)?,
        password_version: row
            .try_get("password_version")
            .map_err(map_repository_error)?,
        roles: row.try_get("roles").map_err(map_repository_error)?,
        scopes: row.try_get("scopes").map_err(map_repository_error)?,
        authz_version: row.try_get("authz_version").map_err(map_repository_error)?,
        disabled: row
            .try_get::<Option<i64>, _>("disabled_at_ms")
            .map_err(map_repository_error)?
            .is_some(),
        failed_login_count: u32::try_from(
            row.try_get::<i32, _>("failed_login_count")
                .map_err(map_repository_error)?,
        )
        .map_err(|_| IdentityError::Unavailable)?,
        locked_until_ms: row
            .try_get("locked_until_ms")
            .map_err(map_repository_error)?,
    })
}

fn user_access_from_row(row: sqlx::postgres::PgRow) -> Result<IdentityUserAccess, IdentityError> {
    Ok(IdentityUserAccess {
        user_id: row.try_get("user_id").map_err(map_repository_error)?,
        email: row
            .try_get("normalized_email")
            .map_err(map_repository_error)?,
        display_name: row.try_get("display_name").map_err(map_repository_error)?,
        roles: row.try_get("roles").map_err(map_repository_error)?,
        scopes: row.try_get("scopes").map_err(map_repository_error)?,
        authz_version: row.try_get("authz_version").map_err(map_repository_error)?,
        disabled: row
            .try_get::<Option<i64>, _>("disabled_at_ms")
            .map_err(map_repository_error)?
            .is_some(),
        created_at_ms: row.try_get("created_at_ms").map_err(map_repository_error)?,
        organizations: Vec::new(),
        projects: Vec::new(),
    })
}

async fn hydrate_memberships(
    pool: &PgPool,
    users: &mut [IdentityUserAccess],
) -> Result<(), IdentityError> {
    if users.is_empty() {
        return Ok(());
    }
    let positions = users
        .iter()
        .enumerate()
        .map(|(index, user)| (user.user_id, index))
        .collect::<HashMap<_, _>>();
    let user_ids = positions.keys().copied().collect::<Vec<_>>();

    let organization_rows = sqlx::query(
        r#"
        SELECT membership.user_id, membership.organization_id,
               organization.display_name, membership.role,
               organization.organization_kind = 'personal' AS is_personal
        FROM identity_organization_memberships membership
        JOIN identity_organizations organization
          ON organization.organization_id = membership.organization_id
        WHERE membership.user_id = ANY($1)
          AND membership.state = 'active'
        ORDER BY membership.user_id, organization.display_name, membership.organization_id
        "#,
    )
    .bind(&user_ids)
    .fetch_all(pool)
    .await
    .map_err(map_repository_error)?;
    for row in organization_rows {
        let user_id: Uuid = row.try_get("user_id").map_err(map_repository_error)?;
        let Some(index) = positions.get(&user_id).copied() else {
            return Err(IdentityError::Unavailable);
        };
        users[index].organizations.push(OrganizationMembership {
            organization_id: row
                .try_get("organization_id")
                .map_err(map_repository_error)?,
            display_name: row.try_get("display_name").map_err(map_repository_error)?,
            role: row.try_get("role").map_err(map_repository_error)?,
            is_personal: row.try_get("is_personal").map_err(map_repository_error)?,
        });
    }

    let project_rows = sqlx::query(
        r#"
        SELECT project_membership.user_id, project_membership.organization_id,
               project_membership.project_id, project.name AS display_name,
               project_membership.role, project_membership.is_default
        FROM identity_project_memberships project_membership
        JOIN identity_organization_memberships organization_membership
          ON organization_membership.organization_id = project_membership.organization_id
         AND organization_membership.user_id = project_membership.user_id
         AND organization_membership.state = 'active'
        JOIN gateway_projects project
          ON project.id = project_membership.project_id
         AND project.tenant_id = project_membership.organization_id
        WHERE project_membership.user_id = ANY($1)
          AND project_membership.state = 'active'
          AND project.archived_at IS NULL
        ORDER BY project_membership.user_id, project_membership.is_default DESC,
                 project.name, project_membership.project_id
        "#,
    )
    .bind(&user_ids)
    .fetch_all(pool)
    .await
    .map_err(map_repository_error)?;
    for row in project_rows {
        let user_id: Uuid = row.try_get("user_id").map_err(map_repository_error)?;
        let Some(index) = positions.get(&user_id).copied() else {
            return Err(IdentityError::Unavailable);
        };
        users[index].projects.push(ProjectMembership {
            organization_id: row
                .try_get("organization_id")
                .map_err(map_repository_error)?,
            project_id: row.try_get("project_id").map_err(map_repository_error)?,
            display_name: row.try_get("display_name").map_err(map_repository_error)?,
            role: row.try_get("role").map_err(map_repository_error)?,
            is_default: row.try_get("is_default").map_err(map_repository_error)?,
        });
    }
    Ok(())
}

async fn reserve_throttle_dimension(
    tx: &mut Transaction<'_, Postgres>,
    throttle_key: &[u8; 32],
    dimension: &str,
    now_ms: i64,
    window_ms: i64,
    block_until_ms: i64,
    limit: i32,
) -> Result<bool, IdentityError> {
    let window_floor_ms = now_ms.saturating_sub(window_ms);
    sqlx::query_scalar::<_, bool>(
        r#"
        INSERT INTO identity_login_throttles AS throttle
          (throttle_key, dimension, window_started_at_ms, failure_count,
           blocked_until_ms, updated_at_ms)
        VALUES ($1, $2, $3, 1, NULL, $3)
        ON CONFLICT (throttle_key) DO UPDATE
        SET window_started_at_ms = CASE
                WHEN throttle.window_started_at_ms <= $4 THEN $3
                ELSE throttle.window_started_at_ms
            END,
            failure_count = CASE
                WHEN throttle.window_started_at_ms <= $4 THEN 1
                ELSE LEAST(throttle.failure_count, 2147483646) + 1
            END,
            blocked_until_ms = CASE
                WHEN throttle.blocked_until_ms > $3 THEN throttle.blocked_until_ms
                WHEN (
                    CASE
                        WHEN throttle.window_started_at_ms <= $4 THEN 1
                        ELSE LEAST(throttle.failure_count, 2147483646) + 1
                    END
                ) > $6 THEN $5
                ELSE NULL
            END,
            updated_at_ms = $3
        RETURNING blocked_until_ms IS NULL OR blocked_until_ms <= $3
        "#,
    )
    .bind(throttle_key.as_slice())
    .bind(dimension)
    .bind(now_ms)
    .bind(window_floor_ms)
    .bind(block_until_ms)
    .bind(limit)
    .fetch_one(&mut **tx)
    .await
    .map_err(map_repository_error)
}

fn seconds_to_ms(seconds: u64) -> i64 {
    i64::try_from(seconds.saturating_mul(1000)).unwrap_or(i64::MAX)
}

async fn revoke_family(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    now_ms: i64,
    reason: &str,
) -> Result<(), IdentityError> {
    sqlx::query(
        r#"
        UPDATE identity_session_families
        SET revoked_at_ms = COALESCE(revoked_at_ms, $2),
            revoke_reason = COALESCE(revoke_reason, $3)
        WHERE session_id = $1
        "#,
    )
    .bind(session_id)
    .bind(now_ms)
    .bind(reason)
    .execute(&mut **tx)
    .await
    .map_err(map_repository_error)?;
    // The family row is the revocation authority checked by every access and
    // refresh path. Keep lineage immutable instead of rewriting O(n) tokens.
    Ok(())
}

async fn insert_audit(
    tx: &mut Transaction<'_, Postgres>,
    actor_user_id: Option<Uuid>,
    session_id: Option<Uuid>,
    action: &str,
    outcome: &str,
    reason_code: Option<&str>,
    now_ms: i64,
) -> Result<(), IdentityError> {
    sqlx::query(
        r#"
        INSERT INTO identity_audit_events
          (event_id, actor_user_id, session_id, action, outcome, reason_code, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(actor_user_id)
    .bind(session_id)
    .bind(action)
    .bind(outcome)
    .bind(reason_code)
    .bind(now_ms)
    .execute(&mut **tx)
    .await
    .map_err(map_repository_error)?;
    Ok(())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for index in 0..max_len {
        diff |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    diff == 0
}
