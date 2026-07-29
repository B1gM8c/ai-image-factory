use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct AuthPolicy {
    pub client_id: String,
    pub access_ttl_seconds: u64,
    pub session_idle_ttl_seconds: u64,
    pub session_absolute_ttl_seconds: u64,
    pub clock_skew_seconds: u64,
    pub max_failed_logins: u32,
    pub lockout_seconds: u64,
    pub password_hash_concurrency: usize,
    pub login_throttle_window_seconds: u64,
    pub max_account_login_attempts: u32,
    pub max_global_login_attempts: u32,
}

impl Default for AuthPolicy {
    fn default() -> Self {
        Self {
            client_id: "ai-image-factory-admin-bff".to_string(),
            access_ttl_seconds: 300,
            session_idle_ttl_seconds: 8 * 60 * 60,
            session_absolute_ttl_seconds: 30 * 24 * 60 * 60,
            clock_skew_seconds: 30,
            max_failed_logins: 5,
            lockout_seconds: 15 * 60,
            password_hash_concurrency: 4,
            login_throttle_window_seconds: 60,
            max_account_login_attempts: 10,
            max_global_login_attempts: 1_000,
        }
    }
}

impl AuthPolicy {
    pub fn validate(&self) -> Result<(), IdentityError> {
        if self.client_id.trim().is_empty()
            || self.client_id.len() > 128
            || !(60..=900).contains(&self.access_ttl_seconds)
            || self.session_idle_ttl_seconds < self.access_ttl_seconds
            || self.session_idle_ttl_seconds > 7 * 24 * 60 * 60
            || self.session_absolute_ttl_seconds < self.session_idle_ttl_seconds
            || self.session_absolute_ttl_seconds > 365 * 24 * 60 * 60
            || self.clock_skew_seconds > 60
            || !(3..=100).contains(&self.max_failed_logins)
            || !(60..=24 * 60 * 60).contains(&self.lockout_seconds)
            || !(1..=256).contains(&self.password_hash_concurrency)
            || !(10..=3_600).contains(&self.login_throttle_window_seconds)
            || !(3..=1_000).contains(&self.max_account_login_attempts)
            || self.max_global_login_attempts < self.max_account_login_attempts
            || self.max_global_login_attempts > 1_000_000
        {
            return Err(IdentityError::Configuration);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
    pub client_id: String,
}

#[derive(Clone, Debug)]
pub struct RefreshRequest {
    pub refresh_token: String,
    pub client_id: String,
}

#[derive(Clone, Debug)]
pub struct CredentialUser {
    pub user_id: Uuid,
    pub normalized_email: String,
    pub display_name: String,
    pub password_hash: String,
    pub password_version: i32,
    pub roles: Vec<String>,
    pub scopes: Vec<String>,
    pub authz_version: i64,
    pub disabled: bool,
    pub failed_login_count: u32,
    pub locked_until_ms: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct BootstrapUser {
    pub user_id: Uuid,
    pub normalized_email: String,
    pub display_name: String,
    pub password_hash: String,
    pub roles: Vec<String>,
    pub scopes: Vec<String>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug)]
pub struct NewSession {
    pub session_id: Uuid,
    pub user_id: Uuid,
    pub password_version: i32,
    pub login_account_key: [u8; 32],
    pub authz_version_at_login: i64,
    pub client_id: String,
    pub refresh_token_id: Uuid,
    pub refresh_secret_hash: [u8; 32],
    pub refresh_pepper_version: u16,
    pub created_at_ms: i64,
    pub idle_expires_at_ms: i64,
    pub absolute_expires_at_ms: i64,
}

#[derive(Clone, Debug)]
pub struct RefreshRotation {
    pub presented_token_id: Uuid,
    pub presented_secret_hash: [u8; 32],
    pub presented_pepper_version: u16,
    pub replacement_token_id: Uuid,
    pub replacement_secret_hash: [u8; 32],
    pub replacement_pepper_version: u16,
    pub client_id: String,
    pub now_ms: i64,
    pub idle_expires_at_ms: i64,
}

#[derive(Clone, Debug)]
pub struct RefreshRevocation {
    pub presented_token_id: Uuid,
    pub presented_secret_hash: [u8; 32],
    pub presented_pepper_version: u16,
    pub now_ms: i64,
    pub reason: String,
}

#[derive(Clone, Debug)]
pub struct LoginAttemptReservation {
    pub account_key: [u8; 32],
    pub global_key: [u8; 32],
    pub now_ms: i64,
    pub window_seconds: u64,
    pub block_seconds: u64,
    pub account_limit: u32,
    pub global_limit: u32,
}

#[derive(Clone, Debug)]
pub enum RefreshRotationOutcome {
    Rotated(SessionSubject),
    Reused,
    Invalid,
}

#[derive(Clone, Debug)]
pub struct SessionSubject {
    pub session_id: Uuid,
    pub user_id: Uuid,
    pub normalized_email: String,
    pub display_name: String,
    pub roles: Vec<String>,
    pub scopes: Vec<String>,
    pub authz_version: i64,
    pub refresh_expires_at_ms: i64,
    pub absolute_expires_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AccessClaims {
    pub iss: String,
    pub sub: String,
    pub aud: String,
    pub client_id: String,
    pub sid: String,
    pub jti: String,
    pub iat: u64,
    pub nbf: u64,
    pub exp: u64,
    pub scope: String,
    pub roles: Vec<String>,
    pub authz_version: i64,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct PublicUser {
    pub id: String,
    pub email: String,
    pub display_name: String,
    pub roles: Vec<String>,
    pub scopes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct PublicSession {
    pub id: String,
    pub absolute_expires_at: String,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct TokenPair {
    pub access_token: String,
    pub token_type: &'static str,
    pub expires_in: u64,
    pub refresh_token: String,
    pub refresh_expires_in: u64,
    pub user: PublicUser,
    pub session: PublicSession,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct OrganizationMembership {
    pub organization_id: String,
    pub display_name: String,
    pub role: String,
    pub is_personal: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct ProjectMembership {
    pub organization_id: String,
    pub project_id: String,
    pub display_name: String,
    pub role: String,
    pub is_default: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct IdentityUserAccess {
    pub user_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub roles: Vec<String>,
    pub scopes: Vec<String>,
    pub authz_version: i64,
    pub disabled: bool,
    pub created_at_ms: i64,
    pub organizations: Vec<OrganizationMembership>,
    pub projects: Vec<ProjectMembership>,
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct AuthenticatedPrincipal {
    pub user_id: Uuid,
    pub session_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub roles: Vec<String>,
    pub scopes: Vec<String>,
    pub authz_version: i64,
    pub organizations: Vec<OrganizationMembership>,
    pub projects: Vec<ProjectMembership>,
}

impl AuthenticatedPrincipal {
    pub fn has_scope(&self, required: &str) -> bool {
        self.scopes
            .iter()
            .any(|scope| scope == required || scope == "admin:*")
    }
}

#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("invalid authentication")]
    InvalidAuthentication,
    #[error("forbidden")]
    Forbidden,
    #[error("invalid identity input")]
    InvalidInput,
    #[error("identity conflict")]
    Conflict,
    #[error("identity service unavailable")]
    Unavailable,
    #[error("identity configuration is invalid")]
    Configuration,
    #[error("identity cryptographic operation failed")]
    Crypto,
}

#[async_trait]
pub trait IdentityRepository: Send + Sync {
    async fn reserve_login_attempt(
        &self,
        reservation: LoginAttemptReservation,
    ) -> Result<bool, IdentityError>;

    async fn credential_user_by_email(
        &self,
        normalized_email: &str,
    ) -> Result<Option<CredentialUser>, IdentityError>;

    async fn record_login_failure(
        &self,
        user_id: Option<Uuid>,
        now_ms: i64,
        max_failed_logins: u32,
        lockout_seconds: u64,
    ) -> Result<(), IdentityError>;

    async fn create_session(&self, session: NewSession) -> Result<bool, IdentityError>;

    /// Atomically consumes one refresh token and inserts its replacement.
    ///
    /// Implementations must serialize rotation with every operation that can
    /// revoke the same session family, using one consistent row-lock order. A
    /// matched consumed or revoked token must revoke the whole family before
    /// returning [`RefreshRotationOutcome::Reused`]. An invalid secret must not
    /// revoke a family, because token identifiers are not credentials.
    async fn rotate_refresh(
        &self,
        rotation: RefreshRotation,
    ) -> Result<RefreshRotationOutcome, IdentityError>;

    async fn revoke_session(
        &self,
        session_id: Uuid,
        now_ms: i64,
        reason: &str,
    ) -> Result<(), IdentityError>;

    async fn revoke_session_by_refresh(
        &self,
        revocation: RefreshRevocation,
    ) -> Result<bool, IdentityError>;

    async fn active_session_principal(
        &self,
        session_id: Uuid,
        user_id: Uuid,
        authz_version: i64,
        now_ms: i64,
    ) -> Result<Option<AuthenticatedPrincipal>, IdentityError>;

    async fn bootstrap_user(&self, user: BootstrapUser) -> Result<bool, IdentityError>;

    /// Returns one user's current identity and active workspace access.
    ///
    /// An unknown user is `Ok(None)`; repository failures remain errors.
    async fn get_user_access(
        &self,
        user_id: Uuid,
    ) -> Result<Option<IdentityUserAccess>, IdentityError>;

    /// Lists users in normalized-email order after the exclusive cursor.
    async fn list_users(
        &self,
        after_email: Option<&str>,
        limit: usize,
    ) -> Result<Vec<IdentityUserAccess>, IdentityError>;
}
