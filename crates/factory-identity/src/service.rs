use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use uuid::Uuid;

use crate::{
    AccessTokenCodec, AuthPolicy, AuthenticatedPrincipal, BootstrapUser, CredentialUser,
    IdentityError, IdentityRepository, IdentityUserAccess, LoginAttemptReservation, LoginRequest,
    NewSession, PasswordEngine, PublicSession, PublicUser, RefreshRequest, RefreshRevocation,
    RefreshRotation, RefreshRotationOutcome, RefreshTokenKeyring, SessionSubject, TokenPair,
};

pub struct IdentityService {
    repository: Arc<dyn IdentityRepository>,
    passwords: PasswordEngine,
    access_tokens: AccessTokenCodec,
    refresh_tokens: RefreshTokenKeyring,
    policy: AuthPolicy,
}

impl IdentityService {
    pub fn new(
        repository: Arc<dyn IdentityRepository>,
        access_tokens: AccessTokenCodec,
        refresh_tokens: RefreshTokenKeyring,
        policy: AuthPolicy,
    ) -> Result<Self, IdentityError> {
        policy.validate()?;
        let passwords = PasswordEngine::new(policy.password_hash_concurrency)?;
        Ok(Self {
            repository,
            passwords,
            access_tokens,
            refresh_tokens,
            policy,
        })
    }

    pub async fn login(&self, request: LoginRequest) -> Result<TokenPair, IdentityError> {
        if request.client_id != self.policy.client_id {
            return Err(IdentityError::InvalidAuthentication);
        }
        let email = normalize_email(&request.email)?;
        let now_ms = now_ms()?;
        let account_key = self
            .refresh_tokens
            .derive_current(b"login-account", email.as_bytes())?;
        let global_key = self
            .refresh_tokens
            .derive_current(b"login-global", b"all")?;
        if !self
            .repository
            .reserve_login_attempt(LoginAttemptReservation {
                account_key,
                global_key,
                now_ms,
                window_seconds: self.policy.login_throttle_window_seconds,
                block_seconds: self.policy.lockout_seconds,
                account_limit: self.policy.max_account_login_attempts,
                global_limit: self.policy.max_global_login_attempts,
            })
            .await?
        {
            return Err(IdentityError::InvalidAuthentication);
        }
        let user = self.repository.credential_user_by_email(&email).await?;
        let verified = self
            .passwords
            .verify(
                request.password,
                user.as_ref().map(|user| user.password_hash.clone()),
            )
            .await?;
        let usable = user.as_ref().is_some_and(|user| {
            !user.disabled && user.locked_until_ms.is_none_or(|until| until <= now_ms)
        });
        if !verified || !usable {
            let failure_user_id = user.as_ref().and_then(|user| {
                let lock_is_active = user.locked_until_ms.is_some_and(|until| until > now_ms);
                (!user.disabled && !lock_is_active).then_some(user.user_id)
            });
            self.repository
                .record_login_failure(
                    failure_user_id,
                    now_ms,
                    self.policy.max_failed_logins,
                    self.policy.lockout_seconds,
                )
                .await?;
            return Err(IdentityError::InvalidAuthentication);
        }
        self.create_session(user.expect("usable user exists"), account_key, now_ms)
            .await
    }

    pub async fn refresh(&self, request: RefreshRequest) -> Result<TokenPair, IdentityError> {
        if request.client_id != self.policy.client_id {
            return Err(IdentityError::InvalidAuthentication);
        }
        let presented = self
            .refresh_tokens
            .parse_and_digest(&request.refresh_token)?;
        let replacement = self.refresh_tokens.issue()?;
        let now_ms = now_ms()?;
        let mut outcome = RefreshRotationOutcome::Invalid;
        for (version, digest) in presented.digests {
            outcome = self
                .repository
                .rotate_refresh(RefreshRotation {
                    presented_token_id: presented.token_id,
                    presented_secret_hash: digest,
                    presented_pepper_version: version,
                    replacement_token_id: replacement.token_id,
                    replacement_secret_hash: replacement.secret_hash,
                    replacement_pepper_version: replacement.pepper_version,
                    client_id: request.client_id.clone(),
                    now_ms,
                    idle_expires_at_ms: now_ms
                        .saturating_add(seconds_to_ms(self.policy.session_idle_ttl_seconds)),
                })
                .await?;
            if !matches!(outcome, RefreshRotationOutcome::Invalid) {
                break;
            }
        }
        match outcome {
            RefreshRotationOutcome::Rotated(subject) => {
                self.token_pair(subject, replacement.value, now_ms)
            }
            RefreshRotationOutcome::Reused | RefreshRotationOutcome::Invalid => {
                Err(IdentityError::InvalidAuthentication)
            }
        }
    }

    pub async fn authenticate_access(
        &self,
        token: &str,
    ) -> Result<AuthenticatedPrincipal, IdentityError> {
        let claims = self.access_tokens.validate(token)?;
        let session_id = claims
            .sid
            .parse()
            .map_err(|_| IdentityError::InvalidAuthentication)?;
        let user_id = claims
            .sub
            .parse()
            .map_err(|_| IdentityError::InvalidAuthentication)?;
        self.repository
            .active_session_principal(session_id, user_id, claims.authz_version, now_ms()?)
            .await?
            .ok_or(IdentityError::InvalidAuthentication)
    }

    pub async fn logout(&self, access_token: &str) -> Result<(), IdentityError> {
        let principal = self.authenticate_access(access_token).await?;
        self.repository
            .revoke_session(principal.session_id, now_ms()?, "logout")
            .await
    }

    pub async fn logout_refresh(&self, refresh_token: &str) -> Result<(), IdentityError> {
        let presented = match self.refresh_tokens.parse_and_digest(refresh_token) {
            Ok(presented) => presented,
            Err(IdentityError::InvalidAuthentication) => return Ok(()),
            Err(error) => return Err(error),
        };
        let now_ms = now_ms()?;
        for (version, digest) in presented.digests {
            if self
                .repository
                .revoke_session_by_refresh(RefreshRevocation {
                    presented_token_id: presented.token_id,
                    presented_secret_hash: digest,
                    presented_pepper_version: version,
                    now_ms,
                    reason: "logout".to_string(),
                })
                .await?
            {
                break;
            }
        }
        Ok(())
    }

    pub async fn bootstrap_admin(
        &self,
        email: String,
        display_name: String,
        password: String,
    ) -> Result<bool, IdentityError> {
        let now_ms = now_ms()?;
        let user = BootstrapUser {
            user_id: Uuid::new_v4(),
            normalized_email: normalize_email(&email)?,
            display_name: validate_display_name(display_name)?,
            password_hash: self.passwords.hash(password).await?,
            roles: vec!["platform_owner".to_string()],
            scopes: vec!["admin:*".to_string()],
            created_at_ms: now_ms,
        };
        self.repository.bootstrap_user(user).await
    }

    pub async fn create_member_user(
        &self,
        email: String,
        display_name: String,
        password: String,
    ) -> Result<IdentityUserAccess, IdentityError> {
        let now_ms = now_ms()?;
        let user_id = Uuid::new_v4();
        let user = BootstrapUser {
            user_id,
            normalized_email: normalize_email(&email)?,
            display_name: validate_display_name(display_name)?,
            password_hash: self.passwords.hash(password).await?,
            roles: vec!["member".to_string()],
            scopes: vec!["workspace:read".to_string(), "workspace:write".to_string()],
            created_at_ms: now_ms,
        };
        if !self.repository.bootstrap_user(user).await? {
            return Err(IdentityError::Conflict);
        }
        self.repository
            .get_user_access(user_id)
            .await?
            .ok_or(IdentityError::Unavailable)
    }

    /// Returns `Ok(None)` when the requested identity does not exist.
    pub async fn get_user_access(
        &self,
        user_id: Uuid,
    ) -> Result<Option<IdentityUserAccess>, IdentityError> {
        self.repository.get_user_access(user_id).await
    }

    pub async fn list_users(
        &self,
        after_email: Option<&str>,
        limit: usize,
    ) -> Result<Vec<IdentityUserAccess>, IdentityError> {
        let after_email = after_email.map(normalize_email).transpose()?;
        self.repository
            .list_users(after_email.as_deref(), limit.clamp(1, 100))
            .await
    }

    async fn create_session(
        &self,
        user: CredentialUser,
        login_account_key: [u8; 32],
        now_ms: i64,
    ) -> Result<TokenPair, IdentityError> {
        let refresh = self.refresh_tokens.issue()?;
        let session_id = Uuid::new_v4();
        let absolute_expires_at_ms =
            now_ms.saturating_add(seconds_to_ms(self.policy.session_absolute_ttl_seconds));
        let idle_expires_at_ms = now_ms
            .saturating_add(seconds_to_ms(self.policy.session_idle_ttl_seconds))
            .min(absolute_expires_at_ms);
        let created = self
            .repository
            .create_session(NewSession {
                session_id,
                user_id: user.user_id,
                password_version: user.password_version,
                login_account_key,
                authz_version_at_login: user.authz_version,
                client_id: self.policy.client_id.clone(),
                refresh_token_id: refresh.token_id,
                refresh_secret_hash: refresh.secret_hash,
                refresh_pepper_version: refresh.pepper_version,
                created_at_ms: now_ms,
                idle_expires_at_ms,
                absolute_expires_at_ms,
            })
            .await?;
        if !created {
            return Err(IdentityError::InvalidAuthentication);
        }
        self.token_pair(
            SessionSubject {
                session_id,
                user_id: user.user_id,
                normalized_email: user.normalized_email,
                display_name: user.display_name,
                roles: user.roles,
                scopes: user.scopes,
                authz_version: user.authz_version,
                refresh_expires_at_ms: idle_expires_at_ms,
                absolute_expires_at_ms,
            },
            refresh.value,
            now_ms,
        )
    }

    fn token_pair(
        &self,
        subject: SessionSubject,
        refresh_token: String,
        now_ms: i64,
    ) -> Result<TokenPair, IdentityError> {
        let now_seconds = u64::try_from(now_ms / 1000).map_err(|_| IdentityError::Crypto)?;
        let refresh_seconds =
            u64::try_from(subject.refresh_expires_at_ms.saturating_sub(now_ms).max(0) / 1000)
                .map_err(|_| IdentityError::Crypto)?;
        Ok(TokenPair {
            access_token: self.access_tokens.issue(&subject, now_seconds)?,
            token_type: "Bearer",
            expires_in: self.policy.access_ttl_seconds,
            refresh_token,
            refresh_expires_in: refresh_seconds,
            user: PublicUser {
                id: subject.user_id.to_string(),
                email: subject.normalized_email,
                display_name: subject.display_name,
                roles: subject.roles,
                scopes: subject.scopes,
            },
            session: PublicSession {
                id: subject.session_id.to_string(),
                absolute_expires_at: subject.absolute_expires_at_ms.to_string(),
            },
        })
    }
}

fn normalize_email(email: &str) -> Result<String, IdentityError> {
    let normalized = email.trim().to_ascii_lowercase();
    if normalized.len() < 3
        || normalized.len() > 254
        || normalized.matches('@').count() != 1
        || normalized.chars().any(char::is_whitespace)
    {
        return Err(IdentityError::InvalidInput);
    }
    Ok(normalized)
}

fn validate_display_name(value: String) -> Result<String, IdentityError> {
    let value = value.trim().to_string();
    if value.is_empty() || value.len() > 128 {
        return Err(IdentityError::InvalidInput);
    }
    Ok(value)
}

fn now_ms() -> Result<i64, IdentityError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| IdentityError::Crypto)?
        .as_millis();
    i64::try_from(millis).map_err(|_| IdentityError::Crypto)
}

fn seconds_to_ms(seconds: u64) -> i64 {
    i64::try_from(seconds.saturating_mul(1000)).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::{
        AccessTokenCodec, OrganizationMembership, ProjectMembership, RefreshRotationOutcome,
    };

    const PRIVATE_KEY: &[u8] = br#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgJ6r5c63M0tPZV05C
Y0U72GBHm9iqV7QaUgFxk/9dBn+hRANCAAT5ufmoZxTrAkeOwJFSjVcbQ1Pvl2sw
892/nV1rvRJwDokKy+s00P46StleDgXLe9hOly8yM81frZfcMeI1krz+
-----END PRIVATE KEY-----
"#;
    const PUBLIC_KEY: &[u8] = br#"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE+bn5qGcU6wJHjsCRUo1XG0NT75dr
MPPdv51da70ScA6JCsvrNND+OkrZXg4Fy3vYTpcvMjPNX62X3DHiNZK8/g==
-----END PUBLIC KEY-----
"#;
    const PASSWORD: &str = "correct horse battery staple";

    struct RefreshRecord {
        token_id: Uuid,
        secret_hash: [u8; 32],
        pepper_version: u16,
        consumed: bool,
    }

    struct FakeState {
        user: CredentialUser,
        users: Vec<IdentityUserAccess>,
        session: Option<NewSession>,
        refresh_tokens: Vec<RefreshRecord>,
        revoked: bool,
        failures: u32,
    }

    struct FakeRepository {
        state: Mutex<FakeState>,
    }

    #[async_trait]
    impl IdentityRepository for FakeRepository {
        async fn reserve_login_attempt(
            &self,
            _reservation: LoginAttemptReservation,
        ) -> Result<bool, IdentityError> {
            Ok(true)
        }

        async fn credential_user_by_email(
            &self,
            normalized_email: &str,
        ) -> Result<Option<CredentialUser>, IdentityError> {
            let state = self.state.lock().unwrap();
            Ok((state.user.normalized_email == normalized_email).then(|| state.user.clone()))
        }

        async fn record_login_failure(
            &self,
            _user_id: Option<Uuid>,
            _now_ms: i64,
            _max_failed_logins: u32,
            _lockout_seconds: u64,
        ) -> Result<(), IdentityError> {
            self.state.lock().unwrap().failures += 1;
            Ok(())
        }

        async fn create_session(&self, session: NewSession) -> Result<bool, IdentityError> {
            let mut state = self.state.lock().unwrap();
            state.refresh_tokens.push(RefreshRecord {
                token_id: session.refresh_token_id,
                secret_hash: session.refresh_secret_hash,
                pepper_version: session.refresh_pepper_version,
                consumed: false,
            });
            state.session = Some(session);
            Ok(true)
        }

        async fn rotate_refresh(
            &self,
            rotation: RefreshRotation,
        ) -> Result<RefreshRotationOutcome, IdentityError> {
            let mut state = self.state.lock().unwrap();
            let Some(index) = state.refresh_tokens.iter().position(|token| {
                token.token_id == rotation.presented_token_id
                    && token.secret_hash == rotation.presented_secret_hash
                    && token.pepper_version == rotation.presented_pepper_version
            }) else {
                return Ok(RefreshRotationOutcome::Invalid);
            };
            if state.refresh_tokens[index].consumed {
                state.revoked = true;
                return Ok(RefreshRotationOutcome::Reused);
            }
            state.refresh_tokens[index].consumed = true;
            state.refresh_tokens.push(RefreshRecord {
                token_id: rotation.replacement_token_id,
                secret_hash: rotation.replacement_secret_hash,
                pepper_version: rotation.replacement_pepper_version,
                consumed: false,
            });
            let session = state.session.as_ref().unwrap();
            Ok(RefreshRotationOutcome::Rotated(SessionSubject {
                session_id: session.session_id,
                user_id: state.user.user_id,
                normalized_email: state.user.normalized_email.clone(),
                display_name: state.user.display_name.clone(),
                roles: state.user.roles.clone(),
                scopes: state.user.scopes.clone(),
                authz_version: state.user.authz_version,
                refresh_expires_at_ms: rotation.idle_expires_at_ms,
                absolute_expires_at_ms: session.absolute_expires_at_ms,
            }))
        }

        async fn revoke_session(
            &self,
            _session_id: Uuid,
            _now_ms: i64,
            _reason: &str,
        ) -> Result<(), IdentityError> {
            self.state.lock().unwrap().revoked = true;
            Ok(())
        }

        async fn revoke_session_by_refresh(
            &self,
            revocation: RefreshRevocation,
        ) -> Result<bool, IdentityError> {
            let mut state = self.state.lock().unwrap();
            let matches = state.refresh_tokens.iter().any(|token| {
                token.token_id == revocation.presented_token_id
                    && token.secret_hash == revocation.presented_secret_hash
                    && token.pepper_version == revocation.presented_pepper_version
            });
            if matches {
                state.revoked = true;
            }
            Ok(matches)
        }

        async fn active_session_principal(
            &self,
            session_id: Uuid,
            user_id: Uuid,
            authz_version: i64,
            _now_ms: i64,
        ) -> Result<Option<AuthenticatedPrincipal>, IdentityError> {
            let state = self.state.lock().unwrap();
            let active = !state.revoked
                && state.session.as_ref().is_some_and(|session| {
                    session.session_id == session_id && session.user_id == user_id
                })
                && state.user.authz_version == authz_version;
            let access = state.users.iter().find(|access| access.user_id == user_id);
            Ok(active.then(|| AuthenticatedPrincipal {
                user_id,
                session_id,
                email: state.user.normalized_email.clone(),
                display_name: state.user.display_name.clone(),
                roles: state.user.roles.clone(),
                scopes: state.user.scopes.clone(),
                authz_version,
                organizations: access
                    .map(|access| access.organizations.clone())
                    .unwrap_or_default(),
                projects: access
                    .map(|access| access.projects.clone())
                    .unwrap_or_default(),
            }))
        }

        async fn bootstrap_user(&self, user: BootstrapUser) -> Result<bool, IdentityError> {
            let mut state = self.state.lock().unwrap();
            if state
                .users
                .iter()
                .any(|existing| existing.email == user.normalized_email)
            {
                return Ok(false);
            }
            state.users.push(fake_user_access(&user));
            Ok(true)
        }

        async fn get_user_access(
            &self,
            user_id: Uuid,
        ) -> Result<Option<IdentityUserAccess>, IdentityError> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .users
                .iter()
                .find(|user| user.user_id == user_id)
                .cloned())
        }

        async fn list_users(
            &self,
            after_email: Option<&str>,
            limit: usize,
        ) -> Result<Vec<IdentityUserAccess>, IdentityError> {
            let mut users = self.state.lock().unwrap().users.clone();
            users.sort_by(|left, right| left.email.cmp(&right.email));
            Ok(users
                .into_iter()
                .filter(|user| after_email.is_none_or(|after| user.email.as_str() > after))
                .take(limit)
                .collect())
        }
    }

    fn fake_user_access(user: &BootstrapUser) -> IdentityUserAccess {
        let suffix = user.user_id.simple();
        let organization_id = format!("org_{suffix}");
        IdentityUserAccess {
            user_id: user.user_id,
            email: user.normalized_email.clone(),
            display_name: user.display_name.clone(),
            roles: user.roles.clone(),
            scopes: user.scopes.clone(),
            authz_version: 1,
            disabled: false,
            created_at_ms: user.created_at_ms,
            organizations: vec![OrganizationMembership {
                organization_id: organization_id.clone(),
                display_name: format!("{} workspace", user.display_name),
                role: "owner".to_string(),
                is_personal: true,
            }],
            projects: vec![ProjectMembership {
                organization_id,
                project_id: format!("proj_{suffix}"),
                display_name: "Default project".to_string(),
                role: "owner".to_string(),
                is_default: true,
            }],
        }
    }

    async fn fixture() -> (IdentityService, Arc<FakeRepository>) {
        let policy = AuthPolicy::default();
        let password_hash = PasswordEngine::new(1)
            .unwrap()
            .hash(PASSWORD.to_string())
            .await
            .unwrap();
        let owner_id = Uuid::new_v4();
        let owner = BootstrapUser {
            user_id: owner_id,
            normalized_email: "owner@example.com".to_string(),
            display_name: "Platform Owner".to_string(),
            password_hash: password_hash.clone(),
            roles: vec!["platform_owner".to_string()],
            scopes: vec!["admin:*".to_string()],
            created_at_ms: 1,
        };
        let repository = Arc::new(FakeRepository {
            state: Mutex::new(FakeState {
                user: CredentialUser {
                    user_id: owner.user_id,
                    normalized_email: owner.normalized_email.clone(),
                    display_name: owner.display_name.clone(),
                    password_hash,
                    password_version: 1,
                    roles: owner.roles.clone(),
                    scopes: owner.scopes.clone(),
                    authz_version: 1,
                    disabled: false,
                    failed_login_count: 0,
                    locked_until_ms: None,
                },
                users: vec![fake_user_access(&owner)],
                session: None,
                refresh_tokens: Vec::new(),
                revoked: false,
                failures: 0,
            }),
        });
        let access_tokens = AccessTokenCodec::new(
            "test-key",
            PRIVATE_KEY,
            [("test-key".to_string(), PUBLIC_KEY.to_vec())],
            "https://identity.example",
            "urn:aif:admin",
            &policy,
        )
        .unwrap();
        let refresh_tokens = RefreshTokenKeyring::new(1, [(1, vec![7; 32])]).unwrap();
        let service =
            IdentityService::new(repository.clone(), access_tokens, refresh_tokens, policy)
                .unwrap();
        (service, repository)
    }

    #[tokio::test]
    async fn login_refresh_replay_revokes_the_session_family() {
        let (service, _) = fixture().await;
        let login = service
            .login(LoginRequest {
                email: "OWNER@example.com".to_string(),
                password: PASSWORD.to_string(),
                client_id: AuthPolicy::default().client_id,
            })
            .await
            .unwrap();
        assert!(
            service
                .authenticate_access(&login.access_token)
                .await
                .is_ok()
        );

        let refreshed = service
            .refresh(RefreshRequest {
                refresh_token: login.refresh_token.clone(),
                client_id: AuthPolicy::default().client_id,
            })
            .await
            .unwrap();
        assert_ne!(refreshed.refresh_token, login.refresh_token);

        assert!(
            service
                .refresh(RefreshRequest {
                    refresh_token: login.refresh_token,
                    client_id: AuthPolicy::default().client_id,
                })
                .await
                .is_err()
        );
        assert!(
            service
                .authenticate_access(&refreshed.access_token)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn concurrent_refresh_has_one_winner_then_revokes_the_family() {
        let (service, _) = fixture().await;
        let login = service
            .login(LoginRequest {
                email: "owner@example.com".to_string(),
                password: PASSWORD.to_string(),
                client_id: AuthPolicy::default().client_id,
            })
            .await
            .unwrap();
        let request = RefreshRequest {
            refresh_token: login.refresh_token,
            client_id: AuthPolicy::default().client_id,
        };

        let (first, second) =
            tokio::join!(service.refresh(request.clone()), service.refresh(request));
        let outcomes = [first, second];
        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Err(IdentityError::InvalidAuthentication)))
                .count(),
            1
        );
        let winner = outcomes.into_iter().find_map(Result::ok).unwrap();
        assert!(
            service
                .authenticate_access(&winner.access_token)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn unknown_email_and_wrong_password_share_the_same_failure() {
        let (service, repository) = fixture().await;
        for (email, password) in [
            ("missing@example.com", PASSWORD),
            ("owner@example.com", "incorrect password value"),
        ] {
            let result = service
                .login(LoginRequest {
                    email: email.to_string(),
                    password: password.to_string(),
                    client_id: AuthPolicy::default().client_id,
                })
                .await;
            assert!(matches!(result, Err(IdentityError::InvalidAuthentication)));
        }
        assert_eq!(repository.state.lock().unwrap().failures, 2);
    }

    #[tokio::test]
    async fn refresh_logout_is_idempotent_and_revokes_access() {
        let (service, _) = fixture().await;
        let login = service
            .login(LoginRequest {
                email: "owner@example.com".to_string(),
                password: PASSWORD.to_string(),
                client_id: AuthPolicy::default().client_id,
            })
            .await
            .unwrap();

        service.logout_refresh(&login.refresh_token).await.unwrap();
        service.logout_refresh(&login.refresh_token).await.unwrap();
        service
            .logout_refresh("invalid-refresh-token")
            .await
            .unwrap();
        assert!(
            service
                .authenticate_access(&login.access_token)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn member_creation_returns_personal_workspace_access() {
        let (service, _) = fixture().await;
        let member = service
            .create_member_user(
                "MEMBER@example.com".to_string(),
                "Member User".to_string(),
                PASSWORD.to_string(),
            )
            .await
            .unwrap();

        assert_eq!(member.email, "member@example.com");
        assert_eq!(member.roles, ["member"]);
        assert_eq!(
            member.scopes,
            ["workspace:read".to_string(), "workspace:write".to_string()]
        );
        assert_eq!(member.organizations.len(), 1);
        assert!(member.organizations[0].is_personal);
        assert_eq!(member.projects.len(), 1);
        assert!(member.projects[0].is_default);

        assert_eq!(
            service.get_user_access(member.user_id).await.unwrap(),
            Some(member.clone())
        );
        assert_eq!(service.get_user_access(Uuid::new_v4()).await.unwrap(), None);
        let users = service.list_users(None, 100).await.unwrap();
        assert_eq!(users.len(), 2);
        let after_member = service
            .list_users(Some("member@example.com"), 100)
            .await
            .unwrap();
        assert_eq!(after_member.len(), 1);
        assert_eq!(after_member[0].email, "owner@example.com");
    }
}
