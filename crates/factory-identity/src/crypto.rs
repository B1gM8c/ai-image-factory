use std::{collections::BTreeMap, sync::Arc};

use argon2::{
    Algorithm as ArgonAlgorithm, Argon2, Params, Version,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
};
use sha2::Sha256;
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::{AccessClaims, AuthPolicy, IdentityError, SessionSubject};

const ACCESS_TOKEN_TYPE: &str = "at+jwt";
const REFRESH_PREFIX: &str = "aifr_";
const REFRESH_SECRET_BYTES: usize = 32;
const PEPPER_BYTES: usize = 32;
const ARGON_MEMORY_KIB: u32 = 64 * 1024;
const ARGON_TIME_COST: u32 = 3;
const ARGON_PARALLELISM: u32 = 1;
const ARGON_OUTPUT_BYTES: usize = 32;
type HmacSha256 = Hmac<Sha256>;

pub struct PasswordEngine {
    semaphore: Arc<Semaphore>,
    admission: Arc<Semaphore>,
    dummy_hash: String,
}

impl PasswordEngine {
    pub fn new(max_concurrency: usize) -> Result<Self, IdentityError> {
        if max_concurrency == 0 {
            return Err(IdentityError::Configuration);
        }
        let dummy_hash = hash_password_sync("identity-dummy-password-not-a-credential")?;
        Ok(Self {
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
            admission: Arc::new(Semaphore::new(max_concurrency.saturating_mul(8).min(1024))),
            dummy_hash,
        })
    }

    pub async fn hash(&self, password: String) -> Result<String, IdentityError> {
        validate_password(&password)?;
        let _admission = self
            .admission
            .try_acquire()
            .map_err(|_| IdentityError::Unavailable)?;
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| IdentityError::Unavailable)?;
        tokio::task::spawn_blocking(move || hash_password_sync(&password))
            .await
            .map_err(|_| IdentityError::Unavailable)?
    }

    pub async fn verify(
        &self,
        password: String,
        password_hash: Option<String>,
    ) -> Result<bool, IdentityError> {
        if password.len() > 1024 {
            return Err(IdentityError::InvalidInput);
        }
        let hash = password_hash.unwrap_or_else(|| self.dummy_hash.clone());
        let _admission = self
            .admission
            .try_acquire()
            .map_err(|_| IdentityError::Unavailable)?;
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| IdentityError::Unavailable)?;
        tokio::task::spawn_blocking(move || verify_password_sync(&password, &hash))
            .await
            .map_err(|_| IdentityError::Unavailable)?
    }
}

pub struct AccessTokenCodec {
    active_kid: String,
    issuer: String,
    audience: String,
    client_id: String,
    access_ttl_seconds: u64,
    clock_skew_seconds: u64,
    encoding_key: EncodingKey,
    decoding_keys: BTreeMap<String, DecodingKey>,
}

impl AccessTokenCodec {
    pub fn new(
        active_kid: impl Into<String>,
        private_key_pem: &[u8],
        public_keys: impl IntoIterator<Item = (String, Vec<u8>)>,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        policy: &AuthPolicy,
    ) -> Result<Self, IdentityError> {
        let active_kid = active_kid.into();
        let issuer = issuer.into();
        let audience = audience.into();
        if active_kid.is_empty()
            || active_kid.len() > 128
            || issuer.is_empty()
            || audience.is_empty()
        {
            return Err(IdentityError::Configuration);
        }
        let encoding_key =
            EncodingKey::from_ec_pem(private_key_pem).map_err(|_| IdentityError::Configuration)?;
        let mut decoding_keys = BTreeMap::new();
        for (kid, pem) in public_keys {
            if kid.is_empty() || kid.len() > 128 || decoding_keys.contains_key(&kid) {
                return Err(IdentityError::Configuration);
            }
            let key = DecodingKey::from_ec_pem(&pem).map_err(|_| IdentityError::Configuration)?;
            decoding_keys.insert(kid, key);
        }
        if !decoding_keys.contains_key(&active_kid) {
            return Err(IdentityError::Configuration);
        }
        let codec = Self {
            active_kid,
            issuer,
            audience,
            client_id: policy.client_id.clone(),
            access_ttl_seconds: policy.access_ttl_seconds,
            clock_skew_seconds: policy.clock_skew_seconds,
            encoding_key,
            decoding_keys,
        };
        let probe = SessionSubject {
            session_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            normalized_email: String::new(),
            display_name: String::new(),
            roles: vec!["key_probe".to_string()],
            scopes: vec!["key-probe:read".to_string()],
            authz_version: 1,
            refresh_expires_at_ms: i64::MAX,
            absolute_expires_at_ms: i64::MAX,
        };
        let token = codec.issue(&probe, jsonwebtoken::get_current_timestamp())?;
        codec
            .validate(&token)
            .map_err(|_| IdentityError::Configuration)?;
        Ok(codec)
    }

    pub fn issue(
        &self,
        subject: &SessionSubject,
        now_seconds: u64,
    ) -> Result<String, IdentityError> {
        let claims = AccessClaims {
            iss: self.issuer.clone(),
            sub: subject.user_id.to_string(),
            aud: self.audience.clone(),
            client_id: self.client_id.clone(),
            sid: subject.session_id.to_string(),
            jti: Uuid::new_v4().to_string(),
            iat: now_seconds,
            nbf: now_seconds,
            exp: now_seconds.saturating_add(self.access_ttl_seconds),
            scope: subject.scopes.join(" "),
            roles: subject.roles.clone(),
            authz_version: subject.authz_version,
        };
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.active_kid.clone());
        header.typ = Some(ACCESS_TOKEN_TYPE.to_string());
        encode(&header, &claims, &self.encoding_key).map_err(|_| IdentityError::Crypto)
    }

    pub fn validate(&self, token: &str) -> Result<AccessClaims, IdentityError> {
        if token.len() > 8192 {
            return Err(IdentityError::InvalidAuthentication);
        }
        let header = decode_header(token).map_err(|_| IdentityError::InvalidAuthentication)?;
        if header.alg != Algorithm::ES256 || header.typ.as_deref() != Some(ACCESS_TOKEN_TYPE) {
            return Err(IdentityError::InvalidAuthentication);
        }
        let kid = header.kid.ok_or(IdentityError::InvalidAuthentication)?;
        let key = self
            .decoding_keys
            .get(&kid)
            .ok_or(IdentityError::InvalidAuthentication)?;
        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_required_spec_claims(&["exp", "nbf", "iat", "jti", "aud", "iss", "sub"]);
        validation.leeway = self.clock_skew_seconds;
        validation.validate_nbf = true;
        let claims = decode::<AccessClaims>(token, key, &validation)
            .map_err(|_| IdentityError::InvalidAuthentication)?
            .claims;
        let now = jsonwebtoken::get_current_timestamp();
        if claims.client_id != self.client_id
            || claims.sid.parse::<Uuid>().is_err()
            || claims.sub.parse::<Uuid>().is_err()
            || claims.jti.parse::<Uuid>().is_err()
            || claims.roles.is_empty()
            || claims.scope.trim().is_empty()
            || claims.authz_version <= 0
            || claims.nbf != claims.iat
            || claims.iat > now.saturating_add(self.clock_skew_seconds)
            || claims.exp <= claims.iat
            || claims.exp.saturating_sub(claims.iat) > self.access_ttl_seconds
        {
            return Err(IdentityError::InvalidAuthentication);
        }
        Ok(claims)
    }
}

#[derive(Clone)]
pub struct RefreshTokenKeyring {
    current_version: u16,
    peppers: Arc<BTreeMap<u16, Vec<u8>>>,
}

impl RefreshTokenKeyring {
    pub fn new(
        current_version: u16,
        peppers: impl IntoIterator<Item = (u16, Vec<u8>)>,
    ) -> Result<Self, IdentityError> {
        let mut unique_peppers = BTreeMap::new();
        for (version, pepper) in peppers {
            if unique_peppers.insert(version, pepper).is_some() {
                return Err(IdentityError::Configuration);
            }
        }
        let peppers = unique_peppers;
        if current_version == 0
            || !peppers.contains_key(&current_version)
            || peppers
                .iter()
                .any(|(version, pepper)| *version == 0 || pepper.len() != PEPPER_BYTES)
        {
            return Err(IdentityError::Configuration);
        }
        Ok(Self {
            current_version,
            peppers: Arc::new(peppers),
        })
    }

    pub fn issue(&self) -> Result<IssuedRefreshToken, IdentityError> {
        let token_id = Uuid::new_v4();
        let mut secret = [0_u8; REFRESH_SECRET_BYTES];
        getrandom::fill(&mut secret).map_err(|_| IdentityError::Crypto)?;
        let value = format!(
            "{REFRESH_PREFIX}{token_id}.{}",
            URL_SAFE_NO_PAD.encode(secret)
        );
        let secret_hash = self.digest(self.current_version, &secret)?;
        Ok(IssuedRefreshToken {
            token_id,
            value,
            secret_hash,
            pepper_version: self.current_version,
        })
    }

    pub fn parse_and_digest(&self, token: &str) -> Result<PresentedRefreshToken, IdentityError> {
        if token.len() > 256 {
            return Err(IdentityError::InvalidAuthentication);
        }
        let (token_id, encoded) = token
            .strip_prefix(REFRESH_PREFIX)
            .and_then(|value| value.split_once('.'))
            .ok_or(IdentityError::InvalidAuthentication)?;
        let token_id = token_id
            .parse::<Uuid>()
            .map_err(|_| IdentityError::InvalidAuthentication)?;
        let secret = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| IdentityError::InvalidAuthentication)?;
        if secret.len() != REFRESH_SECRET_BYTES {
            return Err(IdentityError::InvalidAuthentication);
        }
        let mut digests = Vec::with_capacity(self.peppers.len());
        digests.push((
            self.current_version,
            self.digest(self.current_version, &secret)?,
        ));
        for version in self
            .peppers
            .keys()
            .copied()
            .filter(|version| *version != self.current_version)
        {
            digests.push((version, self.digest(version, &secret)?));
        }
        Ok(PresentedRefreshToken { token_id, digests })
    }

    pub fn derive_current(&self, domain: &[u8], value: &[u8]) -> Result<[u8; 32], IdentityError> {
        if domain.is_empty() || domain.len() > 128 || value.len() > 4096 {
            return Err(IdentityError::InvalidInput);
        }
        let pepper = self
            .peppers
            .get(&self.current_version)
            .ok_or(IdentityError::Crypto)?;
        let mut mac = HmacSha256::new_from_slice(pepper).map_err(|_| IdentityError::Crypto)?;
        mac.update(b"ai-image-factory-key-derivation-v1\0");
        mac.update(&(domain.len() as u32).to_be_bytes());
        mac.update(domain);
        mac.update(&(value.len() as u32).to_be_bytes());
        mac.update(value);
        Ok(mac.finalize().into_bytes().into())
    }

    fn digest(&self, version: u16, secret: &[u8]) -> Result<[u8; 32], IdentityError> {
        let pepper = self.peppers.get(&version).ok_or(IdentityError::Crypto)?;
        let mut mac = HmacSha256::new_from_slice(pepper).map_err(|_| IdentityError::Crypto)?;
        mac.update(secret);
        Ok(mac.finalize().into_bytes().into())
    }
}

pub struct IssuedRefreshToken {
    pub token_id: Uuid,
    pub value: String,
    pub secret_hash: [u8; 32],
    pub pepper_version: u16,
}

pub struct PresentedRefreshToken {
    pub token_id: Uuid,
    pub digests: Vec<(u16, [u8; 32])>,
}

fn validate_password(password: &str) -> Result<(), IdentityError> {
    if !(15..=1024).contains(&password.len()) {
        return Err(IdentityError::InvalidInput);
    }
    Ok(())
}

fn argon2() -> Result<Argon2<'static>, IdentityError> {
    let params = Params::new(
        ARGON_MEMORY_KIB,
        ARGON_TIME_COST,
        ARGON_PARALLELISM,
        Some(ARGON_OUTPUT_BYTES),
    )
    .map_err(|_| IdentityError::Crypto)?;
    Ok(Argon2::new(
        ArgonAlgorithm::Argon2id,
        Version::V0x13,
        params,
    ))
}

fn hash_password_sync(password: &str) -> Result<String, IdentityError> {
    let salt = SaltString::generate(&mut argon2::password_hash::rand_core::OsRng);
    argon2()?
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| IdentityError::Crypto)
}

fn verify_password_sync(password: &str, password_hash: &str) -> Result<bool, IdentityError> {
    let hash = PasswordHash::new(password_hash).map_err(|_| IdentityError::Crypto)?;
    if hash.algorithm.as_str() != "argon2id"
        || hash.version != Some(19)
        || hash.params.get_decimal("m") != Some(ARGON_MEMORY_KIB)
        || hash.params.get_decimal("t") != Some(ARGON_TIME_COST)
        || hash.params.get_decimal("p") != Some(ARGON_PARALLELISM)
        || hash.salt.is_none()
        || hash.hash.as_ref().map(|output| output.len()) != Some(ARGON_OUTPUT_BYTES)
    {
        return Err(IdentityError::Crypto);
    }
    Ok(argon2()?
        .verify_password(password.as_bytes(), &hash)
        .is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn subject() -> SessionSubject {
        SessionSubject {
            session_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            normalized_email: "admin@example.com".to_string(),
            display_name: "Admin".to_string(),
            roles: vec!["platform_owner".to_string()],
            scopes: vec!["admin:*".to_string()],
            authz_version: 1,
            refresh_expires_at_ms: i64::MAX,
            absolute_expires_at_ms: i64::MAX,
        }
    }

    #[test]
    fn access_token_uses_strict_es256_profile() {
        let policy = AuthPolicy::default();
        let codec = AccessTokenCodec::new(
            "key-1",
            PRIVATE_KEY,
            [("key-1".to_string(), PUBLIC_KEY.to_vec())],
            "https://issuer.example",
            "urn:aif:admin",
            &policy,
        )
        .unwrap();
        let now = jsonwebtoken::get_current_timestamp();
        let token = codec.issue(&subject(), now).unwrap();
        let header = decode_header(&token).unwrap();
        assert_eq!(header.alg, Algorithm::ES256);
        assert_eq!(header.typ.as_deref(), Some("at+jwt"));
        assert_eq!(header.kid.as_deref(), Some("key-1"));
        assert!(codec.validate(&token).is_ok());

        let other = AccessTokenCodec::new(
            "other",
            PRIVATE_KEY,
            [("other".to_string(), PUBLIC_KEY.to_vec())],
            "https://issuer.example",
            "urn:aif:admin",
            &policy,
        )
        .unwrap();
        assert!(other.validate(&token).is_err());
    }

    #[test]
    fn access_token_key_rotation_keeps_old_kid_valid_during_overlap() {
        let policy = AuthPolicy::default();
        let old = AccessTokenCodec::new(
            "key-1",
            PRIVATE_KEY,
            [("key-1".to_string(), PUBLIC_KEY.to_vec())],
            "https://issuer.example",
            "urn:aif:admin",
            &policy,
        )
        .unwrap();
        let old_token = old
            .issue(&subject(), jsonwebtoken::get_current_timestamp())
            .unwrap();

        let rotated = AccessTokenCodec::new(
            "key-2",
            PRIVATE_KEY,
            [
                ("key-1".to_string(), PUBLIC_KEY.to_vec()),
                ("key-2".to_string(), PUBLIC_KEY.to_vec()),
            ],
            "https://issuer.example",
            "urn:aif:admin",
            &policy,
        )
        .unwrap();
        assert!(rotated.validate(&old_token).is_ok());

        let new_token = rotated
            .issue(&subject(), jsonwebtoken::get_current_timestamp())
            .unwrap();
        assert_eq!(
            decode_header(&new_token).unwrap().kid.as_deref(),
            Some("key-2")
        );
    }

    #[test]
    fn refresh_token_has_256_bit_secret_and_versioned_digest() {
        let keyring = RefreshTokenKeyring::new(2, [(1, vec![1; 32]), (2, vec![2; 32])]).unwrap();
        let issued = keyring.issue().unwrap();
        let parsed = keyring.parse_and_digest(&issued.value).unwrap();
        assert_eq!(parsed.token_id, issued.token_id);
        assert_eq!(parsed.digests.len(), 2);
        assert_eq!(parsed.digests[0], (2, issued.secret_hash));
        assert!(keyring.parse_and_digest("aifr_invalid.short").is_err());
    }

    #[test]
    fn refresh_keyring_rejects_duplicate_versions() {
        assert!(matches!(
            RefreshTokenKeyring::new(1, [(1, vec![1; 32]), (1, vec![2; 32])]),
            Err(IdentityError::Configuration)
        ));
    }

    #[test]
    fn password_hash_uses_the_bounded_argon2id_profile() {
        let encoded = hash_password_sync("correct horse battery staple").unwrap();
        let hash = PasswordHash::new(&encoded).unwrap();
        assert_eq!(hash.algorithm.as_str(), "argon2id");
        assert_eq!(hash.version, Some(19));
        assert_eq!(hash.params.get_decimal("m"), Some(ARGON_MEMORY_KIB));
        assert_eq!(hash.params.get_decimal("t"), Some(ARGON_TIME_COST));
        assert_eq!(hash.params.get_decimal("p"), Some(ARGON_PARALLELISM));
        assert_eq!(
            hash.hash.as_ref().map(|output| output.len()),
            Some(ARGON_OUTPUT_BYTES)
        );
        assert!(verify_password_sync("correct horse battery staple", &encoded).unwrap());
        assert!(!verify_password_sync("wrong password", &encoded).unwrap());

        let unbounded_profile = encoded.replacen("m=65536", "m=1048576", 1);
        assert!(matches!(
            verify_password_sync("correct horse battery staple", &unbounded_profile),
            Err(IdentityError::Crypto)
        ));
    }

    #[tokio::test]
    async fn password_verification_rejects_oversized_input_before_argon2() {
        let engine = PasswordEngine::new(1).unwrap();
        let result = engine.verify("x".repeat(1025), None).await;
        assert!(matches!(result, Err(IdentityError::InvalidInput)));
    }
}
