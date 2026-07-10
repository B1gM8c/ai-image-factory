use std::{collections::BTreeMap, env, sync::Arc};

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use uuid::Uuid;

use crate::ImageGatewayError;

pub(super) const HMAC_ALGORITHM: &str = "hmac-sha256-v1";
pub(super) const LEGACY_ALGORITHM: &str = "sha256";

const PEPPERS_ENV: &str = "GATEWAY_API_KEY_PEPPERS";
const CURRENT_VERSION_ENV: &str = "GATEWAY_API_KEY_CURRENT_PEPPER_VERSION";
const ALLOW_LEGACY_ENV: &str = "GATEWAY_API_KEY_ALLOW_LEGACY_SHA256";
const PEPPER_BYTES: usize = 32;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct ApiKeyKeyring {
    current_version: u16,
    peppers: Arc<BTreeMap<u16, Vec<u8>>>,
    allow_legacy_sha256: bool,
}

impl ApiKeyKeyring {
    pub fn new(
        current_version: u16,
        peppers: impl IntoIterator<Item = (u16, Vec<u8>)>,
    ) -> Result<Self, ImageGatewayError> {
        if current_version == 0 {
            return Err(ImageGatewayError::config(
                "API key pepper version must be greater than zero",
            ));
        }
        let mut keyed = BTreeMap::new();
        for (version, pepper) in peppers {
            if keyed.insert(version, pepper).is_some() {
                return Err(ImageGatewayError::config(
                    "API key keyring contains a duplicate pepper version",
                ));
            }
        }
        let peppers = keyed;
        if peppers.is_empty()
            || !peppers.contains_key(&current_version)
            || peppers
                .iter()
                .any(|(version, pepper)| *version == 0 || pepper.len() != PEPPER_BYTES)
        {
            return Err(ImageGatewayError::config(
                "API key keyring must contain the current version and every pepper must decode to 32 bytes",
            ));
        }
        Ok(Self {
            current_version,
            peppers: Arc::new(peppers),
            allow_legacy_sha256: false,
        })
    }

    pub fn from_env() -> Result<Self, ImageGatewayError> {
        let raw = env::var(PEPPERS_ENV).map_err(|_| {
            ImageGatewayError::config(
                "GATEWAY_API_KEY_PEPPERS is required as version:64-hex entries",
            )
        })?;
        let current_version = env::var(CURRENT_VERSION_ENV).map_err(|_| {
            ImageGatewayError::config("GATEWAY_API_KEY_CURRENT_PEPPER_VERSION is required")
        })?;
        let allow_legacy_sha256 = match env::var(ALLOW_LEGACY_ENV).as_deref() {
            Ok("1" | "true" | "TRUE" | "yes" | "YES") => true,
            Ok("0" | "false" | "FALSE" | "no" | "NO") | Err(_) => false,
            Ok(_) => {
                return Err(ImageGatewayError::config(
                    "GATEWAY_API_KEY_ALLOW_LEGACY_SHA256 must be a boolean",
                ));
            }
        };
        Ok(Self::parse(&current_version, &raw)?.with_legacy_sha256(allow_legacy_sha256))
    }

    fn parse(current_version: &str, raw: &str) -> Result<Self, ImageGatewayError> {
        let current_version = current_version.trim().parse::<u16>().map_err(|_| {
            ImageGatewayError::config("GATEWAY_API_KEY_CURRENT_PEPPER_VERSION must be an integer")
        })?;
        let mut peppers = Vec::new();
        for entry in raw.split(',') {
            let (version, encoded) = entry.split_once(':').ok_or_else(|| {
                ImageGatewayError::config("GATEWAY_API_KEY_PEPPERS must use version:64-hex entries")
            })?;
            let version = version.trim().parse::<u16>().map_err(|_| {
                ImageGatewayError::config("API key pepper versions must be integers")
            })?;
            let pepper = hex::decode(encoded.trim()).map_err(|_| {
                ImageGatewayError::config("API key peppers must be valid hexadecimal")
            })?;
            peppers.push((version, pepper));
        }
        Self::new(current_version, peppers)
    }

    pub fn ephemeral() -> Self {
        let mut pepper = Vec::with_capacity(PEPPER_BYTES);
        pepper.extend_from_slice(Uuid::new_v4().as_bytes());
        pepper.extend_from_slice(Uuid::new_v4().as_bytes());
        Self::new(1, [(1, pepper)]).expect("ephemeral API key keyring is valid")
    }

    pub fn current_version(&self) -> u16 {
        self.current_version
    }

    pub fn with_legacy_sha256(mut self, enabled: bool) -> Self {
        self.allow_legacy_sha256 = enabled;
        self
    }

    pub(super) fn legacy_sha256_enabled(&self) -> bool {
        self.allow_legacy_sha256
    }

    pub(super) fn digest_current(&self, token: &str) -> String {
        self.digest(self.current_version, token)
            .expect("current pepper must exist")
    }

    pub(super) fn verify(&self, version: u16, token: &str, expected_hex: &str) -> bool {
        let Some(pepper) = self.peppers.get(&version) else {
            return false;
        };
        let Ok(expected) = hex::decode(expected_hex) else {
            return false;
        };
        let Ok(mut mac) = HmacSha256::new_from_slice(pepper) else {
            return false;
        };
        mac.update(token.as_bytes());
        mac.verify_slice(&expected).is_ok()
    }

    fn digest(&self, version: u16, token: &str) -> Option<String> {
        let pepper = self.peppers.get(&version)?;
        let mut mac = HmacSha256::new_from_slice(pepper).ok()?;
        mac.update(token.as_bytes());
        Some(hex::encode(mac.finalize().into_bytes()))
    }
}

pub(super) fn new_key_value(key_id: &str) -> String {
    format!(
        "sk-gw-{key_id}.{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    )
}

pub(super) fn key_id_from_token(token: &str) -> Option<&str> {
    let token = token.strip_prefix("sk-gw-")?;
    let (key_id, secret) = token.split_once('.')?;
    if !key_id.starts_with("key_")
        || key_id.len() > 128
        || secret.len() != 64
        || !secret.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some(key_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pepper(byte: u8) -> Vec<u8> {
        vec![byte; PEPPER_BYTES]
    }

    #[test]
    fn token_contains_public_id_and_256_bit_secret() {
        let value = new_key_value("key_123");
        assert_eq!(key_id_from_token(&value), Some("key_123"));
        assert_eq!(value.rsplit_once('.').unwrap().1.len(), 64);
        assert!(key_id_from_token("sk-gw-key_123.short").is_none());
    }

    #[test]
    fn hmac_verification_is_versioned_and_rejects_wrong_secrets() {
        let v1 = ApiKeyKeyring::new(1, [(1, pepper(1))]).unwrap();
        let token = new_key_value("key_123");
        let digest = v1.digest_current(&token);
        assert!(v1.verify(1, &token, &digest));
        assert!(!v1.verify(1, "wrong", &digest));

        let rotated = ApiKeyKeyring::new(2, [(1, pepper(1)), (2, pepper(2))]).unwrap();
        assert!(rotated.verify(1, &token, &digest));
        assert_eq!(rotated.current_version(), 2);
        assert_ne!(rotated.digest_current(&token), digest);
    }

    #[test]
    fn keyring_rejects_missing_and_malformed_peppers() {
        assert!(ApiKeyKeyring::new(1, []).is_err());
        assert!(ApiKeyKeyring::new(2, [(1, pepper(1))]).is_err());
        assert!(ApiKeyKeyring::new(1, [(1, vec![1; 31])]).is_err());
        assert!(ApiKeyKeyring::new(1, [(1, pepper(1)), (1, pepper(2))]).is_err());
        assert!(ApiKeyKeyring::parse("not-a-version", "1:00").is_err());
        assert!(ApiKeyKeyring::parse("1", "missing-separator").is_err());
        assert!(ApiKeyKeyring::parse("1", "1:not-hex").is_err());
        assert!(ApiKeyKeyring::parse("2", &format!("1:{}", "11".repeat(32))).is_err());
        assert!(
            ApiKeyKeyring::parse("2", &format!("1:{},2:{}", "11".repeat(32), "22".repeat(32)))
                .is_ok()
        );
    }
}
