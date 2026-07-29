use std::{collections::BTreeMap, env, sync::Arc};

use base64::{Engine as _, engine::general_purpose};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use uuid::Uuid;

use crate::ImageGatewayError;

const SIGNING_KEYS_ENV: &str = "GATEWAY_WEBHOOK_SIGNING_KEYS";
const CURRENT_VERSION_ENV: &str = "GATEWAY_WEBHOOK_CURRENT_SIGNING_KEY_VERSION";
const KEY_BYTES: usize = 32;
const SECRET_PREFIX: &str = "whsec_";

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct WebhookSigningKeyring {
    current_version: u16,
    keys: Arc<BTreeMap<u16, Vec<u8>>>,
}

impl WebhookSigningKeyring {
    pub fn new(
        current_version: u16,
        keys: impl IntoIterator<Item = (u16, Vec<u8>)>,
    ) -> Result<Self, ImageGatewayError> {
        if current_version == 0 {
            return Err(ImageGatewayError::config(
                "Webhook signing key version must be greater than zero",
            ));
        }
        let mut keyed = BTreeMap::new();
        for (version, key) in keys {
            if keyed.insert(version, key).is_some() {
                return Err(ImageGatewayError::config(
                    "Webhook keyring contains a duplicate version",
                ));
            }
        }
        if keyed.is_empty()
            || !keyed.contains_key(&current_version)
            || keyed
                .iter()
                .any(|(version, key)| *version == 0 || key.len() != KEY_BYTES)
        {
            return Err(ImageGatewayError::config(
                "Webhook keyring must contain the current version and every key must decode to 32 bytes",
            ));
        }
        Ok(Self {
            current_version,
            keys: Arc::new(keyed),
        })
    }

    pub fn from_env() -> Result<Self, ImageGatewayError> {
        let raw = env::var(SIGNING_KEYS_ENV).map_err(|_| {
            ImageGatewayError::config(
                "GATEWAY_WEBHOOK_SIGNING_KEYS is required as version:64-hex entries",
            )
        })?;
        let current_version = env::var(CURRENT_VERSION_ENV).map_err(|_| {
            ImageGatewayError::config("GATEWAY_WEBHOOK_CURRENT_SIGNING_KEY_VERSION is required")
        })?;
        Self::parse(&current_version, &raw)
    }

    fn parse(current_version: &str, raw: &str) -> Result<Self, ImageGatewayError> {
        let current_version = current_version.trim().parse::<u16>().map_err(|_| {
            ImageGatewayError::config(
                "GATEWAY_WEBHOOK_CURRENT_SIGNING_KEY_VERSION must be an integer",
            )
        })?;
        let mut keys = Vec::new();
        for entry in raw.split(',') {
            let (version, encoded) = entry.split_once(':').ok_or_else(|| {
                ImageGatewayError::config(
                    "GATEWAY_WEBHOOK_SIGNING_KEYS must use version:64-hex entries",
                )
            })?;
            let version = version.trim().parse::<u16>().map_err(|_| {
                ImageGatewayError::config("Webhook signing key versions must be integers")
            })?;
            let key = hex::decode(encoded.trim()).map_err(|_| {
                ImageGatewayError::config("Webhook signing keys must be valid hexadecimal")
            })?;
            keys.push((version, key));
        }
        Self::new(current_version, keys)
    }

    pub fn ephemeral() -> Self {
        let mut key = Vec::with_capacity(KEY_BYTES);
        key.extend_from_slice(Uuid::new_v4().as_bytes());
        key.extend_from_slice(Uuid::new_v4().as_bytes());
        Self::new(1, [(1, key)]).expect("ephemeral webhook signing key is valid")
    }

    pub fn current_version(&self) -> u16 {
        self.current_version
    }

    pub fn signing_secret(
        &self,
        project_id: &str,
        endpoint_id: &str,
        version: u16,
        secret_revision: i64,
    ) -> Result<String, ImageGatewayError> {
        let secret = self.derive_secret_bytes(project_id, endpoint_id, version, secret_revision)?;
        Ok(format!(
            "{SECRET_PREFIX}{}",
            general_purpose::STANDARD.encode(secret)
        ))
    }

    pub fn signature_header(
        &self,
        project_id: &str,
        endpoint_id: &str,
        version: u16,
        secret_revision: i64,
        webhook_id: &str,
        webhook_timestamp: i64,
        body: &[u8],
    ) -> Result<String, ImageGatewayError> {
        let secret = self.derive_secret_bytes(project_id, endpoint_id, version, secret_revision)?;
        let mut mac = HmacSha256::new_from_slice(&secret)
            .map_err(|_| ImageGatewayError::internal("Webhook signing failed"))?;
        mac.update(webhook_id.as_bytes());
        mac.update(b".");
        mac.update(webhook_timestamp.to_string().as_bytes());
        mac.update(b".");
        mac.update(body);
        Ok(format!(
            "v1,{}",
            general_purpose::STANDARD.encode(mac.finalize().into_bytes())
        ))
    }

    fn derive_secret_bytes(
        &self,
        project_id: &str,
        endpoint_id: &str,
        version: u16,
        secret_revision: i64,
    ) -> Result<Vec<u8>, ImageGatewayError> {
        let root = self.keys.get(&version).ok_or_else(|| {
            ImageGatewayError::service_unavailable("Webhook signing key version is unavailable")
        })?;
        let mut mac = HmacSha256::new_from_slice(root)
            .map_err(|_| ImageGatewayError::internal("Webhook secret derivation failed"))?;
        mac.update(b"ai-image-factory/project-webhook/v1\0");
        mac.update(project_id.as_bytes());
        mac.update(b"\0");
        mac.update(endpoint_id.as_bytes());
        mac.update(b"\0");
        mac.update(secret_revision.to_string().as_bytes());
        Ok(mac.finalize().into_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_is_stable_for_endpoint_revision_and_changes_on_rotation() {
        let keyring = WebhookSigningKeyring::new(1, [(1, vec![7; 32])]).unwrap();
        let first = keyring.signing_secret("project-a", "we_a", 1, 1).unwrap();
        assert_eq!(
            first,
            keyring.signing_secret("project-a", "we_a", 1, 1).unwrap()
        );
        assert_ne!(
            first,
            keyring.signing_secret("project-a", "we_a", 1, 2).unwrap()
        );
        assert!(first.starts_with("whsec_"));
    }

    #[test]
    fn signature_uses_standard_webhooks_message_shape() {
        let keyring = WebhookSigningKeyring::new(1, [(1, vec![11; 32])]).unwrap();
        let actual = keyring
            .signature_header(
                "project-a",
                "we_a",
                1,
                1,
                "evt_a",
                1_700_000_000,
                br#"{"type":"test"}"#,
            )
            .unwrap();
        assert!(actual.starts_with("v1,"));
        assert_eq!(actual.split(',').count(), 2);
    }
}
