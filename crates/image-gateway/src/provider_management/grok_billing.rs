use std::{collections::HashMap, path::Path, time::Duration};

use reqwest::{Client, redirect::Policy};
use serde::Deserialize;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{ImageGatewayError, executor::read_verified_auth};

const BILLING_URL: &str = "https://cli-chat-proxy.grok.com/v1/billing?format=credits";
const MAX_RESPONSE_BYTES: u64 = 256 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const WEEKLY_PERIOD: &str = "USAGE_PERIOD_TYPE_WEEKLY";

#[derive(Clone, Debug)]
pub(super) struct GrokQuotaSnapshot {
    pub plan_type: Option<String>,
    pub windows: Vec<GrokQuotaWindow>,
}

#[derive(Clone, Debug)]
pub(super) struct GrokQuotaWindow {
    pub limit_id: &'static str,
    pub limit_name: &'static str,
    pub window_role: &'static str,
    pub window_duration_mins: i64,
    pub used_percent: i32,
    pub resets_at_ms: i64,
}

#[derive(Debug)]
pub(super) struct GrokQuotaObservationError {
    code: &'static str,
    message: &'static str,
}

impl GrokQuotaObservationError {
    pub(super) const fn code(&self) -> &'static str {
        self.code
    }

    pub(super) fn into_gateway_error(self) -> ImageGatewayError {
        ImageGatewayError::service_unavailable(self.message)
    }
}

#[derive(Deserialize)]
struct StoredAuth {
    #[serde(flatten)]
    records: HashMap<String, StoredAuthRecord>,
}

#[derive(Deserialize)]
struct StoredAuthRecord {
    key: String,
}

#[derive(Deserialize)]
struct BillingEnvelope {
    config: BillingConfig,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BillingConfig {
    current_period: BillingPeriod,
    credit_usage_percent: Option<f64>,
    #[serde(alias = "subscription_tier")]
    subscription_tier: Option<String>,
    #[serde(default)]
    is_unified_billing_user: bool,
}

#[derive(Deserialize)]
struct BillingPeriod {
    #[serde(rename = "type")]
    kind: String,
    start: String,
    end: String,
}

pub(super) async fn observe_grok_quota(
    home: &Path,
    expected_auth_sha256: &str,
) -> Result<GrokQuotaSnapshot, GrokQuotaObservationError> {
    let auth = read_verified_auth(home, expected_auth_sha256)
        .map_err(|_| credential_error("Grok account credentials failed integrity validation"))?;
    let bearer = bearer_token(&auth)?;
    let client = Client::builder()
        .redirect(Policy::none())
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|_| upstream_error("Grok quota observer is unavailable"))?;
    let mut response = client
        .get(BILLING_URL)
        .bearer_auth(bearer)
        .header("x-xai-token-auth", "xai-grok-cli")
        .header("x-grok-client-mode", "cli")
        .send()
        .await
        .map_err(|_| upstream_error("Grok quota observation failed"))?;
    match response.status().as_u16() {
        200..=299 => {}
        401 | 403 => {
            return Err(observation_error(
                "grok_quota_auth_expired",
                "Grok login expired; reauthorize this account",
            ));
        }
        429 => {
            return Err(observation_error(
                "grok_quota_rate_limited",
                "Grok quota observation is rate limited",
            ));
        }
        500..=599 => return Err(upstream_error("Grok quota service is unavailable")),
        _ => {
            return Err(protocol_error(
                "Grok quota service returned an invalid response",
            ));
        }
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES)
    {
        return Err(protocol_error("Grok quota response is invalid"));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| upstream_error("Grok quota response is unavailable"))?
    {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES as usize {
            return Err(protocol_error("Grok quota response is invalid"));
        }
        body.extend_from_slice(&chunk);
    }
    parse_billing_response(&body)
}

fn bearer_token(auth: &[u8]) -> Result<String, GrokQuotaObservationError> {
    let stored: StoredAuth = serde_json::from_slice(auth)
        .map_err(|_| credential_error("Grok quota credentials are invalid"))?;
    let mut matching = stored.records.into_iter().filter(|(issuer, record)| {
        issuer.starts_with("https://auth.x.ai::") && !record.key.is_empty()
    });
    let Some((_, record)) = matching.next() else {
        return Err(credential_error("Grok quota credentials are invalid"));
    };
    if matching.next().is_some() || record.key.len() > 16 * 1024 {
        return Err(credential_error("Grok quota credentials are invalid"));
    }
    Ok(record.key)
}

fn parse_billing_response(body: &[u8]) -> Result<GrokQuotaSnapshot, GrokQuotaObservationError> {
    let response: BillingEnvelope = serde_json::from_slice(body)
        .map_err(|_| protocol_error("Grok quota response is invalid"))?;
    if response.config.current_period.kind != WEEKLY_PERIOD {
        return Err(protocol_error("Grok did not return a weekly quota period"));
    }
    let start = parse_timestamp(&response.config.current_period.start)?;
    let end = parse_timestamp(&response.config.current_period.end)?;
    let duration_mins = (end - start).whole_minutes();
    if !(10_000..=10_160).contains(&duration_mins) {
        return Err(protocol_error("Grok weekly quota period is invalid"));
    }
    let used_percent = match response.config.credit_usage_percent {
        Some(value) if value.is_finite() && (0.0..=100.0).contains(&value) => value.round() as i32,
        // Unified-billing accounts currently omit this field; Grok CLI's own
        // `/usage show` renders that response as zero percent used.
        None if response.config.is_unified_billing_user => 0,
        _ => return Err(protocol_error("Grok weekly quota usage is invalid")),
    };
    Ok(GrokQuotaSnapshot {
        plan_type: response.config.subscription_tier,
        windows: vec![GrokQuotaWindow {
            limit_id: "grok-weekly",
            limit_name: "Weekly limit",
            window_role: "primary",
            window_duration_mins: 10_080,
            used_percent,
            resets_at_ms: timestamp_ms(end)?,
        }],
    })
}

fn parse_timestamp(value: &str) -> Result<OffsetDateTime, GrokQuotaObservationError> {
    OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|_| protocol_error("Grok quota reset time is invalid"))
}

fn timestamp_ms(value: OffsetDateTime) -> Result<i64, GrokQuotaObservationError> {
    i64::try_from(value.unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| protocol_error("Grok quota reset time is invalid"))
}

const fn observation_error(code: &'static str, message: &'static str) -> GrokQuotaObservationError {
    GrokQuotaObservationError { code, message }
}

const fn credential_error(message: &'static str) -> GrokQuotaObservationError {
    observation_error("credential_integrity_failed", message)
}

const fn upstream_error(message: &'static str) -> GrokQuotaObservationError {
    observation_error("grok_quota_upstream_unavailable", message)
}

const fn protocol_error(message: &'static str) -> GrokQuotaObservationError {
    observation_error("grok_quota_protocol_changed", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_weekly_usage_and_reset() {
        let snapshot = parse_billing_response(
            br#"{
              "config": {
                "currentPeriod": {
                  "type": "USAGE_PERIOD_TYPE_WEEKLY",
                  "start": "2026-07-21T04:18:11.330823+00:00",
                  "end": "2026-07-28T04:18:11.330823+00:00"
                },
                "creditUsagePercent": 27.6,
                "subscriptionTier": "SuperGrok Heavy",
                "isUnifiedBillingUser": true
              }
            }"#,
        )
        .unwrap();
        assert_eq!(snapshot.plan_type.as_deref(), Some("SuperGrok Heavy"));
        assert_eq!(snapshot.windows.len(), 1);
        assert_eq!(snapshot.windows[0].window_duration_mins, 10_080);
        assert_eq!(snapshot.windows[0].used_percent, 28);
        assert_eq!(snapshot.windows[0].resets_at_ms, 1_785_212_291_330);
    }

    #[test]
    fn matches_cli_for_unified_billing_without_explicit_percent() {
        let snapshot = parse_billing_response(
            br#"{
              "config": {
                "currentPeriod": {
                  "type": "USAGE_PERIOD_TYPE_WEEKLY",
                  "start": "2026-07-21T04:18:11.330823+00:00",
                  "end": "2026-07-28T04:18:11.330823+00:00"
                },
                "isUnifiedBillingUser": true
              }
            }"#,
        )
        .unwrap();
        assert_eq!(snapshot.windows[0].used_percent, 0);
    }

    #[test]
    fn rejects_non_weekly_or_unbounded_usage() {
        let non_weekly = br#"{
          "config": {
            "currentPeriod": {
              "type": "USAGE_PERIOD_TYPE_MONTHLY",
              "start": "2026-07-01T00:00:00Z",
              "end": "2026-08-01T00:00:00Z"
            },
            "creditUsagePercent": 20,
            "isUnifiedBillingUser": false
          }
        }"#;
        assert!(parse_billing_response(non_weekly).is_err());

        let invalid_usage = br#"{
          "config": {
            "currentPeriod": {
              "type": "USAGE_PERIOD_TYPE_WEEKLY",
              "start": "2026-07-21T00:00:00Z",
              "end": "2026-07-28T00:00:00Z"
            },
            "creditUsagePercent": 101,
            "isUnifiedBillingUser": true
          }
        }"#;
        assert!(parse_billing_response(invalid_usage).is_err());
    }
}
