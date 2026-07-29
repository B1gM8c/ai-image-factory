mod postgres;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ImageGatewayError;

pub use postgres::PostgresCreditGrantService;
pub(crate) use postgres::{
    reserve_credit_grants, restore_credit_grants, settle_credit_grant_reservations,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreditGrantActor {
    pub user_id: Uuid,
    pub session_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateCreditGrantRequest {
    pub organization_id: String,
    pub currency: String,
    pub amount_micros: String,
    pub expires_at_ms: i64,
    pub source_reference: String,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RevokeCreditGrantRequest {
    pub reason: String,
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ListCreditGrantsRequest {
    pub organization_id: Option<String>,
    pub currency: Option<String>,
    pub state: Option<String>,
    pub after: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct CreditGrantView {
    pub object: &'static str,
    pub grant_id: String,
    pub organization_id: String,
    pub organization_display_name: Option<String>,
    pub currency: String,
    pub source_kind: String,
    pub source_reference: String,
    pub original_amount_micros: String,
    pub available_micros: String,
    pub reserved_micros: String,
    pub consumed_micros: String,
    pub restored_micros: String,
    pub expired_micros: String,
    pub revoked_micros: String,
    pub state: String,
    pub received_at_ms: i64,
    pub expires_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct CreditGrantSummary {
    pub original_amount_micros: String,
    pub available_micros: String,
    pub reserved_micros: String,
    pub consumed_micros: String,
    pub restored_micros: String,
    pub expired_micros: String,
    pub revoked_micros: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct CreditGrantList {
    pub object: &'static str,
    pub as_of_ms: i64,
    pub organization_id: Option<String>,
    pub currency: String,
    pub summary: CreditGrantSummary,
    pub data: Vec<CreditGrantView>,
    pub has_more: bool,
    pub next_after: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct OrganizationCreditGrantView {
    pub object: &'static str,
    pub grant_id: String,
    pub currency: String,
    pub original_amount_micros: String,
    pub available_micros: String,
    pub state: String,
    pub received_at_ms: i64,
    pub expires_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct OrganizationCreditGrantSummary {
    pub original_amount_micros: String,
    pub available_micros: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct OrganizationCreditGrantList {
    pub object: &'static str,
    pub as_of_ms: i64,
    pub organization_id: String,
    pub currency: String,
    pub summary: OrganizationCreditGrantSummary,
    pub data: Vec<OrganizationCreditGrantView>,
    pub has_more: bool,
    pub next_after: Option<String>,
}

impl OrganizationCreditGrantList {
    pub fn from_admin(value: CreditGrantList, organization_id: String) -> Self {
        Self {
            object: value.object,
            as_of_ms: value.as_of_ms,
            organization_id,
            currency: value.currency,
            summary: OrganizationCreditGrantSummary {
                original_amount_micros: value.summary.original_amount_micros,
                available_micros: value.summary.available_micros,
            },
            data: value
                .data
                .into_iter()
                .map(|grant| OrganizationCreditGrantView {
                    object: grant.object,
                    grant_id: grant.grant_id,
                    currency: grant.currency,
                    original_amount_micros: grant.original_amount_micros,
                    available_micros: grant.available_micros,
                    state: grant.state,
                    received_at_ms: grant.received_at_ms,
                    expires_at_ms: grant.expires_at_ms,
                })
                .collect(),
            has_more: value.has_more,
            next_after: value.next_after,
        }
    }
}

#[async_trait]
pub trait CreditGrantService: Send + Sync + 'static {
    async fn list(
        &self,
        request: ListCreditGrantsRequest,
    ) -> Result<CreditGrantList, ImageGatewayError>;

    async fn get(&self, grant_id: Uuid) -> Result<CreditGrantView, ImageGatewayError>;

    async fn create(
        &self,
        idempotency_key: &str,
        actor: CreditGrantActor,
        request: CreateCreditGrantRequest,
    ) -> Result<CreditGrantView, ImageGatewayError>;

    async fn revoke(
        &self,
        grant_id: Uuid,
        idempotency_key: &str,
        actor: CreditGrantActor,
        request: RevokeCreditGrantRequest,
    ) -> Result<CreditGrantView, ImageGatewayError>;
}

#[cfg(test)]
mod tests {
    use super::{
        CreditGrantList, CreditGrantSummary, CreditGrantView, OrganizationCreditGrantList,
    };

    #[test]
    fn organization_projection_excludes_internal_grant_fields() {
        let projected = OrganizationCreditGrantList::from_admin(
            CreditGrantList {
                object: "list",
                as_of_ms: 100,
                organization_id: Some("org-1".to_string()),
                currency: "USD".to_string(),
                summary: CreditGrantSummary {
                    original_amount_micros: "1000000".to_string(),
                    available_micros: "750000".to_string(),
                    reserved_micros: "100000".to_string(),
                    consumed_micros: "250000".to_string(),
                    restored_micros: "100000".to_string(),
                    expired_micros: "0".to_string(),
                    revoked_micros: "0".to_string(),
                },
                data: vec![CreditGrantView {
                    object: "billing.credit_grant",
                    grant_id: "grant-1".to_string(),
                    organization_id: "org-1".to_string(),
                    organization_display_name: Some("Customer".to_string()),
                    currency: "USD".to_string(),
                    source_kind: "promotional".to_string(),
                    source_reference: "internal-campaign".to_string(),
                    original_amount_micros: "1000000".to_string(),
                    available_micros: "750000".to_string(),
                    reserved_micros: "100000".to_string(),
                    consumed_micros: "250000".to_string(),
                    restored_micros: "100000".to_string(),
                    expired_micros: "0".to_string(),
                    revoked_micros: "0".to_string(),
                    state: "consuming".to_string(),
                    received_at_ms: 10,
                    expires_at_ms: 200,
                }],
                has_more: false,
                next_after: None,
            },
            "org-1".to_string(),
        );

        let json = serde_json::to_value(projected).expect("projection should serialize");
        assert_eq!(json["organization_id"], "org-1");
        assert_eq!(json["data"][0]["available_micros"], "750000");
        assert!(json["data"][0].get("source_reference").is_none());
        assert!(json["data"][0].get("reserved_micros").is_none());
        assert!(json["summary"].get("consumed_micros").is_none());
    }
}
