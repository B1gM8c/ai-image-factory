mod postgres;

use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub use postgres::PostgresEconomicSettlementStore;
pub(crate) use postgres::{
    admit_job_outputs, record_v4_provider_receipt_in_transaction, settle_receipt_in_transaction,
    validate_admitted_job_outputs,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EconomicReceiptOutcome {
    Succeeded,
    Failed,
    NoEffect,
    Uncertain,
}

impl EconomicReceiptOutcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::NoEffect => "no_effect",
            Self::Uncertain => "uncertain",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EconomicReceipt {
    submission_id: Uuid,
    outcome: EconomicReceiptOutcome,
    receipt_schema: String,
    payload_hash: String,
    evidence: Value,
}

impl EconomicReceipt {
    pub fn new(
        submission_id: Uuid,
        outcome: EconomicReceiptOutcome,
        receipt_schema: impl Into<String>,
        evidence: Value,
    ) -> Result<Self, EconomicSettlementError> {
        let receipt_schema = receipt_schema.into();
        let payload_hash = evidence_hash(&evidence)?;
        let receipt = Self {
            submission_id,
            outcome,
            receipt_schema,
            payload_hash,
            evidence,
        };
        postgres::validate_receipt(&receipt)?;
        Ok(receipt)
    }
}

fn evidence_hash(evidence: &Value) -> Result<String, EconomicSettlementError> {
    let bytes = serde_json::to_vec(evidence).map_err(|_| EconomicSettlementError::InvalidInput)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EconomicSettlement {
    pub receipt_id: Uuid,
    pub meter_event_id: Uuid,
    pub rated_usage_id: Option<Uuid>,
    pub customer_ledger_transaction_id: Option<Uuid>,
    pub outcome: EconomicReceiptOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderReceiptRecord {
    pub receipt_id: Uuid,
    pub outcome: EconomicReceiptOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EconomicSettlementError {
    #[error("economic settlement storage is unavailable")]
    Unavailable,
    #[error("economic settlement input is invalid")]
    InvalidInput,
    #[error("economic settlement conflicts with durable provider evidence")]
    Conflict,
    #[error("provider submission is not ready for economic settlement")]
    NotReady,
}

#[async_trait]
pub trait EconomicSettlementStore: Send + Sync + 'static {
    async fn settle(
        &self,
        receipt: &EconomicReceipt,
    ) -> Result<EconomicSettlement, EconomicSettlementError>;
}
