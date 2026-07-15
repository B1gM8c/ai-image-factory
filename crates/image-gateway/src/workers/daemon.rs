use std::{sync::Arc, time::Duration};

use uuid::Uuid;

use super::{
    GenerationWorker, INLINE_LEASE_GRACE, duration_ms, lease_lost_error, run_until_lease_lost,
};
use crate::{
    ImageGatewayError,
    admission::{
        AdmissionContract, AdmissionError, AdmissionStore, EDIT_COMMAND_SCHEMA, EditInputRoleV1,
        GENERATION_COMMAND_SCHEMA,
    },
    artifacts::ArtifactBlobStore,
    core::provider::validate_edit_job,
    execution::{EditExecutionContext, ExecutionContextError, ExecutionContextStore},
    executor::{ExecutorHandoffStore, ExecutorSubmissionError},
    generator::{EditJob, ImageGenerator, InputImage},
    input_blobs::{InputBlobReadError, InputBlobStore},
    settlement::ExecutionSettlementStore,
};

pub struct Workerd {
    worker_id: String,
    admission: Arc<dyn AdmissionStore>,
    contexts: Arc<dyn ExecutionContextStore>,
    generation: Option<GenerationWorker>,
    inputs: Option<Arc<dyn InputBlobStore>>,
    lease_duration: Duration,
    executor_handoff: Option<ExecutorHandoffTarget>,
    contract: AdmissionContract,
}

struct ExecutorHandoffTarget {
    store: Arc<dyn ExecutorHandoffStore>,
    execution_profile_id: Uuid,
}

impl Workerd {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        worker_id: String,
        generator: Arc<dyn ImageGenerator>,
        admission: Arc<dyn AdmissionStore>,
        contexts: Arc<dyn ExecutionContextStore>,
        settlement: Arc<dyn ExecutionSettlementStore>,
        artifacts: Arc<dyn ArtifactBlobStore>,
        inputs: Arc<dyn InputBlobStore>,
        request_timeout: Duration,
    ) -> Result<Self, ImageGatewayError> {
        let artifact_identity = artifacts.storage_identity();
        if artifact_identity != settlement.artifact_storage_identity()
            || artifact_identity != inputs.storage_identity()
        {
            return Err(ImageGatewayError::config(
                "workerd artifact, input, and settlement stores must use the same storage backend instance",
            ));
        }
        let generation = GenerationWorker::new(
            generator,
            admission.clone(),
            settlement,
            artifacts,
            request_timeout,
        );
        Ok(Self {
            worker_id,
            admission,
            contexts,
            generation: Some(generation),
            inputs: Some(inputs),
            lease_duration: request_timeout.saturating_add(INLINE_LEASE_GRACE),
            executor_handoff: None,
            contract: AdmissionContract::LegacyV1,
        })
    }

    pub fn new_handoff_only(
        worker_id: String,
        admission: Arc<dyn AdmissionStore>,
        contexts: Arc<dyn ExecutionContextStore>,
        store: Arc<dyn ExecutorHandoffStore>,
        execution_profile_id: Uuid,
        lease_duration: Duration,
    ) -> Result<Self, ImageGatewayError> {
        if execution_profile_id.is_nil() || lease_duration.is_zero() {
            return Err(ImageGatewayError::config(
                "workerd executor handoff configuration is invalid",
            ));
        }
        Ok(Self {
            worker_id,
            admission,
            contexts,
            generation: None,
            inputs: None,
            lease_duration,
            executor_handoff: Some(ExecutorHandoffTarget {
                store,
                execution_profile_id,
            }),
            contract: AdmissionContract::OutputEconomicsV2,
        })
    }

    pub async fn run_once(&self) -> Result<Option<Uuid>, ImageGatewayError> {
        let Some(lease) = self
            .admission
            .claim_ready(
                &self.worker_id,
                duration_ms(self.lease_duration),
                self.contract,
            )
            .await
            .map_err(map_admission_error)?
        else {
            return Ok(None);
        };
        match lease.command_schema.as_str() {
            GENERATION_COMMAND_SCHEMA => self.execute_generation(&lease).await?,
            EDIT_COMMAND_SCHEMA => self.execute_edit(&lease).await?,
            _ => {
                return Err(ImageGatewayError::internal(
                    "durable work command schema is unsupported",
                ));
            }
        }
        Ok(Some(lease.job_id))
    }

    async fn execute_generation(
        &self,
        lease: &crate::admission::WorkLease,
    ) -> Result<(), ImageGatewayError> {
        let context = match self.contexts.load_generation(lease).await {
            Ok(context) => context,
            Err(ExecutionContextError::Invalid { reservation }) => {
                if let Some(generation) = &self.generation {
                    generation
                        .reject_invalid_context(lease, &reservation)
                        .await?;
                }
                return Err(ImageGatewayError::internal(
                    "durable execution context failed integrity validation",
                ));
            }
            Err(ExecutionContextError::Unavailable) => {
                return Err(ImageGatewayError::service_unavailable(
                    "execution context unavailable",
                ));
            }
        };
        if context.economics_contract_version == 2 {
            return self.handoff_generation(lease).await;
        }
        let generation = self.generation.as_ref().ok_or_else(|| {
            ImageGatewayError::service_unavailable(
                "LegacyV1 inline generation is disabled for this workerd",
            )
        })?;
        self.admission
            .start(lease)
            .await
            .map_err(map_admission_error)?;
        generation
            .execute(
                lease,
                &context.reservation,
                context.job,
                &context.api_profile,
                &context.response_schema,
            )
            .await?;
        Ok(())
    }

    async fn execute_edit(
        &self,
        lease: &crate::admission::WorkLease,
    ) -> Result<(), ImageGatewayError> {
        let context = match self.contexts.load_edit(lease).await {
            Ok(context) => context,
            Err(ExecutionContextError::Invalid { reservation }) => {
                if let Some(generation) = &self.generation {
                    generation
                        .reject_invalid_context(lease, &reservation)
                        .await?;
                }
                return Err(ImageGatewayError::internal(
                    "durable edit execution context failed integrity validation",
                ));
            }
            Err(ExecutionContextError::Unavailable) => {
                return Err(ImageGatewayError::service_unavailable(
                    "edit execution context unavailable",
                ));
            }
        };
        if context.economics_contract_version == 2 {
            return Err(ImageGatewayError::service_unavailable(
                "V2 edit executor handoff is not configured",
            ));
        }
        let generation = self.generation.as_ref().ok_or_else(|| {
            ImageGatewayError::service_unavailable(
                "LegacyV1 inline edits are disabled for this workerd",
            )
        })?;
        let inputs = self.inputs.as_ref().ok_or_else(|| {
            ImageGatewayError::service_unavailable("workerd input storage is unavailable")
        })?;
        let mut hydration_heartbeat = generation.start_heartbeat(lease.clone());
        let hydration = run_until_lease_lost(
            &mut hydration_heartbeat.lost,
            hydrate_edit_job(inputs.as_ref(), &context),
        )
        .await;
        hydration_heartbeat.stop().await;
        let job = match hydration {
            Err(_) => return Err(lease_lost_error()),
            Ok(Ok(job)) => job,
            Ok(Err(EditHydrationError::Retryable(error))) => return Err(error),
            Ok(Err(EditHydrationError::Terminal { error, error_code })) => {
                self.admission
                    .start(lease)
                    .await
                    .map_err(map_admission_error)?;
                generation
                    .reject_before_provider(lease, &context.reservation, error_code)
                    .await?;
                return Err(error);
            }
        };
        self.admission
            .start(lease)
            .await
            .map_err(map_admission_error)?;
        let api_profile = context.command.source_api_profile.clone();
        generation
            .execute_edit(
                lease,
                &context.reservation,
                job,
                &api_profile,
                &context.response_schema,
            )
            .await?;
        Ok(())
    }

    async fn handoff_generation(
        &self,
        lease: &crate::admission::WorkLease,
    ) -> Result<(), ImageGatewayError> {
        let target = self.executor_handoff.as_ref().ok_or_else(|| {
            ImageGatewayError::service_unavailable("V2 generation executor handoff is unavailable")
        })?;
        target
            .store
            .prepare_and_handoff(lease, target.execution_profile_id)
            .await
            .map(drop)
            .map_err(map_handoff_error)
    }
}

enum EditHydrationError {
    Retryable(ImageGatewayError),
    Terminal {
        error: ImageGatewayError,
        error_code: &'static str,
    },
}

async fn hydrate_edit_job(
    inputs: &dyn InputBlobStore,
    context: &EditExecutionContext,
) -> Result<EditJob, EditHydrationError> {
    let mut images = Vec::new();
    let mut mask = None;
    for input in &context.inputs {
        let bytes = inputs.get(&input.blob).await.map_err(|error| match error {
            InputBlobReadError::Integrity => EditHydrationError::Terminal {
                error: ImageGatewayError::artifact_integrity(),
                error_code: "input_artifact_integrity",
            },
            InputBlobReadError::Unavailable => EditHydrationError::Retryable(
                ImageGatewayError::service_unavailable("input storage unavailable"),
            ),
        })?;
        let image = InputImage {
            filename: None,
            content_type: Some(input.media_type.clone()),
            bytes,
        };
        match input.role {
            EditInputRoleV1::Image => images.push(image),
            EditInputRoleV1::Mask => mask = Some(image),
        }
    }
    let command = &context.command;
    let job = EditJob {
        request_id: context.reservation.charge.request_id.clone(),
        model: command.model.clone(),
        prompt: command.prompt.clone(),
        moderation: command
            .moderation
            .clone()
            .unwrap_or_else(|| "auto".to_string()),
        images,
        mask,
        n: command.n,
        size: command.size.clone(),
        quality: command.quality.clone(),
        output_format: command.output_format.clone(),
        output_compression: command.output_compression,
        background: command.background.clone(),
        stream: command.stream,
        partial_images: command.partial_images,
    };
    validate_edit_job(&job).map_err(|error| EditHydrationError::Terminal {
        error,
        error_code: "invalid_edit_input",
    })?;
    Ok(job)
}

fn map_admission_error(error: AdmissionError) -> ImageGatewayError {
    match error {
        AdmissionError::Expired => ImageGatewayError::timeout(),
        AdmissionError::BillingLimitExceeded => ImageGatewayError::queue_overloaded(),
        AdmissionError::Unavailable => {
            ImageGatewayError::service_unavailable("durable work claim unavailable")
        }
        AdmissionError::PricingUnavailable => {
            ImageGatewayError::service_unavailable("durable work pricing unavailable")
        }
        AdmissionError::InvalidOwner
        | AdmissionError::StaleLease
        | AdmissionError::InvalidCommand => {
            ImageGatewayError::internal("durable work lease is stale or invalid")
        }
    }
}

fn map_handoff_error(error: ExecutorSubmissionError) -> ImageGatewayError {
    match error {
        ExecutorSubmissionError::Unavailable => {
            ImageGatewayError::service_unavailable("executor handoff storage unavailable")
        }
        ExecutorSubmissionError::StaleLease => ImageGatewayError::timeout(),
        ExecutorSubmissionError::Conflict | ExecutorSubmissionError::InvalidInput => {
            ImageGatewayError::internal("executor handoff failed integrity validation")
        }
    }
}
