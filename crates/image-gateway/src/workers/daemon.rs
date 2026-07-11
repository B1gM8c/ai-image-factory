use std::{sync::Arc, time::Duration};

use uuid::Uuid;

use super::{
    GenerationWorker, INLINE_LEASE_GRACE, duration_ms, lease_lost_error, run_until_lease_lost,
};
use crate::{
    ImageGatewayError,
    admission::{
        AdmissionError, AdmissionStore, EDIT_COMMAND_SCHEMA, EditInputRoleV1,
        GENERATION_COMMAND_SCHEMA,
    },
    artifacts::ArtifactBlobStore,
    core::provider::validate_edit_job,
    execution::{EditExecutionContext, ExecutionContextError, ExecutionContextStore},
    generator::{EditJob, ImageGenerator, InputImage},
    input_blobs::{InputBlobReadError, InputBlobStore},
    settlement::ExecutionSettlementStore,
};

pub struct Workerd {
    worker_id: String,
    admission: Arc<dyn AdmissionStore>,
    contexts: Arc<dyn ExecutionContextStore>,
    generation: GenerationWorker,
    inputs: Arc<dyn InputBlobStore>,
    lease_duration: Duration,
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
            generation,
            inputs,
            lease_duration: request_timeout.saturating_add(INLINE_LEASE_GRACE),
        })
    }

    pub async fn run_once(&self) -> Result<Option<Uuid>, ImageGatewayError> {
        let Some(lease) = self
            .admission
            .claim_ready(&self.worker_id, duration_ms(self.lease_duration))
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
                self.generation
                    .reject_invalid_context(lease, &reservation)
                    .await?;
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
        self.admission
            .start(lease)
            .await
            .map_err(map_admission_error)?;
        self.generation
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
                self.generation
                    .reject_invalid_context(lease, &reservation)
                    .await?;
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
        let mut hydration_heartbeat = self.generation.start_heartbeat(lease.clone());
        let hydration = run_until_lease_lost(
            &mut hydration_heartbeat.lost,
            hydrate_edit_job(self.inputs.as_ref(), &context),
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
                self.generation
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
        self.generation
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
        AdmissionError::Unavailable => {
            ImageGatewayError::service_unavailable("durable work claim unavailable")
        }
        AdmissionError::InvalidOwner
        | AdmissionError::StaleLease
        | AdmissionError::InvalidCommand => {
            ImageGatewayError::internal("durable work lease is stale or invalid")
        }
    }
}
