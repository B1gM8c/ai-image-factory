use std::sync::Arc;

use gpt_image_2_gateway::{
    ApiKeyKeyring, AppConfig, ExternalControlPlaneServices, ExternalImageGatewayComponents,
    ImageGatewayError, PostgresAdminReadStore, PostgresApiKeyStore, PostgresProviderTaskStore,
    PostgresUsageStore, ProviderAccountRuntimeEventHub, RequestObservationSink,
    admission::PostgresAdmissionStore,
    artifacts::{FilesystemArtifactBlobStore, artifact_root_from_env},
    batches::{BatchFileBlobStore, PostgresBatchService},
    build_router_with_external_execution_and_services,
    database::{
        DEFAULT_MAX_CONNECTIONS, admin_read_database_url_from_env,
        connect_admin_read_pool_with_schema, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, verify_migrations,
    },
    init_telemetry,
    model_routing::PostgresModelRoutingStore,
    pricing::PostgresPricingAdminService,
    project_governance::PostgresProjectGovernanceService,
    project_limits::{PostgresProjectSpendBudgetService, ProjectSpendBudgetService},
    project_model_policy::PostgresProjectModelPolicyService,
    provider_management::PostgresProviderManagementService,
    provider_uploads::ProviderUploadService,
    settlement::PostgresExecutionSettlementStore,
    webhooks::{PostgresProjectWebhookService, WebhookDestinationPolicy, WebhookSigningKeyring},
};

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let config = AppConfig::from_env()?;
    config.validate_startup()?;
    let api_key_keyring = ApiKeyKeyring::from_env()?;
    let webhook_signing_keyring = WebhookSigningKeyring::from_env()?;
    let webhook_destination_policy = WebhookDestinationPolicy::from_env()?;
    let artifact_root = artifact_root_from_env()?;
    let artifact_store = Arc::new(FilesystemArtifactBlobStore::new(&artifact_root)?);
    let provider_upload_service = Arc::new(ProviderUploadService::from_env(&artifact_root)?);
    let telemetry = init_telemetry()?;

    let database_url = database_url_from_env()?;
    let database_schema = database_schema_from_env()?;
    let pool =
        connect_pool_with_schema(&database_url, DEFAULT_MAX_CONNECTIONS, &database_schema).await?;
    verify_migrations(&pool).await?;
    let admin_read_database_url = admin_read_database_url_from_env(&database_url);
    let admin_read_pool =
        connect_admin_read_pool_with_schema(&admin_read_database_url, &database_schema).await?;
    let identity_service = gpt_image_2_gateway::identity::service_from_env(pool.clone()).await?;
    let admin_read_store = Arc::new(PostgresAdminReadStore::new(admin_read_pool));
    let provider_account_runtime_events =
        ProviderAccountRuntimeEventHub::connect(&database_url, admin_read_store.clone())
            .await
            .map_err(|error| {
                ImageGatewayError::internal(format!(
                    "failed to initialize provider account runtime events: {error}"
                ))
            })?;
    let usage_store = Arc::new(PostgresUsageStore::new(pool.clone()));
    let api_key_store = Arc::new(PostgresApiKeyStore::new(pool.clone(), api_key_keyring));
    let batch_blob_store: Arc<dyn BatchFileBlobStore> = artifact_store.clone();
    let batch_service = Arc::new(PostgresBatchService::new(pool.clone(), batch_blob_store));
    let admission_store = Arc::new(PostgresAdmissionStore::new(pool.clone()));
    let provider_readiness_store = Arc::new(PostgresProviderTaskStore::new(pool.clone()));
    let provider_management_service =
        Arc::new(PostgresProviderManagementService::from_env(pool.clone()).await?);
    let model_routing_store = Arc::new(PostgresModelRoutingStore::new(pool.clone()));
    let pricing_admin_service = Arc::new(PostgresPricingAdminService::new(pool.clone()));
    let project_governance_service = Arc::new(PostgresProjectGovernanceService::new(pool.clone()));
    let billing_account_control_service =
        Arc::new(gpt_image_2_gateway::PostgresBillingAccountControlService::new(pool.clone()));
    let billing_integrity_service = Arc::new(
        gpt_image_2_gateway::PostgresBillingIntegrityService::new(pool.clone()),
    );
    let credit_grant_service = Arc::new(gpt_image_2_gateway::PostgresCreditGrantService::new(
        pool.clone(),
    ));
    let credit_grant_expirer = credit_grant_service.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            match credit_grant_expirer.expire_due(100).await {
                Ok(expired) if expired > 0 => {
                    tracing::info!(expired, "expired credit grants retired")
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(?error, "credit grant expiration pass failed"),
            }
        }
    });
    let customer_refund_service = Arc::new(
        gpt_image_2_gateway::PostgresCustomerRefundService::new(pool.clone()),
    );
    let provider_cost_allocation_service =
        Arc::new(gpt_image_2_gateway::PostgresProviderCostAllocationService::new(pool.clone()));
    let provider_cost_obligation_service =
        Arc::new(gpt_image_2_gateway::PostgresProviderCostObligationService::new(pool.clone()));
    let project_spend_budget_service =
        Arc::new(PostgresProjectSpendBudgetService::new(pool.clone()));
    let project_model_policy_service =
        Arc::new(PostgresProjectModelPolicyService::new(pool.clone()));
    let project_webhook_service = Arc::new(PostgresProjectWebhookService::new(
        pool.clone(),
        webhook_signing_keyring,
        webhook_destination_policy,
    ));
    let request_observation_sink = RequestObservationSink::from_env(pool.clone())?;
    let project_spend_budget_evaluator = project_spend_budget_service.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            match project_spend_budget_evaluator.evaluate_pending(100).await {
                Ok(evaluated) if evaluated > 0 => {
                    tracing::info!(evaluated, "project spend budget alerts evaluated")
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(?error, "project spend budget alert evaluation failed")
                }
            }
        }
    });
    let credential_broker = provider_management_service.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            match credential_broker.refresh_due_credentials_once().await {
                Ok(refreshed) if refreshed > 0 => {
                    tracing::info!(refreshed, "provider credentials refreshed")
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(?error, "provider credential refresh pass failed"),
            }
        }
    });
    let settlement_store = Arc::new(PostgresExecutionSettlementStore::new(
        pool,
        artifact_store.clone(),
    ));
    let bind = config.bind;
    let generation_contract = config.generation_admission_contract.as_str();
    let app = build_router_with_external_execution_and_services(
        config,
        ExternalImageGatewayComponents {
            usage_store,
            api_key_store,
            admission_store,
            settlement_store,
            input_blob_store: artifact_store,
            provider_readiness_store,
        },
        ExternalControlPlaneServices {
            identity_service,
            admin_read_store: Some(admin_read_store),
            provider_management_service: Some(provider_management_service),
            provider_upload_service: Some(provider_upload_service),
            provider_account_runtime_event_hub: Some(provider_account_runtime_events),
            model_routing_store: Some(model_routing_store),
            pricing_admin_service: Some(pricing_admin_service),
            billing_account_control_service: Some(billing_account_control_service),
            billing_integrity_service: Some(billing_integrity_service),
            credit_grant_service: Some(credit_grant_service),
            customer_refund_service: Some(customer_refund_service),
            provider_cost_allocation_service: Some(provider_cost_allocation_service),
            provider_cost_obligation_service: Some(provider_cost_obligation_service),
            project_governance_service: Some(project_governance_service),
            project_spend_budget_service: Some(project_spend_budget_service),
            project_model_policy_service: Some(project_model_policy_service),
            project_webhook_service: Some(project_webhook_service),
            batch_service: Some(batch_service),
            request_observation_sink: Some(request_observation_sink),
        },
    )?;

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|_| ImageGatewayError::config("failed to bind HTTP listener"))?;
    tracing::info!(%bind, generation.contract = generation_contract, "gpt-image-2 gateway listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|_| ImageGatewayError::internal("HTTP server failed"))?;

    telemetry.shutdown();
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
