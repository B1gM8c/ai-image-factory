use axum::{Json, response::Html};
use image_provider_contracts::openai_codex;
use serde::Serialize;
use serde_json::{Value, json};
use utoipa::{Modify, OpenApi, ToSchema};

use crate::{
    admin_read::{
        AuditLogActor, AuditLogItem, AuditLogProject, AuditLogResource, AuditLogsSnapshot,
        BillingAccountSnapshot, BillingSnapshot, ConsoleJobEconomicsSnapshot, JobCursor,
        JobCustomerHold, JobCustomerQuote, JobCustomerRating, JobEconomicsSnapshot,
        JobLedgerTransaction, JobListItem, JobProviderCost, JobQuoteLine, JobUsageFact,
        JobsSnapshot, LedgerAggregate, OverviewSnapshot, ProviderAccountConcurrency,
        ProviderAccountRuntimeEvent, ProviderAccountView, ProviderAccountsSnapshot,
        ProviderCostAggregate, ProviderCostCoverage, ProviderQueuePressure, ProviderStateCount,
        RatedUsageAggregate, RequestLogCursor, RequestLogItem, RequestLogsSnapshot,
        SchedulerActiveJob, SchedulerCapacity, SchedulerSnapshot, StageCount, StateCount,
        UpstreamQuotaObservation, UsageActivityPoint, UsageAggregate, UsageAnalysisSnapshot,
        UsageFilterOption, UsageFilterOptions, UsageSpendPoint, WorkStateCount,
    },
    api_keys::{
        CreatedProjectApiKey, Project, ProjectApiKey, ProjectApiKeyDeleted, ProjectApiKeyList,
        ProjectApiKeyOwner, ProjectApiKeyServiceAccountOwner, ProjectApiKeyUserOwner, ProjectList,
        ProjectServiceAccount, ProjectServiceAccountDeleted, RotatedProjectApiKey,
        UpdatedProjectApiKey,
    },
    auth::{ApiKeyPermissionMode, ApiKeyPermissions},
    billing_control::{
        BillingAccountControlList, BillingAccountControlView, UpdateBillingAccountLimitRequest,
    },
    billing_integrity::{
        BillingIntegrityFindingView, BillingIntegrityRunDetail, BillingIntegrityRunList,
        BillingIntegrityRunView,
    },
    credit_grants::{
        CreateCreditGrantRequest, CreditGrantList, CreditGrantSummary, CreditGrantView,
        OrganizationCreditGrantList, OrganizationCreditGrantSummary, OrganizationCreditGrantView,
        RevokeCreditGrantRequest,
    },
    customer_refunds::{
        CreateCustomerRefundRequest, CustomerChargeDetail, CustomerChargeList, CustomerChargeView,
        CustomerRefundView,
    },
    models::{
        HealthResponse, ImageData, ImageStreamEvent, ImagesResponse, ModelData, ModelsResponse,
        ProviderProfileReadinessCounts, ReadinessResponse,
    },
    pricing::{
        ApplyOfficialPriceSnapshotRequest, CreatePriceBookRequest, CreatePriceBookVersionRequest,
        CreatePriceRollbackDraftRequest, OfficialPriceCatalogDescriptor, OfficialPriceCatalogs,
        OfficialPriceComponentDiffView, OfficialPriceSnapshotApplicationView,
        OfficialPriceSnapshotDiffView, OfficialPriceSnapshotPreview, OfficialPriceSnapshotSummary,
        OfficialPriceSyncRunSummary, PriceBookCatalog, PriceBookVersionView, PriceBookView,
        PriceComponentDraft, PriceComponentView, PricePreviewRequest, PricePreviewResult,
        PricePublishReadiness, PriceResolutionRequest, PriceRollbackDraftResult,
        PricingCoverageRow, PricingCoverageSnapshot, PricingCoverageSummary,
        TransitionPriceBookVersionRequest, UpdatePriceBookVersionRequest, UsageFact,
    },
    project_governance::{
        AddProjectMemberRequest, ProjectMemberList, ProjectMemberRole, ProjectMemberView,
        UpdateProjectMemberRequest,
    },
    project_limits::{
        ProjectSpendAlertEventView, ProjectSpendBudgetView, ProjectSpendNotificationList,
        ProjectSpendNotificationView, UpdateProjectSpendBudgetRequest,
    },
    project_model_policy::{
        ProjectModelIdentity, ProjectModelPolicyModelView, ProjectModelPolicyView,
        ProjectModelRateLimitView, UpdateProjectModelPolicyRequest, UpdateProjectModelRateLimit,
    },
    provider_cost_obligations::{
        ProviderCostObligationDetail, ProviderCostObligationEventView, ProviderCostObligationList,
        ProviderCostObligationSummary, ProviderCostObligationView,
    },
    webhooks::{
        CreateProjectWebhookRequest, CreatedProjectWebhook, DeletedProjectWebhook,
        ProjectWebhookDelivery, ProjectWebhookDeliveryList, ProjectWebhookEndpoint,
        ProjectWebhookList, ProjectWebhookTestEvent, RotatedProjectWebhookSecret,
        UpdateProjectWebhookRequest, WebhookDeliveryState, WebhookEndpointState,
    },
};

pub fn scalar_docs_html() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html>
  <head>
	      <title>AI Image Factory API Reference</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      body { margin: 0; }
    </style>
  </head>
  <body>
    <div id="app"></div>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
    <script>
      Scalar.createApiReference('#app', {
        url: '/openapi.json',
        theme: 'default',
        hideClientButton: false,
        telemetry: false
      })
    </script>
  </body>
</html>"#,
    )
}

pub fn openapi_json() -> Json<Value> {
    let mut value = serde_json::to_value(ApiDoc::openapi()).unwrap_or_else(|_| json!({}));
    patch_generated_schema(&mut value);
    Json(value)
}

#[derive(OpenApi)]
#[openapi(
    paths(
        create_image,
        edit_image,
        create_video,
        get_video,
        create_file_doc,
        list_files_doc,
        get_file_doc,
        delete_file_doc,
        get_file_content_doc,
        create_batch_doc,
        list_batches_doc,
        get_batch_doc,
        cancel_batch_doc,
        create_dreamina_image,
        create_dreamina_video,
        get_dreamina_video,
        get_dreamina_video_file_content,
        create_ark_image,
        create_ark_content_task,
        get_ark_content_task,
        get_ark_video_file_content,
        list_models,
        create_project,
        list_projects,
        get_project_doc,
        update_project_doc,
        list_project_members_doc,
        add_project_member_doc,
        update_project_member_doc,
        remove_project_member_doc,
        get_project_model_policy_doc,
        update_project_model_policy_doc,
        list_project_webhooks_doc,
        create_project_webhook_doc,
        update_project_webhook_doc,
        delete_project_webhook_doc,
        rotate_project_webhook_secret_doc,
        test_project_webhook_doc,
        list_project_webhook_deliveries_doc,
        get_project_limits_doc,
        update_project_limits_doc,
        list_project_spend_notifications_doc,
        mark_project_spend_notification_read_doc,
        create_project_service_account,
        delete_project_service_account,
        create_user_api_key_doc,
        list_project_api_keys,
        delete_project_api_key,
        update_project_api_key_doc,
        rotate_project_api_key_doc,
        admin_login,
        admin_refresh,
        admin_logout,
        admin_me,
        admin_overview,
        admin_billing_summary,
        admin_list_billing_accounts,
        admin_get_billing_account,
        admin_update_billing_account_limit,
        admin_list_credit_grants,
        admin_get_credit_grant,
        admin_create_credit_grant,
        admin_revoke_credit_grant,
        list_organization_credit_grants,
        admin_list_customer_charges,
        admin_get_customer_charge,
        admin_create_customer_refund,
        admin_list_billing_integrity_runs,
        admin_create_billing_integrity_run,
        admin_get_billing_integrity_run,
        admin_list_provider_cost_obligations,
        admin_get_provider_cost_obligation,
        admin_list_provider_cost_allocations,
        admin_get_provider_cost_allocation,
        admin_preview_provider_cost_allocation,
        admin_create_provider_cost_allocation_draft,
        admin_close_provider_cost_allocation,
        admin_usage,
        console_usage,
        admin_price_books,
        admin_pricing_coverage,
        admin_price_book_version_publish_readiness,
        admin_create_price_book,
        admin_create_price_book_version,
        admin_update_price_book_version,
        admin_publish_price_book_version,
        admin_retire_price_book_version,
        admin_create_price_rollback_draft,
        admin_preview_price,
        admin_official_price_catalogs,
        admin_observe_official_price_catalog,
        admin_apply_official_price_snapshot,
        admin_provider_accounts,
        admin_provider_account_runtime_events,
        admin_scheduler_queues,
        organization_audit_logs,
        admin_request_logs,
        console_request_logs,
        admin_jobs,
        admin_job_economics,
        console_job_economics,
        healthz,
        readyz,
    ),
    components(schemas(
        ImageGenerationProfileRequestDoc,
        ImageGenerationProfileResponseDoc,
        ImageGenerationRequestDoc,
        XaiImageGenerationRequestDoc,
        XaiImagesResponseDoc,
        XaiImageDataDoc,
        XaiImageStorageOptionsDoc,
        XaiPublicUrlOptionsDoc,
        XaiPublicUrlConfigDoc,
        XaiImageFileOutputDoc,
        XaiImageUsageDoc,
        XaiImageAspectRatioDoc,
        XaiImageResolutionDoc,
        XaiImageResponseFormatDoc,
        ImageEditRequestDoc,
        ImageReferenceDoc,
        VideoGenerationRequestDoc,
        VideoImageReferenceDoc,
        VideoOutputDoc,
        VideoStorageOptionsDoc,
        VideoStartResponseDoc,
        VideoStatusResponseDoc,
        GeneratedVideoDoc,
        VideoFileOutputDoc,
        VideoErrorDoc,
        VideoUsageDoc,
        VideoAspectRatioDoc,
        VideoResolutionDoc,
        CreateFileRequestDoc,
        FilePurposeDoc,
        FileObjectDoc,
        FileListDoc,
        DeletedFileObjectDoc,
        CreateBatchRequestDoc,
        OutputExpiresAfterDoc,
        BatchStatusDoc,
        BatchRequestCountsDoc,
        BatchObjectDoc,
        BatchListDoc,
        DreaminaImageGenerationRequestDoc,
        DreaminaImageModelDoc,
        DreaminaImageRatioDoc,
        DreaminaImageResolutionDoc,
        DreaminaVideoGenerationRequestDoc,
        DreaminaVideoModelDoc,
        DreaminaVideoRatioDoc,
        DreaminaVideoResolutionDoc,
        DreaminaTaskCreatedDoc,
        DreaminaVideoTaskDoc,
        DreaminaVideoContentDoc,
        DreaminaTaskErrorDoc,
        ArkImageGenerationRequestDoc,
        ArkStringOrStringsDoc,
        ArkSequentialImageGenerationOptionsDoc,
        ArkOptimizePromptOptionsDoc,
        ArkContentGenerationToolDoc,
        ArkImageGenerationResponseDoc,
        ArkImageDataDoc,
        ArkImageUsageDoc,
        ArkContentGenerationTaskRequestDoc,
        ArkContentItemDoc,
        ArkMediaUrlDoc,
        ArkDraftTaskRefDoc,
        ArkContentGenerationTaskIdDoc,
        ArkContentGenerationTaskDoc,
        ArkContentGenerationErrorDoc,
        ArkGeneratedContentDoc,
        ArkContentGenerationUsageDoc,
        ImageModelDoc,
        ImageQualityDoc,
        OutputFormatDoc,
        BackgroundDoc,
        ResponseFormatDoc,
        ModerationDoc,
        StyleDoc,
        CreateServiceAccountRequestDoc,
        CreateUserApiKeyRequestDoc,
        UpdateApiKeyRequestDoc,
        CreateProjectRequestDoc,
        AdminLoginRequestDoc,
        AdminRefreshRequestDoc,
        AdminLogoutRequestDoc,
        AdminTokenResponseDoc,
        AdminUserDoc,
        AdminSessionDoc,
        AdminPrincipalDoc,
        OverviewSnapshot,
        BillingSnapshot,
        CreatePriceBookRequest,
        CreatePriceBookVersionRequest,
        UpdatePriceBookVersionRequest,
        TransitionPriceBookVersionRequest,
        CreatePriceRollbackDraftRequest,
        PriceRollbackDraftResult,
        PriceComponentDraft,
        PriceBookCatalog,
        PricingCoverageSnapshot,
        PricingCoverageSummary,
        PricingCoverageRow,
        PricePublishReadiness,
        PriceBookView,
        PriceBookVersionView,
        PriceComponentView,
        PriceResolutionRequest,
        UsageFact,
        PricePreviewRequest,
        PricePreviewResult,
        OfficialPriceCatalogDescriptor,
        OfficialPriceCatalogs,
        OfficialPriceSnapshotSummary,
        OfficialPriceSnapshotDiffView,
        OfficialPriceComponentDiffView,
        OfficialPriceSnapshotApplicationView,
        OfficialPriceSyncRunSummary,
        OfficialPriceSnapshotPreview,
        ApplyOfficialPriceSnapshotRequest,
        BillingAccountSnapshot,
        BillingAccountControlView,
        UpdateBillingAccountLimitRequest,
        CreditGrantList,
        CreditGrantSummary,
        CreditGrantView,
        OrganizationCreditGrantList,
        OrganizationCreditGrantSummary,
        OrganizationCreditGrantView,
        CreateCreditGrantRequest,
        RevokeCreditGrantRequest,
        CustomerChargeList,
        CustomerChargeView,
        CustomerChargeDetail,
        CustomerRefundView,
        CreateCustomerRefundRequest,
        BillingIntegrityRunList,
        BillingIntegrityRunView,
        BillingIntegrityRunDetail,
        BillingIntegrityFindingView,
        ProviderCostObligationList,
        ProviderCostObligationSummary,
        ProviderCostObligationView,
        ProviderCostObligationDetail,
        ProviderCostObligationEventView,
        PreviewProviderCostAllocationRequestDoc,
        CreateProviderCostAllocationDraftRequestDoc,
        CloseProviderCostAllocationRequestDoc,
        ProviderCostAllocationLinePreviewDoc,
        ProviderCostAllocationPreviewDoc,
        ProviderCostAllocationSummaryDoc,
        ProviderCostAllocationLineDoc,
        ProviderCostAllocationClosureDoc,
        ProviderCostAllocationDetailDoc,
        ProviderCostAllocationListDoc,
        UsageAggregate,
        UsageAnalysisSnapshot,
        UsageActivityPoint,
        UsageSpendPoint,
        UsageFilterOptions,
        UsageFilterOption,
        RatedUsageAggregate,
        LedgerAggregate,
        ProviderCostAggregate,
        ProviderCostCoverage,
        ProviderAccountsSnapshot,
        ProviderAccountView,
        ProviderAccountConcurrency,
        ProviderQueuePressure,
        ProviderAccountRuntimeEvent,
        UpstreamQuotaObservation,
        SchedulerSnapshot,
        SchedulerActiveJob,
        SchedulerCapacity,
        WorkStateCount,
        StageCount,
        ProviderStateCount,
        JobsSnapshot,
        JobListItem,
        JobCursor,
        RequestLogsSnapshot,
        RequestLogItem,
        RequestLogCursor,
        AuditLogsSnapshot,
        AuditLogItem,
        AuditLogActor,
        AuditLogProject,
        AuditLogResource,
        JobQuoteLine,
        JobCustomerQuote,
        JobCustomerHold,
        JobUsageFact,
        JobCustomerRating,
        JobLedgerTransaction,
        JobProviderCost,
        JobEconomicsSnapshot,
        ConsoleJobEconomicsSnapshot,
        StateCount,
        ErrorResponseDoc,
        ErrorBodyDoc,
        ImagesResponse,
        ImageData,
        ImageStreamEvent,
        ModelsResponse,
        ModelData,
        HealthResponse,
        ReadinessResponse,
        ProviderProfileReadinessCounts,
        ProjectServiceAccount,
        ProjectServiceAccountDeleted,
        Project,
        ProjectList,
        CreatedProjectApiKey,
        ProjectApiKeyList,
        ProjectApiKey,
        ProjectApiKeyOwner,
        ProjectApiKeyServiceAccountOwner,
        ProjectApiKeyUserOwner,
        CreateProjectWebhookRequest,
        UpdateProjectWebhookRequest,
        WebhookEndpointState,
        ProjectWebhookEndpoint,
        CreatedProjectWebhook,
        RotatedProjectWebhookSecret,
        ProjectWebhookList,
        DeletedProjectWebhook,
        ProjectWebhookTestEvent,
        WebhookDeliveryState,
        ProjectWebhookDelivery,
        ProjectWebhookDeliveryList,
        ApiKeyPermissionMode,
        ApiKeyPermissions,
        ProjectApiKeyDeleted,
        UpdatedProjectApiKey,
        RotatedProjectApiKey,
        ProjectSpendAlertEventView,
        ProjectSpendBudgetView,
        ProjectSpendNotificationList,
        ProjectSpendNotificationView,
        UpdateProjectSpendBudgetRequest,
        AddProjectMemberRequest,
        UpdateProjectMemberRequest,
        ProjectMemberRole,
        ProjectMemberView,
        ProjectMemberList,
        ProjectModelIdentity,
        ProjectModelPolicyModelView,
        ProjectModelPolicyView,
        ProjectModelRateLimitView,
        UpdateProjectModelPolicyRequest,
        UpdateProjectModelRateLimit,
    )),
    modifiers(&SecurityAddon),
    tags(
        (name = "Images"),
        (name = "Videos"),
        (name = "Files"),
        (name = "Batches"),
        (name = "Dreamina CLI"),
        (name = "Volcengine Ark"),
        (name = "Models"),
        (name = "Admin"),
        (name = "Admin Identity"),
        (name = "Admin Operations"),
        (name = "Console"),
        (name = "Admin Pricing"),
        (name = "System"),
    ),
    servers((url = "/")),
    info(
        title = "AI Image Factory API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Provider-neutral media gateway exposing official-compatible image and asynchronous video APIs over isolated CLI execution. Video routes are runtime-gated by GATEWAY_ENABLE_XAI_VIDEO_API and default to disabled."
    )
)]
struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};

        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "BearerAuth",
                SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
            );
        }
    }
}

#[utoipa::path(
    post,
    path = "/v1/images/generations",
    tag = "Images",
    security(("BearerAuth" = [])),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Optional 1-255 character visible ASCII key. Same-key, same-body retries replay the retained result without another provider execution or charge; omitting the header makes every call independent.")
    ),
    request_body(content = ImageGenerationProfileRequestDoc, content_type = "application/json"),
    responses(
        (status = 200, description = "Base64 encoded generated images or final-only SSE when stream=true", content(
            (ImageGenerationProfileResponseDoc = "application/json"),
            (ImageStreamEvent = "text/event-stream")
        )),
        (status = 400, description = "Invalid image request", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 409, description = "Idempotency key is in progress, conflicts with another body, or its result cannot yet be replayed", body = ErrorResponseDoc),
        (status = 410, description = "The accepted idempotent result has passed its bounded retention window", body = ErrorResponseDoc),
        (status = 429, description = "Gateway queue or quota limit reached", body = ErrorResponseDoc),
        (status = 500, description = "A retained live artifact failed integrity validation", body = ErrorResponseDoc),
        (status = 502, description = "Codex CLI image backend failed", body = ErrorResponseDoc),
        (status = 504, description = "Codex CLI image backend timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_image() {}

#[utoipa::path(
    post,
    path = "/v1/images/edits",
    tag = "Images",
    security(("BearerAuth" = [])),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Optional idempotency key with the same retained replay and expiry semantics as image generation")
    ),
    request_body(
        description = "Multipart image upload or JSON base64/data URL references. Remote URLs and file_id are gateway limitations when using native Codex CLI.",
        content(
            (ImageEditRequestDoc = "application/json"),
            (ImageEditRequestDoc = "multipart/form-data")
        )
    ),
    responses(
        (status = 200, description = "Base64 encoded edited images or final-only SSE when stream=true", content(
            (ImagesResponse = "application/json"),
            (ImageStreamEvent = "text/event-stream")
        )),
        (status = 400, description = "Invalid image edit request", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 409, description = "Idempotency key is in progress, conflicts with another body, or its result cannot yet be replayed", body = ErrorResponseDoc),
        (status = 410, description = "The accepted idempotent edit result has passed its bounded retention window", body = ErrorResponseDoc),
        (status = 413, description = "Upload payload is too large", body = ErrorResponseDoc),
        (status = 415, description = "Unsupported content type", body = ErrorResponseDoc),
        (status = 429, description = "Gateway queue or quota limit reached", body = ErrorResponseDoc),
        (status = 500, description = "A retained live artifact failed integrity validation", body = ErrorResponseDoc),
        (status = 502, description = "Codex CLI image backend failed", body = ErrorResponseDoc),
        (status = 504, description = "Codex CLI image backend timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn edit_image() {}

#[utoipa::path(
    post,
    path = "/v1/videos/generations",
    tag = "Videos",
    security(("BearerAuth" = [])),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Optional idempotency key scoped to the authenticated project and video API profile")
    ),
    request_body(
        content = VideoGenerationRequestDoc,
        content_type = "application/json",
        description = "xAI-compatible asynchronous video request. Grok CLI input images currently require base64 data URLs; represented file_id, output, and storage options fail closed when the binding cannot honor them."
    ),
    responses(
        (status = 200, description = "Video request accepted", body = VideoStartResponseDoc),
        (status = 400, description = "Invalid video request", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 409, description = "Idempotency key is in progress or conflicts with another body", body = ErrorResponseDoc),
        (status = 413, description = "Input image payload is too large", body = ErrorResponseDoc),
        (status = 429, description = "Video-second quota or admission capacity reached", body = ErrorResponseDoc),
        (status = 503, description = "Durable video admission is unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_video() {}

#[utoipa::path(
    get,
    path = "/v1/videos/{request_id}",
    tag = "Videos",
    security(("BearerAuth" = [])),
    params(("request_id" = String, Path, description = "Opaque request id returned by video creation")),
    responses(
        (status = 200, description = "Current asynchronous video status", body = VideoStatusResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 404, description = "Video request not found in the authenticated tenant", body = ErrorResponseDoc),
        (status = 503, description = "Video result state is unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn get_video() {}

#[utoipa::path(
    post,
    path = "/v1/files",
    tag = "Files",
    security(("BearerAuth" = [])),
    request_body(
        content = CreateFileRequestDoc,
        content_type = "multipart/form-data",
        description = "OpenAI-compatible file upload. This deployment currently accepts purpose=batch only. Batch input must use a .jsonl filename, contain UTF-8 JSONL content, and may not exceed 8 MiB."
    ),
    responses(
        (status = 200, description = "Uploaded file", body = FileObjectDoc),
        (status = 400, description = "Missing or invalid multipart field, purpose, filename, or batch file", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 413, description = "Batch input file exceeds the 8 MiB deployment limit", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_file_doc() {}

#[utoipa::path(
    get,
    path = "/v1/files",
    tag = "Files",
    security(("BearerAuth" = [])),
    params(
        ("purpose" = Option<FilePurposeDoc>, Query, description = "Filter files by purpose"),
        ("after" = Option<String>, Query, description = "Opaque file id cursor"),
        ("limit" = Option<usize>, Query, description = "Page size; defaults to 20 and is clamped to 1..10000"),
        ("order" = Option<String>, Query, description = "Only desc is currently supported")
    ),
    responses(
        (status = 200, description = "Project-scoped file list in descending creation order", body = FileListDoc),
        (status = 400, description = "Invalid purpose, cursor, or unsupported order", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn list_files_doc() {}

#[utoipa::path(
    get,
    path = "/v1/files/{file_id}",
    tag = "Files",
    security(("BearerAuth" = [])),
    params(("file_id" = String, Path, description = "Project-scoped file id")),
    responses(
        (status = 200, description = "File metadata", body = FileObjectDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 404, description = "File not found in the authenticated project", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn get_file_doc() {}

#[utoipa::path(
    delete,
    path = "/v1/files/{file_id}",
    tag = "Files",
    security(("BearerAuth" = [])),
    params(("file_id" = String, Path, description = "Project-scoped file id")),
    responses(
        (status = 200, description = "File deletion confirmation", body = DeletedFileObjectDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 404, description = "File not found in the authenticated project", body = ErrorResponseDoc),
        (status = 409, description = "File is still referenced by a batch", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn delete_file_doc() {}

#[utoipa::path(
    get,
    path = "/v1/files/{file_id}/content",
    tag = "Files",
    security(("BearerAuth" = [])),
    params(("file_id" = String, Path, description = "Project-scoped file id. Legacy video artifact ids on this route continue to return MP4 content.")),
    responses(
        (status = 200, description = "Raw file bytes. Batch input, output, and error files are returned without JSON wrapping.", body = Vec<u8>, content_type = "application/octet-stream"),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 404, description = "File not found in the authenticated project", body = ErrorResponseDoc),
        (status = 410, description = "Known retained artifact has expired", body = ErrorResponseDoc),
        (status = 500, description = "A retained artifact failed integrity validation", body = ErrorResponseDoc),
        (status = 503, description = "Artifact storage is unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn get_file_content_doc() {}

#[utoipa::path(
    post,
    path = "/v1/batches",
    tag = "Batches",
    security(("BearerAuth" = [])),
    request_body(
        content = CreateBatchRequestDoc,
        content_type = "application/json",
        description = "OpenAI-compatible Batch creation with current deployment limits: endpoint must be /v1/images/generations, completion_window must be 24h, input_file_id must reference a purpose=batch UTF-8 JSONL file no larger than 8 MiB, and the file may contain at most 1000 unique custom_id requests using one model."
    ),
    responses(
        (status = 200, description = "Batch accepted for asynchronous validation and execution", body = BatchObjectDoc),
        (status = 400, description = "Invalid endpoint, completion window, input file, metadata, or JSONL request", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 404, description = "Input file or model route not found in the authenticated project", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_batch_doc() {}

#[utoipa::path(
    get,
    path = "/v1/batches",
    tag = "Batches",
    security(("BearerAuth" = [])),
    params(
        ("after" = Option<String>, Query, description = "Opaque batch id cursor"),
        ("limit" = Option<usize>, Query, description = "Page size; defaults to 20 and is clamped to 1..100")
    ),
    responses(
        (status = 200, description = "Project-scoped batch list", body = BatchListDoc),
        (status = 400, description = "Invalid cursor", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn list_batches_doc() {}

#[utoipa::path(
    get,
    path = "/v1/batches/{batch_id}",
    tag = "Batches",
    security(("BearerAuth" = [])),
    params(("batch_id" = String, Path, description = "Project-scoped batch id")),
    responses(
        (status = 200, description = "Current asynchronous batch state and request counts", body = BatchObjectDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 404, description = "Batch not found in the authenticated project", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn get_batch_doc() {}

#[utoipa::path(
    post,
    path = "/v1/batches/{batch_id}/cancel",
    tag = "Batches",
    security(("BearerAuth" = [])),
    params(("batch_id" = String, Path, description = "Project-scoped batch id")),
    responses(
        (status = 200, description = "Batch after cancellation was requested or confirmed", body = BatchObjectDoc),
        (status = 400, description = "Batch is already terminal and cannot be cancelled", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 404, description = "Batch not found in the authenticated project", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn cancel_batch_doc() {}

#[utoipa::path(
    post,
    path = "/v1/dreamina/images/generations",
    tag = "Dreamina CLI",
    security(("BearerAuth" = [])),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Optional idempotency key scoped to the authenticated project and Dreamina image profile")
    ),
    request_body(
        content = DreaminaImageGenerationRequestDoc,
        content_type = "application/json",
        description = "Dreamina CLI-native text-to-image request. Use ratio or a complete width/height pair, never both. Images are returned as base64 JSON and temporary local files are removed by the executor."
    ),
    responses(
        (status = 200, description = "Base64 encoded generated images", body = ImagesResponse),
        (status = 400, description = "Invalid Dreamina image request", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 409, description = "Idempotency key is in progress or conflicts", body = ErrorResponseDoc),
        (status = 429, description = "Image quota or admission capacity reached", body = ErrorResponseDoc),
        (status = 503, description = "Dreamina execution route is unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_dreamina_image() {}

#[utoipa::path(
    post,
    path = "/v1/dreamina/videos/generations",
    tag = "Dreamina CLI",
    security(("BearerAuth" = [])),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Optional idempotency key scoped to the authenticated project and Dreamina video profile")
    ),
    request_body(
        content = DreaminaVideoGenerationRequestDoc,
        content_type = "application/json",
        description = "Dreamina CLI-native asynchronous Seedance text-to-video request."
    ),
    responses(
        (status = 200, description = "Video task accepted", body = DreaminaTaskCreatedDoc),
        (status = 400, description = "Invalid Dreamina video request", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 409, description = "Idempotency key is in progress or conflicts", body = ErrorResponseDoc),
        (status = 429, description = "Video-second quota or admission capacity reached", body = ErrorResponseDoc),
        (status = 503, description = "Dreamina execution route is unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_dreamina_video() {}

#[utoipa::path(
    get,
    path = "/v1/dreamina/videos/{task_id}",
    tag = "Dreamina CLI",
    security(("BearerAuth" = [])),
    params(("task_id" = String, Path, description = "Task id returned by Dreamina video creation")),
    responses(
        (status = 200, description = "Current Dreamina video task status", body = DreaminaVideoTaskDoc),
        (status = 400, description = "Invalid or unknown task id", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 503, description = "Video result state is unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn get_dreamina_video() {}

#[utoipa::path(
    get,
    path = "/v1/dreamina/files/{file_id}/content",
    tag = "Dreamina CLI",
    security(("BearerAuth" = [])),
    params(("file_id" = String, Path, description = "Tenant-scoped artifact id returned by the completed task")),
    responses(
        (status = 200, description = "Generated MP4 bytes", body = Vec<u8>, content_type = "video/mp4"),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 404, description = "Video file not found in the authenticated tenant", body = ErrorResponseDoc),
        (status = 410, description = "Video artifact retention has expired", body = ErrorResponseDoc),
        (status = 500, description = "Retained artifact failed integrity validation", body = ErrorResponseDoc),
        (status = 503, description = "Artifact storage is unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn get_dreamina_video_file_content() {}

#[utoipa::path(
    post,
    path = "/api/v3/images/generations",
    tag = "Volcengine Ark",
    security(("BearerAuth" = [])),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Optional project-scoped idempotency key")
    ),
    request_body(
        content = ArkImageGenerationRequestDoc,
        content_type = "application/json",
        description = "Volcengine Ark ImageGenerations wire contract. The current Dreamina CLI binding supports text-to-image, b64_json, non-streaming output, 1K/2K/4K or WIDTHxHEIGHT, and deterministic multi-image counts. Represented fields that the CLI cannot honor fail closed."
    ),
    responses(
        (status = 200, description = "Ark-compatible base64 image response", body = ArkImageGenerationResponseDoc),
        (status = 400, description = "Invalid or represented-but-unbound Ark parameter", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 409, description = "Idempotency key is in progress or conflicts", body = ErrorResponseDoc),
        (status = 429, description = "Image quota or admission capacity reached", body = ErrorResponseDoc),
        (status = 503, description = "Dreamina execution route is unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_ark_image() {}

#[utoipa::path(
    post,
    path = "/api/v3/contents/generations/tasks",
    tag = "Volcengine Ark",
    security(("BearerAuth" = [])),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Optional project-scoped idempotency key")
    ),
    request_body(
        content = ArkContentGenerationTaskRequestDoc,
        content_type = "application/json",
        description = "Volcengine Ark asynchronous content-generation contract. The current Dreamina CLI binding accepts exactly one text content item and Seedance 2.0 720p controls; reference media, callbacks, audio, draft, tools, frames, and other unbound fields fail closed."
    ),
    responses(
        (status = 200, description = "Content generation task accepted", body = ArkContentGenerationTaskIdDoc),
        (status = 400, description = "Invalid or represented-but-unbound Ark parameter", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 409, description = "Idempotency key is in progress or conflicts", body = ErrorResponseDoc),
        (status = 429, description = "Video-second quota or admission capacity reached", body = ErrorResponseDoc),
        (status = 503, description = "Dreamina execution route is unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_ark_content_task() {}

#[utoipa::path(
    get,
    path = "/api/v3/contents/generations/tasks/{task_id}",
    tag = "Volcengine Ark",
    security(("BearerAuth" = [])),
    params(("task_id" = String, Path, description = "Ark-shaped cgt-* task id returned by task creation")),
    responses(
        (status = 200, description = "Current content generation task", body = ArkContentGenerationTaskDoc),
        (status = 400, description = "Invalid task id", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 404, description = "Task not found in the authenticated tenant", body = ErrorResponseDoc),
        (status = 503, description = "Video result state is unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn get_ark_content_task() {}

#[utoipa::path(
    get,
    path = "/api/v3/files/{file_id}/content",
    tag = "Volcengine Ark",
    security(("BearerAuth" = [])),
    params(("file_id" = String, Path, description = "Tenant-scoped local artifact id returned by a succeeded task")),
    responses(
        (status = 200, description = "Generated MP4 bytes", body = Vec<u8>, content_type = "video/mp4"),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 404, description = "Video file not found in the authenticated tenant", body = ErrorResponseDoc),
        (status = 410, description = "Video artifact retention has expired", body = ErrorResponseDoc),
        (status = 500, description = "Retained artifact failed integrity validation", body = ErrorResponseDoc),
        (status = 503, description = "Artifact storage is unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn get_ark_video_file_content() {}

#[utoipa::path(
    get,
    path = "/v1/models",
    tag = "Models",
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Supported model list", body = ModelsResponse),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn list_models() {}

#[utoipa::path(
    post,
    path = "/v1/organization/projects",
    tag = "Admin",
    security(("BearerAuth" = [])),
    request_body(content = CreateProjectRequestDoc, content_type = "application/json"),
    responses(
        (status = 200, description = "Created project", body = Project),
        (status = 400, description = "Invalid project request", body = ErrorResponseDoc),
        (status = 401, description = "Invalid admin authentication", body = ErrorResponseDoc),
        (status = 403, description = "Platform owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Project store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_project() {}

#[utoipa::path(
    get,
    path = "/v1/organization/projects",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("after" = Option<String>, Query, description = "Cursor project id"),
        ("limit" = Option<usize>, Query, minimum = 1, maximum = 100, description = "Page size")
    ),
    responses(
        (status = 200, description = "Project list", body = ProjectList),
        (status = 401, description = "Invalid admin authentication", body = ErrorResponseDoc),
        (status = 403, description = "Platform owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Project store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn list_projects() {}

#[utoipa::path(
    get,
    path = "/v1/organization/projects/{project_id}",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(("project_id" = String, Path, description = "Project id")),
    responses(
        (status = 200, description = "Project general settings", body = Project),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 404, description = "Project not found", body = ErrorResponseDoc),
        (status = 503, description = "Project store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn get_project_doc() {}

#[utoipa::path(
    patch,
    path = "/v1/organization/projects/{project_id}",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(("project_id" = String, Path, description = "Project id")),
    request_body(content = UpdateProjectRequestDoc, content_type = "application/json"),
    responses(
        (status = 200, description = "Updated project general settings", body = Project),
        (status = 400, description = "Invalid project settings", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project owner permission required", body = ErrorResponseDoc),
        (status = 404, description = "Project not found", body = ErrorResponseDoc),
        (status = 409, description = "Project settings version conflict", body = ErrorResponseDoc),
        (status = 503, description = "Project store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn update_project_doc() {}

#[utoipa::path(
    get,
    path = "/v1/organization/projects/{project_id}/members",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(("project_id" = String, Path, description = "Project id")),
    responses(
        (status = 200, description = "Active and disabled project memberships visible to this project", body = ProjectMemberList),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project read permission required", body = ErrorResponseDoc),
        (status = 404, description = "Project not found", body = ErrorResponseDoc),
        (status = 503, description = "Project membership store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn list_project_members_doc() {}

#[utoipa::path(
    get,
    path = "/v1/organization/projects/{project_id}/model-policy",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(("project_id" = String, Path, description = "Project id")),
    responses(
        (status = 200, description = "Effective project model allow-list and native media rate limits", body = ProjectModelPolicyView),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project read permission required", body = ErrorResponseDoc),
        (status = 404, description = "Project not found", body = ErrorResponseDoc),
        (status = 503, description = "Model routing or policy store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn get_project_model_policy_doc() {}

#[utoipa::path(
    put,
    path = "/v1/organization/projects/{project_id}/model-policy",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(("project_id" = String, Path, description = "Project id")),
    request_body(content = UpdateProjectModelPolicyRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Updated versioned project model policy", body = ProjectModelPolicyView),
        (status = 400, description = "Invalid model, media unit, or shared-bucket limit", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project owner permission required", body = ErrorResponseDoc),
        (status = 404, description = "Project or requested model not found", body = ErrorResponseDoc),
        (status = 409, description = "Model policy control version conflict", body = ErrorResponseDoc),
        (status = 503, description = "Model routing or policy store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn update_project_model_policy_doc() {}

#[utoipa::path(
    get,
    path = "/v1/organization/projects/{project_id}/webhooks",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("after" = Option<String>, Query, description = "Cursor endpoint id"),
        ("limit" = Option<usize>, Query, minimum = 1, maximum = 100, description = "Page size")
    ),
    responses(
        (status = 200, description = "Project webhook endpoints without secret material", body = ProjectWebhookList),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project read permission required", body = ErrorResponseDoc),
        (status = 404, description = "Project not found", body = ErrorResponseDoc),
        (status = 503, description = "Webhook store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn list_project_webhooks_doc() {}

#[utoipa::path(
    post,
    path = "/v1/organization/projects/{project_id}/webhooks",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(("project_id" = String, Path, description = "Project id")),
    request_body(content = CreateProjectWebhookRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Created endpoint and one-time signing secret", body = CreatedProjectWebhook),
        (status = 400, description = "Invalid URL or event selection", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project owner permission required", body = ErrorResponseDoc),
        (status = 503, description = "Webhook store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_project_webhook_doc() {}

#[utoipa::path(
    patch,
    path = "/v1/organization/projects/{project_id}/webhooks/{endpoint_id}",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("endpoint_id" = String, Path, description = "Webhook endpoint id")
    ),
    request_body(content = UpdateProjectWebhookRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Updated versioned endpoint configuration", body = ProjectWebhookEndpoint),
        (status = 400, description = "Invalid URL or event selection", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project owner permission required", body = ErrorResponseDoc),
        (status = 404, description = "Webhook endpoint not found", body = ErrorResponseDoc),
        (status = 409, description = "Webhook control version conflict", body = ErrorResponseDoc),
        (status = 503, description = "Webhook store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn update_project_webhook_doc() {}

#[utoipa::path(
    delete,
    path = "/v1/organization/projects/{project_id}/webhooks/{endpoint_id}",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("endpoint_id" = String, Path, description = "Webhook endpoint id")
    ),
    responses(
        (status = 200, description = "Soft-deleted endpoint and canceled queued deliveries", body = DeletedProjectWebhook),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project owner permission required", body = ErrorResponseDoc),
        (status = 404, description = "Webhook endpoint not found", body = ErrorResponseDoc),
        (status = 503, description = "Webhook store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn delete_project_webhook_doc() {}

#[utoipa::path(
    post,
    path = "/v1/organization/projects/{project_id}/webhooks/{endpoint_id}/rotate",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("endpoint_id" = String, Path, description = "Webhook endpoint id")
    ),
    responses(
        (status = 200, description = "One-time replacement signing secret", body = RotatedProjectWebhookSecret),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project owner permission required", body = ErrorResponseDoc),
        (status = 404, description = "Webhook endpoint not found", body = ErrorResponseDoc),
        (status = 503, description = "Webhook store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn rotate_project_webhook_secret_doc() {}

#[utoipa::path(
    post,
    path = "/v1/organization/projects/{project_id}/webhooks/{endpoint_id}/test",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("endpoint_id" = String, Path, description = "Webhook endpoint id")
    ),
    responses(
        (status = 200, description = "Queued a signed webhook.test event", body = ProjectWebhookTestEvent),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project owner permission required", body = ErrorResponseDoc),
        (status = 404, description = "Active webhook endpoint not found", body = ErrorResponseDoc),
        (status = 503, description = "Webhook store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn test_project_webhook_doc() {}

#[utoipa::path(
    get,
    path = "/v1/organization/projects/{project_id}/webhooks/{endpoint_id}/deliveries",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("endpoint_id" = String, Path, description = "Webhook endpoint id"),
        ("after" = Option<String>, Query, description = "Cursor delivery id"),
        ("limit" = Option<usize>, Query, minimum = 1, maximum = 100, description = "Page size")
    ),
    responses(
        (status = 200, description = "Endpoint delivery history without payload bodies or secrets", body = ProjectWebhookDeliveryList),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project read permission required", body = ErrorResponseDoc),
        (status = 404, description = "Project not found", body = ErrorResponseDoc),
        (status = 503, description = "Webhook store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn list_project_webhook_deliveries_doc() {}

#[utoipa::path(
    post,
    path = "/v1/organization/projects/{project_id}/members",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(("project_id" = String, Path, description = "Project id")),
    request_body(content = AddProjectMemberRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Added or reactivated project member", body = ProjectMemberView),
        (status = 400, description = "Invalid email or role", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project or organization owner permission required", body = ErrorResponseDoc),
        (status = 404, description = "Project or target user not found", body = ErrorResponseDoc),
        (status = 503, description = "Project membership store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn add_project_member_doc() {}

#[utoipa::path(
    patch,
    path = "/v1/organization/projects/{project_id}/members/{user_id}",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("user_id" = String, Path, description = "Target identity user id")
    ),
    request_body(content = UpdateProjectMemberRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Updated project member role", body = ProjectMemberView),
        (status = 400, description = "Invalid role or user id", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project or organization owner permission required", body = ErrorResponseDoc),
        (status = 404, description = "Project or active membership not found", body = ErrorResponseDoc),
        (status = 409, description = "The project must retain at least one active owner", body = ErrorResponseDoc),
        (status = 503, description = "Project membership store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn update_project_member_doc() {}

#[utoipa::path(
    delete,
    path = "/v1/organization/projects/{project_id}/members/{user_id}",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("user_id" = String, Path, description = "Target identity user id")
    ),
    responses(
        (status = 200, description = "Disabled project membership", body = ProjectMemberView),
        (status = 400, description = "Invalid user id", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project or organization owner permission required", body = ErrorResponseDoc),
        (status = 404, description = "Project or active membership not found", body = ErrorResponseDoc),
        (status = 409, description = "The project must retain at least one active owner", body = ErrorResponseDoc),
        (status = 503, description = "Project membership store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn remove_project_member_doc() {}

#[utoipa::path(
    get,
    path = "/v1/organization/projects/{project_id}/limits",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(("project_id" = String, Path, description = "Project id")),
    responses(
        (status = 200, description = "UTC calendar-month budget, authoritative spend, and active reservations", body = ProjectSpendBudgetView),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 404, description = "Project is missing or not visible to the authenticated user", body = ErrorResponseDoc),
        (status = 503, description = "Project spend budget store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn get_project_limits_doc() {}

#[utoipa::path(
    put,
    path = "/v1/organization/projects/{project_id}/limits",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(("project_id" = String, Path, description = "Project id")),
    request_body(content = UpdateProjectSpendBudgetRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Updated versioned project soft or hard budget", body = ProjectSpendBudgetView),
        (status = 400, description = "Invalid budget, currency, or alert threshold", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project or organization owner permission is required", body = ErrorResponseDoc),
        (status = 404, description = "Project is missing or not visible to the authenticated user", body = ErrorResponseDoc),
        (status = 409, description = "Expected control version is stale", body = ErrorResponseDoc),
        (status = 503, description = "Project spend budget store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn update_project_limits_doc() {}

#[utoipa::path(
    get,
    path = "/v1/console/notifications",
    tag = "Console",
    security(("BearerAuth" = [])),
    params(("limit" = Option<usize>, Query, description = "Maximum notifications, from 1 through 100")),
    responses(
        (status = 200, description = "Authenticated user's project spend notification inbox", body = ProjectSpendNotificationList),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 503, description = "Notification store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn list_project_spend_notifications_doc() {}

#[utoipa::path(
    post,
    path = "/v1/console/notifications/{delivery_id}/read",
    tag = "Console",
    security(("BearerAuth" = [])),
    params(("delivery_id" = String, Path, description = "Recipient-scoped notification delivery id")),
    responses(
        (status = 200, description = "Notification marked read for the authenticated recipient", body = ProjectSpendNotificationView),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 404, description = "Notification is missing or belongs to another user", body = ErrorResponseDoc),
        (status = 503, description = "Notification store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn mark_project_spend_notification_read_doc() {}

#[utoipa::path(
    post,
    path = "/v1/organization/projects/{project_id}/service_accounts",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(("project_id" = String, Path, description = "Project id")),
    request_body(content = CreateServiceAccountRequestDoc, content_type = "application/json"),
    responses(
        (status = 200, description = "Created service account and one-time API key value", body = ProjectServiceAccount),
        (status = 400, description = "Invalid admin request", body = ErrorResponseDoc),
        (status = 401, description = "Invalid admin authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project owner permission required", body = ErrorResponseDoc),
        (status = 503, description = "API key store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_project_service_account() {}

#[utoipa::path(
    delete,
    path = "/v1/organization/projects/{project_id}/service_accounts/{service_account_id}",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("service_account_id" = String, Path, description = "Service account id")
    ),
    responses(
        (status = 200, description = "Service account deletion confirmation", body = ProjectServiceAccountDeleted),
        (status = 401, description = "Invalid admin authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project owner permission required", body = ErrorResponseDoc),
        (status = 404, description = "Service account not found", body = ErrorResponseDoc),
        (status = 503, description = "API key store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn delete_project_service_account() {}

#[utoipa::path(
    post,
    path = "/v1/organization/projects/{project_id}/api_keys",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(("project_id" = String, Path, description = "Project id")),
    request_body(content = CreateUserApiKeyRequestDoc, content_type = "application/json"),
    responses(
        (status = 200, description = "One-time personal project API key secret", body = CreatedProjectApiKey),
        (status = 400, description = "Invalid permission configuration", body = ErrorResponseDoc),
        (status = 401, description = "Invalid user authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project write permission required", body = ErrorResponseDoc),
        (status = 404, description = "Project not found", body = ErrorResponseDoc),
        (status = 503, description = "API key store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_user_api_key_doc() {}

#[utoipa::path(
    get,
    path = "/v1/organization/projects/{project_id}/api_keys",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("after" = Option<String>, Query, description = "Cursor API key id"),
        ("limit" = Option<usize>, Query, minimum = 1, maximum = 100, description = "Page size")
    ),
    responses(
        (status = 200, description = "Project API keys", body = ProjectApiKeyList),
        (status = 401, description = "Invalid admin authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project read permission required", body = ErrorResponseDoc),
        (status = 503, description = "API key store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn list_project_api_keys() {}

#[utoipa::path(
    delete,
    path = "/v1/organization/projects/{project_id}/api_keys/{api_key_id}",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("api_key_id" = String, Path, description = "API key id")
    ),
    responses(
        (status = 200, description = "API key revocation confirmation", body = ProjectApiKeyDeleted),
        (status = 401, description = "Invalid admin authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project write permission required", body = ErrorResponseDoc),
        (status = 404, description = "API key not found", body = ErrorResponseDoc),
        (status = 503, description = "API key store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn delete_project_api_key() {}

#[utoipa::path(
    patch,
    path = "/v1/organization/projects/{project_id}/api_keys/{api_key_id}",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("api_key_id" = String, Path, description = "API key id")
    ),
    request_body(content = UpdateApiKeyRequestDoc, content_type = "application/json"),
    responses(
        (status = 200, description = "Updated API key metadata and permissions", body = UpdatedProjectApiKey),
        (status = 400, description = "Invalid permission configuration", body = ErrorResponseDoc),
        (status = 401, description = "Invalid user authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project write permission required", body = ErrorResponseDoc),
        (status = 404, description = "API key not found or not editable by this principal", body = ErrorResponseDoc),
        (status = 503, description = "API key store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn update_project_api_key_doc() {}

#[utoipa::path(
    post,
    path = "/v1/organization/projects/{project_id}/api_keys/{api_key_id}/rotate",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("api_key_id" = String, Path, description = "API key id")
    ),
    responses(
        (status = 200, description = "One-time replacement secret; the old key is revoked atomically", body = RotatedProjectApiKey),
        (status = 401, description = "Invalid user authentication", body = ErrorResponseDoc),
        (status = 403, description = "Project write permission required", body = ErrorResponseDoc),
        (status = 404, description = "API key not found or not rotatable by this principal", body = ErrorResponseDoc),
        (status = 503, description = "API key store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn rotate_project_api_key_doc() {}

#[utoipa::path(
    post,
    path = "/admin/v1/auth/login",
    tag = "Admin Identity",
    request_body(content = AdminLoginRequestDoc, content_type = "application/json"),
    responses(
        (status = 200, description = "Short-lived access JWT and rotating opaque refresh token", body = AdminTokenResponseDoc),
        (status = 401, description = "Invalid credentials", body = ErrorResponseDoc),
        (status = 503, description = "Identity service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_login() {}

#[utoipa::path(
    post,
    path = "/admin/v1/auth/refresh",
    tag = "Admin Identity",
    request_body(content = AdminRefreshRequestDoc, content_type = "application/json"),
    responses(
        (status = 200, description = "Rotated token pair", body = AdminTokenResponseDoc),
        (status = 401, description = "Invalid, expired, revoked, or reused refresh token", body = ErrorResponseDoc),
        (status = 503, description = "Identity service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_refresh() {}

#[utoipa::path(
    post,
    path = "/admin/v1/auth/logout",
    tag = "Admin Identity",
    security(("BearerAuth" = [])),
    request_body(content = AdminLogoutRequestDoc, content_type = "application/json"),
    responses(
        (status = 204, description = "Session family revoked"),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_logout() {}

#[utoipa::path(
    get,
    path = "/admin/v1/auth/me",
    tag = "Admin Identity",
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Current database-authorized principal", body = AdminPrincipalDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_me() {}

#[utoipa::path(
    get,
    path = "/admin/v1/overview",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(("window" = Option<String>, Query, description = "Database-anchored window: 1h, 6h, 24h, or 7d")),
    responses(
        (status = 200, description = "Global platform operations snapshot", body = OverviewSnapshot),
        (status = 400, description = "Invalid or excessive window", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Isolated admin read store unavailable or timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_overview() {}

#[utoipa::path(
    get,
    path = "/admin/v1/billing/summary",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("window" = Option<String>, Query, description = "Database-anchored window: 1h, 6h, 24h, 7d, or 30d"),
        ("project_id" = Option<String>, Query, description = "Optional project attribution filter")
    ),
    responses(
        (status = 200, description = "Separate current account, usage, rated, sealed-ledger, and provider-cost facts", body = BillingSnapshot),
        (status = 400, description = "Invalid or excessive window", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Isolated admin read store unavailable or timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_billing_summary() {}

#[utoipa::path(
    get,
    path = "/admin/v1/billing/accounts",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("currency" = Option<String>, Query, description = "Three-letter ISO 4217 currency, defaults to USD"),
        ("query" = Option<String>, Query, description = "Literal organization ID or display-name search"),
        ("after" = Option<String>, Query, description = "Exclusive organization-ID cursor"),
        ("limit" = Option<usize>, Query, description = "Page size from 1 to 100")
    ),
    responses(
        (status = 200, description = "Keyset-paginated organizations with billing-account control state", body = BillingAccountControlList),
        (status = 400, description = "Invalid currency, query, cursor, or limit", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Billing-account control unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_list_billing_accounts() {}

#[utoipa::path(
    get,
    path = "/admin/v1/billing/accounts/{tenant_id}/{currency}",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("tenant_id" = String, Path, description = "Organization or workspace identifier"),
        ("currency" = String, Path, description = "Three-letter ISO 4217 currency")
    ),
    responses(
        (status = 200, description = "Versioned organization billing-account control state", body = BillingAccountControlView),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 404, description = "Organization not found", body = ErrorResponseDoc),
        (status = 503, description = "Billing-account control unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_get_billing_account() {}

#[utoipa::path(
    put,
    path = "/admin/v1/billing/accounts/{tenant_id}/{currency}",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("tenant_id" = String, Path, description = "Organization or workspace identifier"),
        ("currency" = String, Path, description = "Three-letter ISO 4217 currency")
    ),
    request_body(content = UpdateBillingAccountLimitRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Updated and audited organization credit limit", body = BillingAccountControlView),
        (status = 400, description = "Invalid amount, currency, version, or reason", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 404, description = "Organization not found", body = ErrorResponseDoc),
        (status = 409, description = "Stale control version or limit below committed spend", body = ErrorResponseDoc),
        (status = 503, description = "Billing-account control unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_update_billing_account_limit() {}

#[utoipa::path(
    get,
    path = "/admin/v1/billing/credit-grants",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("organization_id" = Option<String>, Query, description = "Exact organization identifier"),
        ("currency" = Option<String>, Query, description = "Three-letter ISO 4217 currency, defaults to USD"),
        ("state" = Option<String>, Query, description = "all, active, consuming, exhausted, expired, or revoked"),
        ("after" = Option<String>, Query, description = "Exclusive received-time and grant UUID cursor"),
        ("limit" = Option<usize>, Query, description = "Page size from 1 to 100")
    ),
    responses(
        (status = 200, description = "Keyset-paginated organization credit grants and effective balance", body = CreditGrantList),
        (status = 400, description = "Invalid organization, currency, state, cursor, or limit", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "billing:read platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Credit grant service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_list_credit_grants() {}

#[utoipa::path(
    get,
    path = "/admin/v1/billing/credit-grants/{grant_id}",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("grant_id" = String, Path, description = "Credit grant UUID")
    ),
    responses(
        (status = 200, description = "Credit grant with derived effective state and immutable counters", body = CreditGrantView),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "billing:read platform-owner scope is required", body = ErrorResponseDoc),
        (status = 404, description = "Credit grant not found", body = ErrorResponseDoc),
        (status = 503, description = "Credit grant service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_get_credit_grant() {}

#[utoipa::path(
    post,
    path = "/admin/v1/billing/credit-grants",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("Idempotency-Key" = String, Header, description = "Required operation idempotency key")
    ),
    request_body(content = CreateCreditGrantRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Issued or exactly replayed promotional credit grant", body = CreditGrantView),
        (status = 400, description = "Invalid amount, currency, expiration, source, reason, or idempotency key", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 404, description = "Organization not found", body = ErrorResponseDoc),
        (status = 409, description = "Idempotency or wallet conflict", body = ErrorResponseDoc),
        (status = 503, description = "Credit grant service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_create_credit_grant() {}

#[utoipa::path(
    post,
    path = "/admin/v1/billing/credit-grants/{grant_id}/revoke",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("grant_id" = String, Path, description = "Credit grant UUID"),
        ("Idempotency-Key" = String, Header, description = "Required operation idempotency key")
    ),
    request_body(content = RevokeCreditGrantRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Revoked or exactly replayed credit grant", body = CreditGrantView),
        (status = 400, description = "Invalid reason or idempotency key", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 404, description = "Credit grant not found", body = ErrorResponseDoc),
        (status = 409, description = "Grant is expired, exhausted, reserved, or changed", body = ErrorResponseDoc),
        (status = 503, description = "Credit grant service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_revoke_credit_grant() {}

#[utoipa::path(
    get,
    path = "/v1/organizations/{organization_id}/billing/credit-grants",
    tag = "Console",
    security(("BearerAuth" = [])),
    params(
        ("organization_id" = String, Path, description = "Organization identifier"),
        ("currency" = Option<String>, Query, description = "Three-letter ISO 4217 currency, defaults to USD"),
        ("state" = Option<String>, Query, description = "all, active, consuming, exhausted, expired, or revoked"),
        ("after" = Option<String>, Query, description = "Exclusive received-time and grant UUID cursor"),
        ("limit" = Option<usize>, Query, description = "Page size from 1 to 100")
    ),
    responses(
        (status = 200, description = "Organization-scoped credit grants and effective balance", body = OrganizationCreditGrantList),
        (status = 400, description = "Invalid currency, state, cursor, or limit", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 404, description = "Organization not found or not visible to the principal", body = ErrorResponseDoc),
        (status = 503, description = "Credit grant service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn list_organization_credit_grants() {}

#[utoipa::path(
    get,
    path = "/admin/v1/billing/customer-charges",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("tenant_id" = Option<String>, Query, description = "Exact organization identifier"),
        ("state" = Option<String>, Query, description = "all, refundable, partially_refunded, or fully_refunded"),
        ("after" = Option<String>, Query, description = "Exclusive customer-charge transaction UUID cursor"),
        ("limit" = Option<usize>, Query, description = "Page size from 1 to 100")
    ),
    responses(
        (status = 200, description = "Keyset-paginated sealed customer charges and refund state", body = CustomerChargeList),
        (status = 400, description = "Invalid filter, cursor, or limit", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "billing:read platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Customer refund service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_list_customer_charges() {}

#[utoipa::path(
    get,
    path = "/admin/v1/billing/customer-charges/{transaction_id}",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("transaction_id" = String, Path, description = "Sealed customer-charge ledger transaction UUID")
    ),
    responses(
        (status = 200, description = "Customer charge and immutable partial-refund history", body = CustomerChargeDetail),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "billing:read platform-owner scope is required", body = ErrorResponseDoc),
        (status = 404, description = "Customer charge not found", body = ErrorResponseDoc),
        (status = 503, description = "Customer refund service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_get_customer_charge() {}

#[utoipa::path(
    post,
    path = "/admin/v1/billing/customer-charges/{transaction_id}/refunds",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("transaction_id" = String, Path, description = "Sealed customer-charge ledger transaction UUID"),
        ("Idempotency-Key" = String, Header, description = "Required key scoped to the original customer charge")
    ),
    request_body(content = CreateCustomerRefundRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Created or exactly replayed immutable customer refund", body = CustomerRefundView),
        (status = 400, description = "Invalid amount, reason, request body, or idempotency key", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "billing:refund platform-owner scope is required", body = ErrorResponseDoc),
        (status = 404, description = "Customer charge not found", body = ErrorResponseDoc),
        (status = 409, description = "Idempotency conflict or refund exceeds the remaining charge", body = ErrorResponseDoc),
        (status = 500, description = "Ledger integrity validation failed", body = ErrorResponseDoc),
        (status = 503, description = "Customer refund service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_create_customer_refund() {}

#[utoipa::path(
    get,
    path = "/admin/v1/billing/integrity-runs",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("after" = Option<String>, Query, description = "Previous run UUID for keyset pagination"),
        ("limit" = Option<usize>, Query, description = "Page size from 1 to 100")
    ),
    responses(
        (status = 200, description = "Immutable billing-integrity run history", body = BillingIntegrityRunList),
        (status = 400, description = "Invalid cursor or limit", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Billing integrity service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_list_billing_integrity_runs() {}

#[utoipa::path(
    post,
    path = "/admin/v1/billing/integrity-runs",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Completed evidence-only billing-integrity snapshot", body = BillingIntegrityRunDetail),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 409, description = "Another platform integrity scan is running", body = ErrorResponseDoc),
        (status = 503, description = "Billing integrity service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_create_billing_integrity_run() {}

#[utoipa::path(
    get,
    path = "/admin/v1/billing/integrity-runs/{run_id}",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("run_id" = String, Path, description = "Billing-integrity run UUID")
    ),
    responses(
        (status = 200, description = "Run summary with immutable findings", body = BillingIntegrityRunDetail),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 404, description = "Integrity run not found", body = ErrorResponseDoc),
        (status = 503, description = "Billing integrity service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_get_billing_integrity_run() {}

#[utoipa::path(
    get,
    path = "/admin/v1/billing/provider-cost-obligations",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("after" = Option<String>, Query, description = "Previous receipt UUID for keyset pagination"),
        ("limit" = Option<usize>, Query, description = "Page size from 1 to 100"),
        ("state" = Option<String>, Query, description = "all, open, expected, pending, settled, or waived"),
        ("urgency" = Option<String>, Query, description = "all, overdue, or escalated"),
        ("provider_id" = Option<String>, Query, description = "Exact provider identifier")
    ),
    responses(
        (status = 200, description = "Provider-cost lifecycle queue and platform summary", body = ProviderCostObligationList),
        (status = 400, description = "Invalid cursor or filter", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Provider-cost obligation service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_list_provider_cost_obligations() {}

#[utoipa::path(
    get,
    path = "/admin/v1/billing/provider-cost-obligations/{receipt_id}",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("receipt_id" = String, Path, description = "Provider receipt UUID")
    ),
    responses(
        (status = 200, description = "Provider-cost obligation and immutable event history", body = ProviderCostObligationDetail),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 404, description = "Provider-cost obligation not found", body = ErrorResponseDoc),
        (status = 503, description = "Provider-cost obligation service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_get_provider_cost_obligation() {}

#[utoipa::path(
    get,
    path = "/admin/v1/billing/provider-cost-allocation-pools",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("provider_id" = Option<String>, Query, description = "Exact provider identifier"),
        ("provider_account_id" = Option<String>, Query, description = "Exact provider account UUID"),
        ("currency" = Option<String>, Query, description = "Three-letter settlement currency"),
        ("state" = Option<String>, Query, description = "Allocation pool state; the current HTTP surface creates draft pools only"),
        ("after" = Option<String>, Query, description = "Previous allocation pool UUID for keyset pagination"),
        ("limit" = Option<usize>, Query, description = "Page size from 1 to 100")
    ),
    responses(
        (status = 200, description = "Provider cost allocation draft pools", body = ProviderCostAllocationListDoc),
        (status = 400, description = "Invalid cursor or filter", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Provider cost allocation service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_list_provider_cost_allocations() {}

#[utoipa::path(
    get,
    path = "/admin/v1/billing/provider-cost-allocation-pools/{pool_id}",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("pool_id" = String, Path, description = "Provider cost allocation pool UUID")
    ),
    responses(
        (status = 200, description = "Provider cost allocation draft and deterministic line allocation", body = ProviderCostAllocationDetailDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 404, description = "Provider cost allocation pool not found", body = ErrorResponseDoc),
        (status = 503, description = "Provider cost allocation service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_get_provider_cost_allocation() {}

#[utoipa::path(
    post,
    path = "/admin/v1/billing/provider-cost-allocation-pools/preview",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    request_body(
        content = PreviewProviderCostAllocationRequestDoc,
        content_type = "application/json",
        description = "Compute a deterministic, read-only allocation preview. This does not create authority, ledger entries, or obligation settlement."
    ),
    responses(
        (status = 200, description = "Deterministic provider cost allocation preview", body = ProviderCostAllocationPreviewDoc),
        (status = 400, description = "Invalid allocation dimensions or amount", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 409, description = "Allocation candidates or conservation invariants conflict", body = ErrorResponseDoc),
        (status = 503, description = "Provider cost allocation service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_preview_provider_cost_allocation() {}

#[utoipa::path(
    post,
    path = "/admin/v1/billing/provider-cost-allocation-pools",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    request_body(
        content = CreateProviderCostAllocationDraftRequestDoc,
        content_type = "application/json",
        description = "Persist a draft only when the supplied preview hash still matches the exact candidate set. Draft creation does not create provider-cost authority or ledger postings."
    ),
    responses(
        (status = 200, description = "Created or idempotently replayed provider cost allocation draft", body = ProviderCostAllocationDetailDoc),
        (status = 400, description = "Invalid allocation dimensions, preview hash, or idempotency key", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 409, description = "Preview drift or idempotency body conflict", body = ErrorResponseDoc),
        (status = 503, description = "Provider cost allocation service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_create_provider_cost_allocation_draft() {}

#[utoipa::path(
    post,
    path = "/admin/v1/billing/provider-cost-allocation-pools/{pool_id}",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("pool_id" = String, Path, description = "Provider cost allocation pool UUID"),
        ("Idempotency-Key" = String, Header, description = "Required 1-255 character visible ASCII key. Same-key, same-body retries replay the immutable close result.")
    ),
    request_body(
        content = CloseProviderCostAllocationRequestDoc,
        content_type = "application/json",
        description = "Close an unchanged successful-output draft, bind immutable provider invoice, contract, subscription, or statement evidence, claim every exact receipt, settle its provider-cost obligations, and seal positive allocation ledger entries."
    ),
    responses(
        (status = 200, description = "Closed or idempotently replayed provider cost allocation", body = ProviderCostAllocationDetailDoc),
        (status = 400, description = "Invalid close evidence, hash, version, or idempotency key", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 404, description = "Provider cost allocation pool not found", body = ErrorResponseDoc),
        (status = 409, description = "Draft changed, is not output-based, has residual, already has another authority, or was closed by another command", body = ErrorResponseDoc),
        (status = 503, description = "Provider cost allocation service unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_close_provider_cost_allocation() {}

#[utoipa::path(
    get,
    path = "/admin/v1/usage",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("window" = Option<String>, Query, description = "Database-anchored window: 1h, 6h, 24h, 7d, or 30d"),
        ("interval" = Option<String>, Query, description = "Time bucket: 1m, 1h, or 1d. 1m is limited to a 24h window"),
        ("group_by" = Option<String>, Query, description = "none, line_item, project, api_key, user, provider, model, or operation"),
        ("project_id" = Option<String>, Query),
        ("api_key_id" = Option<String>, Query),
        ("user_id" = Option<String>, Query, description = "Identity user UUID"),
        ("provider_id" = Option<String>, Query),
        ("model" = Option<String>, Query),
        ("operation" = Option<String>, Query)
    ),
    responses(
        (status = 200, description = "Platform-wide activity, customer spend, and filter dimensions", body = UsageAnalysisSnapshot),
        (status = 400, description = "Invalid filter, interval, group, or window", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Isolated admin read store unavailable or timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_usage() {}

#[utoipa::path(
    get,
    path = "/v1/console/usage",
    tag = "Console",
    security(("BearerAuth" = [])),
    params(
        ("window" = Option<String>, Query, description = "Database-anchored window: 1h, 6h, 24h, 7d, or 30d"),
        ("interval" = Option<String>, Query, description = "Time bucket: 1m, 1h, or 1d. 1m is limited to a 24h window"),
        ("group_by" = Option<String>, Query, description = "none, line_item, project, api_key, user, provider, model, or operation"),
        ("project_id" = Option<String>, Query, description = "Optional project scope within the current workspace"),
        ("api_key_id" = Option<String>, Query),
        ("provider_id" = Option<String>, Query),
        ("model" = Option<String>, Query),
        ("operation" = Option<String>, Query)
    ),
    responses(
        (status = 200, description = "Current actor or selected-project activity and customer spend; provider costs are never exposed", body = UsageAnalysisSnapshot),
        (status = 400, description = "Invalid filter, interval, group, or window", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "Requested user scope is not authorized", body = ErrorResponseDoc),
        (status = 503, description = "Isolated console read store unavailable or timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn console_usage() {}

#[utoipa::path(
    get,
    path = "/admin/v1/pricing/price-books",
    tag = "Admin Pricing",
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Versioned customer, provider actual, allocated, and benchmark price books", body = PriceBookCatalog),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Pricing store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_price_books() {}

#[utoipa::path(
    get,
    path = "/admin/v1/pricing/coverage",
    tag = "Admin Pricing",
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Platform route, customer price, metering, and provider-cost coverage", body = PricingCoverageSnapshot),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Pricing coverage unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_pricing_coverage() {}

#[utoipa::path(
    get,
    path = "/admin/v1/pricing/price-book-versions/{price_book_version_id}/publish-readiness",
    tag = "Admin Pricing",
    security(("BearerAuth" = [])),
    params(("price_book_version_id" = String, Path, description = "Draft price book version UUID")),
    responses(
        (status = 200, description = "Authoritative publish preflight using the same pricing and route contracts as publication", body = PricePublishReadiness),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* scope is required", body = ErrorResponseDoc),
        (status = 404, description = "Price book version not found", body = ErrorResponseDoc),
        (status = 503, description = "Pricing readiness unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_price_book_version_publish_readiness() {}

#[utoipa::path(
    post,
    path = "/admin/v1/pricing/price-books",
    tag = "Admin Pricing",
    security(("BearerAuth" = [])),
    request_body(content = CreatePriceBookRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Price book created", body = PriceBookView),
        (status = 400, description = "Invalid scope or currency", body = ErrorResponseDoc),
        (status = 409, description = "Price book key already exists", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_create_price_book() {}

#[utoipa::path(
    post,
    path = "/admin/v1/pricing/price-books/{price_book_id}/versions",
    tag = "Admin Pricing",
    security(("BearerAuth" = [])),
    params(("price_book_id" = String, Path, description = "Price book UUID")),
    request_body(content = CreatePriceBookVersionRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Draft price version created", body = PriceBookVersionView),
        (status = 400, description = "Invalid version or component", body = ErrorResponseDoc),
        (status = 404, description = "Price book not found", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_create_price_book_version() {}

#[utoipa::path(
    put,
    path = "/admin/v1/pricing/price-book-versions/{price_book_version_id}",
    tag = "Admin Pricing",
    security(("BearerAuth" = [])),
    params(("price_book_version_id" = String, Path, description = "Price book version UUID")),
    request_body(content = UpdatePriceBookVersionRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Draft price version replaced", body = PriceBookVersionView),
        (status = 400, description = "Invalid version or component", body = ErrorResponseDoc),
        (status = 409, description = "Stale control version or published version", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_update_price_book_version() {}

#[utoipa::path(
    post,
    path = "/admin/v1/pricing/price-book-versions/{price_book_version_id}/publish",
    tag = "Admin Pricing",
    security(("BearerAuth" = [])),
    params(("price_book_version_id" = String, Path, description = "Price book version UUID")),
    request_body(content = TransitionPriceBookVersionRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Draft published as the active immutable version", body = PriceBookVersionView),
        (status = 409, description = "Stale control version, empty draft, or overlapping active version", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_publish_price_book_version() {}

#[utoipa::path(
    post,
    path = "/admin/v1/pricing/price-book-versions/{price_book_version_id}/retire",
    tag = "Admin Pricing",
    security(("BearerAuth" = [])),
    params(("price_book_version_id" = String, Path, description = "Price book version UUID")),
    request_body(content = TransitionPriceBookVersionRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Active version retired and effective interval closed", body = PriceBookVersionView),
        (status = 409, description = "Stale control version or non-active version", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_retire_price_book_version() {}

#[utoipa::path(
    post,
    path = "/admin/v1/pricing/price-book-versions/{price_book_version_id}/rollback-draft",
    tag = "Admin Pricing",
    security(("BearerAuth" = [])),
    params(("price_book_version_id" = String, Path, description = "Immutable source price version UUID")),
    request_body(content = CreatePriceRollbackDraftRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Historical version cloned into a new reviewable draft", body = PriceRollbackDraftResult),
        (status = 409, description = "Source version is not immutable or cannot be cloned", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_create_price_rollback_draft() {}

#[utoipa::path(
    post,
    path = "/admin/v1/pricing/preview",
    tag = "Admin Pricing",
    security(("BearerAuth" = [])),
    request_body(content = PricePreviewRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Resolved immutable rate version and deterministic native-unit calculation", body = PricePreviewResult),
        (status = 400, description = "Invalid usage fact or insufficient quantity authority", body = ErrorResponseDoc),
        (status = 404, description = "No published price matches the requested scope and model", body = ErrorResponseDoc),
        (status = 409, description = "Multiple equal-precedence prices match", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_preview_price() {}

#[utoipa::path(
    get,
    path = "/admin/v1/pricing/official-catalogs",
    tag = "Admin Pricing",
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Curated official price sources, verification availability, and latest versioned sync result", body = OfficialPriceCatalogs),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "platform_owner role is required", body = ErrorResponseDoc),
        (status = 503, description = "Pricing store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_official_price_catalogs() {}

#[utoipa::path(
    post,
    path = "/admin/v1/pricing/official-catalogs/{catalog_key}/snapshots",
    tag = "Admin Pricing",
    security(("BearerAuth" = [])),
    params(("catalog_key" = String, Path, description = "Curated official catalog key")),
    responses(
        (status = 200, description = "A distinct audited sync run, deduplicated immutable source snapshot, and component-level price differences", body = OfficialPriceSnapshotPreview),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "platform_owner role is required", body = ErrorResponseDoc),
        (status = 404, description = "Catalog not found or not verified", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_observe_official_price_catalog() {}

#[utoipa::path(
    post,
    path = "/admin/v1/pricing/source-snapshots/{snapshot_id}/apply",
    tag = "Admin Pricing",
    security(("BearerAuth" = [])),
    params(("snapshot_id" = String, Path, description = "Official price snapshot UUID")),
    request_body(content = ApplyOfficialPriceSnapshotRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Selected differences linked to existing versions or created as reviewable drafts", body = OfficialPriceSnapshotPreview),
        (status = 400, description = "Invalid or empty item selection", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "platform_owner role is required", body = ErrorResponseDoc),
        (status = 404, description = "Snapshot or selected item not found", body = ErrorResponseDoc),
        (status = 409, description = "Snapshot item conflicts with current catalog state", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_apply_official_price_snapshot() {}

#[utoipa::path(
    get,
    path = "/admin/v1/provider-accounts",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Redacted provider configuration, runtime readiness, and capacity snapshot", body = ProviderAccountsSnapshot),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Isolated admin read store unavailable or timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_provider_accounts() {}

#[utoipa::path(
    get,
    path = "/admin/v1/provider-account-runtime-events",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "SSE stream of execution-concurrency and queue-pressure snapshots with commit-ordered account deltas", body = ProviderAccountRuntimeEvent, content_type = "text/event-stream"),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Runtime event hub or isolated admin read store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_provider_account_runtime_events() {}

#[utoipa::path(
    get,
    path = "/admin/v1/scheduler/queues",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(("window" = Option<String>, Query, description = "Uncertain-state lookback: 1h, 6h, 24h, or 7d")),
    responses(
        (status = 200, description = "Durable scheduler stages and capacity snapshot", body = SchedulerSnapshot),
        (status = 400, description = "Invalid or excessive window", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Isolated admin read store unavailable or timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_scheduler_queues() {}

#[utoipa::path(
    get,
    path = "/v1/organization/audit_logs",
    tag = "Organization",
    security(("BearerAuth" = [])),
    params(
        ("window" = Option<String>, Query, description = "Database-anchored window: 1h, 6h, 24h, 7d, 30d, or 90d"),
        ("to_ms" = Option<i64>, Query, description = "Fixed upper bound returned by the first page"),
        ("limit" = Option<u32>, Query, minimum = 1, maximum = 100),
        ("after" = Option<String>, Query, description = "Last audit event ID from the previous page"),
        ("event_type" = Option<String>, Query),
        ("outcome" = Option<String>, Query, description = "success, denied, or failure"),
        ("actor_user_id" = Option<String>, Query),
        ("project_id" = Option<String>, Query),
        ("resource_type" = Option<String>, Query),
        ("request_id" = Option<String>, Query),
        ("q" = Option<String>, Query, description = "Search event, actor, request, or resource")
    ),
    responses(
        (status = 200, description = "Organization audit events ordered by effective time with keyset pagination", body = AuditLogsSnapshot),
        (status = 400, description = "Invalid filter, cursor, or window", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 404, description = "The after cursor does not exist", body = ErrorResponseDoc),
        (status = 503, description = "Audit read store unavailable or timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn organization_audit_logs() {}

#[utoipa::path(
    get,
    path = "/admin/v1/logs",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("window" = Option<String>, Query, description = "Database-anchored window: 1h, 6h, 24h, 7d, 30d, or 90d"),
        ("to_ms" = Option<i64>, Query, description = "Fixed upper bound returned by the first page"),
        ("limit" = Option<u32>, Query, minimum = 1, maximum = 100),
        ("cursor_created_at_ms" = Option<i64>, Query),
        ("cursor_request_id" = Option<String>, Query, description = "Request ID tiebreaker returned by the previous page"),
        ("visibility" = Option<String>, Query, description = "project for platform/project traffic or mine for actor traffic"),
        ("source" = Option<String>, Query, description = "models, images, videos, or files"),
        ("status" = Option<String>, Query, description = "succeeded, failed, or in_progress"),
        ("provider_id" = Option<String>, Query),
        ("model" = Option<String>, Query),
        ("project_id" = Option<String>, Query),
        ("api_key_id" = Option<String>, Query),
        ("q" = Option<String>, Query, description = "Exact request ID or job UUID")
    ),
    responses(
        (status = 200, description = "Platform request metadata with keyset pagination; request bodies and generated media are not captured", body = RequestLogsSnapshot),
        (status = 400, description = "Invalid filter, cursor, or window", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Isolated request log read store unavailable or timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_request_logs() {}

#[utoipa::path(
    get,
    path = "/v1/console/logs",
    tag = "Console",
    security(("BearerAuth" = [])),
    params(
        ("window" = Option<String>, Query, description = "Database-anchored window: 1h, 6h, 24h, 7d, 30d, or 90d"),
        ("to_ms" = Option<i64>, Query),
        ("limit" = Option<u32>, Query, minimum = 1, maximum = 100),
        ("cursor_created_at_ms" = Option<i64>, Query),
        ("cursor_request_id" = Option<String>, Query),
        ("visibility" = Option<String>, Query, description = "mine for the authenticated actor or project for all traffic in an authorized project"),
        ("source" = Option<String>, Query, description = "models, images, videos, or files"),
        ("status" = Option<String>, Query, description = "succeeded, failed, or in_progress"),
        ("provider_id" = Option<String>, Query),
        ("model" = Option<String>, Query),
        ("project_id" = Option<String>, Query, description = "Required for project visibility"),
        ("api_key_id" = Option<String>, Query),
        ("q" = Option<String>, Query, description = "Exact request ID or job UUID")
    ),
    responses(
        (status = 200, description = "Actor- or project-scoped request metadata without request body capture", body = RequestLogsSnapshot),
        (status = 400, description = "Invalid filter, cursor, or window", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "Workspace read permission is required", body = ErrorResponseDoc),
        (status = 404, description = "Project does not exist or is outside the authorized scope", body = ErrorResponseDoc),
        (status = 503, description = "Isolated request log read store unavailable or timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn console_request_logs() {}

#[utoipa::path(
    get,
    path = "/admin/v1/jobs",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(
        ("window" = Option<String>, Query, description = "Database-anchored window: 1h, 6h, 24h, 7d, or 30d"),
        ("to_ms" = Option<i64>, Query, description = "Fixed upper bound returned by the first page"),
        ("limit" = Option<u32>, Query, minimum = 1, maximum = 100),
        ("cursor_created_at_ms" = Option<i64>, Query),
        ("cursor_job_id" = Option<String>, Query, description = "UUID tiebreaker returned by the previous page"),
        ("provider_id" = Option<String>, Query),
        ("state" = Option<String>, Query, description = "Native job state"),
        ("operation" = Option<String>, Query, description = "Exact operation: generation, edit, or video_generation"),
        ("model" = Option<String>, Query, description = "Exact public model id"),
        ("project_id" = Option<String>, Query),
        ("api_key_id" = Option<String>, Query),
        ("q" = Option<String>, Query, description = "Exact request id or job UUID")
    ),
    responses(
        (status = 200, description = "Keyset-paginated jobs with separate job, work, and provider states", body = JobsSnapshot),
        (status = 400, description = "Invalid filter, cursor, or window", body = ErrorResponseDoc),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 503, description = "Isolated admin read store unavailable or timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_jobs() {}

#[utoipa::path(
    get,
    path = "/admin/v1/jobs/{job_id}/economics",
    tag = "Admin Operations",
    security(("BearerAuth" = [])),
    params(("job_id" = String, Path, description = "Job UUID")),
    responses(
        (status = 200, description = "Request quote, metering, customer settlement, ledger, and provider-cost evidence", body = JobEconomicsSnapshot),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "admin:* platform-owner scope is required", body = ErrorResponseDoc),
        (status = 404, description = "Job does not exist or is outside the authorized scope", body = ErrorResponseDoc),
        (status = 503, description = "Isolated admin read store unavailable or timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn admin_job_economics() {}

#[utoipa::path(
    get,
    path = "/v1/console/jobs/{job_id}/economics",
    tag = "Console",
    security(("BearerAuth" = [])),
    params(
        ("job_id" = String, Path, description = "Job UUID"),
        ("user_id" = Option<String>, Query, description = "Platform-owner impersonation scope"),
        ("project_id" = Option<String>, Query, description = "Authorized project scope, including service-account and API-key jobs")
    ),
    responses(
        (status = 200, description = "Authorized request quote, metering, customer settlement, and customer ledger; provider costs are structurally excluded", body = ConsoleJobEconomicsSnapshot),
        (status = 401, description = "Identity JWT is required", body = ErrorResponseDoc),
        (status = 403, description = "Workspace read permission is required", body = ErrorResponseDoc),
        (status = 404, description = "Job does not exist or is outside the authorized scope", body = ErrorResponseDoc),
        (status = 503, description = "Isolated admin read store unavailable or timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn console_job_economics() {}

#[utoipa::path(
    get,
    path = "/healthz",
    tag = "System",
    responses((status = 200, description = "Gateway is alive", body = HealthResponse))
)]
#[allow(dead_code)]
async fn healthz() {}

#[utoipa::path(
    get,
    path = "/readyz",
    tag = "System",
    responses(
        (status = 200, description = "Gateway dependencies are ready; provider profile states are diagnostic aggregates", body = ReadinessResponse),
        (status = 503, description = "Gateway database readiness probe failed or timed out", body = ReadinessResponse)
    )
)]
#[allow(dead_code)]
async fn readyz() {}

#[derive(Debug, Serialize, ToSchema)]
#[serde(untagged)]
#[allow(dead_code)]
enum ImageGenerationProfileRequestDoc {
    OpenAi(ImageGenerationRequestDoc),
    Xai(XaiImageGenerationRequestDoc),
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(untagged)]
#[allow(dead_code)]
enum ImageGenerationProfileResponseDoc {
    OpenAi(ImagesResponse),
    Xai(XaiImagesResponseDoc),
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ImageGenerationRequest)]
#[allow(dead_code)]
struct ImageGenerationRequestDoc {
    #[schema(inline)]
    model: Option<ImageModelDoc>,
    #[schema(min_length = 1, max_length = 32000)]
    prompt: String,
    #[schema(minimum = 1, maximum = 10)]
    n: Option<u32>,
    #[schema(default = "auto", example = "1024x1024")]
    size: Option<String>,
    #[schema(inline)]
    quality: Option<ImageQualityDoc>,
    #[schema(inline)]
    output_format: Option<OutputFormatDoc>,
    #[schema(minimum = 0, maximum = 100)]
    output_compression: Option<u16>,
    #[schema(inline)]
    background: Option<BackgroundDoc>,
    #[schema(inline)]
    response_format: Option<ResponseFormatDoc>,
    user: Option<String>,
    #[schema(inline)]
    moderation: Option<ModerationDoc>,
    stream: Option<bool>,
    #[schema(minimum = 0, maximum = 3)]
    partial_images: Option<u32>,
    #[schema(inline)]
    style: Option<StyleDoc>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = XaiImageGenerationRequest)]
#[allow(dead_code)]
struct XaiImageGenerationRequestDoc {
    #[schema(inline)]
    aspect_ratio: Option<XaiImageAspectRatioDoc>,
    model: Option<String>,
    #[schema(minimum = 1, maximum = 10)]
    n: Option<u32>,
    #[schema(min_length = 1)]
    prompt: String,
    #[schema(inline)]
    resolution: Option<XaiImageResolutionDoc>,
    #[schema(inline)]
    response_format: Option<XaiImageResponseFormatDoc>,
    storage_options: Option<XaiImageStorageOptionsDoc>,
    user: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = XaiImagesResponse)]
#[allow(dead_code)]
struct XaiImagesResponseDoc {
    data: Vec<XaiImageDataDoc>,
    usage: Option<XaiImageUsageDoc>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = XaiImageData)]
#[allow(dead_code)]
struct XaiImageDataDoc {
    b64_json: Option<String>,
    file_output: Option<XaiImageFileOutputDoc>,
    mime_type: Option<String>,
    revised_prompt: Option<String>,
    storage_error: Option<String>,
    url: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct XaiImageStorageOptionsDoc {
    #[schema(minimum = 3600, maximum = 2592000)]
    expires_after: Option<u32>,
    filename: String,
    public_url: Option<XaiPublicUrlOptionsDoc>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(untagged)]
#[allow(dead_code)]
enum XaiPublicUrlOptionsDoc {
    Enabled(bool),
    Options(XaiPublicUrlConfigDoc),
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct XaiPublicUrlConfigDoc {
    #[schema(minimum = 3600, maximum = 2592000)]
    expires_after: Option<u32>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct XaiImageFileOutputDoc {
    expires_at: Option<i64>,
    file_id: String,
    filename: String,
    public_url: Option<String>,
    public_url_error: Option<String>,
    public_url_expires_at: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct XaiImageUsageDoc {
    cost_in_usd_ticks: u64,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
enum XaiImageAspectRatioDoc {
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "1:1")]
    R1x1,
    #[serde(rename = "3:4")]
    R3x4,
    #[serde(rename = "4:3")]
    R4x3,
    #[serde(rename = "9:16")]
    R9x16,
    #[serde(rename = "16:9")]
    R16x9,
    #[serde(rename = "2:3")]
    R2x3,
    #[serde(rename = "3:2")]
    R3x2,
    #[serde(rename = "9:19.5")]
    R9x19_5,
    #[serde(rename = "19.5:9")]
    R19_5x9,
    #[serde(rename = "9:20")]
    R9x20,
    #[serde(rename = "20:9")]
    R20x9,
    #[serde(rename = "1:2")]
    R1x2,
    #[serde(rename = "2:1")]
    R2x1,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
enum XaiImageResolutionDoc {
    #[serde(rename = "1k")]
    R1k,
    #[serde(rename = "2k")]
    R2k,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
enum XaiImageResponseFormatDoc {
    #[serde(rename = "url")]
    Url,
    #[serde(rename = "b64_json")]
    B64Json,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ImageEditRequest)]
#[allow(dead_code)]
struct ImageEditRequestDoc {
    #[schema(inline)]
    model: Option<ImageModelDoc>,
    #[schema(min_length = 1, max_length = 32000)]
    prompt: String,
    #[schema(min_items = 1, max_items = 16)]
    image: Option<Vec<ImageReferenceDoc>>,
    #[schema(min_items = 1, max_items = 16)]
    images: Option<Vec<ImageReferenceDoc>>,
    mask: Option<ImageReferenceDoc>,
    #[schema(minimum = 1, maximum = 10)]
    n: Option<u32>,
    size: Option<String>,
    #[schema(inline)]
    quality: Option<ImageQualityDoc>,
    #[schema(inline)]
    output_format: Option<OutputFormatDoc>,
    #[schema(minimum = 0, maximum = 100)]
    output_compression: Option<u16>,
    #[schema(inline)]
    background: Option<BackgroundDoc>,
    #[schema(inline)]
    response_format: Option<ResponseFormatDoc>,
    user: Option<String>,
    #[schema(inline)]
    moderation: Option<ModerationDoc>,
    stream: Option<bool>,
    #[schema(minimum = 0, maximum = 3)]
    partial_images: Option<u32>,
    #[schema(inline)]
    style: Option<StyleDoc>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ImageReference)]
#[allow(dead_code)]
struct ImageReferenceDoc {
    image_url: Option<String>,
    b64_json: Option<String>,
    mime_type: Option<String>,
    file_id: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = VideoGenerationRequest)]
#[allow(dead_code)]
struct VideoGenerationRequestDoc {
    #[schema(inline)]
    aspect_ratio: Option<VideoAspectRatioDoc>,
    #[schema(minimum = 1, maximum = 15, default = 8)]
    duration: Option<u8>,
    image: Option<VideoImageReferenceDoc>,
    model: Option<String>,
    output: Option<VideoOutputDoc>,
    prompt: Option<String>,
    #[schema(max_items = 3)]
    reference_images: Option<Vec<VideoImageReferenceDoc>>,
    #[schema(inline)]
    resolution: Option<VideoResolutionDoc>,
    storage_options: Option<VideoStorageOptionsDoc>,
    user: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct VideoImageReferenceDoc {
    file_id: Option<String>,
    /// Base64 data URL for the current Grok CLI binding.
    url: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct VideoOutputDoc {
    upload_url: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct VideoStorageOptionsDoc {
    #[schema(minimum = 3600, maximum = 2592000)]
    expires_after: Option<i64>,
    filename: String,
    public_url: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct VideoStartResponseDoc {
    request_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct VideoStatusResponseDoc {
    #[schema(example = "pending")]
    status: String,
    error: Option<VideoErrorDoc>,
    model: Option<String>,
    #[schema(minimum = 0, maximum = 100)]
    progress: Option<u8>,
    usage: Option<VideoUsageDoc>,
    video: Option<GeneratedVideoDoc>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct VideoErrorDoc {
    code: String,
    message: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct VideoUsageDoc {
    cost_in_usd_ticks: u64,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct GeneratedVideoDoc {
    duration: u8,
    respect_moderation: bool,
    file_output: Option<VideoFileOutputDoc>,
    storage_error: Option<String>,
    url: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct VideoFileOutputDoc {
    expires_at: Option<i64>,
    file_id: String,
    filename: String,
    public_url: Option<String>,
    public_url_error: Option<String>,
    public_url_expires_at: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = DreaminaImageGenerationRequest)]
#[allow(dead_code)]
struct DreaminaImageGenerationRequestDoc {
    #[schema(min_length = 1)]
    prompt: String,
    #[schema(inline)]
    model_version: Option<DreaminaImageModelDoc>,
    #[schema(inline)]
    ratio: Option<DreaminaImageRatioDoc>,
    #[schema(inline)]
    resolution_type: DreaminaImageResolutionDoc,
    width: Option<u32>,
    height: Option<u32>,
    #[schema(minimum = 1, maximum = 10, default = 1)]
    generate_num: Option<u8>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = DreaminaVideoGenerationRequest)]
#[allow(dead_code)]
struct DreaminaVideoGenerationRequestDoc {
    #[schema(min_length = 1)]
    prompt: String,
    #[schema(inline)]
    model_version: Option<DreaminaVideoModelDoc>,
    #[schema(inline)]
    ratio: Option<DreaminaVideoRatioDoc>,
    #[schema(minimum = 4, maximum = 15, default = 5)]
    duration: Option<u8>,
    #[schema(inline)]
    video_resolution: DreaminaVideoResolutionDoc,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct DreaminaTaskCreatedDoc {
    id: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct DreaminaVideoTaskDoc {
    id: String,
    #[schema(example = "running")]
    status: String,
    model: Option<String>,
    error: Option<DreaminaTaskErrorDoc>,
    content: Option<DreaminaVideoContentDoc>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct DreaminaVideoContentDoc {
    video_url: String,
    duration: u8,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct DreaminaTaskErrorDoc {
    code: String,
    message: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ArkImageGenerationRequest)]
#[allow(dead_code)]
struct ArkImageGenerationRequestDoc {
    #[schema(example = "doubao-seedream-5-0-260128")]
    model: String,
    #[schema(min_length = 1)]
    prompt: String,
    image: Option<ArkStringOrStringsDoc>,
    #[schema(example = "b64_json")]
    response_format: Option<String>,
    #[schema(example = "2K")]
    size: Option<String>,
    seed: Option<i64>,
    #[schema(minimum = 1, maximum = 10)]
    guidance_scale: Option<f64>,
    watermark: Option<bool>,
    optimize_prompt: Option<bool>,
    optimize_prompt_options: Option<ArkOptimizePromptOptionsDoc>,
    #[schema(example = "disabled")]
    sequential_image_generation: Option<String>,
    sequential_image_generation_options: Option<ArkSequentialImageGenerationOptionsDoc>,
    tools: Option<Vec<ArkContentGenerationToolDoc>>,
    output_format: Option<String>,
    stream: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(untagged)]
#[allow(dead_code)]
enum ArkStringOrStringsDoc {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ArkOptimizePromptOptionsDoc {
    thinking: Option<String>,
    mode: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ArkSequentialImageGenerationOptionsDoc {
    #[schema(minimum = 1, maximum = 10)]
    max_images: Option<u8>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ArkContentGenerationToolDoc {
    #[serde(rename = "type")]
    tool_type: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ArkImageGenerationResponseDoc {
    model: String,
    created: i64,
    created_at: i64,
    data: Vec<ArkImageDataDoc>,
    error: Option<ArkContentGenerationErrorDoc>,
    usage: ArkImageUsageDoc,
    tool: Vec<ArkContentGenerationToolDoc>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ArkImageDataDoc {
    url: Option<String>,
    b64_json: Option<String>,
    #[schema(example = "2048x2048")]
    size: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ArkImageUsageDoc {
    generated_images: u32,
    output_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ArkContentGenerationTaskRequest)]
#[allow(dead_code)]
struct ArkContentGenerationTaskRequestDoc {
    #[schema(example = "doubao-seedance-2-0-fast-260128")]
    model: String,
    content: Vec<ArkContentItemDoc>,
    safety_identifier: Option<String>,
    callback_url: Option<String>,
    return_last_frame: Option<bool>,
    service_tier: Option<String>,
    execution_expires_after: Option<u32>,
    priority: Option<i32>,
    generate_audio: Option<bool>,
    draft: Option<bool>,
    camera_fixed: Option<bool>,
    watermark: Option<bool>,
    seed: Option<i64>,
    #[schema(example = "720p")]
    resolution: Option<String>,
    #[schema(example = "16:9")]
    ratio: Option<String>,
    #[schema(minimum = 4, maximum = 15, default = 5)]
    duration: Option<u8>,
    frames: Option<u32>,
    tools: Option<Vec<ArkContentGenerationToolDoc>>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "type")]
#[allow(dead_code)]
enum ArkContentItemDoc {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image_url")]
    ImageUrl {
        image_url: ArkMediaUrlDoc,
        role: String,
    },
    #[serde(rename = "audio_url")]
    AudioUrl {
        audio_url: ArkMediaUrlDoc,
        role: String,
    },
    #[serde(rename = "video_url")]
    VideoUrl {
        video_url: ArkMediaUrlDoc,
        role: String,
    },
    #[serde(rename = "draft_task")]
    DraftTask { draft_task: ArkDraftTaskRefDoc },
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ArkMediaUrlDoc {
    url: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ArkDraftTaskRefDoc {
    id: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ArkContentGenerationTaskIdDoc {
    #[schema(example = "cgt-550e8400-e29b-41d4-a716-446655440000")]
    id: String,
    safety_identifier: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ArkContentGenerationTaskDoc {
    id: String,
    model: String,
    #[schema(example = "running")]
    status: String,
    error: Option<ArkContentGenerationErrorDoc>,
    content: Option<ArkGeneratedContentDoc>,
    usage: Option<ArkContentGenerationUsageDoc>,
    duration: Option<u8>,
    resolution: Option<String>,
    ratio: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ArkContentGenerationErrorDoc {
    message: String,
    code: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ArkGeneratedContentDoc {
    video_url: Option<String>,
    last_frame_url: Option<String>,
    file_url: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ArkContentGenerationUsageDoc {
    completion_tokens: u64,
    total_tokens: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
enum DreaminaImageModelDoc {
    #[serde(rename = "3.0")]
    V3_0,
    #[serde(rename = "3.1")]
    V3_1,
    #[serde(rename = "4.0")]
    V4_0,
    #[serde(rename = "4.1")]
    V4_1,
    #[serde(rename = "4.5")]
    V4_5,
    #[serde(rename = "4.6")]
    V4_6,
    #[serde(rename = "4.7")]
    V4_7,
    #[serde(rename = "5.0")]
    V5_0,
    #[serde(rename = "5.0Pro")]
    V5_0Pro,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
enum DreaminaImageRatioDoc {
    #[serde(rename = "21:9")]
    R21x9,
    #[serde(rename = "16:9")]
    R16x9,
    #[serde(rename = "3:2")]
    R3x2,
    #[serde(rename = "4:3")]
    R4x3,
    #[serde(rename = "1:1")]
    R1x1,
    #[serde(rename = "3:4")]
    R3x4,
    #[serde(rename = "2:3")]
    R2x3,
    #[serde(rename = "9:16")]
    R9x16,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
enum DreaminaImageResolutionDoc {
    #[serde(rename = "1k")]
    K1,
    #[serde(rename = "2k")]
    K2,
    #[serde(rename = "4k")]
    K4,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
enum DreaminaVideoModelDoc {
    #[serde(rename = "seedance2.0")]
    Standard,
    #[serde(rename = "seedance2.0fast")]
    Fast,
    #[serde(rename = "seedance2.0_vip")]
    Vip,
    #[serde(rename = "seedance2.0fast_vip")]
    FastVip,
    #[serde(rename = "seedance2.0mini")]
    Mini,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
enum DreaminaVideoRatioDoc {
    #[serde(rename = "1:1")]
    R1x1,
    #[serde(rename = "3:4")]
    R3x4,
    #[serde(rename = "16:9")]
    R16x9,
    #[serde(rename = "4:3")]
    R4x3,
    #[serde(rename = "9:16")]
    R9x16,
    #[serde(rename = "21:9")]
    R21x9,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
enum DreaminaVideoResolutionDoc {
    #[serde(rename = "720p")]
    P720,
    #[serde(rename = "1080p")]
    P1080,
    #[serde(rename = "4k")]
    K4,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
enum VideoAspectRatioDoc {
    #[serde(rename = "1:1")]
    R1x1,
    #[serde(rename = "16:9")]
    R16x9,
    #[serde(rename = "9:16")]
    R9x16,
    #[serde(rename = "4:3")]
    R4x3,
    #[serde(rename = "3:4")]
    R3x4,
    #[serde(rename = "3:2")]
    R3x2,
    #[serde(rename = "2:3")]
    R2x3,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
enum VideoResolutionDoc {
    #[serde(rename = "480p")]
    P480,
    #[serde(rename = "720p")]
    P720,
    #[serde(rename = "1080p")]
    P1080,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = CreateFileRequest)]
#[allow(dead_code)]
struct CreateFileRequestDoc {
    /// File bytes. purpose=batch requires a UTF-8 .jsonl file no larger than 8 MiB.
    #[schema(value_type = String, format = Binary)]
    file: Vec<u8>,
    #[schema(inline)]
    purpose: FilePurposeDoc,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = FilePurpose)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
enum FilePurposeDoc {
    Assistants,
    Batch,
    BatchOutput,
    #[serde(rename = "fine-tune")]
    FineTune,
    Vision,
    UserData,
    Evals,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = FileObject)]
#[allow(dead_code)]
struct FileObjectDoc {
    #[schema(example = "file-550e8400-e29b-41d4-a716-446655440000")]
    id: String,
    #[schema(example = "file")]
    object: String,
    bytes: u64,
    created_at: i64,
    expires_at: Option<i64>,
    filename: String,
    #[schema(inline)]
    purpose: FilePurposeDoc,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = FileList)]
#[allow(dead_code)]
struct FileListDoc {
    #[schema(example = "list")]
    object: String,
    data: Vec<FileObjectDoc>,
    first_id: Option<String>,
    last_id: Option<String>,
    has_more: bool,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = DeletedFileObject)]
#[allow(dead_code)]
struct DeletedFileObjectDoc {
    id: String,
    #[schema(example = "file")]
    object: String,
    deleted: bool,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = CreateBatchRequest)]
#[allow(dead_code)]
struct CreateBatchRequestDoc {
    #[schema(example = "file-550e8400-e29b-41d4-a716-446655440000")]
    input_file_id: String,
    /// Current gateway support is limited to /v1/images/generations.
    #[schema(example = "/v1/images/generations")]
    endpoint: String,
    /// Current gateway support is limited to the OpenAI-compatible 24h completion window.
    #[schema(example = "24h")]
    completion_window: String,
    /// Up to 16 string entries; keys are at most 64 bytes and values at most 512 bytes.
    metadata: Option<std::collections::BTreeMap<String, String>>,
    output_expires_after: Option<OutputExpiresAfterDoc>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = OutputExpiresAfter)]
#[allow(dead_code)]
struct OutputExpiresAfterDoc {
    #[schema(example = "created_at")]
    anchor: String,
    #[schema(minimum = 3600, maximum = 2592000)]
    seconds: u32,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = BatchStatus)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
enum BatchStatusDoc {
    Validating,
    Failed,
    InProgress,
    Finalizing,
    Completed,
    Expired,
    Cancelling,
    Cancelled,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = BatchRequestCounts)]
#[allow(dead_code)]
struct BatchRequestCountsDoc {
    total: u32,
    completed: u32,
    failed: u32,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = BatchObject)]
#[allow(dead_code)]
struct BatchObjectDoc {
    #[schema(example = "batch-550e8400-e29b-41d4-a716-446655440000")]
    id: String,
    #[schema(example = "batch")]
    object: String,
    #[schema(example = "/v1/images/generations")]
    endpoint: String,
    errors: Option<Value>,
    input_file_id: String,
    #[schema(example = "24h")]
    completion_window: String,
    #[schema(inline)]
    status: BatchStatusDoc,
    output_file_id: Option<String>,
    error_file_id: Option<String>,
    created_at: i64,
    in_progress_at: Option<i64>,
    expires_at: Option<i64>,
    finalizing_at: Option<i64>,
    completed_at: Option<i64>,
    failed_at: Option<i64>,
    expired_at: Option<i64>,
    cancelling_at: Option<i64>,
    cancelled_at: Option<i64>,
    request_counts: BatchRequestCountsDoc,
    metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = BatchList)]
#[allow(dead_code)]
struct BatchListDoc {
    #[schema(example = "list")]
    object: String,
    data: Vec<BatchObjectDoc>,
    first_id: Option<String>,
    last_id: Option<String>,
    has_more: bool,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = CreateServiceAccountRequest)]
#[allow(dead_code)]
struct CreateServiceAccountRequestDoc {
    #[schema(min_length = 1, max_length = 128)]
    name: String,
    #[schema(value_type = Option<String>)]
    route_id: Option<String>,
    permission_mode: ApiKeyPermissionMode,
    permissions: ApiKeyPermissions,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = CreateUserApiKeyRequest)]
#[allow(dead_code)]
struct CreateUserApiKeyRequestDoc {
    #[schema(min_length = 1, max_length = 128)]
    name: Option<String>,
    permission_mode: ApiKeyPermissionMode,
    permissions: ApiKeyPermissions,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = UpdateApiKeyRequest)]
#[allow(dead_code)]
struct UpdateApiKeyRequestDoc {
    #[schema(min_length = 1, max_length = 128)]
    name: String,
    permission_mode: ApiKeyPermissionMode,
    permissions: ApiKeyPermissions,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = CreateProjectRequest)]
#[allow(dead_code)]
struct CreateProjectRequestDoc {
    #[schema(min_length = 1, max_length = 256)]
    organization_id: String,
    #[schema(min_length = 1, max_length = 128)]
    name: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = UpdateProjectRequest)]
#[allow(dead_code)]
struct UpdateProjectRequestDoc {
    #[schema(min_length = 1, max_length = 128)]
    name: String,
    service_tier: crate::service_tiers::ProjectServiceTier,
    user_api_keys_disabled: bool,
    #[schema(minimum = 1)]
    expected_settings_version: i64,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct AdminLoginRequestDoc {
    email: String,
    password: String,
    client_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct AdminRefreshRequestDoc {
    refresh_token: String,
    client_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct AdminLogoutRequestDoc {
    refresh_token: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct AdminTokenResponseDoc {
    access_token: String,
    #[schema(example = "Bearer")]
    token_type: String,
    expires_in: u64,
    refresh_token: String,
    refresh_expires_in: u64,
    user: AdminUserDoc,
    session: AdminSessionDoc,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct AdminUserDoc {
    id: String,
    email: String,
    display_name: String,
    roles: Vec<String>,
    scopes: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct AdminSessionDoc {
    id: String,
    absolute_expires_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct AdminPrincipalDoc {
    user_id: String,
    session_id: String,
    roles: Vec<String>,
    scopes: Vec<String>,
    authz_version: i64,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = PreviewProviderCostAllocationRequest)]
#[allow(dead_code)]
struct PreviewProviderCostAllocationRequestDoc {
    provider_id: String,
    provider_account_id: String,
    price_book_version_id: String,
    period_start_ms: i64,
    period_end_ms: i64,
    #[schema(min_length = 3, max_length = 3, example = "USD")]
    currency: String,
    #[schema(example = "12500000")]
    total_amount_micros: String,
    #[schema(example = "successful_output")]
    allocation_basis: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = CreateProviderCostAllocationDraftRequest)]
#[allow(dead_code)]
struct CreateProviderCostAllocationDraftRequestDoc {
    provider_id: String,
    provider_account_id: String,
    price_book_version_id: String,
    period_start_ms: i64,
    period_end_ms: i64,
    #[schema(min_length = 3, max_length = 3, example = "USD")]
    currency: String,
    #[schema(example = "12500000")]
    total_amount_micros: String,
    #[schema(example = "successful_output")]
    allocation_basis: String,
    #[schema(min_length = 64, max_length = 64)]
    expected_preview_hash: String,
    #[schema(min_length = 1, max_length = 255)]
    idempotency_key: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = CloseProviderCostAllocationRequest)]
#[allow(dead_code)]
struct CloseProviderCostAllocationRequestDoc {
    #[schema(minimum = 1, example = 1)]
    expected_control_version: i64,
    #[schema(min_length = 64, max_length = 64)]
    expected_snapshot_hash: String,
    #[schema(example = "provider_subscription")]
    source_kind: String,
    #[schema(example = "subscription:2026-07")]
    source_reference: String,
    #[schema(min_length = 64, max_length = 64)]
    source_evidence_hash: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ProviderCostAllocationLinePreview)]
#[allow(dead_code)]
struct ProviderCostAllocationLinePreviewDoc {
    job_id: String,
    output_id: Option<String>,
    basis_receipt_id: String,
    basis_receipt_payload_hash: String,
    basis_quote_id: String,
    basis_quote_hash: String,
    basis_quantity: String,
    basis_unit: String,
    amount_micros: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ProviderCostAllocationPreview)]
#[allow(dead_code)]
struct ProviderCostAllocationPreviewDoc {
    #[schema(example = "billing.provider_cost_allocation_preview")]
    object: String,
    provider_id: String,
    provider_account_id: String,
    price_book_version_id: String,
    period_start_ms: i64,
    period_end_ms: i64,
    currency: String,
    total_amount_micros: String,
    allocation_basis: String,
    candidate_count: usize,
    allocated_amount_micros: String,
    residual_amount_micros: String,
    preview_hash: String,
    lines: Vec<ProviderCostAllocationLinePreviewDoc>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ProviderCostAllocationSummary)]
#[allow(dead_code)]
struct ProviderCostAllocationSummaryDoc {
    #[schema(example = "billing.provider_cost_allocation")]
    object: String,
    provider_cost_allocation_pool_id: String,
    semantic_key: String,
    provider_id: String,
    provider_account_id: String,
    price_book_version_id: String,
    period_start_ms: i64,
    period_end_ms: i64,
    currency: String,
    total_amount_micros: String,
    residual_amount_micros: String,
    allocated_amount_micros: String,
    allocation_basis: String,
    #[schema(example = "draft")]
    state: String,
    control_version: i64,
    candidate_count: i64,
    created_at_ms: i64,
    closed_at_ms: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ProviderCostAllocationLine)]
#[allow(dead_code)]
struct ProviderCostAllocationLineDoc {
    provider_cost_allocation_line_id: String,
    job_id: String,
    output_id: Option<String>,
    basis_receipt_id: String,
    basis_receipt_payload_hash: String,
    basis_quote_id: String,
    basis_quote_hash: String,
    basis_quantity: String,
    basis_unit: String,
    amount_micros: String,
    created_at_ms: i64,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ProviderCostAllocationClosure)]
#[allow(dead_code)]
struct ProviderCostAllocationClosureDoc {
    source_kind: String,
    source_reference: String,
    source_evidence_hash: String,
    closed_by_user_id: String,
    closed_by_session_id: String,
    created_at_ms: i64,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ProviderCostAllocationDetail)]
#[allow(dead_code)]
struct ProviderCostAllocationDetailDoc {
    #[serde(flatten)]
    pool: ProviderCostAllocationSummaryDoc,
    preview_hash: String,
    lines: Vec<ProviderCostAllocationLineDoc>,
    closure: Option<ProviderCostAllocationClosureDoc>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ProviderCostAllocationList)]
#[allow(dead_code)]
struct ProviderCostAllocationListDoc {
    #[schema(example = "list")]
    object: String,
    as_of_ms: i64,
    data: Vec<ProviderCostAllocationSummaryDoc>,
    has_more: bool,
    next_after: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ErrorResponseDoc {
    error: ErrorBodyDoc,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ErrorBodyDoc {
    message: String,
    #[serde(rename = "type")]
    error_type: String,
    #[schema(nullable)]
    param: Option<String>,
    #[schema(nullable)]
    code: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::openapi_json;

    #[test]
    fn provider_cost_allocation_openapi_exposes_receipt_exact_close() {
        let axum::Json(document) = openapi_json();
        for path in [
            "/admin/v1/billing/provider-cost-allocation-pools",
            "/admin/v1/billing/provider-cost-allocation-pools/{pool_id}",
            "/admin/v1/billing/provider-cost-allocation-pools/preview",
        ] {
            assert!(
                document
                    .pointer(&format!(
                        "/paths/{}",
                        path.replace('~', "~0").replace('/', "~1")
                    ))
                    .is_some(),
                "OpenAPI is missing {path}"
            );
        }
        assert!(
            document
                .pointer(
                    "/components/schemas/PreviewProviderCostAllocationRequest\
                     /properties/total_amount_micros/type"
                )
                .is_some_and(|value| value == "string"),
            "allocation micros must stay a decimal string across JSON"
        );
        assert!(
            document
                .pointer(
                    "/paths/~1admin~1v1~1billing~1provider-cost-allocation-pools\
                     ~1{pool_id}/post"
                )
                .is_some(),
            "allocation close must be documented after receipt snapshots and ledger sealing"
        );
        assert!(
            document
                .pointer(
                    "/components/schemas/CloseProviderCostAllocationRequest\
                     /properties/expected_snapshot_hash/type"
                )
                .is_some_and(|value| value == "string"),
            "allocation close schema must require the immutable candidate snapshot"
        );
        assert!(
            document
                .pointer(
                    "/components/schemas/ProviderCostAllocationLine\
                     /properties/basis_receipt_id/type"
                )
                .is_some_and(|value| value == "string"),
            "allocation lines must expose their exact receipt"
        );
        assert!(
            document
                .pointer("/components/schemas/ProviderCostAllocationDetail")
                .is_some_and(|schema| schema.to_string().contains("\"closure\"")),
            "allocation detail must expose immutable closure evidence"
        );
    }
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
enum ImageModelDoc {
    #[serde(rename = "gpt-image-2")]
    GptImage2,
    #[serde(rename = "gpt-image-2-2026-04-21")]
    GptImage2Snapshot,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
#[serde(rename_all = "lowercase")]
enum ImageQualityDoc {
    Auto,
    Low,
    Medium,
    High,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
#[serde(rename_all = "lowercase")]
enum OutputFormatDoc {
    Png,
    Jpeg,
    Webp,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
#[serde(rename_all = "lowercase")]
enum BackgroundDoc {
    Auto,
    Opaque,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
#[serde(rename_all = "lowercase")]
enum ResponseFormatDoc {
    B64Json,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
#[serde(rename_all = "lowercase")]
enum ModerationDoc {
    Auto,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
#[serde(rename_all = "lowercase")]
enum StyleDoc {
    Vivid,
    Natural,
}

fn patch_generated_schema(value: &mut Value) {
    patch_property_enum(
        value,
        "ImageGenerationRequest",
        "model",
        openai_codex::MODELS,
    );
    patch_property_enum(value, "ImageGenerationRequest", "moderation", &["auto"]);
    patch_property_enum(
        value,
        "ImageGenerationRequest",
        "quality",
        &["auto", "low", "medium", "high"],
    );
    patch_property_enum(
        value,
        "ImageGenerationRequest",
        "output_format",
        &["png", "jpeg", "webp"],
    );
    patch_property_enum(
        value,
        "ImageGenerationRequest",
        "background",
        &["auto", "opaque"],
    );
    patch_property_enum(
        value,
        "ImageGenerationRequest",
        "response_format",
        &["b64_json"],
    );
    patch_property_enum(value, "ImageEditRequest", "model", openai_codex::MODELS);
    patch_property_enum(value, "ImageEditRequest", "moderation", &["auto"]);
    patch_property_enum(
        value,
        "ImageEditRequest",
        "quality",
        &["auto", "low", "medium", "high"],
    );
    patch_property_enum(
        value,
        "ImageEditRequest",
        "output_format",
        &["png", "jpeg", "webp"],
    );
    patch_property_enum(value, "ImageEditRequest", "background", &["auto", "opaque"]);
    patch_property_enum(value, "ImageEditRequest", "response_format", &["b64_json"]);
    if let Some(size_schema) = value
        .pointer_mut("/components/schemas/ImageGenerationRequest/properties/size")
        .and_then(Value::as_object_mut)
    {
        size_schema.insert(
            "description".to_string(),
            json!("auto, WIDTHxHEIGHT, or gateway aspect-ratio extension W:H such as 1:1, 4:3, or 16:9."),
        );
    }
    if let Some(edit_schema) = value
        .pointer_mut("/components/schemas/ImageEditRequest")
        .and_then(Value::as_object_mut)
    {
        edit_schema.insert(
            "anyOf".to_string(),
            json!([
                { "required": ["image"] },
                { "required": ["images"] }
            ]),
        );
    }
    if let Some(reference_schema) = value
        .pointer_mut("/components/schemas/ImageReference")
        .and_then(Value::as_object_mut)
    {
        reference_schema.insert(
            "oneOf".to_string(),
            json!([
                { "required": ["image_url"] },
                { "required": ["b64_json"] },
                { "required": ["file_id"] }
            ]),
        );
        if let Some(properties) = reference_schema
            .get_mut("properties")
            .and_then(Value::as_object_mut)
        {
            if let Some(image_url) = properties
                .get_mut("image_url")
                .and_then(Value::as_object_mut)
            {
                image_url.insert(
                    "description".to_string(),
                    json!("Base64 data URL is supported by the gateway. Official OpenAI also supports remote URLs; native Codex CLI gateway rejects remote URL fetching unless implemented with SSRF controls."),
                );
            }
            if let Some(b64_json) = properties
                .get_mut("b64_json")
                .and_then(Value::as_object_mut)
            {
                b64_json.insert(
                    "description".to_string(),
                    json!("Raw base64 image bytes supported by the gateway for JSON API clients. Use mime_type to declare image/png, image/jpeg, or image/webp; if omitted, the gateway infers the type from image magic bytes."),
                );
            }
            if let Some(mime_type) = properties
                .get_mut("mime_type")
                .and_then(Value::as_object_mut)
            {
                mime_type.insert(
                    "description".to_string(),
                    json!("MIME type for b64_json. Images support image/png, image/jpeg, or image/webp; masks must be image/png."),
                );
                mime_type.insert(
                    "enum".to_string(),
                    json!(["image/png", "image/jpeg", "image/webp"]),
                );
            }
            if let Some(file_id) = properties.get_mut("file_id").and_then(Value::as_object_mut) {
                file_id.insert(
                    "description".to_string(),
                    json!("Official OpenAI file_id reference. Native Codex CLI gateway rejects it because it cannot access OpenAI Files."),
                );
            }
        }
    }
}

fn patch_property_enum(value: &mut Value, schema: &str, property: &str, values: &[&str]) {
    let pointer = format!("/components/schemas/{schema}/properties/{property}");
    if let Some(property_schema) = value.pointer_mut(&pointer).and_then(Value::as_object_mut) {
        property_schema.insert("enum".to_string(), json!(values));
    }
}
