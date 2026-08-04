use std::env;

use gpt_image_2_gateway::{
    CreditGrantService, EditJob, InputImage, PostgresCreditGrantService,
    admission::{
        AdmissionClaim, AdmissionContract, AdmissionError, AdmissionStore, AdmissionTicket,
        AttachInputManifest, AttachInputObject, AttachJob, ClaimAdmission, CustomerPricingIntent,
        DreaminaImageAdmissionPlan, DreaminaVideoAdmissionPlan, EDIT_COMMAND_SCHEMA,
        EDIT_INPUT_MANIFEST_SCHEMA, EditCommandV1, EditInputDescriptorV1, EditInputRoleV1,
        GENERATION_COMMAND_SCHEMA, GenerationCommandV1, PostgresAdmissionStore,
        VIDEO_GENERATION_OPERATION, WorkOutcome, XaiImageAdmissionPlan, XaiImageEditAdmissionPlan,
        XaiImageEditFallbackMode,
    },
    credit_grants::{CreateCreditGrantRequest, CreditGrantActor},
    database::{connect_test_pool_with_search_path, run_migrations},
    input_blobs::{InputBlobKey, InputBlobRef},
};
use image_api_contracts::xai::{
    XAI_IMAGES_API_PROFILE, XaiImageGenerationRequest, XaiImageResolution, XaiImageResponseFormat,
};
use image_api_contracts::{
    ark::{ARK_CONTENT_GENERATION_API_PROFILE, ARK_IMAGES_API_PROFILE},
    dreamina::{
        DREAMINA_IMAGES_API_PROFILE, DREAMINA_VIDEOS_API_PROFILE, DreaminaImageGenerationRequest,
        DreaminaVideoGenerationRequest,
    },
};
use image_provider_dreamina_cli::DREAMINA_SUBMIT_COMMAND_SCHEMA;
use image_provider_grok_cli::{
    GROK_IMAGE_EDIT_COMMAND_SCHEMA, GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
};
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;
const DREAMINA_VIDEO_EXECUTION_MODEL: &str = "dreamina-video-seedance2-fast";

#[tokio::test]
async fn migration_creates_durable_admission_tables() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        for table in [
            "admission_sessions",
            "idempotency_requests",
            "job_payloads",
            "work_items",
            "job_attempts",
            "job_events",
            "outbox_events",
            "scheduler_scopes",
        ] {
            let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                .bind(table)
                .fetch_one(&database.pool)
                .await
                .map_err(|error| format!("failed to inspect {table}: {error}"))?;
            require(exists, format!("migration did not create {table}"))?;
        }
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn final_accept_creates_one_frozen_economic_identity_set() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let ticket = claim_owner(&store, claim_request(None, "9".repeat(64))).await?;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        let mut request = attach_request(ticket, job_id);
        request.contract = AdmissionContract::OutputEconomicsV2;

        let first = store
            .attach(request.clone())
            .await
            .map_err(|error| format!("first attach failed: {error:?}"))?;
        let replay = store
            .attach(request)
            .await
            .map_err(|error| format!("attach replay failed: {error:?}"))?;
        require(
            first == replay,
            "attach replay changed the durable work identity",
        )?;

        let counts: (i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT (SELECT COUNT(*) FROM job_outputs WHERE job_id = $1),
                   (SELECT COUNT(*) FROM price_quotes WHERE job_id = $1),
                   (SELECT COUNT(*) FROM output_holds h
                      JOIN job_outputs o ON o.output_id = h.output_id
                     WHERE o.job_id = $1),
                   (SELECT economics_contract_version FROM jobs WHERE job_id = $1)::BIGINT
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect economic identities: {error}"))?;
        require(
            counts == (1, 1, 1, 2),
            format!("unexpected economic identity counts: {counts:?}"),
        )?;

        let frozen: (String, i64, i64, String, String) = sqlx::query_as(
            r#"
            SELECT q.currency, q.output_count::BIGINT, q.max_total_micros,
                   q.quote_hash, h.state
            FROM price_quotes q
            JOIN job_outputs o ON o.job_id = q.job_id
            JOIN output_holds h ON h.output_id = o.output_id
            WHERE q.job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect frozen economics: {error}"))?;
        require(
            frozen.0 == "USD"
                && frozen.1 == 1
                && frozen.2 == 0
                && frozen.3.len() == 64
                && frozen.4 == "held",
            format!("invalid frozen economics: {frozen:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn customer_pricing_v4_accept_is_atomic_and_replayable() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_customer_price_v4(&database.pool, 11).await?;
        seed_billing_account(&database.pool, 1000).await?;

        let store = PostgresAdmissionStore::new(database.pool.clone());
        let admission_hash = "f".repeat(64);
        let (ticket, command_json, command_hash) =
            claim_customer_pricing_owner_with_ticket_hash(&store, 1, admission_hash.clone())
                .await?;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        seed_job_project_attribution(&database.pool, job_id).await?;
        let mut request = attach_request(ticket, job_id);
        request.command_json = command_json;
        request.contract = AdmissionContract::CustomerPricingV4;
        let mut pricing = customer_pricing_intent();
        pricing.service_tier_decision =
            gpt_image_2_gateway::service_tiers::ServiceTierDecision::for_default_only_project(
                gpt_image_2_gateway::service_tiers::ProjectServiceTier::Priority,
            );
        pricing.provider_command_hash = Some(command_hash.clone());
        request.customer_pricing = Some(pricing);

        let first = store
            .attach(request.clone())
            .await
            .map_err(|error| format!("v4 attach failed: {error:?}"))?;
        let replay = store
            .attach(request)
            .await
            .map_err(|error| format!("v4 attach replay failed: {error:?}"))?;
        require(first == replay, "v4 replay changed the work identity")?;
        let stored_payload_hash: String =
            sqlx::query_scalar("SELECT request_hash FROM job_payloads WHERE job_id = $1")
                .bind(job_id)
                .fetch_one(&database.pool)
                .await
                .map_err(|error| format!("failed to inspect durable payload hash: {error}"))?;
        require(
            admission_hash != command_hash && stored_payload_hash == command_hash,
            "v4 admission did not separate admission and provider command hashes",
        )?;

        let state: (i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, String) = sqlx::query_as(
            r#"
                SELECT
                  (SELECT economics_contract_version FROM jobs WHERE job_id = $1)::BIGINT,
                  (SELECT COUNT(*) FROM job_outputs WHERE job_id = $1),
                  (SELECT COUNT(*) FROM customer_price_quotes WHERE job_id = $1),
                  (SELECT COUNT(*) FROM customer_price_quote_lines WHERE job_id = $1),
                  (SELECT COUNT(*) FROM customer_billing_holds WHERE job_id = $1),
                  (SELECT COUNT(*) FROM work_items WHERE job_id = $1),
                  (SELECT COUNT(*) FROM price_quotes WHERE job_id = $1),
                  (SELECT COUNT(*) FROM output_holds WHERE job_id = $1),
                  (SELECT max_total_micros FROM customer_price_quotes WHERE job_id = $1),
                  (SELECT held_micros FROM customer_billing_holds WHERE job_id = $1),
                  (SELECT state FROM customer_billing_holds WHERE job_id = $1)
                "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect v4 admission: {error}"))?;
        require(
            state == (4, 1, 1, 3, 1, 1, 0, 0, 11, 11, "held".to_string()),
            format!("v4 admission was not atomic: {state:?}"),
        )?;

        let timestamp_is_frozen: bool = sqlx::query_scalar(
            r#"
            SELECT quote.created_at_ms = attribution.admitted_at_ms
            FROM customer_price_quotes quote
            JOIN job_auth_attributions attribution ON attribution.job_id = quote.job_id
            WHERE quote.job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect v4 admission timestamp: {error}"))?;
        require(
            timestamp_is_frozen,
            "v4 quote did not use the admission timestamp",
        )?;
        let decision: (String, String, String, Option<String>) = sqlx::query_as(
            r#"
            SELECT requested_service_tier, project_service_tier,
                   effective_service_tier, fallback_reason
            FROM job_service_tier_decisions
            WHERE job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect service tier decision: {error}"))?;
        require(
            decision
                == (
                    "auto".to_string(),
                    "priority".to_string(),
                    "default".to_string(),
                    Some("model_service_tier_unsupported".to_string()),
                ),
            format!("v4 admission froze the wrong service tier decision: {decision:?}"),
        )?;
        let mutation = sqlx::query(
            "UPDATE job_service_tier_decisions SET effective_service_tier = 'priority' WHERE job_id = $1",
        )
        .bind(job_id)
        .execute(&database.pool)
        .await;
        require(
            mutation.is_err(),
            "service tier decision remained mutable after admission",
        )?;

        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn codex_snapshot_generation_uses_canonical_price_and_snapshot_command() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_customer_price_v4(&database.pool, 11).await?;
        seed_billing_account(&database.pool, 1000).await?;

        let store = PostgresAdmissionStore::new(database.pool.clone());
        let (ticket, command_json) = claim_customer_pricing_owner_with_identity(
            &store,
            1,
            "openai-codex",
            "gpt-image-2-2026-04-21",
            "openai-images-v1",
        )
        .await?;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        set_job_execution_model(&database.pool, job_id, "gpt-image-2-2026-04-21").await?;
        seed_codex_snapshot_job_project_attribution(
            &database.pool,
            job_id,
            "images.generations",
            GENERATION_COMMAND_SCHEMA,
        )
        .await?;
        let mut request = attach_request(ticket, job_id);
        request.command_json = command_json;
        request.contract = AdmissionContract::CustomerPricingV4;
        request.customer_pricing = Some(codex_snapshot_pricing_intent());

        store
            .attach(request)
            .await
            .map_err(|error| format!("snapshot generation admission failed: {error:?}"))?;

        let identity: (String, String, String, String, String, String) = sqlx::query_as(
            r#"
            SELECT job.model, payload.command_json ->> 'model',
                   quote.public_model_id, quote.provider_model_id,
                   version.public_model_id, version.provider_model_id
            FROM jobs job
            JOIN job_payloads payload ON payload.job_id = job.job_id
            JOIN customer_price_quotes quote ON quote.job_id = job.job_id
            JOIN price_book_versions version
              ON version.price_book_version_id = quote.price_book_version_id
            WHERE job.job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect snapshot generation identity: {error}"))?;
        require(
            identity
                == (
                    "gpt-image-2-2026-04-21".to_string(),
                    "gpt-image-2-2026-04-21".to_string(),
                    "gpt-image-2".to_string(),
                    "gpt-image-2".to_string(),
                    "gpt-image-2".to_string(),
                    "gpt-image-2".to_string(),
                ),
            format!("snapshot generation identity drifted: {identity:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn configured_soft_project_budget_does_not_block_customer_admission() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_customer_price_v4(&database.pool, 11).await?;
        seed_billing_account(&database.pool, 1000).await?;
        let store = PostgresAdmissionStore::new(database.pool.clone());

        let (first_ticket, first_command) = claim_customer_pricing_owner(&store, 1).await?;
        let first_job = insert_job_for_ticket(
            &database.pool,
            &first_ticket,
            "tenant-a",
            "generation",
            None,
        )
        .await?;
        seed_job_project_attribution(&database.pool, first_job).await?;
        seed_project_spend_budget(&database.pool, "soft", 11).await?;
        let mut first_request = attach_request(first_ticket, first_job);
        first_request.command_json = first_command;
        first_request.contract = AdmissionContract::CustomerPricingV4;
        first_request.customer_pricing = Some(customer_pricing_intent());
        store
            .attach(first_request)
            .await
            .map_err(|error| format!("first soft-budget admission failed: {error:?}"))?;

        let (second_ticket, second_command) = claim_customer_pricing_owner(&store, 1).await?;
        let second_job = insert_job_for_ticket(
            &database.pool,
            &second_ticket,
            "tenant-a",
            "generation",
            None,
        )
        .await?;
        seed_job_project_attribution(&database.pool, second_job).await?;
        let mut second_request = attach_request(second_ticket, second_job);
        second_request.command_json = second_command;
        second_request.contract = AdmissionContract::CustomerPricingV4;
        second_request.customer_pricing = Some(customer_pricing_intent());
        store
            .attach(second_request)
            .await
            .map_err(|error| format!("second soft-budget admission failed: {error:?}"))?;

        let state: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM customer_price_quotes WHERE project_id = 'project-a'),
              (SELECT COUNT(*)
                 FROM customer_billing_holds hold
                 JOIN customer_price_quotes quote ON quote.quote_id = hold.quote_id
                WHERE quote.project_id = 'project-a' AND hold.state = 'held'),
              (SELECT COALESCE(SUM(hold.held_micros), 0)::BIGINT
                 FROM customer_billing_holds hold
                 JOIN customer_price_quotes quote ON quote.quote_id = hold.quote_id
                WHERE quote.project_id = 'project-a' AND hold.state = 'held')
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect soft-budget admissions: {error}"))?;
        require(
            state == (2, 2, 22),
            format!("soft budget unexpectedly blocked or corrupted admissions: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn project_hard_budget_serializes_concurrent_customer_admission() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_customer_price_v4(&database.pool, 11).await?;
        seed_billing_account(&database.pool, 1000).await?;
        let store = PostgresAdmissionStore::new(database.pool.clone());

        let (first_ticket, first_command) = claim_customer_pricing_owner(&store, 1).await?;
        let first_job =
            insert_job_for_ticket(&database.pool, &first_ticket, "tenant-a", "generation", None)
                .await?;
        seed_job_project_attribution(&database.pool, first_job).await?;
        let mut first_request = attach_request(first_ticket, first_job);
        first_request.command_json = first_command;
        first_request.contract = AdmissionContract::CustomerPricingV4;
        first_request.customer_pricing = Some(customer_pricing_intent());

        let (second_ticket, second_command) = claim_customer_pricing_owner(&store, 1).await?;
        let second_job =
            insert_job_for_ticket(&database.pool, &second_ticket, "tenant-a", "generation", None)
                .await?;
        seed_job_project_attribution(&database.pool, second_job).await?;
        let mut second_request = attach_request(second_ticket, second_job);
        second_request.command_json = second_command;
        second_request.contract = AdmissionContract::CustomerPricingV4;
        second_request.customer_pricing = Some(customer_pricing_intent());
        seed_project_spend_budget(&database.pool, "hard", 11).await?;

        let first_store = store.clone();
        let second_store = store.clone();
        let first_attempt = first_request.clone();
        let second_attempt = second_request.clone();
        let (first_result, second_result) = tokio::join!(
            first_store.attach(first_attempt),
            second_store.attach(second_attempt)
        );
        let (winner_job, winner_request, loser_job) = match (&first_result, &second_result) {
            (Ok(_), Err(AdmissionError::ProjectBudgetExceeded)) => {
                (first_job, first_request, second_job)
            }
            (Err(AdmissionError::ProjectBudgetExceeded), Ok(_)) => {
                (second_job, second_request, first_job)
            }
            _ => {
                return Err(format!(
                    "hard limit did not admit exactly one concurrent request: first={first_result:?}, second={second_result:?}"
                ));
            }
        };

        store
            .attach(winner_request)
            .await
            .map_err(|error| format!("winning admission was not replayable: {error:?}"))?;
        let state: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM customer_price_quotes WHERE project_id = 'project-a'),
              (SELECT COUNT(*)
                 FROM customer_billing_holds hold
                 JOIN customer_price_quotes quote ON quote.quote_id = hold.quote_id
                WHERE quote.project_id = 'project-a' AND hold.state = 'held'),
              (SELECT COUNT(*) FROM work_items WHERE job_id = $1),
              (SELECT economics_contract_version FROM jobs WHERE job_id = $2)::BIGINT,
              (SELECT COUNT(*) FROM customer_price_quotes WHERE job_id = $2),
              (SELECT COUNT(*) FROM work_items WHERE job_id = $2)
            "#,
        )
        .bind(winner_job)
        .bind(loser_job)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect hard-budget admissions: {error}"))?;
        require(
            state == (1, 1, 1, 1, 0, 0),
            format!("hard-limit rejection left partial or duplicate state: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn edit_customer_pricing_v4_freezes_quote_and_preserves_input_manifest() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_customer_price_v4_for_operation(&database.pool, "edit", 13).await?;
        seed_billing_account(&database.pool, 1000).await?;

        let store = PostgresAdmissionStore::new(database.pool.clone());
        let provisional_inputs = edit_input_specs(Uuid::new_v4(), None);
        let provisional_command = edit_command(&provisional_inputs);
        let ticket = claim_edit_owner(&store, &provisional_command).await?;
        let inputs = edit_input_specs(ticket.session_id, None);
        let command = edit_command(&inputs);
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "edit", None).await?;
        seed_codex_job_project_attribution(
            &database.pool,
            job_id,
            "images.edits",
            EDIT_COMMAND_SCHEMA,
        )
        .await?;

        let mut request =
            edit_attach_request(ticket.clone(), job_id, command.clone(), inputs.clone());
        request.contract = AdmissionContract::CustomerPricingV4;
        request.customer_pricing = Some(customer_pricing_intent());
        let attached = store
            .attach(request.clone())
            .await
            .map_err(|error| format!("v4 edit attach failed: {error:?}"))?;
        let replay = store
            .attach(request)
            .await
            .map_err(|error| format!("v4 edit attach replay failed: {error:?}"))?;
        require(attached == replay, "v4 edit replay changed work identity")?;

        let state: (i64, i64, i64, i64, i64, i64, i64, String) = sqlx::query_as(
            r#"
            SELECT
              (SELECT economics_contract_version FROM jobs WHERE job_id = $1)::BIGINT,
              (SELECT COUNT(*) FROM customer_price_quotes WHERE job_id = $1),
              (SELECT COUNT(*) FROM customer_price_quote_lines WHERE job_id = $1),
              (SELECT COUNT(*) FROM customer_billing_holds WHERE job_id = $1),
              (SELECT COUNT(*) FROM price_quotes WHERE job_id = $1),
              (SELECT COUNT(*) FROM output_holds WHERE job_id = $1),
              (SELECT COUNT(*) FROM job_input_objects WHERE job_id = $1),
              (SELECT operation FROM customer_price_quotes WHERE job_id = $1)
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect v4 edit economics: {error}"))?;
        require(
            state == (4, 1, 3, 1, 0, 0, 2, "edit".to_string()),
            format!("edit did not use the v4 economic path: {state:?}"),
        )?;

        let frozen: (i64, i64, String, String) = sqlx::query_as(
            r#"
            SELECT quote.max_total_micros, hold.held_micros, hold.state,
                   manifest.manifest_hash
            FROM customer_price_quotes quote
            JOIN customer_billing_holds hold ON hold.quote_id = quote.quote_id
            JOIN job_input_manifests manifest ON manifest.job_id = quote.job_id
            WHERE quote.job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect frozen v4 edit quote: {error}"))?;
        require(
            frozen
                == (
                    13,
                    13,
                    "held".to_string(),
                    command.input_manifest_hash_hex(),
                ),
            format!("unexpected frozen v4 edit quote: {frozen:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn codex_snapshot_edit_uses_canonical_price_and_snapshot_command() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_customer_price_v4_for_operation(&database.pool, "edit", 13).await?;
        seed_billing_account(&database.pool, 1000).await?;

        let store = PostgresAdmissionStore::new(database.pool.clone());
        let provisional_inputs = edit_input_specs(Uuid::new_v4(), None);
        let provisional_command =
            edit_command_for_model(&provisional_inputs, "gpt-image-2-2026-04-21");
        let ticket = claim_edit_owner(&store, &provisional_command).await?;
        let inputs = edit_input_specs(ticket.session_id, None);
        let command = edit_command_for_model(&inputs, "gpt-image-2-2026-04-21");
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "edit", None).await?;
        set_job_execution_model(&database.pool, job_id, "gpt-image-2-2026-04-21").await?;
        seed_codex_snapshot_job_project_attribution(
            &database.pool,
            job_id,
            "images.edits",
            EDIT_COMMAND_SCHEMA,
        )
        .await?;

        let mut request = edit_attach_request(ticket, job_id, command, inputs);
        request.contract = AdmissionContract::CustomerPricingV4;
        request.customer_pricing = Some(codex_snapshot_pricing_intent());
        store
            .attach(request)
            .await
            .map_err(|error| format!("snapshot edit admission failed: {error:?}"))?;

        let identity: (String, String, String, String, String, String, i64) = sqlx::query_as(
            r#"
            SELECT job.model, payload.command_json ->> 'model',
                   quote.public_model_id, quote.provider_model_id,
                   version.public_model_id, version.provider_model_id,
                   (SELECT COUNT(*) FROM job_input_objects WHERE job_id = job.job_id)
            FROM jobs job
            JOIN job_payloads payload ON payload.job_id = job.job_id
            JOIN customer_price_quotes quote ON quote.job_id = job.job_id
            JOIN price_book_versions version
              ON version.price_book_version_id = quote.price_book_version_id
            WHERE job.job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect snapshot edit identity: {error}"))?;
        require(
            identity
                == (
                    "gpt-image-2-2026-04-21".to_string(),
                    "gpt-image-2-2026-04-21".to_string(),
                    "gpt-image-2".to_string(),
                    "gpt-image-2".to_string(),
                    "gpt-image-2".to_string(),
                    "gpt-image-2".to_string(),
                    2,
                ),
            format!("snapshot edit identity drifted: {identity:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn grok_semantic_mask_edit_attach_persists_mask_and_work() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let provisional_inputs = grok_semantic_mask_input_specs(Uuid::new_v4());
        let provisional_plan = grok_semantic_mask_plan(&provisional_inputs)?;
        let ticket = claim_grok_edit_owner(&store, provisional_plan.source_request_hash()).await?;
        let inputs = grok_semantic_mask_input_specs(ticket.session_id);
        let plan = grok_semantic_mask_plan(&inputs)?;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "edit", None).await?;
        set_job_provider_model(
            &database.pool,
            job_id,
            image_provider_grok_cli::PROVIDER_ID,
            plan.provider_model(),
        )
        .await?;

        store
            .attach(grok_edit_attach_request(ticket, job_id, &plan, inputs))
            .await
            .map_err(|error| format!("semantic-mask Grok edit attach failed: {error:?}"))?;

        let state: (i64, i64, i64, String, String) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM job_payloads WHERE job_id = $1),
              (SELECT COUNT(*) FROM work_items WHERE job_id = $1),
              (SELECT COUNT(*) FROM job_input_objects WHERE job_id = $1),
              (SELECT command_schema FROM job_payloads WHERE job_id = $1),
              (SELECT string_agg(role || ':' || input_index::TEXT, ','
                         ORDER BY CASE role WHEN 'image' THEN 0 ELSE 1 END, input_index)
                 FROM job_input_objects WHERE job_id = $1)
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect semantic-mask admission: {error}"))?;
        require(
            state
                == (
                    1,
                    1,
                    3,
                    GROK_IMAGE_EDIT_COMMAND_SCHEMA.to_owned(),
                    "image:0,image:1,mask:0".to_owned(),
                ),
            format!("semantic-mask admission lost durable inputs or work: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn grok_non_semantic_edit_attach_rejects_mask_before_writes() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let provisional_inputs = grok_image_input_specs(Uuid::new_v4(), 2);
        let provisional_plan = grok_strict_edit_plan(&provisional_inputs)?;
        let ticket = claim_grok_edit_owner(&store, provisional_plan.source_request_hash()).await?;
        let mut inputs = grok_image_input_specs(ticket.session_id, 2);
        let plan = grok_strict_edit_plan(&inputs)?;
        inputs[1].role = EditInputRoleV1::Mask;
        inputs[1].index = 0;
        let manifest_hash = edit_input_manifest_hash(&inputs);
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "edit", None).await?;
        set_job_provider_model(
            &database.pool,
            job_id,
            image_provider_grok_cli::PROVIDER_ID,
            plan.provider_model(),
        )
        .await?;
        let mut request = grok_edit_attach_request(ticket, job_id, &plan, inputs);
        request
            .input_manifest
            .as_mut()
            .expect("Grok edits always have an input manifest")
            .manifest_hash = manifest_hash;

        require(
            matches!(
                store.attach(request).await,
                Err(AdmissionError::InvalidCommand)
            ),
            "non-semantic Grok edit accepted a mask role",
        )?;
        let state: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM job_payloads WHERE job_id = $1),
              (SELECT COUNT(*) FROM work_items WHERE job_id = $1),
              (SELECT COUNT(*) FROM job_input_objects WHERE job_id = $1)
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect rejected Grok edit: {error}"))?;
        require(
            state == (0, 0, 0),
            format!("rejected Grok edit left durable payload state: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn dreamina_customer_pricing_v4_preserves_ark_identity_and_native_price_alias() -> TestResult
{
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_dreamina_customer_price_v4(&database.pool, 7).await?;
        seed_billing_account(&database.pool, 1000).await?;

        let plan = DreaminaImageAdmissionPlan::new(DreaminaImageGenerationRequest {
            prompt: "Ark request with Dreamina execution".to_string(),
            model_version: Some("5.0".to_string()),
            ratio: Some("16:9".to_string()),
            resolution_type: "2k".to_string(),
            width: None,
            height: None,
            generate_num: Some(1),
        })
        .map_err(|error| format!("failed to create Dreamina admission plan: {error:?}"))?;
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let claim = plan.claim_for_profile(
            ARK_IMAGES_API_PROFILE,
            Uuid::new_v4(),
            "tenant-a",
            "project-a",
            format!("req_{}", Uuid::new_v4().simple()),
            None,
            i64::MAX,
        );
        let ticket = claim_owner(&store, claim).await?;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        sqlx::query(
            r#"
            UPDATE jobs
            SET provider_id = 'dreamina-cli', model = 'dreamina-image-5.0'
            WHERE job_id = $1
            "#,
        )
        .bind(job_id)
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to bind Dreamina job identity: {error}"))?;
        seed_dreamina_job_project_attribution(
            &database.pool,
            job_id,
            ARK_IMAGES_API_PROFILE,
            "doubao-seedream-5-0-lite",
        )
        .await?;

        let mut request = plan.attach(ticket, job_id, "tenant-a");
        request.contract = AdmissionContract::CustomerPricingV4;
        request.customer_pricing = Some(CustomerPricingIntent {
            public_model_id: "doubao-seedream-5-0-lite".to_string(),
            provider_model_id: plan.provider_model_id().to_string(),
            execution_model_id: plan.provider_model().to_string(),
            provider_command_hash: Some(plan.provider_command_hash().to_string()),
            media_kind: "image".to_string(),
            service_tier: "standard".to_string(),
            service_tier_decision:
                gpt_image_2_gateway::service_tiers::ServiceTierDecision::for_default_only_project(
                    gpt_image_2_gateway::service_tiers::ProjectServiceTier::Default,
                ),
            execution_surface: "provider_cli".to_string(),
            currency: "USD".to_string(),
            pricing_dimensions: plan.pricing_dimensions().clone(),
            processing_mode: gpt_image_2_gateway::admission::PricingProcessingMode::Synchronous,
        });
        store
            .attach(request)
            .await
            .map_err(|error| format!("Ark-to-Dreamina v4 attach failed: {error:?}"))?;

        let frozen: (
            String,
            String,
            String,
            String,
            String,
            serde_json::Value,
            i64,
            i64,
        ) = sqlx::query_as(
            r#"
            SELECT quote.api_profile, version.api_profile,
                   quote.provider_model_id, quote.public_model_id,
                   job.model, quote.request_dimensions_json,
                   quote.max_total_micros, hold.held_micros
            FROM customer_price_quotes quote
            JOIN price_book_versions version
              ON version.price_book_version_id = quote.price_book_version_id
            JOIN customer_billing_holds hold ON hold.job_id = quote.job_id
            JOIN jobs job ON job.job_id = quote.job_id
            WHERE quote.job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect Dreamina frozen quote: {error}"))?;
        require(
            frozen
                == (
                    ARK_IMAGES_API_PROFILE.to_string(),
                    DREAMINA_IMAGES_API_PROFILE.to_string(),
                    "5.0".to_string(),
                    "doubao-seedream-5-0-lite".to_string(),
                    "dreamina-image-5.0".to_string(),
                    json!({
                        "processing_mode": "synchronous",
                        "ratio": "16:9",
                        "resolution_type": "2k"
                    }),
                    7,
                    7,
                ),
            format!("Ark identity or Dreamina price source drifted: {frozen:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn dreamina_video_customer_pricing_v4_freezes_ark_identity_and_output_seconds() -> TestResult
{
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_dreamina_video_customer_price_v4(&database.pool, 3).await?;
        seed_billing_account(&database.pool, 1000).await?;

        let plan = DreaminaVideoAdmissionPlan::new(DreaminaVideoGenerationRequest {
            prompt: "Ark Seedance request with Dreamina execution".to_string(),
            model_version: Some("seedance2.0fast".to_string()),
            ratio: Some("9:16".to_string()),
            duration: Some(8),
            video_resolution: "720p".to_string(),
        })
        .map_err(|error| format!("failed to create Dreamina video plan: {error:?}"))?;
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let mut claim = plan.claim_for_profile(
            ARK_CONTENT_GENERATION_API_PROFILE,
            Uuid::new_v4(),
            "tenant-a",
            "project-a",
            format!("req_{}", Uuid::new_v4().simple()),
            None,
            i64::MAX,
        );
        claim.request_hash = "d".repeat(64);
        let ticket = claim_owner(&store, claim).await?;
        let job_id = insert_job_for_ticket(
            &database.pool,
            &ticket,
            "tenant-a",
            VIDEO_GENERATION_OPERATION,
            None,
        )
        .await?;
        configure_dreamina_video_job(&database.pool, job_id, plan.duration()).await?;
        seed_dreamina_video_job_project_attribution(
            &database.pool,
            job_id,
            ARK_CONTENT_GENERATION_API_PROFILE,
            "doubao-seedance-2-0-fast-260128",
        )
        .await?;

        let mut request = plan.attach(ticket, job_id, "tenant-a");
        request.contract = AdmissionContract::CustomerPricingV4;
        request.customer_pricing = Some(CustomerPricingIntent {
            public_model_id: "doubao-seedance-2-0-fast-260128".to_string(),
            provider_model_id: plan.provider_model_id().to_string(),
            execution_model_id: DREAMINA_VIDEO_EXECUTION_MODEL.to_string(),
            provider_command_hash: Some(plan.provider_command_hash().to_string()),
            media_kind: "video".to_string(),
            service_tier: "standard".to_string(),
            service_tier_decision:
                gpt_image_2_gateway::service_tiers::ServiceTierDecision::for_default_only_project(
                    gpt_image_2_gateway::service_tiers::ProjectServiceTier::Default,
                ),
            execution_surface: "provider_cli".to_string(),
            currency: "USD".to_string(),
            pricing_dimensions: plan.pricing_dimensions().clone(),
            processing_mode: gpt_image_2_gateway::admission::PricingProcessingMode::Synchronous,
        });
        store
            .attach(request)
            .await
            .map_err(|error| format!("Ark-to-Dreamina video v4 attach failed: {error:?}"))?;

        let frozen: (
            String,
            String,
            String,
            String,
            String,
            serde_json::Value,
            i64,
            i64,
            i64,
            String,
            String,
        ) = sqlx::query_as(
            r#"
            SELECT quote.api_profile, version.api_profile,
                   quote.provider_model_id, quote.public_model_id,
                   job.model, quote.request_dimensions_json,
                   output.billable_units::BIGINT,
                   quote.max_total_micros, hold.held_micros,
                   job.billing_metric, job.billing_unit
            FROM customer_price_quotes quote
            JOIN price_book_versions version
              ON version.price_book_version_id = quote.price_book_version_id
            JOIN customer_billing_holds hold ON hold.job_id = quote.job_id
            JOIN jobs job ON job.job_id = quote.job_id
            JOIN job_outputs output ON output.job_id = quote.job_id
            WHERE quote.job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect Dreamina video quote: {error}"))?;
        require(
            frozen
                == (
                    ARK_CONTENT_GENERATION_API_PROFILE.to_string(),
                    DREAMINA_VIDEOS_API_PROFILE.to_string(),
                    "seedance2.0fast".to_string(),
                    "doubao-seedance-2-0-fast-260128".to_string(),
                    DREAMINA_VIDEO_EXECUTION_MODEL.to_string(),
                    json!({
                        "duration": "8",
                        "processing_mode": "synchronous",
                        "ratio": "9:16",
                        "resolution": "720p"
                    }),
                    8,
                    24,
                    24,
                    "video_second".to_string(),
                    "second".to_string(),
                ),
            format!("Ark video identity or frozen seconds drifted: {frozen:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn customer_pricing_v4_quotes_each_requested_output_partition() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_customer_price_v4(&database.pool, 11).await?;
        seed_billing_account(&database.pool, 1000).await?;

        let store = PostgresAdmissionStore::new(database.pool.clone());
        let (ticket, command_json) = claim_customer_pricing_owner(&store, 3).await?;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        sqlx::query(
            r#"
            UPDATE jobs
            SET requested_units = 3, output_count = 3, billable_units = 3
            WHERE job_id = $1
            "#,
        )
        .bind(job_id)
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to configure multi-output v4 job: {error}"))?;
        sqlx::query(
            r#"
            UPDATE quota_reservations
            SET requested_units = 3, remaining_5h = 97, remaining_7d = 97
            WHERE job_id = $1
            "#,
        )
        .bind(job_id)
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to configure multi-output quota: {error}"))?;
        seed_job_project_attribution(&database.pool, job_id).await?;
        let mut request = attach_request(ticket, job_id);
        request.command_json = command_json;
        request.schedule_cost = 3;
        request.contract = AdmissionContract::CustomerPricingV4;
        request.customer_pricing = Some(customer_pricing_intent());

        let first = store
            .attach(request.clone())
            .await
            .map_err(|error| format!("multi-output v4 attach failed: {error:?}"))?;
        let replay = store
            .attach(request)
            .await
            .map_err(|error| format!("multi-output v4 replay failed: {error:?}"))?;
        require(
            first == replay,
            "multi-output v4 replay changed work identity",
        )?;

        let state: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM job_outputs WHERE job_id = $1),
              (SELECT COUNT(DISTINCT partition_key)
                 FROM customer_price_quote_lines WHERE job_id = $1),
              (SELECT COUNT(*) FROM customer_price_quote_lines WHERE job_id = $1),
              (SELECT max_total_micros FROM customer_price_quotes WHERE job_id = $1),
              (SELECT held_micros FROM customer_billing_holds WHERE job_id = $1),
              (SELECT held_micros FROM billing_accounts
                 WHERE tenant_id = 'tenant-a' AND currency = 'USD')
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect multi-output v4 quote: {error}"))?;
        require(
            state == (3, 3, 9, 33, 33, 33),
            format!("multi-output v4 quote lost partition economics: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn customer_pricing_v4_rejects_legacy_estimated_token_prices_without_partial_state()
-> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_customer_token_price_v4(&database.pool).await?;
        seed_billing_account(&database.pool, 1_000_000).await?;

        let store = PostgresAdmissionStore::new(database.pool.clone());
        let (ticket, command_json) = claim_customer_pricing_owner(&store, 2).await?;
        let owner_token = ticket.owner_token;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        sqlx::query(
            r#"
            UPDATE jobs
            SET requested_units = 2, output_count = 2, billable_units = 2
            WHERE job_id = $1
            "#,
        )
        .bind(job_id)
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to configure token-priced v4 job: {error}"))?;
        sqlx::query(
            r#"
            UPDATE quota_reservations
            SET requested_units = 2, remaining_5h = 98, remaining_7d = 98
            WHERE job_id = $1
            "#,
        )
        .bind(job_id)
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to configure token-priced quota: {error}"))?;
        seed_job_project_attribution(&database.pool, job_id).await?;
        let mut request = attach_request(ticket, job_id);
        request.command_json = command_json;
        request.schedule_cost = 2;
        request.contract = AdmissionContract::CustomerPricingV4;
        request.customer_pricing = Some(customer_pricing_intent());

        require(
            matches!(
                store.attach(request).await,
                Err(AdmissionError::PricingUnavailable)
            ),
            "v4 admission accepted a legacy estimated token price",
        )?;
        let state: (String, i64, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT session.state,
                   (SELECT economics_contract_version FROM jobs WHERE job_id = $1)::BIGINT,
                   (SELECT COUNT(*) FROM job_payloads WHERE job_id = $1),
                   (SELECT COUNT(*) FROM job_outputs WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_price_quotes WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_price_quote_lines WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_billing_holds WHERE job_id = $1),
                   (SELECT COUNT(*) FROM work_items WHERE job_id = $1),
                   (SELECT held_micros FROM billing_accounts
                      WHERE tenant_id = 'tenant-a' AND currency = 'USD')
            FROM admission_sessions session
            WHERE session.owner_token = $2
            "#,
        )
        .bind(job_id)
        .bind(owner_token)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| {
            format!("failed to inspect rejected token-priced v4 admission: {error}")
        })?;
        require(
            state == ("receiving".to_string(), 1, 0, 0, 0, 0, 0, 0, 0),
            format!("rejected token-priced v4 admission left partial state: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn customer_pricing_v4_rejects_forged_dimensions_without_partial_state() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_customer_token_price_v4(&database.pool).await?;
        seed_billing_account(&database.pool, 1_000_000).await?;

        let store = PostgresAdmissionStore::new(database.pool.clone());
        let (ticket, command_json) = claim_customer_pricing_owner(&store, 1).await?;
        let owner_token = ticket.owner_token;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        seed_job_project_attribution(&database.pool, job_id).await?;
        let mut request = attach_request(ticket, job_id);
        request.command_json = command_json;
        request.contract = AdmissionContract::CustomerPricingV4;
        let mut pricing = customer_pricing_intent();
        pricing
            .pricing_dimensions
            .insert("quality".to_string(), "low".to_string());
        request.customer_pricing = Some(pricing);

        require(
            matches!(
                store.attach(request).await,
                Err(AdmissionError::PricingUnavailable)
            ),
            "v4 admission priced a signed high-quality command as low quality",
        )?;
        let state: (String, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT session.state,
                   (SELECT economics_contract_version FROM jobs WHERE job_id = $1)::BIGINT,
                   (SELECT COUNT(*) FROM job_payloads WHERE job_id = $1),
                   (SELECT COUNT(*) FROM job_outputs WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_price_quotes WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_price_quote_lines WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_billing_holds WHERE job_id = $1),
                   (SELECT COUNT(*) FROM work_items WHERE job_id = $1)
            FROM admission_sessions session
            WHERE session.owner_token = $2
            "#,
        )
        .bind(job_id)
        .bind(owner_token)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect forged pricing dimensions: {error}"))?;
        require(
            state == ("receiving".to_string(), 1, 0, 0, 0, 0, 0, 0),
            format!("forged pricing dimensions left partial state: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn customer_pricing_v4_rejects_forged_execution_model_without_partial_state() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_customer_price_v4(&database.pool, 11).await?;
        seed_billing_account(&database.pool, 1000).await?;

        let store = PostgresAdmissionStore::new(database.pool.clone());
        let (ticket, command_json) = claim_customer_pricing_owner(&store, 1).await?;
        let owner_token = ticket.owner_token;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        seed_job_project_attribution(&database.pool, job_id).await?;
        let mut request = attach_request(ticket, job_id);
        request.command_json = command_json;
        request.contract = AdmissionContract::CustomerPricingV4;
        let mut pricing = customer_pricing_intent();
        pricing.execution_model_id = "forged-execution-model".to_string();
        request.customer_pricing = Some(pricing);

        require(
            matches!(
                store.attach(request).await,
                Err(AdmissionError::PricingUnavailable)
            ),
            "v4 admission accepted an execution model outside the frozen route mapping",
        )?;
        let state: (String, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT session.state,
                   (SELECT economics_contract_version FROM jobs WHERE job_id = $1)::BIGINT,
                   (SELECT COUNT(*) FROM job_payloads WHERE job_id = $1),
                   (SELECT COUNT(*) FROM job_outputs WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_price_quotes WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_price_quote_lines WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_billing_holds WHERE job_id = $1),
                   (SELECT COUNT(*) FROM work_items WHERE job_id = $1)
            FROM admission_sessions session
            WHERE session.owner_token = $2
            "#,
        )
        .bind(job_id)
        .bind(owner_token)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect forged v4 admission: {error}"))?;
        require(
            state == ("receiving".to_string(), 1, 0, 0, 0, 0, 0, 0),
            format!("forged v4 admission left partial state: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn customer_pricing_v4_rejects_a_signed_provider_model_mismatch_without_partial_state()
-> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_customer_price_v4(&database.pool, 11).await?;
        seed_billing_account(&database.pool, 1000).await?;

        let store = PostgresAdmissionStore::new(database.pool.clone());
        let (ticket, command_json) = claim_customer_pricing_owner_with_identity(
            &store,
            1,
            "openai-codex",
            "gpt-image-2-2026-04-21",
            "openai-images-v1",
        )
        .await?;
        let owner_token = ticket.owner_token;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        seed_job_project_attribution(&database.pool, job_id).await?;
        let mut request = attach_request(ticket, job_id);
        request.command_json = command_json;
        request.contract = AdmissionContract::CustomerPricingV4;
        request.customer_pricing = Some(customer_pricing_intent());

        require(
            matches!(
                store.attach(request).await,
                Err(AdmissionError::PricingUnavailable)
            ),
            "v4 admission accepted a signed provider model outside the frozen job and price",
        )?;
        let state: (String, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT session.state,
                   (SELECT economics_contract_version FROM jobs WHERE job_id = $1)::BIGINT,
                   (SELECT COUNT(*) FROM job_payloads WHERE job_id = $1),
                   (SELECT COUNT(*) FROM job_outputs WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_price_quotes WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_price_quote_lines WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_billing_holds WHERE job_id = $1),
                   (SELECT COUNT(*) FROM work_items WHERE job_id = $1)
            FROM admission_sessions session
            WHERE session.owner_token = $2
            "#,
        )
        .bind(job_id)
        .bind(owner_token)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect forged signed model: {error}"))?;
        require(
            state == ("receiving".to_string(), 1, 0, 0, 0, 0, 0, 0),
            format!("forged signed provider model left partial state: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn customer_pricing_v4_concurrent_replay_creates_one_economic_identity() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_customer_price_v4(&database.pool, 11).await?;
        seed_billing_account(&database.pool, 1000).await?;

        let store = PostgresAdmissionStore::new(database.pool.clone());
        let (ticket, command_json) = claim_customer_pricing_owner(&store, 1).await?;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        seed_job_project_attribution(&database.pool, job_id).await?;
        let mut request = attach_request(ticket, job_id);
        request.command_json = command_json;
        request.contract = AdmissionContract::CustomerPricingV4;
        request.customer_pricing = Some(customer_pricing_intent());

        let first_store = store.clone();
        let first_request = request.clone();
        let first = tokio::spawn(async move { first_store.attach(first_request).await });
        let second = tokio::spawn(async move { store.attach(request).await });
        let first = first
            .await
            .map_err(|error| format!("first concurrent v4 attach task failed: {error}"))?
            .map_err(|error| format!("first concurrent v4 attach failed: {error:?}"))?;
        let second = second
            .await
            .map_err(|error| format!("second concurrent v4 attach task failed: {error}"))?
            .map_err(|error| format!("second concurrent v4 attach failed: {error:?}"))?;
        require(
            first == second,
            "concurrent replay changed the work identity",
        )?;

        let state: (i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM job_payloads WHERE job_id = $1),
              (SELECT COUNT(*) FROM job_outputs WHERE job_id = $1),
              (SELECT COUNT(*) FROM customer_price_quotes WHERE job_id = $1),
              (SELECT COUNT(*) FROM customer_price_quote_lines WHERE job_id = $1),
              (SELECT COUNT(*) FROM customer_billing_holds WHERE job_id = $1),
              (SELECT COUNT(*) FROM work_items WHERE job_id = $1),
              (SELECT held_micros FROM billing_accounts
                WHERE tenant_id = 'tenant-a' AND currency = 'USD')
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect concurrent v4 admission: {error}"))?;
        require(
            state == (1, 1, 1, 3, 1, 1, 11),
            format!("concurrent v4 admission duplicated economics: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn customer_pricing_v4_insufficient_credit_rolls_back_every_accept_effect() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_customer_price_v4(&database.pool, 11).await?;
        seed_billing_account(&database.pool, 5).await?;
        let now: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to read database time: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO identity_organizations (
                organization_id, display_name, organization_kind,
                owner_user_id, created_at_ms, updated_at_ms
            )
            VALUES ('tenant-a', 'Admission tenant', 'system', NULL, $1, $1)
            "#,
        )
        .bind(now)
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to seed grant organization: {error}"))?;
        PostgresCreditGrantService::new(database.pool.clone())
            .create(
                "admission-insufficient-grant",
                CreditGrantActor {
                    user_id: Uuid::new_v4(),
                    session_id: Uuid::new_v4(),
                },
                CreateCreditGrantRequest {
                    organization_id: "tenant-a".to_string(),
                    currency: "USD".to_string(),
                    amount_micros: "5".to_string(),
                    expires_at_ms: now + 86_400_000,
                    source_reference: "admission-insufficient".to_string(),
                    reason: "Admission rollback test".to_string(),
                },
            )
            .await
            .map_err(|error| format!("failed to issue test grant: {error:?}"))?;

        let store = PostgresAdmissionStore::new(database.pool.clone());
        let (ticket, command_json) = claim_customer_pricing_owner(&store, 1).await?;
        let owner_token = ticket.owner_token;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        seed_job_project_attribution(&database.pool, job_id).await?;
        let mut request = attach_request(ticket, job_id);
        request.command_json = command_json;
        request.contract = AdmissionContract::CustomerPricingV4;
        request.customer_pricing = Some(customer_pricing_intent());

        require(
            matches!(
                store.attach(request).await,
                Err(AdmissionError::BillingLimitExceeded)
            ),
            "v4 accept ignored the billing credit limit",
        )?;
        let state: (String, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT session.state,
                   (SELECT economics_contract_version FROM jobs WHERE job_id = $1)::BIGINT,
                   (SELECT COUNT(*) FROM job_outputs WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_price_quotes WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_price_quote_lines WHERE job_id = $1),
                   (SELECT COUNT(*) FROM customer_billing_holds WHERE job_id = $1),
                   (SELECT COUNT(*) FROM work_items WHERE job_id = $1),
                   (SELECT held_micros FROM billing_accounts
                     WHERE tenant_id = 'tenant-a' AND currency = 'USD'),
                   (SELECT available_micros FROM credit_grants
                     WHERE tenant_id = 'tenant-a' AND currency = 'USD'),
                   (SELECT COUNT(*) FROM customer_billing_hold_grant_reservations
                     WHERE tenant_id = 'tenant-a' AND currency = 'USD'),
                   (SELECT COUNT(*) FROM credit_grant_events
                     WHERE tenant_id = 'tenant-a' AND currency = 'USD'
                       AND event_type = 'reserved')
            FROM admission_sessions session
            WHERE session.owner_token = $2
            "#,
        )
        .bind(job_id)
        .bind(owner_token)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect rejected v4 admission: {error}"))?;
        require(
            state == ("receiving".to_string(), 1, 0, 0, 0, 0, 0, 0, 5, 0, 0),
            format!("rejected v4 admission left partial state: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn ark_profile_uses_the_dreamina_pricing_alias_without_losing_its_api_identity() -> TestResult
{
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let price_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO price_versions
              (price_version_id, price_key, version, api_profile, operation, provider_id, model,
               billing_metric, billing_unit, currency,
               success_micros, failed_micros, no_effect_micros, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, 'dreamina-alias-test', 1, 'dreamina-cli-images-v1', 'generation',
                    'openai-codex', 'gpt-image-2', 'output', 'output', 'USD',
                    7, 0, 0, 'active', 1, 1)
            "#,
        )
        .bind(price_id)
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to seed aliased price: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO billing_accounts
              (tenant_id, currency, credit_limit_micros, held_micros, captured_micros,
               created_at_ms, updated_at_ms)
            VALUES ('tenant-a', 'USD', 1000, 0, 0, 1, 1)
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to seed billing account: {error}"))?;

        let store = PostgresAdmissionStore::new(database.pool.clone());
        let mut claim = claim_request(None, "a".repeat(64));
        claim.api_profile = "volcengine-ark-images-v3".to_owned();
        let ticket = claim_owner(&store, claim).await?;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        let mut request = attach_request(ticket, job_id);
        request.contract = AdmissionContract::OutputEconomicsV2;
        store
            .attach(request)
            .await
            .map_err(|error| format!("Ark attach failed: {error:?}"))?;

        let frozen: (Uuid, String, i64) = sqlx::query_as(
            r#"
            SELECT q.price_version_id, s.api_profile, q.success_micros
            FROM price_quotes q
            JOIN admission_sessions s ON s.job_id = q.job_id
            WHERE q.job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect aliased quote: {error}"))?;
        require(
            frozen == (price_id, "volcengine-ark-images-v3".to_owned(), 7),
            format!("Ark pricing alias or API identity drifted: {frozen:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn ark_specific_price_takes_precedence_over_a_more_specific_alias_price() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let alias_price_id = Uuid::new_v4();
        let ark_price_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO price_versions
              (price_version_id, price_key, version, api_profile, operation, provider_id, model,
               billing_metric, billing_unit, currency,
               success_micros, failed_micros, no_effect_micros, state,
               created_at_ms, updated_at_ms)
            VALUES
              ($1, 'dreamina-specific-alias-test', 1, 'dreamina-cli-images-v1', 'generation',
               'openai-codex', 'gpt-image-2', 'output', 'output', 'USD',
               7, 0, 0, 'active', 1, 1),
              ($2, 'ark-profile-override-test', 1, 'volcengine-ark-images-v3', '*',
               '*', '*', 'output', 'output', 'USD',
               11, 0, 0, 'active', 1, 1)
            "#,
        )
        .bind(alias_price_id)
        .bind(ark_price_id)
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to seed profile precedence prices: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO billing_accounts
              (tenant_id, currency, credit_limit_micros, held_micros, captured_micros,
               created_at_ms, updated_at_ms)
            VALUES ('tenant-a', 'USD', 1000, 0, 0, 1, 1)
            "#,
        )
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to seed billing account: {error}"))?;

        let store = PostgresAdmissionStore::new(database.pool.clone());
        let mut claim = claim_request(None, "b".repeat(64));
        claim.api_profile = "volcengine-ark-images-v3".to_owned();
        let ticket = claim_owner(&store, claim).await?;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        let mut request = attach_request(ticket, job_id);
        request.contract = AdmissionContract::OutputEconomicsV2;
        store
            .attach(request)
            .await
            .map_err(|error| format!("Ark attach failed: {error:?}"))?;

        let frozen: (Uuid, i64) = sqlx::query_as(
            "SELECT price_version_id, success_micros FROM price_quotes WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect Ark-specific quote: {error}"))?;
        require(
            frozen == (ark_price_id, 11),
            format!("Ark-specific price did not take precedence: {frozen:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn ready_claims_are_isolated_by_economics_contract() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());

        let legacy_ticket = claim_owner(&store, claim_request(None, "1".repeat(64))).await?;
        let legacy_job = insert_job_for_ticket(
            &database.pool,
            &legacy_ticket,
            "tenant-a",
            "generation",
            None,
        )
        .await?;
        store
            .attach(attach_request(legacy_ticket, legacy_job))
            .await
            .map_err(|error| format!("legacy attach failed: {error:?}"))?;

        let v2_ticket = claim_owner(&store, claim_request(None, "2".repeat(64))).await?;
        let v2_job =
            insert_job_for_ticket(&database.pool, &v2_ticket, "tenant-a", "generation", None)
                .await?;
        let mut v2_request = attach_request(v2_ticket, v2_job);
        v2_request.contract = AdmissionContract::OutputEconomicsV2;
        store
            .attach(v2_request)
            .await
            .map_err(|error| format!("V2 attach failed: {error:?}"))?;

        let v4_ticket = claim_owner(&store, claim_request(None, "6".repeat(64))).await?;
        let v4_job =
            insert_job_for_ticket(&database.pool, &v4_ticket, "tenant-a", "generation", None)
                .await?;
        store
            .attach(attach_request(v4_ticket, v4_job))
            .await
            .map_err(|error| format!("v4 dispatcher fixture attach failed: {error:?}"))?;
        sqlx::query(
            "UPDATE jobs SET economics_contract_version = 4, updated_at_ms = 2 WHERE job_id = $1",
        )
        .bind(v4_job)
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to mark v4 dispatcher fixture: {error}"))?;

        let v2_lease = store
            .claim_ready(
                "executor-handoff-worker",
                30_000,
                AdmissionContract::OutputEconomicsV2,
            )
            .await
            .map_err(|error| format!("V2 claim failed: {error:?}"))?
            .ok_or_else(|| "V2 worker did not find V2 work".to_string())?;
        require(
            v2_lease.job_id == v2_job,
            "V2 worker claimed the older LegacyV1 job",
        )?;

        let v4_lease = store
            .claim_ready(
                "customer-pricing-worker",
                30_000,
                AdmissionContract::CustomerPricingV4,
            )
            .await
            .map_err(|error| format!("v4 claim failed: {error:?}"))?
            .ok_or_else(|| "v4 worker did not find v4 work".to_string())?;
        require(
            v4_lease.job_id == v4_job,
            "v4 worker claimed another economics contract",
        )?;

        let legacy_lease = store
            .claim_ready("legacy-worker", 30_000, AdmissionContract::LegacyV1)
            .await
            .map_err(|error| format!("legacy claim failed: {error:?}"))?
            .ok_or_else(|| "legacy worker did not find LegacyV1 work".to_string())?;
        require(
            legacy_lease.job_id == legacy_job,
            "LegacyV1 worker claimed V2 work or the legacy job was starved",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn ready_claims_are_isolated_by_provider_command_schema() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());

        let codex_ticket = claim_owner(&store, claim_request(None, "3".repeat(64))).await?;
        let codex_job = insert_job_for_ticket(
            &database.pool,
            &codex_ticket,
            "tenant-a",
            "generation",
            None,
        )
        .await?;
        store
            .attach(attach_request(codex_ticket, codex_job))
            .await
            .map_err(|error| format!("Codex attach failed: {error:?}"))?;

        let plan = XaiImageAdmissionPlan::for_grok_cli(XaiImageGenerationRequest {
            aspect_ratio: None,
            model: Some("grok-imagine-image-quality".to_owned()),
            n: Some(1),
            prompt: "a lighthouse".to_owned(),
            resolution: Some(XaiImageResolution::R1k),
            response_format: Some(XaiImageResponseFormat::B64Json),
            storage_options: None,
            user: None,
        })
        .map_err(|error| format!("xAI plan failed: {error}"))?;
        let grok_ticket = claim_owner(
            &store,
            plan.claim(
                Uuid::new_v4(),
                "tenant-a",
                "project-a",
                format!("req_{}", Uuid::new_v4().simple()),
                None,
                i64::MAX,
            ),
        )
        .await?;
        let grok_job =
            insert_job_for_ticket(&database.pool, &grok_ticket, "tenant-a", "generation", None)
                .await?;
        store
            .attach(plan.attach(
                grok_ticket,
                grok_job,
                "tenant:tenant-a",
                AdmissionContract::LegacyV1,
            ))
            .await
            .map_err(|error| format!("Grok attach failed: {error:?}"))?;

        let grok_lease = store
            .claim_ready_for_schema(
                "grok-worker",
                30_000,
                AdmissionContract::LegacyV1,
                GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
            )
            .await
            .map_err(|error| format!("Grok claim failed: {error:?}"))?
            .ok_or_else(|| "Grok worker did not find Grok work".to_owned())?;
        require(
            grok_lease.job_id == grok_job,
            "Grok worker claimed Codex work",
        )?;

        let codex_lease = store
            .claim_ready_for_schema(
                "codex-worker",
                30_000,
                AdmissionContract::LegacyV1,
                GENERATION_COMMAND_SCHEMA,
            )
            .await
            .map_err(|error| format!("Codex claim failed: {error:?}"))?
            .ok_or_else(|| "Codex worker did not find Codex work".to_owned())?;
        require(
            codex_lease.job_id == codex_job,
            "Codex worker claimed Grok work",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn insufficient_billing_credit_rolls_back_the_entire_accept() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        sqlx::query(
            r#"
            INSERT INTO price_versions
              (price_version_id, price_key, version, api_profile, operation, provider_id, model,
               currency, success_micros, failed_micros, no_effect_micros, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, 'paid-test', 1, 'openai-images-v1', 'generation',
                    'openai-codex', 'gpt-image-2', 'USD', 11, 0, 0, 'active', 1, 1)
            "#,
        )
        .bind(Uuid::new_v4())
        .execute(&database.pool)
        .await
        .map_err(|error| format!("failed to seed paid price: {error}"))?;
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let ticket = claim_owner(&store, claim_request(None, "8".repeat(64))).await?;
        let owner_token = ticket.owner_token;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        let mut request = attach_request(ticket, job_id);
        request.contract = AdmissionContract::OutputEconomicsV2;
        require(
            matches!(
                store.attach(request).await,
                Err(AdmissionError::BillingLimitExceeded)
            ),
            "accept ignored the tenant billing limit",
        )?;
        let state: (String, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT s.state,
                   (SELECT COUNT(*) FROM job_outputs WHERE job_id = $1),
                   (SELECT COUNT(*) FROM price_quotes WHERE job_id = $1),
                   (SELECT COUNT(*) FROM output_holds h
                      JOIN job_outputs o ON o.output_id = h.output_id WHERE o.job_id = $1),
                   (SELECT COUNT(*) FROM billing_accounts WHERE tenant_id = 'tenant-a')
            FROM admission_sessions s
            WHERE s.job_id IS NULL AND s.owner_token = $2
            "#,
        )
        .bind(job_id)
        .bind(owner_token)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect rejected accept: {error}"))?;
        require(
            state == ("receiving".to_string(), 0, 0, 0, 0),
            format!("rejected accept left partial economics: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_same_key_claims_have_one_owner_and_conflicting_hash_is_rejected() -> TestResult
{
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let request = claim_request(Some("a".repeat(64)), "b".repeat(64));
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let store = store.clone();
            let mut request = request.clone();
            request.owner_token = Uuid::new_v4();
            tasks.push(tokio::spawn(async move { store.claim(request).await }));
        }
        let mut owners = 0;
        let mut in_progress = 0;
        for task in tasks {
            match task.await.map_err(|error| format!("claim task failed: {error}"))?
                .map_err(|error| format!("claim failed: {error}"))?
            {
                AdmissionClaim::Owner(_) => owners += 1,
                AdmissionClaim::InProgress { .. } => in_progress += 1,
                other => return Err(format!("unexpected concurrent outcome: {other:?}")),
            }
        }
        require(owners == 1, format!("expected one owner, got {owners}"))?;
        require(
            in_progress == 99,
            format!("expected 99 challengers, got {in_progress}"),
        )?;

        let conflict = store
            .claim(claim_request(Some("a".repeat(64)), "c".repeat(64)))
            .await
            .map_err(|error| format!("conflict claim failed: {error}"))?;
        require(
            matches!(conflict, AdmissionClaim::Conflict { job_id: None }),
            format!("different hash was not rejected: {conflict:?}"),
        )?;
        let counts: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM admission_sessions), (SELECT COUNT(*) FROM idempotency_requests)",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to count identities: {error}"))?;
        require(counts == (1, 1), format!("unexpected identity counts: {counts:?}"))
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn same_attempt_token_recovers_receiving_owner_after_unknown_commit() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let request = claim_request(Some("e".repeat(64)), "f".repeat(64));
        let first = store
            .claim(request.clone())
            .await
            .map_err(|error| format!("initial claim failed: {error}"))?;
        let replay = store
            .claim(request.clone())
            .await
            .map_err(|error| format!("owner recovery failed: {error}"))?;
        require(
            replay == first,
            format!("same attempt did not recover owner: {first:?} != {replay:?}"),
        )?;

        let mut challenger = request;
        challenger.owner_token = Uuid::new_v4();
        let challenged = store
            .claim(challenger)
            .await
            .map_err(|error| format!("challenger claim failed: {error}"))?;
        require(
            matches!(challenged, AdmissionClaim::InProgress { .. }),
            format!("different attempt stole receiving owner: {challenged:?}"),
        )?;

        let unkeyed = claim_request(None, "1".repeat(64));
        let unkeyed_first = store
            .claim(unkeyed.clone())
            .await
            .map_err(|error| format!("initial unkeyed claim failed: {error}"))?;
        let unkeyed_replay = store
            .claim(unkeyed)
            .await
            .map_err(|error| format!("unkeyed owner recovery failed: {error}"))?;
        require(
            unkeyed_replay == unkeyed_first,
            format!(
                "unkeyed attempt did not recover owner: {unkeyed_first:?} != {unkeyed_replay:?}"
            ),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn no_key_claims_are_independent() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        for _ in 0..2 {
            let outcome = store
                .claim(claim_request(None, "d".repeat(64)))
                .await
                .map_err(|error| format!("unkeyed claim failed: {error}"))?;
            require(
                matches!(outcome, AdmissionClaim::Owner(_)),
                "unkeyed claim did not create an owner",
            )?;
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM admission_sessions")
            .fetch_one(&database.pool)
            .await
            .map_err(|error| format!("failed to count sessions: {error}"))?;
        require(count == 2, format!("expected two sessions, got {count}"))
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn expired_new_claim_is_rejected_without_persisting_admission() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let mut request = claim_request(Some("4".repeat(64)), "1".repeat(64));
        request.deadline_at_ms = 0;

        require(
            matches!(store.claim(request).await, Err(AdmissionError::Expired)),
            "expired claim was accepted",
        )?;
        let counts: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM admission_sessions), (SELECT COUNT(*) FROM idempotency_requests)",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to count expired admission rows: {error}"))?;
        require(
            counts == (0, 0),
            format!("expired claim persisted rows: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn owner_expiring_before_attach_is_aborted() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let ticket = match store
            .claim(claim_request(Some("3".repeat(64)), "2".repeat(64)))
            .await
            .map_err(|error| format!("owner claim failed: {error}"))?
        {
            AdmissionClaim::Owner(ticket) => ticket,
            other => return Err(format!("expected owner, got {other:?}")),
        };
        sqlx::query("UPDATE admission_sessions SET deadline_at_ms = 0 WHERE session_id = $1")
            .bind(ticket.session_id)
            .execute(&database.pool)
            .await
            .map_err(|error| format!("failed to expire admission: {error}"))?;
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;

        require(
            matches!(
                store.attach(attach_request(ticket.clone(), job_id)).await,
                Err(AdmissionError::Expired)
            ),
            "expired owner attached a job",
        )?;
        let states: (String, String) = sqlx::query_as(
            r#"
            SELECT admission_sessions.state, idempotency_requests.state
            FROM admission_sessions
            JOIN idempotency_requests USING (session_id)
            WHERE admission_sessions.session_id = $1
            "#,
        )
        .bind(ticket.session_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to read expired admission state: {error}"))?;
        require(
            states == ("aborted".to_string(), "aborted".to_string()),
            format!("expired admission was not aborted: {states:?}"),
        )?;

        let retry = store
            .claim(claim_request(Some("3".repeat(64)), "2".repeat(64)))
            .await
            .map_err(|error| format!("same-hash aborted retry failed: {error}"))?;
        let AdmissionClaim::Owner(retry_ticket) = retry else {
            return Err(format!("aborted retry did not regain ownership: {retry:?}"));
        };
        require(
            retry_ticket.session_id != ticket.session_id,
            "aborted retry reused the old session",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn owner_attachment_and_lease_epoch_are_fenced() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = attachment_and_fencing_case(&database).await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn claim_job_leases_only_the_requested_ready_work() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let mut jobs = Vec::new();
        for digest in ["5".repeat(64), "6".repeat(64)] {
            let ticket = match store
                .claim(claim_request(Some(digest), "7".repeat(64)))
                .await
                .map_err(|error| format!("owner claim failed: {error}"))?
            {
                AdmissionClaim::Owner(ticket) => ticket,
                other => return Err(format!("expected owner, got {other:?}")),
            };
            let job_id =
                insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None)
                    .await?;
            store
                .attach(attach_request(ticket, job_id))
                .await
                .map_err(|error| format!("attach failed: {error}"))?;
            jobs.push(job_id);
        }

        let lease = store
            .claim_job(jobs[1], "inline-gateway", 30_000)
            .await
            .map_err(|error| format!("targeted claim failed: {error}"))?
            .ok_or_else(|| "targeted work was not ready".to_string())?;
        require(lease.job_id == jobs[1], "targeted claim leased another job")?;
        let states: Vec<(Uuid, String)> =
            sqlx::query_as("SELECT job_id, state FROM work_items ORDER BY job_id")
                .fetch_all(&database.pool)
                .await
                .map_err(|error| format!("failed to inspect targeted claim: {error}"))?;
        require(
            states
                .iter()
                .any(|(job_id, state)| *job_id == jobs[0] && state == "ready"),
            "non-targeted work did not remain ready",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn durable_claim_uses_weighted_finish_tags_and_waiting_aging() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        for _ in 0..4 {
            for (tenant, weight) in [("tenant-a", 1_u32), ("tenant-b", 2_u32)] {
                let ticket = match store
                    .claim(claim_request_for_tenant(tenant, None, &"a".repeat(64)))
                    .await
                    .map_err(|error| format!("weighted claim failed: {error}"))?
                {
                    AdmissionClaim::Owner(ticket) => ticket,
                    other => return Err(format!("expected weighted owner, got {other:?}")),
                };
                let job_id = insert_job_for_ticket(
                    &database.pool,
                    &ticket,
                    tenant,
                    "generation",
                    None,
                )
                .await?;
                store
                    .attach(attach_request_with_schedule(
                        ticket,
                        job_id,
                        tenant,
                        weight,
                        1,
                    ))
                    .await
                    .map_err(|error| format!("weighted attach failed: {error}"))?;
            }
        }

        let mut first_four = Vec::new();
        for _ in 0..4 {
            let lease = store
                .claim_ready("fair-worker", 30_000, AdmissionContract::LegacyV1)
                .await
                .map_err(|error| format!("weighted ready claim failed: {error}"))?
                .ok_or_else(|| "weighted queue unexpectedly empty".to_string())?;
            let tenant: String = sqlx::query_scalar(
                "SELECT tenant_id FROM jobs WHERE job_id = $1",
            )
            .bind(lease.job_id)
            .fetch_one(&database.pool)
            .await
            .map_err(|error| format!("failed to inspect weighted job: {error}"))?;
            first_four.push(tenant);
        }
        require(
            first_four.iter().filter(|tenant| *tenant == "tenant-b").count() >= 3,
            format!("weighted scope was not favored: {first_four:?}"),
        )?;

        let tags: Vec<(String, i64)> = sqlx::query_as(
            "SELECT schedule_scope, schedule_finish_tag FROM work_items ORDER BY schedule_scope, schedule_finish_tag",
        )
        .fetch_all(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect durable schedule tags: {error}"))?;
        require(
            tags.iter()
                .filter(|(scope, _)| scope == "tenant-b")
                .map(|(_, tag)| *tag)
                .collect::<Vec<_>>()
                == vec![500_000, 1_000_000, 1_500_000, 2_000_000],
            format!("unexpected heavy scope tags: {tags:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn attach_and_start_is_atomic_and_idempotent_for_the_owner() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let ticket = match store
            .claim(claim_request(Some("8".repeat(64)), "9".repeat(64)))
            .await
            .map_err(|error| format!("owner claim failed: {error}"))?
        {
            AdmissionClaim::Owner(ticket) => ticket,
            other => return Err(format!("expected owner, got {other:?}")),
        };
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
        let request = attach_request(ticket, job_id);

        let first = store
            .attach_and_start(request.clone(), "inline-gateway", 30_000)
            .await
            .map_err(|error| format!("atomic attach/start failed: {error}"))?;
        let replay = store
            .attach_and_start(request, "inline-gateway", 30_000)
            .await
            .map_err(|error| format!("idempotent attach/start replay failed: {error}"))?;
        require(
            first == replay,
            "attach/start replay returned a different lease",
        )?;

        let state: (String, String, i64, i64) = sqlx::query_as(
            r#"
            SELECT a.state, w.state,
                   (SELECT COUNT(*) FROM work_items WHERE job_id = $1),
                   (SELECT COUNT(*) FROM job_attempts WHERE work_item_id = w.work_item_id)
            FROM admission_sessions a
            JOIN work_items w ON w.job_id = a.job_id
            WHERE a.job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect atomic attach/start: {error}"))?;
        require(
            state == ("attached".to_string(), "running".to_string(), 1, 1),
            format!("unexpected atomic attach/start state: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn ticket_cannot_attach_another_request_job_with_same_tenant_and_operation() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let ticket_a = claim_owner(&store, claim_request(None, "a".repeat(64))).await?;
        let ticket_b = claim_owner(&store, claim_request(None, "b".repeat(64))).await?;
        let job_b =
            insert_job_for_ticket(&database.pool, &ticket_b, "tenant-a", "generation", None)
                .await?;

        require(
            matches!(
                store.attach(attach_request(ticket_a.clone(), job_b)).await,
                Err(AdmissionError::InvalidOwner)
            ),
            "ticket A attached ticket B's job despite a different request_id",
        )?;
        require(
            matches!(
                store
                    .attach_and_start(
                        attach_request(ticket_a.clone(), job_b),
                        "cross-request-worker",
                        30_000,
                    )
                    .await,
                Err(AdmissionError::InvalidOwner)
            ),
            "ticket A atomically attached and started ticket B's job",
        )?;

        let state: (String, String, Option<Uuid>, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT state FROM admission_sessions WHERE session_id = $1),
              (SELECT state FROM admission_sessions WHERE session_id = $2),
              (SELECT admission_session_id FROM quota_reservations WHERE job_id = $3),
              (SELECT COUNT(*) FROM job_payloads WHERE job_id = $3),
              (SELECT COUNT(*) FROM work_items WHERE job_id = $3)
            "#,
        )
        .bind(ticket_a.session_id)
        .bind(ticket_b.session_id)
        .bind(job_b)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect rejected cross-request attach: {error}"))?;
        require(
            state == ("receiving".to_string(), "receiving".to_string(), None, 0, 0),
            format!("cross-request attach left durable state: {state:?}"),
        )?;

        store
            .attach(attach_request(ticket_b, job_b))
            .await
            .map_err(|error| format!("rightful ticket could not attach its job: {error}"))?;
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn attach_accepts_quota_prebound_to_the_ticket_session() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let ticket = claim_owner(&store, claim_request(None, "c".repeat(64))).await?;
        let job_id = insert_job_for_ticket(
            &database.pool,
            &ticket,
            "tenant-a",
            "generation",
            Some(ticket.session_id),
        )
        .await?;

        store
            .attach(attach_request(ticket.clone(), job_id))
            .await
            .map_err(|error| format!("matching prebound quota was rejected: {error}"))?;

        let bound_session: Option<Uuid> = sqlx::query_scalar(
            "SELECT admission_session_id FROM quota_reservations WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect prebound quota: {error}"))?;
        require(
            bound_session == Some(ticket.session_id),
            "attach changed the matching quota session binding",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn attach_rejects_quota_prebound_to_another_session() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let wrong_ticket = claim_owner(&store, claim_request(None, "d".repeat(64))).await?;
        let ticket = claim_owner(&store, claim_request(None, "e".repeat(64))).await?;
        let job_id = insert_job_for_ticket(
            &database.pool,
            &ticket,
            "tenant-a",
            "generation",
            Some(wrong_ticket.session_id),
        )
        .await?;

        require(
            matches!(
                store.attach(attach_request(ticket.clone(), job_id)).await,
                Err(AdmissionError::InvalidOwner)
            ),
            "ticket attached a quota reservation bound to another session",
        )?;

        let state: (String, Option<Uuid>, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT state FROM admission_sessions WHERE session_id = $1),
              (SELECT admission_session_id FROM quota_reservations WHERE job_id = $2),
              (SELECT COUNT(*) FROM job_payloads WHERE job_id = $2),
              (SELECT COUNT(*) FROM work_items WHERE job_id = $2)
            "#,
        )
        .bind(ticket.session_id)
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect rejected quota binding: {error}"))?;
        require(
            state == ("receiving".to_string(), Some(wrong_ticket.session_id), 0, 0),
            format!("rejected quota binding left durable state: {state:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn edit_attach_atomically_binds_quota_and_persists_ordered_inputs() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let input_specs = edit_input_specs(Uuid::new_v4(), None);
        let command = edit_command(&input_specs);
        let ticket = claim_edit_owner(&store, &command).await?;
        let input_specs = edit_input_specs(ticket.session_id, None);
        let command = edit_command(&input_specs);
        let job_id =
            insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "edit", None).await?;

        let request = edit_attach_request(ticket.clone(), job_id, command.clone(), input_specs);
        let attached = store
            .attach(request.clone())
            .await
            .map_err(|error| format!("edit attach failed: {error}"))?;
        let replay = store
            .attach(request)
            .await
            .map_err(|error| format!("edit attach replay failed: {error}"))?;
        require(
            replay == attached,
            "edit attach replay returned a different work item",
        )?;

        let bound_session: Option<Uuid> = sqlx::query_scalar(
            "SELECT admission_session_id FROM quota_reservations WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect quota binding: {error}"))?;
        require(
            bound_session == Some(ticket.session_id),
            "quota reservation was not bound to the edit admission",
        )?;

        let manifest: (Uuid, String, String, i16) = sqlx::query_as(
            r#"
            SELECT admission_session_id, manifest_schema, manifest_hash, input_count
            FROM job_input_manifests
            WHERE job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect edit manifest: {error}"))?;
        require(
            manifest
                == (
                    ticket.session_id,
                    EDIT_INPUT_MANIFEST_SCHEMA.to_string(),
                    command.input_manifest_hash_hex(),
                    2,
                ),
            format!("unexpected edit manifest: {manifest:?}"),
        )?;

        let inputs: Vec<(String, i16, String, String)> = sqlx::query_as(
            r#"
            SELECT role, input_index, media_type, object_key
            FROM job_input_objects
            WHERE job_id = $1
            ORDER BY CASE role WHEN 'image' THEN 0 ELSE 1 END, input_index
            "#,
        )
        .bind(job_id)
        .fetch_all(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect edit inputs: {error}"))?;
        require(
            inputs
                == vec![
                    (
                        "image".to_string(),
                        0,
                        "image/png".to_string(),
                        format!("inputs/{}/image-0", ticket.session_id.simple()),
                    ),
                    (
                        "mask".to_string(),
                        0,
                        "image/png".to_string(),
                        format!("inputs/{}/mask-0", ticket.session_id.simple()),
                    ),
                ],
            format!("unexpected persisted input order: {inputs:?}"),
        )?;

        let durable_rows: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM job_payloads WHERE job_id = $1), (SELECT COUNT(*) FROM work_items WHERE job_id = $1)",
        )
        .bind(job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect edit work rows: {error}"))?;
        require(
            durable_rows == (1, 1),
            format!("edit attach did not create one payload and work item: {durable_rows:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn edit_attach_rolls_back_every_row_when_an_input_object_conflicts() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());

        let first_specs = edit_input_specs(Uuid::new_v4(), None);
        let first_command = edit_command(&first_specs);
        let first_ticket = claim_edit_owner(&store, &first_command).await?;
        let first_specs = edit_input_specs(first_ticket.session_id, None);
        let first_command = edit_command(&first_specs);
        let conflicting_object_key = first_specs[0].blob.object_key.clone();
        let first_job =
            insert_job_for_ticket(&database.pool, &first_ticket, "tenant-a", "edit", None).await?;
        store
            .attach(edit_attach_request(
                first_ticket,
                first_job,
                first_command,
                first_specs,
            ))
            .await
            .map_err(|error| format!("first edit attach failed: {error}"))?;

        let second_specs = edit_input_specs(Uuid::new_v4(), Some(conflicting_object_key));
        let second_command = edit_command(&second_specs);
        let second_ticket = claim_edit_owner(&store, &second_command).await?;
        let second_specs = edit_input_specs(
            second_ticket.session_id,
            Some(format!("inputs/{}/image-0", first_job.simple())),
        );
        let mut second_specs = second_specs;
        second_specs[0].blob.object_key = sqlx::query_scalar(
            "SELECT object_key FROM job_input_objects WHERE job_id = $1 AND role = 'image'",
        )
        .bind(first_job)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to load conflicting object key: {error}"))?;
        let second_command = edit_command(&second_specs);
        let second_job =
            insert_job_for_ticket(&database.pool, &second_ticket, "tenant-a", "edit", None).await?;

        require(
            matches!(
                store
                    .attach(edit_attach_request(
                        second_ticket.clone(),
                        second_job,
                        second_command,
                        second_specs,
                    ))
                    .await,
                Err(AdmissionError::Unavailable)
            ),
            "conflicting input object key unexpectedly attached",
        )?;

        let counts: (i64, i64, i64, Option<Uuid>, String) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM job_input_manifests WHERE job_id = $1),
              (SELECT COUNT(*) FROM job_payloads WHERE job_id = $1),
              (SELECT COUNT(*) FROM work_items WHERE job_id = $1),
              (SELECT admission_session_id FROM quota_reservations WHERE job_id = $1),
              (SELECT state FROM admission_sessions WHERE session_id = $2)
            "#,
        )
        .bind(second_job)
        .bind(second_ticket.session_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to inspect rolled back edit attach: {error}"))?;
        require(
            counts == (0, 0, 0, None, "receiving".to_string()),
            format!("failed edit attach left partial durable state: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

async fn attachment_and_fencing_case(database: &TestDatabase) -> TestResult {
    let store = PostgresAdmissionStore::new(database.pool.clone());
    let ticket = match store
        .claim(claim_request(Some("e".repeat(64)), "f".repeat(64)))
        .await
        .map_err(|error| format!("owner claim failed: {error}"))?
    {
        AdmissionClaim::Owner(ticket) => ticket,
        other => return Err(format!("expected owner, got {other:?}")),
    };
    let job_id =
        insert_job_for_ticket(&database.pool, &ticket, "tenant-a", "generation", None).await?;
    let forged = AdmissionTicket {
        owner_token: Uuid::new_v4(),
        ..ticket.clone()
    };
    let forged_result = store.attach(attach_request(forged, job_id)).await;
    require(
        matches!(forged_result, Err(AdmissionError::InvalidOwner)),
        "forged owner attached a job",
    )?;

    let attached = store
        .attach(attach_request(ticket, job_id))
        .await
        .map_err(|error| format!("valid owner failed to attach: {error}"))?;
    let lease = store
        .claim_ready("worker-a", 30_000, AdmissionContract::LegacyV1)
        .await
        .map_err(|error| format!("work claim failed: {error}"))?
        .ok_or_else(|| "attached work was not ready".to_string())?;
    require(
        lease.work_item_id == attached.work_item_id && lease.job_id == job_id,
        "claimed the wrong work item",
    )?;
    require(
        store
            .claim_ready("worker-b", 30_000, AdmissionContract::LegacyV1)
            .await
            .map_err(|error| format!("second claim failed: {error}"))?
            .is_none(),
        "second worker claimed leased work",
    )?;
    store
        .start(&lease)
        .await
        .map_err(|error| format!("valid lease did not start: {error}"))?;

    let other_job_id = insert_unbound_job(&database.pool, "tenant-a", "generation").await?;
    let cross_job = gpt_image_2_gateway::admission::WorkLease {
        job_id: other_job_id,
        ..lease.clone()
    };
    require(
        matches!(
            store
                .settle(&cross_job, WorkOutcome::Failed, Some("cross_job"))
                .await,
            Err(AdmissionError::StaleLease)
        ),
        "lease was able to settle a different job",
    )?;
    let cross_events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_events WHERE job_id = $1")
        .bind(other_job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to count cross-job events: {error}"))?;
    require(cross_events == 0, "cross-job settlement wrote events")?;

    let stale = gpt_image_2_gateway::admission::WorkLease {
        lease_epoch: lease.lease_epoch + 1,
        ..lease.clone()
    };
    require(
        matches!(
            store.heartbeat(&stale, 30_000).await,
            Err(AdmissionError::StaleLease)
        ),
        "stale heartbeat succeeded",
    )?;
    require(
        matches!(
            store.settle(&stale, WorkOutcome::Succeeded, None).await,
            Err(AdmissionError::StaleLease)
        ),
        "stale settlement succeeded",
    )?;
    store
        .settle(&lease, WorkOutcome::Succeeded, None)
        .await
        .map_err(|error| format!("valid settlement failed: {error}"))?;
    require(
        matches!(
            store.settle(&lease, WorkOutcome::Succeeded, None).await,
            Err(AdmissionError::StaleLease)
        ),
        "duplicate settlement was not fenced",
    )?;

    let state: String =
        sqlx::query_scalar("SELECT state FROM idempotency_requests WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(&database.pool)
            .await
            .map_err(|error| format!("failed to read idempotency state: {error}"))?;
    let terminal_outbox: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_events WHERE job_id = $1 AND event_type = 'job.succeeded'",
    )
    .bind(job_id)
    .fetch_one(&database.pool)
    .await
    .map_err(|error| format!("failed to count terminal outbox: {error}"))?;
    require(state == "succeeded", format!("unexpected state {state}"))?;
    require(
        terminal_outbox == 1,
        format!("terminal outbox count {terminal_outbox}"),
    )
}

fn claim_request(key: Option<String>, request_hash: String) -> ClaimAdmission {
    claim_request_for_tenant("tenant-a", key, &request_hash)
}

fn claim_request_for_tenant(
    tenant_id: &str,
    key: Option<String>,
    request_hash: &str,
) -> ClaimAdmission {
    ClaimAdmission {
        owner_token: Uuid::new_v4(),
        tenant_id: tenant_id.to_string(),
        project_id: "project-a".to_string(),
        api_profile: "openai-images-v1".to_string(),
        operation: "generation".to_string(),
        request_id: format!("req_{}", Uuid::new_v4().simple()),
        idempotency_key_digest: key,
        request_hash: request_hash.to_string(),
        deadline_at_ms: i64::MAX,
    }
}

fn attach_request(ticket: AdmissionTicket, job_id: Uuid) -> AttachJob {
    attach_request_with_schedule(ticket, job_id, "tenant-a", 1, 1)
}

fn attach_request_with_schedule(
    ticket: AdmissionTicket,
    job_id: Uuid,
    schedule_scope: &str,
    schedule_weight: u32,
    schedule_cost: u64,
) -> AttachJob {
    let command = GenerationCommandV1 {
        background: "auto".to_string(),
        model: "gpt-image-2".to_string(),
        moderation: None,
        n: 1,
        operation: "generation".to_string(),
        output_compression: None,
        output_format: "png".to_string(),
        partial_images: 0,
        prompt: "durable".to_string(),
        provider_id: "openai-codex".to_string(),
        quality: "high".to_string(),
        schema_version: 1,
        size: "1024x1024".to_string(),
        source_api_profile: "openai-images-v1".to_string(),
        stream: false,
    };
    AttachJob {
        ticket,
        job_id,
        command_schema: "openai.images.generation.v1".to_string(),
        command_json: serde_json::to_value(command)
            .expect("generation command fixture must serialize"),
        input_manifest: None,
        work_kind: "image_batch".to_string(),
        schedule_scope: schedule_scope.to_string(),
        schedule_weight,
        schedule_priority: 1,
        schedule_cost,
        contract: AdmissionContract::LegacyV1,
        customer_pricing: None,
    }
}

fn customer_pricing_intent() -> CustomerPricingIntent {
    CustomerPricingIntent {
        public_model_id: "gpt-image-2".to_string(),
        provider_model_id: "gpt-image-2".to_string(),
        execution_model_id: "gpt-image-2".to_string(),
        provider_command_hash: None,
        media_kind: "image".to_string(),
        service_tier: "standard".to_string(),
        service_tier_decision:
            gpt_image_2_gateway::service_tiers::ServiceTierDecision::for_default_only_project(
                gpt_image_2_gateway::service_tiers::ProjectServiceTier::Default,
            ),
        execution_surface: "provider_cli".to_string(),
        currency: "USD".to_string(),
        pricing_dimensions: std::collections::BTreeMap::from([
            ("quality".to_string(), "high".to_string()),
            ("size".to_string(), "1024x1024".to_string()),
        ]),
        processing_mode: gpt_image_2_gateway::admission::PricingProcessingMode::Synchronous,
    }
}

fn codex_snapshot_pricing_intent() -> CustomerPricingIntent {
    CustomerPricingIntent {
        public_model_id: "gpt-image-2-2026-04-21".to_string(),
        provider_model_id: "gpt-image-2".to_string(),
        execution_model_id: "gpt-image-2-2026-04-21".to_string(),
        ..customer_pricing_intent()
    }
}

async fn claim_customer_pricing_owner(
    store: &PostgresAdmissionStore,
    output_count: u32,
) -> TestResult<(AdmissionTicket, serde_json::Value)> {
    claim_customer_pricing_owner_with_identity(
        store,
        output_count,
        "openai-codex",
        "gpt-image-2",
        "openai-images-v1",
    )
    .await
}

async fn claim_customer_pricing_owner_with_ticket_hash(
    store: &PostgresAdmissionStore,
    output_count: u32,
    ticket_hash: String,
) -> TestResult<(AdmissionTicket, serde_json::Value, String)> {
    let command = GenerationCommandV1 {
        background: "auto".to_string(),
        model: "gpt-image-2".to_string(),
        moderation: None,
        n: output_count,
        operation: "generation".to_string(),
        output_compression: None,
        output_format: "png".to_string(),
        partial_images: 0,
        prompt: "durable customer price".to_string(),
        provider_id: "openai-codex".to_string(),
        quality: "high".to_string(),
        schema_version: 1,
        size: "1024x1024".to_string(),
        source_api_profile: "openai-images-v1".to_string(),
        stream: false,
    };
    let command_hash = command.request_hash_hex();
    let ticket = claim_owner(store, claim_request(None, ticket_hash)).await?;
    let command_json = serde_json::to_value(command)
        .map_err(|error| format!("failed to encode v4 generation command: {error}"))?;
    Ok((ticket, command_json, command_hash))
}

async fn claim_customer_pricing_owner_with_identity(
    store: &PostgresAdmissionStore,
    output_count: u32,
    provider_id: &str,
    model: &str,
    api_profile: &str,
) -> TestResult<(AdmissionTicket, serde_json::Value)> {
    let command = GenerationCommandV1 {
        background: "auto".to_string(),
        model: model.to_string(),
        moderation: None,
        n: output_count,
        operation: "generation".to_string(),
        output_compression: None,
        output_format: "png".to_string(),
        partial_images: 0,
        prompt: "durable customer price".to_string(),
        provider_id: provider_id.to_string(),
        quality: "high".to_string(),
        schema_version: 1,
        size: "1024x1024".to_string(),
        source_api_profile: api_profile.to_string(),
        stream: false,
    };
    let request_hash = command.request_hash_hex();
    let ticket = claim_owner(store, claim_request(None, request_hash)).await?;
    let command_json = serde_json::to_value(command)
        .map_err(|error| format!("failed to encode v4 generation command: {error}"))?;
    Ok((ticket, command_json))
}

async fn seed_customer_price_v4(pool: &PgPool, success_micros: i64) -> TestResult {
    seed_customer_price_v4_for_operation(pool, "generation", success_micros).await
}

async fn seed_customer_price_v4_for_operation(
    pool: &PgPool,
    operation: &str,
    success_micros: i64,
) -> TestResult {
    let price_book_id = Uuid::new_v4();
    let version_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO price_books (
            price_book_id, price_book_key, display_name, purpose,
            scope_type, organization_id, project_id, provider_id,
            currency, state, control_version, created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, 'Admission test customer price', 'customer_sale',
            'platform', NULL, NULL, 'openai-codex',
            'USD', 'active', 1, 1, 1
        )
        "#,
    )
    .bind(price_book_id)
    .bind(format!("admission-test-{}", price_book_id.simple()))
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed v4 price book: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO price_book_versions (
            price_book_version_id, price_book_id, version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier, execution_surface,
            billing_mode, is_free, state, effective_from_ms,
            effective_until_ms, source_kind, source_url,
            source_checked_at_ms, notes, control_version,
            created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, 1, 'openai-images-v1', $3,
            'openai-codex', 'gpt-image-2', 'gpt-image-2',
            'image', 'standard', 'provider_cli', 'customer_rate',
            FALSE, 'draft', 0, NULL, 'manual', NULL, NULL, NULL, 1, 1, 1
        )
        "#,
    )
    .bind(version_id)
    .bind(price_book_id)
    .bind(operation)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed v4 price version: {error}"))?;
    for (outcome, unit_price_micros) in [
        ("succeeded", success_micros),
        ("failed", 0),
        ("no_effect", 0),
    ] {
        sqlx::query(
            r#"
            INSERT INTO price_components (
                price_component_id, price_book_version_id, component_key,
                metric, unit, unit_size, unit_price_micros, outcome,
                quantity_source, required_confidence, rounding_mode,
                dimensions_json, created_at_ms
            )
            VALUES (
                $1, $2, $3, 'image_output', 'image', 1, $4, $5,
                'request_derived', 'exact', 'exact',
                '{"quality":"high","size":"1024x1024"}'::JSONB, 1
            )
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(version_id)
        .bind(format!("image-output-{outcome}"))
        .bind(unit_price_micros)
        .bind(outcome)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to seed {outcome} v4 price component: {error}"))?;
    }
    bind_test_surface_contract(pool, version_id, None, None).await?;
    sqlx::query(
        r#"
        UPDATE price_book_versions
        SET state = 'active', control_version = control_version + 1,
            updated_at_ms = 2
        WHERE price_book_version_id = $1 AND state = 'draft'
        "#,
    )
    .bind(version_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to publish v4 price version: {error}"))?;
    Ok(())
}

async fn seed_dreamina_customer_price_v4(pool: &PgPool, success_micros: i64) -> TestResult {
    let price_book_id = Uuid::new_v4();
    let version_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO price_books (
            price_book_id, price_book_key, display_name, purpose,
            scope_type, organization_id, project_id, provider_id,
            currency, state, control_version, created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, 'Dreamina admission customer price', 'customer_sale',
            'platform', NULL, NULL, 'dreamina-cli',
            'USD', 'active', 1, 1, 1
        )
        "#,
    )
    .bind(price_book_id)
    .bind(format!(
        "dreamina-admission-test-{}",
        price_book_id.simple()
    ))
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina v4 price book: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO price_book_versions (
            price_book_version_id, price_book_id, version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier, execution_surface,
            billing_mode, is_free, state, effective_from_ms,
            effective_until_ms, source_kind, source_url,
            source_checked_at_ms, notes, control_version,
            created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, 1, 'dreamina-cli-images-v1', 'generation',
            'dreamina-cli', '5.0', '*',
            'image', 'standard', 'provider_cli', 'customer_rate',
            FALSE, 'draft', 0, NULL, 'official_document',
            'https://www.volcengine.com/docs/82379/1544106',
            1, 'Dreamina image customer rate', 1, 1, 1
        )
        "#,
    )
    .bind(version_id)
    .bind(price_book_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina v4 price version: {error}"))?;
    for (outcome, unit_price_micros) in [
        ("succeeded", success_micros),
        ("failed", 0),
        ("no_effect", 0),
    ] {
        sqlx::query(
            r#"
            INSERT INTO price_components (
                price_component_id, price_book_version_id, component_key,
                metric, unit, unit_size, unit_price_micros, outcome,
                quantity_source, required_confidence, rounding_mode,
                dimensions_json, created_at_ms
            )
            VALUES (
                $1, $2, $3, 'image_output', 'image', 1, $4, $5,
                'request_derived', 'exact', 'exact',
                '{"ratio":"16:9","resolution_type":"2k"}'::JSONB, 1
            )
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(version_id)
        .bind(format!("dreamina-image-output-{outcome}"))
        .bind(unit_price_micros)
        .bind(outcome)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to seed Dreamina {outcome} component: {error}"))?;
    }
    bind_test_surface_contract(
        pool,
        version_id,
        Some(ARK_IMAGES_API_PROFILE),
        Some("doubao-seedream-5-0-lite"),
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE price_book_versions
        SET state = 'active', control_version = control_version + 1,
            updated_at_ms = 2
        WHERE price_book_version_id = $1 AND state = 'draft'
        "#,
    )
    .bind(version_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to publish Dreamina v4 price version: {error}"))?;
    Ok(())
}

async fn seed_dreamina_video_customer_price_v4(
    pool: &PgPool,
    success_micros_per_second: i64,
) -> TestResult {
    let price_book_id = Uuid::new_v4();
    let version_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO price_books (
            price_book_id, price_book_key, display_name, purpose,
            scope_type, organization_id, project_id, provider_id,
            currency, state, control_version, created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, 'Dreamina video admission customer price', 'customer_sale',
            'platform', NULL, NULL, 'dreamina-cli',
            'USD', 'active', 1, 1, 1
        )
        "#,
    )
    .bind(price_book_id)
    .bind(format!(
        "dreamina-video-admission-test-{}",
        price_book_id.simple()
    ))
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina video price book: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO price_book_versions (
            price_book_version_id, price_book_id, version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier, execution_surface,
            billing_mode, is_free, state, effective_from_ms,
            effective_until_ms, source_kind, source_url,
            source_checked_at_ms, notes, control_version,
            created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, 1, 'dreamina-cli-videos-v1', 'video_generation',
            'dreamina-cli', 'seedance2.0fast', '*',
            'video', 'standard', 'provider_cli', 'customer_rate',
            FALSE, 'draft', 0, NULL, 'manual', NULL,
            1, 'Platform customer sale rate; not an Ark provider-cost claim', 1, 1, 1
        )
        "#,
    )
    .bind(version_id)
    .bind(price_book_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina video price version: {error}"))?;
    for (outcome, unit_price_micros) in [
        ("succeeded", success_micros_per_second),
        ("failed", 0),
        ("no_effect", 0),
    ] {
        sqlx::query(
            r#"
            INSERT INTO price_components (
                price_component_id, price_book_version_id, component_key,
                metric, unit, unit_size, unit_price_micros, outcome,
                quantity_source, required_confidence, rounding_mode,
                dimensions_json, created_at_ms
            )
            VALUES (
                $1, $2, $3, 'video_requested_second', 'second', 1, $4, $5,
                'request_derived', 'exact', 'exact',
                '{"duration":"8","ratio":"9:16","resolution":"720p"}'::JSONB, 1
            )
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(version_id)
        .bind(format!("dreamina-video-output-{outcome}"))
        .bind(unit_price_micros)
        .bind(outcome)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to seed Dreamina video {outcome} component: {error}"))?;
    }
    bind_test_surface_contract(
        pool,
        version_id,
        Some(ARK_CONTENT_GENERATION_API_PROFILE),
        Some("doubao-seedance-2-0-fast-260128"),
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE price_book_versions
        SET state = 'active', control_version = control_version + 1,
            updated_at_ms = 2
        WHERE price_book_version_id = $1 AND state = 'draft'
        "#,
    )
    .bind(version_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to publish Dreamina video price version: {error}"))?;
    Ok(())
}

async fn bind_test_surface_contract(
    pool: &PgPool,
    version_id: Uuid,
    api_profile: Option<&str>,
    public_model_id: Option<&str>,
) -> TestResult {
    let identity: (
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
    ) = sqlx::query_as(
        r#"
            SELECT version.api_profile, version.operation,
                   COALESCE(version.provider_id, book.provider_id, 'test-provider'),
                   COALESCE(version.provider_model_id, version.public_model_id),
                   version.public_model_id, version.media_kind,
                   version.service_tier, version.execution_surface
            FROM price_book_versions version
            JOIN price_books book ON book.price_book_id = version.price_book_id
            WHERE version.price_book_version_id = $1
            "#,
    )
    .bind(version_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to load test surface identity: {error}"))?;
    let api_profile = api_profile.unwrap_or(&identity.0);
    let public_model_id = public_model_id.unwrap_or(&identity.4);
    let contract_key = format!("test.admission-surface.{}", version_id.simple());
    sqlx::query(
        r#"
        INSERT INTO pricing_surface_contract_revisions (
            contract_key, revision, contract_hash, contract_schema_version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier, execution_surface,
            normalizer_key, normalizer_revision, contract_json, created_at_ms
        )
        VALUES (
            $1, 1, repeat('b', 64), 1,
            $2, $3, $4, $5, $6, $7, $8, $9,
            'test.admission-surface', 1, '{}'::JSONB, 1
        )
        "#,
    )
    .bind(&contract_key)
    .bind(api_profile)
    .bind(&identity.1)
    .bind(&identity.2)
    .bind(&identity.3)
    .bind(public_model_id)
    .bind(&identity.5)
    .bind(&identity.6)
    .bind(&identity.7)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed test surface contract: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO price_book_version_surface_contract_bindings (
            price_book_version_id, contract_key, contract_revision,
            contract_hash, bound_at_ms
        )
        VALUES ($1, $2, 1, repeat('b', 64), 1)
        "#,
    )
    .bind(version_id)
    .bind(&contract_key)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to bind test surface contract: {error}"))?;
    Ok(())
}

async fn seed_customer_token_price_v4(pool: &PgPool) -> TestResult {
    let price_book_id = Uuid::new_v4();
    let version_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO price_books (
            price_book_id, price_book_key, display_name, purpose,
            scope_type, organization_id, project_id, provider_id,
            currency, state, control_version, created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, 'Admission test official token price', 'customer_sale',
            'platform', NULL, NULL, 'openai-codex',
            'USD', 'active', 1, 1, 1
        )
        "#,
    )
    .bind(price_book_id)
    .bind(format!("admission-token-test-{}", price_book_id.simple()))
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed token price book: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO price_book_versions (
            price_book_version_id, price_book_id, version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier, execution_surface,
            billing_mode, is_free, state, effective_from_ms,
            effective_until_ms, source_kind, source_url,
            source_checked_at_ms, notes, control_version,
            created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, 1, 'openai-images-v1', 'generation',
            'openai-codex', 'gpt-image-2', 'gpt-image-2',
            'image', 'standard', 'provider_cli', 'customer_rate',
            FALSE, 'draft', 0, NULL, 'official_document',
            'https://developers.openai.com/api/docs/guides/image-generation',
            1, 'Official GPT Image 2 output token estimator', 1, 1, 1
        )
        "#,
    )
    .bind(version_id)
    .bind(price_book_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed token price version: {error}"))?;
    for (outcome, unit_price_micros) in [
        ("succeeded", 30_000_000_i64),
        ("failed", 0_i64),
        ("no_effect", 0_i64),
    ] {
        sqlx::query(
            r#"
            INSERT INTO price_components (
                price_component_id, price_book_version_id, component_key,
                metric, unit, unit_size, unit_price_micros, outcome,
                quantity_source, required_confidence, rounding_mode,
                dimensions_json, created_at_ms
            )
            VALUES (
                $1, $2, $3, 'image_output_token', 'token', 1000000, $4, $5,
                'official_lookup', 'estimated', 'exact',
                '{"quality":"high","size":"1024x1024"}'::JSONB, 1
            )
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(version_id)
        .bind(format!("image-output-token-{outcome}"))
        .bind(unit_price_micros)
        .bind(outcome)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to seed {outcome} token component: {error}"))?;
    }
    let contract_key = format!("test.legacy-token.{}", version_id.simple());
    sqlx::query(
        r#"
        INSERT INTO pricing_surface_contract_revisions (
            contract_key, revision, contract_hash, contract_schema_version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier, execution_surface,
            normalizer_key, normalizer_revision, contract_json, created_at_ms
        )
        VALUES (
            $1, 1, repeat('a', 64), 1,
            'openai-images-v1', 'generation', 'openai-codex', 'gpt-image-2',
            'gpt-image-2', 'image', 'standard', 'provider_cli',
            'test.legacy-token', 1, '{}'::JSONB, 1
        )
        "#,
    )
    .bind(&contract_key)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed legacy token surface contract: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO price_book_version_surface_contract_bindings (
            price_book_version_id, contract_key, contract_revision,
            contract_hash, bound_at_ms
        )
        VALUES ($1, $2, 1, repeat('a', 64), 1)
        "#,
    )
    .bind(version_id)
    .bind(&contract_key)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to bind legacy token surface contract: {error}"))?;
    sqlx::query(
        r#"
        UPDATE price_book_versions
        SET state = 'active', control_version = control_version + 1,
            updated_at_ms = 2
        WHERE price_book_version_id = $1 AND state = 'draft'
        "#,
    )
    .bind(version_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to publish token price version: {error}"))?;
    Ok(())
}

async fn seed_billing_account(pool: &PgPool, credit_limit_micros: i64) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO billing_accounts (
            tenant_id, currency, credit_limit_micros,
            held_micros, captured_micros, created_at_ms, updated_at_ms
        )
        VALUES ('tenant-a', 'USD', $1, 0, 0, 1, 1)
        "#,
    )
    .bind(credit_limit_micros)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed v4 billing account: {error}"))?;
    Ok(())
}

async fn seed_project_spend_budget(
    pool: &PgPool,
    limit_type: &str,
    monthly_budget_micros: i64,
) -> TestResult {
    let actor_user_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO identity_users(
            user_id, normalized_email, display_name, roles, scopes,
            created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, 'Admission budget actor',
            ARRAY['platform_owner'], ARRAY['admin:*'], 1, 1
        )
        "#,
    )
    .bind(actor_user_id)
    .bind(format!("budget-{}@admission.test", actor_user_id.simple()))
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed project budget actor: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO project_spend_budgets(
            project_id, organization_id, currency, monthly_budget_micros,
            limit_type, created_by_user_id, updated_by_user_id,
            created_at_ms, updated_at_ms
        )
        VALUES (
            'project-a', 'tenant-a', 'USD', $1,
            $2, $3, $3, 1, 1
        )
        "#,
    )
    .bind(monthly_budget_micros)
    .bind(limit_type)
    .bind(actor_user_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed project spend budget: {error}"))?;
    Ok(())
}

async fn seed_job_project_attribution(pool: &PgPool, job_id: Uuid) -> TestResult {
    seed_codex_job_project_attribution(
        pool,
        job_id,
        "images.generations",
        GENERATION_COMMAND_SCHEMA,
    )
    .await
}

async fn seed_codex_job_project_attribution(
    pool: &PgPool,
    job_id: Uuid,
    operation_id: &str,
    command_schema: &str,
) -> TestResult {
    seed_codex_job_project_attribution_with_models(
        pool,
        job_id,
        operation_id,
        command_schema,
        "gpt-image-2",
        "gpt-image-2",
        "gpt-image-2",
    )
    .await
}

async fn seed_codex_snapshot_job_project_attribution(
    pool: &PgPool,
    job_id: Uuid,
    operation_id: &str,
    command_schema: &str,
) -> TestResult {
    seed_codex_job_project_attribution_with_models(
        pool,
        job_id,
        operation_id,
        command_schema,
        "gpt-image-2-2026-04-21",
        "gpt-image-2",
        "gpt-image-2-2026-04-21",
    )
    .await
}

async fn seed_codex_job_project_attribution_with_models(
    pool: &PgPool,
    job_id: Uuid,
    operation_id: &str,
    command_schema: &str,
    public_model_id: &str,
    provider_model_id: &str,
    execution_model_id: &str,
) -> TestResult {
    let route_id = Uuid::new_v4();
    let route_key = format!("admission.{}", route_id.simple());
    sqlx::query(
        r#"
        INSERT INTO gateway_projects (id, tenant_id, name, created_at, archived_at)
        VALUES ('project-a', 'tenant-a', 'Admission test project', 1, NULL)
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed v4 project: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO job_auth_attributions (
            job_id, tenant_id, project_id, service_account_id, api_key_id,
            credential_authz_version, actor_user_id, actor_session_id,
            actor_authz_version, route_provider_id, route_operation_id,
            route_command_schema, route_id, route_revision,
            auth_kind, admitted_at_ms
        )
        VALUES (
            $1, 'tenant-a', 'project-a', NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            'legacy', 1
        )
        "#,
    )
    .bind(job_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed v4 job attribution: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_models (
            provider_id, model_id, execution_model_id, media_kind,
            display_name, adapter_state, lifecycle_state, operation_ids,
            source_kind, first_seen_at_ms, last_seen_at_ms, metadata_json
        )
        VALUES (
            'openai-codex', 'gpt-image-2', 'gpt-image-2', 'image',
            'GPT Image 2', 'supported', 'enabled',
            ARRAY[$1], 'adapter_contract', 1, 1, '{}'::JSONB
        )
        ON CONFLICT (provider_id, model_id, media_kind) DO NOTHING
        "#,
    )
    .bind(operation_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed v4 provider model: {error}"))?;
    if execution_model_id != "gpt-image-2" {
        sqlx::query(
            r#"
            INSERT INTO provider_models (
                provider_id, model_id, execution_model_id, media_kind,
                display_name, adapter_state, lifecycle_state, operation_ids,
                source_kind, first_seen_at_ms, last_seen_at_ms, metadata_json
            )
            VALUES (
                'openai-codex', $1, $1, 'image',
                'GPT Image 2 snapshot', 'supported', 'enabled',
                ARRAY[$2], 'adapter_contract', 1, 1, '{}'::JSONB
            )
            ON CONFLICT (provider_id, model_id, media_kind) DO NOTHING
            "#,
        )
        .bind(execution_model_id)
        .bind(operation_id)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to seed v4 snapshot model: {error}"))?;
    }
    sqlx::query(
        r#"
        INSERT INTO provider_routes (
            route_id, revision, route_key, display_name, provider_id,
            operation_id, command_schema, route_kind,
            selection_strategy, state, created_at_ms
        )
        VALUES (
            $1, 1, $2, 'Admission test route', 'openai-codex',
            $3, $4,
            'account', 'quota_aware_least_loaded', 'enabled', 1
        )
        "#,
    )
    .bind(route_id)
    .bind(route_key)
    .bind(operation_id)
    .bind(command_schema)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed v4 provider route: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_model_mappings (
            route_id, route_revision, provider_id, operation_id,
            command_schema, api_profile, public_model_id,
            provider_model_id, execution_model_id, media_kind, created_at_ms
        )
        VALUES (
            $1, 1, 'openai-codex', $2,
            $3, 'openai-images-v1',
            $4, $5, $6, 'image', 1
        )
        "#,
    )
    .bind(route_id)
    .bind(operation_id)
    .bind(command_schema)
    .bind(public_model_id)
    .bind(provider_model_id)
    .bind(execution_model_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed v4 model mapping: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO job_provider_route_attributions (
            job_id, tenant_id, api_key_id, provider_id, operation_id,
            command_schema, route_id, route_revision, attributed_at_ms
        )
        VALUES (
            $1, 'tenant-a', NULL, 'openai-codex', $2,
            $3, $4, 1, 1
        )
        "#,
    )
    .bind(job_id)
    .bind(operation_id)
    .bind(command_schema)
    .bind(route_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed v4 route attribution: {error}"))?;
    Ok(())
}

async fn seed_dreamina_job_project_attribution(
    pool: &PgPool,
    job_id: Uuid,
    api_profile: &str,
    public_model_id: &str,
) -> TestResult {
    let route_id = Uuid::new_v4();
    let route_key = format!("dreamina-admission.{}", route_id.simple());
    sqlx::query(
        r#"
        INSERT INTO gateway_projects (id, tenant_id, name, created_at, archived_at)
        VALUES ('project-a', 'tenant-a', 'Admission test project', 1, NULL)
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina project: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO job_auth_attributions (
            job_id, tenant_id, project_id, service_account_id, api_key_id,
            credential_authz_version, actor_user_id, actor_session_id,
            actor_authz_version, route_provider_id, route_operation_id,
            route_command_schema, route_id, route_revision,
            auth_kind, admitted_at_ms
        )
        VALUES (
            $1, 'tenant-a', 'project-a', NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            'legacy', 1
        )
        "#,
    )
    .bind(job_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina job attribution: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_models (
            provider_id, model_id, execution_model_id, media_kind,
            display_name, adapter_state, lifecycle_state, operation_ids,
            source_kind, first_seen_at_ms, last_seen_at_ms, metadata_json
        )
        VALUES (
            'dreamina-cli', '5.0', 'dreamina-image-5.0', 'image',
            'Dreamina Image 5.0', 'supported', 'enabled',
            ARRAY['images.generations'], 'adapter_contract', 1, 1, '{}'::JSONB
        )
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina provider model: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_routes (
            route_id, revision, route_key, display_name, provider_id,
            operation_id, command_schema, route_kind,
            selection_strategy, state, created_at_ms
        )
        VALUES (
            $1, 1, $2, 'Dreamina admission test route', 'dreamina-cli',
            'images.generations', $3,
            'account', 'quota_aware_least_loaded', 'enabled', 1
        )
        "#,
    )
    .bind(route_id)
    .bind(route_key)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina provider route: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_model_mappings (
            route_id, route_revision, provider_id, operation_id,
            command_schema, api_profile, public_model_id,
            provider_model_id, execution_model_id, media_kind, created_at_ms
        )
        VALUES (
            $1, 1, 'dreamina-cli', 'images.generations',
            $2, $3, $4, '5.0', 'dreamina-image-5.0', 'image', 1
        )
        "#,
    )
    .bind(route_id)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .bind(api_profile)
    .bind(public_model_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina model mapping: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO job_provider_route_attributions (
            job_id, tenant_id, api_key_id, provider_id, operation_id,
            command_schema, route_id, route_revision, attributed_at_ms
        )
        VALUES (
            $1, 'tenant-a', NULL, 'dreamina-cli', 'images.generations',
            $2, $3, 1, 1
        )
        "#,
    )
    .bind(job_id)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .bind(route_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina route attribution: {error}"))?;
    Ok(())
}

async fn configure_dreamina_video_job(pool: &PgPool, job_id: Uuid, duration: u8) -> TestResult {
    sqlx::query(
        r#"
        UPDATE jobs
        SET provider_id = 'dreamina-cli',
            model = $3,
            requested_units = $2,
            output_count = 1,
            billable_units = $2,
            billing_metric = 'video_second',
            billing_unit = 'second'
        WHERE job_id = $1
        "#,
    )
    .bind(job_id)
    .bind(i32::from(duration))
    .bind(DREAMINA_VIDEO_EXECUTION_MODEL)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to configure Dreamina video job: {error}"))?;
    sqlx::query(
        r#"
        UPDATE quota_reservations
        SET requested_units = $2,
            remaining_5h = limit_5h - $2,
            remaining_7d = limit_7d - $2,
            billing_metric = 'video_second',
            billing_unit = 'second'
        WHERE job_id = $1
        "#,
    )
    .bind(job_id)
    .bind(i32::from(duration))
    .execute(pool)
    .await
    .map_err(|error| format!("failed to configure Dreamina video quota: {error}"))?;
    Ok(())
}

async fn seed_dreamina_video_job_project_attribution(
    pool: &PgPool,
    job_id: Uuid,
    api_profile: &str,
    public_model_id: &str,
) -> TestResult {
    let route_id = Uuid::new_v4();
    let route_key = format!("dreamina-video-admission.{}", route_id.simple());
    sqlx::query(
        r#"
        INSERT INTO gateway_projects (id, tenant_id, name, created_at, archived_at)
        VALUES ('project-a', 'tenant-a', 'Admission test project', 1, NULL)
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina video project: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO job_auth_attributions (
            job_id, tenant_id, project_id, service_account_id, api_key_id,
            credential_authz_version, actor_user_id, actor_session_id,
            actor_authz_version, route_provider_id, route_operation_id,
            route_command_schema, route_id, route_revision,
            auth_kind, admitted_at_ms
        )
        VALUES (
            $1, 'tenant-a', 'project-a', NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            'legacy', 1
        )
        "#,
    )
    .bind(job_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina video job attribution: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_models (
            provider_id, model_id, execution_model_id, media_kind,
            display_name, adapter_state, lifecycle_state, operation_ids,
            source_kind, first_seen_at_ms, last_seen_at_ms, metadata_json
        )
        VALUES (
            'dreamina-cli', 'seedance2.0fast', $1, 'video',
            'Seedance 2.0 Fast', 'supported', 'enabled',
            ARRAY['videos.generations'], 'adapter_contract', 1, 1, '{}'::JSONB
        )
        "#,
    )
    .bind(DREAMINA_VIDEO_EXECUTION_MODEL)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina video provider model: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_routes (
            route_id, revision, route_key, display_name, provider_id,
            operation_id, command_schema, route_kind,
            selection_strategy, state, created_at_ms
        )
        VALUES (
            $1, 1, $2, 'Dreamina video admission test route', 'dreamina-cli',
            'videos.generations', $3,
            'account', 'quota_aware_least_loaded', 'enabled', 1
        )
        "#,
    )
    .bind(route_id)
    .bind(route_key)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina video provider route: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_model_mappings (
            route_id, route_revision, provider_id, operation_id,
            command_schema, api_profile, public_model_id,
            provider_model_id, execution_model_id, media_kind, created_at_ms
        )
        VALUES (
            $1, 1, 'dreamina-cli', 'videos.generations',
            $2, $3, $4, 'seedance2.0fast', $5, 'video', 1
        )
        "#,
    )
    .bind(route_id)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .bind(api_profile)
    .bind(public_model_id)
    .bind(DREAMINA_VIDEO_EXECUTION_MODEL)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina video model mapping: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO job_provider_route_attributions (
            job_id, tenant_id, api_key_id, provider_id, operation_id,
            command_schema, route_id, route_revision, attributed_at_ms
        )
        VALUES (
            $1, 'tenant-a', NULL, 'dreamina-cli', 'videos.generations',
            $2, $3, 1, 1
        )
        "#,
    )
    .bind(job_id)
    .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
    .bind(route_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed Dreamina video route attribution: {error}"))?;
    Ok(())
}

async fn claim_owner(
    store: &PostgresAdmissionStore,
    request: ClaimAdmission,
) -> TestResult<AdmissionTicket> {
    match store
        .claim(request)
        .await
        .map_err(|error| format!("owner claim failed: {error}"))?
    {
        AdmissionClaim::Owner(ticket) => Ok(ticket),
        other => Err(format!("expected owner, got {other:?}")),
    }
}

async fn insert_job_for_ticket(
    pool: &PgPool,
    ticket: &AdmissionTicket,
    tenant_id: &str,
    operation: &str,
    admission_session_id: Option<Uuid>,
) -> TestResult<Uuid> {
    let request_id: String =
        sqlx::query_scalar("SELECT request_id FROM admission_sessions WHERE session_id = $1")
            .bind(ticket.session_id)
            .fetch_one(pool)
            .await
            .map_err(|error| format!("failed to load admission request_id: {error}"))?;
    insert_job_record(
        pool,
        tenant_id,
        operation,
        &request_id,
        admission_session_id,
    )
    .await
}

async fn set_job_execution_model(pool: &PgPool, job_id: Uuid, model: &str) -> TestResult {
    let updated = sqlx::query("UPDATE jobs SET model = $2 WHERE job_id = $1")
        .bind(job_id)
        .bind(model)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to set test job execution model: {error}"))?;
    require(
        updated.rows_affected() == 1,
        "test job execution model was not updated",
    )
}

async fn set_job_provider_model(
    pool: &PgPool,
    job_id: Uuid,
    provider_id: &str,
    model: &str,
) -> TestResult {
    let updated = sqlx::query("UPDATE jobs SET provider_id = $2, model = $3 WHERE job_id = $1")
        .bind(job_id)
        .bind(provider_id)
        .bind(model)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to set test job provider model: {error}"))?;
    require(
        updated.rows_affected() == 1,
        "test job provider model was not updated",
    )
}

async fn insert_unbound_job(pool: &PgPool, tenant_id: &str, operation: &str) -> TestResult<Uuid> {
    insert_job_record(
        pool,
        tenant_id,
        operation,
        &format!("req_{}", Uuid::new_v4().simple()),
        None,
    )
    .await
}

async fn insert_job_record(
    pool: &PgPool,
    tenant_id: &str,
    operation: &str,
    request_id: &str,
    admission_session_id: Option<Uuid>,
) -> TestResult<Uuid> {
    let job_id = Uuid::new_v4();
    let reservation_id = Uuid::new_v4();
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin test job transaction: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO jobs
          (job_id, tenant_id, request_id, operation, provider_id, model, state,
           requested_units, output_count, billable_units, billing_metric, billing_unit,
           charged_units, reservation_id, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, 'openai-codex', 'gpt-image-2',
                'reserved', 1, 1, 1, 'output', 'output', 0, $5, 1, 1)
        "#,
    )
    .bind(job_id)
    .bind(tenant_id)
    .bind(request_id)
    .bind(operation)
    .bind(reservation_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("failed to insert test job: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO quota_reservations
          (reservation_id, tenant_id, request_id, job_id, requested_units,
           committed_units, started_units, released_units, state,
           created_at_ms, updated_at_ms, expires_at_ms,
           limit_5h, remaining_5h, limit_7d, remaining_7d,
           admission_session_id, billing_metric, billing_unit)
        VALUES ($1, $2, $3, $4, 1, 0, 0, 0, 'reserved', 1, 1,
                9223372036854775807, 100, 99, 100, 99, $5, 'output', 'output')
        "#,
    )
    .bind(reservation_id)
    .bind(tenant_id)
    .bind(request_id)
    .bind(job_id)
    .bind(admission_session_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("failed to insert test quota reservation: {error}"))?;
    tx.commit()
        .await
        .map_err(|error| format!("failed to commit test job: {error}"))?;
    Ok(job_id)
}

async fn claim_edit_owner(
    store: &PostgresAdmissionStore,
    command: &EditCommandV1,
) -> TestResult<AdmissionTicket> {
    let claim = ClaimAdmission {
        owner_token: Uuid::new_v4(),
        tenant_id: "tenant-a".to_string(),
        project_id: "project-a".to_string(),
        api_profile: "openai-images-v1".to_string(),
        operation: "edit".to_string(),
        request_id: format!("req_{}", Uuid::new_v4().simple()),
        idempotency_key_digest: None,
        request_hash: command.request_hash_hex(),
        deadline_at_ms: i64::MAX,
    };
    match store
        .claim(claim)
        .await
        .map_err(|error| format!("edit claim failed: {error}"))?
    {
        AdmissionClaim::Owner(ticket) => Ok(ticket),
        other => Err(format!("expected edit owner, got {other:?}")),
    }
}

async fn claim_grok_edit_owner(
    store: &PostgresAdmissionStore,
    request_hash: String,
) -> TestResult<AdmissionTicket> {
    let claim = ClaimAdmission {
        owner_token: Uuid::new_v4(),
        tenant_id: "tenant-a".to_owned(),
        project_id: "project-a".to_owned(),
        api_profile: XAI_IMAGES_API_PROFILE.to_owned(),
        operation: "edit".to_owned(),
        request_id: format!("req_{}", Uuid::new_v4().simple()),
        idempotency_key_digest: None,
        request_hash,
        deadline_at_ms: i64::MAX,
    };
    match store
        .claim(claim)
        .await
        .map_err(|error| format!("Grok edit claim failed: {error}"))?
    {
        AdmissionClaim::Owner(ticket) => Ok(ticket),
        other => Err(format!("expected Grok edit owner, got {other:?}")),
    }
}

fn grok_image_input_specs(session_id: Uuid, count: u16) -> Vec<AttachInputObject> {
    (0..count)
        .map(|index| AttachInputObject {
            blob: InputBlobRef {
                key: InputBlobKey {
                    admission_session_id: session_id,
                    input_id: Uuid::new_v4(),
                },
                storage_backend: "filesystem".to_owned(),
                object_key: format!("inputs/{}/image-{index}", session_id.simple()),
                sha256_hex: format!("{:064x}", u64::from(index) + 1),
                byte_size: 100 + u64::from(index),
            },
            role: EditInputRoleV1::Image,
            index,
            media_type: "image/png".to_owned(),
        })
        .collect()
}

fn grok_semantic_mask_input_specs(session_id: Uuid) -> Vec<AttachInputObject> {
    let mut inputs = grok_image_input_specs(session_id, 2);
    inputs.push(AttachInputObject {
        blob: InputBlobRef {
            key: InputBlobKey {
                admission_session_id: session_id,
                input_id: Uuid::new_v4(),
            },
            storage_backend: "filesystem".to_owned(),
            object_key: format!("inputs/{}/mask-0", session_id.simple()),
            sha256_hex: "f".repeat(64),
            byte_size: 45,
        },
        role: EditInputRoleV1::Mask,
        index: 0,
        media_type: "image/png".to_owned(),
    });
    inputs
}

fn grok_edit_job(has_mask: bool) -> EditJob {
    EditJob {
        request_id: "request-grok-edit".to_owned(),
        model: "grok-imagine-image-quality".to_owned(),
        prompt: "replace only the selected region".to_owned(),
        moderation: "auto".to_owned(),
        images: Vec::new(),
        mask: has_mask.then(|| InputImage {
            bytes: Vec::new(),
            content_type: Some("image/png".to_owned()),
            filename: Some("mask.png".to_owned()),
        }),
        n: 1,
        size: "16:9".to_owned(),
        quality: "auto".to_owned(),
        output_format: "png".to_owned(),
        output_compression: None,
        background: "opaque".to_owned(),
        stream: false,
        partial_images: 0,
    }
}

fn grok_edit_descriptors(inputs: &[AttachInputObject]) -> Vec<EditInputDescriptorV1> {
    inputs
        .iter()
        .map(|input| EditInputDescriptorV1 {
            byte_size: input.blob.byte_size,
            index: input.index,
            media_type: input.media_type.clone(),
            role: input.role,
            sha256_hex: input.blob.sha256_hex.clone(),
        })
        .collect()
}

fn grok_semantic_mask_plan(inputs: &[AttachInputObject]) -> TestResult<XaiImageEditAdmissionPlan> {
    XaiImageEditAdmissionPlan::for_grok_cli_with_fallback(
        &grok_edit_job(true),
        grok_edit_descriptors(inputs),
        XaiImageEditFallbackMode::SemanticMask,
    )
    .map_err(|error| format!("failed to build semantic-mask Grok edit plan: {error:?}"))
}

fn grok_strict_edit_plan(inputs: &[AttachInputObject]) -> TestResult<XaiImageEditAdmissionPlan> {
    XaiImageEditAdmissionPlan::for_grok_cli(&grok_edit_job(false), grok_edit_descriptors(inputs))
        .map_err(|error| format!("failed to build strict Grok edit plan: {error:?}"))
}

fn edit_input_manifest_hash(inputs: &[AttachInputObject]) -> String {
    EditCommandV1::from_edit_job(
        &grok_edit_job(true),
        grok_edit_descriptors(inputs),
        XAI_IMAGES_API_PROFILE,
        image_provider_grok_cli::PROVIDER_ID,
    )
    .input_manifest_hash_hex()
}

fn grok_edit_attach_request(
    ticket: AdmissionTicket,
    job_id: Uuid,
    plan: &XaiImageEditAdmissionPlan,
    inputs: Vec<AttachInputObject>,
) -> AttachJob {
    AttachJob {
        ticket,
        job_id,
        command_schema: plan.command_schema().to_owned(),
        command_json: plan.provider_command().clone(),
        input_manifest: Some(AttachInputManifest {
            manifest_schema: plan.input_manifest_schema().to_owned(),
            manifest_hash: plan.input_manifest_hash(),
            inputs,
        }),
        work_kind: "image_batch".to_owned(),
        schedule_scope: "tenant-a".to_owned(),
        schedule_weight: 1,
        schedule_priority: 1,
        schedule_cost: 1,
        contract: AdmissionContract::LegacyV1,
        customer_pricing: None,
    }
}

fn edit_attach_request(
    ticket: AdmissionTicket,
    job_id: Uuid,
    command: EditCommandV1,
    inputs: Vec<AttachInputObject>,
) -> AttachJob {
    AttachJob {
        ticket,
        job_id,
        command_schema: EDIT_COMMAND_SCHEMA.to_string(),
        command_json: serde_json::to_value(&command).expect("edit command serializes"),
        input_manifest: Some(AttachInputManifest {
            manifest_schema: EDIT_INPUT_MANIFEST_SCHEMA.to_string(),
            manifest_hash: command.input_manifest_hash_hex(),
            inputs,
        }),
        work_kind: "image_batch".to_string(),
        schedule_scope: "tenant-a".to_string(),
        schedule_weight: 1,
        schedule_priority: 1,
        schedule_cost: 1,
        contract: AdmissionContract::LegacyV1,
        customer_pricing: None,
    }
}

fn edit_input_specs(session_id: Uuid, image_object_key: Option<String>) -> Vec<AttachInputObject> {
    [
        (EditInputRoleV1::Image, 0, "1".repeat(64), 123_u64),
        (EditInputRoleV1::Mask, 0, "2".repeat(64), 45_u64),
    ]
    .into_iter()
    .map(|(role, index, sha256_hex, byte_size)| {
        let role_name = role.as_str();
        AttachInputObject {
            blob: InputBlobRef {
                key: InputBlobKey {
                    admission_session_id: session_id,
                    input_id: Uuid::new_v4(),
                },
                storage_backend: "filesystem".to_string(),
                object_key: if role == EditInputRoleV1::Image {
                    image_object_key.clone().unwrap_or_else(|| {
                        format!("inputs/{}/{role_name}-{index}", session_id.simple())
                    })
                } else {
                    format!("inputs/{}/{role_name}-{index}", session_id.simple())
                },
                sha256_hex,
                byte_size,
            },
            role,
            index,
            media_type: "image/png".to_string(),
        }
    })
    .collect()
}

fn edit_command(inputs: &[AttachInputObject]) -> EditCommandV1 {
    edit_command_for_model(inputs, "gpt-image-2")
}

fn edit_command_for_model(inputs: &[AttachInputObject], model: &str) -> EditCommandV1 {
    EditCommandV1::from_edit_job(
        &EditJob {
            request_id: "request-edit".to_string(),
            model: model.to_string(),
            prompt: "replace the sky".to_string(),
            moderation: "auto".to_string(),
            images: Vec::new(),
            mask: None,
            n: 1,
            size: "1024x1024".to_string(),
            quality: "high".to_string(),
            output_format: "png".to_string(),
            output_compression: None,
            background: "auto".to_string(),
            stream: false,
            partial_images: 0,
        },
        inputs
            .iter()
            .map(|input| EditInputDescriptorV1 {
                byte_size: input.blob.byte_size,
                index: input.index,
                media_type: input.media_type.clone(),
                role: input.role,
                sha256_hex: input.blob.sha256_hex.clone(),
            })
            .collect(),
        "openai-images-v1",
        "openai-codex",
    )
}

struct TestDatabase {
    schema: String,
    pool: PgPool,
}

impl TestDatabase {
    async fn new() -> TestResult<Option<Self>> {
        let Some(url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set in CI".to_string());
            }
            eprintln!("skipping PostgreSQL admission test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_admission_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 16, &schema)
            .await
            .map_err(|error| format!("failed to connect to test database: {error:?}"))?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(|error| format!("failed to identify database: {error}"))?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!("refusing DDL in non-test database {database_name}"));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(|error| format!("failed to create test schema: {error}"))?;
        if let Err(error) = run_migrations(&pool).await {
            let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                .execute(&pool)
                .await;
            pool.close().await;
            return Err(format!("failed to migrate test schema: {error:?}"));
        }
        Ok(Some(Self { schema, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .map_err(|error| format!("failed to drop test schema: {error}"));
        self.pool.close().await;
        result.map(|_| ())
    }
}

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn combine(primary: TestResult, cleanup: TestResult) -> TestResult {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(format!("{primary}; cleanup also failed: {cleanup}")),
    }
}
