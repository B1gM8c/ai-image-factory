use std::env;

use gpt_image_2_gateway::{
    EditJob,
    admission::{
        AdmissionClaim, AdmissionContract, AdmissionError, AdmissionStore, AdmissionTicket,
        AttachInputManifest, AttachInputObject, AttachJob, ClaimAdmission, EDIT_COMMAND_SCHEMA,
        EDIT_INPUT_MANIFEST_SCHEMA, EditCommandV1, EditInputDescriptorV1, EditInputRoleV1,
        PostgresAdmissionStore, WorkOutcome,
    },
    database::{connect_test_pool_with_search_path, run_migrations},
    input_blobs::{InputBlobKey, InputBlobRef},
};
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

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
                .claim_ready("fair-worker", 30_000)
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
        .claim_ready("worker-a", 30_000)
        .await
        .map_err(|error| format!("work claim failed: {error}"))?
        .ok_or_else(|| "attached work was not ready".to_string())?;
    require(
        lease.work_item_id == attached.work_item_id && lease.job_id == job_id,
        "claimed the wrong work item",
    )?;
    require(
        store
            .claim_ready("worker-b", 30_000)
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
    AttachJob {
        ticket,
        job_id,
        command_schema: "openai.images.generation.v1".to_string(),
        command_json: json!({"prompt": "durable"}),
        input_manifest: None,
        work_kind: "image_batch".to_string(),
        schedule_scope: schedule_scope.to_string(),
        schedule_weight,
        schedule_priority: 1,
        schedule_cost,
        contract: AdmissionContract::LegacyV1,
    }
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
           requested_units, charged_units, reservation_id, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, 'openai-codex', 'gpt-image-2',
                'reserved', 1, 0, $5, 1, 1)
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
           admission_session_id)
        VALUES ($1, $2, $3, $4, 1, 0, 0, 0, 'reserved', 1, 1,
                9223372036854775807, 100, 99, 100, 99, $5)
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
    EditCommandV1::from_edit_job(
        &EditJob {
            request_id: "request-edit".to_string(),
            model: "gpt-image-2".to_string(),
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
