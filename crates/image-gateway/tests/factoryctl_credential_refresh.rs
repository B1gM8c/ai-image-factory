use std::{collections::BTreeMap, fs, os::unix::fs::PermissionsExt, path::PathBuf, time::Duration};

use gpt_image_2_gateway::database::connect_test_pool_with_search_path;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{AssertSqlSafe, PgPool};
use tokio::{process::Command, time::timeout};
use uuid::Uuid;

#[allow(dead_code)]
#[path = "process_smoke_support/database.rs"]
mod database;

type TestResult<T = ()> = Result<T, String>;

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

#[tokio::test]
async fn codex_credential_refresh_is_scoped_and_non_generating() -> TestResult {
    let Some(database) = database::TestDatabase::new().await? else {
        return Ok(());
    };
    let pool = connect_test_pool_with_search_path(database.database_url(), 2, database.schema())
        .await
        .map_err(|error| format!("test pool failed: {error:?}"))?;
    let result = run_refresh_checks(&database, &pool).await;
    pool.close().await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

async fn run_refresh_checks(database: &database::TestDatabase, pool: &PgPool) -> TestResult {
    seed_pending_job_and_hold(pool).await?;
    let business_before = snapshot(pool, BUSINESS_TABLES).await?;

    let account = AccountFixture::new(pool, "openai-codex", false).await?;
    for (revision, fingerprint) in [(2, account.old_hash.clone()), (1, "f".repeat(64))] {
        let before = snapshot(pool, CREDENTIAL_TABLES).await?;
        let output = account.run(database, revision, &fingerprint).await?;
        require(output.status.success(), "stale credential command failed")?;
        let parsed = receipt(&output)?;
        require(
            parsed["result"] == "already_changed",
            "stale input was not rejected",
        )?;
        require(
            parsed["current_revision"] == 1,
            "stale readback changed revision",
        )?;
        require(
            snapshot(pool, CREDENTIAL_TABLES).await? == before,
            "stale input wrote credential records",
        )?;
        require(
            !account.home.join("rpc.jsonl").exists(),
            "stale input launched Codex",
        )?;
        require(
            fs::read(account.home.join("auth.json")).map_err(|e| e.to_string())?
                == account.old_auth,
            "stale input wrote credential material",
        )?;
    }

    let output = account.run(database, 1, &account.old_hash).await?;
    require(
        output.status.success(),
        format!(
            "credential refresh failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ),
    )?;
    let parsed = receipt(&output)?;
    require(
        parsed["result"] == "refreshed",
        "refresh receipt result is incorrect",
    )?;
    require(
        parsed["provider_account_id"] == account.id.to_string(),
        "receipt account is incorrect",
    )?;
    require(
        parsed["previous_revision"] == 1 && parsed["current_revision"] == 2,
        "refresh did not promote exactly one revision",
    )?;
    require(
        parsed["previous_fingerprint_sha256"] == account.old_hash
            && parsed["current_fingerprint_sha256"] == account.new_hash,
        "receipt fingerprints are incorrect",
    )?;
    require(
        parsed.as_object().is_some_and(|object| object.len() == 6),
        "receipt exposed extra metadata",
    )?;
    account.assert_only_refresh_rpc()?;
    let head: (i64, String) = sqlx::query_as("SELECT active_revision, lifecycle_state FROM provider_account_credential_heads WHERE provider_account_id = $1")
        .bind(account.id).fetch_one(pool).await.map_err(|e| e.to_string())?;
    require(
        head == (2, "active".to_owned()),
        "refreshed credential head is not active at revision 2",
    )?;
    let after_success = snapshot(pool, CREDENTIAL_TABLES).await?;
    let rpc_before = fs::read(account.home.join("rpc.jsonl")).map_err(|e| e.to_string())?;
    let output = account.run(database, 1, &account.old_hash).await?;
    require(
        output.status.success() && receipt(&output)?["result"] == "already_changed",
        "original revision triggered a second refresh",
    )?;
    require(
        snapshot(pool, CREDENTIAL_TABLES).await? == after_success,
        "original revision wrote after promotion",
    )?;
    require(
        fs::read(account.home.join("rpc.jsonl")).map_err(|e| e.to_string())? == rpc_before,
        "original revision launched a second Codex process",
    )?;
    require(
        snapshot(pool, BUSINESS_TABLES).await? == business_before,
        "successful refresh changed original job or billing hold",
    )?;

    let changed_identity = AccountFixture::new(pool, "openai-codex", true).await?;
    let output = changed_identity
        .run(database, 1, &changed_identity.old_hash)
        .await?;
    require(
        !output.status.success(),
        "changed upstream identity was accepted",
    )?;
    changed_identity.assert_only_refresh_rpc()?;
    let head: (i64, String) = sqlx::query_as("SELECT active_revision, lifecycle_state FROM provider_account_credential_heads WHERE provider_account_id = $1")
        .bind(changed_identity.id).fetch_one(pool).await.map_err(|e| e.to_string())?;
    require(
        head == (1, "reauth_required".to_owned()),
        "identity change did not fail closed at the original revision",
    )?;
    let revisions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM provider_account_credential_revisions WHERE provider_account_id = $1",
    )
    .bind(changed_identity.id)
    .fetch_one(pool)
    .await
    .map_err(|e| e.to_string())?;
    require(
        revisions == 1,
        "identity change promoted credential material",
    )?;
    require(
        snapshot(pool, BUSINESS_TABLES).await? == business_before,
        "failed refresh changed original job or billing hold",
    )?;

    let other_provider = AccountFixture::new(pool, "grok-cli", false).await?;
    let before = snapshot(pool, CREDENTIAL_TABLES).await?;
    let output = other_provider
        .run(database, 1, &other_provider.old_hash)
        .await?;
    require(!output.status.success(), "non-Codex account was accepted")?;
    require(
        !other_provider.home.join("rpc.jsonl").exists(),
        "non-Codex account launched Codex",
    )?;
    require(
        snapshot(pool, CREDENTIAL_TABLES).await? == before,
        "non-Codex account changed credential records",
    )?;
    require(
        snapshot(pool, BUSINESS_TABLES).await? == business_before,
        "rejected provider changed original job or billing hold",
    )
}

const CREDENTIAL_TABLES: &[&str] = &[
    "provider_accounts",
    "provider_account_environments",
    "provider_account_credential_heads",
    "provider_account_credential_revisions",
    "provider_account_credential_events",
];
const BUSINESS_TABLES: &[&str] = &[
    "jobs",
    "job_outputs",
    "provider_submissions",
    "customer_price_quotes",
    "customer_billing_holds",
    "billing_accounts",
    "ledger_transactions",
    "usage_events",
    "provider_account_login_sessions",
];

async fn snapshot(pool: &PgPool, tables: &[&str]) -> TestResult<BTreeMap<String, Value>> {
    let mut rows = BTreeMap::new();
    for table in tables {
        let value: Value = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT coalesce(jsonb_agg(to_jsonb(row) ORDER BY to_jsonb(row)::text), '[]'::jsonb) FROM {table} row"
        ))).fetch_one(pool).await.map_err(|e| e.to_string())?;
        rows.insert((*table).to_owned(), value);
    }
    Ok(rows)
}

fn receipt(output: &std::process::Output) -> TestResult<Value> {
    serde_json::from_slice(&output.stdout)
        .map_err(|_| "refresh output is not a JSON receipt".to_owned())
}

struct AccountFixture {
    _directory: tempfile::TempDir,
    root: PathBuf,
    home: PathBuf,
    executable: PathBuf,
    id: Uuid,
    old_auth: Vec<u8>,
    old_hash: String,
    new_hash: String,
}

impl AccountFixture {
    async fn new(pool: &PgPool, provider_id: &str, change_identity: bool) -> TestResult<Self> {
        let directory = tempfile::tempdir().map_err(|e| e.to_string())?;
        let root = directory.path().canonicalize().map_err(|e| e.to_string())?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).map_err(|e| e.to_string())?;
        let home = root.join("account");
        fs::create_dir(&home).map_err(|e| e.to_string())?;
        fs::set_permissions(&home, fs::Permissions::from_mode(0o700)).map_err(|e| e.to_string())?;
        let id = Uuid::new_v4();
        let identity = format!("account-{id}");
        let old_auth = format!(
            r#"{{"tokens":{{"account_id":"{identity}","access_token":"old-test-secret"}}}}"#
        )
        .into_bytes();
        let refreshed_identity = if change_identity {
            "different-account"
        } else {
            &identity
        };
        let new_auth = format!(
            r#"{{"tokens":{{"account_id":"{refreshed_identity}","access_token":"new-test-secret"}}}}"#
        );
        let old_hash = hex::encode(Sha256::digest(&old_auth));
        let new_hash = hex::encode(Sha256::digest(new_auth.as_bytes()));
        fs::write(home.join("auth.json"), &old_auth).map_err(|e| e.to_string())?;
        fs::set_permissions(home.join("auth.json"), fs::Permissions::from_mode(0o600))
            .map_err(|e| e.to_string())?;
        let executable = root.join("fake-codex");
        fs::write(&executable, format!(r#"#!/bin/sh
set -eu
[ "$1" = app-server ] && [ "$2" = --stdio ] && [ "$#" = 2 ]
IFS= read -r line
printf '%s\n' "$line" >> "$CODEX_HOME/rpc.jsonl"
printf '%s' "$line" | /usr/bin/grep -F '"method":"initialize"' >/dev/null
printf '%s\n' '{{"id":1,"result":{{}}}}'
IFS= read -r line
printf '%s\n' "$line" >> "$CODEX_HOME/rpc.jsonl"
printf '%s' "$line" | /usr/bin/grep -F '"method":"initialized"' >/dev/null
IFS= read -r line
printf '%s\n' "$line" >> "$CODEX_HOME/rpc.jsonl"
printf '%s' "$line" | /usr/bin/grep -F '"method":"account/read"' >/dev/null
printf '%s' "$line" | /usr/bin/grep -F '"refreshToken":true' >/dev/null
printf '%s' '{new_auth}' > "$CODEX_HOME/auth.json.next"
/bin/chmod 600 "$CODEX_HOME/auth.json.next"
/bin/mv "$CODEX_HOME/auth.json.next" "$CODEX_HOME/auth.json"
printf '%s\n' '{{"id":3,"result":{{"account":{{"email":"fixture@example.invalid","planType":"test"}}}}}}'
while IFS= read -r line; do printf '%s\n' "$line" >> "$CODEX_HOME/rpc.jsonl"; done
"#)).map_err(|e| e.to_string())?;
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755))
            .map_err(|e| e.to_string())?;
        let credential_pool_id = Uuid::new_v4();
        sqlx::query("INSERT INTO provider_credential_pools (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms) VALUES ($1, $2, $3, 'enabled', 1, 1)")
            .bind(credential_pool_id).bind(format!("refresh-{id}")).bind(provider_id)
            .execute(pool).await.map_err(|e| e.to_string())?;
        sqlx::query("INSERT INTO provider_accounts (provider_account_id, credential_pool_id, provider_id, account_key, credential_ref, credential_revision, credential_auth_sha256, state, created_at_ms, updated_at_ms) VALUES ($1, $2, $3, $4, $5, 1, $6, 'enabled', 1, 1)")
            .bind(id).bind(credential_pool_id).bind(provider_id).bind(format!("refresh-{id}"))
            .bind(format!("managed://refresh-{id}")).bind(&old_hash)
            .execute(pool).await.map_err(|e| e.to_string())?;
        sqlx::query("INSERT INTO provider_account_environments (provider_account_id, provider_id, environment_kind, environment_ref, upstream_identity_sha256, display_name, state, created_at_ms, updated_at_ms) VALUES ($1, $2, $3, $4, $5, 'Refresh test', 'active', 1, 1)")
            .bind(id).bind(provider_id).bind(if provider_id == "openai-codex" { "codex_home_v1" } else { "grok_home_v1" })
            .bind(home.to_string_lossy().as_ref()).bind(hex::encode(Sha256::digest(identity.as_bytes())))
            .execute(pool).await.map_err(|e| e.to_string())?;
        Ok(Self {
            _directory: directory,
            root,
            home,
            executable,
            id,
            old_auth,
            old_hash,
            new_hash,
        })
    }

    async fn run(
        &self,
        database: &database::TestDatabase,
        revision: i64,
        hash: &str,
    ) -> TestResult<std::process::Output> {
        let output = timeout(
            Duration::from_secs(20),
            Command::new(env!("CARGO_BIN_EXE_factoryctl"))
                .args([
                    "refresh-codex-credential",
                    &self.id.to_string(),
                    &revision.to_string(),
                    hash,
                ])
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .env("DATABASE_URL", database.database_url())
                .env("GATEWAY_DATABASE_SCHEMA", database.schema())
                .env("GATEWAY_PROVIDER_HOME_ROOT", &self.root)
                .env("GATEWAY_MANAGED_CODEX_EXECUTABLE", &self.executable)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| "credential command timed out".to_owned())?
        .map_err(|e| e.to_string())?;
        for bytes in [&output.stdout, &output.stderr] {
            let text = String::from_utf8_lossy(bytes);
            require(
                !text.contains("old-test-secret")
                    && !text.contains("new-test-secret")
                    && !text.contains(self.root.to_string_lossy().as_ref()),
                "credential command exposed secret material or a private path",
            )?;
        }
        Ok(output)
    }

    fn assert_only_refresh_rpc(&self) -> TestResult {
        let requests: Vec<Value> = fs::read_to_string(self.home.join("rpc.jsonl"))
            .map_err(|e| e.to_string())?
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())?;
        require(requests.len() == 3, "refresh emitted unexpected RPC calls")?;
        require(
            requests[0]["method"] == "initialize"
                && requests[1]["method"] == "initialized"
                && requests[2]["method"] == "account/read"
                && requests[2]["params"]["refreshToken"] == true,
            "refresh did not use only initialize and account/read(refreshToken=true)",
        )
    }
}

async fn seed_pending_job_and_hold(pool: &PgPool) -> TestResult {
    let job = Uuid::new_v4();
    let quote = Uuid::new_v4();
    let project = format!("refresh-fixture-{job}");
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(pool)
            .await
            .map_err(|e| e.to_string())?;
    let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
    sqlx::query("INSERT INTO gateway_projects (id, tenant_id, name, created_at) VALUES ($1, 'tenant_default', 'Credential refresh fixture', $2)")
        .bind(&project).bind(now).execute(&mut *tx).await.map_err(|e| e.to_string())?;
    sqlx::query("INSERT INTO jobs (job_id, request_id, state, requested_units, output_count, billable_units, economics_contract_version, created_at_ms, updated_at_ms) VALUES ($1, $2, 'running', 1, 1, 1, 4, $3, $3)")
        .bind(job).bind(format!("refresh-job-{job}")).bind(now).execute(&mut *tx).await.map_err(|e| e.to_string())?;
    sqlx::query("INSERT INTO job_auth_attributions (job_id, tenant_id, project_id, auth_kind, admitted_at_ms) VALUES ($1, 'tenant_default', $2, 'legacy', $3)")
        .bind(job).bind(&project).bind(now).execute(&mut *tx).await.map_err(|e| e.to_string())?;
    sqlx::query(r#"INSERT INTO customer_price_quotes (
        quote_id, job_id, tenant_id, project_id, price_book_id, price_book_version_id,
        api_profile, operation, provider_id, provider_model_id, public_model_id, media_kind,
        service_tier, execution_surface, request_dimensions_json, billing_mode, is_free,
        currency, max_total_micros, quote_hash, created_at_ms
    ) SELECT $1, $2, 'tenant_default', $3, version.price_book_id, version.price_book_version_id,
        version.api_profile, version.operation, version.provider_id, version.provider_model_id,
        version.public_model_id, version.media_kind, version.service_tier, version.execution_surface,
        '{}'::jsonb, version.billing_mode, version.is_free, book.currency, 40000, repeat('a', 64), $4
      FROM price_book_versions version JOIN price_books book USING (price_book_id)
      WHERE version.price_book_version_id = 'd7734203-0a26-4cc0-8e50-3cc7b7d55117'"#)
        .bind(quote).bind(job).bind(&project).bind(now).execute(&mut *tx).await.map_err(|e| e.to_string())?;
    sqlx::query(
        r#"INSERT INTO customer_price_quote_lines (
        quote_line_id, quote_id, job_id, price_component_id, component_key, partition_key,
        terminal_outcome, metric, unit, unit_size, unit_price_micros, quantity_source,
        required_confidence, rounding_mode, reservation_quantity_source, reservation_confidence,
        dimensions_json, max_quantity, max_amount_micros, created_at_ms
    ) SELECT $1, $2, $3, price_component_id, component_key, 'output:0', outcome, metric, unit,
        unit_size, unit_price_micros, quantity_source, required_confidence, rounding_mode,
        'request_derived', 'exact', dimensions_json, 1, 40000, $4
      FROM price_components WHERE price_component_id = '830a57f9-e68c-4bbb-8880-14b0aab05117'"#,
    )
    .bind(Uuid::new_v4())
    .bind(quote)
    .bind(job)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    sqlx::query("INSERT INTO billing_accounts (tenant_id, currency, credit_limit_micros, held_micros, captured_micros, created_at_ms, updated_at_ms) VALUES ('tenant_default', 'USD', 100000, 40000, 0, $1, $1)")
        .bind(now).execute(&mut *tx).await.map_err(|e| e.to_string())?;
    sqlx::query("INSERT INTO customer_billing_holds (hold_id, quote_id, job_id, tenant_id, currency, held_micros, account_held_micros, state, created_at_ms, updated_at_ms) VALUES ($1, $2, $3, 'tenant_default', 'USD', 40000, 40000, 'held', $4, $4)")
        .bind(Uuid::new_v4()).bind(quote).bind(job).bind(now).execute(&mut *tx).await.map_err(|e| e.to_string())?;
    sqlx::query("INSERT INTO provider_account_login_sessions (login_session_id, provider_id, login_method, account_key, display_name, environment_ref, status, max_concurrency, expires_at_ms, created_at_ms, updated_at_ms) VALUES ($1, 'openai-codex', 'device_code', 'unrelated-login', 'Unrelated login', '/test/unrelated-login', 'waiting_for_user', 1, 1, 1, 1)")
        .bind(Uuid::new_v4()).execute(&mut *tx).await.map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())
}
