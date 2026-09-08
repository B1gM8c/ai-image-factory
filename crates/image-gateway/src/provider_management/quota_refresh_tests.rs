use super::*;
use crate::{
    CodexExecutionProfileProvisioning,
    database::{connect_test_pool_with_search_path, run_migrations},
    provision_codex_execution_profile,
};
use sqlx::AssertSqlSafe;

struct Fixture {
    pool: PgPool,
    schema: String,
    account: Uuid,
    route: Uuid,
}
impl Fixture {
    async fn new() -> Option<Self> {
        let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
            assert!(
                std::env::var_os("CI").is_none(),
                "TEST_DATABASE_URL required in CI"
            );
            eprintln!("skipping quota PostgreSQL test: TEST_DATABASE_URL absent");
            return None;
        };
        let schema = format!("quota_refresh_test_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 8, &schema)
            .await
            .unwrap();
        let database: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            database.to_ascii_lowercase().contains("test"),
            "refusing non-test DB"
        );
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .unwrap();
        run_migrations(&pool).await.unwrap();
        let provisioned = provision_codex_execution_profile(
            &pool,
            &CodexExecutionProfileProvisioning {
                profile_key: "quota-test-profile".into(),
                credential_pool_key: "quota-test-pool".into(),
                provider_account_key: "quota-test-account".into(),
                credential_ref: "mounted.quota-test.1".into(),
                credential_revision: 1,
                credential_auth_sha256: "a".repeat(64),
                max_concurrency: 2,
            },
        )
        .await
        .unwrap();
        let account = provisioned.provider_account_id;
        sqlx::query(r#"INSERT INTO provider_account_environments
            (provider_account_id,provider_id,environment_kind,environment_ref,upstream_identity_sha256,display_name,state,created_at_ms,updated_at_ms)
            VALUES($1,'openai-codex','codex_home_v1','/tmp/quota-test-unused-home',$2,'Quota fixture','active',1,1)"#)
            .bind(account).bind("b".repeat(64)).execute(&pool).await.unwrap();
        let mut fixture = Self {
            pool,
            schema,
            account,
            route: Uuid::nil(),
        };
        fixture.route = fixture.add_route(300_000).await;
        Some(fixture)
    }
    async fn add_route(&self, ttl: i64) -> Uuid {
        let route = Uuid::new_v4();
        let key = format!("quota.{}", route.simple());
        sqlx::query(r#"INSERT INTO provider_routes
            (route_id,revision,route_key,display_name,provider_id,operation_id,command_schema,route_kind,selection_strategy,state,created_at_ms,quota_freshness_ms,unknown_quota_policy)
            SELECT $1,1,$2,'Quota fixture',provider_id,operation_id,command_schema,'account','quota_aware_least_loaded','enabled',1,$3,'block'
            FROM provider_execution_profiles WHERE provider_account_id=$4"#)
            .bind(route).bind(key).bind(ttl).bind(self.account).execute(&self.pool).await.unwrap();
        sqlx::query(r#"INSERT INTO provider_route_heads
            (route_id,route_key,provider_id,operation_id,command_schema,route_kind,current_revision,state,created_at_ms,updated_at_ms)
            SELECT route_id,route_key,provider_id,operation_id,command_schema,route_kind,revision,state,1,1 FROM provider_routes WHERE route_id=$1"#)
            .bind(route).execute(&self.pool).await.unwrap();
        sqlx::query(r#"INSERT INTO provider_route_members
            (route_id,route_revision,provider_id,operation_id,command_schema,provider_account_id,execution_profile_id,created_at_ms)
            SELECT $1,1,provider_id,operation_id,command_schema,provider_account_id,execution_profile_id,1 FROM provider_execution_profiles WHERE provider_account_id=$2"#)
            .bind(route).bind(self.account).execute(&self.pool).await.unwrap();
        route
    }
    fn service(&self) -> PostgresProviderManagementService {
        PostgresProviderManagementService::new(
            self.pool.clone(),
            "/tmp/quota-test-unused-home".into(),
            "/not/a/real/codex".into(),
        )
    }
    async fn expire_lease(&self) {
        sqlx::query("UPDATE provider_account_quota_refreshes SET lease_expires_at_ms=1 WHERE provider_account_id=$1")
            .bind(self.account).execute(&self.pool).await.unwrap();
    }
    async fn cleanup(self) {
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .unwrap();
        self.pool.close().await;
    }
}

fn observation() -> (CodexAccountSnapshot, CodexQuotaSnapshot) {
    (
        CodexAccountSnapshot {
            email: None,
            plan_type: Some("test".into()),
        },
        CodexQuotaSnapshot {
            plan_type: Some("test".into()),
            credits_balance: None,
            credits_unlimited: None,
            windows: vec![super::super::codex_app_server::CodexQuotaWindow {
                limit_id: "codex".into(),
                limit_name: None,
                window_role: "primary",
                window_duration_mins: Some(300),
                used_percent: 50,
                resets_at_ms: Some(wall_now_ms() + 300_000),
            }],
        },
    )
}

#[test]
fn quota_backoff_is_bounded_and_runtime_staleness_is_fail_closed() {
    assert_eq!(
        (0..7).map(backoff_ms).collect::<Vec<_>>(),
        [0, 30_000, 60_000, 120_000, 240_000, 300_000, 300_000]
    );
    let state = QuotaRefreshRuntime::new(QuotaRefreshConfig {
        enabled: true,
        ..Default::default()
    });
    assert!(!state.snapshot().healthy);
    state.running.store(true, Ordering::Relaxed);
    state.last_pass.store(wall_now_ms(), Ordering::Relaxed);
    assert!(state.snapshot().healthy);
    state
        .last_pass
        .store(wall_now_ms() - 46_000, Ordering::Relaxed);
    assert!(!state.snapshot().healthy);
    let guard = RunningGuard(Arc::new(state));
    let state = guard.0.clone();
    drop(guard);
    assert!(!state.snapshot().running);
}

#[tokio::test]
async fn quota_default_off_starts_no_loop_or_observer() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused@127.0.0.1:1/unused")
        .unwrap();
    let service = Arc::new(PostgresProviderManagementService::new(
        pool,
        "/unused".into(),
        "/not/executable".into(),
    ));
    assert!(service.spawn_codex_quota_refresh().is_none());
    let runtime = service.quota_refresh.snapshot();
    assert!(!runtime.enabled && !runtime.running && runtime.healthy);
    assert_eq!(runtime.attempts, 0);
}

#[tokio::test]
async fn quota_fake_app_server_is_killed_when_observation_future_times_out() {
    use super::super::codex_app_server::CodexAppServer;
    use std::os::unix::fs::PermissionsExt;
    let home = tempfile::tempdir().unwrap();
    let executable = home.path().join("fake-codex");
    std::fs::write(&executable, "#!/bin/sh\necho $$ > observer.pid\nIFS= read -r initialize\nprintf '{\"id\":1,\"result\":{}}\\n'\nwhile IFS= read -r request; do :; done\n").unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut server = CodexAppServer::spawn(&executable, home.path())
        .await
        .unwrap();
    let pid = std::fs::read_to_string(home.path().join("observer.pid")).unwrap();
    let timed = timeout(
        Duration::from_millis(20),
        async move { server.account().await },
    )
    .await;
    assert!(timed.is_err());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let status = tokio::process::Command::new("/bin/kill")
            .args(["-0", pid.trim()])
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .unwrap();
        if !status.success() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cancelled fake observer process survived"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn quota_pg_enabled_loop_reports_pass_and_shutdown_without_observer() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    sqlx::query("UPDATE provider_route_heads SET state='disabled'")
        .execute(&f.pool)
        .await
        .unwrap();
    let mut service = f.service();
    service.quota_refresh = Arc::new(QuotaRefreshRuntime::new(QuotaRefreshConfig {
        enabled: true,
        ..Default::default()
    }));
    let service = Arc::new(service);
    let worker = service.spawn_codex_quota_refresh().unwrap();
    assert!(
        service.spawn_codex_quota_refresh().is_none(),
        "loop cannot start twice"
    );
    timeout(Duration::from_secs(3), async {
        while !service.quota_refresh.snapshot().healthy {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let state = service.quota_refresh.snapshot();
    assert!(state.running && state.enabled && state.last_pass_at_ms.is_some());
    assert_eq!(state.attempts, 0, "disabled routes never start observer");
    worker.abort();
    let _ = worker.await;
    assert!(!service.quota_refresh.snapshot().running);
    assert!(!service.quota_refresh.snapshot().healthy);
    f.cleanup().await;
}

#[tokio::test]
async fn quota_pg_manual_auto_race_and_expired_epoch_cannot_publish() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (manual, automatic) = tokio::join!(
        claim(&f.pool, f.account, true),
        claim(&f.pool, f.account, false)
    );
    let manual = manual.unwrap();
    let automatic = automatic.unwrap();
    assert_eq!(
        usize::from(manual.is_some()) + usize::from(automatic.is_some()),
        1
    );
    let old = manual.or(automatic).unwrap();
    assert!(claim(&f.pool, f.account, true).await.unwrap().is_none());
    f.expire_lease().await;
    let new = claim(&f.pool, f.account, false).await.unwrap().unwrap();
    assert!(new.lease_epoch > old.lease_epoch);
    let error = finish(&f.pool, &old, Some(&observation()), None)
        .await
        .unwrap_err();
    assert_eq!(error.error_code(), Some("quota_refresh_lease_lost"));
    finish(&f.pool, &new, Some(&observation()), None)
        .await
        .unwrap();
    assert!(
        finish(&f.pool, &old, None, Some("stale_failure"))
            .await
            .is_err()
    );
    let status: String = sqlx::query_scalar(
        "SELECT status FROM provider_account_quota_snapshots WHERE provider_account_id=$1",
    )
    .bind(f.account)
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(status, "observed");
    f.cleanup().await;
}

#[tokio::test]
async fn quota_pg_timeout_backoff_survives_service_restart_and_manual_retry() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let service = f.service();
    let result = service
        .refresh_codex_quota_observation(
            f.account,
            false,
            Duration::from_millis(5),
            std::future::pending(),
        )
        .await;
    assert!(result.is_err());
    assert_eq!(service.quota_refresh.snapshot().timed_out, 1);
    assert_eq!(service.quota_refresh.snapshot().in_flight, 0);
    let state: (i32,String,i64) = sqlx::query_as("SELECT consecutive_failures,last_error_code,next_attempt_at_ms-last_completed_at_ms FROM provider_account_quota_refreshes WHERE provider_account_id=$1")
        .bind(f.account).fetch_one(&f.pool).await.unwrap();
    assert_eq!(state, (1, "quota_observer_timeout".into(), 30_000));
    let service_after_restart = f.service();
    assert!(
        due_accounts(&service_after_restart.pool)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        claim(&service_after_restart.pool, f.account, false)
            .await
            .unwrap()
            .is_none()
    );
    service_after_restart
        .refresh_codex_quota_observation(f.account, true, Duration::from_secs(1), async {
            Ok(observation())
        })
        .await
        .unwrap();
    assert_eq!(service_after_restart.quota_refresh.snapshot().succeeded, 1);
    let failures: i32 = sqlx::query_scalar("SELECT consecutive_failures FROM provider_account_quota_refreshes WHERE provider_account_id=$1").bind(f.account).fetch_one(&f.pool).await.unwrap();
    assert_eq!(failures, 0);
    f.cleanup().await;
}

#[tokio::test]
async fn quota_pg_selector_deduplicates_strictest_ttl_reset_and_disabled_routes() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.add_route(900_000).await;
    assert_eq!(due_accounts(&f.pool).await.unwrap(), vec![f.account]);
    let lease = claim(&f.pool, f.account, false).await.unwrap().unwrap();
    finish(&f.pool, &lease, Some(&observation()), None)
        .await
        .unwrap();
    assert!(due_accounts(&f.pool).await.unwrap().is_empty());
    let mut scan_micros = Vec::new();
    for _ in 0..25 {
        let start = std::time::Instant::now();
        assert!(due_accounts(&f.pool).await.unwrap().is_empty());
        scan_micros.push(start.elapsed().as_micros());
    }
    scan_micros.sort_unstable();
    eprintln!(
        "quota fresh scan PG fixture (1 account/2 routes, n=25): p50={}us p95={}us",
        scan_micros[12], scan_micros[23]
    );
    sqlx::query("UPDATE provider_account_quota_snapshots SET observed_at_ms=observed_at_ms-250000 WHERE provider_account_id=$1").bind(f.account).execute(&f.pool).await.unwrap();
    assert_eq!(
        due_accounts(&f.pool).await.unwrap(),
        vec![f.account],
        "strict 300s TTL, not 900s"
    );
    sqlx::query("UPDATE provider_account_quota_snapshots SET observed_at_ms=$2 WHERE provider_account_id=$1").bind(f.account).bind(wall_now_ms()-100).execute(&f.pool).await.unwrap();
    sqlx::query(
        "UPDATE provider_account_quota_windows SET resets_at_ms=$2 WHERE provider_account_id=$1",
    )
    .bind(f.account)
    .bind(wall_now_ms() - 10)
    .execute(&f.pool)
    .await
    .unwrap();
    assert_eq!(
        due_accounts(&f.pool).await.unwrap(),
        vec![f.account],
        "window reset overrides fresh snapshot"
    );
    sqlx::query("UPDATE provider_route_heads SET state='disabled' WHERE route_id=$1")
        .bind(f.route)
        .execute(&f.pool)
        .await
        .unwrap();
    assert_eq!(
        due_accounts(&f.pool).await.unwrap(),
        vec![f.account],
        "other enabled route still needs account"
    );
    sqlx::query("UPDATE provider_route_heads SET state='disabled'")
        .execute(&f.pool)
        .await
        .unwrap();
    assert!(due_accounts(&f.pool).await.unwrap().is_empty());
    sqlx::query("UPDATE provider_route_heads SET state='enabled'")
        .execute(&f.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE provider_account_execution_controls SET lifecycle_state='draining' WHERE provider_account_id=$1").bind(f.account).execute(&f.pool).await.unwrap();
    assert!(due_accounts(&f.pool).await.unwrap().is_empty());
    f.cleanup().await;
}

#[tokio::test]
async fn quota_pg_selector_rejects_inactive_credentials_pool_operation_and_old_revision() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    assert_eq!(due_accounts(&f.pool).await.unwrap(), vec![f.account]);
    for (disable, enable) in [
        (
            "UPDATE provider_account_credential_heads SET lifecycle_state='reauth_required'",
            "UPDATE provider_account_credential_heads SET lifecycle_state='active'",
        ),
        (
            "UPDATE provider_credential_pools SET state='disabled'",
            "UPDATE provider_credential_pools SET state='enabled'",
        ),
        (
            "UPDATE provider_account_operations SET state='disabled'",
            "UPDATE provider_account_operations SET state='enabled'",
        ),
        (
            "UPDATE provider_account_environments SET state='invalid'",
            "UPDATE provider_account_environments SET state='active'",
        ),
        (
            "UPDATE provider_execution_profiles SET state='disabled'",
            "UPDATE provider_execution_profiles SET state='enabled'",
        ),
        (
            "UPDATE provider_accounts SET state='disabled'",
            "UPDATE provider_accounts SET state='enabled'",
        ),
    ] {
        sqlx::query(AssertSqlSafe(disable))
            .execute(&f.pool)
            .await
            .unwrap();
        assert!(due_accounts(&f.pool).await.unwrap().is_empty(), "{disable}");
        sqlx::query(AssertSqlSafe(enable))
            .execute(&f.pool)
            .await
            .unwrap();
        assert_eq!(due_accounts(&f.pool).await.unwrap(), vec![f.account]);
    }
    sqlx::query(r#"INSERT INTO provider_routes
        SELECT route_id,revision+1,route_key,display_name,provider_id,operation_id,command_schema,route_kind,selection_strategy,state,created_at_ms,quota_freshness_ms,unknown_quota_policy
        FROM provider_routes WHERE route_id=$1"#).bind(f.route).execute(&f.pool).await.unwrap();
    sqlx::query("UPDATE provider_route_heads SET current_revision=2 WHERE route_id=$1")
        .bind(f.route)
        .execute(&f.pool)
        .await
        .unwrap();
    assert!(
        due_accounts(&f.pool).await.unwrap().is_empty(),
        "old revision member is not current"
    );
    f.cleanup().await;
}

#[tokio::test]
async fn quota_pg_unavailable_is_not_fresh_and_failure_preserves_observation_time() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let lease = claim(&f.pool, f.account, true).await.unwrap().unwrap();
    finish(&f.pool, &lease, Some(&observation()), None)
        .await
        .unwrap();
    let observed: i64 = sqlx::query_scalar(
        "SELECT observed_at_ms FROM provider_account_quota_snapshots WHERE provider_account_id=$1",
    )
    .bind(f.account)
    .fetch_one(&f.pool)
    .await
    .unwrap();
    let lease = claim(&f.pool, f.account, true).await.unwrap().unwrap();
    finish(&f.pool, &lease, None, Some("quota_observer_failed"))
        .await
        .unwrap();
    let time: i64 = sqlx::query_scalar(
        "SELECT observed_at_ms FROM provider_account_quota_snapshots WHERE provider_account_id=$1",
    )
    .bind(f.account)
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(time, observed);
    sqlx::query("UPDATE provider_account_quota_refreshes SET next_attempt_at_ms=0 WHERE provider_account_id=$1").bind(f.account).execute(&f.pool).await.unwrap();
    assert_eq!(due_accounts(&f.pool).await.unwrap(), vec![f.account]);
    f.cleanup().await;
}

#[tokio::test]
async fn quota_pg_scan_claim_race_rechecks_committed_manual_snapshot_under_lock() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    assert_eq!(due_accounts(&f.pool).await.unwrap(), vec![f.account]);
    let lease = claim(&f.pool, f.account, true).await.unwrap().unwrap();
    // Hold exactly the row that publication locks, then start an automatic claim
    // after its old scan. It must evaluate freshness after the wait, not before.
    let mut tx = f.pool.begin().await.unwrap();
    sqlx::query("SELECT provider_account_id FROM provider_account_quota_refreshes WHERE provider_account_id=$1 FOR UPDATE")
        .bind(f.account).fetch_one(&mut *tx).await.unwrap();
    let pool = f.pool.clone();
    let account = f.account;
    let automatic = tokio::spawn(async move { claim(&pool, account, false).await });
    tokio::time::sleep(Duration::from_millis(25)).await;
    let value = observation();
    persist_quota(&mut tx, f.account, &value.1, wall_now_ms())
        .await
        .unwrap();
    sqlx::query("UPDATE provider_account_quota_refreshes SET lease_token=NULL,lease_expires_at_ms=NULL WHERE provider_account_id=$1 AND lease_token=$2")
        .bind(f.account).bind(lease.lease_token).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    assert!(automatic.await.unwrap().unwrap().is_none());
    assert!(claim(&f.pool, f.account, false).await.unwrap().is_none());
    f.cleanup().await;
}

#[tokio::test]
async fn quota_pg_empty_windows_fail_with_backoff_and_mismatched_windows_are_due() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let service = f.service();
    let mut empty = observation();
    empty.1.windows.clear();
    assert!(
        service
            .refresh_codex_quota_observation(f.account, true, Duration::from_secs(1), async {
                Ok(empty)
            })
            .await
            .is_err()
    );
    let code: String = sqlx::query_scalar(
        "SELECT last_error_code FROM provider_account_quota_refreshes WHERE provider_account_id=$1",
    )
    .bind(f.account)
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(code, "quota_windows_unavailable");
    assert!(
        due_accounts(&f.pool).await.unwrap().is_empty(),
        "empty windows back off"
    );
    service
        .refresh_codex_quota_observation(f.account, true, Duration::from_secs(1), async {
            Ok(observation())
        })
        .await
        .unwrap();
    assert!(due_accounts(&f.pool).await.unwrap().is_empty());
    sqlx::query("UPDATE provider_account_quota_windows SET observed_at_ms=observed_at_ms-1 WHERE provider_account_id=$1").bind(f.account).execute(&f.pool).await.unwrap();
    assert_eq!(due_accounts(&f.pool).await.unwrap(), vec![f.account]);
    f.cleanup().await;
}

#[tokio::test]
async fn quota_pg_database_lock_has_server_deadline_and_does_not_strand_claim() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let lease = claim(&f.pool, f.account, true).await.unwrap().unwrap();
    f.expire_lease().await;
    let mut tx = f.pool.begin().await.unwrap();
    sqlx::query("SELECT provider_account_id FROM provider_account_quota_refreshes WHERE provider_account_id=$1 FOR UPDATE")
        .bind(f.account).fetch_one(&mut *tx).await.unwrap();
    let start = std::time::Instant::now();
    assert!(claim(&f.pool, f.account, true).await.is_err());
    assert!(start.elapsed() < Duration::from_secs(6));
    tx.rollback().await.unwrap();
    let epoch: i64 = sqlx::query_scalar(
        "SELECT lease_epoch FROM provider_account_quota_refreshes WHERE provider_account_id=$1",
    )
    .bind(f.account)
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(
        epoch, lease.lease_epoch,
        "timed-out query must not claim later"
    );
    let mut tx = f.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE provider_route_heads IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await
        .unwrap();
    let start = std::time::Instant::now();
    assert!(due_accounts(&f.pool).await.is_err());
    assert!(start.elapsed() < Duration::from_secs(6));
    tx.rollback().await.unwrap();
    assert_eq!(due_accounts(&f.pool).await.unwrap(), vec![f.account]);
    f.cleanup().await;
}

#[tokio::test]
async fn quota_pg_missing_scheduler_table_never_runs_observer_or_reports_success() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    assert_eq!(
        claim(&f.pool, Uuid::new_v4(), true)
            .await
            .unwrap_err()
            .error_code(),
        Some("provider_account_not_found")
    );
    sqlx::query("DROP TABLE provider_account_quota_refreshes")
        .execute(&f.pool)
        .await
        .unwrap();
    let service = f.service();
    let observed = AtomicBool::new(false);
    assert!(
        service
            .refresh_codex_quota_observation(f.account, true, Duration::from_secs(1), async {
                observed.store(true, Ordering::Relaxed);
                Ok(observation())
            })
            .await
            .is_err()
    );
    assert!(!observed.load(Ordering::Relaxed));
    assert_eq!(service.quota_refresh.snapshot().succeeded, 0);
    assert!(due_accounts(&f.pool).await.is_err());
    f.cleanup().await;
}
