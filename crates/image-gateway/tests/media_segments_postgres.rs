use std::{
    env,
    time::{Duration, Instant},
};

use axum::http::StatusCode;
use gpt_image_2_gateway::{
    database::{
        connect_media_segments_pool_with_schema, connect_test_pool_with_search_path, run_migrations,
    },
    input_blobs::{InputBlobKey, InputBlobRef},
    media_segments::{
        ImageSize, MediaAsset, MediaScope, PostgresSegmentStore, SegmentStatus, SegmentStore,
        SegmentTimings, SegmentWork, SourceState, now_ms, store_operation,
    },
};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

#[tokio::test]
async fn postgres_store_contract_covers_dedup_scopes_leases_expiry_and_progress() {
    let Some(database) = TestSchema::new(12).await else {
        return;
    };
    run_migrations(&database.pool).await.unwrap();
    assert_migration_shape(&database.pool).await;
    let store = PostgresSegmentStore::new(database.pool.clone());

    let scope_a = scope("owner-a", "project-main");
    let scope_b = scope("owner-b", "project-main");
    let first = asset(&scope_a, 1, 128);
    let duplicate = MediaAsset {
        id: Uuid::new_v4(),
        blob: blob(1, 128),
        ..first.clone()
    };
    let (winner_a, winner_b) =
        tokio::join!(store.insert_asset(&first), store.insert_asset(&duplicate));
    let winner_a = winner_a.unwrap();
    let winner_b = winner_b.unwrap();
    assert_eq!(
        winner_a.id, winner_b.id,
        "same-scope digest must deduplicate"
    );
    let asset_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM media_segment_assets WHERE tenant_id = $1 AND project_id = $2 AND owner_id = $3",
    )
    .bind(&scope_a.tenant_id)
    .bind(&scope_a.project_id)
    .bind(&scope_a.owner_id)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(asset_count, 1);

    let isolated = store.insert_asset(&asset(&scope_b, 1, 128)).await.unwrap();
    assert_ne!(winner_a.id, isolated.id, "owner scope must isolate assets");
    assert!(
        store
            .get_asset(&scope_b, winner_a.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .find_asset(&scope_a, &winner_a.digest)
            .await
            .unwrap()
            .unwrap()
            .id,
        winner_a.id
    );

    let cache_a = key(100);
    let analyzer_a = key(200);
    let (queued_a, queued_duplicate) = tokio::join!(
        store.enqueue(&winner_a, &cache_a, &analyzer_a),
        store.enqueue(&winner_a, &cache_a, &analyzer_a)
    );
    let queued_a = queued_a.unwrap();
    let queued_duplicate = queued_duplicate.unwrap();
    assert_eq!(queued_a.id, queued_duplicate.id);
    let result_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM media_segment_results WHERE tenant_id = $1 AND project_id = $2 AND owner_id = $3 AND cache_key = $4",
    )
    .bind(&scope_a.tenant_id)
    .bind(&scope_a.project_id)
    .bind(&scope_a.owner_id)
    .bind(&cache_a)
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(result_count, 1);
    assert!(store.cached(&scope_b, &cache_a).await.unwrap().is_none());

    let other_scope_result = store.enqueue(&isolated, &cache_a, &key(201)).await.unwrap();
    assert_ne!(queued_a.id, other_scope_result.id);
    assert_eq!(
        store.cached(&scope_b, &cache_a).await.unwrap().unwrap().id,
        other_scope_result.id
    );

    assert!(store.claim(&key(999), 30_000).await.unwrap().is_none());
    let work = store.claim(&analyzer_a, 30_000).await.unwrap().unwrap();
    assert_eq!(work.result.id, queued_a.id);
    let wrong_fence = SegmentWork {
        id: work.id,
        lease_token: Uuid::new_v4(),
        asset: work.asset.clone(),
        result: work.result.clone(),
    };
    let mut completed = work.result.clone();
    completed.status = SegmentStatus::Completed;
    assert!(
        !store
            .finish(&wrong_fence, &completed, &SegmentTimings::default())
            .await
            .unwrap()
    );
    assert!(
        store
            .finish(&work, &completed, &SegmentTimings::default())
            .await
            .unwrap()
    );
    assert!(
        !store
            .finish(&work, &completed, &SegmentTimings::default())
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .cached(&scope_a, &cache_a)
            .await
            .unwrap()
            .unwrap()
            .status,
        SegmentStatus::Completed
    );

    assert_workerless_timeout_projection_and_fence(&store, &database.pool).await;
    assert_expired_head_does_not_block_claim(&store, &database.pool).await;
    assert_expired_leases_do_not_consume_queue_capacity(&store).await;
    assert_asset_capacity_is_project_scoped(&store).await;
    assert_expired_digest_waits_for_safe_blob_sweep(&store, &database.pool).await;

    database.cleanup().await;
}

async fn assert_workerless_timeout_projection_and_fence(
    store: &PostgresSegmentStore,
    pool: &PgPool,
) {
    let scope = scope("lease-owner", "project-lease");
    let asset = store.insert_asset(&asset(&scope, 10, 64)).await.unwrap();
    let queued = store.enqueue(&asset, &key(300), &key(301)).await.unwrap();
    let work = store.claim(&key(301), 1).await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;

    let projected = store.get(&scope, work.id).await.unwrap().unwrap();
    assert_eq!(projected.status, SegmentStatus::Failed);
    assert_eq!(projected.error.unwrap().code, "bbox_lease_expired");
    let mut late = work.result.clone();
    late.status = SegmentStatus::Completed;
    assert!(
        !store
            .finish(&work, &late, &SegmentTimings::default())
            .await
            .unwrap()
    );
    assert!(store.expire_leases().await.unwrap() >= 1);
    assert_eq!(
        store.get(&scope, work.id).await.unwrap().unwrap().status,
        SegmentStatus::Failed
    );

    let queued_timeout = store.enqueue(&asset, &key(302), &key(303)).await.unwrap();
    let queued_id = parse_result_id(&queued_timeout.id);
    sqlx::query(
        "UPDATE media_segment_results SET created_at_ms = 0, updated_at_ms = 0, queue_deadline_at_ms = 1 WHERE result_id = $1",
    )
    .bind(queued_id)
    .execute(pool)
    .await
    .unwrap();
    let projected = store.get(&scope, queued_id).await.unwrap().unwrap();
    assert_eq!(projected.status, SegmentStatus::Failed);
    assert_eq!(projected.error.unwrap().code, "bbox_queue_timeout");
    assert!(store.expire_leases().await.unwrap() >= 1);

    // The first result remains addressable by its original public identity.
    assert_eq!(queued.id, work.result.id);
}

async fn assert_expired_head_does_not_block_claim(store: &PostgresSegmentStore, pool: &PgPool) {
    let scope = scope("head-owner", "project-head");
    let stale_asset = store.insert_asset(&asset(&scope, 20, 64)).await.unwrap();
    let live_asset = store.insert_asset(&asset(&scope, 21, 64)).await.unwrap();
    let analyzer = key(400);
    let stale = store
        .enqueue(&stale_asset, &key(401), &analyzer)
        .await
        .unwrap();
    let live = store
        .enqueue(&live_asset, &key(402), &analyzer)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE media_segment_results SET created_at_ms = 0, updated_at_ms = 0 WHERE result_id = $1",
    )
    .bind(parse_result_id(&stale.id))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE media_segment_assets SET created_at_ms = 0, expires_at_ms = 1 WHERE asset_id = $1",
    )
    .bind(stale_asset.id)
    .execute(pool)
    .await
    .unwrap();

    let claimed = store.claim(&analyzer, 30_000).await.unwrap().unwrap();
    assert_eq!(claimed.result.id, live.id);
    let mut completed = claimed.result.clone();
    completed.status = SegmentStatus::Completed;
    assert!(
        store
            .finish(&claimed, &completed, &SegmentTimings::default())
            .await
            .unwrap()
    );
}

async fn assert_expired_leases_do_not_consume_queue_capacity(store: &PostgresSegmentStore) {
    let scope = scope("capacity-owner", "project-capacity");
    let asset = store.insert_asset(&asset(&scope, 30, 64)).await.unwrap();
    let analyzer = key(500);
    for offset in 0..16 {
        store
            .enqueue(&asset, &key(510 + offset), &analyzer)
            .await
            .unwrap();
    }
    let full = store
        .enqueue(&asset, &key(600), &analyzer)
        .await
        .unwrap_err();
    assert_eq!(full.status_code(), StatusCode::TOO_MANY_REQUESTS);
    for _ in 0..16 {
        assert!(store.claim(&analyzer, 1).await.unwrap().is_some());
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    store
        .enqueue(&asset, &key(600), &analyzer)
        .await
        .expect("expired processing leases must not occupy active queue capacity");
}

async fn assert_asset_capacity_is_project_scoped(store: &PostgresSegmentStore) {
    let count_scope = scope("count-owner", "project-asset-count");
    for identity in 1_000..1_064 {
        store
            .insert_asset(&asset(&count_scope, identity, 1))
            .await
            .unwrap();
    }
    let count_error = store
        .insert_asset(&asset(&count_scope, 1_064, 1))
        .await
        .unwrap_err();
    assert_eq!(count_error.status_code(), StatusCode::CONFLICT);

    let byte_scope = scope("byte-owner", "project-asset-bytes");
    for identity in 2_000..2_025 {
        store
            .insert_asset(&asset(&byte_scope, identity, 20 * 1024 * 1024))
            .await
            .unwrap();
    }
    let byte_error = store
        .insert_asset(&asset(&byte_scope, 2_025, 20 * 1024 * 1024))
        .await
        .unwrap_err();
    assert_eq!(byte_error.status_code(), StatusCode::CONFLICT);

    // A saturated project cannot consume another project's allowance.
    let independent = scope("other-owner", "project-independent");
    assert!(
        store
            .insert_asset(&asset(&independent, 1_064, 1))
            .await
            .is_ok()
    );
}

async fn assert_expired_digest_waits_for_safe_blob_sweep(
    store: &PostgresSegmentStore,
    pool: &PgPool,
) {
    let scope = scope("cleanup-owner", "project-cleanup");
    let stale = store.insert_asset(&asset(&scope, 40, 64)).await.unwrap();
    let queued = store.enqueue(&stale, &key(700), &key(701)).await.unwrap();
    sqlx::query(
        "UPDATE media_segment_assets SET created_at_ms = 0, expires_at_ms = 1 WHERE asset_id = $1",
    )
    .bind(stale.id)
    .execute(pool)
    .await
    .unwrap();
    let replacement = asset(&scope, 40, 64);
    let error = store.insert_asset(&replacement).await.unwrap_err();
    assert_eq!(error.status_code(), StatusCode::CONFLICT);
    assert!(store.expired_assets(10).await.unwrap().is_empty());

    sqlx::query(
        "UPDATE media_segment_results SET created_at_ms = 0, updated_at_ms = 0, queue_deadline_at_ms = 1 WHERE result_id = $1",
    )
    .bind(parse_result_id(&queued.id))
    .execute(pool)
    .await
    .unwrap();
    store.expire_leases().await.unwrap();
    let expired = store.claim_sources_for_release(10, false).await.unwrap();
    assert_eq!(
        expired.iter().map(|asset| asset.id).collect::<Vec<_>>(),
        vec![stale.id]
    );
    // Metadata cannot be swept before confirmed blob deletion.
    assert!(store.expired_assets(10).await.unwrap().is_empty());
    assert!(store.finish_source_release(&expired[0]).await.unwrap());
    store.delete_expired_asset(stale.id).await.unwrap();
    assert!(store.get_asset(&scope, stale.id).await.unwrap().is_none());
    assert_eq!(
        store.insert_asset(&replacement).await.unwrap().id,
        replacement.id
    );
}

async fn assert_migration_shape(pool: &PgPool) {
    let tables: (bool, bool) = sqlx::query_as(
        "SELECT to_regclass('media_segment_assets') IS NOT NULL, to_regclass('media_segment_results') IS NOT NULL",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(tables, (true, true));
    let cascade_fk: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'media_segment_results'::regclass
              AND conname = 'media_segment_results_asset_scope_fk'
              AND confdeltype = 'c'
        )
        "#,
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert!(cascade_fk);
}

#[tokio::test]
async fn source_ownership_bounds_metadata_expiry_and_concurrent_enqueue_release() {
    let Some(database) = TestSchema::new(12).await else {
        return;
    };
    sqlx::raw_sql(include_str!("../migrations/0129_media_segments.sql"))
        .execute(&database.pool)
        .await
        .unwrap();
    // Migration backfills existing registered sources as owned, never released.
    sqlx::query("INSERT INTO media_segment_assets(asset_id,tenant_id,project_id,owner_id,digest,metadata,byte_size,expires_at_ms,created_at_ms) VALUES($1,'old','old','', $2, '{}',1,2,1)")
        .bind(Uuid::new_v4()).bind(key(1)).execute(&database.pool).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/0130_media_segment_source_lifecycle.sql"
    ))
    .execute(&database.pool)
    .await
    .unwrap();
    let backfill: (String, i64, bool) = sqlx::query_as("SELECT source_state, source_release_attempt_at_ms, source_waiting_for_result FROM media_segment_assets WHERE tenant_id='old'")
        .fetch_one(&database.pool).await.unwrap();
    assert_eq!(backfill, ("retained".into(), 0, false));
    sqlx::query("DELETE FROM media_segment_assets WHERE tenant_id='old'")
        .execute(&database.pool)
        .await
        .unwrap();
    let store = PostgresSegmentStore::new(database.pool.clone());
    let capped = scope("bounded", "bounded");
    for n in 0..64 {
        store
            .insert_asset(&asset(&capped, 10_000 + n, 1))
            .await
            .unwrap();
    }
    sqlx::query("UPDATE media_segment_assets SET expires_at_ms=1,created_at_ms=0 WHERE project_id='bounded'").execute(&database.pool).await.unwrap();
    // Expired physical sources still occupy the original 64-source quota.
    assert_eq!(
        store
            .insert_asset(&asset(&capped, 11_000, 1))
            .await
            .unwrap_err()
            .status_code(),
        StatusCode::CONFLICT
    );
    let claimed = store.claim_sources_for_release(1, false).await.unwrap();
    assert_eq!(claimed.len(), 1);
    store.delete_expired_asset(claimed[0].id).await.unwrap();
    let still_owned:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM media_segment_assets WHERE asset_id=$1 AND source_state='releasing')")
        .bind(claimed[0].id).fetch_one(&database.pool).await.unwrap();
    assert!(
        still_owned,
        "metadata deletion must wait for confirmed source deletion"
    );
    assert_eq!(
        store
            .insert_asset(&asset(&capped, 11_000, 1))
            .await
            .unwrap_err()
            .status_code(),
        StatusCode::CONFLICT
    );
    let next = store.claim_sources_for_release(1, false).await.unwrap();
    assert_ne!(
        next[0].id, claimed[0].id,
        "one failed blob must not starve later cleanup"
    );
    assert!(store.finish_source_release(&claimed[0]).await.unwrap());
    assert!(!store.finish_source_release(&claimed[0]).await.unwrap());
    store
        .insert_asset(&asset(&capped, 11_000, 1))
        .await
        .unwrap();
    store.delete_expired_asset(claimed[0].id).await.unwrap();
    store.delete_expired_asset(claimed[0].id).await.unwrap();
    // Clear only these synthetic source fixtures before the race assertions.
    sqlx::query("DELETE FROM media_segment_assets WHERE project_id='bounded'")
        .execute(&database.pool)
        .await
        .unwrap();

    let byte_scope = scope("bytes", "bytes");
    for n in 0..25 {
        store
            .insert_asset(&asset(&byte_scope, 1000 + n, 20 * 1024 * 1024))
            .await
            .unwrap();
    }
    sqlx::query(
        "UPDATE media_segment_assets SET expires_at_ms=1,created_at_ms=0 WHERE project_id='bytes'",
    )
    .execute(&database.pool)
    .await
    .unwrap();
    let owned = store.claim_sources_for_release(1, false).await.unwrap();
    assert_eq!(
        store
            .insert_asset(&asset(&byte_scope, 2000, 20 * 1024 * 1024))
            .await
            .unwrap_err()
            .status_code(),
        StatusCode::CONFLICT
    );
    store.finish_source_release(&owned[0]).await.unwrap();
    store
        .insert_asset(&asset(&byte_scope, 2000, 20 * 1024 * 1024))
        .await
        .unwrap();
    let physical_bytes:i64=sqlx::query_scalar("SELECT SUM(byte_size)::BIGINT FROM media_segment_assets WHERE project_id='bytes' AND source_state <> 'released'").fetch_one(&database.pool).await.unwrap();
    assert_eq!(physical_bytes, 500 * 1024 * 1024);
    sqlx::query("DELETE FROM media_segment_assets WHERE project_id='bytes'")
        .execute(&database.pool)
        .await
        .unwrap();

    let metadata_scope = scope("metadata", "metadata");
    let template = store
        .insert_asset(&asset(&metadata_scope, 12_000, 1))
        .await
        .unwrap();
    sqlx::query("UPDATE media_segment_assets SET source_state='released' WHERE asset_id=$1")
        .bind(template.id)
        .execute(&database.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO media_segment_assets (asset_id,tenant_id,project_id,owner_id,digest,metadata,byte_size,expires_at_ms,created_at_ms,source_state) SELECT gen_random_uuid(),tenant_id,project_id,owner_id,lpad(to_hex(20000+n),64,'0'),metadata,byte_size,expires_at_ms,created_at_ms,'released' FROM media_segment_assets CROSS JOIN generate_series(1,4095) n WHERE asset_id=$1")
        .bind(template.id).execute(&database.pool).await.unwrap();
    assert_eq!(
        store
            .insert_asset(&asset(&metadata_scope, 30_000, 1))
            .await
            .unwrap_err()
            .status_code(),
        StatusCode::CONFLICT
    );
    // Restoration needs a source slot but not another metadata slot.
    let restored = store
        .insert_asset(&asset(&metadata_scope, 12_000, 1))
        .await
        .unwrap();
    assert_eq!(restored.id, template.id);
    assert_eq!(restored.source_state, SourceState::Retained);
    sqlx::query("DELETE FROM media_segment_assets WHERE project_id='metadata'")
        .execute(&database.pool)
        .await
        .unwrap();

    let race_scope = scope("race", "race");
    for n in 0..20 {
        let item = store
            .insert_asset(&asset(&race_scope, 40_000 + n, 1))
            .await
            .unwrap();
        let config = key(50_000 + n);
        store
            .enqueue(&item, &key(60_000 + n), &config)
            .await
            .unwrap();
        let work = store.claim(&config, 30_000).await.unwrap().unwrap();
        let mut done = work.result.clone();
        done.status = SegmentStatus::Completed;
        store
            .finish(&work, &done, &SegmentTimings::default())
            .await
            .unwrap();
        let second_cache = key(70_000 + n);
        let (enqueue, release) = tokio::join!(
            store.enqueue(&item, &second_cache, &config),
            store.claim_sources_for_release(256, true)
        );
        let release = release.unwrap();
        let state = store
            .get_asset(&race_scope, item.id)
            .await
            .unwrap()
            .unwrap()
            .source_state;
        match enqueue {
            Ok(pending) => {
                assert_eq!(
                    state,
                    SourceState::Retained,
                    "accepted enqueue must protect source"
                );
                assert!(!release.iter().any(|asset| asset.id == item.id));
                let work = store.claim(&config, 30_000).await.unwrap().unwrap();
                assert_eq!(pending.id, work.result.id);
                let mut done = work.result.clone();
                done.status = SegmentStatus::Completed;
                store
                    .finish(&work, &done, &SegmentTimings::default())
                    .await
                    .unwrap();
            }
            Err(error) => {
                assert_eq!(error.status_code(), StatusCode::CONFLICT);
                assert_eq!(state, SourceState::Releasing);
            }
        }
        assert_eq!(
            store
                .cached(&race_scope, &key(60_000 + n))
                .await
                .unwrap()
                .unwrap(),
            done
        );
        for source in store.claim_sources_for_release(256, true).await.unwrap() {
            store.finish_source_release(&source).await.unwrap();
        }
    }
    database.cleanup().await;
}

#[tokio::test]
async fn all_active_results_protect_source_and_worker_heartbeat_is_explicit_and_bounded() {
    let Some(database) = TestSchema::new(6).await else {
        return;
    };
    sqlx::raw_sql(include_str!("../migrations/0129_media_segments.sql"))
        .execute(&database.pool)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/0130_media_segment_source_lifecycle.sql"
    ))
    .execute(&database.pool)
    .await
    .unwrap();
    let store = PostgresSegmentStore::new(database.pool.clone());
    let scope = scope("active", "active");
    let item = store.insert_asset(&asset(&scope, 1, 128)).await.unwrap();
    let first = key(2);
    let second = key(3);
    store.enqueue(&item, &key(4), &first).await.unwrap();
    store.enqueue(&item, &key(5), &second).await.unwrap();
    let work = store.claim(&first, 30_000).await.unwrap().unwrap();
    let mut done = work.result.clone();
    done.status = SegmentStatus::Completed;
    store
        .finish(&work, &done, &SegmentTimings::default())
        .await
        .unwrap();
    assert!(
        store
            .claim_sources_for_release(32, true)
            .await
            .unwrap()
            .is_empty()
    );
    let second_work = store.claim(&second, 30_000).await.unwrap().unwrap();
    assert!(
        store
            .claim_sources_for_release(32, true)
            .await
            .unwrap()
            .is_empty()
    );
    let mut failure = second_work.result.clone();
    failure.status = SegmentStatus::Failed;
    store
        .finish(&second_work, &failure, &SegmentTimings::default())
        .await
        .unwrap();
    assert!(
        store
            .claim_sources_for_release(32, false)
            .await
            .unwrap()
            .is_empty(),
        "terminal release defaults off"
    );
    assert_eq!(
        store
            .claim_sources_for_release(32, true)
            .await
            .unwrap()
            .len(),
        1
    );
    // A failed result remains stable and does not run an automatic paid retry.
    assert_eq!(
        store.enqueue(&item, &key(5), &second).await.unwrap(),
        failure
    );
    assert!(store.claim(&second, 30_000).await.unwrap().is_none());

    let absent: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM media_segment_worker_heartbeats WHERE analyzer_key=$1 AND observed_at_ms > $2)")
        .bind(&first).bind(now_ms()-150_000).fetch_one(&database.pool).await.unwrap();
    assert!(!absent);
    store.record_worker_heartbeat(&first, true).await.unwrap();
    let record: (i64,bool) = sqlx::query_as("SELECT observed_at_ms,release_terminal_sources FROM media_segment_worker_heartbeats WHERE analyzer_key=$1")
        .bind(&first).fetch_one(&database.pool).await.unwrap();
    assert!(record.0 > now_ms() - 150_000);
    assert!(record.1);
    sqlx::query("UPDATE media_segment_worker_heartbeats SET observed_at_ms=1")
        .execute(&database.pool)
        .await
        .unwrap();
    store.record_worker_heartbeat(&second, false).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_segment_worker_heartbeats")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(
        count, 1,
        "stale configuration heartbeats must not grow forever"
    );
    database.cleanup().await;
}

#[tokio::test]
async fn media_store_timeout_pool_settings_and_exhaustion_recover() {
    let Some(database) = TestSchema::new(6).await else {
        return;
    };
    let ordinary_before = pool_timeout_settings(&database.pool).await;
    let pool = connect_media_segments_pool_with_schema(
        &env::var("TEST_DATABASE_URL").unwrap(),
        &database.name,
    )
    .await
    .unwrap();
    assert_eq!(pool.options().get_max_connections(), 3);
    assert_eq!(pool.options().get_acquire_timeout(), Duration::from_secs(1));

    let mut held = Vec::new();
    for _ in 0..3 {
        let mut connection = pool.acquire().await.unwrap();
        let settings: (String, String, String, String) = sqlx::query_as(
            "SELECT current_setting('search_path'), current_setting('statement_timeout'), current_setting('lock_timeout'), current_setting('idle_in_transaction_session_timeout')",
        ).fetch_one(&mut *connection).await.unwrap();
        assert_eq!(
            settings,
            (
                database.name.clone(),
                "4s".into(),
                "2s".into(),
                "10s".into()
            )
        );
        held.push(connection);
    }
    let started = Instant::now();
    let error = tokio::time::timeout(Duration::from_millis(1800), pool.acquire())
        .await
        .expect("pool acquisition exceeded its one-second deadline")
        .unwrap_err();
    assert!(matches!(error, sqlx::Error::PoolTimedOut));
    assert!(started.elapsed() >= Duration::from_millis(750));
    drop(held);
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(pool_timeout_settings(&database.pool).await, ordinary_before);
    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn media_store_timeout_advisory_locks_leave_no_late_asset_or_enqueue() {
    let Some((database, pool)) = media_timeout_schema().await else {
        return;
    };
    let store = PostgresSegmentStore::new(pool.clone());
    let scope = scope("timeout-advisory", &database.name);
    let existing = store.insert_asset(&asset(&scope, 80_000, 1)).await.unwrap();
    let pending = asset(&scope, 80_001, 1);
    let cache = key(80_002);
    let analyzer = key(80_003);
    let mut blocker = database.pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!(
            "media-segment-assets:{}:{}",
            scope.tenant_id, scope.project_id
        ))
        .execute(&mut *blocker)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('media-segments-queue-v1', 0))")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let (insert, enqueue) = tokio::time::timeout(Duration::from_millis(2800), async {
        tokio::join!(
            store_operation(store.insert_asset(&pending)),
            store_operation(store.enqueue(&existing, &cache, &analyzer))
        )
    })
    .await
    .expect("advisory lock waits must stop at the two-second database limit");
    assert_eq!(
        insert.unwrap_err().status_code(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        enqueue.unwrap_err().status_code(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    blocker.rollback().await.unwrap();
    // Taking the row lock fences the cancelled enqueue transaction's rollback.
    sqlx::query("SELECT asset_id FROM media_segment_assets WHERE asset_id=$1 FOR UPDATE")
        .bind(existing.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        store
            .find_asset(&scope, &pending.digest)
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.cached(&scope, &cache).await.unwrap().is_none());
    store.insert_asset(&pending).await.unwrap();
    let queued = store.enqueue(&existing, &cache, &analyzer).await.unwrap();
    assert_eq!(
        store
            .claim(&analyzer, 30_000)
            .await
            .unwrap()
            .unwrap()
            .result
            .id,
        queued.id
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM media_segment_results")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "a timed-out enqueue must not commit later"
    );
    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn media_store_timeout_table_locks_do_not_confirm_source_release() {
    let Some((database, pool)) = media_timeout_schema().await else {
        return;
    };
    let store = PostgresSegmentStore::new(pool.clone());
    let scope = scope("timeout-table", &database.name);
    let source = store.insert_asset(&asset(&scope, 81_000, 1)).await.unwrap();
    sqlx::query("UPDATE media_segment_assets SET source_state='releasing' WHERE asset_id=$1")
        .bind(source.id)
        .execute(&database.pool)
        .await
        .unwrap();
    let mut blocker = database.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE media_segment_assets IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let (read, release) = tokio::time::timeout(Duration::from_millis(2800), async {
        tokio::join!(
            store_operation(store.get_asset(&scope, source.id)),
            store_operation(store.finish_source_release(&source))
        )
    })
    .await
    .expect("table read and write lock waits must be bounded");
    assert_eq!(
        read.unwrap_err().status_code(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        release.unwrap_err().status_code(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    blocker.rollback().await.unwrap();
    assert_eq!(
        store
            .get_asset(&scope, source.id)
            .await
            .unwrap()
            .unwrap()
            .source_state,
        SourceState::Releasing
    );
    assert!(store.finish_source_release(&source).await.unwrap());
    assert!(!store.finish_source_release(&source).await.unwrap());
    pool.close().await;
    database.cleanup().await;
}

#[tokio::test]
async fn media_store_timeout_statement_and_app_deadline_roll_back_and_recover() {
    let Some((database, pool)) = media_timeout_schema().await else {
        return;
    };
    let store = PostgresSegmentStore::new(pool.clone());
    let scope = scope("timeout-cancel", &database.name);
    let source = store.insert_asset(&asset(&scope, 82_000, 1)).await.unwrap();
    sqlx::query("UPDATE media_segment_assets SET source_state='releasing' WHERE asset_id=$1")
        .bind(source.id)
        .execute(&database.pool)
        .await
        .unwrap();
    let statement_pool = pool.clone();
    let statement_timeout = tokio::spawn(async move {
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            sqlx::query("SELECT pg_sleep(10)").execute(&statement_pool),
        )
        .await
        .expect("statement_timeout did not cancel the query")
        .unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("57014")
        );
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT 1")
                .fetch_one(&statement_pool)
                .await
                .unwrap(),
            1
        );
    });
    let heartbeat = key(82_001);
    let started = Instant::now();
    let error = tokio::time::timeout(Duration::from_secs(6), store_operation(async {
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("UPDATE media_segment_assets SET source_state='released' WHERE asset_id=$1")
            .bind(source.id).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO media_segment_worker_heartbeats(analyzer_key,observed_at_ms,release_terminal_sources) VALUES($1,$2,true)")
            .bind(&heartbeat).bind(now_ms()).execute(&mut *tx).await.unwrap();
        // Every statement is below 4s, but the transaction exceeds the 5s app deadline.
        for _ in 0..8 {
            sqlx::query("SELECT pg_sleep(1)").execute(&mut *tx).await.unwrap();
        }
        tx.commit().await.unwrap();
        Ok(())
    })).await.expect("store operation exceeded its five-second app deadline").unwrap_err();
    assert_eq!(error.status_code(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(started.elapsed() >= Duration::from_millis(4500));
    statement_timeout.await.unwrap();
    let state: String = sqlx::query_scalar(
        "SELECT source_state FROM media_segment_assets WHERE asset_id=$1 FOR UPDATE",
    )
    .bind(source.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        state, "releasing",
        "timed-out transaction must not confirm source deletion"
    );
    assert!(store.worker_heartbeat(&heartbeat).await.unwrap().is_none());

    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let cancelled_pool = pool.clone();
    let source_id = source.id;
    let cancelled = tokio::spawn(async move {
        store_operation(async {
            let mut tx = cancelled_pool.begin().await.unwrap();
            sqlx::query(
                "UPDATE media_segment_assets SET source_state='released' WHERE asset_id=$1",
            )
            .bind(source_id)
            .execute(&mut *tx)
            .await
            .unwrap();
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
            started_tx.send(pid).unwrap();
            sqlx::query("SELECT pg_sleep(10)")
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            Ok(())
        })
        .await
    });
    let pid = started_rx.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let sleeping: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='PgSleep')")
                .bind(pid).fetch_one(&database.pool).await.unwrap();
            if sleeping { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("cancellation fixture never started its PostgreSQL query");
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    let state: String = tokio::time::timeout(
        Duration::from_secs(5),
        sqlx::query_scalar(
            "SELECT source_state FROM media_segment_assets WHERE asset_id=$1 FOR UPDATE",
        )
        .bind(source.id)
        .fetch_one(&database.pool),
    )
    .await
    .expect("cancelled transaction did not release its row lock")
    .unwrap();
    assert_eq!(state, "releasing");
    assert!(store.finish_source_release(&source).await.unwrap());
    store
        .record_worker_heartbeat(&heartbeat, true)
        .await
        .unwrap();
    assert!(store.worker_heartbeat(&heartbeat).await.unwrap().is_some());
    pool.close().await;
    database.cleanup().await;
}

async fn pool_timeout_settings(pool: &PgPool) -> (String, String, String, String) {
    sqlx::query_as("SELECT current_setting('search_path'), current_setting('statement_timeout'), current_setting('lock_timeout'), current_setting('idle_in_transaction_session_timeout')")
        .fetch_one(pool).await.unwrap()
}

async fn media_timeout_schema() -> Option<(TestSchema, PgPool)> {
    let database = TestSchema::new(6).await?;
    sqlx::raw_sql(include_str!("../migrations/0129_media_segments.sql"))
        .execute(&database.pool)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/0130_media_segment_source_lifecycle.sql"
    ))
    .execute(&database.pool)
    .await
    .unwrap();
    let pool = connect_media_segments_pool_with_schema(
        &env::var("TEST_DATABASE_URL").unwrap(),
        &database.name,
    )
    .await
    .unwrap();
    Some((database, pool))
}

fn scope(owner_id: &str, project_id: &str) -> MediaScope {
    MediaScope {
        tenant_id: "tenant-test".into(),
        project_id: project_id.into(),
        owner_id: owner_id.into(),
    }
}

fn asset(scope: &MediaScope, identity: u64, byte_size: u64) -> MediaAsset {
    MediaAsset {
        id: Uuid::new_v4(),
        scope: scope.clone(),
        digest: key(identity),
        image: ImageSize {
            width: 100,
            height: 80,
            coordinate_system: "pixel_xyxy".into(),
        },
        blob: blob(identity, byte_size),
        source_state: SourceState::Retained,
        expires_at_ms: now_ms() + 60_000,
    }
}

fn blob(identity: u64, byte_size: u64) -> InputBlobRef {
    let id = Uuid::new_v4();
    InputBlobRef {
        key: InputBlobKey {
            admission_session_id: id,
            input_id: id,
        },
        storage_backend: "test".into(),
        object_key: format!("test/{identity}"),
        sha256_hex: key(identity),
        byte_size,
    }
}

fn key(identity: u64) -> String {
    format!("{identity:064x}")
}

fn parse_result_id(value: &str) -> Uuid {
    Uuid::parse_str(value.strip_prefix("seg_").unwrap()).unwrap()
}

struct TestSchema {
    name: String,
    pool: PgPool,
}

impl TestSchema {
    async fn new(max_connections: u32) -> Option<Self> {
        let Some(database_url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                panic!("TEST_DATABASE_URL must be set in CI");
            }
            eprintln!("skipping media segments PostgreSQL test: TEST_DATABASE_URL is not set");
            return None;
        };
        let name = format!("media_segments_test_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, max_connections, &name)
            .await
            .unwrap();
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            database_name.to_ascii_lowercase().contains("test"),
            "refusing schema DDL in non-test database {database_name:?}"
        );
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .unwrap();
        Some(Self { name, pool })
    }

    async fn cleanup(self) {
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.name
        )))
        .execute(&self.pool)
        .await
        .unwrap();
        self.pool.close().await;
    }
}
