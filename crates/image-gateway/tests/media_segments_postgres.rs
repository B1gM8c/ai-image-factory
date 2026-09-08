use std::{env, time::Duration};

use axum::http::StatusCode;
use gpt_image_2_gateway::{
    database::{connect_test_pool_with_search_path, run_migrations},
    input_blobs::{InputBlobKey, InputBlobRef},
    media_segments::{
        ImageSize, MediaAsset, MediaScope, PostgresSegmentStore, SegmentStatus, SegmentStore,
        SegmentTimings, SegmentWork, now_ms,
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
    let expired = store.expired_assets(10).await.unwrap();
    assert_eq!(
        expired.iter().map(|asset| asset.id).collect::<Vec<_>>(),
        vec![stale.id]
    );
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
