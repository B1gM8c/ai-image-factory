use std::collections::{BTreeSet, HashMap, HashSet};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    PriceBookVersionRow, PriceComponentRow, component_rows, insert_version_row, now_ms,
    replace_components, store_unavailable, validate_draft,
};
use crate::{
    ImageGatewayError,
    pricing::{
        ApplyOfficialPriceSnapshotRequest, OfficialPriceCatalogs, OfficialPriceComponentDiffView,
        OfficialPriceSnapshotApplicationView, OfficialPriceSnapshotDiffView,
        OfficialPriceSnapshotPreview, OfficialPriceSnapshotSummary, OfficialPriceSyncRunSummary,
        PriceBookVersionView, PriceComponentDraft,
        official_catalog::{self, OfficialPriceCatalog, OfficialPriceItem},
    },
};

#[derive(FromRow)]
struct SnapshotRow {
    snapshot_id: Uuid,
    catalog_key: String,
    source_provider_id: String,
    currency: String,
    source_url: String,
    source_checked_at_ms: i64,
    source_revision: Option<String>,
    parser_version: String,
    content_sha256: String,
    state: String,
    item_count: i32,
    normalized_payload: Value,
    created_by_user_id: Uuid,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(FromRow)]
struct ApplicationRow {
    item_key: String,
    action: String,
    price_book_id: Uuid,
    price_book_version_id: Uuid,
    applied_by_user_id: Uuid,
    applied_at_ms: i64,
}

#[derive(Clone, FromRow)]
struct ExistingBookRow {
    price_book_id: Uuid,
    price_book_key: String,
    purpose: String,
    scope_type: String,
    organization_id: Option<String>,
    project_id: Option<String>,
    provider_id: Option<String>,
    currency: String,
    state: String,
}

#[derive(FromRow)]
struct SyncRunRow {
    sync_run_id: Uuid,
    catalog_key: String,
    source_provider_id: String,
    retrieval_method: String,
    parser_version: String,
    source_checked_at_ms: i64,
    source_revision: Option<String>,
    evidence_sha256: String,
    normalized_content_sha256: Option<String>,
    state: String,
    previous_snapshot_id: Option<Uuid>,
    snapshot_id: Option<Uuid>,
    failure_code: Option<String>,
    initiated_by_user_id: Uuid,
    created_at_ms: i64,
    completed_at_ms: i64,
}

pub(super) async fn catalogs(pool: &PgPool) -> Result<OfficialPriceCatalogs, ImageGatewayError> {
    let latest_runs = latest_sync_runs(pool).await?;
    let mut descriptors = official_catalog::descriptors();
    for descriptor in &mut descriptors {
        descriptor.latest_sync_run = latest_runs.get(&descriptor.catalog_key).cloned();
    }
    Ok(OfficialPriceCatalogs {
        as_of_ms: now_ms()?,
        catalogs: descriptors,
    })
}

pub(super) async fn observe_catalog(
    pool: &PgPool,
    catalog_key: &str,
    actor_user_id: Uuid,
    actor_session_id: Uuid,
) -> Result<OfficialPriceSnapshotPreview, ImageGatewayError> {
    let catalog = official_catalog::catalog(catalog_key).ok_or_else(|| {
        ImageGatewayError::invalid_request(
            "Official pricing catalog is unavailable or has not been verified",
            Some("catalog_key".to_string()),
            "official_pricing_catalog_unavailable",
        )
    })?;
    validate_catalog(&catalog)?;
    let payload = serde_json::to_value(&catalog).map_err(|_| {
        ImageGatewayError::service_unavailable("Official pricing catalog could not be encoded")
    })?;
    let normalized_payload = serde_json::to_vec(&payload).map_err(|_| {
        ImageGatewayError::service_unavailable("Official pricing catalog could not be hashed")
    })?;
    let content_sha256 = hex::encode(Sha256::digest(&normalized_payload));
    let now = now_ms()?;
    let snapshot_id = Uuid::new_v4();
    let sync_run_id = Uuid::new_v4();
    let evidence_metadata = json!({
        "catalog_key": catalog.catalog_key,
        "source_provider_id": catalog.source_provider_id,
        "source_url": catalog.source_url,
        "source_checked_at_ms": catalog.source_checked_at_ms,
        "source_revision": catalog.source_revision,
        "parser_version": catalog.parser_version,
        "retrieval_method": "curated_manifest",
        "normalized_content_sha256": content_sha256,
    });
    let evidence_sha256 = hex::encode(Sha256::digest(
        serde_json::to_vec(&evidence_metadata).map_err(|_| {
            ImageGatewayError::service_unavailable(
                "Official pricing source evidence could not be hashed",
            )
        })?,
    ));
    let mut transaction = pool.begin().await.map_err(store_unavailable)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(&catalog.catalog_key)
        .execute(&mut *transaction)
        .await
        .map_err(store_unavailable)?;
    let previous = sqlx::query_as::<_, (Uuid, String)>(
        r#"
        SELECT snapshot_id, content_sha256
        FROM pricing_source_snapshots
        WHERE catalog_key = $1
        ORDER BY created_at_ms DESC, snapshot_id DESC
        LIMIT 1
        "#,
    )
    .bind(&catalog.catalog_key)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(store_unavailable)?;
    let inserted = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO pricing_source_snapshots (
            snapshot_id, catalog_key, source_provider_id, currency,
            source_url, source_checked_at_ms, source_revision,
            parser_version, content_sha256, state, item_count,
            normalized_payload, created_by_user_id, created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, 'observed', $10,
            $11, $12, $13, $13
        )
        ON CONFLICT (catalog_key, content_sha256) DO NOTHING
        RETURNING snapshot_id
        "#,
    )
    .bind(snapshot_id)
    .bind(&catalog.catalog_key)
    .bind(&catalog.source_provider_id)
    .bind(&catalog.currency)
    .bind(&catalog.source_url)
    .bind(catalog.source_checked_at_ms)
    .bind(&catalog.source_revision)
    .bind(&catalog.parser_version)
    .bind(&content_sha256)
    .bind(i32::try_from(catalog.items.len()).map_err(|_| invalid_catalog())?)
    .bind(payload)
    .bind(actor_user_id)
    .bind(now)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(store_unavailable)?;

    let resolved_snapshot_id = if let Some(inserted_id) = inserted {
        inserted_id
    } else {
        sqlx::query_scalar(
            r#"
            SELECT snapshot_id
            FROM pricing_source_snapshots
            WHERE catalog_key = $1 AND content_sha256 = $2
            "#,
        )
        .bind(&catalog.catalog_key)
        .bind(&content_sha256)
        .fetch_one(&mut *transaction)
        .await
        .map_err(store_unavailable)?
    };
    let changed = previous
        .as_ref()
        .is_none_or(|(_, previous_hash)| previous_hash != &content_sha256);
    let previous_snapshot_id = previous
        .filter(|(_, previous_hash)| previous_hash != &content_sha256)
        .map(|(previous_id, _)| previous_id);
    let sync_state = if changed { "changed" } else { "unchanged" };
    sqlx::query(
        r#"
        INSERT INTO pricing_source_sync_runs (
            sync_run_id, catalog_key, source_provider_id, source_url,
            retrieval_method, parser_version, source_checked_at_ms,
            source_revision, evidence_sha256, normalized_content_sha256,
            state, previous_snapshot_id, snapshot_id, failure_code,
            evidence_metadata, initiated_by_user_id, created_at_ms,
            completed_at_ms
        )
        VALUES (
            $1, $2, $3, $4, 'curated_manifest', $5, $6, $7, $8, $9,
            $10, $11, $12, NULL, $13, $14, $15, $15
        )
        "#,
    )
    .bind(sync_run_id)
    .bind(&catalog.catalog_key)
    .bind(&catalog.source_provider_id)
    .bind(&catalog.source_url)
    .bind(&catalog.parser_version)
    .bind(catalog.source_checked_at_ms)
    .bind(&catalog.source_revision)
    .bind(&evidence_sha256)
    .bind(&content_sha256)
    .bind(sync_state)
    .bind(previous_snapshot_id)
    .bind(resolved_snapshot_id)
    .bind(evidence_metadata)
    .bind(actor_user_id)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .map_err(store_unavailable)?;
    insert_audit_event(
        &mut transaction,
        actor_user_id,
        actor_session_id,
        "pricing.official_source.sync",
        resolved_snapshot_id,
        json!({
            "sync_run_id": sync_run_id,
            "catalog_key": catalog.catalog_key,
            "source_provider_id": catalog.source_provider_id,
            "retrieval_method": "curated_manifest",
            "content_sha256": content_sha256,
            "evidence_sha256": evidence_sha256,
            "sync_state": sync_state,
            "previous_snapshot_id": previous_snapshot_id,
            "item_count": catalog.items.len(),
        }),
        now,
    )
    .await?;
    transaction.commit().await.map_err(store_unavailable)?;
    preview(pool, resolved_snapshot_id, Some(sync_run_id)).await
}

pub(super) async fn apply_snapshot(
    pool: &PgPool,
    snapshot_id: Uuid,
    request: ApplyOfficialPriceSnapshotRequest,
    actor_user_id: Uuid,
    actor_session_id: Uuid,
) -> Result<OfficialPriceSnapshotPreview, ImageGatewayError> {
    let selected = validate_selection(&request.item_keys)?;
    let now = now_ms()?;
    let mut transaction = pool.begin().await.map_err(store_unavailable)?;
    let snapshot = snapshot_row_in_transaction(&mut transaction, snapshot_id).await?;
    if snapshot.state == "rejected" {
        return Err(ImageGatewayError::conflict(
            "Rejected official pricing snapshots cannot be applied",
            Some("snapshot_id".to_string()),
            "official_pricing_snapshot_rejected",
        ));
    }
    let catalog = decode_catalog(&snapshot)?;
    let known_items = catalog
        .items
        .iter()
        .map(|item| item.item_key.as_str())
        .collect::<HashSet<_>>();
    if selected
        .iter()
        .any(|item_key| !known_items.contains(item_key.as_str()))
    {
        return Err(ImageGatewayError::invalid_request(
            "One or more selected official pricing items do not exist in the snapshot",
            Some("item_keys".to_string()),
            "invalid_official_pricing_selection",
        ));
    }

    let mut actions = Vec::new();
    for item in catalog
        .items
        .iter()
        .filter(|item| selected.contains(&item.item_key))
    {
        if application_in_transaction(&mut transaction, snapshot_id, &item.item_key)
            .await?
            .is_some()
        {
            continue;
        }
        actions.push(
            apply_item(
                &mut transaction,
                &catalog,
                item,
                snapshot_id,
                actor_user_id,
                now,
            )
            .await?,
        );
    }

    let applied_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pricing_source_snapshot_applications WHERE snapshot_id = $1",
    )
    .bind(snapshot_id)
    .fetch_one(&mut *transaction)
    .await
    .map_err(store_unavailable)?;
    let state = if applied_count == i64::from(snapshot.item_count) {
        "applied"
    } else {
        "partially_applied"
    };
    sqlx::query(
        "UPDATE pricing_source_snapshots SET state = $2, updated_at_ms = $3 WHERE snapshot_id = $1",
    )
    .bind(snapshot_id)
    .bind(state)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .map_err(store_unavailable)?;
    insert_audit_event(
        &mut transaction,
        actor_user_id,
        actor_session_id,
        "pricing.official_snapshot.apply",
        snapshot_id,
        json!({
            "item_keys": selected,
            "new_applications": actions,
            "snapshot_state": state,
        }),
        now,
    )
    .await?;
    transaction.commit().await.map_err(store_unavailable)?;
    preview(pool, snapshot_id, None).await
}

async fn apply_item(
    transaction: &mut Transaction<'_, Postgres>,
    catalog: &OfficialPriceCatalog,
    item: &OfficialPriceItem,
    snapshot_id: Uuid,
    actor_user_id: Uuid,
    now: i64,
) -> Result<String, ImageGatewayError> {
    let existing = book_in_transaction(transaction, &item.price_book_key).await?;
    let price_book_id = if let Some(book) = existing {
        ensure_book_compatible(&book, catalog, item)?;
        book.price_book_id
    } else {
        let price_book_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO price_books (
                price_book_id, price_book_key, display_name, purpose,
                scope_type, organization_id, project_id, provider_id,
                currency, state, control_version, created_at_ms, updated_at_ms
            )
            VALUES (
                $1, $2, $3, 'provider_benchmark', 'platform',
                NULL, NULL, $4, $5, 'active', 1, $6, $6
            )
            "#,
        )
        .bind(price_book_id)
        .bind(&item.price_book_key)
        .bind(&item.display_name)
        .bind(&item.target_provider_id)
        .bind(&catalog.currency)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(store_unavailable)?;
        price_book_id
    };

    let draft = catalog.draft(item);
    let latest = latest_version_in_transaction(transaction, price_book_id).await?;
    let (price_book_version_id, action) = match latest {
        Some(version) if version_matches(&version, &draft) && version.state == "active" => {
            (version.price_book_version_id, "linked_active")
        }
        Some(version) if version_matches(&version, &draft) && version.state == "draft" => {
            (version.price_book_version_id, "linked_draft")
        }
        _ => {
            let version_number: i32 = sqlx::query_scalar(
                r#"
                SELECT COALESCE(MAX(version), 0)::INTEGER + 1
                FROM price_book_versions
                WHERE price_book_id = $1
                "#,
            )
            .bind(price_book_id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(store_unavailable)?;
            let version_id = Uuid::new_v4();
            insert_version_row(
                transaction,
                version_id,
                price_book_id,
                version_number,
                &draft,
            )
            .await?;
            replace_components(transaction, version_id, &draft.components).await?;
            (version_id, "created_draft")
        }
    };

    sqlx::query(
        r#"
        INSERT INTO pricing_source_snapshot_applications (
            snapshot_id, item_key, price_book_id, price_book_version_id,
            action, applied_by_user_id, applied_at_ms
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(snapshot_id)
    .bind(&item.item_key)
    .bind(price_book_id)
    .bind(price_book_version_id)
    .bind(action)
    .bind(actor_user_id)
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(store_unavailable)?;
    Ok(action.to_string())
}

async fn preview(
    pool: &PgPool,
    snapshot_id: Uuid,
    sync_run_id: Option<Uuid>,
) -> Result<OfficialPriceSnapshotPreview, ImageGatewayError> {
    let snapshot = snapshot_row(pool, snapshot_id).await?;
    let catalog = decode_catalog(&snapshot)?;
    let selected_sync_run = match sync_run_id {
        Some(sync_run_id) => sync_run(pool, sync_run_id).await?,
        None => latest_sync_run_for_snapshot(pool, snapshot_id).await?,
    };
    let previous_catalog = match selected_sync_run
        .as_ref()
        .and_then(|run| run.previous_snapshot_id)
    {
        Some(previous_snapshot_id) => Some(decode_catalog(
            &snapshot_row(pool, previous_snapshot_id).await?,
        )?),
        None => None,
    };
    let current_item_keys = catalog
        .items
        .iter()
        .map(|item| item.item_key.as_str())
        .collect::<HashSet<_>>();
    let removed_items = previous_catalog
        .as_ref()
        .map(|previous| {
            previous
                .items
                .iter()
                .filter(|item| !current_item_keys.contains(item.item_key.as_str()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let books = books_by_keys(
        pool,
        &catalog
            .items
            .iter()
            .chain(removed_items.iter().copied())
            .map(|item| item.price_book_key.clone())
            .collect::<Vec<_>>(),
    )
    .await?;
    let versions = latest_versions(
        pool,
        &books
            .values()
            .map(|book| book.price_book_id)
            .collect::<Vec<_>>(),
    )
    .await?;
    let mut differences = Vec::with_capacity(catalog.items.len());
    for item in &catalog.items {
        differences.push(diff_item(&catalog, item, &books, &versions));
    }
    for item in removed_items {
        let book = books.get(&item.price_book_key);
        let version = book.and_then(|book| versions.get(&book.price_book_id));
        differences.push(removed_diff_view(item, book, version));
    }
    let applications = sqlx::query_as::<_, ApplicationRow>(
        r#"
        SELECT item_key, action, price_book_id, price_book_version_id,
               applied_by_user_id, applied_at_ms
        FROM pricing_source_snapshot_applications
        WHERE snapshot_id = $1
        ORDER BY item_key
        "#,
    )
    .bind(snapshot_id)
    .fetch_all(pool)
    .await
    .map_err(store_unavailable)?
    .into_iter()
    .map(ApplicationRow::into_view)
    .collect();
    Ok(OfficialPriceSnapshotPreview {
        sync_run: selected_sync_run,
        snapshot: snapshot.into_summary(),
        differences,
        applications,
    })
}

fn removed_diff_view(
    item: &OfficialPriceItem,
    book: Option<&ExistingBookRow>,
    version: Option<&PriceBookVersionView>,
) -> OfficialPriceSnapshotDiffView {
    OfficialPriceSnapshotDiffView {
        item_key: item.item_key.clone(),
        display_name: item.display_name.clone(),
        public_model_id: item.public_model_id.clone(),
        media_kind: item.media_kind.clone(),
        target_provider_id: item.target_provider_id.clone(),
        component_count: item.components.len(),
        status: "removed".to_string(),
        price_book_id: book.map(|book| book.price_book_id),
        price_book_version_id: version.map(|version| version.price_book_version_id),
        existing_version: version.map(|version| version.version),
        existing_state: version.map(|version| version.state.clone()),
        component_differences: item
            .components
            .iter()
            .cloned()
            .map(|component| OfficialPriceComponentDiffView {
                component_key: component.component_key.clone(),
                status: "removed".to_string(),
                previous: Some(component),
                observed: None,
            })
            .collect(),
    }
}

fn diff_item(
    catalog: &OfficialPriceCatalog,
    item: &OfficialPriceItem,
    books: &HashMap<String, ExistingBookRow>,
    versions: &HashMap<Uuid, PriceBookVersionView>,
) -> OfficialPriceSnapshotDiffView {
    let book = books.get(&item.price_book_key);
    let Some(book) = book else {
        return diff_view(item, "new", None, None);
    };
    if ensure_book_compatible(&book, catalog, item).is_err() {
        return diff_view(item, "conflict", Some(book), None);
    }
    let latest = versions.get(&book.price_book_id);
    let status = match latest {
        None => "new",
        Some(version) if version_matches(version, &catalog.draft(item)) => {
            match version.state.as_str() {
                "active" => "unchanged",
                "draft" => "draft_exists",
                _ => "changed",
            }
        }
        Some(_) => "changed",
    };
    diff_view(item, status, Some(book), latest)
}

fn diff_view(
    item: &OfficialPriceItem,
    status: &str,
    book: Option<&ExistingBookRow>,
    version: Option<&PriceBookVersionView>,
) -> OfficialPriceSnapshotDiffView {
    let observed = item
        .components
        .iter()
        .cloned()
        .map(|component| (component.component_key.clone(), component))
        .collect::<HashMap<_, _>>();
    let previous = version
        .map(|version| {
            version
                .components
                .iter()
                .map(|component| {
                    (
                        component.component_key.clone(),
                        PriceComponentDraft {
                            component_key: component.component_key.clone(),
                            metric: component.metric.clone(),
                            unit: component.unit.clone(),
                            unit_size: component.unit_size.clone(),
                            unit_price_micros: component.unit_price_micros.clone(),
                            outcome: component.outcome.clone(),
                            quantity_source: component.quantity_source.clone(),
                            required_confidence: component.required_confidence.clone(),
                            rounding_mode: component.rounding_mode.clone(),
                            dimensions: component.dimensions.clone(),
                        },
                    )
                })
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();
    let component_keys = previous
        .keys()
        .chain(observed.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let component_differences = component_keys
        .into_iter()
        .map(|component_key| {
            let previous_component = previous.get(&component_key).cloned();
            let observed_component = observed.get(&component_key).cloned();
            let status = match (&previous_component, &observed_component) {
                (None, Some(_)) => "added",
                (Some(_), None) => "removed",
                (Some(previous), Some(observed)) if previous == observed => "unchanged",
                (Some(_), Some(_)) => "changed",
                (None, None) => unreachable!("component key came from one of the maps"),
            };
            OfficialPriceComponentDiffView {
                component_key,
                status: status.to_string(),
                previous: previous_component,
                observed: observed_component,
            }
        })
        .collect();
    OfficialPriceSnapshotDiffView {
        item_key: item.item_key.clone(),
        display_name: item.display_name.clone(),
        public_model_id: item.public_model_id.clone(),
        media_kind: item.media_kind.clone(),
        target_provider_id: item.target_provider_id.clone(),
        component_count: item.components.len(),
        status: status.to_string(),
        price_book_id: book.map(|book| book.price_book_id),
        price_book_version_id: version.map(|version| version.price_book_version_id),
        existing_version: version.map(|version| version.version),
        existing_state: version.map(|version| version.state.clone()),
        component_differences,
    }
}

fn version_matches(
    version: &PriceBookVersionView,
    draft: &crate::pricing::PriceBookVersionDraft,
) -> bool {
    let mut existing = version
        .components
        .iter()
        .map(|component| PriceComponentDraft {
            component_key: component.component_key.clone(),
            metric: component.metric.clone(),
            unit: component.unit.clone(),
            unit_size: component.unit_size.clone(),
            unit_price_micros: component.unit_price_micros.clone(),
            outcome: component.outcome.clone(),
            quantity_source: component.quantity_source.clone(),
            required_confidence: component.required_confidence.clone(),
            rounding_mode: component.rounding_mode.clone(),
            dimensions: component.dimensions.clone(),
        })
        .collect::<Vec<_>>();
    let mut expected = draft.components.clone();
    existing.sort_by(|left, right| left.component_key.cmp(&right.component_key));
    expected.sort_by(|left, right| left.component_key.cmp(&right.component_key));
    version.api_profile == draft.api_profile
        && version.operation == draft.operation
        && version.provider_id == draft.provider_id
        && version.provider_model_id == draft.provider_model_id
        && version.public_model_id == draft.public_model_id
        && version.media_kind == draft.media_kind
        && version.service_tier == draft.service_tier
        && version.execution_surface == draft.execution_surface
        && version.billing_mode == draft.billing_mode
        && version.is_free == draft.is_free
        && existing == expected
}

fn validate_catalog(catalog: &OfficialPriceCatalog) -> Result<(), ImageGatewayError> {
    if catalog.items.is_empty() || catalog.items.len() > 200 {
        return Err(invalid_catalog());
    }
    let mut item_keys = HashSet::with_capacity(catalog.items.len());
    let mut book_keys = HashSet::with_capacity(catalog.items.len());
    for item in &catalog.items {
        if item.item_key.trim().is_empty()
            || !item_keys.insert(item.item_key.as_str())
            || !book_keys.insert(item.price_book_key.as_str())
        {
            return Err(invalid_catalog());
        }
        let draft = catalog.draft(item);
        validate_draft(&draft)?;
        if draft.billing_mode != "published_rate"
            || draft.source_kind != "official_document"
            || draft.source_url.as_deref() != Some(catalog.source_url.as_str())
            || draft.components.is_empty()
        {
            return Err(invalid_catalog());
        }
    }
    Ok(())
}

fn validate_selection(item_keys: &[String]) -> Result<HashSet<String>, ImageGatewayError> {
    if item_keys.is_empty() || item_keys.len() > 200 {
        return Err(ImageGatewayError::invalid_request(
            "Select between 1 and 200 official pricing items",
            Some("item_keys".to_string()),
            "invalid_official_pricing_selection",
        ));
    }
    let selected = item_keys.iter().cloned().collect::<HashSet<_>>();
    if selected.len() != item_keys.len()
        || selected.iter().any(|key| {
            key.is_empty()
                || key.len() > 128
                || !key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_.:-".contains(&byte))
        })
    {
        return Err(ImageGatewayError::invalid_request(
            "Official pricing item keys must be unique valid identifiers",
            Some("item_keys".to_string()),
            "invalid_official_pricing_selection",
        ));
    }
    Ok(selected)
}

fn ensure_book_compatible(
    book: &ExistingBookRow,
    catalog: &OfficialPriceCatalog,
    item: &OfficialPriceItem,
) -> Result<(), ImageGatewayError> {
    if book.purpose == "provider_benchmark"
        && book.scope_type == "platform"
        && book.organization_id.is_none()
        && book.project_id.is_none()
        && book.provider_id.as_deref() == Some(item.target_provider_id.as_str())
        && book.currency == catalog.currency
        && book.state == "active"
    {
        return Ok(());
    }
    Err(ImageGatewayError::conflict(
        "An incompatible price book already uses this official catalog key",
        Some("price_book_key".to_string()),
        "official_pricing_price_book_conflict",
    ))
}

fn decode_catalog(snapshot: &SnapshotRow) -> Result<OfficialPriceCatalog, ImageGatewayError> {
    serde_json::from_value(snapshot.normalized_payload.clone()).map_err(|_| {
        ImageGatewayError::service_unavailable("Official pricing snapshot payload is invalid")
    })
}

async fn snapshot_row(pool: &PgPool, snapshot_id: Uuid) -> Result<SnapshotRow, ImageGatewayError> {
    sqlx::query_as::<_, SnapshotRow>(
        r#"
        SELECT snapshot_id, catalog_key, source_provider_id, currency,
               source_url, source_checked_at_ms, source_revision,
               parser_version, content_sha256, state, item_count,
               normalized_payload, created_by_user_id, created_at_ms, updated_at_ms
        FROM pricing_source_snapshots
        WHERE snapshot_id = $1
        "#,
    )
    .bind(snapshot_id)
    .fetch_optional(pool)
    .await
    .map_err(store_unavailable)?
    .ok_or_else(snapshot_not_found)
}

async fn snapshot_row_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    snapshot_id: Uuid,
) -> Result<SnapshotRow, ImageGatewayError> {
    sqlx::query_as::<_, SnapshotRow>(
        r#"
        SELECT snapshot_id, catalog_key, source_provider_id, currency,
               source_url, source_checked_at_ms, source_revision,
               parser_version, content_sha256, state, item_count,
               normalized_payload, created_by_user_id, created_at_ms, updated_at_ms
        FROM pricing_source_snapshots
        WHERE snapshot_id = $1
        FOR UPDATE
        "#,
    )
    .bind(snapshot_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(store_unavailable)?
    .ok_or_else(snapshot_not_found)
}

async fn books_by_keys(
    pool: &PgPool,
    keys: &[String],
) -> Result<HashMap<String, ExistingBookRow>, ImageGatewayError> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    Ok(sqlx::query_as::<_, ExistingBookRow>(
        r#"
        SELECT price_book_id, price_book_key, purpose, scope_type,
               organization_id, project_id, provider_id, currency, state
        FROM price_books
        WHERE price_book_key = ANY($1)
        "#,
    )
    .bind(keys)
    .fetch_all(pool)
    .await
    .map_err(store_unavailable)?
    .into_iter()
    .map(|book| (book.price_book_key.clone(), book))
    .collect())
}

async fn book_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    key: &str,
) -> Result<Option<ExistingBookRow>, ImageGatewayError> {
    sqlx::query_as::<_, ExistingBookRow>(
        r#"
        SELECT price_book_id, price_book_key, purpose, scope_type,
               organization_id, project_id, provider_id, currency, state
        FROM price_books
        WHERE price_book_key = $1
        FOR UPDATE
        "#,
    )
    .bind(key)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(store_unavailable)
}

async fn latest_versions(
    pool: &PgPool,
    price_book_ids: &[Uuid],
) -> Result<HashMap<Uuid, PriceBookVersionView>, ImageGatewayError> {
    if price_book_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = sqlx::query_as::<_, PriceBookVersionRow>(
        r#"
        SELECT DISTINCT ON (price_book_id)
               price_book_version_id, price_book_id, version, api_profile,
               operation, provider_id, provider_model_id, public_model_id,
               media_kind, service_tier, execution_surface, billing_mode,
               is_free, state, effective_from_ms, effective_until_ms,
               source_kind, source_url, source_checked_at_ms, notes,
               control_version, created_at_ms, updated_at_ms
        FROM price_book_versions
        WHERE price_book_id = ANY($1)
        ORDER BY price_book_id, version DESC
        "#,
    )
    .bind(price_book_ids)
    .fetch_all(pool)
    .await
    .map_err(store_unavailable)?;
    let version_ids = rows
        .iter()
        .map(|row| row.price_book_version_id)
        .collect::<Vec<_>>();
    let components = component_rows(pool, &version_ids).await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let book_id = row.price_book_id;
            let version_id = row.price_book_version_id;
            (
                book_id,
                row.into_view(components.get(&version_id).cloned().unwrap_or_default()),
            )
        })
        .collect())
}

async fn latest_sync_runs(
    pool: &PgPool,
) -> Result<HashMap<String, OfficialPriceSyncRunSummary>, ImageGatewayError> {
    Ok(sqlx::query_as::<_, SyncRunRow>(
        r#"
        SELECT DISTINCT ON (catalog_key)
               sync_run_id, catalog_key, source_provider_id,
               retrieval_method, parser_version, source_checked_at_ms,
               source_revision, evidence_sha256, normalized_content_sha256,
               state, previous_snapshot_id, snapshot_id, failure_code,
               initiated_by_user_id, created_at_ms, completed_at_ms
        FROM pricing_source_sync_runs
        ORDER BY catalog_key, created_at_ms DESC, sync_run_id DESC
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(store_unavailable)?
    .into_iter()
    .map(|run| (run.catalog_key.clone(), run.into_summary()))
    .collect())
}

async fn latest_sync_run_for_snapshot(
    pool: &PgPool,
    snapshot_id: Uuid,
) -> Result<Option<OfficialPriceSyncRunSummary>, ImageGatewayError> {
    Ok(sqlx::query_as::<_, SyncRunRow>(
        r#"
        SELECT sync_run_id, catalog_key, source_provider_id,
               retrieval_method, parser_version, source_checked_at_ms,
               source_revision, evidence_sha256, normalized_content_sha256,
               state, previous_snapshot_id, snapshot_id, failure_code,
               initiated_by_user_id, created_at_ms, completed_at_ms
        FROM pricing_source_sync_runs
        WHERE snapshot_id = $1
        ORDER BY created_at_ms DESC, sync_run_id DESC
        LIMIT 1
        "#,
    )
    .bind(snapshot_id)
    .fetch_optional(pool)
    .await
    .map_err(store_unavailable)?
    .map(SyncRunRow::into_summary))
}

async fn sync_run(
    pool: &PgPool,
    sync_run_id: Uuid,
) -> Result<Option<OfficialPriceSyncRunSummary>, ImageGatewayError> {
    Ok(sqlx::query_as::<_, SyncRunRow>(
        r#"
        SELECT sync_run_id, catalog_key, source_provider_id,
               retrieval_method, parser_version, source_checked_at_ms,
               source_revision, evidence_sha256, normalized_content_sha256,
               state, previous_snapshot_id, snapshot_id, failure_code,
               initiated_by_user_id, created_at_ms, completed_at_ms
        FROM pricing_source_sync_runs
        WHERE sync_run_id = $1
        "#,
    )
    .bind(sync_run_id)
    .fetch_optional(pool)
    .await
    .map_err(store_unavailable)?
    .map(SyncRunRow::into_summary))
}

async fn latest_version_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    price_book_id: Uuid,
) -> Result<Option<PriceBookVersionView>, ImageGatewayError> {
    let row = sqlx::query_as::<_, PriceBookVersionRow>(
        r#"
        SELECT price_book_version_id, price_book_id, version, api_profile,
               operation, provider_id, provider_model_id, public_model_id,
               media_kind, service_tier, execution_surface, billing_mode,
               is_free, state, effective_from_ms, effective_until_ms,
               source_kind, source_url, source_checked_at_ms, notes,
               control_version, created_at_ms, updated_at_ms
        FROM price_book_versions
        WHERE price_book_id = $1
        ORDER BY version DESC
        LIMIT 1
        FOR UPDATE
        "#,
    )
    .bind(price_book_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(store_unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let components = sqlx::query_as::<_, PriceComponentRow>(
        r#"
        SELECT price_component_id, price_book_version_id, component_key,
               metric, unit, unit_size, unit_price_micros, outcome,
               quantity_source, required_confidence, rounding_mode,
               dimensions_json, created_at_ms
        FROM price_components
        WHERE price_book_version_id = $1
        ORDER BY component_key
        "#,
    )
    .bind(row.price_book_version_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(store_unavailable)?
    .into_iter()
    .map(PriceComponentRow::into_view)
    .collect();
    Ok(Some(row.into_view(components)))
}

async fn application_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    snapshot_id: Uuid,
    item_key: &str,
) -> Result<Option<ApplicationRow>, ImageGatewayError> {
    sqlx::query_as::<_, ApplicationRow>(
        r#"
        SELECT item_key, action, price_book_id, price_book_version_id,
               applied_by_user_id, applied_at_ms
        FROM pricing_source_snapshot_applications
        WHERE snapshot_id = $1 AND item_key = $2
        "#,
    )
    .bind(snapshot_id)
    .bind(item_key)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(store_unavailable)
}

async fn insert_audit_event(
    transaction: &mut Transaction<'_, Postgres>,
    actor_user_id: Uuid,
    actor_session_id: Uuid,
    action: &str,
    snapshot_id: Uuid,
    metadata: Value,
    now: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO identity_audit_events (
            event_id, actor_user_id, session_id, request_id, action,
            resource_type, resource_id, outcome, reason_code, metadata,
            created_at_ms
        )
        VALUES (
            $1, $2, $3, NULL, $4, 'pricing_source_snapshot',
            $5, 'success', NULL, $6, $7
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(actor_user_id)
    .bind(actor_session_id)
    .bind(action)
    .bind(snapshot_id.to_string())
    .bind(metadata)
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(store_unavailable)?;
    Ok(())
}

impl SnapshotRow {
    fn into_summary(self) -> OfficialPriceSnapshotSummary {
        OfficialPriceSnapshotSummary {
            snapshot_id: self.snapshot_id,
            catalog_key: self.catalog_key,
            source_provider_id: self.source_provider_id,
            currency: self.currency,
            source_url: self.source_url,
            source_checked_at_ms: self.source_checked_at_ms,
            source_revision: self.source_revision,
            parser_version: self.parser_version,
            content_sha256: self.content_sha256,
            state: self.state,
            item_count: self.item_count,
            created_by_user_id: self.created_by_user_id,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
        }
    }
}

impl ApplicationRow {
    fn into_view(self) -> OfficialPriceSnapshotApplicationView {
        OfficialPriceSnapshotApplicationView {
            item_key: self.item_key,
            action: self.action,
            price_book_id: self.price_book_id,
            price_book_version_id: self.price_book_version_id,
            applied_by_user_id: self.applied_by_user_id,
            applied_at_ms: self.applied_at_ms,
        }
    }
}

impl SyncRunRow {
    fn into_summary(self) -> OfficialPriceSyncRunSummary {
        OfficialPriceSyncRunSummary {
            sync_run_id: self.sync_run_id,
            catalog_key: self.catalog_key,
            source_provider_id: self.source_provider_id,
            retrieval_method: self.retrieval_method,
            parser_version: self.parser_version,
            source_checked_at_ms: self.source_checked_at_ms,
            source_revision: self.source_revision,
            evidence_sha256: self.evidence_sha256,
            normalized_content_sha256: self.normalized_content_sha256,
            state: self.state,
            previous_snapshot_id: self.previous_snapshot_id,
            snapshot_id: self.snapshot_id,
            failure_code: self.failure_code,
            initiated_by_user_id: self.initiated_by_user_id,
            created_at_ms: self.created_at_ms,
            completed_at_ms: self.completed_at_ms,
        }
    }
}

fn snapshot_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Official pricing snapshot was not found",
        Some("snapshot_id".to_string()),
        "official_pricing_snapshot_not_found",
    )
}

fn invalid_catalog() -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Official pricing catalog configuration is invalid")
}
