use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{ImageGatewayError, model_routing::PublicModelRoute, usage::UsageCharge};

const RATE_WINDOW_MS: i64 = 60_000;
const TOKEN_MICROUNITS: i64 = 1_000_000;

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectModelIdentity {
    pub operation_id: String,
    pub api_profile: String,
    pub public_model_id: String,
    pub media_kind: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateProjectModelRateLimit {
    #[serde(flatten)]
    pub model: ProjectModelIdentity,
    pub request_limit_per_minute: Option<u32>,
    pub unit_limit_per_minute: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateProjectModelPolicyRequest {
    pub allowed_models: Vec<ProjectModelIdentity>,
    pub rate_limits: Vec<UpdateProjectModelRateLimit>,
    pub expected_control_version: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProjectModelRateLimitView {
    pub bucket_key: String,
    pub bucket_display_name: String,
    pub shared: bool,
    pub unit_kind: String,
    pub request_limit_per_minute: Option<u32>,
    pub unit_limit_per_minute: Option<u32>,
    pub inherited_request_ceiling_per_minute: Option<u32>,
    pub inherited_unit_ceiling_per_minute: Option<u32>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProjectModelPolicyModelView {
    #[serde(flatten)]
    pub model: ProjectModelIdentity,
    pub providers: Vec<String>,
    pub allowed: bool,
    pub rate_limit: ProjectModelRateLimitView,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProjectModelPolicyView {
    pub object: &'static str,
    pub project_id: String,
    pub organization_id: String,
    pub configured: bool,
    pub default_behavior: &'static str,
    pub models: Vec<ProjectModelPolicyModelView>,
    pub control_version: String,
    pub updated_at_ms: Option<i64>,
}

#[async_trait]
pub trait ProjectModelPolicyService: Send + Sync + 'static {
    async fn get_policy(
        &self,
        project_id: &str,
        available_models: Vec<PublicModelRoute>,
    ) -> Result<ProjectModelPolicyView, ImageGatewayError>;

    async fn update_policy(
        &self,
        project_id: &str,
        actor_user_id: Uuid,
        available_models: Vec<PublicModelRoute>,
        request: UpdateProjectModelPolicyRequest,
    ) -> Result<ProjectModelPolicyView, ImageGatewayError>;
}

#[derive(Clone)]
pub struct PostgresProjectModelPolicyService {
    pool: PgPool,
}

impl PostgresProjectModelPolicyService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ProjectModelPolicyService for PostgresProjectModelPolicyService {
    async fn get_policy(
        &self,
        project_id: &str,
        available_models: Vec<PublicModelRoute>,
    ) -> Result<ProjectModelPolicyView, ImageGatewayError> {
        validate_project_id(project_id)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let project = project_identity(&mut tx, project_id).await?;
        let policy = read_policy_row(&mut tx, project_id).await?;
        let allowed = read_allowed_models(&mut tx, project_id).await?;
        let configured_limits = read_project_rate_limits(&mut tx, project_id).await?;
        let platform_limits = read_platform_limit_members(&mut tx).await?;
        let view = build_view(
            project,
            policy,
            available_models,
            allowed,
            configured_limits,
            platform_limits,
        )?;
        tx.commit().await.map_err(unavailable)?;
        Ok(view)
    }

    async fn update_policy(
        &self,
        project_id: &str,
        actor_user_id: Uuid,
        available_models: Vec<PublicModelRoute>,
        request: UpdateProjectModelPolicyRequest,
    ) -> Result<ProjectModelPolicyView, ImageGatewayError> {
        validate_project_id(project_id)?;
        let expected_control_version = parse_control_version(&request.expected_control_version)?;
        let available = normalize_available_models(&available_models)?;
        let allowed = validate_allowed_models(&available, request.allowed_models)?;
        let now_ms = now_ms()?;

        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("project-model-policy:{project_id}"))
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        let project = project_identity(&mut tx, project_id).await?;
        let platform_limits = read_platform_limit_members(&mut tx).await?;
        let rate_limits = validate_rate_limits(&available, &platform_limits, request.rate_limits)?;
        let current = read_policy_row_for_update(&mut tx, project_id).await?;
        let next_version = match current.as_ref() {
            Some(row) if row.control_version != expected_control_version => {
                return Err(version_conflict());
            }
            None if expected_control_version != 0 => return Err(version_conflict()),
            Some(row) => row.control_version.saturating_add(1),
            None => 1,
        };

        if current.is_some() {
            sqlx::query(
                r#"
                UPDATE project_model_policies
                SET control_version = $2,
                    updated_by_user_id = $3,
                    updated_at_ms = $4
                WHERE project_id = $1
                "#,
            )
            .bind(project_id)
            .bind(next_version)
            .bind(actor_user_id)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        } else {
            sqlx::query(
                r#"
                INSERT INTO project_model_policies(
                    project_id, organization_id, control_version,
                    created_by_user_id, updated_by_user_id,
                    created_at_ms, updated_at_ms
                )
                VALUES ($1, $2, 1, $3, $3, $4, $4)
                "#,
            )
            .bind(project_id)
            .bind(&project.organization_id)
            .bind(actor_user_id)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }

        sqlx::query("DELETE FROM project_model_access_entries WHERE project_id = $1")
            .bind(project_id)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        for model in &allowed {
            sqlx::query(
                r#"
                INSERT INTO project_model_access_entries(
                    project_id, operation_id, api_profile,
                    public_model_id, media_kind, created_at_ms
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(project_id)
            .bind(&model.operation_id)
            .bind(&model.api_profile)
            .bind(&model.public_model_id)
            .bind(&model.media_kind)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }

        let existing_rate_limits = read_project_rate_limits(&mut tx, project_id).await?;
        for bucket_key in existing_rate_limits
            .keys()
            .filter(|bucket_key| !rate_limits.contains_key(*bucket_key))
        {
            sqlx::query(
                "DELETE FROM project_model_rate_limits WHERE project_id = $1 AND bucket_key = $2",
            )
            .bind(project_id)
            .bind(bucket_key)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }
        for limit in rate_limits.values() {
            sqlx::query(
                r#"
                INSERT INTO project_model_rate_limits(
                    project_id, bucket_key, unit_kind,
                    request_limit_per_minute, unit_limit_per_minute,
                    created_at_ms, updated_at_ms
                )
                VALUES ($1, $2, $3, $4, $5, $6, $6)
                ON CONFLICT (project_id, bucket_key) DO UPDATE
                SET unit_kind = EXCLUDED.unit_kind,
                    request_limit_per_minute = EXCLUDED.request_limit_per_minute,
                    unit_limit_per_minute = EXCLUDED.unit_limit_per_minute,
                    updated_at_ms = EXCLUDED.updated_at_ms
                "#,
            )
            .bind(project_id)
            .bind(&limit.bucket_key)
            .bind(&limit.unit_kind)
            .bind(limit.request_limit_per_minute.map(i64::from))
            .bind(limit.unit_limit_per_minute.map(i64::from))
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }

        insert_audit(
            &mut tx,
            actor_user_id,
            project_id,
            next_version,
            allowed.len(),
            rate_limits.len(),
            now_ms,
        )
        .await?;
        tx.commit().await.map_err(unavailable)?;

        self.get_policy(project_id, available_models).await
    }
}

#[derive(Clone, Debug, FromRow)]
struct ProjectIdentity {
    project_id: String,
    organization_id: String,
}

#[derive(Clone, Debug, FromRow)]
struct PolicyRow {
    control_version: i64,
    updated_at_ms: i64,
}

#[derive(Clone, Debug, FromRow)]
struct PlatformLimitMemberRow {
    operation_id: String,
    api_profile: String,
    public_model_id: String,
    media_kind: String,
    bucket_key: String,
    bucket_display_name: String,
    unit_kind: String,
    request_ceiling_per_minute: Option<i32>,
    unit_ceiling_per_minute: Option<i32>,
}

#[derive(Clone, Debug, FromRow)]
struct ProjectRateLimitRow {
    bucket_key: String,
    unit_kind: String,
    request_limit_per_minute: Option<i32>,
    unit_limit_per_minute: Option<i32>,
}

#[derive(Clone, Debug, FromRow)]
struct ProjectRateStateRow {
    request_tokens_microunits: Option<i64>,
    unit_tokens_microunits: Option<i64>,
    last_refill_at_ms: i64,
}

#[derive(Clone, Debug)]
struct AvailableModel {
    identity: ProjectModelIdentity,
    providers: BTreeSet<String>,
}

#[derive(Clone, Debug)]
struct EffectiveLimit {
    bucket_key: String,
    bucket_display_name: String,
    unit_kind: String,
    request_ceiling_per_minute: Option<u32>,
    unit_ceiling_per_minute: Option<u32>,
}

#[derive(Clone, Debug)]
struct ValidatedRateLimit {
    bucket_key: String,
    unit_kind: String,
    request_limit_per_minute: Option<u32>,
    unit_limit_per_minute: Option<u32>,
}

fn build_view(
    project: ProjectIdentity,
    policy: Option<PolicyRow>,
    available_models: Vec<PublicModelRoute>,
    allowed_models: BTreeSet<ProjectModelIdentity>,
    configured_limits: BTreeMap<String, ProjectRateLimitRow>,
    platform_limits: BTreeMap<ProjectModelIdentity, PlatformLimitMemberRow>,
) -> Result<ProjectModelPolicyView, ImageGatewayError> {
    let available = normalize_available_models(&available_models)?;
    let configured = policy.is_some();
    let mut limits = BTreeMap::new();
    let mut bucket_counts = BTreeMap::<String, usize>::new();
    for model in available.values() {
        let limit = effective_limit(&model.identity, &platform_limits)?;
        *bucket_counts.entry(limit.bucket_key.clone()).or_default() += 1;
        limits.insert(model.identity.clone(), limit);
    }

    let models = available
        .into_values()
        .map(|model| {
            let effective = limits
                .remove(&model.identity)
                .ok_or_else(|| ImageGatewayError::internal("model rate limit is missing"))?;
            let configured_limit = configured_limits.get(&effective.bucket_key);
            Ok(ProjectModelPolicyModelView {
                allowed: !configured || allowed_models.contains(&model.identity),
                model: model.identity,
                providers: model.providers.into_iter().collect(),
                rate_limit: ProjectModelRateLimitView {
                    shared: bucket_counts
                        .get(&effective.bucket_key)
                        .copied()
                        .unwrap_or(0)
                        > 1,
                    bucket_key: effective.bucket_key.clone(),
                    bucket_display_name: effective.bucket_display_name,
                    unit_kind: effective.unit_kind,
                    request_limit_per_minute: configured_limit
                        .and_then(|row| nonnegative_u32(row.request_limit_per_minute)),
                    unit_limit_per_minute: configured_limit
                        .and_then(|row| nonnegative_u32(row.unit_limit_per_minute)),
                    inherited_request_ceiling_per_minute: effective.request_ceiling_per_minute,
                    inherited_unit_ceiling_per_minute: effective.unit_ceiling_per_minute,
                },
            })
        })
        .collect::<Result<Vec<_>, ImageGatewayError>>()?;

    Ok(ProjectModelPolicyView {
        object: "organization.project.model_policy",
        project_id: project.project_id,
        organization_id: project.organization_id,
        configured,
        default_behavior: if configured {
            "deny_unlisted"
        } else {
            "allow_routable"
        },
        models,
        control_version: policy
            .as_ref()
            .map(|row| row.control_version)
            .unwrap_or(0)
            .to_string(),
        updated_at_ms: policy.map(|row| row.updated_at_ms),
    })
}

fn normalize_available_models(
    models: &[PublicModelRoute],
) -> Result<BTreeMap<ProjectModelIdentity, AvailableModel>, ImageGatewayError> {
    let mut normalized = BTreeMap::<ProjectModelIdentity, AvailableModel>::new();
    for model in models {
        let identity = ProjectModelIdentity {
            operation_id: model.operation_id.clone(),
            api_profile: model.api_profile.clone(),
            public_model_id: model.id.clone(),
            media_kind: model.media_kind.clone(),
        };
        validate_identity(&identity)?;
        normalized
            .entry(identity.clone())
            .or_insert_with(|| AvailableModel {
                identity,
                providers: BTreeSet::new(),
            })
            .providers
            .insert(model.provider_id.clone());
    }
    Ok(normalized)
}

fn validate_allowed_models(
    available: &BTreeMap<ProjectModelIdentity, AvailableModel>,
    requested: Vec<ProjectModelIdentity>,
) -> Result<BTreeSet<ProjectModelIdentity>, ImageGatewayError> {
    let mut allowed = BTreeSet::new();
    for model in requested {
        validate_identity(&model)?;
        if !available.contains_key(&model) {
            return Err(ImageGatewayError::invalid_request(
                format!(
                    "Model '{}' is not currently routable for this project",
                    model.public_model_id
                ),
                Some("allowed_models".to_string()),
                "invalid_project_model",
            ));
        }
        allowed.insert(model);
    }
    Ok(allowed)
}

fn validate_rate_limits(
    available: &BTreeMap<ProjectModelIdentity, AvailableModel>,
    platform_limits: &BTreeMap<ProjectModelIdentity, PlatformLimitMemberRow>,
    requested: Vec<UpdateProjectModelRateLimit>,
) -> Result<BTreeMap<String, ValidatedRateLimit>, ImageGatewayError> {
    let mut limits = BTreeMap::<String, ValidatedRateLimit>::new();
    for requested_limit in requested {
        validate_identity(&requested_limit.model)?;
        if !available.contains_key(&requested_limit.model) {
            return Err(ImageGatewayError::invalid_request(
                format!(
                    "Model '{}' is not currently routable for this project",
                    requested_limit.model.public_model_id
                ),
                Some("rate_limits".to_string()),
                "invalid_project_model",
            ));
        }
        validate_positive_limit(
            requested_limit.request_limit_per_minute,
            "request_limit_per_minute",
        )?;
        validate_positive_limit(
            requested_limit.unit_limit_per_minute,
            "unit_limit_per_minute",
        )?;
        if requested_limit.request_limit_per_minute.is_none()
            && requested_limit.unit_limit_per_minute.is_none()
        {
            continue;
        }
        let effective = effective_limit(&requested_limit.model, platform_limits)?;
        validate_ceiling(
            requested_limit.request_limit_per_minute,
            effective.request_ceiling_per_minute,
            "request_limit_per_minute",
        )?;
        validate_ceiling(
            requested_limit.unit_limit_per_minute,
            effective.unit_ceiling_per_minute,
            "unit_limit_per_minute",
        )?;
        let next = ValidatedRateLimit {
            bucket_key: effective.bucket_key.clone(),
            unit_kind: effective.unit_kind,
            request_limit_per_minute: requested_limit.request_limit_per_minute,
            unit_limit_per_minute: requested_limit.unit_limit_per_minute,
        };
        if let Some(existing) = limits.get(&effective.bucket_key)
            && (existing.request_limit_per_minute != next.request_limit_per_minute
                || existing.unit_limit_per_minute != next.unit_limit_per_minute)
        {
            return Err(ImageGatewayError::invalid_request(
                "Models sharing one rate-limit bucket must use identical limits",
                Some("rate_limits".to_string()),
                "conflicting_shared_model_limit",
            ));
        }
        limits.insert(effective.bucket_key, next);
    }
    Ok(limits)
}

fn effective_limit(
    model: &ProjectModelIdentity,
    platform_limits: &BTreeMap<ProjectModelIdentity, PlatformLimitMemberRow>,
) -> Result<EffectiveLimit, ImageGatewayError> {
    if let Some(member) = platform_limits.get(model) {
        if member.media_kind != model.media_kind {
            return Err(ImageGatewayError::internal(
                "platform model rate-limit media kind does not match",
            ));
        }
        return Ok(EffectiveLimit {
            bucket_key: member.bucket_key.clone(),
            bucket_display_name: member.bucket_display_name.clone(),
            unit_kind: member.unit_kind.clone(),
            request_ceiling_per_minute: nonnegative_u32(member.request_ceiling_per_minute),
            unit_ceiling_per_minute: nonnegative_u32(member.unit_ceiling_per_minute),
        });
    }
    Ok(EffectiveLimit {
        bucket_key: default_bucket_key(&model.api_profile, &model.public_model_id),
        bucket_display_name: model.public_model_id.clone(),
        unit_kind: unit_kind(&model.media_kind)?.to_string(),
        request_ceiling_per_minute: None,
        unit_ceiling_per_minute: None,
    })
}

pub(crate) async fn enforce_project_model_controls(
    tx: &mut Transaction<'_, Postgres>,
    charge: &UsageCharge,
    now_ms: i64,
) -> Result<(), ImageGatewayError> {
    let Some(attribution) = charge.attribution.as_ref() else {
        return Ok(());
    };
    let Some(route) = attribution.route.as_ref() else {
        return Ok(());
    };
    if attribution.project_id.trim().is_empty() {
        return Err(ImageGatewayError::internal(
            "project model attribution is incomplete",
        ));
    }

    let policy_version = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT control_version
        FROM project_model_policies
        WHERE project_id = $1
        FOR SHARE
        "#,
    )
    .bind(&attribution.project_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    if policy_version.is_some() {
        let allowed: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
              SELECT 1
              FROM project_model_access_entries access
              WHERE access.project_id = $1
                AND access.operation_id = $2
                AND access.api_profile = $3
                AND access.public_model_id = $4
                AND access.media_kind = $5
            )
            "#,
        )
        .bind(&attribution.project_id)
        .bind(&route.operation_id)
        .bind(&route.api_profile)
        .bind(&route.public_model_id)
        .bind(&route.media_kind)
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)?;
        if !allowed {
            return Err(ImageGatewayError::model_not_found(&route.public_model_id));
        }
    }

    let platform = sqlx::query_as::<_, PlatformLimitMemberRow>(
        r#"
        SELECT operation_id, api_profile, public_model_id, media_kind,
               bucket_key, bucket_display_name, unit_kind,
               request_ceiling_per_minute, unit_ceiling_per_minute
        FROM platform_model_limit_members
        WHERE operation_id = $1
          AND api_profile = $2
          AND public_model_id = $3
        "#,
    )
    .bind(&route.operation_id)
    .bind(&route.api_profile)
    .bind(&route.public_model_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    if let Some(platform) = platform.as_ref()
        && (platform.media_kind != route.media_kind
            || platform.unit_kind != unit_kind(&route.media_kind)?)
    {
        return Err(ImageGatewayError::internal(
            "platform model rate-limit media kind does not match",
        ));
    }
    let bucket_key = platform
        .as_ref()
        .map(|row| row.bucket_key.clone())
        .unwrap_or_else(|| default_bucket_key(&route.api_profile, &route.public_model_id));
    let unit_kind = platform
        .as_ref()
        .map(|row| row.unit_kind.as_str())
        .unwrap_or(unit_kind(&route.media_kind)?);

    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!(
            "project-model-rate:{}:{bucket_key}",
            attribution.project_id
        ))
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    let project_limit = sqlx::query_as::<_, ProjectRateLimitRow>(
        r#"
        SELECT bucket_key, unit_kind,
               request_limit_per_minute, unit_limit_per_minute
        FROM project_model_rate_limits
        WHERE project_id = $1 AND bucket_key = $2
        FOR SHARE
        "#,
    )
    .bind(&attribution.project_id)
    .bind(&bucket_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let limit = project_limit.unwrap_or_else(|| ProjectRateLimitRow {
        bucket_key: bucket_key.clone(),
        unit_kind: unit_kind.to_string(),
        request_limit_per_minute: platform
            .as_ref()
            .and_then(|row| row.request_ceiling_per_minute),
        unit_limit_per_minute: platform
            .as_ref()
            .and_then(|row| row.unit_ceiling_per_minute),
    });
    if limit.request_limit_per_minute.is_none() && limit.unit_limit_per_minute.is_none() {
        return Ok(());
    }
    if limit.unit_kind != unit_kind {
        return Err(ImageGatewayError::internal(
            "project model rate-limit unit kind does not match",
        ));
    }
    let Some(admission_session_id) = charge.admission_session_id else {
        return Err(ImageGatewayError::internal(
            "project model rate limit requires an admission session",
        ));
    };
    let already_counted: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
          SELECT 1
          FROM project_model_rate_admissions
          WHERE project_id = $1 AND admission_session_id = $2
        )
        "#,
    )
    .bind(&attribution.project_id)
    .bind(admission_session_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if already_counted {
        return Ok(());
    }

    let unit_count = match route.media_kind.as_str() {
        "image" => i64::from(charge.output_count),
        "video" => i64::from(charge.billable_units),
        _ => {
            return Err(ImageGatewayError::internal(
                "project model rate-limit media kind is invalid",
            ));
        }
    };

    let request_capacity = token_capacity(limit.request_limit_per_minute)?;
    let unit_capacity = token_capacity(limit.unit_limit_per_minute)?;
    sqlx::query(
        r#"
        INSERT INTO project_model_rate_states(
            project_id, bucket_key,
            request_tokens_microunits, unit_tokens_microunits,
            last_refill_at_ms, updated_at_ms
        )
        VALUES ($1, $2, $3, $4, $5, $5)
        ON CONFLICT (project_id, bucket_key) DO NOTHING
        "#,
    )
    .bind(&attribution.project_id)
    .bind(&bucket_key)
    .bind(request_capacity)
    .bind(unit_capacity)
    .bind(now_ms)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    let state = sqlx::query_as::<_, ProjectRateStateRow>(
        r#"
        SELECT request_tokens_microunits,
               unit_tokens_microunits,
               last_refill_at_ms
        FROM project_model_rate_states
        WHERE project_id = $1 AND bucket_key = $2
        FOR UPDATE
        "#,
    )
    .bind(&attribution.project_id)
    .bind(&bucket_key)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    let elapsed_ms = now_ms.saturating_sub(state.last_refill_at_ms).max(0);
    let request_tokens = refill_tokens(
        state.request_tokens_microunits,
        limit.request_limit_per_minute,
        elapsed_ms,
    )?;
    let unit_tokens = refill_tokens(
        state.unit_tokens_microunits,
        limit.unit_limit_per_minute,
        elapsed_ms,
    )?;
    let request_cost = TOKEN_MICROUNITS;
    let unit_cost = unit_count
        .checked_mul(TOKEN_MICROUNITS)
        .ok_or_else(|| ImageGatewayError::internal("project model unit cost overflow"))?;
    let request_exceeded = request_tokens.is_some_and(|tokens| tokens < request_cost);
    let unit_exceeded = unit_tokens.is_some_and(|tokens| tokens < unit_cost);
    if request_exceeded || unit_exceeded {
        let retry_after_ms = [
            retry_after_ms(request_tokens, request_cost, limit.request_limit_per_minute)?,
            retry_after_ms(unit_tokens, unit_cost, limit.unit_limit_per_minute)?,
        ]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(RATE_WINDOW_MS)
        .max(1);
        let retry_after_seconds = u64::try_from((retry_after_ms + 999) / 1_000)
            .unwrap_or(60)
            .max(1);
        return Err(ImageGatewayError::project_model_rate_limit_exceeded(
            &route.public_model_id,
            retry_after_seconds,
            nonnegative_u32(limit.request_limit_per_minute),
            nonnegative_u32(limit.unit_limit_per_minute),
            unit_kind,
            request_tokens.map(|tokens| tokens / TOKEN_MICROUNITS),
            unit_tokens.map(|tokens| tokens / TOKEN_MICROUNITS),
        ));
    }

    sqlx::query(
        r#"
        UPDATE project_model_rate_states
        SET request_tokens_microunits = $3,
            unit_tokens_microunits = $4,
            last_refill_at_ms = $5,
            updated_at_ms = $5
        WHERE project_id = $1 AND bucket_key = $2
        "#,
    )
    .bind(&attribution.project_id)
    .bind(&bucket_key)
    .bind(request_tokens.map(|tokens| tokens - request_cost))
    .bind(unit_tokens.map(|tokens| tokens - unit_cost))
    .bind(now_ms)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO project_model_rate_admissions(
            project_id, bucket_key, admission_session_id,
            request_units, unit_count, admitted_at_ms
        )
        VALUES ($1, $2, $3, 1, $4, $5)
        "#,
    )
    .bind(&attribution.project_id)
    .bind(&bucket_key)
    .bind(admission_session_id)
    .bind(unit_count)
    .bind(now_ms)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

fn token_capacity(limit: Option<i32>) -> Result<Option<i64>, ImageGatewayError> {
    limit
        .map(|value| {
            i64::from(value)
                .checked_mul(TOKEN_MICROUNITS)
                .ok_or_else(|| ImageGatewayError::internal("rate-limit capacity overflow"))
        })
        .transpose()
}

fn refill_tokens(
    current: Option<i64>,
    limit: Option<i32>,
    elapsed_ms: i64,
) -> Result<Option<i64>, ImageGatewayError> {
    let Some(capacity) = token_capacity(limit)? else {
        return Ok(None);
    };
    let current = current.unwrap_or(capacity).min(capacity);
    let added = i128::from(elapsed_ms)
        .saturating_mul(i128::from(capacity))
        .checked_div(i128::from(RATE_WINDOW_MS))
        .unwrap_or(0);
    let added = i64::try_from(added).unwrap_or(i64::MAX);
    Ok(Some(current.saturating_add(added).min(capacity)))
}

fn retry_after_ms(
    current: Option<i64>,
    cost: i64,
    limit: Option<i32>,
) -> Result<Option<i64>, ImageGatewayError> {
    let Some(capacity) = token_capacity(limit)? else {
        return Ok(None);
    };
    let current = current.unwrap_or(capacity);
    if current >= cost {
        return Ok(None);
    }
    let deficit = cost.saturating_sub(current);
    let numerator = i128::from(deficit)
        .saturating_mul(i128::from(RATE_WINDOW_MS))
        .saturating_add(i128::from(capacity).saturating_sub(1));
    let millis = numerator
        .checked_div(i128::from(capacity))
        .unwrap_or(i128::from(RATE_WINDOW_MS));
    Ok(Some(i64::try_from(millis).unwrap_or(RATE_WINDOW_MS).max(1)))
}

async fn project_identity(
    tx: &mut Transaction<'_, Postgres>,
    project_id: &str,
) -> Result<ProjectIdentity, ImageGatewayError> {
    sqlx::query_as::<_, ProjectIdentity>(
        r#"
        SELECT id AS project_id, tenant_id AS organization_id
        FROM gateway_projects
        WHERE id = $1 AND archived_at IS NULL
        "#,
    )
    .bind(project_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or_else(|| {
        ImageGatewayError::not_found(
            "Project was not found",
            Some("project_id".to_string()),
            "project_not_found",
        )
    })
}

async fn read_policy_row(
    tx: &mut Transaction<'_, Postgres>,
    project_id: &str,
) -> Result<Option<PolicyRow>, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT control_version, updated_at_ms
        FROM project_model_policies
        WHERE project_id = $1
        "#,
    )
    .bind(project_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn read_policy_row_for_update(
    tx: &mut Transaction<'_, Postgres>,
    project_id: &str,
) -> Result<Option<PolicyRow>, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT control_version, updated_at_ms
        FROM project_model_policies
        WHERE project_id = $1
        FOR UPDATE
        "#,
    )
    .bind(project_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn read_allowed_models(
    tx: &mut Transaction<'_, Postgres>,
    project_id: &str,
) -> Result<BTreeSet<ProjectModelIdentity>, ImageGatewayError> {
    sqlx::query_as::<_, AllowedModelRow>(
        r#"
        SELECT operation_id, api_profile, public_model_id, media_kind
        FROM project_model_access_entries
        WHERE project_id = $1
        ORDER BY operation_id, api_profile, public_model_id
        "#,
    )
    .bind(project_id)
    .fetch_all(&mut **tx)
    .await
    .map(|rows| rows.into_iter().map(Into::into).collect())
    .map_err(unavailable)
}

#[derive(FromRow)]
struct AllowedModelRow {
    operation_id: String,
    api_profile: String,
    public_model_id: String,
    media_kind: String,
}

impl From<AllowedModelRow> for ProjectModelIdentity {
    fn from(row: AllowedModelRow) -> Self {
        Self {
            operation_id: row.operation_id,
            api_profile: row.api_profile,
            public_model_id: row.public_model_id,
            media_kind: row.media_kind,
        }
    }
}

async fn read_project_rate_limits(
    tx: &mut Transaction<'_, Postgres>,
    project_id: &str,
) -> Result<BTreeMap<String, ProjectRateLimitRow>, ImageGatewayError> {
    sqlx::query_as::<_, ProjectRateLimitRow>(
        r#"
        SELECT bucket_key, unit_kind,
               request_limit_per_minute, unit_limit_per_minute
        FROM project_model_rate_limits
        WHERE project_id = $1
        ORDER BY bucket_key
        "#,
    )
    .bind(project_id)
    .fetch_all(&mut **tx)
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|row| (row.bucket_key.clone(), row))
            .collect()
    })
    .map_err(unavailable)
}

async fn read_platform_limit_members(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<BTreeMap<ProjectModelIdentity, PlatformLimitMemberRow>, ImageGatewayError> {
    sqlx::query_as::<_, PlatformLimitMemberRow>(
        r#"
        SELECT operation_id, api_profile, public_model_id, media_kind,
               bucket_key, bucket_display_name, unit_kind,
               request_ceiling_per_minute, unit_ceiling_per_minute
        FROM platform_model_limit_members
        ORDER BY operation_id, api_profile, public_model_id
        "#,
    )
    .fetch_all(&mut **tx)
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|row| {
                (
                    ProjectModelIdentity {
                        operation_id: row.operation_id.clone(),
                        api_profile: row.api_profile.clone(),
                        public_model_id: row.public_model_id.clone(),
                        media_kind: row.media_kind.clone(),
                    },
                    row,
                )
            })
            .collect()
    })
    .map_err(unavailable)
}

async fn insert_audit(
    tx: &mut Transaction<'_, Postgres>,
    actor_user_id: Uuid,
    project_id: &str,
    control_version: i64,
    allowed_model_count: usize,
    rate_limit_count: usize,
    now_ms: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO identity_audit_events(
            event_id, actor_user_id, action, resource_type,
            resource_id, outcome, metadata, created_at_ms
        )
        VALUES (
            $1, $2, 'project.model_policy.updated', 'project',
            $3, 'success', $4, $5
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(actor_user_id)
    .bind(project_id)
    .bind(serde_json::json!({
        "control_version": control_version,
        "allowed_model_count": allowed_model_count,
        "rate_limit_count": rate_limit_count,
    }))
    .bind(now_ms)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

fn validate_identity(model: &ProjectModelIdentity) -> Result<(), ImageGatewayError> {
    let valid_text = |value: &str, max: usize| {
        !value.trim().is_empty() && value.len() <= max && !value.chars().any(char::is_control)
    };
    if !valid_text(&model.operation_id, 128)
        || !valid_text(&model.api_profile, 128)
        || !valid_text(&model.public_model_id, 255)
        || !matches!(model.media_kind.as_str(), "image" | "video")
    {
        return Err(ImageGatewayError::invalid_request(
            "Project model identity is invalid",
            Some("allowed_models".to_string()),
            "invalid_project_model",
        ));
    }
    Ok(())
}

fn validate_positive_limit(
    value: Option<u32>,
    param: &'static str,
) -> Result<(), ImageGatewayError> {
    if value.is_some_and(|value| value == 0 || i32::try_from(value).is_err()) {
        return Err(ImageGatewayError::invalid_request(
            format!("{param} must be a positive 32-bit integer"),
            Some(param.to_string()),
            "invalid_rate_limit",
        ));
    }
    Ok(())
}

fn validate_ceiling(
    value: Option<u32>,
    ceiling: Option<u32>,
    param: &'static str,
) -> Result<(), ImageGatewayError> {
    if matches!((value, ceiling), (Some(value), Some(ceiling)) if value > ceiling) {
        return Err(ImageGatewayError::invalid_request(
            format!("{param} cannot exceed the inherited platform ceiling"),
            Some(param.to_string()),
            "rate_limit_exceeds_ceiling",
        ));
    }
    Ok(())
}

fn parse_control_version(value: &str) -> Result<i64, ImageGatewayError> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or_else(|| {
            ImageGatewayError::invalid_request(
                "expected_control_version must be a non-negative integer string",
                Some("expected_control_version".to_string()),
                "invalid_control_version",
            )
        })
}

fn validate_project_id(project_id: &str) -> Result<(), ImageGatewayError> {
    if project_id.trim().is_empty()
        || project_id.len() > 128
        || project_id.chars().any(char::is_control)
    {
        return Err(ImageGatewayError::invalid_request(
            "project_id is invalid",
            Some("project_id".to_string()),
            "invalid_project_id",
        ));
    }
    Ok(())
}

fn default_bucket_key(api_profile: &str, public_model_id: &str) -> String {
    format!("{api_profile}:{public_model_id}")
}

fn unit_kind(media_kind: &str) -> Result<&'static str, ImageGatewayError> {
    match media_kind {
        "image" => Ok("image"),
        "video" => Ok("video_second"),
        _ => Err(ImageGatewayError::internal(
            "project model media kind is invalid",
        )),
    }
}

fn nonnegative_u32(value: Option<i32>) -> Option<u32> {
    value.and_then(|value| u32::try_from(value).ok())
}

fn version_conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Project model policy changed; refresh and try again",
        Some("expected_control_version".to_string()),
        "project_model_policy_version_conflict",
    )
}

fn unavailable(_: sqlx::Error) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Project model policy is unavailable")
}

fn now_ms() -> Result<i64, ImageGatewayError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ImageGatewayError::internal("system clock is before Unix epoch"))?
        .as_millis();
    i64::try_from(millis).map_err(|_| ImageGatewayError::internal("system clock overflow"))
}

#[cfg(test)]
mod tests;
