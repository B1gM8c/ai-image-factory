use super::*;
use crate::provider_management::{
    PlatformRouteBinding, PlatformRouteBindingActor, PlatformRouteBindingResponse,
    UpdatePlatformRouteBindingRequest,
};

pub(super) async fn get(
    pool: &PgPool,
    route_id: Uuid,
) -> Result<PlatformRouteBindingResponse, ImageGatewayError> {
    let identity: (String, String) = sqlx::query_as(
        "SELECT provider_id, operation_id FROM provider_route_heads WHERE route_id = $1",
    )
    .bind(route_id)
    .fetch_optional(pool)
    .await
    .map_err(store_unavailable)?
    .ok_or_else(route_not_found)?;
    let binding = sqlx::query_as::<_, PlatformRouteBinding>(
        "SELECT route_id, route_revision, state FROM gateway_platform_provider_routes
         WHERE provider_id = $1 AND operation_id = $2",
    )
    .bind(identity.0)
    .bind(identity.1)
    .fetch_optional(pool)
    .await
    .map_err(store_unavailable)?;
    Ok(PlatformRouteBindingResponse { binding })
}

pub(super) async fn update(
    pool: &PgPool,
    route_id: Uuid,
    request: UpdatePlatformRouteBindingRequest,
    actor: PlatformRouteBindingActor,
) -> Result<PlatformRouteBindingResponse, ImageGatewayError> {
    if request.expected_route_revision < 1
        || request.expected_binding.as_ref().is_some_and(|binding| {
            binding.route_revision < 1 || !matches!(binding.state.as_str(), "enabled" | "disabled")
        })
    {
        return Err(ImageGatewayError::invalid_request(
            "Expected route revision and binding must be valid",
            None,
            "invalid_platform_route_binding",
        ));
    }
    let mut tx = pool.begin().await.map_err(store_unavailable)?;
    let (provider_id, operation_id, command_schema, revision, head_state, route_state): (
        String,
        String,
        String,
        i64,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT head.provider_id, head.operation_id, head.command_schema,
                head.current_revision, head.state, route.state
         FROM provider_route_heads head
         JOIN provider_routes route ON route.route_id = head.route_id
           AND route.revision = head.current_revision
         WHERE head.route_id = $1 FOR UPDATE OF head",
    )
    .bind(route_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(store_unavailable)?
    .ok_or_else(route_not_found)?;
    if revision != request.expected_route_revision
        || head_state != "enabled"
        || route_state != "enabled"
    {
        return Err(ImageGatewayError::conflict(
            "Provider route changed or is disabled; reload the route before binding",
            Some("expected_route_revision".to_owned()),
            "provider_route_revision_conflict",
        ));
    }
    let member_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT provider_account_id FROM provider_route_members
         WHERE route_id = $1 AND route_revision = $2 AND state = 'enabled'
         ORDER BY provider_account_id FOR SHARE",
    )
    .bind(route_id)
    .bind(revision)
    .fetch_all(&mut *tx)
    .await
    .map_err(store_unavailable)?;
    let active_members: Vec<Uuid> = sqlx::query_scalar(
        "SELECT member.provider_account_id FROM provider_route_members member
         JOIN provider_execution_profiles profile
           ON profile.execution_profile_id = member.execution_profile_id
          AND profile.provider_account_id = member.provider_account_id
          AND profile.provider_id = member.provider_id
          AND profile.operation_id = member.operation_id
          AND profile.command_schema = member.command_schema
         JOIN provider_accounts account ON account.provider_account_id = member.provider_account_id
         JOIN provider_account_environments environment
           ON environment.provider_account_id = member.provider_account_id
          AND environment.provider_id = member.provider_id
         WHERE member.route_id = $1 AND member.route_revision = $2
           AND member.state = 'enabled' AND profile.state = 'enabled'
           AND account.state = 'enabled' AND environment.state = 'active'
         ORDER BY member.provider_account_id
         FOR SHARE OF member, profile, account, environment",
    )
    .bind(route_id)
    .bind(revision)
    .fetch_all(&mut *tx)
    .await
    .map_err(store_unavailable)?;
    if member_ids.is_empty() || member_ids != active_members {
        return Err(ImageGatewayError::invalid_request(
            "Every route member must have a compatible active execution profile and account environment",
            Some("members".to_owned()),
            "invalid_provider_route_members",
        ));
    }
    let mappings = existing_route_model_mappings(&mut tx, route_id, revision).await?;
    let requested = mappings
        .iter()
        .map(|mapping| ProviderRouteModelMappingRequest {
            api_profile: mapping.api_profile.clone(),
            public_model_id: mapping.public_model_id.clone(),
            provider_model_id: if provider_id == openai_codex::PROVIDER_ID
                && mapping.execution_model_id == openai_codex::MODEL_GPT_IMAGE_2_SNAPSHOT
            {
                mapping.execution_model_id.clone()
            } else {
                mapping.provider_model_id.clone()
            },
            media_kind: mapping.media_kind.clone(),
        })
        .collect::<Vec<_>>();
    let validated = validated_route_model_mappings(
        &mut tx,
        &provider_id,
        &operation_id,
        &member_ids,
        Some(&requested),
    )
    .await?;
    if mappings.iter().zip(&validated).any(|(saved, current)| {
        saved.provider_model_id != current.provider_model_id
            || saved.execution_model_id != current.execution_model_id
    }) {
        return Err(invalid_route_models());
    }

    let before = sqlx::query_as::<_, PlatformRouteBinding>(
        "SELECT route_id, route_revision, state FROM gateway_platform_provider_routes
         WHERE provider_id = $1 AND operation_id = $2 FOR UPDATE",
    )
    .bind(&provider_id)
    .bind(&operation_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(store_unavailable)?;
    let after = PlatformRouteBinding {
        route_id,
        route_revision: revision,
        state: "enabled".to_owned(),
    };
    // Revalidate the target before replaying: a later head revision must not be silently published.
    if before.as_ref() == Some(&after) {
        tx.commit().await.map_err(store_unavailable)?;
        return Ok(PlatformRouteBindingResponse {
            binding: Some(after),
        });
    }
    if before != request.expected_binding {
        return Err(binding_conflict());
    }
    let now = database_now(&mut tx).await?;
    if before.is_none() {
        let inserted = sqlx::query(
            "INSERT INTO gateway_platform_provider_routes
             (provider_id, operation_id, command_schema, route_id, route_revision, state, created_at_ms, updated_at_ms)
             VALUES ($1, $2, $3, $4, $5, 'enabled', $6, $6)
             ON CONFLICT (provider_id, operation_id) DO NOTHING",
        )
        .bind(&provider_id).bind(&operation_id).bind(&command_schema)
        .bind(route_id).bind(revision).bind(now)
        .execute(&mut *tx).await.map_err(store_unavailable)?;
        if inserted.rows_affected() != 1 {
            return Err(binding_conflict());
        }
    } else {
        sqlx::query(
            "UPDATE gateway_platform_provider_routes
             SET command_schema = $3, route_id = $4, route_revision = $5, state = 'enabled', updated_at_ms = $6
             WHERE provider_id = $1 AND operation_id = $2",
        )
        .bind(&provider_id).bind(&operation_id).bind(&command_schema)
        .bind(route_id).bind(revision).bind(now)
        .execute(&mut *tx).await.map_err(store_unavailable)?;
    }
    sqlx::query(
        "INSERT INTO identity_audit_events
         (event_id, actor_user_id, session_id, action, resource_type, resource_id, outcome, metadata, created_at_ms)
         VALUES ($1, $2, $3, 'provider_route.platform_binding.update', 'provider_route', $4, 'success', $5, $6)",
    )
    .bind(Uuid::new_v4()).bind(actor.user_id).bind(actor.session_id).bind(route_id.to_string())
    .bind(serde_json::json!({"provider_id": provider_id, "operation_id": operation_id, "before": before, "after": after}))
    .bind(now).execute(&mut *tx).await.map_err(store_unavailable)?;
    tx.commit().await.map_err(store_unavailable)?;
    Ok(PlatformRouteBindingResponse {
        binding: Some(after),
    })
}

fn route_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Provider route not found",
        Some("route_id".to_owned()),
        "provider_route_not_found",
    )
}

fn binding_conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Platform binding changed since it was loaded",
        Some("expected_binding".to_owned()),
        "platform_route_binding_conflict",
    )
}
