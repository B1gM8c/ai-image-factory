# Multi-User Control Plane Architecture

Status: first production slice implemented, 2026-07-23

## Decision

The control plane uses an explicit hierarchy:

```text
global identity
  -> organization/workspace membership
    -> project membership
      -> project resource
```

`tenant_id` is the existing organization/workspace boundary. A new user gets a
personal organization and a default project in the same transaction as user
creation. This keeps the single-user experience simple while allowing shared
organizations later without changing resource ownership again.

The primary authorization defense is an explicit, mandatory scope passed from
Axum authorization into repository queries. PostgreSQL row-level security is a
later defense in depth, not a substitute for scoped repositories. It must not
be enabled while the runtime connects as a table owner, superuser, or a role
with `BYPASSRLS`, because those roles normally bypass policies.

## Resource Ownership

| Resource | Security boundary | Notes |
| --- | --- | --- |
| Identity, password, session | User | Global identity; no tenant claims are trusted from the client |
| Organization membership | Organization + user | Fixed roles in the first release |
| Project membership | Project + user | API keys and jobs are project resources |
| Provider model catalog | Platform | Shared discovery metadata, no credentials |
| Managed provider account | Platform | Visible only to platform operators |
| User-owned provider account | Organization + owner user | Introduced only through an explicit BYOA flow |
| Provider route/group | Platform in the first release | Members see only the callable route projection, never account membership |
| Job, usage, billing | Project and organization | Existing `tenant_id` remains the organization key |
| Platform cost and scheduler health | Platform | Never exposed through member routes |

Managed platform capacity and user-owned upstream accounts are intentionally
different products. A member must not gain visibility into platform provider
credentials merely because the console becomes multi-user.

## Request Authorization

JWTs contain identity and global platform grants only. Organization and project
memberships are loaded from PostgreSQL while validating the active session.
This preserves immediate revocation through `authz_version` and avoids stale,
oversized membership claims.

Handlers use one of these server-derived scopes:

```rust
enum DataScope {
    Platform,
    Tenants(Vec<String>),
    Projects(Vec<String>),
}
```

The browser may request a narrower scope. It can never request a broader one.
For example, the implemented read API lets a platform owner select a user for
an operational read and resolves that user's memberships on the server. The
first UI release exposes the global platform view and People page; a visual
per-user drill-down selector is a follow-up. A regular member receives only
their own project memberships. User, tenant, and project identifiers from
query strings, headers, cursors, or request bodies are preferences until
validated against the authenticated principal.

Platform-owner authorization requires both the `platform_owner` role and the
`admin:*` scope. Roles describe the subject; scopes describe permitted actions.

## API Separation

Platform operations remain under `/admin/v1`:

- global provider accounts and routes;
- scheduler and worker health;
- platform cost and all-tenant operational reads;
- people, organizations, and membership administration.

Member reads use `/v1/console`:

- overview and activity;
- usage and customer billing;
- projects and API keys;
- shared model catalog with platform account counts and observation timestamps
  removed for members.

An administrator may use the member read API with an explicitly authorized
user/project scope. That is a data-view context, not impersonation. Mutations
and audit events always retain the real administrator actor.

## Browser Model

The Next.js BFF keeps access and refresh tokens in host-only HttpOnly cookies.
React receives only a public session projection:

- user ID, display name, email, roles, and capabilities;
- organizations and projects the user may select;
- the current data-view scope.

The sidebar follows the OpenAI-style organization/project hierarchy:

- the header contains the current workspace/project selector;
- the footer contains the real signed-in user and logout;
- platform-only navigation is generated only for platform capabilities;
- administrators get an explicit global platform scope and a separate People
  page.

Provider-account SSE remains platform-only in the first release. Before a
member-facing scoped stream is introduced, changing scope must close the old
connection and clear the previous query cache. The server must filter both the
initial snapshot and every delta. Browser filtering of global payloads is
forbidden.

## Database Constraints And Indexes

Migration `0057_identity_workspaces.sql` adds organizations and memberships,
provisions personal workspaces, and attributes provider accounts. Existing
job, usage, and billing projections are scoped through their authoritative
`tenant_id`. Organization-owned custom routes and direct project attribution
on every hot projection are later schema phases, not prerequisites for the
implemented personal-workspace release.

Required hot-path indexes include:

```text
organization_memberships(user_id, organization_id) WHERE state = 'active'
project_memberships(user_id, project_id) WHERE state = 'active'
provider_accounts(tenant_id, owner_user_id, updated_at_ms DESC)
jobs(tenant_id, created_at_ms DESC, job_id DESC)
usage_events(tenant_id, created_at_ms, billing_metric, billing_unit)
```

Cross-boundary references use composite foreign keys wherever both sides are
tenant-owned. Migrations follow add-nullable, backfill, index, validate
constraint, then set-not-null for large existing tables.

## RLS Deployment Gate

RLS is enabled only after deployment separates these roles:

- `migration_owner NOLOGIN`, which owns tables;
- `app_runtime NOSUPERUSER NOBYPASSRLS`, used by request handlers;
- worker and read-only support roles with narrower grants.

Request transactions set tenant/project/user context with transaction-local
settings. Session-level `SET` is forbidden with connection pools. Missing
context defaults to deny. `FORCE ROW LEVEL SECURITY` is used on selected
top-level boundary tables after tests prove the production runtime role does
not bypass it.

## Security And Verification

Every release must prove:

1. User A cannot read or mutate User B resources by replacing path, query,
   request-body, cursor, or SSE identifiers.
2. A member cannot call platform provider, scheduler, system, or people APIs.
3. An administrator can read all data and a selected user's data without
   changing actor identity.
4. Membership removal invalidates subsequent reads and newly opened streams.
5. API key access is the intersection of key grants and active project
   membership.
6. Scope changes do not briefly render cached data from the previous scope.
7. Audit records contain actor, organization, project, permission, resource,
   request ID, and result, but never tokens, API keys, or provider credentials.

The authorization baseline follows deny-by-default and per-request validation
from the [OWASP Authorization Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html)
and tenant context isolation from the
[OWASP Multi-Tenant Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Multi_Tenant_Security_Cheat_Sheet.html).
Refresh-session behavior continues to follow
[RFC 9700](https://www.rfc-editor.org/rfc/rfc9700.html). PostgreSQL RLS behavior,
including owner and `BYPASSRLS` exceptions, is defined by the
[PostgreSQL row security documentation](https://www.postgresql.org/docs/current/ddl-rowsecurity.html).
