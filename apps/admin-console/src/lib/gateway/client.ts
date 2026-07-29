import {
  authCookiesFromRequest,
  authJson,
  isLegacyAdminAuthEnabled,
  isValidEmergencySession,
  noStoreHeaders,
  validateMutationRequest,
} from "@/lib/auth/session";

const DEFAULT_GATEWAY_BASE_URL = "http://127.0.0.1:8787";
const GATEWAY_PROXY_TIMEOUT_MS = 5_000;
const DEFAULT_MAX_MUTATION_BODY_BYTES = 8 * 1024;
const MEDIA_MAX_MUTATION_BODY_BYTES = 64 * 1024;
const IMAGE_EDIT_MAX_MUTATION_BODY_BYTES = 34 * 1024 * 1024;
const VIDEO_MAX_MUTATION_BODY_BYTES = 16 * 1024 * 1024;
const BATCH_FILE_MAX_MUTATION_BODY_BYTES = 9 * 1024 * 1024;

type AllowedGatewayRoute = {
  method: string;
  pattern: RegExp;
};

const ALLOWED_GATEWAY_ROUTES: AllowedGatewayRoute[] = [
  { method: "GET", pattern: /^\/healthz$/ },
  { method: "GET", pattern: /^\/readyz$/ },
  { method: "GET", pattern: /^\/openapi\.json$/ },
  { method: "GET", pattern: /^\/v1\/models$/ },
  { method: "GET", pattern: /^\/v1\/console\/overview$/ },
  { method: "GET", pattern: /^\/v1\/console\/billing\/summary$/ },
  { method: "GET", pattern: /^\/v1\/console\/usage$/ },
  { method: "GET", pattern: /^\/v1\/console\/jobs$/ },
  { method: "GET", pattern: /^\/v1\/console\/logs$/ },
  { method: "GET", pattern: /^\/v1\/console\/notifications$/ },
  {
    method: "POST",
    pattern: /^\/v1\/console\/notifications\/[^/]+\/read$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/console\/jobs\/[^/]+\/economics$/,
  },
  { method: "GET", pattern: /^\/v1\/console\/provider-routes$/ },
  { method: "GET", pattern: /^\/v1\/console\/provider-models$/ },
  {
    method: "GET",
    pattern: /^\/v1\/organizations\/[^/]+\/billing\/credit-grants$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/console\/projects\/[^/]+\/images\/models$/,
  },
  {
    method: "POST",
    pattern: /^\/v1\/console\/projects\/[^/]+\/images\/generations$/,
  },
  {
    method: "POST",
    pattern: /^\/v1\/console\/projects\/[^/]+\/images\/edits$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/console\/projects\/[^/]+\/videos\/models$/,
  },
  {
    method: "POST",
    pattern: /^\/v1\/console\/projects\/[^/]+\/videos\/generations$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/console\/projects\/[^/]+\/videos\/[^/]+$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/console\/projects\/[^/]+\/videos\/files\/[^/]+\/content$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/console\/projects\/[^/]+\/files$/,
  },
  {
    method: "POST",
    pattern: /^\/v1\/console\/projects\/[^/]+\/files$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/console\/projects\/[^/]+\/files\/[^/]+$/,
  },
  {
    method: "DELETE",
    pattern: /^\/v1\/console\/projects\/[^/]+\/files\/[^/]+$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/console\/projects\/[^/]+\/files\/[^/]+\/content$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/console\/projects\/[^/]+\/batches$/,
  },
  {
    method: "POST",
    pattern: /^\/v1\/console\/projects\/[^/]+\/batches$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/console\/projects\/[^/]+\/batches\/[^/]+$/,
  },
  {
    method: "POST",
    pattern: /^\/v1\/console\/projects\/[^/]+\/batches\/[^/]+\/cancel$/,
  },
  { method: "GET", pattern: /^\/admin\/v1\/users$/ },
  { method: "POST", pattern: /^\/admin\/v1\/users$/ },
  { method: "GET", pattern: /^\/admin\/v1\/system\/update$/ },
  { method: "POST", pattern: /^\/admin\/v1\/system\/update\/check$/ },
  { method: "POST", pattern: /^\/admin\/v1\/system\/update\/apply$/ },
  { method: "GET", pattern: /^\/admin\/v1\/overview$/ },
  { method: "GET", pattern: /^\/admin\/v1\/billing\/summary$/ },
  { method: "GET", pattern: /^\/admin\/v1\/billing\/accounts$/ },
  { method: "GET", pattern: /^\/admin\/v1\/billing\/credit-grants$/ },
  { method: "POST", pattern: /^\/admin\/v1\/billing\/credit-grants$/ },
  {
    method: "GET",
    pattern: /^\/admin\/v1\/billing\/credit-grants\/[^/]+$/,
  },
  {
    method: "POST",
    pattern: /^\/admin\/v1\/billing\/credit-grants\/[^/]+\/revoke$/,
  },
  { method: "GET", pattern: /^\/admin\/v1\/billing\/customer-charges$/ },
  {
    method: "GET",
    pattern: /^\/admin\/v1\/billing\/customer-charges\/[^/]+$/,
  },
  {
    method: "POST",
    pattern: /^\/admin\/v1\/billing\/customer-charges\/[^/]+\/refunds$/,
  },
  { method: "GET", pattern: /^\/admin\/v1\/billing\/integrity-runs$/ },
  { method: "POST", pattern: /^\/admin\/v1\/billing\/integrity-runs$/ },
  {
    method: "GET",
    pattern: /^\/admin\/v1\/billing\/provider-cost-obligations$/,
  },
  {
    method: "GET",
    pattern: /^\/admin\/v1\/billing\/provider-cost-obligations\/[^/]+$/,
  },
  {
    method: "GET",
    pattern: /^\/admin\/v1\/billing\/provider-cost-allocation-pools$/,
  },
  {
    method: "GET",
    pattern: /^\/admin\/v1\/billing\/provider-cost-allocation-pools\/[^/]+$/,
  },
  {
    method: "POST",
    pattern: /^\/admin\/v1\/billing\/provider-cost-allocation-pools\/[^/]+$/,
  },
  {
    method: "POST",
    pattern: /^\/admin\/v1\/billing\/provider-cost-allocation-pools\/preview$/,
  },
  {
    method: "POST",
    pattern: /^\/admin\/v1\/billing\/provider-cost-allocation-pools$/,
  },
  {
    method: "GET",
    pattern: /^\/admin\/v1\/billing\/integrity-runs\/[^/]+$/,
  },
  {
    method: "GET",
    pattern: /^\/admin\/v1\/billing\/accounts\/[^/]+\/[^/]+$/,
  },
  {
    method: "PUT",
    pattern: /^\/admin\/v1\/billing\/accounts\/[^/]+\/[^/]+$/,
  },
  { method: "GET", pattern: /^\/admin\/v1\/usage$/ },
  { method: "GET", pattern: /^\/admin\/v1\/pricing\/price-books$/ },
  { method: "GET", pattern: /^\/admin\/v1\/pricing\/coverage$/ },
  {
    method: "GET",
    pattern: /^\/admin\/v1\/pricing\/price-book-versions\/[^/]+\/publish-readiness$/,
  },
  { method: "POST", pattern: /^\/admin\/v1\/pricing\/price-books$/ },
  {
    method: "POST",
    pattern: /^\/admin\/v1\/pricing\/price-books\/[^/]+\/versions$/,
  },
  {
    method: "PUT",
    pattern: /^\/admin\/v1\/pricing\/price-book-versions\/[^/]+$/,
  },
  {
    method: "POST",
    pattern: /^\/admin\/v1\/pricing\/price-book-versions\/[^/]+\/(publish|retire)$/,
  },
  { method: "POST", pattern: /^\/admin\/v1\/pricing\/preview$/ },
  { method: "GET", pattern: /^\/admin\/v1\/pricing\/official-catalogs$/ },
  {
    method: "POST",
    pattern: /^\/admin\/v1\/pricing\/official-catalogs\/[^/]+\/snapshots$/,
  },
  {
    method: "POST",
    pattern: /^\/admin\/v1\/pricing\/source-snapshots\/[^/]+\/apply$/,
  },
  { method: "GET", pattern: /^\/admin\/v1\/provider-accounts$/ },
  { method: "GET", pattern: /^\/admin\/v1\/provider-account-runtime-events$/ },
  { method: "GET", pattern: /^\/admin\/v1\/managed-cli-providers$/ },
  { method: "GET", pattern: /^\/admin\/v1\/provider-models$/ },
  { method: "GET", pattern: /^\/admin\/v1\/provider-model-refreshes\/[^/]+$/ },
  { method: "POST", pattern: /^\/admin\/v1\/provider-accounts\/[^/]+\/model-refreshes$/ },
  { method: "GET", pattern: /^\/admin\/v1\/provider-accounts\/[^/]+\/models$/ },
  { method: "PUT", pattern: /^\/admin\/v1\/provider-accounts\/[^/]+\/models$/ },
  { method: "GET", pattern: /^\/admin\/v1\/provider-accounts\/[^/]+\/grok-video-output$/ },
  { method: "PUT", pattern: /^\/admin\/v1\/provider-accounts\/[^/]+\/grok-video-output$/ },
  { method: "POST", pattern: /^\/admin\/v1\/provider-account-login-sessions$/ },
  { method: "POST", pattern: /^\/admin\/v1\/provider-accounts\/[^/]+\/reauthorization-sessions$/ },
  { method: "POST", pattern: /^\/admin\/v1\/provider-accounts\/codex\/login-sessions$/ },
  { method: "GET", pattern: /^\/admin\/v1\/provider-account-login-sessions\/[^/]+$/ },
  { method: "POST", pattern: /^\/admin\/v1\/provider-accounts\/[^/]+\/quota-refresh$/ },
  { method: "PATCH", pattern: /^\/admin\/v1\/provider-accounts\/[^/]+$/ },
  { method: "GET", pattern: /^\/admin\/v1\/provider-routes$/ },
  { method: "POST", pattern: /^\/admin\/v1\/provider-routes$/ },
  { method: "PUT", pattern: /^\/admin\/v1\/provider-routes\/[^/]+$/ },
  { method: "GET", pattern: /^\/admin\/v1\/scheduler\/queues$/ },
  { method: "GET", pattern: /^\/v1\/organization\/audit_logs$/ },
  { method: "GET", pattern: /^\/admin\/v1\/jobs$/ },
  { method: "GET", pattern: /^\/admin\/v1\/logs$/ },
  { method: "GET", pattern: /^\/admin\/v1\/jobs\/[^/]+\/economics$/ },
  { method: "GET", pattern: /^\/v1\/organization\/projects$/ },
  { method: "POST", pattern: /^\/v1\/organization\/projects$/ },
  {
    method: "GET",
    pattern: /^\/v1\/organization\/projects\/[^/]+$/,
  },
  {
    method: "PATCH",
    pattern: /^\/v1\/organization\/projects\/[^/]+$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/limits$/,
  },
  {
    method: "PUT",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/limits$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/model-policy$/,
  },
  {
    method: "PUT",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/model-policy$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/members$/,
  },
  {
    method: "POST",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/members$/,
  },
  {
    method: "PATCH",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/members\/[^/]+$/,
  },
  {
    method: "DELETE",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/members\/[^/]+$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/webhooks$/,
  },
  {
    method: "POST",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/webhooks$/,
  },
  {
    method: "PATCH",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/webhooks\/[^/]+$/,
  },
  {
    method: "DELETE",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/webhooks\/[^/]+$/,
  },
  {
    method: "POST",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/webhooks\/[^/]+\/(rotate|test)$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/webhooks\/[^/]+\/deliveries$/,
  },
  {
    method: "POST",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/service_accounts$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/api_keys$/,
  },
  {
    method: "POST",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/api_keys$/,
  },
  {
    method: "DELETE",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/service_accounts\/[^/]+$/,
  },
  {
    method: "DELETE",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/api_keys\/[^/]+$/,
  },
  {
    method: "PATCH",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/api_keys\/[^/]+$/,
  },
  {
    method: "POST",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/api_keys\/[^/]+\/rotate$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/api_keys\/[^/]+\/provider-route$/,
  },
  {
    method: "PUT",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/api_keys\/[^/]+\/provider-route$/,
  },
];

export function gatewayBaseUrl() {
  return process.env.GATEWAY_BASE_URL || DEFAULT_GATEWAY_BASE_URL;
}

export function gatewayPathFromSegments(path: string[] = [], search = "") {
  const pathname = `/${path.map((segment) => encodeURIComponent(segment)).join("/")}`;
  return `${pathname}${search}`;
}

export function isAllowedGatewayProxy(method: string, path: string) {
  const url = new URL(path, gatewayBaseUrl());
  const normalizedMethod = method.toUpperCase();

  return ALLOWED_GATEWAY_ROUTES.some(
    (route) => route.method === normalizedMethod && route.pattern.test(url.pathname),
  );
}

export async function proxyGatewayRequest(path: string, request: Request) {
  const credentials = gatewayCredentials(request);
  if (!credentials) {
    return authJson({ error: "Unauthorized" }, 401);
  }

  if (!isAllowedGatewayProxy(request.method, path)) {
    return authJson({ error: "Gateway route is not allowed" }, 403);
  }

  if (isMutation(request.method)) {
    const rejected = validateMutationRequest(
      request,
      gatewayMutationContentTypes(path, request.method),
    );
    if (rejected) return rejected;
  }

  let body: ArrayBuffer | undefined;
  if (request.method !== "GET" && request.method !== "HEAD") {
    const maxBodyBytes = gatewayMaxMutationBodyBytes(path, request.method);
    const contentLength = request.headers.get("content-length");
    if (contentLength && /^\d+$/.test(contentLength) && Number(contentLength) > maxBodyBytes) {
      return authJson({ error: "Gateway request body is too large" }, 413);
    }
    try {
      body = await request.arrayBuffer();
    } catch {
      return authJson({ error: "Gateway request body is invalid" }, 400);
    }
    if (body.byteLength > maxBodyBytes) {
      return authJson({ error: "Gateway request body is too large" }, 413);
    }
  }

  const url = new URL(path, gatewayBaseUrl());
  const headers = gatewayHeaders(request.headers, credentials.token);

  try {
    const timeoutMs = gatewayTimeoutMs(url.pathname, request.method);
    const response = await fetch(url, {
      method: request.method,
      headers,
      body,
      cache: "no-store",
      signal: timeoutMs === null ? undefined : AbortSignal.timeout(timeoutMs),
    });

    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: gatewayResponseHeaders(response.headers),
    });
  } catch (error) {
    const timedOut = error instanceof Error && error.name === "TimeoutError";
    return authJson(
      { error: timedOut ? "Gateway request timed out" : "Gateway request failed" },
      timedOut ? 504 : 502,
    );
  }
}

function gatewayTimeoutMs(pathname: string, method: string) {
  if (method === "GET" && pathname === "/admin/v1/provider-account-runtime-events") return null;
  if (
    method === "POST"
    && /^\/v1\/console\/projects\/[^/]+\/images\/(generations|edits)$/.test(pathname)
  ) return 180_000;
  if (
    method === "POST"
    && /^\/v1\/console\/projects\/[^/]+\/videos\/generations$/.test(pathname)
  ) return 15_000;
  if (
    method === "GET"
    && /^\/v1\/console\/projects\/[^/]+\/videos\/files\/[^/]+\/content$/.test(pathname)
  ) return 30_000;
  if (
    method === "POST"
    && /^\/v1\/console\/projects\/[^/]+\/files$/.test(pathname)
  ) return 120_000;
  if (method === "POST" && pathname.endsWith("/quota-refresh")) return 75_000;
  if (
    method === "POST"
    && (
      pathname.endsWith("/codex/login-sessions")
      || pathname.endsWith("/provider-account-login-sessions")
      || pathname.endsWith("/reauthorization-sessions")
    )
  ) return 35_000;
  return GATEWAY_PROXY_TIMEOUT_MS;
}

function gatewayMaxMutationBodyBytes(path: string, method: string) {
  const pathname = new URL(path, gatewayBaseUrl()).pathname;
  if (
    method.toUpperCase() === "POST"
    && /^\/v1\/console\/projects\/[^/]+\/images\/edits$/.test(pathname)
  ) {
    return IMAGE_EDIT_MAX_MUTATION_BODY_BYTES;
  }
  if (
    method.toUpperCase() === "POST"
    && /^\/v1\/console\/projects\/[^/]+\/images\/generations$/.test(pathname)
  ) {
    return MEDIA_MAX_MUTATION_BODY_BYTES;
  }
  if (
    method.toUpperCase() === "POST"
    && /^\/v1\/console\/projects\/[^/]+\/videos\/generations$/.test(pathname)
  ) {
    return VIDEO_MAX_MUTATION_BODY_BYTES;
  }
  if (
    method.toUpperCase() === "POST"
    && /^\/v1\/console\/projects\/[^/]+\/files$/.test(pathname)
  ) {
    return BATCH_FILE_MAX_MUTATION_BODY_BYTES;
  }
  return DEFAULT_MAX_MUTATION_BODY_BYTES;
}

function gatewayMutationContentTypes(path: string, method: string) {
  const pathname = new URL(path, gatewayBaseUrl()).pathname;
  if (
    method.toUpperCase() === "POST"
    && /^\/v1\/console\/projects\/[^/]+\/images\/edits$/.test(pathname)
  ) {
    return ["multipart/form-data"];
  }
  return ["application/json"];
}

function gatewayCredentials(request: Request) {
  const { accessToken, emergencySession } = authCookiesFromRequest(request);
  if (accessToken) return { token: accessToken };

  const adminToken = process.env.GATEWAY_ADMIN_TOKEN?.trim();
  if (
    adminToken &&
    isLegacyAdminAuthEnabled() &&
    isValidEmergencySession(emergencySession)
  ) {
    return { token: adminToken };
  }
  return null;
}

function gatewayHeaders(incomingHeaders: Headers, token: string) {
  const headers = new Headers();
  for (const name of ["accept", "content-type", "idempotency-key"]) {
    const value = incomingHeaders.get(name);
    if (value) headers.set(name, value);
  }
  headers.set("authorization", `Bearer ${token}`);
  return headers;
}

function gatewayResponseHeaders(upstream: Headers) {
  const headers = new Headers(noStoreHeaders());
  for (const name of [
    "content-type",
    "content-disposition",
    "etag",
    "last-modified",
    "cache-control",
    "x-accel-buffering",
    "x-request-id",
  ]) {
    const value = upstream.get(name);
    if (value) headers.set(name, value);
  }
  return headers;
}

function isMutation(method: string) {
  return !["GET", "HEAD", "OPTIONS"].includes(method.toUpperCase());
}
