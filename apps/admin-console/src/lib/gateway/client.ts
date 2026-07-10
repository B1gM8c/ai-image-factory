const DEFAULT_GATEWAY_BASE_URL = "http://127.0.0.1:8787";

type AllowedGatewayRoute = {
  method: string;
  pattern: RegExp;
};

const ALLOWED_GATEWAY_ROUTES: AllowedGatewayRoute[] = [
  { method: "GET", pattern: /^\/healthz$/ },
  { method: "GET", pattern: /^\/openapi\.json$/ },
  { method: "GET", pattern: /^\/v1\/models$/ },
  {
    method: "POST",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/service_accounts$/,
  },
  {
    method: "GET",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/api_keys$/,
  },
  {
    method: "DELETE",
    pattern: /^\/v1\/organization\/projects\/[^/]+\/api_keys\/[^/]+$/,
  },
];

export function gatewayBaseUrl() {
  return process.env.GATEWAY_BASE_URL || DEFAULT_GATEWAY_BASE_URL;
}

export function adminToken() {
  return process.env.GATEWAY_ADMIN_TOKEN;
}

export function consoleAccessToken() {
  return process.env.ADMIN_CONSOLE_ACCESS_TOKEN;
}

export function gatewayHeaders(extra?: HeadersInit) {
  const incoming = new Headers(extra);
  const headers = new Headers();
  const token = adminToken();

  for (const name of ["accept", "content-type"]) {
    const value = incoming.get(name);
    if (value) {
      headers.set(name, value);
    }
  }

  if (token) {
    headers.set("authorization", `Bearer ${token}`);
  }

  return headers;
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

export function isAuthorizedConsoleRequest(request: Request) {
  const expected = consoleAccessToken();
  const token = bearerToken(request.headers);

  if (!expected || !token) {
    return false;
  }

  return constantTimeEqual(token, expected);
}

export async function proxyGatewayRequest(path: string, request: Request) {
  if (!isAuthorizedConsoleRequest(request)) {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  if (!isAllowedGatewayProxy(request.method, path)) {
    return Response.json({ error: "Gateway route is not allowed" }, { status: 403 });
  }

  if (!adminToken()) {
    return Response.json({ error: "Gateway admin token is not configured" }, { status: 503 });
  }

  const url = new URL(path, gatewayBaseUrl());
  const headers = gatewayHeaders(request.headers);

  const response = await fetch(url, {
    method: request.method,
    headers,
    body: request.method === "GET" || request.method === "HEAD" ? undefined : await request.arrayBuffer(),
    cache: "no-store",
  });

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
}

function bearerToken(headers: Headers) {
  const value = headers.get("authorization");
  return value?.startsWith("Bearer ") ? value.slice("Bearer ".length) : null;
}

function constantTimeEqual(left: string, right: string) {
  const maxLength = Math.max(left.length, right.length);
  let diff = left.length ^ right.length;

  for (let index = 0; index < maxLength; index += 1) {
    diff |= (left.charCodeAt(index) || 0) ^ (right.charCodeAt(index) || 0);
  }

  return diff === 0;
}
