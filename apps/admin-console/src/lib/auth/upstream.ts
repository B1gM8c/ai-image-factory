import "server-only";

import {
  authJson,
  isAuthTokenResponse,
  noStoreHeaders,
  type AuthTokenResponse,
} from "@/lib/auth/session";
import { parseAuthIdentity, type AuthIdentity } from "@/lib/auth/types";
import { gatewayBaseUrl } from "@/lib/gateway/client";

const AUTH_TIMEOUT_MS = 5_000;

export async function requestAuthTokens(
  path: "/admin/v1/auth/login" | "/admin/v1/auth/refresh",
  body: Record<string, string>,
): Promise<{ tokens: AuthTokenResponse } | { response: Response }> {
  const upstream = await authRequest(path, body);
  if (upstream instanceof Response) return { response: upstream };
  if (!isAuthTokenResponse(upstream.body)) {
    return { response: authJson({ error: "Invalid authentication response" }, 502) };
  }
  return { tokens: upstream.body };
}

export async function revokeUpstreamSession(refreshToken: string, accessToken?: string) {
  const result = await authRequest(
    "/admin/v1/auth/logout",
    { refresh_token: refreshToken },
    accessToken,
  );
  return result instanceof Response ? result : null;
}

export async function requestAuthIdentity(
  accessToken: string,
): Promise<{ identity: AuthIdentity } | { response: Response }> {
  try {
    const response = await fetch(new URL("/admin/v1/auth/me", gatewayBaseUrl()), {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${accessToken}`,
      },
      cache: "no-store",
      redirect: "error",
      signal: AbortSignal.timeout(AUTH_TIMEOUT_MS),
    });
    const payload = (await response.json().catch(() => null)) as unknown;
    if (!response.ok) {
      return {
        response: authJson(
          { error: upstreamError(payload, response.status) },
          passthroughStatus(response.status),
        ),
      };
    }
    const identity = parseAuthIdentity(payload);
    if (!identity) {
      return { response: authJson({ error: "Invalid session identity response" }, 502) };
    }
    return { identity };
  } catch (error) {
    const timedOut = error instanceof Error && error.name === "TimeoutError";
    return {
      response: Response.json(
        { error: timedOut ? "Authentication service timed out" : "Authentication service unavailable" },
        { status: timedOut ? 504 : 502, headers: noStoreHeaders() },
      ),
    };
  }
}

async function authRequest(
  path: string,
  body: Record<string, string>,
  accessToken?: string,
): Promise<{ body: unknown } | Response> {
  const headers = new Headers({
    accept: "application/json",
    "content-type": "application/json",
  });
  if (accessToken) headers.set("authorization", `Bearer ${accessToken}`);

  try {
    const response = await fetch(new URL(path, gatewayBaseUrl()), {
      method: "POST",
      headers,
      body: JSON.stringify(body),
      cache: "no-store",
      redirect: "error",
      signal: AbortSignal.timeout(AUTH_TIMEOUT_MS),
    });
    const payload = (await response.json().catch(() => null)) as unknown;
    if (!response.ok) {
      return authJson(
        { error: upstreamError(payload, response.status) },
        passthroughStatus(response.status),
      );
    }
    return { body: payload };
  } catch (error) {
    const timedOut = error instanceof Error && error.name === "TimeoutError";
    return Response.json(
      { error: timedOut ? "Authentication service timed out" : "Authentication service unavailable" },
      { status: timedOut ? 504 : 502, headers: noStoreHeaders() },
    );
  }
}

function upstreamError(payload: unknown, status: number) {
  if (payload && typeof payload === "object" && "error" in payload) {
    const error = (payload as { error?: unknown }).error;
    if (typeof error === "string" && error.length <= 256) return error;
    if (error && typeof error === "object" && "message" in error) {
      const message = (error as { message?: unknown }).message;
      if (typeof message === "string" && message.length <= 256) return message;
    }
  }
  return status === 401
    ? "Incorrect email or password"
    : `Authentication service error (${status})`;
}

function passthroughStatus(status: number) {
  return [400, 401, 403, 409, 422, 429].includes(status) ? status : 502;
}
