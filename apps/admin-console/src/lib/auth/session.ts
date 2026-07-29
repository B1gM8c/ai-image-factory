import "server-only";

import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { cookies } from "next/headers";
import type {
  AuthIdentity,
  AuthUser,
  ConsoleSessionStatus,
} from "@/lib/auth/types";

const DEVELOPMENT_ACCESS_COOKIE = "aif_access";
const DEVELOPMENT_REFRESH_COOKIE = "aif_refresh";
const DEVELOPMENT_CSRF_COOKIE = "aif_csrf";
const PRODUCTION_ACCESS_COOKIE = "__Host-aif_access";
const PRODUCTION_REFRESH_COOKIE = "__Host-aif_refresh";
const PRODUCTION_CSRF_COOKIE = "__Host-aif_csrf";

const DEVELOPMENT_CONSOLE_SESSION_COOKIE = "aif_console_session";
const PRODUCTION_CONSOLE_SESSION_COOKIE = "__Host-aif_console_session";
export const AUTH_CLIENT_ID =
  process.env.ADMIN_CONSOLE_CLIENT_ID?.trim() || "ai-image-factory-admin-bff";

export type AuthSession = {
  id: string;
  absolute_expires_at: string;
};

export type AuthTokenResponse = {
  access_token: string;
  token_type: string;
  expires_in: number;
  refresh_token: string;
  refresh_expires_in: number;
  user: AuthUser;
  session: AuthSession;
};

export type PublicSession = ConsoleSessionStatus;

export function accessCookieName() {
  return isProduction() ? PRODUCTION_ACCESS_COOKIE : DEVELOPMENT_ACCESS_COOKIE;
}

export function refreshCookieName() {
  return isProduction() ? PRODUCTION_REFRESH_COOKIE : DEVELOPMENT_REFRESH_COOKIE;
}

export function csrfCookieName() {
  return isProduction() ? PRODUCTION_CSRF_COOKIE : DEVELOPMENT_CSRF_COOKIE;
}

export function configuredConsoleAccessToken() {
  return process.env.ADMIN_CONSOLE_ACCESS_TOKEN?.trim() || null;
}

export function isLegacyAdminAuthEnabled() {
  return process.env.NODE_ENV === "development";
}

export function isValidEmergencySession(candidate: string | undefined) {
  const key = configuredConsoleAccessToken();
  if (!isLegacyAdminAuthEnabled() || !key || !candidate) return false;
  const [version, expires, nonce, signature, extra] = candidate.split(".");
  if (version !== "aife_v1" || extra !== undefined || !expires || !nonce || !signature) return false;
  const expiresAt = Number(expires);
  const now = Math.floor(Date.now() / 1000);
  if (!Number.isSafeInteger(expiresAt) || expiresAt <= now || expiresAt > now + 60 * 60) return false;
  return constantTimeEqual(signature, emergencySignature(key, expires, nonce));
}

export function cookieValueFromHeader(header: string | null, name: string) {
  if (!header) return undefined;
  for (const part of header.split(";")) {
    const [key, ...value] = part.trim().split("=");
    if (key === name) return decodeCookieValue(value.join("="));
  }
  return undefined;
}

export function authCookiesFromRequest(request: Request) {
  const header = request.headers.get("cookie");
  return {
    accessToken: cookieValueFromHeader(header, accessCookieName()),
    refreshToken: cookieValueFromHeader(header, refreshCookieName()),
    emergencySession: cookieValueFromHeader(header, consoleSessionCookieName()),
  };
}

export async function hasConsoleSession() {
  const cookieStore = await cookies();
  const accessToken = cookieStore.get(accessCookieName())?.value;
  return Boolean(
    isAccessTokenCurrent(accessToken) ||
      cookieStore.get(refreshCookieName())?.value ||
      isValidEmergencySession(cookieStore.get(consoleSessionCookieName())?.value),
  );
}

export async function publicSession(): Promise<PublicSession> {
  const cookieStore = await cookies();
  const accessToken = cookieStore.get(accessCookieName())?.value;
  const refreshAvailable = Boolean(cookieStore.get(refreshCookieName())?.value);
  const emergency = isValidEmergencySession(cookieStore.get(consoleSessionCookieName())?.value);
  const accessExpiresAt = accessToken ? jwtExpiry(accessToken) : null;

  if (isAccessTokenCurrent(accessToken, accessExpiresAt) || refreshAvailable) {
    return {
      authenticated: true,
      mode: "jwt",
      access_expires_at: accessExpiresAt,
      refresh_available: refreshAvailable,
      user: null,
      organizations: [],
      projects: [],
      capabilities: [],
    };
  }

  return {
    authenticated: emergency,
    mode: emergency ? "emergency" : "none",
    access_expires_at: null,
    refresh_available: false,
    user: emergency
      ? {
          id: "emergency",
          email: "development-emergency@localhost",
          display_name: "开发应急管理员",
          roles: ["platform_owner"],
          scopes: ["admin:*"],
        }
      : null,
    organizations: [],
    projects: [],
    capabilities: emergency ? ["admin:*"] : [],
  };
}

export function authenticatedSession(
  identity: AuthIdentity,
  accessExpiresAt: number | null,
  refreshAvailable: boolean,
): PublicSession {
  return {
    authenticated: true,
    mode: "jwt",
    access_expires_at: accessExpiresAt,
    refresh_available: refreshAvailable,
    ...identity,
  };
}

export function unauthenticatedSession(refreshAvailable = false): PublicSession {
  return {
    authenticated: false,
    mode: "none",
    access_expires_at: null,
    refresh_available: refreshAvailable,
    user: null,
    organizations: [],
    projects: [],
    capabilities: [],
  };
}

export async function setAuthCookies(tokens: AuthTokenResponse) {
  const cookieStore = await cookies();
  cookieStore.set(accessCookieName(), tokens.access_token, privateCookie(tokens.expires_in));
  cookieStore.set(refreshCookieName(), tokens.refresh_token, privateCookie(tokens.refresh_expires_in));
  cookieStore.delete(consoleSessionCookieName());
  setCsrfCookie(cookieStore, newCsrfToken());
}

export async function setEmergencySession() {
  const key = configuredConsoleAccessToken();
  if (!isLegacyAdminAuthEnabled() || !key) throw new Error("Emergency login is unavailable");
  const cookieStore = await cookies();
  const expires = String(Math.floor(Date.now() / 1000) + 60 * 60);
  const nonce = randomBytes(32).toString("base64url");
  const value = `aife_v1.${expires}.${nonce}.${emergencySignature(key, expires, nonce)}`;
  cookieStore.set(consoleSessionCookieName(), value, privateCookie(60 * 60));
  cookieStore.delete(accessCookieName());
  cookieStore.delete(refreshCookieName());
  setCsrfCookie(cookieStore, newCsrfToken());
}

export async function clearAuthCookies() {
  const cookieStore = await cookies();
  for (const name of [
    DEVELOPMENT_ACCESS_COOKIE,
    DEVELOPMENT_REFRESH_COOKIE,
    PRODUCTION_ACCESS_COOKIE,
    PRODUCTION_REFRESH_COOKIE,
    DEVELOPMENT_CSRF_COOKIE,
    PRODUCTION_CSRF_COOKIE,
    DEVELOPMENT_CONSOLE_SESSION_COOKIE,
    PRODUCTION_CONSOLE_SESSION_COOKIE,
  ]) {
    cookieStore.delete(name);
  }
}

export async function ensureCsrfCookie() {
  const cookieStore = await cookies();
  const existing = cookieStore.get(csrfCookieName())?.value;
  if (existing) return existing;

  const token = newCsrfToken();
  setCsrfCookie(cookieStore, token);
  return token;
}

export function validateMutationRequest(
  request: Request,
  allowedContentTypes: readonly string[] = ["application/json"],
): Response | null {
  const expectedOrigin = configuredConsoleOrigin(request);
  if (!expectedOrigin) {
    return authJson(
      { error: isProduction() ? "ADMIN_CONSOLE_ORIGIN is required in production" : "Untrusted development host" },
      isProduction() ? 503 : 403,
    );
  }

  const origin = normalizedOrigin(request.headers.get("origin"));
  if (origin !== expectedOrigin || request.headers.get("sec-fetch-site") !== "same-origin") {
    return authJson({ error: "Cross-origin mutation rejected" }, 403);
  }

  const contentType = request.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase();
  if (!contentType || !allowedContentTypes.includes(contentType)) {
    return authJson(
      {
        error:
          allowedContentTypes.length === 1 && allowedContentTypes[0] === "application/json"
            ? "JSON content type is required"
            : "Unsupported content type",
      },
      415,
    );
  }

  const cookieToken = cookieValueFromHeader(request.headers.get("cookie"), csrfCookieName());
  const headerToken = request.headers.get("x-aif-csrf");
  if (!cookieToken || !headerToken || !constantTimeEqual(cookieToken, headerToken)) {
    return authJson({ error: "CSRF validation failed" }, 403);
  }

  return null;
}

export function authJson(body: unknown, status: number) {
  return Response.json(body, { status, headers: noStoreHeaders() });
}

export function noStoreResponse(status = 204) {
  return new Response(null, { status, headers: noStoreHeaders() });
}

export function noStoreHeaders() {
  return {
    "cache-control": "no-store, max-age=0",
    pragma: "no-cache",
  };
}

export function isAuthTokenResponse(value: unknown): value is AuthTokenResponse {
  if (!value || typeof value !== "object") return false;
  const candidate = value as Partial<AuthTokenResponse>;
  return (
    typeof candidate.access_token === "string" &&
    candidate.access_token.length > 0 &&
    candidate.token_type?.toLowerCase() === "bearer" &&
    isPositiveInteger(candidate.expires_in) &&
    typeof candidate.refresh_token === "string" &&
    candidate.refresh_token.length > 0 &&
    isPositiveInteger(candidate.refresh_expires_in) &&
    Boolean(candidate.user && typeof candidate.user.id === "string" && typeof candidate.user.email === "string") &&
    Boolean(candidate.session && typeof candidate.session.id === "string")
  );
}

function configuredConsoleOrigin(request: Request) {
  const configured = normalizedOrigin(process.env.ADMIN_CONSOLE_ORIGIN ?? null);
  if (configured) return configured;
  return isProduction() ? null : loopbackRequestOrigin(request);
}

function loopbackRequestOrigin(request: Request) {
  const requestUrl = new URL(request.url);
  const host = request.headers.get("host");
  if (!host) return requestUrl.origin;

  const origin = normalizedOrigin(`${requestUrl.protocol}//${host}`);
  if (!origin) return null;
  const hostname = new URL(origin).hostname;
  return ["localhost", "127.0.0.1", "[::1]"].includes(hostname) ? origin : null;
}

function normalizedOrigin(value: string | null) {
  if (!value) return null;
  try {
    const url = new URL(value);
    return url.origin === value.replace(/\/$/, "") ? url.origin : null;
  } catch {
    return null;
  }
}

function privateCookie(maxAge: number) {
  return {
    httpOnly: true,
    sameSite: "strict" as const,
    secure: isProduction(),
    path: "/",
    maxAge: Math.max(1, Math.floor(maxAge)),
  };
}

function setCsrfCookie(cookieStore: Awaited<ReturnType<typeof cookies>>, value: string) {
  cookieStore.set(csrfCookieName(), value, {
    httpOnly: false,
    sameSite: "strict",
    secure: isProduction(),
    path: "/",
  });
}

function newCsrfToken() {
  return randomBytes(32).toString("base64url");
}

function consoleSessionCookieName() {
  return isProduction() ? PRODUCTION_CONSOLE_SESSION_COOKIE : DEVELOPMENT_CONSOLE_SESSION_COOKIE;
}

function emergencySignature(key: string, expires: string, nonce: string) {
  return createHmac("sha256", key)
    .update(`ai-image-factory-emergency-v1\0${expires}\0${nonce}`)
    .digest("base64url");
}

function jwtExpiry(token: string) {
  try {
    const payload = JSON.parse(Buffer.from(token.split(".")[1], "base64url").toString("utf8")) as {
      exp?: unknown;
    };
    return typeof payload.exp === "number" && Number.isFinite(payload.exp) ? payload.exp : null;
  } catch {
    return null;
  }
}

function isAccessTokenCurrent(token: string | undefined, expiresAt = token ? jwtExpiry(token) : null) {
  return Boolean(expiresAt && expiresAt > Math.floor(Date.now() / 1000));
}

function decodeCookieValue(value: string) {
  try {
    return decodeURIComponent(value);
  } catch {
    return undefined;
  }
}

function constantTimeEqual(left: string, right: string) {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  if (leftBuffer.length !== rightBuffer.length) return false;
  return timingSafeEqual(leftBuffer, rightBuffer);
}

function isPositiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value > 0;
}

function isProduction() {
  return process.env.NODE_ENV === "production";
}
