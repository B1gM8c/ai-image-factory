import {
  AUTH_CLIENT_ID,
  authenticatedSession,
  authCookiesFromRequest,
  authJson,
  clearAuthCookies,
  ensureCsrfCookie,
  isLegacyAdminAuthEnabled,
  configuredConsoleAccessToken,
  noStoreResponse,
  publicSession,
  setAuthCookies,
  setEmergencySession,
  unauthenticatedSession,
  validateMutationRequest,
} from "@/lib/auth/session";
import {
  requestAuthIdentity,
  requestAuthTokens,
  revokeUpstreamSession,
} from "@/lib/auth/upstream";

type LoginBody = {
  mode?: unknown;
  email?: unknown;
  password?: unknown;
};

export async function GET(request: Request) {
  await ensureCsrfCookie();
  const session = await publicSession();
  if (session.mode === "emergency") return authJson(session, 200);

  const { accessToken, refreshToken } = authCookiesFromRequest(request);
  if (!accessToken) {
    return authJson(unauthenticatedSession(Boolean(refreshToken)), 200);
  }

  const identity = await requestAuthIdentity(accessToken);
  if ("identity" in identity) {
    return authJson(
      authenticatedSession(
        identity.identity,
        session.access_expires_at,
        Boolean(refreshToken),
      ),
      200,
    );
  }

  if (identity.response.status === 401 && refreshToken) {
    return authJson(unauthenticatedSession(true), 200);
  }
  await clearAuthCookies();
  await ensureCsrfCookie();
  return authJson(unauthenticatedSession(), 200);
}

export async function POST(request: Request) {
  const rejected = validateMutationRequest(request);
  if (rejected) return rejected;

  const body = (await request.json().catch(() => null)) as LoginBody | null;
  if (body?.mode === "emergency") {
    return emergencyLogin();
  }

  const email = typeof body?.email === "string" ? body.email.trim() : "";
  const password = typeof body?.password === "string" ? body.password : "";
  if (!email || !password) {
    return authJson({ error: "Email and password are required" }, 400);
  }
  if (email.length > 254 || Buffer.byteLength(password, "utf8") > 1024) {
    return authJson({ error: "The sign-in payload is invalid" }, 400);
  }

  const result = await requestAuthTokens("/admin/v1/auth/login", {
    email,
    password,
    client_id: AUTH_CLIENT_ID,
  });
  if ("response" in result) return result.response;

  await setAuthCookies(result.tokens);
  const identity = await requestAuthIdentity(result.tokens.access_token);
  if ("response" in identity) {
    await clearAuthCookies();
    return identity.response;
  }
  return authJson(
    authenticatedSession(
      identity.identity,
      Math.floor(Date.now() / 1000) + result.tokens.expires_in,
      true,
    ),
    200,
  );
}

export async function DELETE(request: Request) {
  const rejected = validateMutationRequest(request);
  if (rejected) return rejected;

  const { accessToken, refreshToken } = authCookiesFromRequest(request);
  // Browser logout must complete even when the identity service is unavailable.
  // The database-backed session will still expire server-side, while retaining
  // the cookies here would leave the operator visibly signed in.
  if (refreshToken) {
    await revokeUpstreamSession(refreshToken, accessToken);
  }
  await clearAuthCookies();
  return noStoreResponse();
}

async function emergencyLogin() {
  if (!isLegacyAdminAuthEnabled() || !configuredConsoleAccessToken()) {
    return authJson({ error: "Emergency sign-in is disabled" }, 404);
  }
  await setEmergencySession();
  return noStoreResponse();
}
