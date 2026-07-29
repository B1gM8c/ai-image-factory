import {
  AUTH_CLIENT_ID,
  authenticatedSession,
  authCookiesFromRequest,
  authJson,
  clearAuthCookies,
  setAuthCookies,
  validateMutationRequest,
} from "@/lib/auth/session";
import { requestAuthIdentity, requestAuthTokens } from "@/lib/auth/upstream";

export async function POST(request: Request) {
  const rejected = validateMutationRequest(request);
  if (rejected) return rejected;

  const { refreshToken } = authCookiesFromRequest(request);
  if (!refreshToken) {
    return authJson({ error: "Refresh token is unavailable" }, 401);
  }

  const result = await requestAuthTokens("/admin/v1/auth/refresh", {
    refresh_token: refreshToken,
    client_id: AUTH_CLIENT_ID,
  });
  if ("response" in result) {
    if (result.response.status === 401 || result.response.status === 403) {
      await clearAuthCookies();
    }
    return result.response;
  }

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
