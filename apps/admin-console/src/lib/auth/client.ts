"use client";

import {
  isConsoleSessionStatus,
  type ConsoleSessionStatus,
} from "@/lib/auth/types";
import type { LocalizedText } from "@/i18n/config";

const CSRF_COOKIE_NAMES = ["__Host-aif_csrf", "aif_csrf"];
const SESSION_EXPIRED_EVENT = "aif:session-expired";
const REFRESH_LOCK = "aif-session-refresh";
const REFRESH_LEASE_KEY = "aif-session-refresh-lease";
const REFRESH_LEASE_MS = 10_000;
const REFRESH_LEASE_SETTLE_MS = 50;

type ConsoleFetchOptions = {
  retryUnauthorized?: boolean;
};

type Translate = (
  text: LocalizedText,
  values?: Record<string, string | number>,
) => string;

type ConsoleClientErrorCode =
  | "secure_session_initialization_failed"
  | "csrf_cookie_missing";

class ConsoleClientError extends Error {
  constructor(readonly code: ConsoleClientErrorCode) {
    super(code);
    this.name = "ConsoleClientError";
  }
}

let localRefresh: Promise<boolean> | null = null;

export async function consoleFetch(
  input: RequestInfo | URL,
  init: RequestInit = {},
  options: ConsoleFetchOptions = {},
) {
  // CSRF rotation is also the browser-visible session generation. Capture it
  // before the request so another tab's successful refresh suppresses ours.
  const observedCsrf = readCsrfCookie();
  const response = await fetchWithCsrf(input, init);
  if (response.status !== 401 || options.retryUnauthorized === false) return response;

  if (!(await refreshSession(observedCsrf))) {
    dispatchSessionExpired();
    return response;
  }

  const retried = await fetchWithCsrf(input, init);
  if (retried.status === 401) dispatchSessionExpired();
  return retried;
}

export async function getConsoleSession() {
  const response = await fetch("/api/session", { cache: "no-store" });
  if (!response.ok) return null;
  const payload = (await response.json().catch(() => null)) as unknown;
  return isConsoleSessionStatus(payload) ? payload : null;
}

export async function refreshConsoleSession() {
  return refreshSession(readCsrfCookie());
}

export function onSessionExpired(listener: () => void) {
  window.addEventListener(SESSION_EXPIRED_EVENT, listener);
  return () => window.removeEventListener(SESSION_EXPIRED_EVENT, listener);
}

export async function consoleResponseFailure(
  response: Response,
  primary: string,
  t: Translate,
) {
  const payload = (await response.json().catch(() => null)) as unknown;
  const { code, message } = responseErrorDetails(payload);
  const known = knownConsoleFailure(code, message, response.status, t);
  if (known) return joinFailure(primary, known, t);

  return technicalFailure(
    primary,
    message ?? code ?? `HTTP ${response.status}`,
    t,
  );
}

export function consoleRequestFailure(
  reason: unknown,
  primary: string,
  t: Translate,
) {
  if (reason instanceof ConsoleClientError) {
    return joinFailure(primary, clientFailure(reason.code, t), t);
  }

  const detail = reason instanceof Error ? reason.message.trim() : "";
  if (!detail) return primary;
  if (detail.startsWith(primary)) return detail;
  return technicalFailure(primary, detail, t);
}

async function fetchWithCsrf(input: RequestInfo | URL, init: RequestInit) {
  if (!isMutation(init.method)) return fetch(input, { ...init, cache: init.cache ?? "no-store" });

  const csrf = await ensureCsrfToken();
  const headers = new Headers(init.headers);
  headers.set("x-aif-csrf", csrf);
  if (!headers.has("content-type") && !(init.body instanceof FormData)) {
    headers.set("content-type", "application/json");
  }
  return fetch(input, { ...init, headers, cache: "no-store" });
}

async function ensureCsrfToken() {
  const existing = readCsrfCookie();
  if (existing) return existing;

  const response = await fetch("/api/session", { cache: "no-store" });
  if (!response.ok) {
    throw new ConsoleClientError("secure_session_initialization_failed");
  }
  const token = readCsrfCookie();
  if (!token) throw new ConsoleClientError("csrf_cookie_missing");
  return token;
}

async function refreshSession(observedCsrf: string | null) {
  if (localRefresh) return localRefresh;
  localRefresh = withRefreshLock(observedCsrf).finally(() => {
    localRefresh = null;
  });
  return localRefresh;
}

async function withRefreshLock(observedCsrf: string | null) {
  if (!("locks" in navigator)) return withFallbackRefreshLock(observedCsrf);

  return navigator.locks.request(REFRESH_LOCK, async () => {
    const currentCsrf = readCsrfCookie();
    if (observedCsrf && currentCsrf && currentCsrf !== observedCsrf) {
      const session = await getConsoleSession();
      return Boolean(session?.authenticated);
    }
    return performRefresh();
  });
}

async function withFallbackRefreshLock(observedCsrf: string | null) {
  const owner = crypto.randomUUID();
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const now = Date.now();
    const current = readRefreshLease();
    if (!current || current.expiresAt <= now) {
      const mine = { owner, expiresAt: now + REFRESH_LEASE_MS };
      try {
        localStorage.setItem(REFRESH_LEASE_KEY, JSON.stringify(mine));
        await delay(REFRESH_LEASE_SETTLE_MS);
        if (readRefreshLease()?.owner === owner) {
          try {
            return await performRefresh();
          } finally {
            if (readRefreshLease()?.owner === owner) localStorage.removeItem(REFRESH_LEASE_KEY);
          }
        }
      } catch {
        return performRefresh();
      }
    }

    await waitForRefreshLease(current?.expiresAt ?? now + REFRESH_LEASE_MS);
    const currentCsrf = readCsrfCookie();
    if (observedCsrf && currentCsrf && currentCsrf !== observedCsrf) {
      const session = await getConsoleSession();
      return Boolean(session?.authenticated);
    }
  }
  return false;
}

function delay(milliseconds: number) {
  return new Promise<void>((resolve) => window.setTimeout(resolve, milliseconds));
}

function readRefreshLease(): { owner: string; expiresAt: number } | null {
  try {
    const value = localStorage.getItem(REFRESH_LEASE_KEY);
    if (!value) return null;
    const candidate = JSON.parse(value) as { owner?: unknown; expiresAt?: unknown };
    return typeof candidate.owner === "string" &&
      typeof candidate.expiresAt === "number" &&
      Number.isFinite(candidate.expiresAt)
      ? { owner: candidate.owner, expiresAt: candidate.expiresAt }
      : null;
  } catch {
    return null;
  }
}

function waitForRefreshLease(expiresAt: number) {
  return new Promise<void>((resolve) => {
    const timeout = window.setTimeout(done, Math.max(0, Math.min(REFRESH_LEASE_MS, expiresAt - Date.now())));
    function done() {
      window.clearTimeout(timeout);
      window.removeEventListener("storage", onStorage);
      resolve();
    }
    function onStorage(event: StorageEvent) {
      if (event.key === REFRESH_LEASE_KEY) done();
    }
    window.addEventListener("storage", onStorage);
  });
}

async function performRefresh() {
  try {
    const csrf = await ensureCsrfToken();
    const response = await fetch("/api/session/refresh", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-aif-csrf": csrf,
      },
      body: "{}",
      cache: "no-store",
    });
    return response.ok;
  } catch {
    return false;
  }
}

function readCsrfCookie() {
  for (const pair of document.cookie.split(";")) {
    const [name, ...value] = pair.trim().split("=");
    if (CSRF_COOKIE_NAMES.includes(name)) {
      try {
        return decodeURIComponent(value.join("="));
      } catch {
        return null;
      }
    }
  }
  return null;
}

function isMutation(method: string | undefined) {
  return !["GET", "HEAD", "OPTIONS"].includes((method ?? "GET").toUpperCase());
}

function dispatchSessionExpired() {
  window.dispatchEvent(new Event(SESSION_EXPIRED_EVENT));
}

function responseErrorDetails(payload: unknown) {
  if (!payload || typeof payload !== "object") {
    return { code: null, message: null };
  }

  const candidate = payload as {
    code?: unknown;
    error?: unknown;
  };
  if (typeof candidate.error === "string") {
    return {
      code: typeof candidate.code === "string" ? candidate.code : null,
      message: candidate.error,
    };
  }
  if (!candidate.error || typeof candidate.error !== "object") {
    return {
      code: typeof candidate.code === "string" ? candidate.code : null,
      message: null,
    };
  }

  const error = candidate.error as { code?: unknown; message?: unknown };
  return {
    code:
      typeof error.code === "string"
        ? error.code
        : typeof candidate.code === "string"
          ? candidate.code
          : null,
    message: typeof error.message === "string" ? error.message : null,
  };
}

function knownConsoleFailure(
  code: string | null,
  message: string | null,
  status: number,
  t: Translate,
) {
  const kind = consoleFailureKind(code, message, status);
  return kind ? consoleFailureText(kind, t) : null;
}

type ConsoleFailureKind =
  | "session"
  | "verification"
  | "security_policy"
  | "forbidden"
  | "too_large"
  | "invalid_request"
  | "timeout"
  | "unavailable"
  | "rate_limit"
  | "billing"
  | "budget"
  | "model"
  | "conflict"
  | "missing"
  | "provider_image";

function consoleFailureKind(
  code: string | null,
  message: string | null,
  status: number,
): ConsoleFailureKind | null {
  const value = code ?? "";
  if (
    ["console_gateway_unauthorized", "invalid_api_key", "invalid_credentials", "invalid_token"]
      .includes(value)
    || (message === "Unauthorized" && status === 401)
    || status === 401
  ) return "session";
  if (
    message === "CSRF validation failed"
    || message === "Could not initialize a secure session"
    || message === "The CSRF cookie was not set"
  ) return "verification";
  if (
    message === "Cross-origin mutation rejected"
    || message === "Untrusted development host"
    || message === "ADMIN_CONSOLE_ORIGIN is required in production"
  ) return "security_policy";
  if (
    ["console_gateway_route_not_allowed", "insufficient_scope"].includes(value)
    || message === "Gateway route is not allowed"
    || status === 403
  ) return "forbidden";
  if (
    ["console_gateway_request_too_large", "request_too_large"].includes(value)
    || message === "Gateway request body is too large"
    || status === 413
  ) return "too_large";
  if (
    [
      "console_gateway_request_invalid",
      "unsupported_media_type",
      "unsupported_parameter",
      "unknown_parameter",
    ].includes(value)
    || message === "Gateway request body is invalid"
    || message === "JSON content type is required"
    || message === "Unsupported content type"
    || status === 415
  ) return "invalid_request";
  if (
    ["console_gateway_timeout", "timeout"].includes(value)
    || message === "Gateway request timed out"
    || status === 504
  ) return "timeout";
  if (
    [
      "console_gateway_unavailable",
      "service_unavailable",
      "configuration_error",
      "internal_error",
    ].includes(value)
    || message === "Gateway request failed"
    || status === 502
    || status === 503
  ) return "unavailable";
  if (
    value === "rate_limit_exceeded"
    || message === "Rate limit reached for image generation requests"
    || status === 429
  ) return "rate_limit";
  if (value === "billing_limit_exceeded") return "billing";
  if (value === "project_budget_exceeded") return "budget";
  if (
    value === "model_not_found"
    || message === "xAI model is not bound by Grok CLI"
    || message === "xAI video model is not bound by this Grok CLI workflow"
  ) return "model";
  if (["idempotency_conflict", "idempotency_in_progress"].includes(value) || status === 409) {
    return "conflict";
  }
  if (status === 404) return "missing";
  if (["image_generation_failed", "codex_cli_failed", "codex_no_image_output"].includes(value)) {
    return "provider_image";
  }
  return null;
}

function consoleFailureText(kind: ConsoleFailureKind, t: Translate) {
  switch (kind) {
    case "session":
      return t({ en: "Your session is no longer valid. Sign in again.", "zh-CN": "当前会话已失效，请重新登录。", ja: "セッションが無効になりました。もう一度ログインしてください。", ko: "세션이 더 이상 유효하지 않습니다. 다시 로그인하세요." });
    case "verification":
      return t({ en: "Secure request verification failed. Reload the page and try again.", "zh-CN": "安全请求校验失败，请刷新页面后重试。", ja: "安全なリクエストの検証に失敗しました。ページを再読み込みして再試行してください。", ko: "보안 요청 검증에 실패했습니다. 페이지를 새로고침한 후 다시 시도하세요." });
    case "security_policy":
      return t({ en: "The request was blocked by the console security policy.", "zh-CN": "请求已被控制台安全策略拦截。", ja: "コンソールのセキュリティポリシーによりリクエストがブロックされました。", ko: "콘솔 보안 정책에 의해 요청이 차단되었습니다." });
    case "forbidden":
      return t({ en: "You do not have permission to perform this action.", "zh-CN": "你没有执行此操作的权限。", ja: "この操作を実行する権限がありません。", ko: "이 작업을 수행할 권한이 없습니다." });
    case "too_large":
      return t({ en: "The request is too large.", "zh-CN": "请求内容过大。", ja: "リクエストのサイズが大きすぎます。", ko: "요청 크기가 너무 큽니다." });
    case "invalid_request":
      return t({ en: "The request format or parameters are not supported.", "zh-CN": "请求格式或参数不受支持。", ja: "リクエスト形式またはパラメータがサポートされていません。", ko: "요청 형식 또는 매개변수가 지원되지 않습니다." });
    case "timeout":
      return t({ en: "The service took too long to respond. Try again.", "zh-CN": "服务响应超时，请重试。", ja: "サービスの応答がタイムアウトしました。もう一度お試しください。", ko: "서비스 응답 시간이 초과되었습니다. 다시 시도하세요." });
    case "unavailable":
      return t({ en: "The service is temporarily unavailable. Try again shortly.", "zh-CN": "服务暂时不可用，请稍后重试。", ja: "サービスは一時的に利用できません。しばらくしてから再試行してください。", ko: "서비스를 일시적으로 사용할 수 없습니다. 잠시 후 다시 시도하세요." });
    case "rate_limit":
      return t({ en: "Too many requests were submitted. Try again shortly.", "zh-CN": "请求过于频繁，请稍后重试。", ja: "リクエストが多すぎます。しばらくしてから再試行してください。", ko: "요청이 너무 많습니다. 잠시 후 다시 시도하세요." });
    case "billing":
      return t({ en: "The organization does not have enough billing capacity. Contact an administrator.", "zh-CN": "组织计费可用额度不足，请联系管理员。", ja: "組織の請求可能額が不足しています。管理者に連絡してください。", ko: "조직의 청구 가능 한도가 부족합니다. 관리자에게 문의하세요." });
    case "budget":
      return t({ en: "This project has reached its spending limit.", "zh-CN": "当前项目已达到消费限额。", ja: "このプロジェクトは使用上限に達しています。", ko: "이 프로젝트가 지출 한도에 도달했습니다." });
    case "model":
      return t({ en: "The selected model is not available on the assigned provider account.", "zh-CN": "所选模型在分配的供应商账户中不可用。", ja: "選択したモデルは割り当て先のプロバイダーアカウントで利用できません。", ko: "선택한 모델을 할당된 공급자 계정에서 사용할 수 없습니다." });
    case "conflict":
      return t({ en: "The resource changed or the same request is already being processed. Refresh and try again.", "zh-CN": "资源已发生变化或相同请求正在处理中，请刷新后重试。", ja: "リソースが変更されたか、同じリクエストが処理中です。更新して再試行してください。", ko: "리소스가 변경되었거나 동일한 요청이 처리 중입니다. 새로고침 후 다시 시도하세요." });
    case "missing":
      return t({ en: "The requested resource is no longer available.", "zh-CN": "请求的资源已不存在。", ja: "リクエストされたリソースは利用できません。", ko: "요청한 리소스를 더 이상 사용할 수 없습니다." });
    case "provider_image":
      return t({ en: "The provider could not complete the image request.", "zh-CN": "上游服务未能完成图片请求。", ja: "プロバイダーが画像リクエストを完了できませんでした。", ko: "공급자가 이미지 요청을 완료하지 못했습니다." });
  }
}

function clientFailure(code: ConsoleClientErrorCode, t: Translate) {
  return code === "csrf_cookie_missing"
    ? t({
      en: "The browser did not accept the secure session cookie. Check cookie settings and try again.",
      "zh-CN": "浏览器未接受安全会话 Cookie，请检查 Cookie 设置后重试。",
      ja: "ブラウザが安全なセッション Cookie を受け入れませんでした。Cookie 設定を確認して再試行してください。",
      ko: "브라우저가 보안 세션 쿠키를 허용하지 않았습니다. 쿠키 설정을 확인한 후 다시 시도하세요.",
    })
    : t({
      en: "A secure session could not be initialized. Reload the page and try again.",
      "zh-CN": "无法初始化安全会话，请刷新页面后重试。",
      ja: "安全なセッションを初期化できませんでした。ページを再読み込みして再試行してください。",
      ko: "보안 세션을 초기화할 수 없습니다. 페이지를 새로고침한 후 다시 시도하세요.",
    });
}

function joinFailure(primary: string, detail: string, t: Translate) {
  return t(
    {
      en: "{primary} {detail}",
      "zh-CN": "{primary}：{detail}",
      ja: "{primary}：{detail}",
      ko: "{primary}: {detail}",
    },
    { primary, detail },
  );
}

function technicalFailure(primary: string, detail: string, t: Translate) {
  return t(
    {
      en: "{primary} Technical details: {detail}",
      "zh-CN": "{primary}。技术详情：{detail}",
      ja: "{primary} 技術詳細: {detail}",
      ko: "{primary} 기술 세부 정보: {detail}",
    },
    { primary, detail },
  );
}
