"use client";

import {
  isConsoleSessionStatus,
  type ConsoleSessionStatus,
} from "@/lib/auth/types";

const CSRF_COOKIE_NAMES = ["__Host-aif_csrf", "aif_csrf"];
const SESSION_EXPIRED_EVENT = "aif:session-expired";
const REFRESH_LOCK = "aif-session-refresh";
const REFRESH_LEASE_KEY = "aif-session-refresh-lease";
const REFRESH_LEASE_MS = 10_000;
const REFRESH_LEASE_SETTLE_MS = 50;

type ConsoleFetchOptions = {
  retryUnauthorized?: boolean;
};

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
  if (!response.ok) throw new Error("无法初始化安全会话");
  const token = readCsrfCookie();
  if (!token) throw new Error("CSRF Cookie 未写入");
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
