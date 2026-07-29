"use client";

import { AdminApiError } from "@/lib/admin/client";
import { consoleFetch } from "@/lib/auth/client";

const SYSTEM_UPDATE_PATH = "/admin/v1/system/update";

export type SystemUpdateCommand = {
  object: string;
  command_id: string;
  action: string;
  target_version: string | null;
  status: string;
  phase: string;
  progress: Record<string, unknown>;
  failure_code: string | null;
  failure_message: string | null;
  requested_at_ms: number;
  started_at_ms: number | null;
  completed_at_ms: number | null;
  updated_at_ms: number;
};

export type SystemUpdateSnapshot = {
  object: string;
  configured: boolean;
  apply_enabled: boolean;
  repository: string | null;
  target_triple: string;
  current_version: string;
  current_commit_sha: string | null;
  previous_version: string | null;
  latest_version: string | null;
  latest_commit_sha: string | null;
  latest_verified: boolean;
  update_available: boolean;
  last_checked_at_ms: number | null;
  last_applied_at_ms: number | null;
  last_error_code: string | null;
  last_error_message: string | null;
  active_command: SystemUpdateCommand | null;
  recent_commands: SystemUpdateCommand[];
};

export function getSystemUpdate(signal?: AbortSignal) {
  return systemUpdateRequest<SystemUpdateSnapshot>(SYSTEM_UPDATE_PATH, { signal });
}

export function checkSystemUpdate() {
  return systemUpdateRequest<SystemUpdateCommand>(`${SYSTEM_UPDATE_PATH}/check`, {
    method: "POST",
    headers: { "Idempotency-Key": crypto.randomUUID() },
    body: "{}",
  });
}

export function applySystemUpdate(targetVersion: string) {
  return systemUpdateRequest<SystemUpdateCommand>(`${SYSTEM_UPDATE_PATH}/apply`, {
    method: "POST",
    headers: { "Idempotency-Key": crypto.randomUUID() },
    body: JSON.stringify({ target_version: targetVersion }),
  });
}

async function systemUpdateRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await consoleFetch(`/api/gateway${path}`, init);
  if (!response.ok) {
    throw new AdminApiError(await responseMessage(response), response.status);
  }
  return (await response.json()) as T;
}

async function responseMessage(response: Response) {
  try {
    const payload = (await response.json()) as {
      error?: string | { message?: string };
    };
    if (typeof payload.error === "string") return payload.error;
    if (payload.error?.message) return payload.error.message;
  } catch {
    // Keep upstream response bodies private and use a stable operator-facing fallback.
  }
  if (response.status === 403) return "当前账号没有管理系统更新的权限";
  if (response.status === 409) return "已有系统更新命令正在执行";
  return "系统更新服务暂时不可用";
}
