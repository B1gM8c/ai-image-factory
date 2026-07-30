"use client";

import { useCallback, useEffect, useState } from "react";
import { useI18n } from "@/i18n/locale-provider";
import { AdminApiError, fetchAdminJson } from "@/lib/admin/client";

type AdminQueryState<T> = {
  data: T | null;
  error: AdminApiError | null;
  loading: boolean;
  refreshing: boolean;
  retry: () => void;
};

export function useAdminQuery<T>(
  path: string,
  enabled = true,
  refreshIntervalMs?: number,
): AdminQueryState<T> {
  const { t } = useI18n();
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<AdminApiError | null>(null);
  const [request, setRequest] = useState(0);
  const [pending, setPending] = useState(true);

  useEffect(() => {
    if (!enabled) {
      setData(null);
      setError(null);
      setPending(false);
      return;
    }
    const controller = new AbortController();
    setPending(true);
    fetchAdminJson<T>(path, controller.signal)
      .then((value) => {
        setData(value);
        setError(null);
      })
      .catch((caught: unknown) => {
        if (controller.signal.aborted) return;
        setError(
          caught instanceof AdminApiError
            ? caught
            : new AdminApiError(
                t({
                  en: "Admin data is temporarily unavailable",
                  "zh-CN": "管理数据暂时不可用",
                  ja: "管理データは一時的に利用できません",
                  ko: "관리 데이터를 일시적으로 사용할 수 없습니다",
                }),
                0,
              ),
        );
      })
      .finally(() => {
        if (!controller.signal.aborted) setPending(false);
      });
    return () => controller.abort();
  }, [enabled, path, request, t]);

  const retry = useCallback(() => setRequest((value) => value + 1), []);

  useEffect(() => {
    if (!enabled || !refreshIntervalMs) return;
    const interval = window.setInterval(retry, refreshIntervalMs);
    return () => window.clearInterval(interval);
  }, [enabled, refreshIntervalMs, retry]);

  return {
    data,
    error,
    loading: pending && data === null,
    refreshing: pending && data !== null,
    retry,
  };
}
