"use client";

import { useCallback, useEffect, useState } from "react";
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
            : new AdminApiError("管理数据暂时不可用", 0),
        );
      })
      .finally(() => {
        if (!controller.signal.aborted) setPending(false);
      });
    return () => controller.abort();
  }, [enabled, path, request]);

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
