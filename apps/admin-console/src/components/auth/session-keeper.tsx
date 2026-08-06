"use client";

import { useEffect } from "react";
import {
  onSessionExpired,
  refreshConsoleSession,
} from "@/lib/auth/client";
import { useConsoleSession } from "@/components/auth/console-session-provider";

const REFRESH_EARLY_SECONDS = 60;
const FALLBACK_CHECK_MS = 4 * 60 * 1_000;

export function SessionKeeper() {
  const { loading, reload, session } = useConsoleSession();

  useEffect(() => {
    if (loading) return;
    let active = true;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const expire = () => {
      if (!active) return;
      active = false;
      if (timer) clearTimeout(timer);
      window.location.replace("/login?reason=session_expired");
    };

    const schedule = async () => {
      if (!active) return;
      if (timer) clearTimeout(timer);

      const current = session ?? (await reload());
      if (!current?.authenticated) {
        expire();
        return;
      }
      if (current.mode === "emergency") return;

      const now = Math.floor(Date.now() / 1000);
      const refreshAt = current.access_expires_at
        ? Math.max(0, current.access_expires_at - now - REFRESH_EARLY_SECONDS) * 1_000
        : 0;
      if ((!current.access_expires_at || refreshAt === 0) && current.refresh_available) {
        if (!(await refreshConsoleSession())) {
          expire();
          return;
        }
        await reload();
        if (active) timer = setTimeout(schedule, FALLBACK_CHECK_MS);
        return;
      }

      timer = setTimeout(schedule, refreshAt || FALLBACK_CHECK_MS);
    };

    const stopListening = onSessionExpired(expire);
    const onVisible = () => {
      if (document.visibilityState === "visible") void schedule();
    };
    document.addEventListener("visibilitychange", onVisible);
    void schedule();

    return () => {
      active = false;
      if (timer) clearTimeout(timer);
      stopListening();
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [loading, reload, session]);

  return null;
}
