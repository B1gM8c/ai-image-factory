"use client";

import { useCallback, useEffect, useState } from "react";
import { Bell, Check, LoaderCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { consoleFetch } from "@/lib/auth/client";

type SpendNotification = {
  delivery_id: string;
  event_id: string;
  project_id: string;
  project_name: string;
  currency: string;
  threshold_percent: number;
  monthly_budget_micros: string;
  spend_micros: string;
  created_at_ms: number;
  read_at_ms: number | null;
};

type NotificationList = {
  object: "list";
  data: SpendNotification[];
  unread_count: number;
};

const REFRESH_INTERVAL_MS = 60_000;

export function NotificationMenu() {
  const [notifications, setNotifications] = useState<SpendNotification[]>([]);
  const [unreadCount, setUnreadCount] = useState(0);
  const [loading, setLoading] = useState(false);

  const loadNotifications = useCallback(async () => {
    setLoading(true);
    try {
      const response = await consoleFetch(
        "/api/gateway/v1/console/notifications?limit=20",
      );
      if (!response.ok) return;
      const payload = (await response.json()) as NotificationList;
      setNotifications(payload.data);
      setUnreadCount(payload.unread_count);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadNotifications();
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void loadNotifications();
    }, REFRESH_INTERVAL_MS);
    const onVisibilityChange = () => {
      if (document.visibilityState === "visible") void loadNotifications();
    };
    document.addEventListener("visibilitychange", onVisibilityChange);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [loadNotifications]);

  async function markRead(notification: SpendNotification) {
    if (notification.read_at_ms !== null) return;
    const response = await consoleFetch(
      `/api/gateway/v1/console/notifications/${encodeURIComponent(notification.delivery_id)}/read`,
      { method: "POST" },
    );
    if (!response.ok) return;
    const updated = (await response.json()) as SpendNotification;
    setNotifications((current) =>
      current.map((item) =>
        item.delivery_id === updated.delivery_id ? updated : item,
      ),
    );
    setUnreadCount((current) => Math.max(0, current - 1));
  }

  const badge = unreadCount > 99 ? "99+" : String(unreadCount);

  return (
    <DropdownMenu
      onOpenChange={(open) => {
        if (open) void loadNotifications();
      }}
    >
      <DropdownMenuTrigger asChild>
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="relative"
          aria-label={unreadCount > 0 ? `通知，${unreadCount} 条未读` : "通知"}
          title="通知"
        >
          <Bell aria-hidden="true" />
          {unreadCount > 0 ? (
            <span className="absolute right-0 top-0 flex min-w-4 -translate-y-0.5 translate-x-0.5 items-center justify-center rounded-full bg-destructive px-1 text-[9px] font-medium leading-4 text-destructive-foreground">
              {badge}
            </span>
          ) : null}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent
        align="end"
        sideOffset={6}
        className="w-[min(24rem,calc(100vw-2rem))] p-0"
      >
        <DropdownMenuLabel className="flex h-11 items-center justify-between px-3 font-medium">
          <span>通知</span>
          <span className="text-xs font-normal text-muted-foreground">
            {unreadCount > 0 ? `${unreadCount} 条未读` : "全部已读"}
          </span>
        </DropdownMenuLabel>
        <DropdownMenuSeparator className="m-0" />
        <div className="max-h-[min(28rem,70vh)] overflow-y-auto p-1">
          {notifications.length > 0 ? (
            notifications.map((notification) => (
              <DropdownMenuItem
                key={notification.delivery_id}
                className="items-start gap-3 px-3 py-3"
                onSelect={() => void markRead(notification)}
              >
                <span
                  className={
                    notification.read_at_ms === null
                      ? "mt-1.5 size-2 shrink-0 rounded-full bg-foreground"
                      : "mt-1.5 size-2 shrink-0 rounded-full border"
                  }
                  aria-hidden="true"
                />
                <span className="min-w-0 flex-1">
                  <span className="block text-sm font-medium">
                    {notification.project_name} 的预算已达到{" "}
                    {notification.threshold_percent}%
                  </span>
                  <span className="mt-1 block text-xs text-muted-foreground">
                    本月已用{" "}
                    {formatMicros(
                      notification.spend_micros,
                      notification.currency,
                    )}
                    ，预算{" "}
                    {formatMicros(
                      notification.monthly_budget_micros,
                      notification.currency,
                    )}
                  </span>
                  <span className="mt-1.5 block text-xs text-muted-foreground">
                    {formatDateTime(notification.created_at_ms)}
                  </span>
                </span>
                {notification.read_at_ms !== null ? (
                  <Check className="mt-0.5 size-4 shrink-0 text-muted-foreground" aria-label="已读" />
                ) : null}
              </DropdownMenuItem>
            ))
          ) : (
            <div className="grid min-h-28 place-items-center px-4 text-sm text-muted-foreground">
              {loading ? (
                <LoaderCircle className="size-4 animate-spin" aria-label="正在加载通知" />
              ) : (
                "暂无通知"
              )}
            </div>
          )}
        </div>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function formatMicros(value: string, currency: string) {
  try {
    const micros = BigInt(value);
    const whole = micros / 1_000_000n;
    const fraction = micros % 1_000_000n;
    const amount = Number(`${whole}.${fraction.toString().padStart(6, "0")}`);
    return new Intl.NumberFormat("zh-CN", {
      style: "currency",
      currency,
      maximumFractionDigits: 2,
    }).format(amount);
  } catch {
    return `${currency} ${value}`;
  }
}

function formatDateTime(value: number) {
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}
