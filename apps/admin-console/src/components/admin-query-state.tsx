"use client";

import { RefreshCw, ShieldX } from "lucide-react";
import { AdminApiError } from "@/lib/admin/client";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useI18n } from "@/i18n/locale-provider";

export function AdminQuerySkeleton({ rows = 4 }: { rows?: number }) {
  const { t } = useI18n();

  return (
    <div
      className="space-y-3"
      aria-label={t({
        en: "Loading",
        "zh-CN": "正在加载",
        ja: "読み込み中",
        ko: "불러오는 중",
      })}
    >
      {Array.from({ length: rows }, (_, index) => (
        <Skeleton key={index} className="h-12 w-full" />
      ))}
    </div>
  );
}

export function AdminQueryError({ error, retry }: { error: AdminApiError; retry: () => void }) {
  const { t } = useI18n();
  const forbidden = error.status === 403;
  return (
    <Card>
      <CardContent className="flex min-h-36 flex-col items-center justify-center gap-3 p-6 text-center">
        {forbidden ? (
          <ShieldX className="size-5 text-muted-foreground" aria-hidden="true" />
        ) : (
          <RefreshCw className="size-5 text-muted-foreground" aria-hidden="true" />
        )}
        <p className="text-sm text-muted-foreground">{error.message}</p>
        {!forbidden && (
          <Button type="button" variant="outline" size="sm" onClick={retry}>
            <RefreshCw className="size-4" aria-hidden="true" />
            {t({
              en: "Retry",
              "zh-CN": "重试",
              ja: "再試行",
              ko: "다시 시도",
            })}
          </Button>
        )}
      </CardContent>
    </Card>
  );
}
