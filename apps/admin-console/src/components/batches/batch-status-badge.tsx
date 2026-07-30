"use client";

import { Badge } from "@/components/ui/badge";
import type { ProjectBatchStatus } from "@/lib/admin/types";
import { useI18n } from "@/i18n/locale-provider";

type Translate = ReturnType<typeof useI18n>["t"];

const STATUS_LABELS: Record<
  ProjectBatchStatus,
  { en: string; "zh-CN": string; ja: string; ko: string }
> = {
  validating: {
    en: "Validating",
    "zh-CN": "正在验证",
    ja: "検証中",
    ko: "검증 중",
  },
  failed: { en: "Failed", "zh-CN": "失败", ja: "失敗", ko: "실패" },
  in_progress: {
    en: "In progress",
    "zh-CN": "处理中",
    ja: "処理中",
    ko: "처리 중",
  },
  finalizing: {
    en: "Finalizing",
    "zh-CN": "正在汇总",
    ja: "最終処理中",
    ko: "마무리 중",
  },
  completed: {
    en: "Completed",
    "zh-CN": "已完成",
    ja: "完了",
    ko: "완료",
  },
  expired: {
    en: "Expired",
    "zh-CN": "已过期",
    ja: "期限切れ",
    ko: "만료됨",
  },
  cancelling: {
    en: "Cancelling",
    "zh-CN": "正在取消",
    ja: "キャンセル中",
    ko: "취소 중",
  },
  cancelled: {
    en: "Cancelled",
    "zh-CN": "已取消",
    ja: "キャンセル済み",
    ko: "취소됨",
  },
};

export function BatchStatusBadge({ status }: { status: ProjectBatchStatus }) {
  const { t } = useI18n();

  return (
    <Badge
      variant={
        status === "completed"
          ? "default"
          : status === "failed" || status === "expired"
            ? "destructive"
            : "secondary"
      }
    >
      {batchStatusLabel(t, status)}
    </Badge>
  );
}

export function batchStatusLabel(t: Translate, status: ProjectBatchStatus) {
  return t(STATUS_LABELS[status]);
}
