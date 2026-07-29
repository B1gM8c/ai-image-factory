import { Badge } from "@/components/ui/badge";
import type { ProjectBatchStatus } from "@/lib/admin/types";

const STATUS_LABELS: Record<ProjectBatchStatus, string> = {
  validating: "正在验证",
  failed: "失败",
  in_progress: "处理中",
  finalizing: "正在汇总",
  completed: "已完成",
  expired: "已过期",
  cancelling: "正在取消",
  cancelled: "已取消",
};

export function BatchStatusBadge({ status }: { status: ProjectBatchStatus }) {
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
      {STATUS_LABELS[status]}
    </Badge>
  );
}

export function batchStatusLabel(status: ProjectBatchStatus) {
  return STATUS_LABELS[status];
}
