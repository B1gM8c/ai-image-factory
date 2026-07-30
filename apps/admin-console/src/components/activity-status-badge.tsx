"use client";

import { Badge } from "@/components/ui/badge";
import { useI18n } from "@/i18n/locale-provider";

type Translate = ReturnType<typeof useI18n>["t"];

export function ActivityStatusBadge({ state }: { state: string }) {
  const { t } = useI18n();

  return (
    <Badge variant="outline" className="gap-1.5">
      <span
        className={`size-1.5 rounded-full ${statusDot(state)}`}
        aria-hidden="true"
      />
      {formatActivityStatus(t, state)}
    </Badge>
  );
}

export function formatActivityStatus(t: Translate, value: string): string {
  const labels: Record<
    string,
    { en: string; "zh-CN": string; ja: string; ko: string }
  > = {
    active: { en: "Active", "zh-CN": "活跃", ja: "有効", ko: "활성" },
    available: { en: "Available", "zh-CN": "可用", ja: "利用可能", ko: "사용 가능" },
    awaiting_executor: {
      en: "Awaiting executor",
      "zh-CN": "等待执行器",
      ja: "実行ワーカー待ち",
      ko: "실행기 대기",
    },
    blocked: { en: "Blocked", "zh-CN": "阻断", ja: "ブロック中", ko: "차단됨" },
    captured: { en: "Captured", "zh-CN": "已扣款", ja: "売上確定", ko: "결제 확정" },
    charged: { en: "Metered", "zh-CN": "已计量", ja: "計測済み", ko: "계측됨" },
    completed: { en: "Completed", "zh-CN": "已完成", ja: "完了", ko: "완료" },
    configured: { en: "Configured", "zh-CN": "已配置", ja: "設定済み", ko: "구성됨" },
    customer_charge: {
      en: "Customer charge",
      "zh-CN": "客户扣费",
      ja: "顧客請求",
      ko: "고객 청구",
    },
    customer_refund: {
      en: "Customer refund",
      "zh-CN": "客户退款",
      ja: "顧客返金",
      ko: "고객 환불",
    },
    delayed: { en: "Delayed", "zh-CN": "延后", ja: "遅延", ko: "지연됨" },
    draining: { en: "Draining", "zh-CN": "排空中", ja: "ドレイン中", ko: "드레이닝 중" },
    due: { en: "Due", "zh-CN": "已到期", ja: "期限到来", ko: "기한 도래" },
    enabled: { en: "Enabled", "zh-CN": "已启用", ja: "有効", ko: "활성화됨" },
    failed: { en: "Failed", "zh-CN": "失败", ja: "失敗", ko: "실패" },
    in_progress: {
      en: "In progress",
      "zh-CN": "处理中",
      ja: "処理中",
      ko: "처리 중",
    },
    job: { en: "Job", "zh-CN": "任务", ja: "ジョブ", ko: "작업" },
    leased: { en: "Leased", "zh-CN": "已租约", ja: "リース済み", ko: "리스됨" },
    pending: { en: "Pending", "zh-CN": "待处理", ja: "保留中", ko: "대기 중" },
    provider_cost: {
      en: "Provider cost",
      "zh-CN": "Provider 成本",
      ja: "Provider コスト",
      ko: "Provider 비용",
    },
    provider_task: {
      en: "Provider task",
      "zh-CN": "Provider 任务",
      ja: "Provider タスク",
      ko: "Provider 작업",
    },
    queued: { en: "Queued", "zh-CN": "排队中", ja: "キュー待ち", ko: "대기열에 있음" },
    ready: { en: "Ready", "zh-CN": "就绪", ja: "準備完了", ko: "준비됨" },
    remote_task: {
      en: "Remote task",
      "zh-CN": "远端任务",
      ja: "リモートタスク",
      ko: "원격 작업",
    },
    running: { en: "Running", "zh-CN": "运行中", ja: "実行中", ko: "실행 중" },
    sealed: { en: "Sealed", "zh-CN": "已封账", ja: "確定済み", ko: "마감됨" },
    succeeded: { en: "Succeeded", "zh-CN": "成功", ja: "成功", ko: "성공" },
    success: { en: "Success", "zh-CN": "成功", ja: "成功", ko: "성공" },
    api_key: { en: "API Key", "zh-CN": "API Key", ja: "API Key", ko: "API Key" },
    user_session: {
      en: "User session",
      "zh-CN": "用户会话",
      ja: "ユーザーセッション",
      ko: "사용자 세션",
    },
    submission: { en: "Submission", "zh-CN": "提交", ja: "送信", ko: "제출" },
    uncertain: { en: "Uncertain", "zh-CN": "不确定", ja: "不確定", ko: "불확실" },
    unknown: { en: "Unknown", "zh-CN": "未知", ja: "不明", ko: "알 수 없음" },
    unobserved: {
      en: "Unobserved",
      "zh-CN": "未观测",
      ja: "未観測",
      ko: "관측되지 않음",
    },
    waiting: { en: "Waiting", "zh-CN": "等待中", ja: "待機中", ko: "대기 중" },
    work_item: {
      en: "Work item",
      "zh-CN": "工作项",
      ja: "作業項目",
      ko: "작업 항목",
    },
  };
  const normalized = value.toLowerCase();
  const label = labels[normalized];
  if (label) return t(label);
  return value.replaceAll("_", " ");
}

export function formatActivityOperation(t: Translate, value: string): string {
  const labels: Record<
    string,
    { en: string; "zh-CN": string; ja: string; ko: string }
  > = {
    edit: {
      en: "Image edit",
      "zh-CN": "图片编辑",
      ja: "画像編集",
      ko: "이미지 편집",
    },
    generation: {
      en: "Image generation",
      "zh-CN": "图片生成",
      ja: "画像生成",
      ko: "이미지 생성",
    },
    video_generation: {
      en: "Video generation",
      "zh-CN": "视频生成",
      ja: "動画生成",
      ko: "동영상 생성",
    },
  };
  const normalized = value.toLowerCase();
  const label = labels[normalized];
  if (label) return t(label);
  return value.replaceAll("_", " ");
}

function statusDot(state: string): string {
  switch (state.toLowerCase()) {
    case "completed":
    case "succeeded":
      return "bg-emerald-500";
    case "failed":
      return "bg-destructive";
    case "uncertain":
      return "bg-amber-500";
    case "running":
      return "bg-sky-500";
    default:
      return "bg-muted-foreground";
  }
}
