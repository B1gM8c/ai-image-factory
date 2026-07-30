import type { LocalizedText } from "@/i18n/config";

export const WEBHOOK_EVENT_TYPES = [
  "image.generation.completed",
  "image.generation.failed",
  "image.edit.completed",
  "image.edit.failed",
  "video.generation.completed",
  "video.generation.failed",
] as const;

export type WebhookEventType = (typeof WEBHOOK_EVENT_TYPES)[number];
export type WebhookEndpointState = "active" | "disabled";
export type WebhookDeliveryState =
  | "pending"
  | "leased"
  | "retry_wait"
  | "succeeded"
  | "dead_lettered"
  | "canceled";

export type ProjectWebhookEndpoint = {
  object: "organization.project.webhook";
  id: string;
  project_id: string;
  name: string | null;
  url: string;
  event_types: WebhookEventType[];
  state: WebhookEndpointState;
  signing_key_version: number;
  secret_revision: number;
  control_version: number;
  last_delivery_state: WebhookDeliveryState | null;
  last_delivery_at_ms: number | null;
  created_at: number;
  updated_at: number;
};

export type CreatedProjectWebhook = {
  object: "organization.project.webhook.created";
  endpoint: ProjectWebhookEndpoint;
  signing_secret: string;
};

export type RotatedProjectWebhookSecret = {
  object: "organization.project.webhook.secret";
  endpoint_id: string;
  signing_key_version: number;
  secret_revision: number;
  control_version: number;
  signing_secret: string;
};

export type ProjectWebhookDelivery = {
  object: "organization.project.webhook.delivery";
  id: string;
  event_id: string;
  event_type: string;
  endpoint_id: string;
  state: WebhookDeliveryState;
  attempt_count: number;
  next_attempt_at_ms: number;
  retry_deadline_at_ms: number;
  last_http_status: number | null;
  last_error_code: string | null;
  last_attempt_at_ms: number | null;
  delivered_at_ms: number | null;
  created_at_ms: number;
  updated_at_ms: number;
};

export const WEBHOOK_EVENT_LABELS: Record<WebhookEventType, LocalizedText> = {
  "image.generation.completed": { en: "Image generation completed", "zh-CN": "图片生成完成", ja: "画像生成が完了", ko: "이미지 생성 완료" },
  "image.generation.failed": { en: "Image generation failed", "zh-CN": "图片生成失败", ja: "画像生成に失敗", ko: "이미지 생성 실패" },
  "image.edit.completed": { en: "Image edit completed", "zh-CN": "图片编辑完成", ja: "画像編集が完了", ko: "이미지 편집 완료" },
  "image.edit.failed": { en: "Image edit failed", "zh-CN": "图片编辑失败", ja: "画像編集に失敗", ko: "이미지 편집 실패" },
  "video.generation.completed": { en: "Video generation completed", "zh-CN": "视频生成完成", ja: "動画生成が完了", ko: "동영상 생성 완료" },
  "video.generation.failed": { en: "Video generation failed", "zh-CN": "视频生成失败", ja: "動画生成に失敗", ko: "동영상 생성 실패" },
};

export const WEBHOOK_DELIVERY_LABELS: Record<WebhookDeliveryState, LocalizedText> = {
  pending: { en: "Pending", "zh-CN": "等待投递", ja: "配信待ち", ko: "전송 대기" },
  leased: { en: "Delivering", "zh-CN": "投递中", ja: "配信中", ko: "전송 중" },
  retry_wait: { en: "Waiting to retry", "zh-CN": "等待重试", ja: "再試行待ち", ko: "재시도 대기" },
  succeeded: { en: "Succeeded", "zh-CN": "已成功", ja: "成功", ko: "성공" },
  dead_lettered: { en: "Permanently failed", "zh-CN": "永久失败", ja: "永続的な失敗", ko: "영구 실패" },
  canceled: { en: "Canceled", "zh-CN": "已取消", ja: "キャンセル済み", ko: "취소됨" },
};
