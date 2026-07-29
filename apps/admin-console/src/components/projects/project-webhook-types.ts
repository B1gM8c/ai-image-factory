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

export const WEBHOOK_EVENT_LABELS: Record<WebhookEventType, string> = {
  "image.generation.completed": "图片生成完成",
  "image.generation.failed": "图片生成失败",
  "image.edit.completed": "图片编辑完成",
  "image.edit.failed": "图片编辑失败",
  "video.generation.completed": "视频生成完成",
  "video.generation.failed": "视频生成失败",
};

export const WEBHOOK_DELIVERY_LABELS: Record<WebhookDeliveryState, string> = {
  pending: "等待投递",
  leased: "投递中",
  retry_wait: "等待重试",
  succeeded: "已成功",
  dead_lettered: "永久失败",
  canceled: "已取消",
};
