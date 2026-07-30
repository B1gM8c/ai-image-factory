import {
  defaultLocale,
  formatLocalizedText,
  isLocale,
  type Locale,
  type LocalizedText,
} from "@/i18n/config";

const INTEGER_PATTERN = /^-?\d+$/;
const MICROS_PER_UNIT = 1_000_000n;

export function formatInteger(
  value: string,
  locale: Locale = currentLocale(),
): string {
  const parsed = parseInteger(value);
  return parsed === null ? value : parsed.toLocaleString(locale);
}

export function sumIntegers(values: readonly string[]): string {
  let total = 0n;
  for (const value of values) {
    const parsed = parseInteger(value);
    if (parsed !== null) total += parsed;
  }
  return total.toString();
}

export function formatMoneyMicros(
  value: string,
  currency: string,
  locale: Locale = currentLocale(),
): string {
  const parsed = parseInteger(value);
  if (parsed === null) return `${currency.toUpperCase()} ${value}`;

  const negative = parsed < 0n;
  const absolute = negative ? -parsed : parsed;
  const whole = absolute / MICROS_PER_UNIT;
  const micros = (absolute % MICROS_PER_UNIT).toString().padStart(6, "0");
  const fraction = micros.replace(/0+$/, "").padEnd(2, "0");
  return `${currency.toUpperCase()} ${negative ? "-" : ""}${formatInteger(whole.toString(), locale)}.${fraction}`;
}

export function decimalToMicros(
  value: string,
  { allowZero = false }: { allowZero?: boolean } = {},
): string | null {
  const match = /^(\d+)(?:\.(\d{0,6}))?$/.exec(value.trim());
  if (!match) return null;
  const whole = BigInt(match[1]);
  const fraction = BigInt((match[2] ?? "").padEnd(6, "0") || "0");
  const micros = whole * MICROS_PER_UNIT + fraction;
  if ((!allowZero && micros === 0n) || micros > 9_223_372_036_854_775_807n) {
    return null;
  }
  return micros.toString();
}

export function microsToDecimal(value: string): string {
  const micros = BigInt(value);
  const whole = micros / MICROS_PER_UNIT;
  const fraction = (micros % MICROS_PER_UNIT)
    .toString()
    .padStart(6, "0")
    .replace(/0+$/, "");
  return fraction ? `${whole}.${fraction}` : whole.toString();
}

export function formatDateTime(
  value: number | null | undefined,
  locale: Locale = currentLocale(),
): string {
  if (value === null || value === undefined) return "--";
  return new Intl.DateTimeFormat(locale, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

export function formatDurationMs(
  value: number | null,
  locale: Locale = currentLocale(),
): string {
  if (value === null) return "--";
  if (value < 1_000) return `${value} ms`;
  const seconds = Math.floor(value / 100) / 10;
  return `${seconds.toLocaleString(locale)} s`;
}

export function formatStatus(
  value: string,
  locale: Locale = currentLocale(),
): string {
  const label = STATUS_LABELS[value.toLowerCase()];
  return label
    ? formatLocalizedText(label, locale)
    : value.replaceAll("_", " ");
}

export function formatOperation(
  value: string,
  locale: Locale = currentLocale(),
): string {
  const label = OPERATION_LABELS[value.toLowerCase()];
  return label
    ? formatLocalizedText(label, locale)
    : value.replaceAll("_", " ");
}

export function operationEndpoint(value: string): string {
  const endpoints: Record<string, string> = {
    edit: "POST /v1/images/edits",
    generation: "POST /v1/images/generations",
    video_generation: "POST /v1/videos",
  };
  return endpoints[value.toLowerCase()] ?? value;
}

function parseInteger(value: string): bigint | null {
  if (!INTEGER_PATTERN.test(value)) return null;
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

const STATUS_LABELS: Record<string, LocalizedText> = {
  active: { en: "Active", "zh-CN": "活跃", ja: "アクティブ", ko: "활성" },
  available: { en: "Available", "zh-CN": "可用", ja: "利用可能", ko: "사용 가능" },
  awaiting_executor: {
    en: "Awaiting executor",
    "zh-CN": "等待执行器",
    ja: "実行待ち",
    ko: "실행기 대기",
  },
  blocked: { en: "Blocked", "zh-CN": "阻断", ja: "ブロック", ko: "차단" },
  captured: { en: "Captured", "zh-CN": "已扣款", ja: "決済済み", ko: "결제됨" },
  charged: { en: "Metered", "zh-CN": "已计量", ja: "計測済み", ko: "계량됨" },
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
  delayed: { en: "Delayed", "zh-CN": "延后", ja: "遅延", ko: "지연" },
  draining: { en: "Draining", "zh-CN": "排空中", ja: "ドレイン中", ko: "드레이닝" },
  due: { en: "Due", "zh-CN": "已到期", ja: "期限到来", ko: "만기" },
  enabled: { en: "Enabled", "zh-CN": "已启用", ja: "有効", ko: "활성화됨" },
  failed: { en: "Failed", "zh-CN": "失败", ja: "失敗", ko: "실패" },
  in_progress: { en: "In progress", "zh-CN": "处理中", ja: "進行中", ko: "진행 중" },
  job: { en: "Job", "zh-CN": "任务", ja: "ジョブ", ko: "작업" },
  leased: { en: "Leased", "zh-CN": "已租约", ja: "リース済み", ko: "임대됨" },
  pending: { en: "Pending", "zh-CN": "待处理", ja: "保留中", ko: "대기 중" },
  provider_cost: {
    en: "Provider cost",
    "zh-CN": "Provider 成本",
    ja: "プロバイダーコスト",
    ko: "공급자 비용",
  },
  provider_task: {
    en: "Provider task",
    "zh-CN": "Provider 任务",
    ja: "プロバイダータスク",
    ko: "공급자 작업",
  },
  queued: { en: "Queued", "zh-CN": "排队中", ja: "キュー待ち", ko: "대기열" },
  ready: { en: "Ready", "zh-CN": "就绪", ja: "準備完了", ko: "준비됨" },
  remote_task: {
    en: "Remote task",
    "zh-CN": "远端任务",
    ja: "リモートタスク",
    ko: "원격 작업",
  },
  running: { en: "Running", "zh-CN": "运行中", ja: "実行中", ko: "실행 중" },
  sealed: { en: "Sealed", "zh-CN": "已封账", ja: "確定済み", ko: "확정됨" },
  succeeded: { en: "Succeeded", "zh-CN": "成功", ja: "成功", ko: "성공" },
  success: { en: "Success", "zh-CN": "成功", ja: "成功", ko: "성공" },
  api_key: { en: "API key", "zh-CN": "API Key", ja: "API キー", ko: "API 키" },
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
    ja: "ワークアイテム",
    ko: "작업 항목",
  },
};

const OPERATION_LABELS: Record<string, LocalizedText> = {
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

function currentLocale(): Locale {
  if (typeof document === "undefined") return defaultLocale;
  return isLocale(document.documentElement.lang)
    ? document.documentElement.lang
    : defaultLocale;
}
