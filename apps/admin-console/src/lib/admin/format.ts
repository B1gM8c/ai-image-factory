const INTEGER_PATTERN = /^-?\d+$/;
const MICROS_PER_UNIT = 1_000_000n;

export function formatInteger(value: string): string {
  const parsed = parseInteger(value);
  return parsed === null ? value : parsed.toLocaleString("zh-CN");
}

export function sumIntegers(values: readonly string[]): string {
  let total = 0n;
  for (const value of values) {
    const parsed = parseInteger(value);
    if (parsed !== null) total += parsed;
  }
  return total.toString();
}

export function formatMoneyMicros(value: string, currency: string): string {
  const parsed = parseInteger(value);
  if (parsed === null) return `${currency.toUpperCase()} ${value}`;

  const negative = parsed < 0n;
  const absolute = negative ? -parsed : parsed;
  const whole = absolute / MICROS_PER_UNIT;
  const micros = (absolute % MICROS_PER_UNIT).toString().padStart(6, "0");
  const fraction = micros.replace(/0+$/, "").padEnd(2, "0");
  return `${currency.toUpperCase()} ${negative ? "-" : ""}${formatInteger(whole.toString())}.${fraction}`;
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

export function formatDateTime(value: number | null | undefined): string {
  if (value === null || value === undefined) return "--";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

export function formatDurationMs(value: number | null): string {
  if (value === null) return "--";
  if (value < 1_000) return `${value} ms`;
  const seconds = Math.floor(value / 100) / 10;
  return `${seconds.toLocaleString("zh-CN")} s`;
}

export function formatStatus(value: string): string {
  const labels: Record<string, string> = {
    active: "活跃",
    available: "可用",
    awaiting_executor: "等待执行器",
    blocked: "阻断",
    captured: "已扣款",
    charged: "已计量",
    completed: "已完成",
    configured: "已配置",
    customer_charge: "客户扣费",
    customer_refund: "客户退款",
    delayed: "延后",
    draining: "排空中",
    due: "已到期",
    enabled: "已启用",
    failed: "失败",
    in_progress: "处理中",
    job: "任务",
    leased: "已租约",
    pending: "待处理",
    provider_cost: "Provider 成本",
    provider_task: "Provider 任务",
    queued: "排队中",
    ready: "就绪",
    remote_task: "远端任务",
    running: "运行中",
    sealed: "已封账",
    succeeded: "成功",
    success: "成功",
    api_key: "API Key",
    user_session: "用户会话",
    submission: "提交",
    uncertain: "不确定",
    unknown: "未知",
    unobserved: "未观测",
    waiting: "等待中",
    work_item: "工作项",
  };
  return labels[value.toLowerCase()] ?? value.replaceAll("_", " ");
}

export function formatOperation(value: string): string {
  const labels: Record<string, string> = {
    edit: "图片编辑",
    generation: "图片生成",
    video_generation: "视频生成",
  };
  return labels[value.toLowerCase()] ?? value.replaceAll("_", " ");
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
