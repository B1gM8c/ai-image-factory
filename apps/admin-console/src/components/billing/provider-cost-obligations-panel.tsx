"use client";

import { useMemo, useState } from "react";
import {
  AlertTriangle,
  CheckCircle2,
  ChevronRight,
  Clock3,
  RefreshCw,
} from "lucide-react";
import { AdminQueryError, AdminQuerySkeleton } from "@/components/admin-query-state";
import { ProviderCostAllocationsPanel } from "@/components/billing/provider-cost-allocations-panel";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useAdminQuery } from "@/hooks/use-admin-query";
import { useI18n } from "@/i18n/locale-provider";
import { formatDateTime } from "@/lib/admin/format";
import type {
  ProviderCostObligation,
  ProviderCostObligationDetail,
  ProviderCostObligationList,
} from "@/lib/admin/types";

type StateFilter = "open" | "all" | "pending" | "expected" | "settled" | "waived";
type UrgencyFilter = "all" | "overdue" | "escalated";
type Translate = ReturnType<typeof useI18n>["t"];

export function ProviderCostObligationsPanel({ enabled }: { enabled: boolean }) {
  const { t } = useI18n();
  const [view, setView] = useState<"obligations" | "allocations">("obligations");
  return (
    <div className="min-w-0 space-y-5">
      <Tabs
        value={view}
        onValueChange={(value) =>
          setView(value as "obligations" | "allocations")
        }
      >
        <TabsList>
          <TabsTrigger value="obligations">
            {t({
              en: "Cost obligations",
              "zh-CN": "成本义务",
              ja: "コスト義務",
              ko: "비용 의무",
            })}
          </TabsTrigger>
          <TabsTrigger value="allocations">
            {t({
              en: "Allocation drafts",
              "zh-CN": "分摊草稿",
              ja: "配賦下書き",
              ko: "배분 초안",
            })}
          </TabsTrigger>
        </TabsList>
      </Tabs>
      {view === "obligations" ? (
        <ProviderCostObligationQueue enabled={enabled} />
      ) : (
        <ProviderCostAllocationsPanel enabled={enabled} />
      )}
    </div>
  );
}

function ProviderCostObligationQueue({ enabled }: { enabled: boolean }) {
  const { t } = useI18n();
  const [state, setState] = useState<StateFilter>("open");
  const [urgency, setUrgency] = useState<UrgencyFilter>("all");
  const [provider, setProvider] = useState("all");
  const [selectedReceiptId, setSelectedReceiptId] = useState<string | null>(null);
  const path = useMemo(() => {
    const params = new URLSearchParams({
      limit: "50",
      state,
      urgency,
    });
    if (provider !== "all") params.set("provider_id", provider);
    return `/admin/v1/billing/provider-cost-obligations?${params.toString()}`;
  }, [provider, state, urgency]);
  const query = useAdminQuery<ProviderCostObligationList>(path, enabled);

  return (
    <section className="space-y-4">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-base font-semibold">
            {t({
              en: "Provider costs",
              "zh-CN": "上游成本",
              ja: "プロバイダーコスト",
              ko: "공급자 비용",
            })}
          </h2>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            {t({
              en: "Track whether each provider execution has reached one authoritative cost conclusion. Overdue items enter review; they are never assumed to be free.",
              "zh-CN": "跟踪每次上游执行是否已取得唯一成本结论。逾期只会进入复核队列，不会自动视为免费。",
              ja: "各プロバイダー実行について、一意の確定コストが得られたかを追跡します。期限超過はレビュー対象になるだけで、自動的に無料とは扱いません。",
              ko: "각 공급자 실행에 대해 하나의 권위 있는 비용 결론이 도출되었는지 추적합니다. 기한이 지나면 검토 대기열로 이동하며 자동으로 무료 처리되지 않습니다.",
            })}
          </p>
        </div>
        <Button
          type="button"
          variant="outline"
          size="icon"
          aria-label={t({
            en: "Refresh provider costs",
            "zh-CN": "刷新上游成本",
            ja: "プロバイダーコストを更新",
            ko: "공급자 비용 새로 고침",
          })}
          onClick={query.retry}
          disabled={query.refreshing}
        >
          <RefreshCw
            className={query.refreshing ? "animate-spin" : ""}
            aria-hidden="true"
          />
        </Button>
      </div>

      {query.data ? <SummaryBand data={query.data} /> : null}

      <div className="flex flex-wrap items-center gap-2">
        <Select value={state} onValueChange={(value) => setState(value as StateFilter)}>
          <SelectTrigger
            className="w-full sm:w-40"
            aria-label={t({
              en: "Filter by processing status",
              "zh-CN": "筛选处理状态",
              ja: "処理状態で絞り込む",
              ko: "처리 상태로 필터링",
            })}
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="open">
              {t({
                en: "Needs attention",
                "zh-CN": "需要处理",
                ja: "対応が必要",
                ko: "처리 필요",
              })}
            </SelectItem>
            <SelectItem value="all">
              {t({
                en: "All records",
                "zh-CN": "全部记录",
                ja: "すべての記録",
                ko: "모든 기록",
              })}
            </SelectItem>
            <SelectItem value="expected">{stateLabel(t, "expected")}</SelectItem>
            <SelectItem value="pending">{stateLabel(t, "pending")}</SelectItem>
            <SelectItem value="settled">{stateLabel(t, "settled")}</SelectItem>
            <SelectItem value="waived">{stateLabel(t, "waived")}</SelectItem>
          </SelectContent>
        </Select>
        <Select
          value={urgency}
          onValueChange={(value) => setUrgency(value as UrgencyFilter)}
        >
          <SelectTrigger
            className="w-full sm:w-36"
            aria-label={t({
              en: "Filter by deadline",
              "zh-CN": "筛选处理时限",
              ja: "処理期限で絞り込む",
              ko: "처리 기한으로 필터링",
            })}
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">
              {t({
                en: "All deadlines",
                "zh-CN": "全部时限",
                ja: "すべての期限",
                ko: "모든 기한",
              })}
            </SelectItem>
            <SelectItem value="overdue">
              {t({
                en: "Overdue",
                "zh-CN": "已逾期",
                ja: "期限超過",
                ko: "기한 초과",
              })}
            </SelectItem>
            <SelectItem value="escalated">
              {t({
                en: "Escalated",
                "zh-CN": "已升级",
                ja: "エスカレーション済み",
                ko: "에스컬레이션됨",
              })}
            </SelectItem>
          </SelectContent>
        </Select>
        <Select value={provider} onValueChange={setProvider}>
          <SelectTrigger
            className="w-full sm:w-40"
            aria-label={t({
              en: "Filter by provider",
              "zh-CN": "筛选上游供应商",
              ja: "プロバイダーで絞り込む",
              ko: "공급자로 필터링",
            })}
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">
              {t({
                en: "All providers",
                "zh-CN": "全部供应商",
                ja: "すべてのプロバイダー",
                ko: "모든 공급자",
              })}
            </SelectItem>
            <SelectItem value="openai-codex">Codex</SelectItem>
            <SelectItem value="grok-cli">Grok</SelectItem>
            <SelectItem value="xai-grok">Grok API</SelectItem>
            <SelectItem value="dreamina-cli">
              {providerLabel(t, "dreamina-cli")}
            </SelectItem>
            <SelectItem value="volcengine-ark">
              {providerLabel(t, "volcengine-ark")}
            </SelectItem>
          </SelectContent>
        </Select>
      </div>

      {query.loading ? <AdminQuerySkeleton rows={7} /> : null}
      {!query.loading && query.error && !query.data ? (
        <AdminQueryError error={query.error} retry={query.retry} />
      ) : null}
      {query.data ? (
        <ObligationTable
          rows={query.data.data}
          onSelect={(row) => setSelectedReceiptId(row.receipt_id)}
        />
      ) : null}

      <ProviderCostObligationSheet
        receiptId={selectedReceiptId}
        onOpenChange={(open) => {
          if (!open) setSelectedReceiptId(null);
        }}
      />
    </section>
  );
}

function SummaryBand({ data }: { data: ProviderCostObligationList }) {
  const { t } = useI18n();
  const metrics = [
    [
      t({ en: "Open", "zh-CN": "待处理", ja: "未処理", ko: "처리 대기" }),
      data.summary.open,
    ],
    [
      t({
        en: "Overdue",
        "zh-CN": "已逾期",
        ja: "期限超過",
        ko: "기한 초과",
      }),
      data.summary.overdue,
    ],
    [
      t({
        en: "Escalated",
        "zh-CN": "已升级",
        ja: "エスカレーション済み",
        ko: "에스컬레이션됨",
      }),
      data.summary.escalated,
    ],
    [stateLabel(t, "settled"), data.summary.settled],
    [stateLabel(t, "waived"), data.summary.waived],
  ] as const;
  return (
    <div className="grid grid-cols-2 gap-4 rounded-md border bg-muted/20 p-4 sm:grid-cols-5">
      {metrics.map(([label, value]) => (
        <div key={label} className="min-w-0">
          <p className="text-xs text-muted-foreground">{label}</p>
          <p className="mt-1 text-2xl font-semibold tabular-nums">{value}</p>
        </div>
      ))}
    </div>
  );
}

function ObligationTable({
  rows,
  onSelect,
}: {
  rows: ProviderCostObligation[];
  onSelect: (row: ProviderCostObligation) => void;
}) {
  const { locale, t } = useI18n();
  if (rows.length === 0) {
    return (
      <div className="flex min-h-72 flex-col items-center justify-center rounded-md border text-center">
        <CheckCircle2 className="size-8 text-muted-foreground" aria-hidden="true" />
        <h3 className="mt-4 text-sm font-medium">
          {t({
            en: "No provider costs need attention for these filters",
            "zh-CN": "当前筛选下没有待处理成本",
            ja: "現在のフィルターで対応が必要なプロバイダーコストはありません",
            ko: "현재 필터에서 처리할 공급자 비용이 없습니다",
          })}
        </h3>
        <p className="mt-1 text-sm text-muted-foreground">
          {t({
            en: "Each new provider execution creates an independent cost-tracking record here.",
            "zh-CN": "新的上游执行会在这里形成独立成本追踪记录。",
            ja: "新しいプロバイダー実行ごとに、ここへ独立したコスト追跡記録が作成されます。",
            ko: "새 공급자 실행마다 여기에 독립적인 비용 추적 기록이 생성됩니다.",
          })}
        </p>
      </div>
    );
  }
  return (
    <div className="overflow-hidden rounded-md border">
      <Table className="min-w-[880px]">
        <TableHeader>
          <TableRow>
            <TableHead>
              {t({
                en: "Provider",
                "zh-CN": "供应商",
                ja: "プロバイダー",
                ko: "공급자",
              })}
            </TableHead>
            <TableHead>
              {t({
                en: "Organization",
                "zh-CN": "组织",
                ja: "組織",
                ko: "조직",
              })}
            </TableHead>
            <TableHead>
              {t({ en: "Status", "zh-CN": "状态", ja: "状態", ko: "상태" })}
            </TableHead>
            <TableHead>
              {t({
                en: "Cost authority",
                "zh-CN": "成本依据",
                ja: "コスト根拠",
                ko: "비용 근거",
              })}
            </TableHead>
            <TableHead>
              {t({
                en: "Deadline",
                "zh-CN": "处理时限",
                ja: "処理期限",
                ko: "처리 기한",
              })}
            </TableHead>
            <TableHead>
              {t({
                en: "Provider outcome",
                "zh-CN": "上游结果",
                ja: "プロバイダー結果",
                ko: "공급자 결과",
              })}
            </TableHead>
            <TableHead className="w-12">
              <span className="sr-only">
                {t({
                  en: "Details",
                  "zh-CN": "详情",
                  ja: "詳細",
                  ko: "상세",
                })}
              </span>
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => (
            <TableRow
              key={row.receipt_id}
              className="cursor-pointer"
              onClick={() => onSelect(row)}
            >
              <TableCell>
                <p className="font-medium">{providerLabel(t, row.provider_id)}</p>
                <p className="mt-0.5 max-w-44 truncate text-xs text-muted-foreground">
                  {row.provider_account_id ??
                    t({
                      en: "No account linked",
                      "zh-CN": "未绑定账户",
                      ja: "アカウント未連携",
                      ko: "연결된 계정 없음",
                    })}
                </p>
              </TableCell>
              <TableCell>
                <p className="max-w-48 truncate">{row.tenant_id}</p>
                <p className="mt-0.5 text-xs text-muted-foreground">
                  {t(
                    {
                      en: "Job {id}",
                      "zh-CN": "任务 {id}",
                      ja: "ジョブ {id}",
                      ko: "작업 {id}",
                    },
                    { id: shortId(row.job_id) },
                  )}
                </p>
              </TableCell>
              <TableCell>
                <ObligationStatus row={row} />
              </TableCell>
              <TableCell>
                <p>{authorityLabel(t, row.expected_authority_kind)}</p>
                <p className="mt-0.5 text-xs text-muted-foreground">
                  {reasonLabel(t, row.pending_reason_code, row.waiver_reason_code)}
                </p>
              </TableCell>
              <TableCell>
                <p>{formatDateTime(row.due_at_ms, locale)}</p>
                <p className="mt-0.5 text-xs text-muted-foreground">
                  {urgencyLabel(t, row.urgency)}
                </p>
              </TableCell>
              <TableCell>{outcomeLabel(t, row.receipt_outcome)}</TableCell>
              <TableCell>
                <ChevronRight className="size-4 text-muted-foreground" aria-hidden="true" />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function ObligationStatus({ row }: { row: ProviderCostObligation }) {
  const { t } = useI18n();
  if (row.urgency === "escalated") {
    return (
      <Badge variant="destructive">
        {t({
          en: "Escalated",
          "zh-CN": "已升级",
          ja: "エスカレーション済み",
          ko: "에스컬레이션됨",
        })}
      </Badge>
    );
  }
  if (row.urgency === "overdue") {
    return (
      <Badge variant="secondary">
        {t({
          en: "Overdue",
          "zh-CN": "已逾期",
          ja: "期限超過",
          ko: "기한 초과",
        })}
      </Badge>
    );
  }
  if (row.state === "settled") {
    return <Badge variant="outline">{stateLabel(t, row.state)}</Badge>;
  }
  if (row.state === "waived") {
    return <Badge variant="outline">{stateLabel(t, row.state)}</Badge>;
  }
  return (
    <Badge variant="secondary">
      {t({
        en: "In progress",
        "zh-CN": "处理中",
        ja: "処理中",
        ko: "처리 중",
      })}
    </Badge>
  );
}

function ProviderCostObligationSheet({
  receiptId,
  onOpenChange,
}: {
  receiptId: string | null;
  onOpenChange: (open: boolean) => void;
}) {
  const { t } = useI18n();
  const query = useAdminQuery<ProviderCostObligationDetail>(
    receiptId
      ? `/admin/v1/billing/provider-cost-obligations/${encodeURIComponent(receiptId)}`
      : "",
    Boolean(receiptId),
  );
  return (
    <Sheet open={Boolean(receiptId)} onOpenChange={onOpenChange}>
      <SheetContent className="flex h-full w-full min-w-0 flex-col gap-0 overflow-hidden p-0 sm:max-w-2xl">
        <SheetHeader className="shrink-0 border-b px-5 py-5 pr-12 text-left sm:px-6">
          <SheetTitle>
            {t({
              en: "Provider cost details",
              "zh-CN": "上游成本详情",
              ja: "プロバイダーコストの詳細",
              ko: "공급자 비용 상세",
            })}
          </SheetTitle>
          <SheetDescription>
            {query.data
              ? `${providerLabel(t, query.data.provider_id)} · Receipt ${shortId(
                  query.data.receipt_id,
                )}`
              : t({
                  en: "Loading the cost-tracking record",
                  "zh-CN": "正在读取成本追踪记录",
                  ja: "コスト追跡記録を読み込み中",
                  ko: "비용 추적 기록 불러오는 중",
                })}
          </SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1 overflow-y-auto p-5 sm:p-6">
          {query.loading ? <AdminQuerySkeleton rows={7} /> : null}
          {!query.loading && query.error && !query.data ? (
            <AdminQueryError error={query.error} retry={query.retry} />
          ) : null}
          {query.data ? <ObligationDetail detail={query.data} /> : null}
        </div>
      </SheetContent>
    </Sheet>
  );
}

function ObligationDetail({ detail }: { detail: ProviderCostObligationDetail }) {
  const { locale, t } = useI18n();
  const facts = [
    [
      t({ en: "Status", "zh-CN": "状态", ja: "状態", ko: "상태" }),
      stateLabel(t, detail.state),
    ],
    [
      t({
        en: "Provider outcome",
        "zh-CN": "上游结果",
        ja: "プロバイダー結果",
        ko: "공급자 결과",
      }),
      outcomeLabel(t, detail.receipt_outcome),
    ],
    [
      t({
        en: "Expected cost authority",
        "zh-CN": "期望成本依据",
        ja: "想定コスト根拠",
        ko: "예상 비용 근거",
      }),
      authorityLabel(t, detail.expected_authority_kind),
    ],
    [
      t({ en: "Currency", "zh-CN": "币种", ja: "通貨", ko: "통화" }),
      detail.currency ??
        t({
          en: "Pending confirmation",
          "zh-CN": "等待确认",
          ja: "確認待ち",
          ko: "확인 대기",
        }),
    ],
    [
      t({ en: "Due at", "zh-CN": "到期时间", ja: "期限", ko: "기한" }),
      formatDateTime(detail.due_at_ms, locale),
    ],
    [
      t({
        en: "Escalates at",
        "zh-CN": "升级时间",
        ja: "エスカレーション日時",
        ko: "에스컬레이션 시간",
      }),
      formatDateTime(detail.escalate_at_ms, locale),
    ],
    [
      t({
        en: "Organization",
        "zh-CN": "组织",
        ja: "組織",
        ko: "조직",
      }),
      detail.tenant_id,
    ],
    [
      t({
        en: "Provider account",
        "zh-CN": "Provider 账户",
        ja: "プロバイダーアカウント",
        ko: "공급자 계정",
      }),
      detail.provider_account_id ??
        t({
          en: "Not linked",
          "zh-CN": "未绑定",
          ja: "未連携",
          ko: "연결되지 않음",
        }),
    ],
  ] as const;
  return (
    <div className="space-y-6">
      <div className="flex items-start gap-3 rounded-md bg-muted/50 px-4 py-3 text-sm">
        {detail.urgency === "escalated" ? (
          <AlertTriangle className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
        ) : (
          <Clock3 className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
        )}
        <p className="text-muted-foreground">
          {t({
            en: "Provider actual-cost facts or allocation facts remain the sole authority for amounts. This record only tracks whether a conclusion has been reached.",
            "zh-CN": "金额仍以 Provider 实际成本事实或分摊事实为唯一权威；本记录只追踪是否已经取得结论。",
            ja: "金額の唯一の根拠は、プロバイダーの実コスト事実または配賦事実です。この記録は結論が得られたかどうかのみを追跡します。",
            ko: "금액의 유일한 근거는 공급자 실제 비용 사실 또는 배분 사실입니다. 이 기록은 결론 도출 여부만 추적합니다.",
          })}
        </p>
      </div>
      <dl className="grid gap-x-8 gap-y-4 sm:grid-cols-2">
        {facts.map(([label, value]) => (
          <div key={label} className="min-w-0">
            <dt className="text-xs text-muted-foreground">{label}</dt>
            <dd className="mt-1 break-all text-sm font-medium">{value}</dd>
          </div>
        ))}
      </dl>
      <div>
        <h3 className="text-sm font-semibold">
          {t({
            en: "Event history",
            "zh-CN": "事件记录",
            ja: "イベント履歴",
            ko: "이벤트 기록",
          })}
        </h3>
        <div className="mt-3 space-y-3">
          {detail.events.map((event) => (
            <div key={event.event_id} className="rounded-md border px-4 py-3">
              <div className="flex items-center justify-between gap-3">
                <p className="text-sm font-medium">
                  {eventLabel(t, event.event_kind)}
                </p>
                <span className="text-xs text-muted-foreground">
                  {formatDateTime(event.created_at_ms, locale)}
                </span>
              </div>
              <p className="mt-1 text-xs text-muted-foreground">
                {event.previous_state
                  ? `${stateLabel(t, event.previous_state)} → ${stateLabel(t, event.state)}`
                  : stateLabel(t, event.state)}
                {" · "}
                {t(
                  {
                    en: "Version {version}",
                    "zh-CN": "版本 {version}",
                    ja: "バージョン {version}",
                    ko: "버전 {version}",
                  },
                  { version: event.control_version },
                )}
              </p>
            </div>
          ))}
        </div>
      </div>
      <div className="rounded-md border px-4 py-3">
        <p className="text-xs text-muted-foreground">
          {t({
            en: "Receipt ID",
            "zh-CN": "凭证 ID",
            ja: "受領 ID",
            ko: "영수증 ID",
          })}
        </p>
        <p className="mt-1 break-all font-mono text-xs">{detail.receipt_id}</p>
      </div>
    </div>
  );
}

function providerLabel(t: Translate, providerId: string) {
  const labels: Record<
    string,
    string | { en: string; "zh-CN": string; ja: string; ko: string }
  > = {
    "openai-codex": "Codex",
    "grok-cli": "Grok",
    "xai-grok": "Grok API",
    xai: "Grok API",
    "dreamina-cli": {
      en: "Dreamina",
      "zh-CN": "即梦",
      ja: "Dreamina",
      ko: "Dreamina",
    },
    dreamina: {
      en: "Dreamina",
      "zh-CN": "即梦",
      ja: "Dreamina",
      ko: "Dreamina",
    },
    "volcengine-ark": {
      en: "Volcengine Ark",
      "zh-CN": "火山方舟",
      ja: "Volcengine Ark",
      ko: "Volcengine Ark",
    },
  };
  const label = labels[providerId];
  return typeof label === "string" ? label : label ? t(label) : providerId;
}

function authorityLabel(t: Translate, value: string | null) {
  if (value === "provider_actual") {
    return t({
      en: "Provider actual cost",
      "zh-CN": "Provider 实际成本",
      ja: "プロバイダー実コスト",
      ko: "공급자 실제 비용",
    });
  }
  if (value === "provider_allocated") {
    return t({
      en: "Contract or subscription allocation",
      "zh-CN": "合同或订阅分摊",
      ja: "契約またはサブスクリプション配賦",
      ko: "계약 또는 구독 배분",
    });
  }
  return t({
    en: "Not classified",
    "zh-CN": "尚未分类",
    ja: "未分類",
    ko: "분류되지 않음",
  });
}

function reasonLabel(
  t: Translate,
  pending: string | null,
  waived: string | null,
) {
  const value = pending ?? waived;
  const labels: Record<
    string,
    { en: string; "zh-CN": string; ja: string; ko: string }
  > = {
    policy_unresolved: {
      en: "Waiting for cost policy",
      "zh-CN": "等待成本策略",
      ja: "コストポリシー待ち",
      ko: "비용 정책 대기",
    },
    provider_outcome_uncertain: {
      en: "Provider outcome is uncertain",
      "zh-CN": "上游结果不确定",
      ja: "プロバイダー結果が不確定",
      ko: "공급자 결과가 불확실함",
    },
    legacy_unbound_account: {
      en: "Legacy record has no linked account",
      "zh-CN": "历史记录未绑定账户",
      ja: "従来の記録にアカウントが連携されていません",
      ko: "기존 기록에 연결된 계정이 없음",
    },
    authority_pending: {
      en: "Waiting for authoritative cost",
      "zh-CN": "等待权威成本",
      ja: "確定コスト待ち",
      ko: "권위 있는 비용 대기",
    },
    confirmed_no_effect: {
      en: "Confirmed no effect",
      "zh-CN": "已确认未产生效果",
      ja: "効果なしを確認済み",
      ko: "효과 없음 확인됨",
    },
    contractual_no_direct_cost: {
      en: "Contract specifies no direct cost",
      "zh-CN": "合同约定无直接成本",
      ja: "契約上、直接コストなし",
      ko: "계약상 직접 비용 없음",
    },
    provider_invoice_no_charge: {
      en: "Provider invoice confirms no charge",
      "zh-CN": "Provider 账单确认未收费",
      ja: "プロバイダー請求書で課金なしを確認",
      ko: "공급자 청구서에서 청구 없음 확인",
    },
    legal_adjustment: {
      en: "Compliance adjustment",
      "zh-CN": "合规调整",
      ja: "コンプライアンス調整",
      ko: "컴플라이언스 조정",
    },
  };
  return value
    ? labels[value]
      ? t(labels[value])
      : value
    : t({
        en: "Evidence complete",
        "zh-CN": "证据完整",
        ja: "証拠完備",
        ko: "증거 완전",
      });
}

function urgencyLabel(
  t: Translate,
  value: ProviderCostObligation["urgency"],
) {
  const labels = {
    within_sla: {
      en: "Within the processing deadline",
      "zh-CN": "仍在处理时限内",
      ja: "処理期限内",
      ko: "처리 기한 내",
    },
    overdue: {
      en: "Processing deadline exceeded",
      "zh-CN": "已超过处理时限",
      ja: "処理期限を超過",
      ko: "처리 기한 초과",
    },
    escalated: {
      en: "Escalation deadline exceeded",
      "zh-CN": "已超过升级时限",
      ja: "エスカレーション期限を超過",
      ko: "에스컬레이션 기한 초과",
    },
    resolved: {
      en: "Final conclusion reached",
      "zh-CN": "已形成最终结论",
      ja: "最終結論を確定",
      ko: "최종 결론 도출됨",
    },
  };
  return t(labels[value]);
}

function outcomeLabel(t: Translate, value: string) {
  const labels: Record<
    string,
    { en: string; "zh-CN": string; ja: string; ko: string }
  > = {
    succeeded: { en: "Succeeded", "zh-CN": "成功", ja: "成功", ko: "성공" },
    failed: { en: "Failed", "zh-CN": "失败", ja: "失敗", ko: "실패" },
    no_effect: {
      en: "No effect",
      "zh-CN": "未产生效果",
      ja: "効果なし",
      ko: "효과 없음",
    },
    uncertain: {
      en: "Uncertain",
      "zh-CN": "不确定",
      ja: "不確定",
      ko: "불확실",
    },
  };
  return labels[value] ? t(labels[value]) : value;
}

function stateLabel(t: Translate, value: string) {
  const labels: Record<
    string,
    { en: string; "zh-CN": string; ja: string; ko: string }
  > = {
    expected: {
      en: "Awaiting authoritative cost",
      "zh-CN": "等待权威成本",
      ja: "確定コスト待ち",
      ko: "권위 있는 비용 대기",
    },
    pending: {
      en: "Awaiting classification or review",
      "zh-CN": "待分类或复核",
      ja: "分類またはレビュー待ち",
      ko: "분류 또는 검토 대기",
    },
    settled: {
      en: "Settled",
      "zh-CN": "已结算",
      ja: "確定済み",
      ko: "정산됨",
    },
    waived: {
      en: "Waived",
      "zh-CN": "已豁免",
      ja: "免除済み",
      ko: "면제됨",
    },
  };
  return labels[value] ? t(labels[value]) : value;
}

function eventLabel(t: Translate, value: string) {
  const labels: Record<
    string,
    { en: string; "zh-CN": string; ja: string; ko: string }
  > = {
    created: {
      en: "Cost tracking created",
      "zh-CN": "建立成本追踪",
      ja: "コスト追跡を作成",
      ko: "비용 추적 생성",
    },
    classified: {
      en: "Cost authority confirmed",
      "zh-CN": "确认成本依据",
      ja: "コスト根拠を確認",
      ko: "비용 근거 확인",
    },
    reviewed: {
      en: "Review completed",
      "zh-CN": "完成复核",
      ja: "レビュー完了",
      ko: "검토 완료",
    },
    settled: {
      en: "Authoritative cost obtained",
      "zh-CN": "取得权威成本",
      ja: "確定コストを取得",
      ko: "권위 있는 비용 확보",
    },
    waived: {
      en: "Waived based on evidence",
      "zh-CN": "依据证据豁免",
      ja: "証拠に基づき免除",
      ko: "증거에 따라 면제",
    },
  };
  return labels[value] ? t(labels[value]) : value;
}

function shortId(value: string) {
  return value.slice(0, 8);
}
