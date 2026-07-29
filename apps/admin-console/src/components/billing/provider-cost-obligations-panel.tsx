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
import { formatDateTime } from "@/lib/admin/format";
import type {
  ProviderCostObligation,
  ProviderCostObligationDetail,
  ProviderCostObligationList,
} from "@/lib/admin/types";

type StateFilter = "open" | "all" | "pending" | "expected" | "settled" | "waived";
type UrgencyFilter = "all" | "overdue" | "escalated";

export function ProviderCostObligationsPanel({ enabled }: { enabled: boolean }) {
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
          <TabsTrigger value="obligations">成本义务</TabsTrigger>
          <TabsTrigger value="allocations">分摊草稿</TabsTrigger>
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
          <h2 className="text-base font-semibold">上游成本</h2>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            跟踪每次上游执行是否已取得唯一成本结论。逾期只会进入复核队列，不会自动视为免费。
          </p>
        </div>
        <Button
          type="button"
          variant="outline"
          size="icon"
          aria-label="刷新上游成本"
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
          <SelectTrigger className="w-full sm:w-40" aria-label="筛选处理状态">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="open">需要处理</SelectItem>
            <SelectItem value="all">全部记录</SelectItem>
            <SelectItem value="expected">等待权威成本</SelectItem>
            <SelectItem value="pending">待分类或复核</SelectItem>
            <SelectItem value="settled">已结算</SelectItem>
            <SelectItem value="waived">已豁免</SelectItem>
          </SelectContent>
        </Select>
        <Select
          value={urgency}
          onValueChange={(value) => setUrgency(value as UrgencyFilter)}
        >
          <SelectTrigger className="w-full sm:w-36" aria-label="筛选处理时限">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部时限</SelectItem>
            <SelectItem value="overdue">已逾期</SelectItem>
            <SelectItem value="escalated">已升级</SelectItem>
          </SelectContent>
        </Select>
        <Select value={provider} onValueChange={setProvider}>
          <SelectTrigger className="w-full sm:w-40" aria-label="筛选上游供应商">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部供应商</SelectItem>
            <SelectItem value="openai-codex">Codex</SelectItem>
            <SelectItem value="grok-cli">Grok</SelectItem>
            <SelectItem value="xai-grok">Grok API</SelectItem>
            <SelectItem value="dreamina-cli">即梦</SelectItem>
            <SelectItem value="volcengine-ark">火山方舟</SelectItem>
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
  const metrics = [
    ["待处理", data.summary.open],
    ["已逾期", data.summary.overdue],
    ["已升级", data.summary.escalated],
    ["已结算", data.summary.settled],
    ["已豁免", data.summary.waived],
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
  if (rows.length === 0) {
    return (
      <div className="flex min-h-72 flex-col items-center justify-center rounded-md border text-center">
        <CheckCircle2 className="size-8 text-muted-foreground" aria-hidden="true" />
        <h3 className="mt-4 text-sm font-medium">当前筛选下没有待处理成本</h3>
        <p className="mt-1 text-sm text-muted-foreground">
          新的上游执行会在这里形成独立成本追踪记录。
        </p>
      </div>
    );
  }
  return (
    <div className="overflow-hidden rounded-md border">
      <Table className="min-w-[880px]">
        <TableHeader>
          <TableRow>
            <TableHead>供应商</TableHead>
            <TableHead>组织</TableHead>
            <TableHead>状态</TableHead>
            <TableHead>成本依据</TableHead>
            <TableHead>处理时限</TableHead>
            <TableHead>上游结果</TableHead>
            <TableHead className="w-12">
              <span className="sr-only">详情</span>
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
                <p className="font-medium">{providerLabel(row.provider_id)}</p>
                <p className="mt-0.5 max-w-44 truncate text-xs text-muted-foreground">
                  {row.provider_account_id ?? "未绑定账户"}
                </p>
              </TableCell>
              <TableCell>
                <p className="max-w-48 truncate">{row.tenant_id}</p>
                <p className="mt-0.5 text-xs text-muted-foreground">
                  Job {shortId(row.job_id)}
                </p>
              </TableCell>
              <TableCell>
                <ObligationStatus row={row} />
              </TableCell>
              <TableCell>
                <p>{authorityLabel(row.expected_authority_kind)}</p>
                <p className="mt-0.5 text-xs text-muted-foreground">
                  {reasonLabel(row.pending_reason_code, row.waiver_reason_code)}
                </p>
              </TableCell>
              <TableCell>
                <p>{formatDateTime(row.due_at_ms)}</p>
                <p className="mt-0.5 text-xs text-muted-foreground">
                  {urgencyLabel(row.urgency)}
                </p>
              </TableCell>
              <TableCell>{outcomeLabel(row.receipt_outcome)}</TableCell>
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
  if (row.urgency === "escalated") {
    return <Badge variant="destructive">已升级</Badge>;
  }
  if (row.urgency === "overdue") {
    return <Badge variant="secondary">已逾期</Badge>;
  }
  if (row.state === "settled") {
    return <Badge variant="outline">已结算</Badge>;
  }
  if (row.state === "waived") {
    return <Badge variant="outline">已豁免</Badge>;
  }
  return <Badge variant="secondary">处理中</Badge>;
}

function ProviderCostObligationSheet({
  receiptId,
  onOpenChange,
}: {
  receiptId: string | null;
  onOpenChange: (open: boolean) => void;
}) {
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
          <SheetTitle>上游成本详情</SheetTitle>
          <SheetDescription>
            {query.data
              ? `${providerLabel(query.data.provider_id)} · Receipt ${shortId(
                  query.data.receipt_id,
                )}`
              : "正在读取成本追踪记录"}
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
  const facts = [
    ["状态", stateLabel(detail.state)],
    ["上游结果", outcomeLabel(detail.receipt_outcome)],
    ["期望成本依据", authorityLabel(detail.expected_authority_kind)],
    ["币种", detail.currency ?? "等待确认"],
    ["到期时间", formatDateTime(detail.due_at_ms)],
    ["升级时间", formatDateTime(detail.escalate_at_ms)],
    ["组织", detail.tenant_id],
    ["Provider 账户", detail.provider_account_id ?? "未绑定"],
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
          金额仍以 Provider 实际成本事实或分摊事实为唯一权威；本记录只追踪是否已经取得结论。
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
        <h3 className="text-sm font-semibold">事件记录</h3>
        <div className="mt-3 space-y-3">
          {detail.events.map((event) => (
            <div key={event.event_id} className="rounded-md border px-4 py-3">
              <div className="flex items-center justify-between gap-3">
                <p className="text-sm font-medium">{eventLabel(event.event_kind)}</p>
                <span className="text-xs text-muted-foreground">
                  {formatDateTime(event.created_at_ms)}
                </span>
              </div>
              <p className="mt-1 text-xs text-muted-foreground">
                {event.previous_state
                  ? `${stateLabel(event.previous_state)} → ${stateLabel(event.state)}`
                  : stateLabel(event.state)}
                {" · "}版本 {event.control_version}
              </p>
            </div>
          ))}
        </div>
      </div>
      <div className="rounded-md border px-4 py-3">
        <p className="text-xs text-muted-foreground">Receipt ID</p>
        <p className="mt-1 break-all font-mono text-xs">{detail.receipt_id}</p>
      </div>
    </div>
  );
}

function providerLabel(providerId: string) {
  const labels: Record<string, string> = {
    "openai-codex": "Codex",
    "grok-cli": "Grok",
    "xai-grok": "Grok API",
    xai: "Grok API",
    "dreamina-cli": "即梦",
    dreamina: "即梦",
    "volcengine-ark": "火山方舟",
  };
  return labels[providerId] ?? providerId;
}

function authorityLabel(value: string | null) {
  if (value === "provider_actual") return "Provider 实际成本";
  if (value === "provider_allocated") return "合同或订阅分摊";
  return "尚未分类";
}

function reasonLabel(pending: string | null, waived: string | null) {
  const value = pending ?? waived;
  const labels: Record<string, string> = {
    policy_unresolved: "等待成本策略",
    provider_outcome_uncertain: "上游结果不确定",
    legacy_unbound_account: "历史记录未绑定账户",
    authority_pending: "等待权威成本",
    confirmed_no_effect: "已确认未产生效果",
    contractual_no_direct_cost: "合同约定无直接成本",
    provider_invoice_no_charge: "Provider 账单确认未收费",
    legal_adjustment: "合规调整",
  };
  return value ? labels[value] ?? value : "证据完整";
}

function urgencyLabel(value: ProviderCostObligation["urgency"]) {
  const labels = {
    within_sla: "仍在处理时限内",
    overdue: "已超过处理时限",
    escalated: "已超过升级时限",
    resolved: "已形成最终结论",
  };
  return labels[value];
}

function outcomeLabel(value: string) {
  const labels: Record<string, string> = {
    succeeded: "成功",
    failed: "失败",
    no_effect: "未产生效果",
    uncertain: "不确定",
  };
  return labels[value] ?? value;
}

function stateLabel(value: string) {
  const labels: Record<string, string> = {
    expected: "等待权威成本",
    pending: "待分类或复核",
    settled: "已结算",
    waived: "已豁免",
  };
  return labels[value] ?? value;
}

function eventLabel(value: string) {
  const labels: Record<string, string> = {
    created: "建立成本追踪",
    classified: "确认成本依据",
    reviewed: "完成复核",
    settled: "取得权威成本",
    waived: "依据证据豁免",
  };
  return labels[value] ?? value;
}

function shortId(value: string) {
  return value.slice(0, 8);
}
