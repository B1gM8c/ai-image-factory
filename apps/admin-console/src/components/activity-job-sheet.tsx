"use client";

import { Copy, ListTree, ReceiptText, TriangleAlert } from "lucide-react";
import { toast } from "sonner";
import { ActivityStatusBadge } from "@/components/activity-status-badge";
import {
  AdminQueryError,
  AdminQuerySkeleton,
} from "@/components/admin-query-state";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useAdminQuery } from "@/hooks/use-admin-query";
import {
  formatDateTime,
  formatDurationMs,
  formatInteger,
  formatMoneyMicros,
  formatOperation,
  formatStatus,
  operationEndpoint,
} from "@/lib/admin/format";
import type {
  ConsoleJobEconomicsSnapshot,
  JobEconomicsSnapshot,
  JobListItem,
  JobProviderCost,
} from "@/lib/admin/types";

export type EconomicsSnapshot =
  | ConsoleJobEconomicsSnapshot
  | JobEconomicsSnapshot;

export function ActivityJobSheet({
  item,
  economicsPath,
  onOpenChange,
}: {
  item: JobListItem | null;
  economicsPath: string | null;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <Sheet open={item !== null} onOpenChange={onOpenChange}>
      <SheetContent className="flex w-full min-w-0 flex-col gap-0 overflow-hidden p-0 sm:max-w-2xl lg:max-w-3xl">
        {item ? (
          <JobDetails item={item} economicsPath={economicsPath} />
        ) : null}
      </SheetContent>
    </Sheet>
  );
}

function JobDetails({
  item,
  economicsPath,
}: {
  item: JobListItem;
  economicsPath: string | null;
}) {
  const duration = jobDuration(item);
  const economics = useAdminQuery<EconomicsSnapshot>(
    economicsPath ?? "",
    Boolean(economicsPath),
  );

  return (
    <>
      <SheetHeader className="border-b px-5 py-5 pr-12 text-left sm:px-6">
        <div className="flex flex-wrap items-center gap-2">
          <ActivityStatusBadge state={item.job_state} />
          <Badge variant="outline">{formatOperation(item.operation)}</Badge>
        </div>
        <SheetTitle className="pt-1 text-xl">请求详情</SheetTitle>
        <SheetDescription className="break-all font-mono text-xs">
          {item.request_id}
        </SheetDescription>
      </SheetHeader>

      <Tabs defaultValue="overview" className="flex min-h-0 flex-1 flex-col">
        <div className="shrink-0 overflow-x-auto border-b px-5 sm:px-6">
          <TabsList variant="line">
            <TabsTrigger value="overview" variant="line">
              <ListTree className="size-4" aria-hidden="true" />
              概览
            </TabsTrigger>
            <TabsTrigger value="economics" variant="line">
              <ReceiptText className="size-4" aria-hidden="true" />
              计费
            </TabsTrigger>
          </TabsList>
        </div>

        <TabsContent
          value="overview"
          className="m-0 min-h-0 flex-1 overflow-y-auto px-5 py-6 sm:px-6"
        >
          {item.last_error_code ? (
            <div className="mb-7 flex gap-3 border border-destructive/30 bg-destructive/5 p-4 text-sm">
              <TriangleAlert
                className="mt-0.5 size-4 shrink-0 text-destructive"
                aria-hidden="true"
              />
              <div className="min-w-0">
                <p className="font-medium text-destructive">请求执行异常</p>
                <p className="mt-1 break-all font-mono text-xs text-muted-foreground">
                  {item.last_error_code}
                </p>
              </div>
            </div>
          ) : null}

          <div className="space-y-8">
            <DetailSection title="概览">
              <DetailGrid>
                <DetailItem label="请求状态">
                  <ActivityStatusBadge state={item.job_state} />
                </DetailItem>
                <DetailItem label="耗时">
                  {formatDurationMs(duration)}
                </DetailItem>
                <DetailItem label="Provider">{item.provider_id}</DetailItem>
                <DetailItem label="模型">
                  <span className="break-all font-mono text-xs">
                    {item.model}
                  </span>
                </DetailItem>
                <DetailItem label="创建时间">
                  {formatDateTime(item.created_at_ms)}
                </DetailItem>
                <DetailItem label="完成时间">
                  {formatDateTime(item.finished_at_ms)}
                </DetailItem>
              </DetailGrid>
            </DetailSection>

            <DetailSection title="请求">
              <DetailRows>
                <CopyRow label="Request ID" value={item.request_id} />
                <CopyRow label="Job ID" value={item.job_id} />
                <DetailRow label="API 端点">
                  <span className="font-mono text-xs">
                    {operationEndpoint(item.operation)}
                  </span>
                </DetailRow>
                <DetailRow label="操作">
                  {formatOperation(item.operation)}
                </DetailRow>
              </DetailRows>
            </DetailSection>

            <DetailSection title="调用归属">
              <DetailRows>
                <DetailRow label="Project ID">
                  {item.project_id ?? "历史记录未关联"}
                </DetailRow>
                <DetailRow label="API Key ID">
                  {item.api_key_id ?? item.auth_kind ?? "未记录"}
                </DetailRow>
                <DetailRow label="Service Account">
                  {item.service_account_id ?? "未记录"}
                </DetailRow>
                <DetailRow label="Tenant">{item.tenant_id}</DetailRow>
              </DetailRows>
            </DetailSection>

            <DetailSection title="执行过程">
              <DetailRows>
                <DetailRow label="任务">
                  {formatStatus(item.job_state)}
                </DetailRow>
                <DetailRow label="队列">
                  {item.work_state
                    ? formatStatus(item.work_state)
                    : "未创建工作项"}
                </DetailRow>
                <DetailRow label="Provider">
                  {item.provider_states.length > 0 ? (
                    <div className="flex flex-wrap justify-end gap-1.5">
                      {item.provider_states.map((state) => (
                        <Badge
                          key={`${state.stage}-${state.state}`}
                          variant="outline"
                          className="font-normal"
                        >
                          {formatStatus(state.stage)} ·{" "}
                          {formatStatus(state.state)}{" "}
                          {formatInteger(state.count)}
                        </Badge>
                      ))}
                    </div>
                  ) : (
                    "尚无上游执行记录"
                  )}
                </DetailRow>
                <DetailRow label="开始时间">
                  {formatDateTime(item.started_at_ms)}
                </DetailRow>
              </DetailRows>
            </DetailSection>
          </div>
        </TabsContent>

        <TabsContent
          value="economics"
          className="m-0 min-h-0 flex-1 overflow-y-auto px-5 py-6 sm:px-6"
        >
          {economics.loading ? <AdminQuerySkeleton rows={7} /> : null}
          {!economics.loading && economics.error && !economics.data ? (
            <AdminQueryError error={economics.error} retry={economics.retry} />
          ) : null}
          {economics.data ? (
            <EconomicsDetails
              snapshot={economics.data}
              stale={Boolean(economics.error)}
            />
          ) : null}
        </TabsContent>
      </Tabs>
    </>
  );
}

export function EconomicsDetails({
  snapshot,
  stale,
}: {
  snapshot: EconomicsSnapshot;
  stale: boolean;
}) {
  const quote = snapshot.customer_quote;
  const rating = snapshot.customer_rating;
  const hold = snapshot.customer_hold;
  const providerCosts =
    "provider_costs" in snapshot ? snapshot.provider_costs : null;

  return (
    <div className="space-y-8">
      {stale ? (
        <p className="text-sm text-muted-foreground">
          明细刷新失败，当前显示上一次成功结果。
        </p>
      ) : null}

      <div className="grid gap-5 bg-muted/40 p-4 sm:grid-cols-3">
        <SummaryMetric
          label="报价上限"
          value={
            quote
              ? formatMoneyMicros(quote.max_total_micros, quote.currency)
              : "--"
          }
        />
        <SummaryMetric
          label="实际扣费"
          value={
            rating
              ? formatMoneyMicros(
                  rating.total_amount_micros,
                  rating.currency,
                )
              : "--"
          }
        />
        <SummaryMetric
          label="计费状态"
          value={economicsStateLabel(snapshot.economics_state)}
        />
      </div>

      {snapshot.economics_contract_version !== 4 ? (
        <div className="border border-dashed px-4 py-3 text-sm text-muted-foreground">
          此请求使用 v{snapshot.economics_contract_version} 旧计价契约，因此没有
          v4 报价与逐项计费证据。
        </div>
      ) : null}

      {quote ? (
        <DetailSection title="报价与结算">
          <DetailRows>
            <CopyRow label="Quote ID" value={quote.quote_id} />
            <CopyRow
              label="价格版本"
              value={quote.price_book_version_id}
            />
            <DetailRow label="公开模型">
              <span className="font-mono text-xs">
                {quote.public_model_id}
              </span>
            </DetailRow>
            <DetailRow label="服务等级">{quote.service_tier}</DetailRow>
            <DetailRow label="报价时间">
              {formatDateTime(quote.created_at_ms)}
            </DetailRow>
            {hold ? (
              <>
                <DetailRow label="资金状态">
                  {formatStatus(hold.state)}
                </DetailRow>
                <DetailRow label="冻结 / 扣取">
                  {formatMoneyMicros(hold.held_micros, hold.currency)} /{" "}
                  {formatMoneyMicros(hold.captured_micros, hold.currency)}
                </DetailRow>
              </>
            ) : null}
          </DetailRows>
        </DetailSection>
      ) : null}

      {quote?.lines.length ? (
        <DetailSection title="计价明细">
          <div className="overflow-x-auto border">
            <Table className="min-w-[680px]">
              <TableHeader>
                <TableRow>
                  <TableHead>计价项</TableHead>
                  <TableHead>计量</TableHead>
                  <TableHead>单价</TableHead>
                  <TableHead className="text-right">报价上限</TableHead>
                  <TableHead className="text-right">实际费用</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {quote.lines.map((line) => (
                  <TableRow
                    key={`${line.partition_key}-${line.terminal_outcome}-${line.component_key}`}
                  >
                    <TableCell>
                      <p className="font-medium">
                        {formatStatus(line.component_key)}
                      </p>
                      <p className="mt-1 text-xs text-muted-foreground">
                        {formatStatus(line.terminal_outcome)}
                      </p>
                    </TableCell>
                    <TableCell className="tabular-nums">
                      {line.actual_quantity !== null
                        ? formatInteger(line.actual_quantity)
                        : "--"}{" "}
                      {line.unit}
                    </TableCell>
                    <TableCell className="tabular-nums">
                      {formatMoneyMicros(
                        line.unit_price_micros,
                        quote.currency,
                      )}{" "}
                      / {formatInteger(line.unit_size)} {line.unit}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {formatMoneyMicros(
                        line.max_amount_micros,
                        quote.currency,
                      )}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {line.actual_amount_micros !== null
                        ? formatMoneyMicros(
                            line.actual_amount_micros,
                            quote.currency,
                          )
                        : "--"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </DetailSection>
      ) : null}

      <DetailSection title="实际计量">
        {snapshot.usage_facts.length ? (
          <div className="overflow-x-auto border">
            <Table className="min-w-[560px]">
              <TableHeader>
                <TableRow>
                  <TableHead>指标</TableHead>
                  <TableHead>数量</TableHead>
                  <TableHead>来源</TableHead>
                  <TableHead>可信度</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {snapshot.usage_facts.map((fact, index) => (
                  <TableRow
                    key={`${fact.metric}-${fact.billing_partition_key}-${fact.created_at_ms}-${index}`}
                  >
                    <TableCell>
                      <p className="font-medium">
                        {formatStatus(fact.metric)}
                      </p>
                      <p className="mt-1 text-xs text-muted-foreground">
                        {formatStatus(fact.terminal_outcome)}
                      </p>
                    </TableCell>
                    <TableCell className="tabular-nums">
                      {formatInteger(fact.quantity)} {fact.unit}
                    </TableCell>
                    <TableCell>{formatStatus(fact.quantity_source)}</TableCell>
                    <TableCell>{formatStatus(fact.confidence)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        ) : (
          <EmptyEvidence>尚未产生实际计量事实。</EmptyEvidence>
        )}
      </DetailSection>

      <DetailSection title="客户账本">
        {snapshot.ledger_transactions.length ? (
          <div className="space-y-3">
            {snapshot.ledger_transactions.map((transaction) => (
              <div
                key={transaction.transaction_id}
                className="flex min-w-0 items-start justify-between gap-4 text-sm"
              >
                <div className="min-w-0">
                  <p className="font-medium">
                    {formatStatus(transaction.transaction_type)}
                  </p>
                  <p className="mt-1 truncate font-mono text-xs text-muted-foreground">
                    {transaction.transaction_id}
                  </p>
                </div>
                <div className="shrink-0 text-right">
                  <p className="font-medium tabular-nums">
                    {formatMoneyMicros(
                      transaction.amount_micros,
                      transaction.currency,
                    )}
                  </p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    {transaction.sealed_at_ms
                      ? `封账 ${formatDateTime(transaction.sealed_at_ms)}`
                      : "尚未封账"}
                  </p>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <EmptyEvidence>
            {rating?.total_amount_micros === "0"
              ? "零金额请求无需创建账本交易。"
              : "尚未生成客户账本交易。"}
          </EmptyEvidence>
        )}
      </DetailSection>

      {providerCosts ? (
        <ProviderCosts costs={providerCosts} />
      ) : null}
    </div>
  );
}

function ProviderCosts({ costs }: { costs: JobProviderCost[] }) {
  return (
    <DetailSection title="Provider 成本（管理员）">
      {costs.length ? (
        <div className="space-y-4">
          {costs.map((cost) => (
            <div
              key={`${cost.cost_basis}-${cost.cost_id}`}
              className="grid gap-2 bg-muted/30 p-4 text-sm sm:grid-cols-[minmax(0,1fr)_auto]"
            >
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                  <p className="font-medium">
                    {providerCostBasisLabel(cost.cost_basis)}
                  </p>
                  <Badge variant="outline" className="font-normal">
                    {cost.attribution_state === "shared"
                      ? "共享观测"
                      : "已归因"}
                  </Badge>
                </div>
                <p className="mt-1 text-xs text-muted-foreground">
                  {formatStatus(cost.authority)} ·{" "}
                  {formatStatus(cost.confidence)}
                </p>
              </div>
              <div className="text-left sm:text-right">
                <p className="font-medium tabular-nums">
                  {cost.attributed_amount_micros !== null
                    ? formatMoneyMicros(
                        cost.attributed_amount_micros,
                        cost.currency,
                      )
                    : "未归因到单次请求"}
                </p>
                {cost.attribution_state === "shared" ? (
                  <p className="mt-1 text-xs text-muted-foreground">
                    观测总额{" "}
                    {formatMoneyMicros(
                      cost.observed_amount_micros,
                      cost.currency,
                    )}
                  </p>
                ) : null}
              </div>
            </div>
          ))}
        </div>
      ) : (
        <EmptyEvidence>尚无可验证的 Provider 成本证据。</EmptyEvidence>
      )}
    </DetailSection>
  );
}

function SummaryMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="mt-1 truncate text-sm font-semibold tabular-nums">
        {value}
      </p>
    </div>
  );
}

function EmptyEvidence({ children }: { children: React.ReactNode }) {
  return (
    <div className="border border-dashed px-4 py-5 text-sm text-muted-foreground">
      {children}
    </div>
  );
}

function DetailSection({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section>
      <h3 className="mb-3 text-sm font-semibold">{title}</h3>
      {children}
    </section>
  );
}

function DetailGrid({ children }: { children: React.ReactNode }) {
  return <div className="grid gap-x-8 gap-y-4 sm:grid-cols-2">{children}</div>;
}

function DetailItem({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="min-w-0">
      <p className="text-xs text-muted-foreground">{label}</p>
      <div className="mt-1 min-w-0 text-sm">{children}</div>
    </div>
  );
}

function DetailRows({ children }: { children: React.ReactNode }) {
  return <dl className="space-y-3">{children}</dl>;
}

function DetailRow({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="grid min-w-0 gap-1 text-sm sm:grid-cols-[128px_minmax(0,1fr)] sm:gap-4">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="min-w-0 break-all sm:text-right">{children}</dd>
    </div>
  );
}

function CopyRow({ label, value }: { label: string; value: string }) {
  return (
    <DetailRow label={label}>
      <span className="inline-flex max-w-full items-center justify-end gap-1">
        <span className="min-w-0 break-all font-mono text-xs">{value}</span>
        <TooltipProvider delayDuration={300}>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="size-7 shrink-0"
                onClick={() => copyValue(value)}
              >
                <Copy aria-hidden="true" />
                <span className="sr-only">复制 {label}</span>
              </Button>
            </TooltipTrigger>
            <TooltipContent>复制 {label}</TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </span>
    </DetailRow>
  );
}

function economicsStateLabel(value: EconomicsSnapshot["economics_state"]) {
  const labels: Record<EconomicsSnapshot["economics_state"], string> = {
    legacy_contract: "旧版契约",
    awaiting_quote: "等待报价",
    quoted: "已报价",
    metered: "已计量",
    rated: "已结算",
  };
  return labels[value];
}

function providerCostBasisLabel(value: JobProviderCost["cost_basis"]) {
  const labels: Record<JobProviderCost["cost_basis"], string> = {
    provider_actual: "Provider 实际成本",
    provider_allocated: "订阅 / 额度分摊",
    legacy_unverified: "旧版未验证成本",
  };
  return labels[value];
}

function jobDuration(item: JobListItem): number | null {
  if (item.started_at_ms === null || item.finished_at_ms === null) return null;
  return Math.max(0, item.finished_at_ms - item.started_at_ms);
}

async function copyValue(value: string) {
  try {
    await navigator.clipboard.writeText(value);
    toast.success("已复制");
  } catch {
    toast.error("复制失败");
  }
}
