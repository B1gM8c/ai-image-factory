"use client";

import { Activity, CircleDollarSign, Clock3, Gauge, RefreshCw } from "lucide-react";
import { AdminQueryError, AdminQuerySkeleton } from "@/components/admin-query-state";
import { MetricCard } from "@/components/metric-card";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useAdminQuery } from "@/hooks/use-admin-query";
import {
  formatDateTime,
  formatDurationMs,
  formatInteger,
  formatMoneyMicros,
  formatStatus,
  sumIntegers,
} from "@/lib/admin/format";
import type { OverviewSnapshot } from "@/lib/admin/types";

const ENDPOINT = "/v1/console/overview?window=24h";

export function AdminOverview() {
  const query = useAdminQuery<OverviewSnapshot>(ENDPOINT);

  return (
    <div className="min-w-0 space-y-6">
      <PageHeader title="运营总览" description="过去 24 小时任务终态、计量与已封账流水" />
      {query.loading ? <AdminQuerySkeleton rows={7} /> : null}
      {!query.loading && query.error && (!query.data || query.error.status === 403) ? (
        <AdminQueryError error={query.error} retry={query.retry} />
      ) : null}
      {query.data && (!query.error || query.error.status !== 403) ? (
        <OverviewContent
          data={query.data}
          refreshing={query.refreshing}
          stale={Boolean(query.error)}
          retry={query.retry}
        />
      ) : null}
    </div>
  );
}

function OverviewContent({
  data,
  refreshing,
  stale,
  retry,
}: {
  data: OverviewSnapshot;
  refreshing: boolean;
  stale: boolean;
  retry: () => void;
}) {
  const totalJobs = sumIntegers(data.job_states.map((item) => item.count));
  const ledgerTransactions = sumIntegers(data.sealed_ledger.map((item) => item.transaction_count));

  return (
    <>
      {stale || refreshing ? (
        <div className="flex min-h-10 flex-wrap items-center justify-between gap-2 border px-3 py-2 text-sm">
          <span className="text-muted-foreground">{refreshing ? "正在刷新数据" : "当前显示上一次成功快照"}</span>
          {!refreshing ? (
            <Button type="button" variant="outline" size="sm" onClick={retry}>
              <RefreshCw aria-hidden="true" />
              重试
            </Button>
          ) : null}
        </div>
      ) : null}

      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="任务数" value={formatInteger(totalJobs)} detail="按创建时间归入窗口" icon={Activity} />
        <MetricCard
          label="计量维度"
          value={data.charged_usage.length.toLocaleString("zh-CN")}
          detail="按指标、单位与结果拆分"
          icon={Gauge}
          tone="info"
        />
        <MetricCard
          label="终态耗时 P95"
          value={formatDurationMs(data.terminal_job_elapsed_p95_ms)}
          detail={`${formatInteger(data.terminal_job_elapsed_samples)} 个终态样本`}
          icon={Clock3}
        />
        <MetricCard
          label="封账交易"
          value={formatInteger(ledgerTransactions)}
          detail="期间已封账 customer charge"
          icon={CircleDollarSign}
          tone="success"
        />
      </section>

      <section className="grid min-w-0 gap-5 xl:grid-cols-[0.7fr_1.3fr]">
        <Card className="min-w-0">
          <CardHeader className="p-4 pb-2">
            <CardTitle className="text-base">任务终态</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 p-4 pt-2">
            {data.job_states.length === 0 ? (
              <EmptyState label="窗口内暂无任务" />
            ) : (
              data.job_states.map((item) => (
                <div key={item.state} className="flex min-h-10 items-center justify-between gap-3 border-b py-2 last:border-0">
                  <Badge variant="outline">{formatStatus(item.state)}</Badge>
                  <span className="font-mono text-sm tabular-nums">{formatInteger(item.count)}</span>
                </div>
              ))
            )}
          </CardContent>
        </Card>

        <Card className="min-w-0 overflow-hidden">
          <CardHeader className="p-4 pb-2">
            <CardTitle className="text-base">已计量用量</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            {data.charged_usage.length === 0 ? (
              <EmptyState label="窗口内暂无计量记录" />
            ) : (
              <Table className="min-w-[620px]">
                <TableHeader>
                  <TableRow>
                    <TableHead className="pl-4">指标</TableHead>
                    <TableHead>结果</TableHead>
                    <TableHead>单位</TableHead>
                    <TableHead className="pr-4 text-right">数量</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.charged_usage.map((item, index) => (
                    <TableRow key={`${item.tenant_id ?? "all"}-${item.billing_metric}-${item.billing_unit}-${item.outcome}-${index}`}>
                      <TableCell className="pl-4 font-medium">{item.billing_metric}</TableCell>
                      <TableCell>{formatStatus(item.outcome)}</TableCell>
                      <TableCell className="font-mono text-xs">{item.billing_unit}</TableCell>
                      <TableCell className="pr-4 text-right font-mono tabular-nums">{formatInteger(item.quantity)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </section>

      <Card className="min-w-0 overflow-hidden">
        <CardHeader className="p-4 pb-2">
          <CardTitle className="text-base">期间已封账流水</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {data.sealed_ledger.length === 0 ? (
            <EmptyState label="窗口内暂无已封账流水" />
          ) : (
            <Table className="min-w-[680px]">
              <TableHeader>
                <TableRow>
                  <TableHead className="pl-4">币种</TableHead>
                  <TableHead>交易类型</TableHead>
                  <TableHead>交易数</TableHead>
                  <TableHead className="pr-4 text-right">金额</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.sealed_ledger.map((item, index) => (
                  <TableRow key={`${item.tenant_id ?? "all"}-${item.currency}-${item.transaction_type}-${index}`}>
                    <TableCell className="pl-4 font-mono text-xs uppercase">{item.currency}</TableCell>
                    <TableCell>{formatStatus(item.transaction_type)}</TableCell>
                    <TableCell className="font-mono tabular-nums">{formatInteger(item.transaction_count)}</TableCell>
                    <TableCell className="pr-4 text-right font-mono tabular-nums">
                      {formatMoneyMicros(item.amount_micros, item.currency)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <p className="text-right text-xs text-muted-foreground">快照时间 {formatDateTime(data.as_of_ms)}</p>
    </>
  );
}

function EmptyState({ label }: { label: string }) {
  return <div className="flex min-h-28 items-center justify-center px-4 text-sm text-muted-foreground">{label}</div>;
}
