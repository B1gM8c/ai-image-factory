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
import { useI18n } from "@/i18n/locale-provider";

const ENDPOINT = "/v1/console/overview?window=24h";

export function AdminOverview() {
  const { t } = useI18n();
  const query = useAdminQuery<OverviewSnapshot>(ENDPOINT);

  return (
    <div className="min-w-0 space-y-6">
      <PageHeader
        title={t({
          en: "Operations overview",
          "zh-CN": "运营总览",
          ja: "運用概要",
          ko: "운영 개요",
        })}
        description={t({
          en: "Terminal job states, metered usage, and sealed ledger activity from the past 24 hours",
          "zh-CN": "过去 24 小时任务终态、计量与已封账流水",
          ja: "過去 24 時間のジョブ終端状態、計測済み使用量、確定済み台帳",
          ko: "지난 24시간의 작업 최종 상태, 계측 사용량 및 마감 원장",
        })}
      />
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
  const { locale, t } = useI18n();
  const totalJobs = sumIntegers(data.job_states.map((item) => item.count));
  const ledgerTransactions = sumIntegers(data.sealed_ledger.map((item) => item.transaction_count));

  return (
    <>
      {stale || refreshing ? (
        <div className="flex min-h-10 flex-wrap items-center justify-between gap-2 border px-3 py-2 text-sm">
          <span className="text-muted-foreground">
            {refreshing
              ? t({ en: "Refreshing data", "zh-CN": "正在刷新数据", ja: "データを更新中", ko: "데이터 새로 고치는 중" })
              : t({ en: "Showing the last successful snapshot", "zh-CN": "当前显示上一次成功快照", ja: "前回成功したスナップショットを表示しています", ko: "마지막으로 성공한 스냅샷을 표시 중입니다" })}
          </span>
          {!refreshing ? (
            <Button type="button" variant="outline" size="sm" onClick={retry}>
              <RefreshCw aria-hidden="true" />
              {t({ en: "Retry", "zh-CN": "重试", ja: "再試行", ko: "다시 시도" })}
            </Button>
          ) : null}
        </div>
      ) : null}

      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label={t({ en: "Jobs", "zh-CN": "任务数", ja: "ジョブ数", ko: "작업 수" })}
          value={formatInteger(totalJobs)}
          detail={t({ en: "Included by creation time", "zh-CN": "按创建时间归入窗口", ja: "作成時刻に基づいて集計", ko: "생성 시간을 기준으로 집계" })}
          icon={Activity}
        />
        <MetricCard
          label={t({ en: "Metering dimensions", "zh-CN": "计量维度", ja: "計測ディメンション", ko: "계측 차원" })}
          value={data.charged_usage.length.toLocaleString(locale)}
          detail={t({ en: "Split by metric, unit, and outcome", "zh-CN": "按指标、单位与结果拆分", ja: "指標、単位、結果別", ko: "지표, 단위 및 결과별" })}
          icon={Gauge}
          tone="info"
        />
        <MetricCard
          label={t({ en: "Terminal latency P95", "zh-CN": "终态耗时 P95", ja: "終端レイテンシ P95", ko: "최종 상태 지연 시간 P95" })}
          value={formatDurationMs(data.terminal_job_elapsed_p95_ms)}
          detail={t(
            {
              en: "{count} terminal samples",
              "zh-CN": "{count} 个终态样本",
              ja: "終端サンプル {count} 件",
              ko: "최종 상태 샘플 {count}개",
            },
            { count: formatInteger(data.terminal_job_elapsed_samples) },
          )}
          icon={Clock3}
        />
        <MetricCard
          label={t({ en: "Sealed transactions", "zh-CN": "封账交易", ja: "確定済み取引", ko: "마감 거래" })}
          value={formatInteger(ledgerTransactions)}
          detail={t({ en: "Customer charges sealed in this period", "zh-CN": "期间已封账 customer charge", ja: "期間内に確定した顧客請求", ko: "기간 내 마감된 고객 청구" })}
          icon={CircleDollarSign}
          tone="success"
        />
      </section>

      <section className="grid min-w-0 gap-5 xl:grid-cols-[0.7fr_1.3fr]">
        <Card className="min-w-0">
          <CardHeader className="p-4 pb-2">
            <CardTitle className="text-base">{t({ en: "Terminal job states", "zh-CN": "任务终态", ja: "ジョブ終端状態", ko: "작업 최종 상태" })}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 p-4 pt-2">
            {data.job_states.length === 0 ? (
              <EmptyState label={t({ en: "No jobs in this window", "zh-CN": "窗口内暂无任务", ja: "この期間にジョブはありません", ko: "이 기간에 작업이 없습니다" })} />
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
            <CardTitle className="text-base">{t({ en: "Metered usage", "zh-CN": "已计量用量", ja: "計測済み使用量", ko: "계측 사용량" })}</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            {data.charged_usage.length === 0 ? (
              <EmptyState label={t({ en: "No metering records in this window", "zh-CN": "窗口内暂无计量记录", ja: "この期間に計測記録はありません", ko: "이 기간에 계측 기록이 없습니다" })} />
            ) : (
              <Table className="min-w-[620px]">
                <TableHeader>
                  <TableRow>
                    <TableHead className="pl-4">{t({ en: "Metric", "zh-CN": "指标", ja: "指標", ko: "지표" })}</TableHead>
                    <TableHead>{t({ en: "Outcome", "zh-CN": "结果", ja: "結果", ko: "결과" })}</TableHead>
                    <TableHead>{t({ en: "Unit", "zh-CN": "单位", ja: "単位", ko: "단위" })}</TableHead>
                    <TableHead className="pr-4 text-right">{t({ en: "Quantity", "zh-CN": "数量", ja: "数量", ko: "수량" })}</TableHead>
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
          <CardTitle className="text-base">{t({ en: "Sealed ledger entries", "zh-CN": "期间已封账流水", ja: "期間内の確定済み台帳", ko: "기간 내 마감 원장 항목" })}</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {data.sealed_ledger.length === 0 ? (
            <EmptyState label={t({ en: "No sealed ledger entries in this window", "zh-CN": "窗口内暂无已封账流水", ja: "この期間に確定済み台帳はありません", ko: "이 기간에 마감 원장 항목이 없습니다" })} />
          ) : (
            <Table className="min-w-[680px]">
              <TableHeader>
                <TableRow>
                  <TableHead className="pl-4">{t({ en: "Currency", "zh-CN": "币种", ja: "通貨", ko: "통화" })}</TableHead>
                  <TableHead>{t({ en: "Transaction type", "zh-CN": "交易类型", ja: "取引種別", ko: "거래 유형" })}</TableHead>
                  <TableHead>{t({ en: "Transactions", "zh-CN": "交易数", ja: "取引数", ko: "거래 수" })}</TableHead>
                  <TableHead className="pr-4 text-right">{t({ en: "Amount", "zh-CN": "金额", ja: "金額", ko: "금액" })}</TableHead>
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

      <p className="text-right text-xs text-muted-foreground">
        {t(
          {
            en: "Snapshot {time}",
            "zh-CN": "快照时间 {time}",
            ja: "スナップショット {time}",
            ko: "스냅샷 {time}",
          },
          { time: formatDateTime(data.as_of_ms) },
        )}
      </p>
    </>
  );
}

function EmptyState({ label }: { label: string }) {
  return <div className="flex min-h-28 items-center justify-center px-4 text-sm text-muted-foreground">{label}</div>;
}
