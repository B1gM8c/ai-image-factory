"use client";

import { useState } from "react";
import {
  AlertTriangle,
  CheckCircle2,
  ChevronRight,
  LoaderCircle,
  RefreshCw,
  ScanSearch,
  ShieldCheck,
} from "lucide-react";
import { toast } from "sonner";
import { AdminQueryError, AdminQuerySkeleton } from "@/components/admin-query-state";
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
import { useAdminQuery } from "@/hooks/use-admin-query";
import { formatDateTime, formatMoneyMicros } from "@/lib/admin/format";
import type {
  BillingIntegrityFinding,
  BillingIntegrityRun,
  BillingIntegrityRunDetail,
  BillingIntegrityRunList,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

export function BillingIntegrityPanel({ enabled }: { enabled: boolean }) {
  const [running, setRunning] = useState(false);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const query = useAdminQuery<BillingIntegrityRunList>(
    "/admin/v1/billing/integrity-runs?limit=25",
    enabled,
  );

  async function runCheck() {
    setRunning(true);
    try {
      const response = await consoleFetch(
        "/api/gateway/admin/v1/billing/integrity-runs",
        { method: "POST", body: "{}" },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      const result = (await response.json()) as BillingIntegrityRunDetail;
      query.retry();
      setSelectedRunId(result.run_id);
      if (result.finding_count === 0) {
        toast.success("账务检查完成，未发现异常");
      } else {
        toast.warning(`账务检查完成，发现 ${result.finding_count} 项需要处理`);
      }
    } catch (caught) {
      toast.error(caught instanceof Error ? caught.message : "账务检查失败");
    } finally {
      setRunning(false);
    }
  }

  const latest = query.data?.data[0] ?? null;

  return (
    <section className="space-y-4">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-base font-semibold">账务检查</h2>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            核对余额、冻结、客户收费与项目归属。检查只读取并留存证据，不会自动修改账务。
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            type="button"
            variant="outline"
            size="icon"
            aria-label="刷新检查记录"
            onClick={query.retry}
            disabled={query.refreshing || running}
          >
            <RefreshCw
              className={query.refreshing ? "animate-spin" : ""}
              aria-hidden="true"
            />
          </Button>
          <Button type="button" onClick={() => void runCheck()} disabled={running}>
            {running ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <ScanSearch aria-hidden="true" />
            )}
            运行检查
          </Button>
        </div>
      </div>

      {latest ? <LatestRunSummary run={latest} /> : null}

      {query.loading ? <AdminQuerySkeleton rows={5} /> : null}
      {!query.loading && query.error && !query.data ? (
        <AdminQueryError error={query.error} retry={query.retry} />
      ) : null}
      {!query.loading && query.data?.data.length === 0 ? (
        <div className="flex min-h-64 flex-col items-center justify-center rounded-md border px-6 text-center">
          <ShieldCheck className="size-8 text-muted-foreground" aria-hidden="true" />
          <h3 className="mt-4 text-sm font-medium">尚未运行账务检查</h3>
          <p className="mt-1 max-w-md text-sm text-muted-foreground">
            运行后可查看每次一致性快照和完整异常证据。
          </p>
        </div>
      ) : null}
      {query.data && query.data.data.length > 0 ? (
        <div className="overflow-hidden rounded-md border">
          <div className="overflow-x-auto">
            <Table className="min-w-[760px]">
              <TableHeader>
                <TableRow>
                  <TableHead className="pl-4">完成时间</TableHead>
                  <TableHead>结果</TableHead>
                  <TableHead>严重</TableHead>
                  <TableHead>提醒</TableHead>
                  <TableHead>检查项</TableHead>
                  <TableHead className="w-12 pr-4">
                    <span className="sr-only">查看</span>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {query.data.data.map((run) => (
                  <TableRow
                    key={run.run_id}
                    className="cursor-pointer"
                    onClick={() => setSelectedRunId(run.run_id)}
                  >
                    <TableCell className="pl-4 font-medium">
                      {formatDateTime(run.completed_at_ms)}
                    </TableCell>
                    <TableCell>
                      <RunStatus run={run} />
                    </TableCell>
                    <TableCell className="font-mono tabular-nums">
                      {run.critical_count}
                    </TableCell>
                    <TableCell className="font-mono tabular-nums">
                      {run.warning_count}
                    </TableCell>
                    <TableCell className="text-muted-foreground">
                      {run.check_set.length} 项
                    </TableCell>
                    <TableCell className="pr-4 text-right">
                      <Button
                        type="button"
                        variant="ghost"
                        size="icon"
                        aria-label={`查看 ${formatDateTime(run.completed_at_ms)} 的检查结果`}
                        onClick={(event) => {
                          event.stopPropagation();
                          setSelectedRunId(run.run_id);
                        }}
                      >
                        <ChevronRight aria-hidden="true" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </div>
      ) : null}

      <BillingIntegrityRunSheet
        runId={selectedRunId}
        onOpenChange={(open) => {
          if (!open) setSelectedRunId(null);
        }}
      />
    </section>
  );
}

function LatestRunSummary({ run }: { run: BillingIntegrityRun }) {
  const clean = run.finding_count === 0;
  return (
    <div className="flex flex-wrap items-center gap-x-6 gap-y-2 rounded-md border px-4 py-3 text-sm">
      <div className="flex items-center gap-2 font-medium">
        {clean ? (
          <CheckCircle2 className="size-4" aria-hidden="true" />
        ) : (
          <AlertTriangle className="size-4" aria-hidden="true" />
        )}
        最近一次：{clean ? "未发现异常" : `${run.finding_count} 项需要处理`}
      </div>
      <span className="text-muted-foreground">
        严重 {run.critical_count} · 提醒 {run.warning_count}
      </span>
      <span className="ml-auto text-muted-foreground">
        {formatDateTime(run.completed_at_ms)}
      </span>
    </div>
  );
}

function RunStatus({ run }: { run: BillingIntegrityRun }) {
  if (run.critical_count > 0) return <Badge variant="destructive">需要处理</Badge>;
  if (run.warning_count > 0) return <Badge variant="secondary">需要关注</Badge>;
  return <Badge variant="outline">正常</Badge>;
}

function BillingIntegrityRunSheet({
  runId,
  onOpenChange,
}: {
  runId: string | null;
  onOpenChange: (open: boolean) => void;
}) {
  const query = useAdminQuery<BillingIntegrityRunDetail>(
    runId ? `/admin/v1/billing/integrity-runs/${encodeURIComponent(runId)}` : "",
    Boolean(runId),
  );

  return (
    <Sheet open={Boolean(runId)} onOpenChange={onOpenChange}>
      <SheetContent className="flex h-full w-full min-w-0 flex-col gap-0 overflow-hidden p-0 sm:max-w-3xl">
        <SheetHeader className="shrink-0 border-b px-5 py-5 pr-12 text-left sm:px-6">
          <SheetTitle>账务检查结果</SheetTitle>
          <SheetDescription>
            {query.data
              ? `${formatDateTime(query.data.completed_at_ms)} · 一致性快照`
              : "正在读取检查记录"}
          </SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1 overflow-y-auto">
          {query.loading ? (
            <div className="p-5 sm:p-6">
              <AdminQuerySkeleton rows={6} />
            </div>
          ) : null}
          {!query.loading && query.error && !query.data ? (
            <div className="p-5 sm:p-6">
              <AdminQueryError error={query.error} retry={query.retry} />
            </div>
          ) : null}
          {query.data ? <RunDetail detail={query.data} /> : null}
        </div>
      </SheetContent>
    </Sheet>
  );
}

function RunDetail({ detail }: { detail: BillingIntegrityRunDetail }) {
  return (
    <>
      <div className="grid grid-cols-3 border-b">
        <DetailMetric label="严重" value={detail.critical_count.toString()} />
        <DetailMetric label="提醒" value={detail.warning_count.toString()} />
        <DetailMetric
          label="检查项"
          value={detail.check_set.length.toString()}
          last
        />
      </div>
      <div className="space-y-5 p-5 sm:p-6">
        <div className="flex items-start gap-3 rounded-md bg-muted/50 px-4 py-3 text-sm">
          <ShieldCheck className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
          <p className="text-muted-foreground">
            结果来自同一数据库快照。检查不会释放冻结、补记交易或修改历史记录。
          </p>
        </div>
        {detail.findings.length === 0 ? (
          <div className="flex min-h-56 flex-col items-center justify-center text-center">
            <CheckCircle2 className="size-8 text-muted-foreground" aria-hidden="true" />
            <h3 className="mt-4 text-sm font-medium">未发现账务异常</h3>
            <p className="mt-1 text-sm text-muted-foreground">
              本次检查覆盖的余额、冻结、收费与归属数据保持一致。
            </p>
          </div>
        ) : (
          <div className="space-y-3">
            <h3 className="text-sm font-semibold">需要处理</h3>
            {detail.findings.map((finding) => (
              <FindingRow key={finding.finding_id} finding={finding} />
            ))}
          </div>
        )}
      </div>
    </>
  );
}

function DetailMetric({
  label,
  value,
  last = false,
}: {
  label: string;
  value: string;
  last?: boolean;
}) {
  return (
    <div className={last ? "px-5 py-4" : "border-r px-5 py-4"}>
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="mt-1 text-xl font-semibold tabular-nums">{value}</p>
    </div>
  );
}

function FindingRow({ finding }: { finding: BillingIntegrityFinding }) {
  return (
    <article className="rounded-md border p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <Badge
              variant={finding.severity === "critical" ? "destructive" : "secondary"}
            >
              {finding.severity === "critical" ? "严重" : "提醒"}
            </Badge>
            <span className="text-sm font-medium">
              {findingTitle(finding.finding_code)}
            </span>
          </div>
          <p className="mt-2 break-all text-xs text-muted-foreground">
            {categoryLabel(finding.category)} · {finding.resource_id}
          </p>
        </div>
        {finding.currency ? (
          <span className="text-xs text-muted-foreground">{finding.currency}</span>
        ) : null}
      </div>
      <p className="mt-3 text-sm text-muted-foreground">
        {findingDescription(finding)}
      </p>
    </article>
  );
}

function findingTitle(code: string) {
  const labels: Record<string, string> = {
    billing_account_counter_mismatch: "账户汇总与账务事实不一致",
    stale_terminal_hold: "终态任务仍占用额度",
    customer_charge_mismatch: "计价结果与客户收费不一致",
    customer_refund_evidence_missing: "退款交易缺少业务证据",
    customer_refund_mismatch: "退款证据与冲正账本不一致",
    customer_charge_attribution_missing: "收费缺少项目或凭据归属",
    provider_cost_obligation_missing: "上游执行缺少成本追踪记录",
    provider_cost_obligation_overdue: "上游成本结论已逾期",
    provider_cost_authority_missing: "上游成本事实缺少唯一权威",
  };
  return labels[code] ?? "发现账务一致性问题";
}

function categoryLabel(category: string) {
  const labels: Record<string, string> = {
    account_balance: "账户余额",
    hold_lifecycle: "额度冻结",
    customer_charge: "客户收费",
    customer_refund: "客户退款",
    attribution: "项目归属",
    provider_cost: "上游成本",
  };
  return labels[category] ?? category;
}

function findingDescription(finding: BillingIntegrityFinding) {
  if (finding.finding_code === "billing_account_counter_mismatch") {
    return `${moneyField(finding.actual, "held_micros", finding.currency)} 已冻结，账务事实应为 ${moneyField(
      finding.expected,
      "held_micros",
      finding.currency,
    )}；已结算 ${moneyField(
      finding.actual,
      "captured_micros",
      finding.currency,
    )}，账务事实应为 ${moneyField(
      finding.expected,
      "captured_micros",
      finding.currency,
    )}；已退款 ${moneyField(
      finding.actual,
      "refunded_micros",
      finding.currency,
    )}，账务事实应为 ${moneyField(
      finding.expected,
      "refunded_micros",
      finding.currency,
    )}。`;
  }
  if (finding.finding_code === "stale_terminal_hold") {
    return `任务已经结束，但 ${moneyField(
      finding.actual,
      "held_micros",
      finding.currency,
    )} 仍处于冻结状态。`;
  }
  if (finding.finding_code === "customer_charge_mismatch") {
    return "计价金额、客户应收、平台收入或封账记录未能形成一一对应关系。";
  }
  if (finding.finding_code === "customer_charge_attribution_missing") {
    return "该笔收费尚未关联到完整的项目、用户或 API Key 归属信息。";
  }
  if (
    finding.finding_code === "customer_refund_evidence_missing" ||
    finding.finding_code === "customer_refund_mismatch"
  ) {
    return "退款业务证据、原始扣费、冲正交易、双分录与封账状态未能形成一一对应关系。";
  }
  if (finding.finding_code === "provider_cost_authority_missing") {
    return "上游已返回明确的实际成本事实，但尚未形成可审计、唯一且已入账的成本权威。";
  }
  if (finding.finding_code === "provider_cost_obligation_missing") {
    return "该次上游执行没有对应的成本追踪记录，无法证明其最终成本是已结算、待确认或经证据豁免。";
  }
  if (finding.finding_code === "provider_cost_obligation_overdue") {
    return "该次上游执行的成本结论超过处理时限；仍需补充权威成本或可核验的豁免证据，不能按超时自动视为免费。";
  }
  return "请根据该记录的资源标识进一步核对账务事实。";
}

function moneyField(
  value: Record<string, unknown>,
  key: string,
  currency: string | null,
) {
  const raw = value[key];
  if (typeof raw !== "string") return "--";
  return formatMoneyMicros(raw, currency ?? "USD");
}

async function responseMessage(response: Response) {
  try {
    const payload = (await response.json()) as {
      error?: string | { message?: string };
    };
    if (typeof payload.error === "string") return payload.error;
    if (payload.error?.message) return payload.error.message;
  } catch {
    // Keep upstream response bodies private.
  }
  if (response.status === 409) return "已有账务检查正在运行";
  if (response.status === 403) return "当前账号没有运行平台账务检查的权限";
  return `账务检查失败（${response.status}）`;
}
