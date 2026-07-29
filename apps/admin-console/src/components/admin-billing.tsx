"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import { AlertTriangle, Gauge, RefreshCw, WalletCards } from "lucide-react";
import { AdminQueryError, AdminQuerySkeleton } from "@/components/admin-query-state";
import { useConsoleSession } from "@/components/auth/console-session-provider";
import { BillingAccountControlSheet } from "@/components/billing/billing-account-control-sheet";
import { BillingIntegrityPanel } from "@/components/billing/billing-integrity-panel";
import { CreditGrantsPanel } from "@/components/billing/credit-grants-panel";
import { CustomerRefundsPanel } from "@/components/billing/customer-refunds-panel";
import { ProviderCostObligationsPanel } from "@/components/billing/provider-cost-obligations-panel";
import { PageHeader } from "@/components/page-header";
import {
  ProjectLimitsSheet,
  type ProjectSettingsTarget,
} from "@/components/projects/project-limits-sheet";
import {
  UsageAnalysisPanel,
  type UsageWindow,
} from "@/components/usage/usage-analysis-panel";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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
import {
  formatDateTime,
  formatInteger,
  formatMoneyMicros,
  formatStatus,
} from "@/lib/admin/format";
import type {
  BillingSnapshot,
  ConsoleBillingSnapshot,
  LedgerAggregate,
  ProviderCostAggregate,
} from "@/lib/admin/types";

type UsageSnapshot = ConsoleBillingSnapshot | BillingSnapshot;
type BillingSection =
  | "usage"
  | "credit_grants"
  | "refunds"
  | "provider_costs"
  | "integrity";

export function AdminBilling() {
  const {
    activeWorkspace,
    loading: sessionLoading,
    organizations,
    projects,
    user,
  } = useConsoleSession();
  const [window, setWindow] = useState<UsageWindow>("24h");
  const [analysisRefreshKey, setAnalysisRefreshKey] = useState(0);
  const [budgetOpen, setBudgetOpen] = useState(false);
  const [billingControlOpen, setBillingControlOpen] = useState(false);
  const [section, setSection] = useState<BillingSection>("usage");
  const platformOwner = Boolean(
    user?.roles.includes("platform_owner") && hasScope(user.scopes, "admin:*"),
  );
  const projectId = activeWorkspace?.kind === "project" ? activeWorkspace.id : null;
  const organizationId =
    activeWorkspace?.kind === "organization" ? activeWorkspace.id : null;
  const organizationRole = organizationId
    ? organizations.find((organization) => organization.id === organizationId)?.role
    : null;
  const canInspectBilling = platformOwner && !projectId;
  const creditGrantScopeAvailable = Boolean(
    activeWorkspace?.kind === "platform" ||
      activeWorkspace?.kind === "organization",
  );
  const canViewCreditGrants = Boolean(
    creditGrantScopeAvailable &&
      (platformOwner ||
        (organizationId &&
          organizationRole === "owner" &&
          user &&
          hasScope(user.scopes, "workspace:read"))),
  );
  const activeSection: BillingSection =
    section === "credit_grants"
      ? canViewCreditGrants
        ? section
        : "usage"
      : canInspectBilling
        ? section
        : "usage";
  const projectMembership = projectId
    ? projects.find((project) => project.id === projectId)
    : null;
  const canManageBudget = Boolean(
    platformOwner ||
      projectMembership?.role === "owner" ||
      organizations.some(
        (organization) =>
          organization.id === projectMembership?.organization_id &&
          organization.role === "owner",
      ),
  );
  const budgetProject = useMemo<ProjectSettingsTarget | null>(
    () =>
      projectId && activeWorkspace
        ? {
            id: projectId,
            name: activeWorkspace.name,
            status: "active",
            created_at: null,
          }
        : null,
    [activeWorkspace, projectId],
  );
  const endpoint = useMemo(() => {
    const base = platformOwner
      ? "/admin/v1/billing/summary"
      : "/v1/console/billing/summary";
    const params = new URLSearchParams({ window });
    if (projectId) params.set("project_id", projectId);
    return `${base}?${params.toString()}`;
  }, [platformOwner, projectId, window]);
  const query = useAdminQuery<UsageSnapshot>(
    endpoint,
    !sessionLoading &&
      Boolean(user && activeWorkspace) &&
      activeSection === "usage",
  );
  const tenantNames = useMemo(
    () => new Map(organizations.map((organization) => [organization.id, organization.name])),
    [organizations],
  );

  return (
    <div className="min-w-0 space-y-6 overflow-x-clip">
      <PageHeader
        title="用量与计费"
        description={activeWorkspace?.name ?? "当前工作区"}
        actions={
          activeSection === "usage" ? (
            <>
            {platformOwner ? (
              <Button
                type="button"
                variant="outline"
                onClick={() => setBillingControlOpen(true)}
              >
                <WalletCards aria-hidden="true" />
                组织限额
              </Button>
            ) : null}
            {budgetProject ? (
              <Button
                type="button"
                variant="outline"
                onClick={() => setBudgetOpen(true)}
              >
                <Gauge aria-hidden="true" />
                项目预算
              </Button>
            ) : (
              <Button asChild type="button" variant="outline">
                <Link href="/projects">
                  <Gauge aria-hidden="true" />
                  项目预算
                </Link>
              </Button>
            )}
            <Tabs value={window} onValueChange={(value) => setWindow(value as UsageWindow)}>
              <TabsList className="h-9">
                <TabsTrigger value="24h">24 小时</TabsTrigger>
                <TabsTrigger value="7d">7 天</TabsTrigger>
                <TabsTrigger value="30d">30 天</TabsTrigger>
              </TabsList>
            </Tabs>
            <Button
              type="button"
              variant="outline"
              size="icon"
              aria-label="刷新用量"
              onClick={() => {
                query.retry();
                setAnalysisRefreshKey((value) => value + 1);
              }}
              disabled={query.refreshing}
            >
              <RefreshCw
                className={query.refreshing ? "animate-spin" : ""}
                aria-hidden="true"
              />
            </Button>
            </>
          ) : null
        }
      />

      {canInspectBilling || canViewCreditGrants ? (
        <Tabs
          className="min-w-0 max-w-full overflow-x-auto pb-1"
          value={activeSection}
          onValueChange={(value) => setSection(value as BillingSection)}
        >
          <TabsList className="w-max">
            <TabsTrigger value="usage">用量概览</TabsTrigger>
            {canViewCreditGrants ? (
              <TabsTrigger value="credit_grants">Credit Grants</TabsTrigger>
            ) : null}
            {canInspectBilling ? (
              <>
                <TabsTrigger value="refunds">退款与冲正</TabsTrigger>
                <TabsTrigger value="provider_costs">上游成本</TabsTrigger>
                <TabsTrigger value="integrity">账务检查</TabsTrigger>
              </>
            ) : null}
          </TabsList>
        </Tabs>
      ) : null}

      {activeSection === "usage" ? (
        <>
          {query.loading || sessionLoading ? <AdminQuerySkeleton rows={8} /> : null}
          {!query.loading &&
          query.error &&
          (!query.data || query.error.status === 403) ? (
            <AdminQueryError error={query.error} retry={query.retry} />
          ) : null}
          {query.data && (!query.error || query.error.status !== 403) ? (
            <UsageContent
              data={query.data}
              platformOwner={platformOwner}
              tenantNames={tenantNames}
              stale={Boolean(query.error)}
              window={window}
              projectId={projectId}
              analysisRefreshKey={analysisRefreshKey}
              analysisEnabled={!sessionLoading && Boolean(user && activeWorkspace)}
            />
          ) : null}
        </>
      ) : activeSection === "credit_grants" ? (
        <CreditGrantsPanel
          enabled={!sessionLoading && Boolean(user && activeWorkspace)}
          platformOwner={platformOwner}
          organizationId={organizationId}
          organizations={organizations.map((organization) => ({
            id: organization.id,
            name: organization.name,
          }))}
        />
      ) : activeSection === "refunds" ? (
        <CustomerRefundsPanel
          enabled={!sessionLoading && Boolean(user && activeWorkspace)}
          tenantNames={tenantNames}
        />
      ) : activeSection === "provider_costs" ? (
        <ProviderCostObligationsPanel
          enabled={!sessionLoading && Boolean(user && activeWorkspace)}
        />
      ) : (
        <BillingIntegrityPanel
          enabled={!sessionLoading && Boolean(user && activeWorkspace)}
        />
      )}
      <ProjectLimitsSheet
        project={budgetOpen ? budgetProject : null}
        canManage={canManageBudget}
        initialTab="limits"
        onOpenChange={setBudgetOpen}
      />
      <BillingAccountControlSheet
        open={billingControlOpen}
        onOpenChange={setBillingControlOpen}
        onUpdated={() => {
          query.retry();
          setAnalysisRefreshKey((value) => value + 1);
        }}
      />
    </div>
  );
}

function hasScope(scopes: string[], required: string) {
  return scopes.includes(required) || scopes.includes("admin:*");
}

function UsageContent({
  data,
  platformOwner,
  tenantNames,
  stale,
  window,
  projectId,
  analysisRefreshKey,
  analysisEnabled,
}: {
  data: UsageSnapshot;
  platformOwner: boolean;
  tenantNames: Map<string, string>;
  stale: boolean;
  window: UsageWindow;
  projectId: string | null;
  analysisRefreshKey: number;
  analysisEnabled: boolean;
}) {
  const platformData = isPlatformSnapshot(data) ? data : null;
  const requests = sumUsage(data, ["request"]);
  const outputs = sumUsage(data, ["output", "image_output"]);
  const videoSeconds = sumUsage(data, ["video_output_second"]);
  const grossRevenue = moneyByCurrency(data.sealed_ledger, (item) =>
    ["customer_charge", "customer_job_charge"].includes(item.transaction_type),
  );
  const refunds = moneyByCurrency(
    data.sealed_ledger,
    (item) => item.transaction_type === "customer_refund",
  );
  const revenue = subtractCurrencyTotals(grossRevenue, refunds);
  const authoritativeCosts = platformData
    ? costByCurrency(
        platformData.provider_costs.filter(
          (item) =>
            item.cost_basis === "provider_actual" ||
            item.cost_basis === "provider_allocated",
        ),
      )
    : new Map<string, bigint>();
  const coverage = platformData?.provider_cost_coverage ?? null;
  const marginAvailable = Boolean(
    coverage &&
      coverage.uncovered_receipts === "0" &&
      coverage.unattributed_transactions === "0" &&
      coverage.authority_conflicts === "0" &&
      coverage.legacy_unverified_transactions === "0",
  );

  return (
    <>
      {stale ? (
        <div className="flex items-center gap-2 rounded-md border px-3 py-2 text-sm text-muted-foreground">
          <AlertTriangle className="size-4" aria-hidden="true" />
          当前显示上一次成功快照
        </div>
      ) : null}

      <section className="grid overflow-hidden rounded-md border sm:grid-cols-2 xl:grid-cols-4">
        {platformOwner ? (
          <>
            <SummaryMetric
              label="客户净收入"
              {...netRevenueSummary(revenue, grossRevenue, refunds)}
            />
            <SummaryMetric
              label="供应商成本"
              {...providerCostSummary(authoritativeCosts, coverage)}
            />
            <SummaryMetric
              label="毛利"
              {...marginSummary(revenue, authoritativeCosts, marginAvailable)}
            />
            <SummaryMetric
              label="实际成本覆盖"
              {...coverageSummary(coverage)}
              last
            />
          </>
        ) : (
          <>
            <SummaryMetric
              label="本期净支出"
              {...netRevenueSummary(revenue, grossRevenue, refunds)}
            />
            <SummaryMetric
              label="请求"
              value={formatInteger(requests)}
              detail="已进入计量的调用"
            />
            <SummaryMetric
              label="图片输出"
              value={formatInteger(outputs)}
              detail="成功计量的图片"
            />
            <SummaryMetric
              label="实际输出视频时长"
              value={`${formatInteger(videoSeconds)} 秒`}
              detail="成功生成并完成计量"
              last
            />
          </>
        )}
      </section>

      <UsageAnalysisPanel
        window={window}
        projectId={projectId}
        platformOwner={platformOwner}
        enabled={analysisEnabled}
        refreshKey={analysisRefreshKey}
      />

      {platformData ? (
        <ProviderCostTable
          rows={platformData.provider_costs}
          coverage={platformData.provider_cost_coverage}
          tenantNames={tenantNames}
        />
      ) : (
        <BalanceTable data={data} tenantNames={tenantNames} />
      )}

      <p className="text-right text-xs text-muted-foreground">
        更新于 {formatDateTime(data.as_of_ms)}
      </p>
    </>
  );
}

function ProviderCostTable({
  rows,
  coverage,
  tenantNames,
}: {
  rows: ProviderCostAggregate[];
  coverage: BillingSnapshot["provider_cost_coverage"];
  tenantNames: Map<string, string>;
}) {
  return (
    <section>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-sm font-semibold">供应商成本</h2>
        {coverage.authority_conflicts !== "0" ? (
          <Badge variant="destructive">
            {coverage.authority_conflicts} 个成本权威冲突
          </Badge>
        ) : null}
      </div>
      {rows.length === 0 ? (
        <EmptyState
          title={
            coverage.terminal_receipts === "0"
              ? "暂无上游任务"
              : "尚未获得上游实际成本"
          }
          description="基准价格和估算价格不会被计入实际成本。"
        />
      ) : (
        <div className="overflow-hidden rounded-md border">
          <div className="overflow-x-auto">
            <Table className="min-w-[900px]">
              <TableHeader>
                <TableRow>
                  <TableHead className="pl-4">供应商</TableHead>
                  <TableHead>成本口径</TableHead>
                  <TableHead>归属</TableHead>
                  <TableHead>结果</TableHead>
                  <TableHead className="text-right">交易</TableHead>
                  <TableHead className="pr-4 text-right">金额</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((item, index) => (
                  <TableRow
                    key={`${item.provider_id}-${item.cost_basis}-${item.tenant_id ?? "none"}-${item.currency}-${index}`}
                  >
                    <TableCell className="pl-4 font-medium">
                      {providerLabel(item.provider_id)}
                    </TableCell>
                    <TableCell>
                      <CostBasisBadge basis={item.cost_basis} />
                    </TableCell>
                    <TableCell>
                      {item.attribution_state === "unattributed"
                        ? "待归属"
                        : scopeLabel(item.tenant_id, tenantNames)}
                    </TableCell>
                    <TableCell>{formatStatus(item.outcome)}</TableCell>
                    <TableCell className="text-right font-mono tabular-nums">
                      {formatInteger(item.transaction_count)}
                    </TableCell>
                    <TableCell className="pr-4 text-right font-mono tabular-nums">
                      {formatMoneyMicros(item.amount_micros, item.currency)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </div>
      )}
    </section>
  );
}

function BalanceTable({
  data,
  tenantNames,
}: {
  data: UsageSnapshot;
  tenantNames: Map<string, string>;
}) {
  if (data.account_snapshots.length === 0) return null;
  return (
    <section>
      <h2 className="mb-3 text-sm font-semibold">余额</h2>
      <div className="overflow-x-auto rounded-md border">
        <Table className="min-w-[720px]">
          <TableHeader>
            <TableRow>
              <TableHead className="pl-4">工作区</TableHead>
              <TableHead className="text-right">累计扣费</TableHead>
              <TableHead className="text-right">已退款</TableHead>
              <TableHead className="text-right">净支出</TableHead>
              <TableHead className="pr-4 text-right">可用</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.account_snapshots.map((item) => (
              <TableRow key={`${item.tenant_id}-${item.currency}`}>
                <TableCell className="pl-4">
                  {scopeLabel(item.tenant_id, tenantNames)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {formatMoneyMicros(item.captured_micros, item.currency)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {formatMoneyMicros(item.refunded_micros, item.currency)}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {formatMoneyMicros(
                    (
                      parseInteger(item.captured_micros) -
                      parseInteger(item.refunded_micros)
                    ).toString(),
                    item.currency,
                  )}
                </TableCell>
                <TableCell className="pr-4 text-right font-mono tabular-nums">
                  {formatMoneyMicros(item.available_micros, item.currency)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </section>
  );
}

function SummaryMetric({
  label,
  value,
  detail,
  last = false,
}: {
  label: string;
  value: string;
  detail: string;
  last?: boolean;
}) {
  return (
    <div
      className={[
        "min-w-0 border-b p-5 sm:[&:nth-child(odd)]:border-r xl:border-b-0 xl:border-r",
        last ? "xl:border-r-0" : "",
      ].join(" ")}
    >
      <p className="text-sm text-muted-foreground">{label}</p>
      <p className="mt-2 break-words text-2xl font-semibold tabular-nums">{value}</p>
      <p className="mt-2 break-words text-xs leading-5 text-muted-foreground">
        {detail}
      </p>
    </div>
  );
}

function CostBasisBadge({ basis }: { basis: ProviderCostAggregate["cost_basis"] }) {
  if (basis === "provider_actual") return <Badge>上游实际</Badge>;
  if (basis === "provider_allocated") {
    return <Badge variant="secondary">订阅/积分分摊</Badge>;
  }
  return <Badge variant="outline">旧链路未核验</Badge>;
}

function EmptyState({ title, description }: { title: string; description: string }) {
  return (
    <div className="flex min-h-44 flex-col items-center justify-center rounded-md border px-6 text-center">
      <h3 className="text-sm font-medium">{title}</h3>
      <p className="mt-1 max-w-md text-sm text-muted-foreground">{description}</p>
    </div>
  );
}

function isPlatformSnapshot(data: UsageSnapshot): data is BillingSnapshot {
  return "provider_costs" in data && "provider_cost_coverage" in data;
}

function sumUsage(data: UsageSnapshot, metrics: string[]) {
  return data.charged_usage
    .filter((item) => metrics.includes(item.billing_metric))
    .reduce((total, item) => total + parseInteger(item.quantity), 0n)
    .toString();
}

function moneyByCurrency(
  rows: LedgerAggregate[],
  include: (item: LedgerAggregate) => boolean,
) {
  const totals = new Map<string, bigint>();
  for (const item of rows) {
    if (!include(item)) continue;
    totals.set(
      item.currency,
      (totals.get(item.currency) ?? 0n) + parseInteger(item.amount_micros),
    );
  }
  return totals;
}

function subtractCurrencyTotals(
  gross: Map<string, bigint>,
  deductions: Map<string, bigint>,
) {
  const currencies = new Set([...gross.keys(), ...deductions.keys()]);
  const totals = new Map<string, bigint>();
  for (const currency of currencies) {
    totals.set(
      currency,
      (gross.get(currency) ?? 0n) - (deductions.get(currency) ?? 0n),
    );
  }
  return totals;
}

function netRevenueSummary(
  net: Map<string, bigint>,
  gross: Map<string, bigint>,
  refunds: Map<string, bigint>,
) {
  const summary = moneySummary(net, { emptyCurrency: "USD" });
  if (gross.size === 1 && refunds.size <= 1) {
    const [currency, grossAmount] = [...gross.entries()][0];
    const refundedAmount = refunds.get(currency) ?? 0n;
    return {
      ...summary,
      detail: `累计扣费 ${formatMoneyMicros(
        grossAmount.toString(),
        currency,
      )} · 退款 ${formatMoneyMicros(refundedAmount.toString(), currency)}`,
    };
  }
  return { ...summary, detail: "累计扣费减客户退款" };
}

function costByCurrency(rows: ProviderCostAggregate[]) {
  const totals = new Map<string, bigint>();
  for (const item of rows) {
    totals.set(
      item.currency,
      (totals.get(item.currency) ?? 0n) + parseInteger(item.amount_micros),
    );
  }
  return totals;
}

function moneySummary(
  totals: Map<string, bigint>,
  { emptyCurrency }: { emptyCurrency: string },
) {
  if (totals.size === 0) {
    return {
      value: formatMoneyMicros("0", emptyCurrency),
      detail: "已结算金额",
    };
  }
  if (totals.size === 1) {
    const [currency, amount] = [...totals.entries()][0];
    return {
      value: formatMoneyMicros(amount.toString(), currency),
      detail: "已结算金额",
    };
  }
  return {
    value: `${totals.size} 种币种`,
    detail: formatCurrencyTotals(totals),
  };
}

function providerCostSummary(
  totals: Map<string, bigint>,
  coverage: BillingSnapshot["provider_cost_coverage"] | null,
) {
  if (!coverage || coverage.terminal_receipts === "0") {
    return { value: formatMoneyMicros("0", "USD"), detail: "暂无上游任务" };
  }
  if (totals.size === 0) {
    return { value: "待核验", detail: `${coverage.uncovered_receipts} 条结果未覆盖` };
  }
  return {
    ...moneySummary(totals, { emptyCurrency: "USD" }),
    detail:
      coverage.uncovered_receipts === "0"
        ? "实际回执与已关闭分摊"
        : `${coverage.uncovered_receipts} 条结果未覆盖`,
  };
}

function marginSummary(
  revenue: Map<string, bigint>,
  costs: Map<string, bigint>,
  available: boolean,
) {
  if (!available) {
    return { value: "暂不可用", detail: "成本覆盖完整后计算" };
  }
  const currencies = new Set([...revenue.keys(), ...costs.keys()]);
  const margins = new Map<string, bigint>();
  for (const currency of currencies) {
    margins.set(
      currency,
      (revenue.get(currency) ?? 0n) - (costs.get(currency) ?? 0n),
    );
  }
  return {
    ...moneySummary(margins, { emptyCurrency: "USD" }),
    detail: "客户收入减权威供应成本",
  };
}

function coverageSummary(
  coverage: BillingSnapshot["provider_cost_coverage"] | null,
) {
  if (!coverage || coverage.terminal_receipts === "0") {
    return { value: "--", detail: "暂无上游任务" };
  }
  const total = parseInteger(coverage.terminal_receipts);
  const covered = parseInteger(coverage.covered_receipts);
  const basisPoints = total === 0n ? 0n : (covered * 10_000n) / total;
  const value = `${Number(basisPoints) / 100}%`;
  const detail =
    coverage.authority_conflicts !== "0"
      ? `${coverage.authority_conflicts} 个成本权威冲突`
      : `${coverage.covered_receipts} / ${coverage.terminal_receipts} 条结果`;
  return { value, detail };
}

function formatCurrencyTotals(totals: Map<string, bigint>) {
  return [...totals.entries()]
    .slice(0, 2)
    .map(([currency, amount]) => formatMoneyMicros(amount.toString(), currency))
    .join(" · ");
}

function parseInteger(value: string) {
  try {
    return BigInt(value);
  } catch {
    return 0n;
  }
}

function scopeLabel(tenantId: string | undefined, tenantNames: Map<string, string>) {
  if (!tenantId) return "未归属";
  return tenantNames.get(tenantId) ?? tenantId;
}

function providerLabel(providerId: string) {
  const labels: Record<string, string> = {
    "openai-codex": "Codex",
    "xai-grok": "Grok",
    dreamina: "即梦",
    "volcengine-ark": "火山方舟",
  };
  return labels[providerId] ?? providerId;
}
