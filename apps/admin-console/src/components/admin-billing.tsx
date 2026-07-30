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
import { useI18n } from "@/i18n/locale-provider";
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
type Translate = ReturnType<typeof useI18n>["t"];
type BillingSection =
  | "usage"
  | "credit_grants"
  | "refunds"
  | "provider_costs"
  | "integrity";

export function AdminBilling() {
  const { t } = useI18n();
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
        title={t({
          en: "Usage & billing",
          "zh-CN": "用量与计费",
          ja: "使用量と請求",
          ko: "사용량 및 결제",
        })}
        description={
          activeWorkspace?.name ??
          t({
            en: "Current workspace",
            "zh-CN": "当前工作区",
            ja: "現在のワークスペース",
            ko: "현재 워크스페이스",
          })
        }
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
                {t({
                  en: "Organization limits",
                  "zh-CN": "组织限额",
                  ja: "組織の上限",
                  ko: "조직 한도",
                })}
              </Button>
            ) : null}
            {budgetProject ? (
              <Button
                type="button"
                variant="outline"
                onClick={() => setBudgetOpen(true)}
              >
                <Gauge aria-hidden="true" />
                {t({
                  en: "Project budget",
                  "zh-CN": "项目预算",
                  ja: "プロジェクト予算",
                  ko: "프로젝트 예산",
                })}
              </Button>
            ) : (
              <Button asChild type="button" variant="outline">
                <Link href="/projects">
                  <Gauge aria-hidden="true" />
                  {t({
                    en: "Project budget",
                    "zh-CN": "项目预算",
                    ja: "プロジェクト予算",
                    ko: "프로젝트 예산",
                  })}
                </Link>
              </Button>
            )}
            <Tabs value={window} onValueChange={(value) => setWindow(value as UsageWindow)}>
              <TabsList className="h-9">
                <TabsTrigger value="24h">
                  {t({
                    en: "24 hours",
                    "zh-CN": "24 小时",
                    ja: "24 時間",
                    ko: "24시간",
                  })}
                </TabsTrigger>
                <TabsTrigger value="7d">
                  {t({
                    en: "7 days",
                    "zh-CN": "7 天",
                    ja: "7 日間",
                    ko: "7일",
                  })}
                </TabsTrigger>
                <TabsTrigger value="30d">
                  {t({
                    en: "30 days",
                    "zh-CN": "30 天",
                    ja: "30 日間",
                    ko: "30일",
                  })}
                </TabsTrigger>
              </TabsList>
            </Tabs>
            <Button
              type="button"
              variant="outline"
              size="icon"
              aria-label={t({
                en: "Refresh usage",
                "zh-CN": "刷新用量",
                ja: "使用量を更新",
                ko: "사용량 새로고침",
              })}
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
            <TabsTrigger value="usage">
              {t({
                en: "Usage overview",
                "zh-CN": "用量概览",
                ja: "使用量の概要",
                ko: "사용량 개요",
              })}
            </TabsTrigger>
            {canViewCreditGrants ? (
              <TabsTrigger value="credit_grants">
                {t({
                  en: "Credit Grants",
                  "zh-CN": "额度发放",
                  ja: "クレジット付与",
                  ko: "크레딧 지급",
                })}
              </TabsTrigger>
            ) : null}
            {canInspectBilling ? (
              <>
                <TabsTrigger value="refunds">
                  {t({
                    en: "Refunds & reversals",
                    "zh-CN": "退款与冲正",
                    ja: "返金と取消",
                    ko: "환불 및 취소",
                  })}
                </TabsTrigger>
                <TabsTrigger value="provider_costs">
                  {t({
                    en: "Provider costs",
                    "zh-CN": "上游成本",
                    ja: "プロバイダーコスト",
                    ko: "공급자 비용",
                  })}
                </TabsTrigger>
                <TabsTrigger value="integrity">
                  {t({
                    en: "Billing integrity",
                    "zh-CN": "账务检查",
                    ja: "請求整合性",
                    ko: "결제 무결성",
                  })}
                </TabsTrigger>
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
              t={t}
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
  t,
  data,
  platformOwner,
  tenantNames,
  stale,
  window,
  projectId,
  analysisRefreshKey,
  analysisEnabled,
}: {
  t: Translate;
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
          {t({
            en: "Showing the most recent successful snapshot",
            "zh-CN": "当前显示上一次成功快照",
            ja: "直近の成功したスナップショットを表示しています",
            ko: "마지막으로 성공한 스냅샷을 표시합니다",
          })}
        </div>
      ) : null}

      <section className="grid overflow-hidden rounded-md border sm:grid-cols-2 xl:grid-cols-4">
        {platformOwner ? (
          <>
            <SummaryMetric
              label={t({
                en: "Net customer revenue",
                "zh-CN": "客户净收入",
                ja: "顧客純収益",
                ko: "고객 순수익",
              })}
              {...netRevenueSummary(t, revenue, grossRevenue, refunds)}
            />
            <SummaryMetric
              label={t({
                en: "Provider costs",
                "zh-CN": "供应商成本",
                ja: "プロバイダーコスト",
                ko: "공급자 비용",
              })}
              {...providerCostSummary(t, authoritativeCosts, coverage)}
            />
            <SummaryMetric
              label={t({
                en: "Gross margin",
                "zh-CN": "毛利",
                ja: "粗利益",
                ko: "매출총이익",
              })}
              {...marginSummary(t, revenue, authoritativeCosts, marginAvailable)}
            />
            <SummaryMetric
              label={t({
                en: "Actual cost coverage",
                "zh-CN": "实际成本覆盖",
                ja: "実コストカバレッジ",
                ko: "실제 비용 적용 범위",
              })}
              {...coverageSummary(t, coverage)}
              last
            />
          </>
        ) : (
          <>
            <SummaryMetric
              label={t({
                en: "Net spend",
                "zh-CN": "本期净支出",
                ja: "期間純支出",
                ko: "기간 순지출",
              })}
              {...netRevenueSummary(t, revenue, grossRevenue, refunds)}
            />
            <SummaryMetric
              label={t({
                en: "Requests",
                "zh-CN": "请求",
                ja: "リクエスト",
                ko: "요청",
              })}
              value={formatInteger(requests)}
              detail={t({
                en: "Calls included in metering",
                "zh-CN": "已进入计量的调用",
                ja: "計測対象となった呼び出し",
                ko: "측정에 포함된 호출",
              })}
            />
            <SummaryMetric
              label={t({
                en: "Image outputs",
                "zh-CN": "图片输出",
                ja: "画像出力",
                ko: "이미지 출력",
              })}
              value={formatInteger(outputs)}
              detail={t({
                en: "Successfully metered images",
                "zh-CN": "成功计量的图片",
                ja: "正常に計測された画像",
                ko: "성공적으로 측정된 이미지",
              })}
            />
            <SummaryMetric
              label={t({
                en: "Actual video output duration",
                "zh-CN": "实际输出视频时长",
                ja: "実動画出力時間",
                ko: "실제 동영상 출력 시간",
              })}
              value={t(
                {
                  en: "{seconds} sec",
                  "zh-CN": "{seconds} 秒",
                  ja: "{seconds} 秒",
                  ko: "{seconds}초",
                },
                { seconds: formatInteger(videoSeconds) },
              )}
              detail={t({
                en: "Successfully generated and metered",
                "zh-CN": "成功生成并完成计量",
                ja: "正常に生成・計測済み",
                ko: "성공적으로 생성 및 측정됨",
              })}
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
          t={t}
          rows={platformData.provider_costs}
          coverage={platformData.provider_cost_coverage}
          tenantNames={tenantNames}
        />
      ) : (
        <BalanceTable t={t} data={data} tenantNames={tenantNames} />
      )}

      <p className="text-right text-xs text-muted-foreground">
        {t(
          {
            en: "Updated {time}",
            "zh-CN": "更新于 {time}",
            ja: "{time} に更新",
            ko: "{time} 업데이트",
          },
          { time: formatDateTime(data.as_of_ms) },
        )}
      </p>
    </>
  );
}

function ProviderCostTable({
  t,
  rows,
  coverage,
  tenantNames,
}: {
  t: Translate;
  rows: ProviderCostAggregate[];
  coverage: BillingSnapshot["provider_cost_coverage"];
  tenantNames: Map<string, string>;
}) {
  return (
    <section>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-sm font-semibold">
          {t({
            en: "Provider costs",
            "zh-CN": "供应商成本",
            ja: "プロバイダーコスト",
            ko: "공급자 비용",
          })}
        </h2>
        {coverage.authority_conflicts !== "0" ? (
          <Badge variant="destructive">
            {t(
              {
                en: "{count} cost authority conflicts",
                "zh-CN": "{count} 个成本权威冲突",
                ja: "コスト権限の競合 {count} 件",
                ko: "비용 권한 충돌 {count}건",
              },
              { count: coverage.authority_conflicts },
            )}
          </Badge>
        ) : null}
      </div>
      {rows.length === 0 ? (
        <EmptyState
          title={
            coverage.terminal_receipts === "0"
              ? t({
                  en: "No provider jobs yet",
                  "zh-CN": "暂无上游任务",
                  ja: "プロバイダージョブはまだありません",
                  ko: "공급자 작업이 아직 없습니다",
                })
              : t({
                  en: "Actual provider costs are not available yet",
                  "zh-CN": "尚未获得上游实际成本",
                  ja: "プロバイダーの実コストはまだ取得されていません",
                  ko: "실제 공급자 비용을 아직 받지 못했습니다",
                })
          }
          description={t({
            en: "Reference and estimated prices are not counted as actual costs.",
            "zh-CN": "基准价格和估算价格不会被计入实际成本。",
            ja: "基準価格と見積価格は実コストに含まれません。",
            ko: "기준 가격과 예상 가격은 실제 비용에 포함되지 않습니다.",
          })}
        />
      ) : (
        <div className="overflow-hidden rounded-md border">
          <div className="overflow-x-auto">
            <Table className="min-w-[900px]">
              <TableHeader>
                <TableRow>
                  <TableHead className="pl-4">
                    {t({
                      en: "Provider",
                      "zh-CN": "供应商",
                      ja: "プロバイダー",
                      ko: "공급자",
                    })}
                  </TableHead>
                  <TableHead>
                    {t({
                      en: "Cost basis",
                      "zh-CN": "成本口径",
                      ja: "コスト基準",
                      ko: "비용 기준",
                    })}
                  </TableHead>
                  <TableHead>
                    {t({
                      en: "Attribution",
                      "zh-CN": "归属",
                      ja: "帰属",
                      ko: "귀속",
                    })}
                  </TableHead>
                  <TableHead>
                    {t({
                      en: "Outcome",
                      "zh-CN": "结果",
                      ja: "結果",
                      ko: "결과",
                    })}
                  </TableHead>
                  <TableHead className="text-right">
                    {t({
                      en: "Transactions",
                      "zh-CN": "交易",
                      ja: "取引",
                      ko: "거래",
                    })}
                  </TableHead>
                  <TableHead className="pr-4 text-right">
                    {t({
                      en: "Amount",
                      "zh-CN": "金额",
                      ja: "金額",
                      ko: "금액",
                    })}
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((item, index) => (
                  <TableRow
                    key={`${item.provider_id}-${item.cost_basis}-${item.tenant_id ?? "none"}-${item.currency}-${index}`}
                  >
                    <TableCell className="pl-4 font-medium">
                      {providerLabel(t, item.provider_id)}
                    </TableCell>
                    <TableCell>
                      <CostBasisBadge t={t} basis={item.cost_basis} />
                    </TableCell>
                    <TableCell>
                      {item.attribution_state === "unattributed"
                        ? t({
                            en: "Unattributed",
                            "zh-CN": "待归属",
                            ja: "未帰属",
                            ko: "미귀속",
                          })
                        : scopeLabel(t, item.tenant_id, tenantNames)}
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
  t,
  data,
  tenantNames,
}: {
  t: Translate;
  data: UsageSnapshot;
  tenantNames: Map<string, string>;
}) {
  if (data.account_snapshots.length === 0) return null;
  return (
    <section>
      <h2 className="mb-3 text-sm font-semibold">
        {t({
          en: "Balance",
          "zh-CN": "余额",
          ja: "残高",
          ko: "잔액",
        })}
      </h2>
      <div className="overflow-x-auto rounded-md border">
        <Table className="min-w-[720px]">
          <TableHeader>
            <TableRow>
              <TableHead className="pl-4">
                {t({
                  en: "Workspace",
                  "zh-CN": "工作区",
                  ja: "ワークスペース",
                  ko: "워크스페이스",
                })}
              </TableHead>
              <TableHead className="text-right">
                {t({
                  en: "Total charges",
                  "zh-CN": "累计扣费",
                  ja: "累計請求",
                  ko: "누적 청구",
                })}
              </TableHead>
              <TableHead className="text-right">
                {t({
                  en: "Refunded",
                  "zh-CN": "已退款",
                  ja: "返金済み",
                  ko: "환불됨",
                })}
              </TableHead>
              <TableHead className="text-right">
                {t({
                  en: "Net spend",
                  "zh-CN": "净支出",
                  ja: "純支出",
                  ko: "순지출",
                })}
              </TableHead>
              <TableHead className="pr-4 text-right">
                {t({
                  en: "Available",
                  "zh-CN": "可用",
                  ja: "利用可能",
                  ko: "사용 가능",
                })}
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.account_snapshots.map((item) => (
              <TableRow key={`${item.tenant_id}-${item.currency}`}>
                <TableCell className="pl-4">
                  {scopeLabel(t, item.tenant_id, tenantNames)}
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

function CostBasisBadge({
  t,
  basis,
}: {
  t: Translate;
  basis: ProviderCostAggregate["cost_basis"];
}) {
  if (basis === "provider_actual")
    return (
      <Badge>
        {t({
          en: "Provider actual",
          "zh-CN": "上游实际",
          ja: "プロバイダー実績",
          ko: "공급자 실제 비용",
        })}
      </Badge>
    );
  if (basis === "provider_allocated") {
    return (
      <Badge variant="secondary">
        {t({
          en: "Subscription/credit allocation",
          "zh-CN": "订阅/积分分摊",
          ja: "サブスクリプション/クレジット配分",
          ko: "구독/크레딧 배분",
        })}
      </Badge>
    );
  }
  return (
    <Badge variant="outline">
      {t({
        en: "Legacy path unverified",
        "zh-CN": "旧链路未核验",
        ja: "旧経路未検証",
        ko: "레거시 경로 미검증",
      })}
    </Badge>
  );
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
  t: Translate,
  net: Map<string, bigint>,
  gross: Map<string, bigint>,
  refunds: Map<string, bigint>,
) {
  const summary = moneySummary(t, net, { emptyCurrency: "USD" });
  if (gross.size === 1 && refunds.size <= 1) {
    const [currency, grossAmount] = [...gross.entries()][0];
    const refundedAmount = refunds.get(currency) ?? 0n;
    return {
      ...summary,
      detail: t(
        {
          en: "Total charges {gross} · Refunds {refunds}",
          "zh-CN": "累计扣费 {gross} · 退款 {refunds}",
          ja: "累計請求 {gross} · 返金 {refunds}",
          ko: "누적 청구 {gross} · 환불 {refunds}",
        },
        {
          gross: formatMoneyMicros(grossAmount.toString(), currency),
          refunds: formatMoneyMicros(refundedAmount.toString(), currency),
        },
      ),
    };
  }
  return {
    ...summary,
    detail: t({
      en: "Total charges minus customer refunds",
      "zh-CN": "累计扣费减客户退款",
      ja: "累計請求から顧客返金を控除",
      ko: "누적 청구에서 고객 환불 차감",
    }),
  };
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
  t: Translate,
  totals: Map<string, bigint>,
  { emptyCurrency }: { emptyCurrency: string },
) {
  if (totals.size === 0) {
    return {
      value: formatMoneyMicros("0", emptyCurrency),
      detail: t({
        en: "Settled amount",
        "zh-CN": "已结算金额",
        ja: "精算済み金額",
        ko: "정산 금액",
      }),
    };
  }
  if (totals.size === 1) {
    const [currency, amount] = [...totals.entries()][0];
    return {
      value: formatMoneyMicros(amount.toString(), currency),
      detail: t({
        en: "Settled amount",
        "zh-CN": "已结算金额",
        ja: "精算済み金額",
        ko: "정산 금액",
      }),
    };
  }
  return {
    value: t(
      {
        en: "{count} currencies",
        "zh-CN": "{count} 种币种",
        ja: "{count} 通貨",
        ko: "{count}개 통화",
      },
      { count: totals.size },
    ),
    detail: formatCurrencyTotals(totals),
  };
}

function providerCostSummary(
  t: Translate,
  totals: Map<string, bigint>,
  coverage: BillingSnapshot["provider_cost_coverage"] | null,
) {
  if (!coverage || coverage.terminal_receipts === "0") {
    return {
      value: formatMoneyMicros("0", "USD"),
      detail: t({
        en: "No provider jobs yet",
        "zh-CN": "暂无上游任务",
        ja: "プロバイダージョブはまだありません",
        ko: "공급자 작업이 아직 없습니다",
      }),
    };
  }
  if (totals.size === 0) {
    return {
      value: t({
        en: "Pending verification",
        "zh-CN": "待核验",
        ja: "検証待ち",
        ko: "검증 대기",
      }),
      detail: uncoveredResults(t, coverage.uncovered_receipts),
    };
  }
  return {
    ...moneySummary(t, totals, { emptyCurrency: "USD" }),
    detail:
      coverage.uncovered_receipts === "0"
        ? t({
            en: "Actual receipts and closed allocations",
            "zh-CN": "实际回执与已关闭分摊",
            ja: "実績レシートと確定済み配分",
            ko: "실제 영수증 및 마감된 배분",
          })
        : uncoveredResults(t, coverage.uncovered_receipts),
  };
}

function marginSummary(
  t: Translate,
  revenue: Map<string, bigint>,
  costs: Map<string, bigint>,
  available: boolean,
) {
  if (!available) {
    return {
      value: t({
        en: "Unavailable",
        "zh-CN": "暂不可用",
        ja: "利用不可",
        ko: "사용 불가",
      }),
      detail: t({
        en: "Calculated after cost coverage is complete",
        "zh-CN": "成本覆盖完整后计算",
        ja: "コストカバレッジ完了後に計算されます",
        ko: "비용 적용 범위가 완료된 후 계산됩니다",
      }),
    };
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
    ...moneySummary(t, margins, { emptyCurrency: "USD" }),
    detail: t({
      en: "Customer revenue minus authoritative provider costs",
      "zh-CN": "客户收入减权威供应成本",
      ja: "顧客収益から確定プロバイダーコストを控除",
      ko: "고객 수익에서 확정 공급자 비용 차감",
    }),
  };
}

function coverageSummary(
  t: Translate,
  coverage: BillingSnapshot["provider_cost_coverage"] | null,
) {
  if (!coverage || coverage.terminal_receipts === "0") {
    return {
      value: "--",
      detail: t({
        en: "No provider jobs yet",
        "zh-CN": "暂无上游任务",
        ja: "プロバイダージョブはまだありません",
        ko: "공급자 작업이 아직 없습니다",
      }),
    };
  }
  const total = parseInteger(coverage.terminal_receipts);
  const covered = parseInteger(coverage.covered_receipts);
  const basisPoints = total === 0n ? 0n : (covered * 10_000n) / total;
  const value = `${Number(basisPoints) / 100}%`;
  const detail =
    coverage.authority_conflicts !== "0"
      ? t(
          {
            en: "{count} cost authority conflicts",
            "zh-CN": "{count} 个成本权威冲突",
            ja: "コスト権限の競合 {count} 件",
            ko: "비용 권한 충돌 {count}건",
          },
          { count: coverage.authority_conflicts },
        )
      : t(
          {
            en: "{covered} / {total} results",
            "zh-CN": "{covered} / {total} 条结果",
            ja: "{covered} / {total} 件の結果",
            ko: "결과 {covered} / {total}건",
          },
          {
            covered: coverage.covered_receipts,
            total: coverage.terminal_receipts,
          },
        );
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

function uncoveredResults(t: Translate, count: string) {
  return t(
    {
      en: "{count} results are not covered",
      "zh-CN": "{count} 条结果未覆盖",
      ja: "{count} 件の結果が未カバーです",
      ko: "결과 {count}건이 적용되지 않음",
    },
    { count },
  );
}

function scopeLabel(
  t: Translate,
  tenantId: string | undefined,
  tenantNames: Map<string, string>,
) {
  if (!tenantId)
    return t({
      en: "Unattributed",
      "zh-CN": "未归属",
      ja: "未帰属",
      ko: "미귀속",
    });
  return tenantNames.get(tenantId) ?? tenantId;
}

function providerLabel(t: Translate, providerId: string) {
  const labels: Record<string, string> = {
    "openai-codex": "Codex",
    "xai-grok": "Grok",
    dreamina: t({
      en: "Dreamina",
      "zh-CN": "即梦",
      ja: "Dreamina",
      ko: "Dreamina",
    }),
    "volcengine-ark": t({
      en: "Volcengine Ark",
      "zh-CN": "火山方舟",
      ja: "Volcengine Ark",
      ko: "Volcengine Ark",
    }),
  };
  return labels[providerId] ?? providerId;
}
