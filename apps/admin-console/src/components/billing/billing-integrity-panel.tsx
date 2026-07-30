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
import { useI18n } from "@/i18n/locale-provider";
import { formatDateTime, formatMoneyMicros } from "@/lib/admin/format";
import type {
  BillingIntegrityFinding,
  BillingIntegrityRun,
  BillingIntegrityRunDetail,
  BillingIntegrityRunList,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

type Translate = ReturnType<typeof useI18n>["t"];

export function BillingIntegrityPanel({ enabled }: { enabled: boolean }) {
  const { locale, t } = useI18n();
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
      if (!response.ok) throw new Error(await responseMessage(response, t));
      const result = (await response.json()) as BillingIntegrityRunDetail;
      query.retry();
      setSelectedRunId(result.run_id);
      if (result.finding_count === 0) {
        toast.success(
          t({
            en: "Billing check completed with no findings",
            "zh-CN": "账务检查完成，未发现异常",
            ja: "請求整合性チェックが完了し、異常は見つかりませんでした",
            ko: "결제 무결성 검사가 완료되었으며 이상이 없습니다",
          }),
        );
      } else {
        toast.warning(
          t(
            {
              en: "Billing check completed with {count} findings that need attention",
              "zh-CN": "账务检查完成，发现 {count} 项需要处理",
              ja: "請求整合性チェックが完了し、対応が必要な項目が {count} 件見つかりました",
              ko: "결제 무결성 검사가 완료되었으며 처리해야 할 항목 {count}개를 발견했습니다",
            },
            { count: result.finding_count },
          ),
        );
      }
    } catch (caught) {
      toast.error(
        caught instanceof Error
          ? caught.message
          : t({
              en: "Billing check failed",
              "zh-CN": "账务检查失败",
              ja: "請求整合性チェックに失敗しました",
              ko: "결제 무결성 검사 실패",
            }),
      );
    } finally {
      setRunning(false);
    }
  }

  const latest = query.data?.data[0] ?? null;

  return (
    <section className="space-y-4">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-base font-semibold">
            {t({
              en: "Billing integrity",
              "zh-CN": "账务检查",
              ja: "請求整合性",
              ko: "결제 무결성",
            })}
          </h2>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            {t({
              en: "Reconcile balances, holds, customer charges, and project attribution. Checks only read and preserve evidence; they never alter billing records automatically.",
              "zh-CN": "核对余额、冻结、客户收费与项目归属。检查只读取并留存证据，不会自动修改账务。",
              ja: "残高、保留、顧客請求、プロジェクト帰属を照合します。チェックは証拠を読み取り保存するだけで、請求記録を自動変更しません。",
              ko: "잔액, 보류, 고객 청구 및 프로젝트 귀속을 대조합니다. 검사는 증거를 읽고 보존할 뿐 결제 기록을 자동으로 변경하지 않습니다.",
            })}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            type="button"
            variant="outline"
            size="icon"
            aria-label={t({
              en: "Refresh billing checks",
              "zh-CN": "刷新检查记录",
              ja: "請求チェックを更新",
              ko: "결제 검사 새로 고침",
            })}
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
            {t({
              en: "Run check",
              "zh-CN": "运行检查",
              ja: "チェックを実行",
              ko: "검사 실행",
            })}
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
          <h3 className="mt-4 text-sm font-medium">
            {t({
              en: "No billing checks have been run",
              "zh-CN": "尚未运行账务检查",
              ja: "請求整合性チェックはまだ実行されていません",
              ko: "아직 결제 무결성 검사가 실행되지 않았습니다",
            })}
          </h3>
          <p className="mt-1 max-w-md text-sm text-muted-foreground">
            {t({
              en: "Run a check to view each consistency snapshot and complete evidence for every finding.",
              "zh-CN": "运行后可查看每次一致性快照和完整异常证据。",
              ja: "実行すると、各整合性スナップショットとすべての異常の完全な証拠を確認できます。",
              ko: "검사를 실행하면 각 일관성 스냅샷과 모든 이상 항목의 전체 증거를 볼 수 있습니다.",
            })}
          </p>
        </div>
      ) : null}
      {query.data && query.data.data.length > 0 ? (
        <div className="overflow-hidden rounded-md border">
          <div className="overflow-x-auto">
            <Table className="min-w-[760px]">
              <TableHeader>
                <TableRow>
                  <TableHead className="pl-4">
                    {t({
                      en: "Completed",
                      "zh-CN": "完成时间",
                      ja: "完了日時",
                      ko: "완료 시간",
                    })}
                  </TableHead>
                  <TableHead>
                    {t({ en: "Result", "zh-CN": "结果", ja: "結果", ko: "결과" })}
                  </TableHead>
                  <TableHead>
                    {t({
                      en: "Critical",
                      "zh-CN": "严重",
                      ja: "重大",
                      ko: "심각",
                    })}
                  </TableHead>
                  <TableHead>
                    {t({
                      en: "Warnings",
                      "zh-CN": "提醒",
                      ja: "警告",
                      ko: "경고",
                    })}
                  </TableHead>
                  <TableHead>
                    {t({
                      en: "Checks",
                      "zh-CN": "检查项",
                      ja: "チェック項目",
                      ko: "검사 항목",
                    })}
                  </TableHead>
                  <TableHead className="w-12 pr-4">
                    <span className="sr-only">
                      {t({ en: "View", "zh-CN": "查看", ja: "表示", ko: "보기" })}
                    </span>
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
                      {formatDateTime(run.completed_at_ms, locale)}
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
                      {t(
                        {
                          en: "{count} checks",
                          "zh-CN": "{count} 项",
                          ja: "{count} 項目",
                          ko: "{count}개",
                        },
                        { count: run.check_set.length },
                      )}
                    </TableCell>
                    <TableCell className="pr-4 text-right">
                      <Button
                        type="button"
                        variant="ghost"
                        size="icon"
                        aria-label={t(
                          {
                            en: "View check results from {time}",
                            "zh-CN": "查看 {time} 的检查结果",
                            ja: "{time} のチェック結果を表示",
                            ko: "{time} 검사 결과 보기",
                          },
                          { time: formatDateTime(run.completed_at_ms, locale) },
                        )}
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
  const { locale, t } = useI18n();
  const clean = run.finding_count === 0;
  return (
    <div className="flex flex-wrap items-center gap-x-6 gap-y-2 rounded-md border px-4 py-3 text-sm">
      <div className="flex items-center gap-2 font-medium">
        {clean ? (
          <CheckCircle2 className="size-4" aria-hidden="true" />
        ) : (
          <AlertTriangle className="size-4" aria-hidden="true" />
        )}
        {t({
          en: "Latest:",
          "zh-CN": "最近一次：",
          ja: "最新:",
          ko: "최근:",
        })}{" "}
        {clean
          ? t({
              en: "No findings",
              "zh-CN": "未发现异常",
              ja: "異常なし",
              ko: "이상 없음",
            })
          : t(
              {
                en: "{count} findings need attention",
                "zh-CN": "{count} 项需要处理",
                ja: "{count} 件の対応が必要",
                ko: "{count}개 항목 처리 필요",
              },
              { count: run.finding_count },
            )}
      </div>
      <span className="text-muted-foreground">
        {t(
          {
            en: "Critical {critical} · Warnings {warnings}",
            "zh-CN": "严重 {critical} · 提醒 {warnings}",
            ja: "重大 {critical} · 警告 {warnings}",
            ko: "심각 {critical} · 경고 {warnings}",
          },
          {
            critical: run.critical_count,
            warnings: run.warning_count,
          },
        )}
      </span>
      <span className="ml-auto text-muted-foreground">
        {formatDateTime(run.completed_at_ms, locale)}
      </span>
    </div>
  );
}

function RunStatus({ run }: { run: BillingIntegrityRun }) {
  const { t } = useI18n();
  if (run.critical_count > 0) {
    return (
      <Badge variant="destructive">
        {t({
          en: "Needs attention",
          "zh-CN": "需要处理",
          ja: "対応が必要",
          ko: "처리 필요",
        })}
      </Badge>
    );
  }
  if (run.warning_count > 0) {
    return (
      <Badge variant="secondary">
        {t({
          en: "Review needed",
          "zh-CN": "需要关注",
          ja: "確認が必要",
          ko: "검토 필요",
        })}
      </Badge>
    );
  }
  return (
    <Badge variant="outline">
      {t({ en: "Healthy", "zh-CN": "正常", ja: "正常", ko: "정상" })}
    </Badge>
  );
}

function BillingIntegrityRunSheet({
  runId,
  onOpenChange,
}: {
  runId: string | null;
  onOpenChange: (open: boolean) => void;
}) {
  const { locale, t } = useI18n();
  const query = useAdminQuery<BillingIntegrityRunDetail>(
    runId ? `/admin/v1/billing/integrity-runs/${encodeURIComponent(runId)}` : "",
    Boolean(runId),
  );

  return (
    <Sheet open={Boolean(runId)} onOpenChange={onOpenChange}>
      <SheetContent className="flex h-full w-full min-w-0 flex-col gap-0 overflow-hidden p-0 sm:max-w-3xl">
        <SheetHeader className="shrink-0 border-b px-5 py-5 pr-12 text-left sm:px-6">
          <SheetTitle>
            {t({
              en: "Billing check results",
              "zh-CN": "账务检查结果",
              ja: "請求整合性チェック結果",
              ko: "결제 무결성 검사 결과",
            })}
          </SheetTitle>
          <SheetDescription>
            {query.data
              ? `${formatDateTime(query.data.completed_at_ms, locale)} · ${t({
                  en: "Consistency snapshot",
                  "zh-CN": "一致性快照",
                  ja: "整合性スナップショット",
                  ko: "일관성 스냅샷",
                })}`
              : t({
                  en: "Loading the billing check",
                  "zh-CN": "正在读取检查记录",
                  ja: "請求チェックを読み込み中",
                  ko: "결제 검사 불러오는 중",
                })}
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
  const { t } = useI18n();
  return (
    <>
      <div className="grid grid-cols-3 border-b">
        <DetailMetric
          label={t({
            en: "Critical",
            "zh-CN": "严重",
            ja: "重大",
            ko: "심각",
          })}
          value={detail.critical_count.toString()}
        />
        <DetailMetric
          label={t({
            en: "Warnings",
            "zh-CN": "提醒",
            ja: "警告",
            ko: "경고",
          })}
          value={detail.warning_count.toString()}
        />
        <DetailMetric
          label={t({
            en: "Checks",
            "zh-CN": "检查项",
            ja: "チェック項目",
            ko: "검사 항목",
          })}
          value={detail.check_set.length.toString()}
          last
        />
      </div>
      <div className="space-y-5 p-5 sm:p-6">
        <div className="flex items-start gap-3 rounded-md bg-muted/50 px-4 py-3 text-sm">
          <ShieldCheck className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
          <p className="text-muted-foreground">
            {t({
              en: "Results come from one database snapshot. The check does not release holds, create transactions, or alter history.",
              "zh-CN": "结果来自同一数据库快照。检查不会释放冻结、补记交易或修改历史记录。",
              ja: "結果は同一のデータベーススナップショットに基づきます。チェックは保留の解除、取引の追加、履歴の変更を行いません。",
              ko: "결과는 동일한 데이터베이스 스냅샷에서 생성됩니다. 검사는 보류를 해제하거나 거래를 추가하거나 기록을 변경하지 않습니다.",
            })}
          </p>
        </div>
        {detail.findings.length === 0 ? (
          <div className="flex min-h-56 flex-col items-center justify-center text-center">
            <CheckCircle2 className="size-8 text-muted-foreground" aria-hidden="true" />
            <h3 className="mt-4 text-sm font-medium">
              {t({
                en: "No billing inconsistencies found",
                "zh-CN": "未发现账务异常",
                ja: "請求の不整合は見つかりませんでした",
                ko: "결제 불일치가 발견되지 않았습니다",
              })}
            </h3>
            <p className="mt-1 text-sm text-muted-foreground">
              {t({
                en: "Balances, holds, charges, and attribution data covered by this check are consistent.",
                "zh-CN": "本次检查覆盖的余额、冻结、收费与归属数据保持一致。",
                ja: "このチェックの対象となった残高、保留、請求、帰属データは整合しています。",
                ko: "이번 검사에서 확인한 잔액, 보류, 청구 및 귀속 데이터가 일치합니다.",
              })}
            </p>
          </div>
        ) : (
          <div className="space-y-3">
            <h3 className="text-sm font-semibold">
              {t({
                en: "Needs attention",
                "zh-CN": "需要处理",
                ja: "対応が必要",
                ko: "처리 필요",
              })}
            </h3>
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
  const { t } = useI18n();
  return (
    <article className="rounded-md border p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <Badge
              variant={finding.severity === "critical" ? "destructive" : "secondary"}
            >
              {finding.severity === "critical"
                ? t({
                    en: "Critical",
                    "zh-CN": "严重",
                    ja: "重大",
                    ko: "심각",
                  })
                : t({
                    en: "Warning",
                    "zh-CN": "提醒",
                    ja: "警告",
                    ko: "경고",
                  })}
            </Badge>
            <span className="text-sm font-medium">
              {findingTitle(t, finding.finding_code)}
            </span>
          </div>
          <p className="mt-2 break-all text-xs text-muted-foreground">
            {categoryLabel(t, finding.category)} · {finding.resource_id}
          </p>
        </div>
        {finding.currency ? (
          <span className="text-xs text-muted-foreground">{finding.currency}</span>
        ) : null}
      </div>
      <p className="mt-3 text-sm text-muted-foreground">
        {findingDescription(t, finding)}
      </p>
    </article>
  );
}

function findingTitle(t: Translate, code: string) {
  const labels: Record<
    string,
    { en: string; "zh-CN": string; ja: string; ko: string }
  > = {
    billing_account_counter_mismatch: {
      en: "Account totals do not match billing facts",
      "zh-CN": "账户汇总与账务事实不一致",
      ja: "アカウント集計と請求事実が一致しません",
      ko: "계정 합계와 결제 사실이 일치하지 않음",
    },
    stale_terminal_hold: {
      en: "A terminal job still reserves credit",
      "zh-CN": "终态任务仍占用额度",
      ja: "終了済みジョブがまだクレジットを確保しています",
      ko: "종료된 작업이 여전히 크레딧을 예약 중",
    },
    customer_charge_mismatch: {
      en: "Pricing and customer charges do not match",
      "zh-CN": "计价结果与客户收费不一致",
      ja: "価格計算と顧客請求が一致しません",
      ko: "가격 계산과 고객 청구가 일치하지 않음",
    },
    customer_refund_evidence_missing: {
      en: "Refund transaction is missing business evidence",
      "zh-CN": "退款交易缺少业务证据",
      ja: "返金取引に業務証拠がありません",
      ko: "환불 거래에 비즈니스 증거가 없음",
    },
    customer_refund_mismatch: {
      en: "Refund evidence and reversal ledger do not match",
      "zh-CN": "退款证据与冲正账本不一致",
      ja: "返金証拠と取消台帳が一致しません",
      ko: "환불 증거와 역분개 원장이 일치하지 않음",
    },
    customer_charge_attribution_missing: {
      en: "Charge is missing project or credential attribution",
      "zh-CN": "收费缺少项目或凭据归属",
      ja: "請求にプロジェクトまたは認証情報の帰属がありません",
      ko: "청구에 프로젝트 또는 자격 증명 귀속이 없음",
    },
    provider_cost_obligation_missing: {
      en: "Provider execution is missing a cost-tracking record",
      "zh-CN": "上游执行缺少成本追踪记录",
      ja: "プロバイダー実行にコスト追跡記録がありません",
      ko: "공급자 실행에 비용 추적 기록이 없음",
    },
    provider_cost_obligation_overdue: {
      en: "Provider cost conclusion is overdue",
      "zh-CN": "上游成本结论已逾期",
      ja: "プロバイダーコストの結論が期限超過です",
      ko: "공급자 비용 결론이 기한을 초과함",
    },
    provider_cost_authority_missing: {
      en: "Provider cost facts have no single authority",
      "zh-CN": "上游成本事实缺少唯一权威",
      ja: "プロバイダーコスト事実に一意の根拠がありません",
      ko: "공급자 비용 사실에 단일 권위가 없음",
    },
  };
  return labels[code]
    ? t(labels[code])
    : t({
        en: "Billing consistency issue found",
        "zh-CN": "发现账务一致性问题",
        ja: "請求整合性の問題が見つかりました",
        ko: "결제 일관성 문제 발견",
      });
}

function categoryLabel(t: Translate, category: string) {
  const labels: Record<
    string,
    { en: string; "zh-CN": string; ja: string; ko: string }
  > = {
    account_balance: {
      en: "Account balance",
      "zh-CN": "账户余额",
      ja: "アカウント残高",
      ko: "계정 잔액",
    },
    hold_lifecycle: {
      en: "Credit holds",
      "zh-CN": "额度冻结",
      ja: "クレジット保留",
      ko: "크레딧 보류",
    },
    customer_charge: {
      en: "Customer charges",
      "zh-CN": "客户收费",
      ja: "顧客請求",
      ko: "고객 청구",
    },
    customer_refund: {
      en: "Customer refunds",
      "zh-CN": "客户退款",
      ja: "顧客返金",
      ko: "고객 환불",
    },
    attribution: {
      en: "Project attribution",
      "zh-CN": "项目归属",
      ja: "プロジェクト帰属",
      ko: "프로젝트 귀속",
    },
    provider_cost: {
      en: "Provider costs",
      "zh-CN": "上游成本",
      ja: "プロバイダーコスト",
      ko: "공급자 비용",
    },
  };
  return labels[category] ? t(labels[category]) : category;
}

function findingDescription(t: Translate, finding: BillingIntegrityFinding) {
  if (finding.finding_code === "billing_account_counter_mismatch") {
    return t(
      {
        en: "Reserved: {actualHeld}; billing facts: {expectedHeld}. Captured: {actualCaptured}; billing facts: {expectedCaptured}. Refunded: {actualRefunded}; billing facts: {expectedRefunded}.",
        "zh-CN": "已冻结 {actualHeld}，账务事实应为 {expectedHeld}；已结算 {actualCaptured}，账务事实应为 {expectedCaptured}；已退款 {actualRefunded}，账务事实应为 {expectedRefunded}。",
        ja: "保留済み: {actualHeld}、請求事実: {expectedHeld}。確定済み: {actualCaptured}、請求事実: {expectedCaptured}。返金済み: {actualRefunded}、請求事実: {expectedRefunded}。",
        ko: "예약됨: {actualHeld}, 결제 사실: {expectedHeld}. 결제 확정: {actualCaptured}, 결제 사실: {expectedCaptured}. 환불됨: {actualRefunded}, 결제 사실: {expectedRefunded}.",
      },
      {
        actualHeld: moneyField(finding.actual, "held_micros", finding.currency),
        expectedHeld: moneyField(
          finding.expected,
          "held_micros",
          finding.currency,
        ),
        actualCaptured: moneyField(
          finding.actual,
          "captured_micros",
          finding.currency,
        ),
        expectedCaptured: moneyField(
          finding.expected,
          "captured_micros",
          finding.currency,
        ),
        actualRefunded: moneyField(
          finding.actual,
          "refunded_micros",
          finding.currency,
        ),
        expectedRefunded: moneyField(
          finding.expected,
          "refunded_micros",
          finding.currency,
        ),
      },
    );
  }
  if (finding.finding_code === "stale_terminal_hold") {
    return t(
      {
        en: "The job has ended, but {amount} is still reserved.",
        "zh-CN": "任务已经结束，但 {amount} 仍处于冻结状态。",
        ja: "ジョブは終了していますが、{amount} がまだ保留されています。",
        ko: "작업이 종료되었지만 {amount}가 여전히 예약되어 있습니다.",
      },
      {
        amount: moneyField(
          finding.actual,
          "held_micros",
          finding.currency,
        ),
      },
    );
  }
  if (finding.finding_code === "customer_charge_mismatch") {
    return t({
      en: "The priced amount, customer receivable, platform revenue, or sealed ledger record does not have a one-to-one match.",
      "zh-CN": "计价金额、客户应收、平台收入或封账记录未能形成一一对应关系。",
      ja: "価格計算額、顧客売掛、プラットフォーム収益、または確定台帳記録が一対一で対応していません。",
      ko: "가격 산정 금액, 고객 미수금, 플랫폼 수익 또는 마감 원장 기록이 일대일로 대응하지 않습니다.",
    });
  }
  if (finding.finding_code === "customer_charge_attribution_missing") {
    return t({
      en: "This charge is not linked to complete project, user, or API key attribution.",
      "zh-CN": "该笔收费尚未关联到完整的项目、用户或 API Key 归属信息。",
      ja: "この請求には、完全なプロジェクト、ユーザー、または API キーの帰属情報が関連付けられていません。",
      ko: "이 청구에는 완전한 프로젝트, 사용자 또는 API 키 귀속 정보가 연결되어 있지 않습니다.",
    });
  }
  if (
    finding.finding_code === "customer_refund_evidence_missing" ||
    finding.finding_code === "customer_refund_mismatch"
  ) {
    return t({
      en: "Refund evidence, the original charge, reversal transaction, double-entry records, and sealed state do not have a one-to-one match.",
      "zh-CN": "退款业务证据、原始扣费、冲正交易、双分录与封账状态未能形成一一对应关系。",
      ja: "返金証拠、元の請求、取消取引、複式記録、確定状態が一対一で対応していません。",
      ko: "환불 증거, 원 청구, 역분개 거래, 복식부기 기록 및 마감 상태가 일대일로 대응하지 않습니다.",
    });
  }
  if (finding.finding_code === "provider_cost_authority_missing") {
    return t({
      en: "The provider returned an explicit actual-cost fact, but no auditable, unique, and posted cost authority exists yet.",
      "zh-CN": "上游已返回明确的实际成本事实，但尚未形成可审计、唯一且已入账的成本权威。",
      ja: "プロバイダーは明確な実コスト事実を返しましたが、監査可能で一意かつ計上済みのコスト根拠がまだありません。",
      ko: "공급자가 명확한 실제 비용 사실을 반환했지만 감사 가능하고 유일하며 원장에 반영된 비용 근거가 아직 없습니다.",
    });
  }
  if (finding.finding_code === "provider_cost_obligation_missing") {
    return t({
      en: "This provider execution has no cost-tracking record, so its final cost cannot be proven as settled, pending confirmation, or waived by evidence.",
      "zh-CN": "该次上游执行没有对应的成本追踪记录，无法证明其最终成本是已结算、待确认或经证据豁免。",
      ja: "このプロバイダー実行にはコスト追跡記録がなく、最終コストが確定済み、確認待ち、または証拠に基づく免除であることを証明できません。",
      ko: "이 공급자 실행에는 비용 추적 기록이 없어 최종 비용이 정산됨, 확인 대기 또는 증거에 따른 면제인지 입증할 수 없습니다.",
    });
  }
  if (finding.finding_code === "provider_cost_obligation_overdue") {
    return t({
      en: "The cost conclusion for this provider execution is overdue. An authoritative cost or verifiable waiver evidence is still required; timeout never makes it free automatically.",
      "zh-CN": "该次上游执行的成本结论超过处理时限；仍需补充权威成本或可核验的豁免证据，不能按超时自动视为免费。",
      ja: "このプロバイダー実行のコスト結論は期限超過です。確定コストまたは検証可能な免除証拠が引き続き必要であり、タイムアウトだけで自動的に無料にはなりません。",
      ko: "이 공급자 실행의 비용 결론이 기한을 초과했습니다. 권위 있는 비용 또는 검증 가능한 면제 증거가 여전히 필요하며 시간 초과만으로 자동 무료 처리되지 않습니다.",
    });
  }
  return t({
    en: "Use this record's resource identifier to reconcile the underlying billing facts.",
    "zh-CN": "请根据该记录的资源标识进一步核对账务事实。",
    ja: "この記録のリソース識別子を使用して、基礎となる請求事実をさらに照合してください。",
    ko: "이 기록의 리소스 식별자를 사용해 기본 결제 사실을 추가로 대조하세요.",
  });
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

async function responseMessage(response: Response, t: Translate) {
  try {
    const payload = (await response.json()) as {
      error?: string | { message?: string };
    };
    if (typeof payload.error === "string") return payload.error;
    if (payload.error?.message) return payload.error.message;
  } catch {
    // Keep upstream response bodies private.
  }
  if (response.status === 409) {
    return t({
      en: "A billing check is already running",
      "zh-CN": "已有账务检查正在运行",
      ja: "請求整合性チェックはすでに実行中です",
      ko: "결제 무결성 검사가 이미 실행 중입니다",
    });
  }
  if (response.status === 403) {
    return t({
      en: "This account cannot run platform billing checks",
      "zh-CN": "当前账号没有运行平台账务检查的权限",
      ja: "このアカウントにはプラットフォームの請求チェックを実行する権限がありません",
      ko: "현재 계정에는 플랫폼 결제 검사를 실행할 권한이 없습니다",
    });
  }
  return t(
    {
      en: "Billing check failed ({status})",
      "zh-CN": "账务检查失败（{status}）",
      ja: "請求整合性チェックに失敗しました（{status}）",
      ko: "결제 무결성 검사 실패({status})",
    },
    { status: response.status },
  );
}
