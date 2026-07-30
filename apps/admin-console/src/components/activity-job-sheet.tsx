"use client";

import { Copy, ListTree, ReceiptText, TriangleAlert } from "lucide-react";
import { toast } from "sonner";
import {
  ActivityStatusBadge,
  formatActivityOperation,
  formatActivityStatus,
} from "@/components/activity-status-badge";
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
  operationEndpoint,
} from "@/lib/admin/format";
import type {
  ConsoleJobEconomicsSnapshot,
  JobEconomicsSnapshot,
  JobListItem,
  JobProviderCost,
} from "@/lib/admin/types";
import { useI18n } from "@/i18n/locale-provider";

type Translate = ReturnType<typeof useI18n>["t"];

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
  const { t } = useI18n();
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
          <Badge variant="outline">
            {formatActivityOperation(t, item.operation)}
          </Badge>
        </div>
        <SheetTitle className="pt-1 text-xl">
          {t({
            en: "Request details",
            "zh-CN": "请求详情",
            ja: "リクエスト詳細",
            ko: "요청 상세",
          })}
        </SheetTitle>
        <SheetDescription className="break-all font-mono text-xs">
          {item.request_id}
        </SheetDescription>
      </SheetHeader>

      <Tabs defaultValue="overview" className="flex min-h-0 flex-1 flex-col">
        <div className="shrink-0 overflow-x-auto border-b px-5 sm:px-6">
          <TabsList variant="line">
            <TabsTrigger value="overview" variant="line">
              <ListTree className="size-4" aria-hidden="true" />
              {t({
                en: "Overview",
                "zh-CN": "概览",
                ja: "概要",
                ko: "개요",
              })}
            </TabsTrigger>
            <TabsTrigger value="economics" variant="line">
              <ReceiptText className="size-4" aria-hidden="true" />
              {t({
                en: "Billing",
                "zh-CN": "计费",
                ja: "請求",
                ko: "청구",
              })}
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
                <p className="font-medium text-destructive">
                  {t({
                    en: "Request execution error",
                    "zh-CN": "请求执行异常",
                    ja: "リクエスト実行エラー",
                    ko: "요청 실행 오류",
                  })}
                </p>
                <p className="mt-1 break-all font-mono text-xs text-muted-foreground">
                  {item.last_error_code}
                </p>
              </div>
            </div>
          ) : null}

          <div className="space-y-8">
            <DetailSection
              title={t({
                en: "Overview",
                "zh-CN": "概览",
                ja: "概要",
                ko: "개요",
              })}
            >
              <DetailGrid>
                <DetailItem
                  label={t({
                    en: "Request status",
                    "zh-CN": "请求状态",
                    ja: "リクエスト状態",
                    ko: "요청 상태",
                  })}
                >
                  <ActivityStatusBadge state={item.job_state} />
                </DetailItem>
                <DetailItem
                  label={t({
                    en: "Duration",
                    "zh-CN": "耗时",
                    ja: "所要時間",
                    ko: "소요 시간",
                  })}
                >
                  {formatDurationMs(duration)}
                </DetailItem>
                <DetailItem
                  label={t({
                    en: "Provider",
                    "zh-CN": "供应商",
                    ja: "プロバイダー",
                    ko: "공급자",
                  })}
                >
                  {item.provider_id}
                </DetailItem>
                <DetailItem
                  label={t({
                    en: "Model",
                    "zh-CN": "模型",
                    ja: "モデル",
                    ko: "모델",
                  })}
                >
                  <span className="break-all font-mono text-xs">
                    {item.model}
                  </span>
                </DetailItem>
                <DetailItem
                  label={t({
                    en: "Created",
                    "zh-CN": "创建时间",
                    ja: "作成時刻",
                    ko: "생성 시간",
                  })}
                >
                  {formatDateTime(item.created_at_ms)}
                </DetailItem>
                <DetailItem
                  label={t({
                    en: "Completed",
                    "zh-CN": "完成时间",
                    ja: "完了時刻",
                    ko: "완료 시간",
                  })}
                >
                  {formatDateTime(item.finished_at_ms)}
                </DetailItem>
              </DetailGrid>
            </DetailSection>

            <DetailSection
              title={t({
                en: "Request",
                "zh-CN": "请求",
                ja: "リクエスト",
                ko: "요청",
              })}
            >
              <DetailRows>
                <CopyRow
                  label={t({
                    en: "Request ID",
                    "zh-CN": "请求 ID",
                    ja: "リクエスト ID",
                    ko: "요청 ID",
                  })}
                  value={item.request_id}
                />
                <CopyRow
                  label={t({
                    en: "Job ID",
                    "zh-CN": "任务 ID",
                    ja: "ジョブ ID",
                    ko: "작업 ID",
                  })}
                  value={item.job_id}
                />
                <DetailRow
                  label={t({
                    en: "API endpoint",
                    "zh-CN": "API 端点",
                    ja: "API エンドポイント",
                    ko: "API 엔드포인트",
                  })}
                >
                  <span className="font-mono text-xs">
                    {operationEndpoint(item.operation)}
                  </span>
                </DetailRow>
                <DetailRow
                  label={t({
                    en: "Operation",
                    "zh-CN": "操作",
                    ja: "操作",
                    ko: "작업",
                  })}
                >
                  {formatActivityOperation(t, item.operation)}
                </DetailRow>
              </DetailRows>
            </DetailSection>

            <DetailSection
              title={t({
                en: "Request ownership",
                "zh-CN": "调用归属",
                ja: "リクエスト所有者",
                ko: "요청 소유권",
              })}
            >
              <DetailRows>
                <DetailRow
                  label={t({
                    en: "Project ID",
                    "zh-CN": "项目 ID",
                    ja: "プロジェクト ID",
                    ko: "프로젝트 ID",
                  })}
                >
                  {item.project_id ??
                    t({
                      en: "Not linked in historical record",
                      "zh-CN": "历史记录未关联",
                      ja: "履歴レコードでは未関連付け",
                      ko: "기존 기록에 연결되지 않음",
                    })}
                </DetailRow>
                <DetailRow
                  label={t({
                    en: "API Key ID",
                    "zh-CN": "API 密钥 ID",
                    ja: "API キー ID",
                    ko: "API 키 ID",
                  })}
                >
                  {item.api_key_id ??
                    item.auth_kind ??
                    t({
                      en: "Not recorded",
                      "zh-CN": "未记录",
                      ja: "記録なし",
                      ko: "기록되지 않음",
                    })}
                </DetailRow>
                <DetailRow
                  label={t({
                    en: "Service Account",
                    "zh-CN": "服务账户",
                    ja: "サービスアカウント",
                    ko: "서비스 계정",
                  })}
                >
                  {item.service_account_id ??
                    t({
                      en: "Not recorded",
                      "zh-CN": "未记录",
                      ja: "記録なし",
                      ko: "기록되지 않음",
                    })}
                </DetailRow>
                <DetailRow
                  label={t({
                    en: "Tenant",
                    "zh-CN": "租户",
                    ja: "テナント",
                    ko: "테넌트",
                  })}
                >
                  {item.tenant_id}
                </DetailRow>
              </DetailRows>
            </DetailSection>

            <DetailSection
              title={t({
                en: "Execution",
                "zh-CN": "执行过程",
                ja: "実行",
                ko: "실행",
              })}
            >
              <DetailRows>
                <DetailRow
                  label={t({
                    en: "Job",
                    "zh-CN": "任务",
                    ja: "ジョブ",
                    ko: "작업",
                  })}
                >
                  {formatActivityStatus(t, item.job_state)}
                </DetailRow>
                <DetailRow
                  label={t({
                    en: "Queue",
                    "zh-CN": "队列",
                    ja: "キュー",
                    ko: "대기열",
                  })}
                >
                  {item.work_state
                    ? formatActivityStatus(t, item.work_state)
                    : t({
                        en: "No work item created",
                        "zh-CN": "未创建工作项",
                        ja: "作業項目未作成",
                        ko: "생성된 작업 항목 없음",
                      })}
                </DetailRow>
                <DetailRow
                  label={t({
                    en: "Provider",
                    "zh-CN": "供应商",
                    ja: "プロバイダー",
                    ko: "공급자",
                  })}
                >
                  {item.provider_states.length > 0 ? (
                    <div className="flex flex-wrap justify-end gap-1.5">
                      {item.provider_states.map((state) => (
                        <Badge
                          key={`${state.stage}-${state.state}`}
                          variant="outline"
                          className="font-normal"
                        >
                          {formatActivityStatus(t, state.stage)} ·{" "}
                          {formatActivityStatus(t, state.state)}{" "}
                          {formatInteger(state.count)}
                        </Badge>
                      ))}
                    </div>
                  ) : (
                    t({
                      en: "No upstream execution records yet",
                      "zh-CN": "尚无上游执行记录",
                      ja: "上流実行レコードはまだありません",
                      ko: "아직 업스트림 실행 기록 없음",
                    })
                  )}
                </DetailRow>
                <DetailRow
                  label={t({
                    en: "Started",
                    "zh-CN": "开始时间",
                    ja: "開始時刻",
                    ko: "시작 시간",
                  })}
                >
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
  const { t } = useI18n();
  const quote = snapshot.customer_quote;
  const rating = snapshot.customer_rating;
  const hold = snapshot.customer_hold;
  const providerCosts =
    "provider_costs" in snapshot ? snapshot.provider_costs : null;

  return (
    <div className="space-y-8">
      {stale ? (
        <p className="text-sm text-muted-foreground">
          {t({
            en: "Details could not be refreshed. Showing the last successful result.",
            "zh-CN": "明细刷新失败，当前显示上一次成功结果。",
            ja: "詳細を更新できなかったため、前回成功した結果を表示しています。",
            ko: "상세 정보를 새로 고치지 못해 마지막으로 성공한 결과를 표시합니다.",
          })}
        </p>
      ) : null}

      <div className="grid gap-5 bg-muted/40 p-4 sm:grid-cols-3">
        <SummaryMetric
          label={t({
            en: "Quote limit",
            "zh-CN": "报价上限",
            ja: "見積上限",
            ko: "견적 한도",
          })}
          value={
            quote
              ? formatMoneyMicros(quote.max_total_micros, quote.currency)
              : "--"
          }
        />
        <SummaryMetric
          label={t({
            en: "Actual charge",
            "zh-CN": "实际扣费",
            ja: "実際の請求額",
            ko: "실제 청구액",
          })}
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
          label={t({
            en: "Billing status",
            "zh-CN": "计费状态",
            ja: "請求状態",
            ko: "청구 상태",
          })}
          value={economicsStateLabel(t, snapshot.economics_state)}
        />
      </div>

      {snapshot.economics_contract_version !== 4 ? (
        <div className="border border-dashed px-4 py-3 text-sm text-muted-foreground">
          {t(
            {
              en: "This request uses the legacy v{version} pricing contract, so v4 quote and line-item billing evidence is unavailable.",
              "zh-CN":
                "此请求使用 v{version} 旧计价契约，因此没有 v4 报价与逐项计费证据。",
              ja: "このリクエストは旧 v{version} 料金契約を使用しているため、v4 の見積および明細別請求証拠はありません。",
              ko: "이 요청은 레거시 v{version} 가격 계약을 사용하므로 v4 견적 및 항목별 청구 증거가 없습니다.",
            },
            { version: snapshot.economics_contract_version },
          )}
        </div>
      ) : null}

      {quote ? (
        <DetailSection
          title={t({
            en: "Quote and settlement",
            "zh-CN": "报价与结算",
            ja: "見積と精算",
            ko: "견적 및 정산",
          })}
        >
          <DetailRows>
            <CopyRow
              label={t({
                en: "Quote ID",
                "zh-CN": "报价 ID",
                ja: "見積 ID",
                ko: "견적 ID",
              })}
              value={quote.quote_id}
            />
            <CopyRow
              label={t({
                en: "Price version",
                "zh-CN": "价格版本",
                ja: "価格バージョン",
                ko: "가격 버전",
              })}
              value={quote.price_book_version_id}
            />
            <DetailRow
              label={t({
                en: "Public model",
                "zh-CN": "公开模型",
                ja: "公開モデル",
                ko: "공개 모델",
              })}
            >
              <span className="font-mono text-xs">
                {quote.public_model_id}
              </span>
            </DetailRow>
            <DetailRow
              label={t({
                en: "Service tier",
                "zh-CN": "服务等级",
                ja: "サービス階層",
                ko: "서비스 등급",
              })}
            >
              {quote.service_tier}
            </DetailRow>
            <DetailRow
              label={t({
                en: "Quoted at",
                "zh-CN": "报价时间",
                ja: "見積時刻",
                ko: "견적 시간",
              })}
            >
              {formatDateTime(quote.created_at_ms)}
            </DetailRow>
            {hold ? (
              <>
                <DetailRow
                  label={t({
                    en: "Funds status",
                    "zh-CN": "资金状态",
                    ja: "資金状態",
                    ko: "자금 상태",
                  })}
                >
                  {formatActivityStatus(t, hold.state)}
                </DetailRow>
                <DetailRow
                  label={t({
                    en: "Held / captured",
                    "zh-CN": "冻结 / 扣取",
                    ja: "保留 / 売上確定",
                    ko: "보류 / 확정",
                  })}
                >
                  {formatMoneyMicros(hold.held_micros, hold.currency)} /{" "}
                  {formatMoneyMicros(hold.captured_micros, hold.currency)}
                </DetailRow>
              </>
            ) : null}
          </DetailRows>
        </DetailSection>
      ) : null}

      {quote?.lines.length ? (
        <DetailSection
          title={t({
            en: "Pricing details",
            "zh-CN": "计价明细",
            ja: "料金明細",
            ko: "가격 상세",
          })}
        >
          <div className="overflow-x-auto border">
            <Table className="min-w-[680px]">
              <TableHeader>
                <TableRow>
                  <TableHead>
                    {t({
                      en: "Pricing item",
                      "zh-CN": "计价项",
                      ja: "料金項目",
                      ko: "가격 항목",
                    })}
                  </TableHead>
                  <TableHead>
                    {t({
                      en: "Usage",
                      "zh-CN": "计量",
                      ja: "計測量",
                      ko: "사용량",
                    })}
                  </TableHead>
                  <TableHead>
                    {t({
                      en: "Unit price",
                      "zh-CN": "单价",
                      ja: "単価",
                      ko: "단가",
                    })}
                  </TableHead>
                  <TableHead className="text-right">
                    {t({
                      en: "Quote limit",
                      "zh-CN": "报价上限",
                      ja: "見積上限",
                      ko: "견적 한도",
                    })}
                  </TableHead>
                  <TableHead className="text-right">
                    {t({
                      en: "Actual cost",
                      "zh-CN": "实际费用",
                      ja: "実際の費用",
                      ko: "실제 비용",
                    })}
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {quote.lines.map((line) => (
                  <TableRow
                    key={`${line.partition_key}-${line.terminal_outcome}-${line.component_key}`}
                  >
                    <TableCell>
                      <p className="font-medium">
                        {formatActivityStatus(t, line.component_key)}
                      </p>
                      <p className="mt-1 text-xs text-muted-foreground">
                        {formatActivityStatus(t, line.terminal_outcome)}
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

      <DetailSection
        title={t({
          en: "Actual usage",
          "zh-CN": "实际计量",
          ja: "実際の計測",
          ko: "실제 사용량",
        })}
      >
        {snapshot.usage_facts.length ? (
          <div className="overflow-x-auto border">
            <Table className="min-w-[560px]">
              <TableHeader>
                <TableRow>
                  <TableHead>
                    {t({
                      en: "Metric",
                      "zh-CN": "指标",
                      ja: "指標",
                      ko: "지표",
                    })}
                  </TableHead>
                  <TableHead>
                    {t({
                      en: "Quantity",
                      "zh-CN": "数量",
                      ja: "数量",
                      ko: "수량",
                    })}
                  </TableHead>
                  <TableHead>
                    {t({
                      en: "Source",
                      "zh-CN": "来源",
                      ja: "ソース",
                      ko: "출처",
                    })}
                  </TableHead>
                  <TableHead>
                    {t({
                      en: "Confidence",
                      "zh-CN": "可信度",
                      ja: "信頼度",
                      ko: "신뢰도",
                    })}
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {snapshot.usage_facts.map((fact, index) => (
                  <TableRow
                    key={`${fact.metric}-${fact.billing_partition_key}-${fact.created_at_ms}-${index}`}
                  >
                    <TableCell>
                      <p className="font-medium">
                        {formatActivityStatus(t, fact.metric)}
                      </p>
                      <p className="mt-1 text-xs text-muted-foreground">
                        {formatActivityStatus(t, fact.terminal_outcome)}
                      </p>
                    </TableCell>
                    <TableCell className="tabular-nums">
                      {formatInteger(fact.quantity)} {fact.unit}
                    </TableCell>
                    <TableCell>
                      {formatActivityStatus(t, fact.quantity_source)}
                    </TableCell>
                    <TableCell>
                      {formatActivityStatus(t, fact.confidence)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        ) : (
          <EmptyEvidence>
            {t({
              en: "No actual usage facts have been recorded yet.",
              "zh-CN": "尚未产生实际计量事实。",
              ja: "実際の計測事実はまだ記録されていません。",
              ko: "아직 실제 사용량 정보가 기록되지 않았습니다.",
            })}
          </EmptyEvidence>
        )}
      </DetailSection>

      <DetailSection
        title={t({
          en: "Customer ledger",
          "zh-CN": "客户账本",
          ja: "顧客台帳",
          ko: "고객 원장",
        })}
      >
        {snapshot.ledger_transactions.length ? (
          <div className="space-y-3">
            {snapshot.ledger_transactions.map((transaction) => (
              <div
                key={transaction.transaction_id}
                className="flex min-w-0 items-start justify-between gap-4 text-sm"
              >
                <div className="min-w-0">
                  <p className="font-medium">
                    {formatActivityStatus(t, transaction.transaction_type)}
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
                      ? t(
                          {
                            en: "Sealed {time}",
                            "zh-CN": "封账 {time}",
                            ja: "確定 {time}",
                            ko: "마감 {time}",
                          },
                          { time: formatDateTime(transaction.sealed_at_ms) },
                        )
                      : t({
                          en: "Not sealed",
                          "zh-CN": "尚未封账",
                          ja: "未確定",
                          ko: "마감되지 않음",
                        })}
                  </p>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <EmptyEvidence>
            {rating?.total_amount_micros === "0"
              ? t({
                  en: "Zero-amount requests do not require a ledger transaction.",
                  "zh-CN": "零金额请求无需创建账本交易。",
                  ja: "金額がゼロのリクエストには台帳取引は不要です。",
                  ko: "금액이 0인 요청에는 원장 거래가 필요하지 않습니다.",
                })
              : t({
                  en: "No customer ledger transaction has been created yet.",
                  "zh-CN": "尚未生成客户账本交易。",
                  ja: "顧客台帳取引はまだ作成されていません。",
                  ko: "아직 고객 원장 거래가 생성되지 않았습니다.",
                })}
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
  const { t } = useI18n();

  return (
    <DetailSection
      title={t({
        en: "Provider costs (admin)",
        "zh-CN": "Provider 成本（管理员）",
        ja: "Provider コスト（管理者）",
        ko: "Provider 비용(관리자)",
      })}
    >
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
                    {providerCostBasisLabel(t, cost.cost_basis)}
                  </p>
                  <Badge variant="outline" className="font-normal">
                    {cost.attribution_state === "shared"
                      ? t({
                          en: "Shared observation",
                          "zh-CN": "共享观测",
                          ja: "共有観測",
                          ko: "공유 관측",
                        })
                      : t({
                          en: "Attributed",
                          "zh-CN": "已归因",
                          ja: "帰属済み",
                          ko: "귀속됨",
                        })}
                  </Badge>
                </div>
                <p className="mt-1 text-xs text-muted-foreground">
                  {formatActivityStatus(t, cost.authority)} ·{" "}
                  {formatActivityStatus(t, cost.confidence)}
                </p>
              </div>
              <div className="text-left sm:text-right">
                <p className="font-medium tabular-nums">
                  {cost.attributed_amount_micros !== null
                    ? formatMoneyMicros(
                        cost.attributed_amount_micros,
                        cost.currency,
                      )
                    : t({
                        en: "Not attributed to an individual request",
                        "zh-CN": "未归因到单次请求",
                        ja: "個別リクエストに帰属していません",
                        ko: "개별 요청에 귀속되지 않음",
                      })}
                </p>
                {cost.attribution_state === "shared" ? (
                  <p className="mt-1 text-xs text-muted-foreground">
                    {t({
                      en: "Observed total",
                      "zh-CN": "观测总额",
                      ja: "観測合計",
                      ko: "관측 합계",
                    })}{" "}
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
        <EmptyEvidence>
          {t({
            en: "No verifiable provider cost evidence is available yet.",
            "zh-CN": "尚无可验证的 Provider 成本证据。",
            ja: "検証可能な Provider コスト証拠はまだありません。",
            ko: "아직 검증 가능한 Provider 비용 증거가 없습니다.",
          })}
        </EmptyEvidence>
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
  const { t } = useI18n();

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
                onClick={() =>
                  copyValue(
                    value,
                    t({
                      en: "Copied",
                      "zh-CN": "已复制",
                      ja: "コピーしました",
                      ko: "복사됨",
                    }),
                    t({
                      en: "Copy failed",
                      "zh-CN": "复制失败",
                      ja: "コピーに失敗しました",
                      ko: "복사 실패",
                    }),
                  )
                }
              >
                <Copy aria-hidden="true" />
                <span className="sr-only">
                  {t(
                    {
                      en: "Copy {label}",
                      "zh-CN": "复制 {label}",
                      ja: "{label} をコピー",
                      ko: "{label} 복사",
                    },
                    { label },
                  )}
                </span>
              </Button>
            </TooltipTrigger>
            <TooltipContent>
              {t(
                {
                  en: "Copy {label}",
                  "zh-CN": "复制 {label}",
                  ja: "{label} をコピー",
                  ko: "{label} 복사",
                },
                { label },
              )}
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </span>
    </DetailRow>
  );
}

function economicsStateLabel(
  t: Translate,
  value: EconomicsSnapshot["economics_state"],
) {
  const labels: Record<
    EconomicsSnapshot["economics_state"],
    { en: string; "zh-CN": string; ja: string; ko: string }
  > = {
    legacy_contract: {
      en: "Legacy contract",
      "zh-CN": "旧版契约",
      ja: "旧契約",
      ko: "레거시 계약",
    },
    awaiting_quote: {
      en: "Awaiting quote",
      "zh-CN": "等待报价",
      ja: "見積待ち",
      ko: "견적 대기",
    },
    quoted: {
      en: "Quoted",
      "zh-CN": "已报价",
      ja: "見積済み",
      ko: "견적 완료",
    },
    metered: {
      en: "Metered",
      "zh-CN": "已计量",
      ja: "計測済み",
      ko: "계측됨",
    },
    rated: {
      en: "Settled",
      "zh-CN": "已结算",
      ja: "精算済み",
      ko: "정산됨",
    },
  };
  return t(labels[value]);
}

function providerCostBasisLabel(
  t: Translate,
  value: JobProviderCost["cost_basis"],
) {
  const labels: Record<
    JobProviderCost["cost_basis"],
    { en: string; "zh-CN": string; ja: string; ko: string }
  > = {
    provider_actual: {
      en: "Actual provider cost",
      "zh-CN": "Provider 实际成本",
      ja: "Provider の実際コスト",
      ko: "Provider 실제 비용",
    },
    provider_allocated: {
      en: "Subscription / quota allocation",
      "zh-CN": "订阅 / 额度分摊",
      ja: "サブスクリプション / クォータ配賦",
      ko: "구독 / 할당량 배분",
    },
    legacy_unverified: {
      en: "Unverified legacy cost",
      "zh-CN": "旧版未验证成本",
      ja: "未検証の旧コスト",
      ko: "검증되지 않은 레거시 비용",
    },
  };
  return t(labels[value]);
}

function jobDuration(item: JobListItem): number | null {
  if (item.started_at_ms === null || item.finished_at_ms === null) return null;
  return Math.max(0, item.finished_at_ms - item.started_at_ms);
}

async function copyValue(
  value: string,
  successMessage: string,
  errorMessage: string,
) {
  try {
    await navigator.clipboard.writeText(value);
    toast.success(successMessage);
  } catch {
    toast.error(errorMessage);
  }
}
