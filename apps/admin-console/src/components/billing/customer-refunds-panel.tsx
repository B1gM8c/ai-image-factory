"use client";

import { useEffect, useMemo, useState } from "react";
import {
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  LoaderCircle,
  RefreshCw,
  RotateCcw,
} from "lucide-react";
import { toast } from "sonner";
import { AdminQueryError, AdminQuerySkeleton } from "@/components/admin-query-state";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
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
  SheetFooter,
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
import { Textarea } from "@/components/ui/textarea";
import { useAdminQuery } from "@/hooks/use-admin-query";
import { useI18n } from "@/i18n/locale-provider";
import {
  decimalToMicros,
  formatDateTime,
  formatMoneyMicros,
  microsToDecimal,
} from "@/lib/admin/format";
import type {
  CustomerChargeDetail,
  CustomerChargeList,
  CustomerChargeView,
  CustomerRefundView,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

const PAGE_SIZE = 50;
type Translate = ReturnType<typeof useI18n>["t"];

type RefundState =
  | "refundable"
  | "partially_refunded"
  | "fully_refunded"
  | "all";

export function CustomerRefundsPanel({
  enabled,
  tenantNames,
}: {
  enabled: boolean;
  tenantNames: Map<string, string>;
}) {
  const { t } = useI18n();
  const [state, setState] = useState<RefundState>("refundable");
  const [tenantId, setTenantId] = useState("");
  const [debouncedTenantId, setDebouncedTenantId] = useState("");
  const [cursors, setCursors] = useState<Array<string | null>>([null]);
  const [selectedTransactionId, setSelectedTransactionId] =
    useState<string | null>(null);

  useEffect(() => {
    const timeout = window.setTimeout(
      () => setDebouncedTenantId(tenantId.trim()),
      300,
    );
    return () => window.clearTimeout(timeout);
  }, [tenantId]);

  useEffect(() => {
    setCursors([null]);
  }, [debouncedTenantId, state]);

  const cursor = cursors[cursors.length - 1];
  const endpoint = useMemo(() => {
    const params = new URLSearchParams({
      state,
      limit: PAGE_SIZE.toString(),
    });
    if (debouncedTenantId) params.set("tenant_id", debouncedTenantId);
    if (cursor) params.set("after", cursor);
    return `/admin/v1/billing/customer-charges?${params.toString()}`;
  }, [cursor, debouncedTenantId, state]);
  const query = useAdminQuery<CustomerChargeList>(endpoint, enabled);

  return (
    <section className="min-w-0 space-y-4">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-base font-semibold">
            {t({
              en: "Customer charges and refunds",
              "zh-CN": "客户扣费与退款",
              ja: "顧客請求と返金",
              ko: "고객 청구 및 환불",
            })}
          </h2>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            {t({
              en: "Original usage and charges remain unchanged. Refunds are appended to the ledger as separate reversal transactions.",
              "zh-CN": "原始用量和扣费保持不变，退款以独立冲正交易追加到账本。",
              ja: "元の使用量と請求は変更されず、返金は独立した取消取引として台帳に追加されます。",
              ko: "원래 사용량과 청구 내역은 유지되며, 환불은 별도의 취소 거래로 원장에 추가됩니다.",
            })}
          </p>
        </div>
        <Button
          type="button"
          variant="outline"
          size="icon"
          aria-label={t({
            en: "Refresh refund records",
            "zh-CN": "刷新退款记录",
            ja: "返金履歴を更新",
            ko: "환불 기록 새로고침",
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

      <div className="flex flex-col gap-2 sm:flex-row">
        <Select value={state} onValueChange={(value) => setState(value as RefundState)}>
          <SelectTrigger
            className="w-full sm:w-48"
            aria-label={t({
              en: "Filter by refund status",
              "zh-CN": "筛选退款状态",
              ja: "返金ステータスで絞り込む",
              ko: "환불 상태로 필터링",
            })}
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="refundable">
              {t({
                en: "Refundable",
                "zh-CN": "可退款",
                ja: "返金可能",
                ko: "환불 가능",
              })}
            </SelectItem>
            <SelectItem value="partially_refunded">
              {t({
                en: "Partially refunded",
                "zh-CN": "部分退款",
                ja: "一部返金済み",
                ko: "일부 환불됨",
              })}
            </SelectItem>
            <SelectItem value="fully_refunded">
              {t({
                en: "Fully refunded",
                "zh-CN": "已全额退款",
                ja: "全額返金済み",
                ko: "전액 환불됨",
              })}
            </SelectItem>
            <SelectItem value="all">
              {t({
                en: "All charges",
                "zh-CN": "全部扣费",
                ja: "すべての請求",
                ko: "모든 청구",
              })}
            </SelectItem>
          </SelectContent>
        </Select>
        <Input
          className="w-full sm:max-w-sm"
          value={tenantId}
          onChange={(event) => setTenantId(event.target.value)}
          placeholder={t({
            en: "Filter by exact organization ID",
            "zh-CN": "按组织 ID 精确筛选",
            ja: "組織 ID で完全一致検索",
            ko: "정확한 조직 ID로 필터링",
          })}
          aria-label={t({
            en: "Filter by organization ID",
            "zh-CN": "按组织 ID 筛选",
            ja: "組織 ID で絞り込む",
            ko: "조직 ID로 필터링",
          })}
        />
      </div>

      {query.loading ? <AdminQuerySkeleton rows={7} /> : null}
      {!query.loading && query.error && !query.data ? (
        <AdminQueryError error={query.error} retry={query.retry} />
      ) : null}
      {query.data ? (
        <ChargeTable
          rows={query.data.data}
          tenantNames={tenantNames}
          onSelect={(row) => setSelectedTransactionId(row.transaction_id)}
        />
      ) : null}

      {query.data ? (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            {t(
              {
                en: "Page {page}",
                "zh-CN": "第 {page} 页",
                ja: "{page} ページ",
                ko: "{page}페이지",
              },
              { page: cursors.length },
            )}
          </p>
          <div className="flex gap-2">
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() =>
                setCursors((current) =>
                  current.length > 1 ? current.slice(0, -1) : current,
                )
              }
              disabled={cursors.length === 1 || query.refreshing}
            >
              <ChevronLeft aria-hidden="true" />
              {t({
                en: "Previous",
                "zh-CN": "上一页",
                ja: "前へ",
                ko: "이전",
              })}
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => {
                const next = query.data?.next_after;
                if (next) setCursors((current) => [...current, next]);
              }}
              disabled={!query.data.has_more || query.refreshing}
            >
              {t({
                en: "Next",
                "zh-CN": "下一页",
                ja: "次へ",
                ko: "다음",
              })}
              <ChevronRight aria-hidden="true" />
            </Button>
          </div>
        </div>
      ) : null}

      <CustomerChargeSheet
        transactionId={selectedTransactionId}
        tenantNames={tenantNames}
        onOpenChange={(open) => {
          if (!open) setSelectedTransactionId(null);
        }}
        onRefunded={query.retry}
      />
    </section>
  );
}

function ChargeTable({
  rows,
  tenantNames,
  onSelect,
}: {
  rows: CustomerChargeView[];
  tenantNames: Map<string, string>;
  onSelect: (row: CustomerChargeView) => void;
}) {
  const { t, locale } = useI18n();
  if (rows.length === 0) {
    return (
      <div className="flex min-h-72 flex-col items-center justify-center rounded-md border px-6 text-center">
        <CheckCircle2 className="size-8 text-muted-foreground" aria-hidden="true" />
        <h3 className="mt-4 text-sm font-medium">
          {t({
            en: "No customer charges match these filters",
            "zh-CN": "当前筛选下没有客户扣费",
            ja: "この条件に一致する顧客請求はありません",
            ko: "현재 필터와 일치하는 고객 청구가 없습니다",
          })}
        </h3>
        <p className="mt-1 max-w-md text-sm text-muted-foreground">
          {t({
            en: "Finalized customer charges appear here with their immutable refund history.",
            "zh-CN": "已封账的客户扣费会显示在这里，并可查看不可变退款历史。",
            ja: "確定済みの顧客請求と、変更不可能な返金履歴がここに表示されます。",
            ko: "확정된 고객 청구와 변경할 수 없는 환불 이력이 여기에 표시됩니다.",
          })}
        </p>
      </div>
    );
  }

  return (
    <div className="overflow-x-auto rounded-md border">
      <Table className="min-w-[920px]">
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">
              {t({ en: "Organization", "zh-CN": "组织", ja: "組織", ko: "조직" })}
            </TableHead>
            <TableHead>
              {t({
                en: "Original charge",
                "zh-CN": "原始扣费",
                ja: "元の請求",
                ko: "원래 청구",
              })}
            </TableHead>
            <TableHead>
              {t({ en: "Refunded", "zh-CN": "已退款", ja: "返金済み", ko: "환불됨" })}
            </TableHead>
            <TableHead>
              {t({
                en: "Refundable",
                "zh-CN": "剩余可退",
                ja: "返金可能残高",
                ko: "환불 가능 잔액",
              })}
            </TableHead>
            <TableHead>
              {t({ en: "Status", "zh-CN": "状态", ja: "ステータス", ko: "상태" })}
            </TableHead>
            <TableHead>
              {t({
                en: "Charged at",
                "zh-CN": "扣费时间",
                ja: "請求日時",
                ko: "청구 시간",
              })}
            </TableHead>
            <TableHead className="w-24 pr-4 text-right">
              {t({ en: "Actions", "zh-CN": "操作", ja: "操作", ko: "작업" })}
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => (
            <TableRow
              key={row.transaction_id}
              className="cursor-pointer"
              onClick={() => onSelect(row)}
            >
              <TableCell className="max-w-64 pl-4">
                <p className="truncate font-medium">
                  {tenantNames.get(row.tenant_id) ?? row.tenant_id}
                </p>
                <p className="mt-0.5 truncate font-mono text-xs text-muted-foreground">
                  {row.tenant_id}
                </p>
              </TableCell>
              <MoneyCell value={row.amount_micros} currency={row.currency} />
              <MoneyCell value={row.refunded_micros} currency={row.currency} />
              <MoneyCell
                value={row.remaining_refundable_micros}
                currency={row.currency}
              />
              <TableCell>
                <RefundStateBadge state={row.refund_state} />
              </TableCell>
              <TableCell>{formatDateTime(row.created_at_ms, locale)}</TableCell>
              <TableCell className="pr-4 text-right">
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={(event) => {
                    event.stopPropagation();
                    onSelect(row);
                  }}
                >
                  {t({ en: "View", "zh-CN": "查看", ja: "表示", ko: "보기" })}
                  <ChevronRight aria-hidden="true" />
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function CustomerChargeSheet({
  transactionId,
  tenantNames,
  onOpenChange,
  onRefunded,
}: {
  transactionId: string | null;
  tenantNames: Map<string, string>;
  onOpenChange: (open: boolean) => void;
  onRefunded: () => void;
}) {
  const { t, locale } = useI18n();
  const [refundOpen, setRefundOpen] = useState(false);
  const endpoint = transactionId
    ? `/admin/v1/billing/customer-charges/${encodeURIComponent(transactionId)}`
    : "/admin/v1/billing/customer-charges/none";
  const query = useAdminQuery<CustomerChargeDetail>(endpoint, Boolean(transactionId));

  useEffect(() => {
    if (!transactionId) setRefundOpen(false);
  }, [transactionId]);

  const charge =
    query.data?.transaction_id === transactionId ? query.data : null;
  return (
    <>
      <Sheet open={Boolean(transactionId)} onOpenChange={onOpenChange}>
        <SheetContent className="flex h-full w-full flex-col gap-0 overflow-hidden p-0 sm:max-w-2xl">
          <SheetHeader className="shrink-0 border-b px-5 py-5 pr-12 text-left sm:px-6">
            <SheetTitle>
              {t({
                en: "Charge details",
                "zh-CN": "扣费详情",
                ja: "請求の詳細",
                ko: "청구 상세",
              })}
            </SheetTitle>
            <SheetDescription className="truncate font-mono">
              {transactionId ?? "--"}
            </SheetDescription>
          </SheetHeader>

          <div className="min-h-0 flex-1 overflow-y-auto px-5 py-6 sm:px-6">
            {query.loading ? <AdminQuerySkeleton rows={6} /> : null}
            {!query.loading && query.error && !query.data ? (
              <AdminQueryError error={query.error} retry={query.retry} />
            ) : null}
            {charge ? (
              <div className="space-y-7">
                <dl className="grid gap-x-6 gap-y-4 sm:grid-cols-2">
                  <Definition
                    label={t({
                      en: "Organization",
                      "zh-CN": "组织",
                      ja: "組織",
                      ko: "조직",
                    })}
                    value={tenantNames.get(charge.tenant_id) ?? charge.tenant_id}
                  />
                  <Definition
                    label={t({
                      en: "Status",
                      "zh-CN": "状态",
                      ja: "ステータス",
                      ko: "상태",
                    })}
                    value={refundStateLabel(t, charge.refund_state)}
                  />
                  <Definition
                    label={t({
                      en: "Original charge",
                      "zh-CN": "原始扣费",
                      ja: "元の請求",
                      ko: "원래 청구",
                    })}
                    value={formatMoneyMicros(
                      charge.amount_micros,
                      charge.currency,
                    )}
                    mono
                  />
                  <Definition
                    label={t({
                      en: "Refundable",
                      "zh-CN": "剩余可退",
                      ja: "返金可能残高",
                      ko: "환불 가능 잔액",
                    })}
                    value={formatMoneyMicros(
                      charge.remaining_refundable_micros,
                      charge.currency,
                    )}
                    mono
                  />
                  <Definition
                    label={t({
                      en: "Charged at",
                      "zh-CN": "扣费时间",
                      ja: "請求日時",
                      ko: "청구 시간",
                    })}
                    value={formatDateTime(charge.created_at_ms, locale)}
                  />
                  <Definition
                    label={t({
                      en: "Job ID",
                      "zh-CN": "任务 ID",
                      ja: "ジョブ ID",
                      ko: "작업 ID",
                    })}
                    value={charge.job_id}
                    mono
                  />
                </dl>

                <section>
                  <div className="mb-3 flex items-center justify-between gap-3">
                    <h3 className="text-sm font-semibold">
                      {t({
                        en: "Refund history",
                        "zh-CN": "退款历史",
                        ja: "返金履歴",
                        ko: "환불 이력",
                      })}
                    </h3>
                    <span className="text-xs text-muted-foreground">
                      {t(
                        {
                          en: "{count} refunds",
                          "zh-CN": "{count} 笔",
                          ja: "{count} 件",
                          ko: "{count}건",
                        },
                        { count: charge.refunds.length },
                      )}
                    </span>
                  </div>
                  <RefundHistory
                    refunds={charge.refunds}
                    currency={charge.currency}
                  />
                </section>
              </div>
            ) : null}
          </div>

          {charge ? (
            <SheetFooter className="shrink-0 border-t bg-background px-5 py-4 sm:px-6">
              <Button
                type="button"
                onClick={() => setRefundOpen(true)}
                disabled={charge.remaining_refundable_micros === "0"}
              >
                <RotateCcw aria-hidden="true" />
                {charge.remaining_refundable_micros === "0"
                  ? t({
                      en: "Fully refunded",
                      "zh-CN": "已全额退款",
                      ja: "全額返金済み",
                      ko: "전액 환불됨",
                    })
                  : t({
                      en: "Issue refund",
                      "zh-CN": "发起退款",
                      ja: "返金する",
                      ko: "환불 처리",
                    })}
              </Button>
            </SheetFooter>
          ) : null}
        </SheetContent>
      </Sheet>

      <RefundDialog
        charge={charge}
        open={refundOpen}
        onOpenChange={setRefundOpen}
        onSuccess={() => {
          query.retry();
          onRefunded();
        }}
      />
    </>
  );
}

function RefundHistory({
  refunds,
  currency,
}: {
  refunds: CustomerRefundView[];
  currency: string;
}) {
  const { t, locale } = useI18n();
  if (refunds.length === 0) {
    return (
      <div className="rounded-md border px-4 py-10 text-center text-sm text-muted-foreground">
        {t({
          en: "No refunds yet",
          "zh-CN": "尚未发生退款",
          ja: "返金はまだありません",
          ko: "아직 환불 내역이 없습니다",
        })}
      </div>
    );
  }
  return (
    <div className="overflow-x-auto rounded-md border">
      <Table className="min-w-[620px]">
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">
              {t({ en: "Amount", "zh-CN": "金额", ja: "金額", ko: "금액" })}
            </TableHead>
            <TableHead>
              {t({ en: "Reason", "zh-CN": "原因", ja: "理由", ko: "사유" })}
            </TableHead>
            <TableHead>
              {t({ en: "Actor", "zh-CN": "操作人", ja: "実行者", ko: "처리자" })}
            </TableHead>
            <TableHead className="pr-4">
              {t({ en: "Time", "zh-CN": "时间", ja: "日時", ko: "시간" })}
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {refunds.map((refund) => (
            <TableRow key={refund.refund_id}>
              <TableCell className="pl-4 font-mono tabular-nums">
                {formatMoneyMicros(refund.amount_micros, currency)}
              </TableCell>
              <TableCell className="max-w-64">
                <p>{refundReasonLabel(t, refund.reason_code)}</p>
                <p className="mt-0.5 truncate text-xs text-muted-foreground">
                  {refund.reason}
                </p>
              </TableCell>
              <TableCell className="max-w-40 truncate font-mono text-xs">
                {refund.actor_user_id}
              </TableCell>
              <TableCell className="pr-4">
                {formatDateTime(refund.created_at_ms, locale)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function RefundDialog({
  charge,
  open,
  onOpenChange,
  onSuccess,
}: {
  charge: CustomerChargeDetail | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSuccess: () => void;
}) {
  const { t } = useI18n();
  const [amount, setAmount] = useState("");
  const [reasonCode, setReasonCode] = useState("customer_request");
  const [reason, setReason] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!open || !charge) return;
    setAmount(microsToDecimal(charge.remaining_refundable_micros));
    setReasonCode("customer_request");
    setReason("");
    setError(null);
  }, [charge, open]);

  async function submit() {
    if (!charge) return;
    const amountMicros = decimalToMicros(amount);
    if (
      amountMicros === null ||
      BigInt(amountMicros) > BigInt(charge.remaining_refundable_micros)
    ) {
      setError(
        t({
          en: "The refund amount must be greater than 0 and cannot exceed the refundable balance.",
          "zh-CN": "退款金额必须大于 0，且不能超过剩余可退金额",
          ja: "返金額は 0 より大きく、返金可能残高を超えないようにしてください。",
          ko: "환불 금액은 0보다 커야 하며 환불 가능 잔액을 초과할 수 없습니다.",
        }),
      );
      return;
    }
    if (reason.trim().length < 3) {
      setError(
        t({
          en: "Enter a refund note of at least 3 characters.",
          "zh-CN": "请填写至少 3 个字符的退款说明",
          ja: "返金メモを 3 文字以上入力してください。",
          ko: "환불 설명을 3자 이상 입력하세요.",
        }),
      );
      return;
    }

    setSaving(true);
    setError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/admin/v1/billing/customer-charges/${encodeURIComponent(
          charge.transaction_id,
        )}/refunds`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "idempotency-key": crypto.randomUUID(),
          },
          body: JSON.stringify({
            amount_micros: amountMicros,
            reason_code: reasonCode,
            reason: reason.trim(),
          }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response, t));
      toast.success(
        t({
          en: "Refund recorded in the ledger",
          "zh-CN": "退款已记入账本",
          ja: "返金を台帳に記録しました",
          ko: "환불이 원장에 기록되었습니다",
        }),
      );
      onOpenChange(false);
      onSuccess();
    } catch (caught) {
      setError(
        caught instanceof Error
          ? caught.message
          : t({
              en: "Refund failed",
              "zh-CN": "退款失败",
              ja: "返金に失敗しました",
              ko: "환불에 실패했습니다",
            }),
      );
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[calc(100vh-2rem)] overflow-y-auto sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>
            {t({
              en: "Confirm customer refund",
              "zh-CN": "确认客户退款",
              ja: "顧客への返金を確認",
              ko: "고객 환불 확인",
            })}
          </DialogTitle>
          <DialogDescription>
            {t({
              en: "This adds an immutable reversal transaction without deleting the original usage or charge.",
              "zh-CN": "此操作会追加不可变冲正交易，不会删除原始用量或扣费记录。",
              ja: "この操作は変更不可能な取消取引を追加し、元の使用量や請求は削除しません。",
              ko: "이 작업은 변경할 수 없는 취소 거래를 추가하며 원래 사용량이나 청구 기록을 삭제하지 않습니다.",
            })}
          </DialogDescription>
        </DialogHeader>

        {charge ? (
          <div className="grid gap-5 py-1">
            <div className="rounded-md bg-muted/40 px-4 py-3 text-sm">
              <div className="flex items-center justify-between gap-4">
                <span className="text-muted-foreground">
                  {t({
                    en: "Refundable",
                    "zh-CN": "剩余可退",
                    ja: "返金可能残高",
                    ko: "환불 가능 잔액",
                  })}
                </span>
                <span className="font-mono font-medium tabular-nums">
                  {formatMoneyMicros(
                    charge.remaining_refundable_micros,
                    charge.currency,
                  )}
                </span>
              </div>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="refund-amount">
                {t({
                  en: "Refund amount",
                  "zh-CN": "退款金额",
                  ja: "返金額",
                  ko: "환불 금액",
                })}
              </Label>
              <div className="flex">
                <span className="flex h-9 items-center rounded-l-md border border-r-0 bg-muted px-3 text-sm text-muted-foreground">
                  {charge.currency}
                </span>
                <Input
                  id="refund-amount"
                  className="rounded-l-none font-mono tabular-nums"
                  inputMode="decimal"
                  value={amount}
                  onChange={(event) => setAmount(event.target.value)}
                  disabled={saving}
                />
              </div>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="refund-reason-code">
                {t({
                  en: "Refund reason",
                  "zh-CN": "退款原因",
                  ja: "返金理由",
                  ko: "환불 사유",
                })}
              </Label>
              <Select
                value={reasonCode}
                onValueChange={setReasonCode}
                disabled={saving}
              >
                <SelectTrigger id="refund-reason-code">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="customer_request">
                    {refundReasonLabel(t, "customer_request")}
                  </SelectItem>
                  <SelectItem value="service_failure">
                    {refundReasonLabel(t, "service_failure")}
                  </SelectItem>
                  <SelectItem value="billing_correction">
                    {refundReasonLabel(t, "billing_correction")}
                  </SelectItem>
                  <SelectItem value="fraud_dispute">
                    {refundReasonLabel(t, "fraud_dispute")}
                  </SelectItem>
                  <SelectItem value="provider_refund_pass_through">
                    {refundReasonLabel(t, "provider_refund_pass_through")}
                  </SelectItem>
                  <SelectItem value="other">
                    {refundReasonLabel(t, "other")}
                  </SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="refund-reason">
                {t({
                  en: "Refund note",
                  "zh-CN": "退款说明",
                  ja: "返金メモ",
                  ko: "환불 설명",
                })}
              </Label>
              <Textarea
                id="refund-reason"
                value={reason}
                onChange={(event) => setReason(event.target.value)}
                placeholder={t({
                  en: "Enter a ticket number or refund justification",
                  "zh-CN": "填写工单号或退款依据",
                  ja: "チケット番号または返金の根拠を入力",
                  ko: "티켓 번호 또는 환불 근거 입력",
                })}
                maxLength={500}
                disabled={saving}
              />
            </div>
            {error ? (
              <p className="text-sm text-destructive" role="alert">
                {error}
              </p>
            ) : null}
          </div>
        ) : null}

        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={saving}
          >
            {t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}
          </Button>
          <Button type="button" onClick={() => void submit()} disabled={saving}>
            {saving ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <RotateCcw aria-hidden="true" />
            )}
            {t({
              en: "Confirm refund",
              "zh-CN": "确认退款",
              ja: "返金を確定",
              ko: "환불 확인",
            })}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function MoneyCell({ value, currency }: { value: string; currency: string }) {
  return (
    <TableCell className="font-mono tabular-nums">
      {formatMoneyMicros(value, currency)}
    </TableCell>
  );
}

function RefundStateBadge({ state }: { state: CustomerChargeView["refund_state"] }) {
  const { t } = useI18n();
  if (state === "fully_refunded") {
    return (
      <Badge variant="outline">
        {refundStateLabel(t, "fully_refunded")}
      </Badge>
    );
  }
  if (state === "partially_refunded") {
    return (
      <Badge variant="secondary">
        {refundStateLabel(t, "partially_refunded")}
      </Badge>
    );
  }
  return <Badge>{refundStateLabel(t, "refundable")}</Badge>;
}

function Definition({
  label,
  value,
  mono = false,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div className="min-w-0">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd
        className={[
          "mt-1 break-words text-sm",
          mono ? "font-mono tabular-nums" : "",
        ].join(" ")}
      >
        {value}
      </dd>
    </div>
  );
}

function refundStateLabel(
  t: Translate,
  state: CustomerChargeView["refund_state"],
) {
  if (state === "fully_refunded") {
    return t({
      en: "Fully refunded",
      "zh-CN": "已全额退款",
      ja: "全額返金済み",
      ko: "전액 환불됨",
    });
  }
  if (state === "partially_refunded") {
    return t({
      en: "Partially refunded",
      "zh-CN": "部分退款",
      ja: "一部返金済み",
      ko: "일부 환불됨",
    });
  }
  return t({
    en: "Refundable",
    "zh-CN": "可退款",
    ja: "返金可能",
    ko: "환불 가능",
  });
}

function refundReasonLabel(t: Translate, reasonCode: string) {
  const labels: Record<string, ReturnType<Translate>> = {
    customer_request: t({
      en: "Customer request",
      "zh-CN": "客户申请",
      ja: "顧客からの依頼",
      ko: "고객 요청",
    }),
    service_failure: t({
      en: "Service failure",
      "zh-CN": "服务失败",
      ja: "サービス障害",
      ko: "서비스 실패",
    }),
    billing_correction: t({
      en: "Billing correction",
      "zh-CN": "计费更正",
      ja: "請求訂正",
      ko: "청구 정정",
    }),
    fraud_dispute: t({
      en: "Fraud or dispute",
      "zh-CN": "欺诈或争议",
      ja: "不正利用または異議申し立て",
      ko: "사기 또는 분쟁",
    }),
    provider_refund_pass_through: t({
      en: "Provider refund pass-through",
      "zh-CN": "上游退款转付",
      ja: "プロバイダー返金の転送",
      ko: "공급자 환불 전달",
    }),
    other: t({
      en: "Other",
      "zh-CN": "其他",
      ja: "その他",
      ko: "기타",
    }),
  };
  return labels[reasonCode] ?? reasonCode;
}

async function responseMessage(response: Response, t: Translate) {
  try {
    const body = (await response.json()) as {
      error?: { message?: string };
      message?: string;
    };
    return (
      body.error?.message ??
      body.message ??
      t(
        {
          en: "Request failed ({status})",
          "zh-CN": "请求失败（{status}）",
          ja: "リクエストに失敗しました（{status}）",
          ko: "요청 실패({status})",
        },
        { status: response.status },
      )
    );
  } catch {
    return t(
      {
        en: "Request failed ({status})",
        "zh-CN": "请求失败（{status}）",
        ja: "リクエストに失敗しました（{status}）",
        ko: "요청 실패({status})",
      },
      { status: response.status },
    );
  }
}
