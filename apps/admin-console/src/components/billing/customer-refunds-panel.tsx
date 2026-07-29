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
          <h2 className="text-base font-semibold">客户扣费与退款</h2>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            原始用量和扣费保持不变，退款以独立冲正交易追加到账本。
          </p>
        </div>
        <Button
          type="button"
          variant="outline"
          size="icon"
          aria-label="刷新退款记录"
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
          <SelectTrigger className="w-full sm:w-48" aria-label="筛选退款状态">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="refundable">可退款</SelectItem>
            <SelectItem value="partially_refunded">部分退款</SelectItem>
            <SelectItem value="fully_refunded">已全额退款</SelectItem>
            <SelectItem value="all">全部扣费</SelectItem>
          </SelectContent>
        </Select>
        <Input
          className="w-full sm:max-w-sm"
          value={tenantId}
          onChange={(event) => setTenantId(event.target.value)}
          placeholder="按组织 ID 精确筛选"
          aria-label="按组织 ID 筛选"
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
          <p className="text-sm text-muted-foreground">第 {cursors.length} 页</p>
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
              上一页
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
              下一页
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
  if (rows.length === 0) {
    return (
      <div className="flex min-h-72 flex-col items-center justify-center rounded-md border px-6 text-center">
        <CheckCircle2 className="size-8 text-muted-foreground" aria-hidden="true" />
        <h3 className="mt-4 text-sm font-medium">当前筛选下没有客户扣费</h3>
        <p className="mt-1 max-w-md text-sm text-muted-foreground">
          已封账的客户扣费会显示在这里，并可查看不可变退款历史。
        </p>
      </div>
    );
  }

  return (
    <div className="overflow-x-auto rounded-md border">
      <Table className="min-w-[920px]">
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">组织</TableHead>
            <TableHead>原始扣费</TableHead>
            <TableHead>已退款</TableHead>
            <TableHead>剩余可退</TableHead>
            <TableHead>状态</TableHead>
            <TableHead>扣费时间</TableHead>
            <TableHead className="w-24 pr-4 text-right">操作</TableHead>
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
              <TableCell>{formatDateTime(row.created_at_ms)}</TableCell>
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
                  查看
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
            <SheetTitle>扣费详情</SheetTitle>
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
                    label="组织"
                    value={tenantNames.get(charge.tenant_id) ?? charge.tenant_id}
                  />
                  <Definition
                    label="状态"
                    value={refundStateLabel(charge.refund_state)}
                  />
                  <Definition
                    label="原始扣费"
                    value={formatMoneyMicros(
                      charge.amount_micros,
                      charge.currency,
                    )}
                    mono
                  />
                  <Definition
                    label="剩余可退"
                    value={formatMoneyMicros(
                      charge.remaining_refundable_micros,
                      charge.currency,
                    )}
                    mono
                  />
                  <Definition label="扣费时间" value={formatDateTime(charge.created_at_ms)} />
                  <Definition label="Job ID" value={charge.job_id} mono />
                </dl>

                <section>
                  <div className="mb-3 flex items-center justify-between gap-3">
                    <h3 className="text-sm font-semibold">退款历史</h3>
                    <span className="text-xs text-muted-foreground">
                      {charge.refunds.length} 笔
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
                {charge.remaining_refundable_micros === "0" ? "已全额退款" : "发起退款"}
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
  if (refunds.length === 0) {
    return (
      <div className="rounded-md border px-4 py-10 text-center text-sm text-muted-foreground">
        尚未发生退款
      </div>
    );
  }
  return (
    <div className="overflow-x-auto rounded-md border">
      <Table className="min-w-[620px]">
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">金额</TableHead>
            <TableHead>原因</TableHead>
            <TableHead>操作人</TableHead>
            <TableHead className="pr-4">时间</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {refunds.map((refund) => (
            <TableRow key={refund.refund_id}>
              <TableCell className="pl-4 font-mono tabular-nums">
                {formatMoneyMicros(refund.amount_micros, currency)}
              </TableCell>
              <TableCell className="max-w-64">
                <p>{refundReasonLabel(refund.reason_code)}</p>
                <p className="mt-0.5 truncate text-xs text-muted-foreground">
                  {refund.reason}
                </p>
              </TableCell>
              <TableCell className="max-w-40 truncate font-mono text-xs">
                {refund.actor_user_id}
              </TableCell>
              <TableCell className="pr-4">
                {formatDateTime(refund.created_at_ms)}
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
      setError("退款金额必须大于 0，且不能超过剩余可退金额");
      return;
    }
    if (reason.trim().length < 3) {
      setError("请填写至少 3 个字符的退款说明");
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
      if (!response.ok) throw new Error(await responseMessage(response));
      toast.success("退款已记入账本");
      onOpenChange(false);
      onSuccess();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "退款失败");
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[calc(100vh-2rem)] overflow-y-auto sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>确认客户退款</DialogTitle>
          <DialogDescription>
            此操作会追加不可变冲正交易，不会删除原始用量或扣费记录。
          </DialogDescription>
        </DialogHeader>

        {charge ? (
          <div className="grid gap-5 py-1">
            <div className="rounded-md bg-muted/40 px-4 py-3 text-sm">
              <div className="flex items-center justify-between gap-4">
                <span className="text-muted-foreground">剩余可退</span>
                <span className="font-mono font-medium tabular-nums">
                  {formatMoneyMicros(
                    charge.remaining_refundable_micros,
                    charge.currency,
                  )}
                </span>
              </div>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="refund-amount">退款金额</Label>
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
              <Label htmlFor="refund-reason-code">退款原因</Label>
              <Select
                value={reasonCode}
                onValueChange={setReasonCode}
                disabled={saving}
              >
                <SelectTrigger id="refund-reason-code">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="customer_request">客户申请</SelectItem>
                  <SelectItem value="service_failure">服务失败</SelectItem>
                  <SelectItem value="billing_correction">计费更正</SelectItem>
                  <SelectItem value="fraud_dispute">欺诈或争议</SelectItem>
                  <SelectItem value="provider_refund_pass_through">
                    上游退款转付
                  </SelectItem>
                  <SelectItem value="other">其他</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="refund-reason">退款说明</Label>
              <Textarea
                id="refund-reason"
                value={reason}
                onChange={(event) => setReason(event.target.value)}
                placeholder="填写工单号或退款依据"
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
            取消
          </Button>
          <Button type="button" onClick={() => void submit()} disabled={saving}>
            {saving ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <RotateCcw aria-hidden="true" />
            )}
            确认退款
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
  if (state === "fully_refunded") {
    return <Badge variant="outline">已全额退款</Badge>;
  }
  if (state === "partially_refunded") {
    return <Badge variant="secondary">部分退款</Badge>;
  }
  return <Badge>可退款</Badge>;
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

function refundStateLabel(state: CustomerChargeView["refund_state"]) {
  if (state === "fully_refunded") return "已全额退款";
  if (state === "partially_refunded") return "部分退款";
  return "可退款";
}

function refundReasonLabel(reasonCode: string) {
  const labels: Record<string, string> = {
    customer_request: "客户申请",
    service_failure: "服务失败",
    billing_correction: "计费更正",
    fraud_dispute: "欺诈或争议",
    provider_refund_pass_through: "上游退款转付",
    other: "其他",
  };
  return labels[reasonCode] ?? reasonCode;
}

async function responseMessage(response: Response) {
  try {
    const body = (await response.json()) as {
      error?: { message?: string };
      message?: string;
    };
    return body.error?.message ?? body.message ?? `请求失败（${response.status}）`;
  } catch {
    return `请求失败（${response.status}）`;
  }
}
