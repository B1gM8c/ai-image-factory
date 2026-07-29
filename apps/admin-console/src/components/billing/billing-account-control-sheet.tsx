"use client";

import { useEffect, useMemo, useState } from "react";
import {
  ArrowLeft,
  ChevronLeft,
  ChevronRight,
  LoaderCircle,
  Save,
  Search,
  SlidersHorizontal,
} from "lucide-react";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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
  BillingAccountControlList,
  BillingAccountControlView,
  BillingOrganizationAccountView,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

const PAGE_SIZE = 20;

export function BillingAccountControlSheet({
  open,
  onOpenChange,
  onUpdated,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onUpdated: () => void;
}) {
  const [currency, setCurrency] = useState("USD");
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [cursors, setCursors] = useState<Array<string | null>>([null]);
  const [selected, setSelected] =
    useState<BillingOrganizationAccountView | null>(null);
  const [limit, setLimit] = useState("");
  const [reason, setReason] = useState("");
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    const timeout = window.setTimeout(
      () => setDebouncedSearch(search.trim()),
      250,
    );
    return () => window.clearTimeout(timeout);
  }, [search]);

  useEffect(() => {
    setCursors([null]);
  }, [currency, debouncedSearch]);

  useEffect(() => {
    if (open) return;
    setSelected(null);
    setSaveError(null);
    setReason("");
  }, [open]);

  const cursor = cursors[cursors.length - 1];
  const endpoint = useMemo(() => {
    const params = new URLSearchParams({
      currency,
      limit: PAGE_SIZE.toString(),
    });
    if (debouncedSearch) params.set("query", debouncedSearch);
    if (cursor) params.set("after", cursor);
    return `/admin/v1/billing/accounts?${params.toString()}`;
  }, [currency, cursor, debouncedSearch]);
  const query = useAdminQuery<BillingAccountControlList>(endpoint, open);

  function selectAccount(item: BillingOrganizationAccountView) {
    setSelected(item);
    setLimit(microsToDecimal(item.account.credit_limit_micros));
    setReason("");
    setSaveError(null);
  }

  function closeOrBack() {
    if (selected) {
      setSelected(null);
      setSaveError(null);
      return;
    }
    onOpenChange(false);
  }

  async function save() {
    if (!selected) return;
    const creditLimitMicros = decimalToMicros(limit, { allowZero: true });
    if (creditLimitMicros === null) {
      setSaveError("请输入不小于 0、最多 6 位小数的金额");
      return;
    }
    if (reason.trim().length < 3) {
      setSaveError("请填写至少 3 个字符的变更原因");
      return;
    }
    setSaving(true);
    setSaveError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/admin/v1/billing/accounts/${encodeURIComponent(
          selected.organization_id,
        )}/${encodeURIComponent(currency)}`,
        {
          method: "PUT",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            credit_limit_micros: creditLimitMicros,
            expected_control_version: selected.account.control_version,
            reason: reason.trim(),
          }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      const account = (await response.json()) as BillingAccountControlView;
      setSelected((current) => (current ? { ...current, account } : current));
      query.retry();
      onUpdated();
      toast.success("组织限额已更新");
      setSelected(null);
    } catch (caught) {
      setSaveError(caught instanceof Error ? caught.message : "组织限额保存失败");
    } finally {
      setSaving(false);
    }
  }

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="flex h-full w-full flex-col gap-0 overflow-hidden p-0 sm:max-w-4xl">
        <SheetHeader className="shrink-0 border-b px-5 py-5 pr-12 text-left sm:px-6">
          <div className="flex min-w-0 items-start gap-2">
            {selected ? (
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="-ml-2 mt-[-6px] shrink-0"
                aria-label="返回组织列表"
                onClick={() => setSelected(null)}
              >
                <ArrowLeft aria-hidden="true" />
              </Button>
            ) : null}
            <div className="min-w-0">
              <SheetTitle className="truncate">
                {selected?.display_name ?? "组织限额"}
              </SheetTitle>
              <SheetDescription className="truncate">
                {selected
                  ? `${selected.organization_id} · ${currency}`
                  : "平台计费准入"}
              </SheetDescription>
            </div>
          </div>
        </SheetHeader>

        {selected ? (
          <AccountEditor
            item={selected}
            currency={currency}
            limit={limit}
            reason={reason}
            error={saveError}
            saving={saving}
            onLimitChange={setLimit}
            onReasonChange={setReason}
          />
        ) : (
          <AccountList
            currency={currency}
            search={search}
            query={query}
            page={cursors.length}
            onCurrencyChange={setCurrency}
            onSearchChange={setSearch}
            onSelect={selectAccount}
            onPrevious={() =>
              setCursors((current) =>
                current.length > 1 ? current.slice(0, -1) : current,
              )
            }
            onNext={() => {
              const next = query.data?.next_after;
              if (!next) return;
              setCursors((current) => [...current, next]);
            }}
          />
        )}

        {selected ? (
          <SheetFooter className="shrink-0 gap-2 border-t bg-background px-5 py-4 sm:px-6">
            <Button
              type="button"
              variant="outline"
              onClick={closeOrBack}
              disabled={saving}
            >
              取消
            </Button>
            <Button type="button" onClick={() => void save()} disabled={saving}>
              {saving ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <Save aria-hidden="true" />
              )}
              保存限额
            </Button>
          </SheetFooter>
        ) : null}
      </SheetContent>
    </Sheet>
  );
}

function AccountList({
  currency,
  search,
  query,
  page,
  onCurrencyChange,
  onSearchChange,
  onSelect,
  onPrevious,
  onNext,
}: {
  currency: string;
  search: string;
  query: ReturnType<typeof useAdminQuery<BillingAccountControlList>>;
  page: number;
  onCurrencyChange: (value: string) => void;
  onSearchChange: (value: string) => void;
  onSelect: (item: BillingOrganizationAccountView) => void;
  onPrevious: () => void;
  onNext: () => void;
}) {
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="flex shrink-0 flex-col gap-3 border-b px-5 py-4 sm:flex-row sm:px-6">
        <div className="relative min-w-0 flex-1">
          <Search
            className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
            aria-hidden="true"
          />
          <Input
            className="pl-9"
            value={search}
            onChange={(event) => onSearchChange(event.target.value)}
            placeholder="搜索组织名称或 ID"
            aria-label="搜索组织"
          />
        </div>
        <Select value={currency} onValueChange={onCurrencyChange}>
          <SelectTrigger className="w-full sm:w-28" aria-label="选择币种">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="USD">USD</SelectItem>
            <SelectItem value="CNY">CNY</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="min-h-0 flex-1 overflow-auto">
        <Table className="min-w-[760px]">
          <TableHeader>
            <TableRow>
              <TableHead className="pl-6">组织</TableHead>
              <TableHead>状态</TableHead>
              <TableHead className="text-right">限额</TableHead>
              <TableHead className="text-right">可用</TableHead>
              <TableHead className="pr-6 text-right">操作</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {query.loading ? (
              <TableRow>
                <TableCell
                  colSpan={5}
                  className="h-40 text-center text-muted-foreground"
                >
                  <LoaderCircle
                    className="mx-auto mb-2 size-5 animate-spin"
                    aria-hidden="true"
                  />
                  正在加载组织
                </TableCell>
              </TableRow>
            ) : null}
            {!query.loading && query.error ? (
              <TableRow>
                <TableCell colSpan={5} className="h-40 text-center">
                  <p className="font-medium">组织限额加载失败</p>
                  <p className="mt-1 text-sm text-muted-foreground">
                    {query.error.message}
                  </p>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    className="mt-3"
                    onClick={query.retry}
                  >
                    重新加载
                  </Button>
                </TableCell>
              </TableRow>
            ) : null}
            {!query.loading && !query.error && query.data?.data.length === 0 ? (
              <TableRow>
                <TableCell
                  colSpan={5}
                  className="h-40 text-center text-muted-foreground"
                >
                  没有匹配的组织
                </TableCell>
              </TableRow>
            ) : null}
            {query.data?.data.map((item) => (
              <TableRow key={item.organization_id}>
                <TableCell className="max-w-[320px] pl-6">
                  <p className="truncate font-medium">{item.display_name}</p>
                  <p className="truncate font-mono text-xs text-muted-foreground">
                    {item.organization_id}
                  </p>
                </TableCell>
                <TableCell>
                  <Badge variant={item.account.configured ? "secondary" : "outline"}>
                    {item.account.configured ? "已配置" : "未配置"}
                  </Badge>
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {formatMoneyMicros(
                    item.account.credit_limit_micros,
                    item.account.currency,
                  )}
                </TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {formatMoneyMicros(
                    item.account.available_micros,
                    item.account.currency,
                  )}
                </TableCell>
                <TableCell className="pr-6 text-right">
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => onSelect(item)}
                  >
                    <SlidersHorizontal aria-hidden="true" />
                    设置
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>

      <div className="flex shrink-0 items-center justify-between border-t px-5 py-3 sm:px-6">
        <span className="text-sm text-muted-foreground">第 {page} 页</span>
        <div className="flex gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={onPrevious}
            disabled={page === 1 || query.refreshing}
          >
            <ChevronLeft aria-hidden="true" />
            上一页
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={onNext}
            disabled={!query.data?.has_more || query.refreshing}
          >
            下一页
            <ChevronRight aria-hidden="true" />
          </Button>
        </div>
      </div>
    </div>
  );
}

function AccountEditor({
  item,
  currency,
  limit,
  reason,
  error,
  saving,
  onLimitChange,
  onReasonChange,
}: {
  item: BillingOrganizationAccountView;
  currency: string;
  limit: string;
  reason: string;
  error: string | null;
  saving: boolean;
  onLimitChange: (value: string) => void;
  onReasonChange: (value: string) => void;
}) {
  const account = item.account;
  return (
    <div className="min-h-0 flex-1 overflow-y-auto px-5 py-6 sm:px-6">
      <div className="grid gap-x-8 gap-y-5 rounded-md bg-muted/30 p-4 sm:grid-cols-3">
        <AccountMetric
          label="消费上限"
          value={formatMoneyMicros(account.credit_limit_micros, currency)}
        />
        <AccountMetric
          label="已占用"
          value={formatMoneyMicros(account.held_micros, currency)}
        />
        <AccountMetric
          label="累计扣费"
          value={formatMoneyMicros(account.captured_micros, currency)}
        />
        <AccountMetric
          label="已退款"
          value={formatMoneyMicros(account.refunded_micros, currency)}
        />
        <AccountMetric
          label="净支出"
          value={formatMoneyMicros(
            (
              parseMicros(account.captured_micros) -
              parseMicros(account.refunded_micros)
            ).toString(),
            currency,
          )}
        />
        <AccountMetric
          label="可用"
          value={formatMoneyMicros(account.available_micros, currency)}
        />
      </div>

      <div className="mt-7 grid gap-6">
        <div className="grid gap-2">
          <Label htmlFor="billing-credit-limit">新的消费上限</Label>
          <div className="flex">
            <span className="flex h-9 items-center rounded-l-md border border-r-0 bg-muted px-3 text-sm text-muted-foreground">
              {currency}
            </span>
            <Input
              id="billing-credit-limit"
              className="rounded-l-none font-mono tabular-nums"
              inputMode="decimal"
              value={limit}
              onChange={(event) => onLimitChange(event.target.value)}
              disabled={saving}
            />
          </div>
          <p className="text-xs text-muted-foreground">
            设为 0 将停止该组织的新计费请求；已有占用和已结算金额不会被回退。
          </p>
        </div>

        <div className="grid gap-2">
          <Label htmlFor="billing-credit-reason">变更原因</Label>
          <Textarea
            id="billing-credit-reason"
            value={reason}
            onChange={(event) => onReasonChange(event.target.value)}
            placeholder="例如：财务审批单 FIN-2026-042"
            maxLength={500}
            disabled={saving}
          />
        </div>

        {error ? (
          <p className="text-sm text-destructive" role="alert">
            {error}
          </p>
        ) : null}

        <dl className="grid gap-3 border-t pt-5 text-sm sm:grid-cols-2">
          <Definition label="控制版本" value={account.control_version} mono />
          <Definition
            label="最后更新"
            value={formatDateTime(account.updated_at_ms)}
          />
        </dl>
      </div>
    </div>
  );
}

function AccountMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="mt-1 break-words font-mono text-base font-medium tabular-nums">
        {value}
      </p>
    </div>
  );
}

function parseMicros(value: string) {
  try {
    return BigInt(value);
  } catch {
    return 0n;
  }
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
    <div className="grid gap-1">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className={mono ? "font-mono tabular-nums" : ""}>{value}</dd>
    </div>
  );
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
