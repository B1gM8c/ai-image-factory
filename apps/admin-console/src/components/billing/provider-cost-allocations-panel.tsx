"use client";

import { useEffect, useMemo, useState } from "react";
import {
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  Eye,
  FileCheck2,
  LoaderCircle,
  LockKeyhole,
  Plus,
  RefreshCw,
} from "lucide-react";
import { toast } from "sonner";
import { AdminQueryError, AdminQuerySkeleton } from "@/components/admin-query-state";
import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
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
import {
  decimalToMicros,
  formatDateTime,
  formatMoneyMicros,
} from "@/lib/admin/format";
import type {
  CloseProviderCostAllocationRequest,
  CreateProviderCostAllocationDraftRequest,
  PreviewProviderCostAllocationRequest,
  PriceBook,
  PriceBookCatalog,
  PriceBookVersion,
  ProviderAccountView,
  ProviderAccountsSnapshot,
  ProviderCostAllocationBasis,
  ProviderCostAllocationDetail,
  ProviderCostAllocationLine,
  ProviderCostAllocationLinePreview,
  ProviderCostAllocationList,
  ProviderCostAllocationPreview,
  ProviderCostAllocationState,
  ProviderCostAllocationSummary,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

const LIST_ENDPOINT = "/admin/v1/billing/provider-cost-allocation-pools";
const PREVIEW_ENDPOINT =
  "/api/gateway/admin/v1/billing/provider-cost-allocation-pools/preview";
const CREATE_ENDPOINT =
  "/api/gateway/admin/v1/billing/provider-cost-allocation-pools";
const PAGE_SIZE = 50;

type StateFilter = "all" | ProviderCostAllocationState;

type PriceVersionOption = {
  book: PriceBook;
  version: PriceBookVersion;
};

type DraftForm = {
  providerAccountId: string;
  priceBookVersionId: string;
  periodStart: string;
  periodEnd: string;
  totalAmount: string;
  allocationBasis: ProviderCostAllocationBasis;
};

type CloseForm = {
  sourceKind: CloseProviderCostAllocationRequest["source_kind"];
  sourceReference: string;
  sourceEvidenceHash: string;
};

export function ProviderCostAllocationsPanel({
  enabled,
}: {
  enabled: boolean;
}) {
  const [state, setState] = useState<StateFilter>("draft");
  const [providerId, setProviderId] = useState("all");
  const [providerAccountId, setProviderAccountId] = useState("all");
  const [currency, setCurrency] = useState("all");
  const [cursors, setCursors] = useState<Array<string | null>>([null]);
  const [selectedPoolId, setSelectedPoolId] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);

  useEffect(() => {
    setCursors([null]);
  }, [currency, providerAccountId, providerId, state]);

  const cursor = cursors[cursors.length - 1];
  const endpoint = useMemo(() => {
    const params = new URLSearchParams({ limit: PAGE_SIZE.toString() });
    if (state !== "all") params.set("state", state);
    if (providerId !== "all") params.set("provider_id", providerId);
    if (providerAccountId !== "all") {
      params.set("provider_account_id", providerAccountId);
    }
    if (currency !== "all") params.set("currency", currency);
    if (cursor) params.set("after", cursor);
    return `${LIST_ENDPOINT}?${params.toString()}`;
  }, [currency, cursor, providerAccountId, providerId, state]);

  const query = useAdminQuery<ProviderCostAllocationList>(endpoint, enabled);
  const accountsQuery = useAdminQuery<ProviderAccountsSnapshot>(
    "/admin/v1/provider-accounts",
    enabled,
  );
  const priceBooksQuery = useAdminQuery<PriceBookCatalog>(
    "/admin/v1/pricing/price-books",
    enabled,
  );

  const accounts = accountsQuery.data?.accounts ?? [];
  const accountNames = useMemo(
    () =>
      new Map(
        accounts.map((account) => [
          account.provider_account_id,
          accountLabel(account),
        ]),
      ),
    [accounts],
  );
  const priceVersions = useMemo(
    () => allocationPriceVersions(priceBooksQuery.data),
    [priceBooksQuery.data],
  );
  const versionNames = useMemo(
    () =>
      new Map(
        priceVersions.map(({ book, version }) => [
          version.price_book_version_id,
          priceVersionLabel(book, version),
        ]),
      ),
    [priceVersions],
  );
  const providers = useMemo(
    () =>
      unique(
        accounts.map((account) => account.provider_id).filter(Boolean),
      ),
    [accounts],
  );
  const accountOptions = useMemo(
    () =>
      providerId === "all"
        ? accounts
        : accounts.filter((account) => account.provider_id === providerId),
    [accounts, providerId],
  );
  const currencies = useMemo(
    () => unique(priceVersions.map(({ book }) => book.currency.toUpperCase())),
    [priceVersions],
  );

  useEffect(() => {
    if (
      providerAccountId !== "all" &&
      !accountOptions.some(
        (account) => account.provider_account_id === providerAccountId,
      )
    ) {
      setProviderAccountId("all");
    }
  }, [accountOptions, providerAccountId]);

  return (
    <section className="min-w-0 space-y-4">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <h2 className="text-base font-semibold">Provider 成本分摊</h2>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            将合同、订阅或账单总额分配到成功产物；确认后形成不可变成本凭证。
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            type="button"
            variant="outline"
            size="icon"
            aria-label="刷新分摊草稿"
            onClick={query.retry}
            disabled={query.refreshing}
          >
            <RefreshCw
              className={query.refreshing ? "animate-spin" : ""}
              aria-hidden="true"
            />
          </Button>
          <Button type="button" onClick={() => setCreateOpen(true)}>
            <Plus aria-hidden="true" />
            新建分摊草稿
          </Button>
        </div>
      </div>

      <div className="flex flex-col gap-2 lg:flex-row lg:items-center">
        <Select
          value={state}
          onValueChange={(value) => setState(value as StateFilter)}
        >
          <SelectTrigger className="w-full lg:w-40" aria-label="筛选分摊状态">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="draft">草稿</SelectItem>
            <SelectItem value="closed">已关闭</SelectItem>
            <SelectItem value="all">全部状态</SelectItem>
          </SelectContent>
        </Select>
        <Select value={providerId} onValueChange={setProviderId}>
          <SelectTrigger className="w-full lg:w-44" aria-label="筛选 Provider">
            <SelectValue placeholder="全部 Provider" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部 Provider</SelectItem>
            {providers.map((provider) => (
              <SelectItem key={provider} value={provider}>
                {providerLabel(provider)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={providerAccountId} onValueChange={setProviderAccountId}>
          <SelectTrigger className="w-full lg:w-64" aria-label="筛选 Provider 账户">
            <SelectValue placeholder="全部账户" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部账户</SelectItem>
            {accountOptions.map((account) => (
              <SelectItem
                key={account.provider_account_id}
                value={account.provider_account_id}
              >
                {accountLabel(account)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={currency} onValueChange={setCurrency}>
          <SelectTrigger className="w-full lg:w-36" aria-label="筛选币种">
            <SelectValue placeholder="全部币种" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部币种</SelectItem>
            {currencies.map((item) => (
              <SelectItem key={item} value={item}>
                {item}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {query.loading ? <AdminQuerySkeleton rows={7} /> : null}
      {!query.loading && query.error && !query.data ? (
        <AdminQueryError error={query.error} retry={query.retry} />
      ) : null}
      {query.data ? (
        <>
          <AllocationTable
            rows={query.data.data}
            accountNames={accountNames}
            versionNames={versionNames}
            onSelect={(row) =>
              setSelectedPoolId(row.provider_cost_allocation_pool_id)
            }
          />
          <div className="flex items-center justify-between gap-4 text-sm text-muted-foreground">
            <span>第 {cursors.length} 页</span>
            <div className="flex items-center gap-2">
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
        </>
      ) : null}

      <AllocationDetailSheet
        poolId={selectedPoolId}
        accountNames={accountNames}
        versionNames={versionNames}
        onChanged={query.retry}
        onOpenChange={(open) => {
          if (!open) setSelectedPoolId(null);
        }}
      />
      <CreateAllocationDialog
        open={createOpen}
        accounts={accounts}
        priceVersions={priceVersions}
        metadataLoading={accountsQuery.loading || priceBooksQuery.loading}
        metadataUnavailable={Boolean(
          (!accountsQuery.data && accountsQuery.error) ||
            (!priceBooksQuery.data && priceBooksQuery.error),
        )}
        onOpenChange={setCreateOpen}
        onCreated={() => {
          setCursors([null]);
          query.retry();
        }}
      />
    </section>
  );
}

function AllocationTable({
  rows,
  accountNames,
  versionNames,
  onSelect,
}: {
  rows: ProviderCostAllocationSummary[];
  accountNames: Map<string, string>;
  versionNames: Map<string, string>;
  onSelect: (row: ProviderCostAllocationSummary) => void;
}) {
  if (rows.length === 0) {
    return (
      <div className="flex min-h-72 flex-col items-center justify-center rounded-md border px-6 text-center">
        <CheckCircle2
          className="size-8 text-muted-foreground"
          aria-hidden="true"
        />
        <h3 className="mt-4 text-sm font-medium">
          当前筛选下没有分摊记录
        </h3>
        <p className="mt-1 max-w-md text-sm text-muted-foreground">
          新建草稿后可核对候选产物，并使用 Provider 账单证据完成闭账。
        </p>
      </div>
    );
  }

  return (
    <div className="min-w-0 overflow-hidden rounded-md border">
      <Table className="min-w-[1040px]">
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">周期</TableHead>
            <TableHead>Provider / 账户</TableHead>
            <TableHead>价格版本</TableHead>
            <TableHead>总额</TableHead>
            <TableHead>候选数</TableHead>
            <TableHead>分摊基准</TableHead>
            <TableHead>状态</TableHead>
            <TableHead className="w-24 pr-4 text-right">操作</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => (
            <TableRow
              key={row.provider_cost_allocation_pool_id}
              className="cursor-pointer"
              onClick={() => onSelect(row)}
            >
              <TableCell className="pl-4">
                <p className="font-medium">
                  {formatPeriod(row.period_start_ms, row.period_end_ms)}
                </p>
                <p className="mt-0.5 text-xs text-muted-foreground">
                  创建于 {formatDateTime(row.created_at_ms)}
                </p>
              </TableCell>
              <TableCell className="max-w-64">
                <p className="truncate font-medium">
                  {providerLabel(row.provider_id)}
                </p>
                <p className="mt-0.5 truncate text-xs text-muted-foreground">
                  {accountNames.get(row.provider_account_id) ??
                    shortId(row.provider_account_id)}
                </p>
              </TableCell>
              <TableCell className="max-w-64">
                <p className="truncate">
                  {versionNames.get(row.price_book_version_id) ??
                    `版本 ${shortId(row.price_book_version_id)}`}
                </p>
              </TableCell>
              <TableCell className="font-mono tabular-nums">
                {formatMoneyMicros(
                  row.total_amount_micros.toString(),
                  row.currency,
                )}
              </TableCell>
              <TableCell className="tabular-nums">
                {row.candidate_count.toLocaleString("zh-CN")}
              </TableCell>
              <TableCell>{basisLabel(row.allocation_basis)}</TableCell>
              <TableCell>
                <AllocationStateBadge state={row.state} />
              </TableCell>
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

function AllocationDetailSheet({
  poolId,
  accountNames,
  versionNames,
  onChanged,
  onOpenChange,
}: {
  poolId: string | null;
  accountNames: Map<string, string>;
  versionNames: Map<string, string>;
  onChanged: () => void;
  onOpenChange: (open: boolean) => void;
}) {
  const query = useAdminQuery<ProviderCostAllocationDetail>(
    poolId
      ? `${LIST_ENDPOINT}/${encodeURIComponent(poolId)}`
      : `${LIST_ENDPOINT}/none`,
    Boolean(poolId),
  );
  const detail =
    query.data?.provider_cost_allocation_pool_id === poolId ? query.data : null;

  return (
    <Sheet open={Boolean(poolId)} onOpenChange={onOpenChange}>
      <SheetContent className="flex h-full w-full min-w-0 flex-col gap-0 overflow-hidden p-0 sm:max-w-3xl">
        <SheetHeader className="shrink-0 border-b px-5 py-5 pr-12 text-left sm:px-6">
          <SheetTitle>Provider 成本分摊</SheetTitle>
          <SheetDescription>
            {detail
              ? `${providerLabel(detail.provider_id)} · ${formatPeriod(
                  detail.period_start_ms,
                  detail.period_end_ms,
                )}`
              : "正在读取分摊草稿"}
          </SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1 overflow-y-auto p-5 sm:p-6">
          {query.loading ? <AdminQuerySkeleton rows={7} /> : null}
          {!query.loading && query.error && !detail ? (
            <AdminQueryError error={query.error} retry={query.retry} />
          ) : null}
          {detail ? (
            <AllocationDetail
              detail={detail}
              accountName={
                accountNames.get(detail.provider_account_id) ??
                detail.provider_account_id
              }
              versionName={
                versionNames.get(detail.price_book_version_id) ??
                detail.price_book_version_id
              }
              onClosed={() => {
                query.retry();
                onChanged();
              }}
            />
          ) : null}
        </div>
      </SheetContent>
    </Sheet>
  );
}

function AllocationDetail({
  detail,
  accountName,
  versionName,
  onClosed,
}: {
  detail: ProviderCostAllocationDetail;
  accountName: string;
  versionName: string;
  onClosed: () => void;
}) {
  const facts = [
    ["状态", <AllocationStateBadge key="state" state={detail.state} />],
    ["Provider", providerLabel(detail.provider_id)],
    ["账户", accountName],
    ["价格版本", versionName],
    ["周期", formatPeriod(detail.period_start_ms, detail.period_end_ms)],
    ["分摊基准", basisLabel(detail.allocation_basis)],
    [
      "总额",
      formatMoneyMicros(
        detail.total_amount_micros.toString(),
        detail.currency,
      ),
    ],
    [
      "已分配",
      formatMoneyMicros(
        detail.allocated_amount_micros.toString(),
        detail.currency,
      ),
    ],
    [
      "残差",
      formatMoneyMicros(
        detail.residual_amount_micros.toString(),
        detail.currency,
      ),
    ],
    ["候选数", detail.candidate_count.toLocaleString("zh-CN")],
  ] as const;

  return (
    <div className="min-w-0 space-y-6">
      <AllocationCloseStatus detail={detail} onClosed={onClosed} />
      <dl className="grid gap-x-8 gap-y-4 sm:grid-cols-2">
        {facts.map(([label, value]) => (
          <div key={label} className="min-w-0">
            <dt className="text-xs text-muted-foreground">{label}</dt>
            <dd className="mt-1 min-w-0 break-words text-sm font-medium">
              {value}
            </dd>
          </div>
        ))}
      </dl>
      <div className="min-w-0">
        <div className="mb-3 flex items-center justify-between gap-4">
          <h3 className="text-sm font-semibold">分摊明细</h3>
          <span className="text-xs text-muted-foreground">
            {detail.lines.length.toLocaleString("zh-CN")} 条
          </span>
        </div>
        <AllocationLinesTable
          lines={detail.lines}
          currency={detail.currency}
        />
      </div>
      {detail.closure ? (
        <ClosureEvidence detail={detail} />
      ) : null}
      <dl className="grid gap-x-8 gap-y-4 border-t pt-5 text-xs sm:grid-cols-2">
        <IdFact
          label="草稿 ID"
          value={detail.provider_cost_allocation_pool_id}
        />
        <IdFact label="预览哈希" value={detail.preview_hash} />
        <IdFact label="语义键" value={detail.semantic_key} />
        <div>
          <dt className="text-muted-foreground">创建时间</dt>
          <dd className="mt-1">{formatDateTime(detail.created_at_ms)}</dd>
        </div>
      </dl>
    </div>
  );
}

function AllocationCloseStatus({
  detail,
  onClosed,
}: {
  detail: ProviderCostAllocationDetail;
  onClosed: () => void;
}) {
  if (detail.state === "closed") {
    return (
      <div className="flex items-start gap-3 rounded-md bg-muted/45 px-4 py-3">
        <FileCheck2
          className="mt-0.5 size-4 shrink-0 text-foreground"
          aria-hidden="true"
        />
        <div className="min-w-0">
          <p className="text-sm font-medium">已完成闭账</p>
          <p className="mt-0.5 text-sm text-muted-foreground">
            Receipt 权威、Provider 成本义务与正金额账务分录已封存。
          </p>
        </div>
      </div>
    );
  }
  if (detail.allocation_basis !== "successful_output") {
    return (
      <p className="rounded-md bg-muted/45 px-4 py-3 text-sm text-muted-foreground">
        成功任务基准仅用于分析，不能形成逐 Receipt 的成本权威。请以成功产物重新创建草稿。
      </p>
    );
  }
  if (
    detail.lines.length === 0 ||
    detail.residual_amount_micros.toString() !== "0"
  ) {
    return (
      <p className="rounded-md bg-muted/45 px-4 py-3 text-sm text-muted-foreground">
        当前草稿仍有残差或没有可分摊产物，暂不能闭账。请调整周期或总额后重新创建草稿。
      </p>
    );
  }
  return (
    <div className="flex flex-col gap-3 rounded-md border bg-muted/20 px-4 py-4 sm:flex-row sm:items-center sm:justify-between">
      <div className="flex min-w-0 items-start gap-3">
        <LockKeyhole
          className="mt-0.5 size-4 shrink-0 text-foreground"
          aria-hidden="true"
        />
        <div className="min-w-0">
          <p className="text-sm font-medium">草稿已具备闭账条件</p>
          <p className="mt-0.5 text-sm text-muted-foreground">
            确认后将锁定候选快照并写入不可变成本凭证。
          </p>
        </div>
      </div>
      <CloseAllocationDialog detail={detail} onClosed={onClosed} />
    </div>
  );
}

function CloseAllocationDialog({
  detail,
  onClosed,
}: {
  detail: ProviderCostAllocationDetail;
  onClosed: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [form, setForm] = useState<CloseForm>(() => initialCloseForm());
  const [idempotencyKey, setIdempotencyKey] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  function setDialogOpen(next: boolean) {
    if (saving) return;
    setOpen(next);
    if (next) {
      setForm(initialCloseForm());
      setIdempotencyKey(crypto.randomUUID());
      setError(null);
    }
  }

  function updateCloseForm<Key extends keyof CloseForm>(
    key: Key,
    value: CloseForm[Key],
  ) {
    setForm((current) => ({ ...current, [key]: value }));
    setIdempotencyKey(crypto.randomUUID());
    setError(null);
  }

  async function closeAllocation() {
    const sourceReference = form.sourceReference.trim();
    const sourceEvidenceHash = form.sourceEvidenceHash.trim().toLowerCase();
    if (!sourceReference) {
      setError("请填写 Provider 证据引用");
      return;
    }
    if (!/^[0-9a-f]{64}$/.test(sourceEvidenceHash)) {
      setError("证据 SHA-256 必须是 64 位小写十六进制摘要");
      return;
    }
    const request: CloseProviderCostAllocationRequest = {
      expected_control_version: detail.control_version,
      expected_snapshot_hash: detail.preview_hash,
      source_kind: form.sourceKind,
      source_reference: sourceReference,
      source_evidence_hash: sourceEvidenceHash,
    };
    setSaving(true);
    setError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway${LIST_ENDPOINT}/${encodeURIComponent(
          detail.provider_cost_allocation_pool_id,
        )}`,
        {
          method: "POST",
          headers: { "Idempotency-Key": idempotencyKey },
          body: JSON.stringify(request),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      toast.success("Provider 成本分摊已闭账");
      setOpen(false);
      onClosed();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "闭账失败");
    } finally {
      setSaving(false);
    }
  }

  return (
    <>
      <Button
        type="button"
        size="sm"
        className="shrink-0"
        onClick={() => setDialogOpen(true)}
      >
        <LockKeyhole aria-hidden="true" />
        确认闭账
      </Button>
      <AlertDialog open={open} onOpenChange={setDialogOpen}>
        <AlertDialogContent className="max-h-[calc(100vh-2rem)] w-[calc(100vw-2rem)] max-w-lg overflow-y-auto">
          <AlertDialogHeader>
            <AlertDialogTitle>确认 Provider 成本闭账</AlertDialogTitle>
            <AlertDialogDescription>
              这会为 {detail.candidate_count.toLocaleString("zh-CN")} 个成功产物建立唯一
              Receipt 成本权威。闭账记录、分摊明细和账务凭证创建后不可修改。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <div className="grid gap-4 py-1">
            <div className="grid gap-2">
              <Label htmlFor="allocation-close-source-kind">证据类型</Label>
              <Select
                value={form.sourceKind}
                onValueChange={(value) =>
                  updateCloseForm(
                    "sourceKind",
                    value as CloseForm["sourceKind"],
                  )
                }
                disabled={saving}
              >
                <SelectTrigger id="allocation-close-source-kind">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="provider_invoice">Provider 发票</SelectItem>
                  <SelectItem value="provider_statement">Provider 账单</SelectItem>
                  <SelectItem value="provider_contract">Provider 合同</SelectItem>
                  <SelectItem value="provider_subscription">订阅记录</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="allocation-close-source-reference">
                证据引用
              </Label>
              <Input
                id="allocation-close-source-reference"
                value={form.sourceReference}
                onChange={(event) =>
                  updateCloseForm("sourceReference", event.target.value)
                }
                placeholder="例如 invoice:INV-2026-07-001"
                maxLength={512}
                disabled={saving}
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="allocation-close-source-hash">
                证据 SHA-256
              </Label>
              <Input
                id="allocation-close-source-hash"
                className="font-mono text-xs"
                value={form.sourceEvidenceHash}
                onChange={(event) =>
                  updateCloseForm("sourceEvidenceHash", event.target.value)
                }
                placeholder="64 位小写十六进制摘要"
                maxLength={64}
                spellCheck={false}
                autoCapitalize="none"
                disabled={saving}
              />
            </div>
            <dl className="grid gap-3 rounded-md bg-muted/45 p-4 text-sm sm:grid-cols-2">
              <PreviewMetric
                label="Provider 总额"
                value={formatMoneyMicros(
                  detail.total_amount_micros.toString(),
                  detail.currency,
                )}
              />
              <PreviewMetric
                label="成功产物"
                value={`${detail.candidate_count.toLocaleString("zh-CN")} 个`}
              />
            </dl>
            {error ? (
              <p className="text-sm text-destructive">{error}</p>
            ) : null}
          </div>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={saving}>取消</AlertDialogCancel>
            <Button type="button" onClick={closeAllocation} disabled={saving}>
              {saving ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <LockKeyhole aria-hidden="true" />
              )}
              确认并闭账
            </Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

function ClosureEvidence({ detail }: { detail: ProviderCostAllocationDetail }) {
  const closure = detail.closure;
  if (!closure) return null;
  return (
    <div className="space-y-4 border-t pt-5">
      <div>
        <h3 className="text-sm font-semibold">闭账证据</h3>
        <p className="mt-1 text-xs text-muted-foreground">
          此信息与候选快照、Receipt 权威和账务凭证一并封存。
        </p>
      </div>
      <dl className="grid gap-x-8 gap-y-4 sm:grid-cols-2">
        <div>
          <dt className="text-xs text-muted-foreground">证据类型</dt>
          <dd className="mt-1 text-sm font-medium">
            {sourceKindLabel(closure.source_kind)}
          </dd>
        </div>
        <div>
          <dt className="text-xs text-muted-foreground">闭账时间</dt>
          <dd className="mt-1 text-sm font-medium">
            {formatDateTime(closure.created_at_ms)}
          </dd>
        </div>
        <IdFact label="证据引用" value={closure.source_reference} />
        <IdFact label="证据 SHA-256" value={closure.source_evidence_hash} />
        <IdFact label="操作用户" value={closure.closed_by_user_id} />
        <IdFact label="操作会话" value={closure.closed_by_session_id} />
      </dl>
    </div>
  );
}

function CreateAllocationDialog({
  open,
  accounts,
  priceVersions,
  metadataLoading,
  metadataUnavailable,
  onOpenChange,
  onCreated,
}: {
  open: boolean;
  accounts: ProviderAccountView[];
  priceVersions: PriceVersionOption[];
  metadataLoading: boolean;
  metadataUnavailable: boolean;
  onOpenChange: (open: boolean) => void;
  onCreated: () => void;
}) {
  const [form, setForm] = useState<DraftForm>(() => initialDraftForm());
  const [preview, setPreview] =
    useState<ProviderCostAllocationPreview | null>(null);
  const [idempotencyKey, setIdempotencyKey] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [previewing, setPreviewing] = useState(false);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!open) return;
    setForm(initialDraftForm());
    setPreview(null);
    setIdempotencyKey("");
    setError(null);
    setPreviewing(false);
    setSaving(false);
  }, [open]);

  const selectedAccount =
    accounts.find(
      (account) => account.provider_account_id === form.providerAccountId,
    ) ?? null;
  const availableVersions = useMemo(
    () =>
      selectedAccount
        ? priceVersions.filter(
            ({ book, version }) =>
              effectiveProvider(book, version) === selectedAccount.provider_id,
          )
        : [],
    [priceVersions, selectedAccount],
  );
  const selectedPrice =
    availableVersions.find(
      ({ version }) =>
        version.price_book_version_id === form.priceBookVersionId,
    ) ?? null;

  useEffect(() => {
    if (
      form.priceBookVersionId &&
      !availableVersions.some(
        ({ version }) =>
          version.price_book_version_id === form.priceBookVersionId,
      )
    ) {
      setForm((current) => ({ ...current, priceBookVersionId: "" }));
      setPreview(null);
      setIdempotencyKey("");
    }
  }, [availableVersions, form.priceBookVersionId]);

  function updateForm<Key extends keyof DraftForm>(
    key: Key,
    value: DraftForm[Key],
  ) {
    setForm((current) => ({ ...current, [key]: value }));
    setPreview(null);
    setIdempotencyKey("");
    setError(null);
  }

  function selectProviderAccount(providerAccountId: string) {
    setForm((current) => ({
      ...current,
      providerAccountId,
      priceBookVersionId: "",
    }));
    setPreview(null);
    setIdempotencyKey("");
    setError(null);
  }

  function selectPriceVersion(priceBookVersionId: string) {
    const price = availableVersions.find(
      ({ version }) =>
        version.price_book_version_id === priceBookVersionId,
    );
    setForm((current) => {
      if (!price) return { ...current, priceBookVersionId };
      const currentStart = Date.parse(current.periodStart);
      const currentEnd = Date.parse(current.periodEnd);
      const periodStart = Math.max(
        Number.isFinite(currentStart)
          ? currentStart
          : price.version.effective_from_ms,
        price.version.effective_from_ms,
      );
      const periodEnd = Math.min(
        Number.isFinite(currentEnd) ? currentEnd : Date.now(),
        price.version.effective_until_ms ?? Date.now(),
        Date.now(),
      );
      return {
        ...current,
        priceBookVersionId,
        periodStart: toLocalDateTimeInput(periodStart),
        periodEnd: toLocalDateTimeInput(periodEnd),
      };
    });
    setPreview(null);
    setIdempotencyKey("");
    setError(null);
  }

  async function requestPreview() {
    const request = buildPreviewRequest(form, selectedAccount, selectedPrice);
    if ("error" in request) {
      setError(request.error);
      return;
    }
    setPreviewing(true);
    setError(null);
    try {
      const response = await consoleFetch(PREVIEW_ENDPOINT, {
        method: "POST",
        body: JSON.stringify(request.value),
      });
      if (!response.ok) throw new Error(await responseMessage(response));
      const value = (await response.json()) as ProviderCostAllocationPreview;
      setPreview(value);
      setIdempotencyKey(crypto.randomUUID());
    } catch (caught) {
      setPreview(null);
      setIdempotencyKey("");
      setError(
        caught instanceof Error ? caught.message : "暂时无法预览分摊",
      );
    } finally {
      setPreviewing(false);
    }
  }

  async function createDraft() {
    if (!preview || !idempotencyKey) return;
    const request: CreateProviderCostAllocationDraftRequest = {
      provider_id: preview.provider_id,
      provider_account_id: preview.provider_account_id,
      price_book_version_id: preview.price_book_version_id,
      period_start_ms: preview.period_start_ms,
      period_end_ms: preview.period_end_ms,
      currency: preview.currency,
      total_amount_micros: preview.total_amount_micros,
      allocation_basis: preview.allocation_basis,
      expected_preview_hash: preview.preview_hash,
      idempotency_key: idempotencyKey,
    };
    setSaving(true);
    setError(null);
    try {
      const response = await consoleFetch(CREATE_ENDPOINT, {
        method: "POST",
        body: JSON.stringify(request),
      });
      if (!response.ok) throw new Error(await responseMessage(response));
      toast.success("分摊草稿已创建");
      onOpenChange(false);
      onCreated();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "创建草稿失败");
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[calc(100vh-2rem)] w-[calc(100vw-2rem)] max-w-3xl overflow-y-auto">
        <DialogHeader>
          <DialogTitle>新建 Provider 成本分摊草稿</DialogTitle>
          <DialogDescription>
            先按已封存业务事实生成预览，再使用同一候选集确认创建幂等草稿。
            草稿不会入账；成功产物基准可在核对后闭账。
          </DialogDescription>
        </DialogHeader>

        {metadataLoading ? <AdminQuerySkeleton rows={4} /> : null}
        {!metadataLoading && metadataUnavailable ? (
          <p className="rounded-md bg-destructive/10 px-4 py-3 text-sm text-destructive">
            Provider 账户或价格版本暂时不可用，请刷新页面后重试。
          </p>
        ) : null}
        {!metadataLoading && !metadataUnavailable ? (
          <div className="min-w-0 space-y-5">
            <div className="grid min-w-0 gap-4 sm:grid-cols-2">
              <div className="grid min-w-0 gap-2">
                <Label htmlFor="allocation-account">Provider 账户</Label>
                <Select
                  value={form.providerAccountId}
                  onValueChange={selectProviderAccount}
                  disabled={previewing || saving}
                >
                  <SelectTrigger id="allocation-account" className="w-full">
                    <SelectValue placeholder="选择账户" />
                  </SelectTrigger>
                  <SelectContent>
                    {accounts.map((account) => (
                      <SelectItem
                        key={account.provider_account_id}
                        value={account.provider_account_id}
                      >
                        {providerLabel(account.provider_id)} ·{" "}
                        {accountLabel(account)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="grid min-w-0 gap-2">
                <Label htmlFor="allocation-price-version">分摊价格版本</Label>
                <Select
                  value={form.priceBookVersionId}
                  onValueChange={selectPriceVersion}
                  disabled={!selectedAccount || previewing || saving}
                >
                  <SelectTrigger
                    id="allocation-price-version"
                    className="w-full"
                  >
                    <SelectValue placeholder="选择订阅分摊价格版本" />
                  </SelectTrigger>
                  <SelectContent>
                    {availableVersions.map(({ book, version }) => (
                      <SelectItem
                        key={version.price_book_version_id}
                        value={version.price_book_version_id}
                      >
                        {priceVersionLabel(book, version)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                {selectedAccount && availableVersions.length === 0 ? (
                  <p className="text-xs text-muted-foreground">
                    此 Provider 暂无可用于订阅分摊的价格版本。
                  </p>
                ) : null}
              </div>
              <div className="grid min-w-0 gap-2">
                <Label htmlFor="allocation-period-start">周期开始</Label>
                <Input
                  id="allocation-period-start"
                  type="datetime-local"
                  value={form.periodStart}
                  onChange={(event) =>
                    updateForm("periodStart", event.target.value)
                  }
                  disabled={previewing || saving}
                />
              </div>
              <div className="grid min-w-0 gap-2">
                <Label htmlFor="allocation-period-end">周期结束</Label>
                <Input
                  id="allocation-period-end"
                  type="datetime-local"
                  value={form.periodEnd}
                  max={toLocalDateTimeInput(Date.now())}
                  onChange={(event) =>
                    updateForm("periodEnd", event.target.value)
                  }
                  disabled={previewing || saving}
                />
              </div>
              <div className="grid min-w-0 gap-2">
                <Label htmlFor="allocation-total">Provider 总额</Label>
                <div className="flex min-w-0">
                  <span className="flex h-9 shrink-0 items-center rounded-l-md border border-r-0 bg-muted px-3 text-sm text-muted-foreground">
                    {selectedPrice?.book.currency.toUpperCase() ?? "---"}
                  </span>
                  <Input
                    id="allocation-total"
                    className="min-w-0 rounded-l-none font-mono tabular-nums"
                    inputMode="decimal"
                    placeholder="0.00"
                    value={form.totalAmount}
                    onChange={(event) =>
                      updateForm("totalAmount", event.target.value)
                    }
                    disabled={!selectedPrice || previewing || saving}
                  />
                </div>
              </div>
              <div className="grid min-w-0 gap-2">
                <Label htmlFor="allocation-basis">分摊基准</Label>
                <Select
                  value={form.allocationBasis}
                  onValueChange={(value) =>
                    updateForm(
                      "allocationBasis",
                      value as ProviderCostAllocationBasis,
                    )
                  }
                  disabled={previewing || saving}
                >
                  <SelectTrigger id="allocation-basis" className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="successful_output">
                      成功产物
                    </SelectItem>
                    <SelectItem value="successful_job">成功任务</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            {error ? (
              <p className="rounded-md bg-destructive/10 px-4 py-3 text-sm text-destructive">
                {error}
              </p>
            ) : null}
            {preview ? <PreviewResult preview={preview} /> : null}
          </div>
        ) : null}

        <DialogFooter className="gap-2 sm:gap-0">
          <Button
            type="button"
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={saving}
          >
            取消
          </Button>
          {preview ? (
            <>
              <Button
                type="button"
                variant="outline"
                onClick={requestPreview}
                disabled={previewing || saving}
              >
                {previewing ? (
                  <LoaderCircle className="animate-spin" aria-hidden="true" />
                ) : (
                  <RefreshCw aria-hidden="true" />
                )}
                重新预览
              </Button>
              <Button type="button" onClick={createDraft} disabled={saving}>
                {saving ? (
                  <LoaderCircle className="animate-spin" aria-hidden="true" />
                ) : (
                  <Plus aria-hidden="true" />
                )}
                确认创建草稿
              </Button>
            </>
          ) : (
            <Button
              type="button"
              onClick={requestPreview}
              disabled={
                metadataLoading ||
                metadataUnavailable ||
                previewing ||
                saving
              }
            >
              {previewing ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <Eye aria-hidden="true" />
              )}
              预览分摊
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function PreviewResult({
  preview,
}: {
  preview: ProviderCostAllocationPreview;
}) {
  return (
    <div className="min-w-0 space-y-4 border-t pt-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h3 className="text-sm font-semibold">分摊预览</h3>
          <p className="mt-1 text-xs text-muted-foreground">
            确认时会校验候选集哈希；事实变化后必须重新预览。
          </p>
        </div>
        <Badge variant="secondary">
          {preview.candidate_count.toLocaleString("zh-CN")} 个候选
        </Badge>
      </div>
      <dl className="grid gap-4 rounded-md bg-muted/35 p-4 text-sm sm:grid-cols-3">
        <PreviewMetric
          label="Provider 总额"
          value={formatMoneyMicros(
            preview.total_amount_micros.toString(),
            preview.currency,
          )}
        />
        <PreviewMetric
          label="已分配"
          value={formatMoneyMicros(
            preview.allocated_amount_micros.toString(),
            preview.currency,
          )}
        />
        <PreviewMetric
          label="残差"
          value={formatMoneyMicros(
            preview.residual_amount_micros.toString(),
            preview.currency,
          )}
        />
      </dl>
      {preview.candidate_count === 0 ? (
        <p className="rounded-md bg-muted/40 px-4 py-3 text-sm text-muted-foreground">
          周期内没有符合价格版本与账户范围的成功记录；当前总额全部保留为残差。
        </p>
      ) : (
        <AllocationLinesTable
          lines={preview.lines}
          currency={preview.currency}
          compact
        />
      )}
    </div>
  );
}

function AllocationLinesTable({
  lines,
  currency,
  compact = false,
}: {
  lines: Array<
    ProviderCostAllocationLine | ProviderCostAllocationLinePreview
  >;
  currency: string;
  compact?: boolean;
}) {
  if (lines.length === 0) {
    return (
      <div className="rounded-md border px-4 py-8 text-center text-sm text-muted-foreground">
        没有分摊明细
      </div>
    );
  }
  const visibleLines = compact ? lines.slice(0, 10) : lines;
  return (
    <div className="min-w-0 overflow-hidden rounded-md border">
      <Table className="min-w-[640px]">
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">Job</TableHead>
            <TableHead>Output</TableHead>
            <TableHead>基准</TableHead>
            <TableHead className="pr-4 text-right">分摊金额</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {visibleLines.map((line, index) => (
            <TableRow
              key={
                "provider_cost_allocation_line_id" in line
                  ? line.provider_cost_allocation_line_id
                  : `${line.job_id}:${line.output_id ?? "job"}:${index}`
              }
            >
              <TableCell className="pl-4 font-mono text-xs">
                {shortId(line.job_id)}
              </TableCell>
              <TableCell className="font-mono text-xs">
                {line.output_id ? shortId(line.output_id) : "--"}
              </TableCell>
              <TableCell>
                {line.basis_quantity} {basisUnitLabel(line.basis_unit)}
              </TableCell>
              <TableCell className="pr-4 text-right font-mono tabular-nums">
                {formatMoneyMicros(
                  line.amount_micros.toString(),
                  currency,
                )}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      {compact && lines.length > visibleLines.length ? (
        <p className="border-t px-4 py-2 text-xs text-muted-foreground">
          仅显示前 {visibleLines.length} 条，草稿创建后可查看全部明细。
        </p>
      ) : null}
    </div>
  );
}

function AllocationStateBadge({
  state,
}: {
  state: ProviderCostAllocationState;
}) {
  return state === "draft" ? (
    <Badge variant="secondary">草稿</Badge>
  ) : (
    <Badge variant="outline">已关闭</Badge>
  );
}

function PreviewMetric({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div className="min-w-0">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="mt-1 truncate font-mono font-medium tabular-nums">
        {value}
      </dd>
    </div>
  );
}

function IdFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-all font-mono">{value}</dd>
    </div>
  );
}

function allocationPriceVersions(
  catalog: PriceBookCatalog | null,
): PriceVersionOption[] {
  if (!catalog) return [];
  return catalog.price_books
    .filter(
      (book) =>
        book.purpose === "provider_allocated" && book.state === "active",
    )
    .flatMap((book) =>
      book.versions
        .filter(
          (version) =>
            version.billing_mode === "subscription_allocation" &&
            (version.state === "active" || version.state === "retired"),
        )
        .map((version) => ({ book, version })),
    )
    .sort((left, right) => {
      const providerOrder = (
        effectiveProvider(left.book, left.version) ?? ""
      ).localeCompare(effectiveProvider(right.book, right.version) ?? "");
      if (providerOrder !== 0) return providerOrder;
      return right.version.version - left.version.version;
    });
}

function effectiveProvider(book: PriceBook, version: PriceBookVersion) {
  return version.provider_id ?? book.provider_id;
}

function buildPreviewRequest(
  form: DraftForm,
  account: ProviderAccountView | null,
  price: PriceVersionOption | null,
):
  | { value: PreviewProviderCostAllocationRequest }
  | { error: string } {
  if (!account) return { error: "请选择 Provider 账户" };
  if (!price) return { error: "请选择订阅分摊价格版本" };
  const periodStartMs = Date.parse(form.periodStart);
  const periodEndMs = Date.parse(form.periodEnd);
  if (!Number.isFinite(periodStartMs) || !Number.isFinite(periodEndMs)) {
    return { error: "请填写完整的分摊周期" };
  }
  if (periodEndMs <= periodStartMs) {
    return { error: "周期结束时间必须晚于开始时间" };
  }
  if (periodEndMs > Date.now()) {
    return { error: "周期结束时间不能晚于当前时间" };
  }
  if (
    periodStartMs < price.version.effective_from_ms ||
    (price.version.effective_until_ms !== null &&
      periodEndMs > price.version.effective_until_ms)
  ) {
    return { error: "分摊周期超出所选价格版本的生效区间" };
  }
  const micros = decimalToMicros(form.totalAmount, { allowZero: true });
  if (micros === null) {
    return { error: "Provider 总额必须是非负数，最多保留 6 位小数" };
  }
  return {
    value: {
      provider_id: account.provider_id,
      provider_account_id: account.provider_account_id,
      price_book_version_id: price.version.price_book_version_id,
      period_start_ms: periodStartMs,
      period_end_ms: periodEndMs,
      currency: price.book.currency.toUpperCase(),
      total_amount_micros: micros,
      allocation_basis: form.allocationBasis,
    },
  };
}

function initialDraftForm(): DraftForm {
  const now = new Date();
  const thisMonthStart = new Date(
    now.getFullYear(),
    now.getMonth(),
    1,
    0,
    0,
    0,
    0,
  );
  return {
    providerAccountId: "",
    priceBookVersionId: "",
    periodStart: toLocalDateTimeInput(thisMonthStart.getTime()),
    periodEnd: toLocalDateTimeInput(now.getTime()),
    totalAmount: "",
    allocationBasis: "successful_output",
  };
}

function initialCloseForm(): CloseForm {
  return {
    sourceKind: "provider_invoice",
    sourceReference: "",
    sourceEvidenceHash: "",
  };
}

function toLocalDateTimeInput(value: number) {
  const date = new Date(value);
  const local = new Date(value - date.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
}

function formatPeriod(startMs: number, endMs: number) {
  const formatter = new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return `${formatter.format(new Date(startMs))} – ${formatter.format(
    new Date(Math.max(startMs, endMs - 1)),
  )}`;
}

function priceVersionLabel(book: PriceBook, version: PriceBookVersion) {
  const model =
    version.provider_model_id ?? version.public_model_id ?? "全部模型";
  return `${book.display_name} · v${version.version} · ${model}`;
}

function accountLabel(account: ProviderAccountView) {
  return (
    account.display_name ??
    account.account_email ??
    account.account_key ??
    shortId(account.provider_account_id)
  );
}

function providerLabel(providerId: string) {
  const labels: Record<string, string> = {
    "openai-codex": "Codex",
    codex: "Codex",
    "grok-cli": "Grok",
    "xai-grok": "Grok API",
    grok: "Grok",
    "dreamina-cli": "即梦",
    dreamina: "即梦",
    "volcengine-ark": "火山方舟",
  };
  return labels[providerId] ?? providerId;
}

function basisLabel(basis: ProviderCostAllocationBasis) {
  return basis === "successful_output" ? "成功产物" : "成功任务";
}

function basisUnitLabel(unit: string) {
  if (unit === "successful_output") return "个成功产物";
  if (unit === "successful_job") return "个成功任务";
  return unit;
}

function sourceKindLabel(kind: CloseForm["sourceKind"]) {
  const labels: Record<CloseForm["sourceKind"], string> = {
    provider_invoice: "Provider 发票",
    provider_statement: "Provider 账单",
    provider_contract: "Provider 合同",
    provider_subscription: "订阅记录",
  };
  return labels[kind];
}

function shortId(value: string) {
  return value.length <= 12 ? value : `${value.slice(0, 8)}…${value.slice(-4)}`;
}

function unique(values: string[]) {
  return [...new Set(values)].sort((left, right) =>
    left.localeCompare(right),
  );
}

async function responseMessage(response: Response) {
  try {
    const payload = (await response.json()) as {
      error?: string | { message?: string };
    };
    if (typeof payload.error === "string") return payload.error;
    if (payload.error?.message) return payload.error.message;
  } catch {
    // Fall through to the status-specific operator message.
  }
  if (response.status === 409) {
    return "草稿或候选事实已变化，请刷新后重试";
  }
  if (response.status === 403) return "当前账号没有管理成本分摊的权限";
  if (response.status === 429) return "成本分摊服务繁忙，请稍后重试";
  return "成本分摊服务暂时不可用";
}
