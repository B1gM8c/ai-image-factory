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
import { useI18n } from "@/i18n/locale-provider";
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
type Translate = ReturnType<typeof useI18n>["t"];
type Locale = ReturnType<typeof useI18n>["locale"];

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
  const { t } = useI18n();
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
          accountLabel(t, account),
        ]),
      ),
    [accounts, t],
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
          priceVersionLabel(t, book, version),
        ]),
      ),
    [priceVersions, t],
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
          <h2 className="text-base font-semibold">
            {t({
              en: "Provider cost allocation",
              "zh-CN": "Provider 成本分摊",
              ja: "プロバイダー原価配賦",
              ko: "공급자 비용 배분",
            })}
          </h2>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            {t({
              en: "Allocate contract, subscription, or invoice totals to successful outputs. Closing creates immutable cost evidence.",
              "zh-CN": "将合同、订阅或账单总额分配到成功产物；确认后形成不可变成本凭证。",
              ja: "契約、サブスクリプション、請求書の総額を成功した成果物に配賦します。確定すると変更不可能な原価証憑が作成されます。",
              ko: "계약, 구독 또는 청구서 총액을 성공한 결과물에 배분합니다. 마감하면 변경할 수 없는 비용 증빙이 생성됩니다.",
            })}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            type="button"
            variant="outline"
            size="icon"
            aria-label={t({
              en: "Refresh allocation drafts",
              "zh-CN": "刷新分摊草稿",
              ja: "配賦ドラフトを更新",
              ko: "배분 초안 새로고침",
            })}
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
            {t({
              en: "New allocation draft",
              "zh-CN": "新建分摊草稿",
              ja: "配賦ドラフトを作成",
              ko: "배분 초안 만들기",
            })}
          </Button>
        </div>
      </div>

      <div className="flex flex-col gap-2 lg:flex-row lg:items-center">
        <Select
          value={state}
          onValueChange={(value) => setState(value as StateFilter)}
        >
          <SelectTrigger
            className="w-full lg:w-40"
            aria-label={t({
              en: "Filter by allocation status",
              "zh-CN": "筛选分摊状态",
              ja: "配賦ステータスで絞り込む",
              ko: "배분 상태로 필터링",
            })}
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="draft">{allocationStateLabel(t, "draft")}</SelectItem>
            <SelectItem value="closed">{allocationStateLabel(t, "closed")}</SelectItem>
            <SelectItem value="all">
              {t({
                en: "All statuses",
                "zh-CN": "全部状态",
                ja: "すべての状態",
                ko: "모든 상태",
              })}
            </SelectItem>
          </SelectContent>
        </Select>
        <Select value={providerId} onValueChange={setProviderId}>
          <SelectTrigger
            className="w-full lg:w-44"
            aria-label={t({
              en: "Filter by provider",
              "zh-CN": "筛选 Provider",
              ja: "プロバイダーで絞り込む",
              ko: "공급자로 필터링",
            })}
          >
            <SelectValue
              placeholder={t({
                en: "All providers",
                "zh-CN": "全部 Provider",
                ja: "すべてのプロバイダー",
                ko: "모든 공급자",
              })}
            />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">
              {t({
                en: "All providers",
                "zh-CN": "全部 Provider",
                ja: "すべてのプロバイダー",
                ko: "모든 공급자",
              })}
            </SelectItem>
            {providers.map((provider) => (
              <SelectItem key={provider} value={provider}>
                {providerLabel(t, provider)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={providerAccountId} onValueChange={setProviderAccountId}>
          <SelectTrigger
            className="w-full lg:w-64"
            aria-label={t({
              en: "Filter by provider account",
              "zh-CN": "筛选 Provider 账户",
              ja: "プロバイダーアカウントで絞り込む",
              ko: "공급자 계정으로 필터링",
            })}
          >
            <SelectValue
              placeholder={t({
                en: "All accounts",
                "zh-CN": "全部账户",
                ja: "すべてのアカウント",
                ko: "모든 계정",
              })}
            />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">
              {t({
                en: "All accounts",
                "zh-CN": "全部账户",
                ja: "すべてのアカウント",
                ko: "모든 계정",
              })}
            </SelectItem>
            {accountOptions.map((account) => (
              <SelectItem
                key={account.provider_account_id}
                value={account.provider_account_id}
              >
                {accountLabel(t, account)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={currency} onValueChange={setCurrency}>
          <SelectTrigger
            className="w-full lg:w-36"
            aria-label={t({
              en: "Filter by currency",
              "zh-CN": "筛选币种",
              ja: "通貨で絞り込む",
              ko: "통화로 필터링",
            })}
          >
            <SelectValue
              placeholder={t({
                en: "All currencies",
                "zh-CN": "全部币种",
                ja: "すべての通貨",
                ko: "모든 통화",
              })}
            />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">
              {t({
                en: "All currencies",
                "zh-CN": "全部币种",
                ja: "すべての通貨",
                ko: "모든 통화",
              })}
            </SelectItem>
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
            <span>
              {t(
                {
                  en: "Page {page}",
                  "zh-CN": "第 {page} 页",
                  ja: "{page} ページ",
                  ko: "{page}페이지",
                },
                { page: cursors.length },
              )}
            </span>
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
                {t({ en: "Previous", "zh-CN": "上一页", ja: "前へ", ko: "이전" })}
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
                {t({ en: "Next", "zh-CN": "下一页", ja: "次へ", ko: "다음" })}
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
  const { t, locale } = useI18n();
  if (rows.length === 0) {
    return (
      <div className="flex min-h-72 flex-col items-center justify-center rounded-md border px-6 text-center">
        <CheckCircle2
          className="size-8 text-muted-foreground"
          aria-hidden="true"
        />
        <h3 className="mt-4 text-sm font-medium">
          {t({
            en: "No allocations match these filters",
            "zh-CN": "当前筛选下没有分摊记录",
            ja: "この条件に一致する配賦はありません",
            ko: "현재 필터와 일치하는 배분 기록이 없습니다",
          })}
        </h3>
        <p className="mt-1 max-w-md text-sm text-muted-foreground">
          {t({
            en: "Create a draft to review candidate outputs, then close it with provider billing evidence.",
            "zh-CN": "新建草稿后可核对候选产物，并使用 Provider 账单证据完成闭账。",
            ja: "ドラフトを作成して候補成果物を確認し、プロバイダーの請求証憑で確定します。",
            ko: "초안을 만들어 후보 결과물을 검토한 후 공급자 청구 증빙으로 마감하세요.",
          })}
        </p>
      </div>
    );
  }

  return (
    <div className="min-w-0 overflow-hidden rounded-md border">
      <Table className="min-w-[1040px]">
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">
              {t({ en: "Period", "zh-CN": "周期", ja: "期間", ko: "기간" })}
            </TableHead>
            <TableHead>
              {t({
                en: "Provider / account",
                "zh-CN": "Provider / 账户",
                ja: "プロバイダー / アカウント",
                ko: "공급자 / 계정",
              })}
            </TableHead>
            <TableHead>
              {t({
                en: "Price version",
                "zh-CN": "价格版本",
                ja: "価格バージョン",
                ko: "가격 버전",
              })}
            </TableHead>
            <TableHead>
              {t({ en: "Total", "zh-CN": "总额", ja: "総額", ko: "총액" })}
            </TableHead>
            <TableHead>
              {t({
                en: "Candidates",
                "zh-CN": "候选数",
                ja: "候補数",
                ko: "후보 수",
              })}
            </TableHead>
            <TableHead>
              {t({
                en: "Allocation basis",
                "zh-CN": "分摊基准",
                ja: "配賦基準",
                ko: "배분 기준",
              })}
            </TableHead>
            <TableHead>
              {t({ en: "Status", "zh-CN": "状态", ja: "ステータス", ko: "상태" })}
            </TableHead>
            <TableHead className="w-24 pr-4 text-right">
              {t({ en: "Actions", "zh-CN": "操作", ja: "操作", ko: "작업" })}
            </TableHead>
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
                  {formatPeriod(row.period_start_ms, row.period_end_ms, locale)}
                </p>
                <p className="mt-0.5 text-xs text-muted-foreground">
                  {t(
                    {
                      en: "Created {time}",
                      "zh-CN": "创建于 {time}",
                      ja: "{time} に作成",
                      ko: "{time}에 생성",
                    },
                    { time: formatDateTime(row.created_at_ms, locale) },
                  )}
                </p>
              </TableCell>
              <TableCell className="max-w-64">
                <p className="truncate font-medium">
                  {providerLabel(t, row.provider_id)}
                </p>
                <p className="mt-0.5 truncate text-xs text-muted-foreground">
                  {accountNames.get(row.provider_account_id) ??
                    shortId(row.provider_account_id)}
                </p>
              </TableCell>
              <TableCell className="max-w-64">
                <p className="truncate">
                  {versionNames.get(row.price_book_version_id) ??
                    t(
                      {
                        en: "Version {id}",
                        "zh-CN": "版本 {id}",
                        ja: "バージョン {id}",
                        ko: "버전 {id}",
                      },
                      { id: shortId(row.price_book_version_id) },
                    )}
                </p>
              </TableCell>
              <TableCell className="font-mono tabular-nums">
                {formatMoneyMicros(
                  row.total_amount_micros.toString(),
                  row.currency,
                )}
              </TableCell>
              <TableCell className="tabular-nums">
                {row.candidate_count.toLocaleString(locale)}
              </TableCell>
              <TableCell>{basisLabel(t, row.allocation_basis)}</TableCell>
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
  const { t, locale } = useI18n();
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
          <SheetTitle>
            {t({
              en: "Provider cost allocation",
              "zh-CN": "Provider 成本分摊",
              ja: "プロバイダー原価配賦",
              ko: "공급자 비용 배분",
            })}
          </SheetTitle>
          <SheetDescription>
            {detail
              ? `${providerLabel(t, detail.provider_id)} · ${formatPeriod(
                  detail.period_start_ms,
                  detail.period_end_ms,
                  locale,
                )}`
              : t({
                  en: "Loading allocation draft",
                  "zh-CN": "正在读取分摊草稿",
                  ja: "配賦ドラフトを読み込んでいます",
                  ko: "배분 초안을 불러오는 중",
                })}
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
  const { t, locale } = useI18n();
  const facts = [
    [
      t({ en: "Status", "zh-CN": "状态", ja: "ステータス", ko: "상태" }),
      <AllocationStateBadge key="state" state={detail.state} />,
    ],
    ["Provider", providerLabel(t, detail.provider_id)],
    [t({ en: "Account", "zh-CN": "账户", ja: "アカウント", ko: "계정" }), accountName],
    [
      t({
        en: "Price version",
        "zh-CN": "价格版本",
        ja: "価格バージョン",
        ko: "가격 버전",
      }),
      versionName,
    ],
    [
      t({ en: "Period", "zh-CN": "周期", ja: "期間", ko: "기간" }),
      formatPeriod(detail.period_start_ms, detail.period_end_ms, locale),
    ],
    [
      t({
        en: "Allocation basis",
        "zh-CN": "分摊基准",
        ja: "配賦基準",
        ko: "배분 기준",
      }),
      basisLabel(t, detail.allocation_basis),
    ],
    [
      t({ en: "Total", "zh-CN": "总额", ja: "総額", ko: "총액" }),
      formatMoneyMicros(
        detail.total_amount_micros.toString(),
        detail.currency,
      ),
    ],
    [
      t({ en: "Allocated", "zh-CN": "已分配", ja: "配賦済み", ko: "배분됨" }),
      formatMoneyMicros(
        detail.allocated_amount_micros.toString(),
        detail.currency,
      ),
    ],
    [
      t({ en: "Residual", "zh-CN": "残差", ja: "残差", ko: "잔여액" }),
      formatMoneyMicros(
        detail.residual_amount_micros.toString(),
        detail.currency,
      ),
    ],
    [
      t({
        en: "Candidates",
        "zh-CN": "候选数",
        ja: "候補数",
        ko: "후보 수",
      }),
      detail.candidate_count.toLocaleString(locale),
    ],
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
          <h3 className="text-sm font-semibold">
            {t({
              en: "Allocation details",
              "zh-CN": "分摊明细",
              ja: "配賦明細",
              ko: "배분 상세",
            })}
          </h3>
          <span className="text-xs text-muted-foreground">
            {t(
              {
                en: "{count} lines",
                "zh-CN": "{count} 条",
                ja: "{count} 行",
                ko: "{count}개",
              },
              { count: detail.lines.length.toLocaleString(locale) },
            )}
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
          label={t({
            en: "Draft ID",
            "zh-CN": "草稿 ID",
            ja: "ドラフト ID",
            ko: "초안 ID",
          })}
          value={detail.provider_cost_allocation_pool_id}
        />
        <IdFact
          label={t({
            en: "Preview hash",
            "zh-CN": "预览哈希",
            ja: "プレビューハッシュ",
            ko: "미리보기 해시",
          })}
          value={detail.preview_hash}
        />
        <IdFact
          label={t({
            en: "Semantic key",
            "zh-CN": "语义键",
            ja: "セマンティックキー",
            ko: "시맨틱 키",
          })}
          value={detail.semantic_key}
        />
        <div>
          <dt className="text-muted-foreground">
            {t({
              en: "Created at",
              "zh-CN": "创建时间",
              ja: "作成日時",
              ko: "생성 시간",
            })}
          </dt>
          <dd className="mt-1">
            {formatDateTime(detail.created_at_ms, locale)}
          </dd>
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
  const { t } = useI18n();
  if (detail.state === "closed") {
    return (
      <div className="flex items-start gap-3 rounded-md bg-muted/45 px-4 py-3">
        <FileCheck2
          className="mt-0.5 size-4 shrink-0 text-foreground"
          aria-hidden="true"
        />
        <div className="min-w-0">
          <p className="text-sm font-medium">
            {t({
              en: "Allocation closed",
              "zh-CN": "已完成闭账",
              ja: "確定済み",
              ko: "마감 완료",
            })}
          </p>
          <p className="mt-0.5 text-sm text-muted-foreground">
            {t({
              en: "Receipt authority, provider cost obligations, and positive ledger entries are sealed.",
              "zh-CN": "Receipt 权威、Provider 成本义务与正金额账务分录已封存。",
              ja: "Receipt の原価権威、プロバイダー原価債務、正額の台帳仕訳が封印されています。",
              ko: "Receipt 권위, 공급자 비용 의무 및 양수 원장 항목이 봉인되었습니다.",
            })}
          </p>
        </div>
      </div>
    );
  }
  if (detail.allocation_basis !== "successful_output") {
    return (
      <p className="rounded-md bg-muted/45 px-4 py-3 text-sm text-muted-foreground">
        {t({
          en: "The successful-job basis is for analysis only and cannot establish per-Receipt cost authority. Recreate the draft using successful outputs.",
          "zh-CN": "成功任务基准仅用于分析，不能形成逐 Receipt 的成本权威。请以成功产物重新创建草稿。",
          ja: "成功ジョブ基準は分析専用で、Receipt ごとの原価権威を確立できません。成功成果物を使用してドラフトを作成し直してください。",
          ko: "성공 작업 기준은 분석용일 뿐 Receipt별 비용 권위를 설정할 수 없습니다. 성공 결과물을 기준으로 초안을 다시 만드세요.",
        })}
      </p>
    );
  }
  if (
    detail.lines.length === 0 ||
    detail.residual_amount_micros.toString() !== "0"
  ) {
    return (
      <p className="rounded-md bg-muted/45 px-4 py-3 text-sm text-muted-foreground">
        {t({
          en: "This draft still has a residual or no allocatable outputs, so it cannot be closed. Adjust the period or total and recreate the draft.",
          "zh-CN": "当前草稿仍有残差或没有可分摊产物，暂不能闭账。请调整周期或总额后重新创建草稿。",
          ja: "このドラフトには残差があるか配賦対象がないため、確定できません。期間または総額を調整して作成し直してください。",
          ko: "현재 초안에 잔여액이 있거나 배분할 결과물이 없어 마감할 수 없습니다. 기간 또는 총액을 조정해 초안을 다시 만드세요.",
        })}
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
          <p className="text-sm font-medium">
            {t({
              en: "Draft is ready to close",
              "zh-CN": "草稿已具备闭账条件",
              ja: "ドラフトは確定可能です",
              ko: "초안을 마감할 수 있습니다",
            })}
          </p>
          <p className="mt-0.5 text-sm text-muted-foreground">
            {t({
              en: "Closing locks the candidate snapshot and writes immutable cost evidence.",
              "zh-CN": "确认后将锁定候选快照并写入不可变成本凭证。",
              ja: "確定すると候補スナップショットがロックされ、変更不可能な原価証憑が記録されます。",
              ko: "마감하면 후보 스냅샷이 잠기고 변경할 수 없는 비용 증빙이 기록됩니다.",
            })}
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
  const { t, locale } = useI18n();
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
      setError(
        t({
          en: "Enter a provider evidence reference.",
          "zh-CN": "请填写 Provider 证据引用",
          ja: "プロバイダー証憑の参照情報を入力してください。",
          ko: "공급자 증빙 참조를 입력하세요.",
        }),
      );
      return;
    }
    if (!/^[0-9a-f]{64}$/.test(sourceEvidenceHash)) {
      setError(
        t({
          en: "The evidence SHA-256 must be a 64-character lowercase hexadecimal digest.",
          "zh-CN": "证据 SHA-256 必须是 64 位小写十六进制摘要",
          ja: "証憑の SHA-256 は 64 文字の小文字 16 進ダイジェストである必要があります。",
          ko: "증빙 SHA-256은 64자의 소문자 16진수 다이제스트여야 합니다.",
        }),
      );
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
      if (!response.ok) throw new Error(await responseMessage(response, t));
      toast.success(
        t({
          en: "Provider cost allocation closed",
          "zh-CN": "Provider 成本分摊已闭账",
          ja: "プロバイダー原価配賦を確定しました",
          ko: "공급자 비용 배분이 마감되었습니다",
        }),
      );
      setOpen(false);
      onClosed();
    } catch (caught) {
      setError(
        caught instanceof Error
          ? caught.message
          : t({
              en: "Failed to close allocation",
              "zh-CN": "闭账失败",
              ja: "配賦の確定に失敗しました",
              ko: "배분 마감에 실패했습니다",
            }),
      );
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
        {t({
          en: "Close allocation",
          "zh-CN": "确认闭账",
          ja: "配賦を確定",
          ko: "배분 마감",
        })}
      </Button>
      <AlertDialog open={open} onOpenChange={setDialogOpen}>
        <AlertDialogContent className="max-h-[calc(100vh-2rem)] w-[calc(100vw-2rem)] max-w-lg overflow-y-auto">
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t({
                en: "Confirm provider cost allocation",
                "zh-CN": "确认 Provider 成本闭账",
                ja: "プロバイダー原価配賦を確定",
                ko: "공급자 비용 배분 마감 확인",
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {t(
                {
                  en: "This establishes the sole Receipt cost authority for {count} successful outputs. The closure record, allocation lines, and ledger evidence cannot be changed after creation.",
                  "zh-CN": "这会为 {count} 个成功产物建立唯一 Receipt 成本权威。闭账记录、分摊明细和账务凭证创建后不可修改。",
                  ja: "これにより {count} 件の成功成果物について唯一の Receipt 原価権威が確立されます。確定記録、配賦明細、台帳証憑は作成後に変更できません。",
                  ko: "성공 결과물 {count}개에 대한 유일한 Receipt 비용 권위를 설정합니다. 마감 기록, 배분 내역 및 원장 증빙은 생성 후 변경할 수 없습니다.",
                },
                { count: detail.candidate_count.toLocaleString(locale) },
              )}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <div className="grid gap-4 py-1">
            <div className="grid gap-2">
              <Label htmlFor="allocation-close-source-kind">
                {t({
                  en: "Evidence type",
                  "zh-CN": "证据类型",
                  ja: "証憑タイプ",
                  ko: "증빙 유형",
                })}
              </Label>
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
                  {(
                    [
                      "provider_invoice",
                      "provider_statement",
                      "provider_contract",
                      "provider_subscription",
                    ] as const
                  ).map((kind) => (
                    <SelectItem key={kind} value={kind}>
                      {sourceKindLabel(t, kind)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="allocation-close-source-reference">
                {t({
                  en: "Evidence reference",
                  "zh-CN": "证据引用",
                  ja: "証憑参照",
                  ko: "증빙 참조",
                })}
              </Label>
              <Input
                id="allocation-close-source-reference"
                value={form.sourceReference}
                onChange={(event) =>
                  updateCloseForm("sourceReference", event.target.value)
                }
                placeholder={t({
                  en: "For example: invoice:INV-2026-07-001",
                  "zh-CN": "例如 invoice:INV-2026-07-001",
                  ja: "例: invoice:INV-2026-07-001",
                  ko: "예: invoice:INV-2026-07-001",
                })}
                maxLength={512}
                disabled={saving}
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="allocation-close-source-hash">
                {t({
                  en: "Evidence SHA-256",
                  "zh-CN": "证据 SHA-256",
                  ja: "証憑 SHA-256",
                  ko: "증빙 SHA-256",
                })}
              </Label>
              <Input
                id="allocation-close-source-hash"
                className="font-mono text-xs"
                value={form.sourceEvidenceHash}
                onChange={(event) =>
                  updateCloseForm("sourceEvidenceHash", event.target.value)
                }
                placeholder={t({
                  en: "64-character lowercase hexadecimal digest",
                  "zh-CN": "64 位小写十六进制摘要",
                  ja: "64 文字の小文字 16 進ダイジェスト",
                  ko: "64자 소문자 16진수 다이제스트",
                })}
                maxLength={64}
                spellCheck={false}
                autoCapitalize="none"
                disabled={saving}
              />
            </div>
            <dl className="grid gap-3 rounded-md bg-muted/45 p-4 text-sm sm:grid-cols-2">
              <PreviewMetric
                label={t({
                  en: "Provider total",
                  "zh-CN": "Provider 总额",
                  ja: "プロバイダー総額",
                  ko: "공급자 총액",
                })}
                value={formatMoneyMicros(
                  detail.total_amount_micros.toString(),
                  detail.currency,
                )}
              />
              <PreviewMetric
                label={t({
                  en: "Successful outputs",
                  "zh-CN": "成功产物",
                  ja: "成功成果物",
                  ko: "성공 결과물",
                })}
                value={t(
                  {
                    en: "{count} outputs",
                    "zh-CN": "{count} 个",
                    ja: "{count} 件",
                    ko: "{count}개",
                  },
                  { count: detail.candidate_count.toLocaleString(locale) },
                )}
              />
            </dl>
            {error ? (
              <p className="text-sm text-destructive">{error}</p>
            ) : null}
          </div>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={saving}>
              {t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}
            </AlertDialogCancel>
            <Button type="button" onClick={closeAllocation} disabled={saving}>
              {saving ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <LockKeyhole aria-hidden="true" />
              )}
              {t({
                en: "Confirm and close",
                "zh-CN": "确认并闭账",
                ja: "確認して確定",
                ko: "확인 및 마감",
              })}
            </Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

function ClosureEvidence({ detail }: { detail: ProviderCostAllocationDetail }) {
  const { t, locale } = useI18n();
  const closure = detail.closure;
  if (!closure) return null;
  return (
    <div className="space-y-4 border-t pt-5">
      <div>
        <h3 className="text-sm font-semibold">
          {t({
            en: "Closure evidence",
            "zh-CN": "闭账证据",
            ja: "確定証憑",
            ko: "마감 증빙",
          })}
        </h3>
        <p className="mt-1 text-xs text-muted-foreground">
          {t({
            en: "This information is sealed with the candidate snapshot, Receipt authority, and ledger evidence.",
            "zh-CN": "此信息与候选快照、Receipt 权威和账务凭证一并封存。",
            ja: "この情報は候補スナップショット、Receipt 原価権威、台帳証憑とともに封印されます。",
            ko: "이 정보는 후보 스냅샷, Receipt 권위 및 원장 증빙과 함께 봉인됩니다.",
          })}
        </p>
      </div>
      <dl className="grid gap-x-8 gap-y-4 sm:grid-cols-2">
        <div>
          <dt className="text-xs text-muted-foreground">
            {t({
              en: "Evidence type",
              "zh-CN": "证据类型",
              ja: "証憑タイプ",
              ko: "증빙 유형",
            })}
          </dt>
          <dd className="mt-1 text-sm font-medium">
            {sourceKindLabel(t, closure.source_kind)}
          </dd>
        </div>
        <div>
          <dt className="text-xs text-muted-foreground">
            {t({
              en: "Closed at",
              "zh-CN": "闭账时间",
              ja: "確定日時",
              ko: "마감 시간",
            })}
          </dt>
          <dd className="mt-1 text-sm font-medium">
            {formatDateTime(closure.created_at_ms, locale)}
          </dd>
        </div>
        <IdFact
          label={t({
            en: "Evidence reference",
            "zh-CN": "证据引用",
            ja: "証憑参照",
            ko: "증빙 참조",
          })}
          value={closure.source_reference}
        />
        <IdFact
          label={t({
            en: "Evidence SHA-256",
            "zh-CN": "证据 SHA-256",
            ja: "証憑 SHA-256",
            ko: "증빙 SHA-256",
          })}
          value={closure.source_evidence_hash}
        />
        <IdFact
          label={t({
            en: "Actor",
            "zh-CN": "操作用户",
            ja: "実行ユーザー",
            ko: "처리 사용자",
          })}
          value={closure.closed_by_user_id}
        />
        <IdFact
          label={t({
            en: "Session",
            "zh-CN": "操作会话",
            ja: "実行セッション",
            ko: "처리 세션",
          })}
          value={closure.closed_by_session_id}
        />
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
  const { t } = useI18n();
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
    const request = buildPreviewRequest(t, form, selectedAccount, selectedPrice);
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
      if (!response.ok) throw new Error(await responseMessage(response, t));
      const value = (await response.json()) as ProviderCostAllocationPreview;
      setPreview(value);
      setIdempotencyKey(crypto.randomUUID());
    } catch (caught) {
      setPreview(null);
      setIdempotencyKey("");
      setError(
        caught instanceof Error
          ? caught.message
          : t({
              en: "Allocation preview is temporarily unavailable",
              "zh-CN": "暂时无法预览分摊",
              ja: "配賦プレビューは一時的に利用できません",
              ko: "배분 미리보기를 일시적으로 사용할 수 없습니다",
            }),
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
      if (!response.ok) throw new Error(await responseMessage(response, t));
      toast.success(
        t({
          en: "Allocation draft created",
          "zh-CN": "分摊草稿已创建",
          ja: "配賦ドラフトを作成しました",
          ko: "배분 초안이 생성되었습니다",
        }),
      );
      onOpenChange(false);
      onCreated();
    } catch (caught) {
      setError(
        caught instanceof Error
          ? caught.message
          : t({
              en: "Failed to create allocation draft",
              "zh-CN": "创建草稿失败",
              ja: "配賦ドラフトの作成に失敗しました",
              ko: "배분 초안 생성에 실패했습니다",
            }),
      );
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[calc(100vh-2rem)] w-[calc(100vw-2rem)] max-w-3xl overflow-y-auto">
        <DialogHeader>
          <DialogTitle>
            {t({
              en: "New provider cost allocation draft",
              "zh-CN": "新建 Provider 成本分摊草稿",
              ja: "プロバイダー原価配賦ドラフトを作成",
              ko: "공급자 비용 배분 초안 만들기",
            })}
          </DialogTitle>
          <DialogDescription>
            {t({
              en: "Preview from sealed business facts, then create an idempotent draft from the same candidate set. Drafts do not post to the ledger; successful-output drafts can be closed after review.",
              "zh-CN": "先按已封存业务事实生成预览，再使用同一候选集确认创建幂等草稿。草稿不会入账；成功产物基准可在核对后闭账。",
              ja: "封印済みの業務事実からプレビューを生成し、同じ候補集合で冪等なドラフトを作成します。ドラフトは台帳に計上されず、成功成果物基準は確認後に確定できます。",
              ko: "봉인된 비즈니스 사실로 미리본 후 동일한 후보 집합으로 멱등 초안을 만듭니다. 초안은 원장에 반영되지 않으며 성공 결과물 기준은 검토 후 마감할 수 있습니다.",
            })}
          </DialogDescription>
        </DialogHeader>

        {metadataLoading ? <AdminQuerySkeleton rows={4} /> : null}
        {!metadataLoading && metadataUnavailable ? (
          <p className="rounded-md bg-destructive/10 px-4 py-3 text-sm text-destructive">
            {t({
              en: "Provider accounts or price versions are temporarily unavailable. Refresh the page and try again.",
              "zh-CN": "Provider 账户或价格版本暂时不可用，请刷新页面后重试。",
              ja: "プロバイダーアカウントまたは価格バージョンを一時的に利用できません。ページを更新して再試行してください。",
              ko: "공급자 계정 또는 가격 버전을 일시적으로 사용할 수 없습니다. 페이지를 새로고침한 후 다시 시도하세요.",
            })}
          </p>
        ) : null}
        {!metadataLoading && !metadataUnavailable ? (
          <div className="min-w-0 space-y-5">
            <div className="grid min-w-0 gap-4 sm:grid-cols-2">
              <div className="grid min-w-0 gap-2">
                <Label htmlFor="allocation-account">
                  {t({
                    en: "Provider account",
                    "zh-CN": "Provider 账户",
                    ja: "プロバイダーアカウント",
                    ko: "공급자 계정",
                  })}
                </Label>
                <Select
                  value={form.providerAccountId}
                  onValueChange={selectProviderAccount}
                  disabled={previewing || saving}
                >
                  <SelectTrigger id="allocation-account" className="w-full">
                    <SelectValue
                      placeholder={t({
                        en: "Select account",
                        "zh-CN": "选择账户",
                        ja: "アカウントを選択",
                        ko: "계정 선택",
                      })}
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {accounts.map((account) => (
                      <SelectItem
                        key={account.provider_account_id}
                        value={account.provider_account_id}
                      >
                        {providerLabel(t, account.provider_id)} ·{" "}
                        {accountLabel(t, account)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="grid min-w-0 gap-2">
                <Label htmlFor="allocation-price-version">
                  {t({
                    en: "Allocation price version",
                    "zh-CN": "分摊价格版本",
                    ja: "配賦価格バージョン",
                    ko: "배분 가격 버전",
                  })}
                </Label>
                <Select
                  value={form.priceBookVersionId}
                  onValueChange={selectPriceVersion}
                  disabled={!selectedAccount || previewing || saving}
                >
                  <SelectTrigger
                    id="allocation-price-version"
                    className="w-full"
                  >
                    <SelectValue
                      placeholder={t({
                        en: "Select subscription allocation price version",
                        "zh-CN": "选择订阅分摊价格版本",
                        ja: "サブスクリプション配賦価格バージョンを選択",
                        ko: "구독 배분 가격 버전 선택",
                      })}
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {availableVersions.map(({ book, version }) => (
                      <SelectItem
                        key={version.price_book_version_id}
                        value={version.price_book_version_id}
                      >
                        {priceVersionLabel(t, book, version)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                {selectedAccount && availableVersions.length === 0 ? (
                  <p className="text-xs text-muted-foreground">
                    {t({
                      en: "This provider has no price version available for subscription allocation.",
                      "zh-CN": "此 Provider 暂无可用于订阅分摊的价格版本。",
                      ja: "このプロバイダーにはサブスクリプション配賦に利用できる価格バージョンがありません。",
                      ko: "이 공급자에는 구독 배분에 사용할 수 있는 가격 버전이 없습니다.",
                    })}
                  </p>
                ) : null}
              </div>
              <div className="grid min-w-0 gap-2">
                <Label htmlFor="allocation-period-start">
                  {t({
                    en: "Period start",
                    "zh-CN": "周期开始",
                    ja: "期間開始",
                    ko: "기간 시작",
                  })}
                </Label>
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
                <Label htmlFor="allocation-period-end">
                  {t({
                    en: "Period end",
                    "zh-CN": "周期结束",
                    ja: "期間終了",
                    ko: "기간 종료",
                  })}
                </Label>
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
                <Label htmlFor="allocation-total">
                  {t({
                    en: "Provider total",
                    "zh-CN": "Provider 总额",
                    ja: "プロバイダー総額",
                    ko: "공급자 총액",
                  })}
                </Label>
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
                <Label htmlFor="allocation-basis">
                  {t({
                    en: "Allocation basis",
                    "zh-CN": "分摊基准",
                    ja: "配賦基準",
                    ko: "배분 기준",
                  })}
                </Label>
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
                      {basisLabel(t, "successful_output")}
                    </SelectItem>
                    <SelectItem value="successful_job">
                      {basisLabel(t, "successful_job")}
                    </SelectItem>
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
            {t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}
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
                {t({
                  en: "Refresh preview",
                  "zh-CN": "重新预览",
                  ja: "プレビューを更新",
                  ko: "미리보기 새로고침",
                })}
              </Button>
              <Button type="button" onClick={createDraft} disabled={saving}>
                {saving ? (
                  <LoaderCircle className="animate-spin" aria-hidden="true" />
                ) : (
                  <Plus aria-hidden="true" />
                )}
                {t({
                  en: "Create draft",
                  "zh-CN": "确认创建草稿",
                  ja: "ドラフトを作成",
                  ko: "초안 만들기",
                })}
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
              {t({
                en: "Preview allocation",
                "zh-CN": "预览分摊",
                ja: "配賦をプレビュー",
                ko: "배분 미리보기",
              })}
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
  const { t, locale } = useI18n();
  return (
    <div className="min-w-0 space-y-4 border-t pt-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h3 className="text-sm font-semibold">
            {t({
              en: "Allocation preview",
              "zh-CN": "分摊预览",
              ja: "配賦プレビュー",
              ko: "배분 미리보기",
            })}
          </h3>
          <p className="mt-1 text-xs text-muted-foreground">
            {t({
              en: "The candidate-set hash is verified on confirmation. Preview again if the underlying facts change.",
              "zh-CN": "确认时会校验候选集哈希；事实变化后必须重新预览。",
              ja: "確定時に候補集合のハッシュを検証します。基礎事実が変わった場合は再プレビューが必要です。",
              ko: "확인 시 후보 집합 해시를 검증합니다. 기반 사실이 변경되면 다시 미리봐야 합니다.",
            })}
          </p>
        </div>
        <Badge variant="secondary">
          {t(
            {
              en: "{count} candidates",
              "zh-CN": "{count} 个候选",
              ja: "{count} 件の候補",
              ko: "후보 {count}개",
            },
            { count: preview.candidate_count.toLocaleString(locale) },
          )}
        </Badge>
      </div>
      <dl className="grid gap-4 rounded-md bg-muted/35 p-4 text-sm sm:grid-cols-3">
        <PreviewMetric
          label={t({
            en: "Provider total",
            "zh-CN": "Provider 总额",
            ja: "プロバイダー総額",
            ko: "공급자 총액",
          })}
          value={formatMoneyMicros(
            preview.total_amount_micros.toString(),
            preview.currency,
          )}
        />
        <PreviewMetric
          label={t({
            en: "Allocated",
            "zh-CN": "已分配",
            ja: "配賦済み",
            ko: "배분됨",
          })}
          value={formatMoneyMicros(
            preview.allocated_amount_micros.toString(),
            preview.currency,
          )}
        />
        <PreviewMetric
          label={t({
            en: "Residual",
            "zh-CN": "残差",
            ja: "残差",
            ko: "잔여액",
          })}
          value={formatMoneyMicros(
            preview.residual_amount_micros.toString(),
            preview.currency,
          )}
        />
      </dl>
      {preview.candidate_count === 0 ? (
        <p className="rounded-md bg-muted/40 px-4 py-3 text-sm text-muted-foreground">
          {t({
            en: "No successful records match the price version and account scope in this period. The entire total remains residual.",
            "zh-CN": "周期内没有符合价格版本与账户范围的成功记录；当前总额全部保留为残差。",
            ja: "この期間には価格バージョンとアカウント範囲に一致する成功記録がありません。総額はすべて残差として残ります。",
            ko: "이 기간에 가격 버전 및 계정 범위와 일치하는 성공 기록이 없습니다. 총액 전체가 잔여액으로 남습니다.",
          })}
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
  const { t } = useI18n();
  if (lines.length === 0) {
    return (
      <div className="rounded-md border px-4 py-8 text-center text-sm text-muted-foreground">
        {t({
          en: "No allocation lines",
          "zh-CN": "没有分摊明细",
          ja: "配賦明細はありません",
          ko: "배분 내역이 없습니다",
        })}
      </div>
    );
  }
  const visibleLines = compact ? lines.slice(0, 10) : lines;
  return (
    <div className="min-w-0 overflow-hidden rounded-md border">
      <Table className="min-w-[640px]">
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">
              {t({ en: "Job", "zh-CN": "任务", ja: "ジョブ", ko: "작업" })}
            </TableHead>
            <TableHead>
              {t({ en: "Output", "zh-CN": "输出", ja: "出力", ko: "출력" })}
            </TableHead>
            <TableHead>
              {t({ en: "Basis", "zh-CN": "基准", ja: "基準", ko: "기준" })}
            </TableHead>
            <TableHead className="pr-4 text-right">
              {t({
                en: "Allocated amount",
                "zh-CN": "分摊金额",
                ja: "配賦額",
                ko: "배분 금액",
              })}
            </TableHead>
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
                {line.basis_quantity} {basisUnitLabel(t, line.basis_unit)}
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
          {t(
            {
              en: "Showing the first {count} lines. All lines are available after the draft is created.",
              "zh-CN": "仅显示前 {count} 条，草稿创建后可查看全部明细。",
              ja: "先頭 {count} 行のみ表示しています。ドラフト作成後にすべての明細を確認できます。",
              ko: "처음 {count}개만 표시합니다. 초안 생성 후 모든 내역을 볼 수 있습니다.",
            },
            { count: visibleLines.length },
          )}
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
  const { t } = useI18n();
  return state === "draft" ? (
    <Badge variant="secondary">{allocationStateLabel(t, "draft")}</Badge>
  ) : (
    <Badge variant="outline">{allocationStateLabel(t, "closed")}</Badge>
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
  t: Translate,
  form: DraftForm,
  account: ProviderAccountView | null,
  price: PriceVersionOption | null,
):
  | { value: PreviewProviderCostAllocationRequest }
  | { error: string } {
  if (!account) {
    return {
      error: t({
        en: "Select a provider account.",
        "zh-CN": "请选择 Provider 账户",
        ja: "プロバイダーアカウントを選択してください。",
        ko: "공급자 계정을 선택하세요.",
      }),
    };
  }
  if (!price) {
    return {
      error: t({
        en: "Select a subscription allocation price version.",
        "zh-CN": "请选择订阅分摊价格版本",
        ja: "サブスクリプション配賦価格バージョンを選択してください。",
        ko: "구독 배분 가격 버전을 선택하세요.",
      }),
    };
  }
  const periodStartMs = Date.parse(form.periodStart);
  const periodEndMs = Date.parse(form.periodEnd);
  if (!Number.isFinite(periodStartMs) || !Number.isFinite(periodEndMs)) {
    return {
      error: t({
        en: "Enter a complete allocation period.",
        "zh-CN": "请填写完整的分摊周期",
        ja: "配賦期間をすべて入力してください。",
        ko: "전체 배분 기간을 입력하세요.",
      }),
    };
  }
  if (periodEndMs <= periodStartMs) {
    return {
      error: t({
        en: "The period end must be after the start.",
        "zh-CN": "周期结束时间必须晚于开始时间",
        ja: "期間終了は開始より後に設定してください。",
        ko: "기간 종료는 시작 이후여야 합니다.",
      }),
    };
  }
  if (periodEndMs > Date.now()) {
    return {
      error: t({
        en: "The period end cannot be in the future.",
        "zh-CN": "周期结束时间不能晚于当前时间",
        ja: "期間終了を現在時刻より後には設定できません。",
        ko: "기간 종료는 현재 시간 이후일 수 없습니다.",
      }),
    };
  }
  if (
    periodStartMs < price.version.effective_from_ms ||
    (price.version.effective_until_ms !== null &&
      periodEndMs > price.version.effective_until_ms)
  ) {
    return {
      error: t({
        en: "The allocation period falls outside the selected price version's effective range.",
        "zh-CN": "分摊周期超出所选价格版本的生效区间",
        ja: "配賦期間が選択した価格バージョンの有効期間外です。",
        ko: "배분 기간이 선택한 가격 버전의 유효 범위를 벗어납니다.",
      }),
    };
  }
  const micros = decimalToMicros(form.totalAmount, { allowZero: true });
  if (micros === null) {
    return {
      error: t({
        en: "The provider total must be non-negative with up to 6 decimal places.",
        "zh-CN": "Provider 总额必须是非负数，最多保留 6 位小数",
        ja: "プロバイダー総額は 0 以上で、小数点以下 6 桁までにしてください。",
        ko: "공급자 총액은 0 이상이고 소수점 이하 6자리까지여야 합니다.",
      }),
    };
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

function formatPeriod(startMs: number, endMs: number, locale: Locale) {
  const formatter = new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return `${formatter.format(new Date(startMs))} – ${formatter.format(
    new Date(Math.max(startMs, endMs - 1)),
  )}`;
}

function priceVersionLabel(
  t: Translate,
  book: PriceBook,
  version: PriceBookVersion,
) {
  const model =
    version.provider_model_id ??
    version.public_model_id ??
    t({
      en: "All models",
      "zh-CN": "全部模型",
      ja: "すべてのモデル",
      ko: "모든 모델",
    });
  return `${book.display_name} · v${version.version} · ${model}`;
}

function accountLabel(_t: Translate, account: ProviderAccountView) {
  return (
    account.display_name ??
    account.account_email ??
    account.account_key ??
    shortId(account.provider_account_id)
  );
}

function providerLabel(t: Translate, providerId: string) {
  const labels: Record<string, string> = {
    "openai-codex": "Codex",
    codex: "Codex",
    "grok-cli": "Grok",
    "xai-grok": "Grok API",
    grok: "Grok",
    "dreamina-cli": t({
      en: "Dreamina",
      "zh-CN": "即梦",
      ja: "Dreamina",
      ko: "Dreamina",
    }),
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

function basisLabel(t: Translate, basis: ProviderCostAllocationBasis) {
  return basis === "successful_output"
    ? t({
        en: "Successful outputs",
        "zh-CN": "成功产物",
        ja: "成功成果物",
        ko: "성공 결과물",
      })
    : t({
        en: "Successful jobs",
        "zh-CN": "成功任务",
        ja: "成功ジョブ",
        ko: "성공 작업",
      });
}

function basisUnitLabel(t: Translate, unit: string) {
  if (unit === "successful_output") {
    return t({
      en: "successful outputs",
      "zh-CN": "个成功产物",
      ja: "件の成功成果物",
      ko: "개의 성공 결과물",
    });
  }
  if (unit === "successful_job") {
    return t({
      en: "successful jobs",
      "zh-CN": "个成功任务",
      ja: "件の成功ジョブ",
      ko: "개의 성공 작업",
    });
  }
  return unit;
}

function sourceKindLabel(t: Translate, kind: CloseForm["sourceKind"]) {
  const labels: Record<CloseForm["sourceKind"], string> = {
    provider_invoice: t({
      en: "Provider invoice",
      "zh-CN": "Provider 发票",
      ja: "プロバイダー請求書",
      ko: "공급자 인보이스",
    }),
    provider_statement: t({
      en: "Provider statement",
      "zh-CN": "Provider 账单",
      ja: "プロバイダー明細書",
      ko: "공급자 명세서",
    }),
    provider_contract: t({
      en: "Provider contract",
      "zh-CN": "Provider 合同",
      ja: "プロバイダー契約",
      ko: "공급자 계약",
    }),
    provider_subscription: t({
      en: "Subscription record",
      "zh-CN": "订阅记录",
      ja: "サブスクリプション記録",
      ko: "구독 기록",
    }),
  };
  return labels[kind];
}

function allocationStateLabel(
  t: Translate,
  state: ProviderCostAllocationState,
) {
  return state === "draft"
    ? t({ en: "Draft", "zh-CN": "草稿", ja: "ドラフト", ko: "초안" })
    : t({ en: "Closed", "zh-CN": "已关闭", ja: "確定済み", ko: "마감됨" });
}

function shortId(value: string) {
  return value.length <= 12 ? value : `${value.slice(0, 8)}…${value.slice(-4)}`;
}

function unique(values: string[]) {
  return [...new Set(values)].sort((left, right) =>
    left.localeCompare(right),
  );
}

async function responseMessage(response: Response, t: Translate) {
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
    return t({
      en: "The draft or candidate facts changed. Refresh and try again.",
      "zh-CN": "草稿或候选事实已变化，请刷新后重试",
      ja: "ドラフトまたは候補事実が変更されました。更新して再試行してください。",
      ko: "초안 또는 후보 사실이 변경되었습니다. 새로고침한 후 다시 시도하세요.",
    });
  }
  if (response.status === 403) {
    return t({
      en: "Your account cannot manage cost allocations.",
      "zh-CN": "当前账号没有管理成本分摊的权限",
      ja: "このアカウントには原価配賦を管理する権限がありません。",
      ko: "현재 계정에는 비용 배분 관리 권한이 없습니다.",
    });
  }
  if (response.status === 429) {
    return t({
      en: "The cost allocation service is busy. Try again later.",
      "zh-CN": "成本分摊服务繁忙，请稍后重试",
      ja: "原価配賦サービスが混雑しています。しばらくしてから再試行してください。",
      ko: "비용 배분 서비스가 사용 중입니다. 잠시 후 다시 시도하세요.",
    });
  }
  return t({
    en: "The cost allocation service is temporarily unavailable.",
    "zh-CN": "成本分摊服务暂时不可用",
    ja: "原価配賦サービスは一時的に利用できません。",
    ko: "비용 배분 서비스를 일시적으로 사용할 수 없습니다.",
  });
}
