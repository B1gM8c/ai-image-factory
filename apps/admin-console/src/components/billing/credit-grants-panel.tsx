"use client";

import { useEffect, useMemo, useState, type ReactNode } from "react";
import {
  AlertTriangle,
  ChevronLeft,
  ChevronRight,
  Gift,
  LoaderCircle,
  MoreHorizontal,
  Plus,
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
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
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
} from "@/lib/admin/format";
import type {
  BillingAccountControlList,
  CreditGrantList,
  CreditGrantState,
  CreditGrantView,
  OrganizationCreditGrantList,
  OrganizationCreditGrantView,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

const PAGE_SIZE = 50;
type Translate = ReturnType<typeof useI18n>["t"];
type Locale = ReturnType<typeof useI18n>["locale"];

type OrganizationOption = {
  id: string;
  name: string;
};

type CreditGrantRow = CreditGrantView | OrganizationCreditGrantView;

export function CreditGrantsPanel({
  enabled,
  platformOwner,
  organizationId,
  organizations,
}: {
  enabled: boolean;
  platformOwner: boolean;
  organizationId: string | null;
  organizations: OrganizationOption[];
}) {
  const { t, locale } = useI18n();
  const [currency, setCurrency] = useState("USD");
  const [state, setState] = useState<CreditGrantState | "all">("all");
  const [cursors, setCursors] = useState<Array<string | null>>([null]);
  const [issueOpen, setIssueOpen] = useState(false);
  const [revokeTarget, setRevokeTarget] = useState<CreditGrantRow | null>(null);

  useEffect(() => {
    setCursors([null]);
  }, [currency, organizationId, state]);

  const cursor = cursors[cursors.length - 1];
  const endpoint = useMemo(() => {
    const params = new URLSearchParams({
      currency,
      state,
      limit: PAGE_SIZE.toString(),
    });
    if (cursor) params.set("after", cursor);
    if (platformOwner) {
      if (organizationId) params.set("organization_id", organizationId);
      return `/admin/v1/billing/credit-grants?${params.toString()}`;
    }
    if (organizationId) {
      return `/v1/organizations/${encodeURIComponent(
        organizationId,
      )}/billing/credit-grants?${params.toString()}`;
    }
    return "";
  }, [currency, cursor, organizationId, platformOwner, state]);
  const query = useAdminQuery<CreditGrantList | OrganizationCreditGrantList>(
    endpoint,
    enabled && Boolean(platformOwner || organizationId),
  );
  const organizationNames = useMemo(
    () => new Map(organizations.map((organization) => [organization.id, organization.name])),
    [organizations],
  );
  const issueOrganizations = useMemo(
    () =>
      organizationId
        ? organizations.filter(
            (organization) => organization.id === organizationId,
          )
        : organizations,
    [organizationId, organizations],
  );

  return (
    <section className="min-w-0 space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-base font-semibold">
            {t({
              en: "Credit grants",
              "zh-CN": "赠送额度",
              ja: "クレジット付与",
              ko: "크레딧 지급",
            })}
          </h2>
          <p className="mt-1 text-sm text-muted-foreground">
            {t({
              en: "Granted credits are applied before paid balance and consumed by earliest expiration.",
              "zh-CN": "赠送额度优先抵扣用量，并按最早到期顺序使用。",
              ja: "付与クレジットは有料残高より先に適用され、有効期限が早い順に使用されます。",
              ko: "지급 크레딧은 유료 잔액보다 먼저 사용되며 만료일이 빠른 순서로 차감됩니다.",
            })}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {platformOwner ? (
            <Button type="button" onClick={() => setIssueOpen(true)}>
              <Plus aria-hidden="true" />
              {t({
                en: "Issue credits",
                "zh-CN": "发放额度",
                ja: "クレジットを付与",
                ko: "크레딧 지급",
              })}
            </Button>
          ) : null}
          <Button
            type="button"
            variant="outline"
            size="icon"
            aria-label={t({
              en: "Refresh credit grants",
              "zh-CN": "刷新赠送额度",
              ja: "クレジット付与を更新",
              ko: "크레딧 지급 새로고침",
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
      </div>

      {query.data ? (
        <div className="grid overflow-hidden rounded-md border sm:grid-cols-2">
          <SummaryItem
            label={t({
              en: "Available balance",
              "zh-CN": "可用余额",
              ja: "利用可能残高",
              ko: "사용 가능 잔액",
            })}
            value={formatMoneyMicros(
              query.data.summary.available_micros,
              query.data.currency,
            )}
            detail={t(
              {
                en: "As of {time}",
                "zh-CN": "截至 {time}",
                ja: "{time} 時点",
                ko: "{time} 기준",
              },
              { time: formatDateTime(query.data.as_of_ms, locale) },
            )}
          />
          <SummaryItem
            label={t({
              en: "Total granted",
              "zh-CN": "累计到账",
              ja: "累計付与額",
              ko: "누적 지급액",
            })}
            value={formatMoneyMicros(
              query.data.summary.original_amount_micros,
              query.data.currency,
            )}
            detail={t({
              en: "Includes consumed, expired, and revoked credits",
              "zh-CN": "包含已使用、到期和撤销额度",
              ja: "使用済み、期限切れ、取消済みのクレジットを含みます",
              ko: "사용, 만료 및 취소된 크레딧 포함",
            })}
            last
          />
        </div>
      ) : null}

      <div className="flex flex-col gap-2 sm:flex-row">
        <Select value={currency} onValueChange={setCurrency}>
          <SelectTrigger
            className="w-full sm:w-28"
            aria-label={t({
              en: "Select currency",
              "zh-CN": "选择币种",
              ja: "通貨を選択",
              ko: "통화 선택",
            })}
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="USD">USD</SelectItem>
            <SelectItem value="CNY">CNY</SelectItem>
          </SelectContent>
        </Select>
        <Select
          value={state}
          onValueChange={(value) => setState(value as CreditGrantState | "all")}
        >
          <SelectTrigger
            className="w-full sm:w-40"
            aria-label={t({
              en: "Filter by credit status",
              "zh-CN": "筛选额度状态",
              ja: "クレジット状態で絞り込む",
              ko: "크레딧 상태로 필터링",
            })}
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">
              {t({
                en: "All statuses",
                "zh-CN": "全部状态",
                ja: "すべての状態",
                ko: "모든 상태",
              })}
            </SelectItem>
            {(["active", "consuming", "exhausted", "expired", "revoked"] as const).map(
              (value) => (
                <SelectItem key={value} value={value}>
                  {grantStateLabel(t, value)}
                </SelectItem>
              ),
            )}
          </SelectContent>
        </Select>
      </div>

      {query.loading ? <AdminQuerySkeleton rows={7} /> : null}
      {!query.loading && query.error && !query.data ? (
        <AdminQueryError error={query.error} retry={query.retry} />
      ) : null}
      {query.data && query.error ? (
        <div className="flex items-center gap-2 rounded-md border px-3 py-2 text-sm text-muted-foreground">
          <AlertTriangle className="size-4 shrink-0" aria-hidden="true" />
          {t({
            en: "Showing the last successfully synchronized credit snapshot",
            "zh-CN": "当前显示上一次成功同步的额度快照",
            ja: "前回正常に同期されたクレジットのスナップショットを表示しています",
            ko: "마지막으로 동기화된 크레딧 스냅샷을 표시하고 있습니다",
          })}
        </div>
      ) : null}
      {query.data ? (
        <GrantTable
          rows={query.data.data}
          organizationNames={organizationNames}
          showOrganization={!organizationId}
          showSource={platformOwner}
          canRevoke={platformOwner}
          onRevoke={setRevokeTarget}
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
      ) : null}

      <IssueGrantDialog
        open={issueOpen}
        onOpenChange={setIssueOpen}
        organizationId={organizationId}
        organizations={issueOrganizations}
        initialCurrency={currency}
        onIssued={() => {
          if (cursors.length === 1) {
            query.retry();
          } else {
            setCursors([null]);
          }
        }}
      />
      <RevokeGrantDialog
        target={revokeTarget}
        onOpenChange={(open) => {
          if (!open) setRevokeTarget(null);
        }}
        onRevoked={query.retry}
      />
    </section>
  );
}

function GrantTable({
  rows,
  organizationNames,
  showOrganization,
  showSource,
  canRevoke,
  onRevoke,
}: {
  rows: CreditGrantRow[];
  organizationNames: Map<string, string>;
  showOrganization: boolean;
  showSource: boolean;
  canRevoke: boolean;
  onRevoke: (grant: CreditGrantRow) => void;
}) {
  const { t, locale } = useI18n();
  if (rows.length === 0) {
    return (
      <div className="flex min-h-72 flex-col items-center justify-center rounded-md border px-6 text-center">
        <Gift className="size-8 text-muted-foreground" aria-hidden="true" />
        <h3 className="mt-4 text-sm font-medium">
          {t({
            en: "No credit grants",
            "zh-CN": "暂无赠送额度",
            ja: "クレジット付与はありません",
            ko: "지급된 크레딧이 없습니다",
          })}
        </h3>
        <p className="mt-1 text-sm text-muted-foreground">
          {t({
            en: "Issued credits will appear here.",
            "zh-CN": "发放后的赠送额度会显示在这里。",
            ja: "付与されたクレジットがここに表示されます。",
            ko: "지급된 크레딧이 여기에 표시됩니다.",
          })}
        </p>
      </div>
    );
  }

  return (
    <div className="rounded-md border">
      <Table className="min-w-[760px]">
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">
              {t({
                en: "Received at",
                "zh-CN": "到账时间",
                ja: "付与日時",
                ko: "지급 시간",
              })}
            </TableHead>
            {showOrganization ? (
              <TableHead>
                {t({ en: "Organization", "zh-CN": "组织", ja: "組織", ko: "조직" })}
              </TableHead>
            ) : null}
            <TableHead>
              {t({ en: "Status", "zh-CN": "状态", ja: "ステータス", ko: "상태" })}
            </TableHead>
            <TableHead>
              {t({ en: "Balance", "zh-CN": "余额", ja: "残高", ko: "잔액" })}
            </TableHead>
            <TableHead>
              {t({
                en: "Expires at",
                "zh-CN": "到期时间",
                ja: "有効期限",
                ko: "만료 시간",
              })}
            </TableHead>
            {showSource ? (
              <TableHead>
                {t({ en: "Source", "zh-CN": "来源", ja: "付与元", ko: "출처" })}
              </TableHead>
            ) : null}
            {canRevoke ? <TableHead className="w-14 pr-3" /> : null}
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((grant) => (
            <TableRow key={grant.grant_id}>
              <TableCell className="pl-4">
                {formatGrantDateTime(grant.received_at_ms, locale)}
              </TableCell>
              {showOrganization ? (
                <TableCell>
                  <p className="max-w-52 truncate font-medium">
                    {"organization_id" in grant
                      ? grant.organization_display_name ??
                        organizationNames.get(grant.organization_id) ??
                        grant.organization_id
                      : null}
                  </p>
                </TableCell>
              ) : null}
              <TableCell>
                <GrantStateBadge state={grant.state} />
              </TableCell>
              <TableCell className="font-medium">
                {formatMoneyMicros(grant.available_micros, grant.currency)}
              </TableCell>
              <TableCell>
                {formatGrantDateTime(grant.expires_at_ms, locale)}
              </TableCell>
              {showSource ? (
                <TableCell className="max-w-56 truncate text-muted-foreground">
                  {"source_reference" in grant ? grant.source_reference : null}
                </TableCell>
              ) : null}
              {canRevoke ? (
                <TableCell className="pr-3 text-right">
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button
                        type="button"
                        variant="ghost"
                        size="icon"
                        aria-label={t({
                          en: "Credit actions",
                          "zh-CN": "额度操作",
                          ja: "クレジット操作",
                          ko: "크레딧 작업",
                        })}
                      >
                        <MoreHorizontal aria-hidden="true" />
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end">
                      <DropdownMenuItem
                        disabled={
                          !["active", "consuming"].includes(grant.state) ||
                          grant.available_micros === "0"
                        }
                        onSelect={() => onRevoke(grant)}
                      >
                        <RotateCcw aria-hidden="true" />
                        {t({
                          en: "Revoke remaining credits",
                          "zh-CN": "撤销剩余额度",
                          ja: "残りのクレジットを取り消す",
                          ko: "남은 크레딧 취소",
                        })}
                      </DropdownMenuItem>
                    </DropdownMenuContent>
                  </DropdownMenu>
                </TableCell>
              ) : null}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function IssueGrantDialog({
  open,
  onOpenChange,
  organizationId,
  organizations,
  initialCurrency,
  onIssued,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  organizationId: string | null;
  organizations: OrganizationOption[];
  initialCurrency: string;
  onIssued: () => void;
}) {
  const { t } = useI18n();
  const [selectedOrganization, setSelectedOrganization] = useState("");
  const [currency, setCurrency] = useState(initialCurrency);
  const [amount, setAmount] = useState("");
  const [expiresAt, setExpiresAt] = useState("");
  const [sourceReference, setSourceReference] = useState("");
  const [reason, setReason] = useState("");
  const [organizationSearch, setOrganizationSearch] = useState("");
  const [debouncedOrganizationSearch, setDebouncedOrganizationSearch] =
    useState("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [idempotencyKey, setIdempotencyKey] = useState("");

  useEffect(() => {
    if (!open) return;
    setSelectedOrganization(organizationId ?? "");
    setCurrency(initialCurrency);
    setAmount("");
    setExpiresAt(localDateTimeValue(Date.now() + 365 * 86_400_000));
    setSourceReference("");
    setReason("");
    setOrganizationSearch("");
    setDebouncedOrganizationSearch("");
    setError(null);
    setIdempotencyKey(crypto.randomUUID());
  }, [initialCurrency, open, organizationId]);

  useEffect(() => {
    const timeout = window.setTimeout(
      () => setDebouncedOrganizationSearch(organizationSearch.trim()),
      250,
    );
    return () => window.clearTimeout(timeout);
  }, [organizationSearch]);

  const organizationDirectoryEndpoint = useMemo(() => {
    const params = new URLSearchParams({ currency: "USD", limit: "100" });
    if (debouncedOrganizationSearch) {
      params.set("query", debouncedOrganizationSearch);
    }
    return `/admin/v1/billing/accounts?${params.toString()}`;
  }, [debouncedOrganizationSearch]);
  const organizationDirectory = useAdminQuery<BillingAccountControlList>(
    organizationDirectoryEndpoint,
    open && !organizationId,
  );
  const availableOrganizations = useMemo(
    () =>
      organizationId
        ? organizations
        : (organizationDirectory.data?.data ?? []).map((organization) => ({
            id: organization.organization_id,
            name: organization.display_name,
          })),
    [organizationDirectory.data, organizationId, organizations],
  );

  useEffect(() => {
    if (
      open &&
      !selectedOrganization &&
      availableOrganizations.length > 0
    ) {
      setSelectedOrganization(availableOrganizations[0].id);
    }
  }, [availableOrganizations, open, selectedOrganization]);

  async function submit() {
    const amountMicros = decimalToMicros(amount);
    const expiration = new Date(expiresAt).getTime();
    if (!selectedOrganization) {
      return setError(
        t({
          en: "Select an organization.",
          "zh-CN": "请选择组织",
          ja: "組織を選択してください。",
          ko: "조직을 선택하세요.",
        }),
      );
    }
    if (amountMicros === null) {
      return setError(
        t({
          en: "Enter an amount greater than 0 with up to 6 decimal places.",
          "zh-CN": "请输入大于 0、最多 6 位小数的金额",
          ja: "0 より大きく、小数点以下 6 桁までの金額を入力してください。",
          ko: "0보다 크고 소수점 이하 6자리까지의 금액을 입력하세요.",
        }),
      );
    }
    if (!Number.isFinite(expiration) || expiration <= Date.now()) {
      return setError(
        t({
          en: "The expiration time must be in the future.",
          "zh-CN": "到期时间必须晚于当前时间",
          ja: "有効期限は現在時刻より後に設定してください。",
          ko: "만료 시간은 현재 시간 이후여야 합니다.",
        }),
      );
    }
    if (!sourceReference.trim()) {
      return setError(
        t({
          en: "Enter a source reference.",
          "zh-CN": "请填写来源参考",
          ja: "付与元の参照情報を入力してください。",
          ko: "출처 참조를 입력하세요.",
        }),
      );
    }
    if (!reason.trim()) {
      return setError(
        t({
          en: "Enter a reason for issuing the credits.",
          "zh-CN": "请填写发放原因",
          ja: "付与理由を入力してください。",
          ko: "지급 사유를 입력하세요.",
        }),
      );
    }

    setSaving(true);
    setError(null);
    try {
      const response = await consoleFetch(
        "/api/gateway/admin/v1/billing/credit-grants",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "idempotency-key": idempotencyKey,
          },
          body: JSON.stringify({
            organization_id: selectedOrganization,
            currency,
            amount_micros: amountMicros,
            expires_at_ms: expiration,
            source_reference: sourceReference.trim(),
            reason: reason.trim(),
          }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response, t));
      toast.success(
        t({
          en: "Credits issued",
          "zh-CN": "赠送额度已发放",
          ja: "クレジットを付与しました",
          ko: "크레딧이 지급되었습니다",
        }),
      );
      onOpenChange(false);
      onIssued();
    } catch (caught) {
      setError(
        caught instanceof Error
          ? caught.message
          : t({
              en: "Failed to issue credits",
              "zh-CN": "额度发放失败",
              ja: "クレジットの付与に失敗しました",
              ko: "크레딧 지급에 실패했습니다",
            }),
      );
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[calc(100dvh-2rem)] overflow-y-auto sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>
            {t({
              en: "Issue credits",
              "zh-CN": "发放赠送额度",
              ja: "クレジットを付与",
              ko: "크레딧 지급",
            })}
          </DialogTitle>
          <DialogDescription>
            {t({
              en: "Granted credits are applied to organization usage first and expire automatically.",
              "zh-CN": "赠送额度会优先抵扣组织用量，并在到期后自动失效。",
              ja: "付与クレジットは組織の使用量に優先適用され、有効期限後に自動失効します。",
              ko: "지급 크레딧은 조직 사용량에 먼저 적용되며 만료 후 자동으로 소멸됩니다.",
            })}
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-2 sm:grid-cols-2">
          <Field
            label={t({ en: "Organization", "zh-CN": "组织", ja: "組織", ko: "조직" })}
            className="sm:col-span-2"
          >
            {!organizationId ? (
              <Input
                className="mb-2"
                value={organizationSearch}
                onChange={(event) => {
                  setOrganizationSearch(event.target.value);
                  setSelectedOrganization("");
                  setIdempotencyKey(crypto.randomUUID());
                }}
                placeholder={t({
                  en: "Search organization name or ID",
                  "zh-CN": "搜索组织名称或 ID",
                  ja: "組織名または ID を検索",
                  ko: "조직 이름 또는 ID 검색",
                })}
                aria-label={t({
                  en: "Search organizations",
                  "zh-CN": "搜索组织",
                  ja: "組織を検索",
                  ko: "조직 검색",
                })}
              />
            ) : null}
            <Select
              value={selectedOrganization}
              onValueChange={(value) => {
                setSelectedOrganization(value);
                setIdempotencyKey(crypto.randomUUID());
              }}
            >
              <SelectTrigger>
                <SelectValue
                  placeholder={t({
                    en: "Select organization",
                    "zh-CN": "选择组织",
                    ja: "組織を選択",
                    ko: "조직 선택",
                  })}
                />
              </SelectTrigger>
              <SelectContent>
                {availableOrganizations.map((organization) => (
                  <SelectItem key={organization.id} value={organization.id}>
                    {organization.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {!organizationId && organizationDirectory.loading ? (
              <p className="mt-2 text-xs text-muted-foreground">
                {t({
                  en: "Loading organizations",
                  "zh-CN": "正在加载组织",
                  ja: "組織を読み込んでいます",
                  ko: "조직을 불러오는 중",
                })}
              </p>
            ) : null}
            {!organizationId && organizationDirectory.error ? (
              <p className="mt-2 text-xs text-destructive">
                {organizationDirectory.error.message}
              </p>
            ) : null}
          </Field>
          <Field label={t({ en: "Amount", "zh-CN": "金额", ja: "金額", ko: "금액" })}>
            <Input
              inputMode="decimal"
              value={amount}
              onChange={(event) => {
                setAmount(event.target.value);
                setIdempotencyKey(crypto.randomUUID());
              }}
              placeholder="100.00"
            />
          </Field>
          <Field
            label={t({ en: "Currency", "zh-CN": "币种", ja: "通貨", ko: "통화" })}
          >
            <Select
              value={currency}
              onValueChange={(value) => {
                setCurrency(value);
                setIdempotencyKey(crypto.randomUUID());
              }}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="USD">USD</SelectItem>
                <SelectItem value="CNY">CNY</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field
            label={t({
              en: "Expiration",
              "zh-CN": "到期时间",
              ja: "有効期限",
              ko: "만료 시간",
            })}
            className="sm:col-span-2"
          >
            <Input
              type="datetime-local"
              value={expiresAt}
              onChange={(event) => {
                setExpiresAt(event.target.value);
                setIdempotencyKey(crypto.randomUUID());
              }}
            />
          </Field>
          <Field
            label={t({
              en: "Source reference",
              "zh-CN": "来源参考",
              ja: "付与元の参照",
              ko: "출처 참조",
            })}
            className="sm:col-span-2"
          >
            <Input
              value={sourceReference}
              onChange={(event) => {
                setSourceReference(event.target.value);
                setIdempotencyKey(crypto.randomUUID());
              }}
              placeholder={t({
                en: "For example: launch-promotion-2026",
                "zh-CN": "例如：launch-promotion-2026",
                ja: "例: launch-promotion-2026",
                ko: "예: launch-promotion-2026",
              })}
            />
          </Field>
          <Field
            label={t({
              en: "Reason",
              "zh-CN": "发放原因",
              ja: "付与理由",
              ko: "지급 사유",
            })}
            className="sm:col-span-2"
          >
            <Textarea
              value={reason}
              onChange={(event) => {
                setReason(event.target.value);
                setIdempotencyKey(crypto.randomUUID());
              }}
              placeholder={t({
                en: "Record the business reason for this grant",
                "zh-CN": "记录本次发放的业务原因",
                ja: "今回の付与に関する業務上の理由を記録",
                ko: "이번 지급의 비즈니스 사유 기록",
              })}
            />
          </Field>
          {error ? (
            <p className="text-sm text-destructive sm:col-span-2">{error}</p>
          ) : null}
        </div>
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
              <Gift aria-hidden="true" />
            )}
            {t({
              en: "Issue credits",
              "zh-CN": "发放额度",
              ja: "クレジットを付与",
              ko: "크레딧 지급",
            })}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function RevokeGrantDialog({
  target,
  onOpenChange,
  onRevoked,
}: {
  target: CreditGrantRow | null;
  onOpenChange: (open: boolean) => void;
  onRevoked: () => void;
}) {
  const { t } = useI18n();
  const [reason, setReason] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [idempotencyKey, setIdempotencyKey] = useState("");

  useEffect(() => {
    if (!target) return;
    setReason("");
    setError(null);
    setIdempotencyKey(crypto.randomUUID());
  }, [target]);

  async function submit() {
    if (!target || !reason.trim()) {
      return setError(
        t({
          en: "Enter a reason for revoking the credits.",
          "zh-CN": "请填写撤销原因",
          ja: "取消理由を入力してください。",
          ko: "취소 사유를 입력하세요.",
        }),
      );
    }
    setSaving(true);
    setError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/admin/v1/billing/credit-grants/${encodeURIComponent(
          target.grant_id,
        )}/revoke`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "idempotency-key": idempotencyKey,
          },
          body: JSON.stringify({ reason: reason.trim() }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response, t));
      toast.success(
        t({
          en: "Remaining credits revoked",
          "zh-CN": "剩余额度已撤销",
          ja: "残りのクレジットを取り消しました",
          ko: "남은 크레딧이 취소되었습니다",
        }),
      );
      onOpenChange(false);
      onRevoked();
    } catch (caught) {
      setError(
        caught instanceof Error
          ? caught.message
          : t({
              en: "Failed to revoke credits",
              "zh-CN": "额度撤销失败",
              ja: "クレジットの取消に失敗しました",
              ko: "크레딧 취소에 실패했습니다",
            }),
      );
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog open={Boolean(target)} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>
            {t({
              en: "Revoke remaining credits",
              "zh-CN": "撤销剩余额度",
              ja: "残りのクレジットを取り消す",
              ko: "남은 크레딧 취소",
            })}
          </DialogTitle>
          <DialogDescription>
            {target
              ? t(
                  {
                    en: "{amount} will be revoked. Consumed credits will not be restored.",
                    "zh-CN": "将撤销 {amount}，已消费额度不会回退。",
                    ja: "{amount} を取り消します。使用済みクレジットは復元されません。",
                    ko: "{amount}을(를) 취소합니다. 사용된 크레딧은 복원되지 않습니다.",
                  },
                  {
                    amount: formatMoneyMicros(
                      target.available_micros,
                      target.currency,
                    ),
                  },
                )
              : null}
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-2 py-2">
          <Label htmlFor="credit-grant-revoke-reason">
            {t({
              en: "Revocation reason",
              "zh-CN": "撤销原因",
              ja: "取消理由",
              ko: "취소 사유",
            })}
          </Label>
          <Textarea
            id="credit-grant-revoke-reason"
            value={reason}
            onChange={(event) => {
              setReason(event.target.value);
              setIdempotencyKey(crypto.randomUUID());
            }}
            placeholder={t({
              en: "Record the business reason for this revocation",
              "zh-CN": "记录本次撤销的业务原因",
              ja: "今回の取消に関する業務上の理由を記録",
              ko: "이번 취소의 비즈니스 사유 기록",
            })}
          />
          {error ? <p className="text-sm text-destructive">{error}</p> : null}
        </div>
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={saving}
          >
            {t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}
          </Button>
          <Button
            type="button"
            variant="destructive"
            onClick={() => void submit()}
            disabled={saving}
          >
            {saving ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <RotateCcw aria-hidden="true" />
            )}
            {t({
              en: "Confirm revocation",
              "zh-CN": "确认撤销",
              ja: "取消を確定",
              ko: "취소 확인",
            })}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function SummaryItem({
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
    <div className={last ? "p-5" : "border-b p-5 sm:border-b-0 sm:border-r"}>
      <p className="text-sm text-muted-foreground">{label}</p>
      <p className="mt-2 text-2xl font-semibold">{value}</p>
      <p className="mt-2 text-xs text-muted-foreground">{detail}</p>
    </div>
  );
}

function GrantStateBadge({ state }: { state: CreditGrantState }) {
  const { t } = useI18n();
  return (
    <Badge
      variant={["active", "consuming"].includes(state) ? "default" : "secondary"}
    >
      {grantStateLabel(t, state)}
    </Badge>
  );
}

function Field({
  label,
  className,
  children,
}: {
  label: string;
  className?: string;
  children: ReactNode;
}) {
  return (
    <div className={className}>
      <Label className="mb-2 block">{label}</Label>
      {children}
    </div>
  );
}

function localDateTimeValue(timestamp: number) {
  const date = new Date(timestamp);
  const local = new Date(timestamp - date.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
}

function formatGrantDateTime(timestamp: number, locale: Locale) {
  return new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(timestamp));
}

function grantStateLabel(t: Translate, state: CreditGrantState) {
  const labels: Record<CreditGrantState, string> = {
    active: t({
      en: "Available",
      "zh-CN": "可用",
      ja: "利用可能",
      ko: "사용 가능",
    }),
    consuming: t({
      en: "In use",
      "zh-CN": "使用中",
      ja: "使用中",
      ko: "사용 중",
    }),
    exhausted: t({
      en: "Exhausted",
      "zh-CN": "已用完",
      ja: "使用済み",
      ko: "소진됨",
    }),
    expired: t({
      en: "Expired",
      "zh-CN": "已到期",
      ja: "期限切れ",
      ko: "만료됨",
    }),
    revoked: t({
      en: "Revoked",
      "zh-CN": "已撤销",
      ja: "取消済み",
      ko: "취소됨",
    }),
  };
  return labels[state] ?? state;
}

async function responseMessage(response: Response, t: Translate) {
  const payload = (await response.json().catch(() => null)) as {
    error?: { message?: string };
  } | null;
  return (
    payload?.error?.message ??
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
}
