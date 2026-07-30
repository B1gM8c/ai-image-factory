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
import { useI18n } from "@/i18n/locale-provider";
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
type Translate = ReturnType<typeof useI18n>["t"];

export function BillingAccountControlSheet({
  open,
  onOpenChange,
  onUpdated,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onUpdated: () => void;
}) {
  const { t } = useI18n();
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
      setSaveError(
        t({
          en: "Enter an amount of 0 or more with no more than 6 decimal places",
          "zh-CN": "请输入不小于 0、最多 6 位小数的金额",
          ja: "0 以上で小数点以下 6 桁以内の金額を入力してください",
          ko: "0 이상이며 소수점 이하 6자리 이내인 금액을 입력하세요",
        }),
      );
      return;
    }
    if (reason.trim().length < 3) {
      setSaveError(
        t({
          en: "Enter a reason with at least 3 characters",
          "zh-CN": "请填写至少 3 个字符的变更原因",
          ja: "変更理由を 3 文字以上で入力してください",
          ko: "변경 사유를 3자 이상 입력하세요",
        }),
      );
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
      if (!response.ok) throw new Error(await responseMessage(response, t));
      const account = (await response.json()) as BillingAccountControlView;
      setSelected((current) => (current ? { ...current, account } : current));
      query.retry();
      onUpdated();
      toast.success(
        t({
          en: "Organization limit updated",
          "zh-CN": "组织限额已更新",
          ja: "組織の上限を更新しました",
          ko: "조직 한도가 업데이트되었습니다",
        }),
      );
      setSelected(null);
    } catch (caught) {
      setSaveError(
        caught instanceof Error
          ? caught.message
          : t({
              en: "Organization limit could not be saved",
              "zh-CN": "组织限额保存失败",
              ja: "組織の上限を保存できませんでした",
              ko: "조직 한도를 저장하지 못했습니다",
            }),
      );
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
                aria-label={t({
                  en: "Back to organizations",
                  "zh-CN": "返回组织列表",
                  ja: "組織一覧に戻る",
                  ko: "조직 목록으로 돌아가기",
                })}
                onClick={() => setSelected(null)}
              >
                <ArrowLeft aria-hidden="true" />
              </Button>
            ) : null}
            <div className="min-w-0">
              <SheetTitle className="truncate">
                {selected?.display_name ??
                  t({
                    en: "Organization limits",
                    "zh-CN": "组织限额",
                    ja: "組織の上限",
                    ko: "조직 한도",
                  })}
              </SheetTitle>
              <SheetDescription className="truncate">
                {selected
                  ? `${selected.organization_id} · ${currency}`
                  : t({
                      en: "Platform billing access",
                      "zh-CN": "平台计费准入",
                      ja: "プラットフォームの請求アクセス",
                      ko: "플랫폼 결제 접근",
                    })}
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
              {t({
                en: "Cancel",
                "zh-CN": "取消",
                ja: "キャンセル",
                ko: "취소",
              })}
            </Button>
            <Button type="button" onClick={() => void save()} disabled={saving}>
              {saving ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <Save aria-hidden="true" />
              )}
              {t({
                en: "Save limit",
                "zh-CN": "保存限额",
                ja: "上限を保存",
                ko: "한도 저장",
              })}
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
  const { t } = useI18n();

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
        </div>
        <Select value={currency} onValueChange={onCurrencyChange}>
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
      </div>

      <div className="min-h-0 flex-1 overflow-auto">
        <Table className="min-w-[760px]">
          <TableHeader>
            <TableRow>
              <TableHead className="pl-6">
                {t({
                  en: "Organization",
                  "zh-CN": "组织",
                  ja: "組織",
                  ko: "조직",
                })}
              </TableHead>
              <TableHead>
                {t({ en: "Status", "zh-CN": "状态", ja: "状態", ko: "상태" })}
              </TableHead>
              <TableHead className="text-right">
                {t({ en: "Limit", "zh-CN": "限额", ja: "上限", ko: "한도" })}
              </TableHead>
              <TableHead className="text-right">
                {t({
                  en: "Available",
                  "zh-CN": "可用",
                  ja: "利用可能",
                  ko: "사용 가능",
                })}
              </TableHead>
              <TableHead className="pr-6 text-right">
                {t({
                  en: "Actions",
                  "zh-CN": "操作",
                  ja: "操作",
                  ko: "작업",
                })}
              </TableHead>
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
                  {t({
                    en: "Loading organizations",
                    "zh-CN": "正在加载组织",
                    ja: "組織を読み込み中",
                    ko: "조직 불러오는 중",
                  })}
                </TableCell>
              </TableRow>
            ) : null}
            {!query.loading && query.error ? (
              <TableRow>
                <TableCell colSpan={5} className="h-40 text-center">
                  <p className="font-medium">
                    {t({
                      en: "Organization limits could not be loaded",
                      "zh-CN": "组织限额加载失败",
                      ja: "組織の上限を読み込めませんでした",
                      ko: "조직 한도를 불러오지 못했습니다",
                    })}
                  </p>
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
                    {t({
                      en: "Reload",
                      "zh-CN": "重新加载",
                      ja: "再読み込み",
                      ko: "다시 불러오기",
                    })}
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
                  {t({
                    en: "No matching organizations",
                    "zh-CN": "没有匹配的组织",
                    ja: "一致する組織はありません",
                    ko: "일치하는 조직이 없습니다",
                  })}
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
                    {item.account.configured
                      ? t({
                          en: "Configured",
                          "zh-CN": "已配置",
                          ja: "設定済み",
                          ko: "구성됨",
                        })
                      : t({
                          en: "Not configured",
                          "zh-CN": "未配置",
                          ja: "未設定",
                          ko: "구성되지 않음",
                        })}
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
                    {t({
                      en: "Configure",
                      "zh-CN": "设置",
                      ja: "設定",
                      ko: "설정",
                    })}
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>

      <div className="flex shrink-0 items-center justify-between border-t px-5 py-3 sm:px-6">
        <span className="text-sm text-muted-foreground">
          {t(
            {
              en: "Page {page}",
              "zh-CN": "第 {page} 页",
              ja: "{page} ページ",
              ko: "{page}페이지",
            },
            { page },
          )}
        </span>
        <div className="flex gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={onPrevious}
            disabled={page === 1 || query.refreshing}
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
            onClick={onNext}
            disabled={!query.data?.has_more || query.refreshing}
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
  const { locale, t } = useI18n();
  const account = item.account;
  return (
    <div className="min-h-0 flex-1 overflow-y-auto px-5 py-6 sm:px-6">
      <div className="grid gap-x-8 gap-y-5 rounded-md bg-muted/30 p-4 sm:grid-cols-3">
        <AccountMetric
          label={t({
            en: "Spending limit",
            "zh-CN": "消费上限",
            ja: "利用上限",
            ko: "지출 한도",
          })}
          value={formatMoneyMicros(account.credit_limit_micros, currency)}
        />
        <AccountMetric
          label={t({
            en: "Reserved",
            "zh-CN": "已占用",
            ja: "予約済み",
            ko: "예약됨",
          })}
          value={formatMoneyMicros(account.held_micros, currency)}
        />
        <AccountMetric
          label={t({
            en: "Captured",
            "zh-CN": "累计扣费",
            ja: "確定済み",
            ko: "결제 확정",
          })}
          value={formatMoneyMicros(account.captured_micros, currency)}
        />
        <AccountMetric
          label={t({
            en: "Refunded",
            "zh-CN": "已退款",
            ja: "返金済み",
            ko: "환불됨",
          })}
          value={formatMoneyMicros(account.refunded_micros, currency)}
        />
        <AccountMetric
          label={t({
            en: "Net spend",
            "zh-CN": "净支出",
            ja: "純支出",
            ko: "순 지출",
          })}
          value={formatMoneyMicros(
            (
              parseMicros(account.captured_micros) -
              parseMicros(account.refunded_micros)
            ).toString(),
            currency,
          )}
        />
        <AccountMetric
          label={t({
            en: "Available",
            "zh-CN": "可用",
            ja: "利用可能",
            ko: "사용 가능",
          })}
          value={formatMoneyMicros(account.available_micros, currency)}
        />
      </div>

      <div className="mt-7 grid gap-6">
        <div className="grid gap-2">
          <Label htmlFor="billing-credit-limit">
            {t({
              en: "New spending limit",
              "zh-CN": "新的消费上限",
              ja: "新しい利用上限",
              ko: "새 지출 한도",
            })}
          </Label>
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
            {t({
              en: "Set this to 0 to stop new billable requests for this organization. Existing reservations and settled amounts will not be reversed.",
              "zh-CN": "设为 0 将停止该组织的新计费请求；已有占用和已结算金额不会被回退。",
              ja: "0 に設定すると、この組織の新しい課金対象リクエストを停止します。既存の予約額と確定額は取り消されません。",
              ko: "0으로 설정하면 이 조직의 새로운 유료 요청이 중지됩니다. 기존 예약 및 정산 금액은 되돌려지지 않습니다.",
            })}
          </p>
        </div>

        <div className="grid gap-2">
          <Label htmlFor="billing-credit-reason">
            {t({
              en: "Reason for change",
              "zh-CN": "变更原因",
              ja: "変更理由",
              ko: "변경 사유",
            })}
          </Label>
          <Textarea
            id="billing-credit-reason"
            value={reason}
            onChange={(event) => onReasonChange(event.target.value)}
            placeholder={t({
              en: "For example: Finance approval FIN-2026-042",
              "zh-CN": "例如：财务审批单 FIN-2026-042",
              ja: "例: 財務承認 FIN-2026-042",
              ko: "예: 재무 승인 FIN-2026-042",
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

        <dl className="grid gap-3 border-t pt-5 text-sm sm:grid-cols-2">
          <Definition
            label={t({
              en: "Control version",
              "zh-CN": "控制版本",
              ja: "制御バージョン",
              ko: "제어 버전",
            })}
            value={account.control_version}
            mono
          />
          <Definition
            label={t({
              en: "Last updated",
              "zh-CN": "最后更新",
              ja: "最終更新",
              ko: "마지막 업데이트",
            })}
            value={formatDateTime(account.updated_at_ms, locale)}
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
