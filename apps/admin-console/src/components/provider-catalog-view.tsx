"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { ImageIcon, KeyRound, LoaderCircle, RefreshCw, Search, Video } from "lucide-react";
import { toast } from "sonner";
import { AdminQueryError, AdminQuerySkeleton } from "@/components/admin-query-state";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
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
import { useAdminQuery } from "@/hooks/use-admin-query";
import { useI18n } from "@/i18n/locale-provider";
import type { Locale, LocalizedText } from "@/i18n/config";
import type {
  ProviderAccountsSnapshot,
  ProviderModelRefresh,
  ProviderModelsSnapshot,
  ProviderModelView,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";
import { useConsoleSession } from "@/components/auth/console-session-provider";

const MODELS_ENDPOINT = "/v1/console/provider-models";
const ACCOUNTS_ENDPOINT = "/admin/v1/provider-accounts";

export function ProviderCatalogView() {
  const { locale, t } = useI18n();
  const { capabilities } = useConsoleSession();
  const canManageProviders = capabilities.includes("providers:manage");
  const modelsQuery = useAdminQuery<ProviderModelsSnapshot>(MODELS_ENDPOINT);
  const accountsQuery = useAdminQuery<ProviderAccountsSnapshot>(
    ACCOUNTS_ENDPOINT,
    canManageProviders,
  );
  const [refreshingProviders, setRefreshingProviders] = useState<Set<string>>(new Set());
  const [search, setSearch] = useState("");
  const [provider, setProvider] = useState("all");
  const [mediaKind, setMediaKind] = useState("all");
  const [availability, setAvailability] = useState("all");
  const activeAccounts = useMemo(
    () => (accountsQuery.data?.accounts ?? []).filter(
      (account) => account.environment_state === "active" && account.account_state === "enabled",
    ),
    [accountsQuery.data],
  );

  async function refreshModels() {
    const targets = activeAccounts;
    if (targets.length === 0) {
      toast.error(t({
        en: "Add an available CLI account first.",
        "zh-CN": "请先添加可用的 CLI 账户",
        ja: "先に利用可能な CLI アカウントを追加してください。",
        ko: "먼저 사용 가능한 CLI 계정을 추가하세요.",
      }));
      return;
    }
    const providers = new Set(targets.map((account) => account.provider_id));
    setRefreshingProviders((current) => new Set([...current, ...providers]));
    const results = await Promise.allSettled(targets.map(async (account) => {
      const started = await startRefresh(account.provider_account_id, t);
      return waitForRefresh(started.refresh_id, t);
    }));
    setRefreshingProviders((current) => {
      const next = new Set(current);
      for (const provider of providers) next.delete(provider);
      return next;
    });
    modelsQuery.retry();
    const succeeded = results.filter((result) => result.status === "fulfilled").length;
    if (succeeded === results.length) {
      toast.success(t(
        {
          en: "Updated the model catalog for {count} accounts.",
          "zh-CN": "已更新 {count} 个账户的模型目录",
          ja: "{count} 件のアカウントのモデルカタログを更新しました。",
          ko: "{count}개 계정의 모델 카탈로그를 업데이트했습니다.",
        },
        { count: succeeded.toLocaleString(locale) },
      ));
    } else if (succeeded > 0) {
      toast.warning(t(
        {
          en: "Updated {succeeded} accounts; {failed} failed.",
          "zh-CN": "已更新 {succeeded} 个账户，另有 {failed} 个失败",
          ja: "{succeeded} 件を更新し、{failed} 件が失敗しました。",
          ko: "{succeeded}개 계정을 업데이트했고 {failed}개는 실패했습니다.",
        },
        {
          succeeded: succeeded.toLocaleString(locale),
          failed: (results.length - succeeded).toLocaleString(locale),
        },
      ));
    } else {
      toast.error(t({
        en: "Could not update the model catalog. Check account authorization and CLI status.",
        "zh-CN": "模型目录更新失败，请检查账户授权或 CLI 状态",
        ja: "モデルカタログを更新できませんでした。アカウント認証と CLI の状態を確認してください。",
        ko: "모델 카탈로그를 업데이트하지 못했습니다. 계정 권한과 CLI 상태를 확인하세요.",
      }));
    }
  }

  const refreshingAll = refreshingProviders.size > 0;
  const error = modelsQuery.error ?? (canManageProviders ? accountsQuery.error : null);
  const providers = useMemo(() => {
    const unique = new Map<string, string>();
    for (const model of modelsQuery.data?.models ?? []) {
      unique.set(model.provider_id, model.provider_display_name);
    }
    return [...unique.entries()];
  }, [modelsQuery.data]);
  const filteredModels = useMemo(() => {
    const query = search.trim().toLocaleLowerCase();
    return (modelsQuery.data?.models ?? []).filter((model) => {
      if (provider !== "all" && model.provider_id !== provider) return false;
      if (mediaKind !== "all" && model.media_kind !== mediaKind) return false;
      if (availability !== "all" && model.availability !== availability) return false;
      if (!query) return true;
      return model.display_name.toLocaleLowerCase().includes(query)
        || model.model_id.toLocaleLowerCase().includes(query);
    });
  }, [availability, mediaKind, modelsQuery.data, provider, search]);

  return (
    <div className="min-w-0 space-y-6">
      <PageHeader
        title={t({
          en: "Models and capabilities",
          "zh-CN": "模型与能力",
          ja: "モデルと機能",
          ko: "모델 및 기능",
        })}
        actions={canManageProviders ? (
          <>
            <Button asChild variant="outline">
              <Link href="/provider-accounts">
                <KeyRound aria-hidden="true" />
                {t({
                  en: "CLI accounts",
                  "zh-CN": "CLI 账户",
                  ja: "CLI アカウント",
                  ko: "CLI 계정",
                })}
              </Link>
            </Button>
            <Button onClick={() => void refreshModels()} disabled={refreshingAll || accountsQuery.loading}>
              {refreshingAll
                ? <LoaderCircle className="animate-spin" aria-hidden="true" />
                : <RefreshCw aria-hidden="true" />}
              {t({
                en: "Update from CLI",
                "zh-CN": "从 CLI 更新",
                ja: "CLI から更新",
                ko: "CLI에서 업데이트",
              })}
            </Button>
          </>
        ) : undefined}
      />

      <div className="grid gap-3 border-y py-4 md:grid-cols-[minmax(14rem,1fr)_12rem_10rem_12rem_auto]">
        <div className="relative">
          <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" aria-hidden="true" />
          <Input
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder={t({
              en: "Search models",
              "zh-CN": "搜索模型",
              ja: "モデルを検索",
              ko: "모델 검색",
            })}
            className="pl-9"
          />
        </div>
        <Select value={provider} onValueChange={setProvider}>
          <SelectTrigger aria-label={t({
            en: "Provider",
            "zh-CN": "供应商",
            ja: "プロバイダー",
            ko: "공급자",
          })}><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">
              {t({
                en: "All providers",
                "zh-CN": "全部供应商",
                ja: "すべてのプロバイダー",
                ko: "모든 공급자",
              })}
            </SelectItem>
            {providers.map(([value, label]) => <SelectItem key={value} value={value}>{label}</SelectItem>)}
          </SelectContent>
        </Select>
        <Select value={mediaKind} onValueChange={setMediaKind}>
          <SelectTrigger aria-label={t({
            en: "Model type",
            "zh-CN": "模型类型",
            ja: "モデルタイプ",
            ko: "모델 유형",
          })}><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">
              {t({
                en: "All types",
                "zh-CN": "全部类型",
                ja: "すべてのタイプ",
                ko: "모든 유형",
              })}
            </SelectItem>
            <SelectItem value="image">
              {t({ en: "Image", "zh-CN": "图片", ja: "画像", ko: "이미지" })}
            </SelectItem>
            <SelectItem value="video">
              {t({ en: "Video", "zh-CN": "视频", ja: "動画", ko: "동영상" })}
            </SelectItem>
          </SelectContent>
        </Select>
        <Select value={availability} onValueChange={setAvailability}>
          <SelectTrigger aria-label={t({
            en: "Availability",
            "zh-CN": "可用状态",
            ja: "利用可能状況",
            ko: "사용 가능 상태",
          })}><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">
              {t({ en: "All statuses", "zh-CN": "全部状态", ja: "すべての状態", ko: "모든 상태" })}
            </SelectItem>
            <SelectItem value="routable">
              {t({ en: "Available", "zh-CN": "可调用", ja: "利用可能", ko: "사용 가능" })}
            </SelectItem>
            <SelectItem value="observed">
              {t({ en: "Discovered", "zh-CN": "已发现", ja: "検出済み", ko: "발견됨" })}
            </SelectItem>
            <SelectItem value="unobserved">
              {t({ en: "Not observed", "zh-CN": "待观测", ja: "未観測", ko: "관찰 대기" })}
            </SelectItem>
            <SelectItem value="not_supported">
              {t({ en: "Needs adapter", "zh-CN": "待适配", ja: "要アダプター", ko: "어댑터 필요" })}
            </SelectItem>
          </SelectContent>
        </Select>
        <span className="self-center whitespace-nowrap text-sm tabular-nums text-muted-foreground">
          {t(
            {
              en: "{count} models",
              "zh-CN": "{count} 个模型",
              ja: "{count} モデル",
              ko: "모델 {count}개",
            },
            { count: filteredModels.length.toLocaleString(locale) },
          )}
        </span>
      </div>

      {modelsQuery.loading ? <AdminQuerySkeleton rows={8} /> : null}
      {error && !modelsQuery.data ? (
        <AdminQueryError error={error} retry={() => { modelsQuery.retry(); accountsQuery.retry(); }} />
      ) : null}
      {modelsQuery.data ? (
        <ModelTable
          models={filteredModels}
          showOperational={canManageProviders}
          locale={locale}
          t={t}
        />
      ) : null}
    </div>
  );
}

function ModelTable({
  models,
  showOperational,
  locale,
  t,
}: {
  models: ProviderModelView[];
  showOperational: boolean;
  locale: Locale;
  t: Translate;
}) {
  if (models.length === 0) {
    return (
      <div className="border-y py-12 text-center text-sm text-muted-foreground">
        {t({
          en: "No matching models",
          "zh-CN": "没有匹配的模型",
          ja: "一致するモデルはありません",
          ko: "일치하는 모델이 없습니다",
        })}
      </div>
    );
  }
  return (
    <div className="border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">
              {t({ en: "Provider", "zh-CN": "供应商", ja: "プロバイダー", ko: "공급자" })}
            </TableHead>
            <TableHead>{t({ en: "Model", "zh-CN": "模型", ja: "モデル", ko: "모델" })}</TableHead>
            <TableHead>{t({ en: "Type", "zh-CN": "类型", ja: "タイプ", ko: "유형" })}</TableHead>
            <TableHead>{t({ en: "API capabilities", "zh-CN": "API 能力", ja: "API 機能", ko: "API 기능" })}</TableHead>
            <TableHead>
              {showOperational
                ? t({ en: "Account observations", "zh-CN": "账户观测", ja: "アカウント観測", ko: "계정 관찰" })
                : t({ en: "Workspace availability", "zh-CN": "工作区可用性", ja: "ワークスペース可用性", ko: "워크스페이스 가용성" })}
            </TableHead>
            <TableHead>{t({ en: "Production status", "zh-CN": "生产状态", ja: "本番ステータス", ko: "프로덕션 상태" })}</TableHead>
            <TableHead>{t({ en: "Last updated", "zh-CN": "最后更新", ja: "最終更新", ko: "마지막 업데이트" })}</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {models.map((model) => (
            <TableRow key={`${model.provider_id}:${model.media_kind}:${model.model_id}`}>
              <TableCell className="pl-4 font-medium">{model.provider_display_name}</TableCell>
              <TableCell className="min-w-64 whitespace-normal">
                <p className="font-medium">{model.display_name}</p>
                <p className="mt-0.5 break-all font-mono text-xs text-muted-foreground">{model.model_id}</p>
                {model.latest_cli_version ? (
                  <p className="mt-1 text-xs text-muted-foreground">{model.latest_cli_version}</p>
                ) : null}
              </TableCell>
              <TableCell><MediaBadge media={model.media_kind} t={t} /></TableCell>
              <TableCell className="whitespace-normal">
                <div className="flex min-w-44 flex-wrap gap-1.5">
                  {model.operation_ids.length > 0
                    ? model.operation_ids.map((operation) => (
                      <Badge key={operation} variant="outline">{operationLabel(operation, t)}</Badge>
                    ))
                    : (
                      <span className="text-muted-foreground">
                        {t({ en: "Not mapped", "zh-CN": "待映射", ja: "未マッピング", ko: "매핑 대기" })}
                      </span>
                    )}
                </div>
              </TableCell>
              <TableCell>
                {showOperational
                  ? (
                    <span>
                      {t(
                        {
                          en: "{count} accounts",
                          "zh-CN": "{count} 个账户",
                          ja: "{count} アカウント",
                          ko: "계정 {count}개",
                        },
                        { count: (model.observed_account_count ?? 0).toLocaleString(locale) },
                      )}
                    </span>
                  )
                  : (
                    <span className="text-muted-foreground">
                      {t({ en: "Platform routing", "zh-CN": "平台路由", ja: "プラットフォームルーティング", ko: "플랫폼 라우팅" })}
                    </span>
                  )}
              </TableCell>
              <TableCell>
                <AvailabilityBadge
                  model={model}
                  showOperational={showOperational}
                  locale={locale}
                  t={t}
                />
              </TableCell>
              <TableCell className="text-muted-foreground">
                {formatDateTime(
                  model.last_successful_refresh_at_ms ?? model.last_observed_at_ms,
                  locale,
                )}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function MediaBadge({
  media,
  t,
}: {
  media: ProviderModelView["media_kind"];
  t: Translate;
}) {
  const Icon = media === "image" ? ImageIcon : Video;
  return (
    <Badge variant="secondary">
      <Icon aria-hidden="true" />
      {media === "image"
        ? t({ en: "Image", "zh-CN": "图片", ja: "画像", ko: "이미지" })
        : t({ en: "Video", "zh-CN": "视频", ja: "動画", ko: "동영상" })}
    </Badge>
  );
}

function AvailabilityBadge({
  model,
  showOperational,
  locale,
  t,
}: {
  model: ProviderModelView;
  showOperational: boolean;
  locale: Locale;
  t: Translate;
}) {
  if (model.availability === "routable") {
    return (
      <Badge>
        {showOperational
          ? t(
            {
              en: "Available · {count} accounts",
              "zh-CN": "可调用 · {count} 个账户",
              ja: "利用可能 · {count} アカウント",
              ko: "사용 가능 · 계정 {count}개",
            },
            { count: (model.routable_account_count ?? 0).toLocaleString(locale) },
          )
          : t({ en: "Available", "zh-CN": "可调用", ja: "利用可能", ko: "사용 가능" })}
      </Badge>
    );
  }
  if (model.availability === "observed") {
    return (
      <Badge variant="secondary">
        {t({ en: "Discovered", "zh-CN": "已发现", ja: "検出済み", ko: "발견됨" })}
      </Badge>
    );
  }
  if (model.availability === "not_supported") {
    return (
      <Badge variant="outline">
        {t({ en: "Needs adapter", "zh-CN": "待适配", ja: "要アダプター", ko: "어댑터 필요" })}
      </Badge>
    );
  }
  return (
    <Badge variant="outline">
      {t({ en: "Awaiting CLI observation", "zh-CN": "待 CLI 观测", ja: "CLI 観測待ち", ko: "CLI 관찰 대기" })}
    </Badge>
  );
}

function operationLabel(operation: string, t: Translate) {
  return ({
    "images.generations": t({ en: "Image generation", "zh-CN": "图片生成", ja: "画像生成", ko: "이미지 생성" }),
    "images.edits": t({ en: "Image editing", "zh-CN": "图片编辑", ja: "画像編集", ko: "이미지 편집" }),
    "videos.generations": t({ en: "Video generation", "zh-CN": "视频生成", ja: "動画生成", ko: "동영상 생성" }),
  } as Record<string, string>)[operation] ?? operation;
}

async function startRefresh(providerAccountId: string, t: Translate) {
  const response = await consoleFetch(
    `/api/gateway/admin/v1/provider-accounts/${providerAccountId}/model-refreshes`,
    { method: "POST", body: "{}" },
  );
  if (!response.ok) {
    throw new Error(t({
      en: "Could not start the model refresh.",
      "zh-CN": "模型刷新无法启动",
      ja: "モデルの更新を開始できませんでした。",
      ko: "모델 새로 고침을 시작할 수 없습니다.",
    }));
  }
  return (await response.json()) as ProviderModelRefresh;
}

async function waitForRefresh(refreshId: string, t: Translate) {
  for (let attempt = 0; attempt < 120; attempt += 1) {
    const response = await consoleFetch(
      `/api/gateway/admin/v1/provider-model-refreshes/${refreshId}`,
    );
    if (!response.ok) {
      throw new Error(t({
        en: "Model refresh status is unavailable.",
        "zh-CN": "模型刷新状态不可用",
        ja: "モデル更新の状態を取得できません。",
        ko: "모델 새로 고침 상태를 확인할 수 없습니다.",
      }));
    }
    const refresh = (await response.json()) as ProviderModelRefresh;
    if (refresh.status === "succeeded") return refresh;
    if (refresh.status === "failed") {
      throw new Error(refresh.error_code ?? t({
        en: "Model refresh failed.",
        "zh-CN": "模型刷新失败",
        ja: "モデルの更新に失敗しました。",
        ko: "모델 새로 고침에 실패했습니다.",
      }));
    }
    await delay(1_000);
  }
  throw new Error(t({
    en: "Model refresh timed out.",
    "zh-CN": "模型刷新超时",
    ja: "モデルの更新がタイムアウトしました。",
    ko: "모델 새로 고침 시간이 초과되었습니다.",
  }));
}

type Translate = (
  text: LocalizedText,
  values?: Record<string, string | number>,
) => string;

function formatDateTime(value: number | null | undefined, locale: Locale) {
  if (value === null || value === undefined) return "--";
  return new Intl.DateTimeFormat(locale, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function delay(milliseconds: number) {
  return new Promise<void>((resolve) => window.setTimeout(resolve, milliseconds));
}
