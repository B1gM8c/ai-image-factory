"use client";

import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  CloudUpload,
  Gauge,
  Info,
  Layers3,
  LoaderCircle,
  Save,
  Search,
  SquareTerminal,
  UsersRound,
} from "lucide-react";
import { toast } from "sonner";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import { Separator } from "@/components/ui/separator";
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
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Switch } from "@/components/ui/switch";
import {
  apiProfilesFor,
  RouteModelMappingsEditor,
  routeModelMappingsAreValid,
  routeModelMappingsFromRoute,
  routeModelMappingsRequest,
  type EditableRouteModelMappings,
} from "@/components/provider-accounts/route-model-mappings-editor";
import { useI18n } from "@/i18n/locale-provider";
import type {
  GrokVideoOutput,
  ProviderAccountModels,
  ProviderAccountView,
  ProviderModelView,
  ProviderRoute,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

type Translate = ReturnType<typeof useI18n>["t"];
type SchedulingMode = "active" | "draining";
export type AccountSettingsTab = "scheduling" | "models" | "video-storage";
type VideoStorageProvider = "qiniu-kodo" | "aws-s3" | "s3-compatible";

const QINIU_REGIONS = [
  { id: "cn-east-1" },
  { id: "cn-east-2" },
  { id: "cn-north-1" },
  { id: "cn-south-1" },
  { id: "us-north-1" },
  { id: "ap-southeast-1" },
  { id: "ap-southeast-2" },
  { id: "ap-southeast-3" },
] as const;
const DEFAULT_QINIU_REGION = QINIU_REGIONS[0].id;

export function AccountSchedulingSheet({
  account,
  open,
  onOpenChange,
  initialTab = "scheduling",
  groups,
  routes,
  models,
  onSaved,
}: {
  account: ProviderAccountView | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  initialTab?: AccountSettingsTab;
  groups: ProviderRoute[];
  routes: ProviderRoute[];
  models: ProviderModelView[];
  onSaved: () => void;
}) {
  const { locale, t } = useI18n();
  const [tab, setTab] = useState<AccountSettingsTab>(initialTab);
  const [maxConcurrency, setMaxConcurrency] = useState(1);
  const [mode, setMode] = useState<SchedulingMode>("active");
  const [pending, setPending] = useState(false);
  const [modelSettings, setModelSettings] =
    useState<ProviderAccountModels | null>(null);
  const [enabledModels, setEnabledModels] = useState<Set<string>>(new Set());
  const [modelSearch, setModelSearch] = useState("");
  const [modelLoading, setModelLoading] = useState(false);
  const [selectedRouteId, setSelectedRouteId] = useState("");
  const [activeApiProfile, setActiveApiProfile] = useState("");
  const [routeMappingDrafts, setRouteMappingDrafts] = useState<
    Record<string, EditableRouteModelMappings>
  >({});
  const [videoOutput, setVideoOutput] = useState<GrokVideoOutput | null>(null);
  const [videoOutputLoading, setVideoOutputLoading] = useState(false);
  const [videoOutputEnabled, setVideoOutputEnabled] = useState(false);
  const [videoStorageProvider, setVideoStorageProvider] =
    useState<VideoStorageProvider>("qiniu-kodo");
  const [videoBucket, setVideoBucket] = useState("");
  const [videoRegion, setVideoRegion] = useState<string>(DEFAULT_QINIU_REGION);
  const [videoEndpoint, setVideoEndpoint] = useState(
    qiniuEndpoint(DEFAULT_QINIU_REGION),
  );
  const [videoKeyPrefix, setVideoKeyPrefix] = useState("grok-videos");
  const [videoExpiresSecs, setVideoExpiresSecs] = useState(900);
  const [videoAccessKeyId, setVideoAccessKeyId] = useState("");
  const [videoSecretAccessKey, setVideoSecretAccessKey] = useState("");
  const modelRequestId = useRef(0);
  const videoOutputRequestId = useRef(0);

  useEffect(() => {
    if (!account || !open) return;
    setTab(initialTab);
    setMaxConcurrency(Number(account.max_concurrency));
    setMode(account.scheduling_state === "active" ? "active" : "draining");
    setModelSearch("");
    setModelSettings(null);
    setVideoOutput(null);
    setVideoOutputEnabled(false);
    setVideoStorageProvider("qiniu-kodo");
    setVideoBucket("");
    setVideoRegion(DEFAULT_QINIU_REGION);
    setVideoEndpoint(qiniuEndpoint(DEFAULT_QINIU_REGION));
    setVideoKeyPrefix("grok-videos");
    setVideoExpiresSecs(900);
    setVideoAccessKeyId("");
    setVideoSecretAccessKey("");
    const firstRoute = routes[0] ?? null;
    setSelectedRouteId(firstRoute?.route_id ?? "");
    setActiveApiProfile(
      firstRoute?.model_mappings[0]?.api_profile ??
        (firstRoute
          ? apiProfilesFor(firstRoute.provider_id, firstRoute.operation_id)[0]
              ?.id
          : "") ??
        "",
    );
    setRouteMappingDrafts(
      Object.fromEntries(
        routes.map((route) => [
          route.route_id,
          routeModelMappingsFromRoute(route),
        ]),
      ),
    );
    void loadModels(account.provider_account_id);
    if (account.provider_id === "grok-cli") {
      void loadVideoOutput(account.provider_account_id);
    }
  }, [account, initialTab, open, routes]);

  async function loadModels(providerAccountId: string) {
    const requestId = ++modelRequestId.current;
    setModelLoading(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/admin/v1/provider-accounts/${providerAccountId}/models`,
      );
      if (!response.ok) throw new Error(await responseMessage(response, t));
      const settings = (await response.json()) as ProviderAccountModels;
      if (requestId !== modelRequestId.current) return;
      setModelSettings(settings);
      setEnabledModels(
        new Set(
          settings.models
            .filter((model) =>
              settings.mode === "automatic"
                ? model.configurable
                : model.enabled,
            )
            .map(modelKey),
        ),
      );
    } catch (error) {
      if (requestId !== modelRequestId.current) return;
      toast.error(
        error instanceof Error
          ? error.message
          : t({
              en: "Failed to load model permissions",
              "zh-CN": "模型权限加载失败",
              ja: "モデル権限を読み込めませんでした",
              ko: "모델 권한을 불러오지 못했습니다",
            }),
      );
    } finally {
      if (requestId === modelRequestId.current) setModelLoading(false);
    }
  }

  async function loadVideoOutput(providerAccountId: string) {
    const requestId = ++videoOutputRequestId.current;
    setVideoOutputLoading(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/admin/v1/provider-accounts/${providerAccountId}/grok-video-output`,
      );
      if (!response.ok) throw new Error(await responseMessage(response, t));
      const output = (await response.json()) as GrokVideoOutput;
      if (requestId !== videoOutputRequestId.current) return;
      setVideoOutput(output);
      setVideoOutputEnabled(output.enabled);
      const storageProvider = storageProviderFor(output);
      const region =
        output.region ??
        (storageProvider === "qiniu-kodo" ? DEFAULT_QINIU_REGION : "");
      setVideoStorageProvider(storageProvider);
      setVideoBucket(output.bucket ?? "");
      setVideoRegion(region);
      setVideoEndpoint(
        output.endpoint ??
          (storageProvider === "qiniu-kodo" ? qiniuEndpoint(region) : ""),
      );
      setVideoKeyPrefix(output.key_prefix ?? "grok-videos");
      setVideoExpiresSecs(output.expires_secs ?? 900);
      setVideoAccessKeyId("");
      setVideoSecretAccessKey("");
    } catch (error) {
      if (requestId !== videoOutputRequestId.current) return;
      toast.error(
        error instanceof Error
          ? error.message
          : t({
              en: "Failed to load video output settings",
              "zh-CN": "视频输出配置加载失败",
              ja: "動画出力設定を読み込めませんでした",
              ko: "동영상 출력 설정을 불러오지 못했습니다",
            }),
      );
    } finally {
      if (requestId === videoOutputRequestId.current)
        setVideoOutputLoading(false);
    }
  }

  const visibleCatalogModels = useMemo(() => {
    const query = modelSearch.trim().toLocaleLowerCase();
    return models.filter((model) => {
      if (!query) return true;
      return (
        model.display_name.toLocaleLowerCase().includes(query) ||
        model.model_id.toLocaleLowerCase().includes(query)
      );
    });
  }, [modelSearch, models]);

  const selectedRoute =
    routes.find((route) => route.route_id === selectedRouteId) ?? null;
  const routeMappings = routeMappingDrafts[selectedRouteId] ?? {};

  if (!account) return null;
  const currentAccount = account;
  const allocated = Number(account.allocated_count);
  const memberships = groups.flatMap((group) =>
    group.members
      .filter(
        (member) => member.provider_account_id === account.provider_account_id,
      )
      .map((member) => ({ group, member })),
  );
  const capacityValid =
    Number.isInteger(maxConcurrency) &&
    maxConcurrency >= 1 &&
    maxConcurrency <= 64;
  const newVideoCredentialsComplete =
    videoAccessKeyId.trim().length > 0 && videoSecretAccessKey.length > 0;
  const newVideoCredentialsEmpty =
    videoAccessKeyId.trim().length === 0 && videoSecretAccessKey.length === 0;
  const videoOutputValid =
    !videoOutputEnabled ||
    (validBucket(videoBucket.trim()) &&
      videoRegion.trim().length > 0 &&
      videoKeyPrefix.trim().length > 0 &&
      Number.isInteger(videoExpiresSecs) &&
      videoExpiresSecs >= 60 &&
      videoExpiresSecs <= 3600 &&
      validHttpsEndpoint(videoEndpoint.trim()) &&
      (newVideoCredentialsComplete ||
        (newVideoCredentialsEmpty &&
          Boolean(videoOutput?.has_read_write_credentials))));

  async function saveScheduling() {
    if (!capacityValid) return;
    setPending(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/admin/v1/provider-accounts/${currentAccount.provider_account_id}`,
        {
          method: "PATCH",
          body: JSON.stringify({
            expected_control_version: currentAccount.control_version,
            max_concurrency: maxConcurrency,
            accepting_new_work: mode === "active",
          }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response, t));
      toast.success(
        mode === "active"
          ? t({
              en: "Scheduling settings saved",
              "zh-CN": "调度设置已保存",
              ja: "スケジューリング設定を保存しました",
              ko: "스케줄링 설정이 저장되었습니다",
            })
          : t({
              en: "Account is now draining",
              "zh-CN": "账户已开始排空",
              ja: "アカウントのドレインを開始しました",
              ko: "계정 드레이닝이 시작되었습니다",
            }),
      );
      onOpenChange(false);
      onSaved();
    } catch (error) {
      toast.error(
        error instanceof Error
          ? error.message
          : t({
              en: "Failed to save account settings",
              "zh-CN": "账户设置保存失败",
              ja: "アカウント設定を保存できませんでした",
              ko: "계정 설정을 저장하지 못했습니다",
            }),
      );
    } finally {
      setPending(false);
    }
  }

  async function saveModelConfiguration() {
    if (
      !modelSettings ||
      !selectedRoute ||
      !routeModelMappingsAreValid(Object.values(routeMappings))
    )
      return;
    setPending(true);
    try {
      const enabled = modelSettings.models.filter(
        (model) => model.configurable && enabledModels.has(modelKey(model)),
      );
      const response = await consoleFetch(
        `/api/gateway/admin/v1/provider-accounts/${currentAccount.provider_account_id}/model-configuration`,
        {
          method: "PUT",
          body: JSON.stringify({
            expected_model_version: modelSettings.version,
            mode: "allowlist",
            enabled_models: enabled.map((model) => ({
              model_id: model.model_id,
              media_kind: model.media_kind,
            })),
            route_id: selectedRoute.route_id,
            expected_route_revision: selectedRoute.revision,
            model_mappings: routeModelMappingsRequest(routeMappings),
          }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response, t));
      toast.success(
        t(
          {
            en: "{operation} model settings saved",
            "zh-CN": "{operation}模型配置已保存",
            ja: "{operation}のモデル設定を保存しました",
            ko: "{operation} 모델 설정이 저장되었습니다",
          },
          { operation: operationLabel(t, selectedRoute.operation_id) },
        ),
      );
      onSaved();
      await loadModels(currentAccount.provider_account_id);
    } catch (error) {
      toast.error(
        error instanceof Error
          ? error.message
          : t({
              en: "Failed to save model settings",
              "zh-CN": "模型配置保存失败",
              ja: "モデル設定を保存できませんでした",
              ko: "모델 설정을 저장하지 못했습니다",
            }),
      );
    } finally {
      setPending(false);
    }
  }

  async function saveVideoOutput() {
    if (!videoOutput || !videoOutputValid) return;
    setPending(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/admin/v1/provider-accounts/${currentAccount.provider_account_id}/grok-video-output`,
        {
          method: "PUT",
          body: JSON.stringify({
            enabled: videoOutputEnabled,
            bucket: videoBucket.trim(),
            region: videoRegion.trim(),
            endpoint: videoEndpoint.trim() || null,
            key_prefix: videoKeyPrefix.trim(),
            expires_secs: videoExpiresSecs,
            access_key_id: videoAccessKeyId.trim() || null,
            secret_access_key: videoSecretAccessKey || null,
          }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response, t));
      const output = (await response.json()) as GrokVideoOutput;
      setVideoOutput(output);
      setVideoOutputEnabled(output.enabled);
      const storageProvider = storageProviderFor(output);
      const region =
        output.region ??
        (storageProvider === "qiniu-kodo" ? DEFAULT_QINIU_REGION : "");
      setVideoStorageProvider(storageProvider);
      setVideoBucket(output.bucket ?? "");
      setVideoRegion(region);
      setVideoEndpoint(
        output.endpoint ??
          (storageProvider === "qiniu-kodo" ? qiniuEndpoint(region) : ""),
      );
      setVideoKeyPrefix(output.key_prefix ?? "grok-videos");
      setVideoExpiresSecs(output.expires_secs ?? 900);
      setVideoAccessKeyId("");
      setVideoSecretAccessKey("");
      toast.success(
        output.ready
          ? t({
              en: "Grok video output is ready",
              "zh-CN": "Grok 视频输出已就绪",
              ja: "Grok 動画出力の準備ができました",
              ko: "Grok 동영상 출력이 준비되었습니다",
            })
          : t({
              en: "Video output settings saved",
              "zh-CN": "视频输出配置已保存",
              ja: "動画出力設定を保存しました",
              ko: "동영상 출력 설정이 저장되었습니다",
            }),
      );
      onSaved();
    } catch (error) {
      toast.error(
        error instanceof Error
          ? error.message
          : t({
              en: "Failed to save video output settings",
              "zh-CN": "视频输出配置保存失败",
              ja: "動画出力設定を保存できませんでした",
              ko: "동영상 출력 설정을 저장하지 못했습니다",
            }),
      );
    } finally {
      setPending(false);
    }
  }

  function selectRoute(routeId: string) {
    const route = routes.find((item) => item.route_id === routeId);
    if (!route) return;
    setSelectedRouteId(routeId);
    setActiveApiProfile(
      route.model_mappings[0]?.api_profile ??
        apiProfilesFor(route.provider_id, route.operation_id)[0]?.id ??
        "",
    );
  }

  function selectVideoStorageProvider(provider: VideoStorageProvider) {
    setVideoStorageProvider(provider);
    if (provider === "qiniu-kodo") {
      const region = isQiniuRegion(videoRegion)
        ? videoRegion
        : DEFAULT_QINIU_REGION;
      setVideoRegion(region);
      setVideoEndpoint(qiniuEndpoint(region));
      return;
    }
    if (provider === "aws-s3") {
      if (isQiniuRegion(videoRegion)) setVideoRegion("us-east-1");
      setVideoEndpoint("");
      return;
    }
    if (isQiniuEndpoint(videoEndpoint)) {
      setVideoRegion("");
      setVideoEndpoint("");
    }
  }

  function selectQiniuRegion(region: string) {
    setVideoRegion(region);
    setVideoEndpoint(qiniuEndpoint(region));
  }

  function toggleModel(key: string) {
    setEnabledModels((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  function toggleAccountModel(model: ProviderAccountModels["models"][number]) {
    const key = modelKey(model);
    const disabling = enabledModels.has(key);
    toggleModel(key);
    if (disabling) {
      setRouteMappingDrafts((current) =>
        Object.fromEntries(
          Object.entries(current).map(([routeId, mappings]) => [
            routeId,
            Object.fromEntries(
              Object.entries(mappings).filter(
                ([, mapping]) =>
                  mapping.providerModelId !== model.model_id ||
                  mapping.mediaKind !== model.media_kind,
              ),
            ),
          ]),
        ),
      );
    }
  }

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="flex h-full w-full flex-col gap-0 overflow-hidden p-0 sm:max-w-3xl">
        <SheetHeader className="border-b px-5 py-4 pr-14 sm:px-6">
          <div className="flex min-w-0 items-center gap-3 text-left">
            <div className="flex size-9 shrink-0 items-center justify-center rounded-md border bg-muted/40">
              <SquareTerminal className="size-4" aria-hidden="true" />
            </div>
            <div className="min-w-0 flex-1">
              <div className="flex min-w-0 items-center gap-2">
                <SheetTitle className="truncate text-base">
                  {account.display_name ?? account.account_key}
                </SheetTitle>
                <SchedulingBadge t={t} state={account.scheduling_state} />
              </div>
              <SheetDescription className="mt-1 truncate">
                {providerLabel(t, account.provider_id)} ·{" "}
                {account.account_email ?? account.account_key}
              </SheetDescription>
            </div>
          </div>
        </SheetHeader>

        <Tabs
          value={tab}
          onValueChange={(value) => setTab(value as AccountSettingsTab)}
          className="flex min-h-0 flex-1 flex-col gap-0"
        >
          <div className="shrink-0 overflow-x-auto border-b px-5 sm:px-6">
            <TabsList variant="line" className="w-full">
              <TabsTrigger value="scheduling" variant="line">
                <Gauge className="size-4" aria-hidden="true" />
                {t({
                  en: "Scheduling",
                  "zh-CN": "调度设置",
                  ja: "スケジューリング",
                  ko: "스케줄링",
                })}
              </TabsTrigger>
              <TabsTrigger value="models" variant="line">
                <Layers3 className="size-4" aria-hidden="true" />
                {t({
                  en: "Models",
                  "zh-CN": "模型配置",
                  ja: "モデル",
                  ko: "모델",
                })}
              </TabsTrigger>
              {account.provider_id === "grok-cli" ? (
                <TabsTrigger value="video-storage" variant="line">
                  <CloudUpload className="size-4" aria-hidden="true" />
                  {t({
                    en: "Video storage",
                    "zh-CN": "视频存储",
                    ja: "動画ストレージ",
                    ko: "동영상 스토리지",
                  })}
                </TabsTrigger>
              ) : null}
            </TabsList>
          </div>

          <TabsContent
            value="scheduling"
            className="min-h-0 flex-1 overflow-y-auto px-5 sm:px-6"
          >
            <section className="space-y-4 py-5">
              <h3 className="text-sm font-medium">
                {t({
                  en: "Account status",
                  "zh-CN": "账户状态",
                  ja: "アカウント状態",
                  ko: "계정 상태",
                })}
              </h3>
              <dl className="grid min-w-0 gap-x-8 gap-y-4 sm:grid-cols-2">
                <AccountDetail
                  label={t({
                    en: "Account ID",
                    "zh-CN": "账户标识",
                    ja: "アカウント ID",
                    ko: "계정 ID",
                  })}
                  value={account.account_key}
                  mono
                  wide
                />
                <AccountDetail
                  label={t({
                    en: "Scheduling status",
                    "zh-CN": "调度状态",
                    ja: "スケジューリング状態",
                    ko: "스케줄링 상태",
                  })}
                >
                  <SchedulingBadge t={t} state={account.scheduling_state} />
                </AccountDetail>
                <AccountDetail
                  label={t({
                    en: "Authentication",
                    "zh-CN": "登录凭据",
                    ja: "認証情報",
                    ko: "인증 정보",
                  })}
                >
                  <CredentialBadge
                    t={t}
                    state={account.credential_lifecycle_state}
                  />
                </AccountDetail>
                <AccountDetail
                  label={t({
                    en: "Credential version",
                    "zh-CN": "凭据版本",
                    ja: "認証情報バージョン",
                    ko: "인증 정보 버전",
                  })}
                  value={`v${account.operational_credential_revision}`}
                />
                <AccountDetail
                  label={t({
                    en: "Access token expires",
                    "zh-CN": "访问令牌到期",
                    ja: "アクセストークン有効期限",
                    ko: "액세스 토큰 만료",
                  })}
                  value={formatCredentialTime(
                    t,
                    locale,
                    account.credential_access_expires_at_ms,
                  )}
                />
                <AccountDetail
                  label={t({
                    en: "Next check",
                    "zh-CN": "下次检查",
                    ja: "次回確認",
                    ko: "다음 확인",
                  })}
                  value={formatCredentialTime(
                    t,
                    locale,
                    account.credential_next_refresh_at_ms,
                  )}
                />
              </dl>
            </section>

            <Separator />

            <section className="space-y-4 py-5">
              <h3 className="text-sm font-medium">
                {t({
                  en: "Scheduling policy",
                  "zh-CN": "调度策略",
                  ja: "スケジューリングポリシー",
                  ko: "스케줄링 정책",
                })}
              </h3>
              <div className="grid gap-4 sm:grid-cols-2">
                <div className="space-y-2">
                  <Label htmlFor="account-max-concurrency">
                    {t({
                      en: "Maximum concurrency",
                      "zh-CN": "最大并发",
                      ja: "最大同時実行数",
                      ko: "최대 동시 실행",
                    })}
                  </Label>
                  <Input
                    id="account-max-concurrency"
                    type="number"
                    min={1}
                    max={64}
                    step={1}
                    value={maxConcurrency}
                    onChange={(event) =>
                      setMaxConcurrency(Number(event.target.value))
                    }
                    aria-invalid={!capacityValid}
                  />
                  <p className="text-xs text-muted-foreground">
                    {t({
                      en: "Allowed range: 1–64",
                      "zh-CN": "允许范围 1–64",
                      ja: "許容範囲: 1–64",
                      ko: "허용 범위: 1–64",
                    })}
                  </p>
                </div>
                <div className="space-y-2">
                  <Label>
                    {t({
                      en: "Job intake",
                      "zh-CN": "接单模式",
                      ja: "ジョブ受付",
                      ko: "작업 수락",
                    })}
                  </Label>
                  <Select
                    value={mode}
                    onValueChange={(value) => setMode(value as SchedulingMode)}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="active">
                        {schedulingLabel(t, "active")}
                      </SelectItem>
                      <SelectItem value="draining">
                        {schedulingLabel(t, "draining")}
                      </SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-xs text-muted-foreground">
                    {t({
                      en: "Draining does not interrupt jobs already running",
                      "zh-CN": "排空不会中断正在执行的任务",
                      ja: "ドレインしても実行中のジョブは中断されません",
                      ko: "드레이닝은 실행 중인 작업을 중단하지 않습니다",
                    })}
                  </p>
                </div>
              </div>
              <div className="space-y-2">
                <div className="flex items-center justify-between gap-4 text-sm">
                  <span className="text-muted-foreground">
                    {t({
                      en: "Live concurrency",
                      "zh-CN": "实时并发",
                      ja: "リアルタイム同時実行数",
                      ko: "실시간 동시 실행",
                    })}
                  </span>
                  <span className="tabular-nums">
                    {account.allocated_count} / {account.max_concurrency}
                    <span className="ml-2 text-muted-foreground">
                      {t(
                        {
                          en: "{count} available",
                          "zh-CN": "可用 {count}",
                          ja: "利用可能 {count}",
                          ko: "사용 가능 {count}",
                        },
                        { count: account.available_capacity },
                      )}
                    </span>
                  </span>
                </div>
                <Progress
                  value={
                    Number(account.max_concurrency) > 0
                      ? Math.min(
                          100,
                          (allocated / Number(account.max_concurrency)) * 100,
                        )
                      : 0
                  }
                  aria-label={t(
                    {
                      en: "Current concurrency {current}, maximum {maximum}",
                      "zh-CN": "当前并发 {current}，最大并发 {maximum}",
                      ja: "現在の同時実行数 {current}、最大 {maximum}",
                      ko: "현재 동시 실행 {current}, 최대 {maximum}",
                    },
                    {
                      current: account.allocated_count,
                      maximum: account.max_concurrency,
                    },
                  )}
                  className="h-1.5"
                />
              </div>
              {maxConcurrency < allocated ? (
                <p className="border-l-2 border-foreground/30 pl-3 text-sm text-muted-foreground">
                  {t(
                    {
                      en: "Running jobs will not be interrupted. New jobs wait until usage falls below {limit}.",
                      "zh-CN":
                        "现有任务不会中断；新任务会等待占用降至 {limit} 以下。",
                      ja: "実行中のジョブは中断されません。新しいジョブは使用数が {limit} 未満になるまで待機します。",
                      ko: "실행 중인 작업은 중단되지 않습니다. 새 작업은 사용량이 {limit} 미만이 될 때까지 대기합니다.",
                    },
                    { limit: maxConcurrency },
                  )}
                </p>
              ) : null}
            </section>

            <Separator />

            <section className="space-y-3 py-5">
              <div className="flex items-center gap-2">
                <h3 className="text-sm font-medium">
                  {t({
                    en: "Account groups",
                    "zh-CN": "所属账户组",
                    ja: "所属アカウントグループ",
                    ko: "소속 계정 그룹",
                  })}
                </h3>
                <Badge variant="secondary" className="font-normal">
                  {memberships.length}
                </Badge>
              </div>
              {memberships.length === 0 ? (
                <div className="flex min-h-20 items-center justify-center gap-2 text-sm text-muted-foreground">
                  <UsersRound className="size-4" aria-hidden="true" />
                  {t({
                    en: "Not assigned to an account group",
                    "zh-CN": "尚未加入账户组",
                    ja: "アカウントグループに未所属です",
                    ko: "계정 그룹에 속해 있지 않습니다",
                  })}
                </div>
              ) : (
                memberships.map(({ group, member }) => (
                  <div
                    key={group.route_id}
                    className="flex items-center justify-between gap-4 border-t py-3 first:border-t-0"
                  >
                    <div className="min-w-0">
                      <p className="truncate text-sm font-medium">
                        {group.display_name}
                      </p>
                      <p className="mt-0.5 text-xs text-muted-foreground">
                        {t(
                          {
                            en: "Version {version}",
                            "zh-CN": "版本 {version}",
                            ja: "バージョン {version}",
                            ko: "버전 {version}",
                          },
                          { version: group.revision },
                        )}
                      </p>
                    </div>
                    <span className="whitespace-nowrap text-xs tabular-nums text-muted-foreground">
                      {t(
                        {
                          en: "P {priority} · W {weight} · Reserve {reserve}%",
                          "zh-CN":
                            "P {priority} · W {weight} · 保留 {reserve}%",
                          ja: "P {priority} · W {weight} · 予約 {reserve}%",
                          ko: "P {priority} · W {weight} · 보존 {reserve}%",
                        },
                        {
                          priority: member.priority,
                          weight: member.weight,
                          reserve: member.minimum_remaining_percent,
                        },
                      )}
                    </span>
                  </div>
                ))
              )}
            </section>
          </TabsContent>

          <TabsContent
            value="models"
            className="min-h-0 min-w-0 flex-1 space-y-5 overflow-y-auto overflow-x-hidden px-5 py-5 sm:px-6"
          >
            {routes.length === 0 ? (
              <div className="flex min-h-40 items-center justify-center border text-sm text-muted-foreground">
                {t({
                  en: "Image and video generation are not enabled for this account",
                  "zh-CN": "此账户未启用图片或视频生成能力",
                  ja: "このアカウントでは画像または動画生成が有効になっていません",
                  ko: "이 계정에는 이미지 또는 동영상 생성이 활성화되어 있지 않습니다",
                })}
              </div>
            ) : (
              <>
                <div className="grid min-w-0 gap-3 sm:grid-cols-[11rem_minmax(0,1fr)]">
                  <Select value={selectedRouteId} onValueChange={selectRoute}>
                    <SelectTrigger
                      aria-label={t({
                        en: "Select generation capability",
                        "zh-CN": "选择生成能力",
                        ja: "生成機能を選択",
                        ko: "생성 기능 선택",
                      })}
                    >
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {routes.map((route) => (
                        <SelectItem key={route.route_id} value={route.route_id}>
                          {operationLabel(t, route.operation_id)} · v
                          {route.revision}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <div className="relative min-w-0">
                    <Search
                      className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
                      aria-hidden="true"
                    />
                    <Input
                      value={modelSearch}
                      onChange={(event) => setModelSearch(event.target.value)}
                      placeholder={t({
                        en: "Search models",
                        "zh-CN": "搜索模型",
                        ja: "モデルを検索",
                        ko: "모델 검색",
                      })}
                      className="pl-9"
                      aria-label={t({
                        en: "Search models",
                        "zh-CN": "搜索模型",
                        ja: "モデルを検索",
                        ko: "모델 검색",
                      })}
                    />
                  </div>
                </div>

                {modelLoading || !modelSettings ? (
                  <div className="flex min-h-40 items-center justify-center border">
                    <LoaderCircle
                      className="animate-spin"
                      aria-label={t({
                        en: "Loading models",
                        "zh-CN": "正在加载模型",
                        ja: "モデルを読み込み中",
                        ko: "모델 불러오는 중",
                      })}
                    />
                  </div>
                ) : selectedRoute ? (
                  <RouteModelMappingsEditor
                    providerId={selectedRoute.provider_id}
                    operationId={selectedRoute.operation_id}
                    models={visibleCatalogModels}
                    activeApiProfile={activeApiProfile}
                    onActiveApiProfileChange={setActiveApiProfile}
                    mappings={routeMappings}
                    onMappingsChange={(mappings) =>
                      setRouteMappingDrafts((current) => ({
                        ...current,
                        [selectedRoute.route_id]: mappings,
                      }))
                    }
                    accountModels={modelSettings.models}
                    enabledAccountModels={enabledModels}
                    onAccountModelToggle={toggleAccountModel}
                  />
                ) : null}
              </>
            )}
          </TabsContent>

          {account.provider_id === "grok-cli" ? (
            <TabsContent
              value="video-storage"
              className="min-h-0 min-w-0 flex-1 overflow-y-auto overflow-x-hidden px-5 sm:px-6"
            >
              {videoOutputLoading || !videoOutput ? (
                <div className="flex min-h-40 items-center justify-center">
                  <LoaderCircle
                    className="animate-spin"
                    aria-label={t({
                      en: "Loading video output settings",
                      "zh-CN": "正在加载视频输出配置",
                      ja: "動画出力設定を読み込み中",
                      ko: "동영상 출력 설정 불러오는 중",
                    })}
                  />
                </div>
              ) : (
                <div className="space-y-6 py-5">
                  <section className="flex items-start justify-between gap-6">
                    <div className="min-w-0 space-y-1">
                      <div className="flex items-center gap-2">
                        <h3 className="text-sm font-medium">
                          {t({
                            en: "Zero data retention video output",
                            "zh-CN": "零数据保留视频输出",
                            ja: "ゼロデータ保持の動画出力",
                            ko: "데이터 미보존 동영상 출력",
                          })}
                        </h3>
                        <Badge
                          variant={videoOutput.ready ? "secondary" : "outline"}
                          className="font-normal"
                        >
                          {videoOutput.ready
                            ? t({
                                en: "Ready",
                                "zh-CN": "已就绪",
                                ja: "準備完了",
                                ko: "준비됨",
                              })
                            : t({
                                en: "Setup required",
                                "zh-CN": "待配置",
                                ja: "要設定",
                                ko: "설정 필요",
                              })}
                        </Badge>
                      </div>
                      <p className="text-sm text-muted-foreground">
                        {t({
                          en: "xAI writes generated results to S3-compatible storage dedicated to this account.",
                          "zh-CN":
                            "生成结果由 xAI 写入此账户专属的 S3 兼容存储。",
                          ja: "生成結果は xAI により、このアカウント専用の S3 互換ストレージへ書き込まれます。",
                          ko: "xAI가 생성 결과를 이 계정 전용 S3 호환 스토리지에 기록합니다.",
                        })}
                      </p>
                    </div>
                    <Switch
                      checked={videoOutputEnabled}
                      onCheckedChange={setVideoOutputEnabled}
                      aria-label={t({
                        en: "Enable Grok video output storage",
                        "zh-CN": "启用 Grok 视频输出存储",
                        ja: "Grok 動画出力ストレージを有効化",
                        ko: "Grok 동영상 출력 스토리지 활성화",
                      })}
                    />
                  </section>

                  <Alert>
                    <Info aria-hidden="true" />
                    <AlertTitle>
                      {videoStorageProvider === "qiniu-kodo"
                        ? t({
                            en: "Qiniu Kodo upload destination",
                            "zh-CN": "七牛云 Kodo 上传目标",
                            ja: "Qiniu Kodo アップロード先",
                            ko: "Qiniu Kodo 업로드 대상",
                          })
                        : t({
                            en: "Public upload destination",
                            "zh-CN": "公网上传目标",
                            ja: "公開アップロード先",
                            ko: "공개 업로드 대상",
                          })}
                    </AlertTitle>
                    <AlertDescription>
                      {videoStorageProvider === "qiniu-kodo"
                        ? t({
                            en: "Receive xAI uploads through Qiniu's S3-compatible API. Select the region that contains the bucket. Leave credentials blank to keep the current values.",
                            "zh-CN":
                              "使用七牛云 S3 兼容接口接收 xAI 上传；请选择 Bucket 所在区域。密钥留空会保留当前凭据。",
                            ja: "Qiniu の S3 互換 API で xAI のアップロードを受け取ります。Bucket のリージョンを選択してください。認証情報を空欄にすると現在の値が保持されます。",
                            ko: "Qiniu S3 호환 API로 xAI 업로드를 수신합니다. Bucket이 있는 리전을 선택하세요. 자격 증명을 비워 두면 현재 값이 유지됩니다.",
                          })
                        : t({
                            en: "The endpoint must support HTTPS and allow xAI to upload with presigned URLs. Leave credentials blank to keep the current values.",
                            "zh-CN":
                              "端点必须支持 HTTPS，并允许 xAI 使用预签名地址上传。密钥留空会保留当前凭据。",
                            ja: "エンドポイントは HTTPS と、署名付き URL を使った xAI のアップロードに対応している必要があります。認証情報を空欄にすると現在の値が保持されます。",
                            ko: "엔드포인트는 HTTPS와 사전 서명 URL을 사용한 xAI 업로드를 지원해야 합니다. 자격 증명을 비워 두면 현재 값이 유지됩니다.",
                          })}
                    </AlertDescription>
                  </Alert>

                  <fieldset
                    disabled={!videoOutputEnabled}
                    className="space-y-5 disabled:opacity-50"
                  >
                    <div className="space-y-2">
                      <Label>
                        {t({
                          en: "Storage service",
                          "zh-CN": "存储服务",
                          ja: "ストレージサービス",
                          ko: "스토리지 서비스",
                        })}
                      </Label>
                      <Select
                        value={videoStorageProvider}
                        onValueChange={(value) =>
                          selectVideoStorageProvider(
                            value as VideoStorageProvider,
                          )
                        }
                      >
                        <SelectTrigger
                          aria-label={t({
                            en: "Select video storage service",
                            "zh-CN": "选择视频存储服务",
                            ja: "動画ストレージサービスを選択",
                            ko: "동영상 스토리지 서비스 선택",
                          })}
                        >
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="qiniu-kodo">
                            {t({
                              en: "Qiniu Kodo",
                              "zh-CN": "七牛云 Kodo",
                              ja: "Qiniu Kodo",
                              ko: "Qiniu Kodo",
                            })}
                          </SelectItem>
                          <SelectItem value="aws-s3">AWS S3</SelectItem>
                          <SelectItem value="s3-compatible">
                            {t({
                              en: "Other S3-compatible storage",
                              "zh-CN": "其他 S3 兼容存储",
                              ja: "その他の S3 互換ストレージ",
                              ko: "기타 S3 호환 스토리지",
                            })}
                          </SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    <div className="grid gap-4 sm:grid-cols-2">
                      <div className="space-y-2">
                        <Label htmlFor="grok-video-bucket">
                          {t({
                            en: "Bucket",
                            "zh-CN": "存储桶",
                            ja: "バケット",
                            ko: "버킷",
                          })}
                        </Label>
                        <Input
                          id="grok-video-bucket"
                          value={videoBucket}
                          onChange={(event) =>
                            setVideoBucket(event.target.value)
                          }
                          placeholder="factory-videos"
                          autoComplete="off"
                        />
                        {videoStorageProvider === "qiniu-kodo" ? (
                          <p className="text-xs text-muted-foreground">
                            {t({
                              en: "Use the S3 bucket name shown in the Qiniu console.",
                              "zh-CN": "使用七牛控制台显示的 S3 空间名。",
                              ja: "Qiniu コンソールに表示される S3 バケット名を使用してください。",
                              ko: "Qiniu 콘솔에 표시된 S3 버킷 이름을 사용하세요.",
                            })}
                          </p>
                        ) : null}
                      </div>
                      <div className="space-y-2">
                        <Label>
                          {t({
                            en: "Region",
                            "zh-CN": "区域",
                            ja: "リージョン",
                            ko: "리전",
                          })}
                        </Label>
                        {videoStorageProvider === "qiniu-kodo" ? (
                          <Select
                            value={videoRegion}
                            onValueChange={selectQiniuRegion}
                          >
                            <SelectTrigger
                              aria-label={t({
                                en: "Select Qiniu region",
                                "zh-CN": "选择七牛云区域",
                                ja: "Qiniu リージョンを選択",
                                ko: "Qiniu 리전 선택",
                              })}
                            >
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              {QINIU_REGIONS.map((region) => (
                                <SelectItem key={region.id} value={region.id}>
                                  {qiniuRegionLabel(t, region.id)} · {region.id}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        ) : (
                          <Input
                            id="grok-video-region"
                            value={videoRegion}
                            onChange={(event) =>
                              setVideoRegion(event.target.value)
                            }
                            placeholder={
                              videoStorageProvider === "aws-s3"
                                ? "us-east-1"
                                : t({
                                    en: "Region ID",
                                    "zh-CN": "区域 ID",
                                    ja: "リージョン ID",
                                    ko: "리전 ID",
                                  })
                            }
                            autoComplete="off"
                          />
                        )}
                      </div>
                    </div>

                    <div className="space-y-2">
                      <Label htmlFor="grok-video-endpoint">
                        {t({
                          en: "S3-compatible endpoint",
                          "zh-CN": "S3 兼容端点",
                          ja: "S3 互換エンドポイント",
                          ko: "S3 호환 엔드포인트",
                        })}
                      </Label>
                      <Input
                        id="grok-video-endpoint"
                        type="url"
                        value={videoEndpoint}
                        onChange={(event) =>
                          setVideoEndpoint(event.target.value)
                        }
                        placeholder={
                          videoStorageProvider === "aws-s3"
                            ? t({
                                en: "AWS S3 uses its default endpoint",
                                "zh-CN": "AWS S3 使用默认端点",
                                ja: "AWS S3 はデフォルトのエンドポイントを使用します",
                                ko: "AWS S3는 기본 엔드포인트를 사용합니다",
                              })
                            : t({
                                en: "Enter an HTTPS S3-compatible endpoint",
                                "zh-CN": "填写 HTTPS S3 兼容端点",
                                ja: "HTTPS の S3 互換エンドポイントを入力",
                                ko: "HTTPS S3 호환 엔드포인트 입력",
                              })
                        }
                        readOnly={videoStorageProvider === "qiniu-kodo"}
                        autoComplete="off"
                      />
                    </div>

                    <div className="grid gap-4 sm:grid-cols-2">
                      <div className="space-y-2">
                        <Label htmlFor="grok-video-key-prefix">
                          {t({
                            en: "Object prefix",
                            "zh-CN": "对象前缀",
                            ja: "オブジェクトプレフィックス",
                            ko: "객체 접두사",
                          })}
                        </Label>
                        <Input
                          id="grok-video-key-prefix"
                          value={videoKeyPrefix}
                          onChange={(event) =>
                            setVideoKeyPrefix(event.target.value)
                          }
                          placeholder="grok-videos"
                          autoComplete="off"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="grok-video-expires">
                          {t({
                            en: "Upload URL lifetime",
                            "zh-CN": "上传地址有效期",
                            ja: "アップロード URL の有効期間",
                            ko: "업로드 URL 유효 기간",
                          })}
                        </Label>
                        <div className="relative">
                          <Input
                            id="grok-video-expires"
                            type="number"
                            min={60}
                            max={3600}
                            step={60}
                            value={videoExpiresSecs}
                            onChange={(event) =>
                              setVideoExpiresSecs(Number(event.target.value))
                            }
                            className="pr-12"
                          />
                          <span className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-sm text-muted-foreground">
                            {t({
                              en: "sec",
                              "zh-CN": "秒",
                              ja: "秒",
                              ko: "초",
                            })}
                          </span>
                        </div>
                      </div>
                    </div>

                    <Separator />

                    <div className="space-y-1">
                      <h3 className="text-sm font-medium">
                        {t({
                          en: "Write credentials",
                          "zh-CN": "写入凭据",
                          ja: "書き込み認証情報",
                          ko: "쓰기 자격 증명",
                        })}
                      </h3>
                      <p className="text-sm text-muted-foreground">
                        {videoOutput.has_read_write_credentials
                          ? t({
                              en: "Write credentials are saved. Enter new values only when rotating them.",
                              "zh-CN":
                                "已保存写入凭据；仅在需要轮换时重新填写。",
                              ja: "書き込み認証情報は保存済みです。ローテーション時のみ新しい値を入力してください。",
                              ko: "쓰기 자격 증명이 저장되어 있습니다. 교체할 때만 새 값을 입력하세요.",
                            })
                          : t({
                              en: "Write credentials have not been saved.",
                              "zh-CN": "尚未保存写入凭据。",
                              ja: "書き込み認証情報はまだ保存されていません。",
                              ko: "쓰기 자격 증명이 아직 저장되지 않았습니다.",
                            })}
                      </p>
                    </div>
                    <div className="grid gap-4 sm:grid-cols-2">
                      <div className="space-y-2">
                        <Label htmlFor="grok-video-access-key">
                          {videoStorageProvider === "qiniu-kodo"
                            ? t({
                                en: "Qiniu Access Key",
                                "zh-CN": "七牛 Access Key",
                                ja: "Qiniu Access Key",
                                ko: "Qiniu Access Key",
                              })
                            : "Access Key ID"}
                        </Label>
                        <Input
                          id="grok-video-access-key"
                          value={videoAccessKeyId}
                          onChange={(event) =>
                            setVideoAccessKeyId(event.target.value)
                          }
                          placeholder={
                            videoOutput.has_read_write_credentials
                              ? t({
                                  en: "Leave blank to keep",
                                  "zh-CN": "留空以保留",
                                  ja: "空欄で現在の値を保持",
                                  ko: "비워 두면 현재 값 유지",
                                })
                              : t({
                                  en: "Required",
                                  "zh-CN": "必填",
                                  ja: "必須",
                                  ko: "필수",
                                })
                          }
                          autoComplete="new-password"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="grok-video-secret-key">
                          {videoStorageProvider === "qiniu-kodo"
                            ? t({
                                en: "Qiniu Secret Key",
                                "zh-CN": "七牛密钥",
                                ja: "Qiniu シークレットキー",
                                ko: "Qiniu 비밀 키",
                              })
                            : t({
                                en: "Secret Access Key",
                                "zh-CN": "秘密访问密钥",
                                ja: "シークレットアクセスキー",
                                ko: "비밀 액세스 키",
                              })}
                        </Label>
                        <Input
                          id="grok-video-secret-key"
                          type="password"
                          value={videoSecretAccessKey}
                          onChange={(event) =>
                            setVideoSecretAccessKey(event.target.value)
                          }
                          placeholder={
                            videoOutput.has_read_write_credentials
                              ? t({
                                  en: "Leave blank to keep",
                                  "zh-CN": "留空以保留",
                                  ja: "空欄で現在の値を保持",
                                  ko: "비워 두면 현재 값 유지",
                                })
                              : t({
                                  en: "Required",
                                  "zh-CN": "必填",
                                  ja: "必須",
                                  ko: "필수",
                                })
                          }
                          autoComplete="new-password"
                        />
                      </div>
                    </div>
                  </fieldset>
                </div>
              )}
            </TabsContent>
          ) : null}
        </Tabs>

        <SheetFooter className="shrink-0 gap-2 border-t bg-background px-5 py-4 sm:px-6">
          <Button
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={pending}
            className="w-full sm:w-auto"
          >
            {t({
              en: "Cancel",
              "zh-CN": "取消",
              ja: "キャンセル",
              ko: "취소",
            })}
          </Button>
          <Button
            onClick={() =>
              void (tab === "scheduling"
                ? saveScheduling()
                : tab === "models"
                  ? saveModelConfiguration()
                  : saveVideoOutput())
            }
            disabled={
              pending ||
              (tab === "scheduling"
                ? !capacityValid
                : tab === "models"
                  ? modelLoading ||
                    !modelSettings ||
                    !selectedRoute ||
                    !routeModelMappingsAreValid(Object.values(routeMappings))
                  : videoOutputLoading || !videoOutput || !videoOutputValid)
            }
            className="w-full sm:w-auto"
          >
            {pending ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <Save aria-hidden="true" />
            )}
            {tab === "scheduling"
              ? t({
                  en: "Save scheduling",
                  "zh-CN": "保存调度设置",
                  ja: "スケジューリングを保存",
                  ko: "스케줄링 저장",
                })
              : tab === "models"
                ? t(
                    {
                      en: "Save model settings · v{version}",
                      "zh-CN": "保存模型配置 · v{version}",
                      ja: "モデル設定を保存 · v{version}",
                      ko: "모델 설정 저장 · v{version}",
                    },
                    { version: (selectedRoute?.revision ?? 0) + 1 },
                  )
                : t({
                    en: "Save video storage",
                    "zh-CN": "保存视频存储",
                    ja: "動画ストレージを保存",
                    ko: "동영상 스토리지 저장",
                  })}
          </Button>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  );
}

function AccountDetail({
  label,
  value,
  mono = false,
  wide = false,
  children,
}: {
  label: string;
  value?: string;
  mono?: boolean;
  wide?: boolean;
  children?: ReactNode;
}) {
  return (
    <div className={`min-w-0 space-y-1.5 ${wide ? "sm:col-span-2" : ""}`}>
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd
        className={`min-w-0 truncate text-sm ${mono ? "font-mono" : ""}`}
        title={value}
      >
        {children ?? value}
      </dd>
    </div>
  );
}

function SchedulingBadge({
  t,
  state,
}: {
  t: Translate;
  state: ProviderAccountView["scheduling_state"];
}) {
  const active = state === "active";
  return (
    <Badge variant="outline" className="shrink-0 gap-1.5 font-normal">
      <span
        className={`size-1.5 rounded-full ${
          active ? "bg-emerald-500" : "bg-muted-foreground"
        }`}
        aria-hidden="true"
      />
      {schedulingLabel(t, state)}
    </Badge>
  );
}

function CredentialBadge({
  t,
  state,
}: {
  t: Translate;
  state: ProviderAccountView["credential_lifecycle_state"];
}) {
  return (
    <Badge
      variant={state === "reauth_required" ? "destructive" : "secondary"}
      className="font-normal"
    >
      {credentialStateLabel(t, state)}
    </Badge>
  );
}

function modelKey(model: { model_id: string; media_kind: string }) {
  return `${model.media_kind}:${model.model_id}`;
}

function credentialStateLabel(
  t: Translate,
  state: ProviderAccountView["credential_lifecycle_state"],
) {
  if (state === "active")
    return t({
      en: "Active",
      "zh-CN": "正常",
      ja: "有効",
      ko: "정상",
    });
  if (state === "refresh_due")
    return t({
      en: "Refresh due",
      "zh-CN": "等待续期",
      ja: "更新待ち",
      ko: "갱신 대기",
    });
  if (state === "refreshing")
    return t({
      en: "Refreshing",
      "zh-CN": "正在续期",
      ja: "更新中",
      ko: "갱신 중",
    });
  if (state === "reauth_required")
    return t({
      en: "Sign-in required",
      "zh-CN": "需要重新登录",
      ja: "再ログインが必要",
      ko: "다시 로그인 필요",
    });
  return t({
    en: "Waiting for runtime",
    "zh-CN": "等待运行环境",
    ja: "実行環境を待機中",
    ko: "런타임 대기 중",
  });
}

function formatCredentialTime(
  t: Translate,
  locale: string,
  value: number | null,
) {
  if (value === null)
    return t({
      en: "Not provided by CLI",
      "zh-CN": "CLI 未提供",
      ja: "CLI から提供されていません",
      ko: "CLI에서 제공하지 않음",
    });
  return new Intl.DateTimeFormat(locale, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function schedulingLabel(t: Translate, state: string) {
  if (state === "active")
    return t({
      en: "Accepting new jobs",
      "zh-CN": "接收新任务",
      ja: "新しいジョブを受付中",
      ko: "새 작업 수락 중",
    });
  if (state === "draining")
    return t({
      en: "Draining",
      "zh-CN": "排空中",
      ja: "ドレイン中",
      ko: "드레이닝 중",
    });
  return t({
    en: "Disabled",
    "zh-CN": "已停用",
    ja: "無効",
    ko: "비활성화됨",
  });
}

function providerLabel(t: Translate, providerId: string) {
  if (providerId === "openai-codex") return "Codex";
  if (providerId === "grok-cli") return "Grok";
  if (providerId === "dreamina-cli")
    return t({
      en: "Dreamina",
      "zh-CN": "即梦",
      ja: "Dreamina",
      ko: "Dreamina",
    });
  return providerId;
}

function operationLabel(t: Translate, operationId: string) {
  if (operationId === "images.generations")
    return t({
      en: "Image generation",
      "zh-CN": "图片生成",
      ja: "画像生成",
      ko: "이미지 생성",
    });
  if (operationId === "videos.generations")
    return t({
      en: "Video generation",
      "zh-CN": "视频生成",
      ja: "動画生成",
      ko: "동영상 생성",
    });
  return operationId;
}

function qiniuRegionLabel(
  t: Translate,
  region: (typeof QINIU_REGIONS)[number]["id"],
) {
  const labels = {
    "cn-east-1": {
      en: "East China · Zhejiang",
      "zh-CN": "华东-浙江",
      ja: "中国東部・浙江",
      ko: "중국 동부 · 저장",
    },
    "cn-east-2": {
      en: "East China · Zhejiang 2",
      "zh-CN": "华东-浙江 2",
      ja: "中国東部・浙江 2",
      ko: "중국 동부 · 저장 2",
    },
    "cn-north-1": {
      en: "North China · Hebei",
      "zh-CN": "华北-河北",
      ja: "中国北部・河北",
      ko: "중국 북부 · 허베이",
    },
    "cn-south-1": {
      en: "South China · Guangdong",
      "zh-CN": "华南-广东",
      ja: "中国南部・広東",
      ko: "중국 남부 · 광둥",
    },
    "us-north-1": {
      en: "North America · Los Angeles",
      "zh-CN": "北美-洛杉矶",
      ja: "北米・ロサンゼルス",
      ko: "북미 · 로스앤젤레스",
    },
    "ap-southeast-1": {
      en: "Asia Pacific · Singapore",
      "zh-CN": "亚太-新加坡",
      ja: "アジア太平洋・シンガポール",
      ko: "아시아 태평양 · 싱가포르",
    },
    "ap-southeast-2": {
      en: "Asia Pacific · Hanoi",
      "zh-CN": "亚太-河内",
      ja: "アジア太平洋・ハノイ",
      ko: "아시아 태평양 · 하노이",
    },
    "ap-southeast-3": {
      en: "Asia Pacific · Ho Chi Minh City",
      "zh-CN": "亚太-胡志明",
      ja: "アジア太平洋・ホーチミン",
      ko: "아시아 태평양 · 호찌민",
    },
  } as const;
  return t(labels[region]);
}

function validBucket(value: string) {
  return (
    value.length >= 3 &&
    value.length <= 63 &&
    /^[a-z0-9][a-z0-9.-]*[a-z0-9]$/.test(value)
  );
}

function validHttpsEndpoint(value: string) {
  if (!value) return true;
  try {
    return new URL(value).protocol === "https:";
  } catch {
    return false;
  }
}

function storageProviderFor(output: GrokVideoOutput): VideoStorageProvider {
  if (output.endpoint && isQiniuEndpoint(output.endpoint)) return "qiniu-kodo";
  if (output.enabled && !output.endpoint) return "aws-s3";
  if (output.endpoint) return "s3-compatible";
  return "qiniu-kodo";
}

function isQiniuRegion(region: string) {
  return QINIU_REGIONS.some((candidate) => candidate.id === region);
}

function qiniuEndpoint(region: string) {
  return `https://s3.${region}.qiniucs.com`;
}

function isQiniuEndpoint(endpoint: string) {
  try {
    const url = new URL(endpoint);
    return (
      url.protocol === "https:" &&
      (url.hostname === "qiniucs.com" || url.hostname.endsWith(".qiniucs.com"))
    );
  } catch {
    return false;
  }
}

async function responseMessage(response: Response, t: Translate) {
  try {
    const body = (await response.json()) as {
      error?: string | { message?: string };
    };
    if (typeof body.error === "string") return body.error;
    if (body.error && typeof body.error.message === "string")
      return body.error.message;
  } catch {
    // Preserve the stable fallback for non-JSON proxy failures.
  }
  return t(
    {
      en: "Request failed ({status})",
      "zh-CN": "请求失败 ({status})",
      ja: "リクエストに失敗しました ({status})",
      ko: "요청 실패 ({status})",
    },
    { status: response.status },
  );
}
