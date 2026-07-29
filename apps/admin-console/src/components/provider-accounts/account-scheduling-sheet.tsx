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
import type {
  GrokVideoOutput,
  ProviderAccountModels,
  ProviderAccountView,
  ProviderModelView,
  ProviderRoute,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

type SchedulingMode = "active" | "draining";
export type AccountSettingsTab = "scheduling" | "models" | "video-storage";
type VideoStorageProvider = "qiniu-kodo" | "aws-s3" | "s3-compatible";

const QINIU_REGIONS = [
  { id: "cn-east-1", label: "华东-浙江" },
  { id: "cn-east-2", label: "华东-浙江 2" },
  { id: "cn-north-1", label: "华北-河北" },
  { id: "cn-south-1", label: "华南-广东" },
  { id: "us-north-1", label: "北美-洛杉矶" },
  { id: "ap-southeast-1", label: "亚太-新加坡" },
  { id: "ap-southeast-2", label: "亚太-河内" },
  { id: "ap-southeast-3", label: "亚太-胡志明" },
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
      if (!response.ok) throw new Error(await responseMessage(response));
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
      toast.error(error instanceof Error ? error.message : "模型权限加载失败");
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
      if (!response.ok) throw new Error(await responseMessage(response));
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
        error instanceof Error ? error.message : "视频输出配置加载失败",
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
      if (!response.ok) throw new Error(await responseMessage(response));
      toast.success(mode === "active" ? "调度设置已保存" : "账户已开始排空");
      onOpenChange(false);
      onSaved();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "账户设置保存失败");
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
      if (!response.ok) throw new Error(await responseMessage(response));
      toast.success(
        `${operationLabel(selectedRoute.operation_id)}模型配置已保存`,
      );
      onSaved();
      await loadModels(currentAccount.provider_account_id);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "模型配置保存失败");
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
      if (!response.ok) throw new Error(await responseMessage(response));
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
        output.ready ? "Grok 视频输出已就绪" : "视频输出配置已保存",
      );
      onSaved();
    } catch (error) {
      toast.error(
        error instanceof Error ? error.message : "视频输出配置保存失败",
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
                <SchedulingBadge state={account.scheduling_state} />
              </div>
              <SheetDescription className="mt-1 truncate">
                {providerLabel(account.provider_id)} ·{" "}
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
                调度设置
              </TabsTrigger>
              <TabsTrigger value="models" variant="line">
                <Layers3 className="size-4" aria-hidden="true" />
                模型配置
              </TabsTrigger>
              {account.provider_id === "grok-cli" ? (
                <TabsTrigger value="video-storage" variant="line">
                  <CloudUpload className="size-4" aria-hidden="true" />
                  视频存储
                </TabsTrigger>
              ) : null}
            </TabsList>
          </div>

          <TabsContent
            value="scheduling"
            className="min-h-0 flex-1 overflow-y-auto px-5 sm:px-6"
          >
            <section className="space-y-4 py-5">
              <h3 className="text-sm font-medium">账户状态</h3>
              <dl className="grid min-w-0 gap-x-8 gap-y-4 sm:grid-cols-2">
                <AccountDetail
                  label="账户标识"
                  value={account.account_key}
                  mono
                  wide
                />
                <AccountDetail label="调度状态">
                  <SchedulingBadge state={account.scheduling_state} />
                </AccountDetail>
                <AccountDetail label="登录凭据">
                  <CredentialBadge state={account.credential_lifecycle_state} />
                </AccountDetail>
                <AccountDetail
                  label="凭据版本"
                  value={`v${account.operational_credential_revision}`}
                />
                <AccountDetail
                  label="访问令牌到期"
                  value={formatCredentialTime(
                    account.credential_access_expires_at_ms,
                  )}
                />
                <AccountDetail
                  label="下次检查"
                  value={formatCredentialTime(
                    account.credential_next_refresh_at_ms,
                  )}
                />
              </dl>
            </section>

            <Separator />

            <section className="space-y-4 py-5">
              <h3 className="text-sm font-medium">调度策略</h3>
              <div className="grid gap-4 sm:grid-cols-2">
                <div className="space-y-2">
                  <Label htmlFor="account-max-concurrency">最大并发</Label>
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
                  <p className="text-xs text-muted-foreground">允许范围 1–64</p>
                </div>
                <div className="space-y-2">
                  <Label>接单模式</Label>
                  <Select
                    value={mode}
                    onValueChange={(value) => setMode(value as SchedulingMode)}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="active">接收新任务</SelectItem>
                      <SelectItem value="draining">排空中</SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-xs text-muted-foreground">
                    排空不会中断正在执行的任务
                  </p>
                </div>
              </div>
              <div className="space-y-2">
                <div className="flex items-center justify-between gap-4 text-sm">
                  <span className="text-muted-foreground">实时并发</span>
                  <span className="tabular-nums">
                    {account.allocated_count} / {account.max_concurrency}
                    <span className="ml-2 text-muted-foreground">
                      可用 {account.available_capacity}
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
                  aria-label={`当前并发 ${account.allocated_count}，最大并发 ${account.max_concurrency}`}
                  className="h-1.5"
                />
              </div>
              {maxConcurrency < allocated ? (
                <p className="border-l-2 border-foreground/30 pl-3 text-sm text-muted-foreground">
                  现有任务不会中断；新任务会等待占用降至 {maxConcurrency} 以下。
                </p>
              ) : null}
            </section>

            <Separator />

            <section className="space-y-3 py-5">
              <div className="flex items-center gap-2">
                <h3 className="text-sm font-medium">所属账户组</h3>
                <Badge variant="secondary" className="font-normal">
                  {memberships.length}
                </Badge>
              </div>
              {memberships.length === 0 ? (
                <div className="flex min-h-20 items-center justify-center gap-2 text-sm text-muted-foreground">
                  <UsersRound className="size-4" aria-hidden="true" />
                  尚未加入账户组
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
                        版本 {group.revision}
                      </p>
                    </div>
                    <span className="whitespace-nowrap text-xs tabular-nums text-muted-foreground">
                      P {member.priority} · W {member.weight} · 保留{" "}
                      {member.minimum_remaining_percent}%
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
                此账户未启用图片或视频生成能力
              </div>
            ) : (
              <>
                <div className="grid min-w-0 gap-3 sm:grid-cols-[11rem_minmax(0,1fr)]">
                  <Select value={selectedRouteId} onValueChange={selectRoute}>
                    <SelectTrigger aria-label="选择生成能力">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {routes.map((route) => (
                        <SelectItem key={route.route_id} value={route.route_id}>
                          {operationLabel(route.operation_id)} · v
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
                      placeholder="搜索模型"
                      className="pl-9"
                      aria-label="搜索模型"
                    />
                  </div>
                </div>

                {modelLoading || !modelSettings ? (
                  <div className="flex min-h-40 items-center justify-center border">
                    <LoaderCircle
                      className="animate-spin"
                      aria-label="正在加载模型"
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
                    aria-label="正在加载视频输出配置"
                  />
                </div>
              ) : (
                <div className="space-y-6 py-5">
                  <section className="flex items-start justify-between gap-6">
                    <div className="min-w-0 space-y-1">
                      <div className="flex items-center gap-2">
                        <h3 className="text-sm font-medium">
                          零数据保留视频输出
                        </h3>
                        <Badge
                          variant={videoOutput.ready ? "secondary" : "outline"}
                          className="font-normal"
                        >
                          {videoOutput.ready ? "已就绪" : "待配置"}
                        </Badge>
                      </div>
                      <p className="text-sm text-muted-foreground">
                        生成结果由 xAI 写入此账户专属的 S3 兼容存储。
                      </p>
                    </div>
                    <Switch
                      checked={videoOutputEnabled}
                      onCheckedChange={setVideoOutputEnabled}
                      aria-label="启用 Grok 视频输出存储"
                    />
                  </section>

                  <Alert>
                    <Info aria-hidden="true" />
                    <AlertTitle>
                      {videoStorageProvider === "qiniu-kodo"
                        ? "七牛云 Kodo 上传目标"
                        : "公网上传目标"}
                    </AlertTitle>
                    <AlertDescription>
                      {videoStorageProvider === "qiniu-kodo"
                        ? "使用七牛云 S3 兼容接口接收 xAI 上传；请选择 Bucket 所在区域。"
                        : "端点必须支持 HTTPS，并允许 xAI 使用预签名地址上传。"}
                      密钥留空会保留当前凭据。
                    </AlertDescription>
                  </Alert>

                  <fieldset
                    disabled={!videoOutputEnabled}
                    className="space-y-5 disabled:opacity-50"
                  >
                    <div className="space-y-2">
                      <Label>存储服务</Label>
                      <Select
                        value={videoStorageProvider}
                        onValueChange={(value) =>
                          selectVideoStorageProvider(
                            value as VideoStorageProvider,
                          )
                        }
                      >
                        <SelectTrigger aria-label="选择视频存储服务">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="qiniu-kodo">
                            七牛云 Kodo
                          </SelectItem>
                          <SelectItem value="aws-s3">AWS S3</SelectItem>
                          <SelectItem value="s3-compatible">
                            其他 S3 兼容存储
                          </SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    <div className="grid gap-4 sm:grid-cols-2">
                      <div className="space-y-2">
                        <Label htmlFor="grok-video-bucket">Bucket</Label>
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
                            使用七牛控制台显示的 S3 空间名。
                          </p>
                        ) : null}
                      </div>
                      <div className="space-y-2">
                        <Label>Region</Label>
                        {videoStorageProvider === "qiniu-kodo" ? (
                          <Select
                            value={videoRegion}
                            onValueChange={selectQiniuRegion}
                          >
                            <SelectTrigger aria-label="选择七牛云区域">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              {QINIU_REGIONS.map((region) => (
                                <SelectItem key={region.id} value={region.id}>
                                  {region.label} · {region.id}
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
                                : "Region ID"
                            }
                            autoComplete="off"
                          />
                        )}
                      </div>
                    </div>

                    <div className="space-y-2">
                      <Label htmlFor="grok-video-endpoint">S3 兼容端点</Label>
                      <Input
                        id="grok-video-endpoint"
                        type="url"
                        value={videoEndpoint}
                        onChange={(event) =>
                          setVideoEndpoint(event.target.value)
                        }
                        placeholder={
                          videoStorageProvider === "aws-s3"
                            ? "AWS S3 使用默认端点"
                            : "填写 HTTPS S3 兼容端点"
                        }
                        readOnly={videoStorageProvider === "qiniu-kodo"}
                        autoComplete="off"
                      />
                    </div>

                    <div className="grid gap-4 sm:grid-cols-2">
                      <div className="space-y-2">
                        <Label htmlFor="grok-video-key-prefix">对象前缀</Label>
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
                          上传地址有效期
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
                            秒
                          </span>
                        </div>
                      </div>
                    </div>

                    <Separator />

                    <div className="space-y-1">
                      <h3 className="text-sm font-medium">写入凭据</h3>
                      <p className="text-sm text-muted-foreground">
                        {videoOutput.has_read_write_credentials
                          ? "已保存写入凭据；仅在需要轮换时重新填写。"
                          : "尚未保存写入凭据。"}
                      </p>
                    </div>
                    <div className="grid gap-4 sm:grid-cols-2">
                      <div className="space-y-2">
                        <Label htmlFor="grok-video-access-key">
                          {videoStorageProvider === "qiniu-kodo"
                            ? "七牛 Access Key"
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
                              ? "留空以保留"
                              : "必填"
                          }
                          autoComplete="new-password"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="grok-video-secret-key">
                          {videoStorageProvider === "qiniu-kodo"
                            ? "七牛 Secret Key"
                            : "Secret Access Key"}
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
                              ? "留空以保留"
                              : "必填"
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
            取消
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
              ? "保存调度设置"
              : tab === "models"
                ? `保存模型配置 · v${(selectedRoute?.revision ?? 0) + 1}`
                : "保存视频存储"}
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
  state,
}: {
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
      {schedulingLabel(state)}
    </Badge>
  );
}

function CredentialBadge({
  state,
}: {
  state: ProviderAccountView["credential_lifecycle_state"];
}) {
  return (
    <Badge
      variant={state === "reauth_required" ? "destructive" : "secondary"}
      className="font-normal"
    >
      {credentialStateLabel(state)}
    </Badge>
  );
}

function modelKey(model: { model_id: string; media_kind: string }) {
  return `${model.media_kind}:${model.model_id}`;
}

function credentialStateLabel(
  state: ProviderAccountView["credential_lifecycle_state"],
) {
  if (state === "active") return "正常";
  if (state === "refresh_due") return "等待续期";
  if (state === "refreshing") return "正在续期";
  if (state === "reauth_required") return "需要重新登录";
  return "等待运行环境";
}

function formatCredentialTime(value: number | null) {
  if (value === null) return "CLI 未提供";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function schedulingLabel(state: string) {
  if (state === "active") return "接收新任务";
  if (state === "draining") return "排空中";
  return "已停用";
}

function providerLabel(providerId: string) {
  if (providerId === "openai-codex") return "Codex";
  if (providerId === "grok-cli") return "Grok";
  if (providerId === "dreamina-cli") return "即梦";
  return providerId;
}

function operationLabel(operationId: string) {
  if (operationId === "images.generations") return "图片生成";
  if (operationId === "videos.generations") return "视频生成";
  return operationId;
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

async function responseMessage(response: Response) {
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
  return `请求失败 (${response.status})`;
}
