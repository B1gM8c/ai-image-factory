"use client";

import { useEffect, useState, type ReactNode } from "react";
import { Layers3, LoaderCircle, Save, Users } from "lucide-react";
import { toast } from "sonner";
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
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  apiProfilesFor,
  defaultRouteModelMappings,
  RouteModelMappingsEditor,
  routeModelMappingsAreValid,
  routeModelMappingsFromRoute,
  routeModelMappingsRequest,
  type EditableRouteModelMappings,
} from "@/components/provider-accounts/route-model-mappings-editor";
import type {
  ProviderAccountView,
  ProviderModelView,
  ProviderRoute,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

type Strategy = "quota_aware_least_loaded" | "priority_weighted";
type UnknownQuotaPolicy = "allow" | "block";
type MemberPolicy = {
  priority: number;
  weight: number;
  minimumRemainingPercent: number;
};
export function RoutePolicySheet({
  open,
  onOpenChange,
  accounts,
  models,
  route,
  onSaved,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  accounts: ProviderAccountView[];
  models: ProviderModelView[];
  route: ProviderRoute | null;
  onSaved: () => void;
}) {
  const [name, setName] = useState("");
  const [providerId, setProviderId] = useState("openai-codex");
  const [operationId, setOperationId] = useState("images.generations");
  const [activeApiProfile, setActiveApiProfile] = useState("openai-images-v1");
  const [strategy, setStrategy] = useState<Strategy>(
    "quota_aware_least_loaded",
  );
  const [unknownQuotaPolicy, setUnknownQuotaPolicy] =
    useState<UnknownQuotaPolicy>("allow");
  const [quotaFreshnessMinutes, setQuotaFreshnessMinutes] = useState(15);
  const [memberPolicies, setMemberPolicies] = useState<
    Record<string, MemberPolicy>
  >({});
  const [modelMappings, setModelMappings] =
    useState<EditableRouteModelMappings>({});
  const [pending, setPending] = useState(false);
  const accountRoute = route?.route_kind === "account";

  useEffect(() => {
    if (!open) return;
    setName(route?.display_name ?? "");
    const nextProviderId = route?.provider_id ?? preferredProvider(accounts);
    const nextOperationId =
      route?.operation_id ?? preferredOperation(models, nextProviderId);
    setProviderId(nextProviderId);
    setOperationId(nextOperationId);
    const profiles = apiProfilesFor(nextProviderId, nextOperationId);
    setActiveApiProfile(
      route?.model_mappings[0]?.api_profile ?? profiles[0]?.id ?? "",
    );
    setStrategy(
      (route?.selection_strategy as Strategy | undefined) ??
        "quota_aware_least_loaded",
    );
    setUnknownQuotaPolicy(route?.unknown_quota_policy ?? "allow");
    setQuotaFreshnessMinutes(
      Math.round((route?.quota_freshness_ms ?? 900_000) / 60_000),
    );
    setMemberPolicies(
      Object.fromEntries(
        (route?.members ?? []).map((member) => [
          member.provider_account_id,
          {
            priority: member.priority,
            weight: member.weight,
            minimumRemainingPercent: member.minimum_remaining_percent,
          },
        ]),
      ),
    );
    setModelMappings(
      route
        ? routeModelMappingsFromRoute(route)
        : defaultRouteModelMappings(models, nextProviderId, nextOperationId),
    );
  }, [open, route, accounts, models]);

  const routeAccounts = accounts.filter(
    (account) => account.provider_id === providerId,
  );
  const providerOptions = [
    ...new Set(accounts.map((account) => account.provider_id)),
  ];
  const operationOptions = [
    ...new Set(
      models
        .filter(
          (model) =>
            model.provider_id === providerId &&
            model.adapter_state === "supported",
        )
        .flatMap((model) => model.operation_ids),
    ),
  ].filter((operation) => apiProfilesFor(providerId, operation).length > 0);

  const selectedMemberCount = Object.keys(memberPolicies).length;
  const routeValid =
    name.trim().length > 0 &&
    Number.isInteger(quotaFreshnessMinutes) &&
    quotaFreshnessMinutes >= 1 &&
    quotaFreshnessMinutes <= 1_440 &&
    selectedMemberCount > 0 &&
    routeModelMappingsAreValid(Object.values(modelMappings)) &&
    Object.values(memberPolicies).every(
      (member) =>
        Number.isInteger(member.priority) &&
        member.priority >= -1_000 &&
        member.priority <= 1_000 &&
        Number.isInteger(member.weight) &&
        member.weight >= 1 &&
        member.weight <= 1_000_000 &&
        Number.isInteger(member.minimumRemainingPercent) &&
        member.minimumRemainingPercent >= 0 &&
        member.minimumRemainingPercent <= 100,
    );

  function toggleMember(accountId: string) {
    setMemberPolicies((current) => {
      if (current[accountId]) {
        const next = { ...current };
        delete next[accountId];
        return next;
      }
      return {
        ...current,
        [accountId]: { priority: 0, weight: 100, minimumRemainingPercent: 0 },
      };
    });
  }

  function updateMember(
    accountId: string,
    field: keyof MemberPolicy,
    value: number,
  ) {
    setMemberPolicies((current) => ({
      ...current,
      [accountId]: { ...current[accountId], [field]: value },
    }));
  }

  function changeProvider(nextProviderId: string) {
    const nextOperationId = preferredOperation(models, nextProviderId);
    setProviderId(nextProviderId);
    setOperationId(nextOperationId);
    setMemberPolicies({});
    setModelMappings(
      defaultRouteModelMappings(models, nextProviderId, nextOperationId),
    );
    setActiveApiProfile(
      apiProfilesFor(nextProviderId, nextOperationId)[0]?.id ?? "",
    );
  }

  function changeOperation(nextOperationId: string) {
    setOperationId(nextOperationId);
    setMemberPolicies({});
    setModelMappings(
      defaultRouteModelMappings(models, providerId, nextOperationId),
    );
    setActiveApiProfile(
      apiProfilesFor(providerId, nextOperationId)[0]?.id ?? "",
    );
  }

  async function save() {
    if (!routeValid) return;
    setPending(true);
    try {
      const members = Object.entries(memberPolicies).map(
        ([providerAccountId, policy]) => ({
          provider_account_id: providerAccountId,
          priority: policy.priority,
          weight: policy.weight,
          minimum_remaining_percent: policy.minimumRemainingPercent,
        }),
      );
      const mappings = routeModelMappingsRequest(modelMappings);
      const response = await consoleFetch(
        route
          ? `/api/gateway/admin/v1/provider-routes/${route.route_id}`
          : "/api/gateway/admin/v1/provider-routes",
        {
          method: route ? "PUT" : "POST",
          body: JSON.stringify(
            route
              ? {
                  expected_revision: route.revision,
                  display_name: name.trim(),
                  selection_strategy: strategy,
                  quota_freshness_ms: quotaFreshnessMinutes * 60_000,
                  unknown_quota_policy: unknownQuotaPolicy,
                  members,
                  model_mappings: mappings,
                }
              : {
                  route_key: `group.${slug(name)}.${crypto.randomUUID().slice(0, 8)}`,
                  display_name: name.trim(),
                  provider_id: providerId,
                  operation_id: operationId,
                  selection_strategy: strategy,
                  quota_freshness_ms: quotaFreshnessMinutes * 60_000,
                  unknown_quota_policy: unknownQuotaPolicy,
                  members,
                  model_mappings: mappings,
                },
          ),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      toast.success(
        accountRoute
          ? "API 模型映射新版本已发布"
          : route
            ? "账号组新版本已发布"
            : "账号组已创建",
      );
      onOpenChange(false);
      onSaved();
    } catch (error) {
      toast.error(
        error instanceof Error
          ? error.message
          : accountRoute
            ? "API 模型映射保存失败"
            : "账号组保存失败",
      );
    } finally {
      setPending(false);
    }
  }

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="flex h-full w-full flex-col overflow-hidden p-0 sm:max-w-3xl">
        <SheetHeader className="border-b px-5 py-5 pr-12 sm:px-6">
          <SheetTitle>
            {accountRoute
              ? `${route.display_name} · API 模型`
              : route
                ? "编辑账号组"
                : "新建账号组"}
          </SheetTitle>
          <SheetDescription>
            {accountRoute
              ? `${providerLabel(route.provider_id)} · ${operationLabel(route.operation_id)}；保存后发布不可变的新版本。`
              : route
                ? "保存会发布不可变的新版本，已进入队列的任务仍使用原版本。"
                : "API Key 绑定账号组后，只会在组内账号之间调度。"}
          </SheetDescription>
        </SheetHeader>

        <Tabs
          key={`${route?.route_id ?? "new"}:${route?.revision ?? 0}`}
          defaultValue={accountRoute ? "models" : "scheduling"}
          className="flex min-h-0 flex-1 flex-col"
        >
          {!accountRoute ? (
            <div className="shrink-0 overflow-x-auto border-b px-5 sm:px-6">
              <TabsList variant="line">
                <TabsTrigger value="scheduling" variant="line">
                  <Users className="size-4" aria-hidden="true" />
                  成员与调度
                </TabsTrigger>
                <TabsTrigger value="models" variant="line">
                  <Layers3 className="size-4" aria-hidden="true" />
                  对外模型
                </TabsTrigger>
              </TabsList>
            </div>
          ) : null}
          <TabsContent
            value="scheduling"
            className="min-h-0 flex-1 space-y-6 overflow-y-auto px-5 py-5 sm:px-6"
          >
            <div className="space-y-2">
              <Label htmlFor="group-name">组名称</Label>
              <Input
                id="group-name"
                value={name}
                onChange={(event) => setName(event.target.value)}
                placeholder="例如：图片生产组"
                maxLength={128}
              />
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label>CLI 供应商</Label>
                <Select
                  value={providerId}
                  onValueChange={changeProvider}
                  disabled={Boolean(route)}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {providerOptions.map((provider) => (
                      <SelectItem key={provider} value={provider}>
                        {providerLabel(provider)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>API 能力</Label>
                <Select
                  value={operationId}
                  onValueChange={changeOperation}
                  disabled={Boolean(route)}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {operationOptions.map((operation) => (
                      <SelectItem key={operation} value={operation}>
                        {operationLabel(operation)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div className="grid gap-4 md:grid-cols-3">
              <div className="space-y-2">
                <Label>选择规则</Label>
                <Select
                  value={strategy}
                  onValueChange={(value) => setStrategy(value as Strategy)}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="quota_aware_least_loaded">
                      额度与负载优先
                    </SelectItem>
                    <SelectItem value="priority_weighted">
                      优先级与权重
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>额度数据缺失</Label>
                <Select
                  value={unknownQuotaPolicy}
                  onValueChange={(value) =>
                    setUnknownQuotaPolicy(value as UnknownQuotaPolicy)
                  }
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="allow">继续调度</SelectItem>
                    <SelectItem value="block">暂停调度</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label htmlFor="quota-freshness">快照有效期（分钟）</Label>
                <Input
                  id="quota-freshness"
                  type="number"
                  min={1}
                  max={1_440}
                  step={1}
                  value={quotaFreshnessMinutes}
                  onChange={(event) =>
                    setQuotaFreshnessMinutes(Number(event.target.value))
                  }
                />
              </div>
            </div>

            <section className="space-y-2">
              <div className="flex items-center justify-between gap-4">
                <Label>成员策略</Label>
                <span className="text-xs text-muted-foreground">
                  已选择 {selectedMemberCount} 个账号
                </span>
              </div>
              <div className="border">
                <div className="hidden grid-cols-[2.5rem_minmax(12rem,1fr)_7rem_7rem_8rem] items-center border-b bg-muted/40 px-3 py-2 text-xs font-medium text-muted-foreground md:grid">
                  <span />
                  <span>账号</span>
                  <span>优先级</span>
                  <span>权重</span>
                  <span>最低保留额度</span>
                </div>
                {routeAccounts.length === 0 ? (
                  <div className="flex min-h-24 items-center justify-center text-sm text-muted-foreground">
                    该供应商暂无可用账号
                  </div>
                ) : (
                  routeAccounts.map((account) => {
                    const policy = memberPolicies[account.provider_account_id];
                    const checked = Boolean(policy);
                    return (
                      <div
                        key={account.provider_account_id}
                        className="grid grid-cols-[2rem_minmax(0,1fr)] gap-x-2 gap-y-3 border-b px-3 py-3 last:border-b-0 md:grid-cols-[2.5rem_minmax(12rem,1fr)_7rem_7rem_8rem] md:items-center md:gap-0 md:py-2"
                      >
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() =>
                            toggleMember(account.provider_account_id)
                          }
                          className="mt-1 size-4 accent-primary md:mt-0"
                          aria-label={`选择 ${account.display_name ?? account.account_key}`}
                        />
                        <div className="min-w-0 pr-2 text-sm">
                          <p className="truncate font-medium">
                            {account.display_name ?? account.account_key}
                          </p>
                          <p className="truncate text-xs text-muted-foreground">
                            {accountStatusLabel(account)} · 可用并发{" "}
                            {account.available_capacity}
                          </p>
                        </div>
                        {checked ? (
                          <>
                            <MobilePolicyField label="优先级">
                              <PolicyNumberInput
                                label="优先级"
                                value={policy.priority}
                                min={-1_000}
                                max={1_000}
                                onChange={(value) =>
                                  updateMember(
                                    account.provider_account_id,
                                    "priority",
                                    value,
                                  )
                                }
                              />
                            </MobilePolicyField>
                            <MobilePolicyField label="权重">
                              <PolicyNumberInput
                                label="权重"
                                value={policy.weight}
                                min={1}
                                max={1_000_000}
                                onChange={(value) =>
                                  updateMember(
                                    account.provider_account_id,
                                    "weight",
                                    value,
                                  )
                                }
                              />
                            </MobilePolicyField>
                            <MobilePolicyField label="最低保留额度">
                              <div className="flex items-center gap-1.5">
                                <PolicyNumberInput
                                  label="最低保留额度"
                                  value={policy.minimumRemainingPercent}
                                  min={0}
                                  max={100}
                                  onChange={(value) =>
                                    updateMember(
                                      account.provider_account_id,
                                      "minimumRemainingPercent",
                                      value,
                                    )
                                  }
                                />
                                <span className="text-xs text-muted-foreground">
                                  %
                                </span>
                              </div>
                            </MobilePolicyField>
                          </>
                        ) : (
                          <div className="col-span-2 hidden md:col-span-3 md:block" />
                        )}
                      </div>
                    );
                  })
                )}
              </div>
            </section>
          </TabsContent>

          <TabsContent
            value="models"
            className="min-h-0 flex-1 space-y-5 overflow-y-auto px-5 py-5 sm:px-6"
          >
            {accountRoute ? (
              <div className="grid gap-3 border-b pb-5 text-sm sm:grid-cols-2">
                <div>
                  <span className="text-muted-foreground">CLI 供应商</span>
                  <p className="mt-1 font-medium">
                    {providerLabel(providerId)}
                  </p>
                </div>
                <div>
                  <span className="text-muted-foreground">API 能力</span>
                  <p className="mt-1 font-medium">
                    {operationLabel(operationId)}
                  </p>
                </div>
              </div>
            ) : null}
            <RouteModelMappingsEditor
              providerId={providerId}
              operationId={operationId}
              models={models}
              activeApiProfile={activeApiProfile}
              onActiveApiProfileChange={setActiveApiProfile}
              mappings={modelMappings}
              onMappingsChange={setModelMappings}
            />
          </TabsContent>
        </Tabs>

        <SheetFooter className="border-t px-5 py-4 sm:px-6">
          <Button
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={pending}
          >
            取消
          </Button>
          <Button onClick={() => void save()} disabled={pending || !routeValid}>
            {pending ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : route ? (
              <Save aria-hidden="true" />
            ) : (
              <Users aria-hidden="true" />
            )}
            {route ? `发布版本 ${route.revision + 1}` : "创建账号组"}
          </Button>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  );
}

function MobilePolicyField({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="col-span-2 grid grid-cols-[8rem_minmax(0,1fr)] items-center gap-3 pl-8 md:col-span-1 md:block md:pl-0">
      <span className="text-xs text-muted-foreground md:hidden">{label}</span>
      {children}
    </div>
  );
}

function PolicyNumberInput({
  label,
  value,
  min,
  max,
  onChange,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  onChange: (value: number) => void;
}) {
  return (
    <Input
      type="number"
      className="h-8 w-full md:w-24"
      aria-label={label}
      value={value}
      min={min}
      max={max}
      step={1}
      onChange={(event) => onChange(Number(event.target.value))}
    />
  );
}

function accountStatusLabel(account: ProviderAccountView) {
  if (account.environment_state === "invalid") return "登录失效";
  if (account.scheduling_state === "draining") return "排空中";
  if (account.scheduling_state === "disabled") return "已停用";
  if (account.configuration_status !== "configured") return "配置异常";
  return "接收新任务";
}

function preferredProvider(accounts: ProviderAccountView[]) {
  const providers = new Set(accounts.map((account) => account.provider_id));
  if (providers.has("openai-codex")) return "openai-codex";
  return providers.values().next().value ?? "openai-codex";
}

function preferredOperation(models: ProviderModelView[], providerId: string) {
  const operations = new Set(
    models
      .filter(
        (model) =>
          model.provider_id === providerId &&
          model.adapter_state === "supported",
      )
      .flatMap((model) => model.operation_ids),
  );
  if (operations.has("images.generations")) return "images.generations";
  return operations.values().next().value ?? "images.generations";
}

function providerLabel(providerId: string) {
  if (providerId === "openai-codex") return "Codex";
  if (providerId === "grok-cli") return "Grok";
  if (providerId === "dreamina-cli") return "即梦";
  return providerId;
}

function operationLabel(operationId: string) {
  if (operationId === "images.generations") return "图片生成";
  if (operationId === "images.edits") return "图片编辑";
  if (operationId === "videos.generations") return "视频生成";
  return operationId;
}

function slug(value: string) {
  const normalized = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ".")
    .replace(/^\.+|\.+$/g, "");
  return normalized.slice(0, 32) || "accounts";
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
