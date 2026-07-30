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
import { useI18n } from "@/i18n/locale-provider";
import type {
  ProviderAccountView,
  ProviderModelView,
  ProviderRoute,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

type Translate = ReturnType<typeof useI18n>["t"];
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
  const { t } = useI18n();
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
      if (!response.ok) throw new Error(await responseMessage(response, t));
      toast.success(
        accountRoute
          ? t({
              en: "A new API model mapping version has been published",
              "zh-CN": "API 模型映射新版本已发布",
              ja: "API モデルマッピングの新しいバージョンを公開しました",
              ko: "API 모델 매핑의 새 버전이 게시되었습니다",
            })
          : route
            ? t({
                en: "A new account group version has been published",
                "zh-CN": "账号组新版本已发布",
                ja: "アカウントグループの新しいバージョンを公開しました",
                ko: "계정 그룹의 새 버전이 게시되었습니다",
              })
            : t({
                en: "Account group created",
                "zh-CN": "账号组已创建",
                ja: "アカウントグループを作成しました",
                ko: "계정 그룹이 생성되었습니다",
              }),
      );
      onOpenChange(false);
      onSaved();
    } catch (error) {
      toast.error(
        error instanceof Error
          ? error.message
          : accountRoute
            ? t({
                en: "Failed to save API model mappings",
                "zh-CN": "API 模型映射保存失败",
                ja: "API モデルマッピングを保存できませんでした",
                ko: "API 모델 매핑을 저장하지 못했습니다",
              })
            : t({
                en: "Failed to save the account group",
                "zh-CN": "账号组保存失败",
                ja: "アカウントグループを保存できませんでした",
                ko: "계정 그룹을 저장하지 못했습니다",
              }),
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
              ? t(
                  {
                    en: "{name} · API models",
                    "zh-CN": "{name} · API 模型",
                    ja: "{name} · API モデル",
                    ko: "{name} · API 모델",
                  },
                  { name: route.display_name },
                )
              : route
                ? t({
                    en: "Edit account group",
                    "zh-CN": "编辑账号组",
                    ja: "アカウントグループを編集",
                    ko: "계정 그룹 편집",
                  })
                : t({
                    en: "Create account group",
                    "zh-CN": "新建账号组",
                    ja: "アカウントグループを作成",
                    ko: "계정 그룹 만들기",
                  })}
          </SheetTitle>
          <SheetDescription>
            {accountRoute
              ? t(
                  {
                    en: "{provider} · {operation}. Saving publishes an immutable new version.",
                    "zh-CN":
                      "{provider} · {operation}；保存后发布不可变的新版本。",
                    ja: "{provider} · {operation}。保存すると変更不可の新しいバージョンが公開されます。",
                    ko: "{provider} · {operation}. 저장하면 변경할 수 없는 새 버전이 게시됩니다.",
                  },
                  {
                    provider: providerLabel(t, route.provider_id),
                    operation: operationLabel(t, route.operation_id),
                  },
                )
              : route
                ? t({
                    en: "Saving publishes an immutable new version. Jobs already queued continue to use the previous version.",
                    "zh-CN":
                      "保存会发布不可变的新版本，已进入队列的任务仍使用原版本。",
                    ja: "保存すると変更不可の新しいバージョンが公開されます。キュー内の既存ジョブは以前のバージョンを引き続き使用します。",
                    ko: "저장하면 변경할 수 없는 새 버전이 게시됩니다. 이미 대기열에 있는 작업은 이전 버전을 계속 사용합니다.",
                  })
                : t({
                    en: "After an API key is assigned to an account group, requests are scheduled only across accounts in that group.",
                    "zh-CN":
                      "API Key 绑定账号组后，只会在组内账号之间调度。",
                    ja: "API キーをアカウントグループに割り当てると、そのグループ内のアカウントだけがスケジュール対象になります。",
                    ko: "API 키를 계정 그룹에 할당하면 해당 그룹의 계정만 스케줄링됩니다.",
                  })}
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
                  {t({
                    en: "Members & scheduling",
                    "zh-CN": "成员与调度",
                    ja: "メンバーとスケジューリング",
                    ko: "멤버 및 스케줄링",
                  })}
                </TabsTrigger>
                <TabsTrigger value="models" variant="line">
                  <Layers3 className="size-4" aria-hidden="true" />
                  {t({
                    en: "API models",
                    "zh-CN": "对外模型",
                    ja: "API モデル",
                    ko: "API 모델",
                  })}
                </TabsTrigger>
              </TabsList>
            </div>
          ) : null}
          <TabsContent
            value="scheduling"
            className="min-h-0 flex-1 space-y-6 overflow-y-auto px-5 py-5 sm:px-6"
          >
            <div className="space-y-2">
              <Label htmlFor="group-name">
                {t({
                  en: "Group name",
                  "zh-CN": "组名称",
                  ja: "グループ名",
                  ko: "그룹 이름",
                })}
              </Label>
              <Input
                id="group-name"
                value={name}
                onChange={(event) => setName(event.target.value)}
                placeholder={t({
                  en: "For example: Image production",
                  "zh-CN": "例如：图片生产组",
                  ja: "例: 画像生成グループ",
                  ko: "예: 이미지 생성 그룹",
                })}
                maxLength={128}
              />
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label>
                  {t({
                    en: "CLI provider",
                    "zh-CN": "CLI 供应商",
                    ja: "CLI プロバイダー",
                    ko: "CLI 공급자",
                  })}
                </Label>
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
                        {providerLabel(t, provider)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>
                  {t({
                    en: "API capability",
                    "zh-CN": "API 能力",
                    ja: "API 機能",
                    ko: "API 기능",
                  })}
                </Label>
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
                        {operationLabel(t, operation)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div className="grid gap-4 md:grid-cols-3">
              <div className="space-y-2">
                <Label>
                  {t({
                    en: "Selection strategy",
                    "zh-CN": "选择规则",
                    ja: "選択ルール",
                    ko: "선택 규칙",
                  })}
                </Label>
                <Select
                  value={strategy}
                  onValueChange={(value) => setStrategy(value as Strategy)}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="quota_aware_least_loaded">
                      {t({
                        en: "Prefer quota and available capacity",
                        "zh-CN": "额度与负载优先",
                        ja: "クォータと空き容量を優先",
                        ko: "할당량 및 가용 용량 우선",
                      })}
                    </SelectItem>
                    <SelectItem value="priority_weighted">
                      {t({
                        en: "Priority and weight",
                        "zh-CN": "优先级与权重",
                        ja: "優先度と重み",
                        ko: "우선순위 및 가중치",
                      })}
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>
                  {t({
                    en: "When quota data is unavailable",
                    "zh-CN": "额度数据缺失",
                    ja: "クォータデータがない場合",
                    ko: "할당량 데이터가 없을 때",
                  })}
                </Label>
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
                    <SelectItem value="allow">
                      {t({
                        en: "Continue scheduling",
                        "zh-CN": "继续调度",
                        ja: "スケジューリングを続行",
                        ko: "스케줄링 계속",
                      })}
                    </SelectItem>
                    <SelectItem value="block">
                      {t({
                        en: "Pause scheduling",
                        "zh-CN": "暂停调度",
                        ja: "スケジューリングを一時停止",
                        ko: "스케줄링 일시 중지",
                      })}
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label htmlFor="quota-freshness">
                  {t({
                    en: "Snapshot validity (minutes)",
                    "zh-CN": "快照有效期（分钟）",
                    ja: "スナップショット有効期間（分）",
                    ko: "스냅샷 유효 기간(분)",
                  })}
                </Label>
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
                <Label>
                  {t({
                    en: "Member policies",
                    "zh-CN": "成员策略",
                    ja: "メンバーポリシー",
                    ko: "멤버 정책",
                  })}
                </Label>
                <span className="text-xs text-muted-foreground">
                  {t(
                    {
                      en: "{count} accounts selected",
                      "zh-CN": "已选择 {count} 个账号",
                      ja: "{count} 件のアカウントを選択済み",
                      ko: "{count}개 계정 선택됨",
                    },
                    { count: selectedMemberCount },
                  )}
                </span>
              </div>
              <div className="border">
                <div className="hidden grid-cols-[2.5rem_minmax(12rem,1fr)_7rem_7rem_8rem] items-center border-b bg-muted/40 px-3 py-2 text-xs font-medium text-muted-foreground md:grid">
                  <span />
                  <span>
                    {t({
                      en: "Account",
                      "zh-CN": "账号",
                      ja: "アカウント",
                      ko: "계정",
                    })}
                  </span>
                  <span>
                    {t({
                      en: "Priority",
                      "zh-CN": "优先级",
                      ja: "優先度",
                      ko: "우선순위",
                    })}
                  </span>
                  <span>
                    {t({
                      en: "Weight",
                      "zh-CN": "权重",
                      ja: "重み",
                      ko: "가중치",
                    })}
                  </span>
                  <span>
                    {t({
                      en: "Minimum quota",
                      "zh-CN": "最低保留额度",
                      ja: "最低クォータ残量",
                      ko: "최소 보존 할당량",
                    })}
                  </span>
                </div>
                {routeAccounts.length === 0 ? (
                  <div className="flex min-h-24 items-center justify-center text-sm text-muted-foreground">
                    {t({
                      en: "No accounts are available for this provider",
                      "zh-CN": "该供应商暂无可用账号",
                      ja: "このプロバイダーで利用可能なアカウントはありません",
                      ko: "이 공급자에 사용 가능한 계정이 없습니다",
                    })}
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
                          aria-label={t(
                            {
                              en: "Select {account}",
                              "zh-CN": "选择 {account}",
                              ja: "{account} を選択",
                              ko: "{account} 선택",
                            },
                            {
                              account:
                                account.display_name ?? account.account_key,
                            },
                          )}
                        />
                        <div className="min-w-0 pr-2 text-sm">
                          <p className="truncate font-medium">
                            {account.display_name ?? account.account_key}
                          </p>
                          <p className="truncate text-xs text-muted-foreground">
                            {t(
                              {
                                en: "{status} · {count} slots available",
                                "zh-CN": "{status} · 可用并发 {count}",
                                ja: "{status} · 利用可能な同時実行数 {count}",
                                ko: "{status} · 사용 가능한 동시 실행 {count}",
                              },
                              {
                                status: accountStatusLabel(t, account),
                                count: account.available_capacity,
                              },
                            )}
                          </p>
                        </div>
                        {checked ? (
                          <>
                            <MobilePolicyField
                              label={t({
                                en: "Priority",
                                "zh-CN": "优先级",
                                ja: "優先度",
                                ko: "우선순위",
                              })}
                            >
                              <PolicyNumberInput
                                label={t({
                                  en: "Priority",
                                  "zh-CN": "优先级",
                                  ja: "優先度",
                                  ko: "우선순위",
                                })}
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
                            <MobilePolicyField
                              label={t({
                                en: "Weight",
                                "zh-CN": "权重",
                                ja: "重み",
                                ko: "가중치",
                              })}
                            >
                              <PolicyNumberInput
                                label={t({
                                  en: "Weight",
                                  "zh-CN": "权重",
                                  ja: "重み",
                                  ko: "가중치",
                                })}
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
                            <MobilePolicyField
                              label={t({
                                en: "Minimum quota",
                                "zh-CN": "最低保留额度",
                                ja: "最低クォータ残量",
                                ko: "최소 보존 할당량",
                              })}
                            >
                              <div className="flex items-center gap-1.5">
                                <PolicyNumberInput
                                  label={t({
                                    en: "Minimum quota",
                                    "zh-CN": "最低保留额度",
                                    ja: "最低クォータ残量",
                                    ko: "최소 보존 할당량",
                                  })}
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
                  <span className="text-muted-foreground">
                    {t({
                      en: "CLI provider",
                      "zh-CN": "CLI 供应商",
                      ja: "CLI プロバイダー",
                      ko: "CLI 공급자",
                    })}
                  </span>
                  <p className="mt-1 font-medium">
                    {providerLabel(t, providerId)}
                  </p>
                </div>
                <div>
                  <span className="text-muted-foreground">
                    {t({
                      en: "API capability",
                      "zh-CN": "API 能力",
                      ja: "API 機能",
                      ko: "API 기능",
                    })}
                  </span>
                  <p className="mt-1 font-medium">
                    {operationLabel(t, operationId)}
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
            {t({
              en: "Cancel",
              "zh-CN": "取消",
              ja: "キャンセル",
              ko: "취소",
            })}
          </Button>
          <Button onClick={() => void save()} disabled={pending || !routeValid}>
            {pending ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : route ? (
              <Save aria-hidden="true" />
            ) : (
              <Users aria-hidden="true" />
            )}
            {route
              ? t(
                  {
                    en: "Publish version {version}",
                    "zh-CN": "发布版本 {version}",
                    ja: "バージョン {version} を公開",
                    ko: "버전 {version} 게시",
                  },
                  { version: route.revision + 1 },
                )
              : t({
                  en: "Create account group",
                  "zh-CN": "创建账号组",
                  ja: "アカウントグループを作成",
                  ko: "계정 그룹 만들기",
                })}
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

function accountStatusLabel(t: Translate, account: ProviderAccountView) {
  if (account.environment_state === "invalid")
    return t({
      en: "Authentication expired",
      "zh-CN": "登录失效",
      ja: "認証期限切れ",
      ko: "인증 만료",
    });
  if (account.scheduling_state === "draining")
    return t({
      en: "Draining",
      "zh-CN": "排空中",
      ja: "ドレイン中",
      ko: "드레이닝 중",
    });
  if (account.scheduling_state === "disabled")
    return t({
      en: "Disabled",
      "zh-CN": "已停用",
      ja: "無効",
      ko: "비활성화됨",
    });
  if (account.configuration_status !== "configured")
    return t({
      en: "Configuration issue",
      "zh-CN": "配置异常",
      ja: "設定エラー",
      ko: "구성 오류",
    });
  return t({
    en: "Accepting new jobs",
    "zh-CN": "接收新任务",
    ja: "新しいジョブを受付中",
    ko: "새 작업 수락 중",
  });
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
  if (operationId === "images.edits")
    return t({
      en: "Image editing",
      "zh-CN": "图片编辑",
      ja: "画像編集",
      ko: "이미지 편집",
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

function slug(value: string) {
  const normalized = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ".")
    .replace(/^\.+|\.+$/g, "");
  return normalized.slice(0, 32) || "accounts";
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
