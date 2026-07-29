"use client";

import { useEffect, useMemo, useState } from "react";
import {
  BellRing,
  CalendarDays,
  CircleDollarSign,
  Info,
  Layers3,
  LoaderCircle,
  Plus,
  Save,
  Settings2,
  Users,
  Webhook,
  X,
} from "lucide-react";
import { toast } from "sonner";
import type { ProjectSummary } from "@/components/projects/project-create-dialog";
import { ProjectModelPolicyPanel } from "@/components/projects/project-model-policy-panel";
import { ProjectMembersPanel } from "@/components/projects/project-members-panel";
import { ProjectWebhooksPanel } from "@/components/projects/project-webhooks-panel";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { consoleFetch } from "@/lib/auth/client";
import { decimalToMicros, microsToDecimal } from "@/lib/admin/format";

type ProjectSpendAlertEvent = {
  event_id: string;
  threshold_percent: number;
  spend_micros: string;
  notification_state: "pending" | "acknowledged";
  created_at_ms: number;
};

type ProjectSpendBudget = {
  object: "organization.project.spend_budget";
  project_id: string;
  organization_id: string;
  configured: boolean;
  currency: string | null;
  monthly_budget_micros: string | null;
  spend_micros: string;
  reserved_micros: string;
  remaining_micros: string | null;
  usage_basis_points: string | null;
  limit_type: "soft" | "hard";
  period_kind: "calendar_month_utc";
  period_start_ms: number;
  period_end_ms: number;
  alert_thresholds: number[];
  alert_events: ProjectSpendAlertEvent[];
  control_version: string;
  updated_at_ms: number | null;
};

const QUICK_THRESHOLDS = [50, 75, 90] as const;
export type ProjectSettingsTarget = Pick<ProjectSummary, "id" | "name" | "status"> & {
  created_at: number | null;
};

type ProjectSettingsTab =
  | "general"
  | "limits"
  | "models"
  | "members"
  | "webhooks";

export function ProjectLimitsSheet({
  project,
  canManage,
  initialTab = "general",
  onProjectUpdated,
  onOpenChange,
}: {
  project: ProjectSettingsTarget | null;
  canManage: boolean;
  initialTab?: ProjectSettingsTab;
  onProjectUpdated?: (project: ProjectSummary) => void | Promise<void>;
  onOpenChange: (open: boolean) => void;
}) {
  const [generalProject, setGeneralProject] = useState<ProjectSummary | null>(null);
  const [generalName, setGeneralName] = useState("");
  const [projectServiceTier, setProjectServiceTier] = useState<
    "default" | "priority"
  >("default");
  const [userApiKeysDisabled, setUserApiKeysDisabled] = useState(false);
  const [generalLoading, setGeneralLoading] = useState(false);
  const [generalSaving, setGeneralSaving] = useState(false);
  const [generalError, setGeneralError] = useState<string | null>(null);
  const [budget, setBudget] = useState<ProjectSpendBudget | null>(null);
  const [currency, setCurrency] = useState("USD");
  const [monthlyBudget, setMonthlyBudget] = useState("");
  const [limitType, setLimitType] = useState<"soft" | "hard">("soft");
  const [thresholds, setThresholds] = useState<number[]>([100]);
  const [customThreshold, setCustomThreshold] = useState("");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<ProjectSettingsTab>(initialTab);

  useEffect(() => {
    if (!project) {
      setGeneralProject(null);
      setGeneralName("");
      setProjectServiceTier("default");
      setUserApiKeysDisabled(false);
      setGeneralError(null);
      return;
    }
    let active = true;
    setGeneralLoading(true);
    setGeneralError(null);
    void consoleFetch(
      `/api/gateway/v1/organization/projects/${encodeURIComponent(project.id)}`,
    )
      .then(async (response) => {
        if (!response.ok) throw new Error(await responseMessage(response));
        return (await response.json()) as ProjectSummary;
      })
      .then((next) => {
        if (!active) return;
        setGeneralProject(next);
        setGeneralName(next.name);
        setProjectServiceTier(next.service_tier);
        setUserApiKeysDisabled(next.user_api_keys_disabled);
      })
      .catch((reason) => {
        if (active) {
          setGeneralError(
            reason instanceof Error ? reason.message : "项目设置加载失败",
          );
        }
      })
      .finally(() => {
        if (active) setGeneralLoading(false);
      });
    return () => {
      active = false;
    };
  }, [project]);

  useEffect(() => {
    if (!project) {
      setBudget(null);
      setError(null);
      setActiveTab(initialTab);
      return;
    }
    setActiveTab(initialTab);
    let active = true;
    setLoading(true);
    setError(null);
    void consoleFetch(
      `/api/gateway/v1/organization/projects/${encodeURIComponent(project.id)}/limits`,
    )
      .then(async (response) => {
        if (!response.ok) throw new Error(await responseMessage(response));
        return (await response.json()) as ProjectSpendBudget;
      })
      .then((next) => {
        if (!active) return;
        setBudget(next);
        setCurrency(next.currency ?? "USD");
        setMonthlyBudget(
          next.monthly_budget_micros
            ? microsToDecimal(next.monthly_budget_micros)
            : "",
        );
        setLimitType(next.limit_type);
        setThresholds(
          next.alert_thresholds.length > 0
            ? normalizeThresholds(next.alert_thresholds)
            : [100],
        );
      })
      .catch((reason) => {
        if (active) {
          setError(reason instanceof Error ? reason.message : "项目限额加载失败");
        }
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [initialTab, project]);

  const usagePercent = useMemo(() => {
    const basisPoints = Number(budget?.usage_basis_points ?? "0");
    return Number.isFinite(basisPoints) ? basisPoints / 100 : 0;
  }, [budget?.usage_basis_points]);
  const selectedCurrency = budget?.currency ?? currency;
  const customThresholds = thresholds.filter(
    (threshold) =>
      threshold !== 100 &&
      !QUICK_THRESHOLDS.includes(threshold as (typeof QUICK_THRESHOLDS)[number]),
  );

  async function saveGeneral() {
    if (!project || !generalProject || !canManage) return;
    const name = generalName.trim();
    if (!name) {
      setGeneralError("项目名称不能为空");
      return;
    }
    setGeneralSaving(true);
    setGeneralError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(project.id)}`,
        {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            name,
            service_tier: projectServiceTier,
            user_api_keys_disabled: userApiKeysDisabled,
            expected_settings_version: generalProject.settings_version,
          }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      const next = (await response.json()) as ProjectSummary;
      setGeneralProject(next);
      setGeneralName(next.name);
      setProjectServiceTier(next.service_tier);
      setUserApiKeysDisabled(next.user_api_keys_disabled);
      await onProjectUpdated?.(next);
      toast.success("项目设置已保存");
    } catch (reason) {
      setGeneralError(
        reason instanceof Error ? reason.message : "项目设置保存失败",
      );
    } finally {
      setGeneralSaving(false);
    }
  }

  async function save() {
    if (!project || !budget || !canManage) return;
    const monthlyBudgetMicros = decimalToMicros(monthlyBudget);
    if (!monthlyBudgetMicros) {
      setError("请输入大于 0、最多 6 位小数的月度预算");
      return;
    }
    const normalizedCurrency = currency.trim().toUpperCase();
    if (!/^[A-Z]{3}$/.test(normalizedCurrency)) {
      setError("币种必须是 3 位 ISO 4217 代码");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(project.id)}/limits`,
        {
          method: "PUT",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            currency: normalizedCurrency,
            monthly_budget_micros: monthlyBudgetMicros,
            limit_type: limitType,
            alert_thresholds: thresholds,
            expected_control_version: budget.control_version,
          }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      const next = (await response.json()) as ProjectSpendBudget;
      setBudget(next);
      setCurrency(next.currency ?? normalizedCurrency);
      setMonthlyBudget(
        next.monthly_budget_micros
          ? microsToDecimal(next.monthly_budget_micros)
          : monthlyBudget,
      );
      setLimitType(next.limit_type);
      setThresholds(normalizeThresholds(next.alert_thresholds));
      toast.success("项目限额已保存");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "项目限额保存失败");
    } finally {
      setSaving(false);
    }
  }

  function toggleThreshold(threshold: number, checked: boolean) {
    setThresholds((current) =>
      normalizeThresholds(
        checked
          ? [...current, threshold]
          : current.filter((value) => value !== threshold),
      ),
    );
  }

  function addCustomThreshold() {
    const value = Number(customThreshold);
    if (!Number.isInteger(value) || value < 1 || value > 99) {
      setError("自定义阈值必须是 1 到 99 的整数");
      return;
    }
    setThresholds((current) => normalizeThresholds([...current, value]));
    setCustomThreshold("");
    setError(null);
  }

  return (
    <Sheet open={project !== null} onOpenChange={onOpenChange}>
      <SheetContent className="flex h-full w-full flex-col gap-0 overflow-hidden p-0 sm:max-w-3xl">
        <SheetHeader className="shrink-0 border-b px-5 py-5 pr-12 text-left sm:px-6">
          <SheetTitle className="truncate">
            {generalProject?.name ?? project?.name ?? "项目设置"}
          </SheetTitle>
          <SheetDescription className="truncate font-mono text-xs">
            {project?.id}
          </SheetDescription>
        </SheetHeader>

        <Tabs
          value={activeTab}
          onValueChange={(value) => setActiveTab(value as ProjectSettingsTab)}
          className="shrink-0 border-b"
        >
          <div className="overflow-x-auto px-5 sm:px-6">
            <TabsList variant="line">
              <TabsTrigger value="general" variant="line">
                <Settings2 className="size-4" aria-hidden="true" />
                常规
              </TabsTrigger>
              <TabsTrigger value="limits" variant="line">
                <CircleDollarSign className="size-4" aria-hidden="true" />
                预算与限额
              </TabsTrigger>
              <TabsTrigger value="models" variant="line">
                <Layers3 className="size-4" aria-hidden="true" />
                模型
              </TabsTrigger>
              <TabsTrigger value="members" variant="line">
                <Users className="size-4" aria-hidden="true" />
                成员
              </TabsTrigger>
              <TabsTrigger value="webhooks" variant="line">
                <Webhook className="size-4" aria-hidden="true" />
                Webhooks
              </TabsTrigger>
            </TabsList>
          </div>
        </Tabs>

        <div className="min-h-0 flex-1 overflow-y-auto">
          {activeTab === "general" && generalLoading && !generalProject ? (
            <div className="grid min-h-72 place-items-center text-muted-foreground">
              <LoaderCircle
                className="size-5 animate-spin"
                aria-label="正在加载项目设置"
              />
            </div>
          ) : activeTab === "general" ? (
            <div className="space-y-6 px-5 py-6 sm:px-6">
              <section className="space-y-4" aria-labelledby="project-details-heading">
                <div>
                  <h3 id="project-details-heading" className="text-sm font-medium">
                    项目详情
                  </h3>
                  <p className="mt-1 text-sm text-muted-foreground">
                    项目是 API 资源、凭据、用量和访问权限的隔离边界。
                  </p>
                </div>
                <div className="grid gap-5 text-sm">
                  <div className="grid gap-2 sm:grid-cols-[128px_1fr] sm:items-center sm:gap-4">
                    <Label htmlFor="project-general-name" className="text-muted-foreground">
                      项目名称
                    </Label>
                    <Input
                      id="project-general-name"
                      value={generalName}
                      onChange={(event) => setGeneralName(event.target.value)}
                      maxLength={128}
                      disabled={!canManage || generalSaving}
                    />
                  </div>
                  <div className="grid gap-1.5 sm:grid-cols-[128px_1fr] sm:items-center sm:gap-4">
                    <span className="text-muted-foreground">项目 ID</span>
                    <span className="min-w-0 break-all font-mono text-xs">
                      {project?.id}
                    </span>
                  </div>
                  <div className="grid gap-1.5 sm:grid-cols-[128px_1fr] sm:items-center sm:gap-4">
                    <span className="text-muted-foreground">状态</span>
                    <span>
                      <Badge
                        variant={project?.status === "active" ? "secondary" : "outline"}
                      >
                        {project?.status === "active" ? "启用" : "已归档"}
                      </Badge>
                    </span>
                  </div>
                  <div className="grid gap-1.5 sm:grid-cols-[128px_1fr] sm:items-center sm:gap-4">
                    <span className="text-muted-foreground">创建时间</span>
                    <span>
                      {generalProject?.created_at
                        ? formatUnix(generalProject.created_at)
                        : project?.created_at
                          ? formatUnix(project.created_at)
                          : "—"}
                    </span>
                  </div>
                </div>
              </section>

              <section
                className="space-y-4"
                aria-labelledby="project-service-tier-heading"
              >
                <div>
                  <h3
                    id="project-service-tier-heading"
                    className="text-sm font-medium"
                  >
                    项目服务层级
                  </h3>
                  <p className="mt-1 text-sm text-muted-foreground">
                    用于未指定服务层级或使用 auto 的请求；响应和计费始终记录实际执行层级。
                  </p>
                </div>
                <div className="grid gap-2 sm:grid-cols-[128px_1fr] sm:items-start sm:gap-4">
                  <Label
                    htmlFor="project-service-tier"
                    className="pt-2 text-muted-foreground"
                  >
                    默认层级
                  </Label>
                  <div className="space-y-2">
                    <Select
                      value={projectServiceTier}
                      onValueChange={(value) =>
                        setProjectServiceTier(value as "default" | "priority")
                      }
                      disabled={!canManage || generalSaving || !generalProject}
                    >
                      <SelectTrigger id="project-service-tier">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="default">Default</SelectItem>
                        <SelectItem value="priority">Priority</SelectItem>
                      </SelectContent>
                    </Select>
                    <p className="text-xs leading-5 text-muted-foreground">
                      {projectServiceTier === "priority"
                        ? "优先请求会在上游模型支持时使用 Priority；不支持时回退到 Default，并按实际层级计费。"
                        : "使用标准按量处理。单个请求未来仍可显式选择受支持的服务层级。"}
                    </p>
                  </div>
                </div>
              </section>

              <section className="space-y-4" aria-labelledby="project-key-policy-heading">
                <div>
                  <h3 id="project-key-policy-heading" className="text-sm font-medium">
                    用户 API Key
                  </h3>
                  <p className="mt-1 text-sm text-muted-foreground">
                    控制项目成员能否创建和使用个人 Key。
                  </p>
                </div>
                <div className="flex items-start justify-between gap-4 rounded-md border p-4">
                  <div className="space-y-1">
                    <Label htmlFor="disable-project-user-api-keys">
                      禁用用户 API Key
                    </Label>
                    <p className="text-sm text-muted-foreground">
                      开启后，现有个人 Key 会立即停止认证，也不能创建新的个人
                      Key；服务账户 Key 不受影响。
                    </p>
                  </div>
                  <Switch
                    id="disable-project-user-api-keys"
                    checked={userApiKeysDisabled}
                    onCheckedChange={setUserApiKeysDisabled}
                    disabled={!canManage || generalSaving || !generalProject}
                    aria-label="禁用项目用户 API Key"
                  />
                </div>
              </section>

              {!canManage ? (
                <p className="text-sm text-muted-foreground">
                  你可以查看此项目设置；只有项目或组织所有者可以修改。
                </p>
              ) : null}
              {generalError ? (
                <p role="alert" className="text-sm text-destructive">
                  {generalError}
                </p>
              ) : null}
            </div>
          ) : activeTab === "models" && project ? (
            <ProjectModelPolicyPanel
              projectId={project.id}
              canManage={canManage}
              active
            />
          ) : activeTab === "members" && project ? (
            <ProjectMembersPanel
              projectId={project.id}
              canManage={canManage}
              active
            />
          ) : activeTab === "webhooks" && project ? (
            <ProjectWebhooksPanel
              projectId={project.id}
              canManage={canManage}
              active
            />
          ) : loading ? (
            <div className="grid min-h-72 place-items-center text-muted-foreground">
              <LoaderCircle className="size-5 animate-spin" aria-label="正在加载项目预算" />
            </div>
          ) : budget ? (
            <div className="space-y-7 px-5 py-6 sm:px-6">
              <Alert>
                <Info aria-hidden="true" />
                <AlertTitle>项目预算不等于组织计费额度</AlertTitle>
                <AlertDescription>
                  这里控制当前项目的月度预算提醒和可选硬限额。组织可用额度由平台管理员在“全平台 → 用量 → 组织限额”中管理。
                </AlertDescription>
              </Alert>

              <section aria-labelledby="project-spend-heading" className="space-y-4">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <h3 id="project-spend-heading" className="text-sm font-medium">
                      本月消费
                    </h3>
                    <p className="mt-1 text-2xl font-semibold tabular-nums">
                      {formatMicros(budget.spend_micros, selectedCurrency)}
                    </p>
                    {budget.reserved_micros !== "0" ? (
                      <p className="mt-1 text-xs text-muted-foreground">
                        另有{" "}
                        {formatMicros(budget.reserved_micros, selectedCurrency)}{" "}
                        待结算预留
                      </p>
                    ) : null}
                  </div>
                  <div className="text-right text-sm text-muted-foreground">
                    <p>预算</p>
                    <p className="mt-1 font-medium tabular-nums text-foreground">
                      {budget.monthly_budget_micros
                        ? formatMicros(
                            budget.monthly_budget_micros,
                            selectedCurrency,
                          )
                        : "尚未设置"}
                    </p>
                  </div>
                </div>
                <div className="space-y-2">
                  <Progress
                    value={Math.min(usagePercent, 100)}
                    aria-label={`本月预算已使用 ${formatPercent(usagePercent)}`}
                  />
                  <div className="flex items-center justify-between gap-3 text-xs text-muted-foreground">
                    <span>{formatPercent(usagePercent)} 已使用</span>
                    <span className="inline-flex items-center gap-1">
                      <CalendarDays className="size-3.5" aria-hidden="true" />
                      {formatPeriod(budget.period_start_ms, budget.period_end_ms)}
                    </span>
                  </div>
                </div>
                <div className="rounded-md border bg-muted/35 p-3 text-sm text-muted-foreground">
                  {budget.limit_type === "hard"
                    ? "月度限额按 UTC 自然月统计。系统会在准入时按已结算消费、待结算预留和本次最大报价校验，超限请求不会进入执行队列。"
                    : "月度预算按 UTC 自然月统计。达到或超过预算后请求仍会继续执行，组织和项目所有者会收到提醒。"}
                </div>
              </section>

              <section aria-labelledby="project-budget-heading" className="space-y-4">
                <div>
                  <h3 id="project-budget-heading" className="text-sm font-medium">
                    月度预算
                  </h3>
                  <p className="mt-1 text-sm text-muted-foreground">
                    设置项目的月度消费目标，并选择是否在达到上限时阻断新请求。
                  </p>
                </div>
                <div className="grid gap-4 sm:grid-cols-[112px_1fr]">
                  <div className="space-y-2">
                    <Label htmlFor="project-budget-currency">币种</Label>
                    <Input
                      id="project-budget-currency"
                      value={currency}
                      onChange={(event) => setCurrency(event.target.value)}
                      maxLength={3}
                      className="uppercase"
                      disabled={!canManage || saving}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="project-monthly-budget">每月预算</Label>
                    <Input
                      id="project-monthly-budget"
                      inputMode="decimal"
                      value={monthlyBudget}
                      onChange={(event) => setMonthlyBudget(event.target.value)}
                      placeholder="例如 100.00"
                      disabled={!canManage || saving}
                    />
                  </div>
                </div>
                <div className="flex items-start justify-between gap-4 rounded-md border p-4">
                  <div className="space-y-1">
                    <Label htmlFor="project-hard-limit">强制执行硬限额</Label>
                    <p className="text-sm text-muted-foreground">
                      开启后，达到月度限额的新请求将返回项目预算超限错误。
                    </p>
                  </div>
                  <Switch
                    id="project-hard-limit"
                    checked={limitType === "hard"}
                    onCheckedChange={(checked) =>
                      setLimitType(checked ? "hard" : "soft")
                    }
                    disabled={!canManage || saving}
                    aria-label="强制执行项目月度硬限额"
                  />
                </div>
              </section>

              <section aria-labelledby="project-alerts-heading" className="space-y-4">
                <div>
                  <h3 id="project-alerts-heading" className="text-sm font-medium">
                    通知阈值
                  </h3>
                  <p className="mt-1 text-sm text-muted-foreground">
                    每个阈值在当前预算版本和自然月内只通知一次。
                  </p>
                </div>
                <div className="grid gap-3 sm:grid-cols-2">
                  {QUICK_THRESHOLDS.map((threshold) => (
                    <label
                      key={threshold}
                      className="flex min-h-10 items-center gap-3 rounded-md border px-3 text-sm"
                    >
                      <Checkbox
                        checked={thresholds.includes(threshold)}
                        onCheckedChange={(checked) =>
                          toggleThreshold(threshold, checked === true)
                        }
                        disabled={!canManage || saving}
                      />
                      预算达到 {threshold}%
                    </label>
                  ))}
                  <label className="flex min-h-10 items-center gap-3 rounded-md border px-3 text-sm">
                    <Checkbox checked disabled aria-label="预算达到 100%" />
                    预算达到 100%
                  </label>
                </div>
                {customThresholds.length > 0 ? (
                  <div className="flex flex-wrap gap-2">
                    {customThresholds.map((threshold) => (
                      <Badge key={threshold} variant="secondary" className="gap-1">
                        {threshold}%
                        {canManage ? (
                          <button
                            type="button"
                            onClick={() => toggleThreshold(threshold, false)}
                            className="rounded-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
                            aria-label={`移除 ${threshold}% 阈值`}
                          >
                            <X className="size-3" aria-hidden="true" />
                          </button>
                        ) : null}
                      </Badge>
                    ))}
                  </div>
                ) : null}
                {canManage ? (
                  <div className="flex gap-2">
                    <Input
                      type="number"
                      min={1}
                      max={99}
                      step={1}
                      value={customThreshold}
                      onChange={(event) => setCustomThreshold(event.target.value)}
                      onKeyDown={(event) => {
                        if (event.key === "Enter") {
                          event.preventDefault();
                          addCustomThreshold();
                        }
                      }}
                      placeholder="自定义百分比"
                      disabled={saving}
                    />
                    <Button
                      type="button"
                      variant="outline"
                      size="icon"
                      onClick={addCustomThreshold}
                      disabled={saving || !customThreshold}
                      aria-label="添加通知阈值"
                      title="添加通知阈值"
                    >
                      <Plus aria-hidden="true" />
                    </Button>
                  </div>
                ) : null}
              </section>

              {budget.alert_events.length > 0 ? (
                <section aria-labelledby="project-alert-history" className="space-y-3">
                  <div className="flex items-center gap-2">
                    <BellRing className="size-4" aria-hidden="true" />
                    <h3 id="project-alert-history" className="text-sm font-medium">
                      本月提醒
                    </h3>
                  </div>
                  <div className="space-y-2">
                    {budget.alert_events.map((event) => (
                      <div
                        key={event.event_id}
                        className="flex items-center justify-between gap-3 rounded-md border px-3 py-2 text-sm"
                      >
                        <span>已达到 {event.threshold_percent}%</span>
                        <span className="text-xs text-muted-foreground">
                          {formatDateTime(event.created_at_ms)}
                        </span>
                      </div>
                    ))}
                  </div>
                </section>
              ) : null}

              {!canManage ? (
                <p className="text-sm text-muted-foreground">
                  你可以查看此项目的限额；只有项目或组织所有者可以修改。
                </p>
              ) : null}
              {error ? (
                <p role="alert" className="text-sm text-destructive">
                  {error}
                </p>
              ) : null}
            </div>
          ) : (
            <div className="grid min-h-72 place-items-center px-6 text-center text-sm text-muted-foreground">
              {error ?? "项目预算暂时不可用"}
            </div>
          )}
        </div>

        {activeTab === "general" && generalProject && canManage ? (
          <SheetFooter className="shrink-0 gap-2 border-t bg-background px-5 py-4 sm:px-6">
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={generalSaving}
            >
              取消
            </Button>
            <Button
              type="button"
              onClick={() => void saveGeneral()}
              disabled={
                generalSaving ||
                !generalName.trim() ||
                (generalName.trim() === generalProject.name &&
                  projectServiceTier === generalProject.service_tier &&
                  userApiKeysDisabled === generalProject.user_api_keys_disabled)
              }
            >
              {generalSaving ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <Save aria-hidden="true" />
              )}
              保存常规设置
            </Button>
          </SheetFooter>
        ) : null}

        {activeTab === "limits" && budget && canManage ? (
          <SheetFooter className="shrink-0 gap-2 border-t bg-background px-5 py-4 sm:px-6">
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
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
              保存预算设置
            </Button>
          </SheetFooter>
        ) : null}
      </SheetContent>
    </Sheet>
  );
}

function normalizeThresholds(values: number[]) {
  return [...new Set([...values, 100])]
    .filter((value) => Number.isInteger(value) && value >= 1 && value <= 100)
    .sort((left, right) => left - right);
}

function formatMicros(value: string, currency: string) {
  const amount = Number(value) / 1_000_000;
  if (!Number.isFinite(amount)) return `${value} ${currency}`;
  try {
    return new Intl.NumberFormat("zh-CN", {
      style: "currency",
      currency,
      maximumFractionDigits: 6,
    }).format(amount);
  } catch {
    return `${amount.toLocaleString("zh-CN", { maximumFractionDigits: 6 })} ${currency}`;
  }
}

function formatPercent(value: number) {
  return `${new Intl.NumberFormat("zh-CN", {
    maximumFractionDigits: 2,
  }).format(value)}%`;
}

function formatPeriod(startMs: number, endMs: number) {
  const formatter = new Intl.DateTimeFormat("zh-CN", {
    timeZone: "UTC",
    month: "short",
    day: "numeric",
  });
  return `${formatter.format(new Date(startMs))} - ${formatter.format(
    new Date(endMs - 1),
  )} UTC`;
}

function formatDateTime(value: number) {
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}

function formatUnix(value: number) {
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value * 1000));
}

async function responseMessage(response: Response) {
  const body = (await response.json().catch(() => null)) as
    | { error?: string | { message?: string } }
    | null;
  if (typeof body?.error === "string") return body.error;
  if (body?.error && typeof body.error === "object" && body.error.message) {
    return body.error.message;
  }
  return `请求失败 (${response.status})`;
}
