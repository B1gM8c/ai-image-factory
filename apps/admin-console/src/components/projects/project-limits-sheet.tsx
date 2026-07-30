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
import { useI18n } from "@/i18n/locale-provider";
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
  const { locale, t } = useI18n();
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
        if (!response.ok) throw new Error(await responseMessage(response, t({ en: "Request failed", "zh-CN": "请求失败", ja: "リクエストに失敗しました", ko: "요청에 실패했습니다" })));
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
            reason instanceof Error ? reason.message : t({ en: "Failed to load project settings", "zh-CN": "项目设置加载失败", ja: "プロジェクト設定を読み込めませんでした", ko: "프로젝트 설정을 불러오지 못했습니다" }),
          );
        }
      })
      .finally(() => {
        if (active) setGeneralLoading(false);
      });
    return () => {
      active = false;
    };
  }, [project, t]);

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
        if (!response.ok) throw new Error(await responseMessage(response, t({ en: "Request failed", "zh-CN": "请求失败", ja: "リクエストに失敗しました", ko: "요청에 실패했습니다" })));
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
          setError(reason instanceof Error ? reason.message : t({ en: "Failed to load project limits", "zh-CN": "项目限额加载失败", ja: "プロジェクト上限を読み込めませんでした", ko: "프로젝트 한도를 불러오지 못했습니다" }));
        }
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [initialTab, project, t]);

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
      setGeneralError(t({ en: "Project name is required", "zh-CN": "项目名称不能为空", ja: "プロジェクト名を入力してください", ko: "프로젝트 이름을 입력하세요" }));
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
      if (!response.ok) throw new Error(await responseMessage(response, t({ en: "Request failed", "zh-CN": "请求失败", ja: "リクエストに失敗しました", ko: "요청에 실패했습니다" })));
      const next = (await response.json()) as ProjectSummary;
      setGeneralProject(next);
      setGeneralName(next.name);
      setProjectServiceTier(next.service_tier);
      setUserApiKeysDisabled(next.user_api_keys_disabled);
      await onProjectUpdated?.(next);
      toast.success(t({ en: "Project settings saved", "zh-CN": "项目设置已保存", ja: "プロジェクト設定を保存しました", ko: "프로젝트 설정을 저장했습니다" }));
    } catch (reason) {
      setGeneralError(
        reason instanceof Error ? reason.message : t({ en: "Failed to save project settings", "zh-CN": "项目设置保存失败", ja: "プロジェクト設定を保存できませんでした", ko: "프로젝트 설정을 저장하지 못했습니다" }),
      );
    } finally {
      setGeneralSaving(false);
    }
  }

  async function save() {
    if (!project || !budget || !canManage) return;
    const monthlyBudgetMicros = decimalToMicros(monthlyBudget);
    if (!monthlyBudgetMicros) {
      setError(t({ en: "Enter a monthly budget greater than 0 with up to 6 decimal places", "zh-CN": "请输入大于 0、最多 6 位小数的月度预算", ja: "0 より大きく、小数点以下 6 桁以内の月間予算を入力してください", ko: "0보다 크고 소수점 이하 6자리까지인 월 예산을 입력하세요" }));
      return;
    }
    const normalizedCurrency = currency.trim().toUpperCase();
    if (!/^[A-Z]{3}$/.test(normalizedCurrency)) {
      setError(t({ en: "Currency must be a 3-letter ISO 4217 code", "zh-CN": "币种必须是 3 位 ISO 4217 代码", ja: "通貨は 3 文字の ISO 4217 コードで指定してください", ko: "통화는 3자리 ISO 4217 코드여야 합니다" }));
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
      if (!response.ok) throw new Error(await responseMessage(response, t({ en: "Request failed", "zh-CN": "请求失败", ja: "リクエストに失敗しました", ko: "요청에 실패했습니다" })));
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
      toast.success(t({ en: "Project limits saved", "zh-CN": "项目限额已保存", ja: "プロジェクト上限を保存しました", ko: "프로젝트 한도를 저장했습니다" }));
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : t({ en: "Failed to save project limits", "zh-CN": "项目限额保存失败", ja: "プロジェクト上限を保存できませんでした", ko: "프로젝트 한도를 저장하지 못했습니다" }));
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
      setError(t({ en: "Custom threshold must be an integer from 1 to 99", "zh-CN": "自定义阈值必须是 1 到 99 的整数", ja: "カスタムしきい値は 1 から 99 の整数で指定してください", ko: "사용자 지정 임계값은 1~99 사이의 정수여야 합니다" }));
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
            {generalProject?.name ?? project?.name ?? t({ en: "Project settings", "zh-CN": "项目设置", ja: "プロジェクト設定", ko: "프로젝트 설정" })}
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
                {t({ en: "General", "zh-CN": "常规", ja: "一般", ko: "일반" })}
              </TabsTrigger>
              <TabsTrigger value="limits" variant="line">
                <CircleDollarSign className="size-4" aria-hidden="true" />
                {t({ en: "Budget and limits", "zh-CN": "预算与限额", ja: "予算と上限", ko: "예산 및 한도" })}
              </TabsTrigger>
              <TabsTrigger value="models" variant="line">
                <Layers3 className="size-4" aria-hidden="true" />
                {t({ en: "Models", "zh-CN": "模型", ja: "モデル", ko: "모델" })}
              </TabsTrigger>
              <TabsTrigger value="members" variant="line">
                <Users className="size-4" aria-hidden="true" />
                {t({ en: "Members", "zh-CN": "成员", ja: "メンバー", ko: "멤버" })}
              </TabsTrigger>
              <TabsTrigger value="webhooks" variant="line">
                <Webhook className="size-4" aria-hidden="true" />
                {t({ en: "Webhooks", "zh-CN": "Webhook", ja: "Webhook", ko: "Webhook" })}
              </TabsTrigger>
            </TabsList>
          </div>
        </Tabs>

        <div className="min-h-0 flex-1 overflow-y-auto">
          {activeTab === "general" && generalLoading && !generalProject ? (
            <div className="grid min-h-72 place-items-center text-muted-foreground">
              <LoaderCircle
                className="size-5 animate-spin"
                aria-label={t({ en: "Loading project settings", "zh-CN": "正在加载项目设置", ja: "プロジェクト設定を読み込み中", ko: "프로젝트 설정 불러오는 중" })}
              />
            </div>
          ) : activeTab === "general" ? (
            <div className="space-y-6 px-5 py-6 sm:px-6">
              <section className="space-y-4" aria-labelledby="project-details-heading">
                <div>
                  <h3 id="project-details-heading" className="text-sm font-medium">
                    {t({ en: "Project details", "zh-CN": "项目详情", ja: "プロジェクト詳細", ko: "프로젝트 세부 정보" })}
                  </h3>
                  <p className="mt-1 text-sm text-muted-foreground">
                    {t({ en: "Projects isolate API resources, credentials, usage, and access permissions.", "zh-CN": "项目是 API 资源、凭据、用量和访问权限的隔离边界。", ja: "プロジェクトは API リソース、認証情報、使用量、アクセス権を分離する境界です。", ko: "프로젝트는 API 리소스, 자격 증명, 사용량 및 액세스 권한을 격리하는 경계입니다." })}
                  </p>
                </div>
                <div className="grid gap-5 text-sm">
                  <div className="grid gap-2 sm:grid-cols-[128px_1fr] sm:items-center sm:gap-4">
                    <Label htmlFor="project-general-name" className="text-muted-foreground">
                      {t({ en: "Project name", "zh-CN": "项目名称", ja: "プロジェクト名", ko: "프로젝트 이름" })}
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
                    <span className="text-muted-foreground">{t({ en: "Project ID", "zh-CN": "项目 ID", ja: "プロジェクト ID", ko: "프로젝트 ID" })}</span>
                    <span className="min-w-0 break-all font-mono text-xs">
                      {project?.id}
                    </span>
                  </div>
                  <div className="grid gap-1.5 sm:grid-cols-[128px_1fr] sm:items-center sm:gap-4">
                    <span className="text-muted-foreground">{t({ en: "Status", "zh-CN": "状态", ja: "ステータス", ko: "상태" })}</span>
                    <span>
                      <Badge
                        variant={project?.status === "active" ? "secondary" : "outline"}
                      >
                        {project?.status === "active"
                          ? t({ en: "Active", "zh-CN": "启用", ja: "有効", ko: "활성" })
                          : t({ en: "Archived", "zh-CN": "已归档", ja: "アーカイブ済み", ko: "보관됨" })}
                      </Badge>
                    </span>
                  </div>
                  <div className="grid gap-1.5 sm:grid-cols-[128px_1fr] sm:items-center sm:gap-4">
                    <span className="text-muted-foreground">{t({ en: "Created", "zh-CN": "创建时间", ja: "作成日時", ko: "생성 시간" })}</span>
                    <span>
                      {generalProject?.created_at
                        ? formatUnix(generalProject.created_at, locale)
                        : project?.created_at
                          ? formatUnix(project.created_at, locale)
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
                    {t({ en: "Project service tier", "zh-CN": "项目服务层级", ja: "プロジェクトのサービス階層", ko: "프로젝트 서비스 계층" })}
                  </h3>
                  <p className="mt-1 text-sm text-muted-foreground">
                    {t({ en: "Used when a request omits the service tier or selects auto. Responses and billing always record the tier actually used.", "zh-CN": "用于未指定服务层级或使用 auto 的请求；响应和计费始终记录实际执行层级。", ja: "サービス階層が未指定、または auto のリクエストに使用されます。レスポンスと請求には常に実際に使用された階層が記録されます。", ko: "서비스 계층을 지정하지 않거나 auto를 선택한 요청에 사용됩니다. 응답과 청구에는 항상 실제 실행 계층이 기록됩니다." })}
                  </p>
                </div>
                <div className="grid gap-2 sm:grid-cols-[128px_1fr] sm:items-start sm:gap-4">
                  <Label
                    htmlFor="project-service-tier"
                    className="pt-2 text-muted-foreground"
                  >
                    {t({ en: "Default tier", "zh-CN": "默认层级", ja: "デフォルト階層", ko: "기본 계층" })}
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
                        <SelectItem value="default">
                          {t({ en: "Default", "zh-CN": "默认", ja: "デフォルト", ko: "기본" })}
                        </SelectItem>
                        <SelectItem value="priority">Priority</SelectItem>
                      </SelectContent>
                    </Select>
                    <p className="text-xs leading-5 text-muted-foreground">
                      {projectServiceTier === "priority"
                        ? t({ en: "Priority requests use Priority when the upstream model supports it; otherwise they fall back to Default and are billed for the tier actually used.", "zh-CN": "优先请求会在上游模型支持时使用 Priority；不支持时回退到 Default，并按实际层级计费。", ja: "アップストリームモデルが対応している場合は Priority を使用し、非対応の場合は Default にフォールバックして、実際の階層で課金されます。", ko: "업스트림 모델이 지원하면 Priority를 사용하고, 지원하지 않으면 Default로 대체되며 실제 사용 계층으로 청구됩니다." })
                        : t({ en: "Uses standard usage-based processing. Individual requests can still explicitly select a supported service tier.", "zh-CN": "使用标准按量处理。单个请求未来仍可显式选择受支持的服务层级。", ja: "標準の従量処理を使用します。個々のリクエストでは、対応するサービス階層を明示的に選択できます。", ko: "표준 사용량 기반 처리를 사용합니다. 개별 요청은 지원되는 서비스 계층을 명시적으로 선택할 수 있습니다." })}
                    </p>
                  </div>
                </div>
              </section>

              <section className="space-y-4" aria-labelledby="project-key-policy-heading">
                <div>
                  <h3 id="project-key-policy-heading" className="text-sm font-medium">
                    {t({ en: "User API keys", "zh-CN": "用户 API Key", ja: "ユーザー API キー", ko: "사용자 API 키" })}
                  </h3>
                  <p className="mt-1 text-sm text-muted-foreground">
                    {t({ en: "Control whether project members can create and use personal keys.", "zh-CN": "控制项目成员能否创建和使用个人 Key。", ja: "プロジェクトメンバーが個人キーを作成・使用できるかを制御します。", ko: "프로젝트 멤버가 개인 키를 만들고 사용할 수 있는지 제어합니다." })}
                  </p>
                </div>
                <div className="flex items-start justify-between gap-4 rounded-md border p-4">
                  <div className="space-y-1">
                    <Label htmlFor="disable-project-user-api-keys">
                      {t({ en: "Disable user API keys", "zh-CN": "禁用用户 API Key", ja: "ユーザー API キーを無効化", ko: "사용자 API 키 비활성화" })}
                    </Label>
                    <p className="text-sm text-muted-foreground">
                      {t({ en: "When enabled, existing personal keys immediately stop authenticating and new personal keys cannot be created. Service account keys are unaffected.", "zh-CN": "开启后，现有个人 Key 会立即停止认证，也不能创建新的个人 Key；服务账户 Key 不受影响。", ja: "有効にすると、既存の個人キーは直ちに認証できなくなり、新しい個人キーも作成できません。サービスアカウントキーには影響しません。", ko: "활성화하면 기존 개인 키의 인증이 즉시 중지되고 새 개인 키를 만들 수 없습니다. 서비스 계정 키에는 영향을 주지 않습니다." })}
                    </p>
                  </div>
                  <Switch
                    id="disable-project-user-api-keys"
                    checked={userApiKeysDisabled}
                    onCheckedChange={setUserApiKeysDisabled}
                    disabled={!canManage || generalSaving || !generalProject}
                    aria-label={t({ en: "Disable project user API keys", "zh-CN": "禁用项目用户 API Key", ja: "プロジェクトのユーザー API キーを無効化", ko: "프로젝트 사용자 API 키 비활성화" })}
                  />
                </div>
              </section>

              {!canManage ? (
                <p className="text-sm text-muted-foreground">
                  {t({ en: "You can view these project settings. Only project or organization owners can change them.", "zh-CN": "你可以查看此项目设置；只有项目或组织所有者可以修改。", ja: "このプロジェクト設定は閲覧できます。変更できるのはプロジェクトまたは組織の所有者のみです。", ko: "이 프로젝트 설정을 볼 수 있습니다. 프로젝트 또는 조직 소유자만 변경할 수 있습니다." })}
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
              <LoaderCircle className="size-5 animate-spin" aria-label={t({ en: "Loading project budget", "zh-CN": "正在加载项目预算", ja: "プロジェクト予算を読み込み中", ko: "프로젝트 예산 불러오는 중" })} />
            </div>
          ) : budget ? (
            <div className="space-y-7 px-5 py-6 sm:px-6">
              <Alert>
                <Info aria-hidden="true" />
                <AlertTitle>{t({ en: "Project budgets are separate from organization billing limits", "zh-CN": "项目预算不等于组织计费额度", ja: "プロジェクト予算は組織の請求上限とは別です", ko: "프로젝트 예산은 조직 청구 한도와 별개입니다" })}</AlertTitle>
                <AlertDescription>
                  {t({ en: "This controls monthly budget alerts and an optional hard limit for the current project. Platform administrators manage organization credit under Platform → Usage → Organization limits.", "zh-CN": "这里控制当前项目的月度预算提醒和可选硬限额。组织可用额度由平台管理员在“全平台 → 用量 → 组织限额”中管理。", ja: "ここでは現在のプロジェクトの月間予算通知と任意のハード上限を制御します。組織の利用可能額は、プラットフォーム管理者が「プラットフォーム → 使用量 → 組織上限」で管理します。", ko: "여기에서는 현재 프로젝트의 월 예산 알림과 선택적 하드 한도를 제어합니다. 조직의 사용 가능 크레딧은 플랫폼 관리자가 플랫폼 → 사용량 → 조직 한도에서 관리합니다." })}
                </AlertDescription>
              </Alert>

              <section aria-labelledby="project-spend-heading" className="space-y-4">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <h3 id="project-spend-heading" className="text-sm font-medium">
                      {t({ en: "Spend this month", "zh-CN": "本月消费", ja: "今月の支出", ko: "이번 달 지출" })}
                    </h3>
                    <p className="mt-1 text-2xl font-semibold tabular-nums">
                      {formatMicros(budget.spend_micros, selectedCurrency, locale)}
                    </p>
                    {budget.reserved_micros !== "0" ? (
                      <p className="mt-1 text-xs text-muted-foreground">
                        {t({ en: "{amount} additionally reserved for pending settlement", "zh-CN": "另有 {amount} 待结算预留", ja: "別途 {amount} を未決済分として予約", ko: "미결제 금액으로 {amount} 추가 예약됨" }, { amount: formatMicros(budget.reserved_micros, selectedCurrency, locale) })}
                      </p>
                    ) : null}
                  </div>
                  <div className="text-right text-sm text-muted-foreground">
                    <p>{t({ en: "Budget", "zh-CN": "预算", ja: "予算", ko: "예산" })}</p>
                    <p className="mt-1 font-medium tabular-nums text-foreground">
                      {budget.monthly_budget_micros
                        ? formatMicros(
                            budget.monthly_budget_micros,
                            selectedCurrency,
                            locale,
                          )
                        : t({ en: "Not set", "zh-CN": "尚未设置", ja: "未設定", ko: "설정되지 않음" })}
                    </p>
                  </div>
                </div>
                <div className="space-y-2">
                  <Progress
                    value={Math.min(usagePercent, 100)}
                    aria-label={t({ en: "{percent} of this month's budget used", "zh-CN": "本月预算已使用 {percent}", ja: "今月の予算を {percent} 使用", ko: "이번 달 예산의 {percent} 사용" }, { percent: formatPercent(usagePercent, locale) })}
                  />
                  <div className="flex items-center justify-between gap-3 text-xs text-muted-foreground">
                    <span>{t({ en: "{percent} used", "zh-CN": "{percent} 已使用", ja: "{percent} 使用済み", ko: "{percent} 사용됨" }, { percent: formatPercent(usagePercent, locale) })}</span>
                    <span className="inline-flex items-center gap-1">
                      <CalendarDays className="size-3.5" aria-hidden="true" />
                      {formatPeriod(budget.period_start_ms, budget.period_end_ms, locale)}
                    </span>
                  </div>
                </div>
                <div className="rounded-md border bg-muted/35 p-3 text-sm text-muted-foreground">
                  {budget.limit_type === "hard"
                    ? t({ en: "The monthly limit follows UTC calendar months. Admission checks settled spend, pending reservations, and the maximum quote for this request. Requests over the limit do not enter the execution queue.", "zh-CN": "月度限额按 UTC 自然月统计。系统会在准入时按已结算消费、待结算预留和本次最大报价校验，超限请求不会进入执行队列。", ja: "月間上限は UTC の暦月で集計されます。受付時に確定済み支出、未決済予約、このリクエストの最大見積額を確認し、上限超過のリクエストは実行キューに入りません。", ko: "월 한도는 UTC 역월 기준입니다. 승인 시 확정 지출, 미결제 예약 및 이번 요청의 최대 견적을 확인하며 한도를 초과한 요청은 실행 대기열에 들어가지 않습니다." })
                    : t({ en: "The monthly budget follows UTC calendar months. Requests continue to run after the budget is reached or exceeded, and organization and project owners receive alerts.", "zh-CN": "月度预算按 UTC 自然月统计。达到或超过预算后请求仍会继续执行，组织和项目所有者会收到提醒。", ja: "月間予算は UTC の暦月で集計されます。予算に達した後もリクエストは実行され、組織とプロジェクトの所有者に通知されます。", ko: "월 예산은 UTC 역월 기준입니다. 예산에 도달하거나 초과해도 요청은 계속 실행되며 조직 및 프로젝트 소유자에게 알림이 전송됩니다." })}
                </div>
              </section>

              <section aria-labelledby="project-budget-heading" className="space-y-4">
                <div>
                  <h3 id="project-budget-heading" className="text-sm font-medium">
                    {t({ en: "Monthly budget", "zh-CN": "月度预算", ja: "月間予算", ko: "월 예산" })}
                  </h3>
                  <p className="mt-1 text-sm text-muted-foreground">
                    {t({ en: "Set a monthly spending target and choose whether to block new requests when the limit is reached.", "zh-CN": "设置项目的月度消费目标，并选择是否在达到上限时阻断新请求。", ja: "月間支出目標を設定し、上限到達時に新しいリクエストをブロックするか選択します。", ko: "월 지출 목표를 설정하고 한도에 도달할 때 새 요청을 차단할지 선택합니다." })}
                  </p>
                </div>
                <div className="grid gap-4 sm:grid-cols-[112px_1fr]">
                  <div className="space-y-2">
                    <Label htmlFor="project-budget-currency">{t({ en: "Currency", "zh-CN": "币种", ja: "通貨", ko: "통화" })}</Label>
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
                    <Label htmlFor="project-monthly-budget">{t({ en: "Monthly budget", "zh-CN": "每月预算", ja: "月間予算", ko: "월 예산" })}</Label>
                    <Input
                      id="project-monthly-budget"
                      inputMode="decimal"
                      value={monthlyBudget}
                      onChange={(event) => setMonthlyBudget(event.target.value)}
                      placeholder={t({ en: "For example, 100.00", "zh-CN": "例如 100.00", ja: "例: 100.00", ko: "예: 100.00" })}
                      disabled={!canManage || saving}
                    />
                  </div>
                </div>
                <div className="flex items-start justify-between gap-4 rounded-md border p-4">
                  <div className="space-y-1">
                    <Label htmlFor="project-hard-limit">{t({ en: "Enforce hard limit", "zh-CN": "强制执行硬限额", ja: "ハード上限を適用", ko: "하드 한도 적용" })}</Label>
                    <p className="text-sm text-muted-foreground">
                      {t({ en: "When enabled, new requests return a project budget exceeded error after the monthly limit is reached.", "zh-CN": "开启后，达到月度限额的新请求将返回项目预算超限错误。", ja: "有効にすると、月間上限到達後の新しいリクエストはプロジェクト予算超過エラーを返します。", ko: "활성화하면 월 한도에 도달한 후 새 요청에 프로젝트 예산 초과 오류가 반환됩니다." })}
                    </p>
                  </div>
                  <Switch
                    id="project-hard-limit"
                    checked={limitType === "hard"}
                    onCheckedChange={(checked) =>
                      setLimitType(checked ? "hard" : "soft")
                    }
                    disabled={!canManage || saving}
                    aria-label={t({ en: "Enforce project monthly hard limit", "zh-CN": "强制执行项目月度硬限额", ja: "プロジェクトの月間ハード上限を適用", ko: "프로젝트 월 하드 한도 적용" })}
                  />
                </div>
              </section>

              <section aria-labelledby="project-alerts-heading" className="space-y-4">
                <div>
                  <h3 id="project-alerts-heading" className="text-sm font-medium">
                    {t({ en: "Alert thresholds", "zh-CN": "通知阈值", ja: "通知しきい値", ko: "알림 임계값" })}
                  </h3>
                  <p className="mt-1 text-sm text-muted-foreground">
                    {t({ en: "Each threshold sends one alert per budget version and calendar month.", "zh-CN": "每个阈值在当前预算版本和自然月内只通知一次。", ja: "各しきい値の通知は、現在の予算バージョンと暦月ごとに 1 回だけ送信されます。", ko: "각 임계값은 현재 예산 버전 및 역월마다 한 번만 알림을 보냅니다." })}
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
                      {t({ en: "Budget reaches {threshold}%", "zh-CN": "预算达到 {threshold}%", ja: "予算が {threshold}% に到達", ko: "예산 {threshold}% 도달" }, { threshold })}
                    </label>
                  ))}
                  <label className="flex min-h-10 items-center gap-3 rounded-md border px-3 text-sm">
                    <Checkbox checked disabled aria-label={t({ en: "Budget reaches 100%", "zh-CN": "预算达到 100%", ja: "予算が 100% に到達", ko: "예산 100% 도달" })} />
                    {t({ en: "Budget reaches 100%", "zh-CN": "预算达到 100%", ja: "予算が 100% に到達", ko: "예산 100% 도달" })}
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
                            aria-label={t({ en: "Remove {threshold}% threshold", "zh-CN": "移除 {threshold}% 阈值", ja: "{threshold}% のしきい値を削除", ko: "{threshold}% 임계값 제거" }, { threshold })}
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
                      placeholder={t({ en: "Custom percentage", "zh-CN": "自定义百分比", ja: "カスタム割合", ko: "사용자 지정 비율" })}
                      disabled={saving}
                    />
                    <Button
                      type="button"
                      variant="outline"
                      size="icon"
                      onClick={addCustomThreshold}
                      disabled={saving || !customThreshold}
                      aria-label={t({ en: "Add alert threshold", "zh-CN": "添加通知阈值", ja: "通知しきい値を追加", ko: "알림 임계값 추가" })}
                      title={t({ en: "Add alert threshold", "zh-CN": "添加通知阈值", ja: "通知しきい値を追加", ko: "알림 임계값 추가" })}
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
                      {t({ en: "Alerts this month", "zh-CN": "本月提醒", ja: "今月の通知", ko: "이번 달 알림" })}
                    </h3>
                  </div>
                  <div className="space-y-2">
                    {budget.alert_events.map((event) => (
                      <div
                        key={event.event_id}
                        className="flex items-center justify-between gap-3 rounded-md border px-3 py-2 text-sm"
                      >
                        <span>{t({ en: "Reached {threshold}%", "zh-CN": "已达到 {threshold}%", ja: "{threshold}% に到達", ko: "{threshold}% 도달" }, { threshold: event.threshold_percent })}</span>
                        <span className="text-xs text-muted-foreground">
                          {formatDateTime(event.created_at_ms, locale)}
                        </span>
                      </div>
                    ))}
                  </div>
                </section>
              ) : null}

              {!canManage ? (
                <p className="text-sm text-muted-foreground">
                  {t({ en: "You can view this project's limits. Only project or organization owners can change them.", "zh-CN": "你可以查看此项目的限额；只有项目或组织所有者可以修改。", ja: "このプロジェクトの上限は閲覧できます。変更できるのはプロジェクトまたは組織の所有者のみです。", ko: "이 프로젝트의 한도를 볼 수 있습니다. 프로젝트 또는 조직 소유자만 변경할 수 있습니다." })}
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
              {error ?? t({ en: "Project budget is temporarily unavailable", "zh-CN": "项目预算暂时不可用", ja: "プロジェクト予算は一時的に利用できません", ko: "프로젝트 예산을 일시적으로 사용할 수 없습니다" })}
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
              {t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}
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
              {t({ en: "Save general settings", "zh-CN": "保存常规设置", ja: "一般設定を保存", ko: "일반 설정 저장" })}
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
              {t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}
            </Button>
            <Button type="button" onClick={() => void save()} disabled={saving}>
              {saving ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <Save aria-hidden="true" />
              )}
              {t({ en: "Save budget settings", "zh-CN": "保存预算设置", ja: "予算設定を保存", ko: "예산 설정 저장" })}
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

function formatMicros(value: string, currency: string, locale: string) {
  const amount = Number(value) / 1_000_000;
  if (!Number.isFinite(amount)) return `${value} ${currency}`;
  try {
    return new Intl.NumberFormat(locale, {
      style: "currency",
      currency,
      maximumFractionDigits: 6,
    }).format(amount);
  } catch {
    return `${amount.toLocaleString(locale, { maximumFractionDigits: 6 })} ${currency}`;
  }
}

function formatPercent(value: number, locale: string) {
  return `${new Intl.NumberFormat(locale, {
    maximumFractionDigits: 2,
  }).format(value)}%`;
}

function formatPeriod(startMs: number, endMs: number, locale: string) {
  const formatter = new Intl.DateTimeFormat(locale, {
    timeZone: "UTC",
    month: "short",
    day: "numeric",
  });
  return `${formatter.format(new Date(startMs))} - ${formatter.format(
    new Date(endMs - 1),
  )} UTC`;
}

function formatDateTime(value: number, locale: string) {
  return new Intl.DateTimeFormat(locale, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}

function formatUnix(value: number, locale: string) {
  return new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value * 1000));
}

async function responseMessage(response: Response, fallback: string) {
  const body = (await response.json().catch(() => null)) as
    | { error?: string | { message?: string } }
    | null;
  if (typeof body?.error === "string") return body.error;
  if (body?.error && typeof body.error === "object" && body.error.message) {
    return body.error.message;
  }
  return `${fallback} (${response.status})`;
}
