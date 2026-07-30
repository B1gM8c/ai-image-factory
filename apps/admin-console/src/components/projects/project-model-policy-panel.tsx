"use client";

import { useEffect, useMemo, useState } from "react";
import { ImageIcon, LoaderCircle, Save, Search, Video } from "lucide-react";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useI18n } from "@/i18n/locale-provider";
import { consoleFetch } from "@/lib/auth/client";

type ModelIdentity = {
  operation_id: string;
  api_profile: string;
  public_model_id: string;
  media_kind: "image" | "video";
};

type ModelRateLimit = {
  bucket_key: string;
  bucket_display_name: string;
  shared: boolean;
  unit_kind: "image" | "video_second";
  request_limit_per_minute: number | null;
  unit_limit_per_minute: number | null;
  inherited_request_ceiling_per_minute: number | null;
  inherited_unit_ceiling_per_minute: number | null;
};

type ProjectModel = ModelIdentity & {
  providers: string[];
  allowed: boolean;
  rate_limit: ModelRateLimit;
};

type ProjectModelPolicy = {
  object: "organization.project.model_policy";
  project_id: string;
  organization_id: string;
  configured: boolean;
  default_behavior: "allow_routable" | "deny_unlisted";
  models: ProjectModel[];
  control_version: string;
  updated_at_ms: number | null;
};

type EditableModel = ProjectModel & {
  requestLimit: string;
  unitLimit: string;
};

export function ProjectModelPolicyPanel({
  projectId,
  canManage,
  active,
}: {
  projectId: string;
  canManage: boolean;
  active: boolean;
}) {
  const { t } = useI18n();
  const [policy, setPolicy] = useState<ProjectModelPolicy | null>(null);
  const [models, setModels] = useState<EditableModel[]>([]);
  const [query, setQuery] = useState("");
  const [media, setMedia] = useState<"all" | "image" | "video">("all");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!active) return;
    let current = true;
    setLoading(true);
    setError(null);
    void loadPolicy(
      projectId,
      t({ en: "Request failed", "zh-CN": "请求失败", ja: "リクエストに失敗しました", ko: "요청에 실패했습니다" }),
    )
      .then((next) => {
        if (!current) return;
        setPolicy(next);
        setModels(editableModels(next.models));
      })
      .catch((reason) => {
        if (current) {
          setError(reason instanceof Error ? reason.message : t({ en: "Failed to load model policy", "zh-CN": "模型策略加载失败", ja: "モデルポリシーを読み込めませんでした", ko: "모델 정책을 불러오지 못했습니다" }));
        }
      })
      .finally(() => {
        if (current) setLoading(false);
      });
    return () => {
      current = false;
    };
  }, [active, projectId, t]);

  const filtered = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    return models.filter(
      (model) =>
        (media === "all" || model.media_kind === media) &&
        (!normalized ||
          model.public_model_id.toLowerCase().includes(normalized) ||
          model.providers.some((provider) =>
            provider.toLowerCase().includes(normalized),
          )),
    );
  }, [media, models, query]);
  const allowedCount = models.filter((model) => model.allowed).length;
  const allAllowed = models.length > 0 && allowedCount === models.length;
  const partiallyAllowed = allowedCount > 0 && !allAllowed;

  function setAllAllowed(allowed: boolean) {
    setModels((current) => current.map((model) => ({ ...model, allowed })));
  }

  function setAllowed(model: EditableModel, allowed: boolean) {
    setModels((current) =>
      current.map((item) =>
        sameModel(item, model) ? { ...item, allowed } : item,
      ),
    );
  }

  function setBucketLimit(
    bucketKey: string,
    field: "requestLimit" | "unitLimit",
    value: string,
  ) {
    if (value && !/^\d{0,10}$/.test(value)) return;
    setModels((current) =>
      current.map((model) =>
        model.rate_limit.bucket_key === bucketKey
          ? { ...model, [field]: value }
          : model,
      ),
    );
  }

  async function save() {
    if (!policy || !canManage) return;
    const invalid = models.find(
      (model) =>
        !validOptionalLimit(model.requestLimit) ||
        !validOptionalLimit(model.unitLimit) ||
        exceedsCeiling(
          model.requestLimit,
          model.rate_limit.inherited_request_ceiling_per_minute,
        ) ||
        exceedsCeiling(
          model.unitLimit,
          model.rate_limit.inherited_unit_ceiling_per_minute,
        ),
    );
    if (invalid) {
      setError(t({ en: "Check the per-minute limits for {model}", "zh-CN": "请检查 {model} 的每分钟限额", ja: "{model} の 1 分あたりの上限を確認してください", ko: "{model}의 분당 한도를 확인하세요" }, { model: invalid.public_model_id }));
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/model-policy`,
        {
          method: "PUT",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            allowed_models: models
              .filter((model) => model.allowed)
              .map(modelIdentity),
            rate_limits: models
              .filter((model) => model.requestLimit || model.unitLimit)
              .map((model) => ({
                ...modelIdentity(model),
                request_limit_per_minute: optionalNumber(model.requestLimit),
                unit_limit_per_minute: optionalNumber(model.unitLimit),
              })),
            expected_control_version: policy.control_version,
          }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response, t({ en: "Request failed", "zh-CN": "请求失败", ja: "リクエストに失敗しました", ko: "요청에 실패했습니다" })));
      const next = (await response.json()) as ProjectModelPolicy;
      setPolicy(next);
      setModels(editableModels(next.models));
      toast.success(t({ en: "Project model policy saved", "zh-CN": "项目模型策略已保存", ja: "プロジェクトのモデルポリシーを保存しました", ko: "프로젝트 모델 정책을 저장했습니다" }));
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : t({ en: "Failed to save model policy", "zh-CN": "模型策略保存失败", ja: "モデルポリシーを保存できませんでした", ko: "모델 정책을 저장하지 못했습니다" }));
    } finally {
      setSaving(false);
    }
  }

  if (loading && !policy) {
    return (
      <div className="grid min-h-72 place-items-center text-muted-foreground">
        <LoaderCircle className="size-5 animate-spin" aria-label={t({ en: "Loading model policy", "zh-CN": "正在加载模型策略", ja: "モデルポリシーを読み込み中", ko: "모델 정책 불러오는 중" })} />
      </div>
    );
  }

  if (!policy) {
    return (
      <div className="grid min-h-72 place-items-center px-6 text-center text-sm text-muted-foreground">
        {error ?? t({ en: "Project model policy is temporarily unavailable", "zh-CN": "项目模型策略暂时不可用", ja: "プロジェクトのモデルポリシーは一時的に利用できません", ko: "프로젝트 모델 정책을 일시적으로 사용할 수 없습니다" })}
      </div>
    );
  }

  return (
    <div className="space-y-5 px-5 py-6 sm:px-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-medium">{t({ en: "Model access", "zh-CN": "模型使用", ja: "モデルアクセス", ko: "모델 액세스" })}</h3>
          <p className="mt-1 text-sm text-muted-foreground">
            {t({ en: "Project API keys, service accounts, and creation tools inherit this model scope and its limits.", "zh-CN": "项目 API Key、服务账户和创作页面共同继承这里的模型范围与限额。", ja: "プロジェクト API キー、サービスアカウント、作成ツールは、ここで設定したモデル範囲と上限を継承します。", ko: "프로젝트 API 키, 서비스 계정 및 제작 도구는 여기의 모델 범위와 한도를 상속합니다." })}
          </p>
        </div>
        <Badge variant="secondary">
          {t({ en: "{allowed} of {total} enabled", "zh-CN": "已启用 {allowed} / {total}", ja: "{total} 件中 {allowed} 件を有効化", ko: "{total}개 중 {allowed}개 활성화" }, { allowed: allowedCount, total: models.length })}
        </Badge>
      </div>

      <div className="grid gap-3 sm:grid-cols-[1fr_160px]">
        <div className="relative">
          <Search
            className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
            aria-hidden="true"
          />
          <Input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder={t({ en: "Search models or providers", "zh-CN": "搜索模型或供应商", ja: "モデルまたはプロバイダーを検索", ko: "모델 또는 공급자 검색" })}
            className="pl-9"
          />
        </div>
        <Select
          value={media}
          onValueChange={(value) => setMedia(value as typeof media)}
        >
          <SelectTrigger aria-label={t({ en: "Filter by media type", "zh-CN": "筛选媒体类型", ja: "メディアタイプで絞り込み", ko: "미디어 유형 필터" })}>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">{t({ en: "All types", "zh-CN": "全部类型", ja: "すべてのタイプ", ko: "모든 유형" })}</SelectItem>
            <SelectItem value="image">{t({ en: "Image", "zh-CN": "图片", ja: "画像", ko: "이미지" })}</SelectItem>
            <SelectItem value="video">{t({ en: "Video", "zh-CN": "视频", ja: "動画", ko: "동영상" })}</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="overflow-hidden rounded-md border">
        <div className="flex min-h-11 items-center gap-3 border-b bg-muted/30 px-4 text-sm">
          <Checkbox
            checked={partiallyAllowed ? "indeterminate" : allAllowed}
            onCheckedChange={(checked) => setAllAllowed(checked === true)}
            disabled={!canManage || saving || models.length === 0}
            aria-label={t({ en: "Toggle all models", "zh-CN": "切换全部模型", ja: "すべてのモデルを切り替え", ko: "모든 모델 전환" })}
          />
          <span className="font-medium">{t({ en: "Available models", "zh-CN": "可用模型", ja: "利用可能なモデル", ko: "사용 가능한 모델" })}</span>
          <span className="ml-auto text-xs text-muted-foreground">
            {t({ en: "Blank limits inherit platform settings", "zh-CN": "未填写限额时继承平台配置", ja: "上限が空欄の場合はプラットフォーム設定を継承", ko: "한도를 비워 두면 플랫폼 설정 상속" })}
          </span>
        </div>
        {filtered.length > 0 ? (
          <div className="divide-y">
            {filtered.map((model) => (
              <ModelRow
                key={modelKey(model)}
                model={model}
                canManage={canManage}
                saving={saving}
                onAllowedChange={(allowed) => setAllowed(model, allowed)}
                onLimitChange={(field, value) =>
                  setBucketLimit(model.rate_limit.bucket_key, field, value)
                }
              />
            ))}
          </div>
        ) : (
          <div className="grid min-h-40 place-items-center px-6 text-center text-sm text-muted-foreground">
            {models.length === 0
              ? t({ en: "No routable models are available for this project", "zh-CN": "当前项目没有可路由模型", ja: "このプロジェクトでルーティング可能なモデルはありません", ko: "이 프로젝트에 라우팅 가능한 모델이 없습니다" })
              : t({ en: "No models match the filters", "zh-CN": "没有符合条件的模型", ja: "条件に一致するモデルはありません", ko: "필터와 일치하는 모델이 없습니다" })}
          </div>
        )}
      </div>

      {!canManage ? (
        <p className="text-sm text-muted-foreground">
          {t({ en: "You can view the model policy. Only project or organization owners can change it.", "zh-CN": "你可以查看模型策略；只有项目或组织所有者可以修改。", ja: "モデルポリシーは閲覧できます。変更できるのはプロジェクトまたは組織の所有者のみです。", ko: "모델 정책을 볼 수 있습니다. 프로젝트 또는 조직 소유자만 변경할 수 있습니다." })}
        </p>
      ) : null}
      {error ? (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      ) : null}
      {canManage ? (
        <div className="flex justify-end">
          <Button type="button" onClick={() => void save()} disabled={saving}>
            {saving ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <Save aria-hidden="true" />
            )}
            {t({ en: "Save model policy", "zh-CN": "保存模型策略", ja: "モデルポリシーを保存", ko: "모델 정책 저장" })}
          </Button>
        </div>
      ) : null}
    </div>
  );
}

function ModelRow({
  model,
  canManage,
  saving,
  onAllowedChange,
  onLimitChange,
}: {
  model: EditableModel;
  canManage: boolean;
  saving: boolean;
  onAllowedChange: (allowed: boolean) => void;
  onLimitChange: (
    field: "requestLimit" | "unitLimit",
    value: string,
  ) => void;
}) {
  const { t } = useI18n();
  const unitLabel =
    model.rate_limit.unit_kind === "image"
      ? t({ en: "Images / minute", "zh-CN": "图片 / 分钟", ja: "画像 / 分", ko: "이미지 / 분" })
      : t({ en: "Video seconds / minute", "zh-CN": "视频秒 / 分钟", ja: "動画秒 / 分", ko: "동영상 초 / 분" });
  return (
    <div className="grid gap-4 px-4 py-4 lg:grid-cols-[minmax(0,1fr)_140px_140px] lg:items-center">
      <div className="flex min-w-0 items-start gap-3">
        <Checkbox
          checked={model.allowed}
          onCheckedChange={(checked) => onAllowedChange(checked === true)}
          disabled={!canManage || saving}
          aria-label={t(model.allowed
            ? { en: "Disable {model}", "zh-CN": "停用 {model}", ja: "{model} を無効化", ko: "{model} 비활성화" }
            : { en: "Enable {model}", "zh-CN": "启用 {model}", ja: "{model} を有効化", ko: "{model} 활성화" }, { model: model.public_model_id })}
          className="mt-0.5"
        />
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            {model.media_kind === "image" ? (
              <ImageIcon className="size-4 text-muted-foreground" aria-hidden="true" />
            ) : (
              <Video className="size-4 text-muted-foreground" aria-hidden="true" />
            )}
            <span className="break-all font-medium">{model.public_model_id}</span>
            {model.rate_limit.shared ? (
              <Badge variant="outline">{t({ en: "Shared limit", "zh-CN": "共享限额", ja: "共有上限", ko: "공유 한도" })}</Badge>
            ) : null}
          </div>
          <p className="mt-1 truncate text-xs text-muted-foreground">
            {model.providers.join(", ")} · {model.api_profile}
          </p>
          {model.rate_limit.shared ? (
            <p className="mt-1 text-xs text-muted-foreground">
              {t({ en: "Shares capacity with protocol aliases in {bucket}.", "zh-CN": "与 {bucket} 的协议别名共用容量。", ja: "{bucket} のプロトコルエイリアスと容量を共有します。", ko: "{bucket}의 프로토콜 별칭과 용량을 공유합니다." }, { bucket: model.rate_limit.bucket_display_name })}
            </p>
          ) : null}
        </div>
      </div>
      <LimitInput
        id={limitInputId(model, "requests")}
        label={t({ en: "Requests / minute", "zh-CN": "请求 / 分钟", ja: "リクエスト / 分", ko: "요청 / 분" })}
        value={model.requestLimit}
        ceiling={model.rate_limit.inherited_request_ceiling_per_minute}
        disabled={!canManage || saving}
        onChange={(value) => onLimitChange("requestLimit", value)}
      />
      <LimitInput
        id={limitInputId(model, "units")}
        label={unitLabel}
        value={model.unitLimit}
        ceiling={model.rate_limit.inherited_unit_ceiling_per_minute}
        disabled={!canManage || saving}
        onChange={(value) => onLimitChange("unitLimit", value)}
      />
    </div>
  );
}

function LimitInput({
  id,
  label,
  value,
  ceiling,
  disabled,
  onChange,
}: {
  id: string;
  label: string;
  value: string;
  ceiling: number | null;
  disabled: boolean;
  onChange: (value: string) => void;
}) {
  const { t } = useI18n();
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id} className="text-xs text-muted-foreground">
        {label}
      </Label>
      <Input
        id={id}
        value={value}
        inputMode="numeric"
        placeholder={ceiling
          ? t({ en: "Inherit {limit}", "zh-CN": "继承 {limit}", ja: "{limit} を継承", ko: "{limit} 상속" }, { limit: ceiling })
          : t({ en: "Inherit", "zh-CN": "继承", ja: "継承", ko: "상속" })}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        aria-label={label}
      />
    </div>
  );
}

function editableModels(models: ProjectModel[]): EditableModel[] {
  return models.map((model) => ({
    ...model,
    requestLimit: model.rate_limit.request_limit_per_minute?.toString() ?? "",
    unitLimit: model.rate_limit.unit_limit_per_minute?.toString() ?? "",
  }));
}

function modelIdentity(model: ModelIdentity): ModelIdentity {
  return {
    operation_id: model.operation_id,
    api_profile: model.api_profile,
    public_model_id: model.public_model_id,
    media_kind: model.media_kind,
  };
}

function sameModel(left: ModelIdentity, right: ModelIdentity) {
  return modelKey(left) === modelKey(right);
}

function modelKey(model: ModelIdentity) {
  return [
    model.operation_id,
    model.api_profile,
    model.public_model_id,
    model.media_kind,
  ].join("\u0000");
}

function limitInputId(model: ModelIdentity, dimension: string) {
  return ["model-limit", model.operation_id, model.api_profile, model.public_model_id, dimension]
    .join("-")
    .replaceAll(/[^A-Za-z0-9_-]/g, "-");
}

function validOptionalLimit(value: string) {
  if (!value) return true;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 && parsed <= 2_147_483_647;
}

function exceedsCeiling(value: string, ceiling: number | null) {
  return Boolean(value && ceiling && Number(value) > ceiling);
}

function optionalNumber(value: string) {
  return value ? Number(value) : null;
}

async function loadPolicy(projectId: string, fallback: string) {
  const response = await consoleFetch(
    `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/model-policy`,
  );
  if (!response.ok) throw new Error(await responseMessage(response, fallback));
  return (await response.json()) as ProjectModelPolicy;
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
