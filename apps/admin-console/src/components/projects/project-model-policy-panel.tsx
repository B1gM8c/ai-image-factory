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
    void loadPolicy(projectId)
      .then((next) => {
        if (!current) return;
        setPolicy(next);
        setModels(editableModels(next.models));
      })
      .catch((reason) => {
        if (current) {
          setError(reason instanceof Error ? reason.message : "模型策略加载失败");
        }
      })
      .finally(() => {
        if (current) setLoading(false);
      });
    return () => {
      current = false;
    };
  }, [active, projectId]);

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
      setError(`请检查 ${invalid.public_model_id} 的每分钟限额`);
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
      if (!response.ok) throw new Error(await responseMessage(response));
      const next = (await response.json()) as ProjectModelPolicy;
      setPolicy(next);
      setModels(editableModels(next.models));
      toast.success("项目模型策略已保存");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "模型策略保存失败");
    } finally {
      setSaving(false);
    }
  }

  if (loading && !policy) {
    return (
      <div className="grid min-h-72 place-items-center text-muted-foreground">
        <LoaderCircle className="size-5 animate-spin" aria-label="正在加载模型策略" />
      </div>
    );
  }

  if (!policy) {
    return (
      <div className="grid min-h-72 place-items-center px-6 text-center text-sm text-muted-foreground">
        {error ?? "项目模型策略暂时不可用"}
      </div>
    );
  }

  return (
    <div className="space-y-5 px-5 py-6 sm:px-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-medium">模型使用</h3>
          <p className="mt-1 text-sm text-muted-foreground">
            项目 API Key、服务账户和创作页面共同继承这里的模型范围与限额。
          </p>
        </div>
        <Badge variant="secondary">
          已启用 {allowedCount} / {models.length}
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
            placeholder="搜索模型或供应商"
            className="pl-9"
          />
        </div>
        <Select
          value={media}
          onValueChange={(value) => setMedia(value as typeof media)}
        >
          <SelectTrigger aria-label="筛选媒体类型">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部类型</SelectItem>
            <SelectItem value="image">图片</SelectItem>
            <SelectItem value="video">视频</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="overflow-hidden rounded-md border">
        <div className="flex min-h-11 items-center gap-3 border-b bg-muted/30 px-4 text-sm">
          <Checkbox
            checked={partiallyAllowed ? "indeterminate" : allAllowed}
            onCheckedChange={(checked) => setAllAllowed(checked === true)}
            disabled={!canManage || saving || models.length === 0}
            aria-label="切换全部模型"
          />
          <span className="font-medium">可用模型</span>
          <span className="ml-auto text-xs text-muted-foreground">
            未填写限额时继承平台配置
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
            {models.length === 0 ? "当前项目没有可路由模型" : "没有符合条件的模型"}
          </div>
        )}
      </div>

      {!canManage ? (
        <p className="text-sm text-muted-foreground">
          你可以查看模型策略；只有项目或组织所有者可以修改。
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
            保存模型策略
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
  const unitLabel =
    model.rate_limit.unit_kind === "image" ? "图片 / 分钟" : "视频秒 / 分钟";
  return (
    <div className="grid gap-4 px-4 py-4 lg:grid-cols-[minmax(0,1fr)_140px_140px] lg:items-center">
      <div className="flex min-w-0 items-start gap-3">
        <Checkbox
          checked={model.allowed}
          onCheckedChange={(checked) => onAllowedChange(checked === true)}
          disabled={!canManage || saving}
          aria-label={`${model.allowed ? "停用" : "启用"} ${model.public_model_id}`}
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
              <Badge variant="outline">共享限额</Badge>
            ) : null}
          </div>
          <p className="mt-1 truncate text-xs text-muted-foreground">
            {model.providers.join("、")} · {model.api_profile}
          </p>
          {model.rate_limit.shared ? (
            <p className="mt-1 text-xs text-muted-foreground">
              与 {model.rate_limit.bucket_display_name} 的协议别名共用容量。
            </p>
          ) : null}
        </div>
      </div>
      <LimitInput
        id={limitInputId(model, "requests")}
        label="请求 / 分钟"
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
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id} className="text-xs text-muted-foreground">
        {label}
      </Label>
      <Input
        id={id}
        value={value}
        inputMode="numeric"
        placeholder={ceiling ? `继承 ${ceiling}` : "继承"}
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

async function loadPolicy(projectId: string) {
  const response = await consoleFetch(
    `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/model-policy`,
  );
  if (!response.ok) throw new Error(await responseMessage(response));
  return (await response.json()) as ProjectModelPolicy;
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
