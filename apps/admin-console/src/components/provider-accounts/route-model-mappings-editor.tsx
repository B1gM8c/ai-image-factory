"use client";

import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type {
  ProviderAccountModel,
  ProviderModelView,
  ProviderRoute,
  ProviderRouteModelMapping,
} from "@/lib/admin/types";

export type EditableRouteModelMapping = {
  apiProfile: string;
  publicModelId: string;
  providerModelId: string;
  mediaKind: "image" | "video";
};

export type EditableRouteModelMappings = Record<
  string,
  EditableRouteModelMapping
>;

export function RouteModelMappingsEditor({
  providerId,
  operationId,
  models,
  activeApiProfile,
  onActiveApiProfileChange,
  mappings,
  onMappingsChange,
  accountModels,
  enabledAccountModels,
  onAccountModelToggle,
}: {
  providerId: string;
  operationId: string;
  models: ProviderModelView[];
  activeApiProfile: string;
  onActiveApiProfileChange: (value: string) => void;
  mappings: EditableRouteModelMappings;
  onMappingsChange: (value: EditableRouteModelMappings) => void;
  accountModels?: ProviderAccountModel[];
  enabledAccountModels?: Set<string>;
  onAccountModelToggle?: (model: ProviderAccountModel) => void;
}) {
  const profiles = apiProfilesFor(providerId, operationId);
  const routeModels = models.filter(
    (model) =>
      model.provider_id === providerId &&
      model.adapter_state === "supported" &&
      model.lifecycle_state === "enabled" &&
      model.operation_ids.includes(operationId) &&
      defaultPublicModelId(activeApiProfile, model.model_id) !== null,
  );

  function toggleModel(model: ProviderModelView) {
    const key = routeModelMappingKey(activeApiProfile, model.model_id);
    if (mappings[key]) {
      const next = { ...mappings };
      delete next[key];
      onMappingsChange(next);
      return;
    }
    const publicModelId = defaultPublicModelId(
      activeApiProfile,
      model.model_id,
    );
    if (!publicModelId) return;
    onMappingsChange({
      ...mappings,
      [key]: {
        apiProfile: activeApiProfile,
        publicModelId,
        providerModelId: model.model_id,
        mediaKind: model.media_kind,
      },
    });
  }

  function updatePublicModelId(modelId: string, publicModelId: string) {
    const key = routeModelMappingKey(activeApiProfile, modelId);
    onMappingsChange({
      ...mappings,
      [key]: { ...mappings[key], publicModelId },
    });
  }

  return (
    <div className="min-w-0 space-y-5">
      <div className="grid gap-2 md:grid-cols-[12rem_minmax(0,1fr)] md:items-center">
        <Label>API 协议</Label>
        <Select
          value={activeApiProfile}
          onValueChange={onActiveApiProfileChange}
        >
          <SelectTrigger aria-label="API 协议">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {profiles.map((profile) => (
              <SelectItem key={profile.id} value={profile.id}>
                {profile.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="min-w-0 max-w-full overflow-hidden border">
        <div
          className={
            accountModels
              ? "hidden grid-cols-[4.75rem_minmax(8rem,1fr)_5rem_minmax(8rem,1fr)] items-center border-b bg-muted/40 px-3 py-2 text-xs font-medium text-muted-foreground md:grid"
              : "hidden grid-cols-[3rem_minmax(12rem,1fr)_7rem_minmax(15rem,1.2fr)] items-center border-b bg-muted/40 px-3 py-2 text-xs font-medium text-muted-foreground md:grid"
          }
        >
          {accountModels ? <span>账户可用</span> : <span />}
          <span>原生模型</span>
          {accountModels ? <span>API 开放</span> : <span>类型</span>}
          <span>外部模型 ID</span>
        </div>
        {routeModels.length === 0 ? (
          <div className="flex min-h-28 items-center justify-center text-sm text-muted-foreground">
            当前能力没有可配置模型
          </div>
        ) : (
          routeModels.map((model) => {
            const key = routeModelMappingKey(activeApiProfile, model.model_id);
            const mapping = mappings[key];
            const publicModelIdError = mapping
              ? routeModelMappingError(mapping, Object.values(mappings))
              : null;
            const accountModel = accountModels?.find(
              (item) =>
                item.model_id === model.model_id &&
                item.media_kind === model.media_kind,
            );
            const accountModelEnabled = accountModels
              ? Boolean(
                  accountModel &&
                  enabledAccountModels?.has(accountModelKey(accountModel)),
                )
              : true;
            return (
              <div
                key={key}
                className={
                  accountModels
                    ? "grid min-w-0 grid-cols-[2rem_minmax(0,1fr)] items-center gap-x-3 gap-y-3 border-b px-3 py-3 last:border-b-0 md:grid-cols-[4.75rem_minmax(8rem,1fr)_5rem_minmax(8rem,1fr)] md:gap-0 md:py-2.5"
                    : "grid grid-cols-[2rem_minmax(0,1fr)_auto] items-center gap-x-3 gap-y-2 border-b px-3 py-3 last:border-b-0 md:grid-cols-[3rem_minmax(12rem,1fr)_7rem_minmax(15rem,1.2fr)] md:gap-0 md:py-2.5"
                }
              >
                {accountModels ? (
                  <input
                    type="checkbox"
                    checked={accountModelEnabled}
                    disabled={!accountModel?.configurable}
                    onChange={() =>
                      accountModel && onAccountModelToggle?.(accountModel)
                    }
                    className="size-4 accent-primary"
                    aria-label={`允许账户使用 ${model.display_name}`}
                  />
                ) : (
                  <input
                    type="checkbox"
                    checked={Boolean(mapping)}
                    onChange={() => toggleModel(model)}
                    className="size-4 accent-primary"
                    aria-label={`开放 ${model.display_name}`}
                  />
                )}
                <div className="min-w-0 pr-3">
                  <p className="truncate text-sm font-medium">
                    {model.display_name}
                  </p>
                  <p className="truncate font-mono text-xs text-muted-foreground">
                    {model.model_id}
                  </p>
                </div>
                {!accountModels ? (
                  <Badge variant="outline" className="w-fit">
                    {model.media_kind === "image" ? "图片" : "视频"}
                  </Badge>
                ) : null}
                {accountModels ? (
                  <label className="col-span-2 flex items-center gap-2 pl-11 text-xs text-muted-foreground md:col-span-1 md:block md:pl-0">
                    <input
                      type="checkbox"
                      checked={Boolean(mapping)}
                      disabled={!accountModelEnabled}
                      onChange={() => toggleModel(model)}
                      className="size-4 accent-primary"
                      aria-label={`通过 API 开放 ${model.display_name}`}
                    />
                    <span className="md:hidden">通过 API 开放</span>
                  </label>
                ) : null}
                <div
                  className={
                    accountModels
                      ? "col-span-2 min-w-0 max-w-full pl-11 md:col-span-1 md:pl-0"
                      : "col-span-3 min-w-0 max-w-full pl-11 md:col-span-1 md:pl-0"
                  }
                >
                  <span className="text-xs text-muted-foreground md:hidden">
                    外部模型 ID
                  </span>
                  <Input
                    value={mapping?.publicModelId ?? ""}
                    onChange={(event) =>
                      updatePublicModelId(model.model_id, event.target.value)
                    }
                    disabled={!mapping || !accountModelEnabled}
                    className="mt-1 h-8 w-full min-w-0 max-w-full font-mono text-xs md:mt-0"
                    maxLength={255}
                    aria-label={`${model.display_name} 外部模型 ID`}
                    aria-invalid={Boolean(publicModelIdError)}
                  />
                  {publicModelIdError ? (
                    <span className="mt-1 block text-xs text-destructive">
                      {publicModelIdError}
                    </span>
                  ) : null}
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}

export function apiProfilesFor(providerId: string, operationId: string) {
  if (providerId === "openai-codex")
    return [{ id: "openai-images-v1", label: "OpenAI Images API" }];
  if (providerId === "grok-cli" && operationId === "images.generations")
    return [{ id: "xai-images-v1", label: "xAI Images API" }];
  if (providerId === "grok-cli" && operationId === "videos.generations")
    return [{ id: "xai-videos-v1", label: "xAI Video API" }];
  if (providerId === "dreamina-cli" && operationId === "images.generations") {
    return [
      { id: "volcengine-ark-images-v3", label: "火山方舟 Images API" },
      { id: "dreamina-cli-images-v1", label: "即梦 CLI Images API" },
    ];
  }
  if (providerId === "dreamina-cli" && operationId === "videos.generations") {
    return [
      {
        id: "volcengine-ark-content-generation-v3",
        label: "火山方舟 Content API",
      },
      { id: "dreamina-cli-videos-v1", label: "即梦 CLI Video API" },
    ];
  }
  return [];
}

export function routeModelMappingsFromRoute(
  route: Pick<ProviderRoute, "model_mappings">,
): EditableRouteModelMappings {
  return Object.fromEntries(
    route.model_mappings.map((mapping) => [
      routeModelMappingKey(mapping.api_profile, mapping.provider_model_id),
      editableMapping(mapping),
    ]),
  );
}

export function defaultRouteModelMappings(
  models: ProviderModelView[],
  providerId: string,
  operationId: string,
) {
  const mappings: EditableRouteModelMappings = {};
  for (const profile of apiProfilesFor(providerId, operationId)) {
    for (const model of models) {
      if (
        model.provider_id !== providerId ||
        model.adapter_state !== "supported" ||
        model.lifecycle_state !== "enabled" ||
        !model.operation_ids.includes(operationId)
      )
        continue;
      const publicModelId = defaultPublicModelId(profile.id, model.model_id);
      if (!publicModelId) continue;
      mappings[routeModelMappingKey(profile.id, model.model_id)] = {
        apiProfile: profile.id,
        publicModelId,
        providerModelId: model.model_id,
        mediaKind: model.media_kind,
      };
    }
  }
  return mappings;
}

export function routeModelMappingsAreValid(
  mappings: EditableRouteModelMapping[],
) {
  return (
    mappings.length > 0 &&
    mappings.every((mapping) => !routeModelMappingError(mapping, mappings))
  );
}

export function routeModelMappingsRequest(
  mappings: EditableRouteModelMappings,
) {
  return Object.values(mappings).map((mapping) => ({
    api_profile: mapping.apiProfile,
    public_model_id: mapping.publicModelId.trim(),
    provider_model_id: mapping.providerModelId,
    media_kind: mapping.mediaKind,
  }));
}

function editableMapping(
  mapping: ProviderRouteModelMapping,
): EditableRouteModelMapping {
  return {
    apiProfile: mapping.api_profile,
    publicModelId: mapping.public_model_id,
    providerModelId: mapping.provider_model_id,
    mediaKind: mapping.media_kind,
  };
}

function routeModelMappingKey(apiProfile: string, providerModelId: string) {
  return `${apiProfile}\u0000${providerModelId}`;
}

function accountModelKey(
  model: Pick<ProviderAccountModel, "model_id" | "media_kind">,
) {
  return `${model.media_kind}:${model.model_id}`;
}

function routeModelMappingError(
  mapping: EditableRouteModelMapping,
  mappings: EditableRouteModelMapping[],
) {
  const publicModelId = mapping.publicModelId.trim();
  if (!/^[A-Za-z0-9_.:-]{1,255}$/.test(publicModelId)) {
    return "仅支持字母、数字及 _ . : -";
  }
  if (
    mappings.filter(
      (candidate) =>
        candidate.apiProfile === mapping.apiProfile &&
        candidate.publicModelId.trim() === publicModelId,
    ).length > 1
  ) {
    return "同一 API 协议下模型 ID 不能重复";
  }
  return null;
}

function defaultPublicModelId(
  apiProfile: string,
  providerModelId: string,
): string | null {
  if (apiProfile === "volcengine-ark-images-v3") {
    if (providerModelId === "5.0") return "doubao-seedream-5-0-lite";
    if (providerModelId === "5.0Pro") return "doubao-seedream-5-0-260128";
    return null;
  }
  if (apiProfile === "volcengine-ark-content-generation-v3") {
    if (providerModelId === "seedance2.0") return "doubao-seedance-2-0-260128";
    if (providerModelId === "seedance2.0fast")
      return "doubao-seedance-2-0-fast-260128";
    if (providerModelId === "seedance2.0mini")
      return "doubao-seedance-2-0-mini-260128";
    return null;
  }
  return providerModelId;
}
