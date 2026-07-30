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
import { useI18n } from "@/i18n/locale-provider";

type Translate = ReturnType<typeof useI18n>["t"];

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
  const { t } = useI18n();
  const profiles = apiProfilesFor(providerId, operationId, t);
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
        <Label>{t({ en: "API protocol", "zh-CN": "API 协议", ja: "API プロトコル", ko: "API 프로토콜" })}</Label>
        <Select
          value={activeApiProfile}
          onValueChange={onActiveApiProfileChange}
        >
          <SelectTrigger aria-label={t({ en: "API protocol", "zh-CN": "API 协议", ja: "API プロトコル", ko: "API 프로토콜" })}>
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
          {accountModels ? <span>{t({ en: "Account access", "zh-CN": "账户可用", ja: "アカウント利用", ko: "계정 사용" })}</span> : <span />}
          <span>{t({ en: "Native model", "zh-CN": "原生模型", ja: "ネイティブモデル", ko: "네이티브 모델" })}</span>
          {accountModels ? <span>{t({ en: "API access", "zh-CN": "API 开放", ja: "API 公開", ko: "API 공개" })}</span> : <span>{t({ en: "Type", "zh-CN": "类型", ja: "種別", ko: "유형" })}</span>}
          <span>{t({ en: "Public model ID", "zh-CN": "外部模型 ID", ja: "公開モデル ID", ko: "공개 모델 ID" })}</span>
        </div>
        {routeModels.length === 0 ? (
          <div className="flex min-h-28 items-center justify-center text-sm text-muted-foreground">
            {t({ en: "No configurable models for this capability", "zh-CN": "当前能力没有可配置模型", ja: "この機能に設定可能なモデルはありません", ko: "이 기능에 구성 가능한 모델이 없습니다" })}
          </div>
        ) : (
          routeModels.map((model) => {
            const key = routeModelMappingKey(activeApiProfile, model.model_id);
            const mapping = mappings[key];
            const publicModelIdError = mapping
              ? routeModelMappingError(mapping, Object.values(mappings), t)
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
                    aria-label={t(
                      { en: "Allow this account to use {model}", "zh-CN": "允许账户使用 {model}", ja: "このアカウントに {model} の使用を許可", ko: "이 계정에서 {model} 사용 허용" },
                      { model: model.display_name },
                    )}
                  />
                ) : (
                  <input
                    type="checkbox"
                    checked={Boolean(mapping)}
                    onChange={() => toggleModel(model)}
                    className="size-4 accent-primary"
                    aria-label={t(
                      { en: "Expose {model}", "zh-CN": "开放 {model}", ja: "{model} を公開", ko: "{model} 공개" },
                      { model: model.display_name },
                    )}
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
                    {model.media_kind === "image"
                      ? t({ en: "Image", "zh-CN": "图片", ja: "画像", ko: "이미지" })
                      : t({ en: "Video", "zh-CN": "视频", ja: "動画", ko: "동영상" })}
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
                      aria-label={t(
                        { en: "Expose {model} through the API", "zh-CN": "通过 API 开放 {model}", ja: "{model} を API で公開", ko: "API를 통해 {model} 공개" },
                        { model: model.display_name },
                      )}
                    />
                    <span className="md:hidden">{t({ en: "Expose through API", "zh-CN": "通过 API 开放", ja: "API で公開", ko: "API로 공개" })}</span>
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
                    {t({ en: "Public model ID", "zh-CN": "外部模型 ID", ja: "公開モデル ID", ko: "공개 모델 ID" })}
                  </span>
                  <Input
                    value={mapping?.publicModelId ?? ""}
                    onChange={(event) =>
                      updatePublicModelId(model.model_id, event.target.value)
                    }
                    disabled={!mapping || !accountModelEnabled}
                    className="mt-1 h-8 w-full min-w-0 max-w-full font-mono text-xs md:mt-0"
                    maxLength={255}
                    aria-label={t(
                      { en: "Public model ID for {model}", "zh-CN": "{model} 外部模型 ID", ja: "{model} の公開モデル ID", ko: "{model} 공개 모델 ID" },
                      { model: model.display_name },
                    )}
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

export function apiProfilesFor(
  providerId: string,
  operationId: string,
  t?: Translate,
) {
  if (providerId === "openai-codex")
    return [{ id: "openai-images-v1", label: "OpenAI Images API" }];
  if (providerId === "grok-cli" && operationId === "images.generations")
    return [{ id: "xai-images-v1", label: "xAI Images API" }];
  if (providerId === "grok-cli" && operationId === "videos.generations")
    return [{ id: "xai-videos-v1", label: "xAI Video API" }];
  if (providerId === "dreamina-cli" && operationId === "images.generations") {
    return [
      {
        id: "volcengine-ark-images-v3",
        label: localize(t, {
          en: "Volcengine Ark Images API",
          "zh-CN": "火山方舟图片 API",
          ja: "Volcengine Ark Images API",
          ko: "Volcengine Ark Images API",
        }),
      },
      {
        id: "dreamina-cli-images-v1",
        label: localize(t, {
          en: "Dreamina CLI Images API",
          "zh-CN": "即梦 CLI 图片 API",
          ja: "Dreamina CLI Images API",
          ko: "Dreamina CLI Images API",
        }),
      },
    ];
  }
  if (providerId === "dreamina-cli" && operationId === "videos.generations") {
    return [
      {
        id: "volcengine-ark-content-generation-v3",
        label: localize(t, {
          en: "Volcengine Ark Content API",
          "zh-CN": "火山方舟内容生成 API",
          ja: "Volcengine Ark Content API",
          ko: "Volcengine Ark Content API",
        }),
      },
      {
        id: "dreamina-cli-videos-v1",
        label: localize(t, {
          en: "Dreamina CLI Video API",
          "zh-CN": "即梦 CLI 视频 API",
          ja: "Dreamina CLI Video API",
          ko: "Dreamina CLI Video API",
        }),
      },
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
  t?: Translate,
) {
  const publicModelId = mapping.publicModelId.trim();
  if (!/^[A-Za-z0-9_.:-]{1,255}$/.test(publicModelId)) {
    return localize(t, {
      en: "Use only letters, numbers, and _ . : -",
      "zh-CN": "仅支持字母、数字及 _ . : -",
      ja: "英数字と _ . : - のみ使用できます",
      ko: "문자, 숫자 및 _ . : - 만 사용할 수 있습니다",
    });
  }
  if (
    mappings.filter(
      (candidate) =>
        candidate.apiProfile === mapping.apiProfile &&
        candidate.publicModelId.trim() === publicModelId,
    ).length > 1
  ) {
    return localize(t, {
      en: "Model IDs must be unique within an API protocol",
      "zh-CN": "同一 API 协议下模型 ID 不能重复",
      ja: "同じ API プロトコル内でモデル ID を重複させることはできません",
      ko: "동일한 API 프로토콜에서 모델 ID는 중복될 수 없습니다",
    });
  }
  return null;
}

function localize(
  t: Translate | undefined,
  text: Parameters<Translate>[0],
) {
  return t ? t(text) : text.en;
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
