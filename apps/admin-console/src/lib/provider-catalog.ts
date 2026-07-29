export type ProviderModel = {
  id: string;
  name: string;
  media: "image" | "video";
  capabilities: readonly string[];
  status: string;
};

export type ProviderCatalogItem = {
  id: string;
  name: string;
  cliName: string;
  description: string;
  apiCompatibility: string;
  integrationStatus: "ready" | "partial" | "planned";
  integrationLabel: string;
  integrationDetail: string;
  models: readonly ProviderModel[];
};

export const providerCatalog: readonly ProviderCatalogItem[] = [
  {
    id: "openai-codex",
    name: "Codex",
    cliName: "Codex CLI",
    description: "通过 Codex CLI 提供 OpenAI Images 兼容的图片生成与编辑能力。",
    apiCompatibility: "OpenAI Images API",
    integrationStatus: "ready",
    integrationLabel: "适配器已接入",
    integrationDetail: "是否可以立即调用，取决于 Codex 账号与执行 Profile 状态。",
    models: [
      {
        id: "gpt-image-2",
        name: "GPT Image 2",
        media: "image",
        capabilities: ["图片生成", "图片编辑"],
        status: "已接入",
      },
    ],
  },
  {
    id: "grok-cli",
    name: "Grok",
    cliName: "Grok CLI",
    description: "通过 Grok CLI 提供 xAI 兼容的图片与视频生成能力。",
    apiCompatibility: "xAI Images / Video API",
    integrationStatus: "partial",
    integrationLabel: "图片已启用",
    integrationDetail: "视频适配器已接入，但 API 默认关闭，需要启用对应执行 Profile。",
    models: [
      {
        id: "grok-imagine-image",
        name: "Grok Imagine Image",
        media: "image",
        capabilities: ["图片生成", "图片编辑"],
        status: "已接入",
      },
      {
        id: "grok-imagine-video",
        name: "Grok Imagine Video",
        media: "video",
        capabilities: ["视频生成"],
        status: "默认关闭",
      },
    ],
  },
  {
    id: "dreamina-cli",
    name: "即梦",
    cliName: "即梦 CLI",
    description: "为即梦图片与 Seedance 视频保留官方口径的调用契约。",
    apiCompatibility: "即梦图片 / Seedance 视频 API",
    integrationStatus: "planned",
    integrationLabel: "等待生产启用",
    integrationDetail: "调用契约已接入，模型由执行 Profile 固定，目前尚未激活生产账号。",
    models: [
      {
        id: "由执行 Profile 指定",
        name: "即梦图片",
        media: "image",
        capabilities: ["图片生成"],
        status: "等待启用",
      },
      {
        id: "由执行 Profile 指定",
        name: "Seedance 视频",
        media: "video",
        capabilities: ["视频生成"],
        status: "等待启用",
      },
    ],
  },
];
