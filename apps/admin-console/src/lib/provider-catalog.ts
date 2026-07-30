import type { LocalizedText } from "@/i18n/config";

export type ProviderModel = {
  id: string;
  name: LocalizedText;
  media: "image" | "video";
  capabilities: readonly LocalizedText[];
  status: LocalizedText;
};

export type ProviderCatalogItem = {
  id: string;
  name: LocalizedText;
  cliName: LocalizedText;
  description: LocalizedText;
  apiCompatibility: LocalizedText;
  integrationStatus: "ready" | "partial" | "planned";
  integrationLabel: LocalizedText;
  integrationDetail: LocalizedText;
  models: readonly ProviderModel[];
};

export const providerCatalog: readonly ProviderCatalogItem[] = [
  {
    id: "openai-codex",
    name: {
      en: "Codex",
      "zh-CN": "Codex",
      ja: "Codex",
      ko: "Codex",
    },
    cliName: {
      en: "Codex CLI",
      "zh-CN": "Codex CLI",
      ja: "Codex CLI",
      ko: "Codex CLI",
    },
    description: {
      en: "Provides OpenAI Images-compatible image generation and editing through Codex CLI.",
      "zh-CN": "通过 Codex CLI 提供 OpenAI Images 兼容的图片生成与编辑能力。",
      ja: "Codex CLI を通じて OpenAI Images 互換の画像生成と編集を提供します。",
      ko: "Codex CLI를 통해 OpenAI Images 호환 이미지 생성 및 편집 기능을 제공합니다.",
    },
    apiCompatibility: {
      en: "OpenAI Images API",
      "zh-CN": "OpenAI Images API",
      ja: "OpenAI Images API",
      ko: "OpenAI Images API",
    },
    integrationStatus: "ready",
    integrationLabel: {
      en: "Adapter connected",
      "zh-CN": "适配器已接入",
      ja: "アダプター接続済み",
      ko: "어댑터 연결됨",
    },
    integrationDetail: {
      en: "Immediate availability depends on the Codex account and execution profile status.",
      "zh-CN": "是否可以立即调用，取决于 Codex 账号与执行 Profile 状态。",
      ja: "すぐに呼び出せるかどうかは、Codex アカウントと実行プロファイルの状態によって決まります。",
      ko: "즉시 호출 가능 여부는 Codex 계정 및 실행 프로필 상태에 따라 달라집니다.",
    },
    models: [
      {
        id: "gpt-image-2",
        name: {
          en: "GPT Image 2",
          "zh-CN": "GPT Image 2",
          ja: "GPT Image 2",
          ko: "GPT Image 2",
        },
        media: "image",
        capabilities: [
          {
            en: "Image generation",
            "zh-CN": "图片生成",
            ja: "画像生成",
            ko: "이미지 생성",
          },
          {
            en: "Image editing",
            "zh-CN": "图片编辑",
            ja: "画像編集",
            ko: "이미지 편집",
          },
        ],
        status: {
          en: "Connected",
          "zh-CN": "已接入",
          ja: "接続済み",
          ko: "연결됨",
        },
      },
    ],
  },
  {
    id: "grok-cli",
    name: {
      en: "Grok",
      "zh-CN": "Grok",
      ja: "Grok",
      ko: "Grok",
    },
    cliName: {
      en: "Grok CLI",
      "zh-CN": "Grok CLI",
      ja: "Grok CLI",
      ko: "Grok CLI",
    },
    description: {
      en: "Provides xAI-compatible image and video generation through Grok CLI.",
      "zh-CN": "通过 Grok CLI 提供 xAI 兼容的图片与视频生成能力。",
      ja: "Grok CLI を通じて xAI 互換の画像・動画生成を提供します。",
      ko: "Grok CLI를 통해 xAI 호환 이미지 및 동영상 생성 기능을 제공합니다.",
    },
    apiCompatibility: {
      en: "xAI Images / Video API",
      "zh-CN": "xAI Images / Video API",
      ja: "xAI Images / Video API",
      ko: "xAI Images / Video API",
    },
    integrationStatus: "partial",
    integrationLabel: {
      en: "Images enabled",
      "zh-CN": "图片已启用",
      ja: "画像を有効化済み",
      ko: "이미지 활성화됨",
    },
    integrationDetail: {
      en: "The video adapter is connected, but its API is disabled by default until the matching execution profile is enabled.",
      "zh-CN": "视频适配器已接入，但 API 默认关闭，需要启用对应执行 Profile。",
      ja: "動画アダプターは接続済みですが、API は既定で無効です。対応する実行プロファイルを有効にしてください。",
      ko: "동영상 어댑터는 연결되었지만 API는 기본적으로 비활성화되어 있습니다. 해당 실행 프로필을 활성화해야 합니다.",
    },
    models: [
      {
        id: "grok-imagine-image",
        name: {
          en: "Grok Imagine Image",
          "zh-CN": "Grok Imagine Image",
          ja: "Grok Imagine Image",
          ko: "Grok Imagine Image",
        },
        media: "image",
        capabilities: [
          {
            en: "Image generation",
            "zh-CN": "图片生成",
            ja: "画像生成",
            ko: "이미지 생성",
          },
          {
            en: "Image editing",
            "zh-CN": "图片编辑",
            ja: "画像編集",
            ko: "이미지 편집",
          },
        ],
        status: {
          en: "Connected",
          "zh-CN": "已接入",
          ja: "接続済み",
          ko: "연결됨",
        },
      },
      {
        id: "grok-imagine-video",
        name: {
          en: "Grok Imagine Video",
          "zh-CN": "Grok Imagine Video",
          ja: "Grok Imagine Video",
          ko: "Grok Imagine Video",
        },
        media: "video",
        capabilities: [
          {
            en: "Video generation",
            "zh-CN": "视频生成",
            ja: "動画生成",
            ko: "동영상 생성",
          },
        ],
        status: {
          en: "Disabled by default",
          "zh-CN": "默认关闭",
          ja: "既定で無効",
          ko: "기본적으로 비활성화",
        },
      },
    ],
  },
  {
    id: "dreamina-cli",
    name: {
      en: "Dreamina",
      "zh-CN": "即梦",
      ja: "Dreamina",
      ko: "Dreamina",
    },
    cliName: {
      en: "Dreamina CLI",
      "zh-CN": "即梦 CLI",
      ja: "Dreamina CLI",
      ko: "Dreamina CLI",
    },
    description: {
      en: "Preserves provider-aligned request contracts for Dreamina images and Seedance videos.",
      "zh-CN": "为即梦图片与 Seedance 视频保留官方口径的调用契约。",
      ja: "Dreamina 画像と Seedance 動画向けに、公式仕様に沿った呼び出し契約を維持します。",
      ko: "Dreamina 이미지와 Seedance 동영상에 대해 공식 사양에 맞는 호출 계약을 유지합니다.",
    },
    apiCompatibility: {
      en: "Dreamina Images / Seedance Video API",
      "zh-CN": "即梦图片 / Seedance 视频 API",
      ja: "Dreamina Images / Seedance Video API",
      ko: "Dreamina Images / Seedance Video API",
    },
    integrationStatus: "planned",
    integrationLabel: {
      en: "Awaiting production enablement",
      "zh-CN": "等待生产启用",
      ja: "本番環境での有効化待ち",
      ko: "프로덕션 활성화 대기 중",
    },
    integrationDetail: {
      en: "The request contract is connected and the execution profile pins the model, but no production account is active yet.",
      "zh-CN": "调用契约已接入，模型由执行 Profile 固定，目前尚未激活生产账号。",
      ja: "呼び出し契約は接続済みで、モデルは実行プロファイルによって固定されていますが、本番アカウントはまだ有効化されていません。",
      ko: "호출 계약은 연결되었고 모델은 실행 프로필에 고정되어 있지만, 아직 활성화된 프로덕션 계정이 없습니다.",
    },
    models: [
      {
        id: "execution-profile-defined",
        name: {
          en: "Dreamina Image",
          "zh-CN": "即梦图片",
          ja: "Dreamina 画像",
          ko: "Dreamina 이미지",
        },
        media: "image",
        capabilities: [
          {
            en: "Image generation",
            "zh-CN": "图片生成",
            ja: "画像生成",
            ko: "이미지 생성",
          },
        ],
        status: {
          en: "Awaiting enablement",
          "zh-CN": "等待启用",
          ja: "有効化待ち",
          ko: "활성화 대기 중",
        },
      },
      {
        id: "execution-profile-defined",
        name: {
          en: "Seedance Video",
          "zh-CN": "Seedance 视频",
          ja: "Seedance 動画",
          ko: "Seedance 동영상",
        },
        media: "video",
        capabilities: [
          {
            en: "Video generation",
            "zh-CN": "视频生成",
            ja: "動画生成",
            ko: "동영상 생성",
          },
        ],
        status: {
          en: "Awaiting enablement",
          "zh-CN": "等待启用",
          ja: "有効化待ち",
          ko: "활성화 대기 중",
        },
      },
    ],
  },
];
