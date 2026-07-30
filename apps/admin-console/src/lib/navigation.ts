import {
  Activity,
  Banknote,
  Boxes,
  CircleDollarSign,
  Files,
  KeyRound,
  ListChecks,
  ListTodo,
  Images,
  ScrollText,
  ServerCog,
  TerminalSquare,
  UsersRound,
  Video,
  type LucideIcon,
} from "lucide-react";
import type { LocalizedText } from "@/i18n/config";

export type NavigationItem = {
  title: LocalizedText;
  href: string;
  icon: LucideIcon;
  capability: string;
  platformOnly?: boolean;
};

export type NavigationGroup = {
  label: LocalizedText;
  items: NavigationItem[];
};

const navigationGroups: NavigationGroup[] = [
  {
    label: {
      en: "Create",
      "zh-CN": "创作",
      ja: "作成",
      ko: "만들기",
    },
    items: [
      {
        title: {
          en: "Images",
          "zh-CN": "图片",
          ja: "画像",
          ko: "이미지",
        },
        href: "/images",
        icon: Images,
        capability: "workspace:write",
      },
      {
        title: {
          en: "Videos",
          "zh-CN": "视频",
          ja: "動画",
          ko: "동영상",
        },
        href: "/videos",
        icon: Video,
        capability: "workspace:write",
      },
    ],
  },
  {
    label: {
      en: "Workspace",
      "zh-CN": "工作区",
      ja: "ワークスペース",
      ko: "워크스페이스",
    },
    items: [
      {
        title: {
          en: "Overview",
          "zh-CN": "运营总览",
          ja: "概要",
          ko: "개요",
        },
        href: "/overview",
        icon: Activity,
        capability: "console:read",
      },
      {
        title: {
          en: "API activity",
          "zh-CN": "API 调用记录",
          ja: "API アクティビティ",
          ko: "API 활동",
        },
        href: "/activity",
        icon: ListChecks,
        capability: "console:read",
      },
      {
        title: {
          en: "Usage",
          "zh-CN": "用量",
          ja: "使用量",
          ko: "사용량",
        },
        href: "/billing",
        icon: CircleDollarSign,
        capability: "billing:read",
      },
    ],
  },
  {
    label: {
      en: "Providers",
      "zh-CN": "供应能力",
      ja: "プロバイダー",
      ko: "공급자",
    },
    items: [
      {
        title: {
          en: "Models",
          "zh-CN": "模型与能力",
          ja: "モデル",
          ko: "모델",
        },
        href: "/providers",
        icon: Boxes,
        capability: "console:read",
      },
    ],
  },
  {
    label: {
      en: "Developers",
      "zh-CN": "开发者",
      ja: "開発者",
      ko: "개발자",
    },
    items: [
      {
        title: {
          en: "API keys",
          "zh-CN": "API Keys",
          ja: "API キー",
          ko: "API 키",
        },
        href: "/keys",
        icon: KeyRound,
        capability: "projects:read",
      },
      {
        title: {
          en: "Batches",
          "zh-CN": "批处理",
          ja: "バッチ",
          ko: "배치",
        },
        href: "/batches",
        icon: Files,
        capability: "projects:read",
      },
    ],
  },
  {
    label: {
      en: "Platform",
      "zh-CN": "平台运营",
      ja: "プラットフォーム",
      ko: "플랫폼",
    },
    items: [
      {
        title: {
          en: "Model pricing",
          "zh-CN": "模型定价",
          ja: "モデル料金",
          ko: "모델 요금",
        },
        href: "/pricing",
        icon: Banknote,
        capability: "admin:*",
        platformOnly: true,
      },
      {
        title: {
          en: "Users and access",
          "zh-CN": "用户与权限",
          ja: "ユーザーと権限",
          ko: "사용자 및 권한",
        },
        href: "/users",
        icon: UsersRound,
        capability: "users:manage",
        platformOnly: true,
      },
      {
        title: {
          en: "CLI accounts",
          "zh-CN": "CLI 账号与额度",
          ja: "CLI アカウント",
          ko: "CLI 계정",
        },
        href: "/provider-accounts",
        icon: TerminalSquare,
        capability: "providers:manage",
        platformOnly: true,
      },
      {
        title: {
          en: "Task queue",
          "zh-CN": "任务队列",
          ja: "タスクキュー",
          ko: "작업 대기열",
        },
        href: "/scheduling",
        icon: ListTodo,
        capability: "scheduler:read",
        platformOnly: true,
      },
      {
        title: {
          en: "Audit logs",
          "zh-CN": "审计日志",
          ja: "監査ログ",
          ko: "감사 로그",
        },
        href: "/audit-logs",
        icon: ScrollText,
        capability: "admin:*",
        platformOnly: true,
      },
      {
        title: {
          en: "System status",
          "zh-CN": "系统状态",
          ja: "システム状態",
          ko: "시스템 상태",
        },
        href: "/system",
        icon: ServerCog,
        capability: "system:read",
        platformOnly: true,
      },
    ],
  },
];

export function navigationFor(
  roles: readonly string[],
  capabilities: readonly string[],
): NavigationGroup[] {
  const platformOwner = roles.includes("platform_owner");
  return navigationGroups.flatMap((group) => {
    const items = group.items.filter((item) => {
      if (item.platformOnly && !platformOwner) return false;
      return platformOwner || hasCapability(capabilities, item.capability);
    });
    return items.length > 0 ? [{ ...group, items }] : [];
  });
}

export const pageTitles: Record<string, LocalizedText> = {
  ...Object.fromEntries(
    navigationGroups.flatMap((group) =>
      group.items.map((item) => [item.href, item.title]),
    ),
  ),
  "/projects": {
    en: "Projects",
    "zh-CN": "项目",
    ja: "プロジェクト",
    ko: "프로젝트",
  },
};

export function requiresProjectWorkspace(pathname: string) {
  return ["/images", "/videos", "/batches"].some(
    (route) => pathname === route || pathname.startsWith(`${route}/`),
  );
}

function hasCapability(capabilities: readonly string[], required: string) {
  if (capabilities.includes(required) || capabilities.includes("admin:*")) return true;
  const namespace = required.split(":", 1)[0];
  return capabilities.includes(`${namespace}:*`);
}
