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

export type NavigationItem = {
  title: string;
  href: string;
  icon: LucideIcon;
  capability: string;
  platformOnly?: boolean;
};

export type NavigationGroup = {
  label: string;
  items: NavigationItem[];
};

const navigationGroups: NavigationGroup[] = [
  {
    label: "创作",
    items: [
      {
        title: "图片",
        href: "/images",
        icon: Images,
        capability: "workspace:write",
      },
      {
        title: "视频",
        href: "/videos",
        icon: Video,
        capability: "workspace:write",
      },
    ],
  },
  {
    label: "工作区",
    items: [
      {
        title: "运营总览",
        href: "/overview",
        icon: Activity,
        capability: "console:read",
      },
      {
        title: "API 调用记录",
        href: "/activity",
        icon: ListChecks,
        capability: "console:read",
      },
      {
        title: "用量",
        href: "/billing",
        icon: CircleDollarSign,
        capability: "billing:read",
      },
    ],
  },
  {
    label: "供应能力",
    items: [
      {
        title: "模型与能力",
        href: "/providers",
        icon: Boxes,
        capability: "console:read",
      },
    ],
  },
  {
    label: "开发者",
    items: [
      {
        title: "API Keys",
        href: "/keys",
        icon: KeyRound,
        capability: "projects:read",
      },
      {
        title: "批处理",
        href: "/batches",
        icon: Files,
        capability: "projects:read",
      },
    ],
  },
  {
    label: "平台运营",
    items: [
      {
        title: "模型定价",
        href: "/pricing",
        icon: Banknote,
        capability: "admin:*",
        platformOnly: true,
      },
      {
        title: "用户与权限",
        href: "/users",
        icon: UsersRound,
        capability: "users:manage",
        platformOnly: true,
      },
      {
        title: "CLI 账号与额度",
        href: "/provider-accounts",
        icon: TerminalSquare,
        capability: "providers:manage",
        platformOnly: true,
      },
      {
        title: "任务队列",
        href: "/scheduling",
        icon: ListTodo,
        capability: "scheduler:read",
        platformOnly: true,
      },
      {
        title: "审计日志",
        href: "/audit-logs",
        icon: ScrollText,
        capability: "admin:*",
        platformOnly: true,
      },
      {
        title: "系统状态",
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

export const pageTitles: Record<string, string> = {
  ...Object.fromEntries(
    navigationGroups.flatMap((group) =>
      group.items.map((item) => [item.href, item.title]),
    ),
  ),
  "/projects": "项目",
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
