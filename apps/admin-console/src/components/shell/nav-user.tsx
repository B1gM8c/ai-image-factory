"use client";

import Link from "next/link";
import {
  BookOpen,
  Building2,
  ChevronRight,
  Languages,
  LogOut,
  Monitor,
  Moon,
  ServerCog,
  Sun,
} from "lucide-react";
import { useTheme } from "next-themes";
import { useRouter } from "next/navigation";
import { toast } from "sonner";
import { useConsoleSession } from "@/components/auth/console-session-provider";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { consoleFetch } from "@/lib/auth/client";
import {
  isLocale,
  localeLabels,
  supportedLocales,
} from "@/i18n/config";
import { useI18n } from "@/i18n/locale-provider";
import { cn } from "@/lib/utils";

const themeOptions = [
  {
    value: "system",
    label: {
      en: "System",
      "zh-CN": "跟随系统",
      ja: "システム",
      ko: "시스템",
    },
    icon: Monitor,
  },
  {
    value: "light",
    label: {
      en: "Light",
      "zh-CN": "浅色",
      ja: "ライト",
      ko: "라이트",
    },
    icon: Sun,
  },
  {
    value: "dark",
    label: {
      en: "Dark",
      "zh-CN": "深色",
      ja: "ダーク",
      ko: "다크",
    },
    icon: Moon,
  },
] as const;

export function NavUser() {
  const router = useRouter();
  const { setTheme, theme } = useTheme();
  const { locale, setLocale, t } = useI18n();
  const { loading, organizations, user } = useConsoleSession();
  const organization =
    organizations.find((candidate) => candidate.is_default) ??
    organizations[0] ??
    null;
  const accountName = organization?.name ?? "AI Image Factory";
  const accountKind = organization
    ? t({
        en: "Organization",
        "zh-CN": "组织",
        ja: "組織",
        ko: "조직",
      })
    : t({
        en: "Platform",
        "zh-CN": "平台",
        ja: "プラットフォーム",
        ko: "플랫폼",
      });
  const platformOwner = user?.roles.includes("platform_owner") ?? false;

  async function logout() {
    const response = await consoleFetch(
      "/api/session",
      { method: "DELETE" },
      { retryUnauthorized: false },
    );
    if (!response.ok) {
      toast.error(
        t({
          en: "Could not revoke the server session. Please try again.",
          "zh-CN": "服务端会话撤销失败，请重试",
          ja: "サーバーセッションを取り消せませんでした。もう一度お試しください。",
          ko: "서버 세션을 해지하지 못했습니다. 다시 시도해 주세요.",
        }),
      );
      return;
    }
    router.replace("/login");
    router.refresh();
  }

  return (
    <SidebarMenu>
      <SidebarMenuItem>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <SidebarMenuButton
              className="h-12 min-w-0 px-2 hover:bg-sidebar-accent data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
              disabled={loading || !user}
              aria-label={t({
                en: "Open account menu",
                "zh-CN": "打开账户菜单",
                ja: "アカウントメニューを開く",
                ko: "계정 메뉴 열기",
              })}
            >
              <span className="flex size-7 shrink-0 items-center justify-center rounded-full bg-primary text-[10px] font-semibold text-primary-foreground">
                {initials(accountName)}
              </span>
              <span className="min-w-0 flex-1 text-left">
                <span className="block truncate text-sm font-medium">{accountName}</span>
                <span className="block truncate text-xs text-muted-foreground">
                  {accountKind}
                </span>
              </span>
            </SidebarMenuButton>
          </DropdownMenuTrigger>
          <DropdownMenuContent
            className="w-[250px] max-w-[calc(100vw-1.5rem)] rounded-[12px] border-0 p-1 shadow-[0_10px_15px_-3px_rgb(0_0_0/0.1),0_4px_6px_-4px_rgb(0_0_0/0.1)] ring-1 ring-foreground/10 dark:bg-[#303030]"
            side="top"
            align="start"
            sideOffset={6}
          >
            <DropdownMenuLabel className="min-w-0 px-3 pb-1 pt-2 font-normal">
              <span className="block truncate text-xs text-muted-foreground">
                {user?.email ??
                  t({
                    en: "Current user",
                    "zh-CN": "当前用户",
                    ja: "現在のユーザー",
                    ko: "현재 사용자",
                  })}
              </span>
            </DropdownMenuLabel>
            <div className="px-3 pb-2">
              <div
                className="inline-flex items-center gap-0.5 rounded-lg bg-muted/70 p-0.5 dark:bg-[#171717]"
                role="radiogroup"
                aria-label={t({
                  en: "Color theme",
                  "zh-CN": "颜色主题",
                  ja: "カラーテーマ",
                  ko: "색상 테마",
                })}
              >
                {themeOptions.map((option) => {
                  const Icon = option.icon;
                  const selected = (theme ?? "system") === option.value;
                  return (
                    <button
                      key={option.value}
                      type="button"
                      role="radio"
                      aria-checked={selected}
                      aria-label={t(option.label)}
                      title={t(option.label)}
                      onClick={(event) => {
                        event.preventDefault();
                        event.stopPropagation();
                        setTheme(option.value);
                      }}
                      className={cn(
                        "inline-flex h-7 w-[30px] items-center justify-center rounded-md text-muted-foreground outline-none transition-colors hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring",
                        selected &&
                          "bg-background text-foreground shadow-sm dark:bg-[#303030]",
                      )}
                    >
                      <Icon className="size-3.5" aria-hidden="true" />
                    </button>
                  );
                })}
              </div>
            </div>
            <DropdownMenuSub>
              <DropdownMenuSubTrigger className="h-9 rounded-lg px-2.5">
                <Languages aria-hidden="true" />
                {t({
                  en: "Language",
                  "zh-CN": "语言",
                  ja: "言語",
                  ko: "언어",
                })}
                <span className="ml-auto mr-1 text-xs text-muted-foreground">
                  {localeLabels[locale]}
                </span>
              </DropdownMenuSubTrigger>
              <DropdownMenuSubContent className="min-w-40 rounded-lg">
                <DropdownMenuRadioGroup
                  value={locale}
                  onValueChange={(value) => {
                    if (isLocale(value)) {
                      setLocale(value);
                    }
                  }}
                >
                  {supportedLocales.map((option) => (
                    <DropdownMenuRadioItem key={option} value={option}>
                      {localeLabels[option]}
                    </DropdownMenuRadioItem>
                  ))}
                </DropdownMenuRadioGroup>
              </DropdownMenuSubContent>
            </DropdownMenuSub>
            <DropdownMenuSeparator className="mx-0 my-1" />
            <DropdownMenuItem asChild className="h-[54px] rounded-lg p-1.5">
              <Link href="/projects">
                <span className="flex size-7 shrink-0 items-center justify-center rounded-full bg-muted text-[10px] font-semibold">
                  {initials(accountName)}
                </span>
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-sm font-medium">
                    {accountName}
                  </span>
                  <span className="block truncate text-xs text-muted-foreground">
                    {accountKind}
                  </span>
                </span>
                <ChevronRight
                  className="size-4 text-muted-foreground"
                  aria-hidden="true"
                />
              </Link>
            </DropdownMenuItem>
            {platformOwner ? (
              <>
                <DropdownMenuSeparator className="mx-0 my-1" />
                <DropdownMenuItem asChild className="h-9 rounded-lg px-2.5">
                  <Link href="/users">
                    <Building2 aria-hidden="true" />
                    {t({
                      en: "Organization settings",
                      "zh-CN": "组织设置",
                      ja: "組織設定",
                      ko: "조직 설정",
                    })}
                  </Link>
                </DropdownMenuItem>
                <DropdownMenuSeparator className="mx-0 my-1" />
              </>
            ) : (
              <DropdownMenuSeparator className="mx-0 my-1" />
            )}
            <DropdownMenuItem asChild className="h-9 rounded-lg px-2.5">
              <a
                href="/api/gateway/openapi.json"
                target="_blank"
                rel="noreferrer"
              >
                <BookOpen aria-hidden="true" />
                {t({
                  en: "Developer documentation",
                  "zh-CN": "开发文档",
                  ja: "開発者ドキュメント",
                  ko: "개발자 문서",
                })}
              </a>
            </DropdownMenuItem>
            {platformOwner ? (
              <DropdownMenuItem asChild className="h-9 rounded-lg px-2.5">
                <Link href="/system">
                  <ServerCog aria-hidden="true" />
                  {t({
                    en: "System status",
                    "zh-CN": "系统状态",
                    ja: "システム状態",
                    ko: "시스템 상태",
                  })}
                </Link>
              </DropdownMenuItem>
            ) : null}
            <DropdownMenuSeparator className="mx-0 my-1" />
            <DropdownMenuItem className="h-9 rounded-lg px-2.5" onSelect={logout}>
              <LogOut aria-hidden="true" />
              {t({
                en: "Log out",
                "zh-CN": "退出登录",
                ja: "ログアウト",
                ko: "로그아웃",
              })}
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </SidebarMenuItem>
    </SidebarMenu>
  );
}

function initials(value: string) {
  const normalized = value.trim();
  if (!normalized) return "U";
  const words = normalized.split(/\s+/).filter(Boolean);
  return words.length > 1
    ? `${words[0][0]}${words[1][0]}`.toUpperCase()
    : normalized.slice(0, 2).toUpperCase();
}
