"use client";

import Link from "next/link";
import {
  BookOpen,
  Building2,
  ChevronRight,
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
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { consoleFetch } from "@/lib/auth/client";
import { cn } from "@/lib/utils";

const themeOptions = [
  { value: "system", label: "跟随系统", icon: Monitor },
  { value: "light", label: "浅色", icon: Sun },
  { value: "dark", label: "深色", icon: Moon },
] as const;

export function NavUser() {
  const router = useRouter();
  const { setTheme, theme } = useTheme();
  const { loading, organizations, user } = useConsoleSession();
  const organization =
    organizations.find((candidate) => candidate.is_default) ??
    organizations[0] ??
    null;
  const accountName = organization?.name ?? "AI Image Factory";
  const accountKind = organization ? "组织" : "平台";
  const platformOwner = user?.roles.includes("platform_owner") ?? false;

  async function logout() {
    const response = await consoleFetch(
      "/api/session",
      { method: "DELETE" },
      { retryUnauthorized: false },
    );
    if (!response.ok) {
      toast.error("服务端会话撤销失败，请重试");
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
              aria-label="打开账户菜单"
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
                {user?.email ?? "当前用户"}
              </span>
            </DropdownMenuLabel>
            <div className="px-3 pb-2">
              <div
                className="inline-flex items-center gap-0.5 rounded-lg bg-muted/70 p-0.5 dark:bg-[#171717]"
                role="radiogroup"
                aria-label="颜色主题"
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
                      aria-label={option.label}
                      title={option.label}
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
                    组织设置
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
                开发文档
              </a>
            </DropdownMenuItem>
            {platformOwner ? (
              <DropdownMenuItem asChild className="h-9 rounded-lg px-2.5">
                <Link href="/system">
                  <ServerCog aria-hidden="true" />
                  系统状态
                </Link>
              </DropdownMenuItem>
            ) : null}
            <DropdownMenuSeparator className="mx-0 my-1" />
            <DropdownMenuItem className="h-9 rounded-lg px-2.5" onSelect={logout}>
              <LogOut aria-hidden="true" />
              退出登录
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
