"use client";

import { usePathname } from "next/navigation";
import Link from "next/link";
import { useConsoleSession } from "@/components/auth/console-session-provider";
import { NavUser } from "@/components/shell/nav-user";
import { WorkspaceSwitcher } from "@/components/shell/workspace-switcher";
import { navigationFor } from "@/lib/navigation";
import { useI18n } from "@/i18n/locale-provider";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "@/components/ui/sidebar";

export function AppSidebar() {
  const pathname = usePathname();
  const { setOpenMobile } = useSidebar();
  const { capabilities, user } = useConsoleSession();
  const { t } = useI18n();
  const navigationGroups = navigationFor(user?.roles ?? [], capabilities);

  return (
    <Sidebar collapsible="offcanvas" variant="inset">
      <SidebarHeader>
        <WorkspaceSwitcher />
      </SidebarHeader>

      <SidebarContent className="py-2">
        {navigationGroups.map((group) => (
          <SidebarGroup key={group.label.en}>
            <SidebarGroupLabel>{t(group.label)}</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {group.items.map((item) => {
                  const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
                  return (
                    <SidebarMenuItem key={item.href}>
                      <SidebarMenuButton
                        asChild
                        isActive={active}
                        tooltip={t(item.title)}
                      >
                        <Link href={item.href} onClick={() => setOpenMobile(false)}>
                          <item.icon aria-hidden="true" />
                          <span>{t(item.title)}</span>
                        </Link>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  );
                })}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        ))}
      </SidebarContent>

      <SidebarFooter>
        <NavUser />
      </SidebarFooter>
    </Sidebar>
  );
}
