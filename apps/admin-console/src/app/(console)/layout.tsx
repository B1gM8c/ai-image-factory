import { redirect } from "next/navigation";
import type { CSSProperties } from "react";
import { ConsoleSessionProvider } from "@/components/auth/console-session-provider";
import { SessionKeeper } from "@/components/auth/session-keeper";
import { AppSidebar } from "@/components/shell/app-sidebar";
import { ConsoleMain } from "@/components/shell/console-main";
import { SiteHeader } from "@/components/shell/site-header";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import { hasConsoleSession } from "@/lib/auth/session";

export default async function ConsoleLayout({ children }: { children: React.ReactNode }) {
  if (!(await hasConsoleSession())) {
    redirect("/login");
  }

  return (
    <SidebarProvider
      style={
        {
          "--sidebar-width": "calc(var(--spacing) * 72)",
          "--header-height": "calc(var(--spacing) * 12)",
        } as CSSProperties
      }
    >
      <ConsoleSessionProvider>
        <SessionKeeper />
        <AppSidebar />
        <SidebarInset className="min-w-0">
          <SiteHeader />
          <ConsoleMain>{children}</ConsoleMain>
        </SidebarInset>
      </ConsoleSessionProvider>
    </SidebarProvider>
  );
}
