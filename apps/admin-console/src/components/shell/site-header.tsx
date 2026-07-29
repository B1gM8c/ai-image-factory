"use client";

import { usePathname } from "next/navigation";
import { pageTitles } from "@/lib/navigation";
import { NotificationMenu } from "@/components/shell/notification-menu";
import { Separator } from "@/components/ui/separator";
import { SidebarTrigger } from "@/components/ui/sidebar";

export function SiteHeader() {
  const pathname = usePathname();
  const title = pageTitles[pathname] ?? "运营控制台";

  return (
    <header className="flex h-[var(--header-height)] shrink-0 items-center gap-2 border-b transition-[width,height] ease-linear">
      <div className="flex w-full items-center gap-1 px-4 lg:gap-2 lg:px-6">
        <SidebarTrigger className="-ml-1" />
        <Separator orientation="vertical" className="mx-2 h-4" />
        <h1 className="min-w-0 truncate text-base font-medium">{title}</h1>
        <div className="ml-auto">
          <NotificationMenu />
        </div>
      </div>
    </header>
  );
}
