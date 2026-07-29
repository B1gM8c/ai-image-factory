"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";
import { Check, ChevronsUpDown, Plus, Settings } from "lucide-react";
import { useConsoleSession } from "@/components/auth/console-session-provider";
import { ProjectCreateDialog } from "@/components/projects/project-create-dialog";
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
  useSidebar,
} from "@/components/ui/sidebar";
import { requiresProjectWorkspace } from "@/lib/navigation";

export function WorkspaceSwitcher() {
  const pathname = usePathname();
  const [createOpen, setCreateOpen] = useState(false);
  const { setOpenMobile } = useSidebar();
  const {
    activeWorkspace,
    loading,
    reload,
    selectWorkspace,
    workspaces,
  } = useConsoleSession();
  const visibleWorkspaces = requiresProjectWorkspace(pathname)
    ? workspaces.filter((workspace) => workspace.kind === "project")
    : workspaces;

  return (
    <>
      <SidebarMenu>
        <SidebarMenuItem>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <SidebarMenuButton
                className="h-9 min-w-0 px-2 data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
                disabled={loading || !activeWorkspace}
                aria-label="切换项目"
              >
                <span className="min-w-0 flex-1 truncate text-left text-sm font-medium">
                  {activeWorkspace?.name ?? "正在载入项目"}
                </span>
                <ChevronsUpDown
                  className="ml-auto size-3.5 shrink-0 text-muted-foreground"
                  aria-hidden="true"
                />
              </SidebarMenuButton>
            </DropdownMenuTrigger>
            <DropdownMenuContent
              className="w-[var(--radix-dropdown-menu-trigger-width)] min-w-56 max-w-[calc(100vw-2rem)]"
              align="start"
              side="bottom"
              sideOffset={4}
            >
              <DropdownMenuLabel className="text-xs font-normal text-muted-foreground">
                项目
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              {visibleWorkspaces.map((workspace) => (
                <DropdownMenuItem
                  key={workspace.key}
                  className="min-w-0 gap-2"
                  onSelect={() => selectWorkspace(workspace.key)}
                >
                  <span className="min-w-0 flex-1">
                    <span className="block truncate">{workspace.name}</span>
                    <span className="block truncate text-xs text-muted-foreground">
                      {workspace.detail}
                    </span>
                  </span>
                  {workspace.key === activeWorkspace?.key ? (
                    <Check className="size-4 shrink-0" aria-label="当前工作区" />
                  ) : null}
                </DropdownMenuItem>
              ))}
              <DropdownMenuSeparator />
              <DropdownMenuItem
                onSelect={() => {
                  setOpenMobile(false);
                  setCreateOpen(true);
                }}
              >
                <Plus aria-hidden="true" />
                创建项目
              </DropdownMenuItem>
              <DropdownMenuItem asChild>
                <Link href="/projects" onClick={() => setOpenMobile(false)}>
                  <Settings aria-hidden="true" />
                  管理项目
                </Link>
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </SidebarMenuItem>
      </SidebarMenu>
      <ProjectCreateDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        onCreated={async () => {
          await reload();
        }}
      />
    </>
  );
}
