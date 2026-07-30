"use client";

import { usePathname } from "next/navigation";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import {
  getConsoleSession,
  refreshConsoleSession,
} from "@/lib/auth/client";
import type {
  AuthOrganization,
  AuthProject,
  AuthUser,
  ConsoleSessionStatus,
} from "@/lib/auth/types";
import { requiresProjectWorkspace } from "@/lib/navigation";
import { useI18n } from "@/i18n/locale-provider";

const WORKSPACE_STORAGE_KEY = "aif-console-workspace";

export type ConsoleWorkspace = {
  key: string;
  kind: "platform" | "organization" | "project";
  id: string | null;
  organizationId: string | null;
  name: string;
  detail: string;
  role: string;
};

type ConsoleSessionContextValue = {
  session: ConsoleSessionStatus | null;
  user: AuthUser | null;
  organizations: AuthOrganization[];
  projects: AuthProject[];
  capabilities: string[];
  workspaces: ConsoleWorkspace[];
  activeWorkspace: ConsoleWorkspace | null;
  loading: boolean;
  reload: () => Promise<ConsoleSessionStatus | null>;
  selectWorkspace: (key: string) => void;
};

const ConsoleSessionContext = createContext<ConsoleSessionContextValue | null>(null);

export function ConsoleSessionProvider({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const { t } = useI18n();
  const [session, setSession] = useState<ConsoleSessionStatus | null>(null);
  const [selectedWorkspaceKey, setSelectedWorkspaceKey] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const reload = useCallback(async () => {
    setLoading(true);
    let next = await getConsoleSession();
    if (next && !next.authenticated && next.refresh_available) {
      if (await refreshConsoleSession()) next = await getConsoleSession();
    }
    setSession(next);
    setLoading(false);
    return next;
  }, []);

  useEffect(() => {
    void reload();
  }, [reload]);

  const workspaces = useMemo(
    () =>
      buildWorkspaces(session, {
        organization: t({
          en: "Organization",
          "zh-CN": "组织",
          ja: "組織",
          ko: "조직",
        }),
        platform: t({
          en: "All projects",
          "zh-CN": "全平台",
          ja: "すべてのプロジェクト",
          ko: "모든 프로젝트",
        }),
        platformDetail: t({
          en: "Platform operations",
          "zh-CN": "平台运营视图",
          ja: "プラットフォーム運用",
          ko: "플랫폼 운영",
        }),
        project: t({
          en: "Project",
          "zh-CN": "项目",
          ja: "プロジェクト",
          ko: "프로젝트",
        }),
      }),
    [session, t],
  );

  useEffect(() => {
    if (workspaces.length === 0) {
      setSelectedWorkspaceKey(null);
      return;
    }
    const stored = window.localStorage.getItem(WORKSPACE_STORAGE_KEY);
    const preferred = selectedWorkspaceKey ?? stored;
    let next =
      preferred && workspaces.some((workspace) => workspace.key === preferred)
        ? preferred
        : workspaces[0].key;
    if (
      requiresProjectWorkspace(pathname) &&
      workspaces.find((workspace) => workspace.key === next)?.kind !== "project"
    ) {
      next =
        workspaces.find((workspace) => workspace.kind === "project")?.key ??
        next;
    }
    setSelectedWorkspaceKey(next);
    window.localStorage.setItem(WORKSPACE_STORAGE_KEY, next);
  }, [pathname, selectedWorkspaceKey, workspaces]);

  const selectWorkspace = useCallback((key: string) => {
    setSelectedWorkspaceKey(key);
    window.localStorage.setItem(WORKSPACE_STORAGE_KEY, key);
  }, []);

  const activeWorkspace =
    workspaces.find((workspace) => workspace.key === selectedWorkspaceKey) ??
    workspaces[0] ??
    null;

  const value = useMemo<ConsoleSessionContextValue>(
    () => ({
      session,
      user: session?.user ?? null,
      organizations: session?.organizations ?? [],
      projects: session?.projects ?? [],
      capabilities: session?.capabilities ?? [],
      workspaces,
      activeWorkspace,
      loading,
      reload,
      selectWorkspace,
    }),
    [activeWorkspace, loading, reload, session, selectWorkspace, workspaces],
  );

  return (
    <ConsoleSessionContext.Provider value={value}>
      {children}
    </ConsoleSessionContext.Provider>
  );
}

export function useConsoleSession() {
  const context = useContext(ConsoleSessionContext);
  if (!context) {
    throw new Error("useConsoleSession must be used within ConsoleSessionProvider");
  }
  return context;
}

function buildWorkspaces(
  session: ConsoleSessionStatus | null,
  copy: {
    organization: string;
    platform: string;
    platformDetail: string;
    project: string;
  },
): ConsoleWorkspace[] {
  if (!session?.user) return [];

  const workspaces: ConsoleWorkspace[] = [];
  if (session.user.roles.includes("platform_owner")) {
    workspaces.push({
      key: "platform",
      kind: "platform",
      id: null,
      organizationId: null,
      name: copy.platform,
      detail: copy.platformDetail,
      role: "platform_owner",
    });
  }

  const organizations = new Map(
    session.organizations.map((organization) => [organization.id, organization]),
  );

  for (const project of orderedDefaultsFirst(session.projects)) {
    workspaces.push({
      key: `project:${project.id}`,
      kind: "project",
      id: project.id,
      organizationId: project.organization_id,
      name: project.name,
      detail: organizations.get(project.organization_id)?.name ?? copy.project,
      role: project.role,
    });
  }

  for (const organization of orderedDefaultsFirst(session.organizations)) {
    if (session.projects.some((project) => project.organization_id === organization.id)) continue;
    workspaces.push({
      key: `organization:${organization.id}`,
      kind: "organization",
      id: organization.id,
      organizationId: organization.id,
      name: organization.name,
      detail: copy.organization,
      role: organization.role,
    });
  }

  return workspaces;
}

function orderedDefaultsFirst<T extends { is_default: boolean }>(items: T[]) {
  return [...items].sort((left, right) => Number(right.is_default) - Number(left.is_default));
}
