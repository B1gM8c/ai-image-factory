"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { LoaderCircle, Plus, RefreshCw, Search, Settings2 } from "lucide-react";
import { useConsoleSession } from "@/components/auth/console-session-provider";
import {
  ProjectCreateDialog,
  type ProjectSummary,
} from "@/components/projects/project-create-dialog";
import { ProjectLimitsSheet } from "@/components/projects/project-limits-sheet";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { consoleFetch } from "@/lib/auth/client";

type ProjectListResponse = {
  object: "list";
  data: ProjectSummary[];
  has_more: boolean;
  last_id: string | null;
};

export function ProjectsManager() {
  const {
    activeWorkspace,
    organizations,
    projects: memberships,
    reload,
    user,
  } = useConsoleSession();
  const [projects, setProjects] = useState<ProjectSummary[]>([]);
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [selectedProject, setSelectedProject] = useState<ProjectSummary | null>(null);

  const loadProjects = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await consoleFetch(
        "/api/gateway/v1/organization/projects?limit=100",
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      const payload = (await response.json()) as ProjectListResponse;
      setProjects(payload.data);
    } catch (reason) {
      setProjects([]);
      setError(reason instanceof Error ? reason.message : "项目加载失败");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadProjects();
  }, [loadProjects]);

  const visibleProjects = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!normalized) return projects;
    return projects.filter(
      (project) =>
        project.name.toLowerCase().includes(normalized) ||
        project.id.toLowerCase().includes(normalized),
    );
  }, [projects, query]);

  async function handleCreated() {
    await Promise.all([loadProjects(), reload()]);
  }

  const creationOrganizationId =
    activeWorkspace?.kind === "project"
      ? activeWorkspace.organizationId
      : activeWorkspace?.kind === "organization"
        ? activeWorkspace.id
        : null;
  const creationOrganization = organizations.find(
    (organization) => organization.id === creationOrganizationId,
  );
  const canCreateProject =
    Boolean(creationOrganizationId) &&
    (Boolean(user?.roles.includes("platform_owner")) ||
      creationOrganization?.role === "owner");
  const canManageSelected = selectedProject
    ? Boolean(
        user?.roles.includes("platform_owner") ||
          memberships.some(
            (membership) =>
              membership.id === selectedProject.id && membership.role === "owner",
          ) ||
          memberships.some(
            (membership) =>
              membership.id === selectedProject.id &&
              organizations.some(
                (organization) =>
                  organization.id === membership.organization_id &&
                  organization.role === "owner",
              ),
          ),
      )
    : false;

  return (
    <>
      <div className="space-y-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
          <div className="relative min-w-0 flex-1">
            <Search
              className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
              aria-hidden="true"
            />
            <Input
              className="pl-9"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="按名称或项目 ID 搜索"
              aria-label="搜索项目"
            />
          </div>
          <Button
            variant="outline"
            size="icon"
            disabled={loading}
            onClick={() => void loadProjects()}
            aria-label="刷新项目"
            title="刷新项目"
          >
            {loading ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <RefreshCw aria-hidden="true" />
            )}
          </Button>
          <Button
            onClick={() => setCreateOpen(true)}
            disabled={!canCreateProject}
            title={
              activeWorkspace?.kind === "platform"
                ? "请先切换到一个组织或项目工作区"
                : !canCreateProject
                  ? "只有当前组织的所有者可以创建项目"
                  : undefined
            }
          >
            <Plus aria-hidden="true" />
            创建项目
          </Button>
        </div>

        <div className="overflow-x-auto border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>名称</TableHead>
                <TableHead>项目 ID</TableHead>
                <TableHead>状态</TableHead>
                <TableHead className="text-right">创建时间</TableHead>
                <TableHead className="w-12">
                  <span className="sr-only">操作</span>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {visibleProjects.map((project) => (
                <TableRow
                  key={project.id}
                  className="cursor-pointer"
                  onClick={() => setSelectedProject(project)}
                >
                  <TableCell className="font-medium">{project.name}</TableCell>
                  <TableCell className="font-mono text-xs">{project.id}</TableCell>
                  <TableCell>
                    <Badge variant={project.status === "active" ? "default" : "secondary"}>
                      {project.status === "active" ? "启用" : "已归档"}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-right text-muted-foreground">
                    {formatUnix(project.created_at)}
                  </TableCell>
                  <TableCell>
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      onClick={(event) => {
                        event.stopPropagation();
                        setSelectedProject(project);
                      }}
                      aria-label={`打开 ${project.name} 的项目设置`}
                      title="项目设置"
                    >
                      <Settings2 aria-hidden="true" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
              {!loading && visibleProjects.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-32 text-center text-muted-foreground">
                    {error ?? (query.trim() ? "没有匹配的项目" : "暂无项目")}
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
          </Table>
        </div>
      </div>

      <ProjectCreateDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        onCreated={handleCreated}
      />
      <ProjectLimitsSheet
        project={selectedProject}
        canManage={canManageSelected}
        onProjectUpdated={async (updated) => {
          setProjects((current) =>
            current.map((project) =>
              project.id === updated.id ? updated : project,
            ),
          );
          setSelectedProject(updated);
          await reload();
        }}
        onOpenChange={(open) => {
          if (!open) setSelectedProject(null);
        }}
      />
    </>
  );
}

async function responseMessage(response: Response) {
  const body = (await response.json().catch(() => null)) as
    | { error?: string | { message?: string } }
    | null;
  if (typeof body?.error === "string") return body.error;
  if (body?.error && typeof body.error === "object" && body.error.message) {
    return body.error.message;
  }
  return `请求失败 (${response.status})`;
}

function formatUnix(value: number) {
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(value * 1000));
}
