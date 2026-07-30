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
import { useI18n } from "@/i18n/locale-provider";
import { consoleFetch } from "@/lib/auth/client";

type ProjectListResponse = {
  object: "list";
  data: ProjectSummary[];
  has_more: boolean;
  last_id: string | null;
};

export function ProjectsManager() {
  const { locale, t } = useI18n();
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
      if (!response.ok) {
        throw new Error(
          await responseMessage(
            response,
            t({
              en: "Request failed",
              "zh-CN": "请求失败",
              ja: "リクエストに失敗しました",
              ko: "요청에 실패했습니다",
            }),
          ),
        );
      }
      const payload = (await response.json()) as ProjectListResponse;
      setProjects(payload.data);
    } catch (reason) {
      setProjects([]);
      setError(
        reason instanceof Error
          ? reason.message
          : t({
              en: "Failed to load projects",
              "zh-CN": "项目加载失败",
              ja: "プロジェクトを読み込めませんでした",
              ko: "프로젝트를 불러오지 못했습니다",
            }),
      );
    } finally {
      setLoading(false);
    }
  }, [t]);

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
              placeholder={t({
                en: "Search by name or project ID",
                "zh-CN": "按名称或项目 ID 搜索",
                ja: "名前またはプロジェクト ID で検索",
                ko: "이름 또는 프로젝트 ID로 검색",
              })}
              aria-label={t({
                en: "Search projects",
                "zh-CN": "搜索项目",
                ja: "プロジェクトを検索",
                ko: "프로젝트 검색",
              })}
            />
          </div>
          <Button
            variant="outline"
            size="icon"
            disabled={loading}
            onClick={() => void loadProjects()}
            aria-label={t({
              en: "Refresh projects",
              "zh-CN": "刷新项目",
              ja: "プロジェクトを更新",
              ko: "프로젝트 새로고침",
            })}
            title={t({
              en: "Refresh projects",
              "zh-CN": "刷新项目",
              ja: "プロジェクトを更新",
              ko: "프로젝트 새로고침",
            })}
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
                ? t({
                    en: "Switch to an organization or project workspace first",
                    "zh-CN": "请先切换到一个组织或项目工作区",
                    ja: "先に組織またはプロジェクトのワークスペースへ切り替えてください",
                    ko: "먼저 조직 또는 프로젝트 워크스페이스로 전환하세요",
                  })
                : !canCreateProject
                  ? t({
                      en: "Only owners of the current organization can create projects",
                      "zh-CN": "只有当前组织的所有者可以创建项目",
                      ja: "現在の組織の所有者のみがプロジェクトを作成できます",
                      ko: "현재 조직의 소유자만 프로젝트를 만들 수 있습니다",
                    })
                  : undefined
            }
          >
            <Plus aria-hidden="true" />
            {t({
              en: "Create project",
              "zh-CN": "创建项目",
              ja: "プロジェクトを作成",
              ko: "프로젝트 만들기",
            })}
          </Button>
        </div>

        <div className="overflow-x-auto border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{t({ en: "Name", "zh-CN": "名称", ja: "名前", ko: "이름" })}</TableHead>
                <TableHead>{t({ en: "Project ID", "zh-CN": "项目 ID", ja: "プロジェクト ID", ko: "프로젝트 ID" })}</TableHead>
                <TableHead>{t({ en: "Status", "zh-CN": "状态", ja: "ステータス", ko: "상태" })}</TableHead>
                <TableHead className="text-right">
                  {t({ en: "Created", "zh-CN": "创建时间", ja: "作成日時", ko: "생성 시간" })}
                </TableHead>
                <TableHead className="w-12">
                  <span className="sr-only">
                    {t({ en: "Actions", "zh-CN": "操作", ja: "操作", ko: "작업" })}
                  </span>
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
                      {project.status === "active"
                        ? t({ en: "Active", "zh-CN": "启用", ja: "有効", ko: "활성" })
                        : t({ en: "Archived", "zh-CN": "已归档", ja: "アーカイブ済み", ko: "보관됨" })}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-right text-muted-foreground">
                    {formatUnix(project.created_at, locale)}
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
                      aria-label={t(
                        {
                          en: "Open settings for {project}",
                          "zh-CN": "打开 {project} 的项目设置",
                          ja: "{project} のプロジェクト設定を開く",
                          ko: "{project} 프로젝트 설정 열기",
                        },
                        { project: project.name },
                      )}
                      title={t({
                        en: "Project settings",
                        "zh-CN": "项目设置",
                        ja: "プロジェクト設定",
                        ko: "프로젝트 설정",
                      })}
                    >
                      <Settings2 aria-hidden="true" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
              {!loading && visibleProjects.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-32 text-center text-muted-foreground">
                    {error ??
                      (query.trim()
                        ? t({
                            en: "No matching projects",
                            "zh-CN": "没有匹配的项目",
                            ja: "一致するプロジェクトはありません",
                            ko: "일치하는 프로젝트가 없습니다",
                          })
                        : t({
                            en: "No projects yet",
                            "zh-CN": "暂无项目",
                            ja: "プロジェクトはまだありません",
                            ko: "아직 프로젝트가 없습니다",
                          }))}
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

async function responseMessage(response: Response, fallback: string) {
  const body = (await response.json().catch(() => null)) as
    | { error?: string | { message?: string } }
    | null;
  if (typeof body?.error === "string") return body.error;
  if (body?.error && typeof body.error === "object" && body.error.message) {
    return body.error.message;
  }
  return `${fallback} (${response.status})`;
}

function formatUnix(value: number, locale: string) {
  return new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(value * 1000));
}
