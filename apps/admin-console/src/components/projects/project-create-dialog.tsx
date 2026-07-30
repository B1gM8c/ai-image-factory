"use client";

import { useState } from "react";
import { LoaderCircle } from "lucide-react";
import { toast } from "sonner";
import { useConsoleSession } from "@/components/auth/console-session-provider";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useI18n } from "@/i18n/locale-provider";
import { consoleFetch } from "@/lib/auth/client";

export type ProjectSummary = {
  id: string;
  object: "organization.project";
  name: string;
  created_at: number;
  archived_at: number | null;
  service_tier: "default" | "priority";
  user_api_keys_disabled: boolean;
  settings_version: number;
  status: "active" | "archived";
};

export function ProjectCreateDialog({
  open,
  onOpenChange,
  onCreated,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCreated?: (project: ProjectSummary) => void | Promise<void>;
}) {
  const { activeWorkspace, organizations, user } = useConsoleSession();
  const { t } = useI18n();
  const [name, setName] = useState("");
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const organizationId =
    activeWorkspace?.kind === "project"
      ? activeWorkspace.organizationId
      : activeWorkspace?.kind === "organization"
        ? activeWorkspace.id
        : null;
  const organization = organizations.find((item) => item.id === organizationId);
  const organizationName =
    organization?.name ??
    (activeWorkspace?.kind === "project" ? activeWorkspace.detail : activeWorkspace?.name) ??
    organizationId;
  const canCreate =
    Boolean(organizationId) &&
    (Boolean(user?.roles.includes("platform_owner")) || organization?.role === "owner");

  function setOpen(next: boolean) {
    if (pending) return;
    if (!next) {
      setName("");
      setError(null);
    }
    onOpenChange(next);
  }

  async function createProject(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalized = name.trim();
    if (!normalized || !organizationId || !canCreate) return;

    setPending(true);
    setError(null);
    try {
      const response = await consoleFetch("/api/gateway/v1/organization/projects", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: normalized,
          organization_id: organizationId,
        }),
      });
      if (!response.ok) {
        setError(
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
        return;
      }

      const project = (await response.json()) as ProjectSummary;
      setName("");
      onOpenChange(false);
      await onCreated?.(project);
      toast.success(
        t({
          en: "Project created",
          "zh-CN": "项目已创建",
          ja: "プロジェクトを作成しました",
          ko: "프로젝트를 만들었습니다",
        }),
      );
    } catch (reason) {
      setError(
        reason instanceof Error
          ? reason.message
          : t({
              en: "Failed to create project",
              "zh-CN": "项目创建失败",
              ja: "プロジェクトを作成できませんでした",
              ko: "프로젝트를 만들지 못했습니다",
            }),
      );
    } finally {
      setPending(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogContent className="sm:max-w-lg">
        <form onSubmit={createProject}>
          <DialogHeader>
            <DialogTitle>
              {t({
                en: "Create project",
                "zh-CN": "创建新项目",
                ja: "プロジェクトを作成",
                ko: "프로젝트 만들기",
              })}
            </DialogTitle>
            <DialogDescription>
              {organizationId ? (
                t(
                  {
                    en: "The project will be created in {organization}.",
                    "zh-CN": "项目将创建在组织 {organization} 中。",
                    ja: "プロジェクトは組織「{organization}」に作成されます。",
                    ko: "프로젝트가 {organization} 조직에 생성됩니다.",
                  },
                  { organization: organizationName ?? organizationId },
                )
              ) : (
                t({
                  en: "Projects cannot be created from the platform view. Switch to an organization or project workspace first.",
                  "zh-CN": "全平台视图不能创建项目，请先切换到一个组织或项目工作区。",
                  ja: "プラットフォーム表示ではプロジェクトを作成できません。先に組織またはプロジェクトのワークスペースへ切り替えてください。",
                  ko: "플랫폼 보기에서는 프로젝트를 만들 수 없습니다. 먼저 조직 또는 프로젝트 워크스페이스로 전환하세요.",
                })
              )}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-2 py-5">
            <Label htmlFor="sidebar-project-name">
              {t({
                en: "Name",
                "zh-CN": "名称",
                ja: "名前",
                ko: "이름",
              })}
            </Label>
            <p className="text-xs text-muted-foreground">
              {t({
                en: "Use a recognizable name. The project will appear in the workspace switcher after it is created.",
                "zh-CN": "使用便于识别的名称，项目创建后会出现在项目切换器中。",
                ja: "識別しやすい名前を使用してください。作成後、プロジェクトはワークスペース切り替えに表示されます。",
                ko: "알아보기 쉬운 이름을 사용하세요. 생성된 프로젝트는 워크스페이스 전환 메뉴에 표시됩니다.",
              })}
            </p>
            <Input
              id="sidebar-project-name"
              value={name}
              onChange={(event) => setName(event.target.value)}
              maxLength={128}
              placeholder={t({
                en: "Project name",
                "zh-CN": "项目名称",
                ja: "プロジェクト名",
                ko: "프로젝트 이름",
              })}
              autoFocus
              required
              disabled={!canCreate || pending}
            />
            {organizationId && !canCreate ? (
              <p className="text-sm text-muted-foreground">
                {t({
                  en: "Only owners of the current organization can create projects.",
                  "zh-CN": "只有当前组织的所有者可以创建项目。",
                  ja: "現在の組織の所有者のみがプロジェクトを作成できます。",
                  ko: "현재 조직의 소유자만 프로젝트를 만들 수 있습니다.",
                })}
              </p>
            ) : null}
            {error ? (
              <p role="alert" className="text-sm text-destructive">
                {error}
              </p>
            ) : null}
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" disabled={pending} onClick={() => setOpen(false)}>
              {t({
                en: "Cancel",
                "zh-CN": "取消",
                ja: "キャンセル",
                ko: "취소",
              })}
            </Button>
            <Button type="submit" disabled={pending || !name.trim() || !canCreate}>
              {pending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : null}
              {t({
                en: "Create",
                "zh-CN": "创建",
                ja: "作成",
                ko: "만들기",
              })}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
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
