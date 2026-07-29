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
        setError(await responseMessage(response));
        return;
      }

      const project = (await response.json()) as ProjectSummary;
      setName("");
      onOpenChange(false);
      await onCreated?.(project);
      toast.success("项目已创建");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "项目创建失败");
    } finally {
      setPending(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogContent className="sm:max-w-lg">
        <form onSubmit={createProject}>
          <DialogHeader>
            <DialogTitle>创建新项目</DialogTitle>
            <DialogDescription>
              {organizationId ? (
                <>
                  项目将创建在组织 <strong>{organizationName}</strong> 中。
                </>
              ) : (
                "全平台视图不能创建项目，请先切换到一个组织或项目工作区。"
              )}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-2 py-5">
            <Label htmlFor="sidebar-project-name">名称</Label>
            <p className="text-xs text-muted-foreground">
              使用便于识别的名称，项目创建后会出现在项目切换器中。
            </p>
            <Input
              id="sidebar-project-name"
              value={name}
              onChange={(event) => setName(event.target.value)}
              maxLength={128}
              placeholder="项目名称"
              autoFocus
              required
              disabled={!canCreate || pending}
            />
            {organizationId && !canCreate ? (
              <p className="text-sm text-muted-foreground">
                只有当前组织的所有者可以创建项目。
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
              取消
            </Button>
            <Button type="submit" disabled={pending || !name.trim() || !canCreate}>
              {pending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : null}
              创建
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
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
