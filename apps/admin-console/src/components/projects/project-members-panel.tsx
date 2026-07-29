"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { LoaderCircle, Plus, RefreshCw, Trash2, UserRound } from "lucide-react";
import { toast } from "sonner";
import { useConsoleSession } from "@/components/auth/console-session-provider";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Badge } from "@/components/ui/badge";
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { consoleFetch } from "@/lib/auth/client";

type ProjectMemberRole = "owner" | "member";

type ProjectMember = {
  object: "organization.project.user";
  user_id: string;
  email: string;
  display_name: string;
  role: ProjectMemberRole;
  state: "active" | "disabled";
  is_default: boolean;
  created_at_ms: number;
  updated_at_ms: number;
};

type ProjectMemberList = {
  object: "list";
  data: ProjectMember[];
};

export function ProjectMembersPanel({
  projectId,
  canManage,
  active,
}: {
  projectId: string;
  canManage: boolean;
  active: boolean;
}) {
  const { user } = useConsoleSession();
  const [members, setMembers] = useState<ProjectMember[]>([]);
  const [loading, setLoading] = useState(false);
  const [pendingUserId, setPendingUserId] = useState<string | null>(null);
  const [addOpen, setAddOpen] = useState(false);
  const [removeMember, setRemoveMember] = useState<ProjectMember | null>(null);
  const [email, setEmail] = useState("");
  const [role, setRole] = useState<ProjectMemberRole>("member");
  const [error, setError] = useState<string | null>(null);

  const loadMembers = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/members`,
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      const payload = (await response.json()) as ProjectMemberList;
      setMembers(payload.data);
    } catch (reason) {
      setMembers([]);
      setError(reason instanceof Error ? reason.message : "项目成员加载失败");
    } finally {
      setLoading(false);
    }
  }, [projectId]);

  useEffect(() => {
    if (active) void loadMembers();
  }, [active, loadMembers]);

  const activeOwnerCount = useMemo(
    () =>
      members.filter(
        (member) => member.state === "active" && member.role === "owner",
      ).length,
    [members],
  );

  async function addMember(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalizedEmail = email.trim();
    if (!normalizedEmail || !canManage) return;
    setPendingUserId("add");
    setError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/members`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ email: normalizedEmail, role }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      setEmail("");
      setRole("member");
      setAddOpen(false);
      await loadMembers();
      toast.success("项目成员已添加");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "添加项目成员失败");
    } finally {
      setPendingUserId(null);
    }
  }

  async function updateRole(member: ProjectMember, nextRole: ProjectMemberRole) {
    if (!canManage || member.role === nextRole) return;
    setPendingUserId(member.user_id);
    setError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/members/${encodeURIComponent(member.user_id)}`,
        {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ role: nextRole }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      await loadMembers();
      toast.success("成员角色已更新");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "成员角色更新失败");
    } finally {
      setPendingUserId(null);
    }
  }

  async function confirmRemove() {
    if (!removeMember || !canManage) return;
    const target = removeMember;
    setPendingUserId(target.user_id);
    setError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/members/${encodeURIComponent(target.user_id)}`,
        { method: "DELETE" },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      setRemoveMember(null);
      await loadMembers();
      toast.success("项目成员已移除");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "移除项目成员失败");
    } finally {
      setPendingUserId(null);
    }
  }

  return (
    <div className="space-y-5 px-5 py-6 sm:px-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h3 className="text-sm font-medium">项目成员</h3>
          <p className="mt-1 text-sm text-muted-foreground">
            成员只能访问已加入的项目；所有者可以管理成员、限额与项目凭据。
          </p>
        </div>
        <div className="flex shrink-0 gap-2">
          <Button
            type="button"
            variant="outline"
            size="icon"
            onClick={() => void loadMembers()}
            disabled={loading}
            aria-label="刷新项目成员"
            title="刷新项目成员"
          >
            {loading ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <RefreshCw aria-hidden="true" />
            )}
          </Button>
          {canManage ? (
            <Button type="button" onClick={() => setAddOpen(true)}>
              <Plus aria-hidden="true" />
              添加成员
            </Button>
          ) : null}
        </div>
      </div>

      <div className="overflow-x-auto border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>成员</TableHead>
              <TableHead className="w-36">角色</TableHead>
              <TableHead className="w-24">状态</TableHead>
              <TableHead className="w-14">
                <span className="sr-only">操作</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {members.map((member) => {
              const pending = pendingUserId === member.user_id;
              const isLastOwner =
                member.state === "active" &&
                member.role === "owner" &&
                activeOwnerCount === 1;
              return (
                <TableRow
                  key={member.user_id}
                  className={member.state === "disabled" ? "opacity-60" : undefined}
                >
                  <TableCell>
                    <div className="flex min-w-52 items-center gap-3">
                      <div className="grid size-8 shrink-0 place-items-center rounded-md border bg-muted/40">
                        <UserRound className="size-4" aria-hidden="true" />
                      </div>
                      <div className="min-w-0">
                        <p className="truncate text-sm font-medium">
                          {member.display_name}
                          {member.user_id === user?.id ? (
                            <span className="ml-1 font-normal text-muted-foreground">
                              （你）
                            </span>
                          ) : null}
                        </p>
                        <p className="truncate text-xs text-muted-foreground">
                          {member.email}
                        </p>
                      </div>
                    </div>
                  </TableCell>
                  <TableCell>
                    {canManage && member.state === "active" ? (
                      <Select
                        value={member.role}
                        onValueChange={(value) =>
                          void updateRole(member, value as ProjectMemberRole)
                        }
                        disabled={pending || isLastOwner}
                      >
                        <SelectTrigger
                          className="h-8"
                          aria-label={`${member.display_name} 的项目角色`}
                          title={isLastOwner ? "项目必须保留至少一位所有者" : undefined}
                        >
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="member">成员</SelectItem>
                          <SelectItem value="owner">所有者</SelectItem>
                        </SelectContent>
                      </Select>
                    ) : (
                      <span className="text-sm">
                        {member.role === "owner" ? "所有者" : "成员"}
                      </span>
                    )}
                  </TableCell>
                  <TableCell>
                    <Badge variant={member.state === "active" ? "secondary" : "outline"}>
                      {member.state === "active" ? "已加入" : "已移除"}
                    </Badge>
                  </TableCell>
                  <TableCell>
                    {canManage && member.state === "active" ? (
                      <Button
                        type="button"
                        variant="ghost"
                        size="icon"
                        onClick={() => setRemoveMember(member)}
                        disabled={pending || isLastOwner}
                        aria-label={`移除 ${member.display_name}`}
                        title={
                          isLastOwner ? "项目必须保留至少一位所有者" : "移除成员"
                        }
                      >
                        {pending ? (
                          <LoaderCircle className="animate-spin" aria-hidden="true" />
                        ) : (
                          <Trash2 aria-hidden="true" />
                        )}
                      </Button>
                    ) : null}
                  </TableCell>
                </TableRow>
              );
            })}
            {!loading && members.length === 0 ? (
              <TableRow>
                <TableCell
                  colSpan={4}
                  className="h-28 text-center text-muted-foreground"
                >
                  {error ?? "暂无项目成员"}
                </TableCell>
              </TableRow>
            ) : null}
          </TableBody>
        </Table>
      </div>

      {!canManage ? (
        <p className="text-sm text-muted-foreground">
          只有项目或组织所有者可以调整成员角色和访问权限。
        </p>
      ) : null}
      {error ? (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      ) : null}

      <Dialog open={addOpen} onOpenChange={setAddOpen}>
        <DialogContent className="sm:max-w-md">
          <form onSubmit={addMember}>
            <DialogHeader>
              <DialogTitle>添加项目成员</DialogTitle>
              <DialogDescription>
                选择一个已注册账户，并仅授予当前项目的访问权限。
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4 py-5">
              <div className="space-y-2">
                <Label htmlFor="project-member-email">账户邮箱</Label>
                <Input
                  id="project-member-email"
                  type="email"
                  value={email}
                  onChange={(event) => setEmail(event.target.value)}
                  placeholder="name@example.com"
                  autoFocus
                  required
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="project-member-role">项目角色</Label>
                <Select
                  value={role}
                  onValueChange={(value) => setRole(value as ProjectMemberRole)}
                >
                  <SelectTrigger id="project-member-role">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="member">成员</SelectItem>
                    <SelectItem value="owner">所有者</SelectItem>
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  所有者可以管理成员、项目限额和项目 API 凭据。
                </p>
              </div>
            </div>
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => setAddOpen(false)}
                disabled={pendingUserId === "add"}
              >
                取消
              </Button>
              <Button
                type="submit"
                disabled={pendingUserId === "add" || !email.trim()}
              >
                {pendingUserId === "add" ? (
                  <LoaderCircle className="animate-spin" aria-hidden="true" />
                ) : (
                  <Plus aria-hidden="true" />
                )}
                添加
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <AlertDialog
        open={removeMember !== null}
        onOpenChange={(open) => {
          if (!open && pendingUserId === null) setRemoveMember(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>移除项目成员？</AlertDialogTitle>
            <AlertDialogDescription>
              {removeMember
                ? `${removeMember.display_name} 将立即失去此项目的访问权限，已有会话和用户 API Key 权限会同步失效。`
                : ""}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={pendingUserId !== null}>取消</AlertDialogCancel>
            <AlertDialogAction
              onClick={(event) => {
                event.preventDefault();
                void confirmRemove();
              }}
              disabled={pendingUserId !== null}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              {pendingUserId ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <Trash2 aria-hidden="true" />
              )}
              移除
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
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
