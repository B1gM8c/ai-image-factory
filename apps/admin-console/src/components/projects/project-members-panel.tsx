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
import { useI18n } from "@/i18n/locale-provider";
import {
  consoleFetch,
  consoleRequestFailure,
  consoleResponseFailure,
} from "@/lib/auth/client";

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
  const { t } = useI18n();
  const [members, setMembers] = useState<ProjectMember[]>([]);
  const [loading, setLoading] = useState(false);
  const [pendingUserId, setPendingUserId] = useState<string | null>(null);
  const [addOpen, setAddOpen] = useState(false);
  const [removeMember, setRemoveMember] = useState<ProjectMember | null>(null);
  const [email, setEmail] = useState("");
  const [role, setRole] = useState<ProjectMemberRole>("member");
  const [error, setError] = useState<string | null>(null);

  const loadMembers = useCallback(async () => {
    const failure = t({
      en: "Failed to load project members.",
      "zh-CN": "项目成员加载失败。",
      ja: "プロジェクトメンバーを読み込めませんでした。",
      ko: "프로젝트 멤버를 불러오지 못했습니다.",
    });
    setLoading(true);
    setError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/members`,
      );
      if (!response.ok) {
        throw new Error(await consoleResponseFailure(response, failure, t));
      }
      const payload = (await response.json()) as ProjectMemberList;
      setMembers(payload.data);
    } catch (reason) {
      setMembers([]);
      setError(consoleRequestFailure(reason, failure, t));
    } finally {
      setLoading(false);
    }
  }, [projectId, t]);

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
    const failure = t({
      en: "Failed to add the project member.",
      "zh-CN": "添加项目成员失败。",
      ja: "プロジェクトメンバーを追加できませんでした。",
      ko: "프로젝트 멤버를 추가하지 못했습니다.",
    });
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
      if (!response.ok) {
        throw new Error(await consoleResponseFailure(response, failure, t));
      }
      setEmail("");
      setRole("member");
      setAddOpen(false);
      await loadMembers();
      toast.success(t({ en: "Project member added", "zh-CN": "项目成员已添加", ja: "プロジェクトメンバーを追加しました", ko: "프로젝트 멤버를 추가했습니다" }));
    } catch (reason) {
      setError(consoleRequestFailure(reason, failure, t));
    } finally {
      setPendingUserId(null);
    }
  }

  async function updateRole(member: ProjectMember, nextRole: ProjectMemberRole) {
    if (!canManage || member.role === nextRole) return;
    const failure = t({
      en: "Failed to update the member role.",
      "zh-CN": "成员角色更新失败。",
      ja: "メンバーのロールを更新できませんでした。",
      ko: "멤버 역할을 업데이트하지 못했습니다.",
    });
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
      if (!response.ok) {
        throw new Error(await consoleResponseFailure(response, failure, t));
      }
      await loadMembers();
      toast.success(t({ en: "Member role updated", "zh-CN": "成员角色已更新", ja: "メンバーのロールを更新しました", ko: "멤버 역할을 업데이트했습니다" }));
    } catch (reason) {
      setError(consoleRequestFailure(reason, failure, t));
    } finally {
      setPendingUserId(null);
    }
  }

  async function confirmRemove() {
    if (!removeMember || !canManage) return;
    const target = removeMember;
    const failure = t({
      en: "Failed to remove the project member.",
      "zh-CN": "移除项目成员失败。",
      ja: "プロジェクトメンバーを削除できませんでした。",
      ko: "프로젝트 멤버를 제거하지 못했습니다.",
    });
    setPendingUserId(target.user_id);
    setError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/members/${encodeURIComponent(target.user_id)}`,
        { method: "DELETE" },
      );
      if (!response.ok) {
        throw new Error(await consoleResponseFailure(response, failure, t));
      }
      setRemoveMember(null);
      await loadMembers();
      toast.success(t({ en: "Project member removed", "zh-CN": "项目成员已移除", ja: "プロジェクトメンバーを削除しました", ko: "프로젝트 멤버를 제거했습니다" }));
    } catch (reason) {
      setError(consoleRequestFailure(reason, failure, t));
    } finally {
      setPendingUserId(null);
    }
  }

  return (
    <div className="space-y-5 px-5 py-6 sm:px-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h3 className="text-sm font-medium">{t({ en: "Project members", "zh-CN": "项目成员", ja: "プロジェクトメンバー", ko: "프로젝트 멤버" })}</h3>
          <p className="mt-1 text-sm text-muted-foreground">
            {t({ en: "Members can access only the projects they join. Owners can manage members, limits, and project credentials.", "zh-CN": "成员只能访问已加入的项目；所有者可以管理成员、限额与项目凭据。", ja: "メンバーは参加しているプロジェクトのみにアクセスできます。所有者はメンバー、上限、プロジェクト認証情報を管理できます。", ko: "멤버는 참여한 프로젝트에만 액세스할 수 있습니다. 소유자는 멤버, 한도 및 프로젝트 자격 증명을 관리할 수 있습니다." })}
          </p>
        </div>
        <div className="flex shrink-0 gap-2">
          <Button
            type="button"
            variant="outline"
            size="icon"
            onClick={() => void loadMembers()}
            disabled={loading}
            aria-label={t({ en: "Refresh project members", "zh-CN": "刷新项目成员", ja: "プロジェクトメンバーを更新", ko: "프로젝트 멤버 새로고침" })}
            title={t({ en: "Refresh project members", "zh-CN": "刷新项目成员", ja: "プロジェクトメンバーを更新", ko: "프로젝트 멤버 새로고침" })}
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
              {t({ en: "Add member", "zh-CN": "添加成员", ja: "メンバーを追加", ko: "멤버 추가" })}
            </Button>
          ) : null}
        </div>
      </div>

      <div className="overflow-x-auto border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t({ en: "Member", "zh-CN": "成员", ja: "メンバー", ko: "멤버" })}</TableHead>
              <TableHead className="w-36">{t({ en: "Role", "zh-CN": "角色", ja: "ロール", ko: "역할" })}</TableHead>
              <TableHead className="w-24">{t({ en: "Status", "zh-CN": "状态", ja: "ステータス", ko: "상태" })}</TableHead>
              <TableHead className="w-14">
                <span className="sr-only">{t({ en: "Actions", "zh-CN": "操作", ja: "操作", ko: "작업" })}</span>
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
                              {t({ en: " (you)", "zh-CN": "（你）", ja: "（あなた）", ko: " (나)" })}
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
                          aria-label={t({ en: "{member}'s project role", "zh-CN": "{member} 的项目角色", ja: "{member} のプロジェクトロール", ko: "{member}의 프로젝트 역할" }, { member: member.display_name })}
                          title={isLastOwner ? t({ en: "The project must retain at least one owner", "zh-CN": "项目必须保留至少一位所有者", ja: "プロジェクトには少なくとも 1 人の所有者が必要です", ko: "프로젝트에는 소유자가 한 명 이상 있어야 합니다" }) : undefined}
                        >
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="member">{t({ en: "Member", "zh-CN": "成员", ja: "メンバー", ko: "멤버" })}</SelectItem>
                          <SelectItem value="owner">{t({ en: "Owner", "zh-CN": "所有者", ja: "所有者", ko: "소유자" })}</SelectItem>
                        </SelectContent>
                      </Select>
                    ) : (
                      <span className="text-sm">
                        {member.role === "owner"
                          ? t({ en: "Owner", "zh-CN": "所有者", ja: "所有者", ko: "소유자" })
                          : t({ en: "Member", "zh-CN": "成员", ja: "メンバー", ko: "멤버" })}
                      </span>
                    )}
                  </TableCell>
                  <TableCell>
                    <Badge variant={member.state === "active" ? "secondary" : "outline"}>
                      {member.state === "active"
                        ? t({ en: "Active", "zh-CN": "已加入", ja: "参加中", ko: "활성" })
                        : t({ en: "Removed", "zh-CN": "已移除", ja: "削除済み", ko: "제거됨" })}
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
                        aria-label={t({ en: "Remove {member}", "zh-CN": "移除 {member}", ja: "{member} を削除", ko: "{member} 제거" }, { member: member.display_name })}
                        title={
                          isLastOwner
                            ? t({ en: "The project must retain at least one owner", "zh-CN": "项目必须保留至少一位所有者", ja: "プロジェクトには少なくとも 1 人の所有者が必要です", ko: "프로젝트에는 소유자가 한 명 이상 있어야 합니다" })
                            : t({ en: "Remove member", "zh-CN": "移除成员", ja: "メンバーを削除", ko: "멤버 제거" })
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
                  {error ?? t({ en: "No project members", "zh-CN": "暂无项目成员", ja: "プロジェクトメンバーはいません", ko: "프로젝트 멤버가 없습니다" })}
                </TableCell>
              </TableRow>
            ) : null}
          </TableBody>
        </Table>
      </div>

      {!canManage ? (
        <p className="text-sm text-muted-foreground">
          {t({ en: "Only project or organization owners can change member roles and access.", "zh-CN": "只有项目或组织所有者可以调整成员角色和访问权限。", ja: "メンバーのロールとアクセス権を変更できるのは、プロジェクトまたは組織の所有者のみです。", ko: "프로젝트 또는 조직 소유자만 멤버 역할과 액세스를 변경할 수 있습니다." })}
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
              <DialogTitle>{t({ en: "Add project member", "zh-CN": "添加项目成员", ja: "プロジェクトメンバーを追加", ko: "프로젝트 멤버 추가" })}</DialogTitle>
              <DialogDescription>
                {t({ en: "Select a registered account and grant access only to this project.", "zh-CN": "选择一个已注册账户，并仅授予当前项目的访问权限。", ja: "登録済みアカウントを選択し、このプロジェクトへのアクセスのみを付与します。", ko: "등록된 계정을 선택하고 현재 프로젝트에만 액세스 권한을 부여합니다." })}
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4 py-5">
              <div className="space-y-2">
                <Label htmlFor="project-member-email">{t({ en: "Account email", "zh-CN": "账户邮箱", ja: "アカウントのメール", ko: "계정 이메일" })}</Label>
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
                <Label htmlFor="project-member-role">{t({ en: "Project role", "zh-CN": "项目角色", ja: "プロジェクトロール", ko: "프로젝트 역할" })}</Label>
                <Select
                  value={role}
                  onValueChange={(value) => setRole(value as ProjectMemberRole)}
                >
                  <SelectTrigger id="project-member-role">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="member">{t({ en: "Member", "zh-CN": "成员", ja: "メンバー", ko: "멤버" })}</SelectItem>
                    <SelectItem value="owner">{t({ en: "Owner", "zh-CN": "所有者", ja: "所有者", ko: "소유자" })}</SelectItem>
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  {t({ en: "Owners can manage members, project limits, and project API credentials.", "zh-CN": "所有者可以管理成员、项目限额和项目 API 凭据。", ja: "所有者はメンバー、プロジェクト上限、プロジェクト API 認証情報を管理できます。", ko: "소유자는 멤버, 프로젝트 한도 및 프로젝트 API 자격 증명을 관리할 수 있습니다." })}
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
                {t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}
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
                {t({ en: "Add", "zh-CN": "添加", ja: "追加", ko: "추가" })}
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
            <AlertDialogTitle>{t({ en: "Remove project member?", "zh-CN": "移除项目成员？", ja: "プロジェクトメンバーを削除しますか？", ko: "프로젝트 멤버를 제거할까요?" })}</AlertDialogTitle>
            <AlertDialogDescription>
              {removeMember
                ? t({ en: "{member} will immediately lose access to this project. Existing sessions and user API key access will also be revoked.", "zh-CN": "{member} 将立即失去此项目的访问权限，已有会话和用户 API Key 权限会同步失效。", ja: "{member} は直ちにこのプロジェクトへアクセスできなくなります。既存のセッションとユーザー API キーの権限も無効になります。", ko: "{member}은(는) 즉시 이 프로젝트에 대한 액세스 권한을 잃으며 기존 세션과 사용자 API 키 권한도 함께 취소됩니다." }, { member: removeMember.display_name })
                : ""}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={pendingUserId !== null}>{t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}</AlertDialogCancel>
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
              {t({ en: "Remove", "zh-CN": "移除", ja: "削除", ko: "제거" })}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
