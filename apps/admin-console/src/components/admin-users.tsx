"use client";

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { LoaderCircle, Plus, RefreshCw, Search, UserRound } from "lucide-react";
import { toast } from "sonner";
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
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useI18n } from "@/i18n/locale-provider";
import { consoleFetch } from "@/lib/auth/client";

type Translate = ReturnType<typeof useI18n>["t"];

type OrganizationAccess = {
  organization_id: string;
  display_name: string;
  role: string;
  is_personal: boolean;
};

type ProjectAccess = {
  organization_id: string;
  project_id: string;
  display_name: string;
  role: string;
  is_default: boolean;
};

export type IdentityUserAccess = {
  user_id: string;
  email: string;
  display_name: string;
  roles: string[];
  scopes: string[];
  disabled: boolean;
  created_at_ms: number;
  organizations: OrganizationAccess[];
  projects: ProjectAccess[];
};

type NewUserForm = {
  email: string;
  displayName: string;
  password: string;
};

const EMPTY_FORM: NewUserForm = {
  email: "",
  displayName: "",
  password: "",
};

export function AdminUsers() {
  const { locale, t } = useI18n();
  const [users, setUsers] = useState<IdentityUserAccess[]>([]);
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [createPending, setCreatePending] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [form, setForm] = useState<NewUserForm>(EMPTY_FORM);

  const loadUsers = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    setLoadError(null);
    try {
      const response = await consoleFetch("/api/gateway/admin/v1/users", { signal });
      if (!response.ok) throw new Error(await responseMessage(response, t));
      const body = (await response.json()) as unknown;
      if (!Array.isArray(body)) {
        throw new Error(
          t({
            en: "The user list response has an invalid format",
            "zh-CN": "用户列表响应格式不正确",
            ja: "ユーザー一覧の応答形式が正しくありません",
            ko: "사용자 목록 응답 형식이 올바르지 않습니다",
          }),
        );
      }
      setUsers(body as IdentityUserAccess[]);
    } catch (reason) {
      if (reason instanceof DOMException && reason.name === "AbortError") return;
      setLoadError(
        reason instanceof Error
          ? reason.message
          : t({
              en: "Users are temporarily unavailable",
              "zh-CN": "暂时无法加载用户",
              ja: "ユーザーを一時的に読み込めません",
              ko: "사용자를 일시적으로 불러올 수 없습니다",
            }),
      );
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, [t]);

  useEffect(() => {
    const controller = new AbortController();
    void loadUsers(controller.signal);
    return () => controller.abort();
  }, [loadUsers]);

  const filteredUsers = useMemo(() => {
    const keyword = query.trim().toLocaleLowerCase(locale);
    if (!keyword) return users;

    return users.filter((user) => {
      const searchable = [
        user.display_name,
        user.email,
        ...user.roles.map((role) => formatRole(t, role)),
        ...user.organizations.map((organization) => organization.display_name),
        ...user.projects.map((project) => project.display_name),
      ];
      return searchable.some((value) => value.toLocaleLowerCase(locale).includes(keyword));
    });
  }, [locale, query, t, users]);

  async function createUser(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const email = form.email.trim();
    const displayName = form.displayName.trim();
    if (!email || !displayName || !form.password) return;

    setCreatePending(true);
    setCreateError(null);
    try {
      const response = await consoleFetch("/api/gateway/admin/v1/users", {
        method: "POST",
        body: JSON.stringify({
          email,
          display_name: displayName,
          password: form.password,
        }),
      });
      if (!response.ok) throw new Error(await responseMessage(response, t));

      const created = (await response.json()) as IdentityUserAccess;
      setUsers((current) => [
        created,
        ...current.filter((user) => user.user_id !== created.user_id),
      ]);
      setForm(EMPTY_FORM);
      setCreateOpen(false);
      toast.success(
        t({
          en: "User added",
          "zh-CN": "用户已添加",
          ja: "ユーザーを追加しました",
          ko: "사용자가 추가되었습니다",
        }),
      );
    } catch (reason) {
      setCreateError(
        reason instanceof Error
          ? reason.message
          : t({
              en: "Could not create the user. Try again later.",
              "zh-CN": "用户创建失败，请稍后重试",
              ja: "ユーザーを作成できませんでした。しばらくしてから再試行してください。",
              ko: "사용자를 만들 수 없습니다. 잠시 후 다시 시도하세요.",
            }),
      );
      setForm((current) => ({ ...current, password: "" }));
    } finally {
      setCreatePending(false);
    }
  }

  function handleDialogChange(open: boolean) {
    if (createPending) return;
    setCreateOpen(open);
    setCreateError(null);
    if (!open) setForm(EMPTY_FORM);
  }

  return (
    <section className="min-w-0 overflow-hidden rounded-lg border bg-background">
      <div className="flex flex-col gap-3 border-b p-4 sm:flex-row sm:items-center sm:justify-between">
        <div className="relative w-full sm:max-w-sm">
          <Search
            className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
            aria-hidden="true"
          />
          <Input
            className="pl-9"
            aria-label={t({
              en: "Search users",
              "zh-CN": "搜索用户",
              ja: "ユーザーを検索",
              ko: "사용자 검색",
            })}
            placeholder={t({
              en: "Search by name, email, or workspace",
              "zh-CN": "搜索姓名、邮箱或工作区",
              ja: "名前、メール、ワークスペースで検索",
              ko: "이름, 이메일 또는 워크스페이스 검색",
            })}
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
        </div>
        <div className="flex items-center justify-between gap-3 sm:justify-end">
          <span className="text-sm text-muted-foreground">
            {t(
              {
                en: "{count} users",
                "zh-CN": "{count} 位用户",
                ja: "{count} 人のユーザー",
                ko: "사용자 {count}명",
              },
              { count: filteredUsers.length },
            )}
          </span>
          <Button onClick={() => setCreateOpen(true)}>
            <Plus aria-hidden="true" />
            {t({
              en: "Add user",
              "zh-CN": "添加用户",
              ja: "ユーザーを追加",
              ko: "사용자 추가",
            })}
          </Button>
        </div>
      </div>

      {loadError ? (
        <div
          className="flex min-h-40 flex-col items-center justify-center gap-3 px-6 py-10 text-center"
          role="alert"
        >
          <div>
            <p className="font-medium">
              {t({
                en: "Could not load users",
                "zh-CN": "用户列表加载失败",
                ja: "ユーザーを読み込めませんでした",
                ko: "사용자를 불러올 수 없습니다",
              })}
            </p>
            <p className="mt-1 text-sm text-muted-foreground">{loadError}</p>
          </div>
          <Button variant="outline" size="sm" onClick={() => void loadUsers()}>
            <RefreshCw aria-hidden="true" />
            {t({
              en: "Reload",
              "zh-CN": "重新加载",
              ja: "再読み込み",
              ko: "다시 불러오기",
            })}
          </Button>
        </div>
      ) : (
        <div className="max-w-full overflow-hidden">
          <Table className="min-w-[900px]">
            <TableHeader>
              <TableRow>
                <TableHead className="pl-4">{t({ en: "User", "zh-CN": "用户", ja: "ユーザー", ko: "사용자" })}</TableHead>
                <TableHead>{t({ en: "Role", "zh-CN": "角色", ja: "ロール", ko: "역할" })}</TableHead>
                <TableHead>{t({ en: "Default workspace", "zh-CN": "默认工作区", ja: "既定のワークスペース", ko: "기본 워크스페이스" })}</TableHead>
                <TableHead>{t({ en: "Default project", "zh-CN": "默认项目", ja: "既定のプロジェクト", ko: "기본 프로젝트" })}</TableHead>
                <TableHead>{t({ en: "Status", "zh-CN": "状态", ja: "ステータス", ko: "상태" })}</TableHead>
                <TableHead className="pr-4 text-right">{t({ en: "Created", "zh-CN": "创建时间", ja: "作成日時", ko: "생성 시간" })}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                <TableRow>
                  <TableCell colSpan={6} className="h-40 text-center text-muted-foreground">
                    <LoaderCircle className="mx-auto mb-2 size-5 animate-spin" aria-hidden="true" />
                    {t({ en: "Loading users", "zh-CN": "正在加载用户", ja: "ユーザーを読み込み中", ko: "사용자 불러오는 중" })}
                  </TableCell>
                </TableRow>
              ) : null}
              {!loading && users.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="h-48 text-center">
                    <UserRound className="mx-auto mb-3 size-8 text-muted-foreground" aria-hidden="true" />
                    <p className="font-medium">{t({ en: "No users yet", "zh-CN": "还没有用户", ja: "ユーザーはまだいません", ko: "아직 사용자가 없습니다" })}</p>
                    <p className="mt-1 text-sm text-muted-foreground">
                      {t({
                        en: "Add the first user to provision their default workspace and project.",
                        "zh-CN": "添加首位用户后，系统会为其准备默认工作区和项目。",
                        ja: "最初のユーザーを追加すると、既定のワークスペースとプロジェクトが作成されます。",
                        ko: "첫 사용자를 추가하면 기본 워크스페이스와 프로젝트가 준비됩니다.",
                      })}
                    </p>
                    <Button className="mt-4" size="sm" onClick={() => setCreateOpen(true)}>
                      <Plus aria-hidden="true" />
                      {t({ en: "Add user", "zh-CN": "添加用户", ja: "ユーザーを追加", ko: "사용자 추가" })}
                    </Button>
                  </TableCell>
                </TableRow>
              ) : null}
              {!loading && users.length > 0 && filteredUsers.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="h-40 text-center">
                    <p className="font-medium">{t({ en: "No matching users", "zh-CN": "没有匹配的用户", ja: "一致するユーザーはいません", ko: "일치하는 사용자가 없습니다" })}</p>
                    <p className="mt-1 text-sm text-muted-foreground">
                      {t({
                        en: "Try searching by name, email, or workspace.",
                        "zh-CN": "尝试使用姓名、邮箱或工作区搜索。",
                        ja: "名前、メール、またはワークスペースで検索してください。",
                        ko: "이름, 이메일 또는 워크스페이스로 검색해 보세요.",
                      })}
                    </p>
                  </TableCell>
                </TableRow>
              ) : null}
              {!loading
                ? filteredUsers.map((user) => {
                    const organization = defaultOrganization(user);
                    const project = defaultProject(user, organization?.organization_id);
                    return (
                      <TableRow key={user.user_id}>
                        <TableCell className="pl-4">
                          <p className="max-w-64 truncate font-medium">{user.display_name}</p>
                          <p className="mt-0.5 max-w-64 truncate text-xs text-muted-foreground">
                            {user.email}
                          </p>
                        </TableCell>
                        <TableCell>
                          <div className="flex flex-wrap gap-1">
                            {(user.roles.length ? user.roles : ["member"]).map((role) => (
                              <Badge key={role} variant="outline">
                                {formatRole(t, role)}
                              </Badge>
                            ))}
                          </div>
                        </TableCell>
                        <TableCell>
                          <p className="max-w-52 truncate">{organization?.display_name ?? t({ en: "Unassigned", "zh-CN": "尚未分配", ja: "未割り当て", ko: "할당되지 않음" })}</p>
                          {organization?.is_personal ? (
                            <p className="mt-0.5 text-xs text-muted-foreground">{t({ en: "Personal workspace", "zh-CN": "个人工作区", ja: "個人ワークスペース", ko: "개인 워크스페이스" })}</p>
                          ) : null}
                        </TableCell>
                        <TableCell>
                          <p className="max-w-52 truncate">{project?.display_name ?? t({ en: "Unassigned", "zh-CN": "尚未分配", ja: "未割り当て", ko: "할당되지 않음" })}</p>
                        </TableCell>
                        <TableCell>
                          <Badge variant={user.disabled ? "secondary" : "outline"}>
                            {user.disabled
                              ? t({ en: "Disabled", "zh-CN": "已停用", ja: "無効", ko: "비활성화됨" })
                              : t({ en: "Active", "zh-CN": "正常", ja: "有効", ko: "활성" })}
                          </Badge>
                        </TableCell>
                        <TableCell className="pr-4 text-right text-muted-foreground">
                          {formatDateTime(user.created_at_ms, locale)}
                        </TableCell>
                      </TableRow>
                    );
                  })
                : null}
            </TableBody>
          </Table>
        </div>
      )}

      <Dialog open={createOpen} onOpenChange={handleDialogChange}>
        <DialogContent className="max-h-[calc(100dvh-2rem)] w-[calc(100%-2rem)] overflow-y-auto sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>{t({ en: "Add user", "zh-CN": "添加用户", ja: "ユーザーを追加", ko: "사용자 추가" })}</DialogTitle>
            <DialogDescription>
              {t({
                en: "The user will receive a personal workspace and default project, and can sign in with their own account.",
                "zh-CN": "新用户将获得个人工作区和默认项目，可使用自己的账号登录。",
                ja: "新しいユーザーには個人ワークスペースと既定のプロジェクトが付与され、自分のアカウントでサインインできます。",
                ko: "새 사용자에게 개인 워크스페이스와 기본 프로젝트가 제공되며 본인 계정으로 로그인할 수 있습니다.",
              })}
            </DialogDescription>
          </DialogHeader>
          <form className="space-y-4" onSubmit={createUser}>
            <div className="space-y-2">
              <Label htmlFor="new-user-name">{t({ en: "Name", "zh-CN": "姓名", ja: "名前", ko: "이름" })}</Label>
              <Input
                id="new-user-name"
                value={form.displayName}
                onChange={(event) => setForm((current) => ({ ...current, displayName: event.target.value }))}
                autoComplete="name"
                maxLength={128}
                autoFocus
                required
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="new-user-email">{t({ en: "Email", "zh-CN": "邮箱", ja: "メール", ko: "이메일" })}</Label>
              <Input
                id="new-user-email"
                type="email"
                value={form.email}
                onChange={(event) => setForm((current) => ({ ...current, email: event.target.value }))}
                autoComplete="email"
                maxLength={254}
                required
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="new-user-password">{t({ en: "Initial password", "zh-CN": "初始密码", ja: "初期パスワード", ko: "초기 비밀번호" })}</Label>
              <Input
                id="new-user-password"
                type="password"
                value={form.password}
                onChange={(event) => setForm((current) => ({ ...current, password: event.target.value }))}
                autoComplete="new-password"
                minLength={12}
                maxLength={256}
                required
              />
              <p className="text-xs text-muted-foreground">
                {t({
                  en: "Use at least 12 characters and share it with the user through a secure channel.",
                  "zh-CN": "至少 12 个字符，请通过安全方式告知用户。",
                  ja: "12 文字以上にし、安全な方法でユーザーに共有してください。",
                  ko: "12자 이상을 사용하고 안전한 방법으로 사용자에게 전달하세요.",
                })}
              </p>
            </div>
            {createError ? (
              <p className="text-sm text-destructive" role="alert">{createError}</p>
            ) : null}
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => handleDialogChange(false)} disabled={createPending}>
                {t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}
              </Button>
              <Button
                type="submit"
                disabled={
                  createPending ||
                  !form.email.trim() ||
                  !form.displayName.trim() ||
                  form.password.length < 12
                }
              >
                {createPending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <Plus aria-hidden="true" />}
                {t({ en: "Add user", "zh-CN": "添加用户", ja: "ユーザーを追加", ko: "사용자 추가" })}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </section>
  );
}

function defaultOrganization(user: IdentityUserAccess) {
  return user.organizations.find((organization) => organization.is_personal) ?? user.organizations[0];
}

function defaultProject(user: IdentityUserAccess, organizationId?: string) {
  return (
    user.projects.find((project) => project.is_default && project.organization_id === organizationId) ??
    user.projects.find((project) => project.is_default) ??
    user.projects[0]
  );
}

function formatRole(t: Translate, role: string) {
  const labels: Record<string, Parameters<Translate>[0]> = {
    platform_owner: { en: "Platform admin", "zh-CN": "平台管理员", ja: "プラットフォーム管理者", ko: "플랫폼 관리자" },
    organization_owner: { en: "Workspace owner", "zh-CN": "工作区所有者", ja: "ワークスペース所有者", ko: "워크스페이스 소유자" },
    organization_admin: { en: "Workspace admin", "zh-CN": "工作区管理员", ja: "ワークスペース管理者", ko: "워크스페이스 관리자" },
    project_owner: { en: "Project owner", "zh-CN": "项目负责人", ja: "プロジェクト所有者", ko: "프로젝트 소유자" },
    member: { en: "Member", "zh-CN": "成员", ja: "メンバー", ko: "멤버" },
  };
  return t(labels[role] ?? labels.member);
}

function formatDateTime(timestampMs: number, locale: string) {
  if (!Number.isFinite(timestampMs)) return "--";
  return new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(timestampMs));
}

async function responseMessage(response: Response, t: Translate) {
  const fallback =
    response.status >= 500
      ? t({
          en: "The service is temporarily unavailable. Try again later.",
          "zh-CN": "服务暂时不可用，请稍后重试",
          ja: "サービスは一時的に利用できません。しばらくしてから再試行してください。",
          ko: "서비스를 일시적으로 사용할 수 없습니다. 잠시 후 다시 시도하세요.",
        })
      : t({
          en: "The request could not be completed",
          "zh-CN": "请求未完成",
          ja: "リクエストを完了できませんでした",
          ko: "요청을 완료하지 못했습니다",
        });
  try {
    const body = (await response.json()) as {
      error?: { message?: unknown };
      message?: unknown;
    };
    if (typeof body.error?.message === "string" && body.error.message) return body.error.message;
    if (typeof body.message === "string" && body.message) return body.message;
  } catch {
    // Use the user-facing fallback when the gateway did not return JSON.
  }
  return fallback;
}
