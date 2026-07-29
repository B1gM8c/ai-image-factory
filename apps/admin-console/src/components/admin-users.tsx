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
import { consoleFetch } from "@/lib/auth/client";

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
      if (!response.ok) throw new Error(await responseMessage(response));
      const body = (await response.json()) as unknown;
      if (!Array.isArray(body)) throw new Error("用户列表响应格式不正确");
      setUsers(body as IdentityUserAccess[]);
    } catch (reason) {
      if (reason instanceof DOMException && reason.name === "AbortError") return;
      setLoadError(reason instanceof Error ? reason.message : "暂时无法加载用户");
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    void loadUsers(controller.signal);
    return () => controller.abort();
  }, [loadUsers]);

  const filteredUsers = useMemo(() => {
    const keyword = query.trim().toLocaleLowerCase("zh-CN");
    if (!keyword) return users;

    return users.filter((user) => {
      const searchable = [
        user.display_name,
        user.email,
        ...user.roles.map(formatRole),
        ...user.organizations.map((organization) => organization.display_name),
        ...user.projects.map((project) => project.display_name),
      ];
      return searchable.some((value) => value.toLocaleLowerCase("zh-CN").includes(keyword));
    });
  }, [query, users]);

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
      if (!response.ok) throw new Error(await responseMessage(response));

      const created = (await response.json()) as IdentityUserAccess;
      setUsers((current) => [
        created,
        ...current.filter((user) => user.user_id !== created.user_id),
      ]);
      setForm(EMPTY_FORM);
      setCreateOpen(false);
      toast.success("用户已添加");
    } catch (reason) {
      setCreateError(reason instanceof Error ? reason.message : "用户创建失败，请稍后重试");
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
            aria-label="搜索用户"
            placeholder="搜索姓名、邮箱或工作区"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
        </div>
        <div className="flex items-center justify-between gap-3 sm:justify-end">
          <span className="text-sm text-muted-foreground">
            {filteredUsers.length} 位用户
          </span>
          <Button onClick={() => setCreateOpen(true)}>
            <Plus aria-hidden="true" />
            添加用户
          </Button>
        </div>
      </div>

      {loadError ? (
        <div
          className="flex min-h-40 flex-col items-center justify-center gap-3 px-6 py-10 text-center"
          role="alert"
        >
          <div>
            <p className="font-medium">用户列表加载失败</p>
            <p className="mt-1 text-sm text-muted-foreground">{loadError}</p>
          </div>
          <Button variant="outline" size="sm" onClick={() => void loadUsers()}>
            <RefreshCw aria-hidden="true" />
            重新加载
          </Button>
        </div>
      ) : (
        <div className="max-w-full overflow-hidden">
          <Table className="min-w-[900px]">
            <TableHeader>
              <TableRow>
                <TableHead className="pl-4">用户</TableHead>
                <TableHead>角色</TableHead>
                <TableHead>默认工作区</TableHead>
                <TableHead>默认项目</TableHead>
                <TableHead>状态</TableHead>
                <TableHead className="pr-4 text-right">创建时间</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                <TableRow>
                  <TableCell colSpan={6} className="h-40 text-center text-muted-foreground">
                    <LoaderCircle className="mx-auto mb-2 size-5 animate-spin" aria-hidden="true" />
                    正在加载用户
                  </TableCell>
                </TableRow>
              ) : null}
              {!loading && users.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="h-48 text-center">
                    <UserRound className="mx-auto mb-3 size-8 text-muted-foreground" aria-hidden="true" />
                    <p className="font-medium">还没有用户</p>
                    <p className="mt-1 text-sm text-muted-foreground">
                      添加首位用户后，系统会为其准备默认工作区和项目。
                    </p>
                    <Button className="mt-4" size="sm" onClick={() => setCreateOpen(true)}>
                      <Plus aria-hidden="true" />
                      添加用户
                    </Button>
                  </TableCell>
                </TableRow>
              ) : null}
              {!loading && users.length > 0 && filteredUsers.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="h-40 text-center">
                    <p className="font-medium">没有匹配的用户</p>
                    <p className="mt-1 text-sm text-muted-foreground">尝试使用姓名、邮箱或工作区搜索。</p>
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
                                {formatRole(role)}
                              </Badge>
                            ))}
                          </div>
                        </TableCell>
                        <TableCell>
                          <p className="max-w-52 truncate">{organization?.display_name ?? "尚未分配"}</p>
                          {organization?.is_personal ? (
                            <p className="mt-0.5 text-xs text-muted-foreground">个人工作区</p>
                          ) : null}
                        </TableCell>
                        <TableCell>
                          <p className="max-w-52 truncate">{project?.display_name ?? "尚未分配"}</p>
                        </TableCell>
                        <TableCell>
                          <Badge variant={user.disabled ? "secondary" : "outline"}>
                            {user.disabled ? "已停用" : "正常"}
                          </Badge>
                        </TableCell>
                        <TableCell className="pr-4 text-right text-muted-foreground">
                          {formatDateTime(user.created_at_ms)}
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
            <DialogTitle>添加用户</DialogTitle>
            <DialogDescription>
              新用户将获得个人工作区和默认项目，可使用自己的账号登录。
            </DialogDescription>
          </DialogHeader>
          <form className="space-y-4" onSubmit={createUser}>
            <div className="space-y-2">
              <Label htmlFor="new-user-name">姓名</Label>
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
              <Label htmlFor="new-user-email">邮箱</Label>
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
              <Label htmlFor="new-user-password">初始密码</Label>
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
              <p className="text-xs text-muted-foreground">至少 12 个字符，请通过安全方式告知用户。</p>
            </div>
            {createError ? (
              <p className="text-sm text-destructive" role="alert">{createError}</p>
            ) : null}
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => handleDialogChange(false)} disabled={createPending}>
                取消
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
                添加用户
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

function formatRole(role: string) {
  const labels: Record<string, string> = {
    platform_owner: "平台管理员",
    organization_owner: "工作区所有者",
    organization_admin: "工作区管理员",
    project_owner: "项目负责人",
    member: "成员",
  };
  return labels[role] ?? "成员";
}

function formatDateTime(timestampMs: number) {
  if (!Number.isFinite(timestampMs)) return "--";
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(timestampMs));
}

async function responseMessage(response: Response) {
  const fallback = response.status >= 500 ? "服务暂时不可用，请稍后重试" : "请求未完成";
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
