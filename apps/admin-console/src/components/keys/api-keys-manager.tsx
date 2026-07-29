"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import {
  Check,
  Copy,
  FolderPlus,
  KeyRound,
  LoaderCircle,
  MoreHorizontal,
  Pencil,
  Plus,
  RefreshCw,
  Trash2,
} from "lucide-react";
import { toast } from "sonner";
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
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
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
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useConsoleSession } from "@/components/auth/console-session-provider";
import { ProjectCreateDialog } from "@/components/projects/project-create-dialog";
import { consoleFetch } from "@/lib/auth/client";

type ConsoleProviderRoute = {
  route_id: string;
  display_name: string;
  provider_id: string;
  operation_id: string;
  route_kind: "group";
};

type ConsoleProviderRoutesSnapshot = {
  as_of_ms: number;
  routes: ConsoleProviderRoute[];
};

type Project = {
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

type ApiKey = {
  id: string;
  name: string;
  redacted_value: string;
  created_at: number;
  last_used_at: number | null;
  expires_at: number | null;
  status:
    | "active"
    | "expired"
    | "owner_access_lost"
    | "project_user_keys_disabled";
  owner_project_access: "active" | "inactive";
  owner: {
    type: "user" | "service_account";
    service_account: { id: string; name: string } | null;
    user: { id: string; name: string; email: string } | null;
  };
  permission_mode: PermissionMode;
  permissions: Partial<Record<PermissionResource, PermissionLevel>>;
  provider_routes: Array<{
    route_id: string;
    route_revision: number;
    display_name: string;
    route_kind: "account" | "group";
    provider_id: string;
    operation_id: string;
    model_count: number;
  }>;
};

type ListResponse<T> = {
  object: "list";
  data: T[];
  first_id: string | null;
  last_id: string | null;
  has_more: boolean;
};

type CreatedServiceAccount = {
  id: string;
  name: string;
  api_key: { id: string; value: string; name: string; created_at: number };
};

type CreatedApiKey = {
  id: string;
  value: string;
  name: string;
  created_at: number;
};

type RotatedApiKey = {
  replaced_api_key_id: string;
  api_key: CreatedApiKey;
};

type CreatedSecret = CreatedApiKey & {
  owner_type: "user" | "service_account";
};

type PermissionMode = "all" | "restricted" | "read_only";
type PermissionLevel = "none" | "read" | "write";
type PermissionResource = "models" | "images" | "videos" | "files" | "batches";

type UnknownOutcome = {
  title: string;
  message: string;
};

const PAGE_SIZE = 100;
const STANDARD_SERVICE = "standard";

export function ApiKeysManager() {
  const {
    activeWorkspace,
    loading: sessionLoading,
    organizations,
    projects: sessionProjects,
    reload,
    selectWorkspace,
    user,
  } = useConsoleSession();
  const projectsRequest = useRef<AbortController | null>(null);
  const keysRequest = useRef<AbortController | null>(null);
  const secretRef = useRef<CreatedSecret | null>(null);

  const [projects, setProjects] = useState<Project[]>([]);
  const [routes, setRoutes] = useState<ConsoleProviderRoute[]>([]);
  const [selectedRouteId, setSelectedRouteId] = useState(STANDARD_SERVICE);
  const [routesLoading, setRoutesLoading] = useState(true);
  const [projectsLoading, setProjectsLoading] = useState(true);
  const [projectError, setProjectError] = useState<string | null>(null);

  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [keysLoading, setKeysLoading] = useState(false);
  const [keysLoadingMore, setKeysLoadingMore] = useState(false);
  const [keysError, setKeysError] = useState<string | null>(null);
  const [keysHasMore, setKeysHasMore] = useState(false);
  const [keysLastId, setKeysLastId] = useState<string | null>(null);

  const [projectCreateOpen, setProjectCreateOpen] = useState(false);
  const [serviceCreateOpen, setServiceCreateOpen] = useState(false);
  const [ownerType, setOwnerType] = useState<"user" | "service_account">("user");
  const [serviceName, setServiceName] = useState("");
  const [permissionMode, setPermissionMode] = useState<PermissionMode>("all");
  const [permissions, setPermissions] = useState<Record<PermissionResource, PermissionLevel>>({
    models: "read",
    images: "write",
    videos: "write",
    files: "write",
    batches: "write",
  });
  const [serviceCreating, setServiceCreating] = useState(false);
  const [created, setCreated] = useState<CreatedSecret | null>(null);
  const [copied, setCopied] = useState(false);
  const [revokeTarget, setRevokeTarget] = useState<ApiKey | null>(null);
  const [revokePending, setRevokePending] = useState(false);
  const [editTarget, setEditTarget] = useState<ApiKey | null>(null);
  const [editName, setEditName] = useState("");
  const [editPermissionMode, setEditPermissionMode] = useState<PermissionMode>("all");
  const [editPermissions, setEditPermissions] = useState<Record<PermissionResource, PermissionLevel>>({
    models: "read",
    images: "write",
    videos: "write",
    files: "write",
    batches: "write",
  });
  const [editPending, setEditPending] = useState(false);
  const [rotateTarget, setRotateTarget] = useState<ApiKey | null>(null);
  const [rotatePending, setRotatePending] = useState(false);
  const [unknownOutcome, setUnknownOutcome] = useState<UnknownOutcome | null>(null);
  const selectedProjectId =
    activeWorkspace?.kind === "project" ? activeWorkspace.id ?? "" : "";
  const selectedMembership = sessionProjects.find(
    (project) => project.id === selectedProjectId,
  );
  const canManageSelectedProject = Boolean(
    selectedProjectId &&
      (user?.roles.includes("platform_owner") ||
        selectedMembership?.role === "owner" ||
        organizations.some(
          (organization) =>
            organization.id === selectedMembership?.organization_id &&
            organization.role === "owner",
        )),
  );
  const canOwnSelectedProject =
    !selectedProjectId ||
    sessionProjects.some((project) => project.id === selectedProjectId);

  const clearSecret = useCallback(() => {
    secretRef.current = null;
    setCreated(null);
    setCopied(false);
  }, []);

  const loadRoutes = useCallback(async () => {
    setRoutesLoading(true);
    try {
      const response = await consoleFetch("/api/gateway/v1/console/provider-routes");
      if (!response.ok) throw new Error(await responseMessage(response));
      const body = (await response.json()) as ConsoleProviderRoutesSnapshot;
      const available = body.routes.filter((route) => route.route_kind === "group");
      setRoutes(available);
      setSelectedRouteId((current) => (
        current === STANDARD_SERVICE || available.some((route) => route.route_id === current)
          ? current
          : STANDARD_SERVICE
      ));
    } catch (reason) {
      setRoutes([]);
      setSelectedRouteId(STANDARD_SERVICE);
      toast.error(reason instanceof Error ? reason.message : "账户组加载失败，仍可使用标准服务");
    } finally {
      setRoutesLoading(false);
    }
  }, []);

  const loadProjects = useCallback(async () => {
    projectsRequest.current?.abort();
    const controller = new AbortController();
    projectsRequest.current = controller;
    setProjectsLoading(true);
    setProjectError(null);

    try {
      const loaded: Project[] = [];
      const seenCursors = new Set<string>();
      let after: string | null = null;
      let hasMore = false;

      do {
        const response = await consoleFetch(projectCollectionPath(after), { signal: controller.signal });
        if (!response.ok) throw new Error(await responseMessage(response));
        const body = (await response.json()) as ListResponse<Project>;
        loaded.push(...body.data);
        hasMore = body.has_more;
        after = body.last_id;
        if (hasMore && !after) throw new Error("项目分页响应缺少 last_id");
        if (hasMore && after && seenCursors.has(after)) throw new Error("项目分页游标未向前推进");
        if (after) seenCursors.add(after);
      } while (hasMore);

      if (controller.signal.aborted) return;
      setProjects(loaded);
    } catch (reason) {
      if (isAbortError(reason)) return;
      setProjects([]);
      setProjectError(reason instanceof Error ? reason.message : "项目加载失败");
    } finally {
      if (projectsRequest.current === controller) {
        projectsRequest.current = null;
        setProjectsLoading(false);
      }
    }
  }, []);

  const loadKeys = useCallback(async (projectId: string, after: string | null = null) => {
    if (!projectId) return;
    keysRequest.current?.abort();
    const controller = new AbortController();
    keysRequest.current = controller;
    const appending = Boolean(after);
    if (appending) setKeysLoadingMore(true);
    else setKeysLoading(true);
    setKeysError(null);

    try {
      const response = await consoleFetch(keyCollectionPath(projectId, after), { signal: controller.signal });
      if (!response.ok) throw new Error(await responseMessage(response));
      const body = (await response.json()) as ListResponse<ApiKey>;
      if (controller.signal.aborted) return;

      setKeys((current) => appending ? mergeKeys(current, body.data) : body.data);
      setKeysHasMore(body.has_more);
      setKeysLastId(body.last_id);
    } catch (reason) {
      if (isAbortError(reason)) return;
      if (!appending) setKeys([]);
      setKeysError(reason instanceof Error ? reason.message : "API Key 加载失败");
    } finally {
      if (keysRequest.current === controller) {
        keysRequest.current = null;
        setKeysLoading(false);
        setKeysLoadingMore(false);
      }
    }
  }, []);

  useEffect(() => {
    void loadProjects();
    return () => projectsRequest.current?.abort();
  }, [loadProjects]);

  useEffect(() => {
    if (sessionLoading || projectsLoading || projectError || projects.length === 0) return;
    if (
      selectedProjectId &&
      projects.some(
        (project) => project.id === selectedProjectId && project.status === "active",
      )
    ) {
      return;
    }
    const firstActiveProject = projects.find((project) => project.status === "active");
    if (firstActiveProject) {
      selectWorkspace(`project:${firstActiveProject.id}`);
    }
  }, [
    projectError,
    projects,
    projectsLoading,
    selectWorkspace,
    selectedProjectId,
    sessionLoading,
  ]);

  useEffect(() => {
    if (canManageSelectedProject) void loadRoutes();
    else {
      setRoutes([]);
      setRoutesLoading(false);
      setSelectedRouteId(STANDARD_SERVICE);
    }
  }, [canManageSelectedProject, loadRoutes, selectedProjectId]);

  useEffect(() => {
    if (!canManageSelectedProject) {
      setOwnerType("user");
    } else if (canOwnSelectedProject) {
      setOwnerType("user");
    } else if (!canOwnSelectedProject) {
      setOwnerType("service_account");
    }
  }, [canManageSelectedProject, canOwnSelectedProject]);

  useEffect(() => {
    keysRequest.current?.abort();
    setKeys([]);
    setKeysHasMore(false);
    setKeysLastId(null);
    setKeysError(null);
    if (selectedProjectId) void loadKeys(selectedProjectId);
  }, [loadKeys, selectedProjectId]);

  useEffect(() => {
    const handlePageHide = () => clearSecret();
    window.addEventListener("pagehide", handlePageHide);
    return () => {
      window.removeEventListener("pagehide", handlePageHide);
      keysRequest.current?.abort();
      secretRef.current = null;
    };
  }, [clearSecret]);

  async function createApiKey() {
    const name = serviceName.trim();
    if (
      !selectedProjectId ||
      (ownerType === "user" && !canOwnSelectedProject) ||
      (ownerType === "user" && selectedProject?.user_api_keys_disabled) ||
      (ownerType === "service_account" &&
        (!canManageSelectedProject || !name || !selectedRouteId))
    ) return;
    setServiceCreating(true);
    setUnknownOutcome(null);
    try {
      const userOwned = ownerType === "user";
      const response = await consoleFetch(
        userOwned
          ? apiKeyCollectionPath(selectedProjectId)
          : serviceAccountCollectionPath(selectedProjectId),
        {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(userOwned
          ? {
              ...(name ? { name } : {}),
              ...permissionRequest(permissionMode, permissions),
            }
          : (
              selectedRouteId === STANDARD_SERVICE
                ? {
                    name,
                    ...permissionRequest(permissionMode, permissions),
                  }
                : {
                    name,
                    route_id: selectedRouteId,
                    ...permissionRequest(permissionMode, permissions),
                  }
            )),
        },
      );
      if (!response.ok) {
        const message = await responseMessage(response);
        if (isUnknownMutationResponse(response)) {
          showUnknownOutcome("API Key 创建结果未知", message);
          setServiceCreateOpen(false);
          return;
        }
        toast.error(message);
        return;
      }

      const body = (await response.json()) as CreatedApiKey | CreatedServiceAccount;
      const apiKey = userOwned ? body as CreatedApiKey : (body as CreatedServiceAccount).api_key;
      const createdSecret: CreatedSecret = { ...apiKey, owner_type: ownerType };
      secretRef.current = createdSecret;
      setCreated(createdSecret);
      setCopied(false);
      setServiceCreateOpen(false);
      setServiceName("");
      await loadKeys(selectedProjectId);
      toast.success(userOwned ? "个人 API Key 已创建" : "服务账户 API Key 已创建");
    } catch (reason) {
      showUnknownOutcome(
        "API Key 创建结果未知",
        reason instanceof Error ? reason.message : "请求未返回明确结果",
      );
      setServiceCreateOpen(false);
    } finally {
      setServiceCreating(false);
    }
  }

  async function revokeApiKey() {
    if (!revokeTarget || !selectedProjectId) return;
    const target = revokeTarget;
    setRevokePending(true);
    setUnknownOutcome(null);
    try {
      const response = await consoleFetch(
        apiKeyPath(selectedProjectId, target.id),
        { method: "DELETE" },
      );
      if (!response.ok) {
        const message = await responseMessage(response);
        if (isUnknownMutationResponse(response)) {
          showUnknownOutcome("API Key 吊销结果未知", message);
          setRevokeTarget(null);
          return;
        }
        toast.error(message);
        return;
      }

      setRevokeTarget(null);
      await loadKeys(selectedProjectId);
      toast.success("API Key 已删除");
    } catch (reason) {
      showUnknownOutcome(
        "API Key 吊销结果未知",
        reason instanceof Error ? reason.message : "请求未返回明确结果",
      );
      setRevokeTarget(null);
    } finally {
      setRevokePending(false);
    }
  }

  function openEditApiKey(key: ApiKey) {
    if (!canEditApiKey(key)) return;
    setEditTarget(key);
    setEditName(key.name);
    setEditPermissionMode(key.permission_mode);
    setEditPermissions(permissionValues(key.permissions));
  }

  async function updateApiKey() {
    if (
      !editTarget ||
      !selectedProjectId ||
      !editName.trim() ||
      !canEditApiKey(editTarget)
    ) return;
    setEditPending(true);
    setUnknownOutcome(null);
    try {
      const response = await consoleFetch(
        apiKeyPath(selectedProjectId, editTarget.id),
        {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            name: editName.trim(),
            ...permissionRequest(editPermissionMode, editPermissions),
          }),
        },
      );
      if (!response.ok) {
        const message = await responseMessage(response);
        if (isUnknownMutationResponse(response)) {
          showUnknownOutcome("API Key 更新结果未知", message);
          setEditTarget(null);
          return;
        }
        toast.error(message);
        return;
      }
      setEditTarget(null);
      await loadKeys(selectedProjectId);
      toast.success("API Key 已更新");
    } catch (reason) {
      showUnknownOutcome(
        "API Key 更新结果未知",
        reason instanceof Error ? reason.message : "请求未返回明确结果",
      );
      setEditTarget(null);
    } finally {
      setEditPending(false);
    }
  }

  async function rotateApiKey() {
    if (!rotateTarget || !selectedProjectId || !canEditApiKey(rotateTarget)) return;
    const target = rotateTarget;
    setRotatePending(true);
    setUnknownOutcome(null);
    try {
      const response = await consoleFetch(
        apiKeyRotationPath(selectedProjectId, target.id),
        { method: "POST" },
      );
      if (!response.ok) {
        const message = await responseMessage(response);
        if (isUnknownMutationResponse(response)) {
          showUnknownOutcome("API Key 轮换结果未知", message);
          setRotateTarget(null);
          return;
        }
        toast.error(message);
        return;
      }
      const body = (await response.json()) as RotatedApiKey;
      const createdSecret: CreatedSecret = {
        ...body.api_key,
        owner_type: target.owner.type,
      };
      secretRef.current = createdSecret;
      setCreated(createdSecret);
      setCopied(false);
      setRotateTarget(null);
      await loadKeys(selectedProjectId);
      toast.success("API Key 已轮换，旧 Key 已失效");
    } catch (reason) {
      showUnknownOutcome(
        "API Key 轮换结果未知",
        reason instanceof Error ? reason.message : "请求未返回明确结果",
      );
      setRotateTarget(null);
    } finally {
      setRotatePending(false);
    }
  }

  async function copySecret() {
    if (!secretRef.current) return;
    try {
      await navigator.clipboard.writeText(secretRef.current.value);
      setCopied(true);
      toast.success("密钥已复制");
    } catch {
      toast.error("无法访问剪贴板");
    }
  }

  function showUnknownOutcome(title: string, detail: string) {
    setUnknownOutcome({
      title,
      message: `${detail}。请刷新当前列表确认状态；不要直接重复提交。`,
    });
  }

  const selectedProject = projects.find((project) => project.id === selectedProjectId) ?? null;
  const mutationPending =
    serviceCreating || editPending || rotatePending || revokePending;
  const canEditApiKey = (key: ApiKey) =>
    apiKeyIsUsable(key) &&
    (key.owner.type === "service_account"
      ? canManageSelectedProject
      : key.owner.user?.id === user?.id);

  return (
    <>
      <div className="flex flex-col gap-3 border-b bg-muted/20 p-3 lg:flex-row lg:items-end">
        <div className="min-w-0 flex-1 space-y-1.5">
          <Label htmlFor="project-select">项目</Label>
          <Select
            value={selectedProjectId}
            onValueChange={(projectId) => selectWorkspace(`project:${projectId}`)}
            disabled={projectsLoading || projects.length === 0 || mutationPending}
          >
            <SelectTrigger id="project-select">
              <SelectValue placeholder={projectsLoading ? "正在加载项目" : "选择项目"} />
            </SelectTrigger>
            <SelectContent>
              {projects.map((project) => (
                <SelectItem key={project.id} value={project.id} disabled={project.status === "archived"}>
                  {project.name} · {project.id}{project.status === "archived" ? " · 已归档" : ""}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button
            variant="outline"
            size="icon"
            aria-label="刷新项目"
            title="刷新项目"
            onClick={() => void loadProjects()}
            disabled={projectsLoading || mutationPending}
          >
            {projectsLoading ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <RefreshCw aria-hidden="true" />}
          </Button>
          <Button variant="outline" onClick={() => setProjectCreateOpen(true)} disabled={mutationPending}>
            <FolderPlus aria-hidden="true" />
            新建项目
          </Button>
          <Button
            onClick={() => {
              if (selectedProject?.user_api_keys_disabled) {
                setOwnerType("service_account");
              }
              setServiceCreateOpen(true);
            }}
            disabled={
              !selectedProject ||
              (ownerType === "user" && !canOwnSelectedProject) ||
              (ownerType === "service_account" && routesLoading) ||
              mutationPending
            }
          >
            <Plus aria-hidden="true" />
            创建 API Key
          </Button>
        </div>
      </div>

      {projectError ? <ErrorBanner message={projectError} /> : null}
      {unknownOutcome ? (
        <div className="flex flex-col gap-2 border-b border-amber-300 bg-amber-50 px-4 py-3 text-sm text-amber-950 sm:flex-row sm:items-center sm:justify-between" role="alert">
          <div>
            <p className="font-medium">{unknownOutcome.title}</p>
            <p className="mt-0.5 text-amber-800">{unknownOutcome.message}</p>
          </div>
          <Button
            variant="outline"
            size="sm"
            className="shrink-0 bg-background"
            onClick={() => {
              setUnknownOutcome(null);
              if (selectedProjectId) void loadKeys(selectedProjectId);
              else void loadProjects();
            }}
          >
            <RefreshCw aria-hidden="true" />
            刷新状态
          </Button>
        </div>
      ) : null}
      {keysError ? <ErrorBanner message={keysError} /> : null}

      <div className="overflow-x-auto">
        <Table className="min-w-[1120px]">
          <TableHeader>
            <TableRow>
              <TableHead>名称</TableHead>
              <TableHead>状态</TableHead>
              <TableHead>Tracking ID</TableHead>
              <TableHead>Secret Key</TableHead>
              <TableHead>所有者</TableHead>
              <TableHead>权限</TableHead>
              <TableHead>创建时间</TableHead>
              <TableHead>最后使用</TableHead>
              <TableHead className="w-20 text-right">操作</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {keysLoading ? (
              <TableRow>
                <TableCell colSpan={9} className="h-32 text-center text-muted-foreground">
                  <LoaderCircle className="mx-auto mb-2 size-5 animate-spin" aria-hidden="true" />
                  正在加载 API Key
                </TableCell>
              </TableRow>
            ) : null}
            {!keysLoading && selectedProjectId && keys.length === 0 ? (
              <TableRow>
                <TableCell colSpan={9} className="h-32 text-center text-muted-foreground">该项目暂无可用 API Key</TableCell>
              </TableRow>
            ) : null}
            {!keysLoading && !selectedProjectId && !projectError ? (
              <TableRow>
                <TableCell colSpan={9} className="h-32 text-center text-muted-foreground">暂无可管理的活动项目</TableCell>
              </TableRow>
            ) : null}
            {keys.map((key) => (
              <TableRow key={key.id}>
                <TableCell>
                  <p className="font-medium">{key.name}</p>
                  {canManageSelectedProject && key.provider_routes.length > 0 ? (
                    <p className="mt-0.5 text-xs text-muted-foreground">
                      {key.provider_routes.map((route) => route.display_name).join("、")}
                    </p>
                  ) : null}
                </TableCell>
                <TableCell>
                  <Badge variant={apiKeyIsUsable(key) ? "default" : "secondary"}>
                    {apiKeyStatusLabel(key)}
                  </Badge>
                  {key.status === "owner_access_lost" ? (
                    <p className="mt-1 text-xs text-muted-foreground">
                      所有者已失去项目访问权限
                    </p>
                  ) : key.status === "project_user_keys_disabled" ? (
                    <p className="mt-1 text-xs text-muted-foreground">
                      项目已禁用个人 Key
                    </p>
                  ) : null}
                </TableCell>
                <TableCell className="font-mono text-xs">{key.id}</TableCell>
                <TableCell className="font-mono text-xs">{key.redacted_value}</TableCell>
                <TableCell>
                  <p>{apiKeyOwnerName(key)}</p>
                  <p className="mt-0.5 text-xs text-muted-foreground">
                    {key.owner.type === "user" ? "用户" : "服务账户"}
                  </p>
                </TableCell>
                <TableCell>{permissionModeLabel(key.permission_mode)}</TableCell>
                <TableCell>{formatUnix(key.created_at)}</TableCell>
                <TableCell>{key.last_used_at ? formatUnix(key.last_used_at) : "从未使用"}</TableCell>
                <TableCell className="text-right">
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button
                        variant="ghost"
                        size="icon"
                        aria-label={`管理 API Key ${key.name}`}
                        disabled={mutationPending}
                      >
                        <MoreHorizontal aria-hidden="true" />
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end">
                      {canEditApiKey(key) ? (
                        <>
                          <DropdownMenuItem onSelect={() => openEditApiKey(key)}>
                            <Pencil aria-hidden="true" />
                            编辑名称与权限
                          </DropdownMenuItem>
                          <DropdownMenuItem onSelect={() => setRotateTarget(key)}>
                            <RefreshCw aria-hidden="true" />
                            轮换密钥
                          </DropdownMenuItem>
                          <DropdownMenuSeparator />
                        </>
                      ) : null}
                      <DropdownMenuItem
                        className="text-destructive focus:text-destructive"
                        onSelect={() => setRevokeTarget(key)}
                      >
                        <Trash2 aria-hidden="true" />
                        吊销密钥
                      </DropdownMenuItem>
                    </DropdownMenuContent>
                  </DropdownMenu>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>

      {keysHasMore ? (
        <div className="flex justify-center border-t p-3">
          <Button
            variant="outline"
            size="sm"
            onClick={() => void loadKeys(selectedProjectId, keysLastId)}
            disabled={keysLoadingMore || !keysLastId}
          >
            {keysLoadingMore ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <Plus aria-hidden="true" />}
            加载更多
          </Button>
        </div>
      ) : null}

      <ProjectCreateDialog
        open={projectCreateOpen}
        onOpenChange={setProjectCreateOpen}
        onCreated={async () => {
          await Promise.all([loadProjects(), reload()]);
        }}
      />

      <Dialog open={serviceCreateOpen} onOpenChange={(open) => !serviceCreating && setServiceCreateOpen(open)}>
        <DialogContent className="sm:max-w-xl">
          <DialogHeader>
            <DialogTitle>创建 API Key</DialogTitle>
            <DialogDescription>
              为项目 <code>{selectedProject?.name ?? selectedProjectId}</code> 创建新的访问密钥。
            </DialogDescription>
          </DialogHeader>
          {canManageSelectedProject ? (
            <div className="space-y-2">
              <Label>所有者</Label>
              <Tabs value={ownerType} onValueChange={(value) => setOwnerType(value as typeof ownerType)}>
                <TabsList className="grid w-full grid-cols-2">
                  <TabsTrigger
                    value="user"
                    disabled={
                      !canOwnSelectedProject ||
                      selectedProject?.user_api_keys_disabled
                    }
                  >
                    你
                  </TabsTrigger>
                  <TabsTrigger value="service_account">服务账户</TabsTrigger>
                </TabsList>
              </Tabs>
              <p className="text-sm text-muted-foreground">
                {ownerType === "user"
                  ? "个人 Key 随你的项目成员资格生效或失效。"
                  : selectedProject?.user_api_keys_disabled
                    ? "此项目已禁用个人 Key；服务账户 Key 仍可正常创建和使用。"
                    : "系统将创建新的机器身份，并同时签发一个 Key。"}
              </p>
            </div>
          ) : null}
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <Label htmlFor="service-name">
                {ownerType === "user" ? "名称" : "服务账户名称"}
              </Label>
              {ownerType === "user" ? <span className="text-xs text-muted-foreground">可选</span> : null}
            </div>
            <Input
              id="service-name"
              value={serviceName}
              onChange={(event) => setServiceName(event.target.value)}
              placeholder={ownerType === "user" ? "例如：本地开发" : "例如：生产环境机器人"}
              maxLength={128}
              autoFocus
            />
          </div>
          {ownerType === "service_account" ? (
            <div className="space-y-2">
              <Label htmlFor="service-route">服务方案</Label>
              <Select value={selectedRouteId} onValueChange={setSelectedRouteId} disabled={routesLoading}>
                <SelectTrigger id="service-route">
                  <SelectValue placeholder="选择服务方案" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={STANDARD_SERVICE}>
                    标准服务 · 平台自动调度
                  </SelectItem>
                  {routes.map((route) => (
                    <SelectItem key={route.route_id} value={route.route_id}>
                      {route.display_name} · {providerName(route.provider_id)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          ) : null}
          <PermissionControls
            mode={permissionMode}
            permissions={permissions}
            onModeChange={setPermissionMode}
            onPermissionChange={(resource, value) =>
              setPermissions((current) => ({ ...current, [resource]: value }))
            }
          />
          <DialogFooter>
            <Button variant="outline" onClick={() => setServiceCreateOpen(false)} disabled={serviceCreating}>取消</Button>
            <Button
              onClick={() => void createApiKey()}
              disabled={
                serviceCreating ||
                (ownerType === "user" &&
                  selectedProject?.user_api_keys_disabled) ||
                (ownerType === "service_account" && !serviceName.trim())
              }
            >
              {serviceCreating ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <KeyRound aria-hidden="true" />}
              创建密钥
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={Boolean(editTarget)}
        onOpenChange={(open) => !open && !editPending && setEditTarget(null)}
      >
        <DialogContent className="sm:max-w-xl">
          <DialogHeader>
            <DialogTitle>编辑 API Key</DialogTitle>
            <DialogDescription>
              修改名称或权限后，现有 Key 明文不变，新的权限立即生效。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-2">
            <Label htmlFor="edit-key-name">名称</Label>
            <Input
              id="edit-key-name"
              value={editName}
              onChange={(event) => setEditName(event.target.value)}
              maxLength={128}
              autoFocus
            />
          </div>
          <PermissionControls
            mode={editPermissionMode}
            permissions={editPermissions}
            onModeChange={setEditPermissionMode}
            onPermissionChange={(resource, value) =>
              setEditPermissions((current) => ({ ...current, [resource]: value }))
            }
            disabled={editPending}
          />
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setEditTarget(null)}
              disabled={editPending}
            >
              取消
            </Button>
            <Button
              onClick={() => void updateApiKey()}
              disabled={editPending || !editName.trim()}
            >
              {editPending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <Pencil aria-hidden="true" />}
              保存更改
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <AlertDialog
        open={Boolean(rotateTarget)}
        onOpenChange={(open) => !open && !rotatePending && setRotateTarget(null)}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>轮换 API Key？</AlertDialogTitle>
            <AlertDialogDescription>
              系统会签发新的 Key，并在同一事务中立即吊销 <strong>{rotateTarget?.name}</strong> 的旧 Key。
              新明文仍只显示一次。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={rotatePending}>取消</AlertDialogCancel>
            <AlertDialogAction
              disabled={rotatePending}
              onClick={(event) => {
                event.preventDefault();
                void rotateApiKey();
              }}
            >
              {rotatePending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <RefreshCw aria-hidden="true" />}
              轮换密钥
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={Boolean(created)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>保存新的 API Key</AlertDialogTitle>
            <AlertDialogDescription>密钥明文仅在本次响应中出现，关闭后无法再次查看。</AlertDialogDescription>
          </AlertDialogHeader>
          {created ? (
            <div className="space-y-3">
              <div className="border bg-muted/40 p-3 font-mono text-xs break-all">{created.value}</div>
              <Button className="w-full" variant="outline" onClick={() => void copySecret()}>
                {copied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
                {copied ? "已复制" : "复制密钥"}
              </Button>
            </div>
          ) : null}
          <AlertDialogFooter>
            <AlertDialogAction onClick={clearSecret}>我已保存并关闭</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={Boolean(revokeTarget)} onOpenChange={(open) => !open && !revokePending && setRevokeTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>删除 API Key？</AlertDialogTitle>
            <AlertDialogDescription>
              <strong>{revokeTarget?.name}</strong> 将立即失效且不能恢复，其他 Key 不受影响。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={revokePending}>取消</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={revokePending}
              onClick={(event) => {
                event.preventDefault();
                void revokeApiKey();
              }}
            >
              {revokePending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <Trash2 aria-hidden="true" />}
              吊销此 Key
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

function PermissionControls({
  mode,
  permissions,
  onModeChange,
  onPermissionChange,
  disabled = false,
}: {
  mode: PermissionMode;
  permissions: Record<PermissionResource, PermissionLevel>;
  onModeChange: (mode: PermissionMode) => void;
  onPermissionChange: (
    resource: PermissionResource,
    value: PermissionLevel,
  ) => void;
  disabled?: boolean;
}) {
  return (
    <div className="space-y-3">
      <Label>权限</Label>
      <Tabs
        value={mode}
        onValueChange={(value) => onModeChange(value as PermissionMode)}
      >
        <TabsList className="grid w-full grid-cols-3">
          <TabsTrigger value="all" disabled={disabled}>全部</TabsTrigger>
          <TabsTrigger value="restricted" disabled={disabled}>受限</TabsTrigger>
          <TabsTrigger value="read_only" disabled={disabled}>只读</TabsTrigger>
        </TabsList>
      </Tabs>
      {mode === "restricted" ? (
        <div className="divide-y rounded-md border">
          <PermissionSelect
            label="模型列表"
            value={permissions.models}
            onValueChange={(value) => onPermissionChange("models", value)}
            disabled={disabled}
          />
          <PermissionSelect
            label="图片 API"
            value={permissions.images}
            onValueChange={(value) => onPermissionChange("images", value)}
            disabled={disabled}
          />
          <PermissionSelect
            label="视频 API"
            value={permissions.videos}
            onValueChange={(value) => onPermissionChange("videos", value)}
            disabled={disabled}
          />
          <PermissionSelect
            label="文件 API"
            value={permissions.files}
            onValueChange={(value) => onPermissionChange("files", value)}
            disabled={disabled}
          />
          <PermissionSelect
            label="Batch API"
            value={permissions.batches}
            onValueChange={(value) => onPermissionChange("batches", value)}
            disabled={disabled}
          />
        </div>
      ) : null}
    </div>
  );
}

function PermissionSelect({
  label,
  value,
  onValueChange,
  disabled = false,
}: {
  label: string;
  value: PermissionLevel;
  onValueChange: (value: PermissionLevel) => void;
  disabled?: boolean;
}) {
  return (
    <div className="flex items-center justify-between gap-4 px-3 py-2.5">
      <span className="text-sm">{label}</span>
      <Select
        value={value}
        onValueChange={(next) => onValueChange(next as PermissionLevel)}
        disabled={disabled}
      >
        <SelectTrigger className="w-32" aria-label={`${label}权限`}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="none">无权限</SelectItem>
          <SelectItem value="read">读取</SelectItem>
          <SelectItem value="write">写入</SelectItem>
        </SelectContent>
      </Select>
    </div>
  );
}

function permissionRequest(
  mode: PermissionMode,
  permissions: Record<PermissionResource, PermissionLevel>,
) {
  return {
    permission_mode: mode,
    permissions: mode === "restricted" ? permissions : {},
  };
}

function apiKeyIsUsable(key: ApiKey) {
  return key.status === "active" && key.owner_project_access === "active";
}

function apiKeyStatusLabel(key: ApiKey) {
  if (key.status === "expired") return "已过期";
  if (key.status === "project_user_keys_disabled") return "项目已禁用";
  if (key.status === "owner_access_lost") return "不可用";
  return "有效";
}

function apiKeyOwnerName(key: ApiKey) {
  return key.owner.type === "user"
    ? key.owner.user?.name ?? "未知用户"
    : key.owner.service_account?.name ?? "未知服务账户";
}

function permissionModeLabel(mode: PermissionMode) {
  switch (mode) {
    case "all":
      return "全部";
    case "restricted":
      return "受限";
    case "read_only":
      return "只读";
  }
}

function permissionValues(
  permissions: Partial<Record<PermissionResource, PermissionLevel>>,
): Record<PermissionResource, PermissionLevel> {
  return {
    models: permissions.models ?? "read",
    images: permissions.images ?? "write",
    videos: permissions.videos ?? "write",
    files: permissions.files ?? "write",
    batches: permissions.batches ?? "write",
  };
}

function ErrorBanner({ message }: { message: string }) {
  return (
    <div className="border-b border-destructive/20 bg-destructive/5 px-4 py-3 text-sm text-destructive" role="alert">
      {message}
    </div>
  );
}

function projectCollectionPath(after: string | null) {
  const params = new URLSearchParams({ limit: String(PAGE_SIZE) });
  if (after) params.set("after", after);
  return `/api/gateway/v1/organization/projects?${params.toString()}`;
}

function keyCollectionPath(projectId: string, after: string | null) {
  const params = new URLSearchParams({ limit: String(PAGE_SIZE) });
  if (after) params.set("after", after);
  return `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/api_keys?${params.toString()}`;
}

function apiKeyCollectionPath(projectId: string) {
  return `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/api_keys`;
}

function serviceAccountCollectionPath(projectId: string) {
  return `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/service_accounts`;
}

function apiKeyPath(projectId: string, apiKeyId: string) {
  return `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/api_keys/${encodeURIComponent(apiKeyId)}`;
}

function apiKeyRotationPath(projectId: string, apiKeyId: string) {
  return `${apiKeyPath(projectId, apiKeyId)}/rotate`;
}

function mergeKeys(current: ApiKey[], incoming: ApiKey[]) {
  const known = new Set(current.map((key) => key.id));
  return [...current, ...incoming.filter((key) => !known.has(key.id))];
}

function providerName(providerId: string) {
  switch (providerId) {
    case "openai-codex":
      return "Codex";
    case "grok-cli":
      return "Grok";
    case "dreamina-cli":
      return "即梦";
    default:
      return providerId;
  }
}

function isAbortError(reason: unknown) {
  return reason instanceof DOMException && reason.name === "AbortError";
}

function isUnknownMutationResponse(response: Response) {
  return response.status >= 500;
}

async function responseMessage(response: Response) {
  const body = (await response.json().catch(() => null)) as
    | { error?: string | { message?: string } }
    | null;
  if (typeof body?.error === "string") return body.error;
  if (body?.error && typeof body.error === "object" && body.error.message) return body.error.message;
  return `请求失败 (${response.status})`;
}

function formatUnix(value: number) {
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(value * 1000));
}
