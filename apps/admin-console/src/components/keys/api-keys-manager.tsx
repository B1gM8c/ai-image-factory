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
import { useI18n } from "@/i18n/locale-provider";
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
type Translate = ReturnType<typeof useI18n>["t"];
type Locale = ReturnType<typeof useI18n>["locale"];

type UnknownOutcome = {
  title: string;
  message: string;
};

const PAGE_SIZE = 100;
const STANDARD_SERVICE = "standard";

export function ApiKeysManager() {
  const { locale, t } = useI18n();
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
      if (!response.ok) throw new Error(await responseMessage(response, t));
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
      toast.error(
        reason instanceof Error
          ? reason.message
          : t({
              en: "Account groups could not be loaded. Standard service is still available.",
              "zh-CN": "账户组加载失败，仍可使用标准服务",
              ja: "アカウントグループを読み込めませんでした。標準サービスは引き続き利用できます。",
              ko: "계정 그룹을 불러오지 못했습니다. 표준 서비스는 계속 사용할 수 있습니다.",
            }),
      );
    } finally {
      setRoutesLoading(false);
    }
  }, [t]);

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
        if (!response.ok) throw new Error(await responseMessage(response, t));
        const body = (await response.json()) as ListResponse<Project>;
        loaded.push(...body.data);
        hasMore = body.has_more;
        after = body.last_id;
        if (hasMore && !after) {
          throw new Error(
            t({
              en: "The project pagination response is missing last_id",
              "zh-CN": "项目分页响应缺少 last_id",
              ja: "プロジェクトのページネーション応答に last_id がありません",
              ko: "프로젝트 페이지네이션 응답에 last_id가 없습니다",
            }),
          );
        }
        if (hasMore && after && seenCursors.has(after)) {
          throw new Error(
            t({
              en: "The project pagination cursor did not advance",
              "zh-CN": "项目分页游标未向前推进",
              ja: "プロジェクトのページネーションカーソルが進みませんでした",
              ko: "프로젝트 페이지네이션 커서가 진행되지 않았습니다",
            }),
          );
        }
        if (after) seenCursors.add(after);
      } while (hasMore);

      if (controller.signal.aborted) return;
      setProjects(loaded);
    } catch (reason) {
      if (isAbortError(reason)) return;
      setProjects([]);
      setProjectError(
        reason instanceof Error
          ? reason.message
          : t({
              en: "Projects could not be loaded",
              "zh-CN": "项目加载失败",
              ja: "プロジェクトを読み込めませんでした",
              ko: "프로젝트를 불러오지 못했습니다",
            }),
      );
    } finally {
      if (projectsRequest.current === controller) {
        projectsRequest.current = null;
        setProjectsLoading(false);
      }
    }
  }, [t]);

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
      if (!response.ok) throw new Error(await responseMessage(response, t));
      const body = (await response.json()) as ListResponse<ApiKey>;
      if (controller.signal.aborted) return;

      setKeys((current) => appending ? mergeKeys(current, body.data) : body.data);
      setKeysHasMore(body.has_more);
      setKeysLastId(body.last_id);
    } catch (reason) {
      if (isAbortError(reason)) return;
      if (!appending) setKeys([]);
      setKeysError(
        reason instanceof Error
          ? reason.message
          : t({
              en: "API keys could not be loaded",
              "zh-CN": "API Key 加载失败",
              ja: "API キーを読み込めませんでした",
              ko: "API 키를 불러오지 못했습니다",
            }),
      );
    } finally {
      if (keysRequest.current === controller) {
        keysRequest.current = null;
        setKeysLoading(false);
        setKeysLoadingMore(false);
      }
    }
  }, [t]);

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
        const message = await responseMessage(response, t);
        if (isUnknownMutationResponse(response)) {
          showUnknownOutcome(
            t({
              en: "API key creation outcome is unknown",
              "zh-CN": "API Key 创建结果未知",
              ja: "API キー作成結果が不明です",
              ko: "API 키 생성 결과를 확인할 수 없습니다",
            }),
            message,
          );
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
      toast.success(
        userOwned
          ? t({
              en: "Personal API key created",
              "zh-CN": "个人 API Key 已创建",
              ja: "個人 API キーを作成しました",
              ko: "개인 API 키가 생성되었습니다",
            })
          : t({
              en: "Service account API key created",
              "zh-CN": "服务账户 API Key 已创建",
              ja: "サービスアカウント API キーを作成しました",
              ko: "서비스 계정 API 키가 생성되었습니다",
            }),
      );
    } catch (reason) {
      showUnknownOutcome(
        t({
          en: "API key creation outcome is unknown",
          "zh-CN": "API Key 创建结果未知",
          ja: "API キー作成結果が不明です",
          ko: "API 키 생성 결과를 확인할 수 없습니다",
        }),
        reason instanceof Error
          ? reason.message
          : t({
              en: "The request did not return a definitive result",
              "zh-CN": "请求未返回明确结果",
              ja: "リクエストから明確な結果が返されませんでした",
              ko: "요청에서 명확한 결과를 반환하지 않았습니다",
            }),
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
        const message = await responseMessage(response, t);
        if (isUnknownMutationResponse(response)) {
          showUnknownOutcome(
            t({
              en: "API key revocation outcome is unknown",
              "zh-CN": "API Key 吊销结果未知",
              ja: "API キー失効結果が不明です",
              ko: "API 키 폐기 결과를 확인할 수 없습니다",
            }),
            message,
          );
          setRevokeTarget(null);
          return;
        }
        toast.error(message);
        return;
      }

      setRevokeTarget(null);
      await loadKeys(selectedProjectId);
      toast.success(
        t({
          en: "API key deleted",
          "zh-CN": "API Key 已删除",
          ja: "API キーを削除しました",
          ko: "API 키가 삭제되었습니다",
        }),
      );
    } catch (reason) {
      showUnknownOutcome(
        t({
          en: "API key revocation outcome is unknown",
          "zh-CN": "API Key 吊销结果未知",
          ja: "API キー失効結果が不明です",
          ko: "API 키 폐기 결과를 확인할 수 없습니다",
        }),
        reason instanceof Error
          ? reason.message
          : t({
              en: "The request did not return a definitive result",
              "zh-CN": "请求未返回明确结果",
              ja: "リクエストから明確な結果が返されませんでした",
              ko: "요청에서 명확한 결과를 반환하지 않았습니다",
            }),
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
        const message = await responseMessage(response, t);
        if (isUnknownMutationResponse(response)) {
          showUnknownOutcome(
            t({
              en: "API key update outcome is unknown",
              "zh-CN": "API Key 更新结果未知",
              ja: "API キー更新結果が不明です",
              ko: "API 키 업데이트 결과를 확인할 수 없습니다",
            }),
            message,
          );
          setEditTarget(null);
          return;
        }
        toast.error(message);
        return;
      }
      setEditTarget(null);
      await loadKeys(selectedProjectId);
      toast.success(
        t({
          en: "API key updated",
          "zh-CN": "API Key 已更新",
          ja: "API キーを更新しました",
          ko: "API 키가 업데이트되었습니다",
        }),
      );
    } catch (reason) {
      showUnknownOutcome(
        t({
          en: "API key update outcome is unknown",
          "zh-CN": "API Key 更新结果未知",
          ja: "API キー更新結果が不明です",
          ko: "API 키 업데이트 결과를 확인할 수 없습니다",
        }),
        reason instanceof Error
          ? reason.message
          : t({
              en: "The request did not return a definitive result",
              "zh-CN": "请求未返回明确结果",
              ja: "リクエストから明確な結果が返されませんでした",
              ko: "요청에서 명확한 결과를 반환하지 않았습니다",
            }),
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
        const message = await responseMessage(response, t);
        if (isUnknownMutationResponse(response)) {
          showUnknownOutcome(
            t({
              en: "API key rotation outcome is unknown",
              "zh-CN": "API Key 轮换结果未知",
              ja: "API キーローテーション結果が不明です",
              ko: "API 키 교체 결과를 확인할 수 없습니다",
            }),
            message,
          );
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
      toast.success(
        t({
          en: "API key rotated. The old key is no longer valid.",
          "zh-CN": "API Key 已轮换，旧 Key 已失效",
          ja: "API キーをローテーションしました。古いキーは無効です。",
          ko: "API 키가 교체되었으며 이전 키는 더 이상 유효하지 않습니다.",
        }),
      );
    } catch (reason) {
      showUnknownOutcome(
        t({
          en: "API key rotation outcome is unknown",
          "zh-CN": "API Key 轮换结果未知",
          ja: "API キーローテーション結果が不明です",
          ko: "API 키 교체 결과를 확인할 수 없습니다",
        }),
        reason instanceof Error
          ? reason.message
          : t({
              en: "The request did not return a definitive result",
              "zh-CN": "请求未返回明确结果",
              ja: "リクエストから明確な結果が返されませんでした",
              ko: "요청에서 명확한 결과를 반환하지 않았습니다",
            }),
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
      toast.success(
        t({
          en: "Key copied",
          "zh-CN": "密钥已复制",
          ja: "キーをコピーしました",
          ko: "키가 복사되었습니다",
        }),
      );
    } catch {
      toast.error(
        t({
          en: "Clipboard access is unavailable",
          "zh-CN": "无法访问剪贴板",
          ja: "クリップボードにアクセスできません",
          ko: "클립보드에 접근할 수 없습니다",
        }),
      );
    }
  }

  function showUnknownOutcome(title: string, detail: string) {
    setUnknownOutcome({
      title,
      message: t(
        {
          en: "{detail}. Refresh the current list to confirm the status. Do not submit the request again yet.",
          "zh-CN": "{detail}。请刷新当前列表确认状态；不要直接重复提交。",
          ja: "{detail}。現在の一覧を更新して状態を確認してください。すぐに再送信しないでください。",
          ko: "{detail}. 현재 목록을 새로 고쳐 상태를 확인하세요. 요청을 바로 다시 제출하지 마세요.",
        },
        { detail },
      ),
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
          <Label htmlFor="project-select">
            {t({
              en: "Project",
              "zh-CN": "项目",
              ja: "プロジェクト",
              ko: "프로젝트",
            })}
          </Label>
          <Select
            value={selectedProjectId}
            onValueChange={(projectId) => selectWorkspace(`project:${projectId}`)}
            disabled={projectsLoading || projects.length === 0 || mutationPending}
          >
            <SelectTrigger id="project-select">
              <SelectValue
                placeholder={
                  projectsLoading
                    ? t({
                        en: "Loading projects",
                        "zh-CN": "正在加载项目",
                        ja: "プロジェクトを読み込み中",
                        ko: "프로젝트 불러오는 중",
                      })
                    : t({
                        en: "Select a project",
                        "zh-CN": "选择项目",
                        ja: "プロジェクトを選択",
                        ko: "프로젝트 선택",
                      })
                }
              />
            </SelectTrigger>
            <SelectContent>
              {projects.map((project) => (
                <SelectItem key={project.id} value={project.id} disabled={project.status === "archived"}>
                  {project.name} · {project.id}
                  {project.status === "archived"
                    ? t({
                        en: " · Archived",
                        "zh-CN": " · 已归档",
                        ja: " · アーカイブ済み",
                        ko: " · 보관됨",
                      })
                    : ""}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button
            variant="outline"
            size="icon"
            aria-label={t({
              en: "Refresh projects",
              "zh-CN": "刷新项目",
              ja: "プロジェクトを更新",
              ko: "프로젝트 새로 고침",
            })}
            title={t({
              en: "Refresh projects",
              "zh-CN": "刷新项目",
              ja: "プロジェクトを更新",
              ko: "프로젝트 새로 고침",
            })}
            onClick={() => void loadProjects()}
            disabled={projectsLoading || mutationPending}
          >
            {projectsLoading ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <RefreshCw aria-hidden="true" />}
          </Button>
          <Button variant="outline" onClick={() => setProjectCreateOpen(true)} disabled={mutationPending}>
            <FolderPlus aria-hidden="true" />
            {t({
              en: "New project",
              "zh-CN": "新建项目",
              ja: "新しいプロジェクト",
              ko: "새 프로젝트",
            })}
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
            {t({
              en: "Create API key",
              "zh-CN": "创建 API Key",
              ja: "API キーを作成",
              ko: "API 키 생성",
            })}
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
            {t({
              en: "Refresh status",
              "zh-CN": "刷新状态",
              ja: "状態を更新",
              ko: "상태 새로 고침",
            })}
          </Button>
        </div>
      ) : null}
      {keysError ? <ErrorBanner message={keysError} /> : null}

      <div className="overflow-x-auto">
        <Table className="min-w-[1120px]">
          <TableHeader>
            <TableRow>
              <TableHead>
                {t({ en: "Name", "zh-CN": "名称", ja: "名前", ko: "이름" })}
              </TableHead>
              <TableHead>
                {t({ en: "Status", "zh-CN": "状态", ja: "状態", ko: "상태" })}
              </TableHead>
              <TableHead>
                {t({
                  en: "Tracking ID",
                  "zh-CN": "追踪 ID",
                  ja: "トラッキング ID",
                  ko: "추적 ID",
                })}
              </TableHead>
              <TableHead>
                {t({
                  en: "Secret Key",
                  "zh-CN": "密钥",
                  ja: "シークレットキー",
                  ko: "비밀 키",
                })}
              </TableHead>
              <TableHead>
                {t({ en: "Owner", "zh-CN": "所有者", ja: "所有者", ko: "소유자" })}
              </TableHead>
              <TableHead>
                {t({
                  en: "Permissions",
                  "zh-CN": "权限",
                  ja: "権限",
                  ko: "권한",
                })}
              </TableHead>
              <TableHead>
                {t({
                  en: "Created",
                  "zh-CN": "创建时间",
                  ja: "作成日時",
                  ko: "생성 시간",
                })}
              </TableHead>
              <TableHead>
                {t({
                  en: "Last used",
                  "zh-CN": "最后使用",
                  ja: "最終使用",
                  ko: "마지막 사용",
                })}
              </TableHead>
              <TableHead className="w-20 text-right">
                {t({
                  en: "Actions",
                  "zh-CN": "操作",
                  ja: "操作",
                  ko: "작업",
                })}
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {keysLoading ? (
              <TableRow>
                <TableCell colSpan={9} className="h-32 text-center text-muted-foreground">
                  <LoaderCircle className="mx-auto mb-2 size-5 animate-spin" aria-hidden="true" />
                  {t({
                    en: "Loading API keys",
                    "zh-CN": "正在加载 API Key",
                    ja: "API キーを読み込み中",
                    ko: "API 키 불러오는 중",
                  })}
                </TableCell>
              </TableRow>
            ) : null}
            {!keysLoading && selectedProjectId && keys.length === 0 ? (
              <TableRow>
                <TableCell colSpan={9} className="h-32 text-center text-muted-foreground">
                  {t({
                    en: "This project has no available API keys",
                    "zh-CN": "该项目暂无可用 API Key",
                    ja: "このプロジェクトには利用可能な API キーがありません",
                    ko: "이 프로젝트에는 사용 가능한 API 키가 없습니다",
                  })}
                </TableCell>
              </TableRow>
            ) : null}
            {!keysLoading && !selectedProjectId && !projectError ? (
              <TableRow>
                <TableCell colSpan={9} className="h-32 text-center text-muted-foreground">
                  {t({
                    en: "There are no active projects you can manage",
                    "zh-CN": "暂无可管理的活动项目",
                    ja: "管理できる有効なプロジェクトはありません",
                    ko: "관리할 수 있는 활성 프로젝트가 없습니다",
                  })}
                </TableCell>
              </TableRow>
            ) : null}
            {keys.map((key) => (
              <TableRow key={key.id}>
                <TableCell>
                  <p className="font-medium">{key.name}</p>
                  {canManageSelectedProject && key.provider_routes.length > 0 ? (
                    <p className="mt-0.5 text-xs text-muted-foreground">
                      {key.provider_routes
                        .map((route) => route.display_name)
                        .join(
                          t({
                            en: ", ",
                            "zh-CN": "、",
                            ja: "、",
                            ko: ", ",
                          }),
                        )}
                    </p>
                  ) : null}
                </TableCell>
                <TableCell>
                  <Badge variant={apiKeyIsUsable(key) ? "default" : "secondary"}>
                    {apiKeyStatusLabel(t, key)}
                  </Badge>
                  {key.status === "owner_access_lost" ? (
                    <p className="mt-1 text-xs text-muted-foreground">
                      {t({
                        en: "The owner no longer has access to this project",
                        "zh-CN": "所有者已失去项目访问权限",
                        ja: "所有者はこのプロジェクトへのアクセス権を失いました",
                        ko: "소유자가 이 프로젝트에 대한 접근 권한을 잃었습니다",
                      })}
                    </p>
                  ) : key.status === "project_user_keys_disabled" ? (
                    <p className="mt-1 text-xs text-muted-foreground">
                      {t({
                        en: "Personal keys are disabled for this project",
                        "zh-CN": "项目已禁用个人 Key",
                        ja: "このプロジェクトでは個人キーが無効です",
                        ko: "이 프로젝트에서는 개인 키가 비활성화되어 있습니다",
                      })}
                    </p>
                  ) : null}
                </TableCell>
                <TableCell className="font-mono text-xs">{key.id}</TableCell>
                <TableCell className="font-mono text-xs">{key.redacted_value}</TableCell>
                <TableCell>
                  <p>{apiKeyOwnerName(t, key)}</p>
                  <p className="mt-0.5 text-xs text-muted-foreground">
                    {key.owner.type === "user"
                      ? t({
                          en: "User",
                          "zh-CN": "用户",
                          ja: "ユーザー",
                          ko: "사용자",
                        })
                      : t({
                          en: "Service account",
                          "zh-CN": "服务账户",
                          ja: "サービスアカウント",
                          ko: "서비스 계정",
                        })}
                  </p>
                </TableCell>
                <TableCell>{permissionModeLabel(t, key.permission_mode)}</TableCell>
                <TableCell>{formatUnix(key.created_at, locale)}</TableCell>
                <TableCell>
                  {key.last_used_at
                    ? formatUnix(key.last_used_at, locale)
                    : t({
                        en: "Never",
                        "zh-CN": "从未使用",
                        ja: "未使用",
                        ko: "사용한 적 없음",
                      })}
                </TableCell>
                <TableCell className="text-right">
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button
                        variant="ghost"
                        size="icon"
                        aria-label={t(
                          {
                            en: "Manage API key {name}",
                            "zh-CN": "管理 API Key {name}",
                            ja: "API キー {name} を管理",
                            ko: "API 키 {name} 관리",
                          },
                          { name: key.name },
                        )}
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
                            {t({
                              en: "Edit name and permissions",
                              "zh-CN": "编辑名称与权限",
                              ja: "名前と権限を編集",
                              ko: "이름 및 권한 편집",
                            })}
                          </DropdownMenuItem>
                          <DropdownMenuItem onSelect={() => setRotateTarget(key)}>
                            <RefreshCw aria-hidden="true" />
                            {t({
                              en: "Rotate key",
                              "zh-CN": "轮换密钥",
                              ja: "キーをローテーション",
                              ko: "키 교체",
                            })}
                          </DropdownMenuItem>
                          <DropdownMenuSeparator />
                        </>
                      ) : null}
                      <DropdownMenuItem
                        className="text-destructive focus:text-destructive"
                        onSelect={() => setRevokeTarget(key)}
                      >
                        <Trash2 aria-hidden="true" />
                        {t({
                          en: "Revoke key",
                          "zh-CN": "吊销密钥",
                          ja: "キーを失効",
                          ko: "키 폐기",
                        })}
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
            {t({
              en: "Load more",
              "zh-CN": "加载更多",
              ja: "さらに読み込む",
              ko: "더 불러오기",
            })}
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
            <DialogTitle>
              {t({
                en: "Create API key",
                "zh-CN": "创建 API Key",
                ja: "API キーを作成",
                ko: "API 키 생성",
              })}
            </DialogTitle>
            <DialogDescription>
              {t({
                en: "Create a new access key for project",
                "zh-CN": "为项目",
                ja: "プロジェクト",
                ko: "프로젝트",
              })}{" "}
              <code>{selectedProject?.name ?? selectedProjectId}</code>
              {t({
                en: ".",
                "zh-CN": " 创建新的访问密钥。",
                ja: " の新しいアクセスキーを作成します。",
                ko: "의 새 액세스 키를 생성합니다.",
              })}
            </DialogDescription>
          </DialogHeader>
          {canManageSelectedProject ? (
            <div className="space-y-2">
              <Label>
                {t({ en: "Owner", "zh-CN": "所有者", ja: "所有者", ko: "소유자" })}
              </Label>
              <Tabs value={ownerType} onValueChange={(value) => setOwnerType(value as typeof ownerType)}>
                <TabsList className="grid w-full grid-cols-2">
                  <TabsTrigger
                    value="user"
                    disabled={
                      !canOwnSelectedProject ||
                      selectedProject?.user_api_keys_disabled
                    }
                  >
                    {t({ en: "You", "zh-CN": "你", ja: "自分", ko: "나" })}
                  </TabsTrigger>
                  <TabsTrigger value="service_account">
                    {t({
                      en: "Service account",
                      "zh-CN": "服务账户",
                      ja: "サービスアカウント",
                      ko: "서비스 계정",
                    })}
                  </TabsTrigger>
                </TabsList>
              </Tabs>
              <p className="text-sm text-muted-foreground">
                {ownerType === "user"
                  ? t({
                      en: "A personal key is enabled or disabled with your project membership.",
                      "zh-CN": "个人 Key 随你的项目成员资格生效或失效。",
                      ja: "個人キーの有効性は、プロジェクトメンバーシップに連動します。",
                      ko: "개인 키는 프로젝트 멤버십에 따라 활성화되거나 비활성화됩니다.",
                    })
                  : selectedProject?.user_api_keys_disabled
                    ? t({
                        en: "Personal keys are disabled for this project. Service account keys can still be created and used.",
                        "zh-CN": "此项目已禁用个人 Key；服务账户 Key 仍可正常创建和使用。",
                        ja: "このプロジェクトでは個人キーが無効です。サービスアカウントキーは引き続き作成して使用できます。",
                        ko: "이 프로젝트에서는 개인 키가 비활성화되어 있습니다. 서비스 계정 키는 계속 생성하고 사용할 수 있습니다.",
                      })
                    : t({
                        en: "A new machine identity and key will be created together.",
                        "zh-CN": "系统将创建新的机器身份，并同时签发一个 Key。",
                        ja: "新しいマシン ID を作成し、同時にキーを発行します。",
                        ko: "새 머신 ID와 키가 함께 생성됩니다.",
                      })}
              </p>
            </div>
          ) : null}
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <Label htmlFor="service-name">
                {ownerType === "user"
                  ? t({ en: "Name", "zh-CN": "名称", ja: "名前", ko: "이름" })
                  : t({
                      en: "Service account name",
                      "zh-CN": "服务账户名称",
                      ja: "サービスアカウント名",
                      ko: "서비스 계정 이름",
                    })}
              </Label>
              {ownerType === "user" ? (
                <span className="text-xs text-muted-foreground">
                  {t({
                    en: "Optional",
                    "zh-CN": "可选",
                    ja: "任意",
                    ko: "선택 사항",
                  })}
                </span>
              ) : null}
            </div>
            <Input
              id="service-name"
              value={serviceName}
              onChange={(event) => setServiceName(event.target.value)}
              placeholder={
                ownerType === "user"
                  ? t({
                      en: "For example: Local development",
                      "zh-CN": "例如：本地开发",
                      ja: "例: ローカル開発",
                      ko: "예: 로컬 개발",
                    })
                  : t({
                      en: "For example: Production bot",
                      "zh-CN": "例如：生产环境机器人",
                      ja: "例: 本番環境ボット",
                      ko: "예: 프로덕션 봇",
                    })
              }
              maxLength={128}
              autoFocus
            />
          </div>
          {ownerType === "service_account" ? (
            <div className="space-y-2">
              <Label htmlFor="service-route">
                {t({
                  en: "Service option",
                  "zh-CN": "服务方案",
                  ja: "サービスオプション",
                  ko: "서비스 옵션",
                })}
              </Label>
              <Select value={selectedRouteId} onValueChange={setSelectedRouteId} disabled={routesLoading}>
                <SelectTrigger id="service-route">
                  <SelectValue
                    placeholder={t({
                      en: "Select a service option",
                      "zh-CN": "选择服务方案",
                      ja: "サービスオプションを選択",
                      ko: "서비스 옵션 선택",
                    })}
                  />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={STANDARD_SERVICE}>
                    {t({
                      en: "Standard service · Platform-managed routing",
                      "zh-CN": "标准服务 · 平台自动调度",
                      ja: "標準サービス · プラットフォームによる自動ルーティング",
                      ko: "표준 서비스 · 플랫폼 자동 라우팅",
                    })}
                  </SelectItem>
                  {routes.map((route) => (
                    <SelectItem key={route.route_id} value={route.route_id}>
                      {route.display_name} · {providerName(t, route.provider_id)}
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
            <Button variant="outline" onClick={() => setServiceCreateOpen(false)} disabled={serviceCreating}>
              {t({
                en: "Cancel",
                "zh-CN": "取消",
                ja: "キャンセル",
                ko: "취소",
              })}
            </Button>
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
              {t({
                en: "Create key",
                "zh-CN": "创建密钥",
                ja: "キーを作成",
                ko: "키 생성",
              })}
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
            <DialogTitle>
              {t({
                en: "Edit API key",
                "zh-CN": "编辑 API Key",
                ja: "API キーを編集",
                ko: "API 키 편집",
              })}
            </DialogTitle>
            <DialogDescription>
              {t({
                en: "Changing the name or permissions does not change the existing key value. New permissions take effect immediately.",
                "zh-CN": "修改名称或权限后，现有 Key 明文不变，新的权限立即生效。",
                ja: "名前や権限を変更しても、既存のキー値は変わりません。新しい権限はすぐに有効になります。",
                ko: "이름이나 권한을 변경해도 기존 키 값은 바뀌지 않으며 새 권한은 즉시 적용됩니다.",
              })}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-2">
            <Label htmlFor="edit-key-name">
              {t({ en: "Name", "zh-CN": "名称", ja: "名前", ko: "이름" })}
            </Label>
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
              {t({
                en: "Cancel",
                "zh-CN": "取消",
                ja: "キャンセル",
                ko: "취소",
              })}
            </Button>
            <Button
              onClick={() => void updateApiKey()}
              disabled={editPending || !editName.trim()}
            >
              {editPending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <Pencil aria-hidden="true" />}
              {t({
                en: "Save changes",
                "zh-CN": "保存更改",
                ja: "変更を保存",
                ko: "변경 사항 저장",
              })}
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
            <AlertDialogTitle>
              {t({
                en: "Rotate API key?",
                "zh-CN": "轮换 API Key？",
                ja: "API キーをローテーションしますか？",
                ko: "API 키를 교체할까요?",
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {t({
                en: "A new key will be issued and the old key for",
                "zh-CN": "系统会签发新的 Key，并在同一事务中立即吊销",
                ja: "新しいキーを発行し、",
                ko: "새 키를 발급하고",
              })}{" "}
              <strong>{rotateTarget?.name}</strong>
              {t({
                en: " will be revoked immediately in the same transaction. The new key value will be shown only once.",
                "zh-CN": " 的旧 Key。新明文仍只显示一次。",
                ja: " の古いキーを同じトランザクションですぐに失効します。新しいキー値は一度だけ表示されます。",
                ko: "의 이전 키를 동일한 트랜잭션에서 즉시 폐기합니다. 새 키 값은 한 번만 표시됩니다.",
              })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={rotatePending}>
              {t({
                en: "Cancel",
                "zh-CN": "取消",
                ja: "キャンセル",
                ko: "취소",
              })}
            </AlertDialogCancel>
            <AlertDialogAction
              disabled={rotatePending}
              onClick={(event) => {
                event.preventDefault();
                void rotateApiKey();
              }}
            >
              {rotatePending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <RefreshCw aria-hidden="true" />}
              {t({
                en: "Rotate key",
                "zh-CN": "轮换密钥",
                ja: "キーをローテーション",
                ko: "키 교체",
              })}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={Boolean(created)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t({
                en: "Save your new API key",
                "zh-CN": "保存新的 API Key",
                ja: "新しい API キーを保存",
                ko: "새 API 키 저장",
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {t({
                en: "The key value appears only in this response. You cannot view it again after closing this dialog.",
                "zh-CN": "密钥明文仅在本次响应中出现，关闭后无法再次查看。",
                ja: "キー値はこの応答でのみ表示されます。このダイアログを閉じると再表示できません。",
                ko: "키 값은 이번 응답에만 표시되며 이 대화상자를 닫으면 다시 확인할 수 없습니다.",
              })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          {created ? (
            <div className="space-y-3">
              <div className="border bg-muted/40 p-3 font-mono text-xs break-all">{created.value}</div>
              <Button className="w-full" variant="outline" onClick={() => void copySecret()}>
                {copied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
                {copied
                  ? t({
                      en: "Copied",
                      "zh-CN": "已复制",
                      ja: "コピー済み",
                      ko: "복사됨",
                    })
                  : t({
                      en: "Copy key",
                      "zh-CN": "复制密钥",
                      ja: "キーをコピー",
                      ko: "키 복사",
                    })}
              </Button>
            </div>
          ) : null}
          <AlertDialogFooter>
            <AlertDialogAction onClick={clearSecret}>
              {t({
                en: "I saved it; close",
                "zh-CN": "我已保存并关闭",
                ja: "保存したので閉じる",
                ko: "저장했으며 닫기",
              })}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={Boolean(revokeTarget)} onOpenChange={(open) => !open && !revokePending && setRevokeTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t({
                en: "Delete API key?",
                "zh-CN": "删除 API Key？",
                ja: "API キーを削除しますか？",
                ko: "API 키를 삭제할까요?",
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>
              <strong>{revokeTarget?.name}</strong>
              {t({
                en: " will stop working immediately and cannot be restored. Other keys are not affected.",
                "zh-CN": " 将立即失效且不能恢复，其他 Key 不受影响。",
                ja: " はすぐに無効になり、復元できません。他のキーには影響しません。",
                ko: "는 즉시 비활성화되며 복구할 수 없습니다. 다른 키에는 영향을 주지 않습니다.",
              })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={revokePending}>
              {t({
                en: "Cancel",
                "zh-CN": "取消",
                ja: "キャンセル",
                ko: "취소",
              })}
            </AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={revokePending}
              onClick={(event) => {
                event.preventDefault();
                void revokeApiKey();
              }}
            >
              {revokePending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <Trash2 aria-hidden="true" />}
              {t({
                en: "Revoke this key",
                "zh-CN": "吊销此 Key",
                ja: "このキーを失効",
                ko: "이 키 폐기",
              })}
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
  const { t } = useI18n();

  return (
    <div className="space-y-3">
      <Label>
        {t({
          en: "Permissions",
          "zh-CN": "权限",
          ja: "権限",
          ko: "권한",
        })}
      </Label>
      <Tabs
        value={mode}
        onValueChange={(value) => onModeChange(value as PermissionMode)}
      >
        <TabsList className="grid w-full grid-cols-3">
          <TabsTrigger value="all" disabled={disabled}>
            {t({ en: "All", "zh-CN": "全部", ja: "すべて", ko: "전체" })}
          </TabsTrigger>
          <TabsTrigger value="restricted" disabled={disabled}>
            {t({
              en: "Restricted",
              "zh-CN": "受限",
              ja: "制限付き",
              ko: "제한됨",
            })}
          </TabsTrigger>
          <TabsTrigger value="read_only" disabled={disabled}>
            {t({
              en: "Read only",
              "zh-CN": "只读",
              ja: "読み取り専用",
              ko: "읽기 전용",
            })}
          </TabsTrigger>
        </TabsList>
      </Tabs>
      {mode === "restricted" ? (
        <div className="divide-y rounded-md border">
          <PermissionSelect
            label={t({
              en: "Model list",
              "zh-CN": "模型列表",
              ja: "モデル一覧",
              ko: "모델 목록",
            })}
            value={permissions.models}
            onValueChange={(value) => onPermissionChange("models", value)}
            disabled={disabled}
          />
          <PermissionSelect
            label={t({
              en: "Images API",
              "zh-CN": "图片 API",
              ja: "画像 API",
              ko: "이미지 API",
            })}
            value={permissions.images}
            onValueChange={(value) => onPermissionChange("images", value)}
            disabled={disabled}
          />
          <PermissionSelect
            label={t({
              en: "Videos API",
              "zh-CN": "视频 API",
              ja: "動画 API",
              ko: "동영상 API",
            })}
            value={permissions.videos}
            onValueChange={(value) => onPermissionChange("videos", value)}
            disabled={disabled}
          />
          <PermissionSelect
            label={t({
              en: "Files API",
              "zh-CN": "文件 API",
              ja: "ファイル API",
              ko: "파일 API",
            })}
            value={permissions.files}
            onValueChange={(value) => onPermissionChange("files", value)}
            disabled={disabled}
          />
          <PermissionSelect
            label={t({
              en: "Batch API",
              "zh-CN": "批处理 API",
              ja: "バッチ API",
              ko: "배치 API",
            })}
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
  const { t } = useI18n();

  return (
    <div className="flex items-center justify-between gap-4 px-3 py-2.5">
      <span className="text-sm">{label}</span>
      <Select
        value={value}
        onValueChange={(next) => onValueChange(next as PermissionLevel)}
        disabled={disabled}
      >
        <SelectTrigger
          className="w-32"
          aria-label={t(
            {
              en: "{label} permission",
              "zh-CN": "{label}权限",
              ja: "{label}の権限",
              ko: "{label} 권한",
            },
            { label },
          )}
        >
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="none">
            {t({
              en: "No access",
              "zh-CN": "无权限",
              ja: "アクセスなし",
              ko: "접근 권한 없음",
            })}
          </SelectItem>
          <SelectItem value="read">
            {t({ en: "Read", "zh-CN": "读取", ja: "読み取り", ko: "읽기" })}
          </SelectItem>
          <SelectItem value="write">
            {t({ en: "Write", "zh-CN": "写入", ja: "書き込み", ko: "쓰기" })}
          </SelectItem>
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

function apiKeyStatusLabel(t: Translate, key: ApiKey) {
  if (key.status === "expired") {
    return t({
      en: "Expired",
      "zh-CN": "已过期",
      ja: "期限切れ",
      ko: "만료됨",
    });
  }
  if (key.status === "project_user_keys_disabled") {
    return t({
      en: "Disabled by project",
      "zh-CN": "项目已禁用",
      ja: "プロジェクトで無効",
      ko: "프로젝트에서 비활성화됨",
    });
  }
  if (key.status === "owner_access_lost") {
    return t({
      en: "Unavailable",
      "zh-CN": "不可用",
      ja: "利用不可",
      ko: "사용할 수 없음",
    });
  }
  return t({ en: "Active", "zh-CN": "有效", ja: "有効", ko: "활성" });
}

function apiKeyOwnerName(t: Translate, key: ApiKey) {
  return key.owner.type === "user"
    ? key.owner.user?.name ??
        t({
          en: "Unknown user",
          "zh-CN": "未知用户",
          ja: "不明なユーザー",
          ko: "알 수 없는 사용자",
        })
    : key.owner.service_account?.name ??
        t({
          en: "Unknown service account",
          "zh-CN": "未知服务账户",
          ja: "不明なサービスアカウント",
          ko: "알 수 없는 서비스 계정",
        });
}

function permissionModeLabel(t: Translate, mode: PermissionMode) {
  switch (mode) {
    case "all":
      return t({ en: "All", "zh-CN": "全部", ja: "すべて", ko: "전체" });
    case "restricted":
      return t({
        en: "Restricted",
        "zh-CN": "受限",
        ja: "制限付き",
        ko: "제한됨",
      });
    case "read_only":
      return t({
        en: "Read only",
        "zh-CN": "只读",
        ja: "読み取り専用",
        ko: "읽기 전용",
      });
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

function providerName(t: Translate, providerId: string) {
  switch (providerId) {
    case "openai-codex":
      return "Codex";
    case "grok-cli":
      return "Grok";
    case "dreamina-cli":
      return t({
        en: "Dreamina",
        "zh-CN": "即梦",
        ja: "Dreamina",
        ko: "Dreamina",
      });
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

async function responseMessage(response: Response, t: Translate) {
  const body = (await response.json().catch(() => null)) as
    | { error?: string | { message?: string } }
    | null;
  if (typeof body?.error === "string") return body.error;
  if (body?.error && typeof body.error === "object" && body.error.message) return body.error.message;
  return t(
    {
      en: "Request failed ({status})",
      "zh-CN": "请求失败 ({status})",
      ja: "リクエストに失敗しました ({status})",
      ko: "요청 실패 ({status})",
    },
    { status: response.status },
  );
}

function formatUnix(value: number, locale: Locale) {
  return new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(value * 1000));
}
