"use client";

import { useCallback, useEffect, useState } from "react";
import {
  ArrowLeft,
  Check,
  Copy,
  Ellipsis,
  FlaskConical,
  LoaderCircle,
  Pencil,
  Play,
  Plus,
  RefreshCw,
  RotateCw,
  Trash2,
  Webhook,
} from "lucide-react";
import { toast } from "sonner";
import { ProjectWebhookDialog } from "./project-webhook-dialog";
import {
  WEBHOOK_DELIVERY_LABELS,
  WEBHOOK_EVENT_LABELS,
  type ProjectWebhookDelivery,
  type ProjectWebhookEndpoint,
  type RotatedProjectWebhookSecret,
  type WebhookDeliveryState,
} from "./project-webhook-types";
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
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { consoleFetch } from "@/lib/auth/client";

type EndpointList = {
  object: "list";
  data: ProjectWebhookEndpoint[];
  has_more: boolean;
  last_id: string | null;
};

type DeliveryList = {
  object: "list";
  data: ProjectWebhookDelivery[];
  has_more: boolean;
  last_id: string | null;
};

export function ProjectWebhooksPanel({
  projectId,
  canManage,
  active,
}: {
  projectId: string;
  canManage: boolean;
  active: boolean;
}) {
  const [endpoints, setEndpoints] = useState<ProjectWebhookEndpoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editTarget, setEditTarget] = useState<ProjectWebhookEndpoint | null>(null);
  const [selected, setSelected] = useState<ProjectWebhookEndpoint | null>(null);
  const [deliveries, setDeliveries] = useState<ProjectWebhookDelivery[]>([]);
  const [deliveriesLoading, setDeliveriesLoading] = useState(false);
  const [deliveryError, setDeliveryError] = useState<string | null>(null);
  const [secret, setSecret] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const [rotateTarget, setRotateTarget] =
    useState<ProjectWebhookEndpoint | null>(null);
  const [deleteTarget, setDeleteTarget] =
    useState<ProjectWebhookEndpoint | null>(null);
  const [mutationPending, setMutationPending] = useState(false);

  const loadEndpoints = useCallback(
    async (background = false) => {
      if (!active) return;
      background ? setRefreshing(true) : setLoading(true);
      setError(null);
      try {
        const response = await consoleFetch(
          `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/webhooks?limit=100`,
        );
        if (!response.ok) throw new Error(await responseMessage(response));
        const payload = (await response.json()) as EndpointList;
        setEndpoints(payload.data);
        setSelected((current) =>
          current
            ? payload.data.find((endpoint) => endpoint.id === current.id) ?? null
            : null,
        );
      } catch (reason) {
        if (!background) setEndpoints([]);
        setError(reason instanceof Error ? reason.message : "Webhook 加载失败");
      } finally {
        background ? setRefreshing(false) : setLoading(false);
      }
    },
    [active, projectId],
  );

  const loadDeliveries = useCallback(async (endpointId: string, background = false) => {
    if (!active) return;
    if (!background) setDeliveriesLoading(true);
    setDeliveryError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/webhooks/${encodeURIComponent(endpointId)}/deliveries?limit=100`,
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      const payload = (await response.json()) as DeliveryList;
      setDeliveries(payload.data);
      if (
        background &&
        !payload.data.some((delivery) =>
          ["pending", "leased", "retry_wait"].includes(delivery.state),
        )
      ) {
        void loadEndpoints(true);
      }
    } catch (reason) {
      setDeliveries([]);
      setDeliveryError(
        reason instanceof Error ? reason.message : "投递记录加载失败",
      );
    } finally {
      if (!background) setDeliveriesLoading(false);
    }
  }, [active, loadEndpoints, projectId]);

  useEffect(() => {
    if (active) void loadEndpoints();
  }, [active, loadEndpoints]);

  useEffect(() => {
    if (selected) void loadDeliveries(selected.id);
    else {
      setDeliveries([]);
      setDeliveryError(null);
    }
  }, [loadDeliveries, selected]);

  useEffect(() => {
    if (
      !selected ||
      !deliveries.some((delivery) =>
        ["pending", "leased", "retry_wait"].includes(delivery.state),
      )
    ) {
      return;
    }
    const timer = window.setTimeout(() => {
      void loadDeliveries(selected.id, true);
    }, 1_500);
    return () => window.clearTimeout(timer);
  }, [deliveries, loadDeliveries, selected]);

  useEffect(() => {
    const clear = () => {
      setSecret(null);
      setCopied(false);
    };
    window.addEventListener("pagehide", clear);
    return () => window.removeEventListener("pagehide", clear);
  }, []);

  async function toggleEndpoint(endpoint: ProjectWebhookEndpoint) {
    if (!canManage) return;
    setMutationPending(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/webhooks/${encodeURIComponent(endpoint.id)}`,
        {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            name: endpoint.name,
            url: endpoint.url,
            event_types: endpoint.event_types,
            state: endpoint.state === "active" ? "disabled" : "active",
            expected_control_version: endpoint.control_version,
          }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      toast.success(endpoint.state === "active" ? "Webhook 已停用" : "Webhook 已启用");
      await loadEndpoints(true);
    } catch (reason) {
      toast.error(reason instanceof Error ? reason.message : "状态更新失败");
    } finally {
      setMutationPending(false);
    }
  }

  async function enqueueTest(endpoint: ProjectWebhookEndpoint) {
    if (!canManage) return;
    setMutationPending(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/webhooks/${encodeURIComponent(endpoint.id)}/test`,
        { method: "POST" },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      toast.success("测试事件已加入投递队列");
      setSelected(endpoint);
      await Promise.all([loadEndpoints(true), loadDeliveries(endpoint.id)]);
    } catch (reason) {
      toast.error(reason instanceof Error ? reason.message : "测试事件创建失败");
    } finally {
      setMutationPending(false);
    }
  }

  async function rotateSecret() {
    if (!rotateTarget) return;
    setMutationPending(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/webhooks/${encodeURIComponent(rotateTarget.id)}/rotate`,
        { method: "POST" },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      const payload = (await response.json()) as RotatedProjectWebhookSecret;
      setSecret(payload.signing_secret);
      setCopied(false);
      setRotateTarget(null);
      await loadEndpoints(true);
      toast.success("签名密钥已轮换");
    } catch (reason) {
      toast.error(reason instanceof Error ? reason.message : "密钥轮换失败");
    } finally {
      setMutationPending(false);
    }
  }

  async function deleteEndpoint() {
    if (!deleteTarget) return;
    setMutationPending(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/webhooks/${encodeURIComponent(deleteTarget.id)}`,
        { method: "DELETE" },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      if (selected?.id === deleteTarget.id) setSelected(null);
      setDeleteTarget(null);
      await loadEndpoints(true);
      toast.success("Webhook 已删除");
    } catch (reason) {
      toast.error(reason instanceof Error ? reason.message : "Webhook 删除失败");
    } finally {
      setMutationPending(false);
    }
  }

  async function copySecret() {
    if (!secret) return;
    try {
      await navigator.clipboard.writeText(secret);
      setCopied(true);
    } catch {
      toast.error("复制失败，请手动选择密钥");
    }
  }

  if (selected) {
    return (
      <div className="space-y-5 px-5 py-6 sm:px-6">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div className="min-w-0">
            <Button
              variant="ghost"
              size="sm"
              className="-ml-2 mb-2"
              onClick={() => setSelected(null)}
            >
              <ArrowLeft aria-hidden="true" />
              返回 Webhooks
            </Button>
            <h3 className="truncate text-sm font-medium">
              {selected.name || endpointDisplayUrl(selected.url)}
            </h3>
            <p className="mt-1 truncate font-mono text-xs text-muted-foreground">
              {selected.id}
            </p>
          </div>
          <Button
            variant="outline"
            size="sm"
            disabled={deliveriesLoading}
            onClick={() => void loadDeliveries(selected.id)}
          >
            {deliveriesLoading ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <RefreshCw aria-hidden="true" />
            )}
            刷新
          </Button>
        </div>

        <div className="grid gap-3 text-sm sm:grid-cols-[128px_1fr]">
          <span className="text-muted-foreground">Endpoint URL</span>
          <span className="break-all font-mono text-xs">{selected.url}</span>
          <span className="text-muted-foreground">订阅事件</span>
          <div className="flex flex-wrap gap-1.5">
            {selected.event_types.map((eventType) => (
              <Badge key={eventType} variant="outline">
                {WEBHOOK_EVENT_LABELS[eventType]}
              </Badge>
            ))}
          </div>
        </div>

        <div className="overflow-x-auto border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>事件</TableHead>
                <TableHead>状态</TableHead>
                <TableHead className="hidden md:table-cell">HTTP</TableHead>
                <TableHead className="hidden md:table-cell">尝试</TableHead>
                <TableHead className="text-right">更新时间</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {deliveries.map((delivery) => (
                <TableRow key={delivery.id}>
                  <TableCell>
                    <div className="font-medium">{eventLabel(delivery.event_type)}</div>
                    <div className="max-w-56 truncate font-mono text-xs text-muted-foreground">
                      {delivery.event_id}
                    </div>
                  </TableCell>
                  <TableCell>
                    <DeliveryBadge state={delivery.state} />
                    {delivery.last_error_code ? (
                      <div className="mt-1 max-w-48 truncate text-xs text-muted-foreground">
                        {delivery.last_error_code}
                      </div>
                    ) : null}
                  </TableCell>
                  <TableCell className="hidden tabular-nums md:table-cell">
                    {delivery.last_http_status ?? "—"}
                  </TableCell>
                  <TableCell className="hidden tabular-nums md:table-cell">
                    {delivery.attempt_count}
                  </TableCell>
                  <TableCell className="text-right text-muted-foreground">
                    {formatTime(delivery.updated_at_ms)}
                  </TableCell>
                </TableRow>
              ))}
              {!deliveriesLoading && deliveries.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-32 text-center text-muted-foreground">
                    {deliveryError ?? "尚无投递记录"}
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
          </Table>
        </div>
      </div>
    );
  }

  return (
    <>
      <div className="space-y-5 px-5 py-6 sm:px-6">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <h3 className="text-sm font-medium">Webhooks</h3>
            <p className="mt-1 text-sm text-muted-foreground">
              使用签名 HTTP 请求把项目中的图片和视频事件发送到你的服务。
            </p>
          </div>
          <div className="flex shrink-0 gap-2">
            <Button
              variant="outline"
              size="icon"
              disabled={refreshing || loading}
              onClick={() => void loadEndpoints(true)}
              aria-label="刷新 Webhooks"
              title="刷新 Webhooks"
            >
              {refreshing ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <RefreshCw aria-hidden="true" />
              )}
            </Button>
            {canManage ? (
              <Button
                onClick={() => {
                  setEditTarget(null);
                  setDialogOpen(true);
                }}
              >
                <Plus aria-hidden="true" />
                创建 Webhook
              </Button>
            ) : null}
          </div>
        </div>

        {loading ? (
          <div className="grid min-h-48 place-items-center text-muted-foreground">
            <LoaderCircle className="size-5 animate-spin" aria-label="正在加载 Webhooks" />
          </div>
        ) : endpoints.length === 0 ? (
          <div className="grid min-h-56 place-items-center border border-dashed px-6 text-center">
            <div className="max-w-sm">
              <Webhook className="mx-auto mb-3 size-6 text-muted-foreground" aria-hidden="true" />
              <p className="text-sm font-medium">尚未创建 Webhook</p>
              <p className="mt-1 text-sm text-muted-foreground">
                创建 Endpoint 后即可订阅项目事件并查看每次投递。
              </p>
              {canManage ? (
                <Button
                  className="mt-4"
                  size="sm"
                  onClick={() => setDialogOpen(true)}
                >
                  <Plus aria-hidden="true" />
                  创建 Webhook
                </Button>
              ) : null}
            </div>
          </div>
        ) : (
          <div className="overflow-x-auto border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Endpoint</TableHead>
                  <TableHead className="hidden lg:table-cell">订阅事件</TableHead>
                  <TableHead>状态</TableHead>
                  <TableHead className="hidden sm:table-cell">最近投递</TableHead>
                  <TableHead className="w-12">
                    <span className="sr-only">操作</span>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {endpoints.map((endpoint) => (
                  <TableRow key={endpoint.id}>
                    <TableCell className="min-w-0">
                      <button
                        type="button"
                        className="block max-w-72 text-left"
                        onClick={() => setSelected(endpoint)}
                      >
                        <span className="block truncate font-medium">
                          {endpoint.name || endpointDisplayUrl(endpoint.url)}
                        </span>
                        <span className="block truncate text-xs text-muted-foreground">
                          {endpointDisplayUrl(endpoint.url)}
                        </span>
                      </button>
                    </TableCell>
                    <TableCell className="hidden lg:table-cell">
                      <span className="text-sm">
                        {WEBHOOK_EVENT_LABELS[endpoint.event_types[0]]}
                        {endpoint.event_types.length > 1
                          ? ` 等 ${endpoint.event_types.length} 项`
                          : ""}
                      </span>
                    </TableCell>
                    <TableCell>
                      <Badge variant={endpoint.state === "active" ? "secondary" : "outline"}>
                        {endpoint.state === "active" ? "启用" : "已停用"}
                      </Badge>
                    </TableCell>
                    <TableCell className="hidden sm:table-cell">
                      {endpoint.last_delivery_state ? (
                        <div>
                          <DeliveryBadge state={endpoint.last_delivery_state} />
                          <div className="mt-1 text-xs text-muted-foreground">
                            {formatTime(endpoint.last_delivery_at_ms)}
                          </div>
                        </div>
                      ) : (
                        <span className="text-sm text-muted-foreground">尚无投递</span>
                      )}
                    </TableCell>
                    <TableCell>
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon"
                            aria-label={`管理 ${endpoint.name || endpoint.id}`}
                          >
                            <Ellipsis aria-hidden="true" />
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end">
                          <DropdownMenuItem onSelect={() => setSelected(endpoint)}>
                            <Webhook aria-hidden="true" />
                            查看投递记录
                          </DropdownMenuItem>
                          {canManage ? (
                            <>
                              <DropdownMenuItem
                                onSelect={() => {
                                  setEditTarget(endpoint);
                                  setDialogOpen(true);
                                }}
                              >
                                <Pencil aria-hidden="true" />
                                编辑
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                disabled={endpoint.state !== "active" || mutationPending}
                                onSelect={() => void enqueueTest(endpoint)}
                              >
                                <FlaskConical aria-hidden="true" />
                                发送测试事件
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                disabled={mutationPending}
                                onSelect={() => void toggleEndpoint(endpoint)}
                              >
                                <Play aria-hidden="true" />
                                {endpoint.state === "active" ? "停用" : "启用"}
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                disabled={mutationPending}
                                onSelect={() => setRotateTarget(endpoint)}
                              >
                                <RotateCw aria-hidden="true" />
                                轮换签名密钥
                              </DropdownMenuItem>
                              <DropdownMenuSeparator />
                              <DropdownMenuItem
                                className="text-destructive focus:text-destructive"
                                disabled={mutationPending}
                                onSelect={() => setDeleteTarget(endpoint)}
                              >
                                <Trash2 aria-hidden="true" />
                                删除
                              </DropdownMenuItem>
                            </>
                          ) : null}
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}

        {error ? (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        ) : null}
        {!canManage ? (
          <p className="text-sm text-muted-foreground">
            你可以查看 Endpoint 和投递记录；只有项目或组织所有者可以更改。
          </p>
        ) : null}
      </div>

      <ProjectWebhookDialog
        projectId={projectId}
        open={dialogOpen}
        endpoint={editTarget}
        onOpenChange={(open) => {
          setDialogOpen(open);
          if (!open) setEditTarget(null);
        }}
        onSaved={() => loadEndpoints(true)}
        onSecret={(value) => {
          setSecret(value);
          setCopied(false);
        }}
      />

      <AlertDialog open={Boolean(secret)}>
        <AlertDialogContent className="w-[calc(100%-2rem)]">
          <AlertDialogHeader>
            <AlertDialogTitle>保存 Webhook 签名密钥</AlertDialogTitle>
            <AlertDialogDescription>
              密钥只在本次响应中显示。关闭后无法再次查看，可通过轮换生成新的密钥。
            </AlertDialogDescription>
          </AlertDialogHeader>
          {secret ? (
            <div className="space-y-3">
              <div className="break-all border bg-muted/40 p-3 font-mono text-xs">
                {secret}
              </div>
              <Button className="w-full" variant="outline" onClick={() => void copySecret()}>
                {copied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
                {copied ? "已复制" : "复制密钥"}
              </Button>
            </div>
          ) : null}
          <AlertDialogFooter>
            <AlertDialogAction
              onClick={() => {
                setSecret(null);
                setCopied(false);
              }}
            >
              我已保存并关闭
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog
        open={Boolean(rotateTarget)}
        onOpenChange={(open) => !open && !mutationPending && setRotateTarget(null)}
      >
        <AlertDialogContent className="w-[calc(100%-2rem)]">
          <AlertDialogHeader>
            <AlertDialogTitle>轮换签名密钥？</AlertDialogTitle>
            <AlertDialogDescription>
              旧密钥会立即停止签名后续请求。请准备好同步更新接收端配置。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={mutationPending}>取消</AlertDialogCancel>
            <AlertDialogAction
              disabled={mutationPending}
              onClick={(event) => {
                event.preventDefault();
                void rotateSecret();
              }}
            >
              {mutationPending ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <RotateCw aria-hidden="true" />
              )}
              轮换密钥
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog
        open={Boolean(deleteTarget)}
        onOpenChange={(open) => !open && !mutationPending && setDeleteTarget(null)}
      >
        <AlertDialogContent className="w-[calc(100%-2rem)]">
          <AlertDialogHeader>
            <AlertDialogTitle>删除 Webhook？</AlertDialogTitle>
            <AlertDialogDescription>
              Endpoint 会停止接收新事件，所有尚未发送的投递会被取消。历史投递记录仍保留用于审计。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={mutationPending}>取消</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={mutationPending}
              onClick={(event) => {
                event.preventDefault();
                void deleteEndpoint();
              }}
            >
              {mutationPending ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <Trash2 aria-hidden="true" />
              )}
              删除 Webhook
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

function DeliveryBadge({ state }: { state: WebhookDeliveryState }) {
  return (
    <Badge
      variant={
        state === "succeeded"
          ? "secondary"
          : state === "dead_lettered"
            ? "destructive"
            : "outline"
      }
    >
      {WEBHOOK_DELIVERY_LABELS[state]}
    </Badge>
  );
}

function eventLabel(eventType: string) {
  return (
    WEBHOOK_EVENT_LABELS[eventType as keyof typeof WEBHOOK_EVENT_LABELS] ??
    (eventType === "webhook.test" ? "测试事件" : eventType)
  );
}

function endpointDisplayUrl(value: string) {
  try {
    const url = new URL(value);
    return `${url.host}${url.pathname === "/" ? "" : url.pathname}`;
  } catch {
    return value;
  }
}

function formatTime(value: number | null) {
  if (!value) return "—";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
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
