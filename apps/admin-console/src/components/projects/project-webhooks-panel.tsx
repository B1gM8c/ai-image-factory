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
import { useI18n } from "@/i18n/locale-provider";
import {
  consoleFetch,
  consoleRequestFailure,
  consoleResponseFailure,
} from "@/lib/auth/client";

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
  const { locale, t } = useI18n();
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
      const failure = t({
        en: "Failed to load webhooks.",
        "zh-CN": "Webhook 加载失败。",
        ja: "Webhook を読み込めませんでした。",
        ko: "Webhook을 불러오지 못했습니다.",
      });
      background ? setRefreshing(true) : setLoading(true);
      setError(null);
      try {
        const response = await consoleFetch(
          `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/webhooks?limit=100`,
        );
        if (!response.ok) {
          throw new Error(await consoleResponseFailure(response, failure, t));
        }
        const payload = (await response.json()) as EndpointList;
        setEndpoints(payload.data);
        setSelected((current) =>
          current
            ? payload.data.find((endpoint) => endpoint.id === current.id) ?? null
            : null,
        );
      } catch (reason) {
        if (!background) setEndpoints([]);
        setError(consoleRequestFailure(reason, failure, t));
      } finally {
        background ? setRefreshing(false) : setLoading(false);
      }
    },
    [active, projectId, t],
  );

  const loadDeliveries = useCallback(async (endpointId: string, background = false) => {
    if (!active) return;
    const failure = t({
      en: "Failed to load delivery history.",
      "zh-CN": "投递记录加载失败。",
      ja: "配信履歴を読み込めませんでした。",
      ko: "전송 기록을 불러오지 못했습니다.",
    });
    if (!background) setDeliveriesLoading(true);
    setDeliveryError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/webhooks/${encodeURIComponent(endpointId)}/deliveries?limit=100`,
      );
      if (!response.ok) {
        throw new Error(await consoleResponseFailure(response, failure, t));
      }
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
      setDeliveryError(consoleRequestFailure(reason, failure, t));
    } finally {
      if (!background) setDeliveriesLoading(false);
    }
  }, [active, loadEndpoints, projectId, t]);

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
    const failure = t({
      en: "Failed to update the webhook status.",
      "zh-CN": "Webhook 状态更新失败。",
      ja: "Webhook のステータスを更新できませんでした。",
      ko: "Webhook 상태를 업데이트하지 못했습니다.",
    });
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
      if (!response.ok) {
        throw new Error(await consoleResponseFailure(response, failure, t));
      }
      toast.success(endpoint.state === "active"
        ? t({ en: "Webhook disabled", "zh-CN": "Webhook 已停用", ja: "Webhook を無効にしました", ko: "Webhook을 비활성화했습니다" })
        : t({ en: "Webhook enabled", "zh-CN": "Webhook 已启用", ja: "Webhook を有効にしました", ko: "Webhook을 활성화했습니다" }));
      await loadEndpoints(true);
    } catch (reason) {
      toast.error(consoleRequestFailure(reason, failure, t));
    } finally {
      setMutationPending(false);
    }
  }

  async function enqueueTest(endpoint: ProjectWebhookEndpoint) {
    if (!canManage) return;
    const failure = t({
      en: "Failed to create the test event.",
      "zh-CN": "测试事件创建失败。",
      ja: "テストイベントを作成できませんでした。",
      ko: "테스트 이벤트를 만들지 못했습니다.",
    });
    setMutationPending(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/webhooks/${encodeURIComponent(endpoint.id)}/test`,
        { method: "POST" },
      );
      if (!response.ok) {
        throw new Error(await consoleResponseFailure(response, failure, t));
      }
      toast.success(t({ en: "Test event queued for delivery", "zh-CN": "测试事件已加入投递队列", ja: "テストイベントを配信キューに追加しました", ko: "테스트 이벤트가 전송 대기열에 추가되었습니다" }));
      setSelected(endpoint);
      await Promise.all([loadEndpoints(true), loadDeliveries(endpoint.id)]);
    } catch (reason) {
      toast.error(consoleRequestFailure(reason, failure, t));
    } finally {
      setMutationPending(false);
    }
  }

  async function rotateSecret() {
    if (!rotateTarget) return;
    const failure = t({
      en: "Failed to rotate the signing secret.",
      "zh-CN": "签名密钥轮换失败。",
      ja: "署名シークレットをローテーションできませんでした。",
      ko: "서명 시크릿을 교체하지 못했습니다.",
    });
    setMutationPending(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/webhooks/${encodeURIComponent(rotateTarget.id)}/rotate`,
        { method: "POST" },
      );
      if (!response.ok) {
        throw new Error(await consoleResponseFailure(response, failure, t));
      }
      const payload = (await response.json()) as RotatedProjectWebhookSecret;
      setSecret(payload.signing_secret);
      setCopied(false);
      setRotateTarget(null);
      await loadEndpoints(true);
      toast.success(t({ en: "Signing secret rotated", "zh-CN": "签名密钥已轮换", ja: "署名シークレットをローテーションしました", ko: "서명 시크릿을 교체했습니다" }));
    } catch (reason) {
      toast.error(consoleRequestFailure(reason, failure, t));
    } finally {
      setMutationPending(false);
    }
  }

  async function deleteEndpoint() {
    if (!deleteTarget) return;
    const failure = t({
      en: "Failed to delete the webhook.",
      "zh-CN": "Webhook 删除失败。",
      ja: "Webhook を削除できませんでした。",
      ko: "Webhook을 삭제하지 못했습니다.",
    });
    setMutationPending(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/webhooks/${encodeURIComponent(deleteTarget.id)}`,
        { method: "DELETE" },
      );
      if (!response.ok) {
        throw new Error(await consoleResponseFailure(response, failure, t));
      }
      if (selected?.id === deleteTarget.id) setSelected(null);
      setDeleteTarget(null);
      await loadEndpoints(true);
      toast.success(t({ en: "Webhook deleted", "zh-CN": "Webhook 已删除", ja: "Webhook を削除しました", ko: "Webhook을 삭제했습니다" }));
    } catch (reason) {
      toast.error(consoleRequestFailure(reason, failure, t));
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
      toast.error(t({ en: "Copy failed. Select and copy the secret manually.", "zh-CN": "复制失败，请手动选择密钥", ja: "コピーできませんでした。シークレットを手動で選択してコピーしてください。", ko: "복사하지 못했습니다. 시크릿을 직접 선택해 복사하세요." }));
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
              {t({ en: "Back to webhooks", "zh-CN": "返回 Webhooks", ja: "Webhook に戻る", ko: "Webhook으로 돌아가기" })}
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
            {t({ en: "Refresh", "zh-CN": "刷新", ja: "更新", ko: "새로고침" })}
          </Button>
        </div>

        <div className="grid gap-3 text-sm sm:grid-cols-[128px_1fr]">
          <span className="text-muted-foreground">
            {t({
              en: "Endpoint URL",
              "zh-CN": "端点 URL",
              ja: "エンドポイント URL",
              ko: "엔드포인트 URL",
            })}
          </span>
          <span className="break-all font-mono text-xs">{selected.url}</span>
          <span className="text-muted-foreground">{t({ en: "Events", "zh-CN": "订阅事件", ja: "購読イベント", ko: "구독 이벤트" })}</span>
          <div className="flex flex-wrap gap-1.5">
            {selected.event_types.map((eventType) => (
              <Badge key={eventType} variant="outline">
                {t(WEBHOOK_EVENT_LABELS[eventType])}
              </Badge>
            ))}
          </div>
        </div>

        <div className="overflow-x-auto border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>{t({ en: "Event", "zh-CN": "事件", ja: "イベント", ko: "이벤트" })}</TableHead>
                <TableHead>{t({ en: "Status", "zh-CN": "状态", ja: "ステータス", ko: "상태" })}</TableHead>
                <TableHead className="hidden md:table-cell">HTTP</TableHead>
                <TableHead className="hidden md:table-cell">{t({ en: "Attempts", "zh-CN": "尝试", ja: "試行回数", ko: "시도 횟수" })}</TableHead>
                <TableHead className="text-right">{t({ en: "Updated", "zh-CN": "更新时间", ja: "更新日時", ko: "업데이트 시간" })}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {deliveries.map((delivery) => (
                <TableRow key={delivery.id}>
                  <TableCell>
                    <div className="font-medium">{eventLabel(delivery.event_type, t)}</div>
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
                    {formatTime(delivery.updated_at_ms, locale)}
                  </TableCell>
                </TableRow>
              ))}
              {!deliveriesLoading && deliveries.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-32 text-center text-muted-foreground">
                    {deliveryError ?? t({ en: "No delivery history", "zh-CN": "尚无投递记录", ja: "配信履歴はありません", ko: "전송 기록이 없습니다" })}
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
            <h3 className="text-sm font-medium">
              {t({
                en: "Webhooks",
                "zh-CN": "Webhook",
                ja: "Webhook",
                ko: "Webhook",
              })}
            </h3>
            <p className="mt-1 text-sm text-muted-foreground">
              {t({ en: "Send image and video events from this project to your service with signed HTTP requests.", "zh-CN": "使用签名 HTTP 请求把项目中的图片和视频事件发送到你的服务。", ja: "署名付き HTTP リクエストで、このプロジェクトの画像・動画イベントをサービスに送信します。", ko: "서명된 HTTP 요청으로 이 프로젝트의 이미지 및 동영상 이벤트를 서비스에 전송합니다." })}
            </p>
          </div>
          <div className="flex shrink-0 gap-2">
            <Button
              variant="outline"
              size="icon"
              disabled={refreshing || loading}
              onClick={() => void loadEndpoints(true)}
              aria-label={t({ en: "Refresh webhooks", "zh-CN": "刷新 Webhooks", ja: "Webhook を更新", ko: "Webhook 새로고침" })}
              title={t({ en: "Refresh webhooks", "zh-CN": "刷新 Webhooks", ja: "Webhook を更新", ko: "Webhook 새로고침" })}
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
                {t({ en: "Create webhook", "zh-CN": "创建 Webhook", ja: "Webhook を作成", ko: "Webhook 만들기" })}
              </Button>
            ) : null}
          </div>
        </div>

        {loading ? (
          <div className="grid min-h-48 place-items-center text-muted-foreground">
            <LoaderCircle className="size-5 animate-spin" aria-label={t({ en: "Loading webhooks", "zh-CN": "正在加载 Webhooks", ja: "Webhook を読み込み中", ko: "Webhook 불러오는 중" })} />
          </div>
        ) : endpoints.length === 0 ? (
          <div className="grid min-h-56 place-items-center border border-dashed px-6 text-center">
            <div className="max-w-sm">
              <Webhook className="mx-auto mb-3 size-6 text-muted-foreground" aria-hidden="true" />
              <p className="text-sm font-medium">{t({ en: "No webhooks yet", "zh-CN": "尚未创建 Webhook", ja: "Webhook はまだありません", ko: "아직 Webhook이 없습니다" })}</p>
              <p className="mt-1 text-sm text-muted-foreground">
                {t({ en: "Create an endpoint to subscribe to project events and inspect each delivery.", "zh-CN": "创建端点后即可订阅项目事件并查看每次投递。", ja: "エンドポイントを作成すると、プロジェクトイベントを購読し、各配信を確認できます。", ko: "엔드포인트를 만들면 프로젝트 이벤트를 구독하고 각 전송을 확인할 수 있습니다." })}
              </p>
              {canManage ? (
                <Button
                  className="mt-4"
                  size="sm"
                  onClick={() => setDialogOpen(true)}
                >
                  <Plus aria-hidden="true" />
                  {t({ en: "Create webhook", "zh-CN": "创建 Webhook", ja: "Webhook を作成", ko: "Webhook 만들기" })}
                </Button>
              ) : null}
            </div>
          </div>
        ) : (
          <div className="overflow-x-auto border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>
                    {t({
                      en: "Endpoint",
                      "zh-CN": "端点",
                      ja: "エンドポイント",
                      ko: "엔드포인트",
                    })}
                  </TableHead>
                  <TableHead className="hidden lg:table-cell">{t({ en: "Events", "zh-CN": "订阅事件", ja: "購読イベント", ko: "구독 이벤트" })}</TableHead>
                  <TableHead>{t({ en: "Status", "zh-CN": "状态", ja: "ステータス", ko: "상태" })}</TableHead>
                  <TableHead className="hidden sm:table-cell">{t({ en: "Latest delivery", "zh-CN": "最近投递", ja: "最新の配信", ko: "최근 전송" })}</TableHead>
                  <TableHead className="w-12">
                    <span className="sr-only">{t({ en: "Actions", "zh-CN": "操作", ja: "操作", ko: "작업" })}</span>
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
                        {t(WEBHOOK_EVENT_LABELS[endpoint.event_types[0]])}
                        {endpoint.event_types.length > 1
                          ? t({ en: " and {count} more", "zh-CN": " 等 {count} 项", ja: "、ほか {count} 件", ko: " 외 {count}개" }, { count: endpoint.event_types.length - 1 })
                          : ""}
                      </span>
                    </TableCell>
                    <TableCell>
                      <Badge variant={endpoint.state === "active" ? "secondary" : "outline"}>
                        {endpoint.state === "active"
                          ? t({ en: "Active", "zh-CN": "启用", ja: "有効", ko: "활성" })
                          : t({ en: "Disabled", "zh-CN": "已停用", ja: "無効", ko: "비활성" })}
                      </Badge>
                    </TableCell>
                    <TableCell className="hidden sm:table-cell">
                      {endpoint.last_delivery_state ? (
                        <div>
                          <DeliveryBadge state={endpoint.last_delivery_state} />
                          <div className="mt-1 text-xs text-muted-foreground">
                            {formatTime(endpoint.last_delivery_at_ms, locale)}
                          </div>
                        </div>
                      ) : (
                        <span className="text-sm text-muted-foreground">{t({ en: "No deliveries", "zh-CN": "尚无投递", ja: "配信なし", ko: "전송 없음" })}</span>
                      )}
                    </TableCell>
                    <TableCell>
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon"
                            aria-label={t({ en: "Manage {endpoint}", "zh-CN": "管理 {endpoint}", ja: "{endpoint} を管理", ko: "{endpoint} 관리" }, { endpoint: endpoint.name || endpoint.id })}
                          >
                            <Ellipsis aria-hidden="true" />
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end">
                          <DropdownMenuItem onSelect={() => setSelected(endpoint)}>
                            <Webhook aria-hidden="true" />
                            {t({ en: "View deliveries", "zh-CN": "查看投递记录", ja: "配信履歴を表示", ko: "전송 기록 보기" })}
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
                                {t({ en: "Edit", "zh-CN": "编辑", ja: "編集", ko: "편집" })}
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                disabled={endpoint.state !== "active" || mutationPending}
                                onSelect={() => void enqueueTest(endpoint)}
                              >
                                <FlaskConical aria-hidden="true" />
                                {t({ en: "Send test event", "zh-CN": "发送测试事件", ja: "テストイベントを送信", ko: "테스트 이벤트 전송" })}
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                disabled={mutationPending}
                                onSelect={() => void toggleEndpoint(endpoint)}
                              >
                                <Play aria-hidden="true" />
                                {endpoint.state === "active"
                                  ? t({ en: "Disable", "zh-CN": "停用", ja: "無効化", ko: "비활성화" })
                                  : t({ en: "Enable", "zh-CN": "启用", ja: "有効化", ko: "활성화" })}
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                disabled={mutationPending}
                                onSelect={() => setRotateTarget(endpoint)}
                              >
                                <RotateCw aria-hidden="true" />
                                {t({ en: "Rotate signing secret", "zh-CN": "轮换签名密钥", ja: "署名シークレットをローテーション", ko: "서명 시크릿 교체" })}
                              </DropdownMenuItem>
                              <DropdownMenuSeparator />
                              <DropdownMenuItem
                                className="text-destructive focus:text-destructive"
                                disabled={mutationPending}
                                onSelect={() => setDeleteTarget(endpoint)}
                              >
                                <Trash2 aria-hidden="true" />
                                {t({ en: "Delete", "zh-CN": "删除", ja: "削除", ko: "삭제" })}
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
            {t({ en: "You can view endpoints and delivery history. Only project or organization owners can make changes.", "zh-CN": "你可以查看端点和投递记录；只有项目或组织所有者可以更改。", ja: "エンドポイントと配信履歴は閲覧できます。変更できるのはプロジェクトまたは組織の所有者のみです。", ko: "엔드포인트와 전송 기록을 볼 수 있습니다. 프로젝트 또는 조직 소유자만 변경할 수 있습니다." })}
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
            <AlertDialogTitle>{t({ en: "Save the webhook signing secret", "zh-CN": "保存 Webhook 签名密钥", ja: "Webhook 署名シークレットを保存", ko: "Webhook 서명 시크릿 저장" })}</AlertDialogTitle>
            <AlertDialogDescription>
              {t({ en: "This secret is shown only once. It cannot be viewed again after closing, but you can rotate it to generate a new one.", "zh-CN": "密钥只在本次响应中显示。关闭后无法再次查看，可通过轮换生成新的密钥。", ja: "このシークレットは今回のみ表示されます。閉じると再表示できませんが、ローテーションして新しいシークレットを生成できます。", ko: "이 시크릿은 이번에만 표시됩니다. 닫은 후에는 다시 볼 수 없지만 교체하여 새 시크릿을 만들 수 있습니다." })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          {secret ? (
            <div className="space-y-3">
              <div className="break-all border bg-muted/40 p-3 font-mono text-xs">
                {secret}
              </div>
              <Button className="w-full" variant="outline" onClick={() => void copySecret()}>
                {copied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
                {copied
                  ? t({ en: "Copied", "zh-CN": "已复制", ja: "コピー済み", ko: "복사됨" })
                  : t({ en: "Copy secret", "zh-CN": "复制密钥", ja: "シークレットをコピー", ko: "시크릿 복사" })}
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
              {t({ en: "I saved it. Close", "zh-CN": "我已保存并关闭", ja: "保存しました。閉じる", ko: "저장했습니다. 닫기" })}
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
            <AlertDialogTitle>{t({ en: "Rotate signing secret?", "zh-CN": "轮换签名密钥？", ja: "署名シークレットをローテーションしますか？", ko: "서명 시크릿을 교체할까요?" })}</AlertDialogTitle>
            <AlertDialogDescription>
              {t({ en: "The old secret immediately stops signing new requests. Be ready to update the receiving service at the same time.", "zh-CN": "旧密钥会立即停止签名后续请求。请准备好同步更新接收端配置。", ja: "古いシークレットは直ちに新しいリクエストへの署名を停止します。受信側の設定も同時に更新できるよう準備してください。", ko: "이전 시크릿은 즉시 새 요청 서명을 중지합니다. 수신 서비스 설정도 동시에 업데이트할 준비를 하세요." })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={mutationPending}>{t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}</AlertDialogCancel>
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
              {t({ en: "Rotate secret", "zh-CN": "轮换密钥", ja: "シークレットをローテーション", ko: "시크릿 교체" })}
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
            <AlertDialogTitle>{t({ en: "Delete webhook?", "zh-CN": "删除 Webhook？", ja: "Webhook を削除しますか？", ko: "Webhook을 삭제할까요?" })}</AlertDialogTitle>
            <AlertDialogDescription>
              {t({ en: "The endpoint will stop receiving new events and all undelivered attempts will be canceled. Historical delivery records remain available for audit.", "zh-CN": "端点会停止接收新事件，所有尚未发送的投递会被取消。历史投递记录仍保留用于审计。", ja: "エンドポイントは新しいイベントの受信を停止し、未配信の試行はすべてキャンセルされます。過去の配信履歴は監査用に保持されます。", ko: "엔드포인트가 새 이벤트 수신을 중지하고 아직 전송되지 않은 모든 시도가 취소됩니다. 과거 전송 기록은 감사를 위해 유지됩니다." })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={mutationPending}>{t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}</AlertDialogCancel>
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
              {t({ en: "Delete webhook", "zh-CN": "删除 Webhook", ja: "Webhook を削除", ko: "Webhook 삭제" })}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

function DeliveryBadge({ state }: { state: WebhookDeliveryState }) {
  const { t } = useI18n();
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
      {t(WEBHOOK_DELIVERY_LABELS[state])}
    </Badge>
  );
}

function eventLabel(
  eventType: string,
  t: ReturnType<typeof useI18n>["t"],
) {
  const label = WEBHOOK_EVENT_LABELS[eventType as keyof typeof WEBHOOK_EVENT_LABELS];
  if (label) return t(label);
  return eventType === "webhook.test"
    ? t({ en: "Test event", "zh-CN": "测试事件", ja: "テストイベント", ko: "테스트 이벤트" })
    : eventType;
}

function endpointDisplayUrl(value: string) {
  try {
    const url = new URL(value);
    return `${url.host}${url.pathname === "/" ? "" : url.pathname}`;
  } catch {
    return value;
  }
}

function formatTime(value: number | null, locale: string) {
  if (!value) return "—";
  return new Intl.DateTimeFormat(locale, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}
