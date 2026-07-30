"use client";

import { useEffect, useState } from "react";
import { LoaderCircle, Save, Webhook } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
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
import {
  WEBHOOK_EVENT_LABELS,
  WEBHOOK_EVENT_TYPES,
  type CreatedProjectWebhook,
  type ProjectWebhookEndpoint,
  type WebhookEventType,
} from "./project-webhook-types";

export function ProjectWebhookDialog({
  projectId,
  open,
  endpoint,
  onOpenChange,
  onSaved,
  onSecret,
}: {
  projectId: string;
  open: boolean;
  endpoint: ProjectWebhookEndpoint | null;
  onOpenChange: (open: boolean) => void;
  onSaved: () => void | Promise<void>;
  onSecret: (secret: string) => void;
}) {
  const { t } = useI18n();
  const [name, setName] = useState("");
  const [url, setUrl] = useState("");
  const [eventTypes, setEventTypes] = useState<WebhookEventType[]>([
    ...WEBHOOK_EVENT_TYPES,
  ]);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setName(endpoint?.name ?? "");
    setUrl(endpoint?.url ?? "");
    setEventTypes(
      endpoint?.event_types.length
        ? endpoint.event_types
        : [...WEBHOOK_EVENT_TYPES],
    );
    setError(null);
  }, [endpoint, open]);

  function toggleEvent(eventType: WebhookEventType, checked: boolean) {
    setEventTypes((current) =>
      checked
        ? [...new Set([...current, eventType])]
        : current.filter((value) => value !== eventType),
    );
  }

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!url.trim()) {
      setError(t({ en: "Enter an Endpoint URL", "zh-CN": "请输入 Endpoint URL", ja: "Endpoint URL を入力してください", ko: "Endpoint URL을 입력하세요" }));
      return;
    }
    if (eventTypes.length === 0) {
      setError(t({ en: "Select at least one event", "zh-CN": "至少选择一个订阅事件", ja: "少なくとも 1 つのイベントを選択してください", ko: "이벤트를 하나 이상 선택하세요" }));
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const path = endpoint
        ? `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/webhooks/${encodeURIComponent(endpoint.id)}`
        : `/api/gateway/v1/organization/projects/${encodeURIComponent(projectId)}/webhooks`;
      const response = await consoleFetch(path, {
        method: endpoint ? "PATCH" : "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(
          endpoint
            ? {
                name: name.trim() || null,
                url: url.trim(),
                event_types: eventTypes,
                state: endpoint.state,
                expected_control_version: endpoint.control_version,
              }
            : {
                name: name.trim() || null,
                url: url.trim(),
                event_types: eventTypes,
              },
        ),
      });
      if (!response.ok) {
        throw new Error(
          await responseMessage(
            response,
            t({ en: "Request failed", "zh-CN": "请求失败", ja: "リクエストに失敗しました", ko: "요청에 실패했습니다" }),
          ),
        );
      }
      if (endpoint) {
        toast.success(t({ en: "Webhook updated", "zh-CN": "Webhook 已更新", ja: "Webhook を更新しました", ko: "Webhook을 업데이트했습니다" }));
      } else {
        const created = (await response.json()) as CreatedProjectWebhook;
        onSecret(created.signing_secret);
        toast.success(t({ en: "Webhook created", "zh-CN": "Webhook 已创建", ja: "Webhook を作成しました", ko: "Webhook을 만들었습니다" }));
      }
      onOpenChange(false);
      await onSaved();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : t({ en: "Failed to save webhook", "zh-CN": "Webhook 保存失败", ja: "Webhook を保存できませんでした", ko: "Webhook을 저장하지 못했습니다" }));
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!saving) onOpenChange(next);
      }}
    >
      <DialogContent className="max-h-[calc(100dvh-2rem)] w-[calc(100%-2rem)] overflow-y-auto sm:max-w-xl">
        <form onSubmit={submit} className="space-y-5">
          <DialogHeader>
            <DialogTitle>
              {endpoint
                ? t({ en: "Edit webhook", "zh-CN": "编辑 Webhook", ja: "Webhook を編集", ko: "Webhook 편집" })
                : t({ en: "Create webhook", "zh-CN": "创建 Webhook", ja: "Webhook を作成", ko: "Webhook 만들기" })}
            </DialogTitle>
            <DialogDescription>
              {t({ en: "Project events are signed using Standard Webhooks and sent to this URL.", "zh-CN": "项目事件会使用 Standard Webhooks 签名后发送到这个地址。", ja: "プロジェクトイベントは Standard Webhooks で署名され、この URL に送信されます。", ko: "프로젝트 이벤트는 Standard Webhooks로 서명된 후 이 URL로 전송됩니다." })}
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-2">
            <Label htmlFor="webhook-name">{t({ en: "Name (optional)", "zh-CN": "名称（可选）", ja: "名前（任意）", ko: "이름(선택 사항)" })}</Label>
            <Input
              id="webhook-name"
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder={t({ en: "For example: Production media events", "zh-CN": "例如：生产环境媒体事件", ja: "例: 本番メディアイベント", ko: "예: 프로덕션 미디어 이벤트" })}
              maxLength={128}
              disabled={saving}
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="webhook-url">
              {t({
                en: "Endpoint URL",
                "zh-CN": "端点 URL",
                ja: "エンドポイント URL",
                ko: "엔드포인트 URL",
              })}
            </Label>
            <Input
              id="webhook-url"
              type="url"
              value={url}
              onChange={(event) => setUrl(event.target.value)}
              placeholder="https://example.com/webhooks/media"
              autoComplete="off"
              disabled={saving}
            />
            <p className="text-xs leading-5 text-muted-foreground">
              {t({ en: "Production only accepts HTTPS URLs that resolve to public addresses. Redirects are not followed.", "zh-CN": "生产环境仅允许解析到公网地址的 HTTPS URL，不跟随重定向。", ja: "本番環境では、公開アドレスに解決される HTTPS URL のみ使用できます。リダイレクトには追従しません。", ko: "프로덕션에서는 공개 주소로 확인되는 HTTPS URL만 허용하며 리디렉션을 따르지 않습니다." })}
            </p>
          </div>

          <fieldset className="space-y-3">
            <legend className="text-sm font-medium">{t({ en: "Events", "zh-CN": "订阅事件", ja: "購読イベント", ko: "구독 이벤트" })}</legend>
            <div className="max-h-64 space-y-1 overflow-y-auto rounded-md border p-2">
              {WEBHOOK_EVENT_TYPES.map((eventType) => {
                const checked = eventTypes.includes(eventType);
                return (
                  <label
                    key={eventType}
                    className="flex cursor-pointer items-start gap-3 rounded-sm px-2 py-2 hover:bg-muted/60"
                  >
                    <Checkbox
                      checked={checked}
                      onCheckedChange={(value) =>
                        toggleEvent(eventType, value === true)
                      }
                      disabled={saving}
                      aria-label={t(WEBHOOK_EVENT_LABELS[eventType])}
                    />
                    <span className="min-w-0">
                      <span className="block text-sm">
                        {t(WEBHOOK_EVENT_LABELS[eventType])}
                      </span>
                      <span className="block break-all font-mono text-xs text-muted-foreground">
                        {eventType}
                      </span>
                    </span>
                  </label>
                );
              })}
            </div>
          </fieldset>

          {error ? (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          ) : null}

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={saving}
            >
              {t({ en: "Cancel", "zh-CN": "取消", ja: "キャンセル", ko: "취소" })}
            </Button>
            <Button type="submit" disabled={saving || !url.trim()}>
              {saving ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : endpoint ? (
                <Save aria-hidden="true" />
              ) : (
                <Webhook aria-hidden="true" />
              )}
              {endpoint
                ? t({ en: "Save changes", "zh-CN": "保存更改", ja: "変更を保存", ko: "변경 사항 저장" })
                : t({ en: "Create webhook", "zh-CN": "创建 Webhook", ja: "Webhook を作成", ko: "Webhook 만들기" })}
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
