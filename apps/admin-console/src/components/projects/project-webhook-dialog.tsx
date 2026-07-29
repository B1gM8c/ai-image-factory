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
      setError("请输入 Endpoint URL");
      return;
    }
    if (eventTypes.length === 0) {
      setError("至少选择一个订阅事件");
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
      if (!response.ok) throw new Error(await responseMessage(response));
      if (endpoint) {
        toast.success("Webhook 已更新");
      } else {
        const created = (await response.json()) as CreatedProjectWebhook;
        onSecret(created.signing_secret);
        toast.success("Webhook 已创建");
      }
      onOpenChange(false);
      await onSaved();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Webhook 保存失败");
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
            <DialogTitle>{endpoint ? "编辑 Webhook" : "创建 Webhook"}</DialogTitle>
            <DialogDescription>
              项目事件会使用 Standard Webhooks 签名后发送到这个地址。
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-2">
            <Label htmlFor="webhook-name">名称（可选）</Label>
            <Input
              id="webhook-name"
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="例如：生产环境媒体事件"
              maxLength={128}
              disabled={saving}
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="webhook-url">Endpoint URL</Label>
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
              生产环境仅允许解析到公网地址的 HTTPS URL，不跟随重定向。
            </p>
          </div>

          <fieldset className="space-y-3">
            <legend className="text-sm font-medium">订阅事件</legend>
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
                      aria-label={WEBHOOK_EVENT_LABELS[eventType]}
                    />
                    <span className="min-w-0">
                      <span className="block text-sm">
                        {WEBHOOK_EVENT_LABELS[eventType]}
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
              取消
            </Button>
            <Button type="submit" disabled={saving || !url.trim()}>
              {saving ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : endpoint ? (
                <Save aria-hidden="true" />
              ) : (
                <Webhook aria-hidden="true" />
              )}
              {endpoint ? "保存更改" : "创建 Webhook"}
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
