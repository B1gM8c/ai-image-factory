"use client";

import { Fragment, useEffect, useMemo, useState } from "react";
import {
  BookOpen,
  CheckCircle2,
  ExternalLink,
  FileDiff,
  Loader2,
  RotateCcw,
} from "lucide-react";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatDateTime, formatInteger, formatMoneyMicros } from "@/lib/admin/format";
import type {
  OfficialPriceCatalogDescriptor,
  OfficialPriceCatalogs,
  OfficialPriceSnapshotDiff,
  OfficialPriceSnapshotPreview,
  PriceComponentDraft,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

const CATALOG_ENDPOINT = "/api/gateway/admin/v1/pricing/official-catalogs";
const selectableStatuses = new Set<OfficialPriceSnapshotDiff["status"]>(["new", "changed"]);

export function OfficialPricingImportDialog({
  open,
  onOpenChange,
  onImported,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onImported: () => void;
}) {
  const [catalogs, setCatalogs] = useState<OfficialPriceCatalogDescriptor[]>([]);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [observingKey, setObservingKey] = useState<string | null>(null);
  const [preview, setPreview] = useState<OfficialPriceSnapshotPreview | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [applying, setApplying] = useState(false);

  useEffect(() => {
    if (!open) {
      setPreview(null);
      setSelected(new Set());
      setLoadError(null);
      return;
    }
    void loadCatalogs();
  }, [open]);

  const selectable = useMemo(
    () => preview?.differences.filter((item) => selectableStatuses.has(item.status)) ?? [],
    [preview],
  );

  async function loadCatalogs() {
    setLoading(true);
    setLoadError(null);
    try {
      const response = await consoleFetch(CATALOG_ENDPOINT);
      if (!response.ok) throw new Error(await responseMessage(response));
      const payload = (await response.json()) as OfficialPriceCatalogs;
      setCatalogs(payload.catalogs);
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : "官方价格目录加载失败");
    } finally {
      setLoading(false);
    }
  }

  async function observe(catalog: OfficialPriceCatalogDescriptor) {
    setObservingKey(catalog.catalog_key);
    try {
      const response = await consoleFetch(
        `${CATALOG_ENDPOINT}/${encodeURIComponent(catalog.catalog_key)}/snapshots`,
        { method: "POST", body: "{}" },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      const payload = (await response.json()) as OfficialPriceSnapshotPreview;
      setPreview(payload);
      if (payload.sync_run) {
        setCatalogs((current) => current.map((item) => (
          item.catalog_key === catalog.catalog_key
            ? { ...item, latest_sync_run: payload.sync_run }
            : item
        )));
      }
      setSelected(new Set(
        payload.differences
          .filter((item) => selectableStatuses.has(item.status))
          .map((item) => item.item_key),
      ));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "官方价格差异检查失败");
    } finally {
      setObservingKey(null);
    }
  }

  async function applySelected() {
    if (!preview || selected.size === 0) return;
    setApplying(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/admin/v1/pricing/source-snapshots/${preview.snapshot.snapshot_id}/apply`,
        {
          method: "POST",
          body: JSON.stringify({ item_keys: Array.from(selected) }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      const payload = (await response.json()) as OfficialPriceSnapshotPreview;
      setPreview(payload);
      setSelected(new Set());
      onImported();
      toast.success("官方价格已生成草稿，审核发布后才会参与计价");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "官方价格草稿生成失败");
    } finally {
      setApplying(false);
    }
  }

  function setItemChecked(itemKey: string, checked: boolean) {
    setSelected((current) => {
      const next = new Set(current);
      if (checked) next.add(itemKey);
      else next.delete(itemKey);
      return next;
    });
  }

  function handleOpenChange(nextOpen: boolean) {
    if (applying || observingKey) return;
    onOpenChange(nextOpen);
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="max-h-[calc(100dvh-2rem)] w-[calc(100%-2rem)] max-w-5xl gap-0 overflow-hidden p-0">
        <DialogHeader className="border-b px-6 py-5 pr-12">
          <DialogTitle>{preview ? "审核官方价格差异" : "同步官方价格"}</DialogTitle>
          <DialogDescription>
            {preview
              ? "仅选择新增或发生变化的条目。导入只创建草稿，不会自动改变线上计价。"
              : "从已核验的官方清单建立版本化同步记录和不可变快照，再人工审核差异。"}
          </DialogDescription>
        </DialogHeader>

        <div className="min-h-0 flex-1 overflow-y-auto px-6 py-5">
          {preview ? (
            <SnapshotPreview
              preview={preview}
              selected={selected}
              onCheckedChange={setItemChecked}
            />
          ) : (
            <CatalogList
              catalogs={catalogs}
              loading={loading}
              error={loadError}
              observingKey={observingKey}
              onRetry={loadCatalogs}
              onObserve={observe}
            />
          )}
        </div>

        <DialogFooter className="border-t bg-background px-6 py-4">
          {preview ? (
            <>
              <Button
                type="button"
                variant="outline"
                onClick={() => {
                  setPreview(null);
                  setSelected(new Set());
                }}
                disabled={applying}
              >
                <RotateCcw aria-hidden="true" />
                返回目录
              </Button>
              <Button
                type="button"
                onClick={applySelected}
                disabled={applying || selected.size === 0}
              >
                {applying ? <Loader2 className="animate-spin" aria-hidden="true" /> : <FileDiff aria-hidden="true" />}
                生成 {selected.size} 个价格草稿
              </Button>
            </>
          ) : (
            <Button
              type="button"
              variant="outline"
              onClick={() => handleOpenChange(false)}
              disabled={observingKey !== null}
            >
              关闭
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function CatalogList({
  catalogs,
  loading,
  error,
  observingKey,
  onRetry,
  onObserve,
}: {
  catalogs: OfficialPriceCatalogDescriptor[];
  loading: boolean;
  error: string | null;
  observingKey: string | null;
  onRetry: () => Promise<void>;
  onObserve: (catalog: OfficialPriceCatalogDescriptor) => Promise<void>;
}) {
  if (loading && catalogs.length === 0) {
    return (
      <div className="flex min-h-64 items-center justify-center text-sm text-muted-foreground">
        <Loader2 className="mr-2 size-4 animate-spin" aria-hidden="true" />
        正在读取官方目录
      </div>
    );
  }
  if (error) {
    return (
      <div className="flex min-h-64 flex-col items-center justify-center text-center">
        <p className="text-sm text-destructive">{error}</p>
        <Button type="button" variant="outline" size="sm" className="mt-4" onClick={() => void onRetry()}>
          重试
        </Button>
      </div>
    );
  }

  return (
    <div className="overflow-hidden rounded-md border">
      <Table className="min-w-[760px]">
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">官方目录</TableHead>
            <TableHead>条目</TableHead>
            <TableHead>最近同步</TableHead>
            <TableHead>核验方式</TableHead>
            <TableHead>来源</TableHead>
            <TableHead className="w-32 pr-4 text-right">操作</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {catalogs.map((catalog) => (
            <TableRow key={catalog.catalog_key}>
              <TableCell className="pl-4">
                <span className="block font-medium">{catalog.display_name}</span>
                <span className="mt-0.5 block text-xs text-muted-foreground">
                  {providerLabel(catalog.source_provider_id)} · {catalog.currency}
                </span>
              </TableCell>
              <TableCell>{catalog.available ? `${catalog.item_count} 个模型价格` : "--"}</TableCell>
              <TableCell>
                {catalog.available ? (
                  catalog.latest_sync_run ? (
                    <div className="text-sm">
                      <div className="flex items-center gap-2">
                        <CheckCircle2 className="size-4 text-emerald-600" aria-hidden="true" />
                        <span>{syncStateLabel(catalog.latest_sync_run.state)}</span>
                      </div>
                      <span className="mt-0.5 block text-xs text-muted-foreground">
                        {formatDateTime(catalog.latest_sync_run.completed_at_ms)}
                      </span>
                    </div>
                  ) : (
                    <span className="text-sm text-muted-foreground">尚未同步</span>
                  )
                ) : (
                  <span className="block max-w-72 whitespace-normal text-sm text-muted-foreground">
                    {catalog.unavailable_reason}
                  </span>
                )}
              </TableCell>
              <TableCell>
                <span className="block text-sm">{retrievalMethodLabel(catalog.retrieval_method)}</span>
                <span className="mt-0.5 block text-xs text-muted-foreground">
                  来源核验 {formatDateTime(catalog.source_checked_at_ms)}
                </span>
              </TableCell>
              <TableCell>
                <Button type="button" variant="link" className="h-auto px-0" asChild>
                  <a href={catalog.source_url} target="_blank" rel="noreferrer">
                    官方文档
                    <ExternalLink aria-hidden="true" />
                  </a>
                </Button>
              </TableCell>
              <TableCell className="pr-4 text-right">
                <Button
                  type="button"
                  size="sm"
                  variant={catalog.available ? "outline" : "ghost"}
                  disabled={!catalog.available || observingKey !== null}
                  onClick={() => void onObserve(catalog)}
                >
                  {observingKey === catalog.catalog_key ? (
                    <Loader2 className="animate-spin" aria-hidden="true" />
                  ) : (
                    <BookOpen aria-hidden="true" />
                  )}
                  同步并检查
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function SnapshotPreview({
  preview,
  selected,
  onCheckedChange,
}: {
  preview: OfficialPriceSnapshotPreview;
  selected: Set<string>;
  onCheckedChange: (itemKey: string, checked: boolean) => void;
}) {
  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-start gap-x-6 gap-y-2 text-sm">
        <div>
          <span className="text-muted-foreground">来源</span>
          <span className="ml-2 font-medium">{providerLabel(preview.snapshot.source_provider_id)}</span>
        </div>
        <div>
          <span className="text-muted-foreground">核验时间</span>
          <span className="ml-2">{formatDateTime(preview.snapshot.source_checked_at_ms)}</span>
        </div>
        <div className="min-w-0">
          <span className="text-muted-foreground">内容快照</span>
          <span className="ml-2 font-mono text-xs">{preview.snapshot.content_sha256.slice(0, 16)}</span>
        </div>
        {preview.sync_run ? (
          <>
            <div>
              <span className="text-muted-foreground">同步结果</span>
              <span className="ml-2 font-medium">{syncStateLabel(preview.sync_run.state)}</span>
            </div>
            <div className="min-w-0">
              <span className="text-muted-foreground">来源证据</span>
              <span className="ml-2 font-mono text-xs">{preview.sync_run.evidence_sha256.slice(0, 16)}</span>
            </div>
          </>
        ) : null}
        <Button type="button" variant="link" className="ml-auto h-auto px-0" asChild>
          <a href={preview.snapshot.source_url} target="_blank" rel="noreferrer">
            查看官方文档
            <ExternalLink aria-hidden="true" />
          </a>
        </Button>
      </div>

      <div className="overflow-hidden rounded-md border">
        <Table className="min-w-[900px]">
          <TableHeader>
            <TableRow>
              <TableHead className="w-12 pl-4"><span className="sr-only">选择</span></TableHead>
              <TableHead>模型</TableHead>
              <TableHead>供应商</TableHead>
              <TableHead>计价组件</TableHead>
              <TableHead className="pr-4">差异</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {preview.differences.map((item) => {
              const enabled = selectableStatuses.has(item.status);
              const changedComponents = item.component_differences.filter(
                (component) => component.status !== "unchanged",
              );
              return (
                <Fragment key={item.item_key}>
                  <TableRow data-state={selected.has(item.item_key) ? "selected" : undefined}>
                    <TableCell className="pl-4">
                      <Checkbox
                        aria-label={`选择 ${item.display_name}`}
                        checked={selected.has(item.item_key)}
                        disabled={!enabled}
                        onCheckedChange={(checked) => onCheckedChange(item.item_key, checked === true)}
                      />
                    </TableCell>
                    <TableCell>
                      <span className="block font-medium">{item.display_name}</span>
                      <span className="mt-0.5 block font-mono text-xs text-muted-foreground">{item.public_model_id}</span>
                    </TableCell>
                    <TableCell>{providerLabel(item.target_provider_id)}</TableCell>
                    <TableCell>
                      {item.component_count} 项 · {item.media_kind === "video" ? "视频" : "图片"}
                    </TableCell>
                    <TableCell className="pr-4"><DifferenceBadge status={item.status} /></TableCell>
                  </TableRow>
                  {changedComponents.length > 0 ? (
                    <TableRow className="hover:bg-transparent">
                      <TableCell />
                      <TableCell colSpan={4} className="pb-4 pt-0">
                        <div className="grid gap-2 pt-2 sm:grid-cols-2">
                          {changedComponents.map((component) => (
                            <ComponentDifference
                              key={component.component_key}
                              currency={preview.snapshot.currency}
                              component={component}
                            />
                          ))}
                        </div>
                      </TableCell>
                    </TableRow>
                  ) : null}
                </Fragment>
              );
            })}
          </TableBody>
        </Table>
      </div>
      <p className="text-xs text-muted-foreground">
        已存在草稿或已同步条目不会重复导入。冲突项需要先在价格簿中人工处理。
      </p>
    </div>
  );
}

function ComponentDifference({
  currency,
  component,
}: {
  currency: string;
  component: OfficialPriceSnapshotDiff["component_differences"][number];
}) {
  return (
    <div className="min-w-0 rounded-md bg-muted/40 px-3 py-2 text-xs">
      <div className="flex items-center justify-between gap-3">
        <span className="truncate font-medium">{metricLabel(component.observed ?? component.previous)}</span>
        <span className="text-muted-foreground">{componentStatusLabel(component.status)}</span>
      </div>
      <div className="mt-1 flex min-w-0 items-center gap-2 font-mono tabular-nums">
        <span className="truncate text-muted-foreground">
          {component.previous ? componentRate(component.previous, currency) : "未配置"}
        </span>
        <span aria-hidden="true">→</span>
        <span className="truncate font-medium">
          {component.observed ? componentRate(component.observed, currency) : "已移除"}
        </span>
      </div>
    </div>
  );
}

function DifferenceBadge({ status }: { status: OfficialPriceSnapshotDiff["status"] }) {
  const labels: Record<OfficialPriceSnapshotDiff["status"], string> = {
    new: "新增",
    changed: "官方价格有变化",
    removed: "官方目录已移除",
    unchanged: "已同步",
    draft_exists: "已有草稿",
    conflict: "需人工处理",
  };
  return (
    <Badge variant={status === "conflict" ? "destructive" : status === "new" || status === "changed" ? "default" : "outline"}>
      {labels[status]}
    </Badge>
  );
}

function providerLabel(providerId: string) {
  if (providerId === "openai" || providerId === "openai-codex") return "OpenAI";
  if (providerId === "xai" || providerId === "xai-grok") return "xAI";
  if (providerId === "volcengine-ark") return "火山方舟";
  return providerId;
}

function retrievalMethodLabel(method: OfficialPriceCatalogDescriptor["retrieval_method"]) {
  return {
    curated_manifest: "人工核验清单",
    official_api: "官方 API",
    official_document: "官方文档解析",
  }[method];
}

function syncStateLabel(state: "changed" | "unchanged" | "invalid") {
  return {
    changed: "发现新版本",
    unchanged: "内容未变化",
    invalid: "核验失败",
  }[state];
}

function componentStatusLabel(status: OfficialPriceSnapshotDiff["component_differences"][number]["status"]) {
  return {
    added: "新增",
    removed: "移除",
    changed: "价格变化",
    unchanged: "未变化",
  }[status];
}

function metricLabel(component: PriceComponentDraft | null) {
  if (!component) return "计量项";
  const labels: Record<string, string> = {
    text_input_token: "文本输入",
    cached_text_input_token: "缓存文本输入",
    image_input_token: "图片输入 Token",
    cached_image_input_token: "缓存图片输入",
    image_output_token: "图片输出 Token",
    image_input: "输入图片",
    image_output: "输出图片",
    video_input_second: "输入视频秒数",
    video_output_second: "输出视频秒数",
  };
  const dimension = typeof component.dimensions.resolution === "string"
    ? ` · ${component.dimensions.resolution}`
    : "";
  return `${labels[component.metric] ?? component.metric}${dimension}`;
}

function componentRate(component: PriceComponentDraft, currency: string) {
  return `${formatMoneyMicros(component.unit_price_micros, currency)} / ${formatInteger(component.unit_size)} ${unitLabel(component.unit)}`;
}

function unitLabel(unit: string) {
  return {
    token: "Token",
    image: "张",
    second: "秒",
  }[unit] ?? unit;
}

async function responseMessage(response: Response) {
  try {
    const payload = (await response.json()) as { error?: string | { message?: string } };
    if (typeof payload.error === "string") return payload.error;
    if (payload.error && typeof payload.error.message === "string") return payload.error.message;
  } catch {
    // Fall through to status text.
  }
  return response.statusText || `请求失败 (${response.status})`;
}
