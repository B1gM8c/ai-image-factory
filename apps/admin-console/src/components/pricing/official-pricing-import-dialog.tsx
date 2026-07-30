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
import { useI18n } from "@/i18n/locale-provider";

const CATALOG_ENDPOINT = "/api/gateway/admin/v1/pricing/official-catalogs";
const selectableStatuses = new Set<OfficialPriceSnapshotDiff["status"]>(["new", "changed"]);
type Translate = ReturnType<typeof useI18n>["t"];

export function OfficialPricingImportDialog({
  open,
  onOpenChange,
  onImported,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onImported: () => void;
}) {
  const { t } = useI18n();
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
      if (!response.ok) {
        throw new Error(await responseMessage(response, t({
          en: "Failed to load official pricing catalogs",
          "zh-CN": "官方价格目录加载失败",
          ja: "公式価格カタログを読み込めませんでした",
          ko: "공식 가격 카탈로그를 불러오지 못했습니다",
        })));
      }
      const payload = (await response.json()) as OfficialPriceCatalogs;
      setCatalogs(payload.catalogs);
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : t({
        en: "Failed to load official pricing catalogs",
        "zh-CN": "官方价格目录加载失败",
        ja: "公式価格カタログを読み込めませんでした",
        ko: "공식 가격 카탈로그를 불러오지 못했습니다",
      }));
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
      if (!response.ok) {
        throw new Error(await responseMessage(response, t({
          en: "Failed to check official pricing differences",
          "zh-CN": "官方价格差异检查失败",
          ja: "公式価格の差分を確認できませんでした",
          ko: "공식 가격 차이를 확인하지 못했습니다",
        })));
      }
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
      toast.error(error instanceof Error ? error.message : t({
        en: "Failed to check official pricing differences",
        "zh-CN": "官方价格差异检查失败",
        ja: "公式価格の差分を確認できませんでした",
        ko: "공식 가격 차이를 확인하지 못했습니다",
      }));
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
      if (!response.ok) {
        throw new Error(await responseMessage(response, t({
          en: "Failed to create pricing drafts from official prices",
          "zh-CN": "官方价格草稿生成失败",
          ja: "公式価格から価格下書きを作成できませんでした",
          ko: "공식 가격으로 가격 초안을 만들지 못했습니다",
        })));
      }
      const payload = (await response.json()) as OfficialPriceSnapshotPreview;
      setPreview(payload);
      setSelected(new Set());
      onImported();
      toast.success(t({
        en: "Official prices were imported as drafts. They will take effect only after review and publication.",
        "zh-CN": "官方价格已生成草稿，审核发布后才会参与计价",
        ja: "公式価格を下書きとして取り込みました。レビューして公開するまで課金には反映されません。",
        ko: "공식 가격을 초안으로 가져왔습니다. 검토 후 게시해야 과금에 반영됩니다.",
      }));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t({
        en: "Failed to create pricing drafts from official prices",
        "zh-CN": "官方价格草稿生成失败",
        ja: "公式価格から価格下書きを作成できませんでした",
        ko: "공식 가격으로 가격 초안을 만들지 못했습니다",
      }));
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
          <DialogTitle>
            {preview
              ? t({
                en: "Review official pricing changes",
                "zh-CN": "审核官方价格差异",
                ja: "公式価格の差分を確認",
                ko: "공식 가격 변경 검토",
              })
              : t({
                en: "Sync official pricing",
                "zh-CN": "同步官方价格",
                ja: "公式価格を同期",
                ko: "공식 가격 동기화",
              })}
          </DialogTitle>
          <DialogDescription>
            {preview
              ? t({
                en: "Select only new or changed entries. Importing creates drafts and does not change live pricing automatically.",
                "zh-CN": "仅选择新增或发生变化的条目。导入只创建草稿，不会自动改变线上计价。",
                ja: "新規または変更された項目のみ選択してください。取り込みでは下書きのみ作成され、公開中の価格は自動的に変更されません。",
                ko: "새 항목 또는 변경된 항목만 선택하세요. 가져오기는 초안만 만들며 운영 가격을 자동으로 변경하지 않습니다.",
              })
              : t({
                en: "Create versioned sync records and immutable snapshots from verified official catalogs, then review changes before importing.",
                "zh-CN": "从已核验的官方清单建立版本化同步记录和不可变快照，再人工审核差异。",
                ja: "検証済みの公式カタログからバージョン付き同期記録と不変スナップショットを作成し、差分を確認してから取り込みます。",
                ko: "검증된 공식 카탈로그에서 버전 관리 동기화 기록과 변경 불가 스냅샷을 만든 뒤 차이를 검토합니다.",
              })}
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
                {t({
                  en: "Back to catalogs",
                  "zh-CN": "返回目录",
                  ja: "カタログに戻る",
                  ko: "카탈로그로 돌아가기",
                })}
              </Button>
              <Button
                type="button"
                onClick={applySelected}
                disabled={applying || selected.size === 0}
              >
                {applying ? <Loader2 className="animate-spin" aria-hidden="true" /> : <FileDiff aria-hidden="true" />}
                {t({
                  en: "Create pricing drafts ({count})",
                  "zh-CN": "生成 {count} 个价格草稿",
                  ja: "価格下書きを作成（{count}件）",
                  ko: "가격 초안 만들기 ({count}개)",
                }, { count: selected.size })}
              </Button>
            </>
          ) : (
            <Button
              type="button"
              variant="outline"
              onClick={() => handleOpenChange(false)}
              disabled={observingKey !== null}
            >
              {t({
                en: "Close",
                "zh-CN": "关闭",
                ja: "閉じる",
                ko: "닫기",
              })}
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
  const { t } = useI18n();
  if (loading && catalogs.length === 0) {
    return (
      <div className="flex min-h-64 items-center justify-center text-sm text-muted-foreground">
        <Loader2 className="mr-2 size-4 animate-spin" aria-hidden="true" />
        {t({
          en: "Loading official catalogs",
          "zh-CN": "正在读取官方目录",
          ja: "公式カタログを読み込んでいます",
          ko: "공식 카탈로그를 불러오는 중",
        })}
      </div>
    );
  }
  if (error) {
    return (
      <div className="flex min-h-64 flex-col items-center justify-center text-center">
        <p className="text-sm text-destructive">{error}</p>
        <Button type="button" variant="outline" size="sm" className="mt-4" onClick={() => void onRetry()}>
          {t({
            en: "Retry",
            "zh-CN": "重试",
            ja: "再試行",
            ko: "다시 시도",
          })}
        </Button>
      </div>
    );
  }

  return (
    <div className="overflow-hidden rounded-md border">
      <Table className="min-w-[760px]">
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">{t({ en: "Official catalog", "zh-CN": "官方目录", ja: "公式カタログ", ko: "공식 카탈로그" })}</TableHead>
            <TableHead>{t({ en: "Entries", "zh-CN": "条目", ja: "項目", ko: "항목" })}</TableHead>
            <TableHead>{t({ en: "Latest sync", "zh-CN": "最近同步", ja: "最新の同期", ko: "최근 동기화" })}</TableHead>
            <TableHead>{t({ en: "Verification", "zh-CN": "核验方式", ja: "検証方法", ko: "검증 방식" })}</TableHead>
            <TableHead>{t({ en: "Source", "zh-CN": "来源", ja: "ソース", ko: "출처" })}</TableHead>
            <TableHead className="w-32 pr-4 text-right">{t({ en: "Actions", "zh-CN": "操作", ja: "操作", ko: "작업" })}</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {catalogs.map((catalog) => (
            <TableRow key={catalog.catalog_key}>
              <TableCell className="pl-4">
                <span className="block font-medium">{catalog.display_name}</span>
                <span className="mt-0.5 block text-xs text-muted-foreground">
                  {providerLabel(catalog.source_provider_id, t)} · {catalog.currency}
                </span>
              </TableCell>
              <TableCell>
                {catalog.available
                  ? t({
                    en: "{count} model prices",
                    "zh-CN": "{count} 个模型价格",
                    ja: "{count}件のモデル価格",
                    ko: "모델 가격 {count}개",
                  }, { count: catalog.item_count })
                  : "--"}
              </TableCell>
              <TableCell>
                {catalog.available ? (
                  catalog.latest_sync_run ? (
                    <div className="text-sm">
                      <div className="flex items-center gap-2">
                        <CheckCircle2 className="size-4 text-emerald-600" aria-hidden="true" />
                        <span>{syncStateLabel(catalog.latest_sync_run.state, t)}</span>
                      </div>
                      <span className="mt-0.5 block text-xs text-muted-foreground">
                        {formatDateTime(catalog.latest_sync_run.completed_at_ms)}
                      </span>
                    </div>
                  ) : (
                    <span className="text-sm text-muted-foreground">
                      {t({ en: "Not synced yet", "zh-CN": "尚未同步", ja: "未同期", ko: "아직 동기화되지 않음" })}
                    </span>
                  )
                ) : (
                  <span className="block max-w-72 whitespace-normal text-sm text-muted-foreground">
                    {catalog.unavailable_reason}
                  </span>
                )}
              </TableCell>
              <TableCell>
                <span className="block text-sm">{retrievalMethodLabel(catalog.retrieval_method, t)}</span>
                <span className="mt-0.5 block text-xs text-muted-foreground">
                  {t({
                    en: "Source verified {time}",
                    "zh-CN": "来源核验 {time}",
                    ja: "ソース検証 {time}",
                    ko: "출처 검증 {time}",
                  }, { time: formatDateTime(catalog.source_checked_at_ms) })}
                </span>
              </TableCell>
              <TableCell>
                <Button type="button" variant="link" className="h-auto px-0" asChild>
                  <a href={catalog.source_url} target="_blank" rel="noreferrer">
                    {t({ en: "Official documentation", "zh-CN": "官方文档", ja: "公式ドキュメント", ko: "공식 문서" })}
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
                  {t({
                    en: "Sync and review",
                    "zh-CN": "同步并检查",
                    ja: "同期して確認",
                    ko: "동기화 및 검토",
                  })}
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
  const { t } = useI18n();
  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-start gap-x-6 gap-y-2 text-sm">
        <div>
          <span className="text-muted-foreground">{t({ en: "Source", "zh-CN": "来源", ja: "ソース", ko: "출처" })}</span>
          <span className="ml-2 font-medium">{providerLabel(preview.snapshot.source_provider_id, t)}</span>
        </div>
        <div>
          <span className="text-muted-foreground">{t({ en: "Verified", "zh-CN": "核验时间", ja: "検証日時", ko: "검증 시간" })}</span>
          <span className="ml-2">{formatDateTime(preview.snapshot.source_checked_at_ms)}</span>
        </div>
        <div className="min-w-0">
          <span className="text-muted-foreground">{t({ en: "Content snapshot", "zh-CN": "内容快照", ja: "コンテンツスナップショット", ko: "콘텐츠 스냅샷" })}</span>
          <span className="ml-2 font-mono text-xs">{preview.snapshot.content_sha256.slice(0, 16)}</span>
        </div>
        {preview.sync_run ? (
          <>
            <div>
              <span className="text-muted-foreground">{t({ en: "Sync result", "zh-CN": "同步结果", ja: "同期結果", ko: "동기화 결과" })}</span>
              <span className="ml-2 font-medium">{syncStateLabel(preview.sync_run.state, t)}</span>
            </div>
            <div className="min-w-0">
              <span className="text-muted-foreground">{t({ en: "Source evidence", "zh-CN": "来源证据", ja: "ソース証跡", ko: "출처 증거" })}</span>
              <span className="ml-2 font-mono text-xs">{preview.sync_run.evidence_sha256.slice(0, 16)}</span>
            </div>
          </>
        ) : null}
        <Button type="button" variant="link" className="ml-auto h-auto px-0" asChild>
          <a href={preview.snapshot.source_url} target="_blank" rel="noreferrer">
            {t({ en: "View official documentation", "zh-CN": "查看官方文档", ja: "公式ドキュメントを表示", ko: "공식 문서 보기" })}
            <ExternalLink aria-hidden="true" />
          </a>
        </Button>
      </div>

      <div className="overflow-hidden rounded-md border">
        <Table className="min-w-[900px]">
          <TableHeader>
            <TableRow>
              <TableHead className="w-12 pl-4"><span className="sr-only">{t({ en: "Select", "zh-CN": "选择", ja: "選択", ko: "선택" })}</span></TableHead>
              <TableHead>{t({ en: "Model", "zh-CN": "模型", ja: "モデル", ko: "모델" })}</TableHead>
              <TableHead>{t({ en: "Provider", "zh-CN": "供应商", ja: "プロバイダー", ko: "제공업체" })}</TableHead>
              <TableHead>{t({ en: "Pricing components", "zh-CN": "计价组件", ja: "価格コンポーネント", ko: "가격 구성 요소" })}</TableHead>
              <TableHead className="pr-4">{t({ en: "Difference", "zh-CN": "差异", ja: "差分", ko: "차이" })}</TableHead>
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
                        aria-label={t({
                          en: "Select {name}",
                          "zh-CN": "选择 {name}",
                          ja: "{name}を選択",
                          ko: "{name} 선택",
                        }, { name: item.display_name })}
                        checked={selected.has(item.item_key)}
                        disabled={!enabled}
                        onCheckedChange={(checked) => onCheckedChange(item.item_key, checked === true)}
                      />
                    </TableCell>
                    <TableCell>
                      <span className="block font-medium">{item.display_name}</span>
                      <span className="mt-0.5 block font-mono text-xs text-muted-foreground">{item.public_model_id}</span>
                    </TableCell>
                    <TableCell>{providerLabel(item.target_provider_id, t)}</TableCell>
                    <TableCell>
                      {t({
                        en: "{count} components",
                        "zh-CN": "{count} 项",
                        ja: "{count}項目",
                        ko: "{count}개 구성 요소",
                      }, { count: item.component_count })}
                      {" · "}
                      {item.media_kind === "video"
                        ? t({ en: "Video", "zh-CN": "视频", ja: "動画", ko: "동영상" })
                        : t({ en: "Image", "zh-CN": "图片", ja: "画像", ko: "이미지" })}
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
        {t({
          en: "Existing drafts and previously synced entries are not imported again. Resolve conflicts manually in the price book first.",
          "zh-CN": "已存在草稿或已同步条目不会重复导入。冲突项需要先在价格簿中人工处理。",
          ja: "既存の下書きや同期済み項目は再取り込みされません。競合は先に価格表で手動解決してください。",
          ko: "기존 초안과 이미 동기화된 항목은 다시 가져오지 않습니다. 충돌 항목은 가격표에서 먼저 수동으로 해결하세요.",
        })}
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
  const { t } = useI18n();
  return (
    <div className="min-w-0 rounded-md bg-muted/40 px-3 py-2 text-xs">
      <div className="flex items-center justify-between gap-3">
        <span className="truncate font-medium">{metricLabel(component.observed ?? component.previous, t)}</span>
        <span className="text-muted-foreground">{componentStatusLabel(component.status, t)}</span>
      </div>
      <div className="mt-1 flex min-w-0 items-center gap-2 font-mono tabular-nums">
        <span className="truncate text-muted-foreground">
          {component.previous
            ? componentRate(component.previous, currency, t)
            : t({ en: "Not configured", "zh-CN": "未配置", ja: "未設定", ko: "설정되지 않음" })}
        </span>
        <span aria-hidden="true">→</span>
        <span className="truncate font-medium">
          {component.observed
            ? componentRate(component.observed, currency, t)
            : t({ en: "Removed", "zh-CN": "已移除", ja: "削除済み", ko: "삭제됨" })}
        </span>
      </div>
    </div>
  );
}

function DifferenceBadge({ status }: { status: OfficialPriceSnapshotDiff["status"] }) {
  const { t } = useI18n();
  const labels: Record<OfficialPriceSnapshotDiff["status"], string> = {
    new: t({ en: "New", "zh-CN": "新增", ja: "新規", ko: "신규" }),
    changed: t({ en: "Official price changed", "zh-CN": "官方价格有变化", ja: "公式価格が変更", ko: "공식 가격 변경됨" }),
    removed: t({ en: "Removed from official catalog", "zh-CN": "官方目录已移除", ja: "公式カタログから削除", ko: "공식 카탈로그에서 삭제됨" }),
    unchanged: t({ en: "Synced", "zh-CN": "已同步", ja: "同期済み", ko: "동기화됨" }),
    draft_exists: t({ en: "Draft exists", "zh-CN": "已有草稿", ja: "下書きあり", ko: "초안 있음" }),
    conflict: t({ en: "Manual review required", "zh-CN": "需人工处理", ja: "手動確認が必要", ko: "수동 검토 필요" }),
  };
  return (
    <Badge variant={status === "conflict" ? "destructive" : status === "new" || status === "changed" ? "default" : "outline"}>
      {labels[status]}
    </Badge>
  );
}

function providerLabel(providerId: string, t: Translate) {
  if (providerId === "openai" || providerId === "openai-codex") return "OpenAI";
  if (providerId === "xai" || providerId === "xai-grok") return "xAI";
  if (providerId === "volcengine-ark") {
    return t({ en: "Volcengine Ark", "zh-CN": "火山方舟", ja: "Volcengine Ark", ko: "Volcengine Ark" });
  }
  return providerId;
}

function retrievalMethodLabel(method: OfficialPriceCatalogDescriptor["retrieval_method"], t: Translate) {
  return {
    curated_manifest: t({ en: "Manually verified catalog", "zh-CN": "人工核验清单", ja: "手動検証済みカタログ", ko: "수동 검증 카탈로그" }),
    official_api: t({ en: "Official API", "zh-CN": "官方 API", ja: "公式 API", ko: "공식 API" }),
    official_document: t({ en: "Official documentation parser", "zh-CN": "官方文档解析", ja: "公式ドキュメント解析", ko: "공식 문서 파싱" }),
  }[method];
}

function syncStateLabel(state: "changed" | "unchanged" | "invalid", t: Translate) {
  return {
    changed: t({ en: "New version found", "zh-CN": "发现新版本", ja: "新しいバージョンを検出", ko: "새 버전 발견" }),
    unchanged: t({ en: "No content changes", "zh-CN": "内容未变化", ja: "内容に変更なし", ko: "콘텐츠 변경 없음" }),
    invalid: t({ en: "Verification failed", "zh-CN": "核验失败", ja: "検証に失敗", ko: "검증 실패" }),
  }[state];
}

function componentStatusLabel(
  status: OfficialPriceSnapshotDiff["component_differences"][number]["status"],
  t: Translate,
) {
  return {
    added: t({ en: "Added", "zh-CN": "新增", ja: "追加", ko: "추가됨" }),
    removed: t({ en: "Removed", "zh-CN": "移除", ja: "削除", ko: "삭제됨" }),
    changed: t({ en: "Price changed", "zh-CN": "价格变化", ja: "価格変更", ko: "가격 변경" }),
    unchanged: t({ en: "Unchanged", "zh-CN": "未变化", ja: "変更なし", ko: "변경 없음" }),
  }[status];
}

function metricLabel(component: PriceComponentDraft | null, t: Translate) {
  if (!component) {
    return t({ en: "Metered item", "zh-CN": "计量项", ja: "計量項目", ko: "계량 항목" });
  }
  const labels: Record<string, string> = {
    text_input_token: t({ en: "Text input", "zh-CN": "文本输入", ja: "テキスト入力", ko: "텍스트 입력" }),
    cached_text_input_token: t({ en: "Cached text input", "zh-CN": "缓存文本输入", ja: "キャッシュ済みテキスト入力", ko: "캐시된 텍스트 입력" }),
    image_input_token: t({ en: "Image input tokens", "zh-CN": "图片输入 Token", ja: "画像入力トークン", ko: "이미지 입력 토큰" }),
    cached_image_input_token: t({ en: "Cached image input", "zh-CN": "缓存图片输入", ja: "キャッシュ済み画像入力", ko: "캐시된 이미지 입력" }),
    image_output_token: t({ en: "Image output tokens", "zh-CN": "图片输出 Token", ja: "画像出力トークン", ko: "이미지 출력 토큰" }),
    image_input: t({ en: "Input images", "zh-CN": "输入图片", ja: "入力画像", ko: "입력 이미지" }),
    image_output: t({ en: "Output images", "zh-CN": "输出图片", ja: "出力画像", ko: "출력 이미지" }),
    video_input_second: t({ en: "Video input seconds", "zh-CN": "输入视频秒数", ja: "動画入力秒数", ko: "동영상 입력 초" }),
    video_output_second: t({ en: "Video output seconds", "zh-CN": "输出视频秒数", ja: "動画出力秒数", ko: "동영상 출력 초" }),
  };
  const dimension = typeof component.dimensions.resolution === "string"
    ? ` · ${component.dimensions.resolution}`
    : "";
  return `${labels[component.metric] ?? component.metric}${dimension}`;
}

function componentRate(component: PriceComponentDraft, currency: string, t: Translate) {
  return `${formatMoneyMicros(component.unit_price_micros, currency)} / ${formatInteger(component.unit_size)} ${unitLabel(component.unit, t)}`;
}

function unitLabel(unit: string, t: Translate) {
  return {
    token: "Token",
    image: t({ en: "images", "zh-CN": "张", ja: "枚", ko: "장" }),
    second: t({ en: "seconds", "zh-CN": "秒", ja: "秒", ko: "초" }),
  }[unit] ?? unit;
}

async function responseMessage(response: Response, fallback: string) {
  try {
    const payload = (await response.json()) as { error?: string | { message?: string } };
    if (typeof payload.error === "string") return payload.error;
    if (payload.error && typeof payload.error.message === "string") return payload.error.message;
  } catch {
    // Fall through to status text.
  }
  return response.statusText || `${fallback} (${response.status})`;
}
