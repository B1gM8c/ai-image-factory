"use client";

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  Ban,
  Download,
  FileJson2,
  LoaderCircle,
  Plus,
  RefreshCw,
  Search,
  Trash2,
  Upload,
} from "lucide-react";
import { toast } from "sonner";
import {
  BatchStatusBadge,
  batchStatusLabel,
} from "@/components/batches/batch-status-badge";
import { useConsoleSession } from "@/components/auth/console-session-provider";
import { PageHeader } from "@/components/page-header";
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
import { Progress } from "@/components/ui/progress";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  cancelProjectBatch,
  createProjectBatch,
  deleteProjectFile,
  downloadProjectFile,
  getProjectBatch,
  listProjectBatches,
  listProjectFiles,
  uploadProjectFile,
} from "@/lib/admin/client";
import type {
  ProjectBatch,
  ProjectBatchStatus,
  ProjectFile,
} from "@/lib/admin/types";
import { useI18n } from "@/i18n/locale-provider";

type BatchStatusFilter = ProjectBatchStatus | "all";
type FileMode = "upload" | "existing";

const MAX_BATCH_FILE_BYTES = 8 * 1024 * 1024;
const MAX_BATCH_REQUESTS = 1_000;
const ACTIVE_BATCH_STATES = new Set<ProjectBatchStatus>([
  "validating",
  "in_progress",
  "finalizing",
  "cancelling",
]);
const CANCELLABLE_BATCH_STATES = new Set<ProjectBatchStatus>([
  "validating",
  "in_progress",
  "finalizing",
]);

export function BatchWorkspace() {
  const { locale, t } = useI18n();
  const { activeWorkspace, loading: sessionLoading } = useConsoleSession();
  const projectId =
    activeWorkspace?.kind === "project" ? activeWorkspace.id : null;
  const batchRequest = useRef<AbortController | null>(null);
  const fileRequest = useRef<AbortController | null>(null);

  const [batches, setBatches] = useState<ProjectBatch[]>([]);
  const [files, setFiles] = useState<ProjectFile[]>([]);
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] =
    useState<BatchStatusFilter>("all");
  const [selectedBatchId, setSelectedBatchId] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [filesOpen, setFilesOpen] = useState(false);
  const [preferredInputFileId, setPreferredInputFileId] = useState<
    string | null
  >(null);
  const [cancelTarget, setCancelTarget] = useState<ProjectBatch | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<ProjectFile | null>(null);
  const [mutationPending, setMutationPending] = useState(false);
  const [downloadingId, setDownloadingId] = useState<string | null>(null);
  const [refreshingBatchId, setRefreshingBatchId] = useState<string | null>(
    null,
  );

  const loadBatches = useCallback(
    async (background = false) => {
      batchRequest.current?.abort();
      if (!projectId) {
        setBatches([]);
        setError(null);
        return;
      }
      const controller = new AbortController();
      batchRequest.current = controller;
      background ? setRefreshing(true) : setLoading(true);
      setError(null);
      try {
        const payload = await listProjectBatches(projectId, controller.signal);
        if (!controller.signal.aborted) setBatches(payload.data);
      } catch (reason) {
        if (!controller.signal.aborted) {
          setError(
            errorMessage(
              reason,
              t({
                en: "Failed to load batches",
                "zh-CN": "批次加载失败",
                ja: "バッチの読み込みに失敗しました",
                ko: "배치를 불러오지 못했습니다",
              }),
            ),
          );
          if (!background) setBatches([]);
        }
      } finally {
        if (batchRequest.current === controller) {
          batchRequest.current = null;
          background ? setRefreshing(false) : setLoading(false);
        }
      }
    },
    [projectId, t],
  );

  const loadFiles = useCallback(async () => {
    fileRequest.current?.abort();
    if (!projectId) {
      setFiles([]);
      return;
    }
    const controller = new AbortController();
    fileRequest.current = controller;
    try {
      const payload = await listProjectFiles(projectId, controller.signal);
      if (!controller.signal.aborted) setFiles(payload.data);
    } catch (reason) {
      if (!controller.signal.aborted) {
        toast.error(
          errorMessage(
            reason,
            t({
              en: "Failed to load input files",
              "zh-CN": "输入文件加载失败",
              ja: "入力ファイルの読み込みに失敗しました",
              ko: "입력 파일을 불러오지 못했습니다",
            }),
          ),
        );
      }
    } finally {
      if (fileRequest.current === controller) fileRequest.current = null;
    }
  }, [projectId, t]);

  useEffect(() => {
    setQuery("");
    setStatusFilter("all");
    setSelectedBatchId(null);
    setCreateOpen(false);
    setFilesOpen(false);
    void loadBatches();
    void loadFiles();
    return () => {
      batchRequest.current?.abort();
      fileRequest.current?.abort();
    };
  }, [loadBatches, loadFiles]);

  const visibleBatches = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase();
    return batches.filter(
      (batch) =>
        (statusFilter === "all" || batch.status === statusFilter) &&
        (!normalizedQuery ||
          batch.id.toLowerCase().includes(normalizedQuery) ||
          batch.input_file_id.toLowerCase().includes(normalizedQuery) ||
          Object.values(batch.metadata ?? {}).some((value) =>
            value.toLowerCase().includes(normalizedQuery),
          )),
    );
  }, [batches, query, statusFilter]);

  const selectedBatch =
    visibleBatches.find((batch) => batch.id === selectedBatchId) ??
    visibleBatches[0] ??
    null;

  useEffect(() => {
    if (selectedBatch && selectedBatch.id !== selectedBatchId) {
      setSelectedBatchId(selectedBatch.id);
    }
  }, [selectedBatch, selectedBatchId]);

  const hasActiveBatches = batches.some((batch) =>
    ACTIVE_BATCH_STATES.has(batch.status),
  );
  useEffect(() => {
    if (!hasActiveBatches || refreshing) return;
    const timer = window.setTimeout(() => void loadBatches(true), 3_000);
    return () => window.clearTimeout(timer);
  }, [hasActiveBatches, loadBatches, refreshing]);

  async function refreshBatch(batch: ProjectBatch) {
    if (!projectId) return;
    setRefreshingBatchId(batch.id);
    try {
      const next = await getProjectBatch(projectId, batch.id);
      setBatches((current) =>
        current.map((item) => (item.id === next.id ? next : item)),
      );
    } catch (reason) {
      toast.error(
        errorMessage(
          reason,
          t({
            en: "Failed to refresh batch status",
            "zh-CN": "批次状态刷新失败",
            ja: "バッチ状態の更新に失敗しました",
            ko: "배치 상태를 새로 고치지 못했습니다",
          }),
        ),
      );
    } finally {
      setRefreshingBatchId(null);
    }
  }

  async function cancelBatch() {
    if (!projectId || !cancelTarget) return;
    setMutationPending(true);
    try {
      const next = await cancelProjectBatch(projectId, cancelTarget.id);
      setBatches((current) =>
        current.map((batch) => (batch.id === next.id ? next : batch)),
      );
      setCancelTarget(null);
      toast.success(
        t({
          en: "Batch cancellation request submitted",
          "zh-CN": "批次取消请求已提交",
          ja: "バッチのキャンセルリクエストを送信しました",
          ko: "배치 취소 요청이 제출되었습니다",
        }),
      );
    } catch (reason) {
      toast.error(
        errorMessage(
          reason,
          t({
            en: "Failed to cancel batch",
            "zh-CN": "取消批次失败",
            ja: "バッチのキャンセルに失敗しました",
            ko: "배치를 취소하지 못했습니다",
          }),
        ),
      );
    } finally {
      setMutationPending(false);
    }
  }

  async function removeFile() {
    if (!projectId || !deleteTarget) return;
    setMutationPending(true);
    try {
      await deleteProjectFile(projectId, deleteTarget.id);
      setFiles((current) =>
        current.filter((file) => file.id !== deleteTarget.id),
      );
      setDeleteTarget(null);
      toast.success(
        t({
          en: "Input file deleted",
          "zh-CN": "输入文件已删除",
          ja: "入力ファイルを削除しました",
          ko: "입력 파일이 삭제되었습니다",
        }),
      );
    } catch (reason) {
      toast.error(
        errorMessage(
          reason,
          t({
            en: "Failed to delete input file",
            "zh-CN": "删除输入文件失败",
            ja: "入力ファイルの削除に失敗しました",
            ko: "입력 파일을 삭제하지 못했습니다",
          }),
        ),
      );
    } finally {
      setMutationPending(false);
    }
  }

  async function downloadFile(fileId: string, filename: string) {
    if (!projectId) return;
    setDownloadingId(fileId);
    try {
      const content = await downloadProjectFile(projectId, fileId);
      const url = URL.createObjectURL(content);
      const link = document.createElement("a");
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.setTimeout(() => URL.revokeObjectURL(url), 0);
    } catch (reason) {
      toast.error(
        errorMessage(
          reason,
          t({
            en: "File download failed",
            "zh-CN": "文件下载失败",
            ja: "ファイルのダウンロードに失敗しました",
            ko: "파일 다운로드에 실패했습니다",
          }),
        ),
      );
    } finally {
      setDownloadingId(null);
    }
  }

  function openCreate(inputFileId?: string) {
    setPreferredInputFileId(inputFileId ?? null);
    setCreateOpen(true);
  }

  if (sessionLoading) return <PageLoading />;

  if (!projectId) {
    return (
      <div className="grid min-h-[55vh] place-items-center px-6 text-center">
        <div className="max-w-sm">
          <FileJson2
            className="mx-auto size-6 text-muted-foreground"
            aria-hidden="true"
          />
          <h2 className="mt-4 text-base font-medium">
            {t({
              en: "Project context required",
              "zh-CN": "需要项目上下文",
              ja: "プロジェクトコンテキストが必要です",
              ko: "프로젝트 컨텍스트 필요",
            })}
          </h2>
          <p className="mt-2 text-sm text-muted-foreground">
            {t({
              en: "Create or switch to a project to manage batch jobs.",
              "zh-CN": "创建或切换到一个项目后，才能管理批处理任务。",
              ja: "バッチジョブを管理するには、プロジェクトを作成または切り替えてください。",
              ko: "배치 작업을 관리하려면 프로젝트를 만들거나 전환하세요.",
            })}
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-w-0 space-y-5">
      <PageHeader
        title={t({
          en: "Batches",
          "zh-CN": "批处理",
          ja: "バッチ",
          ko: "배치",
        })}
        description={t({
          en: "Submit image generation jobs as JSONL. Batch requests are billed at 50% of the synchronous price.",
          "zh-CN":
            "以 JSONL 提交图片生成任务，批量请求按同步价格的 50% 计费。",
          ja: "画像生成ジョブを JSONL で送信します。バッチリクエストは同期価格の 50% で請求されます。",
          ko: "이미지 생성 작업을 JSONL로 제출합니다. 배치 요청은 동기 가격의 50%로 청구됩니다.",
        })}
        actions={
          <>
            <Button
              type="button"
              variant="outline"
              size="icon"
              disabled={loading || refreshing}
              onClick={() => void loadBatches(true)}
              aria-label={t({
                en: "Refresh batches",
                "zh-CN": "刷新批次",
                ja: "バッチを更新",
                ko: "배치 새로 고침",
              })}
              title={t({
                en: "Refresh batches",
                "zh-CN": "刷新批次",
                ja: "バッチを更新",
                ko: "배치 새로 고침",
              })}
            >
              <RefreshCw
                className={refreshing ? "animate-spin" : undefined}
                aria-hidden="true"
              />
            </Button>
            <Button
              type="button"
              variant="outline"
              onClick={() => setFilesOpen(true)}
            >
              <FileJson2 aria-hidden="true" />
              {t({
                en: "Input files",
                "zh-CN": "输入文件",
                ja: "入力ファイル",
                ko: "입력 파일",
              })}
            </Button>
            <Button type="button" onClick={() => openCreate()}>
              <Plus aria-hidden="true" />
              {t({
                en: "Create",
                "zh-CN": "创建",
                ja: "作成",
                ko: "생성",
              })}
            </Button>
          </>
        }
      />

      <div className="grid min-h-[620px] min-w-0 overflow-hidden border lg:grid-cols-[minmax(320px,0.42fr)_minmax(0,0.58fr)]">
        <section className="min-w-0 border-b lg:border-b-0 lg:border-r">
          <div className="space-y-3 border-b p-4">
            <div className="relative">
              <Search
                className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
                aria-hidden="true"
              />
              <Input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder={t({
                  en: "Search Batch ID",
                  "zh-CN": "搜索 Batch ID",
                  ja: "Batch ID を検索",
                  ko: "Batch ID 검색",
                })}
                aria-label={t({
                  en: "Search batches",
                  "zh-CN": "搜索批次",
                  ja: "バッチを検索",
                  ko: "배치 검색",
                })}
                className="pl-9"
              />
            </div>
            <Select
              value={statusFilter}
              onValueChange={(value) =>
                setStatusFilter(value as BatchStatusFilter)
              }
            >
              <SelectTrigger
                aria-label={t({
                  en: "Filter batch status",
                  "zh-CN": "筛选批次状态",
                  ja: "バッチ状態で絞り込む",
                  ko: "배치 상태 필터링",
                })}
              >
                <SelectValue
                  placeholder={t({
                    en: "All statuses",
                    "zh-CN": "全部状态",
                    ja: "すべての状態",
                    ko: "모든 상태",
                  })}
                />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">
                  {t({
                    en: "All statuses",
                    "zh-CN": "全部状态",
                    ja: "すべての状態",
                    ko: "모든 상태",
                  })}
                </SelectItem>
                {(
                  [
                    "validating",
                    "in_progress",
                    "finalizing",
                    "completed",
                    "failed",
                    "expired",
                    "cancelling",
                    "cancelled",
                  ] as ProjectBatchStatus[]
                ).map((status) => (
                  <SelectItem key={status} value={status}>
                    {batchStatusLabel(t, status)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="max-h-[540px] overflow-y-auto">
            {loading ? <BatchListSkeleton /> : null}
            {!loading && error ? (
              <PaneState
                title={t({
                  en: "Batches are temporarily unavailable",
                  "zh-CN": "批次暂时不可用",
                  ja: "バッチは一時的に利用できません",
                  ko: "배치를 일시적으로 사용할 수 없습니다",
                })}
                description={error}
                action={
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={() => void loadBatches()}
                  >
                    <RefreshCw aria-hidden="true" />
                    {t({
                      en: "Retry",
                      "zh-CN": "重试",
                      ja: "再試行",
                      ko: "다시 시도",
                    })}
                  </Button>
                }
              />
            ) : null}
            {!loading && !error && visibleBatches.length === 0 ? (
              <PaneState
                title={t({
                  en: "No batches",
                  "zh-CN": "暂无批次",
                  ja: "バッチはありません",
                  ko: "배치 없음",
                })}
                description={
                  query || statusFilter !== "all"
                    ? t({
                        en: "No batches match the current filters.",
                        "zh-CN": "没有匹配当前筛选条件的批次。",
                        ja: "現在のフィルターに一致するバッチはありません。",
                        ko: "현재 필터와 일치하는 배치가 없습니다.",
                      })
                    : t({
                        en: "Execution progress will appear here after you create your first batch.",
                        "zh-CN": "创建第一个批次后，执行进度会显示在这里。",
                        ja: "最初のバッチを作成すると、実行進捗がここに表示されます。",
                        ko: "첫 번째 배치를 생성하면 실행 진행 상황이 여기에 표시됩니다.",
                      })
                }
              />
            ) : null}
            {!loading && !error
              ? visibleBatches.map((batch) => {
                  const counts = normalizedCounts(batch);
                  const selected = batch.id === selectedBatch?.id;
                  const progress =
                    counts.total > 0
                      ? ((counts.completed + counts.failed) / counts.total) *
                        100
                      : 0;
                  return (
                    <button
                      type="button"
                      key={batch.id}
                      className={`block w-full border-b p-4 text-left transition-colors hover:bg-muted/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset ${
                        selected ? "bg-muted" : ""
                      }`}
                      onClick={() => setSelectedBatchId(batch.id)}
                    >
                      <div className="flex min-w-0 items-center justify-between gap-3">
                        <span
                          className="truncate font-mono text-xs font-medium"
                          title={batch.id}
                        >
                          {batch.metadata?.name || batch.id}
                        </span>
                        <BatchStatusBadge status={batch.status} />
                      </div>
                      <p className="mt-2 text-xs text-muted-foreground">
                        {t(
                          {
                            en: "{processed} / {total} processed",
                            "zh-CN": "{processed} / {total} 已处理",
                            ja: "{processed} / {total} 処理済み",
                            ko: "{processed} / {total} 처리됨",
                          },
                          {
                            processed: counts.completed + counts.failed,
                            total: counts.total,
                          },
                        )}
                        {counts.failed > 0
                          ? t(
                              {
                                en: " · {count} failed",
                                "zh-CN": " · {count} 失败",
                                ja: " · {count} 件失敗",
                                ko: " · {count}건 실패",
                              },
                              { count: counts.failed },
                            )
                          : ""}
                      </p>
                      <Progress value={progress} className="mt-2 h-1" />
                      <p className="mt-2 text-xs text-muted-foreground">
                        {formatUnixSeconds(batch.created_at, locale)}
                      </p>
                    </button>
                  );
                })
              : null}
          </div>
        </section>

        <section className="min-w-0">
          {selectedBatch ? (
            <BatchDetail
              batch={selectedBatch}
              downloadingId={downloadingId}
              refreshing={refreshingBatchId === selectedBatch.id}
              onRefresh={() => void refreshBatch(selectedBatch)}
              onCancel={() => setCancelTarget(selectedBatch)}
              onDownload={(fileId, filename) =>
                void downloadFile(fileId, filename)
              }
            />
          ) : (
            <PaneState
              title={t({
                en: "Select a batch",
                "zh-CN": "选择一个批次",
                ja: "バッチを選択",
                ko: "배치 선택",
              })}
              description={t({
                en: "Batch status, request counts, and result files will appear here.",
                "zh-CN": "批次状态、请求计数和结果文件会显示在这里。",
                ja: "バッチ状態、リクエスト数、結果ファイルがここに表示されます。",
                ko: "배치 상태, 요청 수 및 결과 파일이 여기에 표시됩니다.",
              })}
            />
          )}
        </section>
      </div>

      <CreateBatchDialog
        projectId={projectId}
        files={files.filter((file) => file.purpose === "batch")}
        preferredInputFileId={preferredInputFileId}
        open={createOpen}
        onOpenChange={(open) => {
          setCreateOpen(open);
          if (!open) setPreferredInputFileId(null);
        }}
        onFileAdded={(file) =>
          setFiles((current) => [
            file,
            ...current.filter((item) => item.id !== file.id),
          ])
        }
        onCreated={(batch) => {
          setBatches((current) => [
            batch,
            ...current.filter((item) => item.id !== batch.id),
          ]);
          setSelectedBatchId(batch.id);
        }}
      />

      <FileManagerDialog
        open={filesOpen}
        files={files}
        downloadingId={downloadingId}
        onOpenChange={setFilesOpen}
        onCreateBatch={(fileId) => {
          setFilesOpen(false);
          openCreate(fileId);
        }}
        onDownload={(file) => void downloadFile(file.id, file.filename)}
        onDelete={setDeleteTarget}
      />

      <AlertDialog
        open={Boolean(cancelTarget)}
        onOpenChange={(open) => {
          if (!open && !mutationPending) setCancelTarget(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t({
                en: "Cancel this batch?",
                "zh-CN": "取消这个批次？",
                ja: "このバッチをキャンセルしますか？",
                ko: "이 배치를 취소할까요?",
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {t({
                en: "Completed requests keep their results and are billed normally. Requests that have not started will stop.",
                "zh-CN":
                  "已完成请求会保留结果并正常计费，尚未开始的请求将停止执行。",
                ja: "完了済みのリクエストは結果が保持され通常どおり請求されます。未開始のリクエストは停止します。",
                ko: "완료된 요청은 결과가 유지되고 정상적으로 청구됩니다. 시작하지 않은 요청은 중지됩니다.",
              })}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={mutationPending}>
              {t({
                en: "Back",
                "zh-CN": "返回",
                ja: "戻る",
                ko: "뒤로",
              })}
            </AlertDialogCancel>
            <AlertDialogAction
              disabled={mutationPending}
              onClick={(event) => {
                event.preventDefault();
                void cancelBatch();
              }}
            >
              {mutationPending ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <Ban aria-hidden="true" />
              )}
              {t({
                en: "Confirm cancellation",
                "zh-CN": "确认取消",
                ja: "キャンセルを確定",
                ko: "취소 확인",
              })}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog
        open={Boolean(deleteTarget)}
        onOpenChange={(open) => {
          if (!open && !mutationPending) setDeleteTarget(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {t({
                en: "Delete this input file?",
                "zh-CN": "删除这个输入文件？",
                ja: "この入力ファイルを削除しますか？",
                ko: "이 입력 파일을 삭제할까요?",
              })}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {t(
                {
                  en: "{filename} will be deleted from the current project. Files in use by a batch cannot be deleted.",
                  "zh-CN":
                    "{filename} 将从当前项目删除。正在被批次使用的文件无法删除。",
                  ja: "{filename} は現在のプロジェクトから削除されます。バッチで使用中のファイルは削除できません。",
                  ko: "{filename} 파일이 현재 프로젝트에서 삭제됩니다. 배치에서 사용 중인 파일은 삭제할 수 없습니다.",
                },
                { filename: deleteTarget?.filename ?? "" },
              )}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={mutationPending}>
              {t({
                en: "Cancel",
                "zh-CN": "取消",
                ja: "キャンセル",
                ko: "취소",
              })}
            </AlertDialogCancel>
            <AlertDialogAction
              disabled={mutationPending}
              onClick={(event) => {
                event.preventDefault();
                void removeFile();
              }}
            >
              {mutationPending ? (
                <LoaderCircle className="animate-spin" aria-hidden="true" />
              ) : (
                <Trash2 aria-hidden="true" />
              )}
              {t({
                en: "Delete",
                "zh-CN": "删除",
                ja: "削除",
                ko: "삭제",
              })}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

function BatchDetail({
  batch,
  downloadingId,
  refreshing,
  onRefresh,
  onCancel,
  onDownload,
}: {
  batch: ProjectBatch;
  downloadingId: string | null;
  refreshing: boolean;
  onRefresh: () => void;
  onCancel: () => void;
  onDownload: (fileId: string, filename: string) => void;
}) {
  const { locale, t } = useI18n();
  const counts = normalizedCounts(batch);
  const processed = counts.completed + counts.failed;
  const progress = counts.total > 0 ? (processed / counts.total) * 100 : 0;
  const firstError = batch.errors?.data[0]?.message;

  return (
    <div className="min-w-0">
      <header className="border-b p-5">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              <h2
                className="truncate font-mono text-sm font-semibold"
                title={batch.id}
              >
                {batch.metadata?.name || batch.id}
              </h2>
              <BatchStatusBadge status={batch.status} />
            </div>
            {batch.metadata?.name ? (
              <p
                className="mt-2 truncate font-mono text-xs text-muted-foreground"
                title={batch.id}
              >
                {batch.id}
              </p>
            ) : null}
          </div>
          <div className="flex shrink-0 items-center gap-2">
            <Button
              type="button"
              size="icon"
              variant="outline"
              onClick={onRefresh}
              disabled={refreshing}
              aria-label={t({
                en: "Refresh current batch",
                "zh-CN": "刷新当前批次",
                ja: "現在のバッチを更新",
                ko: "현재 배치 새로 고침",
              })}
              title={t({
                en: "Refresh current batch",
                "zh-CN": "刷新当前批次",
                ja: "現在のバッチを更新",
                ko: "현재 배치 새로 고침",
              })}
            >
              <RefreshCw
                className={refreshing ? "animate-spin" : undefined}
                aria-hidden="true"
              />
            </Button>
            {CANCELLABLE_BATCH_STATES.has(batch.status) ? (
              <Button type="button" variant="outline" onClick={onCancel}>
                <Ban aria-hidden="true" />
                {t({
                  en: "Cancel",
                  "zh-CN": "取消",
                  ja: "キャンセル",
                  ko: "취소",
                })}
              </Button>
            ) : null}
          </div>
        </div>
      </header>

      <div className="space-y-6 p-5">
        <section>
          <div className="flex items-end justify-between gap-4">
            <div>
              <p className="text-sm font-medium">
                {t({
                  en: "Execution progress",
                  "zh-CN": "执行进度",
                  ja: "実行進捗",
                  ko: "실행 진행률",
                })}
              </p>
              <p className="mt-1 text-xs text-muted-foreground">
                {t(
                  {
                    en: "{processed} / {total} processed",
                    "zh-CN": "{processed} / {total} 已处理",
                    ja: "{processed} / {total} 処理済み",
                    ko: "{processed} / {total} 처리됨",
                  },
                  { processed, total: counts.total },
                )}
              </p>
            </div>
            <p className="text-sm tabular-nums">{Math.round(progress)}%</p>
          </div>
          <Progress value={progress} className="mt-3 h-2" />
          <div className="mt-4 grid grid-cols-3 gap-3 text-sm">
            <Metric
              label={t({
                en: "Completed",
                "zh-CN": "完成",
                ja: "完了",
                ko: "완료",
              })}
              value={counts.completed}
            />
            <Metric
              label={t({
                en: "Failed",
                "zh-CN": "失败",
                ja: "失敗",
                ko: "실패",
              })}
              value={counts.failed}
            />
            <Metric
              label={t({
                en: "Pending",
                "zh-CN": "待处理",
                ja: "保留中",
                ko: "대기 중",
              })}
              value={Math.max(0, counts.total - processed)}
            />
          </div>
        </section>

        {firstError ? (
          <div className="border border-destructive/30 bg-destructive/5 p-4 text-sm text-destructive">
            {firstError}
          </div>
        ) : null}

        <section className="grid gap-x-8 gap-y-4 border-t pt-5 sm:grid-cols-2">
          <Detail
            label={t({
              en: "Endpoint",
              "zh-CN": "端点",
              ja: "エンドポイント",
              ko: "엔드포인트",
            })}
            value={batch.endpoint}
            mono
          />
          <Detail
            label={t({
              en: "Completion window",
              "zh-CN": "完成窗口",
              ja: "完了期限",
              ko: "완료 기간",
            })}
            value={batch.completion_window}
          />
          <Detail
            label={t({
              en: "Input file",
              "zh-CN": "输入文件",
              ja: "入力ファイル",
              ko: "입력 파일",
            })}
            value={batch.input_file_id}
            mono
          />
          <Detail
            label={t({
              en: "Created",
              "zh-CN": "创建时间",
              ja: "作成時刻",
              ko: "생성 시간",
            })}
            value={formatUnixSeconds(batch.created_at, locale)}
          />
          <Detail
            label={t({
              en: "Started",
              "zh-CN": "开始时间",
              ja: "開始時刻",
              ko: "시작 시간",
            })}
            value={
              batch.in_progress_at
                ? formatUnixSeconds(batch.in_progress_at, locale)
                : "--"
            }
          />
          <Detail
            label={t({
              en: "Completed",
              "zh-CN": "完成时间",
              ja: "完了時刻",
              ko: "완료 시간",
            })}
            value={
              batch.completed_at
                ? formatUnixSeconds(batch.completed_at, locale)
                : "--"
            }
          />
          <Detail
            label={t({
              en: "Expires",
              "zh-CN": "到期时间",
              ja: "有効期限",
              ko: "만료 시간",
            })}
            value={
              batch.expires_at
                ? formatUnixSeconds(batch.expires_at, locale)
                : "--"
            }
          />
          <Detail
            label={t({
              en: "Billing mode",
              "zh-CN": "计费模式",
              ja: "請求モード",
              ko: "청구 방식",
            })}
            value={t({
              en: "Batch · 50% of synchronous price",
              "zh-CN": "Batch · 同步价格 50%",
              ja: "Batch · 同期価格の 50%",
              ko: "Batch · 동기 가격의 50%",
            })}
          />
        </section>

        <section className="border-t pt-5">
          <p className="text-sm font-medium">
            {t({
              en: "Result files",
              "zh-CN": "结果文件",
              ja: "結果ファイル",
              ko: "결과 파일",
            })}
          </p>
          <div className="mt-3 flex flex-wrap gap-2">
            {batch.output_file_id ? (
              <Button
                type="button"
                variant="outline"
                disabled={downloadingId === batch.output_file_id}
                onClick={() =>
                  onDownload(
                    batch.output_file_id as string,
                    `${batch.id}-output.jsonl`,
                  )
                }
              >
                {downloadingId === batch.output_file_id ? (
                  <LoaderCircle className="animate-spin" aria-hidden="true" />
                ) : (
                  <Download aria-hidden="true" />
                )}
                {t({
                  en: "Download results",
                  "zh-CN": "下载结果",
                  ja: "結果をダウンロード",
                  ko: "결과 다운로드",
                })}
              </Button>
            ) : null}
            {batch.error_file_id ? (
              <Button
                type="button"
                variant="outline"
                disabled={downloadingId === batch.error_file_id}
                onClick={() =>
                  onDownload(
                    batch.error_file_id as string,
                    `${batch.id}-errors.jsonl`,
                  )
                }
              >
                <Download aria-hidden="true" />
                {t({
                  en: "Download errors",
                  "zh-CN": "下载错误",
                  ja: "エラーをダウンロード",
                  ko: "오류 다운로드",
                })}
              </Button>
            ) : null}
            {!batch.output_file_id && !batch.error_file_id ? (
              <p className="text-sm text-muted-foreground">
                {t({
                  en: "Result or error JSONL files are created when the batch reaches a terminal state.",
                  "zh-CN": "批次终结后生成结果或错误 JSONL。",
                  ja: "バッチが終端状態になると、結果またはエラーの JSONL が生成されます。",
                  ko: "배치가 최종 상태에 도달하면 결과 또는 오류 JSONL 파일이 생성됩니다.",
                })}
              </p>
            ) : null}
          </div>
        </section>
      </div>
    </div>
  );
}

function CreateBatchDialog({
  projectId,
  files,
  preferredInputFileId,
  open,
  onOpenChange,
  onFileAdded,
  onCreated,
}: {
  projectId: string;
  files: ProjectFile[];
  preferredInputFileId: string | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onFileAdded: (file: ProjectFile) => void;
  onCreated: (batch: ProjectBatch) => void;
}) {
  const { locale, t } = useI18n();
  const [mode, setMode] = useState<FileMode>("upload");
  const [selectedFileId, setSelectedFileId] = useState("");
  const [upload, setUpload] = useState<File | null>(null);
  const [name, setName] = useState("");
  const [pending, setPending] = useState(false);

  useEffect(() => {
    if (!open) return;
    const preferred = preferredInputFileId ?? files[0]?.id ?? "";
    setMode(preferredInputFileId || files.length > 0 ? "existing" : "upload");
    setSelectedFileId(preferred);
    setUpload(null);
    setName("");
  }, [files, open, preferredInputFileId]);

  const canSubmit =
    !pending &&
    (mode === "upload" ? Boolean(upload) : Boolean(selectedFileId));

  async function submit() {
    if (!canSubmit) return;
    setPending(true);
    try {
      let inputFileId = selectedFileId;
      if (mode === "upload") {
        if (!upload) return;
        if (upload.size > MAX_BATCH_FILE_BYTES) {
          throw new Error(
            t({
              en: "The JSONL file cannot exceed 8 MiB",
              "zh-CN": "JSONL 文件不能超过 8MiB",
              ja: "JSONL ファイルは 8 MiB を超えることができません",
              ko: "JSONL 파일은 8 MiB를 초과할 수 없습니다",
            }),
          );
        }
        if (!upload.name.toLowerCase().endsWith(".jsonl")) {
          throw new Error(
            t({
              en: "Select a .jsonl file",
              "zh-CN": "请选择 .jsonl 文件",
              ja: ".jsonl ファイルを選択してください",
              ko: ".jsonl 파일을 선택하세요",
            }),
          );
        }
        const file = await uploadProjectFile(projectId, upload);
        onFileAdded(file);
        inputFileId = file.id;
      }
      const batch = await createProjectBatch(projectId, {
        input_file_id: inputFileId,
        endpoint: "/v1/images/generations",
        completion_window: "24h",
        metadata: name.trim() ? { name: name.trim() } : undefined,
      });
      onCreated(batch);
      onOpenChange(false);
      toast.success(
        t({
          en: "Batch created",
          "zh-CN": "批次已创建",
          ja: "バッチを作成しました",
          ko: "배치가 생성되었습니다",
        }),
      );
    } catch (reason) {
      toast.error(
        errorMessage(
          reason,
          t({
            en: "Failed to create batch",
            "zh-CN": "创建批次失败",
            ja: "バッチの作成に失敗しました",
            ko: "배치를 생성하지 못했습니다",
          }),
        ),
      );
    } finally {
      setPending(false);
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!pending) onOpenChange(next);
      }}
    >
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>
            {t({
              en: "Create batch",
              "zh-CN": "创建批次",
              ja: "バッチを作成",
              ko: "배치 생성",
            })}
          </DialogTitle>
          <DialogDescription>
            {t({
              en: "Each JSONL line is one image generation request. Only the 24-hour completion window is currently supported.",
              "zh-CN":
                "JSONL 中每行是一条图片生成请求。当前仅支持 24 小时完成窗口。",
              ja: "JSONL の各行は 1 件の画像生成リクエストです。現在は 24 時間の完了期限のみ対応しています。",
              ko: "JSONL의 각 줄은 하나의 이미지 생성 요청입니다. 현재 24시간 완료 기간만 지원됩니다.",
            })}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-5 py-2">
          <div className="space-y-2">
            <Label>
              {t({
                en: "Input file",
                "zh-CN": "输入文件",
                ja: "入力ファイル",
                ko: "입력 파일",
              })}
            </Label>
            <div className="grid grid-cols-2 gap-1 bg-muted p-1">
              <Button
                type="button"
                variant={mode === "upload" ? "secondary" : "ghost"}
                className="shadow-none"
                onClick={() => setMode("upload")}
              >
                <Upload aria-hidden="true" />
                {t({
                  en: "Upload new file",
                  "zh-CN": "上传新文件",
                  ja: "新しいファイルをアップロード",
                  ko: "새 파일 업로드",
                })}
              </Button>
              <Button
                type="button"
                variant={mode === "existing" ? "secondary" : "ghost"}
                className="shadow-none"
                onClick={() => setMode("existing")}
                disabled={files.length === 0}
              >
                <FileJson2 aria-hidden="true" />
                {t({
                  en: "Choose existing file",
                  "zh-CN": "选择已有文件",
                  ja: "既存ファイルを選択",
                  ko: "기존 파일 선택",
                })}
              </Button>
            </div>
          </div>

          {mode === "upload" ? (
            <div className="space-y-2">
              <Label htmlFor="batch-file">
                {t({
                  en: "JSONL file",
                  "zh-CN": "JSONL 文件",
                  ja: "JSONL ファイル",
                  ko: "JSONL 파일",
                })}
              </Label>
              <Input
                id="batch-file"
                type="file"
                accept=".jsonl,application/jsonl,application/x-ndjson"
                disabled={pending}
                onChange={(event) =>
                  setUpload(event.target.files?.[0] ?? null)
                }
              />
              <p className="text-xs text-muted-foreground">
                {t(
                  {
                    en: "UTF-8 encoded, up to {count} lines or 8 MiB.",
                    "zh-CN": "UTF-8 编码，最多 {count} 行或 8MiB。",
                    ja: "UTF-8 エンコード、最大 {count} 行または 8 MiB。",
                    ko: "UTF-8 인코딩, 최대 {count}줄 또는 8 MiB.",
                  },
                  { count: MAX_BATCH_REQUESTS.toLocaleString(locale) },
                )}
              </p>
            </div>
          ) : (
            <div className="space-y-2">
              <Label htmlFor="batch-existing-file">
                {t({
                  en: "Existing file",
                  "zh-CN": "已有文件",
                  ja: "既存ファイル",
                  ko: "기존 파일",
                })}
              </Label>
              <Select
                value={selectedFileId}
                onValueChange={setSelectedFileId}
                disabled={pending}
              >
                <SelectTrigger id="batch-existing-file">
                  <SelectValue
                    placeholder={t({
                      en: "Select input file",
                      "zh-CN": "选择输入文件",
                      ja: "入力ファイルを選択",
                      ko: "입력 파일 선택",
                    })}
                  />
                </SelectTrigger>
                <SelectContent>
                  {files.map((file) => (
                    <SelectItem key={file.id} value={file.id}>
                      {file.filename}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}

          <div className="space-y-2">
            <Label htmlFor="batch-name">
              {t({
                en: "Name (optional)",
                "zh-CN": "名称（可选）",
                ja: "名前（任意）",
                ko: "이름(선택 사항)",
              })}
            </Label>
            <Input
              id="batch-name"
              value={name}
              maxLength={128}
              disabled={pending}
              onChange={(event) => setName(event.target.value)}
              placeholder={t({
                en: "For example: July asset generation",
                "zh-CN": "例如：七月素材生成",
                ja: "例：7 月の素材生成",
                ko: "예: 7월 에셋 생성",
              })}
            />
          </div>

          <div className="grid gap-3 border-t pt-4 sm:grid-cols-3">
            <SummaryItem
              label={t({
                en: "Endpoint",
                "zh-CN": "端点",
                ja: "エンドポイント",
                ko: "엔드포인트",
              })}
              value="/v1/images/generations"
            />
            <SummaryItem
              label={t({
                en: "Completion window",
                "zh-CN": "完成窗口",
                ja: "完了期限",
                ko: "완료 기간",
              })}
              value={t({
                en: "24 hours",
                "zh-CN": "24 小时",
                ja: "24 時間",
                ko: "24시간",
              })}
            />
            <SummaryItem
              label={t({
                en: "Result retention",
                "zh-CN": "结果保留",
                ja: "結果の保持期間",
                ko: "결과 보존",
              })}
              value={t({
                en: "30 days",
                "zh-CN": "30 天",
                ja: "30 日間",
                ko: "30일",
              })}
            />
          </div>
        </div>

        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={pending}
            onClick={() => onOpenChange(false)}
          >
            {t({
              en: "Cancel",
              "zh-CN": "取消",
              ja: "キャンセル",
              ko: "취소",
            })}
          </Button>
          <Button type="button" disabled={!canSubmit} onClick={() => void submit()}>
            {pending ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <Plus aria-hidden="true" />
            )}
            {t({
              en: "Create batch",
              "zh-CN": "创建批次",
              ja: "バッチを作成",
              ko: "배치 생성",
            })}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function FileManagerDialog({
  open,
  files,
  downloadingId,
  onOpenChange,
  onCreateBatch,
  onDownload,
  onDelete,
}: {
  open: boolean;
  files: ProjectFile[];
  downloadingId: string | null;
  onOpenChange: (open: boolean) => void;
  onCreateBatch: (fileId: string) => void;
  onDownload: (file: ProjectFile) => void;
  onDelete: (file: ProjectFile) => void;
}) {
  const { locale, t } = useI18n();

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>
            {t({
              en: "Input files",
              "zh-CN": "输入文件",
              ja: "入力ファイル",
              ko: "입력 파일",
            })}
          </DialogTitle>
          <DialogDescription>
            {t({
              en: "JSONL files in the current project that can be used to create batches.",
              "zh-CN": "当前项目中可用于创建批次的 JSONL 文件。",
              ja: "現在のプロジェクトでバッチ作成に使用できる JSONL ファイルです。",
              ko: "현재 프로젝트에서 배치를 생성하는 데 사용할 수 있는 JSONL 파일입니다.",
            })}
          </DialogDescription>
        </DialogHeader>
        <div className="max-h-[480px] overflow-auto border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>
                  {t({
                    en: "File",
                    "zh-CN": "文件",
                    ja: "ファイル",
                    ko: "파일",
                  })}
                </TableHead>
                <TableHead className="w-28">
                  {t({
                    en: "Size",
                    "zh-CN": "大小",
                    ja: "サイズ",
                    ko: "크기",
                  })}
                </TableHead>
                <TableHead className="w-36">
                  {t({
                    en: "Created",
                    "zh-CN": "创建时间",
                    ja: "作成時刻",
                    ko: "생성 시간",
                  })}
                </TableHead>
                <TableHead className="w-40">
                  <span className="sr-only">
                    {t({
                      en: "Actions",
                      "zh-CN": "操作",
                      ja: "操作",
                      ko: "작업",
                    })}
                  </span>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {files.map((file) => (
                <TableRow key={file.id}>
                  <TableCell className="max-w-0">
                    <p className="truncate font-medium" title={file.filename}>
                      {file.filename}
                    </p>
                    <p
                      className="truncate font-mono text-xs text-muted-foreground"
                      title={file.id}
                    >
                      {file.id}
                    </p>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {formatBytes(file.bytes)}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {formatUnixSeconds(file.created_at, locale)}
                  </TableCell>
                  <TableCell>
                    <div className="flex justify-end gap-1">
                      <Button
                        type="button"
                        size="icon"
                        variant="ghost"
                        onClick={() => onCreateBatch(file.id)}
                        aria-label={t(
                          {
                            en: "Create a batch with {filename}",
                            "zh-CN": "使用 {filename} 创建批次",
                            ja: "{filename} でバッチを作成",
                            ko: "{filename} 파일로 배치 생성",
                          },
                          { filename: file.filename },
                        )}
                        title={t({
                          en: "Create batch",
                          "zh-CN": "创建批次",
                          ja: "バッチを作成",
                          ko: "배치 생성",
                        })}
                      >
                        <Plus aria-hidden="true" />
                      </Button>
                      <Button
                        type="button"
                        size="icon"
                        variant="ghost"
                        disabled={downloadingId === file.id}
                        onClick={() => onDownload(file)}
                        aria-label={t(
                          {
                            en: "Download {filename}",
                            "zh-CN": "下载 {filename}",
                            ja: "{filename} をダウンロード",
                            ko: "{filename} 다운로드",
                          },
                          { filename: file.filename },
                        )}
                        title={t({
                          en: "Download",
                          "zh-CN": "下载",
                          ja: "ダウンロード",
                          ko: "다운로드",
                        })}
                      >
                        <Download aria-hidden="true" />
                      </Button>
                      <Button
                        type="button"
                        size="icon"
                        variant="ghost"
                        onClick={() => onDelete(file)}
                        aria-label={t(
                          {
                            en: "Delete {filename}",
                            "zh-CN": "删除 {filename}",
                            ja: "{filename} を削除",
                            ko: "{filename} 삭제",
                          },
                          { filename: file.filename },
                        )}
                        title={t({
                          en: "Delete",
                          "zh-CN": "删除",
                          ja: "削除",
                          ko: "삭제",
                        })}
                      >
                        <Trash2 aria-hidden="true" />
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
              {files.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={4} className="h-40 text-center">
                    <p className="text-sm font-medium">
                      {t({
                        en: "No input files",
                        "zh-CN": "暂无输入文件",
                        ja: "入力ファイルはありません",
                        ko: "입력 파일 없음",
                      })}
                    </p>
                    <p className="mt-1 text-sm text-muted-foreground">
                      {t({
                        en: "Upload your first JSONL file when creating a batch.",
                        "zh-CN": "在创建批次时上传第一个 JSONL 文件。",
                        ja: "バッチ作成時に最初の JSONL ファイルをアップロードしてください。",
                        ko: "배치를 생성할 때 첫 번째 JSONL 파일을 업로드하세요.",
                      })}
                    </p>
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
          </Table>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function PaneState({
  title,
  description,
  action,
}: {
  title: string;
  description: string;
  action?: React.ReactNode;
}) {
  return (
    <div className="grid min-h-[300px] place-items-center p-8 text-center">
      <div className="max-w-sm">
        <FileJson2
          className="mx-auto size-5 text-muted-foreground"
          aria-hidden="true"
        />
        <p className="mt-4 text-sm font-medium">{title}</p>
        <p className="mt-1 text-sm text-muted-foreground">{description}</p>
        {action ? <div className="mt-4">{action}</div> : null}
      </div>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div className="bg-muted/50 p-3">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="mt-1 text-lg font-semibold tabular-nums">{value}</p>
    </div>
  );
}

function Detail({
  label,
  value,
  mono = false,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div className="min-w-0">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p
        className={`mt-1 truncate text-sm ${mono ? "font-mono text-xs" : ""}`}
        title={value}
      >
        {value}
      </p>
    </div>
  );
}

function SummaryItem({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="mt-1 truncate text-sm font-medium" title={value}>
        {value}
      </p>
    </div>
  );
}

function BatchListSkeleton() {
  const { t } = useI18n();

  return (
    <div
      aria-label={t({
        en: "Loading batches",
        "zh-CN": "正在加载批次",
        ja: "バッチを読み込み中",
        ko: "배치 불러오는 중",
      })}
    >
      {[0, 1, 2, 3].map((row) => (
        <div key={row} className="space-y-3 border-b p-4">
          <div className="flex justify-between gap-4">
            <Skeleton className="h-4 w-36" />
            <Skeleton className="h-5 w-16" />
          </div>
          <Skeleton className="h-3 w-24" />
          <Skeleton className="h-1 w-full" />
        </div>
      ))}
    </div>
  );
}

function PageLoading() {
  const { t } = useI18n();

  return (
    <div
      className="space-y-5"
      aria-label={t({
        en: "Loading batches",
        "zh-CN": "正在加载批处理",
        ja: "バッチを読み込み中",
        ko: "배치 불러오는 중",
      })}
    >
      <div className="flex justify-between">
        <div className="space-y-2">
          <Skeleton className="h-7 w-28" />
          <Skeleton className="h-4 w-80 max-w-full" />
        </div>
        <Skeleton className="h-9 w-28" />
      </div>
      <Skeleton className="h-[620px] w-full" />
    </div>
  );
}

function normalizedCounts(batch: ProjectBatch) {
  return (
    batch.request_counts ?? {
      total: 0,
      completed: 0,
      failed: 0,
    }
  );
}

function formatBytes(bytes: number) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatUnixSeconds(value: number, locale: string) {
  return new Intl.DateTimeFormat(locale, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value * 1_000));
}

function errorMessage(reason: unknown, fallback: string) {
  return reason instanceof Error && reason.message ? reason.message : fallback;
}
