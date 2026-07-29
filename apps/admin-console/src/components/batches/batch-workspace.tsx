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
          setError(errorMessage(reason, "批次加载失败"));
          if (!background) setBatches([]);
        }
      } finally {
        if (batchRequest.current === controller) {
          batchRequest.current = null;
          background ? setRefreshing(false) : setLoading(false);
        }
      }
    },
    [projectId],
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
        toast.error(errorMessage(reason, "输入文件加载失败"));
      }
    } finally {
      if (fileRequest.current === controller) fileRequest.current = null;
    }
  }, [projectId]);

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
      toast.error(errorMessage(reason, "批次状态刷新失败"));
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
      toast.success("批次取消请求已提交");
    } catch (reason) {
      toast.error(errorMessage(reason, "取消批次失败"));
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
      toast.success("输入文件已删除");
    } catch (reason) {
      toast.error(errorMessage(reason, "删除输入文件失败"));
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
      toast.error(errorMessage(reason, "文件下载失败"));
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
          <h2 className="mt-4 text-base font-medium">需要项目上下文</h2>
          <p className="mt-2 text-sm text-muted-foreground">
            创建或切换到一个项目后，才能管理批处理任务。
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-w-0 space-y-5">
      <PageHeader
        title="批处理"
        description="以 JSONL 提交图片生成任务，批量请求按同步价格的 50% 计费。"
        actions={
          <>
            <Button
              type="button"
              variant="outline"
              size="icon"
              disabled={loading || refreshing}
              onClick={() => void loadBatches(true)}
              aria-label="刷新批次"
              title="刷新批次"
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
              输入文件
            </Button>
            <Button type="button" onClick={() => openCreate()}>
              <Plus aria-hidden="true" />
              创建
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
                placeholder="搜索 Batch ID"
                aria-label="搜索批次"
                className="pl-9"
              />
            </div>
            <Select
              value={statusFilter}
              onValueChange={(value) =>
                setStatusFilter(value as BatchStatusFilter)
              }
            >
              <SelectTrigger aria-label="筛选批次状态">
                <SelectValue placeholder="全部状态" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部状态</SelectItem>
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
                    {batchStatusLabel(status)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="max-h-[540px] overflow-y-auto">
            {loading ? <BatchListSkeleton /> : null}
            {!loading && error ? (
              <PaneState
                title="批次暂时不可用"
                description={error}
                action={
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={() => void loadBatches()}
                  >
                    <RefreshCw aria-hidden="true" />
                    重试
                  </Button>
                }
              />
            ) : null}
            {!loading && !error && visibleBatches.length === 0 ? (
              <PaneState
                title="暂无批次"
                description={
                  query || statusFilter !== "all"
                    ? "没有匹配当前筛选条件的批次。"
                    : "创建第一个批次后，执行进度会显示在这里。"
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
                        {counts.completed + counts.failed} / {counts.total} 已处理
                        {counts.failed > 0 ? ` · ${counts.failed} 失败` : ""}
                      </p>
                      <Progress value={progress} className="mt-2 h-1" />
                      <p className="mt-2 text-xs text-muted-foreground">
                        {formatUnixSeconds(batch.created_at)}
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
              title="选择一个批次"
              description="批次状态、请求计数和结果文件会显示在这里。"
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
            <AlertDialogTitle>取消这个批次？</AlertDialogTitle>
            <AlertDialogDescription>
              已完成请求会保留结果并正常计费，尚未开始的请求将停止执行。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={mutationPending}>
              返回
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
              确认取消
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
            <AlertDialogTitle>删除这个输入文件？</AlertDialogTitle>
            <AlertDialogDescription>
              {deleteTarget?.filename} 将从当前项目删除。正在被批次使用的文件无法删除。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={mutationPending}>
              取消
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
              删除
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
              aria-label="刷新当前批次"
              title="刷新当前批次"
            >
              <RefreshCw
                className={refreshing ? "animate-spin" : undefined}
                aria-hidden="true"
              />
            </Button>
            {CANCELLABLE_BATCH_STATES.has(batch.status) ? (
              <Button type="button" variant="outline" onClick={onCancel}>
                <Ban aria-hidden="true" />
                取消
              </Button>
            ) : null}
          </div>
        </div>
      </header>

      <div className="space-y-6 p-5">
        <section>
          <div className="flex items-end justify-between gap-4">
            <div>
              <p className="text-sm font-medium">执行进度</p>
              <p className="mt-1 text-xs text-muted-foreground">
                {processed} / {counts.total} 已处理
              </p>
            </div>
            <p className="text-sm tabular-nums">{Math.round(progress)}%</p>
          </div>
          <Progress value={progress} className="mt-3 h-2" />
          <div className="mt-4 grid grid-cols-3 gap-3 text-sm">
            <Metric label="完成" value={counts.completed} />
            <Metric label="失败" value={counts.failed} />
            <Metric
              label="待处理"
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
          <Detail label="端点" value={batch.endpoint} mono />
          <Detail label="完成窗口" value={batch.completion_window} />
          <Detail label="输入文件" value={batch.input_file_id} mono />
          <Detail
            label="创建时间"
            value={formatUnixSeconds(batch.created_at)}
          />
          <Detail
            label="开始时间"
            value={
              batch.in_progress_at
                ? formatUnixSeconds(batch.in_progress_at)
                : "--"
            }
          />
          <Detail
            label="完成时间"
            value={
              batch.completed_at
                ? formatUnixSeconds(batch.completed_at)
                : "--"
            }
          />
          <Detail
            label="到期时间"
            value={
              batch.expires_at ? formatUnixSeconds(batch.expires_at) : "--"
            }
          />
          <Detail label="计费模式" value="Batch · 同步价格 50%" />
        </section>

        <section className="border-t pt-5">
          <p className="text-sm font-medium">结果文件</p>
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
                下载结果
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
                下载错误
              </Button>
            ) : null}
            {!batch.output_file_id && !batch.error_file_id ? (
              <p className="text-sm text-muted-foreground">
                批次终结后生成结果或错误 JSONL。
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
          throw new Error("JSONL 文件不能超过 8MiB");
        }
        if (!upload.name.toLowerCase().endsWith(".jsonl")) {
          throw new Error("请选择 .jsonl 文件");
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
      toast.success("批次已创建");
    } catch (reason) {
      toast.error(errorMessage(reason, "创建批次失败"));
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
          <DialogTitle>创建批次</DialogTitle>
          <DialogDescription>
            JSONL 中每行是一条图片生成请求。当前仅支持 24 小时完成窗口。
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-5 py-2">
          <div className="space-y-2">
            <Label>输入文件</Label>
            <div className="grid grid-cols-2 gap-1 bg-muted p-1">
              <Button
                type="button"
                variant={mode === "upload" ? "secondary" : "ghost"}
                className="shadow-none"
                onClick={() => setMode("upload")}
              >
                <Upload aria-hidden="true" />
                上传新文件
              </Button>
              <Button
                type="button"
                variant={mode === "existing" ? "secondary" : "ghost"}
                className="shadow-none"
                onClick={() => setMode("existing")}
                disabled={files.length === 0}
              >
                <FileJson2 aria-hidden="true" />
                选择已有文件
              </Button>
            </div>
          </div>

          {mode === "upload" ? (
            <div className="space-y-2">
              <Label htmlFor="batch-file">JSONL 文件</Label>
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
                UTF-8 编码，最多 {MAX_BATCH_REQUESTS.toLocaleString()} 行或
                8MiB。
              </p>
            </div>
          ) : (
            <div className="space-y-2">
              <Label htmlFor="batch-existing-file">已有文件</Label>
              <Select
                value={selectedFileId}
                onValueChange={setSelectedFileId}
                disabled={pending}
              >
                <SelectTrigger id="batch-existing-file">
                  <SelectValue placeholder="选择输入文件" />
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
            <Label htmlFor="batch-name">名称（可选）</Label>
            <Input
              id="batch-name"
              value={name}
              maxLength={128}
              disabled={pending}
              onChange={(event) => setName(event.target.value)}
              placeholder="例如：七月素材生成"
            />
          </div>

          <div className="grid gap-3 border-t pt-4 sm:grid-cols-3">
            <SummaryItem label="端点" value="/v1/images/generations" />
            <SummaryItem label="完成窗口" value="24 小时" />
            <SummaryItem label="结果保留" value="30 天" />
          </div>
        </div>

        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={pending}
            onClick={() => onOpenChange(false)}
          >
            取消
          </Button>
          <Button type="button" disabled={!canSubmit} onClick={() => void submit()}>
            {pending ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <Plus aria-hidden="true" />
            )}
            创建批次
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
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>输入文件</DialogTitle>
          <DialogDescription>
            当前项目中可用于创建批次的 JSONL 文件。
          </DialogDescription>
        </DialogHeader>
        <div className="max-h-[480px] overflow-auto border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>文件</TableHead>
                <TableHead className="w-28">大小</TableHead>
                <TableHead className="w-36">创建时间</TableHead>
                <TableHead className="w-40">
                  <span className="sr-only">操作</span>
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
                    {formatUnixSeconds(file.created_at)}
                  </TableCell>
                  <TableCell>
                    <div className="flex justify-end gap-1">
                      <Button
                        type="button"
                        size="icon"
                        variant="ghost"
                        onClick={() => onCreateBatch(file.id)}
                        aria-label={`使用 ${file.filename} 创建批次`}
                        title="创建批次"
                      >
                        <Plus aria-hidden="true" />
                      </Button>
                      <Button
                        type="button"
                        size="icon"
                        variant="ghost"
                        disabled={downloadingId === file.id}
                        onClick={() => onDownload(file)}
                        aria-label={`下载 ${file.filename}`}
                        title="下载"
                      >
                        <Download aria-hidden="true" />
                      </Button>
                      <Button
                        type="button"
                        size="icon"
                        variant="ghost"
                        onClick={() => onDelete(file)}
                        aria-label={`删除 ${file.filename}`}
                        title="删除"
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
                    <p className="text-sm font-medium">暂无输入文件</p>
                    <p className="mt-1 text-sm text-muted-foreground">
                      在创建批次时上传第一个 JSONL 文件。
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
  return (
    <div aria-label="正在加载批次">
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
  return (
    <div className="space-y-5" aria-label="正在加载批处理">
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

function formatUnixSeconds(value: number) {
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value * 1_000));
}

function errorMessage(reason: unknown, fallback: string) {
  return reason instanceof Error && reason.message ? reason.message : fallback;
}
