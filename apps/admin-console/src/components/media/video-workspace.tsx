"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import {
  Controls,
  FullscreenButton,
  MediaPlayer,
  MediaProvider,
  MuteButton,
  PlayButton,
  Time,
  TimeSlider,
  useMediaState,
} from "@vidstack/react";
import {
  ArrowUp,
  ChevronDown,
  Download,
  History,
  LoaderCircle,
  Maximize,
  Paperclip,
  Pause,
  Play,
  Plus,
  RotateCcw,
  Video,
  Volume2,
  VolumeX,
  X,
} from "lucide-react";
import { toast } from "sonner";
import { ActivityRequestSheet } from "@/components/activity-request-sheet";
import { useConsoleSession } from "@/components/auth/console-session-provider";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Textarea } from "@/components/ui/textarea";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { ProviderBrandIcon } from "@/components/media/provider-brand-icon";
import { consoleFetch } from "@/lib/auth/client";
import type { RequestLogItem, RequestLogsSnapshot } from "@/lib/admin/types";
import { cn } from "@/lib/utils";

type ConsoleVideoModel = {
  id: string;
  provider: string;
  api_profile:
    | "xai-videos-v1"
    | "dreamina-cli-videos-v1"
    | "volcengine-ark-content-generation-v3";
  media_kind: string;
  operation: string;
  created: number;
  controls: {
    aspect_ratio?: {
      default: string;
      options: string[];
    };
    duration: {
      default: number;
      options: number[];
    };
    resolution: {
      default: string;
      options: string[];
    };
    first_frame: {
      supported: boolean;
      required: boolean;
    };
  };
};

type ConsoleVideoModelsResponse = {
  object: "list";
  data: ConsoleVideoModel[];
};

type VideoTask = {
  taskId: string;
  status: "pending" | "uncertain" | "done" | "failed";
  stage?: "queued" | "dispatching" | "processing";
  model?: string;
  duration?: number;
  progress?: number;
  contentUrl?: string;
  error?: {
    code: string;
    message: string;
  };
};

type FirstFrame = {
  name: string;
  dataUrl: string;
};

type VideoHistoryItem = {
  task: VideoTask;
  submittedPrompt: string;
  modelId: string;
  aspectRatio: string;
  duration: number;
  resolution: string;
  firstFrame: FirstFrame | null;
  submittedAt: number;
  completedAt?: number;
};

type PendingIdempotency = {
  requestBody: string;
  key: string;
};

const MAX_FIRST_FRAME_BYTES = 10 * 1024 * 1024;
const HISTORY_TIME_FORMATTER = new Intl.DateTimeFormat("zh-CN", {
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
});

export function VideoWorkspace() {
  const {
    activeWorkspace,
    loading: sessionLoading,
    workspaces,
  } = useConsoleSession();
  const projectWorkspace =
    activeWorkspace?.kind === "project"
      ? activeWorkspace
      : workspaces.find((workspace) => workspace.kind === "project");
  const projectId = projectWorkspace?.id ?? null;
  const [models, setModels] = useState<ConsoleVideoModel[]>([]);
  const [modelId, setModelId] = useState("");
  const [prompt, setPrompt] = useState("");
  const [aspectRatio, setAspectRatio] = useState("16:9");
  const [duration, setDuration] = useState("6");
  const [resolution, setResolution] = useState("480p");
  const [firstFrame, setFirstFrame] = useState<FirstFrame | null>(null);
  const [task, setTask] = useState<VideoTask | null>(null);
  const [submittedPrompt, setSubmittedPrompt] = useState("");
  const [submittedAspectRatio, setSubmittedAspectRatio] = useState("16:9");
  const [generationStartedAt, setGenerationStartedAt] = useState<number | null>(
    null,
  );
  const [generationElapsedSeconds, setGenerationElapsedSeconds] = useState(0);
  const [loadingModels, setLoadingModels] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [polling, setPolling] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [history, setHistory] = useState<VideoHistoryItem[]>([]);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [promptExpanded, setPromptExpanded] = useState(false);
  const [headerActionHost, setHeaderActionHost] = useState<HTMLElement | null>(
    null,
  );
  const requestSequence = useRef(0);
  const taskSequence = useRef(0);
  const taskController = useRef<AbortController | null>(null);
  const generationStartedAtRef = useRef<number | null>(null);
  const fileInput = useRef<HTMLInputElement | null>(null);
  const pendingHistoryRestore = useRef<VideoHistoryItem | null>(null);
  const pendingIdempotency = useRef<PendingIdempotency | null>(null);

  useEffect(() => {
    const sequence = ++requestSequence.current;
    taskSequence.current += 1;
    taskController.current?.abort();
    taskController.current = null;
    setModels([]);
    setModelId("");
    setTask(null);
    setFirstFrame(null);
    setSubmittedPrompt("");
    setSubmittedAspectRatio("16:9");
    setGenerationStartedAt(null);
    generationStartedAtRef.current = null;
    setGenerationElapsedSeconds(0);
    setSubmitting(false);
    setPolling(false);
    setError(null);
    setHistory([]);
    setHistoryOpen(false);
    setPromptExpanded(false);
    pendingHistoryRestore.current = null;
    pendingIdempotency.current = null;
    if (!projectId) return;

    const controller = new AbortController();
    setLoadingModels(true);
    void consoleFetch(
      `/api/gateway/v1/console/projects/${encodeURIComponent(projectId)}/videos/models`,
      { signal: controller.signal },
    )
      .then(async (response) => {
        if (!response.ok) throw new Error(await responseMessage(response));
        return (await response.json()) as ConsoleVideoModelsResponse;
      })
      .then((payload) => {
        if (sequence !== requestSequence.current) return;
        const available = payload.data.filter(
          (model) => model.media_kind === "video",
        );
        setModels(available);
        setModelId(available[0]?.id ?? "");
      })
      .catch((reason: unknown) => {
        if (controller.signal.aborted || sequence !== requestSequence.current)
          return;
        setError(
          reason instanceof Error ? reason.message : "模型目录暂时不可用",
        );
      })
      .finally(() => {
        if (sequence === requestSequence.current) setLoadingModels(false);
      });

    return () => controller.abort();
  }, [projectId]);

  useEffect(
    () => () => {
      taskController.current?.abort();
    },
    [],
  );

  useEffect(() => {
    const heading = document.querySelector("header h1");
    const actionHost = heading?.nextElementSibling;
    setHeaderActionHost(actionHost instanceof HTMLElement ? actionHost : null);
  }, []);

  const selectedModel = useMemo(
    () => models.find((model) => model.id === modelId) ?? null,
    [modelId, models],
  );
  const generationActive = submitting || task?.status === "pending";
  const canSubmit =
    Boolean(projectId && selectedModel && prompt.trim()) &&
    !(selectedModel?.controls.first_frame.required && !firstFrame) &&
    !(firstFrame && !selectedModel?.controls.first_frame.supported) &&
    !generationActive &&
    !submitting &&
    !loadingModels;

  useEffect(() => {
    if (!generationActive || generationStartedAt === null) return;
    const updateElapsed = () =>
      setGenerationElapsedSeconds(
        Math.max(0, Math.floor((Date.now() - generationStartedAt) / 1_000)),
      );
    updateElapsed();
    const timer = window.setInterval(updateElapsed, 1_000);
    return () => window.clearInterval(timer);
  }, [generationActive, generationStartedAt]);

  useEffect(() => {
    if (!selectedModel) return;
    const restored = pendingHistoryRestore.current;
    if (restored?.modelId === selectedModel.id) {
      setAspectRatio(
        choiceValue(selectedModel.controls.aspect_ratio, restored.aspectRatio),
      );
      setDuration(
        String(
          selectedModel.controls.duration.options.includes(restored.duration)
            ? restored.duration
            : selectedModel.controls.duration.default,
        ),
      );
      setResolution(
        choiceValue(selectedModel.controls.resolution, restored.resolution),
      );
      setFirstFrame(
        selectedModel.controls.first_frame.supported
          ? restored.firstFrame
          : null,
      );
      pendingHistoryRestore.current = null;
      return;
    }
    setAspectRatio(selectedModel.controls.aspect_ratio?.default ?? "");
    setDuration(String(selectedModel.controls.duration.default));
    setResolution(selectedModel.controls.resolution.default);
  }, [selectedModel]);

  async function submit() {
    if (!projectId || !selectedModel || !prompt.trim() || submitting) return;
    const sequence = ++taskSequence.current;
    const controller = new AbortController();
    taskController.current?.abort();
    taskController.current = controller;
    const submittedProjectId = projectId;
    const nextPrompt = prompt.trim();
    const startedAt = Date.now();
    setSubmitting(true);
    setPolling(false);
    setTask(null);
    setError(null);
    setPromptExpanded(false);
    setSubmittedPrompt(nextPrompt);
    setSubmittedAspectRatio(aspectRatio || "16:9");
    generationStartedAtRef.current = startedAt;
    setGenerationStartedAt(startedAt);
    setGenerationElapsedSeconds(0);
    try {
      const body = {
        model: selectedModel.id,
        prompt: nextPrompt,
        duration: Number(duration),
        resolution,
        ...(selectedModel.controls.aspect_ratio
          ? { aspect_ratio: aspectRatio }
          : {}),
        ...(firstFrame ? { image: firstFrame.dataUrl } : {}),
      };
      const serializedBody = JSON.stringify(body);
      const idempotency =
        pendingIdempotency.current?.requestBody === serializedBody
          ? pendingIdempotency.current
          : { requestBody: serializedBody, key: crypto.randomUUID() };
      pendingIdempotency.current = idempotency;
      const response = await consoleFetch(
        `/api/gateway/v1/console/projects/${encodeURIComponent(submittedProjectId)}/videos/generations`,
        {
          method: "POST",
          headers: { "idempotency-key": idempotency.key },
          body: serializedBody,
          signal: controller.signal,
        },
      );
      if (sequence !== taskSequence.current) return;
      pendingIdempotency.current = null;
      if (!response.ok) throw new Error(await responseMessage(response));
      const payload = (await response.json()) as unknown;
      const taskId = readTaskId(payload);
      if (!taskId) throw new Error("任务已提交，但服务未返回任务编号");
      const nextTask: VideoTask = {
        taskId,
        status: "pending",
        stage: "queued",
        model: selectedModel.id,
        duration: Number(duration),
      };
      setTask(nextTask);
      setHistory((items) => [
        {
          task: nextTask,
          submittedPrompt: nextPrompt,
          modelId: selectedModel.id,
          aspectRatio,
          duration: Number(duration),
          resolution,
          firstFrame,
          submittedAt: startedAt,
        },
        ...items.filter((item) => item.task.taskId !== taskId),
      ]);
      setSubmitting(false);
      await pollTask(submittedProjectId, taskId, sequence, controller);
    } catch (reason) {
      if (controller.signal.aborted || sequence !== taskSequence.current)
        return;
      setError(reason instanceof Error ? reason.message : "视频生成失败");
    } finally {
      if (sequence === taskSequence.current) {
        taskController.current = null;
        setSubmitting(false);
        setPolling(false);
      }
    }
  }

  async function pollTask(
    submittedProjectId: string,
    taskId: string,
    sequence: number,
    controller: AbortController,
  ) {
    setPolling(true);
    let attempt = 0;
    while (!controller.signal.aborted && sequence === taskSequence.current) {
      await waitUntilVisible(controller.signal);
      const response = await consoleFetch(
        `/api/gateway/v1/console/projects/${encodeURIComponent(submittedProjectId)}/videos/${encodeURIComponent(taskId)}`,
        { signal: controller.signal },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      const nextTask = parseVideoTask(await response.json(), taskId);
      setTask(nextTask);
      syncHistoryTask(nextTask);
      if (
        nextTask.status === "done" ||
        nextTask.status === "failed" ||
        nextTask.status === "uncertain"
      ) {
        if (generationStartedAtRef.current !== null) {
          setGenerationElapsedSeconds(
            Math.max(
              0,
              Math.round((Date.now() - generationStartedAtRef.current) / 1_000),
            ),
          );
        }
        return;
      }
      attempt += 1;
      await delay(
        attempt < 15 ? 2_000 : attempt < 33 ? 5_000 : 10_000,
        controller.signal,
      );
    }
  }

  function syncHistoryTask(nextTask: VideoTask) {
    setHistory((items) =>
      items.map((item) =>
        item.task.taskId === nextTask.taskId
          ? {
              ...item,
              completedAt:
                nextTask.status === "done" ||
                nextTask.status === "failed" ||
                nextTask.status === "uncertain"
                  ? (item.completedAt ?? Date.now())
                  : item.completedAt,
              task: {
                ...nextTask,
                model: nextTask.model ?? item.task.model,
                duration: nextTask.duration ?? item.task.duration,
              },
            }
          : item,
      ),
    );
  }

  function stopPolling() {
    taskSequence.current += 1;
    taskController.current?.abort();
    taskController.current = null;
    setPolling(false);
    setSubmitting(false);
  }

  function startNewVideo() {
    taskSequence.current += 1;
    taskController.current?.abort();
    taskController.current = null;
    setTask(null);
    setPrompt("");
    setFirstFrame(null);
    setSubmittedPrompt("");
    setSubmittedAspectRatio("16:9");
    setGenerationStartedAt(null);
    generationStartedAtRef.current = null;
    setGenerationElapsedSeconds(0);
    setSubmitting(false);
    setPolling(false);
    setError(null);
    setPromptExpanded(false);
    pendingIdempotency.current = null;
  }

  function resumePolling() {
    if (
      !projectId ||
      !task ||
      !["pending", "uncertain"].includes(task.status) ||
      polling
    )
      return;
    const sequence = ++taskSequence.current;
    const controller = new AbortController();
    taskController.current = controller;
    setError(null);
    void pollTask(projectId, task.taskId, sequence, controller)
      .catch((reason: unknown) => {
        if (controller.signal.aborted || sequence !== taskSequence.current)
          return;
        setError(
          reason instanceof Error ? reason.message : "任务状态暂时不可用",
        );
      })
      .finally(() => {
        if (sequence === taskSequence.current) {
          taskController.current = null;
          setPolling(false);
        }
      });
  }

  function restoreHistoryItem(item: VideoHistoryItem) {
    taskSequence.current += 1;
    taskController.current?.abort();
    taskController.current = null;
    setPolling(false);
    setSubmitting(false);
    setTask(item.task);
    setPrompt(item.submittedPrompt);
    setSubmittedPrompt(item.submittedPrompt);
    setSubmittedAspectRatio(item.aspectRatio || "16:9");
    generationStartedAtRef.current = item.submittedAt;
    setGenerationStartedAt(item.submittedAt);
    setGenerationElapsedSeconds(
      Math.max(
        0,
        Math.floor(
          ((item.completedAt ?? Date.now()) - item.submittedAt) / 1_000,
        ),
      ),
    );
    setError(
      item.task.status === "failed" ? (item.task.error?.message ?? null) : null,
    );
    if (selectedModel?.id === item.modelId) {
      setAspectRatio(
        choiceValue(selectedModel.controls.aspect_ratio, item.aspectRatio),
      );
      setDuration(
        String(
          selectedModel.controls.duration.options.includes(item.duration)
            ? item.duration
            : selectedModel.controls.duration.default,
        ),
      );
      setResolution(
        choiceValue(selectedModel.controls.resolution, item.resolution),
      );
      setFirstFrame(
        selectedModel.controls.first_frame.supported ? item.firstFrame : null,
      );
      pendingHistoryRestore.current = null;
    } else {
      pendingHistoryRestore.current = item;
      setModelId(item.modelId);
    }
    setHistoryOpen(false);
  }

  async function selectFirstFrame(file: File | undefined) {
    if (!file) return;
    if (!selectedModel?.controls.first_frame.supported) {
      setError("当前模型不支持首帧图片");
      return;
    }
    if (!["image/png", "image/jpeg", "image/webp"].includes(file.type)) {
      setError("首帧仅支持 PNG、JPEG 或 WebP");
      return;
    }
    if (file.size > MAX_FIRST_FRAME_BYTES) {
      setError("首帧图片不能超过 10 MB");
      return;
    }
    try {
      setFirstFrame({
        name: file.name,
        dataUrl: await validatedImageDataUrl(file),
      });
      setError(null);
    } catch {
      setError("首帧图片无法解码，请重新选择有效的 PNG、JPEG 或 WebP 图片");
    } finally {
      if (fileInput.current) fileInput.current.value = "";
    }
  }

  function handlePromptKeyDown(
    event: React.KeyboardEvent<HTMLTextAreaElement>,
  ) {
    if (
      event.key !== "Enter" ||
      event.shiftKey ||
      event.nativeEvent.isComposing
    )
      return;
    event.preventDefault();
    if (canSubmit) void submit();
  }

  function handlePromptPaste(event: React.ClipboardEvent<HTMLTextAreaElement>) {
    const image = Array.from(event.clipboardData.files).find((file) =>
      file.type.startsWith("image/"),
    );
    if (!image) return;
    event.preventDefault();
    void selectFirstFrame(image);
  }

  const historyButton = (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button
          type="button"
          size="icon"
          variant="ghost"
          onClick={() => setHistoryOpen(true)}
          aria-label="打开当前会话历史"
        >
          <History aria-hidden="true" />
        </Button>
      </TooltipTrigger>
      <TooltipContent>当前会话历史</TooltipContent>
    </Tooltip>
  );

  if (!sessionLoading && !projectId) {
    return (
      <section className="flex min-h-0 flex-1 items-center justify-center bg-muted/20 px-6">
        <div className="max-w-sm text-center">
          <Video
            className="mx-auto mb-4 size-8 text-muted-foreground"
            aria-hidden="true"
          />
          <h2 className="text-lg font-semibold">选择一个项目开始创作</h2>
          <p className="mt-2 text-sm leading-6 text-muted-foreground">
            视频任务、调用记录和用量都归属于项目。请从左上角切换到具体项目。
          </p>
        </div>
      </section>
    );
  }

  return (
    <section className="relative grid min-h-0 flex-1 grid-rows-[minmax(0,1fr)_auto] overflow-hidden bg-muted/20">
      {headerActionHost ? (
        createPortal(historyButton, headerActionHost)
      ) : (
        <div className="absolute right-3 top-3 z-10 rounded-md bg-background/80 shadow-sm backdrop-blur">
          {historyButton}
        </div>
      )}

      <Sheet open={historyOpen} onOpenChange={setHistoryOpen}>
        <SheetContent className="flex w-full min-w-0 flex-col gap-0 overflow-hidden p-0 sm:max-w-md">
          <SheetHeader className="shrink-0 border-b px-5 py-5 pr-12 text-left sm:px-6">
            <SheetTitle>当前会话历史</SheetTitle>
            <SheetDescription>本次页面会话中提交的视频任务</SheetDescription>
          </SheetHeader>
          {history.length > 0 ? (
            <div className="min-h-0 flex-1 overflow-y-auto p-3">
              {history.map((item) => (
                <button
                  key={item.task.taskId}
                  type="button"
                  className={cn(
                    "flex w-full min-w-0 flex-col gap-3 rounded-md p-3 text-left transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                    task?.taskId === item.task.taskId && "bg-muted",
                  )}
                  onClick={() => restoreHistoryItem(item)}
                  aria-pressed={task?.taskId === item.task.taskId}
                >
                  <span className="flex w-full min-w-0 items-start gap-3">
                    <span className="line-clamp-2 min-w-0 flex-1 text-sm font-medium leading-5">
                      {item.submittedPrompt}
                    </span>
                    <VideoStatusBadge
                      status={item.task.status}
                      stage={item.task.stage}
                      progress={item.task.progress}
                    />
                  </span>
                  <span className="flex w-full min-w-0 items-center justify-between gap-3 text-xs text-muted-foreground">
                    <span className="min-w-0 truncate">
                      {item.modelId} · {item.duration} 秒 · {item.resolution}
                    </span>
                    <time
                      className="shrink-0"
                      dateTime={new Date(item.submittedAt).toISOString()}
                    >
                      {formatHistoryTime(item.submittedAt)}
                    </time>
                  </span>
                  <span className="w-full truncate font-mono text-xs text-muted-foreground">
                    {item.task.taskId}
                  </span>
                  {item.task.status === "failed" && item.task.error?.message ? (
                    <span className="line-clamp-2 text-xs leading-5 text-destructive">
                      {item.task.error.message}
                    </span>
                  ) : null}
                </button>
              ))}
            </div>
          ) : (
            <div className="flex min-h-0 flex-1 flex-col items-center justify-center px-6 text-center">
              <History
                className="mb-4 size-8 text-muted-foreground"
                aria-hidden="true"
              />
              <p className="text-sm font-medium">还没有会话记录</p>
            </div>
          )}
        </SheetContent>
      </Sheet>

      <div className="flex min-h-0 overflow-y-auto px-4 py-8 md:px-8">
        {task?.status === "done" && task.contentUrl ? (
          <div className="mx-auto flex w-full max-w-5xl flex-col justify-center">
            <div className="mx-auto w-full max-w-4xl overflow-hidden rounded-lg border bg-black">
              <MediaPlayer
                className="aif-video-player relative"
                title={submittedPrompt || "生成视频"}
                src={{
                  src: `/api/gateway${task.contentUrl}`,
                  type: "video/mp4",
                }}
                viewType="video"
                streamType="on-demand"
                load="visible"
                playsInline
              >
                <MediaProvider />
                <VideoPlayerControls />
              </MediaPlayer>
            </div>
            <div className="mt-4 flex flex-col gap-3">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
                  <Badge variant="secondary">已完成</Badge>
                  {task.duration ? <span>{task.duration} 秒视频</span> : null}
                  {generationElapsedSeconds > 0 ? (
                    <>
                      <span aria-hidden="true">·</span>
                      <span>
                        生成耗时 {formatElapsed(generationElapsedSeconds)}
                      </span>
                    </>
                  ) : null}
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <Button asChild variant="outline" size="sm">
                    <a href={`/api/gateway${task.contentUrl}`} download>
                      <Download aria-hidden="true" />
                      下载
                    </a>
                  </Button>
                  <ActivityButton projectId={projectId} taskId={task.taskId} />
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={!canSubmit}
                    onClick={() => void submit()}
                  >
                    <RotateCcw aria-hidden="true" />
                    再次生成
                  </Button>
                  <Button size="sm" onClick={startNewVideo}>
                    <Plus aria-hidden="true" />
                    新建视频
                  </Button>
                </div>
              </div>
              <div className="max-w-3xl">
                <p
                  className={cn(
                    "whitespace-pre-wrap text-sm leading-6 text-muted-foreground",
                    !promptExpanded && "line-clamp-2",
                  )}
                >
                  {submittedPrompt}
                </p>
                {submittedPrompt.length > 160 ? (
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    className="mt-1 h-7 px-1 text-muted-foreground"
                    onClick={() => setPromptExpanded((expanded) => !expanded)}
                  >
                    <ChevronDown
                      className={cn(
                        "transition-transform",
                        promptExpanded && "rotate-180",
                      )}
                      aria-hidden="true"
                    />
                    {promptExpanded ? "收起提示词" : "展开提示词"}
                  </Button>
                ) : null}
              </div>
            </div>
          </div>
        ) : generationActive ? (
          <div
            data-testid="video-generation-pending"
            className="mx-auto flex h-full min-h-0 w-full max-w-4xl flex-col"
          >
            <div className="mb-3 flex min-w-0 shrink-0 items-center justify-between gap-4">
              <p className="min-w-0 truncate rounded-full bg-muted px-3 py-1.5 text-sm">
                {submittedPrompt}
              </p>
              <p
                data-testid="video-generation-pending-elapsed"
                className="shrink-0 text-xs text-muted-foreground"
                aria-hidden="true"
              >
                {pendingStageLabel(task, submitting)} ·{" "}
                {formatElapsed(generationElapsedSeconds)}
              </p>
            </div>
            <div className="flex min-h-0 flex-1 items-center justify-center">
              <div
                className="video-generation-placeholder relative flex max-h-full max-w-full items-center justify-center overflow-hidden rounded-lg border"
                style={{
                  aspectRatio: cssAspectRatio(submittedAspectRatio),
                  width: constrainedPlaceholderWidth(submittedAspectRatio),
                }}
                aria-hidden="true"
              >
                <div className="relative z-10 flex items-center gap-2 rounded-full bg-foreground/70 px-4 py-2 text-sm font-medium text-background shadow-sm backdrop-blur-sm">
                  <span>
                    {submitting
                      ? "正在创建任务"
                      : task?.status === "uncertain"
                        ? "正在确认状态"
                        : pendingStageLabel(task, false)}
                  </span>
                  <span className="opacity-50">·</span>
                  <span>
                    {normalizeProgress(task?.progress) === null
                      ? pendingStageDetail(task, submitting)
                      : `${normalizeProgress(task?.progress)}%`}
                  </span>
                </div>
              </div>
            </div>
            {task?.taskId ? (
              <div className="mt-3 flex min-w-0 shrink-0 flex-wrap items-center justify-between gap-3">
                <p className="min-w-0 truncate font-mono text-xs text-muted-foreground">
                  {task.taskId}
                </p>
                <div className="flex shrink-0 flex-wrap items-center gap-2">
                  <ActivityButton projectId={projectId} taskId={task.taskId} />
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={polling ? stopPolling : resumePolling}
                  >
                    {polling ? (
                      <Pause aria-hidden="true" />
                    ) : (
                      <Play aria-hidden="true" />
                    )}
                    {polling ? "停止等待" : "继续查看"}
                  </Button>
                </div>
              </div>
            ) : null}
            <span className="sr-only" role="status">
              视频任务已提交，正在后台生成
            </span>
          </div>
        ) : task?.status === "uncertain" ? (
          <div className="m-auto max-w-md text-center">
            <Video
              className="mx-auto mb-5 size-8 text-muted-foreground"
              aria-hidden="true"
            />
            <h2 className="text-xl font-semibold">生成结果待确认</h2>
            <p className="mt-3 text-sm leading-6 text-muted-foreground">
              上游执行已经结束，但系统未能安全确认生成结果。请查看调用记录后再决定是否重新生成。
            </p>
            {generationElapsedSeconds > 0 ? (
              <p className="mt-2 text-xs text-muted-foreground">
                已停止等待 · {formatElapsed(generationElapsedSeconds)}
              </p>
            ) : null}
            <div className="mt-5 flex flex-wrap items-center justify-center gap-2">
              <ActivityButton projectId={projectId} taskId={task.taskId} />
              <Button
                variant="outline"
                size="sm"
                onClick={resumePolling}
                disabled={polling}
              >
                <RotateCcw aria-hidden="true" />
                重新检查
              </Button>
            </div>
          </div>
        ) : task?.status === "failed" ? (
          <div className="m-auto max-w-md text-center">
            <Video
              className="mx-auto mb-5 size-8 text-muted-foreground"
              aria-hidden="true"
            />
            <h2 className="text-xl font-semibold">视频生成失败</h2>
            <p className="mt-3 text-sm leading-6 text-muted-foreground">
              {error ?? task.error?.message ?? "请调整提示词或稍后重试"}
            </p>
            <div className="mt-5 flex flex-wrap items-center justify-center gap-2">
              <ActivityButton projectId={projectId} taskId={task.taskId} />
              <Button
                variant="outline"
                disabled={!canSubmit}
                onClick={() => void submit()}
              >
                <RotateCcw aria-hidden="true" />
                重新生成
              </Button>
            </div>
          </div>
        ) : (
          <div className="m-auto max-w-xl pb-8 text-center">
            <Video
              className="mx-auto mb-5 size-8 text-muted-foreground"
              aria-hidden="true"
            />
            <h2 className="text-2xl font-semibold md:text-3xl">让画面动起来</h2>
            <p className="mt-3 text-sm leading-6 text-muted-foreground">
              描述场景、动作和镜头，任务会在后台持续生成。
            </p>
          </div>
        )}
      </div>

      {generationActive ? (
        <div className="border-t bg-background/95 px-4 py-3 backdrop-blur">
          <p className="mx-auto max-w-3xl text-center text-sm text-muted-foreground">
            当前任务已提交，完成后可继续创作
          </p>
        </div>
      ) : task?.status === "done" ? null : (
        <div className="min-h-0 max-h-[min(42dvh,24rem)] overflow-y-auto border-t bg-background/95 px-3 pb-[max(0.75rem,env(safe-area-inset-bottom))] pt-3 backdrop-blur md:px-6 md:pb-6 md:pt-4">
          <div
            data-testid="video-composer"
            className="mx-auto w-full max-w-3xl rounded-lg border bg-background p-2 shadow-sm"
          >
            {firstFrame ? (
              <div className="mb-1 flex items-center gap-2 rounded-md bg-muted p-2">
                <img
                  src={firstFrame.dataUrl}
                  alt=""
                  className="size-10 rounded object-cover"
                />
                <span className="min-w-0 flex-1 truncate text-sm">
                  {firstFrame.name}
                </span>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button
                      type="button"
                      size="icon"
                      variant="ghost"
                      className="size-8"
                      onClick={() => setFirstFrame(null)}
                      aria-label="移除首帧"
                    >
                      <X aria-hidden="true" />
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>移除首帧</TooltipContent>
                </Tooltip>
              </div>
            ) : null}
            <Textarea
              value={prompt}
              onChange={(event) => setPrompt(event.target.value)}
              onKeyDown={handlePromptKeyDown}
              onPaste={handlePromptPaste}
              placeholder="描述你想生成的视频"
              aria-label="视频提示词"
              maxLength={6_000}
              className="min-h-20 max-h-36 overflow-y-auto border-0 px-2 py-2 text-base shadow-none focus-visible:ring-0"
            />
            {error ? (
              <p role="alert" className="px-2 pb-2 text-sm text-destructive">
                {error}
              </p>
            ) : null}
            {firstFrame &&
            selectedModel &&
            !selectedModel.controls.first_frame.supported ? (
              <p role="alert" className="px-2 pb-2 text-sm text-destructive">
                当前模型不支持首帧，请移除首帧或切换模型
              </p>
            ) : null}
            <div className="flex min-w-0 flex-wrap items-center gap-2">
              <Select
                value={modelId}
                onValueChange={setModelId}
                disabled={loadingModels}
              >
                <SelectTrigger
                  data-testid="video-model-select"
                  className="h-8 min-w-0 flex-[1_1_18rem] border-0 bg-muted px-2 shadow-none [&>span]:!flex [&>span]:line-clamp-none sm:max-w-[26rem]"
                >
                  {loadingModels ? (
                    <span className="flex items-center gap-2 text-sm text-muted-foreground">
                      <LoaderCircle
                        className="size-3.5 animate-spin"
                        aria-hidden="true"
                      />
                      载入模型
                    </span>
                  ) : selectedModel ? (
                    <span className="flex min-w-0 items-center gap-2 whitespace-nowrap">
                      <ProviderBrandIcon provider={selectedModel.provider} />
                      <span>{selectedModel.id}</span>
                    </span>
                  ) : (
                    <SelectValue placeholder="选择模型" />
                  )}
                </SelectTrigger>
                <SelectContent>
                  {models.map((model) => (
                    <SelectItem
                      key={`${model.api_profile}:${model.id}`}
                      value={model.id}
                      disabled={
                        Boolean(firstFrame) &&
                        !model.controls.first_frame.supported
                      }
                    >
                      <span className="flex items-center gap-2">
                        <ProviderBrandIcon provider={model.provider} />
                        <span>{model.id}</span>
                      </span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>

              {selectedModel?.controls.aspect_ratio ? (
                <Select value={aspectRatio} onValueChange={setAspectRatio}>
                  <SelectTrigger className="h-8 w-24 border-0 bg-muted px-2 shadow-none">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {selectedModel.controls.aspect_ratio.options.map(
                      (ratio) => (
                        <SelectItem key={ratio} value={ratio}>
                          {ratio}
                        </SelectItem>
                      ),
                    )}
                  </SelectContent>
                </Select>
              ) : null}

              <Select value={duration} onValueChange={setDuration}>
                <SelectTrigger className="h-8 w-20 border-0 bg-muted px-2 shadow-none">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {(selectedModel?.controls.duration.options ?? []).map(
                    (seconds) => (
                      <SelectItem key={seconds} value={String(seconds)}>
                        {seconds} 秒
                      </SelectItem>
                    ),
                  )}
                </SelectContent>
              </Select>

              <Select value={resolution} onValueChange={setResolution}>
                <SelectTrigger className="h-8 w-20 border-0 bg-muted px-2 shadow-none">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {(selectedModel?.controls.resolution.options ?? []).map(
                    (value) => (
                      <SelectItem key={value} value={value}>
                        {value}
                      </SelectItem>
                    ),
                  )}
                </SelectContent>
              </Select>

              <input
                ref={fileInput}
                type="file"
                accept="image/png,image/jpeg,image/webp"
                className="sr-only"
                onChange={(event) =>
                  void selectFirstFrame(event.target.files?.[0])
                }
              />
              <Tooltip>
                <TooltipTrigger asChild>
                  <span className="inline-flex">
                    <Button
                      type="button"
                      size="icon"
                      variant="ghost"
                      className="relative size-8"
                      disabled={!selectedModel?.controls.first_frame.supported}
                      onClick={() => fileInput.current?.click()}
                      aria-label={
                        selectedModel?.controls.first_frame.required &&
                        !firstFrame
                          ? "添加必需首帧"
                          : "添加首帧"
                      }
                    >
                      <Paperclip aria-hidden="true" />
                      {selectedModel?.controls.first_frame.required &&
                      !firstFrame ? (
                        <span
                          className="absolute right-1 top-1 size-1.5 rounded-full bg-destructive"
                          aria-hidden="true"
                        />
                      ) : null}
                    </Button>
                  </span>
                </TooltipTrigger>
                <TooltipContent>
                  {!selectedModel?.controls.first_frame.supported
                    ? "当前模型的 CLI 尚未支持首帧"
                    : selectedModel.controls.first_frame.required
                      ? "添加首帧（必需，也可直接粘贴）"
                      : "添加首帧（也可直接粘贴）"}
                </TooltipContent>
              </Tooltip>

              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    size="icon"
                    className={cn(
                      "ml-auto size-8",
                      submitting && "pointer-events-none",
                    )}
                    disabled={!canSubmit}
                    onClick={() => void submit()}
                    aria-label="生成视频"
                  >
                    {submitting ? (
                      <LoaderCircle
                        className="animate-spin"
                        aria-hidden="true"
                      />
                    ) : (
                      <ArrowUp aria-hidden="true" />
                    )}
                  </Button>
                </TooltipTrigger>
                <TooltipContent>生成视频</TooltipContent>
              </Tooltip>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}

function VideoPlayerControls() {
  const paused = useMediaState("paused");
  const muted = useMediaState("muted");

  return (
    <Controls.Root className="absolute inset-x-0 bottom-0 z-10 flex flex-col bg-gradient-to-t from-black/90 via-black/45 to-transparent px-3 pb-2 pt-12 text-white">
      <Controls.Group>
        <TimeSlider.Root
          className="group relative flex h-5 w-full cursor-pointer touch-none select-none items-center outline-none"
          aria-label="视频播放进度"
        >
          <TimeSlider.Track className="relative h-1 w-full rounded-full bg-white/35 group-data-[focus]:ring-2 group-data-[focus]:ring-white/70">
            <TimeSlider.Progress className="absolute h-full w-[var(--slider-progress)] rounded-full bg-white/30" />
            <TimeSlider.TrackFill className="absolute h-full w-[var(--slider-fill)] rounded-full bg-white" />
          </TimeSlider.Track>
          <TimeSlider.Thumb className="absolute left-[var(--slider-fill)] top-1/2 size-3 -translate-x-1/2 -translate-y-1/2 rounded-full bg-white opacity-0 shadow-sm transition-opacity group-data-[active]:opacity-100 group-data-[dragging]:opacity-100" />
        </TimeSlider.Root>
      </Controls.Group>
      <Controls.Group className="flex h-9 items-center gap-1.5">
        <PlayButton
          className="inline-flex size-8 items-center justify-center rounded-md hover:bg-white/15 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/70"
          aria-label={paused ? "播放" : "暂停"}
        >
          {paused ? (
            <Play className="size-4 fill-current" aria-hidden="true" />
          ) : (
            <Pause className="size-4 fill-current" aria-hidden="true" />
          )}
        </PlayButton>
        <MuteButton
          className="inline-flex size-8 items-center justify-center rounded-md hover:bg-white/15 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/70"
          aria-label={muted ? "取消静音" : "静音"}
        >
          {muted ? (
            <VolumeX className="size-4" aria-hidden="true" />
          ) : (
            <Volume2 className="size-4" aria-hidden="true" />
          )}
        </MuteButton>
        <div className="flex min-w-[6.5rem] items-center gap-1 font-mono text-xs tabular-nums text-white/90">
          <Time type="current" />
          <span aria-hidden="true">/</span>
          <Time type="duration" />
        </div>
        <FullscreenButton
          className="ml-auto inline-flex size-8 items-center justify-center rounded-md hover:bg-white/15 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/70"
          aria-label="全屏"
        >
          <Maximize className="size-4" aria-hidden="true" />
        </FullscreenButton>
      </Controls.Group>
    </Controls.Root>
  );
}

function VideoStatusBadge({
  status,
  stage,
  progress,
}: {
  status: VideoTask["status"];
  stage?: VideoTask["stage"];
  progress?: number;
}) {
  const normalizedProgress = normalizeProgress(progress);
  const label =
    status === "done"
      ? "已完成"
      : status === "failed"
        ? "失败"
        : status === "uncertain"
          ? "待确认"
          : stage === "queued"
            ? "排队中"
            : stage === "dispatching"
              ? "启动中"
              : normalizedProgress === null
                ? "生成中"
                : `生成中 ${normalizedProgress}%`;
  return (
    <Badge
      variant={
        status === "failed"
          ? "destructive"
          : status === "done"
            ? "secondary"
            : "outline"
      }
    >
      {label}
    </Badge>
  );
}

function formatHistoryTime(timestamp: number) {
  return HISTORY_TIME_FORMATTER.format(timestamp);
}

function formatElapsed(seconds: number) {
  if (seconds < 60) return `${seconds} 秒`;
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return `${minutes} 分 ${remainder} 秒`;
}

function cssAspectRatio(value: string) {
  const [width, height] = value.split(":").map(Number);
  if (
    !Number.isFinite(width) ||
    !Number.isFinite(height) ||
    width <= 0 ||
    height <= 0
  ) {
    return "16 / 9";
  }
  return `${width} / ${height}`;
}

function normalizeProgress(progress: number | undefined) {
  if (progress === undefined || !Number.isFinite(progress)) return null;
  const percentage = progress > 0 && progress <= 1 ? progress * 100 : progress;
  return Math.min(100, Math.max(0, Math.round(percentage)));
}

function choiceValue(
  control: { default: string; options: string[] } | undefined,
  value: string,
) {
  if (!control) return "";
  return control.options.includes(value) ? value : control.default;
}

function readTaskId(payload: unknown) {
  if (!isRecord(payload) || typeof payload.task_id !== "string") return null;
  return payload.task_id;
}

function parseVideoTask(payload: unknown, taskId: string): VideoTask {
  if (!isRecord(payload)) throw new Error("任务状态响应无效");
  if (
    !["pending", "uncertain", "done", "failed"].includes(String(payload.status))
  ) {
    throw new Error("任务状态响应无效");
  }
  const status = payload.status as VideoTask["status"];
  return {
    taskId: typeof payload.task_id === "string" ? payload.task_id : taskId,
    status,
    stage:
      payload.stage === "queued" ||
      payload.stage === "dispatching" ||
      payload.stage === "processing"
        ? payload.stage
        : undefined,
    model: typeof payload.model === "string" ? payload.model : undefined,
    duration:
      typeof payload.duration === "number" ? payload.duration : undefined,
    progress:
      typeof payload.progress === "number" ? payload.progress : undefined,
    contentUrl:
      typeof payload.content_url === "string" ? payload.content_url : undefined,
    error:
      isRecord(payload.error) &&
      typeof payload.error.code === "string" &&
      typeof payload.error.message === "string"
        ? {
            code: payload.error.code,
            message: friendlyVideoTaskError(
              payload.error.code,
              payload.error.message,
            ),
          }
        : undefined,
  };
}

function pendingStageLabel(task: VideoTask | null, submitting: boolean) {
  if (submitting) return "提交中";
  if (task?.status === "uncertain") return "确认中";
  if (task?.stage === "queued") return "排队中";
  if (task?.stage === "dispatching") return "正在启动";
  return "生成中";
}

function pendingStageDetail(task: VideoTask | null, submitting: boolean) {
  if (submitting) return "正在创建任务";
  if (task?.status === "uncertain") return "等待上游确认";
  if (task?.stage === "queued") return "等待可用执行器";
  if (task?.stage === "dispatching") return "正在提交到服务";
  return "后台处理中";
}

async function responseMessage(response: Response) {
  const payload = (await response.json().catch(() => null)) as unknown;
  if (isRecord(payload)) {
    if (typeof payload.error === "string") return payload.error;
    if (isRecord(payload.error) && typeof payload.error.message === "string") {
      return friendlyVideoError(payload.error.message);
    }
  }
  if (response.status === 403) return "当前账号没有在此项目中生成视频的权限";
  if (response.status === 404) return "视频任务不存在或不属于当前项目";
  if (response.status === 429) return "当前项目请求较多，请稍后重试";
  return "视频生成服务暂时不可用";
}

function friendlyVideoError(message: string) {
  if (message === "video pricing is unavailable") {
    return "当前视频模型尚未发布价格，请联系平台管理员配置模型定价";
  }
  return message;
}

function friendlyVideoTaskError(code: string, message: string) {
  if (code === "grok_video_output_upload_url_required") {
    return "当前 Grok 账号启用了零数据保留，CLI 视频生成需要先配置上传目标。请联系平台管理员完成账号的视频输出配置后重试。";
  }
  return friendlyVideoError(message);
}

function ActivityButton({
  projectId,
  taskId,
}: {
  projectId: string | null;
  taskId: string;
}) {
  const [loading, setLoading] = useState(false);
  const [item, setItem] = useState<RequestLogItem | null>(null);

  if (!projectId) return null;

  async function openActivity() {
    if (loading) return;
    setLoading(true);
    try {
      const query = new URLSearchParams({
        source: "videos",
        project_id: projectId!,
        visibility: "project",
        window: "30d",
        limit: "10",
        q: taskId,
      });
      const response = await consoleFetch(
        `/api/gateway/v1/console/logs?${query.toString()}`,
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      const payload = (await response.json()) as RequestLogsSnapshot;
      const selected =
        payload.items.find(
          (candidate) =>
            candidate.job_id === taskId || candidate.request_id === taskId,
        ) ?? payload.items[0];
      if (!selected) {
        throw new Error("调用记录仍在写入，请稍后重试");
      }
      setItem(selected);
    } catch (reason: unknown) {
      toast.error(
        reason instanceof Error ? reason.message : "调用记录暂时不可用",
      );
    } finally {
      setLoading(false);
    }
  }

  const economicsPath =
    item?.job_id && item.project_id
      ? `/v1/console/jobs/${encodeURIComponent(item.job_id)}/economics?${new URLSearchParams({ project_id: item.project_id }).toString()}`
      : null;

  return (
    <>
      <Button
        type="button"
        variant="outline"
        size="sm"
        disabled={loading}
        onClick={() => void openActivity()}
      >
        {loading ? (
          <LoaderCircle className="animate-spin" aria-hidden="true" />
        ) : (
          <History aria-hidden="true" />
        )}
        调用记录
      </Button>
      <ActivityRequestSheet
        item={item}
        economicsPath={economicsPath}
        onOpenChange={(open) => {
          if (!open) setItem(null);
        }}
      />
    </>
  );
}

function constrainedPlaceholderWidth(value: string) {
  const [width, height] = value.split(":").map(Number);
  const ratio =
    Number.isFinite(width) && Number.isFinite(height) && width > 0 && height > 0
      ? width / height
      : 16 / 9;
  return `min(100%, calc(52dvh * ${ratio}))`;
}

function readDataUrl(file: File) {
  return new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () =>
      typeof reader.result === "string"
        ? resolve(reader.result)
        : reject(new Error("invalid file"));
    reader.onerror = () =>
      reject(reader.error ?? new Error("file read failed"));
    reader.readAsDataURL(file);
  });
}

async function validatedImageDataUrl(file: File) {
  const dataUrl = await readDataUrl(file);
  await new Promise<void>((resolve, reject) => {
    const image = new Image();
    image.onload = () => resolve();
    image.onerror = () => reject(new Error("invalid image"));
    image.src = dataUrl;
  });
  return dataUrl;
}

function delay(milliseconds: number, signal: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    const abort = () => {
      window.clearTimeout(timeout);
      reject(new DOMException("Aborted", "AbortError"));
    };
    const timeout = window.setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, milliseconds);
    signal.addEventListener("abort", abort, { once: true });
  });
}

function waitUntilVisible(signal: AbortSignal) {
  if (!document.hidden) return Promise.resolve();
  return new Promise<void>((resolve, reject) => {
    const visible = () => {
      if (document.hidden) return;
      cleanup();
      resolve();
    };
    const abort = () => {
      cleanup();
      reject(new DOMException("Aborted", "AbortError"));
    };
    const cleanup = () => {
      document.removeEventListener("visibilitychange", visible);
      signal.removeEventListener("abort", abort);
    };
    document.addEventListener("visibilitychange", visible);
    signal.addEventListener("abort", abort, { once: true });
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}
