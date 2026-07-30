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
import type { Locale, LocalizedText } from "@/i18n/config";
import { useI18n } from "@/i18n/locale-provider";
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

export function VideoWorkspace() {
  const { locale, t } = useI18n();
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
        if (!response.ok) throw new Error(await responseMessage(response, t));
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
          reason instanceof Error
            ? reason.message
            : t({
              en: "The model catalog is temporarily unavailable.",
              "zh-CN": "模型目录暂时不可用",
              ja: "モデルカタログは一時的に利用できません。",
              ko: "모델 카탈로그를 일시적으로 사용할 수 없습니다.",
            }),
        );
      })
      .finally(() => {
        if (sequence === requestSequence.current) setLoadingModels(false);
      });

    return () => controller.abort();
  }, [projectId, t]);

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
      if (!response.ok) throw new Error(await responseMessage(response, t));
      const payload = (await response.json()) as unknown;
      const taskId = readTaskId(payload);
      if (!taskId) {
        throw new Error(t({
          en: "The task was submitted, but the service did not return a task ID.",
          "zh-CN": "任务已提交，但服务未返回任务编号",
          ja: "タスクは送信されましたが、サービスからタスク ID が返されませんでした。",
          ko: "작업이 제출되었지만 서비스가 작업 ID를 반환하지 않았습니다.",
        }));
      }
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
      setError(
        reason instanceof Error
          ? reason.message
          : t({
            en: "Video generation failed.",
            "zh-CN": "视频生成失败",
            ja: "動画生成に失敗しました。",
            ko: "동영상 생성에 실패했습니다.",
          }),
      );
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
      if (!response.ok) throw new Error(await responseMessage(response, t));
      const nextTask = parseVideoTask(await response.json(), taskId, t);
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
          reason instanceof Error
            ? reason.message
            : t({
              en: "Task status is temporarily unavailable.",
              "zh-CN": "任务状态暂时不可用",
              ja: "タスクの状態は一時的に取得できません。",
              ko: "작업 상태를 일시적으로 확인할 수 없습니다.",
            }),
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
      setError(t({
        en: "This model does not support a first-frame image.",
        "zh-CN": "当前模型不支持首帧图片",
        ja: "このモデルは先頭フレーム画像に対応していません。",
        ko: "이 모델은 첫 프레임 이미지를 지원하지 않습니다.",
      }));
      return;
    }
    if (!["image/png", "image/jpeg", "image/webp"].includes(file.type)) {
      setError(t({
        en: "The first frame must be PNG, JPEG, or WebP.",
        "zh-CN": "首帧仅支持 PNG、JPEG 或 WebP",
        ja: "先頭フレームは PNG、JPEG、WebP のみ使用できます。",
        ko: "첫 프레임은 PNG, JPEG 또는 WebP 형식만 지원합니다.",
      }));
      return;
    }
    if (file.size > MAX_FIRST_FRAME_BYTES) {
      setError(t({
        en: "The first-frame image cannot exceed 10 MB.",
        "zh-CN": "首帧图片不能超过 10 MB",
        ja: "先頭フレーム画像は 10 MB 以下にしてください。",
        ko: "첫 프레임 이미지는 10MB를 초과할 수 없습니다.",
      }));
      return;
    }
    try {
      setFirstFrame({
        name: file.name,
        dataUrl: await validatedImageDataUrl(file),
      });
      setError(null);
    } catch {
      setError(t({
        en: "The first-frame image could not be decoded. Choose a valid PNG, JPEG, or WebP image.",
        "zh-CN": "首帧图片无法解码，请重新选择有效的 PNG、JPEG 或 WebP 图片",
        ja: "先頭フレーム画像をデコードできませんでした。有効な PNG、JPEG、WebP 画像を選択してください。",
        ko: "첫 프레임 이미지를 디코딩할 수 없습니다. 유효한 PNG, JPEG 또는 WebP 이미지를 선택하세요.",
      }));
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
          aria-label={t({
            en: "Open session history",
            "zh-CN": "打开当前会话历史",
            ja: "セッション履歴を開く",
            ko: "세션 기록 열기",
          })}
        >
          <History aria-hidden="true" />
        </Button>
      </TooltipTrigger>
      <TooltipContent>
        {t({
          en: "Session history",
          "zh-CN": "当前会话历史",
          ja: "セッション履歴",
          ko: "세션 기록",
        })}
      </TooltipContent>
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
          <h2 className="text-lg font-semibold">
            {t({
              en: "Select a project to start creating",
              "zh-CN": "选择一个项目开始创作",
              ja: "プロジェクトを選択して作成を開始",
              ko: "프로젝트를 선택해 창작을 시작하세요",
            })}
          </h2>
          <p className="mt-2 text-sm leading-6 text-muted-foreground">
            {t({
              en: "Video tasks, request logs, and usage belong to a project. Choose a project from the top left.",
              "zh-CN": "视频任务、调用记录和用量都归属于项目。请从左上角切换到具体项目。",
              ja: "動画タスク、リクエストログ、使用量はプロジェクトに紐づきます。左上からプロジェクトを選択してください。",
              ko: "동영상 작업, 요청 기록 및 사용량은 프로젝트에 속합니다. 왼쪽 상단에서 프로젝트를 선택하세요.",
            })}
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
            <SheetTitle>
              {t({
                en: "Session history",
                "zh-CN": "当前会话历史",
                ja: "セッション履歴",
                ko: "세션 기록",
              })}
            </SheetTitle>
            <SheetDescription>
              {t({
                en: "Video tasks submitted during this page session",
                "zh-CN": "本次页面会话中提交的视频任务",
                ja: "このページセッションで送信した動画タスク",
                ko: "이 페이지 세션에서 제출한 동영상 작업",
              })}
            </SheetDescription>
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
                      {t(
                        {
                          en: "{model} · {duration}s · {resolution}",
                          "zh-CN": "{model} · {duration} 秒 · {resolution}",
                          ja: "{model} · {duration} 秒 · {resolution}",
                          ko: "{model} · {duration}초 · {resolution}",
                        },
                        {
                          model: item.modelId,
                          duration: item.duration,
                          resolution: item.resolution,
                        },
                      )}
                    </span>
                    <time
                      className="shrink-0"
                      dateTime={new Date(item.submittedAt).toISOString()}
                    >
                      {formatHistoryTime(item.submittedAt, locale)}
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
              <p className="text-sm font-medium">
                {t({
                  en: "No session history yet",
                  "zh-CN": "还没有会话记录",
                  ja: "セッション履歴はまだありません",
                  ko: "아직 세션 기록이 없습니다",
                })}
              </p>
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
                title={submittedPrompt || t({
                  en: "Generated video",
                  "zh-CN": "生成视频",
                  ja: "生成された動画",
                  ko: "생성된 동영상",
                })}
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
                  <Badge variant="secondary">
                    {t({ en: "Completed", "zh-CN": "已完成", ja: "完了", ko: "완료됨" })}
                  </Badge>
                  {task.duration ? (
                    <span>
                      {t(
                        {
                          en: "{duration}s video",
                          "zh-CN": "{duration} 秒视频",
                          ja: "{duration} 秒の動画",
                          ko: "{duration}초 동영상",
                        },
                        { duration: task.duration },
                      )}
                    </span>
                  ) : null}
                  {generationElapsedSeconds > 0 ? (
                    <>
                      <span aria-hidden="true">·</span>
                      <span>
                        {t(
                          {
                            en: "Generated in {duration}",
                            "zh-CN": "生成耗时 {duration}",
                            ja: "生成時間 {duration}",
                            ko: "생성 시간 {duration}",
                          },
                          { duration: formatElapsed(generationElapsedSeconds, t) },
                        )}
                      </span>
                    </>
                  ) : null}
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <Button asChild variant="outline" size="sm">
                    <a href={`/api/gateway${task.contentUrl}`} download>
                      <Download aria-hidden="true" />
                      {t({ en: "Download", "zh-CN": "下载", ja: "ダウンロード", ko: "다운로드" })}
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
                    {t({
                      en: "Generate again",
                      "zh-CN": "再次生成",
                      ja: "もう一度生成",
                      ko: "다시 생성",
                    })}
                  </Button>
                  <Button size="sm" onClick={startNewVideo}>
                    <Plus aria-hidden="true" />
                    {t({
                      en: "New video",
                      "zh-CN": "新建视频",
                      ja: "新しい動画",
                      ko: "새 동영상",
                    })}
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
                    {promptExpanded
                      ? t({
                        en: "Collapse prompt",
                        "zh-CN": "收起提示词",
                        ja: "プロンプトを折りたたむ",
                        ko: "프롬프트 접기",
                      })
                      : t({
                        en: "Expand prompt",
                        "zh-CN": "展开提示词",
                        ja: "プロンプトを展開",
                        ko: "프롬프트 펼치기",
                      })}
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
                {pendingStageLabel(task, submitting, t)} ·{" "}
                {formatElapsed(generationElapsedSeconds, t)}
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
                      ? t({
                        en: "Creating task",
                        "zh-CN": "正在创建任务",
                        ja: "タスクを作成中",
                        ko: "작업 생성 중",
                      })
                      : task?.status === "uncertain"
                        ? t({
                          en: "Confirming status",
                          "zh-CN": "正在确认状态",
                          ja: "状態を確認中",
                          ko: "상태 확인 중",
                        })
                        : pendingStageLabel(task, false, t)}
                  </span>
                  <span className="opacity-50">·</span>
                  <span>
                    {normalizeProgress(task?.progress) === null
                      ? pendingStageDetail(task, submitting, t)
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
                    {polling
                      ? t({
                        en: "Stop waiting",
                        "zh-CN": "停止等待",
                        ja: "待機を停止",
                        ko: "대기 중지",
                      })
                      : t({
                        en: "Keep checking",
                        "zh-CN": "继续查看",
                        ja: "確認を続ける",
                        ko: "계속 확인",
                      })}
                  </Button>
                </div>
              </div>
            ) : null}
            <span className="sr-only" role="status">
              {t({
                en: "Video task submitted and generating in the background.",
                "zh-CN": "视频任务已提交，正在后台生成",
                ja: "動画タスクを送信し、バックグラウンドで生成中です。",
                ko: "동영상 작업을 제출했으며 백그라운드에서 생성 중입니다.",
              })}
            </span>
          </div>
        ) : task?.status === "uncertain" ? (
          <div className="m-auto max-w-md text-center">
            <Video
              className="mx-auto mb-5 size-8 text-muted-foreground"
              aria-hidden="true"
            />
            <h2 className="text-xl font-semibold">
              {t({
                en: "Result awaiting confirmation",
                "zh-CN": "生成结果待确认",
                ja: "結果の確認待ち",
                ko: "결과 확인 대기 중",
              })}
            </h2>
            <p className="mt-3 text-sm leading-6 text-muted-foreground">
              {t({
                en: "The provider finished, but the system could not safely confirm the result. Review the request log before generating again.",
                "zh-CN": "上游执行已经结束，但系统未能安全确认生成结果。请查看调用记录后再决定是否重新生成。",
                ja: "プロバイダーでの実行は完了しましたが、システムは結果を安全に確認できませんでした。再生成する前にリクエストログを確認してください。",
                ko: "공급자 실행은 끝났지만 시스템이 결과를 안전하게 확인하지 못했습니다. 다시 생성하기 전에 요청 기록을 확인하세요.",
              })}
            </p>
            {generationElapsedSeconds > 0 ? (
              <p className="mt-2 text-xs text-muted-foreground">
                {t(
                  {
                    en: "Stopped waiting · {duration}",
                    "zh-CN": "已停止等待 · {duration}",
                    ja: "待機停止 · {duration}",
                    ko: "대기 중지 · {duration}",
                  },
                  { duration: formatElapsed(generationElapsedSeconds, t) },
                )}
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
                {t({
                  en: "Check again",
                  "zh-CN": "重新检查",
                  ja: "再確認",
                  ko: "다시 확인",
                })}
              </Button>
            </div>
          </div>
        ) : task?.status === "failed" ? (
          <div className="m-auto max-w-md text-center">
            <Video
              className="mx-auto mb-5 size-8 text-muted-foreground"
              aria-hidden="true"
            />
            <h2 className="text-xl font-semibold">
              {t({
                en: "Video generation failed",
                "zh-CN": "视频生成失败",
                ja: "動画生成に失敗しました",
                ko: "동영상 생성 실패",
              })}
            </h2>
            <p className="mt-3 text-sm leading-6 text-muted-foreground">
              {error ?? task.error?.message ?? t({
                en: "Adjust the prompt or try again later.",
                "zh-CN": "请调整提示词或稍后重试",
                ja: "プロンプトを調整するか、後でもう一度お試しください。",
                ko: "프롬프트를 조정하거나 나중에 다시 시도하세요.",
              })}
            </p>
            <div className="mt-5 flex flex-wrap items-center justify-center gap-2">
              <ActivityButton projectId={projectId} taskId={task.taskId} />
              <Button
                variant="outline"
                disabled={!canSubmit}
                onClick={() => void submit()}
              >
                <RotateCcw aria-hidden="true" />
                {t({
                  en: "Generate again",
                  "zh-CN": "重新生成",
                  ja: "再生成",
                  ko: "다시 생성",
                })}
              </Button>
            </div>
          </div>
        ) : (
          <div className="m-auto max-w-xl pb-8 text-center">
            <Video
              className="mx-auto mb-5 size-8 text-muted-foreground"
              aria-hidden="true"
            />
            <h2 className="text-2xl font-semibold md:text-3xl">
              {t({
                en: "Bring your scene to life",
                "zh-CN": "让画面动起来",
                ja: "シーンに動きを加えよう",
                ko: "장면에 생동감을 더하세요",
              })}
            </h2>
            <p className="mt-3 text-sm leading-6 text-muted-foreground">
              {t({
                en: "Describe the scene, motion, and camera. Generation continues in the background.",
                "zh-CN": "描述场景、动作和镜头，任务会在后台持续生成。",
                ja: "シーン、動き、カメラワークを説明してください。生成はバックグラウンドで続行されます。",
                ko: "장면, 동작과 카메라를 설명하세요. 생성은 백그라운드에서 계속됩니다.",
              })}
            </p>
          </div>
        )}
      </div>

      {generationActive ? (
        <div className="border-t bg-background/95 px-4 py-3 backdrop-blur">
          <p className="mx-auto max-w-3xl text-center text-sm text-muted-foreground">
            {t({
              en: "The current task has been submitted. You can create again when it finishes.",
              "zh-CN": "当前任务已提交，完成后可继续创作",
              ja: "現在のタスクを送信しました。完了後に次の作成を開始できます。",
              ko: "현재 작업이 제출되었습니다. 완료 후 다시 생성할 수 있습니다.",
            })}
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
                      aria-label={t({
                        en: "Remove first frame",
                        "zh-CN": "移除首帧",
                        ja: "先頭フレームを削除",
                        ko: "첫 프레임 제거",
                      })}
                    >
                      <X aria-hidden="true" />
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>
                    {t({
                      en: "Remove first frame",
                      "zh-CN": "移除首帧",
                      ja: "先頭フレームを削除",
                      ko: "첫 프레임 제거",
                    })}
                  </TooltipContent>
                </Tooltip>
              </div>
            ) : null}
            <Textarea
              value={prompt}
              onChange={(event) => setPrompt(event.target.value)}
              onKeyDown={handlePromptKeyDown}
              onPaste={handlePromptPaste}
              placeholder={t({
                en: "Describe the video you want to create",
                "zh-CN": "描述你想生成的视频",
                ja: "生成したい動画を説明してください",
                ko: "생성할 동영상을 설명하세요",
              })}
              aria-label={t({
                en: "Video prompt",
                "zh-CN": "视频提示词",
                ja: "動画プロンプト",
                ko: "동영상 프롬프트",
              })}
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
                {t({
                  en: "This model does not support a first frame. Remove it or switch models.",
                  "zh-CN": "当前模型不支持首帧，请移除首帧或切换模型",
                  ja: "このモデルは先頭フレームに対応していません。削除するかモデルを切り替えてください。",
                  ko: "이 모델은 첫 프레임을 지원하지 않습니다. 이미지를 제거하거나 모델을 전환하세요.",
                })}
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
                      {t({
                        en: "Loading models",
                        "zh-CN": "载入模型",
                        ja: "モデルを読み込み中",
                        ko: "모델 불러오는 중",
                      })}
                    </span>
                  ) : selectedModel ? (
                    <span className="flex min-w-0 items-center gap-2 whitespace-nowrap">
                      <ProviderBrandIcon provider={selectedModel.provider} />
                      <span>{selectedModel.id}</span>
                    </span>
                  ) : (
                    <SelectValue placeholder={t({
                      en: "Select a model",
                      "zh-CN": "选择模型",
                      ja: "モデルを選択",
                      ko: "모델 선택",
                    })} />
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
                        {t(
                          {
                            en: "{seconds}s",
                            "zh-CN": "{seconds} 秒",
                            ja: "{seconds} 秒",
                            ko: "{seconds}초",
                          },
                          { seconds },
                        )}
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
                          ? t({
                            en: "Add required first frame",
                            "zh-CN": "添加必需首帧",
                            ja: "必須の先頭フレームを追加",
                            ko: "필수 첫 프레임 추가",
                          })
                          : t({
                            en: "Add first frame",
                            "zh-CN": "添加首帧",
                            ja: "先頭フレームを追加",
                            ko: "첫 프레임 추가",
                          })
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
                    ? t({
                      en: "The CLI for this model does not support first frames yet",
                      "zh-CN": "当前模型的 CLI 尚未支持首帧",
                      ja: "このモデルの CLI はまだ先頭フレームに対応していません",
                      ko: "이 모델의 CLI는 아직 첫 프레임을 지원하지 않습니다",
                    })
                    : selectedModel.controls.first_frame.required
                      ? t({
                        en: "Add a first frame (required; you can also paste it)",
                        "zh-CN": "添加首帧（必需，也可直接粘贴）",
                        ja: "先頭フレームを追加（必須、貼り付けも可能）",
                        ko: "첫 프레임 추가(필수, 붙여넣기도 가능)",
                      })
                      : t({
                        en: "Add a first frame (you can also paste it)",
                        "zh-CN": "添加首帧（也可直接粘贴）",
                        ja: "先頭フレームを追加（貼り付けも可能）",
                        ko: "첫 프레임 추가(붙여넣기도 가능)",
                      })}
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
                    aria-label={t({
                      en: "Generate video",
                      "zh-CN": "生成视频",
                      ja: "動画を生成",
                      ko: "동영상 생성",
                    })}
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
                <TooltipContent>
                  {t({
                    en: "Generate video",
                    "zh-CN": "生成视频",
                    ja: "動画を生成",
                    ko: "동영상 생성",
                  })}
                </TooltipContent>
              </Tooltip>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}

function VideoPlayerControls() {
  const { t } = useI18n();
  const paused = useMediaState("paused");
  const muted = useMediaState("muted");

  return (
    <Controls.Root className="absolute inset-x-0 bottom-0 z-10 flex flex-col bg-gradient-to-t from-black/90 via-black/45 to-transparent px-3 pb-2 pt-12 text-white">
      <Controls.Group>
        <TimeSlider.Root
          className="group relative flex h-5 w-full cursor-pointer touch-none select-none items-center outline-none"
          aria-label={t({
            en: "Video playback progress",
            "zh-CN": "视频播放进度",
            ja: "動画の再生位置",
            ko: "동영상 재생 진행률",
          })}
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
          aria-label={paused
            ? t({ en: "Play", "zh-CN": "播放", ja: "再生", ko: "재생" })
            : t({ en: "Pause", "zh-CN": "暂停", ja: "一時停止", ko: "일시 정지" })}
        >
          {paused ? (
            <Play className="size-4 fill-current" aria-hidden="true" />
          ) : (
            <Pause className="size-4 fill-current" aria-hidden="true" />
          )}
        </PlayButton>
        <MuteButton
          className="inline-flex size-8 items-center justify-center rounded-md hover:bg-white/15 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/70"
          aria-label={muted
            ? t({ en: "Unmute", "zh-CN": "取消静音", ja: "ミュート解除", ko: "음소거 해제" })
            : t({ en: "Mute", "zh-CN": "静音", ja: "ミュート", ko: "음소거" })}
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
          aria-label={t({
            en: "Fullscreen",
            "zh-CN": "全屏",
            ja: "全画面",
            ko: "전체 화면",
          })}
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
  const { t } = useI18n();
  const normalizedProgress = normalizeProgress(progress);
  const label =
    status === "done"
      ? t({ en: "Completed", "zh-CN": "已完成", ja: "完了", ko: "완료됨" })
      : status === "failed"
        ? t({ en: "Failed", "zh-CN": "失败", ja: "失敗", ko: "실패" })
        : status === "uncertain"
          ? t({ en: "Needs confirmation", "zh-CN": "待确认", ja: "確認待ち", ko: "확인 필요" })
          : stage === "queued"
            ? t({ en: "Queued", "zh-CN": "排队中", ja: "キュー待ち", ko: "대기열" })
            : stage === "dispatching"
              ? t({ en: "Starting", "zh-CN": "启动中", ja: "開始中", ko: "시작 중" })
              : normalizedProgress === null
                ? t({ en: "Generating", "zh-CN": "生成中", ja: "生成中", ko: "생성 중" })
                : t(
                  {
                    en: "Generating {progress}%",
                    "zh-CN": "生成中 {progress}%",
                    ja: "生成中 {progress}%",
                    ko: "생성 중 {progress}%",
                  },
                  { progress: normalizedProgress },
                );
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

function formatHistoryTime(timestamp: number, locale: Locale) {
  return new Intl.DateTimeFormat(locale, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(timestamp);
}

function formatElapsed(seconds: number, t: Translate) {
  if (seconds < 60) {
    return t(
      {
        en: "{seconds}s",
        "zh-CN": "{seconds} 秒",
        ja: "{seconds} 秒",
        ko: "{seconds}초",
      },
      { seconds },
    );
  }
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return t(
    {
      en: "{minutes}m {seconds}s",
      "zh-CN": "{minutes} 分 {seconds} 秒",
      ja: "{minutes} 分 {seconds} 秒",
      ko: "{minutes}분 {seconds}초",
    },
    { minutes, seconds: remainder },
  );
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

function parseVideoTask(
  payload: unknown,
  taskId: string,
  t: Translate,
): VideoTask {
  const invalidResponse = t({
    en: "The task status response is invalid.",
    "zh-CN": "任务状态响应无效",
    ja: "タスク状態のレスポンスが無効です。",
    ko: "작업 상태 응답이 유효하지 않습니다.",
  });
  if (!isRecord(payload)) throw new Error(invalidResponse);
  if (
    !["pending", "uncertain", "done", "failed"].includes(String(payload.status))
  ) {
    throw new Error(invalidResponse);
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
              t,
            ),
          }
        : undefined,
  };
}

function pendingStageLabel(
  task: VideoTask | null,
  submitting: boolean,
  t: Translate,
) {
  if (submitting) return t({ en: "Submitting", "zh-CN": "提交中", ja: "送信中", ko: "제출 중" });
  if (task?.status === "uncertain") {
    return t({ en: "Confirming", "zh-CN": "确认中", ja: "確認中", ko: "확인 중" });
  }
  if (task?.stage === "queued") {
    return t({ en: "Queued", "zh-CN": "排队中", ja: "キュー待ち", ko: "대기열" });
  }
  if (task?.stage === "dispatching") {
    return t({ en: "Starting", "zh-CN": "正在启动", ja: "開始中", ko: "시작 중" });
  }
  return t({ en: "Generating", "zh-CN": "生成中", ja: "生成中", ko: "생성 중" });
}

function pendingStageDetail(
  task: VideoTask | null,
  submitting: boolean,
  t: Translate,
) {
  if (submitting) {
    return t({ en: "Creating task", "zh-CN": "正在创建任务", ja: "タスクを作成中", ko: "작업 생성 중" });
  }
  if (task?.status === "uncertain") {
    return t({ en: "Awaiting provider confirmation", "zh-CN": "等待上游确认", ja: "プロバイダーの確認待ち", ko: "공급자 확인 대기" });
  }
  if (task?.stage === "queued") {
    return t({ en: "Waiting for an executor", "zh-CN": "等待可用执行器", ja: "利用可能なエグゼキューターを待機中", ko: "사용 가능한 실행기 대기 중" });
  }
  if (task?.stage === "dispatching") {
    return t({ en: "Submitting to the service", "zh-CN": "正在提交到服务", ja: "サービスに送信中", ko: "서비스에 제출 중" });
  }
  return t({ en: "Processing in the background", "zh-CN": "后台处理中", ja: "バックグラウンドで処理中", ko: "백그라운드 처리 중" });
}

async function responseMessage(response: Response, t: Translate) {
  const payload = (await response.json().catch(() => null)) as unknown;
  if (isRecord(payload)) {
    if (typeof payload.error === "string") return payload.error;
    if (isRecord(payload.error) && typeof payload.error.message === "string") {
      return friendlyVideoError(payload.error.message, t);
    }
  }
  if (response.status === 403) {
    return t({
      en: "Your account cannot generate videos in this project.",
      "zh-CN": "当前账号没有在此项目中生成视频的权限",
      ja: "このアカウントには、このプロジェクトで動画を生成する権限がありません。",
      ko: "현재 계정에는 이 프로젝트에서 동영상을 생성할 권한이 없습니다.",
    });
  }
  if (response.status === 404) {
    return t({
      en: "The video task does not exist or does not belong to this project.",
      "zh-CN": "视频任务不存在或不属于当前项目",
      ja: "動画タスクが存在しないか、このプロジェクトに属していません。",
      ko: "동영상 작업이 없거나 현재 프로젝트에 속하지 않습니다.",
    });
  }
  if (response.status === 429) {
    return t({
      en: "This project is receiving too many requests. Try again shortly.",
      "zh-CN": "当前项目请求较多，请稍后重试",
      ja: "このプロジェクトへのリクエストが集中しています。しばらくしてから再試行してください。",
      ko: "현재 프로젝트에 요청이 많습니다. 잠시 후 다시 시도하세요.",
    });
  }
  return t({
    en: "Video generation is temporarily unavailable.",
    "zh-CN": "视频生成服务暂时不可用",
    ja: "動画生成サービスは一時的に利用できません。",
    ko: "동영상 생성 서비스를 일시적으로 사용할 수 없습니다.",
  });
}

function friendlyVideoError(message: string, t: Translate) {
  if (message === "video pricing is unavailable") {
    return t({
      en: "Pricing has not been published for this video model. Ask a platform administrator to configure model pricing.",
      "zh-CN": "当前视频模型尚未发布价格，请联系平台管理员配置模型定价",
      ja: "この動画モデルの価格はまだ公開されていません。プラットフォーム管理者にモデル価格の設定を依頼してください。",
      ko: "현재 동영상 모델의 가격이 아직 게시되지 않았습니다. 플랫폼 관리자에게 모델 가격 구성을 요청하세요.",
    });
  }
  return message;
}

function friendlyVideoTaskError(code: string, message: string, t: Translate) {
  if (code === "grok_video_output_upload_url_required") {
    return t({
      en: "This Grok account has zero data retention enabled. CLI video generation requires an upload destination. Ask a platform administrator to configure video output for the account, then try again.",
      "zh-CN": "当前 Grok 账号启用了零数据保留，CLI 视频生成需要先配置上传目标。请联系平台管理员完成账号的视频输出配置后重试。",
      ja: "この Grok アカウントではゼロデータ保持が有効です。CLI で動画を生成するにはアップロード先が必要です。プラットフォーム管理者に動画出力の設定を依頼してから再試行してください。",
      ko: "현재 Grok 계정에는 데이터 미보존이 활성화되어 있습니다. CLI 동영상 생성을 위해 업로드 대상이 필요합니다. 플랫폼 관리자에게 계정의 동영상 출력 구성을 요청한 뒤 다시 시도하세요.",
    });
  }
  return friendlyVideoError(message, t);
}

function ActivityButton({
  projectId,
  taskId,
}: {
  projectId: string | null;
  taskId: string;
}) {
  const { t } = useI18n();
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
      if (!response.ok) throw new Error(await responseMessage(response, t));
      const payload = (await response.json()) as RequestLogsSnapshot;
      const selected =
        payload.items.find(
          (candidate) =>
            candidate.job_id === taskId || candidate.request_id === taskId,
        ) ?? payload.items[0];
      if (!selected) {
        throw new Error(t({
          en: "The request log is still being written. Try again shortly.",
          "zh-CN": "调用记录仍在写入，请稍后重试",
          ja: "リクエストログはまだ書き込み中です。しばらくしてから再試行してください。",
          ko: "요청 기록이 아직 작성 중입니다. 잠시 후 다시 시도하세요.",
        }));
      }
      setItem(selected);
    } catch (reason: unknown) {
      toast.error(
        reason instanceof Error
          ? reason.message
          : t({
            en: "Request logs are temporarily unavailable.",
            "zh-CN": "调用记录暂时不可用",
            ja: "リクエストログは一時的に利用できません。",
            ko: "요청 기록을 일시적으로 사용할 수 없습니다.",
          }),
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
        {t({
          en: "Request log",
          "zh-CN": "调用记录",
          ja: "リクエストログ",
          ko: "요청 기록",
        })}
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

type Translate = (
  text: LocalizedText,
  values?: Record<string, string | number>,
) => string;

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
