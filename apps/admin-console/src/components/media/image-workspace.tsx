"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import {
  ArrowUp,
  Clock3,
  Download,
  Expand,
  History,
  ImageIcon,
  LoaderCircle,
  Paperclip,
  RotateCcw,
  Sparkles,
  X,
} from "lucide-react";
import { useConsoleSession } from "@/components/auth/console-session-provider";
import {
  ImageControlsMenu,
  type ImageChoiceControl,
} from "@/components/media/image-controls-menu";
import { ImageViewerDialog } from "@/components/media/image-viewer-dialog";
import { ProviderBrandIcon } from "@/components/media/provider-brand-icon";
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
import { consoleFetch } from "@/lib/auth/client";
import { cn } from "@/lib/utils";

type ConsoleImageModel = {
  id: string;
  provider: string;
  api_profile:
    | "openai-images-v1"
    | "xai-images-v1"
    | "dreamina-cli-images-v1"
    | "volcengine-ark-images-v3";
  media_kind: string;
  operation: string;
  created: number;
  supports_edit: boolean;
  max_reference_images: number;
  controls: {
    aspect_ratio: {
      default: string;
      options: string[];
    };
    count: {
      default: number;
      min: number;
      max: number;
    };
    resolution?: {
      default: string;
      options: string[];
    };
    quality?: ImageChoiceControl;
    output_format?: ImageChoiceControl;
    background?: ImageChoiceControl;
  };
};

type ConsoleImageModelsResponse = {
  object: "list";
  data: ConsoleImageModel[];
};

type GeneratedImage = {
  objectUrl: string;
  mimeType: string;
};

type ReferenceImage = {
  id: string;
  file: File;
  previewUrl: string;
};

type ImageHistoryEntry = {
  id: string;
  prompt: string;
  createdAt: number;
  durationMs: number;
  modelId: string;
  aspectRatio: string;
  count: number;
  resolution?: string;
  quality?: string;
  outputFormat?: string;
  background?: string;
  images: GeneratedImage[];
};

type PendingIdempotency = {
  requestBody: string;
  key: string;
};

type PendingImageGeneration = {
  id: string;
  prompt: string;
  modelId: string;
  aspectRatio: string;
  count: number;
  startedAt: number;
};

export function ImageWorkspace() {
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
  const [models, setModels] = useState<ConsoleImageModel[]>([]);
  const [modelId, setModelId] = useState("");
  const [prompt, setPrompt] = useState("");
  const [aspectRatio, setAspectRatio] = useState("1:1");
  const [count, setCount] = useState("1");
  const [resolution, setResolution] = useState("");
  const [quality, setQuality] = useState("");
  const [outputFormat, setOutputFormat] = useState("");
  const [background, setBackground] = useState("");
  const [referenceImages, setReferenceImages] = useState<ReferenceImage[]>([]);
  const [history, setHistory] = useState<ImageHistoryEntry[]>([]);
  const [activeHistoryId, setActiveHistoryId] = useState<string | null>(null);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [viewerIndex, setViewerIndex] = useState<number | null>(null);
  const [headerActionHost, setHeaderActionHost] = useState<HTMLElement | null>(null);
  const [loadingModels, setLoadingModels] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [pendingGeneration, setPendingGeneration] =
    useState<PendingImageGeneration | null>(null);
  const [generationElapsedSeconds, setGenerationElapsedSeconds] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const requestSequence = useRef(0);
  const generationSequence = useRef(0);
  const generationController = useRef<AbortController | null>(null);
  const fileInput = useRef<HTMLInputElement | null>(null);
  const ownedObjectUrls = useRef(new Set<string>());
  const pendingHistoryRestore = useRef<ImageHistoryEntry | null>(null);
  const pendingIdempotency = useRef<PendingIdempotency | null>(null);

  const revokeOwnedObjectUrls = useCallback(() => {
    for (const objectUrl of ownedObjectUrls.current) {
      URL.revokeObjectURL(objectUrl);
    }
    ownedObjectUrls.current.clear();
  }, []);

  useEffect(() => {
    const sequence = ++requestSequence.current;
    generationSequence.current += 1;
    generationController.current?.abort();
    generationController.current = null;
    revokeOwnedObjectUrls();
    setModels([]);
    setModelId("");
    setReferenceImages([]);
    setGenerating(false);
    setPendingGeneration(null);
    setGenerationElapsedSeconds(0);
    setError(null);
    setHistory([]);
    setActiveHistoryId(null);
    setHistoryOpen(false);
    setViewerIndex(null);
    pendingHistoryRestore.current = null;
    pendingIdempotency.current = null;
    if (!projectId) return;

    const controller = new AbortController();
    setLoadingModels(true);
    void consoleFetch(
      `/api/gateway/v1/console/projects/${encodeURIComponent(projectId)}/images/models`,
      { signal: controller.signal },
    )
      .then(async (response) => {
        if (!response.ok) throw new Error(await responseMessage(response));
        return (await response.json()) as ConsoleImageModelsResponse;
      })
      .then((payload) => {
        if (sequence !== requestSequence.current) return;
        const available = payload.data.filter((model) => model.media_kind === "image");
        setModels(available);
        setModelId(available[0]?.id ?? "");
      })
      .catch((reason: unknown) => {
        if (controller.signal.aborted || sequence !== requestSequence.current) return;
        setError(reason instanceof Error ? reason.message : "模型目录暂时不可用");
      })
      .finally(() => {
        if (sequence === requestSequence.current) setLoadingModels(false);
      });

    return () => controller.abort();
  }, [projectId, revokeOwnedObjectUrls]);

  const selectedModel = useMemo(
    () => models.find((model) => model.id === modelId) ?? null,
    [modelId, models],
  );
  const activeHistory = useMemo(
    () =>
      history.find((entry) => entry.id === activeHistoryId) ??
      history.at(-1) ??
      null,
    [activeHistoryId, history],
  );
  const images = activeHistory?.images ?? [];
  const submittedPrompt = activeHistory?.prompt ?? "";
  const resultAspectRatio = activeHistory?.aspectRatio ?? "1:1";
  const maxCount = selectedModel?.controls.count.max ?? 1;
  const minCount = selectedModel?.controls.count.min ?? 1;
  const aspectRatioOptions = selectedModel?.controls.aspect_ratio.options ?? [];
  const canSubmit =
    Boolean(projectId && selectedModel && prompt.trim()) &&
    (referenceImages.length === 0 || Boolean(selectedModel?.supports_edit)) &&
    !generating &&
    !loadingModels;

  useEffect(() => {
    if (!selectedModel) return;
    const restored = pendingHistoryRestore.current;
    if (restored?.modelId === selectedModel.id) {
      setAspectRatio(
        selectedModel.controls.aspect_ratio.options.includes(restored.aspectRatio)
          ? restored.aspectRatio
          : selectedModel.controls.aspect_ratio.default,
      );
      setCount(
        String(
          Math.min(
            selectedModel.controls.count.max,
            Math.max(selectedModel.controls.count.min, restored.count),
          ),
        ),
      );
      setResolution(
        choiceValue(selectedModel.controls.resolution, restored.resolution),
      );
      setQuality(choiceValue(selectedModel.controls.quality, restored.quality));
      setOutputFormat(
        choiceValue(selectedModel.controls.output_format, restored.outputFormat),
      );
      setBackground(
        choiceValue(selectedModel.controls.background, restored.background),
      );
      pendingHistoryRestore.current = null;
      return;
    }
    setAspectRatio(selectedModel.controls.aspect_ratio.default);
    setCount(String(selectedModel.controls.count.default));
    setResolution(selectedModel.controls.resolution?.default ?? "");
    setQuality(selectedModel.controls.quality?.default ?? "");
    setOutputFormat(selectedModel.controls.output_format?.default ?? "");
    setBackground(selectedModel.controls.background?.default ?? "");
  }, [selectedModel]);

  useEffect(() => {
    const heading = document.querySelector("header h1");
    const actionHost = heading?.nextElementSibling;
    setHeaderActionHost(actionHost instanceof HTMLElement ? actionHost : null);
  }, []);

  useEffect(
    () => () => {
      generationController.current?.abort();
      revokeOwnedObjectUrls();
    },
    [revokeOwnedObjectUrls],
  );

  useEffect(() => {
    if (!pendingGeneration) {
      setGenerationElapsedSeconds(0);
      return;
    }
    const updateElapsed = () => {
      setGenerationElapsedSeconds(
        Math.max(0, Math.floor((Date.now() - pendingGeneration.startedAt) / 1_000)),
      );
    };
    updateElapsed();
    const timer = window.setInterval(updateElapsed, 1_000);
    return () => window.clearInterval(timer);
  }, [pendingGeneration]);

  async function submit() {
    if (!projectId || !selectedModel || !prompt.trim() || generating) return;
    const sequence = ++generationSequence.current;
    const controller = new AbortController();
    generationController.current?.abort();
    generationController.current = controller;
    const submittedProjectId = projectId;
    setGenerating(true);
    setError(null);
    const nextPrompt = prompt.trim();
    const submittedReferences = [...referenceImages];
    const startedAt = Date.now();
    setPendingGeneration({
      id: crypto.randomUUID(),
      prompt: nextPrompt,
      modelId: selectedModel.id,
      aspectRatio,
      count: Number(count),
      startedAt,
    });
    try {
      const body = requestBody(selectedModel, nextPrompt, {
        aspectRatio,
        count: Number(count),
        resolution,
        quality,
        outputFormat,
        background,
      });
      const serializedBody = JSON.stringify(body);
      const requestFingerprint =
        submittedReferences.length > 0
          ? await editRequestFingerprint(serializedBody, submittedReferences)
          : serializedBody;
      const idempotency =
        pendingIdempotency.current?.requestBody === requestFingerprint
          ? pendingIdempotency.current
          : { requestBody: requestFingerprint, key: crypto.randomUUID() };
      pendingIdempotency.current = idempotency;
      const editing = submittedReferences.length > 0;
      const response = await consoleFetch(
        `/api/gateway/v1/console/projects/${encodeURIComponent(submittedProjectId)}/images/${
          editing ? "edits" : "generations"
        }`,
        {
          method: "POST",
          headers: { "idempotency-key": idempotency.key },
          body: editing
            ? editRequestBody(body, submittedReferences)
            : serializedBody,
          signal: controller.signal,
        },
      );
      if (sequence !== generationSequence.current) return;
      pendingIdempotency.current = null;
      if (!response.ok) throw new Error(await responseMessage(response));
      const payload = (await response.json()) as unknown;
      const generated = parseGeneratedImages(payload);
      if (generated.length === 0) throw new Error("上游未返回可展示的图片");
      if (sequence !== generationSequence.current) {
        revokeImages(generated);
        return;
      }
      for (const image of generated) {
        ownedObjectUrls.current.add(image.objectUrl);
      }
      const entry: ImageHistoryEntry = {
        id: crypto.randomUUID(),
        prompt: nextPrompt,
        createdAt: Date.now(),
        durationMs: Date.now() - startedAt,
        modelId: selectedModel.id,
        aspectRatio,
        count: Number(count),
        resolution: selectedModel.controls.resolution ? resolution : undefined,
        quality: selectedModel.controls.quality ? quality : undefined,
        outputFormat: selectedModel.controls.output_format
          ? outputFormat
          : undefined,
        background: selectedModel.controls.background ? background : undefined,
        images: generated,
      };
      setHistory((current) => [...current, entry]);
      setActiveHistoryId(entry.id);
    } catch (reason) {
      if (controller.signal.aborted || sequence !== generationSequence.current) return;
      setError(reason instanceof Error ? reason.message : "图片生成失败");
    } finally {
      if (sequence === generationSequence.current) {
        generationController.current = null;
        setGenerating(false);
        setPendingGeneration(null);
      }
    }
  }

  function handlePromptKeyDown(event: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key !== "Enter" || event.shiftKey || event.nativeEvent.isComposing) return;
    event.preventDefault();
    if (canSubmit) void submit();
  }

  async function addReferenceFiles(files: File[]) {
    if (!selectedModel?.supports_edit) {
      setError("当前模型不支持参考图编辑");
      return;
    }
    const supported = files.filter((file) =>
      ["image/png", "image/jpeg", "image/webp"].includes(file.type),
    );
    if (supported.length === 0) {
      setError("参考图仅支持 PNG、JPEG 或 WebP");
      return;
    }
    const currentBytes = referenceImages.reduce(
      (total, image) => total + image.file.size,
      0,
    );
    const maxReferenceImages = selectedModel.max_reference_images;
    const candidates: File[] = [];
    let nextBytes = currentBytes;
    for (const file of supported) {
      if (referenceImages.length + candidates.length >= maxReferenceImages) break;
      if (nextBytes + file.size > 32 * 1024 * 1024) break;
      candidates.push(file);
      nextBytes += file.size;
    }
    if (candidates.length === 0) {
      setError(
        referenceImages.length >= maxReferenceImages
          ? `当前模型最多添加 ${maxReferenceImages} 张参考图`
          : "参考图总大小不能超过 32 MiB",
      );
      return;
    }
    let accepted: ReferenceImage[];
    try {
      accepted = await Promise.all(
        candidates.map(async (file) => {
          const bytes = await file.arrayBuffer();
          const stableFile = new File([bytes], file.name, {
            type: file.type,
            lastModified: file.lastModified,
          });
          const previewUrl = await validatedImageDataUrl(stableFile);
          return {
            id: crypto.randomUUID(),
            file: stableFile,
            previewUrl,
          };
        }),
      );
    } catch {
      setError("参考图无法解码，请重新选择有效的 PNG、JPEG 或 WebP 图片");
      return;
    }
    setReferenceImages((current) => [...current, ...accepted]);
    setError(
      accepted.length < supported.length
        ? "部分图片因数量或总大小限制未添加"
        : null,
    );
  }

  function removeReferenceImage(id: string) {
    setReferenceImages((current) => current.filter((image) => image.id !== id));
  }

  function handlePromptPaste(event: React.ClipboardEvent<HTMLTextAreaElement>) {
    const images = Array.from(event.clipboardData.files).filter((file) =>
      file.type.startsWith("image/"),
    );
    if (images.length === 0) return;
    event.preventDefault();
    void addReferenceFiles(images);
  }

  function restoreHistory(entry: ImageHistoryEntry) {
    setActiveHistoryId(entry.id);
    setPrompt(entry.prompt);
    if (selectedModel?.id === entry.modelId) {
      setAspectRatio(entry.aspectRatio);
      setCount(String(entry.count));
      setResolution(
        choiceValue(selectedModel.controls.resolution, entry.resolution),
      );
      setQuality(choiceValue(selectedModel.controls.quality, entry.quality));
      setOutputFormat(
        choiceValue(selectedModel.controls.output_format, entry.outputFormat),
      );
      setBackground(
        choiceValue(selectedModel.controls.background, entry.background),
      );
      pendingHistoryRestore.current = null;
    } else {
      pendingHistoryRestore.current = entry;
      setModelId(entry.modelId);
    }
    setHistoryOpen(false);
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
  const resultHeightCap =
    images.length === 1 ? 54 : images.length === 2 ? 34 : 28;
  const resultMaxWidth = `${roundDimension(
    resultHeightCap * numericAspectRatio(resultAspectRatio),
  )}dvh`;
  const viewerItems = images.map((image, index) => ({
    src: image.objectUrl,
    alt: `${submittedPrompt}，结果 ${index + 1}`,
  }));

  if (!sessionLoading && !projectId) {
    return (
      <section className="flex min-h-0 flex-1 items-center justify-center bg-muted/20 px-6">
        <div className="max-w-sm text-center">
          <ImageIcon className="mx-auto mb-4 size-8 text-muted-foreground" aria-hidden="true" />
          <h2 className="text-lg font-semibold">选择一个项目开始创作</h2>
          <p className="mt-2 text-sm leading-6 text-muted-foreground">
            图片、调用记录和用量都归属于项目。请从左上角切换到具体项目。
          </p>
        </div>
      </section>
    );
  }

  return (
    <section className="relative grid min-h-0 flex-1 grid-rows-[minmax(0,1fr)_auto] overflow-hidden bg-muted/20">
      {headerActionHost ? createPortal(historyButton, headerActionHost) : (
        <div className="absolute right-3 top-3 z-10 rounded-md bg-background/80 shadow-sm backdrop-blur">
          {historyButton}
        </div>
      )}
      <ImageViewerDialog
        items={viewerItems}
        activeIndex={viewerIndex}
        onActiveIndexChange={setViewerIndex}
        onDownload={(index) => {
          const image = images[index];
          if (image) downloadImage(image, index);
        }}
      />

      <div className="flex min-h-0 overflow-y-auto px-4 py-8 md:px-8">
        {pendingGeneration ? (
          <ImageGenerationPending
            generation={pendingGeneration}
            elapsedSeconds={generationElapsedSeconds}
          />
        ) : images.length > 0 ? (
          <div className="mx-auto flex w-full max-w-5xl flex-col justify-center">
            <div
              className={cn(
                "grid w-full gap-3",
                images.length === 1
                  ? "mx-auto max-w-3xl grid-cols-1"
                  : "grid-cols-1 sm:grid-cols-2",
              )}
            >
              {images.map((image, index) => (
                <figure
                  key={`${activeHistory?.id ?? "result"}-${index}`}
                  className="group relative w-full justify-self-center overflow-hidden rounded-lg border bg-background"
                  style={{ maxWidth: `min(100%, ${resultMaxWidth})` }}
                >
                  {/* The upstream image is intentionally kept only in browser memory. */}
                  <button
                    type="button"
                    className="relative block w-full cursor-zoom-in focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                    onClick={() => setViewerIndex(index)}
                    aria-label={`查看结果 ${index + 1} 原图`}
                  >
                    <img
                      src={image.objectUrl}
                      alt={`${submittedPrompt}，结果 ${index + 1}`}
                      className="h-auto w-full object-contain"
                      style={{ aspectRatio: cssAspectRatio(resultAspectRatio) }}
                    />
                    <span className="absolute inset-0 grid place-items-center bg-black/0 transition-colors group-hover:bg-black/5">
                      <span className="grid size-9 scale-95 place-items-center rounded-md bg-black/65 text-white opacity-0 shadow-sm transition-all group-hover:scale-100 group-hover:opacity-100">
                        <Expand className="size-4" aria-hidden="true" />
                      </span>
                    </span>
                  </button>
                  <div className="absolute right-2 top-2 opacity-100 transition-opacity sm:opacity-0 sm:group-hover:opacity-100">
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <Button
                          size="icon"
                          variant="secondary"
                          onClick={() => downloadImage(image, index)}
                          aria-label={`下载结果 ${index + 1}`}
                        >
                          <Download aria-hidden="true" />
                        </Button>
                      </TooltipTrigger>
                      <TooltipContent>下载图片</TooltipContent>
                    </Tooltip>
                  </div>
                </figure>
              ))}
            </div>
            <div className="mt-4 flex items-start justify-between gap-4">
              <div className="min-w-0 max-w-3xl">
                <p className="text-sm leading-6 text-muted-foreground">
                  {submittedPrompt}
                </p>
                {activeHistory ? (
                  <p className="mt-1 flex items-center gap-1.5 text-xs text-muted-foreground">
                    <Clock3 className="size-3.5" aria-hidden="true" />
                    完成用时 {formatDuration(activeHistory.durationMs)}
                  </p>
                ) : null}
              </div>
              <Button
                variant="outline"
                size="sm"
                disabled={!canSubmit}
                onClick={() => void submit()}
              >
                <RotateCcw aria-hidden="true" />
                再次生成
              </Button>
            </div>
          </div>
        ) : (
          <div className="m-auto max-w-xl pb-8 text-center">
            <Sparkles className="mx-auto mb-5 size-8 text-muted-foreground" aria-hidden="true" />
            <h2 className="text-2xl font-semibold md:text-3xl">今天想创作什么？</h2>
            <p className="mt-3 text-sm leading-6 text-muted-foreground">
              描述画面、主体、构图和风格，生成结果只在当前会话中展示。
            </p>
          </div>
        )}
      </div>

      <Sheet open={historyOpen} onOpenChange={setHistoryOpen}>
        <SheetContent className="flex w-full min-w-0 flex-col gap-0 overflow-hidden p-0 sm:max-w-md">
          <SheetHeader className="border-b px-6 py-5 pr-14 text-left">
            <SheetTitle>当前会话历史</SheetTitle>
            <SheetDescription>
              仅保留本次页面会话中成功生成的图片，切换项目或离开页面后自动清除。
            </SheetDescription>
          </SheetHeader>
          {history.length > 0 ? (
            <ul className="min-h-0 flex-1 overflow-y-auto p-3">
              {[...history].reverse().map((entry) => (
                <li key={entry.id}>
                  <button
                    type="button"
                    className={cn(
                      "w-full min-w-0 rounded-md p-3 text-left transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                      entry.id === activeHistory?.id && "bg-muted",
                    )}
                    onClick={() => restoreHistory(entry)}
                    aria-pressed={entry.id === activeHistory?.id}
                  >
                    <span className="flex min-w-0 items-center justify-between gap-3">
                      <span className="truncate text-sm font-medium">{entry.prompt}</span>
                      <span className="shrink-0 text-xs text-muted-foreground">
                        {formatHistoryTime(entry.createdAt)}
                      </span>
                    </span>
                    <span className="mt-1 block text-xs text-muted-foreground">
                      {entry.modelId} · {entry.aspectRatio} · {entry.images.length} 张
                    </span>
                    <span className="mt-3 flex max-w-full gap-2 overflow-x-auto pb-1">
                      {entry.images.map((image, index) => (
                        <img
                          key={`${entry.id}-${index}`}
                          src={image.objectUrl}
                          alt=""
                          className="size-16 shrink-0 rounded-md border object-cover"
                          aria-hidden="true"
                        />
                      ))}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          ) : (
            <div className="grid min-h-0 flex-1 place-items-center px-8 text-center">
              <div className="max-w-xs">
                <History
                  className="mx-auto mb-4 size-8 text-muted-foreground"
                  aria-hidden="true"
                />
                <p className="text-sm font-medium">还没有会话记录</p>
                <p className="mt-2 text-sm leading-6 text-muted-foreground">
                  成功生成图片后，结果会临时显示在这里。
                </p>
              </div>
            </div>
          )}
        </SheetContent>
      </Sheet>

      <div className="border-t bg-background/95 px-3 pb-[max(0.75rem,env(safe-area-inset-bottom))] pt-3 backdrop-blur md:px-6 md:pb-6 md:pt-4">
        <div
          data-testid="image-composer"
          className="mx-auto w-full max-w-3xl rounded-lg border bg-background p-2 shadow-sm"
        >
          <input
            ref={fileInput}
            type="file"
            accept="image/png,image/jpeg,image/webp"
            multiple
            className="sr-only"
            onChange={(event) => {
              const input = event.currentTarget;
              void addReferenceFiles(Array.from(input.files ?? [])).finally(() => {
                input.value = "";
              });
            }}
          />
          {referenceImages.length > 0 ? (
            <div className="flex gap-2 overflow-x-auto px-2 pb-1 pt-1">
              {referenceImages.map((image, index) => (
                <div
                  key={image.id}
                  className="group relative size-16 shrink-0 overflow-hidden rounded-md border bg-muted"
                >
                  <img
                    src={image.previewUrl}
                    alt={`参考图 ${index + 1}`}
                    className="size-full object-cover"
                  />
                  <Button
                    type="button"
                    size="icon"
                    variant="secondary"
                    className="absolute right-1 top-1 size-6 opacity-100 shadow-sm sm:opacity-0 sm:group-hover:opacity-100"
                    disabled={generating}
                    onClick={() => removeReferenceImage(image.id)}
                    aria-label={`移除参考图 ${index + 1}`}
                  >
                    <X className="size-3.5" aria-hidden="true" />
                  </Button>
                </div>
              ))}
            </div>
          ) : null}
          <Textarea
            value={prompt}
            onChange={(event) => setPrompt(event.target.value)}
            onKeyDown={handlePromptKeyDown}
            onPaste={handlePromptPaste}
            placeholder="描述你想生成的图片"
            aria-label="图片提示词"
            maxLength={6_000}
            className="min-h-20 border-0 px-2 py-2 text-base shadow-none focus-visible:ring-0"
          />
          {error ? (
            <p role="alert" className="px-2 pb-2 text-sm text-destructive">
              {error}
            </p>
          ) : null}
          <div className="flex min-w-0 flex-wrap items-center gap-2">
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  type="button"
                  size="icon"
                  variant="ghost"
                  className="size-8 shrink-0"
                  disabled={!selectedModel?.supports_edit || generating}
                  onClick={() => fileInput.current?.click()}
                  aria-label="添加参考图"
                >
                  <Paperclip aria-hidden="true" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                {selectedModel?.supports_edit
                  ? `添加参考图（最多 ${selectedModel.max_reference_images} 张，也可直接粘贴）`
                  : "当前模型不支持参考图"}
              </TooltipContent>
            </Tooltip>
            <Select value={modelId} onValueChange={setModelId} disabled={loadingModels}>
              <SelectTrigger
                data-testid="image-model-select"
                className="h-8 min-w-0 flex-[1_1_18rem] border-0 bg-muted px-2 shadow-none [&>span]:!flex [&>span]:line-clamp-none sm:max-w-[26rem]"
              >
                {loadingModels ? (
                  <span className="flex items-center gap-2 text-sm text-muted-foreground">
                    <LoaderCircle className="size-3.5 animate-spin" aria-hidden="true" />
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
                    disabled={referenceImages.length > 0 && !model.supports_edit}
                  >
                    <span className="flex items-center gap-2">
                      <ProviderBrandIcon provider={model.provider} />
                      <span>{model.id}</span>
                    </span>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={aspectRatio} onValueChange={setAspectRatio}>
              <SelectTrigger className="h-8 w-24 border-0 bg-muted px-2 shadow-none">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {aspectRatioOptions.map((ratio) => (
                  <SelectItem key={ratio} value={ratio}>
                    {aspectRatioLabel(ratio)} {ratio}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            {maxCount > 1 ? (
              <Select value={count} onValueChange={setCount}>
                <SelectTrigger className="h-8 w-20 border-0 bg-muted px-2 shadow-none">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {Array.from(
                    { length: maxCount - minCount + 1 },
                    (_, index) => minCount + index,
                  ).map((value) => (
                    <SelectItem key={value} value={String(value)}>
                      {value} 张
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            ) : null}

            {selectedModel?.controls.resolution &&
            selectedModel.controls.resolution.options.length > 1 ? (
              <Select value={resolution} onValueChange={setResolution}>
                <SelectTrigger className="h-8 w-20 border-0 bg-muted px-2 shadow-none">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {selectedModel.controls.resolution.options.map((value) => (
                    <SelectItem key={value} value={value}>
                      {value.toUpperCase()}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            ) : null}

            <ImageControlsMenu
              quality={quality}
              outputFormat={outputFormat}
              background={background}
              qualityControl={selectedModel?.controls.quality}
              outputFormatControl={selectedModel?.controls.output_format}
              backgroundControl={selectedModel?.controls.background}
              onQualityChange={setQuality}
              onOutputFormatChange={setOutputFormat}
              onBackgroundChange={setBackground}
            />

            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  size="icon"
                  className="ml-auto size-8"
                  disabled={!canSubmit}
                  onClick={() => void submit()}
                  aria-label={generating ? "图片生成中" : "生成图片"}
                >
                  <ArrowUp aria-hidden="true" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                {generating ? "图片生成中" : "生成图片"}
              </TooltipContent>
            </Tooltip>
          </div>
        </div>
      </div>
    </section>
  );
}

function ImageGenerationPending({
  generation,
  elapsedSeconds,
}: {
  generation: PendingImageGeneration;
  elapsedSeconds: number;
}) {
  const placeholderCount = Math.min(4, Math.max(1, generation.count));
  const aspectRatioValue = numericAspectRatio(generation.aspectRatio);
  const placeholderHeightCap =
    placeholderCount === 1 ? 52 : placeholderCount === 2 ? 26 : 22;
  const placeholderMaxWidth = `${roundDimension(
    placeholderHeightCap * aspectRatioValue,
  )}dvh`;

  return (
    <div
      key={generation.id}
      data-testid="image-generation-pending"
      className="m-auto flex min-h-0 w-full max-w-5xl flex-col"
    >
      <div className="mb-4 flex min-w-0 items-center justify-between gap-4">
        <p className="min-w-0 truncate rounded-full bg-muted px-3 py-1.5 text-sm">
          {generation.prompt}
        </p>
        <p
          data-testid="image-generation-pending-elapsed"
          className="shrink-0 text-xs text-muted-foreground"
          aria-hidden="true"
        >
          生成中 · {formatElapsed(elapsedSeconds)}
        </p>
      </div>
      <div
        className={cn(
          "grid w-full gap-3",
          placeholderCount === 1
            ? "mx-auto max-w-3xl grid-cols-1"
            : "grid-cols-1 sm:grid-cols-2",
        )}
      >
        {Array.from({ length: placeholderCount }, (_, index) => (
          <div
            key={index}
            className="generation-placeholder relative w-full justify-self-center overflow-hidden rounded-lg border bg-muted"
            style={{
              aspectRatio: cssAspectRatio(generation.aspectRatio),
              maxWidth: `min(100%, ${placeholderMaxWidth})`,
            }}
            aria-hidden="true"
          />
        ))}
      </div>
      <span className="sr-only" role="status">
        图片生成任务已提交，模型 {generation.modelId}，正在生成
      </span>
    </div>
  );
}

function requestBody(
  model: ConsoleImageModel,
  prompt: string,
  settings: {
    aspectRatio: string;
    count: number;
    resolution: string;
    quality: string;
    outputFormat: string;
    background: string;
  },
) {
  return {
    model: model.id,
    prompt,
    count: settings.count,
    aspect_ratio: settings.aspectRatio,
    ...(model.controls.resolution
      ? { resolution: settings.resolution }
      : {}),
    ...(model.controls.quality ? { quality: settings.quality } : {}),
    ...(model.controls.output_format
      ? { output_format: settings.outputFormat }
      : {}),
    ...(model.controls.background ? { background: settings.background } : {}),
  };
}

function formatElapsed(seconds: number) {
  if (seconds < 60) return `${seconds} 秒`;
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return `${minutes} 分 ${remainder} 秒`;
}

function formatDuration(milliseconds: number) {
  if (milliseconds < 1_000) return "不足 1 秒";
  return formatElapsed(Math.round(milliseconds / 1_000));
}

function editRequestBody(
  body: ReturnType<typeof requestBody>,
  references: ReferenceImage[],
) {
  const form = new FormData();
  form.set("model", body.model);
  form.set("prompt", body.prompt);
  form.set("n", String(body.count));
  form.set("size", body.aspect_ratio);
  if (body.quality) form.set("quality", body.quality);
  if (body.output_format) form.set("output_format", body.output_format);
  if (body.background) form.set("background", body.background);
  form.set("response_format", "b64_json");
  for (const reference of references) {
    form.append("image[]", reference.file, reference.file.name);
  }
  return form;
}

async function editRequestFingerprint(
  serializedBody: string,
  references: ReferenceImage[],
) {
  const images = await Promise.all(
    references.map(async ({ file }) => ({
      type: file.type,
      size: file.size,
      sha256: hexDigest(await crypto.subtle.digest("SHA-256", await file.arrayBuffer())),
    })),
  );
  return JSON.stringify({ request: serializedBody, images });
}

function hexDigest(bytes: ArrayBuffer) {
  return Array.from(new Uint8Array(bytes), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}

async function validatedImageDataUrl(file: File) {
  const dataUrl = await new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.addEventListener("load", () => {
      if (typeof reader.result === "string") resolve(reader.result);
      else reject(new Error("image preview is not a data URL"));
    });
    reader.addEventListener("error", () => reject(reader.error));
    reader.readAsDataURL(file);
  });
  await new Promise<void>((resolve, reject) => {
    const image = new Image();
    image.addEventListener("load", () => resolve());
    image.addEventListener("error", () => reject(new Error("image preview cannot be decoded")));
    image.src = dataUrl;
  });
  return dataUrl;
}

function choiceValue(control: ImageChoiceControl | undefined, value: string | undefined) {
  if (!control) return "";
  return value && control.options.includes(value) ? value : control.default;
}

function parseGeneratedImages(payload: unknown): GeneratedImage[] {
  if (!isRecord(payload) || !Array.isArray(payload.data)) return [];
  const defaultMime =
    typeof payload.output_format === "string"
      ? mimeTypeForFormat(payload.output_format)
      : "image/png";
  const generated: GeneratedImage[] = [];
  try {
    for (const item of payload.data) {
      if (!isRecord(item) || typeof item.b64_json !== "string") continue;
      const mimeType = typeof item.mime_type === "string" ? item.mime_type : defaultMime;
      generated.push({
        objectUrl: objectUrlFromBase64(item.b64_json, mimeType),
        mimeType,
      });
    }
    return generated;
  } catch (error) {
    revokeImages(generated);
    throw error;
  }
}

function downloadImage(image: GeneratedImage, index: number) {
  const anchor = document.createElement("a");
  anchor.href = image.objectUrl;
  anchor.download = `ai-image-${index + 1}.${extensionForMimeType(image.mimeType)}`;
  anchor.click();
}

async function responseMessage(response: Response) {
  const payload = (await response.json().catch(() => null)) as unknown;
  if (isRecord(payload)) {
    if (typeof payload.error === "string") return payload.error;
    if (isRecord(payload.error)) {
      if (payload.error.code === "billing_limit_exceeded") {
        return "组织计费可用额度不足，请联系管理员调整组织限额";
      }
      if (typeof payload.error.message === "string") {
        return payload.error.message;
      }
    }
  }
  if (response.status === 403) return "当前账号没有在此项目中生成图片的权限";
  if (response.status === 429) return "当前项目请求较多，请稍后重试";
  return "图片生成服务暂时不可用";
}

function aspectRatioLabel(value: string) {
  if (value === "1:1") return "方形";
  if (value === "3:4") return "竖版";
  if (value === "4:3") return "横版";
  if (value === "9:16") return "故事";
  if (value === "16:9") return "宽屏";
  return "比例";
}

function cssAspectRatio(value: string) {
  return value.replace(":", " / ");
}

function numericAspectRatio(value: string) {
  const [width, height] = value.split(":").map(Number);
  if (!width || !height || !Number.isFinite(width) || !Number.isFinite(height)) {
    return 1;
  }
  return width / height;
}

function roundDimension(value: number) {
  return Math.round(value * 100) / 100;
}

function formatHistoryTime(value: number) {
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}

function objectUrlFromBase64(encoded: string, mimeType: string) {
  const binary = window.atob(encoded);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return URL.createObjectURL(new Blob([bytes], { type: mimeType }));
}

function revokeImages(images: GeneratedImage[]) {
  for (const image of images) URL.revokeObjectURL(image.objectUrl);
}

function mimeTypeForFormat(format: string) {
  return format === "jpg" || format === "jpeg" ? "image/jpeg" : `image/${format}`;
}

function extensionForMimeType(mimeType: string) {
  if (mimeType === "image/jpeg") return "jpg";
  if (mimeType === "image/webp") return "webp";
  return "png";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}
