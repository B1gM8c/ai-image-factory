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
import type { Locale, LocalizedText } from "@/i18n/config";
import { useI18n } from "@/i18n/locale-provider";
import {
  consoleFetch,
  consoleRequestFailure,
  consoleResponseFailure,
} from "@/lib/auth/client";
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

    const catalogFailure = t({
      en: "The model catalog is temporarily unavailable.",
      "zh-CN": "模型目录暂时不可用。",
      ja: "モデルカタログは一時的に利用できません。",
      ko: "모델 카탈로그를 일시적으로 사용할 수 없습니다.",
    });
    const controller = new AbortController();
    setLoadingModels(true);
    void consoleFetch(
      `/api/gateway/v1/console/projects/${encodeURIComponent(projectId)}/images/models`,
      { signal: controller.signal },
    )
      .then(async (response) => {
        if (!response.ok) {
          throw new Error(
            await consoleResponseFailure(response, catalogFailure, t),
          );
        }
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
        setError(consoleRequestFailure(reason, catalogFailure, t));
      })
      .finally(() => {
        if (sequence === requestSequence.current) setLoadingModels(false);
      });

    return () => controller.abort();
  }, [projectId, revokeOwnedObjectUrls, t]);

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
    const generationFailure = t({
      en: "Image generation failed.",
      "zh-CN": "图片生成失败。",
      ja: "画像生成に失敗しました。",
      ko: "이미지 생성에 실패했습니다.",
    });
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
      if (!response.ok) {
        throw new Error(
          await consoleResponseFailure(response, generationFailure, t),
        );
      }
      const payload = (await response.json()) as unknown;
      const generated = parseGeneratedImages(payload);
      if (generated.length === 0) {
        throw new Error(
          t(
            {
              en: "{primary} The provider did not return a displayable image.",
              "zh-CN": "{primary} 上游未返回可展示的图片。",
              ja: "{primary} プロバイダーから表示可能な画像が返されませんでした。",
              ko: "{primary} 공급자가 표시 가능한 이미지를 반환하지 않았습니다.",
            },
            { primary: generationFailure },
          ),
        );
      }
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
      setError(consoleRequestFailure(reason, generationFailure, t));
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
      setError(t({
        en: "This model does not support reference-image editing.",
        "zh-CN": "当前模型不支持参考图编辑",
        ja: "このモデルは参照画像を使った編集に対応していません。",
        ko: "이 모델은 참조 이미지 편집을 지원하지 않습니다.",
      }));
      return;
    }
    const supported = files.filter((file) =>
      ["image/png", "image/jpeg", "image/webp"].includes(file.type),
    );
    if (supported.length === 0) {
      setError(t({
        en: "Reference images must be PNG, JPEG, or WebP.",
        "zh-CN": "参考图仅支持 PNG、JPEG 或 WebP",
        ja: "参照画像は PNG、JPEG、WebP のみ使用できます。",
        ko: "참조 이미지는 PNG, JPEG 또는 WebP 형식만 지원합니다.",
      }));
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
          ? t(
            {
              en: "This model accepts up to {count} reference images.",
              "zh-CN": "当前模型最多添加 {count} 张参考图",
              ja: "このモデルでは参照画像を最大 {count} 枚追加できます。",
              ko: "이 모델에는 참조 이미지를 최대 {count}개까지 추가할 수 있습니다.",
            },
            { count: maxReferenceImages },
          )
          : t({
            en: "Reference images cannot exceed 32 MiB in total.",
            "zh-CN": "参考图总大小不能超过 32 MiB",
            ja: "参照画像の合計サイズは 32 MiB 以下にしてください。",
            ko: "참조 이미지의 총 크기는 32 MiB를 초과할 수 없습니다.",
          }),
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
      setError(t({
        en: "The reference image could not be decoded. Choose a valid PNG, JPEG, or WebP image.",
        "zh-CN": "参考图无法解码，请重新选择有效的 PNG、JPEG 或 WebP 图片",
        ja: "参照画像をデコードできませんでした。有効な PNG、JPEG、WebP 画像を選択してください。",
        ko: "참조 이미지를 디코딩할 수 없습니다. 유효한 PNG, JPEG 또는 WebP 이미지를 선택하세요.",
      }));
      return;
    }
    setReferenceImages((current) => [...current, ...accepted]);
    setError(
      accepted.length < supported.length
        ? t({
          en: "Some images were not added because of the count or total-size limit.",
          "zh-CN": "部分图片因数量或总大小限制未添加",
          ja: "枚数または合計サイズの上限により、一部の画像は追加されませんでした。",
          ko: "개수 또는 총 크기 제한으로 일부 이미지가 추가되지 않았습니다.",
        })
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
  const resultHeightCap =
    images.length === 1 ? 54 : images.length === 2 ? 34 : 28;
  const resultMaxWidth = `${roundDimension(
    resultHeightCap * numericAspectRatio(resultAspectRatio),
  )}dvh`;
  const viewerItems = images.map((image, index) => ({
    src: image.objectUrl,
    alt: t(
      {
        en: "{prompt}, result {index}",
        "zh-CN": "{prompt}，结果 {index}",
        ja: "{prompt}、結果 {index}",
        ko: "{prompt}, 결과 {index}",
      },
      { prompt: submittedPrompt, index: index + 1 },
    ),
  }));

  if (!sessionLoading && !projectId) {
    return (
      <section className="flex min-h-0 flex-1 items-center justify-center bg-muted/20 px-6">
        <div className="max-w-sm text-center">
          <ImageIcon className="mx-auto mb-4 size-8 text-muted-foreground" aria-hidden="true" />
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
              en: "Images, request logs, and usage belong to a project. Choose a project from the top left.",
              "zh-CN": "图片、调用记录和用量都归属于项目。请从左上角切换到具体项目。",
              ja: "画像、リクエストログ、使用量はプロジェクトに紐づきます。左上からプロジェクトを選択してください。",
              ko: "이미지, 요청 기록 및 사용량은 프로젝트에 속합니다. 왼쪽 상단에서 프로젝트를 선택하세요.",
            })}
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
                    aria-label={t(
                      {
                        en: "View original for result {index}",
                        "zh-CN": "查看结果 {index} 原图",
                        ja: "結果 {index} の元画像を表示",
                        ko: "결과 {index}의 원본 이미지 보기",
                      },
                      { index: index + 1 },
                    )}
                  >
                    <img
                      src={image.objectUrl}
                      alt={t(
                        {
                          en: "{prompt}, result {index}",
                          "zh-CN": "{prompt}，结果 {index}",
                          ja: "{prompt}、結果 {index}",
                          ko: "{prompt}, 결과 {index}",
                        },
                        { prompt: submittedPrompt, index: index + 1 },
                      )}
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
                          aria-label={t(
                            {
                              en: "Download result {index}",
                              "zh-CN": "下载结果 {index}",
                              ja: "結果 {index} をダウンロード",
                              ko: "결과 {index} 다운로드",
                            },
                            { index: index + 1 },
                          )}
                        >
                          <Download aria-hidden="true" />
                        </Button>
                      </TooltipTrigger>
                      <TooltipContent>
                        {t({
                          en: "Download image",
                          "zh-CN": "下载图片",
                          ja: "画像をダウンロード",
                          ko: "이미지 다운로드",
                        })}
                      </TooltipContent>
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
                    {t(
                      {
                        en: "Completed in {duration}",
                        "zh-CN": "完成用时 {duration}",
                        ja: "{duration} で完了",
                        ko: "{duration} 만에 완료",
                      },
                      { duration: formatDuration(activeHistory.durationMs, t) },
                    )}
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
                {t({
                  en: "Generate again",
                  "zh-CN": "再次生成",
                  ja: "もう一度生成",
                  ko: "다시 생성",
                })}
              </Button>
            </div>
          </div>
        ) : (
          <div className="m-auto max-w-xl pb-8 text-center">
            <Sparkles className="mx-auto mb-5 size-8 text-muted-foreground" aria-hidden="true" />
            <h2 className="text-2xl font-semibold md:text-3xl">
              {t({
                en: "What will you create today?",
                "zh-CN": "今天想创作什么？",
                ja: "今日は何を作りますか？",
                ko: "오늘은 무엇을 만들어 볼까요?",
              })}
            </h2>
            <p className="mt-3 text-sm leading-6 text-muted-foreground">
              {t({
                en: "Describe the scene, subject, composition, and style. Results stay in this session only.",
                "zh-CN": "描述画面、主体、构图和风格，生成结果只在当前会话中展示。",
                ja: "シーン、被写体、構図、スタイルを説明してください。結果は現在のセッションにのみ表示されます。",
                ko: "장면, 피사체, 구도와 스타일을 설명하세요. 결과는 현재 세션에만 표시됩니다.",
              })}
            </p>
          </div>
        )}
      </div>

      <Sheet open={historyOpen} onOpenChange={setHistoryOpen}>
        <SheetContent className="flex w-full min-w-0 flex-col gap-0 overflow-hidden p-0 sm:max-w-md">
          <SheetHeader className="border-b px-6 py-5 pr-14 text-left">
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
                en: "Only images generated successfully in this page session are kept. They are cleared when you switch projects or leave the page.",
                "zh-CN": "仅保留本次页面会话中成功生成的图片，切换项目或离开页面后自动清除。",
                ja: "このページセッションで正常に生成された画像のみ保持します。プロジェクトを切り替えるかページを離れると自動的に消去されます。",
                ko: "이 페이지 세션에서 성공적으로 생성된 이미지만 보관합니다. 프로젝트를 전환하거나 페이지를 나가면 자동으로 삭제됩니다.",
              })}
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
                        {formatHistoryTime(entry.createdAt, locale)}
                      </span>
                    </span>
                    <span className="mt-1 block text-xs text-muted-foreground">
                      {t(
                        {
                          en: "{model} · {ratio} · {count} images",
                          "zh-CN": "{model} · {ratio} · {count} 张",
                          ja: "{model} · {ratio} · {count} 枚",
                          ko: "{model} · {ratio} · 이미지 {count}개",
                        },
                        {
                          model: entry.modelId,
                          ratio: entry.aspectRatio,
                          count: entry.images.length.toLocaleString(locale),
                        },
                      )}
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
                <p className="text-sm font-medium">
                  {t({
                    en: "No session history yet",
                    "zh-CN": "还没有会话记录",
                    ja: "セッション履歴はまだありません",
                    ko: "아직 세션 기록이 없습니다",
                  })}
                </p>
                <p className="mt-2 text-sm leading-6 text-muted-foreground">
                  {t({
                    en: "Successfully generated images will appear here temporarily.",
                    "zh-CN": "成功生成图片后，结果会临时显示在这里。",
                    ja: "正常に生成された画像は一時的にここに表示されます。",
                    ko: "성공적으로 생성된 이미지가 여기에 임시로 표시됩니다.",
                  })}
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
                    alt={t(
                      {
                        en: "Reference image {index}",
                        "zh-CN": "参考图 {index}",
                        ja: "参照画像 {index}",
                        ko: "참조 이미지 {index}",
                      },
                      { index: index + 1 },
                    )}
                    className="size-full object-cover"
                  />
                  <Button
                    type="button"
                    size="icon"
                    variant="secondary"
                    className="absolute right-1 top-1 size-6 opacity-100 shadow-sm sm:opacity-0 sm:group-hover:opacity-100"
                    disabled={generating}
                    onClick={() => removeReferenceImage(image.id)}
                    aria-label={t(
                      {
                        en: "Remove reference image {index}",
                        "zh-CN": "移除参考图 {index}",
                        ja: "参照画像 {index} を削除",
                        ko: "참조 이미지 {index} 제거",
                      },
                      { index: index + 1 },
                    )}
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
            placeholder={t({
              en: "Describe the image you want to create",
              "zh-CN": "描述你想生成的图片",
              ja: "生成したい画像を説明してください",
              ko: "생성할 이미지를 설명하세요",
            })}
            aria-label={t({
              en: "Image prompt",
              "zh-CN": "图片提示词",
              ja: "画像プロンプト",
              ko: "이미지 프롬프트",
            })}
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
                  aria-label={t({
                    en: "Add reference images",
                    "zh-CN": "添加参考图",
                    ja: "参照画像を追加",
                    ko: "참조 이미지 추가",
                  })}
                >
                  <Paperclip aria-hidden="true" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                {selectedModel?.supports_edit
                  ? t(
                    {
                      en: "Add reference images (up to {count}; you can also paste them)",
                      "zh-CN": "添加参考图（最多 {count} 张，也可直接粘贴）",
                      ja: "参照画像を追加（最大 {count} 枚、貼り付けも可能）",
                      ko: "참조 이미지 추가(최대 {count}개, 붙여넣기도 가능)",
                    },
                    { count: selectedModel.max_reference_images },
                  )
                  : t({
                    en: "This model does not support reference images",
                    "zh-CN": "当前模型不支持参考图",
                    ja: "このモデルは参照画像に対応していません",
                    ko: "이 모델은 참조 이미지를 지원하지 않습니다",
                  })}
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
                    {aspectRatioLabel(ratio, t)} {ratio}
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
                      {t(
                        {
                          en: "{count} images",
                          "zh-CN": "{count} 张",
                          ja: "{count} 枚",
                          ko: "이미지 {count}개",
                        },
                        { count: value },
                      )}
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
                  aria-label={generating
                    ? t({
                      en: "Generating image",
                      "zh-CN": "图片生成中",
                      ja: "画像を生成中",
                      ko: "이미지 생성 중",
                    })
                    : t({
                      en: "Generate image",
                      "zh-CN": "生成图片",
                      ja: "画像を生成",
                      ko: "이미지 생성",
                    })}
                >
                  <ArrowUp aria-hidden="true" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                {generating
                  ? t({
                    en: "Generating image",
                    "zh-CN": "图片生成中",
                    ja: "画像を生成中",
                    ko: "이미지 생성 중",
                  })
                  : t({
                    en: "Generate image",
                    "zh-CN": "生成图片",
                    ja: "画像を生成",
                    ko: "이미지 생성",
                  })}
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
  const { t } = useI18n();
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
          {t(
            {
              en: "Generating · {duration}",
              "zh-CN": "生成中 · {duration}",
              ja: "生成中 · {duration}",
              ko: "생성 중 · {duration}",
            },
            { duration: formatElapsed(elapsedSeconds, t) },
          )}
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
        {t(
          {
            en: "Image generation submitted. Model {model} is generating the result.",
            "zh-CN": "图片生成任务已提交，模型 {model}，正在生成",
            ja: "画像生成を送信しました。モデル {model} が生成中です。",
            ko: "이미지 생성 작업을 제출했습니다. 모델 {model}이 결과를 생성 중입니다.",
          },
          { model: generation.modelId },
        )}
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

function formatDuration(milliseconds: number, t: Translate) {
  if (milliseconds < 1_000) {
    return t({
      en: "under 1s",
      "zh-CN": "不足 1 秒",
      ja: "1 秒未満",
      ko: "1초 미만",
    });
  }
  return formatElapsed(Math.round(milliseconds / 1_000), t);
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

function aspectRatioLabel(value: string, t: Translate) {
  if (value === "1:1") {
    return t({ en: "Square", "zh-CN": "方形", ja: "正方形", ko: "정사각형" });
  }
  if (value === "3:4") {
    return t({ en: "Portrait", "zh-CN": "竖版", ja: "縦長", ko: "세로형" });
  }
  if (value === "4:3") {
    return t({ en: "Landscape", "zh-CN": "横版", ja: "横長", ko: "가로형" });
  }
  if (value === "9:16") {
    return t({ en: "Story", "zh-CN": "故事", ja: "ストーリー", ko: "스토리" });
  }
  if (value === "16:9") {
    return t({ en: "Widescreen", "zh-CN": "宽屏", ja: "ワイド", ko: "와이드스크린" });
  }
  return t({ en: "Ratio", "zh-CN": "比例", ja: "比率", ko: "비율" });
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

function formatHistoryTime(value: number, locale: Locale) {
  return new Intl.DateTimeFormat(locale, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}

type Translate = (
  text: LocalizedText,
  values?: Record<string, string | number>,
) => string;

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
