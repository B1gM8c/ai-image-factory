"use client";

import { useState } from "react";
import { ChevronLeft, ChevronRight, Download } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogTitle,
} from "@/components/ui/dialog";
import { useI18n } from "@/i18n/locale-provider";

export type ImageViewerItem = {
  src: string;
  alt: string;
};

export function ImageViewerDialog({
  items,
  activeIndex,
  onActiveIndexChange,
  onDownload,
}: {
  items: ImageViewerItem[];
  activeIndex: number | null;
  onActiveIndexChange: (index: number | null) => void;
  onDownload: (index: number) => void;
}) {
  const { t } = useI18n();
  const [dimensions, setDimensions] = useState<string | null>(null);
  const currentIndex =
    activeIndex === null || items.length === 0
      ? null
      : Math.min(activeIndex, items.length - 1);
  const current = currentIndex === null ? null : items[currentIndex];

  function move(offset: number) {
    if (currentIndex === null || items.length < 2) return;
    setDimensions(null);
    onActiveIndexChange(
      (currentIndex + offset + items.length) % items.length,
    );
  }

  return (
    <Dialog
      open={current !== null}
      onOpenChange={(open) => {
        if (!open) {
          setDimensions(null);
          onActiveIndexChange(null);
        }
      }}
    >
      <DialogContent
        className="flex h-[calc(100dvh-2rem)] w-[calc(100vw-2rem)] max-w-[96rem] gap-0 overflow-hidden border-white/10 bg-black p-0 text-white shadow-2xl [&>button]:z-20 [&>button]:text-white"
        onKeyDown={(event) => {
          if (event.key === "ArrowLeft") move(-1);
          if (event.key === "ArrowRight") move(1);
        }}
      >
        <DialogTitle className="sr-only">
          {t(
            {
              en: "View original{index}",
              "zh-CN": "查看原图{index}",
              ja: "元画像を表示{index}",
              ko: "원본 이미지 보기{index}",
            },
            { index: currentIndex === null ? "" : ` ${currentIndex + 1}` },
          )}
        </DialogTitle>
        {current && currentIndex !== null ? (
          <div className="flex min-h-0 flex-1 flex-col">
            <div className="flex h-12 shrink-0 items-center gap-3 border-b border-white/10 px-4 pr-14 text-sm text-white/70">
              <span>
                {t(
                  {
                    en: "Original {current} / {total}",
                    "zh-CN": "原图 {current} / {total}",
                    ja: "元画像 {current} / {total}",
                    ko: "원본 {current} / {total}",
                  },
                  { current: currentIndex + 1, total: items.length },
                )}
              </span>
              {dimensions ? (
                <span className="text-white/45">{dimensions}</span>
              ) : null}
              <Button
                type="button"
                size="icon"
                variant="ghost"
                className="ml-auto size-8 text-white hover:bg-white/10 hover:text-white"
                onClick={() => onDownload(currentIndex)}
                aria-label={t(
                  {
                    en: "Download original {index}",
                    "zh-CN": "下载原图 {index}",
                    ja: "元画像 {index} をダウンロード",
                    ko: "원본 이미지 {index} 다운로드",
                  },
                  { index: currentIndex + 1 },
                )}
              >
                <Download aria-hidden="true" />
              </Button>
            </div>
            <div className="relative flex min-h-0 flex-1 items-center justify-center p-4 sm:p-6">
              {items.length > 1 ? (
                <Button
                  type="button"
                  size="icon"
                  variant="secondary"
                  className="absolute left-3 z-10 size-9 bg-black/60 text-white hover:bg-black/80 sm:left-5"
                  onClick={() => move(-1)}
                  aria-label={t({
                    en: "View previous original",
                    "zh-CN": "查看上一张原图",
                    ja: "前の元画像を表示",
                    ko: "이전 원본 이미지 보기",
                  })}
                >
                  <ChevronLeft aria-hidden="true" />
                </Button>
              ) : null}
              <img
                key={current.src}
                src={current.src}
                alt={current.alt}
                className="max-h-full max-w-full select-none object-contain"
                draggable={false}
                onLoad={(event) => {
                  setDimensions(
                    `${event.currentTarget.naturalWidth} × ${event.currentTarget.naturalHeight}`,
                  );
                }}
              />
              {items.length > 1 ? (
                <Button
                  type="button"
                  size="icon"
                  variant="secondary"
                  className="absolute right-3 z-10 size-9 bg-black/60 text-white hover:bg-black/80 sm:right-5"
                  onClick={() => move(1)}
                  aria-label={t({
                    en: "View next original",
                    "zh-CN": "查看下一张原图",
                    ja: "次の元画像を表示",
                    ko: "다음 원본 이미지 보기",
                  })}
                >
                  <ChevronRight aria-hidden="true" />
                </Button>
              ) : null}
            </div>
          </div>
        ) : null}
      </DialogContent>
    </Dialog>
  );
}
