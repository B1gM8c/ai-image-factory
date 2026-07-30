"use client";

import { SlidersHorizontal } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useI18n } from "@/i18n/locale-provider";

export type ImageChoiceControl = {
  default: string;
  options: string[];
};

export function ImageControlsMenu({
  quality,
  outputFormat,
  background,
  qualityControl,
  outputFormatControl,
  backgroundControl,
  onQualityChange,
  onOutputFormatChange,
  onBackgroundChange,
}: {
  quality: string;
  outputFormat: string;
  background: string;
  qualityControl?: ImageChoiceControl;
  outputFormatControl?: ImageChoiceControl;
  backgroundControl?: ImageChoiceControl;
  onQualityChange: (value: string) => void;
  onOutputFormatChange: (value: string) => void;
  onBackgroundChange: (value: string) => void;
}) {
  const { t } = useI18n();

  if (!qualityControl && !outputFormatControl && !backgroundControl) return null;

  return (
    <DropdownMenu>
      <Tooltip>
        <TooltipTrigger asChild>
          <DropdownMenuTrigger asChild>
            <Button
              type="button"
              size="icon"
              variant="ghost"
              className="size-8 bg-muted"
              aria-label={t({
                en: "Image settings",
                "zh-CN": "图片参数",
                ja: "画像設定",
                ko: "이미지 설정",
              })}
            >
              <SlidersHorizontal aria-hidden="true" />
            </Button>
          </DropdownMenuTrigger>
        </TooltipTrigger>
        <TooltipContent>
          {t({
            en: "Image settings",
            "zh-CN": "图片参数",
            ja: "画像設定",
            ko: "이미지 설정",
          })}
        </TooltipContent>
      </Tooltip>
      <DropdownMenuContent align="start" className="w-48">
        <DropdownMenuLabel>
          {t({
            en: "Image settings",
            "zh-CN": "图片参数",
            ja: "画像設定",
            ko: "이미지 설정",
          })}
        </DropdownMenuLabel>
        {qualityControl ? (
          <ChoiceSubmenu
            label={t({
              en: "Quality",
              "zh-CN": "质量",
              ja: "品質",
              ko: "품질",
            })}
            value={quality}
            control={qualityControl}
            onValueChange={onQualityChange}
            formatLabel={(value) => qualityLabel(value, t)}
          />
        ) : null}
        {outputFormatControl ? (
          <ChoiceSubmenu
            label={t({
              en: "Format",
              "zh-CN": "格式",
              ja: "形式",
              ko: "형식",
            })}
            value={outputFormat}
            control={outputFormatControl}
            onValueChange={onOutputFormatChange}
            formatLabel={(value) => value.toUpperCase()}
          />
        ) : null}
        {backgroundControl ? (
          <ChoiceSubmenu
            label={t({
              en: "Background",
              "zh-CN": "背景",
              ja: "背景",
              ko: "배경",
            })}
            value={background}
            control={backgroundControl}
            onValueChange={onBackgroundChange}
            formatLabel={(value) => backgroundLabel(value, t)}
          />
        ) : null}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function ChoiceSubmenu({
  label,
  value,
  control,
  onValueChange,
  formatLabel,
}: {
  label: string;
  value: string;
  control: ImageChoiceControl;
  onValueChange: (value: string) => void;
  formatLabel: (value: string) => string;
}) {
  return (
    <DropdownMenuSub>
      <DropdownMenuSubTrigger>
        <span>{label}</span>
        <span className="ml-auto mr-2 text-xs text-muted-foreground">
          {formatLabel(value)}
        </span>
      </DropdownMenuSubTrigger>
      <DropdownMenuSubContent className="min-w-36">
        <DropdownMenuRadioGroup value={value} onValueChange={onValueChange}>
          {control.options.map((option) => (
            <DropdownMenuRadioItem key={option} value={option}>
              {formatLabel(option)}
            </DropdownMenuRadioItem>
          ))}
        </DropdownMenuRadioGroup>
      </DropdownMenuSubContent>
    </DropdownMenuSub>
  );
}

type Translate = ReturnType<typeof useI18n>["t"];

function qualityLabel(value: string, t: Translate) {
  if (value === "auto") {
    return t({ en: "Auto", "zh-CN": "自动", ja: "自動", ko: "자동" });
  }
  if (value === "high") {
    return t({ en: "High", "zh-CN": "高", ja: "高", ko: "높음" });
  }
  if (value === "medium") {
    return t({ en: "Medium", "zh-CN": "中", ja: "中", ko: "중간" });
  }
  if (value === "low") {
    return t({ en: "Low", "zh-CN": "低", ja: "低", ko: "낮음" });
  }
  return value;
}

function backgroundLabel(value: string, t: Translate) {
  if (value === "auto") {
    return t({ en: "Auto", "zh-CN": "自动", ja: "自動", ko: "자동" });
  }
  if (value === "opaque") {
    return t({
      en: "Opaque",
      "zh-CN": "不透明",
      ja: "不透明",
      ko: "불투명",
    });
  }
  return value;
}
