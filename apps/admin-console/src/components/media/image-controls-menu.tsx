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
              aria-label="图片参数"
            >
              <SlidersHorizontal aria-hidden="true" />
            </Button>
          </DropdownMenuTrigger>
        </TooltipTrigger>
        <TooltipContent>图片参数</TooltipContent>
      </Tooltip>
      <DropdownMenuContent align="start" className="w-48">
        <DropdownMenuLabel>图片参数</DropdownMenuLabel>
        {qualityControl ? (
          <ChoiceSubmenu
            label="质量"
            value={quality}
            control={qualityControl}
            onValueChange={onQualityChange}
            formatLabel={qualityLabel}
          />
        ) : null}
        {outputFormatControl ? (
          <ChoiceSubmenu
            label="格式"
            value={outputFormat}
            control={outputFormatControl}
            onValueChange={onOutputFormatChange}
            formatLabel={(value) => value.toUpperCase()}
          />
        ) : null}
        {backgroundControl ? (
          <ChoiceSubmenu
            label="背景"
            value={background}
            control={backgroundControl}
            onValueChange={onBackgroundChange}
            formatLabel={backgroundLabel}
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

function qualityLabel(value: string) {
  if (value === "auto") return "自动";
  if (value === "high") return "高";
  if (value === "medium") return "中";
  if (value === "low") return "低";
  return value;
}

function backgroundLabel(value: string) {
  if (value === "auto") return "自动";
  if (value === "opaque") return "不透明";
  return value;
}
