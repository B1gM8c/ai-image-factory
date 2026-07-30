"use client";

import { Languages } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useI18n } from "@/i18n/locale-provider";
import {
  isLocale,
  localeLabels,
  supportedLocales,
  type LocalizedText,
} from "@/i18n/config";

const copy: Record<"label" | "ariaLabel", LocalizedText> = {
  label: {
    en: "Language",
    "zh-CN": "语言",
    ja: "言語",
    ko: "언어",
  },
  ariaLabel: {
    en: "Change language",
    "zh-CN": "切换语言",
    ja: "言語を変更",
    ko: "언어 변경",
  },
};

export function LocaleSwitcher({
  compact = false,
}: {
  compact?: boolean;
}) {
  const { locale, setLocale, t } = useI18n();

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          type="button"
          variant="ghost"
          size={compact ? "icon" : "sm"}
          aria-label={t(copy.ariaLabel)}
          title={t(copy.ariaLabel)}
        >
          <Languages aria-hidden="true" />
          {compact ? null : <span>{localeLabels[locale]}</span>}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="min-w-40">
        <DropdownMenuLabel>{t(copy.label)}</DropdownMenuLabel>
        <DropdownMenuRadioGroup
          value={locale}
          onValueChange={(value) => {
            if (isLocale(value)) setLocale(value);
          }}
        >
          {supportedLocales.map((option) => (
            <DropdownMenuRadioItem key={option} value={option}>
              {localeLabels[option]}
            </DropdownMenuRadioItem>
          ))}
        </DropdownMenuRadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
