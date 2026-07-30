export const supportedLocales = ["en", "zh-CN", "ja", "ko"] as const;

export type Locale = (typeof supportedLocales)[number];

export type LocalizedText = {
  en: string;
  "zh-CN": string;
  ja: string;
  ko: string;
};

export const defaultLocale: Locale = "en";
export const localeCookieName = "aif-locale";
export const localeStorageKey = "aif-locale";

export const localeLabels: Record<Locale, string> = {
  en: "English",
  "zh-CN": "简体中文",
  ja: "日本語",
  ko: "한국어",
};

export function isLocale(value: string | null): value is Locale {
  return supportedLocales.some((locale) => locale === value);
}

export function formatLocalizedText(
  text: LocalizedText,
  locale: Locale,
  values: Record<string, string | number> = {},
) {
  const template = text[locale];
  return Object.entries(values).reduce(
    (message, [key, value]) =>
      message.replaceAll(`{${key}}`, String(value)),
    template,
  );
}
