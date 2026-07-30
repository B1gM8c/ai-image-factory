"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import {
  formatLocalizedText,
  isLocale,
  localeCookieName,
  localeStorageKey,
  type Locale,
  type LocalizedText,
} from "@/i18n/config";

type LocaleContextValue = {
  locale: Locale;
  setLocale: (locale: Locale) => void;
  t: (
    text: LocalizedText,
    values?: Record<string, string | number>,
  ) => string;
};

const LocaleContext = createContext<LocaleContextValue | null>(null);

export function LocaleProvider({
  children,
  initialLocale,
}: {
  children: React.ReactNode;
  initialLocale: Locale;
}) {
  const [locale, setLocaleState] = useState<Locale>(initialLocale);

  useEffect(() => {
    function syncLocale(event: StorageEvent) {
      if (event.key === localeStorageKey && isLocale(event.newValue)) {
        setLocaleState(event.newValue);
        document.documentElement.lang = event.newValue;
        persistLocaleCookie(event.newValue);
      }
    }

    window.addEventListener("storage", syncLocale);
    return () => window.removeEventListener("storage", syncLocale);
  }, []);

  const setLocale = useCallback((nextLocale: Locale) => {
    setLocaleState(nextLocale);
    try {
      window.localStorage.setItem(localeStorageKey, nextLocale);
    } catch {
      // The in-memory preference still applies when storage is unavailable.
    }
    persistLocaleCookie(nextLocale);
    document.documentElement.lang = nextLocale;
  }, []);

  const t = useCallback(
    (
      text: LocalizedText,
      values?: Record<string, string | number>,
    ) => formatLocalizedText(text, locale, values),
    [locale],
  );

  const value = useMemo(
    () => ({ locale, setLocale, t }),
    [locale, setLocale, t],
  );

  return (
    <LocaleContext.Provider value={value}>{children}</LocaleContext.Provider>
  );
}

export function useI18n() {
  const value = useContext(LocaleContext);
  if (!value) {
    throw new Error("useI18n must be used within LocaleProvider");
  }
  return value;
}

function persistLocaleCookie(locale: Locale) {
  document.cookie = `${localeCookieName}=${encodeURIComponent(locale)}; Path=/; Max-Age=31536000; SameSite=Lax`;
}
