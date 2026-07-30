import type { Metadata } from "next";
import { cookies } from "next/headers";
import { ThemeProvider } from "@/components/theme-provider";
import { Toaster } from "@/components/ui/sonner";
import {
  defaultLocale,
  isLocale,
  localeCookieName,
} from "@/i18n/config";
import { LocaleProvider } from "@/i18n/locale-provider";
import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "AI Image Factory",
    template: "%s | AI Image Factory",
  },
  description: "AI Image Factory operations console",
};

export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const cookieStore = await cookies();
  const savedLocale = cookieStore.get(localeCookieName)?.value ?? null;
  const initialLocale = isLocale(savedLocale) ? savedLocale : defaultLocale;

  return (
    <html lang={initialLocale} suppressHydrationWarning>
      <body className="min-h-screen bg-background text-foreground antialiased">
        <ThemeProvider
          attribute="class"
          defaultTheme="system"
          enableSystem
          disableTransitionOnChange
          storageKey="aif-theme"
        >
          <LocaleProvider initialLocale={initialLocale}>
            {children}
            <Toaster richColors position="top-right" />
          </LocaleProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
