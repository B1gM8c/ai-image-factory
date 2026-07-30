"use client"

import { useTheme } from "next-themes"
import { Toaster as Sonner } from "sonner"
import { useI18n } from "@/i18n/locale-provider"
import type { LocalizedText } from "@/i18n/config"

type ToasterProps = React.ComponentProps<typeof Sonner>

const notificationsLabel: LocalizedText = {
  en: "Notifications",
  "zh-CN": "通知",
  ja: "通知",
  ko: "알림",
}

const Toaster = ({ ...props }: ToasterProps) => {
  const { theme = "system" } = useTheme()
  const { t } = useI18n()

  return (
    <Sonner
      theme={theme as ToasterProps["theme"]}
      className="toaster group"
      containerAriaLabel={t(notificationsLabel)}
      toastOptions={{
        classNames: {
          toast:
            "group toast group-[.toaster]:bg-background group-[.toaster]:text-foreground group-[.toaster]:border-border group-[.toaster]:shadow-lg",
          description: "group-[.toast]:text-muted-foreground",
          actionButton:
            "group-[.toast]:bg-primary group-[.toast]:text-primary-foreground",
          cancelButton:
            "group-[.toast]:bg-muted group-[.toast]:text-muted-foreground",
        },
      }}
      {...props}
    />
  )
}

export { Toaster }
