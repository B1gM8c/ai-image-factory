"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { LoaderCircle } from "lucide-react";
import { useConsoleSession } from "@/components/auth/console-session-provider";
import { useI18n } from "@/i18n/locale-provider";

export function CapabilityGuard({
  capability,
  platformOnly = false,
  children,
}: {
  capability: string;
  platformOnly?: boolean;
  children: React.ReactNode;
}) {
  const router = useRouter();
  const { t } = useI18n();
  const { capabilities, loading, user } = useConsoleSession();
  const platformOwner = Boolean(user?.roles.includes("platform_owner"));
  const allowed =
    (!platformOnly || platformOwner) &&
    (platformOwner ||
      capabilities.includes("admin:*") ||
      capabilities.includes(capability) ||
      capabilities.includes(`${capability.split(":", 1)[0]}:*`));

  useEffect(() => {
    if (!loading && !allowed) router.replace("/overview");
  }, [allowed, loading, router]);

  if (loading || !allowed) {
    return (
      <div className="flex min-h-48 items-center justify-center text-muted-foreground">
        <LoaderCircle
          className="size-5 animate-spin"
          aria-label={t({
            en: "Checking access",
            "zh-CN": "正在检查访问权限",
            ja: "アクセス権を確認中",
            ko: "접근 권한 확인 중",
          })}
        />
      </div>
    );
  }

  return children;
}
