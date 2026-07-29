"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { LoaderCircle } from "lucide-react";
import { useConsoleSession } from "@/components/auth/console-session-provider";

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
        <LoaderCircle className="size-5 animate-spin" aria-label="正在检查访问权限" />
      </div>
    );
  }

  return children;
}
