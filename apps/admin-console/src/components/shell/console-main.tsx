"use client";

import { usePathname } from "next/navigation";
import { requiresProjectWorkspace } from "@/lib/navigation";
import { cn } from "@/lib/utils";

export function ConsoleMain({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const isCreativeWorkspace = requiresProjectWorkspace(pathname);

  return (
    <main
      className={cn(
        "@container/main min-w-0 flex flex-1 flex-col",
        isCreativeWorkspace
          ? "min-h-0 overflow-hidden"
          : "gap-4 px-4 py-4 md:gap-6 md:py-6 lg:px-6",
      )}
    >
      {children}
    </main>
  );
}
