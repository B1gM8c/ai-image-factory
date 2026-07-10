import type { LucideIcon } from "lucide-react";
import { Badge } from "@/components/ui/badge";

const badgeTone = {
  green: "green",
  blue: "blue",
  amber: "amber",
  neutral: "neutral",
} as const;

export function MetricCard({
  label,
  value,
  detail,
  tone,
  icon: Icon,
}: {
  label: string;
  value: string;
  detail: string;
  tone: keyof typeof badgeTone;
  icon: LucideIcon;
}) {
  return (
    <section className="min-w-0 rounded-lg border border-[var(--line)] bg-[var(--panel)] p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-sm text-[var(--muted)]">{label}</p>
          <p className="mt-2 truncate text-2xl font-semibold">{value}</p>
        </div>
        <span className="inline-flex size-9 shrink-0 items-center justify-center rounded-md bg-[var(--panel-strong)] text-[#30445a]">
          <Icon className="size-4" aria-hidden="true" />
        </span>
      </div>
      <Badge tone={badgeTone[tone]} className="mt-4 max-w-full truncate">
        {detail}
      </Badge>
    </section>
  );
}
