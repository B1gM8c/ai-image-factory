import { Badge } from "@/components/ui/badge";
import { formatStatus } from "@/lib/admin/format";

export function ActivityStatusBadge({ state }: { state: string }) {
  return (
    <Badge variant="outline" className="gap-1.5">
      <span
        className={`size-1.5 rounded-full ${statusDot(state)}`}
        aria-hidden="true"
      />
      {formatStatus(state)}
    </Badge>
  );
}

function statusDot(state: string): string {
  switch (state.toLowerCase()) {
    case "completed":
    case "succeeded":
      return "bg-emerald-500";
    case "failed":
      return "bg-destructive";
    case "uncertain":
      return "bg-amber-500";
    case "running":
      return "bg-sky-500";
    default:
      return "bg-muted-foreground";
  }
}
