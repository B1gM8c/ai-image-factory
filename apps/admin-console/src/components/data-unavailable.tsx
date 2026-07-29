import { DatabaseZap } from "lucide-react";

export function DataUnavailable({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <div className="flex min-h-44 flex-col items-center justify-center border border-dashed bg-muted/20 px-6 py-8 text-center">
      <span className="mb-3 flex size-9 items-center justify-center rounded-md border bg-background text-muted-foreground">
        <DatabaseZap className="size-4" aria-hidden="true" />
      </span>
      <p className="text-sm font-medium">{title}</p>
      <p className="mt-1 max-w-xl text-sm text-muted-foreground">{description}</p>
    </div>
  );
}
