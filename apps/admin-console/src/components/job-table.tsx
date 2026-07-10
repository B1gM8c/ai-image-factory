import { Badge } from "@/components/ui/badge";
import { jobs } from "@/lib/platform-data";

export function JobTable() {
  return (
    <section className="min-w-0">
      <div className="mb-3">
        <h2 className="text-base font-semibold">Scheduling Surface</h2>
        <p className="mt-1 text-sm text-[var(--muted)]">What is active today and what becomes async job infrastructure next.</p>
      </div>
      <div className="rounded-lg border border-[var(--line)] bg-[var(--panel)]">
        {jobs.map((job) => (
          <div key={job.id} className="grid gap-3 border-t border-[var(--line)] p-4 first:border-t-0 md:grid-cols-[1.1fr_1fr_1fr_1.3fr]">
            <div>
              <p className="font-mono text-xs text-[var(--muted)]">{job.id}</p>
              <p className="mt-1 font-medium">{job.provider}</p>
            </div>
            <Badge tone={job.state === "active" ? "green" : job.state === "planned" ? "amber" : "blue"}>
              {job.state}
            </Badge>
            <p className="text-sm">{job.units}</p>
            <p className="text-sm text-[var(--muted)]">{job.latency}</p>
          </div>
        ))}
      </div>
    </section>
  );
}
