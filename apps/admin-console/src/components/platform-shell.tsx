import { ExternalLink, FileJson, RadioTower, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { JobTable } from "@/components/job-table";
import { MetricCard } from "@/components/metric-card";
import { ProviderTable } from "@/components/provider-table";
import { metrics, navItems } from "@/lib/platform-data";

export function PlatformShell() {
  return (
    <div className="min-h-screen">
      <aside className="fixed inset-y-0 left-0 hidden w-64 border-r border-[var(--line)] bg-[#fbfbf8] px-4 py-5 lg:block">
        <div className="mb-8">
          <p className="text-sm font-semibold">AI Image Factory</p>
          <p className="mt-1 text-xs text-[var(--muted)]">API platform console</p>
        </div>
        <nav className="space-y-1">
          {navItems.map((item, index) => (
            <button
              key={item.label}
              className={`flex h-9 w-full items-center gap-3 rounded-md px-3 text-left text-sm ${
                index === 0 ? "bg-[var(--panel-strong)] text-[#18222f]" : "text-[#56616d] hover:bg-[var(--panel-strong)]"
              }`}
            >
              <item.icon className="size-4" aria-hidden="true" />
              <span className="truncate">{item.label}</span>
            </button>
          ))}
        </nav>
      </aside>

      <main className="lg:pl-64">
        <header className="sticky top-0 z-10 border-b border-[var(--line)] bg-[rgba(247,247,244,0.92)] px-4 py-3 backdrop-blur md:px-8">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h1 className="text-xl font-semibold">Platform Operations</h1>
              <p className="mt-1 text-sm text-[var(--muted)]">OpenAI-compatible image API, provider routing, billing, scheduling, and observability.</p>
            </div>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="icon" aria-label="Refresh console data" title="Refresh console data">
                <RefreshCw className="size-4" aria-hidden="true" />
              </Button>
              <Button variant="outline" className="hidden sm:inline-flex">
                <FileJson className="size-4" aria-hidden="true" />
                OpenAPI
              </Button>
              <Button>
                <RadioTower className="size-4" aria-hidden="true" />
                Traces
              </Button>
            </div>
          </div>
        </header>

        <div className="space-y-8 px-4 py-6 md:px-8">
          <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            {metrics.map((metric) => (
              <MetricCard key={metric.label} {...metric} />
            ))}
          </section>

          <section className="grid gap-6 xl:grid-cols-[1.35fr_0.65fr]">
            <ProviderTable />
            <div className="rounded-lg border border-[var(--line)] bg-[var(--panel)] p-4">
              <h2 className="text-base font-semibold">Gateway Links</h2>
              <div className="mt-4 space-y-3">
                {[
                  ["/healthz", "Liveness"],
                  ["/openapi.json", "OpenAPI 3.1"],
                  ["/docs", "Scalar reference"],
                ].map(([path, label]) => (
                  <a
                    key={path}
                    href={`/api/gateway${path}`}
                    className="flex h-10 items-center justify-between rounded-md border border-[var(--line)] px-3 text-sm hover:bg-[var(--panel-strong)]"
                  >
                    <span>{label}</span>
                    <ExternalLink className="size-4 text-[var(--muted)]" aria-hidden="true" />
                  </a>
                ))}
              </div>
            </div>
          </section>

          <JobTable />
        </div>
      </main>
    </div>
  );
}
