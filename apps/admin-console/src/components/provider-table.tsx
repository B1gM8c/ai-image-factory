import { Badge } from "@/components/ui/badge";
import { providers } from "@/lib/platform-data";

export function ProviderTable() {
  return (
    <section className="min-w-0">
      <div className="mb-3 flex items-center justify-between gap-4">
        <div>
          <h2 className="text-base font-semibold">Provider Registry</h2>
          <p className="mt-1 text-sm text-[var(--muted)]">Active adapters and planned expansion slots.</p>
        </div>
      </div>
      <div className="overflow-x-auto rounded-lg border border-[var(--line)] bg-[var(--panel)]">
        <table className="w-full min-w-[760px] border-collapse text-left text-sm">
          <thead className="bg-[var(--panel-strong)] text-xs uppercase text-[#59636f]">
            <tr>
              <th className="px-4 py-3 font-semibold">Provider</th>
              <th className="px-4 py-3 font-semibold">Status</th>
              <th className="px-4 py-3 font-semibold">Model</th>
              <th className="px-4 py-3 font-semibold">Mode</th>
              <th className="px-4 py-3 font-semibold">Capability</th>
            </tr>
          </thead>
          <tbody>
            {providers.map((provider) => (
              <tr key={provider.id} className="border-t border-[var(--line)]">
                <td className="px-4 py-3">
                  <div className="font-medium">{provider.name}</div>
                  <code className="text-xs text-[var(--muted)]">{provider.id}</code>
                </td>
                <td className="px-4 py-3">
                  <Badge tone={provider.status === "Active" ? "green" : "amber"}>{provider.status}</Badge>
                </td>
                <td className="px-4 py-3 font-mono text-xs">{provider.model}</td>
                <td className="px-4 py-3">{provider.mode}</td>
                <td className="px-4 py-3 text-[var(--muted)]">{provider.capability}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
