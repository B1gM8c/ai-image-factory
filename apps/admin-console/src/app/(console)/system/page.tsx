import { CapabilityGuard } from "@/components/auth/capability-guard";
import { SystemStatusView } from "@/components/system-update-panel";
import { getGatewaySnapshot } from "@/lib/gateway/server";

export default async function SystemPage() {
  const snapshot = await getGatewaySnapshot();
  return (
    <CapabilityGuard capability="system:read" platformOnly>
      <SystemStatusView snapshot={snapshot} />
    </CapabilityGuard>
  );
}
