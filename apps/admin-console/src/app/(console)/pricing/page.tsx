import { CapabilityGuard } from "@/components/auth/capability-guard";
import { PricingManager } from "@/components/pricing/pricing-manager";

export default function PricingPage() {
  return (
    <CapabilityGuard capability="admin:*" platformOnly>
      <PricingManager />
    </CapabilityGuard>
  );
}
