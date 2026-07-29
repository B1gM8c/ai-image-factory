import { AdminScheduler } from "@/components/admin-scheduler";
import { CapabilityGuard } from "@/components/auth/capability-guard";

export default function SchedulingPage() {
  return (
    <CapabilityGuard capability="scheduler:read" platformOnly>
      <AdminScheduler />
    </CapabilityGuard>
  );
}
