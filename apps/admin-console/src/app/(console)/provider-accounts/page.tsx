import { AdminProviderAccounts } from "@/components/admin-provider-accounts";
import { CapabilityGuard } from "@/components/auth/capability-guard";

export default function ProviderAccountsPage() {
  return (
    <CapabilityGuard capability="providers:manage" platformOnly>
      <AdminProviderAccounts />
    </CapabilityGuard>
  );
}
