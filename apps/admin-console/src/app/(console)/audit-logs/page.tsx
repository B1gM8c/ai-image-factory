import { AdminAuditLogs } from "@/components/admin-audit-logs";
import { CapabilityGuard } from "@/components/auth/capability-guard";
import { PageHeader } from "@/components/page-header";

export default function AuditLogsPage() {
  return (
    <CapabilityGuard capability="admin:*" platformOnly>
      <div className="min-w-0 space-y-6">
        <PageHeader
          title="审计日志"
          description="查看登录、项目配置、密钥和计费等控制平面操作。"
        />
        <AdminAuditLogs />
      </div>
    </CapabilityGuard>
  );
}
