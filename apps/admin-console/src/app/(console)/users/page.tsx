import { AdminUsers } from "@/components/admin-users";
import { CapabilityGuard } from "@/components/auth/capability-guard";
import { PageHeader } from "@/components/page-header";

export default function UsersPage() {
  return (
    <CapabilityGuard capability="users:manage" platformOnly>
      <div className="min-w-0 space-y-6">
        <PageHeader
          title="用户管理"
          description="添加可独立登录的用户，并查看其默认工作区、项目和账号状态。"
        />
        <AdminUsers />
      </div>
    </CapabilityGuard>
  );
}
