"use client";

import { AdminUsers } from "@/components/admin-users";
import { CapabilityGuard } from "@/components/auth/capability-guard";
import { PageHeader } from "@/components/page-header";
import { useI18n } from "@/i18n/locale-provider";

export default function UsersPage() {
  const { t } = useI18n();

  return (
    <CapabilityGuard capability="users:manage" platformOnly>
      <div className="min-w-0 space-y-6">
        <PageHeader
          title={t({
            en: "Users",
            "zh-CN": "用户管理",
            ja: "ユーザー管理",
            ko: "사용자 관리",
          })}
          description={t({
            en: "Add users with independent sign-in access and review their default workspace, project, and account status.",
            "zh-CN": "添加可独立登录的用户，并查看其默认工作区、项目和账号状态。",
            ja: "個別にサインインできるユーザーを追加し、既定のワークスペース、プロジェクト、アカウント状態を確認します。",
            ko: "독립적으로 로그인할 수 있는 사용자를 추가하고 기본 워크스페이스, 프로젝트 및 계정 상태를 확인합니다.",
          })}
        />
        <AdminUsers />
      </div>
    </CapabilityGuard>
  );
}
