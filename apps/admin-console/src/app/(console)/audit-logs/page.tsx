"use client";

import { AdminAuditLogs } from "@/components/admin-audit-logs";
import { CapabilityGuard } from "@/components/auth/capability-guard";
import { PageHeader } from "@/components/page-header";
import { useI18n } from "@/i18n/locale-provider";

export default function AuditLogsPage() {
  const { t } = useI18n();

  return (
    <CapabilityGuard capability="admin:*" platformOnly>
      <div className="min-w-0 space-y-6">
        <PageHeader
          title={t({
            en: "Audit logs",
            "zh-CN": "审计日志",
            ja: "監査ログ",
            ko: "감사 로그",
          })}
          description={t({
            en: "Review control-plane activity for sign-ins, project settings, API keys, and billing.",
            "zh-CN": "查看登录、项目配置、密钥和计费等控制平面操作。",
            ja: "サインイン、プロジェクト設定、API キー、請求に関するコントロールプレーン操作を確認します。",
            ko: "로그인, 프로젝트 설정, API 키, 결제 관련 제어 영역 활동을 확인합니다.",
          })}
        />
        <AdminAuditLogs />
      </div>
    </CapabilityGuard>
  );
}
