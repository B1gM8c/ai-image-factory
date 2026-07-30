"use client";

import { PageHeader } from "@/components/page-header";
import { ProjectsManager } from "@/components/projects/projects-manager";
import { useI18n } from "@/i18n/locale-provider";

export default function ProjectsPage() {
  const { t } = useI18n();

  return (
    <div className="space-y-6">
      <PageHeader
        description={t({
          en: "Manage projects that share API resources and access policies across your organization.",
          "zh-CN": "管理组织下可供团队共享 API 资源与访问策略的项目。",
          ja: "組織内で API リソースとアクセスポリシーを共有するプロジェクトを管理します。",
          ko: "조직에서 API 리소스와 액세스 정책을 공유하는 프로젝트를 관리합니다.",
        })}
        title={t({ en: "Projects", "zh-CN": "项目", ja: "プロジェクト", ko: "프로젝트" })}
      />
      <ProjectsManager />
    </div>
  );
}
