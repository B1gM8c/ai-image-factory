import { PageHeader } from "@/components/page-header";
import { ProjectsManager } from "@/components/projects/projects-manager";

export default function ProjectsPage() {
  return (
    <div className="space-y-6">
      <PageHeader description="管理组织下可供团队共享 API 资源与访问策略的项目。" title="项目" />
      <ProjectsManager />
    </div>
  );
}
