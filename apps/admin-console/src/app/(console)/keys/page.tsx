import { ApiKeysManager } from "@/components/keys/api-keys-manager";
import { PageHeader } from "@/components/page-header";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

export default function KeysPage() {
  return (
    <div className="space-y-6">
      <PageHeader
        title="API Keys"
        description="按项目创建访问密钥，并选择平台标准服务或管理员配置的账户组。"
      />
      <Card className="overflow-hidden">
        <CardHeader>
          <CardTitle className="text-base">项目访问密钥</CardTitle>
          <CardDescription>Key 明文只在创建时返回一次；具体 CLI 账户由平台调度，不会暴露给项目用户。</CardDescription>
        </CardHeader>
        <CardContent className="p-0">
          <ApiKeysManager />
        </CardContent>
      </Card>
    </div>
  );
}
