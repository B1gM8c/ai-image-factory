"use client";

import { ApiKeysManager } from "@/components/keys/api-keys-manager";
import { PageHeader } from "@/components/page-header";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { useI18n } from "@/i18n/locale-provider";

export default function KeysPage() {
  const { t } = useI18n();

  return (
    <div className="space-y-6">
      <PageHeader
        title={t({
          en: "API keys",
          "zh-CN": "API 密钥",
          ja: "API キー",
          ko: "API 키",
        })}
        description={t({
          en: "Create access keys by project and choose either the platform's standard service or an administrator-configured account group.",
          "zh-CN": "按项目创建访问密钥，并选择平台标准服务或管理员配置的账户组。",
          ja: "プロジェクトごとにアクセスキーを作成し、プラットフォームの標準サービスまたは管理者が設定したアカウントグループを選択します。",
          ko: "프로젝트별로 액세스 키를 생성하고 플랫폼 표준 서비스 또는 관리자가 구성한 계정 그룹을 선택합니다.",
        })}
      />
      <Card className="overflow-hidden">
        <CardHeader>
          <CardTitle className="text-base">
            {t({
              en: "Project access keys",
              "zh-CN": "项目访问密钥",
              ja: "プロジェクトアクセスキー",
              ko: "프로젝트 액세스 키",
            })}
          </CardTitle>
          <CardDescription>
            {t({
              en: "A key value is returned only once when it is created. The platform selects the underlying CLI account and does not expose it to project users.",
              "zh-CN": "Key 明文只在创建时返回一次；具体 CLI 账户由平台调度，不会暴露给项目用户。",
              ja: "キー値は作成時に一度だけ返されます。使用する CLI アカウントはプラットフォームが選択し、プロジェクトユーザーには公開されません。",
              ko: "키 값은 생성 시 한 번만 반환됩니다. 실제 CLI 계정은 플랫폼이 선택하며 프로젝트 사용자에게 공개되지 않습니다.",
            })}
          </CardDescription>
        </CardHeader>
        <CardContent className="p-0">
          <ApiKeysManager />
        </CardContent>
      </Card>
    </div>
  );
}
