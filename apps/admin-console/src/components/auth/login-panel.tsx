"use client";

import { Factory } from "lucide-react";
import { LoginForm } from "@/components/auth/login-form";
import { LocaleSwitcher } from "@/components/i18n/locale-switcher";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { useI18n } from "@/i18n/locale-provider";

export function LoginPanel() {
  const { t } = useI18n();

  return (
    <div className="flex w-full max-w-sm flex-col gap-6">
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2 font-medium">
          <span className="flex size-6 items-center justify-center rounded-md bg-primary text-primary-foreground">
            <Factory className="size-4" aria-hidden="true" />
          </span>
          <span>AI Image Factory</span>
        </div>
        <LocaleSwitcher compact />
      </div>
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">
            {t({
              en: "Sign in",
              "zh-CN": "账户登录",
              ja: "ログイン",
              ko: "로그인",
            })}
          </CardTitle>
          <CardDescription>
            {t({
              en: "Sign in to access your authorized workspaces.",
              "zh-CN": "登录后进入你有权访问的工作区。",
              ja: "ログインして、アクセス権のあるワークスペースを開きます。",
              ko: "로그인하여 접근 권한이 있는 워크스페이스를 이용하세요.",
            })}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <LoginForm />
        </CardContent>
      </Card>
    </div>
  );
}
