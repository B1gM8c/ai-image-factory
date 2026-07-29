import { redirect } from "next/navigation";
import { Factory } from "lucide-react";
import { LoginForm } from "@/components/auth/login-form";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { hasConsoleSession } from "@/lib/auth/session";

export default async function LoginPage() {
  if (await hasConsoleSession()) {
    redirect("/overview");
  }

  return (
    <main className="flex min-h-svh flex-col items-center justify-center gap-6 bg-muted p-6 md:p-10">
      <div className="flex w-full max-w-sm flex-col gap-6">
        <div className="flex items-center justify-center gap-2 font-medium">
          <span className="flex size-6 items-center justify-center rounded-md bg-primary text-primary-foreground">
            <Factory className="size-4" aria-hidden="true" />
          </span>
          <span>AI Image Factory</span>
        </div>
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">账户登录</CardTitle>
            <CardDescription>登录后进入你有权访问的工作区。</CardDescription>
          </CardHeader>
          <CardContent>
            <LoginForm />
          </CardContent>
        </Card>
      </div>
    </main>
  );
}
