"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { LoaderCircle, LogIn } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useI18n } from "@/i18n/locale-provider";
import {
  consoleFetch,
  consoleRequestFailure,
} from "@/lib/auth/client";

export function LoginForm() {
  const router = useRouter();
  const { t } = useI18n();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  async function passwordLogin(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await login({ email: email.trim(), password });
  }

  async function login(body: Record<string, string>) {
    setPending(true);
    setError(null);
    const signInFailed = t({
      en: "Sign-in failed.",
      "zh-CN": "登录失败。",
      ja: "ログインに失敗しました。",
      ko: "로그인에 실패했습니다.",
    });
    try {
      const response = await consoleFetch(
        "/api/session",
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        },
        { retryUnauthorized: false },
      );
      if (!response.ok) {
        setError(
          response.status === 401
            ? t({
                en: "Incorrect email or password",
                "zh-CN": "邮箱或密码错误",
                ja: "メールアドレスまたはパスワードが正しくありません",
                ko: "이메일 또는 비밀번호가 올바르지 않습니다",
              })
            : t({
                en: "Sign-in failed.",
                "zh-CN": "登录失败。",
                ja: "ログインに失敗しました。",
                ko: "로그인에 실패했습니다.",
              }),
        );
        return;
      }
      router.replace("/overview");
      router.refresh();
    } catch (reason) {
      setError(consoleRequestFailure(reason, signInFailed, t));
    } finally {
      setPending(false);
    }
  }

  return (
    <form className="space-y-4" onSubmit={passwordLogin}>
      <div className="space-y-2">
        <Label htmlFor="email">
          {t({
            en: "Email",
            "zh-CN": "邮箱",
            ja: "メールアドレス",
            ko: "이메일",
          })}
        </Label>
        <Input
          id="email"
          type="email"
          autoComplete="username"
          value={email}
          onChange={(event) => setEmail(event.target.value)}
          required
          autoFocus
        />
      </div>
      <div className="space-y-2">
        <Label htmlFor="password">
          {t({
            en: "Password",
            "zh-CN": "密码",
            ja: "パスワード",
            ko: "비밀번호",
          })}
        </Label>
        <Input
          id="password"
          type="password"
          autoComplete="current-password"
          value={password}
          onChange={(event) => setPassword(event.target.value)}
          required
        />
      </div>
      <LoginError message={error} />
      <Button className="w-full" type="submit" disabled={pending || !email.trim() || !password}>
        {pending ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <LogIn aria-hidden="true" />}
        {t({
          en: "Sign in",
          "zh-CN": "登录",
          ja: "ログイン",
          ko: "로그인",
        })}
      </Button>
    </form>
  );
}

function LoginError({ message }: { message: string | null }) {
  return message ? (
    <p role="alert" className="text-sm text-destructive">
      {message}
    </p>
  ) : null;
}
