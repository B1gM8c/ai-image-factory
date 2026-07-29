"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { LoaderCircle, LogIn } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { consoleFetch } from "@/lib/auth/client";

export function LoginForm() {
  const router = useRouter();
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
        const payload = (await response.json().catch(() => null)) as { error?: string } | null;
        setError(payload?.error ?? "登录失败");
        return;
      }
      router.replace("/overview");
      router.refresh();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "登录失败");
    } finally {
      setPending(false);
    }
  }

  return (
    <form className="space-y-4" onSubmit={passwordLogin}>
      <div className="space-y-2">
        <Label htmlFor="email">邮箱</Label>
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
        <Label htmlFor="password">密码</Label>
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
        登录
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
