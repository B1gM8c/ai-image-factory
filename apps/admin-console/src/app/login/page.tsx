import { redirect } from "next/navigation";
import { LoginPanel } from "@/components/auth/login-panel";
import { hasActiveConsoleSession } from "@/lib/auth/session";

type LoginPageProps = {
  searchParams: Promise<{ reason?: string | string[] }>;
};

export default async function LoginPage({ searchParams }: LoginPageProps) {
  const reason = (await searchParams).reason;
  if (reason !== "session_expired" && (await hasActiveConsoleSession())) {
    redirect("/overview");
  }

  return (
    <main className="flex min-h-svh flex-col items-center justify-center gap-6 bg-muted p-6 md:p-10">
      <LoginPanel />
    </main>
  );
}
