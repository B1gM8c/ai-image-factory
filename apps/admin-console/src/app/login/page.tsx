import { redirect } from "next/navigation";
import { LoginPanel } from "@/components/auth/login-panel";
import { hasConsoleSession } from "@/lib/auth/session";

export default async function LoginPage() {
  if (await hasConsoleSession()) {
    redirect("/overview");
  }

  return (
    <main className="flex min-h-svh flex-col items-center justify-center gap-6 bg-muted p-6 md:p-10">
      <LoginPanel />
    </main>
  );
}
