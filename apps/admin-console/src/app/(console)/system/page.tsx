import Link from "next/link";
import { BookOpen, Braces, Database, HeartPulse, Network } from "lucide-react";
import { CapabilityGuard } from "@/components/auth/capability-guard";
import { MetricCard } from "@/components/metric-card";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { getGatewaySnapshot } from "@/lib/gateway/server";

export default async function SystemPage() {
  const snapshot = await getGatewaySnapshot();
  const profiles = snapshot.providerProfiles;
  return (
    <CapabilityGuard capability="system:read" platformOnly>
      <div className="space-y-6">
        <PageHeader
          title="系统状态"
          description="Gateway 探针、Provider Runtime 聚合和 API 契约入口。"
          actions={
            <>
              <Button variant="outline" size="sm" asChild>
                <Link href="/api/gateway/openapi.json" target="_blank"><Braces aria-hidden="true" />OpenAPI</Link>
              </Button>
              <Button size="sm" asChild>
                <Link href="/api/gateway/openapi.json" target="_blank"><BookOpen aria-hidden="true" />OpenAPI JSON</Link>
              </Button>
            </>
          }
        />
        <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <MetricCard label="Liveness" value={snapshot.health === "ok" ? "正常" : "不可达"} detail="Gateway `/healthz`" icon={HeartPulse} tone={snapshot.health === "ok" ? "success" : "danger"} />
          <MetricCard label="Readiness" value={snapshot.readiness === "ready" ? "就绪" : snapshot.readiness === "not_ready" ? "未就绪" : "不可达"} detail="Gateway `/readyz`" icon={Network} tone={snapshot.readiness === "ready" ? "success" : "danger"} />
          <MetricCard label="Active Profile" value={profiles ? String(profiles.active) : "--"} detail="Provider readiness projection" icon={Database} tone={profiles?.active ? "info" : "neutral"} />
          <MetricCard label="Blocked Profile" value={profiles ? String(profiles.blocked) : "--"} detail="不包含具体凭据原因" icon={Database} tone={profiles?.blocked ? "danger" : "success"} />
        </section>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">探针详情</CardTitle>
            <CardDescription>检查时间 {new Date(snapshot.checkedAt).toLocaleString("zh-CN", { hour12: false })}</CardDescription>
          </CardHeader>
          <CardContent className="grid gap-3 sm:grid-cols-2">
            <StatusRow label="Gateway 进程" ok={snapshot.health === "ok"} />
            <StatusRow label="数据库与 Profile 投影" ok={snapshot.readiness === "ready"} />
            <StatusRow label="Provider Profile 数据" ok={Boolean(profiles)} />
            <div className="flex items-center justify-between border px-3 py-2.5 text-sm">
              <span>执行器逐实例状态</span>
              <Badge variant="outline">等待 Read API</Badge>
            </div>
          </CardContent>
        </Card>
      </div>
    </CapabilityGuard>
  );
}

function StatusRow({ label, ok }: { label: string; ok: boolean }) {
  return (
    <div className="flex items-center justify-between border px-3 py-2.5 text-sm">
      <span>{label}</span>
      <Badge className={ok ? "bg-muted/50" : ""} variant={ok ? "outline" : "destructive"}>
        {ok ? "正常" : "异常"}
      </Badge>
    </div>
  );
}
