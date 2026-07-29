"use client";

import { Copy, FileText, ReceiptText, ShieldCheck, TriangleAlert } from "lucide-react";
import { toast } from "sonner";
import {
  EconomicsDetails,
  type EconomicsSnapshot,
} from "@/components/activity-job-sheet";
import { ActivityStatusBadge } from "@/components/activity-status-badge";
import {
  AdminQueryError,
  AdminQuerySkeleton,
} from "@/components/admin-query-state";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import { useAdminQuery } from "@/hooks/use-admin-query";
import {
  formatDateTime,
  formatDurationMs,
  formatInteger,
  formatOperation,
  formatStatus,
} from "@/lib/admin/format";
import type { RequestLogItem } from "@/lib/admin/types";

export function ActivityRequestSheet({
  item,
  economicsPath,
  onOpenChange,
}: {
  item: RequestLogItem | null;
  economicsPath: string | null;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <Sheet open={item !== null} onOpenChange={onOpenChange}>
      <SheetContent className="flex w-full min-w-0 flex-col gap-0 overflow-hidden p-0 sm:max-w-2xl lg:max-w-3xl">
        {item ? (
          <RequestDetails item={item} economicsPath={economicsPath} />
        ) : null}
      </SheetContent>
    </Sheet>
  );
}

function RequestDetails({
  item,
  economicsPath,
}: {
  item: RequestLogItem;
  economicsPath: string | null;
}) {
  const economics = useAdminQuery<EconomicsSnapshot>(
    economicsPath ?? "",
    Boolean(economicsPath),
  );
  const state = requestState(item);

  return (
    <>
      <SheetHeader className="border-b px-5 py-5 pr-12 text-left sm:px-6">
        <div className="flex flex-wrap items-center gap-2">
          <ActivityStatusBadge state={state} />
          <Badge variant="outline">{sourceLabel(item.source)}</Badge>
          <Badge variant="secondary">{item.status_code}</Badge>
        </div>
        <SheetTitle className="pt-1 text-xl">请求详情</SheetTitle>
        <SheetDescription className="break-all font-mono text-xs">
          {item.request_id}
        </SheetDescription>
      </SheetHeader>

      <Tabs defaultValue="request" className="flex min-h-0 flex-1 flex-col">
        <div className="shrink-0 overflow-x-auto border-b px-5 sm:px-6">
          <TabsList variant="line">
            <TabsTrigger value="request" variant="line">
              <FileText className="size-4" aria-hidden="true" />
              请求
            </TabsTrigger>
            <TabsTrigger value="economics" variant="line" disabled={!item.job_id}>
              <ReceiptText className="size-4" aria-hidden="true" />
              计费
            </TabsTrigger>
          </TabsList>
        </div>

        <TabsContent
          value="request"
          className="m-0 min-h-0 flex-1 overflow-y-auto px-5 py-6 sm:px-6"
        >
          {item.error_code ? (
            <div className="mb-7 flex gap-3 border border-destructive/30 bg-destructive/5 p-4 text-sm">
              <TriangleAlert
                className="mt-0.5 size-4 shrink-0 text-destructive"
                aria-hidden="true"
              />
              <div className="min-w-0">
                <p className="font-medium text-destructive">请求未成功完成</p>
                <p className="mt-1 break-all font-mono text-xs text-muted-foreground">
                  {item.error_code}
                </p>
              </div>
            </div>
          ) : null}

          <div className="space-y-8">
            <DetailSection title="概览">
              <div className="grid gap-x-8 gap-y-5 sm:grid-cols-2">
                <DetailItem label="状态">
                  <ActivityStatusBadge state={state} />
                </DetailItem>
                <DetailItem label="耗时">
                  {formatDurationMs(item.duration_ms)}
                </DetailItem>
                <DetailItem label="API">
                  <span className="font-mono text-xs">
                    {item.method} {item.route_pattern}
                  </span>
                </DetailItem>
                <DetailItem label="HTTP 状态">{item.status_code}</DetailItem>
                <DetailItem label="开始时间">
                  {formatDateTime(item.created_at_ms)}
                </DetailItem>
                <DetailItem label="完成时间">
                  {formatDateTime(item.completed_at_ms)}
                </DetailItem>
              </div>
            </DetailSection>

            <DetailSection title="标识">
              <DetailRows>
                <CopyRow label="Request ID" value={item.request_id} />
                <CopyRow label="Job ID" value={item.job_id} />
                <CopyRow
                  label="Idempotency 指纹"
                  value={item.idempotency_key_digest}
                />
                <DetailRow label="请求路径">
                  <span className="break-all font-mono text-xs">
                    {item.request_path}
                  </span>
                </DetailRow>
              </DetailRows>
            </DetailSection>

            <DetailSection title="调用归属">
              <DetailRows>
                <DetailRow label="Project ID">
                  {item.project_id ?? "未关联项目"}
                </DetailRow>
                <DetailRow label="API Key ID">
                  {item.api_key_id ?? "未使用 API Key"}
                </DetailRow>
                <DetailRow label="Service Account">
                  {item.service_account_id ?? "未使用服务账户"}
                </DetailRow>
                <DetailRow label="用户">
                  {item.actor_user_id ?? "非用户会话"}
                </DetailRow>
                <DetailRow label="认证方式">
                  {item.auth_kind ? formatStatus(item.auth_kind) : "未识别"}
                </DetailRow>
              </DetailRows>
            </DetailSection>

            {item.job_id ? (
              <DetailSection title="执行">
                <DetailRows>
                  <DetailRow label="操作">
                    {item.operation
                      ? formatOperation(item.operation)
                      : "未记录"}
                  </DetailRow>
                  <DetailRow label="Provider">
                    {item.provider_id ?? "尚未选择"}
                  </DetailRow>
                  <DetailRow label="模型">
                    {item.model ? (
                      <span className="break-all font-mono text-xs">
                        {item.model}
                      </span>
                    ) : (
                      "尚未选择"
                    )}
                  </DetailRow>
                  <DetailRow label="服务层级">
                    {item.effective_service_tier ? (
                      <span>
                        {serviceTierLabel(item.effective_service_tier)}
                        {item.requested_service_tier ? (
                          <span className="ml-2 text-muted-foreground">
                            请求 {serviceTierLabel(item.requested_service_tier)}
                          </span>
                        ) : null}
                      </span>
                    ) : (
                      "未记录"
                    )}
                  </DetailRow>
                  {item.service_tier_fallback_reason ? (
                    <DetailRow label="层级回退">
                      当前模型不支持项目选择的
                      {" "}
                      {serviceTierLabel(item.project_service_tier ?? "default")}
                      ，已按 Default 执行和计费
                    </DetailRow>
                  ) : null}
                  <DetailRow label="任务状态">
                    {item.job_state
                      ? formatStatus(item.job_state)
                      : "未创建任务"}
                  </DetailRow>
                  <DetailRow label="队列状态">
                    {item.work_state
                      ? formatStatus(item.work_state)
                      : "未创建工作项"}
                  </DetailRow>
                  <DetailRow label="输出 / 计量">
                    {item.output_count !== null
                      ? `${formatInteger(item.output_count)} / ${formatInteger(
                          item.billable_units ?? "0",
                        )} ${item.billing_unit ?? ""}`
                      : "尚无终态计量"}
                  </DetailRow>
                </DetailRows>
              </DetailSection>
            ) : null}

            <DetailSection title="请求内容">
              <div className="flex gap-3 bg-muted/40 p-4 text-sm">
                <ShieldCheck
                  className="mt-0.5 size-4 shrink-0 text-muted-foreground"
                  aria-hidden="true"
                />
                <div>
                  <p className="font-medium">
                    {item.content_captured
                      ? "请求内容按保留策略受控存储"
                      : "未采集请求内容"}
                  </p>
                  <p className="mt-1 text-muted-foreground">
                    当前日志只记录路由、状态、耗时和调用归属，不保存提示词、输入图片或生成结果。
                  </p>
                </div>
              </div>
            </DetailSection>
          </div>
        </TabsContent>

        <TabsContent
          value="economics"
          className="m-0 min-h-0 flex-1 overflow-y-auto px-5 py-6 sm:px-6"
        >
          {economics.loading ? <AdminQuerySkeleton rows={7} /> : null}
          {!economics.loading && economics.error && !economics.data ? (
            <AdminQueryError error={economics.error} retry={economics.retry} />
          ) : null}
          {economics.data ? (
            <EconomicsDetails
              snapshot={economics.data}
              stale={Boolean(economics.error)}
            />
          ) : null}
        </TabsContent>
      </Tabs>
    </>
  );
}

function DetailSection({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section>
      <h3 className="mb-4 text-sm font-semibold">{title}</h3>
      {children}
    </section>
  );
}

function serviceTierLabel(value: string): string {
  const labels: Record<string, string> = {
    auto: "Auto",
    default: "Default",
    flex: "Flex",
    priority: "Priority",
    standard: "Default",
  };
  return labels[value] ?? value;
}

function DetailRows({ children }: { children: React.ReactNode }) {
  return <dl className="space-y-4">{children}</dl>;
}

function DetailRow({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="grid gap-1 text-sm sm:grid-cols-[150px_minmax(0,1fr)] sm:gap-6">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="min-w-0 sm:text-right">{children}</dd>
    </div>
  );
}

function CopyRow({
  label,
  value,
}: {
  label: string;
  value: string | null;
}) {
  return (
    <DetailRow label={label}>
      {value ? (
        <span className="inline-flex max-w-full items-center gap-1">
          <span className="truncate font-mono text-xs">{value}</span>
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="size-7 shrink-0"
            aria-label={`复制${label}`}
            title={`复制${label}`}
            onClick={() => {
              void navigator.clipboard.writeText(value);
              toast.success(`${label} 已复制`);
            }}
          >
            <Copy className="size-3.5" aria-hidden="true" />
          </Button>
        </span>
      ) : (
        "未生成"
      )}
    </DetailRow>
  );
}

function DetailItem({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <p className="text-xs text-muted-foreground">{label}</p>
      <div className="mt-1.5 text-sm font-medium">{children}</div>
    </div>
  );
}

export function requestState(item: RequestLogItem): string {
  if (
    item.status_code >= 400 ||
    item.job_state === "failed" ||
    item.job_state === "uncertain"
  ) {
    return "failed";
  }
  if (
    item.status_code === 202 ||
    item.job_state === "queued" ||
    item.job_state === "running"
  ) {
    return "running";
  }
  return "succeeded";
}

export function sourceLabel(source: RequestLogItem["source"]): string {
  const labels: Record<RequestLogItem["source"], string> = {
    models: "模型",
    images: "图片",
    videos: "视频",
    files: "文件",
    batches: "批处理",
  };
  return labels[source];
}
