"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  ArrowRight,
  CheckCircle2,
  CircleAlert,
  Clock3,
  Gauge,
  PanelRightOpen,
  PlayCircle,
  RefreshCw,
  Search,
} from "lucide-react";
import {
  AdminQueryError,
  AdminQuerySkeleton,
} from "@/components/admin-query-state";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useAdminQuery } from "@/hooks/use-admin-query";
import { useIsMobile } from "@/hooks/use-mobile";
import { formatDateTime, formatInteger, sumIntegers } from "@/lib/admin/format";
import type {
  BlockedTerminalReduction,
  SchedulerActiveJob,
  SchedulerCapacity,
  SchedulerSnapshot,
} from "@/lib/admin/types";

const ENDPOINT = "/admin/v1/scheduler/queues?window=24h";
const REFRESH_INTERVAL_MS = 15_000;

export function AdminScheduler() {
  const query = useAdminQuery<SchedulerSnapshot>(ENDPOINT);
  const activePolling = Boolean(
    query.data &&
      (query.data.active_jobs.length > 0 ||
        query.data.blocked_terminal_reductions !== "0" ||
        query.data.expired_leases !== "0"),
  );

  useEffect(() => {
    const interval = window.setInterval(() => {
      if (document.visibilityState === "visible") query.retry();
    }, activePolling ? 3_000 : REFRESH_INTERVAL_MS);
    return () => window.clearInterval(interval);
  }, [activePolling, query.retry]);

  return (
    <div className="min-w-0 space-y-6">
      <PageHeader
        title="任务队列"
        description="实时队列、执行状态与 CLI 账户容量"
        actions={
          <>
            <Button asChild variant="outline" size="sm">
              <Link href="/activity">
                调用记录
                <ArrowRight aria-hidden="true" />
              </Link>
            </Button>
            <Button
              type="button"
              variant="outline"
              size="icon"
              aria-label="刷新任务队列"
              title="刷新"
              disabled={query.refreshing}
              onClick={query.retry}
            >
              <RefreshCw
                className={query.refreshing ? "animate-spin" : ""}
                aria-hidden="true"
              />
            </Button>
          </>
        }
      />

      {query.loading ? <AdminQuerySkeleton rows={7} /> : null}
      {!query.loading &&
      query.error &&
      (!query.data || query.error.status === 403) ? (
        <AdminQueryError error={query.error} retry={query.retry} />
      ) : null}
      {query.data && (!query.error || query.error.status !== 403) ? (
        <SchedulerContent
          data={query.data}
          refreshing={query.refreshing}
          stale={Boolean(query.error)}
          retry={query.retry}
        />
      ) : null}
    </div>
  );
}

function SchedulerContent({
  data,
  refreshing,
  stale,
  retry,
}: {
  data: SchedulerSnapshot;
  refreshing: boolean;
  stale: boolean;
  retry: () => void;
}) {
  const [selectedBlocked, setSelectedBlocked] =
    useState<BlockedTerminalReduction | null>(null);
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const queued = workCount(data, ["ready"]);
  const running = workCount(data, ["leased", "running", "awaiting_executor"]);
  const maxCapacity = sumIntegers(
    data.capacity.map((item) => item.max_concurrency),
  );
  const allocatedCapacity = sumIntegers(
    data.capacity.map((item) => item.allocated_count),
  );
  const availableCapacity = sumIntegers(
    data.capacity.map((item) => item.available_capacity),
  );
  const uncertain = sumIntegers(
    data.recent_uncertain.map((item) => item.count),
  );
  const attention = sumIntegers([
    data.expired_leases,
    data.pending_terminal_reductions,
    data.blocked_terminal_reductions,
    data.artifact_retention_failures,
    uncertain,
  ]);
  const selectedJob =
    data.active_jobs.find((item) => item.job_id === selectedJobId) ?? null;

  useEffect(() => {
    if (selectedJobId && !selectedJob) setSelectedJobId(null);
  }, [selectedJob, selectedJobId]);

  return (
    <>
      {stale ? (
        <div className="flex min-h-10 flex-wrap items-center justify-between gap-2 border px-3 py-2 text-sm">
          <span className="text-muted-foreground">当前显示上一次成功快照</span>
          <Button type="button" variant="outline" size="sm" onClick={retry}>
            <RefreshCw aria-hidden="true" />
            重试
          </Button>
        </div>
      ) : null}

      <section className="grid grid-cols-2 gap-px overflow-hidden rounded-lg border bg-border xl:grid-cols-4">
        <SummaryMetric
          label="等待执行"
          value={queued}
          detail={
            isPositive(queued) ? "任务正在等待可用账户" : "当前没有排队任务"
          }
          icon={Clock3}
        />
        <SummaryMetric
          label="执行中"
          value={running}
          detail={
            isPositive(running) ? "已分配给 CLI 账户" : "当前没有执行中任务"
          }
          icon={PlayCircle}
        />
        <SummaryMetric
          label="可用并发"
          value={availableCapacity}
          detail={`总容量 ${formatInteger(maxCapacity)} · 已占用 ${formatInteger(
            allocatedCapacity,
          )}`}
          icon={Gauge}
        />
        <SummaryMetric
          label="需要关注"
          value={attention}
          detail={
            isPositive(attention) ? "存在需要处理的运行状态" : "调度运行正常"
          }
          icon={CircleAlert}
          attention={isPositive(attention)}
        />
      </section>

      <Tabs defaultValue="tasks" className="min-w-0">
        <div className="overflow-x-auto border-b">
          <TabsList className="h-11 min-w-max rounded-none bg-transparent p-0">
            <TabsTrigger
              value="tasks"
              className="h-11 rounded-none border-b-2 border-transparent bg-transparent px-4 shadow-none data-[state=active]:border-foreground data-[state=active]:bg-transparent"
            >
              实时任务
              {data.active_jobs.length > 0 ? (
                <Badge variant="secondary" className="ml-2 tabular-nums">
                  {formatInteger(data.active_jobs.length.toString())}
                </Badge>
              ) : null}
            </TabsTrigger>
            <TabsTrigger
              value="attention"
              className="h-11 rounded-none border-b-2 border-transparent bg-transparent px-4 shadow-none data-[state=active]:border-foreground data-[state=active]:bg-transparent"
            >
              异常
              {isPositive(attention) ? (
                <Badge variant="destructive" className="ml-2 tabular-nums">
                  {formatInteger(attention)}
                </Badge>
              ) : null}
            </TabsTrigger>
            <TabsTrigger
              value="capacity"
              className="h-11 rounded-none border-b-2 border-transparent bg-transparent px-4 shadow-none data-[state=active]:border-foreground data-[state=active]:bg-transparent"
            >
              账户容量
            </TabsTrigger>
          </TabsList>
        </div>
        <TabsContent value="tasks" className="mt-4">
          <ActiveJobsPanel
            asOfMs={data.as_of_ms}
            jobs={data.active_jobs}
            selected={selectedJob}
            onSelect={setSelectedJobId}
          />
        </TabsContent>
        <TabsContent value="attention" className="mt-4 space-y-4">
          <AttentionPanel data={data} uncertain={uncertain} />
          <BlockedTerminalPanel
            count={data.blocked_terminal_reductions}
            items={data.blocked_terminals}
            onSelect={setSelectedBlocked}
          />
        </TabsContent>
        <TabsContent value="capacity" className="mt-4">
          <CapacityPanel
            capacity={data.capacity}
            allocated={allocatedCapacity}
            maximum={maxCapacity}
          />
        </TabsContent>
      </Tabs>

      <p
        className="text-right text-xs text-muted-foreground"
        role="status"
        aria-live="polite"
      >
        {refreshing ? "正在更新" : `更新于 ${formatDateTime(data.as_of_ms)}`}
      </p>

      <BlockedTerminalSheet
        item={selectedBlocked}
        onOpenChange={(open) => {
          if (!open) setSelectedBlocked(null);
        }}
      />
    </>
  );
}

function SummaryMetric({
  label,
  value,
  detail,
  icon: Icon,
  attention = false,
}: {
  label: string;
  value: string;
  detail: string;
  icon: typeof Clock3;
  attention?: boolean;
}) {
  return (
    <div className="min-h-28 bg-background p-4">
      <div className="flex items-center justify-between gap-3">
        <p className="text-sm text-muted-foreground">{label}</p>
        <Icon
          className={`size-4 ${
            attention ? "text-destructive" : "text-muted-foreground"
          }`}
          aria-hidden="true"
        />
      </div>
      <p className="mt-2 text-2xl font-semibold tabular-nums">
        {formatInteger(value)}
      </p>
      <p className="mt-3 text-xs text-muted-foreground">{detail}</p>
    </div>
  );
}

function ActiveJobsPanel({
  asOfMs,
  jobs,
  selected,
  onSelect,
}: {
  asOfMs: number;
  jobs: SchedulerActiveJob[];
  selected: SchedulerActiveJob | null;
  onSelect: (jobId: string | null) => void;
}) {
  const [query, setQuery] = useState("");
  const [state, setState] = useState("all");
  const isMobile = useIsMobile();
  const normalizedQuery = query.trim().toLowerCase();
  const filtered = useMemo(
    () =>
      jobs.filter((job) => {
        if (state !== "all" && activeJobStage(job, asOfMs) !== state) {
          return false;
        }
        if (!normalizedQuery) return true;
        return [
          job.request_id,
          job.job_id,
          job.organization_name ?? job.organization_id ?? "",
          job.project_name ?? job.project_id ?? "",
          job.user_display_name ?? "",
          job.user_email ?? "",
          job.service_account_name ?? "",
          job.api_key_name ?? "",
          job.project_id ?? "",
          job.model,
          providerLabel(job.provider_id),
        ].some((value) => value.toLowerCase().includes(normalizedQuery));
      }),
    [asOfMs, jobs, normalizedQuery, state],
  );

  useEffect(() => {
    if (
      selected &&
      !filtered.some((candidate) => candidate.job_id === selected.job_id)
    ) {
      onSelect(null);
    }
  }, [filtered, onSelect, selected]);

  return (
    <>
      <section className="grid min-h-[30rem] min-w-0 overflow-hidden rounded-lg border lg:grid-cols-[minmax(20rem,0.9fr)_minmax(24rem,1.1fr)]">
        <div className="min-w-0 border-b lg:border-b-0 lg:border-r">
          <div className="space-y-3 border-b p-3">
            <div className="relative">
              <Search
                className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
                aria-hidden="true"
              />
              <Input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="搜索 Request ID、Job ID、项目或模型"
                aria-label="搜索实时任务"
                className="pl-9"
              />
            </div>
            <div
              className="grid h-9 grid-cols-4 rounded-md bg-muted p-1"
              role="group"
              aria-label="任务状态筛选"
            >
              {[
                ["all", "全部"],
                ["queued", "等待"],
                ["running", "执行中"],
                ["delayed", "延后"],
              ].map(([value, label]) => (
                <Button
                  key={value}
                  type="button"
                  variant={state === value ? "secondary" : "ghost"}
                  size="sm"
                  className="h-7 px-2"
                  aria-pressed={state === value}
                  onClick={() => setState(value)}
                >
                  {label}
                </Button>
              ))}
            </div>
          </div>
          {filtered.length === 0 ? (
            <EmptyState
              icon={CheckCircle2}
              label={
                jobs.length === 0
                  ? "当前没有排队或执行中的任务"
                  : "没有符合筛选条件的任务"
              }
            />
          ) : (
            <div className="max-h-[32rem] overflow-y-auto">
              {filtered.map((job) => {
                const stage = activeJobStage(job, asOfMs);
                const active = selected?.job_id === job.job_id;
                return (
                  <button
                    key={job.job_id}
                    type="button"
                    className={`flex w-full min-w-0 items-start justify-between gap-3 border-b px-4 py-3 text-left transition-colors last:border-b-0 hover:bg-muted/50 ${
                      active ? "bg-muted" : ""
                    }`}
                    aria-pressed={active}
                    onClick={() => onSelect(job.job_id)}
                  >
                    <span className="min-w-0">
                      <span className="block truncate text-sm font-medium">
                        {job.request_id}
                      </span>
                      <span className="mt-1 block truncate text-xs text-muted-foreground">
                        {providerLabel(job.provider_id)} · {job.model}
                      </span>
                      <span className="mt-1 block truncate text-xs text-muted-foreground">
                        {jobContextLabel(job)}
                      </span>
                      <span className="mt-1 block text-xs text-muted-foreground">
                        {formatDateTime(job.created_at_ms)}
                      </span>
                    </span>
                    <ActiveJobBadge stage={stage} />
                  </button>
                );
              })}
            </div>
          )}
        </div>
        <div className="hidden min-w-0 lg:block">
          {selected ? (
            <ActiveJobDetail asOfMs={asOfMs} job={selected} />
          ) : (
            <div className="flex min-h-[24rem] flex-col items-center justify-center gap-2 px-6 text-center text-sm text-muted-foreground">
              <PanelRightOpen className="size-5" aria-hidden="true" />
              <p className="font-medium text-foreground">选择任务查看详情</p>
              <p>查看当前阶段、执行账户和重试时间。</p>
            </div>
          )}
        </div>
      </section>
      <Sheet
        open={isMobile && selected !== null}
        onOpenChange={(open) => {
          if (!open) onSelect(null);
        }}
      >
        <SheetContent className="w-full min-w-0 overflow-y-auto p-0 sm:max-w-lg lg:hidden">
          <SheetHeader className="sr-only">
            <SheetTitle>任务详情</SheetTitle>
            <SheetDescription>
              查看当前任务阶段、执行账户和重试时间。
            </SheetDescription>
          </SheetHeader>
          {selected ? <ActiveJobDetail asOfMs={asOfMs} job={selected} /> : null}
        </SheetContent>
      </Sheet>
    </>
  );
}

function ActiveJobDetail({
  asOfMs,
  job,
}: {
  asOfMs: number;
  job: SchedulerActiveJob;
}) {
  const stage = activeJobStage(job, asOfMs);
  return (
    <div className="min-w-0">
      <div className="flex min-h-16 items-start justify-between gap-4 border-b px-5 py-4">
        <div className="min-w-0">
          <p className="truncate font-medium">{job.request_id}</p>
          <p className="mt-1 truncate font-mono text-xs text-muted-foreground">
            {job.job_id}
          </p>
        </div>
        <ActiveJobBadge stage={stage} />
      </div>
      <dl className="grid grid-cols-[8rem_minmax(0,1fr)] gap-x-5 gap-y-4 px-5 py-5 text-sm">
        <DetailTerm label="工作区">
          {job.organization_name ?? job.organization_id ?? "未归属"}
        </DetailTerm>
        <DetailTerm label="项目">
          <div>
            <p>{job.project_name ?? "未命名项目"}</p>
            <code className="break-all text-xs text-muted-foreground">
              {job.project_id ?? "未归属"}
            </code>
          </div>
        </DetailTerm>
        <DetailTerm label="发起用户">
          {job.user_display_name ?? job.user_email ?? "服务账户"}
        </DetailTerm>
        <DetailTerm label="Service Account">
          {job.service_account_name ?? "未使用"}
        </DetailTerm>
        <DetailTerm label="API Key">{job.api_key_name ?? "控制台会话"}</DetailTerm>
        <DetailTerm label="类型">
          {operationLabel(job.operation)}
        </DetailTerm>
        <DetailTerm label="Provider">
          {providerLabel(job.provider_id)}
        </DetailTerm>
        <DetailTerm label="模型">
          <code className="break-all text-xs">{job.model}</code>
        </DetailTerm>
        <DetailTerm label="执行账户">
          {job.provider_account_name ?? "尚未分配"}
        </DetailTerm>
        <DetailTerm label="尝试次数">
          {formatInteger(job.attempt_count)}
        </DetailTerm>
        <DetailTerm label="创建时间">
          {formatDateTime(job.created_at_ms)}
        </DetailTerm>
        <DetailTerm label="开始时间">
          {job.started_at_ms ? formatDateTime(job.started_at_ms) : "尚未开始"}
        </DetailTerm>
        {job.available_at_ms && job.available_at_ms > asOfMs ? (
          <DetailTerm label="下次执行">
            {formatDateTime(job.available_at_ms)}
          </DetailTerm>
        ) : null}
        {job.lease_expires_at_ms ? (
          <DetailTerm label="租约到期">
            {formatDateTime(job.lease_expires_at_ms)}
          </DetailTerm>
        ) : null}
      </dl>
      <div className="border-t px-5 py-4">
        <Button asChild variant="outline" size="sm">
          <Link href={`/activity?q=${encodeURIComponent(job.request_id)}`}>
            查看调用详情
            <ArrowRight aria-hidden="true" />
          </Link>
        </Button>
      </div>
    </div>
  );
}

function ActiveJobBadge({ stage }: { stage: string }) {
  return (
    <Badge
      variant={stage === "running" ? "default" : "outline"}
      className="shrink-0"
    >
      {activeJobStageLabel(stage)}
    </Badge>
  );
}

function AttentionPanel({
  data,
  uncertain,
}: {
  data: SchedulerSnapshot;
  uncertain: string;
}) {
  const alerts = [
    {
      label: "执行任务超时",
      detail: "执行心跳已超过租约期限",
      count: data.expired_leases,
    },
    {
      label: "结果等待归并",
      detail: "上游结果尚未写入最终状态",
      count: data.pending_terminal_reductions,
    },
    {
      label: "结果归并已阻断",
      detail: "检测到永久冲突或完整性错误，需要人工处理",
      count: data.blocked_terminal_reductions,
    },
    {
      label: "任务状态待确认",
      detail: "最近 24 小时存在无法确认的终态",
      count: uncertain,
    },
    {
      label: "输出文件清理失败",
      detail: "系统将在退避后自动重试",
      count: data.artifact_retention_failures,
    },
  ].filter((item) => isPositive(item.count));

  return (
    <section className="min-w-0 overflow-hidden rounded-lg border">
      <div className="flex min-h-14 items-center border-b px-4">
        <div>
          <h3 className="text-sm font-medium">运行提醒</h3>
          <p className="mt-0.5 text-xs text-muted-foreground">
            需要关注的调度状态
          </p>
        </div>
      </div>
      {alerts.length === 0 ? (
        <EmptyState icon={CheckCircle2} label="当前运行正常" />
      ) : (
        <div className="divide-y">
          {alerts.map((item) => (
            <div
              key={item.label}
              className="flex items-center justify-between gap-4 px-4 py-3"
            >
              <div className="min-w-0">
                <p className="text-sm font-medium">{item.label}</p>
                <p className="mt-0.5 text-xs text-muted-foreground">
                  {item.detail}
                </p>
              </div>
              <Badge variant="destructive" className="tabular-nums">
                {formatInteger(item.count)}
              </Badge>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

function BlockedTerminalPanel({
  count,
  items,
  onSelect,
}: {
  count: string;
  items: BlockedTerminalReduction[];
  onSelect: (item: BlockedTerminalReduction) => void;
}) {
  return (
    <section className="min-w-0 overflow-hidden rounded-lg border">
      <div className="flex min-h-14 flex-wrap items-center justify-between gap-3 border-b px-4 py-3">
        <div>
          <h3 className="text-sm font-medium">归并阻断</h3>
          <p className="mt-0.5 text-xs text-muted-foreground">
            共 {formatInteger(count)} 项，按最近阻断时间显示
          </p>
        </div>
        {isPositive(count) ? (
          <Badge variant="destructive">{formatInteger(count)} 项待处理</Badge>
        ) : null}
      </div>
      {items.length === 0 ? (
        <EmptyState icon={CheckCircle2} label="当前没有被阻断的结果归并" />
      ) : (
        <Table className="min-w-[880px]">
          <TableHeader>
            <TableRow>
              <TableHead className="pl-4">任务</TableHead>
              <TableHead>Provider / 模型</TableHead>
              <TableHead>错误码</TableHead>
              <TableHead>阻断时间</TableHead>
              <TableHead className="pr-4 text-right">
                <span className="sr-only">操作</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {items.map((item) => (
              <TableRow key={item.submission_id}>
                <TableCell className="max-w-72 pl-4">
                  <p className="truncate font-medium">{item.request_id}</p>
                  <p className="mt-0.5 truncate font-mono text-xs text-muted-foreground">
                    {item.job_id}
                  </p>
                </TableCell>
                <TableCell>
                  <p className="font-medium">
                    {providerLabel(item.provider_id)}
                  </p>
                  <p className="mt-0.5 text-xs text-muted-foreground">
                    {item.model}
                  </p>
                </TableCell>
                <TableCell>
                  <Badge variant="outline" className="font-mono font-normal">
                    {item.error_code}
                  </Badge>
                  <p className="mt-1 text-xs text-muted-foreground">
                    {blockedErrorLabel(item.error_code)}
                  </p>
                </TableCell>
                <TableCell className="whitespace-nowrap text-muted-foreground">
                  {formatDateTime(item.blocked_at_ms)}
                </TableCell>
                <TableCell className="pr-4 text-right">
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon"
                    aria-label={`查看任务 ${item.request_id} 的阻断详情`}
                    title="查看详情"
                    onClick={() => onSelect(item)}
                  >
                    <PanelRightOpen aria-hidden="true" />
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </section>
  );
}

function BlockedTerminalSheet({
  item,
  onOpenChange,
}: {
  item: BlockedTerminalReduction | null;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <Sheet open={item !== null} onOpenChange={onOpenChange}>
      <SheetContent className="flex w-full min-w-0 flex-col gap-0 overflow-hidden p-0 sm:max-w-xl">
        {item ? (
          <>
            <SheetHeader className="border-b px-5 py-5 pr-12 text-left sm:px-6">
              <SheetTitle>归并阻断详情</SheetTitle>
              <SheetDescription>
                {item.request_id} · {providerLabel(item.provider_id)}
              </SheetDescription>
            </SheetHeader>
            <div className="min-h-0 flex-1 overflow-y-auto px-5 py-6 sm:px-6">
              <dl className="grid grid-cols-[7rem_minmax(0,1fr)] gap-x-5 gap-y-5 text-sm">
                <DetailTerm label="错误码">
                  <Badge variant="destructive" className="font-mono font-normal">
                    {item.error_code}
                  </Badge>
                  <p className="mt-1.5 text-xs text-muted-foreground">
                    {blockedErrorLabel(item.error_code)}
                  </p>
                </DetailTerm>
                <DetailTerm label="阻断时间">
                  {formatDateTime(item.blocked_at_ms)}
                </DetailTerm>
                <DetailTerm label="处理进程">
                  <code className="break-all text-xs">{item.blocked_by}</code>
                </DetailTerm>
                <DetailTerm label="归并终态">
                  {resolvedStateLabel(item.resolved_state)}
                </DetailTerm>
                <DetailTerm label="Provider">
                  {providerLabel(item.provider_id)}
                </DetailTerm>
                <DetailTerm label="模型">
                  <code className="break-all text-xs">{item.model}</code>
                </DetailTerm>
                <DetailTerm label="Job ID">
                  <code className="break-all text-xs">{item.job_id}</code>
                </DetailTerm>
                <DetailTerm label="Submission ID">
                  <code className="break-all text-xs">{item.submission_id}</code>
                </DetailTerm>
                <DetailTerm label="Execution ID">
                  <code className="break-all text-xs">
                    {item.executor_execution_id}
                  </code>
                </DetailTerm>
              </dl>
            </div>
          </>
        ) : null}
      </SheetContent>
    </Sheet>
  );
}

function DetailTerm({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="min-w-0">{children}</dd>
    </>
  );
}

function CapacityPanel({
  capacity,
  allocated,
  maximum,
}: {
  capacity: SchedulerCapacity[];
  allocated: string;
  maximum: string;
}) {
  return (
    <section className="min-w-0 overflow-hidden rounded-lg border">
      <div className="flex min-h-14 flex-wrap items-center justify-between gap-3 border-b px-4 py-3">
        <div>
          <h3 className="text-sm font-medium">CLI 账户容量</h3>
          <p className="mt-0.5 text-xs text-muted-foreground">
            已占用 {formatInteger(allocated)} / {formatInteger(maximum)}
          </p>
        </div>
        <Button asChild variant="ghost" size="sm">
          <Link href="/provider-accounts">
            管理账户
            <ArrowRight aria-hidden="true" />
          </Link>
        </Button>
      </div>
      {capacity.length === 0 ? (
        <EmptyState icon={Gauge} label="暂无可调度的 CLI 账户" />
      ) : (
        <Table className="min-w-[680px]">
          <TableHeader>
            <TableRow>
              <TableHead className="pl-4">账户</TableHead>
              <TableHead>并发使用</TableHead>
              <TableHead className="text-right">可用</TableHead>
              <TableHead className="pr-4 text-right">状态</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {capacity.map((item) => (
              <CapacityRow key={item.provider_account_id} item={item} />
            ))}
          </TableBody>
        </Table>
      )}
    </section>
  );
}

function CapacityRow({ item }: { item: SchedulerCapacity }) {
  const maximum = Number(item.max_concurrency);
  const allocated = Number(item.allocated_count);
  const available = Number(item.available_capacity);
  const usage = maximum > 0 ? Math.min(100, (allocated / maximum) * 100) : 0;

  return (
    <TableRow>
      <TableCell className="pl-4">
        <p className="max-w-72 truncate font-medium">
          {item.display_name ?? item.account_key}
        </p>
        <p className="mt-0.5 text-xs text-muted-foreground">
          {providerLabel(item.provider_id)}
          {item.account_email ? ` · ${item.account_email}` : ""}
        </p>
      </TableCell>
      <TableCell>
        <div className="flex min-w-52 items-center gap-3">
          <Progress
            value={usage}
            className="h-1.5"
            aria-label={`${item.account_key} 已占用 ${item.allocated_count}，最大并发 ${item.max_concurrency}`}
          />
          <span className="w-16 shrink-0 text-right font-mono text-xs tabular-nums">
            {formatInteger(item.allocated_count)} /{" "}
            {formatInteger(item.max_concurrency)}
          </span>
        </div>
      </TableCell>
      <TableCell className="text-right font-mono tabular-nums">
        {formatInteger(item.available_capacity)}
      </TableCell>
      <TableCell className="pr-4 text-right">
        <Badge variant={available > 0 ? "outline" : "secondary"}>
          {available > 0 ? "可接收任务" : "容量已满"}
        </Badge>
      </TableCell>
    </TableRow>
  );
}

function EmptyState({
  icon: Icon,
  label,
}: {
  icon: typeof Gauge;
  label: string;
}) {
  return (
    <div className="flex min-h-40 flex-col items-center justify-center gap-2 px-4 text-sm text-muted-foreground">
      <Icon className="size-5" aria-hidden="true" />
      {label}
    </div>
  );
}

function workCount(
  data: SchedulerSnapshot,
  states: string[],
  readyTiming?: string,
): string {
  return sumIntegers(
    data.work_items
      .filter(
        (item) =>
          states.includes(item.state) &&
          (readyTiming === undefined || item.ready_timing === readyTiming),
      )
      .map((item) => item.count),
  );
}

function isPositive(value: string): boolean {
  try {
    return BigInt(value) > 0n;
  } catch {
    return false;
  }
}

function providerLabel(providerId: string): string {
  if (providerId.includes("codex")) return "Codex";
  if (providerId.includes("grok")) return "Grok";
  if (providerId.includes("dreamina")) return "即梦";
  return providerId;
}

function jobContextLabel(job: SchedulerActiveJob): string {
  const project = job.project_name ?? job.project_id ?? "未归属项目";
  const actor =
    job.user_display_name ??
    job.user_email ??
    job.service_account_name ??
    "系统任务";
  return `${project} · ${actor}`;
}

function activeJobStage(job: SchedulerActiveJob, asOfMs: number): string {
  if (
    job.job_state === "queued" &&
    job.available_at_ms !== null &&
    job.available_at_ms > asOfMs
  ) {
    return "delayed";
  }
  if (
    job.job_state === "running" ||
    ["leased", "running", "awaiting_executor"].includes(job.work_state ?? "")
  ) {
    return "running";
  }
  return "queued";
}

function activeJobStageLabel(stage: string): string {
  switch (stage) {
    case "running":
      return "执行中";
    case "delayed":
      return "延后";
    default:
      return "等待";
  }
}

function operationLabel(operation: string): string {
  switch (operation) {
    case "generation":
      return "图片生成";
    case "edit":
      return "图片编辑";
    case "video_generation":
      return "视频生成";
    default:
      return operation;
  }
}

function blockedErrorLabel(errorCode: string): string {
  switch (errorCode) {
    case "canonical_conflict":
      return "权威结果或结算事实发生冲突";
    case "invalid_input":
      return "归并输入不满足稳定契约";
    case "artifact_integrity":
      return "输出文件完整性校验失败";
    default:
      return "未知阻断原因";
  }
}

function resolvedStateLabel(state: string): string {
  switch (state) {
    case "succeeded":
      return "执行成功";
    case "failed":
      return "执行失败";
    case "uncertain":
      return "终态待确认";
    case "canceled":
      return "已取消";
    default:
      return state;
  }
}
