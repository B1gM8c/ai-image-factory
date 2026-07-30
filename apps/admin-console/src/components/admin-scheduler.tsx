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
import { useI18n } from "@/i18n/locale-provider";
import { formatDateTime, formatInteger, sumIntegers } from "@/lib/admin/format";
import type {
  BlockedTerminalReduction,
  SchedulerActiveJob,
  SchedulerCapacity,
  SchedulerSnapshot,
} from "@/lib/admin/types";

const ENDPOINT = "/admin/v1/scheduler/queues?window=24h";
const REFRESH_INTERVAL_MS = 15_000;
type Translate = ReturnType<typeof useI18n>["t"];

export function AdminScheduler() {
  const { t } = useI18n();
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
        title={t({
          en: "Task queue",
          "zh-CN": "任务队列",
          ja: "タスクキュー",
          ko: "작업 대기열",
        })}
        description={t({
          en: "Live queues, execution status, and CLI account capacity",
          "zh-CN": "实时队列、执行状态与 CLI 账户容量",
          ja: "リアルタイムキュー、実行状態、CLI アカウント容量",
          ko: "실시간 대기열, 실행 상태 및 CLI 계정 용량",
        })}
        actions={
          <>
            <Button asChild variant="outline" size="sm">
              <Link href="/activity">
                {t({
                  en: "Activity",
                  "zh-CN": "调用记录",
                  ja: "アクティビティ",
                  ko: "호출 기록",
                })}
                <ArrowRight aria-hidden="true" />
              </Link>
            </Button>
            <Button
              type="button"
              variant="outline"
              size="icon"
              aria-label={t({
                en: "Refresh task queue",
                "zh-CN": "刷新任务队列",
                ja: "タスクキューを更新",
                ko: "작업 대기열 새로고침",
              })}
              title={t({
                en: "Refresh",
                "zh-CN": "刷新",
                ja: "更新",
                ko: "새로고침",
              })}
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
  const { t } = useI18n();
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
          <span className="text-muted-foreground">
            {t({
              en: "Showing the most recent successful snapshot",
              "zh-CN": "当前显示上一次成功快照",
              ja: "直近の成功したスナップショットを表示しています",
              ko: "마지막으로 성공한 스냅샷을 표시합니다",
            })}
          </span>
          <Button type="button" variant="outline" size="sm" onClick={retry}>
            <RefreshCw aria-hidden="true" />
            {t({
              en: "Retry",
              "zh-CN": "重试",
              ja: "再試行",
              ko: "다시 시도",
            })}
          </Button>
        </div>
      ) : null}

      <section className="grid grid-cols-2 gap-px overflow-hidden rounded-lg border bg-border xl:grid-cols-4">
        <SummaryMetric
          label={t({
            en: "Queued",
            "zh-CN": "等待执行",
            ja: "実行待ち",
            ko: "실행 대기",
          })}
          value={queued}
          detail={
            isPositive(queued)
              ? t({
                  en: "Jobs are waiting for an available account",
                  "zh-CN": "任务正在等待可用账户",
                  ja: "ジョブは利用可能なアカウントを待っています",
                  ko: "작업이 사용 가능한 계정을 기다리고 있습니다",
                })
              : t({
                  en: "No jobs are queued",
                  "zh-CN": "当前没有排队任务",
                  ja: "待機中のジョブはありません",
                  ko: "대기 중인 작업이 없습니다",
                })
          }
          icon={Clock3}
        />
        <SummaryMetric
          label={t({
            en: "Running",
            "zh-CN": "执行中",
            ja: "実行中",
            ko: "실행 중",
          })}
          value={running}
          detail={
            isPositive(running)
              ? t({
                  en: "Assigned to CLI accounts",
                  "zh-CN": "已分配给 CLI 账户",
                  ja: "CLI アカウントに割り当て済み",
                  ko: "CLI 계정에 할당됨",
                })
              : t({
                  en: "No jobs are running",
                  "zh-CN": "当前没有执行中任务",
                  ja: "実行中のジョブはありません",
                  ko: "실행 중인 작업이 없습니다",
                })
          }
          icon={PlayCircle}
        />
        <SummaryMetric
          label={t({
            en: "Available concurrency",
            "zh-CN": "可用并发",
            ja: "利用可能な同時実行数",
            ko: "사용 가능한 동시 실행",
          })}
          value={availableCapacity}
          detail={t(
            {
              en: "Total {total} · {used} in use",
              "zh-CN": "总容量 {total} · 已占用 {used}",
              ja: "合計 {total} · 使用中 {used}",
              ko: "총 {total} · 사용 중 {used}",
            },
            {
              total: formatInteger(maxCapacity),
              used: formatInteger(allocatedCapacity),
            },
          )}
          icon={Gauge}
        />
        <SummaryMetric
          label={t({
            en: "Needs attention",
            "zh-CN": "需要关注",
            ja: "要対応",
            ko: "확인 필요",
          })}
          value={attention}
          detail={
            isPositive(attention)
              ? t({
                  en: "Operational states require attention",
                  "zh-CN": "存在需要处理的运行状态",
                  ja: "対応が必要な実行状態があります",
                  ko: "확인이 필요한 운영 상태가 있습니다",
                })
              : t({
                  en: "Scheduling is healthy",
                  "zh-CN": "调度运行正常",
                  ja: "スケジューリングは正常です",
                  ko: "스케줄링이 정상입니다",
                })
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
              {t({
                en: "Live tasks",
                "zh-CN": "实时任务",
                ja: "リアルタイムタスク",
                ko: "실시간 작업",
              })}
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
              {t({
                en: "Attention",
                "zh-CN": "异常",
                ja: "異常",
                ko: "이상",
              })}
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
              {t({
                en: "Account capacity",
                "zh-CN": "账户容量",
                ja: "アカウント容量",
                ko: "계정 용량",
              })}
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
        {refreshing
          ? t({
              en: "Updating",
              "zh-CN": "正在更新",
              ja: "更新中",
              ko: "업데이트 중",
            })
          : t(
              {
                en: "Updated {time}",
                "zh-CN": "更新于 {time}",
                ja: "{time} に更新",
                ko: "{time} 업데이트",
              },
              { time: formatDateTime(data.as_of_ms) },
            )}
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
  const { t } = useI18n();
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
          providerLabel(t, job.provider_id),
        ].some((value) => value.toLowerCase().includes(normalizedQuery));
      }),
    [asOfMs, jobs, normalizedQuery, state, t],
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
                placeholder={t({
                  en: "Search Request ID, Job ID, project, or model",
                  "zh-CN": "搜索 Request ID、Job ID、项目或模型",
                  ja: "Request ID、Job ID、プロジェクト、モデルを検索",
                  ko: "Request ID, Job ID, 프로젝트 또는 모델 검색",
                })}
                aria-label={t({
                  en: "Search live tasks",
                  "zh-CN": "搜索实时任务",
                  ja: "リアルタイムタスクを検索",
                  ko: "실시간 작업 검색",
                })}
                className="pl-9"
              />
            </div>
            <div
              className="grid h-9 grid-cols-4 rounded-md bg-muted p-1"
              role="group"
              aria-label={t({
                en: "Filter by task status",
                "zh-CN": "任务状态筛选",
                ja: "タスク状態で絞り込み",
                ko: "작업 상태 필터",
              })}
            >
              {[
                [
                  "all",
                  t({
                    en: "All",
                    "zh-CN": "全部",
                    ja: "すべて",
                    ko: "전체",
                  }),
                ],
                ["queued", activeJobStageLabel(t, "queued")],
                ["running", activeJobStageLabel(t, "running")],
                ["delayed", activeJobStageLabel(t, "delayed")],
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
                  ? t({
                      en: "No queued or running tasks",
                      "zh-CN": "当前没有排队或执行中的任务",
                      ja: "待機中または実行中のタスクはありません",
                      ko: "대기 중이거나 실행 중인 작업이 없습니다",
                    })
                  : t({
                      en: "No tasks match the current filters",
                      "zh-CN": "没有符合筛选条件的任务",
                      ja: "現在のフィルターに一致するタスクはありません",
                      ko: "현재 필터와 일치하는 작업이 없습니다",
                    })
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
                        {providerLabel(t, job.provider_id)} · {job.model}
                      </span>
                      <span className="mt-1 block truncate text-xs text-muted-foreground">
                        {jobContextLabel(t, job)}
                      </span>
                      <span className="mt-1 block text-xs text-muted-foreground">
                        {formatDateTime(job.created_at_ms)}
                      </span>
                    </span>
                    <ActiveJobBadge t={t} stage={stage} />
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
              <p className="font-medium text-foreground">
                {t({
                  en: "Select a task to view details",
                  "zh-CN": "选择任务查看详情",
                  ja: "タスクを選択して詳細を表示",
                  ko: "작업을 선택해 세부 정보 보기",
                })}
              </p>
              <p>
                {t({
                  en: "Review its current stage, execution account, and retry time.",
                  "zh-CN": "查看当前阶段、执行账户和重试时间。",
                  ja: "現在のステージ、実行アカウント、再試行時刻を確認できます。",
                  ko: "현재 단계, 실행 계정 및 재시도 시간을 확인합니다.",
                })}
              </p>
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
            <SheetTitle>
              {t({
                en: "Task details",
                "zh-CN": "任务详情",
                ja: "タスク詳細",
                ko: "작업 세부 정보",
              })}
            </SheetTitle>
            <SheetDescription>
              {t({
                en: "Review the current stage, execution account, and retry time.",
                "zh-CN": "查看当前任务阶段、执行账户和重试时间。",
                ja: "現在のタスクステージ、実行アカウント、再試行時刻を確認できます。",
                ko: "현재 작업 단계, 실행 계정 및 재시도 시간을 확인합니다.",
              })}
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
  const { t } = useI18n();
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
        <ActiveJobBadge t={t} stage={stage} />
      </div>
      <dl className="grid grid-cols-[8rem_minmax(0,1fr)] gap-x-5 gap-y-4 px-5 py-5 text-sm">
        <DetailTerm
          label={t({
            en: "Workspace",
            "zh-CN": "工作区",
            ja: "ワークスペース",
            ko: "워크스페이스",
          })}
        >
          {job.organization_name ??
            job.organization_id ??
            t({
              en: "Unattributed",
              "zh-CN": "未归属",
              ja: "未帰属",
              ko: "미귀속",
            })}
        </DetailTerm>
        <DetailTerm
          label={t({
            en: "Project",
            "zh-CN": "项目",
            ja: "プロジェクト",
            ko: "프로젝트",
          })}
        >
          <div>
            <p>
              {job.project_name ??
                t({
                  en: "Untitled project",
                  "zh-CN": "未命名项目",
                  ja: "名称未設定のプロジェクト",
                  ko: "이름 없는 프로젝트",
                })}
            </p>
            <code className="break-all text-xs text-muted-foreground">
              {job.project_id ??
                t({
                  en: "Unattributed",
                  "zh-CN": "未归属",
                  ja: "未帰属",
                  ko: "미귀속",
                })}
            </code>
          </div>
        </DetailTerm>
        <DetailTerm
          label={t({
            en: "Initiated by",
            "zh-CN": "发起用户",
            ja: "実行ユーザー",
            ko: "요청 사용자",
          })}
        >
          {job.user_display_name ??
            job.user_email ??
            t({
              en: "Service account",
              "zh-CN": "服务账户",
              ja: "サービスアカウント",
              ko: "서비스 계정",
            })}
        </DetailTerm>
        <DetailTerm
          label={t({
            en: "Service Account",
            "zh-CN": "服务账户",
            ja: "サービスアカウント",
            ko: "서비스 계정",
          })}
        >
          {job.service_account_name ??
            t({
              en: "Not used",
              "zh-CN": "未使用",
              ja: "未使用",
              ko: "사용 안 함",
            })}
        </DetailTerm>
        <DetailTerm
          label={t({
            en: "API Key",
            "zh-CN": "API 密钥",
            ja: "API キー",
            ko: "API 키",
          })}
        >
          {job.api_key_name ??
            t({
              en: "Console session",
              "zh-CN": "控制台会话",
              ja: "コンソールセッション",
              ko: "콘솔 세션",
            })}
        </DetailTerm>
        <DetailTerm
          label={t({
            en: "Type",
            "zh-CN": "类型",
            ja: "種類",
            ko: "유형",
          })}
        >
          {operationLabel(t, job.operation)}
        </DetailTerm>
        <DetailTerm
          label={t({
            en: "Provider",
            "zh-CN": "供应商",
            ja: "プロバイダー",
            ko: "공급자",
          })}
        >
          {providerLabel(t, job.provider_id)}
        </DetailTerm>
        <DetailTerm
          label={t({
            en: "Model",
            "zh-CN": "模型",
            ja: "モデル",
            ko: "모델",
          })}
        >
          <code className="break-all text-xs">{job.model}</code>
        </DetailTerm>
        <DetailTerm
          label={t({
            en: "Execution account",
            "zh-CN": "执行账户",
            ja: "実行アカウント",
            ko: "실행 계정",
          })}
        >
          {job.provider_account_name ??
            t({
              en: "Not assigned",
              "zh-CN": "尚未分配",
              ja: "未割り当て",
              ko: "할당되지 않음",
            })}
        </DetailTerm>
        <DetailTerm
          label={t({
            en: "Attempts",
            "zh-CN": "尝试次数",
            ja: "試行回数",
            ko: "시도 횟수",
          })}
        >
          {formatInteger(job.attempt_count)}
        </DetailTerm>
        <DetailTerm
          label={t({
            en: "Created",
            "zh-CN": "创建时间",
            ja: "作成時刻",
            ko: "생성 시간",
          })}
        >
          {formatDateTime(job.created_at_ms)}
        </DetailTerm>
        <DetailTerm
          label={t({
            en: "Started",
            "zh-CN": "开始时间",
            ja: "開始時刻",
            ko: "시작 시간",
          })}
        >
          {job.started_at_ms
            ? formatDateTime(job.started_at_ms)
            : t({
                en: "Not started",
                "zh-CN": "尚未开始",
                ja: "未開始",
                ko: "시작 전",
              })}
        </DetailTerm>
        {job.available_at_ms && job.available_at_ms > asOfMs ? (
          <DetailTerm
            label={t({
              en: "Next run",
              "zh-CN": "下次执行",
              ja: "次回実行",
              ko: "다음 실행",
            })}
          >
            {formatDateTime(job.available_at_ms)}
          </DetailTerm>
        ) : null}
        {job.lease_expires_at_ms ? (
          <DetailTerm
            label={t({
              en: "Lease expires",
              "zh-CN": "租约到期",
              ja: "リース有効期限",
              ko: "리스 만료",
            })}
          >
            {formatDateTime(job.lease_expires_at_ms)}
          </DetailTerm>
        ) : null}
      </dl>
      <div className="border-t px-5 py-4">
        <Button asChild variant="outline" size="sm">
          <Link href={`/activity?q=${encodeURIComponent(job.request_id)}`}>
            {t({
              en: "View activity details",
              "zh-CN": "查看调用详情",
              ja: "アクティビティ詳細を表示",
              ko: "호출 세부 정보 보기",
            })}
            <ArrowRight aria-hidden="true" />
          </Link>
        </Button>
      </div>
    </div>
  );
}

function ActiveJobBadge({ t, stage }: { t: Translate; stage: string }) {
  return (
    <Badge
      variant={stage === "running" ? "default" : "outline"}
      className="shrink-0"
    >
      {activeJobStageLabel(t, stage)}
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
  const { t } = useI18n();
  const alerts = [
    {
      label: t({
        en: "Execution timed out",
        "zh-CN": "执行任务超时",
        ja: "実行タイムアウト",
        ko: "실행 시간 초과",
      }),
      detail: t({
        en: "The execution heartbeat exceeded the lease deadline",
        "zh-CN": "执行心跳已超过租约期限",
        ja: "実行ハートビートがリース期限を超えました",
        ko: "실행 하트비트가 리스 기한을 초과했습니다",
      }),
      count: data.expired_leases,
    },
    {
      label: t({
        en: "Result awaiting reduction",
        "zh-CN": "结果等待归并",
        ja: "結果の集約待ち",
        ko: "결과 병합 대기",
      }),
      detail: t({
        en: "The provider result has not been written to a terminal state",
        "zh-CN": "上游结果尚未写入最终状态",
        ja: "プロバイダー結果が終端状態に書き込まれていません",
        ko: "공급자 결과가 최종 상태에 기록되지 않았습니다",
      }),
      count: data.pending_terminal_reductions,
    },
    {
      label: t({
        en: "Result reduction blocked",
        "zh-CN": "结果归并已阻断",
        ja: "結果集約がブロックされています",
        ko: "결과 병합이 차단됨",
      }),
      detail: t({
        en: "A permanent conflict or integrity error requires manual review",
        "zh-CN": "检测到永久冲突或完整性错误，需要人工处理",
        ja: "永続的な競合または整合性エラーが検出され、手動対応が必要です",
        ko: "영구 충돌 또는 무결성 오류가 감지되어 수동 검토가 필요합니다",
      }),
      count: data.blocked_terminal_reductions,
    },
    {
      label: t({
        en: "Task state uncertain",
        "zh-CN": "任务状态待确认",
        ja: "タスク状態が未確定",
        ko: "작업 상태 확인 필요",
      }),
      detail: t({
        en: "An unconfirmed terminal state occurred in the last 24 hours",
        "zh-CN": "最近 24 小时存在无法确认的终态",
        ja: "過去 24 時間に確認できない終端状態が発生しました",
        ko: "최근 24시간 동안 확인되지 않은 최종 상태가 발생했습니다",
      }),
      count: uncertain,
    },
    {
      label: t({
        en: "Output cleanup failed",
        "zh-CN": "输出文件清理失败",
        ja: "出力ファイルのクリーンアップに失敗",
        ko: "출력 파일 정리 실패",
      }),
      detail: t({
        en: "The system retries automatically after backoff",
        "zh-CN": "系统将在退避后自动重试",
        ja: "バックオフ後に自動的に再試行します",
        ko: "백오프 후 시스템이 자동으로 다시 시도합니다",
      }),
      count: data.artifact_retention_failures,
    },
  ].filter((item) => isPositive(item.count));

  return (
    <section className="min-w-0 overflow-hidden rounded-lg border">
      <div className="flex min-h-14 items-center border-b px-4">
        <div>
          <h3 className="text-sm font-medium">
            {t({
              en: "Operational alerts",
              "zh-CN": "运行提醒",
              ja: "運用アラート",
              ko: "운영 알림",
            })}
          </h3>
          <p className="mt-0.5 text-xs text-muted-foreground">
            {t({
              en: "Scheduling states that need attention",
              "zh-CN": "需要关注的调度状态",
              ja: "対応が必要なスケジューリング状態",
              ko: "확인이 필요한 스케줄링 상태",
            })}
          </p>
        </div>
      </div>
      {alerts.length === 0 ? (
        <EmptyState
          icon={CheckCircle2}
          label={t({
            en: "Operations are healthy",
            "zh-CN": "当前运行正常",
            ja: "現在、正常に稼働しています",
            ko: "현재 정상적으로 운영 중입니다",
          })}
        />
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
  const { t } = useI18n();
  return (
    <section className="min-w-0 overflow-hidden rounded-lg border">
      <div className="flex min-h-14 flex-wrap items-center justify-between gap-3 border-b px-4 py-3">
        <div>
          <h3 className="text-sm font-medium">
            {t({
              en: "Blocked reductions",
              "zh-CN": "归并阻断",
              ja: "ブロックされた集約",
              ko: "차단된 병합",
            })}
          </h3>
          <p className="mt-0.5 text-xs text-muted-foreground">
            {t(
              {
                en: "{count} items, sorted by most recently blocked",
                "zh-CN": "共 {count} 项，按最近阻断时间显示",
                ja: "{count} 件、直近のブロック時刻順",
                ko: "{count}건, 최근 차단 시간순",
              },
              { count: formatInteger(count) },
            )}
          </p>
        </div>
        {isPositive(count) ? (
          <Badge variant="destructive">
            {t(
              {
                en: "{count} pending",
                "zh-CN": "{count} 项待处理",
                ja: "{count} 件保留中",
                ko: "{count}건 처리 대기",
              },
              { count: formatInteger(count) },
            )}
          </Badge>
        ) : null}
      </div>
      {items.length === 0 ? (
        <EmptyState
          icon={CheckCircle2}
          label={t({
            en: "No result reductions are blocked",
            "zh-CN": "当前没有被阻断的结果归并",
            ja: "ブロックされた結果集約はありません",
            ko: "차단된 결과 병합이 없습니다",
          })}
        />
      ) : (
        <Table className="min-w-[880px]">
          <TableHeader>
            <TableRow>
              <TableHead className="pl-4">
                {t({
                  en: "Task",
                  "zh-CN": "任务",
                  ja: "タスク",
                  ko: "작업",
                })}
              </TableHead>
              <TableHead>
                {t({
                  en: "Provider / Model",
                  "zh-CN": "Provider / 模型",
                  ja: "Provider / モデル",
                  ko: "Provider / 모델",
                })}
              </TableHead>
              <TableHead>
                {t({
                  en: "Error code",
                  "zh-CN": "错误码",
                  ja: "エラーコード",
                  ko: "오류 코드",
                })}
              </TableHead>
              <TableHead>
                {t({
                  en: "Blocked",
                  "zh-CN": "阻断时间",
                  ja: "ブロック時刻",
                  ko: "차단 시간",
                })}
              </TableHead>
              <TableHead className="pr-4 text-right">
                <span className="sr-only">
                  {t({
                    en: "Actions",
                    "zh-CN": "操作",
                    ja: "操作",
                    ko: "작업",
                  })}
                </span>
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
                    {providerLabel(t, item.provider_id)}
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
                    {blockedErrorLabel(t, item.error_code)}
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
                    aria-label={t(
                      {
                        en: "View blocked details for task {requestId}",
                        "zh-CN": "查看任务 {requestId} 的阻断详情",
                        ja: "タスク {requestId} のブロック詳細を表示",
                        ko: "작업 {requestId}의 차단 세부 정보 보기",
                      },
                      { requestId: item.request_id },
                    )}
                    title={t({
                      en: "View details",
                      "zh-CN": "查看详情",
                      ja: "詳細を表示",
                      ko: "세부 정보 보기",
                    })}
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
  const { t } = useI18n();
  return (
    <Sheet open={item !== null} onOpenChange={onOpenChange}>
      <SheetContent className="flex w-full min-w-0 flex-col gap-0 overflow-hidden p-0 sm:max-w-xl">
        {item ? (
          <>
            <SheetHeader className="border-b px-5 py-5 pr-12 text-left sm:px-6">
              <SheetTitle>
                {t({
                  en: "Blocked reduction details",
                  "zh-CN": "归并阻断详情",
                  ja: "ブロックされた集約の詳細",
                  ko: "차단된 병합 세부 정보",
                })}
              </SheetTitle>
              <SheetDescription>
                {item.request_id} · {providerLabel(t, item.provider_id)}
              </SheetDescription>
            </SheetHeader>
            <div className="min-h-0 flex-1 overflow-y-auto px-5 py-6 sm:px-6">
              <dl className="grid grid-cols-[7rem_minmax(0,1fr)] gap-x-5 gap-y-5 text-sm">
                <DetailTerm
                  label={t({
                    en: "Error code",
                    "zh-CN": "错误码",
                    ja: "エラーコード",
                    ko: "오류 코드",
                  })}
                >
                  <Badge variant="destructive" className="font-mono font-normal">
                    {item.error_code}
                  </Badge>
                  <p className="mt-1.5 text-xs text-muted-foreground">
                    {blockedErrorLabel(t, item.error_code)}
                  </p>
                </DetailTerm>
                <DetailTerm
                  label={t({
                    en: "Blocked",
                    "zh-CN": "阻断时间",
                    ja: "ブロック時刻",
                    ko: "차단 시간",
                  })}
                >
                  {formatDateTime(item.blocked_at_ms)}
                </DetailTerm>
                <DetailTerm
                  label={t({
                    en: "Worker",
                    "zh-CN": "处理进程",
                    ja: "処理プロセス",
                    ko: "처리 프로세스",
                  })}
                >
                  <code className="break-all text-xs">{item.blocked_by}</code>
                </DetailTerm>
                <DetailTerm
                  label={t({
                    en: "Reduced terminal state",
                    "zh-CN": "归并终态",
                    ja: "集約後の終端状態",
                    ko: "병합 최종 상태",
                  })}
                >
                  {resolvedStateLabel(t, item.resolved_state)}
                </DetailTerm>
                <DetailTerm
                  label={t({
                    en: "Provider",
                    "zh-CN": "供应商",
                    ja: "プロバイダー",
                    ko: "공급자",
                  })}
                >
                  {providerLabel(t, item.provider_id)}
                </DetailTerm>
                <DetailTerm
                  label={t({
                    en: "Model",
                    "zh-CN": "模型",
                    ja: "モデル",
                    ko: "모델",
                  })}
                >
                  <code className="break-all text-xs">{item.model}</code>
                </DetailTerm>
                <DetailTerm
                  label={t({
                    en: "Job ID",
                    "zh-CN": "任务 ID",
                    ja: "ジョブ ID",
                    ko: "작업 ID",
                  })}
                >
                  <code className="break-all text-xs">{item.job_id}</code>
                </DetailTerm>
                <DetailTerm
                  label={t({
                    en: "Submission ID",
                    "zh-CN": "提交 ID",
                    ja: "送信 ID",
                    ko: "제출 ID",
                  })}
                >
                  <code className="break-all text-xs">{item.submission_id}</code>
                </DetailTerm>
                <DetailTerm
                  label={t({
                    en: "Execution ID",
                    "zh-CN": "执行 ID",
                    ja: "実行 ID",
                    ko: "실행 ID",
                  })}
                >
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
  const { t } = useI18n();
  return (
    <section className="min-w-0 overflow-hidden rounded-lg border">
      <div className="flex min-h-14 flex-wrap items-center justify-between gap-3 border-b px-4 py-3">
        <div>
          <h3 className="text-sm font-medium">
            {t({
              en: "CLI account capacity",
              "zh-CN": "CLI 账户容量",
              ja: "CLI アカウント容量",
              ko: "CLI 계정 용량",
            })}
          </h3>
          <p className="mt-0.5 text-xs text-muted-foreground">
            {t(
              {
                en: "{allocated} / {maximum} in use",
                "zh-CN": "已占用 {allocated} / {maximum}",
                ja: "使用中 {allocated} / {maximum}",
                ko: "사용 중 {allocated} / {maximum}",
              },
              {
                allocated: formatInteger(allocated),
                maximum: formatInteger(maximum),
              },
            )}
          </p>
        </div>
        <Button asChild variant="ghost" size="sm">
          <Link href="/provider-accounts">
            {t({
              en: "Manage accounts",
              "zh-CN": "管理账户",
              ja: "アカウントを管理",
              ko: "계정 관리",
            })}
            <ArrowRight aria-hidden="true" />
          </Link>
        </Button>
      </div>
      {capacity.length === 0 ? (
        <EmptyState
          icon={Gauge}
          label={t({
            en: "No schedulable CLI accounts",
            "zh-CN": "暂无可调度的 CLI 账户",
            ja: "スケジュール可能な CLI アカウントはありません",
            ko: "스케줄링 가능한 CLI 계정이 없습니다",
          })}
        />
      ) : (
        <Table className="min-w-[680px]">
          <TableHeader>
            <TableRow>
              <TableHead className="pl-4">
                {t({
                  en: "Account",
                  "zh-CN": "账户",
                  ja: "アカウント",
                  ko: "계정",
                })}
              </TableHead>
              <TableHead>
                {t({
                  en: "Concurrency",
                  "zh-CN": "并发使用",
                  ja: "同時実行数",
                  ko: "동시 실행",
                })}
              </TableHead>
              <TableHead className="text-right">
                {t({
                  en: "Available",
                  "zh-CN": "可用",
                  ja: "利用可能",
                  ko: "사용 가능",
                })}
              </TableHead>
              <TableHead className="pr-4 text-right">
                {t({
                  en: "Status",
                  "zh-CN": "状态",
                  ja: "状態",
                  ko: "상태",
                })}
              </TableHead>
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
  const { t } = useI18n();
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
          {providerLabel(t, item.provider_id)}
          {item.account_email ? ` · ${item.account_email}` : ""}
        </p>
      </TableCell>
      <TableCell>
        <div className="flex min-w-52 items-center gap-3">
          <Progress
            value={usage}
            className="h-1.5"
            aria-label={t(
              {
                en: "{account} uses {allocated} of {maximum} concurrent slots",
                "zh-CN":
                  "{account} 已占用 {allocated}，最大并发 {maximum}",
                ja: "{account} は同時実行枠 {maximum} のうち {allocated} を使用中",
                ko: "{account}이 동시 실행 {maximum}개 중 {allocated}개 사용 중",
              },
              {
                account: item.account_key,
                allocated: item.allocated_count,
                maximum: item.max_concurrency,
              },
            )}
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
          {available > 0
            ? t({
                en: "Available",
                "zh-CN": "可接收任务",
                ja: "受付可能",
                ko: "작업 수락 가능",
              })
            : t({
                en: "At capacity",
                "zh-CN": "容量已满",
                ja: "容量上限",
                ko: "용량 가득 참",
              })}
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

function providerLabel(t: Translate, providerId: string): string {
  if (providerId.includes("codex")) return "Codex";
  if (providerId.includes("grok")) return "Grok";
  if (providerId.includes("dreamina"))
    return t({
      en: "Dreamina",
      "zh-CN": "即梦",
      ja: "Dreamina",
      ko: "Dreamina",
    });
  return providerId;
}

function jobContextLabel(t: Translate, job: SchedulerActiveJob): string {
  const project =
    job.project_name ??
    job.project_id ??
    t({
      en: "Unattributed project",
      "zh-CN": "未归属项目",
      ja: "未帰属プロジェクト",
      ko: "미귀속 프로젝트",
    });
  const actor =
    job.user_display_name ??
    job.user_email ??
    job.service_account_name ??
    t({
      en: "System task",
      "zh-CN": "系统任务",
      ja: "システムタスク",
      ko: "시스템 작업",
    });
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

function activeJobStageLabel(t: Translate, stage: string): string {
  switch (stage) {
    case "running":
      return t({
        en: "Running",
        "zh-CN": "执行中",
        ja: "実行中",
        ko: "실행 중",
      });
    case "delayed":
      return t({
        en: "Delayed",
        "zh-CN": "延后",
        ja: "遅延",
        ko: "지연",
      });
    default:
      return t({
        en: "Queued",
        "zh-CN": "等待",
        ja: "待機中",
        ko: "대기",
      });
  }
}

function operationLabel(t: Translate, operation: string): string {
  switch (operation) {
    case "generation":
      return t({
        en: "Image generation",
        "zh-CN": "图片生成",
        ja: "画像生成",
        ko: "이미지 생성",
      });
    case "edit":
      return t({
        en: "Image editing",
        "zh-CN": "图片编辑",
        ja: "画像編集",
        ko: "이미지 편집",
      });
    case "video_generation":
      return t({
        en: "Video generation",
        "zh-CN": "视频生成",
        ja: "動画生成",
        ko: "동영상 생성",
      });
    default:
      return operation;
  }
}

function blockedErrorLabel(t: Translate, errorCode: string): string {
  switch (errorCode) {
    case "canonical_conflict":
      return t({
        en: "Authoritative results or settlement facts conflict",
        "zh-CN": "权威结果或结算事实发生冲突",
        ja: "正規結果または決済事実が競合しています",
        ko: "확정 결과 또는 정산 사실이 충돌합니다",
      });
    case "invalid_input":
      return t({
        en: "Reduction input does not satisfy the stable contract",
        "zh-CN": "归并输入不满足稳定契约",
        ja: "集約入力が安定契約を満たしていません",
        ko: "병합 입력이 안정적인 계약을 충족하지 않습니다",
      });
    case "artifact_integrity":
      return t({
        en: "Output file integrity validation failed",
        "zh-CN": "输出文件完整性校验失败",
        ja: "出力ファイルの整合性検証に失敗しました",
        ko: "출력 파일 무결성 검증에 실패했습니다",
      });
    default:
      return t({
        en: "Unknown blocking reason",
        "zh-CN": "未知阻断原因",
        ja: "不明なブロック理由",
        ko: "알 수 없는 차단 원인",
      });
  }
}

function resolvedStateLabel(t: Translate, state: string): string {
  switch (state) {
    case "succeeded":
      return t({
        en: "Succeeded",
        "zh-CN": "执行成功",
        ja: "成功",
        ko: "성공",
      });
    case "failed":
      return t({
        en: "Failed",
        "zh-CN": "执行失败",
        ja: "失敗",
        ko: "실패",
      });
    case "uncertain":
      return t({
        en: "Terminal state uncertain",
        "zh-CN": "终态待确认",
        ja: "終端状態が未確定",
        ko: "최종 상태 확인 필요",
      });
    case "canceled":
      return t({
        en: "Canceled",
        "zh-CN": "已取消",
        ja: "キャンセル済み",
        ko: "취소됨",
      });
    default:
      return state;
  }
}
