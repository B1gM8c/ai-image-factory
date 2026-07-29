"use client";

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import {
  ChevronLeft,
  ChevronRight,
  Clock3,
  LoaderCircle,
  RefreshCw,
  Search,
  ShieldCheck,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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
import { consoleFetch } from "@/lib/auth/client";

type AuditLogActor = {
  type: "user" | "system";
  user_id: string | null;
  email: string | null;
  display_name: string | null;
  session_id: string | null;
  ip_address: string | null;
};

type AuditLogProject = {
  id: string;
  name: string | null;
  organization_id: string | null;
};

type AuditLogResource = {
  type: string | null;
  id: string | null;
};

type AuditLogItem = {
  id: string;
  object: "audit_log";
  type: string;
  effective_at: number;
  actor: AuditLogActor;
  project: AuditLogProject | null;
  resource: AuditLogResource;
  request_id: string | null;
  outcome: "success" | "denied" | "failure";
  reason_code: string | null;
  details: Record<string, unknown>;
};

type AuditLogsSnapshot = {
  object: "list";
  as_of_ms: number;
  from_ms: number;
  to_ms: number;
  data: AuditLogItem[];
  first_id: string | null;
  last_id: string | null;
  has_more: boolean;
  next_after: string | null;
};

type Filters = {
  window: "24h" | "7d" | "30d" | "90d";
  outcome: "all" | AuditLogItem["outcome"];
  eventType: string;
  query: string;
};

type PageAnchor = {
  after: string | null;
  toMs: number | null;
};

const DEFAULT_FILTERS: Filters = {
  window: "7d",
  outcome: "all",
  eventType: "all",
  query: "",
};

export function AdminAuditLogs() {
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS);
  const [search, setSearch] = useState("");
  const [page, setPage] = useState<PageAnchor>({ after: null, toMs: null });
  const [previousPages, setPreviousPages] = useState<PageAnchor[]>([]);
  const [data, setData] = useState<AuditLogsSnapshot | null>(null);
  const [selected, setSelected] = useState<AuditLogItem | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        window: filters.window,
        limit: "50",
      });
      if (filters.outcome !== "all") params.set("outcome", filters.outcome);
      if (filters.eventType !== "all") {
        params.set("event_type", filters.eventType);
      }
      if (filters.query) params.set("q", filters.query);
      if (page.after) params.set("after", page.after);
      if (page.toMs) params.set("to_ms", page.toMs.toString());
      const response = await consoleFetch(
        `/api/gateway/v1/organization/audit_logs?${params.toString()}`,
        { signal },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      setData((await response.json()) as AuditLogsSnapshot);
    } catch (reason) {
      if (reason instanceof DOMException && reason.name === "AbortError") return;
      setError(reason instanceof Error ? reason.message : "审计日志加载失败");
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, [filters, page]);

  useEffect(() => {
    const controller = new AbortController();
    void load(controller.signal);
    return () => controller.abort();
  }, [load]);

  const eventTypes = useMemo(() => {
    const values = new Set(data?.data.map((item) => item.type) ?? []);
    if (filters.eventType !== "all") values.add(filters.eventType);
    return [...values].sort((left, right) => left.localeCompare(right));
  }, [data?.data, filters.eventType]);

  function resetPage(nextFilters: Filters) {
    setFilters(nextFilters);
    setPage({ after: null, toMs: null });
    setPreviousPages([]);
  }

  function applySearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    resetPage({ ...filters, query: search.trim() });
  }

  function nextPage() {
    if (!data?.next_after) return;
    setPreviousPages((current) => [...current, page]);
    setPage({ after: data.next_after, toMs: data.to_ms });
  }

  function previousPage() {
    const previous = previousPages.at(-1);
    if (!previous) return;
    setPreviousPages((current) => current.slice(0, -1));
    setPage(previous);
  }

  return (
    <>
      <section className="min-w-0 overflow-hidden rounded-lg border bg-background">
        <div className="flex flex-col gap-3 border-b p-4 xl:flex-row xl:items-center">
          <form className="relative min-w-0 flex-1" onSubmit={applySearch}>
            <Search
              className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
              aria-hidden="true"
            />
            <Input
              className="pl-9"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="搜索事件、用户、Request ID 或资源"
              aria-label="搜索审计日志"
            />
          </form>
          <div className="grid grid-cols-2 gap-2 sm:flex">
            <Select
              value={filters.window}
              onValueChange={(value) =>
                resetPage({ ...filters, window: value as Filters["window"] })
              }
            >
              <SelectTrigger className="w-full sm:w-36" aria-label="时间范围">
                <Clock3 aria-hidden="true" />
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="24h">最近 24 小时</SelectItem>
                <SelectItem value="7d">最近 7 天</SelectItem>
                <SelectItem value="30d">最近 30 天</SelectItem>
                <SelectItem value="90d">最近 90 天</SelectItem>
              </SelectContent>
            </Select>
            <Select
              value={filters.outcome}
              onValueChange={(value) =>
                resetPage({
                  ...filters,
                  outcome: value as Filters["outcome"],
                })
              }
            >
              <SelectTrigger className="w-full sm:w-32" aria-label="操作结果">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部结果</SelectItem>
                <SelectItem value="success">成功</SelectItem>
                <SelectItem value="denied">已拒绝</SelectItem>
                <SelectItem value="failure">失败</SelectItem>
              </SelectContent>
            </Select>
            <Select
              value={filters.eventType}
              onValueChange={(value) =>
                resetPage({ ...filters, eventType: value })
              }
            >
              <SelectTrigger
                className="col-span-2 w-full sm:w-64"
                aria-label="事件类型"
              >
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部事件</SelectItem>
                {eventTypes.map((eventType) => (
                  <SelectItem key={eventType} value={eventType}>
                    {formatEventType(eventType)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button
              type="button"
              variant="outline"
              size="icon"
              disabled={loading}
              onClick={() => void load()}
              title="刷新"
              aria-label="刷新审计日志"
            >
              <RefreshCw
                className={loading ? "animate-spin" : undefined}
                aria-hidden="true"
              />
            </Button>
          </div>
        </div>

        {error ? (
          <div
            className="flex min-h-64 flex-col items-center justify-center gap-3 px-6 text-center"
            role="alert"
          >
            <ShieldCheck className="size-8 text-muted-foreground" aria-hidden="true" />
            <div>
              <p className="font-medium">审计日志加载失败</p>
              <p className="mt-1 text-sm text-muted-foreground">{error}</p>
            </div>
            <Button variant="outline" size="sm" onClick={() => void load()}>
              <RefreshCw aria-hidden="true" />
              重新加载
            </Button>
          </div>
        ) : (
          <div className="max-w-full overflow-x-auto">
            <Table className="min-w-[920px]">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-44 pl-4">时间</TableHead>
                  <TableHead>事件</TableHead>
                  <TableHead>操作者</TableHead>
                  <TableHead>项目</TableHead>
                  <TableHead className="w-28">结果</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {loading && !data ? (
                  <TableRow>
                    <TableCell
                      colSpan={5}
                      className="h-64 text-center text-muted-foreground"
                    >
                      <LoaderCircle
                        className="mx-auto mb-2 size-5 animate-spin"
                        aria-hidden="true"
                      />
                      正在加载审计日志
                    </TableCell>
                  </TableRow>
                ) : null}
                {!loading && data?.data.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={5} className="h-64 text-center">
                      <ShieldCheck
                        className="mx-auto mb-3 size-8 text-muted-foreground"
                        aria-hidden="true"
                      />
                      <p className="font-medium">没有匹配的审计事件</p>
                      <p className="mt-1 text-sm text-muted-foreground">
                        调整时间范围或筛选条件后再试。
                      </p>
                    </TableCell>
                  </TableRow>
                ) : null}
                {data?.data.map((item) => (
                  <TableRow
                    key={item.id}
                    className="cursor-pointer"
                    tabIndex={0}
                    onClick={() => setSelected(item)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault();
                        setSelected(item);
                      }
                    }}
                  >
                    <TableCell className="pl-4 text-sm text-muted-foreground">
                      {formatDateTime(item.effective_at)}
                    </TableCell>
                    <TableCell>
                      <p className="font-medium">{formatEventType(item.type)}</p>
                      <p className="mt-0.5 max-w-80 truncate font-mono text-xs text-muted-foreground">
                        {item.type}
                      </p>
                    </TableCell>
                    <TableCell>
                      <p className="max-w-56 truncate">
                        {item.actor.display_name ??
                          item.actor.email ??
                          (item.actor.type === "system" ? "系统" : "未知用户")}
                      </p>
                      {item.actor.display_name && item.actor.email ? (
                        <p className="mt-0.5 max-w-56 truncate text-xs text-muted-foreground">
                          {item.actor.email}
                        </p>
                      ) : null}
                    </TableCell>
                    <TableCell>
                      <p className="max-w-52 truncate">
                        {item.project?.name ?? item.project?.id ?? "全局"}
                      </p>
                    </TableCell>
                    <TableCell>
                      <OutcomeBadge outcome={item.outcome} />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}

        <div className="flex items-center justify-between border-t px-4 py-3">
          <p className="text-sm text-muted-foreground">
            {data ? `更新于 ${formatTimestamp(data.as_of_ms)}` : " "}
          </p>
          <div className="flex gap-2">
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={previousPages.length === 0 || loading}
              onClick={previousPage}
            >
              <ChevronLeft aria-hidden="true" />
              上一页
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={!data?.has_more || loading}
              onClick={nextPage}
            >
              下一页
              <ChevronRight aria-hidden="true" />
            </Button>
          </div>
        </div>
      </section>

      <AuditLogDetails item={selected} onOpenChange={(open) => !open && setSelected(null)} />
    </>
  );
}

function AuditLogDetails({
  item,
  onOpenChange,
}: {
  item: AuditLogItem | null;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <Sheet open={item !== null} onOpenChange={onOpenChange}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-xl">
        <SheetHeader className="text-left">
          <SheetTitle>{item ? formatEventType(item.type) : "审计事件"}</SheetTitle>
          <SheetDescription>
            {item ? formatDateTime(item.effective_at) : ""}
          </SheetDescription>
        </SheetHeader>
        {item ? (
          <div className="mt-6 space-y-6">
            <div className="grid grid-cols-[7rem_minmax(0,1fr)] gap-x-4 gap-y-3 text-sm">
              <span className="text-muted-foreground">结果</span>
              <OutcomeBadge outcome={item.outcome} />
              <span className="text-muted-foreground">事件类型</span>
              <code className="break-all text-xs">{item.type}</code>
              <span className="text-muted-foreground">操作者</span>
              <span className="min-w-0 break-words">
                {item.actor.display_name ?? item.actor.email ?? item.actor.type}
              </span>
              <span className="text-muted-foreground">项目</span>
              <span className="min-w-0 break-all">
                {item.project?.name ?? item.project?.id ?? "全局"}
              </span>
              <span className="text-muted-foreground">资源</span>
              <span className="min-w-0 break-all">
                {[item.resource.type, item.resource.id].filter(Boolean).join(" · ") ||
                  "无"}
              </span>
              <span className="text-muted-foreground">Request ID</span>
              <code className="min-w-0 break-all text-xs">
                {item.request_id ?? "无"}
              </code>
              <span className="text-muted-foreground">Session ID</span>
              <code className="min-w-0 break-all text-xs">
                {item.actor.session_id ?? "无"}
              </code>
              <span className="text-muted-foreground">事件 ID</span>
              <code className="min-w-0 break-all text-xs">{item.id}</code>
            </div>
            <div>
              <h3 className="text-sm font-medium">事件详情</h3>
              <pre className="mt-3 max-h-80 overflow-auto rounded-md bg-muted p-4 text-xs leading-5">
                {JSON.stringify(item.details, null, 2)}
              </pre>
            </div>
          </div>
        ) : null}
      </SheetContent>
    </Sheet>
  );
}

function OutcomeBadge({ outcome }: { outcome: AuditLogItem["outcome"] }) {
  return (
    <Badge variant={outcome === "success" ? "outline" : "secondary"}>
      {outcome === "success" ? "成功" : outcome === "denied" ? "已拒绝" : "失败"}
    </Badge>
  );
}

function formatEventType(value: string) {
  const known: Record<string, string> = {
    "billing.credit_grant.issue": "发放赠送额度",
    "billing.credit_grant.revoke": "撤销赠送额度",
    "billing.integrity.run": "执行计费完整性校验",
    "identity.bootstrap": "初始化管理员",
    "identity.login": "用户登录",
    "identity.login.succeeded": "用户登录",
    "identity.login.failed": "登录失败",
    "identity.refresh": "刷新会话",
    "identity.logout": "用户退出",
    "pricing.official_source.sync": "同步官方价格",
    "pricing.price_book_version.publish": "发布价格版本",
    "project.create": "创建项目",
    "project.settings.update": "更新项目设置",
    "project.member.add": "添加项目成员",
    "project.member.update": "更新项目成员",
    "project.member.remove": "移除项目成员",
    "project.api_key.create": "创建 API Key",
    "project.api_key.update": "更新 API Key",
    "project.api_key.revoke": "撤销 API Key",
    "project.service_account.create": "创建服务账号",
    "project.service_account.delete": "删除服务账号",
    "project.model_policy.updated": "更新模型策略",
  };
  return known[value] ?? value;
}

function formatDateTime(unixSeconds: number) {
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(unixSeconds * 1_000));
}

function formatTimestamp(timestampMs: number) {
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(timestampMs));
}

async function responseMessage(response: Response) {
  const body = (await response.json().catch(() => null)) as
    | { error?: { message?: string } }
    | null;
  return body?.error?.message ?? `请求失败（${response.status}）`;
}
