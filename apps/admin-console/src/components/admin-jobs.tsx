"use client";

import { useEffect, useMemo, useState } from "react";
import {
  ChevronLeft,
  ChevronRight,
  ChevronRightIcon,
  Download,
  RefreshCw,
} from "lucide-react";
import {
  ActivityFilters,
  type ActivityFilterValues,
} from "@/components/activity-filters";
import {
  ActivityRequestSheet,
  requestState,
  sourceLabel,
} from "@/components/activity-request-sheet";
import { ActivityStatusBadge } from "@/components/activity-status-badge";
import {
  AdminQueryError,
  AdminQuerySkeleton,
} from "@/components/admin-query-state";
import { useConsoleSession } from "@/components/auth/console-session-provider";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useAdminQuery } from "@/hooks/use-admin-query";
import {
  formatDateTime,
  formatDurationMs,
  formatInteger,
} from "@/lib/admin/format";
import type {
  RequestLogCursor,
  RequestLogItem,
  RequestLogsSnapshot,
} from "@/lib/admin/types";

type Filters = ActivityFilterValues & {
  source: string;
};

type PageAnchor = {
  cursor: RequestLogCursor;
  toMs: number;
};

type Visibility = "mine" | "project";

const EMPTY_FILTERS: Filters = {
  q: "",
  provider: "all",
  state: "all",
  model: "",
  projectId: "",
  apiKeyId: "",
  window: "24h",
  source: "all",
};

const REQUEST_STATES = ["succeeded", "failed", "in_progress"];
const SOURCES = ["all", "images", "videos", "models"] as const;
const WINDOWS = ["1h", "6h", "24h", "7d", "30d", "90d"] as const;

export function AdminJobs({
  initialSearchParams = {},
}: {
  initialSearchParams?: Record<string, string | string[] | undefined>;
}) {
  const { activeWorkspace, loading: sessionLoading, user } = useConsoleSession();
  const platformOwner = Boolean(user?.roles.includes("platform_owner"));
  const workspaceProjectId =
    activeWorkspace?.kind === "project" ? activeWorkspace.id : null;
  const initialFilters = useMemo(
    () => filtersFromSearchParams(initialSearchParams),
    [initialSearchParams],
  );
  const [draft, setDraft] = useState<Filters>(initialFilters);
  const [filters, setFilters] = useState<Filters>(initialFilters);
  const [visibility, setVisibility] = useState<Visibility>("project");
  const [cursorStack, setCursorStack] = useState<PageAnchor[]>([]);
  const [selectedItem, setSelectedItem] = useState<RequestLogItem | null>(null);
  const effectiveVisibility: Visibility = workspaceProjectId
    ? visibility
    : platformOwner
      ? "project"
      : "mine";
  const path = useMemo(
    () =>
      buildPath(
        filters,
        cursorStack.at(-1),
        platformOwner,
        workspaceProjectId,
        effectiveVisibility,
      ),
    [
      cursorStack,
      effectiveVisibility,
      filters,
      platformOwner,
      workspaceProjectId,
    ],
  );
  const query = useAdminQuery<RequestLogsSnapshot>(
    path,
    !sessionLoading && Boolean(user && activeWorkspace),
    15_000,
  );

  useEffect(() => {
    setCursorStack([]);
    setSelectedItem(null);
  }, [activeWorkspace?.key]);

  const providers = useMemo(
    () =>
      uniqueOptions(
        query.data?.items.flatMap((item) =>
          item.provider_id ? [item.provider_id] : [],
        ) ?? [],
        draft.provider,
      ),
    [draft.provider, query.data],
  );
  const states = useMemo(
    () => uniqueOptions(REQUEST_STATES, draft.state),
    [draft.state],
  );

  function applyFilters() {
    const next = normalizedFilters(draft);
    setDraft(next);
    setFilters(next);
    setCursorStack([]);
    setSelectedItem(null);
  }

  function clearFilters() {
    const next = { ...EMPTY_FILTERS, source: filters.source };
    setDraft(next);
    setFilters(next);
    setCursorStack([]);
    setSelectedItem(null);
  }

  function changeSource(source: string) {
    const nextDraft = { ...draft, source };
    setDraft(nextDraft);
    setFilters({ ...normalizedFilters(nextDraft), source });
    setCursorStack([]);
    setSelectedItem(null);
  }

  function changeVisibility(next: string) {
    setVisibility(next as Visibility);
    setCursorStack([]);
    setSelectedItem(null);
  }

  return (
    <div className="min-w-0 space-y-5">
      <div className="min-w-0 overflow-hidden border">
        <div className="flex min-w-0 flex-col gap-3 border-b px-3 py-3 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex min-w-0 flex-col gap-2 sm:flex-row sm:items-center">
            <Tabs
              value={draft.source}
              onValueChange={changeSource}
              className="min-w-0"
            >
              <TabsList className="grid h-auto w-full grid-cols-4 gap-1 sm:inline-flex sm:w-auto">
                <TabsTrigger value="all">全部</TabsTrigger>
                <TabsTrigger value="images">图片</TabsTrigger>
                <TabsTrigger value="videos">视频</TabsTrigger>
                <TabsTrigger value="models">模型</TabsTrigger>
              </TabsList>
            </Tabs>
            {workspaceProjectId ? (
              <Tabs value={visibility} onValueChange={changeVisibility}>
                <TabsList className="h-9">
                  <TabsTrigger value="project">项目调用</TabsTrigger>
                  <TabsTrigger value="mine">我的调用</TabsTrigger>
                </TabsList>
              </Tabs>
            ) : null}
          </div>
          <div className="flex shrink-0 items-center justify-between gap-2">
            <p className="text-xs text-muted-foreground">
              {query.data
                ? `本页 ${formatInteger(query.data.items.length.toString())} 条 · ${formatDateTime(query.data.as_of_ms)}`
                : "正在读取调用记录"}
            </p>
            <Button
              type="button"
              variant="outline"
              size="icon"
              disabled={!query.data?.items.length}
              aria-label="导出当前结果"
              title="导出当前结果"
              onClick={() => {
                if (query.data) downloadRequestLogsCsv(query.data.items);
              }}
            >
              <Download aria-hidden="true" />
            </Button>
          </div>
        </div>

        <ActivityFilters
          value={draft}
          providers={providers}
          states={states}
          showProjectFilter={!workspaceProjectId}
          disabled={query.loading}
          onChange={(value) =>
            setDraft((current) => ({ ...current, ...value }))
          }
          onSubmit={applyFilters}
          onClear={clearFilters}
        />

        {query.loading ? (
          <div className="p-4">
            <AdminQuerySkeleton rows={7} />
          </div>
        ) : null}
        {!query.loading &&
        query.error &&
        (!query.data || query.error.status === 403) ? (
          <div className="p-4">
            <AdminQueryError error={query.error} retry={query.retry} />
          </div>
        ) : null}
        {query.data && (!query.error || query.error.status !== 403) ? (
          <>
            {query.error || query.refreshing ? (
              <div className="flex min-h-10 flex-wrap items-center justify-between gap-2 border-b px-3 py-2 text-sm">
                <span className="text-muted-foreground">
                  {query.refreshing
                    ? "正在刷新调用记录"
                    : "当前显示上一次成功结果"}
                </span>
                {!query.refreshing ? (
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={query.retry}
                  >
                    <RefreshCw aria-hidden="true" />
                    重试
                  </Button>
                ) : null}
              </div>
            ) : null}
            {query.data.items.length === 0 ? (
              <EmptyState filtered={hasActiveFilters(filters)} />
            ) : (
              <RequestLogsTable
                items={query.data.items}
                onSelect={setSelectedItem}
              />
            )}
            <Pagination
              data={query.data}
              page={cursorStack.length + 1}
              refreshing={query.refreshing}
              hasPrevious={cursorStack.length > 0}
              onPrevious={() =>
                setCursorStack((current) => current.slice(0, -1))
              }
              onNext={() => {
                if (query.data?.next_cursor) {
                  setCursorStack((current) => [
                    ...current,
                    {
                      cursor: query.data!.next_cursor!,
                      toMs: query.data!.to_ms,
                    },
                  ]);
                }
              }}
            />
          </>
        ) : null}
      </div>

      <ActivityRequestSheet
        item={selectedItem}
        economicsPath={
          selectedItem
            ? buildEconomicsPath(selectedItem, platformOwner)
            : null
        }
        onOpenChange={(open) => {
          if (!open) setSelectedItem(null);
        }}
      />
    </div>
  );
}

function RequestLogsTable({
  items,
  onSelect,
}: {
  items: RequestLogItem[];
  onSelect: (item: RequestLogItem) => void;
}) {
  return (
    <Table className="table-fixed">
      <TableHeader>
        <TableRow>
          <TableHead className="hidden w-40 pl-4 md:table-cell">时间</TableHead>
          <TableHead className="w-28 pl-4 md:pl-2">状态</TableHead>
          <TableHead>请求</TableHead>
          <TableHead className="hidden w-64 lg:table-cell">
            模型 / 归属
          </TableHead>
          <TableHead className="hidden w-24 sm:table-cell">耗时</TableHead>
          <TableHead className="w-10 pr-4">
            <span className="sr-only">查看详情</span>
          </TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {items.map((item) => (
          <TableRow
            key={item.request_id}
            className="cursor-pointer"
            onClick={() => onSelect(item)}
          >
            <TableCell className="hidden pl-4 text-xs text-muted-foreground md:table-cell">
              {formatDateTime(item.created_at_ms)}
            </TableCell>
            <TableCell className="pl-4 md:pl-2">
              <ActivityStatusBadge state={requestState(item)} />
            </TableCell>
            <TableCell className="min-w-0 max-w-0 whitespace-normal">
              <div className="flex min-w-0 items-center gap-2">
                <BadgeText>{sourceLabel(item.source)}</BadgeText>
                <p className="truncate font-mono text-xs font-medium">
                  {item.method} {item.route_pattern}
                </p>
              </div>
              <p className="mt-1 truncate font-mono text-xs text-muted-foreground">
                {item.request_id}
              </p>
              <p className="mt-1 text-xs text-muted-foreground md:hidden">
                {formatDateTime(item.created_at_ms)}
              </p>
            </TableCell>
            <TableCell className="hidden max-w-72 lg:table-cell">
              <p className="truncate font-mono text-xs">
                {item.model ?? item.api_key_id ?? item.auth_kind ?? "未识别"}
              </p>
              <p className="mt-1 truncate text-xs text-muted-foreground">
                {item.provider_id ??
                  item.service_account_id ??
                  item.project_id ??
                  "请求在归属前结束"}
              </p>
            </TableCell>
            <TableCell className="hidden text-xs tabular-nums sm:table-cell">
              <p>{formatDurationMs(item.duration_ms)}</p>
              <p className="mt-1 text-muted-foreground">
                HTTP {item.status_code}
              </p>
            </TableCell>
            <TableCell className="pr-4 text-right">
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="ml-auto size-8"
                aria-label={`查看请求 ${item.request_id}`}
                title="查看详情"
                onClick={(event) => {
                  event.stopPropagation();
                  onSelect(item);
                }}
              >
                <ChevronRightIcon
                  className="size-4 text-muted-foreground"
                  aria-hidden="true"
                />
              </Button>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

function BadgeText({ children }: { children: React.ReactNode }) {
  return (
    <span className="shrink-0 border px-1.5 py-0.5 text-[11px] text-muted-foreground">
      {children}
    </span>
  );
}

function EmptyState({ filtered }: { filtered: boolean }) {
  return (
    <div className="flex min-h-64 flex-col items-center justify-center px-4 text-center">
      <p className="text-sm font-medium">
        {filtered ? "没有匹配的调用记录" : "暂无 API 调用"}
      </p>
      <p className="mt-1 max-w-sm text-sm text-muted-foreground">
        {filtered
          ? "调整筛选条件后重新查询。"
          : "通过 API 发起模型、图片或视频请求后，记录会显示在这里。"}
      </p>
    </div>
  );
}

function Pagination({
  data,
  page,
  refreshing,
  hasPrevious,
  onPrevious,
  onNext,
}: {
  data: RequestLogsSnapshot;
  page: number;
  refreshing: boolean;
  hasPrevious: boolean;
  onPrevious: () => void;
  onNext: () => void;
}) {
  return (
    <div className="flex min-h-14 flex-wrap items-center justify-between gap-3 border-t px-4 py-3">
      <p className="text-xs text-muted-foreground">第 {page} 页</p>
      <div className="flex items-center gap-2">
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={!hasPrevious || refreshing}
          onClick={onPrevious}
        >
          <ChevronLeft aria-hidden="true" />
          上一页
        </Button>
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={!data.next_cursor || refreshing}
          onClick={onNext}
        >
          下一页
          <ChevronRight aria-hidden="true" />
        </Button>
      </div>
    </div>
  );
}

function buildPath(
  filters: Filters,
  page: PageAnchor | undefined,
  platformOwner: boolean,
  workspaceProjectId: string | null,
  visibility: Visibility,
): string {
  const query = new URLSearchParams({
    window: filters.window,
    limit: "50",
    visibility,
  });
  if (filters.source !== "all") query.set("source", filters.source);
  if (filters.provider !== "all") query.set("provider_id", filters.provider);
  if (filters.state !== "all") query.set("status", filters.state);
  if (filters.model) query.set("model", filters.model);
  const projectId = workspaceProjectId ?? filters.projectId;
  if (projectId) query.set("project_id", projectId);
  if (filters.apiKeyId) query.set("api_key_id", filters.apiKeyId);
  if (filters.q) query.set("q", filters.q);
  if (page) {
    query.set("to_ms", page.toMs.toString());
    query.set("cursor_created_at_ms", page.cursor.created_at_ms.toString());
    query.set("cursor_request_id", page.cursor.request_id);
  }
  const endpoint = platformOwner ? "/admin/v1/logs" : "/v1/console/logs";
  return `${endpoint}?${query.toString()}`;
}

function normalizedFilters(filters: Filters): Filters {
  return {
    ...filters,
    q: filters.q.trim(),
    model: filters.model.trim(),
    projectId: filters.projectId.trim(),
    apiKeyId: filters.apiKeyId.trim(),
  };
}

function filtersFromSearchParams(
  searchParams: Record<string, string | string[] | undefined>,
): Filters {
  const source = firstValue(searchParams.source);
  const window = firstValue(searchParams.window);
  return normalizedFilters({
    q: firstValue(searchParams.q),
    provider: firstValue(searchParams.provider_id) || "all",
    state:
      firstValue(searchParams.status) ||
      firstValue(searchParams.state) ||
      "all",
    model: firstValue(searchParams.model),
    projectId: firstValue(searchParams.project_id),
    apiKeyId: firstValue(searchParams.api_key_id),
    window: WINDOWS.includes(window as (typeof WINDOWS)[number])
      ? window
      : EMPTY_FILTERS.window,
    source: SOURCES.includes(source as (typeof SOURCES)[number])
      ? source
      : EMPTY_FILTERS.source,
  });
}

function firstValue(value: string | string[] | undefined): string {
  return Array.isArray(value) ? (value[0] ?? "") : (value ?? "");
}

function hasActiveFilters(filters: Filters): boolean {
  return Boolean(
    filters.q ||
      filters.source !== "all" ||
      filters.provider !== "all" ||
      filters.state !== "all" ||
      filters.model ||
      filters.projectId ||
      filters.apiKeyId,
  );
}

function uniqueOptions(values: string[], selected: string): string[] {
  const options = new Set(values.filter(Boolean));
  if (selected !== "all") options.add(selected);
  return [...options].sort((left, right) => left.localeCompare(right));
}

function buildEconomicsPath(item: RequestLogItem, platformOwner: boolean) {
  if (!item.job_id) return null;
  const base = `${platformOwner ? "/admin/v1" : "/v1/console"}/jobs/${item.job_id}/economics`;
  if (platformOwner || !item.project_id) return base;
  return `${base}?${new URLSearchParams({ project_id: item.project_id }).toString()}`;
}

function downloadRequestLogsCsv(items: RequestLogItem[]) {
  const rows = [
    [
      "created_at",
      "request_id",
      "source",
      "method",
      "route",
      "status_code",
      "duration_ms",
      "project_id",
      "api_key_id",
      "service_account_id",
      "provider_id",
      "model",
      "job_id",
      "error_code",
    ],
    ...items.map((item) => [
      new Date(item.created_at_ms).toISOString(),
      item.request_id,
      item.source,
      item.method,
      item.route_pattern,
      item.status_code.toString(),
      item.duration_ms.toString(),
      item.project_id ?? "",
      item.api_key_id ?? "",
      item.service_account_id ?? "",
      item.provider_id ?? "",
      item.model ?? "",
      item.job_id ?? "",
      item.error_code ?? "",
    ]),
  ];
  const content = rows
    .map((row) => row.map(csvCell).join(","))
    .join("\n");
  const url = URL.createObjectURL(
    new Blob([`\uFEFF${content}`], { type: "text/csv;charset=utf-8" }),
  );
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `api-request-logs-${new Date().toISOString().slice(0, 10)}.csv`;
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1_000);
}

function csvCell(value: string) {
  return `"${value.replaceAll('"', '""')}"`;
}
