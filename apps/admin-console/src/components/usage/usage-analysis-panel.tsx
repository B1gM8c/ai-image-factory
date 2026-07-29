"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { Download } from "lucide-react";
import {
  CartesianGrid,
  Line,
  LineChart,
  XAxis,
  YAxis,
} from "recharts";
import { AdminQueryError, AdminQuerySkeleton } from "@/components/admin-query-state";
import { Button } from "@/components/ui/button";
import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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
  formatInteger,
  formatMoneyMicros,
  formatStatus,
} from "@/lib/admin/format";
import type {
  UsageActivityPoint,
  UsageAnalysisSnapshot,
  UsageFilterOption,
  UsageSpendPoint,
} from "@/lib/admin/types";

export type UsageWindow = "24h" | "7d" | "30d";
type UsageInterval = "1m" | "1h" | "1d";
type UsageGroupBy = UsageAnalysisSnapshot["group_by"];
type UsageView = "activity" | "cost";

const chartColors = [
  "var(--chart-1)",
  "var(--chart-2)",
  "var(--chart-3)",
  "var(--chart-4)",
  "var(--chart-5)",
];

const groupOptions: Array<{ value: UsageGroupBy; label: string }> = [
  { value: "line_item", label: "计量项目" },
  { value: "project", label: "项目" },
  { value: "api_key", label: "API Key" },
  { value: "user", label: "用户" },
  { value: "provider", label: "供应商" },
  { value: "model", label: "模型" },
  { value: "operation", label: "操作" },
  { value: "service_tier", label: "服务层级" },
  { value: "none", label: "不分组" },
];

const serviceTierOptions: UsageFilterOption[] = [
  { value: "default", label: "Default" },
  { value: "flex", label: "Flex" },
  { value: "priority", label: "Priority" },
];

export function UsageAnalysisPanel({
  window,
  projectId,
  platformOwner,
  enabled,
  refreshKey,
}: {
  window: UsageWindow;
  projectId: string | null;
  platformOwner: boolean;
  enabled: boolean;
  refreshKey: number;
}) {
  const [view, setView] = useState<UsageView>("activity");
  const [interval, setInterval] = useState<UsageInterval>(
    window === "24h" ? "1h" : "1d",
  );
  const [groupBy, setGroupBy] = useState<UsageGroupBy>("line_item");
  const [apiKeyId, setApiKeyId] = useState("all");
  const [providerId, setProviderId] = useState("all");
  const [model, setModel] = useState("all");
  const [operation, setOperation] = useState("all");
  const [serviceTier, setServiceTier] = useState("all");
  const [userId, setUserId] = useState("all");
  const [activityMetric, setActivityMetric] = useState("request::request");
  const [exportOpen, setExportOpen] = useState(false);
  const [exportType, setExportType] = useState<UsageView>("activity");
  const previousRefreshKey = useRef(refreshKey);

  useEffect(() => {
    setInterval(window === "24h" ? "1h" : "1d");
  }, [window]);

  useEffect(() => {
    setApiKeyId("all");
    setProviderId("all");
    setModel("all");
    setOperation("all");
    setServiceTier("all");
    setUserId("all");
  }, [projectId]);

  const endpoint = useMemo(() => {
    const base = platformOwner ? "/admin/v1/usage" : "/v1/console/usage";
    const params = new URLSearchParams({
      window,
      interval,
      group_by: groupBy,
    });
    if (projectId) params.set("project_id", projectId);
    appendFilter(params, "api_key_id", apiKeyId);
    appendFilter(params, "provider_id", providerId);
    appendFilter(params, "model", model);
    appendFilter(params, "operation", operation);
    appendFilter(params, "service_tier", serviceTier);
    if (platformOwner) appendFilter(params, "user_id", userId);
    return `${base}?${params.toString()}`;
  }, [
    apiKeyId,
    groupBy,
    interval,
    model,
    operation,
    platformOwner,
    projectId,
    providerId,
    serviceTier,
    userId,
    window,
  ]);
  const query = useAdminQuery<UsageAnalysisSnapshot>(endpoint, enabled);
  const options = query.data?.filter_options;
  const activityMetricOptions = useMemo(
    () => collectActivityMetrics(query.data?.activity ?? []),
    [query.data?.activity],
  );
  const chart = useMemo(
    () => buildChart(query.data ?? undefined, view, activityMetric),
    [activityMetric, query.data, view],
  );
  const visibleGroupOptions = useMemo(
    () =>
      projectId
        ? groupOptions.filter((option) => option.value !== "project")
        : groupOptions,
    [projectId],
  );

  useEffect(() => {
    if (previousRefreshKey.current === refreshKey) return;
    previousRefreshKey.current = refreshKey;
    query.retry();
  }, [query.retry, refreshKey]);

  useEffect(() => {
    if (activityMetricOptions.length === 0) return;
    if (!activityMetricOptions.some((option) => option.value === activityMetric)) {
      setActivityMetric(activityMetricOptions[0].value);
    }
  }, [activityMetric, activityMetricOptions]);

  useEffect(() => {
    if (projectId && groupBy === "project") setGroupBy("line_item");
  }, [groupBy, projectId]);

  return (
    <section className="min-w-0 space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <FilterSelect
          label="全部 API Keys"
          value={apiKeyId}
          options={options?.api_keys ?? []}
          onValueChange={setApiKeyId}
        />
        <FilterSelect
          label="全部供应商"
          value={providerId}
          options={options?.providers ?? []}
          onValueChange={setProviderId}
        />
        <FilterSelect
          label="全部模型"
          value={model}
          options={options?.models ?? []}
          onValueChange={setModel}
        />
        <FilterSelect
          label="全部操作"
          value={operation}
          options={options?.operations ?? []}
          onValueChange={setOperation}
        />
        <FilterSelect
          label="全部服务层级"
          value={serviceTier}
          options={serviceTierOptions}
          onValueChange={setServiceTier}
        />
        {platformOwner ? (
          <FilterSelect
            label="全部用户"
            value={userId}
            options={options?.users ?? []}
            onValueChange={setUserId}
          />
        ) : null}
        <Button
          type="button"
          variant="outline"
          className="ml-auto"
          onClick={() => {
            setExportType(view);
            setExportOpen(true);
          }}
          disabled={!query.data}
        >
          <Download aria-hidden="true" />
          导出
        </Button>
      </div>

      {query.loading ? <AdminQuerySkeleton rows={5} /> : null}
      {!query.loading && query.error && !query.data ? (
        <AdminQueryError error={query.error} retry={query.retry} />
      ) : null}
      {query.data ? (
        <>
          <div className="overflow-hidden rounded-md border">
            <div className="flex flex-wrap items-center justify-between gap-3 border-b px-4 py-3">
              <Tabs
                value={view}
                onValueChange={(value) => setView(value as UsageView)}
              >
                <TabsList className="h-9">
                  <TabsTrigger value="activity">API 用量</TabsTrigger>
                  <TabsTrigger value="cost">费用分类</TabsTrigger>
                </TabsList>
              </Tabs>
              <div className="flex flex-wrap items-center gap-2">
                {view === "activity" && activityMetricOptions.length > 0 ? (
                  <Select value={activityMetric} onValueChange={setActivityMetric}>
                    <SelectTrigger
                      className="w-[168px] max-w-full"
                      aria-label="计量指标"
                    >
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {activityMetricOptions.map((option) => (
                        <SelectItem key={option.value} value={option.value}>
                          {option.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                ) : null}
                <Select
                  value={groupBy}
                  onValueChange={(value) => setGroupBy(value as UsageGroupBy)}
                >
                  <SelectTrigger className="w-[132px]" aria-label="分组方式">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {visibleGroupOptions.map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        {option.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Tabs
                  value={interval}
                  onValueChange={(value) => setInterval(value as UsageInterval)}
                >
                  <TabsList className="h-9">
                    {window === "24h" ? (
                      <TabsTrigger value="1m">1m</TabsTrigger>
                    ) : null}
                    <TabsTrigger value="1h">1h</TabsTrigger>
                    <TabsTrigger value="1d">1d</TabsTrigger>
                  </TabsList>
                </Tabs>
              </div>
            </div>
            <div className="p-4">
              {chart.rows.length === 0 ? (
                <EmptyTrend view={view} />
              ) : (
                <ChartContainer
                  config={chart.config}
                  className="h-72 w-full aspect-auto"
                >
                  <LineChart
                    data={chart.rows}
                    margin={{ left: 4, right: 16, top: 12, bottom: 0 }}
                  >
                    <CartesianGrid vertical={false} />
                    <XAxis
                      dataKey="label"
                      tickLine={false}
                      axisLine={false}
                      tickMargin={8}
                      minTickGap={28}
                    />
                    <YAxis
                      tickLine={false}
                      axisLine={false}
                      width={52}
                      tickFormatter={compactNumber}
                    />
                    <ChartTooltip
                      cursor={false}
                      content={<ChartTooltipContent />}
                    />
                    <ChartLegend content={<ChartLegendContent />} />
                    {chart.series.map((series) => (
                      <Line
                        key={series.key}
                        type="monotone"
                        dataKey={series.key}
                        name={series.label}
                        stroke={`var(--color-${series.key})`}
                        strokeWidth={2}
                        dot={false}
                        connectNulls
                      />
                    ))}
                  </LineChart>
                </ChartContainer>
              )}
            </div>
          </div>

          <UsageBreakdown
            data={query.data}
            view={view}
            activityMetric={activityMetric}
          />
        </>
      ) : null}

      <UsageExportDialog
        open={exportOpen}
        onOpenChange={setExportOpen}
        data={query.data ?? null}
        exportType={exportType}
        onExportTypeChange={setExportType}
        activityMetric={activityMetric}
      />
    </section>
  );
}

function FilterSelect({
  label,
  value,
  options,
  onValueChange,
}: {
  label: string;
  value: string;
  options: UsageFilterOption[];
  onValueChange: (value: string) => void;
}) {
  return (
    <Select value={value} onValueChange={onValueChange}>
      <SelectTrigger
        className="w-full min-w-0 sm:w-[160px]"
        aria-label={label}
      >
        <SelectValue placeholder={label} />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="all">{label}</SelectItem>
        {options.map((option) => (
          <SelectItem key={option.value} value={option.value}>
            {option.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

function UsageBreakdown({
  data,
  view,
  activityMetric,
}: {
  data: UsageAnalysisSnapshot;
  view: UsageView;
  activityMetric: string;
}) {
  const rows = useMemo(
    () =>
      view === "activity"
        ? activityBreakdown(
            data.activity.filter(
              (point) =>
                metricKey(point.billing_metric, point.billing_unit) === activityMetric,
            ),
          )
        : spendBreakdown(data.spend),
    [activityMetric, data.activity, data.spend, view],
  );
  if (rows.length === 0) return null;
  return (
    <div className="overflow-hidden rounded-md border">
      <div className="border-b px-4 py-3">
        <h2 className="text-sm font-semibold">分组明细</h2>
      </div>
      <div className="overflow-x-auto">
        <Table className="min-w-[760px]">
          <TableHeader>
            <TableRow>
              <TableHead className="pl-4">分组</TableHead>
              <TableHead>计量项目</TableHead>
              <TableHead>结果</TableHead>
              <TableHead className="text-right">数量</TableHead>
              {view === "cost" ? (
                <TableHead className="pr-4 text-right">金额</TableHead>
              ) : null}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <TableRow key={row.key}>
                <TableCell className="pl-4 font-medium">{row.group}</TableCell>
                <TableCell>{metricLabel(row.metric)}</TableCell>
                <TableCell>{formatStatus(row.outcome)}</TableCell>
                <TableCell className="text-right font-mono tabular-nums">
                  {formatInteger(row.quantity)} {unitLabel(row.unit)}
                </TableCell>
                {view === "cost" ? (
                  <TableCell className="pr-4 text-right font-mono tabular-nums">
                    {formatMoneyMicros(row.amountMicros ?? "0", row.currency ?? "USD")}
                  </TableCell>
                ) : null}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

function UsageExportDialog({
  open,
  onOpenChange,
  data,
  exportType,
  onExportTypeChange,
  activityMetric,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  data: UsageAnalysisSnapshot | null;
  exportType: UsageView;
  onExportTypeChange: (value: UsageView) => void;
  activityMetric: string;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>导出用量</DialogTitle>
          <DialogDescription>
            导出当前项目、筛选、分组和时间粒度下的聚合结果。
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-2">
          <div>
            <p className="mb-2 text-sm font-medium">导出类型</p>
            <Tabs
              value={exportType}
              onValueChange={(value) => onExportTypeChange(value as UsageView)}
            >
              <TabsList className="grid w-full grid-cols-2">
                <TabsTrigger value="activity">活动数据</TabsTrigger>
                <TabsTrigger value="cost">费用数据</TabsTrigger>
              </TabsList>
            </Tabs>
          </div>
          <dl className="grid grid-cols-[112px_1fr] gap-x-4 gap-y-2 text-sm">
            <dt className="text-muted-foreground">时间范围</dt>
            <dd>
              {data
                ? `${new Date(data.from_ms).toLocaleString()} - ${new Date(data.to_ms).toLocaleString()}`
                : "--"}
            </dd>
            <dt className="text-muted-foreground">分组</dt>
            <dd>{data ? groupLabel(data.group_by) : "--"}</dd>
            <dt className="text-muted-foreground">时间粒度</dt>
            <dd>{data?.interval ?? "--"}</dd>
            <dt className="text-muted-foreground">文件格式</dt>
            <dd>CSV</dd>
          </dl>
        </div>
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            取消
          </Button>
          <Button
            type="button"
            onClick={() => {
              if (!data) return;
              downloadUsageCsv(data, exportType, activityMetric);
              onOpenChange(false);
            }}
            disabled={!data}
          >
            <Download aria-hidden="true" />
            下载 CSV
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function EmptyTrend({ view }: { view: UsageView }) {
  return (
    <div className="flex h-64 flex-col items-center justify-center text-center">
      <p className="text-sm font-medium">
        {view === "activity" ? "暂无活动数据" : "暂无费用数据"}
      </p>
      <p className="mt-1 text-sm text-muted-foreground">
        当前项目和筛选条件下没有可展示的数据。
      </p>
    </div>
  );
}

function buildChart(
  data: UsageAnalysisSnapshot | undefined,
  view: UsageView,
  activityMetric: string,
) {
  if (!data) return { rows: [], series: [], config: {} as ChartConfig };
  const points =
    view === "activity"
      ? data.activity.filter(
          (point) =>
            metricKey(point.billing_metric, point.billing_unit) === activityMetric,
        )
      : data.spend;
  const seriesIdentities = new Map<string, string>();
  for (const point of points) {
    const currency = "currency" in point ? point.currency : "";
    const identity = `${point.group_value}\u0000${currency}`;
    const groupLabel =
      point.group_kind === "line_item"
        ? metricLabel(point.billing_metric)
        : point.group_label;
    const label = currency ? `${groupLabel} · ${currency}` : groupLabel;
    if (!seriesIdentities.has(identity)) seriesIdentities.set(identity, label);
  }
  const series = [...seriesIdentities.entries()].map(([identity, label], index) => ({
    identity,
    label,
    key: `series_${index}`,
  }));
  const seriesByIdentity = new Map(series.map((item) => [item.identity, item]));
  const buckets = new Map<number, Record<string, number | string>>();
  for (const point of points) {
    const bucket =
      buckets.get(point.bucket_start_ms) ??
      {
        bucket: point.bucket_start_ms,
        label: formatBucket(point.bucket_start_ms, data.interval),
      };
    const currency = "currency" in point ? point.currency : "";
    const item = seriesByIdentity.get(`${point.group_value}\u0000${currency}`);
    if (!item) continue;
    const value =
      view === "activity"
        ? Number(BigInt(point.quantity))
        : Number(BigInt((point as UsageSpendPoint).amount_micros)) / 1_000_000;
    bucket[item.key] = Number(bucket[item.key] ?? 0) + value;
    buckets.set(point.bucket_start_ms, bucket);
  }
  const config = Object.fromEntries(
    series.map((item, index) => [
      item.key,
      { label: item.label, color: chartColors[index % chartColors.length] },
    ]),
  ) satisfies ChartConfig;
  return {
    rows: [...buckets.values()].sort(
      (left, right) => Number(left.bucket) - Number(right.bucket),
    ),
    series,
    config,
  };
}

type BreakdownRow = {
  key: string;
  group: string;
  metric: string;
  unit: string;
  outcome: string;
  quantity: string;
  currency?: string;
  amountMicros?: string;
};

function activityBreakdown(points: UsageActivityPoint[]): BreakdownRow[] {
  const totals = new Map<string, BreakdownRow>();
  for (const point of points) {
    const key = `${point.group_value}\u0000${point.billing_metric}\u0000${point.billing_unit}\u0000${point.outcome}`;
    const current = totals.get(key);
    totals.set(key, {
      key,
      group:
        point.group_kind === "line_item"
          ? metricLabel(point.billing_metric)
          : point.group_label,
      metric: point.billing_metric,
      unit: point.billing_unit,
      outcome: point.outcome,
      quantity: (
        BigInt(current?.quantity ?? "0") + BigInt(point.quantity)
      ).toString(),
    });
  }
  return [...totals.values()];
}

function spendBreakdown(points: UsageSpendPoint[]): BreakdownRow[] {
  const totals = new Map<string, BreakdownRow>();
  for (const point of points) {
    const key = `${point.group_value}\u0000${point.billing_metric}\u0000${point.billing_unit}\u0000${point.outcome}\u0000${point.currency}`;
    const current = totals.get(key);
    totals.set(key, {
      key,
      group:
        point.group_kind === "line_item"
          ? metricLabel(point.billing_metric)
          : point.group_label,
      metric: point.billing_metric,
      unit: point.billing_unit,
      outcome: point.outcome,
      currency: point.currency,
      quantity: (
        BigInt(current?.quantity ?? "0") + BigInt(point.quantity)
      ).toString(),
      amountMicros: (
        BigInt(current?.amountMicros ?? "0") + BigInt(point.amount_micros)
      ).toString(),
    });
  }
  return [...totals.values()];
}

function downloadUsageCsv(
  data: UsageAnalysisSnapshot,
  type: UsageView,
  activityMetric: string,
) {
  const rows =
    type === "activity"
      ? data.activity
          .filter(
            (point) =>
              metricKey(point.billing_metric, point.billing_unit) === activityMetric,
          )
          .map((point) => [
            new Date(point.bucket_start_ms).toISOString(),
            data.interval,
            point.group_kind,
            point.group_value,
            point.group_label,
            point.billing_metric,
            point.billing_unit,
            point.outcome,
            point.quantity,
          ])
      : data.spend.map((point) => [
          new Date(point.bucket_start_ms).toISOString(),
          data.interval,
          point.group_kind,
          point.group_value,
          point.group_label,
          point.billing_metric,
          point.billing_unit,
          point.outcome,
          point.quantity,
          point.currency,
          point.amount_micros,
        ]);
  const headers =
    type === "activity"
      ? [
          "bucket_start",
          "interval",
          "group_by",
          "group_value",
          "group_label",
          "metric",
          "unit",
          "outcome",
          "quantity",
        ]
      : [
          "bucket_start",
          "interval",
          "group_by",
          "group_value",
          "group_label",
          "metric",
          "unit",
          "outcome",
          "quantity",
          "currency",
          "amount_micros",
        ];
  const csv = [headers, ...rows].map(csvRow).join("\n");
  const blob = new Blob([`\uFEFF${csv}`], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `usage-${type}-${new Date(data.to_ms).toISOString().slice(0, 10)}.csv`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function csvRow(values: Array<string | number>) {
  return values
    .map((value) => `"${String(value).replaceAll('"', '""')}"`)
    .join(",");
}

function appendFilter(params: URLSearchParams, name: string, value: string) {
  if (value !== "all") params.set(name, value);
}

function formatBucket(value: number, interval: UsageInterval) {
  return new Intl.DateTimeFormat("zh-CN", {
    month: interval === "1d" ? "2-digit" : undefined,
    day: "2-digit",
    hour: interval === "1d" ? undefined : "2-digit",
    minute: interval === "1d" ? undefined : "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function compactNumber(value: number) {
  return new Intl.NumberFormat("zh-CN", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

function metricLabel(metric: string) {
  const labels: Record<string, string> = {
    request: "请求",
    output: "输出",
    image_output: "图片输出",
    image_output_token: "图片输出 Token",
    image_input: "图片输入",
    image_input_token: "图片输入 Token",
    text_input_token: "文本输入 Token",
    cached_text_input_token: "缓存文本 Token",
    cached_image_input_token: "缓存图片 Token",
    video_requested_second: "视频请求时长",
    video_output_second: "视频输出时长",
    membership_point: "会员积分",
  };
  return labels[metric] ?? "其他用量";
}

function unitLabel(unit: string) {
  const labels: Record<string, string> = {
    request: "次",
    output: "个",
    image: "张",
    token: "Token",
    second: "秒",
    point: "积分",
  };
  return labels[unit] ?? "单位";
}

function metricKey(metric: string, unit: string) {
  return `${metric}::${unit}`;
}

function collectActivityMetrics(points: UsageActivityPoint[]) {
  const metrics = new Map<string, { value: string; label: string }>();
  for (const point of points) {
    const value = metricKey(point.billing_metric, point.billing_unit);
    if (!metrics.has(value)) {
      metrics.set(value, {
        value,
        label: `${metricLabel(point.billing_metric)} · ${unitLabel(point.billing_unit)}`,
      });
    }
  }
  return [...metrics.values()].sort((left, right) => {
    if (left.value === "request::request") return -1;
    if (right.value === "request::request") return 1;
    return left.label.localeCompare(right.label, "zh-CN");
  });
}

function groupLabel(group: UsageGroupBy) {
  return groupOptions.find((option) => option.value === group)?.label ?? group;
}
