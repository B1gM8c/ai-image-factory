"use client";

import { useMemo, useState } from "react";
import { CircleDollarSign, RefreshCw, Search } from "lucide-react";
import { AdminQueryError, AdminQuerySkeleton } from "@/components/admin-query-state";
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
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useAdminQuery } from "@/hooks/use-admin-query";
import { formatDateTime } from "@/lib/admin/format";
import type {
  PricingCoverageRow,
  PricingCoverageSnapshot,
} from "@/lib/admin/types";

const ENDPOINT = "/admin/v1/pricing/coverage";

export function PricingCoverageTable({
  onConfigurePrice,
  configuringSurfaceKey = null,
}: {
  onConfigurePrice?: (row: PricingCoverageRow) => Promise<void> | void;
  configuringSurfaceKey?: string | null;
}) {
  const query = useAdminQuery<PricingCoverageSnapshot>(ENDPOINT);
  const [search, setSearch] = useState("");
  const [provider, setProvider] = useState("all");
  const [readiness, setReadiness] = useState("all");
  const rows = query.data?.rows ?? [];
  const providers = useMemo(
    () =>
      Array.from(
        new Map(rows.map((row) => [row.provider_id, row.provider_display_name])).entries(),
      ),
    [rows],
  );
  const filteredRows = useMemo(() => {
    const needle = search.trim().toLowerCase();
    return rows.filter((row) => {
      if (provider !== "all" && row.provider_id !== provider) return false;
      if (readiness !== "all" && row.readiness !== readiness) return false;
      return !needle || [
        row.provider_display_name,
        row.provider_model_display_name,
        row.provider_model_id,
        row.public_model_id ?? "",
        row.api_profile ?? "",
        row.operation,
        row.pricing_operation ?? "",
      ].some((value) => value.toLowerCase().includes(needle));
    });
  }, [provider, readiness, rows, search]);

  if (query.loading) return <AdminQuerySkeleton rows={8} />;
  if (query.error && (!query.data || query.error.status === 403)) {
    return <AdminQueryError error={query.error} retry={query.retry} />;
  }
  if (!query.data) return null;

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center gap-x-6 gap-y-2 border-y py-3 text-sm">
        <SummaryValue value={query.data.summary.surfaces} label="API 模型入口" />
        <SummaryValue value={query.data.summary.routable_surfaces} label="可路由" />
        <SummaryValue value={query.data.summary.sale_priced_surfaces} label="已配置售价" />
        <SummaryValue value={query.data.summary.actual_cost_surfaces} label="实际成本可归集" />
        <SummaryValue value={query.data.summary.blocked_surfaces} label="阻断项" alert />
        <span className="ml-auto text-xs text-muted-foreground">
          平台基础计价契约 · {formatDateTime(query.data.as_of_ms)}
        </span>
      </div>

      <div className="flex flex-col gap-3 lg:flex-row">
        <div className="relative min-w-0 flex-1">
          <Search
            className="pointer-events-none absolute left-3 top-2.5 size-4 text-muted-foreground"
            aria-hidden="true"
          />
          <Input
            className="pl-9"
            value={search}
            placeholder="搜索模型、API 协议或操作"
            onChange={(event) => setSearch(event.target.value)}
          />
        </div>
        <Select value={provider} onValueChange={setProvider}>
          <SelectTrigger className="w-full lg:w-44"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部供应商</SelectItem>
            {providers.map(([id, label]) => (
              <SelectItem key={id} value={id}>{label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={readiness} onValueChange={setReadiness}>
          <SelectTrigger className="w-full lg:w-40"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部状态</SelectItem>
            <SelectItem value="ready">就绪</SelectItem>
            <SelectItem value="warning">需关注</SelectItem>
            <SelectItem value="blocked">已阻断</SelectItem>
          </SelectContent>
        </Select>
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={query.retry}
          disabled={query.refreshing}
        >
          <RefreshCw className={query.refreshing ? "animate-spin" : ""} aria-hidden="true" />
          刷新
        </Button>
      </div>

      <div className="min-w-0 overflow-hidden rounded-md border">
        <div className="overflow-x-auto">
          <Table className="min-w-[1060px]">
            <TableHeader>
              <TableRow>
                <TableHead className="pl-4">模型</TableHead>
                <TableHead>API 入口</TableHead>
                <TableHead>路由</TableHead>
                <TableHead>客户售价</TableHead>
                <TableHead>计量</TableHead>
                <TableHead>上游成本</TableHead>
                <TableHead className="pr-4">结论</TableHead>
                {onConfigurePrice ? <TableHead className="w-28 pr-4 text-right">操作</TableHead> : null}
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredRows.length > 0 ? filteredRows.map((row) => (
                <CoverageRow
                  key={coverageKey(row)}
                  row={row}
                  onConfigurePrice={onConfigurePrice}
                  configuring={configuringSurfaceKey === configurableSurfaceKey(row)}
                />
              )) : (
                <TableRow>
                  <TableCell
                    colSpan={onConfigurePrice ? 8 : 7}
                    className="h-40 text-center text-muted-foreground"
                  >
                    没有符合条件的模型入口
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </div>
    </div>
  );
}

function CoverageRow({
  row,
  onConfigurePrice,
  configuring,
}: {
  row: PricingCoverageRow;
  onConfigurePrice?: (row: PricingCoverageRow) => Promise<void> | void;
  configuring: boolean;
}) {
  return (
    <TableRow>
      <TableCell className="pl-4">
        <span className="block max-w-56 truncate font-medium">
          {row.provider_model_display_name}
        </span>
        <span className="mt-0.5 block max-w-56 truncate font-mono text-xs text-muted-foreground">
          {row.provider_display_name} · {row.provider_model_id}
        </span>
      </TableCell>
      <TableCell>
        <span className="block max-w-56 truncate font-mono text-sm">
          {row.public_model_id ?? "尚未绑定"}
        </span>
        <span className="mt-0.5 block max-w-56 truncate font-mono text-xs text-muted-foreground">
          {row.api_profile ?? "缺少平台路由"} · {operationLabel(row.operation)}
        </span>
        {row.pricing_operation && (
          <span className="mt-0.5 block text-xs text-muted-foreground">
            计价操作 {pricingOperationLabel(row.pricing_operation)}
          </span>
        )}
      </TableCell>
      <TableCell>
        <StatusBadge status={row.route_status} labels={{
          routable: "可路由",
          unavailable: "无可用账户",
          missing: "未绑定",
        }} />
        <span className="mt-1 block text-xs text-muted-foreground">
          {row.routable_account_count} 个可执行账户
        </span>
      </TableCell>
      <TableCell>
        <StatusBadge status={row.customer_price_status} labels={{
          ready: "已配置",
          ambiguous: "价格冲突",
          missing: "缺少售价",
        }} />
        <span className="mt-1 block text-xs text-muted-foreground">
          {row.customer_price_currencies.length > 0
            ? row.customer_price_currencies.join("、")
            : "请求将无法冻结报价"}
        </span>
      </TableCell>
      <TableCell>
        <StatusBadge status={row.metering_status} labels={{
          exact: "精确",
          estimated: "含估算",
          ambiguous: "规则冲突",
          incompatible: "契约不兼容",
          missing: "缺少规则",
        }} />
      </TableCell>
      <TableCell>
        <StatusBadge status={row.provider_cost_status} labels={{
          provider_actual: "实际成本",
          provider_allocated: "订阅分摊",
          provider_estimated: "估算成本",
          benchmark_only: "仅官方基准",
          actual_price_missing: "缺少实际成本规则",
          not_emitted: "上游不提供",
          ambiguous: "成本冲突",
        }} />
        <span className="mt-1 block text-xs text-muted-foreground">
          {row.provider_cost_currencies.length > 0
            ? row.provider_cost_currencies.join("、")
            : "不影响请求，影响毛利"}
        </span>
      </TableCell>
      <TableCell className="pr-4">
        <StatusBadge status={row.readiness} labels={{
          ready: "基础契约可结算",
          warning: "基础契约可用",
          blocked: "基础契约阻断",
        }} />
        {row.blocking_reasons.length > 0 ? (
          <span className="mt-1 block max-w-52 text-xs text-muted-foreground">
            {row.blocking_reasons.map(reasonLabel).join("；")}
          </span>
        ) : (
          <span className="mt-1 block text-xs text-muted-foreground">
            {row.source_status === "verified" ? "来源可复核" : "含人工维护来源"}
          </span>
        )}
      </TableCell>
      {onConfigurePrice ? (
        <TableCell className="pr-4 text-right">
          <Button
            type="button"
            size="sm"
            variant={row.customer_price_status === "ready" ? "outline" : "default"}
            disabled={
              configuring
              ||
              !row.api_profile
              || !row.public_model_id
              || !row.pricing_operation
              || row.route_status === "missing"
            }
            onClick={() => void onConfigurePrice(row)}
          >
            {configuring
              ? <RefreshCw className="animate-spin" aria-hidden="true" />
              : <CircleDollarSign aria-hidden="true" />}
            {row.customer_price_status === "ready" ? "调整" : "定价"}
          </Button>
        </TableCell>
      ) : null}
    </TableRow>
  );
}

function SummaryValue({
  value,
  label,
  alert = false,
}: {
  value: number;
  label: string;
  alert?: boolean;
}) {
  return (
    <span className={alert && value > 0 ? "text-destructive" : undefined}>
      <strong className="font-semibold tabular-nums">{value}</strong>
      <span className="ml-1.5 text-muted-foreground">{label}</span>
    </span>
  );
}

function StatusBadge({
  status,
  labels,
}: {
  status: string;
  labels: Record<string, string>;
}) {
  const destructive = [
    "blocked",
    "missing",
    "ambiguous",
    "unavailable",
  ].includes(status);
  const ready = ["ready", "routable", "exact", "provider_actual"].includes(status);
  return (
    <Badge variant={destructive ? "destructive" : ready ? "default" : "outline"}>
      {labels[status] ?? status}
    </Badge>
  );
}

function reasonLabel(reason: string) {
  return ({
    platform_route_missing: "缺少平台路由",
    routable_account_missing: "没有可执行账户",
    customer_price_missing: "缺少客户售价",
    customer_price_ambiguous: "售价解析冲突",
    pricing_admission_unsupported: "当前执行路径尚未接入 V4 计价",
    metering_contract_missing: "计量规则不完整",
    metering_contract_ambiguous: "计量规则冲突",
    metering_contract_incompatible: "计量规则与执行事实不兼容",
    metering_not_exact: "计量包含估算",
    provider_actual_price_missing: "上游会返回实际成本，但缺少归集规则",
  } as Record<string, string>)[reason] ?? reason;
}

function operationLabel(operation: string) {
  return ({
    "images.generations": "图片生成",
    "images.edits": "图片编辑",
    "videos.generations": "视频生成",
  } as Record<string, string>)[operation] ?? operation;
}

function pricingOperationLabel(operation: string) {
  return ({
    generation: "图片生成",
    edit: "图片编辑",
    video_generation: "视频生成",
  } as Record<string, string>)[operation] ?? operation;
}

function coverageKey(row: PricingCoverageRow) {
  return [
    row.provider_id,
    row.provider_model_id,
    row.operation,
    row.api_profile ?? "no-profile",
    row.public_model_id ?? "no-public-model",
  ].join(":");
}

function configurableSurfaceKey(row: PricingCoverageRow) {
  return [
    row.provider_id,
    row.provider_model_id,
    row.api_profile ?? "",
    row.public_model_id ?? "",
    row.pricing_operation ?? "",
  ].join(":");
}
