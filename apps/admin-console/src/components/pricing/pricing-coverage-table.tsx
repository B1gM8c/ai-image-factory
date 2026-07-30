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
import { useI18n } from "@/i18n/locale-provider";
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
  const { t } = useI18n();
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
        <SummaryValue value={query.data.summary.surfaces} label={t({ en: "API model surfaces", "zh-CN": "API 模型入口", ja: "API モデルサーフェス", ko: "API 모델 진입점" })} />
        <SummaryValue value={query.data.summary.routable_surfaces} label={t({ en: "Routable", "zh-CN": "可路由", ja: "ルーティング可能", ko: "라우팅 가능" })} />
        <SummaryValue value={query.data.summary.sale_priced_surfaces} label={t({ en: "Sale price configured", "zh-CN": "已配置售价", ja: "販売価格設定済み", ko: "판매가 설정됨" })} />
        <SummaryValue value={query.data.summary.actual_cost_surfaces} label={t({ en: "Actual cost attributable", "zh-CN": "实际成本可归集", ja: "実コスト集計可能", ko: "실제 비용 귀속 가능" })} />
        <SummaryValue value={query.data.summary.blocked_surfaces} label={t({ en: "Blockers", "zh-CN": "阻断项", ja: "ブロッカー", ko: "차단 항목" })} alert />
        <span className="ml-auto text-xs text-muted-foreground">
          {t({ en: "Platform base pricing contract", "zh-CN": "平台基础计价契约", ja: "プラットフォーム基本価格契約", ko: "플랫폼 기본 가격 계약" })} · {formatDateTime(query.data.as_of_ms)}
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
            placeholder={t({ en: "Search models, API profiles, or operations", "zh-CN": "搜索模型、API 协议或操作", ja: "モデル、API プロファイル、操作を検索", ko: "모델, API 프로필 또는 작업 검색" })}
            onChange={(event) => setSearch(event.target.value)}
          />
        </div>
        <Select value={provider} onValueChange={setProvider}>
          <SelectTrigger className="w-full lg:w-44"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">{t({ en: "All providers", "zh-CN": "全部供应商", ja: "すべてのプロバイダー", ko: "모든 공급자" })}</SelectItem>
            {providers.map(([id, label]) => (
              <SelectItem key={id} value={id}>{label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={readiness} onValueChange={setReadiness}>
          <SelectTrigger className="w-full lg:w-40"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">{t({ en: "All statuses", "zh-CN": "全部状态", ja: "すべてのステータス", ko: "모든 상태" })}</SelectItem>
            <SelectItem value="ready">{t({ en: "Ready", "zh-CN": "就绪", ja: "準備完了", ko: "준비됨" })}</SelectItem>
            <SelectItem value="warning">{t({ en: "Needs attention", "zh-CN": "需关注", ja: "要確認", ko: "확인 필요" })}</SelectItem>
            <SelectItem value="blocked">{t({ en: "Blocked", "zh-CN": "已阻断", ja: "ブロック済み", ko: "차단됨" })}</SelectItem>
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
          {t({ en: "Refresh", "zh-CN": "刷新", ja: "更新", ko: "새로고침" })}
        </Button>
      </div>

      <div className="min-w-0 overflow-hidden rounded-md border">
        <div className="overflow-x-auto">
          <Table className="min-w-[1060px]">
            <TableHeader>
              <TableRow>
                <TableHead className="pl-4">{t({ en: "Model", "zh-CN": "模型", ja: "モデル", ko: "모델" })}</TableHead>
                <TableHead>{t({ en: "API surface", "zh-CN": "API 入口", ja: "API サーフェス", ko: "API 진입점" })}</TableHead>
                <TableHead>{t({ en: "Routing", "zh-CN": "路由", ja: "ルーティング", ko: "라우팅" })}</TableHead>
                <TableHead>{t({ en: "Customer price", "zh-CN": "客户售价", ja: "顧客価格", ko: "고객 판매가" })}</TableHead>
                <TableHead>{t({ en: "Metering", "zh-CN": "计量", ja: "計量", ko: "계량" })}</TableHead>
                <TableHead>{t({ en: "Upstream cost", "zh-CN": "上游成本", ja: "アップストリームコスト", ko: "업스트림 비용" })}</TableHead>
                <TableHead className="pr-4">{t({ en: "Assessment", "zh-CN": "结论", ja: "判定", ko: "판정" })}</TableHead>
                {onConfigurePrice ? <TableHead className="w-28 pr-4 text-right">{t({ en: "Actions", "zh-CN": "操作", ja: "操作", ko: "작업" })}</TableHead> : null}
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
                    {t({ en: "No model surfaces match the filters", "zh-CN": "没有符合条件的模型入口", ja: "条件に一致するモデルサーフェスはありません", ko: "필터와 일치하는 모델 진입점이 없습니다" })}
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
  const { t } = useI18n();
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
          {row.public_model_id ?? t({ en: "Not bound", "zh-CN": "尚未绑定", ja: "未バインド", ko: "바인딩되지 않음" })}
        </span>
        <span className="mt-0.5 block max-w-56 truncate font-mono text-xs text-muted-foreground">
          {row.api_profile ?? t({ en: "Platform route missing", "zh-CN": "缺少平台路由", ja: "プラットフォームルートなし", ko: "플랫폼 경로 누락" })} · {operationLabel(row.operation, t)}
        </span>
        {row.pricing_operation && (
          <span className="mt-0.5 block text-xs text-muted-foreground">
            {t({ en: "Pricing operation {operation}", "zh-CN": "计价操作 {operation}", ja: "価格操作 {operation}", ko: "가격 작업 {operation}" }, { operation: pricingOperationLabel(row.pricing_operation, t) })}
          </span>
        )}
      </TableCell>
      <TableCell>
        <StatusBadge status={row.route_status} labels={{
          routable: t({ en: "Routable", "zh-CN": "可路由", ja: "ルーティング可能", ko: "라우팅 가능" }),
          unavailable: t({ en: "No available accounts", "zh-CN": "无可用账户", ja: "利用可能なアカウントなし", ko: "사용 가능한 계정 없음" }),
          missing: t({ en: "Not bound", "zh-CN": "未绑定", ja: "未バインド", ko: "바인딩되지 않음" }),
        }} />
        <span className="mt-1 block text-xs text-muted-foreground">
          {t({ en: "{count} executable accounts", "zh-CN": "{count} 个可执行账户", ja: "実行可能なアカウント {count} 件", ko: "실행 가능한 계정 {count}개" }, { count: row.routable_account_count })}
        </span>
      </TableCell>
      <TableCell>
        <StatusBadge status={row.customer_price_status} labels={{
          ready: t({ en: "Configured", "zh-CN": "已配置", ja: "設定済み", ko: "설정됨" }),
          ambiguous: t({ en: "Price conflict", "zh-CN": "价格冲突", ja: "価格競合", ko: "가격 충돌" }),
          missing: t({ en: "Sale price missing", "zh-CN": "缺少售价", ja: "販売価格なし", ko: "판매가 누락" }),
        }} />
        <span className="mt-1 block text-xs text-muted-foreground">
          {row.customer_price_currencies.length > 0
            ? row.customer_price_currencies.join(", ")
            : t({ en: "Requests cannot lock a quote", "zh-CN": "请求将无法冻结报价", ja: "リクエストで見積額を固定できません", ko: "요청에서 견적을 확정할 수 없습니다" })}
        </span>
      </TableCell>
      <TableCell>
        <StatusBadge status={row.metering_status} labels={{
          exact: t({ en: "Exact", "zh-CN": "精确", ja: "正確", ko: "정확" }),
          estimated: t({ en: "Includes estimates", "zh-CN": "含估算", ja: "推定を含む", ko: "추정 포함" }),
          ambiguous: t({ en: "Rule conflict", "zh-CN": "规则冲突", ja: "ルール競合", ko: "규칙 충돌" }),
          incompatible: t({ en: "Contract incompatible", "zh-CN": "契约不兼容", ja: "契約非互換", ko: "계약 비호환" }),
          missing: t({ en: "Rules missing", "zh-CN": "缺少规则", ja: "ルールなし", ko: "규칙 누락" }),
        }} />
      </TableCell>
      <TableCell>
        <StatusBadge status={row.provider_cost_status} labels={{
          provider_actual: t({ en: "Actual cost", "zh-CN": "实际成本", ja: "実コスト", ko: "실제 비용" }),
          provider_allocated: t({ en: "Subscription allocation", "zh-CN": "订阅分摊", ja: "サブスクリプション配賦", ko: "구독 배분" }),
          provider_estimated: t({ en: "Estimated cost", "zh-CN": "估算成本", ja: "推定コスト", ko: "추정 비용" }),
          benchmark_only: t({ en: "Official benchmark only", "zh-CN": "仅官方基准", ja: "公式ベンチマークのみ", ko: "공식 벤치마크만" }),
          actual_price_missing: t({ en: "Actual cost rule missing", "zh-CN": "缺少实际成本规则", ja: "実コストルールなし", ko: "실제 비용 규칙 누락" }),
          not_emitted: t({ en: "Not provided upstream", "zh-CN": "上游不提供", ja: "アップストリーム未提供", ko: "업스트림에서 제공하지 않음" }),
          ambiguous: t({ en: "Cost conflict", "zh-CN": "成本冲突", ja: "コスト競合", ko: "비용 충돌" }),
        }} />
        <span className="mt-1 block text-xs text-muted-foreground">
          {row.provider_cost_currencies.length > 0
            ? row.provider_cost_currencies.join(", ")
            : t({ en: "Does not block requests; affects gross margin", "zh-CN": "不影响请求，影响毛利", ja: "リクエストには影響せず、粗利益に影響", ko: "요청에는 영향이 없고 매출총이익에 영향" })}
        </span>
      </TableCell>
      <TableCell className="pr-4">
        <StatusBadge status={row.readiness} labels={{
          ready: t({ en: "Base contract billable", "zh-CN": "基础契约可结算", ja: "基本契約で決済可能", ko: "기본 계약 청구 가능" }),
          warning: t({ en: "Base contract usable", "zh-CN": "基础契约可用", ja: "基本契約を使用可能", ko: "기본 계약 사용 가능" }),
          blocked: t({ en: "Base contract blocked", "zh-CN": "基础契约阻断", ja: "基本契約がブロック", ko: "기본 계약 차단됨" }),
        }} />
        {row.blocking_reasons.length > 0 ? (
          <span className="mt-1 block max-w-52 text-xs text-muted-foreground">
            {row.blocking_reasons.map((reason) => reasonLabel(reason, t)).join("; ")}
          </span>
        ) : (
          <span className="mt-1 block text-xs text-muted-foreground">
            {row.source_status === "verified"
              ? t({ en: "Source verifiable", "zh-CN": "来源可复核", ja: "出典を検証可能", ko: "출처 검증 가능" })
              : t({ en: "Includes manually maintained sources", "zh-CN": "含人工维护来源", ja: "手動管理の出典を含む", ko: "수동 관리 출처 포함" })}
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
            {row.customer_price_status === "ready"
              ? t({ en: "Adjust", "zh-CN": "调整", ja: "調整", ko: "조정" })
              : t({ en: "Set price", "zh-CN": "定价", ja: "価格設定", ko: "가격 설정" })}
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

function reasonLabel(reason: string, t: ReturnType<typeof useI18n>["t"]) {
  return ({
    platform_route_missing: t({ en: "Platform route missing", "zh-CN": "缺少平台路由", ja: "プラットフォームルートなし", ko: "플랫폼 경로 누락" }),
    routable_account_missing: t({ en: "No executable accounts", "zh-CN": "没有可执行账户", ja: "実行可能なアカウントなし", ko: "실행 가능한 계정 없음" }),
    customer_price_missing: t({ en: "Customer sale price missing", "zh-CN": "缺少客户售价", ja: "顧客販売価格なし", ko: "고객 판매가 누락" }),
    customer_price_ambiguous: t({ en: "Sale price resolution conflict", "zh-CN": "售价解析冲突", ja: "販売価格の解決競合", ko: "판매가 해석 충돌" }),
    pricing_admission_unsupported: t({ en: "This execution path is not yet connected to V4 pricing", "zh-CN": "当前执行路径尚未接入 V4 计价", ja: "この実行パスは V4 価格設定に未接続です", ko: "현재 실행 경로는 아직 V4 가격 책정에 연결되지 않았습니다" }),
    metering_contract_missing: t({ en: "Metering contract incomplete", "zh-CN": "计量规则不完整", ja: "計量契約が不完全", ko: "계량 계약 불완전" }),
    metering_contract_ambiguous: t({ en: "Metering rule conflict", "zh-CN": "计量规则冲突", ja: "計量ルール競合", ko: "계량 규칙 충돌" }),
    metering_contract_incompatible: t({ en: "Metering contract conflicts with execution facts", "zh-CN": "计量规则与执行事实不兼容", ja: "計量契約が実行結果と非互換", ko: "계량 계약이 실행 사실과 호환되지 않음" }),
    metering_not_exact: t({ en: "Metering includes estimates", "zh-CN": "计量包含估算", ja: "計量に推定を含む", ko: "계량에 추정 포함" }),
    provider_actual_price_missing: t({ en: "Upstream reports actual cost, but no attribution rule is configured", "zh-CN": "上游会返回实际成本，但缺少归集规则", ja: "アップストリームは実コストを返しますが、集計ルールがありません", ko: "업스트림에서 실제 비용을 반환하지만 귀속 규칙이 없습니다" }),
  } as Record<string, string>)[reason] ?? reason;
}

function operationLabel(operation: string, t: ReturnType<typeof useI18n>["t"]) {
  return ({
    "images.generations": t({ en: "Image generation", "zh-CN": "图片生成", ja: "画像生成", ko: "이미지 생성" }),
    "images.edits": t({ en: "Image editing", "zh-CN": "图片编辑", ja: "画像編集", ko: "이미지 편집" }),
    "videos.generations": t({ en: "Video generation", "zh-CN": "视频生成", ja: "動画生成", ko: "동영상 생성" }),
  } as Record<string, string>)[operation] ?? operation;
}

function pricingOperationLabel(operation: string, t: ReturnType<typeof useI18n>["t"]) {
  return ({
    generation: t({ en: "Image generation", "zh-CN": "图片生成", ja: "画像生成", ko: "이미지 생성" }),
    edit: t({ en: "Image editing", "zh-CN": "图片编辑", ja: "画像編集", ko: "이미지 편집" }),
    video_generation: t({ en: "Video generation", "zh-CN": "视频生成", ja: "動画生成", ko: "동영상 생성" }),
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
