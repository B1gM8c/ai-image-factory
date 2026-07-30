"use client";

import { FormEvent, useState } from "react";
import { Filter, Search, X } from "lucide-react";
import { formatActivityStatus } from "@/components/activity-status-badge";
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
import { useI18n } from "@/i18n/locale-provider";

export type ActivityFilterValues = {
  q: string;
  provider: string;
  state: string;
  model: string;
  projectId: string;
  apiKeyId: string;
  window: string;
};

type ActivityFiltersProps = {
  value: ActivityFilterValues;
  providers: string[];
  states: string[];
  showProjectFilter?: boolean;
  disabled?: boolean;
  onChange: (value: ActivityFilterValues) => void;
  onSubmit: () => void;
  onClear: () => void;
};

export function ActivityFilters({
  value,
  providers,
  states,
  showProjectFilter = true,
  disabled = false,
  onChange,
  onSubmit,
  onClear,
}: ActivityFiltersProps) {
  const { t } = useI18n();
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const advancedCount = [
    value.model,
    showProjectFilter ? value.projectId : "",
    value.apiKeyId,
  ].filter(Boolean).length;
  const hasFilters = Boolean(
    value.q ||
    value.provider !== "all" ||
    value.state !== "all" ||
    advancedCount,
  );

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    onSubmit();
  }

  return (
    <form className="border-b" onSubmit={submit}>
      <div className="grid min-w-0 gap-2 p-3 md:grid-cols-2 2xl:grid-cols-[minmax(240px,1fr)_150px_170px_170px_auto]">
        <label className="relative min-w-0">
          <Search
            className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
            aria-hidden="true"
          />
          <span className="sr-only">
            {t({
              en: "Search by Request ID or Job ID",
              "zh-CN": "搜索 Request ID 或 Job ID",
              ja: "Request ID または Job ID で検索",
              ko: "Request ID 또는 Job ID로 검색",
            })}
          </span>
          <Input
            className="pl-9"
            value={value.q}
            onChange={(event) => onChange({ ...value, q: event.target.value })}
            placeholder={t({
              en: "Search by Request ID or Job ID",
              "zh-CN": "搜索 Request ID 或 Job ID",
              ja: "Request ID または Job ID で検索",
              ko: "Request ID 또는 Job ID로 검색",
            })}
            maxLength={255}
            disabled={disabled}
          />
        </label>
        <Select
          value={value.window}
          onValueChange={(window) => onChange({ ...value, window })}
          disabled={disabled}
        >
          <SelectTrigger
            aria-label={t({
              en: "Select time range",
              "zh-CN": "选择时间范围",
              ja: "期間を選択",
              ko: "기간 선택",
            })}
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="1h">
              {t({
                en: "Last hour",
                "zh-CN": "最近 1 小时",
                ja: "過去 1 時間",
                ko: "최근 1시간",
              })}
            </SelectItem>
            <SelectItem value="6h">
              {t({
                en: "Last 6 hours",
                "zh-CN": "最近 6 小时",
                ja: "過去 6 時間",
                ko: "최근 6시간",
              })}
            </SelectItem>
            <SelectItem value="24h">
              {t({
                en: "Last 24 hours",
                "zh-CN": "最近 24 小时",
                ja: "過去 24 時間",
                ko: "최근 24시간",
              })}
            </SelectItem>
            <SelectItem value="7d">
              {t({
                en: "Last 7 days",
                "zh-CN": "最近 7 天",
                ja: "過去 7 日間",
                ko: "최근 7일",
              })}
            </SelectItem>
            <SelectItem value="30d">
              {t({
                en: "Last 30 days",
                "zh-CN": "最近 30 天",
                ja: "過去 30 日間",
                ko: "최근 30일",
              })}
            </SelectItem>
            <SelectItem value="90d">
              {t({
                en: "Last 90 days",
                "zh-CN": "最近 90 天",
                ja: "過去 90 日間",
                ko: "최근 90일",
              })}
            </SelectItem>
          </SelectContent>
        </Select>
        <Select
          value={value.provider}
          onValueChange={(provider) => onChange({ ...value, provider })}
          disabled={disabled}
        >
          <SelectTrigger
            aria-label={t({
              en: "Select provider",
              "zh-CN": "选择 Provider",
              ja: "Provider を選択",
              ko: "Provider 선택",
            })}
          >
            <SelectValue
              placeholder={t({
                en: "All providers",
                "zh-CN": "全部 Provider",
                ja: "すべての Provider",
                ko: "모든 Provider",
              })}
            />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">
              {t({
                en: "All providers",
                "zh-CN": "全部 Provider",
                ja: "すべての Provider",
                ko: "모든 Provider",
              })}
            </SelectItem>
            {providers.map((provider) => (
              <SelectItem key={provider} value={provider}>
                {provider}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select
          value={value.state}
          onValueChange={(state) => onChange({ ...value, state })}
          disabled={disabled}
        >
          <SelectTrigger
            aria-label={t({
              en: "Select request status",
              "zh-CN": "选择请求状态",
              ja: "リクエスト状態を選択",
              ko: "요청 상태 선택",
            })}
          >
            <SelectValue
              placeholder={t({
                en: "All statuses",
                "zh-CN": "全部状态",
                ja: "すべての状態",
                ko: "모든 상태",
              })}
            />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">
              {t({
                en: "All statuses",
                "zh-CN": "全部状态",
                ja: "すべての状態",
                ko: "모든 상태",
              })}
            </SelectItem>
            {states.map((state) => (
              <SelectItem key={state} value={state}>
                {formatActivityStatus(t, state)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <div className="flex min-w-0 justify-end gap-2 md:col-span-2 2xl:col-span-1">
          <Button
            type="button"
            variant="outline"
            className="min-w-0 flex-1 sm:flex-none"
            onClick={() => setAdvancedOpen((open) => !open)}
            aria-expanded={advancedOpen}
          >
            <Filter aria-hidden="true" />
            {t({
              en: "More filters",
              "zh-CN": "更多筛选",
              ja: "その他のフィルター",
              ko: "추가 필터",
            })}
            {advancedCount > 0 ? (
              <Badge variant="secondary">{advancedCount}</Badge>
            ) : null}
          </Button>
          <Button type="submit" disabled={disabled}>
            <Search aria-hidden="true" />
            {t({
              en: "Search",
              "zh-CN": "查询",
              ja: "検索",
              ko: "검색",
            })}
          </Button>
        </div>
      </div>

      {advancedOpen ? (
        <div
          className={`grid gap-2 border-t bg-muted/20 p-3 ${
            showProjectFilter ? "md:grid-cols-3" : "md:grid-cols-2"
          }`}
        >
          <Input
            aria-label={t({
              en: "Filter by model",
              "zh-CN": "按模型筛选",
              ja: "モデルで絞り込む",
              ko: "모델로 필터링",
            })}
            value={value.model}
            onChange={(event) =>
              onChange({ ...value, model: event.target.value })
            }
            placeholder={t({
              en: "Model ID",
              "zh-CN": "模型 ID",
              ja: "Model ID",
              ko: "Model ID",
            })}
            maxLength={255}
            disabled={disabled}
          />
          {showProjectFilter ? (
            <Input
              aria-label={t({
                en: "Filter by Project ID",
                "zh-CN": "按项目 ID 筛选",
                ja: "Project ID で絞り込む",
                ko: "Project ID로 필터링",
              })}
              value={value.projectId}
              onChange={(event) =>
                onChange({ ...value, projectId: event.target.value })
              }
              placeholder={t({
                en: "Project ID",
                "zh-CN": "项目 ID",
                ja: "プロジェクト ID",
                ko: "프로젝트 ID",
              })}
              maxLength={128}
              disabled={disabled}
            />
          ) : null}
          <Input
            aria-label={t({
              en: "Filter by API Key ID",
              "zh-CN": "按 API Key ID 筛选",
              ja: "API Key ID で絞り込む",
              ko: "API Key ID로 필터링",
            })}
            value={value.apiKeyId}
            onChange={(event) =>
              onChange({ ...value, apiKeyId: event.target.value })
            }
            placeholder={t({
              en: "API key ID",
              "zh-CN": "API 密钥 ID",
              ja: "API キー ID",
              ko: "API 키 ID",
            })}
            maxLength={128}
            disabled={disabled}
          />
        </div>
      ) : null}

      {hasFilters ? (
        <div className="flex items-center justify-end border-t px-3 py-2">
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={onClear}
            disabled={disabled}
          >
            <X aria-hidden="true" />
            {t({
              en: "Clear filters",
              "zh-CN": "清除筛选",
              ja: "フィルターをクリア",
              ko: "필터 지우기",
            })}
          </Button>
        </div>
      ) : null}
    </form>
  );
}
