"use client";

import { FormEvent, useState } from "react";
import { Filter, Search, X } from "lucide-react";
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
import { formatStatus } from "@/lib/admin/format";

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
          <span className="sr-only">搜索 Request ID 或 Job ID</span>
          <Input
            className="pl-9"
            value={value.q}
            onChange={(event) => onChange({ ...value, q: event.target.value })}
            placeholder="搜索 Request ID 或 Job ID"
            maxLength={255}
            disabled={disabled}
          />
        </label>
        <Select
          value={value.window}
          onValueChange={(window) => onChange({ ...value, window })}
          disabled={disabled}
        >
          <SelectTrigger aria-label="选择时间范围">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="1h">最近 1 小时</SelectItem>
            <SelectItem value="6h">最近 6 小时</SelectItem>
            <SelectItem value="24h">最近 24 小时</SelectItem>
            <SelectItem value="7d">最近 7 天</SelectItem>
            <SelectItem value="30d">最近 30 天</SelectItem>
            <SelectItem value="90d">最近 90 天</SelectItem>
          </SelectContent>
        </Select>
        <Select
          value={value.provider}
          onValueChange={(provider) => onChange({ ...value, provider })}
          disabled={disabled}
        >
          <SelectTrigger aria-label="选择 Provider">
            <SelectValue placeholder="全部 Provider" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部 Provider</SelectItem>
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
          <SelectTrigger aria-label="选择请求状态">
            <SelectValue placeholder="全部状态" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部状态</SelectItem>
            {states.map((state) => (
              <SelectItem key={state} value={state}>
                {formatStatus(state)}
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
            更多筛选
            {advancedCount > 0 ? (
              <Badge variant="secondary">{advancedCount}</Badge>
            ) : null}
          </Button>
          <Button type="submit" disabled={disabled}>
            <Search aria-hidden="true" />
            查询
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
            aria-label="按模型筛选"
            value={value.model}
            onChange={(event) =>
              onChange({ ...value, model: event.target.value })
            }
            placeholder="模型 ID"
            maxLength={255}
            disabled={disabled}
          />
          {showProjectFilter ? (
            <Input
              aria-label="按项目 ID 筛选"
              value={value.projectId}
              onChange={(event) =>
                onChange({ ...value, projectId: event.target.value })
              }
              placeholder="Project ID"
              maxLength={128}
              disabled={disabled}
            />
          ) : null}
          <Input
            aria-label="按 API Key ID 筛选"
            value={value.apiKeyId}
            onChange={(event) =>
              onChange({ ...value, apiKeyId: event.target.value })
            }
            placeholder="API Key ID"
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
            清除筛选
          </Button>
        </div>
      ) : null}
    </form>
  );
}
