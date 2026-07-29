"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { ImageIcon, KeyRound, LoaderCircle, RefreshCw, Search, Video } from "lucide-react";
import { toast } from "sonner";
import { AdminQueryError, AdminQuerySkeleton } from "@/components/admin-query-state";
import { PageHeader } from "@/components/page-header";
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
  ProviderAccountsSnapshot,
  ProviderModelRefresh,
  ProviderModelsSnapshot,
  ProviderModelView,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";
import { useConsoleSession } from "@/components/auth/console-session-provider";

const MODELS_ENDPOINT = "/v1/console/provider-models";
const ACCOUNTS_ENDPOINT = "/admin/v1/provider-accounts";

export function ProviderCatalogView() {
  const { capabilities } = useConsoleSession();
  const canManageProviders = capabilities.includes("providers:manage");
  const modelsQuery = useAdminQuery<ProviderModelsSnapshot>(MODELS_ENDPOINT);
  const accountsQuery = useAdminQuery<ProviderAccountsSnapshot>(
    ACCOUNTS_ENDPOINT,
    canManageProviders,
  );
  const [refreshingProviders, setRefreshingProviders] = useState<Set<string>>(new Set());
  const [search, setSearch] = useState("");
  const [provider, setProvider] = useState("all");
  const [mediaKind, setMediaKind] = useState("all");
  const [availability, setAvailability] = useState("all");
  const activeAccounts = useMemo(
    () => (accountsQuery.data?.accounts ?? []).filter(
      (account) => account.environment_state === "active" && account.account_state === "enabled",
    ),
    [accountsQuery.data],
  );

  async function refreshModels() {
    const targets = activeAccounts;
    if (targets.length === 0) {
      toast.error("请先添加可用的 CLI 账户");
      return;
    }
    const providers = new Set(targets.map((account) => account.provider_id));
    setRefreshingProviders((current) => new Set([...current, ...providers]));
    const results = await Promise.allSettled(targets.map(async (account) => {
      const started = await startRefresh(account.provider_account_id);
      return waitForRefresh(started.refresh_id);
    }));
    setRefreshingProviders((current) => {
      const next = new Set(current);
      for (const provider of providers) next.delete(provider);
      return next;
    });
    modelsQuery.retry();
    const succeeded = results.filter((result) => result.status === "fulfilled").length;
    if (succeeded === results.length) {
      toast.success(`已更新 ${succeeded.toLocaleString("zh-CN")} 个账户的模型目录`);
    } else if (succeeded > 0) {
      toast.warning(`已更新 ${succeeded.toLocaleString("zh-CN")} 个账户，另有 ${results.length - succeeded} 个失败`);
    } else {
      toast.error("模型目录更新失败，请检查账户授权或 CLI 状态");
    }
  }

  const refreshingAll = refreshingProviders.size > 0;
  const error = modelsQuery.error ?? (canManageProviders ? accountsQuery.error : null);
  const providers = useMemo(() => {
    const unique = new Map<string, string>();
    for (const model of modelsQuery.data?.models ?? []) {
      unique.set(model.provider_id, model.provider_display_name);
    }
    return [...unique.entries()];
  }, [modelsQuery.data]);
  const filteredModels = useMemo(() => {
    const query = search.trim().toLocaleLowerCase();
    return (modelsQuery.data?.models ?? []).filter((model) => {
      if (provider !== "all" && model.provider_id !== provider) return false;
      if (mediaKind !== "all" && model.media_kind !== mediaKind) return false;
      if (availability !== "all" && model.availability !== availability) return false;
      if (!query) return true;
      return model.display_name.toLocaleLowerCase().includes(query)
        || model.model_id.toLocaleLowerCase().includes(query);
    });
  }, [availability, mediaKind, modelsQuery.data, provider, search]);

  return (
    <div className="min-w-0 space-y-6">
      <PageHeader
        title="模型与能力"
        actions={canManageProviders ? (
          <>
            <Button asChild variant="outline">
              <Link href="/provider-accounts">
                <KeyRound aria-hidden="true" />
                CLI 账户
              </Link>
            </Button>
            <Button onClick={() => void refreshModels()} disabled={refreshingAll || accountsQuery.loading}>
              {refreshingAll
                ? <LoaderCircle className="animate-spin" aria-hidden="true" />
                : <RefreshCw aria-hidden="true" />}
              从 CLI 更新
            </Button>
          </>
        ) : undefined}
      />

      <div className="grid gap-3 border-y py-4 md:grid-cols-[minmax(14rem,1fr)_12rem_10rem_12rem_auto]">
        <div className="relative">
          <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" aria-hidden="true" />
          <Input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="搜索模型" className="pl-9" />
        </div>
        <Select value={provider} onValueChange={setProvider}>
          <SelectTrigger aria-label="供应商"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部供应商</SelectItem>
            {providers.map(([value, label]) => <SelectItem key={value} value={value}>{label}</SelectItem>)}
          </SelectContent>
        </Select>
        <Select value={mediaKind} onValueChange={setMediaKind}>
          <SelectTrigger aria-label="模型类型"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部类型</SelectItem>
            <SelectItem value="image">图片</SelectItem>
            <SelectItem value="video">视频</SelectItem>
          </SelectContent>
        </Select>
        <Select value={availability} onValueChange={setAvailability}>
          <SelectTrigger aria-label="可用状态"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部状态</SelectItem>
            <SelectItem value="routable">可调用</SelectItem>
            <SelectItem value="observed">已发现</SelectItem>
            <SelectItem value="unobserved">待观测</SelectItem>
            <SelectItem value="not_supported">待适配</SelectItem>
          </SelectContent>
        </Select>
        <span className="self-center whitespace-nowrap text-sm tabular-nums text-muted-foreground">
          {filteredModels.length.toLocaleString("zh-CN")} 个模型
        </span>
      </div>

      {modelsQuery.loading ? <AdminQuerySkeleton rows={8} /> : null}
      {error && !modelsQuery.data ? (
        <AdminQueryError error={error} retry={() => { modelsQuery.retry(); accountsQuery.retry(); }} />
      ) : null}
      {modelsQuery.data ? (
        <ModelTable models={filteredModels} showOperational={canManageProviders} />
      ) : null}
    </div>
  );
}

function ModelTable({
  models,
  showOperational,
}: {
  models: ProviderModelView[];
  showOperational: boolean;
}) {
  if (models.length === 0) {
    return <div className="border-y py-12 text-center text-sm text-muted-foreground">没有匹配的模型</div>;
  }
  return (
    <div className="border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">供应商</TableHead>
            <TableHead>模型</TableHead>
            <TableHead>类型</TableHead>
            <TableHead>API 能力</TableHead>
            <TableHead>{showOperational ? "账户观测" : "工作区可用性"}</TableHead>
            <TableHead>生产状态</TableHead>
            <TableHead>最后更新</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {models.map((model) => (
            <TableRow key={`${model.provider_id}:${model.media_kind}:${model.model_id}`}>
              <TableCell className="pl-4 font-medium">{model.provider_display_name}</TableCell>
              <TableCell className="min-w-64 whitespace-normal">
                <p className="font-medium">{model.display_name}</p>
                <p className="mt-0.5 break-all font-mono text-xs text-muted-foreground">{model.model_id}</p>
                {model.latest_cli_version ? (
                  <p className="mt-1 text-xs text-muted-foreground">{model.latest_cli_version}</p>
                ) : null}
              </TableCell>
              <TableCell><MediaBadge media={model.media_kind} /></TableCell>
              <TableCell className="whitespace-normal">
                <div className="flex min-w-44 flex-wrap gap-1.5">
                  {model.operation_ids.length > 0
                    ? model.operation_ids.map((operation) => (
                      <Badge key={operation} variant="outline">{operationLabel(operation)}</Badge>
                    ))
                    : <span className="text-muted-foreground">待映射</span>}
                </div>
              </TableCell>
              <TableCell>
                {showOperational
                  ? <span>{(model.observed_account_count ?? 0).toLocaleString("zh-CN")} 个账户</span>
                  : <span className="text-muted-foreground">平台路由</span>}
              </TableCell>
              <TableCell><AvailabilityBadge model={model} showOperational={showOperational} /></TableCell>
              <TableCell className="text-muted-foreground">
                {formatDateTime(model.last_successful_refresh_at_ms ?? model.last_observed_at_ms)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function MediaBadge({ media }: { media: ProviderModelView["media_kind"] }) {
  const Icon = media === "image" ? ImageIcon : Video;
  return (
    <Badge variant="secondary">
      <Icon aria-hidden="true" />
      {media === "image" ? "图片" : "视频"}
    </Badge>
  );
}

function AvailabilityBadge({
  model,
  showOperational,
}: {
  model: ProviderModelView;
  showOperational: boolean;
}) {
  if (model.availability === "routable") {
    return (
      <Badge>
        {showOperational
          ? `可调用 · ${(model.routable_account_count ?? 0).toLocaleString("zh-CN")} 个账户`
          : "可调用"}
      </Badge>
    );
  }
  if (model.availability === "observed") return <Badge variant="secondary">已发现</Badge>;
  if (model.availability === "not_supported") return <Badge variant="outline">待适配</Badge>;
  return <Badge variant="outline">待 CLI 观测</Badge>;
}

function operationLabel(operation: string) {
  return ({
    "images.generations": "图片生成",
    "images.edits": "图片编辑",
    "videos.generations": "视频生成",
  } as Record<string, string>)[operation] ?? operation;
}

async function startRefresh(providerAccountId: string) {
  const response = await consoleFetch(
    `/api/gateway/admin/v1/provider-accounts/${providerAccountId}/model-refreshes`,
    { method: "POST", body: "{}" },
  );
  if (!response.ok) throw new Error("模型刷新无法启动");
  return (await response.json()) as ProviderModelRefresh;
}

async function waitForRefresh(refreshId: string) {
  for (let attempt = 0; attempt < 120; attempt += 1) {
    const response = await consoleFetch(
      `/api/gateway/admin/v1/provider-model-refreshes/${refreshId}`,
    );
    if (!response.ok) throw new Error("模型刷新状态不可用");
    const refresh = (await response.json()) as ProviderModelRefresh;
    if (refresh.status === "succeeded") return refresh;
    if (refresh.status === "failed") throw new Error(refresh.error_code ?? "模型刷新失败");
    await delay(1_000);
  }
  throw new Error("模型刷新超时");
}

function delay(milliseconds: number) {
  return new Promise<void>((resolve) => window.setTimeout(resolve, milliseconds));
}
