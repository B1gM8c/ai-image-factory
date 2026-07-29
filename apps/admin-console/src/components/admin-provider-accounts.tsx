"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  CloudUpload,
  Clock3,
  Copy,
  ExternalLink,
  Gauge,
  ImageIcon,
  KeyRound,
  Layers3,
  LoaderCircle,
  MoreHorizontal,
  Plus,
  RefreshCw,
  Settings2,
  TerminalSquare,
  Users,
  Video,
} from "lucide-react";
import { toast } from "sonner";
import {
  AdminQueryError,
  AdminQuerySkeleton,
} from "@/components/admin-query-state";
import { MetricCard } from "@/components/metric-card";
import { PageHeader } from "@/components/page-header";
import {
  AccountSchedulingSheet,
  type AccountSettingsTab,
} from "@/components/provider-accounts/account-scheduling-sheet";
import { RoutePolicySheet } from "@/components/provider-accounts/route-policy-sheet";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
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
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useAdminQuery } from "@/hooks/use-admin-query";
import { formatDateTime, formatInteger, sumIntegers } from "@/lib/admin/format";
import type {
  ManagedCliProviderCapability,
  ManagedCliProvidersSnapshot,
  ProviderAccountView,
  ProviderAccountConcurrency,
  ProviderAccountRuntimeEvent,
  ProviderQueuePressure,
  ProviderAccountsSnapshot,
  ProviderLoginSession,
  ProviderModelView,
  ProviderModelsSnapshot,
  ProviderRoute,
  ProviderRoutesSnapshot,
  UpstreamQuotaWindow,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

const ACCOUNTS_ENDPOINT = "/admin/v1/provider-accounts";
const ROUTES_ENDPOINT = "/admin/v1/provider-routes";
const MANAGED_PROVIDERS_ENDPOINT = "/admin/v1/managed-cli-providers";
const PROVIDER_MODELS_ENDPOINT = "/admin/v1/provider-models";
const ACCOUNT_RUNTIME_EVENTS_ENDPOINT =
  "/api/gateway/admin/v1/provider-account-runtime-events";
const EMPTY_QUEUE_PRESSURE: ProviderQueuePressure = {
  queued_work_items: "0",
  pending_batch_requests: "0",
};

export function AdminProviderAccounts() {
  const accountsQuery =
    useAdminQuery<ProviderAccountsSnapshot>(ACCOUNTS_ENDPOINT);
  const routesQuery = useAdminQuery<ProviderRoutesSnapshot>(ROUTES_ENDPOINT);
  const providersQuery = useAdminQuery<ManagedCliProvidersSnapshot>(
    MANAGED_PROVIDERS_ENDPOINT,
  );
  const modelsQuery = useAdminQuery<ProviderModelsSnapshot>(
    PROVIDER_MODELS_ENDPOINT,
  );
  const [loginOpen, setLoginOpen] = useState(false);
  const [routePolicyOpen, setRoutePolicyOpen] = useState(false);
  const [editingRoute, setEditingRoute] = useState<ProviderRoute | null>(null);
  const [loginSession, setLoginSession] = useState<ProviderLoginSession | null>(
    null,
  );
  const [reauthorizingAccount, setReauthorizingAccount] =
    useState<ProviderAccountView | null>(null);
  const [runtimeAccounts, setRuntimeAccounts] = useState<
    Record<string, ProviderAccountConcurrency>
  >({});
  const [runtimeQueue, setRuntimeQueue] =
    useState<ProviderQueuePressure>(EMPTY_QUEUE_PRESSURE);
  const runtimeSequence = useRef(-1);
  const runtimeAsOf = useRef(-1);

  const accounts = useMemo(
    () =>
      deduplicateAccounts(accountsQuery.data?.accounts ?? []).map((account) => {
        const runtime = runtimeAccounts[account.provider_account_id];
        return runtime ? { ...account, ...runtime } : account;
      }),
    [accountsQuery.data, runtimeAccounts],
  );
  const managedAccounts = accounts.filter(
    (account) => account.environment_state !== null,
  );
  const routableAccounts = managedAccounts.filter(
    (account) =>
      account.environment_state === "active" &&
      account.account_state === "enabled" &&
      account.credential_pool_state === "enabled" &&
      account.profile_state === "enabled",
  );
  const routes = useMemo(
    () => routesQuery.data?.routes ?? [],
    [routesQuery.data],
  );
  const groups = useMemo(
    () => routes.filter((route) => route.route_kind === "group"),
    [routes],
  );
  const accountRoutes = useMemo(
    () => routes.filter((route) => route.route_kind === "account"),
    [routes],
  );
  const canCreateGroup = [
    ...new Set(routableAccounts.map((account) => account.provider_id)),
  ].some(
    (providerId) =>
      routableAccounts.filter((account) => account.provider_id === providerId)
        .length >= 2,
  );

  useEffect(() => {
    const source = new EventSource(ACCOUNT_RUNTIME_EVENTS_ENDPOINT);
    source.onmessage = (message) => {
      let event: ProviderAccountRuntimeEvent;
      try {
        event = JSON.parse(message.data) as ProviderAccountRuntimeEvent;
      } catch {
        return;
      }
      if (event.kind === "resync_required") {
        setRuntimeAccounts({});
        setRuntimeQueue(EMPTY_QUEUE_PRESSURE);
        accountsQuery.retry();
        return;
      }
      if (event.kind === "delta" && event.sequence <= runtimeSequence.current)
        return;
      if (event.as_of_ms < runtimeAsOf.current) return;
      runtimeSequence.current = Math.max(
        runtimeSequence.current,
        event.sequence,
      );
      runtimeAsOf.current = Math.max(runtimeAsOf.current, event.as_of_ms);
      setRuntimeQueue(event.queue ?? EMPTY_QUEUE_PRESSURE);
      setRuntimeAccounts((current) => {
        if (event.kind === "snapshot") {
          return Object.fromEntries(
            event.accounts.map((account) => [
              account.provider_account_id,
              account,
            ]),
          );
        }
        const next = { ...current };
        for (const account of event.accounts)
          next[account.provider_account_id] = account;
        return next;
      });
    };
    return () => source.close();
  }, [accountsQuery.retry]);

  useEffect(() => {
    if (loginSession?.status !== "succeeded") return;
    setLoginSession(null);
    setLoginOpen(false);
    toast.success(
      reauthorizingAccount
        ? "账户已重新授权"
        : `${providerLabel(loginSession.provider_id)} 账号已添加`,
    );
    setReauthorizingAccount(null);
    accountsQuery.retry();
    routesQuery.retry();
  }, [loginSession, reauthorizingAccount, accountsQuery, routesQuery]);

  useEffect(() => {
    if (
      !loginSession ||
      !["starting", "waiting_for_user", "validating"].includes(
        loginSession.status,
      )
    )
      return;
    let active = true;
    const timer = window.setInterval(async () => {
      try {
        const response = await consoleFetch(
          `/api/gateway/admin/v1/provider-account-login-sessions/${loginSession.login_session_id}`,
        );
        if (!response.ok || !active) return;
        const next = (await response.json()) as ProviderLoginSession;
        setLoginSession(next);
        if (next.status === "failed" || next.status === "expired") {
          toast.error(
            `${providerLabel(next.provider_id)} 登录未完成，请重新发起`,
          );
        }
      } catch {
        // Polling is best-effort; the next tick retries without duplicating login.
      }
    }, 2_000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [loginSession, accountsQuery, routesQuery]);

  const loading =
    accountsQuery.loading || routesQuery.loading || modelsQuery.loading;
  const hardError =
    accountsQuery.error ?? routesQuery.error ?? modelsQuery.error;
  const retry = () => {
    accountsQuery.retry();
    routesQuery.retry();
    modelsQuery.retry();
  };

  return (
    <div className="min-w-0 space-y-6">
      <PageHeader
        title="CLI 账号与额度"
        description="添加独立账号、查看上游额度，并把多个账号组成可分配给 API Key 的账号组"
        actions={
          <>
            <Button
              variant="outline"
              onClick={() => {
                setEditingRoute(null);
                setRoutePolicyOpen(true);
              }}
              disabled={!canCreateGroup}
              title={
                !canCreateGroup ? "同一供应商至少需要两个可用账号" : undefined
              }
            >
              <Users aria-hidden="true" />
              新建账号组
            </Button>
            <Button
              onClick={() => {
                setReauthorizingAccount(null);
                setLoginSession(null);
                setLoginOpen(true);
              }}
            >
              <Plus aria-hidden="true" />
              添加 CLI 账户
            </Button>
          </>
        }
      />

      {loading ? <AdminQuerySkeleton rows={6} /> : null}
      {!loading &&
      hardError &&
      (!accountsQuery.data || !routesQuery.data || !modelsQuery.data) ? (
        <AdminQueryError error={hardError} retry={retry} />
      ) : null}
      {accountsQuery.data && routesQuery.data && modelsQuery.data ? (
        <AccountsContent
          accounts={managedAccounts}
          queue={runtimeQueue}
          groups={groups}
          accountRoutes={accountRoutes}
          models={modelsQuery.data.models}
          refreshing={
            accountsQuery.refreshing ||
            routesQuery.refreshing ||
            modelsQuery.refreshing
          }
          retry={retry}
          onEditRoute={(route) => {
            setEditingRoute(route);
            setRoutePolicyOpen(true);
          }}
          onReauthorize={(account) => {
            setReauthorizingAccount(account);
            setLoginSession(null);
            setLoginOpen(true);
          }}
        />
      ) : null}

      <AddCliAccountDialog
        open={loginOpen}
        onOpenChange={(open) => {
          setLoginOpen(open);
          if (!open) {
            setLoginSession(null);
            setReauthorizingAccount(null);
          }
        }}
        session={loginSession}
        onSession={setLoginSession}
        providers={providersQuery.data?.providers ?? []}
        account={reauthorizingAccount}
      />
      <RoutePolicySheet
        open={routePolicyOpen}
        onOpenChange={(open) => {
          setRoutePolicyOpen(open);
          if (!open) setEditingRoute(null);
        }}
        accounts={editingRoute ? managedAccounts : routableAccounts}
        models={modelsQuery.data?.models ?? []}
        route={editingRoute}
        onSaved={() => routesQuery.retry()}
      />
    </div>
  );
}

function AccountsContent({
  accounts,
  queue,
  groups,
  accountRoutes,
  models,
  refreshing,
  retry,
  onEditRoute,
  onReauthorize,
}: {
  accounts: ProviderAccountView[];
  queue: ProviderQueuePressure;
  groups: ProviderRoute[];
  accountRoutes: ProviderRoute[];
  models: ProviderModelView[];
  refreshing: boolean;
  retry: () => void;
  onEditRoute: (route: ProviderRoute) => void;
  onReauthorize: (account: ProviderAccountView) => void;
}) {
  const [selectedAccount, setSelectedAccount] =
    useState<ProviderAccountView | null>(null);
  const [selectedAccountTab, setSelectedAccountTab] =
    useState<AccountSettingsTab>("scheduling");
  const schedulableAccounts = accounts.filter(isSchedulableAccount);
  const allocated = sumIntegers(
    schedulableAccounts.map((account) => account.allocated_count),
  );
  const totalConcurrency = sumIntegers(
    schedulableAccounts.map((account) => account.max_concurrency),
  );
  const available = sumIntegers(
    schedulableAccounts.map((account) => account.available_capacity),
  );
  const queued = sumIntegers([
    queue.queued_work_items,
    queue.pending_batch_requests,
  ]);
  const observed = accounts.filter(
    (account) => account.upstream_quota.status === "observed",
  ).length;
  const selectedAccountRoutes = useMemo(
    () =>
      accountRoutes.filter((route) =>
        route.members.some(
          (member) =>
            member.provider_account_id === selectedAccount?.provider_account_id,
        ),
      ),
    [accountRoutes, selectedAccount?.provider_account_id],
  );

  return (
    <>
      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label="受管 CLI 账号"
          value={formatInteger(accounts.length.toString())}
          detail="每个账号使用独立凭据环境"
          icon={TerminalSquare}
        />
        <MetricCard
          label="账号组"
          value={formatInteger(groups.length.toString())}
          detail="可直接分配给 API Key"
          icon={Layers3}
          tone="info"
        />
        <MetricCard
          label="执行并发"
          value={`${formatInteger(allocated)} / ${formatInteger(totalConcurrency)}`}
          detail={
            queued === "0"
              ? `已执行 / 上限 · 可用 ${formatInteger(available)} · 无等待任务`
              : `等待 ${formatInteger(queued)} · Batch 待分发 ${formatInteger(
                  queue.pending_batch_requests,
                )}`
          }
          icon={Gauge}
          tone="success"
        />
        <MetricCard
          label="额度已同步"
          value={`${observed} / ${accounts.length}`}
          detail="额度由 CLI 上游实时观测"
          icon={Clock3}
        />
      </section>

      <div className="flex min-h-9 items-center justify-end">
        <Button
          variant="outline"
          size="sm"
          onClick={retry}
          disabled={refreshing}
        >
          {refreshing ? (
            <LoaderCircle className="animate-spin" aria-hidden="true" />
          ) : (
            <RefreshCw aria-hidden="true" />
          )}
          刷新
        </Button>
      </div>

      <Tabs defaultValue="accounts">
        <TabsList>
          <TabsTrigger value="accounts">账号</TabsTrigger>
          <TabsTrigger value="groups">账号组</TabsTrigger>
        </TabsList>
        <TabsContent value="accounts" className="mt-4">
          {accounts.length === 0 ? (
            <EmptyState
              title="还没有 CLI 账号"
              description="添加 Codex、Grok 或即梦账号后，可以在这里统一管理运行能力与上游额度。"
            />
          ) : (
            <TooltipProvider delayDuration={300}>
              <div className="overflow-hidden rounded-md border">
                <Table className="table-fixed">
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-[36%] pl-4 md:w-[25%]">
                        账号
                      </TableHead>
                      <TableHead className="hidden w-[24%] md:table-cell">
                        状态
                      </TableHead>
                      <TableHead className="hidden w-[31%] lg:table-cell">
                        上游额度 / 积分
                      </TableHead>
                      <TableHead className="w-[42%] md:w-36">
                        执行并发
                      </TableHead>
                      <TableHead className="w-14 pr-4 text-right">
                        操作
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {accounts.map((account) => (
                      <AccountTableRow
                        key={account.provider_account_id}
                        account={account}
                        onRefresh={retry}
                        onManage={() => {
                          setSelectedAccountTab("scheduling");
                          setSelectedAccount(account);
                        }}
                        onManageVideoStorage={() => {
                          setSelectedAccountTab("video-storage");
                          setSelectedAccount(account);
                        }}
                        onReauthorize={() => onReauthorize(account)}
                      />
                    ))}
                  </TableBody>
                </Table>
              </div>
            </TooltipProvider>
          )}
        </TabsContent>
        <TabsContent value="groups" className="mt-4">
          {groups.length === 0 ? (
            <EmptyState
              title="还没有账号组"
              description="把多个同能力账号加入一组，创建 API Key 时可直接选择该组。"
            />
          ) : (
            <div className="border">
              <Table className="min-w-[1080px]">
                <TableHeader>
                  <TableRow>
                    <TableHead className="pl-4">账号组</TableHead>
                    <TableHead>成员</TableHead>
                    <TableHead>能力</TableHead>
                    <TableHead>对外模型</TableHead>
                    <TableHead>调度策略</TableHead>
                    <TableHead>额度保护</TableHead>
                    <TableHead>状态</TableHead>
                    <TableHead className="pr-4">创建时间</TableHead>
                    <TableHead className="w-14 pr-4 text-right">操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {groups.map((group) => (
                    <GroupTableRow
                      key={group.route_id}
                      group={group}
                      onEdit={() => onEditRoute(group)}
                    />
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </TabsContent>
      </Tabs>
      <AccountSchedulingSheet
        account={selectedAccount}
        open={Boolean(selectedAccount)}
        initialTab={selectedAccountTab}
        onOpenChange={(open) => {
          if (!open) setSelectedAccount(null);
        }}
        groups={groups}
        routes={selectedAccountRoutes}
        models={models}
        onSaved={retry}
      />
    </>
  );
}

function AccountTableRow({
  account,
  onRefresh,
  onManage,
  onManageVideoStorage,
  onReauthorize,
}: {
  account: ProviderAccountView;
  onRefresh: () => void;
  onManage: () => void;
  onManageVideoStorage: () => void;
  onReauthorize: () => void;
}) {
  const [refreshing, setRefreshing] = useState(false);
  const currentWindows =
    account.upstream_quota.status === "observed"
      ? account.upstream_quota.windows
      : [];
  const fiveHour = findWindow(currentWindows, 300);
  const weekly = findWindow(currentWindows, 10_080);
  const canRefreshQuota =
    (account.environment_state === "active" ||
      (account.provider_id === "dreamina-cli" &&
        account.environment_state === "disabled")) &&
    (account.provider_id === "openai-codex" ||
      account.provider_id === "grok-cli" ||
      account.provider_id === "dreamina-cli");
  const quotaRefreshLabel =
    account.provider_id === "grok-cli"
      ? "刷新每周额度"
      : account.provider_id === "dreamina-cli"
        ? "刷新积分余额"
        : "刷新 5 小时与每周额度";
  const canReauthorize =
    account.environment_state !== null &&
    (account.provider_id === "openai-codex" ||
      account.provider_id === "grok-cli" ||
      account.provider_id === "dreamina-cli");
  const reauthorizationRequired =
    account.credential_lifecycle_state === "reauth_required" ||
    account.environment_state === "invalid";

  async function refreshQuota() {
    setRefreshing(true);
    try {
      const response = await consoleFetch(
        `/api/gateway/admin/v1/provider-accounts/${account.provider_account_id}/quota-refresh`,
        { method: "POST" },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      toast.success("账号额度已更新");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "额度刷新失败");
    } finally {
      onRefresh();
      setRefreshing(false);
    }
  }

  return (
    <TableRow>
      <TableCell className="max-w-0 pl-4">
        <div className="min-w-0">
          <p className="truncate font-medium">
            {account.display_name ?? account.account_key}
          </p>
          <div className="mt-0.5 flex min-w-0 items-center gap-1.5 text-xs text-muted-foreground">
            <span className="shrink-0">
              {providerLabel(account.provider_id)}
            </span>
            <span aria-hidden="true">·</span>
            <span className="truncate">
              {account.account_email ?? account.account_key}
            </span>
          </div>
          <div className="mt-2 md:hidden">
            <Badge variant="outline">{accountStatusLabel(account)}</Badge>
          </div>
          <div className="mt-2 lg:hidden">
            <MobileQuotaSummary
              account={account}
              fiveHour={fiveHour}
              weekly={weekly}
            />
          </div>
        </div>
      </TableCell>
      <TableCell className="hidden overflow-hidden md:table-cell">
        <div className="flex items-center gap-2 whitespace-nowrap">
          <Badge variant="outline">{accountStatusLabel(account)}</Badge>
          {reauthorizationRequired && canReauthorize ? (
            <Button
              variant="outline"
              size="sm"
              className="h-7"
              onClick={onReauthorize}
            >
              <KeyRound aria-hidden="true" />
              重新授权
            </Button>
          ) : null}
        </div>
        {account.provider_id === "dreamina-cli" ? (
          <p className="mt-1 line-clamp-2 whitespace-normal break-words text-xs leading-5 text-muted-foreground">
            {dreaminaCredentialStatusLabel(account)}
          </p>
        ) : (
          <p className="mt-1 line-clamp-2 whitespace-normal break-words text-xs leading-5 text-muted-foreground">
            {runtimeLabel(account.runtime_status, account.completion_mode)}
          </p>
        )}
      </TableCell>
      <TableCell className="hidden lg:table-cell">
        <div className="space-y-2">
          <AccountQuotaSummary
            account={account}
            fiveHour={fiveHour}
            weekly={weekly}
          />
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <Clock3 className="size-3 shrink-0" aria-hidden="true" />
            <span className="min-w-0 flex-1 truncate">
              同步 {formatDateTime(account.upstream_quota.observed_at_ms)}
            </span>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon"
                  className="size-7 shrink-0"
                  aria-label={`刷新 ${account.display_name ?? account.account_key} 的额度`}
                  disabled={refreshing || !canRefreshQuota}
                  onClick={() => void refreshQuota()}
                >
                  {refreshing ? (
                    <LoaderCircle className="animate-spin" aria-hidden="true" />
                  ) : (
                    <RefreshCw aria-hidden="true" />
                  )}
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                {canRefreshQuota
                  ? quotaRefreshLabel
                  : "该供应商暂未启用额度观测"}
              </TooltipContent>
            </Tooltip>
          </div>
        </div>
      </TableCell>
      <TableCell>
        <ConcurrencyCell account={account} />
      </TableCell>
      <TableCell className="pr-4 text-right">
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              aria-label={`管理 ${account.display_name ?? account.account_key}`}
            >
              <MoreHorizontal aria-hidden="true" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onSelect={onManage}>
              <Settings2 aria-hidden="true" />
              账户设置
            </DropdownMenuItem>
            {account.provider_id === "grok-cli" ? (
              <DropdownMenuItem onSelect={onManageVideoStorage}>
                <CloudUpload aria-hidden="true" />
                视频存储
              </DropdownMenuItem>
            ) : null}
            <DropdownMenuItem
              onSelect={onReauthorize}
              disabled={!canReauthorize}
            >
              <KeyRound aria-hidden="true" />
              重新授权登录
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem
              onSelect={() => void refreshQuota()}
              disabled={refreshing || !canRefreshQuota}
            >
              <RefreshCw aria-hidden="true" />
              刷新额度
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </TableCell>
    </TableRow>
  );
}

function AccountQuotaSummary({
  account,
  fiveHour,
  weekly,
}: {
  account: ProviderAccountView;
  fiveHour: UpstreamQuotaWindow | null;
  weekly: UpstreamQuotaWindow | null;
}) {
  if (account.provider_id === "dreamina-cli") {
    return (
      <CreditBalanceCell
        balance={account.upstream_quota.credits_balance}
        observed={account.upstream_quota.status === "observed"}
        planType={account.upstream_quota.plan_type}
      />
    );
  }

  const windows = [
    account.provider_id === "openai-codex"
      ? { label: "5 小时", window: fiveHour }
      : null,
    { label: "每周", window: weekly },
  ].filter(
    (
      item,
    ): item is {
      label: string;
      window: UpstreamQuotaWindow | null;
    } => item !== null,
  );

  return (
    <div className="space-y-2">
      {windows.map((item) => (
        <QuotaWindowRow
          key={item.label}
          label={item.label}
          window={item.window}
        />
      ))}
    </div>
  );
}

function QuotaWindowRow({
  label,
  window,
}: {
  label: string;
  window: UpstreamQuotaWindow | null;
}) {
  if (!window) {
    return (
      <div className="flex items-center justify-between gap-3 text-xs">
        <span className="font-medium">{label}</span>
        <span className="text-muted-foreground">未返回</span>
      </div>
    );
  }
  const remaining = Math.max(0, 100 - window.used_percent);
  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between gap-3 text-xs">
        <span className="font-medium">
          {label} · <span className="tabular-nums">剩余 {remaining}%</span>
        </span>
        <span className="whitespace-nowrap text-muted-foreground">
          {window.resets_at_ms
            ? `${formatDateTime(window.resets_at_ms)} 重置`
            : "重置时间未知"}
        </span>
      </div>
      <Progress className="h-1.5" value={remaining} />
    </div>
  );
}

function MobileQuotaSummary({
  account,
  fiveHour,
  weekly,
}: {
  account: ProviderAccountView;
  fiveHour: UpstreamQuotaWindow | null;
  weekly: UpstreamQuotaWindow | null;
}) {
  if (account.provider_id === "dreamina-cli") {
    const balance = account.upstream_quota.credits_balance;
    return (
      <p className="text-xs text-muted-foreground">
        {account.upstream_quota.status === "observed" && balance !== null
          ? `${formatInteger(balance)} 积分`
          : "积分暂未同步"}
      </p>
    );
  }
  const quota = weekly ?? fiveHour;
  if (!quota)
    return <p className="text-xs text-muted-foreground">额度暂未同步</p>;
  return (
    <p className="text-xs text-muted-foreground">
      {weekly ? "每周" : "5 小时"}剩余{" "}
      <span className="font-medium tabular-nums text-foreground">
        {Math.max(0, 100 - quota.used_percent)}%
      </span>
    </p>
  );
}

function ConcurrencyCell({ account }: { account: ProviderAccountView }) {
  const allocated = Number(account.allocated_count);
  const maximum = Number(account.max_concurrency);
  const available = Number(account.available_capacity);
  const schedulable = isSchedulableAccount(account);
  const utilization =
    maximum > 0 ? Math.min(100, Math.max(0, (allocated / maximum) * 100)) : 0;

  return (
    <div
      className="w-full max-w-36 space-y-1.5"
      aria-label={
        schedulable
          ? `当前执行 ${account.allocated_count}，最大并发 ${account.max_concurrency}，可用 ${account.available_capacity}`
          : `当前执行 ${account.allocated_count}，最大并发 ${account.max_concurrency}，当前不参与调度`
      }
    >
      <div className="flex items-center justify-between gap-2 text-xs">
        <span className="font-medium tabular-nums">
          {formatInteger(account.allocated_count)} /{" "}
          {formatInteger(account.max_concurrency)}
        </span>
        <span className="text-muted-foreground">
          {concurrencyStateLabel(account, allocated, maximum)}
        </span>
      </div>
      <Progress className="h-1.5" value={utilization} />
      <p className="text-xs tabular-nums text-muted-foreground">
        {schedulable
          ? `可用 ${
              Number.isFinite(available)
                ? formatInteger(account.available_capacity)
                : "--"
            }`
          : "当前不参与调度"}
      </p>
    </div>
  );
}

function CreditBalanceCell({
  balance,
  observed,
  planType,
}: {
  balance: string | null;
  observed: boolean;
  planType: string | null;
}) {
  if (!observed || balance === null) {
    return (
      <span className="text-sm text-muted-foreground">本次同步未返回</span>
    );
  }
  return (
    <div>
      <p className="font-medium tabular-nums">{formatInteger(balance)} 积分</p>
      <p className="mt-0.5 text-xs text-muted-foreground">
        {planType ? `会员等级 ${planType}` : "即梦账户余额"}
      </p>
    </div>
  );
}

function GroupTableRow({
  group,
  onEdit,
}: {
  group: ProviderRoute;
  onEdit: () => void;
}) {
  return (
    <TableRow>
      <TableCell className="max-w-64 pl-4">
        <p className="truncate font-medium">{group.display_name}</p>
        <p className="mt-0.5 truncate text-xs text-muted-foreground">
          {group.route_key}
        </p>
      </TableCell>
      <TableCell>
        <div className="max-w-96 space-y-1.5">
          {group.members.map((member) => (
            <div
              key={member.provider_account_id}
              className="flex items-center gap-2 text-sm"
            >
              <span className="min-w-0 flex-1 truncate">
                {member.account_key}
              </span>
              <span className="whitespace-nowrap text-xs tabular-nums text-muted-foreground">
                P {member.priority} · W {member.weight} · 保留{" "}
                {member.minimum_remaining_percent}%
              </span>
            </div>
          ))}
        </div>
      </TableCell>
      <TableCell>
        {providerLabel(group.provider_id)} ·{" "}
        {operationLabel(group.operation_id)}
      </TableCell>
      <TableCell>
        <p className="text-sm">{group.model_mappings.length} 个映射</p>
        <p className="mt-0.5 text-xs text-muted-foreground">
          v{group.revision}
        </p>
      </TableCell>
      <TableCell>{selectionStrategyLabel(group.selection_strategy)}</TableCell>
      <TableCell className="text-sm">
        <p>
          {group.unknown_quota_policy === "block"
            ? "无新鲜额度时暂停"
            : "无新鲜额度时继续"}
        </p>
        <p className="mt-0.5 text-xs text-muted-foreground">
          快照有效 {formatFreshness(group.quota_freshness_ms)}
        </p>
      </TableCell>
      <TableCell>
        <Badge variant="outline">
          {group.state === "enabled" ? "已启用" : "已停用"}
        </Badge>
      </TableCell>
      <TableCell className="pr-4 text-muted-foreground">
        {formatDateTime(group.created_at_ms)}
      </TableCell>
      <TableCell className="pr-4 text-right">
        <Button
          variant="ghost"
          size="icon"
          onClick={onEdit}
          title="编辑账号组"
          aria-label={`编辑 ${group.display_name}`}
        >
          <Settings2 aria-hidden="true" />
        </Button>
      </TableCell>
    </TableRow>
  );
}

function providerLabel(providerId: string) {
  if (providerId === "openai-codex") return "Codex";
  if (providerId === "grok-cli") return "Grok";
  if (providerId === "dreamina-cli") return "即梦";
  return providerId;
}

function operationLabel(operationId: string) {
  if (operationId === "images.generations") return "图片生成";
  if (operationId === "videos.generations") return "视频生成";
  return operationId;
}

function selectionStrategyLabel(strategy: string) {
  if (strategy === "quota_aware_least_loaded") return "额度感知 · 最低负载";
  if (strategy === "priority_weighted") return "优先级 · 权重";
  return strategy;
}

function accountStatusLabel(account: ProviderAccountView) {
  if (account.credential_lifecycle_state === "reauth_required")
    return "需要重新登录";
  if (account.credential_lifecycle_state === "refreshing") return "更新登录中";
  if (account.credential_lifecycle_state === "refresh_due")
    return "等待更新登录";
  if (account.credential_lifecycle_state === "unsupported") {
    return account.provider_id === "dreamina-cli"
      ? "等待隔离环境"
      : "暂不支持自动续期";
  }
  if (account.environment_state === "invalid") return "登录失效";
  if (
    account.provider_id === "dreamina-cli" &&
    account.environment_state === "disabled"
  ) {
    return "缺少 CLI 权限";
  }
  if (account.scheduling_state === "draining") return "排空中";
  if (account.scheduling_state === "disabled") return "已停用";
  if (account.configuration_status !== "configured") return "配置异常";
  return "接收新任务";
}

function isSchedulableAccount(account: ProviderAccountView) {
  return (
    account.environment_state === "active" &&
    account.account_state === "enabled" &&
    account.credential_pool_state === "enabled" &&
    account.profile_state === "enabled" &&
    account.resource_policy_state === "enabled" &&
    account.scheduling_state === "active" &&
    account.configuration_status === "configured"
  );
}

function concurrencyStateLabel(
  account: ProviderAccountView,
  allocated: number,
  maximum: number,
) {
  if (account.scheduling_state === "draining") return "排空中";
  if (!isSchedulableAccount(account)) return "不可调度";
  if (maximum > 0 && allocated >= maximum) return "已满";
  if (allocated > 0) return "运行中";
  return "空闲";
}

function dreaminaCredentialStatusLabel(account: ProviderAccountView) {
  if (account.credential_lifecycle_state === "reauth_required")
    return "登录已失效，需要重新授权";
  if (account.credential_lifecycle_state === "refreshing")
    return "正在检查登录并同步积分";
  if (account.environment_state === "disabled") {
    return "登录成功 · 仅高级或高级以上会员可使用即梦 CLI 生成";
  }
  if (account.credential_consecutive_failures > 0) {
    return `自动检查暂时失败，将按退避策略重试`;
  }
  if (account.credential_next_refresh_at_ms) {
    return `自动续期已开启 · ${formatDateTime(account.credential_next_refresh_at_ms)} 检查`;
  }
  return "自动续期已开启";
}

function AddCliAccountDialog({
  open,
  onOpenChange,
  session,
  onSession,
  providers,
  account,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  session: ProviderLoginSession | null;
  onSession: (session: ProviderLoginSession | null) => void;
  providers: ManagedCliProviderCapability[];
  account: ProviderAccountView | null;
}) {
  const [provider, setProvider] = useState("openai-codex");
  const [displayName, setDisplayName] = useState("");
  const [loginMethod, setLoginMethod] =
    useState<ProviderLoginSession["login_method"]>("browser_oauth");
  const [maxConcurrency, setMaxConcurrency] = useState(1);
  const [selectedOperations, setSelectedOperations] = useState<Set<string>>(
    new Set(["images.generations"]),
  );
  const [pending, setPending] = useState(false);
  const reauthorizing = account !== null;
  const selectedProvider =
    providers.find((item) => item.provider_id === provider) ?? null;
  const maxConcurrencyLimit = selectedProvider?.max_concurrency_limit ?? 64;
  const concurrencyValid =
    Number.isInteger(maxConcurrency) &&
    maxConcurrency >= 1 &&
    maxConcurrency <= maxConcurrencyLimit;

  useEffect(() => {
    if (open && account) {
      setProvider(account.provider_id);
      setDisplayName(account.display_name ?? account.account_key);
      setMaxConcurrency(Number(account.max_concurrency));
      const capability = providers.find(
        (item) => item.provider_id === account.provider_id,
      );
      setLoginMethod(capability?.login_methods[0] ?? "device_code");
      return;
    }
    if (open) return;
    setProvider("openai-codex");
    setDisplayName("");
    setLoginMethod("browser_oauth");
    setMaxConcurrency(1);
    setSelectedOperations(new Set(["images.generations"]));
    setPending(false);
  }, [open, account, providers]);

  useEffect(() => {
    if (!open || account || session) return;
    const capability = providers.find((item) => item.provider_id === provider);
    if (capability) {
      setSelectedOperations(new Set(capability.operation_ids));
    }
  }, [open, account, provider, providers, session]);

  async function startLogin() {
    if (
      selectedProvider?.availability !== "available" ||
      (!reauthorizing &&
        (!displayName.trim() ||
          !concurrencyValid ||
          selectedOperations.size === 0))
    )
      return;
    setPending(true);
    try {
      const endpoint = account
        ? `/api/gateway/admin/v1/provider-accounts/${account.provider_account_id}/reauthorization-sessions`
        : "/api/gateway/admin/v1/provider-account-login-sessions";
      const body = account
        ? { login_method: loginMethod }
        : {
            provider_id: provider,
            display_name: displayName.trim(),
            login_method: loginMethod,
            max_concurrency: maxConcurrency,
            operation_ids: [...selectedOperations],
          };
      const response = await consoleFetch(endpoint, {
        method: "POST",
        body: JSON.stringify(body),
      });
      if (!response.ok) throw new Error(await responseMessage(response));
      onSession((await response.json()) as ProviderLoginSession);
    } catch (error) {
      toast.error(
        error instanceof Error
          ? error.message
          : `无法启动 ${selectedProvider?.display_name ?? "CLI"} 登录`,
      );
    } finally {
      setPending(false);
    }
  }

  const waiting =
    session &&
    ["starting", "waiting_for_user", "validating"].includes(session.status);
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="min-w-0 w-[calc(100%-2rem)]">
        <DialogHeader>
          <DialogTitle>
            {reauthorizing ? `重新授权 ${displayName}` : "添加 CLI 账户"}
          </DialogTitle>
          <DialogDescription>
            {reauthorizing
              ? "重新登录同一个上游账户；账号组、调度参数与 API Key 绑定保持不变。"
              : "选择 CLI 供应商并添加独立账户，账户之间不会共享登录状态。"}
          </DialogDescription>
        </DialogHeader>
        {!session ? (
          <div className="min-w-0 space-y-4">
            <div className="space-y-2">
              <Label htmlFor="cli-provider">CLI 供应商</Label>
              <Select
                value={provider}
                disabled={reauthorizing}
                onValueChange={(value) => {
                  setProvider(value);
                  const capability = providers.find(
                    (item) => item.provider_id === value,
                  );
                  const nextMethod =
                    value === "grok-cli" &&
                    capability?.login_methods.includes("device_code")
                      ? "device_code"
                      : (capability?.login_methods[0] ?? "device_code");
                  setLoginMethod(nextMethod);
                  setSelectedOperations(
                    new Set(capability?.operation_ids ?? []),
                  );
                }}
              >
                <SelectTrigger id="cli-provider">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {providers.map((item) => (
                    <SelectItem key={item.provider_id} value={item.provider_id}>
                      {item.display_name}
                      {item.availability === "available"
                        ? ""
                        : "（当前不可用）"}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                {selectedProvider?.availability === "unavailable"
                  ? selectedProvider.unavailable_reason
                  : selectedProvider
                    ? reauthorizing
                      ? `将更新此 ${selectedProvider.display_name} 账户的登录凭据。`
                      : `将创建 ${[...selectedOperations].map(operationLabel).join("、")}运行配置。`
                    : "正在读取 CLI 运行能力。"}
              </p>
            </div>
            {!reauthorizing ? (
              <div className="space-y-2">
                <Label id="cli-operations-label">启用能力</Label>
                <div
                  className="grid gap-2 sm:grid-cols-2"
                  role="group"
                  aria-labelledby="cli-operations-label"
                >
                  {(selectedProvider?.operation_ids ?? []).map((operation) => {
                    const checked = selectedOperations.has(operation);
                    const Icon =
                      operation === "videos.generations" ? Video : ImageIcon;
                    return (
                      <label
                        key={operation}
                        className="flex cursor-pointer items-center gap-3 border px-3 py-3"
                      >
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() =>
                            setSelectedOperations((current) => {
                              const next = new Set(current);
                              if (next.has(operation)) next.delete(operation);
                              else next.add(operation);
                              return next;
                            })
                          }
                          className="size-4 accent-primary"
                        />
                        <Icon aria-hidden="true" className="size-4" />
                        <span className="text-sm font-medium">
                          {operationLabel(operation)}
                        </span>
                      </label>
                    );
                  })}
                </div>
                {selectedOperations.size === 0 ? (
                  <p className="text-xs text-destructive">
                    至少选择一种生成能力
                  </p>
                ) : null}
              </div>
            ) : null}
            <div className="space-y-2">
              <Label>登录方式</Label>
              <Tabs
                value={loginMethod}
                onValueChange={(value) =>
                  setLoginMethod(value as ProviderLoginSession["login_method"])
                }
              >
                <TabsList className="grid w-full grid-cols-2">
                  <TabsTrigger
                    value="browser_oauth"
                    disabled={
                      !selectedProvider?.login_methods.includes("browser_oauth")
                    }
                  >
                    浏览器 OAuth
                  </TabsTrigger>
                  <TabsTrigger
                    value="device_code"
                    disabled={
                      !selectedProvider?.login_methods.includes("device_code")
                    }
                  >
                    设备码
                  </TabsTrigger>
                </TabsList>
              </Tabs>
              <p className="text-xs text-muted-foreground">
                {loginMethod === "browser_oauth"
                  ? `推荐用于本机部署；授权回调由 ${selectedProvider?.display_name ?? "CLI"} 在服务端接收。`
                  : provider === "openai-codex"
                    ? "适用于远程服务器；需先在 ChatGPT 安全设置中启用设备代码授权。"
                    : "适用于远程部署；在上游页面确认界面展示的短验证码。"}
              </p>
            </div>
            {!reauthorizing ? (
              <div className="grid gap-4 sm:grid-cols-[minmax(0,1fr)_9rem]">
                <div className="space-y-2">
                  <Label htmlFor="cli-account-display-name">备注名称</Label>
                  <Input
                    id="cli-account-display-name"
                    value={displayName}
                    onChange={(event) => setDisplayName(event.target.value)}
                    placeholder="例如：主力 Pro 账户"
                    maxLength={128}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="cli-max-concurrency">初始最大并发</Label>
                  <Input
                    id="cli-max-concurrency"
                    type="number"
                    min={1}
                    max={maxConcurrencyLimit}
                    step={1}
                    value={maxConcurrency}
                    onChange={(event) =>
                      setMaxConcurrency(Number(event.target.value))
                    }
                  />
                </div>
              </div>
            ) : null}
            {!reauthorizing ? (
              <p className="text-xs text-muted-foreground">
                并发属于账号级执行控制；优先级、权重和最低保留额度在账号加入账号组时分别配置。
              </p>
            ) : null}
            <DialogFooter>
              <Button
                onClick={() => void startLogin()}
                disabled={
                  pending ||
                  selectedProvider?.availability !== "available" ||
                  (!reauthorizing &&
                    (!displayName.trim() ||
                      !concurrencyValid ||
                      selectedOperations.size === 0))
                }
              >
                {pending ? (
                  <LoaderCircle className="animate-spin" aria-hidden="true" />
                ) : (
                  <ExternalLink aria-hidden="true" />
                )}
                {loginMethod === "browser_oauth"
                  ? "开始 OAuth 登录"
                  : "生成设备码"}
              </Button>
            </DialogFooter>
          </div>
        ) : (
          <div className="min-w-0 space-y-4">
            {session.status === "waiting_for_user" ? (
              session.login_method === "browser_oauth" ? (
                <>
                  <div className="border p-4 text-center">
                    <p className="font-medium">
                      等待 {providerLabel(session.provider_id)} 授权
                    </p>
                    <p className="mt-1 text-sm text-muted-foreground">
                      登录完成后，此页面会
                      {reauthorizing ? "更新原账户授权" : "添加账号"}
                      并尝试同步额度。
                    </p>
                  </div>
                  <Button
                    className="w-full"
                    disabled={!session.authorization_url}
                    onClick={() =>
                      window.open(
                        session.authorization_url ?? "",
                        "_blank",
                        "noopener,noreferrer",
                      )
                    }
                  >
                    <ExternalLink aria-hidden="true" />
                    打开 {providerLabel(session.provider_id)} 登录
                  </Button>
                </>
              ) : (
                <>
                  <div className="min-w-0 border p-4 text-center">
                    <p className="text-sm text-muted-foreground">设备验证码</p>
                    <p className="mt-2 max-w-full break-all font-mono text-lg font-semibold leading-relaxed tracking-wider sm:text-xl">
                      {session.user_code}
                    </p>
                  </div>
                  <div className="grid min-w-0 gap-2 sm:grid-cols-2">
                    <Button
                      className="min-w-0"
                      variant="outline"
                      onClick={() =>
                        void navigator.clipboard
                          .writeText(session.user_code ?? "")
                          .then(() => toast.success("验证码已复制"))
                      }
                    >
                      <Copy aria-hidden="true" />
                      复制验证码
                    </Button>
                    <Button
                      className="min-w-0"
                      onClick={() =>
                        window.open(
                          session.authorization_url ?? "",
                          "_blank",
                          "noopener,noreferrer",
                        )
                      }
                    >
                      <ExternalLink aria-hidden="true" />
                      打开登录页面
                    </Button>
                  </div>
                </>
              )
            ) : null}
            {waiting && session.status !== "waiting_for_user" ? (
              <div className="flex min-h-28 items-center justify-center gap-2 text-sm text-muted-foreground">
                <LoaderCircle className="animate-spin" aria-hidden="true" />
                {session.status === "validating"
                  ? reauthorizing
                    ? "正在验证身份并更新账户授权"
                    : "正在验证账号并创建运行环境"
                  : "正在启动登录"}
              </div>
            ) : null}
            {session.status === "failed" || session.status === "expired" ? (
              <div className="space-y-3 text-center">
                <p className="text-sm text-destructive">
                  登录未完成，请重新发起。
                </p>
                <Button variant="outline" onClick={() => onSession(null)}>
                  {reauthorizing ? "重新授权" : "重新添加"}
                </Button>
              </div>
            ) : null}
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

function EmptyState({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <div className="flex min-h-48 flex-col items-center justify-center border px-4 text-center">
      <p className="font-medium">{title}</p>
      <p className="mt-1 max-w-md text-sm text-muted-foreground">
        {description}
      </p>
    </div>
  );
}

function deduplicateAccounts(accounts: ProviderAccountView[]) {
  return [
    ...new Map(
      accounts.map((account) => [account.provider_account_id, account]),
    ).values(),
  ];
}

function findWindow(windows: UpstreamQuotaWindow[], durationMins: number) {
  return (
    windows.find(
      (window) =>
        window.limit_id === "codex" &&
        window.window_duration_mins === durationMins,
    ) ??
    windows.find((window) => window.window_duration_mins === durationMins) ??
    null
  );
}

function runtimeLabel(status: string, completionMode: string) {
  if (completionMode === "inline") return "网关内置";
  if (status === "active") return "远程执行 · 在线";
  if (status === "draining") return "远程执行 · 停止中";
  if (status === "configured") return "远程执行 · 待启动";
  if (status === "blocked") return "远程执行 · 配置异常";
  return "状态未知";
}

function formatFreshness(milliseconds: number) {
  const minutes = Math.round(milliseconds / 60_000);
  return minutes >= 60 && minutes % 60 === 0
    ? `${minutes / 60} 小时`
    : `${minutes} 分钟`;
}

async function responseMessage(response: Response) {
  try {
    const body = (await response.json()) as {
      error?: string | { message?: string };
    };
    if (typeof body.error === "string") return body.error;
    if (body.error && typeof body.error.message === "string")
      return body.error.message;
  } catch {
    // Preserve the stable fallback below for non-JSON proxy failures.
  }
  return `请求失败 (${response.status})`;
}
