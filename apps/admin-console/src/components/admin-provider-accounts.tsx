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
import { useI18n } from "@/i18n/locale-provider";
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

type Translate = ReturnType<typeof useI18n>["t"];

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
  const { t } = useI18n();
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
        ? t({
            en: "Account reauthorized",
            "zh-CN": "账户已重新授权",
            ja: "アカウントを再認証しました",
            ko: "계정이 다시 인증되었습니다",
          })
        : t(
            {
              en: "{provider} account added",
              "zh-CN": "{provider} 账号已添加",
              ja: "{provider} アカウントを追加しました",
              ko: "{provider} 계정이 추가되었습니다",
            },
            { provider: providerLabel(t, loginSession.provider_id) },
          ),
    );
    setReauthorizingAccount(null);
    accountsQuery.retry();
    routesQuery.retry();
  }, [loginSession, reauthorizingAccount, accountsQuery, routesQuery, t]);

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
            t(
              {
                en: "{provider} sign-in was not completed. Start again.",
                "zh-CN": "{provider} 登录未完成，请重新发起",
                ja: "{provider} のログインが完了しませんでした。もう一度開始してください。",
                ko: "{provider} 로그인이 완료되지 않았습니다. 다시 시작하세요.",
              },
              { provider: providerLabel(t, next.provider_id) },
            ),
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
  }, [loginSession, accountsQuery, routesQuery, t]);

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
        title={t({
          en: "CLI accounts and quotas",
          "zh-CN": "CLI 账号与额度",
          ja: "CLI アカウントとクォータ",
          ko: "CLI 계정 및 할당량",
        })}
        description={t({
          en: "Add isolated accounts, monitor upstream quotas, and group accounts for assignment to API keys.",
          "zh-CN":
            "添加独立账号、查看上游额度，并把多个账号组成可分配给 API Key 的账号组",
          ja: "独立したアカウントを追加し、上流クォータを確認し、API キーに割り当てるアカウントグループを作成します。",
          ko: "독립 계정을 추가하고 상위 할당량을 확인하며 API 키에 할당할 계정 그룹을 구성합니다.",
        })}
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
                !canCreateGroup
                  ? t({
                      en: "At least two available accounts from the same provider are required",
                      "zh-CN": "同一供应商至少需要两个可用账号",
                      ja: "同じプロバイダーの利用可能なアカウントが 2 件以上必要です",
                      ko: "동일한 공급자의 사용 가능한 계정이 2개 이상 필요합니다",
                    })
                  : undefined
              }
            >
              <Users aria-hidden="true" />
              {t({
                en: "New account group",
                "zh-CN": "新建账号组",
                ja: "アカウントグループを作成",
                ko: "새 계정 그룹",
              })}
            </Button>
            <Button
              onClick={() => {
                setReauthorizingAccount(null);
                setLoginSession(null);
                setLoginOpen(true);
              }}
            >
              <Plus aria-hidden="true" />
              {t({
                en: "Add CLI account",
                "zh-CN": "添加 CLI 账户",
                ja: "CLI アカウントを追加",
                ko: "CLI 계정 추가",
              })}
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
  const { t } = useI18n();
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
          label={t({
            en: "Managed CLI accounts",
            "zh-CN": "受管 CLI 账号",
            ja: "管理対象 CLI アカウント",
            ko: "관리형 CLI 계정",
          })}
          value={formatInteger(accounts.length.toString())}
          detail={t({
            en: "Each account has an isolated credential environment",
            "zh-CN": "每个账号使用独立凭据环境",
            ja: "各アカウントは独立した認証情報環境を使用します",
            ko: "각 계정은 격리된 인증 정보 환경을 사용합니다",
          })}
          icon={TerminalSquare}
        />
        <MetricCard
          label={t({
            en: "Account groups",
            "zh-CN": "账号组",
            ja: "アカウントグループ",
            ko: "계정 그룹",
          })}
          value={formatInteger(groups.length.toString())}
          detail={t({
            en: "Can be assigned directly to API keys",
            "zh-CN": "可直接分配给 API Key",
            ja: "API キーに直接割り当て可能",
            ko: "API 키에 직접 할당 가능",
          })}
          icon={Layers3}
          tone="info"
        />
        <MetricCard
          label={t({
            en: "Execution concurrency",
            "zh-CN": "执行并发",
            ja: "実行同時数",
            ko: "실행 동시성",
          })}
          value={`${formatInteger(allocated)} / ${formatInteger(totalConcurrency)}`}
          detail={
            queued === "0"
              ? t(
                  {
                    en: "Running / limit · {available} available · no queued jobs",
                    "zh-CN": "已执行 / 上限 · 可用 {available} · 无等待任务",
                    ja: "実行中 / 上限 · 空き {available} · 待機ジョブなし",
                    ko: "실행 중 / 한도 · 사용 가능 {available} · 대기 작업 없음",
                  },
                  { available: formatInteger(available) },
                )
              : t(
                  {
                    en: "{queued} waiting · {batch} batch requests awaiting dispatch",
                    "zh-CN": "等待 {queued} · Batch 待分发 {batch}",
                    ja: "{queued} 件待機 · Batch 配信待ち {batch} 件",
                    ko: "{queued}개 대기 · Batch 배포 대기 {batch}개",
                  },
                  {
                    queued: formatInteger(queued),
                    batch: formatInteger(queue.pending_batch_requests),
                  },
                )
          }
          icon={Gauge}
          tone="success"
        />
        <MetricCard
          label={t({
            en: "Quota synchronized",
            "zh-CN": "额度已同步",
            ja: "クォータ同期済み",
            ko: "할당량 동기화됨",
          })}
          value={`${observed} / ${accounts.length}`}
          detail={t({
            en: "Quotas are observed from the upstream CLI",
            "zh-CN": "额度由 CLI 上游实时观测",
            ja: "クォータは上流 CLI から観測されます",
            ko: "할당량은 상위 CLI에서 관측됩니다",
          })}
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
          {t({
            en: "Refresh",
            "zh-CN": "刷新",
            ja: "更新",
            ko: "새로 고침",
          })}
        </Button>
      </div>

      <Tabs defaultValue="accounts">
        <TabsList>
          <TabsTrigger value="accounts">
            {t({
              en: "Accounts",
              "zh-CN": "账号",
              ja: "アカウント",
              ko: "계정",
            })}
          </TabsTrigger>
          <TabsTrigger value="groups">
            {t({
              en: "Account groups",
              "zh-CN": "账号组",
              ja: "アカウントグループ",
              ko: "계정 그룹",
            })}
          </TabsTrigger>
        </TabsList>
        <TabsContent value="accounts" className="mt-4">
          {accounts.length === 0 ? (
            <EmptyState
              title={t({
                en: "No CLI accounts yet",
                "zh-CN": "还没有 CLI 账号",
                ja: "CLI アカウントはまだありません",
                ko: "아직 CLI 계정이 없습니다",
              })}
              description={t({
                en: "Add a Codex, Grok, or Dreamina account to manage execution capacity and upstream quotas here.",
                "zh-CN":
                  "添加 Codex、Grok 或即梦账号后，可以在这里统一管理运行能力与上游额度。",
                ja: "Codex、Grok、または Dreamina アカウントを追加すると、実行能力と上流クォータをここで一元管理できます。",
                ko: "Codex, Grok 또는 Dreamina 계정을 추가하면 여기에서 실행 용량과 상위 할당량을 통합 관리할 수 있습니다.",
              })}
            />
          ) : (
            <TooltipProvider delayDuration={300}>
              <div className="overflow-hidden rounded-md border">
                <Table className="table-fixed">
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-[36%] pl-4 md:w-[25%]">
                        {t({
                          en: "Account",
                          "zh-CN": "账号",
                          ja: "アカウント",
                          ko: "계정",
                        })}
                      </TableHead>
                      <TableHead className="hidden w-[24%] md:table-cell">
                        {t({
                          en: "Status",
                          "zh-CN": "状态",
                          ja: "ステータス",
                          ko: "상태",
                        })}
                      </TableHead>
                      <TableHead className="hidden w-[31%] lg:table-cell">
                        {t({
                          en: "Upstream quota / credits",
                          "zh-CN": "上游额度 / 积分",
                          ja: "上流クォータ / クレジット",
                          ko: "상위 할당량 / 크레딧",
                        })}
                      </TableHead>
                      <TableHead className="w-[42%] md:w-36">
                        {t({
                          en: "Concurrency",
                          "zh-CN": "执行并发",
                          ja: "同時実行数",
                          ko: "동시 실행",
                        })}
                      </TableHead>
                      <TableHead className="w-14 pr-4 text-right">
                        {t({
                          en: "Actions",
                          "zh-CN": "操作",
                          ja: "操作",
                          ko: "작업",
                        })}
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
              title={t({
                en: "No account groups yet",
                "zh-CN": "还没有账号组",
                ja: "アカウントグループはまだありません",
                ko: "아직 계정 그룹이 없습니다",
              })}
              description={t({
                en: "Group accounts with the same capability so the group can be selected when creating an API key.",
                "zh-CN":
                  "把多个同能力账号加入一组，创建 API Key 时可直接选择该组。",
                ja: "同じ機能を持つアカウントをグループ化すると、API キー作成時にそのグループを選択できます。",
                ko: "동일한 기능의 계정을 그룹화하면 API 키를 만들 때 해당 그룹을 선택할 수 있습니다.",
              })}
            />
          ) : (
            <div className="border">
              <Table className="min-w-[1080px]">
                <TableHeader>
                  <TableRow>
                    <TableHead className="pl-4">
                      {t({
                        en: "Account group",
                        "zh-CN": "账号组",
                        ja: "アカウントグループ",
                        ko: "계정 그룹",
                      })}
                    </TableHead>
                    <TableHead>
                      {t({
                        en: "Members",
                        "zh-CN": "成员",
                        ja: "メンバー",
                        ko: "멤버",
                      })}
                    </TableHead>
                    <TableHead>
                      {t({
                        en: "Capability",
                        "zh-CN": "能力",
                        ja: "機能",
                        ko: "기능",
                      })}
                    </TableHead>
                    <TableHead>
                      {t({
                        en: "External models",
                        "zh-CN": "对外模型",
                        ja: "外部モデル",
                        ko: "외부 모델",
                      })}
                    </TableHead>
                    <TableHead>
                      {t({
                        en: "Scheduling policy",
                        "zh-CN": "调度策略",
                        ja: "スケジューリングポリシー",
                        ko: "스케줄링 정책",
                      })}
                    </TableHead>
                    <TableHead>
                      {t({
                        en: "Quota protection",
                        "zh-CN": "额度保护",
                        ja: "クォータ保護",
                        ko: "할당량 보호",
                      })}
                    </TableHead>
                    <TableHead>
                      {t({
                        en: "Status",
                        "zh-CN": "状态",
                        ja: "ステータス",
                        ko: "상태",
                      })}
                    </TableHead>
                    <TableHead className="pr-4">
                      {t({
                        en: "Created",
                        "zh-CN": "创建时间",
                        ja: "作成日時",
                        ko: "생성 시간",
                      })}
                    </TableHead>
                    <TableHead className="w-14 pr-4 text-right">
                      {t({
                        en: "Actions",
                        "zh-CN": "操作",
                        ja: "操作",
                        ko: "작업",
                      })}
                    </TableHead>
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
  const { t } = useI18n();
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
      ? t({
          en: "Refresh weekly quota",
          "zh-CN": "刷新每周额度",
          ja: "週間クォータを更新",
          ko: "주간 할당량 새로 고침",
        })
      : account.provider_id === "dreamina-cli"
        ? t({
            en: "Refresh credit balance",
            "zh-CN": "刷新积分余额",
            ja: "クレジット残高を更新",
            ko: "크레딧 잔액 새로 고침",
          })
        : t({
            en: "Refresh 5-hour and weekly quotas",
            "zh-CN": "刷新 5 小时与每周额度",
            ja: "5 時間・週間クォータを更新",
            ko: "5시간 및 주간 할당량 새로 고침",
          });
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
      if (!response.ok) throw new Error(await responseMessage(response, t));
      toast.success(
        t({
          en: "Account quota updated",
          "zh-CN": "账号额度已更新",
          ja: "アカウントのクォータを更新しました",
          ko: "계정 할당량이 업데이트되었습니다",
        }),
      );
    } catch (error) {
      toast.error(
        error instanceof Error
          ? error.message
          : t({
              en: "Failed to refresh quota",
              "zh-CN": "额度刷新失败",
              ja: "クォータを更新できませんでした",
              ko: "할당량을 새로 고치지 못했습니다",
            }),
      );
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
              {providerLabel(t, account.provider_id)}
            </span>
            <span aria-hidden="true">·</span>
            <span className="truncate">
              {account.account_email ?? account.account_key}
            </span>
          </div>
          <div className="mt-2 md:hidden">
            <Badge variant="outline">{accountStatusLabel(t, account)}</Badge>
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
          <Badge variant="outline">{accountStatusLabel(t, account)}</Badge>
          {reauthorizationRequired && canReauthorize ? (
            <Button
              variant="outline"
              size="sm"
              className="h-7"
              onClick={onReauthorize}
            >
              <KeyRound aria-hidden="true" />
              {t({
                en: "Reauthorize",
                "zh-CN": "重新授权",
                ja: "再認証",
                ko: "다시 인증",
              })}
            </Button>
          ) : null}
        </div>
        {account.provider_id === "dreamina-cli" ? (
          <p className="mt-1 line-clamp-2 whitespace-normal break-words text-xs leading-5 text-muted-foreground">
            {dreaminaCredentialStatusLabel(t, account)}
          </p>
        ) : (
          <p className="mt-1 line-clamp-2 whitespace-normal break-words text-xs leading-5 text-muted-foreground">
            {runtimeLabel(
              t,
              account.runtime_status,
              account.completion_mode,
            )}
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
              {t(
                {
                  en: "Synchronized {time}",
                  "zh-CN": "同步 {time}",
                  ja: "{time} に同期",
                  ko: "{time} 동기화",
                },
                {
                  time: formatDateTime(
                    account.upstream_quota.observed_at_ms,
                  ),
                },
              )}
            </span>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon"
                  className="size-7 shrink-0"
                  aria-label={t(
                    {
                      en: "Refresh quota for {account}",
                      "zh-CN": "刷新 {account} 的额度",
                      ja: "{account} のクォータを更新",
                      ko: "{account} 할당량 새로 고침",
                    },
                    {
                      account: account.display_name ?? account.account_key,
                    },
                  )}
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
                  : t({
                      en: "Quota observation is not enabled for this provider",
                      "zh-CN": "该供应商暂未启用额度观测",
                      ja: "このプロバイダーではクォータ観測が有効になっていません",
                      ko: "이 공급자에는 할당량 관측이 활성화되지 않았습니다",
                    })}
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
              aria-label={t(
                {
                  en: "Manage {account}",
                  "zh-CN": "管理 {account}",
                  ja: "{account} を管理",
                  ko: "{account} 관리",
                },
                { account: account.display_name ?? account.account_key },
              )}
            >
              <MoreHorizontal aria-hidden="true" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onSelect={onManage}>
              <Settings2 aria-hidden="true" />
              {t({
                en: "Account settings",
                "zh-CN": "账户设置",
                ja: "アカウント設定",
                ko: "계정 설정",
              })}
            </DropdownMenuItem>
            {account.provider_id === "grok-cli" ? (
              <DropdownMenuItem onSelect={onManageVideoStorage}>
                <CloudUpload aria-hidden="true" />
                {t({
                  en: "Video storage",
                  "zh-CN": "视频存储",
                  ja: "動画ストレージ",
                  ko: "동영상 스토리지",
                })}
              </DropdownMenuItem>
            ) : null}
            <DropdownMenuItem
              onSelect={onReauthorize}
              disabled={!canReauthorize}
            >
              <KeyRound aria-hidden="true" />
              {t({
                en: "Reauthorize sign-in",
                "zh-CN": "重新授权登录",
                ja: "ログインを再認証",
                ko: "로그인 다시 인증",
              })}
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem
              onSelect={() => void refreshQuota()}
              disabled={refreshing || !canRefreshQuota}
            >
              <RefreshCw aria-hidden="true" />
              {t({
                en: "Refresh quota",
                "zh-CN": "刷新额度",
                ja: "クォータを更新",
                ko: "할당량 새로 고침",
              })}
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
  const { t } = useI18n();
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
      ? {
          label: t({
            en: "5 hours",
            "zh-CN": "5 小时",
            ja: "5 時間",
            ko: "5시간",
          }),
          window: fiveHour,
        }
      : null,
    {
      label: t({
        en: "Weekly",
        "zh-CN": "每周",
        ja: "週間",
        ko: "주간",
      }),
      window: weekly,
    },
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
  const { t } = useI18n();
  if (!window) {
    return (
      <div className="flex items-center justify-between gap-3 text-xs">
        <span className="font-medium">{label}</span>
        <span className="text-muted-foreground">
          {t({
            en: "Not returned",
            "zh-CN": "未返回",
            ja: "未取得",
            ko: "반환되지 않음",
          })}
        </span>
      </div>
    );
  }
  const remaining = Math.max(0, 100 - window.used_percent);
  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between gap-3 text-xs">
        <span className="font-medium">
          {t(
            {
              en: "{label} · {remaining}% remaining",
              "zh-CN": "{label} · 剩余 {remaining}%",
              ja: "{label} · 残り {remaining}%",
              ko: "{label} · {remaining}% 남음",
            },
            { label, remaining },
          )}
        </span>
        <span className="whitespace-nowrap text-muted-foreground">
          {window.resets_at_ms
            ? t(
                {
                  en: "Resets {time}",
                  "zh-CN": "{time} 重置",
                  ja: "{time} にリセット",
                  ko: "{time} 재설정",
                },
                { time: formatDateTime(window.resets_at_ms) },
              )
            : t({
                en: "Reset time unknown",
                "zh-CN": "重置时间未知",
                ja: "リセット時刻不明",
                ko: "재설정 시간 알 수 없음",
              })}
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
  const { t } = useI18n();
  if (account.provider_id === "dreamina-cli") {
    const balance = account.upstream_quota.credits_balance;
    return (
      <p className="text-xs text-muted-foreground">
        {account.upstream_quota.status === "observed" && balance !== null
          ? t(
              {
                en: "{count} credits",
                "zh-CN": "{count} 积分",
                ja: "{count} クレジット",
                ko: "{count} 크레딧",
              },
              { count: formatInteger(balance) },
            )
          : t({
              en: "Credits not synchronized",
              "zh-CN": "积分暂未同步",
              ja: "クレジット未同期",
              ko: "크레딧이 동기화되지 않음",
            })}
      </p>
    );
  }
  const quota = weekly ?? fiveHour;
  if (!quota)
    return (
      <p className="text-xs text-muted-foreground">
        {t({
          en: "Quota not synchronized",
          "zh-CN": "额度暂未同步",
          ja: "クォータ未同期",
          ko: "할당량이 동기화되지 않음",
        })}
      </p>
    );
  return (
    <p className="text-xs text-muted-foreground">
      {t(
        {
          en: "{window}: {remaining}% remaining",
          "zh-CN": "{window}剩余 {remaining}%",
          ja: "{window}: 残り {remaining}%",
          ko: "{window}: {remaining}% 남음",
        },
        {
          window: weekly
            ? t({
                en: "Weekly",
                "zh-CN": "每周",
                ja: "週間",
                ko: "주간",
              })
            : t({
                en: "5 hours",
                "zh-CN": "5 小时",
                ja: "5 時間",
                ko: "5시간",
              }),
          remaining: Math.max(0, 100 - quota.used_percent),
        },
      )}
    </p>
  );
}

function ConcurrencyCell({ account }: { account: ProviderAccountView }) {
  const { t } = useI18n();
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
          ? t(
              {
                en: "{allocated} running, maximum concurrency {maximum}, {available} available",
                "zh-CN":
                  "当前执行 {allocated}，最大并发 {maximum}，可用 {available}",
                ja: "実行中 {allocated}、最大同時実行数 {maximum}、空き {available}",
                ko: "현재 실행 {allocated}, 최대 동시 실행 {maximum}, 사용 가능 {available}",
              },
              {
                allocated: account.allocated_count,
                maximum: account.max_concurrency,
                available: account.available_capacity,
              },
            )
          : t(
              {
                en: "{allocated} running, maximum concurrency {maximum}, not currently scheduled",
                "zh-CN":
                  "当前执行 {allocated}，最大并发 {maximum}，当前不参与调度",
                ja: "実行中 {allocated}、最大同時実行数 {maximum}、現在はスケジューリング対象外",
                ko: "현재 실행 {allocated}, 최대 동시 실행 {maximum}, 현재 스케줄링 제외",
              },
              {
                allocated: account.allocated_count,
                maximum: account.max_concurrency,
              },
            )
      }
    >
      <div className="flex items-center justify-between gap-2 text-xs">
        <span className="font-medium tabular-nums">
          {formatInteger(account.allocated_count)} /{" "}
          {formatInteger(account.max_concurrency)}
        </span>
        <span className="text-muted-foreground">
          {concurrencyStateLabel(t, account, allocated, maximum)}
        </span>
      </div>
      <Progress className="h-1.5" value={utilization} />
      <p className="text-xs tabular-nums text-muted-foreground">
        {schedulable
          ? t(
              {
                en: "{count} available",
                "zh-CN": "可用 {count}",
                ja: "空き {count}",
                ko: "사용 가능 {count}",
              },
              {
                count: Number.isFinite(available)
                  ? formatInteger(account.available_capacity)
                  : "--",
              },
            )
          : t({
              en: "Not currently scheduled",
              "zh-CN": "当前不参与调度",
              ja: "現在はスケジューリング対象外",
              ko: "현재 스케줄링 제외",
            })}
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
  const { t } = useI18n();
  if (!observed || balance === null) {
    return (
      <span className="text-sm text-muted-foreground">
        {t({
          en: "No value returned by this synchronization",
          "zh-CN": "本次同步未返回",
          ja: "今回の同期では値を取得できませんでした",
          ko: "이번 동기화에서 값이 반환되지 않았습니다",
        })}
      </span>
    );
  }
  return (
    <div>
      <p className="font-medium tabular-nums">
        {t(
          {
            en: "{count} credits",
            "zh-CN": "{count} 积分",
            ja: "{count} クレジット",
            ko: "{count} 크레딧",
          },
          { count: formatInteger(balance) },
        )}
      </p>
      <p className="mt-0.5 text-xs text-muted-foreground">
        {planType
          ? t(
              {
                en: "Membership tier: {tier}",
                "zh-CN": "会员等级 {tier}",
                ja: "メンバーシップ: {tier}",
                ko: "멤버십 등급: {tier}",
              },
              { tier: planType },
            )
          : t({
              en: "Dreamina account balance",
              "zh-CN": "即梦账户余额",
              ja: "Dreamina アカウント残高",
              ko: "Dreamina 계정 잔액",
            })}
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
  const { t } = useI18n();
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
                {t(
                  {
                    en: "P {priority} · W {weight} · reserve {reserve}%",
                    "zh-CN": "P {priority} · W {weight} · 保留 {reserve}%",
                    ja: "P {priority} · W {weight} · 予約 {reserve}%",
                    ko: "P {priority} · W {weight} · 보존 {reserve}%",
                  },
                  {
                    priority: member.priority,
                    weight: member.weight,
                    reserve: member.minimum_remaining_percent,
                  },
                )}
              </span>
            </div>
          ))}
        </div>
      </TableCell>
      <TableCell>
        {providerLabel(t, group.provider_id)} ·{" "}
        {operationLabel(t, group.operation_id)}
      </TableCell>
      <TableCell>
        <p className="text-sm">
          {t(
            {
              en: "{count} mappings",
              "zh-CN": "{count} 个映射",
              ja: "{count} 件のマッピング",
              ko: "{count}개 매핑",
            },
            { count: group.model_mappings.length },
          )}
        </p>
        <p className="mt-0.5 text-xs text-muted-foreground">
          v{group.revision}
        </p>
      </TableCell>
      <TableCell>
        {selectionStrategyLabel(t, group.selection_strategy)}
      </TableCell>
      <TableCell className="text-sm">
        <p>
          {group.unknown_quota_policy === "block"
            ? t({
                en: "Pause without fresh quota data",
                "zh-CN": "无新鲜额度时暂停",
                ja: "新しいクォータデータがない場合は一時停止",
                ko: "최신 할당량 데이터가 없으면 일시 중지",
              })
            : t({
                en: "Continue without fresh quota data",
                "zh-CN": "无新鲜额度时继续",
                ja: "新しいクォータデータがなくても続行",
                ko: "최신 할당량 데이터가 없어도 계속",
              })}
        </p>
        <p className="mt-0.5 text-xs text-muted-foreground">
          {t(
            {
              en: "Snapshot valid for {duration}",
              "zh-CN": "快照有效 {duration}",
              ja: "スナップショット有効期間 {duration}",
              ko: "스냅샷 유효 기간 {duration}",
            },
            { duration: formatFreshness(t, group.quota_freshness_ms) },
          )}
        </p>
      </TableCell>
      <TableCell>
        <Badge variant="outline">
          {group.state === "enabled"
            ? t({
                en: "Enabled",
                "zh-CN": "已启用",
                ja: "有効",
                ko: "활성화됨",
              })
            : t({
                en: "Disabled",
                "zh-CN": "已停用",
                ja: "無効",
                ko: "비활성화됨",
              })}
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
          title={t({
            en: "Edit account group",
            "zh-CN": "编辑账号组",
            ja: "アカウントグループを編集",
            ko: "계정 그룹 편집",
          })}
          aria-label={t(
            {
              en: "Edit {group}",
              "zh-CN": "编辑 {group}",
              ja: "{group} を編集",
              ko: "{group} 편집",
            },
            { group: group.display_name },
          )}
        >
          <Settings2 aria-hidden="true" />
        </Button>
      </TableCell>
    </TableRow>
  );
}

function providerLabel(t: Translate, providerId: string) {
  if (providerId === "openai-codex") return "Codex";
  if (providerId === "grok-cli") return "Grok";
  if (providerId === "dreamina-cli")
    return t({
      en: "Dreamina",
      "zh-CN": "即梦",
      ja: "Dreamina",
      ko: "Dreamina",
    });
  return providerId;
}

function operationLabel(t: Translate, operationId: string) {
  if (operationId === "images.generations")
    return t({
      en: "Image generation",
      "zh-CN": "图片生成",
      ja: "画像生成",
      ko: "이미지 생성",
    });
  if (operationId === "images.edits")
    return t({
      en: "Image editing",
      "zh-CN": "图片编辑",
      ja: "画像編集",
      ko: "이미지 편집",
    });
  if (operationId === "videos.generations")
    return t({
      en: "Video generation",
      "zh-CN": "视频生成",
      ja: "動画生成",
      ko: "동영상 생성",
    });
  return operationId;
}

function selectionStrategyLabel(t: Translate, strategy: string) {
  if (strategy === "quota_aware_least_loaded")
    return t({
      en: "Quota-aware · least loaded",
      "zh-CN": "额度感知 · 最低负载",
      ja: "クォータ考慮 · 最小負荷",
      ko: "할당량 인식 · 최소 부하",
    });
  if (strategy === "priority_weighted")
    return t({
      en: "Priority · weight",
      "zh-CN": "优先级 · 权重",
      ja: "優先度 · 重み",
      ko: "우선순위 · 가중치",
    });
  return strategy;
}

function accountStatusLabel(t: Translate, account: ProviderAccountView) {
  if (account.credential_lifecycle_state === "reauth_required")
    return t({
      en: "Sign-in required",
      "zh-CN": "需要重新登录",
      ja: "再ログインが必要",
      ko: "다시 로그인 필요",
    });
  if (account.credential_lifecycle_state === "refreshing")
    return t({
      en: "Refreshing sign-in",
      "zh-CN": "更新登录中",
      ja: "ログインを更新中",
      ko: "로그인 갱신 중",
    });
  if (account.credential_lifecycle_state === "refresh_due")
    return t({
      en: "Sign-in refresh due",
      "zh-CN": "等待更新登录",
      ja: "ログイン更新待ち",
      ko: "로그인 갱신 대기",
    });
  if (account.credential_lifecycle_state === "unsupported") {
    return account.provider_id === "dreamina-cli"
      ? t({
          en: "Awaiting isolated environment",
          "zh-CN": "等待隔离环境",
          ja: "分離環境待ち",
          ko: "격리 환경 대기 중",
        })
      : t({
          en: "Automatic renewal unsupported",
          "zh-CN": "暂不支持自动续期",
          ja: "自動更新は未対応",
          ko: "자동 갱신 미지원",
        });
  }
  if (account.environment_state === "invalid")
    return t({
      en: "Authentication expired",
      "zh-CN": "登录失效",
      ja: "認証期限切れ",
      ko: "인증 만료",
    });
  if (
    account.provider_id === "dreamina-cli" &&
    account.environment_state === "disabled"
  ) {
    return t({
      en: "CLI access unavailable",
      "zh-CN": "缺少 CLI 权限",
      ja: "CLI 権限なし",
      ko: "CLI 권한 없음",
    });
  }
  if (account.scheduling_state === "draining")
    return t({
      en: "Draining",
      "zh-CN": "排空中",
      ja: "ドレイン中",
      ko: "드레이닝 중",
    });
  if (account.scheduling_state === "disabled")
    return t({
      en: "Disabled",
      "zh-CN": "已停用",
      ja: "無効",
      ko: "비활성화됨",
    });
  if (account.configuration_status !== "configured")
    return t({
      en: "Configuration issue",
      "zh-CN": "配置异常",
      ja: "設定エラー",
      ko: "구성 오류",
    });
  return t({
    en: "Accepting new jobs",
    "zh-CN": "接收新任务",
    ja: "新しいジョブを受付中",
    ko: "새 작업 수락 중",
  });
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
  t: Translate,
  account: ProviderAccountView,
  allocated: number,
  maximum: number,
) {
  if (account.scheduling_state === "draining")
    return t({
      en: "Draining",
      "zh-CN": "排空中",
      ja: "ドレイン中",
      ko: "드레이닝 중",
    });
  if (!isSchedulableAccount(account))
    return t({
      en: "Unavailable",
      "zh-CN": "不可调度",
      ja: "スケジュール不可",
      ko: "스케줄링 불가",
    });
  if (maximum > 0 && allocated >= maximum)
    return t({
      en: "Full",
      "zh-CN": "已满",
      ja: "上限到達",
      ko: "가득 참",
    });
  if (allocated > 0)
    return t({
      en: "Running",
      "zh-CN": "运行中",
      ja: "実行中",
      ko: "실행 중",
    });
  return t({
    en: "Idle",
    "zh-CN": "空闲",
    ja: "アイドル",
    ko: "유휴",
  });
}

function dreaminaCredentialStatusLabel(
  t: Translate,
  account: ProviderAccountView,
) {
  if (account.credential_lifecycle_state === "reauth_required")
    return t({
      en: "Authentication expired. Reauthorization is required.",
      "zh-CN": "登录已失效，需要重新授权",
      ja: "認証の有効期限が切れました。再認証が必要です。",
      ko: "인증이 만료되었습니다. 다시 인증해야 합니다.",
    });
  if (account.credential_lifecycle_state === "refreshing")
    return t({
      en: "Checking authentication and synchronizing credits",
      "zh-CN": "正在检查登录并同步积分",
      ja: "認証を確認し、クレジットを同期しています",
      ko: "인증을 확인하고 크레딧을 동기화하는 중",
    });
  if (account.environment_state === "disabled") {
    return t({
      en: "Signed in · Dreamina CLI generation requires an Advanced tier or higher",
      "zh-CN": "登录成功 · 仅高级或高级以上会员可使用即梦 CLI 生成",
      ja: "ログイン済み · Dreamina CLI の生成には上級以上のメンバーシップが必要です",
      ko: "로그인됨 · Dreamina CLI 생성은 고급 이상 멤버십이 필요합니다",
    });
  }
  if (account.credential_consecutive_failures > 0) {
    return t({
      en: "Automatic check failed temporarily and will retry with backoff",
      "zh-CN": "自动检查暂时失败，将按退避策略重试",
      ja: "自動確認が一時的に失敗しました。バックオフして再試行します",
      ko: "자동 확인이 일시적으로 실패하여 백오프 후 재시도합니다",
    });
  }
  if (account.credential_next_refresh_at_ms) {
    return t(
      {
        en: "Automatic renewal enabled · next check {time}",
        "zh-CN": "自动续期已开启 · {time} 检查",
        ja: "自動更新有効 · 次回確認 {time}",
        ko: "자동 갱신 활성화 · 다음 확인 {time}",
      },
      { time: formatDateTime(account.credential_next_refresh_at_ms) },
    );
  }
  return t({
    en: "Automatic renewal enabled",
    "zh-CN": "自动续期已开启",
    ja: "自動更新有効",
    ko: "자동 갱신 활성화",
  });
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
  const { locale, t } = useI18n();
  const [provider, setProvider] = useState("openai-codex");
  const [displayName, setDisplayName] = useState("");
  const [loginMethod, setLoginMethod] =
    useState<ProviderLoginSession["login_method"]>("browser_oauth");
  const [localBrowserOAuth, setLocalBrowserOAuth] = useState(false);
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
    setLocalBrowserOAuth(
      ["localhost", "127.0.0.1", "::1"].includes(window.location.hostname),
    );
  }, []);

  const browserOAuthAvailable =
    selectedProvider?.login_methods.includes("browser_oauth") &&
    (provider !== "grok-cli" || localBrowserOAuth);

  useEffect(() => {
    if (
      open &&
      provider === "grok-cli" &&
      !localBrowserOAuth &&
      loginMethod === "browser_oauth"
    ) {
      setLoginMethod("device_code");
    }
  }, [localBrowserOAuth, loginMethod, open, provider]);

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
      if (!response.ok) throw new Error(await responseMessage(response, t));
      onSession((await response.json()) as ProviderLoginSession);
    } catch (error) {
      toast.error(
        error instanceof Error
          ? error.message
          : t(
              {
                en: "Could not start {provider} sign-in",
                "zh-CN": "无法启动 {provider} 登录",
                ja: "{provider} のログインを開始できませんでした",
                ko: "{provider} 로그인을 시작할 수 없습니다",
              },
              { provider: selectedProvider?.display_name ?? "CLI" },
            ),
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
            {reauthorizing
              ? t(
                  {
                    en: "Reauthorize {account}",
                    "zh-CN": "重新授权 {account}",
                    ja: "{account} を再認証",
                    ko: "{account} 다시 인증",
                  },
                  { account: displayName },
                )
              : t({
                  en: "Add CLI account",
                  "zh-CN": "添加 CLI 账户",
                  ja: "CLI アカウントを追加",
                  ko: "CLI 계정 추가",
                })}
          </DialogTitle>
          <DialogDescription>
            {reauthorizing
              ? t({
                  en: "Sign in to the same upstream account again. Account groups, scheduling settings, and API key bindings will remain unchanged.",
                  "zh-CN":
                    "重新登录同一个上游账户；账号组、调度参数与 API Key 绑定保持不变。",
                  ja: "同じ上流アカウントに再度ログインします。アカウントグループ、スケジューリング設定、API キーの紐付けは維持されます。",
                  ko: "동일한 상위 계정에 다시 로그인합니다. 계정 그룹, 스케줄링 설정 및 API 키 연결은 그대로 유지됩니다.",
                })
              : t({
                  en: "Choose a CLI provider and add an isolated account. Sign-in state is not shared between accounts.",
                  "zh-CN":
                    "选择 CLI 供应商并添加独立账户，账户之间不会共享登录状态。",
                  ja: "CLI プロバイダーを選択して独立したアカウントを追加します。アカウント間でログイン状態は共有されません。",
                  ko: "CLI 공급자를 선택하고 격리된 계정을 추가합니다. 계정 간에 로그인 상태를 공유하지 않습니다.",
                })}
          </DialogDescription>
        </DialogHeader>
        {!session ? (
          <div className="min-w-0 space-y-4">
            <div className="space-y-2">
              <Label htmlFor="cli-provider">
                {t({
                  en: "CLI provider",
                  "zh-CN": "CLI 供应商",
                  ja: "CLI プロバイダー",
                  ko: "CLI 공급자",
                })}
              </Label>
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
                        : t({
                            en: " (currently unavailable)",
                            "zh-CN": "（当前不可用）",
                            ja: "（現在利用不可）",
                            ko: "(현재 사용 불가)",
                          })}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                {selectedProvider?.availability === "unavailable"
                  ? selectedProvider.unavailable_reason
                  : selectedProvider
                    ? reauthorizing
                      ? t(
                          {
                            en: "The sign-in credentials for this {provider} account will be updated.",
                            "zh-CN":
                              "将更新此 {provider} 账户的登录凭据。",
                            ja: "この {provider} アカウントのログイン認証情報を更新します。",
                            ko: "이 {provider} 계정의 로그인 인증 정보를 업데이트합니다.",
                          },
                          { provider: selectedProvider.display_name },
                        )
                      : t(
                          {
                            en: "Execution profiles will be created for: {operations}.",
                            "zh-CN": "将创建 {operations} 运行配置。",
                            ja: "{operations} の実行設定を作成します。",
                            ko: "{operations} 실행 구성을 생성합니다.",
                          },
                          {
                            operations: [...selectedOperations]
                              .map((operation) => operationLabel(t, operation))
                              .join(locale === "en" ? ", " : "、"),
                          },
                        )
                    : t({
                        en: "Reading CLI capabilities.",
                        "zh-CN": "正在读取 CLI 运行能力。",
                        ja: "CLI の実行機能を読み込んでいます。",
                        ko: "CLI 실행 기능을 불러오는 중입니다.",
                      })}
              </p>
            </div>
            {!reauthorizing ? (
              <div className="space-y-2">
                <Label id="cli-operations-label">
                  {t({
                    en: "Enabled capabilities",
                    "zh-CN": "启用能力",
                    ja: "有効にする機能",
                    ko: "활성화할 기능",
                  })}
                </Label>
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
                          {operationLabel(t, operation)}
                        </span>
                      </label>
                    );
                  })}
                </div>
                {selectedOperations.size === 0 ? (
                  <p className="text-xs text-destructive">
                    {t({
                      en: "Select at least one generation capability",
                      "zh-CN": "至少选择一种生成能力",
                      ja: "生成機能を 1 つ以上選択してください",
                      ko: "하나 이상의 생성 기능을 선택하세요",
                    })}
                  </p>
                ) : null}
              </div>
            ) : null}
            <div className="space-y-2">
              <Label>
                {t({
                  en: "Sign-in method",
                  "zh-CN": "登录方式",
                  ja: "ログイン方法",
                  ko: "로그인 방법",
                })}
              </Label>
              <Tabs
                value={loginMethod}
                onValueChange={(value) =>
                  setLoginMethod(value as ProviderLoginSession["login_method"])
                }
              >
                <TabsList className="grid w-full grid-cols-2">
                  <TabsTrigger
                    value="browser_oauth"
                    disabled={!browserOAuthAvailable}
                  >
                    {t({
                      en: "Browser OAuth",
                      "zh-CN": "浏览器 OAuth",
                      ja: "ブラウザー OAuth",
                      ko: "브라우저 OAuth",
                    })}
                  </TabsTrigger>
                  <TabsTrigger
                    value="device_code"
                    disabled={
                      !selectedProvider?.login_methods.includes("device_code")
                    }
                  >
                    {t({
                      en: "Device code",
                      "zh-CN": "设备码",
                      ja: "デバイスコード",
                      ko: "기기 코드",
                    })}
                  </TabsTrigger>
                </TabsList>
              </Tabs>
              <p className="text-xs text-muted-foreground">
                {loginMethod === "browser_oauth"
                  ? t(
                      {
                        en: "Recommended for local deployments. {provider} receives the authorization callback on the server.",
                        "zh-CN":
                          "推荐用于本机部署；授权回调由 {provider} 在服务端接收。",
                        ja: "ローカル環境に推奨します。認証コールバックはサーバー上の {provider} が受信します。",
                        ko: "로컬 배포에 권장됩니다. {provider}가 서버에서 인증 콜백을 수신합니다.",
                      },
                      { provider: selectedProvider?.display_name ?? "CLI" },
                    )
                  : provider === "openai-codex"
                    ? t({
                        en: "For remote servers. Enable device-code authorization in ChatGPT security settings first.",
                        "zh-CN":
                          "适用于远程服务器；需先在 ChatGPT 安全设置中启用设备代码授权。",
                        ja: "リモートサーバー向けです。先に ChatGPT のセキュリティ設定でデバイスコード認証を有効にしてください。",
                        ko: "원격 서버용입니다. 먼저 ChatGPT 보안 설정에서 기기 코드 인증을 활성화하세요.",
                      })
                    : t({
                        en: "For remote deployments. Confirm the short verification code on the upstream page.",
                        "zh-CN":
                          "适用于远程部署；在上游页面确认界面展示的短验证码。",
                        ja: "リモート環境向けです。上流ページで画面に表示された短い確認コードを承認してください。",
                        ko: "원격 배포용입니다. 상위 페이지에서 화면에 표시된 짧은 인증 코드를 확인하세요.",
                      })}
              </p>
            </div>
            {!reauthorizing ? (
              <div className="grid gap-4 sm:grid-cols-[minmax(0,1fr)_9rem]">
                <div className="space-y-2">
                  <Label htmlFor="cli-account-display-name">
                    {t({
                      en: "Display name",
                      "zh-CN": "备注名称",
                      ja: "表示名",
                      ko: "표시 이름",
                    })}
                  </Label>
                  <Input
                    id="cli-account-display-name"
                    value={displayName}
                    onChange={(event) => setDisplayName(event.target.value)}
                    placeholder={t({
                      en: "For example: Primary Pro account",
                      "zh-CN": "例如：主力 Pro 账户",
                      ja: "例: メイン Pro アカウント",
                      ko: "예: 기본 Pro 계정",
                    })}
                    maxLength={128}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="cli-max-concurrency">
                    {t({
                      en: "Initial maximum concurrency",
                      "zh-CN": "初始最大并发",
                      ja: "初期最大同時実行数",
                      ko: "초기 최대 동시 실행",
                    })}
                  </Label>
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
                {t({
                  en: "Concurrency is controlled per account. Configure priority, weight, and minimum remaining quota when adding the account to a group.",
                  "zh-CN":
                    "并发属于账号级执行控制；优先级、权重和最低保留额度在账号加入账号组时分别配置。",
                  ja: "同時実行数はアカウント単位で制御されます。優先度、重み、最低クォータ残量は、アカウントをグループに追加するときに設定します。",
                  ko: "동시 실행은 계정 단위로 제어됩니다. 우선순위, 가중치 및 최소 잔여 할당량은 계정을 그룹에 추가할 때 설정합니다.",
                })}
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
                  ? t({
                      en: "Start OAuth sign-in",
                      "zh-CN": "开始 OAuth 登录",
                      ja: "OAuth ログインを開始",
                      ko: "OAuth 로그인 시작",
                    })
                  : t({
                      en: "Generate device code",
                      "zh-CN": "生成设备码",
                      ja: "デバイスコードを生成",
                      ko: "기기 코드 생성",
                    })}
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
                      {t(
                        {
                          en: "Waiting for {provider} authorization",
                          "zh-CN": "等待 {provider} 授权",
                          ja: "{provider} の認証待ち",
                          ko: "{provider} 인증 대기 중",
                        },
                        {
                          provider: providerLabel(t, session.provider_id),
                        },
                      )}
                    </p>
                    <p className="mt-1 text-sm text-muted-foreground">
                      {reauthorizing
                        ? t({
                            en: "After sign-in, this page will update the existing authorization and attempt to synchronize quota.",
                            "zh-CN":
                              "登录完成后，此页面会更新原账户授权并尝试同步额度。",
                            ja: "ログイン完了後、このページで既存の認証を更新し、クォータの同期を試みます。",
                            ko: "로그인이 완료되면 이 페이지에서 기존 인증을 업데이트하고 할당량 동기화를 시도합니다.",
                          })
                        : t({
                            en: "After sign-in, this page will add the account and attempt to synchronize quota.",
                            "zh-CN":
                              "登录完成后，此页面会添加账号并尝试同步额度。",
                            ja: "ログイン完了後、このページでアカウントを追加し、クォータの同期を試みます。",
                            ko: "로그인이 완료되면 이 페이지에서 계정을 추가하고 할당량 동기화를 시도합니다.",
                          })}
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
                    {t(
                      {
                        en: "Open {provider} sign-in",
                        "zh-CN": "打开 {provider} 登录",
                        ja: "{provider} のログインを開く",
                        ko: "{provider} 로그인 열기",
                      },
                      {
                        provider: providerLabel(t, session.provider_id),
                      },
                    )}
                  </Button>
                </>
              ) : (
                <>
                  <div className="min-w-0 border p-4 text-center">
                    <p className="text-sm text-muted-foreground">
                      {t({
                        en: "Device verification code",
                        "zh-CN": "设备验证码",
                        ja: "デバイス確認コード",
                        ko: "기기 인증 코드",
                      })}
                    </p>
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
                          .then(() =>
                            toast.success(
                              t({
                                en: "Verification code copied",
                                "zh-CN": "验证码已复制",
                                ja: "確認コードをコピーしました",
                                ko: "인증 코드가 복사되었습니다",
                              }),
                            ),
                          )
                      }
                    >
                      <Copy aria-hidden="true" />
                      {t({
                        en: "Copy verification code",
                        "zh-CN": "复制验证码",
                        ja: "確認コードをコピー",
                        ko: "인증 코드 복사",
                      })}
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
                      {t({
                        en: "Open sign-in page",
                        "zh-CN": "打开登录页面",
                        ja: "ログインページを開く",
                        ko: "로그인 페이지 열기",
                      })}
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
                    ? t({
                        en: "Validating identity and updating account authorization",
                        "zh-CN": "正在验证身份并更新账户授权",
                        ja: "本人確認を行い、アカウント認証を更新しています",
                        ko: "신원을 확인하고 계정 인증을 업데이트하는 중",
                      })
                    : t({
                        en: "Validating account and creating the execution environment",
                        "zh-CN": "正在验证账号并创建运行环境",
                        ja: "アカウントを確認し、実行環境を作成しています",
                        ko: "계정을 확인하고 실행 환경을 생성하는 중",
                      })
                  : t({
                      en: "Starting sign-in",
                      "zh-CN": "正在启动登录",
                      ja: "ログインを開始しています",
                      ko: "로그인 시작 중",
                    })}
              </div>
            ) : null}
            {session.status === "failed" || session.status === "expired" ? (
              <div className="space-y-3 text-center">
                <p className="text-sm text-destructive">
                  {t({
                    en: "Sign-in was not completed. Start again.",
                    "zh-CN": "登录未完成，请重新发起。",
                    ja: "ログインが完了しませんでした。もう一度開始してください。",
                    ko: "로그인이 완료되지 않았습니다. 다시 시작하세요.",
                  })}
                </p>
                <Button variant="outline" onClick={() => onSession(null)}>
                  {reauthorizing
                    ? t({
                        en: "Reauthorize",
                        "zh-CN": "重新授权",
                        ja: "再認証",
                        ko: "다시 인증",
                      })
                    : t({
                        en: "Add again",
                        "zh-CN": "重新添加",
                        ja: "もう一度追加",
                        ko: "다시 추가",
                      })}
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

function runtimeLabel(t: Translate, status: string, completionMode: string) {
  if (completionMode === "inline")
    return t({
      en: "Built into gateway",
      "zh-CN": "网关内置",
      ja: "ゲートウェイ内蔵",
      ko: "게이트웨이 내장",
    });
  if (status === "active")
    return t({
      en: "Remote execution · online",
      "zh-CN": "远程执行 · 在线",
      ja: "リモート実行 · オンライン",
      ko: "원격 실행 · 온라인",
    });
  if (status === "draining")
    return t({
      en: "Remote execution · stopping",
      "zh-CN": "远程执行 · 停止中",
      ja: "リモート実行 · 停止中",
      ko: "원격 실행 · 중지 중",
    });
  if (status === "configured")
    return t({
      en: "Remote execution · awaiting start",
      "zh-CN": "远程执行 · 待启动",
      ja: "リモート実行 · 起動待ち",
      ko: "원격 실행 · 시작 대기",
    });
  if (status === "blocked")
    return t({
      en: "Remote execution · configuration issue",
      "zh-CN": "远程执行 · 配置异常",
      ja: "リモート実行 · 設定エラー",
      ko: "원격 실행 · 구성 오류",
    });
  return t({
    en: "Unknown status",
    "zh-CN": "状态未知",
    ja: "状態不明",
    ko: "알 수 없는 상태",
  });
}

function formatFreshness(t: Translate, milliseconds: number) {
  const minutes = Math.round(milliseconds / 60_000);
  return minutes >= 60 && minutes % 60 === 0
    ? t(
        {
          en: "{count} hours",
          "zh-CN": "{count} 小时",
          ja: "{count} 時間",
          ko: "{count}시간",
        },
        { count: minutes / 60 },
      )
    : t(
        {
          en: "{count} minutes",
          "zh-CN": "{count} 分钟",
          ja: "{count} 分",
          ko: "{count}분",
        },
        { count: minutes },
      );
}

async function responseMessage(response: Response, t: Translate) {
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
  return t(
    {
      en: "Request failed ({status})",
      "zh-CN": "请求失败 ({status})",
      ja: "リクエストに失敗しました ({status})",
      ko: "요청 실패 ({status})",
    },
    { status: response.status },
  );
}
