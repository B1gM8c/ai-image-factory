"use client";

import { useEffect, useMemo, useState, type ReactNode } from "react";
import {
  AlertTriangle,
  ChevronLeft,
  ChevronRight,
  Gift,
  LoaderCircle,
  MoreHorizontal,
  Plus,
  RefreshCw,
  RotateCcw,
} from "lucide-react";
import { toast } from "sonner";
import { AdminQueryError, AdminQuerySkeleton } from "@/components/admin-query-state";
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
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
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
import { Textarea } from "@/components/ui/textarea";
import { useAdminQuery } from "@/hooks/use-admin-query";
import {
  decimalToMicros,
  formatDateTime,
  formatMoneyMicros,
} from "@/lib/admin/format";
import type {
  BillingAccountControlList,
  CreditGrantList,
  CreditGrantState,
  CreditGrantView,
  OrganizationCreditGrantList,
  OrganizationCreditGrantView,
} from "@/lib/admin/types";
import { consoleFetch } from "@/lib/auth/client";

const PAGE_SIZE = 50;

type OrganizationOption = {
  id: string;
  name: string;
};

type CreditGrantRow = CreditGrantView | OrganizationCreditGrantView;

export function CreditGrantsPanel({
  enabled,
  platformOwner,
  organizationId,
  organizations,
}: {
  enabled: boolean;
  platformOwner: boolean;
  organizationId: string | null;
  organizations: OrganizationOption[];
}) {
  const [currency, setCurrency] = useState("USD");
  const [state, setState] = useState<CreditGrantState | "all">("all");
  const [cursors, setCursors] = useState<Array<string | null>>([null]);
  const [issueOpen, setIssueOpen] = useState(false);
  const [revokeTarget, setRevokeTarget] = useState<CreditGrantRow | null>(null);

  useEffect(() => {
    setCursors([null]);
  }, [currency, organizationId, state]);

  const cursor = cursors[cursors.length - 1];
  const endpoint = useMemo(() => {
    const params = new URLSearchParams({
      currency,
      state,
      limit: PAGE_SIZE.toString(),
    });
    if (cursor) params.set("after", cursor);
    if (platformOwner) {
      if (organizationId) params.set("organization_id", organizationId);
      return `/admin/v1/billing/credit-grants?${params.toString()}`;
    }
    if (organizationId) {
      return `/v1/organizations/${encodeURIComponent(
        organizationId,
      )}/billing/credit-grants?${params.toString()}`;
    }
    return "";
  }, [currency, cursor, organizationId, platformOwner, state]);
  const query = useAdminQuery<CreditGrantList | OrganizationCreditGrantList>(
    endpoint,
    enabled && Boolean(platformOwner || organizationId),
  );
  const organizationNames = useMemo(
    () => new Map(organizations.map((organization) => [organization.id, organization.name])),
    [organizations],
  );
  const issueOrganizations = useMemo(
    () =>
      organizationId
        ? organizations.filter(
            (organization) => organization.id === organizationId,
          )
        : organizations,
    [organizationId, organizations],
  );

  return (
    <section className="min-w-0 space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-base font-semibold">Credit Grants</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            赠送额度优先抵扣用量，并按最早到期顺序使用。
          </p>
        </div>
        <div className="flex items-center gap-2">
          {platformOwner ? (
            <Button type="button" onClick={() => setIssueOpen(true)}>
              <Plus aria-hidden="true" />
              发放额度
            </Button>
          ) : null}
          <Button
            type="button"
            variant="outline"
            size="icon"
            aria-label="刷新 Credit Grants"
            onClick={query.retry}
            disabled={query.refreshing}
          >
            <RefreshCw
              className={query.refreshing ? "animate-spin" : ""}
              aria-hidden="true"
            />
          </Button>
        </div>
      </div>

      {query.data ? (
        <div className="grid overflow-hidden rounded-md border sm:grid-cols-2">
          <SummaryItem
            label="可用余额"
            value={formatMoneyMicros(
              query.data.summary.available_micros,
              query.data.currency,
            )}
            detail={`截至 ${formatDateTime(query.data.as_of_ms)}`}
          />
          <SummaryItem
            label="累计到账"
            value={formatMoneyMicros(
              query.data.summary.original_amount_micros,
              query.data.currency,
            )}
            detail="包含已使用、到期和撤销额度"
            last
          />
        </div>
      ) : null}

      <div className="flex flex-col gap-2 sm:flex-row">
        <Select value={currency} onValueChange={setCurrency}>
          <SelectTrigger className="w-full sm:w-28" aria-label="选择币种">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="USD">USD</SelectItem>
            <SelectItem value="CNY">CNY</SelectItem>
          </SelectContent>
        </Select>
        <Select
          value={state}
          onValueChange={(value) => setState(value as CreditGrantState | "all")}
        >
          <SelectTrigger className="w-full sm:w-40" aria-label="筛选额度状态">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部状态</SelectItem>
            <SelectItem value="active">可用</SelectItem>
            <SelectItem value="consuming">使用中</SelectItem>
            <SelectItem value="exhausted">已用完</SelectItem>
            <SelectItem value="expired">已到期</SelectItem>
            <SelectItem value="revoked">已撤销</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {query.loading ? <AdminQuerySkeleton rows={7} /> : null}
      {!query.loading && query.error && !query.data ? (
        <AdminQueryError error={query.error} retry={query.retry} />
      ) : null}
      {query.data && query.error ? (
        <div className="flex items-center gap-2 rounded-md border px-3 py-2 text-sm text-muted-foreground">
          <AlertTriangle className="size-4 shrink-0" aria-hidden="true" />
          当前显示上一次成功同步的额度快照
        </div>
      ) : null}
      {query.data ? (
        <GrantTable
          rows={query.data.data}
          organizationNames={organizationNames}
          showOrganization={!organizationId}
          showSource={platformOwner}
          canRevoke={platformOwner}
          onRevoke={setRevokeTarget}
        />
      ) : null}

      {query.data ? (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">第 {cursors.length} 页</p>
          <div className="flex gap-2">
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() =>
                setCursors((current) =>
                  current.length > 1 ? current.slice(0, -1) : current,
                )
              }
              disabled={cursors.length === 1 || query.refreshing}
            >
              <ChevronLeft aria-hidden="true" />
              上一页
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => {
                const next = query.data?.next_after;
                if (next) setCursors((current) => [...current, next]);
              }}
              disabled={!query.data.has_more || query.refreshing}
            >
              下一页
              <ChevronRight aria-hidden="true" />
            </Button>
          </div>
        </div>
      ) : null}

      <IssueGrantDialog
        open={issueOpen}
        onOpenChange={setIssueOpen}
        organizationId={organizationId}
        organizations={issueOrganizations}
        initialCurrency={currency}
        onIssued={() => {
          if (cursors.length === 1) {
            query.retry();
          } else {
            setCursors([null]);
          }
        }}
      />
      <RevokeGrantDialog
        target={revokeTarget}
        onOpenChange={(open) => {
          if (!open) setRevokeTarget(null);
        }}
        onRevoked={query.retry}
      />
    </section>
  );
}

function GrantTable({
  rows,
  organizationNames,
  showOrganization,
  showSource,
  canRevoke,
  onRevoke,
}: {
  rows: CreditGrantRow[];
  organizationNames: Map<string, string>;
  showOrganization: boolean;
  showSource: boolean;
  canRevoke: boolean;
  onRevoke: (grant: CreditGrantRow) => void;
}) {
  if (rows.length === 0) {
    return (
      <div className="flex min-h-72 flex-col items-center justify-center rounded-md border px-6 text-center">
        <Gift className="size-8 text-muted-foreground" aria-hidden="true" />
        <h3 className="mt-4 text-sm font-medium">暂无 Credit Grants</h3>
        <p className="mt-1 text-sm text-muted-foreground">
          发放后的赠送额度会显示在这里。
        </p>
      </div>
    );
  }

  return (
    <div className="rounded-md border">
      <Table className="min-w-[760px]">
        <TableHeader>
          <TableRow>
            <TableHead className="pl-4">到账时间</TableHead>
            {showOrganization ? <TableHead>组织</TableHead> : null}
            <TableHead>状态</TableHead>
            <TableHead>余额</TableHead>
            <TableHead>到期时间</TableHead>
            {showSource ? <TableHead>来源</TableHead> : null}
            {canRevoke ? <TableHead className="w-14 pr-3" /> : null}
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((grant) => (
            <TableRow key={grant.grant_id}>
              <TableCell className="pl-4">
                {formatGrantDateTime(grant.received_at_ms)}
              </TableCell>
              {showOrganization ? (
                <TableCell>
                  <p className="max-w-52 truncate font-medium">
                    {"organization_id" in grant
                      ? grant.organization_display_name ??
                        organizationNames.get(grant.organization_id) ??
                        grant.organization_id
                      : null}
                  </p>
                </TableCell>
              ) : null}
              <TableCell>
                <GrantStateBadge state={grant.state} />
              </TableCell>
              <TableCell className="font-medium">
                {formatMoneyMicros(grant.available_micros, grant.currency)}
              </TableCell>
              <TableCell>{formatGrantDateTime(grant.expires_at_ms)}</TableCell>
              {showSource ? (
                <TableCell className="max-w-56 truncate text-muted-foreground">
                  {"source_reference" in grant ? grant.source_reference : null}
                </TableCell>
              ) : null}
              {canRevoke ? (
                <TableCell className="pr-3 text-right">
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button
                        type="button"
                        variant="ghost"
                        size="icon"
                        aria-label="额度操作"
                      >
                        <MoreHorizontal aria-hidden="true" />
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end">
                      <DropdownMenuItem
                        disabled={
                          !["active", "consuming"].includes(grant.state) ||
                          grant.available_micros === "0"
                        }
                        onSelect={() => onRevoke(grant)}
                      >
                        <RotateCcw aria-hidden="true" />
                        撤销剩余额度
                      </DropdownMenuItem>
                    </DropdownMenuContent>
                  </DropdownMenu>
                </TableCell>
              ) : null}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function IssueGrantDialog({
  open,
  onOpenChange,
  organizationId,
  organizations,
  initialCurrency,
  onIssued,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  organizationId: string | null;
  organizations: OrganizationOption[];
  initialCurrency: string;
  onIssued: () => void;
}) {
  const [selectedOrganization, setSelectedOrganization] = useState("");
  const [currency, setCurrency] = useState(initialCurrency);
  const [amount, setAmount] = useState("");
  const [expiresAt, setExpiresAt] = useState("");
  const [sourceReference, setSourceReference] = useState("");
  const [reason, setReason] = useState("");
  const [organizationSearch, setOrganizationSearch] = useState("");
  const [debouncedOrganizationSearch, setDebouncedOrganizationSearch] =
    useState("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [idempotencyKey, setIdempotencyKey] = useState("");

  useEffect(() => {
    if (!open) return;
    setSelectedOrganization(organizationId ?? "");
    setCurrency(initialCurrency);
    setAmount("");
    setExpiresAt(localDateTimeValue(Date.now() + 365 * 86_400_000));
    setSourceReference("");
    setReason("");
    setOrganizationSearch("");
    setDebouncedOrganizationSearch("");
    setError(null);
    setIdempotencyKey(crypto.randomUUID());
  }, [initialCurrency, open, organizationId]);

  useEffect(() => {
    const timeout = window.setTimeout(
      () => setDebouncedOrganizationSearch(organizationSearch.trim()),
      250,
    );
    return () => window.clearTimeout(timeout);
  }, [organizationSearch]);

  const organizationDirectoryEndpoint = useMemo(() => {
    const params = new URLSearchParams({ currency: "USD", limit: "100" });
    if (debouncedOrganizationSearch) {
      params.set("query", debouncedOrganizationSearch);
    }
    return `/admin/v1/billing/accounts?${params.toString()}`;
  }, [debouncedOrganizationSearch]);
  const organizationDirectory = useAdminQuery<BillingAccountControlList>(
    organizationDirectoryEndpoint,
    open && !organizationId,
  );
  const availableOrganizations = useMemo(
    () =>
      organizationId
        ? organizations
        : (organizationDirectory.data?.data ?? []).map((organization) => ({
            id: organization.organization_id,
            name: organization.display_name,
          })),
    [organizationDirectory.data, organizationId, organizations],
  );

  useEffect(() => {
    if (
      open &&
      !selectedOrganization &&
      availableOrganizations.length > 0
    ) {
      setSelectedOrganization(availableOrganizations[0].id);
    }
  }, [availableOrganizations, open, selectedOrganization]);

  async function submit() {
    const amountMicros = decimalToMicros(amount);
    const expiration = new Date(expiresAt).getTime();
    if (!selectedOrganization) return setError("请选择组织");
    if (amountMicros === null) return setError("请输入大于 0、最多 6 位小数的金额");
    if (!Number.isFinite(expiration) || expiration <= Date.now()) {
      return setError("到期时间必须晚于当前时间");
    }
    if (!sourceReference.trim()) return setError("请填写来源参考");
    if (!reason.trim()) return setError("请填写发放原因");

    setSaving(true);
    setError(null);
    try {
      const response = await consoleFetch(
        "/api/gateway/admin/v1/billing/credit-grants",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "idempotency-key": idempotencyKey,
          },
          body: JSON.stringify({
            organization_id: selectedOrganization,
            currency,
            amount_micros: amountMicros,
            expires_at_ms: expiration,
            source_reference: sourceReference.trim(),
            reason: reason.trim(),
          }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      toast.success("Credit Grant 已发放");
      onOpenChange(false);
      onIssued();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "额度发放失败");
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[calc(100dvh-2rem)] overflow-y-auto sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>发放 Credit Grant</DialogTitle>
          <DialogDescription>
            赠送额度会优先抵扣组织用量，并在到期后自动失效。
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-2 sm:grid-cols-2">
          <Field label="组织" className="sm:col-span-2">
            {!organizationId ? (
              <Input
                className="mb-2"
                value={organizationSearch}
                onChange={(event) => {
                  setOrganizationSearch(event.target.value);
                  setSelectedOrganization("");
                  setIdempotencyKey(crypto.randomUUID());
                }}
                placeholder="搜索组织名称或 ID"
                aria-label="搜索组织"
              />
            ) : null}
            <Select
              value={selectedOrganization}
              onValueChange={(value) => {
                setSelectedOrganization(value);
                setIdempotencyKey(crypto.randomUUID());
              }}
            >
              <SelectTrigger>
                <SelectValue placeholder="选择组织" />
              </SelectTrigger>
              <SelectContent>
                {availableOrganizations.map((organization) => (
                  <SelectItem key={organization.id} value={organization.id}>
                    {organization.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {!organizationId && organizationDirectory.loading ? (
              <p className="mt-2 text-xs text-muted-foreground">正在加载组织</p>
            ) : null}
            {!organizationId && organizationDirectory.error ? (
              <p className="mt-2 text-xs text-destructive">
                {organizationDirectory.error.message}
              </p>
            ) : null}
          </Field>
          <Field label="金额">
            <Input
              inputMode="decimal"
              value={amount}
              onChange={(event) => {
                setAmount(event.target.value);
                setIdempotencyKey(crypto.randomUUID());
              }}
              placeholder="100.00"
            />
          </Field>
          <Field label="币种">
            <Select
              value={currency}
              onValueChange={(value) => {
                setCurrency(value);
                setIdempotencyKey(crypto.randomUUID());
              }}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="USD">USD</SelectItem>
                <SelectItem value="CNY">CNY</SelectItem>
              </SelectContent>
            </Select>
          </Field>
          <Field label="到期时间" className="sm:col-span-2">
            <Input
              type="datetime-local"
              value={expiresAt}
              onChange={(event) => {
                setExpiresAt(event.target.value);
                setIdempotencyKey(crypto.randomUUID());
              }}
            />
          </Field>
          <Field label="来源参考" className="sm:col-span-2">
            <Input
              value={sourceReference}
              onChange={(event) => {
                setSourceReference(event.target.value);
                setIdempotencyKey(crypto.randomUUID());
              }}
              placeholder="例如：launch-promotion-2026"
            />
          </Field>
          <Field label="发放原因" className="sm:col-span-2">
            <Textarea
              value={reason}
              onChange={(event) => {
                setReason(event.target.value);
                setIdempotencyKey(crypto.randomUUID());
              }}
              placeholder="记录本次发放的业务原因"
            />
          </Field>
          {error ? (
            <p className="text-sm text-destructive sm:col-span-2">{error}</p>
          ) : null}
        </div>
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={saving}
          >
            取消
          </Button>
          <Button type="button" onClick={() => void submit()} disabled={saving}>
            {saving ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <Gift aria-hidden="true" />
            )}
            发放额度
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function RevokeGrantDialog({
  target,
  onOpenChange,
  onRevoked,
}: {
  target: CreditGrantRow | null;
  onOpenChange: (open: boolean) => void;
  onRevoked: () => void;
}) {
  const [reason, setReason] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [idempotencyKey, setIdempotencyKey] = useState("");

  useEffect(() => {
    if (!target) return;
    setReason("");
    setError(null);
    setIdempotencyKey(crypto.randomUUID());
  }, [target]);

  async function submit() {
    if (!target || !reason.trim()) return setError("请填写撤销原因");
    setSaving(true);
    setError(null);
    try {
      const response = await consoleFetch(
        `/api/gateway/admin/v1/billing/credit-grants/${encodeURIComponent(
          target.grant_id,
        )}/revoke`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "idempotency-key": idempotencyKey,
          },
          body: JSON.stringify({ reason: reason.trim() }),
        },
      );
      if (!response.ok) throw new Error(await responseMessage(response));
      toast.success("剩余额度已撤销");
      onOpenChange(false);
      onRevoked();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "额度撤销失败");
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog open={Boolean(target)} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>撤销剩余额度</DialogTitle>
          <DialogDescription>
            {target
              ? `将撤销 ${formatMoneyMicros(
                  target.available_micros,
                  target.currency,
                )}，已消费额度不会回退。`
              : null}
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-2 py-2">
          <Label htmlFor="credit-grant-revoke-reason">撤销原因</Label>
          <Textarea
            id="credit-grant-revoke-reason"
            value={reason}
            onChange={(event) => {
              setReason(event.target.value);
              setIdempotencyKey(crypto.randomUUID());
            }}
            placeholder="记录本次撤销的业务原因"
          />
          {error ? <p className="text-sm text-destructive">{error}</p> : null}
        </div>
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={saving}
          >
            取消
          </Button>
          <Button
            type="button"
            variant="destructive"
            onClick={() => void submit()}
            disabled={saving}
          >
            {saving ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <RotateCcw aria-hidden="true" />
            )}
            确认撤销
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function SummaryItem({
  label,
  value,
  detail,
  last = false,
}: {
  label: string;
  value: string;
  detail: string;
  last?: boolean;
}) {
  return (
    <div className={last ? "p-5" : "border-b p-5 sm:border-b-0 sm:border-r"}>
      <p className="text-sm text-muted-foreground">{label}</p>
      <p className="mt-2 text-2xl font-semibold">{value}</p>
      <p className="mt-2 text-xs text-muted-foreground">{detail}</p>
    </div>
  );
}

function GrantStateBadge({ state }: { state: CreditGrantState }) {
  const label: Record<string, string> = {
    active: "可用",
    consuming: "使用中",
    exhausted: "已用完",
    expired: "已到期",
    revoked: "已撤销",
  };
  return (
    <Badge
      variant={["active", "consuming"].includes(state) ? "default" : "secondary"}
    >
      {label[state] ?? state}
    </Badge>
  );
}

function Field({
  label,
  className,
  children,
}: {
  label: string;
  className?: string;
  children: ReactNode;
}) {
  return (
    <div className={className}>
      <Label className="mb-2 block">{label}</Label>
      {children}
    </div>
  );
}

function localDateTimeValue(timestamp: number) {
  const date = new Date(timestamp);
  const local = new Date(timestamp - date.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
}

function formatGrantDateTime(timestamp: number) {
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(timestamp));
}

async function responseMessage(response: Response) {
  const payload = (await response.json().catch(() => null)) as {
    error?: { message?: string };
  } | null;
  return payload?.error?.message ?? `请求失败（${response.status}）`;
}
