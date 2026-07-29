"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  AlertCircle,
  CheckCircle2,
  GitBranch,
  LoaderCircle,
  RefreshCw,
  ServerCog,
  ShieldCheck,
} from "lucide-react";
import { toast } from "sonner";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatDateTime } from "@/lib/admin/format";
import {
  applySystemUpdate,
  checkSystemUpdate,
  getSystemUpdate,
  type SystemUpdateCommand,
  type SystemUpdateSnapshot,
} from "@/lib/admin/system-updates";

const POLL_INTERVAL_MS = 2_500;
const RECENT_COMMAND_LIMIT = 6;

export function SystemUpdatePanel() {
  const [snapshot, setSnapshot] = useState<SystemUpdateSnapshot | null>(null);
  const [loading, setLoading] = useState(true);
  const [mutation, setMutation] = useState<"check" | "apply" | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [confirmOpen, setConfirmOpen] = useState(false);

  const refresh = useCallback(async (signal?: AbortSignal, silent = false) => {
    if (!silent) setLoading(true);
    try {
      const next = await getSystemUpdate(signal);
      setSnapshot(next);
      setError(null);
    } catch (reason) {
      if (!isAbortError(reason)) {
        setError(reason instanceof Error ? reason.message : "系统更新状态加载失败");
      }
    } finally {
      if (!silent) setLoading(false);
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    void refresh(controller.signal);
    return () => controller.abort();
  }, [refresh]);

  const pollingCommandId =
    snapshot?.active_command &&
    ["queued", "running", "restoring"].includes(snapshot.active_command.status)
      ? snapshot.active_command.command_id
      : null;
  useEffect(() => {
    if (!pollingCommandId) return;

    let disposed = false;
    let timer: number | undefined;
    let controller: AbortController | undefined;

    async function poll() {
      controller = new AbortController();
      try {
        const next = await getSystemUpdate(controller.signal);
        if (!disposed) {
          setSnapshot(next);
          setError(null);
        }
      } catch (reason) {
        if (!disposed && !isAbortError(reason)) {
          setError(reason instanceof Error ? reason.message : "系统更新状态刷新失败");
        }
      } finally {
        if (!disposed) timer = window.setTimeout(poll, POLL_INTERVAL_MS);
      }
    }

    timer = window.setTimeout(poll, POLL_INTERVAL_MS);
    return () => {
      disposed = true;
      if (timer !== undefined) window.clearTimeout(timer);
      controller?.abort();
    };
  }, [pollingCommandId]);

  const installEnabled = Boolean(
    snapshot?.apply_enabled === true &&
      snapshot.latest_verified === true &&
      snapshot.update_available === true &&
      snapshot.latest_version &&
      !snapshot.active_command &&
      !mutation,
  );

  const recentCommands = useMemo(
    () => snapshot?.recent_commands.slice(0, RECENT_COMMAND_LIMIT) ?? [],
    [snapshot?.recent_commands],
  );

  async function requestCheck() {
    setMutation("check");
    setError(null);
    try {
      const command = await checkSystemUpdate();
      mergeCommand(command);
      toast.success("更新检查已加入队列");
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : "无法检查更新";
      setError(message);
      toast.error(message);
    } finally {
      setMutation(null);
    }
  }

  async function requestApply() {
    if (!snapshot?.latest_version || !installEnabled) return;
    setMutation("apply");
    setError(null);
    try {
      const command = await applySystemUpdate(snapshot.latest_version);
      mergeCommand(command);
      setConfirmOpen(false);
      toast.success(`已提交 ${snapshot.latest_version} 安装命令`);
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : "无法安装更新";
      setError(message);
      toast.error(message);
    } finally {
      setMutation(null);
    }
  }

  function mergeCommand(command: SystemUpdateCommand) {
    setSnapshot((current) => {
      if (!current) return current;
      return {
        ...current,
        active_command: command,
        recent_commands: [
          command,
          ...current.recent_commands.filter(
            (item) => item.command_id !== command.command_id,
          ),
        ],
      };
    });
  }

  if (loading && !snapshot) {
    return (
      <Card aria-label="正在加载系统更新状态">
        <CardContent className="flex h-28 items-center justify-center">
          <LoaderCircle className="size-5 animate-spin text-muted-foreground" aria-hidden="true" />
        </CardContent>
      </Card>
    );
  }

  if (!snapshot) {
    return (
      <Card>
        <CardContent className="flex min-h-32 flex-col items-center justify-center gap-3 text-center">
          <AlertCircle className="size-5 text-muted-foreground" aria-hidden="true" />
          <p className="text-sm text-muted-foreground">{error ?? "系统更新状态不可用"}</p>
          <Button variant="outline" size="sm" onClick={() => void refresh()}>
            <RefreshCw aria-hidden="true" />
            重试
          </Button>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader className="border-b has-data-[slot=card-action]:grid-cols-1 sm:has-data-[slot=card-action]:grid-cols-[1fr_auto]">
        <CardTitle className="flex items-center gap-2 text-base">
          <ServerCog className="size-4" aria-hidden="true" />
          系统更新
        </CardTitle>
        <CardDescription>
          仅安装通过发布完整性验证的 GitHub Release。
        </CardDescription>
        <CardAction className="col-start-1 row-span-1 row-start-3 flex w-full items-center gap-2 justify-self-stretch sm:col-start-2 sm:row-span-2 sm:row-start-1 sm:w-auto sm:justify-self-end">
          <Button
            variant="outline"
            size="sm"
            disabled={!snapshot.configured || Boolean(snapshot.active_command) || mutation !== null}
            onClick={() => void requestCheck()}
          >
            {mutation === "check" ? (
              <LoaderCircle className="animate-spin" aria-hidden="true" />
            ) : (
              <RefreshCw aria-hidden="true" />
            )}
            检查更新
          </Button>
          <AlertDialog open={confirmOpen} onOpenChange={setConfirmOpen}>
            <AlertDialogTrigger asChild>
              <Button size="sm" disabled={!installEnabled}>
                {mutation === "apply" ? (
                  <LoaderCircle className="animate-spin" aria-hidden="true" />
                ) : (
                  <ShieldCheck aria-hidden="true" />
                )}
                安装更新
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>安装 {snapshot.latest_version}？</AlertDialogTitle>
                <AlertDialogDescription>
                  系统将排空任务、创建恢复点、执行数据库迁移并原子切换完整发布版本。
                  更新期间服务可能短暂进入维护状态。
                </AlertDialogDescription>
              </AlertDialogHeader>
              <div className="grid gap-2 border bg-muted/30 p-3 text-sm sm:grid-cols-2">
                <Definition label="当前版本" value={snapshot.current_version} />
                <Definition label="目标版本" value={snapshot.latest_version ?? "--"} />
              </div>
              <AlertDialogFooter>
                <AlertDialogCancel disabled={mutation === "apply"}>取消</AlertDialogCancel>
                <AlertDialogAction
                  disabled={!installEnabled || mutation === "apply"}
                  onClick={(event) => {
                    event.preventDefault();
                    void requestApply();
                  }}
                >
                  {mutation === "apply" ? (
                    <LoaderCircle className="animate-spin" aria-hidden="true" />
                  ) : (
                    <ShieldCheck aria-hidden="true" />
                  )}
                  确认安装
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </CardAction>
      </CardHeader>

      <CardContent className="space-y-5">
        <div className="grid gap-x-8 gap-y-4 sm:grid-cols-2 xl:grid-cols-4">
          <ReleaseVersion
            label="当前版本"
            version={snapshot.current_version}
            commit={snapshot.current_commit_sha}
            icon={<GitBranch className="size-4" aria-hidden="true" />}
          />
          <ReleaseVersion
            label="最新版本"
            version={snapshot.latest_version ?? "尚未检查"}
            commit={snapshot.latest_commit_sha}
            icon={
              snapshot.latest_verified ? (
                <CheckCircle2 className="size-4" aria-hidden="true" />
              ) : (
                <GitBranch className="size-4" aria-hidden="true" />
              )
            }
          />
          <Definition
            label="发布验证"
            value={
              <Badge variant={snapshot.latest_verified ? "outline" : "secondary"}>
                {snapshot.latest_verified ? "已验证" : "未验证"}
              </Badge>
            }
          />
          <Definition
            label="更新状态"
            value={
              <Badge variant={snapshot.update_available ? "default" : "outline"}>
                {releaseStateLabel(snapshot)}
              </Badge>
            }
          />
        </div>

        <div className="grid gap-3 border-t pt-4 text-sm sm:grid-cols-2 xl:grid-cols-4">
          <Definition label="Release 仓库" value={snapshot.repository ?? "未配置"} mono />
          <Definition label="运行目标" value={snapshot.target_triple} mono />
          <Definition label="最近检查" value={formatDateTime(snapshot.last_checked_at_ms)} />
          <Definition label="最近安装" value={formatDateTime(snapshot.last_applied_at_ms)} />
        </div>

        {!snapshot.configured ? (
          <Notice>
            尚未配置 GitHub Release 仓库。设置服务端
            <code className="mx-1 font-mono text-xs">AIF_UPDATE_GITHUB_REPOSITORY</code>
            后才可检查更新。
          </Notice>
        ) : null}
        {snapshot.configured && !snapshot.apply_enabled ? (
          <Notice>
            当前仅开放更新检查；自动安装门禁尚未启用。
          </Notice>
        ) : null}
        {error || snapshot.last_error_message ? (
          <div className="flex gap-3 border border-destructive/30 bg-destructive/5 p-3 text-sm">
            <AlertCircle className="mt-0.5 size-4 shrink-0 text-destructive" aria-hidden="true" />
            <div className="min-w-0">
              <p className="font-medium text-destructive">更新服务异常</p>
              <p className="mt-1 break-words text-muted-foreground">
                {error ?? snapshot.last_error_message}
              </p>
            </div>
          </div>
        ) : null}

        {snapshot.active_command ? <ActiveCommand command={snapshot.active_command} /> : null}

        <section className="space-y-3" aria-labelledby="recent-system-updates">
          <div>
            <h3 id="recent-system-updates" className="text-sm font-medium">最近命令</h3>
            <p className="text-xs text-muted-foreground">显示最近 {RECENT_COMMAND_LIMIT} 次检查与安装操作。</p>
          </div>
          {recentCommands.length ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>操作</TableHead>
                  <TableHead>版本</TableHead>
                  <TableHead>状态</TableHead>
                  <TableHead>阶段</TableHead>
                  <TableHead>提交时间</TableHead>
                  <TableHead className="text-right">结果</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {recentCommands.map((command) => (
                  <TableRow key={command.command_id}>
                    <TableCell className="font-medium">{actionLabel(command.action)}</TableCell>
                    <TableCell className="font-mono text-xs">
                      {command.target_version ?? "--"}
                    </TableCell>
                    <TableCell><CommandStatus status={command.status} /></TableCell>
                    <TableCell>{phaseLabel(command.phase)}</TableCell>
                    <TableCell>{formatDateTime(command.requested_at_ms)}</TableCell>
                    <TableCell className="max-w-64 text-right text-muted-foreground">
                      <span className="block truncate" title={command.failure_message ?? undefined}>
                        {command.failure_message ?? formatDateTime(command.completed_at_ms)}
                      </span>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <div className="border border-dashed px-4 py-8 text-center text-sm text-muted-foreground">
              暂无更新命令
            </div>
          )}
        </section>
      </CardContent>
    </Card>
  );
}

function ActiveCommand({ command }: { command: SystemUpdateCommand }) {
  if (command.status === "restore_required") {
    return (
      <section
        className="flex gap-3 border border-destructive/30 bg-destructive/5 p-4"
        aria-live="assertive"
      >
        <AlertCircle
          className="mt-0.5 size-4 shrink-0 text-destructive"
          aria-hidden="true"
        />
        <div className="min-w-0">
          <p className="text-sm font-medium text-destructive">系统更新已冻结</p>
          <p className="mt-1 text-sm text-muted-foreground">
            自动恢复未能完成。平台已阻断新的更新命令，请先按恢复手册处理并确认运行版本和数据库状态。
          </p>
          {command.failure_message ? (
            <p className="mt-2 break-words font-mono text-xs text-muted-foreground">
              {command.failure_message}
            </p>
          ) : null}
        </div>
      </section>
    );
  }
  const progress = numericProgress(command.progress);
  return (
    <section className="space-y-3 border bg-muted/20 p-4" aria-live="polite">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-sm font-medium">{actionLabel(command.action)}进行中</p>
          <p className="mt-1 text-xs text-muted-foreground">
            {phaseLabel(command.phase)} · 更新于 {formatDateTime(command.updated_at_ms)}
          </p>
        </div>
        <CommandStatus status={command.status} />
      </div>
      <div
        className="h-1.5 overflow-hidden rounded-full bg-muted"
        role={progress === null ? "status" : "progressbar"}
        aria-label="系统更新进度"
        aria-valuemin={progress === null ? undefined : 0}
        aria-valuemax={progress === null ? undefined : 100}
        aria-valuenow={progress ?? undefined}
      >
        <div
          className={`h-full rounded-full bg-foreground transition-[width] ${
            progress === null ? "w-1/3 animate-pulse" : ""
          }`}
          style={progress === null ? undefined : { width: `${progress}%` }}
        />
      </div>
    </section>
  );
}

function ReleaseVersion({
  label,
  version,
  commit,
  icon,
}: {
  label: string;
  version: string;
  commit: string | null;
  icon: React.ReactNode;
}) {
  return (
    <div className="min-w-0">
      <div className="flex items-center gap-2 text-xs text-muted-foreground">
        {icon}
        {label}
      </div>
      <p className="mt-2 truncate text-lg font-semibold" title={version}>{version}</p>
      <p className="mt-1 truncate font-mono text-xs text-muted-foreground" title={commit ?? undefined}>
        {commit ? commit.slice(0, 12) : "--"}
      </p>
    </div>
  );
}

function Definition({
  label,
  value,
  mono = false,
}: {
  label: string;
  value: React.ReactNode;
  mono?: boolean;
}) {
  return (
    <div className="min-w-0">
      <p className="text-xs text-muted-foreground">{label}</p>
      <div className={`mt-1.5 truncate text-sm ${mono ? "font-mono text-xs" : ""}`} title={typeof value === "string" ? value : undefined}>
        {value}
      </div>
    </div>
  );
}

function Notice({ children }: { children: React.ReactNode }) {
  return (
    <div className="border bg-muted/30 px-3 py-2.5 text-sm text-muted-foreground">
      {children}
    </div>
  );
}

function CommandStatus({ status }: { status: string }) {
  const variant =
    status === "failed" || status === "restore_required"
      ? "destructive"
      : status === "succeeded" || status === "restored"
        ? "outline"
        : "secondary";
  return <Badge variant={variant}>{statusLabel(status)}</Badge>;
}

function actionLabel(action: string) {
  return action === "apply" ? "安装更新" : action === "check" ? "检查更新" : action;
}

function statusLabel(status: string) {
  const labels: Record<string, string> = {
    queued: "排队中",
    running: "执行中",
    succeeded: "已完成",
    failed: "失败",
    restoring: "恢复中",
    restored: "已恢复",
    restore_required: "需要恢复",
  };
  return labels[status] ?? status;
}

function phaseLabel(phase: string) {
  const labels: Record<string, string> = {
    queued: "等待执行",
    preflight: "发布预检",
    staged: "制品已暂存",
    quiescing: "正在排空流量",
    quiesced: "流量已排空",
    recovery_ready: "恢复点已就绪",
    migrated: "数据库已迁移",
    switched: "版本已切换",
    verified: "健康验证",
    restoring: "正在恢复",
    restored: "恢复完成",
    failed: "执行失败",
  };
  return labels[phase] ?? phase;
}

function releaseStateLabel(snapshot: SystemUpdateSnapshot) {
  if (!snapshot.latest_version) return "待检查";
  if (!snapshot.latest_verified) return "等待验证";
  return snapshot.update_available ? "有可用更新" : "已是最新";
}

function numericProgress(progress: Record<string, unknown>) {
  const value = progress.percent;
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return Math.min(100, Math.max(0, value));
}

function isAbortError(reason: unknown) {
  return reason instanceof DOMException && reason.name === "AbortError";
}
